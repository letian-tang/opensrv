// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::error::Error;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::FutureExt;
use mysql_async::prelude::*;
use mysql_async::Opts;
use mysql_common as myc;
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ErrorKind, InitWriter, IntermediaryOptions,
    OkResponse, ParamParser, QueryResultWriter, StatementMetaWriter, ValueInner, U24_MAX,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{timeout, Duration};

struct TestingShim<Q, P, E> {
    columns: Vec<Column>,
    params: Vec<Column>,
    on_q: Q,
    on_p: P,
    on_e: E,
}

#[async_trait]
impl<Q, P, E> AsyncMysqlShim<BufWriter<OwnedWriteHalf>> for TestingShim<Q, P, E>
where
    for<'s> Q: 'static
        + Send
        + Sync
        + FnMut(
            &'s str,
            QueryResultWriter<'s, BufWriter<OwnedWriteHalf>>,
        )
            -> Pin<Box<dyn std::future::Future<Output = Result<(), std::io::Error>> + Send + 's>>,
    P: 'static + Send + Sync + FnMut(&str) -> u32,
    for<'s> E: 'static
        + Send
        + Sync
        + FnMut(
            u32,
            Vec<opensrv_mysql::ParamValue<'s>>,
            QueryResultWriter<'s, BufWriter<OwnedWriteHalf>>,
        )
            -> Pin<Box<dyn std::future::Future<Output = Result<(), std::io::Error>> + Send + 's>>,
{
    type Error = io::Error;

    async fn authenticate(&self, _: &str, _: &[u8], _: &[u8], _: &[u8]) -> bool {
        true
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        let id = (self.on_p)(query);
        info.reply(id, &self.params, &self.columns).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        (self.on_e)(id, params.into_iter().collect(), results).await
    }

    async fn on_close<'a>(&'a mut self, _stmt: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        if query.eq_ignore_ascii_case("SELECT @@socket")
            || query.eq_ignore_ascii_case("SELECT @@wait_timeout")
            || query.eq_ignore_ascii_case("SELECT @@max_allowed_packet")
        {
            results.completed(OkResponse::default()).await
        } else if query.eq_ignore_ascii_case("SELECT @@max_allowed_packet,@@wait_timeout,@@socket")
        {
            let cols = [
                Column {
                    table: String::new(),
                    column: "@@max_allowed_packet".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                    colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                },
                Column {
                    table: String::new(),
                    column: "@@wait_timeout".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                    colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                },
                Column {
                    table: String::new(),
                    column: "@@socket".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING,
                    colflags: myc::constants::ColumnFlags::empty(),
                },
            ];
            let mut row_writer = results.start(&cols).await?;
            row_writer.write_col(67108864u32)?;
            row_writer.write_col(28800u32)?;
            row_writer.write_col(None::<String>)?;
            row_writer.end_row().await?;
            row_writer.finish().await
        } else {
            (self.on_q)(query, results).await
        }
    }

    async fn on_init<'a>(
        &'a mut self,
        _schema: &'a str,
        writer: InitWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        writer.ok().await
    }
}

impl<Q, P, E> TestingShim<Q, P, E>
where
    for<'s> Q: 'static
        + Send
        + Sync
        + FnMut(
            &'s str,
            QueryResultWriter<'s, BufWriter<OwnedWriteHalf>>,
        )
            -> Pin<Box<dyn std::future::Future<Output = Result<(), std::io::Error>> + Send + 's>>,
    P: 'static + Send + Sync + FnMut(&str) -> u32,
    for<'s> E: 'static
        + Send
        + Sync
        + FnMut(
            u32,
            Vec<opensrv_mysql::ParamValue<'s>>,
            QueryResultWriter<'s, BufWriter<OwnedWriteHalf>>,
        )
            -> Pin<Box<dyn std::future::Future<Output = Result<(), std::io::Error>> + Send + 's>>,
{
    fn new(on_q: Q, on_p: P, on_e: E) -> Self {
        TestingShim {
            columns: Vec::new(),
            params: Vec::new(),
            on_q,
            on_p,
            on_e,
        }
    }

    fn with_params(mut self, p: Vec<Column>) -> Self {
        self.params = p;
        self
    }

    fn with_columns(mut self, c: Vec<Column>) -> Self {
        self.columns = c;
        self
    }

    async fn test<C, F>(self, c: C)
    where
        F: Future<Output = Result<(), Box<dyn Error>>> + 'static + Send,
        C: FnOnce(mysql_async::Conn) -> F + Send + Sync + 'static,
    {
        self.test_with_opts(
            |port| Opts::from_url(&format!("mysql://127.0.0.1:{}", port)).unwrap(),
            c,
        )
        .await;
    }

    async fn test_with_opts<C, F, O>(self, opts: O, c: C)
    where
        F: Future<Output = Result<(), Box<dyn Error>>> + 'static + Send,
        C: FnOnce(mysql_async::Conn) -> F + Send + Sync + 'static,
        O: FnOnce(u16) -> Opts + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let opts = opts(port);

        let listen = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();

            let (r, w) = socket.into_split();
            let w = BufWriter::with_capacity(100 * 1024, w);
            AsyncMysqlIntermediary::run_on(self, r, w).await.unwrap();
        });

        let conn = mysql_async::Conn::new(opts).await.unwrap();
        c(conn).await.unwrap();

        let (r1,) = tokio::join!(listen);

        r1.unwrap();
    }
}

#[tokio::test]
async fn it_connects() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|_| async { Ok(()) })
    .await;
}

#[tokio::test]
async fn it_pings() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        db.ping().await.map(|_| ())?;
        Ok(())
    })
    .await;
}

struct InitCountingShim {
    on_init_called: Arc<AtomicBool>,
}

#[async_trait]
impl AsyncMysqlShim<BufWriter<OwnedWriteHalf>> for InitCountingShim {
    type Error = io::Error;

    async fn authenticate(&self, _: &str, _: &[u8], _: &[u8], _: &[u8]) -> bool {
        true
    }

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        _info: StatementMetaWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "prepare not supported in test shim",
        ))
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        _results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "execute not supported in test shim",
        ))
    }

    async fn on_close<'a>(&'a mut self, _stmt: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        if query.eq_ignore_ascii_case("SELECT @@socket")
            || query.eq_ignore_ascii_case("SELECT @@wait_timeout")
            || query.eq_ignore_ascii_case("SELECT @@max_allowed_packet")
        {
            results.completed(OkResponse::default()).await
        } else if query.eq_ignore_ascii_case("SELECT @@max_allowed_packet,@@wait_timeout,@@socket")
        {
            let columns = [
                Column {
                    table: String::new(),
                    column: "@@max_allowed_packet".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                    colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                },
                Column {
                    table: String::new(),
                    column: "@@wait_timeout".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                    colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                },
                Column {
                    table: String::new(),
                    column: "@@socket".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING,
                    colflags: myc::constants::ColumnFlags::empty(),
                },
            ];
            let mut row_writer = results.start(&columns).await?;
            row_writer.write_col(67108864u32)?;
            row_writer.write_col(28800u32)?;
            row_writer.write_col(None::<String>)?;
            row_writer.end_row().await?;
            row_writer.finish().await
        } else {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("unexpected query: {query}"),
            ))
        }
    }

    async fn on_init<'a>(
        &'a mut self,
        _schema: &'a str,
        writer: InitWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        self.on_init_called.store(true, Ordering::SeqCst);
        writer.ok().await
    }
}

#[tokio::test]
async fn handshake_with_initial_database_relies_on_backend_ack() {
    let on_init_called = Arc::new(AtomicBool::new(false));
    let shim = InitCountingShim {
        on_init_called: on_init_called.clone(),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();

        let (r, w) = socket.into_split();
        let w = BufWriter::with_capacity(100 * 1024, w);
        AsyncMysqlIntermediary::run_on(shim, r, w).await.unwrap();
    });

    let opts = Opts::from_url(&format!("mysql://127.0.0.1:{}/initial_db", port)).unwrap();
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.ping().await.unwrap();
    conn.disconnect().await.unwrap();

    server.await.unwrap();

    assert!(
        on_init_called.load(Ordering::SeqCst),
        "backend on_init was not invoked"
    );
}

struct WireShim {
    auth_plugin: &'static str,
}

struct DefaultAuthShim;

#[async_trait]
impl AsyncMysqlShim<BufWriter<OwnedWriteHalf>> for DefaultAuthShim {
    type Error = io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        _info: StatementMetaWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        unreachable!()
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        _results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        unreachable!()
    }

    async fn on_close(&mut self, _stmt: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        _query: &'a str,
        _results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        unreachable!()
    }
}

#[tokio::test]
async fn auth_switch_uses_saved_scramble_and_preserves_sequence() {
    for correct in [false, true] {
        let (mut client, server) = start_wire_server(StrictHandshakeShim {
            password: b"secret",
        })
        .await;
        let (_, greeting) = read_wire_packet(&mut client).await.unwrap();
        let salt_start = greeting[1..].iter().position(|&b| b == 0).unwrap() + 6;
        write_wire_packet(
            &mut client,
            1,
            &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
        )
        .await
        .unwrap();
        let (seq, switch) = timeout(Duration::from_secs(2), read_wire_packet(&mut client))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(seq, 2);
        let prefix = b"\xfecaching_sha2_password\0";
        assert!(switch.starts_with(prefix));
        assert_eq!(switch.len(), prefix.len() + 21);
        assert_eq!(switch.last(), Some(&0));
        let salt = &switch[prefix.len()..prefix.len() + 20];
        assert_eq!(&salt[..8], &greeting[salt_start..salt_start + 8]);
        assert_eq!(&salt[8..], &greeting[salt_start + 27..salt_start + 39]);
        let password = if correct {
            &b"secret"[..]
        } else {
            &b"wrong"[..]
        };
        let scramble = opensrv_mysql::scramble_sha256(salt, password).unwrap();
        write_wire_packet(&mut client, 3, &scramble).await.unwrap();
        let (seq, packet) = timeout(Duration::from_secs(2), read_wire_packet(&mut client))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(seq, 4);
        if correct {
            assert_eq!(packet, [1, 3]);
            let (seq, ok) = read_wire_packet(&mut client).await.unwrap();
            assert_eq!(seq, 5);
            assert_eq!(ok[0], 0);
            write_wire_packet(&mut client, 0, &[0x0e]).await.unwrap();
            assert_eq!(read_wire_packet(&mut client).await.unwrap().0, 1);
            write_wire_packet(&mut client, 0, &[1]).await.unwrap();
        } else {
            assert_eq!(packet[0], 0xff);
            assert_eq!(
                u16::from_le_bytes([packet[1], packet[2]]),
                ErrorKind::ER_ACCESS_DENIED_NO_PASSWORD_ERROR as u16
            );
        }
        let result = timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.is_ok(), correct);
    }
}

#[tokio::test]
async fn auth_switch_timeout_and_disconnect_terminate_connection_task() {
    for disconnect in [false, true] {
        let (mut client, server) = start_wire_server_with_options(
            StrictHandshakeShim {
                password: b"secret",
            },
            IntermediaryOptions {
                auth_timeout: Some(Duration::from_millis(100)),
                ..Default::default()
            },
        )
        .await;
        read_wire_packet(&mut client).await.unwrap();
        write_wire_packet(
            &mut client,
            1,
            &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
        )
        .await
        .unwrap();
        let (seq, packet) = read_wire_packet(&mut client).await.unwrap();
        assert_eq!(seq, 2);
        assert_eq!(packet[0], 0xfe);
        if disconnect {
            client.shutdown().await.unwrap();
        }
        let err = timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(
            err.kind(),
            if disconnect {
                io::ErrorKind::ConnectionAborted
            } else {
                io::ErrorKind::TimedOut
            }
        );
    }
}

#[tokio::test]
async fn initial_handshake_timeout_and_truncated_disconnect_are_errors() {
    for disconnect in [false, true] {
        let (mut client, server) = start_wire_server_with_options(
            StrictHandshakeShim { password: b"" },
            IntermediaryOptions {
                auth_timeout: Some(Duration::from_millis(100)),
                ..Default::default()
            },
        )
        .await;
        read_wire_packet(&mut client).await.unwrap();
        if disconnect {
            client.write_all(&[32, 0, 0, 1, 0]).await.unwrap();
            client.shutdown().await.unwrap();
        }
        let err = timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(
            err.kind(),
            if disconnect {
                io::ErrorKind::UnexpectedEof
            } else {
                io::ErrorKind::TimedOut
            }
        );
    }
}

struct StrictHandshakeShim {
    password: &'static [u8],
}

#[tokio::test]
async fn authentication_uses_the_challenge_from_the_greeting() {
    let (mut client, server) = start_wire_server(StrictHandshakeShim {
        password: b"secret",
    })
    .await;
    let (_, greeting) = read_wire_packet(&mut client).await.unwrap();
    let version_end = greeting[1..].iter().position(|&b| b == 0).unwrap() + 1;
    let salt_start = version_end + 5;
    let mut salt = greeting[salt_start..salt_start + 8].to_vec();
    salt.extend_from_slice(&greeting[salt_start + 27..salt_start + 39]);
    let response = opensrv_mysql::scramble_sha256(&salt, b"secret").unwrap();
    write_wire_packet(
        &mut client,
        1,
        &handshake_response_with_auth(opensrv_mysql::CACHING_SHA2_PASSWORD, None, &response),
    )
    .await
    .unwrap();
    assert_eq!(
        read_wire_packet(&mut client).await.unwrap(),
        (2, vec![1, 3])
    );
    let (seq, ok) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!((seq, ok[0]), (3, 0));
    write_wire_packet(&mut client, 0, &[1]).await.unwrap();
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn handshake_rejects_wrong_initial_and_auth_switch_sequences() {
    for switched in [false, true] {
        for bad_seq in [0, 2, 255] {
            let (mut client, server) =
                start_wire_server(StrictHandshakeShim { password: b"" }).await;
            read_wire_packet(&mut client).await.unwrap();
            if switched {
                write_wire_packet(
                    &mut client,
                    1,
                    &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
                )
                .await
                .unwrap();
                assert_eq!(read_wire_packet(&mut client).await.unwrap().0, 2);
                write_wire_packet(&mut client, bad_seq, &[]).await.unwrap();
            } else {
                write_wire_packet(
                    &mut client,
                    bad_seq,
                    &handshake_response(opensrv_mysql::CACHING_SHA2_PASSWORD, None),
                )
                .await
                .unwrap();
            }
            let err = timeout(Duration::from_secs(2), server)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        }
    }
}

#[async_trait]
impl AsyncMysqlShim<BufWriter<OwnedWriteHalf>> for StrictHandshakeShim {
    type Error = io::Error;
    fn salt(&self) -> [u8; 20] {
        // Deliberately changes on every invocation, including within one connection.
        static NEXT: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(1);
        [NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed); 20]
    }
    fn default_auth_plugin(&self) -> &str {
        opensrv_mysql::CACHING_SHA2_PASSWORD
    }
    async fn auth_plugin_for_username(&self, _: &[u8]) -> &'static str {
        opensrv_mysql::CACHING_SHA2_PASSWORD
    }
    async fn authenticate(&self, _: &str, _: &[u8], salt: &[u8], data: &[u8]) -> bool {
        opensrv_mysql::verify_caching_sha2_password(self.password, salt, data)
    }
    async fn on_init<'a>(
        &'a mut self,
        _: &'a str,
        _: InitWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> io::Result<()> {
        panic!("invalid initial database must not reach on_init")
    }
    async fn on_prepare<'a>(
        &'a mut self,
        _: &'a str,
        _: StatementMetaWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> io::Result<()> {
        unreachable!()
    }
    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: ParamParser<'a>,
        _: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> io::Result<()> {
        unreachable!()
    }
    async fn on_close<'a>(&'a mut self, _: u32) {}
    async fn on_query<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> io::Result<()> {
        unreachable!()
    }
}

#[tokio::test]
async fn caching_sha2_nul_auth_is_verified_as_empty_password() {
    for (password, accepted) in [(&b""[..], true), (&b"secret"[..], false)] {
        let (mut client, server) = start_wire_server(StrictHandshakeShim { password }).await;
        read_wire_packet(&mut client).await.unwrap();
        write_wire_packet(
            &mut client,
            1,
            &handshake_response_with_auth(opensrv_mysql::CACHING_SHA2_PASSWORD, None, &[0]),
        )
        .await
        .unwrap();
        let (seq, packet) = timeout(Duration::from_secs(2), read_wire_packet(&mut client))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(seq, 2);
        assert_eq!(packet[0], if accepted { 0 } else { 0xff });
        if accepted {
            write_wire_packet(&mut client, 0, &[1]).await.unwrap();
        }
        let result = timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.is_ok(), accepted);
    }
}

#[tokio::test]
async fn invalid_initial_database_fails_handshake_without_init() {
    for require_db in [false, true] {
        let (mut client, server) = start_wire_server_with_options(
            StrictHandshakeShim { password: b"" },
            IntermediaryOptions {
                reject_connection_on_dbname_absence: require_db,
                ..Default::default()
            },
        )
        .await;
        read_wire_packet(&mut client).await.unwrap();
        let mut response =
            handshake_response(opensrv_mysql::CACHING_SHA2_PASSWORD, Some("invalid_db"));
        let offset = response
            .windows(10)
            .position(|w| w == b"invalid_db")
            .unwrap();
        response[offset] = 0xff;
        write_wire_packet(&mut client, 1, &response).await.unwrap();
        let (seq, packet) = timeout(Duration::from_secs(2), read_wire_packet(&mut client))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(seq, 2);
        assert_eq!(packet[0], 0xff);
        assert_eq!(
            u16::from_le_bytes([packet[1], packet[2]]),
            ErrorKind::ER_MALFORMED_PACKET as u16
        );
        let result = timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert!(read_wire_packet(&mut client).await.is_err());
    }
}

#[async_trait]
impl AsyncMysqlShim<BufWriter<OwnedWriteHalf>> for WireShim {
    type Error = io::Error;

    fn default_auth_plugin(&self) -> &str {
        self.auth_plugin
    }

    async fn auth_plugin_for_username(&self, _user: &[u8]) -> &'static str {
        self.auth_plugin
    }

    async fn authenticate(
        &self,
        _auth_plugin: &str,
        _username: &[u8],
        _salt: &[u8],
        _auth_data: &[u8],
    ) -> bool {
        true
    }

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        let params = [Column {
            table: String::new(),
            column: "param".to_string(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_BLOB,
            colflags: myc::constants::ColumnFlags::empty(),
        }];
        info.reply(42, &params, &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        results.completed(OkResponse::default()).await
    }

    async fn on_close<'a>(&'a mut self, _stmt: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        _query: &'a str,
        _results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "query unsupported",
        ))
    }
}

struct ResettableShim {
    reset_called: Arc<AtomicBool>,
}

struct MultiStatementShim {
    next_id: u32,
}

#[async_trait]
impl AsyncMysqlShim<BufWriter<OwnedWriteHalf>> for MultiStatementShim {
    type Error = io::Error;

    async fn authenticate(&self, _: &str, _: &[u8], _: &[u8], _: &[u8]) -> bool {
        true
    }

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        self.next_id += 1;
        let params = [Column {
            table: String::new(),
            column: "param".to_string(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_BLOB,
            colflags: myc::constants::ColumnFlags::empty(),
        }];
        info.reply(self.next_id, &params, &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        results.completed(OkResponse::default()).await
    }

    async fn on_close(&mut self, _stmt: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        _query: &'a str,
        results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        results.completed(OkResponse::default()).await
    }
}

#[async_trait]
impl AsyncMysqlShim<BufWriter<OwnedWriteHalf>> for ResettableShim {
    type Error = io::Error;

    async fn authenticate(&self, _: &str, _: &[u8], _: &[u8], _: &[u8]) -> bool {
        true
    }

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        let params = [Column {
            table: String::new(),
            column: "param".to_string(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_BLOB,
            colflags: myc::constants::ColumnFlags::empty(),
        }];
        info.reply(42, &params, &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        results.completed(OkResponse::default()).await
    }

    async fn on_close(&mut self, _stmt: u32) {}

    async fn on_reset_connection(&mut self) -> Result<bool, Self::Error> {
        self.reset_called.store(true, Ordering::SeqCst);
        Ok(true)
    }

    async fn on_query<'a>(
        &'a mut self,
        _query: &'a str,
        results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        results.completed(OkResponse::default()).await
    }
}

async fn read_wire_packet(stream: &mut TcpStream) -> io::Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await?;
    let payload_len =
        (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
    let mut payload = vec![0; payload_len];
    stream.read_exact(&mut payload).await?;
    Ok((header[3], payload))
}

async fn write_wire_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) -> io::Result<()> {
    let len = payload.len();
    assert!(len <= U24_MAX);
    stream
        .write_all(&[len as u8, (len >> 8) as u8, (len >> 16) as u8, seq])
        .await?;
    stream.write_all(payload).await
}

fn handshake_response(auth_plugin: &str, database: Option<&str>) -> Vec<u8> {
    handshake_response_with_auth(auth_plugin, database, &[])
}

fn handshake_response_with_auth(
    auth_plugin: &str,
    database: Option<&str>,
    auth_response: &[u8],
) -> Vec<u8> {
    let mut capabilities = myc::constants::CapabilityFlags::CLIENT_PROTOCOL_41
        | myc::constants::CapabilityFlags::CLIENT_SECURE_CONNECTION
        | myc::constants::CapabilityFlags::CLIENT_PLUGIN_AUTH;
    if database.is_some() {
        capabilities |= myc::constants::CapabilityFlags::CLIENT_CONNECT_WITH_DB;
    }

    let mut response = Vec::new();
    response.extend_from_slice(&capabilities.bits().to_le_bytes());
    response.extend_from_slice(&0u32.to_le_bytes());
    response.push(0x21);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(b"user\0");
    response.push(auth_response.len().try_into().unwrap());
    response.extend_from_slice(auth_response);
    if let Some(database) = database {
        response.extend_from_slice(database.as_bytes());
        response.push(0);
    }
    response.extend_from_slice(auth_plugin.as_bytes());
    response.push(0);
    response
}

async fn execute_wire_statement(client: &mut TcpStream) {
    write_wire_packet(
        client,
        0,
        &[
            0x17,
            0x2a,
            0,
            0,
            0, // command and statement id
            0, // flags
            1,
            0,
            0,
            0, // iteration count
            1, // null bitmap: parameter 0 is NULL
            1, // new params bound
            myc::constants::ColumnType::MYSQL_TYPE_BLOB as u8,
            0, // signed
        ],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn reset_connection_requires_backend_reset_and_clears_statements() {
    let reset_called = Arc::new(AtomicBool::new(false));
    let (mut client, server) = start_wire_server(ResettableShim {
        reset_called: Arc::clone(&reset_called),
    })
    .await;

    read_wire_packet(&mut client).await.unwrap();
    write_wire_packet(
        &mut client,
        1,
        &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
    )
    .await
    .unwrap();
    read_wire_packet(&mut client).await.unwrap();
    prepare_wire_statement(&mut client).await;

    write_wire_packet(&mut client, 0, b"\x1f").await.unwrap();
    assert_eq!(read_wire_packet(&mut client).await.unwrap().1[0], 0);
    assert!(reset_called.load(Ordering::SeqCst));

    execute_wire_statement(&mut client).await;
    assert_execute_error(&mut client, ErrorKind::ER_UNKNOWN_STMT_HANDLER).await;

    write_wire_packet(&mut client, 0, b"\x01").await.unwrap();
    drop(client);
    server.await.unwrap().unwrap();
}

async fn assert_execute_error(client: &mut TcpStream, kind: ErrorKind) {
    let (seq, payload) = read_wire_packet(client).await.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(payload[0], 0xff);
    assert_eq!(u16::from_le_bytes([payload[1], payload[2]]), kind as u16);
}

async fn start_wire_server_with_options<S>(
    shim: S,
    options: IntermediaryOptions,
) -> (TcpStream, tokio::task::JoinHandle<Result<(), io::Error>>)
where
    S: AsyncMysqlShim<BufWriter<OwnedWriteHalf>, Error = io::Error> + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let (r, w) = socket.into_split();
        let w = BufWriter::with_capacity(100 * 1024, w);
        AsyncMysqlIntermediary::run_with_options(shim, r, w, &options).await
    });
    (
        TcpStream::connect(("127.0.0.1", port)).await.unwrap(),
        server,
    )
}

async fn start_wire_server<S>(
    shim: S,
) -> (TcpStream, tokio::task::JoinHandle<Result<(), io::Error>>)
where
    S: AsyncMysqlShim<BufWriter<OwnedWriteHalf>, Error = io::Error> + Send + Sync + 'static,
{
    start_wire_server_with_options(shim, IntermediaryOptions::default()).await
}

#[tokio::test]
async fn greeting_advertises_utf8mb4_autocommit_and_multi_results() {
    let (mut client, server) = start_wire_server(WireShim {
        auth_plugin: opensrv_mysql::MYSQL_NATIVE_PASSWORD,
    })
    .await;
    let greeting = read_wire_packet(&mut client).await.unwrap().1;
    let version_end = greeting[1..].iter().position(|byte| *byte == 0).unwrap() + 1;
    let capability_low = version_end + 1 + 4 + 8 + 1;
    let low = u16::from_le_bytes([greeting[capability_low], greeting[capability_low + 1]]);
    let collation = greeting[capability_low + 2];
    let status = u16::from_le_bytes([greeting[capability_low + 3], greeting[capability_low + 4]]);
    let high = u16::from_le_bytes([greeting[capability_low + 5], greeting[capability_low + 6]]);
    let capabilities = myc::constants::CapabilityFlags::from_bits_truncate(
        u32::from(low) | (u32::from(high) << 16),
    );
    assert_eq!(collation, myc::constants::UTF8MB4_GENERAL_CI as u8);
    assert_ne!(
        status & myc::constants::StatusFlags::SERVER_STATUS_AUTOCOMMIT.bits(),
        0
    );
    assert!(capabilities.contains(myc::constants::CapabilityFlags::CLIENT_MULTI_RESULTS));
    assert!(capabilities.contains(myc::constants::CapabilityFlags::CLIENT_PS_MULTI_RESULTS));

    drop(client);
    assert!(server.await.unwrap().is_err());
}

#[tokio::test]
async fn default_on_init_acks_initial_and_command_database_changes() {
    let (mut client, server) = start_wire_server(WireShim {
        auth_plugin: opensrv_mysql::MYSQL_NATIVE_PASSWORD,
    })
    .await;

    let (seq, _) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 0);
    write_wire_packet(
        &mut client,
        1,
        &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, Some("initial_db")),
    )
    .await
    .unwrap();

    let (seq, payload) = timeout(Duration::from_millis(250), read_wire_packet(&mut client))
        .await
        .expect("default on_init must acknowledge the initial database")
        .unwrap();
    assert_eq!(seq, 2);
    assert_eq!(payload[0], 0x00);

    write_wire_packet(&mut client, 0, b"\x02other_db")
        .await
        .unwrap();
    let (seq, payload) = timeout(Duration::from_millis(250), read_wire_packet(&mut client))
        .await
        .expect("default on_init must acknowledge COM_INIT_DB")
        .unwrap();
    assert_eq!(seq, 1);
    assert_eq!(payload[0], 0x00);

    write_wire_packet(&mut client, 0, b"\x01").await.unwrap();
    drop(client);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn caching_sha2_fast_auth_sends_auth_more_data_before_ok() {
    let (mut client, server) = start_wire_server(WireShim {
        auth_plugin: opensrv_mysql::CACHING_SHA2_PASSWORD,
    })
    .await;

    let (seq, _) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 0);
    write_wire_packet(
        &mut client,
        1,
        &handshake_response_with_auth(opensrv_mysql::CACHING_SHA2_PASSWORD, None, &[0x42; 32]),
    )
    .await
    .unwrap();

    let (seq, payload) = timeout(Duration::from_millis(250), read_wire_packet(&mut client))
        .await
        .expect("fast authentication response")
        .unwrap();
    assert_eq!(seq, 2);
    assert_eq!(payload, [0x01, 0x03]);

    let (seq, payload) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 3);
    assert_eq!(payload[0], 0x00);

    write_wire_packet(&mut client, 0, b"\x01").await.unwrap();
    drop(client);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn default_shim_authentication_is_fail_closed() {
    let (mut client, server) = start_wire_server(DefaultAuthShim).await;
    read_wire_packet(&mut client).await.unwrap();
    write_wire_packet(
        &mut client,
        1,
        &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
    )
    .await
    .unwrap();
    let payload = read_wire_packet(&mut client).await.unwrap().1;
    assert_eq!(payload[0], 0xff);
    assert_eq!(
        u16::from_le_bytes([payload[1], payload[2]]),
        ErrorKind::ER_ACCESS_DENIED_NO_PASSWORD_ERROR as u16
    );
    assert_eq!(
        server.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
}

#[tokio::test]
async fn caching_sha2_empty_auth_response_sends_ok_without_auth_more_data() {
    let (mut client, server) = start_wire_server(WireShim {
        auth_plugin: opensrv_mysql::CACHING_SHA2_PASSWORD,
    })
    .await;

    let (seq, _) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 0);
    write_wire_packet(
        &mut client,
        1,
        &handshake_response(opensrv_mysql::CACHING_SHA2_PASSWORD, None),
    )
    .await
    .unwrap();

    let (seq, payload) = timeout(Duration::from_millis(250), read_wire_packet(&mut client))
        .await
        .expect("empty password authentication response")
        .unwrap();
    assert_eq!(seq, 2);
    assert_eq!(payload[0], 0x00);

    write_wire_packet(&mut client, 0, b"\x01").await.unwrap();
    drop(client);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn caching_sha2_rejects_nonempty_response_without_digest_length() {
    let (mut client, server) = start_wire_server(WireShim {
        auth_plugin: opensrv_mysql::CACHING_SHA2_PASSWORD,
    })
    .await;

    read_wire_packet(&mut client).await.unwrap();
    write_wire_packet(
        &mut client,
        1,
        &handshake_response_with_auth(opensrv_mysql::CACHING_SHA2_PASSWORD, None, &[0x42]),
    )
    .await
    .unwrap();

    let (seq, payload) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 2);
    assert_eq!(payload[0], 0xff);
    assert_eq!(
        u16::from_le_bytes([payload[1], payload[2]]),
        ErrorKind::ER_ACCESS_DENIED_NO_PASSWORD_ERROR as u16
    );

    let err = server.await.unwrap().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
}

async fn prepare_wire_statement(client: &mut TcpStream) {
    write_wire_packet(client, 0, b"\x16SELECT ?").await.unwrap();
    let (seq, payload) = read_wire_packet(client).await.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(payload[0], 0x00);
    let (seq, _) = read_wire_packet(client).await.unwrap();
    assert_eq!(seq, 2);
    let (seq, payload) = read_wire_packet(client).await.unwrap();
    assert_eq!(seq, 3);
    assert_eq!(payload, [0xfe, 0, 0, 0, 0]);
}

#[tokio::test]
async fn command_phase_rejects_nonzero_initial_sequence() {
    let (mut client, server) = start_wire_server(WireShim {
        auth_plugin: opensrv_mysql::MYSQL_NATIVE_PASSWORD,
    })
    .await;

    read_wire_packet(&mut client).await.unwrap();
    write_wire_packet(
        &mut client,
        1,
        &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
    )
    .await
    .unwrap();
    read_wire_packet(&mut client).await.unwrap();

    write_wire_packet(&mut client, 1, b"\x0e").await.unwrap();
    let (seq, payload) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 2);
    assert_eq!(payload[0], 0xff);
    assert_eq!(
        u16::from_le_bytes([payload[1], payload[2]]),
        ErrorKind::ER_MALFORMED_PACKET as u16
    );

    let err = server.await.unwrap().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn execute_rejects_cursor_flags_and_keeps_connection_open() {
    let (mut client, server) = start_wire_server(WireShim {
        auth_plugin: opensrv_mysql::MYSQL_NATIVE_PASSWORD,
    })
    .await;
    read_wire_packet(&mut client).await.unwrap();
    write_wire_packet(
        &mut client,
        1,
        &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
    )
    .await
    .unwrap();
    read_wire_packet(&mut client).await.unwrap();
    prepare_wire_statement(&mut client).await;

    write_wire_packet(&mut client, 0, b"\x17\x2a\0\0\0\x01\x01\0\0\0")
        .await
        .unwrap();
    assert_execute_error(&mut client, ErrorKind::ER_UNSUPPORTED_PS).await;

    write_wire_packet(&mut client, 0, b"\x0e").await.unwrap();
    assert_eq!(read_wire_packet(&mut client).await.unwrap().1[0], 0);
    write_wire_packet(&mut client, 0, b"\x01").await.unwrap();
    drop(client);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn prepared_statement_count_limit_is_enforced() {
    let (mut client, server) = start_wire_server_with_options(
        WireShim {
            auth_plugin: opensrv_mysql::MYSQL_NATIVE_PASSWORD,
        },
        IntermediaryOptions {
            max_prepared_statements: Some(1),
            ..Default::default()
        },
    )
    .await;
    read_wire_packet(&mut client).await.unwrap();
    write_wire_packet(
        &mut client,
        1,
        &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
    )
    .await
    .unwrap();
    read_wire_packet(&mut client).await.unwrap();
    prepare_wire_statement(&mut client).await;

    write_wire_packet(&mut client, 0, b"\x16SELECT ?")
        .await
        .unwrap();
    assert_execute_error(&mut client, ErrorKind::ER_MAX_PREPARED_STMT_COUNT_REACHED).await;

    write_wire_packet(&mut client, 0, b"\x01").await.unwrap();
    drop(client);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn aggregate_long_data_limit_spans_all_statements() {
    let (mut client, server) = start_wire_server_with_options(
        MultiStatementShim { next_id: 0 },
        IntermediaryOptions {
            max_packet_size: Some(128),
            max_connection_long_data_size: Some(10),
            ..Default::default()
        },
    )
    .await;
    read_wire_packet(&mut client).await.unwrap();
    write_wire_packet(
        &mut client,
        1,
        &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
    )
    .await
    .unwrap();
    read_wire_packet(&mut client).await.unwrap();
    prepare_wire_statement(&mut client).await;
    prepare_wire_statement(&mut client).await;

    write_wire_packet(&mut client, 0, b"\x18\x01\0\0\0\0\0abcdef")
        .await
        .unwrap();
    write_wire_packet(&mut client, 0, b"\x18\x02\0\0\0\0\0ghijkl")
        .await
        .unwrap();
    write_wire_packet(&mut client, 0, b"\x17\x02\0\0\0\0\x01\0\0\0\x01\x01\xfc\0")
        .await
        .unwrap();
    assert_execute_error(&mut client, ErrorKind::ER_NET_PACKET_TOO_LARGE).await;

    write_wire_packet(&mut client, 0, b"\x01").await.unwrap();
    drop(client);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn field_list_empty_response_is_eof_even_with_deprecate_eof() {
    let (mut client, server) = start_wire_server(WireShim {
        auth_plugin: opensrv_mysql::MYSQL_NATIVE_PASSWORD,
    })
    .await;

    read_wire_packet(&mut client).await.unwrap();
    let mut response = handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None);
    let capabilities = u32::from_le_bytes(response[..4].try_into().unwrap())
        | myc::constants::CapabilityFlags::CLIENT_DEPRECATE_EOF.bits();
    response[..4].copy_from_slice(&capabilities.to_le_bytes());
    write_wire_packet(&mut client, 1, &response).await.unwrap();
    read_wire_packet(&mut client).await.unwrap();

    write_wire_packet(&mut client, 0, b"\x04table\0")
        .await
        .unwrap();
    let (seq, payload) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(payload, [0xfe, 0, 0, 0, 0]);

    write_wire_packet(&mut client, 0, b"\x01").await.unwrap();
    drop(client);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn send_long_data_invalid_parameter_is_reported_by_execute_until_reset() {
    let (mut client, server) = start_wire_server(WireShim {
        auth_plugin: opensrv_mysql::MYSQL_NATIVE_PASSWORD,
    })
    .await;

    read_wire_packet(&mut client).await.unwrap();
    write_wire_packet(
        &mut client,
        1,
        &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
    )
    .await
    .unwrap();
    read_wire_packet(&mut client).await.unwrap();
    prepare_wire_statement(&mut client).await;

    write_wire_packet(&mut client, 0, b"\x18\x2a\0\0\0\x01\0data")
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(50), read_wire_packet(&mut client))
            .await
            .is_err()
    );

    execute_wire_statement(&mut client).await;
    assert_execute_error(&mut client, ErrorKind::ER_WRONG_ARGUMENTS).await;
    execute_wire_statement(&mut client).await;
    assert_execute_error(&mut client, ErrorKind::ER_WRONG_ARGUMENTS).await;

    write_wire_packet(&mut client, 0, b"\x1a\x2a\0\0\0")
        .await
        .unwrap();
    let (seq, payload) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(payload[0], 0x00);

    execute_wire_statement(&mut client).await;
    let (seq, payload) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(payload[0], 0x00);

    write_wire_packet(&mut client, 0, b"\x0e").await.unwrap();
    let (seq, payload) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(payload[0], 0x00);

    write_wire_packet(&mut client, 0, b"\x01").await.unwrap();
    drop(client);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn send_long_data_over_limit_is_reported_by_execute_and_keeps_connection_open() {
    let (mut client, server) = start_wire_server_with_options(
        WireShim {
            auth_plugin: opensrv_mysql::MYSQL_NATIVE_PASSWORD,
        },
        IntermediaryOptions {
            max_packet_size: Some(128),
            ..Default::default()
        },
    )
    .await;

    read_wire_packet(&mut client).await.unwrap();
    write_wire_packet(
        &mut client,
        1,
        &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
    )
    .await
    .unwrap();
    read_wire_packet(&mut client).await.unwrap();
    prepare_wire_statement(&mut client).await;

    let mut payload = b"\x18\x2a\0\0\0\0\0".to_vec();
    payload.extend_from_slice(&[0; 60]);
    write_wire_packet(&mut client, 0, &payload).await.unwrap();
    write_wire_packet(&mut client, 0, &payload).await.unwrap();
    let mut overflowing = b"\x18\x2a\0\0\0\0\0".to_vec();
    overflowing.extend_from_slice(&[0; 9]);
    write_wire_packet(&mut client, 0, &overflowing)
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(50), read_wire_packet(&mut client))
            .await
            .is_err()
    );

    execute_wire_statement(&mut client).await;
    assert_execute_error(&mut client, ErrorKind::ER_NET_PACKET_TOO_LARGE).await;

    write_wire_packet(&mut client, 0, b"\x0e").await.unwrap();
    let (seq, payload) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(payload[0], 0x00);

    write_wire_packet(&mut client, 0, b"\x01").await.unwrap();
    drop(client);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn send_long_data_unknown_statement_is_silent_and_connection_stays_open() {
    let (mut client, server) = start_wire_server(WireShim {
        auth_plugin: opensrv_mysql::MYSQL_NATIVE_PASSWORD,
    })
    .await;

    read_wire_packet(&mut client).await.unwrap();
    write_wire_packet(
        &mut client,
        1,
        &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
    )
    .await
    .unwrap();
    read_wire_packet(&mut client).await.unwrap();

    write_wire_packet(&mut client, 0, b"\x18\x99\0\0\0\0\0data")
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(50), read_wire_packet(&mut client))
            .await
            .is_err()
    );

    write_wire_packet(&mut client, 0, b"\x0e").await.unwrap();
    let (seq, payload) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(payload[0], 0x00);

    write_wire_packet(&mut client, 0, b"\x1a\x99\0\0\0")
        .await
        .unwrap();
    assert_execute_error(&mut client, ErrorKind::ER_UNKNOWN_STMT_HANDLER).await;

    write_wire_packet(&mut client, 0, b"\x01").await.unwrap();
    drop(client);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn execute_unknown_statement_returns_error_and_keeps_connection_open() {
    let (mut client, server) = start_wire_server(WireShim {
        auth_plugin: opensrv_mysql::MYSQL_NATIVE_PASSWORD,
    })
    .await;

    read_wire_packet(&mut client).await.unwrap();
    write_wire_packet(
        &mut client,
        1,
        &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
    )
    .await
    .unwrap();
    read_wire_packet(&mut client).await.unwrap();

    // COM_STMT_EXECUTE (0x17) for statement 999: stmt(4), flags(1), iterations(4)
    write_wire_packet(&mut client, 0, b"\x17\xe7\x03\0\0\0\x01\0\0\0")
        .await
        .unwrap();
    assert_execute_error(&mut client, ErrorKind::ER_UNKNOWN_STMT_HANDLER).await;

    // Subsequent ping succeeds, verifying connection is alive
    write_wire_packet(&mut client, 0, b"\x0e").await.unwrap();
    let (seq, payload) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(payload[0], 0x00);

    write_wire_packet(&mut client, 0, b"\x01").await.unwrap();
    drop(client);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn query_non_utf8_returns_malformed_packet_and_keeps_connection_open() {
    let (mut client, server) = start_wire_server(WireShim {
        auth_plugin: opensrv_mysql::MYSQL_NATIVE_PASSWORD,
    })
    .await;

    read_wire_packet(&mut client).await.unwrap();
    write_wire_packet(
        &mut client,
        1,
        &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
    )
    .await
    .unwrap();
    read_wire_packet(&mut client).await.unwrap();

    // COM_QUERY (0x03) with invalid UTF-8 bytes: 0xff, 0xfe
    write_wire_packet(&mut client, 0, b"\x03SELECT \xff\xfe")
        .await
        .unwrap();
    let (seq, payload) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(payload[0], 0xff);
    assert_eq!(
        u16::from_le_bytes([payload[1], payload[2]]),
        ErrorKind::ER_MALFORMED_PACKET as u16
    );

    // Subsequent ping succeeds, verifying connection is alive
    write_wire_packet(&mut client, 0, b"\x0e").await.unwrap();
    let (seq, payload) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(payload[0], 0x00);

    write_wire_packet(&mut client, 0, b"\x01").await.unwrap();
    drop(client);
    server.await.unwrap().unwrap();
}

struct ParamRecordingShim {
    recorded: Arc<Mutex<Vec<Vec<u8>>>>,
}

#[async_trait]
impl AsyncMysqlShim<BufWriter<OwnedWriteHalf>> for ParamRecordingShim {
    type Error = io::Error;

    fn default_auth_plugin(&self) -> &str {
        opensrv_mysql::MYSQL_NATIVE_PASSWORD
    }

    async fn auth_plugin_for_username(&self, _user: &[u8]) -> &'static str {
        opensrv_mysql::MYSQL_NATIVE_PASSWORD
    }

    async fn authenticate(
        &self,
        _auth_plugin: &str,
        _username: &[u8],
        _salt: &[u8],
        _auth_data: &[u8],
    ) -> bool {
        true
    }

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        let params = [Column {
            table: String::new(),
            column: "param".to_string(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING,
            colflags: myc::constants::ColumnFlags::empty(),
        }];
        info.reply(42, &params, &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        for p in params {
            if let ValueInner::Bytes(b) = p.value.into_inner() {
                self.recorded.lock().unwrap().push(b.to_vec());
            }
        }
        results.completed(OkResponse::default()).await
    }

    async fn on_close<'a>(&'a mut self, _stmt: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        _query: &'a str,
        _results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "query unsupported",
        ))
    }
}

#[tokio::test]
async fn incompatible_long_data_returns_wrong_arguments_and_recovers() {
    for ty in [3u8, 10] {
        for null in [0u8, 1] {
            let recorded = Arc::new(Mutex::new(Vec::new()));
            let (mut client, server) = start_wire_server(ParamRecordingShim {
                recorded: Arc::clone(&recorded),
            })
            .await;
            read_wire_packet(&mut client).await.unwrap();
            write_wire_packet(
                &mut client,
                1,
                &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
            )
            .await
            .unwrap();
            assert_eq!(read_wire_packet(&mut client).await.unwrap().1[0], 0);
            prepare_wire_statement(&mut client).await;
            write_wire_packet(&mut client, 0, b"\x18\x2a\0\0\0\0\0old")
                .await
                .unwrap();
            let mut execute = b"\x17\x2a\0\0\0\0\x01\0\0\0".to_vec();
            execute.extend_from_slice(&[null, 1, ty, 0]);
            write_wire_packet(&mut client, 0, &execute).await.unwrap();
            let (seq, err) = read_wire_packet(&mut client).await.unwrap();
            assert_eq!(seq, 1);
            assert_eq!(err[0], 0xff);
            assert_eq!(
                u16::from_le_bytes([err[1], err[2]]),
                ErrorKind::ER_WRONG_ARGUMENTS as u16
            );
            assert_eq!(&err[3..9], b"#HY000");
            assert!(recorded.lock().unwrap().is_empty());
            write_wire_packet(&mut client, 0, &[0x0e]).await.unwrap();
            let (seq, ok) = read_wire_packet(&mut client).await.unwrap();
            assert_eq!((seq, ok[0]), (1, 0));
            write_wire_packet(
                &mut client,
                0,
                b"\x17\x2a\0\0\0\0\x01\0\0\0\0\x01\xfd\0\x03new",
            )
            .await
            .unwrap();
            let (seq, ok) = read_wire_packet(&mut client).await.unwrap();
            assert_eq!((seq, ok[0]), (1, 0));
            assert_eq!(*recorded.lock().unwrap(), vec![b"new".to_vec()]);
            write_wire_packet(&mut client, 0, &[1]).await.unwrap();
            timeout(Duration::from_secs(2), server)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }
}

#[tokio::test]
async fn send_long_data_cleared_on_execute_param_error_and_retry_uses_new_params() {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let (mut client, server) = start_wire_server(ParamRecordingShim {
        recorded: Arc::clone(&recorded),
    })
    .await;

    read_wire_packet(&mut client).await.unwrap();
    write_wire_packet(
        &mut client,
        1,
        &handshake_response(opensrv_mysql::MYSQL_NATIVE_PASSWORD, None),
    )
    .await
    .unwrap();
    read_wire_packet(&mut client).await.unwrap();

    // 1. Prepare statement 42
    prepare_wire_statement(&mut client).await;

    // 2. Upload long data "old" for stmt 42, param 0
    write_wire_packet(&mut client, 0, b"\x18\x2a\0\0\0\0\0old")
        .await
        .unwrap();

    // 3. Send malformed EXECUTE (truncated type map: new_params_bound = 1, but only 1 byte of type map)
    // stmt 42 (\x2a\0\0\0), flags 0, iterations 1, nullmap [0], new_params_bound 1, type map truncated (\xfe)
    write_wire_packet(&mut client, 0, b"\x17\x2a\0\0\0\0\x01\0\0\0\0\x01\xfe")
        .await
        .unwrap();
    let (seq, payload) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(payload[0], 0xff);
    assert_eq!(
        u16::from_le_bytes([payload[1], payload[2]]),
        ErrorKind::ER_MALFORMED_PACKET as u16
    );

    // 4. Retry with valid EXECUTE sending inline parameter "new"
    // stmt 42, flags 0, iterations 1, nullmap [0], new_params_bound 1, type map [MYSQL_TYPE_VAR_STRING (0xfe), 0x00], lenenc string "new" (\x03new)
    write_wire_packet(
        &mut client,
        0,
        b"\x17\x2a\0\0\0\0\x01\0\0\0\0\x01\xfe\0\x03new",
    )
    .await
    .unwrap();
    let (seq, payload) = read_wire_packet(&mut client).await.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(payload[0], 0x00);

    // 5. Assert recorded parameter is "new", and old long_data was cleared
    let values = recorded.lock().unwrap().clone();
    assert_eq!(values, vec![b"new".to_vec()]);

    write_wire_packet(&mut client, 0, b"\x01").await.unwrap();
    drop(client);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn empty_response() {
    TestingShim::new(
        |_, w| w.completed(OkResponse::default()).boxed(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn no_rows() {
    TestingShim::new(
        move |_, w| {
            async move {
                let cols = [Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: myc::constants::ColumnFlags::empty(),
                }];
                w.start(&cols[..]).await?.finish().await
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn no_columns() {
    TestingShim::new(
        move |_, w| async { w.start(&[]).await?.finish().await }.boxed(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn no_columns_but_rows() {
    TestingShim::new(
        move |_, w| {
            async {
                let mut row_writer = w.start(&[]).await?;
                row_writer.write_col(42)?;
                row_writer.finish().await
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn really_long_query() {
    let long = "CREATE TABLE `stories` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `always_null` int, `created_at` datetime, `user_id` int unsigned, `url` varchar(250) DEFAULT '', `title` varchar(150) DEFAULT '' NOT NULL, `description` mediumtext, `short_id` varchar(6) DEFAULT '' NOT NULL, `is_expired` tinyint(1) DEFAULT 0 NOT NULL, `is_moderated` tinyint(1) DEFAULT 0 NOT NULL, `markeddown_description` mediumtext, `story_cache` mediumtext, `merged_story_id` int, `unavailable_at` datetime, `twitter_id` varchar(20), `user_is_author` tinyint(1) DEFAULT 0,  INDEX `index_stories_on_created_at`  (`created_at`), fulltext INDEX `index_stories_on_description`  (`description`),   INDEX `is_idxes`  (`is_expired`, `is_moderated`),  INDEX `index_stories_on_is_expired`  (`is_expired`),  INDEX `index_stories_on_is_moderated`  (`is_moderated`),  INDEX `index_stories_on_merged_story_id`  (`merged_story_id`), UNIQUE INDEX `unique_short_id`  (`short_id`), fulltext INDEX `index_stories_on_story_cache`  (`story_cache`), fulltext INDEX `index_stories_on_title`  (`title`),  INDEX `index_stories_on_twitter_id`  (`twitter_id`),  INDEX `url`  (`url`(191)),  INDEX `index_stories_on_user_id`  (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
    TestingShim::new(
        move |q, w| {
            async move {
                assert_eq!(q, long);
                let mut row_writer = w.start(&[]).await?;
                row_writer.write_col(42).map(|_| ())?;
                row_writer.finish().await
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(move |mut db| async move {
        db.query_drop(long).await?;
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn error_response() {
    let err = (ErrorKind::ER_NO, "clearly not".to_string());
    let err_clone = (ErrorKind::ER_NO, "clearly not".to_string());
    TestingShim::new(
        move |_, w| {
            let message = err_clone.1.clone();
            let kind = err_clone.0;
            async move { w.error(kind, message.as_bytes()).await }.boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(move |mut db| async move {
        let res: Result<Vec<mysql_async::Row>, _> = db.query("SELECT a, b FROM foo").await;
        match res {
            Ok(_) => panic!(),
            Err(mysql_async::Error::Server(mysql_async::ServerError {
                code,
                message: ref msg,
                ref state,
            })) => {
                assert_eq!(
                    state,
                    &String::from_utf8(err.0.sqlstate().to_vec()).unwrap()
                );
                assert_eq!(code, err.0 as u16);
                assert_eq!(msg, &err.1);
            }
            Err(e) => {
                eprintln!("unexpected {:?}", e);
                panic!();
            }
        }
        Ok(())
    })
    .await;
}

#[tokio::test]
// TODO rename this case, row_writer must be used!
async fn empty_on_drop() {
    TestingShim::new(
        move |_, w| {
            async move {
                let cols = [Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: myc::constants::ColumnFlags::empty(),
                }];
                let row_writer = w.start(&cols[..]).await?;
                row_writer.finish().await
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn it_queries_nulls() {
    TestingShim::new(
        |_, w| {
            async move {
                let cols = &[Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: myc::constants::ColumnFlags::empty(),
                }];
                let mut w = w.start(cols).await?;
                w.write_col(None::<i16>)?;
                w.finish().await
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].len(), 1);
        assert_eq!(rs[0][0], mysql_async::Value::NULL);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn it_queries() {
    TestingShim::new(
        |_, w| {
            async move {
                let cols = &[Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: myc::constants::ColumnFlags::empty(),
                }];
                let mut w = w.start(cols).await?;
                w.write_col(1024i16)?;
                w.finish().await
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].len(), 1);
        assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn it_queries_many_rows() {
    TestingShim::new(
        |_, w| {
            async move {
                let cols = &[
                    Column {
                        table: String::new(),
                        column: "a".to_owned(),
                        collen: 0,
                        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                        colflags: myc::constants::ColumnFlags::empty(),
                    },
                    Column {
                        table: String::new(),
                        column: "b".to_owned(),
                        collen: 0,
                        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                        colflags: myc::constants::ColumnFlags::empty(),
                    },
                ];
                let mut w = w.start(cols).await?;
                w.write_col(1024i16)?;
                w.write_col(1025i16)?;
                w.end_row().await?;
                w.write_row(&[1024i16, 1025i16]).await?;
                w.finish().await
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 2);
        assert_eq!(rs[0].len(), 2);
        assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
        assert_eq!(rs[0].get::<i16, _>(1), Some(1025));
        assert_eq!(rs[1].len(), 2);
        assert_eq!(rs[1].get::<i16, _>(0), Some(1024));
        assert_eq!(rs[1].get::<i16, _>(1), Some(1025));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn it_prepares() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];

    TestingShim::new(
        |_, _| unreachable!(),
        |q| {
            assert_eq!(q, "SELECT a FROM b WHERE c = ?");
            41
        },
        move |stmt, params, w| {
            let cols3 = cols.clone();
            async move {
                assert_eq!(stmt, 41);
                assert_eq!(params.len(), 1);
                // rust-mysql sends all numbers as LONGLONG
                assert_eq!(
                    params[0].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_LONGLONG
                );
                assert_eq!(<i8>::try_from(params[0].value).unwrap(), 42i8);

                let mut w = w.start(&cols3).await?;
                w.write_col(1024i16)?;
                w.finish().await
            }
            .boxed()
        },
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|mut db| async move {
        let prep = db.prep("SELECT a FROM b WHERE c = ?").await?;
        let rs: Vec<mysql_async::Row> = db.exec(prep, (42i16,)).await?;
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].len(), 1);
        assert_eq!(rs[0].get::<i16, _>(0), Some(1024));

        Ok(())
    })
    .await;
}

#[tokio::test]
async fn insert_exec() {
    let params = vec![
        Column {
            table: String::new(),
            column: "username".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "email".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "pw".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "created".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_DATETIME,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "session".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "rss".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "mail".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 1,
        move |_, params, w| {
            async move {
                assert_eq!(params.len(), 7);
                assert_eq!(
                    params[0].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
                );
                assert_eq!(
                    params[1].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
                );
                assert_eq!(
                    params[2].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
                );
                assert_eq!(
                    params[3].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_DATETIME
                );
                assert_eq!(
                    params[4].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
                );
                assert_eq!(
                    params[5].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
                );
                assert_eq!(
                    params[6].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
                );
                assert_eq!(<&str>::try_from(params[0].value).unwrap(), "user199");
                assert_eq!(
                    <&str>::try_from(params[1].value).unwrap(),
                    "user199@example.com"
                );
                assert_eq!(
                    <&str>::try_from(params[2].value).unwrap(),
                    "$2a$10$Tq3wrGeC0xtgzuxqOlc3v.07VTUvxvwI70kuoVihoO2cE5qj7ooka"
                );
                assert_eq!(
                    <chrono::NaiveDateTime>::try_from(params[3].value).unwrap(),
                    chrono::NaiveDate::from_ymd_opt(2018, 4, 6)
                        .unwrap()
                        .and_hms_opt(13, 0, 56)
                        .unwrap()
                );
                assert_eq!(<&str>::try_from(params[4].value).unwrap(), "token199");
                assert_eq!(<&str>::try_from(params[5].value).unwrap(), "rsstoken199");
                assert_eq!(<&str>::try_from(params[6].value).unwrap(), "mtok199");

                let info = OkResponse {
                    affected_rows: 42,
                    last_insert_id: 1,
                    ..Default::default()
                };
                w.completed(info).await
            }
            .boxed()
        },
    )
    .with_params(params)
    .test(|mut db| async move {
        let prep = db
            .prep(
                "INSERT INTO `users` \
        (`username`, `email`, `password_digest`, `created_at`, \
        `session_token`, `rss_token`, `mailing_list_token`) \
        VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .await?;

        let _res: Vec<mysql_async::Row> = db
            .exec(
                prep,
                (
                    "user199",
                    "user199@example.com",
                    "$2a$10$Tq3wrGeC0xtgzuxqOlc3v.07VTUvxvwI70kuoVihoO2cE5qj7ooka",
                    mysql_async::Value::Date(2018, 4, 6, 13, 0, 56, 0),
                    "token199",
                    "rsstoken199",
                    "mtok199",
                ),
            )
            .await?;

        assert_eq!(db.affected_rows(), 42);
        assert_eq!(db.last_insert_id(), Some(1));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn send_long() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_BLOB,
        colflags: myc::constants::ColumnFlags::empty(),
    }];

    TestingShim::new(
        |_, _| unreachable!(),
        |q| {
            assert_eq!(q, "SELECT a FROM b WHERE c = ?");
            41
        },
        move |stmt, params, w| {
            let cols = cols.clone();
            async move {
                assert_eq!(stmt, 41);
                assert_eq!(params.len(), 1);
                // rust-mysql sends all strings as VAR_STRING
                assert_eq!(
                    params[0].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
                );
                assert_eq!(<&[u8]>::try_from(params[0].value).unwrap(), b"Hello world");

                let mut w = w.start(&cols).await?;
                w.write_col(1024i16)?;
                w.finish().await
            }
            .boxed()
        },
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|mut db| async move {
        let prep = db.prep("SELECT a FROM b WHERE c = ?").await?;
        let rs: Vec<mysql_async::Row> = db.exec(prep, (b"Hello world",)).await?;

        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].len(), 1);
        assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn it_prepares_many() {
    let cols = vec![
        Column {
            table: String::new(),
            column: "a".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "b".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];
    let cols2 = cols.clone();

    TestingShim::new(
        |_, _| unreachable!(),
        |q| {
            assert_eq!(q, "SELECT a, b FROM x");
            41
        },
        move |stmt, params, w| {
            let cols = cols.clone();
            async move {
                assert_eq!(stmt, 41);
                assert_eq!(params.len(), 0);

                let mut w = w.start(&cols).await?;
                w.write_col(1024i16)?;
                w.write_col(1025i16)?;
                w.end_row().await?;
                w.write_row(&[1024i16, 1025i16]).await?;
                w.finish().await
            }
            .boxed()
        },
    )
    .with_params(Vec::new())
    .with_columns(cols2)
    .test(|mut db| async move {
        let prep = db.prep("SELECT a, b FROM x").await?;
        let rs: Vec<mysql_async::Row> = db.exec(prep, ()).await?;
        assert_eq!(rs.len(), 2);
        assert_eq!(rs[0].len(), 2);
        assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
        assert_eq!(rs[0].get::<i16, _>(1), Some(1025));
        assert_eq!(rs[1].len(), 2);
        assert_eq!(rs[1].get::<i16, _>(0), Some(1024));
        assert_eq!(rs[1].get::<i16, _>(1), Some(1025));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn prepared_empty() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, params, w| {
            async move {
                assert!(!params.is_empty());
                w.completed(OkResponse::default()).await
            }
            .boxed()
        },
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|mut db| async move {
        let prep = db.prep("SELECT a FROM b WHERE c = ?").await.unwrap();
        let rs: Vec<mysql_async::Row> = db.exec(prep, (42i16,)).await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn prepared_no_params() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, params, w| {
            let cols = cols.clone();
            async move {
                assert!(params.is_empty());
                let mut w = w.start(&cols).await?;
                w.write_col(1024i16)?;
                w.finish().await
            }
            .boxed()
        },
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|mut db| async move {
        let prep = db.prep("foo").await.unwrap();
        let rs: Vec<mysql_async::Row> = db.exec(prep, ()).await?;
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].len(), 1);
        assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn prepared_nulls() {
    let cols = vec![
        Column {
            table: String::new(),
            column: "a".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "b".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];
    let cols2 = cols.clone();
    let params = vec![
        Column {
            table: String::new(),
            column: "c".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "d".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, params, w| {
            let cols = cols.clone();
            async move {
                assert_eq!(params.len(), 2);
                assert!(params[0].value.is_null());
                assert!(!params[1].value.is_null());
                assert_eq!(
                    params[0].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_NULL
                );
                // rust-mysql sends all numbers as LONGLONG :'(
                assert_eq!(
                    params[1].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_LONGLONG
                );
                assert_eq!(<i8>::try_from(params[1].value).unwrap(), 42i8);

                let mut w = w.start(&cols).await?;
                w.write_row(vec![None::<i16>, Some(42)]).await?;
                w.finish().await
            }
            .boxed()
        },
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|mut db| async move {
        let prep = db.prep("SELECT a, b FROM x WHERE c = ? AND d = ?").await?;
        let rs: Vec<mysql_async::Row> = db.exec(prep, (mysql_async::Value::NULL, 42)).await?;
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].len(), 2);
        assert_eq!(rs[0].get::<Option<i16>, _>(0), Some(None));
        assert_eq!(rs[0].get::<i16, _>(1), Some(42));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn prepared_no_rows() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| {
            let cols = cols.clone();
            async move { w.start(&cols[..]).await?.finish().await }.boxed()
        },
    )
    .with_columns(cols2)
    .test(|mut db| async move {
        let prep = db.prep("SELECT a, b FROM foo").await?;
        let rs: Vec<mysql_async::Row> = db.exec(prep, ()).await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn prepared_no_cols_but_rows() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| {
            async move {
                let mut row_writer = w.start(&[]).await?;
                row_writer.write_col(42)?;
                row_writer.finish().await
            }
            .boxed()
        },
    )
    .test(|mut db| async move {
        let prep = db.prep("SELECT a, b FROM foo").await?;
        let rs: Vec<mysql_async::Row> = db.exec(prep, ()).await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn prepared_no_cols() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| async move { w.start(&[]).await?.finish().await }.boxed(),
    )
    .test(|mut db| async move {
        let prep = db.prep("SELECT a, b FROM foo").await?;
        let rs: Vec<mysql_async::Row> = db.exec(prep, ()).await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn large_packet() {
    TestingShim::new(
        move |_, w| {
            async move {
                let cols = vec![Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_BLOB,
                    colflags: myc::constants::ColumnFlags::empty(),
                }];
                let mut row_writer = w.start(&cols).await?;
                let blob_col = vec![0; U24_MAX + 1];
                row_writer.write_row(vec![blob_col.clone()]).await?;
                row_writer.write_row(vec![blob_col]).await?;
                let row_writer = row_writer.finish_one().await?;
                row_writer.no_more_results().await
            }
            .boxed()
        },
        |_| 0,
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let mut result = db.query_iter("SELECT a, b from foo").await?;

        // check row numbers and packet size
        let mut number_rows = 0;
        while let Some(mut row) = result.next().await? {
            number_rows += 1;
            let value: Vec<u8> = row.take(0).unwrap();
            assert_eq!(U24_MAX + 1, value.len());
        }
        assert_eq!(2, number_rows);

        result.drop_result().await?;
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn ok_packet_with_info_when_session_track_disabled() {
    let info = "Query finished in 0.007 sec.".repeat(12);
    TestingShim::new(
        move |query, w| {
            let info = info.clone();
            async move {
                match query.trim() {
                    "SELECT @@max_allowed_packet,@@wait_timeout,@@socket" => {
                        let cols = &[
                            Column {
                                table: String::new(),
                                column: "@@max_allowed_packet".to_owned(),
                                collen: 0,
                                coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                                colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                            },
                            Column {
                                table: String::new(),
                                column: "@@wait_timeout".to_owned(),
                                collen: 0,
                                coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                                colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                            },
                            Column {
                                table: String::new(),
                                column: "@@socket".to_owned(),
                                collen: 0,
                                coltype: myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING,
                                colflags: myc::constants::ColumnFlags::empty(),
                            },
                        ];

                        let mut row_writer = w.start(cols).await?;
                        row_writer.write_col(67108864u32)?;
                        row_writer.write_col(28800u32)?;
                        row_writer.write_col(None::<String>)?;
                        row_writer.end_row().await?;
                        row_writer.finish().await
                    }
                    "SELECT @@version_comment" => {
                        let cols = &[Column {
                            table: String::new(),
                            column: "@@version_comment".to_owned(),
                            collen: 0,
                            coltype: myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING,
                            colflags: myc::constants::ColumnFlags::empty(),
                        }];

                        let mut row_writer = w.start(cols).await?;
                        row_writer
                            .write_row(vec!["Databend (test)".to_string()])
                            .await?;
                        let query_writer = row_writer.finish_one_with_info(&info).await?;
                        query_writer.no_more_results().await
                    }
                    other => panic!("unexpected query: {other}"),
                }
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let version: Option<String> = db.query_first("SELECT @@version_comment").await?;
        assert_eq!(version.as_deref(), Some("Databend (test)"));
        Ok(())
    })
    .await;
}
