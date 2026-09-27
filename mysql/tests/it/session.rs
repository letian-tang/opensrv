use opensrv_mysql::*;
use std::io;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

struct Session;
fn columns() -> [Column; 1] {
    [Column {
        table: String::new(),
        column: "v".into(),
        collen: 0,
        coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
        colflags: ColumnFlags::empty(),
    }]
}
#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for Session {
    type Error = io::Error;
    async fn authenticate(&self, _: &str, _: &[u8], _: &[u8], _: &[u8]) -> bool {
        true
    }
    async fn on_prepare<'a>(
        &'a mut self,
        _: &'a str,
        w: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        w.reply(7, &[], &columns()).await
    }
    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: ParamParser<'a>,
        w: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        let cols = columns();
        let mut rows = w.start(&cols).await?;
        rows.write_row(["value"]).await?;
        rows.finish().await
    }
    async fn on_close(&mut self, _: u32) {}
    async fn on_reset_connection(&mut self) -> io::Result<bool> {
        Ok(true)
    }
    async fn on_init<'a>(&'a mut self, db: &'a str, w: InitWriter<'a, W>) -> io::Result<()> {
        if db == "clear" {
            w.ok_with_status(StatusFlags::empty()).await
        } else {
            w.ok().await
        }
    }
    async fn on_query<'a>(
        &'a mut self,
        q: &'a str,
        mut w: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        match q {
            "empty_finish" => w.no_more_results().await,
            "ignored_empty_finish" => {
                // Even a backend that discards the error must not bypass the
                // intermediary's incomplete-response guard.
                let _ = w.no_more_results().await;
                Ok(())
            }
            "off" | "commit" => w.completed_with_status(OkResponse::default()).await,
            "begin" => {
                w.completed(OkResponse {
                    status_flags: StatusFlags::SERVER_STATUS_IN_TRANS,
                    ..Default::default()
                })
                .await
            }
            "error" => w.error(ErrorKind::ER_UNKNOWN_ERROR, b"test error").await,
            "rollback_error" => {
                w.set_status_flags(StatusFlags::empty());
                w.error(ErrorKind::ER_LOCK_DEADLOCK, b"transaction rolled back")
                    .await
            }
            "select" => {
                let cols = columns();
                let mut rows = w.start(&cols).await?;
                rows.write_row(["value"]).await?;
                rows.finish().await
            }
            "row_begin" => {
                w.set_status_flags(StatusFlags::SERVER_STATUS_IN_TRANS);
                let cols = columns();
                let mut rows = w.start(&cols).await?;
                rows.set_status_flags(
                    StatusFlags::SERVER_STATUS_IN_TRANS | StatusFlags::SERVER_STATUS_NO_INDEX_USED,
                );
                rows.write_row(["value"]).await?;
                rows.finish().await
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn empty_finalization_closes_instead_of_waiting_for_another_command() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for query in ["empty_finish", "ignored_empty_finish"] {
            let (mut client, server) = tokio::io::duplex(8192);
            let task = tokio::spawn(async move {
                let (r, w) = tokio::io::split(server);
                AsyncMysqlIntermediary::run_on(Session, r, w).await
            });
            assert_eq!(read(&mut client).await.0, 0);
            let flags =
                CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_SECURE_CONNECTION;
            let mut hs = vec![0; 32];
            hs[..4].copy_from_slice(&flags.bits().to_le_bytes());
            hs[8] = 45;
            hs.extend_from_slice(b"user\0\0");
            write(&mut client, 1, &hs).await;
            assert_eq!(read(&mut client).await.1[0], 0);
            let mut command = vec![3];
            command.extend_from_slice(query.as_bytes());
            write(&mut client, 0, &command).await;
            let mut response = [0; 1];
            assert_eq!(client.read(&mut response).await.unwrap(), 0);
            assert_eq!(
                task.await.unwrap().unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
    })
    .await
    .unwrap();
}

async fn read(c: &mut tokio::io::DuplexStream) -> (u8, Vec<u8>) {
    let mut h = [0; 4];
    c.read_exact(&mut h).await.unwrap();
    let mut b = vec![0; u32::from_le_bytes([h[0], h[1], h[2], 0]) as usize];
    c.read_exact(&mut b).await.unwrap();
    (h[3], b)
}
async fn write(c: &mut tokio::io::DuplexStream, seq: u8, b: &[u8]) {
    c.write_all(&[b.len() as u8, 0, 0, seq]).await.unwrap();
    c.write_all(b).await.unwrap();
}
fn assert_status(b: &[u8], status: StatusFlags) {
    assert!(b[0] == 0 || b[0] == 0xfe, "not OK/EOF: {b:?}");
    assert_eq!(u16::from_le_bytes([b[3], b[4]]), status.bits());
}
async fn command(c: &mut tokio::io::DuplexStream, b: &[u8], status: StatusFlags) {
    write(c, 0, b).await;
    let (seq, b) = read(c).await;
    assert_eq!(seq, 1);
    assert_status(&b, status);
}
async fn result(
    c: &mut tokio::io::DuplexStream,
    b: &[u8],
    deprecate: bool,
    metadata_status: StatusFlags,
    final_status: StatusFlags,
) {
    write(c, 0, b).await;
    assert_eq!(read(c).await, (1, vec![1]));
    assert_eq!(read(c).await.0, 2); // column
    let mut seq = 3;
    if !deprecate {
        let (s, b) = read(c).await;
        assert_eq!(s, seq);
        assert_status(&b, metadata_status);
        seq += 1;
    }
    assert_eq!(read(c).await.0, seq); // row
    let (s, b) = read(c).await;
    assert_eq!(s, seq + 1);
    assert_status(&b, final_status);
}

#[tokio::test]
async fn session_status_survives_queries_and_auxiliary_commands() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for deprecate in [false, true] {
            let (mut client, server) = tokio::io::duplex(8192);
            let task = tokio::spawn(async move {
                let (r, w) = tokio::io::split(server);
                AsyncMysqlIntermediary::run_on(Session, r, w).await
            });
            assert_eq!(read(&mut client).await.0, 0);
            let mut flags =
                CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_SECURE_CONNECTION;
            flags.set(CapabilityFlags::CLIENT_DEPRECATE_EOF, deprecate);
            let mut hs = vec![0; 32];
            hs[..4].copy_from_slice(&flags.bits().to_le_bytes());
            hs[8] = 45;
            hs.extend_from_slice(b"user\0\0");
            write(&mut client, 1, &hs).await;
            let auto = StatusFlags::SERVER_STATUS_AUTOCOMMIT;
            let trans = StatusFlags::SERVER_STATUS_IN_TRANS;
            let zero = StatusFlags::empty();
            assert_status(&read(&mut client).await.1, auto);
            command(&mut client, b"\x03off", zero).await;
            command(&mut client, b"\x0e", zero).await;
            command(&mut client, b"\x03begin", trans).await;
            result(&mut client, b"\x03select", deprecate, trans, trans).await;
            write(&mut client, 0, b"\x16select").await;
            assert_eq!(read(&mut client).await.1[0], 0); // PREPARE_OK
            assert_eq!(read(&mut client).await.0, 2);
            if !deprecate {
                assert_status(&read(&mut client).await.1, trans);
            }
            result(
                &mut client,
                &[0x17, 7, 0, 0, 0, 0, 1, 0, 0, 0],
                deprecate,
                trans,
                trans,
            )
            .await;
            command(&mut client, &[0x1a, 7, 0, 0, 0], trans).await;
            command(&mut client, b"\x02db", trans).await;
            command(&mut client, b"\x04table\0", trans).await;
            write(&mut client, 0, b"\x03error").await;
            assert_eq!(read(&mut client).await.1[0], 0xff);
            command(&mut client, b"\x0e", trans).await;
            command(&mut client, b"\x03commit", zero).await;
            command(&mut client, b"\x0e", zero).await;
            result(
                &mut client,
                b"\x03row_begin",
                deprecate,
                trans,
                trans | StatusFlags::SERVER_STATUS_NO_INDEX_USED,
            )
            .await;
            command(&mut client, b"\x0e", trans).await; // no query-local flag leakage
            write(&mut client, 0, b"\x03rollback_error").await;
            assert_eq!(read(&mut client).await.1[0], 0xff);
            command(&mut client, b"\x0e", zero).await;
            command(&mut client, b"\x03begin", trans).await;
            command(&mut client, b"\x02clear", zero).await;
            command(&mut client, b"\x0e", zero).await;
            command(&mut client, b"\x03begin", trans).await;
            command(&mut client, b"\x1f", auto).await;
            command(&mut client, b"\x0e", auto).await;
            write(&mut client, 0, &[1]).await;
            task.await.unwrap().unwrap();
        }
    })
    .await
    .unwrap();
}
