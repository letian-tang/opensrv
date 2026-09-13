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

//! After running this, you should be able to run:
//!
//! ```console
//! $ echo "SELECT * FROM foo" | mysql -h 127.0.0.1 --table --ssl-mode=REQUIRED
//! ```

#[cfg(feature = "tls")]
mod tls {

    use rustls_pemfile::{certs, pkcs8_private_keys};
    use rustls_pki_types::{CertificateDer, PrivateKeyDer};
    use std::{
        fs::File,
        io::{self, BufReader, ErrorKind},
        sync::Arc,
    };
    use tokio::io::AsyncWrite;
    use tokio_rustls::rustls::ServerConfig;

    use opensrv_mysql::*;
    use tokio::net::TcpListener;

    struct Backend;

    #[async_trait::async_trait]
    impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for Backend {
        type Error = io::Error;

        async fn authenticate(&self, _: &str, _: &[u8], _: &[u8], _: &[u8]) -> bool {
            true
        }

        async fn on_prepare<'a>(
            &'a mut self,
            _: &'a str,
            info: StatementMetaWriter<'a, W>,
        ) -> io::Result<()> {
            info.reply(42, &[], &[]).await
        }

        async fn on_execute<'a>(
            &'a mut self,
            _: u32,
            _: opensrv_mysql::ParamParser<'a>,
            results: QueryResultWriter<'a, W>,
        ) -> io::Result<()> {
            results.completed(OkResponse::default()).await
        }

        async fn on_close(&mut self, _: u32) {}

        async fn on_query<'a>(
            &'a mut self,
            sql: &'a str,
            results: QueryResultWriter<'a, W>,
        ) -> io::Result<()> {
            println!("execute sql {:?}", sql);
            results.start(&[]).await?.finish().await
        }
    }

    fn setup_tls() -> Result<ServerConfig, io::Error> {
        let cert = certs(&mut BufReader::new(File::open("tests/ssl/server.crt")?))
            .collect::<Result<Vec<CertificateDer>, io::Error>>()?;

        let key = pkcs8_private_keys(&mut BufReader::new(File::open("tests/ssl/server.key")?))
            .map(|key| key.map(PrivateKeyDer::from))
            .collect::<Result<Vec<PrivateKeyDer>, io::Error>>()?
            .remove(0);

        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert, key)
            .map_err(|err| io::Error::new(ErrorKind::InvalidInput, err))?;

        Ok(config)
    }

    // Feed already-decrypted MySQL bytes to exercise the post-TLS protocol stage.
    #[tokio::test]
    async fn post_tls_handshake_checks_sequence() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for seq in [0, 1, 2, 255] {
            let (mut client, stream) = tokio::io::duplex(4096);
            let config = Arc::new(setup_tls().unwrap());
            let task = tokio::spawn(async move {
                let (reader, mut writer) = tokio::io::split(stream);
                let mut backend = Backend;
                let opts = IntermediaryOptions::default();
                let (ssl, init) = AsyncMysqlIntermediary::init_before_ssl_with_options(
                    &mut backend,
                    reader,
                    &mut writer,
                    &opts,
                    &Some(config),
                )
                .await?;
                assert!(ssl);
                plain_run_with_options(backend, writer, opts, init).await
            });
            let mut header = [0; 4];
            client.read_exact(&mut header).await.unwrap();
            let size = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
            let mut greeting = vec![0; size];
            client.read_exact(&mut greeting).await.unwrap();
            let mut response = vec![0; 32];
            response[..4].copy_from_slice(&0x00088a00u32.to_le_bytes());
            response[8] = 33;
            client.write_all(&[32, 0, 0, 1]).await.unwrap();
            client.write_all(&response).await.unwrap();
            response.extend_from_slice(b"user\0\0");
            response.extend_from_slice(b"mysql_native_password\0");
            client
                .write_all(&[response.len() as u8, 0, 0, seq])
                .await
                .unwrap();
            client.write_all(&response).await.unwrap();
            if seq == 2 {
                client.read_exact(&mut header).await.unwrap();
                assert_eq!(header[3], 3);
                let mut ok = vec![0; header[0] as usize];
                client.read_exact(&mut ok).await.unwrap();
                assert_eq!(ok[0], 0);
                client.write_all(&[1, 0, 0, 0, 1]).await.unwrap();
            }
            let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
            if seq == 2 {
                result.unwrap();
            } else {
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
            }
        }
    }

    #[tokio::test]
    async fn tls_upgrade_rejects_invalid_records_and_disconnects() {
        for invalid_record in [false, true] {
            let (mut client, task) = start_tls_upgrade().await;
            use tokio::io::AsyncWriteExt;
            if invalid_record {
                client.write_all(b"GET / HTTP/1.0\r\n\r\n").await.unwrap();
            }
            client.shutdown().await.unwrap();
            let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
                .await
                .expect("TLS failure must terminate task")
                .unwrap();
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn tls_upgrade_enforces_authentication_timeout() {
        let (_client, mut task) = start_tls_upgrade().await;
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), &mut task).await;
        if result.is_err() {
            task.abort();
        }
        let error = result
            .expect("auth_timeout must bound TLS negotiation")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    async fn start_tls_upgrade() -> (
        tokio::io::DuplexStream,
        tokio::task::JoinHandle<io::Result<()>>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let config = Arc::new(setup_tls().unwrap());
        let (mut client, stream) = tokio::io::duplex(4096);
        let task = tokio::spawn(async move {
            let (mut reader, mut writer) = tokio::io::split(stream);
            let mut backend = Backend;
            let opts = IntermediaryOptions {
                auth_timeout: Some(std::time::Duration::from_millis(100)),
                ..Default::default()
            };
            let (is_ssl, init) = AsyncMysqlIntermediary::init_before_ssl_with_options(
                &mut backend,
                &mut reader,
                &mut writer,
                &opts,
                &Some(config.clone()),
            )
            .await?;
            assert!(is_ssl);
            secure_run_with_options(backend, writer, opts, config, init).await
        });
        let mut header = [0; 4];
        client.read_exact(&mut header).await.unwrap();
        let len = header[0] as usize | (header[1] as usize) << 8 | (header[2] as usize) << 16;
        let mut greeting = vec![0; len];
        client.read_exact(&mut greeting).await.unwrap();
        let mut request = vec![32, 0, 0, 1];
        // CLIENT_PROTOCOL_41 | CLIENT_SSL
        request.extend_from_slice(&0x00000a00u32.to_le_bytes());
        request.extend_from_slice(&0u32.to_le_bytes());
        request.push(33);
        request.extend_from_slice(&[0; 23]);
        client.write_all(&request).await.unwrap();
        (client, task)
    }

    pub async fn serve_on(listener: TcpListener) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            let (stream, _) = listener.accept().await?;
            let (mut r, mut w) = stream.into_split();

            tokio::spawn(async move {
                let tls_config = setup_tls().unwrap();
                let tls_config = Arc::new(tls_config);
                let mut shim = Backend;
                let ops = IntermediaryOptions::default();

                let (is_ssl, init_params) = opensrv_mysql::AsyncMysqlIntermediary::init_before_ssl(
                    &mut shim,
                    &mut r,
                    &mut w,
                    &Some(tls_config.clone()),
                )
                .await
                .unwrap();

                if is_ssl {
                    opensrv_mysql::secure_run_with_options(shim, w, ops, tls_config, init_params)
                        .await
                } else {
                    opensrv_mysql::plain_run_with_options(shim, w, ops, init_params).await
                }
            });
        }
    }
}

#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_secure() -> Result<(), Box<dyn std::error::Error>> {
    use std::{
        process::{Command, Stdio},
        time::Duration,
    };

    use std::os::unix::process::ExitStatusExt;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();

    tokio::spawn(async move {
        let _ = tls::serve_on(listener).await;
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut echo_output = Command::new("echo")
        .arg("\"SELECT * FROM foo\"")
        .stdout(Stdio::piped())
        .spawn()?;

    let echo_output = echo_output.stdout.take().unwrap();

    let supports_ssl_mode = Command::new("mysql")
        .arg("--help")
        .output()
        .map(|output| {
            let help = String::from_utf8_lossy(&output.stdout);
            help.contains("--ssl-mode")
        })
        .unwrap_or(true);

    let mut mysql_cmd = Command::new("mysql");
    mysql_cmd.args(["-h", "127.0.0.1", "--table"]);
    mysql_cmd.args(["-P", &port.to_string()]);
    if supports_ssl_mode {
        mysql_cmd.arg("--ssl-mode=REQUIRED");
    } else {
        mysql_cmd.arg("--ssl");
    }

    let mut mysql_ssl = mysql_cmd
        .stdin(echo_output)
        .stdout(Stdio::piped())
        .spawn()?;

    let status = mysql_ssl.wait()?;
    assert_eq!(status.into_raw(), 0);

    Ok(())
}
