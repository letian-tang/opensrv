use opensrv_mysql::*;
use std::io;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

struct Observed(Arc<AtomicUsize>, &'static str);
#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for Observed {
    type Error = io::Error;
    async fn auth_plugin_for_username(&self, _: &[u8]) -> &'static str {
        self.1
    }
    async fn authenticate(&self, _: &str, _: &[u8], _: &[u8], _: &[u8]) -> bool {
        self.0.fetch_add(1, Ordering::SeqCst);
        true
    }
    async fn on_init<'a>(&'a mut self, _: &'a str, writer: InitWriter<'a, W>) -> io::Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        writer.ok().await
    }
    async fn on_prepare<'a>(
        &'a mut self,
        _: &'a str,
        _: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        unreachable!()
    }
    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: ParamParser<'a>,
        _: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        unreachable!()
    }
    async fn on_query<'a>(&'a mut self, _: &'a str, _: QueryResultWriter<'a, W>) -> io::Result<()> {
        unreachable!()
    }
    async fn on_close(&mut self, _: u32) {}
}

async fn read<S: AsyncRead + Unpin>(s: &mut S) -> io::Result<(u8, Vec<u8>)> {
    let mut h = [0; 4];
    s.read_exact(&mut h).await?;
    let mut b = vec![0; u32::from_le_bytes([h[0], h[1], h[2], 0]) as usize];
    s.read_exact(&mut b).await?;
    Ok((h[3], b))
}
async fn write<S: AsyncWrite + Unpin>(s: &mut S, seq: u8, b: &[u8]) {
    let len = (b.len() as u32).to_le_bytes();
    s.write_all(&[len[0], len[1], len[2], seq]).await.unwrap();
    s.write_all(b).await.unwrap();
    s.flush().await.unwrap();
}

fn response(collation: u8, tls: bool) -> Vec<u8> {
    let mut flags = CapabilityFlags::CLIENT_PROTOCOL_41
        | CapabilityFlags::CLIENT_SECURE_CONNECTION
        | CapabilityFlags::CLIENT_PLUGIN_AUTH
        | CapabilityFlags::CLIENT_CONNECT_WITH_DB;
    flags.set(CapabilityFlags::CLIENT_SSL, tls);
    let mut b = vec![0; 32];
    b[..4].copy_from_slice(&flags.bits().to_le_bytes());
    b[8] = collation;
    b.extend_from_slice(b"user\0\0db\0mysql_native_password\0");
    b
}

async fn check<S: AsyncRead + AsyncWrite + Unpin>(
    mut client: S,
    collation: u8,
    truncated: bool,
    tls: bool,
    auth_marker: Option<u8>,
    calls: Arc<AtomicUsize>,
) {
    let mut b = response(collation, tls);
    if truncated {
        b.pop();
    }
    if let Some(marker) = auth_marker {
        let flags = u32::from_le_bytes(b[..4].try_into().unwrap())
            | CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA.bits();
        b[..4].copy_from_slice(&flags.to_le_bytes());
        b[37] = marker;
        if marker == 0xff {
            b.splice(38..38, [0; 255]);
        }
    }
    write(&mut client, if tls { 2 } else { 1 }, &b).await;
    let (seq, packet) = read(&mut client).await.unwrap();
    assert_eq!(seq, if tls { 3 } else { 2 });
    let expected = if truncated || auth_marker.is_some() {
        Some(ErrorKind::ER_MALFORMED_PACKET)
    } else if collation == 0 {
        Some(ErrorKind::ER_UNKNOWN_COLLATION)
    } else if [8, 28, 63].contains(&collation) {
        Some(ErrorKind::ER_UNKNOWN_CHARACTER_SET)
    } else {
        None
    };
    if let Some(kind) = expected {
        assert_eq!(packet[0], 0xff);
        assert_eq!(u16::from_le_bytes([packet[1], packet[2]]), kind as u16);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(read(&mut client).await.is_err());
    } else {
        assert_eq!(packet[0], 0);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        write(&mut client, 0, &[1]).await;
    }
}

const HANDSHAKE_CASES: &[(u8, bool, Option<u8>)] = &[
    (33, false, None),
    (45, false, None),
    (46, false, None),
    (255, false, None),
    (8, false, None),
    (28, false, None),
    (63, false, None),
    (0, false, None),
    (45, true, None),
    (45, false, Some(0xfb)),
    (45, false, Some(0xff)),
];

#[tokio::test]
async fn plain_handshake_validation() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for &(collation, truncated, auth_marker) in HANDSHAKE_CASES {
            let calls = Arc::new(AtomicUsize::new(0));
            let (mut client, server) = tokio::io::duplex(4096);
            let shim = Observed(calls.clone(), "mysql_native_password");
            let task = tokio::spawn(async move {
                let (r, w) = tokio::io::split(server);
                AsyncMysqlIntermediary::run_on(shim, r, w).await
            });
            read(&mut client).await.unwrap();
            check(client, collation, truncated, false, auth_marker, calls).await;
            assert_eq!(
                task.await.unwrap().is_ok(),
                !truncated && auth_marker.is_none() && [33, 45, 46, 255].contains(&collation)
            );
        }
    })
    .await
    .unwrap();
}

#[cfg(feature = "tls")]
mod encrypted {
    use super::*;
    use tokio_rustls::{
        rustls::{self, pki_types::*},
        TlsConnector,
    };
    // The repository's fixed test certificate lacks SAN. This verifier is local
    // to an in-memory test; production certificate verification is unaffected.
    #[derive(Debug)]
    struct FixtureVerifier;
    impl rustls::client::danger::ServerCertVerifier for FixtureVerifier {
        fn verify_server_cert(
            &self,
            _: &CertificateDer<'_>,
            _: &[CertificateDer<'_>],
            _: &ServerName<'_>,
            _: &[u8],
            _: UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            vec![
                rustls::SignatureScheme::RSA_PSS_SHA256,
                rustls::SignatureScheme::RSA_PKCS1_SHA256,
            ]
        }
    }

    #[tokio::test]
    async fn tls_auth_capability_matrix() {
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            for plugin in ["mysql_native_password", "caching_sha2_password"] {
                for secure in [false, true] {
                    for plugin_auth in [false, true] {
                        let certs =
                            rustls_pemfile::certs(&mut &include_bytes!("../ssl/server.crt")[..])
                                .collect::<Result<Vec<_>, _>>()
                                .unwrap();
                        let key = rustls_pemfile::private_key(
                            &mut &include_bytes!("../ssl/server.key")[..],
                        )
                        .unwrap()
                        .unwrap();
                        let config = Arc::new(
                            rustls::ServerConfig::builder()
                                .with_no_client_auth()
                                .with_single_cert(certs, key)
                                .unwrap(),
                        );
                        let calls = Arc::new(AtomicUsize::new(0));
                        let shim = Observed(calls.clone(), plugin);
                        let (mut client, server) = tokio::io::duplex(8192);
                        let task = tokio::spawn(async move {
                            let (r, mut w) = tokio::io::split(server);
                            let mut shim = shim;
                            let opts = IntermediaryOptions::default();
                            let (_, init) = AsyncMysqlIntermediary::init_before_ssl_with_options(
                                &mut shim,
                                r,
                                &mut w,
                                &opts,
                                &Some(config.clone()),
                            )
                            .await?;
                            secure_run_with_options(shim, w, opts, config, init).await
                        });
                        read(&mut client).await.unwrap();
                        let mut request = response(45, true);
                        let mut caps = CapabilityFlags::from_bits_truncate(u32::from_le_bytes(
                            request[..4].try_into().unwrap(),
                        ));
                        caps.set(CapabilityFlags::CLIENT_SECURE_CONNECTION, secure);
                        caps.set(CapabilityFlags::CLIENT_PLUGIN_AUTH, plugin_auth);
                        request[..4].copy_from_slice(&caps.bits().to_le_bytes());
                        write(&mut client, 1, &request[..32]).await;
                        let config = rustls::ClientConfig::builder()
                            .dangerous()
                            .with_custom_certificate_verifier(Arc::new(FixtureVerifier))
                            .with_no_client_auth();
                        let client = TlsConnector::from(Arc::new(config))
                            .connect(ServerName::try_from("localhost").unwrap(), client)
                            .await
                            .unwrap();
                        check_auth_capabilities(client, plugin, secure, plugin_auth, true, calls)
                            .await;
                        assert_eq!(
                            task.await.unwrap().is_ok(),
                            plugin_auth || (secure && plugin == "mysql_native_password")
                        );
                    }
                }
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn tls_handshake_validation() {
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            for &(collation, truncated, auth_marker) in HANDSHAKE_CASES {
                let certs = rustls_pemfile::certs(&mut &include_bytes!("../ssl/server.crt")[..])
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                let key =
                    rustls_pemfile::private_key(&mut &include_bytes!("../ssl/server.key")[..])
                        .unwrap()
                        .unwrap();
                let config = Arc::new(
                    rustls::ServerConfig::builder()
                        .with_no_client_auth()
                        .with_single_cert(certs, key)
                        .unwrap(),
                );
                let calls = Arc::new(AtomicUsize::new(0));
                let shim = Observed(calls.clone(), "mysql_native_password");
                let (mut client, server) = tokio::io::duplex(8192);
                let task = tokio::spawn(async move {
                    let (r, mut w) = tokio::io::split(server);
                    let mut shim = shim;
                    let opts = IntermediaryOptions::default();
                    let (_, init) = AsyncMysqlIntermediary::init_before_ssl_with_options(
                        &mut shim,
                        r,
                        &mut w,
                        &opts,
                        &Some(config.clone()),
                    )
                    .await?;
                    secure_run_with_options(shim, w, opts, config, init).await
                });
                read(&mut client).await.unwrap();
                write(&mut client, 1, &response(45, true)[..32]).await;
                let config = rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(FixtureVerifier))
                    .with_no_client_auth();
                let client = TlsConnector::from(Arc::new(config))
                    .connect(ServerName::try_from("localhost").unwrap(), client)
                    .await
                    .unwrap();
                check(client, collation, truncated, true, auth_marker, calls).await;
                assert_eq!(
                    task.await.unwrap().is_ok(),
                    !truncated && auth_marker.is_none() && [33, 45, 46, 255].contains(&collation)
                );
            }
        })
        .await
        .unwrap();
    }
}

async fn check_auth_capabilities<S: AsyncRead + AsyncWrite + Unpin>(
    mut client: S,
    plugin: &str,
    secure: bool,
    plugin_auth: bool,
    tls: bool,
    calls: Arc<AtomicUsize>,
) {
    let mut flags = CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_CONNECT_WITH_DB;
    flags.set(CapabilityFlags::CLIENT_SECURE_CONNECTION, secure);
    flags.set(CapabilityFlags::CLIENT_PLUGIN_AUTH, plugin_auth);
    flags.set(CapabilityFlags::CLIENT_SSL, tls);
    let mut b = vec![0; 32];
    b[..4].copy_from_slice(&flags.bits().to_le_bytes());
    b[8] = 45;
    b.extend_from_slice(b"user\0\0db\0");
    if plugin_auth {
        // A negotiated but empty plugin name still requires an auth switch.
        b.push(0);
    }
    let seq = if tls { 2 } else { 1 };
    write(&mut client, seq, &b).await;
    let (mut response_seq, mut packet) = read(&mut client).await.unwrap();
    assert_eq!(response_seq, seq + 1);
    if plugin_auth {
        assert_eq!(packet[0], 0xfe);
        assert!(packet[1..].starts_with(plugin.as_bytes()));
        write(&mut client, seq + 2, &[]).await;
        (response_seq, packet) = read(&mut client).await.unwrap();
        assert_eq!(response_seq, seq + 3);
    }
    if plugin_auth || (secure && plugin == "mysql_native_password") {
        assert_eq!(packet[0], 0);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        write(&mut client, 0, &[1]).await;
    } else {
        assert_eq!(packet[0], 0xff);
        assert_eq!(
            u16::from_le_bytes([packet[1], packet[2]]),
            ErrorKind::ER_NOT_SUPPORTED_AUTH_MODE as u16
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(read(&mut client).await.is_err());
    }
}

#[tokio::test]
async fn plain_auth_capability_matrix() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for plugin in ["mysql_native_password", "caching_sha2_password"] {
            for secure in [false, true] {
                for plugin_auth in [false, true] {
                    let calls = Arc::new(AtomicUsize::new(0));
                    let shim = Observed(calls.clone(), plugin);
                    let (mut client, server) = tokio::io::duplex(4096);
                    let task = tokio::spawn(async move {
                        let (r, w) = tokio::io::split(server);
                        AsyncMysqlIntermediary::run_on(shim, r, w).await
                    });
                    read(&mut client).await.unwrap();
                    check_auth_capabilities(client, plugin, secure, plugin_auth, false, calls)
                        .await;
                    assert_eq!(
                        task.await.unwrap().is_ok(),
                        plugin_auth || (secure && plugin == "mysql_native_password")
                    );
                }
            }
        }
    })
    .await
    .unwrap();
}
