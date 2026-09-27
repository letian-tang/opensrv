use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::{command_parse_error, ensure_response_completed, ErrorKind};

#[tokio::test]
async fn configured_reader_preserves_default_and_explicit_packet_limits() {
    for (limit, accepts) in [
        (None, true),
        (Some(4), true),
        (Some(3), false),
        (Some(0), false),
    ] {
        let options = crate::IntermediaryOptions {
            max_packet_size: limit,
            ..Default::default()
        };
        let mut reader = options.packet_reader(&b"\x04\0\0\0PING"[..]);
        let result = reader.next_async().await;
        if accepts {
            let (seq, packet) = result.unwrap().unwrap();
            assert_eq!(seq, 0);
            assert_eq!(&packet[..], b"PING");
        } else {
            assert_eq!(
                result.err().unwrap().kind(),
                std::io::ErrorKind::InvalidData
            );
        }
    }
}

#[test]
fn handshake_collation_classification_covers_all_byte_ids() {
    use crate::myc::collations::{Collation, CollationId};
    for id in 0..=255u16 {
        let mapped = CollationId::from(id);
        let result = crate::validate_collation(id);
        if mapped == CollationId::UNKNOWN_COLLATION_ID {
            assert_eq!(result.unwrap_err().0, ErrorKind::ER_UNKNOWN_COLLATION);
        } else {
            let charset = Collation::from(mapped);
            if matches!(charset.charset(), "utf8mb3" | "utf8mb4") {
                result.unwrap();
            } else {
                assert_eq!(result.unwrap_err().0, ErrorKind::ER_UNKNOWN_CHARACTER_SET);
            }
        }
    }
    for id in [33, 45, 46, 255] {
        crate::validate_collation(id).unwrap();
    }
}

#[tokio::test]
async fn invalid_greeting_collation_writes_nothing() {
    let config = crate::ServerHandshakeConfig {
        version: "8.0.0".into(),
        connection_id: 1,
        default_auth_plugin: crate::MYSQL_NATIVE_PASSWORD.into(),
        scramble: [1; 20],
    };
    let mut output = Vec::new();
    // Use the test shim from a local implementation to satisfy the generic bound.
    struct Shim;
    #[async_trait::async_trait]
    impl crate::AsyncMysqlShim<Vec<u8>> for Shim {
        type Error = std::io::Error;
        async fn on_prepare<'a>(
            &'a mut self,
            _: &'a str,
            _: crate::StatementMetaWriter<'a, Vec<u8>>,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }
        async fn on_execute<'a>(
            &'a mut self,
            _: u32,
            _: crate::ParamParser<'a>,
            _: crate::QueryResultWriter<'a, Vec<u8>>,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }
        async fn on_close(&mut self, _: u32) {}
        async fn on_query<'a>(
            &'a mut self,
            _: &'a str,
            _: crate::QueryResultWriter<'a, Vec<u8>>,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }
    }
    let result =
        crate::AsyncMysqlIntermediary::<Shim, _, _>::init_before_ssl_with_config_and_options(
            &config,
            &b""[..],
            &mut output,
            &crate::IntermediaryOptions {
                initial_collation: 8,
                ..Default::default()
            },
            #[cfg(feature = "tls")]
            &None,
        )
        .await;
    assert_eq!(
        result.err().unwrap().kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert!(output.is_empty());
}

#[test]
fn unknown_command_maps_to_unknown_com_error() {
    let (kind, msg) = command_parse_error(&[0xaa]);
    assert_eq!(kind, ErrorKind::ER_UNKNOWN_COM_ERROR);
    assert_eq!(msg, "unsupported command: 0xaa");
}

#[test]
fn empty_command_maps_to_malformed_packet() {
    let (kind, msg) = command_parse_error(&[]);
    assert_eq!(kind, ErrorKind::ER_MALFORMED_PACKET);
    assert_eq!(msg, "malformed command packet");
}

#[test]
fn incomplete_backend_response_is_rejected() {
    let completion = Arc::new(AtomicBool::new(false));
    let error = ensure_response_completed(&completion, "query").unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    completion.store(true, Ordering::Release);
    ensure_response_completed(&completion, "query").unwrap();
}
