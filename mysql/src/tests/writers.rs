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

// Note to developers: you can find decent overviews of the protocol at
//
//   https://github.com/cwarden/mysql-proxy/blob/master/doc/protocol.rst
//
// and
//
//   https://mariadb.com/kb/en/library/clientserver-protocol/
//
// Wireshark also does a pretty good job at parsing the MySQL protocol.

use std::io::Write;

use tokio::io::{duplex, AsyncReadExt};

use crate::packet_writer::PacketWriter;
use crate::writers::write_ok_packet;
use crate::{CapabilityFlags, OkResponse, U24_MAX};

fn split_wire_packets(mut wire: &[u8]) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    while !wire.is_empty() {
        assert!(wire.len() >= 4);
        let len = wire[0] as usize | (wire[1] as usize) << 8 | (wire[2] as usize) << 16;
        assert_eq!(wire[3], packets.len() as u8);
        assert!(wire.len() >= 4 + len);
        packets.push(wire[4..4 + len].to_vec());
        wire = &wire[4 + len..];
    }
    packets
}

#[tokio::test]
async fn explicit_metadata_matches_query_and_prepare_wire() {
    use crate::{
        Column, ColumnFlags, ColumnMetadata, ColumnType, QueryResultWriter, StatementMetaWriter,
        StatusFlags,
    };
    use std::sync::{atomic::AtomicBool, Arc};
    let mut c = Column {
        table: "t".into(),
        column: "v".into(),
        collen: 123,
        coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
        colflags: ColumnFlags::BINARY_FLAG,
    };
    let cols = vec![
        c.clone(),
        {
            c.coltype = ColumnType::MYSQL_TYPE_BLOB;
            c.clone()
        },
        {
            c.coltype = ColumnType::MYSQL_TYPE_NEWDECIMAL;
            c
        },
    ];
    let metadata = [
        ColumnMetadata {
            collation: Some(46),
            decimals: 0,
        },
        ColumnMetadata {
            collation: Some(45),
            decimals: 0,
        },
        ColumnMetadata {
            collation: Some(63),
            decimals: 2,
        },
    ];
    for deprecate in [false, true] {
        let mut caps = CapabilityFlags::CLIENT_PROTOCOL_41;
        caps.set(CapabilityFlags::CLIENT_DEPRECATE_EOF, deprecate);
        let mut query_wire = Vec::new();
        let mut w = PacketWriter::new(&mut query_wire);
        QueryResultWriter::new(&mut w, false, caps, StatusFlags::SERVER_STATUS_IN_TRANS)
            .start_with_metadata(&cols, &metadata)
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        drop(w);
        let query_packets = split_wire_packets(&query_wire);
        for (packet, expected) in query_packets[1..4].iter().zip(metadata) {
            let mut pos = 0;
            for _ in 0..6 {
                pos += 1 + packet[pos] as usize;
            }
            assert_eq!(packet[pos], 12);
            assert_eq!(
                u16::from_le_bytes([packet[pos + 1], packet[pos + 2]]),
                expected.collation.unwrap()
            );
            assert_eq!(
                u32::from_le_bytes(packet[pos + 3..pos + 7].try_into().unwrap()),
                123
            );
            assert_eq!(packet[pos + 10], expected.decimals);
        }
        let mut prepare_wire = Vec::new();
        let mut w = PacketWriter::new(&mut prepare_wire);
        let mut stmts = std::collections::HashMap::new();
        StatementMetaWriter {
            writer: &mut w,
            stmts: &mut stmts,
            client_capabilities: caps,
            status_flags: StatusFlags::SERVER_STATUS_IN_TRANS,
            completion: Arc::new(AtomicBool::new(false)),
        }
        .reply_with_metadata(1, &cols, &cols, &metadata, &metadata)
        .await
        .unwrap();
        drop(w);
        let prepare_packets = split_wire_packets(&prepare_wire);
        assert_eq!(prepare_packets[1..4], query_packets[1..4]);
        let start = if deprecate { 4 } else { 5 };
        assert_eq!(prepare_packets[start..start + 3], query_packets[1..4]);
        if !deprecate {
            assert_eq!(prepare_packets[4], vec![0xfe, 0, 0, 1, 0]);
            assert_eq!(prepare_packets[8], prepare_packets[4]);
        }
    }
}

#[tokio::test]
async fn invalid_metadata_is_rejected_before_any_response() {
    use crate::{
        Column, ColumnFlags, ColumnMetadata, ColumnType, QueryResultWriter, StatementMetaWriter,
        StatusFlags,
    };
    use std::sync::{atomic::AtomicBool, Arc};
    let cols = [Column {
        table: String::new(),
        column: "v".into(),
        collen: 0,
        coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
        colflags: ColumnFlags::empty(),
    }];
    for metadata in [
        vec![],
        vec![ColumnMetadata {
            collation: Some(8),
            decimals: 0,
        }],
        vec![ColumnMetadata {
            collation: Some(0),
            decimals: 0,
        }],
        vec![ColumnMetadata {
            collation: Some(65535),
            decimals: 0,
        }],
        vec![ColumnMetadata {
            collation: Some(45),
            decimals: 32,
        }],
    ] {
        let mut wire = Vec::new();
        let mut w = PacketWriter::new(&mut wire);
        let caps = CapabilityFlags::CLIENT_PROTOCOL_41;
        let error = QueryResultWriter::new(&mut w, false, caps, StatusFlags::empty())
            .start_with_metadata(&cols, &metadata)
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        let mut stmts = std::collections::HashMap::new();
        for bad_params in [false, true] {
            let meta = StatementMetaWriter {
                writer: &mut w,
                stmts: &mut stmts,
                client_capabilities: caps,
                status_flags: StatusFlags::empty(),
                completion: Arc::new(AtomicBool::new(false)),
            };
            let error = if bad_params {
                meta.reply_with_metadata(1, &cols, &[], &metadata, &[])
                    .await
            } else {
                meta.reply_with_metadata(1, &[], &cols, &[], &metadata)
                    .await
            }
            .unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        }
        assert!(stmts.is_empty());
        drop(w);
        assert!(wire.is_empty());
    }
    // Upper UTF-8 collation IDs are legal in column metadata (u16), even though
    // the client's handshake can only express IDs up to 255.
    ColumnMetadata {
        collation: Some(309),
        decimals: 31,
    }
    .validate()
    .unwrap();
}

#[tokio::test]
async fn explicit_zero_and_result_local_flags_survive_multiple_results() {
    use crate::{QueryResultWriter, StatusFlags};
    for deprecate in [false, true] {
        let mut caps = CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_MULTI_RESULTS;
        caps.set(CapabilityFlags::CLIENT_DEPRECATE_EOF, deprecate);
        let mut wire = Vec::new();
        let mut w = PacketWriter::new(&mut wire);
        let mut status = StatusFlags::SERVER_STATUS_AUTOCOMMIT;
        let (results, _) = QueryResultWriter::new_tracked(&mut w, false, caps, &mut status);
        let results = results
            .complete_one_with_status(OkResponse {
                status_flags: StatusFlags::SERVER_STATUS_IN_TRANS
                    | StatusFlags::SERVER_STATUS_NO_INDEX_USED,
                ..Default::default()
            })
            .await
            .unwrap();
        let results = results.complete_one(OkResponse::default()).await.unwrap();
        results
            .completed_with_status(OkResponse::default())
            .await
            .unwrap();
        drop(w);
        assert!(status.is_empty());
        let packets = split_wire_packets(&wire);
        let flags: Vec<_> = packets
            .iter()
            .map(|p| u16::from_le_bytes([p[3], p[4]]))
            .collect();
        assert_eq!(
            flags,
            vec![
                (StatusFlags::SERVER_STATUS_IN_TRANS
                    | StatusFlags::SERVER_STATUS_NO_INDEX_USED
                    | StatusFlags::SERVER_MORE_RESULTS_EXISTS)
                    .bits(),
                (StatusFlags::SERVER_STATUS_IN_TRANS | StatusFlags::SERVER_MORE_RESULTS_EXISTS)
                    .bits(),
                0
            ]
        );
    }
}

#[tokio::test]
async fn multiple_results_gate_capabilities_and_end_errors_without_extra_eof() {
    use crate::{Column, ColumnFlags, ColumnType, ErrorKind, QueryResultWriter, StatusFlags};
    use std::sync::atomic::Ordering;
    let columns = [Column {
        table: String::new(),
        column: "v".into(),
        collen: 0,
        coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
        colflags: ColumnFlags::empty(),
    }];
    for binary in [false, true] {
        for deprecate in [false, true] {
            for negotiated in [false, true] {
                let mut caps = CapabilityFlags::CLIENT_PROTOCOL_41;
                caps.set(CapabilityFlags::CLIENT_DEPRECATE_EOF, deprecate);
                // Negotiating only the other command's capability is insufficient.
                caps |= if binary == negotiated {
                    CapabilityFlags::CLIENT_PS_MULTI_RESULTS
                } else {
                    CapabilityFlags::CLIENT_MULTI_RESULTS
                };
                let mut wire = Vec::new();
                let mut writer = PacketWriter::new(&mut wire);
                writer.set_max_packet_size(64);
                let trans = StatusFlags::SERVER_STATUS_IN_TRANS;
                let mut status = trans;
                let (results, completed) =
                    QueryResultWriter::new_tracked(&mut writer, binary, caps, &mut status);
                let mut rows = results.start(&columns).await.unwrap();
                rows.write_row(["ok"]).await.unwrap();
                let results = rows.finish_one().await.unwrap();
                let second = results.start(&columns).await;
                if negotiated {
                    let mut rows = second.unwrap();
                    // A row limit violation is recoverable before that row's
                    // bytes reach the transport. No partial row/EOF may precede ERR.
                    assert!(rows.write_col("x".repeat(65)).is_err());
                    rows.set_status_flags(StatusFlags::empty()); // backend rolled back
                    rows.finish_error(ErrorKind::ER_NET_PACKET_TOO_LARGE, b"large")
                        .await
                        .unwrap();
                } else {
                    assert_eq!(
                        second.err().unwrap().kind(),
                        std::io::ErrorKind::InvalidInput
                    );
                }
                assert_eq!(completed.load(Ordering::Acquire), negotiated);
                drop(writer);
                assert_eq!(
                    status,
                    if negotiated {
                        StatusFlags::empty()
                    } else {
                        trans
                    }
                );
                let packets = split_wire_packets(&wire);
                let first_row = if deprecate { 2 } else { 3 };
                let expected_row = if binary {
                    &b"\0\0\x02ok"[..]
                } else {
                    &b"\x02ok"[..]
                };
                assert_eq!(packets[first_row], expected_row);
                if negotiated {
                    let end = &packets[first_row + 1];
                    assert_eq!(end[0], 0xfe);
                    assert_eq!(
                        u16::from_le_bytes([end[3], end[4]]),
                        (trans | StatusFlags::SERVER_MORE_RESULTS_EXISTS).bits()
                    );
                    assert_eq!(packets[first_row + 2], [1]); // second result header
                    let last = packets.last().unwrap();
                    assert_eq!(last[0], 0xff);
                    assert_eq!(
                        u16::from_le_bytes([last[1], last[2]]),
                        ErrorKind::ER_NET_PACKET_TOO_LARGE as u16
                    );
                    assert_eq!(packets.len(), if deprecate { 7 } else { 9 });
                } else {
                    assert_eq!(packets.len(), first_row + 1); // no false success terminator
                }
            }
        }
    }
}

#[tokio::test]
async fn ok_session_state_requires_both_capability_and_status() {
    for session_track in [false, true] {
        for deprecate_eof in [false, true] {
            for changed in [false, true] {
                let mut caps = CapabilityFlags::CLIENT_PROTOCOL_41;
                caps.set(CapabilityFlags::CLIENT_SESSION_TRACK, session_track);
                caps.set(CapabilityFlags::CLIENT_DEPRECATE_EOF, deprecate_eof);
                let mut status = crate::StatusFlags::empty();
                status.set(crate::StatusFlags::SERVER_SESSION_STATE_CHANGED, changed);
                let mut wire = Vec::new();
                let mut writer = PacketWriter::new(&mut wire);
                // A session-state-change record: type 2, data length 2, string "1".
                write_ok_packet(
                    &mut writer,
                    caps,
                    OkResponse {
                        header: if deprecate_eof { 0xfe } else { 0 },
                        status_flags: status,
                        info: "info".into(),
                        session_state_info: "\x02\x02\x011".into(),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
                let mut expected = vec![if deprecate_eof { 0xfe } else { 0 }, 0, 0];
                expected.extend_from_slice(&status.bits().to_le_bytes());
                expected.extend_from_slice(&[0, 0]);
                if session_track {
                    expected.push(4);
                }
                expected.extend_from_slice(b"info");
                if session_track && changed {
                    expected.extend_from_slice(&[4, 2, 2, 1, b'1']);
                }
                drop(writer);
                assert_eq!(split_wire_packets(&wire), vec![expected]);
            }
        }
    }
}

#[tokio::test]
async fn resultset_terminators_follow_capability_matrix() {
    for session_track in [false, true] {
        for deprecate_eof in [false, true] {
            for binary in [false, true] {
                let mut caps = CapabilityFlags::CLIENT_PROTOCOL_41;
                caps.set(CapabilityFlags::CLIENT_SESSION_TRACK, session_track);
                caps.set(CapabilityFlags::CLIENT_DEPRECATE_EOF, deprecate_eof);
                let columns = [crate::Column {
                    table: String::new(),
                    column: "n".into(),
                    collen: 4,
                    coltype: crate::ColumnType::MYSQL_TYPE_LONG,
                    colflags: crate::ColumnFlags::empty(),
                }];
                let mut wire = Vec::new();
                let mut writer = PacketWriter::new(&mut wire);
                let mut rows = crate::QueryResultWriter::new(
                    &mut writer,
                    binary,
                    caps,
                    crate::StatusFlags::empty(),
                )
                .start(&columns)
                .await
                .unwrap();
                rows.write_row([42i32]).await.unwrap();
                rows.finish().await.unwrap();
                drop(writer);
                let packets = split_wire_packets(&wire);
                assert_eq!(packets.len(), if deprecate_eof { 4 } else { 5 });
                assert_eq!(packets[0], [1]);
                let row_index = if deprecate_eof {
                    2
                } else {
                    assert_eq!(packets[2], [0xfe, 0, 0, 0, 0]);
                    3
                };
                assert_eq!(
                    packets[row_index],
                    if binary {
                        vec![0, 0, 42, 0, 0, 0]
                    } else {
                        vec![2, b'4', b'2']
                    }
                );
                let mut end = if deprecate_eof {
                    vec![0xfe, 0, 0, 0, 0, 0, 0]
                } else {
                    vec![0xfe, 0, 0, 0, 0]
                };
                if deprecate_eof && session_track {
                    end.push(0);
                }
                assert_eq!(packets[row_index + 1], end);
            }
        }
    }
}

#[tokio::test]
async fn partial_text_row_is_discarded_before_error_packet() {
    let caps = CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_DEPRECATE_EOF;
    let columns = [
        crate::Column {
            table: String::new(),
            column: "a".into(),
            collen: 4,
            coltype: crate::ColumnType::MYSQL_TYPE_LONG,
            colflags: crate::ColumnFlags::empty(),
        },
        crate::Column {
            table: String::new(),
            column: "b".into(),
            collen: 4,
            coltype: crate::ColumnType::MYSQL_TYPE_LONG,
            colflags: crate::ColumnFlags::empty(),
        },
    ];
    let mut wire = Vec::new();
    let mut writer = PacketWriter::new(&mut wire);
    let mut rows = crate::QueryResultWriter::new(
        &mut writer,
        false,
        caps,
        crate::StatusFlags::SERVER_STATUS_AUTOCOMMIT,
    )
    .start(&columns)
    .await
    .unwrap();
    rows.write_col(42i32).unwrap();
    rows.finish_error(crate::ErrorKind::ER_UNKNOWN_ERROR, b"failed")
        .await
        .unwrap();
    drop(writer);

    let packets = split_wire_packets(&wire);
    assert_eq!(packets.len(), 4);
    assert_eq!(packets[0], [2]);
    assert_eq!(packets[3][0], 0xff);
}

async fn capture_ok_payload(info: &str, capabilities: CapabilityFlags, header: u8) -> Vec<u8> {
    let (mut client, server) = duplex(1024);
    let mut writer = PacketWriter::new(server);

    let ok_packet = OkResponse {
        header,
        info: info.to_string(),
        ..Default::default()
    };

    write_ok_packet(&mut writer, capabilities, ok_packet)
        .await
        .expect("write_ok_packet succeeds");

    let mut header_buf = [0u8; 4];
    client
        .read_exact(&mut header_buf)
        .await
        .expect("payload header available");
    let payload_len = (header_buf[0] as usize)
        | ((header_buf[1] as usize) << 8)
        | ((header_buf[2] as usize) << 16);
    let mut payload = vec![0u8; payload_len];
    client
        .read_exact(&mut payload)
        .await
        .expect("payload body available");
    payload
}

fn parse_lenenc_int(data: &[u8]) -> (u64, usize) {
    match data[0] {
        0xFC => {
            let len = u16::from_le_bytes([data[1], data[2]]) as u64;
            (len, 3)
        }
        0xFD => {
            let len = (data[1] as u64) | ((data[2] as u64) << 8) | ((data[3] as u64) << 16);
            (len, 4)
        }
        0xFE => {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&data[1..9]);
            (u64::from_le_bytes(buf), 9)
        }
        v => (v as u64, 1),
    }
}

fn consume_ok_prefix(payload: &[u8]) -> (usize, u8, u16, u16) {
    let mut idx = 0;
    let header = payload[idx];
    idx += 1;

    let (affected_rows, consumed) = parse_lenenc_int(&payload[idx..]);
    assert_eq!(affected_rows, 0);
    idx += consumed;

    let (last_insert_id, consumed) = parse_lenenc_int(&payload[idx..]);
    assert_eq!(last_insert_id, 0);
    idx += consumed;

    let status = u16::from_le_bytes([payload[idx], payload[idx + 1]]);
    idx += 2;

    let warnings = u16::from_le_bytes([payload[idx], payload[idx + 1]]);
    idx += 2;

    (idx, header, status, warnings)
}

#[tokio::test]
async fn ok_packet_info_lenenc_when_session_track() {
    let info = "Read 1 rows, 1.00 B in 0.007 sec.";
    let payload = capture_ok_payload(
        info,
        CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_SESSION_TRACK,
        0x00,
    )
    .await;

    let (mut idx, header, status, warnings) = consume_ok_prefix(&payload);
    assert_eq!(header, 0x00);
    assert_eq!(status, 0);
    assert_eq!(warnings, 0);

    let (info_len, consumed) = parse_lenenc_int(&payload[idx..]);
    assert_eq!(info_len as usize, info.len());
    idx += consumed;

    let encoded = &payload[idx..idx + info.len()];
    assert_eq!(encoded, info.as_bytes());
}

#[tokio::test]
async fn ok_packet_info_is_plain_when_deprecate_eof_without_session_track() {
    let info = "Read 1 rows, 1.00 B in 0.007 sec.";
    let payload = capture_ok_payload(
        info,
        CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_DEPRECATE_EOF,
        0x00,
    )
    .await;

    let (idx, header, status, warnings) = consume_ok_prefix(&payload);
    assert_eq!(header, 0x00);
    assert_eq!(status, 0);
    assert_eq!(warnings, 0);

    assert_eq!(&payload[idx..], info.as_bytes());
}

#[tokio::test]
async fn ok_packet_info_is_plain_when_header_is_fe_without_session_track() {
    let info = "Read 1 rows, 1.00 B in 0.007 sec.";
    let payload = capture_ok_payload(info, CapabilityFlags::CLIENT_PROTOCOL_41, 0xfe).await;

    let (idx, header, status, warnings) = consume_ok_prefix(&payload);
    assert_eq!(header, 0xfe);
    assert_eq!(status, 0);
    assert_eq!(warnings, 0);

    assert_eq!(&payload[idx..], info.as_bytes());
}

#[tokio::test]
async fn ok_packet_info_plain_when_no_flags() {
    let info = "Read 1 rows, 1.00 B in 0.007 sec.";
    let payload = capture_ok_payload(info, CapabilityFlags::CLIENT_PROTOCOL_41, 0x00).await;

    let (idx, header, status, warnings) = consume_ok_prefix(&payload);
    assert_eq!(header, 0x00);
    assert_eq!(status, 0);
    assert_eq!(warnings, 0);

    let encoded = &payload[idx..];
    assert_eq!(encoded, info.as_bytes());
}

#[tokio::test]
async fn ok_packet_info_extended_lenenc_with_flags() {
    let info = "x".repeat(300);
    let payload = capture_ok_payload(
        &info,
        CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_SESSION_TRACK,
        0x00,
    )
    .await;

    let (mut idx, header, status, warnings) = consume_ok_prefix(&payload);
    assert_eq!(header, 0x00);
    assert_eq!(status, 0);
    assert_eq!(warnings, 0);

    let (info_len, consumed) = parse_lenenc_int(&payload[idx..]);
    assert_eq!(consumed, 3); // expect 0xFC marker with two-byte length
    assert_eq!(payload[idx], 0xFC);
    assert_eq!(info_len as usize, info.len());
    idx += consumed;

    let encoded = &payload[idx..idx + info.len()];
    assert_eq!(encoded, info.as_bytes());
}

#[tokio::test]
async fn packet_writer_terminates_exact_multiple_with_empty_packet() {
    let (mut client, server) = duplex(U24_MAX + 16);
    let mut writer = PacketWriter::new(server);

    writer
        .write_all(&vec![0u8; U24_MAX])
        .expect("write payload");
    writer.end_packet().await.expect("finish packet");

    let mut header_buf = [0u8; 4];
    client
        .read_exact(&mut header_buf)
        .await
        .expect("first packet header");
    assert_eq!(header_buf, [0xff, 0xff, 0xff, 0]);

    let mut payload = vec![0u8; U24_MAX];
    client
        .read_exact(&mut payload)
        .await
        .expect("first packet payload");

    client
        .read_exact(&mut header_buf)
        .await
        .expect("terminator packet header");
    assert_eq!(header_buf, [0x00, 0x00, 0x00, 1]);
}
