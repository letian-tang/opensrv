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
