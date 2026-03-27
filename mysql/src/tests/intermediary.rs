use crate::{command_parse_error, ErrorKind};

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
