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

use std::collections::HashMap;
use std::io;

use crate::myc;
use crate::{StatementData, Value};

/// A valid EXECUTE header whose parameter type cannot accept streamed data.
/// Keep this distinct from malformed wire data when choosing the MySQL error code.
#[derive(Debug)]
pub(crate) struct IncompatibleLongData;

impl std::fmt::Display for IncompatibleLongData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("long data requires a string or blob parameter type")
    }
}

impl std::error::Error for IncompatibleLongData {}

/// A `ParamParser` decodes query parameters included in a client's `EXECUTE` command given
/// type information for the expected parameters.
///
/// Users should invoke [`iter`](struct.ParamParser.html#method.iter) method to iterate over the
/// provided parameters.
pub struct ParamParser<'a> {
    pub(crate) params: u16,
    pub(crate) bytes: &'a [u8],
    pub(crate) long_data: &'a HashMap<u16, Vec<u8>>,
    pub(crate) bound_types: &'a mut Vec<(myc::constants::ColumnType, bool)>,
}

impl<'a> ParamParser<'a> {
    pub(crate) fn new(input: &'a [u8], stmt: &'a mut StatementData) -> io::Result<Self> {
        let mut parser = ParamParser {
            params: stmt.params,
            bytes: input,
            long_data: &stmt.long_data,
            bound_types: &mut stmt.bound_types,
        };
        parser.validate()?;
        Ok(parser)
    }

    fn validate(&mut self) -> io::Result<()> {
        let mut input = self.bytes;
        let nullmap_len = (self.params as usize).div_ceil(8);
        if input.len() < nullmap_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "malformed execute packet: null-bitmap truncated",
            ));
        }
        let (nullmap, rest) = input.split_at(nullmap_len);
        input = rest;

        if self.params > 0 {
            if input.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "malformed execute packet: missing new-params-bound flag",
                ));
            }

            let new_params_bound = input[0] != 0x00;
            input = &input[1..];

            if new_params_bound {
                let type_map_len = 2 * self.params as usize;
                if input.len() < type_map_len {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "malformed execute packet: parameter type map truncated",
                    ));
                }

                let (typmap, rest) = input.split_at(type_map_len);
                self.bound_types.clear();
                for i in 0..self.params as usize {
                    let coltype =
                        myc::constants::ColumnType::try_from(typmap[2 * i]).map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("bad column type 0x{:x}: {}", typmap[2 * i], e),
                            )
                        })?;
                    self.bound_types
                        .push((coltype, (typmap[2 * i + 1] & 128) != 0));
                }
                input = rest;
            } else if self.bound_types.len() < self.params as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "execute packet omitted parameter types before they were bound",
                ));
            }
        }

        for col in 0..self.params {
            let byte = col as usize / 8;
            if byte >= nullmap.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "malformed execute packet: null-bitmap too short",
                ));
            }
            let (coltype, unsigned) =
                self.bound_types.get(col as usize).copied().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("missing bound type for parameter {}", col),
                    )
                })?;
            if self.long_data.contains_key(&col) {
                use myc::constants::ColumnType::*;
                if !matches!(
                    coltype,
                    MYSQL_TYPE_TINY_BLOB
                        | MYSQL_TYPE_MEDIUM_BLOB
                        | MYSQL_TYPE_LONG_BLOB
                        | MYSQL_TYPE_BLOB
                        | MYSQL_TYPE_VAR_STRING
                        | MYSQL_TYPE_STRING
                ) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        IncompatibleLongData,
                    ));
                }
                continue;
            }
            if (nullmap[byte] & (1u8 << (col % 8))) != 0 {
                continue;
            }
            Value::parse_from(&mut input, coltype, unsigned)?;
        }

        if !input.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "malformed execute packet: {} trailing parameter bytes",
                    input.len()
                ),
            ));
        }

        Ok(())
    }
}

impl<'a> IntoIterator for ParamParser<'a> {
    type IntoIter = Params<'a>;
    type Item = ParamValue<'a>;
    fn into_iter(self) -> Params<'a> {
        Params {
            params: self.params,
            input: self.bytes,
            nullmap: None,
            col: 0,
            long_data: self.long_data,
            bound_types: self.bound_types,
        }
    }
}

/// An iterator over parameters provided by a client in an `EXECUTE` command.
pub struct Params<'a> {
    params: u16,
    input: &'a [u8],
    nullmap: Option<&'a [u8]>,
    col: u16,
    long_data: &'a HashMap<u16, Vec<u8>>,
    bound_types: &'a mut Vec<(myc::constants::ColumnType, bool)>,
}

/// A single parameter value provided by a client when issuing an `EXECUTE` command.
pub struct ParamValue<'a> {
    /// The value provided for this parameter.
    pub value: Value<'a>,
    /// The column type assigned to this parameter.
    pub coltype: myc::constants::ColumnType,
}

impl<'a> Iterator for Params<'a> {
    type Item = ParamValue<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.nullmap.is_none() {
            let nullmap_len = (self.params as usize).div_ceil(8);
            let (nullmap, rest) = self.input.split_at(nullmap_len);
            self.nullmap = Some(nullmap);

            if self.params == 0 {
                self.input = rest;
            } else {
                let new_params_bound = rest[0] != 0x00;
                let rest = &rest[1..];
                if new_params_bound {
                    let (typmap, rest) = rest.split_at(2 * self.params as usize);
                    self.bound_types.clear();
                    for i in 0..self.params as usize {
                        self.bound_types.push((
                            myc::constants::ColumnType::try_from(typmap[2 * i]).unwrap(),
                            (typmap[2 * i + 1] & 128) != 0,
                        ));
                    }
                    self.input = rest;
                } else {
                    self.input = rest;
                }
            }
        }

        if self.col >= self.params {
            return None;
        }
        let pt = &self.bound_types[self.col as usize];
        // MySQL's final binding gives accumulated long data precedence over NULL.
        if let Some(data) = self.long_data.get(&self.col) {
            self.col += 1;
            return Some(ParamValue {
                value: Value::bytes(data),
                coltype: pt.0,
            });
        }

        // https://web.archive.org/web/20170404144156/https://dev.mysql.com/doc/internals/en/null-bitmap.html
        // NULL-bitmap-byte = ((field-pos + offset) / 8)
        // NULL-bitmap-bit  = ((field-pos + offset) % 8)
        if let Some(nullmap) = self.nullmap {
            let byte = self.col as usize / 8;
            if byte >= nullmap.len() {
                return None;
            }
            if (nullmap[byte] & (1u8 << (self.col % 8))) != 0 {
                self.col += 1;
                return Some(ParamValue {
                    value: Value::null(),
                    coltype: pt.0,
                });
            }
        } else {
            unreachable!();
        }

        let v = Value::parse_from(&mut self.input, pt.0, pt.1).unwrap();
        self.col += 1;
        Some(ParamValue {
            value: v,
            coltype: pt.0,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::myc::constants::ColumnType;

    use super::*;

    #[test]
    fn accepted_parameter_types_preserve_alignment_across_binding_modes() {
        use crate::ValueInner;
        use ColumnType::*;
        // Independent wire fixtures, not generated by the parser under test.
        for id in 0..=u8::MAX {
            let Ok(kind) = ColumnType::try_from(id) else {
                continue;
            };
            for unsigned in [false, true] {
                let (payload, value): (Vec<u8>, ValueInner<'_>) = match kind {
                    MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_YEAR | MYSQL_TYPE_LONG
                    | MYSQL_TYPE_INT24 | MYSQL_TYPE_LONGLONG => {
                        let width = match kind {
                            MYSQL_TYPE_TINY => 1,
                            MYSQL_TYPE_SHORT | MYSQL_TYPE_YEAR => 2,
                            MYSQL_TYPE_LONG | MYSQL_TYPE_INT24 => 4,
                            _ => 8,
                        };
                        let value = if unsigned {
                            ValueInner::UInt(u64::MAX >> (64 - 8 * width))
                        } else {
                            ValueInner::Int(-1)
                        };
                        (vec![255; width], value)
                    }
                    MYSQL_TYPE_FLOAT => (1.5f32.to_le_bytes().to_vec(), ValueInner::Double(1.5)),
                    MYSQL_TYPE_DOUBLE => {
                        ((-2.5f64).to_le_bytes().to_vec(), ValueInner::Double(-2.5))
                    }
                    MYSQL_TYPE_STRING
                    | MYSQL_TYPE_VAR_STRING
                    | MYSQL_TYPE_VARCHAR
                    | MYSQL_TYPE_BLOB
                    | MYSQL_TYPE_TINY_BLOB
                    | MYSQL_TYPE_MEDIUM_BLOB
                    | MYSQL_TYPE_LONG_BLOB
                    | MYSQL_TYPE_SET
                    | MYSQL_TYPE_ENUM
                    | MYSQL_TYPE_DECIMAL
                    | MYSQL_TYPE_NEWDECIMAL
                    | MYSQL_TYPE_BIT
                    | MYSQL_TYPE_GEOMETRY
                    | MYSQL_TYPE_JSON => (vec![2, 0, 255], ValueInner::Bytes(b"\0\xff")),
                    MYSQL_TYPE_DATE => {
                        (vec![4, 0xea, 7, 9, 27], ValueInner::Date(&[0xea, 7, 9, 27]))
                    }
                    MYSQL_TYPE_DATETIME | MYSQL_TYPE_TIMESTAMP => (
                        vec![7, 0xea, 7, 9, 27, 1, 2, 3],
                        ValueInner::Datetime(&[0xea, 7, 9, 27, 1, 2, 3]),
                    ),
                    MYSQL_TYPE_TIME => (
                        vec![8, 1, 1, 0, 0, 0, 1, 2, 3],
                        ValueInner::Time(&[1, 1, 0, 0, 0, 1, 2, 3]),
                    ),
                    MYSQL_TYPE_NULL => (vec![], ValueInner::NULL),
                    _ => continue,
                };
                for null in [false, true] {
                    for long_data in [false, true] {
                        let accepts_long_data = matches!(
                            kind,
                            MYSQL_TYPE_STRING
                                | MYSQL_TYPE_VAR_STRING
                                | MYSQL_TYPE_BLOB
                                | MYSQL_TYPE_TINY_BLOB
                                | MYSQL_TYPE_MEDIUM_BLOB
                                | MYSQL_TYPE_LONG_BLOB
                        );
                        let mut stmt = StatementData {
                            params: 2,
                            ..Default::default()
                        };
                        if long_data {
                            stmt.long_data.insert(0, b"uploaded".to_vec());
                        }
                        let expected = if long_data {
                            ValueInner::Bytes(b"uploaded")
                        } else if null {
                            ValueInner::NULL
                        } else {
                            value
                        };
                        // The second parameter is a sentinel; no NULL/long-data
                        // value may consume any bytes belonging to it.
                        for rebind in [true, false, true] {
                            let mut wire = vec![u8::from(null), u8::from(rebind)];
                            if rebind {
                                wire.extend_from_slice(&[
                                    id,
                                    if unsigned { 128 } else { 0 },
                                    MYSQL_TYPE_TINY as u8,
                                    0,
                                ]);
                            }
                            if !null && !long_data {
                                wire.extend_from_slice(&payload);
                            }
                            wire.push(42);
                            let parsed = ParamParser::new(&wire, &mut stmt);
                            if long_data && !accepts_long_data {
                                assert!(parsed
                                    .err()
                                    .unwrap()
                                    .get_ref()
                                    .unwrap()
                                    .is::<IncompatibleLongData>());
                                continue;
                            }
                            let values: Vec<_> = parsed.unwrap().into_iter().collect();
                            assert_eq!(values.len(), 2);
                            assert_eq!(values[0].coltype, kind);
                            assert_eq!(values[0].value.into_inner(), expected,
                                "kind={kind:?}, unsigned={unsigned}, null={null}, long_data={long_data}, rebind={rebind}");
                            assert_eq!(values[1].value.into_inner(), ValueInner::Int(42));
                            wire.push(99);
                            assert!(ParamParser::new(&wire, &mut stmt).is_err());
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn temporal_values_preserve_zero_fraction_offset_and_parameter_alignment() {
        for (ty, value) in [
            (ColumnType::MYSQL_TYPE_DATE, vec![]),
            (ColumnType::MYSQL_TYPE_DATE, vec![234, 7, 9, 27]),
            (
                ColumnType::MYSQL_TYPE_TIME,
                vec![1, 1, 0, 0, 0, 1, 2, 3, 64, 226, 1, 0],
            ),
            (
                ColumnType::MYSQL_TYPE_DATETIME,
                vec![234, 7, 9, 27, 1, 2, 3, 64, 226, 1, 0],
            ),
            // MySQL 8 DATETIME with timezone displacement +08:00 (480 minutes).
            (
                ColumnType::MYSQL_TYPE_TIMESTAMP,
                vec![234, 7, 9, 27, 1, 2, 3, 64, 226, 1, 0, 224, 1],
            ),
        ] {
            let mut wire = vec![
                0,
                1,
                ty as u8,
                0,
                ColumnType::MYSQL_TYPE_TINY as u8,
                0,
                value.len() as u8,
            ];
            wire.extend_from_slice(&value);
            wire.push(42);
            for end in 0..wire.len() {
                let mut stmt = StatementData {
                    params: 2,
                    ..Default::default()
                };
                assert!(
                    ParamParser::new(&wire[..end], &mut stmt).is_err(),
                    "type={ty:?}, prefix={end}"
                );
            }
            let mut stmt = StatementData {
                params: 2,
                ..Default::default()
            };
            for rebind in [true, false] {
                let payload = if rebind {
                    wire.clone()
                } else {
                    let mut payload = vec![0, 0];
                    payload.extend_from_slice(&wire[6..]);
                    payload
                };
                let values: Vec<_> = ParamParser::new(&payload, &mut stmt)
                    .unwrap()
                    .into_iter()
                    .map(|p| p.value.into_inner())
                    .collect();
                let raw = match values[0] {
                    crate::ValueInner::Date(b)
                    | crate::ValueInner::Time(b)
                    | crate::ValueInner::Datetime(b) => b,
                    _ => panic!("not temporal"),
                };
                assert_eq!(raw, value);
                assert_eq!(values[1], crate::ValueInner::Int(42));
            }
        }
    }

    #[test]
    fn temporal_parameter_lengths_are_validated_before_iteration() {
        for ty in [
            ColumnType::MYSQL_TYPE_DATE,
            ColumnType::MYSQL_TYPE_TIME,
            ColumnType::MYSQL_TYPE_DATETIME,
            ColumnType::MYSQL_TYPE_TIMESTAMP,
        ] {
            for len in 0..=255u8 {
                let mut stmt = StatementData {
                    params: 1,
                    ..Default::default()
                };
                let mut wire = vec![0, 1, ty as u8, 0, len];
                wire.extend(std::iter::repeat_n(0, len as usize));
                let accepted = match ty {
                    ColumnType::MYSQL_TYPE_TIME => [0, 8, 12].contains(&len),
                    ColumnType::MYSQL_TYPE_DATE => [0, 4, 7, 11].contains(&len),
                    _ => [0, 4, 7, 11, 13].contains(&len),
                };
                let parser = ParamParser::new(&wire, &mut stmt);
                assert_eq!(parser.is_ok(), accepted, "type={ty:?}, length={len}");
                if let Ok(parser) = parser {
                    assert_eq!(parser.into_iter().count(), 1);
                }
            }
        }
    }

    #[test]
    fn long_data_type_validation_and_null_precedence() {
        for type_byte in 0..=255u8 {
            if let Ok(ty) = ColumnType::try_from(type_byte) {
                for null in [0, 1] {
                    for rebind in [false, true] {
                        let mut stmt = StatementData {
                            params: 1,
                            bound_types: vec![(ty, false)],
                            ..Default::default()
                        };
                        stmt.long_data.insert(0, b"uploaded".to_vec());
                        let mut wire = vec![null, u8::from(rebind)];
                        if rebind {
                            wire.extend_from_slice(&[type_byte, 0]);
                        }
                        let parser = ParamParser::new(&wire, &mut stmt);
                        assert_eq!(
                            parser.is_ok(),
                            (249..=254).contains(&type_byte),
                            "type={ty:?}, null={null}, rebind={rebind}"
                        );
                        if let Ok(parser) = parser {
                            let values: Vec<_> =
                                parser.into_iter().map(|p| p.value.into_inner()).collect();
                            assert_eq!(values, vec![crate::ValueInner::Bytes(b"uploaded")]);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn mixed_parameters_preserve_signedness_nulls_and_long_data() {
        let mut stmt = StatementData {
            params: 4,
            ..Default::default()
        };
        stmt.long_data.insert(2, b"uploaded".to_vec());
        // TINY signed, LONGLONG unsigned, BLOB long data, STRING NULL.
        let mut wire = vec![0b1000, 1, 1, 0, 8, 128, 252, 0, 254, 0, 255];
        wire.extend_from_slice(&u64::MAX.to_le_bytes());
        let values: Vec<_> = ParamParser::new(&wire, &mut stmt)
            .unwrap()
            .into_iter()
            .map(|p| p.value.into_inner())
            .collect();
        assert_eq!(
            values,
            vec![
                crate::ValueInner::Int(-1),
                crate::ValueInner::UInt(u64::MAX),
                crate::ValueInner::Bytes(b"uploaded"),
                crate::ValueInner::NULL
            ]
        );
    }

    #[test]
    fn null_bitmap_crosses_byte_boundary_without_shifting_parameters() {
        let mut stmt = StatementData {
            params: 9,
            bound_types: vec![(ColumnType::MYSQL_TYPE_TINY, false); 9],
            ..Default::default()
        };
        let wire = [0x80, 0x01, 0, 1, 2, 3, 4, 5, 6, 7];
        let values: Vec<_> = ParamParser::new(&wire, &mut stmt)
            .unwrap()
            .into_iter()
            .map(|p| p.value.into_inner())
            .collect();
        assert_eq!(values.len(), 9);
        for (i, value) in values[..7].iter().enumerate() {
            assert_eq!(*value, crate::ValueInner::Int(i as i64 + 1));
        }
        assert_eq!(
            &values[7..],
            &[crate::ValueInner::NULL, crate::ValueInner::NULL]
        );
    }

    #[test]
    fn every_truncated_prefix_of_mixed_execute_is_rejected() {
        // LONG + STRING; neither NULL nor supplied by SEND_LONG_DATA.
        let wire = [0, 1, 3, 0, 254, 0, 42, 0, 0, 0, 3, b'a', b'b', b'c'];
        for len in 0..wire.len() {
            let mut stmt = StatementData {
                params: 2,
                ..Default::default()
            };
            assert!(
                ParamParser::new(&wire[..len], &mut stmt).is_err(),
                "prefix {len}"
            );
        }
        let mut stmt = StatementData {
            params: 2,
            ..Default::default()
        };
        let values: Vec<_> = ParamParser::new(&wire, &mut stmt)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(i32::try_from(values[0].value).unwrap(), 42);
        assert_eq!(<&str>::try_from(values[1].value).unwrap(), "abc");
    }

    #[test]
    fn execute_parameters_reject_trailing_bytes() {
        let mut stmt = StatementData {
            params: 1,
            bound_types: vec![(ColumnType::MYSQL_TYPE_TINY, false)],
            ..Default::default()
        };
        assert!(ParamParser::new(&[0, 0, 42, 99], &mut stmt).is_err());
    }

    #[test]
    fn invalid_type_then_explicit_rebind_recovers_all_parameters() {
        let mut stmt = StatementData {
            params: 2,
            ..Default::default()
        };
        assert!(ParamParser::new(&[0, 0], &mut stmt).is_err());
        assert!(ParamParser::new(&[0, 1, 1, 0, 0x7f, 0], &mut stmt).is_err());
        let values: Vec<_> = ParamParser::new(&[0, 1, 1, 0, 1, 128, 254, 255], &mut stmt)
            .unwrap()
            .into_iter()
            .map(|p| p.value.into_inner())
            .collect();
        assert_eq!(
            values,
            [crate::ValueInner::Int(-2), crate::ValueInner::UInt(255)]
        );
        let values: Vec<_> = ParamParser::new(&[0, 0, 253, 254], &mut stmt)
            .unwrap()
            .into_iter()
            .map(|p| p.value.into_inner())
            .collect();
        assert_eq!(
            values,
            [crate::ValueInner::Int(-3), crate::ValueInner::UInt(254)]
        );
    }

    #[test]
    fn parses_execute_params_without_rebinding_types() {
        let mut stmt = StatementData {
            params: 1,
            bound_types: vec![(ColumnType::MYSQL_TYPE_LONG, false)],
            ..Default::default()
        };
        let parser = ParamParser::new(&[0x00, 0x00, 42, 0, 0, 0], &mut stmt).unwrap();
        let values: Vec<_> = parser.into_iter().collect();

        assert_eq!(values.len(), 1);
        assert_eq!(values[0].coltype, ColumnType::MYSQL_TYPE_LONG);
        assert_eq!(i32::try_from(values[0].value).unwrap(), 42);
    }

    #[test]
    fn rejects_truncated_execute_params() {
        let mut stmt = StatementData {
            params: 1,
            bound_types: vec![(ColumnType::MYSQL_TYPE_LONG, false)],
            ..Default::default()
        };

        let err = ParamParser::new(&[0x00], &mut stmt).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }
}
