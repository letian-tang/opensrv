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
                    let coltype = myc::constants::ColumnType::try_from(typmap[2 * i]).map_err(
                        |e| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("bad column type 0x{:x}: {}", typmap[2 * i], e),
                            )
                        },
                    )?;
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
            if (nullmap[byte] & (1u8 << (col % 8))) != 0 || self.long_data.contains_key(&col) {
                continue;
            }

            let (coltype, unsigned) = self.bound_types.get(col as usize).copied().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("missing bound type for parameter {}", col),
                )
            })?;
            Value::parse_from(&mut input, coltype, unsigned)?;
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

        let v = if let Some(data) = self.long_data.get(&self.col) {
            Value::bytes(&data[..])
        } else {
            Value::parse_from(&mut self.input, pt.0, pt.1).unwrap()
        };
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
