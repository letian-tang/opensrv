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

use std::io;

use byteorder::{LittleEndian, ReadBytesExt};

use crate::myc::constants::ColumnType;
use crate::myc::io::ReadMysqlExt;

/// MySQL value as provided when executing prepared statements.
#[derive(Debug, PartialEq, Copy, Clone)]
pub struct Value<'a>(ValueInner<'a>);

/// A representation of a concrete, typed MySQL value.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ValueInner<'a> {
    /// The MySQL `NULL` value.
    NULL,
    /// An untyped sequence of bytes (usually a text type or `MYSQL_TYPE_BLOB`).
    Bytes(&'a [u8]),
    /// A signed integer.
    Int(i64),
    /// An unsigned integer.
    UInt(u64),
    /// A floating point number.
    Double(f64),
    /// A [binary encoding](https://mariadb.com/kb/en/library/resultset-row/#date-binary-encoding)
    /// of a `MYSQL_TYPE_DATE`.
    Date(&'a [u8]),
    /// A [binary encoding](https://mariadb.com/kb/en/library/resultset-row/#time-binary-encoding)
    /// of a `MYSQL_TYPE_TIME`.
    Time(&'a [u8]),
    /// A [binary
    /// encoding](https://mariadb.com/kb/en/library/resultset-row/#timestamp-binary-encoding) of a
    /// `MYSQL_TYPE_TIMESTAMP` or `MYSQL_TYPE_DATETIME`.
    Datetime(&'a [u8]),
}

impl<'a> Value<'a> {
    /// Return the inner stored representation of this value.
    ///
    /// This may be useful for when you do not care about the exact data type used for a column,
    /// but instead wish to introspect a value you are given at runtime. Note that the contained
    /// value may be stored in a type that is more general than what the corresponding parameter
    /// type allows (e.g., a `u8` will be stored as an `u64`).
    pub fn into_inner(self) -> ValueInner<'a> {
        self.0
    }

    pub(crate) fn null() -> Self {
        Value(ValueInner::NULL)
    }

    /// Returns true if this is a NULL value
    pub fn is_null(&self) -> bool {
        matches!(self.0, ValueInner::NULL)
    }

    pub(crate) fn parse_from(
        input: &mut &'a [u8],
        ct: ColumnType,
        unsigned: bool,
    ) -> io::Result<Self> {
        ValueInner::parse_from(input, ct, unsigned).map(Value)
    }

    pub(crate) fn bytes(input: &'a [u8]) -> Value<'a> {
        Value(ValueInner::Bytes(input))
    }
}

macro_rules! read_bytes {
    ($input:expr, $len:expr) => {
        if $len as usize > $input.len() {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "EOF while reading length-encoded string",
            ))
        } else {
            let (bits, rest) = $input.split_at($len as usize);
            *$input = rest;
            Ok(bits)
        }
    };
}

impl<'a> ValueInner<'a> {
    fn parse_from(input: &mut &'a [u8], ct: ColumnType, unsigned: bool) -> io::Result<Self> {
        match ct {
            ColumnType::MYSQL_TYPE_STRING
            | ColumnType::MYSQL_TYPE_VAR_STRING
            | ColumnType::MYSQL_TYPE_BLOB
            | ColumnType::MYSQL_TYPE_TINY_BLOB
            | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
            | ColumnType::MYSQL_TYPE_LONG_BLOB
            | ColumnType::MYSQL_TYPE_SET
            | ColumnType::MYSQL_TYPE_ENUM
            | ColumnType::MYSQL_TYPE_DECIMAL
            | ColumnType::MYSQL_TYPE_VARCHAR
            | ColumnType::MYSQL_TYPE_BIT
            | ColumnType::MYSQL_TYPE_NEWDECIMAL
            | ColumnType::MYSQL_TYPE_GEOMETRY
            | ColumnType::MYSQL_TYPE_JSON => {
                let len = input.read_lenenc_int()?;
                Ok(ValueInner::Bytes(read_bytes!(input, len)?))
            }
            ColumnType::MYSQL_TYPE_TINY => {
                if unsigned {
                    Ok(ValueInner::UInt(u64::from(input.read_u8()?)))
                } else {
                    Ok(ValueInner::Int(i64::from(input.read_i8()?)))
                }
            }
            ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                if unsigned {
                    Ok(ValueInner::UInt(u64::from(
                        input.read_u16::<LittleEndian>()?,
                    )))
                } else {
                    Ok(ValueInner::Int(i64::from(
                        input.read_i16::<LittleEndian>()?,
                    )))
                }
            }
            ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                if unsigned {
                    Ok(ValueInner::UInt(u64::from(
                        input.read_u32::<LittleEndian>()?,
                    )))
                } else {
                    Ok(ValueInner::Int(i64::from(
                        input.read_i32::<LittleEndian>()?,
                    )))
                }
            }
            ColumnType::MYSQL_TYPE_LONGLONG => {
                if unsigned {
                    Ok(ValueInner::UInt(input.read_u64::<LittleEndian>()?))
                } else {
                    Ok(ValueInner::Int(input.read_i64::<LittleEndian>()?))
                }
            }
            ColumnType::MYSQL_TYPE_FLOAT => {
                let f = input.read_f32::<LittleEndian>()?;
                Ok(ValueInner::Double(f64::from(f)))
            }
            ColumnType::MYSQL_TYPE_DOUBLE => {
                Ok(ValueInner::Double(input.read_f64::<LittleEndian>()?))
            }
            ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_DATETIME => {
                let len = input.read_u8()?;
                Ok(ValueInner::Datetime(read_bytes!(input, len)?))
            }
            ColumnType::MYSQL_TYPE_DATE => {
                let len = input.read_u8()?;
                Ok(ValueInner::Date(read_bytes!(input, len)?))
            }
            ColumnType::MYSQL_TYPE_TIME => {
                let len = input.read_u8()?;
                Ok(ValueInner::Time(read_bytes!(input, len)?))
            }
            ColumnType::MYSQL_TYPE_NULL => Ok(ValueInner::NULL),
            ct => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown column type {:?}", ct),
            )),
        }
    }
}

// NOTE: these are now TryFrom to avoid panics on invalid data
macro_rules! impl_try_into_int {
    ($t:ty, $($variant:path),*) => {
        impl<'a> std::convert::TryFrom<Value<'a>> for $t {
            type Error = io::Error;
            fn try_from(val: Value<'a>) -> Result<Self, Self::Error> {
                match val.0 {
                    $($variant(v) => v.try_into().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, format!("value out of bounds for {}", stringify!($t))))),*,
                    v => Err(io::Error::new(io::ErrorKind::InvalidData, format!("invalid type conversion from {:?} to {}", v, stringify!($t))))
                }
            }
        }
    }
}

macro_rules! impl_try_into_float {
    ($t:ty, $($variant:path),*) => {
        impl<'a> std::convert::TryFrom<Value<'a>> for $t {
            type Error = io::Error;
            fn try_from(val: Value<'a>) -> Result<Self, Self::Error> {
                match val.0 {
                    $($variant(v) => Ok(v as $t)),*,
                    v => Err(io::Error::new(io::ErrorKind::InvalidData, format!("invalid type conversion from {:?} to {}", v, stringify!($t))))
                }
            }
        }
    }
}

impl_try_into_int!(u8, ValueInner::UInt, ValueInner::Int);
impl_try_into_int!(u16, ValueInner::UInt, ValueInner::Int);
impl_try_into_int!(u32, ValueInner::UInt, ValueInner::Int);
impl_try_into_int!(u64, ValueInner::UInt, ValueInner::Int);
impl_try_into_int!(i8, ValueInner::UInt, ValueInner::Int);
impl_try_into_int!(i16, ValueInner::UInt, ValueInner::Int);
impl_try_into_int!(i32, ValueInner::UInt, ValueInner::Int);
impl_try_into_int!(i64, ValueInner::UInt, ValueInner::Int);
impl_try_into_float!(f32, ValueInner::Double);
impl_try_into_float!(f64, ValueInner::Double);

impl<'a> std::convert::TryFrom<Value<'a>> for &'a [u8] {
    type Error = io::Error;
    fn try_from(val: Value<'a>) -> Result<Self, Self::Error> {
        match val.0 {
            ValueInner::Bytes(v) => Ok(v),
            v => Err(io::Error::new(io::ErrorKind::InvalidData, format!("invalid type conversion from {:?} to bytes", v))),
        }
    }
}

impl<'a> std::convert::TryFrom<Value<'a>> for &'a str {
    type Error = io::Error;
    fn try_from(val: Value<'a>) -> Result<Self, Self::Error> {
        if let ValueInner::Bytes(v) = val.0 {
            ::std::str::from_utf8(v).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("invalid utf8: {}", e)))
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidData, format!("invalid type conversion from {:?} to string", val)))
        }
    }
}

use chrono::{NaiveDate, NaiveDateTime};
impl<'a> std::convert::TryFrom<Value<'a>> for NaiveDate {
    type Error = io::Error;
    fn try_from(val: Value<'a>) -> Result<Self, Self::Error> {
        if let ValueInner::Date(mut v) = val.0 {
            if v.len() != 4 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid date length"));
            }
            let y = i32::from(v.read_u16::<LittleEndian>()?);
            let m = u32::from(v.read_u8()?);
            let d = u32::from(v.read_u8()?);
            NaiveDate::from_ymd_opt(y, m, d)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid date format"))
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidData, format!("invalid type conversion from {:?} to date", val)))
        }
    }
}

impl<'a> std::convert::TryFrom<Value<'a>> for NaiveDateTime {
    type Error = io::Error;
    fn try_from(val: Value<'a>) -> Result<Self, Self::Error> {
        to_naive_datetime(val)
    }
}

pub fn to_naive_datetime(val: Value) -> Result<NaiveDateTime, io::Error> {
    let ValueInner::Datetime(v) = val.0 else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid type conversion from {:?} to datetime", val),
        ));
    };

    let len = v.len();

    let v = &mut io::Cursor::new(v);

    // unwrap safety: guarded by `v.len()` check
    fn read_ymd(v: &mut io::Cursor<&[u8]>) -> (i32, u32, u32) {
        let y = i32::from(v.read_u16::<LittleEndian>().unwrap());
        let m = u32::from(v.read_u8().unwrap());
        let d = u32::from(v.read_u8().unwrap());
        (y, m, d)
    }

    // unwrap safety: guarded by `v.len()` check
    fn read_hms(v: &mut io::Cursor<&[u8]>) -> (u32, u32, u32) {
        let h = u32::from(v.read_u8().unwrap());
        let m = u32::from(v.read_u8().unwrap());
        let s = u32::from(v.read_u8().unwrap());
        (h, m, s)
    }

    // Timestamp binary encoding:
    // https://mariadb.com/kb/en/resultset-row/#timestamp-binary-encoding
    let d = match len {
        0 => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "'0000-00-00 00:00:00' is a valid timestamp value but not representable by NaiveDateTime!",
            ))
        }
        4 => {
            let (y, m, d) = read_ymd(v);
            NaiveDate::from_ymd_opt(y, m, d).and_then(|x| x.and_hms_opt(0, 0, 0))
        }
        7 => {
            let (y, m, d) = read_ymd(v);
            NaiveDate::from_ymd_opt(y, m, d).and_then(|x| {
                let (h, m, s) = read_hms(v);
                x.and_hms_opt(h, m, s)
            })
        }
        11 => {
            let (y, m, d) = read_ymd(v);
            NaiveDate::from_ymd_opt(y, m, d).and_then(|x| {
                let (h, m, s) = read_hms(v);

                // unwrap safety: guarded by `v.len()` check
                let us = v.read_u32::<LittleEndian>().unwrap();

                x.and_hms_micro_opt(h, m, s, us)
            })
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("illegal timestamp value length: {}", len),
            ))
        }
    };

    d.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid data conversion from {:?} to datetime", val),
        )
    })
}

use std::time::Duration;

impl<'a> std::convert::TryFrom<Value<'a>> for Duration {
    type Error = io::Error;
    fn try_from(val: Value<'a>) -> Result<Self, Self::Error> {
        if let ValueInner::Time(mut v) = val.0 {
            if !v.is_empty() && v.len() != 8 && v.len() != 12 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid time length"));
            }

            if v.is_empty() {
                return Ok(Duration::from_secs(0));
            }

            let neg = v.read_u8()?;
            if neg != 0u8 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "negative time not supported as Duration"));
            }

            let days = u64::from(v.read_u32::<LittleEndian>()?);
            let hours = u64::from(v.read_u8()?);
            let minutes = u64::from(v.read_u8()?);
            let seconds = u64::from(v.read_u8()?);
            let micros = if v.len() == 12 {
                v.read_u32::<LittleEndian>()?
            } else {
                0
            };

            Ok(Duration::new(
                days * 86_400 + hours * 3_600 + minutes * 60 + seconds,
                micros * 1_000,
            ))
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidData, format!("invalid type conversion from {:?} to duration", val)))
        }
    }
}
