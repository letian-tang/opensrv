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

use crate::value::ToMysqlValue;
use crate::{Column, ColumnFlags, ColumnType};
use chrono::{self, TimeZone};
use std::time;

#[test]
fn signed_time_values_match_mysql_wire_encoding() {
    use myc::{
        io::ParseBuf,
        proto::{MyDeserialize, MySerialize},
        value::{BinValue, Value, ValueDeserializer},
    };
    let column = Column {
        table: String::new(),
        column: "v".into(),
        collen: 0,
        coltype: ColumnType::MYSQL_TYPE_TIME,
        colflags: ColumnFlags::empty(),
    };
    for (negative, days, hours, minutes, seconds, micros, expected) in [
        (true, 0, 0, 0, 0, 0, "00:00:00"),
        (true, 0, 0, 0, 0, 1, "-00:00:00.000001"),
        (true, 0, 0, 2, 3, 456789, "-00:02:03.456789"),
        (true, 0, 1, 2, 3, 0, "-01:02:03"),
        (true, 0, 23, 59, 59, 999999, "-23:59:59.999999"),
        (true, 1, 0, 0, 0, 0, "-24:00:00"),
        (true, 1, 1, 2, 3, 123456, "-25:02:03.123456"),
        (true, 34, 22, 59, 58, 999999, "-838:59:58.999999"),
        (false, 34, 22, 59, 58, 999999, "838:59:58.999999"),
        (true, 34, 22, 59, 59, 0, "-838:59:59"),
        (false, 34, 22, 59, 59, 0, "838:59:59"),
    ] {
        let value = Value::Time(negative, days, hours, minutes, seconds, micros);
        let mut text = Vec::new();
        value.to_mysql_text(&mut text).unwrap();
        assert_eq!(text[0] as usize, expected.len());
        assert_eq!(&text[1..], expected.as_bytes());
        let mut binary = Vec::new();
        value.to_mysql_bin(&mut binary, &column).unwrap();
        let mut expected_binary = Vec::new();
        value.serialize(&mut expected_binary);
        assert_eq!(binary, expected_binary);
        let mut input = ParseBuf(&binary);
        let decoded = ValueDeserializer::<BinValue>::deserialize(
            (column.coltype, column.colflags),
            &mut input,
        )
        .unwrap()
        .0;
        let expected_value = if expected == "00:00:00" {
            Value::Time(false, 0, 0, 0, 0, 0)
        } else {
            value
        };
        assert_eq!(decoded, expected_value);
        assert!(input.0.is_empty());
        if expected == "-25:02:03.123456" {
            assert_eq!(binary, [12, 1, 1, 0, 0, 0, 1, 2, 3, 64, 226, 1, 0]);
        }
    }
    for value in [
        Value::Time(true, 35, 0, 0, 0, 0),
        Value::Time(true, 0, 0, 60, 0, 0),
        Value::Time(true, 0, 0, 0, 0, 1_000_000),
    ] {
        assert!(value.to_mysql_text(&mut Vec::new()).is_err());
        assert!(value.to_mysql_bin(&mut Vec::new(), &column).is_err());
    }
}

#[test]
fn time_range_endpoint_checks_encoded_microseconds_before_writing() {
    use myc::value::Value;
    let column = Column {
        table: String::new(),
        column: "v".into(),
        collen: 0,
        coltype: ColumnType::MYSQL_TYPE_TIME,
        colflags: ColumnFlags::empty(),
    };
    const MAX_SECONDS: u64 = 838 * 3600 + 59 * 60 + 59;
    for micros in [1, 999_999] {
        for negative in [false, true] {
            let value = Value::Time(negative, 34, 22, 59, 59, micros);
            let mut text = vec![0xaa];
            let mut binary = vec![0xaa];
            assert!(value.to_mysql_text(&mut text).is_err());
            assert!(value.to_mysql_bin(&mut binary, &column).is_err());
            assert_eq!(text, [0xaa]);
            assert_eq!(binary, [0xaa]);
        }
        let value = time::Duration::new(MAX_SECONDS, micros * 1000);
        let mut text = vec![0xaa];
        let mut binary = vec![0xaa];
        assert!(value.to_mysql_text(&mut text).is_err());
        assert!(value.to_mysql_bin(&mut binary, &column).is_err());
        assert_eq!(text, [0xaa]);
        assert_eq!(binary, [0xaa]);
    }
    // Duration encoding truncates nanoseconds to microseconds, including at
    // the endpoint: a sub-microsecond remainder does not change the wire value.
    for nanos in [0, 1, 999] {
        let value = time::Duration::new(MAX_SECONDS, nanos);
        let mut text = Vec::new();
        let mut binary = Vec::new();
        value.to_mysql_text(&mut text).unwrap();
        value.to_mysql_bin(&mut binary, &column).unwrap();
        assert_eq!(text, b"\x09838:59:59");
        assert_eq!(binary, [8, 0, 34, 0, 0, 0, 22, 59, 59]);
    }
}

#[test]
fn owned_date_text_uses_column_type_through_reference_and_option() {
    use myc::value::Value;
    let column = Column {
        table: String::new(),
        column: "v".into(),
        collen: 0,
        coltype: ColumnType::MYSQL_TYPE_DATE,
        colflags: ColumnFlags::empty(),
    };
    for (value, expected) in [
        (Value::Date(2026, 9, 27, 0, 0, 0, 0), "2026-09-27"),
        (Value::Date(0, 0, 0, 0, 0, 0, 0), "0000-00-00"),
    ] {
        let mut output = Vec::new();
        Some(&value)
            .to_mysql_text_with_column(&mut output, &column)
            .unwrap();
        assert_eq!(output[0], 10);
        assert_eq!(&output[1..], expected.as_bytes());
    }
    let mut output = Vec::new();
    None::<Value>
        .to_mysql_text_with_column(&mut output, &column)
        .unwrap();
    assert_eq!(output, [0xfb]);
    output.clear();
    assert!(Value::Date(2026, 9, 27, 1, 0, 0, 0)
        .to_mysql_text_with_column(&mut output, &column)
        .is_err());
    assert!(output.is_empty());
}

#[test]
fn chrono_encoders_reject_out_of_mysql_range_without_writing() {
    use chrono::NaiveDate;
    let column = Column {
        table: String::new(),
        column: "v".into(),
        collen: 0,
        coltype: ColumnType::MYSQL_TYPE_DATE,
        colflags: ColumnFlags::empty(),
    };
    for year in [-1, 10_000, 65_536] {
        let date = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
        let mut text = Vec::new();
        let mut binary = Vec::new();
        assert!(date.to_mysql_text(&mut text).is_err());
        assert!(date.to_mysql_bin(&mut binary, &column).is_err());
        assert!(text.is_empty() && binary.is_empty());
        let timestamp = date.and_hms_opt(0, 0, 0).unwrap();
        let column = Column {
            coltype: ColumnType::MYSQL_TYPE_DATETIME,
            ..column.clone()
        };
        assert!(timestamp.to_mysql_text(&mut text).is_err());
        assert!(timestamp.to_mysql_bin(&mut binary, &column).is_err());
        assert!(text.is_empty() && binary.is_empty());
    }
    // chrono represents leap seconds with nanoseconds >= 1_000_000_000;
    // MySQL's fractional microsecond field cannot represent that value.
    let leap = NaiveDate::from_ymd_opt(2016, 12, 31)
        .unwrap()
        .and_hms_nano_opt(23, 59, 59, 1_500_000_000)
        .unwrap();
    let column = Column {
        coltype: ColumnType::MYSQL_TYPE_TIMESTAMP,
        ..column
    };
    assert!(leap.to_mysql_text(&mut Vec::new()).is_err());
    assert!(leap.to_mysql_bin(&mut Vec::new(), &column).is_err());
}

#[test]
fn platform_integers_obey_destination_ranges() {
    macro_rules! check {
        ($values:expr) => {
            for value in $values {
                for (coltype, width) in [
                    (ColumnType::MYSQL_TYPE_TINY, 1),
                    (ColumnType::MYSQL_TYPE_SHORT, 2),
                    (ColumnType::MYSQL_TYPE_YEAR, 2),
                    (ColumnType::MYSQL_TYPE_LONG, 4),
                    (ColumnType::MYSQL_TYPE_INT24, 4),
                    (ColumnType::MYSQL_TYPE_LONGLONG, 8),
                ] {
                    for unsigned in [false, true] {
                        let column = Column {
                            table: String::new(),
                            column: String::new(),
                            collen: 0,
                            coltype,
                            colflags: if unsigned {
                                ColumnFlags::UNSIGNED_FLAG
                            } else {
                                ColumnFlags::empty()
                            },
                        };
                        let bits = width * 8;
                        let min = if unsigned { 0 } else { -(1i128 << (bits - 1)) };
                        let max = (1i128 << (bits - usize::from(!unsigned))) - 1;
                        let number = value as i128;
                        let mut output = Vec::new();
                        let result = value.to_mysql_bin(&mut output, &column);
                        if (min..=max).contains(&number) {
                            result.unwrap();
                            assert_eq!(output, number.to_le_bytes()[..width]);
                        } else {
                            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
                            assert!(output.is_empty());
                        }
                    }
                }
            }
        };
    }
    check!([
        0usize,
        1,
        127,
        128,
        255,
        256,
        32767,
        32768,
        65535,
        65536,
        usize::MAX
    ]);
    check!([
        isize::MIN,
        -32769,
        -32768,
        -129,
        -128,
        -1,
        0,
        1,
        127,
        128,
        32767,
        32768,
        isize::MAX
    ]);
}

#[test]
fn mysql_date_values_encode_without_chrono_restrictions() {
    use myc::value::Value;
    let column = Column {
        table: String::new(),
        column: String::new(),
        collen: 0,
        coltype: ColumnType::MYSQL_TYPE_DATETIME,
        colflags: ColumnFlags::empty(),
    };
    for (value, text, binary) in [
        (
            Value::Date(0, 0, 0, 0, 0, 0, 0),
            "0000-00-00 00:00:00",
            vec![0],
        ),
        (
            Value::Date(2026, 0, 8, 0, 0, 0, 0),
            "2026-00-08 00:00:00",
            vec![4, 234, 7, 0, 8],
        ),
        (
            Value::Date(2026, 9, 8, 1, 2, 3, 123456),
            "2026-09-08 01:02:03.123456",
            vec![11, 234, 7, 9, 8, 1, 2, 3, 64, 226, 1, 0],
        ),
    ] {
        let mut output = Vec::new();
        value.to_mysql_text(&mut output).unwrap();
        assert_eq!(&output[1..], text.as_bytes());
        output.clear();
        value.to_mysql_bin(&mut output, &column).unwrap();
        assert_eq!(output, binary);
    }
    let date_column = Column {
        coltype: ColumnType::MYSQL_TYPE_DATE,
        ..column
    };
    let mut output = Vec::new();
    Value::Date(2026, 9, 8, 0, 0, 0, 0)
        .to_mysql_bin(&mut output, &date_column)
        .unwrap();
    assert_eq!(output, [4, 234, 7, 9, 8]);
    for value in [
        Value::Date(2026, 13, 1, 0, 0, 0, 0),
        Value::Date(2026, 1, 1, 24, 0, 0, 0),
        Value::Date(2026, 1, 1, 0, 0, 0, 1000000),
    ] {
        assert!(value.to_mysql_text(&mut Vec::new()).is_err());
        assert!(value.to_mysql_bin(&mut Vec::new(), &date_column).is_err());
    }
}

#[test]
fn invalid_numeric_signedness_and_time_range_return_errors() {
    let signed_tiny = Column {
        table: String::new(),
        column: String::new(),
        collen: 0,
        coltype: ColumnType::MYSQL_TYPE_TINY,
        colflags: ColumnFlags::empty(),
    };
    assert!(1u8.to_mysql_bin(&mut Vec::new(), &signed_tiny).is_err());

    let unsigned_bigint = Column {
        coltype: ColumnType::MYSQL_TYPE_LONGLONG,
        colflags: ColumnFlags::UNSIGNED_FLAG,
        ..signed_tiny
    };
    assert!(1i64
        .to_mysql_bin(&mut Vec::new(), &unsigned_bigint)
        .is_err());
    let unsigned_long = Column {
        coltype: ColumnType::MYSQL_TYPE_LONG,
        ..unsigned_bigint.clone()
    };
    let unsigned_short = Column {
        coltype: ColumnType::MYSQL_TYPE_SHORT,
        ..unsigned_bigint.clone()
    };
    for column in [&unsigned_bigint, &unsigned_long, &unsigned_short] {
        for value in [i8::MIN, -1] {
            assert!(value.to_mysql_bin(&mut Vec::new(), column).is_err());
        }
    }
    for column in [&unsigned_bigint, &unsigned_long] {
        for value in [i16::MIN, -1] {
            assert!(value.to_mysql_bin(&mut Vec::new(), column).is_err());
        }
    }
    for value in [i32::MIN, -1] {
        assert!(value
            .to_mysql_bin(&mut Vec::new(), &unsigned_bigint)
            .is_err());
    }

    for value in [0i8, i8::MAX] {
        for (column, expected) in [
            (
                &unsigned_bigint,
                u64::from(value as u8).to_le_bytes().to_vec(),
            ),
            (
                &unsigned_long,
                u32::from(value as u8).to_le_bytes().to_vec(),
            ),
            (
                &unsigned_short,
                u16::from(value as u8).to_le_bytes().to_vec(),
            ),
        ] {
            let mut encoded = Vec::new();
            value.to_mysql_bin(&mut encoded, column).unwrap();
            assert_eq!(encoded, expected);
        }
    }
    for value in [0i16, i16::MAX] {
        for (column, expected) in [
            (
                &unsigned_bigint,
                u64::from(value as u16).to_le_bytes().to_vec(),
            ),
            (
                &unsigned_long,
                u32::from(value as u16).to_le_bytes().to_vec(),
            ),
        ] {
            let mut encoded = Vec::new();
            value.to_mysql_bin(&mut encoded, column).unwrap();
            assert_eq!(encoded, expected);
        }
    }
    for value in [0i32, i32::MAX] {
        let mut encoded = Vec::new();
        value.to_mysql_bin(&mut encoded, &unsigned_bigint).unwrap();
        assert_eq!(encoded, u64::from(value as u32).to_le_bytes());
    }

    let time_column = Column {
        coltype: ColumnType::MYSQL_TYPE_TIME,
        colflags: ColumnFlags::empty(),
        ..unsigned_bigint
    };
    let too_large = time::Duration::from_secs(839 * 3600);
    assert!(too_large.to_mysql_text(&mut Vec::new()).is_err());
    assert!(too_large
        .to_mysql_bin(&mut Vec::new(), &time_column)
        .is_err());
}

mod roundtrip_text {
    use super::*;

    use myc::{
        io::ParseBuf,
        proto::MyDeserialize,
        value::{convert::FromValue, TextValue, ValueDeserializer},
    };

    macro_rules! rt {
        ($name:ident, $t:ty, $v:expr) => {
            #[test]
            fn $name() {
                let mut data = Vec::new();
                let v: $t = $v;
                v.to_mysql_text(&mut data).unwrap();
                let mut pb = ParseBuf(&data[..]);
                assert_eq!(
                    <$t>::from_value(
                        ValueDeserializer::<TextValue>::deserialize((), &mut pb)
                            .unwrap()
                            .0,
                    ),
                    v
                );
            }
        };
    }

    rt!(u8_one, u8, 1);
    rt!(i8_one, i8, 1);
    rt!(u16_one, u16, 1);
    rt!(i16_one, i16, 1);
    rt!(u32_one, u32, 1);
    rt!(i32_one, i32, 1);
    rt!(u64_one, u64, 1);
    rt!(i64_one, i64, 1);
    rt!(f32_one, f32, 1.0);
    rt!(f64_one, f64, 1.0);

    rt!(u8_max, u8, u8::MAX);
    rt!(i8_max, i8, i8::MAX);
    rt!(u16_max, u16, u16::MAX);
    rt!(i16_max, i16, i16::MAX);
    rt!(u32_max, u32, u32::MAX);
    rt!(i32_max, i32, i32::MAX);
    rt!(u64_max, u64, u64::MAX);
    rt!(i64_max, i64, i64::MAX);

    rt!(opt_none, Option<u8>, None);
    rt!(opt_some, Option<u8>, Some(1));

    rt!(time, chrono::NaiveDate, chrono::Local::now().date_naive());
    rt!(
        datetime,
        chrono::NaiveDateTime,
        chrono::Utc
            .with_ymd_and_hms(1989, 12, 7, 8, 0, 4)
            .unwrap()
            .naive_utc()
    );
    rt!(dur, time::Duration, time::Duration::from_secs(1893));
    rt!(dur_micro, time::Duration, time::Duration::new(1893, 5000));
    rt!(dur_zero, time::Duration, time::Duration::from_secs(0));
    rt!(bytes, Vec<u8>, vec![0x42, 0x00, 0x1a]);
    rt!(string, String, "foobar".to_owned());
}

mod roundtrip_bin {
    use super::*;

    use myc::{
        io::ParseBuf,
        proto::MyDeserialize,
        value::{convert::FromValue, BinValue, ValueDeserializer},
    };

    macro_rules! rt {
        ($name:ident, $t:ty, $v:expr, $ct:expr) => {
            rt!($name, $t, $v, $ct, false);
        };
        ($name:ident, $t:ty, $v:expr, $ct:expr, $sig:expr) => {
            #[test]
            fn $name() {
                let mut data = Vec::new();
                let mut col = Column {
                    table: String::new(),
                    column: String::new(),
                    collen: 0,
                    coltype: $ct,
                    colflags: ColumnFlags::empty(),
                };

                if !$sig {
                    col.colflags.insert(ColumnFlags::UNSIGNED_FLAG);
                }

                let v: $t = $v;
                v.to_mysql_bin(&mut data, &col).unwrap();
                let mut pb = ParseBuf(&data[..]);
                assert_eq!(
                    <$t>::from_value(
                        ValueDeserializer::<BinValue>::deserialize(
                            (
                                $ct,
                                if $sig {
                                    ColumnFlags::empty()
                                } else {
                                    ColumnFlags::UNSIGNED_FLAG
                                }
                            ),
                            &mut pb
                        )
                        .unwrap()
                        .0,
                    ),
                    v
                );
            }
        };
    }

    rt!(u8_one, u8, 1, ColumnType::MYSQL_TYPE_TINY, false);
    rt!(i8_one, i8, 1, ColumnType::MYSQL_TYPE_TINY, true);
    rt!(u8_one_short, u8, 1, ColumnType::MYSQL_TYPE_SHORT, false);
    rt!(i8_one_short, i8, 1, ColumnType::MYSQL_TYPE_SHORT, true);
    rt!(u8_one_long, u8, 1, ColumnType::MYSQL_TYPE_LONG, false);
    rt!(i8_one_long, i8, 1, ColumnType::MYSQL_TYPE_LONG, true);
    rt!(
        u8_one_longlong,
        u8,
        1,
        ColumnType::MYSQL_TYPE_LONGLONG,
        false
    );
    rt!(
        i8_one_longlong,
        i8,
        1,
        ColumnType::MYSQL_TYPE_LONGLONG,
        true
    );
    rt!(u16_one, u16, 1, ColumnType::MYSQL_TYPE_SHORT, false);
    rt!(i16_one, i16, 1, ColumnType::MYSQL_TYPE_SHORT, true);
    rt!(u16_one_long, u16, 1, ColumnType::MYSQL_TYPE_LONG, false);
    rt!(i16_one_long, i16, 1, ColumnType::MYSQL_TYPE_LONG, true);
    rt!(
        u16_one_longlong,
        u16,
        1,
        ColumnType::MYSQL_TYPE_LONGLONG,
        false
    );
    rt!(
        i16_one_longlong,
        i16,
        1,
        ColumnType::MYSQL_TYPE_LONGLONG,
        true
    );
    rt!(u32_one_long, u32, 1, ColumnType::MYSQL_TYPE_LONG, false);
    rt!(i32_one_long, i32, 1, ColumnType::MYSQL_TYPE_LONG, true);
    rt!(
        u32_one_longlong,
        u32,
        1,
        ColumnType::MYSQL_TYPE_LONGLONG,
        false
    );
    rt!(
        i32_one_longlong,
        i32,
        1,
        ColumnType::MYSQL_TYPE_LONGLONG,
        true
    );
    rt!(u64_one, u64, 1, ColumnType::MYSQL_TYPE_LONGLONG, false);
    rt!(i64_one, i64, 1, ColumnType::MYSQL_TYPE_LONGLONG, true);

    rt!(f32_one, f32, 1.0, ColumnType::MYSQL_TYPE_FLOAT, false);
    rt!(f64_one, f64, 1.0, ColumnType::MYSQL_TYPE_DOUBLE, false);

    rt!(u8_max, u8, u8::MAX, ColumnType::MYSQL_TYPE_TINY, false);
    rt!(i8_max, i8, i8::MAX, ColumnType::MYSQL_TYPE_TINY, true);
    rt!(u16_max, u16, u16::MAX, ColumnType::MYSQL_TYPE_SHORT, false);
    rt!(i16_max, i16, i16::MAX, ColumnType::MYSQL_TYPE_SHORT, true);
    rt!(u32_max, u32, u32::MAX, ColumnType::MYSQL_TYPE_LONG, false);
    rt!(i32_max, i32, i32::MAX, ColumnType::MYSQL_TYPE_LONG, true);
    rt!(
        u64_max,
        u64,
        u64::MAX,
        ColumnType::MYSQL_TYPE_LONGLONG,
        false
    );
    rt!(
        i64_max,
        i64,
        i64::MAX,
        ColumnType::MYSQL_TYPE_LONGLONG,
        true
    );

    rt!(opt_some, Option<u8>, Some(1), ColumnType::MYSQL_TYPE_TINY);

    rt!(
        time,
        chrono::NaiveDate,
        chrono::Local::now().date_naive(),
        ColumnType::MYSQL_TYPE_DATE
    );
    rt!(
        datetime,
        chrono::NaiveDateTime,
        chrono::Utc
            .with_ymd_and_hms(1989, 12, 7, 8, 0, 4)
            .unwrap()
            .naive_utc(),
        ColumnType::MYSQL_TYPE_DATETIME
    );
    rt!(
        dur,
        time::Duration,
        time::Duration::from_secs(1893),
        ColumnType::MYSQL_TYPE_TIME
    );
    rt!(
        bytes,
        Vec<u8>,
        vec![0x42, 0x00, 0x1a],
        ColumnType::MYSQL_TYPE_BLOB
    );
    rt!(
        string,
        String,
        "foobar".to_owned(),
        ColumnType::MYSQL_TYPE_STRING
    );
}
