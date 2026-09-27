//! Loopback-only, deterministic protocol fixture for mysql/tests/connector-j.
use mysql_common::value::Value as OwnedValue;
use opensrv_mysql::*;
use std::{collections::HashMap, io};
use tokio::{io::AsyncWrite, net::TcpListener};

struct Backend {
    next_id: u32,
    statements: HashMap<u32, Prepared>,
    status: StatusFlags,
    commits: u64,
}
#[derive(Clone, Copy)]
enum Prepared {
    Echo,
    Metadata,
    Temporal,
    NegativeTemporal,
    TemporalEcho,
}
impl Default for Backend {
    fn default() -> Self {
        Self {
            next_id: 0,
            statements: HashMap::new(),
            status: StatusFlags::SERVER_STATUS_AUTOCOMMIT,
            commits: 0,
        }
    }
}
fn column(name: &str, ty: ColumnType) -> Column {
    Column {
        table: String::new(),
        column: name.into(),
        collen: 0,
        coltype: ty,
        colflags: ColumnFlags::empty(),
    }
}
fn echo_columns() -> [Column; 3] {
    [
        column("id", ColumnType::MYSQL_TYPE_LONGLONG),
        column("text", ColumnType::MYSQL_TYPE_VAR_STRING),
        column("number", ColumnType::MYSQL_TYPE_LONGLONG),
    ]
}
fn metadata_columns() -> ([Column; 4], [ColumnMetadata; 4]) {
    let mut text = column("binary_collation_text", ColumnType::MYSQL_TYPE_VAR_STRING);
    text.colflags = ColumnFlags::BINARY_FLAG;
    let mut blob = column("text", ColumnType::MYSQL_TYPE_BLOB);
    blob.colflags = ColumnFlags::BLOB_FLAG;
    let mut bytes = column("bytes", ColumnType::MYSQL_TYPE_BLOB);
    bytes.colflags = ColumnFlags::BLOB_FLAG | ColumnFlags::BINARY_FLAG;
    (
        [
            text,
            blob,
            bytes,
            column("decimal", ColumnType::MYSQL_TYPE_NEWDECIMAL),
        ],
        [
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
                decimals: 0,
            },
            ColumnMetadata {
                collation: Some(63),
                decimals: 2,
            },
        ],
    )
}

fn temporal_columns() -> ([Column; 3], [ColumnMetadata; 3]) {
    (
        [
            column("date", ColumnType::MYSQL_TYPE_DATE),
            column("datetime", ColumnType::MYSQL_TYPE_DATETIME),
            column("time", ColumnType::MYSQL_TYPE_TIME),
        ],
        [
            ColumnMetadata {
                collation: Some(63),
                decimals: 0,
            },
            ColumnMetadata {
                collation: Some(63),
                decimals: 6,
            },
            ColumnMetadata {
                collation: Some(63),
                decimals: 6,
            },
        ],
    )
}

async fn temporal_result<W: AsyncWrite + Unpin>(
    results: QueryResultWriter<'_, W>,
    negative: bool,
) -> io::Result<()> {
    let (cols, metadata) = temporal_columns();
    let mut rows = results.start_with_metadata(&cols, &metadata).await?;
    for row in [
        [
            OwnedValue::Date(2026, 9, 27, 0, 0, 0, 0),
            OwnedValue::Date(2026, 9, 27, 1, 2, 3, 123456),
            OwnedValue::Time(negative, 1, 1, 2, 3, 123456),
        ],
        [
            OwnedValue::Date(0, 0, 0, 0, 0, 0, 0),
            OwnedValue::Date(0, 0, 0, 0, 0, 0, 0),
            OwnedValue::Time(false, 0, 0, 0, 0, 0),
        ],
        [
            OwnedValue::Date(9999, 12, 31, 0, 0, 0, 0),
            OwnedValue::Date(9999, 12, 31, 23, 59, 59, 999999),
            OwnedValue::Time(negative, 34, 22, 59, 58, 999999),
        ],
    ] {
        rows.write_row(row).await?;
    }
    if negative {
        for value in [
            OwnedValue::Time(true, 0, 0, 0, 0, 1),
            OwnedValue::Time(true, 0, 0, 2, 3, 456789),
            OwnedValue::Time(true, 0, 1, 2, 3, 0),
            OwnedValue::Time(true, 0, 23, 59, 59, 999999),
            OwnedValue::Time(true, 1, 0, 0, 0, 0),
            OwnedValue::Time(true, 34, 22, 59, 59, 0),
            OwnedValue::Time(true, 0, 0, 0, 0, 0),
            OwnedValue::NULL,
        ] {
            rows.write_row([
                OwnedValue::Date(0, 0, 0, 0, 0, 0, 0),
                OwnedValue::Date(0, 0, 0, 0, 0, 0, 0),
                value,
            ])
            .await?;
        }
    }
    rows.finish().await
}
async fn metadata_result<W: AsyncWrite + Unpin>(
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    let (cols, metadata) = metadata_columns();
    let mut rows = results.start_with_metadata(&cols, &metadata).await?;
    rows.write_row([
        "中文🙂".as_bytes(),
        "文本🙂".as_bytes(),
        &[0, 0xff, 0x80],
        b"12.34",
    ])
    .await?;
    rows.finish().await
}
fn variable(name: &str) -> Option<&'static str> {
    Some(match name {
        "auto_increment_increment" => "1",
        "character_set_client"
        | "character_set_connection"
        | "character_set_results"
        | "character_set_server" => "utf8mb4",
        "collation_server" | "collation_connection" => "utf8mb4_general_ci",
        "init_connect" | "sql_mode" => "",
        "interactive_timeout" | "wait_timeout" => "28800",
        "lower_case_table_names" => "0",
        "max_allowed_packet" => "67108864",
        "net_write_timeout" => "60",
        "performance_schema" => "0",
        "query_cache_size" | "query_cache_type" => "0",
        "system_time_zone" | "time_zone" => "UTC",
        "transaction_isolation" | "tx_isolation" => "REPEATABLE-READ",
        "transaction_read_only" | "tx_read_only" => "0",
        "autocommit" => "1",
        "license" => "Apache-2.0",
        _ => return None,
    })
}
#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for Backend {
    type Error = io::Error;
    fn version(&self) -> String {
        "8.0.36-opensrv-test".into()
    }
    async fn authenticate(&self, _: &str, user: &[u8], _: &[u8], data: &[u8]) -> bool {
        user == b"test" && data.is_empty()
    }
    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        let kind = match query.trim() {
            "SELECT ?, ?, ?" => Prepared::Echo,
            "SELECT metadata" => Prepared::Metadata,
            "SELECT temporal" => Prepared::Temporal,
            "SELECT negative_temporal" => Prepared::NegativeTemporal,
            "SELECT temporal(?, ?, ?)" => Prepared::TemporalEcho,
            _ => {
                return info
                    .error(
                        ErrorKind::ER_NOT_SUPPORTED_YET,
                        b"unsupported fixture prepare",
                    )
                    .await
            }
        };
        self.next_id += 1;
        self.statements.insert(self.next_id, kind);
        if let Prepared::Metadata = kind {
            let (cols, metadata) = metadata_columns();
            return info
                .reply_with_metadata(self.next_id, &[], &cols, &[], &metadata)
                .await;
        }
        if matches!(
            kind,
            Prepared::Temporal | Prepared::NegativeTemporal | Prepared::TemporalEcho
        ) {
            let (cols, metadata) = temporal_columns();
            return if matches!(kind, Prepared::TemporalEcho) {
                info.reply_with_metadata(self.next_id, &cols, &cols, &metadata, &metadata)
                    .await
            } else {
                info.reply_with_metadata(self.next_id, &[], &cols, &[], &metadata)
                    .await
            };
        }
        let cols = echo_columns();
        info.reply(self.next_id, &cols, &cols).await
    }
    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        match self.statements[&id] {
            Prepared::Metadata => return metadata_result(results).await,
            Prepared::Temporal => return temporal_result(results, false).await,
            Prepared::NegativeTemporal => return temporal_result(results, true).await,
            Prepared::TemporalEcho => {
                let mut params = params.into_iter();
                let date = params.next().unwrap().value;
                let datetime = params.next().unwrap().value;
                let time = params.next().unwrap().value;
                let date = if date.is_null() {
                    None
                } else {
                    Some(chrono::NaiveDate::try_from(date)?)
                };
                let datetime = if datetime.is_null() {
                    None
                } else {
                    Some(chrono::NaiveDateTime::try_from(datetime)?)
                };
                let time = if time.is_null() {
                    None
                } else {
                    Some(std::time::Duration::try_from(time)?)
                };
                let (cols, metadata) = temporal_columns();
                let mut rows = results.start_with_metadata(&cols, &metadata).await?;
                rows.write_col(date)?;
                rows.write_col(datetime)?;
                rows.write_col(time)?;
                rows.end_row().await?;
                return rows.finish().await;
            }
            Prepared::Echo => {}
        }
        let values: Vec<_> = params
            .into_iter()
            .map(|p| match p.value.into_inner() {
                ValueInner::NULL => OwnedValue::NULL,
                ValueInner::Int(n) => OwnedValue::Int(n),
                ValueInner::UInt(n) => OwnedValue::Int(i64::try_from(n).unwrap()),
                ValueInner::Bytes(b) => OwnedValue::Bytes(b.to_vec()),
                other => panic!("unexpected fixture parameter {other:?}"),
            })
            .collect();
        let cols = echo_columns();
        let mut rows = results.start(&cols).await?;
        rows.write_row(values).await?;
        rows.finish().await
    }
    async fn on_close(&mut self, id: u32) {
        self.statements.remove(&id);
    }
    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        let mut q = query.trim();
        if q.starts_with("/*") {
            q = q.split_once("*/").map(|(_, tail)| tail.trim()).unwrap_or(q);
        }
        let lower = q.to_ascii_lowercase();
        if lower == "select metadata" {
            return metadata_result(results).await;
        }
        if lower == "select temporal" {
            return temporal_result(results, false).await;
        }
        if lower == "select negative_temporal" {
            return temporal_result(results, true).await;
        }
        if lower == "select commits" {
            let cols = [column("commits", ColumnType::MYSQL_TYPE_LONGLONG)];
            let mut rows = results.start(&cols).await?;
            rows.write_row([self.commits]).await?;
            return rows.finish().await;
        }
        if lower == "set autocommit=0" {
            self.status.remove(StatusFlags::SERVER_STATUS_AUTOCOMMIT);
        } else if lower == "set autocommit=1" {
            self.status = StatusFlags::SERVER_STATUS_AUTOCOMMIT;
        } else if lower == "begin" {
            self.status.insert(StatusFlags::SERVER_STATUS_IN_TRANS);
        } else if lower == "commit" || lower == "rollback" {
            if lower == "commit" {
                self.commits += 1;
            }
            self.status.remove(StatusFlags::SERVER_STATUS_IN_TRANS);
        }
        if lower.starts_with("select ") && lower.contains("@@") {
            let mut cols = Vec::new();
            let mut values = Vec::new();
            for expression in lower[7..].split(',') {
                let pieces: Vec<_> = expression.split_whitespace().collect();
                let name = pieces[0]
                    .trim_start_matches('@')
                    .rsplit('.')
                    .next()
                    .unwrap();
                let alias = pieces.last().unwrap().trim_matches('`');
                let Some(value) = variable(name) else {
                    eprintln!("unsupported fixture variable: {name}");
                    return results
                        .error(ErrorKind::ER_UNKNOWN_SYSTEM_VARIABLE, name.as_bytes())
                        .await;
                };
                cols.push(column(alias, ColumnType::MYSQL_TYPE_VAR_STRING));
                values.push(value);
            }
            let mut rows = results.start(&cols).await?;
            rows.write_row(values).await?;
            return rows.finish().await;
        }
        if lower.starts_with("set ") || lower == "rollback" || lower == "commit" || lower == "begin"
        {
            return results
                .completed_with_status(OkResponse {
                    status_flags: self.status,
                    ..Default::default()
                })
                .await;
        }
        let cols = [column("value", ColumnType::MYSQL_TYPE_VAR_STRING)];
        if q == "SELECT unicode" {
            let mut rows = results.start(&cols).await?;
            rows.write_row(["中文🙂"]).await?;
            return rows.finish().await;
        }
        if let Some(marker) = q.strip_prefix("SELECT marker_") {
            let mut rows = results.start(&cols).await?;
            rows.write_row([marker]).await?;
            return rows.finish().await;
        }
        if q == "SELECT large" || q == "SELECT stream" {
            let cols = [column("payload", ColumnType::MYSQL_TYPE_BLOB)];
            let mut rows = results.start(&cols).await?;
            let data = vec![
                b'x';
                if q == "SELECT large" {
                    U24_MAX + 1
                } else {
                    64 * 1024
                }
            ];
            for _ in 0..if q == "SELECT large" { 1 } else { 10000 } {
                rows.write_row([&data]).await?;
            }
            return rows.finish().await;
        }
        eprintln!("unsupported fixture SQL: {query}");
        results
            .error(ErrorKind::ER_NOT_SUPPORTED_YET, b"unsupported fixture SQL")
            .await
    }
}
#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    println!("READY {}", listener.local_addr()?.port());
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let (r, w) = stream.into_split();
            let options = IntermediaryOptions {
                max_packet_size: Some(64 * 1024 * 1024),
                ..Default::default()
            };
            let _ = AsyncMysqlIntermediary::run_with_options(
                Backend::default(),
                r,
                tokio::io::BufWriter::new(w),
                &options,
            )
            .await;
        });
    }
}
