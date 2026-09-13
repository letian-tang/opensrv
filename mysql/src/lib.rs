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

extern crate mysql_common as myc;

use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};
use tokio::time::timeout;
#[cfg(feature = "tls")]
use tokio_rustls::rustls::ServerConfig;

pub use crate::myc::constants::{CapabilityFlags, ColumnFlags, ColumnType, StatusFlags};
pub use crate::myc::scramble::{scramble_native, scramble_sha256};
#[cfg(feature = "tls")]
pub use crate::tls::{plain_run_with_options, secure_run_with_options};

mod commands;
mod errorcodes;
mod packet_reader;
mod packet_writer;
mod params;
mod resultset;
#[cfg(feature = "tls")]
mod tls;
mod value;
mod writers;

#[cfg(test)]
mod tests;

// max payload size 2^(24-1)
pub const U24_MAX: usize = 16_777_215;

/// Meta-information abot a single column, used either to describe a prepared statement parameter
/// or an output column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// This column's associated table.
    ///
    /// Note that this is *technically* the table's alias.
    pub table: String,
    /// This column's name.
    ///
    /// Note that this is *technically* the column's alias.
    pub column: String,
    /// Column length (in bytes) reported through COLUMN_DEFINITION41.
    /// 0 means "use default".
    pub collen: u32,
    /// This column's type>
    pub coltype: ColumnType,
    /// Any flags associated with this column.
    ///
    /// Of particular interest are `ColumnFlags::UNSIGNED_FLAG` and `ColumnFlags::NOT_NULL_FLAG`.
    pub colflags: ColumnFlags,
}

/// QueryStatusInfo represents the status of a query.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OkResponse {
    /// header
    pub header: u8,
    /// affected rows in update/insert
    pub affected_rows: u64,
    /// insert_id in update/insert
    pub last_insert_id: u64,
    /// StatusFlags associated with this query
    pub status_flags: StatusFlags,
    /// Warnings
    pub warnings: u16,
    /// Extra infomation
    pub info: String,
    /// session state change information
    pub session_state_info: String,
}

pub use crate::errorcodes::ErrorKind;
pub use crate::params::{ParamParser, ParamValue, Params};
pub use crate::resultset::{InitWriter, QueryResultWriter, RowWriter, StatementMetaWriter};
pub use crate::value::{decode::to_naive_datetime, ToMysqlValue, Value, ValueInner};
use crate::{
    commands::ClientHandshake,
    packet_reader::{PacketReader, DEFAULT_MAX_PACKET_SIZE},
    packet_writer::PacketWriter,
};

const SCRAMBLE_SIZE: usize = 20;
const CACHING_SHA2_DIGEST_LENGTH: usize = 32;
pub const MYSQL_NATIVE_PASSWORD: &str = "mysql_native_password";
pub const CACHING_SHA2_PASSWORD: &str = "caching_sha2_password";
static NEXT_CONNECTION_ID: AtomicU32 = AtomicU32::new(1);

/// Generate a fresh authentication challenge suitable for a MySQL handshake.
pub fn generate_scramble() -> io::Result<[u8; SCRAMBLE_SIZE]> {
    let mut scramble = [0; SCRAMBLE_SIZE];
    getrandom::fill(&mut scramble).map_err(io::Error::other)?;
    for byte in &mut scramble {
        if *byte == b'\0' || *byte == b'$' {
            *byte = byte.wrapping_add(1);
        }
    }
    Ok(scramble)
}

fn ensure_response_completed(completion: &Arc<AtomicBool>, command: &str) -> io::Result<()> {
    if completion.load(Ordering::Acquire) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("backend returned without completing {command} response"),
        ))
    }
}

fn command_parse_error(packet: &[u8]) -> (ErrorKind, String) {
    if packet.is_empty() {
        (
            ErrorKind::ER_MALFORMED_PACKET,
            "malformed command packet".to_string(),
        )
    } else {
        use crate::myc::constants::Command as CommandByte;
        let cmd = packet[0];
        let is_known = matches!(
            cmd,
            x if x == CommandByte::COM_QUERY as u8
                || x == CommandByte::COM_FIELD_LIST as u8
                || x == CommandByte::COM_INIT_DB as u8
                || x == CommandByte::COM_STMT_PREPARE as u8
                || x == CommandByte::COM_STMT_EXECUTE as u8
                || x == CommandByte::COM_STMT_SEND_LONG_DATA as u8
                || x == CommandByte::COM_STMT_CLOSE as u8
                || x == CommandByte::COM_STMT_RESET as u8
                || x == CommandByte::COM_RESET_CONNECTION as u8
                || x == CommandByte::COM_QUIT as u8
                || x == CommandByte::COM_PING as u8
        );

        if is_known {
            (
                ErrorKind::ER_MALFORMED_PACKET,
                format!("malformed packet for command: 0x{:02x}", cmd),
            )
        } else {
            (
                ErrorKind::ER_UNKNOWN_COM_ERROR,
                format!("unsupported command: 0x{:02x}", cmd),
            )
        }
    }
}

pub fn verify_mysql_native_password(password: &[u8], salt: &[u8], auth_data: &[u8]) -> bool {
    match scramble_native(salt, password) {
        Some(expected) => auth_data == expected,
        None => auth_data.is_empty(),
    }
}

pub fn verify_caching_sha2_password(password: &[u8], salt: &[u8], auth_data: &[u8]) -> bool {
    let auth_data = if auth_data == [0] { &[][..] } else { auth_data };
    match scramble_sha256(salt, password) {
        Some(expected) => auth_data == expected,
        None => auth_data.is_empty(),
    }
}

pub fn verify_auth_plugin_data(
    auth_plugin: &str,
    password: &[u8],
    salt: &[u8],
    auth_data: &[u8],
) -> bool {
    match auth_plugin {
        MYSQL_NATIVE_PASSWORD => verify_mysql_native_password(password, salt, auth_data),
        CACHING_SHA2_PASSWORD => verify_caching_sha2_password(password, salt, auth_data),
        _ => false,
    }
}

#[async_trait]
/// Implementors of this async-trait can be used to drive a MySQL-compatible database backend.
pub trait AsyncMysqlShim<W: Send> {
    /// The error type produced by operations on this shim.
    ///
    /// Must implement `From<io::Error>` so that transport-level errors can be lifted.
    type Error: From<io::Error>;

    /// Server version
    fn version(&self) -> String {
        // 5.1.10 because that's what Ruby's ActiveRecord requires
        "5.1.10-alpha-msql-proxy".to_string()
    }

    /// Connection id
    fn connect_id(&self) -> u32 {
        NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed)
    }

    /// get auth plugin
    fn default_auth_plugin(&self) -> &str {
        MYSQL_NATIVE_PASSWORD
    }

    /// get auth plugin
    async fn auth_plugin_for_username(&self, _user: &[u8]) -> &'static str {
        MYSQL_NATIVE_PASSWORD
    }

    /// Default salt(scramble) for auth plugin
    fn salt(&self) -> [u8; SCRAMBLE_SIZE] {
        // Authentication is denied by default. If the OS RNG is unavailable, return an
        // unusable challenge rather than panic in a connection task.
        generate_scramble().unwrap_or([0; SCRAMBLE_SIZE])
    }

    /// Authenticate using the specified plugin.
    ///
    /// `caching_sha2_password` is supported only through its fast-authentication
    /// exchange. Return `false` if full authentication would be required.
    async fn authenticate(
        &self,
        _auth_plugin: &str,
        _username: &[u8],
        _salt: &[u8],
        _auth_data: &[u8],
    ) -> bool {
        false
    }

    /// Called when the client issues a request to prepare `query` for later execution.
    ///
    /// The provided [`StatementMetaWriter`](struct.StatementMetaWriter.html) should be used to
    /// notify the client of the statement id assigned to the prepared statement, as well as to
    /// give metadata about the types of parameters and returned columns.
    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), Self::Error>;

    /// Called when the client executes a previously prepared statement.
    ///
    /// Any parameters included with the client's command is given in `params`.
    /// A response to the query should be given using the provided
    /// [`QueryResultWriter`](struct.QueryResultWriter.html).
    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error>;

    /// Called when the client wishes to deallocate resources associated with a previously prepared
    /// statement.
    async fn on_close<'a>(&'a mut self, stmt: u32)
    where
        W: 'async_trait;

    /// Reset backend session state for `COM_RESET_CONNECTION`.
    ///
    /// Return `true` only after transactions, session variables, and other connection-local
    /// state have been restored. The default keeps the command unsupported rather than falsely
    /// acknowledging an incomplete reset.
    async fn on_reset_connection(&mut self) -> Result<bool, Self::Error> {
        Ok(false)
    }

    /// Called when the client issues a query for immediate execution.
    ///
    /// Results should be returned using the given
    /// [`QueryResultWriter`](struct.QueryResultWriter.html).
    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error>;

    /// Called when the client issues a system variable query (e.g., `SELECT @@version_comment`).
    ///
    /// By default, it handles `max_allowed_packet` and `version_comment`, and delegates
    /// other variables to `on_query`. Implementors can override this to seamlessly handle
    /// driver-specific initialization variables.
    async fn on_system_variable<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error>
    where
        W: tokio::io::AsyncWrite + Send + std::marker::Unpin,
    {
        let q = query.to_lowercase();
        let var = &q["select @@".len()..];
        if var == "max_allowed_packet" {
            let cols = &[Column {
                table: String::new(),
                column: "@@max_allowed_packet".to_string(),
                collen: 0,
                coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
            }];
            let mut w = results.start(cols).await?;
            w.write_row(std::iter::once(67108864u32)).await?;
            w.finish().await?;
        } else {
            self.on_query(query, results).await?;
        }
        Ok(())
    }

    /// Called when client switches database.
    async fn on_init<'a>(
        &'a mut self,
        _: &'a str,
        writer: InitWriter<'a, W>,
    ) -> Result<(), Self::Error>
    where
        W: AsyncWrite + Unpin,
    {
        writer.ok().await.map_err(Into::into)
    }
}

/// The options which passed to AsyncMysqlIntermediary struct
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntermediaryOptions {
    /// process use statement on the on_query handler
    pub process_use_statement_on_query: bool,
    /// reject connection if dbname not provided
    pub reject_connection_on_dbname_absence: bool,
    /// Optional read buffer size for buffered convenience entrypoints.
    pub read_buffer_size: Option<usize>,
    /// Optional write buffer size for buffered convenience entrypoints.
    pub write_buffer_size: Option<usize>,
    /// Hard protocol-layer ceiling for a single incoming or outgoing logical packet.
    /// This also bounds accumulated `COM_STMT_SEND_LONG_DATA` bytes per statement.
    pub max_packet_size: Option<usize>,
    /// Optional timeout applied when waiting for the next client packet after authentication.
    pub read_timeout: Option<Duration>,
    /// Optional timeout applied during handshake and authentication packet exchange.
    pub auth_timeout: Option<Duration>,
    /// Optional timeout applied to packet writes and flushes.
    pub write_timeout: Option<Duration>,
    /// Flush underlying buffered writers once a packet payload reaches this threshold.
    pub write_high_watermark: Option<usize>,
    /// Maximum number of prepared statements retained by one connection.
    pub max_prepared_statements: Option<usize>,
    /// Aggregate `COM_STMT_SEND_LONG_DATA` bytes retained by one connection.
    pub max_connection_long_data_size: Option<usize>,
    /// Status flags advertised in the initial handshake.
    pub initial_status_flags: StatusFlags,
    /// Character set/collation advertised in the initial handshake.
    pub initial_collation: u8,
    /// Advertise and enforce support for multiple result sets.
    pub enable_multi_results: bool,
}

impl Default for IntermediaryOptions {
    fn default() -> Self {
        Self {
            process_use_statement_on_query: false,
            reject_connection_on_dbname_absence: false,
            read_buffer_size: None,
            write_buffer_size: None,
            max_packet_size: None,
            read_timeout: None,
            auth_timeout: None,
            write_timeout: Some(Duration::from_secs(60)),
            write_high_watermark: None,
            max_prepared_statements: None,
            max_connection_long_data_size: None,
            initial_status_flags: StatusFlags::SERVER_STATUS_AUTOCOMMIT,
            initial_collation: myc::constants::UTF8MB4_GENERAL_CI as u8,
            enable_multi_results: true,
        }
    }
}

#[derive(Default)]
struct StatementData {
    long_data: HashMap<u16, Vec<u8>>,
    bound_types: Vec<(myc::constants::ColumnType, bool)>,
    params: u16,
    pending_error: Option<(ErrorKind, String)>,
}

const AUTH_PLUGIN_DATA_PART_1_LENGTH: usize = 8;

/// A server that speaks the MySQL/MariaDB protocol, and can delegate client commands to a backend
/// that implements [`AsyncMysqlShim`](trait.AsyncMysqlShim.html).
pub struct AsyncMysqlIntermediary<B, S: AsyncRead + Unpin, W> {
    pub(crate) client_capabilities: CapabilityFlags,
    process_use_statement_on_query: bool,
    reject_connection_on_dbname_absence: bool,
    read_timeout: Option<Duration>,
    auth_timeout: Option<Duration>,
    max_long_data_size: usize,
    max_connection_long_data_size: usize,
    max_prepared_statements: usize,
    status_flags: StatusFlags,
    shim: B,
    reader: packet_reader::PacketReader<S>,
    writer: packet_writer::PacketWriter<W>,
}

/// Configuration used to craft the initial server handshake before a shim is available.
#[derive(Clone, Debug)]
pub struct ServerHandshakeConfig {
    pub version: String,
    pub connection_id: u32,
    pub default_auth_plugin: String,
    pub scramble: [u8; SCRAMBLE_SIZE],
}

impl<B, R, W> AsyncMysqlIntermediary<B, R, W>
where
    W: AsyncWrite + Send + Unpin,
    B: AsyncMysqlShim<W> + Send + Sync,
    R: AsyncRead + Send + Unpin,
{
    /// Create a new server over two one-way channels and process client commands until the client
    /// disconnects or an error occurs.
    pub async fn run_on(shim: B, stream: R, output_stream: W) -> Result<(), B::Error> {
        Self::run_with_options(shim, stream, output_stream, &Default::default()).await
    }

    /// Create a new server over two one-way channels and process client commands until the client
    /// disconnects or an error occurs, with config options.
    pub async fn run_with_options(
        mut shim: B,
        input_stream: R,
        mut output_stream: W,
        opts: &IntermediaryOptions,
    ) -> Result<(), B::Error> {
        let process_use_statement_on_query = opts.process_use_statement_on_query;
        let reject_connection_on_dbname_absence = opts.reject_connection_on_dbname_absence;
        let max_long_data_size = opts.max_packet_size.unwrap_or(DEFAULT_MAX_PACKET_SIZE);
        let max_connection_long_data_size = opts
            .max_connection_long_data_size
            .unwrap_or(max_long_data_size);
        let max_prepared_statements = opts.max_prepared_statements.unwrap_or(16_382);
        let (_, (handshake, seq, client_capabilities, input_stream)) =
            AsyncMysqlIntermediary::init_before_ssl_with_options(
                &mut shim,
                input_stream,
                &mut output_stream,
                opts,
                #[cfg(feature = "tls")]
                &None,
            )
            .await?;

        let reader = PacketReader::new_with_max_packet_size(
            input_stream,
            opts.max_packet_size.unwrap_or(DEFAULT_MAX_PACKET_SIZE),
        );
        let mut writer = PacketWriter::new(output_stream);
        writer.set_flush_threshold(opts.write_high_watermark.unwrap_or(64 * 1024));
        writer.set_max_packet_size(opts.max_packet_size.unwrap_or(DEFAULT_MAX_PACKET_SIZE));
        writer.set_write_timeout(opts.write_timeout);

        let mut mi = AsyncMysqlIntermediary {
            client_capabilities,
            process_use_statement_on_query,
            reject_connection_on_dbname_absence,
            read_timeout: opts.read_timeout,
            auth_timeout: opts.auth_timeout,
            max_long_data_size,
            max_connection_long_data_size,
            max_prepared_statements,
            status_flags: opts.initial_status_flags,
            shim,
            reader,
            writer,
        };
        mi.init_after_ssl(handshake, seq).await?;
        mi.run().await
    }

    pub async fn init_before_ssl(
        shim: &mut B,
        input_stream: R,
        output_stream: &mut W,
        #[cfg(feature = "tls")] tls_conf: &Option<std::sync::Arc<ServerConfig>>,
    ) -> Result<
        (
            bool,
            (ClientHandshake, u8, CapabilityFlags, PacketReader<R>),
        ),
        B::Error,
    > {
        Self::init_before_ssl_with_options(
            shim,
            input_stream,
            output_stream,
            &IntermediaryOptions::default(),
            #[cfg(feature = "tls")]
            tls_conf,
        )
        .await
    }

    pub async fn init_before_ssl_with_options(
        shim: &mut B,
        input_stream: R,
        output_stream: &mut W,
        opts: &IntermediaryOptions,
        #[cfg(feature = "tls")] tls_conf: &Option<std::sync::Arc<ServerConfig>>,
    ) -> Result<
        (
            bool,
            (ClientHandshake, u8, CapabilityFlags, PacketReader<R>),
        ),
        B::Error,
    > {
        let config = ServerHandshakeConfig {
            version: shim.version(),
            connection_id: shim.connect_id(),
            default_auth_plugin: shim.default_auth_plugin().to_string(),
            scramble: shim.salt(),
        };

        AsyncMysqlIntermediary::<B, R, W>::init_before_ssl_with_config_and_options(
            &config,
            input_stream,
            output_stream,
            opts,
            #[cfg(feature = "tls")]
            tls_conf,
        )
        .await
        .map_err(B::Error::from)
    }

    pub async fn init_before_ssl_with_config(
        config: &ServerHandshakeConfig,
        input_stream: R,
        output_stream: &mut W,
        #[cfg(feature = "tls")] tls_conf: &Option<std::sync::Arc<ServerConfig>>,
    ) -> io::Result<(
        bool,
        (ClientHandshake, u8, CapabilityFlags, PacketReader<R>),
    )> {
        Self::init_before_ssl_with_config_and_options(
            config,
            input_stream,
            output_stream,
            &IntermediaryOptions::default(),
            #[cfg(feature = "tls")]
            tls_conf,
        )
        .await
    }

    pub async fn init_before_ssl_with_config_and_options(
        config: &ServerHandshakeConfig,
        input_stream: R,
        output_stream: &mut W,
        opts: &IntermediaryOptions,
        #[cfg(feature = "tls")] tls_conf: &Option<std::sync::Arc<ServerConfig>>,
    ) -> io::Result<(
        bool,
        (ClientHandshake, u8, CapabilityFlags, PacketReader<R>),
    )> {
        if config.scramble.iter().any(|byte| matches!(*byte, 0 | b'$')) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "authentication challenge contains a protocol delimiter",
            ));
        }
        let mut reader = PacketReader::new_with_max_packet_size(
            input_stream,
            opts.max_packet_size.unwrap_or(DEFAULT_MAX_PACKET_SIZE),
        );
        let mut writer = PacketWriter::new(output_stream);
        writer.set_flush_threshold(opts.write_high_watermark.unwrap_or(64 * 1024));
        writer.set_max_packet_size(opts.max_packet_size.unwrap_or(DEFAULT_MAX_PACKET_SIZE));
        writer.set_write_timeout(opts.write_timeout);
        // https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeV10
        writer.write_all(&[10])?; // protocol 10

        writer.write_all(config.version.as_bytes())?;
        writer.write_all(&[0x00])?;

        // connection_id (4 bytes)
        writer.write_all(&config.connection_id.to_le_bytes())?;

        let mut server_capabilities = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_PLUGIN_AUTH
            | CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
            | CapabilityFlags::CLIENT_CONNECT_WITH_DB
            | CapabilityFlags::CLIENT_SESSION_TRACK
            | CapabilityFlags::CLIENT_DEPRECATE_EOF;

        if opts.enable_multi_results {
            server_capabilities |=
                CapabilityFlags::CLIENT_MULTI_RESULTS | CapabilityFlags::CLIENT_PS_MULTI_RESULTS;
        }

        #[cfg(feature = "tls")]
        let server_capabilities = if tls_conf.is_some() {
            server_capabilities | CapabilityFlags::CLIENT_SSL
        } else {
            server_capabilities
        };

        let server_capabilities_vec = server_capabilities.bits().to_le_bytes();
        let default_auth_plugin = config.default_auth_plugin.as_str();
        let scramble = &config.scramble;

        writer.write_all(&scramble[0..AUTH_PLUGIN_DATA_PART_1_LENGTH])?; // auth-plugin-data-part-1
        writer.write_all(&[0x00])?;

        writer.write_all(&server_capabilities_vec[..2])?; // The lower 2 bytes of the Capabilities Flags, 0x42
                                                          // self.writer.write_all(&[0x00, 0x42])?;
        writer.write_all(&[opts.initial_collation])?;
        writer.write_all(&opts.initial_status_flags.bits().to_le_bytes())?;
        writer.write_all(&server_capabilities_vec[2..4])?; // The upper 2 bytes of the Capabilities Flags

        if default_auth_plugin.is_empty() {
            // no plugins
            writer.write_all(&[0x00])?;
        } else {
            writer.write_all(&((scramble.len() + 1) as u8).to_le_bytes())?; // length of the combined auth_plugin_data(scramble), if auth_plugin_data_len is > 0
        }
        writer.write_all(&[0x00; 10][..])?; // 10 bytes filler

        // Part2 of the auth_plugin_data
        // $len=MAX(13, length of auth-plugin-data - 8)
        writer.write_all(&scramble[AUTH_PLUGIN_DATA_PART_1_LENGTH..])?; // 12 bytes
        writer.write_all(&[0x00])?;

        // Plugin name
        writer.write_all(default_auth_plugin.as_bytes())?;
        writer.write_all(&[0x00])?;
        writer.end_packet().await?;
        writer.flush_all().await?;

        let (seq, handshake) = read_packet_with_timeout(&mut reader, opts.auth_timeout)
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "peer terminated connection",
                )
            })?;

        if handshake.first_sequence_id() != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected initial handshake sequence",
            ));
        }
        let mut handshake = commands::client_handshake(&handshake, false)
            .map_err(|e| match e {
                nom::Err::Incomplete(_) => io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "client sent incomplete handshake",
                ),
                nom::Err::Failure(nom_error) | nom::Err::Error(nom_error) => {
                    if let nom::error::ErrorKind::Eof = nom_error.code {
                        io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!(
                                "client did not complete handshake; got {:?}",
                                nom_error.input
                            ),
                        )
                    } else {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "bad client handshake; got {:?} ({:?})",
                                nom_error.input, nom_error.code
                            ),
                        )
                    }
                }
            })?
            .1;

        handshake.server_scramble = Some(config.scramble);
        writer.set_seq(seq.wrapping_add(1));

        #[cfg(not(feature = "tls"))]
        if handshake.capabilities.contains(CapabilityFlags::CLIENT_SSL) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "client requested SSL despite us not advertising support for it",
            )
            .into());
        }

        #[cfg(feature = "tls")]
        if handshake.capabilities.contains(CapabilityFlags::CLIENT_SSL) {
            return Ok((true, (handshake, seq, server_capabilities, reader)));
        }

        Ok((false, (handshake, seq, server_capabilities, reader)))
    }

    pub async fn init_after_ssl(
        &mut self,
        #[cfg(feature = "tls")] mut handshake: ClientHandshake,
        #[cfg(not(feature = "tls"))] handshake: ClientHandshake,
        mut seq: u8,
    ) -> Result<(), B::Error> {
        let scramble = handshake.server_scramble.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "missing server handshake challenge",
            )
        })?;
        #[cfg(feature = "tls")]
        if handshake.capabilities.contains(CapabilityFlags::CLIENT_SSL) {
            let (_seq, hs) = read_packet_with_timeout(&mut self.reader, self.auth_timeout)
                .await?
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "peer terminated connection",
                    )
                })?;
            if hs.first_sequence_id() != seq.wrapping_add(1) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected TLS handshake sequence",
                )
                .into());
            }
            seq = _seq;

            handshake = commands::client_handshake(&hs, true)
                .map_err(|e| match e {
                    nom::Err::Incomplete(_) => io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "client sent incomplete handshake",
                    ),
                    nom::Err::Failure(nom_error) | nom::Err::Error(nom_error) => {
                        if let nom::error::ErrorKind::Eof = nom_error.code {
                            io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                format!(
                                    "client did not complete handshake; got {:?}",
                                    nom_error.input
                                ),
                            )
                        } else {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "bad client handshake; got {:?} ({:?})",
                                    nom_error.input, nom_error.code
                                ),
                            )
                        }
                    }
                })?
                .1;

            self.writer.set_seq(seq.wrapping_add(1));
        }

        {
            if !handshake
                .capabilities
                .contains(CapabilityFlags::CLIENT_PROTOCOL_41)
            {
                let err = io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "Required capability: CLIENT_PROTOCOL_41, please upgrade your MySQL client version",
                );
                return Err(err.into());
            }

            self.client_capabilities &= handshake.capabilities;
            let mut auth_response = handshake.auth_response.clone();
            if let Some(username) = &handshake.username {
                let auth_plugin_expect = self.shim.auth_plugin_for_username(username).await;

                // auth switch
                if !auth_plugin_expect.is_empty()
                    && handshake.auth_plugin != auth_plugin_expect.as_bytes()
                {
                    self.writer.set_seq(seq.wrapping_add(1));
                    self.writer.write_all(&[0xfe])?;
                    self.writer.write_all(auth_plugin_expect.as_bytes())?;
                    self.writer.write_all(&[0x00])?;
                    self.writer.write_all(&scramble)?;
                    self.writer.write_all(&[0x00])?;

                    self.writer.end_packet().await?;
                    self.writer.flush_all().await?;

                    {
                        let (rseq, auth_response_data) =
                            read_packet_with_timeout(&mut self.reader, self.auth_timeout)
                                .await?
                                .ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::ConnectionAborted,
                                        "peer terminated connection",
                                    )
                                })?;

                        if auth_response_data.first_sequence_id() != seq.wrapping_add(2) {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "unexpected auth-switch sequence",
                            )
                            .into());
                        }
                        seq = rseq;
                        auth_response = auth_response_data.to_vec();
                    }
                }

                self.writer.set_seq(seq.wrapping_add(1));

                if auth_plugin_expect == CACHING_SHA2_PASSWORD && auth_response == [0] {
                    auth_response.clear();
                }

                if auth_plugin_expect == CACHING_SHA2_PASSWORD
                    && !auth_response.is_empty()
                    && auth_response.len() != CACHING_SHA2_DIGEST_LENGTH
                {
                    let err_msg = format!(
                        "Authenticate failed, user: {:?}, auth_plugin: {:?}",
                        String::from_utf8_lossy(username),
                        auth_plugin_expect,
                    );
                    writers::write_err(
                        ErrorKind::ER_ACCESS_DENIED_NO_PASSWORD_ERROR,
                        err_msg.as_bytes(),
                        &mut self.writer,
                    )
                    .await?;
                    self.writer.flush_all().await?;
                    return Err(io::Error::new(io::ErrorKind::PermissionDenied, err_msg).into());
                }

                if !self
                    .shim
                    .authenticate(
                        auth_plugin_expect,
                        username,
                        &scramble,
                        auth_response.as_slice(),
                    )
                    .await
                {
                    let err_msg = format!(
                        "Authenticate failed, user: {:?}, auth_plugin: {:?}",
                        String::from_utf8_lossy(username),
                        auth_plugin_expect,
                    );
                    writers::write_err(
                        ErrorKind::ER_ACCESS_DENIED_NO_PASSWORD_ERROR,
                        err_msg.as_bytes(),
                        &mut self.writer,
                    )
                    .await?;
                    self.writer.flush_all().await?;
                    return Err(io::Error::new(io::ErrorKind::PermissionDenied, err_msg).into());
                }

                if auth_plugin_expect == CACHING_SHA2_PASSWORD
                    && auth_response.len() == CACHING_SHA2_DIGEST_LENGTH
                {
                    // A valid caching_sha2_password scramble completes only the fast-auth
                    // phase. The protocol requires AuthMoreData(0x03) before the final OK.
                    self.writer.write_all(&[0x01, 0x03])?;
                    self.writer.end_packet().await?;
                }

                let mut needs_default_ok = true;

                if let Some(db_bytes) = handshake.db.as_ref() {
                    let db = match std::str::from_utf8(db_bytes) {
                        Ok(db) => db,
                        Err(_) => {
                            writers::write_err(
                                ErrorKind::ER_MALFORMED_PACKET,
                                b"initial database is not valid utf-8",
                                &mut self.writer,
                            )
                            .await?;
                            self.writer.flush_all().await?;
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "initial database is not valid utf-8",
                            )
                            .into());
                        }
                    };
                    {
                        let completion = Arc::new(AtomicBool::new(false));
                        let w = InitWriter {
                            client_capabilities: self.client_capabilities,
                            status_flags: self.status_flags,
                            writer: &mut self.writer,
                            completion: Arc::clone(&completion),
                        };
                        self.shim.on_init(db, w).await?;
                        ensure_response_completed(&completion, "initial database")?;
                        needs_default_ok = false;
                    }
                } else if self.reject_connection_on_dbname_absence {
                    writers::write_err(
                        ErrorKind::ER_DATABASE_NAME,
                        "database required on connection".as_bytes(),
                        &mut self.writer,
                    )
                    .await?;
                    self.writer.flush_all().await?;
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "database name requried: please add db name to the connection",
                    )
                    .into());
                }

                if needs_default_ok {
                    writers::write_ok_packet(
                        &mut self.writer,
                        self.client_capabilities,
                        OkResponse {
                            status_flags: self.status_flags,
                            ..Default::default()
                        },
                    )
                    .await?;
                }
            }

            self.writer.flush_all().await?;
        };

        Ok(())
    }

    async fn run(mut self) -> Result<(), B::Error> {
        use crate::commands::Command;

        let mut stmts: HashMap<u32, _> = HashMap::new();
        let mut total_long_data_size = 0usize;
        while let Some((seq, packet)) =
            read_packet_with_timeout(&mut self.reader, self.read_timeout).await?
        {
            if packet.first_sequence_id() != 0 {
                self.writer.set_seq(seq.wrapping_add(1));
                writers::write_err(
                    ErrorKind::ER_MALFORMED_PACKET,
                    b"command packet sequence id must start at 0",
                    &mut self.writer,
                )
                .await?;
                self.writer.flush_all().await?;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "command packet sequence id must start at 0",
                )
                .into());
            }
            self.writer.set_seq(seq.wrapping_add(1));
            let res = commands::parse(&packet);
            match res {
                Ok(cmd) => {
                    match cmd.1 {
                        Command::Query(q) => {
                            let q_str = match ::std::str::from_utf8(q) {
                                Ok(s) => s,
                                Err(e) => {
                                    writers::write_err(
                                        ErrorKind::ER_MALFORMED_PACKET,
                                        format!("query is not valid utf-8: {}", e).as_bytes(),
                                        &mut self.writer,
                                    )
                                    .await?;
                                    self.writer.flush_all().await?;
                                    continue;
                                }
                            };
                            if q_str.starts_with("SELECT @@") || q_str.starts_with("select @@") {
                                let (w, completion) = QueryResultWriter::new_tracked(
                                    &mut self.writer,
                                    false,
                                    self.client_capabilities,
                                    self.status_flags,
                                );
                                self.shim.on_system_variable(q_str, w).await?;
                                ensure_response_completed(&completion, "query")?;
                            } else if !self.process_use_statement_on_query
                                && (q_str.starts_with("USE ") || q_str.starts_with("use "))
                            {
                                let completion = Arc::new(AtomicBool::new(false));
                                let w = InitWriter {
                                    client_capabilities: self.client_capabilities,
                                    status_flags: self.status_flags,
                                    writer: &mut self.writer,
                                    completion: Arc::clone(&completion),
                                };
                                let schema = &q_str["USE ".len()..];
                                let schema = schema.trim().trim_end_matches(';').trim_matches('`');
                                self.shim.on_init(schema, w).await?;
                                ensure_response_completed(&completion, "USE")?;
                            } else {
                                let (w, completion) = QueryResultWriter::new_tracked(
                                    &mut self.writer,
                                    false,
                                    self.client_capabilities,
                                    self.status_flags,
                                );
                                self.shim.on_query(q_str, w).await?;
                                ensure_response_completed(&completion, "query")?;
                            }
                        }
                        Command::Prepare(q) => {
                            if stmts.len() >= self.max_prepared_statements {
                                writers::write_err(
                                    ErrorKind::ER_MAX_PREPARED_STMT_COUNT_REACHED,
                                    format!(
                                        "maximum prepared statement count reached: {}",
                                        self.max_prepared_statements
                                    )
                                    .as_bytes(),
                                    &mut self.writer,
                                )
                                .await?;
                                self.writer.flush_all().await?;
                                continue;
                            }
                            let q_str = match ::std::str::from_utf8(q) {
                                Ok(s) => s,
                                Err(e) => {
                                    writers::write_err(
                                        ErrorKind::ER_MALFORMED_PACKET,
                                        format!("prepare query is not valid utf-8: {}", e)
                                            .as_bytes(),
                                        &mut self.writer,
                                    )
                                    .await?;
                                    self.writer.flush_all().await?;
                                    continue;
                                }
                            };
                            let completion = Arc::new(AtomicBool::new(false));
                            let w = StatementMetaWriter {
                                writer: &mut self.writer,
                                stmts: &mut stmts,
                                client_capabilities: self.client_capabilities,
                                completion: Arc::clone(&completion),
                            };

                            self.shim.on_prepare(q_str, w).await?;
                            ensure_response_completed(&completion, "prepare")?;
                        }
                        Command::Execute {
                            stmt,
                            flags,
                            iteration_count,
                            params,
                        } => {
                            if flags != 0 || iteration_count != 1 {
                                writers::write_err(
                                    ErrorKind::ER_UNSUPPORTED_PS,
                                    format!(
                                        "unsupported COM_STMT_EXECUTE flags ({}) or iteration count ({})",
                                        flags, iteration_count
                                    )
                                    .as_bytes(),
                                    &mut self.writer,
                                )
                                .await?;
                                self.writer.flush_all().await?;
                                continue;
                            }
                            let state = match stmts.get_mut(&stmt) {
                                Some(s) => s,
                                None => {
                                    writers::write_err(
                                        ErrorKind::ER_UNKNOWN_STMT_HANDLER,
                                        format!("unknown statement {}", stmt).as_bytes(),
                                        &mut self.writer,
                                    )
                                    .await?;
                                    self.writer.flush_all().await?;
                                    continue;
                                }
                            };
                            let retained_long_data = state
                                .long_data
                                .values()
                                .fold(0usize, |total, value| total.saturating_add(value.len()));
                            if let Some((kind, message)) = state.pending_error.as_ref() {
                                writers::write_err(*kind, message.as_bytes(), &mut self.writer)
                                    .await?;
                            } else {
                                let params = match params::ParamParser::new(params, state) {
                                    Ok(p) => p,
                                    Err(e) => {
                                        state.long_data.clear();
                                        let kind = if e.get_ref().is_some_and(|cause| {
                                            cause.is::<params::IncompatibleLongData>()
                                        }) {
                                            ErrorKind::ER_WRONG_ARGUMENTS
                                        } else {
                                            ErrorKind::ER_MALFORMED_PACKET
                                        };
                                        writers::write_err(
                                            kind,
                                            format!("invalid execute parameters: {}", e).as_bytes(),
                                            &mut self.writer,
                                        )
                                        .await?;
                                        self.writer.flush_all().await?;
                                        total_long_data_size =
                                            total_long_data_size.saturating_sub(retained_long_data);
                                        continue;
                                    }
                                };
                                let (w, completion) = QueryResultWriter::new_tracked(
                                    &mut self.writer,
                                    true,
                                    self.client_capabilities,
                                    self.status_flags,
                                );
                                self.shim.on_execute(stmt, params, w).await?;
                                ensure_response_completed(&completion, "execute")?;
                                state.long_data.clear();
                                total_long_data_size =
                                    total_long_data_size.saturating_sub(retained_long_data);
                            }
                        }
                        Command::SendLongData { stmt, param, data } => {
                            // COM_STMT_SEND_LONG_DATA never has a response. MySQL silently
                            // ignores unknown statements and defers statement errors to EXECUTE.
                            if let Some(state) = stmts.get_mut(&stmt) {
                                if state.pending_error.is_none() {
                                    if param >= state.params {
                                        state.pending_error = Some((
                                            ErrorKind::ER_WRONG_ARGUMENTS,
                                            format!(
                                                "got long data for parameter {} but statement {} has {} parameters",
                                                param, stmt, state.params
                                            ),
                                        ));
                                    } else {
                                        let current_size = state
                                            .long_data
                                            .values()
                                            .try_fold(0usize, |total, value| {
                                                total.checked_add(value.len())
                                            });
                                        let new_size = current_size
                                            .and_then(|size| size.checked_add(data.len()));
                                        let new_connection_size =
                                            total_long_data_size.checked_add(data.len());
                                        if let (Some(new_size), Some(new_connection_size)) =
                                            (new_size, new_connection_size)
                                        {
                                            if new_size <= self.max_long_data_size
                                                && new_connection_size
                                                    <= self.max_connection_long_data_size
                                            {
                                                state
                                                    .long_data
                                                    .entry(param)
                                                    .or_insert_with(Vec::new)
                                                    .extend(data);
                                                total_long_data_size = new_connection_size;
                                            } else {
                                                state.pending_error = Some((
                                                    ErrorKind::ER_NET_PACKET_TOO_LARGE,
                                                    format!(
                                                        "long data exceeds configured statement/connection limits: {} / {} bytes",
                                                        self.max_long_data_size,
                                                        self.max_connection_long_data_size
                                                    ),
                                                ));
                                            }
                                        } else {
                                            state.pending_error = Some((
                                                ErrorKind::ER_NET_PACKET_TOO_LARGE,
                                                "long data size overflow".to_string(),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        Command::Close(stmt) => {
                            self.shim.on_close(stmt).await;
                            if let Some(state) = stmts.remove(&stmt) {
                                let removed = state
                                    .long_data
                                    .values()
                                    .fold(0usize, |total, value| total.saturating_add(value.len()));
                                total_long_data_size = total_long_data_size.saturating_sub(removed);
                            }
                            // NOTE: spec dictates no response from server
                        }
                        Command::Reset(stmt) => {
                            if let Some(state) = stmts.get_mut(&stmt) {
                                let removed = state
                                    .long_data
                                    .values()
                                    .fold(0usize, |total, value| total.saturating_add(value.len()));
                                state.long_data.clear();
                                state.pending_error = None;
                                total_long_data_size = total_long_data_size.saturating_sub(removed);
                                writers::write_ok_packet(
                                    &mut self.writer,
                                    self.client_capabilities,
                                    OkResponse {
                                        status_flags: self.status_flags,
                                        ..Default::default()
                                    },
                                )
                                .await?;
                            } else {
                                writers::write_err(
                                    ErrorKind::ER_UNKNOWN_STMT_HANDLER,
                                    format!("unknown statement {}", stmt).as_bytes(),
                                    &mut self.writer,
                                )
                                .await?;
                            }
                        }
                        Command::ListFields(_) => {
                            // mysql_list_fields (CommandByte::COM_FIELD_LIST / 0x04) has been deprecated in mysql 5.7
                            // and will be removed in a future version.
                            // The mysql command line tool issues one of these commands after switching databases with USE <DB>.
                            // An empty COM_FIELD_LIST response is closed by a real EOF
                            // packet, even when the client negotiated CLIENT_DEPRECATE_EOF.
                            writers::write_eof_packet(&mut self.writer, StatusFlags::empty())
                                .await?;
                        }
                        Command::Init(schema) => {
                            let schema_str = match ::std::str::from_utf8(schema) {
                                Ok(s) => s,
                                Err(e) => {
                                    writers::write_err(
                                        ErrorKind::ER_MALFORMED_PACKET,
                                        format!("schema name is not valid utf-8: {}", e).as_bytes(),
                                        &mut self.writer,
                                    )
                                    .await?;
                                    self.writer.flush_all().await?;
                                    continue;
                                }
                            };
                            let completion = Arc::new(AtomicBool::new(false));
                            let w = InitWriter {
                                client_capabilities: self.client_capabilities,
                                status_flags: self.status_flags,
                                writer: &mut self.writer,
                                completion: Arc::clone(&completion),
                            };
                            self.shim.on_init(schema_str, w).await?;
                            ensure_response_completed(&completion, "init database")?;
                        }
                        Command::Ping => {
                            writers::write_ok_packet(
                                &mut self.writer,
                                self.client_capabilities,
                                OkResponse {
                                    status_flags: self.status_flags,
                                    ..Default::default()
                                },
                            )
                            .await?;
                        }
                        Command::ResetConnection => {
                            if self.shim.on_reset_connection().await? {
                                stmts.clear();
                                total_long_data_size = 0;
                                writers::write_ok_packet(
                                    &mut self.writer,
                                    self.client_capabilities,
                                    OkResponse {
                                        status_flags: self.status_flags,
                                        ..Default::default()
                                    },
                                )
                                .await?;
                            } else {
                                writers::write_err(
                                    ErrorKind::ER_UNKNOWN_COM_ERROR,
                                    b"COM_RESET_CONNECTION is not supported by this backend",
                                    &mut self.writer,
                                )
                                .await?;
                            }
                        }
                        Command::Quit => {
                            break;
                        }
                    }
                    self.writer.flush_all().await?;
                }
                Err(_) => {
                    let (kind, msg) = command_parse_error(&packet);
                    writers::write_err(kind, msg.as_bytes(), &mut self.writer).await?;
                    self.writer.flush_all().await?;
                }
            }
        }
        Ok(())
    }
}

async fn read_packet_with_timeout<R: AsyncRead + Unpin>(
    reader: &mut PacketReader<R>,
    timeout_duration: Option<Duration>,
) -> io::Result<Option<(u8, packet_reader::Packet<'_>)>> {
    if let Some(timeout_duration) = timeout_duration {
        match timeout(timeout_duration, reader.next_async()).await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "timed out waiting for client packet after {:?}",
                    timeout_duration
                ),
            )),
        }
    } else {
        reader.next_async().await
    }
}

impl<B, R, W> AsyncMysqlIntermediary<B, BufReader<R>, BufWriter<W>>
where
    W: AsyncWrite + Send + Unpin,
    B: AsyncMysqlShim<BufWriter<W>> + Send + Sync,
    R: AsyncRead + Send + Unpin,
{
    /// Create a new server over buffered channels and process client commands until the client
    /// disconnects or an error occurs.
    pub async fn run_on_buffered(
        shim: B,
        input_stream: R,
        output_stream: W,
    ) -> Result<(), <B as AsyncMysqlShim<BufWriter<W>>>::Error> {
        Self::run_with_options_buffered(shim, input_stream, output_stream, &Default::default())
            .await
    }

    /// Create a new server over buffered channels and process client commands until the client
    /// disconnects or an error occurs, with config options.
    pub async fn run_with_options_buffered(
        shim: B,
        input_stream: R,
        output_stream: W,
        opts: &IntermediaryOptions,
    ) -> Result<(), <B as AsyncMysqlShim<BufWriter<W>>>::Error> {
        let read_cap = opts.read_buffer_size.unwrap_or(8 * 1024);
        let write_cap = opts.write_buffer_size.unwrap_or(8 * 1024);
        let input_stream = BufReader::with_capacity(read_cap, input_stream);
        let output_stream = BufWriter::with_capacity(write_cap, output_stream);
        AsyncMysqlIntermediary::<B, BufReader<R>, BufWriter<W>>::run_with_options(
            shim,
            input_stream,
            output_stream,
            opts,
        )
        .await
    }
}
