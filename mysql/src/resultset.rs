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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;

use mysql_common::constants::{CapabilityFlags, ColumnFlags, StatusFlags};
use tokio::io::AsyncWrite;

use crate::packet_writer::PacketWriter;
use crate::value::ToMysqlValue;
use crate::{session_status_flags, writers, ColumnMetadata, OkResponse};
use crate::{Column, ErrorKind, InitResponse, StatementData};

fn default_column_metadata(column: &Column) -> (&Column, ColumnMetadata) {
    (column, ColumnMetadata::default())
}

/// Convenience type for responding to a client `USE <db>` command.
pub struct InitWriter<'a, W> {
    pub(crate) client_capabilities: CapabilityFlags,
    pub(crate) status_flags: &'a mut StatusFlags,
    pub(crate) writer: &'a mut PacketWriter<W>,
    pub(crate) completion: Arc<AtomicU8>,
}

impl<'a, W: 'a + AsyncWrite + Unpin> InitWriter<'a, W> {
    pub(crate) fn new_tracked(
        writer: &'a mut PacketWriter<W>,
        client_capabilities: CapabilityFlags,
        status_flags: &'a mut StatusFlags,
    ) -> (Self, Arc<AtomicU8>) {
        let completion = Arc::new(AtomicU8::new(InitResponse::Pending as u8));
        (
            Self {
                client_capabilities,
                status_flags,
                writer,
                completion: Arc::clone(&completion),
            },
            completion,
        )
    }

    /// Tell client that database context has been changed
    pub async fn ok(self) -> io::Result<()> {
        let status = *self.status_flags;
        self.ok_with_status(status).await
    }

    /// Complete initialization with explicit session state, including zero.
    pub async fn ok_with_status(self, status: StatusFlags) -> io::Result<()> {
        writers::write_ok_packet(
            self.writer,
            self.client_capabilities,
            OkResponse {
                status_flags: status,
                ..Default::default()
            },
        )
        .await?;
        *self.status_flags = session_status_flags(status);
        self.completion
            .store(InitResponse::Ok as u8, Ordering::Release);
        Ok(())
    }

    /// Tell client that there was a problem changing the database context.
    /// Although you can return any valid MySQL error code you probably want
    /// to keep it similar to the MySQL server and issue either a
    /// `ErrorKind::ER_BAD_DB_ERROR` or a `ErrorKind::ER_DBACCESS_DENIED_ERROR`.
    pub async fn error<E>(self, kind: ErrorKind, msg: &E) -> io::Result<()>
    where
        E: Borrow<[u8]> + ?Sized,
    {
        writers::write_err(kind, msg.borrow(), self.writer).await?;
        self.completion
            .store(InitResponse::Error as u8, Ordering::Release);
        Ok(())
    }
}

/// Convenience type for responding to a client `PREPARE` command.
///
/// This type should not be dropped without calling
/// [`reply`](struct.StatementMetaWriter.html#method.reply) or
/// [`error`](struct.StatementMetaWriter.html#method.error).
#[must_use]
pub struct StatementMetaWriter<'a, W> {
    pub(crate) writer: &'a mut PacketWriter<W>,
    pub(crate) stmts: &'a mut HashMap<u32, StatementData>,
    pub(crate) client_capabilities: CapabilityFlags,
    pub(crate) status_flags: StatusFlags,
    pub(crate) completion: Arc<AtomicBool>,
}

impl<'a, W: AsyncWrite + Unpin + 'a> StatementMetaWriter<'a, W> {
    /// Reply to the client with the given meta-information.
    ///
    /// `id` is a statement identifier that the client should supply when it later wants to execute
    /// this statement. `params` is a set of [`Column`](struct.Column.html) descriptors for the
    /// parameters the client must provide when executing the prepared statement. `columns` is a
    /// second set of [`Column`](struct.Column.html) descriptors for the values that will be
    /// returned in each row then the statement is later executed.
    pub async fn reply<PI, CI>(self, id: u32, params: PI, columns: CI) -> io::Result<()>
    where
        PI: IntoIterator<Item = &'a Column>,
        CI: IntoIterator<Item = &'a Column>,
        <PI as IntoIterator>::IntoIter: ExactSizeIterator,
        <CI as IntoIterator>::IntoIter: ExactSizeIterator,
    {
        let default_metadata = default_column_metadata as fn(&Column) -> (&Column, ColumnMetadata);
        let params = params.into_iter().map(default_metadata);
        let columns = columns.into_iter().map(default_metadata);
        self.reply_inner(id, params, columns).await
    }

    /// Reply with explicit collation and decimals for parameters and result columns.
    /// Each metadata slice must have exactly one entry per corresponding column.
    pub async fn reply_with_metadata(
        self,
        id: u32,
        params: &[Column],
        columns: &[Column],
        param_metadata: &[ColumnMetadata],
        column_metadata: &[ColumnMetadata],
    ) -> io::Result<()> {
        validate_metadata(params, param_metadata)?;
        validate_metadata(columns, column_metadata)?;
        self.reply_inner(
            id,
            params.iter().zip(param_metadata.iter().copied()),
            columns.iter().zip(column_metadata.iter().copied()),
        )
        .await
    }

    async fn reply_inner<'c>(
        self,
        id: u32,
        params: impl ExactSizeIterator<Item = (&'c Column, ColumnMetadata)>,
        columns: impl ExactSizeIterator<Item = (&'c Column, ColumnMetadata)>,
    ) -> io::Result<()> {
        let param_count = u16::try_from(params.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "prepared statement has more than 65535 parameters",
            )
        })?;
        u16::try_from(columns.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "prepared statement has more than 65535 result columns",
            )
        })?;
        if self.stmts.contains_key(&id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("duplicate prepared statement id {id}"),
            ));
        }
        writers::write_prepare_ok(
            id,
            params,
            columns,
            self.writer,
            self.client_capabilities,
            self.status_flags,
        )
        .await?;
        self.stmts.insert(
            id,
            StatementData {
                params: param_count,
                ..Default::default()
            },
        );
        self.completion.store(true, Ordering::Release);
        Ok(())
    }

    /// Reply to the client's `PREPARE` with an error.
    pub async fn error<E>(self, kind: ErrorKind, msg: &E) -> io::Result<()>
    where
        E: Borrow<[u8]> + ?Sized,
    {
        writers::write_err(kind, msg.borrow(), self.writer).await?;
        self.completion.store(true, Ordering::Release);
        Ok(())
    }
}

enum Finalizer {
    Ok(OkResponse),
    Eof(StatusFlags),
}

fn validate_metadata(columns: &[Column], metadata: &[ColumnMetadata]) -> io::Result<()> {
    if columns.len() != metadata.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "column metadata count mismatch",
        ));
    }
    for metadata in metadata {
        metadata.validate()?;
    }
    Ok(())
}

/// Completes a read-only cursor execute with metadata, or an ERR response.
#[must_use]
pub struct CursorExecuteWriter<'a, W> {
    pub(crate) result: QueryResultWriter<'a, W>,
    pub(crate) columns: &'a mut Option<Vec<Column>>,
}

impl<W: AsyncWrite + Unpin> CursorExecuteWriter<'_, W> {
    /// Report backend session state, including explicit zero, before open/error.
    /// Cursor flags are managed by the protocol, not by the backend.
    pub fn set_status_flags(&mut self, status: StatusFlags) {
        self.result.set_status_flags(status);
    }

    /// Publish the cursor only after the backend has successfully executed it.
    pub async fn open(self, columns: &[Column], metadata: &[ColumnMetadata]) -> io::Result<()> {
        validate_metadata(columns, metadata)?;
        if columns.is_empty() || columns.len() > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid cursor column count",
            ));
        }
        let mut result = self.result;
        let status = session_status_flags(result.default_status_flags)
            | StatusFlags::SERVER_STATUS_CURSOR_EXISTS;
        // Suppress the ordinary metadata EOF: cursor execute has exactly one
        // terminal EOF/OK carrying CURSOR_EXISTS, including DEPRECATE_EOF clients.
        writers::column_definitions(
            columns.iter().zip(metadata.iter().copied()),
            result.writer,
            result.client_capabilities | CapabilityFlags::CLIENT_DEPRECATE_EOF,
            status,
        )
        .await?;
        result.last_end = Some(cursor_finalizer(result.client_capabilities, status));
        result.no_more_results().await?;
        *self.columns = Some(columns.to_vec());
        Ok(())
    }

    /// Reject execution without opening a cursor.
    pub async fn error(self, kind: ErrorKind, message: &[u8]) -> io::Result<()> {
        self.result.error(kind, message).await
    }
}

fn cursor_finalizer(capabilities: CapabilityFlags, status: StatusFlags) -> Finalizer {
    if capabilities.contains(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
        Finalizer::Ok(OkResponse {
            header: 0xfe,
            status_flags: status,
            ..Default::default()
        })
    } else {
        Finalizer::Eof(status)
    }
}

/// A FETCH response. Rows use the same binary encoder as ordinary EXECUTE.
#[must_use]
pub struct CursorFetchWriter<'a, W: AsyncWrite + Unpin> {
    rows: RowWriter<'a, W>,
    remaining: u32,
    pub(crate) exhausted: &'a mut bool,
}

impl<'a, W: AsyncWrite + Unpin> CursorFetchWriter<'a, W> {
    /// Report backend session state even for an empty batch or a failed FETCH.
    /// Durable flags carry into subsequent commands; cursor flags do not.
    pub fn set_status_flags(&mut self, status: StatusFlags) {
        self.rows.set_status_flags(status);
    }

    pub(crate) fn new(
        result: QueryResultWriter<'a, W>,
        columns: &'a [Column],
        limit: u32,
        exhausted: &'a mut bool,
    ) -> Self {
        Self {
            rows: RowWriter {
                client_capabilities: result.client_capabilities,
                result: Some(result),
                columns,
                bitmap_len: (columns.len() + 9) / 8,
                data: Vec::new(),
                col: 0,
                finished: false,
            },
            remaining: limit,
            exhausted,
        }
    }

    /// Encode a complete row synchronously, then send it. The closure must only
    /// encode columns; FETCH row-count enforcement remains in this writer.
    pub async fn write_row_with<F>(&mut self, encode: F) -> io::Result<()>
    where
        F: FnOnce(&mut RowWriter<'a, W>) -> io::Result<()>,
    {
        if self.remaining == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "FETCH row limit exceeded",
            ));
        }
        encode(&mut self.rows)?;
        self.rows.end_row().await?;
        self.remaining -= 1;
        Ok(())
    }

    /// End this batch; `exhausted` means the backend actually reached EOF.
    pub async fn finish(mut self, exhausted: bool) -> io::Result<()> {
        if self.rows.col != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unfinished FETCH row",
            ));
        }
        let mut result = self.rows.result.take().unwrap();
        let flag = if exhausted {
            StatusFlags::SERVER_STATUS_LAST_ROW_SENT
        } else {
            StatusFlags::SERVER_STATUS_CURSOR_EXISTS
        };
        let status = session_status_flags(result.default_status_flags) | flag;
        result.last_end = Some(cursor_finalizer(result.client_capabilities, status));
        result.no_more_results().await?;
        *self.exhausted = exhausted;
        Ok(())
    }

    /// Discard an unsent partial row and close the cursor with an ERR.
    pub async fn error(self, kind: ErrorKind, message: &[u8]) -> io::Result<()> {
        *self.exhausted = true;
        self.rows.finish_error(kind, &message).await
    }
}

/// Convenience type for providing query results to clients.
///
/// This type should not be dropped without calling
/// [`start`](struct.QueryResultWriter.html#method.start),
/// [`completed`](struct.QueryResultWriter.html#method.completed), or
/// [`error`](struct.QueryResultWriter.html#method.error).
///
/// To send multiple resultsets, use
/// [`RowWriter::finish_one`](struct.RowWriter.html#method.finish_one) and
/// [`complete_one`](struct.QueryResultWriter.html#method.complete_one). These are similar to
/// `RowWriter::finish` and `completed`, but both eventually yield back the `QueryResultWriter` so
/// that another resultset can be sent. To indicate that no more resultset will be sent, call
/// [`no_more_results`](struct.QueryResultWriter.html#method.no_more_results). All methods on
/// `QueryResultWriter` (except `no_more_results`) automatically start a new resultset. The
#[must_use]
pub struct QueryResultWriter<'a, W> {
    // XXX: specialization instead?
    pub(crate) is_bin: bool,
    pub(crate) client_capabilities: CapabilityFlags,
    pub(crate) writer: &'a mut PacketWriter<W>,
    last_end: Option<Finalizer>,
    default_status_flags: StatusFlags,
    session_status: Option<&'a mut StatusFlags>,
    completion: Option<Arc<AtomicBool>>,
}

impl<'a, W: AsyncWrite + Unpin> QueryResultWriter<'a, W> {
    #[cfg(test)]
    pub(crate) fn new(
        writer: &'a mut PacketWriter<W>,
        is_bin: bool,
        client_capabilities: CapabilityFlags,
        default_status_flags: StatusFlags,
    ) -> Self {
        QueryResultWriter {
            is_bin,
            client_capabilities,
            writer,
            last_end: None,
            default_status_flags,
            session_status: None,
            completion: None,
        }
    }

    pub(crate) fn new_tracked(
        writer: &'a mut PacketWriter<W>,
        is_bin: bool,
        client_capabilities: CapabilityFlags,
        session_status: &'a mut StatusFlags,
    ) -> (Self, Arc<AtomicBool>) {
        let completion = Arc::new(AtomicBool::new(false));
        (
            QueryResultWriter {
                is_bin,
                client_capabilities,
                writer,
                last_end: None,
                default_status_flags: *session_status,
                session_status: Some(session_status),
                completion: Some(Arc::clone(&completion)),
            },
            completion,
        )
    }

    async fn finalize(&mut self, more_exists: bool) -> io::Result<()> {
        if more_exists && self.last_end.is_some() {
            let required = if self.is_bin {
                CapabilityFlags::CLIENT_PS_MULTI_RESULTS
            } else {
                CapabilityFlags::CLIENT_MULTI_RESULTS
            };
            if !self.client_capabilities.contains(required) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "client did not negotiate multiple result sets",
                ));
            }
        }

        let status = match self.last_end.take() {
            None => return Ok(()),
            Some(Finalizer::Ok(mut ok_packet)) => {
                ok_packet
                    .status_flags
                    .set(StatusFlags::SERVER_MORE_RESULTS_EXISTS, more_exists);
                let status = ok_packet.status_flags;
                writers::write_ok_packet(self.writer, self.client_capabilities, ok_packet).await?;
                status
            }
            Some(Finalizer::Eof(mut status)) => {
                status.set(StatusFlags::SERVER_MORE_RESULTS_EXISTS, more_exists);
                writers::write_eof_packet(self.writer, status).await?;
                status
            }
        };
        if let Some(session_status) = &mut self.session_status {
            **session_status = session_status_flags(status);
        }
        Ok(())
    }

    /// Set the status of the next resultset, including an explicit zero.
    /// Completing the response carries durable flags (transaction/autocommit/SQL mode)
    /// into subsequent commands. An ERR packet cannot report status on the wire,
    /// but `error` also saves it for subsequent responses (e.g. after rollback).
    /// opensrv does not infer transaction state from SQL.
    pub fn set_status_flags(&mut self, status: StatusFlags) {
        self.default_status_flags = status;
    }

    /// Start a resultset response to the client that conforms to the given `columns`.
    ///
    /// Note that if no columns are emitted, any written rows are ignored.
    ///
    /// See [`RowWriter`](struct.RowWriter.html).
    pub async fn start(mut self, columns: &'a [Column]) -> io::Result<RowWriter<'a, W>> {
        self.finalize(true).await?;
        RowWriter::new(self, columns, None).await
    }

    /// Start a resultset with one explicit metadata entry per column.
    /// The metadata changes only the wire description, not row value encoding.
    pub async fn start_with_metadata(
        mut self,
        columns: &'a [Column],
        metadata: &[ColumnMetadata],
    ) -> io::Result<RowWriter<'a, W>> {
        validate_metadata(columns, metadata)?;
        self.finalize(true).await?;
        RowWriter::new(self, columns, Some(metadata)).await
    }

    /// Send an empty resultset response to the client indicating that `rows` rows were affected by
    /// the query in this resultset. `last_insert_id` may be given to communiate an identifier for
    /// a client's most recent insertion.
    pub async fn complete_one(
        self,
        mut ok_packet: OkResponse,
    ) -> io::Result<QueryResultWriter<'a, W>> {
        // Compatibility: existing callers commonly pass OkResponse::default().
        if ok_packet.status_flags.is_empty() {
            ok_packet.status_flags = self.default_status_flags;
        }
        self.complete_one_with_status(ok_packet).await
    }

    /// Like `complete_one`, but treats `status_flags` literally, including zero.
    pub async fn complete_one_with_status(
        mut self,
        ok_packet: OkResponse,
    ) -> io::Result<QueryResultWriter<'a, W>> {
        self.finalize(true).await?;
        self.default_status_flags = session_status_flags(ok_packet.status_flags);
        self.last_end = Some(Finalizer::Ok(ok_packet));
        Ok(self)
    }

    /// Send an empty resultset response to the client indicating that `rows` rows were affected by
    /// the query. `last_insert_id` may be given to communiate an identifier for a client's most
    /// recent insertion.
    pub async fn completed(self, ok_packet: OkResponse) -> io::Result<()> {
        self.complete_one(ok_packet).await?.no_more_results().await
    }

    /// Complete a command with explicit status flags; zero is a valid value.
    pub async fn completed_with_status(self, ok_packet: OkResponse) -> io::Result<()> {
        self.complete_one_with_status(ok_packet)
            .await?
            .no_more_results()
            .await
    }

    /// Reply to the client's query with an error.
    pub async fn error<E>(mut self, kind: ErrorKind, msg: &E) -> io::Result<()>
    where
        E: Borrow<[u8]> + ?Sized,
    {
        self.finalize(true).await?;
        writers::write_err(kind, msg.borrow(), self.writer).await?;
        if let Some(session_status) = &mut self.session_status {
            **session_status = session_status_flags(self.default_status_flags);
        }
        if let Some(completion) = &self.completion {
            completion.store(true, Ordering::Release);
        }
        Ok(())
    }

    /// Send the last bits of the last resultset to the client, and indicate that there are no more
    /// resultsets coming.
    /// At least one result must have been started/completed. To return an empty
    /// OK response, use `completed(OkResponse::default())` instead.
    pub async fn no_more_results(mut self) -> io::Result<()> {
        if self.last_end.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cannot finish a command without a result or OK response",
            ));
        }
        self.finalize(false).await?;
        if let Some(completion) = &self.completion {
            completion.store(true, Ordering::Release);
        }
        Ok(())
    }
}

/// Convenience type for sending rows of a resultset to a client.
///
/// Rows can either be written out one column at a time (using
/// [`write_col`](struct.RowWriter.html#method.write_col) and
/// [`end_row`](struct.RowWriter.html#method.end_row)), or one row at a time (using
/// [`write_row`](struct.RowWriter.html#method.write_row)).
///
/// This type must be completed with [`finish`](struct.RowWriter.html#method.finish) or
/// [`finish_error`](struct.RowWriter.html#method.finish_error). Dropping it does not send an
/// end-of-records marker, so the client cannot safely continue using the connection.
#[must_use]
pub struct RowWriter<'a, W: AsyncWrite + Unpin> {
    client_capabilities: CapabilityFlags,
    result: Option<QueryResultWriter<'a, W>>,
    bitmap_len: usize,
    data: Vec<u8>,
    columns: &'a [Column],

    // next column to write for the current row
    // NOTE: (ab)used to track number of *rows* for a zero-column resultset
    col: usize,
    finished: bool,
}

struct RowBufferWriter<'a> {
    buffer: &'a mut Vec<u8>,
    limit: usize,
}

impl Write for RowBufferWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let new_len = self
            .buffer
            .len()
            .checked_add(buf.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "row size overflow"))?;
        if new_len > self.limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "outgoing MySQL packet exceeds configured limit: {} bytes > {} bytes",
                    new_len, self.limit
                ),
            ));
        }
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a, W> RowWriter<'a, W>
where
    W: 'a + AsyncWrite + Unpin,
{
    async fn new(
        result: QueryResultWriter<'a, W>,
        columns: &'a [Column],
        metadata: Option<&[ColumnMetadata]>,
    ) -> io::Result<RowWriter<'a, W>> {
        let bitmap_len = (columns.len() + 7 + 2) / 8;
        let client_capabilities = result.client_capabilities;
        let mut rw = RowWriter {
            client_capabilities,
            result: Some(result),
            columns,
            bitmap_len,
            data: Vec::new(),

            col: 0,

            finished: false,
        };
        rw.start(metadata).await?;
        Ok(rw)
    }

    #[inline]
    async fn start(&mut self, metadata: Option<&[ColumnMetadata]>) -> io::Result<()> {
        if !self.columns.is_empty() {
            let result = self.result.as_mut().unwrap();
            if let Some(metadata) = metadata {
                writers::column_definitions(
                    self.columns.iter().zip(metadata.iter().copied()),
                    result.writer,
                    self.client_capabilities,
                    result.default_status_flags,
                )
                .await?;
            } else {
                let default_metadata =
                    default_column_metadata as fn(&Column) -> (&Column, ColumnMetadata);
                writers::column_definitions(
                    self.columns.iter().map(default_metadata),
                    result.writer,
                    self.client_capabilities,
                    result.default_status_flags,
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Write a value to the next column of the current row as a part of this resultset.
    ///
    /// If you do not call [`end_row`](struct.RowWriter.html#method.end_row) after the last row,
    /// any errors that occur when writing out the last row will be returned by
    /// [`finish`](struct.RowWriter.html#method.finish). If you do not call `finish` either, the
    /// response remains incomplete and the connection cannot be safely reused.
    ///
    /// Note that the row *must* conform to the column specification provided to
    /// [`QueryResultWriter::start`](struct.QueryResultWriter.html#method.start). If it does not,
    /// this method will return an error indicating that an invalid value type or specification was
    /// provided.
    pub fn write_col<T>(&mut self, v: T) -> io::Result<()>
    where
        T: ToMysqlValue,
    {
        if self.columns.is_empty() {
            return Ok(());
        }

        let c = self.columns.get(self.col).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "row has more columns than specification",
            )
        })?;
        let checkpoint = self.data.len();
        let packet_capacity = self
            .result
            .as_ref()
            .unwrap()
            .writer
            .remaining_packet_capacity();
        let result = if self.result.as_mut().unwrap().is_bin {
            if self.col == 0 {
                let header_len = 1 + self.bitmap_len;
                if header_len > packet_capacity {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "outgoing MySQL packet exceeds configured limit: {} bytes > {} bytes",
                            header_len, packet_capacity
                        ),
                    ));
                }
                self.data.resize(header_len, 0);
            }

            if v.is_null() {
                if c.colflags.contains(ColumnFlags::NOT_NULL_FLAG) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "given NULL value for NOT NULL column",
                    ));
                } else {
                    // https://web.archive.org/web/20170404144156/https://dev.mysql.com/doc/internals/en/null-bitmap.html
                    // NULL-bitmap-byte = ((field-pos + offset) / 8)
                    // NULL-bitmap-bit  = ((field-pos + offset) % 8)
                    self.data[1 + (self.col + 2) / 8] |= 1u8 << ((self.col + 2) % 8);
                }
                Ok(())
            } else {
                v.to_mysql_bin(
                    &mut RowBufferWriter {
                        buffer: &mut self.data,
                        limit: packet_capacity,
                    },
                    c,
                )
            }
        } else {
            v.to_mysql_text_with_column(
                &mut RowBufferWriter {
                    buffer: &mut self.data,
                    limit: packet_capacity,
                },
                c,
            )
        };
        if let Err(error) = result {
            self.data.truncate(checkpoint);
            return Err(error);
        }
        self.col += 1;
        Ok(())
    }

    /// Indicate that no more column data will be written for the current row.
    pub async fn end_row(&mut self) -> io::Result<()> {
        if self.columns.is_empty() {
            self.col += 1;
            return Ok(());
        }

        if self.col != self.columns.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "row has fewer columns than specification",
            ));
        }

        self.result
            .as_mut()
            .unwrap()
            .writer
            .write_packet(&self.data)
            .await?;
        self.data.clear();
        self.col = 0;

        Ok(())
    }

    /// Write a single row as a part of this resultset.
    ///
    /// Note that the row *must* conform to the column specification provided to
    /// [`QueryResultWriter::start`](struct.QueryResultWriter.html#method.start). If it does not,
    /// this method will return an error indicating that an invalid value type or specification was
    /// provided.
    pub async fn write_row<I, E>(&mut self, row: I) -> io::Result<()>
    where
        I: IntoIterator<Item = E>,
        E: ToMysqlValue,
    {
        if !self.columns.is_empty() {
            for v in row {
                self.write_col(v)?;
            }
        }
        self.end_row().await
    }
}

impl<'a, W: AsyncWrite + Unpin + 'a> RowWriter<'a, W> {
    /// Set the status sent with this resultset's final EOF/OK packet.
    pub fn set_status_flags(&mut self, status: StatusFlags) {
        self.result.as_mut().unwrap().set_status_flags(status);
    }

    async fn finish_inner(&mut self, extra_info: &str, complete: bool) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }

        self.finished = true;

        if !self.columns.is_empty() && self.col != 0 {
            self.end_row().await?;
        }

        if complete {
            let status = self.result.as_ref().unwrap().default_status_flags;
            if self.columns.is_empty() {
                let resp = OkResponse {
                    info: extra_info.to_string(),
                    status_flags: status,
                    ..Default::default()
                };
                self.result.as_mut().unwrap().last_end = Some(Finalizer::Ok(resp));
            } else if self
                .client_capabilities
                .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
            {
                // With CLIENT_DEPRECATE_EOF the server must terminate the resultset with an OK packet (header 0xFE).
                let resp = OkResponse {
                    header: 0xfe,
                    info: extra_info.to_string(),
                    status_flags: status,
                    ..Default::default()
                };
                self.result.as_mut().unwrap().last_end = Some(Finalizer::Ok(resp));
            } else {
                // we wrote out at least one row
                self.result.as_mut().unwrap().last_end = Some(Finalizer::Eof(status));
            }
            self.result.as_mut().unwrap().default_status_flags = session_status_flags(status);
        }

        Ok(())
    }

    /// Indicate to the client that no more rows are coming.
    pub async fn finish(self) -> io::Result<()> {
        self.finish_with_info("").await
    }

    /// End this resultset response, and indicate to the client that no more rows are coming.
    pub async fn finish_one(self) -> io::Result<QueryResultWriter<'a, W>> {
        self.finish_one_with_info("").await
    }

    /// Indicate to the client that no more rows are coming.
    pub async fn finish_with_info(self, extra_info: &str) -> io::Result<()> {
        self.finish_one_with_info(extra_info)
            .await?
            .no_more_results()
            .await
    }

    /// End this resultset response, and indicate to the client that no more rows are coming.
    pub async fn finish_one_with_info(
        mut self,
        extra_info: &str,
    ) -> io::Result<QueryResultWriter<'a, W>> {
        self.finish_inner(extra_info, true).await?;

        Ok(self.result.take().unwrap())
    }

    /// End this resultset response, and indicate to the client there was an error.
    pub async fn finish_error<E>(mut self, kind: ErrorKind, msg: &E) -> io::Result<()>
    where
        E: Borrow<[u8]>,
    {
        self.finished = true;
        self.col = 0;
        self.data.clear();
        self.result.take().unwrap().error(kind, msg).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct PartialFailure;

    #[tokio::test]
    async fn cursor_status_updates_include_empty_fetch_and_error_paths() {
        let columns = [Column {
            table: String::new(),
            column: "v".into(),
            collen: 0,
            coltype: crate::ColumnType::MYSQL_TYPE_LONG,
            colflags: ColumnFlags::empty(),
        }];
        for deprecate_eof in [false, true] {
            let mut wire = Vec::new();
            let mut packet = PacketWriter::new(&mut wire);
            let mut session = StatusFlags::SERVER_STATUS_AUTOCOMMIT;
            let mut caps = CapabilityFlags::CLIENT_PROTOCOL_41;
            caps.set(CapabilityFlags::CLIENT_DEPRECATE_EOF, deprecate_eof);
            let (result, completion) =
                QueryResultWriter::new_tracked(&mut packet, true, caps, &mut session);
            let mut saved_columns = None;
            let mut execute = CursorExecuteWriter {
                result,
                columns: &mut saved_columns,
            };
            execute.set_status_flags(StatusFlags::SERVER_STATUS_IN_TRANS);
            execute
                .open(&columns, &[ColumnMetadata::default()])
                .await
                .unwrap();
            assert!(completion.load(Ordering::Acquire));
            assert_eq!(session, StatusFlags::SERVER_STATUS_IN_TRANS);
            assert!(saved_columns.is_some());

            let (result, _) = QueryResultWriter::new_tracked(&mut packet, true, caps, &mut session);
            let mut exhausted = false;
            let mut fetch = CursorFetchWriter::new(result, &columns, 0, &mut exhausted);
            fetch.set_status_flags(StatusFlags::empty());
            fetch.finish(false).await.unwrap();
            assert!(session.is_empty());
            assert!(!exhausted);

            let (result, _) = QueryResultWriter::new_tracked(&mut packet, true, caps, &mut session);
            let mut fetch = CursorFetchWriter::new(result, &columns, 1, &mut exhausted);
            fetch.set_status_flags(StatusFlags::SERVER_STATUS_AUTOCOMMIT);
            fetch.finish(true).await.unwrap();
            assert_eq!(session, StatusFlags::SERVER_STATUS_AUTOCOMMIT);
            assert!(exhausted);

            // ERR has no status field; nevertheless save rollback state for PING
            // and the next command, including explicit zero and no rows written.
            let (result, _) = QueryResultWriter::new_tracked(&mut packet, true, caps, &mut session);
            let mut fetch = CursorFetchWriter::new(result, &columns, 1, &mut exhausted);
            fetch.set_status_flags(StatusFlags::empty());
            fetch
                .error(ErrorKind::ER_UNKNOWN_ERROR, b"rollback")
                .await
                .unwrap();
            assert!(session.is_empty());
            let (result, _) = QueryResultWriter::new_tracked(&mut packet, true, caps, &mut session);
            let mut execute = CursorExecuteWriter {
                result,
                columns: &mut saved_columns,
            };
            execute.set_status_flags(StatusFlags::SERVER_STATUS_AUTOCOMMIT);
            execute
                .error(ErrorKind::ER_UNKNOWN_ERROR, b"execute failed")
                .await
                .unwrap();
            assert_eq!(session, StatusFlags::SERVER_STATUS_AUTOCOMMIT);

            packet.flush_all().await.unwrap();
            let mut bytes = wire.as_slice();
            let mut packets = Vec::new();
            while !bytes.is_empty() {
                let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]) as usize;
                packets.push(&bytes[4..4 + len]);
                bytes = &bytes[4 + len..];
            }
            for (index, expected) in [
                (
                    2,
                    StatusFlags::SERVER_STATUS_IN_TRANS | StatusFlags::SERVER_STATUS_CURSOR_EXISTS,
                ),
                (3, StatusFlags::SERVER_STATUS_CURSOR_EXISTS),
                (
                    4,
                    StatusFlags::SERVER_STATUS_AUTOCOMMIT
                        | StatusFlags::SERVER_STATUS_LAST_ROW_SENT,
                ),
            ] {
                assert_eq!(packets[index][0], 0xfe);
                assert_eq!(
                    u16::from_le_bytes([packets[index][3], packets[index][4]]),
                    expected.bits()
                );
            }
            assert_eq!(packets[5][0], 0xff);
            assert_eq!(packets[6][0], 0xff);
        }
    }

    #[tokio::test]
    async fn cursor_writers_track_completion_and_discard_partial_errors() {
        let columns = [Column {
            table: String::new(),
            column: "v".into(),
            collen: 0,
            coltype: crate::ColumnType::MYSQL_TYPE_LONG,
            colflags: ColumnFlags::empty(),
        }];
        for eof in [false, true] {
            let mut wire = Vec::new();
            let mut packet = PacketWriter::new(&mut wire);
            let mut status = StatusFlags::SERVER_STATUS_AUTOCOMMIT;
            let mut caps = CapabilityFlags::CLIENT_PROTOCOL_41;
            caps.set(CapabilityFlags::CLIENT_DEPRECATE_EOF, eof);
            let (result, completion) =
                QueryResultWriter::new_tracked(&mut packet, true, caps, &mut status);
            let mut exhausted = false;
            let mut fetch = CursorFetchWriter::new(result, &columns, 1, &mut exhausted);
            fetch
                .write_row_with(|row| row.write_col(42i32))
                .await
                .unwrap();
            assert!(fetch
                .write_row_with(|row| row.write_col(43i32))
                .await
                .is_err());
            fetch.finish(false).await.unwrap();
            assert!(completion.load(Ordering::Acquire));
            assert!(!exhausted);
            assert_eq!(status, StatusFlags::SERVER_STATUS_AUTOCOMMIT);

            let (result, completion) =
                QueryResultWriter::new_tracked(&mut packet, true, caps, &mut status);
            let mut fetch = CursorFetchWriter::new(result, &columns, 1, &mut exhausted);
            assert!(fetch
                .write_row_with(|row| {
                    row.write_col(1i32)?;
                    row.write_col(2i32)
                })
                .await
                .is_err());
            fetch
                .error(ErrorKind::ER_UNKNOWN_ERROR, b"bad row")
                .await
                .unwrap();
            assert!(completion.load(Ordering::Acquire));
            assert!(exhausted);
            packet.flush_all().await.unwrap();
            let mut packets = Vec::new();
            let mut bytes = wire.as_slice();
            while !bytes.is_empty() {
                let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]) as usize;
                packets.push(&bytes[4..4 + len]);
                bytes = &bytes[4 + len..];
            }
            assert_eq!(packets.len(), 3); // one row, batch terminator, ERR (no partial row)
            assert_eq!(packets[0], &[0, 0, 42, 0, 0, 0]);
            assert_eq!(packets[2][0], 0xff);
        }
    }

    #[tokio::test]
    async fn empty_finalization_does_not_mark_a_missing_response_complete() {
        for binary in [false, true] {
            let mut wire = Vec::new();
            let mut writer = PacketWriter::new(&mut wire);
            let mut status = StatusFlags::SERVER_STATUS_AUTOCOMMIT;
            let (response, completed) = QueryResultWriter::new_tracked(
                &mut writer,
                binary,
                CapabilityFlags::CLIENT_PROTOCOL_41,
                &mut status,
            );
            assert_eq!(
                response.no_more_results().await.unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
            assert!(!completed.load(Ordering::Acquire));
            drop(writer);
            assert!(wire.is_empty());
            assert_eq!(status, StatusFlags::SERVER_STATUS_AUTOCOMMIT);
        }
    }

    impl ToMysqlValue for PartialFailure {
        fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
            w.write_all(b"partial")?;
            Err(io::Error::new(io::ErrorKind::InvalidData, "encoder failed"))
        }

        fn to_mysql_bin<W: Write>(&self, w: &mut W, _: &Column) -> io::Result<()> {
            self.to_mysql_text(w)
        }
    }

    #[tokio::test]
    async fn extra_column_rejection_does_not_corrupt_a_valid_row() {
        for binary in [false, true] {
            let columns = [Column {
                table: String::new(),
                column: "v".into(),
                collen: 0,
                coltype: crate::ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            }];
            let mut wire = Vec::new();
            let mut writer = PacketWriter::new(&mut wire);
            let mut rows = QueryResultWriter::new(
                &mut writer,
                binary,
                CapabilityFlags::CLIENT_PROTOCOL_41,
                StatusFlags::empty(),
            )
            .start(&columns)
            .await
            .unwrap();
            rows.write_col("valid").unwrap();
            let before = rows.data.clone();
            assert_eq!(
                rows.write_col("extra").unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(rows.data, before);
            assert_eq!(rows.col, 1);
            rows.end_row().await.unwrap();
            rows.write_row(["next"]).await.unwrap();
            rows.finish().await.unwrap();
        }
    }

    #[tokio::test]
    async fn row_limit_and_column_rollback_preserve_wire_payload() {
        for is_bin in [false, true] {
            let column = Column {
                table: String::new(),
                column: "v".into(),
                collen: 0,
                coltype: crate::ColumnType::MYSQL_TYPE_BLOB,
                colflags: ColumnFlags::empty(),
            };
            let columns = [column.clone(), column];
            let mut wire = Vec::new();
            let mut writer = PacketWriter::new(&mut wire);
            let mut rows = QueryResultWriter::new(
                &mut writer,
                is_bin,
                CapabilityFlags::CLIENT_PROTOCOL_41,
                StatusFlags::SERVER_STATUS_AUTOCOMMIT,
            )
            .start(&columns)
            .await
            .unwrap();
            // Two length-prefixed strings, plus the binary row header/null bitmap.
            let limit = 16 + if is_bin { 2 } else { 0 };
            rows.result
                .as_mut()
                .unwrap()
                .writer
                .set_max_packet_size(limit);
            assert!(rows.write_col(PartialFailure).is_err());
            assert!(rows.data.is_empty());
            assert_eq!(rows.col, 0);
            rows.write_col("1234567").unwrap();
            let checkpoint = rows.data.clone();
            assert!(rows.write_col(PartialFailure).is_err());
            assert_eq!(rows.data, checkpoint);
            assert_eq!(rows.col, 1);
            // Each string fits separately, but their combined encoding is one byte over.
            assert!(rows.write_col("12345678").is_err());
            assert_eq!(rows.data, checkpoint);
            assert_eq!(rows.col, 1);
            rows.write_col("abcdefg").unwrap();
            assert_eq!(rows.data.len(), limit);
            rows.end_row().await.unwrap();
            rows.finish().await.unwrap();
            drop(writer);
            let mut packets = Vec::new();
            let mut remaining = wire.as_slice();
            while !remaining.is_empty() {
                let len = usize::from(remaining[0])
                    | (usize::from(remaining[1]) << 8)
                    | (usize::from(remaining[2]) << 16);
                packets.push(remaining[4..4 + len].to_vec());
                remaining = &remaining[4 + len..];
            }
            let mut expected = if is_bin { vec![0, 0] } else { Vec::new() };
            expected.extend_from_slice(b"\x071234567\x07abcdefg");
            assert_eq!(packets.len(), 6);
            assert_eq!(packets[4], expected);
            assert_eq!(packets[5][0], 0xfe);
        }
    }

    #[tokio::test]
    async fn oversized_row_value_is_rejected_without_buffering_the_value() {
        for is_bin in [false, true] {
            let columns = [Column {
                table: String::new(),
                column: "payload".to_string(),
                collen: 0,
                coltype: crate::ColumnType::MYSQL_TYPE_BLOB,
                colflags: ColumnFlags::empty(),
            }];
            let mut wire = Vec::new();
            let mut packet_writer = PacketWriter::new(&mut wire);
            let mut rows = QueryResultWriter::new(
                &mut packet_writer,
                is_bin,
                CapabilityFlags::CLIENT_PROTOCOL_41,
                StatusFlags::SERVER_STATUS_AUTOCOMMIT,
            )
            .start(&columns)
            .await
            .unwrap();
            rows.result.as_mut().unwrap().writer.set_max_packet_size(16);

            let error = rows.write_col(vec![0u8; 1024]).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(rows.data.is_empty());
            assert!(rows.data.capacity() < 1024);
        }
    }
}
