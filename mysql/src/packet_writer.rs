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

use byteorder::{ByteOrder, LittleEndian};
use std::io;
use std::io::prelude::*;
use std::io::IoSlice;

use crate::U24_MAX;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::time::{timeout, Duration};

// Reuse small control/metadata packet allocations, but don't retain a large
// packet for the lifetime of an idle connection. Independent of I/O buffering
// and the logical packet size limit; result rows own their separate buffer.
const MAX_RETAINED_PACKET_BUFFER_CAPACITY: usize = 64 * 1024;

/// The writer of mysql packet.
/// - behaves as a sync writer, while build the packet
///   so that trivial async writes could be avoided
/// - behaves like a async writer, while writing data to the output stream
pub struct PacketWriter<W> {
    packet_builder: PacketBuilder,
    output_stream: W,
    flush_threshold: usize,
    write_timeout: Option<Duration>,
}

// exports the internal builder as sync Write
impl<W> Write for PacketWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.packet_builder.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.packet_builder.flush()
    }
}

impl<W> PacketWriter<W> {
    pub fn new(output_stream: W) -> Self {
        Self {
            packet_builder: PacketBuilder::new(),
            output_stream,
            flush_threshold: 64 * 1024,
            write_timeout: None,
        }
    }
    pub fn set_seq(&mut self, seq: u8) {
        self.packet_builder.set_seq(seq)
    }

    pub fn set_flush_threshold(&mut self, flush_threshold: usize) {
        self.flush_threshold = flush_threshold;
    }

    pub fn set_max_packet_size(&mut self, max_packet_size: usize) {
        self.packet_builder.max_packet_size = max_packet_size;
    }

    pub(crate) fn max_packet_size(&self) -> usize {
        self.packet_builder.max_packet_size
    }

    pub(crate) fn remaining_packet_capacity(&self) -> usize {
        self.packet_builder
            .max_packet_size
            .saturating_sub(self.packet_builder.buffer.len())
    }

    pub fn set_write_timeout(&mut self, write_timeout: Option<Duration>) {
        self.write_timeout = write_timeout;
    }
}

const PACKET_HEADER_SIZE: usize = 4;
impl<W: AsyncWrite + Unpin> PacketWriter<W> {
    async fn write_chunk(&mut self, chunk: &[u8]) -> io::Result<()> {
        let mut header = [0; PACKET_HEADER_SIZE];
        LittleEndian::write_u24(&mut header, chunk.len() as u32);
        header[3] = self.packet_builder.seq();
        self.packet_builder.increase_seq();

        let mut header_offset = 0;
        let mut chunk_offset = 0;
        while header_offset < header.len() || chunk_offset < chunk.len() {
            let slices = if header_offset < header.len() {
                [
                    IoSlice::new(&header[header_offset..]),
                    IoSlice::new(&chunk[chunk_offset..]),
                ]
            } else {
                [IoSlice::new(&chunk[chunk_offset..]), IoSlice::new(&[])]
            };

            let written = if let Some(duration) = self.write_timeout {
                timeout(duration, self.output_stream.write_vectored(&slices))
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "timed out writing MySQL packet")
                    })??
            } else {
                self.output_stream.write_vectored(&slices).await?
            };
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write packet chunk",
                ));
            }

            let header_remaining = header.len() - header_offset;
            if written < header_remaining {
                header_offset += written;
            } else {
                header_offset = header.len();
                chunk_offset += written - header_remaining;
            }
        }

        Ok(())
    }

    /// Build packet(s) and write them to the output stream
    pub async fn end_packet(&mut self) -> io::Result<()> {
        if !self.packet_builder.is_empty() {
            let mut raw_packet = self.packet_builder.take_buffer();
            self.write_packet(&raw_packet).await?;
            // Only recycle after a successful send (including any required flush).
            // On transport failure the allocation is dropped and the connection
            // must be discarded, just as before.
            if raw_packet.capacity() <= MAX_RETAINED_PACKET_BUFFER_CAPACITY {
                raw_packet.clear();
                self.packet_builder.buffer = raw_packet;
            }
        }
        Ok(())
    }

    /// Send an already encoded logical packet without copying it into the builder.
    /// No partially built packet may be pending. Empty payloads preserve end_packet's
    /// no-op behavior. After a transport error the connection must be discarded.
    pub(crate) async fn write_packet(&mut self, payload: &[u8]) -> io::Result<()> {
        if !self.packet_builder.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot send a complete packet while a packet is being built",
            ));
        }
        if payload.len() > self.max_packet_size() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "outgoing MySQL packet exceeds configured limit",
            ));
        }
        if payload.is_empty() {
            return Ok(());
        }
        for chunk in payload.chunks(U24_MAX) {
            self.write_chunk(chunk).await?;
        }
        if payload.len().is_multiple_of(U24_MAX) {
            self.write_chunk(&[]).await?;
        }
        if self.flush_threshold > 0 && payload.len() >= self.flush_threshold {
            self.flush_output().await?;
        }
        Ok(())
    }

    pub async fn flush_all(&mut self) -> io::Result<()> {
        self.flush_output().await
    }

    async fn flush_output(&mut self) -> io::Result<()> {
        if let Some(duration) = self.write_timeout {
            timeout(duration, self.output_stream.flush())
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "timed out flushing MySQL packet")
                })?
        } else {
            self.output_stream.flush().await
        }
    }
}

// Builder that exports as sync `Write`, so that  trivial scattered async writes
// could be avoided during constructing the packet, especially the writes in mod [writers]
struct PacketBuilder {
    buffer: Vec<u8>,
    seq: u8,
    max_packet_size: usize,
}

impl Write for PacketBuilder {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Here we take them all, and split them into raw packets later in `end_packet` if the size
        // of buffer is larger than max payload size (16MB)
        let new_len =
            self.buffer.len().checked_add(buf.len()).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "packet size overflow")
            })?;
        if new_len > self.max_packet_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "outgoing MySQL packet exceeds configured limit: {} bytes > {} bytes",
                    new_len, self.max_packet_size
                ),
            ));
        }
        self.buffer.extend(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl PacketBuilder {
    pub fn new() -> Self {
        PacketBuilder {
            buffer: vec![],
            seq: 0,
            max_packet_size: usize::MAX,
        }
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn take_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }

    fn set_seq(&mut self, seq: u8) {
        self.seq = seq;
    }

    fn increase_seq(&mut self) {
        self.seq = self.seq.wrapping_add(1);
    }

    fn seq(&self) -> u8 {
        self.seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    struct PartialAsyncWrite {
        written: Vec<u8>,
        max_bytes_per_call: usize,
        flushes: usize,
        fail_flush: bool,
        fail_after: Option<(usize, io::ErrorKind)>,
        yield_writes: bool,
        pending: bool,
        pending_flush: bool,
    }

    impl PartialAsyncWrite {
        fn new(max_bytes_per_call: usize) -> Self {
            Self {
                written: Vec::new(),
                max_bytes_per_call,
                flushes: 0,
                fail_flush: false,
                fail_after: None,
                yield_writes: false,
                pending: true,
                pending_flush: false,
            }
        }

        fn poll_budget(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
            if self.yield_writes && self.pending {
                self.pending = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            self.pending = true;
            let mut budget = self.max_bytes_per_call;
            if let Some((limit, kind)) = self.fail_after {
                if self.written.len() == limit {
                    return Poll::Ready(if kind == io::ErrorKind::WriteZero {
                        Ok(0)
                    } else {
                        Err(io::Error::new(kind, "injected write error"))
                    });
                }
                budget = budget.min(limit - self.written.len());
            }
            Poll::Ready(Ok(budget))
        }
    }

    impl AsyncWrite for PartialAsyncWrite {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let budget = std::task::ready!(self.poll_budget(cx))?;
            let written = buf.len().min(budget);
            self.written.extend_from_slice(&buf[..written]);
            Poll::Ready(Ok(written))
        }

        fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            if self.pending_flush {
                return Poll::Pending;
            }
            self.flushes += 1;
            Poll::Ready(if self.fail_flush {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "flush failed"))
            } else {
                Ok(())
            })
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let mut remaining = std::task::ready!(self.poll_budget(cx))?;
            let mut written = 0;
            for buf in bufs {
                if remaining == 0 {
                    break;
                }
                let take = buf.len().min(remaining);
                self.written.extend_from_slice(&buf[..take]);
                written += take;
                remaining -= take;
            }
            Poll::Ready(Ok(written))
        }
    }

    #[tokio::test]
    async fn fragmented_writes_preserve_wire_prefix_on_every_failure_offset() {
        let expected = b"\x07\0\0\xffpayload";
        for chunk_size in [1, 2, 3, 4, 5, 64] {
            for cut in 0..expected.len() {
                for kind in [io::ErrorKind::BrokenPipe, io::ErrorKind::WriteZero] {
                    let mut output = PartialAsyncWrite::new(chunk_size);
                    output.fail_after = Some((cut, kind));
                    output.yield_writes = true;
                    let mut writer = PacketWriter::new(output);
                    writer.set_seq(255);
                    writer.set_flush_threshold(1);
                    writer.write_all(b"payload").unwrap();
                    assert_eq!(writer.end_packet().await.unwrap_err().kind(), kind);
                    assert_eq!(writer.output_stream.written, &expected[..cut]);
                    assert_eq!(writer.output_stream.flushes, 0);
                    assert_eq!(writer.packet_builder.buffer.capacity(), 0);
                    // A failed transport is discarded, never retried as a new
                    // logical packet or followed by an ERR on the broken wire.
                }
            }
            let mut output = PartialAsyncWrite::new(chunk_size);
            output.yield_writes = true;
            let mut writer = PacketWriter::new(output);
            writer.set_seq(255);
            writer.write_all(b"payload").unwrap();
            writer.end_packet().await.unwrap();
            writer.write_packet(b"next").await.unwrap();
            assert_eq!(
                writer.output_stream.written,
                b"\x07\0\0\xffpayload\x04\0\0\0next"
            );
        }
    }

    #[tokio::test]
    async fn flush_timeout_discards_sent_packet_without_recycling_buffer() {
        let mut output = PartialAsyncWrite::new(2);
        output.yield_writes = true;
        output.pending_flush = true;
        let mut writer = PacketWriter::new(output);
        writer.set_write_timeout(Some(Duration::from_millis(10)));
        writer.set_flush_threshold(1);
        writer.write_all(b"payload").unwrap();
        assert_eq!(
            writer.end_packet().await.unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(writer.output_stream.written, b"\x07\0\0\0payload");
        assert_eq!(writer.packet_builder.buffer.capacity(), 0);
    }

    #[tokio::test]
    async fn write_chunk_handles_partial_vectored_writes() {
        let mut writer = PacketWriter::new(PartialAsyncWrite::new(3));
        writer.write_all(b"hello world").unwrap();
        writer.end_packet().await.unwrap();

        let output = &writer.output_stream.written;
        assert_eq!(&output[..4], &[11, 0, 0, 0]);
        assert_eq!(&output[4..], b"hello world");
    }

    #[tokio::test]
    async fn end_packet_flushes_when_payload_crosses_threshold() {
        let mut writer = PacketWriter::new(PartialAsyncWrite::new(1024));
        writer.set_flush_threshold(4);
        writer.write_all(b"hello world").unwrap();
        writer.end_packet().await.unwrap();

        assert_eq!(writer.output_stream.flushes, 1);
    }

    #[tokio::test]
    async fn end_packet_reuses_allocation_without_replaying_data() {
        let mut writer = PacketWriter::new(PartialAsyncWrite::new(3));
        writer.set_flush_threshold(0);
        writer.write_all(b"warmup").unwrap();
        let allocation = writer.packet_builder.buffer.as_ptr();
        let capacity = writer.packet_builder.buffer.capacity();
        writer.end_packet().await.unwrap();

        let mut expected = b"\x06\0\0\0warmup".to_vec();
        // Also cross the sequence-id rollover while reusing the same allocation.
        for index in 1..=300u16 {
            assert!(writer.packet_builder.is_empty());
            assert_eq!(writer.packet_builder.buffer.capacity(), capacity);
            assert_eq!(writer.packet_builder.buffer.as_ptr(), allocation);
            writer.end_packet().await.unwrap(); // An empty builder must remain a no-op.
            let payload = if index % 2 == 0 {
                &b"ok"[..]
            } else {
                &b"!"[..]
            };
            writer.write_all(payload).unwrap();
            writer.end_packet().await.unwrap();
            expected.extend_from_slice(&[payload.len() as u8, 0, 0, index as u8]);
            expected.extend_from_slice(payload);
        }
        assert_eq!(writer.output_stream.written, expected);
        assert_eq!(writer.output_stream.flushes, 0);
        writer.flush_all().await.unwrap();
        assert_eq!(writer.output_stream.flushes, 1);
    }

    #[tokio::test]
    async fn end_packet_bounds_retained_capacity_not_payload_length() {
        const LIMIT: usize = 64 * 1024;
        for (capacity, length) in [(LIMIT, 1), (LIMIT + 1, 1), (LIMIT + 1, LIMIT + 1)] {
            let mut writer = PacketWriter::new(PartialAsyncWrite::new(1024));
            writer.packet_builder.buffer = Vec::with_capacity(capacity);
            writer.packet_builder.buffer.resize(length, 0x42);
            let allocated = writer.packet_builder.buffer.capacity();
            writer.end_packet().await.unwrap();
            assert!(writer.packet_builder.is_empty());
            assert_eq!(
                writer.packet_builder.buffer.capacity(),
                if allocated <= LIMIT { allocated } else { 0 }
            );
            assert_eq!(
                writer.output_stream.flushes,
                usize::from(length >= 64 * 1024)
            );
            writer.write_all(b"next").unwrap();
            writer.end_packet().await.unwrap();
            assert_eq!(
                &writer.output_stream.written[length + 4..],
                b"\x04\0\0\x01next"
            );
        }
    }

    #[tokio::test]
    async fn borrowed_packet_preserves_recycled_builder_allocation() {
        let mut writer = PacketWriter::new(PartialAsyncWrite::new(3));
        writer.write_all(b"meta").unwrap();
        let allocation = writer.packet_builder.buffer.as_ptr();
        let capacity = writer.packet_builder.buffer.capacity();
        writer.end_packet().await.unwrap();
        writer.write_packet(b"row").await.unwrap();
        assert_eq!(writer.packet_builder.buffer.capacity(), capacity);
        assert_eq!(writer.packet_builder.buffer.as_ptr(), allocation);
        writer.write_all(b"end").unwrap();
        writer.end_packet().await.unwrap();
        assert_eq!(
            writer.output_stream.written,
            b"\x04\0\0\0meta\x03\0\0\x01row\x03\0\0\x02end"
        );
    }

    #[tokio::test]
    async fn end_packet_does_not_retain_buffer_after_write_error() {
        let mut writer = PacketWriter::new(PartialAsyncWrite::new(0));
        writer.write_all(b"payload").unwrap();
        assert_eq!(
            writer.end_packet().await.unwrap_err().kind(),
            io::ErrorKind::WriteZero
        );
        assert!(writer.packet_builder.is_empty());
        assert_eq!(writer.packet_builder.buffer.capacity(), 0);
    }

    #[tokio::test]
    async fn end_packet_does_not_retain_buffer_after_flush_error() {
        let mut output = PartialAsyncWrite::new(3);
        output.fail_flush = true;
        let mut writer = PacketWriter::new(output);
        writer.set_flush_threshold(1);
        writer.write_all(b"payload").unwrap();
        assert_eq!(
            writer.end_packet().await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(writer.output_stream.written, b"\x07\0\0\0payload");
        assert_eq!(writer.output_stream.flushes, 1);
        assert!(writer.packet_builder.is_empty());
        assert_eq!(writer.packet_builder.buffer.capacity(), 0);
    }

    #[test]
    fn packet_writer_options_preserve_defaults_and_explicit_values() {
        let defaults = crate::IntermediaryOptions::default();
        let writer = defaults.packet_writer(Vec::<u8>::new());
        assert_eq!(
            writer.max_packet_size(),
            crate::packet_reader::DEFAULT_MAX_PACKET_SIZE
        );
        assert_eq!(writer.flush_threshold, 64 * 1024);
        assert_eq!(writer.write_timeout, Some(Duration::from_secs(60)));

        for (limit, threshold, duration) in [(4, 3, Some(Duration::from_millis(25))), (0, 0, None)]
        {
            let options = crate::IntermediaryOptions {
                max_packet_size: Some(limit),
                write_high_watermark: Some(threshold),
                write_timeout: duration,
                ..Default::default()
            };
            let writer = options.packet_writer(Vec::<u8>::new());
            assert_eq!(writer.max_packet_size(), limit);
            assert_eq!(writer.flush_threshold, threshold);
            assert_eq!(writer.write_timeout, duration);
        }
    }

    #[tokio::test]
    async fn configured_writer_preserves_packet_limit_and_flush_behavior() {
        for threshold in [0, 4, 5] {
            let options = crate::IntermediaryOptions {
                max_packet_size: Some(4),
                write_high_watermark: Some(threshold),
                ..Default::default()
            };
            let mut writer = options.packet_writer(PartialAsyncWrite::new(3));
            writer.write_all(b"four").unwrap();
            assert_eq!(
                writer.write_all(b"!").unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
            writer.end_packet().await.unwrap();
            assert_eq!(writer.output_stream.written, b"\x04\0\0\0four");
            assert_eq!(writer.output_stream.flushes, usize::from(threshold == 4));
            writer.flush_all().await.unwrap();
            assert_eq!(
                writer.output_stream.flushes,
                usize::from(threshold == 4) + 1
            );
        }
    }

    #[tokio::test]
    async fn built_large_packet_releases_allocation_and_preserves_frame_terminator() {
        let payload = vec![0x42; U24_MAX];
        let mut writer = PacketWriter::new(PartialAsyncWrite::new(1024 * 1024));
        writer.set_seq(255);
        writer.write_all(&payload).unwrap();
        writer.end_packet().await.unwrap();
        assert_eq!(writer.packet_builder.buffer.capacity(), 0);
        assert_eq!(&writer.output_stream.written[..4], &[255, 255, 255, 255]);
        assert_eq!(&writer.output_stream.written[4..4 + U24_MAX], &payload);
        assert_eq!(&writer.output_stream.written[4 + U24_MAX..], &[0, 0, 0, 0]);
        writer.write_all(b"next").unwrap();
        writer.end_packet().await.unwrap();
        assert_eq!(
            &writer.output_stream.written[8 + U24_MAX..],
            b"\x04\0\0\x01next"
        );
    }

    #[test]
    fn outgoing_packet_limit_is_enforced_before_allocation() {
        let mut writer = PacketWriter::new(PartialAsyncWrite::new(1024));
        writer.set_max_packet_size(4);
        writer.write_all(b"four").unwrap();
        let error = writer.write_all(b"!").unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn borrowed_packet_preserves_framing_sequence_and_flush() {
        for size in [3, U24_MAX, U24_MAX + 1] {
            let payload = vec![0x42; size];
            let mut writer = PacketWriter::new(PartialAsyncWrite::new(1024 * 1024));
            writer.set_seq(255);
            writer.set_max_packet_size(size);
            writer.set_flush_threshold(size);
            writer.write_packet(&payload).await.unwrap();
            assert_eq!(writer.packet_builder.buffer.capacity(), 0);
            assert_eq!(writer.output_stream.flushes, 1);
            let output = &writer.output_stream.written;
            let first_len = size.min(U24_MAX);
            assert_eq!(LittleEndian::read_u24(&output[..3]) as usize, first_len);
            assert_eq!(output[3], 255);
            assert_eq!(&output[4..4 + first_len], &payload[..first_len]);
            if size >= U24_MAX {
                let tail = &output[4 + first_len..];
                assert_eq!(
                    LittleEndian::read_u24(&tail[..3]) as usize,
                    size - first_len
                );
                assert_eq!(tail[3], 0);
                assert_eq!(&tail[4..], &payload[first_len..]);
            } else {
                assert_eq!(output.len(), size + 4);
            }
        }
    }

    #[tokio::test]
    async fn borrowed_packet_rejects_invalid_state_before_writing() {
        let mut writer = PacketWriter::new(PartialAsyncWrite::new(3));
        writer.set_seq(7);
        writer.set_max_packet_size(4);
        assert_eq!(
            writer.write_packet(b"large").await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        writer.write_all(b"old").unwrap();
        assert_eq!(
            writer.write_packet(b"new").await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(writer.output_stream.written.is_empty());
        assert_eq!(writer.packet_builder.seq(), 7);
        writer.end_packet().await.unwrap();
        writer.write_packet(b"next").await.unwrap();
        assert_eq!(
            writer.output_stream.written,
            b"\x03\0\0\x07old\x04\0\0\x08next"
        );
    }

    struct PendingWrite;

    impl AsyncWrite for PendingWrite {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn write_timeout_terminates_stalled_client() {
        let options = crate::IntermediaryOptions {
            write_timeout: Some(Duration::from_millis(10)),
            ..Default::default()
        };
        let mut writer = options.packet_writer(PendingWrite);
        writer.write_all(b"payload").unwrap();
        let error = writer.end_packet().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(writer.packet_builder.is_empty());
        assert_eq!(writer.packet_builder.buffer.capacity(), 0);
    }
}
