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

/// The writer of mysql packet.
/// - behaves as a sync writer, while build the packet
///   so that trivial async writes could be avoided
/// - behaves like a async writer, while writing data to the output stream
pub struct PacketWriter<W> {
    packet_builder: PacketBuilder,
    output_stream: W,
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
        }
    }
    pub fn set_seq(&mut self, seq: u8) {
        self.packet_builder.set_seq(seq)
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

            let written = self.output_stream.write_vectored(&slices).await?;
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
            let raw_packet = self.packet_builder.take_buffer();
            let needs_empty_packet = raw_packet.len() % U24_MAX == 0;

            // split the raw buffer at the boundary of size U24_MAX
            for chunk in raw_packet.chunks(U24_MAX) {
                self.write_chunk(chunk).await?;
            }

            // Exact multiples of U24_MAX must be terminated by an empty packet.
            if needs_empty_packet {
                self.write_chunk(&[]).await?;
            }

            Ok(())
        } else {
            Ok(())
        }
    }

    pub async fn flush_all(&mut self) -> io::Result<()> {
        self.output_stream.flush().await
    }
}

// Builder that exports as sync `Write`, so that  trivial scattered async writes
// could be avoided during constructing the packet, especially the writes in mod [writers]
struct PacketBuilder {
    buffer: Vec<u8>,
    seq: u8,
}

impl Write for PacketBuilder {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Here we take them all, and split them into raw packets later in `end_packet` if the size
        // of buffer is larger than max payload size (16MB)
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
    }

    impl PartialAsyncWrite {
        fn new(max_bytes_per_call: usize) -> Self {
            Self {
                written: Vec::new(),
                max_bytes_per_call,
            }
        }
    }

    impl AsyncWrite for PartialAsyncWrite {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let written = buf.len().min(self.max_bytes_per_call);
            self.written.extend_from_slice(&buf[..written]);
            Poll::Ready(Ok(written))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let mut remaining = self.max_bytes_per_call;
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
    async fn write_chunk_handles_partial_vectored_writes() {
        let mut writer = PacketWriter::new(PartialAsyncWrite::new(3));
        writer.write_all(b"hello world").unwrap();
        writer.end_packet().await.unwrap();

        let output = &writer.output_stream.written;
        assert_eq!(&output[..4], &[11, 0, 0, 0]);
        assert_eq!(&output[4..], b"hello world");
    }
}
