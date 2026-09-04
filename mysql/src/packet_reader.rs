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
use std::io::prelude::*;

use std::iter::Enumerate;
use std::marker::PhantomData;
use std::ops::RangeFrom;

use bytes::{BufMut, BytesMut};
use nom::Needed;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;

const PACKET_BUFFER_SIZE: usize = 4_096;
const PACKET_LARGE_BUFFER_SIZE: usize = 1_048_576;
pub const DEFAULT_MAX_PACKET_SIZE: usize = 1024 * 1024 * 1024;

/// Calculate the new buffer size for the next read
fn calc_new_buf_size(last_buf_size: usize) -> usize {
    if last_buf_size >= PACKET_BUFFER_SIZE * 2 {
        // if packet is already too large, use larger buffer to avoid multiple allocation
        PACKET_LARGE_BUFFER_SIZE
    } else {
        std::cmp::max(PACKET_BUFFER_SIZE, last_buf_size * 2)
    }
}

/// reuse old buffer if possible, otherwise create a new buffer
///
/// return (the idx to start writing to the buffer,  and the buffer itself)
///
/// will copy the remain bytes from the old buffer to the new buffer if reusing
fn reuse_or_create_buf(old_buf: bytes::Bytes, last_buf_size: usize) -> BytesMut {
    let new_buf_size = calc_new_buf_size(last_buf_size);
    match old_buf.try_into_mut() {
        Ok(mut unique) => {
            let len = unique.len();
            let resize_buf = if new_buf_size <= len {
                // if new buffer is smaller than old buffer, just double the size
                len * 2
            } else {
                new_buf_size
            };
            debug_assert!(len < resize_buf);
            let required_capacity = resize_buf.saturating_sub(len);
            unique.reserve(required_capacity);
            unique
        }
        Err(remain) => {
            let len = remain.len();
            let resize_buf = if new_buf_size <= len {
                len.checked_mul(2).unwrap_or(len)
            } else {
                new_buf_size
            };
            let mut buf = BytesMut::with_capacity(resize_buf);
            // if old buffer still contain bytes unread, need to save those bytes too
            buf.extend_from_slice(&remain);
            buf
        }
    }
}

fn read_into_bytesmut<R: Read>(reader: &mut R, buf: &mut BytesMut) -> io::Result<usize> {
    if buf.spare_capacity_mut().is_empty() {
        buf.reserve(PACKET_BUFFER_SIZE);
    }
    let spare = buf.spare_capacity_mut();
    // Cap the read buffer to 64KB to avoid O(capacity) memset overhead
    // on large unused capacities, while maintaining good read throughput.
    let to_read = std::cmp::min(spare.len(), 65536);
    let spare = &mut spare[..to_read];

    // Safety: std::io::Read requires an initialized buffer.
    unsafe {
        std::ptr::write_bytes(spare.as_mut_ptr(), 0, spare.len());
    }
    let dst = unsafe { std::slice::from_raw_parts_mut(spare.as_mut_ptr() as *mut u8, spare.len()) };
    let read_cnt = reader.read(dst)?;
    unsafe {
        buf.advance_mut(read_cnt);
    }
    Ok(read_cnt)
}

pub struct PacketReader<R> {
    bytes: bytes::Bytes,
    pub r: R,
    max_packet_size: usize,
}

impl<R> PacketReader<R> {
    pub fn new(r: R) -> Self {
        Self::new_with_max_packet_size(r, DEFAULT_MAX_PACKET_SIZE)
    }

    pub fn new_with_max_packet_size(r: R, max_packet_size: usize) -> Self {
        PacketReader {
            bytes: bytes::Bytes::new(),
            r,
            max_packet_size,
        }
    }

    fn ensure_packet_limit(&self) -> io::Result<()> {
        let mut input = self.bytes.as_ref();
        let mut payload_size = 0usize;

        loop {
            if input.len() < 3 {
                return Ok(());
            }

            let frame_payload_size =
                input[0] as usize | ((input[1] as usize) << 8) | ((input[2] as usize) << 16);
            payload_size = payload_size
                .checked_add(frame_payload_size)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "packet exceeds configured limit: payload size overflow",
                    )
                })?;
            if payload_size > self.max_packet_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "packet exceeds configured limit: payload {} bytes > limit {} bytes",
                        payload_size, self.max_packet_size
                    ),
                ));
            }

            // The length field is known, so the limit above can reject early even
            // when the payload has not arrived in full yet. Do not inspect a
            // following frame until this frame is complete.
            let frame_size = 4usize.saturating_add(frame_payload_size);
            if input.len() < frame_size {
                return Ok(());
            }

            if frame_payload_size < U24_MAX {
                return Ok(());
            }
            input = &input[frame_size..];
        }
    }
}

impl<R: Read> PacketReader<R> {
    #[allow(dead_code)]
    pub fn next(&mut self) -> io::Result<Option<(u8, Packet<'_>)>> {
        loop {
            let last_buffer_size = self.bytes.len();
            if !self.bytes.is_empty() {
                self.ensure_packet_limit()?;
                // coping `bytes::Bytes` are very cheap, just move the pointer and increase the ref count.
                match packet(self.bytes.clone().into()) {
                    Ok((rest, p)) => {
                        // most time the `rest` is either empty or very small, so it's cheap to copy it later into next buffer
                        self.bytes = rest.into();
                        return Ok(Some(p));
                    }
                    Err(nom::Err::Incomplete(_)) | Err(nom::Err::Error(_)) => {}
                    Err(nom::Err::Failure(ctx)) => {
                        let err = Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("{:?}", ctx),
                        ));
                        return err;
                    }
                }
            }

            // read more buffer
            let mut buf = reuse_or_create_buf(std::mem::take(&mut self.bytes), last_buffer_size);
            let read_cnt = read_into_bytesmut(&mut self.r, &mut buf)?;
            self.bytes = buf.freeze();

            // for a [TcpStream], returning zero indicates the connection was shut down correctly.
            if read_cnt == 0 {
                if self.bytes.is_empty() {
                    return Ok(None);
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("{} unhandled bytes", self.bytes.len()),
                    ));
                }
            }
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for PacketReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        // if our buffer have content, send those immediately
        if !self.bytes.is_empty() {
            let to_copy = buf.remaining().min(self.bytes.len());
            let chunk = self.bytes.split_to(to_copy);
            buf.put_slice(&chunk);
            std::task::Poll::Ready(Ok(()))
        } else {
            std::pin::Pin::new(&mut self.r).poll_read(cx, buf)
        }
    }
}

impl<R: AsyncRead + Unpin> PacketReader<R> {
    pub async fn next_async(&mut self) -> io::Result<Option<(u8, Packet<'_>)>> {
        loop {
            let last_buffer_size = self.bytes.len();
            if !self.bytes.is_empty() {
                self.ensure_packet_limit()?;
                match packet(self.bytes.clone().into()) {
                    Ok((rest, p)) => {
                        self.bytes = rest.into();
                        return Ok(Some(p));
                    }
                    Err(nom::Err::Incomplete(_)) | Err(nom::Err::Error(_)) => {}
                    Err(nom::Err::Failure(ctx)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("{:?}", ctx),
                        ));
                    }
                }
            }

            // read more buffer
            let mut buf = reuse_or_create_buf(std::mem::take(&mut self.bytes), last_buffer_size);
            let read_cnt = self.r.read_buf(&mut buf).await?;
            self.bytes = buf.freeze();

            if read_cnt == 0 {
                if self.bytes.is_empty() {
                    return Ok(None);
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("{} unhandled bytes", self.bytes.len()),
                    ));
                }
            }
        }
    }
}

pub fn fullpacket(i: NomBytes) -> nom::IResult<NomBytes, (u8, NomBytes)> {
    let (i, _) = nom::bytes::complete::tag(&[0xff, 0xff, 0xff][..])(i)?;
    let (i, seq) = nom::bytes::complete::take(1u8)(i)?;
    let (i, bytes) = nom::bytes::complete::take(U24_MAX)(i)?;
    Ok((i, (seq.as_ref()[0], bytes)))
}

pub fn onepacket(i: NomBytes) -> nom::IResult<NomBytes, (u8, NomBytes)> {
    let (i, length) = nom::number::complete::le_u24(i)?;
    let (i, seq) = nom::bytes::complete::take(1u8)(i)?;
    let (i, bytes) = nom::bytes::complete::take(length)(i)?;
    Ok((i, (seq.as_ref()[0], bytes)))
}

/// Bytes wrapper for nom, allowing nom to parse bytes::Bytes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NomBytes(bytes::Bytes);

impl NomBytes {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl From<&[u8]> for NomBytes {
    fn from(value: &[u8]) -> Self {
        NomBytes(bytes::Bytes::copy_from_slice(value))
    }
}

impl From<bytes::Bytes> for NomBytes {
    fn from(value: bytes::Bytes) -> Self {
        NomBytes(value)
    }
}

impl From<NomBytes> for bytes::Bytes {
    fn from(value: NomBytes) -> Self {
        value.0
    }
}

impl AsRef<[u8]> for NomBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl nom::InputTake for NomBytes {
    fn take(&self, count: usize) -> Self {
        NomBytes(self.0.slice(0..count))
    }

    fn take_split(&self, count: usize) -> (Self, Self) {
        let mut prefix = self.0.clone();
        let suffix = prefix.split_off(count);
        (NomBytes(suffix), NomBytes(prefix))
    }
}

impl nom::Compare<&[u8]> for NomBytes {
    fn compare(&self, t: &[u8]) -> nom::CompareResult {
        self.0.as_ref().compare(t)
    }

    fn compare_no_case(&self, t: &[u8]) -> nom::CompareResult {
        self.0.as_ref().compare_no_case(t)
    }
}

impl nom::InputLength for NomBytes {
    fn input_len(&self) -> usize {
        self.0.len()
    }
}

impl nom::InputIter for NomBytes {
    type Item = u8;
    type Iter = Enumerate<Self::IterElem>;
    type IterElem = bytes::buf::IntoIter<bytes::Bytes>;

    #[inline]
    fn iter_indices(&self) -> Self::Iter {
        self.iter_elements().enumerate()
    }
    #[inline]
    fn iter_elements(&self) -> Self::IterElem {
        self.0.clone().into_iter()
    }
    #[inline]
    fn position<P>(&self, predicate: P) -> Option<usize>
    where
        P: Fn(Self::Item) -> bool,
    {
        self.0.iter().position(|b| predicate(*b))
    }
    #[inline]
    fn slice_index(&self, count: usize) -> Result<usize, Needed> {
        if self.0.len() >= count {
            Ok(count)
        } else {
            Err(Needed::new(count - self.0.len()))
        }
    }
}

impl nom::Slice<RangeFrom<usize>> for NomBytes {
    fn slice(&self, range: RangeFrom<usize>) -> Self {
        NomBytes(self.0.slice(range))
    }
}

// a simple wrapper around bytes::Bytes to make sure interface stays the same
#[derive(Clone)]
pub struct Packet<'a> {
    bytes: bytes::Bytes,
    first_seq: u8,
    _lifetime: PhantomData<&'a ()>, // NOTE: the lifetime can be removed since Bytes mangaes the lifetime by itself
}

impl Packet<'_> {
    fn from_bytes(bytes: bytes::Bytes, first_seq: u8) -> Self {
        Packet {
            bytes,
            first_seq,
            _lifetime: PhantomData,
        }
    }

    pub(crate) fn first_sequence_id(&self) -> u8 {
        self.first_seq
    }
}

use crate::U24_MAX;
use std::ops::Deref;

impl Deref for Packet<'_> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.bytes.as_ref()
    }
}

// Note: For small packets (< 2^24), this function is zero-copy.
// For multi-frame packets (>= 2^24), frames are probed first to ensure complete arrival
// before allocating and assembling into a single contiguous buffer.
pub(crate) fn packet<'a>(i: NomBytes) -> nom::IResult<NomBytes, (u8, Packet<'a>)> {
    fn sequence_mismatch(input: &NomBytes) -> nom::Err<nom::error::Error<NomBytes>> {
        nom::Err::Failure(nom::error::Error::new(
            input.clone(),
            nom::error::ErrorKind::Verify,
        ))
    }

    let header = i
        .as_ref()
        .get(..3)
        .ok_or(nom::Err::Incomplete(Needed::Unknown))?;
    let payload_len =
        header[0] as usize | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);

    if payload_len < U24_MAX {
        let (rest, (seq, payload)) = onepacket(i)?;
        return Ok((rest, (seq, Packet::from_bytes(payload.0, seq))));
    }

    // Multi-frame packet: probe all frames first to ensure the full packet has arrived
    // before allocating and assembling. This prevents O(N^2) allocations/copies while
    // frames are arriving in streaming chunks.
    let mut probe_input = i.as_ref();
    let mut total_payload_size = 0usize;
    let mut probe_prev_seq: Option<u8> = None;

    loop {
        if probe_input.len() < 4 {
            return Err(nom::Err::Incomplete(Needed::Unknown));
        }
        let frame_len = probe_input[0] as usize
            | ((probe_input[1] as usize) << 8)
            | ((probe_input[2] as usize) << 16);
        let frame_seq = probe_input[3];

        if let Some(prev) = probe_prev_seq {
            if frame_seq != prev.wrapping_add(1) {
                return Err(sequence_mismatch(&NomBytes::from(&probe_input[4..])));
            }
        }
        probe_prev_seq = Some(frame_seq);

        let frame_total = 4usize.saturating_add(frame_len);
        if probe_input.len() < frame_total {
            return Err(nom::Err::Incomplete(Needed::Unknown));
        }

        total_payload_size = total_payload_size
            .checked_add(frame_len)
            .ok_or_else(|| sequence_mismatch(&NomBytes::from(&probe_input[4..])))?;

        probe_input = &probe_input[frame_total..];

        if frame_len < U24_MAX {
            break;
        }
    }

    // All frames are present in memory. Assemble in a single pre-allocated BytesMut.
    let mut input = i;
    let mut assembled = BytesMut::with_capacity(total_payload_size);
    let mut previous_seq = None::<u8>;
    let mut first_seq = None::<u8>;

    loop {
        let header = input
            .as_ref()
            .get(..3)
            .ok_or(nom::Err::Incomplete(Needed::Unknown))?;
        let payload_len =
            header[0] as usize | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);

        let (rest, (seq, payload)) = if payload_len == U24_MAX {
            fullpacket(input)?
        } else {
            onepacket(input)?
        };
        let first = *first_seq.get_or_insert(seq);

        if let Some(prev) = previous_seq {
            if seq != prev.wrapping_add(1) {
                return Err(sequence_mismatch(&payload));
            }
        }
        previous_seq = Some(seq);

        assembled.extend_from_slice(payload.as_ref());
        input = rest;

        if payload_len < U24_MAX {
            return Ok((input, (seq, Packet::from_bytes(assembled.freeze(), first))));
        }
    }
}

#[cfg(test)]
mod test {
    use bytes::{Buf, BufMut};
    use std::io;

    use super::*;

    fn mock_packet(mut data: bytes::Bytes, start_seq: u8) -> bytes::Bytes {
        let mut buf = BytesMut::new();
        let mut seq = start_seq;
        let mut sent_full_packet = false;
        while data.len() >= U24_MAX {
            buf.extend_from_slice(&[0xff, 0xff, 0xff]);
            buf.put_u8(seq);
            buf.put(&data[0..U24_MAX]);
            data.advance(U24_MAX);
            seq += 1;
            sent_full_packet = true;
        }
        if !data.is_empty() || sent_full_packet {
            let le_u64: [u8; 8] = data.len().to_le_bytes();
            let le_u24 = &le_u64[0..3];
            buf.extend_from_slice(le_u24);
            buf.put_u8(seq);
            if !data.is_empty() {
                buf.put(data);
            }
        }
        buf.freeze()
    }

    #[tokio::test]
    async fn next_async_waits_for_terminator_after_full_packet() {
        use tokio::io::AsyncWriteExt;
        use tokio::sync::oneshot;
        use tokio::time::{timeout, Duration};

        let (mut client, server) = tokio::io::duplex(U24_MAX + 16);
        let (first_written_tx, first_written_rx) = oneshot::channel();
        let (send_terminator_tx, send_terminator_rx) = oneshot::channel();

        let writer = tokio::spawn(async move {
            client.write_all(&[0xff, 0xff, 0xff, 0]).await.unwrap();
            client.write_all(&vec![0; U24_MAX]).await.unwrap();
            first_written_tx.send(()).unwrap();
            send_terminator_rx.await.unwrap();
            client.write_all(&[0x00, 0x00, 0x00, 1]).await.unwrap();
        });

        first_written_rx.await.unwrap();
        let mut reader = PacketReader::new(server);
        let mut next = Box::pin(reader.next_async());

        assert!(
            timeout(Duration::from_millis(25), &mut next).await.is_err(),
            "a full-size fragment is not a complete logical packet"
        );

        send_terminator_tx.send(()).unwrap();
        let (seq, packet) = next.await.unwrap().unwrap();
        assert_eq!(seq, 1);
        assert_eq!(packet.len(), U24_MAX);
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn test_various_packet_size() {
        // test for off by one, and off by header size(3 bytes for length and 1 for seq num)
        let testcases = [
            0,
            1,
            2,
            PACKET_BUFFER_SIZE - 1 - 4,
            PACKET_BUFFER_SIZE - 1,
            PACKET_BUFFER_SIZE,
            PACKET_BUFFER_SIZE + 1,
            PACKET_BUFFER_SIZE + 1 + 4,
            PACKET_LARGE_BUFFER_SIZE - 4 - 1,
            PACKET_LARGE_BUFFER_SIZE - 4,
            PACKET_LARGE_BUFFER_SIZE - 1,
            PACKET_LARGE_BUFFER_SIZE,
            PACKET_LARGE_BUFFER_SIZE + 1,
            PACKET_LARGE_BUFFER_SIZE + 4,
            PACKET_LARGE_BUFFER_SIZE + 4 + 1,
            U24_MAX - 4 - 1,
            U24_MAX - 4,
            U24_MAX - 1,
            U24_MAX,
            U24_MAX + 1,
            U24_MAX + 4,
            U24_MAX + 4 + 1,
            U24_MAX * 2 - 4 - 1,
            U24_MAX * 2 - 4,
            U24_MAX * 2 - 1,
            U24_MAX * 2,
            U24_MAX * 2 + 1,
            U24_MAX * 2 + 4,
            U24_MAX * 2 + 4 + 1,
        ];
        for input_size in testcases {
            let large_data = bytes::Bytes::from(vec![0; input_size]);
            let packet = mock_packet(large_data, 0);
            let mut reader = PacketReader::new(packet.reader());
            let mut last_seq = 0;
            let mut total_size = 0;
            while let Some((seq, packet)) = reader.next().unwrap() {
                if seq != 0 {
                    assert!(seq > last_seq);
                }
                total_size += packet.len();
                last_seq = seq;
            }
            assert_eq!(total_size, input_size);
        }
        for input_size in testcases {
            let large_data = bytes::Bytes::from(vec![0; input_size]);
            let packet = mock_packet(large_data, 0);
            let mut reader = PacketReader::new(packet.as_ref());
            let mut last_seq = 0;
            let mut total_size = 0;
            while let Some((seq, packet)) = reader.next_async().await.unwrap() {
                if seq != 0 {
                    assert!(seq > last_seq);
                }
                total_size += packet.len();
                last_seq = seq;
            }
            assert_eq!(total_size, input_size);
        }
    }

    #[test]
    fn next_rejects_packets_above_limit() {
        let payload = bytes::Bytes::from(vec![1u8; 17]);
        let packet = mock_packet(payload, 0);
        let mut reader = PacketReader::new_with_max_packet_size(packet.reader(), 16);

        let err = match reader.next() {
            Ok(value) => panic!(
                "packet larger than configured limit must fail, got {:?}",
                value.map(|(seq, _)| seq)
            ),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("packet exceeds configured limit"));
    }

    #[test]
    fn next_rejects_declared_payload_before_frame_body_arrives() {
        let mut reader = PacketReader::new_with_max_packet_size([17, 0, 0].reader(), 16);

        let err = match reader.next() {
            Ok(_) => panic!("declared payload above the limit must fail before its body arrives"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn next_allows_payload_exactly_at_limit() {
        let payload = bytes::Bytes::from(vec![1u8; 16]);
        let packet = mock_packet(payload, 0);
        let mut reader = PacketReader::new_with_max_packet_size(packet.reader(), 16);

        let (_, parsed) = reader.next().unwrap().unwrap();
        assert_eq!(parsed.len(), 16);
    }

    #[test]
    fn next_ignores_read_ahead_when_enforcing_limit() {
        let mut wire = BytesMut::new();
        wire.extend_from_slice(&[4, 0, 0, 0]);
        wire.extend_from_slice(b"ping");
        wire.extend_from_slice(&[4, 0, 0, 0]);
        wire.extend_from_slice(b"pong");
        let mut reader = PacketReader::new_with_max_packet_size(wire.freeze().reader(), 4);

        let (_, first) = reader.next().unwrap().unwrap();
        assert_eq!(&*first, b"ping");
        let (_, second) = reader.next().unwrap().unwrap();
        assert_eq!(&*second, b"pong");
    }

    #[test]
    fn next_rejects_multi_frame_payload_above_limit() {
        let payload = bytes::Bytes::from(vec![1u8; U24_MAX + 1]);
        let packet = mock_packet(payload, 0);
        let mut reader = PacketReader::new_with_max_packet_size(packet.reader(), U24_MAX);

        let err = match reader.next() {
            Ok(_) => panic!("multi-frame payload above the limit must fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("packet exceeds configured limit"));
    }

    #[test]
    fn reuse_or_create_buf_guarantees_spare_capacity_on_large_shared_buffer() {
        let large_size = 2 * 1024 * 1024; // 2 MB > PACKET_LARGE_BUFFER_SIZE (1 MB)
        let original = bytes::Bytes::from(vec![0x42u8; large_size]);
        let shared = original.clone(); // ensure refcount > 1, forcing Err(remain)

        let buf = reuse_or_create_buf(shared, large_size);
        assert!(
            buf.capacity() > buf.len(),
            "allocated buffer must have spare capacity to receive more data: cap={}, len={}",
            buf.capacity(),
            buf.len()
        );
        assert_eq!(buf.len(), large_size);
    }

    #[test]
    fn read_into_bytesmut_reserves_when_spare_capacity_is_zero() {
        let mut buf = BytesMut::with_capacity(10);
        buf.extend_from_slice(b"0123456789");
        assert_eq!(buf.capacity(), buf.len());

        let mut data = &b"hello"[..];
        let n = read_into_bytesmut(&mut data, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[10..], b"hello");
    }
}
