//! Opt-in Mongo transport and initial document command adapter.
//!
//! This codec validates message boundaries, not BSON or command semantics.
//! The standalone public codec is uncompressed; listeners separately negotiate
//! bounded zlib transport without changing this envelope API.

mod client_metadata;
mod commands;
mod compression;
mod events;
mod listener;
mod metrics;
mod readiness;
mod wire;

pub use client_metadata::{MongoClientMetadata, MongoDriverKind};
pub use listener::MongoServer;
pub use metrics::{
    MONGO_LATENCY_UPPER_BOUNDS_MICROS, MONGO_READ_SHARD_FANOUT_UPPER_BOUNDS, MongoCommandKind,
    MongoCommandMetrics, MongoCursorMetrics, MongoMetricsSnapshot, MongoReadMetrics,
    MongoTransportFailures,
};
pub use readiness::{
    MongoListenerState, MongoReadinessReason, MongoReadinessSnapshot, MongoSecurityMode,
};
pub use wire::{DocumentSequence, MAX_BOOTSTRAP_MESSAGE_BYTES, Request, decode_request};

use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

const HEADER_BYTES: usize = 16;
/// General envelope ceiling. Listeners may enforce and advertise a smaller budget.
pub const MAX_MESSAGE_BYTES: usize = 48_000_000;

/// Opcodes admitted by the initial uncompressed transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Opcode {
    Reply,
    Query,
    Message,
}

impl Opcode {
    const fn number(self) -> i32 {
        match self {
            Self::Reply => 1,
            Self::Query => 2004,
            Self::Message => 2013,
        }
    }

    fn from_number(number: i32) -> io::Result<Self> {
        match number {
            1 => Ok(Self::Reply),
            2004 => Ok(Self::Query),
            2013 => Ok(Self::Message),
            _ => Err(invalid("unsupported Mongo opcode")),
        }
    }
}

/// An owned message envelope. Payload validation belongs to the opcode parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub request_id: i32,
    pub response_to: i32,
    pub opcode: Opcode,
    pub payload: Bytes,
}

/// Incremental, allocation-bounded message framing for a TCP byte stream.
///
/// Limits are checked from the length prefix before reserving or consuming data.
/// A framing error is fatal: callers must close the connection rather than attempt
/// to resynchronize. EOF in a partial frame is an error, not a clean disconnect.
#[derive(Debug, Clone)]
pub struct FrameCodec {
    max_message_bytes: usize,
}

impl Default for FrameCodec {
    fn default() -> Self {
        Self {
            max_message_bytes: MAX_MESSAGE_BYTES,
        }
    }
}

impl FrameCodec {
    /// Select a narrower transport budget; the global ceiling cannot be raised.
    pub fn with_max_message_bytes(max_message_bytes: usize) -> io::Result<Self> {
        if !(HEADER_BYTES..=MAX_MESSAGE_BYTES).contains(&max_message_bytes) {
            return Err(invalid("invalid Mongo message budget"));
        }
        Ok(Self { max_message_bytes })
    }

    fn checked_length(&self, length: i32) -> io::Result<usize> {
        let length =
            usize::try_from(length).map_err(|_| invalid("invalid Mongo message length"))?;
        if !(HEADER_BYTES..=self.max_message_bytes).contains(&length) {
            return Err(invalid("Mongo message length outside transport budget"));
        }
        Ok(length)
    }
}

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, source: &mut BytesMut) -> io::Result<Option<Frame>> {
        if source.len() < 4 {
            return Ok(None);
        }
        let length = self.checked_length(i32::from_le_bytes(
            source[..4].try_into().expect("length prefix checked"),
        ))?;
        if source.len() < HEADER_BYTES {
            return Ok(None);
        }
        let opcode = Opcode::from_number(i32::from_le_bytes(
            source[12..16].try_into().expect("header size checked"),
        ))?;
        if source.len() < length {
            return Ok(None);
        }
        let mut message = source.split_to(length).freeze();
        message.advance(4);
        let request_id = message.get_i32_le();
        let response_to = message.get_i32_le();
        message.advance(4);
        Ok(Some(Frame {
            request_id,
            response_to,
            opcode,
            payload: message,
        }))
    }

    fn decode_eof(&mut self, source: &mut BytesMut) -> io::Result<Option<Frame>> {
        match self.decode(source)? {
            Some(frame) => Ok(Some(frame)),
            None if source.is_empty() => Ok(None),
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated Mongo message",
            )),
        }
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = io::Error;

    fn encode(&mut self, frame: Frame, destination: &mut BytesMut) -> io::Result<()> {
        let length = frame
            .payload
            .len()
            .checked_add(HEADER_BYTES)
            .and_then(|length| i32::try_from(length).ok())
            .ok_or_else(|| invalid("Mongo message length overflow"))?;
        self.checked_length(length)?;
        // Validate before modifying the destination, including for a narrower budget.
        destination.reserve(length as usize);
        destination.put_i32_le(length);
        destination.put_i32_le(frame.request_id);
        destination.put_i32_le(frame.response_to);
        destination.put_i32_le(frame.opcode.number());
        destination.extend_from_slice(&frame.payload);
        Ok(())
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn arbitrary_input_never_panics_or_grows_the_input(
            input in proptest::collection::vec(any::<u8>(), 0..512)
        ) {
            let mut source = BytesMut::from(input.as_slice());
            let capacity = source.capacity();
            let _ = FrameCodec::default().decode(&mut source);
            prop_assert!(source.len() <= input.len());
            prop_assert!(source.capacity() <= capacity);
        }

        #[test]
        fn randomized_headers_and_payloads_round_trip(
            request_id in any::<i32>(), response_to in any::<i32>(),
            payload in proptest::collection::vec(any::<u8>(), 0..2048)
        ) {
            let expected = Frame { request_id, response_to, opcode: Opcode::Message,
                payload: Bytes::from(payload) };
            let mut source = encoded(expected.clone());
            prop_assert_eq!(FrameCodec::default().decode(&mut source).unwrap(), Some(expected));
            prop_assert!(source.is_empty());
        }
    }

    fn frame(opcode: Opcode) -> Frame {
        Frame {
            request_id: -7,
            response_to: 42,
            opcode,
            payload: Bytes::from_static(b"payload"),
        }
    }

    fn encoded(frame: Frame) -> BytesMut {
        let mut bytes = BytesMut::new();
        FrameCodec::default().encode(frame, &mut bytes).unwrap();
        bytes
    }

    #[test]
    fn golden_little_endian_envelope() {
        let bytes = encoded(frame(Opcode::Message));
        assert_eq!(
            &bytes[..16],
            &[23, 0, 0, 0, 249, 255, 255, 255, 42, 0, 0, 0, 221, 7, 0, 0]
        );
        assert_eq!(&bytes[16..], b"payload");
    }

    #[test]
    fn every_fragment_boundary_preserves_incomplete_input() {
        let expected = frame(Opcode::Message);
        let bytes = encoded(expected.clone());
        for split in 0..bytes.len() {
            let mut codec = FrameCodec::default();
            let mut source = BytesMut::from(&bytes[..split]);
            let original = source.clone();
            assert!(codec.decode(&mut source).unwrap().is_none());
            assert_eq!(source, original);
            source.extend_from_slice(&bytes[split..]);
            assert_eq!(codec.decode(&mut source).unwrap(), Some(expected.clone()));
            assert!(source.is_empty());
        }
    }

    #[test]
    fn coalesced_frames_and_partial_successor() {
        let mut codec = FrameCodec::default();
        let mut source = encoded(frame(Opcode::Query));
        let second = encoded(frame(Opcode::Message));
        source.extend_from_slice(&second);
        source.extend_from_slice(&second[..9]);
        assert_eq!(
            codec.decode(&mut source).unwrap(),
            Some(frame(Opcode::Query))
        );
        assert_eq!(
            codec.decode(&mut source).unwrap(),
            Some(frame(Opcode::Message))
        );
        assert!(codec.decode(&mut source).unwrap().is_none());
        assert_eq!(&source[..], &second[..9]);
    }

    #[test]
    fn invalid_lengths_fail_from_prefix_without_allocation() {
        for length in [i32::MIN, -1, 0, 15, MAX_MESSAGE_BYTES as i32 + 1, i32::MAX] {
            let mut source = BytesMut::from(&length.to_le_bytes()[..]);
            let capacity = source.capacity();
            assert_eq!(
                FrameCodec::default()
                    .decode(&mut source)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(source.capacity(), capacity);
            assert_eq!(source.len(), 4);
        }
    }

    #[test]
    fn maximum_length_header_waits_without_reserving_the_advertised_body() {
        let mut source = encoded(frame(Opcode::Message));
        source[..4].copy_from_slice(&(MAX_MESSAGE_BYTES as i32).to_le_bytes());
        source.truncate(16);
        let capacity = source.capacity();
        assert!(FrameCodec::default().decode(&mut source).unwrap().is_none());
        assert_eq!(source.len(), 16);
        assert_eq!(source.capacity(), capacity);
    }

    #[test]
    fn unsupported_and_compressed_opcodes_are_fatal_before_body() {
        for opcode in [0i32, 2001, 2012, i32::MAX] {
            let mut source = encoded(frame(Opcode::Message));
            source[12..16].copy_from_slice(&opcode.to_le_bytes());
            source.truncate(16);
            assert!(FrameCodec::default().decode(&mut source).is_err());
            assert_eq!(source.len(), 16);
        }
    }

    #[test]
    fn eof_distinguishes_clean_disconnect_from_every_truncated_prefix() {
        let bytes = encoded(frame(Opcode::Message));
        assert!(
            FrameCodec::default()
                .decode_eof(&mut BytesMut::new())
                .unwrap()
                .is_none()
        );
        for split in 1..bytes.len() {
            let mut source = BytesMut::from(&bytes[..split]);
            assert_eq!(
                FrameCodec::default()
                    .decode_eof(&mut source)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::UnexpectedEof
            );
        }
        let mut source = bytes;
        assert_eq!(
            FrameCodec::default().decode_eof(&mut source).unwrap(),
            Some(frame(Opcode::Message))
        );
    }

    #[test]
    fn narrowed_budget_checks_both_directions_without_partial_write() {
        let mut codec = FrameCodec::with_max_message_bytes(23).unwrap();
        let mut source = encoded(frame(Opcode::Reply));
        assert_eq!(
            codec.decode(&mut source).unwrap(),
            Some(frame(Opcode::Reply))
        );
        let mut codec = FrameCodec::with_max_message_bytes(22).unwrap();
        let mut source = encoded(frame(Opcode::Message));
        assert!(codec.decode(&mut source).is_err());
        let mut destination = BytesMut::from(&b"existing"[..]);
        assert!(
            codec
                .encode(frame(Opcode::Message), &mut destination)
                .is_err()
        );
        assert_eq!(&destination[..], b"existing");
        for budget in [0, 15, MAX_MESSAGE_BYTES + 1, usize::MAX] {
            assert!(FrameCodec::with_max_message_bytes(budget).is_err());
        }
    }
}
