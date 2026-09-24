//! Independently implemented, per-connection zlib transport.
//!
//! Both wire and expanded messages obey the existing listener ceiling. Inflation
//! and reply compression run in the listener's bounded blocking-parser slots.
//! Public Frame/Opcode/FrameCodec remain the uncompressed envelope API.

use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};
use tokio_util::codec::{Decoder, Encoder};

use super::{
    Frame, FrameCodec, HEADER_BYTES, MAX_BOOTSTRAP_MESSAGE_BYTES, Opcode, Request, invalid,
};
use crate::document::{BsonDocument, BsonValue};

const COMPRESSED_OPCODE: i32 = 2012;
const WRAPPER_BYTES: usize = HEADER_BYTES + 9;
const ZLIB_ID: u8 = 2;
// Leave room for the OP_COMPRESSED wrapper and zlib stored-block overhead.
// PyMongo sizes its batches before compression, including at level zero.
pub(super) const ADVERTISED_ZLIB_MESSAGE_BYTES: usize = MAX_BOOTSTRAP_MESSAGE_BYTES - 1024;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(super) enum Incoming {
    Plain(Frame),
    Zlib {
        request_id: i32,
        expanded_bytes: usize,
        payload: Bytes,
    },
}

impl Incoming {
    pub(super) const fn is_compressed(&self) -> bool {
        matches!(self, Self::Zlib { .. })
    }

    /// Call only from the connection's joined blocking-parser task.
    pub(super) fn into_frame(self) -> io::Result<Frame> {
        match self {
            Self::Plain(frame) => Ok(frame),
            Self::Zlib {
                request_id,
                expanded_bytes,
                payload,
            } => {
                // The extra byte detects streams larger than their declaration.
                // Never use an unbounded Read::read_to_end inflater here.
                let mut output = vec![0; expanded_bytes + 1];
                let mut decoder = Decompress::new(true);
                let status = decoder
                    .decompress(&payload, &mut output, FlushDecompress::Finish)
                    .map_err(|_| invalid("invalid Mongo zlib stream"))?;
                if status != Status::StreamEnd
                    || decoder.total_out() != expanded_bytes as u64
                    || decoder.total_in() != payload.len() as u64
                {
                    return Err(invalid("Mongo zlib stream length mismatch"));
                }
                output.truncate(expanded_bytes);
                Ok(Frame {
                    request_id,
                    response_to: 0,
                    opcode: Opcode::Message,
                    payload: Bytes::from(output),
                })
            }
        }
    }
}

pub(super) struct TransportCodec {
    plain: FrameCodec,
    zlib: bool,
}

impl TransportCodec {
    pub(super) fn new() -> io::Result<Self> {
        Ok(Self {
            plain: FrameCodec::with_max_message_bytes(MAX_BOOTSTRAP_MESSAGE_BYTES)?,
            zlib: false,
        })
    }

    pub(super) fn enable_zlib(&mut self) {
        self.zlib = true;
    }
}

impl Decoder for TransportCodec {
    type Item = Incoming;
    type Error = io::Error;

    fn decode(&mut self, source: &mut BytesMut) -> io::Result<Option<Incoming>> {
        if source.len() < HEADER_BYTES
            || i32::from_le_bytes(source[12..16].try_into().unwrap()) != COMPRESSED_OPCODE
        {
            return self
                .plain
                .decode(source)
                .map(|frame| frame.map(Incoming::Plain));
        }
        let length = self
            .plain
            .checked_length(i32::from_le_bytes(source[..4].try_into().unwrap()))?;
        if !self.zlib {
            return Err(invalid("Mongo compression was not negotiated"));
        }
        if length < WRAPPER_BYTES {
            return Err(invalid("truncated Mongo compression wrapper"));
        }
        if source.len() < WRAPPER_BYTES {
            return Ok(None);
        }
        // Reject unsupported/nested opcodes, replies, codecs and advertised
        // inflation sizes before waiting for a body or allocating output.
        if source[8..12] != [0; 4]
            || i32::from_le_bytes(source[16..20].try_into().unwrap()) != Opcode::Message.number()
            || source[24] != ZLIB_ID
        {
            return Err(invalid("invalid Mongo compression envelope"));
        }
        let expanded_bytes =
            usize::try_from(i32::from_le_bytes(source[20..24].try_into().unwrap()))
                .map_err(|_| invalid("invalid Mongo expanded message size"))?;
        if !(5..=MAX_BOOTSTRAP_MESSAGE_BYTES - HEADER_BYTES).contains(&expanded_bytes) {
            return Err(invalid("Mongo expanded message outside transport budget"));
        }
        if source.len() < length {
            return Ok(None);
        }
        let mut message = source.split_to(length).freeze();
        message.advance(4);
        let request_id = message.get_i32_le();
        message.advance(WRAPPER_BYTES - 8);
        Ok(Some(Incoming::Zlib {
            request_id,
            expanded_bytes,
            payload: message,
        }))
    }

    fn decode_eof(&mut self, source: &mut BytesMut) -> io::Result<Option<Incoming>> {
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

pub(super) fn offers_zlib(body: &BsonDocument) -> bool {
    matches!(body.get_first("compression"), Some(BsonValue::Array(items))
        if items.iter().any(|item| matches!(item, BsonValue::String(name) if name == "zlib")))
}

pub(super) fn negotiated_zlib(request: &Request, reply: &BsonDocument) -> bool {
    !request.more_to_come
        && offers_zlib(&request.body)
        && matches!(
            request.body.iter().next(),
            Some(("hello" | "ismaster" | "isMaster", _))
        )
        && reply.get_first("ok") == Some(&BsonValue::Double(1.0))
        && offers_zlib(reply)
}

pub(super) fn validate_command(request: &Request) -> io::Result<()> {
    if matches!(
        request.body.iter().next(),
        Some((
            "hello"
                | "ismaster"
                | "isMaster"
                | "saslStart"
                | "saslContinue"
                | "getnonce"
                | "authenticate"
                | "createUser"
                | "updateUser"
                | "copydbSaslStart"
                | "copydbgetnonce"
                | "copydb",
            _
        ))
    ) {
        return Err(invalid("Mongo command must not be compressed"));
    }
    Ok(())
}

/// Encode in the joined blocking task, retaining plain replies when compression
/// would expand the message. Drivers must accept either response representation.
pub(super) fn encode_reply(frame: Frame, compress: bool) -> io::Result<BytesMut> {
    let message = frame.opcode == Opcode::Message;
    let mut plain = BytesMut::new();
    FrameCodec::with_max_message_bytes(MAX_BOOTSTRAP_MESSAGE_BYTES)?.encode(frame, &mut plain)?;
    if !compress || !message || plain.len() <= WRAPPER_BYTES {
        return Ok(plain);
    }
    // Fixed destination, strictly smaller than the valid uncompressed packet.
    // Incompressible or over-budget output falls back without growing this buffer.
    let mut output = vec![0; plain.len() - WRAPPER_BYTES - 1];
    let mut compressor = Compress::new(Compression::fast(), true);
    let status = compressor
        .compress(&plain[HEADER_BYTES..], &mut output, FlushCompress::Finish)
        .map_err(|_| invalid("unable to compress Mongo reply"))?;
    if status != Status::StreamEnd || compressor.total_in() != (plain.len() - HEADER_BYTES) as u64 {
        return Ok(plain);
    }
    let size = compressor.total_out() as usize;
    let mut result = BytesMut::with_capacity(WRAPPER_BYTES + size);
    result.put_i32_le((WRAPPER_BYTES + size) as i32);
    result.extend_from_slice(&plain[4..12]);
    result.put_i32_le(COMPRESSED_OPCODE);
    result.put_i32_le(Opcode::Message.number());
    result.put_i32_le((plain.len() - HEADER_BYTES) as i32);
    result.put_u8(ZLIB_ID);
    result.extend_from_slice(&output[..size]);
    Ok(result)
}
