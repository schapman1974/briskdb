//! Request framing above the envelope codec; data commands are not dispatched here.

use std::io;

use bytes::{BufMut, Bytes, BytesMut};

use super::{Frame, HEADER_BYTES, Opcode, invalid};
use crate::document::{
    BsonCodecOptions, BsonDocument, BsonValue, decode_document_with_options, encode_document,
};

/// Conservative initial listener budget, also advertised during discovery.
pub const MAX_BOOTSTRAP_MESSAGE_BYTES: usize = 1024 * 1024;
pub const MAX_BOOTSTRAP_BSON_BYTES: usize = 512 * 1024;
const MAX_DECODED_DOCUMENT_BYTES: usize = 4 * 1024 * 1024;
const MAX_SEQUENCE_DOCUMENTS: usize = 1000;
const MAX_SEQUENCES: usize = 16;

#[derive(Debug)]
pub struct DocumentSequence {
    pub identifier: String,
    /// Validated, exact BSON bytes. No JSON conversion or expanded retained batch.
    pub documents: Vec<Bytes>,
}

#[derive(Debug)]
pub struct Request {
    pub request_id: i32,
    pub database: String,
    pub body: BsonDocument,
    pub sequences: Vec<DocumentSequence>,
    pub more_to_come: bool,
    pub legacy_handshake: bool,
}

/// Malformed transport/BSON requests are fatal to the connection. Errors are redacted.
pub fn decode_request(frame: Frame) -> io::Result<Request> {
    if frame.response_to != 0 || frame.payload.len() > MAX_BOOTSTRAP_MESSAGE_BYTES - HEADER_BYTES {
        return Err(invalid("invalid Mongo request envelope"));
    }
    match frame.opcode {
        Opcode::Message => decode_message(frame),
        Opcode::Query => decode_legacy_handshake(frame),
        Opcode::Reply => Err(invalid("Mongo reply received as a request")),
    }
}

pub(super) fn document(bytes: &[u8]) -> io::Result<BsonDocument> {
    decode_document_with_options(
        bytes,
        &BsonCodecOptions::new()
            .with_max_document_bytes(MAX_BOOTSTRAP_BSON_BYTES)
            .with_max_decoded_bytes(MAX_DECODED_DOCUMENT_BYTES),
    )
    .map_err(|_| invalid("invalid or over-budget Mongo BSON document"))
}

fn int32(bytes: &[u8]) -> io::Result<i32> {
    let prefix = bytes
        .get(..4)
        .ok_or_else(|| invalid("truncated Mongo integer"))?;
    Ok(i32::from_le_bytes(
        prefix.try_into().expect("prefix length checked"),
    ))
}

fn bson_slice(bytes: &mut Bytes) -> io::Result<Bytes> {
    let length =
        usize::try_from(int32(bytes)?).map_err(|_| invalid("invalid Mongo BSON length"))?;
    if !(5..=MAX_BOOTSTRAP_BSON_BYTES).contains(&length) || length > bytes.len() {
        return Err(invalid("invalid Mongo BSON boundary"));
    }
    Ok(bytes.split_to(length))
}

fn cstring(bytes: &mut Bytes) -> io::Result<String> {
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| invalid("unterminated Mongo string"))?;
    if end == 0 || end > 255 {
        return Err(invalid("invalid Mongo string length"));
    }
    let value = std::str::from_utf8(&bytes[..end])
        .map_err(|_| invalid("invalid Mongo string encoding"))?
        .to_owned();
    let _ = bytes.split_to(end + 1);
    Ok(value)
}

fn decode_message(frame: Frame) -> io::Result<Request> {
    let flags = int32(&frame.payload)? as u32;
    if flags & 0xffff & !3 != 0 {
        return Err(invalid("unknown required Mongo flags"));
    }
    let mut payload = frame.payload.clone();
    let _ = payload.split_to(4);
    if flags & 1 != 0 {
        if payload.len() < 4 {
            return Err(invalid("truncated Mongo checksum"));
        }
        let boundary = frame.payload.len() - 4;
        let expected = int32(&frame.payload[boundary..])? as u32;
        if checksum(&frame, &frame.payload[..boundary]) != expected {
            return Err(invalid("invalid Mongo checksum"));
        }
        payload.truncate(payload.len() - 4);
    }
    let mut body = None;
    let mut sequences: Vec<DocumentSequence> = Vec::new();
    let mut document_count = 0;
    while !payload.is_empty() {
        let kind = payload.split_to(1)[0];
        match kind {
            0 => {
                if body.is_some() {
                    return Err(invalid("duplicate Mongo body section"));
                }
                body = Some(document(&bson_slice(&mut payload)?)?);
            }
            1 => {
                if sequences.len() == MAX_SEQUENCES {
                    return Err(invalid("Mongo sequence budget exceeded"));
                }
                let length = usize::try_from(int32(&payload)?)
                    .map_err(|_| invalid("invalid Mongo sequence length"))?;
                if length < 6 || length > payload.len() {
                    return Err(invalid("invalid Mongo sequence boundary"));
                }
                let mut sequence = payload.split_to(length);
                let _ = sequence.split_to(4);
                let identifier = cstring(&mut sequence)?;
                if identifier.contains('.')
                    || sequences.iter().any(|item| item.identifier == identifier)
                {
                    return Err(invalid(
                        "unsupported or duplicate Mongo sequence identifier",
                    ));
                }
                let mut documents = Vec::new();
                while !sequence.is_empty() {
                    document_count += 1;
                    if document_count > MAX_SEQUENCE_DOCUMENTS {
                        return Err(invalid("Mongo sequence document budget exceeded"));
                    }
                    let raw = bson_slice(&mut sequence)?;
                    document(&raw)?;
                    documents.push(raw);
                }
                sequences.push(DocumentSequence {
                    identifier,
                    documents,
                });
            }
            _ => return Err(invalid("unsupported Mongo section kind")),
        }
    }
    let body = body.ok_or_else(|| invalid("missing Mongo body section"))?;
    if sequences
        .iter()
        .any(|sequence| body.get_first(&sequence.identifier).is_some())
    {
        return Err(invalid("Mongo sequence conflicts with body field"));
    }
    let database = match body.get_first("$db") {
        Some(BsonValue::String(database)) if valid_database(database) => database.clone(),
        _ => return Err(invalid("missing or invalid Mongo database")),
    };
    if body
        .iter()
        .next()
        .is_none_or(|(name, _)| name.starts_with('$'))
    {
        return Err(invalid("missing Mongo command"));
    }
    Ok(Request {
        request_id: frame.request_id,
        database,
        body,
        sequences,
        more_to_come: flags & 2 != 0,
        legacy_handshake: false,
    })
}

fn valid_database(database: &str) -> bool {
    !database.is_empty()
        && database.len() <= 63
        && !database.contains(['\0', '.', '/', '\\', ' ', '"', '$'])
}

fn decode_legacy_handshake(frame: Frame) -> io::Result<Request> {
    let flags = int32(&frame.payload)?;
    if flags & !4 != 0 {
        return Err(invalid("unsupported legacy Mongo flags"));
    }
    let mut payload = frame.payload;
    let _ = payload.split_to(4);
    let namespace = cstring(&mut payload)?;
    let database = namespace
        .strip_suffix(".$cmd")
        .filter(|name| valid_database(name))
        .ok_or_else(|| invalid("legacy Mongo queries are handshake-only"))?
        .to_owned();
    let skip = int32(&payload)?;
    let _ = payload.split_to(4);
    let count = int32(&payload)?;
    let _ = payload.split_to(4);
    if skip != 0 || ![-1, 1].contains(&count) {
        return Err(invalid("unsupported legacy Mongo query options"));
    }
    let body = document(&bson_slice(&mut payload)?)?;
    if !payload.is_empty()
        || !matches!(
            body.iter().next().map(|(name, _)| name),
            Some("hello" | "ismaster" | "isMaster")
        )
    {
        return Err(invalid("legacy Mongo queries are handshake-only"));
    }
    if body.get_first("$db").is_some() {
        return Err(invalid("conflicting legacy Mongo database"));
    }
    Ok(Request {
        request_id: frame.request_id,
        database,
        body,
        sequences: Vec::new(),
        more_to_come: false,
        legacy_handshake: true,
    })
}

pub(super) fn reply(request: &Request, body: &BsonDocument, request_id: i32) -> io::Result<Frame> {
    let bson = encode_document(body).map_err(|_| invalid("unable to encode Mongo reply"))?;
    let mut payload = BytesMut::new();
    payload.put_i32_le(0);
    let opcode = if request.legacy_handshake {
        payload.put_i64_le(0);
        payload.put_i32_le(0);
        payload.put_i32_le(1);
        Opcode::Reply
    } else {
        payload.put_u8(0);
        Opcode::Message
    };
    payload.extend_from_slice(&bson);
    Ok(Frame {
        request_id,
        response_to: request.request_id,
        opcode,
        payload: payload.freeze(),
    })
}

// Table-driven Castagnoli CRC, independently implemented; no runtime table allocation.
const CRC_TABLE: [u32; 256] = {
    let mut table = [0; 256];
    let mut index = 0;
    while index < 256 {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            value = (value >> 1) ^ if value & 1 != 0 { 0x82f63b78 } else { 0 };
            bit += 1;
        }
        table[index] = value;
        index += 1;
    }
    table
};

fn crc_update(mut crc: u32, bytes: &[u8]) -> u32 {
    for byte in bytes {
        crc = CRC_TABLE[((crc ^ u32::from(*byte)) & 255) as usize] ^ (crc >> 8);
    }
    crc
}

fn checksum(frame: &Frame, payload: &[u8]) -> u32 {
    let mut header = [0u8; HEADER_BYTES];
    header[..4].copy_from_slice(&((frame.payload.len() + HEADER_BYTES) as i32).to_le_bytes());
    header[4..8].copy_from_slice(&frame.request_id.to_le_bytes());
    header[8..12].copy_from_slice(&frame.response_to.to_le_bytes());
    header[12..16].copy_from_slice(&frame.opcode.number().to_le_bytes());
    !crc_update(crc_update(!0, &header), payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn body() -> BsonDocument {
        BsonDocument::from_entries([
            ("ping", BsonValue::Int32(1)),
            ("$db", BsonValue::String("admin".into())),
        ])
        .unwrap()
    }

    fn message(flags: u32) -> Frame {
        let mut payload = BytesMut::new();
        payload.put_u32_le(flags);
        payload.put_u8(0);
        payload.extend_from_slice(&encode_document(&body()).unwrap());
        Frame {
            request_id: 17,
            response_to: 0,
            opcode: Opcode::Message,
            payload: payload.freeze(),
        }
    }

    #[test]
    fn checksum_known_vector_and_corruption() {
        assert_eq!(!crc_update(!0, b"123456789"), 0xe3069283);
        let mut frame = message(1);
        let mut payload = BytesMut::from(frame.payload.as_ref());
        payload.put_u32_le(0);
        frame.payload = payload.clone().freeze();
        let crc = checksum(&frame, &payload[..payload.len() - 4]);
        let end = payload.len();
        payload[end - 4..].copy_from_slice(&crc.to_le_bytes());
        frame.payload = payload.clone().freeze();
        assert_eq!(decode_request(frame.clone()).unwrap().database, "admin");
        frame.request_id += 1;
        assert!(decode_request(frame).is_err());
        payload[end - 1] ^= 1;
        let mut corrupt = message(1);
        corrupt.payload = payload.freeze();
        assert!(decode_request(corrupt).is_err());
    }

    #[test]
    fn flags_required_optional_and_one_way() {
        assert!(decode_request(message(4)).is_err());
        assert!(!decode_request(message(1 << 30)).unwrap().more_to_come);
        assert!(decode_request(message(2)).unwrap().more_to_come);
        let mut frame = message(0);
        frame.response_to = 1;
        assert!(decode_request(frame).is_err());
    }

    #[test]
    fn sections_validate_order_duplicates_and_conflicts() {
        let raw = encode_document(&BsonDocument::new()).unwrap();
        let mut sequence = BytesMut::new();
        sequence.put_u8(1);
        sequence.put_i32_le((4 + 10 + raw.len()) as i32);
        sequence.extend_from_slice(b"documents\0");
        sequence.extend_from_slice(&raw);
        let mut payload = BytesMut::new();
        payload.put_u32_le(0);
        payload.extend_from_slice(&sequence);
        payload.extend_from_slice(&message(0).payload[4..]);
        let mut frame = message(0);
        frame.payload = payload.clone().freeze();
        let parsed = decode_request(frame.clone()).unwrap();
        assert_eq!(parsed.sequences[0].identifier, "documents");
        assert_eq!(parsed.sequences[0].documents.len(), 1);
        payload.extend_from_slice(&sequence);
        frame.payload = payload.freeze();
        assert!(decode_request(frame).is_err());

        let mut duplicate = message(0);
        let mut payload = BytesMut::from(duplicate.payload.as_ref());
        payload.extend_from_slice(&duplicate.payload[4..]);
        duplicate.payload = payload.freeze();
        assert!(decode_request(duplicate).is_err());

        let mut conflict = message(0);
        let mut payload = BytesMut::from(conflict.payload.as_ref());
        let mut conflicting_sequence = BytesMut::new();
        conflicting_sequence.put_u8(1);
        conflicting_sequence.put_i32_le((4 + 4 + raw.len()) as i32);
        conflicting_sequence.extend_from_slice(b"$db\0");
        conflicting_sequence.extend_from_slice(&raw);
        payload.extend_from_slice(&conflicting_sequence);
        conflict.payload = payload.freeze();
        assert!(decode_request(conflict).is_err());
    }

    #[test]
    fn sequence_counts_and_lengths_are_bounded() {
        for length in [i32::MIN, -1, 0, 5, i32::MAX] {
            let mut frame = message(0);
            let mut payload = BytesMut::from(frame.payload.as_ref());
            payload.put_u8(1);
            payload.put_i32_le(length);
            frame.payload = payload.freeze();
            assert!(decode_request(frame).is_err());
        }
        let mut frame = message(0);
        let mut payload = BytesMut::from(frame.payload.as_ref());
        for index in 0..=MAX_SEQUENCES {
            let identifier = format!("s{index}\0");
            payload.put_u8(1);
            payload.put_i32_le((4 + identifier.len()) as i32);
            payload.extend_from_slice(identifier.as_bytes());
        }
        frame.payload = payload.freeze();
        assert!(decode_request(frame).is_err());

        let mut frame = message(0);
        let mut payload = BytesMut::from(frame.payload.as_ref());
        payload.put_u8(1);
        payload.put_i32_le((4 + 10 + 5 * (MAX_SEQUENCE_DOCUMENTS + 1)) as i32);
        payload.extend_from_slice(b"documents\0");
        for _ in 0..=MAX_SEQUENCE_DOCUMENTS {
            payload.extend_from_slice(&[5, 0, 0, 0, 0]);
        }
        frame.payload = payload.freeze();
        assert!(decode_request(frame).is_err());
    }

    #[test]
    fn legacy_requests_are_strictly_handshake_only() {
        let mut payload = BytesMut::new();
        payload.put_i32_le(0);
        payload.extend_from_slice(b"admin.$cmd\0");
        payload.put_i32_le(0);
        payload.put_i32_le(-1);
        let body = BsonDocument::from_entries([("hello", BsonValue::Int32(1))]).unwrap();
        payload.extend_from_slice(&encode_document(&body).unwrap());
        let frame = Frame {
            request_id: 1,
            response_to: 0,
            opcode: Opcode::Query,
            payload: payload.clone().freeze(),
        };
        assert!(decode_request(frame.clone()).unwrap().legacy_handshake);
        for end in 0..payload.len() {
            let mut truncated = frame.clone();
            truncated.payload.truncate(end);
            assert!(decode_request(truncated).is_err());
        }
        let mut invalid = frame.clone();
        payload[15..19].copy_from_slice(&1i32.to_le_bytes());
        invalid.payload = payload.freeze();
        assert!(decode_request(invalid).is_err());
        let mut reply = frame;
        reply.opcode = Opcode::Reply;
        assert!(decode_request(reply).is_err());
    }

    #[test]
    fn every_truncated_payload_and_unknown_section_fails() {
        let complete = message(0);
        for end in 0..complete.payload.len() {
            let mut frame = complete.clone();
            frame.payload.truncate(end);
            assert!(decode_request(frame).is_err());
        }
        let mut unknown = message(0);
        let mut payload = BytesMut::from(unknown.payload.as_ref());
        payload[4] = 3;
        unknown.payload = payload.freeze();
        assert!(decode_request(unknown).is_err());
    }

    proptest! {
        #[test]
        fn arbitrary_payloads_never_panic(payload in proptest::collection::vec(any::<u8>(), 0..1024)) {
            let _ = decode_request(Frame { request_id: 1, response_to: 0, opcode: Opcode::Message, payload: Bytes::from(payload) });
        }
    }
}
