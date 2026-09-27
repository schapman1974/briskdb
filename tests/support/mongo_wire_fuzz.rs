//! Shared by deterministic/proptest regressions and the instrumented fuzz target.
//! Pure codecs only: no sockets, database files, processes or global state.

use std::io;

use briskdb::{
    document::{BsonBinary, BsonDocument, BsonValue, decode_document, encode_document},
    protocol::mongo::{Frame, FrameCodec, MAX_BOOTSTRAP_MESSAGE_BYTES, Opcode, decode_request},
};
use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

fn encode(frame: Frame) -> BytesMut {
    let mut bytes = BytesMut::new();
    FrameCodec::with_max_message_bytes(MAX_BOOTSTRAP_MESSAGE_BYTES)
        .unwrap()
        .encode(frame, &mut bytes)
        .unwrap();
    bytes
}

fn request(frame: &Frame) {
    let first = decode_request(frame.clone());
    let second = decode_request(frame.clone());
    match (first, second) {
        (Ok(left), Ok(right)) => {
            assert_eq!(left.request_id, frame.request_id);
            assert_eq!(left.request_id, right.request_id);
            assert_eq!(left.database, right.database);
            assert_eq!(left.more_to_come, right.more_to_come);
            assert_eq!(left.legacy_handshake, right.legacy_handshake);
            assert!(left.body.representation_eq(&right.body));
            assert_eq!(left.sequences.len(), right.sequences.len());
            assert!(left.sequences.len() <= 16);
            assert!(
                left.sequences
                    .iter()
                    .map(|s| s.documents.len())
                    .sum::<usize>()
                    <= 1000
            );
            for (a, b) in left.sequences.iter().zip(&right.sequences) {
                assert_eq!(a.identifier, b.identifier);
                assert_eq!(a.documents, b.documents);
                for bytes in &a.documents {
                    assert!(decode_document(bytes).is_ok());
                }
            }
        }
        (Err(left), Err(right)) => {
            assert_eq!(left.kind(), right.kind());
            assert_eq!(left.to_string(), right.to_string());
        }
        _ => panic!("request decoding must be deterministic"),
    }
}

fn step(codec: &mut FrameCodec, source: &mut BytesMut, eof: bool) -> io::Result<Option<Frame>> {
    let before = source.to_vec();
    let capacity = source.capacity();
    let result = if eof {
        codec.decode_eof(source)
    } else {
        codec.decode(source)
    };
    assert!(
        source.capacity() <= capacity,
        "decoder reserved input capacity"
    );
    if let Ok(Some(frame)) = &result {
        let bytes = encode(frame.clone());
        assert_eq!(&before[..bytes.len()], bytes.as_ref());
        assert_eq!(&before[bytes.len()..], source.as_ref());
        request(frame);
    } else {
        assert_eq!(
            before.as_slice(),
            source.as_ref(),
            "incomplete/error input consumed"
        );
    }
    result
}

fn feed(data: &[u8], width: usize, budget: usize) -> (Vec<Frame>, Option<io::ErrorKind>) {
    let mut codec = FrameCodec::with_max_message_bytes(budget).unwrap();
    let mut source = BytesMut::new();
    let mut frames = Vec::new();
    for chunk in data.chunks(width) {
        source.extend_from_slice(chunk);
        loop {
            match step(&mut codec, &mut source, false) {
                Ok(Some(frame)) => frames.push(frame),
                Ok(None) => break,
                Err(error) => return (frames, Some(error.kind())), // Fatal: never resynchronize.
            }
        }
    }
    loop {
        match step(&mut codec, &mut source, true) {
            Ok(Some(frame)) => frames.push(frame),
            Ok(None) => return (frames, None),
            Err(error) => return (frames, Some(error.kind())),
        }
    }
}

// Independent bitwise Castagnoli implementation, not the production lookup table.
fn crc32c(bytes: &[u8]) -> u32 {
    let mut value = !0u32;
    for byte in bytes {
        value ^= u32::from(*byte);
        for _ in 0..8 {
            value = (value >> 1) ^ if value & 1 != 0 { 0x82f63b78 } else { 0 };
        }
    }
    !value
}

fn generated_message(data: &[u8], id: i32) -> Frame {
    let body = BsonDocument::from_entries([
        ("insert", BsonValue::from("items")),
        ("$db", BsonValue::from("fuzz")),
    ])
    .unwrap();
    let record = BsonDocument::from_entries([
        ("_id", BsonValue::Int32(id)),
        ("raw", BsonValue::Binary(BsonBinary::new(0, data.to_vec()))),
    ])
    .unwrap();
    let record = encode_document(&record).unwrap();
    let mut payload = BytesMut::new();
    payload.put_u32_le(3); // Checksum plus moreToCome.
    payload.put_u8(0);
    payload.extend_from_slice(&encode_document(&body).unwrap());
    payload.put_u8(1);
    payload.put_i32_le((4 + 10 + record.len()) as i32);
    payload.extend_from_slice(b"documents\0");
    payload.extend_from_slice(&record);
    payload.put_u32_le(0);
    let mut frame = Frame {
        request_id: id,
        response_to: 0,
        opcode: Opcode::Message,
        payload: payload.freeze(),
    };
    let encoded = encode(frame.clone());
    let checksum = crc32c(&encoded[..encoded.len() - 4]);
    let mut payload = BytesMut::from(frame.payload.as_ref());
    let end = payload.len();
    payload[end - 4..].copy_from_slice(&checksum.to_le_bytes());
    frame.payload = payload.freeze();
    let parsed = decode_request(frame.clone()).expect("valid generated checksummed sequence");
    assert_eq!(parsed.database, "fuzz");
    assert!(parsed.more_to_come);
    assert!(!parsed.legacy_handshake);
    assert!(parsed.body.representation_eq(&body));
    assert_eq!(parsed.sequences.len(), 1);
    assert_eq!(parsed.sequences[0].identifier, "documents");
    assert_eq!(parsed.sequences[0].documents, [Bytes::from(record)]);
    let mut corrupt = frame.clone();
    corrupt.request_id ^= 1;
    assert!(
        decode_request(corrupt).is_err(),
        "CRC must cover the header"
    );
    let mut corrupt = frame.clone();
    let mut payload = BytesMut::from(corrupt.payload.as_ref());
    payload[end - 1] ^= 1;
    corrupt.payload = payload.freeze();
    assert!(decode_request(corrupt).is_err(), "corrupt CRC accepted");
    frame
}

pub fn check(data: &[u8]) {
    // Bound harness copying/work separately from the product's transport ceiling.
    if data.len() > 16 * 1024 {
        return;
    }
    let byte = |index| data.get(index).copied().unwrap_or(0);
    let id = i32::from_le_bytes(std::array::from_fn(byte));
    let width = usize::from(byte(4)) + 1;
    let budget = 16 + usize::from(u16::from_le_bytes([byte(5), byte(6)]));
    for budget in [budget, MAX_BOOTSTRAP_MESSAGE_BYTES] {
        assert_eq!(
            feed(data, data.len().max(1), budget),
            feed(data, width, budget)
        );
    }
    // Also reach each request parser without requiring a valid envelope prefix.
    for opcode in [Opcode::Message, Opcode::Query, Opcode::Reply] {
        request(&Frame {
            request_id: id,
            response_to: 0,
            opcode,
            payload: Bytes::copy_from_slice(data),
        });
    }
    assert_eq!(crc32c(b"123456789"), 0xe3069283);
    let message = generated_message(data, id);
    // Mutate/truncate a valid scaffold with its checksum removed so malformed
    // section/BSON branches are reachable without first rediscovering CRC32C.
    let mut mutated = message.clone();
    let mut payload = BytesMut::from(&message.payload[..message.payload.len() - 4]);
    payload[..4].copy_from_slice(&2u32.to_le_bytes());
    let position = usize::from(u16::from_le_bytes([byte(9), byte(10)])) % payload.len();
    payload[position] ^= byte(11) | 1;
    mutated.payload = payload.freeze();
    request(&mutated);
    let mut truncated = mutated;
    truncated.payload.truncate(position);
    request(&truncated);
    let mut legacy = BytesMut::new();
    legacy.put_i32_le(0);
    legacy.extend_from_slice(b"fuzz.$cmd\0");
    legacy.put_i32_le(0);
    legacy.put_i32_le(-1);
    legacy.extend_from_slice(
        &encode_document(&BsonDocument::from_entries([("hello", BsonValue::Int32(1))]).unwrap())
            .unwrap(),
    );
    let legacy = Frame {
        request_id: id,
        response_to: 0,
        opcode: Opcode::Query,
        payload: legacy.freeze(),
    };
    assert!(decode_request(legacy.clone()).unwrap().legacy_handshake);
    // Coalesced modern + legacy messages followed by every selected partial
    // successor: fragmentation cannot alter preceding frames or EOF outcome.
    let encoded = encode(message.clone());
    let mut stream = encoded.clone();
    stream.extend_from_slice(&encode(legacy.clone()));
    let prefix = usize::from(u16::from_le_bytes([byte(7), byte(8)])) % encoded.len();
    stream.extend_from_slice(&encoded[..prefix]);
    let expected = (
        vec![message.clone(), legacy],
        (prefix > 0).then_some(io::ErrorKind::UnexpectedEof),
    );
    assert_eq!(
        feed(&stream, stream.len(), MAX_BOOTSTRAP_MESSAGE_BYTES),
        expected
    );
    assert_eq!(feed(&stream, width, MAX_BOOTSTRAP_MESSAGE_BYTES), expected);
    let mut destination = BytesMut::from(&b"sentinel"[..]);
    assert!(
        FrameCodec::with_max_message_bytes(encoded.len() - 1)
            .unwrap()
            .encode(message, &mut destination)
            .is_err()
    );
    assert_eq!(
        destination.as_ref(),
        b"sentinel",
        "rejected encode partially wrote output"
    );
}
