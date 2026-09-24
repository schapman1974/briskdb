use std::io::Write;

use flate2::write::ZlibEncoder;
use proptest::prelude::*;

use super::*;

fn frame(payload: &[u8]) -> Frame {
    Frame {
        request_id: 77,
        response_to: 0,
        opcode: Opcode::Message,
        payload: Bytes::copy_from_slice(payload),
    }
}

fn wrapper(payload: &[u8], expanded: i32) -> BytesMut {
    let mut result = BytesMut::new();
    result.put_i32_le((WRAPPER_BYTES + payload.len()) as i32);
    result.put_i32_le(77);
    result.put_i32_le(0);
    result.put_i32_le(COMPRESSED_OPCODE);
    result.put_i32_le(Opcode::Message.number());
    result.put_i32_le(expanded);
    result.put_u8(ZLIB_ID);
    result.extend_from_slice(payload);
    result
}

fn compressed(payload: &[u8]) -> BytesMut {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(payload).unwrap();
    wrapper(&encoder.finish().unwrap(), payload.len() as i32)
}

fn codec() -> TransportCodec {
    let mut codec = TransportCodec::new().unwrap();
    codec.enable_zlib();
    codec
}

fn decode(mut packet: BytesMut) -> io::Result<Frame> {
    codec().decode(&mut packet)?.unwrap().into_frame()
}

#[test]
fn fragmented_coalesced_and_plain_packets_keep_exact_boundaries_and_ids() {
    let payload = vec![42; 8192];
    let packet = compressed(&payload);
    for split in 0..packet.len() {
        let mut decoder = codec();
        let mut source = BytesMut::from(&packet[..split]);
        let capacity = source.capacity();
        assert!(decoder.decode(&mut source).unwrap().is_none());
        assert_eq!(source.len(), split);
        assert_eq!(source.capacity(), capacity);
        source.extend_from_slice(&packet[split..]);
        let incoming = decoder.decode(&mut source).unwrap().unwrap();
        assert!(incoming.is_compressed());
        assert_eq!(incoming.into_frame().unwrap(), frame(&payload));
        assert!(source.is_empty());
    }
    let plain = encode_reply(frame(b"uncompressed"), false).unwrap();
    let mut source = plain.clone();
    source.extend_from_slice(&packet);
    source.extend_from_slice(&plain);
    let mut decoder = codec();
    assert_eq!(
        decoder
            .decode(&mut source)
            .unwrap()
            .unwrap()
            .into_frame()
            .unwrap(),
        frame(b"uncompressed")
    );
    assert_eq!(
        decoder
            .decode(&mut source)
            .unwrap()
            .unwrap()
            .into_frame()
            .unwrap(),
        frame(&payload)
    );
    assert_eq!(
        decoder
            .decode(&mut source)
            .unwrap()
            .unwrap()
            .into_frame()
            .unwrap(),
        frame(b"uncompressed")
    );
    assert!(source.is_empty());
}

#[test]
fn compression_requires_negotiation_and_keeps_public_codec_uncompressed() {
    let mut packet = compressed(&[0; 100]);
    packet.truncate(HEADER_BYTES);
    assert!(TransportCodec::new().unwrap().decode(&mut packet).is_err());
    assert!(FrameCodec::default().decode(&mut packet).is_err());
    assert_eq!(packet.len(), HEADER_BYTES);
}

#[test]
fn every_partial_prefix_fails_at_eof_without_reserving_advertised_size() {
    let packet = compressed(&[0; 4096]);
    assert!(codec().decode_eof(&mut BytesMut::new()).unwrap().is_none());
    for split in 1..packet.len() {
        let mut source = BytesMut::from(&packet[..split]);
        let capacity = source.capacity();
        assert_eq!(
            codec().decode_eof(&mut source).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert_eq!(source.capacity(), capacity);
    }
    let mut header = packet.clone();
    header[..4].copy_from_slice(&(MAX_BOOTSTRAP_MESSAGE_BYTES as i32).to_le_bytes());
    header.truncate(WRAPPER_BYTES);
    let capacity = header.capacity();
    assert!(codec().decode(&mut header).unwrap().is_none());
    assert_eq!(header.capacity(), capacity);
    assert_eq!(header.len(), WRAPPER_BYTES);
}

#[test]
fn invalid_metadata_is_rejected_before_reading_the_body() {
    let packet = compressed(&[0; 8192]);
    let mut cases = Vec::new();
    for size in [
        -1,
        0,
        4,
        (MAX_BOOTSTRAP_MESSAGE_BYTES - HEADER_BYTES + 1) as i32,
        i32::MAX,
    ] {
        let mut invalid = packet.clone();
        invalid[20..24].copy_from_slice(&size.to_le_bytes());
        cases.push(invalid);
    }
    for opcode in [0i32, 1, 2004, COMPRESSED_OPCODE, i32::MAX] {
        let mut invalid = packet.clone();
        invalid[16..20].copy_from_slice(&opcode.to_le_bytes());
        cases.push(invalid);
    }
    for compressor in [0, 1, 3, 255] {
        let mut invalid = packet.clone();
        invalid[24] = compressor;
        cases.push(invalid);
    }
    for size in [
        -1,
        0,
        15,
        16,
        24,
        MAX_BOOTSTRAP_MESSAGE_BYTES as i32 + 1,
        i32::MAX,
    ] {
        let mut invalid = packet.clone();
        invalid[..4].copy_from_slice(&size.to_le_bytes());
        cases.push(invalid);
    }
    let mut reply = packet.clone();
    reply[8..12].copy_from_slice(&1i32.to_le_bytes());
    cases.push(reply);
    for mut invalid in cases {
        invalid.truncate(WRAPPER_BYTES);
        let before = invalid.clone();
        let capacity = invalid.capacity();
        assert!(codec().decode(&mut invalid).is_err());
        assert_eq!(invalid, before);
        assert_eq!(invalid.capacity(), capacity);
    }
}

#[test]
fn zlib_lengths_checksum_truncation_and_trailing_members_are_exact() {
    let packet = compressed(&[42; 4096]);
    assert_eq!(decode(packet.clone()).unwrap(), frame(&[42; 4096]));
    for expanded in [5i32, 4095, 4097, 65536] {
        let mut wrong = packet.clone();
        wrong[20..24].copy_from_slice(&expanded.to_le_bytes());
        assert!(decode(wrong).is_err());
    }
    for size in 0..packet.len() - WRAPPER_BYTES {
        let truncated = wrapper(&packet[WRAPPER_BYTES..WRAPPER_BYTES + size], 4096);
        assert!(decode(truncated).is_err());
    }
    let mut corrupt = packet.clone();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 1;
    assert!(decode(corrupt).is_err());
    for suffix in [b"junk".as_slice(), &packet[WRAPPER_BYTES..]] {
        let mut appended = packet.clone();
        appended.extend_from_slice(suffix);
        let length = appended.len() as i32;
        appended[..4].copy_from_slice(&length.to_le_bytes());
        assert!(decode(appended).is_err());
    }
}

#[test]
fn valid_maximum_expansion_and_overstated_small_bomb_remain_bounded() {
    let payload = vec![42; MAX_BOOTSTRAP_MESSAGE_BYTES - HEADER_BYTES];
    assert_eq!(decode(compressed(&payload)).unwrap(), frame(&payload));
    let mut bomb = compressed(&vec![0; 8 * MAX_BOOTSTRAP_MESSAGE_BYTES]);
    bomb[20..24].copy_from_slice(&32i32.to_le_bytes());
    assert!(decode(bomb).is_err());
}

#[test]
fn preset_dictionary_gzip_and_raw_deflate_are_not_zlib_messages() {
    // Independent CPython zlib fixtures for b"dictionary data " * 8:
    // compressobj(zdict=b"dictionary data"), compress(wbits=31), compress(wbits=-15).
    // The Mongo zlib transport accepts no alternate wrapper or preset dictionary.
    let fixtures: &[&[u8]] = &[
        &[
            120, 187, 48, 111, 5, 241, 75, 65, 229, 42, 208, 155, 15, 0, 80, 180, 48, 129,
        ],
        &[
            31, 139, 8, 0, 0, 0, 0, 0, 0, 19, 75, 201, 76, 46, 201, 204, 207, 75, 44, 170, 84, 72,
            73, 44, 73, 84, 72, 161, 51, 31, 0, 243, 122, 30, 144, 128, 0, 0, 0,
        ],
        &[
            75, 201, 76, 46, 201, 204, 207, 75, 44, 170, 84, 72, 73, 44, 73, 84, 72, 161, 51, 31, 0,
        ],
    ];
    for fixture in fixtures {
        assert!(decode(wrapper(fixture, 128)).is_err());
    }
}

#[test]
fn replies_preserve_headers_and_fall_back_without_expanding_packets() {
    let payload = vec![42; 4096];
    let packet = encode_reply(frame(&payload), true).unwrap();
    assert_eq!(
        i32::from_le_bytes(packet[12..16].try_into().unwrap()),
        COMPRESSED_OPCODE
    );
    assert!(packet.len() < payload.len());
    assert_eq!(decode(packet.clone()).unwrap(), frame(&payload));
    let mut reply = frame(&payload);
    reply.request_id = -77;
    reply.response_to = 123;
    let reply = encode_reply(reply, true).unwrap();
    assert_eq!(&reply[4..8], &(-77i32).to_le_bytes());
    assert_eq!(&reply[8..12], &123i32.to_le_bytes());

    let mut seed = 23u32;
    let noise: Vec<_> = (0..65536)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed as u8
        })
        .collect();
    for bytes in [b"small".as_slice(), b"ten-bytes!".as_slice(), &noise] {
        let mut encoded = encode_reply(frame(bytes), true).unwrap();
        assert_eq!(encoded.len(), bytes.len() + HEADER_BYTES);
        assert_eq!(
            FrameCodec::default().decode(&mut encoded).unwrap().unwrap(),
            frame(bytes)
        );
    }
    assert!(encode_reply(frame(&vec![0; MAX_BOOTSTRAP_MESSAGE_BYTES]), true).is_err());
}

#[test]
fn sensitive_commands_and_unsuccessful_handshakes_cannot_enable_compression() {
    use crate::document::encode_document;
    for command in [
        "hello",
        "ismaster",
        "isMaster",
        "saslStart",
        "saslContinue",
        "getnonce",
        "authenticate",
        "createUser",
        "updateUser",
        "copydbSaslStart",
        "copydbgetnonce",
        "copydb",
        "ping",
    ] {
        let body = BsonDocument::from_entries([
            (command, BsonValue::Int32(1)),
            ("$db", BsonValue::from("admin")),
        ])
        .unwrap();
        let mut payload = BytesMut::new();
        payload.put_u32_le(0);
        payload.put_u8(0);
        payload.extend_from_slice(&encode_document(&body).unwrap());
        let mut request = super::super::decode_request(frame(&payload)).unwrap();
        assert_eq!(validate_command(&request).is_ok(), command == "ping");
        let supported = BsonDocument::from_entries([
            ("ok", BsonValue::Double(1.0)),
            (
                "compression",
                BsonValue::Array(vec![BsonValue::from("zlib")]),
            ),
        ])
        .unwrap();
        assert!(!negotiated_zlib(&request, &supported));
        request
            .body
            .push(
                "compression",
                BsonValue::Array(vec![BsonValue::from("zlib")]),
            )
            .unwrap();
        assert_eq!(
            negotiated_zlib(&request, &supported),
            matches!(command, "hello" | "ismaster" | "isMaster")
        );
        assert!(!negotiated_zlib(&request, &BsonDocument::new()));
        request.more_to_come = true;
        assert!(!negotiated_zlib(&request, &supported));
    }
}

proptest! {
    #[test]
    fn arbitrary_zlib_bodies_never_escape_declared_budgets(
        bytes in prop::collection::vec(any::<u8>(), 0..4096), expanded in 5i32..65536,
    ) {
        let result = decode(wrapper(&bytes, expanded));
        if let Ok(frame) = result {
            prop_assert_eq!(frame.payload.len(), expanded as usize);
        }
    }

    #[test]
    fn arbitrary_transport_prefixes_are_panic_free(bytes in prop::collection::vec(any::<u8>(), 0..4096)) {
        let mut source = BytesMut::from(bytes.as_slice());
        if let Ok(Some(incoming)) = codec().decode(&mut source) {
            let _ = incoming.into_frame();
        }
    }
}
