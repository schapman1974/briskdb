#![cfg(feature = "mongo")]

use std::{
    io::{Read, Write},
    time::Duration,
};

use briskdb::{
    BriskDb, DocumentSupport, EngineState,
    document::{BsonDocument, BsonValue, decode_document, encode_document},
    protocol::mongo::{Frame, FrameCodec, MAX_BOOTSTRAP_MESSAGE_BYTES, MongoServer, Opcode},
};
use bytes::{BufMut, BytesMut};
use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use tokio_util::codec::Encoder;

fn document(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(entries).unwrap()
}

fn ping() -> BsonDocument {
    document([
        ("ping", BsonValue::Int32(1)),
        ("$db", BsonValue::from("admin")),
    ])
}

fn packet(body: &BsonDocument, id: i32, flags: u32) -> BytesMut {
    let mut payload = BytesMut::new();
    payload.put_u32_le(flags);
    payload.put_u8(0);
    payload.extend_from_slice(&encode_document(body).unwrap());
    let mut output = BytesMut::new();
    FrameCodec::default()
        .encode(
            Frame {
                request_id: id,
                response_to: 0,
                opcode: Opcode::Message,
                payload: payload.freeze(),
            },
            &mut output,
        )
        .unwrap();
    output
}

fn compress(packet: &[u8]) -> BytesMut {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&packet[16..]).unwrap();
    let payload = encoder.finish().unwrap();
    let mut output = BytesMut::new();
    output.put_i32_le((25 + payload.len()) as i32);
    output.extend_from_slice(&packet[4..12]);
    output.put_i32_le(2012);
    output.extend_from_slice(&packet[12..16]);
    output.put_i32_le((packet.len() - 16) as i32);
    output.put_u8(2);
    output.extend_from_slice(&payload);
    output
}

async fn reply(stream: &mut TcpStream, request: i32) -> (bool, BsonDocument) {
    timeout(Duration::from_secs(5), async {
        let mut header = [0; 16];
        stream.read_exact(&mut header).await.unwrap();
        let size = i32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        assert!((21..=MAX_BOOTSTRAP_MESSAGE_BYTES).contains(&size));
        assert_eq!(
            i32::from_le_bytes(header[8..12].try_into().unwrap()),
            request
        );
        let mut body = vec![0; size - 16];
        stream.read_exact(&mut body).await.unwrap();
        let compressed = i32::from_le_bytes(header[12..16].try_into().unwrap()) == 2012;
        if compressed {
            assert_eq!(i32::from_le_bytes(body[..4].try_into().unwrap()), 2013);
            assert_eq!(body[8], 2);
            let expanded = i32::from_le_bytes(body[4..8].try_into().unwrap()) as usize;
            assert!(expanded <= MAX_BOOTSTRAP_MESSAGE_BYTES - 16);
            let mut decoded = Vec::new();
            ZlibDecoder::new(&body[9..])
                .take(expanded as u64 + 1)
                .read_to_end(&mut decoded)
                .unwrap();
            assert_eq!(decoded.len(), expanded);
            body = decoded;
        } else {
            assert_eq!(i32::from_le_bytes(header[12..16].try_into().unwrap()), 2013);
        }
        (compressed, decode_document(&body[5..]).unwrap())
    })
    .await
    .unwrap()
}

async fn negotiate(stream: &mut TcpStream, offers: Vec<BsonValue>, invalid: bool) -> BsonDocument {
    let mut body = document([
        ("hello", BsonValue::Int32(1)),
        ("compression", BsonValue::Array(offers)),
        ("$db", BsonValue::from("admin")),
    ]);
    if invalid {
        body.push("unknownOption", BsonValue::Boolean(true))
            .unwrap();
    }
    stream.write_all(&packet(&body, 1, 0)).await.unwrap();
    let (compressed, body) = reply(stream, 1).await;
    assert!(
        !compressed,
        "handshake is deliberately returned uncompressed"
    );
    body
}

async fn disconnected(stream: &mut TcpStream) {
    match timeout(Duration::from_secs(3), stream.read(&mut [0]))
        .await
        .unwrap()
    {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            ) => {}
        result => panic!("connection not closed: {result:?}"),
    }
}

async fn setup() -> (tempfile::TempDir, BriskDb, MongoServer) {
    let root = tempfile::tempdir().unwrap();
    let db = BriskDb::builder(root.path())
        .with_shard_count(2)
        .with_document_support(DocumentSupport::Enabled)
        .open()
        .await
        .unwrap();
    let server = MongoServer::start(&db, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    (root, db, server)
}

#[tokio::test]
async fn negotiated_compressed_sequences_reads_one_way_and_mixed_frames() {
    let (_root, db, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let hello = negotiate(
        &mut stream,
        vec![BsonValue::from("zstd"), BsonValue::from("zlib")],
        false,
    )
    .await;
    assert_eq!(
        hello.get_first("compression"),
        Some(&BsonValue::Array(vec![BsonValue::from("zlib")]))
    );
    assert_eq!(
        hello.get_first("maxMessageSizeBytes"),
        Some(&BsonValue::Int32(
            (MAX_BOOTSTRAP_MESSAGE_BYTES - 1024) as i32
        ))
    );
    let row = document([
        ("_id", BsonValue::Int32(123)),
        ("payload", BsonValue::from("repeated".repeat(4096))),
    ]);
    let insert = document([
        ("insert", BsonValue::from("items")),
        ("$db", BsonValue::from("compressed")),
    ]);
    let mut sequence = BytesMut::new();
    sequence.put_i32_le(0);
    sequence.extend_from_slice(b"documents\0");
    sequence.extend_from_slice(&encode_document(&row).unwrap());
    let size = sequence.len() as i32;
    sequence[..4].copy_from_slice(&size.to_le_bytes());
    let mut insert = packet(&insert, 2, 2); // OP_MSG moreToCome: no response.
    insert.put_u8(1);
    insert.extend_from_slice(&sequence);
    let size = insert.len() as i32;
    insert[..4].copy_from_slice(&size.to_le_bytes());
    let insert = compress(&insert);
    // Fragment the compressed header, then coalesce the tail with a plain ping.
    for byte in &insert[..25] {
        stream.write_all(&[*byte]).await.unwrap();
    }
    let mut tail = BytesMut::from(&insert[25..]);
    tail.extend_from_slice(&packet(&ping(), 3, 0));
    stream.write_all(&tail).await.unwrap();
    assert_eq!(
        reply(&mut stream, 3).await.1.get_first("ok"),
        Some(&BsonValue::Double(1.0))
    );

    let find = document([
        ("find", BsonValue::from("items")),
        ("$db", BsonValue::from("compressed")),
    ]);
    stream
        .write_all(&compress(&packet(&find, 4, 0)))
        .await
        .unwrap();
    let (compressed, found) = reply(&mut stream, 4).await;
    assert!(
        compressed,
        "large compressible reply must use OP_COMPRESSED"
    );
    let Some(BsonValue::Document(cursor)) = found.get_first("cursor") else {
        panic!("{found:?}")
    };
    assert_eq!(
        cursor.get_first("firstBatch"),
        Some(&BsonValue::Array(vec![BsonValue::Document(row)]))
    );

    // A later ordinary hello must not reset an established compression agreement.
    negotiate(&mut stream, Vec::new(), false).await;
    stream
        .write_all(&compress(&packet(&ping(), 5, 0)))
        .await
        .unwrap();
    assert_eq!(
        reply(&mut stream, 5).await.1.get_first("ok"),
        Some(&BsonValue::Double(1.0))
    );
    server.close().await.unwrap();
    disconnected(&mut stream).await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn missing_unsupported_invalid_or_other_connection_handshakes_do_not_enable_zlib() {
    let (_root, db, mut server) = setup().await;
    let mut established = TcpStream::connect(server.address()).await.unwrap();
    negotiate(&mut established, vec![BsonValue::from("zlib")], false).await;
    for mode in 0..5 {
        let mut stream = TcpStream::connect(server.address()).await.unwrap();
        if mode != 0 {
            let offers = match mode {
                1 => vec![],
                2 => vec![BsonValue::from("snappy")],
                3 => vec![BsonValue::from("zlib")],
                _ => vec![BsonValue::Int32(2)],
            };
            let hello = negotiate(&mut stream, offers, mode == 3).await;
            assert_ne!(
                hello.get_first("compression"),
                Some(&BsonValue::Array(vec![BsonValue::from("zlib")]))
            );
        }
        stream
            .write_all(&compress(&packet(&ping(), 2, 0)))
            .await
            .unwrap();
        disconnected(&mut stream).await;
    }
    established
        .write_all(&compress(&packet(&ping(), 2, 0)))
        .await
        .unwrap();
    assert_eq!(
        reply(&mut established, 2).await.1.get_first("ok"),
        Some(&BsonValue::Double(1.0))
    );
    assert_eq!(db.state(), EngineState::Running);
    server.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn malformed_compressed_envelopes_and_sensitive_commands_close_only_their_connection() {
    let (_root, db, mut server) = setup().await;
    let good = compress(&packet(&ping(), 2, 0));
    let mut packets = Vec::new();
    for (offset, bytes) in [
        (16, 2012i32.to_le_bytes()),
        (20, i32::MAX.to_le_bytes()),
        (20, 5i32.to_le_bytes()),
    ] {
        let mut bad = good.clone();
        bad[offset..offset + 4].copy_from_slice(&bytes);
        packets.push(bad);
    }
    let mut unknown = good.clone();
    unknown[24] = 3;
    packets.push(unknown);
    let mut corrupt = good.clone();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 1;
    packets.push(corrupt);
    let mut trailing = good.clone();
    trailing.extend_from_slice(b"junk");
    let size = trailing.len() as i32;
    trailing[..4].copy_from_slice(&size.to_le_bytes());
    packets.push(trailing);
    for command in [
        "hello",
        "isMaster",
        "saslStart",
        "authenticate",
        "createUser",
    ] {
        packets.push(compress(&packet(
            &document([
                (command, BsonValue::Int32(1)),
                ("$db", BsonValue::from("admin")),
            ]),
            2,
            0,
        )));
    }
    for bad in packets {
        let mut stream = TcpStream::connect(server.address()).await.unwrap();
        negotiate(&mut stream, vec![BsonValue::from("zlib")], false).await;
        stream.write_all(&bad).await.unwrap();
        disconnected(&mut stream).await;
    }
    let mut healthy = TcpStream::connect(server.address()).await.unwrap();
    healthy.write_all(&packet(&ping(), 2, 0)).await.unwrap();
    assert_eq!(
        reply(&mut healthy, 2).await.1.get_first("ok"),
        Some(&BsonValue::Double(1.0))
    );
    assert_eq!(db.state(), EngineState::Running);
    server.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn shutdown_joins_a_partial_compressed_frame_without_closing_the_borrowed_engine() {
    let (_root, db, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    negotiate(&mut stream, vec![BsonValue::from("zlib")], false).await;
    let mut header = compress(&packet(&ping(), 2, 0));
    header[..4].copy_from_slice(&(MAX_BOOTSTRAP_MESSAGE_BYTES as i32).to_le_bytes());
    stream.write_all(&header[..25]).await.unwrap();
    timeout(Duration::from_secs(3), server.close())
        .await
        .unwrap()
        .unwrap();
    disconnected(&mut stream).await;
    assert_eq!(db.state(), EngineState::Running);
    db.close().await.unwrap();
}
