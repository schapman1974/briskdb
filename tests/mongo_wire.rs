#![cfg(feature = "mongo")]

use std::{io, process::Command, time::Duration};

use briskdb::{
    BriskDb, EngineState,
    document::{BsonDocument, BsonValue, decode_document, encode_document},
    protocol::mongo::{Frame, FrameCodec, MAX_BOOTSTRAP_MESSAGE_BYTES, MongoServer, Opcode},
};
use bytes::{BufMut, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use tokio_util::codec::{Decoder, Encoder};

async fn setup() -> (tempfile::TempDir, BriskDb, MongoServer) {
    let root = tempfile::tempdir().unwrap();
    let database = BriskDb::builder(root.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    (root, database, server)
}

fn command(name: &str) -> BsonDocument {
    BsonDocument::from_entries([
        (name, BsonValue::Int32(1)),
        ("$db", BsonValue::String("admin".into())),
    ])
    .unwrap()
}

fn packet(body: &BsonDocument, request_id: i32, flags: u32) -> BytesMut {
    let mut payload = BytesMut::new();
    payload.put_u32_le(flags);
    payload.put_u8(0);
    payload.extend_from_slice(&encode_document(body).unwrap());
    let mut encoded = BytesMut::new();
    FrameCodec::default()
        .encode(
            Frame {
                request_id,
                response_to: 0,
                opcode: Opcode::Message,
                payload: payload.freeze(),
            },
            &mut encoded,
        )
        .unwrap();
    encoded
}

async fn response(stream: &mut TcpStream) -> (Frame, BsonDocument) {
    timeout(Duration::from_secs(5), async {
        let mut length = [0; 4];
        stream.read_exact(&mut length).await.unwrap();
        let length = i32::from_le_bytes(length) as usize;
        assert!((16..=MAX_BOOTSTRAP_MESSAGE_BYTES).contains(&length));
        let mut bytes = BytesMut::zeroed(length);
        bytes[..4].copy_from_slice(&(length as i32).to_le_bytes());
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        let frame = FrameCodec::default().decode(&mut bytes).unwrap().unwrap();
        let offset = if frame.opcode == Opcode::Reply { 20 } else { 5 };
        let body = decode_document(&frame.payload[offset..]).unwrap();
        (frame, body)
    })
    .await
    .unwrap()
}

async fn disconnected(stream: &mut TcpStream) {
    let mut byte = [0];
    match timeout(Duration::from_secs(3), stream.read(&mut byte))
        .await
        .unwrap()
    {
        Ok(length) => assert_eq!(length, 0),
        Err(error) => assert!(matches!(
            error.kind(),
            io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
        )),
    }
}

#[tokio::test]
async fn modern_discovery_ping_and_unsupported_commands() {
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let hello = BsonDocument::from_entries([
        ("hello", BsonValue::Int32(1)),
        ("backpressure", BsonValue::Boolean(true)),
        ("$db", BsonValue::String("admin".into())),
    ])
    .unwrap();
    let bytes = packet(&hello, 42, 0);
    // Exercise fragmented delivery through the actual socket, not just the codec.
    for chunk in bytes.chunks(3) {
        stream.write_all(chunk).await.unwrap();
    }
    let (frame, body) = response(&mut stream).await;
    assert_eq!(frame.response_to, 42);
    assert_eq!(frame.opcode, Opcode::Message);
    assert!(matches!(
        body.get_first("isWritablePrimary"),
        Some(BsonValue::Boolean(true))
    ));
    for absent in [
        "logicalSessionTimeoutMinutes",
        "setName",
        "topologyVersion",
        "serviceId",
    ] {
        assert!(body.get_first(absent).is_none());
    }
    assert!(
        matches!(body.get_first("compression"), Some(BsonValue::Array(items)) if items.is_empty())
    );
    let mut coalesced = packet(&command("ping"), 43, 0);
    coalesced.extend_from_slice(&packet(&command("find"), 44, 0));
    stream.write_all(&coalesced).await.unwrap();
    assert_eq!(response(&mut stream).await.0.response_to, 43);
    let (frame, body) = response(&mut stream).await;
    assert_eq!(frame.response_to, 44);
    assert!(matches!(body.get_first("code"), Some(BsonValue::Int32(59))));
    let mut invalid = command("ping");
    invalid
        .push("lsid", BsonValue::Document(BsonDocument::new()))
        .unwrap();
    stream.write_all(&packet(&invalid, 45, 0)).await.unwrap();
    assert!(matches!(
        response(&mut stream).await.1.get_first("code"),
        Some(BsonValue::Int32(72))
    ));
    server.close().await.unwrap();
    assert_eq!(database.engine().state(), EngineState::Running);
    database.close().await.unwrap();
}

#[tokio::test]
async fn legacy_handshake_uses_correlated_op_reply() {
    let (_root, database, mut server) = setup().await;
    let body = BsonDocument::from_entries([
        ("ismaster", BsonValue::Int32(1)),
        ("helloOk", BsonValue::Boolean(true)),
    ])
    .unwrap();
    let mut payload = BytesMut::new();
    payload.put_i32_le(0);
    payload.extend_from_slice(b"admin.$cmd\0");
    payload.put_i32_le(0);
    payload.put_i32_le(-1);
    payload.extend_from_slice(&encode_document(&body).unwrap());
    let mut bytes = BytesMut::new();
    FrameCodec::default()
        .encode(
            Frame {
                request_id: -123,
                response_to: 0,
                opcode: Opcode::Query,
                payload: payload.freeze(),
            },
            &mut bytes,
        )
        .unwrap();
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
    let (frame, body) = response(&mut stream).await;
    assert_eq!(frame.opcode, Opcode::Reply);
    assert_eq!(frame.response_to, -123);
    assert_eq!(
        &frame.payload[..20],
        &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0]
    );
    assert!(matches!(
        body.get_first("helloOk"),
        Some(BsonValue::Boolean(true))
    ));
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn one_way_requests_do_not_emit_replies() {
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let mut bytes = packet(&command("ping"), 1, 2);
    bytes.extend_from_slice(&packet(&command("ping"), 2, 0));
    stream.write_all(&bytes).await.unwrap();
    assert_eq!(response(&mut stream).await.0.response_to, 2);
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn bad_length_is_fatal_and_shutdown_closes_partial_connections() {
    let (_root, database, mut server) = setup().await;
    let mut malformed = TcpStream::connect(server.address()).await.unwrap();
    malformed
        .write_all(&((MAX_BOOTSTRAP_MESSAGE_BYTES + 1) as i32).to_le_bytes())
        .await
        .unwrap();
    disconnected(&mut malformed).await;
    let mut partial = TcpStream::connect(server.address()).await.unwrap();
    partial.write_all(&[16, 0]).await.unwrap();
    let mut idle = TcpStream::connect(server.address()).await.unwrap();
    // A completed ping is an admission barrier, rather than an arbitrary sleep.
    idle.write_all(&packet(&command("ping"), 1, 0))
        .await
        .unwrap();
    response(&mut idle).await;
    timeout(Duration::from_secs(3), server.close())
        .await
        .unwrap()
        .unwrap();
    disconnected(&mut partial).await;
    disconnected(&mut idle).await;
    server.close().await.unwrap();
    assert_eq!(database.engine().state(), EngineState::Running);
    database.close().await.unwrap();
}

#[tokio::test]
async fn remote_binds_are_refused_and_engine_close_stops_listener() {
    let (_root, database, mut server) = setup().await;
    assert!(
        MongoServer::start(&database, "0.0.0.0:0".parse().unwrap())
            .await
            .is_err()
    );
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    stream
        .write_all(&packet(&command("ping"), 1, 0))
        .await
        .unwrap();
    response(&mut stream).await;
    database.close().await.unwrap();
    disconnected(&mut stream).await;
    server.close().await.unwrap();
    assert!(
        MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn finite_connection_cap_rejects_overflow() {
    let (_root, database, mut server) = setup().await;
    let mut streams = Vec::new();
    for index in 0..8 {
        let mut stream = TcpStream::connect(server.address()).await.unwrap();
        stream
            .write_all(&packet(&command("ping"), index, 0))
            .await
            .unwrap();
        response(&mut stream).await;
        streams.push(stream);
    }
    let mut overflow = TcpStream::connect(server.address()).await.unwrap();
    disconnected(&mut overflow).await;
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires pinned PyMongo; CI runs this explicitly with BRISKDB_MONGO_WIRE_PYTHON"]
async fn real_pymongo_sync_async_discovery() {
    let (_root, database, mut server) = setup().await;
    let python = std::env::var("BRISKDB_MONGO_WIRE_PYTHON").unwrap_or_else(|_| "python3".into());
    let uri = format!("mongodb://{}/", server.address());
    let output = tokio::task::spawn_blocking(move || {
        Command::new(python)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/mongo_wire_client.py"
            ))
            .arg(uri)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    server.close().await.unwrap();
    database.close().await.unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
