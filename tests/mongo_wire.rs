#![cfg(feature = "mongo")]

use std::{io, process::Command, time::Duration};

use briskdb::{
    BriskDb, DocumentSupport, EngineState,
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
        .with_document_support(DocumentSupport::Enabled)
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

fn insert_sequence(collection: &str, documents: &[BsonDocument]) -> BytesMut {
    let body = BsonDocument::from_entries([
        ("insert", BsonValue::from(collection)),
        ("$db", BsonValue::from("wire")),
    ])
    .unwrap();
    let mut bytes = packet(&body, 77, 0);
    let mut sequence = BytesMut::new();
    sequence.put_i32_le(0);
    sequence.extend_from_slice(b"documents\0");
    for document in documents {
        sequence.extend_from_slice(&encode_document(document).unwrap());
    }
    let length = sequence.len() as i32;
    sequence[..4].copy_from_slice(&length.to_le_bytes());
    bytes.put_u8(1);
    bytes.extend_from_slice(&sequence);
    let length = bytes.len() as i32;
    bytes[..4].copy_from_slice(&length.to_le_bytes());
    bytes
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
    coalesced.extend_from_slice(&packet(&command("aggregate"), 44, 0));
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
    let (root, database, mut server) = setup().await;
    let output = run_driver(server.address(), "initial").await;
    server.close().await.unwrap();
    database.close().await.unwrap();
    assert_driver(output);
    let database = BriskDb::builder(root.path())
        .with_shard_count(2)
        .with_document_support(DocumentSupport::Enabled)
        .open()
        .await
        .unwrap();
    let mut server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let output = run_driver(server.address(), "reopened").await;
    server.close().await.unwrap();
    database.close().await.unwrap();
    assert_driver(output);
}

async fn run_driver(address: std::net::SocketAddr, phase: &'static str) -> std::process::Output {
    let python = std::env::var("BRISKDB_MONGO_WIRE_PYTHON").unwrap_or_else(|_| "python3".into());
    let uri = format!("mongodb://{address}/");
    tokio::task::spawn_blocking(move || {
        Command::new(python)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/mongo_wire_client.py"
            ))
            .arg(uri)
            .arg(phase)
            .output()
            .unwrap()
    })
    .await
    .unwrap()
}

fn assert_driver(output: std::process::Output) {
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn insert_command(collection: &str, document: BsonDocument) -> BsonDocument {
    BsonDocument::from_entries([
        ("insert", BsonValue::from(collection)),
        (
            "documents",
            BsonValue::Array(vec![BsonValue::Document(document)]),
        ),
        ("$db", BsonValue::from("wire")),
    ])
    .unwrap()
}

fn find_command(collection: &str, id: BsonValue) -> BsonDocument {
    BsonDocument::from_entries([
        ("find", BsonValue::from(collection)),
        (
            "filter",
            BsonValue::Document(BsonDocument::from_entries([("_id", id)]).unwrap()),
        ),
        ("$db", BsonValue::from("wire")),
    ])
    .unwrap()
}

async fn send_command(stream: &mut TcpStream, body: &BsonDocument) -> BsonDocument {
    stream.write_all(&packet(body, 77, 0)).await.unwrap();
    let (frame, response) = response(stream).await;
    assert_eq!(frame.response_to, 77);
    response
}

fn first_batch(body: &BsonDocument) -> &[BsonValue] {
    let Some(BsonValue::Document(cursor)) = body.get_first("cursor") else {
        panic!("expected cursor: {body:?}");
    };
    assert!(matches!(cursor.get_first("id"), Some(BsonValue::Int64(0))));
    let Some(BsonValue::Array(documents)) = cursor.get_first("firstBatch") else {
        panic!("expected firstBatch");
    };
    documents
}

fn engine_request(
    command: briskdb::document::DocumentCommand,
) -> briskdb::document::DocumentRequest {
    briskdb::document::DocumentRequest::new(
        briskdb::document::DocumentRequestId::new([1; 16]).unwrap(),
        briskdb::RequestContext::new(),
        command,
    )
}

#[tokio::test]
async fn wire_and_embedded_documents_share_the_engine_and_survive_reopen() {
    use briskdb::document::{
        DocumentCommand, DocumentFilter, DocumentFindRequest, DocumentNamespace, DocumentPlan,
        DocumentReadOptions, DocumentResult,
    };
    let (root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let document = BsonDocument::from_entries([
        ("_id", BsonValue::Int64(42)),
        ("value", BsonValue::from("shared")),
    ])
    .unwrap();
    let inserted = send_command(&mut stream, &insert_command("items", document.clone())).await;
    assert!(matches!(inserted.get_first("n"), Some(BsonValue::Int32(1))));
    let execution = database
        .execute_document(
            &database.session(),
            engine_request(DocumentCommand::Find(DocumentFindRequest::new(
                DocumentNamespace::new("wire", "items").unwrap(),
                DocumentFilter::new(
                    BsonDocument::from_entries([("_id", BsonValue::Double(42.0))]).unwrap(),
                )
                .unwrap(),
                DocumentReadOptions::new(),
            ))),
        )
        .await
        .unwrap();
    assert!(matches!(execution.plan(), Some(DocumentPlan::Point(_))));
    let DocumentResult::Cursor(batch) = execution.result() else {
        panic!("expected engine cursor");
    };
    assert!(batch.documents()[0].representation_eq(&document));
    server.close().await.unwrap();
    database.close().await.unwrap();
    let database = BriskDb::builder(root.path())
        .with_shard_count(2)
        .with_document_support(DocumentSupport::Enabled)
        .open()
        .await
        .unwrap();
    let mut server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let found = send_command(&mut stream, &find_command("items", BsonValue::Int32(42))).await;
    assert!(
        matches!(first_batch(&found), [BsonValue::Document(actual)] if actual.representation_eq(&document))
    );
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn rejected_data_commands_and_missing_reads_do_not_create_collections() {
    use briskdb::document::{
        DocumentCommand, DocumentListCollectionsRequest, DocumentReadOptions, DocumentResult,
    };
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let document = BsonDocument::from_entries([("_id", BsonValue::Int32(1))]).unwrap();
    for (field, value) in [
        ("ordered", BsonValue::Int32(0)),
        ("bypassDocumentValidation", BsonValue::Boolean(true)),
        ("txnNumber", BsonValue::Int64(1)),
        (
            "writeConcern",
            BsonValue::Document(
                BsonDocument::from_entries([("w", BsonValue::from("majority"))]).unwrap(),
            ),
        ),
    ] {
        let mut command = insert_command("rejected", document.clone());
        command.push(field, value).unwrap();
        let reply = send_command(&mut stream, &command).await;
        assert!(
            matches!(reply.get_first("code"), Some(BsonValue::Int32(72))),
            "{reply:?}"
        );
    }
    let missing = send_command(&mut stream, &find_command("missing", BsonValue::Int32(1))).await;
    assert!(first_batch(&missing).is_empty());
    let metadata = database
        .execute_document(
            &database.session(),
            engine_request(DocumentCommand::ListCollections(
                DocumentListCollectionsRequest::new("wire", DocumentReadOptions::new()).unwrap(),
            )),
        )
        .await
        .unwrap();
    assert!(matches!(metadata.result(), DocumentResult::Collections(items) if items.is_empty()));
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn document_commands_respect_disabled_host_support() {
    let root = tempfile::tempdir().unwrap();
    let database = BriskDb::builder(root.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let mut server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let reply = send_command(
        &mut stream,
        &insert_command(
            "disabled",
            BsonDocument::from_entries([("_id", BsonValue::Int32(1))]).unwrap(),
        ),
    )
    .await;
    assert!(matches!(
        reply.get_first("code"),
        Some(BsonValue::Int32(20))
    ));
    let reply = send_command(&mut stream, &command("ping")).await;
    assert!(matches!(
        reply.get_first("ok"),
        Some(BsonValue::Double(1.0))
    ));
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn unacknowledged_insert_executes_without_emitting_a_reply() {
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let document = BsonDocument::from_entries([("_id", BsonValue::from("one-way"))]).unwrap();
    let mut insert = insert_command("items", document.clone());
    insert
        .push(
            "writeConcern",
            BsonValue::Document(BsonDocument::from_entries([("w", BsonValue::Int32(0))]).unwrap()),
        )
        .unwrap();
    let mut bytes = packet(&insert, 1, 2);
    bytes.extend_from_slice(&packet(
        &find_command("items", BsonValue::from("one-way")),
        2,
        0,
    ));
    stream.write_all(&bytes).await.unwrap();
    let (frame, body) = response(&mut stream).await;
    assert_eq!(frame.response_to, 2);
    assert!(
        matches!(first_batch(&body), [BsonValue::Document(actual)] if actual.representation_eq(&document))
    );
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn embedded_oversized_document_returns_a_bounded_error_and_keeps_socket_usable() {
    use briskdb::document::{
        DocumentCommand, DocumentInsertRequest, DocumentNamespace, DocumentWriteOptions,
    };
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let seed = BsonDocument::from_entries([("_id", BsonValue::from("seed"))]).unwrap();
    assert!(matches!(
        send_command(&mut stream, &insert_command("items", seed))
            .await
            .get_first("n"),
        Some(BsonValue::Int32(1))
    ));
    let large = BsonDocument::from_entries([
        ("_id", BsonValue::from("large")),
        ("value", BsonValue::String("x".repeat(600 * 1024))),
    ])
    .unwrap();
    database
        .execute_document(
            &database.session(),
            engine_request(DocumentCommand::Insert(
                DocumentInsertRequest::new(
                    DocumentNamespace::new("wire", "items").unwrap(),
                    vec![large],
                    DocumentWriteOptions::new(),
                )
                .unwrap(),
            )),
        )
        .await
        .unwrap();
    let reply = send_command(
        &mut stream,
        &find_command("items", BsonValue::from("large")),
    )
    .await;
    assert!(
        matches!(reply.get_first("code"), Some(BsonValue::Int32(10334))),
        "{reply:?}"
    );
    assert!(matches!(
        send_command(&mut stream, &command("ping"))
            .await
            .get_first("ok"),
        Some(BsonValue::Double(1.0))
    ));
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn raw_insert_generates_unique_ids_preserves_null_and_survives_reopen() {
    use briskdb::document::{
        DocumentCommand, DocumentFilter, DocumentFindRequest, DocumentNamespace,
        DocumentReadOptions, DocumentResult,
    };
    let (root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let documents = [
        BsonDocument::from_entries([("value", BsonValue::Int32(1))]).unwrap(),
        BsonDocument::from_entries([("_id", BsonValue::Null)]).unwrap(),
        BsonDocument::from_entries([("value", BsonValue::Int32(2))]).unwrap(),
    ];
    stream
        .write_all(&insert_sequence("generated", &documents))
        .await
        .unwrap();
    let (_, inserted) = response(&mut stream).await;
    assert!(matches!(inserted.get_first("n"), Some(BsonValue::Int32(3))));
    let execution = database
        .execute_document(
            &database.session(),
            engine_request(DocumentCommand::Find(DocumentFindRequest::new(
                DocumentNamespace::new("wire", "generated").unwrap(),
                DocumentFilter::empty(),
                DocumentReadOptions::new(),
            ))),
        )
        .await
        .unwrap();
    let DocumentResult::Cursor(batch) = execution.into_parts().2 else {
        panic!("expected documents");
    };
    let stored = batch.into_parts().2;
    assert_eq!(stored.len(), 3);
    let ids: Vec<_> = stored
        .iter()
        .map(|doc| doc.get_first("_id").unwrap().clone())
        .collect();
    assert!(matches!(ids[0], BsonValue::ObjectId(_)));
    assert!(matches!(ids[1], BsonValue::Null));
    assert!(matches!(ids[2], BsonValue::ObjectId(_)));
    assert_ne!(ids[0], ids[2]);
    assert_eq!(stored[0].iter().next().unwrap().0, "_id");
    assert!(documents[0].get_first("_id").is_none());
    server.close().await.unwrap();
    database.close().await.unwrap();
    let database = BriskDb::builder(root.path())
        .with_shard_count(2)
        .with_document_support(DocumentSupport::Enabled)
        .open()
        .await
        .unwrap();
    let mut server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    for (id, expected) in ids.into_iter().zip(stored) {
        let reply = send_command(&mut stream, &find_command("generated", id)).await;
        assert!(
            matches!(first_batch(&reply), [BsonValue::Document(actual)] if actual.representation_eq(&expected))
        );
    }
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn batch_memory_and_generated_id_size_limits_fail_before_catalog_creation() {
    use briskdb::document::{
        BsonCodecOptions, DocumentCommand, DocumentListCollectionsRequest, DocumentReadOptions,
        DocumentResult, decode_document_with_options,
    };
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    // Each document fits its own budget; the sequence as a whole must not get
    // a fresh four-MiB allocation budget for every document.
    let amplified =
        BsonDocument::from_entries((0..14000).map(|index| (format!("k{index}"), BsonValue::Null)))
            .unwrap();
    let options = BsonCodecOptions::new().with_max_decoded_bytes(4 * 1024 * 1024);
    decode_document_with_options(&encode_document(&amplified).unwrap(), &options).unwrap();
    stream
        .write_all(&insert_sequence(
            "amplified",
            &[amplified.clone(), amplified],
        ))
        .await
        .unwrap();
    let (_, reply) = response(&mut stream).await;
    assert!(
        matches!(reply.get_first("code"), Some(BsonValue::Int32(10334))),
        "{reply:?}"
    );
    // Exactly one byte too large after adding the generated ObjectId element.
    let oversize =
        BsonDocument::from_entries([("value", BsonValue::String("x".repeat(512 * 1024 - 33)))])
            .unwrap();
    assert_eq!(
        encode_document(&oversize).unwrap().len() + 17,
        512 * 1024 + 1
    );
    stream
        .write_all(&insert_sequence(
            "oversize",
            &[BsonDocument::new(), oversize],
        ))
        .await
        .unwrap();
    let (_, reply) = response(&mut stream).await;
    assert!(matches!(
        reply.get_first("code"),
        Some(BsonValue::Int32(10334))
    ));
    let execution = database
        .execute_document(
            &database.session(),
            engine_request(DocumentCommand::ListCollections(
                DocumentListCollectionsRequest::new("wire", DocumentReadOptions::new()).unwrap(),
            )),
        )
        .await
        .unwrap();
    assert!(matches!(execution.result(), DocumentResult::Collections(items) if items.is_empty()));
    assert!(matches!(
        send_command(&mut stream, &command("ping"))
            .await
            .get_first("ok"),
        Some(BsonValue::Double(1.0))
    ));
    server.close().await.unwrap();
    database.close().await.unwrap();
}
