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

#[path = "mongo_wire/cancellation.rs"]
mod cancellation;
#[path = "mongo_wire/client_metadata.rs"]
mod client_metadata;
#[path = "mongo_wire/events.rs"]
mod events;
#[path = "mongo_wire/metrics.rs"]
mod metrics;
#[path = "mongo_wire/read_metrics.rs"]
mod read_metrics;
#[path = "mongo_wire/readiness.rs"]
mod readiness;

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
async fn count_and_namespace_checks_do_not_enumerate_the_catalog() {
    use briskdb::document::{
        DocumentCollectionExistsRequest, DocumentCollectionOptions, DocumentCommand,
        DocumentCreateCollectionRequest, DocumentNamespace, DocumentResult, DocumentWriteOptions,
    };
    let (_root, database, mut server) = setup().await;
    let session = database.session();
    let options = DocumentCollectionOptions::new(
        BsonDocument::from_entries([("opaque", BsonValue::from("x".repeat(32 * 1024)))]).unwrap(),
    )
    .unwrap();
    for index in 0..102 {
        let collection_options = match index {
            100 => DocumentCollectionOptions::new(
                BsonDocument::from_entries([(
                    "large_unrelated",
                    BsonValue::from("x".repeat(1_100_000)),
                )])
                .unwrap(),
            )
            .unwrap(),
            101 => options.clone(),
            _ => DocumentCollectionOptions::empty(),
        };
        database
            .execute_document(
                &session,
                engine_request(DocumentCommand::CreateCollection(
                    DocumentCreateCollectionRequest::new(
                        DocumentNamespace::new("wire", format!("collection_{index}")).unwrap(),
                        collection_options,
                        DocumentWriteOptions::new(),
                    ),
                )),
            )
            .await
            .unwrap();
    }
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let documents = (0..16)
        .map(|index| {
            BsonDocument::from_entries([
                ("_id", BsonValue::Int32(index)),
                ("group", BsonValue::Int32(index % 2)),
            ])
            .unwrap()
        })
        .collect::<Vec<_>>();
    stream
        .write_all(&insert_sequence("collection_101", &documents))
        .await
        .unwrap();
    assert_eq!(
        response(&mut stream).await.1.get_first("n"),
        Some(&BsonValue::Int32(16))
    );
    assert_eq!(
        first_batch(
            &send_command(
                &mut stream,
                &find_command("collection_101", BsonValue::Int32(7))
            )
            .await
        )
        .len(),
        1
    );
    // Creating another collection must also work once the catalog exceeds a page.
    assert_eq!(
        send_command(
            &mut stream,
            &insert_command("new_collection", documents[0].clone())
        )
        .await
        .get_first("n"),
        Some(&BsonValue::Int32(1))
    );
    for (collection, filter, skip, limit, expected) in [
        ("collection_101", BsonDocument::new(), 0, 0, 16),
        ("collection_101", BsonDocument::new(), 3, 5, 5),
        ("collection_101", BsonDocument::new(), 30, 5, 0),
        (
            "collection_101",
            BsonDocument::from_entries([("group", BsonValue::Int32(1))]).unwrap(),
            2,
            0,
            6,
        ),
        (
            "collection_101",
            BsonDocument::from_entries([("_id", BsonValue::Int32(7))]).unwrap(),
            0,
            0,
            1,
        ),
        (
            "collection_101",
            BsonDocument::from_entries([("_id", BsonValue::Int32(7))]).unwrap(),
            1,
            0,
            0,
        ),
        ("absent", BsonDocument::new(), 0, 0, 0),
    ] {
        let count = BsonDocument::from_entries([
            ("count", BsonValue::from(collection)),
            ("$db", BsonValue::from("wire")),
            ("query", BsonValue::Document(filter)),
            ("skip", BsonValue::Int64(skip)),
            ("limit", BsonValue::Int64(limit)),
            ("maxTimeMS", BsonValue::Int32(10_000)),
        ])
        .unwrap();
        let reply = send_command(&mut stream, &count).await;
        assert_eq!(
            reply.get_first("n"),
            Some(&BsonValue::Int64(expected)),
            "{reply:?}"
        );
    }
    // Missing reads must not create metadata, and probing/inserting an existing
    // namespace must not replace its collection options.
    let exists = database
        .execute_document(
            &session,
            engine_request(DocumentCommand::CollectionExists(
                DocumentCollectionExistsRequest::new(
                    DocumentNamespace::new("wire", "absent").unwrap(),
                ),
            )),
        )
        .await
        .unwrap();
    assert_eq!(exists.result(), &DocumentResult::CollectionExists(false));
    let same_options = database
        .execute_document(
            &session,
            engine_request(DocumentCommand::CreateCollection(
                DocumentCreateCollectionRequest::new(
                    DocumentNamespace::new("wire", "collection_101").unwrap(),
                    options.clone(),
                    DocumentWriteOptions::new(),
                ),
            )),
        )
        .await
        .unwrap();
    assert!(
        matches!(same_options.result(), DocumentResult::Collection(value) if value.options() == &options)
    );
    server.close().await.unwrap();
    database.close().await.unwrap();
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
    coalesced.extend_from_slice(&packet(&command("unsupportedCommand"), 44, 0));
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
    let mut body = BsonDocument::from_entries([
        ("ismaster", BsonValue::Int32(1)),
        ("helloOk", BsonValue::Boolean(true)),
    ])
    .unwrap();
    body.push(
        "client",
        client_metadata::hello("PyMongo|c", "4.17.0")
            .get_first("client")
            .unwrap()
            .clone(),
    )
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
    assert_eq!(server.client_metadata()[0].driver_version, Some([4, 17, 0]));
    server.close().await.unwrap();
    assert!(server.client_metadata().is_empty());
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
async fn validation_bypass_requires_booleans_and_does_not_enable_validators() {
    let doc = |entries| BsonDocument::from_entries(entries).unwrap();
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    for verb in ["insert", "update", "findAndModify"] {
        for invalid in [
            BsonValue::Null,
            BsonValue::Int32(1),
            BsonValue::from("true"),
            BsonValue::Array(vec![]),
        ] {
            let reply = send_command(
                &mut stream,
                &doc(vec![
                    (verb, BsonValue::from("absent")),
                    ("bypassDocumentValidation", invalid),
                    ("$db", BsonValue::from("wire")),
                ]),
            )
            .await;
            assert_eq!(reply.get_first("code"), Some(&BsonValue::Int32(72)));
        }
    }
    let reply = send_command(
        &mut stream,
        &doc(vec![
            ("create", BsonValue::from("absent")),
            (
                "validator",
                BsonValue::Document(doc(vec![("required", BsonValue::Boolean(true))])),
            ),
            ("$db", BsonValue::from("wire")),
        ]),
    )
    .await;
    assert_eq!(reply.get_first("code"), Some(&BsonValue::Int32(72)));
    let reply = send_command(
        &mut stream,
        &doc(vec![
            ("listCollections", BsonValue::Int32(1)),
            ("nameOnly", BsonValue::Boolean(true)),
            ("$db", BsonValue::from("wire")),
        ]),
    )
    .await;
    let Some(BsonValue::Document(cursor)) = reply.get_first("cursor") else {
        panic!("cursor")
    };
    assert_eq!(
        cursor.get_first("firstBatch"),
        Some(&BsonValue::Array(vec![]))
    );
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
    let metrics = server.metrics();
    assert_eq!(
        (
            metrics.accepted_connections,
            metrics.admitted_connections,
            metrics.rejected_connections,
            metrics.active_connections
        ),
        (9, 8, 1, 8)
    );
    assert_eq!(metrics.peak_connections, 8);
    server.close().await.unwrap();
    let metrics = server.metrics();
    assert_eq!(
        (metrics.active_connections, metrics.closed_connections),
        (0, 8)
    );
    database.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires pinned PyMongo; CI runs this explicitly with BRISKDB_MONGO_WIRE_PYTHON"]
async fn real_pymongo_sync_async_discovery() {
    assert_driver_restart("/tests/mongo_wire_client.py").await;
}

#[tokio::test]
#[ignore = "requires pinned stock PyMongo; CI runs this explicitly"]
async fn real_pymongo_async_cancellation() {
    cancellation::run().await;
}

#[tokio::test]
#[ignore = "focused local index-candidate checks; also included in the full real-driver gate"]
async fn real_pymongo_index_candidates() {
    assert_driver_restart("/tests/mongo_index_candidates_client.py").await;
}

#[tokio::test]
#[ignore = "focused local count checks; also included in the full real-driver gate"]
async fn real_pymongo_counts() {
    assert_driver_restart("/tests/mongo_count_client.py").await;
}

#[tokio::test]
#[ignore = "focused local read-option checks; also included in the full real-driver gate"]
async fn real_pymongo_read_options() {
    assert_driver_restart("/tests/mongo_read_options_client.py").await;
}

async fn assert_driver_restart(script: &'static str) {
    let (root, database, mut server) = setup().await;
    server.set_read_metrics_enabled(true);
    seed_index_metadata(&database).await;
    let output = client_metadata::observe_driver(&server, "initial", script).await;
    server.close().await.unwrap();
    assert!(server.client_metadata().is_empty());
    metrics::assert_driver_metrics_drained(&server.metrics());
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
    server.set_read_metrics_enabled(true);
    let output = client_metadata::observe_driver(&server, "reopened", script).await;
    server.close().await.unwrap();
    assert!(server.client_metadata().is_empty());
    metrics::assert_driver_metrics_drained(&server.metrics());
    database.close().await.unwrap();
    assert_driver(output);
}

async fn seed_index_metadata(database: &BriskDb) {
    use briskdb::document::{
        DocumentBuildIndexRequest, DocumentCollectionOptions, DocumentCommand,
        DocumentCreateCollectionRequest, DocumentCreateIndexRequest, DocumentFilter,
        DocumentIndexRequest, DocumentNamespace, DocumentWriteOptions,
    };
    let namespace = DocumentNamespace::new("wire_indexes", "items").unwrap();
    let session = database.session();
    database
        .execute_document(
            &session,
            engine_request(DocumentCommand::CreateCollection(
                DocumentCreateCollectionRequest::new(
                    namespace.clone(),
                    DocumentCollectionOptions::empty(),
                    DocumentWriteOptions::new(),
                ),
            )),
        )
        .await
        .unwrap();
    for (name, sparse, partial, unique, build) in [
        ("z", false, false, false, true),
        ("!before_id", true, false, false, true),
        ("partial", false, true, false, true),
        ("pending", false, false, false, false),
        ("pending_unique", false, false, true, false),
    ] {
        let mut definition = DocumentIndexRequest::new(
            BsonDocument::from_entries([
                ("value", BsonValue::Int32(1)),
                ("tail", BsonValue::Int32(-1)),
            ])
            .unwrap(),
        )
        .unwrap()
        .with_name(name)
        .unwrap()
        .with_sparse(sparse)
        .with_unique(unique);
        if partial {
            definition = definition.with_partial_filter(
                DocumentFilter::new(
                    BsonDocument::from_entries([("active", BsonValue::Boolean(true))]).unwrap(),
                )
                .unwrap(),
            );
        }
        database
            .execute_document(
                &session,
                engine_request(DocumentCommand::CreateIndex(
                    DocumentCreateIndexRequest::new(
                        namespace.clone(),
                        definition,
                        DocumentWriteOptions::new(),
                    ),
                )),
            )
            .await
            .unwrap();
        if build {
            database
                .execute_document(
                    &session,
                    engine_request(DocumentCommand::BuildIndex(
                        DocumentBuildIndexRequest::new(
                            namespace.clone(),
                            name,
                            DocumentWriteOptions::new(),
                        )
                        .unwrap(),
                    )),
                )
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn wire_index_metadata_lists_only_ready_definitions_and_validates_options() {
    let (_root, database, mut server) = setup().await;
    seed_index_metadata(&database).await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let listing = BsonDocument::from_entries([
        ("listIndexes", BsonValue::from("items")),
        ("$db", BsonValue::from("wire_indexes")),
    ])
    .unwrap();
    let reply = send_command(&mut stream, &listing).await;
    let names = first_batch(&reply)
        .iter()
        .map(|value| {
            let BsonValue::Document(row) = value else {
                panic!("metadata")
            };
            assert!(row.get_first("v").is_none() && row.get_first("unique").is_none());
            row.get_first("name").unwrap().clone()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        ["_id_", "!before_id", "partial", "z"].map(BsonValue::from)
    );
    for (field, value, code) in [
        ("cursor", BsonValue::Boolean(true), 14),
        (
            "cursor",
            BsonValue::Document(
                BsonDocument::from_entries([("batchSize", BsonValue::Int32(-1))]).unwrap(),
            ),
            2,
        ),
        (
            "cursor",
            BsonValue::Document(
                BsonDocument::from_entries([("batchSize", BsonValue::Int32(1001))]).unwrap(),
            ),
            115,
        ),
        (
            "cursor",
            BsonValue::Document(
                BsonDocument::from_entries([("unknown", BsonValue::Int32(1))]).unwrap(),
            ),
            72,
        ),
        ("includeBuildUUIDs", BsonValue::Boolean(true), 72),
        ("includeIndexBuildInfo", BsonValue::Boolean(true), 72),
        ("filter", BsonValue::Document(BsonDocument::new()), 72),
        ("writeConcern", BsonValue::Document(BsonDocument::new()), 72),
    ] {
        let mut invalid = listing.clone();
        invalid.push(field, value).unwrap();
        let reply = send_command(&mut stream, &invalid).await;
        assert_eq!(
            reply.get_first("code"),
            Some(&BsonValue::Int32(code)),
            "{reply:?}"
        );
    }
    let absent = BsonDocument::from_entries([
        ("listIndexes", BsonValue::from("missing")),
        ("$db", BsonValue::from("wire_indexes")),
    ])
    .unwrap();
    assert_eq!(
        send_command(&mut stream, &absent).await.get_first("code"),
        Some(&BsonValue::Int32(26))
    );
    server.close().await.unwrap();
    database.close().await.unwrap();
}

async fn run_driver(
    address: std::net::SocketAddr,
    phase: &'static str,
    script: &'static str,
) -> std::process::Output {
    let python = std::env::var("BRISKDB_MONGO_WIRE_PYTHON").unwrap_or_else(|_| "python3".into());
    let uri = format!("mongodb://{address}/");
    tokio::task::spawn_blocking(move || {
        Command::new(python)
            .arg(format!("{}{script}", env!("CARGO_MANIFEST_DIR")))
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

#[tokio::test]
async fn index_creation_rejects_duplicate_options_and_malformed_batches_before_namespace_creation()
{
    fn doc(entries: Vec<(&str, BsonValue)>) -> BsonDocument {
        BsonDocument::from_entries(entries).unwrap()
    }
    async fn reject_duplicate(address: std::net::SocketAddr, body: &BsonDocument) {
        use briskdb::document::{
            BsonCodecOptions, DuplicateFieldPolicy, encode_document_with_options,
        };
        let raw = encode_document_with_options(
            body,
            &BsonCodecOptions::new().with_duplicate_field_policy(DuplicateFieldPolicy::Preserve),
        )
        .unwrap();
        let mut payload = BytesMut::new();
        payload.put_u32_le(0);
        payload.put_u8(0);
        payload.extend_from_slice(&raw);
        let mut bytes = BytesMut::new();
        FrameCodec::default()
            .encode(
                Frame {
                    request_id: 77,
                    response_to: 0,
                    opcode: Opcode::Message,
                    payload: payload.freeze(),
                },
                &mut bytes,
            )
            .unwrap();
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(&bytes).await.unwrap();
        // Duplicate BSON fields are fatal at the decoder, before command parsing.
        disconnected(&mut stream).await;
    }
    let (root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let keys = || BsonValue::Document(doc(vec![("value", BsonValue::Int32(1))]));
    let good = || BsonValue::Document(doc(vec![("key", keys())]));
    let bad_entries = vec![
        BsonValue::Document(doc(vec![
            ("key", keys()),
            ("name", BsonValue::from("a")),
            ("name", BsonValue::from("b")),
        ])),
        BsonValue::Document(doc(vec![(
            "key",
            BsonValue::Document(doc(vec![
                ("value", BsonValue::Int32(1)),
                ("value", BsonValue::Int32(-1)),
            ])),
        )])),
        BsonValue::Document(doc(vec![
            ("key", keys()),
            ("sparse", BsonValue::Boolean(true)),
            ("sparse", BsonValue::Boolean(false)),
        ])),
    ];
    for bad in bad_entries {
        let body = doc(vec![
            ("createIndexes", BsonValue::from("absent")),
            ("indexes", BsonValue::Array(vec![good(), bad])),
            ("$db", BsonValue::from("wire")),
        ]);
        reject_duplicate(server.address(), &body).await;
    }
    let duplicate_body = doc(vec![
        ("createIndexes", BsonValue::from("absent")),
        ("indexes", BsonValue::Array(vec![good()])),
        ("indexes", BsonValue::Array(vec![good()])),
        ("$db", BsonValue::from("wire")),
    ]);
    reject_duplicate(server.address(), &duplicate_body).await;
    for indexes in [
        vec![],
        vec![good(); 1001],
        vec![good(), BsonValue::Int32(1)],
        vec![
            good(),
            BsonValue::Document(doc(vec![
                ("key", keys()),
                ("expireAfterSeconds", BsonValue::Int32(-1)),
            ])),
        ],
        vec![
            good(),
            BsonValue::Document(doc(vec![
                ("key", keys()),
                ("expireAfterSeconds", BsonValue::Int32(60)),
                ("unique", BsonValue::Boolean(true)),
            ])),
        ],
        vec![
            good(),
            BsonValue::Document(doc(vec![
                ("key", keys()),
                ("background", BsonValue::Int32(1)),
            ])),
        ],
        vec![
            good(),
            BsonValue::Document(doc(vec![
                (
                    "key",
                    BsonValue::Document(doc(vec![("body", BsonValue::from("text"))])),
                ),
                ("unique", BsonValue::Boolean(true)),
            ])),
        ],
        vec![
            BsonValue::Document(doc(vec![(
                "key",
                BsonValue::Document(doc(vec![("body", BsonValue::from("text"))])),
            )])),
            BsonValue::Document(doc(vec![(
                "key",
                BsonValue::Document(doc(vec![("bad", BsonValue::Boolean(true))])),
            )])),
        ],
    ] {
        let body = doc(vec![
            ("createIndexes", BsonValue::from("absent")),
            ("indexes", BsonValue::Array(indexes)),
            ("$db", BsonValue::from("wire")),
        ]);
        assert_eq!(
            send_command(&mut stream, &body).await.get_first("ok"),
            Some(&BsonValue::Double(0.0))
        );
    }
    for extra in [
        (
            "writeConcern",
            BsonValue::Document(doc(vec![("w", BsonValue::Int32(0))])),
        ),
        ("commitQuorum", BsonValue::Int32(1)),
        ("maxTimeMS", BsonValue::Int32(-1)),
    ] {
        let body = doc(vec![
            ("createIndexes", BsonValue::from("absent")),
            ("indexes", BsonValue::Array(vec![good()])),
            ("$db", BsonValue::from("wire")),
            extra,
        ]);
        assert_eq!(
            send_command(&mut stream, &body).await.get_first("ok"),
            Some(&BsonValue::Double(0.0))
        );
    }
    // Index DDL rejects document sequences rather than treating them as writes.
    let body = doc(vec![
        ("createIndexes", BsonValue::from("absent")),
        ("indexes", BsonValue::Array(vec![good()])),
        ("$db", BsonValue::from("wire")),
    ]);
    let mut bytes = packet(&body, 77, 0);
    let raw = encode_document(&BsonDocument::new()).unwrap();
    bytes.put_u8(1);
    bytes.put_i32_le((4 + 10 + raw.len()) as i32);
    bytes.extend_from_slice(b"documents\0");
    bytes.extend_from_slice(&raw);
    let length = bytes.len() as i32;
    bytes[..4].copy_from_slice(&length.to_le_bytes());
    stream.write_all(&bytes).await.unwrap();
    assert_eq!(
        response(&mut stream).await.1.get_first("code"),
        Some(&BsonValue::Int32(72))
    );
    drop(stream);
    server.close().await.unwrap();
    database.close().await.unwrap();
    let manifest = rusqlite::Connection::open(root.path().join("manifest.sqlite")).unwrap();
    let count: i64 = manifest
        .query_row(
            "SELECT count(*) FROM briskdb_document_collections",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn mixed_index_model_replies_are_explicit_and_catalogs_describe_effective_keys() {
    fn doc(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
        BsonDocument::from_entries(entries).unwrap()
    }
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let body = doc([
        ("createIndexes", BsonValue::from("models")),
        (
            "indexes",
            BsonValue::Array(vec![
                BsonValue::Document(doc([
                    (
                        "key",
                        BsonValue::Document(doc([("token", BsonValue::from("hashed"))])),
                    ),
                    ("expireAfterSeconds", BsonValue::Double(0.5)),
                    ("background", BsonValue::Boolean(true)),
                ])),
                BsonValue::Document(doc([
                    (
                        "key",
                        BsonValue::Document(doc([("email", BsonValue::Int32(1))])),
                    ),
                    ("unique", BsonValue::Boolean(true)),
                ])),
            ]),
        ),
        ("$db", BsonValue::from("wire")),
    ]);
    let created = send_command(&mut stream, &body).await;
    assert_eq!(created.get_first("ok"), Some(&BsonValue::Double(1.0)));
    assert_eq!(
        created.get_first("numIndexesBefore"),
        Some(&BsonValue::Int64(1))
    );
    assert_eq!(
        created.get_first("numIndexesAfter"),
        Some(&BsonValue::Int64(3))
    );
    assert_eq!(
        created.get_first("briskdbIndexWarnings"),
        Some(&BsonValue::Array(vec![BsonValue::Document(doc([
            ("name", BsonValue::from("token_hashed")),
            (
                "reducedBehavior",
                BsonValue::Array(vec![
                    BsonValue::from("hashed: ascending equality indexing"),
                    BsonValue::from("ttl: expiration is not performed"),
                    BsonValue::from("background: builds run synchronously"),
                ])
            ),
        ])),]))
    );
    let retry = send_command(&mut stream, &body).await;
    assert_eq!(
        retry.get_first("numIndexesBefore"),
        Some(&BsonValue::Int64(3))
    );
    assert_eq!(
        retry.get_first("numIndexesAfter"),
        Some(&BsonValue::Int64(3))
    );
    assert_eq!(
        retry.get_first("briskdbIndexWarnings"),
        created.get_first("briskdbIndexWarnings")
    );
    let listing = send_command(
        &mut stream,
        &doc([
            ("listIndexes", BsonValue::from("models")),
            ("$db", BsonValue::from("wire")),
        ]),
    )
    .await;
    let Some(BsonValue::Document(cursor)) = listing.get_first("cursor") else {
        panic!("{listing:?}")
    };
    let Some(BsonValue::Array(rows)) = cursor.get_first("firstBatch") else {
        panic!("{cursor:?}")
    };
    assert_eq!(rows.len(), 3);
    let Some(BsonValue::Document(token)) = rows.last() else {
        panic!("{rows:?}")
    };
    assert_eq!(
        token,
        &doc([
            ("name", BsonValue::from("token_hashed")),
            (
                "key",
                BsonValue::Document(doc([("token", BsonValue::Int32(1))]))
            ),
        ])
    );
    drop(stream);
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn skipped_text_models_have_explicit_noop_counts_without_phantom_catalog_entries() {
    fn doc(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
        BsonDocument::from_entries(entries).unwrap()
    }
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let text = BsonValue::Document(doc([
        (
            "key",
            BsonValue::Document(doc([
                ("body", BsonValue::from("text")),
                ("token", BsonValue::from("hashed")),
            ])),
        ),
        ("background", BsonValue::Boolean(true)),
        ("expireAfterSeconds", BsonValue::Int32(1)),
    ]));
    let command = |indexes| {
        doc([
            ("createIndexes", BsonValue::from("text_models")),
            ("indexes", BsonValue::Array(indexes)),
            ("$db", BsonValue::from("wire")),
        ])
    };
    for _ in 0..2 {
        let reply = send_command(&mut stream, &command(vec![text.clone()])).await;
        assert_eq!(reply.get_first("ok"), Some(&BsonValue::Double(1.0)));
        assert_eq!(
            reply.get_first("numIndexesBefore"),
            Some(&BsonValue::Int64(1))
        );
        assert_eq!(
            reply.get_first("numIndexesAfter"),
            Some(&BsonValue::Int64(1))
        );
        assert_eq!(
            reply.get_first("briskdbIndexWarnings"),
            Some(&BsonValue::Array(vec![BsonValue::Document(doc([
                ("name", BsonValue::from("body_text_token_hashed")),
                ("skipped", BsonValue::Boolean(true)),
                (
                    "reducedBehavior",
                    BsonValue::Array(vec![BsonValue::from(
                        "text: entire index is skipped; $text queries are not supported"
                    )])
                ),
            ]))]))
        );
    }
    let mixed = send_command(
        &mut stream,
        &command(vec![
            text,
            BsonValue::Document(doc([
                (
                    "key",
                    BsonValue::Document(doc([("email", BsonValue::Int32(1))])),
                ),
                ("unique", BsonValue::Boolean(true)),
            ])),
        ]),
    )
    .await;
    assert_eq!(
        mixed.get_first("numIndexesBefore"),
        Some(&BsonValue::Int64(1))
    );
    assert_eq!(
        mixed.get_first("numIndexesAfter"),
        Some(&BsonValue::Int64(2))
    );
    let listing = send_command(
        &mut stream,
        &doc([
            ("listIndexes", BsonValue::from("text_models")),
            ("$db", BsonValue::from("wire")),
        ]),
    )
    .await;
    let Some(BsonValue::Document(cursor)) = listing.get_first("cursor") else {
        panic!("{listing:?}")
    };
    let Some(BsonValue::Array(rows)) = cursor.get_first("firstBatch") else {
        panic!("{cursor:?}")
    };
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| match row {
        BsonValue::Document(row) => matches!(row.get_first("name"), Some(BsonValue::String(name)) if name == "_id_" || name == "email_1"),
        _ => false,
    }));
    drop(stream);
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn index_removal_validates_selection_options_and_counts_without_record_loss() {
    fn doc<const N: usize>(entries: [(&str, BsonValue); N]) -> BsonDocument {
        BsonDocument::from_entries(entries).unwrap()
    }
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let create = doc([
        ("createIndexes", BsonValue::from("items")),
        (
            "indexes",
            BsonValue::Array(
                ["value", "tail"]
                    .map(|field| {
                        BsonValue::Document(doc([(
                            "key",
                            BsonValue::Document(doc([(field, BsonValue::Int32(1))])),
                        )]))
                    })
                    .to_vec(),
            ),
        ),
        ("$db", BsonValue::from("wire")),
    ]);
    assert_eq!(
        send_command(&mut stream, &create).await.get_first("ok"),
        Some(&BsonValue::Double(1.0))
    );
    let listing = doc([
        ("listIndexes", BsonValue::from("items")),
        ("$db", BsonValue::from("wire")),
    ]);
    let base = doc([
        ("dropIndexes", BsonValue::from("items")),
        ("$db", BsonValue::from("wire")),
    ]);
    for (selection, code) in [
        (BsonValue::Int32(1), 14),
        (BsonValue::Array(vec![BsonValue::from("value_1")]), 14),
        (
            BsonValue::Document(doc([("value", BsonValue::Int32(1))])),
            14,
        ),
        (BsonValue::from(""), 2),
        (BsonValue::from("_id"), 72),
        (BsonValue::from("_id_"), 72),
        (BsonValue::from("missing"), 27),
    ] {
        let mut invalid = base.clone();
        invalid.push("index", selection).unwrap();
        assert_eq!(
            send_command(&mut stream, &invalid).await.get_first("code"),
            Some(&BsonValue::Int32(code))
        );
        assert_eq!(
            first_batch(&send_command(&mut stream, &listing).await).len(),
            3
        );
    }
    assert_eq!(
        send_command(&mut stream, &base).await.get_first("code"),
        Some(&BsonValue::Int32(2))
    );
    let mut all = base.clone();
    all.push("index", BsonValue::from("*")).unwrap();
    for (field, value) in [
        (
            "writeConcern",
            BsonValue::Document(doc([("w", BsonValue::Int32(0))])),
        ),
        ("unknown", BsonValue::Boolean(true)),
        ("maxTimeMS", BsonValue::Int32(-1)),
    ] {
        let mut invalid = all.clone();
        invalid.push(field, value).unwrap();
        assert_eq!(
            send_command(&mut stream, &invalid).await.get_first("ok"),
            Some(&BsonValue::Double(0.0))
        );
        assert_eq!(
            first_batch(&send_command(&mut stream, &listing).await).len(),
            3
        );
    }
    let mut bytes = packet(&all, 77, 0);
    let raw = encode_document(&BsonDocument::new()).unwrap();
    bytes.put_u8(1);
    bytes.put_i32_le((4 + 10 + raw.len()) as i32);
    bytes.extend_from_slice(b"documents\0");
    bytes.extend_from_slice(&raw);
    let length = bytes.len() as i32;
    bytes[..4].copy_from_slice(&length.to_le_bytes());
    stream.write_all(&bytes).await.unwrap();
    assert_eq!(
        response(&mut stream).await.1.get_first("code"),
        Some(&BsonValue::Int32(72))
    );
    let record = doc([("_id", BsonValue::Int32(1)), ("value", BsonValue::Int32(2))]);
    assert_eq!(
        send_command(&mut stream, &insert_command("items", record.clone()))
            .await
            .get_first("ok"),
        Some(&BsonValue::Double(1.0))
    );
    let reply = send_command(&mut stream, &all).await;
    assert_eq!(reply.get_first("nIndexesWas"), Some(&BsonValue::Int64(3)));
    assert_eq!(
        first_batch(&send_command(&mut stream, &listing).await).len(),
        1
    );
    assert_eq!(
        first_batch(&send_command(&mut stream, &find_command("items", BsonValue::Int32(1))).await),
        &[BsonValue::Document(record)]
    );
    assert_eq!(
        send_command(&mut stream, &all)
            .await
            .get_first("nIndexesWas"),
        Some(&BsonValue::Int64(1))
    );
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn multi_update_errors_distinguish_confirmed_rollback_from_prior_commits() {
    fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
        BsonDocument::from_entries(fields).unwrap()
    }
    fn shard_rows(root: &std::path::Path, shard: u16) -> Vec<BsonDocument> {
        let connection =
            rusqlite::Connection::open(root.join(format!("shards/{shard:04}.sqlite"))).unwrap();
        let mut statement = connection
            .prepare("SELECT document_bson FROM briskdb_documents_v1 ORDER BY natural_order")
            .unwrap();
        statement
            .query_map([], |row| row.get::<_, Vec<u8>>(0))
            .unwrap()
            .map(|row| decode_document(&row.unwrap()).unwrap())
            .collect()
    }
    fn statement(id: Option<BsonValue>, field: &str, value: BsonValue) -> BsonValue {
        BsonValue::Document(doc([
            (
                "q",
                BsonValue::Document(id.map(|id| doc([("_id", id)])).unwrap_or_default()),
            ),
            (
                "u",
                BsonValue::Document(doc([("$set", BsonValue::Document(doc([(field, value)])))])),
            ),
        ]))
    }
    fn batch(statements: Vec<BsonValue>, ordered: bool) -> BsonDocument {
        doc([
            ("update", BsonValue::from("items")),
            ("updates", BsonValue::Array(statements)),
            ("ordered", BsonValue::Boolean(ordered)),
            ("$db", BsonValue::from("wire")),
        ])
    }
    for (bad_shard, earlier_noops, increment, upsert) in [
        (0, false, false),
        (1, true, false),
        (1, false, false),
        (0, false, true),
        (1, true, true),
        (1, false, true),
    ]
    .into_iter()
    .flat_map(|(shard, noops, increment)| {
        [false, true].map(|upsert| (shard, noops, increment, upsert))
    }) {
        let code = if increment { 14 } else { 2 };
        for ordered in [true, false] {
            let (root, database, mut server) = setup().await;
            let mut stream = TcpStream::connect(server.address()).await.unwrap();
            let documents: Vec<_> = (0..24)
                .map(|id| {
                    doc([
                        ("_id", BsonValue::Int32(id)),
                        (
                            "items",
                            if increment {
                                BsonValue::Decimal128(
                                    briskdb::document::BsonDecimal128::parse("NaN").unwrap(),
                                )
                            } else {
                                BsonValue::Array(vec![])
                            },
                        ),
                    ])
                })
                .collect();
            stream
                .write_all(&insert_sequence("items", &documents))
                .await
                .unwrap();
            assert_eq!(
                response(&mut stream).await.1.get_first("n"),
                Some(&BsonValue::Int32(24))
            );
            if earlier_noops {
                let statements = shard_rows(root.path(), 0)
                    .iter()
                    .map(|row| {
                        statement(
                            row.get_first("_id").cloned(),
                            "items",
                            if increment {
                                BsonValue::Int32(0)
                            } else {
                                BsonValue::Array(vec![BsonValue::Int32(1)])
                            },
                        )
                    })
                    .collect();
                assert_eq!(
                    send_command(&mut stream, &batch(statements, true))
                        .await
                        .get_first("ok"),
                    Some(&BsonValue::Double(1.0))
                );
            }
            let bad_rows = shard_rows(root.path(), bad_shard);
            assert!(bad_rows.len() > 1);
            // Fail after earlier records in this shard have changed privately,
            // not just on the first record before any SQL has executed.
            let bad_id = bad_rows.last().unwrap().get_first("_id").cloned();
            assert_eq!(
                send_command(
                    &mut stream,
                    &batch(vec![statement(bad_id, "items", BsonValue::Null)], true)
                )
                .await
                .get_first("nModified"),
                Some(&BsonValue::Int64(1))
            );
            let before: Vec<_> = (0..2).map(|shard| shard_rows(root.path(), shard)).collect();
            let failing = BsonValue::Document(doc([
                ("q", BsonValue::Document(BsonDocument::new())),
                (
                    "u",
                    BsonValue::Document(doc([(
                        if increment { "$inc" } else { "$addToSet" },
                        BsonValue::Document(doc([(
                            "items",
                            BsonValue::Int32(i32::from(!increment)),
                        )])),
                    )])),
                ),
                ("multi", BsonValue::Boolean(true)),
                ("upsert", BsonValue::Boolean(upsert)),
            ]));
            let reply = send_command(
                &mut stream,
                &batch(
                    vec![
                        failing,
                        statement(
                            Some(BsonValue::Int32(0)),
                            "continued",
                            BsonValue::Boolean(true),
                        ),
                    ],
                    ordered,
                ),
            )
            .await;
            let partial = bad_shard == 1 && !earlier_noops;
            if partial {
                assert_eq!(reply.get_first("ok"), Some(&BsonValue::Double(0.0)));
                assert_eq!(reply.get_first("code"), Some(&BsonValue::Int32(code)));
                assert!(reply.get_first("writeErrors").is_none());
                assert!(reply.get_first("n").is_none());
                assert!(reply.get_first("nModified").is_none());
            } else {
                assert_eq!(reply.get_first("ok"), Some(&BsonValue::Double(1.0)));
                let count = i64::from(!ordered);
                assert_eq!(reply.get_first("n"), Some(&BsonValue::Int64(count)));
                assert_eq!(reply.get_first("nModified"), Some(&BsonValue::Int64(count)));
                let Some(BsonValue::Array(errors)) = reply.get_first("writeErrors") else {
                    panic!("indexed errors: {reply:?}")
                };
                assert_eq!(errors.len(), 1);
                let BsonValue::Document(error) = &errors[0] else {
                    panic!("write error")
                };
                assert_eq!(error.get_first("index"), Some(&BsonValue::Int32(0)));
                assert_eq!(error.get_first("code"), Some(&BsonValue::Int32(code)));
            }
            let mut expected = before;
            for (shard, rows) in expected.iter_mut().enumerate() {
                for row in rows {
                    if partial && shard == 0 && !increment {
                        *row = BsonDocument::from_entries(row.iter().map(|(field, value)| {
                            (
                                field,
                                if field == "items" {
                                    BsonValue::Array(vec![BsonValue::Int32(1)])
                                } else {
                                    value.clone()
                                },
                            )
                        }))
                        .unwrap();
                    }
                    if !partial && !ordered && row.get_first("_id") == Some(&BsonValue::Int32(0)) {
                        row.push("continued", BsonValue::Boolean(true)).unwrap();
                    }
                }
            }
            for (shard, rows) in expected.iter().enumerate() {
                let encode = |rows: &[BsonDocument]| {
                    rows.iter()
                        .map(|row| encode_document(row).unwrap())
                        .collect::<Vec<_>>()
                };
                assert_eq!(encode(&shard_rows(root.path(), shard as u16)), encode(rows));
            }
            assert_eq!(
                send_command(&mut stream, &command("ping"))
                    .await
                    .get_first("ok"),
                Some(&BsonValue::Double(1.0))
            );
            drop(stream);
            server.close().await.unwrap();
            database.close().await.unwrap();
            let reopened = BriskDb::builder(root.path())
                .with_shard_count(2)
                .with_document_support(DocumentSupport::Enabled)
                .open()
                .await
                .unwrap();
            for (shard, rows) in expected.iter().enumerate() {
                assert_eq!(shard_rows(root.path(), shard as u16), *rows);
            }
            reopened.close().await.unwrap();
        }
    }
}

fn live_cursor_id(body: &BsonDocument) -> i64 {
    let Some(BsonValue::Document(cursor)) = body.get_first("cursor") else {
        panic!("cursor: {body:?}");
    };
    let Some(BsonValue::Int64(id)) = cursor.get_first("id") else {
        panic!("cursor ID");
    };
    *id
}

fn cursor_find(collection: &str, batch: i32) -> BsonDocument {
    BsonDocument::from_entries([
        ("find", BsonValue::from(collection)),
        ("batchSize", BsonValue::Int32(batch)),
        ("$db", BsonValue::from("wire")),
    ])
    .unwrap()
}

fn cursor_more(collection: &str, id: i64, batch: i32) -> BsonDocument {
    BsonDocument::from_entries([
        ("getMore", BsonValue::Int64(id)),
        ("collection", BsonValue::from(collection)),
        ("batchSize", BsonValue::Int32(batch)),
        ("$db", BsonValue::from("wire")),
    ])
    .unwrap()
}

fn cursor_aggregate(collection: &str, batch: i32, sorted: bool) -> BsonDocument {
    let pipeline = if sorted {
        vec![BsonValue::Document(
            BsonDocument::from_entries([(
                "$sort",
                BsonValue::Document(
                    BsonDocument::from_entries([("_id", BsonValue::Int32(-1))]).unwrap(),
                ),
            )])
            .unwrap(),
        )]
    } else {
        Vec::new()
    };
    BsonDocument::from_entries([
        ("aggregate", BsonValue::from(collection)),
        ("pipeline", BsonValue::Array(pipeline)),
        (
            "cursor",
            BsonValue::Document(
                BsonDocument::from_entries([("batchSize", BsonValue::Int32(batch))]).unwrap(),
            ),
        ),
        ("$db", BsonValue::from("wire")),
    ])
    .unwrap()
}

#[tokio::test]
async fn aggregate_cursors_follow_pool_handoff_disconnect_and_shared_limits() {
    let (_root, database, mut server) = setup().await;
    let mut writer = TcpStream::connect(server.address()).await.unwrap();
    let documents: Vec<_> = (0..12)
        .map(|id| BsonDocument::from_entries([("_id", BsonValue::Int32(id))]).unwrap())
        .collect();
    writer
        .write_all(&insert_sequence("aggregate_items", &documents))
        .await
        .unwrap();
    assert_eq!(
        response(&mut writer).await.1.get_first("n"),
        Some(&BsonValue::Int32(12))
    );
    for sorted in [false, true] {
        let mut first = TcpStream::connect(server.address()).await.unwrap();
        let opened =
            send_command(&mut first, &cursor_aggregate("aggregate_items", 2, sorted)).await;
        let id = live_cursor_id(&opened);
        assert!(id > 0);
        let mut second = TcpStream::connect(server.address()).await.unwrap();
        let rejected = send_command(&mut second, &cursor_more("wrong", id, 2)).await;
        assert_eq!(rejected.get_first("code"), Some(&BsonValue::Int32(43)));
        assert_eq!(
            live_cursor_id(
                &send_command(&mut second, &cursor_more("aggregate_items", id, 2)).await
            ),
            id
        );
        first.shutdown().await.unwrap();
        disconnected(&mut first).await;
        assert_eq!(
            live_cursor_id(
                &send_command(&mut second, &cursor_more("aggregate_items", id, 2)).await
            ),
            id
        );
        second.shutdown().await.unwrap();
        disconnected(&mut second).await;
        assert_eq!(
            send_command(&mut writer, &cursor_more("aggregate_items", id, 2))
                .await
                .get_first("code"),
            Some(&BsonValue::Int32(43))
        );
    }
    let mut ids = Vec::new();
    for index in 0..8 {
        let command = if index % 2 == 0 {
            cursor_find("aggregate_items", 0)
        } else {
            cursor_aggregate("aggregate_items", 0, false)
        };
        ids.push(live_cursor_id(&send_command(&mut writer, &command).await));
    }
    assert!(ids.iter().all(|id| *id > 0));
    assert_eq!(
        send_command(&mut writer, &cursor_aggregate("aggregate_items", 0, true))
            .await
            .get_first("code"),
        Some(&BsonValue::Int32(10334))
    );
    // Unacknowledged aggregate requests must never open an unreachable cursor.
    let mut bytes = packet(&cursor_aggregate("aggregate_items", 0, false), 100, 2);
    bytes.extend_from_slice(&packet(&command("ping"), 101, 0));
    writer.write_all(&bytes).await.unwrap();
    assert_eq!(response(&mut writer).await.0.response_to, 101);
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn cursor_handoff_disconnect_cleanup_and_restart_are_explicit() {
    let (root, database, mut server) = setup().await;
    let mut first = TcpStream::connect(server.address()).await.unwrap();
    let documents: Vec<_> = (0..12)
        .map(|id| BsonDocument::from_entries([("_id", BsonValue::Int32(id))]).unwrap())
        .collect();
    first
        .write_all(&insert_sequence("cursor_items", &documents))
        .await
        .unwrap();
    assert_eq!(
        response(&mut first).await.1.get_first("n"),
        Some(&BsonValue::Int32(12))
    );
    let opened = send_command(&mut first, &cursor_find("cursor_items", 2)).await;
    let id = live_cursor_id(&opened);
    assert!(id > 0);
    let mut second = TcpStream::connect(server.address()).await.unwrap();
    let rejected = send_command(&mut second, &cursor_more("wrong", id, 2)).await;
    assert_eq!(rejected.get_first("code"), Some(&BsonValue::Int32(43)));
    let page = send_command(&mut second, &cursor_more("cursor_items", id, 2)).await;
    assert_eq!(live_cursor_id(&page), id);
    // Ownership follows getMore to the second socket; closing the first must
    // not invalidate a cursor already transferred by the driver's pool.
    drop(first);
    let page = send_command(&mut second, &cursor_more("cursor_items", id, 2)).await;
    assert_eq!(live_cursor_id(&page), id);
    // Half-close and observe the server's EOF so cleanup has completed before
    // checking it. Polling killCursors would itself remove the cursor and could
    // conceal a disconnect-cleanup bug.
    second.shutdown().await.unwrap();
    disconnected(&mut second).await;
    let mut probe = TcpStream::connect(server.address()).await.unwrap();
    let kill = BsonDocument::from_entries([
        ("killCursors", BsonValue::from("cursor_items")),
        ("cursors", BsonValue::Array(vec![BsonValue::Int64(id)])),
        ("$db", BsonValue::from("wire")),
    ])
    .unwrap();
    assert_eq!(
        send_command(&mut probe, &kill)
            .await
            .get_first("cursorsNotFound"),
        Some(&BsonValue::Array(vec![BsonValue::Int64(id)]))
    );
    assert_eq!(
        send_command(&mut probe, &cursor_more("cursor_items", id, 2))
            .await
            .get_first("code"),
        Some(&BsonValue::Int32(43))
    );
    let stale = live_cursor_id(&send_command(&mut probe, &cursor_find("cursor_items", 0)).await);
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
    assert_eq!(
        send_command(&mut stream, &cursor_more("cursor_items", stale, 2))
            .await
            .get_first("code"),
        Some(&BsonValue::Int32(43))
    );
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn cursor_limits_malformed_commands_and_unacknowledged_reads_do_not_leak() {
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let seed = BsonDocument::from_entries([("_id", BsonValue::Int32(1))]).unwrap();
    send_command(&mut stream, &insert_command("cursor_limits", seed)).await;
    for _ in 0..12 {
        stream
            .write_all(&packet(&cursor_find("cursor_limits", 0), 4, 2))
            .await
            .unwrap();
    }
    assert_eq!(
        send_command(&mut stream, &command("ping"))
            .await
            .get_first("ok"),
        Some(&BsonValue::Double(1.0))
    );
    let mut ids = Vec::new();
    for _ in 0..8 {
        ids.push(BsonValue::Int64(live_cursor_id(
            &send_command(&mut stream, &cursor_find("cursor_limits", 0)).await,
        )));
    }
    assert_eq!(
        send_command(&mut stream, &cursor_find("cursor_limits", 0))
            .await
            .get_first("code"),
        Some(&BsonValue::Int32(10334))
    );
    let BsonValue::Int64(id) = ids[0] else {
        unreachable!()
    };
    for batch in [0, -1, 1001] {
        assert_eq!(
            send_command(&mut stream, &cursor_more("cursor_limits", id, batch))
                .await
                .get_first("code"),
            Some(&BsonValue::Int32(2))
        );
    }
    let kill = BsonDocument::from_entries([
        ("killCursors", BsonValue::from("cursor_limits")),
        ("cursors", BsonValue::Array(ids.clone())),
        ("$db", BsonValue::from("wire")),
    ])
    .unwrap();
    assert_eq!(
        send_command(&mut stream, &kill)
            .await
            .get_first("cursorsKilled"),
        Some(&BsonValue::Array(ids))
    );
    assert!(live_cursor_id(&send_command(&mut stream, &cursor_find("cursor_limits", 0)).await) > 0);
    let cursors = server.metrics().cursors;
    assert_eq!(
        (
            cursors.registered,
            cursors.closed,
            cursors.active,
            cursors.peak
        ),
        (9, 8, 1, 8)
    );
    assert_eq!((cursors.limit_rejections, cursors.idle_expired), (1, 0));
    server.close().await.unwrap();
    assert_eq!(
        (
            server.metrics().cursors.active,
            server.metrics().cursors.closed
        ),
        (0, 9)
    );
    database.close().await.unwrap();
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
        ("bypassDocumentValidation", BsonValue::from("true")),
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
async fn unacknowledged_writes_execute_without_emitting_a_reply() {
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
    let replacement = BsonDocument::from_entries([
        ("_id", BsonValue::from("one-way")),
        ("value", BsonValue::Int64(7)),
    ])
    .unwrap();
    let update = BsonDocument::from_entries([
        ("update", BsonValue::from("items")),
        ("$db", BsonValue::from("wire")),
        (
            "updates",
            BsonValue::Array(vec![BsonValue::Document(
                BsonDocument::from_entries([
                    ("q", BsonValue::Document(BsonDocument::new())),
                    ("u", BsonValue::Document(replacement.clone())),
                ])
                .unwrap(),
            )]),
        ),
        (
            "writeConcern",
            BsonValue::Document(BsonDocument::from_entries([("w", BsonValue::Int32(0))]).unwrap()),
        ),
    ])
    .unwrap();
    let mut bytes = packet(&update, 5, 2);
    bytes.extend_from_slice(&packet(
        &find_command("items", BsonValue::from("one-way")),
        6,
        0,
    ));
    stream.write_all(&bytes).await.unwrap();
    let (frame, body) = response(&mut stream).await;
    assert_eq!(frame.response_to, 6);
    assert!(
        matches!(first_batch(&body), [BsonValue::Document(actual)] if actual.representation_eq(&replacement))
    );
    let delete = BsonDocument::from_entries([
        ("delete", BsonValue::from("items")),
        ("$db", BsonValue::from("wire")),
        (
            "deletes",
            BsonValue::Array(vec![BsonValue::Document(
                BsonDocument::from_entries([
                    ("q", BsonValue::Document(BsonDocument::new())),
                    ("limit", BsonValue::Int32(0)),
                ])
                .unwrap(),
            )]),
        ),
        (
            "writeConcern",
            BsonValue::Document(BsonDocument::from_entries([("w", BsonValue::Int32(0))]).unwrap()),
        ),
    ])
    .unwrap();
    let mut bytes = packet(&delete, 3, 2);
    bytes.extend_from_slice(&packet(
        &find_command("items", BsonValue::from("one-way")),
        4,
        0,
    ));
    stream.write_all(&bytes).await.unwrap();
    let (frame, body) = response(&mut stream).await;
    assert_eq!(frame.response_to, 4);
    assert!(first_batch(&body).is_empty());
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn find_and_modify_rejects_wire_envelope_depth_before_mutation() {
    use briskdb::document::{
        BSON_MAX_NESTING_DEPTH, DocumentCommand, DocumentInsertRequest, DocumentNamespace,
        DocumentWriteOptions,
    };
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    send_command(
        &mut stream,
        &insert_command(
            "deep",
            BsonDocument::from_entries([("_id", BsonValue::from("seed"))]).unwrap(),
        ),
    )
    .await;
    let mut deep = BsonDocument::new();
    for _ in 1..BSON_MAX_NESTING_DEPTH {
        deep = BsonDocument::from_entries([("nested", BsonValue::Document(deep))]).unwrap();
    }
    deep.push("_id", BsonValue::from("deep")).unwrap();
    database
        .execute_document(
            &database.session(),
            engine_request(DocumentCommand::Insert(
                DocumentInsertRequest::new(
                    DocumentNamespace::new("wire", "deep").unwrap(),
                    vec![deep],
                    DocumentWriteOptions::new(),
                )
                .unwrap(),
            )),
        )
        .await
        .unwrap();
    for (operator, field, after) in [("$unset", "nested", false), ("$set", "changed", true)] {
        let mutation = BsonDocument::from_entries([
            ("findAndModify", BsonValue::from("deep")),
            ("$db", BsonValue::from("wire")),
            (
                "query",
                BsonValue::Document(
                    BsonDocument::from_entries([("_id", BsonValue::from("deep"))]).unwrap(),
                ),
            ),
            (
                "update",
                BsonValue::Document(
                    BsonDocument::from_entries([(
                        operator,
                        BsonValue::Document(
                            BsonDocument::from_entries([(field, BsonValue::Int32(1))]).unwrap(),
                        ),
                    )])
                    .unwrap(),
                ),
            ),
            ("new", BsonValue::Boolean(after)),
        ])
        .unwrap();
        assert_eq!(
            send_command(&mut stream, &mutation).await.get_first("code"),
            Some(&BsonValue::Int32(10334))
        );
    }
    let replacement = BsonDocument::from_entries([
        ("findAndModify", BsonValue::from("deep")),
        ("$db", BsonValue::from("wire")),
        (
            "query",
            BsonValue::Document(
                BsonDocument::from_entries([("_id", BsonValue::from("deep"))]).unwrap(),
            ),
        ),
        ("update", BsonValue::Document(BsonDocument::new())),
    ])
    .unwrap();
    assert_eq!(
        send_command(&mut stream, &replacement)
            .await
            .get_first("code"),
        Some(&BsonValue::Int32(10334))
    );
    // The following delete still sees the deep original, proving replacement
    // could not commit before discovering its return-envelope depth error.
    let mut removal = BsonDocument::from_entries([
        ("findAndModify", BsonValue::from("deep")),
        ("$db", BsonValue::from("wire")),
        ("remove", BsonValue::Boolean(true)),
        (
            "query",
            BsonValue::Document(
                BsonDocument::from_entries([("_id", BsonValue::from("deep"))]).unwrap(),
            ),
        ),
    ])
    .unwrap();
    assert_eq!(
        send_command(&mut stream, &removal).await.get_first("code"),
        Some(&BsonValue::Int32(10334))
    );
    removal
        .push(
            "fields",
            BsonValue::Document(
                BsonDocument::from_entries([("_id", BsonValue::Int32(1))]).unwrap(),
            ),
        )
        .unwrap();
    let reply = send_command(&mut stream, &removal).await;
    assert_eq!(
        reply.get_first("value"),
        Some(&BsonValue::Document(
            BsonDocument::from_entries([("_id", BsonValue::from("deep"))]).unwrap()
        ))
    );
    assert_eq!(
        send_command(&mut stream, &removal).await.get_first("value"),
        Some(&BsonValue::Null)
    );
    assert_eq!(
        first_batch(
            &send_command(&mut stream, &find_command("deep", BsonValue::from("seed"))).await
        )
        .len(),
        1
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
    server.set_read_metrics_enabled(true);
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
    let mut replacement = BsonDocument::from_entries([
        ("findAndModify", BsonValue::from("items")),
        ("$db", BsonValue::from("wire")),
        (
            "query",
            BsonValue::Document(
                BsonDocument::from_entries([("_id", BsonValue::from("large"))]).unwrap(),
            ),
        ),
    ])
    .unwrap();
    let mut update = replacement.clone();
    update
        .push(
            "update",
            BsonValue::Document(
                BsonDocument::from_entries([(
                    "$unset",
                    BsonValue::Document(
                        BsonDocument::from_entries([("value", BsonValue::Int32(1))]).unwrap(),
                    ),
                )])
                .unwrap(),
            ),
        )
        .unwrap();
    assert_eq!(
        send_command(&mut stream, &update).await.get_first("code"),
        Some(&BsonValue::Int32(10334))
    );
    replacement
        .push("update", BsonValue::Document(BsonDocument::new()))
        .unwrap();
    assert_eq!(
        send_command(&mut stream, &replacement)
            .await
            .get_first("code"),
        Some(&BsonValue::Int32(10334))
    );
    let removal = BsonDocument::from_entries([
        ("findAndModify", BsonValue::from("items")),
        ("$db", BsonValue::from("wire")),
        ("remove", BsonValue::Boolean(true)),
        (
            "query",
            BsonValue::Document(
                BsonDocument::from_entries([("_id", BsonValue::from("large"))]).unwrap(),
            ),
        ),
    ])
    .unwrap();
    assert_eq!(
        send_command(&mut stream, &removal).await.get_first("code"),
        Some(&BsonValue::Int32(10334))
    );
    // The projected read below must still find the large document: a response
    // BSON limit is rejected inside the write transaction, before deletion.
    let mut projected = find_command("items", BsonValue::from("large"));
    projected
        .push(
            "projection",
            BsonValue::Document(
                BsonDocument::from_entries([("_id", BsonValue::Int32(1))]).unwrap(),
            ),
        )
        .unwrap();
    assert_eq!(
        first_batch(&send_command(&mut stream, &projected).await),
        &[BsonValue::Document(
            BsonDocument::from_entries([("_id", BsonValue::from("large"))]).unwrap()
        ),]
    );
    let tail = BsonDocument::from_entries([("_id", BsonValue::from("tail"))]).unwrap();
    send_command(&mut stream, &insert_command("items", tail)).await;
    let id = live_cursor_id(&send_command(&mut stream, &cursor_find("items", 1)).await);
    assert!(id > 0);
    // The oversized document is followed by another row, so the engine retains
    // a continuation before the wire encoder rejects the batch. That error
    // must discard the cursor instead of leaking an inaccessible slot.
    assert_eq!(
        send_command(&mut stream, &cursor_more("items", id, 1))
            .await
            .get_first("code"),
        Some(&BsonValue::Int32(10334))
    );
    assert_eq!(
        send_command(&mut stream, &cursor_more("items", id, 1))
            .await
            .get_first("code"),
        Some(&BsonValue::Int32(43))
    );
    let id = live_cursor_id(&send_command(&mut stream, &cursor_aggregate("items", 1, false)).await);
    assert!(id > 0);
    assert_eq!(
        send_command(&mut stream, &cursor_more("items", id, 1))
            .await
            .get_first("code"),
        Some(&BsonValue::Int32(10334))
    );
    assert_eq!(
        send_command(&mut stream, &cursor_more("items", id, 1))
            .await
            .get_first("code"),
        Some(&BsonValue::Int32(43))
    );
    for _ in 0..8 {
        assert!(live_cursor_id(&send_command(&mut stream, &cursor_find("items", 0)).await) > 0);
    }
    assert!(matches!(
        send_command(&mut stream, &command("ping"))
            .await
            .get_first("ok"),
        Some(BsonValue::Double(1.0))
    ));
    let metrics = server.metrics();
    assert_eq!(metrics.response_limit_rejections, 3);
    // Successful engine reads are counted even when later wire encoding fails.
    assert!(metrics.reads.executions > 8);
    assert!(metrics.reads.documents_examined >= 3);
    assert_eq!(
        (
            metrics.cursors.registered,
            metrics.cursors.closed,
            metrics.cursors.active,
            metrics.cursors.peak
        ),
        (10, 2, 8, 8)
    );
    assert_eq!(metrics.errors_with_code(10334), Some(6));
    assert_eq!(metrics.errors_with_code(43), Some(2));
    assert_eq!(
        metrics
            .command(briskdb::protocol::mongo::MongoCommandKind::Find)
            .failed,
        1
    );
    assert_eq!(
        metrics
            .command(briskdb::protocol::mongo::MongoCommandKind::GetMore)
            .failed,
        4
    );
    server.close().await.unwrap();
    assert_eq!(
        (
            server.metrics().cursors.active,
            server.metrics().cursors.closed
        ),
        (0, 10)
    );
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
