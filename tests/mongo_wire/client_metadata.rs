use super::*;
use briskdb::protocol::mongo::MongoDriverKind;

pub(super) fn hello(name: &str, version: &str) -> BsonDocument {
    let driver = BsonDocument::from_entries([
        ("name", BsonValue::from(name)),
        ("version", BsonValue::from(version)),
    ])
    .unwrap();
    let client = BsonDocument::from_entries([
        ("driver", BsonValue::Document(driver)),
        ("application", BsonValue::from("secret-application")),
        ("platform", BsonValue::from("private-platform")),
    ])
    .unwrap();
    let mut body = command("hello");
    body.push("client", BsonValue::Document(client)).unwrap();
    body
}

async fn drained(server: &MongoServer) {
    timeout(Duration::from_secs(5), async {
        while !server.client_metadata().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn metadata_records_success_once_per_socket_and_drains_without_payloads() {
    let (_root, database, mut server) = setup().await;
    let mut first = TcpStream::connect(server.address()).await.unwrap();
    assert!(server.client_metadata().is_empty());
    let mut invalid = hello("PyMongo", "4.17.0");
    invalid
        .push("lsid", BsonValue::Document(BsonDocument::new()))
        .unwrap();
    assert_eq!(
        send_command(&mut first, &invalid).await.get_first("code"),
        Some(&BsonValue::Int32(72))
    );
    assert!(server.client_metadata().is_empty());
    let response = send_command(&mut first, &hello("PyMongo|c", "4.17.0")).await;
    assert_eq!(
        response.get_first("maxWireVersion"),
        Some(&BsonValue::Int32(8))
    );
    let original = server.client_metadata();
    assert_eq!(original.len(), 1);
    assert_eq!(original[0].driver, MongoDriverKind::PyMongo);
    assert_eq!(original[0].driver_version, Some([4, 17, 0]));
    send_command(&mut first, &hello("private-name", "1.2.3-private")).await;
    assert_eq!(server.client_metadata(), original);

    let mut second = TcpStream::connect(server.address()).await.unwrap();
    send_command(&mut second, &hello("PyMongo|c|async", "4.17.0")).await;
    let snapshot = server.client_metadata();
    assert_eq!(snapshot.len(), 2);
    assert_ne!(snapshot[0].connection_id, snapshot[1].connection_id);
    assert_eq!(snapshot[1].driver, MongoDriverKind::PyMongoAsync);
    let rendered = format!("{snapshot:?} {:?}", server.metrics());
    for value in ["secret-application", "private-platform", "private-name"] {
        assert!(!rendered.contains(value));
    }
    drop(first);
    drop(second);
    drained(&server).await;
    let mut reconnected = TcpStream::connect(server.address()).await.unwrap();
    send_command(&mut reconnected, &hello("private-name", "4.17.0")).await;
    let record = server.client_metadata()[0];
    assert_eq!(record.driver, MongoDriverKind::Other);
    assert_eq!(record.driver_version, None);
    assert_ne!(record.connection_id, original[0].connection_id);
    server.close().await.unwrap();
    assert!(server.client_metadata().is_empty());
    database.close().await.unwrap();
}

#[tokio::test]
async fn metadata_is_listener_local_and_cannot_be_added_after_initial_hello() {
    let (_root, database, mut server) = setup().await;
    let mut other = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    send_command(&mut stream, &command("hello")).await;
    send_command(&mut stream, &hello("PyMongo", "4.17.0")).await;
    assert!(server.client_metadata().is_empty());
    let mut stream = TcpStream::connect(other.address()).await.unwrap();
    send_command(&mut stream, &hello("PyMongo", "4.17.0-secret")).await;
    assert_eq!(other.client_metadata()[0].driver_version, None);
    assert!(server.client_metadata().is_empty());
    server.close().await.unwrap();
    assert_eq!(other.client_metadata().len(), 1);
    other.close().await.unwrap();
    assert!(other.client_metadata().is_empty());
    database.close().await.unwrap();
}

pub(super) async fn observe_driver(
    server: &MongoServer,
    phase: &'static str,
    script: &'static str,
) -> std::process::Output {
    let driver = run_driver(server.address(), phase, script);
    tokio::pin!(driver);
    let mut ticks = tokio::time::interval(Duration::from_millis(5));
    let mut observed = [false; 2];
    let output = loop {
        tokio::select! {
            output = &mut driver => break output,
            _ = ticks.tick() => {
                let records = server.client_metadata();
                assert!(records.len() <= 8);
                for record in records {
                    let index = match record.driver {
                        MongoDriverKind::PyMongo => 0,
                        MongoDriverKind::PyMongoAsync => 1,
                        _ => panic!("unexpected driver metadata: {record:?}"),
                    };
                    assert_eq!(record.driver_version, Some([4, 17, 0]));
                    observed[index] = true;
                }
            }
        }
    };
    // The full suite keeps both driver kinds connected across many commands.
    // Focused micro-checkpoints may finish between observer ticks; do not turn
    // their runtime duration into a new acceptance condition.
    if output.status.success() && script == "/tests/mongo_wire_client.py" {
        assert_eq!(
            observed, [true; 2],
            "both stock drivers must provide metadata in {phase}"
        );
    }
    output
}
