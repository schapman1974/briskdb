use super::*;
use briskdb::protocol::mongo::{MongoCommandKind, MongoMetricsSnapshot, MongoTransportFailures};

pub(super) fn assert_driver_metrics_drained(snapshot: &MongoMetricsSnapshot) {
    assert!(snapshot.accepted_connections > 0 && snapshot.admitted_connections > 0);
    assert_eq!(snapshot.active_connections, 0);
    assert_eq!(snapshot.closed_connections, snapshot.admitted_connections);
    for kind in [MongoCommandKind::Hello, MongoCommandKind::Find] {
        assert!(
            snapshot.command(kind).completed > 0,
            "real driver must reach {kind:?}"
        );
    }
    for command in snapshot.commands() {
        assert_eq!(command.in_flight, 0);
        assert_eq!(command.started, command.completed + command.aborted);
        assert_eq!(command.latency_buckets.iter().sum::<u64>(), command.started);
    }
}

fn doc(fields: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}

#[tokio::test]
async fn listener_metrics_count_final_errors_one_way_outcomes_and_reset_per_listener() {
    let (_root, database, mut server) = setup().await;
    assert_eq!(server.metrics().accepted_connections, 0);
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    for name in ["hello", "ping", "private-command-never-an-exporter-label"] {
        send_command(&mut stream, &command(name)).await;
    }
    let seed = doc([("_id", BsonValue::Int32(1))]);
    send_command(&mut stream, &insert_command("metrics", seed.clone())).await;
    let duplicate = doc([
        ("insert", BsonValue::from("metrics")),
        (
            "documents",
            BsonValue::Array(vec![
                BsonValue::Document(seed.clone()),
                BsonValue::Document(seed),
                BsonValue::Document(doc([("_id", BsonValue::Int32(2))])),
            ]),
        ),
        ("ordered", BsonValue::Boolean(false)),
        ("$db", BsonValue::from("wire")),
    ]);
    let reply = send_command(&mut stream, &duplicate).await;
    assert_eq!(reply.get_first("n"), Some(&BsonValue::Int32(1)));
    let Some(BsonValue::Array(errors)) = reply.get_first("writeErrors") else {
        panic!("write errors")
    };
    assert_eq!(errors.len(), 2);
    let mut one_way = insert_command("metrics", doc([("_id", BsonValue::Int32(3))]));
    one_way
        .push(
            "writeConcern",
            BsonValue::Document(doc([("w", BsonValue::Int32(0))])),
        )
        .unwrap();
    stream.write_all(&packet(&one_way, 88, 2)).await.unwrap();
    // The following reply fences completion of the preceding one-way request.
    send_command(&mut stream, &command("ping")).await;
    let snapshot = server.metrics();
    assert_eq!(
        (
            snapshot.accepted_connections,
            snapshot.admitted_connections,
            snapshot.active_connections
        ),
        (1, 1, 1)
    );
    assert_eq!(snapshot.command(MongoCommandKind::Hello).completed, 1);
    assert_eq!(snapshot.command(MongoCommandKind::Ping).completed, 2);
    let insert = snapshot.command(MongoCommandKind::Insert);
    assert_eq!(
        (
            insert.started,
            insert.completed,
            insert.failed,
            insert.suppressed_responses
        ),
        (3, 3, 1, 1)
    );
    assert_eq!(snapshot.command(MongoCommandKind::Other).failed, 1);
    assert_eq!(snapshot.errors_with_code(59), Some(1));
    assert_eq!(snapshot.errors_with_code(11000), Some(2));
    assert_eq!(snapshot.write_errors, 2);
    assert_eq!(snapshot.other_error_codes, 0);
    assert_eq!(
        snapshot
            .commands()
            .iter()
            .map(|command| command.started)
            .sum::<u64>(),
        7
    );
    assert!(!format!("{snapshot:?}").contains("private-command"));
    for command in snapshot.commands() {
        assert_eq!((command.in_flight, command.aborted), (0, 0));
        assert_eq!(
            command.latency_buckets.iter().sum::<u64>(),
            command.completed
        );
    }
    server.close().await.unwrap();
    let final_snapshot = server.metrics();
    assert_eq!(
        (
            final_snapshot.active_connections,
            final_snapshot.closed_connections
        ),
        (0, 1)
    );
    assert_eq!(
        final_snapshot.transport_failures,
        MongoTransportFailures::default()
    );
    assert_eq!(final_snapshot.connection_task_failures, 0);
    server.close().await.unwrap();
    assert_eq!(server.metrics(), final_snapshot);
    let mut restarted = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    assert_eq!(restarted.metrics().accepted_connections, 0);
    assert!(
        restarted
            .metrics()
            .commands()
            .iter()
            .all(|command| command.started == 0)
    );
    restarted.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn listener_metrics_separate_malformed_truncated_and_clean_shutdown_connections() {
    let (_root, database, mut server) = setup().await;
    let mut bad_length = TcpStream::connect(server.address()).await.unwrap();
    bad_length.write_all(&1_i32.to_le_bytes()).await.unwrap();
    disconnected(&mut bad_length).await;
    let mut truncated = TcpStream::connect(server.address()).await.unwrap();
    truncated.write_all(&64_i32.to_le_bytes()).await.unwrap();
    truncated.shutdown().await.unwrap();
    disconnected(&mut truncated).await;
    let mut malformed_bson = TcpStream::connect(server.address()).await.unwrap();
    let mut bytes = packet(&command("ping"), 2, 0);
    *bytes.last_mut().unwrap() = 1; // Corrupt BSON EOO, not a command-level error.
    malformed_bson.write_all(&bytes).await.unwrap();
    disconnected(&mut malformed_bson).await;
    let mut clean = TcpStream::connect(server.address()).await.unwrap();
    send_command(&mut clean, &command("ping")).await;
    server.close().await.unwrap();
    let snapshot = server.metrics();
    assert_eq!(
        (
            snapshot.accepted_connections,
            snapshot.admitted_connections,
            snapshot.closed_connections,
            snapshot.active_connections
        ),
        (4, 4, 4, 0)
    );
    assert_eq!(
        snapshot.transport_failures,
        MongoTransportFailures {
            malformed: 2,
            truncated: 1,
            timed_out: 0,
            io: 0
        }
    );
    assert_eq!(
        snapshot
            .commands()
            .iter()
            .map(|command| command.started)
            .sum::<u64>(),
        1
    );
    assert_eq!(snapshot.command(MongoCommandKind::Ping).completed, 1);
    assert_eq!(
        snapshot.error_codes().map(|(_, count)| count).sum::<u64>(),
        0
    );
    database.close().await.unwrap();
}
