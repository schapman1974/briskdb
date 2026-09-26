//! Deterministic bounded stress, not a wall-clock performance or RSS benchmark.

use super::*;
use briskdb::protocol::mongo::{MongoCommandKind, MongoListenerState, MongoMetricsSnapshot};
use metrics::doc;

const COLLECTION: &str = "resource_churn";

fn assert_drained(server: &MongoServer) {
    let snapshot = server.metrics();
    assert_eq!(snapshot.active_connections, 0);
    assert_eq!(snapshot.closed_connections, snapshot.admitted_connections);
    assert_eq!(
        snapshot.accepted_connections,
        snapshot.admitted_connections + snapshot.rejected_connections
    );
    assert_eq!(snapshot.cursors.active, 0);
    assert_eq!(snapshot.cursors.registered, snapshot.cursors.closed);
    assert!(snapshot.peak_connections <= 8);
    assert!(snapshot.cursors.peak <= 32);
    assert_eq!(snapshot.accept_failures, 0);
    assert_eq!(snapshot.connection_task_failures, 0);
    assert!(server.client_metadata().is_empty());
    for command in snapshot.commands() {
        assert_eq!(command.in_flight, 0);
        assert_eq!(command.aborted, 0);
        assert_eq!(command.started, command.completed + command.aborted);
        assert_eq!(command.latency_buckets.iter().sum::<u64>(), command.started);
    }
}

async fn wait_drained(server: &MongoServer) {
    timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = server.metrics();
            if snapshot.active_connections == 0
                && snapshot.cursors.active == 0
                && server.client_metadata().is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all connection and cursor owners must drain between waves");
    assert_drained(server);
}

async fn close_peer(stream: &mut TcpStream) {
    stream.shutdown().await.unwrap();
    disconnected(stream).await;
}

async fn connected(server: &MongoServer) -> TcpStream {
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let reply = send_command(&mut stream, &client_metadata::hello("PyMongo", "4.17.0")).await;
    assert_eq!(reply.get_first("ok"), Some(&BsonValue::Double(1.0)));
    stream
}

fn assert_wave(before: &MongoMetricsSnapshot, after: &MongoMetricsSnapshot) {
    assert_eq!(after.accepted_connections - before.accepted_connections, 9);
    assert_eq!(after.admitted_connections - before.admitted_connections, 8);
    assert_eq!(after.rejected_connections - before.rejected_connections, 1);
    assert_eq!(after.cursors.registered - before.cursors.registered, 32);
    assert_eq!(after.cursors.closed - before.cursors.closed, 32);
    assert_eq!(
        after.transport_failures.malformed - before.transport_failures.malformed,
        2
    );
    assert_eq!(
        after.transport_failures.truncated - before.transport_failures.truncated,
        4
    );
    assert_eq!(after.transport_failures.timed_out, 0);
    assert_eq!(after.transport_failures.io, 0);
    assert_eq!(
        after.errors_with_code(11000).unwrap() - before.errors_with_code(11000).unwrap(),
        1
    );
    assert_eq!(
        after.errors_with_code(59).unwrap() - before.errors_with_code(59).unwrap(),
        1
    );
    assert_eq!(
        after.errors_with_code(10334).unwrap() - before.errors_with_code(10334).unwrap(),
        1
    );
    assert_eq!(
        after.command(MongoCommandKind::Insert).suppressed_responses
            - before
                .command(MongoCommandKind::Insert)
                .suppressed_responses,
        1
    );
}

async fn wave(server: &MongoServer, exhaust: bool) {
    assert_drained(server);
    let before = server.metrics();
    let mut peers = Vec::new();
    let mut ids = Vec::new();
    for _ in 0..4 {
        let mut stream = connected(server).await;
        let mut owned = Vec::new();
        for slot in 0..8 {
            let request = if slot % 2 == 0 {
                cursor_find(COLLECTION, 0)
            } else {
                cursor_aggregate(COLLECTION, 0, true)
            };
            let id = live_cursor_id(&send_command(&mut stream, &request).await);
            assert!(id > 0);
            owned.push(id);
        }
        peers.push(stream);
        ids.push(owned);
    }
    // Reaching full capacity anew proves prior waves released native cursor
    // slots too, not just their frontend metrics. Both cursor kinds count.
    assert_eq!(server.metrics().cursors.active, 32);
    let mut receiver = connected(server).await;
    let full = send_command(&mut receiver, &cursor_find(COLLECTION, 0)).await;
    assert_eq!(full.get_first("code"), Some(&BsonValue::Int32(10334)));
    let mut idle = Vec::new();
    for _ in 0..3 {
        idle.push(connected(server).await);
    }
    assert_eq!(server.metrics().active_connections, 8);
    let mut overflow = TcpStream::connect(server.address()).await.unwrap();
    disconnected(&mut overflow).await;
    let id = ids[0][0];
    assert_eq!(
        live_cursor_id(&send_command(&mut receiver, &cursor_more(COLLECTION, id, 1)).await),
        id
    );
    close_peer(&mut peers[0]).await;
    assert_eq!(
        live_cursor_id(&send_command(&mut receiver, &cursor_more(COLLECTION, id, 1)).await),
        id
    );
    assert_eq!(server.metrics().cursors.active, 25);

    // One-way reads must not register unreachable cursors. A following reply
    // fences both that rejected read and the duplicate unacknowledged write.
    receiver
        .write_all(&packet(&cursor_find(COLLECTION, 0), 10, 2))
        .await
        .unwrap();
    let mut duplicate = insert_command(COLLECTION, doc([("_id", BsonValue::Int32(0))]));
    duplicate
        .push(
            "writeConcern",
            BsonValue::Document(doc([("w", BsonValue::Int32(0))])),
        )
        .unwrap();
    receiver
        .write_all(&packet(&duplicate, 11, 2))
        .await
        .unwrap();
    let unknown = send_command(&mut receiver, &command("private-churn-command")).await;
    assert_eq!(unknown.get_first("code"), Some(&BsonValue::Int32(59)));
    assert_eq!(
        server.metrics().cursors.registered - before.cursors.registered,
        32
    );

    peers[1]
        .write_all(&((MAX_BOOTSTRAP_MESSAGE_BYTES + 1) as i32).to_le_bytes())
        .await
        .unwrap();
    disconnected(&mut peers[1]).await;
    peers[2].write_all(&[16, 0]).await.unwrap();
    close_peer(&mut peers[2]).await;
    let mut malformed = packet(&command("ping"), 12, 0);
    *malformed.last_mut().unwrap() = 1;
    peers[3].write_all(&malformed).await.unwrap();
    disconnected(&mut peers[3]).await;
    assert_eq!(server.metrics().cursors.active, 1);
    if exhaust {
        let reply = send_command(&mut receiver, &cursor_more(COLLECTION, id, 1000)).await;
        assert_eq!(live_cursor_id(&reply), 0);
    }
    // Alternate explicit exhaustion with disconnect of a retained handoff.
    close_peer(&mut receiver).await;
    for stream in &mut idle {
        stream.write_all(&[16, 0]).await.unwrap();
        close_peer(stream).await;
    }
    wait_drained(server).await;
    assert_wave(&before, &server.metrics());
    assert!(server.readiness().ready());
}

async fn run(cycles: usize, waves: usize) {
    let root = tempfile::tempdir().unwrap();
    let mut old_listeners = Vec::new();
    let mut stale = None;
    for cycle in 0..cycles {
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
        let mut seed = connected(&server).await;
        if cycle == 0 {
            let documents: Vec<_> = (0..12)
                .map(|id| doc([("_id", BsonValue::Int32(id))]))
                .collect();
            seed.write_all(&insert_sequence(COLLECTION, &documents))
                .await
                .unwrap();
            assert_eq!(
                response(&mut seed).await.1.get_first("n"),
                Some(&BsonValue::Int32(12))
            );
        }
        if let Some(id) = stale {
            assert_eq!(
                send_command(&mut seed, &cursor_more(COLLECTION, id, 1))
                    .await
                    .get_first("code"),
                Some(&BsonValue::Int32(43))
            );
        }
        let reply = send_command(&mut seed, &cursor_find(COLLECTION, 1000)).await;
        let actual: std::collections::BTreeSet<_> = first_batch(&reply)
            .iter()
            .map(|item| {
                let BsonValue::Document(document) = item else {
                    panic!("document")
                };
                let Some(BsonValue::Int32(id)) = document.get_first("_id") else {
                    panic!("id")
                };
                *id
            })
            .collect();
        assert_eq!(actual, (0..12).collect());
        close_peer(&mut seed).await;
        wait_drained(&server).await;
        for index in 0..waves {
            wave(&server, index % 2 == 0).await;
        }
        // Every cycle also drains shutdown with a retained cursor and partial
        // frames, then reopens the same durable records under a fresh listener.
        let mut partial = connected(&server).await;
        stale = Some(live_cursor_id(
            &send_command(&mut partial, &cursor_find(COLLECTION, 0)).await,
        ));
        assert!(stale.unwrap() > 0);
        partial.write_all(&[16, 0]).await.unwrap();
        timeout(Duration::from_secs(5), server.close())
            .await
            .unwrap()
            .unwrap();
        disconnected(&mut partial).await;
        assert_eq!(server.readiness().listener, MongoListenerState::Closed);
        assert_drained(&server);
        assert_eq!(server.metrics().peak_connections, 8);
        assert_eq!(server.metrics().cursors.peak, 32);
        database.close().await.unwrap();
        drop(database);
        assert!(server.readiness().engine.is_none());
        old_listeners.push(server);
        assert!(
            old_listeners
                .iter()
                .all(|closed| closed.readiness().engine.is_none())
        );
    }
    println!(
        "Resource churn passed: {cycles} engine cycles, {} waves, {} full-capacity cursor registrations, {} wave socket admissions (plus setup/shutdown).",
        cycles * waves,
        cycles * waves * 32,
        cycles * waves * 8
    );
}

#[tokio::test]
async fn repeated_cursor_connection_fault_churn_reclaims_capacity() {
    run(2, 4).await;
}

#[tokio::test]
#[ignore = "extended bounded resource stress; CI runs this explicitly, no Python required"]
async fn bounded_resource_soak() {
    run(4, 32).await;
}
