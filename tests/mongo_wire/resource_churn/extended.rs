//! Opt-in timed CRUD/restart soak. Finite stress, not allocator leak certification.

use super::*;
use std::time::Instant;

fn record(id: i32, revision: i64) -> BsonDocument {
    let mut record = doc([("_id", BsonValue::Int32(id))]);
    if id == 11 || id == 777 {
        record.push("revision", BsonValue::Int64(revision)).unwrap();
        record
            .push("payload", BsonValue::from("bounded-soak-record"))
            .unwrap();
    }
    record
}

fn assert_write(reply: &BsonDocument, count: BsonValue) {
    assert_eq!(reply.get_first("ok"), Some(&BsonValue::Double(1.0)));
    assert!(reply.get_first("writeErrors").is_none(), "{reply:?}");
    assert_eq!(reply.get_first("n"), Some(&count));
}

async fn assert_record(stream: &mut TcpStream, id: i32, revision: i64) {
    let reply = send_command(stream, &find_command(COLLECTION, BsonValue::Int32(id))).await;
    assert!(
        matches!(first_batch(&reply), [BsonValue::Document(actual)]
            if actual.representation_eq(&record(id, revision))),
        "record changed: {reply:?}"
    );
}

pub(super) async fn mutate(stream: &mut TcpStream, revision: i64) {
    assert_write(
        &send_command(stream, &insert_command(COLLECTION, record(777, revision))).await,
        BsonValue::Int32(1),
    );
    assert_record(stream, 777, revision).await;
    let delete = doc([
        ("delete", BsonValue::from(COLLECTION)),
        ("$db", BsonValue::from("wire")),
        (
            "deletes",
            BsonValue::Array(vec![BsonValue::Document(doc([
                (
                    "q",
                    BsonValue::Document(doc([("_id", BsonValue::Int32(777))])),
                ),
                ("limit", BsonValue::Int32(1)),
            ]))]),
        ),
    ]);
    assert_write(&send_command(stream, &delete).await, BsonValue::Int64(1));
    assert!(
        first_batch(&send_command(stream, &find_command(COLLECTION, BsonValue::Int32(777))).await)
            .is_empty()
    );
    let update = doc([
        ("update", BsonValue::from(COLLECTION)),
        ("$db", BsonValue::from("wire")),
        (
            "updates",
            BsonValue::Array(vec![BsonValue::Document(doc([
                (
                    "q",
                    BsonValue::Document(doc([("_id", BsonValue::Int32(11))])),
                ),
                (
                    "u",
                    BsonValue::Document(doc([(
                        "$inc",
                        BsonValue::Document(doc([("revision", BsonValue::Int64(1))])),
                    )])),
                ),
            ]))]),
        ),
    ]);
    let reply = send_command(stream, &update).await;
    assert_write(&reply, BsonValue::Int64(1));
    assert_eq!(reply.get_first("nModified"), Some(&BsonValue::Int64(1)));
    assert_record(stream, 11, revision).await;
}

async fn verify_all(stream: &mut TcpStream, revision: i64) {
    let reply = send_command(stream, &cursor_find(COLLECTION, 1000)).await;
    let batch = first_batch(&reply);
    assert_eq!(batch.len(), 12, "temporary records must not accumulate");
    for id in 0..12 {
        let expected = record(id, revision);
        assert_eq!(
            batch
                .iter()
                .filter(|value| matches!(value, BsonValue::Document(actual)
                    if actual.representation_eq(&expected)))
                .count(),
            1,
            "missing, duplicated or changed record {id} at revision {revision}"
        );
    }
}

async fn run_for(duration: Duration) {
    let root = tempfile::tempdir().unwrap();
    let started = Instant::now();
    let mut revision = 0;
    let mut cycles = 0;
    let mut stale = None;
    // Keep only one closed observer: the test itself must not grow a history of
    // listeners and then mistake that intentional retention for an engine leak.
    let mut previous: Option<MongoServer> = None;
    loop {
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
        let mut stream = connected(&server).await;
        if cycles == 0 {
            let documents: Vec<_> = (0..12).map(|id| record(id, 0)).collect();
            stream
                .write_all(&insert_sequence(COLLECTION, &documents))
                .await
                .unwrap();
            assert_write(&response(&mut stream).await.1, BsonValue::Int32(12));
        }
        if let Some(id) = stale {
            assert_eq!(
                send_command(&mut stream, &cursor_more(COLLECTION, id, 1))
                    .await
                    .get_first("code"),
                Some(&BsonValue::Int32(43))
            );
        }
        verify_all(&mut stream, revision).await;
        close_peer(&mut stream).await;
        wait_drained(&server).await;
        if previous
            .as_ref()
            .is_some_and(|old| old.readiness().engine.is_some())
        {
            panic!("closed observer retained the prior engine");
        }
        // Always perform a final, read-only reopen after the last write wave.
        let finished = cycles > 0 && started.elapsed() >= duration;
        if !finished {
            for _ in 0..32 {
                revision += 1;
                wave(&server, revision % 2 == 0, Some(revision)).await;
                if started.elapsed() >= duration {
                    break;
                }
            }
        }
        let mut partial = connected(&server).await;
        verify_all(&mut partial, revision).await;
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
        database.close().await.unwrap();
        drop(database);
        assert!(server.readiness().engine.is_none());
        previous = Some(server);
        cycles += 1;
        println!(
            "Timed soak: {:.1}s elapsed, {cycles} engine lifetimes, {revision} mixed-CRUD waves, {} cursor registrations, {} wave socket admissions; durable records exact, gauges drained.",
            started.elapsed().as_secs_f64(),
            revision * 32,
            revision * 8
        );
        if finished {
            assert!(revision > 0);
            assert!(cycles >= 2);
            break;
        }
    }
}

#[tokio::test]
#[ignore = "opt-in 10-minute CRUD/restart soak; BRISKDB_MONGO_SOAK_SECONDS=10..3600 overrides duration"]
async fn timed_mixed_crud_restart_soak() {
    let seconds = match std::env::var("BRISKDB_MONGO_SOAK_SECONDS") {
        Ok(value) => value
            .parse::<u64>()
            .expect("soak seconds must be an integer"),
        Err(std::env::VarError::NotPresent) => 600,
        Err(error) => panic!("invalid soak seconds: {error}"),
    };
    assert!(
        (10..=3600).contains(&seconds),
        "soak seconds must be 10..3600"
    );
    timeout(
        Duration::from_secs(seconds + 60),
        run_for(Duration::from_secs(seconds)),
    )
    .await
    .expect("soak exceeded its duration plus 60-second cleanup allowance");
}
