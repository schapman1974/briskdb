use super::metrics::doc;
use super::*;

async fn seed(stream: &mut TcpStream, count: i32) {
    for id in 0..count {
        let reply = send_command(
            stream,
            &insert_command(
                "read_metrics",
                doc([
                    ("_id", BsonValue::Int32(id)),
                    ("a", BsonValue::Int32(id % 6)),
                ]),
            ),
        )
        .await;
        assert_eq!(reply.get_first("n"), Some(&BsonValue::Int32(1)));
    }
}

fn find_by_a() -> BsonDocument {
    doc([
        ("find", BsonValue::from("read_metrics")),
        (
            "filter",
            BsonValue::Document(doc([("a", BsonValue::Int32(1))])),
        ),
        ("$db", BsonValue::from("wire")),
    ])
}

#[tokio::test]
async fn optional_read_metrics_preserve_replies_and_measure_scan_index_and_point_work() {
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    seed(&mut stream, 12).await;
    assert!(!server.metrics().read_metrics_enabled);
    let plain = send_command(&mut stream, &find_by_a()).await;
    assert_eq!(first_batch(&plain).len(), 2);
    assert_eq!(server.metrics().reads.executions, 0);
    server.set_read_metrics_enabled(true);
    let observed = send_command(&mut stream, &find_by_a()).await;
    assert!(observed.representation_eq(&plain));
    let scan = server.metrics().reads;
    assert_eq!(
        (
            scan.executions,
            scan.storage_reads,
            scan.documents_examined,
            scan.matcher_evaluations,
            scan.source_matches,
            scan.output_items
        ),
        (1, 14, 12, 12, 2, 2)
    );
    assert_eq!(
        (
            scan.scan_plans,
            scan.planned_shard_targets,
            scan.shard_visits
        ),
        (1, 2, 2)
    );
    assert_eq!(&scan.shard_requests[..2], &[1, 1]);
    send_command(
        &mut stream,
        &find_command("read_metrics", BsonValue::Int32(1)),
    )
    .await;
    let point = server.metrics().reads;
    assert_eq!(point.point_plans, 1);
    assert_eq!(point.storage_reads - scan.storage_reads, 1);
    assert_eq!(point.documents_examined - scan.documents_examined, 1);
    assert_eq!(point.matcher_evaluations, scan.matcher_evaluations);
    assert_eq!(point.source_matches - scan.source_matches, 1);
    assert_eq!(point.shard_visits - scan.shard_visits, 1);
    let created = send_command(
        &mut stream,
        &doc([
            ("createIndexes", BsonValue::from("read_metrics")),
            (
                "indexes",
                BsonValue::Array(vec![BsonValue::Document(doc([(
                    "key",
                    BsonValue::Document(doc([("a", BsonValue::Int32(1))])),
                )]))]),
            ),
            ("$db", BsonValue::from("wire")),
        ]),
    )
    .await;
    assert_eq!(created.get_first("ok"), Some(&BsonValue::Double(1.0)));
    assert!(
        send_command(&mut stream, &find_by_a())
            .await
            .representation_eq(&plain)
    );
    let indexed = server.metrics().reads;
    assert_eq!(indexed.index_candidate_plans, 1);
    assert_eq!(indexed.documents_examined - point.documents_examined, 2);
    assert_eq!(indexed.matcher_evaluations - point.matcher_evaluations, 2);
    assert_eq!(indexed.source_matches - point.source_matches, 2);
    assert_eq!(indexed.storage_reads - point.storage_reads, 4);
    assert_eq!(indexed.shard_visits - point.shard_visits, 2);
    let mut sorted_find = find_by_a();
    sorted_find
        .push(
            "sort",
            BsonValue::Document(doc([("_id", BsonValue::Int32(1))])),
        )
        .unwrap();
    assert!(
        send_command(&mut stream, &sorted_find)
            .await
            .representation_eq(&plain)
    );
    let sorted = server.metrics().reads;
    // The indexed key window reads two matches, then output fetches recheck both.
    assert_eq!(
        sorted.index_candidate_plans - indexed.index_candidate_plans,
        1
    );
    assert_eq!(sorted.documents_examined - indexed.documents_examined, 4);
    assert_eq!(sorted.matcher_evaluations - indexed.matcher_evaluations, 4);
    assert_eq!(sorted.source_matches - indexed.source_matches, 4);
    assert_eq!(sorted.storage_reads - indexed.storage_reads, 6);
    // Catalog cursors and legacy count have no engine read-work snapshots.
    let listing = send_command(
        &mut stream,
        &doc([
            ("listIndexes", BsonValue::from("read_metrics")),
            (
                "cursor",
                BsonValue::Document(doc([("batchSize", BsonValue::Int32(0))])),
            ),
            ("$db", BsonValue::from("wire")),
        ]),
    )
    .await;
    let id = live_cursor_id(&listing);
    assert!(id > 0);
    let Some(BsonValue::Document(cursor)) = listing.get_first("cursor") else {
        panic!("cursor")
    };
    let Some(BsonValue::String(namespace)) = cursor.get_first("ns") else {
        panic!("namespace")
    };
    let collection = namespace.split_once('.').unwrap().1;
    assert_eq!(
        live_cursor_id(&send_command(&mut stream, &cursor_more(collection, id, 1000)).await),
        0
    );
    let count = send_command(
        &mut stream,
        &doc([
            ("count", BsonValue::from("read_metrics")),
            ("$db", BsonValue::from("wire")),
        ]),
    )
    .await;
    assert_eq!(count.get_first("n"), Some(&BsonValue::Int64(12)));
    assert_eq!(server.metrics().reads, sorted);
    server.set_read_metrics_enabled(false);
    assert!(
        send_command(&mut stream, &find_by_a())
            .await
            .representation_eq(&plain)
    );
    assert_eq!(server.metrics().reads, sorted);
    server.close().await.unwrap();
    assert_eq!(server.metrics().reads, sorted);
    let mut restarted = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    assert!(!restarted.metrics().read_metrics_enabled);
    assert_eq!(restarted.metrics().reads.executions, 0);
    restarted.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn read_metrics_capture_each_cursor_page_and_distinguish_buffered_aggregate_output() {
    let (_root, database, mut server) = setup().await;
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    seed(&mut stream, 7).await;
    server.set_read_metrics_enabled(true);
    let id = live_cursor_id(&send_command(&mut stream, &cursor_find("read_metrics", 0)).await);
    assert!(id > 0);
    let empty = server.metrics().reads;
    assert_eq!(
        (empty.executions, empty.storage_reads, empty.output_items),
        (1, 0, 0)
    );
    assert_eq!(empty.fanout_buckets[0], 1);
    assert_eq!(empty.source_matches, 0);
    server.set_read_metrics_enabled(false);
    assert_eq!(
        live_cursor_id(&send_command(&mut stream, &cursor_more("read_metrics", id, 1)).await),
        id
    );
    assert_eq!(server.metrics().reads, empty);
    server.set_read_metrics_enabled(true);
    assert_eq!(
        live_cursor_id(&send_command(&mut stream, &cursor_more("read_metrics", id, 1000)).await),
        0
    );
    let continued = server.metrics().reads;
    assert_eq!(continued.executions, 2);
    assert_eq!(continued.output_items, 6);
    assert!(continued.documents_examined >= 6);
    assert_eq!(continued.source_matches, continued.documents_examined);
    let id = live_cursor_id(
        &send_command(&mut stream, &cursor_aggregate("read_metrics", 1, true)).await,
    );
    assert!(id > 0);
    let aggregated = server.metrics().reads;
    assert_eq!(aggregated.executions, 3);
    assert!(aggregated.documents_examined - continued.documents_examined >= 7);
    assert_eq!(
        aggregated.matcher_evaluations,
        continued.matcher_evaluations
    );
    assert_eq!(aggregated.shard_visits - continued.shard_visits, 2);
    assert_eq!(aggregated.source_matches, aggregated.documents_examined);
    assert_eq!(
        live_cursor_id(&send_command(&mut stream, &cursor_more("read_metrics", id, 1000)).await),
        0
    );
    let buffered = server.metrics().reads;
    assert_eq!(buffered.executions, 4);
    assert_eq!(buffered.output_items - aggregated.output_items, 6);
    assert_eq!(buffered.storage_reads, aggregated.storage_reads);
    assert_eq!(buffered.source_matches, aggregated.source_matches);
    assert_eq!(buffered.shard_visits, aggregated.shard_visits);
    assert_eq!(buffered.fanout_buckets[0] - aggregated.fanout_buckets[0], 1);
    let values = send_command(
        &mut stream,
        &doc([
            ("distinct", BsonValue::from("read_metrics")),
            ("key", BsonValue::from("_id")),
            ("$db", BsonValue::from("wire")),
        ]),
    )
    .await;
    assert!(
        matches!(values.get_first("values"), Some(BsonValue::Array(values)) if values.len() == 7)
    );
    let distinct = server.metrics().reads;
    assert_eq!(distinct.executions, 5);
    assert_eq!(distinct.output_items - buffered.output_items, 7);
    // Distinct buffers one source row per internal page: seven returned rows
    // plus six repeated lookahead rows are thirteen examinations, not seven.
    assert_eq!(
        distinct.documents_examined - buffered.documents_examined,
        13
    );
    assert_eq!(distinct.shard_visits - buffered.shard_visits, 2);
    assert_eq!(distinct.source_matches - buffered.source_matches, 13);
    // Stale continuations do not fabricate successful read work.
    assert_eq!(
        send_command(&mut stream, &cursor_more("read_metrics", id, 1000))
            .await
            .get_first("code"),
        Some(&BsonValue::Int32(43))
    );
    assert_eq!(server.metrics().reads, distinct);
    // Sort syntax is valid, but parallel array keys fail while examining a row.
    // No partial successful-work snapshot may be fabricated from that failure.
    send_command(
        &mut stream,
        &insert_command(
            "bad_read_metrics",
            doc([
                ("_id", BsonValue::Int32(1)),
                (
                    "a",
                    BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(2)]),
                ),
                (
                    "b",
                    BsonValue::Array(vec![BsonValue::Int32(3), BsonValue::Int32(4)]),
                ),
            ]),
        ),
    )
    .await;
    let failed = send_command(
        &mut stream,
        &doc([
            ("find", BsonValue::from("bad_read_metrics")),
            (
                "sort",
                BsonValue::Document(doc([
                    ("a", BsonValue::Int32(1)),
                    ("b", BsonValue::Int32(1)),
                ])),
            ),
            ("$db", BsonValue::from("wire")),
        ]),
    )
    .await;
    assert_eq!(failed.get_first("ok"), Some(&BsonValue::Double(0.0)));
    assert_eq!(server.metrics().reads, distinct);
    server.close().await.unwrap();
    assert_eq!(server.metrics().cursors.active, 0);
    database.close().await.unwrap();
}
