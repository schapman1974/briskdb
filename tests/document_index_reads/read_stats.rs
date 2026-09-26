use super::*;
use briskdb::{
    core::ResultLimits,
    document::{DocumentAggregateRequest, DocumentPipeline, DocumentPlan, DocumentReadStats},
};

fn options() -> DocumentReadOptions {
    DocumentReadOptions::new().with_execution_stats(true)
}
fn command(
    namespace: &DocumentNamespace,
    query: BsonDocument,
    options: DocumentReadOptions,
) -> DocumentCommand {
    DocumentCommand::Find(DocumentFindRequest::new(
        namespace.clone(),
        DocumentFilter::new(query).unwrap(),
        options,
    ))
}
fn stats(execution: &DocumentExecution) -> DocumentReadStats {
    let stats = execution.read_stats().expect("opt-in counters");
    assert_eq!(
        stats
            .shard_work()
            .map(|row| row.shard())
            .collect::<Vec<_>>(),
        stats.shards_read().collect::<Vec<_>>()
    );
    assert_eq!(
        stats
            .shard_work()
            .map(|row| row.documents_examined())
            .sum::<u64>(),
        stats.documents_examined()
    );
    assert_eq!(
        stats
            .shard_work()
            .map(|row| row.source_matches())
            .sum::<u64>(),
        stats.source_matches()
    );
    assert!(
        stats
            .shard_work()
            .all(|row| row.source_matches() <= row.documents_examined())
    );
    stats
}

#[tokio::test]
async fn read_stats_observe_points_pruned_shards_and_index_candidate_work() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("read_stats");
    seed(&engine, &session, &namespace, 14).await;
    let query = doc([("a", BsonValue::Int32(1))]);
    let scan = call(
        &engine,
        &session,
        command(&namespace, query.clone(), options()),
    )
    .await;
    assert_eq!(stats(&scan).documents_examined(), 14);
    assert_eq!(stats(&scan).matcher_evaluations(), 14);
    assert_eq!(stats(&scan).storage_reads(), 18);
    assert_eq!(
        stats(&scan).shards_read().collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert!(
        matches!(scan.plan(), Some(DocumentPlan::Scatter(plan)) if plan.read_access().is_none())
    );
    let scan_matches = stats(&scan).source_matches();
    let scan_work = stats(&scan).shard_work().collect::<Vec<_>>();
    let expected = page(scan).1;
    let expected_matches = expected.len() as u64;
    assert_eq!(scan_matches, expected_matches);
    let mut documents_by_shard = [0_u64; 4];
    let mut matches_by_shard = [0_u64; 4];
    for id in 0..14 {
        let point = call(
            &engine,
            &session,
            command(
                &namespace,
                doc([("_id", BsonValue::Int32(id))]),
                DocumentReadOptions::new(),
            ),
        )
        .await;
        let shard = usize::from(point.plan().unwrap().shards()[0]);
        documents_by_shard[shard] += 1;
        if expected
            .iter()
            .any(|row| row.get_first("_id") == Some(&BsonValue::Int32(id)))
        {
            matches_by_shard[shard] += 1;
        }
    }
    for work in scan_work {
        let shard = usize::from(work.shard());
        assert_eq!(work.documents_examined(), documents_by_shard[shard]);
        assert_eq!(work.source_matches(), matches_by_shard[shard]);
    }
    build(
        &engine,
        &session,
        &namespace,
        DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap(),
    )
    .await;
    let indexed = call(
        &engine,
        &session,
        command(
            &namespace,
            query.clone(),
            options().with_plan_diagnostics(true),
        ),
    )
    .await;
    assert!(stats(&indexed).documents_examined() < 14);
    assert_eq!(
        stats(&indexed).documents_examined(),
        stats(&indexed).matcher_evaluations()
    );
    assert_eq!(stats(&indexed).source_matches(), expected_matches);
    for work in stats(&indexed).shard_work() {
        assert_eq!(
            work.documents_examined(),
            matches_by_shard[usize::from(work.shard())]
        );
        assert_eq!(work.source_matches(), work.documents_examined());
    }
    assert_eq!(page(indexed).1, expected);
    let missing = call(
        &engine,
        &session,
        command(&namespace, doc([("a", BsonValue::Int32(999))]), options()),
    )
    .await;
    assert_eq!(stats(&missing).documents_examined(), 0);
    assert_eq!(stats(&missing).source_matches(), 0);
    assert_eq!(stats(&missing).storage_reads(), 4);
    assert_eq!(stats(&missing).shards_read().count(), 4);
    for id in [1, 999] {
        let point = call(
            &engine,
            &session,
            command(&namespace, doc([("_id", BsonValue::Int32(id))]), options()),
        )
        .await;
        let actual = stats(&point);
        assert_eq!(actual.storage_reads(), 1);
        assert_eq!(actual.documents_examined(), u64::from(id == 1));
        assert_eq!(actual.matcher_evaluations(), 0);
        assert_eq!(actual.source_matches(), u64::from(id == 1));
        assert_eq!(
            actual.shards_read().collect::<Vec<_>>(),
            point.plan().unwrap().shards()
        );
    }
    let subset = call(
        &engine,
        &session,
        command(
            &namespace,
            doc([(
                "_id",
                obj([(
                    "$in",
                    BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(2)]),
                )]),
            )]),
            options(),
        ),
    )
    .await;
    assert_eq!(
        stats(&subset).shards_read().collect::<Vec<_>>(),
        subset.plan().unwrap().shards()
    );
    assert!(stats(&subset).shards_read().count() <= 2);
    assert_eq!(stats(&subset).source_matches(), 2);
    assert_eq!(page(subset).1.len(), 2);
    drop_index(&engine, &session, &namespace).await;
    let scan = call(
        &engine,
        &session,
        command(&namespace, query.clone(), options()),
    )
    .await;
    assert_eq!(stats(&scan).documents_examined(), 14);
    assert_eq!(stats(&scan).source_matches(), expected_matches);
    let ordinary = call(
        &engine,
        &session,
        command(&namespace, query, DocumentReadOptions::new()),
    )
    .await;
    assert!(ordinary.read_stats().is_none());
    assert_eq!(page(ordinary).1, expected);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn read_stats_are_per_request_and_count_lookahead_and_blocking_source_work() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    let namespace = ns("stats_pages");
    seed(&engine, &session, &namespace, 7).await;
    let first = call(
        &engine,
        &session,
        command(&namespace, doc([]), options().with_batch_size(0).unwrap()),
    )
    .await;
    assert_eq!(stats(&first).storage_reads(), 0);
    assert_eq!(stats(&first).source_matches(), 0);
    assert_eq!(stats(&first).shards_read().count(), 0);
    let mut cursor = page(first).0;
    let mut count = 0;
    let mut pages = 0;
    while let Some(id) = cursor {
        let enabled = pages % 2 == 0;
        let next = call(
            &engine,
            &session,
            DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
                namespace.clone(),
                id,
                options()
                    .with_execution_stats(enabled)
                    .with_batch_size(1)
                    .unwrap(),
            )),
        )
        .await;
        if enabled {
            assert_eq!(stats(&next).storage_reads(), 3);
            assert!((1..=3).contains(&stats(&next).documents_examined()));
            assert_eq!(stats(&next).matcher_evaluations(), 0);
            assert_eq!(
                stats(&next).source_matches(),
                stats(&next).documents_examined()
            );
        } else {
            assert!(next.read_stats().is_none());
        }
        let (next_cursor, documents) = page(next);
        cursor = next_cursor;
        count += documents.len();
        pages += 1;
    }
    assert_eq!(count, 7);
    let pipeline =
        DocumentPipeline::new(vec![doc([("$sort", obj([("_id", BsonValue::Int32(1))]))])]).unwrap();
    let aggregate = call(
        &engine,
        &session,
        DocumentCommand::Aggregate(
            DocumentAggregateRequest::new(
                namespace.clone(),
                pipeline,
                options().with_batch_size(1).unwrap(),
            )
            .unwrap(),
        ),
    )
    .await;
    assert!(stats(&aggregate).documents_examined() >= 7);
    assert_eq!(stats(&aggregate).matcher_evaluations(), 0);
    assert_eq!(
        stats(&aggregate).source_matches(),
        stats(&aggregate).documents_examined()
    );
    let (cursor, _) = page(aggregate);
    let next = call(
        &engine,
        &session,
        DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
            namespace.clone(),
            cursor.unwrap(),
            options().with_batch_size(1).unwrap(),
        )),
    )
    .await;
    assert_eq!(
        stats(&next).storage_reads(),
        0,
        "buffered aggregate output is not a new source read"
    );
    assert_eq!(stats(&next).shards_read().count(), 0);
    assert_eq!(stats(&next).source_matches(), 0);
    let distinct = call(
        &engine,
        &session,
        DocumentCommand::Distinct(
            DocumentDistinctRequest::new(
                namespace.clone(),
                "a",
                DocumentFilter::empty(),
                options(),
            )
            .unwrap(),
        ),
    )
    .await;
    assert!(stats(&distinct).documents_examined() >= 7);
    assert_eq!(stats(&distinct).matcher_evaluations(), 0);
    assert_eq!(
        stats(&distinct).source_matches(),
        stats(&distinct).documents_examined()
    );
    let sorted = call(
        &engine,
        &session,
        command(
            &namespace,
            doc([]),
            options().with_sort(DocumentSort::new(doc([("_id", BsonValue::Int32(-1))])).unwrap()),
        ),
    )
    .await;
    // Seven rows are scanned for sort keys, then fetched again for output.
    assert_eq!(stats(&sorted).documents_examined(), 14);
    assert_eq!(stats(&sorted).storage_reads(), 16);
    assert_eq!(stats(&sorted).matcher_evaluations(), 0);
    assert_eq!(stats(&sorted).source_matches(), 14);
    let filtered_sorted = call(
        &engine,
        &session,
        command(
            &namespace,
            doc([("_id", obj([("$gte", BsonValue::Int32(0))]))]),
            options().with_sort(DocumentSort::new(doc([("_id", BsonValue::Int32(-1))])).unwrap()),
        ),
    )
    .await;
    assert_eq!(stats(&filtered_sorted).documents_examined(), 14);
    assert_eq!(stats(&filtered_sorted).storage_reads(), 16);
    assert_eq!(stats(&filtered_sorted).matcher_evaluations(), 14);
    assert_eq!(stats(&filtered_sorted).source_matches(), 14);
    assert_eq!(page(filtered_sorted).1, page(sorted).1);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn source_matches_precede_skip_and_pipeline_filters() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    let namespace = ns("stats_source_matches");
    seed(&engine, &session, &namespace, 7).await;
    let skipped = call(
        &engine,
        &session,
        command(
            &namespace,
            doc([("_id", BsonValue::Int32(1))]),
            options().with_skip(1),
        ),
    )
    .await;
    assert_eq!(stats(&skipped).source_matches(), 1);
    assert!(page(skipped).1.is_empty());
    let pipeline = DocumentPipeline::new(vec![
        doc([("$project", obj([("_id", BsonValue::Int32(1))]))]),
        doc([("$match", obj([("_id", BsonValue::Int32(1))]))]),
    ])
    .unwrap();
    let filtered = call(
        &engine,
        &session,
        DocumentCommand::Aggregate(
            DocumentAggregateRequest::new(namespace.clone(), pipeline, options()).unwrap(),
        ),
    )
    .await;
    assert_eq!(stats(&filtered).matcher_evaluations(), 0);
    assert_eq!(
        stats(&filtered).source_matches(),
        stats(&filtered).documents_examined()
    );
    assert!(stats(&filtered).source_matches() >= 7);
    assert_eq!(page(filtered).1.len(), 1);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn read_stats_charge_bounded_metadata_and_cleanup_failed_empty_pages() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    let namespace = ns("stats_budget");
    seed(&engine, &session, &namespace, 3).await;
    let base = 16
        + 8
        + namespace.database().len() as u64
        + 1
        + namespace.collection().len() as u64
        + 9
        + 8
        + 4;
    for _ in 0..12 {
        let request = DocumentRequest::new(
            DocumentRequestId::new([2; 16]).unwrap(),
            RequestContext::new().with_result_limits(ResultLimits::new(1, base + 2047).unwrap()),
            command(&namespace, doc([]), options().with_batch_size(0).unwrap()),
        );
        assert_eq!(
            engine
                .execute_document(&session, request)
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
    }
    let request = DocumentRequest::new(
        DocumentRequestId::new([3; 16]).unwrap(),
        RequestContext::new().with_result_limits(ResultLimits::new(1, base + 2048).unwrap()),
        command(&namespace, doc([]), options().with_batch_size(0).unwrap()),
    );
    assert_eq!(
        stats(&engine.execute_document(&session, request).await.unwrap()).storage_reads(),
        0
    );
    for aggregate in [false, true] {
        let result = call(
            &engine,
            &session,
            command(&namespace, doc([]), DocumentReadOptions::new()),
        )
        .await;
        let documents = page(result).1;
        let row = 17 + encode_document(&documents[0]).unwrap().len() as u64;
        let read = options().with_batch_byte_limit(base + 2048 + row).unwrap();
        let command = if aggregate {
            DocumentCommand::Aggregate(
                DocumentAggregateRequest::new(
                    namespace.clone(),
                    DocumentPipeline::new(Vec::new()).unwrap(),
                    read,
                )
                .unwrap(),
            )
        } else {
            command(&namespace, doc([]), read)
        };
        let result = call(&engine, &session, command).await;
        assert!(stats(&result).storage_reads() > 0);
        assert_eq!(page(result).1.len(), 1);
    }
    let token = CancellationToken::new();
    token.cancel();
    for (context, kind) in [
        (
            RequestContext::new().with_cancellation_token(token),
            EngineErrorKind::Cancelled,
        ),
        (
            RequestContext::new().with_deadline(Instant::now() - Duration::from_secs(1)),
            EngineErrorKind::DeadlineExceeded,
        ),
    ] {
        let result = engine
            .execute_document(
                &session,
                DocumentRequest::new(
                    DocumentRequestId::new([4; 16]).unwrap(),
                    context,
                    command(&namespace, doc([]), options()),
                ),
            )
            .await;
        assert_eq!(result.unwrap_err().kind(), kind);
    }
    assert_eq!(
        stats(&call(&engine, &session, command(&namespace, doc([]), options())).await)
            .documents_examined(),
        3
    );
    engine.shutdown().await.unwrap();
}
