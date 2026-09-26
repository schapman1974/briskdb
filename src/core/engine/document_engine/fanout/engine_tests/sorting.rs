use super::*;
use crate::document::{DocumentContinueCursorRequest, DocumentSort};

fn sorted(options: DocumentReadOptions) -> DocumentCommand {
    DocumentCommand::Find(DocumentFindRequest::new(
        namespace(),
        DocumentFilter::empty(),
        options.with_sort(
            DocumentSort::new(BsonDocument::from_entries([("_id", BsonValue::Int32(-1))]).unwrap())
                .unwrap(),
        ),
    ))
}

fn all() -> DocumentCommand {
    sorted(DocumentReadOptions::new().with_execution_stats(true))
}

fn rows(execution: &DocumentExecution) -> Vec<BsonValue> {
    let DocumentResult::Cursor(batch) = execution.result() else {
        panic!("cursor")
    };
    batch
        .documents()
        .iter()
        .map(|document| document.get_first("_id").unwrap().clone())
        .collect()
}

#[tokio::test]
async fn invalid_sorted_shard_cancels_blocked_peers_and_keeps_the_parent_usable() {
    let (_root, engine, session) = setup().await;
    let bad_id = (100..10000)
        .find(|id| {
            engine
                .inner
                .database
                .storage
                .prepare_document_id(&BsonValue::Int32(*id))
                .unwrap()
                .1
                == 0
        })
        .unwrap();
    let document = BsonDocument::from_entries([
        ("_id", BsonValue::Int32(bad_id)),
        (
            "a",
            BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(2)]),
        ),
        (
            "b",
            BsonValue::Array(vec![BsonValue::Int32(3), BsonValue::Int32(4)]),
        ),
    ])
    .unwrap();
    engine
        .execute_document(
            &session,
            request(
                DocumentCommand::Insert(
                    DocumentInsertRequest::new(
                        namespace(),
                        vec![document],
                        DocumentWriteOptions::new(),
                    )
                    .unwrap(),
                ),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    let mut permits = block(&engine).await;
    let parent = CancellationToken::new();
    let command = DocumentCommand::Find(DocumentFindRequest::new(
        namespace(),
        DocumentFilter::empty(),
        DocumentReadOptions::new().with_sort(
            DocumentSort::new(
                BsonDocument::from_entries([
                    ("a", BsonValue::Int32(1)),
                    ("b", BsonValue::Int32(1)),
                ])
                .unwrap(),
            )
            .unwrap(),
        ),
    ));
    let task = query_command(
        &engine,
        &session,
        command,
        RequestContext::new().with_cancellation_token(parent.clone()),
    );
    wait_for(&engine, |pool| {
        pool.shards[..8].iter().all(|shard| shard.queued == 1)
    })
    .await;
    permits[0].take();
    assert_eq!(
        bounded(task).await.unwrap().unwrap_err().kind(),
        EngineErrorKind::InvalidQuery
    );
    assert!(!parent.is_cancelled());
    assert!(
        engine
            .pool_snapshot_for_test()
            .unwrap()
            .shards
            .iter()
            .all(|shard| shard.queued == 0)
    );
    assert_eq!(engine.readiness().active_schema_operations(), 0);
    drop(permits);
    let result = bounded(query_command(
        &engine,
        &session,
        all(),
        RequestContext::new().with_cancellation_token(parent),
    ))
    .await
    .unwrap()
    .unwrap();
    assert_eq!(rows(&result).len(), 13);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn sorted_scans_admit_bounded_waves_before_global_skip_limit() {
    let (_root, engine, session) = setup().await;
    let mut permits = block(&engine).await;
    let task = query_command(
        &engine,
        &session,
        sorted(
            DocumentReadOptions::new()
                .with_execution_stats(true)
                .with_skip(2)
                .with_limit(5)
                .unwrap(),
        ),
        RequestContext::new(),
    );
    wait_for(&engine, |pool| {
        pool.shards[..8].iter().all(|shard| shard.queued == 1)
    })
    .await;
    assert!(
        engine.pool_snapshot_for_test().unwrap().shards[8..]
            .iter()
            .all(|shard| shard.queued == 0)
    );
    assert!(!task.is_finished());
    for permit in permits[..8].iter_mut().rev() {
        permit.take();
        tokio::task::yield_now().await;
    }
    wait_for(&engine, |pool| {
        pool.shards[8..].iter().all(|shard| shard.queued == 1)
    })
    .await;
    assert!(
        !task.is_finished(),
        "a slow shard cannot yield a partial page"
    );
    drop(permits);
    let execution = bounded(task).await.unwrap().unwrap();
    assert_eq!(execution.read_stats().unwrap().shards_read().count(), 16);
    assert_eq!(
        rows(&execution),
        (5..10).rev().map(BsonValue::Int32).collect::<Vec<_>>()
    );
    assert!(
        matches!(execution.result(), DocumentResult::Cursor(batch) if batch.cursor_id().is_none())
    );
    assert!(
        engine
            .pool_snapshot_for_test()
            .unwrap()
            .shards
            .iter()
            .all(|shard| shard.active == 0 && shard.queued == 0)
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn sorted_failure_cancellation_and_caller_abort_drain_before_reuse() {
    assert_failure_cleanup(all, |execution| {
        assert_eq!(execution.read_stats().unwrap().shards_read().count(), 16);
        assert_eq!(
            rows(&execution),
            (0..12).rev().map(BsonValue::Int32).collect::<Vec<_>>()
        );
    })
    .await;
}

#[tokio::test]
async fn sorted_deadline_cancels_queued_peers_without_partial_results() {
    let (_root, engine, session) = setup().await;
    let permits = block(&engine).await;
    let parent = CancellationToken::new();
    let task = query_command(
        &engine,
        &session,
        all(),
        RequestContext::new()
            .with_cancellation_token(parent.clone())
            .with_deadline(Instant::now() + Duration::from_secs(1)),
    );
    wait_for(&engine, |pool| {
        pool.shards[..8].iter().all(|shard| shard.queued == 1)
    })
    .await;
    assert_eq!(
        bounded(task).await.unwrap().unwrap_err().kind(),
        EngineErrorKind::DeadlineExceeded
    );
    assert!(!parent.is_cancelled());
    assert!(
        engine
            .pool_snapshot_for_test()
            .unwrap()
            .shards
            .iter()
            .all(|shard| shard.queued == 0)
    );
    assert_eq!(engine.readiness().active_schema_operations(), 0);
    drop(permits);
    let execution = bounded(query_command(
        &engine,
        &session,
        all(),
        RequestContext::new().with_cancellation_token(parent),
    ))
    .await
    .unwrap()
    .unwrap();
    assert_eq!(rows(&execution).len(), 12);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn sorted_continuations_cross_empty_uneven_shards_without_duplicates_or_leases() {
    let (_root, engine, session) = setup().await;
    let mut command = sorted(
        DocumentReadOptions::new()
            .with_skip(2)
            .with_limit(7)
            .unwrap()
            .with_batch_size(3)
            .unwrap(),
    );
    let mut actual = Vec::new();
    loop {
        let execution = bounded(query_command(
            &engine,
            &session,
            command,
            RequestContext::new(),
        ))
        .await
        .unwrap()
        .unwrap();
        actual.extend(rows(&execution));
        assert!(
            engine
                .pool_snapshot_for_test()
                .unwrap()
                .shards
                .iter()
                .all(|shard| shard.active == 0 && shard.queued == 0)
        );
        let DocumentResult::Cursor(batch) = execution.result() else {
            panic!("cursor")
        };
        let Some(id) = batch.cursor_id() else { break };
        command = DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
            namespace(),
            id,
            DocumentReadOptions::new().with_batch_size(2).unwrap(),
        ));
    }
    assert_eq!(
        actual,
        (3..10).rev().map(BsonValue::Int32).collect::<Vec<_>>()
    );
    engine.shutdown().await.unwrap();
}
