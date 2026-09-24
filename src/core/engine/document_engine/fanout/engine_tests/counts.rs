use super::*;
use crate::document::DocumentCountRequest;
use std::collections::BTreeSet;

fn count(filter: DocumentFilter, options: DocumentReadOptions) -> DocumentCommand {
    DocumentCommand::Count(DocumentCountRequest::new(namespace(), filter, options))
}
fn all() -> DocumentCommand {
    count(DocumentFilter::empty(), DocumentReadOptions::new())
}
fn assert_count(execution: DocumentExecution, expected: u64) {
    assert!(matches!(execution.result(), DocumentResult::Count(n) if *n == expected));
    assert!(execution.read_stats().is_none());
}
fn filter(value: BsonValue) -> DocumentFilter {
    DocumentFilter::new(BsonDocument::from_entries([("_id", value)]).unwrap()).unwrap()
}

#[tokio::test]
async fn plain_and_filtered_counts_admit_two_bounded_waves_before_global_skip_limit() {
    let (_root, engine, session) = setup().await;
    let filtered = filter(BsonValue::Document(
        BsonDocument::from_entries([("$gte", BsonValue::Int32(6))]).unwrap(),
    ));
    for (filter, expected) in [(DocumentFilter::empty(), 8), (filtered, 4)] {
        let mut permits = block(&engine).await;
        let task = query_command(
            &engine,
            &session,
            count(
                filter,
                DocumentReadOptions::new()
                    .with_skip(2)
                    .with_limit(8)
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
        for permit in &mut permits[..8] {
            permit.take();
        }
        wait_for(&engine, |pool| {
            pool.shards[8..].iter().all(|shard| shard.queued == 1)
        })
        .await;
        assert!(!task.is_finished());
        drop(permits);
        let result = bounded(task).await.unwrap().unwrap();
        assert_eq!(
            result.plan().unwrap().shards(),
            &(0..16).collect::<Vec<_>>()
        );
        assert_count(result, expected);
        assert!(
            engine
                .pool_snapshot_for_test()
                .unwrap()
                .shards
                .iter()
                .all(|shard| shard.active == 0 && shard.queued == 0)
        );
    }
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn subset_and_point_counts_never_wait_for_unrelated_shards() {
    let (_root, engine, session) = setup().await;
    let ids = [0, 5, 11];
    let owners = ids
        .map(|id| {
            engine
                .inner
                .database
                .storage
                .prepare_document_id(&BsonValue::Int32(id))
                .unwrap()
                .1
        })
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert!(owners.len() <= 3);
    let mut permits = block(&engine).await;
    let task = query_command(
        &engine,
        &session,
        count(
            filter(BsonValue::Document(
                BsonDocument::from_entries([(
                    "$in",
                    BsonValue::Array(ids.into_iter().map(BsonValue::Int32).collect()),
                )])
                .unwrap(),
            )),
            DocumentReadOptions::new()
                .with_skip(1)
                .with_limit(1)
                .unwrap(),
        ),
        RequestContext::new(),
    );
    wait_for(&engine, |pool| {
        owners
            .iter()
            .all(|shard| pool.shards[usize::from(*shard)].queued == 1)
    })
    .await;
    assert!(
        engine
            .pool_snapshot_for_test()
            .unwrap()
            .shards
            .iter()
            .all(|shard| { owners.contains(&shard.shard) || shard.queued == 0 })
    );
    for owner in &owners {
        permits[usize::from(*owner)].take();
    }
    let result = bounded(task).await.unwrap().unwrap();
    assert_eq!(
        result.plan().unwrap().shards(),
        &owners.into_iter().collect::<Vec<_>>()
    );
    assert_count(result, 1);
    let result = bounded(query_command(
        &engine,
        &session,
        count(filter(BsonValue::Int32(5)), DocumentReadOptions::new()),
        RequestContext::new(),
    ))
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(result.plan(), Some(DocumentPlan::Point(_))));
    assert_count(result, 1);
    drop(permits);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn counts_drain_failure_cancellation_and_caller_abort_without_poisoning_shared_controls() {
    assert_failure_cleanup(all, |result| assert_count(result, 12)).await;
}
