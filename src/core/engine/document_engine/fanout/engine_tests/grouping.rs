use super::*;
use crate::document::{DocumentAggregateRequest, DocumentPipeline};

fn all() -> DocumentCommand {
    let doc = |entries| BsonDocument::from_entries(entries).unwrap();
    let pipeline = DocumentPipeline::new(vec![doc(vec![(
        "$group",
        BsonValue::Document(doc(vec![
            ("_id", BsonValue::Null),
            (
                "n",
                BsonValue::Document(doc(vec![("$sum", BsonValue::Int32(1))])),
            ),
            (
                "first",
                BsonValue::Document(doc(vec![("$first", BsonValue::from("$_id"))])),
            ),
            (
                "last",
                BsonValue::Document(doc(vec![("$last", BsonValue::from("$_id"))])),
            ),
        ])),
    )])])
    .unwrap();
    DocumentCommand::Aggregate(
        DocumentAggregateRequest::new(
            namespace(),
            pipeline,
            DocumentReadOptions::new().with_execution_stats(true),
        )
        .unwrap(),
    )
}

fn assert_group(execution: DocumentExecution) {
    assert_eq!(execution.read_stats().unwrap().shards_read().count(), 16);
    let DocumentResult::Cursor(batch) = execution.result() else {
        panic!("cursor")
    };
    assert!(batch.cursor_id().is_none());
    assert_eq!(batch.documents().len(), 1);
    let row = &batch.documents()[0];
    assert_eq!(row.get_first("n"), Some(&BsonValue::Int32(12)));
    assert_eq!(row.get_first("first"), Some(&BsonValue::Int32(0)));
    assert_eq!(row.get_first("last"), Some(&BsonValue::Int32(11)));
}

#[tokio::test]
async fn partial_groups_use_two_bounded_waves_and_merge_only_after_every_shard() {
    let (_root, engine, session) = setup().await;
    let mut permits = block(&engine).await;
    let task = query_command(&engine, &session, all(), RequestContext::new());
    wait_for(&engine, |pool| {
        pool.shards[..8].iter().all(|shard| shard.queued == 1)
    })
    .await;
    assert!(
        engine.pool_snapshot_for_test().unwrap().shards[8..]
            .iter()
            .all(|shard| shard.queued == 0)
    );
    for permit in &mut permits[..8] {
        permit.take();
    }
    wait_for(&engine, |pool| {
        pool.shards[8..].iter().all(|shard| shard.queued == 1)
    })
    .await;
    assert!(!task.is_finished());
    // Release in reverse order to separate completion order from natural order.
    for permit in permits.iter_mut().rev() {
        permit.take();
    }
    assert_group(bounded(task).await.unwrap().unwrap());
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
async fn partial_groups_drain_failure_cancellation_and_caller_abort_before_reuse() {
    assert_failure_cleanup(all, assert_group).await;
}
