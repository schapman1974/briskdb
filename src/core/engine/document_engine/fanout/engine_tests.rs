use super::*;
use crate::core::{EngineOptions, RequestContext};
use crate::document::{
    DocumentCreateCollectionRequest, DocumentFindRequest, DocumentInsertRequest, DocumentRequestId,
};
use std::time::Duration;

fn namespace() -> DocumentNamespace {
    DocumentNamespace::new("frontier", "items").unwrap()
}
fn find() -> DocumentCommand {
    DocumentCommand::Find(DocumentFindRequest::new(
        namespace(),
        DocumentFilter::empty(),
        DocumentReadOptions::new().with_execution_stats(true),
    ))
}
fn request(command: DocumentCommand, context: RequestContext) -> DocumentRequest {
    DocumentRequest::new(DocumentRequestId::new([1; 16]).unwrap(), context, command)
}
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .unwrap()
}
async fn wait_for(
    engine: &Engine,
    predicate: impl Fn(&crate::storage::pool::PoolSnapshot) -> bool,
) {
    bounded(async {
        loop {
            if predicate(&engine.pool_snapshot_for_test().unwrap()) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
}
fn query(
    engine: &Engine,
    session: &Arc<Session>,
    context: RequestContext,
) -> tokio::task::JoinHandle<EngineResult<DocumentExecution>> {
    let engine = engine.clone();
    let session = Arc::clone(session);
    tokio::spawn(async move {
        engine
            .execute_document(&session, request(find(), context))
            .await
    })
}
fn assert_rows(execution: DocumentExecution) {
    assert_eq!(execution.read_stats().unwrap().shards_read().count(), 16);
    let DocumentResult::Cursor(batch) = execution.result() else {
        panic!("cursor")
    };
    assert!(batch.cursor_id().is_none());
    let ids: Vec<_> = batch
        .documents()
        .iter()
        .map(|document| document.get_first("_id").unwrap().clone())
        .collect();
    assert_eq!(ids, (0..12).map(BsonValue::Int32).collect::<Vec<_>>());
}
async fn setup() -> (tempfile::TempDir, Engine, Arc<Session>) {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open_with_options(root.path(), 16, EngineOptions::new(1, 1).unwrap())
        .await
        .unwrap();
    let session = Arc::new(engine.session());
    engine
        .execute_document(
            &session,
            request(
                DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                    namespace(),
                    DocumentCollectionOptions::empty(),
                    DocumentWriteOptions::new(),
                )),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    engine
        .execute_document(
            &session,
            request(
                DocumentCommand::Insert(
                    DocumentInsertRequest::new(
                        namespace(),
                        (0..12)
                            .map(|id| {
                                BsonDocument::from_entries([("_id", BsonValue::Int32(id))]).unwrap()
                            })
                            .collect::<Vec<_>>(),
                        DocumentWriteOptions::new(),
                    )
                    .unwrap(),
                ),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    (root, engine, session)
}
async fn block(engine: &Engine) -> Vec<Option<crate::storage::pool::PoolPermit>> {
    let mut permits = Vec::new();
    for shard in 0..16 {
        permits.push(Some(
            engine
                .inner
                .connections
                .acquire_for_owner(shard, ConnectionOwner::new(u64::MAX))
                .await
                .unwrap(),
        ));
    }
    permits
}

#[tokio::test]
async fn real_document_frontiers_admit_two_bounded_waves_and_merge_empty_uneven_shards() {
    let (_root, engine, session) = setup().await;
    let mut permits = block(&engine).await;
    let task = query(&engine, &session, RequestContext::new());
    wait_for(&engine, |pool| {
        pool.shards[..8].iter().all(|shard| shard.queued == 1)
    })
    .await;
    let snapshot = engine.pool_snapshot_for_test().unwrap();
    assert!(snapshot.shards[8..].iter().all(|shard| shard.queued == 0));
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
    assert_rows(bounded(task).await.unwrap().unwrap());
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
async fn real_frontier_failure_cancel_and_caller_abort_drain_without_poisoning_shared_controls() {
    let (_root, engine, session) = setup().await;
    let permits = block(&engine).await;
    let pools = engine.inner.connections.clone();
    let queued = tokio::spawn(async move {
        pools
            .acquire_for_owner(0, ConnectionOwner::new(u64::MAX - 1))
            .await
    });
    wait_for(&engine, |pool| pool.shards[0].queued == 1).await;
    let shared = CancellationToken::new();
    let error = bounded(query(
        &engine,
        &session,
        RequestContext::new().with_cancellation_token(shared.clone()),
    ))
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    assert!(!shared.is_cancelled());
    assert!(
        engine.pool_snapshot_for_test().unwrap().shards[1..]
            .iter()
            .all(|shard| shard.queued == 0)
    );
    assert_eq!(engine.readiness().active_schema_operations(), 0);
    queued.abort();
    assert!(bounded(queued).await.unwrap_err().is_cancelled());
    drop(permits);
    assert_rows(
        bounded(query(
            &engine,
            &session,
            RequestContext::new().with_cancellation_token(shared),
        ))
        .await
        .unwrap()
        .unwrap(),
    );

    for abort in [false, true] {
        let permits = block(&engine).await;
        let parent = CancellationToken::new();
        let task = query(
            &engine,
            &session,
            RequestContext::new().with_cancellation_token(parent.clone()),
        );
        wait_for(&engine, |pool| {
            pool.shards[..8].iter().all(|shard| shard.queued == 1)
        })
        .await;
        if abort {
            task.abort();
            assert!(bounded(task).await.unwrap_err().is_cancelled());
        } else {
            parent.cancel();
            assert_eq!(
                bounded(task).await.unwrap().unwrap_err().kind(),
                EngineErrorKind::Cancelled
            );
        }
        wait_for(&engine, |pool| {
            pool.shards.iter().all(|shard| shard.queued == 0)
        })
        .await;
        bounded(async {
            while engine.readiness().active_schema_operations() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        drop(permits);
        assert_rows(
            bounded(query(&engine, &session, RequestContext::new()))
                .await
                .unwrap()
                .unwrap(),
        );
    }
    engine.shutdown().await.unwrap();
}
