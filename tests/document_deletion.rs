#![cfg(feature = "documents")]

use briskdb::{
    core::{CancellationToken, Engine, EngineErrorKind, RequestContext, ResultLimits, Session},
    document::{
        BsonDocument, BsonValue, DocumentCollectionOptions, DocumentCommand,
        DocumentCreateCollectionRequest, DocumentDeleteRequest, DocumentFilter,
        DocumentFindRequest, DocumentInsertRequest, DocumentMutationScope, DocumentNamespace,
        DocumentPlan, DocumentReadOptions, DocumentRequest, DocumentRequestId, DocumentResult,
        DocumentWriteOptions,
    },
};
use rusqlite::{Connection, TransactionBehavior};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}
fn ns() -> DocumentNamespace {
    DocumentNamespace::new("app", "items").unwrap()
}
fn request(command: DocumentCommand, context: RequestContext) -> DocumentRequest {
    DocumentRequest::new(DocumentRequestId::new([1; 16]).unwrap(), context, command)
}
fn delete(filter: BsonDocument, scope: DocumentMutationScope) -> DocumentCommand {
    DocumentCommand::Delete(DocumentDeleteRequest::new(
        ns(),
        DocumentFilter::new(filter).unwrap(),
        scope,
        DocumentWriteOptions::new(),
    ))
}
async fn seed(engine: &Engine, session: &Session) {
    engine
        .execute_document(
            session,
            request(
                DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                    ns(),
                    DocumentCollectionOptions::empty(),
                    DocumentWriteOptions::new(),
                )),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    let rows: Vec<_> = (0..80)
        .rev()
        .map(|i| {
            doc([
                ("_id", BsonValue::Int32(i)),
                ("group", BsonValue::Int32(i % 2)),
            ])
        })
        .collect();
    engine
        .execute_document(
            session,
            request(
                DocumentCommand::Insert(
                    DocumentInsertRequest::new(ns(), rows, DocumentWriteOptions::new()).unwrap(),
                ),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
}
async fn ids(engine: &Engine, session: &Session) -> Vec<i32> {
    let result = engine
        .execute_document(
            session,
            request(
                DocumentCommand::Find(DocumentFindRequest::new(
                    ns(),
                    DocumentFilter::new(BsonDocument::new()).unwrap(),
                    DocumentReadOptions::new(),
                )),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    let DocumentResult::Cursor(batch) = result.result() else {
        panic!("cursor")
    };
    assert!(batch.cursor_id().is_none());
    batch
        .documents()
        .iter()
        .map(|row| match row.get_first("_id").unwrap() {
            BsonValue::Int32(i) => *i,
            _ => panic!("id"),
        })
        .collect()
}
async fn deleted(
    engine: &Engine,
    session: &Session,
    filter: BsonDocument,
    scope: DocumentMutationScope,
    point: bool,
) -> u64 {
    let result = engine
        .execute_document(
            session,
            request(delete(filter, scope), RequestContext::new()),
        )
        .await
        .unwrap();
    assert_eq!(matches!(result.plan(), Some(DocumentPlan::Point(_))), point);
    let DocumentResult::Delete(result) = result.result() else {
        panic!("delete")
    };
    assert!(result.acknowledged());
    result.deleted_count()
}

#[tokio::test]
async fn filtered_deletes_keep_global_natural_order_counts_point_route_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    assert_eq!(
        deleted(
            &engine,
            &session,
            doc([("group", BsonValue::Int32(0))]),
            DocumentMutationScope::One,
            false
        )
        .await,
        1
    );
    assert_eq!(
        ids(&engine, &session).await,
        (0..80).rev().filter(|i| *i != 78).collect::<Vec<_>>()
    );
    assert_eq!(
        deleted(
            &engine,
            &session,
            doc([("group", BsonValue::Int32(0))]),
            DocumentMutationScope::Many,
            false
        )
        .await,
        39
    );
    assert_eq!(
        deleted(
            &engine,
            &session,
            doc([(
                "_id",
                BsonValue::Document(doc([("$eq", BsonValue::Double(79.0))]))
            )]),
            DocumentMutationScope::Many,
            true
        )
        .await,
        1
    );
    assert_eq!(
        deleted(
            &engine,
            &session,
            BsonDocument::new(),
            DocumentMutationScope::One,
            false
        )
        .await,
        1
    );
    assert_eq!(
        deleted(
            &engine,
            &session,
            doc([("group", BsonValue::Int32(42))]),
            DocumentMutationScope::Many,
            false
        )
        .await,
        0
    );
    assert_eq!(
        ids(&engine, &session).await,
        (0..76).rev().filter(|i| i % 2 == 1).collect::<Vec<_>>()
    );
    drop(session);
    engine.shutdown().await.unwrap();
    drop(engine);
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    assert_eq!(
        deleted(
            &engine,
            &session,
            BsonDocument::new(),
            DocumentMutationScope::Many,
            false
        )
        .await,
        38
    );
    assert!(ids(&engine, &session).await.is_empty());
    assert_eq!(
        deleted(
            &engine,
            &session,
            BsonDocument::new(),
            DocumentMutationScope::One,
            false
        )
        .await,
        0
    );
}

#[tokio::test]
async fn delete_preflight_controls_filters_and_delivery_budgets_never_mutate() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    for scope in [DocumentMutationScope::One, DocumentMutationScope::Many] {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        for (context, kind) in [
            (
                RequestContext::new().with_result_limits(ResultLimits::new(1, 1).unwrap()),
                EngineErrorKind::LimitExceeded,
            ),
            (
                RequestContext::new().with_cancellation_token(cancellation),
                EngineErrorKind::Cancelled,
            ),
            (
                RequestContext::new().with_deadline(Instant::now() - Duration::from_millis(1)),
                EngineErrorKind::DeadlineExceeded,
            ),
        ] {
            assert_eq!(
                engine
                    .execute_document(
                        &session,
                        request(delete(BsonDocument::new(), scope), context)
                    )
                    .await
                    .unwrap_err()
                    .kind(),
                kind
            );
        }
        assert_eq!(
            engine
                .execute_document(
                    &session,
                    request(
                        delete(doc([("$where", BsonValue::from("secret"))]), scope),
                        RequestContext::new()
                    )
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
    }
    assert_eq!(ids(&engine, &session).await.len(), 80);
}

#[tokio::test]
async fn many_rolls_back_current_shard_on_corruption_but_keeps_earlier_commits() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let shards: Vec<_> = (0..4)
        .map(|shard| {
            Connection::open(root.path().join(format!("shards/{shard:04}.sqlite"))).unwrap()
        })
        .collect();
    let count = |connection: &Connection| {
        connection
            .query_row("SELECT count(*) FROM briskdb_documents_v1", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap()
    };
    let before = count(&shards[3]);
    assert!(before > 1);
    shards[3].execute("UPDATE briskdb_documents_v1 SET document_checksum = zeroblob(32) WHERE natural_order = (SELECT max(natural_order) FROM briskdb_documents_v1)", []).unwrap();
    assert_eq!(
        engine
            .execute_document(
                &session,
                request(
                    delete(BsonDocument::new(), DocumentMutationScope::Many),
                    RequestContext::new()
                )
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::DataCorruption
    );
    assert!(shards[..3].iter().all(|connection| count(connection) == 0));
    assert_eq!(
        count(&shards[3]),
        before,
        "all deletes in the failed shard must roll back"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_after_first_shard_commit_releases_leases_without_global_rollback() {
    for abort in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), 2).await.unwrap();
        let session = Arc::new(engine.session());
        seed(&engine, &session).await;
        let first = Connection::open(root.path().join("shards/0000.sqlite")).unwrap();
        let mut second = Connection::open(root.path().join("shards/0001.sqlite")).unwrap();
        let blocker = second
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let count = |connection: &Connection| {
            connection
                .query_row("SELECT count(*) FROM briskdb_documents_v1", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap()
        };
        let remaining = count(&blocker);
        assert!(remaining > 0);
        let cancellation = CancellationToken::new();
        let task_engine = engine.clone();
        let task_session = Arc::clone(&session);
        let context = RequestContext::new().with_cancellation_token(cancellation.clone());
        let task = tokio::spawn(async move {
            task_engine
                .execute_document(
                    &task_session,
                    request(
                        delete(BsonDocument::new(), DocumentMutationScope::Many),
                        context,
                    ),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while count(&first) != 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        if abort {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            cancellation.cancel();
            assert_eq!(
                task.await.unwrap().unwrap_err().kind(),
                EngineErrorKind::Cancelled
            );
        }
        assert_eq!(count(&blocker), remaining);
        blocker.rollback().unwrap();
        assert_eq!(ids(&engine, &session).await.len() as i64, remaining);
        assert_eq!(
            deleted(
                &engine,
                &session,
                BsonDocument::new(),
                DocumentMutationScope::Many,
                false
            )
            .await,
            remaining as u64
        );
    }
}
