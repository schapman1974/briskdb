#![cfg(feature = "documents")]

use briskdb::{
    core::{CancellationToken, Engine, EngineErrorKind, RequestContext, ResultLimits, Session},
    document::{
        BsonDocument, BsonValue, DocumentCollectionOptions, DocumentCommand,
        DocumentCreateCollectionRequest, DocumentDeleteRequest, DocumentFilter,
        DocumentFindOneAndDeleteRequest, DocumentFindRequest, DocumentInsertRequest,
        DocumentMutationScope, DocumentNamespace, DocumentPlan, DocumentProjection,
        DocumentReadOptions, DocumentRequest, DocumentRequestId, DocumentResult, DocumentSort,
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

fn find_delete(filter: BsonDocument, options: DocumentReadOptions) -> DocumentCommand {
    DocumentCommand::FindOneAndDelete(DocumentFindOneAndDeleteRequest::new(
        ns(),
        DocumentFilter::new(filter).unwrap(),
        options,
    ))
}

#[tokio::test]
async fn find_delete_rejects_parallel_array_sort_before_point_or_scatter_mutation() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let row = doc([
        ("_id", BsonValue::Int32(100)),
        ("a", BsonValue::Array(vec![])),
        ("b", BsonValue::Array(vec![])),
    ]);
    engine
        .execute_document(
            &session,
            request(
                DocumentCommand::Insert(
                    DocumentInsertRequest::new(ns(), vec![row], DocumentWriteOptions::new())
                        .unwrap(),
                ),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    for filter in [
        doc([("_id", BsonValue::Int32(100))]),
        doc([(
            "a",
            BsonValue::Document(doc([("$exists", BsonValue::Boolean(true))])),
        )]),
    ] {
        let options = DocumentReadOptions::new().with_sort(
            DocumentSort::new(doc([
                ("a", BsonValue::Int32(1)),
                ("b", BsonValue::Int32(1)),
            ]))
            .unwrap(),
        );
        assert!(
            engine
                .execute_document(
                    &session,
                    request(find_delete(filter, options), RequestContext::new())
                )
                .await
                .is_err()
        );
    }
    assert!(
        removed(
            &engine,
            &session,
            doc([("_id", BsonValue::Int32(100))]),
            DocumentReadOptions::new()
        )
        .await
        .is_some()
    );
    assert_eq!(ids(&engine, &session).await.len(), 80);
}

#[tokio::test]
async fn find_delete_envelope_depth_rejection_preserves_record_and_storage_health() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let mut deep = BsonDocument::new();
    for _ in 1..briskdb::document::BSON_MAX_NESTING_DEPTH {
        deep = doc([("nested", BsonValue::Document(deep))]);
    }
    deep.push("_id", BsonValue::Int32(100)).unwrap();
    engine
        .execute_document(
            &session,
            request(
                DocumentCommand::Insert(
                    DocumentInsertRequest::new(ns(), vec![deep], DocumentWriteOptions::new())
                        .unwrap(),
                ),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    let filter = doc([("_id", BsonValue::Int32(100))]);
    let error = engine
        .execute_document(
            &session,
            request(
                find_delete(filter.clone(), DocumentReadOptions::new()),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    let options = DocumentReadOptions::new()
        .with_projection(DocumentProjection::new(doc([("_id", BsonValue::Int32(1))])).unwrap());
    assert_eq!(
        removed(&engine, &session, filter, options).await,
        Some(doc([("_id", BsonValue::Int32(100))]))
    );
    assert_eq!(ids(&engine, &session).await.len(), 80);
}

async fn removed(
    engine: &Engine,
    session: &Session,
    filter: BsonDocument,
    options: DocumentReadOptions,
) -> Option<BsonDocument> {
    let result = engine
        .execute_document(
            session,
            request(find_delete(filter, options), RequestContext::new()),
        )
        .await
        .unwrap();
    let DocumentResult::Document(document) = result.into_parts().2 else {
        panic!("document")
    };
    document
}

#[tokio::test]
async fn find_delete_sorts_before_projection_keeps_natural_ties_and_reopens() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let projected = DocumentReadOptions::new()
        .with_sort(DocumentSort::new(doc([("_id", BsonValue::Int32(1))])).unwrap())
        .with_projection(
            DocumentProjection::new(doc([
                ("group", BsonValue::Int32(1)),
                ("_id", BsonValue::Int32(0)),
            ]))
            .unwrap(),
        );
    assert_eq!(
        removed(
            &engine,
            &session,
            doc([("group", BsonValue::Int32(0))]),
            projected
        )
        .await,
        Some(doc([("group", BsonValue::Int32(0))]))
    );
    assert!(!ids(&engine, &session).await.contains(&0));
    let point = engine
        .execute_document(
            &session,
            request(
                find_delete(
                    doc([("_id", BsonValue::Double(79.0))]),
                    DocumentReadOptions::new(),
                ),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    assert!(matches!(point.plan(), Some(DocumentPlan::Point(_))));
    assert_eq!(
        point.result(),
        &DocumentResult::Document(Some(doc([
            ("_id", BsonValue::Int32(79)),
            ("group", BsonValue::Int32(1))
        ])))
    );
    // All remaining group=0 rows tie; durable insertion order wins.
    let tied = DocumentReadOptions::new()
        .with_sort(DocumentSort::new(doc([("group", BsonValue::Int32(1))])).unwrap());
    assert_eq!(
        removed(&engine, &session, BsonDocument::new(), tied)
            .await
            .unwrap()
            .get_first("_id"),
        Some(&BsonValue::Int32(78))
    );
    assert!(
        removed(
            &engine,
            &session,
            doc([("group", BsonValue::Int32(3))]),
            DocumentReadOptions::new()
        )
        .await
        .is_none()
    );
    drop(session);
    engine.shutdown().await.unwrap();
    drop(engine);
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    assert_eq!(ids(&engine, &session).await.len(), 77);
    assert_eq!(
        removed(
            &engine,
            &session,
            BsonDocument::new(),
            DocumentReadOptions::new()
        )
        .await
        .unwrap()
        .get_first("_id"),
        Some(&BsonValue::Int32(77))
    );
}

#[tokio::test]
async fn find_delete_preflights_exact_output_and_invalid_semantics_without_mutation() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let large = doc([
        ("_id", BsonValue::Int32(100)),
        ("payload", BsonValue::from("x".repeat(600 * 1024))),
    ]);
    engine
        .execute_document(
            &session,
            request(
                DocumentCommand::Insert(
                    DocumentInsertRequest::new(ns(), vec![large], DocumentWriteOptions::new())
                        .unwrap(),
                ),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    let filter = doc([("_id", BsonValue::Int32(100))]);
    let budget = RequestContext::new().with_result_limits(ResultLimits::new(1, 128).unwrap());
    assert_eq!(
        engine
            .execute_document(
                &session,
                request(
                    find_delete(filter.clone(), DocumentReadOptions::new()),
                    budget.clone()
                )
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    for options in [
        DocumentReadOptions::new()
            .with_sort(DocumentSort::new(doc([("group", BsonValue::Int32(0))])).unwrap()),
        DocumentReadOptions::new().with_projection(
            DocumentProjection::new(doc([
                ("group", BsonValue::Int32(1)),
                ("payload", BsonValue::Int32(0)),
            ]))
            .unwrap(),
        ),
        DocumentReadOptions::new().with_skip(1),
    ] {
        assert!(
            engine
                .execute_document(
                    &session,
                    request(
                        find_delete(BsonDocument::new(), options),
                        RequestContext::new()
                    )
                )
                .await
                .is_err()
        );
    }
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert_eq!(
        engine
            .execute_document(
                &session,
                request(
                    find_delete(BsonDocument::new(), DocumentReadOptions::new()),
                    RequestContext::new().with_cancellation_token(cancellation)
                )
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::Cancelled
    );
    let projection = DocumentReadOptions::new()
        .with_projection(DocumentProjection::new(doc([("_id", BsonValue::Int32(1))])).unwrap());
    let execution = engine
        .execute_document(&session, request(find_delete(filter, projection), budget))
        .await
        .unwrap();
    assert_eq!(
        execution.result(),
        &DocumentResult::Document(Some(doc([("_id", BsonValue::Int32(100))])))
    );
    assert_eq!(ids(&engine, &session).await.len(), 80);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_find_deletes_return_each_preimage_exactly_once() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let mut workers = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let engine = engine.clone();
        workers.spawn(async move {
            let session = engine.session();
            let mut ids = Vec::new();
            let options = DocumentReadOptions::new()
                .with_sort(DocumentSort::new(doc([("_id", BsonValue::Int32(1))])).unwrap());
            while let Some(row) =
                removed(&engine, &session, BsonDocument::new(), options.clone()).await
            {
                let Some(BsonValue::Int32(id)) = row.get_first("_id") else {
                    panic!("id")
                };
                ids.push(*id);
            }
            ids
        });
    }
    let mut returned = Vec::new();
    tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(worker) = workers.join_next().await {
            returned.extend(worker.unwrap());
        }
    })
    .await
    .unwrap();
    returned.sort_unstable();
    assert_eq!(returned, (0..80).collect::<Vec<_>>());
    assert!(ids(&engine, &session).await.is_empty());
}

#[tokio::test]
async fn find_delete_waiting_for_write_lock_times_out_without_deleting() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let mut first = Connection::open(root.path().join("shards/0000.sqlite")).unwrap();
    let mut second = Connection::open(root.path().join("shards/0001.sqlite")).unwrap();
    let left = first
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let right = second
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let error = engine
        .execute_document(
            &session,
            request(
                find_delete(BsonDocument::new(), DocumentReadOptions::new()),
                RequestContext::new()
                    .with_timeout(Duration::from_millis(150))
                    .unwrap(),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
    left.rollback().unwrap();
    right.rollback().unwrap();
    assert_eq!(ids(&engine, &session).await.len(), 80);
    assert!(
        removed(
            &engine,
            &session,
            BsonDocument::new(),
            DocumentReadOptions::new()
        )
        .await
        .is_some()
    );
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
