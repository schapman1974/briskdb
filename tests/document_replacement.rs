#![cfg(feature = "documents")]

use briskdb::{
    core::{CancellationToken, Engine, EngineErrorKind, RequestContext, ResultLimits, Session},
    document::{
        BsonDocument, BsonTimestamp, BsonValue, DocumentCollectionOptions, DocumentCommand,
        DocumentCreateCollectionRequest, DocumentFilter, DocumentFindRequest,
        DocumentInsertRequest, DocumentMutationError, DocumentNamespace, DocumentPlan,
        DocumentReadOptions, DocumentReplaceRequest, DocumentRequest, DocumentRequestId,
        DocumentResult, DocumentWriteOptions, encode_document,
    },
};
use rusqlite::{Connection, TransactionBehavior};
use std::{
    error::Error,
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
fn replace(filter: BsonDocument, replacement: BsonDocument) -> DocumentReplaceRequest {
    DocumentReplaceRequest::new(
        ns(),
        DocumentFilter::new(filter).unwrap(),
        replacement,
        DocumentWriteOptions::new(),
    )
    .unwrap()
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
    let rows = (0..24)
        .map(|i| {
            doc([
                ("_id", BsonValue::Int32(i)),
                ("group", BsonValue::Int32(i % 2)),
                ("done", BsonValue::Boolean(false)),
            ])
        })
        .collect::<Vec<_>>();
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
async fn rows(engine: &Engine, session: &Session) -> Vec<BsonDocument> {
    let result = engine
        .execute_document(
            session,
            request(
                DocumentCommand::Find(DocumentFindRequest::new(
                    ns(),
                    DocumentFilter::empty(),
                    DocumentReadOptions::new(),
                )),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    let DocumentResult::Cursor(batch) = result.into_parts().2 else {
        panic!("cursor")
    };
    assert!(batch.is_exhausted());
    batch.into_parts().2
}
async fn changed(
    engine: &Engine,
    session: &Session,
    filter: BsonDocument,
    replacement: BsonDocument,
    point: bool,
) -> (u64, u64) {
    let execution = engine
        .execute_document(
            session,
            request(
                DocumentCommand::Replace(replace(filter, replacement)),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        matches!(execution.plan(), Some(DocumentPlan::Point(_))),
        point
    );
    let DocumentResult::Update(result) = execution.result() else {
        panic!("update")
    };
    assert!(result.acknowledged());
    assert!(result.upserted_id().is_none());
    (result.matched_count(), result.modified_count())
}

#[tokio::test]
async fn replace_preserves_id_natural_order_and_exact_representation_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let replacement = doc([
        ("value", BsonValue::Int64(7)),
        ("_id", BsonValue::Double(1.0)),
    ]);
    assert_eq!(
        changed(
            &engine,
            &session,
            doc([("group", BsonValue::Int32(1))]),
            replacement.clone(),
            false
        )
        .await,
        (1, 1)
    );
    let current = rows(&engine, &session).await;
    assert_eq!(current.len(), 24);
    for (i, row) in current.iter().enumerate() {
        assert!(matches!(row.get_first("_id"), Some(BsonValue::Int32(n)) if *n == i as i32));
    }
    assert_eq!(
        current[1].iter().map(|(name, _)| name).collect::<Vec<_>>(),
        ["_id", "value"]
    );
    let filter = doc([("_id", BsonValue::Double(1.0))]);
    assert_eq!(
        changed(&engine, &session, filter.clone(), replacement, true).await,
        (1, 0)
    );
    for value in [
        BsonValue::Int32(7),
        BsonValue::Double(7.0),
        BsonValue::Int64(7),
    ] {
        let replacement = doc([("value", value)]);
        assert_eq!(
            changed(&engine, &session, filter.clone(), replacement.clone(), true).await,
            (1, 1)
        );
        assert_eq!(
            changed(&engine, &session, filter.clone(), replacement, true).await,
            (1, 0)
        );
    }
    assert_eq!(
        changed(
            &engine,
            &session,
            doc([("_id", BsonValue::Int32(-1))]),
            BsonDocument::new(),
            true
        )
        .await,
        (0, 0)
    );
    assert_eq!(
        changed(
            &engine,
            &session,
            doc([("absent", BsonValue::Boolean(true))]),
            BsonDocument::new(),
            false
        )
        .await,
        (0, 0)
    );
    let before = rows(&engine, &session).await;
    drop(session);
    engine.shutdown().await.unwrap();
    drop(engine);
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let after = rows(&engine, &engine.session()).await;
    assert_eq!(
        before
            .iter()
            .map(|row| encode_document(row).unwrap())
            .collect::<Vec<_>>(),
        after
            .iter()
            .map(|row| encode_document(row).unwrap())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn replace_rejects_invalid_options_identity_and_budgets_before_mutation() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let before = rows(&engine, &session).await;
    for filter in [doc([("_id", BsonValue::Int32(0))]), BsonDocument::new()] {
        let error = engine
            .execute_document(
                &session,
                request(
                    DocumentCommand::Replace(replace(
                        filter.clone(),
                        doc([("_id", BsonValue::Int32(99))]),
                    )),
                    RequestContext::new(),
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<DocumentMutationError>(),
            Some(&DocumentMutationError::ImmutableId)
        );
        let cancel = CancellationToken::new();
        cancel.cancel();
        for (context, kind) in [
            (
                RequestContext::new().with_result_limits(ResultLimits::new(1, 1).unwrap()),
                EngineErrorKind::LimitExceeded,
            ),
            (
                RequestContext::new().with_cancellation_token(cancel),
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
                        request(
                            DocumentCommand::Replace(replace(filter.clone(), BsonDocument::new())),
                            context
                        )
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
                        DocumentCommand::Replace(
                            replace(filter, doc([("payload", BsonValue::from("x".repeat(100)))]))
                                .with_max_document_bytes(32)
                                .unwrap()
                        ),
                        RequestContext::new()
                    )
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
    }
    for replacement in [
        doc([("$set", BsonValue::Document(BsonDocument::new()))]),
        doc([
            ("ordinary", BsonValue::Int32(1)),
            ("$inc", BsonValue::Int32(1)),
        ]),
    ] {
        assert!(
            DocumentReplaceRequest::new(
                ns(),
                DocumentFilter::empty(),
                replacement,
                DocumentWriteOptions::new()
            )
            .is_err()
        );
    }
    for options in [
        DocumentWriteOptions::new().with_upsert(true),
        DocumentWriteOptions::new().with_ordered(false),
        DocumentWriteOptions::new().with_bypass_document_validation(true),
    ] {
        let command = DocumentReplaceRequest::new(
            ns(),
            DocumentFilter::empty(),
            BsonDocument::new(),
            options,
        )
        .unwrap();
        assert_eq!(
            engine
                .execute_document(
                    &session,
                    request(DocumentCommand::Replace(command), RequestContext::new())
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
    }
    assert_eq!(
        engine
            .execute_document(
                &session,
                request(
                    DocumentCommand::Replace(replace(
                        doc([("$where", BsonValue::from("private"))]),
                        BsonDocument::new()
                    )),
                    RequestContext::new()
                )
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::Unsupported
    );
    assert_eq!(before, rows(&engine, &session).await);
}

#[tokio::test]
async fn replacements_stamp_only_top_level_zero_timestamps_and_keep_id_first() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let zero = BsonValue::Timestamp(BsonTimestamp::new(0, 0));
    let replacement = doc([
        ("a", zero.clone()),
        ("b", zero.clone()),
        ("nested", BsonValue::Document(doc([("v", zero.clone())]))),
        ("array", BsonValue::Array(vec![zero.clone()])),
    ]);
    changed(
        &engine,
        &session,
        doc([("_id", BsonValue::Int32(0))]),
        replacement.clone(),
        true,
    )
    .await;
    let current = rows(&engine, &session).await;
    assert_ne!(current[0].get_first("a"), Some(&zero));
    assert_ne!(current[0].get_first("a"), current[0].get_first("b"));
    assert_eq!(
        current[0].get_first("nested"),
        replacement.get_first("nested")
    );
    assert_eq!(
        current[0].get_first("array"),
        replacement.get_first("array")
    );
    assert_eq!(
        changed(
            &engine,
            &session,
            doc([("_id", BsonValue::Int32(0))]),
            BsonDocument::new(),
            true
        )
        .await,
        (1, 1)
    );
    assert_eq!(
        rows(&engine, &session).await[0],
        doc([("_id", BsonValue::Int32(0))])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_filtered_replacements_do_not_rewrite_stale_matches() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(root.path(), 4).await.unwrap());
    seed(&engine, &engine.session()).await;
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let engine = Arc::clone(&engine);
        tasks.push(tokio::spawn(async move {
            let session = engine.session();
            let mut count = 0;
            loop {
                let (matched, modified) = changed(
                    &engine,
                    &session,
                    doc([("done", BsonValue::Boolean(false))]),
                    doc([("done", BsonValue::Boolean(true))]),
                    false,
                )
                .await;
                assert_eq!(matched, modified);
                if matched == 0 {
                    return count;
                }
                count += modified;
            }
        }));
    }
    let mut count = 0;
    for task in tasks {
        count += task.await.unwrap();
    }
    assert_eq!(count, 24);
    assert!(
        rows(&engine, &engine.session())
            .await
            .iter()
            .all(|row| row.get_first("done") == Some(&BsonValue::Boolean(true)))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_write_lock_deadline_leaves_documents_and_session_usable() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let before = rows(&engine, &session).await;
    let mut connections = (0..2)
        .map(|shard| {
            Connection::open(root.path().join(format!("shards/{shard:04}.sqlite"))).unwrap()
        })
        .collect::<Vec<_>>();
    let locks = connections
        .iter_mut()
        .map(|connection| {
            connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let error = engine
        .execute_document(
            &session,
            request(
                DocumentCommand::Replace(replace(BsonDocument::new(), BsonDocument::new())),
                RequestContext::new()
                    .with_timeout(Duration::from_millis(150))
                    .unwrap(),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
    drop(locks);
    assert_eq!(before, rows(&engine, &session).await);
    assert_eq!(
        changed(
            &engine,
            &session,
            BsonDocument::new(),
            BsonDocument::new(),
            false
        )
        .await,
        (1, 1)
    );
}
