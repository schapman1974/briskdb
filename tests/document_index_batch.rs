#![cfg(feature = "documents")]

use briskdb::{
    core::{CancellationToken, Engine, EngineError, EngineErrorKind, RequestContext, ResultLimits},
    document::*,
};
use std::{
    error::Error,
    time::{Duration, Instant},
};

fn namespace() -> DocumentNamespace {
    DocumentNamespace::new("app", "items").unwrap()
}
fn request(command: DocumentCommand, context: RequestContext) -> DocumentRequest {
    DocumentRequest::new(DocumentRequestId::new([1; 16]).unwrap(), context, command)
}
fn index(field: &str, name: &str) -> DocumentIndexRequest {
    DocumentIndexRequest::new(BsonDocument::from_entries([(field, BsonValue::Int32(1))]).unwrap())
        .unwrap()
        .with_name(name)
        .unwrap()
}
fn batch(indexes: Vec<DocumentIndexRequest>) -> DocumentCommand {
    DocumentCommand::CreateIndexes(
        DocumentCreateIndexesRequest::new(namespace(), indexes, DocumentWriteOptions::new())
            .unwrap(),
    )
}
fn index_code(error: &EngineError) -> i32 {
    let mut source = error.source();
    while let Some(cause) = source {
        if let Some(index) = cause.downcast_ref::<DocumentIndexError>() {
            return index.mongo_code();
        }
        source = cause.source();
    }
    panic!("missing typed index error: {error:?}");
}

#[tokio::test]
async fn index_batch_counts_conflicts_completed_prefix_and_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
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
    let definitions = || {
        vec![
            index("a", "a"),
            index("b", "b").with_sparse(true),
            index("_id", "ignored"),
        ]
    };
    for (before, after) in [(1, 3), (3, 3)] {
        let result = engine
            .execute_document(
                &session,
                request(
                    batch(definitions()),
                    RequestContext::new().with_result_limits(ResultLimits::new(1, 49).unwrap()),
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            result.result(),
            &DocumentResult::IndexesBuilt { before, after }
        );
    }
    for (definition, code) in [
        (index("b", "a"), 86),
        (index("a", "a").with_unique(true), 86),
        (index("a", "other"), 85),
        (index("a", "a").with_sparse(true), 86),
        (index("_id", "ignored").with_unique(true), 197),
    ] {
        let error = engine
            .execute_document(
                &session,
                request(batch(vec![definition]), RequestContext::new()),
            )
            .await
            .unwrap_err();
        assert_eq!(index_code(&error), code);
    }
    // Runtime failures retain the completed prefix, without fencing a clean root.
    let error = engine
        .execute_document(
            &session,
            request(
                batch(vec![index("c", "c"), index("d", "d").with_unique(true)]),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Unsupported);
    let result = engine
        .execute_document(
            &session,
            request(batch(vec![index("_id", "_id_")]), RequestContext::new()),
        )
        .await
        .unwrap();
    assert_eq!(
        result.result(),
        &DocumentResult::IndexesBuilt {
            before: 4,
            after: 4
        }
    );
    // New writes maintain every completed index; startup validates their coverage.
    engine
        .execute_document(
            &session,
            request(
                DocumentCommand::Insert(
                    DocumentInsertRequest::new(
                        namespace(),
                        vec![
                            BsonDocument::from_entries([
                                ("_id", BsonValue::Int32(1)),
                                ("a", BsonValue::Int32(2)),
                                ("b", BsonValue::Int32(3)),
                                ("c", BsonValue::Int32(4)),
                            ])
                            .unwrap(),
                        ],
                        DocumentWriteOptions::new(),
                    )
                    .unwrap(),
                ),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    drop(session);
    drop(engine);
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
    let result = engine
        .execute_document(
            &session,
            request(batch(definitions()), RequestContext::new()),
        )
        .await
        .unwrap();
    assert_eq!(
        result.result(),
        &DocumentResult::IndexesBuilt {
            before: 4,
            after: 4
        }
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn index_batch_eager_validation_and_result_control_failures_do_not_mutate() {
    assert!(
        DocumentCreateIndexesRequest::new(namespace(), vec![], DocumentWriteOptions::new())
            .is_err()
    );
    assert!(
        DocumentCreateIndexesRequest::new(
            namespace(),
            vec![index("a", "a"); 1001],
            DocumentWriteOptions::new()
        )
        .is_err()
    );
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
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
    let invalid = DocumentIndexRequest::new(
        BsonDocument::from_entries([("late", BsonValue::Boolean(true))]).unwrap(),
    )
    .unwrap();
    assert!(
        engine
            .execute_document(
                &session,
                request(batch(vec![index("a", "a"), invalid]), RequestContext::new())
            )
            .await
            .is_err()
    );
    let token = CancellationToken::new();
    token.cancel();
    for (context, kind) in [
        (
            RequestContext::new().with_result_limits(ResultLimits::new(1, 48).unwrap()),
            EngineErrorKind::LimitExceeded,
        ),
        (
            RequestContext::new().with_cancellation_token(token),
            EngineErrorKind::Cancelled,
        ),
        (
            RequestContext::new().with_deadline(Instant::now() - Duration::from_secs(1)),
            EngineErrorKind::DeadlineExceeded,
        ),
    ] {
        let error = engine
            .execute_document(&session, request(batch(vec![index("a", "a")]), context))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), kind);
    }
    let result = engine
        .execute_document(
            &session,
            request(batch(vec![index("_id", "_id_")]), RequestContext::new()),
        )
        .await
        .unwrap();
    assert_eq!(
        result.result(),
        &DocumentResult::IndexesBuilt {
            before: 1,
            after: 1
        }
    );
    engine.shutdown().await.unwrap();
}
