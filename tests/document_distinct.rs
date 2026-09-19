#![cfg(feature = "documents")]

use briskdb::{
    core::{CancellationToken, Engine, EngineErrorKind, RequestContext, ResultLimits},
    document::{
        BsonDocument, BsonValue, DocumentCollectionOptions, DocumentCommand,
        DocumentCreateCollectionRequest, DocumentDistinctRequest, DocumentFilter,
        DocumentFindRequest, DocumentInsertRequest, DocumentNamespace, DocumentPlan,
        DocumentReadOptions, DocumentRequest, DocumentRequestId, DocumentResult,
        DocumentWriteOptions, encode_document,
    },
};
use std::time::{Duration, Instant};

fn namespace() -> DocumentNamespace {
    DocumentNamespace::new("app", "distinct").unwrap()
}
fn request(command: DocumentCommand, context: RequestContext) -> DocumentRequest {
    DocumentRequest::new(DocumentRequestId::new([1; 16]).unwrap(), context, command)
}
fn distinct(field: &str, filter: BsonDocument) -> DocumentCommand {
    DocumentCommand::Distinct(
        DocumentDistinctRequest::new(
            namespace(),
            field,
            DocumentFilter::new(filter).unwrap(),
            DocumentReadOptions::new(),
        )
        .unwrap(),
    )
}
fn encoded(values: &[BsonValue]) -> Vec<u8> {
    encode_document(
        &BsonDocument::from_entries([("values", BsonValue::Array(values.to_vec()))]).unwrap(),
    )
    .unwrap()
}
async fn setup(documents: Vec<BsonDocument>) -> (tempfile::TempDir, Engine) {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
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
    if !documents.is_empty() {
        engine
            .execute_document(
                &session,
                request(
                    DocumentCommand::Insert(
                        DocumentInsertRequest::new(
                            namespace(),
                            documents,
                            DocumentWriteOptions::new(),
                        )
                        .unwrap(),
                    ),
                    RequestContext::new(),
                ),
            )
            .await
            .unwrap();
    }
    (root, engine)
}

#[tokio::test]
async fn distinct_merges_global_first_representations_and_reopens() {
    let documents = (0..120)
        .map(|index| {
            BsonDocument::from_entries([
                ("_id", BsonValue::Int32(index)),
                (
                    "v",
                    if index < 7 {
                        BsonValue::Int64(i64::from(index))
                    } else {
                        BsonValue::Double(f64::from(index % 7))
                    },
                ),
                ("group", BsonValue::Int32(index % 2)),
            ])
            .unwrap()
        })
        .collect();
    let (root, engine) = setup(documents).await;
    let session = engine.session();
    let all = engine
        .execute_document(
            &session,
            request(distinct("v", BsonDocument::new()), RequestContext::new()),
        )
        .await
        .unwrap();
    assert!(
        matches!(all.plan(), Some(DocumentPlan::Scatter(plan)) if plan.shards() == [0, 1, 2, 3])
    );
    let DocumentResult::Distinct(values) = all.result() else {
        panic!("distinct");
    };
    assert_eq!(
        encoded(values),
        encoded(&(0..7).map(BsonValue::Int64).collect::<Vec<_>>())
    );
    let filtered = engine
        .execute_document(
            &session,
            request(
                distinct(
                    "v",
                    BsonDocument::from_entries([("group", BsonValue::Int32(1))]).unwrap(),
                ),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    let DocumentResult::Distinct(values) = filtered.result() else {
        panic!("distinct");
    };
    assert_eq!(
        encoded(values),
        encoded(&[
            BsonValue::Int64(1),
            BsonValue::Int64(3),
            BsonValue::Int64(5),
            BsonValue::Double(0.0),
            BsonValue::Double(2.0),
            BsonValue::Double(4.0),
            BsonValue::Double(6.0)
        ])
    );
    let point = engine
        .execute_document(
            &session,
            request(
                distinct(
                    "v",
                    BsonDocument::from_entries([("_id", BsonValue::Int32(3))]).unwrap(),
                ),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    assert!(matches!(point.plan(), Some(DocumentPlan::Point(_))));
    assert_eq!(
        point.result(),
        &DocumentResult::Distinct(vec![BsonValue::Int64(3)].into_boxed_slice())
    );
    engine.shutdown().await.unwrap();
    let reopened = Engine::open(root.path(), 4).await.unwrap();
    let again = reopened
        .execute_document(
            &reopened.session(),
            request(distinct("v", BsonDocument::new()), RequestContext::new()),
        )
        .await
        .unwrap();
    let DocumentResult::Distinct(values) = again.result() else {
        panic!("distinct");
    };
    assert_eq!(
        encoded(values),
        encoded(&(0..7).map(BsonValue::Int64).collect::<Vec<_>>())
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn distinct_budgets_only_output_and_uses_no_retained_cursor_slot() {
    let documents = (0..12)
        .map(|index| {
            BsonDocument::from_entries([
                ("_id", BsonValue::Int32(index)),
                ("v", BsonValue::Int32(index % 2)),
                ("payload", BsonValue::from("x".repeat(100_000))),
            ])
            .unwrap()
        })
        .collect();
    let (_root, engine) = setup(documents).await;
    let session = engine.session();
    for _ in 0..8 {
        let result = engine
            .execute_document(
                &session,
                request(
                    DocumentCommand::Find(DocumentFindRequest::new(
                        namespace(),
                        DocumentFilter::empty(),
                        DocumentReadOptions::new().with_batch_size(0).unwrap(),
                    )),
                    RequestContext::new(),
                ),
            )
            .await
            .unwrap();
        assert!(
            matches!(result.result(), DocumentResult::Cursor(batch) if batch.cursor_id().is_some())
        );
    }
    let result = engine
        .execute_document(
            &session,
            request(
                distinct("v", BsonDocument::new()),
                RequestContext::new().with_result_limits(ResultLimits::new(2, 256).unwrap()),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        result.result(),
        &DocumentResult::Distinct(
            vec![BsonValue::Int32(0), BsonValue::Int32(1)].into_boxed_slice()
        )
    );
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    for (context, kind) in [
        (
            RequestContext::new().with_result_limits(ResultLimits::new(1, 256).unwrap()),
            EngineErrorKind::LimitExceeded,
        ),
        (
            RequestContext::new().with_result_limits(ResultLimits::new(2, 70).unwrap()),
            EngineErrorKind::LimitExceeded,
        ),
        (
            RequestContext::new().with_cancellation_token(cancelled),
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
                    request(distinct("v", BsonDocument::new()), context)
                )
                .await
                .unwrap_err()
                .kind(),
            kind
        );
    }
    let result = engine
        .execute_document(
            &session,
            request(
                distinct("missing", BsonDocument::new()),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        result.result(),
        &DocumentResult::Distinct(Vec::new().into_boxed_slice())
    );
    // No query failure can leave the session busy or consume a cursor slot.
    session.close().await.unwrap();
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn distinct_empty_input_and_unsupported_options_are_explicit() {
    let (_root, engine) = setup(Vec::new()).await;
    let session = engine.session();
    let result = engine
        .execute_document(
            &session,
            request(distinct("", BsonDocument::new()), RequestContext::new()),
        )
        .await
        .unwrap();
    assert_eq!(
        result.result(),
        &DocumentResult::Distinct(Vec::new().into_boxed_slice())
    );
    for options in [
        DocumentReadOptions::new().with_skip(1),
        DocumentReadOptions::new().with_limit(1).unwrap(),
        DocumentReadOptions::new().with_batch_size(0).unwrap(),
        DocumentReadOptions::new()
            .with_batch_byte_limit(256)
            .unwrap(),
    ] {
        let command = DocumentCommand::Distinct(
            DocumentDistinctRequest::new(namespace(), "v", DocumentFilter::empty(), options)
                .unwrap(),
        );
        assert_eq!(
            engine
                .execute_document(&session, request(command, RequestContext::new()))
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
    }
    engine.shutdown().await.unwrap();
}
