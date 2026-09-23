#![cfg(feature = "documents")]

use std::{
    error::Error,
    time::{Duration, Instant},
};

use briskdb::{
    core::{CancellationToken, Engine, EngineErrorKind, RequestContext, ResultLimits, Session},
    document::{
        BsonDocument, BsonValue, DocumentBuildIndexRequest, DocumentCollectionOptions,
        DocumentCommand, DocumentContinueCursorRequest, DocumentCreateCollectionRequest,
        DocumentCreateIndexRequest, DocumentCursorBatch, DocumentCursorError, DocumentCursorId,
        DocumentDropCollectionRequest, DocumentDropIndexRequest, DocumentIndexRequest,
        DocumentKillCursorRequest, DocumentListIndexMetadataRequest, DocumentNamespace,
        DocumentReadOptions, DocumentRequest, DocumentRequestId, DocumentResult,
        DocumentWriteOptions,
    },
};

fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}

fn ns() -> DocumentNamespace {
    DocumentNamespace::new("app", "items").unwrap()
}
fn options(size: u64) -> DocumentReadOptions {
    DocumentReadOptions::new().with_batch_size(size).unwrap()
}
fn listing(options: DocumentReadOptions) -> DocumentCommand {
    DocumentCommand::ListIndexMetadata(DocumentListIndexMetadataRequest::new(ns(), options))
}
fn next(id: DocumentCursorId, options: DocumentReadOptions) -> DocumentCommand {
    DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(ns(), id, options))
}
fn request(command: DocumentCommand, context: RequestContext) -> DocumentRequest {
    DocumentRequest::new(DocumentRequestId::new([1; 16]).unwrap(), context, command)
}
async fn execute(engine: &Engine, session: &Session, command: DocumentCommand) -> DocumentResult {
    engine
        .execute_document(session, request(command, RequestContext::new()))
        .await
        .unwrap()
        .into_parts()
        .2
}
async fn page(engine: &Engine, session: &Session, command: DocumentCommand) -> DocumentCursorBatch {
    let result = engine
        .execute_document(session, request(command, RequestContext::new()))
        .await
        .unwrap();
    assert!(result.plan().is_none());
    let DocumentResult::Cursor(page) = result.into_parts().2 else {
        panic!("metadata page")
    };
    assert_eq!(page.namespace(), &ns());
    page
}
async fn create(engine: &Engine, session: &Session) {
    execute(
        engine,
        session,
        DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
            ns(),
            DocumentCollectionOptions::empty(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
}
async fn index(
    engine: &Engine,
    session: &Session,
    name: &str,
    definition: DocumentIndexRequest,
    build: bool,
) {
    execute(
        engine,
        session,
        DocumentCommand::CreateIndex(DocumentCreateIndexRequest::new(
            ns(),
            definition.with_name(name).unwrap(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    if build {
        execute(
            engine,
            session,
            DocumentCommand::BuildIndex(
                DocumentBuildIndexRequest::new(ns(), name, DocumentWriteOptions::new()).unwrap(),
            ),
        )
        .await;
    }
}
fn simple() -> DocumentIndexRequest {
    DocumentIndexRequest::new(doc([("value", BsonValue::Int32(1))])).unwrap()
}
fn names(page: &DocumentCursorBatch) -> Vec<&str> {
    page.documents()
        .iter()
        .map(|row| match row.get_first("name") {
            Some(BsonValue::String(name)) => name.as_str(),
            _ => panic!("name"),
        })
        .collect()
}
async fn drop_index(engine: &Engine, session: &Session, name: &str) {
    execute(
        engine,
        session,
        DocumentCommand::DropIndex(
            DocumentDropIndexRequest::new(ns(), name, DocumentWriteOptions::new()).unwrap(),
        ),
    )
    .await;
}

#[tokio::test]
async fn ready_metadata_preserves_options_builtin_first_name_order_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    let absent = page(&engine, &session, listing(options(0))).await;
    assert!(absent.documents().is_empty() && absent.cursor_id().is_none());
    create(&engine, &session).await;
    index(&engine, &session, "z", simple(), true).await;
    let keys = doc([
        ("nested.value", BsonValue::Int32(1)),
        ("tail", BsonValue::Int32(-1)),
    ]);
    index(
        &engine,
        &session,
        "!before_id",
        DocumentIndexRequest::new(keys.clone())
            .unwrap()
            .with_sparse(true),
        true,
    )
    .await;
    let partial = doc([("active", BsonValue::Boolean(true))]);
    index(
        &engine,
        &session,
        "partial",
        simple()
            .with_partial_filter(briskdb::document::DocumentFilter::new(partial.clone()).unwrap()),
        true,
    )
    .await;
    index(&engine, &session, "pending", simple(), false).await;
    index(
        &engine,
        &session,
        "pending_unique",
        simple().with_unique(true),
        false,
    )
    .await;
    let mut batch = page(&engine, &session, listing(options(1))).await;
    assert_eq!(names(&batch), ["_id_"]);
    let mut rows = batch.documents().to_vec();
    while let Some(id) = batch.cursor_id() {
        batch = page(&engine, &session, next(id, options(1))).await;
        rows.extend_from_slice(batch.documents());
    }
    assert_eq!(
        rows,
        vec![
            doc([
                ("name", BsonValue::from("_id_")),
                (
                    "key",
                    BsonValue::Document(doc([("_id", BsonValue::Int32(1))]))
                )
            ]),
            doc([
                ("name", BsonValue::from("!before_id")),
                ("key", BsonValue::Document(keys)),
                ("sparse", BsonValue::Boolean(true))
            ]),
            doc([
                ("name", BsonValue::from("partial")),
                (
                    "key",
                    BsonValue::Document(doc([("value", BsonValue::Int32(1))]))
                ),
                ("partialFilterExpression", BsonValue::Document(partial))
            ]),
            doc([
                ("name", BsonValue::from("z")),
                (
                    "key",
                    BsonValue::Document(doc([("value", BsonValue::Int32(1))]))
                )
            ]),
        ]
    );
    engine.shutdown().await.unwrap();
    drop(session);
    drop(engine);
    let engine = Engine::open(root.path(), 2).await.unwrap();
    assert_eq!(
        page(&engine, &engine.session(), listing(options(100)))
            .await
            .documents(),
        rows
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn index_metadata_fences_recreation_and_excludes_new_identities() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    create(&engine, &session).await;
    for name in ["a", "b"] {
        index(&engine, &session, name, simple(), true).await;
    }
    index(&engine, &session, "pending_late", simple(), false).await;
    let id = page(&engine, &session, listing(options(0)))
        .await
        .cursor_id()
        .unwrap();
    index(&engine, &session, "later", simple(), true).await;
    // A cursor is not a cross-page snapshot. Activation of an already allocated
    // declaration is visible if its name has not yet been passed.
    execute(
        &engine,
        &session,
        DocumentCommand::BuildIndex(
            DocumentBuildIndexRequest::new(ns(), "pending_late", DocumentWriteOptions::new())
                .unwrap(),
        ),
    )
    .await;
    drop_index(&engine, &session, "a").await;
    index(&engine, &session, "a", simple(), true).await;
    let error = engine
        .execute_document(
            &engine.session(),
            request(next(id, options(10)), RequestContext::new()),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<DocumentCursorError>(),
        Some(&DocumentCursorError::NotFound)
    );
    let batch = page(&engine, &session, next(id, options(10))).await;
    assert_eq!(names(&batch), ["_id_", "b", "pending_late"]);
    assert!(batch.cursor_id().is_none());
    let id = page(&engine, &session, listing(options(0)))
        .await
        .cursor_id()
        .unwrap();
    execute(
        &engine,
        &session,
        DocumentCommand::DropCollection(DocumentDropCollectionRequest::new(
            ns(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    create(&engine, &session).await;
    let error = engine
        .execute_document(
            &session,
            request(next(id, options(1)), RequestContext::new()),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<DocumentCursorError>(),
        Some(&DocumentCursorError::NotFound)
    );
    assert!(
        engine
            .execute_document(
                &session,
                request(next(id, options(1)), RequestContext::new())
            )
            .await
            .is_err()
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn index_metadata_budgets_controls_quotas_and_error_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    create(&engine, &session).await;
    index(&engine, &session, "a", simple(), true).await;
    for limits in [
        ResultLimits::new(1, 4096).unwrap(),
        ResultLimits::new(10, 1).unwrap(),
    ] {
        let error = engine
            .execute_document(
                &session,
                request(
                    listing(options(10)),
                    RequestContext::new().with_result_limits(limits),
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    }
    // Both rows individually fit, but not together. The continuation retains
    // the opening soft byte cap and must neither skip nor duplicate a row.
    let batch = page(
        &engine,
        &session,
        listing(options(10).with_batch_byte_limit(110).unwrap()),
    )
    .await;
    assert_eq!(names(&batch), ["_id_"]);
    let batch = page(
        &engine,
        &session,
        next(batch.cursor_id().unwrap(), options(10)),
    )
    .await;
    assert_eq!(names(&batch), ["a"]);
    assert!(batch.cursor_id().is_none());
    let token = CancellationToken::new();
    token.cancel();
    for context in [
        RequestContext::new().with_cancellation_token(token),
        RequestContext::new().with_deadline(Instant::now() - Duration::from_secs(1)),
    ] {
        assert!(
            engine
                .execute_document(&session, request(listing(options(0)), context))
                .await
                .is_err()
        );
    }
    for _ in 0..12 {
        let id = page(&engine, &session, listing(options(0)))
            .await
            .cursor_id()
            .unwrap();
        let error = engine
            .execute_document(
                &session,
                request(
                    next(id, options(1).with_batch_byte_limit(1).unwrap()),
                    RequestContext::new(),
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    }
    let mut ids = Vec::new();
    for _ in 0..8 {
        ids.push(
            page(&engine, &session, listing(options(0)))
                .await
                .cursor_id()
                .unwrap(),
        );
    }
    assert_eq!(
        engine
            .execute_document(
                &session,
                request(listing(options(0)), RequestContext::new())
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    for id in ids {
        assert!(matches!(
            execute(
                &engine,
                &session,
                DocumentCommand::KillCursor(DocumentKillCursorRequest::new(
                    ns(),
                    id,
                    DocumentWriteOptions::new()
                ))
            )
            .await,
            DocumentResult::CursorKilled(true)
        ));
    }
    for invalid in [
        DocumentReadOptions::new().with_skip(1),
        DocumentReadOptions::new().with_limit(1).unwrap(),
    ] {
        assert_eq!(
            engine
                .execute_document(&session, request(listing(invalid), RequestContext::new()))
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
    }
    engine.shutdown().await.unwrap();
}
