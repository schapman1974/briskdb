#![cfg(feature = "documents")]

use std::error::Error;

use briskdb::{
    core::{Engine, EngineErrorKind, RequestContext, ResultLimits, Session},
    document::{
        BsonDocument, BsonValue, DocumentCollectionOptions, DocumentCommand,
        DocumentContinueCursorRequest, DocumentCreateCollectionRequest, DocumentCursorError,
        DocumentCursorId, DocumentExecution, DocumentFilter, DocumentFindRequest,
        DocumentInsertRequest, DocumentKillCursorRequest, DocumentNamespace, DocumentPlan,
        DocumentProjection, DocumentReadOptions, DocumentRequest, DocumentRequestId,
        DocumentResult, DocumentWriteOptions, encode_document,
    },
};

fn namespace() -> DocumentNamespace {
    DocumentNamespace::new("app", "items").unwrap()
}
fn request(command: DocumentCommand) -> DocumentRequest {
    DocumentRequest::new(
        DocumentRequestId::new([1; 16]).unwrap(),
        RequestContext::new(),
        command,
    )
}
async fn call(engine: &Engine, session: &Session, command: DocumentCommand) -> DocumentExecution {
    engine
        .execute_document(session, request(command))
        .await
        .unwrap()
}
fn find(options: DocumentReadOptions) -> DocumentCommand {
    DocumentCommand::Find(DocumentFindRequest::new(
        namespace(),
        DocumentFilter::empty(),
        options,
    ))
}
fn more(id: DocumentCursorId, batch: u64) -> DocumentCommand {
    DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
        namespace(),
        id,
        DocumentReadOptions::new().with_batch_size(batch).unwrap(),
    ))
}
fn cursor(execution: DocumentExecution) -> (Option<DocumentCursorId>, Vec<BsonDocument>) {
    let DocumentResult::Cursor(batch) = execution.into_parts().2 else {
        panic!("cursor result");
    };
    let (_, id, documents) = batch.into_parts();
    (id, documents)
}
fn ids(documents: &[BsonDocument]) -> Vec<i32> {
    documents
        .iter()
        .map(|doc| match doc.get_first("_id").unwrap() {
            BsonValue::Int32(id) => *id,
            _ => panic!("integer id"),
        })
        .collect()
}
async fn seed(engine: &Engine, session: &Session, count: i32) {
    call(
        engine,
        session,
        DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
            namespace(),
            DocumentCollectionOptions::empty(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    call(
        engine,
        session,
        DocumentCommand::Insert(
            DocumentInsertRequest::new(
                namespace(),
                (0..count)
                    .map(|id| {
                        BsonDocument::from_entries([
                            ("_id", BsonValue::Int32(id)),
                            ("rank", BsonValue::Int32(id)),
                            ("payload", BsonValue::String("x".repeat(300))),
                        ])
                        .unwrap()
                    })
                    .collect::<Vec<_>>(),
                DocumentWriteOptions::new(),
            )
            .unwrap(),
        ),
    )
    .await;
}

#[tokio::test]
async fn projected_cursors_filter_original_values_and_budget_only_returned_fields() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session, 24).await;
    let projection = DocumentProjection::new(
        BsonDocument::from_entries([("rank", BsonValue::Int32(1)), ("_id", BsonValue::Int32(0))])
            .unwrap(),
    )
    .unwrap();
    let filter = DocumentFilter::new(
        BsonDocument::from_entries([
            (
                "rank",
                BsonValue::Document(
                    BsonDocument::from_entries([("$gte", BsonValue::Int32(5))]).unwrap(),
                ),
            ),
            ("payload", BsonValue::String("x".repeat(300))),
        ])
        .unwrap(),
    )
    .unwrap();
    let first = engine
        .execute_document(
            &session,
            DocumentRequest::new(
                DocumentRequestId::new([3; 16]).unwrap(),
                RequestContext::new().with_result_limits(ResultLimits::new(4, 400).unwrap()),
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace(),
                    filter,
                    DocumentReadOptions::new()
                        .with_projection(projection.clone())
                        .with_skip(2)
                        .with_limit(13)
                        .unwrap()
                        .with_batch_size(4)
                        .unwrap()
                        .with_batch_byte_limit(400)
                        .unwrap(),
                )),
            ),
        )
        .await
        .unwrap();
    let (mut id, mut documents) = cursor(first);
    let rejected = engine
        .execute_document(
            &session,
            request(DocumentCommand::ContinueCursor(
                DocumentContinueCursorRequest::new(
                    namespace(),
                    id.unwrap(),
                    DocumentReadOptions::new().with_projection(projection.clone()),
                ),
            )),
        )
        .await
        .unwrap_err();
    assert_eq!(rejected.kind(), EngineErrorKind::InvalidArgument);
    while let Some(current) = id {
        let (next, page) = cursor(call(&engine, &session, more(current, 4)).await);
        documents.extend(page);
        id = next;
    }
    assert_eq!(documents.len(), 13);
    for (index, document) in documents.iter().enumerate() {
        assert_eq!(document.len(), 1);
        assert_eq!(
            document.get_first("rank"),
            Some(&BsonValue::Int32(7 + index as i32))
        );
    }
    let point =
        DocumentFilter::new(BsonDocument::from_entries([("_id", BsonValue::Int32(7))]).unwrap())
            .unwrap();
    let empty = call(
        &engine,
        &session,
        DocumentCommand::Find(DocumentFindRequest::new(
            namespace(),
            point.clone(),
            DocumentReadOptions::new()
                .with_projection(projection)
                .with_batch_size(0)
                .unwrap(),
        )),
    )
    .await;
    assert!(matches!(empty.plan(), Some(DocumentPlan::Point(_))));
    let (id, _) = cursor(empty);
    let projected = call(&engine, &session, more(id.unwrap(), 1)).await;
    assert!(matches!(projected.plan(), Some(DocumentPlan::Point(_))));
    assert!(
        cursor(projected).1[0].representation_eq(
            &BsonDocument::from_entries([("rank", BsonValue::Int32(7))]).unwrap()
        )
    );
    let full = cursor(
        call(
            &engine,
            &session,
            DocumentCommand::Find(DocumentFindRequest::new(
                namespace(),
                point,
                DocumentReadOptions::new(),
            )),
        )
        .await,
    )
    .1;
    assert_eq!(full[0].len(), 3);
    assert_eq!(
        full[0].get_first("payload"),
        Some(&BsonValue::String("x".repeat(300)))
    );
    engine.shutdown().await.unwrap();
}

fn assert_missing(error: briskdb::core::EngineError) {
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<DocumentCursorError>(),
        Some(&DocumentCursorError::NotFound)
    );
}

#[tokio::test]
async fn filtered_cursor_preserves_global_order_skip_limit_and_point_routes() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session, 96).await;
    let filter = DocumentFilter::new(
        BsonDocument::from_entries([(
            "rank",
            BsonValue::Document(
                BsonDocument::from_entries([("$gte", BsonValue::Int32(17))]).unwrap(),
            ),
        )])
        .unwrap(),
    )
    .unwrap();
    let first = call(
        &engine,
        &session,
        DocumentCommand::Find(DocumentFindRequest::new(
            namespace(),
            filter,
            DocumentReadOptions::new()
                .with_skip(5)
                .with_limit(33)
                .unwrap()
                .with_batch_size(7)
                .unwrap(),
        )),
    )
    .await;
    assert!(matches!(first.plan(), Some(DocumentPlan::Scatter(_))));
    let (mut id, documents) = cursor(first);
    let original = id.unwrap();
    assert!(original.get() <= i64::MAX as u64);
    let mut found = ids(&documents);
    assert_eq!(found, (22..29).collect::<Vec<_>>());
    while let Some(current) = id {
        assert_eq!(current, original);
        let (next, documents) = cursor(call(&engine, &session, more(current, 3)).await);
        found.extend(ids(&documents));
        id = next;
    }
    assert_eq!(found, (22..55).collect::<Vec<_>>());
    assert_missing(
        engine
            .execute_document(&session, request(more(original, 3)))
            .await
            .unwrap_err(),
    );

    let filter =
        DocumentFilter::new(BsonDocument::from_entries([("_id", BsonValue::Int32(7))]).unwrap())
            .unwrap();
    let first = call(
        &engine,
        &session,
        DocumentCommand::Find(DocumentFindRequest::new(
            namespace(),
            filter,
            DocumentReadOptions::new().with_batch_size(0).unwrap(),
        )),
    )
    .await;
    assert!(matches!(first.plan(), Some(DocumentPlan::Point(_))));
    let (id, documents) = cursor(first);
    assert!(documents.is_empty());
    let next = call(&engine, &session, more(id.unwrap(), 3)).await;
    assert!(matches!(next.plan(), Some(DocumentPlan::Point(_))));
    let (id, documents) = cursor(next);
    assert!(id.is_none());
    assert_eq!(ids(&documents), vec![7]);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn cursor_ownership_namespace_kill_and_failed_continuation_are_enforced() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 3).await.unwrap();
    let session = engine.session();
    let foreign = engine.session();
    seed(&engine, &session, 12).await;
    let (id, _) = cursor(
        call(
            &engine,
            &session,
            find(DocumentReadOptions::new().with_batch_size(2).unwrap()),
        )
        .await,
    );
    let id = id.unwrap();
    assert_missing(
        engine
            .execute_document(&foreign, request(more(id, 2)))
            .await
            .unwrap_err(),
    );
    let wrong = DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
        DocumentNamespace::new("app", "other").unwrap(),
        id,
        DocumentReadOptions::new(),
    ));
    assert_missing(
        engine
            .execute_document(&session, request(wrong))
            .await
            .unwrap_err(),
    );
    let kill = || {
        DocumentCommand::KillCursor(DocumentKillCursorRequest::new(
            namespace(),
            id,
            DocumentWriteOptions::new(),
        ))
    };
    assert!(matches!(
        call(&engine, &foreign, kill()).await.result(),
        DocumentResult::CursorKilled(false)
    ));
    let (still_id, documents) = cursor(call(&engine, &session, more(id, 2)).await);
    assert_eq!(still_id, Some(id));
    assert_eq!(ids(&documents), vec![2, 3]);
    assert!(matches!(
        call(&engine, &session, kill()).await.result(),
        DocumentResult::CursorKilled(true)
    ));
    assert!(matches!(
        call(&engine, &session, kill()).await.result(),
        DocumentResult::CursorKilled(false)
    ));
    assert_missing(
        engine
            .execute_document(&session, request(more(id, 2)))
            .await
            .unwrap_err(),
    );

    let (id, _) = cursor(
        call(
            &engine,
            &session,
            find(DocumentReadOptions::new().with_batch_size(0).unwrap()),
        )
        .await,
    );
    let id = id.unwrap();
    let failing = DocumentRequest::new(
        DocumentRequestId::new([2; 16]).unwrap(),
        RequestContext::new().with_result_limits(ResultLimits::new(1, 4096).unwrap()),
        more(id, 3),
    );
    assert_eq!(
        engine
            .execute_document(&session, failing)
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    assert_missing(
        engine
            .execute_document(&session, request(more(id, 1)))
            .await
            .unwrap_err(),
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn byte_bounded_pages_resume_without_holding_storage_leases() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session, 13).await;
    let (mut id, documents) = cursor(
        call(
            &engine,
            &session,
            find(
                DocumentReadOptions::new()
                    .with_batch_size(100)
                    .unwrap()
                    .with_batch_byte_limit(800)
                    .unwrap(),
            ),
        )
        .await,
    );
    assert_eq!(documents.len(), 2);
    let mut found = ids(&documents);
    // A live document cursor must not keep a schema operation or SQLite lease.
    call(
        &engine,
        &session,
        DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
            DocumentNamespace::new("app", "while_cursor_open").unwrap(),
            DocumentCollectionOptions::empty(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    while let Some(current) = id {
        let (next, documents) = cursor(call(&engine, &session, more(current, 100)).await);
        let bytes: usize = 16
            + documents
                .iter()
                .map(|doc| encode_document(doc).unwrap().len() + 8)
                .sum::<usize>();
        assert!(bytes <= 800);
        found.extend(ids(&documents));
        id = next;
    }
    assert_eq!(found, (0..13).collect::<Vec<_>>());
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn cursor_caps_and_close_drop_and_failed_delivery_release_slots() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    seed(&engine, &session, 3).await;
    let empty = || find(DocumentReadOptions::new().with_batch_size(0).unwrap());
    // Fail final response accounting AFTER cursor registration; no ID is leaked.
    for _ in 0..12 {
        let failing = DocumentRequest::new(
            DocumentRequestId::new([3; 16]).unwrap(),
            RequestContext::new().with_result_limits(ResultLimits::new(10, 16).unwrap()),
            empty(),
        );
        assert_eq!(
            engine
                .execute_document(&session, failing)
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
    }
    let mut owners = Vec::new();
    for _ in 0..4 {
        let owner = engine.session();
        for _ in 0..8 {
            call(&engine, &owner, empty()).await;
        }
        assert_eq!(
            engine
                .execute_document(&owner, request(empty()))
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        owners.push(owner);
    }
    assert_eq!(
        engine
            .execute_document(&session, request(empty()))
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    owners.remove(0).close().await.unwrap();
    for _ in 0..8 {
        call(&engine, &session, empty()).await;
    }
    drop(owners.remove(0));
    let new_owner = engine.session();
    for _ in 0..8 {
        call(&engine, &new_owner, empty()).await;
    }
    engine.shutdown().await.unwrap();
}
