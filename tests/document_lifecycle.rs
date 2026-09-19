#![cfg(feature = "documents")]

use briskdb::{
    core::{CancellationToken, Engine, EngineErrorKind, RequestContext, ResultLimits, Session},
    document::{
        BsonDocument, BsonValue, DocumentAggregateRequest, DocumentCollectionMetadata,
        DocumentCollectionOptions, DocumentCommand, DocumentContinueCursorRequest,
        DocumentCreateCollectionRequest, DocumentDropCollectionRequest,
        DocumentDropDatabaseRequest, DocumentFilter, DocumentFindRequest, DocumentInsertRequest,
        DocumentNamespace, DocumentPipeline, DocumentReadOptions, DocumentRequest,
        DocumentRequestId, DocumentResult, DocumentWriteOptions,
    },
};
use std::time::{Duration, Instant};

fn ns(database: &str, collection: &str) -> DocumentNamespace {
    DocumentNamespace::new(database, collection).unwrap()
}
fn request(command: DocumentCommand, context: RequestContext) -> DocumentRequest {
    DocumentRequest::new(DocumentRequestId::new([9; 16]).unwrap(), context, command)
}
async fn call(engine: &Engine, session: &Session, command: DocumentCommand) -> DocumentResult {
    engine
        .execute_document(session, request(command, RequestContext::new()))
        .await
        .unwrap()
        .into_parts()
        .2
}
async fn create(
    engine: &Engine,
    session: &Session,
    namespace: &DocumentNamespace,
) -> DocumentCollectionMetadata {
    let DocumentResult::Collection(collection) = call(
        engine,
        session,
        DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
            namespace.clone(),
            DocumentCollectionOptions::empty(),
            DocumentWriteOptions::new(),
        )),
    )
    .await
    else {
        panic!("collection");
    };
    collection
}
fn drop_collection(namespace: &DocumentNamespace) -> DocumentCommand {
    DocumentCommand::DropCollection(DocumentDropCollectionRequest::new(
        namespace.clone(),
        DocumentWriteOptions::new(),
    ))
}
fn drop_database(database: &str) -> DocumentCommand {
    DocumentCommand::DropDatabase(
        DocumentDropDatabaseRequest::new(database, DocumentWriteOptions::new()).unwrap(),
    )
}
async fn insert(engine: &Engine, session: &Session, namespace: &DocumentNamespace, start: i64) {
    let rows: Vec<_> = (start..start + 40)
        .map(|id| BsonDocument::from_entries([("_id", BsonValue::Int64(id))]).unwrap())
        .collect();
    call(
        engine,
        session,
        DocumentCommand::Insert(
            DocumentInsertRequest::new(namespace.clone(), rows, DocumentWriteOptions::new())
                .unwrap(),
        ),
    )
    .await;
}
async fn find(
    engine: &Engine,
    session: &Session,
    namespace: &DocumentNamespace,
) -> Vec<BsonDocument> {
    let DocumentResult::Cursor(batch) = call(
        engine,
        session,
        DocumentCommand::Find(DocumentFindRequest::new(
            namespace.clone(),
            DocumentFilter::empty(),
            DocumentReadOptions::new(),
        )),
    )
    .await
    else {
        panic!("cursor");
    };
    assert!(batch.cursor_id().is_none());
    batch.documents().to_vec()
}

#[tokio::test]
async fn drops_are_scoped_durable_and_never_reuse_namespace_identity() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let one = ns("app", "one");
    let two = ns("app", "two");
    let other = ns("other", "one");
    assert_eq!(
        call(&engine, &session, drop_collection(&one)).await,
        DocumentResult::NamespaceDropped(false)
    );
    assert_eq!(
        call(&engine, &session, drop_database("app")).await,
        DocumentResult::NamespaceDropped(false)
    );
    let first = create(&engine, &session, &one).await;
    create(&engine, &session, &two).await;
    let last = create(&engine, &session, &other).await;
    for namespace in [&one, &two, &other] {
        insert(&engine, &session, namespace, 0).await;
    }
    assert_eq!(
        call(&engine, &session, drop_collection(&one)).await,
        DocumentResult::NamespaceDropped(true)
    );
    assert_eq!(find(&engine, &session, &two).await.len(), 40);
    assert_eq!(find(&engine, &session, &other).await.len(), 40);
    let recreated = create(&engine, &session, &one).await;
    assert!(recreated.id() > last.id());
    assert_ne!(recreated.id(), first.id());
    assert!(find(&engine, &session, &one).await.is_empty());
    assert_eq!(
        call(&engine, &session, drop_database("app")).await,
        DocumentResult::NamespaceDropped(true)
    );
    assert_eq!(find(&engine, &session, &other).await.len(), 40);
    assert_eq!(
        call(&engine, &session, drop_database("other")).await,
        DocumentResult::NamespaceDropped(true)
    );
    engine.shutdown().await.unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let fresh = create(&engine, &session, &one).await;
    assert!(fresh.id() > recreated.id());
    assert!(fresh.database_id() > last.database_id());
    assert!(find(&engine, &session, &one).await.is_empty());
    insert(&engine, &session, &one, 100).await;
    assert_eq!(find(&engine, &session, &one).await.len(), 40);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn retained_find_and_aggregate_cursors_cannot_cross_a_drop_recreate_boundary() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let peer = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let peer_session = peer.session();
    let namespace = ns("app", "items");
    create(&engine, &session, &namespace).await;
    insert(&engine, &session, &namespace, 0).await;
    for aggregate in [false, true] {
        let options = DocumentReadOptions::new().with_batch_size(1).unwrap();
        let command = if aggregate {
            DocumentCommand::Aggregate(
                DocumentAggregateRequest::new(
                    namespace.clone(),
                    DocumentPipeline::new(vec![]).unwrap(),
                    options,
                )
                .unwrap(),
            )
        } else {
            DocumentCommand::Find(DocumentFindRequest::new(
                namespace.clone(),
                DocumentFilter::empty(),
                options,
            ))
        };
        let DocumentResult::Cursor(batch) = call(&peer, &peer_session, command).await else {
            panic!("cursor");
        };
        let id = batch.cursor_id().unwrap();
        call(&engine, &session, drop_collection(&namespace)).await;
        create(&engine, &session, &namespace).await;
        insert(&engine, &session, &namespace, 100).await;
        for _ in 0..2 {
            let command = DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
                namespace.clone(),
                id,
                DocumentReadOptions::new(),
            ));
            assert_eq!(
                peer.execute_document(&peer_session, request(command, RequestContext::new()))
                    .await
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::FailedPrecondition
            );
        }
    }
    peer.shutdown().await.unwrap();
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn drop_controls_and_result_preflight_leave_storage_unchanged() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("app", "items");
    create(&engine, &session, &namespace).await;
    insert(&engine, &session, &namespace, 0).await;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    for (context, kind) in [
        (
            RequestContext::new().with_cancellation_token(cancelled),
            EngineErrorKind::Cancelled,
        ),
        (
            RequestContext::new().with_deadline(Instant::now() - Duration::from_secs(1)),
            EngineErrorKind::DeadlineExceeded,
        ),
        (
            RequestContext::new().with_result_limits(ResultLimits::new(1, 1).unwrap()),
            EngineErrorKind::LimitExceeded,
        ),
    ] {
        assert_eq!(
            engine
                .execute_document(&session, request(drop_database("app"), context))
                .await
                .unwrap_err()
                .kind(),
            kind
        );
        assert_eq!(find(&engine, &session, &namespace).await.len(), 40);
    }
    engine.shutdown().await.unwrap();
}
