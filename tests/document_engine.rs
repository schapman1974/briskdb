#![cfg(feature = "documents")]

use std::time::{Duration, Instant};

use briskdb::{
    core::{CancellationToken, Engine, EngineErrorKind, RequestContext, ResultLimits, Session},
    document::{
        BsonDocument, BsonValue, DocumentAggregateRequest, DocumentCollectionExistsRequest,
        DocumentCollectionMetadata, DocumentCollectionOptions, DocumentCommand,
        DocumentCountRequest, DocumentCreateCollectionRequest, DocumentCreateIndexRequest,
        DocumentDeleteRequest, DocumentDropIndexRequest, DocumentExecution, DocumentFilter,
        DocumentFindRequest, DocumentIndexError, DocumentIndexKeyGenerator, DocumentIndexLifecycle,
        DocumentIndexMetadata, DocumentIndexRequest, DocumentInsertRequest,
        DocumentListCollectionsRequest, DocumentListIndexesRequest, DocumentMutationScope,
        DocumentNamespace, DocumentPipeline, DocumentPlan, DocumentProjection, DocumentReadOptions,
        DocumentRequest, DocumentRequestId, DocumentResult, DocumentWriteOptions, encode_document,
    },
};
use rusqlite::Connection;

fn namespace() -> DocumentNamespace {
    DocumentNamespace::new("app", "events").unwrap()
}

fn request(seed: u8, context: RequestContext, command: DocumentCommand) -> DocumentRequest {
    DocumentRequest::new(
        DocumentRequestId::new([seed; 16]).unwrap(),
        context,
        command,
    )
}

fn document(id: BsonValue, label: &str) -> BsonDocument {
    BsonDocument::from_entries([
        ("_id", id),
        ("label", BsonValue::from(label)),
        ("sequence", BsonValue::Int64(i64::from(label.as_bytes()[0]))),
    ])
    .unwrap()
}

async fn create_collection(
    engine: &Engine,
    session: &Session,
    request_seed: u8,
) -> DocumentCollectionMetadata {
    let command = DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
        namespace(),
        DocumentCollectionOptions::empty(),
        DocumentWriteOptions::new(),
    ));
    let execution = engine
        .execute_document(
            session,
            request(request_seed, RequestContext::new(), command),
        )
        .await
        .unwrap();
    assert!(execution.plan().is_none());
    match execution.into_parts().2 {
        DocumentResult::Collection(collection) => collection,
        result => panic!("expected collection metadata, got {:?}", result.kind()),
    }
}

async fn insert(
    engine: &Engine,
    session: &Session,
    request_seed: u8,
    documents: Vec<BsonDocument>,
) -> DocumentExecution {
    let insert =
        DocumentInsertRequest::new(namespace(), documents, DocumentWriteOptions::new()).unwrap();
    engine
        .execute_document(
            session,
            request(
                request_seed,
                RequestContext::new(),
                DocumentCommand::Insert(insert),
            ),
        )
        .await
        .unwrap()
}

async fn declare_pending_index(engine: &Engine, session: &Session, name: &str) {
    let index = DocumentIndexRequest::new(
        BsonDocument::from_entries([("label", BsonValue::Int32(1))]).unwrap(),
    )
    .unwrap()
    .with_name(name)
    .unwrap();
    engine
        .execute_document(
            session,
            request(
                90,
                RequestContext::new(),
                DocumentCommand::CreateIndex(DocumentCreateIndexRequest::new(
                    namespace(),
                    index,
                    DocumentWriteOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
}

async fn pending_indexes(engine: &Engine, session: &Session) -> Box<[DocumentIndexMetadata]> {
    let result = engine
        .execute_document(
            session,
            request(
                91,
                RequestContext::new(),
                DocumentCommand::ListIndexes(DocumentListIndexesRequest::new(
                    namespace(),
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    let DocumentResult::Indexes(indexes) = result.into_parts().2 else {
        panic!("indexes")
    };
    indexes
}

fn drop_index(name: &str) -> DocumentCommand {
    DocumentCommand::DropIndex(
        DocumentDropIndexRequest::new(namespace(), name, DocumentWriteOptions::new()).unwrap(),
    )
}

fn create_index_command(index: DocumentIndexRequest) -> DocumentCommand {
    DocumentCommand::CreateIndex(DocumentCreateIndexRequest::new(
        namespace(),
        index,
        DocumentWriteOptions::new(),
    ))
}

fn partial_index_filter() -> BsonDocument {
    BsonDocument::from_entries([
        (
            "sequence",
            BsonValue::Document(
                BsonDocument::from_entries([("$gte", BsonValue::Int64(0))]).unwrap(),
            ),
        ),
        ("label", BsonValue::from("same")),
    ])
    .unwrap()
}

#[tokio::test]
async fn sparse_partial_declarations_preserve_options_ids_and_exact_bson_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
    create_collection(&engine, &session, 1).await;
    let keys = BsonDocument::from_entries([
        ("label", BsonValue::Int64(1)),
        ("sequence", BsonValue::Double(-1.0)),
    ])
    .unwrap();
    let sparse = DocumentIndexRequest::new(keys.clone())
        .unwrap()
        .with_unique(true)
        .with_sparse(true);
    let partial = DocumentIndexRequest::new(keys)
        .unwrap()
        .with_name("partial")
        .unwrap()
        .with_unique(true)
        .with_partial_filter(DocumentFilter::new(partial_index_filter()).unwrap());
    for index in [&sparse, &partial] {
        engine
            .execute_document(
                &session,
                request(
                    2,
                    RequestContext::new(),
                    create_index_command(index.clone()),
                ),
            )
            .await
            .unwrap();
    }
    let before = pending_indexes(&engine, &session).await;
    let specs: Vec<_> = before
        .iter()
        .map(|index| encode_document(index.specification()).unwrap())
        .collect();
    assert_eq!(before.len(), 3);
    for index in before.iter().filter(|index| !index.is_built_in()) {
        assert_eq!(index.lifecycle(), DocumentIndexLifecycle::PendingBuild);
        assert!(index.is_unique());
        let definition = index.definition().unwrap();
        assert_eq!(
            definition
                .keys()
                .iter()
                .map(|(key, _)| key)
                .collect::<Vec<_>>(),
            ["label", "sequence"]
        );
        if index.name() == "partial" {
            assert!(!definition.sparse());
            assert_eq!(
                encode_document(definition.partial_filter().unwrap()).unwrap(),
                encode_document(&partial_index_filter()).unwrap()
            );
        } else {
            assert_eq!(index.name(), "label_1_sequence_-1");
            assert!(definition.sparse());
            assert!(definition.partial_filter().is_none());
        }
        let generator = DocumentIndexKeyGenerator::compile(
            definition.keys(),
            definition.sparse(),
            definition.partial_filter(),
        )
        .unwrap();
        assert!(generator.keys(&BsonDocument::new()).unwrap().is_empty());
        assert_eq!(
            generator
                .keys(&document(BsonValue::Int32(1), "same"))
                .unwrap()
                .len(),
            1
        );
    }
    // Same normalized definitions retain both identity and exact stored bytes.
    for index in [&sparse, &partial] {
        engine
            .execute_document(
                &session,
                request(
                    3,
                    RequestContext::new(),
                    create_index_command(index.clone()),
                ),
            )
            .await
            .unwrap();
    }
    assert_eq!(pending_indexes(&engine, &session).await, before);
    for conflict in [
        sparse.clone().with_sparse(false),
        partial.clone().with_unique(false),
    ] {
        assert_eq!(
            engine
                .execute_document(
                    &session,
                    request(4, RequestContext::new(), create_index_command(conflict))
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(pending_indexes(&engine, &session).await, before);
    }
    // Declaring a unique index still does not activate it or reject documents.
    let inserted = insert(
        &engine,
        &session,
        5,
        vec![
            document(BsonValue::Int32(1), "same"),
            document(BsonValue::Int32(2), "same"),
        ],
    )
    .await;
    let DocumentResult::Insert(result) = inserted.result() else {
        panic!("insert")
    };
    assert_eq!(result.inserted_ids().len(), 2);
    assert!(result.write_errors().is_empty());
    engine.shutdown().await.unwrap();
    drop(session);
    drop(engine);
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
    let reopened = pending_indexes(&engine, &session).await;
    assert_eq!(reopened, before);
    assert_eq!(
        reopened
            .iter()
            .map(|index| encode_document(index.specification()).unwrap())
            .collect::<Vec<_>>(),
        specs
    );
    // The metadata-only drop path also removes advanced envelopes unchanged.
    engine
        .execute_document(
            &session,
            request(6, RequestContext::new(), drop_index("partial")),
        )
        .await
        .unwrap();
    assert_eq!(pending_indexes(&engine, &session).await.len(), 2);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn sparse_partial_validation_limits_and_controls_leave_no_declaration() {
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
    create_collection(&engine, &session, 1).await;
    let before = pending_indexes(&engine, &session).await;
    let base = DocumentIndexRequest::new(
        BsonDocument::from_entries([("label", BsonValue::Int32(1))]).unwrap(),
    )
    .unwrap()
    .with_name("bounded")
    .unwrap();
    let invalid = BsonDocument::from_entries([(
        "private",
        BsonValue::Document(BsonDocument::from_entries([("$ne", BsonValue::Int32(1))]).unwrap()),
    )])
    .unwrap();
    let invalid_branch = BsonDocument::from_entries([(
        "$or",
        BsonValue::Array(vec![
            BsonValue::Document(partial_index_filter()),
            BsonValue::Document(invalid.clone()),
        ]),
    )])
    .unwrap();
    for index in [
        base.clone()
            .with_sparse(true)
            .with_partial_filter(DocumentFilter::new(partial_index_filter()).unwrap()),
        base.clone().with_partial_filter(DocumentFilter::empty()),
        base.clone()
            .with_partial_filter(DocumentFilter::new(invalid).unwrap()),
        base.clone()
            .with_partial_filter(DocumentFilter::new(invalid_branch).unwrap()),
    ] {
        let error = engine
            .execute_document(
                &session,
                request(2, RequestContext::new(), create_index_command(index)),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Unsupported);
        assert!(!error.to_string().contains("private"));
        assert_eq!(pending_indexes(&engine, &session).await, before);
    }
    let valid = base.with_partial_filter(DocumentFilter::new(partial_index_filter()).unwrap());
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    for (context, kind) in [
        (
            RequestContext::new().with_result_limits(ResultLimits::new(1, 1).unwrap()),
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
                    request(3, context, create_index_command(valid.clone()))
                )
                .await
                .unwrap_err()
                .kind(),
            kind
        );
        assert_eq!(pending_indexes(&engine, &session).await, before);
    }
    let blocker = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let start = Instant::now();
    let result = engine
        .execute_document(
            &session,
            request(
                4,
                RequestContext::new()
                    .with_timeout(Duration::from_millis(50))
                    .unwrap(),
                create_index_command(valid),
            ),
        )
        .await;
    assert_eq!(
        result.unwrap_err().kind(),
        EngineErrorKind::DeadlineExceeded
    );
    assert!(start.elapsed() < Duration::from_secs(1));
    blocker.execute_batch("ROLLBACK").unwrap();
    assert_eq!(pending_indexes(&engine, &session).await, before);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn pending_index_drop_is_exact_bounded_protected_and_persistent() {
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
    create_collection(&engine, &session, 1).await;
    insert(
        &engine,
        &session,
        2,
        vec![document(BsonValue::Int32(1), "kept")],
    )
    .await;
    for name in ["label_1", "keep", "*"] {
        declare_pending_index(&engine, &session, name).await;
    }
    let before = pending_indexes(&engine, &session).await;
    for (name, expected) in [
        ("_id", DocumentIndexError::Protected),
        ("_id_", DocumentIndexError::Protected),
        ("label", DocumentIndexError::NotFound),
        ("LABEL_1", DocumentIndexError::NotFound),
        ("absent", DocumentIndexError::NotFound),
    ] {
        let error = engine
            .execute_document(
                &session,
                request(3, RequestContext::new(), drop_index(name)),
            )
            .await
            .unwrap_err();
        assert_eq!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<DocumentIndexError>(),
            Some(&expected)
        );
        assert_eq!(pending_indexes(&engine, &session).await, before);
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    for (context, kind) in [
        (
            RequestContext::new().with_result_limits(ResultLimits::new(1, 33).unwrap()),
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
        let error = engine
            .execute_document(&session, request(4, context, drop_index("label_1")))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), kind);
        assert_eq!(pending_indexes(&engine, &session).await, before);
    }
    // '*' is an exact native name here, never a bulk-removal selector.
    let result = engine
        .execute_document(
            &session,
            request(
                5,
                RequestContext::new().with_result_limits(ResultLimits::new(1, 34).unwrap()),
                drop_index("*"),
            ),
        )
        .await
        .unwrap();
    assert_eq!(result.result(), &DocumentResult::Acknowledged(true));
    let remaining = pending_indexes(&engine, &session).await;
    assert_eq!(
        remaining.iter().map(|i| i.name()).collect::<Vec<_>>(),
        ["_id_", "keep", "label_1"]
    );
    engine.shutdown().await.unwrap();
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
    assert_eq!(pending_indexes(&engine, &session).await, remaining);
    let old_id = before.iter().find(|i| i.name() == "*").unwrap().id();
    declare_pending_index(&engine, &session, "*").await;
    assert!(
        pending_indexes(&engine, &session)
            .await
            .iter()
            .find(|i| i.name() == "*")
            .unwrap()
            .id()
            > old_id
    );
    let connection = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT count(*) FROM briskdb_document_index_identities",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        4
    );
    let count = engine
        .execute_document(
            &session,
            request(
                6,
                RequestContext::new(),
                DocumentCommand::Count(DocumentCountRequest::new(
                    namespace(),
                    DocumentFilter::empty(),
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    assert_eq!(count.result(), &DocumentResult::Count(1));
}

#[tokio::test]
async fn pending_index_drop_lock_wait_deadline_leaves_catalog_unchanged() {
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
    create_collection(&engine, &session, 1).await;
    declare_pending_index(&engine, &session, "label_1").await;
    let before = pending_indexes(&engine, &session).await;
    let blocker = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let start = Instant::now();
    let error = engine
        .execute_document(
            &session,
            request(
                7,
                RequestContext::new().with_deadline(start + Duration::from_millis(50)),
                drop_index("label_1"),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
    assert!(start.elapsed() < Duration::from_secs(1));
    blocker.execute_batch("ROLLBACK").unwrap();
    assert_eq!(pending_indexes(&engine, &session).await, before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_pending_index_drops_commit_exactly_once() {
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
    create_collection(&engine, &session, 1).await;
    declare_pending_index(&engine, &session, "label_1").await;
    let mut workers = tokio::task::JoinSet::new();
    for n in 0..4 {
        let engine = engine.clone();
        workers.spawn(async move {
            engine
                .execute_document(
                    &engine.session(),
                    request(10 + n, RequestContext::new(), drop_index("label_1")),
                )
                .await
        });
    }
    let mut successes = 0;
    while let Some(result) = workers.join_next().await {
        match result.unwrap() {
            Ok(result) => {
                assert_eq!(result.result(), &DocumentResult::Acknowledged(true));
                successes += 1;
            }
            Err(error) => assert_eq!(
                std::error::Error::source(&error)
                    .unwrap()
                    .downcast_ref::<DocumentIndexError>(),
                Some(&DocumentIndexError::NotFound)
            ),
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(pending_indexes(&engine, &session).await.len(), 1);
}

#[tokio::test]
async fn index_definitions_normalize_names_before_catalog_changes_and_survive_restart() {
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
    create_collection(&engine, &session, 1).await;
    let keys = BsonDocument::from_entries([
        ("profile.name", BsonValue::Int64(1)),
        ("rank", BsonValue::Double(-1.0)),
    ])
    .unwrap();
    for supplied_name in [None, Some("profile.name_1_rank_-1")] {
        let mut index = DocumentIndexRequest::new(keys.clone())
            .unwrap()
            .with_unique(true);
        if let Some(name) = supplied_name {
            index = index.with_name(name).unwrap();
        }
        let execution = engine
            .execute_document(
                &session,
                request(
                    2,
                    RequestContext::new(),
                    DocumentCommand::CreateIndex(DocumentCreateIndexRequest::new(
                        namespace(),
                        index,
                        DocumentWriteOptions::new(),
                    )),
                ),
            )
            .await
            .unwrap();
        assert!(
            matches!(execution.result(), DocumentResult::IndexName(name) if name == "profile.name_1_rank_-1")
        );
    }
    for (key, direction) in [
        ("bad..path", BsonValue::Int32(1)),
        ("valid", BsonValue::Boolean(true)),
        ("valid", BsonValue::from("hashed")),
    ] {
        let index =
            DocumentIndexRequest::new(BsonDocument::from_entries([(key, direction)]).unwrap())
                .unwrap()
                .with_name("must_not_exist")
                .unwrap();
        let error = engine
            .execute_document(
                &session,
                request(
                    3,
                    RequestContext::new(),
                    DocumentCommand::CreateIndex(DocumentCreateIndexRequest::new(
                        namespace(),
                        index,
                        DocumentWriteOptions::new(),
                    )),
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
    }
    let conflict = DocumentIndexRequest::new(keys)
        .unwrap()
        .with_name("profile.name_1_rank_-1")
        .unwrap();
    assert_eq!(
        engine
            .execute_document(
                &session,
                request(
                    4,
                    RequestContext::new(),
                    DocumentCommand::CreateIndex(DocumentCreateIndexRequest::new(
                        namespace(),
                        conflict,
                        DocumentWriteOptions::new()
                    ))
                )
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
    drop(session);
    drop(engine);
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let execution = engine
        .execute_document(
            &engine.session(),
            request(
                5,
                RequestContext::new(),
                DocumentCommand::ListIndexes(DocumentListIndexesRequest::new(
                    namespace(),
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    let DocumentResult::Indexes(indexes) = execution.result() else {
        panic!("indexes")
    };
    assert_eq!(indexes.len(), 2);
    let index = indexes.iter().find(|index| !index.is_built_in()).unwrap();
    assert_eq!(index.name(), "profile.name_1_rank_-1");
    assert!(index.is_unique());
    assert_eq!(index.lifecycle(), DocumentIndexLifecycle::PendingBuild);
    assert!(
        index.specification().representation_eq(
            &BsonDocument::from_entries([
                ("profile.name", BsonValue::Int32(1)),
                ("rank", BsonValue::Int32(-1))
            ])
            .unwrap()
        )
    );
}

#[tokio::test]
async fn collection_existence_is_targeted_bounded_read_only_and_persistent() {
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
    create_collection(&engine, &session, 1).await;
    // More than one default catalog page, with metadata far larger than the
    // scalar result budget. Neither may interfere with a targeted probe.
    for index in 0..102 {
        let options = if index == 0 {
            DocumentCollectionOptions::new(
                BsonDocument::from_entries([("opaque", BsonValue::from("x".repeat(32 * 1024)))])
                    .unwrap(),
            )
            .unwrap()
        } else {
            DocumentCollectionOptions::empty()
        };
        engine
            .execute_document(
                &session,
                request(
                    2,
                    RequestContext::new(),
                    DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                        DocumentNamespace::new("app", format!("other_{index}")).unwrap(),
                        options,
                        DocumentWriteOptions::new(),
                    )),
                ),
            )
            .await
            .unwrap();
    }
    let probe = |database, collection| {
        DocumentCommand::CollectionExists(DocumentCollectionExistsRequest::new(
            DocumentNamespace::new(database, collection).unwrap(),
        ))
    };
    for (database, collection, expected) in [
        ("app", "events", true),
        ("app", "other_0", true),
        ("app", "missing", false),
        ("missing", "events", false),
        ("app", "Events", false),
    ] {
        let result = engine
            .execute_document(
                &session,
                request(
                    3,
                    RequestContext::new().with_result_limits(ResultLimits::new(1, 34).unwrap()),
                    probe(database, collection),
                ),
            )
            .await
            .unwrap();
        assert!(result.plan().is_none());
        assert_eq!(result.result(), &DocumentResult::CollectionExists(expected));
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    for (context, kind) in [
        (
            RequestContext::new().with_result_limits(ResultLimits::new(1, 33).unwrap()),
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
                .execute_document(&session, request(4, context, probe("app", "events")))
                .await
                .unwrap_err()
                .kind(),
            kind
        );
    }
    let listed = engine
        .execute_document(
            &session,
            request(
                5,
                RequestContext::new(),
                DocumentCommand::ListCollections(
                    DocumentListCollectionsRequest::new(
                        "app",
                        DocumentReadOptions::new().with_batch_size(200).unwrap(),
                    )
                    .unwrap(),
                ),
            ),
        )
        .await
        .unwrap();
    assert!(matches!(listed.result(), DocumentResult::Collections(items) if items.len() == 103));
    session.close().await.unwrap();
    assert_eq!(
        engine
            .execute_document(
                &session,
                request(6, RequestContext::new(), probe("app", "events"))
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
    engine.shutdown().await.unwrap();
    let reopened = Engine::open(temp.path(), 2).await.unwrap();
    for (collection, expected) in [("events", true), ("other_101", true), ("missing", false)] {
        let result = reopened
            .execute_document(
                &reopened.session(),
                request(7, RequestContext::new(), probe("app", collection)),
            )
            .await
            .unwrap();
        assert_eq!(result.result(), &DocumentResult::CollectionExists(expected));
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn catalog_and_index_commands_run_without_a_listener() {
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 4).await.unwrap();
    let session = engine.session();

    let created = create_collection(&engine, &session, 1).await;
    assert_eq!(created.namespace(), "app.events");
    assert_eq!(created.indexes().len(), 1);
    assert_eq!(created.indexes()[0].name(), "_id_");
    assert!(created.indexes()[0].is_built_in());
    assert_eq!(
        created.indexes()[0].lifecycle(),
        DocumentIndexLifecycle::Ready
    );

    let listed = engine
        .execute_document(
            &session,
            request(
                2,
                RequestContext::new(),
                DocumentCommand::ListCollections(
                    DocumentListCollectionsRequest::new("app", DocumentReadOptions::new()).unwrap(),
                ),
            ),
        )
        .await
        .unwrap();
    assert!(listed.plan().is_none());
    match listed.result() {
        DocumentResult::Collections(collections) => {
            assert_eq!(collections.len(), 1);
            assert_eq!(collections[0].id(), created.id());
            assert_eq!(collections[0].namespace(), "app.events");
        }
        result => panic!("expected collections, got {:?}", result.kind()),
    }

    let keys = BsonDocument::from_entries([("label", BsonValue::Int32(1))]).unwrap();
    let index = DocumentIndexRequest::new(keys)
        .unwrap()
        .with_name("label_1")
        .unwrap();
    let created_index = engine
        .execute_document(
            &session,
            request(
                3,
                RequestContext::new(),
                DocumentCommand::CreateIndex(DocumentCreateIndexRequest::new(
                    namespace(),
                    index,
                    DocumentWriteOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    match created_index.result() {
        DocumentResult::IndexName(name) => assert_eq!(name, "label_1"),
        result => panic!("expected index name, got {:?}", result.kind()),
    }

    let listed_indexes = engine
        .execute_document(
            &session,
            request(
                4,
                RequestContext::new(),
                DocumentCommand::ListIndexes(DocumentListIndexesRequest::new(
                    namespace(),
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    match listed_indexes.result() {
        DocumentResult::Indexes(indexes) => {
            let secondary = indexes
                .iter()
                .find(|index| index.name() == "label_1")
                .expect("declared secondary index");
            assert!(!secondary.is_built_in());
            assert_eq!(secondary.lifecycle(), DocumentIndexLifecycle::PendingBuild);
            assert_eq!(
                secondary.specification().iter().next(),
                Some(("label", &BsonValue::Int32(1)))
            );
        }
        result => panic!("expected indexes, got {:?}", result.kind()),
    }

    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn insert_find_and_count_preserve_identity_routes_and_bson_order() {
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 4).await.unwrap();
    let session = engine.session();
    let collection = create_collection(&engine, &session, 10).await;

    let documents = [
        document(BsonValue::Int32(1), "a"),
        document(BsonValue::from("second"), "b"),
        document(BsonValue::Int64(3), "c"),
        document(BsonValue::from("fourth"), "d"),
    ];
    for (offset, source) in documents.iter().enumerate() {
        let inserted = insert(
            &engine,
            &session,
            50 + u8::try_from(offset).unwrap(),
            vec![source.clone()],
        )
        .await;
        match inserted.result() {
            DocumentResult::Insert(result) => {
                assert_eq!(result.inserted_ids().len(), 1);
                assert!(
                    result.inserted_ids()[0]
                        .representation_eq(source.get_unique("_id").unwrap().expect("source _id"))
                );
            }
            result => panic!("expected insert result, got {:?}", result.kind()),
        }
    }

    // Mongo numeric equality makes this double filter select the Int32 ID,
    // while the returned document retains its original Int32 representation.
    let exact_filter =
        DocumentFilter::new(BsonDocument::from_entries([("_id", BsonValue::Double(1.0))]).unwrap())
            .unwrap();
    let exact_request_id = DocumentRequestId::new([12; 16]).unwrap();
    let exact = engine
        .execute_document(
            &session,
            DocumentRequest::new(
                exact_request_id,
                RequestContext::new(),
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace(),
                    exact_filter,
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    assert_eq!(exact.request_id(), exact_request_id);
    match exact.plan().expect("exact-ID route") {
        DocumentPlan::Point(plan) => {
            assert_eq!(plan.collection_id(), collection.id());
            assert!(plan.shard() < 4);
        }
        plan => panic!("expected a point plan, got {plan:?}"),
    }
    match exact.result() {
        DocumentResult::Cursor(batch) => {
            assert!(batch.is_exhausted());
            assert_eq!(batch.namespace(), &namespace());
            assert_eq!(batch.documents().len(), 1);
            assert!(batch.documents()[0].representation_eq(&documents[0]));
            assert!(matches!(
                batch.documents()[0].get_unique("_id").unwrap(),
                Some(BsonValue::Int32(1))
            ));
            assert_eq!(
                batch.documents()[0]
                    .iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
                vec!["_id", "label", "sequence"]
            );
        }
        result => panic!("expected cursor, got {:?}", result.kind()),
    }

    let options = DocumentReadOptions::new()
        .with_skip(1)
        .with_limit(2)
        .unwrap();
    let scattered = engine
        .execute_document(
            &session,
            request(
                13,
                RequestContext::new(),
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace(),
                    DocumentFilter::empty(),
                    options,
                )),
            ),
        )
        .await
        .unwrap();
    match scattered.plan().expect("empty-filter route") {
        DocumentPlan::Scatter(plan) => assert_eq!(plan.shards(), &[0, 1, 2, 3]),
        plan => panic!("expected a scatter plan, got {plan:?}"),
    }
    match scattered.result() {
        DocumentResult::Cursor(batch) => {
            assert_eq!(batch.documents().len(), 2);
            assert!(batch.documents()[0].representation_eq(&documents[1]));
            assert!(batch.documents()[1].representation_eq(&documents[2]));
        }
        result => panic!("expected cursor, got {:?}", result.kind()),
    }

    let count = engine
        .execute_document(
            &session,
            request(
                14,
                RequestContext::new(),
                DocumentCommand::Count(DocumentCountRequest::new(
                    namespace(),
                    DocumentFilter::empty(),
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    assert!(matches!(count.plan(), Some(DocumentPlan::Scatter(_))));
    assert!(matches!(count.result(), DocumentResult::Count(4)));

    let deleted = engine
        .execute_document(
            &session,
            request(
                15,
                RequestContext::new(),
                DocumentCommand::Delete(DocumentDeleteRequest::new(
                    namespace(),
                    DocumentFilter::new(
                        BsonDocument::from_entries([("_id", BsonValue::Int32(1))]).unwrap(),
                    )
                    .unwrap(),
                    DocumentMutationScope::One,
                    DocumentWriteOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    assert!(matches!(deleted.plan(), Some(DocumentPlan::Point(_))));
    match deleted.result() {
        DocumentResult::Delete(result) => assert_eq!(result.deleted_count(), 1),
        result => panic!("expected delete result, got {:?}", result.kind()),
    }

    let missing = engine
        .execute_document(
            &session,
            request(
                16,
                RequestContext::new(),
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace(),
                    DocumentFilter::new(
                        BsonDocument::from_entries([("_id", BsonValue::Int64(1))]).unwrap(),
                    )
                    .unwrap(),
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    match missing.result() {
        DocumentResult::Cursor(batch) => assert!(batch.documents().is_empty()),
        result => panic!("expected cursor, got {:?}", result.kind()),
    }

    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn request_controls_and_session_ownership_are_enforced() {
    let first_temp = tempfile::tempdir().unwrap();
    let second_temp = tempfile::tempdir().unwrap();
    let first = Engine::open(first_temp.path(), 2).await.unwrap();
    let second = Engine::open(second_temp.path(), 2).await.unwrap();
    let first_session = first.session();
    let foreign_session = second.session();

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let error = first
        .execute_document(
            &first_session,
            request(
                20,
                RequestContext::new().with_cancellation_token(cancelled),
                DocumentCommand::ListCollections(
                    DocumentListCollectionsRequest::new("app", DocumentReadOptions::new()).unwrap(),
                ),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Cancelled);

    let error = first
        .execute_document(
            &first_session,
            request(
                21,
                RequestContext::new().with_deadline(Instant::now() - Duration::from_millis(1)),
                DocumentCommand::ListCollections(
                    DocumentListCollectionsRequest::new("app", DocumentReadOptions::new()).unwrap(),
                ),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);

    let error = first
        .execute_document(
            &foreign_session,
            request(
                22,
                RequestContext::new(),
                DocumentCommand::ListCollections(
                    DocumentListCollectionsRequest::new("app", DocumentReadOptions::new()).unwrap(),
                ),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);

    first_session.close().await.unwrap();
    let error = first
        .execute_document(
            &first_session,
            request(
                23,
                RequestContext::new(),
                DocumentCommand::ListCollections(
                    DocumentListCollectionsRequest::new("app", DocumentReadOptions::new()).unwrap(),
                ),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);

    second.begin_shutdown();
    let error = second
        .execute_document(
            &foreign_session,
            request(
                24,
                RequestContext::new(),
                DocumentCommand::ListCollections(
                    DocumentListCollectionsRequest::new("app", DocumentReadOptions::new()).unwrap(),
                ),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::ShuttingDown);

    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn manifest_write_lock_waits_honor_document_deadlines() {
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
    create_collection(&engine, &session, 25).await;

    let blocker = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

    let index = DocumentIndexRequest::new(
        BsonDocument::from_entries([("label", BsonValue::Int32(1))]).unwrap(),
    )
    .unwrap()
    .with_name("deadline_index")
    .unwrap();
    let started = Instant::now();
    let error = engine
        .execute_document(
            &session,
            request(
                26,
                RequestContext::new().with_deadline(Instant::now() + Duration::from_millis(50)),
                DocumentCommand::CreateIndex(DocumentCreateIndexRequest::new(
                    namespace(),
                    index,
                    DocumentWriteOptions::new(),
                )),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "document manifest lock wait ignored its deadline"
    );
    blocker.execute_batch("ROLLBACK").unwrap();

    let indexes = engine
        .execute_document(
            &session,
            request(
                27,
                RequestContext::new(),
                DocumentCommand::ListIndexes(DocumentListIndexesRequest::new(
                    namespace(),
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    match indexes.result() {
        DocumentResult::Indexes(indexes) => {
            assert!(indexes.iter().all(|index| index.name() != "deadline_index"));
        }
        result => panic!("expected indexes, got {:?}", result.kind()),
    }

    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn document_reads_fail_whole_when_result_limits_are_exceeded() {
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
    create_collection(&engine, &session, 30).await;
    insert(
        &engine,
        &session,
        31,
        vec![document(BsonValue::Int32(1), "a")],
    )
    .await;
    insert(
        &engine,
        &session,
        48,
        vec![document(BsonValue::Int32(2), "b")],
    )
    .await;

    let row_limited =
        RequestContext::new().with_result_limits(ResultLimits::new(1, 1_000_000).unwrap());
    let error = engine
        .execute_document(
            &session,
            request(
                32,
                row_limited,
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace(),
                    DocumentFilter::empty(),
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);

    let byte_limited =
        RequestContext::new().with_result_limits(ResultLimits::new(100, 16).unwrap());
    let error = engine
        .execute_document(
            &session,
            request(
                33,
                byte_limited,
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace(),
                    DocumentFilter::new(
                        BsonDocument::from_entries([("_id", BsonValue::Int32(1))]).unwrap(),
                    )
                    .unwrap(),
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);

    let mutation_limits =
        || RequestContext::new().with_result_limits(ResultLimits::new(100, 16).unwrap());
    let limited_namespace = DocumentNamespace::new("app", "limited").unwrap();
    let error = engine
        .execute_document(
            &session,
            request(
                34,
                mutation_limits(),
                DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                    limited_namespace,
                    DocumentCollectionOptions::empty(),
                    DocumentWriteOptions::new(),
                )),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);

    let index = DocumentIndexRequest::new(
        BsonDocument::from_entries([("label", BsonValue::Int32(1))]).unwrap(),
    )
    .unwrap()
    .with_name("must_not_be_created")
    .unwrap();
    let error = engine
        .execute_document(
            &session,
            request(
                35,
                mutation_limits(),
                DocumentCommand::CreateIndex(DocumentCreateIndexRequest::new(
                    namespace(),
                    index,
                    DocumentWriteOptions::new(),
                )),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);

    let error = engine
        .execute_document(
            &session,
            request(
                36,
                mutation_limits(),
                DocumentCommand::Delete(DocumentDeleteRequest::new(
                    namespace(),
                    DocumentFilter::new(
                        BsonDocument::from_entries([("_id", BsonValue::Int32(1))]).unwrap(),
                    )
                    .unwrap(),
                    DocumentMutationScope::One,
                    DocumentWriteOptions::new(),
                )),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);

    let oversized_plan_filter = DocumentFilter::new(
        BsonDocument::from_entries([("_id", BsonValue::from("x".repeat(1_024)))]).unwrap(),
    )
    .unwrap();
    let error = engine
        .execute_document(
            &session,
            request(
                37,
                RequestContext::new().with_result_limits(ResultLimits::new(100, 128).unwrap()),
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace(),
                    oversized_plan_filter,
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);

    let catalog = engine
        .execute_document(
            &session,
            request(
                38,
                RequestContext::new(),
                DocumentCommand::ListCollections(
                    DocumentListCollectionsRequest::new("app", DocumentReadOptions::new()).unwrap(),
                ),
            ),
        )
        .await
        .unwrap();
    match catalog.result() {
        DocumentResult::Collections(collections) => {
            assert_eq!(
                collections.len(),
                1,
                "limited create must not mutate catalog"
            )
        }
        result => panic!("expected collections, got {:?}", result.kind()),
    }
    let indexes = engine
        .execute_document(
            &session,
            request(
                39,
                RequestContext::new(),
                DocumentCommand::ListIndexes(DocumentListIndexesRequest::new(
                    namespace(),
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    match indexes.result() {
        DocumentResult::Indexes(indexes) => assert!(
            indexes
                .iter()
                .all(|index| index.name() != "must_not_be_created"),
            "limited index creation must not mutate catalog"
        ),
        result => panic!("expected indexes, got {:?}", result.kind()),
    }
    let retained = engine
        .execute_document(
            &session,
            request(
                45,
                RequestContext::new(),
                DocumentCommand::Count(DocumentCountRequest::new(
                    namespace(),
                    DocumentFilter::new(
                        BsonDocument::from_entries([("_id", BsonValue::Int64(1))]).unwrap(),
                    )
                    .unwrap(),
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    assert!(matches!(retained.result(), DocumentResult::Count(1)));

    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn unsupported_document_semantics_fail_at_the_engine_boundary() {
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 2).await.unwrap();
    let session = engine.session();
    create_collection(&engine, &session, 40).await;

    let general_filter = DocumentFilter::new(
        BsonDocument::from_entries([("$where", BsonValue::from("a"))]).unwrap(),
    )
    .unwrap();
    let error = engine
        .execute_document(
            &session,
            request(
                41,
                RequestContext::new(),
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace(),
                    general_filter,
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Unsupported);

    let id_operator = DocumentFilter::new(
        BsonDocument::from_entries([(
            "_id",
            BsonValue::Document(
                BsonDocument::from_entries([("$bitsAllSet", BsonValue::Int32(1))]).unwrap(),
            ),
        )])
        .unwrap(),
    )
    .unwrap();
    let error = engine
        .execute_document(
            &session,
            request(
                44,
                RequestContext::new(),
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace(),
                    id_operator,
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Unsupported);

    let insert = DocumentInsertRequest::new(
        namespace(),
        vec![document(BsonValue::Int32(99), "z")],
        DocumentWriteOptions::new().with_bypass_document_validation(true),
    )
    .unwrap();
    let error = engine
        .execute_document(
            &session,
            request(46, RequestContext::new(), DocumentCommand::Insert(insert)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Unsupported);

    let projection = DocumentProjection::new(
        BsonDocument::from_entries([(
            "label",
            BsonValue::Document(
                BsonDocument::from_entries([("$slice", BsonValue::Int32(1))]).unwrap(),
            ),
        )])
        .unwrap(),
    )
    .unwrap();
    let error = engine
        .execute_document(
            &session,
            request(
                42,
                RequestContext::new(),
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace(),
                    DocumentFilter::empty(),
                    DocumentReadOptions::new().with_projection(projection),
                )),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Unsupported);

    let error = engine
        .execute_document(
            &session,
            request(
                43,
                RequestContext::new(),
                DocumentCommand::Aggregate(
                    DocumentAggregateRequest::new(
                        namespace(),
                        DocumentPipeline::new(vec![
                            BsonDocument::from_entries([("$unsupported", BsonValue::Null)])
                                .unwrap(),
                        ])
                        .unwrap(),
                        DocumentReadOptions::new(),
                    )
                    .unwrap(),
                ),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Unsupported);

    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn insert_batches_preserve_partial_results_ordering_and_restart() {
    async fn route(engine: &Engine, session: &Session, id: i32) -> u16 {
        let found = engine
            .execute_document(
                session,
                request(
                    90,
                    RequestContext::new(),
                    DocumentCommand::Find(DocumentFindRequest::new(
                        namespace(),
                        DocumentFilter::new(
                            BsonDocument::from_entries([("_id", BsonValue::Int32(id))]).unwrap(),
                        )
                        .unwrap(),
                        DocumentReadOptions::new(),
                    )),
                ),
            )
            .await
            .unwrap();
        let Some(DocumentPlan::Point(plan)) = found.plan() else {
            panic!("expected point route");
        };
        plan.shard()
    }
    for (same_shard, ordered) in [(true, true), (true, false), (false, true), (false, false)] {
        let temp = tempfile::tempdir().unwrap();
        let engine = Engine::open(temp.path(), 4).await.unwrap();
        let session = engine.session();
        create_collection(&engine, &session, 1).await;
        let first_shard = route(&engine, &session, 1).await;
        let mut last_id = None;
        for id in 2..64 {
            if (route(&engine, &session, id).await == first_shard) == same_shard {
                last_id = Some(id);
                break;
            }
        }
        let last_id = last_id.expect("same-shard and different-shard IDs exist");
        let documents = vec![
            document(BsonValue::Int32(1), "a"),
            document(BsonValue::Double(1.0), "duplicate"),
            document(BsonValue::Int32(last_id), "c"),
        ];
        let execution = engine
            .execute_document(
                &session,
                request(
                    2,
                    RequestContext::new(),
                    DocumentCommand::Insert(
                        DocumentInsertRequest::new(
                            namespace(),
                            documents,
                            DocumentWriteOptions::new().with_ordered(ordered),
                        )
                        .unwrap(),
                    ),
                ),
            )
            .await
            .unwrap();
        let DocumentResult::Insert(result) = execution.result() else {
            panic!("expected insert result");
        };
        assert_eq!(
            result.inserted_ids(),
            if ordered {
                vec![BsonValue::Int32(1)]
            } else {
                vec![BsonValue::Int32(1), BsonValue::Int32(last_id)]
            }
        );
        assert_eq!(result.write_errors().len(), 1);
        let Some(DocumentPlan::Scatter(plan)) = execution.plan() else {
            panic!("expected batch plan");
        };
        assert_eq!(plan.shards().len(), if same_shard { 1 } else { 2 });
        assert_eq!(result.write_errors()[0].index(), 1);
        assert_eq!(
            result.write_errors()[0].kind(),
            EngineErrorKind::UniqueViolation
        );
        engine.shutdown().await.unwrap();
        let engine = Engine::open(temp.path(), 4).await.unwrap();
        let session = engine.session();
        let rows = engine
            .execute_document(
                &session,
                request(
                    3,
                    RequestContext::new(),
                    DocumentCommand::Find(DocumentFindRequest::new(
                        namespace(),
                        DocumentFilter::empty(),
                        DocumentReadOptions::new(),
                    )),
                ),
            )
            .await
            .unwrap();
        let DocumentResult::Cursor(rows) = rows.result() else {
            panic!("expected cursor");
        };
        assert_eq!(rows.documents().len(), if ordered { 1 } else { 2 });
        assert_eq!(
            rows.documents()[0].get_first("label"),
            Some(&BsonValue::from("a"))
        );
        // A batch may legitimately have zero successes and a first-item error.
        let result = engine
            .execute_document(
                &session,
                request(
                    4,
                    RequestContext::new(),
                    DocumentCommand::Insert(
                        DocumentInsertRequest::new(
                            namespace(),
                            vec![
                                document(BsonValue::Int32(1), "d"),
                                document(BsonValue::Int32(3), "e"),
                            ],
                            DocumentWriteOptions::new(),
                        )
                        .unwrap(),
                    ),
                ),
            )
            .await
            .unwrap();
        let DocumentResult::Insert(result) = result.result() else {
            panic!("expected insert result");
        };
        assert!(result.inserted_ids().is_empty());
        assert_eq!(result.write_errors()[0].index(), 0);
        engine.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn insert_normalizes_missing_ids_and_only_direct_zero_timestamps() {
    use briskdb::document::BsonTimestamp;
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 4).await.unwrap();
    let session = engine.session();
    create_collection(&engine, &session, 1).await;
    let zero = BsonValue::Timestamp(BsonTimestamp::new(0, 0));
    let source = BsonDocument::from_entries([
        ("first", zero.clone()),
        ("second", zero.clone()),
        (
            "nested",
            BsonValue::Document(BsonDocument::from_entries([("value", zero.clone())]).unwrap()),
        ),
        ("array", BsonValue::Array(vec![zero.clone()])),
    ])
    .unwrap();
    let result = insert(
        &engine,
        &session,
        2,
        vec![
            source.clone(),
            BsonDocument::from_entries([("_id", BsonValue::Null), ("stamp", zero.clone())])
                .unwrap(),
            BsonDocument::from_entries([("_id", zero.clone()), ("stamp", zero.clone())]).unwrap(),
        ],
    )
    .await;
    let DocumentResult::Insert(result) = result.result() else {
        panic!("expected insert result");
    };
    assert!(matches!(result.inserted_ids()[0], BsonValue::ObjectId(_)));
    assert_eq!(result.inserted_ids()[1], BsonValue::Null);
    assert_eq!(result.inserted_ids()[2], zero);
    assert!(source.get_first("_id").is_none());
    assert_eq!(source.get_first("first"), Some(&zero));
    let rows = engine
        .execute_document(
            &session,
            request(
                3,
                RequestContext::new(),
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace(),
                    DocumentFilter::empty(),
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    let DocumentResult::Cursor(rows) = rows.result() else {
        panic!("expected cursor");
    };
    let first = &rows.documents()[0];
    assert_eq!(first.iter().next().unwrap().0, "_id");
    assert_ne!(first.get_first("first"), Some(&zero));
    assert_ne!(first.get_first("first"), first.get_first("second"));
    assert_eq!(first.get_first("nested"), source.get_first("nested"));
    assert_eq!(first.get_first("array"), source.get_first("array"));
    assert_ne!(rows.documents()[2].get_first("stamp"), Some(&zero));
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn matcher_filters_before_global_pagination_and_count_and_reopens() {
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 4).await.unwrap();
    let session = engine.session();
    create_collection(&engine, &session, 1).await;
    insert(
        &engine,
        &session,
        2,
        (0..24)
            .map(|number| {
                BsonDocument::from_entries([
                    ("_id", BsonValue::Int32(number)),
                    ("rank", BsonValue::Int32(number)),
                ])
                .unwrap()
            })
            .collect(),
    )
    .await;
    let filter = DocumentFilter::new(
        BsonDocument::from_entries([(
            "rank",
            BsonValue::Document(
                BsonDocument::from_entries([("$gte", BsonValue::Int32(12))]).unwrap(),
            ),
        )])
        .unwrap(),
    )
    .unwrap();
    let options = DocumentReadOptions::new()
        .with_skip(3)
        .with_limit(4)
        .unwrap()
        .with_batch_size(4)
        .unwrap();
    let found = engine
        .execute_document(
            &session,
            request(
                3,
                RequestContext::new(),
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace(),
                    filter.clone(),
                    options.clone(),
                )),
            ),
        )
        .await
        .unwrap();
    let Some(DocumentPlan::Scatter(plan)) = found.plan() else {
        panic!("expected scatter");
    };
    assert_eq!(plan.shards(), &[0, 1, 2, 3]);
    let DocumentResult::Cursor(batch) = found.result() else {
        panic!("expected cursor");
    };
    let ids: Vec<_> = batch
        .documents()
        .iter()
        .map(|doc| doc.get_first("_id").unwrap().clone())
        .collect();
    assert_eq!(ids, (15..19).map(BsonValue::Int32).collect::<Vec<_>>());
    let count = engine
        .execute_document(
            &session,
            request(
                4,
                RequestContext::new(),
                DocumentCommand::Count(DocumentCountRequest::new(
                    namespace(),
                    filter.clone(),
                    options,
                )),
            ),
        )
        .await
        .unwrap();
    assert!(matches!(count.result(), DocumentResult::Count(4)));
    let point_filter = DocumentFilter::new(
        BsonDocument::from_entries([(
            "_id",
            BsonValue::Document(
                BsonDocument::from_entries([("$eq", BsonValue::Double(7.0))]).unwrap(),
            ),
        )])
        .unwrap(),
    )
    .unwrap();
    let point = engine
        .execute_document(
            &session,
            request(
                5,
                RequestContext::new(),
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace(),
                    point_filter,
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    assert!(matches!(point.plan(), Some(DocumentPlan::Point(_))));
    let DocumentResult::Cursor(batch) = point.result() else {
        panic!("expected cursor");
    };
    assert_eq!(
        batch.documents()[0].get_first("_id"),
        Some(&BsonValue::Int32(7))
    );
    engine.shutdown().await.unwrap();
    let engine = Engine::open(temp.path(), 4).await.unwrap();
    let count = engine
        .execute_document(
            &engine.session(),
            request(
                6,
                RequestContext::new(),
                DocumentCommand::Count(DocumentCountRequest::new(
                    namespace(),
                    filter,
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    assert!(matches!(count.result(), DocumentResult::Count(12)));
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn batch_result_limits_fail_before_any_document_write() {
    let temp = tempfile::tempdir().unwrap();
    let engine = Engine::open(temp.path(), 4).await.unwrap();
    let session = engine.session();
    create_collection(&engine, &session, 1).await;
    let command = DocumentCommand::Insert(
        DocumentInsertRequest::new(
            namespace(),
            vec![
                document(BsonValue::Int32(1), "a"),
                document(BsonValue::Int32(2), "b"),
            ],
            DocumentWriteOptions::new(),
        )
        .unwrap(),
    );
    let error = engine
        .execute_document(
            &session,
            request(
                2,
                RequestContext::new().with_result_limits(ResultLimits::new(1, 1_000_000).unwrap()),
                command,
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    let count = engine
        .execute_document(
            &session,
            request(
                3,
                RequestContext::new(),
                DocumentCommand::Count(DocumentCountRequest::new(
                    namespace(),
                    DocumentFilter::empty(),
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap();
    assert!(matches!(count.result(), DocumentResult::Count(0)));
    engine.shutdown().await.unwrap();
}

#[test]
fn production_protocol_modules_do_not_bypass_the_engine_for_document_storage() {
    fn production(source: &str) -> &str {
        source
            .split_once("\n#[cfg(test)]\nmod tests {")
            .map_or(source, |parts| parts.0)
    }

    let modules = [
        ("protocol/mod.rs", include_str!("../src/protocol/mod.rs")),
        (
            "protocol/error.rs",
            include_str!("../src/protocol/error.rs"),
        ),
        ("protocol/http.rs", include_str!("../src/protocol/http.rs")),
        (
            "protocol/http/v1.rs",
            include_str!("../src/protocol/http/v1.rs"),
        ),
        (
            "protocol/http/admin.rs",
            include_str!("../src/protocol/http/admin.rs"),
        ),
        (
            "protocol/http/openapi.rs",
            include_str!("../src/protocol/http/openapi.rs"),
        ),
        (
            "protocol/postgres.rs",
            include_str!("../src/protocol/postgres.rs"),
        ),
    ];

    for (name, source) in modules {
        let source = production(source);
        for forbidden in ["rusqlite", "crate::storage", "storage::document"] {
            assert!(
                !source.contains(forbidden),
                "{name} production code contains backend escape hatch {forbidden}"
            );
        }
    }
}
