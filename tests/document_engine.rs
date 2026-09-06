#![cfg(feature = "documents")]

use std::time::{Duration, Instant};

use briskdb::{
    core::{CancellationToken, Engine, EngineErrorKind, RequestContext, ResultLimits, Session},
    document::{
        BsonDocument, BsonValue, DocumentAggregateRequest, DocumentCollectionMetadata,
        DocumentCollectionOptions, DocumentCommand, DocumentCountRequest,
        DocumentCreateCollectionRequest, DocumentCreateIndexRequest, DocumentDeleteRequest,
        DocumentExecution, DocumentFilter, DocumentFindRequest, DocumentIndexLifecycle,
        DocumentIndexRequest, DocumentInsertRequest, DocumentListCollectionsRequest,
        DocumentListIndexesRequest, DocumentMutationScope, DocumentNamespace, DocumentPipeline,
        DocumentPlan, DocumentProjection, DocumentReadOptions, DocumentRequest, DocumentRequestId,
        DocumentResult, DocumentWriteOptions,
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

    let general_filter =
        DocumentFilter::new(BsonDocument::from_entries([("label", BsonValue::from("a"))]).unwrap())
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
                BsonDocument::from_entries([("$eq", BsonValue::Int32(1))]).unwrap(),
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

    let multi_insert = DocumentInsertRequest::new(
        namespace(),
        vec![
            document(BsonValue::Int32(97), "x"),
            document(BsonValue::Int32(98), "y"),
        ],
        DocumentWriteOptions::new(),
    )
    .unwrap();
    let error = engine
        .execute_document(
            &session,
            request(
                47,
                RequestContext::new(),
                DocumentCommand::Insert(multi_insert),
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
        BsonDocument::from_entries([("label", BsonValue::Int32(1))]).unwrap(),
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
                        DocumentPipeline::new(Vec::<BsonDocument>::new()).unwrap(),
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
