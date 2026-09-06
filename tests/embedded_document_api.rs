#![cfg(feature = "documents")]

use std::time::{Duration, Instant};

use briskdb::{
    BriskDb, CancellationToken, DocumentSupport, EngineErrorKind, RequestContext,
    document::{
        BsonBinary, BsonDateTime, BsonDecimal128, BsonDocument, BsonValue, DocumentCommand,
        DocumentCountRequest, DocumentCreateCollectionRequest, DocumentFilter, DocumentFindRequest,
        DocumentInsertRequest, DocumentListCollectionsRequest, DocumentPlan, DocumentReadOptions,
        DocumentRequest, DocumentRequestId, DocumentResult, DocumentWriteOptions,
    },
};

fn namespace() -> briskdb::document::DocumentNamespace {
    briskdb::document::DocumentNamespace::new("app", "records").unwrap()
}

fn request(seed: u8, command: DocumentCommand) -> DocumentRequest {
    request_with_context(seed, RequestContext::new(), command)
}

fn request_with_context(
    seed: u8,
    context: RequestContext,
    command: DocumentCommand,
) -> DocumentRequest {
    DocumentRequest::new(
        DocumentRequestId::new([seed; 16]).unwrap(),
        context,
        command,
    )
}

fn create_collection_command() -> DocumentCommand {
    DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
        namespace(),
        Default::default(),
        DocumentWriteOptions::new(),
    ))
}

fn list_collections_command() -> DocumentCommand {
    DocumentCommand::ListCollections(
        DocumentListCollectionsRequest::new("app", DocumentReadOptions::new()).unwrap(),
    )
}

fn exact_filter(id: BsonValue) -> DocumentFilter {
    DocumentFilter::new(BsonDocument::from_entries([("_id", id)]).unwrap()).unwrap()
}

fn fidelity_document() -> BsonDocument {
    let nested = BsonDocument::from_entries([
        ("first", BsonValue::Int32(1)),
        ("second", BsonValue::Int64(2)),
    ])
    .unwrap();
    BsonDocument::from_entries([
        ("_id", BsonValue::from("typed-id")),
        ("int32", BsonValue::Int32(7)),
        ("int64", BsonValue::Int64(7)),
        ("double", BsonValue::Double(-0.0)),
        ("null", BsonValue::Null),
        ("boolean", BsonValue::Boolean(true)),
        (
            "binary",
            BsonValue::Binary(BsonBinary::new(0, [0x00, 0x7f, 0xff])),
        ),
        ("date", BsonValue::DateTime(BsonDateTime::from_millis(-1))),
        (
            "decimal",
            BsonValue::Decimal128(BsonDecimal128::parse("123.4500").unwrap()),
        ),
        ("nested", BsonValue::Document(nested)),
        (
            "array",
            BsonValue::Array(vec![BsonValue::Int32(3), BsonValue::from("four")]),
        ),
    ])
    .unwrap()
}

#[tokio::test]
async fn document_support_is_an_enforced_runtime_opt_in() {
    let temp = tempfile::tempdir().unwrap();
    let database = BriskDb::builder(temp.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    assert_eq!(database.document_support(), DocumentSupport::Disabled);
    let session = database.session();

    let error = database
        .execute_document(&session, request(1, create_collection_command()))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
    assert!(error.to_string().contains("document support is disabled"));

    // The facade rejects the request before forwarding it to the document
    // engine, so the failed opt-in check cannot leave catalog mutations behind.
    let listed = database
        .engine()
        .execute_document(&session, request(2, list_collections_command()))
        .await
        .unwrap();
    match listed.result() {
        DocumentResult::Collections(collections) => assert!(collections.is_empty()),
        result => panic!("expected collections, got {:?}", result.kind()),
    }

    session.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn raw_and_owned_facades_preserve_engine_document_outcomes_and_bson() {
    let temp = tempfile::tempdir().unwrap();
    let database = BriskDb::builder(temp.path())
        .with_shard_count(4)
        .with_document_support(DocumentSupport::Enabled)
        .open()
        .await
        .unwrap();
    assert_eq!(database.document_support(), DocumentSupport::Enabled);
    let raw = database.session();
    let owned = database.owned_session();

    let created = database
        .execute_document(&raw, request(10, create_collection_command()))
        .await
        .unwrap();
    assert_eq!(
        created.request_id(),
        DocumentRequestId::new([10; 16]).unwrap()
    );
    assert!(created.plan().is_none());
    assert!(matches!(created.result(), DocumentResult::Collection(_)));

    let source = fidelity_document();
    let insert = DocumentInsertRequest::new(
        namespace(),
        vec![source.clone()],
        DocumentWriteOptions::new(),
    )
    .unwrap();
    let inserted = owned
        .execute_document(request(11, DocumentCommand::Insert(insert)))
        .await
        .unwrap();
    assert_eq!(
        inserted.request_id(),
        DocumentRequestId::new([11; 16]).unwrap()
    );
    assert!(matches!(inserted.plan(), Some(DocumentPlan::Point(_))));
    match inserted.result() {
        DocumentResult::Insert(result) => {
            assert_eq!(result.inserted_ids().len(), 1);
            assert!(result.inserted_ids()[0].representation_eq(&BsonValue::from("typed-id")));
        }
        result => panic!("expected insert result, got {:?}", result.kind()),
    }

    let exact_find = request(
        12,
        DocumentCommand::Find(DocumentFindRequest::new(
            namespace(),
            exact_filter(BsonValue::from("typed-id")),
            DocumentReadOptions::new(),
        )),
    );
    let direct_exact = database
        .engine()
        .execute_document(&raw, exact_find.clone())
        .await
        .unwrap();
    let facade_exact = database.execute_document(&raw, exact_find).await.unwrap();
    assert_eq!(facade_exact, direct_exact);
    assert!(matches!(facade_exact.plan(), Some(DocumentPlan::Point(_))));
    match facade_exact.result() {
        DocumentResult::Cursor(batch) => {
            assert_eq!(batch.documents().len(), 1);
            let returned = &batch.documents()[0];
            assert!(returned.representation_eq(&source));
            assert_eq!(
                returned.iter().map(|(name, _)| name).collect::<Vec<_>>(),
                vec![
                    "_id", "int32", "int64", "double", "null", "boolean", "binary", "date",
                    "decimal", "nested", "array",
                ]
            );
            assert!(matches!(
                returned.get_unique("int32").unwrap(),
                Some(BsonValue::Int32(7))
            ));
            assert!(matches!(
                returned.get_unique("int64").unwrap(),
                Some(BsonValue::Int64(7))
            ));
            assert!(matches!(
                returned.get_unique("double").unwrap(),
                Some(BsonValue::Double(value)) if value.to_bits() == (-0.0_f64).to_bits()
            ));
        }
        result => panic!("expected cursor, got {:?}", result.kind()),
    }

    let scatter_find = request(
        13,
        DocumentCommand::Find(DocumentFindRequest::new(
            namespace(),
            DocumentFilter::empty(),
            DocumentReadOptions::new(),
        )),
    );
    let direct_scatter = database
        .engine()
        .execute_document(&raw, scatter_find.clone())
        .await
        .unwrap();
    let facade_scatter = owned.execute_document(scatter_find).await.unwrap();
    assert_eq!(facade_scatter, direct_scatter);
    match facade_scatter.plan().expect("scatter find plan") {
        DocumentPlan::Scatter(plan) => assert_eq!(plan.shards(), &[0, 1, 2, 3]),
        plan => panic!("expected scatter plan, got {plan:?}"),
    }

    let exact_count = request(
        14,
        DocumentCommand::Count(DocumentCountRequest::new(
            namespace(),
            exact_filter(BsonValue::from("typed-id")),
            DocumentReadOptions::new(),
        )),
    );
    let direct_exact_count = database
        .engine()
        .execute_document(&raw, exact_count.clone())
        .await
        .unwrap();
    let facade_exact_count = database.execute_document(&raw, exact_count).await.unwrap();
    assert_eq!(facade_exact_count, direct_exact_count);
    assert!(matches!(
        facade_exact_count.plan(),
        Some(DocumentPlan::Point(_))
    ));
    assert!(matches!(
        facade_exact_count.result(),
        DocumentResult::Count(1)
    ));

    let scatter_count = request(
        15,
        DocumentCommand::Count(DocumentCountRequest::new(
            namespace(),
            DocumentFilter::empty(),
            DocumentReadOptions::new(),
        )),
    );
    let direct_scatter_count = database
        .engine()
        .execute_document(&raw, scatter_count.clone())
        .await
        .unwrap();
    let facade_scatter_count = owned.execute_document(scatter_count).await.unwrap();
    assert_eq!(facade_scatter_count, direct_scatter_count);
    assert!(matches!(
        facade_scatter_count.plan(),
        Some(DocumentPlan::Scatter(_))
    ));
    assert!(matches!(
        facade_scatter_count.result(),
        DocumentResult::Count(1)
    ));

    raw.close().await.unwrap();
    owned.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn document_facades_preserve_controls_session_ownership_and_shutdown() {
    let parent = tempfile::tempdir().unwrap();
    let first = BriskDb::builder(parent.path().join("first"))
        .with_shard_count(2)
        .with_document_support(DocumentSupport::Enabled)
        .open()
        .await
        .unwrap();
    let second = BriskDb::builder(parent.path().join("second"))
        .with_shard_count(2)
        .with_document_support(DocumentSupport::Enabled)
        .open()
        .await
        .unwrap();
    let first_raw = first.session();
    let foreign_raw = second.session();

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = first
        .execute_document(
            &first_raw,
            request_with_context(
                20,
                RequestContext::new().with_cancellation_token(cancellation),
                list_collections_command(),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(cancelled.kind(), EngineErrorKind::Cancelled);

    let owned = first.owned_session();
    let deadline = owned
        .execute_document(request_with_context(
            21,
            RequestContext::new().with_deadline(Instant::now() - Duration::from_millis(1)),
            list_collections_command(),
        ))
        .await
        .unwrap_err();
    assert_eq!(deadline.kind(), EngineErrorKind::DeadlineExceeded);

    let foreign = first
        .execute_document(&foreign_raw, request(22, list_collections_command()))
        .await
        .unwrap_err();
    assert_eq!(foreign.kind(), EngineErrorKind::FailedPrecondition);

    first_raw.close().await.unwrap();
    let closed_raw = first
        .execute_document(&first_raw, request(23, list_collections_command()))
        .await
        .unwrap_err();
    assert_eq!(closed_raw.kind(), EngineErrorKind::FailedPrecondition);

    owned.close().await.unwrap();
    let closed_owned = owned
        .execute_document(request(24, list_collections_command()))
        .await
        .unwrap_err();
    assert_eq!(closed_owned.kind(), EngineErrorKind::FailedPrecondition);

    let leaked = first.owned_session();
    first.close().await.unwrap();
    let stopped = leaked
        .execute_document(request(25, list_collections_command()))
        .await
        .unwrap_err();
    assert_eq!(stopped.kind(), EngineErrorKind::ShuttingDown);
    leaked.close().await.unwrap();

    foreign_raw.close().await.unwrap();
    second.close().await.unwrap();
}
