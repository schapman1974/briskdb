#![cfg(feature = "documents")]

use std::{
    error::Error,
    time::{Duration, Instant},
};

use briskdb::{
    core::{CancellationToken, Engine, EngineErrorKind, RequestContext, ResultLimits, Session},
    document::{
        BsonDocument, BsonValue, DocumentCollectionOptions, DocumentCommand,
        DocumentContinueCursorRequest, DocumentCreateCollectionRequest, DocumentCursorBatch,
        DocumentCursorError, DocumentCursorId, DocumentDropCollectionRequest,
        DocumentDropDatabaseRequest, DocumentFilter, DocumentKillCursorRequest,
        DocumentListCollectionMetadataRequest, DocumentNamespace, DocumentReadOptions,
        DocumentRequest, DocumentRequestId, DocumentResult, DocumentWriteOptions,
    },
};

fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}

fn request(command: DocumentCommand, context: RequestContext) -> DocumentRequest {
    DocumentRequest::new(DocumentRequestId::new([1; 16]).unwrap(), context, command)
}

fn listing(
    database: &str,
    filter: BsonDocument,
    names: bool,
    options: DocumentReadOptions,
) -> DocumentCommand {
    DocumentCommand::ListCollectionMetadata(
        DocumentListCollectionMetadataRequest::new(
            database,
            DocumentFilter::new(filter).unwrap(),
            names,
            options,
        )
        .unwrap(),
    )
}

fn next(database: &str, id: DocumentCursorId, options: DocumentReadOptions) -> DocumentCommand {
    DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
        DocumentNamespace::new(database, "$cmd.listCollections").unwrap(),
        id,
        options,
    ))
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
    let DocumentResult::Cursor(batch) = result.into_parts().2 else {
        panic!("metadata cursor")
    };
    batch
}

async fn create(
    engine: &Engine,
    session: &Session,
    database: &str,
    collection: &str,
    options: BsonDocument,
) {
    execute(
        engine,
        session,
        DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
            DocumentNamespace::new(database, collection).unwrap(),
            DocumentCollectionOptions::new(options).unwrap(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
}

fn options(size: u64) -> DocumentReadOptions {
    DocumentReadOptions::new().with_batch_size(size).unwrap()
}

#[tokio::test]
async fn collection_metadata_streams_filters_and_preserves_uuid_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    // Cross a default-size batch; interleaved databases must not leak through.
    for index in 0..105 {
        create(
            &engine,
            &session,
            "app",
            &format!("items{index:03}"),
            if index == 0 {
                doc([("marker", BsonValue::from("preserved"))])
            } else {
                BsonDocument::new()
            },
        )
        .await;
    }
    create(
        &engine,
        &session,
        "elsewhere",
        "hidden",
        BsonDocument::new(),
    )
    .await;
    let mut batch = page(
        &engine,
        &session,
        listing("app", BsonDocument::new(), false, options(7)),
    )
    .await;
    assert_eq!(batch.namespace().to_string(), "app.$cmd.listCollections");
    let mut rows = batch.documents().to_vec();
    while let Some(id) = batch.cursor_id() {
        batch = page(&engine, &session, next("app", id, options(7))).await;
        assert!(batch.documents().len() <= 7);
        rows.extend_from_slice(batch.documents());
    }
    assert_eq!(rows.len(), 105);
    let first = rows[0].clone();
    assert_eq!(
        first.get_first("options"),
        Some(&BsonValue::Document(doc([(
            "marker",
            BsonValue::from("preserved")
        )])))
    );
    let Some(BsonValue::Document(info)) = first.get_first("info") else {
        panic!("info")
    };
    let Some(BsonValue::Binary(uuid)) = info.get_first("uuid") else {
        panic!("uuid")
    };
    assert_eq!(uuid.subtype(), 4);
    assert_eq!(uuid.bytes().len(), 16);
    assert_eq!(uuid.bytes()[6] >> 4, 8);
    assert_eq!(uuid.bytes()[8] >> 6, 2);
    assert!(first.get_first("idIndex").is_some());
    let filtered = page(
        &engine,
        &session,
        listing(
            "app",
            doc([("options.marker", BsonValue::from("preserved"))]),
            false,
            options(1),
        ),
    )
    .await;
    assert_eq!(filtered.documents(), std::slice::from_ref(&first));
    assert!(filtered.cursor_id().is_none());
    // Filters see only returned fields when nameOnly is selected.
    assert!(
        page(
            &engine,
            &session,
            listing(
                "app",
                doc([("options.marker", BsonValue::from("preserved"))]),
                true,
                options(1)
            )
        )
        .await
        .documents()
        .is_empty()
    );
    let names = page(
        &engine,
        &session,
        listing(
            "app",
            doc([(
                "name",
                BsonValue::Document(doc([("$regex", BsonValue::from("^items10"))])),
            )]),
            true,
            options(10),
        ),
    )
    .await;
    assert_eq!(names.documents().len(), 5);
    assert!(
        names
            .documents()
            .iter()
            .all(|row| row.len() == 2
                && row.get_first("type") == Some(&BsonValue::from("collection")))
    );
    engine.shutdown().await.unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    let reopened = page(
        &engine,
        &session,
        listing(
            "app",
            doc([("name", BsonValue::from("items000"))]),
            false,
            options(1),
        ),
    )
    .await;
    assert_eq!(reopened.documents(), &[first]);
    engine.shutdown().await.unwrap();
}

fn database_names(filter: BsonDocument) -> DocumentCommand {
    DocumentCommand::ListDatabaseNames(briskdb::document::DocumentListDatabaseNamesRequest::new(
        DocumentFilter::new(filter).unwrap(),
    ))
}

async fn names(engine: &Engine, session: &Session, filter: BsonDocument) -> Vec<String> {
    let DocumentResult::DatabaseNames(names) =
        execute(engine, session, database_names(filter)).await
    else {
        panic!("database names")
    };
    names.into_vec()
}

#[tokio::test]
async fn database_names_are_filtered_exact_persistent_and_follow_collection_lifecycle() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    assert!(
        names(&engine, &session, BsonDocument::new())
            .await
            .is_empty()
    );
    for filter in [
        doc([("sizeOnDisk", BsonValue::Int32(0))]),
        doc([("empty", BsonValue::Boolean(false))]),
        doc([("$unsupported", BsonValue::Int32(1))]),
    ] {
        assert!(
            engine
                .execute_document(
                    &session,
                    request(database_names(filter), RequestContext::new())
                )
                .await
                .is_err()
        );
    }
    create(&engine, &session, "App", "one", BsonDocument::new()).await;
    create(&engine, &session, "App", "two", BsonDocument::new()).await;
    create(&engine, &session, "app", "one", BsonDocument::new()).await;
    create(&engine, &session, "日本語", "one", BsonDocument::new()).await;
    assert_eq!(
        names(&engine, &session, BsonDocument::new()).await,
        ["App", "app", "日本語"]
    );
    assert_eq!(
        names(&engine, &session, doc([("name", BsonValue::from("App"))])).await,
        ["App"]
    );
    let filter = doc([(
        "$or",
        BsonValue::Array(vec![
            BsonValue::Document(doc([(
                "name",
                BsonValue::Document(doc([("$regex", BsonValue::from("^a"))])),
            )])),
            BsonValue::Document(doc([("name", BsonValue::from("日本語"))])),
        ]),
    )]);
    assert_eq!(names(&engine, &session, filter).await, ["app", "日本語"]);
    execute(
        &engine,
        &session,
        DocumentCommand::DropCollection(DocumentDropCollectionRequest::new(
            DocumentNamespace::new("App", "one").unwrap(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    assert_eq!(names(&engine, &session, BsonDocument::new()).await.len(), 3);
    execute(
        &engine,
        &session,
        DocumentCommand::DropCollection(DocumentDropCollectionRequest::new(
            DocumentNamespace::new("App", "two").unwrap(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    execute(
        &engine,
        &session,
        DocumentCommand::DropDatabase(
            DocumentDropDatabaseRequest::new("app", DocumentWriteOptions::new()).unwrap(),
        ),
    )
    .await;
    assert_eq!(
        names(&engine, &session, BsonDocument::new()).await,
        ["日本語"]
    );
    engine.shutdown().await.unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    assert_eq!(
        names(&engine, &session, BsonDocument::new()).await,
        ["日本語"]
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn database_name_catalog_ceiling_and_request_limits_are_enforced() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    for index in 0..64 {
        create(
            &engine,
            &session,
            &format!("db{index:02}"),
            "items",
            if index == 0 {
                doc([("opaque", BsonValue::from("x".repeat(64 * 1024)))])
            } else {
                BsonDocument::new()
            },
        )
        .await;
    }
    let all = database_names(BsonDocument::new());
    assert_eq!(
        names(&engine, &session, BsonDocument::new()).await.len(),
        64
    );
    let overflow = DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
        DocumentNamespace::new("overflow", "items").unwrap(),
        DocumentCollectionOptions::empty(),
        DocumentWriteOptions::new(),
    ));
    let identities = || {
        rusqlite::Connection::open_with_flags(root.path().join("manifest.sqlite"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
            .query_row("SELECT database_high_water, collection_high_water FROM briskdb_document_identities WHERE singleton = 1", [], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))).unwrap()
    };
    let before = identities();
    assert_eq!(
        engine
            .execute_document(&session, request(overflow, RequestContext::new()))
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    assert_eq!(identities(), before);
    for limits in [
        ResultLimits::new(63, 4096).unwrap(),
        ResultLimits::new(64, 20).unwrap(),
    ] {
        assert_eq!(
            engine
                .execute_document(
                    &session,
                    request(
                        all.clone(),
                        RequestContext::new().with_result_limits(limits)
                    )
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
    }
    let filtered = database_names(doc([("name", BsonValue::from("db00"))]));
    let execution = engine
        .execute_document(
            &session,
            request(
                filtered,
                RequestContext::new().with_result_limits(ResultLimits::new(1, 64).unwrap()),
            ),
        )
        .await
        .unwrap();
    assert!(execution.plan().is_none());
    let result = execution.into_parts().2;
    assert!(!format!("{result:?}").contains("db00"));
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        engine
            .execute_document(
                &session,
                request(
                    all.clone(),
                    RequestContext::new().with_cancellation_token(token)
                )
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::Cancelled
    );
    assert!(
        engine
            .execute_document(
                &session,
                request(
                    all,
                    RequestContext::new().with_deadline(Instant::now() - Duration::from_secs(1))
                )
            )
            .await
            .is_err()
    );
    assert_eq!(
        names(&engine, &session, BsonDocument::new()).await.len(),
        64
    );
    execute(
        &engine,
        &session,
        DocumentCommand::DropDatabase(
            DocumentDropDatabaseRequest::new("db63", DocumentWriteOptions::new()).unwrap(),
        ),
    )
    .await;
    create(
        &engine,
        &session,
        "replacement",
        "items",
        BsonDocument::new(),
    )
    .await;
    assert_eq!(
        names(&engine, &session, BsonDocument::new()).await.len(),
        64
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn metadata_cursor_ceiling_drop_recreate_ownership_and_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    for name in ["a", "b", "c"] {
        create(&engine, &session, "app", name, BsonDocument::new()).await;
    }
    let first = page(
        &engine,
        &session,
        listing("app", BsonDocument::new(), true, options(0)),
    )
    .await;
    assert!(first.documents().is_empty());
    let id = first.cursor_id().unwrap();
    create(&engine, &session, "app", "later", BsonDocument::new()).await;
    execute(
        &engine,
        &session,
        DocumentCommand::DropCollection(DocumentDropCollectionRequest::new(
            DocumentNamespace::new("app", "b").unwrap(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    create(&engine, &session, "app", "b", BsonDocument::new()).await;
    let foreign = engine.session();
    let error = engine
        .execute_document(
            &foreign,
            request(next("app", id, options(10)), RequestContext::new()),
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
    let rows = page(&engine, &session, next("app", id, options(10))).await;
    assert_eq!(
        rows.documents()
            .iter()
            .map(|row| row.get_first("name").unwrap())
            .collect::<Vec<_>>(),
        vec![&BsonValue::from("a"), &BsonValue::from("c")]
    );
    assert!(rows.cursor_id().is_none());
    let old = page(
        &engine,
        &session,
        listing("app", BsonDocument::new(), false, options(0)),
    )
    .await
    .cursor_id()
    .unwrap();
    execute(
        &engine,
        &session,
        DocumentCommand::DropDatabase(
            DocumentDropDatabaseRequest::new("app", DocumentWriteOptions::new()).unwrap(),
        ),
    )
    .await;
    create(&engine, &session, "app", "a", BsonDocument::new()).await;
    assert!(
        engine
            .execute_document(
                &session,
                request(next("app", old, options(1)), RequestContext::new())
            )
            .await
            .is_err()
    );
    // Every failed continuation releases its cursor; successful kills do too.
    for _ in 0..12 {
        let id = page(
            &engine,
            &session,
            listing("app", BsonDocument::new(), true, options(0)),
        )
        .await
        .cursor_id()
        .unwrap();
        let error = engine
            .execute_document(
                &session,
                request(
                    next("app", id, options(1).with_batch_byte_limit(1).unwrap()),
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
            page(
                &engine,
                &session,
                listing("app", BsonDocument::new(), true, options(0)),
            )
            .await
            .cursor_id()
            .unwrap(),
        );
    }
    assert_eq!(
        engine
            .execute_document(
                &session,
                request(
                    listing("app", BsonDocument::new(), true, options(0)),
                    RequestContext::new()
                )
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
                    DocumentNamespace::new("app", "$cmd.listCollections").unwrap(),
                    id,
                    DocumentWriteOptions::new()
                ))
            )
            .await,
            DocumentResult::CursorKilled(true)
        ));
    }
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn metadata_empty_invalid_filters_controls_and_hard_soft_limits() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    let absent = page(
        &engine,
        &session,
        listing("missing", BsonDocument::new(), false, options(0)),
    )
    .await;
    assert!(absent.documents().is_empty() && absent.cursor_id().is_none());
    let invalid = listing(
        "missing",
        doc([("$unsupported", BsonValue::Int32(1))]),
        true,
        options(0),
    );
    assert!(
        engine
            .execute_document(&session, request(invalid, RequestContext::new()))
            .await
            .is_err()
    );
    create(
        &engine,
        &session,
        "app",
        "a",
        doc([("opaque", BsonValue::from("x".repeat(64 * 1024)))]),
    )
    .await;
    create(&engine, &session, "app", "b", BsonDocument::new()).await;
    // Name-only must not decode/copy the large collection options into results.
    let small = listing("app", BsonDocument::new(), true, options(2));
    assert!(
        engine
            .execute_document(
                &session,
                request(
                    small.clone(),
                    RequestContext::new().with_result_limits(ResultLimits::new(2, 256).unwrap())
                )
            )
            .await
            .is_ok()
    );
    for limits in [
        ResultLimits::new(1, 4096).unwrap(),
        ResultLimits::new(2, 70).unwrap(),
    ] {
        assert_eq!(
            engine
                .execute_document(
                    &session,
                    request(
                        small.clone(),
                        RequestContext::new().with_result_limits(limits)
                    )
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
    }
    let soft = page(
        &engine,
        &session,
        listing(
            "app",
            BsonDocument::new(),
            true,
            options(10).with_batch_byte_limit(130).unwrap(),
        ),
    )
    .await;
    assert_eq!(soft.documents().len(), 1);
    assert_eq!(
        page(
            &engine,
            &session,
            next("app", soft.cursor_id().unwrap(), options(10))
        )
        .await
        .documents()
        .len(),
        1
    );
    let full = listing(
        "app",
        BsonDocument::new(),
        false,
        options(1).with_batch_byte_limit(1024).unwrap(),
    );
    assert_eq!(
        engine
            .execute_document(&session, request(full, RequestContext::new()))
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        engine
            .execute_document(
                &session,
                request(
                    small.clone(),
                    RequestContext::new().with_cancellation_token(token)
                )
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::Cancelled
    );
    assert!(
        engine
            .execute_document(
                &session,
                request(
                    small,
                    RequestContext::new().with_deadline(Instant::now() - Duration::from_secs(1))
                )
            )
            .await
            .is_err()
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn valid_options_that_outgrow_metadata_envelope_do_not_degrade_storage() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    let mut nested = BsonDocument::new();
    for _ in 1..briskdb::document::BSON_MAX_NESTING_DEPTH {
        nested = doc([("nested", BsonValue::Document(nested))]);
    }
    create(&engine, &session, "app", "deep", nested).await;
    let error = engine
        .execute_document(
            &session,
            request(
                listing("app", BsonDocument::new(), false, options(1)),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    assert_eq!(
        page(
            &engine,
            &session,
            listing("app", BsonDocument::new(), true, options(1))
        )
        .await
        .documents()
        .len(),
        1
    );
    create(&engine, &session, "app", "still_ready", BsonDocument::new()).await;
    engine.shutdown().await.unwrap();
}
