#![cfg(feature = "documents")]

use briskdb::{
    core::{CancellationToken, Engine, EngineErrorKind, RequestContext, ResultLimits, Session},
    document::{
        BsonDocument, BsonTimestamp, BsonValue, DocumentCollectionOptions, DocumentCommand,
        DocumentCreateCollectionRequest, DocumentFilter, DocumentFindOneAndReplaceRequest,
        DocumentFindOneAndUpdateRequest, DocumentFindRequest, DocumentInsertRequest,
        DocumentMutationError, DocumentMutationScope, DocumentNamespace, DocumentPlan,
        DocumentProjection, DocumentReadOptions, DocumentReplaceRequest, DocumentRequest,
        DocumentRequestId, DocumentResult, DocumentSort, DocumentUpdate, DocumentUpdateRequest,
        DocumentWriteOptions, encode_document,
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

fn update(filter: BsonDocument, expression: BsonDocument) -> DocumentUpdateRequest {
    DocumentUpdateRequest::new(
        ns(),
        DocumentFilter::new(filter).unwrap(),
        DocumentUpdate::new(expression).unwrap(),
        DocumentMutationScope::One,
        DocumentWriteOptions::new(),
    )
}

fn set(fields: BsonDocument) -> BsonDocument {
    doc([("$set", BsonValue::Document(fields))])
}

fn update_many(filter: BsonDocument, expression: BsonDocument) -> DocumentCommand {
    DocumentCommand::Update(DocumentUpdateRequest::new(
        ns(),
        DocumentFilter::new(filter).unwrap(),
        DocumentUpdate::new(expression).unwrap(),
        DocumentMutationScope::Many,
        DocumentWriteOptions::new(),
    ))
}

async fn many_counts(
    engine: &Engine,
    session: &Session,
    filter: BsonDocument,
    expression: BsonDocument,
) -> (u64, u64) {
    let execution = engine
        .execute_document(
            session,
            request(update_many(filter, expression), RequestContext::new()),
        )
        .await
        .unwrap();
    let DocumentResult::Update(result) = execution.into_parts().2 else {
        panic!("update result")
    };
    (result.matched_count(), result.modified_count())
}

fn shard_rows(connection: &Connection) -> Vec<BsonDocument> {
    connection
        .prepare("SELECT document_bson FROM briskdb_documents_v1 ORDER BY natural_order")
        .unwrap()
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .unwrap()
        .map(|bytes| briskdb::document::decode_document(&bytes.unwrap()).unwrap())
        .collect()
}

#[tokio::test]
async fn update_many_counts_filters_noops_point_routes_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let expression = set(doc([
        ("done", BsonValue::Boolean(true)),
        ("value", BsonValue::Int64(1)),
    ]));
    let filter = doc([("group", BsonValue::Int32(0))]);
    assert_eq!(
        many_counts(&engine, &session, filter.clone(), expression.clone()).await,
        (12, 12)
    );
    assert_eq!(
        many_counts(&engine, &session, filter, expression.clone()).await,
        (12, 0)
    );
    assert_eq!(
        many_counts(
            &engine,
            &session,
            doc([("_id", BsonValue::Double(1.0))]),
            expression.clone()
        )
        .await,
        (1, 1)
    );
    assert_eq!(
        many_counts(
            &engine,
            &session,
            doc([("_id", BsonValue::Int32(-1))]),
            expression.clone()
        )
        .await,
        (0, 0)
    );
    assert_eq!(
        many_counts(&engine, &session, BsonDocument::new(), expression).await,
        (24, 11)
    );
    assert_eq!(
        many_counts(
            &engine,
            &session,
            BsonDocument::new(),
            doc([(
                "$unset",
                BsonValue::Document(doc([("group", BsonValue::Null)]))
            )])
        )
        .await,
        (24, 24)
    );
    let expected = rows(&engine, &session).await;
    for (index, row) in expected.iter().enumerate() {
        assert_eq!(row.get_first("_id"), Some(&BsonValue::Int32(index as i32)));
        assert!(matches!(row.get_first("value"), Some(BsonValue::Int64(1))));
        assert!(row.get_first("group").is_none());
    }
    drop(session);
    engine.shutdown().await.unwrap();
    drop(engine);
    let engine = Engine::open(root.path(), 4).await.unwrap();
    assert_eq!(rows(&engine, &engine.session()).await, expected);
}

#[tokio::test]
async fn update_many_eager_validation_and_result_limits_precede_first_commit() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let before = rows(&engine, &session).await;
    let expression = set(doc([("done", BsonValue::Boolean(true))]));
    for context in [
        RequestContext::new().with_result_limits(ResultLimits::new(1, 1).unwrap()),
        RequestContext::new().with_deadline(Instant::now() - Duration::from_secs(1)),
    ] {
        assert!(
            engine
                .execute_document(
                    &session,
                    request(
                        update_many(BsonDocument::new(), expression.clone()),
                        context
                    )
                )
                .await
                .is_err()
        );
    }
    for filter in [BsonDocument::new(), doc([("_id", BsonValue::Int32(-1))])] {
        assert!(
            engine
                .execute_document(
                    &session,
                    request(
                        update_many(
                            filter,
                            set(doc([("a", BsonValue::Null), ("a.b", BsonValue::Null)]))
                        ),
                        RequestContext::new()
                    )
                )
                .await
                .is_err()
        );
    }
    assert_eq!(rows(&engine, &session).await, before);
}

#[tokio::test]
async fn update_many_validation_rolls_back_current_shard_but_preserves_prior_commits() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let shards: Vec<_> = (0..4)
        .map(|shard| {
            Connection::open(root.path().join(format!("shards/{shard:04}.sqlite"))).unwrap()
        })
        .collect();
    let last = shard_rows(&shards[3]).pop().unwrap();
    update_counts(
        &engine,
        &session,
        doc([("_id", last.get_first("_id").unwrap().clone())]),
        set(doc([("target", BsonValue::Int32(1))])),
    )
    .await;
    let before = shard_rows(&shards[3]);
    assert!(before.len() > 1);
    let error = engine
        .execute_document(
            &session,
            request(
                update_many(
                    BsonDocument::new(),
                    set(doc([
                        ("done", BsonValue::Boolean(true)),
                        ("target.x", BsonValue::Int32(1)),
                    ])),
                ),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
    for shard in &shards[..3] {
        let rows = shard_rows(shard);
        assert!(!rows.is_empty());
        assert!(
            rows.iter()
                .all(|row| row.get_first("done") == Some(&BsonValue::Boolean(true)))
        );
    }
    assert_eq!(shard_rows(&shards[3]), before);
    assert_eq!(rows(&engine, &session).await.len(), 24);
    let expected = rows(&engine, &session).await;
    drop(shards);
    drop(session);
    engine.shutdown().await.unwrap();
    drop(engine);
    let engine = Engine::open(root.path(), 4).await.unwrap();
    assert_eq!(rows(&engine, &engine.session()).await, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_many_cancellation_and_abort_keep_prior_commits_and_release_locks() {
    for abort in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), 2).await.unwrap();
        let session = Arc::new(engine.session());
        seed(&engine, &session).await;
        let first = Connection::open(root.path().join("shards/0000.sqlite")).unwrap();
        let mut second = Connection::open(root.path().join("shards/0001.sqlite")).unwrap();
        let blocker = second
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let before = shard_rows(&blocker);
        assert!(!before.is_empty());
        let cancellation = CancellationToken::new();
        let task_engine = engine.clone();
        let task_session = Arc::clone(&session);
        let context = RequestContext::new().with_cancellation_token(cancellation.clone());
        let task = tokio::spawn(async move {
            task_engine
                .execute_document(
                    &task_session,
                    request(
                        update_many(
                            BsonDocument::new(),
                            set(doc([("done", BsonValue::Boolean(true))])),
                        ),
                        context,
                    ),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if shard_rows(&first)
                    .iter()
                    .all(|row| row.get_first("done") == Some(&BsonValue::Boolean(true)))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        if abort {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            cancellation.cancel();
            assert_eq!(
                task.await.unwrap().unwrap_err().kind(),
                EngineErrorKind::Cancelled
            );
        }
        assert_eq!(shard_rows(&blocker), before);
        blocker.rollback().unwrap();
        assert_eq!(
            many_counts(
                &engine,
                &session,
                BsonDocument::new(),
                set(doc([("done", BsonValue::Boolean(true))]))
            )
            .await,
            (24, before.len() as u64)
        );
    }
}

async fn update_counts(
    engine: &Engine,
    session: &Session,
    filter: BsonDocument,
    expression: BsonDocument,
) -> (u64, u64) {
    let result = engine
        .execute_document(
            session,
            request(
                DocumentCommand::Update(update(filter, expression)),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    let DocumentResult::Update(result) = result.into_parts().2 else {
        panic!("update result")
    };
    assert!(result.upserted_id().is_none());
    (result.matched_count(), result.modified_count())
}

#[tokio::test]
async fn operator_updates_keep_fields_order_identity_and_literal_timestamps_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let expression = set(doc([
        ("group", BsonValue::Int64(0)),
        ("nested.0.value", BsonValue::from("$literal")),
        ("stamp", BsonValue::Timestamp(BsonTimestamp::new(0, 0))),
    ]));
    assert_eq!(
        update_counts(
            &engine,
            &session,
            doc([("done", BsonValue::Boolean(false))]),
            expression.clone()
        )
        .await,
        (1, 1)
    );
    assert_eq!(
        update_counts(
            &engine,
            &session,
            doc([("_id", BsonValue::Double(0.0))]),
            expression
        )
        .await,
        (1, 0)
    );
    assert_eq!(
        update_counts(
            &engine,
            &session,
            doc([("_id", BsonValue::Int32(-1))]),
            set(BsonDocument::new())
        )
        .await,
        (0, 0)
    );
    let current = rows(&engine, &session).await;
    assert_eq!(
        current[0].iter().map(|(name, _)| name).collect::<Vec<_>>(),
        ["_id", "group", "done", "nested", "stamp"]
    );
    assert!(matches!(
        current[0].get_first("group"),
        Some(BsonValue::Int64(0))
    ));
    assert_eq!(
        current[0].get_first("stamp"),
        Some(&BsonValue::Timestamp(BsonTimestamp::new(0, 0)))
    );
    assert_eq!(
        update_counts(
            &engine,
            &session,
            doc([("_id", BsonValue::Int32(0))]),
            doc([(
                "$unset",
                BsonValue::Document(doc([("nested.0.value", BsonValue::Null)]))
            )])
        )
        .await,
        (1, 1)
    );
    let expected = rows(&engine, &session).await;
    drop(session);
    engine.shutdown().await.unwrap();
    drop(engine);
    let engine = Engine::open(root.path(), 4).await.unwrap();
    assert_eq!(rows(&engine, &engine.session()).await, expected);
}

#[tokio::test]
async fn operator_failures_and_result_limits_precede_all_writes() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let before = rows(&engine, &session).await;
    let expressions = [
        set(doc([
            ("new", BsonValue::Int32(1)),
            ("group.x", BsonValue::Null),
        ])),
        set(doc([
            ("new", BsonValue::Int32(1)),
            ("_id", BsonValue::Int32(-1)),
        ])),
        set(doc([("a", BsonValue::Null), ("a.b", BsonValue::Null)])),
        doc([(
            "$unset",
            BsonValue::Document(doc([("_id", BsonValue::Null)])),
        )]),
    ];
    for expression in expressions {
        for filter in [BsonDocument::new(), doc([("_id", BsonValue::Int32(0))])] {
            assert!(
                engine
                    .execute_document(
                        &session,
                        request(
                            DocumentCommand::Update(update(filter, expression.clone())),
                            RequestContext::new()
                        )
                    )
                    .await
                    .is_err()
            );
            assert_eq!(rows(&engine, &session).await, before);
        }
    }
    let expression = set(doc([("x", BsonValue::from("x".repeat(256)))]));
    let capped = update(BsonDocument::new(), expression.clone())
        .with_max_document_bytes(128)
        .unwrap();
    assert!(
        engine
            .execute_document(
                &session,
                request(DocumentCommand::Update(capped), RequestContext::new())
            )
            .await
            .is_err()
    );
    let context = RequestContext::new().with_result_limits(ResultLimits::new(1, 1).unwrap());
    assert!(
        engine
            .execute_document(
                &session,
                request(
                    DocumentCommand::Update(update(BsonDocument::new(), expression)),
                    context
                )
            )
            .await
            .is_err()
    );
    assert_eq!(rows(&engine, &session).await, before);
    // Invalid syntax is checked even when the filter matches no documents.
    assert!(
        engine
            .execute_document(
                &session,
                request(
                    DocumentCommand::Update(update(
                        doc([("_id", BsonValue::Int32(-1))]),
                        set(doc([("a", BsonValue::Null), ("a.b", BsonValue::Null)]))
                    )),
                    RequestContext::new()
                )
            )
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_operator_updates_reselect_and_do_not_lose_unrelated_fields() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(root.path(), 4).await.unwrap());
    seed(&engine, &engine.session()).await;
    let mut tasks = Vec::new();
    for index in 0..24 {
        let engine = Arc::clone(&engine);
        tasks.push(tokio::spawn(async move {
            let field = format!("field{index}");
            update_counts(
                &engine,
                &engine.session(),
                doc([("_id", BsonValue::Int32(0))]),
                set(doc([(&field, BsonValue::Int32(index))])),
            )
            .await
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), (1, 1));
    }
    let current = rows(&engine, &engine.session()).await;
    for index in 0..24 {
        assert_eq!(
            current[0].get_first(&format!("field{index}")),
            Some(&BsonValue::Int32(index))
        );
    }
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let engine = Arc::clone(&engine);
        tasks.push(tokio::spawn(async move {
            let session = engine.session();
            let mut total = 0;
            loop {
                let (matched, modified) = update_counts(
                    &engine,
                    &session,
                    doc([("done", BsonValue::Boolean(false))]),
                    set(doc([("done", BsonValue::Boolean(true))])),
                )
                .await;
                assert_eq!(matched, modified);
                if matched == 0 {
                    return total;
                }
                total += modified;
            }
        }));
    }
    let mut total = 0;
    for task in tasks {
        total += task.await.unwrap();
    }
    assert_eq!(total, 24);
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

fn find_update(
    filter: BsonDocument,
    expression: BsonDocument,
    options: DocumentReadOptions,
    after: bool,
) -> DocumentCommand {
    DocumentCommand::FindOneAndUpdate(
        DocumentFindOneAndUpdateRequest::new(update(filter, expression), options)
            .with_return_after(after),
    )
}

#[tokio::test]
async fn find_update_returns_original_or_updated_projection_without_replacing_other_fields() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let options = DocumentReadOptions::new()
        .with_sort(DocumentSort::new(doc([("_id", BsonValue::Int32(-1))])).unwrap())
        .with_projection(
            DocumentProjection::new(doc([
                ("done", BsonValue::Int32(1)),
                ("_id", BsonValue::Int32(0)),
            ]))
            .unwrap(),
        );
    let before = returned(
        &engine,
        &session,
        find_update(
            doc([("group", BsonValue::Int32(0))]),
            set(doc([
                ("done", BsonValue::Boolean(true)),
                ("stamp", BsonValue::Timestamp(BsonTimestamp::new(0, 0))),
            ])),
            options,
            false,
        ),
    )
    .await
    .unwrap();
    assert_eq!(before, doc([("done", BsonValue::Boolean(false))]));
    let current = rows(&engine, &session).await;
    assert_eq!(current[22].get_first("group"), Some(&BsonValue::Int32(0)));
    assert_eq!(
        current[22].get_first("done"),
        Some(&BsonValue::Boolean(true))
    );
    assert_eq!(
        current[22].get_first("stamp"),
        Some(&BsonValue::Timestamp(BsonTimestamp::new(0, 0)))
    );
    for after in [false, true] {
        let image = returned(
            &engine,
            &session,
            find_update(
                doc([("_id", BsonValue::Double(22.0))]),
                set(doc([("done", BsonValue::Boolean(true))])),
                DocumentReadOptions::new(),
                after,
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            encode_document(&image).unwrap(),
            encode_document(&current[22]).unwrap()
        );
        assert!(
            returned(
                &engine,
                &session,
                find_update(
                    doc([("_id", BsonValue::Int32(-1))]),
                    set(BsonDocument::new()),
                    DocumentReadOptions::new(),
                    after
                )
            )
            .await
            .is_none()
        );
    }
    let after = returned(
        &engine,
        &session,
        find_update(
            doc([("_id", BsonValue::Int32(22))]),
            doc([(
                "$unset",
                BsonValue::Document(doc([("group", BsonValue::Null)])),
            )]),
            DocumentReadOptions::new().with_projection(
                DocumentProjection::new(doc([
                    ("group", BsonValue::Int32(1)),
                    ("_id", BsonValue::Int32(0)),
                ]))
                .unwrap(),
            ),
            true,
        ),
    )
    .await
    .unwrap();
    assert!(after.is_empty());
    let expected = rows(&engine, &session).await;
    assert!(expected[22].get_first("group").is_none());
    drop(session);
    engine.shutdown().await.unwrap();
    drop(engine);
    let engine = Engine::open(root.path(), 4).await.unwrap();
    assert_eq!(rows(&engine, &engine.session()).await, expected);
}

#[tokio::test]
async fn find_update_preflights_images_and_rejects_invalid_scopes_paths_and_runtime_sort() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let filter = doc([("_id", BsonValue::Int32(0))]);
    let large = BsonValue::from("x".repeat(600_000));
    for after in [false, true] {
        if !after {
            update_counts(
                &engine,
                &session,
                filter.clone(),
                set(doc([("large", large.clone())])),
            )
            .await;
        }
        let before = rows(&engine, &session).await;
        let expression = if after {
            set(doc([("large", large.clone())]))
        } else {
            doc([(
                "$unset",
                BsonValue::Document(doc([("large", BsonValue::Null)])),
            )])
        };
        let error = engine
            .execute_document(
                &session,
                request(
                    find_update(
                        filter.clone(),
                        expression.clone(),
                        DocumentReadOptions::new(),
                        after,
                    ),
                    RequestContext::new().with_result_limits(ResultLimits::new(1, 128).unwrap()),
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(rows(&engine, &session).await, before);
        let image = returned(
            &engine,
            &session,
            find_update(
                filter.clone(),
                expression,
                DocumentReadOptions::new().with_projection(
                    DocumentProjection::new(doc([("_id", BsonValue::Int32(1))])).unwrap(),
                ),
                after,
            ),
        )
        .await
        .unwrap();
        assert_eq!(image, doc([("_id", BsonValue::Int32(0))]));
    }
    update_counts(
        &engine,
        &session,
        filter.clone(),
        set(doc([
            ("a", BsonValue::Array(vec![BsonValue::Int32(1)])),
            ("b", BsonValue::Array(vec![BsonValue::Int32(1)])),
        ])),
    )
    .await;
    let before = rows(&engine, &session).await;
    let many = DocumentUpdateRequest::new(
        ns(),
        DocumentFilter::empty(),
        DocumentUpdate::new(set(BsonDocument::new())).unwrap(),
        DocumentMutationScope::Many,
        DocumentWriteOptions::new(),
    );
    let commands = [
        DocumentCommand::FindOneAndUpdate(DocumentFindOneAndUpdateRequest::new(
            many,
            DocumentReadOptions::new(),
        )),
        find_update(
            filter.clone(),
            set(doc([("group.x", BsonValue::Null)])),
            DocumentReadOptions::new(),
            true,
        ),
        find_update(
            filter.clone(),
            set(doc([("_id", BsonValue::Int32(-1))])),
            DocumentReadOptions::new(),
            false,
        ),
        find_update(
            filter.clone(),
            set(BsonDocument::new()),
            DocumentReadOptions::new().with_skip(1),
            false,
        ),
        find_update(
            filter.clone(),
            set(doc([("done", BsonValue::Boolean(true))])),
            DocumentReadOptions::new().with_sort(
                DocumentSort::new(doc([
                    ("a", BsonValue::Int32(1)),
                    ("b", BsonValue::Int32(1)),
                ]))
                .unwrap(),
            ),
            false,
        ),
        find_update(
            BsonDocument::new(),
            set(doc([("done", BsonValue::Boolean(true))])),
            DocumentReadOptions::new().with_sort(
                DocumentSort::new(doc([
                    ("a", BsonValue::Int32(1)),
                    ("b", BsonValue::Int32(1)),
                ]))
                .unwrap(),
            ),
            true,
        ),
    ];
    for command in commands {
        assert!(
            engine
                .execute_document(&session, request(command, RequestContext::new()))
                .await
                .is_err()
        );
        assert_eq!(rows(&engine, &session).await, before);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_find_updates_return_each_selected_record_once() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(root.path(), 4).await.unwrap());
    seed(&engine, &engine.session()).await;
    let mut tasks = Vec::new();
    for after in [false, true, false, true] {
        let engine = Arc::clone(&engine);
        tasks.push(tokio::spawn(async move {
            let session = engine.session();
            let mut ids = Vec::new();
            while let Some(image) = returned(
                &engine,
                &session,
                find_update(
                    doc([("done", BsonValue::Boolean(false))]),
                    set(doc([("done", BsonValue::Boolean(true))])),
                    DocumentReadOptions::new(),
                    after,
                ),
            )
            .await
            {
                assert_eq!(image.get_first("done"), Some(&BsonValue::Boolean(after)));
                let Some(BsonValue::Int32(id)) = image.get_first("_id") else {
                    panic!("id")
                };
                ids.push(*id);
            }
            ids
        }));
    }
    let mut ids = Vec::new();
    for task in tasks {
        ids.extend(task.await.unwrap());
    }
    ids.sort_unstable();
    assert_eq!(ids, (0..24).collect::<Vec<_>>());
}

fn find_replace(
    filter: BsonDocument,
    replacement: BsonDocument,
    options: DocumentReadOptions,
    after: bool,
) -> DocumentCommand {
    DocumentCommand::FindOneAndReplace(
        DocumentFindOneAndReplaceRequest::new(replace(filter, replacement), options)
            .with_return_after(after),
    )
}

async fn returned(
    engine: &Engine,
    session: &Session,
    command: DocumentCommand,
) -> Option<BsonDocument> {
    let execution = engine
        .execute_document(session, request(command, RequestContext::new()))
        .await
        .unwrap();
    let DocumentResult::Document(document) = execution.into_parts().2 else {
        panic!("document")
    };
    document
}

#[tokio::test]
async fn find_replace_sorts_original_values_and_returns_projected_before_or_after() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let options = DocumentReadOptions::new()
        .with_sort(DocumentSort::new(doc([("_id", BsonValue::Int32(-1))])).unwrap())
        .with_projection(
            DocumentProjection::new(doc([
                ("group", BsonValue::Int32(1)),
                ("_id", BsonValue::Int32(0)),
            ]))
            .unwrap(),
        );
    let before = returned(
        &engine,
        &session,
        find_replace(
            doc([("group", BsonValue::Int32(0))]),
            doc([("value", BsonValue::Int64(9))]),
            options,
            false,
        ),
    )
    .await
    .unwrap();
    assert_eq!(before, doc([("group", BsonValue::Int32(0))]));
    let current = rows(&engine, &session).await;
    assert_eq!(
        current[22],
        doc([
            ("_id", BsonValue::Int32(22)),
            ("value", BsonValue::Int64(9))
        ])
    );
    let replacement = doc([
        ("_id", BsonValue::Double(22.0)),
        ("value", BsonValue::Int64(9)),
    ]);
    for after in [false, true] {
        let document = returned(
            &engine,
            &session,
            find_replace(
                doc([("_id", BsonValue::Double(22.0))]),
                replacement.clone(),
                DocumentReadOptions::new(),
                after,
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            encode_document(&document).unwrap(),
            encode_document(&current[22]).unwrap()
        );
        assert!(
            returned(
                &engine,
                &session,
                find_replace(
                    doc([("_id", BsonValue::Int32(-1))]),
                    BsonDocument::new(),
                    DocumentReadOptions::new(),
                    after
                )
            )
            .await
            .is_none()
        );
    }
    let projected = returned(
        &engine,
        &session,
        find_replace(
            doc([("_id", BsonValue::Int32(22))]),
            doc([("value", BsonValue::from("new"))]),
            DocumentReadOptions::new().with_projection(
                DocumentProjection::new(doc([
                    ("value", BsonValue::Int32(1)),
                    ("_id", BsonValue::Int32(0)),
                ]))
                .unwrap(),
            ),
            true,
        ),
    )
    .await
    .unwrap();
    assert_eq!(projected, doc([("value", BsonValue::from("new"))]));
    drop(session);
    engine.shutdown().await.unwrap();
    drop(engine);
    let engine = Engine::open(root.path(), 4).await.unwrap();
    assert_eq!(
        rows(&engine, &engine.session()).await[22],
        doc([
            ("_id", BsonValue::Int32(22)),
            ("value", BsonValue::from("new"))
        ])
    );
}

#[tokio::test]
async fn find_replace_preflights_both_return_images_and_does_not_store_the_projection() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let filter = doc([("_id", BsonValue::Int32(0))]);
    let large = doc([("payload", BsonValue::from("x".repeat(600000)))]);
    changed(&engine, &session, filter.clone(), large.clone(), true).await;
    let budget = RequestContext::new().with_result_limits(ResultLimits::new(1, 128).unwrap());
    for (after, replacement) in [(false, BsonDocument::new()), (true, large.clone())] {
        let error = engine
            .execute_document(
                &session,
                request(
                    find_replace(
                        filter.clone(),
                        replacement,
                        DocumentReadOptions::new(),
                        after,
                    ),
                    budget.clone(),
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(
            rows(&engine, &session).await[0].get_first("payload"),
            large.get_first("payload")
        );
    }
    let only_id = DocumentReadOptions::new()
        .with_projection(DocumentProjection::new(doc([("_id", BsonValue::Int32(1))])).unwrap());
    let execution = engine
        .execute_document(
            &session,
            request(
                find_replace(filter.clone(), large.clone(), only_id.clone(), true),
                budget,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        execution.result(),
        &DocumentResult::Document(Some(doc([("_id", BsonValue::Int32(0))])))
    );
    assert_eq!(
        rows(&engine, &session).await[0].get_first("payload"),
        large.get_first("payload")
    );
    // Root-depth-100 data is valid to store, but not to nest in a reply.
    let mut deep = BsonDocument::new();
    for _ in 1..briskdb::document::BSON_MAX_NESTING_DEPTH {
        deep = doc([("nested", BsonValue::Document(deep))]);
    }
    for after in [false, true] {
        changed(
            &engine,
            &session,
            filter.clone(),
            if after {
                BsonDocument::new()
            } else {
                deep.clone()
            },
            true,
        )
        .await;
        let before = rows(&engine, &session).await;
        let replacement = if after {
            deep.clone()
        } else {
            BsonDocument::new()
        };
        let error = engine
            .execute_document(
                &session,
                request(
                    find_replace(
                        filter.clone(),
                        replacement.clone(),
                        DocumentReadOptions::new(),
                        after,
                    ),
                    RequestContext::new(),
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(before, rows(&engine, &session).await);
        assert!(
            returned(
                &engine,
                &session,
                find_replace(filter.clone(), replacement, only_id.clone(), after)
            )
            .await
            .is_some()
        );
    }
}

#[tokio::test]
async fn find_replace_rejects_bad_runtime_sort_projection_identity_and_controls_before_writing() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    changed(
        &engine,
        &session,
        doc([("_id", BsonValue::Int32(0))]),
        doc([
            ("a", BsonValue::Array(vec![])),
            ("b", BsonValue::Array(vec![])),
        ]),
        true,
    )
    .await;
    let before = rows(&engine, &session).await;
    for after in [false, true] {
        for filter in [BsonDocument::new(), doc([("_id", BsonValue::Int32(0))])] {
            for options in [
                DocumentReadOptions::new().with_skip(1),
                DocumentReadOptions::new().with_sort(
                    DocumentSort::new(doc([
                        ("a", BsonValue::Int32(1)),
                        ("b", BsonValue::Int32(1)),
                    ]))
                    .unwrap(),
                ),
                DocumentReadOptions::new().with_projection(
                    DocumentProjection::new(doc([
                        ("a", BsonValue::Int32(1)),
                        ("b", BsonValue::Int32(0)),
                    ]))
                    .unwrap(),
                ),
            ] {
                assert!(
                    engine
                        .execute_document(
                            &session,
                            request(
                                find_replace(filter.clone(), BsonDocument::new(), options, after),
                                RequestContext::new()
                            )
                        )
                        .await
                        .is_err()
                );
            }
            let cancel = CancellationToken::new();
            cancel.cancel();
            assert_eq!(
                engine
                    .execute_document(
                        &session,
                        request(
                            find_replace(
                                filter.clone(),
                                BsonDocument::new(),
                                DocumentReadOptions::new(),
                                after
                            ),
                            RequestContext::new().with_cancellation_token(cancel)
                        )
                    )
                    .await
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::Cancelled
            );
            assert_eq!(
                engine
                    .execute_document(
                        &session,
                        request(
                            find_replace(
                                filter,
                                doc([("_id", BsonValue::Int32(99))]),
                                DocumentReadOptions::new(),
                                after
                            ),
                            RequestContext::new()
                        )
                    )
                    .await
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::InvalidArgument
            );
        }
    }
    assert_eq!(before, rows(&engine, &session).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_find_replacements_return_each_selected_id_once() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(root.path(), 4).await.unwrap());
    seed(&engine, &engine.session()).await;
    let mut tasks = Vec::new();
    for after in [false, true, false, true] {
        let engine = Arc::clone(&engine);
        tasks.push(tokio::spawn(async move {
            let session = engine.session();
            let mut ids = Vec::new();
            let options = DocumentReadOptions::new()
                .with_sort(DocumentSort::new(doc([("_id", BsonValue::Int32(1))])).unwrap());
            while let Some(document) = returned(
                &engine,
                &session,
                find_replace(
                    doc([("done", BsonValue::Boolean(false))]),
                    doc([("done", BsonValue::Boolean(true))]),
                    options.clone(),
                    after,
                ),
            )
            .await
            {
                assert_eq!(document.get_first("done"), Some(&BsonValue::Boolean(after)));
                let Some(BsonValue::Int32(id)) = document.get_first("_id") else {
                    panic!("id")
                };
                ids.push(*id);
            }
            ids
        }));
    }
    let mut ids = Vec::new();
    for task in tasks {
        ids.extend(task.await.unwrap());
    }
    ids.sort_unstable();
    assert_eq!(ids, (0..24).collect::<Vec<_>>());
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
    for command in [
        find_update(
            BsonDocument::new(),
            set(doc([("done", BsonValue::Boolean(true))])),
            DocumentReadOptions::new(),
            false,
        ),
        find_update(
            BsonDocument::new(),
            set(doc([("done", BsonValue::Boolean(true))])),
            DocumentReadOptions::new(),
            true,
        ),
        DocumentCommand::Update(update(
            BsonDocument::new(),
            set(doc([("done", BsonValue::Boolean(true))])),
        )),
        DocumentCommand::Replace(replace(BsonDocument::new(), BsonDocument::new())),
        find_replace(
            BsonDocument::new(),
            BsonDocument::new(),
            DocumentReadOptions::new(),
            false,
        ),
        find_replace(
            BsonDocument::new(),
            BsonDocument::new(),
            DocumentReadOptions::new(),
            true,
        ),
    ] {
        let error = engine
            .execute_document(
                &session,
                request(
                    command,
                    RequestContext::new()
                        .with_timeout(Duration::from_millis(150))
                        .unwrap(),
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
    }
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
