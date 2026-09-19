#![cfg(feature = "documents")]

use briskdb::{
    core::{CancellationToken, Engine, EngineErrorKind, RequestContext, ResultLimits, Session},
    document::{
        BsonDocument, BsonValue, DocumentAggregateRequest, DocumentAggregator,
        DocumentCollectionOptions, DocumentCommand, DocumentContinueCursorRequest,
        DocumentCreateCollectionRequest, DocumentCursorId, DocumentExecution,
        DocumentInsertRequest, DocumentKillCursorRequest, DocumentNamespace, DocumentPipeline,
        DocumentPlan, DocumentProjection, DocumentReadOptions, DocumentRequest, DocumentRequestId,
        DocumentResult, DocumentSort, DocumentWriteOptions, encode_document,
    },
};
use std::time::{Duration, Instant};

fn doc(entries: &[(&str, BsonValue)]) -> BsonDocument {
    BsonDocument::from_entries(entries.iter().map(|(name, value)| (*name, value.clone()))).unwrap()
}
fn namespace() -> DocumentNamespace {
    DocumentNamespace::new("app", "aggregate").unwrap()
}
fn pipeline(entries: &[(&str, BsonValue)]) -> DocumentPipeline {
    DocumentPipeline::new(
        entries
            .iter()
            .map(|entry| doc(std::slice::from_ref(entry)))
            .collect::<Vec<_>>(),
    )
    .unwrap()
}
fn aggregate(pipeline: DocumentPipeline, options: DocumentReadOptions) -> DocumentCommand {
    DocumentCommand::Aggregate(
        DocumentAggregateRequest::new(namespace(), pipeline, options).unwrap(),
    )
}
fn more(id: DocumentCursorId, size: u64) -> DocumentCommand {
    DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
        namespace(),
        id,
        DocumentReadOptions::new().with_batch_size(size).unwrap(),
    ))
}
fn request(command: DocumentCommand, context: RequestContext) -> DocumentRequest {
    DocumentRequest::new(DocumentRequestId::new([7; 16]).unwrap(), context, command)
}
async fn call(engine: &Engine, session: &Session, command: DocumentCommand) -> DocumentExecution {
    engine
        .execute_document(session, request(command, RequestContext::new()))
        .await
        .unwrap()
}
fn cursor(execution: DocumentExecution) -> (Option<DocumentCursorId>, Vec<BsonDocument>) {
    let DocumentResult::Cursor(batch) = execution.into_parts().2 else {
        panic!("cursor result");
    };
    let (_, id, documents) = batch.into_parts();
    (id, documents)
}
async fn setup(documents: Vec<BsonDocument>) -> (tempfile::TempDir, Engine) {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    call(
        &engine,
        &session,
        DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
            namespace(),
            DocumentCollectionOptions::empty(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    if !documents.is_empty() {
        call(
            &engine,
            &session,
            DocumentCommand::Insert(
                DocumentInsertRequest::new(namespace(), documents, DocumentWriteOptions::new())
                    .unwrap(),
            ),
        )
        .await;
    }
    (root, engine)
}
fn source_rows() -> Vec<BsonDocument> {
    (0..180)
        .map(|index| {
            doc(&[
                ("_id", BsonValue::Int64(index)),
                ("group", BsonValue::Int32((index % 3) as i32)),
                ("value", BsonValue::Int64((179 - index) % 7)),
                ("payload", BsonValue::String(format!("row-{index}"))),
            ])
        })
        .collect()
}
fn encoded(rows: &[BsonDocument]) -> Vec<Vec<u8>> {
    rows.iter()
        .map(|row| encode_document(row).unwrap())
        .collect()
}
async fn drain(
    engine: &Engine,
    session: &Session,
    first: DocumentExecution,
    size: u64,
) -> Vec<BsonDocument> {
    let (mut id, mut rows) = cursor(first);
    while let Some(current) = id {
        let (next, page) = cursor(call(engine, session, more(current, size)).await);
        assert!(page.len() <= size as usize);
        rows.extend(page);
        id = next;
    }
    rows
}

#[tokio::test]
async fn computed_keys_and_literal_count_pipeline_survive_paging_and_reopen() {
    let documents = source_rows();
    let (root, engine) = setup(documents.clone()).await;
    let group = |key| {
        BsonValue::Document(doc(&[
            ("_id", key),
            (
                "n",
                BsonValue::Document(doc(&[("$sum", BsonValue::Int32(1))])),
            ),
        ]))
    };
    let compound = pipeline(&[(
        "$group",
        group(BsonValue::Document(doc(&[
            ("team", BsonValue::from("$group")),
            ("missing", BsonValue::from("$absent")),
        ]))),
    )]);
    let count = pipeline(&[
        (
            "$match",
            BsonValue::Document(doc(&[("group", BsonValue::Int32(1))])),
        ),
        ("$skip", BsonValue::Int32(3)),
        ("$limit", BsonValue::Int32(7)),
        ("$group", group(BsonValue::Int32(1))),
    ]);
    let expected: Vec<_> = (0..3)
        .map(|key| {
            doc(&[
                (
                    "_id",
                    BsonValue::Document(doc(&[("team", BsonValue::Int32(key))])),
                ),
                ("n", BsonValue::Int32(60)),
            ])
        })
        .collect();
    for reopen in [false, true] {
        let current;
        let database = if reopen {
            engine.shutdown().await.unwrap();
            current = Engine::open(root.path(), 4).await.unwrap();
            &current
        } else {
            &engine
        };
        let session = database.session();
        let first = call(
            database,
            &session,
            aggregate(
                compound.clone(),
                DocumentReadOptions::new().with_batch_size(0).unwrap(),
            ),
        )
        .await;
        let rows = drain(database, &session, first, 1).await;
        assert_eq!(encoded(&rows), encoded(&expected));
        let (_, rows) = cursor(
            call(
                database,
                &session,
                aggregate(count.clone(), DocumentReadOptions::new()),
            )
            .await,
        );
        assert_eq!(
            encoded(&rows),
            encoded(&[doc(&[
                ("_id", BsonValue::Int32(1)),
                ("n", BsonValue::Int32(7))
            ])])
        );
        let first = call(
            database,
            &session,
            aggregate(
                DocumentPipeline::new(vec![]).unwrap(),
                DocumentReadOptions::new(),
            ),
        )
        .await;
        assert_eq!(
            encoded(&drain(database, &session, first, 17).await),
            encoded(&documents)
        );
        drop(session);
        if reopen {
            database.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn groups_follow_global_sorted_input_and_page_exact_results_after_reopen() {
    let documents = source_rows();
    let before = encoded(&documents);
    let accumulator = |name: &str, operand: BsonValue| BsonValue::Document(doc(&[(name, operand)]));
    let stages = pipeline(&[
        (
            "$sort",
            BsonValue::Document(doc(&[("_id", BsonValue::Int32(-1))])),
        ),
        (
            "$project",
            BsonValue::Document(doc(&[
                ("_id", BsonValue::Int32(1)),
                ("value", BsonValue::Int32(1)),
                (
                    "key",
                    BsonValue::Document(doc(&[("team", BsonValue::from("$group"))])),
                ),
            ])),
        ),
        (
            "$group",
            BsonValue::Document(doc(&[
                ("_id", BsonValue::from("$key")),
                ("n", accumulator("$sum", BsonValue::Int32(1))),
                ("sum", accumulator("$sum", BsonValue::from("$value"))),
                ("avg", accumulator("$avg", BsonValue::from("$value"))),
                ("first", accumulator("$first", BsonValue::from("$_id"))),
                ("last", accumulator("$last", BsonValue::from("$_id"))),
                ("ids", accumulator("$push", BsonValue::from("$_id"))),
                (
                    "unique",
                    accumulator("$addToSet", BsonValue::from("$value")),
                ),
                ("min", accumulator("$min", BsonValue::from("$value"))),
                ("max", accumulator("$max", BsonValue::from("$value"))),
            ])),
        ),
    ]);
    let expected: Vec<_> = (0..3)
        .rev()
        .map(|group| {
            let ids: Vec<_> = (0_i64..180).rev().filter(|id| id % 3 == group).collect();
            let values: Vec<_> = ids.iter().map(|id| (179 - id) % 7).collect();
            let mut unique = Vec::new();
            for value in &values {
                if !unique.contains(value) {
                    unique.push(*value);
                }
            }
            let sum: i64 = values.iter().sum();
            doc(&[
                (
                    "_id",
                    BsonValue::Document(doc(&[("team", BsonValue::Int32(group as i32))])),
                ),
                ("n", BsonValue::Int32(60)),
                ("sum", BsonValue::Int32(sum as i32)),
                ("avg", BsonValue::Double(sum as f64 / 60.0)),
                ("first", BsonValue::Int64(ids[0])),
                ("last", BsonValue::Int64(*ids.last().unwrap())),
                (
                    "ids",
                    BsonValue::Array(ids.into_iter().map(BsonValue::Int64).collect()),
                ),
                (
                    "unique",
                    BsonValue::Array(unique.into_iter().map(BsonValue::Int64).collect()),
                ),
                ("min", BsonValue::Int64(0)),
                ("max", BsonValue::Int64(6)),
            ])
        })
        .collect();
    let (root, engine) = setup(documents).await;
    let session = engine.session();
    let first = call(
        &engine,
        &session,
        aggregate(
            stages.clone(),
            DocumentReadOptions::new()
                .with_batch_size(0)
                .unwrap()
                .with_batch_byte_limit(1500)
                .unwrap(),
        ),
    )
    .await;
    assert!(
        matches!(first.plan(), Some(DocumentPlan::Scatter(plan)) if plan.shards() == [0, 1, 2, 3])
    );
    let (id, empty) = cursor(first);
    assert!(empty.is_empty());
    let first = call(&engine, &session, more(id.unwrap(), 3)).await;
    let DocumentResult::Cursor(batch) = first.result() else {
        panic!("cursor");
    };
    assert_eq!(
        batch.documents().len(),
        1,
        "soft byte cap pages group results"
    );
    assert_eq!(
        encoded(&drain(&engine, &session, first, 3).await),
        encoded(&expected)
    );
    let original = call(
        &engine,
        &session,
        aggregate(pipeline(&[]), DocumentReadOptions::new()),
    )
    .await;
    assert_eq!(
        encoded(&drain(&engine, &session, original, 100).await),
        before
    );
    engine.shutdown().await.unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let result = call(
        &engine,
        &session,
        aggregate(stages, DocumentReadOptions::new()),
    )
    .await;
    assert_eq!(encoded(&cursor(result).1), encoded(&expected));
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn group_expression_failure_discards_the_entire_result_and_cursor() {
    let (_root, engine) = setup(vec![
        doc(&[
            ("_id", BsonValue::Int32(1)),
            ("v", BsonValue::Array(vec![])),
        ]),
        doc(&[("_id", BsonValue::Int32(2)), ("v", BsonValue::Null)]),
    ])
    .await;
    let session = engine.session();
    let group = BsonValue::Document(doc(&[
        ("_id", BsonValue::from("$_id")),
        (
            "n",
            BsonValue::Document(doc(&[(
                "$first",
                BsonValue::Document(doc(&[("$size", BsonValue::from("$v"))])),
            )])),
        ),
    ]));
    let first = call(
        &engine,
        &session,
        aggregate(
            pipeline(&[("$group", group.clone())]),
            DocumentReadOptions::new().with_batch_size(0).unwrap(),
        ),
    )
    .await;
    let (id, rows) = cursor(first);
    assert!(rows.is_empty());
    let id = id.unwrap();
    assert_eq!(
        engine
            .execute_document(&session, request(more(id, 1), RequestContext::new()))
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::InvalidQuery
    );
    assert_eq!(
        engine
            .execute_document(&session, request(more(id, 1), RequestContext::new()))
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
    let limited = call(
        &engine,
        &session,
        aggregate(
            pipeline(&[("$limit", BsonValue::Int32(1)), ("$group", group)]),
            DocumentReadOptions::new(),
        ),
    )
    .await;
    assert_eq!(
        cursor(limited).1,
        vec![doc(&[
            ("_id", BsonValue::Int32(1)),
            ("n", BsonValue::Int32(0))
        ])]
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn transforms_share_global_execution_byte_paging_and_immutable_storage() {
    let mut documents = source_rows();
    for row in &mut documents {
        row.push(
            "items",
            BsonValue::Array(vec![
                BsonValue::Document(doc(&[("x", BsonValue::Int32(1))])),
                BsonValue::Null,
                BsonValue::Document(doc(&[])),
            ]),
        )
        .unwrap();
    }
    let before = encoded(&documents);
    let stages = pipeline(&[
        (
            "$set",
            BsonValue::Document(doc(&[
                ("items.tag", BsonValue::from("$payload")),
                ("copied", BsonValue::from("$value")),
                ("value", BsonValue::Int32(99)),
                ("gone.deep", BsonValue::from("$$REMOVE")),
            ])),
        ),
        (
            "$project",
            BsonValue::Document(doc(&[
                ("_id", BsonValue::Int32(1)),
                ("items", BsonValue::Int32(1)),
                ("prior", BsonValue::from("$copied")),
                (
                    "n",
                    BsonValue::Document(doc(&[("$size", BsonValue::from("$items"))])),
                ),
                (
                    "literal",
                    BsonValue::Document(doc(&[("$literal", BsonValue::from("$$REMOVE"))])),
                ),
            ])),
        ),
        (
            "$match",
            BsonValue::Document(doc(&[("n", BsonValue::Int32(3))])),
        ),
        (
            "$sort",
            BsonValue::Document(doc(&[
                ("prior", BsonValue::Int32(1)),
                ("_id", BsonValue::Int32(-1)),
            ])),
        ),
        ("$unset", BsonValue::from("literal")),
        ("$skip", BsonValue::Int32(2)),
        ("$limit", BsonValue::Int32(17)),
    ]);
    let expected = DocumentAggregator::compile(&stages)
        .unwrap()
        .execute(&documents)
        .unwrap();
    assert_eq!(expected.len(), 17);
    assert!(expected.iter().all(
        |row| matches!(row.get_first("prior"), Some(BsonValue::Int64(_)))
            && row.get_first("literal").is_none()
    ));
    let (root, engine) = setup(documents).await;
    let session = engine.session();
    let first = call(
        &engine,
        &session,
        aggregate(
            stages.clone(),
            DocumentReadOptions::new()
                .with_batch_size(0)
                .unwrap()
                .with_batch_byte_limit(700)
                .unwrap(),
        ),
    )
    .await;
    let actual = drain(&engine, &session, first, 5).await;
    assert_eq!(encoded(&actual), encoded(&expected));
    let original = call(
        &engine,
        &session,
        aggregate(pipeline(&[]), DocumentReadOptions::new()),
    )
    .await;
    assert_eq!(
        encoded(&drain(&engine, &session, original, 30).await),
        before
    );
    engine.shutdown().await.unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let first = call(
        &engine,
        &engine.session(),
        aggregate(stages, DocumentReadOptions::new()),
    )
    .await;
    assert_eq!(encoded(&cursor(first).1), encoded(&expected));
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn computed_errors_release_retained_cursors_and_limits_stop_evaluation() {
    let (root, engine) = setup(vec![
        doc(&[
            ("_id", BsonValue::Int32(1)),
            ("v", BsonValue::Array(vec![])),
        ]),
        doc(&[("_id", BsonValue::Int32(2)), ("v", BsonValue::Null)]),
    ])
    .await;
    let session = engine.session();
    let transform = BsonValue::Document(doc(&[(
        "n",
        BsonValue::Document(doc(&[("$size", BsonValue::from("$v"))])),
    )]));
    let first = call(
        &engine,
        &session,
        aggregate(
            pipeline(&[("$set", transform.clone())]),
            DocumentReadOptions::new().with_batch_size(1).unwrap(),
        ),
    )
    .await;
    let (id, rows) = cursor(first);
    assert_eq!(rows[0].get_first("n"), Some(&BsonValue::Int32(0)));
    let id = id.unwrap();
    let error = engine
        .execute_document(&session, request(more(id, 1), RequestContext::new()))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
    assert_eq!(
        engine
            .execute_document(&session, request(more(id, 1), RequestContext::new()))
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
    for sorted in [false, true] {
        let mut entries = vec![];
        if sorted {
            entries.push((
                "$sort",
                BsonValue::Document(doc(&[("_id", BsonValue::Int32(1))])),
            ));
        }
        entries.extend([("$set", transform.clone()), ("$limit", BsonValue::Int32(1))]);
        let rows = cursor(
            call(
                &engine,
                &session,
                aggregate(pipeline(&entries), DocumentReadOptions::new()),
            )
            .await,
        )
        .1;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_first("n"), Some(&BsonValue::Int32(0)));
    }
    engine.shutdown().await.unwrap();
    drop(root);
}

#[tokio::test]
async fn global_pipeline_order_exact_bson_and_byte_bounded_cursors_survive_reopen() {
    let documents = source_rows();
    let before = encoded(&documents);
    let stages = pipeline(&[
        (
            "$sort",
            BsonValue::Document(doc(&[("_id", BsonValue::Int32(-1))])),
        ),
        (
            "$match",
            BsonValue::Document(doc(&[("group", BsonValue::Int32(1))])),
        ),
        ("$skip", BsonValue::Int32(3)),
        ("$limit", BsonValue::Int32(30)),
        (
            "$sort",
            BsonValue::Document(doc(&[("value", BsonValue::Int32(1))])),
        ),
    ]);
    let expected = encoded(
        &DocumentAggregator::compile(&stages)
            .unwrap()
            .execute(&documents)
            .unwrap(),
    );
    let (root, engine) = setup(documents).await;
    let session = engine.session();
    let options = DocumentReadOptions::new()
        .with_batch_size(0)
        .unwrap()
        .with_batch_byte_limit(600)
        .unwrap();
    let first = call(&engine, &session, aggregate(stages.clone(), options)).await;
    assert!(
        matches!(first.plan(), Some(DocumentPlan::Scatter(plan)) if plan.shards() == [0, 1, 2, 3])
    );
    let (id, empty) = cursor(first);
    assert!(empty.is_empty());
    let first = call(&engine, &session, more(id.unwrap(), 7)).await;
    let DocumentResult::Cursor(batch) = first.result() else {
        panic!("cursor");
    };
    assert!(
        batch.documents().len() < 7,
        "the byte cap must split this page"
    );
    let actual = drain(&engine, &session, first, 7).await;
    assert_eq!(encoded(&actual), expected);
    let original = call(
        &engine,
        &session,
        aggregate(
            pipeline(&[]),
            DocumentReadOptions::new().with_batch_size(19).unwrap(),
        ),
    )
    .await;
    assert_eq!(
        encoded(&drain(&engine, &session, original, 19).await),
        before
    );
    let (stale, _) = cursor(
        call(
            &engine,
            &session,
            aggregate(
                stages.clone(),
                DocumentReadOptions::new().with_batch_size(1).unwrap(),
            ),
        )
        .await,
    );
    engine.shutdown().await.unwrap();
    let reopened = Engine::open(root.path(), 4).await.unwrap();
    let session = reopened.session();
    assert!(
        reopened
            .execute_document(
                &session,
                request(more(stale.unwrap(), 2), RequestContext::new())
            )
            .await
            .is_err()
    );
    let first = call(
        &reopened,
        &session,
        aggregate(
            stages,
            DocumentReadOptions::new().with_batch_size(4).unwrap(),
        ),
    )
    .await;
    assert_eq!(
        encoded(&drain(&reopened, &session, first, 4).await),
        expected
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn streaming_stage_counters_ownership_quotas_and_failed_delivery_are_shared() {
    let documents = source_rows();
    let stages = pipeline(&[
        ("$skip", BsonValue::Int32(7)),
        ("$limit", BsonValue::Int32(30)),
        (
            "$match",
            BsonValue::Document(doc(&[("group", BsonValue::Int32(1))])),
        ),
        ("$skip", BsonValue::Int32(1)),
        ("$limit", BsonValue::Int32(6)),
    ]);
    let expected = encoded(
        &DocumentAggregator::compile(&stages)
            .unwrap()
            .execute(&documents)
            .unwrap(),
    );
    let (_root, engine) = setup(documents).await;
    let session = engine.session();
    let first = call(
        &engine,
        &session,
        aggregate(
            stages,
            DocumentReadOptions::new().with_batch_size(2).unwrap(),
        ),
    )
    .await;
    assert_eq!(encoded(&drain(&engine, &session, first, 1).await), expected);
    let mut cursors = Vec::new();
    for _ in 0..8 {
        let (id, _) = cursor(
            call(
                &engine,
                &session,
                aggregate(
                    pipeline(&[]),
                    DocumentReadOptions::new().with_batch_size(0).unwrap(),
                ),
            )
            .await,
        );
        cursors.push(id.unwrap());
    }
    let error = engine
        .execute_document(
            &session,
            request(
                aggregate(
                    pipeline(&[]),
                    DocumentReadOptions::new().with_batch_size(0).unwrap(),
                ),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    let id = cursors[0];
    assert!(
        engine
            .execute_document(
                &engine.session(),
                request(more(id, 1), RequestContext::new())
            )
            .await
            .is_err()
    );
    assert!(
        engine
            .execute_document(
                &session,
                request(
                    DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
                        DocumentNamespace::new("app", "wrong").unwrap(),
                        id,
                        DocumentReadOptions::new(),
                    )),
                    RequestContext::new()
                )
            )
            .await
            .is_err()
    );
    let first = call(&engine, &session, more(id, 1)).await;
    assert_eq!(
        cursor(first).1[0].get_first("_id"),
        Some(&BsonValue::Int64(0))
    );
    let error = engine
        .execute_document(
            &session,
            request(
                more(id, 2),
                RequestContext::new().with_result_limits(ResultLimits::new(1, 8).unwrap()),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    assert!(
        engine
            .execute_document(&session, request(more(id, 1), RequestContext::new()))
            .await
            .is_err()
    );
    for id in cursors.into_iter().skip(1) {
        assert_eq!(
            call(
                &engine,
                &session,
                DocumentCommand::KillCursor(DocumentKillCursorRequest::new(
                    namespace(),
                    id,
                    DocumentWriteOptions::new()
                ))
            )
            .await
            .result(),
            &DocumentResult::CursorKilled(true)
        );
    }
    // An open aggregate cursor does not retain a SQLite lease or schema gate.
    let (id, _) = cursor(
        call(
            &engine,
            &session,
            aggregate(
                pipeline(&[(
                    "$sort",
                    BsonValue::Document(doc(&[("value", BsonValue::Int32(-1))])),
                )]),
                DocumentReadOptions::new().with_batch_size(1).unwrap(),
            ),
        )
        .await,
    );
    call(
        &engine,
        &engine.session(),
        DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
            DocumentNamespace::new("app", "while_aggregate_open").unwrap(),
            DocumentCollectionOptions::empty(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    session.close().await.unwrap();
    assert!(
        engine
            .execute_document(
                &engine.session(),
                request(more(id.unwrap(), 1), RequestContext::new())
            )
            .await
            .is_err()
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn aggregate_validation_controls_empty_counts_and_result_limits_are_eager() {
    let (_root, engine) = setup(Vec::new()).await;
    let session = engine.session();
    let count = pipeline(&[("$count", BsonValue::String("total".into()))]);
    assert!(
        cursor(
            call(
                &engine,
                &session,
                aggregate(count.clone(), DocumentReadOptions::new())
            )
            .await
        )
        .1
        .is_empty()
    );
    let token = CancellationToken::new();
    token.cancel();
    for (context, kind) in [
        (
            RequestContext::new().with_cancellation_token(token),
            EngineErrorKind::Cancelled,
        ),
        (
            RequestContext::new().with_deadline(Instant::now() - Duration::from_millis(1)),
            EngineErrorKind::DeadlineExceeded,
        ),
        (
            RequestContext::new().with_result_limits(ResultLimits::new(1, 8).unwrap()),
            EngineErrorKind::LimitExceeded,
        ),
    ] {
        assert_eq!(
            engine
                .execute_document(
                    &session,
                    request(
                        aggregate(count.clone(), DocumentReadOptions::new()),
                        context
                    )
                )
                .await
                .unwrap_err()
                .kind(),
            kind
        );
    }
    for options in [
        DocumentReadOptions::new().with_skip(1),
        DocumentReadOptions::new().with_limit(1).unwrap(),
        DocumentReadOptions::new()
            .with_projection(DocumentProjection::new(doc(&[("v", BsonValue::Int32(1))])).unwrap()),
        DocumentReadOptions::new()
            .with_sort(DocumentSort::new(doc(&[("v", BsonValue::Int32(1))])).unwrap()),
    ] {
        assert_eq!(
            engine
                .execute_document(
                    &session,
                    request(aggregate(pipeline(&[]), options), RequestContext::new())
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
    }
    let missing = DocumentNamespace::new("app", "never_created").unwrap();
    let bad = DocumentCommand::Aggregate(
        DocumentAggregateRequest::new(
            missing,
            pipeline(&[("$unknown-private", BsonValue::Null)]),
            DocumentReadOptions::new(),
        )
        .unwrap(),
    );
    let error = engine
        .execute_document(&session, request(bad, RequestContext::new()))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Unsupported);
    assert!(!format!("{error:?}").contains("private"));
    assert!(
        cursor(
            call(
                &engine,
                &session,
                aggregate(count, DocumentReadOptions::new())
            )
            .await
        )
        .1
        .is_empty()
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn streaming_count_does_not_materialize_large_inputs_or_spend_output_budget_on_them() {
    let (_root, engine) = setup(Vec::new()).await;
    let session = engine.session();
    // Together these inputs exceed the sort working-set quota, while each
    // bounded source frontier and individual BSON document still fits.
    for index in 0..12 {
        call(
            &engine,
            &session,
            DocumentCommand::Insert(
                DocumentInsertRequest::new(
                    namespace(),
                    [doc(&[
                        ("_id", BsonValue::Int32(index)),
                        ("payload", BsonValue::String("x".repeat(6_000_000))),
                    ])],
                    DocumentWriteOptions::new(),
                )
                .unwrap(),
            ),
        )
        .await;
    }
    let count = || {
        aggregate(
            pipeline(&[("$count", BsonValue::String("n".into()))]),
            DocumentReadOptions::new(),
        )
    };
    let result = engine
        .execute_document(
            &session,
            request(
                count(),
                RequestContext::new().with_result_limits(ResultLimits::new(1, 256).unwrap()),
            ),
        )
        .await
        .unwrap();
    assert_eq!(cursor(result).1, [doc(&[("n", BsonValue::Int32(12))])]);
    let sorted = aggregate(
        pipeline(&[(
            "$sort",
            BsonValue::Document(doc(&[("_id", BsonValue::Int32(1))])),
        )]),
        DocumentReadOptions::new().with_batch_size(1).unwrap(),
    );
    assert_eq!(
        engine
            .execute_document(&session, request(sorted, RequestContext::new()))
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    assert_eq!(
        cursor(call(&engine, &session, count()).await).1,
        [doc(&[("n", BsonValue::Int32(12))])]
    );
    let limited = aggregate(
        pipeline(&[
            ("$limit", BsonValue::Int32(1)),
            ("$count", BsonValue::String("n".into())),
        ]),
        DocumentReadOptions::new(),
    );
    assert_eq!(
        cursor(call(&engine, &session, limited).await).1,
        [doc(&[("n", BsonValue::Int32(1))])]
    );
    engine.shutdown().await.unwrap();
}
