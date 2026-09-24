use super::*;
mod aggregation;
mod logical;
use crate::{
    core::RequestContext,
    document::{
        BsonBinary, BsonDateTime, BsonDecimal128, BsonJavaScript, BsonRegex, BsonUuid,
        DocumentContinueCursorRequest, DocumentCountRequest, DocumentCreateCollectionRequest,
        DocumentDeleteRequest, DocumentDistinctRequest, DocumentFindOneAndDeleteRequest,
        DocumentFindOneAndReplaceRequest, DocumentFindOneAndUpdateRequest, DocumentFindRequest,
        DocumentInsertRequest, DocumentReplaceRequest, DocumentRequestId, DocumentSort,
        DocumentUpdate, DocumentUpdateRequest, UuidRepresentation,
    },
};

fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}

fn ns() -> DocumentNamespace {
    DocumentNamespace::new("routing", "items").unwrap()
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

fn checkouts(engine: &Engine) -> Vec<u64> {
    engine
        .pool_snapshot_for_test()
        .unwrap()
        .shards
        .iter()
        .map(|shard| shard.checkouts)
        .collect()
}

fn assert_touched(engine: &Engine, before: &[u64], expected: &[u16]) {
    let snapshot = engine.pool_snapshot_for_test().unwrap();
    let touched = snapshot
        .shards
        .iter()
        .filter_map(|shard| {
            assert_eq!(shard.active, 0);
            (shard.checkouts > before[usize::from(shard.shard)]).then_some(shard.shard)
        })
        .collect::<Vec<_>>();
    assert_eq!(touched, expected, "actual physical shard checkouts");
}

async fn routed(
    engine: &Engine,
    session: &Session,
    command: DocumentCommand,
    shards: &[u16],
) -> DocumentExecution {
    let before = checkouts(engine);
    let result = call(engine, session, command).await;
    assert_eq!(result.plan().unwrap().shards(), shards);
    assert_touched(engine, &before, shards);
    result
}

async fn seed(engine: &Engine, session: &Session) {
    call(
        engine,
        session,
        DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
            ns(),
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
                ns(),
                (0..64)
                    .map(|id| {
                        doc([
                            ("_id", BsonValue::Int32(id)),
                            ("rank", BsonValue::Int32(64 - id)),
                        ])
                    })
                    .collect::<Vec<_>>(),
                DocumentWriteOptions::new(),
            )
            .unwrap(),
        ),
    )
    .await;
}

fn selected_ids(engine: &Engine) -> Vec<i32> {
    let mut ids = Vec::new();
    for target in [1, 6] {
        ids.extend(
            (0..64)
                .filter(|id| {
                    engine
                        .inner
                        .database
                        .storage
                        .prepare_document_id(&BsonValue::Int32(*id))
                        .unwrap()
                        .1
                        == target
                })
                .take(3),
        );
    }
    assert_eq!(ids.len(), 6);
    ids.sort_unstable();
    ids
}

fn in_values(ids: Vec<BsonValue>) -> DocumentFilter {
    DocumentFilter::new(doc([(
        "_id",
        BsonValue::Document(doc([("$in", BsonValue::Array(ids))])),
    )]))
    .unwrap()
}

fn filter(ids: &[i32]) -> DocumentFilter {
    // Query order, duplicates and equivalent numeric widths must not change
    // matching, output order, or the set of physical owners.
    in_values(
        ids.iter()
            .rev()
            .flat_map(|id| {
                [
                    BsonValue::Int64(i64::from(*id)),
                    BsonValue::Double(f64::from(*id)),
                ]
            })
            .collect(),
    )
}

fn find(filter: DocumentFilter, options: DocumentReadOptions) -> DocumentCommand {
    DocumentCommand::Find(DocumentFindRequest::new(ns(), filter, options))
}

async fn rows(
    engine: &Engine,
    session: &Session,
    filter: DocumentFilter,
    options: DocumentReadOptions,
    shards: &[u16],
) -> Vec<BsonDocument> {
    let mut result = routed(engine, session, find(filter, options), shards).await;
    let mut documents = Vec::new();
    loop {
        let DocumentResult::Cursor(batch) = result.into_parts().2 else {
            panic!("cursor");
        };
        documents.extend_from_slice(batch.documents());
        let Some(id) = batch.cursor_id() else {
            break;
        };
        result = routed(
            engine,
            session,
            DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
                ns(),
                id,
                DocumentReadOptions::new().with_batch_size(2).unwrap(),
            )),
            shards,
        )
        .await;
    }
    documents
}

fn ids(rows: &[BsonDocument]) -> Vec<BsonValue> {
    rows.iter()
        .map(|row| row.get_first("_id").unwrap().clone())
        .collect()
}

#[tokio::test]
async fn literal_in_reads_prune_actual_shards_and_preserve_global_pagination_after_restart() {
    let root = tempfile::tempdir().unwrap();
    for reopen in [false, true] {
        let engine = Engine::open(root.path(), 8).await.unwrap();
        let session = engine.session();
        if !reopen {
            seed(&engine, &session).await;
        }
        let selected = selected_ids(&engine);
        let query = filter(&selected);
        let actual = rows(
            &engine,
            &session,
            query.clone(),
            DocumentReadOptions::new().with_batch_size(2).unwrap(),
            &[1, 6],
        )
        .await;
        assert_eq!(
            ids(&actual),
            selected
                .iter()
                .copied()
                .map(BsonValue::Int32)
                .collect::<Vec<_>>()
        );

        let sorted = rows(
            &engine,
            &session,
            query.clone(),
            DocumentReadOptions::new()
                .with_sort(DocumentSort::new(doc([("rank", BsonValue::Int32(1))])).unwrap())
                .with_skip(1)
                .with_limit(4)
                .unwrap()
                .with_batch_size(2)
                .unwrap(),
            &[1, 6],
        )
        .await;
        assert_eq!(
            ids(&sorted),
            selected
                .iter()
                .rev()
                .skip(1)
                .take(4)
                .copied()
                .map(BsonValue::Int32)
                .collect::<Vec<_>>()
        );

        let counted = routed(
            &engine,
            &session,
            DocumentCommand::Count(DocumentCountRequest::new(
                ns(),
                query.clone(),
                DocumentReadOptions::new()
                    .with_skip(1)
                    .with_limit(3)
                    .unwrap(),
            )),
            &[1, 6],
        )
        .await;
        assert!(matches!(counted.result(), DocumentResult::Count(3)));
        let distinct = routed(
            &engine,
            &session,
            DocumentCommand::Distinct(
                DocumentDistinctRequest::new(
                    ns(),
                    "_id",
                    query.clone(),
                    DocumentReadOptions::new(),
                )
                .unwrap(),
            ),
            &[1, 6],
        )
        .await;
        let DocumentResult::Distinct(values) = distinct.result() else {
            panic!("distinct");
        };
        assert_eq!(values.as_ref(), ids(&actual));

        // Double negation keeps an equivalent, independent forced scatter;
        // positive conjunctions themselves can now establish an ID route.
        let forced = DocumentFilter::new(doc([(
            "$nor",
            BsonValue::Array(vec![BsonValue::Document(doc([(
                "$nor",
                BsonValue::Array(vec![BsonValue::Document(query.document().clone())]),
            )]))]),
        )]))
        .unwrap();
        let oracle = rows(
            &engine,
            &session,
            forced,
            DocumentReadOptions::new(),
            &(0..8).collect::<Vec<_>>(),
        )
        .await;
        assert_eq!(actual, oracle);
        engine.shutdown().await.unwrap();
    }
}

fn update(query: DocumentFilter, scope: DocumentMutationScope) -> DocumentUpdateRequest {
    DocumentUpdateRequest::new(
        ns(),
        query,
        DocumentUpdate::new(doc([(
            "$set",
            BsonValue::Document(doc([("changed", BsonValue::Int32(1))])),
        )]))
        .unwrap(),
        scope,
        DocumentWriteOptions::new(),
    )
}

#[tokio::test]
async fn literal_in_mutations_visit_only_selected_shards_and_keep_single_selection_order() {
    for mode in 0..8 {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), 8).await.unwrap();
        let session = engine.session();
        seed(&engine, &session).await;
        let selected = selected_ids(&engine);
        let query = filter(&selected);
        let replacement = || {
            DocumentReplaceRequest::new(
                ns(),
                query.clone(),
                doc([("changed", BsonValue::Int32(1))]),
                DocumentWriteOptions::new(),
            )
            .unwrap()
        };
        let descending = || {
            DocumentReadOptions::new()
                .with_sort(DocumentSort::new(doc([("rank", BsonValue::Int32(1))])).unwrap())
        };
        let command = match mode {
            0 => DocumentCommand::Update(update(query.clone(), DocumentMutationScope::One)),
            1 => DocumentCommand::Update(update(query.clone(), DocumentMutationScope::Many)),
            2 => DocumentCommand::Replace(replacement()),
            3 => DocumentCommand::FindOneAndUpdate(
                DocumentFindOneAndUpdateRequest::new(
                    update(query.clone(), DocumentMutationScope::One),
                    descending(),
                )
                .with_return_after(true),
            ),
            4 => DocumentCommand::FindOneAndReplace(
                DocumentFindOneAndReplaceRequest::new(replacement(), DocumentReadOptions::new())
                    .with_return_after(true),
            ),
            5 | 6 => DocumentCommand::Delete(DocumentDeleteRequest::new(
                ns(),
                query,
                if mode == 5 {
                    DocumentMutationScope::One
                } else {
                    DocumentMutationScope::Many
                },
                DocumentWriteOptions::new(),
            )),
            _ => DocumentCommand::FindOneAndDelete(DocumentFindOneAndDeleteRequest::new(
                ns(),
                query,
                descending(),
            )),
        };
        let result = routed(&engine, &session, command, &[1, 6]).await;
        let count = if mode == 1 || mode == 6 { 6 } else { 1 };
        match result.result() {
            DocumentResult::Update(result) => {
                assert_eq!(result.matched_count(), count);
                assert_eq!(result.modified_count(), count);
            }
            DocumentResult::Delete(result) => assert_eq!(result.deleted_count(), count),
            DocumentResult::Document(Some(result)) => assert_eq!(
                result.get_first("_id"),
                Some(&BsonValue::Int32(if mode == 3 || mode == 7 {
                    selected[5]
                } else {
                    selected[0]
                }))
            ),
            result => panic!("unexpected mutation result: {result:?}"),
        }
        engine.shutdown().await.unwrap();
        let engine = Engine::open(root.path(), 8).await.unwrap();
        let session = engine.session();
        let stored = rows(
            &engine,
            &session,
            DocumentFilter::empty(),
            DocumentReadOptions::new(),
            &(0..8).collect::<Vec<_>>(),
        )
        .await;
        let affected: Vec<_> = (0..64)
            .filter(|id| {
                let row = stored
                    .iter()
                    .find(|row| row.get_first("_id") == Some(&BsonValue::Int32(*id)));
                if mode >= 5 {
                    row.is_none()
                } else {
                    row.unwrap().get_first("changed").is_some()
                }
            })
            .collect();
        assert_eq!(
            affected,
            if mode == 1 || mode == 6 {
                selected.clone()
            } else {
                vec![if mode == 3 || mode == 7 {
                    selected[5]
                } else {
                    selected[0]
                }]
            }
        );
        engine.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn unsafe_empty_large_and_invalid_lists_keep_eager_validation_and_scan_fallback() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 8).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    for values in [
        vec![],
        vec![BsonValue::RegularExpression(
            BsonRegex::new("^x", "").unwrap(),
        )],
        vec![BsonValue::Int32(1); MAX_ROUTED_IDS + 1],
    ] {
        let query = in_values(values);
        let _ = routed(
            &engine,
            &session,
            find(query, DocumentReadOptions::new()),
            &(0..8).collect::<Vec<_>>(),
        )
        .await;
    }
    for operand in [
        BsonValue::Int32(1),
        BsonValue::Array(vec![
            BsonValue::Int32(1),
            BsonValue::Document(doc([("$unknown", BsonValue::Int32(1))])),
        ]),
    ] {
        let before = checkouts(&engine);
        let query =
            DocumentFilter::new(doc([("_id", BsonValue::Document(doc([("$in", operand)])))]))
                .unwrap();
        let error = engine
            .execute_document(&session, request(find(query, DocumentReadOptions::new())))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
        assert_touched(&engine, &before, &[]);
    }
    engine.shutdown().await.unwrap();
}

fn equivalent(value: &BsonValue) -> BsonValue {
    match value {
        BsonValue::Int32(value) => BsonValue::Double(f64::from(*value)),
        BsonValue::Document(value) => BsonValue::Document(
            BsonDocument::from_entries(value.iter().map(|(name, value)| (name, equivalent(value))))
                .unwrap(),
        ),
        BsonValue::Array(values) => BsonValue::Array(values.iter().map(equivalent).collect()),
        BsonValue::Decimal128(_) => BsonValue::Double(1.25),
        _ => value.clone(),
    }
}

#[tokio::test]
async fn bson_literal_lists_keep_canonical_identity_and_exact_array_semantics_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let values = vec![
        BsonValue::Null,
        BsonValue::Boolean(false),
        BsonValue::Int32(7),
        BsonValue::Int64(9_007_199_254_740_993),
        BsonValue::Double(f64::NAN),
        BsonValue::Double(f64::INFINITY),
        BsonValue::Double(f64::NEG_INFINITY),
        BsonValue::Decimal128(BsonDecimal128::parse("1.25").unwrap()),
        BsonValue::from("7"),
        BsonValue::Document(doc([
            ("a", BsonValue::Int32(1)),
            ("b", BsonValue::Int32(2)),
        ])),
        BsonValue::Document(doc([
            ("b", BsonValue::Int32(2)),
            ("a", BsonValue::Int32(1)),
        ])),
        BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(2)]),
        BsonValue::Binary(BsonBinary::new(0, vec![1, 2, 3])),
        BsonValue::Binary(BsonBinary::new(128, vec![1, 2, 3])),
        BsonValue::Uuid(BsonUuid::new([4; 16], UuidRepresentation::Standard)),
        BsonValue::ObjectId(BsonObjectId::from_bytes([3; 12])),
        BsonValue::DateTime(BsonDateTime::from_millis(-123)),
        BsonValue::Timestamp(BsonTimestamp::new(12, 34)),
        BsonValue::JavaScript(BsonJavaScript::new("value")),
        BsonValue::MinKey,
        BsonValue::MaxKey,
    ];
    for reopen in [false, true] {
        let engine = Engine::open(root.path(), 8).await.unwrap();
        let session = engine.session();
        if !reopen {
            call(
                &engine,
                &session,
                DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                    ns(),
                    DocumentCollectionOptions::empty(),
                    DocumentWriteOptions::new(),
                )),
            )
            .await;
            let inserted = call(
                &engine,
                &session,
                DocumentCommand::Insert(
                    DocumentInsertRequest::new(
                        ns(),
                        values
                            .iter()
                            .cloned()
                            .map(|id| doc([("_id", id)]))
                            .collect::<Vec<_>>(),
                        DocumentWriteOptions::new(),
                    )
                    .unwrap(),
                ),
            )
            .await;
            let DocumentResult::Insert(inserted) = inserted.result() else {
                panic!("insert");
            };
            assert!(inserted.write_errors().is_empty());
            assert_eq!(inserted.inserted_ids().len(), values.len());
        }
        for value in &values {
            let variant = equivalent(value);
            let (_, shard) = engine
                .inner
                .database
                .storage
                .prepare_document_id(value)
                .unwrap();
            assert_eq!(
                engine
                    .inner
                    .database
                    .storage
                    .prepare_document_id(&variant)
                    .unwrap()
                    .1,
                shard
            );
            let found = rows(
                &engine,
                &session,
                in_values(vec![value.clone(), variant.clone()]),
                DocumentReadOptions::new(),
                &[shard],
            )
            .await;
            assert_eq!(ids(&found), vec![value.clone()]);
            // Compound and nested positive constraints retain exactly the
            // same canonical identity, including arrays and ordered objects.
            let equality = doc([("_id", BsonValue::Document(doc([("$eq", variant)])))]);
            let absent = (
                "absent",
                BsonValue::Document(doc([("$exists", BsonValue::Boolean(false))])),
            );
            for query in [
                doc([("_id", value.clone()), absent.clone()]),
                doc([(
                    "$and",
                    BsonValue::Array(vec![
                        BsonValue::Document(equality),
                        BsonValue::Document(doc([absent])),
                    ]),
                )]),
            ] {
                let actual = rows(
                    &engine,
                    &session,
                    DocumentFilter::new(query).unwrap(),
                    DocumentReadOptions::new(),
                    &[shard],
                )
                .await;
                assert_eq!(ids(&actual), vec![value.clone()]);
            }
        }
        // An array ID is one identity, not a match for any scalar member.
        let scalar = BsonValue::Int32(1);
        let (_, shard) = engine
            .inner
            .database
            .storage
            .prepare_document_id(&scalar)
            .unwrap();
        assert!(
            rows(
                &engine,
                &session,
                in_values(vec![scalar]),
                DocumentReadOptions::new(),
                &[shard]
            )
            .await
            .is_empty()
        );
        engine.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn highest_shard_bit_is_retained_and_cancelled_routing_never_admits_a_shard() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 64).await.unwrap();
    let session = engine.session();
    call(
        &engine,
        &session,
        DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
            ns(),
            DocumentCollectionOptions::empty(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    let value = (0..4096)
        .map(BsonValue::Int32)
        .find(|id| {
            engine
                .inner
                .database
                .storage
                .prepare_document_id(id)
                .unwrap()
                .1
                == 63
        })
        .unwrap();
    let query = in_values(vec![value.clone(), value]);
    let found = routed(
        &engine,
        &session,
        find(query.clone(), DocumentReadOptions::new()),
        &[63],
    )
    .await;
    let DocumentResult::Cursor(batch) = found.result() else {
        panic!("cursor");
    };
    assert!(batch.documents().is_empty());
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let before = checkouts(&engine);
    let error = engine
        .execute_document(
            &session,
            DocumentRequest::new(
                DocumentRequestId::new([1; 16]).unwrap(),
                RequestContext::new().with_cancellation_token(cancellation),
                find(query, DocumentReadOptions::new()),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Cancelled);
    assert_touched(&engine, &before, &[]);
    engine.shutdown().await.unwrap();
}

#[cfg(feature = "tinymongo-import")]
#[tokio::test]
async fn imported_legacy_physical_ids_prune_by_restored_logical_identity_after_restart() {
    use crate::import::{TinyMongoImportOptions, TinyMongoImportPlan, import_tinymongo_database};

    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("legacy.sqlite");
    let destination = root.path().join("imported");
    let connection = rusqlite::Connection::open(&source).unwrap();
    connection
        .execute_batch("CREATE TABLE items (_id TEXT PRIMARY KEY, data TEXT NOT NULL)")
        .unwrap();
    for (physical, data) in [
        (
            "{'b': 2, 'a': 1}",
            r#"{"_id":{"a":1.0,"b":2.0},"label":"object"}"#,
        ),
        (
            "(1, {'b': 2, 'a': 1})",
            r#"{"_id":[1,{"a":1,"b":2}],"label":"array"}"#,
        ),
        ("7.0", r#"{"_id":7,"label":"number"}"#),
    ] {
        connection
            .execute(
                "INSERT INTO items VALUES (?1, ?2)",
                rusqlite::params![physical, data],
            )
            .unwrap();
    }
    drop(connection);
    let original = std::fs::read(&source).unwrap();
    let report = import_tinymongo_database(
        &source,
        &destination,
        &TinyMongoImportPlan::new("routing", ["items"]).unwrap(),
        TinyMongoImportOptions::new(8).unwrap(),
    )
    .unwrap();
    assert_eq!(report.legacy_physical_ids(), 3);
    assert_eq!(std::fs::read(&source).unwrap(), original);
    let object = BsonValue::Document(doc([
        ("b", BsonValue::Int32(2)),
        ("a", BsonValue::Int32(1)),
    ]));
    let values = [
        object.clone(),
        BsonValue::Array(vec![BsonValue::Int32(1), object]),
        BsonValue::Int32(7),
    ];
    for _ in 0..2 {
        let engine = Engine::open(&destination, 8).await.unwrap();
        let session = engine.session();
        for value in &values {
            let (_, shard) = engine
                .inner
                .database
                .storage
                .prepare_document_id(value)
                .unwrap();
            let found = rows(
                &engine,
                &session,
                in_values(vec![equivalent(value)]),
                DocumentReadOptions::new(),
                &[shard],
            )
            .await;
            assert_eq!(ids(&found), vec![value.clone()]);
        }
        engine.shutdown().await.unwrap();
    }
    assert_eq!(std::fs::read(&source).unwrap(), original);
}
