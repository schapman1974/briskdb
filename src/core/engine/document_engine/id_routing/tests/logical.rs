use super::*;

fn logical(name: &str, clauses: Vec<BsonDocument>) -> BsonDocument {
    doc([(
        name,
        BsonValue::Array(clauses.into_iter().map(BsonValue::Document).collect()),
    )])
}

fn exact(id: i32) -> BsonDocument {
    doc([("_id", BsonValue::Int64(i64::from(id)))])
}

fn compound(id: i32, rank: i32) -> DocumentFilter {
    DocumentFilter::new(doc([
        ("_id", BsonValue::Double(f64::from(id))),
        ("rank", BsonValue::Int32(rank)),
    ]))
    .unwrap()
}

fn owner(engine: &Engine, id: i32) -> u16 {
    engine
        .inner
        .database
        .storage
        .prepare_document_id(&BsonValue::Int32(id))
        .unwrap()
        .1
}

pub(super) fn cases(engine: &Engine) -> Vec<(DocumentFilter, Vec<u16>)> {
    let selected = selected_ids(engine);
    let a = *selected.iter().find(|id| owner(engine, **id) == 1).unwrap();
    let b = *selected.iter().find(|id| owner(engine, **id) == 6).unwrap();
    let list = filter(&selected).document().clone();
    let rank = doc([("rank", BsonValue::Int32(64 - a))]);
    let contradiction = logical("$and", vec![exact(a), exact(b)]);
    let all = || (0..8).collect::<Vec<_>>();
    vec![
        (compound(a, 64 - a), vec![1]),
        (compound(a, -1), vec![1]),
        (
            DocumentFilter::new(logical("$and", vec![list.clone(), rank.clone()])).unwrap(),
            vec![1, 6],
        ),
        (
            DocumentFilter::new(logical(
                "$or",
                vec![
                    compound(a, 64 - a).document().clone(),
                    compound(b, -1).document().clone(),
                ],
            ))
            .unwrap(),
            vec![1, 6],
        ),
        (
            DocumentFilter::new(logical(
                "$and",
                vec![list.clone(), logical("$or", vec![rank.clone(), exact(b)])],
            ))
            .unwrap(),
            vec![1, 6],
        ),
        (
            DocumentFilter::new(logical("$or", vec![exact(a), rank.clone()])).unwrap(),
            all(),
        ),
        (
            DocumentFilter::new(doc([(
                "_id",
                BsonValue::Document(doc([
                    ("$eq", BsonValue::Int32(a)),
                    ("$ne", BsonValue::Int32(a)),
                ])),
            )]))
            .unwrap(),
            vec![1],
        ),
        (
            DocumentFilter::new(logical(
                "$and",
                vec![list.clone(), logical("$and", vec![rank, exact(a)])],
            ))
            .unwrap(),
            vec![1],
        ),
        (
            DocumentFilter::new(logical("$or", vec![contradiction.clone(), exact(b)])).unwrap(),
            vec![6],
        ),
        (DocumentFilter::new(contradiction).unwrap(), all()),
        (
            DocumentFilter::new(logical("$nor", vec![exact(a)])).unwrap(),
            all(),
        ),
        (
            DocumentFilter::new(doc([(
                "_id",
                BsonValue::Document(doc([(
                    "$not",
                    BsonValue::Document(doc([("$eq", BsonValue::Int32(a))])),
                )])),
            )]))
            .unwrap(),
            all(),
        ),
        (
            DocumentFilter::new(doc([("_id.part", BsonValue::Int32(a))])).unwrap(),
            all(),
        ),
        (
            DocumentFilter::new(doc([(
                "items",
                BsonValue::Document(doc([("$elemMatch", BsonValue::Document(exact(a)))])),
            )]))
            .unwrap(),
            all(),
        ),
        (
            DocumentFilter::new(logical("$or", vec![list, BsonDocument::new()])).unwrap(),
            all(),
        ),
    ]
}

#[tokio::test]
async fn logical_reads_counts_distinct_and_sorted_pages_match_forced_scatter_after_restart() {
    let root = tempfile::tempdir().unwrap();
    for reopen in [false, true] {
        let engine = Engine::open(root.path(), 8).await.unwrap();
        let session = engine.session();
        if !reopen {
            seed(&engine, &session).await;
        }
        for (query, shards) in cases(&engine) {
            let forced = DocumentFilter::new(logical(
                "$nor",
                vec![logical("$nor", vec![query.document().clone()])],
            ))
            .unwrap();
            let expected = rows(
                &engine,
                &session,
                forced,
                DocumentReadOptions::new(),
                &(0..8).collect::<Vec<_>>(),
            )
            .await;
            let actual = rows(
                &engine,
                &session,
                query.clone(),
                DocumentReadOptions::new().with_batch_size(2).unwrap(),
                &shards,
            )
            .await;
            assert_eq!(actual, expected, "query: {query:?}");
            let sorted = rows(
                &engine,
                &session,
                query.clone(),
                DocumentReadOptions::new()
                    .with_sort(DocumentSort::new(doc([("rank", BsonValue::Int32(1))])).unwrap())
                    .with_skip(1)
                    .with_limit(3)
                    .unwrap()
                    .with_batch_size(1)
                    .unwrap(),
                &shards,
            )
            .await;
            assert_eq!(
                ids(&sorted),
                ids(&expected)
                    .into_iter()
                    .rev()
                    .skip(1)
                    .take(3)
                    .collect::<Vec<_>>()
            );
            let count = routed(
                &engine,
                &session,
                DocumentCommand::Count(DocumentCountRequest::new(
                    ns(),
                    query.clone(),
                    DocumentReadOptions::new(),
                )),
                &shards,
            )
            .await;
            assert!(
                matches!(count.result(), DocumentResult::Count(n) if *n == expected.len() as u64)
            );
            let distinct = routed(
                &engine,
                &session,
                DocumentCommand::Distinct(
                    DocumentDistinctRequest::new(ns(), "_id", query, DocumentReadOptions::new())
                        .unwrap(),
                ),
                &shards,
            )
            .await;
            let DocumentResult::Distinct(values) = distinct.result() else {
                panic!("distinct");
            };
            assert_eq!(values.as_ref(), ids(&expected));
        }
        engine.shutdown().await.unwrap();
    }
}

fn mutation(mode: u8, query: DocumentFilter, upsert: bool) -> DocumentCommand {
    let options = DocumentWriteOptions::new().with_upsert(upsert);
    let update = || {
        DocumentUpdateRequest::new(
            ns(),
            query.clone(),
            DocumentUpdate::new(doc([(
                "$set",
                BsonValue::Document(doc([("changed", BsonValue::Int32(1))])),
            )]))
            .unwrap(),
            if mode == 1 {
                DocumentMutationScope::Many
            } else {
                DocumentMutationScope::One
            },
            options,
        )
    };
    let replacement = || {
        DocumentReplaceRequest::new(
            ns(),
            query.clone(),
            doc([("changed", BsonValue::Int32(1))]),
            options,
        )
        .unwrap()
    };
    let descending = || {
        DocumentReadOptions::new()
            .with_sort(DocumentSort::new(doc([("rank", BsonValue::Int32(1))])).unwrap())
    };
    match mode {
        0 | 1 => DocumentCommand::Update(update()),
        2 => DocumentCommand::Replace(replacement()),
        3 => DocumentCommand::FindOneAndUpdate(
            DocumentFindOneAndUpdateRequest::new(update(), descending()).with_return_after(true),
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
            options,
        )),
        _ => DocumentCommand::FindOneAndDelete(DocumentFindOneAndDeleteRequest::new(
            ns(),
            query,
            descending(),
        )),
    }
}

#[tokio::test]
async fn all_mutations_recheck_compound_predicates_and_keep_logical_selection_order() {
    for mode in 0..8 {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), 8).await.unwrap();
        let session = engine.session();
        seed(&engine, &session).await;
        let selected = selected_ids(&engine);
        let id = selected[0];
        let miss = routed(
            &engine,
            &session,
            mutation(mode, compound(id, -1), false),
            &[owner(&engine, id)],
        )
        .await;
        match miss.result() {
            DocumentResult::Update(result) => {
                assert_eq!(result.matched_count(), 0);
                assert_eq!(result.modified_count(), 0);
            }
            DocumentResult::Delete(result) => assert_eq!(result.deleted_count(), 0),
            DocumentResult::Document(None) => (),
            other => panic!("unexpected miss: {other:?}"),
        }
        // A routed existing ID with a false extra condition must remain an
        // upsert conflict, never become an update of the nonmatching record.
        if mode < 5 {
            let error = engine
                .execute_document(&session, request(mutation(mode, compound(id, -1), true)))
                .await
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::UniqueViolation);
        }
        let before = rows(
            &engine,
            &session,
            DocumentFilter::empty(),
            DocumentReadOptions::new(),
            &(0..8).collect::<Vec<_>>(),
        )
        .await;
        assert_eq!(
            before,
            (0..64)
                .map(|id| doc([
                    ("_id", BsonValue::Int32(id)),
                    ("rank", BsonValue::Int32(64 - id))
                ]))
                .collect::<Vec<_>>()
        );
        let rank = doc([(
            "rank",
            BsonValue::Document(doc([("$gte", BsonValue::Int32(64 - selected[2]))])),
        )]);
        let query = DocumentFilter::new(logical(
            "$and",
            vec![filter(&selected).document().clone(), rank],
        ))
        .unwrap();
        let result = routed(&engine, &session, mutation(mode, query, false), &[1, 6]).await;
        let count = if mode == 1 || mode == 6 { 3 } else { 1 };
        let chosen = if mode == 3 || mode == 7 {
            selected[2]
        } else {
            selected[0]
        };
        match result.result() {
            DocumentResult::Update(result) => {
                assert_eq!(result.matched_count(), count);
                assert_eq!(result.modified_count(), count);
            }
            DocumentResult::Delete(result) => assert_eq!(result.deleted_count(), count),
            DocumentResult::Document(Some(result)) => {
                assert_eq!(result.get_first("_id"), Some(&BsonValue::Int32(chosen)))
            }
            other => panic!("unexpected mutation: {other:?}"),
        }
        engine.shutdown().await.unwrap();
        let engine = Engine::open(root.path(), 8).await.unwrap();
        let stored = rows(
            &engine,
            &engine.session(),
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
                selected[..3].to_vec()
            } else {
                vec![chosen]
            }
        );
        engine.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn explicit_equality_keeps_regex_and_operator_named_ids_literal() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 8).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let regex = BsonValue::RegularExpression(BsonRegex::new("^needle", "").unwrap());
    let values = [
        regex.clone(),
        BsonValue::Document(doc([("$eq", BsonValue::Int32(7))])),
        BsonValue::Array(vec![regex]),
        BsonValue::from("needle"),
    ];
    let inserted = call(
        &engine,
        &session,
        DocumentCommand::Insert(
            DocumentInsertRequest::new(
                ns(),
                values
                    .iter()
                    .cloned()
                    .map(|id| doc([("_id", id), ("tag", BsonValue::Int32(1))]))
                    .collect::<Vec<_>>(),
                DocumentWriteOptions::new(),
            )
            .unwrap(),
        ),
    )
    .await;
    let DocumentResult::Insert(result) = inserted.result() else {
        panic!("insert");
    };
    assert!(result.write_errors().is_empty());
    for value in values {
        let (_, shard) = engine
            .inner
            .database
            .storage
            .prepare_document_id(&value)
            .unwrap();
        let query = DocumentFilter::new(doc([
            ("_id", BsonValue::Document(doc([("$eq", value.clone())]))),
            ("tag", BsonValue::Int32(1)),
        ]))
        .unwrap();
        let actual = rows(
            &engine,
            &session,
            query,
            DocumentReadOptions::new(),
            &[shard],
        )
        .await;
        assert_eq!(ids(&actual), vec![value]);
    }
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn logical_routes_bound_total_id_work_and_validate_hidden_branches_before_io() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 8).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let id = selected_ids(&engine)[0];
    let token = CancellationToken::new();
    let control = OperationControl::new(None);
    for count in [MAX_ROUTED_IDS, MAX_ROUTED_IDS + 1] {
        let query = DocumentFilter::new(logical("$or", vec![exact(id); count])).unwrap();
        DocumentMatcher::compile(query.document()).unwrap();
        assert_eq!(
            proven_id_shards(&engine.inner.database.storage, &query, &token, &control).unwrap(),
            (count == MAX_ROUTED_IDS).then_some(1_u64 << owner(&engine, id))
        );
    }
    let invalid = doc([(
        "bad",
        BsonValue::Document(doc([("$unknown", BsonValue::Int32(1))])),
    )]);
    for op in ["$and", "$or"] {
        let query = DocumentFilter::new(logical(op, vec![exact(id), invalid.clone()])).unwrap();
        let before = checkouts(&engine);
        let error = engine
            .execute_document(&session, request(find(query, DocumentReadOptions::new())))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Unsupported);
        assert_touched(&engine, &before, &[]);
    }
    for deadline in [false, true] {
        let token = CancellationToken::new();
        let mut context = RequestContext::new().with_cancellation_token(token.clone());
        if deadline {
            context = context
                .with_deadline(std::time::Instant::now() - std::time::Duration::from_secs(1));
        } else {
            token.cancel();
        }
        let before = checkouts(&engine);
        let error = engine
            .execute_document(
                &session,
                DocumentRequest::new(
                    DocumentRequestId::new([1; 16]).unwrap(),
                    context,
                    find(compound(id, 64 - id), DocumentReadOptions::new()),
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            if deadline {
                EngineErrorKind::DeadlineExceeded
            } else {
                EngineErrorKind::Cancelled
            }
        );
        assert_touched(&engine, &before, &[]);
    }
    engine.shutdown().await.unwrap();
}
