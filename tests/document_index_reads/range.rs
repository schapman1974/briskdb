use super::*;
use briskdb::document::{BsonDecimal128, DocumentCandidateKind, DocumentPlan, DocumentReadAccess};

fn query(path: &'static str, operator: &'static str, value: BsonValue) -> BsonDocument {
    doc([(path, obj([(operator, value)]))])
}
fn selected() -> BsonDocument {
    query("a", "$gte", BsonValue::from("k090"))
}
fn index() -> DocumentIndexRequest {
    DocumentIndexRequest::new(doc([("a", BsonValue::Int32(-1))])).unwrap()
}
async fn fixture(engine: &Engine, session: &Session, namespace: &DocumentNamespace) {
    call(
        engine,
        session,
        DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
            namespace.clone(),
            DocumentCollectionOptions::empty(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    let mut values: Vec<_> = (0..100)
        .map(|i| Some(BsonValue::from(format!("k{i:03}"))))
        .collect();
    values.extend([
        None,
        Some(BsonValue::Null),
        Some(BsonValue::Int64(9_007_199_254_740_993)),
        Some(BsonValue::Decimal128(
            BsonDecimal128::parse("1.00").unwrap(),
        )),
        Some(BsonValue::Double(f64::NAN)),
        Some(BsonValue::Boolean(true)),
        Some(BsonValue::from("")),
        Some(BsonValue::from("k090\0")),
        Some(BsonValue::from("é")),
        Some(BsonValue::Array(vec![])),
        Some(BsonValue::Array(vec![
            BsonValue::from("k000"),
            BsonValue::from("k099"),
            BsonValue::from("k099"),
        ])),
        Some(BsonValue::Array(vec![BsonValue::Array(vec![
            BsonValue::from("k099"),
        ])])),
        Some(obj([("s", BsonValue::from("zz"))])),
        Some(BsonValue::Array(vec![
            obj([("s", BsonValue::from("zz"))]),
            obj([("s", BsonValue::from("a"))]),
        ])),
    ]);
    let records: Vec<_> = values
        .into_iter()
        .enumerate()
        .map(|(i, value)| {
            let mut document = doc([
                ("_id", BsonValue::Int32(i as i32)),
                ("rank", BsonValue::Int32(i as i32)),
                ("enabled", BsonValue::Boolean(i % 2 == 0)),
            ]);
            if let Some(value) = value {
                document.push("a", value).unwrap();
            }
            document
        })
        .collect();
    call(
        engine,
        session,
        DocumentCommand::Insert(
            DocumentInsertRequest::new(namespace.clone(), records, DocumentWriteOptions::new())
                .unwrap(),
        ),
    )
    .await;
}

fn options() -> DocumentReadOptions {
    DocumentReadOptions::new()
        .with_plan_diagnostics(true)
        .with_execution_stats(true)
}
async fn counted(
    engine: &Engine,
    session: &Session,
    namespace: &DocumentNamespace,
    query: &BsonDocument,
) -> DocumentExecution {
    call(
        engine,
        session,
        DocumentCommand::Count(DocumentCountRequest::new(
            namespace.clone(),
            DocumentFilter::new(query.clone()).unwrap(),
            DocumentReadOptions::new(),
        )),
    )
    .await
}
async fn observed(
    engine: &Engine,
    session: &Session,
    namespace: &DocumentNamespace,
    query: &BsonDocument,
) -> DocumentExecution {
    call(
        engine,
        session,
        DocumentCommand::Find(DocumentFindRequest::new(
            namespace.clone(),
            DocumentFilter::new(query.clone()).unwrap(),
            options().with_batch_size(1000).unwrap(),
        )),
    )
    .await
}
fn assert_range(result: &DocumentExecution) {
    assert!(matches!(result.plan(), Some(DocumentPlan::Scatter(plan))
        if matches!(plan.read_access(), Some(DocumentReadAccess::IndexCandidates { kind: DocumentCandidateKind::StringRange, key_count: 1, .. }))));
}

async fn distinct(
    engine: &Engine,
    session: &Session,
    namespace: &DocumentNamespace,
    query: &BsonDocument,
) -> DocumentResult {
    call(
        engine,
        session,
        DocumentCommand::Distinct(
            DocumentDistinctRequest::new(
                namespace.clone(),
                "rank",
                DocumentFilter::new(query.clone()).unwrap(),
                options(),
            )
            .unwrap(),
        ),
    )
    .await
    .into_parts()
    .2
}

#[tokio::test]
async fn string_range_reads_equal_scans_with_sparse_partial_multikey_and_restart() {
    for mode in 0..4 {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), 4).await.unwrap();
        let session = engine.session();
        let namespace = ns("string_ranges");
        fixture(&engine, &session, &namespace).await;
        let path = if mode == 3 { "a.s" } else { "a" };
        let mut queries = Vec::new();
        for operator in ["$gt", "$gte", "$lt", "$lte"] {
            for value in [
                BsonValue::from("k090"),
                BsonValue::from("é"),
                BsonValue::Int32(1),
            ] {
                let mut query = query(path, operator, value);
                if mode == 2 {
                    query.push("enabled", BsonValue::Boolean(true)).unwrap();
                }
                queries.push(query);
            }
        }
        queries.push(doc([(
            path,
            obj([
                ("$gt", BsonValue::from("k090")),
                ("$lt", BsonValue::from("k010")),
            ]),
        )]));
        queries.push(doc([(
            "$or",
            BsonValue::Array(vec![
                BsonValue::Document(selected()),
                obj([("a", BsonValue::Null)]),
            ]),
        )]));
        let read = options()
            .with_batch_size(3)
            .unwrap()
            .with_sort(DocumentSort::new(doc([("rank", BsonValue::Int32(-1))])).unwrap())
            .with_projection(DocumentProjection::new(doc([("a", BsonValue::Int32(1))])).unwrap())
            .with_skip(1)
            .with_limit(7)
            .unwrap();
        let mut expected = Vec::new();
        for query in &queries {
            expected.push((
                find(&engine, &session, &namespace, query, read.clone()).await,
                counted(&engine, &session, &namespace, query).await,
                distinct(&engine, &session, &namespace, query).await,
            ));
        }
        let mut declaration =
            DocumentIndexRequest::new(doc([(path, BsonValue::Int32(-1))])).unwrap();
        if mode == 1 {
            declaration = declaration.with_sparse(true);
        }
        if mode == 2 {
            declaration = declaration.with_partial_filter(
                DocumentFilter::new(doc([("enabled", BsonValue::Boolean(true))])).unwrap(),
            );
        }
        build(&engine, &session, &namespace, declaration).await;
        for (i, query) in queries.iter().enumerate() {
            let actual = counted(&engine, &session, &namespace, query).await;
            assert_eq!(actual.result(), expected[i].1.result());
            assert_eq!(
                distinct(&engine, &session, &namespace, query).await,
                expected[i].2
            );
            if i < 12 && i % 3 != 2 {
                assert_range(&observed(&engine, &session, &namespace, query).await);
            }
            assert_eq!(
                find(&engine, &session, &namespace, query, read.clone()).await,
                expected[i].0
            );
        }
        if mode == 0 {
            let reduced = observed(&engine, &session, &namespace, &selected()).await;
            assert!(reduced.read_stats().unwrap().documents_examined() < 25);
            assert_eq!(reduced.read_stats().unwrap().shards_read().count(), 4);
        }
        engine.shutdown().await.unwrap();
        let reopened = Engine::open(root.path(), 4).await.unwrap();
        let session = reopened.session();
        let actual = counted(&reopened, &session, &namespace, &queries[0]).await;
        assert_range(&observed(&reopened, &session, &namespace, &queries[0]).await);
        assert_eq!(actual.result(), expected[0].1.result());
        assert_eq!(
            find(&reopened, &session, &namespace, &queries[0], read).await,
            expected[0].0
        );
        reopened.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn string_range_cursor_reselects_after_drop_rebuild_and_membership_changes() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("range_cursor");
    fixture(&engine, &session, &namespace).await;
    build(&engine, &session, &namespace, index()).await;
    let expected = find(&engine, &session, &namespace, &selected(), options()).await;
    let first = call(
        &engine,
        &session,
        DocumentCommand::Find(DocumentFindRequest::new(
            namespace.clone(),
            DocumentFilter::new(selected()).unwrap(),
            options().with_batch_size(2).unwrap(),
        )),
    )
    .await;
    assert_range(&first);
    let (mut cursor, mut documents) = page(first);
    drop_index(&engine, &session, &namespace).await;
    let next = call(
        &engine,
        &session,
        DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
            namespace.clone(),
            cursor.unwrap(),
            options().with_batch_size(2).unwrap(),
        )),
    )
    .await;
    assert!(
        matches!(next.plan(), Some(DocumentPlan::Scatter(plan)) if matches!(plan.read_access(), Some(DocumentReadAccess::Scan { .. })))
    );
    let (id, batch) = page(next);
    cursor = id;
    documents.extend(batch);
    build(&engine, &session, &namespace, index()).await;
    while let Some(id) = cursor {
        let next = call(
            &engine,
            &session,
            DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
                namespace.clone(),
                id,
                options().with_batch_size(2).unwrap(),
            )),
        )
        .await;
        assert_range(&next);
        let (id, batch) = page(next);
        cursor = id;
        documents.extend(batch);
    }
    assert_eq!(
        documents
            .iter()
            .map(|document| encode_document(document).unwrap())
            .collect::<Vec<_>>(),
        expected
    );
    let changed = call(
        &engine,
        &session,
        DocumentCommand::Update(DocumentUpdateRequest::new(
            namespace.clone(),
            DocumentFilter::new(selected()).unwrap(),
            DocumentUpdate::new(doc([("$set", obj([("a", BsonValue::from("000"))]))])).unwrap(),
            DocumentMutationScope::Many,
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    let DocumentResult::Update(result) = changed.result() else {
        panic!("update")
    };
    assert_eq!(result.modified_count(), expected.len() as u64);
    assert_eq!(
        counted(&engine, &session, &namespace, &selected())
            .await
            .result(),
        &DocumentResult::Count(0)
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn string_range_checks_the_selected_multikey_entry_and_fails_closed_on_corruption() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("range_checksum");
    fixture(&engine, &session, &namespace).await;
    build(&engine, &session, &namespace, index()).await;
    let key = CanonicalBsonKey::encode(&BsonValue::Int32(110)).unwrap();
    let mut corrupted = 0;
    for shard in 0..4 {
        let connection =
            rusqlite::Connection::open(root.path().join(format!("shards/{shard:04}.sqlite")))
                .unwrap();
        corrupted += connection.execute("UPDATE briskdb_document_index_entries_v1 SET entry_checksum=zeroblob(32) WHERE id_key=?1", [key.as_bytes()]).unwrap();
    }
    assert_eq!(corrupted, 2);
    let error = engine
        .execute_document(
            &session,
            DocumentRequest::new(
                DocumentRequestId::new([1; 16]).unwrap(),
                RequestContext::new(),
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace,
                    DocumentFilter::new(selected()).unwrap(),
                    options(),
                )),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
    engine.shutdown().await.unwrap();
    drop(engine);
    assert_eq!(
        Engine::open(root.path(), 4).await.unwrap_err().kind(),
        EngineErrorKind::DataCorruption
    );
}

#[tokio::test]
async fn string_range_mutations_match_scans_without_revisiting_array_members() {
    for mode in 0..8 {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), 4).await.unwrap();
        let session = engine.session();
        for name in ["scan", "indexed"] {
            fixture(&engine, &session, &ns(name)).await;
        }
        build(&engine, &session, &ns("indexed"), index()).await;
        assert_range(&observed(&engine, &session, &ns("indexed"), &selected()).await);
        let mut outcomes = Vec::new();
        for name in ["scan", "indexed"] {
            outcomes.push(
                call(
                    &engine,
                    &session,
                    membership::mutation_with_filter(
                        ns(name),
                        mode,
                        DocumentFilter::new(selected()).unwrap(),
                    ),
                )
                .await
                .into_parts()
                .2,
            );
        }
        assert_eq!(outcomes[0], outcomes[1], "mode {mode}");
        assert_eq!(
            find(&engine, &session, &ns("indexed"), &doc([]), options()).await,
            find(&engine, &session, &ns("scan"), &doc([]), options()).await
        );
        engine.shutdown().await.unwrap();
        let reopened = Engine::open(root.path(), 4).await.unwrap();
        let session = reopened.session();
        assert_eq!(
            find(&reopened, &session, &ns("indexed"), &doc([]), options()).await,
            find(&reopened, &session, &ns("scan"), &doc([]), options()).await
        );
        reopened.shutdown().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "manual same-root string-range candidate benchmark; timing is not a CI assertion"]
async fn string_range_candidate_benchmark() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("range_benchmark");
    call(
        &engine,
        &session,
        DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
            namespace.clone(),
            DocumentCollectionOptions::empty(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    let records: Vec<_> = (0..1000)
        .map(|i| {
            doc([
                ("_id", BsonValue::Int32(i)),
                ("a", BsonValue::from(format!("k{i:04}"))),
                ("payload", BsonValue::from("x".repeat(4096))),
            ])
        })
        .collect();
    call(
        &engine,
        &session,
        DocumentCommand::Insert(
            DocumentInsertRequest::new(namespace.clone(), records, DocumentWriteOptions::new())
                .unwrap(),
        ),
    )
    .await;
    for indexed in [false, true] {
        if indexed {
            build(&engine, &session, &namespace, index()).await;
        }
        for (bound, count) in [("k0990", 10), ("z", 0)] {
            let query = query("a", "$gte", BsonValue::from(bound));
            observed(&engine, &session, &namespace, &query).await;
            let started = Instant::now();
            let mut documents_examined = 0;
            for _ in 0..10 {
                let result = observed(&engine, &session, &namespace, &query).await;
                let DocumentResult::Cursor(batch) = result.result() else {
                    panic!("cursor")
                };
                assert!(batch.cursor_id().is_none());
                assert_eq!(batch.documents().len() as u64, count);
                if indexed {
                    assert_range(&result);
                }
                documents_examined += result.read_stats().unwrap().documents_examined();
            }
            println!(
                "string-range benchmark operation=find indexed={indexed} bound={bound} documents=1000 payload_bytes=4096 shards=4 iterations=10 examined={documents_examined} elapsed_us={}",
                started.elapsed().as_micros()
            );
            assert_eq!(documents_examined, if indexed { count * 10 } else { 10000 });
        }
    }
    engine.shutdown().await.unwrap();
}
