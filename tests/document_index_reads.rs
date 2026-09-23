#![cfg(feature = "documents")]

use std::time::Instant;

use briskdb::{
    core::{Engine, RequestContext, Session},
    document::{
        BsonDocument, BsonValue, DocumentCollectionOptions, DocumentCommand,
        DocumentContinueCursorRequest, DocumentCountRequest, DocumentCreateCollectionRequest,
        DocumentCreateIndexRequest, DocumentCursorId, DocumentDeleteRequest,
        DocumentDistinctRequest, DocumentDropIndexRequest, DocumentExecution, DocumentFilter,
        DocumentFindRequest, DocumentIndexRequest, DocumentInsertRequest, DocumentMutationScope,
        DocumentNamespace, DocumentReadOptions, DocumentRequest, DocumentRequestId, DocumentResult,
        DocumentSort, DocumentUpdate, DocumentUpdateRequest, DocumentWriteOptions, encode_document,
    },
};

fn doc(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(entries).unwrap()
}
fn obj(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonValue {
    BsonValue::Document(doc(entries))
}
fn ns(name: &str) -> DocumentNamespace {
    DocumentNamespace::new("index_reads", name).unwrap()
}
async fn call(engine: &Engine, session: &Session, command: DocumentCommand) -> DocumentExecution {
    engine
        .execute_document(
            session,
            DocumentRequest::new(
                DocumentRequestId::new([1; 16]).unwrap(),
                RequestContext::new(),
                command,
            ),
        )
        .await
        .unwrap()
}
async fn seed(engine: &Engine, session: &Session, namespace: &DocumentNamespace, count: i32) {
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
    let documents: Vec<_> = (0..count)
        .map(|i| {
            let mut entries = vec![
                ("_id", BsonValue::Int32(i)),
                ("b", BsonValue::Int32(i % 3)),
                ("rank", BsonValue::Int32(count - i)),
                ("enabled", BsonValue::Boolean(i % 2 == 0)),
                ("nested", obj([("x", BsonValue::Int32(i % 4))])),
            ];
            let a = match i % 7 {
                0 => None,
                1 => Some(BsonValue::Null),
                2 => Some(BsonValue::Int32(1)),
                3 => Some(BsonValue::Double(1.0)),
                4 => Some(BsonValue::Boolean(true)),
                5 => Some(BsonValue::Array(vec![
                    BsonValue::Int32(1),
                    BsonValue::Int32(2),
                    BsonValue::Int64(1),
                ])),
                _ => Some(BsonValue::Array(vec![])),
            };
            if let Some(a) = a {
                entries.push(("a", a));
            }
            doc(entries)
        })
        .collect();
    call(
        engine,
        session,
        DocumentCommand::Insert(
            DocumentInsertRequest::new(namespace.clone(), documents, DocumentWriteOptions::new())
                .unwrap(),
        ),
    )
    .await;
}
async fn build(
    engine: &Engine,
    session: &Session,
    namespace: &DocumentNamespace,
    definition: DocumentIndexRequest,
) {
    call(
        engine,
        session,
        DocumentCommand::CreateBuiltIndex(DocumentCreateIndexRequest::new(
            namespace.clone(),
            definition.with_name("candidate").unwrap(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
}
async fn drop_index(engine: &Engine, session: &Session, namespace: &DocumentNamespace) {
    call(
        engine,
        session,
        DocumentCommand::DropIndex(
            DocumentDropIndexRequest::new(
                namespace.clone(),
                "candidate",
                DocumentWriteOptions::new(),
            )
            .unwrap(),
        ),
    )
    .await;
}
fn page(execution: DocumentExecution) -> (Option<DocumentCursorId>, Vec<BsonDocument>) {
    let DocumentResult::Cursor(batch) = execution.into_parts().2 else {
        panic!("cursor")
    };
    let (_, cursor, documents) = batch.into_parts();
    (cursor, documents)
}
async fn find(
    engine: &Engine,
    session: &Session,
    namespace: &DocumentNamespace,
    query: &BsonDocument,
    options: DocumentReadOptions,
) -> Vec<Vec<u8>> {
    let batch = options.batch_size();
    let (mut cursor, mut documents) = page(
        call(
            engine,
            session,
            DocumentCommand::Find(DocumentFindRequest::new(
                namespace.clone(),
                DocumentFilter::new(query.clone()).unwrap(),
                options,
            )),
        )
        .await,
    );
    while let Some(id) = cursor {
        let next = page(
            call(
                engine,
                session,
                DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
                    namespace.clone(),
                    id,
                    DocumentReadOptions::new().with_batch_size(batch).unwrap(),
                )),
            )
            .await,
        );
        cursor = next.0;
        documents.extend(next.1);
    }
    documents
        .iter()
        .map(|d| encode_document(d).unwrap())
        .collect()
}
fn queries() -> Vec<BsonDocument> {
    let mut queries = vec![doc([])];
    for value in [
        BsonValue::Null,
        BsonValue::Int64(1),
        BsonValue::Boolean(true),
        BsonValue::Array(vec![]),
    ] {
        queries.push(doc([("a", value.clone())]));
        queries.push(doc([
            ("a", obj([("$eq", value.clone())])),
            ("b", BsonValue::Int32(1)),
        ]));
        queries.push(doc([(
            "$and",
            BsonValue::Array(vec![
                obj([("a", value)]),
                obj([("enabled", BsonValue::Boolean(true))]),
            ]),
        )]));
    }
    queries.extend([
        doc([("a", obj([("$gt", BsonValue::Int32(0))]))]),
        doc([(
            "$or",
            BsonValue::Array(vec![
                obj([("a", BsonValue::Int32(1))]),
                obj([("b", BsonValue::Int32(2))]),
            ]),
        )]),
        doc([(
            "$and",
            BsonValue::Array(vec![
                obj([("a", BsonValue::Int32(1))]),
                obj([("a", BsonValue::Int32(2))]),
            ]),
        )]),
        doc([("nested.x", BsonValue::Double(2.0))]),
        doc([("a", BsonValue::Int32(999))]),
    ]);
    queries
}

#[tokio::test]
async fn indexed_reads_equal_scans_for_filters_sort_skip_pages_count_distinct_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(root.path(), 4).await.unwrap();
    let mut session = engine.session();
    let namespace = ns("items");
    seed(&engine, &session, &namespace, 70).await;
    let queries = queries();
    let options = [
        DocumentReadOptions::new().with_batch_size(3).unwrap(),
        DocumentReadOptions::new()
            .with_batch_size(2)
            .unwrap()
            .with_skip(2)
            .with_limit(7)
            .unwrap()
            .with_sort(DocumentSort::new(doc([("rank", BsonValue::Int32(-1))])).unwrap()),
    ];
    let mut baseline = Vec::new();
    for query in &queries {
        for option in &options {
            baseline.push(find(&engine, &session, &namespace, query, option.clone()).await);
        }
    }
    let mut summaries = Vec::new();
    for query in &queries {
        let filter = DocumentFilter::new(query.clone()).unwrap();
        for command in [
            DocumentCommand::Count(DocumentCountRequest::new(
                namespace.clone(),
                filter.clone(),
                DocumentReadOptions::new(),
            )),
            DocumentCommand::Distinct(
                DocumentDistinctRequest::new(
                    namespace.clone(),
                    "b",
                    filter,
                    DocumentReadOptions::new(),
                )
                .unwrap(),
            ),
        ] {
            let result = call(&engine, &session, command.clone())
                .await
                .into_parts()
                .2;
            summaries.push((command, result));
        }
    }
    let definitions = [
        DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap(),
        DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))]))
            .unwrap()
            .with_sparse(true),
        DocumentIndexRequest::new(doc([
            ("a", BsonValue::Int32(1)),
            ("b", BsonValue::Int32(-1)),
        ]))
        .unwrap()
        .with_sparse(true),
        DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))]))
            .unwrap()
            .with_partial_filter(
                DocumentFilter::new(doc([("enabled", BsonValue::Boolean(true))])).unwrap(),
            ),
        DocumentIndexRequest::new(doc([("nested.x", BsonValue::Int32(1))])).unwrap(),
    ];
    for definition in definitions {
        build(&engine, &session, &namespace, definition).await;
        engine.shutdown().await.unwrap();
        engine = Engine::open(root.path(), 4).await.unwrap();
        session = engine.session();
        let mut n = 0;
        for query in &queries {
            for option in &options {
                assert_eq!(
                    find(&engine, &session, &namespace, query, option.clone()).await,
                    baseline[n],
                    "query {query:?}, options {option:?}"
                );
                n += 1;
            }
        }
        for (command, expected) in &summaries {
            assert_eq!(
                &call(&engine, &session, command.clone())
                    .await
                    .into_parts()
                    .2,
                expected
            );
        }
        drop_index(&engine, &session, &namespace).await;
    }
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn live_cursor_reselects_authority_after_index_drop_and_recreation() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("cursor");
    seed(&engine, &session, &namespace, 70).await;
    let query = doc([("a", BsonValue::Int32(1))]);
    let expected = find(
        &engine,
        &session,
        &namespace,
        &query,
        DocumentReadOptions::new(),
    )
    .await;
    let definition = || DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap();
    build(&engine, &session, &namespace, definition()).await;
    let (mut cursor, mut documents) = page(
        call(
            &engine,
            &session,
            DocumentCommand::Find(DocumentFindRequest::new(
                namespace.clone(),
                DocumentFilter::new(query).unwrap(),
                DocumentReadOptions::new().with_batch_size(1).unwrap(),
            )),
        )
        .await,
    );
    let mut pages = 0;
    while let Some(id) = cursor {
        if pages % 2 == 0 {
            drop_index(&engine, &session, &namespace).await;
        } else {
            build(&engine, &session, &namespace, definition()).await;
        }
        let next = page(
            call(
                &engine,
                &session,
                DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
                    namespace.clone(),
                    id,
                    DocumentReadOptions::new().with_batch_size(2).unwrap(),
                )),
            )
            .await,
        );
        cursor = next.0;
        documents.extend(next.1);
        pages += 1;
    }
    assert!(pages > 5);
    assert_eq!(
        documents
            .iter()
            .map(|d| encode_document(d).unwrap())
            .collect::<Vec<_>>(),
        expected
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn maintained_index_reads_equal_scans_after_single_and_many_mutations() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    for namespace in [ns("scan"), ns("indexed")] {
        seed(&engine, &session, &namespace, 35).await;
    }
    build(
        &engine,
        &session,
        &ns("indexed"),
        DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap(),
    )
    .await;
    for phase in 0..4 {
        let mut results = Vec::new();
        for namespace in [ns("scan"), ns("indexed")] {
            let filter = DocumentFilter::new(doc([(
                "a",
                BsonValue::Int32(if phase < 2 { 1 } else { 8 }),
            )]))
            .unwrap();
            let command = if phase < 2 {
                DocumentCommand::Update(DocumentUpdateRequest::new(
                    namespace,
                    filter,
                    DocumentUpdate::new(doc([("$set", obj([("a", BsonValue::Int32(8))]))]))
                        .unwrap(),
                    if phase == 0 {
                        DocumentMutationScope::One
                    } else {
                        DocumentMutationScope::Many
                    },
                    DocumentWriteOptions::new(),
                ))
            } else {
                DocumentCommand::Delete(DocumentDeleteRequest::new(
                    namespace,
                    filter,
                    if phase == 2 {
                        DocumentMutationScope::One
                    } else {
                        DocumentMutationScope::Many
                    },
                    DocumentWriteOptions::new(),
                ))
            };
            results.push(call(&engine, &session, command).await.into_parts().2);
        }
        assert_eq!(results[0], results[1]);
        for query in queries()
            .into_iter()
            .chain([doc([("a", BsonValue::Int32(8))])])
        {
            assert_eq!(
                find(
                    &engine,
                    &session,
                    &ns("scan"),
                    &query,
                    DocumentReadOptions::new()
                )
                .await,
                find(
                    &engine,
                    &session,
                    &ns("indexed"),
                    &query,
                    DocumentReadOptions::new()
                )
                .await
            );
        }
    }
    engine.shutdown().await.unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    assert_eq!(
        find(
            &engine,
            &session,
            &ns("scan"),
            &doc([]),
            DocumentReadOptions::new()
        )
        .await,
        find(
            &engine,
            &session,
            &ns("indexed"),
            &doc([]),
            DocumentReadOptions::new()
        )
        .await
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "manual same-root equality candidate benchmark; timing is not a CI assertion"]
async fn equality_candidate_benchmark() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("benchmark");
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
    let documents: Vec<_> = (0..1_000)
        .map(|i| {
            doc([
                ("_id", BsonValue::Int32(i)),
                ("a", BsonValue::Int32(i % 100)),
                ("payload", BsonValue::String("x".repeat(4096))),
            ])
        })
        .collect();
    call(
        &engine,
        &session,
        DocumentCommand::Insert(
            DocumentInsertRequest::new(namespace.clone(), documents, DocumentWriteOptions::new())
                .unwrap(),
        ),
    )
    .await;
    for indexed in [false, true] {
        if indexed {
            build(
                &engine,
                &session,
                &namespace,
                DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap(),
            )
            .await;
        }
        for (value, expected) in [(3, 10), (999, 0)] {
            let command = DocumentCommand::Count(DocumentCountRequest::new(
                namespace.clone(),
                DocumentFilter::new(doc([("a", BsonValue::Int32(value))])).unwrap(),
                DocumentReadOptions::new(),
            ));
            call(&engine, &session, command.clone()).await;
            let started = Instant::now();
            for _ in 0..10 {
                assert_eq!(
                    call(&engine, &session, command.clone()).await.result(),
                    &DocumentResult::Count(expected)
                );
            }
            println!(
                "equality benchmark indexed={indexed} value={value} documents=1000 payload_bytes=4096 shards=4 iterations=10 elapsed_us={}",
                started.elapsed().as_micros()
            );
        }
    }
    engine.shutdown().await.unwrap();
}
