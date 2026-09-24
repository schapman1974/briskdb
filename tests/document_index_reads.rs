#![cfg(feature = "documents")]

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use briskdb::{
    core::{CancellationToken, Engine, EngineErrorKind, RequestContext, Session},
    document::{
        BsonDocument, BsonValue, CanonicalBsonKey, DocumentCollectionOptions, DocumentCommand,
        DocumentContinueCursorRequest, DocumentCountRequest, DocumentCreateCollectionRequest,
        DocumentCreateIndexRequest, DocumentCursorId, DocumentDeleteRequest,
        DocumentDistinctRequest, DocumentDropIndexRequest, DocumentExecution, DocumentFilter,
        DocumentFindOneAndDeleteRequest, DocumentFindOneAndReplaceRequest,
        DocumentFindOneAndUpdateRequest, DocumentFindRequest, DocumentIndexRequest,
        DocumentInsertRequest, DocumentMutationScope, DocumentNamespace, DocumentProjection,
        DocumentReadOptions, DocumentReplaceRequest, DocumentRequest, DocumentRequestId,
        DocumentResult, DocumentSort, DocumentUpdate, DocumentUpdateRequest, DocumentWriteOptions,
        decode_document, encode_document,
    },
};

#[path = "document_index_reads/absence.rs"]
mod absence;
#[path = "document_index_reads/membership.rs"]
mod membership;
#[path = "document_index_reads/presence.rs"]
mod presence;

fn doc(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(entries).unwrap()
}
fn obj(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonValue {
    BsonValue::Document(doc(entries))
}

#[tokio::test]
async fn nonunique_fallback_candidates_preserve_nested_bson_matches_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let records = vec![
        doc([
            ("_id", BsonValue::Int32(1)),
            ("v", obj([("score", BsonValue::Int32(2))])),
        ]),
        doc([
            ("_id", BsonValue::Int32(2)),
            ("v", obj([("score", BsonValue::Int32(0))])),
        ]),
        doc([
            ("_id", BsonValue::Int32(3)),
            (
                "v",
                BsonValue::Array(vec![obj([("score", BsonValue::Int32(3))])]),
            ),
        ]),
        doc([("_id", BsonValue::Int32(4)), ("v", BsonValue::Int32(1))]),
        doc([("_id", BsonValue::Int32(5)), ("v", BsonValue::Null)]),
        doc([
            ("_id", BsonValue::Int32(6)),
            (
                "v",
                BsonValue::Array(vec![BsonValue::Array(vec![
                    BsonValue::Int32(1),
                    BsonValue::Int32(2),
                ])]),
            ),
        ]),
        doc([
            ("_id", BsonValue::Int32(7)),
            (
                "v",
                BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(2)]),
            ),
        ]),
        doc([("_id", BsonValue::Int32(8)), ("v", BsonValue::Double(1.0))]),
        doc([
            ("_id", BsonValue::Int32(9)),
            ("v", BsonValue::Boolean(true)),
        ]),
        doc([
            ("_id", BsonValue::Int32(10)),
            ("a", BsonValue::Array(vec![BsonValue::Int32(1)])),
            ("b", BsonValue::Array(vec![BsonValue::Int32(2)])),
        ]),
        doc([("_id", BsonValue::Int32(11))]),
    ];
    let mut queries = vec![
        doc([]),
        doc([("v", BsonValue::Int32(1))]),
        doc([("v", BsonValue::Boolean(true))]),
        doc([("v", BsonValue::Null)]),
        doc([("v", obj([("$gt", obj([("score", BsonValue::Int32(1))]))]))]),
        doc([("v", obj([("$eq", obj([("score", BsonValue::Int32(2))]))]))]),
        doc([(
            "v",
            BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(2)]),
        )]),
        doc([("v.score", BsonValue::Int32(2))]),
        doc([("v.score", obj([("$exists", BsonValue::Boolean(false))]))]),
        doc([("a", BsonValue::Int32(1)), ("b", BsonValue::Int32(2))]),
    ];
    for field in ["v", "v.score"] {
        queries.push(doc([(
            field,
            obj([(
                "$in",
                BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(2)]),
            )]),
        )]));
    }
    queries.extend(membership::queries());
    queries.extend(presence::queries());
    queries.extend(absence::queries());
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let mut expected = Vec::new();
    for (name, keys, sparse) in [
        ("fallback_value", doc([("v", BsonValue::Int32(1))]), false),
        (
            "fallback_path",
            doc([("v.score", BsonValue::Int32(1))]),
            false,
        ),
        (
            "fallback_compound",
            doc([("a", BsonValue::Int32(1)), ("b", BsonValue::Int32(1))]),
            false,
        ),
        (
            "fallback_sparse_value",
            doc([("v", BsonValue::Int32(1))]),
            true,
        ),
        (
            "fallback_sparse_path",
            doc([("v.score", BsonValue::Int32(1))]),
            true,
        ),
        (
            "fallback_sparse_compound",
            doc([("a", BsonValue::Int32(1)), ("b", BsonValue::Int32(1))]),
            true,
        ),
    ] {
        let namespace = ns(name);
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
        call(
            &engine,
            &session,
            DocumentCommand::Insert(
                DocumentInsertRequest::new(
                    namespace.clone(),
                    records.clone(),
                    DocumentWriteOptions::new(),
                )
                .unwrap(),
            ),
        )
        .await;
        let mut scans = Vec::new();
        for query in &queries {
            scans.push(
                find(
                    &engine,
                    &session,
                    &namespace,
                    query,
                    DocumentReadOptions::new().with_batch_size(1).unwrap(),
                )
                .await,
            );
        }
        build(
            &engine,
            &session,
            &namespace,
            DocumentIndexRequest::new(keys).unwrap().with_sparse(sparse),
        )
        .await;
        for (query, scan) in queries.iter().zip(&scans) {
            assert_eq!(
                &find(
                    &engine,
                    &session,
                    &namespace,
                    query,
                    DocumentReadOptions::new().with_batch_size(1).unwrap()
                )
                .await,
                scan,
                "query changed after non-unique build: {query:?}"
            );
        }
        expected.push((namespace, scans));
    }
    drop(session);
    engine.shutdown().await.unwrap();
    let reopened = Engine::open(root.path(), 4).await.unwrap();
    let session = reopened.session();
    for (namespace, scans) in expected {
        for (query, scan) in queries.iter().zip(scans) {
            assert_eq!(
                find(
                    &reopened,
                    &session,
                    &namespace,
                    query,
                    DocumentReadOptions::new().with_batch_size(1).unwrap()
                )
                .await,
                scan,
                "query changed after reopen: {query:?}"
            );
        }
    }
    reopened.shutdown().await.unwrap();
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
    queries.extend(membership::queries());
    queries.extend(presence::queries());
    queries.extend(absence::queries());
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

fn indexed_mutation(namespace: DocumentNamespace, phase: u8) -> DocumentCommand {
    let filter = DocumentFilter::new(if phase < 5 {
        doc([("a", BsonValue::Int32(1))])
    } else {
        doc([("a", BsonValue::Int32(77)), ("_id", BsonValue::Int32(1234))])
    })
    .unwrap();
    let options = DocumentReadOptions::new()
        .with_sort(DocumentSort::new(doc([("rank", BsonValue::Int32(-1))])).unwrap())
        .with_projection(
            DocumentProjection::new(doc([
                ("_id", BsonValue::Int32(1)),
                ("a", BsonValue::Int32(1)),
                ("rank", BsonValue::Int32(1)),
            ]))
            .unwrap(),
        );
    let update = DocumentUpdateRequest::new(
        namespace.clone(),
        filter.clone(),
        DocumentUpdate::new(doc([("$inc", obj([("rank", BsonValue::Int32(1))]))])).unwrap(),
        if phase == 0 || phase == 6 {
            DocumentMutationScope::Many
        } else {
            DocumentMutationScope::One
        },
        DocumentWriteOptions::new().with_upsert(phase >= 5),
    );
    match phase {
        0 | 5 | 6 => DocumentCommand::Update(update),
        1 | 2 => DocumentCommand::FindOneAndUpdate(
            DocumentFindOneAndUpdateRequest::new(update, options).with_return_after(phase == 2),
        ),
        3 => DocumentCommand::FindOneAndReplace(DocumentFindOneAndReplaceRequest::new(
            DocumentReplaceRequest::new(
                namespace,
                filter,
                doc([("a", BsonValue::Int32(8)), ("rank", BsonValue::Int32(-5))]),
                DocumentWriteOptions::new(),
            )
            .unwrap(),
            options,
        )),
        4 => DocumentCommand::FindOneAndDelete(DocumentFindOneAndDeleteRequest::new(
            namespace, filter, options,
        )),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn indexed_find_modify_and_upserts_equal_scan_images_counts_and_postimages() {
    for sparse in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), 4).await.unwrap();
        let session = engine.session();
        for namespace in [ns("scan"), ns("indexed")] {
            seed(&engine, &session, &namespace, 35).await;
        }
        build(
            &engine,
            &session,
            &ns("indexed"),
            DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))]))
                .unwrap()
                .with_sparse(sparse),
        )
        .await;
        for phase in 0..7 {
            let expected = call(&engine, &session, indexed_mutation(ns("scan"), phase))
                .await
                .into_parts()
                .2;
            let actual = call(&engine, &session, indexed_mutation(ns("indexed"), phase))
                .await
                .into_parts()
                .2;
            assert_eq!(actual, expected, "phase {phase}, sparse {sparse}");
            assert_eq!(
                find(
                    &engine,
                    &session,
                    &ns("indexed"),
                    &doc([]),
                    DocumentReadOptions::new()
                )
                .await,
                find(
                    &engine,
                    &session,
                    &ns("scan"),
                    &doc([]),
                    DocumentReadOptions::new()
                )
                .await,
                "phase {phase}, sparse {sparse}"
            );
        }
        engine.shutdown().await.unwrap();
        let engine = Engine::open(root.path(), 4).await.unwrap();
        let session = engine.session();
        assert_eq!(
            find(
                &engine,
                &session,
                &ns("indexed"),
                &doc([]),
                DocumentReadOptions::new()
            )
            .await,
            find(
                &engine,
                &session,
                &ns("scan"),
                &doc([]),
                DocumentReadOptions::new()
            )
            .await
        );
        engine.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn indexed_mutation_selection_does_not_decode_non_candidates() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    let namespace = ns("physical");
    seed(&engine, &session, &namespace, 35).await;
    build(
        &engine,
        &session,
        &namespace,
        DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap(),
    )
    .await;
    let key = CanonicalBsonKey::encode(&BsonValue::Int32(0)).unwrap();
    let mut restore = None;
    for shard in 0..2 {
        let connection =
            rusqlite::Connection::open(root.path().join(format!("shards/{shard:04}.sqlite")))
                .unwrap();
        let rows: Vec<Vec<u8>> = connection
            .prepare("SELECT document_checksum FROM briskdb_documents_v1 WHERE id_key = ?1")
            .unwrap()
            .query_map([key.as_bytes()], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        if let Some(checksum) = rows.into_iter().next() {
            // Test-owned damage outside a == 1: a scan would fail before a
            // write; indexed selection must not decode this unrelated record.
            connection.execute("UPDATE briskdb_documents_v1 SET document_checksum = zeroblob(32) WHERE id_key = ?1", [key.as_bytes()]).unwrap();
            restore = Some((connection, checksum));
        }
    }
    assert!(restore.is_some());
    for phase in 0..5 {
        call(
            &engine,
            &session,
            indexed_mutation(namespace.clone(), phase),
        )
        .await;
    }
    let filter = DocumentFilter::new(doc([("a", BsonValue::Int32(1))])).unwrap();
    call(
        &engine,
        &session,
        DocumentCommand::Replace(
            DocumentReplaceRequest::new(
                namespace.clone(),
                filter.clone(),
                doc([("a", BsonValue::Int32(1)), ("rank", BsonValue::Int32(99))]),
                DocumentWriteOptions::new(),
            )
            .unwrap(),
        ),
    )
    .await;
    for scope in [DocumentMutationScope::One, DocumentMutationScope::Many] {
        call(
            &engine,
            &session,
            DocumentCommand::Delete(DocumentDeleteRequest::new(
                namespace.clone(),
                filter.clone(),
                scope,
                DocumentWriteOptions::new(),
            )),
        )
        .await;
    }
    let (connection, checksum) = restore.unwrap();
    assert_eq!(
        connection
            .execute(
                "UPDATE briskdb_documents_v1 SET document_checksum = ?1 WHERE id_key = ?2",
                rusqlite::params![checksum, key.as_bytes()]
            )
            .unwrap(),
        1
    );
    drop(connection);
    engine.shutdown().await.unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    assert!(
        find(
            &engine,
            &session,
            &namespace,
            filter.document(),
            DocumentReadOptions::new()
        )
        .await
        .is_empty()
    );
    assert_eq!(
        find(
            &engine,
            &session,
            &namespace,
            &doc([("_id", BsonValue::Int32(0))]),
            DocumentReadOptions::new()
        )
        .await
        .len(),
        1
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn indexed_multi_update_rolls_back_the_failing_shard_and_preserves_prior_commits() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    let namespace = ns("rollback");
    seed(&engine, &session, &namespace, 35).await;
    build(
        &engine,
        &session,
        &namespace,
        DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap(),
    )
    .await;
    let second = rusqlite::Connection::open(root.path().join("shards/0001.sqlite")).unwrap();
    let mut stored: Vec<BsonDocument> = second
        .prepare("SELECT document_bson FROM briskdb_documents_v1 ORDER BY natural_order")
        .unwrap()
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .unwrap()
        .map(|row| decode_document(&row.unwrap()).unwrap())
        .collect();
    let matcher =
        briskdb::document::DocumentMatcher::compile(&doc([("a", BsonValue::Int32(1))])).unwrap();
    stored.retain(|row| matcher.matches(row).unwrap());
    assert!(stored.len() >= 2);
    let last_id = stored.last().unwrap().get_first("_id").unwrap().clone();
    call(
        &engine,
        &session,
        DocumentCommand::Update(DocumentUpdateRequest::new(
            namespace.clone(),
            DocumentFilter::new(doc([("_id", last_id)])).unwrap(),
            DocumentUpdate::new(doc([(
                "$set",
                obj([("rank", BsonValue::from("not a number"))]),
            )]))
            .unwrap(),
            DocumentMutationScope::One,
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    let snapshot = |connection: &rusqlite::Connection| -> Vec<Vec<rusqlite::types::Value>> {
        ["SELECT * FROM briskdb_documents_v1 ORDER BY collection_id, id_key",
         "SELECT * FROM briskdb_document_index_entries_v1 ORDER BY collection_id, index_id, index_key, id_key"]
            .into_iter().flat_map(|sql| {
                let mut statement = connection.prepare(sql).unwrap();
                let columns = statement.column_count();
                statement.query_map([], |row| (0..columns).map(|i| row.get(i)).collect()).unwrap()
                    .collect::<Result<Vec<_>, _>>().unwrap()
            }).collect()
    };
    let before = snapshot(&second);
    let first = rusqlite::Connection::open(root.path().join("shards/0000.sqlite")).unwrap();
    let first_before = snapshot(&first);
    let error = engine
        .execute_document(
            &session,
            DocumentRequest::new(
                DocumentRequestId::new([2; 16]).unwrap(),
                RequestContext::new(),
                indexed_mutation(namespace.clone(), 0),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
    assert_eq!(
        snapshot(&second),
        before,
        "records and index entries on failing shard must roll back together"
    );
    assert_ne!(
        snapshot(&first),
        first_before,
        "earlier shard commits remain visible"
    );
    drop(first);
    drop(second);
    engine.shutdown().await.unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn unique_multi_update_rolls_back_records_and_entries_then_accepts_safe_changes() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    let namespace = ns("unique_rollback");
    seed(&engine, &session, &namespace, 35).await;
    build(
        &engine,
        &session,
        &namespace,
        DocumentIndexRequest::new(doc([("rank", BsonValue::Int32(1))]))
            .unwrap()
            .with_unique(true),
    )
    .await;
    let snapshot = || -> Vec<Vec<rusqlite::types::Value>> {
        (0..2).flat_map(|shard| {
            let connection = rusqlite::Connection::open(root.path().join(format!("shards/{shard:04}.sqlite"))).unwrap();
            ["SELECT * FROM briskdb_documents_v1 ORDER BY collection_id, id_key",
             "SELECT * FROM briskdb_document_index_entries_v1 ORDER BY collection_id, index_id, index_key, id_key"]
                .into_iter().flat_map(|sql| {
                    let mut statement = connection.prepare(sql).unwrap();
                    let columns = statement.column_count();
                    statement.query_map([], |row| (0..columns).map(|i| row.get(i)).collect()).unwrap()
                        .collect::<Result<Vec<_>, _>>().unwrap()
                }).collect::<Vec<_>>()
        }).collect()
    };
    let first = rusqlite::Connection::open(root.path().join("shards/0000.sqlite")).unwrap();
    assert!(
        first
            .query_row("SELECT count(*) FROM briskdb_documents_v1", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap()
            >= 2
    );
    drop(first);
    let before = snapshot();
    let update = |operator, value| {
        DocumentCommand::Update(DocumentUpdateRequest::new(
            namespace.clone(),
            DocumentFilter::new(doc([])).unwrap(),
            DocumentUpdate::new(doc([(operator, obj([("rank", BsonValue::Int32(value))]))]))
                .unwrap(),
            DocumentMutationScope::Many,
            DocumentWriteOptions::new(),
        ))
    };
    let error = engine
        .execute_document(
            &session,
            DocumentRequest::new(
                DocumentRequestId::new([2; 16]).unwrap(),
                RequestContext::new(),
                update("$set", 9999),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::UniqueViolation);
    assert_eq!(
        snapshot(),
        before,
        "failed shard must restore exact BSON and entry bytes"
    );
    call(&engine, &session, update("$inc", 100)).await;
    assert_ne!(snapshot(), before);
    let after = find(
        &engine,
        &session,
        &namespace,
        &doc([]),
        DocumentReadOptions::new(),
    )
    .await;
    engine.shutdown().await.unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    assert_eq!(
        find(
            &engine,
            &session,
            &namespace,
            &doc([]),
            DocumentReadOptions::new()
        )
        .await,
        after
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_multi_update_cancellation_preserves_prior_commits_and_releases_admission() {
    for abort in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), 2).await.unwrap();
        let session = Arc::new(engine.session());
        let namespace = ns("cancel");
        seed(&engine, &session, &namespace, 35).await;
        build(
            &engine,
            &session,
            &namespace,
            DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap(),
        )
        .await;
        let snapshot = |connection: &rusqlite::Connection| -> Vec<Vec<u8>> {
            connection
                .prepare("SELECT document_bson FROM briskdb_documents_v1 ORDER BY natural_order")
                .unwrap()
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
        };
        let first = rusqlite::Connection::open(root.path().join("shards/0000.sqlite")).unwrap();
        let mut second =
            rusqlite::Connection::open(root.path().join("shards/0001.sqlite")).unwrap();
        let blocker = second
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        let before = snapshot(&blocker);
        let first_before = snapshot(&first);
        let cancellation = CancellationToken::new();
        let context = RequestContext::new().with_cancellation_token(cancellation.clone());
        let task_engine = engine.clone();
        let task_session = Arc::clone(&session);
        let command = indexed_mutation(namespace.clone(), 0);
        let task = tokio::spawn(async move {
            task_engine
                .execute_document(
                    &task_session,
                    DocumentRequest::new(
                        DocumentRequestId::new([3; 16]).unwrap(),
                        context,
                        command,
                    ),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while snapshot(&first) == first_before {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        let committed = snapshot(&first);
        if abort {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            cancellation.cancel();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), task)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::Cancelled
            );
        }
        assert_eq!(snapshot(&blocker), before);
        blocker.rollback().unwrap();
        // A fresh request waits for any detached worker cleanup and proves
        // session/schema admission and the index remain usable.
        find(
            &engine,
            &session,
            &namespace,
            &doc([("a", BsonValue::Int32(1))]),
            DocumentReadOptions::new(),
        )
        .await;
        assert_eq!(snapshot(&first), committed);
        assert_eq!(snapshot(&second), before);
        drop(first);
        drop(second);
        engine.shutdown().await.unwrap();
        let engine = Engine::open(root.path(), 2).await.unwrap();
        engine.shutdown().await.unwrap();
    }
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
            let command = DocumentCommand::Update(DocumentUpdateRequest::new(
                namespace.clone(),
                DocumentFilter::new(doc([("a", BsonValue::Int32(value))])).unwrap(),
                DocumentUpdate::new(doc([("$inc", obj([("counter", BsonValue::Int32(1))]))]))
                    .unwrap(),
                DocumentMutationScope::Many,
                DocumentWriteOptions::new(),
            ));
            call(&engine, &session, command.clone()).await;
            let started = Instant::now();
            for _ in 0..10 {
                let result = call(&engine, &session, command.clone())
                    .await
                    .into_parts()
                    .2;
                let DocumentResult::Update(result) = result else {
                    panic!("update")
                };
                assert_eq!(
                    (result.matched_count(), result.modified_count()),
                    (expected, expected)
                );
            }
            println!(
                "mutation equality benchmark indexed={indexed} value={value} documents=1000 payload_bytes=4096 shards=4 iterations=10 elapsed_us={}",
                started.elapsed().as_micros()
            );
        }
    }
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_concurrent_find_modify_claims_each_matching_record_once() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("claims");
    seed(&engine, &session, &namespace, 35).await;
    build(
        &engine,
        &session,
        &namespace,
        DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap(),
    )
    .await;
    let query = doc([("a", BsonValue::Int32(1))]);
    let expected = find(
        &engine,
        &session,
        &namespace,
        &query,
        DocumentReadOptions::new(),
    )
    .await
    .len();
    let mut tasks = Vec::new();
    for _ in 0..expected + 3 {
        let engine = engine.clone();
        let namespace = namespace.clone();
        let query = query.clone();
        tasks.push(tokio::spawn(async move {
            call(
                &engine,
                &engine.session(),
                DocumentCommand::FindOneAndUpdate(DocumentFindOneAndUpdateRequest::new(
                    DocumentUpdateRequest::new(
                        namespace,
                        DocumentFilter::new(query).unwrap(),
                        DocumentUpdate::new(doc([("$set", obj([("a", BsonValue::Int32(8))]))]))
                            .unwrap(),
                        DocumentMutationScope::One,
                        DocumentWriteOptions::new(),
                    ),
                    DocumentReadOptions::new().with_sort(
                        DocumentSort::new(doc([("rank", BsonValue::Int32(1))])).unwrap(),
                    ),
                )),
            )
            .await
            .into_parts()
            .2
        }));
    }
    let mut ids = std::collections::HashSet::new();
    for task in tasks {
        match task.await.unwrap() {
            DocumentResult::Document(Some(image)) => {
                let Some(BsonValue::Int32(id)) = image.get_first("_id") else {
                    panic!("id")
                };
                assert!(
                    ids.insert(*id),
                    "a claimed record must not be selected twice"
                );
            }
            DocumentResult::Document(None) => (),
            _ => panic!("before image"),
        }
    }
    assert_eq!(ids.len(), expected);
    assert!(
        find(
            &engine,
            &session,
            &namespace,
            &query,
            DocumentReadOptions::new()
        )
        .await
        .is_empty()
    );
    assert_eq!(
        find(
            &engine,
            &session,
            &namespace,
            &doc([("a", BsonValue::Int32(8))]),
            DocumentReadOptions::new()
        )
        .await
        .len(),
        expected
    );
    engine.shutdown().await.unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    engine.shutdown().await.unwrap();
}
