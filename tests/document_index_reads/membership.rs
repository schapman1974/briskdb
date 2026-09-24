use super::*;
use briskdb::document::BsonRegex;

fn values(values: Vec<BsonValue>) -> BsonValue {
    obj([("$in", BsonValue::Array(values))])
}

fn list() -> BsonValue {
    values(vec![
        BsonValue::Int32(2),
        BsonValue::Int64(1),
        BsonValue::Double(1.0),
        BsonValue::Int32(2),
    ])
}

pub(super) fn queries() -> Vec<BsonDocument> {
    vec![
        doc([("a", list())]),
        doc([("a", list()), ("b", BsonValue::Int32(1))]),
        doc([
            ("a", list()),
            ("b", values(vec![BsonValue::Int32(1), BsonValue::Int32(2)])),
        ]),
        doc([(
            "$and",
            BsonValue::Array(vec![
                obj([("a", list())]),
                obj([("enabled", BsonValue::Boolean(true))]),
            ]),
        )]),
        doc([("a", values(vec![BsonValue::Null, BsonValue::Int32(1)]))]),
        doc([
            ("a", values(vec![BsonValue::Null, BsonValue::Int32(1)])),
            ("b", values(vec![BsonValue::Int32(1), BsonValue::Int32(2)])),
        ]),
        doc([(
            "a",
            values(vec![BsonValue::Int32(999), BsonValue::Int32(998)]),
        )]),
        doc([(
            "a",
            values(vec![BsonValue::Int32(1), BsonValue::Array(vec![])]),
        )]),
        doc([(
            "a",
            values(vec![
                BsonValue::Int32(1),
                BsonValue::RegularExpression(BsonRegex::new("^1", "").unwrap()),
            ]),
        )]),
        doc([(
            "nested.x",
            values(vec![BsonValue::Int32(1), BsonValue::Int32(2)]),
        )]),
    ]
}

fn mutation(namespace: DocumentNamespace, mode: u8) -> DocumentCommand {
    let upsert = mode >= 8;
    let filter = DocumentFilter::new(if upsert {
        doc([
            (
                "a",
                values(vec![BsonValue::Int32(77), BsonValue::Int32(78)]),
            ),
            ("_id", BsonValue::Int32(1234)),
        ])
    } else {
        doc([("a", list())])
    })
    .unwrap();
    mutation_with_filter(namespace, mode, filter)
}

pub(super) fn mutation_with_filter(
    namespace: DocumentNamespace,
    mode: u8,
    filter: DocumentFilter,
) -> DocumentCommand {
    let upsert = mode >= 8;
    let mode = if upsert { mode - 8 } else { mode };
    let write = DocumentWriteOptions::new().with_upsert(upsert);
    let read = DocumentReadOptions::new()
        .with_sort(DocumentSort::new(doc([("rank", BsonValue::Int32(-1))])).unwrap())
        .with_projection(
            DocumentProjection::new(doc([
                ("a", BsonValue::Int32(1)),
                ("rank", BsonValue::Int32(1)),
            ]))
            .unwrap(),
        );
    let update = || {
        DocumentUpdateRequest::new(
            namespace.clone(),
            filter.clone(),
            DocumentUpdate::new(doc([
                ("$inc", obj([("rank", BsonValue::Int32(1))])),
                ("$set", obj([("touched", BsonValue::Boolean(true))])),
            ]))
            .unwrap(),
            if mode == 1 {
                DocumentMutationScope::Many
            } else {
                DocumentMutationScope::One
            },
            write,
        )
    };
    let replacement = || {
        DocumentReplaceRequest::new(
            namespace.clone(),
            filter.clone(),
            doc([("a", BsonValue::Int32(77)), ("rank", BsonValue::Int32(999))]),
            write,
        )
        .unwrap()
    };
    match mode {
        0 | 1 => DocumentCommand::Update(update()),
        2 => DocumentCommand::Replace(replacement()),
        3 => DocumentCommand::FindOneAndUpdate(
            DocumentFindOneAndUpdateRequest::new(update(), read).with_return_after(true),
        ),
        4 => DocumentCommand::FindOneAndReplace(DocumentFindOneAndReplaceRequest::new(
            replacement(),
            read,
        )),
        5 | 6 => DocumentCommand::Delete(DocumentDeleteRequest::new(
            namespace,
            filter,
            if mode == 5 {
                DocumentMutationScope::One
            } else {
                DocumentMutationScope::Many
            },
            write,
        )),
        _ => DocumentCommand::FindOneAndDelete(DocumentFindOneAndDeleteRequest::new(
            namespace, filter, read,
        )),
    }
}

#[tokio::test]
async fn membership_mutations_and_upserts_match_scans_without_double_visiting_multikey_rows() {
    for mode in 0..13 {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), 4).await.unwrap();
        let session = engine.session();
        for name in ["scan", "indexed"] {
            seed(&engine, &session, &ns(name), 35).await;
        }
        build(
            &engine,
            &session,
            &ns("indexed"),
            DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap(),
        )
        .await;
        let expected = call(&engine, &session, mutation(ns("scan"), mode))
            .await
            .into_parts()
            .2;
        let actual = call(&engine, &session, mutation(ns("indexed"), mode))
            .await
            .into_parts()
            .2;
        assert_eq!(actual, expected, "mode {mode}");
        if mode == 1 {
            let DocumentResult::Update(result) = &actual else {
                panic!("update");
            };
            assert_eq!(result.matched_count(), 15);
            assert_eq!(result.modified_count(), 15);
        }
        if mode == 6 {
            let DocumentResult::Delete(result) = &actual else {
                panic!("delete");
            };
            assert_eq!(result.deleted_count(), 15);
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
            .await,
            "stored mode {mode}"
        );
        engine.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn membership_cursor_reselects_current_index_authority_between_pages() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("membership_cursor");
    seed(&engine, &session, &namespace, 35).await;
    let query = doc([("a", list())]);
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
        assert!(pages < 30);
    }
    assert!(pages > 5);
    assert_eq!(
        documents
            .iter()
            .map(|row| encode_document(row).unwrap())
            .collect::<Vec<_>>(),
        expected
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn membership_selection_skips_unselected_bson_before_reads_and_writes() {
    assert_physical_selection(
        DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap(),
        doc([("a", list())]),
        15,
        0,
    )
    .await;
}

pub(super) async fn assert_physical_selection(
    definition: DocumentIndexRequest,
    query: BsonDocument,
    expected: usize,
    excluded_id: i32,
) {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    let namespace = ns("membership_physical");
    seed(&engine, &session, &namespace, 35).await;
    build(&engine, &session, &namespace, definition).await;
    let key = CanonicalBsonKey::encode(&BsonValue::Int32(excluded_id)).unwrap();
    let mut restore = None;
    for shard in 0..2 {
        let connection =
            rusqlite::Connection::open(root.path().join(format!("shards/{shard:04}.sqlite")))
                .unwrap();
        let checksums = connection
            .prepare("SELECT document_checksum FROM briskdb_documents_v1 WHERE id_key=?1")
            .unwrap()
            .query_map([key.as_bytes()], |row| row.get::<_, Vec<u8>>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        if let Some(checksum) = checksums.into_iter().next() {
            connection.execute("UPDATE briskdb_documents_v1 SET document_checksum=zeroblob(32) WHERE id_key=?1", [key.as_bytes()]).unwrap();
            restore = Some((connection, checksum));
        }
    }
    assert!(restore.is_some());
    let found = find(
        &engine,
        &session,
        &namespace,
        &query,
        DocumentReadOptions::new().with_batch_size(2).unwrap(),
    )
    .await;
    assert_eq!(found.len(), expected);
    let result = call(
        &engine,
        &session,
        mutation_with_filter(
            namespace.clone(),
            1,
            DocumentFilter::new(query.clone()).unwrap(),
        ),
    )
    .await;
    let DocumentResult::Update(result) = result.result() else {
        panic!("update");
    };
    assert_eq!(result.modified_count(), expected as u64);
    let (connection, checksum) = restore.unwrap();
    assert_eq!(
        connection
            .execute(
                "UPDATE briskdb_documents_v1 SET document_checksum=?1 WHERE id_key=?2",
                rusqlite::params![checksum, key.as_bytes()]
            )
            .unwrap(),
        1
    );
    drop(connection);
    engine.shutdown().await.unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    assert_eq!(
        find(
            &engine,
            &engine.session(),
            &namespace,
            &query,
            DocumentReadOptions::new()
        )
        .await
        .len(),
        expected
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn membership_selection_validates_the_chosen_multikey_entry_checksum() {
    assert_candidate_checksum(
        DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap(),
        doc([("a", list())]),
        5,
        2,
    )
    .await;
}

pub(super) async fn assert_candidate_checksum(
    definition: DocumentIndexRequest,
    query: BsonDocument,
    selected_id: i32,
    expected_entries: usize,
) {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
    let namespace = ns("membership_checksum");
    seed(&engine, &session, &namespace, 35).await;
    build(&engine, &session, &namespace, definition).await;
    let id = CanonicalBsonKey::encode(&BsonValue::Int32(selected_id)).unwrap();
    let mut restore = None;
    for shard in 0..2 {
        let connection =
            rusqlite::Connection::open(root.path().join(format!("shards/{shard:04}.sqlite")))
                .unwrap();
        let entries = connection.prepare("SELECT index_key,entry_checksum FROM briskdb_document_index_entries_v1 WHERE id_key=?1").unwrap()
            .query_map([id.as_bytes()], |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        if !entries.is_empty() {
            assert_eq!(entries.len(), expected_entries, "selected entry count");
            assert_eq!(connection.execute("UPDATE briskdb_document_index_entries_v1 SET entry_checksum=zeroblob(32) WHERE id_key=?1", [id.as_bytes()]).unwrap(), expected_entries);
            restore = Some((connection, entries));
        }
    }
    let error = engine
        .execute_document(
            &session,
            DocumentRequest::new(
                DocumentRequestId::new([1; 16]).unwrap(),
                RequestContext::new(),
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace.clone(),
                    DocumentFilter::new(query).unwrap(),
                    DocumentReadOptions::new(),
                )),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
    let (connection, entries) = restore.unwrap();
    for (key, checksum) in entries {
        assert_eq!(connection.execute("UPDATE briskdb_document_index_entries_v1 SET entry_checksum=?1 WHERE id_key=?2 AND index_key=?3", rusqlite::params![checksum, id.as_bytes(), key]).unwrap(), 1);
    }
    drop(connection);
    engine.shutdown().await.unwrap();
    drop(session);
    drop(engine);
    // Detection persists terminal degradation; manually restoring test-owned
    // bytes must not silently clear the root's corruption fence on reopen.
    let error = Engine::open(root.path(), 2)
        .await
        .expect_err("degraded root");
    assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
}

#[tokio::test]
#[ignore = "manual same-root membership candidate benchmark; timing is not a CI assertion"]
async fn membership_candidate_benchmark() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("membership_benchmark");
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
    let records = (0..1000)
        .map(|id| {
            doc([
                ("_id", BsonValue::Int32(id)),
                ("a", BsonValue::Int32(id % 100)),
                ("payload", BsonValue::from("x".repeat(4096))),
            ])
        })
        .collect::<Vec<_>>();
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
            build(
                &engine,
                &session,
                &namespace,
                DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap(),
            )
            .await;
        }
        for (members, expected) in [(vec![3, 17], 20), (vec![998, 999], 0)] {
            let query = DocumentFilter::new(doc([(
                "a",
                values(members.iter().copied().map(BsonValue::Int32).collect()),
            )]))
            .unwrap();
            for write in [false, true] {
                let command = if write {
                    DocumentCommand::Update(DocumentUpdateRequest::new(
                        namespace.clone(),
                        query.clone(),
                        DocumentUpdate::new(doc([(
                            "$inc",
                            obj([("counter", BsonValue::Int32(1))]),
                        )]))
                        .unwrap(),
                        DocumentMutationScope::Many,
                        DocumentWriteOptions::new(),
                    ))
                } else {
                    DocumentCommand::Count(DocumentCountRequest::new(
                        namespace.clone(),
                        query.clone(),
                        DocumentReadOptions::new(),
                    ))
                };
                call(&engine, &session, command.clone()).await;
                let started = Instant::now();
                for _ in 0..10 {
                    let result = call(&engine, &session, command.clone()).await;
                    match result.result() {
                        DocumentResult::Count(count) => assert_eq!(*count, expected),
                        DocumentResult::Update(result) => assert_eq!(
                            (result.matched_count(), result.modified_count()),
                            (expected, expected)
                        ),
                        other => panic!("unexpected result {other:?}"),
                    }
                }
                println!(
                    "membership benchmark indexed={indexed} write={write} values={members:?} documents=1000 payload_bytes=4096 shards=4 iterations=10 elapsed_us={}",
                    started.elapsed().as_micros()
                );
            }
        }
    }
    engine.shutdown().await.unwrap();
}
