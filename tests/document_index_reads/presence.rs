use super::*;

fn exists(path: &'static str, present: bool) -> BsonDocument {
    doc([(path, obj([("$exists", BsonValue::Boolean(present))]))])
}

fn index() -> DocumentIndexRequest {
    DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))]))
        .unwrap()
        .with_sparse(true)
}

pub(super) fn queries() -> Vec<BsonDocument> {
    let mut queries = vec![
        exists("a", true),
        exists("a", false),
        exists("b", true),
        exists("nested.x", true),
        exists("v", true),
        exists("v.score", true),
    ];
    queries.push(doc([(
        "$and",
        BsonValue::Array(vec![
            BsonValue::Document(exists("a", true)),
            obj([("enabled", BsonValue::Boolean(true))]),
        ]),
    )]));
    queries.push(doc([(
        "$or",
        BsonValue::Array(vec![
            BsonValue::Document(exists("a", true)),
            obj([("b", BsonValue::Int32(1))]),
        ]),
    )]));
    queries
}

#[tokio::test]
async fn sparse_presence_mutations_and_upserts_match_scans_and_survive_reopen() {
    for mode in 0..13 {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), 4).await.unwrap();
        let session = engine.session();
        for name in ["scan", "indexed"] {
            seed(&engine, &session, &ns(name), 35).await;
        }
        build(&engine, &session, &ns("indexed"), index()).await;
        let mut query = exists("a", true);
        if mode >= 8 {
            query.push("_id", BsonValue::Int32(1234)).unwrap();
        }
        let filter = DocumentFilter::new(query).unwrap();
        let expected = call(
            &engine,
            &session,
            membership::mutation_with_filter(ns("scan"), mode, filter.clone()),
        )
        .await
        .into_parts()
        .2;
        let actual = call(
            &engine,
            &session,
            membership::mutation_with_filter(ns("indexed"), mode, filter),
        )
        .await
        .into_parts()
        .2;
        assert_eq!(actual, expected, "mode={mode}");
        if mode == 1 {
            let DocumentResult::Update(result) = &actual else {
                panic!("update");
            };
            assert_eq!((result.matched_count(), result.modified_count()), (30, 30));
        }
        if mode == 6 {
            let DocumentResult::Delete(result) = &actual else {
                panic!("delete");
            };
            assert_eq!(result.deleted_count(), 30);
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
            "stored mode={mode}",
        );
        engine.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn sparse_presence_reselects_indexes_between_pages_and_updates_out_of_the_index() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("presence_churn");
    seed(&engine, &session, &namespace, 35).await;
    let query = exists("a", true);
    let expected = find(
        &engine,
        &session,
        &namespace,
        &query,
        DocumentReadOptions::new(),
    )
    .await;
    assert_eq!(expected.len(), 30);
    build(&engine, &session, &namespace, index()).await;
    let (mut cursor, mut documents) = page(
        call(
            &engine,
            &session,
            DocumentCommand::Find(DocumentFindRequest::new(
                namespace.clone(),
                DocumentFilter::new(query.clone()).unwrap(),
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
            build(&engine, &session, &namespace, index()).await;
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
        assert!(pages < 40);
    }
    assert_eq!(
        documents
            .iter()
            .map(|row| encode_document(row).unwrap())
            .collect::<Vec<_>>(),
        expected
    );
    if pages % 2 == 1 {
        build(&engine, &session, &namespace, index()).await;
    }
    let result = call(
        &engine,
        &session,
        DocumentCommand::Update(DocumentUpdateRequest::new(
            namespace.clone(),
            DocumentFilter::new(query.clone()).unwrap(),
            DocumentUpdate::new(doc([("$unset", obj([("a", BsonValue::Int32(1))]))])).unwrap(),
            DocumentMutationScope::Many,
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    let DocumentResult::Update(result) = result.result() else {
        panic!("update");
    };
    assert_eq!((result.matched_count(), result.modified_count()), (30, 30));
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
    engine.shutdown().await.unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    assert!(
        find(
            &engine,
            &engine.session(),
            &namespace,
            &query,
            DocumentReadOptions::new()
        )
        .await
        .is_empty()
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn sparse_presence_skips_absent_bson_before_reads_and_writes() {
    membership::assert_physical_selection(index(), exists("a", true), 30).await;
}

#[tokio::test]
async fn sparse_presence_validates_selected_entry_checksums() {
    membership::assert_candidate_checksum(index(), exists("a", true)).await;
}

#[tokio::test]
#[ignore = "manual same-root sparse presence benchmark; timing is not a CI assertion"]
async fn sparse_presence_candidate_benchmark() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("presence_benchmark");
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
            let mut row = doc([
                ("_id", BsonValue::Int32(id)),
                ("payload", BsonValue::from("x".repeat(4096))),
            ]);
            if id % 20 == 0 {
                row.push(
                    "a",
                    if id % 40 == 0 {
                        BsonValue::Array(vec![])
                    } else {
                        BsonValue::Null
                    },
                )
                .unwrap();
            }
            row
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
            build(&engine, &session, &namespace, index()).await;
        }
        for residual_miss in [false, true] {
            let mut query = exists("a", true);
            if residual_miss {
                query.push("unmatched", BsonValue::Int32(1)).unwrap();
            }
            let filter = DocumentFilter::new(query).unwrap();
            let expected = if residual_miss { 0 } else { 50 };
            for write in [false, true] {
                let command = if write {
                    DocumentCommand::Update(DocumentUpdateRequest::new(
                        namespace.clone(),
                        filter.clone(),
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
                        filter.clone(),
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
                    "presence benchmark indexed={indexed} write={write} residual_miss={residual_miss} documents=1000 present=50 payload_bytes=4096 shards=4 iterations=10 elapsed_us={}",
                    started.elapsed().as_micros()
                );
            }
        }
    }
    engine.shutdown().await.unwrap();
}
