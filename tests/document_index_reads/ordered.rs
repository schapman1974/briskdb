use super::*;

fn sorted(direction: i32, limit: u64, skip: u64) -> DocumentReadOptions {
    DocumentReadOptions::new()
        .with_sort(DocumentSort::new(doc([("rank", BsonValue::Int32(direction))])).unwrap())
        .with_limit(limit)
        .unwrap()
        .with_skip(skip)
        .with_batch_size(limit)
        .unwrap()
        .with_execution_stats(true)
}

async fn measured(
    engine: &Engine,
    session: &Session,
    namespace: &DocumentNamespace,
    options: DocumentReadOptions,
) -> (u64, Vec<BsonDocument>) {
    let execution = call(
        engine,
        session,
        DocumentCommand::Find(DocumentFindRequest::new(
            namespace.clone(),
            DocumentFilter::empty(),
            options,
        )),
    )
    .await;
    let examined = execution.read_stats().unwrap().documents_examined();
    let (cursor, documents) = page(execution);
    assert!(cursor.is_none());
    (examined, documents)
}

#[tokio::test]
async fn ordered_top_k_reads_are_bounded_independently_of_collection_size_in_both_directions() {
    for count in [600, 4800] {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), 4).await.unwrap();
        let session = engine.session();
        let namespace = ns("ordered_top_k");
        seed(&engine, &session, &namespace, count).await;
        let mut expected = Vec::new();
        for direction in [1, -1] {
            let (examined, rows) =
                measured(&engine, &session, &namespace, sorted(direction, 10, 5)).await;
            assert_eq!(examined, count as u64 + 10);
            expected.push(rows);
        }
        build(
            &engine,
            &session,
            &namespace,
            DocumentIndexRequest::new(doc([("rank", BsonValue::Int32(1))])).unwrap(),
        )
        .await;
        for (direction, expected) in [1, -1].into_iter().zip(&expected) {
            let (examined, rows) =
                measured(&engine, &session, &namespace, sorted(direction, 10, 5)).await;
            assert_eq!(&rows, expected);
            // skip + k + one retained lookahead + one early-stop row per shard,
            // followed by exactly k result refetches. No collection-sized scan.
            assert!(
                examined <= 4 * (5 + 10 + 2) + 10,
                "{count}: examined {examined}"
            );
        }
        let equality = call(
            &engine,
            &session,
            DocumentCommand::Find(DocumentFindRequest::new(
                namespace.clone(),
                DocumentFilter::new(doc([("rank", BsonValue::Int32(count / 2))])).unwrap(),
                sorted(-1, 10, 0),
            )),
        )
        .await;
        assert_eq!(
            equality.read_stats().unwrap().documents_examined(),
            2,
            "sorting must preserve the existing selective equality seek"
        );
        assert_eq!(page(equality).1.len(), 1);
        engine.shutdown().await.unwrap();
        let engine = Engine::open(root.path(), 4).await.unwrap();
        let session = engine.session();
        let (examined, rows) = measured(&engine, &session, &namespace, sorted(-1, 10, 5)).await;
        assert_eq!(rows, expected[1]);
        assert!(examined <= 78);
        engine.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn ordered_compound_arrays_ties_filters_and_small_pages_equal_the_scan_sorter() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("ordered_compound");
    seed(&engine, &session, &namespace, 65).await;
    let query = doc([("enabled", BsonValue::Boolean(true))]);
    let options = |inverse| {
        DocumentReadOptions::new()
            .with_sort(
                DocumentSort::new(doc([
                    ("a", BsonValue::Int32(if inverse { -1 } else { 1 })),
                    ("b", BsonValue::Int32(if inverse { 1 } else { -1 })),
                ]))
                .unwrap(),
            )
            .with_batch_size(3)
            .unwrap()
            .with_skip(2)
    };
    let forward = find(&engine, &session, &namespace, &query, options(false)).await;
    let reverse = find(&engine, &session, &namespace, &query, options(true)).await;
    build(
        &engine,
        &session,
        &namespace,
        DocumentIndexRequest::new(doc([
            ("a", BsonValue::Int32(1)),
            ("b", BsonValue::Int32(-1)),
        ]))
        .unwrap(),
    )
    .await;
    assert_eq!(
        find(&engine, &session, &namespace, &query, options(false)).await,
        forward
    );
    assert_eq!(
        find(&engine, &session, &namespace, &query, options(true)).await,
        reverse
    );
    engine.shutdown().await.unwrap();
}
