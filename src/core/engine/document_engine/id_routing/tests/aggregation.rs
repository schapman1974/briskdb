use super::*;
use crate::document::{DocumentAggregateRequest, DocumentAggregator};

fn stage(name: &str, value: BsonValue) -> BsonDocument {
    doc([(name, value)])
}

fn matched(query: &DocumentFilter) -> BsonDocument {
    stage("$match", BsonValue::Document(query.document().clone()))
}

fn aggregate(pipeline: DocumentPipeline, batch: u64) -> DocumentCommand {
    DocumentCommand::Aggregate(
        DocumentAggregateRequest::new(
            ns(),
            pipeline,
            DocumentReadOptions::new().with_batch_size(batch).unwrap(),
        )
        .unwrap(),
    )
}

async fn aggregate_rows(
    engine: &Engine,
    session: &Session,
    pipeline: DocumentPipeline,
    batch: u64,
    shards: &[u16],
) -> Vec<BsonDocument> {
    let before = checkouts(engine);
    let mut result = call(engine, session, aggregate(pipeline, batch)).await;
    if batch == 0 {
        assert_touched(engine, &before, &[]);
    }
    let mut rows = Vec::new();
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages <= 100, "bounded fixture cursor must exhaust");
        assert_eq!(result.plan().unwrap().shards(), shards);
        let DocumentResult::Cursor(page) = result.into_parts().2 else {
            panic!("cursor");
        };
        rows.extend_from_slice(page.documents());
        let Some(id) = page.cursor_id() else {
            break;
        };
        result = call(
            engine,
            session,
            DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
                ns(),
                id,
                DocumentReadOptions::new().with_batch_size(2).unwrap(),
            )),
        )
        .await;
    }
    // Blocking pipeline stages may deliver later pages without more reads;
    // verify the union of actual source accesses across the entire cursor.
    assert_touched(engine, &before, shards);
    rows
}

fn source_documents() -> Vec<BsonDocument> {
    (0..64)
        .map(|id| {
            doc([
                ("_id", BsonValue::Int32(id)),
                ("rank", BsonValue::Int32(64 - id)),
            ])
        })
        .collect()
}

fn encoded(rows: &[BsonDocument]) -> Vec<Vec<u8>> {
    rows.iter()
        .map(|row| encode_document(row).unwrap())
        .collect()
}

#[tokio::test]
async fn leading_id_lists_preserve_pipeline_stages_global_paging_and_restart() {
    let root = tempfile::tempdir().unwrap();
    for reopen in [false, true] {
        let engine = Engine::open(root.path(), 8).await.unwrap();
        let session = engine.session();
        if !reopen {
            seed(&engine, &session).await;
        }
        let query = filter(&selected_ids(&engine));
        for tail in [
            vec![],
            vec![
                stage(
                    "$sort",
                    BsonValue::Document(doc([("rank", BsonValue::Int32(1))])),
                ),
                stage("$skip", BsonValue::Int32(1)),
                stage("$limit", BsonValue::Int32(4)),
                stage(
                    "$project",
                    BsonValue::Document(doc([("_id", BsonValue::Int32(1))])),
                ),
            ],
            vec![stage("$count", BsonValue::from("n"))],
            vec![
                stage("$skip", BsonValue::Int32(1)),
                stage("$limit", BsonValue::Int32(3)),
                stage(
                    "$group",
                    BsonValue::Document(doc([
                        ("_id", BsonValue::Int32(1)),
                        (
                            "n",
                            BsonValue::Document(doc([("$sum", BsonValue::Int32(1))])),
                        ),
                    ])),
                ),
            ],
        ] {
            let pipeline = DocumentPipeline::new(
                std::iter::once(matched(&query))
                    .chain(tail)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            let expected = DocumentAggregator::compile(&pipeline)
                .unwrap()
                .execute(&source_documents())
                .unwrap();
            for batch in [0, 2] {
                let actual =
                    aggregate_rows(&engine, &session, pipeline.clone(), batch, &[1, 6]).await;
                assert_eq!(encoded(&actual), encoded(&expected));
            }
        }
        engine.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn leading_exact_ids_use_one_owner_without_repeating_a_point_on_continuation() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 8).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    for id in [7, 1000] {
        let (_, shard) = engine
            .inner
            .database
            .storage
            .prepare_document_id(&BsonValue::Int32(id))
            .unwrap();
        for operand in [
            BsonValue::Int64(i64::from(id)),
            BsonValue::Document(doc([("$eq", BsonValue::Double(f64::from(id)))])),
        ] {
            let query = DocumentFilter::new(doc([("_id", operand)])).unwrap();
            let pipeline =
                DocumentPipeline::new(vec![matched(&query), stage("$count", BsonValue::from("n"))])
                    .unwrap();
            let expected = DocumentAggregator::compile(&pipeline)
                .unwrap()
                .execute(&source_documents())
                .unwrap();
            assert_eq!(
                encoded(&aggregate_rows(&engine, &session, pipeline, 0, &[shard]).await),
                encoded(&expected)
            );
        }
    }
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn selected_shards_still_deliver_unmatched_input_to_the_original_runner() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 8).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let selected = selected_ids(&engine);
    let pipeline = DocumentPipeline::new(vec![matched(&filter(&selected))]).unwrap();
    let mut runner = DocumentAggregator::compile(&pipeline)
        .unwrap()
        .into_stream();
    let token = CancellationToken::new();
    let control = OperationControl::new(None);
    let source =
        leading_match_source(&engine.inner.database.storage, &pipeline, &token, &control).unwrap();
    assert!(matches!(
        &source,
        PreparedFilterRoute::ShardSubset { matcher: None, .. }
    ));
    let collection_id = engine
        .inner
        .database
        .storage
        .document_collection_controlled(ns().database(), ns().collection(), Arc::clone(&control))
        .unwrap()
        .unwrap()
        .id();
    let mut cursor = CursorState {
        namespace: ns(),
        collection_id,
        source,
        projection: None,
        sorter: None,
        sort_after: None,
        after: None,
        skip: 0,
        remaining: None,
        batch_byte_limit: None,
        aggregation: None,
    };
    let (rows, more) = engine
        .read_document_source_page(
            ConnectionOwner::new(session.id().get()),
            &mut cursor,
            token,
            None,
            &DocumentReadOptions::new().with_batch_size(100).unwrap(),
            ResultLimits::new(100, 4 * 1024 * 1024).unwrap(),
        )
        .await
        .unwrap();
    assert!(!more);
    assert!(
        rows.len() > selected.len(),
        "source must not prefilter before aggregation accounting"
    );
    let expected_inputs: Vec<_> = source_documents()
        .into_iter()
        .filter(|row| {
            let (_, shard) = engine
                .inner
                .database
                .storage
                .prepare_document_id(row.get_first("_id").unwrap())
                .unwrap();
            [1, 6].contains(&shard)
        })
        .collect();
    assert_eq!(encoded(&rows), encoded(&expected_inputs));
    let mut matched = Vec::new();
    for row in rows {
        if let Some(row) = runner.push(row).unwrap() {
            matched.push(row);
        }
    }
    matched.extend(runner.finish().unwrap());
    assert_eq!(
        ids(&matched),
        selected
            .into_iter()
            .map(BsonValue::Int32)
            .collect::<Vec<_>>()
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn unsafe_leading_shapes_and_matches_after_transformations_keep_full_source_routing() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 8).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let query = filter(&selected_ids(&engine));
    for stages in [
        vec![
            stage(
                "$set",
                BsonValue::Document(doc([("_id", BsonValue::Int32(42))])),
            ),
            stage(
                "$match",
                BsonValue::Document(doc([("_id", BsonValue::Int32(42))])),
            ),
        ],
        vec![stage("$skip", BsonValue::Int32(3)), matched(&query)],
        vec![matched(&DocumentFilter::empty())],
        vec![matched(&in_values(vec![]))],
        vec![matched(&in_values(vec![
            BsonValue::Int32(7),
            BsonValue::RegularExpression(BsonRegex::new("^7", "").unwrap()),
        ]))],
        vec![stage(
            "$match",
            BsonValue::Document(doc([(
                "rank",
                BsonValue::Document(doc([("$in", BsonValue::Array(vec![BsonValue::Int32(7)]))])),
            )])),
        )],
    ] {
        let pipeline = DocumentPipeline::new(stages).unwrap();
        let expected = DocumentAggregator::compile(&pipeline)
            .unwrap()
            .execute(&source_documents())
            .unwrap();
        let actual =
            aggregate_rows(&engine, &session, pipeline, 2, &(0..8).collect::<Vec<_>>()).await;
        assert_eq!(encoded(&actual), encoded(&expected));
    }
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn later_invalid_stages_and_request_controls_fail_before_any_routed_source_read() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 8).await.unwrap();
    let session = engine.session();
    seed(&engine, &session).await;
    let query = filter(&selected_ids(&engine));
    for first in [
        matched(&query),
        stage(
            "$match",
            BsonValue::Document(doc([("_id", BsonValue::Int32(1000))])),
        ),
    ] {
        let before = checkouts(&engine);
        let pipeline = DocumentPipeline::new(vec![
            first,
            stage("$unsupported", BsonValue::Document(BsonDocument::new())),
        ])
        .unwrap();
        assert!(
            engine
                .execute_document(&session, request(aggregate(pipeline, 0)))
                .await
                .is_err()
        );
        assert_touched(&engine, &before, &[]);
    }
    for deadline in [false, true] {
        let before = checkouts(&engine);
        let mut context = RequestContext::new();
        if deadline {
            context = context
                .with_deadline(std::time::Instant::now() - std::time::Duration::from_secs(1));
        } else {
            let cancellation = CancellationToken::new();
            cancellation.cancel();
            context = context.with_cancellation_token(cancellation);
        }
        let error = engine
            .execute_document(
                &session,
                DocumentRequest::new(
                    DocumentRequestId::new([1; 16]).unwrap(),
                    context,
                    aggregate(DocumentPipeline::new(vec![matched(&query)]).unwrap(), 2),
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
