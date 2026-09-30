use super::*;
use briskdb::document::{
    DocumentDeleteRequest, DocumentMutationScope, DocumentUpdate, DocumentUpdateRequest,
};

#[tokio::test]
async fn large_unindexed_sort_spools_once_across_small_pages_and_preserves_skip_limit() {
    let root = tempfile::tempdir().unwrap();
    let options = briskdb::core::EngineOptions::default()
        .with_request_timeout(Some(std::time::Duration::from_secs(180)))
        .unwrap();
    let engine = Engine::open_with_options(root.path(), 4, options)
        .await
        .unwrap();
    let session = engine.session();
    seed(&engine, &session, 0).await;
    const COUNT: i32 = 288;
    for id in 0..COUNT {
        let document = BsonDocument::from_entries([
            ("_id", BsonValue::Int32(id)),
            ("visible", BsonValue::Boolean(true)),
            (
                "rank",
                BsonValue::String(format!("{:03}{}", COUNT - id, "x".repeat(512 * 1024))),
            ),
        ])
        .unwrap();
        call(
            &engine,
            &session,
            DocumentCommand::Insert(
                DocumentInsertRequest::new(
                    namespace(),
                    vec![document],
                    DocumentWriteOptions::new(),
                )
                .unwrap(),
            ),
        )
        .await;
    }
    for (direction, skip, limit) in [(1, 0, None), (-1, 5, Some(270))] {
        let mut options = DocumentReadOptions::new()
            .with_execution_stats(true)
            .with_sort(
                DocumentSort::new(
                    BsonDocument::from_entries([("rank", BsonValue::Int32(direction))]).unwrap(),
                )
                .unwrap(),
            )
            .with_projection(
                DocumentProjection::new(
                    BsonDocument::from_entries([("_id", BsonValue::Int32(1))]).unwrap(),
                )
                .unwrap(),
            )
            .with_skip(skip)
            .with_batch_size(7)
            .unwrap();
        if let Some(limit) = limit {
            options = options.with_limit(limit).unwrap();
        }
        let first = call(&engine, &session, find(options)).await;
        let mut examined = first.read_stats().unwrap().documents_examined();
        let (mut id, mut documents) = cursor(first);
        assert_eq!(
            examined,
            COUNT as u64 + documents.len() as u64,
            "the first page must reuse the memory prefix without a second source scan"
        );
        while let Some(current) = id {
            let next = call(
                &engine,
                &session,
                DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
                    namespace(),
                    current,
                    DocumentReadOptions::new()
                        .with_execution_stats(true)
                        .with_batch_size(7)
                        .unwrap(),
                )),
            )
            .await;
            examined += next.read_stats().unwrap().documents_examined();
            let (next, rows) = cursor(next);
            assert!(rows.len() <= 7);
            assert!(rows.iter().all(|row| row.len() == 1));
            documents.extend(rows);
            id = next;
        }
        let mut expected: Vec<_> = (0..COUNT).collect();
        if direction == 1 {
            expected.reverse();
        }
        let expected: Vec<_> = expected
            .into_iter()
            .skip(skip as usize)
            .take(limit.unwrap_or(COUNT as u64) as usize)
            .collect();
        assert_eq!(ids(&documents), expected);
        // One bounded discovery scan, one spool scan, then selected refetches;
        // neither subsequent output pages nor extra windows rescan the source.
        assert_eq!(examined, 2 * COUNT as u64 + documents.len() as u64);
    }
    let no_disk = DocumentReadOptions::new()
        .with_allow_disk_use(false)
        .with_sort(
            DocumentSort::new(BsonDocument::from_entries([("rank", BsonValue::Int32(1))]).unwrap())
                .unwrap(),
        )
        .with_projection(
            DocumentProjection::new(
                BsonDocument::from_entries([("_id", BsonValue::Int32(1))]).unwrap(),
            )
            .unwrap(),
        )
        .with_batch_size(7)
        .unwrap();
    let (mut id, _) = cursor(call(&engine, &session, find(no_disk.clone())).await);
    let change_policy = DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
        namespace(),
        id.unwrap(),
        DocumentReadOptions::new().with_allow_disk_use(true),
    ));
    assert_eq!(
        engine
            .execute_document(&session, request(change_policy))
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::InvalidArgument
    );
    loop {
        let result = engine
            .execute_document(&session, request(more(id.unwrap(), 7)))
            .await;
        match result {
            Ok(next) => {
                id = cursor(next).0;
                assert!(
                    id.is_some(),
                    "large no-disk sort cannot finish by silently spilling"
                );
            }
            Err(error) => {
                assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
                break;
            }
        }
    }
    // A limited query fitting the memory prefix is still valid with no disk.
    let mut options = no_disk.with_limit(10).unwrap().with_batch_size(10).unwrap();
    options = options.with_execution_stats(true);
    let (id, documents) = cursor(call(&engine, &session, find(options)).await);
    assert!(id.is_none());
    assert_eq!(
        ids(&documents),
        (COUNT - 10..COUNT).rev().collect::<Vec<_>>()
    );

    // Spool keys are candidate positions, not cached documents or a snapshot.
    // Byte-short pages must recheck deletion, membership and sort-key changes.
    let options = DocumentReadOptions::new()
        .with_sort(
            DocumentSort::new(
                BsonDocument::from_entries([("rank", BsonValue::Int32(-1))]).unwrap(),
            )
            .unwrap(),
        )
        .with_projection(
            DocumentProjection::new(
                BsonDocument::from_entries([
                    ("_id", BsonValue::Int32(1)),
                    ("tag", BsonValue::Int32(1)),
                ])
                .unwrap(),
            )
            .unwrap(),
        )
        .with_batch_size(7)
        .unwrap()
        .with_batch_byte_limit(256)
        .unwrap();
    let first = call(
        &engine,
        &session,
        DocumentCommand::Find(DocumentFindRequest::new(
            namespace(),
            DocumentFilter::new(
                BsonDocument::from_entries([("visible", BsonValue::Boolean(true))]).unwrap(),
            )
            .unwrap(),
            options,
        )),
    )
    .await;
    let (mut id, mut documents) = cursor(first);
    assert!(id.is_some());
    assert!(documents.len() < 7);
    // Consume the memory prefix before mutating later positions, so these
    // changes exercise an already-built external spool rather than just keys
    // retained by the initial bounded window.
    while documents.len() < 40 {
        let (next, rows) = cursor(call(&engine, &session, more(id.unwrap(), 7)).await);
        documents.extend(rows);
        id = next;
    }
    let by_id = |id| {
        DocumentFilter::new(BsonDocument::from_entries([("_id", BsonValue::Int32(id))]).unwrap())
            .unwrap()
    };
    call(
        &engine,
        &session,
        DocumentCommand::Delete(DocumentDeleteRequest::new(
            namespace(),
            by_id(100),
            DocumentMutationScope::One,
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    for (id, field, value) in [
        (101, "rank", BsonValue::String("moved".into())),
        (102, "tag", BsonValue::String("changed".into())),
        (103, "visible", BsonValue::Boolean(false)),
    ] {
        let changes = BsonDocument::from_entries([(field, value)]).unwrap();
        call(
            &engine,
            &session,
            DocumentCommand::Update(DocumentUpdateRequest::new(
                namespace(),
                by_id(id),
                DocumentUpdate::new(
                    BsonDocument::from_entries([("$set", BsonValue::Document(changes))]).unwrap(),
                )
                .unwrap(),
                DocumentMutationScope::One,
                DocumentWriteOptions::new(),
            )),
        )
        .await;
    }
    while let Some(current) = id {
        let (next, rows) = cursor(call(&engine, &session, more(current, 7)).await);
        documents.extend(rows);
        id = next;
    }
    assert_eq!(
        ids(&documents),
        (0..COUNT)
            .filter(|id| ![100, 101, 103].contains(id))
            .collect::<Vec<_>>()
    );
    let changed = documents
        .iter()
        .find(|row| row.get_first("_id") == Some(&BsonValue::Int32(102)))
        .unwrap();
    assert_eq!(
        changed.get_first("tag"),
        Some(&BsonValue::String("changed".into()))
    );
    engine.shutdown().await.unwrap();
}
