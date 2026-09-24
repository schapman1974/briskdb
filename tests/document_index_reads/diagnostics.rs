use super::*;
use briskdb::{
    core::ResultLimits,
    document::{
        DocumentAggregateRequest, DocumentCandidateKind, DocumentPipeline, DocumentPlan,
        DocumentReadAccess, DocumentScanReason,
    },
};

fn options() -> DocumentReadOptions {
    DocumentReadOptions::new().with_plan_diagnostics(true)
}

fn access(execution: &DocumentExecution) -> DocumentReadAccess {
    let Some(DocumentPlan::Scatter(plan)) = execution.plan() else {
        panic!("scatter plan expected")
    };
    plan.read_access().expect("opt-in access diagnostics")
}

fn command(
    namespace: &DocumentNamespace,
    filter: BsonDocument,
    options: DocumentReadOptions,
) -> DocumentCommand {
    DocumentCommand::Find(DocumentFindRequest::new(
        namespace.clone(),
        DocumentFilter::new(filter).unwrap(),
        options,
    ))
}

#[tokio::test]
async fn diagnostics_classify_candidate_proofs_and_scan_reasons_without_payloads() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("diagnostics");
    seed(&engine, &session, &namespace, 21).await;
    let query = doc([("a", BsonValue::Int32(1))]);
    let ordinary = call(
        &engine,
        &session,
        command(&namespace, query.clone(), DocumentReadOptions::new()),
    )
    .await;
    assert!(
        matches!(ordinary.plan(), Some(DocumentPlan::Scatter(plan)) if plan.read_access().is_none())
    );
    let unindexed = call(
        &engine,
        &session,
        command(&namespace, query.clone(), options()),
    )
    .await;
    assert_eq!(
        access(&unindexed),
        DocumentReadAccess::Scan {
            reason: DocumentScanReason::NoReadyIndex
        }
    );
    assert_eq!(page(ordinary).1, page(unindexed).1);
    let unfiltered = call(&engine, &session, command(&namespace, doc([]), options())).await;
    assert_eq!(
        access(&unfiltered),
        DocumentReadAccess::Scan {
            reason: DocumentScanReason::Unfiltered
        }
    );
    build(
        &engine,
        &session,
        &namespace,
        DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap(),
    )
    .await;
    let cases = [
        (query, DocumentCandidateKind::Equality, 1),
        (
            doc([(
                "a",
                obj([(
                    "$in",
                    BsonValue::Array(vec![
                        BsonValue::Int32(1),
                        BsonValue::Int32(2),
                        BsonValue::Double(1.0),
                    ]),
                )]),
            )]),
            DocumentCandidateKind::NecessaryFinite,
            2,
        ),
        (
            doc([("a", obj([("$exists", BsonValue::Boolean(false))]))]),
            DocumentCandidateKind::NecessaryFinite,
            1,
        ),
        (
            doc([(
                "$or",
                BsonValue::Array(vec![
                    obj([("a", BsonValue::Int32(1))]),
                    obj([("a", BsonValue::Int32(2))]),
                ]),
            )]),
            DocumentCandidateKind::LogicalFinite,
            2,
        ),
    ];
    for (query, kind, key_count) in cases {
        let expected = find(
            &engine,
            &session,
            &namespace,
            &query,
            DocumentReadOptions::new(),
        )
        .await;
        let actual = call(&engine, &session, command(&namespace, query, options())).await;
        assert!(
            matches!(access(&actual), DocumentReadAccess::IndexCandidates { kind: k, key_count: n, .. } if k == kind && n == key_count)
        );
        assert_eq!(
            page(actual)
                .1
                .iter()
                .map(|d| encode_document(d).unwrap())
                .collect::<Vec<_>>(),
            expected
        );
    }
    let private = "diagnostics-must-not-expose-this-value";
    let private_result = call(
        &engine,
        &session,
        command(
            &namespace,
            doc([("a", BsonValue::from(private))]),
            options(),
        ),
    )
    .await;
    assert!(!format!("{:?}", private_result.plan()).contains(private));
    for query in [
        doc([("a", obj([("$gt", BsonValue::Int32(0))]))]),
        doc([("unknown", BsonValue::from(private))]),
        doc([(
            "a",
            obj([(
                "$in",
                BsonValue::Array((0..129).map(BsonValue::Int32).collect()),
            )]),
        )]),
    ] {
        let result = call(&engine, &session, command(&namespace, query, options())).await;
        assert_eq!(
            access(&result),
            DocumentReadAccess::Scan {
                reason: DocumentScanReason::NoSafeProbe
            }
        );
    }
    drop_index(&engine, &session, &namespace).await;
    build(
        &engine,
        &session,
        &namespace,
        DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))]))
            .unwrap()
            .with_sparse(true),
    )
    .await;
    let result = call(
        &engine,
        &session,
        command(
            &namespace,
            doc([("a", obj([("$exists", BsonValue::Boolean(true))]))]),
            options(),
        ),
    )
    .await;
    assert!(matches!(
        access(&result),
        DocumentReadAccess::IndexCandidates {
            kind: DocumentCandidateKind::SparsePresence,
            key_count: 0,
            ..
        }
    ));
    let result = call(
        &engine,
        &session,
        command(
            &namespace,
            doc([("a", obj([("$exists", BsonValue::Boolean(false))]))]),
            options(),
        ),
    )
    .await;
    assert_eq!(
        access(&result),
        DocumentReadAccess::Scan {
            reason: DocumentScanReason::NoSafeProbe
        }
    );
    let result = call(
        &engine,
        &session,
        command(&namespace, doc([("_id", BsonValue::Int32(1))]), options()),
    )
    .await;
    assert!(matches!(result.plan(), Some(DocumentPlan::Point(_))));
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn diagnostics_reselect_after_cursor_index_drop_rebuild_and_opt_out() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("diagnostic_cursor");
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
    let first = call(
        &engine,
        &session,
        command(&namespace, query, options().with_batch_size(1).unwrap()),
    )
    .await;
    let DocumentReadAccess::IndexCandidates {
        index_id: initial, ..
    } = access(&first)
    else {
        panic!("index")
    };
    let (mut cursor, mut documents) = page(first);
    let mut pages = 0;
    while let Some(id) = cursor {
        if pages % 2 == 0 {
            drop_index(&engine, &session, &namespace).await;
        } else {
            build(&engine, &session, &namespace, definition()).await;
        }
        let enabled = pages % 3 != 2;
        let result = call(
            &engine,
            &session,
            DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
                namespace.clone(),
                id,
                options()
                    .with_plan_diagnostics(enabled)
                    .with_batch_size(3)
                    .unwrap(),
            )),
        )
        .await;
        if !enabled {
            assert!(
                matches!(result.plan(), Some(DocumentPlan::Scatter(plan)) if plan.read_access().is_none())
            );
        } else if pages % 2 == 0 {
            assert_eq!(
                access(&result),
                DocumentReadAccess::Scan {
                    reason: DocumentScanReason::NoReadyIndex
                }
            );
        } else {
            assert!(
                matches!(access(&result), DocumentReadAccess::IndexCandidates { index_id, .. } if index_id != initial)
            );
        }
        let next = page(result);
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
async fn diagnostics_distinct_and_aggregation_describe_their_actual_source_paths() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("diagnostic_commands");
    seed(&engine, &session, &namespace, 14).await;
    build(
        &engine,
        &session,
        &namespace,
        DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap(),
    )
    .await;
    let query = doc([("a", BsonValue::Int32(1))]);
    let distinct = call(
        &engine,
        &session,
        DocumentCommand::Distinct(
            DocumentDistinctRequest::new(
                namespace.clone(),
                "a",
                DocumentFilter::new(query.clone()).unwrap(),
                options(),
            )
            .unwrap(),
        ),
    )
    .await;
    assert!(matches!(
        access(&distinct),
        DocumentReadAccess::IndexCandidates {
            kind: DocumentCandidateKind::Equality,
            ..
        }
    ));
    let aggregate = call(
        &engine,
        &session,
        DocumentCommand::Aggregate(
            DocumentAggregateRequest::new(
                namespace.clone(),
                DocumentPipeline::new(vec![doc([("$match", BsonValue::Document(query.clone()))])])
                    .unwrap(),
                options().with_batch_size(1).unwrap(),
            )
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        access(&aggregate),
        DocumentReadAccess::Scan {
            reason: DocumentScanReason::AggregationInput
        }
    );
    let (cursor, _) = page(aggregate);
    let next = call(
        &engine,
        &session,
        DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
            namespace.clone(),
            cursor.unwrap(),
            options(),
        )),
    )
    .await;
    assert_eq!(
        access(&next),
        DocumentReadAccess::Scan {
            reason: DocumentScanReason::AggregationInput
        }
    );
    let count = DocumentCommand::Count(DocumentCountRequest::new(
        namespace.clone(),
        DocumentFilter::new(query).unwrap(),
        options(),
    ));
    let error = engine
        .execute_document(
            &session,
            DocumentRequest::new(
                DocumentRequestId::new([2; 16]).unwrap(),
                RequestContext::new(),
                count,
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Unsupported);
    let mutation = DocumentCommand::FindOneAndDelete(DocumentFindOneAndDeleteRequest::new(
        namespace.clone(),
        DocumentFilter::empty(),
        options(),
    ));
    let error = engine
        .execute_document(
            &session,
            DocumentRequest::new(
                DocumentRequestId::new([3; 16]).unwrap(),
                RequestContext::new(),
                mutation,
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Unsupported);
    assert_eq!(
        page(call(&engine, &session, command(&namespace, doc([]), options())).await)
            .1
            .len(),
        14
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn diagnostics_charge_exact_metadata_bytes_before_page_packing_and_release_on_error() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("diagnostic_budget");
    seed(&engine, &session, &namespace, 7).await;
    let rows = page(
        call(
            &engine,
            &session,
            command(&namespace, doc([]), DocumentReadOptions::new()),
        )
        .await,
    )
    .1;
    let base = 16
        + 8
        + namespace.database().len() as u64
        + 1
        + namespace.collection().len() as u64
        + 9
        + 8
        + 4 * 2;
    let row_bytes = |i: usize| 8 + 9 + encode_document(&rows[i]).unwrap().len() as u64;
    let limit = base + row_bytes(0) + row_bytes(1);
    for sorted in [false, true] {
        let read = |enabled| {
            let opts = options()
                .with_plan_diagnostics(enabled)
                .with_batch_byte_limit(limit)
                .unwrap();
            if sorted {
                opts.with_sort(DocumentSort::new(doc([("_id", BsonValue::Int32(1))])).unwrap())
            } else {
                opts
            }
        };
        let normal = call(&engine, &session, command(&namespace, doc([]), read(false))).await;
        assert_eq!(page(normal).1.len(), 2);
        let diagnostic = call(&engine, &session, command(&namespace, doc([]), read(true))).await;
        assert_eq!(page(diagnostic).1.len(), 1);
    }
    let exact = base + row_bytes(0) + 32;
    // Empty initial pages register before final envelope accounting; failed
    // delivery must remove every slot even though the client never saw its ID.
    for _ in 0..12 {
        let result = engine
            .execute_document(
                &session,
                DocumentRequest::new(
                    DocumentRequestId::new([6; 16]).unwrap(),
                    RequestContext::new()
                        .with_result_limits(ResultLimits::new(1, base + 31).unwrap()),
                    command(&namespace, doc([]), options().with_batch_size(0).unwrap()),
                ),
            )
            .await;
        assert_eq!(result.unwrap_err().kind(), EngineErrorKind::LimitExceeded);
    }
    for bytes in [exact - 1, exact] {
        let context =
            RequestContext::new().with_result_limits(ResultLimits::new(1, bytes).unwrap());
        let request = DocumentRequest::new(
            DocumentRequestId::new([3; 16]).unwrap(),
            context,
            command(&namespace, doc([]), options().with_batch_size(1).unwrap()),
        );
        let result = engine.execute_document(&session, request).await;
        if bytes < exact {
            assert_eq!(result.unwrap_err().kind(), EngineErrorKind::LimitExceeded);
        } else {
            assert_eq!(page(result.unwrap()).1, rows[..1]);
        }
    }
    let token = CancellationToken::new();
    token.cancel();
    let result = engine
        .execute_document(
            &session,
            DocumentRequest::new(
                DocumentRequestId::new([4; 16]).unwrap(),
                RequestContext::new().with_cancellation_token(token),
                command(&namespace, doc([]), options()),
            ),
        )
        .await;
    assert_eq!(result.unwrap_err().kind(), EngineErrorKind::Cancelled);
    let result = engine
        .execute_document(
            &session,
            DocumentRequest::new(
                DocumentRequestId::new([5; 16]).unwrap(),
                RequestContext::new().with_deadline(Instant::now() - Duration::from_secs(1)),
                command(&namespace, doc([]), options()),
            ),
        )
        .await;
    assert_eq!(
        result.unwrap_err().kind(),
        EngineErrorKind::DeadlineExceeded
    );
    let result = call(&engine, &session, command(&namespace, doc([]), options())).await;
    assert_eq!(page(result).1, rows);
    engine.shutdown().await.unwrap();
}
