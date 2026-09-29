use super::*;
use briskdb::document::{DocumentReadAccess, DocumentScanReason};

fn count_pipeline(query: BsonDocument, windows: &[(&str, BsonValue)]) -> DocumentPipeline {
    let mut stages = vec![("$match", BsonValue::Document(query))];
    stages.extend_from_slice(windows);
    stages.push((
        "$group",
        BsonValue::Document(doc(&[
            ("_id", BsonValue::Int32(1)),
            (
                "n",
                BsonValue::Document(doc(&[("$sum", BsonValue::Int32(1))])),
            ),
        ])),
    ));
    pipeline(&stages)
}

fn access(result: &DocumentExecution) -> DocumentReadAccess {
    let Some(DocumentPlan::Scatter(plan)) = result.plan() else {
        panic!("scatter plan")
    };
    plan.read_access().unwrap()
}

#[tokio::test]
async fn scalar_count_stats_measure_storage_without_inventing_decoded_rows() {
    let (_root, engine) = setup(source_rows()).await;
    let session = engine.session();
    let options = DocumentReadOptions::new()
        .with_execution_stats(true)
        .with_plan_diagnostics(true);
    let first = call(
        &engine,
        &session,
        aggregate(
            count_pipeline(doc(&[]), &[]),
            options.clone().with_batch_size(0).unwrap(),
        ),
    )
    .await;
    assert_eq!(
        access(&first),
        DocumentReadAccess::Scan {
            reason: DocumentScanReason::CountRows
        }
    );
    assert_eq!(first.read_stats().unwrap().storage_reads(), 0);
    let id = cursor(first).0.unwrap();
    let next = call(
        &engine,
        &session,
        DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
            namespace(),
            id,
            options.clone(),
        )),
    )
    .await;
    assert_eq!(
        access(&next),
        DocumentReadAccess::Scan {
            reason: DocumentScanReason::CountRows
        }
    );
    let stats = next.read_stats().unwrap();
    assert_eq!(stats.storage_reads(), 4);
    assert_eq!(stats.shards_read().count(), 4);
    assert_eq!(stats.documents_examined(), 0);
    assert_eq!(stats.source_matches(), 0);
    assert_eq!(stats.matcher_evaluations(), 0);
    assert!(stats.storage_read_nanos() > 0);
    let (id, rows) = cursor(next);
    assert!(id.is_none());
    assert_eq!(
        rows,
        vec![doc(&[
            ("_id", BsonValue::Int32(1)),
            ("n", BsonValue::Int32(180))
        ])]
    );
    let filtered = call(
        &engine,
        &session,
        aggregate(
            count_pipeline(doc(&[("group", BsonValue::Int32(1))]), &[]),
            options.clone(),
        ),
    )
    .await;
    assert_eq!(filtered.read_stats().unwrap().documents_examined(), 180);
    assert_eq!(filtered.read_stats().unwrap().matcher_evaluations(), 180);
    assert_eq!(filtered.read_stats().unwrap().source_matches(), 60);
    assert_eq!(
        cursor(filtered).1[0].get_first("n"),
        Some(&BsonValue::Int32(60))
    );
    let limited = call(
        &engine,
        &session,
        aggregate(
            count_pipeline(
                doc(&[("group", BsonValue::Int32(1))]),
                &[("$limit", BsonValue::Int32(1))],
            ),
            options,
        ),
    )
    .await;
    assert_eq!(
        access(&limited),
        DocumentReadAccess::Scan {
            reason: DocumentScanReason::AggregationInput
        }
    );
    assert_eq!(limited.read_stats().unwrap().matcher_evaluations(), 0);
    assert!(limited.read_stats().unwrap().documents_examined() < 180);
    assert_eq!(
        cursor(limited).1[0].get_first("n"),
        Some(&BsonValue::Int32(1))
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn scalar_counts_keep_request_errors_result_bounds_and_cursor_cleanup() {
    let (_root, engine) = setup(source_rows()).await;
    let session = engine.session();
    let stages = count_pipeline(doc(&[]), &[]);
    let token = CancellationToken::new();
    token.cancel();
    for (context, expected) in [
        (
            RequestContext::new().with_cancellation_token(token),
            EngineErrorKind::Cancelled,
        ),
        (
            RequestContext::new().with_deadline(Instant::now() - Duration::from_millis(1)),
            EngineErrorKind::DeadlineExceeded,
        ),
        (
            RequestContext::new().with_result_limits(ResultLimits::new(1, 8).unwrap()),
            EngineErrorKind::LimitExceeded,
        ),
    ] {
        assert_eq!(
            engine
                .execute_document(
                    &session,
                    request(
                        aggregate(stages.clone(), DocumentReadOptions::new()),
                        context
                    )
                )
                .await
                .unwrap_err()
                .kind(),
            expected
        );
    }
    let first = call(
        &engine,
        &session,
        aggregate(
            stages.clone(),
            DocumentReadOptions::new().with_batch_size(0).unwrap(),
        ),
    )
    .await;
    let id = cursor(first).0.unwrap();
    let error = engine
        .execute_document(
            &session,
            request(
                more(id, 1),
                RequestContext::new().with_result_limits(ResultLimits::new(1, 8).unwrap()),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    assert_eq!(
        engine
            .execute_document(&session, request(more(id, 1), RequestContext::new()))
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
    let output = engine
        .execute_document(
            &session,
            request(
                aggregate(stages, DocumentReadOptions::new()),
                RequestContext::new().with_result_limits(ResultLimits::new(1, 256).unwrap()),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        cursor(output).1[0].get_first("n"),
        Some(&BsonValue::Int32(180))
    );
    engine.shutdown().await.unwrap();
}
