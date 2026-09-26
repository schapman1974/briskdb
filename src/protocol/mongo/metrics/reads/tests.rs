use super::*;
use crate::document::{
    BsonValue, CanonicalBsonKey, DocumentCandidateKind, DocumentCollectionId, DocumentIndexId,
    DocumentPointPlan, DocumentReadStats, DocumentRequestId, DocumentScanReason,
    DocumentScatterPlan,
};

fn execution(plan: Option<DocumentPlan>, shards: u64) -> DocumentExecution {
    DocumentExecution::new(
        DocumentRequestId::new([1; 16]).unwrap(),
        plan,
        DocumentResult::Distinct(vec![BsonValue::from("private-value")].into_boxed_slice()),
    )
    .with_read_stats(Some(DocumentReadStats::from_counters(7, 5, 3, 2, shards)))
}

#[test]
fn read_metrics_classify_plans_without_retaining_payloads_or_counting_unobserved_calls() {
    let counters = ReadCounters::default();
    let collection = DocumentCollectionId::from_validated(1);
    let point = DocumentPointPlan::new(
        collection,
        2,
        CanonicalBsonKey::encode(&BsonValue::from("private-id")).unwrap(),
    )
    .unwrap();
    counters.observe(&execution(Some(DocumentPlan::Point(point)), 1 << 2));
    let scatter = DocumentScatterPlan::new(collection, vec![0, 1, 2]).unwrap();
    for kind in [
        DocumentCandidateKind::Equality,
        DocumentCandidateKind::NecessaryFinite,
        DocumentCandidateKind::LogicalFinite,
        DocumentCandidateKind::SparsePresence,
        DocumentCandidateKind::StringRange,
    ] {
        counters.observe(&execution(
            Some(DocumentPlan::Scatter(scatter.clone().with_read_access(
                DocumentReadAccess::IndexCandidates {
                    index_id: DocumentIndexId::from_validated(2),
                    kind,
                    key_count: 1,
                },
            ))),
            0b101,
        ));
    }
    counters.observe(&execution(
        Some(DocumentPlan::Scatter(scatter.clone().with_read_access(
            DocumentReadAccess::Scan {
                reason: DocumentScanReason::AggregationInput,
            },
        ))),
        0,
    ));
    counters.observe(&execution(Some(DocumentPlan::Scatter(scatter)), 0b010));
    let unplanned = execution(None, 0);
    counters.observe(&unplanned);
    let snapshot = counters.snapshot();
    counters.observe(&unplanned.with_read_stats(None));
    assert_eq!(counters.snapshot(), snapshot);
    assert_eq!(
        (
            snapshot.executions,
            snapshot.point_plans,
            snapshot.index_candidate_plans,
            snapshot.scan_plans,
            snapshot.unclassified_plans
        ),
        (9, 1, 5, 1, 2)
    );
    assert_eq!(
        (
            snapshot.storage_reads,
            snapshot.documents_examined,
            snapshot.matcher_evaluations,
            snapshot.source_matches,
            snapshot.output_items
        ),
        (63, 45, 27, 18, 9)
    );
    assert_eq!(
        (snapshot.planned_shard_targets, snapshot.shard_visits),
        (22, 12)
    );
    assert_eq!(snapshot.fanout_buckets, [2, 2, 5, 0, 0, 0, 0, 0]);
    assert_eq!(&snapshot.shard_requests[..3], &[5, 1, 6]);
    assert!(snapshot.shard_requests[3..].iter().all(|count| *count == 0));
    assert!(!format!("{snapshot:?}").contains("private"));
}

#[test]
fn read_metrics_bucket_every_bounded_fanout_and_saturate_totals() {
    let counters = ReadCounters::default();
    for count in 0..=64 {
        let mask = if count == 64 {
            u64::MAX
        } else {
            (1_u64 << count) - 1
        };
        counters.observe(&execution(None, mask));
    }
    let snapshot = counters.snapshot();
    assert_eq!(snapshot.executions, 65);
    assert_eq!(snapshot.fanout_buckets, [1, 1, 1, 2, 4, 8, 16, 32]);
    assert_eq!(
        (snapshot.shard_visits, snapshot.peak_shards_read),
        (2080, 64)
    );
    assert_eq!(
        snapshot.shard_requests,
        std::array::from_fn(|i| 64 - i as u64)
    );
    counters.executions.store(u64::MAX, Ordering::Relaxed);
    counters
        .storage_reads
        .store(u64::MAX - 1, Ordering::Relaxed);
    counters.fanout[7].store(u64::MAX, Ordering::Relaxed);
    counters.matches.store(u64::MAX - 1, Ordering::Relaxed);
    counters.shards[63].store(u64::MAX, Ordering::Relaxed);
    counters.observe(&execution(None, u64::MAX));
    let snapshot = counters.snapshot();
    assert_eq!(snapshot.executions, u64::MAX);
    assert_eq!(snapshot.storage_reads, u64::MAX);
    assert_eq!(snapshot.source_matches, u64::MAX);
    assert_eq!(snapshot.fanout_buckets[7], u64::MAX);
    assert_eq!(snapshot.shard_requests[63], u64::MAX);
}

#[test]
fn read_metrics_toggle_preserves_inflight_observations_and_concurrent_totals() {
    use crate::protocol::mongo::metrics::Metrics;
    let metrics = std::sync::Arc::new(Metrics::default());
    assert!(!metrics.read_metrics_enabled());
    metrics.set_read_metrics_enabled(true);
    assert!(metrics.snapshot().read_metrics_enabled);
    metrics.set_read_metrics_enabled(false);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let metrics = std::sync::Arc::clone(&metrics);
            scope.spawn(move || {
                let execution = execution(None, 0b101);
                for _ in 0..1000 {
                    metrics.observe_read(&execution);
                }
            });
        }
    });
    let snapshot = metrics.snapshot();
    assert!(!snapshot.read_metrics_enabled);
    assert_eq!(snapshot.reads.executions, 4000);
    assert_eq!(snapshot.reads.shard_visits, 8000);
    assert_eq!(snapshot.reads.storage_reads, 28000);
    assert_eq!(snapshot.reads.source_matches, 8000);
    assert_eq!(snapshot.reads.fanout_buckets, [0, 0, 4000, 0, 0, 0, 0, 0]);
}
