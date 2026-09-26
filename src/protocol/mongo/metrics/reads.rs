//! Optional successful engine-read observations, never query or namespace labels.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::document::{DocumentExecution, DocumentPlan, DocumentReadAccess, DocumentResult};

use super::{add, get};

/// Inclusive, disjoint buckets for actual distinct shards read per engine page.
/// The engine supports at most 64 shards; zero includes buffered/empty pages.
pub const MONGO_READ_SHARD_FANOUT_UPPER_BOUNDS: [u64; 8] = [0, 1, 2, 4, 8, 16, 32, 64];

/// Cumulative successful engine find/getMore/aggregate/distinct observations.
/// A page can succeed here and subsequently fail wire encoding or delivery.
/// Failed engine calls expose no partial work snapshot and are excluded.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MongoReadMetrics {
    pub executions: u64,
    /// Record-read calls, including misses and repeated lookahead/rescans.
    pub storage_reads: u64,
    pub documents_examined: u64,
    /// Source matcher evaluations, not pipeline predicates or matched rows.
    pub matcher_evaluations: u64,
    /// Source-predicate acceptances, including repeated/lookahead reads and
    /// unfiltered/ID hits, before skip/limit/projection/pipeline processing.
    pub source_matches: u64,
    /// Documents in returned engine batches, or distinct values (not matches).
    pub output_items: u64,
    pub point_plans: u64,
    pub index_candidate_plans: u64,
    pub scan_plans: u64,
    pub unclassified_plans: u64,
    /// Planned owners, including owners not actually read on this page.
    pub planned_shard_targets: u64,
    /// Sum of distinct actually-read shards per observed execution.
    pub shard_visits: u64,
    pub peak_shards_read: u64,
    pub fanout_buckets: [u64; 8],
    /// Read requests touching each fixed physical shard ordinal, not row counts.
    /// Ordinals are shared across namespaces; no identity becomes a label.
    pub shard_requests: [u64; 64],
    /// Examined BSON record observations per physical ordinal, including rereads.
    pub shard_documents_examined: [u64; 64],
    /// Source predicate acceptances per physical ordinal, including rereads.
    /// This is row-work distribution, not CPU, bytes or unique-result skew.
    pub shard_source_matches: [u64; 64],
}

pub(super) struct ReadCounters {
    executions: AtomicU64,
    storage_reads: AtomicU64,
    documents: AtomicU64,
    matchers: AtomicU64,
    matches: AtomicU64,
    outputs: AtomicU64,
    point: AtomicU64,
    indexed: AtomicU64,
    scanned: AtomicU64,
    unclassified: AtomicU64,
    planned_shards: AtomicU64,
    shard_visits: AtomicU64,
    peak_shards: AtomicU64,
    fanout: [AtomicU64; 8],
    shards: [AtomicU64; 64],
    shard_documents: [AtomicU64; 64],
    shard_matches: [AtomicU64; 64],
}

impl Default for ReadCounters {
    fn default() -> Self {
        Self {
            executions: AtomicU64::new(0),
            storage_reads: AtomicU64::new(0),
            documents: AtomicU64::new(0),
            matchers: AtomicU64::new(0),
            matches: AtomicU64::new(0),
            outputs: AtomicU64::new(0),
            point: AtomicU64::new(0),
            indexed: AtomicU64::new(0),
            scanned: AtomicU64::new(0),
            unclassified: AtomicU64::new(0),
            planned_shards: AtomicU64::new(0),
            shard_visits: AtomicU64::new(0),
            peak_shards: AtomicU64::new(0),
            fanout: std::array::from_fn(|_| AtomicU64::new(0)),
            shards: std::array::from_fn(|_| AtomicU64::new(0)),
            shard_documents: std::array::from_fn(|_| AtomicU64::new(0)),
            shard_matches: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl ReadCounters {
    pub(super) fn observe(&self, execution: &DocumentExecution) {
        let Some(stats) = execution.read_stats() else {
            return;
        };
        add(&self.executions, 1);
        add(&self.storage_reads, stats.storage_reads());
        add(&self.documents, stats.documents_examined());
        add(&self.matchers, stats.matcher_evaluations());
        add(&self.matches, stats.source_matches());
        let outputs = match execution.result() {
            DocumentResult::Cursor(batch) => batch.documents().len(),
            DocumentResult::Distinct(values) => values.len(),
            _ => 0,
        };
        add(&self.outputs, outputs as u64);
        let plan_counter = match execution.plan() {
            Some(DocumentPlan::Point(_)) => &self.point,
            Some(DocumentPlan::Scatter(plan)) => match plan.read_access() {
                Some(DocumentReadAccess::IndexCandidates { .. }) => &self.indexed,
                Some(DocumentReadAccess::Scan { .. }) => &self.scanned,
                None => &self.unclassified,
            },
            None => &self.unclassified,
        };
        add(plan_counter, 1);
        if let Some(plan) = execution.plan() {
            add(&self.planned_shards, plan.shards().len() as u64);
        }
        let mut visits = 0;
        for work in stats.shard_work() {
            let shard = usize::from(work.shard());
            add(&self.shards[shard], 1);
            add(&self.shard_documents[shard], work.documents_examined());
            add(&self.shard_matches[shard], work.source_matches());
            visits += 1;
        }
        add(&self.shard_visits, visits);
        self.peak_shards.fetch_max(visits, Ordering::Relaxed);
        let bucket = MONGO_READ_SHARD_FANOUT_UPPER_BOUNDS
            .iter()
            .position(|bound| visits <= *bound)
            .expect("document read stats have at most 64 shard bits");
        add(&self.fanout[bucket], 1);
    }

    pub(super) fn snapshot(&self) -> MongoReadMetrics {
        MongoReadMetrics {
            executions: get(&self.executions),
            storage_reads: get(&self.storage_reads),
            documents_examined: get(&self.documents),
            matcher_evaluations: get(&self.matchers),
            source_matches: get(&self.matches),
            output_items: get(&self.outputs),
            point_plans: get(&self.point),
            index_candidate_plans: get(&self.indexed),
            scan_plans: get(&self.scanned),
            unclassified_plans: get(&self.unclassified),
            planned_shard_targets: get(&self.planned_shards),
            shard_visits: get(&self.shard_visits),
            peak_shards_read: get(&self.peak_shards),
            fanout_buckets: std::array::from_fn(|i| get(&self.fanout[i])),
            shard_requests: std::array::from_fn(|i| get(&self.shards[i])),
            shard_documents_examined: std::array::from_fn(|i| get(&self.shard_documents[i])),
            shard_source_matches: std::array::from_fn(|i| get(&self.shard_matches[i])),
        }
    }
}

#[cfg(test)]
mod tests;
