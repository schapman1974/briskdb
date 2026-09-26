//! Payload-free work observed during one successful native read request.

/// Request-local counters, including lookahead, sorting rescans and repeated
/// reads. These are not distinct-document counts, SQLite rows/pages visited,
/// byte I/O, or cumulative cursor totals. Failed requests return no snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DocumentReadStats {
    storage_reads: u64,
    documents_examined: u64,
    matcher_evaluations: u64,
    source_matches: u64,
    shard_mask: u64,
}

impl DocumentReadStats {
    pub(crate) const fn from_counters(
        storage_reads: u64,
        documents_examined: u64,
        matcher_evaluations: u64,
        source_matches: u64,
        shard_mask: u64,
    ) -> Self {
        Self {
            storage_reads,
            documents_examined,
            matcher_evaluations,
            source_matches,
            shard_mask,
        }
    }

    /// Point/candidate record-read calls, including calls that find no record.
    pub const fn storage_reads(&self) -> u64 {
        self.storage_reads
    }

    /// Stored BSON records delivered to the read engine, before matching,
    /// projection, skip/limit or pipeline processing; repeated reads count again.
    pub const fn documents_examined(&self) -> u64 {
        self.documents_examined
    }

    /// Full source-matcher evaluations. Aggregation pipeline predicates are not
    /// source matcher evaluations and are deliberately not counted here.
    pub const fn matcher_evaluations(&self) -> u64 {
        self.matcher_evaluations
    }

    /// Record observations accepted by the source predicate (including direct
    /// ID hits and unfiltered reads), before sort-key position rechecks,
    /// projection, skip/limit and pipeline processing. Lookahead and repeated
    /// sort reads count again; this is neither distinct matches nor output rows.
    pub const fn source_matches(&self) -> u64 {
        self.source_matches
    }

    /// Distinct physical shards on which a record-read call actually ran.
    pub fn shards_read(&self) -> impl Iterator<Item = u16> + '_ {
        (0_u16..64).filter(|shard| self.shard_mask & (1_u64 << shard) != 0)
    }
}
