# Global-index shard summaries

BriskDB keeps one conservative Bloom/min-max summary per global index and
physical source shard. The summary is a routing hint, never authority: it may
retain an irrelevant shard, but it must never exclude a matching shard.

## What can be pruned

- Equality and non-negated `IN` on every supported column-only global-index key
  use the Bloom filter. Compound exact keys use the same canonical encoding as
  the global authority.
- `<`, `<=`, `>`, `>=`, and `BETWEEN` on supported single-column definitions use
  canonical typed minima and maxima. `AND` intersects ranges; `OR` uses a safe
  convex envelope.
- Shard-key inference runs first. Verified global-index candidate shards are
  protected, and summaries inspect only the remaining candidate or uncertain
  shards.
- Partial/expression definitions, unresolved values, unsupported shapes,
  collations, or NULL semantics retain the ordinary route.

`BoundStatementPlan::shard_summary_routing()` reports the selected index and
predicate family, examined shards, every excluded shard and proof, estimated
Bloom false-positive rate in parts per million, and observed pruning rate.

## No-false-negative maintenance

Each Bloom filter is 16 KiB with seven stable BLAKE3-derived bit positions.
Min/max values are complete canonical keys, so comparisons preserve the index's
declared type, order, NULL placement, and binary collation.

Coordinator writes add each new qualifying key in the same SQLite transaction
as the application row and non-unique outbox event. Rollback removes all three.
Deletes and old update keys are deliberately not subtracted: stale bits and
extrema cost selectivity but remain correct. At 95% bit occupancy the shard is
reported saturated and equality pruning is disabled; range proofs remain safe.
Legacy raw writes mark only their affected shard summary stale before execution.

## Rebuild and recovery

`Database::rebuild_global_index_shard_summaries()` rebuilds one shard at a time
with caller-owned cancellation available through the `_with_cancellation`
variant. A shard is marked `Building`, scanned in bounded memory, merged with
new keys committed during the scan, and atomically published `Ready`. Reads are
not blocked by the scan and never use `Building` state. A crash or cancellation
is restartable by calling rebuild again.

`Database::global_index_shard_summary_status()` reports per-shard state, Bloom
bytes/set bits, observed rows, additions, saturation, estimated false-positive
rate, and total memory. Missing, building, stale, saturated-for-equality,
corrupt, version-mismatched, or definition-mismatched state keeps that shard in
the plan. Exact reserved-table SQL is storage-fenced; incompatible rows degrade
planning conservatively instead of risking a false negative.

## Tests and measurement

`tests/global_index_shard_summaries.rs` covers equality, `IN`, ranges, NULL,
transaction rollback, inserts, updates, deletes, saturation, interrupted and
cancelled rebuilds, row corruption, format mismatch, and randomized comparison
with actual shard contents. Criterion's `global_index_shard_summaries` group
measures Bloom/range planning, shards avoided, status memory, and the existing
outbox comparison includes summary write maintenance.
