# Global-index storage topology

## Decision

BriskDB's first physical global-index layout will use
`SharedSqliteV1`: one WAL-mode SQLite database containing all global-index
entries. Its partition count is exactly one and every canonical key routes to
partition `0`.

This is a deliberately reversible first-release choice. The catalog already
represents `HashPartitionedSqliteV1`, so a later measured migration can rebuild
an index into a different topology without reinterpreting key bytes.

## Why shared storage won

Issue #229 compared one shared SQLite file with 16 hash-partitioned files using
the same canonical keys, rows, FULL durability, disabled WAL autocheckpoint,
warmup, workloads, concurrency, hardware, and three-trial median selection.
Both prototypes passed the same reference-model, process-abort/reopen, and
four-process correctness tests.

The [raw 2/4/10/64-shard results](benchmarks/global-index-topology-2026-08-15.tsv)
showed:

- Single-process lookup throughput was 2–5% higher with the shared file.
- Partitioned multi-process lookup throughput was 3–34% higher, but absolute
  p99 remained 8–10 microseconds for partitioned and 10–14 microseconds for
  shared in this small prototype.
- Partitioned distinct-write throughput was inconsistent: it won at 4 shards
  but lost at 2, 10, and 64 shards. Its p99 reached 1.7–1.8 milliseconds in all
  four multi-process cases, versus 120–291 microseconds for shared storage.
- Partitioning reduced distinct-write WAL growth by roughly 22–41%.
- Opening and checking 16 files took 3.7–13.6 times longer and required exactly
  16 times as many SQLite connections per worker. Empty/small storage also
  occupied up to 16 times as many bytes.
- A hot key still maps to one writer file, so partitioning cannot remove that
  fundamental contention point. Measured hot-key throughput varied by run and
  did not justify the permanent file and recovery cost.

For the initial release, the shared layout is the smaller and more predictable
authority for uniqueness and allocation. It also minimizes backup, recovery,
file-descriptor, and corruption surface while those workflows are being built.

## Frozen routing rules

`GlobalIndexStorageTopology::selected_v1()` returns `SharedSqliteV1`.
`partition_count()` returns `1`, and `partition_for_key()` returns `0` for every
valid index ID and canonical key.

The retained 16-file comparison/migration topology is also deterministic:

```text
BLAKE3("briskdb.global-index.partition.v1\0"
       || index_id_as_little_endian_u64
       || canonical_index_key_bytes)
partition = little_endian_u64(digest[0..8]) & 15
```

Version 1 permits only a power-of-two partition count. The comparison count is
frozen as `HASH_PARTITIONED_GLOBAL_INDEX_PARTITIONS_V1 = 16`; it is not the
initial production default.

## Expected limit and migration path

One shared SQLite database has one concurrent writer. Readers can continue in
WAL mode, but sustained independent global-index mutations may eventually make
the writer the bottleneck. Release benchmarks must keep measuring throughput,
tail latency, busy time, WAL growth, and recovery cost before that limit is
claimed or changed.

A future topology change must use the existing `Rebuilding` lifecycle: build a
complete target, validate it, atomically publish the new topology metadata, and
only then remove the old files. It must never edit an existing topology in
place. Online dual-writing is not implied; the first supported build remains
offline/maintenance-mode work in issue #230.

## Prototype boundary and reproduction

The two minimal stores live only in `tests/global_index_topology.rs`; they are
not linked into the service, Rust library, or Python wheel. They exercise exact
lookup, insert, replace, delete, recovery, and multi-process workloads. Issue
#230 will implement and validate the selected production file format.

Run the stable correctness smoke with:

```bash
cargo test --locked --test global_index_topology \
  global_index_topology_smoke -- --ignored --exact --nocapture --test-threads=1
```

Run the full comparison with:

```bash
cargo test --release --locked --all-features --test global_index_topology \
  release_global_index_topology_benchmark -- \
  --ignored --exact --nocapture --test-threads=1
```
