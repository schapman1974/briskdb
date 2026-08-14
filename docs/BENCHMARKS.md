# Benchmark baseline

BriskDB's Criterion suite establishes repeatable controls for the synchronous
storage path and the bounded asynchronous engine. It is a measurement tool, not
a claim about production capacity or a timing threshold for shared CI runners.

## Global-index before/after gate

Issue [#226](https://github.com/schapman1974/briskdb/issues/226) freezes the
protocol-neutral Engine baseline that every global-index phase must compare
against. The matrix covers 2, 4, 10, and 64 shards in both one-process and
four-process modes. Each case uses deterministic data and validates returned
rows, affected rows, shard targets, constraint outcomes, and every SQLite
file's `PRAGMA quick_check` before accepting timing data.

| Workload | Current routing | Purpose |
| --- | --- | --- |
| `point_read` | One shard | Preserve exact shard-key routing cost |
| `scatter_read` | Every shard | Measure bounded logical fan-out |
| `indexed_hit` / `indexed_miss` | Every shard, using a shard-local SQLite index | Freeze the cost that global index routing should remove |
| `insert` / `update` / `delete` | One shard | Quantify foreground write cost before index maintenance |
| `contended_unique_insert` | One authoritative shard today | Quantify unique-conflict and multi-process contention cost before global reservations |

Every result is a tab-separated record with attempts, successes, constraint
failures, returned rows, visited shards, throughput, p50/p95/p99 latency,
process CPU, peak RSS, operating-system-reported physical write bytes, peak WAL
growth, and SQLite durability mode. `physical_write_bytes` comes from
`getrusage`; a platform/filesystem may report zero. The harness does not invent
an fsync count when portable syscall accounting is unavailable. It records the
production `WAL` plus `synchronous=FULL` policy explicitly, while WAL growth
provides a portable storage-cost signal.

Run the parser/budget unit test and the same short correctness smoke used by CI:

```bash
cargo test --locked --test global_index_baseline \
  report_parser_and_regression_budgets_are_deterministic -- --exact
cargo test --locked --test global_index_baseline \
  global_index_baseline_smoke -- --ignored --exact --test-threads=1
```

One command runs the complete optimized matrix locally:

```bash
cargo test --release --locked --test global_index_baseline \
  release_global_index_baseline -- \
  --ignored --exact --nocapture --test-threads=1
```

Use a quiet machine, the same local filesystem, toolchain, power policy, and
warm-cache policy for before/after comparisons. Set `BRISKDB_BENCH_COMPARE` to
the committed TSV baseline to enforce the deliberately broad stable-host
budgets: at least 50% of baseline throughput; p99 no greater than the broader
of 3x baseline or 5 ms of host-scheduling jitter; at most 2x CPU, physical
writes, or WAL growth per attempt; and at most 64 MiB additional peak RSS.
Shared CI runs correctness smoke and synthetic budget tests, not cross-host
timing thresholds.

```bash
BRISKDB_BENCH_COMPARE=docs/benchmarks/global-index-before-2026-08-14.tsv \
  cargo test --release --locked --test global_index_baseline \
  release_global_index_baseline -- \
  --ignored --exact --nocapture --test-threads=1
```

The frozen pre-index artifact is
[`global-index-before-2026-08-14.tsv`](benchmarks/global-index-before-2026-08-14.tsv).
It records the exact engine revision, host, compiler, controls, and all 64
results. The local SQLite secondary index makes individual child lookups cheap,
but `indexed_hit` and `indexed_miss` still visit 2/4/10/64 shards respectively;
future gains therefore cannot be mistaken for cache-only improvements.

## Workload contract

Every benchmark creates a fresh temporary BriskDB database with exactly four
shards, applies the same untimed journaled schema migration to every shard, and
seeds one primary-key row per shard. Point updates keep the database size
constant, preventing ever-growing insert cost from distorting later samples.
Concurrent workloads deterministically find and verify one key for every
physical shard.

The original `storage/*` group is retained unchanged as the synchronous,
unpooled control. Timed operations use the public
`briskdb::storage::Database` interface.

| Benchmark | One timed iteration | Reported throughput |
| --- | --- | --- |
| `storage/point_read` | Route a fixed key and select its row by primary key | rows read per second |
| `storage/point_write` | Route a fixed key and increment one row by primary key | rows written per second |
| `storage/four_shard_concurrent_writes` | Release four threads together; each routes and updates one key on a different shard, then join them | total rows written per second; four per iteration |

The concurrent storage control intentionally includes thread creation, barrier
synchronization, and joins. Keeping that cost and the benchmark names stable
allows results to remain comparable with the initial issue #3 snapshot.

The `engine/*` group exercises the same logical operations through the public
asynchronous `Engine` and routed `Session` interface, using the default four
active connections and queue capacity of 32 per shard.

| Benchmark | One timed iteration | Reported throughput |
| --- | --- | --- |
| `engine/point_read` | Route through the engine and select one fixed primary-key row | rows read per second |
| `engine/point_write` | Route through the engine and increment one fixed primary-key row | rows written per second |
| `engine/four_shard_concurrent_writes` | Submit one routed update to each of four shards concurrently and await all four | total rows written per second; four per iteration |

Engine fixtures perform successful untimed preflight operations before
measurement. Criterion warm-up therefore establishes the lazily opened pooled
connections needed by each workload. Engine samples measure steady-state
connection reuse, not startup or first-checkout cost. Each key has one
long-lived routed `Session` established during fixture setup. This models a
connection-oriented frontend and lets write-bearing handles remain with their
owning session; it is not a model of separate ephemeral HTTP requests.

The `storage/*` measurements include:

- BLAKE3 routing;
- opening and configuring a SQLite connection for each operation;
- parameter and result conversion at the storage boundary;
- SQLite query/update work and filesystem I/O; and
- WAL mode, `synchronous=FULL`, foreign-key checks, and the configured busy
  timeout.

They exclude HTTP parsing, networking, Tokio scheduling, server startup,
database creation, schema migration, key discovery, and seed inserts.

The `engine/*` measurements include routing, session serialization, asynchronous
admission, blocking-worker dispatch, pooled connection checkout and return,
parameter/result conversion, and SQLite/filesystem work. They exclude HTTP and
networking, engine/database construction, schema and seed setup, key discovery,
session creation, routing-context setup, and pool warm-up. Both groups use warm
operating-system caches after Criterion's warm-up period; neither measures first
process access or a cold page cache. A deliberate change to either workload
contract must be documented before comparing it with an older result.

## Run and compare

First verify the workloads. Dedicated tests assert exact read results,
single-row write counts, distinct routing to all four shards, and final state
after concurrent writes for both paths:

```bash
cargo test --locked --test benchmark_workloads
cargo test --locked --bench storage
```

The second command runs Criterion's one-iteration test mode. It is also covered
by CI's `cargo test --locked --all-targets --all-features` command.

Collect both benchmark groups in the optimized benchmark profile:

```bash
cargo bench --locked --bench storage
```

Criterion results are written below `target/criterion/`, which is intentionally
untracked. To save and later compare a named local baseline on the same host:

```bash
cargo bench --locked --bench storage -- --save-baseline before-change
cargo bench --locked --bench storage -- --baseline before-change
```

Use a quiet machine, the same Rust toolchain and locked dependency graph, the
same power mode, and the same local filesystem. Virtualized CI storage,
antivirus/indexing activity, thermal throttling, and filesystem cache state can
change these numbers substantially. Compare distributions, not a single run,
and investigate correctness separately from performance. Record the exact
branch or commit and `EngineOptions` for engine comparisons; the canonical
warm-pool control uses four connections and 32 queued operations per shard.

## Experimental sharded virtual-table decision workload

The `experimental-vtab` feature includes a no-fork, read-only `brisk_shard`
candidate that must be compared with the established Rust scatter path before
any rollout decision. The comparison uses equivalent registered fixtures and
reports point lookup, full scan, `COUNT(*)`, and ordered limited-read results.
Point lookup binds an exact typed shard-key equality; the other workloads expose
the cost of reading through the virtual table before stock SQLite evaluates
non-pushed aggregation, ordering, or limits.

Run the virtual-table correctness tests and benchmark harness with the feature
enabled. Record the exact command, revision, shard count, fixture row count,
database size, filesystem, toolchain, and warm/cold-cache policy alongside any
measurements. At minimum, exercise 2-, 10-, and 64-shard fixtures. Compare the
same returned rows and duplicate semantics before comparing elapsed time or
throughput.

```bash
cargo test --locked --all-features storage::sharded_vtab
cargo test --release --locked --features experimental-vtab --lib \
  storage::sharded_vtab::benchmarks::release_benchmark_matrix_reports_issue_126_comparison \
  -- --ignored --exact --nocapture --test-threads=1
```

The issue #126 implementation was measured on 2026-08-12 at implementation
commit `7f0d598` (from branch `issue-126-read-only-vtab`, based on `f71f705`) on
the repository's Apple M1 Pro (10 cores, 16 GiB RAM), internal APFS volume,
macOS/Darwin 25.2.0, and Rust
1.94.1 `aarch64-apple-darwin`. Each fresh fixture contained 256 deterministic
rows per shard in both a hash-routed table and a native-ID table. The measured
facade used validated OS-level `SQLITE_OPEN_READ_ONLY` handles for both bootstrap
and child-shard access. The harness performed an untimed result/routing
preflight and then took one fixture-size snapshot before any timed sample. It
counts the logical file lengths reported by the filesystem for
`manifest.sqlite`, an optional `manifest.sqlite-wal`, all expected
`shards/NNNN.sqlite` files, and their optional `-wal` files. Missing WAL files
count as zero. Volatile `-shm` files, directory metadata, filesystem block
rounding, and unrelated temporary files are deliberately excluded.

For every workload and path, the harness performs three untimed warm-up
operations and an untimed calibration probe. Point probes start at 100
operations; scan probes start with enough operations to process about 100,000
logical rows. Calibration targets 500 ms and any measured sample shorter than
250 ms is repeated with more iterations. The reported result is the median by
throughput of five measured samples. Paired vtab/Engine samples alternate which
path runs first; coordinator-only samples have no comparison path to alternate.
Caches were warm; setup, schema migration, seeding, coordinator construction,
correctness preflight, warm-up, and calibration were not timed.

| Shards | Rows/shard/table | Manifest DB | Manifest WAL | Shard DBs | Shard WALs | Counted total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 256 | 118,784 B | 0 B | 90,112 B | 0 B | 208,896 B |
| 10 | 256 | 122,880 B | 0 B | 450,560 B | 0 B | 573,440 B |
| 64 | 256 | 122,880 B | 0 B | 2,883,584 B | 0 B | 3,006,464 B |

| Shards | Hash point: vtab | Hash point: Engine | vtab/Engine | Full scan: vtab | Full scan: Engine | vtab/Engine |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 4,957.00 ops/s | 37,967.69 ops/s | 0.131x | 1,130,050.19 rows/s | 3,184,983.99 rows/s | 0.355x |
| 10 | 5,096.96 ops/s | 39,902.11 ops/s | 0.128x | 1,202,843.36 rows/s | 2,050,456.15 rows/s | 0.587x |
| 64 | 4,754.41 ops/s | 32,304.24 ops/s | 0.147x | 1,162,523.00 rows/s | 2,061,907.79 rows/s | 0.564x |

The coordinator-only workloads measured as follows. `COUNT(*)` and ordered
`LIMIT` represent all logical input rows, even though they return one and 50
rows respectively. The current Engine scatter surface rejects those forms, so
the report does not invent a benchmark-only reducer for a false comparison.

| Shards | `COUNT(*)` input rows/s | `ORDER BY ... LIMIT 50` input rows/s | Native-ID point ops/s |
| ---: | ---: | ---: | ---: |
| 2 | 1,448,435.79 | 1,229,253.95 | 7,147.31 |
| 10 | 1,290,473.27 | 1,191,658.33 | 6,868.60 |
| 64 | 1,332,706.05 | 1,366,877.11 | 7,323.54 |

Exact harness records from the command above follow. The final test result was
`1 passed; 0 failed` in 56.24 seconds.

```text
record	shards	rows_per_shard	path	workload	samples	median_iterations	median_elapsed_ms	median_ops_per_sec	median_logical_rows_per_sec
fixture_record	shards	rows_per_shard	manifest_db_bytes	manifest_wal_bytes	shard_db_bytes	shard_wal_bytes	total_db_and_wal_bytes
issue126_fixture_bytes	2	256	118784	0	90112	0	208896
issue126_benchmark	2	256	vtab	hash_point	5	2221	448.053	4957.00	4957.00
issue126_benchmark	2	256	engine_logical	hash_point	5	15774	415.459	37967.69	37967.69
issue126_benchmark	2	256	vtab	hash_full	5	1036	469.388	2207.13	1130050.19
issue126_benchmark	2	256	engine_logical	hash_full	5	3264	524.702	6220.67	3184983.99
issue126_benchmark	2	256	vtab	count	5	1341	474.023	2828.98	1448435.79
issue126_benchmark	2	256	vtab	order_limit_50	5	1221	508.562	2400.89	1229253.95
issue126_benchmark	2	256	vtab	native_point	5	3662	512.361	7147.31	7147.31
issue126_comparison	2	256	hash_point	0.131	hash_full	0.355
issue126_fixture_bytes	10	256	122880	0	450560	0	573440
issue126_benchmark	10	256	vtab	hash_point	5	2589	507.950	5096.96	5096.96
issue126_benchmark	10	256	engine_logical	hash_point	5	21410	536.563	39902.11	39902.11
issue126_benchmark	10	256	vtab	hash_full	5	235	500.148	469.86	1202843.36
issue126_benchmark	10	256	engine_logical	hash_full	5	420	524.371	800.96	2050456.15
issue126_benchmark	10	256	vtab	count	5	225	446.348	504.09	1290473.27
issue126_benchmark	10	256	vtab	order_limit_50	5	236	506.991	465.49	1191658.33
issue126_benchmark	10	256	vtab	native_point	5	3235	470.984	6868.60	6868.60
issue126_comparison	10	256	hash_point	0.128	hash_full	0.587
issue126_fixture_bytes	64	256	122880	0	2883584	0	3006464
issue126_benchmark	64	256	vtab	hash_point	5	2455	516.363	4754.41	4754.41
issue126_benchmark	64	256	engine_logical	hash_point	5	17959	555.933	32304.24	32304.24
issue126_benchmark	64	256	vtab	hash_full	5	35	493.272	70.95	1162523.00
issue126_benchmark	64	256	engine_logical	hash_full	5	66	524.439	125.85	2061907.79
issue126_benchmark	64	256	vtab	count	5	42	516.339	81.34	1332706.05
issue126_benchmark	64	256	vtab	order_limit_50	5	44	527.404	83.43	1366877.11
issue126_benchmark	64	256	vtab	native_point	5	2894	395.164	7323.54	7323.54
issue126_comparison	64	256	hash_point	0.147	hash_full	0.564
```

These are one-host engineering measurements rather than CI thresholds or a
statistical capacity claim. They show that correctness and scale are viable,
but the connection-per-child, materializing facade is currently about 6.8-7.8x
slower for hash point reads and 1.7-2.8x slower for full scans than the pooled
Rust Engine path. The issue #126 decision is therefore to retain the facade as
an experimental complement and optimization foundation. It must not replace
the Engine/protocol query path at this stage. Streaming child cursors and pooled
read handles are the clearest measured follow-up opportunities.

## Hi/lo versus native generated-write workload

Issue #129 adds a separate ignored release harness for the two internal
generated-ID seams. It is a four-shard, one-row autocommit comparison, not a
wire-protocol benchmark. Each fresh fixture registers these exact empty table
shapes on every shard:

```sql
CREATE TABLE benchmark_generated_native (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    payload TEXT NOT NULL
) STRICT;

CREATE TABLE benchmark_generated_hilo (
    id INTEGER PRIMARY KEY,
    payload TEXT NOT NULL
) STRICT;
```

The matrix is frozen at exactly 2, 4, 8, and 10 concurrent writers. Each writer
owns one pre-opened writable coordinator on its own OS thread. A barrier releases
all writers together, after coordinator construction, and each performs 10,000
single-row inserts. The native workload uses its automatic active-owner
selection with a per-table round-robin start and exhaustion fallback. The hi/lo
workload consumes a globally leased ID and hash-routes the complete encoded
value. Five samples are taken per policy and writer count;
which policy runs first alternates by sample. The report uses the median by
total writes per second. A fresh fixture is used for each writer count, and
both physical table counts must equal the exact expected cumulative writes
before that comparison is reported.

Timing includes generated-ID consumption, route selection, virtual-table
callback and reconciliation, physical SQLite WAL work, `synchronous=FULL`, and
all 10,000 autocommit inserts per worker. For `hilo_v1`, it therefore includes
one immediate manifest reservation and semantic-root refresh per 4,096-value
block. It excludes database and table creation, registration/provisioning,
coordinator opening, and thread creation. Timing starts immediately before the
parent joins the start barrier, so it includes the final worker rendezvous and
barrier release.
The two policies intentionally retain their production allocation semantics:
native generation advances shard-local `sqlite_sequence`, whereas hi/lo makes
one central durable write per block and hash-distributes its encoded IDs.

Run the correctness smoke test first, then the optimized matrix on a quiet local
filesystem:

```bash
cargo test --locked --features experimental-vtab --lib \
  storage::sharded_vtab::benchmarks::generated_write_benchmark_smoke_covers_the_frozen_writer_matrix_and_both_policies \
  -- --exact

cargo test --release --locked --features experimental-vtab --lib \
  storage::sharded_vtab::benchmarks::release_benchmark_matrix_reports_issue_129_generated_write_comparison \
  -- --ignored --exact --nocapture --test-threads=1
```

The tab-separated output schema is:

```text
record  policy  shards  writers  writes_per_worker  samples  median_total_writes  median_elapsed_ms  median_writes_per_sec
comparison_record  shards  writers  hilo_over_native
```

The issue #129 matrix was measured on 2026-08-12 from branch
`agent/129-hilo-v1`, based on `e8a1a05`, with the exact release command above.
The host was an Apple M1 Pro with 10 cores and 16 GiB RAM, macOS 26.2/Darwin
25.2.0, Rust 1.94.1 (`aarch64-apple-darwin`), and Cargo 1.94.1. The repository
and temporary fixtures were on the internal solid-state APFS data volume. The
machine was on AC power with a charged battery; samples used the operating
system's normal warm cache and no cache flush. The complete test took 757.34
seconds and all post-run physical row counts matched.

| Writers | `native_range_v1` writes/s | `hilo_v1` writes/s | Hi/lo ÷ native |
| ---: | ---: | ---: | ---: |
| 2 | 1,911.16 | 2,059.28 | 1.078× |
| 4 | 2,566.69 | 3,021.51 | 1.177× |
| 8 | 3,683.81 | 3,782.75 | 1.027× |
| 10 | 3,490.22 | 3,614.71 | 1.036× |

On this host, hi/lo remained ahead at every tested concurrency, with the
largest measured gain at four writers. These values are one-host engineering
measurements, not capacity guarantees or CI thresholds.

The decision record must report measurements for both paths, including startup
where relevant, and explain whether the virtual-table boundary advances,
remains experimental, or is rejected. The feature remains off the authoritative
Engine and protocol paths until the separate rollout gate is approved.

## Issue #131 final rollout matrix

The final rollout harness is separate from the historical issue #126 and #129
measurements above. Those runs remain useful snapshots, but they used different
shard counts, sampling windows, and fixture contracts. The issue #131 harness
freezes one ten-shard comparison at exactly 2, 4, 8, and 10 concurrent clients
across five workload families:

| Workload | Facade path | Independent existing path |
| --- | --- | --- |
| Point read | read-only virtual-table coordinator | logical Engine router |
| Scatter read | read-only virtual-table coordinator | logical Engine scatter/gather |
| Explicit-key write | writable virtual-table coordinator | routed Engine write |
| `native_range_v1` omitted-key write | writable virtual-table coordinator | unavailable |
| `hilo_v1` omitted-key write | writable virtual-table coordinator | unavailable |

The two generated-write comparator cells are deliberately reported as
`unsupported`, with a fixed reason. Public Engine omitted-key writes delegate
to the writable virtual-table coordinator, so timing that call as an
"existing-router" control would compare the implementation with itself. The
report validator rejects fabricated trials for those cells and also rejects an
unexplained missing cell.

For every client-count/workload/trial tuple, the harness builds one closed
template and byte-copies it for each executable path. The report includes a
BLAKE3 digest over the relative file names and contents and refuses paired
results whose baseline digests differ. Both copies therefore start with the
same manifest, catalog, shard databases, schema, allocation-owner state, and
256 deterministic `benchmark_hash` rows per shard. Volatile `-shm` files are
excluded from the template. Paired paths bind the same SQL and values. Each
copy keeps production `WAL`, `synchronous=FULL`, and foreign-key behavior; no
benchmark-only durability relaxation is allowed.

The rest of the contract is also fixed:

- 100 untimed warm-up operations per client;
- three measured trials per executable cell;
- 10 seconds per trial, released through a common start barrier;
- one telemetry observation every 50 milliseconds plus baseline and final
  observations;
- four Engine connections and 32 queued operations per shard; and
- two Tokio runtime threads with at most ten blocking threads.

Timing includes operation execution, contention, the final worker rendezvous,
and telemetry overhead. Setup, template creation/copying, schema/catalog
validation, coordinator/session construction, and warm-up are excluded. Path
order alternates by trial. Both paths use the normal warm operating-system
cache policy; the harness does not claim cold-cache results.

Each trial records successful operations and classified busy, cancelled,
constraint, corruption, storage-full, and other errors. It also records:

- user-plus-system process CPU from `getrusage` and CPU as a percentage of wall
  time (which may exceed 100% on a multicore host);
- baseline and final current RSS from `ps`, plus the maximum of
  baseline/50 ms/final samples and its growth above that trial's baseline;
  compare the growth rather than absolute RSS because allocator/runtime pages
  may be retained between cases. Process-lifetime high-water RSS from
  `getrusage` is emitted only as a diagnostic, normalizing the macOS byte and
  Linux KiB conventions;
- baseline, final, and sampled peak bytes across the manifest and all shard WAL
  files. Peak file-size growth per successful operation is emitted only as a
  diagnostic: checkpoints and WAL reuse make file size non-monotonic, so it
  cannot prove bytes written or pass the resource gate;
- total and per-shard sampled Engine pool active/queued occupancy (zero for the
  direct facade path, which has no Engine pool); 50 ms samples are diagnostic
  and do not claim a true high-water mark; and
- per-shard successful touches plus minimum, maximum, mean, and maximum/mean
  skew.

Point and explicit-key workloads rotate the deterministic routed key by both
client and operation, so every trial exercises all ten shards even when only
two, four, or eight clients are active. Generated-ID placement remains the
production allocator's decision and its observed per-shard distribution is
reported rather than forced by the benchmark.

The 50 ms snapshots are deterministic accounting points, not a continuous
profiler, so a very brief occupancy spike can fall between samples. RSS is also
diagnostic because all trials share one process and may reuse allocations from
earlier cases. WAL file-size deltas are diagnostic because they are not a
monotonic counter of frames written. RSS, WAL, and sampled pool occupancy
therefore remain explicitly unresolved and keep the benchmark gate at `HOLD`.
CPU includes sampler work in both paired paths and is compared per successful
operation. Every successful point read or write must report one valid shard;
every scatter read must report all ten distinct shards. A mismatch is counted
as an error rather than silently inflating throughput.

Timed point reads compare the exact key, row number, and payload. Timed scatter
reads fully materialize the result and verify its row count; untimed warm-up
operations verify an order-independent content fingerprint against the
precomputed fixture fingerprint. This keeps full validation on both paths
without adding a large common hashing cost to the measured interval. After the
final telemetry sample, an untimed reconciliation opens the manifest and every
shard, runs `PRAGMA quick_check` and `pragma_foreign_key_check`, and compares
every acknowledged write key with the physical rows after subtracting tracked
warm-up keys. Generated IDs must also be globally unique, and native-range and
hi/lo IDs must decode and route to their physical shard. A failed
reconciliation rejects the trial before it can enter the report.

Run the fast matrix/report accounting test and the real two-path telemetry
smoke test first:

```bash
cargo test --locked --features experimental-vtab --lib \
  storage::sharded_vtab::benchmarks::rollout_benchmark_matrix_and_report_account_for_every_frozen_case_and_metric \
  -- --exact

cargo test --locked --features experimental-vtab --lib \
  storage::sharded_vtab::benchmarks::rollout_benchmark_smoke_executes_both_independent_read_paths_with_real_telemetry \
  -- --exact
```

The process sampler currently requires Unix `getrusage` and `ps`. Run the full
optimized matrix on a quiet machine with:

```bash
cargo test --release --locked --features experimental-vtab --lib \
  storage::sharded_vtab::benchmarks::release_benchmark_matrix_reports_issue_131_rollout_gate \
  -- --ignored --exact --nocapture --test-threads=1
```

The full command executes 96 ten-second trials and takes at least 16 minutes,
plus fixture and warm-up time. Its TSV output includes the frozen controls,
baseline digest, throughput, CPU, current/sampled-peak RSS, lifetime-peak RSS
diagnostic, WAL measurements, sampled pool occupancy, shard skew, error classes,
and telemetry sample count for every executed trial, plus eight typed
unsupported comparator records. It then emits median case rows, paired
throughput/CPU-per-operation ratios, diagnostic WAL file-size ratios, explicit
failure or unresolved reasons, and an overall benchmark `HOLD`/`PASS` row. Known snapshot
and live-protocol blockers are included so this benchmark-only summary cannot
claim full rollout. Attach the complete output and correctness results to the
decision record. Do not substitute historical issue #126/#129 numbers or turn
an unavailable comparator into a ratio.

## Issue #131 frozen rollout result (2026-08-12)

The full optimized command above completed in 1,057.76 seconds on an Apple M1
Pro (10 cores, 16 GiB), macOS 26.2, Rust/Cargo 1.94.1. This was an interactive
host with normal background services, not a dedicated benchmark runner. The
alternating path order and byte-identical paired fixtures remain important
controls for that reason.

All 96 timed trials completed. They reported zero operation errors, and every
post-trial manifest/shard quick check, foreign-key check, acknowledged-row
reconciliation, generated-ID uniqueness check, and placement check completed
without rejecting a trial. The report also contains the eight required typed
unavailable records. The complete 159-line artifact is
[issue-131-rollout-2026-08-12.tsv](benchmarks/issue-131-rollout-2026-08-12.tsv)
(SHA-256
`6fbdf6d0ccd7fa9f8d8f8fac8b4963b3e20787646fbd1432717cdea1b7a5cac3`).

The paired medians were:

| Clients | Workload | Facade ops/s | Engine ops/s | Throughput ratio | CPU/op ratio | Gate |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 2 | point read | 3,221.272 | 58,312.984 | 0.055 | 13.466 | fail |
| 2 | scatter read | 321.495 | 1,862.361 | 0.173 | 1.239 | fail |
| 2 | explicit write | 1,917.859 | 5,572.737 | 0.344 | 11.920 | fail |
| 4 | point read | 3,573.597 | 76,976.795 | 0.046 | 12.824 | fail |
| 4 | scatter read | 370.214 | 2,617.840 | 0.141 | 2.143 | fail |
| 4 | explicit write | 2,086.287 | 10,397.291 | 0.201 | 7.981 | fail |
| 8 | point read | 3,489.517 | 102,936.149 | 0.034 | 19.004 | fail |
| 8 | scatter read | 351.913 | 4,062.149 | 0.087 | 7.053 | fail |
| 8 | explicit write | 2,129.595 | 5,439.228 | 0.392 | 1.733 | fail |
| 10 | point read | 3,621.235 | 109,586.853 | 0.033 | 22.423 | fail |
| 10 | scatter read | 365.763 | 5,643.218 | 0.065 | 12.299 | fail |
| 10 | explicit write | 2,420.113 | 4,734.179 | 0.511 | 1.261 | fail |

Every throughput ratio is below the frozen 0.80 threshold. Every CPU ratio
except the two-client scatter result exceeds the 1.25 ceiling; the ten-client
explicit-write CPU ratio misses narrowly at 1.261. Sampled RSS, sampled pool
occupancy, and WAL file-size ratios remain diagnostics and cannot turn a case
into a pass.

The standalone generated-write medians were:

| Clients | Native ops/s | Native minimum/client | Native spread | Hi/lo ops/s | Hi/lo minimum/client | Hi/lo max/mean |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 1,931.623 | 9,590 | 2 | 2,065.439 | 9,846 | 1.031 |
| 4 | 2,016.159 | 5,010 | 4 | 2,627.435 | 6,080 | 1.032 |
| 8 | 2,095.238 | 2,524 | 8 | 2,559.076 | 3,098 | 1.031 |
| 10 | 2,308.715 | 2,216 | 10 | 2,724.337 | 2,674 | 1.029 |

No generated-write case met the prerequisite of at least 10,000 successful
writes by every client in every trial, so none may pass the placement/skew
gate. Native-range also observed spreads above its one-write bound. The
established path remains honestly unavailable for both generated-ID policies.

The issue #131 rollout decision is therefore **HOLD**. The facade remains
experimental and off by default. Performance misses alone are sufficient;
unresolved cross-shard snapshot semantics and missing live PostgreSQL/MySQL
conformance independently keep the gate closed.

## Initial snapshot

The initial issue #3 branch was measured on 2026-08-07 with `cargo bench
--locked --bench storage` and the suite's 2-second warm-up, 5-second measurement
window, flat sampling, and 20 samples per workload.

- Apple M1 Pro, 10 cores, 16 GiB RAM
- macOS/Darwin 25.2.0 on an internal APFS solid-state volume
- `aarch64-apple-darwin`, Rust 1.94.1, Cargo 1.94.1
- Criterion 0.7.0 and the repository's committed `Cargo.lock`

| Benchmark | Time per iteration | Throughput |
| --- | ---: | ---: |
| `storage/point_read` | 382.90 µs (369.62–397.56 µs) | 2.612 K rows/s |
| `storage/point_write` | 616.74 µs (604.04–629.88 µs) | 1.621 K rows/s |
| `storage/four_shard_concurrent_writes` | 1.5832 ms per four-write wave (1.5374–1.6352 ms) | 2.527 K rows/s total |

These values are a hardware-specific reference point. They must not be used as
cross-machine guarantees or CI pass/fail limits.

## Issue #10 pooled-engine comparison

The issue #10 implementation was measured against unpooled main commit
`1c0f9ab` on the same host and stable Rust 1.94.1 described above. The new
`engine/*` harness was copied byte-for-byte into the detached base worktree so
both revisions executed the same SQL, session setup, Tokio runtime strategy,
Criterion settings, dependency lockfile, and temporary-filesystem workload.
The pooled revision used default `EngineOptions` (four active connections and
32 queued operations per shard). Criterion saved the unpooled result as a named
baseline and performed its statistical comparison in the same target directory.

| Benchmark | Unpooled main | Default pooled engine | Median elapsed-time change |
| --- | ---: | ---: | ---: |
| `engine/point_read` | 386.00 µs; 2.591 K rows/s | 10.516 µs; 95.09 K rows/s | −97.28% |
| `engine/point_write` | 629.56 µs; 1.588 K rows/s | 43.860 µs; 22.80 K rows/s | −93.03% |
| `engine/four_shard_concurrent_writes` | 1.5817 ms; 2.529 K rows/s | 106.03 µs; 37.72 K rows/s | −93.30% |

Criterion classified all three changes as statistically significant
improvements (`p < 0.05`). These local results primarily quantify removal of
per-operation SQLite connection opening and configuration; they remain a
hardware-specific engineering comparison, not a production capacity promise.

## Issue #121 ephemeral HTTP write comparison

Issue #121 was measured on 2026-08-11 against base commit `f5ab846` and the
release build of the completed change. The host was the Apple M1 Pro described
above (10 cores, 16 GiB RAM), running macOS 26.2 with Rust and Cargo 1.94.1.

Each run used an independent APFS clone of the same imported LARGE_Data
database with ten shards. One persistent HTTP/1.1 client per worker repeatedly
toggled `work_order_items.is_highlight` on an existing primary-key row. Workers
were assigned distinct rows and explicit routing keys on distinct shards. The
server used one SQLite connection per shard, queue capacity 32, two asynchronous
Tokio worker threads, and a Tokio blocking-thread cap equal to the tested worker
count. Only 2, 4, 8, and 10 workers were tested. Every result is the median of
three 10-second trials after 100 untimed warm-up writes per active shard.

| HTTP writers | Base writes/s | Fixed writes/s | Speedup | Base p50 | Fixed p50 | Base CPU | Fixed CPU |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 776 | 13,352 | 17.20x | 2,545 µs | 137 µs | 152% | 108% |
| 4 | 704 | 12,664 | 17.99x | 5,716 µs | 299 µs | 295% | 147% |
| 8 | 634 | 11,194 | 17.65x | 12,465 µs | 677 µs | 673% | 145% |
| 10 | 690 | 11,229 | 16.27x | 14,256 µs | 837 µs | 808% | 143% |

All timed requests returned the expected shard and affected-row count; timed
errors were zero. Fixed-path p95 latency was 211, 454, 1,135, and 1,477 µs for
2, 4, 8, and 10 writers respectively. Peak resident memory remained below
12.9 MiB, process thread count never exceeded the requested blocking cap plus
the two runtime threads and main thread, and total observed WAL size remained
below 39.3 MiB.

The process monitor also recorded about 60 KiB of physical writes per completed
operation on the base path versus 20 KiB after the fix, a 66-67% reduction.
Together with the CPU and latency change, this supports the code and stack-sample
finding: repeated opening, schema validation, and closing of SQLite handles was
the dominant HTTP bottleneck, not a lock-selection race. The fix keeps clean
planner-validated write handles warm; it does not reroute a row or reuse a
handle while that handle is checked out.
