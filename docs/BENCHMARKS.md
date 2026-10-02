# Benchmark baseline

BriskDB's Criterion suite establishes repeatable controls for the synchronous
storage path and the bounded asynchronous engine. It is a measurement tool, not
a claim about production capacity or a timing threshold for shared CI runners.

## Optional S3/Parquet overlay: Lambda/EFS, 2026-10-02

The separate `experimental-s3-overlay` API was measured with native ISAM catalog
metadata, indexed immutable SQLite bases directly on EFS, and Parquet pending
writes in S3. This is not the ordinary SQL/Mongo engine, not a local metadata
microbenchmark, and not a comparison against SQLite-metadata mode.

| Workload | Workers | Elapsed | Chapters/second | Failures |
| --- | ---: | ---: | ---: | ---: |
| 400 chapter pulls + 400 distinct inserts | 30 | 15.152478 s | 26.398322 | 0 |

Each pull returned and verified all 36 John 3 verses from the 31,103-verse WEB
fixture. Activity inserts used independent IDs, not repeated updates of Bible
records. Four shard directories received 97, 97, 100 and 106 inserts. All 400
inserts were visible before compaction; merging all 400 Parquet files preserved
all 420 activity rows (including 20 seeded rows) and unchanged chapter results.

Every invocation spawned/opened/closed/exited a fresh BriskDB process. The
denominator is the entire mixed workload wall time, including all process/ISAM
startup, network/SDK calls, SQL, publication retries and shutdown. No warmup
phase or subtracted startup. AWS reused 30 execution environments, so this is
not a claim of 800 cold microVM launches or an empty OS/EFS cache. Fixture
creation and final correctness/merge checks were outside the timed workload.
Lambda used ARM64, 2048 MiB, the existing NFSv4.1 EFS mount and private S3 gateway
endpoint. The original test function image/configuration was restored.

Raw local evidence is `efs-s3-overlay-42fdf384089b427e903e783a41dc7a8c.json` in
the existing external `nfs-lock-lab` workspace. It records source hashes, every
operation, configuration, errors and restoration. Measured native binary SHA256:
`f409901f504213c359da531987d9300062a7bd3f507f32b618ff1ef3d9632476`.
The benchmark container was private; no PyPI release was performed.

## Optional DuckDB reader: Lambda/EFS, 2026-10-02

The opt-in `experimental-duckdb-reader` uses DuckDB 1.5.6 to execute a SELECT
over a routed overlay partition. The control uses the existing SQLite query
engine. Both share the same ISAM catalog, immutable SQLite bases on EFS, and
BriskDB S3 publication/verified Parquet reader. This is not DuckDB reading
Parquet directly from S3, a DuckDB data-storage backend, or a SQLite-metadata
comparison. The normal database defaults do not change.

| Reader | Total elapsed | Chapters/second | Failed requests |
| --- | ---: | ---: | ---: |
| SQLite control | 15.379767 s | 26.008197 | 0 |
| DuckDB, 1 thread | 108.285190 s | 3.693949 | 0 |
| DuckDB, 2 threads | 110.152495 s | 3.631329 | 0 |
| DuckDB, 4 threads | 109.341794 s | 3.658254 | 0 |

These are actual individual runs, not averages or capacity guarantees. Increasing
DuckDB threads did not improve this chapter workload; SQLite remains the default.
All 3,200 timed requests succeeded. Each case verified all 420 activity rows
(20 seeded plus 400 inserted) before and after compaction and unchanged chapter
results. Inserts were distributed 97/97/100/106 across the four shard directories.

Each case uses a new 31,103-verse fixture, 400 complete John 3 pulls, 400
independent activity inserts and 30 concurrent callers. Lambda is ARM64,
2048 MiB, with two visible CPUs; four DuckDB threads tests twice the **visible**
CPU count, not twice the CPU quota. Every request opens/closes/exits a fresh
native BriskDB process. DuckDB is initialized and its bundled signed SQLite
extension loaded inside every DuckDB read. No database warmup is subtracted,
no handles are retained between requests, and the base is not copied locally.
AWS may reuse execution environments and filesystem caches. The denominator
is the full mixed-run wall time. Fixture setup and final correctness/merge
checks are outside the timer. Writes use the same code in every case.

The chapter table is unchanged during this workload; writes target independent
activity records. Correctness of pending updates, inserts, deletes, binary/text
values and compaction is tested separately against the SQLite reader, including
the built Python wheel with real S3. This benchmark does not establish read
performance over a large backlog of pending Parquet files.

The local query plan puts `FILTER` above `SQLITE_SCAN`, rather than an indexed
SQLite point lookup. The bases use `WITHOUT ROWID`; DuckDB's
[SQLite scanner](https://github.com/duckdb/sqlite_scanner/blob/5274128/src/sqlite_scanner.cpp)
limits scans without usable row IDs to one scan thread. A larger engine thread
setting therefore does not make this source scan parallel. Requested thread
counts are also checked against `current_setting('threads')` at runtime.

All cases use private image digest
`451489f411c51564d7422f2f3678483ec74e6ad931b88433d1bb13af8e79e1db`
and native executable SHA256
`56d3cbf64f38dd049c4b83c74f44d3999949e5172c830b775f2ed8ae3e8498fd`.
The official Linux ARM64 DuckDB library and signed SQLite extension are
bundled in that test container; they are not bundled in the Python wheel and
are never installed over the network inside a request. No PyPI release.

Raw evidence is in the external `nfs-lock-lab` workspace, as
`efs-s3-overlay-<run>.json`, with run IDs in table order:

- SQLite: `7a9189506530411fa1f6b3555feeed3b`
- DuckDB 1 thread: `1924ddf58e40424086c22a7bf2605e65`
- DuckDB 2 threads: `3124713449e745fd970fdeaf733c8304`
- DuckDB 4 threads: `81585a20c9234a32b88101385948dd8f`

Each artifact records every operation, source hashes, configuration, validation
and restoration of the original test Lambda image/configuration. The 2+2 smoke
test and an initial rejected image package are not throughput measurements.

## ISAM Parquet-file pruning: Lambda/EFS, 2026-10-02

The optional overlay's SQLite reader now skips pending files using per-partition
ISAM primary-key min/max and Bloom summaries. This comparison deliberately puts
**32 pending files in the queried verse partition**, not another table or a
partition already excluded by routing. Each file represents 36 verse rows:
one updates the requested John 3 chapter without changing its content, and 31
hold distinct synthetic chapter keys routed to that same partition (55).
The native write results verify this placement during fixture creation.

| Workload | SQLite before: no pruning | SQLite after: ISAM pruning | Before chapters/s | After chapters/s |
| --- | ---: | ---: | ---: | ---: |
| 400 reads + 400 distinct activity inserts | 22.060870 s | 15.907662 s | 18.131651 | 25.145115 |
| 400 reads, no writes | 16.794936 s | 8.951054 s | 23.816703 | 44.687474 |

Throughput improved by **38.68% mixed** and **87.63% read-only**. These are
individual measured runs, not averages or production-capacity guarantees.
Every chapter read returned all 36 expected verses; all 2,400 timed requests
succeeded. The mixed pair ran disabled then enabled; the read-only pair ran
enabled then disabled. No recurring workload, microVM warmup or test timer is
left running. Each case's original Lambda restoration was verified.
Pruning reduced its Parquet fetches from **32 to 1**. Across 400 chapter reads,
that is 12,800 versus 400 payload GETs and 106,457,200 versus 3,239,600 payload
bytes (96.96% fewer). The head GET remains, and the pruned reader additionally
opens one shared ISAM file-index snapshot. No directory/bucket listing is used.

Every case uses 30 callers, ARM64 Lambda at 2048 MiB, native ISAM metadata and
immutable indexed SQLite bases directly on EFS/NFSv4.1. Each request starts,
opens, operates, closes and exits a fresh native BriskDB process; all startup,
client/network time and shutdown remain in total workload wall time. There is
no warmup phase or subtraction, retained database, or whole-base copy to `/tmp`.
AWS may reuse microVMs and filesystem caches. Fixture creation and final
verification/compaction are outside the measured workload.

Mixed inserts use distinct activity IDs and distribute 97/97/100/106 across
four shard directories. All 420 activity rows were verified both before and
after compaction, with unchanged chapter results. The enabled case publishes
new activity-file summaries too, so its write-side indexing cost is included.
34 advisory index publications could not complete in the enabled mixed run;
those writes still committed and their files used the safe unindexed fallback.
All 400 chapter reads had complete usable summaries, with zero index fallbacks.
SQL-scan counters exclude separate publication/rebase/compaction I/O.

All four cases used the same native executable (SHA256
`b9919647e7005bdb68f7390b882c83aa73ba367cb9398444088fc948b3dd6002`)
and private image (`f9ced2914a5a318e448a77db0b8872a071075a0f64c2f2d6ba443637a8a0dca2`).
Pruning is controlled by the per-handle switch; each fixture starts with the
same indexed pending backlog. No PyPI release. The original Lambda image and
configuration are restored after each case.

Raw evidence is in the external `nfs-lock-lab`, as `efs-s3-overlay-<run>.json`:

- Mixed, disabled: `93a104471c3546e8a69d2cfd1e2b48b4`
- Mixed, enabled: `df6f160f580244cba4a11cb5ab4deeb9`
- Read-only, enabled: `156b71a029b243a6b18a90ba5fa8c2c1`
- Read-only, disabled: `a4ab10cd095d40a69bb9208a283f9f8b`

This is a SQLite-before/after optimization comparison, **not** SQLite metadata
versus ISAM metadata. The previous 26 chapters/s mixed and 41 chapters/s
read-only runs had no pending files in the chapter table; they are different
workloads and must not be used as the baseline for the pruning speedup.
Pruning currently covers typed primary-key BINARY equality, not arbitrary
non-key predicates, SQL range expressions or the optional DuckDB reader.

Validation also passed: 1,145 Rust library tests (3 ignored), 24 focused overlay
tests including tombstones/key moves, joins, corrupt/missing summaries,
coercions, failures and concurrent writers/compaction; 16 Python surface tests;
and a rebuilt source wheel's real-S3 CRUD/join/delete/compaction/reopen test,
including the pruning toggle and counters. The ordinary no-feature build
continues to compile. No remote CI or PyPI publication was started.

### Scale-up: 2,200 reads + 2,200 writes

The same SQLite/ISAM/S3 workload was repeated with 30 callers and 2,200 complete
chapter pulls plus 2,200 distinct activity inserts. The same 32 pending verse
files remain in the queried partition; activity files accumulate across 64
partitions. Both cases retain the earlier benchmark's 128-file compaction
threshold (not the ordinary overlay default of 32). Only the external harness's
400-operation guard was raised to 2,200; the native BriskDB executable is
byte-identical to the 400/400 test above.

| SQLite reader | Total elapsed | Chapters/second | Successful reads | Successful writes | Failures |
| --- | ---: | ---: | ---: | ---: | ---: |
| Pruning disabled | 150.819066 s | 14.587015 | 2,200 | 2,200 | 0 |
| ISAM pruning enabled | 80.822156 s | 27.220259 | 2,200 | 2,200 | 0 |

This pair measured **1.866x throughput** with pruning. Its 27.220259 chapters/s
compares with 25.145115 at 400/400; these are individual runs, not a claim that
larger workloads inherently run faster. Fresh native process/catalog startup,
client/SDK/network time, publication retries and process exit remain included.
There is no warmup subtraction. Both cases used 30 Lambda execution environments
and 4,400 distinct native processes. Fixture creation and final verification/
compaction remain outside the timed interval; AWS filesystem caches may persist.

Chapter payload GETs fell from 70,400 to 2,200, bytes from 585,514,600 to
17,817,800. All pruned chapter reads skipped 31 files and fetched one, with no
missing-index fallback. New inserts distributed 556/542/561/541 across shard
directories 0/1/2/3. Advisory index publication was unavailable for 153 enabled
case writes; those writes succeeded with unindexed fallback, not lost data.
Conditional publication retries totaled 310 disabled and 384 enabled; SQL itself
was never blindly replayed. The larger activity backlog is included, unlike a
fixed-size write-only metadata microbenchmark.

Both cases verified all 2,220 activity rows (20 seed + 2,200 new) before and
after compaction, plus unchanged complete chapter results. All 8,800 timed
requests across the pair succeeded. The original test Lambda image and
configuration were restored after each case; no benchmark was left running.

Raw evidence in the external `nfs-lock-lab`:

- Disabled: `efs-s3-overlay-392cc72b6db64d118a90fbb2e916a78f.json`
- Enabled: `efs-s3-overlay-e7a0dc4f0dfc49248f581a113586343d.json`

The private image is
`4d2c112ba47bec2bf1e7db85c98f888a1fb2e0abe8a367ea86f4ae04867b06d9`,
with unchanged native SHA256
`b9919647e7005bdb68f7390b882c83aa73ba367cb9398444088fc948b3dd6002`.
No PyPI publication or production configuration change was requested.

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
process CPU, peak RSS, operating-system-reported process block-output bytes,
peak WAL growth, and SQLite durability mode. The historical
`physical_write_bytes` column is `getrusage(RUSAGE_SELF).ru_oublock * 512`; it
is not an fsync or durable-device-write count. A platform/filesystem may report
the counter as unavailable (including zero for workloads that do write), while
Linux can charge transient or later-deleted SQLite WAL shared-memory pages.
The harness preserves the raw counter instead of inventing an fsync count. It
records the production `WAL` plus `synchronous=FULL` policy explicitly, while
WAL growth provides a separate storage-cost signal.

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

The first Ubuntu 24.04 release-gate run made this platform distinction
observable. Its indexed hit/miss path charged exactly 32 KiB per configured
shard per attempt while reporting zero WAL growth. That is consistent with
repeated SQLite `-shm` initialization as the freshness path opened and closed
every source shard. The hosted release comparator now applies explicit finite
alpha caps to that counter instead of a ratio whose baseline is zero: 64 KiB
per source shard for indexed reads and 1 MiB per indexed mutation. Point and
scatter reads keep their zero-baseline relative guard, and WAL retains its
existing ratios. See the
[global-index production gate](GLOBAL_INDEX_RELEASE_GATE.md) for the run
evidence and release decision; [#293](https://github.com/schapman1974/briskdb/issues/293)
tracks removal of the per-query shard connection churn.

Issue #239 adds an `after` mode that builds a non-unique lookup index and a
unique constraint index before running the identical matrix. It includes the
global authority WAL in storage telemetry, runs `quick_check` on that file,
requires every indexed hit and miss to visit exactly one shard, and writes a
complete report through `BRISKDB_BENCH_OUTPUT`. A miss still compiles one shard
to preserve exact result metadata.

```bash
BRISKDB_BENCH_OUTPUT=/tmp/global-index-before.tsv \
  cargo test --release --locked --test global_index_baseline \
  release_global_index_baseline -- \
  --ignored --exact --test-threads=1

BRISKDB_BENCH_COMPARE=/tmp/global-index-before.tsv \
BRISKDB_BENCH_OUTPUT=/tmp/global-index-after.tsv \
  cargo test --release --locked --test global_index_baseline \
  release_global_index_after -- \
  --ignored --exact --nocapture --test-threads=1
```

The committed same-host artifacts are the
[#239 before rerun](benchmarks/global-index-before-239-2026-08-15.tsv) and
the [after run](benchmarks/global-index-after-2026-08-15.tsv). They prove exact
result/constraint parity and 2/4/10/64-to-1 shard avoidance, but they do not
show an end-to-end speedup: durable freshness/summary inspection dominates
this small hot-cache fixture. Global indexes therefore remain an explicit
experimental alpha feature while that metadata path and write coordination are
optimized. Exact measurements, accepted guardrails, and the release decision
are in the [global-index production gate](GLOBAL_INDEX_RELEASE_GATE.md).

The Criterion `global_index_routing/*` group records the new authority-planning
cost for a rotating hit, a miss, a repeated hot key, and a four-key `IN`
lookup:

```bash
cargo bench --locked --bench storage -- global_index_routing
```

Each timed iteration includes bound predicate inference, canonical encoding,
one consistent authority snapshot, active-mutation coverage, shard
deduplication, and shard-key intersection. Database creation, index build, and
seed writes remain outside measurement.

The `global_index_outbox/*` group compares an identical registered-table key
update on the direct path, on the writable-coordinator control path with no
index, and with a ready non-unique index whose event is captured in the row
transaction:

```bash
cargo bench --locked --bench storage -- global_index_outbox
```

Criterion reports foreground latency and throughput. Run the release
`global_index_baseline` before/after matrix on the same host to compare its
physical-write and peak-WAL-growth columns; the outbox deliberately performs no
separate transaction or durability sync.

An illustrative 2026-08-15 Apple ARM64 release run measured the direct path at
51.5 µs / 19.4k updates/s, the writable-coordinator control at 798.6 µs / 1.25k
updates/s, and the transactional outbox at 1.092 ms / 916 updates/s. The focused
increment was therefore about 293 µs and 27% throughput versus the coordinator
control. These local numbers expose the current alpha cost; they are not a
cross-host CI threshold. The larger #239 release gate owns optimization and the
same-host p99/WAL regression decision.

The `global_index_async/*` group measures a one-event catch-up pass, foreground
write plus immediate apply, fully fresh miss planning, and a miss while one of
four shards is lagging:

```bash
cargo bench --locked --bench storage -- global_index_async
```

On the same Apple M1 Pro host, a 2026-08-15 release run measured one-event
catch-up at 3.99 ms / 251 events/s and write-plus-apply at 9.45 ms / 106 ops/s.
The apply is normally performed by the background worker and is not part of the
foreground row acknowledgement. A fresh miss plan measured 3.60 ms / 278
plans/s; the one-lagging-shard hybrid plan measured 3.63 ms / 276 plans/s, about
0.6% slower in this fixture. These are local medians, not release thresholds.

The `global_index_shard_summaries/*` group measures Bloom and min/max planning,
logical Bloom memory, estimated and observed false-positive rates, and shards
avoided. The existing transactional-outbox update is the before/after write-cost
control because summary additions commit in that same row transaction:

```bash
cargo bench --locked --bench storage -- global_index_shard_summaries
cargo bench --locked --bench storage -- global_index_outbox/
```

An Apple M1 Pro release run on 2026-08-15 produced:

| Measurement | Result |
| --- | ---: |
| Bloom lagged miss | 5.997 ms per plan; all 4 shards avoided |
| Typed min/max range | 2.356 ms per plan; 3 of 4 shards avoided |
| Summary status | 2.297 ms; 65,536 logical Bloom bytes across 4 shards |
| Estimated / observed Bloom FPR | 0 / 0 ppm in 64 absent-key probes (256 shard checks) |
| Indexed write before summaries | 1.092 ms; 916 updates/s |
| Indexed write with summaries | 1.152 ms; 868 updates/s |

The foreground summary increment was about 60 µs, or 5.5% elapsed time and
5.2% throughput in this tiny one-row fixture. The 16-KiB-per-index/shard Bloom
allocation is fixed; SQLite row/page overhead is not included in the logical
memory figure. False-positive rate grows with occupancy, is exposed per shard,
and disables equality pruning at 95% occupancy. These are engineering results
from one local filesystem, not capacity promises or CI thresholds.

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

## Experimental ISAM and SQLite comparison

Issue [#536](https://github.com/schapman1974/briskdb/issues/536) adds a
release-mode comparative harness for the opt-in original ISAM store, the
native catalog row/index API, the existing BriskDB SQLite backend, and a raw
fixed-record file control. It is
diagnostic, ignored by normal test runs, and does not create performance
thresholds or qualify NFS/EFS.

Run the bounded automated smoke check:

```bash
cargo test --locked --no-default-features \
  --features isam-benchmark \
  --test isam_benchmark bounded_comparison_smoke
```

The ordinary feature-enabled test verifies workload correctness, report schema,
disk-growth metadata, and per-writer rows without asserting machine-dependent
performance. To run the ignored release comparison with a short five-sample
manual smoke:

```bash
BRISKDB_ISAM_BENCH_SAMPLES=5 \
BRISKDB_ISAM_REVISION="$(git rev-parse HEAD)" \
cargo test --locked --no-default-features \
  --features isam-benchmark \
  --test isam_benchmark release_isam_sqlite_comparison -- \
  --ignored --exact --nocapture
```

For a candidate comparison, use a quiet host and release mode with the default
100 samples:

```bash
BRISKDB_ISAM_REVISION="$(git rev-parse HEAD)" \
BRISKDB_ISAM_BENCH_OUTPUT=target/isam-benchmark.tsv \
cargo test --release --locked --no-default-features \
  --features isam-benchmark \
  --test isam_benchmark release_isam_sqlite_comparison -- \
  --ignored --exact --nocapture
```

`BRISKDB_ISAM_BENCH_SAMPLES` changes the sample count within 2–100, the
fixed fixture's supported range. The TSV reports mean/low/high and p50/p95/p99 elapsed time,
workload-iteration throughput, ISAM logical page/root read/write counts, sync
calls, retained data/lock descriptor opens/closes, explicit file metadata
(`fstat`) calls, root/page I/O and sync timings, publication duration, lock
requests/retries/wait time, and process peak RSS. ISAM write-lock columns
separately report batch/key counts, OS byte-range lock attempts, retries, wait
time, and successful deduplicated stripe acquisitions. `write_lock_local_*`
separates in-process mutex retries/wait from `write_lock_range_*` OS
byte-range-lock retries/wait. The report's
`write_lock_stripe_acquisitions`, `write_lock_stripe_retries`, and
`write_lock_stripe_wait_ns` metadata rows show run-wide totals for each of the
64 stable hash stripes; stripe IDs identify lock buckets, not record keys.
Aggregate `lock_requests` / `lock_retries` continue to include file-level and
striped-lock activity. Schema v7 additionally records `preflight_rebases`
(discarded plans before I/O), `publication_retries` (post-sync root conflicts),
and `commit_lock_wait_ns` (also included in aggregate lock wait).
These counters are application-side measurements, not
NFS RPC counts or evidence of cross-host lock correctness. The revision field is
supplied explicitly so the artifact records the tested tree. Header metadata
records total byte growth across the main fixture directories during the run;
the separate explicit-batching fixtures are excluded, as marked in the report.
Growth is not attributed to an individual workload. Criterion is not required: samples and
machine-readable results are produced by this bounded harness.
`catalog_insert_1`, `catalog_update_1`, and `catalog_delete_1` additionally
compare one typed native catalog row operation, including maintained indexes,
against a single SQLite statement with a corresponding secondary index. Their
ISAM statistics include page/root I/O and commit sync duration. These new rows
are the direct performance signal for catalog writes; the existing `chunk_*_36`
cases remain lower-level store batch comparisons.
The `catalog_*_36` rows exercise the catalog's atomic bulk APIs with matching
36-row SQLite statements/upserts. Compare these to assess amortized durable
commit cost; they are not a substitute for measuring single-row transactions.
The `group_insert_36`, `group_update_36`, and `group_delete_36` rows compare
36 individual native commits (`isam_individual`) with one explicit native
bulk commit (`isam_bulk`) and one SQLite statement on separately seeded,
identical logical fixtures. Native commit counts are asserted: 36 roots/72
syncs versus 1 root/2 syncs. Backend order alternates by sample. Result and
index validation happens outside the timed interval. This is **caller-driven
batching**, not automatic group commit across independent requests; the atomic
boundaries deliberately differ. Aggregate disk-growth columns exclude these
separate group fixtures. Fixed fixtures support 2–100 samples per run.
The native catalog fixture also has a redundant unique `by_id` index; SQLite
uses its primary-key index plus the matching body index. These are API-level
comparisons, not identical physical index layouts.
`result_json_serialization_36` measures serialization of the same 36-row
`[{"id": ..., "body": ...}]` JSON row shape for ISAM and SQLite. Database reads
and conversion into that shared shape happen outside the timed interval; this
isolates JSON encoding cost and is not a complete HTTP response or transport
benchmark.

The CI workflow has a separate, opt-in `isam_benchmark` dispatch input. When
selected, it runs the 100-sample Linux release comparison and retains both the
TSV and full run log as a 90-day artifact, including unsuccessful runs. It does
not run on ordinary pushes or pull requests.
For four-writer waves, additional per-worker rows report each independent
writer's completion latency and successful sample count; a failed write aborts
the run rather than silently counting as progress.

The paired fixtures use the same 11-byte key and 128-byte payload, a single
logical routing key, and 36-row chapter-style ranges. BriskDB SQLite uses two
physical shards because the public database requires at least two; every row
and operation is routed to the same shard. Both database paths retain open
handles for warm operations. The measured workloads are open-existing,
point-read, range-36, atomic 36-row insert, 36-row refresh, 36-row delete,
same-key duplicate rejection, matched result JSON serialization for 36 rows,
and synchronized four-writer disjoint-key waves.
Writes use each backend's normal durable commit path; the SQLite fixture uses
BriskDB's existing local WAL/FULL policy.
These defaults do not provide an identical flush primitive on macOS:
[Rust's `File::sync_all`](https://doc.rust-lang.org/src/std/sys/fs/unix.rs.html)
uses `F_FULLFSYNC`, whereas SQLite's `synchronous=FULL` does not by itself
enable `fullfsync`. The native path is not weakened for the comparison, and
SQLite's default is not changed. Report these as **default-policy** results,
not a matched power-loss-durability comparison or NFS qualification.

The `NA` values in SQLite and flat-file logical-counter columns mean those
counters are unavailable, not zero. The raw flat-file control provides direct
fixed-offset point/range reads and
36-record overwrite+sync timings. It has no database lock, atomic batch, index,
or recovery semantics; treat it as a simple filesystem floor, not an
equivalent database competitor. SQLite logical I/O/RPC counts are unavailable
from this harness. ISAM counters are application call counts, not syscall,
filesystem metadata, or NFS RPC counts; collect actual NFS/EFS RPC telemetry
separately. The ISAM file-stat column counts explicit metadata calls made by
the storage code during open/validation; it does not include implicit kernel
work performed by file opens, reads, writes, or syncs. Peak RSS is
process-wide for the full harness, not per operation. The publication timer
includes root-lock admission, root write, and final sync, so it overlaps those
phase counters. The separate JSON measurement times only serialization of the
common row shape; it excludes database reads and value-to-row conversion. The
report schema is versioned because fields may be added; keep the matching
schema metadata with each archived TSV.
Local results on macOS/Linux do not predict shared-filesystem performance.

### Snapshot-local caching and opt-in packed pages (v5 / v6 experiments)

Performance experiment labels v5/v6 are **not** storage format versions.
The v5 experiment adds a per-snapshot decoded page cache (64 pages / 256 KiB
decoded storage, plus bookkeeping), including reuse between catalog validation
and write planning. Its files retain format v2. The v6 experiment additionally
supports new format-v3 files with variable-length leaf values and moves planned
entries into pages instead of cloning them. No compression is enabled.

Set `BRISKDB_ISAM_PACKED=1` for either benchmark to create v3 fixtures; omit it
for v2. Both versions still use the same key locks, commit gate, checksums, and
two syncs. Reports identify the chosen format. Benchmark verification remains
outside timings; `verify()` deliberately bypasses the snapshot cache.

The local 100-sample [v5 report](../benchmarks/results/isam-v5-macos-arm64.tsv)
reduced average page reads for a 36-row catalog update from v4's 770 to 85,
and p50 from 17.134 ms to 11.013 ms. Explicit bulk updates dropped from 614
to 64 page reads and from 15.157 ms to 10.813 ms. A one-pass 36-row range,
which has little reuse, moved from 42.000 to 43.917 microseconds. The
[v5 strict control](../benchmarks/results/isam-strict-v5-macos-arm64.tsv)
completed the 30-worker 400-read + 400-write burst in 3.823 seconds, still
with 800 syncs. These sequential local measurements do not establish sustained
read throughput, fairness, power-loss durability, or remote-filesystem behavior.

V5 was measured from the uncommitted `b1790aa` worktree with scoped diff hash
`5b94c113009fd0cbf7bcec82dea5f3c028ff0d4db97d93796e2875aea91385da`
(`git diff -- src/storage/isam tests/isam_benchmark.rs tests/isam_concurrency.rs`).

The 100-sample [v6 packed report](../benchmarks/results/isam-v6-macos-arm64.tsv)
adds denser pages without a codec. Local p50s, in milliseconds:

| Workload | v4 | v5 cache | v6 cache + packed |
| --- | ---: | ---: | ---: |
| 36-row range | 0.0420 | 0.0439 | 0.0418 |
| Catalog update, 36 rows | 17.134 | 11.013 | 10.910 |
| Catalog delete, 36 rows | 15.413 | 10.047 | 9.922 |
| Explicit bulk update, 36 rows | 15.157 | 10.813 | 10.066 |
| Explicit bulk delete, 36 rows | 15.067 | 10.566 | 9.952 |

Catalog-update page reads were 770 / 85 / 39, and page writes 40 / 40 / 19.
For the separately seeded explicit-bulk update they were 614 / 64 / 23 reads
and 30 / 30 / 10 writes. Catalog file **growth during the measured suite**
fell from 52,346,880 to 30,420,992 bytes (41.9%); this is not a compression
ratio or evidence of reclamation. The one-pass range is essentially unchanged,
and raw insert/update/delete timing did not consistently improve: fewer pages
do not eliminate durable flush time.

A [same-build v6 fixed-format control](../benchmarks/results/isam-v6-fixed-macos-arm64.tsv)
keeps v2 pages while retaining caching and the reduced-copy planner. It measured
42.333 microseconds for the 36-row range, 11.892 ms for catalog update, and
10.851 ms for explicit-bulk update. Its page counts and catalog file growth
match v5 exactly. This isolates the page-count/storage benefit of packing;
wall-clock differences remain subject to sequential-run and flush variance.

The [v6 strict control](../benchmarks/results/isam-strict-v6-macos-arm64.tsv)
completed 400 reads + 400 writes with 30 workers in 3.864 seconds (v5: 3.823;
SQLite strict in the v6 run: 2.663). Native writes still required 800 syncs;
there is no material independent-write speedup attributable to page packing.
The write-only case took 3.659 seconds with p99 1.652 seconds, so fairness
remains unqualified. The read-only 400-call burst took 0.00569 seconds, but a
millisecond-scale burst is not a sustained-read guardrail. Single-worker direct
SQLite reads remained faster (p50 6.291 microseconds vs native 60.166); the
BriskDB API benchmark and direct-engine control must not be conflated.

V6 was built from scoped diff hash
`fd7df919e8c08b6ab3ccc8d2e1e07d00e54a64ce366355c4064ac09c17b88243`
and strict-harness SHA-256
`5496f768d05ab7e8cb5c29fef69aa774096ac24235bd04b16ec07d270b6fff6b`.
Two additional tests were added afterward for dense empty-value pages and
parent-fence validation on cache hits; benchmarked production code is unchanged.
Final local validation passed 54 ISAM unit tests, four concurrency/compatibility
integration tests, both benchmark smokes, and all five release runs (v5 API and
strict, v6 packed API and strict, and v6 fixed API). Minimal embedded-feature
checking and scoped Clippy also passed, with four pre-existing dead-code
warnings. Final source/test scoped diff hash:
`e09a802e43cec3139f7694bbd55120035ec1624eb36e4a3d3f231b281590bfcf`.
No package release or shared-filesystem qualification was performed.

### Direct-engine durability control and v4 commit admission

`tests/isam_commit_benchmark.rs` preserves the default-policy/API benchmark
above and adds a separate comparison without BriskDB SQL-routing overhead.
Both engines store identical 11-byte BLOB keys and bounded BLOB values in one
primary tree, without secondary indexes. SQLite uses a `WITHOUT ROWID` table,
4096-byte pages, WAL, `synchronous=FULL`, `fullfsync=ON`, and
`checkpoint_fullfsync=ON`, with every setting read back on **each** retained
connection. ISAM retains its existing two `File::sync_all` calls per commit.
This matches the requested durability/flush policy without asserting identical
power-loss behavior or identical physical tree implementations. See SQLite's
[fullfsync documentation](https://www.sqlite.org/pragma.html#pragma_fullfsync).
Production SQLite settings are not modified.

```bash
BRISKDB_ISAM_REVISION="$(git rev-parse HEAD)" \
BRISKDB_ISAM_COMMIT_OUTPUT=target/isam-strict.tsv \
cargo test --release --locked --no-default-features \
  --features isam-benchmark --test isam_commit_benchmark \
  release_strict_commit_control -- --ignored --exact --nocapture
```

The bounded smoke uses two samples and 12 calls; the release run uses 100
single-worker samples and 400 calls per concurrent workload. Concurrent cases
have 4/10/30 retained handles/threads sharing one file. Read-only and write-only
cases have 400 calls; mixed cases have 400 reads **plus** 400 independent
single-record writes, divided among reader/writer workers. A read returns 36
records; each write changes its worker-owned value. Reported mixed throughput
uses the **whole workload completion time**, not just the reader completion
time: these finite bursts do not prove sustained read throughput under load.
Setup and result verification are excluded; checkpoint work incurred during
the operations remains included. SQLite uses its normal 1000-page automatic
checkpoint setting. SQLite sync counts are unavailable (`NA`; the initial v3
control used uninstrumented zeros). No filesystem, EFS or network RPC counts
are inferred from these application counters.

The v4 gate coordinates page writing/syncing before root publication, while
the first in-memory preparation and immutable-snapshot readers remain
concurrent. It must eliminate post-sync retries among participating writers,
not remove either durability sync. Unit tests cover stale-plan rebasing without
I/O, bounded busy returns without allocation, reader progress, older ungated
writer coexistence, and process-exit commit boundaries. Independent-process
integration tests assert exactly two syncs per successful commit. This is
neither automatic group commit nor cross-host qualification. Full recovery,
fairness, sustained workload and shared-filesystem gates remain open.

Local macOS ARM64 results (2026-10-01), in **whole-workload seconds**:

| Workload | Workers | v3 | v4 | SQLite strict, v4 run |
| --- | ---: | ---: | ---: | ---: |
| 400 independent writes | 4 | 8.5542 | 4.0605 | 2.3076 |
| 400 independent writes | 10 | 10.4387 | 4.0139 | 2.1686 |
| 400 independent writes | 30 | 13.5265 | 4.2374 | 2.9191 |
| 400 chapter reads + 400 writes | 4 | 5.5825 | 4.1616 | 2.0340 |
| 400 chapter reads + 400 writes | 10 | 8.4610 | 4.2200 | 2.2100 |
| 400 chapter reads + 400 writes | 30 | 11.5535 | 4.1076 | 2.9988 |

The [v3 control](../benchmarks/results/isam-strict-v3-macos-arm64.tsv) was run
before the gate; the [v4 control](../benchmarks/results/isam-strict-v4-macos-arm64.tsv)
uses the same workload. For 400 writes, native flush counts dropped from
1,802 / 2,325 / 3,025 to exactly 800 at 4 / 10 / 30 workers. A
[second v4 run](../benchmarks/results/isam-strict-v4-verification-macos-arm64.tsv)
finished those write-only cases in 3.7565 / 3.7907 / 3.9759 seconds, again with
800 syncs. The repeated 30-worker mixed case took 3.6255 seconds. These are
sequential local runs, not interleaved A/B trials or serverless measurements.

Read performance is **not fully qualified** by these results. The original
[100-sample v4 suite](../benchmarks/results/isam-v4-macos-arm64.tsv) retained
a 36-row read p50 of 0.0000420 seconds (v3: 0.0000422). However, the short
400-read-only bursts are noisy: at four workers, the repeated v4 elapsed time
was 0.0111 seconds versus the baseline's 0.00626 seconds. Consequently the
proposed <10% read-throughput-loss gate is **not established**. Longer sustained
and mixed-load trials are required. High-contention write tails also remain:
30-worker write-only p99 was 2.88 seconds, and 2.00 seconds on the repeat.
Single-worker writes are not consistently faster; sync cost remains dominant.

The original API/default-policy four-writer wave improved from v3's p50
0.06510 to 0.03993 seconds, using eight instead of thirteen average syncs and
eight instead of nineteen average page writes. This restores performance near
the historical v1 p50 of 0.03786 seconds; it is not a claim to beat that older
baseline. Its SQLite/default-policy p50 was 0.00394 seconds. Do not confuse
these API/default-policy timings with the strict direct-engine table above.

The tested native code remains an uncommitted `b1790aa` worktree. Its scoped
source/test diff SHA-256 is
`91e97d3af73fbf176bc7464d38a9091e74960693d929ea1146cfc58c4bc9fe93`
(`git diff -- src/storage/isam tests/isam_benchmark.rs tests/isam_concurrency.rs`).
The first strict-run harness SHA-256 is
`bb7afe102b564940e15398ad6fcc06d5107932ef9c8590d7fa8137d208da5be7`;
later harness cleanups affect only report argument grouping and untimed final
verification. These artifacts are not published releases.

### Local v3 explicit-batching checkpoint (2026-10-01)

The [v3 report](../benchmarks/results/isam-v3-macos-arm64.tsv) records 100
samples on macOS ARM64 with Rust 1.94.1. The tested source is the local
`b1790aa` handoff plus the uncommitted diff fingerprint in the report header;
this is not a published revision. The
[inherited v2 report](../benchmarks/results/isam-v2-macos-arm64.tsv) and
[pre-optimization handoff baseline](../benchmarks/results/isam-handoff-baseline-macos-arm64.tsv)
are retained separately. Benchmark-generation labels v1/v2/v3 are not storage
format versions.

Same-run p50 **seconds for 36 typed rows**, with correctness checked after
every operation:

| Operation | Native individual commits | Native explicit bulk commit | SQLite statement |
| --- | ---: | ---: | ---: |
| Insert | 0.341229 | 0.009826 | 0.001296 |
| Update | 0.337058 | 0.014953 | 0.001771 |
| Delete | 0.335870 | 0.014776 | 0.001695 |

Bulk calls use 1 root publication / 2 syncs instead of 36 / 72. In the
existing catalog-update workload, skipping unchanged index entries reduced
physical mutation keys from 216 to 144 per 36-row batch; unchanged indexes
remain validated. These are explicit API batches, not automatic grouping of
independent requests. Native bulk writes still trail SQLite with the default
policies and physical-index differences described above.

The unchanged lower-level same-file four-writer workload remains around
0.0651 seconds per wave (SQLite 0.00398). The native 36-record read remains
around 0.0000422 seconds (SQLite 0.000620). This checkpoint therefore does
**not** resolve independent-writer performance, recovery qualification, or
NFS/EFS acceptance. Full CI and those issue gates remain open.

The first fully instrumented optimized run was recorded against commit
`27e99ba` (Rust and Cargo 1.94.1, Darwin 25.6.0 ARM64, 100 samples, warm local
temporary directories). The complete v5 TSV is
[isam-27e99ba-macos-arm64.tsv](../benchmarks/results/isam-27e99ba-macos-arm64.tsv).
The earlier v1 artifact from the initial smoke revision remains available at
[isam-fc0f9fe-macos-arm64.tsv](../benchmarks/results/isam-fc0f9fe-macos-arm64.tsv).
Selected latency percentiles, in microseconds:

| Workload | ISAM p50 / p95 | SQLite p50 / p95 |
| --- | ---: | ---: |
| Open existing | 68 / 118 | 27,745 / 31,212 |
| Point read | 31 / 35 | 658 / 1,107 |
| Range of 36 | 40 / 48 | 660 / 1,106 |
| Insert 36 | 8,093 / 10,974 | 1,062 / 1,686 |
| Refresh 36 | 8,839 / 13,531 | 672 / 1,395 |
| Delete 36 | 7,915 / 11,183 | 1,014 / 1,684 |
| Same-key conflict | 34 / 71 | 665 / 1,193 |
| Four disjoint writers | 35,172 / 41,030 | 3,220 / 5,869 |

The four-writer ISAM wave averaged 56 lock requests, 44 retries, and 55.0 ms
of accumulated lock-wait time across its four store handles, exposing the
current same-file writer-serialization bottleneck. A single 36-row read used
one lock request and three logical page reads. Per-writer p50 completion
latencies were 24.2, 25.2, 27.1, and 9.0 ms for ISAM (100 successes per
writer); consult the TSV for the full distributions. Whole-run directory
growth was 7,991,296 bytes for ISAM, 196,608 bytes for SQLite, and 10,008
bytes for the fixed-file control. These totals combine all workloads and
include each backend's differing storage/reclamation behavior.

The fixed-file control is faster for reads and refreshes, but offers no
transaction or locking guarantees. This is a baseline, not a release gate or a
claim that ISAM is a performance win: small-chunk writes and same-file
contention currently lose to BriskDB SQLite. Prospective pass/fail budgets
remain unset until workload priorities and target storage are agreed. Result
conversion and full response/transport serialization are not separately timed,
and SQLite logical I/O/RPC/phase counters are unavailable. Do not treat these
local results as NFS/EFS evidence.

### Opt-in v4 pipelined commits (local, 2026-10-01)

This is **on-disk format v4**, not the earlier benchmark iteration named v4.
The new `tests/isam_pipeline_benchmark.rs` compares new v3 packed and v4
pipelined catalogs on the same local macOS ARM64 filesystem. Each fixture has
100,000 typed records, a primary key, a unique secondary ID index, and a
nonunique body index. It runs 400 indexed reads of 36 rows and 400 changing
single-row updates across 15 reader and 15 writer threads, with retained
handles/warm OS cache. Three fresh-fixture trials alternate execution order.
All rows are checked after seeding; reopened primary/index ownership, removed
old index entries, and the complete live tree are checked after each burst.

Medians across those three trials (latency rows are medians of each trial's
reported statistic):

| Measurement | v3 packed | v4 pipeline + same-process flush sharing |
| --- | ---: | ---: |
| Entire 400-read + 400-write burst | 4.422 s | 2.802 s |
| Mixed writer mean latency | 130.536 ms | 86.364 ms |
| Mixed writer p50 latency | 30.023 ms | 45.122 ms |
| Mixed writer p99 latency | 1387.116 ms | 518.090 ms |
| Mixed 36-row read p50 | 0.371 ms | 0.471 ms |
| Read-only 36-row p50 | 0.188 ms | 0.188 ms |
| Physical flushes for 400 updates | 800 | 561 |
| Required durability barriers for 400 updates | 800 | 800 |

The mixed burst took about 36.6% less wall time (57.8% more aggregate
throughput). This is **not** a universal latency improvement: mixed reader
latency and median writer latency regressed, while mean/tail writer latency
improved. The unchanged read path still rechecks/hashes pages between batches.
There is no read-cache optimization or compression in this change.

Pipelining alone, before same-process flush sharing, measured 3.893 s versus
4.361 s in a separate three-trial experiment: only about 10.7% less wall time.
Most of the additional gain above comes from combining queued flush requests
within one process. It does not combine independent Lambda clients' flushes.
Separate-process correctness tests pass, but neither these timing results nor
the process-exit tests establish multi-host NFS/EFS performance, power-loss
recovery, lease safety, or production readiness. Working-state coordination
also adds root I/O; metadata/RPC effects must be measured on the target filesystem.

Reproduce the local timing experiment without enabling cloud tests:

```bash
cargo test --release --locked --no-default-features --features experimental-isam \
  --test isam_pipeline_benchmark -- --ignored --nocapture --test-threads=1
```

Optionally set `BRISK_ISAM_PIPELINE_REPORT` to a **new** TSV output path; the
harness refuses to overwrite an existing file. This opt-in format does not
modify existing files or change SQLite/SQL/Mongo/Python defaults.

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
