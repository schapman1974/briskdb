# Benchmark baseline

BriskDB's Criterion suite establishes repeatable controls for the synchronous
storage path and the bounded asynchronous engine. It is a measurement tool, not
a claim about production capacity or a timing threshold for shared CI runners.

## Workload contract

Every benchmark creates a fresh temporary BriskDB database with exactly four
shards, broadcasts the same table definition to each shard, and seeds one
primary-key row per shard. Point updates keep the database size constant,
preventing ever-growing insert cost from distorting later samples. Concurrent
workloads deterministically find and verify one key for every physical shard.

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
database creation, schema broadcast, key discovery, and seed inserts.

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
