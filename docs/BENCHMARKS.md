# Benchmark baseline

BriskDB's Criterion suite establishes a repeatable performance baseline for
the current sharded storage prototype. It is a measurement tool, not a claim
about production capacity or a timing threshold for shared CI runners.

## Workload contract

Every benchmark creates a fresh temporary BriskDB database with exactly four
shards, broadcasts the same table definition to each shard, and seeds one
primary-key row per shard. Timed operations use the public
`briskdb::storage::Database` interface.

| Benchmark | One timed iteration | Reported throughput |
| --- | --- | --- |
| `storage/point_read` | Route a fixed key and select its row by primary key | rows read per second |
| `storage/point_write` | Route a fixed key and increment one row by primary key | rows written per second |
| `storage/four_shard_concurrent_writes` | Release four threads together; each routes and updates one key on a different shard, then join them | total rows written per second; four per iteration |

Point updates keep the database size constant, preventing ever-growing insert
cost from distorting later samples. The concurrent workload deterministically
finds and verifies one key for each physical shard. Its timing intentionally
includes thread creation, barrier synchronization, and joins because the
prototype does not yet have a persistent worker pool.

These are steady-state, warm-cache workloads after Criterion's warm-up period.
They do not measure first access after process start or a cold operating-system
page cache.

The measurements include:

- BLAKE3 routing;
- opening and configuring a SQLite connection for each operation;
- parameter and result conversion at the storage boundary;
- SQLite query/update work and filesystem I/O; and
- WAL mode, `synchronous=FULL`, foreign-key checks, and the configured busy
  timeout.

They exclude HTTP parsing, networking, Tokio scheduling, server startup,
database creation, schema broadcast, key discovery, and seed inserts. Future
pooling or engine work should keep these benchmark names stable or document a
deliberate workload-contract change.

## Run and compare

First verify the workloads. The dedicated tests assert exact read results,
single-row write counts, distinct routing to all four shards, and the final
state after concurrent writes:

```bash
cargo test --locked --test benchmark_workloads
cargo test --locked --bench storage
```

The second command runs Criterion's one-iteration test mode. It is also covered
by CI's `cargo test --locked --all-targets --all-features` command.

Collect the complete baseline in the optimized benchmark profile:

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
and investigate correctness separately from performance.

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
