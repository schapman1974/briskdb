# Global-index production gate

Issue [#239](https://github.com/schapman1974/briskdb/issues/239) closes the
global-index rollout with operational visibility, stopped-backup evidence,
fault/soak tests, and an identical same-host before/after matrix. It is a
correctness gate for the alpha feature, not a claim that global indexes are
ready for latency-sensitive production use.

## Operator surfaces

- `GET /health` reports aggregate global-index state, unavailable/degraded
  counts, async lag, retained outbox bytes/events, and backpressured shards.
- `GET /v1/admin/global-indexes` reports per-index lifecycle, authority rows,
  unique keys, active operations/reservations/value leases, read repairs,
  async lag/failures/poison/leases, rebuild state, and Bloom/min-max summary
  health. It never returns indexed values or locators.
- `GET /metrics` exposes those counters in Prometheus text format. Per-index
  series use only the stable numeric `index_id` label, avoiding application
  values and unbounded index-name labels.
- `Database::global_index_operational_report()` and
  `Engine::global_index_operational_report()` expose the same redaction-safe
  state to embedded Rust hosts. HTTP health checks emit the aggregate fields as
  structured tracing data at `debug` level for journald or another subscriber.

`healthy` means the index is `Ready`, fresh, poison-free, and has usable shard
summaries. `degraded` remains correct but indicates lag, pause, rebuild need,
summary fallback/saturation, or outbox pressure. `unavailable` means the
lifecycle is not `Ready`. Failure to inspect validated storage returns the
normal classified engine error instead of an optimistic health result.

## Durability and recovery

`Engine::checkpoint()` now passively checkpoints every physical shard plus
`manifest.sqlite` and `global-indexes/global.sqlite` when present. Its ordered
report names each auxiliary database and includes busy/count/complete state.
A checkpoint reduces WAL recovery work; it is not an online snapshot or a
cross-file consistency boundary.

The supported alpha backup remains a complete directory copy after every
server and embedder has stopped. The restore test preserves and reopens:

- catalog, routing, schema, and all shard rows;
- unique and non-unique physical index entries;
- one pending outbox event plus its high-water/watermark lag;
- one live unique reservation and operation record;
- the global value sequence and completed lease history; and
- shard-summary state and all SQLite WAL sidecars that remain.

After restore, the test releases the reservation, proves the next leased range
does not reuse values, catches up the async index, rebuilds and fully validates
all indexes, and reads representative rows. Follow
[the stopped-server procedure](OFFLINE_BACKUP.md); live/partial copies and
online backup remain unsupported.

## Fault and soak evidence

The manual `Global-index release gate` GitHub workflow runs these release-mode
classes on Ubuntu 24.04:

| Risk | Automated evidence |
| --- | --- |
| Duplicate unique owner/value | authority reference model, process races, write-maintenance retries, value-range tests |
| False-negative indexed read | lag/scatter differential tests, candidate verification/repair, Bloom/min-max property tests |
| Process death | every authority, indexed-write, outbox, async apply/watermark, build/recovery, and topology commit boundary |
| Corruption/file drift | missing/changed authority, metadata/entry corruption, summary corruption/version mismatch, schema/file replacement checks |
| Disk full | real SQLite `SQLITE_FULL`, rollback, reopen, exact retry, generated-ID and row invariants |
| Clock movement | backward expiry conservatively retains the lease; forward takeover increments the fence; the stale owner cannot revive |
| Mixed sustained use | one in-process worker plus four concurrent processes repeatedly insert, replace indexed keys, read, delete, catch up, and verify exact authority counts |
| Backup/restore | stopped copy reopens all catalog, reservation, lease, outbox, watermark, summary, index, and row state |

Ordinary CI also runs a short single/multi-process after-index benchmark smoke.
The full workflow uploads the before/after TSV files and this operator report.

## Performance decision

The committed Apple M1 Pro reports use the same release binary, data,
filesystem, warm-cache policy, and 2/4/10/64-shard matrix:

- [before, current engine without global indexes](benchmarks/global-index-before-239-2026-08-15.tsv)
- [after, unique and non-unique global indexes enabled](benchmarks/global-index-after-2026-08-15.tsv)

Correctness counters were identical. Indexed hits and misses visited one shard
instead of 2/4/10/64; a known-empty read still compiles one shard to preserve
exact result metadata. Point and scatter reads remained inside the stable-host
budget.

The end-to-end latency result is a hold, not a speed claim. On the 64-shard
single-process cases, indexed-hit throughput changed from 1,683 to 22 ops/s and
p99 from 685 µs to 61.4 ms; indexed miss changed from 1,678 to 23 ops/s and p99
from 697 µs to 58.4 ms. The current planner repeatedly inspects durable
freshness and summary state, so metadata work outweighs the saved hot-cache
SQLite scans in this small fixture. Indexed writes also measured roughly
31–112x lower throughput across the selected 2/64-shard single/multi-process
samples.

Decision: global indexes remain explicit, experimental alpha functionality.
They pass correctness, recovery, shard-avoidance, and bounded regression
guardrails, but are not recommended yet for latency-sensitive production
workloads. Caching/batching freshness and summary inspection, then reducing
write-coordination transactions, is required before making a performance
recommendation. The broad alpha guardrails in the harness prevent silent
worsening; they are not performance targets.

### Hosted Linux physical-output accounting

The [first Ubuntu 24.04 workflow run](https://github.com/schapman1974/briskdb/actions/runs/34014711881)
exposed a platform gap in the original Apple M1 calibration. The harness
records the timed-operation delta of
`getrusage(RUSAGE_SELF).ru_oublock * 512`. The M1 reports returned zero for
every case, including mutations, while the hosted Linux result charged output
consistent with SQLite WAL shared-memory (`-shm`) initialization.

On Linux, a single-process indexed hit or miss produced exactly 32 KiB per
configured shard per attempt, from 64 KiB at two shards to 2 MiB at 64 shards,
with zero WAL growth. The current freshness snapshot opens and closes every
source shard for every non-unique indexed query. Indexed mutations produced at
most 730,016 bytes per attempt in that run. This is real connection and
shared-memory churn even though it does not represent the same durable row or
WAL growth measured by the separate WAL column.

The hosted alpha gate therefore uses explicit finite Linux guardrails for this
counter: 64 KiB per configured shard per indexed-read attempt and 1 MiB per
indexed-mutation attempt. Point and scatter reads retain the same relative
budget and gain no nonzero allowance; WAL growth retains the 4x indexed-read
and 16x indexed-write ratios. Exact-boundary tests fail on the first byte over
either physical-output limit. These ceilings record an explicit alpha release
decision rather than a production target. Issue
[#293](https://github.com/schapman1974/briskdb/issues/293) tracks removal of the
per-query shard connection and shared-memory churn. Workflow artifacts upload
even when enforcement fails so a rejected run retains both TSV reports.

## Run locally

```bash
cargo test --release --locked --test global_index_release_gate \
  global_index_release_soak -- \
  --ignored --exact --test-threads=1

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
