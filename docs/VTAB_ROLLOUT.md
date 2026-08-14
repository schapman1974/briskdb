# Virtual-table rollout gate

This document is the decision record for issue #131. It applies to the
no-fork `brisk_shard` read facade and its writable coordinator. It does not
change the contracts of the established Rust scatter path or the physical
SQLite files below either path.

## Decision

**Hold: the facade remains experimental and off by default.**

Compiling the `experimental-vtab` feature does not select the writable path.
The caller must also opt in with
`EngineOptions::with_experimental_vtab_writes(true)`,
`--experimental-vtab-writes`, or
`BRISKDB_EXPERIMENTAL_VTAB_WRITES=true`. The read coordinator remains an
internal comparison surface and is not the protocol query path.

The decision is a gate result, not a claim that the prototype is unusable. The
facade now has strong local correctness, recovery, and failure-classification
evidence. It does not yet meet the performance, snapshot, or supported-client
criteria required to replace the existing path.

## Frozen pass criteria

Timing thresholds are manual release gates, not shared-runner CI assertions.
All compared rows must use the same physical files, SQL, values, shard count,
WAL mode, `synchronous=FULL`, warm-up, trial duration, trial count, and cache
policy.

| Area | Required result |
| --- | --- |
| Row and ID correctness | Zero missing or duplicate acknowledged rows; zero duplicate generated IDs; every native ID decodes to its physical owner; every failed or cancelled statement leaves no partial row. |
| Recovery | After every tested forced-termination boundary, every database reports `PRAGMA integrity_check = 'ok'`, `pragma_foreign_key_check` is empty, acknowledged rows exist exactly once, uncommitted rows are absent, and a clean retry succeeds. |
| Errors | Timed normal workloads report zero errors. Busy, cancellation, constraint, corruption, and storage-full drills return their exact protocol-neutral class and never acknowledge an ambiguous write. |
| Throughput | At every 2/4/8/10-client point-read, scatter-read, and explicit-write comparison, facade median throughput is at least 80% of the established path. A missing honest comparator is reported as unavailable, never replaced by a benchmark-only proxy. |
| Resources | For identical compared workloads, facade CPU per successful operation is no more than 125% of the established path. Sampled WAL file-size growth remains unresolved until monotonic frame-level write accounting is available because checkpoints and WAL reuse can shrink or recycle the file. RSS remains unresolved until trials run in isolated processes, and pool maxima remain unresolved until continuous high-water accounting replaces 50 ms sampling; none of these diagnostics may pass the gate. |
| Placement | Native-range writes differ by at most one successful operation between shards. Per-client round-robin explicit writes have aggregate shard spread no greater than the client count. Hi/lo `max / mean` shard touches are at most 1.25 after every client completes at least 10,000 successful writes. |
| Read semantics | A documented cross-shard consistency policy is implemented. If one logical query promises a snapshot, every child participates in that same snapshot. |
| Protocols | The same supported and unsupported cases run through HTTP, PostgreSQL, and MySQL. Error code, recovery state, affected rows, generated key, and no-mutation behavior agree with the shared mapping. |

Generated native-range and hi/lo writes currently have no independent
pre-facade omitted-key Engine implementation. Their benchmark rows therefore
compare the two real allocation policies and record the missing established
path as unavailable. Calling the same coordinator once directly and once
through an Engine wrapper would not be an independent comparison.

## Correctness evidence

The all-feature suite contains the following durable evidence. Test names are
listed so the boundary can be reproduced without a special external harness.

| Concern | Representative automated coverage |
| --- | --- |
| Stock SQLite union and current-router differential | `scans_match_physical_union_all_at_two_ten_and_sixty_four_shards`; `normal_select_rows_and_storage_types_match_the_engine_scatter_path` |
| Property/state-machine sequences | `randomized_write_sequences_match_the_model_physical_union_and_facade` runs 24 bounded generated insert/update/delete/commit/rollback sequences and compares the model, physical SQLite union, placement, and facade |
| Concurrent native IDs | `writable_native_auto_generation_is_concurrent_unique_and_uses_every_wal`; `writable_native_generation_is_concurrent_and_unique_across_supported_shard_counts` |
| Competing-process hi/lo IDs | `competing_processes_insert_unique_hilo_ids_on_their_hash_routed_shards` |
| Cancellation | `writable_cancellation_interrupts_lock_wait_rolls_back_and_allows_reuse`; native and hi/lo cancellation tests |
| Busy/independent progress | `writable_distinct_shards_progress_while_same_shard_writer_is_locked`; Engine locked-write deadline coverage |
| Corruption | child operation, commit, and savepoint corruption tests verify terminal degradation and rollback |
| Restart | dropped-connection rollback, committed reopen, and persisted native sequence tests |
| Storage full | `writable_sqlite_full_rolls_back_and_exact_retry_preserves_invariants` caps the selected child at its current `max_page_count`, forces a real SQLite `SQLITE_FULL` during the statement, verifies rollback and every file, reopens, and retries once |
| Forced termination | `forced_termination_recovers_wal_without_lost_acknowledged_or_duplicate_generated_rows` kills a real child with one acknowledged autocommit and one open transaction, then verifies WAL recovery, all files, and a unique retry |

The deterministic page ceiling proves SQLite `SQLITE_FULL` classification and
statement rollback; it is not a substitute for later filesystem/VFS ENOSPC
campaigns or a COMMIT-time allocation failure. Similarly, the child-process
test exercises the public WAL transaction boundary, not every possible kernel
write or fsync instruction. Long-duration soak, a malformed-file corpus,
larger model-based campaigns, and broader filesystem fault injection remain
production-hardening work.

## Protocol evidence and blockers

With the write opt-in enabled, HTTP rejects transactions, savepoints,
attachments, and caller-authored DML `RETURNING` as `Unsupported` before pool
admission; tests also verify every shard remains unchanged. The PostgreSQL
listener now permits simple Query and parameterized text/binary extended
queries to enter the shared Engine lifecycle. Unsupported session statements
return to idle without reaching storage.

There is not yet a MySQL listener or command state machine. The shared mapping
already freezes `Unsupported` as MySQL error 1235 / SQLSTATE `42000`, but a
mapping function is not a live wire conformance test. PostgreSQL parameter/type
transactions remain tracked by #34. MySQL listener/query,
result/error, and transaction work remains tracked by #40-#44 and #47.

## Benchmark evidence

The reproducible commands, frozen matrix, telemetry schema, and raw historical
records are in [the benchmark document](BENCHMARKS.md). The earlier read
comparison measured facade/Engine ratios of about 0.13 for point reads and
0.36-0.59 for full scans. Both are below the 0.80 rollout threshold. The
generated-write comparison found both allocation policies viable at exactly
2/4/8/10 writers, but it did not create an independent non-facade generated-key
path.

Issue #131 adds a complete matrix contract for 2/4/8/10 clients, five workload
families, explicit availability, CPU, resident memory, sampled WAL size, pool
occupancy, shard touches/skew, and classified error counts. Sampled WAL size is
recorded but explicitly unresolved rather than interpreted as bytes written. A
report is invalid if a required case/trial is missing, duplicated, or silently
substitutes a different SQL path.

The frozen release run completed on 2026-08-12: all 96 timed trials reported
zero operation errors and passed their post-trial integrity and complete-row
reconciliation. Facade/Engine median throughput ratios were 0.033-0.055 for
point reads, 0.065-0.173 for scatter reads, and 0.201-0.511 for explicit writes,
all below the 0.80 threshold. All paired CPU/op ratios except two-client scatter
exceeded 1.25. No generated-write case reached the prerequisite of 10,000
successful writes for every client in every trial. The full medians,
environment, decision, and raw 159-line TSV are in
[the issue #131 benchmark result](BENCHMARKS.md#issue-131-frozen-rollout-result-2026-08-12).

## What keeps the gate closed

The current decision has three independent blockers:

1. measured point, scatter, and explicit-write performance misses the frozen
   relative thresholds, and generated placement lacks its minimum sample;
2. child connections do not provide one cross-shard read snapshot while
   writers commit; and
3. live PostgreSQL execution and MySQL conformance are not implemented.

Any one of these is sufficient to retain the opt-in. Reconsideration requires a
new complete report after the relevant implementation changes; it must not
reinterpret or delete a failing historical record.
