# Offline global-index construction

BriskDB builds the first physical global-index format through the Rust library:

```rust
let declaration = declaration
    .with_topology(GlobalIndexStorageTopology::selected_v1());
let index_id = database.create_global_index(declaration)?;
let report = database.build_global_index(index_id)?;
assert_eq!(report.shard_count(), database.shard_count());
```

`build_global_index_with_cancellation` accepts the same sticky
`CancellationToken` used by the rest of the core API. Cancellation never makes
partial data visible; call the builder again to resume.

## Maintenance-mode boundary

The initial builder is deliberately offline. It closes admission to new local
operations, drains admitted work, and upgrades the data-directory process lease
to sole-process ownership before reading a source row. If another BriskDB
process has the root open, the build returns retryable `Busy` without changing
physical index data.

Direct writes to shard SQLite files outside BriskDB are unsupported. Operators
must stop every service and embedded process using the root, open one exclusive
`Database` handle, run the build, and only then restart normal traffic.

## Physical format

`SharedSqliteV1` is stored at:

```text
<data-root>/global-indexes/global.sqlite
```

It is a separate, application-identified, storage-version-4 SQLite database in
WAL mode with `synchronous=FULL`. It contains fourteen BriskDB-owned tables:

| Table | Authority |
| --- | --- |
| `briskdb_global_index_storage` | Physical format and canonical-key codec versions |
| `briskdb_global_index_builds` | Definition digest, schema generation, state, and final row count |
| `briskdb_global_index_checkpoints` | One digest and row count for each completed source shard |
| `briskdb_global_index_entries` | Canonical key to source shard, scan ordinal, and typed physical row locator |
| `briskdb_global_index_read_repairs` | Bounded, idempotent non-unique stale-candidate tombstones |
| `briskdb_global_index_async_controls` | Pause and rebuild-required fences for non-unique maintenance |
| `briskdb_global_index_async_watermarks` | Per-shard applied cursors, poison state, counters, and batch timing |
| `briskdb_global_index_async_leases` | Expiring cross-process owners and monotonic fencing tokens |
| `briskdb_global_index_unique_keys` | One reservation per key when the definition is unique |
| `briskdb_global_operations` | Idempotent uniqueness/value operation state and request digest |
| `briskdb_global_unique_mutations` | Old/new owner intent for a unique operation |
| `briskdb_global_unique_reservations` | Active atomic locks on affected unique keys |
| `briskdb_global_value_sequences` | Per-index positive integer head and fence token |
| `briskdb_global_value_leases` | Irrevocable operation-bound value ranges |

After physical authority completion, the builder also publishes one
`briskdb_global_index_shard_summaries` row inside each source shard. These
restartable Bloom/min-max hints are not authority and are ignored unless their
format, definition digest, and state are ready; see
[shard-summary pruning](GLOBAL_INDEX_SHARD_SUMMARIES.md).

Storage versions 1 and 2 are upgraded atomically during sole-process startup. The
online state machines are documented in [global uniqueness and value
authority](GLOBAL_INDEX_AUTHORITY.md).

Rowid tables retain an unshadowed physical rowid locator. `WITHOUT ROWID`
tables retain the complete ordered primary key. The locator is a typed,
length-framed version-1 blob; it is not an application key and never changes a
source shard.

## Checkpoints and publication

Each source shard is scanned in ascending shard order through a read
transaction. Its entries, unique reservations, BLAKE3 source digest, and
checkpoint commit atomically in the physical index database. A unique
per-shard scan ordinal lets validation reproduce digest order without treating
locator bytes as an ordering codec. An interrupted
shard transaction rolls back and is retried. On restart, completed checkpoint
digests are recomputed from a stable, locator-ordered source scan:

- unchanged prefix shards are reused;
- any changed prefix restarts that index from shard zero; and
- missing, duplicate, or out-of-order checkpoints fail as corruption.

After all shards complete, BriskDB revalidates every source digest and exact
physical row count, marks the build complete, checkpoints the WAL, synchronizes
the file and directory, and only then commits the checksummed manifest
transition from `Creating` to `Ready`. The public lifecycle method cannot set
`Ready`; only the builder owns that publication boundary. A process exit before
the manifest commit leaves `Creating`, while an exit after it leaves `Ready`.

Removing a `Dropping` definition also removes its physical rows. A later build
opportunistically deletes records whose definition no longer exists.

## Keys, predicates, and uniqueness

Column and expression parts are evaluated by bundled SQLite on every source
shard. BriskDB converts the result to the declared logical type and then uses
the protocol-neutral canonical-key codec, including compound order, explicit
NULL placement, and binary collation. A partial-index predicate is evaluated by
SQLite before key conversion.

Unique builds reserve the canonical key across all shards. `NULLS DISTINCT`
does not reserve a key containing NULL; `NULLS NOT DISTINCT` does. A collision
returns `UniqueViolation` with both source shards and redacted locator labels,
while the catalog remains `Creating` for an exact retry after the data is fixed.

The repeated source-digest pass also rejects nondeterministic expressions or
predicates instead of publishing an index that cannot be reproduced.

## Validation and replacement

[Global-index recovery](GLOBAL_INDEX_RECOVERY.md) adds full or sampled
validation, bounded machine-readable findings, targeted non-unique repair, and
resumable replacement builds. Unique authority is never repaired by inference;
it must be rebuilt. All construction and recovery remain offline. Ready unique
indexes receive synchronous single-shard SQL write maintenance; indexed query
routing remains a later rollout stage.
