# Global-index validation and recovery

Global-index recovery is an offline Rust-library operation. Stop every BriskDB
service and embedded process using the data root, then open one exclusive
`Database` handle.

```rust
let report = database.validate_global_index(index_id)?;
if !report.is_valid() {
    // Unique indexes must always be rebuilt. Non-unique indexes may be repaired.
    database.rebuild_global_index(index_id)?;
}
```

Use `GlobalIndexValidationOptions::sampled(n)` for a fast, evenly distributed
check or `GlobalIndexValidationOptions::full()` for exact source-to-index
comparison. Reports contain stable issue codes, exact issue totals, bounded
detail, source shard IDs where known, and redacted BLAKE3 key/row fingerprints.
Both modes support `CancellationToken`.

## Fail-closed lifecycle

Before scanning, BriskDB durably fences the index as `Rebuilding`; queries must
not use it. A clean validation publishes `Ready` only when the requested mode
is strong enough for the prior lifecycle. A finding publishes `Invalid`.
Cancellation or a process exit leaves `Rebuilding`, never a partially visible
index. Reopen the root and retry a full validation, repair, or rebuild.

Validation detects missing physical storage/build/checkpoint state, incomplete
or definition-mismatched builds, missing/dangling/stale entries, bad shard
targets, incompatible key/locator encodings, duplicate authoritative keys, and
missing/dangling/mismatched unique reservations.
It also reports an interrupted `active_unique_reservation`. A rebuild rolls
that operation back before reconstructing unique authority; see [global
uniqueness and value authority](GLOBAL_INDEX_AUTHORITY.md).

## Repair versus rebuild

- `repair_global_index` is only for non-unique indexes. It atomically replaces
  every affected source shard, revalidates the full result, and publishes
  `Ready` only after the physical file is checkpointed and synchronized.
- `rebuild_global_index` is required for unique indexes. BriskDB never guesses
  which duplicate or conflicting reservation owns a key. Replacement work is
  resumable from complete shard checkpoints and remains fenced until verified.

All three operations require sole-process ownership. A live peer causes a
retryable `Busy` before lifecycle or physical state changes.

## Service status

`GET /v1/admin/global-indexes` returns a machine-readable catalog snapshot:

```json
{
  "indexes": [{
    "id": "1",
    "name": "users_email_unique",
    "unique": true,
    "lifecycle": "invalid",
    "available": false,
    "recovery": "rebuild"
  }]
}
```

Recovery execution remains an explicit offline library operation; the HTTP
endpoint reports availability and the required next action but does not mutate
storage.
