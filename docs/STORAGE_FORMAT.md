# Manifest storage format and migrations

`manifest.sqlite` is BriskDB-owned storage. It is not a user database and is
never exposed through HTTP, PostgreSQL, MySQL, or the SQL execution API. The
format is pre-1.0, but every format change must still have an ordered migration,
failure coverage, and an explicit compatibility decision.

## Current format: version 2

SQLite header fields identify the file and its format:

| Header field | Value | Meaning |
| --- | --- | --- |
| `PRAGMA application_id` | `0x42524442` (`BRDB`) | Permanent BriskDB manifest-family marker |
| `PRAGMA user_version` | `2` | Authoritative manifest schema version |

The application ID prevents an accidental foreign SQLite file from being
adopted as a manifest. It is not authentication or tamper protection: a process
that can write the data directory can forge it. Checksums and explicit degraded
states remain separate roadmap work.

Version 2 has two strict tables:

```sql
CREATE TABLE briskdb_manifest (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    shard_count INTEGER NOT NULL CHECK (shard_count BETWEEN 2 AND 64)
) STRICT;

CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 2)
) STRICT;
```

`briskdb_manifest` contains exactly the singleton row. Its `shard_count` remains
immutable after creation. `briskdb_metadata` contains exactly the value `2` and
is an intentional downgrade fence, not a second version authority. The shipped
version-1 startup code expects `briskdb_metadata(key, value)`; retaining that
name with an incompatible shape makes an old binary fail instead of silently
opening a version-2 manifest. `user_version` is the sole canonical version.

Version 2 changes only `manifest.sqlite`. It does not alter shard contents,
shard filenames, BLAKE3 modulo routing, public Rust signatures, CLI options, or
any wire request or response.

## Legacy version 1

The original format has `application_id = 0`, `user_version = 0`, and this
key/value table:

```sql
CREATE TABLE briskdb_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
```

An initialized legacy manifest has exactly canonical `schema_version = "1"`
and `shard_count` rows. BriskDB also accepts `user_version = 1` with that exact
shape. No other unmarked SQLite schema is adopted.

The old initializer created the table before transactionally inserting its two
rows. A crash could therefore leave the exact empty table. Version 2 recognizes
only that empty shape as interrupted initialization and safely initializes it
using the requested shard count. One-row, extra-row, malformed, or
non-canonically encoded legacy states are `DataCorruption`.

## Upgrade and startup algorithm

Opening a `Database` or `Engine`, including server startup, may automatically
upgrade the manifest before any shard connection is opened:

1. Validate the requested shard-count range and open the manifest with a finite
   busy timeout plus `synchronous=FULL`.
2. Acquire `BEGIN IMMEDIATE`, then read identity, version, schema, and stored
   configuration under that write lock.
3. Reject a foreign file, a newer version, a requested shard-count mismatch, or
   an invalid recognized schema before changing it.
4. Apply one compile-time registered migration using static SQL and bound data.
5. Write and read back `application_id` and the destination `user_version` as
   the final mutations, validate the complete destination schema, and commit.
6. Re-lock and repeat if more than one numbered step is required. Each step has
   its own transaction, so restart begins at the last committed version.
7. After a compatible current manifest is committed, enable and verify WAL mode
   and only then create/open the shard layout.

SQLite transactional DDL makes the schema rewrite, copied configuration, and
header stamps one transaction within `manifest.sqlite`. Tests prove rollback
and retry after injected errors and Rust panic unwinding. SQLite is intended to
recover an uncommitted transaction after process loss, but BriskDB has not yet
certified process-kill, power-loss, or filesystem-fault recovery. Concurrent
open calls within the supported single-process deployment serialize on the
immediate transaction and re-read the version after acquiring it.

This guarantee is manifest-local. It is not an application-table migration API,
does not make broadcast atomic across shard files, and is not the planned
crash-resumable cross-shard schema migration journal.

## Compatibility and operations

- A version-1 manifest upgrades automatically to version 2 on first open by a
  current binary. Opening is therefore potentially mutating.
- There is no automatic downgrade. A pre-version-2 binary is intentionally
  fenced out after upgrade.
- A manifest with the BriskDB application ID and a `user_version` newer than the
  binary supports fails with non-retryable `FailedPrecondition`; it is not
  downgraded or switched to WAL, and shard files are not opened.
- A foreign nonzero application ID or unrelated unversioned SQLite database is
  a failed precondition. A recognized BriskDB identity whose schema, rows, or
  invariants disagree is `DataCorruption`.
- SQLite lock contention remains retryable `Busy`; permission, read-only, full,
  and I/O failures retain the storage error taxonomy.

Do not edit the header, tables, or rows manually. The current deployment
boundary remains local storage and one BriskDB process per data directory. A
formal backup/restore workflow is not implemented; recovery to an older binary
requires a known-good copy made while BriskDB was stopped.

## Verification contract

Tests cover fresh creation and no-op reopen, both recognized legacy headers,
the interrupted empty-table state, incompatible and future formats, canonical
metadata, old-reader fencing, errors and panics on both sides of the version
stamp, retry recovery, reader atomicity, and concurrent openers with matching
and conflicting shard counts. Process termination and filesystem faults remain
part of the later storage-hardening suite. Every new migration must extend the
registry, destination validator, format documentation, and the same
normal/error/concurrency/recovery matrix.
