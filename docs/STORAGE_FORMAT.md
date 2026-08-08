# Manifest storage format and migrations

`manifest.sqlite` is BriskDB-owned storage. It is not a user database and is
never exposed through HTTP, PostgreSQL, MySQL, or the SQL execution API. The
format is pre-1.0, but every format change must still have an ordered migration,
failure coverage, and an explicit compatibility decision.

## Current format: version 3

SQLite header fields identify the file and its format:

| Header field | Value | Meaning |
| --- | --- | --- |
| `PRAGMA application_id` | `0x42524442` (`BRDB`) | Permanent BriskDB manifest-family marker |
| `PRAGMA user_version` | `3` | Authoritative manifest schema version |

The application ID prevents an accidental foreign SQLite file from being
adopted as a manifest. It is not authentication or tamper protection: a process
that can write the data directory can forge it. Checksums and explicit degraded
states remain separate roadmap work.

Version 3 has five strict tables:

```sql
CREATE TABLE briskdb_manifest (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    shard_count INTEGER NOT NULL CHECK (shard_count BETWEEN 2 AND 64)
) STRICT;

CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 3)
) STRICT;

CREATE TABLE briskdb_routing (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    hash_version INTEGER NOT NULL CHECK (hash_version = 1),
    key_encoding_version INTEGER NOT NULL CHECK (key_encoding_version = 1),
    bucket_algorithm_version INTEGER NOT NULL CHECK (bucket_algorithm_version = 1),
    virtual_bucket_count INTEGER NOT NULL CHECK (virtual_bucket_count = 4096),
    map_generation INTEGER NOT NULL CHECK (map_generation = 1)
) STRICT;

CREATE TABLE briskdb_physical_shards (
    shard_id INTEGER PRIMARY KEY CHECK (shard_id BETWEEN 0 AND 63),
    lifecycle_state TEXT NOT NULL CHECK (lifecycle_state = 'active')
) STRICT;

CREATE TABLE briskdb_virtual_buckets (
    bucket_id INTEGER PRIMARY KEY CHECK (bucket_id BETWEEN 0 AND 4095),
    physical_shard_id INTEGER NOT NULL,
    FOREIGN KEY (physical_shard_id) REFERENCES briskdb_physical_shards (shard_id)
) STRICT;
```

Both singleton tables contain exactly one row. `briskdb_manifest.shard_count`
is immutable and is the initial routing modulus. In version 3 it is also the
live physical-shard count. Physical IDs are exactly `0..shard_count`; filenames
remain derived by trusted code as `shards/{shard_id:04}.sqlite` and are never
read from catalog-controlled paths. Version 3 supports only the `active`
lifecycle state. Adding resumable provisioning, draining, or retirement states
requires a later format and state-machine change.

The routing singleton contains exactly these generation-1 values:

| Field | Value | Contract |
| --- | --- | --- |
| `hash_version` | `1` | BLAKE3 of the canonical key bytes, using digest bytes `0..8` as an unsigned little-endian `u64` |
| `key_encoding_version` | `1` | Exact caller-supplied bytes; string shard keys contribute their UTF-8 bytes, with no Unicode normalization |
| `bucket_algorithm_version` | `1` | Compatibility-preserving range algorithm below |
| `virtual_bucket_count` | `4096` | Fixed virtual bucket space `0..4095` |
| `map_generation` | `1` | Initial committed bucket map and the only generation version 3 can interpret |

Every bucket ID exists exactly once and references an active physical shard.
Every physical shard owns at least one bucket. The generation-1 map partitions
the 4,096 bucket IDs into contiguous ranges whose sizes differ by at most one.
For initial shard count `N`, shard `s` owns the range beginning at
`s * base + min(s, extra)`, where `base = 4096 / N` and
`extra = 4096 % N`.

Bucket algorithm version 1 deliberately preserves legacy placement even when
`N` does not divide 4,096. Given the version-1 64-bit hash `H`:

```text
s      = H % N
base   = 4096 / N
extra  = 4096 % N
size   = base + (s < extra ? 1 : 0)
offset = s * base + min(s, extra)
bucket = offset + ((H / N) % size)
```

The generation-1 catalog maps that bucket back to `s`, so
`map[bucket(H)] == H % N` for every supported shard count. Runtime routing uses
the complete calculation and always obtains its final physical shard from
`map[bucket(H)]`; it does not recompute modulo as the final routing step. Golden
vectors freeze the exact key bytes, BLAKE3 prefix, little-endian hash integer,
bucket ID, and persisted physical shard.

`map_generation` is separate from manifest `user_version` and from the future
application-schema generation. Version 3 accepts only generation 1 and validates
its exact deterministic assignment; no public map-mutation operation exists yet.
A future format that can commit a changed map must bump `user_version` and its
downgrade fence as well as `map_generation`. That requirement makes this
pre-lookup binary reject the manifest instead of silently using legacy modulo
routing against a remapped catalog.

At each open, BriskDB validates the exact objects, columns, strict flags, frozen
schema SQL, singleton rows, supported algorithm values, contiguous physical and
bucket IDs, active lifecycle states, assignments, coverage, and foreign keys.
A recognized version-3 manifest that violates any of these invariants is
`DataCorruption` and is rejected before shard connections are opened. The same
locked transaction returns the validated routing rows as an immutable in-memory
snapshot, so request routing performs no manifest query and cannot fall back to
modulo after a failed validation.

## Previous version 2

Version 2 introduced the application ID, authoritative `user_version`, typed
shard count, and old-reader fence. Its exact two-table schema is:

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

The fence contains exactly `2`. A current opener validates this complete format
before transactionally constructing the version-3 catalog and replacing the
fence with its version-3 definition and row.

## Legacy version 1

The original format has `application_id = 0`, normally `user_version = 0`, and
this key/value table:

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
rows. A crash could therefore leave the exact empty table. BriskDB recognizes
only that empty shape as interrupted initialization and safely initializes it
using the requested shard count. One-row, extra-row, malformed, or
non-canonically encoded legacy states are `DataCorruption`.

## Upgrade and startup algorithm

Opening a `Database` or `Engine`, including server startup, may automatically
upgrade the manifest before any shard connection is opened:

1. Validate the requested shard-count range and open the manifest with a finite
   busy timeout, `synchronous=FULL`, and foreign keys enabled.
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

SQLite transactional DDL makes the schema, catalog rows, downgrade fence, and
header stamps one transaction within `manifest.sqlite`. A version-1 upgrade
commits version 2 before beginning version 3. Tests prove rollback and retry
after injected errors and Rust panic unwinding. SQLite is intended to recover an
uncommitted transaction after process loss, but BriskDB has not yet certified
process-kill, power-loss, or filesystem-fault recovery. Concurrent open calls
within the supported single-process deployment serialize on the immediate
transaction and re-read the version after acquiring it.

This guarantee is manifest-local. It is not an application-table migration API,
does not make broadcast atomic across shard files, and is not the planned
crash-resumable cross-shard schema migration journal. Catalog validation also
does not yet establish shard-file health: current startup can recreate a missing
shard file. No-create opening, per-shard identity/version, WAL, and schema-
generation checks belong to the later shard-validation milestone.

## Compatibility and operations

- Version 1 upgrades through version 2 to version 3; version 2 upgrades directly
  to version 3. Opening is therefore potentially mutating.
- There is no automatic downgrade. Version-1 code is fenced by the incompatible
  metadata table, and a version-2 reader rejects `user_version = 3`.
- A manifest with the BriskDB application ID and a `user_version` newer than the
  binary supports fails with non-retryable `FailedPrecondition`; it is not
  downgraded or switched to WAL, and shard files are not opened.
- A foreign nonzero application ID or unrelated unversioned SQLite database is
  a failed precondition. A recognized BriskDB identity whose schema, rows, or
  invariants disagree is `DataCorruption`.
- SQLite lock contention remains retryable `Busy`; permission, read-only, full,
  and I/O failures retain the storage error taxonomy.

Do not edit the header, tables, or rows manually. Until the checksum milestone,
a coordinated manual edit that still satisfies every structural invariant
cannot be distinguished from an intentional catalog update. The current
deployment boundary remains local storage and one BriskDB process per data
directory. A formal backup/restore workflow is not implemented; recovery to an
older binary requires a known-good copy made while BriskDB was stopped.

## Verification contract

Tests cover fresh and version-2 catalog creation for every supported shard
count, deterministic and balanced bucket ownership, non-divisor compatibility,
no-op reopen, both recognized legacy headers, the interrupted empty-table state,
exact schema and relational corruption, future and foreign formats, both old-
reader fences, errors and panics on both sides of the version stamp, multi-step
resume, observer atomicity, and concurrent openers with matching and conflicting
shard counts. Routing tests additionally freeze golden hash/bucket/shard vectors,
cover every algorithm state for every supported shard count, prove the final map
lookup with a synthetic reassignment, and exercise parallel and reopen stability.
Process termination and filesystem faults remain part of the later
storage-hardening suite. Every new migration must extend the registry,
destination validator, format documentation, and the same
normal/error/concurrency/recovery matrix.
