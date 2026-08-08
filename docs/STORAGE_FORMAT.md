# Manifest storage format and migrations

`manifest.sqlite` is BriskDB-owned storage. It is not a user database and is
never exposed through HTTP, PostgreSQL, MySQL, or the SQL execution API. The
format is pre-1.0, but every format change must still have an ordered migration,
failure coverage, and an explicit compatibility decision.

## Current format: version 4

SQLite header fields identify the file and its format:

| Header field | Value | Meaning |
| --- | --- | --- |
| `PRAGMA application_id` | `0x42524442` (`BRDB`) | Permanent BriskDB manifest-family marker |
| `PRAGMA user_version` | `4` | Authoritative manifest schema version |

The application ID prevents an accidental foreign SQLite file from being
adopted as a manifest. It is not authentication or tamper protection: a process
that can write the data directory can forge it. Checksums and explicit degraded
states remain separate roadmap work.

Version 4 has eight strict tables. The routing tables are unchanged from
version 3; the downgrade fence and three logical-schema tables are new.

```sql
CREATE TABLE briskdb_manifest (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    shard_count INTEGER NOT NULL CHECK (shard_count BETWEEN 2 AND 64)
) STRICT;

CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 4)
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

CREATE TABLE briskdb_logical_databases (
    database_id INTEGER PRIMARY KEY CHECK (database_id > 0),
    database_name TEXT NOT NULL COLLATE BINARY UNIQUE
        CHECK (
            length(database_name) BETWEEN 1 AND 63
            AND instr(database_name, char(0)) = 0
            AND database_name NOT GLOB '*[^a-z0-9_]*'
            AND substr(database_name, 1, 1) GLOB '[a-z_]'
            AND database_name <> 'briskdb'
            AND database_name NOT GLOB 'briskdb_*'
            AND database_name NOT GLOB 'sqlite_*'
        )
) STRICT;

CREATE TABLE briskdb_schema_catalog (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    identifier_encoding_version INTEGER NOT NULL CHECK (identifier_encoding_version = 1),
    schema_generation INTEGER NOT NULL CHECK (schema_generation = 0),
    default_database_id INTEGER NOT NULL CHECK (default_database_id = 1),
    FOREIGN KEY (default_database_id)
        REFERENCES briskdb_logical_databases (database_id)
) STRICT;

CREATE TABLE briskdb_tables (
    table_id INTEGER PRIMARY KEY CHECK (table_id > 0),
    database_id INTEGER NOT NULL,
    table_name TEXT NOT NULL COLLATE BINARY
        CHECK (
            length(table_name) BETWEEN 1 AND 63
            AND instr(table_name, char(0)) = 0
            AND table_name NOT GLOB '*[^a-z0-9_]*'
            AND substr(table_name, 1, 1) GLOB '[a-z_]'
            AND table_name <> 'briskdb'
            AND table_name NOT GLOB 'briskdb_*'
            AND table_name NOT GLOB 'sqlite_*'
        ),
    placement INTEGER NOT NULL CHECK (placement IN (1, 2, 3)),
    shard_key_column TEXT COLLATE BINARY
        CHECK (
            shard_key_column IS NULL OR (
                length(shard_key_column) BETWEEN 1 AND 63
                AND instr(shard_key_column, char(0)) = 0
                AND shard_key_column NOT GLOB '*[^a-z0-9_]*'
                AND substr(shard_key_column, 1, 1) GLOB '[a-z_]'
                AND shard_key_column <> 'briskdb'
                AND shard_key_column NOT GLOB 'briskdb_*'
                AND shard_key_column NOT GLOB 'sqlite_*'
            )
        ),
    shard_key_type INTEGER
        CHECK (shard_key_type IS NULL OR shard_key_type IN (1, 2, 3)),
    UNIQUE (database_id, table_name),
    FOREIGN KEY (database_id)
        REFERENCES briskdb_logical_databases (database_id)
        ON DELETE RESTRICT,
    CHECK (
        (
            placement = 1
            AND shard_key_column IS NOT NULL
            AND shard_key_type IS NOT NULL
        )
        OR
        (
            placement IN (2, 3)
            AND shard_key_column IS NULL
            AND shard_key_type IS NULL
        )
    )
) STRICT;
```

The manifest, metadata, routing, and schema-catalog tables each contain exactly
one row. The v4 downgrade-fence row is exactly `4`.
`briskdb_manifest.shard_count` is immutable and is the initial routing modulus;
it is also the live physical-shard count. Physical IDs are exactly
`0..shard_count - 1`. Filenames remain derived by trusted code as
`shards/{shard_id:04}.sqlite` and are never read from catalog-controlled paths.
Version 4 supports only the `active` lifecycle state. Adding resumable
provisioning, draining, or retirement states requires a later format and
state-machine change.

### Logical catalog

Every fresh or upgraded v4 manifest contains logical database ID `1` named
`default`. The schema-catalog singleton contains identifier encoding version
`1`, application-schema generation `0`, and default database ID `1`. Version 4
can interpret only schema generation 0. It permits at most 64 logical databases
and 4,096 table rows. Database and table IDs are positive; table names are
unique within their owning database, and every table references an existing
database.

Identifier encoding version 1 is a canonical lowercase ASCII contract for
logical-database names, table names, and shard-key column names:

- the encoded name is 1 to 63 bytes;
- the first byte is `a` through `z` or `_`;
- every later byte is `a` through `z`, `0` through `9`, or `_`; and
- `briskdb`, every `briskdb_*` name, and every `sqlite_*` name are reserved.

Names use binary comparison. There is no case folding, quoting transform, or
Unicode normalization; a caller must supply the canonical lowercase name.

Table placement and shard-key type use stable numeric codes:

| Column | Code | Rust meaning | Stored metadata |
| --- | --- | --- | --- |
| `placement` | `1` | `Sharded` | One non-null shard-key column and type are required |
| `placement` | `2` | `Global` | Shard-key column and type must both be null |
| `placement` | `3` | `Catalog` | Shard-key column and type must both be null |
| `shard_key_type` | `1` | `Int64` | Signed 64-bit integer |
| `shard_key_type` | `2` | `Text` | UTF-8 text without Unicode normalization |
| `shard_key_type` | `3` | `Binary` | Arbitrary bytes |

`Sharded` means the same logical schema is expected on every shard and rows
are key-routed. `Global` describes small replicated lookup data. `Catalog`
describes manifest-owned metadata rather than a user shard table.

The loaded `Catalog` is immutable and read-only. It is advisory in version 4:
there is no catalog mutation API, planner integration, or enforcement against
physical shard schemas. Fresh initialization and every v1/v2/v3 upgrade leave
`briskdb_tables` empty. Migration does not inspect, infer, or adopt tables that
already exist in shard files. Such tables remain usable through the existing
explicit-key execute/query API and broadcast API; absence from the catalog does
not mean physical absence. Version 4 therefore changes no current SQL
execution, routing result, broadcast behavior, shard file, or wire contract.

### Routing catalog

The routing singleton contains exactly these generation-1 values:

| Field | Value | Contract |
| --- | --- | --- |
| `hash_version` | `1` | BLAKE3 of the canonical key bytes, using digest bytes `0..8` as an unsigned little-endian `u64` |
| `key_encoding_version` | `1` | Exact caller-supplied bytes; string shard keys contribute their UTF-8 bytes, with no Unicode normalization |
| `bucket_algorithm_version` | `1` | Compatibility-preserving range algorithm below |
| `virtual_bucket_count` | `4096` | Fixed virtual bucket space `0..4095` |
| `map_generation` | `1` | Initial committed bucket map and the only generation version 4 can interpret |

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

`map_generation` is separate from manifest `user_version` and from
`schema_generation`. Version 4 accepts only routing generation 1 and validates
its exact deterministic assignment; no public map-mutation operation exists
yet. A future format that can commit a changed map must bump `user_version` and
its downgrade fence as well as `map_generation`. That requirement makes this
pre-lookup binary reject the manifest instead of silently using legacy modulo
routing against a remapped catalog.

At each open, BriskDB validates the exact objects, columns, strict flags, frozen
schema SQL, singleton rows, logical identifiers and limits, metadata codes,
supported algorithm values, contiguous physical and bucket IDs, active
lifecycle states, assignments, coverage, and foreign keys. A recognized
version-4 manifest that violates any invariant is `DataCorruption` and is
rejected before shard connections are opened. The same locked transaction
returns routing and logical rows as one immutable in-memory snapshot, so
request routing performs no manifest query and cannot fall back to modulo after
a failed validation.

## Previous version 3

Version 3 uses the same manifest, routing, physical-shard, and virtual-bucket
table definitions shown above. It has no logical-database, schema-catalog, or
table-metadata tables. Its fifth table is this exact downgrade fence:

```sql
CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 3)
) STRICT;
```

The fence contains exactly `3`, and `PRAGMA user_version` is `3`. Version 3
supports only routing generation 1 and validates the same deterministic bucket
map. A current opener validates the complete v3 source before applying the
atomic v3-to-v4 migration.

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
before applying the numbered v2-to-v3 and v3-to-v4 transactions.

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

Fresh initialization creates the complete v4 schema and initial rows in one
transaction before stamping version 4. For an existing manifest, SQLite
transactional DDL makes each numbered step's schema, catalog rows, downgrade
fence, and header stamps one transaction within `manifest.sqlite`. A version-1
upgrade commits version 2, then version 3, then version 4; a version-2 upgrade
commits version 3 before beginning version 4.

The v3-to-v4 step atomically replaces the version-3 fence with the version-4
fence, creates the three logical-catalog tables, inserts database ID 1 named
`default` plus the schema-generation-0 singleton, and leaves the table catalog
empty. The destination is fully validated and the header is stamped last before
commit. An error or panic rolls the whole step back to the exact v3 source, so a
later open can retry it. The step never opens or inspects shard files and never
infers or adopts an existing physical table.

Tests prove rollback and retry after injected errors and Rust panic unwinding.
SQLite is intended to recover an uncommitted transaction after process loss,
but BriskDB has not yet certified process-kill, power-loss, or filesystem-fault
recovery. Concurrent open calls within the supported single-process deployment
serialize on the immediate transaction and re-read the version after acquiring
it.

This guarantee is manifest-local. The schema generation and table rows are
read-only advisory metadata, not an application-table migration API. They do
not make broadcast atomic across shard files and are not the planned
crash-resumable cross-shard schema migration journal. Catalog validation also
does not yet establish shard-file health: current startup can recreate a
missing shard file. No-create opening, per-shard identity/version, WAL, and
schema-generation checks belong to the later shard-validation milestone.

## Compatibility and operations

- Version 1 upgrades through versions 2 and 3 to version 4; version 2 upgrades
  through version 3; version 3 upgrades directly to version 4. Opening is
  therefore potentially mutating.
- There is no automatic downgrade. Older readers are fenced by the incompatible
  metadata schema and authoritative header; in particular, a version-3 reader
  rejects `user_version = 4`.
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

Tests cover fresh and v1/v2/v3-to-v4 catalog creation, the exact default logical
database and schema singleton, every placement and shard-key type code,
identifier and relational corruption, catalog row limits, immutable public
lookups, deterministic and balanced bucket ownership, non-divisor
compatibility, and no-op reopen. Migration coverage includes both recognized
legacy headers, the interrupted empty-table state, future and foreign formats,
old-reader fences, errors and panics on both sides of every version stamp,
multi-step resume, v3-to-v4 observer atomicity, and concurrent openers with
matching and conflicting shard counts. Routing tests additionally freeze golden
hash/bucket/shard vectors, cover every algorithm state for every supported shard
count, prove the final map lookup with a synthetic reassignment, and exercise
parallel and reopen stability. Process termination and filesystem faults remain
part of the later storage-hardening suite. Every new migration must extend the
registry, destination validator, format documentation, and the same
normal/error/concurrency/recovery matrix.
