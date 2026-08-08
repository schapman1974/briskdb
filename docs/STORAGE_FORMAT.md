# Manifest storage format and migrations

`manifest.sqlite` is BriskDB-owned storage. It is not a user database and is
never exposed through HTTP, PostgreSQL, MySQL, or the SQL execution API. The
format is pre-1.0, but every format change must still have an ordered migration,
failure coverage, and an explicit compatibility decision.

The final `manifest.sqlite` path component must be a regular file, never a
symbolic link. Startup may create that exact file when a fresh layout is
permitted; later migration opens never create a missing replacement, use
SQLite's no-follow mode, and revalidate the freshly opened layout identity
inside the same manifest transaction before changing journal state.

## Current format: version 6

SQLite header fields identify the file and its format:

| Header field | Value | Meaning |
| --- | --- | --- |
| `PRAGMA application_id` | `0x42524442` (`BRDB`) | Permanent BriskDB manifest-family marker |
| `PRAGMA user_version` | `6` | Authoritative manifest schema version |

The application ID prevents an accidental foreign SQLite file from being
adopted as a manifest. It is not authentication or tamper protection: a process
that can write the data directory can forge it. Checksums and explicit degraded
states remain issue #18.

Version 6 has ten strict manifest tables. It retains the v5 routing, logical
catalog, and physical-layout tables; rebuilds the schema-catalog singleton to
allow journaled generations; replaces the downgrade fence; and adds the
application-schema migration journal.

```sql
CREATE TABLE briskdb_manifest (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    shard_count INTEGER NOT NULL CHECK (shard_count BETWEEN 2 AND 64)
) STRICT;

CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 6)
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
    schema_generation INTEGER NOT NULL
        CHECK (schema_generation BETWEEN 0 AND 2147483647),
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

CREATE TABLE briskdb_shard_layout (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    layout_id BLOB NOT NULL
        CHECK (typeof(layout_id) = 'blob' AND length(layout_id) = 16),
    shard_application_id INTEGER NOT NULL CHECK (shard_application_id = 1112691528),
    shard_metadata_version INTEGER NOT NULL CHECK (shard_metadata_version = 1),
    layout_state INTEGER NOT NULL CHECK (layout_state IN (1, 2, 3))
) STRICT;

CREATE TABLE briskdb_schema_migrations (
    target_generation INTEGER PRIMARY KEY
        CHECK (target_generation BETWEEN 1 AND 2147483647),
    source_generation INTEGER NOT NULL
        CHECK (source_generation = target_generation - 1),
    migration_id BLOB NOT NULL UNIQUE
        CHECK (typeof(migration_id) = 'blob' AND length(migration_id) = 32),
    digest_version INTEGER NOT NULL CHECK (digest_version = 1),
    sql_text TEXT NOT NULL
        CHECK (
            typeof(sql_text) = 'text'
            AND length(CAST(sql_text AS BLOB)) BETWEEN 1 AND 65536
            AND instr(sql_text, char(0)) = 0
        ),
    shard_count INTEGER NOT NULL CHECK (shard_count BETWEEN 2 AND 64),
    migration_state INTEGER NOT NULL CHECK (migration_state IN (1, 2)),
    next_shard INTEGER NOT NULL CHECK (next_shard BETWEEN 0 AND shard_count),
    CHECK (migration_state = 1 OR next_shard = shard_count)
) STRICT;
```

The manifest, metadata, routing, schema-catalog, and shard-layout tables each
contain exactly one row. The v6 downgrade-fence row is exactly `6`.
`briskdb_manifest.shard_count` is immutable and is the initial routing modulus;
it is also the live physical-shard count. Physical IDs are exactly
`0..shard_count - 1`. Filenames remain derived by trusted code as
`shards/{shard_id:04}.sqlite` and are never read from catalog-controlled paths.
Version 6 supports only the `active` physical-shard lifecycle state. Adding
provisioning, draining, or retirement states to the routing catalog requires a
later format and state-machine change. The separate shard-layout state governs
only startup identity reconciliation.

### Logical catalog

Every fresh or upgraded v6 manifest contains logical database ID `1` named
`default`. A fresh or v5-upgraded manifest begins at application-schema
generation `0`; each completed journal row advances it by exactly one, through
a maximum of `2,147,483,647`. The schema-catalog singleton also contains
identifier encoding version `1` and default database ID `1`. Version 6 permits
at most 64 logical databases and 4,096 table rows. Database and table IDs are
positive; table names are unique within their owning database, and every table
references an existing database.

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

The loaded `Catalog` is read-only to callers and remains advisory in version 6:
there is no table-catalog mutation API, planner integration, or enforcement
against physical shard schemas. The engine publishes a newly committed schema
generation into its shared catalog snapshot only after every shard has reached
that generation. Fresh initialization and every v1/v2/v3 upgrade leave
`briskdb_tables` empty; a v4-to-v5 upgrade retains every validated v4 catalog
row. Schema migration does not inspect, infer, or mutate `briskdb_tables`, and
it does not compare the journaled SQL with physical schema equivalence. Tables
that are absent from the advisory catalog remain usable through the existing
explicit-key execute/query API. Version-5 adoption preserves existing tables
and rows while adding only BriskDB-owned shard identity metadata.

### Application-schema migration journal

`briskdb_schema_migrations` is retained generation history, not a transient
work queue. `migration_state = 1` means `Applying`; `migration_state = 2` means
`Complete`. `next_shard` is the ascending durable prefix boundary. Complete
rows always have `next_shard = shard_count` and are never deleted by the
current implementation.

Digest version 1 defines `migration_id` as the full 32-byte BLAKE3 digest of
the exact UTF-8 bytes in `sql_text`. There is no whitespace normalization,
case folding, comment removal, or statement parsing for identity. A
byte-identical retry finds and validates its existing active or completed row;
changing even insignificant-looking SQL bytes identifies a different
migration. Public migration SQL must be nonempty, at most 65,536 UTF-8 bytes,
and contain no NUL. The exact SQL remains in the manifest permanently so that
startup can recover without an external migration file. Operators must not put
passwords, access tokens, personal data, or other sensitive literals in a
migration batch.

Journal generations are contiguous from target generation 1 through the
committed catalog generation. Every such row is `Complete`. At most one
additional `Applying` row may exist, and it must target
`schema_generation + 1`. Its stored source generation, shard count, SQL text,
digest, state, and progress are validated on every manifest open. Any journal
history requires the physical layout to be `Ready`; `Creating` and `Adopting`
manifests have an empty journal. Fresh v6 initialization and v5-to-v6 upgrade
also create an empty journal at generation 0.

The retained journal proves which exact batches BriskDB coordinated; it does
not prove that a shard schema is equivalent to another schema or that its file
contents are untampered. Checksums, schema-equivalence checks, and explicit
degraded states remain issue #18. Richer migration submission, history, and
status APIs remain issue #53; the current public entry point retains the
`broadcast` name and response shape.

### Physical shard layout

The `briskdb_shard_layout` row is the durable authority for cross-file startup:

| Field | Value | Contract |
| --- | --- | --- |
| `layout_id` | 16 random bytes | Accidental binding shared by this manifest and all of its shards |
| `shard_application_id` | `0x42525348` (`BRSH`) | Distinguishes a BriskDB shard from `BRDB` manifest and foreign SQLite files |
| `shard_metadata_version` | `1` | Exact physical-shard metadata encoding described below |
| `layout_state` | `1` | `Creating`: fresh provisioning may create expected missing shards and enable WAL |
| `layout_state` | `2` | `Adopting`: existing legacy v4 shards may receive identity metadata |
| `layout_state` | `3` | `Ready`: only strict validation is allowed |

`layout_id` is generated with SQLite `randomblob(16)` when the v5 manifest row
is created and never changes. It detects an accidental shard copy from another
layout. Because the value and all other identity fields are stored in writable
files, they are not authentication, tamper protection, or proof of provenance.

Every current shard uses these persistent SQLite settings:

| Shard property | Required value | Meaning |
| --- | --- | --- |
| `PRAGMA application_id` | `0x42525348` (`BRSH`) | Permanent BriskDB shard-family marker |
| `PRAGMA user_version` | Current generation | Must equal the manifest's committed generation, except for the narrowly authorized source/target prefix of one active migration |
| `PRAGMA journal_mode` | `wal` | Required persistent journal mode |

Metadata encoding version 1 requires this exact strict identity table:

```sql
CREATE TABLE briskdb_shard_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    layout_id BLOB NOT NULL
        CHECK (typeof(layout_id) = 'blob' AND length(layout_id) = 16),
    shard_id INTEGER NOT NULL CHECK (shard_id BETWEEN 0 AND 63)
) STRICT;
```

It contains exactly `(1, manifest_layout_id, physical_shard_id)`. The physical
ID is derived from the canonical filename rather than trusted from the table.
Header identity, frozen table SQL, columns, strict flag, singleton row, value
types, layout ID, shard ID, and case-insensitive conflicts with the reserved
metadata name are all validated.

Startup scans the shard directory for the exact expected filenames from
`0000.sqlite` through the zero-padded final physical ID and rejects an
unexpected canonical four-digit `.sqlite` shard filename in every layout
state. Every existing
shard is opened read-write with SQLite create and symbolic-link following
disabled; only `Creating` may open a missing canonical path with create enabled.
An unrelated non-symlink entry with a UTF-8, noncanonical name is ignored for
an already recognized layout; symlinks and non-UTF-8 names fail closed. Any
pre-existing entry blocks fresh-manifest initialization because BriskDB cannot
prove that the physical layout is new.
Runtime pool opens use the same no-create path and validator, so deleting or
replacing a shard after startup cannot make a later checkout create or accept a
new file. A copied shard in a different slot fails its physical ID, a swapped
pair fails both IDs, and a shard from another data directory fails its layout
ID.

WAL validation reads `PRAGMA journal_mode` without assigning it. Only
`Creating` may issue `PRAGMA journal_mode=WAL`; `Adopting` and `Ready` reject a
different persisted mode. The transient `-wal`, `-shm`, and rollback-journal
sidecars can be absent and are not counted as canonical shard files.

Creation of new objects named `briskdb` or in the `briskdb_*` namespace is reserved. The metadata
table and persistent `application_id`, `user_version`, `journal_mode`,
`schema_version`, and `writable_schema` controls are BriskDB-owned. The SQL
authorizer denies client access to the metadata, creation of reserved objects,
and client mutation of those controls through every SQL path. Ordinary routed SQL
also denies all persistent DDL. Only the journaled migration connection permits
application DDL, including `ALTER TABLE`; it denies transaction/savepoint
escape, attachments, temporary and virtual objects, and storage-owned controls,
then compares the reserved schema before and after the batch. BriskDB, not the
submitted SQL, stamps `user_version` inside the shard transaction.

### Routing catalog

The routing singleton contains exactly these generation-1 values:

| Field | Value | Contract |
| --- | --- | --- |
| `hash_version` | `1` | BLAKE3 of the canonical key bytes, using digest bytes `0..8` as an unsigned little-endian `u64` |
| `key_encoding_version` | `1` | Exact caller-supplied bytes; string shard keys contribute their UTF-8 bytes, with no Unicode normalization |
| `bucket_algorithm_version` | `1` | Compatibility-preserving range algorithm below |
| `virtual_bucket_count` | `4096` | Fixed virtual bucket space `0..4095` |
| `map_generation` | `1` | Initial committed bucket map and the only generation version 6 can interpret |

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
`schema_generation`. Version 6 accepts only routing generation 1 and validates
its exact deterministic assignment; no public map-mutation operation exists
yet. A future format that can commit a changed map must bump `user_version` and
its downgrade fence as well as `map_generation`. That requirement makes this
pre-lookup binary reject the manifest instead of silently using legacy modulo
routing against a remapped catalog.

At each open, BriskDB validates the exact objects, columns, strict flags, frozen
schema SQL, singleton rows, logical identifiers and limits, metadata codes,
supported algorithm values, contiguous physical and bucket IDs, active
lifecycle states, assignments, coverage, and foreign keys. A recognized
version-6 manifest that violates any invariant is `DataCorruption` and is
rejected before shard connections are opened. The same locked transaction
returns routing and logical rows as one coherent shared snapshot. Request
routing performs no manifest query and cannot fall back to modulo after a failed
validation; only successful migration finalization publishes a newer logical
schema generation into the snapshot.

## Previous version 5

Version 5 has the first nine current tables and no
`briskdb_schema_migrations` table. Its schema-catalog definition constrains
`schema_generation = 0`, and its exact downgrade fence is:

```sql
CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 5)
) STRICT;
```

The fence contains exactly `5`, and `PRAGMA user_version` is `5`. A v6 opener
fully validates the v5 manifest, then atomically rebuilds only the
schema-catalog table, creates the empty migration journal, replaces the fence,
and stamps manifest version 6. It preserves the layout ID and state, routing,
logical databases, table-catalog rows, and every application table and row.
After that manifest-only step, an unfinished v5 `Creating` or `Adopting` layout
is reconciled exactly as before. That upgrade creates no active migration. In a
later v6 open, an active journal can exist only beside a durable `Ready` layout
and is resumed before ordinary layout reconciliation and final shard opens.

## Previous version 4

Version 4 has the first eight manifest-table roles shown above, uses the fixed
`schema_generation = 0` schema-catalog definition described for v5, and has no
`briskdb_shard_layout` or `briskdb_schema_migrations` table. Its exact downgrade
fence is:

```sql
CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 4)
) STRICT;
```

The fence contains exactly `4`, and `PRAGMA user_version` is `4`. Version 4 did
not own shard header fields or an identity table: shard files created by shipped
v4 code have `application_id = 0` and `user_version = 0`, while ordinary storage
open configured them for WAL. It also opened shards with create-capable flags,
so a missing file could previously be recreated silently.

A v5 opener fully validates the v4 manifest before committing a random layout
ID and state `Adopting` in the atomic v4-to-v5 manifest migration. Cross-file
adoption then accepts only existing WAL shard files with the exact legacy
zero/zero header or already valid resumable v5 identity. It never infers that a
missing shard is empty, changes an application table, or adopts a foreign header.

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
before applying the numbered v2-to-v3, v3-to-v4, v4-to-v5, and v5-to-v6
transactions.

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

Opening a `Database` or `Engine`, including server startup, may upgrade the
manifest, recover schema migration, and reconcile physical layout before
returning:

1. Validate the requested shard-count range, create and canonicalize the data
   directory, join its process-wide root coordination, and acquire the shared
   schema gate. Independent in-process handles resolving to the same canonical
   path therefore coordinate startup, migrations, admissions, and live catalog
   generation.
2. Determine whether fresh layout creation is allowed, validate the canonical
   parent and regular final file, then open the manifest with no symbolic-link
   following, a finite busy timeout, `synchronous=FULL`, and foreign keys
   enabled.
3. Acquire `BEGIN IMMEDIATE`, then read identity, version, schema, and stored
   configuration under that write lock.
4. Reject a foreign file, a newer version, a requested shard-count mismatch, or
   an invalid recognized schema before changing it.
5. Apply each compile-time registered manifest migration with static SQL and
   bound data. The destination application ID and `user_version` are the final
   mutations; validate the complete destination and commit one numbered step at
   a time.
6. Fresh initialization is allowed only beside an otherwise empty physical
   layout and commits v6 state `Creating` with generation 0 and an empty
   migration journal. An existing v1/v2/v3 manifest first advances through v4;
   the v4-to-v5 transaction commits a random 16-byte layout ID and state
   `Adopting` before any shard file changes. The v5-to-v6 manifest-only step
   preserves that state and creates an empty journal.
7. If the validated v6 manifest contains one `Applying` schema migration, mark
   startup as pending on interruption, validate its exact source/target shard
   prefix, and resume it in ascending order. Its layout is necessarily already
   durable `Ready`. The final migration transaction publishes `Complete` and
   the target catalog generation before ordinary layout reconciliation begins.
8. Acquire a new `BEGIN IMMEDIATE`, re-read and validate the committed layout
   identity and state, and reconcile every expected physical ID in ascending
   order while retaining the lock. Only `Creating` may create a missing file
   and enable WAL. `Adopting` requires an existing WAL file and accepts either
   the exact legacy zero/zero header or resumable current metadata. `Ready`
   accepts only the exact current format. Re-scan for unexpected canonical
   shard filenames, strictly revalidate every expected file, publish `Ready`
   only after full validation, and commit.
9. Open every shard once more with read-write, no-create, no-follow flags;
   apply connection-local durability and foreign-key settings only after
   identity, generation, and WAL validation. Reconcile the validated catalog
   generation with other live handles for the canonical root, publish the
   startup gate `Ready`, and only then return `Storage`, `Database`, or `Engine`.

SQLite transactional DDL keeps each numbered manifest step atomic within
`manifest.sqlite`. A version-1 upgrade commits versions 2, 3, 4, 5, and then 6;
a version-2 upgrade begins at 3, and so on. The v3-to-v4 step still creates the
logical catalog, inserts database ID 1 named `default` plus the
schema-generation-0 singleton, and leaves the table catalog empty.

The v4-to-v5 manifest transaction replaces the downgrade fence, creates the
shard-layout row in `Adopting`, and stamps version 5 before cross-file work.
Shard adoption is deliberately not one SQLite transaction across files. Each
completed shard keeps its metadata, while the durable manifest state remains
`Adopting`; a later open validates the completed prefix and resumes. The same
rule applies to partially completed fresh `Creating`. State becomes `Ready`
only after a full strict revalidation, so a ready manifest certifies the exact
layout observed at that transition.

The adoption path changes no application table or row and does not derive
identity from application schema. A legacy shard with a nonzero header, a
non-WAL mode, or a missing file is rejected without being repaired or replaced.
The v5-to-v6 step does not change any shard because a valid v5 database is at
schema generation 0 and has no journal history. Matching a shard generation
validates only the declared generation; `briskdb_tables` remains advisory and
is not checked against physical DDL.

A new application-schema migration follows a separate durable protocol:

1. Acquire the sole in-process schema-migration gate, stop admitting ordinary
   operations, and wait for operations already admitted to finish.
2. Validate the SQL identity and limits, then execute the complete batch inside
   a rollback-only immediate transaction on every shard at the source
   generation. The preflight also runs `PRAGMA main.foreign_key_check` after
   the batch so deferred violations cannot survive until a real commit. If any
   preflight fails or the request is cancelled, no journal or shard change is
   retained and ordinary work becomes ready again.
3. Append one `Applying` journal row at target generation
   `source_generation + 1` with `next_shard = 0`.
4. For each shard in ascending physical-ID order, execute the complete SQL
   batch and stamp its target `user_version` in the same immediate SQLite
   transaction. After that shard commit, advance the journal prefix by one in
   a separate manifest transaction.
5. Strictly validate every shard at the target generation, then atomically mark
   the journal row `Complete` and advance the schema-catalog generation in one
   final manifest transaction. Publish the new in-memory generation and admit
   ordinary work again.

There is deliberately no transaction spanning shard files and the manifest.
A crash after a shard commit but before its journal acknowledgement leaves the
durable prefix one position behind. Recovery permits exactly that one
already-target shard at the boundary, revalidates it, and advances without
executing the batch twice. Any other hole, regression, or out-of-range shard
generation is rejected. A byte-identical public retry resumes an active row or
validates and returns an already completed row without creating another
generation.

The controlled migration path installs cancellation-aware busy, progress, and
interrupt hooks. Cancellation or deadline expiration rolls back the currently
running shard transaction. Before journal publication it restores ordinary
admission with no durable migration; afterward, the committed prefix and
`Applying` row remain and ordinary operations fail with
`FailedPrecondition` until the same SQL or startup resumes. While a coordinator
is actively preflighting or applying, new ordinary work and another migration
coordinator receive retryable `Busy`. If SQLite attempts a manifest `COMMIT`
but reports an ambiguous cleanup or I/O failure, admission remains `Pending`
until recovery validates whether that boundary became durable.

Tests prove retry after injected errors and Rust panic unwinding at cross-file
persistence boundaries. Targeted subprocess-abort tests also prove migration
recovery when termination lands inside a shard transaction after its SQL or
generation stamp, and after the journal commit, a shard commit, or a
journal-progress commit. SQLite rolls the tested uncommitted shard transaction
back before exact retry. BriskDB has not certified arbitrary
process-kill timing, machine power loss, torn writes, or filesystem faults. The
state machines are therefore defined software-interruption contracts, not the
later storage-hardening certification.

## Compatibility and operations

- Version 1 upgrades through versions 2, 3, 4, and 5 to version 6; version 2
  begins at version 3, version 3 begins at version 4, version 4 begins at
  version 5, and version 5 upgrades directly to version 6. Opening is therefore
  potentially mutating.
- There is no automatic downgrade. Older readers are fenced by the incompatible
  metadata schema and authoritative header; in particular, a version-5 reader
  rejects `user_version = 6` before it can ignore the migration journal.
- A manifest with the BriskDB application ID and a `user_version` newer than the
  binary supports fails with non-retryable `FailedPrecondition`; it is not
  downgraded or switched to WAL, and shard files are not opened.
- A foreign nonzero application ID or unrelated unversioned SQLite database is
  a failed precondition. A recognized BriskDB identity whose schema, rows, or
  invariants disagree is `DataCorruption`.
- In `Ready` or `Adopting`, a missing shard is never created and a non-WAL shard
  is never switched to WAL. Extra four-digit `.sqlite` shard filenames are
  rejected rather than ignored. SQLite's transient `-wal` and `-shm` sidecars
  are not canonical shard files and need not exist after a clean close.
- The layout ID and application IDs detect accidental wrong-file placement;
  they are forgeable by anyone who can modify the directory and are not a
  security or checksum mechanism.
- BriskDB owns the shard identity table and the persistent `application_id`,
  `user_version`, and `journal_mode` settings. Client SQL access to the identity
  table and mutation of those settings is denied. `user_version` is the
  application-schema generation, not SQLite's internal `schema_version`.
- SQLite lock contention remains retryable `Busy`; permission, read-only, full,
  and I/O failures retain the storage error taxonomy.

Do not edit the header, tables, or rows manually. Until the checksum milestone,
a coordinated manual edit that still satisfies every structural invariant
cannot be distinguished from an intentional catalog update. The current
deployment boundary remains local storage and one BriskDB process per data
directory. Independent handles inside that process share a gate and live
catalog coordination keyed by the canonical root. A formal backup/restore
workflow is not implemented; recovery from a missing, swapped, or otherwise
incompatible shard—including one cloned into the wrong slot or from another
layout—requires the correct complete layout or a known-good copy made while
BriskDB was stopped. Recovery to an older binary likewise requires a pre-v6
backup. Separate server processes against one data directory are unsupported
even though SQLite and the manifest use file locks; the in-process coordination
is intentionally not a distributed lock.

## Verification contract

Tests cover fresh creation and every v1/v2/v3/v4/v5-to-v6 upgrade, every
manifest layout state, the exact shard header and metadata row, dynamic schema
generations, retained migration history, and no-op ready reopen. Failure
coverage includes missing and extra canonical files, foreign headers, non-WAL
mode, malformed metadata or journal rows, cross-layout shard clones, shard
swaps, symlinks, wrong generations, client attempts to reach storage-owned
state, and runtime pool replacement opens. Injection around each manifest,
layout, shard-migration, journal-progress, and finalization persistence boundary
proves resumability and prevents a ready layout or completed generation from
being published before full strict revalidation. Concurrent openers exercise
matching and conflicting shard counts and layout transitions.

Catalog tests continue to cover default logical metadata, every placement and
shard-key type code, identifier and relational corruption, row limits, and
immutable public lookups. Routing tests freeze golden hash/bucket/shard vectors,
cover every algorithm state for every supported shard count, prove the final
map lookup with a synthetic reassignment, and exercise parallel and reopen
stability. Schema-migration tests cover exact identity and input limits,
byte-identical retry, preflight rollback, per-shard atomicity, gate states,
cancellation, panic, and targeted process abort at durable boundaries.
Arbitrary process termination and filesystem faults remain part of the later
storage-hardening suite. Every new manifest-format migration must extend the
registry, destination validator, format documentation, and the same
normal/error/concurrency/recovery matrix.
