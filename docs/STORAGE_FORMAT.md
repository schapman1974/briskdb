# Manifest storage format and migrations

`manifest.sqlite` is BriskDB-owned storage. It is not a user database and is
never exposed through HTTP, PostgreSQL, MySQL, or the SQL execution API. The
format is pre-1.0, but every format change must still have an ordered migration,
failure coverage, and an explicit compatibility decision.
The [pre-1.0 compatibility policy](PRE_1_COMPATIBILITY.md) defines the required
operator backup, forward-upgrade, downgrade-refusal, and release-note contract.

The final `manifest.sqlite` path component must be a regular file, never a
symbolic link. Startup may create that exact file when a fresh layout is
permitted; later migration opens never create a missing replacement, use
SQLite's no-follow mode, and revalidate the freshly opened layout identity
inside the same manifest transaction before changing journal state.

The root also contains `.briskdb-process.lock` and
`.briskdb-startup.lock`. They are owner-only regular coordination files, not
SQLite databases or manifest state. Their bytes do not record ownership; the
kernel releases their advisory locks when the process exits. They must not be
replaced while a process is live. See the
[multi-process contract](MULTIPROCESS.md).

## Current format: version 13

SQLite header fields identify the file and its format:

| Header field | Value | Meaning |
| --- | --- | --- |
| `PRAGMA application_id` | `0x42524442` (`BRDB`) | Permanent BriskDB manifest-family marker |
| `PRAGMA user_version` | `13` | Authoritative manifest schema version |

The application ID prevents an accidental foreign SQLite file from being
adopted as a manifest. It is not authentication or tamper protection: a process
that can write the data directory can forge it and the unkeyed checksums
described below.

Version 13 has nineteen strict manifest tables plus the partial unique
allocation-owner index shown below. It retains the v12 routing,
authoritative logical catalog, physical layout, application-schema migration,
integrity, generated-ID activation, allocation-owner lifecycle, and recoverable
table-provisioning and hi/lo leasing tables; replaces the downgrade fence; adds
the durable global-index catalog and lifecycle; and changes no shard-file format.

```sql
CREATE TABLE briskdb_manifest (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    shard_count INTEGER NOT NULL CHECK (shard_count BETWEEN 2 AND 64)
) STRICT;

CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 13)
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

CREATE TABLE briskdb_generated_ids (
    table_id INTEGER PRIMARY KEY CHECK (table_id > 0),
    policy INTEGER NOT NULL CHECK (policy >= 0),
    generated_column TEXT COLLATE BINARY
        CHECK (
            generated_column IS NULL OR (
                length(generated_column) BETWEEN 1 AND 63
                AND instr(generated_column, char(0)) = 0
                AND generated_column NOT GLOB '*[^a-z0-9_]*'
                AND substr(generated_column, 1, 1) GLOB '[a-z_]'
                AND generated_column <> 'briskdb'
                AND generated_column NOT GLOB 'briskdb_*'
                AND generated_column NOT GLOB 'sqlite_*'
            )
        ),
    encoding_version INTEGER
        CHECK (encoding_version IS NULL OR encoding_version > 0),
    activation_state INTEGER NOT NULL CHECK (activation_state IN (0, 1)),
    FOREIGN KEY (table_id)
        REFERENCES briskdb_tables (table_id)
        ON DELETE RESTRICT,
    CHECK (
        (
            policy = 0
            AND generated_column IS NULL
            AND encoding_version IS NULL
            AND activation_state = 0
        )
        OR
        (
            policy > 0
            AND generated_column IS NOT NULL
            AND encoding_version IS NOT NULL
        )
    )
) STRICT;

CREATE TABLE briskdb_allocation_owners (
    owner_slot INTEGER PRIMARY KEY CHECK (owner_slot BETWEEN 0 AND 1023),
    physical_shard_id INTEGER NOT NULL
        CHECK (physical_shard_id BETWEEN 0 AND 63),
    owner_state INTEGER NOT NULL CHECK (owner_state IN (1, 2)),
    FOREIGN KEY (physical_shard_id)
        REFERENCES briskdb_physical_shards (shard_id)
        ON DELETE RESTRICT
) STRICT;

CREATE UNIQUE INDEX briskdb_one_active_owner_per_shard
ON briskdb_allocation_owners (physical_shard_id)
WHERE owner_state = 1;

CREATE TABLE briskdb_hilo_leases (
    table_id INTEGER PRIMARY KEY CHECK (table_id > 0),
    block_size INTEGER NOT NULL CHECK (block_size = 4096),
    next_sequence INTEGER NOT NULL
        CHECK (next_sequence BETWEEN 1 AND 2305843009213693952),
    fence_token INTEGER NOT NULL CHECK (fence_token >= 0),
    last_owner_id BLOB
        CHECK (
            last_owner_id IS NULL
            OR (typeof(last_owner_id) = 'blob' AND length(last_owner_id) = 32)
        ),
    last_first_sequence INTEGER
        CHECK (
            last_first_sequence IS NULL
            OR last_first_sequence BETWEEN 1 AND 2305843009213693951
        ),
    last_last_sequence INTEGER
        CHECK (
            last_last_sequence IS NULL
            OR last_last_sequence BETWEEN 1 AND 2305843009213693951
        ),
    FOREIGN KEY (table_id)
        REFERENCES briskdb_generated_ids (table_id)
        ON DELETE RESTRICT,
    CHECK (
        (
            fence_token = 0
            AND next_sequence = 1
            AND last_owner_id IS NULL
            AND last_first_sequence IS NULL
            AND last_last_sequence IS NULL
        )
        OR
        (
            fence_token > 0
            AND last_owner_id IS NOT NULL
            AND last_first_sequence IS NOT NULL
            AND last_last_sequence IS NOT NULL
            AND last_first_sequence <= last_last_sequence
            AND last_last_sequence < next_sequence
            AND last_last_sequence = next_sequence - 1
            AND last_last_sequence - last_first_sequence + 1 BETWEEN 1 AND block_size
        )
    )
) STRICT;

CREATE TABLE briskdb_generated_table_ddl (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    logical_id BLOB NOT NULL UNIQUE
        CHECK (typeof(logical_id) = 'blob' AND length(logical_id) = 32),
    logical_digest_version INTEGER NOT NULL CHECK (logical_digest_version = 1),
    source_dialect INTEGER NOT NULL CHECK (source_dialect IN (1, 2, 3)),
    translation_version INTEGER NOT NULL CHECK (translation_version = 1),
    source_sql TEXT NOT NULL
        CHECK (
            typeof(source_sql) = 'text'
            AND length(CAST(source_sql AS BLOB)) BETWEEN 1 AND 65536
            AND instr(source_sql, char(0)) = 0
        ),
    physical_migration_id BLOB NOT NULL UNIQUE
        CHECK (typeof(physical_migration_id) = 'blob' AND length(physical_migration_id) = 32),
    physical_sql TEXT NOT NULL
        CHECK (
            typeof(physical_sql) = 'text'
            AND length(CAST(physical_sql AS BLOB)) BETWEEN 1 AND 65536
            AND instr(physical_sql, char(0)) = 0
        ),
    database_id INTEGER NOT NULL CHECK (database_id > 0),
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
    generated_column TEXT NOT NULL COLLATE BINARY
        CHECK (
            length(generated_column) BETWEEN 1 AND 63
            AND instr(generated_column, char(0)) = 0
            AND generated_column NOT GLOB '*[^a-z0-9_]*'
            AND substr(generated_column, 1, 1) GLOB '[a-z_]'
            AND generated_column <> 'briskdb'
            AND generated_column NOT GLOB 'briskdb_*'
            AND generated_column NOT GLOB 'sqlite_*'
        ),
    generated_policy INTEGER NOT NULL CHECK (generated_policy = 1),
    generated_encoding_version INTEGER NOT NULL CHECK (generated_encoding_version = 1),
    lifecycle_state INTEGER NOT NULL CHECK (lifecycle_state IN (1, 2, 3)),
    provisioning_id BLOB
        CHECK (
            provisioning_id IS NULL
            OR (typeof(provisioning_id) = 'blob' AND length(provisioning_id) = 32)
        ),
    provisioning_schema_digest BLOB
        CHECK (
            provisioning_schema_digest IS NULL
            OR (
                typeof(provisioning_schema_digest) = 'blob'
                AND length(provisioning_schema_digest) = 32
            )
        ),
    table_id INTEGER CHECK (table_id IS NULL OR table_id > 0),
    FOREIGN KEY (physical_migration_id)
        REFERENCES briskdb_schema_migrations (migration_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (database_id)
        REFERENCES briskdb_logical_databases (database_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (table_id)
        REFERENCES briskdb_tables (table_id)
        ON DELETE RESTRICT,
    CHECK (
        (
            lifecycle_state = 1
            AND provisioning_id IS NULL
            AND provisioning_schema_digest IS NULL
            AND table_id IS NULL
        )
        OR (
            lifecycle_state = 2
            AND provisioning_id IS NOT NULL
            AND provisioning_schema_digest IS NOT NULL
            AND table_id IS NULL
        )
        OR (
            lifecycle_state = 3
            AND provisioning_id IS NOT NULL
            AND provisioning_schema_digest IS NOT NULL
            AND table_id IS NOT NULL
        )
    )
) STRICT;

CREATE TABLE briskdb_global_indexes (
    index_id INTEGER PRIMARY KEY CHECK (index_id > 0),
    table_id INTEGER NOT NULL,
    index_name TEXT NOT NULL COLLATE BINARY
        CHECK (
            length(index_name) BETWEEN 1 AND 63
            AND instr(index_name, char(0)) = 0
            AND index_name NOT GLOB '*[^a-z0-9_]*'
            AND substr(index_name, 1, 1) GLOB '[a-z_]'
            AND index_name <> 'briskdb'
            AND index_name NOT GLOB 'briskdb_*'
            AND index_name NOT GLOB 'sqlite_*'
        ),
    is_unique INTEGER NOT NULL CHECK (is_unique IN (0, 1)),
    null_semantics INTEGER NOT NULL CHECK (null_semantics > 0),
    predicate_sql TEXT
        CHECK (
            predicate_sql IS NULL
            OR (
                typeof(predicate_sql) = 'text'
                AND length(CAST(predicate_sql AS BLOB)) BETWEEN 1 AND 4096
                AND instr(predicate_sql, char(0)) = 0
            )
        ),
    lifecycle_state INTEGER NOT NULL CHECK (lifecycle_state > 0),
    key_encoding_version INTEGER NOT NULL CHECK (key_encoding_version > 0),
    schema_generation INTEGER NOT NULL
        CHECK (schema_generation BETWEEN 0 AND 2147483647),
    topology_kind INTEGER NOT NULL CHECK (topology_kind >= 0),
    topology_version INTEGER NOT NULL CHECK (topology_version >= 0),
    partition_count INTEGER NOT NULL CHECK (partition_count BETWEEN 0 AND 256),
    UNIQUE (table_id, index_name),
    FOREIGN KEY (table_id) REFERENCES briskdb_tables (table_id) ON DELETE RESTRICT,
    CHECK (is_unique = 1 OR null_semantics = 1)
) STRICT;

CREATE TABLE briskdb_global_index_parts (
    index_id INTEGER NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 15),
    source_kind INTEGER NOT NULL CHECK (source_kind > 0),
    source_text TEXT NOT NULL COLLATE BINARY
        CHECK (
            typeof(source_text) = 'text'
            AND length(CAST(source_text AS BLOB)) BETWEEN 1 AND 4096
            AND instr(source_text, char(0)) = 0
        ),
    key_type INTEGER NOT NULL CHECK (key_type > 0),
    sort_order INTEGER NOT NULL CHECK (sort_order > 0),
    null_order INTEGER NOT NULL CHECK (null_order > 0),
    collation_version INTEGER NOT NULL CHECK (collation_version > 0),
    PRIMARY KEY (index_id, ordinal),
    FOREIGN KEY (index_id)
        REFERENCES briskdb_global_indexes (index_id)
        ON DELETE CASCADE
) STRICT;

CREATE TABLE briskdb_table_provisioning (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    provisioning_id BLOB NOT NULL
        CHECK (typeof(provisioning_id) = 'blob' AND length(provisioning_id) = 32),
    digest_version INTEGER NOT NULL CHECK (digest_version = 1),
    schema_digest_version INTEGER NOT NULL CHECK (schema_digest_version = 1),
    committed_schema_digest BLOB NOT NULL
        CHECK (typeof(committed_schema_digest) = 'blob' AND length(committed_schema_digest) = 32),
    shard_count INTEGER NOT NULL CHECK (shard_count BETWEEN 2 AND 64),
    declaration_count INTEGER NOT NULL CHECK (declaration_count BETWEEN 1 AND 4096),
    next_shard INTEGER NOT NULL CHECK (next_shard BETWEEN 0 AND shard_count)
) STRICT;

CREATE TABLE briskdb_table_provisioning_declarations (
    provisioning_singleton INTEGER NOT NULL CHECK (provisioning_singleton = 1),
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 4095),
    database_id INTEGER NOT NULL CHECK (database_id > 0),
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
    generated_policy INTEGER NOT NULL CHECK (generated_policy >= 0),
    generated_column TEXT COLLATE BINARY,
    generated_encoding_version INTEGER
        CHECK (generated_encoding_version IS NULL OR generated_encoding_version > 0),
    PRIMARY KEY (provisioning_singleton, ordinal),
    UNIQUE (provisioning_singleton, database_id, table_name),
    FOREIGN KEY (provisioning_singleton)
        REFERENCES briskdb_table_provisioning (singleton)
        ON DELETE CASCADE,
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
    ),
    CHECK (
        (
            generated_policy = 0
            AND generated_column IS NULL
            AND generated_encoding_version IS NULL
        )
        OR
        (
            generated_policy > 0
            AND generated_column IS NOT NULL
            AND generated_encoding_version IS NOT NULL
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

CREATE TABLE briskdb_integrity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    manifest_digest_version INTEGER NOT NULL CHECK (manifest_digest_version > 0),
    manifest_digest BLOB NOT NULL
        CHECK (typeof(manifest_digest) = 'blob' AND length(manifest_digest) = 32),
    schema_digest_version INTEGER NOT NULL CHECK (schema_digest_version > 0),
    database_state INTEGER NOT NULL CHECK (database_state IN (1, 2, 3, 4)),
    committed_schema_digest BLOB
        CHECK (
            committed_schema_digest IS NULL
            OR (
                typeof(committed_schema_digest) = 'blob'
                AND length(committed_schema_digest) = 32
            )
        ),
    target_schema_digest BLOB
        CHECK (
            target_schema_digest IS NULL
            OR (
                typeof(target_schema_digest) = 'blob'
                AND length(target_schema_digest) = 32
            )
        ),
    CHECK (
        (database_state = 1 AND target_schema_digest IS NULL)
        OR (
            database_state = 2
            AND committed_schema_digest IS NOT NULL
            AND target_schema_digest IS NULL
        )
        OR (
            database_state = 3
            AND committed_schema_digest IS NOT NULL
            AND target_schema_digest IS NOT NULL
        )
        OR database_state = 4
    )
) STRICT;
```

The manifest, metadata, routing, schema-catalog, shard-layout, and integrity
tables each contain exactly one row. The v13 downgrade-fence row is exactly
`13`. `briskdb_generated_table_ddl` and `briskdb_table_provisioning` each
contain zero or one row. A completed generated-table bridge is retained;
table-provisioning declaration rows exist only while their transient parent row
exists.
The two integrity-version columns deliberately accept any positive integer so
a future digest encoding can remain structurally readable long enough for an
older binary to reject it as `FailedPrecondition`; v13 writers emit manifest
digest version `6` and schema digest version `1`.
Zero or negative versions are malformed and are `DataCorruption`.
`briskdb_manifest.shard_count` is immutable and is the initial routing modulus;
it is also the live physical-shard count. Physical IDs are exactly
`0..shard_count - 1`. Filenames remain derived by trusted code as
`shards/{shard_id:04}.sqlite` and are never read from catalog-controlled paths.
Version 13 supports only the `active` physical-shard lifecycle state. Adding
provisioning, draining, or retirement states to the routing catalog requires a
later format and state-machine change. The separate shard-layout state governs
only startup identity reconciliation.

### Logical catalog

Every fresh or upgraded v13 manifest contains logical database ID `1` named
`default`. A fresh or pre-v6 upgrade begins at application-schema
generation `0`; each completed journal row advances it by exactly one, through
a maximum of `2,147,483,647`. The schema-catalog singleton also contains
identifier encoding version `1` and default database ID `1`. Version 13 permits
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

### Global-index catalog and lifecycle

Version 13 adds up to 4,096 durable global-index definitions with at most 16
ordered key parts each. A definition records its stable ID, owning Sharded
table, canonical name, column/expression parts and types, uniqueness and NULL
semantics, optional predicate, schema generation, canonical key-encoding
version, and selected storage topology. Unsupported positive encoding,
lifecycle, topology, type, ordering, or collation versions return an actionable
`FailedPrecondition`; malformed values are `DataCorruption`.

Lifecycle transitions are explicit: `Creating` may become `Ready`, `Invalid`,
or `Dropping`; `Ready` may become `Invalid`, `Rebuilding`, or `Dropping`;
`Invalid` may become `Rebuilding` or `Dropping`; and `Rebuilding` may become
`Ready`, `Invalid`, or `Dropping`. Removal is legal only from `Dropping`.
`Ready` and `Rebuilding` require an assigned versioned topology and the current
schema generation. Public callers cannot transition an index to `Ready`; the
offline builder owns that transition after validating durable physical state.
Application-schema migration is fenced while any definition exists, avoiding a
catalog/schema split until later coordinated rebuild work.

Each create, transition, and removal is one `BEGIN IMMEDIATE` transaction that
reseals the semantic root before commit. The existing process mutation fence
requires sole-process ownership for writers. `Database::inspect_global_indexes`
opens the manifest read-only, performs full version/checksum/catalog validation,
and neither initializes nor upgrades storage; concurrent readers therefore see
only the complete old or complete new snapshot. Version 13 stores metadata
only. The measured initial physical choice is `SharedSqliteV1`, with one
partition and every key routed to partition zero; the exact comparison and
migration option are documented in the [topology decision](GLOBAL_INDEX_TOPOLOGY.md).
Issue #230 adds the separate physical format and offline construction described
in [offline global-index construction](GLOBAL_INDEX_BUILD.md); it does not
change the version-13 manifest or any shard-file format.

Table placement and shard-key type use stable numeric codes:

| Column | Code | Rust meaning | Stored metadata |
| --- | --- | --- | --- |
| `placement` | `1` | `Sharded` | One non-null shard-key column and type are required |
| `placement` | `2` | `Global` | Shard-key column and type must both be null |
| `placement` | `3` | `Catalog` | Shard-key column and type must both be null |
| `shard_key_type` | `1` | `Int64` | Signed 64-bit integer |
| `shard_key_type` | `2` | `Text` | Exact UTF-8 without Unicode normalization; authoritative physical columns must use SQLite `BINARY` collation |
| `shard_key_type` | `3` | `Binary` | Arbitrary bytes |

`Sharded` means the same logical schema exists on every shard but each ordinary
row has exactly one physical owner selected from its canonical shard-key bytes.
A duplicate ordinary row on several shards violates that placement; it is not a
sharded copy. `Global` explicitly describes replicated lookup rows. `Catalog`
describes manifest-owned logical metadata and has no physical application-table
shadow.

An implicit SQLite `rowid` on a Sharded table is a shard-local physical locator,
not a globally unique logical identity, routing key, or ordering key. Different
owners may therefore contain the same hidden `rowid`. Declared primary and
unique keys remain globally valid because every one contains the authoritative
shard key and every possible collision meets on one owner. An `INTEGER PRIMARY
KEY` rowid alias is different: it is a visible declared key and is validated by
the ordinary shard-key and unique-locality rules.

### Generated-ID policies and allocation owners

`briskdb_generated_ids` contains exactly one row for every
`briskdb_tables` row and no others. Policy is stored separately from physical
SQLite DDL so a mutable `AUTOINCREMENT` clause, `DEFAULT`, or `sqlite_sequence`
row can never grant allocation authority by inference. The stable codes are:

| `policy` | Rust policy | `generated_column` | `encoding_version` | Allowed `activation_state` |
| --- | --- | --- | --- | --- |
| `0` | `GeneratedIdPolicy::None` | null | null | `0` (`Inactive`) only |
| `1` | `GeneratedIdPolicy::NativeRangeV1` | The table's canonical Int64 shard-key column | `1` | `0` (`Inactive`) or `1` (`Active`) |
| `2` | `GeneratedIdPolicy::HiloV1` | The table's canonical Int64 shard-key column | `1` | `0` (`Inactive`) or `1` (`Active`) |

Policy and activation are orthogonal. An inactive generated-policy row preserves
its ID classification and routing domain, but it grants no omitted-key
allocation authority. Generated inserts fail closed until provisioning has
made every shard durable and atomically changed that row to active. A `None`
policy can never be active.

The SQL shape permits a positive future policy or encoding so an older binary
can distinguish an unsupported format from malformed storage. The reader first
verifies the version-5 semantic root, so an unsealed change remains
`DataCorruption`; with a valid root, it returns `FailedPrecondition` as soon as
a structurally admitted positive policy or encoding is newer than it supports.
It does not apply current-version relational semantics to that future format.
For supported policy and encoding
codes, zero or negative encodings, null native fields, a noncanonical column,
missing or extra policy rows, and relational disagreement are `DataCorruption`.
Current cross-table validation accepts `native_range_v1` and `hilo_v1` only for
a `Sharded` table whose generated column exactly equals its visible `Int64`
shard key. Native activation requires that physical column to be exactly
`INTEGER PRIMARY KEY AUTOINCREMENT` on every shard. Hi/lo activation requires
exactly `INTEGER PRIMARY KEY` without `AUTOINCREMENT`. Both shapes require one
visible primary-key column with no default. `Global`, `Catalog`, Text, Binary,
or a different generated column fail closed. An inactive migrated policy
remains readable and explicitly routable without claiming that a legacy
physical schema is an allocator. An exact provisioning retry compares the
complete policy and activation outcome as part of idempotency.

`briskdb_allocation_owners.owner_state = 1` means `Active`; `2` means
`Retired`. Every physical shard has exactly one active allocation owner, which
is the only slot that may allocate a new native ID there. A retired owner keeps
its immutable mapping to the same physical shard so historical native IDs
continue to route for reads and mutation of existing rows; a new insert that
names a retired owner is rejected. Several historical retired owners may map to
one shard, while the partial unique index enforces exactly one live allocator
after manifest validation. Owner succession is strictly increasing per physical
shard: its active slot must be greater than every retired slot mapped to that
shard. SQLite never lowers an `AUTOINCREMENT` high-water mark, even after the
row which established it is deleted, so a lower replacement could otherwise
continue allocating values in a retired owner's encoded range.

Fresh version 13 storage seeds active `owner_slot = physical_shard_id`, so the
initial slots are the contiguous range `0..shard_count - 1`. A slot is an
immutable ID namespace, not a value that may be recomputed from a later shard
count or bucket map. Version 13 has no public owner-map mutation operation, but
its format preserves the state required by a later resharding workflow: retire
the old slot without deleting it and explicitly add a never-used replacement
whose slot is greater than every prior owner of that physical shard.
Reactivating, remapping, deleting, reassigning a used slot, or installing a
non-increasing successor would make old IDs ambiguous or violate SQLite's
persisted high-water mark. Missing active coverage, two active owners for one
shard, out-of-range slots, or mappings to foreign shards are corruption.

Native range encoding version 1 uses these signed 64-bit fields:

```text
bit       63  62  61................52  51........................0
meaning    0   1    owner slot (10)       local sequence (52)
mask           0x4000_0000_0000_0000      0x000f_ffff_ffff_ffff
```

Owner slots are `0..=1023`. Local sequence zero is reserved as the allocator
floor and is not a valid row ID; valid values are `1..=2^52-1`. Thus every
native ID is positive, disjoint per owner, and the maximum owner and local
sequence encode to `i64::MAX`. Encoding rejects an owner or sequence outside
those bounds before arithmetic. Under `NativeRangeV1`, negative values and
marker-clear positive values classify as explicit legacy IDs, while a
marker-set value with local sequence zero is `DataCorruption`; all other
marker-set values decode to their owner and local sequence. Under `None`, every
signed integer—including a marker-looking imported value—is legacy. The strict
native decoder rejects rather than classifies negative and marker-clear values.

Activation seeds one `sqlite_sequence` row per native table and physical shard
to that shard's reserved owner floor before manifest publication. A retry may
raise a marker-clear legacy high-water mark to the floor and preserves an
existing same-owner native high-water mark; it never lowers one. Missing,
duplicate, non-integer, below-floor, above-ceiling, foreign-owner, or
row-lagging sequence state after activation is corruption. The exact ceiling is
a valid exhausted state, and allocation rejects it before SQLite can cross into
the next owner's range.

Issue #130's AST planner can now recognize exactly one omitted-key row and hand
it to the gated allocator-backed coordinator; that execution contract is
documented in [generated keys](GENERATED_KEYS.md). The physical design still
cannot replace exact `INTEGER PRIMARY KEY` with a schema `DEFAULT` function:
it is a rowid alias, and SQLite chooses an omitted or NULL rowid through its
special insert path instead of evaluating a column default. A side-effecting
allocation UDF would also make schema evaluation connection-local and
retry-sensitive; putting its counter in one central file would recreate one
serialized writer. The no-fork design instead lets unmodified SQLite allocate
inside disjoint shard-local ranges after those ranges are explicitly installed.

Hi/lo encoding version 1 uses bit 61 as its format marker and the lower 61 bits
as one global per-table sequence:

```text
bit       63  62  61  60........................................0
meaning    0   0   1          global sequence (61)
mask               0x2000_0000_0000_0000
```

Sequence zero is reserved, so valid IDs are
`0x2000_0000_0000_0001..=0x3fff_ffff_ffff_ffff`. The interval is positive and
disjoint from `native_range_v1`, whose marker begins at bit 62. Under
`HiloV1`, negative and positive values below the hi/lo marker classify as
explicit legacy IDs and retain the canonical Int64 hash route. Valid hi/lo IDs
also hash-route as their complete encoded Int64 value through routing
generation 1 and the persisted virtual-bucket map. Every explicit INSERT value
at or above the hi/lo marker is allocator-owned and rejected; that rule also
prevents a native-range encoding from being introduced into a hi/lo table.
Sequence-zero and incompatible generated namespaces fail closed on reads and
existing-row mutations rather than being treated as legacy.

`briskdb_hilo_leases` contains exactly one row for every active `hilo_v1`
policy and no row for an inactive hi/lo, native, or `None` policy. Activation
inserts the initial state `block_size = 4096`, `next_sequence = 1`, and
`fence_token = 0`, with all last-lease columns null. A lease transaction uses
`BEGIN IMMEDIATE`, validates the complete current manifest, and reserves
`first = next_sequence` through
`last = min(first + 4095, 2^61 - 1)`. It advances `next_sequence` to
`last + 1`, increments the positive fence, records the requester's random
32-byte process-incarnation ID and exact range, refreshes semantic digest
version 5, validates the result, and commits before returning the block. The
terminal `next_sequence = 2^61` is a valid exhausted head; another reservation
returns `LimitExceeded`. Fence overflow also returns `LimitExceeded`.

The fence identifies a successive committed reservation; it is not a lock
file, timestamp, expiry, or revocation of IDs issued under an older fence. No
wall or monotonic clock is stored or consulted. Independent processes contend
on SQLite's manifest transaction and therefore cannot commit overlapping
ranges. Process-local handles for the same canonical root share one cache per
table and write the manifest once per block, not once per row. Independently
started same-host processes may also serve ordinary work on the same ready
local root. Each must initialize BriskDB after its own start, including after an
`exec` boundary. A child must not inherit and continue using a live BriskDB
handle or cached lease across `fork()`.

Every committed block is irrevocable. The allocator consumes an ID before
taking the target-shard write lock and never returns it to the cache after a
rollback, cancellation, ignored insert, constraint failure, or child commit
failure. Process exit abandons the unused tail; startup never recovers it. If a
manifest commit reports an error after it may have become durable, BriskDB
returns no lease and a later reservation advances from the durable head, so the
ambiguous block is burned rather than reused. Gaps are expected. Numeric order
represents allocation order only and may differ from commit order. The policy
guarantees uniqueness and non-reuse across processes and shards, not a gapless
sequence or global transaction ordering.

The loaded `Catalog` remains read-only to observers. Version 8 added the one-time
`Database::register_tables` mutation boundary for an empty table catalog. The
caller supplies the complete declaration set for the storage-default logical
database. On every shard, the ordinary application-table names must exactly
equal the `Sharded` and `Global` names, every declared physical table must be
empty, and each `Catalog` name must have no physical table or view shadow. A
sharded key must be a visible, physically non-null column whose SQLite affinity
is compatible with its declared `Int64`, `Text`, or `Binary` type. The
non-null `INTEGER PRIMARY KEY` rowid alias qualifies; nullable legacy
primary-key forms do not. A Text shard key must use SQLite `BINARY` collation.
Every primary or unique key on a sharded table must include its shard-key column
with `BINARY` collation, so every possible collision has one physical owner.
Foreign keys are accepted only when authoritative placement proves local
enforcement: matching co-sharded keys in the same generated-ID routing domain,
Sharded-to-Global, or Global-to-Global. Missing, Catalog, cross-placement, or
SQLite-unenforceable relationships, triggers, and virtual tables are
unsupported. Registration assigns table IDs after sorting by logical database
and canonical table name. A declaration set containing only `None` policies
commits every table and policy row, refreshes the manifest root atomically, and
publishes the replacement catalog only after revalidation. A set containing a
native or hi/lo policy instead uses the provisioning journal below; it does not
publish allocator authority in the transaction that records intent.

Registration is initialization-only and requires exclusive live ownership of
the data root. An exact complete repeat is a read-only idempotent success;
empty, partial, different, duplicate, nonempty, or physically mismatched
declarations fail without replacing the catalog. Once populated, the catalog
cannot be edited in place. The one exception is an exact declaration repeat for
a v9 catalog migrated with an inactive native policy: v13 may provision that
unchanged policy if all declared physical tables are still empty, but it may
not alter a declaration or catalog ID. A later journaled schema migration must preserve the
exact registered physical table set on every shard and retain every sharded
key's visible, physically non-null column, compatible affinity, Text collation,
and one-owner unique keys. Foreign-key changes must retain the same conservative
co-location rules; a migration must not introduce a trigger or virtual table.
A populated-catalog migration also rejects row-moving
DML, `CREATE TABLE ... AS SELECT`, `DROP TABLE`, and `CREATE TRIGGER`. A
violation is rejected before journal or shard publication.

The registration guard changes in-process schema admission to `Pending` before
attempting a manifest commit that could durably change registration or
provisioning state. If SQLite reports an ambiguous commit cleanup or I/O
failure, the registering handle deliberately retains its old catalog and
ordinary work remains closed. Close that stale handle and reopen the canonical
data root; startup then validates whether no request, a recoverable provisioning
prefix, or the complete new catalog became durable. While the stale handle is
live, a reopen that observes a committed replacement fails `FailedPrecondition`
rather than publishing conflicting catalog snapshots. A new registration must
not be attempted through the pending handle.

### Generated-table DDL bridge

Version 12 adds `briskdb_generated_table_ddl`, a zero-or-one-row retained bridge
between exact source-dialect DDL, its canonical physical schema migration, and
native-range catalog provisioning. `Database::apply_generated_table_ddl`
requires exclusive mutable initialization ownership, an empty authoritative
catalog, and exactly one supported generated-key `CREATE TABLE`. It always uses
compatibility translation, derives one default-database `Sharded` declaration
whose canonical generated `Int64` column is its shard key, and selects
`native_range_v1` encoding version 1. This bridge is not a general catalog edit,
multi-table batch, hi/lo declaration, or network DDL endpoint.

The table's stable codes are:

| Column | Code | Meaning |
| --- | ---: | --- |
| `logical_digest_version` | `1` | Exact generated-table logical identity format |
| `source_dialect` | `1` | SQLite |
| `source_dialect` | `2` | PostgreSQL |
| `source_dialect` | `3` | MySQL |
| `translation_version` | `1` | Finite generated-table compatibility translation |
| `generated_policy` | `1` | `native_range_v1` |
| `generated_encoding_version` | `1` | Native-range ID encoding version 1 |
| `lifecycle_state` | `1` | `ApplyingPhysical` |
| `lifecycle_state` | `2` | `Provisioning` |
| `lifecycle_state` | `3` | `Complete` |

Logical identity version 1 is the full 32-byte BLAKE3 digest of this exact byte
stream:

1. ASCII domain bytes `briskdb.generated-table-ddl.v1\0`;
2. little-endian `u32` logical digest version `1`;
3. little-endian `i64` source-dialect code;
4. little-endian `u32` translation version `1`; and
5. little-endian `u64` exact source-SQL byte length followed by those exact
   UTF-8 bytes.

Source SQL is 1 through 65,536 bytes with no NUL. Whitespace, comments, keyword
case, identifier quoting, and dialect are identity-bearing. The separate
`physical_migration_id` is schema-migration digest version 1: full BLAKE3 over
the exact canonical SQLite bytes in `physical_sql`. Two logical declarations
may therefore have compatible physical SQL without sharing logical identity.
`provisioning_id` is the separate table-provisioning digest defined below and
binds the derived declaration, shard count, and the retained
`provisioning_schema_digest` committed after the physical migration.
`GeneratedTableDdlReceipt` returns those three 32-byte identities plus the
stable final `table_id`; the schema digest remains a checksummed input to the
provisioning identity rather than a fourth receipt identity.

The bridge begins only when there is no other table provisioning and the
authoritative table catalog is empty. One manifest transaction both inserts
the `ApplyingPhysical` bridge and begins the referenced schema-migration row;
the foreign key and validation require its migration ID and exact SQL to stay
identical. In this state `provisioning_id`, `provisioning_schema_digest`, and
`table_id` are null. The normal schema coordinator preflights every shard,
applies canonical DDL in ascending order, durably acknowledges each committed
prefix, and finalizes that migration before the bridge may advance.

The transition to `Provisioning` records the current committed physical-schema
digest as `provisioning_schema_digest` and computes `provisioning_id` from that
digest only after the referenced migration is complete; `table_id` remains
null. While this phase is active, the retained schema digest must still equal
the current committed digest. The matching transient provisioning journal may
be absent briefly before it is created or may retain its exact acknowledged
shard prefix. It must use the bridge's one reconstructed declaration, retained
schema digest, and provisioning identity. After every shard's native sequence
floor is durable, one manifest transaction finalizes provisioning, publishes
the table and active policy, deletes the transient provisioning rows, stores
the stable `table_id`, and moves the retained bridge to `Complete`.

A complete bridge requires its physical migration complete, its provisioning
identity reproducible from the retained provisioning-time schema digest, no
active provisioning journal, and a matching active catalog table. The retained
digest is deliberately historical after completion: a later supported schema
migration may advance the manifest's current committed schema digest without
changing the completed bridge or invalidating its receipt.

An exact call retry may find the same bridge in any phase and resumes it; any
different logical, physical, or declaration input receives
`FailedPrecondition`. Startup resumes an active physical migration first, then
uses only the retained validated declaration to advance or reconstruct the
provisioning phase. It replays an unacknowledged sequence seed idempotently and
atomically finalizes catalog plus bridge before publishing `Ready`. Lifecycle,
digest, migration, provisioning, or catalog disagreement is `DataCorruption`
and fails closed; recovery never reparses stored SQL to guess a replacement
declaration. `Complete` is retained permanently for audit and exact retry.

### Generated table-provisioning journal

`briskdb_table_provisioning` is a transient, checksummed recovery record for
the cross-file work required to activate generated-ID allocation. It has either no
row or singleton row `1`. `digest_version = 1` identifies a deterministic
32-byte `provisioning_id` over the complete normalized declaration set, the
requested shard count, and the committed schema digest. The recorded
`schema_digest_version`, digest bytes, shard count, declaration count, and
identity must all match the current trusted manifest and declarations.

Provisioning digest version 1 freezes this exact BLAKE3 byte stream:

1. ASCII domain bytes `briskdb.table-provisioning.v1\0`;
2. little-endian `u32` digest version, little-endian `u16` shard count, the 32
   committed-schema-digest bytes, and little-endian `u64` declaration count;
3. for each declaration in normalized `(database_id, table_name)` order:
   little-endian `u64` database ID; the table name as little-endian `u64`
   byte length followed by its bytes; little-endian `i64` placement; optional
   shard column as tag byte `0` or tag `1` plus the same length-prefixed text;
   optional shard-key type as tag byte `0` or tag `1` plus little-endian `i64`;
   little-endian `i64` generated policy; optional generated column with that
   same tagged-text encoding; and optional generated encoding version with
   that same tagged-`i64` encoding.

Changing this stream requires a new provisioning digest version; an active
journal must remain reproducible byte-for-byte across upgrades and restart.

`briskdb_table_provisioning_declarations` stores that complete canonical
declaration set in ascending `ordinal` order. It repeats placement, shard-key,
and generated-policy metadata rather than depending on an uncommitted catalog.
Ordinals must be contiguous from zero, names must be unique in their logical
database, the row count must equal `declaration_count`, and the parent foreign
key deletes all rows when finalization removes the singleton. An active
application-schema migration and active table provisioning are mutually
exclusive.
The protocol is deliberately one-way and prefix based:

1. Under the exclusive in-process schema gate, validate the complete empty
   physical table set and committed schema digest. In one manifest transaction,
   write the provisioning singleton and all declarations, reseal digest version
   5, and commit before changing any shard.
2. Starting at `next_shard`, validate every generated table's exact physical
   shape and seed every native table's active-owner `sqlite_sequence` floor in
   one shard-local transaction. Hi/lo needs no shard-local allocator state.
   Only after that shard commits may a separate manifest transaction advance
   `next_shard` by exactly one and reseal the root.
3. Once `next_shard = shard_count`, one final manifest transaction assigns or
   verifies the authoritative catalog, changes every requested generated policy
   to `activation_state = 1`, inserts the initial lease row for every hi/lo
   table, deletes both provisioning tables' rows, reseals the root, and commits.
   Only that complete catalog snapshot may be published to callers.

A process loss after journal publication leaves ordinary admission closed on
the next open while startup resumes the exact recorded request. A loss after a
shard commit but before its prefix acknowledgement safely repeats that shard:
seeding is idempotent, preserves a same-owner high-water mark, and never lowers
the sequence. Startup revalidates the declarations as empty, verifies each
acknowledged or replayed shard, resumes in ascending order, and publishes
`Ready` only after atomic finalization. Conflicting declarations, schema
digests, owner ranges, or nonempty tables fail closed; BriskDB never infers a
different request from partial shard state.

Fresh v13 initialization leaves `briskdb_tables`, `briskdb_generated_ids`,
`briskdb_hilo_leases`, `briskdb_generated_table_ddl`, both global-index tables,
and both provisioning tables empty. The v7-to-v8 migration
also clears all v7 table rows because those rows were advisory and were never
proved against the physical schema; silently promoting them would create false
routing authority. The upgrade preserves logical databases, routing, schema
generation and history, layout and integrity state, shard files, application
schema, and rows. Registration does not inspect or repartition existing row
data, which is why it accepts only empty declared physical tables. Raw HTTP
execute/query remains caller-key-only while this catalog is empty. Once
populated, it parses and strictly translates one SQLite common-subset statement,
consults placement, keeps writes on one proven owner, and selects the relevant
owner set for supported logical reads; undeclared and Catalog targets fail
closed, Global reads use shard 0, and Global writes require a future replication
operation.

The authoritative catalog supplies placement to inference, planning, prepared
execution, import, logical scatter/gather, and the admin browser. Runtime
coordination now provides bounded `UNION ALL` semantics without changing the
manifest or shard-file format. Later planner issues extend global ordering,
aggregation, and pagination semantics for arbitrary client SQL.

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
manifests have an empty journal. Fresh v13 initialization and pre-v6 upgrades
begin with an empty journal at generation 0.

The retained journal proves which exact batches BriskDB coordinated; it does
not authenticate the files or cryptographically cover application rows. Version
7 separately requires every shard's persistent application-schema fingerprint
to match the trusted source or target fingerprint for its position in an active
migration. Richer migration submission, history, and status APIs remain issue
#53. The general physical-SQL entry point retains the `broadcast` name and
response shape; the generated-table DDL bridge is a separate initialization-only
producer of one canonical physical migration.

### Integrity metadata and durable database states

`briskdb_integrity` contains the supported digest versions, a semantic manifest
root, the durable database state, the trusted committed application-schema
fingerprint, and an optional migration target fingerprint. Its numeric states
are distinct from the v5 physical-layout states:

| Code | State | Required checksum and journal shape | Admission contract |
| --- | --- | --- | --- |
| `1` | `Verifying` | No active migration and no target fingerprint; the committed fingerprint may be absent during first bootstrap | Startup must verify one shard-schema consensus and seal it before serving work |
| `2` | `Ready` | One committed fingerprint, no target fingerprint, no active schema migration, physical layout `Ready`; may contain one active table-provisioning prefix and a matching `Provisioning` bridge, or a retained `Complete` bridge | Ordinary operations may run only when no transient provisioning record exists; bridge or registration recovery otherwise owns the gate |
| `3` | `Migrating` | Committed source and target fingerprints plus exactly one `Applying` schema-migration row, no table provisioning, physical layout `Ready`; an `ApplyingPhysical` bridge, when present, must reference that migration exactly | Only the migration coordinator may advance the validated prefix |
| `4` | `Degraded` | Preserves whatever trusted committed/target fingerprints and active journal existed when validation failed | Terminal fail-closed state; startup and all new work return non-retryable `DataCorruption` until the complete database is restored from a known-good copy |

`Pending` is an in-process admission state used after durable schema-migration,
table-provisioning, or generated-table bridge progress, and after a
durability-ambiguous manifest commit, including first catalog registration; it
is not a fifth manifest state. A state transition, its checksum fields, the
corresponding journal/catalog mutation, and the refreshed semantic root commit
in one `manifest.sqlite` transaction. A failed validation
marks the canonical
root's shared in-process gate `Degraded` immediately. BriskDB also persists
`Degraded` on a best-effort basis when the existing manifest root can first be
validated; it never reseals altered manifest payload as part of handling a root
mismatch. If that emergency write cannot commit because of lock contention,
read-only media, a full disk, I/O failure, or process loss, the current process
still remains fail-closed but the manifest may retain its prior state. Because
startup does not yet scan every application-data page, operators must not use a
restart as repair after any reported `DataCorruption`; whole-shard corruption
drills and failure handling remain issue #68. Those storage failures retain
their own error kinds and do not themselves justify rebaselining data.

Manifest digest version 6 is a full 32-byte, unkeyed BLAKE3 digest. The stream
begins with `briskdb.manifest.semantic-root.v6` plus its terminating NUL. It
then encodes the length-prefixed name `application_id` and its tagged integer,
followed by the length-prefixed name `user_version` and its tagged integer.
These tables and columns follow in fixed order:

| Table | Canonical columns |
| --- | --- |
| `briskdb_manifest` | `singleton`, `shard_count` |
| `briskdb_metadata` | `requires_manifest_version` |
| `briskdb_routing` | `singleton`, `hash_version`, `key_encoding_version`, `bucket_algorithm_version`, `virtual_bucket_count`, `map_generation` |
| `briskdb_physical_shards` | `shard_id`, `lifecycle_state` |
| `briskdb_allocation_owners` | `owner_slot`, `physical_shard_id`, `owner_state` |
| `briskdb_virtual_buckets` | `bucket_id`, `physical_shard_id` |
| `briskdb_logical_databases` | `database_id`, `database_name` |
| `briskdb_schema_catalog` | `singleton`, `identifier_encoding_version`, `schema_generation`, `default_database_id` |
| `briskdb_tables` | `table_id`, `database_id`, `table_name`, `placement`, `shard_key_column`, `shard_key_type` |
| `briskdb_generated_ids` | `table_id`, `policy`, `generated_column`, `encoding_version`, `activation_state` |
| `briskdb_generated_table_ddl` | `singleton`, `logical_id`, `logical_digest_version`, `source_dialect`, `translation_version`, `source_sql`, `physical_migration_id`, `physical_sql`, `database_id`, `table_name`, `generated_column`, `generated_policy`, `generated_encoding_version`, `lifecycle_state`, `provisioning_id`, `provisioning_schema_digest`, `table_id` |
| `briskdb_global_indexes` | `index_id`, `table_id`, `index_name`, `is_unique`, `null_semantics`, `predicate_sql`, `lifecycle_state`, `key_encoding_version`, `schema_generation`, `topology_kind`, `topology_version`, `partition_count` |
| `briskdb_global_index_parts` | `index_id`, `ordinal`, `source_kind`, `source_text`, `key_type`, `sort_order`, `null_order`, `collation_version` |
| `briskdb_hilo_leases` | `table_id`, `block_size`, `next_sequence`, `fence_token`, `last_owner_id`, `last_first_sequence`, `last_last_sequence` |
| `briskdb_table_provisioning` | `singleton`, `provisioning_id`, `digest_version`, `schema_digest_version`, `committed_schema_digest`, `shard_count`, `declaration_count`, `next_shard` |
| `briskdb_table_provisioning_declarations` | `provisioning_singleton`, `ordinal`, `database_id`, `table_name`, `placement`, `shard_key_column`, `shard_key_type`, `generated_policy`, `generated_column`, `generated_encoding_version` |
| `briskdb_shard_layout` | `singleton`, `layout_id`, `shard_application_id`, `shard_metadata_version`, `layout_state` |
| `briskdb_schema_migrations` | `target_generation`, `source_generation`, `migration_id`, `digest_version`, `sql_text`, `shard_count`, `migration_state`, `next_shard` |
| `briskdb_integrity` | `singleton`, `manifest_digest_version`, `schema_digest_version`, `database_state`, `committed_schema_digest`, `target_schema_digest` |

Rows use ascending primary-key or singleton order; the metadata fence uses
`rowid` order. The encoding includes table and column names with unsigned
64-bit little-endian byte lengths. Each table starts with `0x10`, then its name,
unsigned 64-bit little-endian column count, and column names. Each row starts
with `0x11`; `0x12` ends a table and `0xff` ends the stream. Values are tagged
by SQLite storage class: `0` is null with no payload, `1` is a signed 64-bit
little-endian integer, and `2`/`3` are text/blob followed by their byte length
and exact bytes. A real value is invalid for this manifest. The stored
`manifest_digest` column itself is the sole omitted field, avoiding
self-reference; its version, database state, and schema digest fields are
covered. Frozen SQL definitions, STRICT flags, indexes, and foreign keys are
validated separately rather than encoded as semantic rows.

Every BriskDB-owned v13 manifest mutation recalculates the root after its row
changes and before the same transaction commits. Progress acknowledgement,
migration publication/finalization, provisioning intent/progress/finalization,
generated-table bridge transitions, layout publication, owner/activation state
changes, catalog changes, and each hi/lo block reservation
therefore cannot commit with a stale root through supported code. Hashing
semantic values instead of the raw SQLite file makes the root stable across WAL
checkpoints, page relocation, and `VACUUM`; it deliberately does not cover
SQLite page bytes, rollback journals, WAL, or shared memory.

Digest version 5 remains frozen solely to verify and migrate version-12
manifests. It uses the `briskdb.manifest.semantic-root.v5` domain and the same
encoding but omits both global-index tables. Digest version 4 remains frozen
solely to verify and migrate version-11
manifests. It uses the `briskdb.manifest.semantic-root.v4` domain and the same
encoding, covers `briskdb_hilo_leases`, but omits
`briskdb_generated_table_ddl`. Digest version 3 remains frozen solely to verify
and migrate version-10
manifests. It uses the `briskdb.manifest.semantic-root.v3` domain and the same
encoding, covers activation, owner lifecycle, and both provisioning tables, but
omits `briskdb_hilo_leases`. Digest version 2 remains frozen solely to verify
and migrate version-9 manifests. It uses the
`briskdb.manifest.semantic-root.v2` domain and the same
encoding, covers generated-ID policy and the owner-to-shard mapping, but omits
activation, owner lifecycle, and both provisioning tables. Digest version 1
remains frozen for version-7 and version-8 manifests. It uses the
`briskdb.manifest.semantic-root.v1` domain and the same encoding, but omits the
two version-9 tables. A v13 manifest must store version 6; storing an older
digest in an otherwise v13 shape is corruption, and an unsupported future
positive digest version is a failed precondition.

The frozen four-shard v8/version-1 fixture with layout ID
`000102030405060708090a0b0c0d0e0f`, committed schema fingerprint `5a` repeated
32 times, and the catalog rows in
`manifest_semantic_digest_v1_has_a_frozen_golden_vector` hashes to
`7be14b4f0af4d041799be8d219e55add133829623c2befcefb5c4dc9e9ff5ce0`.
The adjacent reverse-insertion fixture must produce the same root.

Schema digest version 1 is also a full 32-byte, unkeyed BLAKE3 digest. It begins
with `briskdb.shard.application-schema.v1` plus its terminating NUL, the
unsigned 32-bit little-endian digest version, and the unsigned 64-bit
little-endian application-schema generation. BriskDB then streams
`type`, `name`, `tbl_name`, and nullable `sql` from `main.sqlite_schema`, sorted
by those four fields with binary collation. Each object starts with `1`; every
text field uses an unsigned 64-bit little-endian byte length and exact UTF-8
bytes; nullable SQL uses `1` for present and `0` for absent; and `0` ends the
object stream. The generation binding means identical DDL at two generations
has different fingerprints.

SQLite-owned objects whose names begin with `sqlite_` (case-insensitive) and
the one exact `briskdb_shard_metadata` table are excluded. Any other reserved
object is corruption. The fingerprint includes persistent application tables,
indexes, views, and triggers exactly as SQLite records them. It excludes
`rootpage`, application row values, shard ID and layout metadata, file headers,
page layout, WAL state, and temporary schema objects. Thus it is stable across
row DML, checkpoint, reopen, and `VACUUM`, while differing for any persistent
application-schema change or generation change.

Every manifest connection first enables and reads back
`PRAGMA cell_size_check=ON`, then requires `PRAGMA main.integrity_check(1)` to
return exactly one `ok` row before manifest parsing or migration. Strict
manifest validation separately runs the foreign-key check because SQLite's
integrity check does not report foreign-key violations. Every BriskDB-opened
shard connection likewise enables and reads back `cell_size_check`. Strict
shard identity validation requires
`PRAGMA main.integrity_check('briskdb_shard_metadata')` to return exactly one
`ok` row. That shard check is intentionally scoped to the storage-owned
metadata table; startup does not claim a whole-shard application-data scan.

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

Before registration, the legacy migration batch may include main-schema DML.
After registration, BriskDB parses the exact submitted SQLite batch before
preflight and rejects row-moving `INSERT`, `UPDATE`, `DELETE`, `MERGE`, or
`TRUNCATE`, `CREATE TABLE ... AS SELECT`, `DROP TABLE`, and `CREATE TRIGGER`.
Rollback-only postflight then rechecks the complete authoritative table set,
key affinity/nullability/collation, unique-key locality, conservative
foreign-key co-location, and the ban on triggers and virtual tables. Parsing never replaces
the exact submitted bytes used for migration identity.

### Routing catalog

The routing singleton contains exactly these generation-1 values:

| Field | Value | Contract |
| --- | --- | --- |
| `hash_version` | `1` | BLAKE3 of the canonical key bytes, using digest bytes `0..8` as an unsigned little-endian `u64` |
| `key_encoding_version` | `1` | Canonical bytes defined below for raw, explicit, and typed inferred routing keys |
| `bucket_algorithm_version` | `1` | Compatibility-preserving range algorithm below |
| `virtual_bucket_count` | `4096` | Fixed virtual bucket space `0..4095` |
| `map_generation` | `1` | Initial committed bucket map and the only generation version 13 can interpret |

Every bucket ID exists exactly once and references an active physical shard.
Every physical shard owns at least one bucket. The generation-1 map partitions
the 4,096 bucket IDs into contiguous ranges whose sizes differ by at most one.
For initial shard count `N`, shard `s` owns the range beginning at
`s * base + min(s, extra)`, where `base = 4096 / N` and
`extra = 4096 % N`.

Key encoding version 1 preserves every raw `Database`/`Engine` routing key and
every explicit bound-plan routing key exactly as supplied. The bound statement
planner converts inferred `Int64` keys to their shortest signed base-10 ASCII
form, inferred `Text` keys to exact UTF-8 without Unicode normalization or case
conversion, and inferred `Binary` keys to exact bytes. The encoding adds no
type, logical-database, table, or column prefix. These typed rules define input
to the already persisted version-1 hash. Routing policy compares and
deduplicates only the physical shard IDs produced by that persisted catalog;
it does not rewrite or persist key bytes. Issues #23 and #24 add no manifest
version, encoding, bucket-map, shard-file, or migration change. The complete
planning and policy contract is in [bound statement
planning](SQL_PLANNING.md).

This routing format is intentionally distinct from the tagged, order-preserving
[canonical global-index key format](INDEX_KEY_ENCODING.md). Version 13 records
the codec version in every global-index definition but does not persist physical
index entries or change shard placement. Physical entries live in the separate
storage-version-3 `global-indexes/global.sqlite` authority, never in a shard.

### Physical global-index storage version 3

The selected `SharedSqliteV1` layout is one real regular file at
`global-indexes/global.sqlite`. Its `application_id` is `0x42524749`, its
`user_version` is `3`, its journal mode is `WAL`, and writers use
`synchronous=FULL`. The build tables and ownership rules are listed
in [offline global-index construction](GLOBAL_INDEX_BUILD.md).

One build row binds the index ID to a BLAKE3 digest of its complete manifest
definition, schema generation, shard count, and build state. Checkpoints form
exactly one contiguous source-shard prefix. Each checkpoint contains the
qualifying row count, unique-reservation count, and a domain-separated digest
of canonical key plus versioned physical locator in locator order. Entries use
`(index_id, encoded_key, source_shard, source_locator)` as their primary key and
retain a unique per-shard scan ordinal so validation can replay digest order;
unique reservations use `(index_id, encoded_key)`.

Version 2 adds a shared operation journal, atomic unique-key locks and mutation
records, per-index integer sequence heads, and irrevocable range leases.
Sixteen-byte operation IDs plus request digests make exact retries idempotent.
Unique reserve/finalize/rollback transactions and value lease state transitions
are described in [global uniqueness and value authority](GLOBAL_INDEX_AUTHORITY.md).
Startup upgrades version 1 in one transaction only after acquiring sole-process
ownership.

Version 3 adds `briskdb_global_index_read_repairs`. Its tuple identity is the
index ID, canonical key, source shard, and stable source locator. Repair kind
distinguishes missing rows, changed keys, and malformed locators; state moves
idempotently from queued to applied, while a saturating observation count makes
repeated staleness observable without growing one record per read. Applied
tombstones hide only matching non-unique candidate entries. They never alter
unique-key ownership or the base build/checkpoint digest. One plan reads at
most 4,096 candidates and queues at most 64 repairs. Startup upgrades either
version 1 or 2 under the same sole-process fence.

Source-shard entries and their checkpoint commit together. Resume re-hashes
every completed prefix shard and restarts from zero if application data changed.
After a second full digest pass and exact count validation, the builder commits
physical `Complete`, truncates the WAL, synchronizes the file and parent
directory, and then publishes manifest lifecycle `Ready` in its existing
checksummed transaction. Thus physical completion always precedes visibility.
Removal is legal only after `Dropping` and deletes physical rows before removing
the unavailable manifest definition.

Validation first commits manifest lifecycle `Rebuilding`, then compares source
rows with physical entries in bounded memory. Full validation streams both
sides in source-ordinal order; sampled validation uses deterministic,
evenly-distributed ordinals per shard. Findings publish `Invalid`. Non-unique
repair replaces complete affected-shard entry/checkpoint sets transactionally.
Unique indexes reject repair and require a replacement build, so authoritative
ownership is never inferred. Physical completion and synchronization precede
the final manifest publication to `Ready`; cancellation or process loss leaves
`Rebuilding` and no partial index is eligible for use. The operational contract
is documented in [global-index recovery](GLOBAL_INDEX_RECOVERY.md).

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
`schema_generation`. Version 13 accepts only routing generation 1 and validates
its exact deterministic assignment; no public map-mutation operation exists
yet. A future format that can commit a changed map must bump `user_version` and
its downgrade fence as well as `map_generation`. That requirement makes this
pre-lookup binary reject the manifest instead of silently using legacy modulo
routing against a remapped catalog.

At each open, BriskDB validates the exact objects, columns, strict flags, frozen
schema SQL, singleton rows, logical identifiers and limits, metadata codes,
supported algorithm values, contiguous physical and bucket IDs, active
lifecycle states, assignments, coverage, and foreign keys. A recognized
version-13 manifest that violates any invariant is `DataCorruption` and is
rejected before shard connections are opened. The same locked transaction
returns routing and logical rows as one coherent shared snapshot. Request
routing performs no manifest query and cannot fall back to modulo after a failed
validation; only successful migration finalization publishes a newer logical
schema generation into the snapshot.

## Previous version 12

Version 12 has seventeen strict manifest tables and semantic manifest digest
version 5. It includes the retained generated-table DDL bridge but has no
global-index catalog. Its downgrade fence requires 12 and its header stores
`user_version = 12`.

The atomic v12-to-v13 transaction creates both empty global-index tables,
replaces the downgrade fence with 13, changes the integrity row to manifest
digest version 6, stamps `user_version = 13`, and reseals the version-6 root.
It preserves routing, logical tables, generated-ID and DDL state, migration
history, schema generations, layout, integrity, shard files, schemas, and rows.
The migration never opens or mutates a shard. A version-12 reader rejects the
version-13 header and fence before it could ignore global-index authority.

## Previous version 11

Version 11 has sixteen strict manifest tables. It includes durable
`hilo_v1` allocation heads and semantic manifest digest version 4, but has no
`briskdb_generated_table_ddl` bridge. Its downgrade fence requires 11 and its
header stores `user_version = 11`.

The atomic v11-to-v12 transaction creates the empty generated-table DDL bridge,
replaces the downgrade fence with 12, changes the integrity row to manifest
digest version 5, stamps `user_version = 12`, and reseals the version-5 root.
It preserves routing, placement, generated-ID policies and activation, owner
lifecycle, hi/lo heads, valid table-provisioning and schema-migration state,
schema generations and history, layout and integrity state, schema
fingerprints, shard files, and application rows. The migration never opens or
mutates a shard. A version-11 reader rejects the version-12 header and downgrade
fence before it could ignore the retained DDL request, its lifecycle, or its
three linked identities.

## Previous version 10

Version 10 has fifteen strict tables. It has activation state, explicit owner
lifecycle, and both provisioning tables, but no `briskdb_hilo_leases` table and
no supported `hilo_v1` policy. Its downgrade fence requires 10 and it stores
semantic manifest digest version 3.

The atomic v10-to-v11 transaction creates the empty hi/lo lease table, replaces
the downgrade fence with 11, changes the integrity row to manifest digest
version 4, stamps `user_version = 11`, and reseals the version-4 root. It
preserves routing, placement, every existing `None` or native generated-ID
policy and activation, owner lifecycle, provisioning state, schema generation
and history, layout and integrity state, schema fingerprints, shard files, and
application rows. The migration never opens or mutates a shard. Because no v10
policy can already be hi/lo, the new lease table is necessarily empty. A
version-10 reader rejects the version-11 header and downgrade fence before it
could ignore durable global allocation state.

## Previous version 9

Version 9 has thirteen strict tables. Its generated-ID table has policy,
generated-column, and encoding fields but no independent `activation_state`.
Its owner table maps each physical shard to one unique slot but has no
`owner_state`. It has neither table-provisioning table, requires downgrade fence
9, and stores semantic manifest digest version 2.

The atomic v9-to-v10 transaction rebuilds every generated-ID row with
`activation_state = 0`, preserving its exact policy metadata without treating
prior cross-file state as proven active. It rebuilds every owner row as active,
adds the partial one-active-owner-per-shard index, creates both empty
provisioning tables, replaces the downgrade fence with 10, changes the integrity
row to manifest digest version 3, stamps `user_version = 10`, and reseals the
version-3 root. Routing, placement, schema generation and history, layout and
integrity state, schema fingerprints, shard files, and application rows are
preserved. The transaction never opens or mutates a shard.

A migrated native policy remains usable for policy-aware classification and
explicit key routing, but generated allocation remains disabled until the exact
declaration set is safely reprovisioned through the v10 journal. This avoids
asserting that an old manifest transaction proved all shard-local allocator
floors durable. A version-9 reader rejects the version-10 header and downgrade
fence before it could ignore activation, retired-owner routing, or an active
provisioning journal.

## Previous version 8

Version 8 has the eleven manifest tables retained from version 7. It has no
`briskdb_generated_ids` or `briskdb_allocation_owners` table, its exact
downgrade fence requires version 8, and it stores semantic manifest digest
version 1. Its authoritative `briskdb_tables` rows have no generated-ID field;
every such table therefore migrates as explicit `GeneratedIdPolicy::None`.

The atomic v8-to-v9 transaction creates one `None` policy row for every table,
creates one allocation-owner row for every active physical shard with
`owner_slot = physical_shard_id`, replaces the downgrade fence, changes the
integrity row to manifest digest version 2, stamps `user_version = 9`, and
reseals the version-2 semantic root. It preserves all table placement and shard
keys, routing and map generation, logical database and schema generation,
migration history and progress, layout and durable integrity state,
application-schema fingerprints, shard files, and application rows. All of
those manifest changes commit or roll back together; the migration neither
opens nor mutates a shard. A version-8 reader rejects the version-9 header
before it could ignore generated-ID policy or owner-slot authority.

## Previous version 7

Version 7 has the same eleven table roles as version 8 and introduced the
integrity singleton, semantic manifest root, durable database states, and
generation-bound shard-schema fingerprints. Its exact downgrade fence requires
manifest version 7, contains exactly `7`, and `PRAGMA user_version` is `7`.

Its `briskdb_tables` rows were explicitly advisory: version 7 exposed no safe
registration operation and did not prove those rows against the physical
application schema. The atomic v7-to-v8 manifest migration therefore deletes
all such rows, replaces the fence with the exact version-8 definition and row,
stamps version 8, reseals the semantic root, validates the complete destination,
and commits those changes together. It does not change any logical-database, routing, schema
generation/history, layout, integrity, shard-schema, or application-row data.
A version-7 reader rejects the version-8 header before it can interpret an
authoritative table row as advisory.

## Previous version 6

Version 6 has the first ten current tables and no `briskdb_integrity` table.
Its exact downgrade fence requires manifest version 6, contains exactly `6`,
and `PRAGMA user_version` is `6`. It introduced the retained application-schema
migration journal but had no trusted manifest or shard-schema checksum and no
durable database integrity state.

A current opener first performs full v6 structural validation. If v6 has an
`Applying` journal row, startup completes that exact ascending source/target
prefix under the frozen v6 recovery rules while the manifest is still version
6. Only after the row is `Complete` and its target generation is committed may
the manifest-only v6-to-v7 transaction replace the downgrade fence, add the
integrity singleton in `Verifying`, and stamp version 7. A crash before that
upgrade remains recoverable by v6 rules; a crash after it restarts from the
durable v7 `Verifying` state.

Because v6 stored no application-schema checksum, this first v7 open is an
explicit trust-on-first-upgrade boundary. BriskDB strictly validates every
shard and requires all generation-bound schema fingerprints to agree, then
stores that consensus as the first trusted committed fingerprint and moves to
`Ready` atomically. Divergent shards fail closed; matching fingerprints prove
consensus at upgrade time, not that the pre-v7 schema has an authenticated
history.

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

The fence contains exactly `5`, and `PRAGMA user_version` is `5`. A current
opener fully validates the v5 manifest, then atomically rebuilds only the
schema-catalog table, creates the empty migration journal, replaces the fence,
and stamps manifest version 6. It preserves the layout ID and state, routing,
logical databases, table-catalog rows, and every application table and row.
The next manifest-only step establishes v7 `Verifying`. An unfinished v5
`Creating` or `Adopting` layout is then reconciled exactly as before. These
upgrades create no active migration. An active journal found in an already
existing v6 manifest can exist only beside a durable `Ready` layout and is
finished under v6 rules before the v7 step.

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
before applying the numbered v2-to-v3, v3-to-v4, v4-to-v5, v5-to-v6,
v6-to-v7, v7-to-v8, v8-to-v9, v9-to-v10, v10-to-v11, v11-to-v12, and v12-to-v13
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
   following and a finite busy timeout. Enable and read back
   `cell_size_check`, require a clean full manifest `integrity_check`, then
   configure `synchronous=FULL` and foreign keys before interpreting manifest
   state.
3. If the file is version 6 with one `Applying` migration, validate and finish
   its exact source/target prefix under v6 rules while it is still version 6.
   Do not establish v7 checksum authority over a partial v6 migration.
4. Acquire `BEGIN IMMEDIATE`, then read identity, version, exact schema,
   invariant rows, foreign keys, and the semantic root under that write lock.
   Reject a foreign file, a newer version, a requested shard-count mismatch, a
   stale root, or another invalid recognized state before changing it.
5. Apply each compile-time registered manifest migration with static SQL and
   bound data. Stamp the destination application ID and `user_version` only
   after the schema/data change, then refresh the version-selected semantic
   root, validate the complete destination, and commit one numbered step at a
   time. The v6-to-v7 transaction adds an integrity row in `Verifying`; the
   v7-to-v8 transaction clears advisory table rows and installs the
   authoritative-catalog downgrade fence; v8-to-v9 installs explicit
   generated-ID policies, allocation-owner slots, and semantic digest version
   2; and v9-to-v10 installs activation/lifecycle state, the provisioning
   journal, and semantic digest version 3. The v10-to-v11 step adds durable
   hi/lo allocation heads and semantic digest version 4. The v11-to-v12 step
   adds the retained generated-table DDL bridge and semantic digest version 5;
   v12-to-v13 adds the global-index catalog and semantic digest version 6.
   Older formats therefore cannot be mistaken for checksummed,
   authoritative-catalog, allocator-authority, recoverable provisioning, or
   durable logical-to-physical DDL identity.
6. Fresh initialization is allowed only beside an otherwise empty physical
   layout and commits v13 physical state `Creating`, integrity state
   `Verifying`, generation 0, and empty application-schema and provisioning
   journals. An existing
   v1/v2/v3 manifest first advances through v4; the v4-to-v5 transaction commits
   a random 16-byte layout ID and physical state `Adopting` before any shard
   file changes. The v5-to-v6 step creates the journal, v6-to-v7 establishes
   the unsealed integrity row, and v7-to-v8 establishes an empty authoritative
   table catalog. The v8-to-v9 step adds the empty generated-policy catalog and
   immutable owner map; v9-to-v10 adds inactive activation fields, active owner
   states, and empty provisioning tables; v10-to-v11 adds the empty hi/lo lease
   table; v11-to-v12 adds the empty generated-table DDL bridge; and v12-to-v13
   adds the empty global-index catalog.
7. If the validated integrity state is `Degraded`, fail startup without
   changing it. Otherwise, if a validated v13 manifest contains one `Applying`
   migration, require state `Migrating`, validate every shard against the
   trusted source/target fingerprint for its exact journal-prefix position,
   and resume it in ascending order. The final transaction publishes the
   target catalog generation, target fingerprint, `Complete`, `Ready`, and a
   refreshed root before ordinary layout reconciliation continues.
8. Acquire a new `BEGIN IMMEDIATE`, re-read and validate the committed layout
   identity and state, and reconcile every expected physical ID in ascending
   order while retaining the lock. Only `Creating` may create a missing file
   and enable WAL. `Adopting` requires an existing WAL file and accepts either
   the exact legacy zero/zero header or resumable current metadata. `Ready`
   accepts only the exact current format. Re-scan for unexpected canonical
   shard filenames, strictly revalidate every expected file, publish `Ready`
   only after full validation, and commit.
9. If the manifest retains a generated-table DDL bridge, keep startup admission
   `Pending`. A completed physical migration permits `ApplyingPhysical` to move
   to `Provisioning`; create or validate the matching transient provisioning
   journal from the retained declaration, then resume its exact `next_shard`
   prefix. Re-seed an unacknowledged shard idempotently and acknowledge only a
   committed seed. After every shard is durable, one manifest transaction
   activates the native policy, clears the transient journal, stores the stable
   table ID, seals the bridge `Complete`, and publishes the catalog. A bridge
   already `Complete` must have no active provisioning journal.
10. Otherwise, if the manifest contains a standalone active table-provisioning
    journal, keep startup admission `Pending`, verify its exact identity and
    complete empty declaration set, and resume from `next_shard`. Re-seed the
    first unacknowledged shard idempotently, acknowledge only committed shards,
    then atomically activate the native policies, clear the transient journal,
    and publish the complete catalog only after every shard is durable.
11. Open every shard once more with read-write, no-create, no-follow flags;
   enable cell-size checking; and validate identity, generation, WAL, the
   metadata-table integrity check, and the generation-bound application-schema
   fingerprint. `Verifying` requires one consensus across all shards. `Ready`
   requires the existing trusted fingerprint.
12. In one manifest transaction, seal a first consensus as the committed
    fingerprint, transition `Verifying` to `Ready`, and refresh the root.
    Reconcile the catalog generation with other live handles for the canonical
    root, publish the startup gate `Ready`, and only then return `Storage`,
    `Database`, or `Engine`.

SQLite transactional DDL keeps each numbered manifest step atomic within
`manifest.sqlite`. A version-1 upgrade commits versions 2 through 12; a
version-2 upgrade begins at 3, and so on. The v3-to-v4 step
still creates the
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
schema generation 0 and has no journal history. The v6-to-v7 step also changes
no shard; it establishes a first fingerprint only after later cross-file
consensus verification. The v7-to-v8 step changes no shard and clears advisory
`briskdb_tables` rows rather than inferring or blessing declarations from
physical DDL. The v8-to-v9 step likewise changes no shard: it records every
existing authoritative table as `None`, seeds owner slots from immutable
physical shard IDs, and changes the manifest checksum domain.
The v9-to-v10 step also changes no shard: it preserves every policy but marks
it inactive, marks existing owners active, creates empty provisioning tables,
and moves the checksum to version 3. Any later native activation is a distinct
recoverable provisioning operation, not part of the format migration.
The v10-to-v11 step likewise changes no shard: it creates an empty hi/lo lease
table and moves the checksum to version 4. Existing policies, activation,
owner state, provisioning progress, schema, and application rows are unchanged.
The v11-to-v12 step also changes no shard: it creates the empty retained
generated-table DDL bridge and moves the checksum to version 5. Existing
policies, allocator state, provisioning progress, migration history, schema,
and application rows are unchanged.
The v12-to-v13 step likewise changes no shard: it creates the empty global-index
catalog, raises the downgrade fence, and moves the checksum to version 6.

A new application-schema migration follows a separate durable protocol:

1. Acquire the sole in-process schema-migration gate, stop admitting ordinary
   operations, and wait for operations already admitted to finish.
2. Validate the SQL identity and limits. Require every shard's source schema to
   match the committed fingerprint, then execute the complete batch inside a
   rollback-only immediate transaction on every shard. Each preflight computes
   the generation-bound target fingerprint and runs
   `PRAGMA main.foreign_key_check`; every shard must produce the same target.
   If any preflight fails or the request is cancelled, no journal or shard
   change is retained and ordinary work becomes ready again.
3. In one manifest transaction, append one `Applying` journal row at target
   generation `source_generation + 1` with `next_shard = 0`, store the target
   fingerprint, transition `Ready` to `Migrating`, and refresh the manifest
   root.
4. For each shard in ascending physical-ID order, execute the complete SQL
   batch, stamp its target `user_version`, and require the target fingerprint in
   the same immediate SQLite transaction. After that shard commits, advance the
   journal prefix and refresh the root in one manifest transaction.
5. Strictly validate every shard at the target generation and fingerprint.
   Then, in one final manifest transaction, mark the journal row `Complete`,
   advance the schema-catalog generation, promote the target fingerprint to
   committed, clear the target, transition to `Ready`, and refresh the root.
   Publish the new in-memory generation and admit ordinary work again.

There is deliberately no transaction spanning shard files and the manifest.
A crash after a shard commit but before its journal acknowledgement leaves the
durable prefix one position behind. Recovery permits exactly that one
already-target shard at the boundary, requires its target fingerprint, and
advances without executing the batch twice. Every earlier shard must match the
target generation and fingerprint; every later shard must match the source.
Any other hole, regression, generation, or checksum mismatch is corruption and
fails closed. A byte-identical public retry resumes an active row or validates
and returns an already completed row without creating another generation.

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

If checksum or structural validation fails, the canonical-root gate becomes
sticky `Degraded`, so a migration guard's later drop cannot reopen admission.
When the semantic manifest root was still trustworthy, BriskDB records durable
state `Degraded` without discarding its committed/target fingerprints or active
journal. That state is terminal for integrity-checksummed formats (v7 and
later): startup never clears it or chooses a new baseline. This is deliberately
conservative because a runtime SQLite
corruption result can originate in application-row pages that the schema
fingerprint does not cover, while whole-shard scans are deferred.

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

- Version 1 upgrades through versions 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, and 12;
  later versions begin at the next numbered transaction. Opening is therefore
  potentially mutating. An active v6 journal is completed before its v7
  transaction.
- There is no automatic downgrade. Older readers are fenced by the incompatible
  metadata schema and authoritative header. In particular, a version-11 reader
  rejects `user_version = 12` before it can ignore the retained generated-table
  DDL request, lifecycle, or linked identities; a version-10 reader
  rejects `user_version = 11` before it can ignore durable hi/lo allocation
  heads; a version-9 reader
  rejects `user_version = 10` before it can ignore policy activation, owner
  lifecycle, or table provisioning; a version-8 reader
  rejects `user_version = 9` before it can ignore generated-ID policy and owner
  mappings, a version-7 reader
  rejects `user_version = 8` before it can treat authoritative table rows as
  advisory, and a version-6 reader rejects version 7 before it can ignore
  integrity state.
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
  security mechanism.
- BriskDB owns the shard identity table and the persistent `application_id`,
  `user_version`, and `journal_mode` settings. Client SQL access to the identity
  table and mutation of those settings is denied. `user_version` is the
  application-schema generation, not SQLite's internal `schema_version`.
- SQLite lock contention remains retryable `Busy`; permission, read-only, full,
  and I/O failures retain the storage error taxonomy.

Issue #25's SQL translation API is an in-memory, opt-in SQL-layer operation. It
does not change the manifest version, shard files, stored schema text, migration
digest, or migration identity by itself. The general `broadcast` migration path
continues to retain and compare the caller's exact submitted physical SQL.
Version 12's generated-table DDL bridge is the explicit composed exception: it
retains the exact logical source and its identity separately, then uses the
exact canonical compatibility output as the physical migration text and
identity.

Issue #26's prepared statements, descriptions, bound portals, captured routing
bytes, and transient plans are also process-memory state. Portals retain no
plan. These objects add no manifest or shard table, header value, routing
version, schema fingerprint, journal record, or recovery step.
Prepare/describe do not write application data. A supported portal command has
only its ordinary one-shard SQLite row effects; persistent schema changes
remain exclusive to the exact-text journaled migration path.

Issue #27's `StatementBatchClassification`, nested behavior enums, and behavior
retained by plans/descriptions are likewise process-memory analysis metadata.
They add no manifest or shard table, format version, header value, routing/key
encoding, virtual-bucket map, schema fingerprint, journal record, CLI setting,
or restart-recovery step. The raw migration endpoint keeps its separate
parameterless schema-batch contract: it still digests and retains the caller's
exact submitted SQL and does not substitute classified, normalized, or
translated text as migration identity.

Issue #28's optional PostgreSQL socket address and accept/close listener were
process-configuration changes only. They added no manifest or shard table,
header value, format version, digest input, routing metadata, schema
fingerprint, journal record, or recovery step. Listener settings are not
persisted. Because engine open and its existing recovery precede listener
binding, a later bind failure does not undo a migration or recovery transaction
that already committed; a subsequent startup revalidates the same version-13
layout normally.

Issue #29's pinned `pgwire` dependency and issue #30's production startup,
`protocol::postgres::Adapter`, selected identity, and per-connection core
`Session` are also process-only code and memory state. They add no manifest or
shard table, header value, format version, digest input, routing metadata,
schema fingerprint, journal record, file, or recovery step. Startup performs a
read-only engine-status operation; current wire queries are rejected before
preparation, routing, or SQLite execution. The historical private probe's
prepared metadata is transient and explicitly closed in tests. None of these
paths writes application rows or schema.

The checksums are corruption detectors, not authentication. Both use unkeyed
BLAKE3 and are writable by anyone who can modify the data directory. The
manifest root covers canonical control-plane values, not raw SQLite pages. The
shard fingerprint covers persistent application-schema definitions, not
application row values. The SQLite integrity and cell checks describe what was
read at validation time; they are not continuous monitoring, and startup's
table-scoped shard check is not a whole-data integrity scan. Whole-shard data
scans, authenticated storage, online repair, and backup orchestration remain
outside this format milestone.

Do not edit headers, manifest rows, integrity state, or checksum values
manually. There is no supported command that rebaselines a damaged database.
For a degraded or rejected root, stop every BriskDB process using it, preserve
the complete current directory for diagnosis, and restore the exact known-good
manifest and shard contents from a consistent copy made while BriskDB was
stopped. Do not copy an individual shard from another layout, mix backup times,
or rewrite a checksum to match observed contents. The restored set must include
the known-good manifest, rather than the terminal `Degraded` manifest from the
failed root. A corrupt semantic root likewise must be restored, because
BriskDB will not sign altered manifest payload while reporting the failure.

The deployment boundary remains one host and local storage. Independent
processes hold shared root leases for steady-state operations; schema, catalog,
initialization, upgrade, and recovery work requires a sole-process lease.
In-process handles additionally share a gate, schema fingerprints, and live
catalog publication keyed by the canonical root. This coordination is not a
distributed lock and does not support NFS, SMB, shared multi-host volumes, or
object storage. The supported recovery workflow is the complete
stopped-directory copy documented in [offline backup](OFFLINE_BACKUP.md);
coordinated online backup remains unimplemented. Recovery to an older binary
requires a backup from before the unsupported format.

## Verification contract

Tests cover fresh creation and every v1/v2/v3/v4/v5/v6/v7/v8/v9/v10/v11/v12
upgrade path to v13, every
manifest layout and integrity state, the exact shard header and metadata row,
dynamic schema generations, retained migration history, checksum golden
vectors, and no-op ready reopen. Failure
coverage includes missing and extra canonical files, foreign headers, non-WAL
mode, malformed metadata or journal rows, cross-layout shard clones, shard
swaps, symlinks, wrong generations, manifest-root and shard-schema mismatches,
client attempts to reach storage-owned state, and runtime pool replacement
opens. Injection around each manifest,
layout, shard-migration, journal-progress, and finalization persistence boundary
proves resumability and prevents a ready layout or completed generation from
being published before full strict revalidation. Concurrent openers exercise
matching and conflicting shard counts and layout transitions.

Catalog tests continue to cover default logical metadata, every placement and
shard-key type code, every generated-ID policy, activation, and encoding
boundary, legacy/native/hi-lo classification, active and retired allocation-owner
coverage, identifier and relational corruption, row limits, v7 advisory-row
clearing, v8 explicit `None` migration, v9 inactive-policy migration, atomic
registration and commit ambiguity, provisioning-prefix replay at every commit
boundary, exact idempotency, empty-schema and key/constraint validation,
immutable public lookups, stale-handle exclusion, and migration enforcement.
Generated-table DDL bridge tests cover exact logical, physical, and
provisioning identity; the retained provisioning-time schema digest and a later
schema migration; manifest-v5 checksum and downgrade fencing; strict lifecycle
cross-validation; conflicting retry; atomic physical-journal begin; and
subprocess abort/reopen recovery across physical migration, provisioning intent
and prefix, and final catalog publication.
Hi/lo tests cover one manifest write per block, restart, process abort before
and after durable reservation, rollback, cancellation, constraint failure,
exhaustion, fence overflow, clock-independent schema, manifest contention, and
competing process incarnations. They assert uniqueness across shards and
processes, burned abandoned tails, and non-reuse.
Routing tests
freeze golden hash/bucket/shard vectors,
cover every algorithm state for every supported shard count, prove the final
map lookup with a synthetic reassignment, and exercise parallel and reopen
stability. Schema-migration tests cover exact identity and input limits,
byte-identical retry, preflight rollback, per-shard atomicity, gate states,
cancellation, panic, target-fingerprint consensus, checksummed prefix recovery,
sticky degradation, exact restoration, and targeted process abort at durable
boundaries.
Arbitrary process termination and filesystem faults remain part of the later
storage-hardening suite. Every new manifest-format migration must extend the
registry, destination validator, format documentation, and the same
normal/error/concurrency/recovery matrix.
