//! Version detection and transactional upgrades for `manifest.sqlite`.

use rusqlite::{Connection, Transaction, TransactionBehavior, types::ValueRef};

#[cfg(test)]
use std::cell::Cell;

#[cfg(all(test, feature = "experimental-vtab"))]
use crate::core::generated_id::AllocationOwnerSlot;
use crate::core::generated_id::AllocationOwnerState;
use crate::{
    core::{
        AllocationOwnerMap, BUCKET_ALGORITHM_VERSION, Catalog, CatalogSnapshot,
        DEFAULT_LOGICAL_DATABASE_ID, DEFAULT_LOGICAL_DATABASE_NAME, EngineError, EngineErrorKind,
        EngineResult, GeneratedIdPolicy, HASH_VERSION, IDENTIFIER_ENCODING_VERSION,
        INITIAL_MAP_GENERATION, KEY_ENCODING_VERSION, LogicalDatabaseId, LogicalDatabaseMetadata,
        MAX_LOGICAL_DATABASES, MAX_TABLES, RoutingCatalog, ShardKeyMetadata, ShardKeyType,
        TableDeclaration, TableId, TableMetadata, TablePlacement, VIRTUAL_BUCKET_COUNT,
        initial_physical_shard, validate_catalog_identifier,
    },
    sql::SqlDialect,
    sqlite_error,
};

use super::{
    hilo::DurableHiloLease,
    shard::{SHARD_APPLICATION_ID, SHARD_METADATA_VERSION, ShardLayout, ShardLayoutState},
};

/// `BRDB` encoded as SQLite's 32-bit application identifier.
pub(super) const MANIFEST_APPLICATION_ID: i64 = 0x4252_4442;
const LEGACY_SCHEMA_VERSION: u32 = 1;
const V2_SCHEMA_VERSION: u32 = 2;
const V3_SCHEMA_VERSION: u32 = 3;
const V4_SCHEMA_VERSION: u32 = 4;
const V5_SCHEMA_VERSION: u32 = 5;
const V6_SCHEMA_VERSION: u32 = 6;
const V7_SCHEMA_VERSION: u32 = 7;
const V8_SCHEMA_VERSION: u32 = 8;
const V9_SCHEMA_VERSION: u32 = 9;
const V10_SCHEMA_VERSION: u32 = 10;
const V11_SCHEMA_VERSION: u32 = 11;
const V12_SCHEMA_VERSION: u32 = 12;
pub(super) const CURRENT_SCHEMA_VERSION: u32 = V12_SCHEMA_VERSION;
const MAX_TABLE_SQL_BYTES: i64 = 4_096;

pub(super) const MAX_SCHEMA_MIGRATION_SQL_BYTES: usize = 65_536;
pub(super) const MAX_SCHEMA_GENERATION: u64 = i32::MAX as u64;
const SCHEMA_MIGRATION_DIGEST_VERSION: u32 = 1;
const SCHEMA_MIGRATION_APPLYING: i64 = 1;
const SCHEMA_MIGRATION_COMPLETE: i64 = 2;
const TABLE_PROVISIONING_DIGEST_VERSION: u32 = 1;
const V1_MANIFEST_DIGEST_VERSION: u32 = 1;
const V2_MANIFEST_DIGEST_VERSION: u32 = 2;
const V3_MANIFEST_DIGEST_VERSION: u32 = 3;
const V4_MANIFEST_DIGEST_VERSION: u32 = 4;
const V5_MANIFEST_DIGEST_VERSION: u32 = 5;
pub(super) const SCHEMA_DIGEST_VERSION: u32 = 1;
const V1_MANIFEST_DIGEST_DOMAIN: &[u8] = b"briskdb.manifest.semantic-root.v1\0";
const V2_MANIFEST_DIGEST_DOMAIN: &[u8] = b"briskdb.manifest.semantic-root.v2\0";
const V3_MANIFEST_DIGEST_DOMAIN: &[u8] = b"briskdb.manifest.semantic-root.v3\0";
const V4_MANIFEST_DIGEST_DOMAIN: &[u8] = b"briskdb.manifest.semantic-root.v4\0";
const V5_MANIFEST_DIGEST_DOMAIN: &[u8] = b"briskdb.manifest.semantic-root.v5\0";
const TABLE_PROVISIONING_DIGEST_DOMAIN: &[u8] = b"briskdb.table-provisioning.v1\0";
const GENERATED_TABLE_DDL_DIGEST_DOMAIN: &[u8] = b"briskdb.generated-table-ddl.v1\0";

const GENERATED_TABLE_DDL_DIGEST_VERSION: u32 = 1;
pub(super) const GENERATED_TABLE_DDL_TRANSLATION_VERSION: u32 = 1;
const GENERATED_TABLE_DDL_APPLYING_PHYSICAL: i64 = 1;
const GENERATED_TABLE_DDL_PROVISIONING: i64 = 2;
const GENERATED_TABLE_DDL_COMPLETE: i64 = 3;
const GENERATED_TABLE_DDL_SQLITE: i64 = 1;
const GENERATED_TABLE_DDL_POSTGRESQL: i64 = 2;
const GENERATED_TABLE_DDL_MYSQL: i64 = 3;

const DATABASE_STATE_VERIFYING: i64 = 1;
const DATABASE_STATE_READY: i64 = 2;
const DATABASE_STATE_MIGRATING: i64 = 3;
const DATABASE_STATE_DEGRADED: i64 = 4;

const INITIAL_SCHEMA_GENERATION: u64 = 0;
const SHARDED_PLACEMENT: i64 = 1;
const GLOBAL_PLACEMENT: i64 = 2;
const CATALOG_PLACEMENT: i64 = 3;
const INT64_SHARD_KEY_TYPE: i64 = 1;
const TEXT_SHARD_KEY_TYPE: i64 = 2;
const BINARY_SHARD_KEY_TYPE: i64 = 3;
const GENERATED_ID_POLICY_NONE: i64 = 0;
const GENERATED_ID_POLICY_NATIVE_RANGE_V1: i64 = 1;
const GENERATED_ID_POLICY_HILO_V1: i64 = 2;
const GENERATED_ID_INACTIVE: i64 = 0;
const GENERATED_ID_ACTIVE: i64 = 1;
const NATIVE_RANGE_V1_ENCODING_VERSION: u32 = 1;
const HILO_V1_ENCODING_VERSION: u32 = 1;
pub(super) const HILO_V1_BLOCK_SIZE: u64 = 4_096;
const MAX_HILO_V1_SEQUENCE: u64 = (1_u64 << 61) - 1;
const HILO_V1_EXHAUSTED_HEAD: u64 = MAX_HILO_V1_SEQUENCE + 1;
const MAX_ALLOCATION_OWNER_SLOT: i64 = 1_023;
const ALLOCATION_OWNER_ACTIVE: i64 = 1;
const ALLOCATION_OWNER_RETIRED: i64 = 2;

const ACTIVE_LIFECYCLE_STATE: &str = "active";

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_TABLE_REGISTRATION_POST_COMMIT: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
pub(super) fn fail_next_table_registration_post_commit_for_test() {
    FAIL_NEXT_TABLE_REGISTRATION_POST_COMMIT.with(|fail| fail.set(true));
}

#[cfg(test)]
fn abort_table_registration_at_test_boundary(boundary: &str) {
    if std::env::var("BRISKDB_TABLE_REGISTRATION_ABORT_POINT").as_deref() == Ok(boundary) {
        std::process::abort();
    }
}

#[cfg(test)]
fn abort_hilo_lease_at_test_boundary(boundary: &str) {
    if std::env::var("BRISKDB_HILO_LEASE_ABORT_POINT").as_deref() == Ok(boundary) {
        std::process::abort();
    }
}

const LEGACY_METADATA_TABLE_SQL: &str = "CREATE TABLE briskdb_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)";
const V2_MANIFEST_TABLE_SQL: &str = "CREATE TABLE briskdb_manifest (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    shard_count INTEGER NOT NULL CHECK (shard_count BETWEEN 2 AND 64)
) STRICT";
const V2_DOWNGRADE_FENCE_SQL: &str = "CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 2)
) STRICT";
const V3_DOWNGRADE_FENCE_SQL: &str = "CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 3)
) STRICT";
const V4_DOWNGRADE_FENCE_SQL: &str = "CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 4)
) STRICT";
const V5_DOWNGRADE_FENCE_SQL: &str = "CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 5)
) STRICT";
const V6_DOWNGRADE_FENCE_SQL: &str = "CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 6)
) STRICT";
const V7_DOWNGRADE_FENCE_SQL: &str = "CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 7)
) STRICT";
const V8_DOWNGRADE_FENCE_SQL: &str = "CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 8)
) STRICT";
const V9_DOWNGRADE_FENCE_SQL: &str = "CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 9)
) STRICT";
const V10_DOWNGRADE_FENCE_SQL: &str = "CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 10)
) STRICT";
const V11_DOWNGRADE_FENCE_SQL: &str = "CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 11)
) STRICT";
const V12_DOWNGRADE_FENCE_SQL: &str = "CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 12)
) STRICT";
const V3_ROUTING_TABLE_SQL: &str = "CREATE TABLE briskdb_routing (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    hash_version INTEGER NOT NULL CHECK (hash_version = 1),
    key_encoding_version INTEGER NOT NULL CHECK (key_encoding_version = 1),
    bucket_algorithm_version INTEGER NOT NULL CHECK (bucket_algorithm_version = 1),
    virtual_bucket_count INTEGER NOT NULL CHECK (virtual_bucket_count = 4096),
    map_generation INTEGER NOT NULL CHECK (map_generation = 1)
) STRICT";
const V3_PHYSICAL_SHARDS_TABLE_SQL: &str = "CREATE TABLE briskdb_physical_shards (
    shard_id INTEGER PRIMARY KEY CHECK (shard_id BETWEEN 0 AND 63),
    lifecycle_state TEXT NOT NULL CHECK (lifecycle_state = 'active')
) STRICT";
const V3_VIRTUAL_BUCKETS_TABLE_SQL: &str = "CREATE TABLE briskdb_virtual_buckets (
    bucket_id INTEGER PRIMARY KEY CHECK (bucket_id BETWEEN 0 AND 4095),
    physical_shard_id INTEGER NOT NULL,
    FOREIGN KEY (physical_shard_id) REFERENCES briskdb_physical_shards (shard_id)
) STRICT";
const V4_LOGICAL_DATABASES_TABLE_SQL: &str = "CREATE TABLE briskdb_logical_databases (
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
) STRICT";
const V4_SCHEMA_CATALOG_TABLE_SQL: &str = "CREATE TABLE briskdb_schema_catalog (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    identifier_encoding_version INTEGER NOT NULL CHECK (identifier_encoding_version = 1),
    schema_generation INTEGER NOT NULL CHECK (schema_generation = 0),
    default_database_id INTEGER NOT NULL CHECK (default_database_id = 1),
    FOREIGN KEY (default_database_id)
        REFERENCES briskdb_logical_databases (database_id)
) STRICT";
const V6_SCHEMA_CATALOG_TABLE_SQL: &str = "CREATE TABLE briskdb_schema_catalog (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    identifier_encoding_version INTEGER NOT NULL CHECK (identifier_encoding_version = 1),
    schema_generation INTEGER NOT NULL
        CHECK (schema_generation BETWEEN 0 AND 2147483647),
    default_database_id INTEGER NOT NULL CHECK (default_database_id = 1),
    FOREIGN KEY (default_database_id)
        REFERENCES briskdb_logical_databases (database_id)
) STRICT";
const V4_TABLES_TABLE_SQL: &str = "CREATE TABLE briskdb_tables (
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
) STRICT";
const V9_GENERATED_IDS_TABLE_SQL: &str = "CREATE TABLE briskdb_generated_ids (
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
    FOREIGN KEY (table_id)
        REFERENCES briskdb_tables (table_id)
        ON DELETE RESTRICT,
    CHECK (
        (
            policy = 0
            AND generated_column IS NULL
            AND encoding_version IS NULL
        )
        OR
        (
            policy > 0
            AND generated_column IS NOT NULL
            AND encoding_version IS NOT NULL
        )
    )
) STRICT";
const V10_GENERATED_IDS_TABLE_SQL: &str = "CREATE TABLE briskdb_generated_ids (
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
) STRICT";
const V9_ALLOCATION_OWNERS_TABLE_SQL: &str = "CREATE TABLE briskdb_allocation_owners (
    owner_slot INTEGER PRIMARY KEY CHECK (owner_slot BETWEEN 0 AND 1023),
    physical_shard_id INTEGER NOT NULL UNIQUE
        CHECK (physical_shard_id BETWEEN 0 AND 63),
    FOREIGN KEY (physical_shard_id)
        REFERENCES briskdb_physical_shards (shard_id)
        ON DELETE RESTRICT
) STRICT";
const V10_ALLOCATION_OWNERS_TABLE_SQL: &str = "CREATE TABLE briskdb_allocation_owners (
    owner_slot INTEGER PRIMARY KEY CHECK (owner_slot BETWEEN 0 AND 1023),
    physical_shard_id INTEGER NOT NULL
        CHECK (physical_shard_id BETWEEN 0 AND 63),
    owner_state INTEGER NOT NULL CHECK (owner_state IN (1, 2)),
    FOREIGN KEY (physical_shard_id)
        REFERENCES briskdb_physical_shards (shard_id)
        ON DELETE RESTRICT
) STRICT";
const V10_ACTIVE_OWNER_INDEX_SQL: &str = "CREATE UNIQUE INDEX briskdb_one_active_owner_per_shard
ON briskdb_allocation_owners (physical_shard_id)
WHERE owner_state = 1";
const V10_TABLE_PROVISIONING_SQL: &str = "CREATE TABLE briskdb_table_provisioning (
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
) STRICT";
const V10_TABLE_PROVISIONING_DECLARATIONS_SQL: &str =
    "CREATE TABLE briskdb_table_provisioning_declarations (
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
) STRICT";
const V11_HILO_LEASES_TABLE_SQL: &str = "CREATE TABLE briskdb_hilo_leases (
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
) STRICT";
const V12_GENERATED_TABLE_DDL_TABLE_SQL: &str = "CREATE TABLE briskdb_generated_table_ddl (
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
) STRICT";
const V5_SHARD_LAYOUT_TABLE_SQL: &str = "CREATE TABLE briskdb_shard_layout (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    layout_id BLOB NOT NULL
        CHECK (typeof(layout_id) = 'blob' AND length(layout_id) = 16),
    shard_application_id INTEGER NOT NULL CHECK (shard_application_id = 1112691528),
    shard_metadata_version INTEGER NOT NULL CHECK (shard_metadata_version = 1),
    layout_state INTEGER NOT NULL CHECK (layout_state IN (1, 2, 3))
) STRICT";
const V6_SCHEMA_MIGRATIONS_TABLE_SQL: &str = "CREATE TABLE briskdb_schema_migrations (
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
) STRICT";
const V7_INTEGRITY_TABLE_SQL: &str = "CREATE TABLE briskdb_integrity (
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
) STRICT";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RoutingConfiguration {
    hash_version: u32,
    key_encoding_version: u32,
    bucket_algorithm_version: u32,
    map_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SchemaCatalogConfiguration {
    identifier_encoding_version: u32,
    schema_generation: u64,
    default_database_id: u64,
}

/// Durable integrity/readiness state stored in the v7 manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DatabaseIntegrityState {
    Verifying,
    Ready,
    Migrating,
    Degraded,
}

impl DatabaseIntegrityState {
    const fn code(self) -> i64 {
        match self {
            Self::Verifying => DATABASE_STATE_VERIFYING,
            Self::Ready => DATABASE_STATE_READY,
            Self::Migrating => DATABASE_STATE_MIGRATING,
            Self::Degraded => DATABASE_STATE_DEGRADED,
        }
    }

    fn from_code(code: i64) -> EngineResult<Self> {
        match code {
            DATABASE_STATE_VERIFYING => Ok(Self::Verifying),
            DATABASE_STATE_READY => Ok(Self::Ready),
            DATABASE_STATE_MIGRATING => Ok(Self::Migrating),
            DATABASE_STATE_DEGRADED => Ok(Self::Degraded),
            _ => Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "manifest contains an unsupported database integrity state",
            )),
        }
    }
}

/// Fully validated v7 checksum metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ManifestIntegrity {
    state: DatabaseIntegrityState,
    committed_schema_digest: Option<[u8; 32]>,
    target_schema_digest: Option<[u8; 32]>,
}

impl ManifestIntegrity {
    pub(super) const fn state(self) -> DatabaseIntegrityState {
        self.state
    }

    pub(super) const fn committed_schema_digest(self) -> Option<[u8; 32]> {
        self.committed_schema_digest
    }

    pub(super) const fn target_schema_digest(self) -> Option<[u8; 32]> {
        self.target_schema_digest
    }
}

/// One fully validated application-schema migration journal row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SchemaMigration {
    source_generation: u64,
    target_generation: u64,
    migration_id: [u8; 32],
    sql_text: String,
    shard_count: u16,
    state: SchemaMigrationState,
    next_shard: u16,
}

impl SchemaMigration {
    pub(super) const fn source_generation(&self) -> u64 {
        self.source_generation
    }

    pub(super) const fn target_generation(&self) -> u64 {
        self.target_generation
    }

    pub(super) const fn migration_id(&self) -> [u8; 32] {
        self.migration_id
    }

    pub(super) fn sql_text(&self) -> &str {
        &self.sql_text
    }

    pub(super) const fn shard_count(&self) -> u16 {
        self.shard_count
    }

    pub(super) const fn is_applying(&self) -> bool {
        matches!(self.state, SchemaMigrationState::Applying)
    }

    pub(super) const fn is_complete(&self) -> bool {
        matches!(self.state, SchemaMigrationState::Complete)
    }

    pub(super) const fn next_shard(&self) -> u16 {
        self.next_shard
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaMigrationState {
    Applying,
    Complete,
}

impl SchemaMigrationState {
    fn from_code(code: i64) -> EngineResult<Self> {
        match code {
            SCHEMA_MIGRATION_APPLYING => Ok(Self::Applying),
            SCHEMA_MIGRATION_COMPLETE => Ok(Self::Complete),
            _ => Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("manifest has unsupported schema-migration state {code}"),
            )),
        }
    }
}

/// Result of looking up the migration identified by exact SQL bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SchemaMigrationClassification {
    Absent,
    Active(SchemaMigration),
    Complete(SchemaMigration),
}

/// One fully validated, checksummed table-provisioning journal.
///
/// The durable prefix means every shard below `next_shard` has committed the
/// exact native-range sequence seed for every declaration in this journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NativeTableProvisioning {
    provisioning_id: [u8; 32],
    committed_schema_digest: [u8; 32],
    shard_count: u16,
    declarations: Box<[TableDeclaration]>,
    next_shard: u16,
}

impl NativeTableProvisioning {
    pub(super) const fn provisioning_id(&self) -> [u8; 32] {
        self.provisioning_id
    }

    pub(super) const fn committed_schema_digest(&self) -> [u8; 32] {
        self.committed_schema_digest
    }

    pub(super) const fn shard_count(&self) -> u16 {
        self.shard_count
    }

    pub(super) fn declarations(&self) -> &[TableDeclaration] {
        &self.declarations
    }

    pub(super) const fn next_shard(&self) -> u16 {
        self.next_shard
    }
}

/// Exact classification of a requested table-provisioning operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum NativeTableProvisioningClassification {
    Absent,
    Active(NativeTableProvisioning),
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub(super) enum GeneratedTableDdlClassification {
    Absent,
    Existing(GeneratedTableDdl),
}

/// Durable phase of the generated-table DDL bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GeneratedTableDdlLifecycle {
    /// The canonical SQLite migration is active or durably complete.
    ApplyingPhysical,
    /// Physical DDL is complete and the deterministic table provisioning is
    /// pending or active.
    Provisioning,
    /// Physical DDL and authoritative catalog publication are complete.
    Complete,
}

impl GeneratedTableDdlLifecycle {
    const fn code(self) -> i64 {
        match self {
            Self::ApplyingPhysical => GENERATED_TABLE_DDL_APPLYING_PHYSICAL,
            Self::Provisioning => GENERATED_TABLE_DDL_PROVISIONING,
            Self::Complete => GENERATED_TABLE_DDL_COMPLETE,
        }
    }

    fn from_code(code: i64) -> EngineResult<Self> {
        match code {
            GENERATED_TABLE_DDL_APPLYING_PHYSICAL => Ok(Self::ApplyingPhysical),
            GENERATED_TABLE_DDL_PROVISIONING => Ok(Self::Provisioning),
            GENERATED_TABLE_DDL_COMPLETE => Ok(Self::Complete),
            _ => Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "generated-table DDL bridge has an unsupported lifecycle state",
            )),
        }
    }
}

/// One fully validated, checksummed generated-table DDL bridge record.
///
/// The exact source bytes and their logical identity remain distinct from the
/// canonical SQLite migration identity. The derived declaration is retained
/// so recovery never has to parse or translate untrusted journal text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GeneratedTableDdl {
    logical_id: [u8; 32],
    source_dialect: SqlDialect,
    translation_version: u32,
    source_sql: String,
    physical_migration_id: [u8; 32],
    physical_sql: String,
    declaration: TableDeclaration,
    lifecycle: GeneratedTableDdlLifecycle,
    provisioning_id: Option<[u8; 32]>,
    provisioning_schema_digest: Option<[u8; 32]>,
    table_id: Option<TableId>,
}

impl GeneratedTableDdl {
    pub(super) const fn logical_id(&self) -> [u8; 32] {
        self.logical_id
    }

    #[cfg(test)]
    pub(super) const fn source_dialect(&self) -> SqlDialect {
        self.source_dialect
    }

    #[cfg(test)]
    pub(super) const fn translation_version(&self) -> u32 {
        self.translation_version
    }

    #[cfg(test)]
    pub(super) fn source_sql(&self) -> &str {
        &self.source_sql
    }

    pub(super) const fn physical_migration_id(&self) -> [u8; 32] {
        self.physical_migration_id
    }

    pub(super) fn physical_sql(&self) -> &str {
        &self.physical_sql
    }

    pub(super) fn declaration(&self) -> &TableDeclaration {
        &self.declaration
    }

    pub(super) const fn lifecycle(&self) -> GeneratedTableDdlLifecycle {
        self.lifecycle
    }

    pub(super) const fn provisioning_id(&self) -> Option<[u8; 32]> {
        self.provisioning_id
    }

    #[cfg(test)]
    pub(super) const fn provisioning_schema_digest(&self) -> Option<[u8; 32]> {
        self.provisioning_schema_digest
    }

    pub(super) const fn table_id(&self) -> Option<TableId> {
        self.table_id
    }
}

#[derive(Clone, Copy)]
struct Migration {
    from: u32,
    to: u32,
    name: &'static str,
    apply: fn(&Transaction<'_>, u16) -> EngineResult<()>,
    validate: fn(&Connection, u16, &[SchemaObject]) -> EngineResult<ManifestSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ManifestSnapshot {
    shard_count: u16,
    routing_catalog: Option<RoutingCatalog>,
    logical_catalog: Option<Catalog>,
    shard_layout: Option<ShardLayout>,
    active_migration: Option<SchemaMigration>,
    integrity: Option<ManifestIntegrity>,
    allocation_owners: Option<AllocationOwnerMap>,
    active_native_id_table_ids: Box<[TableId]>,
    active_hilo_id_table_ids: Box<[TableId]>,
    active_table_provisioning: Option<NativeTableProvisioning>,
    generated_table_ddl: Option<GeneratedTableDdl>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LoadedManifest {
    catalog: CatalogSnapshot,
    shard_layout: ShardLayout,
    active_migration: Option<SchemaMigration>,
    integrity: ManifestIntegrity,
    active_native_id_table_ids: Box<[TableId]>,
    active_hilo_id_table_ids: Box<[TableId]>,
    active_table_provisioning: Option<NativeTableProvisioning>,
    generated_table_ddl: Option<GeneratedTableDdl>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct V6ActiveMigration {
    catalog: CatalogSnapshot,
    shard_layout: ShardLayout,
    migration: SchemaMigration,
}

impl V6ActiveMigration {
    pub(super) fn into_parts(self) -> (CatalogSnapshot, ShardLayout, SchemaMigration) {
        (self.catalog, self.shard_layout, self.migration)
    }
}

impl LoadedManifest {
    #[cfg(test)]
    pub(super) fn into_parts(self) -> (CatalogSnapshot, ShardLayout) {
        (self.catalog, self.shard_layout)
    }

    #[cfg(test)]
    pub(super) fn active_migration(&self) -> Option<&SchemaMigration> {
        self.active_migration.as_ref()
    }

    #[cfg(test)]
    pub(super) fn active_table_provisioning(&self) -> Option<&NativeTableProvisioning> {
        self.active_table_provisioning.as_ref()
    }

    #[cfg(test)]
    pub(super) fn generated_table_ddl(&self) -> Option<&GeneratedTableDdl> {
        self.generated_table_ddl.as_ref()
    }

    #[cfg(test)]
    pub(super) fn into_parts_with_migration(
        self,
    ) -> (
        CatalogSnapshot,
        ShardLayout,
        Option<SchemaMigration>,
        ManifestIntegrity,
    ) {
        (
            self.catalog,
            self.shard_layout,
            self.active_migration,
            self.integrity,
        )
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn into_parts_with_recovery(
        self,
    ) -> (
        CatalogSnapshot,
        ShardLayout,
        Option<SchemaMigration>,
        ManifestIntegrity,
        Box<[TableId]>,
        Box<[TableId]>,
        Option<NativeTableProvisioning>,
        Option<GeneratedTableDdl>,
    ) {
        (
            self.catalog,
            self.shard_layout,
            self.active_migration,
            self.integrity,
            self.active_native_id_table_ids,
            self.active_hilo_id_table_ids,
            self.active_table_provisioning,
            self.generated_table_ddl,
        )
    }
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        from: LEGACY_SCHEMA_VERSION,
        to: V2_SCHEMA_VERSION,
        name: "typed_manifest_and_downgrade_fence",
        apply: migrate_v1_to_v2,
        validate: validate_v2,
    },
    Migration {
        from: V2_SCHEMA_VERSION,
        to: V3_SCHEMA_VERSION,
        name: "durable_shard_catalog",
        apply: migrate_v2_to_v3,
        validate: validate_v3,
    },
    Migration {
        from: V3_SCHEMA_VERSION,
        to: V4_SCHEMA_VERSION,
        name: "logical_database_and_table_catalog",
        apply: migrate_v3_to_v4,
        validate: validate_v4,
    },
    Migration {
        from: V4_SCHEMA_VERSION,
        to: V5_SCHEMA_VERSION,
        name: "validated_physical_shard_layout",
        apply: migrate_v4_to_v5,
        validate: validate_v5,
    },
    Migration {
        from: V5_SCHEMA_VERSION,
        to: V6_SCHEMA_VERSION,
        name: "application_schema_migration_journal",
        apply: migrate_v5_to_v6,
        validate: validate_v6,
    },
    Migration {
        from: V6_SCHEMA_VERSION,
        to: V7_SCHEMA_VERSION,
        name: "checksummed_integrity_state",
        apply: migrate_v6_to_v7,
        validate: validate_v7,
    },
    Migration {
        from: V7_SCHEMA_VERSION,
        to: V8_SCHEMA_VERSION,
        name: "authoritative_table_catalog",
        apply: migrate_v7_to_v8,
        validate: validate_v8,
    },
    Migration {
        from: V8_SCHEMA_VERSION,
        to: V9_SCHEMA_VERSION,
        name: "generated_id_policies_and_allocation_owners",
        apply: migrate_v8_to_v9,
        validate: validate_v9,
    },
    Migration {
        from: V9_SCHEMA_VERSION,
        to: V10_SCHEMA_VERSION,
        name: "native_id_activation_and_table_provisioning",
        apply: migrate_v9_to_v10,
        validate: validate_v10,
    },
    Migration {
        from: V10_SCHEMA_VERSION,
        to: V11_SCHEMA_VERSION,
        name: "durable_hilo_v1_block_leases",
        apply: migrate_v10_to_v11,
        validate: validate_v11,
    },
    Migration {
        from: V11_SCHEMA_VERSION,
        to: V12_SCHEMA_VERSION,
        name: "durable_generated_table_ddl_bridge",
        apply: migrate_v11_to_v12,
        validate: validate_v12,
    },
];

#[derive(Clone, Copy)]
struct MigrationPlan<'a> {
    current_version: u32,
    migrations: &'a [Migration],
    initialize_current: fn(&Transaction<'_>, u16) -> EngineResult<()>,
    initialize_interrupted_legacy: fn(&Transaction<'_>, u16) -> EngineResult<()>,
}

const CURRENT_PLAN: MigrationPlan<'static> = MigrationPlan {
    current_version: CURRENT_SCHEMA_VERSION,
    migrations: MIGRATIONS,
    initialize_current: create_v12_schema,
    initialize_interrupted_legacy: migrate_interrupted_legacy_to_v12,
};

// Startup uses this frozen plan only to finish an already-active v6 journal
// before the v7 checksum bootstrap. It must never initialize or upgrade data.
const V6_PLAN: MigrationPlan<'static> = MigrationPlan {
    current_version: V6_SCHEMA_VERSION,
    migrations: MIGRATIONS,
    initialize_current: create_v6_schema,
    initialize_interrupted_legacy: migrate_interrupted_legacy_to_v6,
};

#[derive(Clone, Copy)]
struct SchemaChange {
    from: u32,
    to: u32,
    apply: fn(&Transaction<'_>, u16) -> EngineResult<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrationPhase {
    AfterSchemaChange,
    AfterVersionStamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MigrationPoint {
    from: u32,
    to: u32,
    phase: MigrationPhase,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ManifestState {
    Empty,
    LegacyUninitialized,
    LegacyV1 {
        shard_count: u16,
    },
    Versioned {
        version: u32,
        snapshot: Box<ManifestSnapshot>,
    },
}

/// Initialize or advance the manifest under an immediate transaction.
///
/// The state is inspected again after the write lock is acquired, so two
/// concurrent openers cannot both act on a stale version. Each numbered
/// migration owns its transaction and stamps the new version last.
#[cfg(test)]
fn load_or_create_manifest(
    connection: &mut Connection,
    requested_shards: u16,
) -> EngineResult<LoadedManifest> {
    load_or_create_manifest_with_fresh_layout(connection, requested_shards, true)
}

pub(super) fn load_or_create_manifest_with_fresh_layout(
    connection: &mut Connection,
    requested_shards: u16,
    fresh_layout_allowed: bool,
) -> EngineResult<LoadedManifest> {
    let mut snapshot = load_or_create_snapshot_with_plan(
        connection,
        requested_shards,
        CURRENT_PLAN,
        fresh_layout_allowed,
        &mut |_| Ok(()),
    )?;
    let active_native_id_table_ids = std::mem::take(&mut snapshot.active_native_id_table_ids);
    let active_hilo_id_table_ids = std::mem::take(&mut snapshot.active_hilo_id_table_ids);
    let catalog = catalog_snapshot_from_parts(
        snapshot.routing_catalog.take(),
        snapshot.logical_catalog.take(),
        snapshot.allocation_owners.take(),
    )?;
    let shard_layout = snapshot.shard_layout.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "current manifest validation did not produce a physical shard layout",
        )
    })?;
    let integrity = snapshot.integrity.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "current manifest validation did not produce integrity metadata",
        )
    })?;
    Ok(LoadedManifest {
        catalog,
        shard_layout,
        active_migration: snapshot.active_migration,
        integrity,
        active_native_id_table_ids,
        active_hilo_id_table_ids,
        active_table_provisioning: snapshot.active_table_provisioning,
        generated_table_ddl: snapshot.generated_table_ddl,
    })
}

/// Return an active v6 journal without upgrading it. Startup completes this
/// recovery under the v6 rules before establishing v7 checksum authority.
pub(super) fn load_v6_active_migration(
    connection: &Connection,
    requested_shards: u16,
) -> EngineResult<Option<V6ActiveMigration>> {
    let (application_id, version) = read_identity(connection)?;
    if application_id != MANIFEST_APPLICATION_ID || version != i64::from(V6_SCHEMA_VERSION) {
        return Ok(None);
    }
    let ManifestState::Versioned { version, snapshot } =
        inspect_with_plan(connection, requested_shards, V6_PLAN)?
    else {
        return Ok(None);
    };
    if version != V6_SCHEMA_VERSION {
        return Ok(None);
    }
    let Some(migration) = snapshot.active_migration else {
        return Ok(None);
    };
    let routing = snapshot.routing_catalog.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "v6 recovery validation omitted its routing catalog",
        )
    })?;
    let logical = snapshot.logical_catalog.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "v6 recovery validation omitted its logical catalog",
        )
    })?;
    let shard_layout = snapshot.shard_layout.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "v6 recovery validation omitted its physical shard layout",
        )
    })?;
    Ok(Some(V6ActiveMigration {
        catalog: CatalogSnapshot::from_validated_parts(routing, logical),
        shard_layout,
        migration,
    }))
}

#[cfg(test)]
fn load_or_create_catalog(
    connection: &mut Connection,
    requested_shards: u16,
) -> EngineResult<CatalogSnapshot> {
    load_or_create_manifest(connection, requested_shards).map(|loaded| loaded.catalog)
}

/// Classify the migration identified by the exact UTF-8 SQL bytes.
#[cfg(test)]
pub(super) fn classify_schema_migration(
    connection: &mut Connection,
    requested_shards: u16,
    sql: &str,
) -> EngineResult<SchemaMigrationClassification> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let classification =
        classify_schema_migration_in_transaction(&transaction, requested_shards, sql)?;
    transaction.commit().map_err(sqlite_error::storage)?;
    Ok(classification)
}

pub(super) fn classify_schema_migration_in_transaction(
    transaction: &Connection,
    requested_shards: u16,
    sql: &str,
) -> EngineResult<SchemaMigrationClassification> {
    let migration_id = schema_migration_id(sql)?;
    let snapshot = current_manifest_snapshot(transaction, requested_shards)?;
    if let Some(active) = snapshot.active_migration {
        if active.migration_id != migration_id {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "a different schema migration is already active",
            ));
        }
        return classify_matching_schema_migration(Some(active), sql);
    }
    let migration = find_schema_migration(transaction, snapshot.shard_count, &migration_id)?;
    classify_matching_schema_migration(migration, sql)
}

/// Load the single active journal row under a manifest write transaction.
#[cfg(test)]
pub(super) fn load_active_schema_migration(
    connection: &mut Connection,
    requested_shards: u16,
) -> EngineResult<Option<SchemaMigration>> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let active = load_active_schema_migration_in_transaction(&transaction, requested_shards)?;
    transaction.commit().map_err(sqlite_error::storage)?;
    Ok(active)
}

pub(super) fn load_active_schema_migration_in_transaction(
    transaction: &Connection,
    requested_shards: u16,
) -> EngineResult<Option<SchemaMigration>> {
    Ok(current_manifest_snapshot(transaction, requested_shards)?.active_migration)
}

pub(super) fn ensure_schema_migration_layout(
    connection: &Connection,
    requested_shards: u16,
    expected: &ShardLayout,
) -> EngineResult<()> {
    let observed = current_manifest_snapshot(connection, requested_shards)?
        .shard_layout
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                "current manifest validation omitted its physical shard layout",
            )
        })?;
    if observed != *expected || observed.state() != ShardLayoutState::Ready {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest physical layout does not match the opened storage root",
        ));
    }
    Ok(())
}

/// Classify the exact generated-table DDL bridge request.
///
/// The bridge is a retained singleton. An exact retry returns its validated
/// record at any lifecycle phase; any different request fails closed.
pub(super) fn classify_generated_table_ddl(
    connection: &mut Connection,
    requested_shards: u16,
    source_dialect: SqlDialect,
    source_sql: &str,
    physical_sql: &str,
    declaration: &TableDeclaration,
) -> EngineResult<GeneratedTableDdlClassification> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let snapshot = current_manifest_snapshot(&transaction, requested_shards)?;
    let result = classify_generated_table_ddl_snapshot(
        &snapshot,
        source_dialect,
        source_sql,
        physical_sql,
        declaration,
    )?;
    transaction.commit().map_err(sqlite_error::storage)?;
    Ok(result)
}

/// Atomically begin the canonical physical migration and retain the exact
/// generated-table logical request in the same caller-owned transaction.
///
/// The caller owns the commit durability boundary and must not restore the
/// schema gate to Ready if COMMIT may have been attempted.
#[allow(clippy::too_many_arguments)]
pub(super) fn begin_generated_table_ddl_in_transaction(
    transaction: &Connection,
    requested_shards: u16,
    expected_source_generation: u64,
    source_dialect: SqlDialect,
    source_sql: &str,
    physical_sql: &str,
    declaration: TableDeclaration,
    expected_source_digest: [u8; 32],
    target_digest: [u8; 32],
) -> EngineResult<(GeneratedTableDdl, SchemaMigration)> {
    validate_generated_table_ddl_declaration(&declaration)?;
    validate_schema_migration_sql(source_sql)?;
    validate_schema_migration_sql(physical_sql)?;
    let snapshot = current_manifest_snapshot(transaction, requested_shards)?;
    match classify_generated_table_ddl_snapshot(
        &snapshot,
        source_dialect,
        source_sql,
        physical_sql,
        &declaration,
    )? {
        GeneratedTableDdlClassification::Existing(existing) => {
            let migration = find_schema_migration(
                transaction,
                requested_shards,
                &existing.physical_migration_id,
            )?
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::DataCorruption,
                    "generated-table DDL bridge lost its physical migration",
                )
            })?;
            return Ok((existing, migration));
        }
        GeneratedTableDdlClassification::Absent => {}
    }
    if snapshot.active_table_provisioning.is_some() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "generated-table DDL cannot begin during table provisioning",
        ));
    }
    if snapshot
        .logical_catalog
        .as_ref()
        .is_none_or(|catalog| !catalog.tables().is_empty())
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "generated-table DDL bridge currently requires an empty authoritative catalog",
        ));
    }
    let migration = begin_schema_migration_with_digests_in_transaction(
        transaction,
        requested_shards,
        expected_source_generation,
        physical_sql,
        expected_source_digest,
        target_digest,
    )?;
    let logical_id = generated_table_ddl_logical_id(source_dialect, source_sql)?;
    let physical_migration_id = schema_migration_id(physical_sql)?;
    let generated_column = validate_generated_table_ddl_declaration(&declaration)?;
    transaction
        .execute(
            "INSERT INTO briskdb_generated_table_ddl (
                singleton,
                logical_id,
                logical_digest_version,
                source_dialect,
                translation_version,
                source_sql,
                physical_migration_id,
                physical_sql,
                database_id,
                table_name,
                generated_column,
                generated_policy,
                generated_encoding_version,
                lifecycle_state,
                provisioning_id,
                provisioning_schema_digest,
                table_id
             ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, NULL, NULL, NULL)",
            rusqlite::params![
                logical_id.as_slice(),
                GENERATED_TABLE_DDL_DIGEST_VERSION,
                encoded_generated_table_ddl_dialect(source_dialect),
                GENERATED_TABLE_DDL_TRANSLATION_VERSION,
                source_sql,
                physical_migration_id.as_slice(),
                physical_sql,
                i64::try_from(declaration.database_id().get()).map_err(|error| {
                    EngineError::from_source(
                        EngineErrorKind::NumericOutOfRange,
                        "generated-table DDL database ID does not fit in SQLite",
                        error,
                    )
                })?,
                declaration.name(),
                generated_column,
                GENERATED_ID_POLICY_NATIVE_RANGE_V1,
                NATIVE_RANGE_V1_ENCODING_VERSION,
                GeneratedTableDdlLifecycle::ApplyingPhysical.code(),
            ],
        )
        .map_err(sqlite_error::storage)?;
    refresh_manifest_digest(transaction)?;
    let persisted = current_manifest_snapshot(transaction, requested_shards)?
        .generated_table_ddl
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                "generated-table DDL bridge did not persist",
            )
        })?;
    ensure_same_generated_table_ddl_request(
        &persisted,
        source_dialect,
        source_sql,
        physical_sql,
        &declaration,
    )?;
    Ok((persisted, migration))
}

pub(super) fn current_integrity(
    connection: &Connection,
    requested_shards: u16,
) -> EngineResult<ManifestIntegrity> {
    current_manifest_snapshot(connection, requested_shards)?
        .integrity
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "database manifest predates integrity metadata",
            )
        })
}

pub(super) fn current_integrity_optional(
    connection: &Connection,
    requested_shards: u16,
) -> EngineResult<Option<ManifestIntegrity>> {
    Ok(current_manifest_snapshot(connection, requested_shards)?.integrity)
}

/// Seal a freshly verified v7 schema baseline or validate an already-ready
/// database. Terminal `Degraded` state is never cleared here.
pub(super) fn seal_verified_schema(
    connection: &mut Connection,
    requested_shards: u16,
    observed_digest: [u8; 32],
) -> EngineResult<ManifestIntegrity> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let snapshot = current_manifest_snapshot(&transaction, requested_shards)?;
    if snapshot.active_migration.is_some() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "an active schema migration cannot be published as ready",
        ));
    }
    let integrity = snapshot.integrity.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "database manifest predates integrity metadata",
        )
    })?;
    match integrity.state {
        DatabaseIntegrityState::Verifying => {
            if integrity
                .committed_schema_digest
                .is_some_and(|expected| expected != observed_digest)
            {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    "verified application schema does not match the trusted checksum",
                ));
            }
            transaction
                .execute(
                    "UPDATE briskdb_integrity
                     SET database_state = ?1, committed_schema_digest = ?2
                     WHERE singleton = 1 AND database_state = ?3",
                    rusqlite::params![
                        DATABASE_STATE_READY,
                        observed_digest.as_slice(),
                        DATABASE_STATE_VERIFYING,
                    ],
                )
                .map_err(sqlite_error::storage)?;
            refresh_manifest_digest(&transaction)?;
        }
        DatabaseIntegrityState::Ready => {
            if integrity.committed_schema_digest != Some(observed_digest) {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    "application schema checksum does not match the manifest",
                ));
            }
        }
        DatabaseIntegrityState::Degraded => {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "degraded database requires a complete known-good restore",
            ));
        }
        DatabaseIntegrityState::Migrating => {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "an active schema migration cannot be published as ready",
            ));
        }
    }
    let sealed = current_integrity(&transaction, requested_shards)?;
    transaction.commit().map_err(sqlite_error::storage)?;
    Ok(sealed)
}

/// Persist the fail-closed state only after the current semantic root and the
/// caller's exact shard layout have been validated. A corrupt or replaced
/// manifest therefore never signs its own altered data.
pub(super) fn mark_degraded(
    connection: &mut Connection,
    requested_shards: u16,
    expected_layout: &ShardLayout,
) -> EngineResult<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let snapshot = current_manifest_snapshot(&transaction, requested_shards)?;
    let observed_layout = snapshot.shard_layout.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest degradation requires a validated shard-layout identity",
        )
    })?;
    if observed_layout != *expected_layout {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest shard-layout identity changed before degradation could be recorded",
        ));
    }
    let integrity = snapshot.integrity.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "database manifest predates integrity metadata",
        )
    })?;
    if integrity.state != DatabaseIntegrityState::Degraded {
        let changed = transaction
            .execute(
                "UPDATE briskdb_integrity SET database_state = ?1
                 WHERE singleton = 1 AND database_state = ?2",
                rusqlite::params![DATABASE_STATE_DEGRADED, integrity.state.code()],
            )
            .map_err(sqlite_error::storage)?;
        if changed != 1 {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "database integrity state changed before degradation was recorded",
            ));
        }
        refresh_manifest_digest(&transaction)?;
        let persisted = current_integrity(&transaction, requested_shards)?;
        if persisted.state != DatabaseIntegrityState::Degraded {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "database degradation state did not persist",
            ));
        }
    }
    transaction.commit().map_err(sqlite_error::storage)
}

/// Atomically append a new active journal row after the caller has preflighted
/// every shard at `expected_source_generation`.
///
/// Repeating the exact SQL is idempotent and returns its existing active or
/// completed row. Only one different migration may be active at a time.
#[cfg(test)]
pub(super) fn begin_schema_migration(
    connection: &mut Connection,
    requested_shards: u16,
    expected_source_generation: u64,
    sql: &str,
) -> EngineResult<SchemaMigration> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let migration = begin_schema_migration_in_transaction(
        &transaction,
        requested_shards,
        expected_source_generation,
        sql,
    )?;
    transaction.commit().map_err(sqlite_error::storage)?;
    Ok(migration)
}

#[cfg(test)]
pub(super) fn begin_schema_migration_in_transaction(
    transaction: &Connection,
    requested_shards: u16,
    expected_source_generation: u64,
    sql: &str,
) -> EngineResult<SchemaMigration> {
    begin_schema_migration_with_digests_inner(
        transaction,
        requested_shards,
        expected_source_generation,
        sql,
        None,
    )
}

pub(super) fn begin_schema_migration_with_digests_in_transaction(
    transaction: &Connection,
    requested_shards: u16,
    expected_source_generation: u64,
    sql: &str,
    expected_source_digest: [u8; 32],
    target_digest: [u8; 32],
) -> EngineResult<SchemaMigration> {
    begin_schema_migration_with_digests_inner(
        transaction,
        requested_shards,
        expected_source_generation,
        sql,
        Some((expected_source_digest, target_digest)),
    )
}

fn begin_schema_migration_with_digests_inner(
    transaction: &Connection,
    requested_shards: u16,
    expected_source_generation: u64,
    sql: &str,
    schema_digests: Option<([u8; 32], [u8; 32])>,
) -> EngineResult<SchemaMigration> {
    let migration_id = schema_migration_id(sql)?;
    let snapshot = current_manifest_snapshot(transaction, requested_shards)?;

    if let Some(active) = snapshot.active_migration {
        if active.migration_id != migration_id {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "a different schema migration is already active",
            ));
        }
        if active.sql_text != sql {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "schema migration identifier collides with different SQL bytes",
            ));
        }
        return Ok(active);
    }
    if let Some(existing) = find_schema_migration(transaction, snapshot.shard_count, &migration_id)?
    {
        if existing.sql_text != sql {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "schema migration identifier collides with different SQL bytes",
            ));
        }
        return Ok(existing);
    }
    let layout = snapshot.shard_layout.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "current manifest validation omitted its physical shard layout",
        )
    })?;
    if layout.state() != ShardLayoutState::Ready {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "schema migration requires a ready physical shard layout",
        ));
    }
    let catalog_generation = snapshot
        .logical_catalog
        .as_ref()
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                "current manifest validation omitted its logical catalog",
            )
        })?
        .schema_generation();
    if catalog_generation != expected_source_generation {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "schema migration preflight used generation {expected_source_generation}, but the catalog is at generation {catalog_generation}"
            ),
        ));
    }
    let target_generation = catalog_generation.checked_add(1).ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "schema migration generation is exhausted",
        )
    })?;
    if target_generation > MAX_SCHEMA_GENERATION {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "schema migration generation is exhausted",
        ));
    }
    let target_schema_digest = if let Some(integrity) = snapshot.integrity {
        if integrity.state != DatabaseIntegrityState::Ready {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "schema migration requires a checksum-verified ready database",
            ));
        }
        let committed = integrity.committed_schema_digest.ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                "ready manifest is missing its committed application-schema checksum",
            )
        })?;
        let (expected, target) = schema_digests.unwrap_or((committed, committed));
        if expected != committed {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "schema migration source checksum does not match the manifest",
            ));
        }
        Some(target)
    } else {
        None
    };
    transaction
        .execute(
            "INSERT INTO briskdb_schema_migrations (
                target_generation,
                source_generation,
                migration_id,
                digest_version,
                sql_text,
                shard_count,
                migration_state,
                next_shard
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)",
            rusqlite::params![
                i64::try_from(target_generation).expect("schema generation fits in SQLite"),
                i64::try_from(catalog_generation).expect("schema generation fits in SQLite"),
                migration_id.as_slice(),
                SCHEMA_MIGRATION_DIGEST_VERSION,
                sql,
                snapshot.shard_count,
                SCHEMA_MIGRATION_APPLYING,
            ],
        )
        .map_err(sqlite_error::storage)?;
    if let Some(target_schema_digest) = target_schema_digest {
        let changed = transaction
            .execute(
                "UPDATE briskdb_integrity
                 SET database_state = ?1, target_schema_digest = ?2
                 WHERE singleton = 1 AND database_state = ?3",
                rusqlite::params![
                    DATABASE_STATE_MIGRATING,
                    target_schema_digest.as_slice(),
                    DATABASE_STATE_READY,
                ],
            )
            .map_err(sqlite_error::storage)?;
        if changed != 1 {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "database integrity state changed before migration publication",
            ));
        }
        refresh_manifest_digest(transaction)?;
    }
    let validated = current_manifest_snapshot(transaction, requested_shards)?
        .active_migration
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                "new schema migration did not produce an active journal row",
            )
        })?;
    if validated.migration_id != migration_id || validated.sql_text != sql {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "new schema migration did not preserve its identity",
        ));
    }
    Ok(validated)
}

/// Advance an active migration's durable prefix by at most one shard.
/// Repeating an already-persisted position is an idempotent no-op.
#[cfg(test)]
pub(super) fn advance_schema_migration(
    connection: &mut Connection,
    requested_shards: u16,
    expected: &SchemaMigration,
    next_shard: u16,
) -> EngineResult<SchemaMigration> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let migration = advance_schema_migration_in_transaction(
        &transaction,
        requested_shards,
        expected,
        next_shard,
    )?;
    transaction.commit().map_err(sqlite_error::storage)?;
    Ok(migration)
}

pub(super) fn advance_schema_migration_in_transaction(
    transaction: &Connection,
    requested_shards: u16,
    expected: &SchemaMigration,
    next_shard: u16,
) -> EngineResult<SchemaMigration> {
    let snapshot = current_manifest_snapshot(transaction, requested_shards)?;
    let Some(active) = snapshot.active_migration else {
        let completed =
            find_schema_migration(transaction, snapshot.shard_count, &expected.migration_id)?
                .filter(|migration| migration.is_complete());
        if let Some(completed) = completed {
            ensure_same_schema_migration(&completed, expected)?;
            return Ok(completed);
        }
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "schema migration is no longer active",
        ));
    };
    ensure_same_schema_migration(&active, expected)?;
    if next_shard > active.shard_count {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "schema migration progress exceeds its shard count",
        ));
    }
    if next_shard <= active.next_shard {
        return Ok(active);
    }
    if next_shard != active.next_shard + 1 {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "schema migration progress cannot skip a shard",
        ));
    }
    transaction
        .execute(
            "UPDATE briskdb_schema_migrations
             SET next_shard = ?1
             WHERE target_generation = ?2
               AND migration_state = ?3
               AND next_shard = ?4",
            rusqlite::params![
                next_shard,
                i64::try_from(active.target_generation).expect("schema generation fits in SQLite"),
                SCHEMA_MIGRATION_APPLYING,
                active.next_shard,
            ],
        )
        .map_err(sqlite_error::storage)?;
    refresh_manifest_digest_if_checksummed(transaction)?;
    let advanced = current_manifest_snapshot(transaction, requested_shards)?
        .active_migration
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                "schema migration progress update lost its active row",
            )
        })?;
    ensure_same_schema_migration(&advanced, expected)?;
    if advanced.next_shard != next_shard {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "schema migration progress update did not persist",
        ));
    }
    Ok(advanced)
}

/// Atomically publish a fully applied migration as the new catalog generation.
#[cfg(test)]
pub(super) fn finalize_schema_migration(
    connection: &mut Connection,
    requested_shards: u16,
    expected: &SchemaMigration,
) -> EngineResult<SchemaMigration> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let migration =
        finalize_schema_migration_in_transaction(&transaction, requested_shards, expected)?;
    transaction.commit().map_err(sqlite_error::storage)?;
    Ok(migration)
}

pub(super) fn finalize_schema_migration_in_transaction(
    transaction: &Connection,
    requested_shards: u16,
    expected: &SchemaMigration,
) -> EngineResult<SchemaMigration> {
    let snapshot = current_manifest_snapshot(transaction, requested_shards)?;
    let Some(active) = snapshot.active_migration else {
        let completed =
            find_schema_migration(transaction, snapshot.shard_count, &expected.migration_id)?
                .filter(|migration| migration.is_complete())
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::FailedPrecondition,
                        "schema migration is no longer active",
                    )
                })?;
        ensure_same_schema_migration(&completed, expected)?;
        return Ok(completed);
    };
    ensure_same_schema_migration(&active, expected)?;
    if active.next_shard != active.shard_count {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "schema migration cannot finish before every shard is durable",
        ));
    }
    transaction
        .execute(
            "UPDATE briskdb_schema_catalog
             SET schema_generation = ?1
             WHERE singleton = 1 AND schema_generation = ?2",
            rusqlite::params![
                i64::try_from(active.target_generation).expect("schema generation fits in SQLite"),
                i64::try_from(active.source_generation).expect("schema generation fits in SQLite"),
            ],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "UPDATE briskdb_schema_migrations
             SET migration_state = ?1
             WHERE target_generation = ?2
               AND migration_state = ?3
               AND next_shard = shard_count",
            rusqlite::params![
                SCHEMA_MIGRATION_COMPLETE,
                i64::try_from(active.target_generation).expect("schema generation fits in SQLite"),
                SCHEMA_MIGRATION_APPLYING,
            ],
        )
        .map_err(sqlite_error::storage)?;
    if snapshot.integrity.is_some() {
        let changed = transaction
            .execute(
                "UPDATE briskdb_integrity
                 SET committed_schema_digest = target_schema_digest,
                     target_schema_digest = NULL,
                     database_state = ?1
                 WHERE singleton = 1
                   AND database_state = ?2
                   AND target_schema_digest IS NOT NULL",
                rusqlite::params![DATABASE_STATE_READY, DATABASE_STATE_MIGRATING],
            )
            .map_err(sqlite_error::storage)?;
        if changed != 1 {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "schema migration finalization found inconsistent integrity metadata",
            ));
        }
        refresh_manifest_digest(transaction)?;
    }
    let finalized_snapshot = current_manifest_snapshot(transaction, requested_shards)?;
    if finalized_snapshot.active_migration.is_some()
        || finalized_snapshot
            .logical_catalog
            .as_ref()
            .map(Catalog::schema_generation)
            != Some(active.target_generation)
    {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "schema migration finalization did not publish its target generation",
        ));
    }
    let completed = find_schema_migration(
        transaction,
        finalized_snapshot.shard_count,
        &active.migration_id,
    )?
    .ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "schema migration finalization lost its journal row",
        )
    })?;
    if !completed.is_complete() {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "schema migration finalization did not complete its journal row",
        ));
    }
    Ok(completed)
}

fn current_manifest_snapshot(
    connection: &Connection,
    requested_shards: u16,
) -> EngineResult<ManifestSnapshot> {
    match inspect_with_plan(connection, requested_shards, CURRENT_PLAN)? {
        ManifestState::Versioned { version, snapshot }
            if version == CURRENT_SCHEMA_VERSION || version == V6_SCHEMA_VERSION =>
        {
            Ok(*snapshot)
        }
        _ => Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "schema migrations require a current manifest",
        )),
    }
}

/// Reserve one durable global `hilo_v1` block before any target-shard write
/// lock is acquired. A commit failure is deliberately not reconciled into a
/// returned lease: if SQLite committed despite the error, that block remains
/// burned and the next reservation advances beyond it.
pub(super) fn reserve_hilo_v1_block(
    connection: &mut Connection,
    requested_shards: u16,
    table_id: TableId,
    owner_id: [u8; 32],
) -> EngineResult<DurableHiloLease> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let snapshot = current_manifest_snapshot(&transaction, requested_shards)?;
    ensure_table_registration_ready(&snapshot)?;
    if snapshot.active_table_provisioning.is_some() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "hilo_v1 block reservation cannot run during table provisioning",
        ));
    }
    let catalog = snapshot.logical_catalog.as_ref().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "hilo_v1 reservation validation omitted the logical catalog",
        )
    })?;
    let table = catalog.table_by_id(table_id).ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::InvalidArgument,
            format!("hilo_v1 reservation refers to unknown table {table_id}"),
        )
    })?;
    if !matches!(
        table.generated_id_policy(),
        GeneratedIdPolicy::HiloV1 { .. }
    ) || snapshot
        .active_hilo_id_table_ids
        .binary_search(&table_id)
        .is_err()
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("hilo_v1 generation is not active for table {table_id}"),
        ));
    }
    let stored_table_id = i64::try_from(table_id.get()).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::NumericOutOfRange,
            "hilo_v1 table ID does not fit in SQLite",
            error,
        )
    })?;
    let (stored_block_size, stored_next, stored_fence) = transaction
        .query_row(
            "SELECT block_size, next_sequence, fence_token
             FROM briskdb_hilo_leases
             WHERE table_id = ?1",
            [stored_table_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .map_err(|error| manifest_read_error(error, "failed to read hilo_v1 allocation head"))?;
    if stored_block_size != i64::try_from(HILO_V1_BLOCK_SIZE).expect("block size fits i64") {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "hilo_v1 allocation head has an unsupported block size",
        ));
    }
    let first_sequence = u64::try_from(stored_next).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "hilo_v1 allocation head is outside its numeric range",
            error,
        )
    })?;
    if first_sequence > MAX_HILO_V1_SEQUENCE {
        return Err(EngineError::new(
            EngineErrorKind::LimitExceeded,
            format!("hilo_v1 table {table_id} exhausted its global sequence"),
        ));
    }
    let fence_token = u64::try_from(stored_fence).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "hilo_v1 fence token is outside its numeric range",
            error,
        )
    })?;
    let next_fence = fence_token
        .checked_add(1)
        .filter(|value| *value <= i64::MAX as u64)
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!("hilo_v1 table {table_id} exhausted its fence tokens"),
            )
        })?;
    let last_sequence = first_sequence
        .saturating_add(HILO_V1_BLOCK_SIZE - 1)
        .min(MAX_HILO_V1_SEQUENCE);
    let next_sequence = last_sequence
        .checked_add(1)
        .expect("the maximum hilo_v1 sequence has a representable sentinel");
    let changed = transaction
        .execute(
            "UPDATE briskdb_hilo_leases
             SET next_sequence = ?1,
                 fence_token = ?2,
                 last_owner_id = ?3,
                 last_first_sequence = ?4,
                 last_last_sequence = ?5
             WHERE table_id = ?6
               AND next_sequence = ?7
               AND fence_token = ?8",
            rusqlite::params![
                i64::try_from(next_sequence).expect("hi/lo sentinel fits SQLite"),
                i64::try_from(next_fence).expect("bounded hi/lo fence fits SQLite"),
                owner_id.as_slice(),
                i64::try_from(first_sequence).expect("hi/lo sequence fits SQLite"),
                i64::try_from(last_sequence).expect("hi/lo sequence fits SQLite"),
                stored_table_id,
                stored_next,
                stored_fence,
            ],
        )
        .map_err(sqlite_error::storage)?;
    if changed != 1 {
        return Err(EngineError::new(
            EngineErrorKind::Busy,
            "hilo_v1 allocation head changed concurrently",
        ));
    }
    refresh_manifest_digest(&transaction)?;
    let verified = current_manifest_snapshot(&transaction, requested_shards)?;
    if verified
        .active_hilo_id_table_ids
        .binary_search(&table_id)
        .is_err()
    {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "hilo_v1 block reservation lost its active policy",
        ));
    }
    #[cfg(test)]
    abort_hilo_lease_at_test_boundary("before-commit");
    transaction.commit().map_err(sqlite_error::storage)?;
    #[cfg(test)]
    abort_hilo_lease_at_test_boundary("after-commit");
    Ok(DurableHiloLease::new(
        table_id,
        owner_id,
        next_fence,
        first_sequence,
        last_sequence,
    ))
}

/// Classify an exact native table-provisioning request without changing it.
#[cfg(test)]
pub(super) fn classify_native_table_provisioning(
    connection: &mut Connection,
    requested_shards: u16,
    declarations: Vec<TableDeclaration>,
    committed_schema_digest: [u8; 32],
) -> EngineResult<NativeTableProvisioningClassification> {
    let declarations = normalize_table_provisioning_declarations(declarations)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let snapshot = current_manifest_snapshot(&transaction, requested_shards)?;
    let result = classify_native_table_provisioning_snapshot(
        &snapshot,
        &declarations,
        committed_schema_digest,
    )?;
    transaction.commit().map_err(sqlite_error::storage)?;
    Ok(result)
}

/// Durably publish a pending native table-provisioning journal before any
/// shard-local sequence is seeded.
pub(super) fn begin_native_table_provisioning<F>(
    connection: &mut Connection,
    requested_shards: u16,
    declarations: Vec<TableDeclaration>,
    committed_schema_digest: [u8; 32],
    on_commit_attempted: F,
) -> EngineResult<NativeTableProvisioningClassification>
where
    F: FnOnce(),
{
    let mut on_commit_attempted = Some(on_commit_attempted);
    let declarations = normalize_table_provisioning_declarations(declarations)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let snapshot = current_manifest_snapshot(&transaction, requested_shards)?;
    match classify_native_table_provisioning_snapshot(
        &snapshot,
        &declarations,
        committed_schema_digest,
    )? {
        NativeTableProvisioningClassification::Active(active) => {
            on_commit_attempted
                .take()
                .expect("table-provisioning commit callback is one-shot")();
            transaction.commit().map_err(sqlite_error::storage)?;
            return Ok(NativeTableProvisioningClassification::Active(active));
        }
        NativeTableProvisioningClassification::Complete => {
            transaction.commit().map_err(sqlite_error::storage)?;
            return Ok(NativeTableProvisioningClassification::Complete);
        }
        NativeTableProvisioningClassification::Absent => {}
    }
    ensure_table_registration_ready(&snapshot)?;
    if snapshot.active_migration.is_some() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "table provisioning cannot run during an application-schema migration",
        ));
    }
    let integrity = snapshot.integrity.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "table provisioning requires checksummed integrity metadata",
        )
    })?;
    if integrity.committed_schema_digest() != Some(committed_schema_digest) {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "table provisioning schema digest does not match the committed schema",
        ));
    }
    let catalog = snapshot.logical_catalog.as_ref().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "table provisioning validation omitted the logical catalog",
        )
    })?;
    for declaration in declarations.iter() {
        if catalog.database_by_id(declaration.database_id()).is_none() {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!(
                    "table {} references an unknown logical database",
                    declaration.name()
                ),
            ));
        }
    }
    if !catalog.tables().is_empty() && !declarations_match_catalog_owned(&declarations, catalog) {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "the authoritative table catalog is already registered with different declarations",
        ));
    }

    let provisioning_id =
        table_provisioning_id(&declarations, requested_shards, committed_schema_digest);
    let initial_next_shard = if declarations.iter().any(|declaration| {
        matches!(
            declaration.generated_id_policy(),
            GeneratedIdPolicy::NativeRangeV1 { .. }
        )
    }) {
        0
    } else {
        requested_shards
    };
    transaction
        .execute(
            "INSERT INTO briskdb_table_provisioning (
                singleton,
                provisioning_id,
                digest_version,
                schema_digest_version,
                committed_schema_digest,
                shard_count,
                declaration_count,
                next_shard
             ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                provisioning_id.as_slice(),
                TABLE_PROVISIONING_DIGEST_VERSION,
                SCHEMA_DIGEST_VERSION,
                committed_schema_digest.as_slice(),
                requested_shards,
                i64::try_from(declarations.len()).expect("bounded declaration count fits SQLite"),
                initial_next_shard,
            ],
        )
        .map_err(sqlite_error::storage)?;
    {
        let mut insert = transaction
            .prepare(
                "INSERT INTO briskdb_table_provisioning_declarations (
                    provisioning_singleton,
                    ordinal,
                    database_id,
                    table_name,
                    placement,
                    shard_key_column,
                    shard_key_type,
                    generated_policy,
                    generated_column,
                    generated_encoding_version
                 ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )
            .map_err(sqlite_error::storage)?;
        for (ordinal, declaration) in declarations.iter().enumerate() {
            let (placement, shard_column, shard_type) =
                encoded_table_placement(declaration.placement());
            let (policy, generated_column, encoding_version) =
                encoded_generated_id_policy(declaration.generated_id_policy());
            insert
                .execute(rusqlite::params![
                    i64::try_from(ordinal).expect("bounded ordinal fits SQLite"),
                    i64::try_from(declaration.database_id().get()).map_err(|error| {
                        EngineError::from_source(
                            EngineErrorKind::NumericOutOfRange,
                            "table-provisioning database ID does not fit in SQLite",
                            error,
                        )
                    })?,
                    declaration.name(),
                    placement,
                    shard_column,
                    shard_type,
                    policy,
                    generated_column,
                    encoding_version,
                ])
                .map_err(sqlite_error::storage)?;
        }
    }
    refresh_manifest_digest(&transaction)?;
    let current = current_manifest_snapshot(&transaction, requested_shards)?;
    let active = current.active_table_provisioning.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "table provisioning did not publish its journal",
        )
    })?;
    ensure_same_native_table_provisioning_request(&active, &declarations, committed_schema_digest)?;
    on_commit_attempted
        .take()
        .expect("table-provisioning commit callback is one-shot")();
    transaction.commit().map_err(sqlite_error::storage)?;
    Ok(NativeTableProvisioningClassification::Active(active))
}

/// Advance the durable seeded-shard prefix by exactly one shard.
pub(super) fn advance_native_table_provisioning(
    connection: &mut Connection,
    requested_shards: u16,
    expected: &NativeTableProvisioning,
    next_shard: u16,
) -> EngineResult<NativeTableProvisioning> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let snapshot = current_manifest_snapshot(&transaction, requested_shards)?;
    let active = snapshot.active_table_provisioning.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "table provisioning is no longer active",
        )
    })?;
    ensure_same_native_table_provisioning(&active, expected)?;
    if next_shard == active.next_shard {
        transaction.commit().map_err(sqlite_error::storage)?;
        return Ok(active);
    }
    if next_shard != active.next_shard.saturating_add(1) || next_shard > active.shard_count {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "table-provisioning progress must advance by exactly one shard",
        ));
    }
    let changed = transaction
        .execute(
            "UPDATE briskdb_table_provisioning
             SET next_shard = ?1
             WHERE singleton = 1
               AND provisioning_id = ?2
               AND next_shard = ?3",
            rusqlite::params![
                next_shard,
                active.provisioning_id.as_slice(),
                active.next_shard,
            ],
        )
        .map_err(sqlite_error::storage)?;
    if changed != 1 {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "table-provisioning progress changed concurrently",
        ));
    }
    refresh_manifest_digest(&transaction)?;
    let advanced = current_manifest_snapshot(&transaction, requested_shards)?
        .active_table_provisioning
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                "table-provisioning progress update lost its journal",
            )
        })?;
    ensure_same_native_table_provisioning(&advanced, expected)?;
    if advanced.next_shard != next_shard {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "table-provisioning progress did not persist",
        ));
    }
    transaction.commit().map_err(sqlite_error::storage)?;
    Ok(advanced)
}

#[cfg(all(test, feature = "experimental-vtab"))]
pub(super) fn replace_allocation_owner_for_test(
    connection: &mut Connection,
    requested_shards: u16,
    retired_owner: u16,
    replacement_owner: u16,
    physical_shard: u16,
) -> EngineResult<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let snapshot = current_manifest_snapshot(&transaction, requested_shards)?;
    ensure_table_registration_ready(&snapshot)?;
    if snapshot.active_migration.is_some() || snapshot.active_table_provisioning.is_some() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "allocation-owner test transition requires no active recovery journal",
        ));
    }
    let retired_owner = AllocationOwnerSlot::new(retired_owner)?;
    let replacement_owner = AllocationOwnerSlot::new(replacement_owner)?;
    let current_owners = snapshot.allocation_owners.as_ref().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "allocation-owner test transition requires a current owner map",
        )
    })?;
    if current_owners.owner_for_physical_shard(physical_shard) != Some(retired_owner) {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "allocation-owner test transition did not identify one active owner",
        ));
    }
    let proposed = current_owners
        .assignments()
        .map(|(owner, shard, state)| {
            if owner == retired_owner.get() {
                (owner, shard, AllocationOwnerState::Retired)
            } else {
                (owner, shard, state)
            }
        })
        .chain(std::iter::once((
            replacement_owner.get(),
            physical_shard,
            AllocationOwnerState::Active,
        )))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    AllocationOwnerMap::try_from_assignments(requested_shards, proposed)?;

    let changed = transaction
        .execute(
            "UPDATE briskdb_allocation_owners
             SET owner_state = ?1
             WHERE owner_slot = ?2
               AND physical_shard_id = ?3
               AND owner_state = ?4",
            rusqlite::params![
                ALLOCATION_OWNER_RETIRED,
                retired_owner.get(),
                physical_shard,
                ALLOCATION_OWNER_ACTIVE,
            ],
        )
        .map_err(sqlite_error::storage)?;
    if changed != 1 {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "allocation-owner test transition did not identify one active owner",
        ));
    }
    transaction
        .execute(
            "INSERT INTO briskdb_allocation_owners (
                owner_slot, physical_shard_id, owner_state
             ) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                replacement_owner.get(),
                physical_shard,
                ALLOCATION_OWNER_ACTIVE,
            ],
        )
        .map_err(sqlite_error::storage)?;
    refresh_manifest_digest(&transaction)?;
    let verified = current_manifest_snapshot(&transaction, requested_shards)?;
    let owners = verified.allocation_owners.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "allocation-owner transition validation omitted its owner map",
        )
    })?;
    if owners.physical_shard(retired_owner) != Some(physical_shard)
        || owners.owner_is_active(retired_owner)
        || owners.owner_for_physical_shard(physical_shard) != Some(replacement_owner)
    {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "allocation-owner transition did not preserve historical and active routing",
        ));
    }
    transaction.commit().map_err(sqlite_error::storage)
}

#[cfg(test)]
pub(super) fn install_v9_native_catalog_for_test(
    connection: &mut Connection,
    requested_shards: u16,
    declarations: &[TableDeclaration],
) -> EngineResult<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let snapshot = current_manifest_snapshot(&transaction, requested_shards)?;
    ensure_table_registration_ready(&snapshot)?;
    if snapshot
        .logical_catalog
        .as_ref()
        .is_none_or(|catalog| !catalog.tables().is_empty())
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "v9 compatibility fixture requires an empty authoritative catalog",
        ));
    }
    insert_authoritative_table_catalog(&transaction, declarations, false)?;
    downgrade_v10_manifest_to_v9_for_test(&transaction)?;
    set_identity(&transaction, V9_SCHEMA_VERSION)?;
    refresh_manifest_digest(&transaction)?;
    let objects = schema_objects(&transaction)?;
    validate_v9(&transaction, requested_shards, &objects)?;
    transaction.commit().map_err(sqlite_error::storage)
}

#[cfg(test)]
pub(super) fn inspect_with_v9_plan_for_test(
    connection: &Connection,
    requested_shards: u16,
) -> EngineResult<()> {
    const V9_PLAN: MigrationPlan<'static> = MigrationPlan {
        current_version: V9_SCHEMA_VERSION,
        migrations: MIGRATIONS,
        initialize_current: create_v9_schema,
        initialize_interrupted_legacy: migrate_interrupted_legacy_to_v9,
    };
    inspect_with_plan(connection, requested_shards, V9_PLAN).map(|_| ())
}

#[cfg(test)]
fn downgrade_v10_manifest_to_v9_for_test(connection: &Connection) -> EngineResult<()> {
    connection
        .execute_batch(
            "DROP TABLE IF EXISTS briskdb_hilo_leases;
             DROP TABLE IF EXISTS briskdb_generated_table_ddl;
             DROP TABLE briskdb_table_provisioning_declarations;
             DROP TABLE briskdb_table_provisioning;
             DROP INDEX briskdb_one_active_owner_per_shard;
             ALTER TABLE briskdb_generated_ids RENAME TO briskdb_generated_ids_v10;
             ALTER TABLE briskdb_allocation_owners RENAME TO briskdb_allocation_owners_v10;",
        )
        .map_err(sqlite_error::storage)?;
    connection
        .execute_batch(V9_GENERATED_IDS_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    connection
        .execute(
            "INSERT INTO briskdb_generated_ids
             SELECT table_id, policy, generated_column, encoding_version
             FROM briskdb_generated_ids_v10",
            [],
        )
        .map_err(sqlite_error::storage)?;
    connection
        .execute_batch(V9_ALLOCATION_OWNERS_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    connection
        .execute(
            "INSERT INTO briskdb_allocation_owners
             SELECT owner_slot, physical_shard_id
             FROM briskdb_allocation_owners_v10",
            [],
        )
        .map_err(sqlite_error::storage)?;
    connection
        .execute_batch(
            "DROP TABLE briskdb_generated_ids_v10;
             DROP TABLE briskdb_allocation_owners_v10;
             DROP TABLE briskdb_metadata;",
        )
        .map_err(sqlite_error::storage)?;
    connection
        .execute_batch(V9_DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    connection
        .execute(
            "INSERT INTO briskdb_metadata VALUES (?1)",
            [V9_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    connection
        .execute(
            "UPDATE briskdb_integrity SET manifest_digest_version = ?1",
            [V2_MANIFEST_DIGEST_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

#[cfg(test)]
fn downgrade_v11_manifest_to_v10_for_test(
    connection: &Connection,
    shard_count: u16,
) -> EngineResult<()> {
    connection
        .execute_batch(
            "DROP TABLE briskdb_generated_table_ddl;
             DROP TABLE briskdb_hilo_leases;
             DROP TABLE briskdb_metadata;",
        )
        .map_err(sqlite_error::storage)?;
    connection
        .execute_batch(V10_DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    connection
        .execute(
            "INSERT INTO briskdb_metadata (requires_manifest_version) VALUES (?1)",
            [V10_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    connection
        .execute(
            "UPDATE briskdb_integrity SET manifest_digest_version = ?1 WHERE singleton = 1",
            [V3_MANIFEST_DIGEST_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    set_identity(connection, V10_SCHEMA_VERSION)?;
    refresh_manifest_digest(connection)?;
    validate_v10(connection, shard_count, &schema_objects(connection)?)?;
    Ok(())
}

#[cfg(test)]
fn downgrade_v12_manifest_to_v11_for_test(
    connection: &Connection,
    shard_count: u16,
) -> EngineResult<()> {
    connection
        .execute_batch(
            "DROP TABLE briskdb_generated_table_ddl;
             DROP TABLE briskdb_metadata;",
        )
        .map_err(sqlite_error::storage)?;
    connection
        .execute_batch(V11_DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    connection
        .execute(
            "INSERT INTO briskdb_metadata (requires_manifest_version) VALUES (?1)",
            [V11_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    connection
        .execute(
            "UPDATE briskdb_integrity SET manifest_digest_version = ?1 WHERE singleton = 1",
            [V4_MANIFEST_DIGEST_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    set_identity(connection, V11_SCHEMA_VERSION)?;
    refresh_manifest_digest(connection)?;
    validate_v11(connection, shard_count, &schema_objects(connection)?)?;
    Ok(())
}

/// Atomically publish the authoritative catalog and activate native policies
/// after every shard-local sequence seed is durable.
pub(super) fn finalize_native_table_provisioning<F>(
    connection: &mut Connection,
    requested_shards: u16,
    expected: &NativeTableProvisioning,
    on_commit_attempted: F,
) -> EngineResult<CatalogSnapshot>
where
    F: FnOnce(),
{
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let catalog = finalize_native_table_provisioning_in_transaction(
        &transaction,
        requested_shards,
        expected,
    )?;
    on_commit_attempted();
    transaction.commit().map_err(sqlite_error::storage)?;
    Ok(catalog)
}

fn finalize_native_table_provisioning_in_transaction(
    transaction: &Transaction<'_>,
    requested_shards: u16,
    expected: &NativeTableProvisioning,
) -> EngineResult<CatalogSnapshot> {
    let snapshot = current_manifest_snapshot(transaction, requested_shards)?;
    let Some(active) = snapshot.active_table_provisioning else {
        let declarations = expected.declarations.to_vec();
        let classification = classify_native_table_provisioning_snapshot(
            &snapshot,
            &declarations,
            expected.committed_schema_digest,
        )?;
        if classification == NativeTableProvisioningClassification::Complete {
            return catalog_snapshot_from_manifest(snapshot);
        }
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "table provisioning is no longer active",
        ));
    };
    ensure_same_native_table_provisioning(&active, expected)?;
    if active.next_shard != active.shard_count {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "table provisioning cannot finish before every shard is durable",
        ));
    }
    let catalog = snapshot.logical_catalog.as_ref().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "table provisioning finalization omitted the logical catalog",
        )
    })?;
    if catalog.tables().is_empty() {
        insert_authoritative_table_catalog(transaction, &active.declarations, true)?;
    } else if !declarations_match_catalog_owned(&active.declarations, catalog) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "table provisioning finalization found a conflicting catalog",
        ));
    } else {
        transaction
            .execute(
                "UPDATE briskdb_generated_ids
                 SET activation_state = ?1
                 WHERE policy IN (?2, ?3)",
                rusqlite::params![
                    GENERATED_ID_ACTIVE,
                    GENERATED_ID_POLICY_NATIVE_RANGE_V1,
                    GENERATED_ID_POLICY_HILO_V1,
                ],
            )
            .map_err(sqlite_error::storage)?;
    }
    transaction
        .execute(
            "INSERT INTO briskdb_hilo_leases (
                table_id, block_size, next_sequence, fence_token,
                last_owner_id, last_first_sequence, last_last_sequence
             )
             SELECT table_id, ?1, 1, 0, NULL, NULL, NULL
             FROM briskdb_generated_ids
             WHERE policy = ?2 AND activation_state = ?3
             ORDER BY table_id
             ON CONFLICT(table_id) DO NOTHING",
            rusqlite::params![
                i64::try_from(HILO_V1_BLOCK_SIZE).expect("hi/lo block size fits SQLite"),
                GENERATED_ID_POLICY_HILO_V1,
                GENERATED_ID_ACTIVE,
            ],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute("DELETE FROM briskdb_table_provisioning_declarations", [])
        .map_err(sqlite_error::storage)?;
    transaction
        .execute("DELETE FROM briskdb_table_provisioning", [])
        .map_err(sqlite_error::storage)?;
    refresh_manifest_digest(transaction)?;
    let finalized = current_manifest_snapshot(transaction, requested_shards)?;
    if finalized.active_table_provisioning.is_some()
        || !native_table_provisioning_complete(&finalized, &active.declarations)
    {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "table-provisioning finalization did not publish the active catalog",
        ));
    }
    let catalog = catalog_snapshot_from_manifest(finalized)?;
    Ok(catalog)
}

/// Publish the provisioning identity after the canonical physical migration
/// is durable. An exact repeat is idempotent.
pub(super) fn mark_generated_table_ddl_provisioning<F>(
    connection: &mut Connection,
    requested_shards: u16,
    expected: &GeneratedTableDdl,
    on_commit_attempted: F,
) -> EngineResult<GeneratedTableDdl>
where
    F: FnOnce(),
{
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let snapshot = current_manifest_snapshot(&transaction, requested_shards)?;
    let current = snapshot.generated_table_ddl.clone().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "generated-table DDL bridge is not retained",
        )
    })?;
    ensure_same_generated_table_ddl_request(
        &current,
        expected.source_dialect,
        &expected.source_sql,
        &expected.physical_sql,
        &expected.declaration,
    )?;
    if current.lifecycle == GeneratedTableDdlLifecycle::Complete
        || current.lifecycle == GeneratedTableDdlLifecycle::Provisioning
    {
        transaction.commit().map_err(sqlite_error::storage)?;
        return Ok(current);
    }
    let physical = find_schema_migration(
        &transaction,
        requested_shards,
        &current.physical_migration_id,
    )?
    .filter(SchemaMigration::is_complete)
    .ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "generated-table DDL physical migration is not complete",
        )
    })?;
    if physical.sql_text() != current.physical_sql {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "generated-table DDL physical migration changed before provisioning",
        ));
    }
    let committed_schema_digest = snapshot
        .integrity
        .and_then(ManifestIntegrity::committed_schema_digest)
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "generated-table DDL provisioning requires a committed schema checksum",
            )
        })?;
    let provisioning_id = table_provisioning_id(
        std::slice::from_ref(&current.declaration),
        requested_shards,
        committed_schema_digest,
    );
    let changed = transaction
        .execute(
            "UPDATE briskdb_generated_table_ddl
             SET lifecycle_state = ?1,
                 provisioning_id = ?2,
                 provisioning_schema_digest = ?3
             WHERE singleton = 1
               AND logical_id = ?4
               AND lifecycle_state = ?5
               AND provisioning_id IS NULL
               AND provisioning_schema_digest IS NULL
               AND table_id IS NULL",
            rusqlite::params![
                GeneratedTableDdlLifecycle::Provisioning.code(),
                provisioning_id.as_slice(),
                committed_schema_digest.as_slice(),
                current.logical_id.as_slice(),
                GeneratedTableDdlLifecycle::ApplyingPhysical.code(),
            ],
        )
        .map_err(sqlite_error::storage)?;
    if changed != 1 {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "generated-table DDL lifecycle changed concurrently",
        ));
    }
    refresh_manifest_digest(&transaction)?;
    let marked = current_manifest_snapshot(&transaction, requested_shards)?
        .generated_table_ddl
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                "generated-table DDL provisioning transition lost its bridge",
            )
        })?;
    on_commit_attempted();
    transaction.commit().map_err(sqlite_error::storage)?;
    Ok(marked)
}

/// Atomically finalize the authoritative catalog, activate its generated-ID
/// policy, and seal the retained DDL bridge as complete.
pub(super) fn finalize_generated_table_ddl_provisioning<F>(
    connection: &mut Connection,
    requested_shards: u16,
    expected_ddl: &GeneratedTableDdl,
    expected_provisioning: &NativeTableProvisioning,
    on_commit_attempted: F,
) -> EngineResult<(CatalogSnapshot, GeneratedTableDdl)>
where
    F: FnOnce(),
{
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let snapshot = current_manifest_snapshot(&transaction, requested_shards)?;
    let current = snapshot.generated_table_ddl.clone().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "generated-table DDL bridge is not retained",
        )
    })?;
    ensure_same_generated_table_ddl_request(
        &current,
        expected_ddl.source_dialect,
        &expected_ddl.source_sql,
        &expected_ddl.physical_sql,
        &expected_ddl.declaration,
    )?;
    if current.lifecycle == GeneratedTableDdlLifecycle::Complete {
        let catalog = catalog_snapshot_from_manifest(snapshot)?;
        transaction.commit().map_err(sqlite_error::storage)?;
        return Ok((catalog, current));
    }
    if current.lifecycle != GeneratedTableDdlLifecycle::Provisioning
        || current.provisioning_id != Some(expected_provisioning.provisioning_id())
        || current.provisioning_schema_digest
            != Some(expected_provisioning.committed_schema_digest())
        || expected_provisioning.declarations() != std::slice::from_ref(&current.declaration)
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "generated-table DDL bridge does not match the table provisioning being finalized",
        ));
    }
    let catalog = finalize_native_table_provisioning_in_transaction(
        &transaction,
        requested_shards,
        expected_provisioning,
    )?;
    let table_id = catalog
        .logical()
        .tables()
        .iter()
        .find(|table| {
            table.database_id() == current.declaration.database_id()
                && table.name() == current.declaration.name()
        })
        .map(TableMetadata::id)
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                "generated-table DDL finalization did not publish its table",
            )
        })?;
    let changed = transaction
        .execute(
            "UPDATE briskdb_generated_table_ddl
             SET lifecycle_state = ?1, table_id = ?2
             WHERE singleton = 1
               AND logical_id = ?3
               AND lifecycle_state = ?4
               AND provisioning_id = ?5
               AND provisioning_schema_digest = ?6
               AND table_id IS NULL",
            rusqlite::params![
                GeneratedTableDdlLifecycle::Complete.code(),
                i64::try_from(table_id.get()).expect("bounded table ID fits SQLite"),
                current.logical_id.as_slice(),
                GeneratedTableDdlLifecycle::Provisioning.code(),
                expected_provisioning.provisioning_id().as_slice(),
                expected_provisioning.committed_schema_digest().as_slice(),
            ],
        )
        .map_err(sqlite_error::storage)?;
    if changed != 1 {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "generated-table DDL lifecycle changed concurrently",
        ));
    }
    refresh_manifest_digest(&transaction)?;
    let completed = current_manifest_snapshot(&transaction, requested_shards)?
        .generated_table_ddl
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                "generated-table DDL completion lost its bridge",
            )
        })?;
    if completed.lifecycle != GeneratedTableDdlLifecycle::Complete
        || completed.table_id != Some(table_id)
    {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "generated-table DDL completion did not persist",
        ));
    }
    on_commit_attempted();
    transaction.commit().map_err(sqlite_error::storage)?;
    Ok((catalog, completed))
}

fn classify_native_table_provisioning_snapshot(
    snapshot: &ManifestSnapshot,
    declarations: &[TableDeclaration],
    committed_schema_digest: [u8; 32],
) -> EngineResult<NativeTableProvisioningClassification> {
    if let Some(active) = snapshot.active_table_provisioning.as_ref() {
        ensure_same_native_table_provisioning_request(
            active,
            declarations,
            committed_schema_digest,
        )?;
        return Ok(NativeTableProvisioningClassification::Active(
            active.clone(),
        ));
    }
    if native_table_provisioning_complete(snapshot, declarations) {
        return Ok(NativeTableProvisioningClassification::Complete);
    }
    Ok(NativeTableProvisioningClassification::Absent)
}

fn classify_generated_table_ddl_snapshot(
    snapshot: &ManifestSnapshot,
    source_dialect: SqlDialect,
    source_sql: &str,
    physical_sql: &str,
    declaration: &TableDeclaration,
) -> EngineResult<GeneratedTableDdlClassification> {
    validate_generated_table_ddl_declaration(declaration)?;
    validate_schema_migration_sql(source_sql)?;
    validate_schema_migration_sql(physical_sql)?;
    let Some(existing) = snapshot.generated_table_ddl.as_ref() else {
        return Ok(GeneratedTableDdlClassification::Absent);
    };
    ensure_same_generated_table_ddl_request(
        existing,
        source_dialect,
        source_sql,
        physical_sql,
        declaration,
    )?;
    Ok(GeneratedTableDdlClassification::Existing(existing.clone()))
}

fn ensure_same_generated_table_ddl_request(
    existing: &GeneratedTableDdl,
    source_dialect: SqlDialect,
    source_sql: &str,
    physical_sql: &str,
    declaration: &TableDeclaration,
) -> EngineResult<()> {
    let logical_id = generated_table_ddl_logical_id(source_dialect, source_sql)?;
    let physical_migration_id = schema_migration_id(physical_sql)?;
    if existing.logical_id != logical_id
        || existing.source_dialect != source_dialect
        || existing.translation_version != GENERATED_TABLE_DDL_TRANSLATION_VERSION
        || existing.source_sql != source_sql
        || existing.physical_migration_id != physical_migration_id
        || existing.physical_sql != physical_sql
        || existing.declaration != *declaration
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "a different generated-table DDL bridge request is already retained",
        ));
    }
    Ok(())
}

fn native_table_provisioning_complete(
    snapshot: &ManifestSnapshot,
    declarations: &[TableDeclaration],
) -> bool {
    let Some(catalog) = snapshot.logical_catalog.as_ref() else {
        return false;
    };
    if !declarations_match_catalog_owned(declarations, catalog) {
        return false;
    }
    let expected_native = catalog
        .tables()
        .iter()
        .filter(|table| {
            matches!(
                table.generated_id_policy(),
                GeneratedIdPolicy::NativeRangeV1 { .. }
            )
        })
        .map(TableMetadata::id)
        .collect::<Vec<_>>();
    let expected_hilo = catalog
        .tables()
        .iter()
        .filter(|table| {
            matches!(
                table.generated_id_policy(),
                GeneratedIdPolicy::HiloV1 { .. }
            )
        })
        .map(TableMetadata::id)
        .collect::<Vec<_>>();
    expected_native.as_slice() == snapshot.active_native_id_table_ids.as_ref()
        && expected_hilo.as_slice() == snapshot.active_hilo_id_table_ids.as_ref()
}

fn ensure_same_native_table_provisioning_request(
    active: &NativeTableProvisioning,
    declarations: &[TableDeclaration],
    committed_schema_digest: [u8; 32],
) -> EngineResult<()> {
    if active.declarations.as_ref() != declarations
        || active.committed_schema_digest != committed_schema_digest
        || active.provisioning_id
            != table_provisioning_id(declarations, active.shard_count, committed_schema_digest)
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "a different table-provisioning operation is already active",
        ));
    }
    Ok(())
}

fn ensure_same_native_table_provisioning(
    observed: &NativeTableProvisioning,
    expected: &NativeTableProvisioning,
) -> EngineResult<()> {
    if observed.provisioning_id != expected.provisioning_id
        || observed.committed_schema_digest != expected.committed_schema_digest
        || observed.shard_count != expected.shard_count
        || observed.declarations != expected.declarations
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "table-provisioning identity changed while it was being applied",
        ));
    }
    Ok(())
}

/// Atomically install the complete authoritative table catalog.
///
/// Physical-table and emptiness validation belongs to the storage coordinator,
/// which holds the root schema gate before entering this manifest transaction.
/// This layer revalidates the current checksummed manifest under its write lock,
/// assigns deterministic IDs, refreshes the semantic root in the same
/// transaction, and returns only the fully revalidated replacement snapshot.
/// An exact repeat is a read-only idempotent success. Every other attempt to
/// replace an already-populated authoritative catalog fails closed.
pub(super) fn register_table_catalog<F>(
    connection: &mut Connection,
    requested_shards: u16,
    declarations: Vec<TableDeclaration>,
    on_commit_attempted: F,
) -> EngineResult<CatalogSnapshot>
where
    F: FnOnce(),
{
    if declarations.is_empty() {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "table registration requires at least one declaration",
        ));
    }
    if declarations.len() > MAX_TABLES {
        return Err(EngineError::new(
            EngineErrorKind::LimitExceeded,
            format!("table registration exceeds its {MAX_TABLES}-table limit"),
        ));
    }

    let mut declarations = declarations
        .into_iter()
        .map(TableDeclaration::into_parts)
        .collect::<Vec<_>>();
    declarations.sort_by(|left, right| (left.0, left.1.as_str()).cmp(&(right.0, right.1.as_str())));
    if declarations
        .windows(2)
        .any(|rows| (rows[0].0, rows[0].1.as_str()) == (rows[1].0, rows[1].1.as_str()))
    {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "table registration contains a duplicate logical table",
        ));
    }

    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let current = current_manifest_snapshot(&transaction, requested_shards)?;
    if current.active_table_provisioning.is_some() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "table registration cannot run during native table provisioning",
        ));
    }
    ensure_table_registration_ready(&current)?;
    let current_catalog = current.logical_catalog.as_ref().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "current manifest validation omitted its logical catalog",
        )
    })?;
    for (database_id, table_name, _, _) in &declarations {
        if current_catalog.database_by_id(*database_id).is_none() {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!("table {table_name} references an unknown logical database"),
            ));
        }
    }

    if !current_catalog.tables().is_empty() {
        if declarations_match_catalog(&declarations, current_catalog) {
            let current = catalog_snapshot_from_manifest(current)?;
            transaction.commit().map_err(sqlite_error::storage)?;
            return Ok(current);
        }
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "the authoritative table catalog is already registered with different declarations",
        ));
    }

    {
        let mut insert = transaction
            .prepare(
                "INSERT INTO briskdb_tables (
                    table_id,
                    database_id,
                    table_name,
                    placement,
                    shard_key_column,
                    shard_key_type
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .map_err(sqlite_error::storage)?;
        for (index, (database_id, table_name, placement, generated_id_policy)) in
            declarations.iter().enumerate()
        {
            let table_id = i64::try_from(index + 1).expect("bounded table ID fits in SQLite");
            let database_id = i64::try_from(database_id.get()).map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::NumericOutOfRange,
                    format!("logical database ID for table {table_name} does not fit in SQLite"),
                    error,
                )
            })?;
            let (placement, shard_key_column, shard_key_type) = encoded_table_placement(placement);
            insert
                .execute(rusqlite::params![
                    table_id,
                    database_id,
                    table_name,
                    placement,
                    shard_key_column,
                    shard_key_type,
                ])
                .map_err(sqlite_error::storage)?;
            let (policy, generated_column, encoding_version) =
                encoded_generated_id_policy(generated_id_policy);
            transaction
                .execute(
                    "INSERT INTO briskdb_generated_ids (
                        table_id,
                        policy,
                        generated_column,
                        encoding_version,
                        activation_state
                     ) VALUES (?1, ?2, ?3, ?4, ?5)",
                    rusqlite::params![
                        table_id,
                        policy,
                        generated_column,
                        encoding_version,
                        GENERATED_ID_INACTIVE,
                    ],
                )
                .map_err(sqlite_error::storage)?;
        }
    }

    refresh_manifest_digest(&transaction)?;
    let replacement = current_manifest_snapshot(&transaction, requested_shards)?;
    ensure_table_registration_ready(&replacement)?;
    let replacement = catalog_snapshot_from_manifest(replacement)?;
    if !declarations_match_catalog(&declarations, replacement.logical()) {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "table registration did not preserve its complete declaration set",
        ));
    }

    // From this point, a failed COMMIT can be durability-ambiguous. The root
    // coordinator uses this boundary to stop ordinary admission until it has
    // reconciled the checksummed manifest to either the old or new snapshot.
    on_commit_attempted();
    #[cfg(test)]
    abort_table_registration_at_test_boundary("before-commit");
    transaction.commit().map_err(sqlite_error::storage)?;
    #[cfg(test)]
    abort_table_registration_at_test_boundary("after-commit");
    #[cfg(test)]
    if FAIL_NEXT_TABLE_REGISTRATION_POST_COMMIT.with(|fail| fail.replace(false)) {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "injected table-registration post-commit publication failure",
        ));
    }
    Ok(replacement)
}

fn insert_authoritative_table_catalog(
    transaction: &Transaction<'_>,
    declarations: &[TableDeclaration],
    activate_native: bool,
) -> EngineResult<()> {
    let mut insert_table = transaction
        .prepare(
            "INSERT INTO briskdb_tables (
                table_id,
                database_id,
                table_name,
                placement,
                shard_key_column,
                shard_key_type
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .map_err(sqlite_error::storage)?;
    let mut insert_policy = transaction
        .prepare(
            "INSERT INTO briskdb_generated_ids (
                table_id,
                policy,
                generated_column,
                encoding_version,
                activation_state
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .map_err(sqlite_error::storage)?;
    for (index, declaration) in declarations.iter().enumerate() {
        let table_id = i64::try_from(index + 1).expect("bounded table ID fits in SQLite");
        let database_id = i64::try_from(declaration.database_id().get()).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::NumericOutOfRange,
                format!(
                    "logical database ID for table {} does not fit in SQLite",
                    declaration.name()
                ),
                error,
            )
        })?;
        let (placement, shard_key_column, shard_key_type) =
            encoded_table_placement(declaration.placement());
        insert_table
            .execute(rusqlite::params![
                table_id,
                database_id,
                declaration.name(),
                placement,
                shard_key_column,
                shard_key_type,
            ])
            .map_err(sqlite_error::storage)?;
        let (policy, generated_column, encoding_version) =
            encoded_generated_id_policy(declaration.generated_id_policy());
        let activation_state = if activate_native
            && !matches!(declaration.generated_id_policy(), GeneratedIdPolicy::None)
        {
            GENERATED_ID_ACTIVE
        } else {
            GENERATED_ID_INACTIVE
        };
        insert_policy
            .execute(rusqlite::params![
                table_id,
                policy,
                generated_column,
                encoding_version,
                activation_state,
            ])
            .map_err(sqlite_error::storage)?;
    }
    Ok(())
}

fn ensure_table_registration_ready(snapshot: &ManifestSnapshot) -> EngineResult<()> {
    if snapshot.active_migration.is_some() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "table registration cannot run during an application-schema migration",
        ));
    }
    if snapshot.integrity.map(ManifestIntegrity::state) != Some(DatabaseIntegrityState::Ready) {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "table registration requires a ready checksummed manifest",
        ));
    }
    if snapshot
        .shard_layout
        .as_ref()
        .is_none_or(|layout| layout.state() != ShardLayoutState::Ready)
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "table registration requires a ready physical shard layout",
        ));
    }
    Ok(())
}

fn catalog_snapshot_from_manifest(snapshot: ManifestSnapshot) -> EngineResult<CatalogSnapshot> {
    let catalog = catalog_snapshot_from_parts(
        snapshot.routing_catalog,
        snapshot.logical_catalog,
        snapshot.allocation_owners,
    )?;
    Ok(catalog
        .with_active_native_id_table_ids(snapshot.active_native_id_table_ids)
        .with_active_hilo_id_table_ids(snapshot.active_hilo_id_table_ids))
}

fn catalog_snapshot_from_parts(
    routing: Option<RoutingCatalog>,
    logical: Option<Catalog>,
    allocation_owners: Option<AllocationOwnerMap>,
) -> EngineResult<CatalogSnapshot> {
    let routing = routing.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "current manifest validation omitted its routing catalog",
        )
    })?;
    let logical = logical.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "current manifest validation omitted its logical catalog",
        )
    })?;
    Ok(match allocation_owners {
        Some(allocation_owners) => CatalogSnapshot::from_validated_parts_with_allocation_owners(
            routing,
            logical,
            allocation_owners,
        ),
        None => CatalogSnapshot::from_validated_parts(routing, logical),
    })
}

fn declarations_match_catalog(
    declarations: &[(
        crate::core::LogicalDatabaseId,
        String,
        TablePlacement,
        GeneratedIdPolicy,
    )],
    catalog: &Catalog,
) -> bool {
    declarations.len() == catalog.tables().len()
        && declarations.iter().zip(catalog.tables()).all(
            |((database_id, name, placement, generated_id_policy), table)| {
                *database_id == table.database_id()
                    && name == table.name()
                    && placement == table.placement()
                    && generated_id_policy == table.generated_id_policy()
            },
        )
}

fn declarations_match_catalog_owned(declarations: &[TableDeclaration], catalog: &Catalog) -> bool {
    declarations.len() == catalog.tables().len()
        && declarations
            .iter()
            .zip(catalog.tables())
            .all(|(declaration, table)| {
                declaration.database_id() == table.database_id()
                    && declaration.name() == table.name()
                    && declaration.placement() == table.placement()
                    && declaration.generated_id_policy() == table.generated_id_policy()
            })
}

fn is_sorted_unique_declarations(declarations: &[TableDeclaration]) -> bool {
    declarations.windows(2).all(|rows| {
        (rows[0].database_id(), rows[0].name()) < (rows[1].database_id(), rows[1].name())
    })
}

fn normalize_table_provisioning_declarations(
    declarations: Vec<TableDeclaration>,
) -> EngineResult<Box<[TableDeclaration]>> {
    if declarations.is_empty() {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "table provisioning requires at least one declaration",
        ));
    }
    if declarations.len() > MAX_TABLES {
        return Err(EngineError::new(
            EngineErrorKind::LimitExceeded,
            format!("table provisioning exceeds its {MAX_TABLES}-table limit"),
        ));
    }
    let mut declarations = declarations;
    declarations.sort_by(|left, right| {
        (left.database_id(), left.name()).cmp(&(right.database_id(), right.name()))
    });
    if !is_sorted_unique_declarations(&declarations) {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "table provisioning contains a duplicate logical table",
        ));
    }
    if !declarations
        .iter()
        .any(|declaration| !matches!(declaration.generated_id_policy(), GeneratedIdPolicy::None))
    {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "table provisioning requires a generated-ID declaration",
        ));
    }
    Ok(declarations.into_boxed_slice())
}

fn table_provisioning_id(
    declarations: &[TableDeclaration],
    shard_count: u16,
    committed_schema_digest: [u8; 32],
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(TABLE_PROVISIONING_DIGEST_DOMAIN);
    hasher.update(&TABLE_PROVISIONING_DIGEST_VERSION.to_le_bytes());
    hasher.update(&shard_count.to_le_bytes());
    hasher.update(&committed_schema_digest);
    hasher.update(
        &u64::try_from(declarations.len())
            .expect("bounded declaration count fits u64")
            .to_le_bytes(),
    );
    for declaration in declarations {
        hasher.update(&declaration.database_id().get().to_le_bytes());
        hash_manifest_name(&mut hasher, declaration.name().as_bytes());
        let (placement, column, key_type) = encoded_table_placement(declaration.placement());
        hasher.update(&placement.to_le_bytes());
        hash_optional_provisioning_text(&mut hasher, column);
        hash_optional_provisioning_integer(&mut hasher, key_type);
        let (policy, generated_column, encoding_version) =
            encoded_generated_id_policy(declaration.generated_id_policy());
        hasher.update(&policy.to_le_bytes());
        hash_optional_provisioning_text(&mut hasher, generated_column);
        hash_optional_provisioning_integer(&mut hasher, encoding_version);
    }
    *hasher.finalize().as_bytes()
}

fn hash_optional_provisioning_text(hasher: &mut blake3::Hasher, value: Option<&str>) {
    match value {
        None => {
            hasher.update(&[0]);
        }
        Some(value) => {
            hasher.update(&[1]);
            hash_manifest_name(hasher, value.as_bytes());
        }
    }
}

fn hash_optional_provisioning_integer(hasher: &mut blake3::Hasher, value: Option<i64>) {
    match value {
        None => {
            hasher.update(&[0]);
        }
        Some(value) => {
            hasher.update(&[1]);
            hasher.update(&value.to_le_bytes());
        }
    }
}

fn encoded_generated_id_policy(policy: &GeneratedIdPolicy) -> (i64, Option<&str>, Option<i64>) {
    match policy {
        GeneratedIdPolicy::None => (GENERATED_ID_POLICY_NONE, None, None),
        GeneratedIdPolicy::NativeRangeV1 { column } => (
            GENERATED_ID_POLICY_NATIVE_RANGE_V1,
            Some(column),
            Some(i64::from(NATIVE_RANGE_V1_ENCODING_VERSION)),
        ),
        GeneratedIdPolicy::HiloV1 { column } => (
            GENERATED_ID_POLICY_HILO_V1,
            Some(column),
            Some(i64::from(HILO_V1_ENCODING_VERSION)),
        ),
    }
}

fn encoded_table_placement(placement: &TablePlacement) -> (i64, Option<&str>, Option<i64>) {
    match placement {
        TablePlacement::Sharded(shard_key) => (
            SHARDED_PLACEMENT,
            Some(shard_key.column()),
            Some(match shard_key.key_type() {
                ShardKeyType::Int64 => INT64_SHARD_KEY_TYPE,
                ShardKeyType::Text => TEXT_SHARD_KEY_TYPE,
                ShardKeyType::Binary => BINARY_SHARD_KEY_TYPE,
            }),
        ),
        TablePlacement::Global => (GLOBAL_PLACEMENT, None, None),
        TablePlacement::Catalog => (CATALOG_PLACEMENT, None, None),
    }
}

fn find_schema_migration(
    connection: &Connection,
    expected_shard_count: u16,
    migration_id: &[u8; 32],
) -> EngineResult<Option<SchemaMigration>> {
    connection
        .query_row(
            "SELECT target_generation,
                    source_generation,
                    migration_id,
                    digest_version,
                    sql_text,
                    shard_count,
                    migration_state,
                    next_shard
             FROM briskdb_schema_migrations
             WHERE migration_id = ?1",
            [migration_id.as_slice()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .map(Some)
        .or_else(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            error => Err(error),
        })
        .map_err(|error| manifest_read_error(error, "failed to read schema migration journal"))?
        .map(|stored| schema_migration_from_stored(stored, expected_shard_count))
        .transpose()
}

fn classify_matching_schema_migration(
    migration: Option<SchemaMigration>,
    sql: &str,
) -> EngineResult<SchemaMigrationClassification> {
    let Some(migration) = migration else {
        return Ok(SchemaMigrationClassification::Absent);
    };
    if migration.sql_text != sql {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "schema migration identifier collides with different SQL bytes",
        ));
    }
    if migration.is_applying() {
        Ok(SchemaMigrationClassification::Active(migration))
    } else {
        Ok(SchemaMigrationClassification::Complete(migration))
    }
}

fn ensure_same_schema_migration(
    observed: &SchemaMigration,
    expected: &SchemaMigration,
) -> EngineResult<()> {
    if observed.source_generation != expected.source_generation
        || observed.target_generation != expected.target_generation
        || observed.migration_id != expected.migration_id
        || observed.sql_text != expected.sql_text
        || observed.shard_count != expected.shard_count
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "schema migration identity changed while it was being applied",
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn mark_shard_layout_ready(
    connection: &mut Connection,
    requested_shards: u16,
    expected: &ShardLayout,
) -> EngineResult<()> {
    reconcile_shard_layout(connection, requested_shards, expected, |_| Ok(())).map(|_| ())
}

/// Serialize physical reconciliation and the final `Ready` publication under
/// one manifest write lock. The callback receives the state re-read after that
/// lock is acquired, so a lagging opener can never provision from a stale
/// `Creating` observation after another opener has committed `Ready`.
pub(super) fn reconcile_shard_layout<F>(
    connection: &mut Connection,
    requested_shards: u16,
    expected: &ShardLayout,
    reconcile: F,
) -> EngineResult<ShardLayout>
where
    F: FnOnce(&ShardLayout) -> EngineResult<()>,
{
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let observed = match inspect_with_plan(&transaction, requested_shards, CURRENT_PLAN)? {
        ManifestState::Versioned { version, snapshot } if version == CURRENT_SCHEMA_VERSION => {
            snapshot.shard_layout.ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    "current manifest validation omitted its physical shard layout",
                )
            })?
        }
        _ => {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "physical shard layout changed before it could be marked ready",
            ));
        }
    };
    ensure_same_shard_layout(&observed, expected)?;
    reconcile(&observed)?;

    if observed.state() != ShardLayoutState::Ready {
        transaction
            .execute(
                "UPDATE briskdb_shard_layout
                 SET layout_state = ?1
                 WHERE singleton = 1 AND layout_id = ?2",
                rusqlite::params![
                    ShardLayoutState::Ready.code(),
                    expected.layout_id().as_slice()
                ],
            )
            .map_err(sqlite_error::storage)?;
        refresh_manifest_digest_if_checksummed(&transaction)?;

        let ready = match inspect_with_plan(&transaction, requested_shards, CURRENT_PLAN)? {
            ManifestState::Versioned { version, snapshot } if version == CURRENT_SCHEMA_VERSION => {
                snapshot.shard_layout.ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::Internal,
                        "ready manifest validation omitted its physical shard layout",
                    )
                })?
            }
            _ => {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "physical shard layout ready transition did not produce a current manifest",
                ));
            }
        };
        ensure_same_shard_layout(&ready, expected)?;
        if ready.state() != ShardLayoutState::Ready {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "physical shard layout did not persist its ready state",
            ));
        }
        transaction.commit().map_err(sqlite_error::storage)?;
        return Ok(ready);
    }

    transaction.commit().map_err(sqlite_error::storage)?;
    Ok(observed)
}

fn ensure_same_shard_layout(observed: &ShardLayout, expected: &ShardLayout) -> EngineResult<()> {
    if observed.layout_id() != expected.layout_id()
        || observed.expected_application_id() != expected.expected_application_id()
        || observed.metadata_version() != expected.metadata_version()
        || (observed.state() != expected.state() && observed.state() != ShardLayoutState::Ready)
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "physical shard layout identity changed during startup",
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn load_or_create(
    connection: &mut Connection,
    requested_shards: u16,
) -> EngineResult<u16> {
    load_or_create_with_hook(connection, requested_shards, |_| Ok(()))
}

#[cfg(test)]
fn load_or_create_with_hook<F>(
    connection: &mut Connection,
    requested_shards: u16,
    mut hook: F,
) -> EngineResult<u16>
where
    F: FnMut(MigrationPoint) -> EngineResult<()>,
{
    load_or_create_snapshot_with_plan(connection, requested_shards, CURRENT_PLAN, true, &mut hook)
        .map(|snapshot| snapshot.shard_count)
}

fn load_or_create_snapshot_with_plan<F>(
    connection: &mut Connection,
    requested_shards: u16,
    plan: MigrationPlan<'_>,
    fresh_layout_allowed: bool,
    hook: &mut F,
) -> EngineResult<ManifestSnapshot>
where
    F: FnMut(MigrationPoint) -> EngineResult<()>,
{
    loop {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error::storage)?;

        let (from, shard_count) = match inspect_with_plan(&transaction, requested_shards, plan)? {
            ManifestState::Versioned { version, snapshot } if version == plan.current_version => {
                transaction.commit().map_err(sqlite_error::storage)?;
                return Ok(*snapshot);
            }
            ManifestState::Empty => {
                if !fresh_layout_allowed {
                    return Err(EngineError::new(
                        EngineErrorKind::FailedPrecondition,
                        "cannot initialize an empty manifest beside an existing shard layout",
                    ));
                }
                return apply_schema_change(
                    transaction,
                    requested_shards,
                    SchemaChange {
                        from: 0,
                        to: plan.current_version,
                        apply: plan.initialize_current,
                    },
                    plan,
                    hook,
                );
            }
            ManifestState::LegacyUninitialized => {
                if !fresh_layout_allowed {
                    return Err(EngineError::new(
                        EngineErrorKind::FailedPrecondition,
                        "cannot recover interrupted legacy initialization beside an existing shard layout",
                    ));
                }
                return apply_schema_change(
                    transaction,
                    requested_shards,
                    SchemaChange {
                        from: LEGACY_SCHEMA_VERSION,
                        to: plan.current_version,
                        apply: plan.initialize_interrupted_legacy,
                    },
                    plan,
                    hook,
                );
            }
            ManifestState::LegacyV1 { shard_count } => (LEGACY_SCHEMA_VERSION, shard_count),
            ManifestState::Versioned { version, snapshot } => (version, snapshot.shard_count),
        };

        let migration = migration_from(plan.migrations, from)?;
        if migration.to <= migration.from || migration.to > plan.current_version {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                format!(
                    "invalid manifest migration {} -> {} ({})",
                    migration.from, migration.to, migration.name
                ),
            ));
        }
        let migrated = apply_schema_change(
            transaction,
            shard_count,
            SchemaChange {
                from,
                to: migration.to,
                apply: migration.apply,
            },
            plan,
            hook,
        )?;
        if migration.to == plan.current_version {
            return Ok(migrated);
        }
    }
}

fn migration_from(migrations: &[Migration], version: u32) -> EngineResult<Migration> {
    migrations
        .iter()
        .copied()
        .find(|migration| migration.from == version)
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                format!("no manifest migration starts at schema version {version}"),
            )
        })
}

fn apply_schema_change<F>(
    transaction: Transaction<'_>,
    shard_count: u16,
    change: SchemaChange,
    plan: MigrationPlan<'_>,
    hook: &mut F,
) -> EngineResult<ManifestSnapshot>
where
    F: FnMut(MigrationPoint) -> EngineResult<()>,
{
    (change.apply)(&transaction, shard_count)?;
    hook(MigrationPoint {
        from: change.from,
        to: change.to,
        phase: MigrationPhase::AfterSchemaChange,
    })?;

    set_identity(&transaction, change.to)?;
    if change.to >= V7_SCHEMA_VERSION {
        refresh_manifest_digest(&transaction)?;
    }
    hook(MigrationPoint {
        from: change.from,
        to: change.to,
        phase: MigrationPhase::AfterVersionStamp,
    })?;

    match inspect_with_plan(&transaction, shard_count, plan)? {
        ManifestState::Versioned { version, snapshot } if version == change.to => {
            transaction.commit().map_err(sqlite_error::storage)?;
            Ok(*snapshot)
        }
        _ => Err(EngineError::new(
            EngineErrorKind::Internal,
            format!(
                "manifest migration {} -> {} did not produce its declared schema",
                change.from, change.to
            ),
        )),
    }
}

fn create_v2_schema(transaction: &Transaction<'_>, shard_count: u16) -> EngineResult<()> {
    transaction
        .execute_batch(V2_MANIFEST_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V2_DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    insert_v2_rows(transaction, shard_count)
}

fn migrate_v1_to_v2(transaction: &Transaction<'_>, shard_count: u16) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    create_v2_schema(transaction, shard_count)
}

fn insert_v2_rows(transaction: &Transaction<'_>, shard_count: u16) -> EngineResult<()> {
    transaction
        .execute(
            "INSERT INTO briskdb_manifest (singleton, shard_count) VALUES (1, ?1)",
            [shard_count],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_metadata (requires_manifest_version) VALUES (?1)",
            [V2_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn create_v3_schema(transaction: &Transaction<'_>, shard_count: u16) -> EngineResult<()> {
    create_v2_schema(transaction, shard_count)?;
    migrate_v2_to_v3(transaction, shard_count)
}

fn create_v4_schema(transaction: &Transaction<'_>, shard_count: u16) -> EngineResult<()> {
    create_v3_schema(transaction, shard_count)?;
    migrate_v3_to_v4(transaction, shard_count)
}

fn create_v5_schema(transaction: &Transaction<'_>, shard_count: u16) -> EngineResult<()> {
    create_v4_schema(transaction, shard_count)?;
    add_v5_schema(transaction, ShardLayoutState::Creating)
}

fn create_v6_schema(transaction: &Transaction<'_>, shard_count: u16) -> EngineResult<()> {
    create_v5_schema(transaction, shard_count)?;
    migrate_v5_to_v6(transaction, shard_count)
}

fn create_v7_schema(transaction: &Transaction<'_>, shard_count: u16) -> EngineResult<()> {
    create_v6_schema(transaction, shard_count)?;
    migrate_v6_to_v7(transaction, shard_count)
}

fn create_v8_schema(transaction: &Transaction<'_>, shard_count: u16) -> EngineResult<()> {
    create_v7_schema(transaction, shard_count)?;
    migrate_v7_to_v8(transaction, shard_count)
}

fn create_v9_schema(transaction: &Transaction<'_>, shard_count: u16) -> EngineResult<()> {
    create_v8_schema(transaction, shard_count)?;
    migrate_v8_to_v9(transaction, shard_count)
}

fn create_v10_schema(transaction: &Transaction<'_>, shard_count: u16) -> EngineResult<()> {
    create_v9_schema(transaction, shard_count)?;
    migrate_v9_to_v10(transaction, shard_count)
}

fn create_v11_schema(transaction: &Transaction<'_>, shard_count: u16) -> EngineResult<()> {
    create_v10_schema(transaction, shard_count)?;
    migrate_v10_to_v11(transaction, shard_count)
}

fn create_v12_schema(transaction: &Transaction<'_>, shard_count: u16) -> EngineResult<()> {
    create_v11_schema(transaction, shard_count)?;
    migrate_v11_to_v12(transaction, shard_count)
}

fn migrate_interrupted_legacy_to_v6(
    transaction: &Transaction<'_>,
    shard_count: u16,
) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    create_v6_schema(transaction, shard_count)
}

#[cfg(test)]
fn migrate_interrupted_legacy_to_v7(
    transaction: &Transaction<'_>,
    shard_count: u16,
) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    create_v7_schema(transaction, shard_count)
}

#[cfg(test)]
#[allow(dead_code)]
fn migrate_interrupted_legacy_to_v8(
    transaction: &Transaction<'_>,
    shard_count: u16,
) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    create_v8_schema(transaction, shard_count)
}

#[cfg(test)]
fn migrate_interrupted_legacy_to_v9(
    transaction: &Transaction<'_>,
    shard_count: u16,
) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    create_v9_schema(transaction, shard_count)
}

#[cfg(test)]
#[allow(dead_code)]
fn migrate_interrupted_legacy_to_v10(
    transaction: &Transaction<'_>,
    shard_count: u16,
) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    create_v10_schema(transaction, shard_count)
}

#[cfg(test)]
fn migrate_interrupted_legacy_to_v11(
    transaction: &Transaction<'_>,
    shard_count: u16,
) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    create_v11_schema(transaction, shard_count)
}

fn migrate_interrupted_legacy_to_v12(
    transaction: &Transaction<'_>,
    shard_count: u16,
) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    create_v12_schema(transaction, shard_count)
}

#[cfg(test)]
fn migrate_interrupted_legacy_to_v3(
    transaction: &Transaction<'_>,
    shard_count: u16,
) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    create_v3_schema(transaction, shard_count)
}

#[cfg(test)]
fn migrate_interrupted_legacy_to_v4(
    transaction: &Transaction<'_>,
    shard_count: u16,
) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    create_v4_schema(transaction, shard_count)
}

#[cfg(test)]
fn migrate_interrupted_legacy_to_v5(
    transaction: &Transaction<'_>,
    shard_count: u16,
) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    create_v5_schema(transaction, shard_count)
}

fn migrate_v2_to_v3(transaction: &Transaction<'_>, shard_count: u16) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V3_DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_metadata (requires_manifest_version) VALUES (?1)",
            [V3_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;

    transaction
        .execute_batch(V3_ROUTING_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V3_PHYSICAL_SHARDS_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V3_VIRTUAL_BUCKETS_TABLE_SQL)
        .map_err(sqlite_error::storage)?;

    transaction
        .execute(
            "INSERT INTO briskdb_routing (
                singleton,
                hash_version,
                key_encoding_version,
                bucket_algorithm_version,
                virtual_bucket_count,
                map_generation
             ) VALUES (1, ?1, ?2, ?3, ?4, ?5)",
            [
                i64::from(HASH_VERSION),
                i64::from(KEY_ENCODING_VERSION),
                i64::from(BUCKET_ALGORITHM_VERSION),
                i64::from(VIRTUAL_BUCKET_COUNT),
                i64::try_from(INITIAL_MAP_GENERATION).expect("initial generation fits in SQLite"),
            ],
        )
        .map_err(sqlite_error::storage)?;

    {
        let mut insert_shard = transaction
            .prepare(
                "INSERT INTO briskdb_physical_shards (shard_id, lifecycle_state)
                 VALUES (?1, ?2)",
            )
            .map_err(sqlite_error::storage)?;
        for shard_id in 0..shard_count {
            insert_shard
                .execute(rusqlite::params![
                    i64::from(shard_id),
                    ACTIVE_LIFECYCLE_STATE
                ])
                .map_err(sqlite_error::storage)?;
        }
    }

    {
        let mut insert_bucket = transaction
            .prepare(
                "INSERT INTO briskdb_virtual_buckets (
                    bucket_id,
                    physical_shard_id
                 ) VALUES (?1, ?2)",
            )
            .map_err(sqlite_error::storage)?;
        for bucket_id in 0..VIRTUAL_BUCKET_COUNT {
            insert_bucket
                .execute([
                    i64::from(bucket_id),
                    i64::from(initial_physical_shard(bucket_id, shard_count)),
                ])
                .map_err(sqlite_error::storage)?;
        }
    }

    Ok(())
}

fn migrate_v3_to_v4(transaction: &Transaction<'_>, _shard_count: u16) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V4_DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_metadata (requires_manifest_version) VALUES (?1)",
            [V4_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;

    transaction
        .execute_batch(V4_LOGICAL_DATABASES_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_logical_databases (database_id, database_name)
             VALUES (?1, ?2)",
            rusqlite::params![
                i64::try_from(DEFAULT_LOGICAL_DATABASE_ID)
                    .expect("default logical database ID fits in SQLite"),
                DEFAULT_LOGICAL_DATABASE_NAME
            ],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V4_SCHEMA_CATALOG_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_schema_catalog (
                singleton,
                identifier_encoding_version,
                schema_generation,
                default_database_id
             ) VALUES (1, ?1, ?2, ?3)",
            rusqlite::params![
                i64::from(IDENTIFIER_ENCODING_VERSION),
                i64::try_from(INITIAL_SCHEMA_GENERATION)
                    .expect("initial schema generation fits in SQLite"),
                i64::try_from(DEFAULT_LOGICAL_DATABASE_ID)
                    .expect("default logical database ID fits in SQLite"),
            ],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V4_TABLES_TABLE_SQL)
        .map_err(sqlite_error::storage)?;

    Ok(())
}

fn migrate_v4_to_v5(transaction: &Transaction<'_>, _shard_count: u16) -> EngineResult<()> {
    add_v5_schema(transaction, ShardLayoutState::Adopting)
}

fn migrate_v5_to_v6(transaction: &Transaction<'_>, _shard_count: u16) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V6_DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_metadata (requires_manifest_version) VALUES (?1)",
            [V6_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;

    transaction
        .execute_batch(
            "ALTER TABLE briskdb_schema_catalog
                 RENAME TO briskdb_schema_catalog_v5;",
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V6_SCHEMA_CATALOG_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_schema_catalog (
                singleton,
                identifier_encoding_version,
                schema_generation,
                default_database_id
             )
             SELECT singleton,
                    identifier_encoding_version,
                    schema_generation,
                    default_database_id
             FROM briskdb_schema_catalog_v5",
            [],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch("DROP TABLE briskdb_schema_catalog_v5;")
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V6_SCHEMA_MIGRATIONS_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn migrate_v6_to_v7(transaction: &Transaction<'_>, _shard_count: u16) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V7_DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_metadata (requires_manifest_version) VALUES (?1)",
            [V7_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V7_INTEGRITY_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_integrity (
                singleton,
                manifest_digest_version,
                manifest_digest,
                schema_digest_version,
                database_state,
                committed_schema_digest,
                target_schema_digest
             ) VALUES (1, ?1, zeroblob(32), ?2, ?3, NULL, NULL)",
            rusqlite::params![
                V1_MANIFEST_DIGEST_VERSION,
                SCHEMA_DIGEST_VERSION,
                DATABASE_STATE_VERIFYING,
            ],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn migrate_v7_to_v8(transaction: &Transaction<'_>, _shard_count: u16) -> EngineResult<()> {
    // Version 7 table rows were explicitly advisory and could be installed
    // without proving any relationship to the physical shard schema. They
    // cannot be silently promoted into authoritative routing declarations.
    transaction
        .execute("DELETE FROM briskdb_tables", [])
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V8_DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_metadata (requires_manifest_version) VALUES (?1)",
            [V8_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn migrate_v8_to_v9(transaction: &Transaction<'_>, _shard_count: u16) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V9_DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_metadata (requires_manifest_version) VALUES (?1)",
            [V9_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;

    transaction
        .execute_batch(V9_GENERATED_IDS_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_generated_ids (
                table_id,
                policy,
                generated_column,
                encoding_version
             )
             SELECT table_id, ?1, NULL, NULL
             FROM briskdb_tables
             ORDER BY table_id",
            [GENERATED_ID_POLICY_NONE],
        )
        .map_err(sqlite_error::storage)?;

    transaction
        .execute_batch(V9_ALLOCATION_OWNERS_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_allocation_owners (owner_slot, physical_shard_id)
             SELECT shard_id, shard_id
             FROM briskdb_physical_shards
             ORDER BY shard_id",
            [],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "UPDATE briskdb_integrity
             SET manifest_digest_version = ?1
             WHERE singleton = 1",
            [V2_MANIFEST_DIGEST_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn migrate_v9_to_v10(transaction: &Transaction<'_>, _shard_count: u16) -> EngineResult<()> {
    transaction
        .execute_batch(
            "ALTER TABLE briskdb_allocation_owners RENAME TO briskdb_allocation_owners_v9;",
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V10_ALLOCATION_OWNERS_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_allocation_owners (
                owner_slot, physical_shard_id, owner_state
             )
             SELECT owner_slot, physical_shard_id, ?1
             FROM briskdb_allocation_owners_v9
             ORDER BY owner_slot",
            [ALLOCATION_OWNER_ACTIVE],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch("DROP TABLE briskdb_allocation_owners_v9;")
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V10_ACTIVE_OWNER_INDEX_SQL)
        .map_err(sqlite_error::storage)?;

    transaction
        .execute_batch("ALTER TABLE briskdb_generated_ids RENAME TO briskdb_generated_ids_v9;")
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V10_GENERATED_IDS_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_generated_ids (
                table_id,
                policy,
                generated_column,
                encoding_version,
                activation_state
             )
             SELECT table_id,
                    policy,
                    generated_column,
                    encoding_version,
                    ?1
             FROM briskdb_generated_ids_v9
             ORDER BY table_id",
            [GENERATED_ID_INACTIVE],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch("DROP TABLE briskdb_generated_ids_v9;")
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V10_TABLE_PROVISIONING_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V10_TABLE_PROVISIONING_DECLARATIONS_SQL)
        .map_err(sqlite_error::storage)?;

    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V10_DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_metadata (requires_manifest_version) VALUES (?1)",
            [V10_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "UPDATE briskdb_integrity
             SET manifest_digest_version = ?1
             WHERE singleton = 1",
            [V3_MANIFEST_DIGEST_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn migrate_v10_to_v11(transaction: &Transaction<'_>, _shard_count: u16) -> EngineResult<()> {
    transaction
        .execute_batch(V11_HILO_LEASES_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V11_DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_metadata (requires_manifest_version) VALUES (?1)",
            [V11_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "UPDATE briskdb_integrity
             SET manifest_digest_version = ?1
             WHERE singleton = 1",
            [V4_MANIFEST_DIGEST_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn migrate_v11_to_v12(transaction: &Transaction<'_>, _shard_count: u16) -> EngineResult<()> {
    transaction
        .execute_batch(V12_GENERATED_TABLE_DDL_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V12_DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_metadata (requires_manifest_version) VALUES (?1)",
            [V12_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "UPDATE briskdb_integrity
             SET manifest_digest_version = ?1
             WHERE singleton = 1",
            [V5_MANIFEST_DIGEST_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn add_v5_schema(transaction: &Transaction<'_>, state: ShardLayoutState) -> EngineResult<()> {
    transaction
        .execute_batch("DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V5_DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_metadata (requires_manifest_version) VALUES (?1)",
            [V5_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(V5_SHARD_LAYOUT_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_shard_layout (
                singleton,
                layout_id,
                shard_application_id,
                shard_metadata_version,
                layout_state
             ) VALUES (1, randomblob(16), ?1, ?2, ?3)",
            rusqlite::params![SHARD_APPLICATION_ID, SHARD_METADATA_VERSION, state.code(),],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn set_identity(connection: &Connection, version: u32) -> EngineResult<()> {
    connection
        .pragma_update(None, "application_id", MANIFEST_APPLICATION_ID)
        .map_err(sqlite_error::storage)?;
    connection
        .pragma_update(None, "user_version", i64::from(version))
        .map_err(sqlite_error::storage)?;

    let (application_id, stored_version) = read_identity(connection)?;
    if application_id != MANIFEST_APPLICATION_ID || stored_version != i64::from(version) {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "SQLite did not persist the requested manifest identity",
        ));
    }
    Ok(())
}

fn inspect_with_plan(
    connection: &Connection,
    requested_shards: u16,
    plan: MigrationPlan<'_>,
) -> EngineResult<ManifestState> {
    let (application_id, version) = read_identity(connection)?;

    if application_id == MANIFEST_APPLICATION_ID {
        if version > i64::from(plan.current_version) {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "manifest schema version {version} is newer than this BriskDB build supports ({})",
                    plan.current_version
                ),
            ));
        }
        if version < 0 {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("BriskDB manifest has invalid negative schema version {version}"),
            ));
        }
        let objects = schema_objects(connection)?;
        let version = u32::try_from(version).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::DataCorruption,
                "BriskDB manifest schema version is outside the supported numeric range",
                error,
            )
        })?;
        let validator = plan
            .migrations
            .iter()
            .find(|migration| migration.to == version)
            .map(|migration| migration.validate)
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!(
                        "BriskDB manifest application identifier has unsupported schema version {version}"
                    ),
                )
            })?;
        return validator(connection, requested_shards, &objects).map(|snapshot| {
            ManifestState::Versioned {
                version,
                snapshot: Box::new(snapshot),
            }
        });
    }

    if application_id != 0 {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("manifest.sqlite has foreign application identifier {application_id:#010x}"),
        ));
    }

    let objects = schema_objects(connection)?;

    if version == 0 && objects.is_empty() {
        return Ok(ManifestState::Empty);
    }

    if (version == 0 || version == i64::from(LEGACY_SCHEMA_VERSION)) && objects == legacy_objects()
    {
        validate_table(
            connection,
            "briskdb_metadata",
            &[
                TableColumn::expected(0, "key", "TEXT", false, 1),
                TableColumn::expected(1, "value", "TEXT", true, 0),
            ],
            false,
        )?;
        validate_table_sql(connection, "briskdb_metadata", LEGACY_METADATA_TABLE_SQL)?;
        return match read_legacy_metadata(connection)? {
            LegacyMetadata::Empty if version == 0 => Ok(ManifestState::LegacyUninitialized),
            LegacyMetadata::Empty => Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "versioned legacy manifest is missing its metadata",
            )),
            LegacyMetadata::Initialized { shard_count } => {
                ensure_requested_shards(shard_count, requested_shards)?;
                Ok(ManifestState::LegacyV1 { shard_count })
            }
        };
    }

    if objects
        .iter()
        .any(|object| object.name.starts_with("briskdb_"))
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest identity and BriskDB schema objects are inconsistent",
        ));
    }

    Err(EngineError::new(
        EngineErrorKind::FailedPrecondition,
        "manifest.sqlite is not an empty or recognized BriskDB manifest",
    ))
}

fn read_identity(connection: &Connection) -> EngineResult<(i64, i64)> {
    let application_id = connection
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .map_err(|error| manifest_read_error(error, "failed to read manifest application ID"))?;
    let version = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|error| manifest_read_error(error, "failed to read manifest schema version"))?;
    Ok((application_id, version))
}

#[derive(Debug, PartialEq, Eq)]
struct SchemaObject {
    object_type: String,
    name: String,
}

fn legacy_objects() -> Vec<SchemaObject> {
    vec![SchemaObject {
        object_type: "table".to_owned(),
        name: "briskdb_metadata".to_owned(),
    }]
}

fn v2_objects() -> Vec<SchemaObject> {
    vec![
        SchemaObject {
            object_type: "table".to_owned(),
            name: "briskdb_manifest".to_owned(),
        },
        SchemaObject {
            object_type: "table".to_owned(),
            name: "briskdb_metadata".to_owned(),
        },
    ]
}

fn v3_objects() -> Vec<SchemaObject> {
    vec![
        SchemaObject {
            object_type: "table".to_owned(),
            name: "briskdb_manifest".to_owned(),
        },
        SchemaObject {
            object_type: "table".to_owned(),
            name: "briskdb_metadata".to_owned(),
        },
        SchemaObject {
            object_type: "table".to_owned(),
            name: "briskdb_physical_shards".to_owned(),
        },
        SchemaObject {
            object_type: "table".to_owned(),
            name: "briskdb_routing".to_owned(),
        },
        SchemaObject {
            object_type: "table".to_owned(),
            name: "briskdb_virtual_buckets".to_owned(),
        },
    ]
}

fn v4_objects() -> Vec<SchemaObject> {
    vec![
        SchemaObject {
            object_type: "table".to_owned(),
            name: "briskdb_logical_databases".to_owned(),
        },
        SchemaObject {
            object_type: "table".to_owned(),
            name: "briskdb_manifest".to_owned(),
        },
        SchemaObject {
            object_type: "table".to_owned(),
            name: "briskdb_metadata".to_owned(),
        },
        SchemaObject {
            object_type: "table".to_owned(),
            name: "briskdb_physical_shards".to_owned(),
        },
        SchemaObject {
            object_type: "table".to_owned(),
            name: "briskdb_routing".to_owned(),
        },
        SchemaObject {
            object_type: "table".to_owned(),
            name: "briskdb_schema_catalog".to_owned(),
        },
        SchemaObject {
            object_type: "table".to_owned(),
            name: "briskdb_tables".to_owned(),
        },
        SchemaObject {
            object_type: "table".to_owned(),
            name: "briskdb_virtual_buckets".to_owned(),
        },
    ]
}

fn v5_objects() -> Vec<SchemaObject> {
    let mut objects = v4_objects();
    objects.push(SchemaObject {
        object_type: "table".to_owned(),
        name: "briskdb_shard_layout".to_owned(),
    });
    objects.sort_by(|left, right| {
        (&left.object_type, &left.name).cmp(&(&right.object_type, &right.name))
    });
    objects
}

fn v6_objects() -> Vec<SchemaObject> {
    let mut objects = v5_objects();
    objects.push(SchemaObject {
        object_type: "table".to_owned(),
        name: "briskdb_schema_migrations".to_owned(),
    });
    objects.sort_by(|left, right| {
        (&left.object_type, &left.name).cmp(&(&right.object_type, &right.name))
    });
    objects
}

fn v7_objects() -> Vec<SchemaObject> {
    let mut objects = v6_objects();
    objects.push(SchemaObject {
        object_type: "table".to_owned(),
        name: "briskdb_integrity".to_owned(),
    });
    objects.sort_by(|left, right| {
        (&left.object_type, &left.name).cmp(&(&right.object_type, &right.name))
    });
    objects
}

fn v9_objects() -> Vec<SchemaObject> {
    let mut objects = v7_objects();
    for name in ["briskdb_allocation_owners", "briskdb_generated_ids"] {
        objects.push(SchemaObject {
            object_type: "table".to_owned(),
            name: name.to_owned(),
        });
    }
    objects.sort_by(|left, right| {
        (&left.object_type, &left.name).cmp(&(&right.object_type, &right.name))
    });
    objects
}

fn v10_objects() -> Vec<SchemaObject> {
    let mut objects = v9_objects();
    objects.push(SchemaObject {
        object_type: "index".to_owned(),
        name: "briskdb_one_active_owner_per_shard".to_owned(),
    });
    for name in [
        "briskdb_table_provisioning",
        "briskdb_table_provisioning_declarations",
    ] {
        objects.push(SchemaObject {
            object_type: "table".to_owned(),
            name: name.to_owned(),
        });
    }
    objects.sort_by(|left, right| {
        (&left.object_type, &left.name).cmp(&(&right.object_type, &right.name))
    });
    objects
}

fn v11_objects() -> Vec<SchemaObject> {
    let mut objects = v10_objects();
    objects.push(SchemaObject {
        object_type: "table".to_owned(),
        name: "briskdb_hilo_leases".to_owned(),
    });
    objects.sort_by(|left, right| {
        (&left.object_type, &left.name).cmp(&(&right.object_type, &right.name))
    });
    objects
}

fn v12_objects() -> Vec<SchemaObject> {
    let mut objects = v11_objects();
    objects.push(SchemaObject {
        object_type: "table".to_owned(),
        name: "briskdb_generated_table_ddl".to_owned(),
    });
    objects.sort_by(|left, right| {
        (&left.object_type, &left.name).cmp(&(&right.object_type, &right.name))
    });
    objects
}

fn schema_objects(connection: &Connection) -> EngineResult<Vec<SchemaObject>> {
    let mut statement = connection
        .prepare(
            "SELECT type, name
             FROM sqlite_schema
             WHERE name NOT LIKE 'sqlite_%'
             ORDER BY type, name
             LIMIT 65",
        )
        .map_err(|error| manifest_read_error(error, "failed to inspect manifest schema"))?;
    let rows = statement
        .query_map([], |row| {
            Ok(SchemaObject {
                object_type: row.get(0)?,
                name: row.get(1)?,
            })
        })
        .map_err(|error| manifest_read_error(error, "failed to inspect manifest schema"))?;
    let mut objects = Vec::new();
    for row in rows {
        objects.push(
            row.map_err(|error| manifest_read_error(error, "failed to inspect manifest schema"))?,
        );
    }
    Ok(objects)
}

#[derive(Debug, PartialEq, Eq)]
struct TableColumn {
    id: i64,
    name: String,
    declared_type: String,
    not_null: bool,
    default_value: Option<String>,
    primary_key_position: i64,
    hidden: i64,
}

impl TableColumn {
    fn expected(
        id: i64,
        name: &str,
        declared_type: &str,
        not_null: bool,
        primary_key_position: i64,
    ) -> Self {
        Self {
            id,
            name: name.to_owned(),
            declared_type: declared_type.to_owned(),
            not_null,
            default_value: None,
            primary_key_position,
            hidden: 0,
        }
    }
}

fn validate_table(
    connection: &Connection,
    table: &str,
    expected_columns: &[TableColumn],
    expected_strict: bool,
) -> EngineResult<()> {
    let pragma = match table {
        "briskdb_manifest" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_manifest') LIMIT ?1"
        }
        "briskdb_metadata" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_metadata') LIMIT ?1"
        }
        "briskdb_physical_shards" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_physical_shards') LIMIT ?1"
        }
        "briskdb_routing" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_routing') LIMIT ?1"
        }
        "briskdb_virtual_buckets" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_virtual_buckets') LIMIT ?1"
        }
        "briskdb_logical_databases" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_logical_databases') LIMIT ?1"
        }
        "briskdb_schema_catalog" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_schema_catalog') LIMIT ?1"
        }
        "briskdb_tables" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_tables') LIMIT ?1"
        }
        "briskdb_shard_layout" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_shard_layout') LIMIT ?1"
        }
        "briskdb_schema_migrations" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_schema_migrations') LIMIT ?1"
        }
        "briskdb_integrity" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_integrity') LIMIT ?1"
        }
        "briskdb_generated_ids" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_generated_ids') LIMIT ?1"
        }
        "briskdb_allocation_owners" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_allocation_owners') LIMIT ?1"
        }
        "briskdb_table_provisioning" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_table_provisioning') LIMIT ?1"
        }
        "briskdb_table_provisioning_declarations" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_table_provisioning_declarations') LIMIT ?1"
        }
        "briskdb_hilo_leases" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_hilo_leases') LIMIT ?1"
        }
        "briskdb_generated_table_ddl" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_generated_table_ddl') LIMIT ?1"
        }
        _ => {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "attempted to validate an unknown manifest table",
            ));
        }
    };
    let mut statement = connection
        .prepare(pragma)
        .map_err(|error| manifest_read_error(error, "failed to inspect manifest table"))?;
    let inspection_limit = i64::try_from(expected_columns.len() + 1).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::Internal,
            "manifest table definition is too large to validate",
            error,
        )
    })?;
    let rows = statement
        .query_map([inspection_limit], |row| {
            Ok(TableColumn {
                id: row.get(0)?,
                name: row.get(1)?,
                declared_type: row.get(2)?,
                not_null: row.get::<_, i64>(3)? != 0,
                default_value: row.get(4)?,
                primary_key_position: row.get(5)?,
                hidden: row.get(6)?,
            })
        })
        .map_err(|error| manifest_read_error(error, "failed to inspect manifest table"))?;
    let mut actual_columns = Vec::new();
    for row in rows {
        actual_columns.push(
            row.map_err(|error| manifest_read_error(error, "failed to inspect manifest table"))?,
        );
    }
    if actual_columns != expected_columns {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("manifest table {table} has an incompatible definition"),
        ));
    }

    let strict: Option<i64> = connection
        .query_row(
            "SELECT strict FROM pragma_table_list WHERE schema = 'main' AND name = ?1",
            [table],
            |row| row.get(0),
        )
        .map(Some)
        .or_else(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            error => Err(error),
        })
        .map_err(|error| manifest_read_error(error, "failed to inspect manifest table flags"))?;
    if strict != Some(i64::from(expected_strict)) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("manifest table {table} has incompatible table flags"),
        ));
    }
    Ok(())
}

fn validate_table_sql(
    connection: &Connection,
    table: &str,
    expected_sql: &str,
) -> EngineResult<()> {
    let (length, actual): (i64, String) = connection
        .query_row(
            "SELECT length(sql), substr(sql, 1, 4097)
             FROM sqlite_schema
             WHERE type = 'table' AND name = ?1",
            [table],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| manifest_read_error(error, "failed to read manifest table SQL"))?;
    if !(0..=MAX_TABLE_SQL_BYTES).contains(&length)
        || normalize_schema_sql(&actual) != normalize_schema_sql(expected_sql)
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("manifest table {table} has incompatible schema SQL"),
        ));
    }
    Ok(())
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

enum LegacyMetadata {
    Empty,
    Initialized { shard_count: u16 },
}

fn read_legacy_metadata(connection: &Connection) -> EngineResult<LegacyMetadata> {
    let mut statement = connection
        .prepare("SELECT key, value FROM briskdb_metadata ORDER BY key LIMIT 4")
        .map_err(|error| manifest_read_error(error, "failed to read legacy manifest metadata"))?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| manifest_read_error(error, "failed to read legacy manifest metadata"))?;
    let mut metadata = Vec::new();
    for row in rows {
        metadata.push(row.map_err(|error| {
            manifest_read_error(error, "failed to read legacy manifest metadata")
        })?);
    }
    if metadata.is_empty() {
        return Ok(LegacyMetadata::Empty);
    }
    if metadata.len() != 2 || metadata[0].0 != "schema_version" || metadata[1].0 != "shard_count" {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "legacy manifest has incomplete or unexpected metadata",
        ));
    }
    if metadata[0].1 != LEGACY_SCHEMA_VERSION.to_string() {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "legacy manifest has an invalid schema-version value",
        ));
    }
    let shard_count = metadata[1].1.parse::<u16>().map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "manifest has an invalid shard count",
            error,
        )
    })?;
    if shard_count.to_string() != metadata[1].1 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "legacy manifest shard count is not canonically encoded",
        ));
    }
    validate_shard_range(shard_count)?;
    Ok(LegacyMetadata::Initialized { shard_count })
}

fn validate_v2(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
) -> EngineResult<ManifestSnapshot> {
    if objects != v2_objects() {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest schema version 2 has unexpected database objects",
        ));
    }
    validate_table(
        connection,
        "briskdb_manifest",
        &[
            TableColumn::expected(0, "singleton", "INTEGER", false, 1),
            TableColumn::expected(1, "shard_count", "INTEGER", true, 0),
        ],
        true,
    )?;
    validate_table_sql(connection, "briskdb_manifest", V2_MANIFEST_TABLE_SQL)?;
    validate_table(
        connection,
        "briskdb_metadata",
        &[TableColumn::expected(
            0,
            "requires_manifest_version",
            "INTEGER",
            true,
            0,
        )],
        true,
    )?;
    validate_table_sql(connection, "briskdb_metadata", V2_DOWNGRADE_FENCE_SQL)?;

    let shard_count = validate_manifest_configuration(connection, requested_shards)?;
    validate_downgrade_fence(connection, V2_SCHEMA_VERSION)?;
    Ok(ManifestSnapshot {
        shard_count,
        routing_catalog: None,
        logical_catalog: None,
        shard_layout: None,
        active_migration: None,
        integrity: None,
        allocation_owners: None,
        active_native_id_table_ids: Box::new([]),
        active_hilo_id_table_ids: Box::new([]),
        active_table_provisioning: None,
        generated_table_ddl: None,
    })
}

fn validate_v3(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
) -> EngineResult<ManifestSnapshot> {
    if objects != v3_objects() {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest schema version 3 has unexpected database objects",
        ));
    }

    let (shard_count, routing_catalog) = validate_routing_manifest(
        connection,
        requested_shards,
        V3_SCHEMA_VERSION,
        V3_DOWNGRADE_FENCE_SQL,
    )?;
    validate_foreign_keys(connection)?;
    Ok(ManifestSnapshot {
        shard_count,
        routing_catalog: Some(routing_catalog),
        logical_catalog: None,
        shard_layout: None,
        active_migration: None,
        integrity: None,
        allocation_owners: None,
        active_native_id_table_ids: Box::new([]),
        active_hilo_id_table_ids: Box::new([]),
        active_table_provisioning: None,
        generated_table_ddl: None,
    })
}

fn validate_routing_manifest(
    connection: &Connection,
    requested_shards: u16,
    expected_version: u32,
    downgrade_fence_sql: &str,
) -> EngineResult<(u16, RoutingCatalog)> {
    validate_table(
        connection,
        "briskdb_manifest",
        &[
            TableColumn::expected(0, "singleton", "INTEGER", false, 1),
            TableColumn::expected(1, "shard_count", "INTEGER", true, 0),
        ],
        true,
    )?;
    validate_table_sql(connection, "briskdb_manifest", V2_MANIFEST_TABLE_SQL)?;
    validate_table(
        connection,
        "briskdb_metadata",
        &[TableColumn::expected(
            0,
            "requires_manifest_version",
            "INTEGER",
            true,
            0,
        )],
        true,
    )?;
    validate_table_sql(connection, "briskdb_metadata", downgrade_fence_sql)?;
    validate_table(
        connection,
        "briskdb_routing",
        &[
            TableColumn::expected(0, "singleton", "INTEGER", false, 1),
            TableColumn::expected(1, "hash_version", "INTEGER", true, 0),
            TableColumn::expected(2, "key_encoding_version", "INTEGER", true, 0),
            TableColumn::expected(3, "bucket_algorithm_version", "INTEGER", true, 0),
            TableColumn::expected(4, "virtual_bucket_count", "INTEGER", true, 0),
            TableColumn::expected(5, "map_generation", "INTEGER", true, 0),
        ],
        true,
    )?;
    validate_table_sql(connection, "briskdb_routing", V3_ROUTING_TABLE_SQL)?;
    validate_table(
        connection,
        "briskdb_physical_shards",
        &[
            TableColumn::expected(0, "shard_id", "INTEGER", false, 1),
            TableColumn::expected(1, "lifecycle_state", "TEXT", true, 0),
        ],
        true,
    )?;
    validate_table_sql(
        connection,
        "briskdb_physical_shards",
        V3_PHYSICAL_SHARDS_TABLE_SQL,
    )?;
    validate_table(
        connection,
        "briskdb_virtual_buckets",
        &[
            TableColumn::expected(0, "bucket_id", "INTEGER", false, 1),
            TableColumn::expected(1, "physical_shard_id", "INTEGER", true, 0),
        ],
        true,
    )?;
    validate_table_sql(
        connection,
        "briskdb_virtual_buckets",
        V3_VIRTUAL_BUCKETS_TABLE_SQL,
    )?;

    let shard_count = validate_manifest_configuration(connection, requested_shards)?;
    validate_downgrade_fence(connection, expected_version)?;
    let routing = validate_routing_configuration(connection)?;
    validate_physical_shards(connection, shard_count)?;
    let buckets = validate_virtual_buckets(connection, shard_count)?;
    Ok((
        shard_count,
        RoutingCatalog::from_validated_parts(
            shard_count,
            routing.hash_version,
            routing.key_encoding_version,
            routing.bucket_algorithm_version,
            routing.map_generation,
            buckets,
        ),
    ))
}

fn validate_v4(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
) -> EngineResult<ManifestSnapshot> {
    validate_catalog_manifest(
        connection,
        requested_shards,
        objects,
        CatalogManifestDefinition {
            version: V4_SCHEMA_VERSION,
            downgrade_fence_sql: V4_DOWNGRADE_FENCE_SQL,
            expected_objects: &v4_objects(),
            schema_catalog_sql: V4_SCHEMA_CATALOG_TABLE_SQL,
            generation_policy: SchemaGenerationPolicy::InitialOnly,
            generated_ids: false,
        },
    )
}

fn validate_v5(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
) -> EngineResult<ManifestSnapshot> {
    let mut snapshot = validate_catalog_manifest(
        connection,
        requested_shards,
        objects,
        CatalogManifestDefinition {
            version: V5_SCHEMA_VERSION,
            downgrade_fence_sql: V5_DOWNGRADE_FENCE_SQL,
            expected_objects: &v5_objects(),
            schema_catalog_sql: V4_SCHEMA_CATALOG_TABLE_SQL,
            generation_policy: SchemaGenerationPolicy::InitialOnly,
            generated_ids: false,
        },
    )?;
    validate_table(
        connection,
        "briskdb_shard_layout",
        &[
            TableColumn::expected(0, "singleton", "INTEGER", false, 1),
            TableColumn::expected(1, "layout_id", "BLOB", true, 0),
            TableColumn::expected(2, "shard_application_id", "INTEGER", true, 0),
            TableColumn::expected(3, "shard_metadata_version", "INTEGER", true, 0),
            TableColumn::expected(4, "layout_state", "INTEGER", true, 0),
        ],
        true,
    )?;
    validate_table_sql(
        connection,
        "briskdb_shard_layout",
        V5_SHARD_LAYOUT_TABLE_SQL,
    )?;
    snapshot.shard_layout = Some(validate_shard_layout(connection)?);
    Ok(snapshot)
}

fn validate_v6(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
) -> EngineResult<ManifestSnapshot> {
    let mut snapshot = validate_catalog_manifest(
        connection,
        requested_shards,
        objects,
        CatalogManifestDefinition {
            version: V6_SCHEMA_VERSION,
            downgrade_fence_sql: V6_DOWNGRADE_FENCE_SQL,
            expected_objects: &v6_objects(),
            schema_catalog_sql: V6_SCHEMA_CATALOG_TABLE_SQL,
            generation_policy: SchemaGenerationPolicy::Journaled,
            generated_ids: false,
        },
    )?;
    validate_table(
        connection,
        "briskdb_shard_layout",
        &[
            TableColumn::expected(0, "singleton", "INTEGER", false, 1),
            TableColumn::expected(1, "layout_id", "BLOB", true, 0),
            TableColumn::expected(2, "shard_application_id", "INTEGER", true, 0),
            TableColumn::expected(3, "shard_metadata_version", "INTEGER", true, 0),
            TableColumn::expected(4, "layout_state", "INTEGER", true, 0),
        ],
        true,
    )?;
    validate_table_sql(
        connection,
        "briskdb_shard_layout",
        V5_SHARD_LAYOUT_TABLE_SQL,
    )?;
    let layout = validate_shard_layout(connection)?;

    validate_table(
        connection,
        "briskdb_schema_migrations",
        &[
            TableColumn::expected(0, "target_generation", "INTEGER", false, 1),
            TableColumn::expected(1, "source_generation", "INTEGER", true, 0),
            TableColumn::expected(2, "migration_id", "BLOB", true, 0),
            TableColumn::expected(3, "digest_version", "INTEGER", true, 0),
            TableColumn::expected(4, "sql_text", "TEXT", true, 0),
            TableColumn::expected(5, "shard_count", "INTEGER", true, 0),
            TableColumn::expected(6, "migration_state", "INTEGER", true, 0),
            TableColumn::expected(7, "next_shard", "INTEGER", true, 0),
        ],
        true,
    )?;
    validate_table_sql(
        connection,
        "briskdb_schema_migrations",
        V6_SCHEMA_MIGRATIONS_TABLE_SQL,
    )?;

    let catalog_generation = snapshot
        .logical_catalog
        .as_ref()
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                "current manifest validation omitted its logical catalog",
            )
        })?
        .schema_generation();
    let active =
        validate_schema_migration_history(connection, snapshot.shard_count, catalog_generation)?;
    let has_history = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM briskdb_schema_migrations)",
            [],
            |row| row.get::<_, bool>(0),
        )
        .map_err(|error| {
            manifest_read_error(error, "failed to inspect schema migration journal")
        })?;
    if has_history && layout.state() != ShardLayoutState::Ready {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "schema migration history requires a ready physical shard layout",
        ));
    }
    snapshot.shard_layout = Some(layout);
    snapshot.active_migration = active;
    Ok(snapshot)
}

fn validate_v7(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
) -> EngineResult<ManifestSnapshot> {
    validate_integrity_manifest(
        connection,
        requested_shards,
        objects,
        V7_SCHEMA_VERSION,
        V7_DOWNGRADE_FENCE_SQL,
    )
}

fn validate_v8(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
) -> EngineResult<ManifestSnapshot> {
    validate_integrity_manifest(
        connection,
        requested_shards,
        objects,
        V8_SCHEMA_VERSION,
        V8_DOWNGRADE_FENCE_SQL,
    )
}

fn validate_v9(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
) -> EngineResult<ManifestSnapshot> {
    let mut snapshot = validate_integrity_manifest_with_definition(
        connection,
        requested_shards,
        objects,
        IntegrityManifestDefinition {
            version: V9_SCHEMA_VERSION,
            downgrade_fence_sql: V9_DOWNGRADE_FENCE_SQL,
            expected_objects: &v9_objects(),
            expected_manifest_digest_version: V2_MANIFEST_DIGEST_VERSION,
            generated_ids: true,
        },
    )?;
    snapshot.allocation_owners = Some(validate_allocation_owners(
        connection,
        snapshot.shard_count,
    )?);
    Ok(snapshot)
}

fn validate_v10(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
) -> EngineResult<ManifestSnapshot> {
    let mut snapshot = validate_integrity_manifest_with_definition(
        connection,
        requested_shards,
        objects,
        IntegrityManifestDefinition {
            version: V10_SCHEMA_VERSION,
            downgrade_fence_sql: V10_DOWNGRADE_FENCE_SQL,
            expected_objects: &v10_objects(),
            expected_manifest_digest_version: V3_MANIFEST_DIGEST_VERSION,
            generated_ids: true,
        },
    )?;
    snapshot.allocation_owners = Some(validate_allocation_owners(
        connection,
        snapshot.shard_count,
    )?);
    snapshot.active_native_id_table_ids = validate_active_native_id_tables(connection)?;
    snapshot.active_table_provisioning = validate_table_provisioning(
        connection,
        V10_SCHEMA_VERSION,
        snapshot.shard_count,
        snapshot.logical_catalog.as_ref(),
        snapshot.active_migration.as_ref(),
        snapshot.integrity,
    )?;
    Ok(snapshot)
}

fn validate_v11(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
) -> EngineResult<ManifestSnapshot> {
    let mut snapshot = validate_integrity_manifest_with_definition(
        connection,
        requested_shards,
        objects,
        IntegrityManifestDefinition {
            version: V11_SCHEMA_VERSION,
            downgrade_fence_sql: V11_DOWNGRADE_FENCE_SQL,
            expected_objects: &v11_objects(),
            expected_manifest_digest_version: V4_MANIFEST_DIGEST_VERSION,
            generated_ids: true,
        },
    )?;
    snapshot.allocation_owners = Some(validate_allocation_owners(
        connection,
        snapshot.shard_count,
    )?);
    snapshot.active_native_id_table_ids =
        validate_active_generated_id_tables(connection, GENERATED_ID_POLICY_NATIVE_RANGE_V1)?;
    snapshot.active_hilo_id_table_ids =
        validate_active_generated_id_tables(connection, GENERATED_ID_POLICY_HILO_V1)?;
    validate_hilo_v1_leases(
        connection,
        snapshot.logical_catalog.as_ref(),
        &snapshot.active_hilo_id_table_ids,
    )?;
    snapshot.active_table_provisioning = validate_table_provisioning(
        connection,
        V11_SCHEMA_VERSION,
        snapshot.shard_count,
        snapshot.logical_catalog.as_ref(),
        snapshot.active_migration.as_ref(),
        snapshot.integrity,
    )?;
    Ok(snapshot)
}

fn validate_v12(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
) -> EngineResult<ManifestSnapshot> {
    let mut snapshot = validate_integrity_manifest_with_definition(
        connection,
        requested_shards,
        objects,
        IntegrityManifestDefinition {
            version: V12_SCHEMA_VERSION,
            downgrade_fence_sql: V12_DOWNGRADE_FENCE_SQL,
            expected_objects: &v12_objects(),
            expected_manifest_digest_version: V5_MANIFEST_DIGEST_VERSION,
            generated_ids: true,
        },
    )?;
    snapshot.allocation_owners = Some(validate_allocation_owners(
        connection,
        snapshot.shard_count,
    )?);
    snapshot.active_native_id_table_ids =
        validate_active_generated_id_tables(connection, GENERATED_ID_POLICY_NATIVE_RANGE_V1)?;
    snapshot.active_hilo_id_table_ids =
        validate_active_generated_id_tables(connection, GENERATED_ID_POLICY_HILO_V1)?;
    validate_hilo_v1_leases(
        connection,
        snapshot.logical_catalog.as_ref(),
        &snapshot.active_hilo_id_table_ids,
    )?;
    snapshot.active_table_provisioning = validate_table_provisioning(
        connection,
        V12_SCHEMA_VERSION,
        snapshot.shard_count,
        snapshot.logical_catalog.as_ref(),
        snapshot.active_migration.as_ref(),
        snapshot.integrity,
    )?;
    validate_table(
        connection,
        "briskdb_generated_table_ddl",
        &[
            TableColumn::expected(0, "singleton", "INTEGER", false, 1),
            TableColumn::expected(1, "logical_id", "BLOB", true, 0),
            TableColumn::expected(2, "logical_digest_version", "INTEGER", true, 0),
            TableColumn::expected(3, "source_dialect", "INTEGER", true, 0),
            TableColumn::expected(4, "translation_version", "INTEGER", true, 0),
            TableColumn::expected(5, "source_sql", "TEXT", true, 0),
            TableColumn::expected(6, "physical_migration_id", "BLOB", true, 0),
            TableColumn::expected(7, "physical_sql", "TEXT", true, 0),
            TableColumn::expected(8, "database_id", "INTEGER", true, 0),
            TableColumn::expected(9, "table_name", "TEXT", true, 0),
            TableColumn::expected(10, "generated_column", "TEXT", true, 0),
            TableColumn::expected(11, "generated_policy", "INTEGER", true, 0),
            TableColumn::expected(12, "generated_encoding_version", "INTEGER", true, 0),
            TableColumn::expected(13, "lifecycle_state", "INTEGER", true, 0),
            TableColumn::expected(14, "provisioning_id", "BLOB", false, 0),
            TableColumn::expected(15, "provisioning_schema_digest", "BLOB", false, 0),
            TableColumn::expected(16, "table_id", "INTEGER", false, 0),
        ],
        true,
    )?;
    validate_table_sql(
        connection,
        "briskdb_generated_table_ddl",
        V12_GENERATED_TABLE_DDL_TABLE_SQL,
    )?;
    snapshot.generated_table_ddl = validate_generated_table_ddl(connection, &snapshot)?;
    Ok(snapshot)
}

#[allow(clippy::type_complexity)]
fn validate_generated_table_ddl(
    connection: &Connection,
    snapshot: &ManifestSnapshot,
) -> EngineResult<Option<GeneratedTableDdl>> {
    let rows = connection
        .prepare(
            "SELECT singleton,
                    logical_id,
                    logical_digest_version,
                    source_dialect,
                    translation_version,
                    source_sql,
                    physical_migration_id,
                    physical_sql,
                    database_id,
                    table_name,
                    generated_column,
                    generated_policy,
                    generated_encoding_version,
                    lifecycle_state,
                    provisioning_id,
                    provisioning_schema_digest,
                    table_id
             FROM briskdb_generated_table_ddl
             ORDER BY singleton
             LIMIT 3",
        )
        .and_then(|mut statement| {
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, Vec<u8>>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, String>(10)?,
                        row.get::<_, i64>(11)?,
                        row.get::<_, i64>(12)?,
                        row.get::<_, i64>(13)?,
                        row.get::<_, Option<Vec<u8>>>(14)?,
                        row.get::<_, Option<Vec<u8>>>(15)?,
                        row.get::<_, Option<i64>>(16)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| manifest_read_error(error, "failed to read generated-table DDL bridge"))?;
    if rows.is_empty() {
        return Ok(None);
    }
    if rows.len() != 1 || rows[0].0 != 1 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "generated-table DDL bridge must contain at most its singleton row",
        ));
    }
    let (
        _,
        logical_id,
        logical_digest_version,
        source_dialect,
        translation_version,
        source_sql,
        physical_migration_id,
        physical_sql,
        database_id,
        table_name,
        generated_column,
        generated_policy,
        generated_encoding_version,
        lifecycle_state,
        provisioning_id,
        provisioning_schema_digest,
        table_id,
    ) = rows.into_iter().next().expect("one bridge row exists");
    if logical_digest_version != i64::from(GENERATED_TABLE_DDL_DIGEST_VERSION)
        || translation_version != i64::from(GENERATED_TABLE_DDL_TRANSLATION_VERSION)
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "generated-table DDL bridge uses a newer identity or translation version",
        ));
    }
    if generated_policy != GENERATED_ID_POLICY_NATIVE_RANGE_V1
        || generated_encoding_version != i64::from(NATIVE_RANGE_V1_ENCODING_VERSION)
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "generated-table DDL bridge uses an unsupported generated-ID policy encoding",
        ));
    }
    let source_dialect = decode_generated_table_ddl_dialect(source_dialect)?;
    let logical_id = digest_from_blob(&logical_id, "generated-table logical identity")?;
    let expected_logical_id = generated_table_ddl_logical_id(source_dialect, &source_sql)?;
    if logical_id != expected_logical_id {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "generated-table DDL logical identity does not match its exact source",
        ));
    }
    let physical_migration_id = digest_from_blob(
        &physical_migration_id,
        "generated-table physical migration identity",
    )?;
    if schema_migration_id(&physical_sql)? != physical_migration_id {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "generated-table physical identity does not match its canonical SQL",
        ));
    }
    let stored_migration =
        find_schema_migration(connection, snapshot.shard_count, &physical_migration_id)?
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::DataCorruption,
                    "generated-table DDL bridge references a missing physical migration",
                )
            })?;
    if stored_migration.sql_text() != physical_sql {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "generated-table DDL bridge conflicts with its physical migration",
        ));
    }
    let database_id = u64::try_from(database_id).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "generated-table DDL database ID is outside the supported range",
            error,
        )
    })?;
    if !validate_catalog_identifier(&table_name) || !validate_catalog_identifier(&generated_column)
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "generated-table DDL bridge contains an invalid catalog identifier",
        ));
    }
    let declaration = generated_table_ddl_declaration(
        LogicalDatabaseId::from_validated(database_id),
        table_name,
        generated_column,
    )?;
    let catalog = snapshot.logical_catalog.as_ref().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "generated-table DDL validation omitted its logical catalog",
        )
    })?;
    if catalog.database_by_id(declaration.database_id()).is_none() {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "generated-table DDL bridge references an unknown logical database",
        ));
    }
    let lifecycle = GeneratedTableDdlLifecycle::from_code(lifecycle_state)?;
    let provisioning_id = provisioning_id
        .as_deref()
        .map(|id| digest_from_blob(id, "generated-table provisioning identity"))
        .transpose()?;
    let provisioning_schema_digest = provisioning_schema_digest
        .as_deref()
        .map(|digest| digest_from_blob(digest, "generated-table provisioning schema checksum"))
        .transpose()?;
    let table_id = table_id
        .map(|id| {
            u64::try_from(id)
                .map(TableId::from_validated)
                .map_err(|error| {
                    EngineError::from_source(
                        EngineErrorKind::DataCorruption,
                        "generated-table DDL table ID is outside the supported range",
                        error,
                    )
                })
        })
        .transpose()?;
    let expected_provisioning_id = provisioning_schema_digest.map(|digest| {
        table_provisioning_id(
            std::slice::from_ref(&declaration),
            snapshot.shard_count,
            digest,
        )
    });
    match lifecycle {
        GeneratedTableDdlLifecycle::ApplyingPhysical => {
            if provisioning_id.is_some()
                || provisioning_schema_digest.is_some()
                || table_id.is_some()
            {
                return Err(invalid_generated_table_ddl_lifecycle());
            }
        }
        GeneratedTableDdlLifecycle::Provisioning => {
            if !stored_migration.is_complete()
                || provisioning_id.is_none()
                || provisioning_schema_digest.is_none()
                || provisioning_id != expected_provisioning_id
                || snapshot
                    .integrity
                    .and_then(ManifestIntegrity::committed_schema_digest)
                    != provisioning_schema_digest
                || table_id.is_some()
            {
                return Err(invalid_generated_table_ddl_lifecycle());
            }
            if let Some(active) = snapshot.active_table_provisioning.as_ref() {
                if active.provisioning_id() != provisioning_id.expect("checked above")
                    || Some(active.committed_schema_digest()) != provisioning_schema_digest
                    || active.declarations() != std::slice::from_ref(&declaration)
                {
                    return Err(EngineError::new(
                        EngineErrorKind::DataCorruption,
                        "generated-table DDL bridge conflicts with active table provisioning",
                    ));
                }
            }
        }
        GeneratedTableDdlLifecycle::Complete => {
            let Some(table_id) = table_id else {
                return Err(invalid_generated_table_ddl_lifecycle());
            };
            if !stored_migration.is_complete()
                || provisioning_id.is_none()
                || provisioning_schema_digest.is_none()
                || provisioning_id != expected_provisioning_id
                || snapshot.active_table_provisioning.is_some()
            {
                return Err(invalid_generated_table_ddl_lifecycle());
            }
            let Some(table) = catalog.table_by_id(table_id) else {
                return Err(invalid_generated_table_ddl_lifecycle());
            };
            if table.database_id() != declaration.database_id()
                || table.name() != declaration.name()
                || table.placement() != declaration.placement()
                || table.generated_id_policy() != declaration.generated_id_policy()
                || snapshot
                    .active_native_id_table_ids
                    .binary_search(&table_id)
                    .is_err()
            {
                return Err(invalid_generated_table_ddl_lifecycle());
            }
        }
    }
    Ok(Some(GeneratedTableDdl {
        logical_id,
        source_dialect,
        translation_version: GENERATED_TABLE_DDL_TRANSLATION_VERSION,
        source_sql,
        physical_migration_id,
        physical_sql,
        declaration,
        lifecycle,
        provisioning_id,
        provisioning_schema_digest,
        table_id,
    }))
}

fn invalid_generated_table_ddl_lifecycle() -> EngineError {
    EngineError::new(
        EngineErrorKind::DataCorruption,
        "generated-table DDL lifecycle is inconsistent with durable migration and catalog state",
    )
}

fn validate_integrity_manifest(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
    version: u32,
    downgrade_fence_sql: &str,
) -> EngineResult<ManifestSnapshot> {
    validate_integrity_manifest_with_definition(
        connection,
        requested_shards,
        objects,
        IntegrityManifestDefinition {
            version,
            downgrade_fence_sql,
            expected_objects: &v7_objects(),
            expected_manifest_digest_version: V1_MANIFEST_DIGEST_VERSION,
            generated_ids: false,
        },
    )
}

#[derive(Debug, Clone, Copy)]
struct IntegrityManifestDefinition<'a> {
    version: u32,
    downgrade_fence_sql: &'a str,
    expected_objects: &'a [SchemaObject],
    expected_manifest_digest_version: u32,
    generated_ids: bool,
}

fn generated_ids_table_sql(version: u32) -> &'static str {
    if version >= V10_SCHEMA_VERSION {
        V10_GENERATED_IDS_TABLE_SQL
    } else {
        V9_GENERATED_IDS_TABLE_SQL
    }
}

fn validate_integrity_manifest_with_definition(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
    definition: IntegrityManifestDefinition<'_>,
) -> EngineResult<ManifestSnapshot> {
    if objects != definition.expected_objects {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "manifest schema version {} has unexpected database objects",
                definition.version
            ),
        ));
    }

    // Establish the semantic root's authority before interpreting extensible
    // catalog fields. This distinguishes an unsealed bit flip (corruption)
    // from a correctly sealed policy written by a newer compatible build
    // (failed precondition when that field is decoded below).
    validate_manifest_semantic_root(connection, definition.expected_manifest_digest_version)?;

    let mut snapshot = validate_catalog_manifest(
        connection,
        requested_shards,
        objects,
        CatalogManifestDefinition {
            version: definition.version,
            downgrade_fence_sql: definition.downgrade_fence_sql,
            expected_objects: definition.expected_objects,
            schema_catalog_sql: V6_SCHEMA_CATALOG_TABLE_SQL,
            generation_policy: SchemaGenerationPolicy::Journaled,
            generated_ids: definition.generated_ids,
        },
    )?;
    validate_table(
        connection,
        "briskdb_shard_layout",
        &[
            TableColumn::expected(0, "singleton", "INTEGER", false, 1),
            TableColumn::expected(1, "layout_id", "BLOB", true, 0),
            TableColumn::expected(2, "shard_application_id", "INTEGER", true, 0),
            TableColumn::expected(3, "shard_metadata_version", "INTEGER", true, 0),
            TableColumn::expected(4, "layout_state", "INTEGER", true, 0),
        ],
        true,
    )?;
    validate_table_sql(
        connection,
        "briskdb_shard_layout",
        V5_SHARD_LAYOUT_TABLE_SQL,
    )?;
    let layout = validate_shard_layout(connection)?;

    validate_table(
        connection,
        "briskdb_schema_migrations",
        &[
            TableColumn::expected(0, "target_generation", "INTEGER", false, 1),
            TableColumn::expected(1, "source_generation", "INTEGER", true, 0),
            TableColumn::expected(2, "migration_id", "BLOB", true, 0),
            TableColumn::expected(3, "digest_version", "INTEGER", true, 0),
            TableColumn::expected(4, "sql_text", "TEXT", true, 0),
            TableColumn::expected(5, "shard_count", "INTEGER", true, 0),
            TableColumn::expected(6, "migration_state", "INTEGER", true, 0),
            TableColumn::expected(7, "next_shard", "INTEGER", true, 0),
        ],
        true,
    )?;
    validate_table_sql(
        connection,
        "briskdb_schema_migrations",
        V6_SCHEMA_MIGRATIONS_TABLE_SQL,
    )?;
    validate_table(
        connection,
        "briskdb_integrity",
        &[
            TableColumn::expected(0, "singleton", "INTEGER", false, 1),
            TableColumn::expected(1, "manifest_digest_version", "INTEGER", true, 0),
            TableColumn::expected(2, "manifest_digest", "BLOB", true, 0),
            TableColumn::expected(3, "schema_digest_version", "INTEGER", true, 0),
            TableColumn::expected(4, "database_state", "INTEGER", true, 0),
            TableColumn::expected(5, "committed_schema_digest", "BLOB", false, 0),
            TableColumn::expected(6, "target_schema_digest", "BLOB", false, 0),
        ],
        true,
    )?;
    validate_table_sql(connection, "briskdb_integrity", V7_INTEGRITY_TABLE_SQL)?;

    let catalog_generation = snapshot
        .logical_catalog
        .as_ref()
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                "current manifest validation omitted its logical catalog",
            )
        })?
        .schema_generation();
    let active =
        validate_schema_migration_history(connection, snapshot.shard_count, catalog_generation)?;
    let integrity = validate_manifest_integrity(
        connection,
        &layout,
        active.as_ref(),
        definition.expected_manifest_digest_version,
    )?;
    snapshot.shard_layout = Some(layout);
    snapshot.active_migration = active;
    snapshot.integrity = Some(integrity);
    Ok(snapshot)
}

fn validate_manifest_semantic_root(
    connection: &Connection,
    expected_manifest_digest_version: u32,
) -> EngineResult<()> {
    let rows = connection
        .prepare(
            "SELECT singleton, manifest_digest_version, manifest_digest
             FROM briskdb_integrity
             ORDER BY singleton
             LIMIT 3",
        )
        .and_then(|mut statement| {
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| {
            manifest_read_error(error, "failed to read manifest checksum authority")
        })?;
    if rows.len() != 1 || rows[0].0 != 1 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest integrity metadata must contain exactly its singleton row",
        ));
    }
    let (_, version, stored_root) = &rows[0];
    if *version <= 0 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest checksum version must be positive",
        ));
    }
    if *version > i64::from(V5_MANIFEST_DIGEST_VERSION) {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "manifest checksum version is newer than this BriskDB build supports",
        ));
    }
    if *version != i64::from(expected_manifest_digest_version) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest checksum version does not match its schema version",
        ));
    }
    let stored_root = digest_from_blob(stored_root, "manifest semantic checksum")?;
    if manifest_semantic_digest_for_version(connection, expected_manifest_digest_version)?
        != stored_root
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest semantic checksum does not match its authoritative contents",
        ));
    }
    Ok(())
}

struct ManifestDigestQuery {
    table: &'static str,
    columns: &'static [&'static str],
    sql: &'static str,
}

const V1_MANIFEST_DIGEST_QUERIES: &[ManifestDigestQuery] = &[
    ManifestDigestQuery {
        table: "briskdb_manifest",
        columns: &["singleton", "shard_count"],
        sql: "SELECT singleton, shard_count FROM briskdb_manifest ORDER BY singleton",
    },
    ManifestDigestQuery {
        table: "briskdb_metadata",
        columns: &["requires_manifest_version"],
        sql: "SELECT requires_manifest_version FROM briskdb_metadata ORDER BY rowid",
    },
    ManifestDigestQuery {
        table: "briskdb_routing",
        columns: &[
            "singleton",
            "hash_version",
            "key_encoding_version",
            "bucket_algorithm_version",
            "virtual_bucket_count",
            "map_generation",
        ],
        sql: "SELECT singleton, hash_version, key_encoding_version, bucket_algorithm_version, virtual_bucket_count, map_generation FROM briskdb_routing ORDER BY singleton",
    },
    ManifestDigestQuery {
        table: "briskdb_physical_shards",
        columns: &["shard_id", "lifecycle_state"],
        sql: "SELECT shard_id, lifecycle_state FROM briskdb_physical_shards ORDER BY shard_id",
    },
    ManifestDigestQuery {
        table: "briskdb_virtual_buckets",
        columns: &["bucket_id", "physical_shard_id"],
        sql: "SELECT bucket_id, physical_shard_id FROM briskdb_virtual_buckets ORDER BY bucket_id",
    },
    ManifestDigestQuery {
        table: "briskdb_logical_databases",
        columns: &["database_id", "database_name"],
        sql: "SELECT database_id, database_name FROM briskdb_logical_databases ORDER BY database_id",
    },
    ManifestDigestQuery {
        table: "briskdb_schema_catalog",
        columns: &[
            "singleton",
            "identifier_encoding_version",
            "schema_generation",
            "default_database_id",
        ],
        sql: "SELECT singleton, identifier_encoding_version, schema_generation, default_database_id FROM briskdb_schema_catalog ORDER BY singleton",
    },
    ManifestDigestQuery {
        table: "briskdb_tables",
        columns: &[
            "table_id",
            "database_id",
            "table_name",
            "placement",
            "shard_key_column",
            "shard_key_type",
        ],
        sql: "SELECT table_id, database_id, table_name, placement, shard_key_column, shard_key_type FROM briskdb_tables ORDER BY table_id",
    },
    ManifestDigestQuery {
        table: "briskdb_shard_layout",
        columns: &[
            "singleton",
            "layout_id",
            "shard_application_id",
            "shard_metadata_version",
            "layout_state",
        ],
        sql: "SELECT singleton, layout_id, shard_application_id, shard_metadata_version, layout_state FROM briskdb_shard_layout ORDER BY singleton",
    },
    ManifestDigestQuery {
        table: "briskdb_schema_migrations",
        columns: &[
            "target_generation",
            "source_generation",
            "migration_id",
            "digest_version",
            "sql_text",
            "shard_count",
            "migration_state",
            "next_shard",
        ],
        sql: "SELECT target_generation, source_generation, migration_id, digest_version, sql_text, shard_count, migration_state, next_shard FROM briskdb_schema_migrations ORDER BY target_generation",
    },
    ManifestDigestQuery {
        table: "briskdb_integrity",
        columns: &[
            "singleton",
            "manifest_digest_version",
            "schema_digest_version",
            "database_state",
            "committed_schema_digest",
            "target_schema_digest",
        ],
        sql: "SELECT singleton, manifest_digest_version, schema_digest_version, database_state, committed_schema_digest, target_schema_digest FROM briskdb_integrity ORDER BY singleton",
    },
];

const V2_ALLOCATION_OWNERS_DIGEST_QUERY: ManifestDigestQuery = ManifestDigestQuery {
    table: "briskdb_allocation_owners",
    columns: &["owner_slot", "physical_shard_id"],
    sql: "SELECT owner_slot, physical_shard_id FROM briskdb_allocation_owners ORDER BY owner_slot",
};
const V2_GENERATED_IDS_DIGEST_QUERY: ManifestDigestQuery = ManifestDigestQuery {
    table: "briskdb_generated_ids",
    columns: &["table_id", "policy", "generated_column", "encoding_version"],
    sql: "SELECT table_id, policy, generated_column, encoding_version FROM briskdb_generated_ids ORDER BY table_id",
};
const V3_GENERATED_IDS_DIGEST_QUERY: ManifestDigestQuery = ManifestDigestQuery {
    table: "briskdb_generated_ids",
    columns: &[
        "table_id",
        "policy",
        "generated_column",
        "encoding_version",
        "activation_state",
    ],
    sql: "SELECT table_id, policy, generated_column, encoding_version, activation_state FROM briskdb_generated_ids ORDER BY table_id",
};
const V3_ALLOCATION_OWNERS_DIGEST_QUERY: ManifestDigestQuery = ManifestDigestQuery {
    table: "briskdb_allocation_owners",
    columns: &["owner_slot", "physical_shard_id", "owner_state"],
    sql: "SELECT owner_slot, physical_shard_id, owner_state FROM briskdb_allocation_owners ORDER BY owner_slot",
};
const V3_TABLE_PROVISIONING_DIGEST_QUERY: ManifestDigestQuery = ManifestDigestQuery {
    table: "briskdb_table_provisioning",
    columns: &[
        "singleton",
        "provisioning_id",
        "digest_version",
        "schema_digest_version",
        "committed_schema_digest",
        "shard_count",
        "declaration_count",
        "next_shard",
    ],
    sql: "SELECT singleton, provisioning_id, digest_version, schema_digest_version, committed_schema_digest, shard_count, declaration_count, next_shard FROM briskdb_table_provisioning ORDER BY singleton",
};
const V3_TABLE_PROVISIONING_DECLARATIONS_DIGEST_QUERY: ManifestDigestQuery = ManifestDigestQuery {
    table: "briskdb_table_provisioning_declarations",
    columns: &[
        "provisioning_singleton",
        "ordinal",
        "database_id",
        "table_name",
        "placement",
        "shard_key_column",
        "shard_key_type",
        "generated_policy",
        "generated_column",
        "generated_encoding_version",
    ],
    sql: "SELECT provisioning_singleton, ordinal, database_id, table_name, placement, shard_key_column, shard_key_type, generated_policy, generated_column, generated_encoding_version FROM briskdb_table_provisioning_declarations ORDER BY provisioning_singleton, ordinal",
};
const V4_HILO_LEASES_DIGEST_QUERY: ManifestDigestQuery = ManifestDigestQuery {
    table: "briskdb_hilo_leases",
    columns: &[
        "table_id",
        "block_size",
        "next_sequence",
        "fence_token",
        "last_owner_id",
        "last_first_sequence",
        "last_last_sequence",
    ],
    sql: "SELECT table_id, block_size, next_sequence, fence_token, last_owner_id, last_first_sequence, last_last_sequence FROM briskdb_hilo_leases ORDER BY table_id",
};
const V5_GENERATED_TABLE_DDL_DIGEST_QUERY: ManifestDigestQuery = ManifestDigestQuery {
    table: "briskdb_generated_table_ddl",
    columns: &[
        "singleton",
        "logical_id",
        "logical_digest_version",
        "source_dialect",
        "translation_version",
        "source_sql",
        "physical_migration_id",
        "physical_sql",
        "database_id",
        "table_name",
        "generated_column",
        "generated_policy",
        "generated_encoding_version",
        "lifecycle_state",
        "provisioning_id",
        "provisioning_schema_digest",
        "table_id",
    ],
    sql: "SELECT singleton, logical_id, logical_digest_version, source_dialect, translation_version, source_sql, physical_migration_id, physical_sql, database_id, table_name, generated_column, generated_policy, generated_encoding_version, lifecycle_state, provisioning_id, provisioning_schema_digest, table_id FROM briskdb_generated_table_ddl ORDER BY singleton",
};

fn manifest_semantic_digest_for_version(
    connection: &Connection,
    digest_version: u32,
) -> EngineResult<[u8; 32]> {
    let (domain, queries) = match digest_version {
        V1_MANIFEST_DIGEST_VERSION => (
            V1_MANIFEST_DIGEST_DOMAIN,
            V1_MANIFEST_DIGEST_QUERIES.iter().collect::<Vec<_>>(),
        ),
        V2_MANIFEST_DIGEST_VERSION => {
            let mut queries = Vec::with_capacity(V1_MANIFEST_DIGEST_QUERIES.len() + 2);
            for query in V1_MANIFEST_DIGEST_QUERIES {
                queries.push(query);
                if query.table == "briskdb_physical_shards" {
                    queries.push(&V2_ALLOCATION_OWNERS_DIGEST_QUERY);
                }
                if query.table == "briskdb_tables" {
                    queries.push(&V2_GENERATED_IDS_DIGEST_QUERY);
                }
            }
            (V2_MANIFEST_DIGEST_DOMAIN, queries)
        }
        V3_MANIFEST_DIGEST_VERSION => {
            let mut queries = Vec::with_capacity(V1_MANIFEST_DIGEST_QUERIES.len() + 4);
            for query in V1_MANIFEST_DIGEST_QUERIES {
                queries.push(query);
                if query.table == "briskdb_physical_shards" {
                    queries.push(&V3_ALLOCATION_OWNERS_DIGEST_QUERY);
                }
                if query.table == "briskdb_tables" {
                    queries.push(&V3_GENERATED_IDS_DIGEST_QUERY);
                    queries.push(&V3_TABLE_PROVISIONING_DIGEST_QUERY);
                    queries.push(&V3_TABLE_PROVISIONING_DECLARATIONS_DIGEST_QUERY);
                }
            }
            (V3_MANIFEST_DIGEST_DOMAIN, queries)
        }
        V4_MANIFEST_DIGEST_VERSION => {
            let mut queries = Vec::with_capacity(V1_MANIFEST_DIGEST_QUERIES.len() + 5);
            for query in V1_MANIFEST_DIGEST_QUERIES {
                queries.push(query);
                if query.table == "briskdb_physical_shards" {
                    queries.push(&V3_ALLOCATION_OWNERS_DIGEST_QUERY);
                }
                if query.table == "briskdb_tables" {
                    queries.push(&V3_GENERATED_IDS_DIGEST_QUERY);
                    queries.push(&V4_HILO_LEASES_DIGEST_QUERY);
                    queries.push(&V3_TABLE_PROVISIONING_DIGEST_QUERY);
                    queries.push(&V3_TABLE_PROVISIONING_DECLARATIONS_DIGEST_QUERY);
                }
            }
            (V4_MANIFEST_DIGEST_DOMAIN, queries)
        }
        V5_MANIFEST_DIGEST_VERSION => {
            let mut queries = Vec::with_capacity(V1_MANIFEST_DIGEST_QUERIES.len() + 6);
            for query in V1_MANIFEST_DIGEST_QUERIES {
                queries.push(query);
                if query.table == "briskdb_physical_shards" {
                    queries.push(&V3_ALLOCATION_OWNERS_DIGEST_QUERY);
                }
                if query.table == "briskdb_tables" {
                    queries.push(&V3_GENERATED_IDS_DIGEST_QUERY);
                    queries.push(&V5_GENERATED_TABLE_DDL_DIGEST_QUERY);
                    queries.push(&V4_HILO_LEASES_DIGEST_QUERY);
                    queries.push(&V3_TABLE_PROVISIONING_DIGEST_QUERY);
                    queries.push(&V3_TABLE_PROVISIONING_DECLARATIONS_DIGEST_QUERY);
                }
            }
            (V5_MANIFEST_DIGEST_DOMAIN, queries)
        }
        0 => {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "manifest checksum version must be positive",
            ));
        }
        version => {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "manifest checksum version {version} is newer than this BriskDB build supports"
                ),
            ));
        }
    };
    let (application_id, user_version) = read_identity(connection)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hash_manifest_name(&mut hasher, b"application_id");
    hash_manifest_value(&mut hasher, ValueRef::Integer(application_id))?;
    hash_manifest_name(&mut hasher, b"user_version");
    hash_manifest_value(&mut hasher, ValueRef::Integer(user_version))?;

    for query in queries {
        hasher.update(&[0x10]);
        hash_manifest_name(&mut hasher, query.table.as_bytes());
        hasher.update(
            &u64::try_from(query.columns.len())
                .expect("manifest digest column count fits u64")
                .to_le_bytes(),
        );
        for column in query.columns {
            hash_manifest_name(&mut hasher, column.as_bytes());
        }
        let mut statement = connection.prepare(query.sql).map_err(|error| {
            manifest_read_error(error, "failed to prepare manifest semantic checksum")
        })?;
        let column_count = query.columns.len();
        let mut rows = statement.query([]).map_err(|error| {
            manifest_read_error(error, "failed to read manifest semantic checksum")
        })?;
        while let Some(row) = rows.next().map_err(|error| {
            manifest_read_error(error, "failed to read manifest semantic checksum")
        })? {
            hasher.update(&[0x11]);
            for index in 0..column_count {
                let value = row.get_ref(index).map_err(|error| {
                    manifest_read_error(error, "failed to decode manifest semantic checksum")
                })?;
                hash_manifest_value(&mut hasher, value)?;
            }
        }
        hasher.update(&[0x12]);
    }
    hasher.update(&[0xff]);
    Ok(*hasher.finalize().as_bytes())
}

fn manifest_semantic_digest(connection: &Connection) -> EngineResult<[u8; 32]> {
    let digest_version = connection
        .query_row(
            "SELECT manifest_digest_version FROM briskdb_integrity WHERE singleton = 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|error| manifest_read_error(error, "failed to read manifest checksum version"))?;
    let digest_version = u32::try_from(digest_version).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "manifest checksum version is outside the supported numeric range",
            error,
        )
    })?;
    manifest_semantic_digest_for_version(connection, digest_version)
}

fn hash_manifest_name(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(
        &u64::try_from(value.len())
            .expect("manifest digest field length fits u64")
            .to_le_bytes(),
    );
    hasher.update(value);
}

fn hash_manifest_value(hasher: &mut blake3::Hasher, value: ValueRef<'_>) -> EngineResult<()> {
    match value {
        ValueRef::Null => {
            hasher.update(&[0]);
        }
        ValueRef::Integer(value) => {
            hasher.update(&[1]);
            hasher.update(&value.to_le_bytes());
        }
        ValueRef::Text(value) => {
            hasher.update(&[2]);
            hash_manifest_name(hasher, value);
        }
        ValueRef::Blob(value) => {
            hasher.update(&[3]);
            hash_manifest_name(hasher, value);
        }
        ValueRef::Real(_) => {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "manifest semantic checksum encountered an unsupported value type",
            ));
        }
    }
    Ok(())
}

fn refresh_manifest_digest(connection: &Connection) -> EngineResult<[u8; 32]> {
    let digest = manifest_semantic_digest(connection)?;
    let changed = connection
        .execute(
            "UPDATE briskdb_integrity SET manifest_digest = ?1 WHERE singleton = 1",
            [digest.as_slice()],
        )
        .map_err(sqlite_error::storage)?;
    if changed != 1 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest integrity metadata is missing its singleton row",
        ));
    }
    Ok(digest)
}

fn refresh_manifest_digest_if_checksummed(connection: &Connection) -> EngineResult<()> {
    let (application_id, version) = read_identity(connection)?;
    if application_id == MANIFEST_APPLICATION_ID
        && matches!(
            u32::try_from(version),
            Ok(V7_SCHEMA_VERSION
                | V8_SCHEMA_VERSION
                | V9_SCHEMA_VERSION
                | V10_SCHEMA_VERSION
                | V11_SCHEMA_VERSION
                | V12_SCHEMA_VERSION)
        )
    {
        let _ = refresh_manifest_digest(connection)?;
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn reseal_manifest_for_test(connection: &Connection) -> EngineResult<[u8; 32]> {
    refresh_manifest_digest(connection)
}

fn validate_manifest_integrity(
    connection: &Connection,
    layout: &ShardLayout,
    active_migration: Option<&SchemaMigration>,
    expected_manifest_digest_version: u32,
) -> EngineResult<ManifestIntegrity> {
    let rows = connection
        .prepare(
            "SELECT singleton,
                    manifest_digest_version,
                    manifest_digest,
                    schema_digest_version,
                    database_state,
                    committed_schema_digest,
                    target_schema_digest
             FROM briskdb_integrity
             ORDER BY singleton
             LIMIT 3",
        )
        .and_then(|mut statement| {
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<Vec<u8>>>(5)?,
                        row.get::<_, Option<Vec<u8>>>(6)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| {
            manifest_read_error(error, "failed to read manifest integrity metadata")
        })?;
    if rows.len() != 1 || rows[0].0 != 1 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest integrity metadata must contain exactly its singleton row",
        ));
    }
    let (_, manifest_version, stored_root, schema_version, state_code, committed, target) =
        &rows[0];
    if *manifest_version <= 0 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest checksum version must be positive",
        ));
    }
    if *manifest_version > i64::from(V5_MANIFEST_DIGEST_VERSION) {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "manifest checksum version is newer than this BriskDB build supports",
        ));
    }
    if *manifest_version != i64::from(expected_manifest_digest_version) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest checksum version does not match its schema version",
        ));
    }
    if *schema_version <= 0 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "application-schema checksum version must be positive",
        ));
    }
    if *schema_version > i64::from(SCHEMA_DIGEST_VERSION) {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "application-schema checksum version is newer than this BriskDB build supports",
        ));
    }
    let stored_root = digest_from_blob(stored_root, "manifest semantic checksum")?;
    let committed = committed
        .as_deref()
        .map(|digest| digest_from_blob(digest, "committed application-schema checksum"))
        .transpose()?;
    let target = target
        .as_deref()
        .map(|digest| digest_from_blob(digest, "target application-schema checksum"))
        .transpose()?;
    if manifest_semantic_digest_for_version(connection, expected_manifest_digest_version)?
        != stored_root
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest semantic checksum does not match its authoritative contents",
        ));
    }
    let state = DatabaseIntegrityState::from_code(*state_code)?;
    match state {
        DatabaseIntegrityState::Verifying => {
            if active_migration.is_some() || target.is_some() {
                return Err(invalid_integrity_state());
            }
        }
        DatabaseIntegrityState::Ready => {
            if layout.state() != ShardLayoutState::Ready
                || active_migration.is_some()
                || committed.is_none()
                || target.is_some()
            {
                return Err(invalid_integrity_state());
            }
        }
        DatabaseIntegrityState::Migrating => {
            if layout.state() != ShardLayoutState::Ready
                || active_migration.is_none()
                || committed.is_none()
                || target.is_none()
            {
                return Err(invalid_integrity_state());
            }
        }
        DatabaseIntegrityState::Degraded => {
            if active_migration.is_some() {
                if committed.is_none() || target.is_none() {
                    return Err(invalid_integrity_state());
                }
            } else if target.is_some() {
                return Err(invalid_integrity_state());
            }
        }
    }
    Ok(ManifestIntegrity {
        state,
        committed_schema_digest: committed,
        target_schema_digest: target,
    })
}

fn digest_from_blob(value: &[u8], description: &'static str) -> EngineResult<[u8; 32]> {
    value.try_into().map_err(|_| {
        EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("{description} is not exactly 32 bytes"),
        )
    })
}

fn invalid_integrity_state() -> EngineError {
    EngineError::new(
        EngineErrorKind::DataCorruption,
        "manifest integrity state is inconsistent with durable database metadata",
    )
}

#[derive(Debug, Clone, Copy)]
enum SchemaGenerationPolicy {
    InitialOnly,
    Journaled,
}

#[derive(Debug, Clone, Copy)]
struct CatalogManifestDefinition<'a> {
    version: u32,
    downgrade_fence_sql: &'a str,
    expected_objects: &'a [SchemaObject],
    schema_catalog_sql: &'a str,
    generation_policy: SchemaGenerationPolicy,
    generated_ids: bool,
}

fn validate_catalog_manifest(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
    definition: CatalogManifestDefinition<'_>,
) -> EngineResult<ManifestSnapshot> {
    if objects != definition.expected_objects {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "manifest schema version {} has unexpected database objects",
                definition.version
            ),
        ));
    }

    let (shard_count, routing_catalog) = validate_routing_manifest(
        connection,
        requested_shards,
        definition.version,
        definition.downgrade_fence_sql,
    )?;
    validate_table(
        connection,
        "briskdb_logical_databases",
        &[
            TableColumn::expected(0, "database_id", "INTEGER", false, 1),
            TableColumn::expected(1, "database_name", "TEXT", true, 0),
        ],
        true,
    )?;
    validate_table_sql(
        connection,
        "briskdb_logical_databases",
        V4_LOGICAL_DATABASES_TABLE_SQL,
    )?;
    validate_table(
        connection,
        "briskdb_schema_catalog",
        &[
            TableColumn::expected(0, "singleton", "INTEGER", false, 1),
            TableColumn::expected(1, "identifier_encoding_version", "INTEGER", true, 0),
            TableColumn::expected(2, "schema_generation", "INTEGER", true, 0),
            TableColumn::expected(3, "default_database_id", "INTEGER", true, 0),
        ],
        true,
    )?;
    validate_table_sql(
        connection,
        "briskdb_schema_catalog",
        definition.schema_catalog_sql,
    )?;
    validate_table(
        connection,
        "briskdb_tables",
        &[
            TableColumn::expected(0, "table_id", "INTEGER", false, 1),
            TableColumn::expected(1, "database_id", "INTEGER", true, 0),
            TableColumn::expected(2, "table_name", "TEXT", true, 0),
            TableColumn::expected(3, "placement", "INTEGER", true, 0),
            TableColumn::expected(4, "shard_key_column", "TEXT", false, 0),
            TableColumn::expected(5, "shard_key_type", "INTEGER", false, 0),
        ],
        true,
    )?;
    validate_table_sql(connection, "briskdb_tables", V4_TABLES_TABLE_SQL)?;
    if definition.generated_ids {
        let mut columns = vec![
            TableColumn::expected(0, "table_id", "INTEGER", false, 1),
            TableColumn::expected(1, "policy", "INTEGER", true, 0),
            TableColumn::expected(2, "generated_column", "TEXT", false, 0),
            TableColumn::expected(3, "encoding_version", "INTEGER", false, 0),
        ];
        if definition.version >= V10_SCHEMA_VERSION {
            columns.push(TableColumn::expected(
                4,
                "activation_state",
                "INTEGER",
                true,
                0,
            ));
        }
        validate_table(connection, "briskdb_generated_ids", &columns, true)?;
        validate_table_sql(
            connection,
            "briskdb_generated_ids",
            generated_ids_table_sql(definition.version),
        )?;
    }

    let catalog_configuration =
        validate_schema_catalog_configuration(connection, definition.generation_policy)?;
    let databases =
        validate_logical_databases(connection, catalog_configuration.default_database_id)?;
    let tables = validate_table_metadata(
        connection,
        &databases,
        definition.generated_ids,
        definition.version,
    )?;
    validate_foreign_keys(connection)?;

    Ok(ManifestSnapshot {
        shard_count,
        routing_catalog: Some(routing_catalog),
        logical_catalog: Some(Catalog::from_validated_parts(
            catalog_configuration.identifier_encoding_version,
            catalog_configuration.schema_generation,
            catalog_configuration.default_database_id,
            databases,
            tables,
        )),
        shard_layout: None,
        active_migration: None,
        integrity: None,
        allocation_owners: None,
        active_native_id_table_ids: Box::new([]),
        active_hilo_id_table_ids: Box::new([]),
        active_table_provisioning: None,
        generated_table_ddl: None,
    })
}

fn validate_shard_layout(connection: &Connection) -> EngineResult<ShardLayout> {
    let mut statement = connection
        .prepare(
            "SELECT singleton,
                    layout_id,
                    shard_application_id,
                    shard_metadata_version,
                    layout_state
             FROM briskdb_shard_layout
             ORDER BY singleton
             LIMIT 3",
        )
        .map_err(|error| manifest_read_error(error, "failed to read physical shard layout"))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .map_err(|error| manifest_read_error(error, "failed to read physical shard layout"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| manifest_read_error(error, "failed to read physical shard layout"))?;
    if rows.len() != 1 || rows[0].0 != 1 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "physical shard layout must contain exactly its singleton row",
        ));
    }

    let (_, layout_id, application_id, metadata_version, state) = &rows[0];
    let layout_id: [u8; 16] = layout_id.as_slice().try_into().map_err(|_| {
        EngineError::new(
            EngineErrorKind::DataCorruption,
            "physical shard layout identifier must contain exactly 16 bytes",
        )
    })?;
    if *application_id != SHARD_APPLICATION_ID {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "physical shard layout has unsupported application identifier {application_id:#010x}"
            ),
        ));
    }
    let metadata_version = u32::try_from(*metadata_version).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "physical shard metadata version is outside the supported numeric range",
            error,
        )
    })?;
    if metadata_version != SHARD_METADATA_VERSION {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("physical shard layout has unsupported metadata version {metadata_version}"),
        ));
    }
    let state = ShardLayoutState::from_code(*state)?;
    Ok(ShardLayout::from_validated_parts(
        layout_id,
        *application_id,
        metadata_version,
        state,
    ))
}

fn validate_allocation_owners(
    connection: &Connection,
    shard_count: u16,
) -> EngineResult<AllocationOwnerMap> {
    let current = read_identity(connection)?.1 >= i64::from(V10_SCHEMA_VERSION);
    validate_table(
        connection,
        "briskdb_allocation_owners",
        &if current {
            vec![
                TableColumn::expected(0, "owner_slot", "INTEGER", false, 1),
                TableColumn::expected(1, "physical_shard_id", "INTEGER", true, 0),
                TableColumn::expected(2, "owner_state", "INTEGER", true, 0),
            ]
        } else {
            vec![
                TableColumn::expected(0, "owner_slot", "INTEGER", false, 1),
                TableColumn::expected(1, "physical_shard_id", "INTEGER", true, 0),
            ]
        },
        true,
    )?;
    validate_table_sql(
        connection,
        "briskdb_allocation_owners",
        if current {
            V10_ALLOCATION_OWNERS_TABLE_SQL
        } else {
            V9_ALLOCATION_OWNERS_TABLE_SQL
        },
    )?;

    if current {
        let sql: String = connection
            .query_row(
                "SELECT sql FROM sqlite_schema
                 WHERE type = 'index' AND name = 'briskdb_one_active_owner_per_shard'",
                [],
                |row| row.get(0),
            )
            .map_err(|error| manifest_read_error(error, "failed to inspect active-owner index"))?;
        if normalize_schema_sql(&sql) != normalize_schema_sql(V10_ACTIVE_OWNER_INDEX_SQL) {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "allocation-owner active index has an incompatible definition",
            ));
        }
    }

    let sql = if current {
        "SELECT owner_slot, physical_shard_id, owner_state
         FROM briskdb_allocation_owners
         ORDER BY owner_slot
         LIMIT 1025"
    } else {
        "SELECT owner_slot, physical_shard_id, 1 AS owner_state
         FROM briskdb_allocation_owners
         ORDER BY owner_slot
         LIMIT 1025"
    };
    let rows = connection
        .prepare(sql)
        .and_then(|mut statement| {
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| manifest_read_error(error, "failed to read allocation-owner metadata"))?;
    if rows.len() < usize::from(shard_count) || rows.len() > 1_024 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "allocation-owner metadata has an invalid number of historical owners",
        ));
    }

    let mut owners = Vec::with_capacity(rows.len());
    for (ordinal, (owner_slot, physical_shard_id, owner_state)) in rows.into_iter().enumerate() {
        if !(0..=MAX_ALLOCATION_OWNER_SLOT).contains(&owner_slot) {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "allocation-owner slot is outside the supported range",
            ));
        }
        let owner_slot = u16::try_from(owner_slot).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::DataCorruption,
                "allocation-owner slot is outside the supported range",
                error,
            )
        })?;
        let physical_shard_id = u16::try_from(physical_shard_id).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::DataCorruption,
                "allocation owner references an invalid physical shard",
                error,
            )
        })?;
        if !current
            && (owner_slot != u16::try_from(ordinal).expect("bounded owner ordinal fits u16")
                || physical_shard_id != owner_slot)
        {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "allocation-owner metadata does not match the immutable v9 owner mapping",
            ));
        }
        let state = match owner_state {
            ALLOCATION_OWNER_ACTIVE => AllocationOwnerState::Active,
            ALLOCATION_OWNER_RETIRED => AllocationOwnerState::Retired,
            _ => {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    "allocation-owner metadata has an unsupported lifecycle state",
                ));
            }
        };
        owners.push((owner_slot, physical_shard_id, state));
    }
    AllocationOwnerMap::try_from_assignments(shard_count, owners.into_boxed_slice()).map_err(
        |error| {
            EngineError::from_source(
                EngineErrorKind::DataCorruption,
                "allocation-owner metadata does not define one active allocator with monotonic succession per shard",
                error,
            )
        },
    )
}

fn validate_active_native_id_tables(connection: &Connection) -> EngineResult<Box<[TableId]>> {
    validate_active_generated_id_tables(connection, GENERATED_ID_POLICY_NATIVE_RANGE_V1)
}

fn validate_active_generated_id_tables(
    connection: &Connection,
    selected_policy: i64,
) -> EngineResult<Box<[TableId]>> {
    let rows = connection
        .prepare(
            "SELECT table_id, policy, activation_state
             FROM briskdb_generated_ids
             ORDER BY table_id
             LIMIT 4097",
        )
        .and_then(|mut statement| {
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| {
            manifest_read_error(error, "failed to read generated-ID activation metadata")
        })?;
    if rows.len() > MAX_TABLES {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "generated-ID activation metadata exceeds the table limit",
        ));
    }
    let mut active = Vec::new();
    for (table_id, policy, state) in rows {
        match (policy, state) {
            (GENERATED_ID_POLICY_NONE, GENERATED_ID_INACTIVE) => {}
            (GENERATED_ID_POLICY_NATIVE_RANGE_V1, GENERATED_ID_INACTIVE) => {}
            (GENERATED_ID_POLICY_HILO_V1, GENERATED_ID_INACTIVE) => {}
            (
                GENERATED_ID_POLICY_NATIVE_RANGE_V1 | GENERATED_ID_POLICY_HILO_V1,
                GENERATED_ID_ACTIVE,
            ) => {
                if policy != selected_policy {
                    continue;
                }
                active.push(TableId::from_validated(positive_catalog_id(
                    table_id, "table",
                )?));
            }
            (_, GENERATED_ID_ACTIVE) if policy > GENERATED_ID_POLICY_HILO_V1 => {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!("table {table_id} activates a newer generated-ID policy"),
                ));
            }
            _ => {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!("table {table_id} has inconsistent generated-ID activation metadata"),
                ));
            }
        }
    }
    Ok(active.into_boxed_slice())
}

fn validate_hilo_v1_leases(
    connection: &Connection,
    catalog: Option<&Catalog>,
    active_hilo_tables: &[TableId],
) -> EngineResult<()> {
    validate_table(
        connection,
        "briskdb_hilo_leases",
        &[
            TableColumn::expected(0, "table_id", "INTEGER", false, 1),
            TableColumn::expected(1, "block_size", "INTEGER", true, 0),
            TableColumn::expected(2, "next_sequence", "INTEGER", true, 0),
            TableColumn::expected(3, "fence_token", "INTEGER", true, 0),
            TableColumn::expected(4, "last_owner_id", "BLOB", false, 0),
            TableColumn::expected(5, "last_first_sequence", "INTEGER", false, 0),
            TableColumn::expected(6, "last_last_sequence", "INTEGER", false, 0),
        ],
        true,
    )?;
    validate_table_sql(connection, "briskdb_hilo_leases", V11_HILO_LEASES_TABLE_SQL)?;
    let rows = connection
        .prepare(
            "SELECT table_id, block_size, next_sequence, fence_token,
                    last_owner_id, last_first_sequence, last_last_sequence
             FROM briskdb_hilo_leases
             ORDER BY table_id
             LIMIT 4097",
        )
        .and_then(|mut statement| {
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<Vec<u8>>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<i64>>(6)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| manifest_read_error(error, "failed to read hi/lo lease metadata"))?;
    if rows.len() > MAX_TABLES || rows.len() != active_hilo_tables.len() {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "hi/lo lease metadata must contain exactly one row per active hilo_v1 table",
        ));
    }
    let catalog = catalog.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "hi/lo lease validation omitted the logical catalog",
        )
    })?;
    for (row, expected_id) in rows.into_iter().zip(active_hilo_tables) {
        let (stored_id, block_size, next, fence, owner, first, last) = row;
        let id = TableId::from_validated(positive_catalog_id(stored_id, "table")?);
        if id != *expected_id
            || block_size != i64::try_from(HILO_V1_BLOCK_SIZE).expect("block size fits i64")
            || !(1..=i64::try_from(HILO_V1_EXHAUSTED_HEAD).expect("hi/lo head fits i64"))
                .contains(&next)
            || fence < 0
            || catalog.table_by_id(id).is_none_or(|table| {
                !matches!(
                    table.generated_id_policy(),
                    GeneratedIdPolicy::HiloV1 { .. }
                )
            })
        {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "hi/lo lease metadata conflicts with its active table policy",
            ));
        }
        let initial =
            fence == 0 && next == 1 && owner.is_none() && first.is_none() && last.is_none();
        let leased = fence > 0
            && owner.as_ref().is_some_and(|value| value.len() == 32)
            && first.is_some_and(|value| value >= 1)
            && last.is_some_and(|value| value >= first.unwrap_or_default())
            && last == next.checked_sub(1)
            && last.zip(first).is_some_and(|(last, first)| {
                last - first < i64::try_from(HILO_V1_BLOCK_SIZE).unwrap()
            });
        if !initial && !leased {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "hi/lo lease metadata has an incoherent durable block",
            ));
        }
    }
    Ok(())
}

fn validate_table_provisioning(
    connection: &Connection,
    manifest_version: u32,
    expected_shard_count: u16,
    catalog: Option<&Catalog>,
    active_migration: Option<&SchemaMigration>,
    integrity: Option<ManifestIntegrity>,
) -> EngineResult<Option<NativeTableProvisioning>> {
    validate_table(
        connection,
        "briskdb_table_provisioning",
        &[
            TableColumn::expected(0, "singleton", "INTEGER", false, 1),
            TableColumn::expected(1, "provisioning_id", "BLOB", true, 0),
            TableColumn::expected(2, "digest_version", "INTEGER", true, 0),
            TableColumn::expected(3, "schema_digest_version", "INTEGER", true, 0),
            TableColumn::expected(4, "committed_schema_digest", "BLOB", true, 0),
            TableColumn::expected(5, "shard_count", "INTEGER", true, 0),
            TableColumn::expected(6, "declaration_count", "INTEGER", true, 0),
            TableColumn::expected(7, "next_shard", "INTEGER", true, 0),
        ],
        true,
    )?;
    validate_table_sql(
        connection,
        "briskdb_table_provisioning",
        V10_TABLE_PROVISIONING_SQL,
    )?;
    validate_table(
        connection,
        "briskdb_table_provisioning_declarations",
        &[
            TableColumn::expected(0, "provisioning_singleton", "INTEGER", true, 1),
            TableColumn::expected(1, "ordinal", "INTEGER", true, 2),
            TableColumn::expected(2, "database_id", "INTEGER", true, 0),
            TableColumn::expected(3, "table_name", "TEXT", true, 0),
            TableColumn::expected(4, "placement", "INTEGER", true, 0),
            TableColumn::expected(5, "shard_key_column", "TEXT", false, 0),
            TableColumn::expected(6, "shard_key_type", "INTEGER", false, 0),
            TableColumn::expected(7, "generated_policy", "INTEGER", true, 0),
            TableColumn::expected(8, "generated_column", "TEXT", false, 0),
            TableColumn::expected(9, "generated_encoding_version", "INTEGER", false, 0),
        ],
        true,
    )?;
    validate_table_sql(
        connection,
        "briskdb_table_provisioning_declarations",
        V10_TABLE_PROVISIONING_DECLARATIONS_SQL,
    )?;

    let singleton_rows = connection
        .prepare(
            "SELECT singleton,
                    provisioning_id,
                    digest_version,
                    schema_digest_version,
                    committed_schema_digest,
                    shard_count,
                    declaration_count,
                    next_shard
             FROM briskdb_table_provisioning
             ORDER BY singleton
             LIMIT 3",
        )
        .and_then(|mut statement| {
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Vec<u8>>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, i64>(7)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| manifest_read_error(error, "failed to read table-provisioning journal"))?;
    if singleton_rows.is_empty() {
        let declaration_exists = connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM briskdb_table_provisioning_declarations
                 )",
                [],
                |row| row.get::<_, bool>(0),
            )
            .map_err(|error| {
                manifest_read_error(error, "failed to inspect table-provisioning declarations")
            })?;
        if declaration_exists {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "table-provisioning declarations exist without their journal",
            ));
        }
        return Ok(None);
    }
    if singleton_rows.len() != 1 || singleton_rows[0].0 != 1 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "table provisioning must contain exactly its singleton journal",
        ));
    }
    if active_migration.is_some() {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "table provisioning cannot overlap an application-schema migration",
        ));
    }
    let (_, id, digest_version, schema_version, committed, shards, count, next) =
        &singleton_rows[0];
    if *digest_version != i64::from(TABLE_PROVISIONING_DIGEST_VERSION)
        || *schema_version != i64::from(SCHEMA_DIGEST_VERSION)
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "table-provisioning journal has unsupported digest metadata",
        ));
    }
    let provisioning_id = digest_from_blob(id, "table-provisioning identifier")?;
    let committed_schema_digest = digest_from_blob(committed, "table-provisioning schema digest")?;
    let integrity_state = integrity.map(ManifestIntegrity::state);
    if integrity.and_then(ManifestIntegrity::committed_schema_digest)
        != Some(committed_schema_digest)
        || !matches!(
            integrity_state,
            Some(DatabaseIntegrityState::Ready | DatabaseIntegrityState::Degraded)
        )
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "table provisioning does not match a valid committed schema",
        ));
    }
    let shard_count = u16::try_from(*shards).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "table-provisioning shard count is outside the supported range",
            error,
        )
    })?;
    let next_shard = u16::try_from(*next).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "table-provisioning progress is outside the supported range",
            error,
        )
    })?;
    if shard_count != expected_shard_count || next_shard > shard_count {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "table provisioning has inconsistent shard progress",
        ));
    }
    let declaration_count = usize::try_from(*count).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "table-provisioning declaration count is outside the supported range",
            error,
        )
    })?;
    let declarations =
        read_table_provisioning_declarations(connection, manifest_version, declaration_count)?;
    if table_provisioning_id(&declarations, shard_count, committed_schema_digest) != provisioning_id
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "table-provisioning identifier does not match its declarations",
        ));
    }
    if !declarations
        .iter()
        .any(|declaration| !matches!(declaration.generated_id_policy(), GeneratedIdPolicy::None))
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "table provisioning does not contain a generated-ID policy",
        ));
    }
    let has_native = declarations.iter().any(|declaration| {
        matches!(
            declaration.generated_id_policy(),
            GeneratedIdPolicy::NativeRangeV1 { .. }
        )
    });
    if !has_native && next_shard != shard_count {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "hi/lo-only table provisioning must not claim pending shard-local work",
        ));
    }
    if let Some(catalog) = catalog {
        if !catalog.tables().is_empty() && !declarations_match_catalog_owned(&declarations, catalog)
        {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "table provisioning declarations conflict with the authoritative catalog",
            ));
        }
    }
    Ok(Some(NativeTableProvisioning {
        provisioning_id,
        committed_schema_digest,
        shard_count,
        declarations,
        next_shard,
    }))
}

fn read_table_provisioning_declarations(
    connection: &Connection,
    manifest_version: u32,
    expected_count: usize,
) -> EngineResult<Box<[TableDeclaration]>> {
    let limit = i64::try_from(MAX_TABLES + 1).expect("table journal limit fits SQLite");
    let rows = connection
        .prepare(
            "SELECT ordinal,
                    database_id,
                    table_name,
                    placement,
                    shard_key_column,
                    shard_key_type,
                    generated_policy,
                    generated_column,
                    generated_encoding_version
             FROM briskdb_table_provisioning_declarations
             ORDER BY ordinal
             LIMIT ?1",
        )
        .and_then(|mut statement| {
            statement
                .query_map([limit], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<i64>>(8)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| {
            manifest_read_error(error, "failed to read table-provisioning declarations")
        })?;
    if rows.len() != expected_count || rows.is_empty() || rows.len() > MAX_TABLES {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "table-provisioning declaration count is inconsistent",
        ));
    }
    let mut declarations = Vec::with_capacity(rows.len());
    for (
        expected_ordinal,
        (
            ordinal,
            database_id,
            name,
            placement_code,
            shard_column,
            shard_type,
            generated_policy,
            generated_column,
            generated_version,
        ),
    ) in rows.into_iter().enumerate()
    {
        if ordinal != i64::try_from(expected_ordinal).expect("bounded ordinal fits SQLite") {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "table-provisioning declaration ordinals are not contiguous",
            ));
        }
        let database_id = crate::core::LogicalDatabaseId::new(positive_catalog_id(
            database_id,
            "logical database",
        )?)
        .map_err(|error| error.context("invalid table-provisioning database ID"))?;
        let placement = match (placement_code, shard_column, shard_type) {
            (SHARDED_PLACEMENT, Some(column), Some(key_type)) => TablePlacement::Sharded(
                ShardKeyMetadata::new(column, decode_shard_key_type(key_type, 1)?)
                    .map_err(|error| error.context("invalid table-provisioning shard key"))?,
            ),
            (GLOBAL_PLACEMENT, None, None) => TablePlacement::Global,
            (CATALOG_PLACEMENT, None, None) => TablePlacement::Catalog,
            _ => {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    "table-provisioning declaration has inconsistent placement",
                ));
            }
        };
        let policy = decode_generated_id_policy(
            1,
            &placement,
            manifest_version,
            Some(generated_policy),
            generated_column,
            generated_version,
        )?;
        let declaration = match placement {
            TablePlacement::Sharded(key) => TableDeclaration::sharded(database_id, name, key),
            TablePlacement::Global => TableDeclaration::global(database_id, name),
            TablePlacement::Catalog => TableDeclaration::catalog(database_id, name),
        }
        .and_then(|declaration| declaration.with_generated_id_policy(policy))
        .map_err(|error| error.context("invalid table-provisioning declaration"))?;
        declarations.push(declaration);
    }
    if !is_sorted_unique_declarations(&declarations) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "table-provisioning declarations are not in canonical order",
        ));
    }
    Ok(declarations.into_boxed_slice())
}

fn validate_schema_catalog_configuration(
    connection: &Connection,
    generation_policy: SchemaGenerationPolicy,
) -> EngineResult<SchemaCatalogConfiguration> {
    let mut statement = connection
        .prepare(
            "SELECT singleton,
                    identifier_encoding_version,
                    schema_generation,
                    default_database_id
             FROM briskdb_schema_catalog
             ORDER BY singleton
             LIMIT 3",
        )
        .map_err(|error| {
            manifest_read_error(error, "failed to read schema catalog configuration")
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .map_err(|error| manifest_read_error(error, "failed to read schema catalog configuration"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            manifest_read_error(error, "failed to read schema catalog configuration")
        })?;
    if rows.len() != 1 || rows[0].0 != 1 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "schema catalog configuration must contain exactly its singleton row",
        ));
    }

    let (_, identifier_encoding_version, schema_generation, default_database_id) = rows[0];
    if identifier_encoding_version != i64::from(IDENTIFIER_ENCODING_VERSION) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "manifest has unsupported identifier-encoding version {identifier_encoding_version}"
            ),
        ));
    }
    let schema_generation = u64::try_from(schema_generation).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "schema generation is outside the supported numeric range",
            error,
        )
    })?;
    match generation_policy {
        SchemaGenerationPolicy::InitialOnly if schema_generation != INITIAL_SCHEMA_GENERATION => {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("manifest has unsupported schema generation {schema_generation}"),
            ));
        }
        SchemaGenerationPolicy::Journaled if schema_generation > MAX_SCHEMA_GENERATION => {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("manifest has unsupported schema generation {schema_generation}"),
            ));
        }
        SchemaGenerationPolicy::InitialOnly | SchemaGenerationPolicy::Journaled => {}
    }
    let default_database_id = u64::try_from(default_database_id).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "default logical database ID is outside the supported numeric range",
            error,
        )
    })?;
    if default_database_id != DEFAULT_LOGICAL_DATABASE_ID {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("manifest has unsupported default logical database ID {default_database_id}"),
        ));
    }

    Ok(SchemaCatalogConfiguration {
        identifier_encoding_version: IDENTIFIER_ENCODING_VERSION,
        schema_generation,
        default_database_id,
    })
}

type StoredSchemaMigrationRow = (i64, i64, Vec<u8>, i64, String, i64, i64, i64);

fn validate_schema_migration_history(
    connection: &Connection,
    expected_shard_count: u16,
    catalog_generation: u64,
) -> EngineResult<Option<SchemaMigration>> {
    let mut statement = connection
        .prepare(
            "SELECT target_generation,
                    source_generation,
                    migration_id,
                    digest_version,
                    sql_text,
                    shard_count,
                    migration_state,
                    next_shard
             FROM briskdb_schema_migrations
             ORDER BY target_generation",
        )
        .map_err(|error| manifest_read_error(error, "failed to read schema migration journal"))?;
    let mut rows = statement
        .query([])
        .map_err(|error| manifest_read_error(error, "failed to read schema migration journal"))?;

    let mut expected_target = 1_u64;
    let mut active = None;
    while let Some(row) = rows
        .next()
        .map_err(|error| manifest_read_error(error, "failed to read schema migration journal"))?
    {
        let stored: StoredSchemaMigrationRow = (
            row.get(0).map_err(|error| {
                manifest_read_error(error, "failed to read schema migration journal")
            })?,
            row.get(1).map_err(|error| {
                manifest_read_error(error, "failed to read schema migration journal")
            })?,
            row.get(2).map_err(|error| {
                manifest_read_error(error, "failed to read schema migration journal")
            })?,
            row.get(3).map_err(|error| {
                manifest_read_error(error, "failed to read schema migration journal")
            })?,
            row.get(4).map_err(|error| {
                manifest_read_error(error, "failed to read schema migration journal")
            })?,
            row.get(5).map_err(|error| {
                manifest_read_error(error, "failed to read schema migration journal")
            })?,
            row.get(6).map_err(|error| {
                manifest_read_error(error, "failed to read schema migration journal")
            })?,
            row.get(7).map_err(|error| {
                manifest_read_error(error, "failed to read schema migration journal")
            })?,
        );
        let migration = schema_migration_from_stored(stored, expected_shard_count)?;
        if migration.target_generation != expected_target {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "schema migration journal is not contiguous at generation {expected_target}"
                ),
            ));
        }

        if migration.target_generation <= catalog_generation {
            if !migration.is_complete() {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!(
                        "committed schema generation {} has an incomplete journal row",
                        migration.target_generation
                    ),
                ));
            }
        } else if migration.target_generation == catalog_generation.saturating_add(1) {
            if !migration.is_applying() || active.is_some() {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    "schema migration journal has an invalid active row",
                ));
            }
            active = Some(migration);
        } else {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "schema migration journal extends beyond the next catalog generation",
            ));
        }
        expected_target = expected_target.checked_add(1).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                "schema migration journal generation overflowed",
            )
        })?;
    }

    if expected_target <= catalog_generation {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("schema migration journal is missing committed generation {expected_target}"),
        ));
    }
    Ok(active)
}

fn schema_migration_from_stored(
    stored: StoredSchemaMigrationRow,
    expected_shard_count: u16,
) -> EngineResult<SchemaMigration> {
    let (
        target_generation,
        source_generation,
        migration_id,
        digest_version,
        sql_text,
        shard_count,
        migration_state,
        next_shard,
    ) = stored;
    let target_generation = u64::try_from(target_generation).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "schema migration target generation is outside the supported range",
            error,
        )
    })?;
    let source_generation = u64::try_from(source_generation).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "schema migration source generation is outside the supported range",
            error,
        )
    })?;
    if !(1..=MAX_SCHEMA_GENERATION).contains(&target_generation)
        || source_generation.checked_add(1) != Some(target_generation)
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "schema migration generations are not consecutive",
        ));
    }
    let migration_id: [u8; 32] = migration_id.as_slice().try_into().map_err(|_| {
        EngineError::new(
            EngineErrorKind::DataCorruption,
            "schema migration identifier must contain exactly 32 bytes",
        )
    })?;
    if digest_version != i64::from(SCHEMA_MIGRATION_DIGEST_VERSION) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("schema migration has unsupported digest version {digest_version}"),
        ));
    }
    validate_stored_schema_migration_sql(&sql_text)?;
    if schema_migration_digest(&sql_text) != migration_id {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "schema migration identifier does not match its exact SQL bytes",
        ));
    }
    let shard_count = u16::try_from(shard_count).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "schema migration shard count is outside the supported range",
            error,
        )
    })?;
    if shard_count != expected_shard_count {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "schema migration targets {shard_count} shards but the manifest has {expected_shard_count}"
            ),
        ));
    }
    let state = SchemaMigrationState::from_code(migration_state)?;
    let next_shard = u16::try_from(next_shard).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "schema migration progress is outside the supported range",
            error,
        )
    })?;
    if next_shard > shard_count
        || (state == SchemaMigrationState::Complete && next_shard != shard_count)
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "schema migration progress is inconsistent with its state",
        ));
    }

    Ok(SchemaMigration {
        source_generation,
        target_generation,
        migration_id,
        sql_text,
        shard_count,
        state,
        next_shard,
    })
}

fn validate_stored_schema_migration_sql(sql: &str) -> EngineResult<()> {
    if sql.is_empty() || sql.len() > MAX_SCHEMA_MIGRATION_SQL_BYTES || sql.as_bytes().contains(&0) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "schema migration SQL violates its storage limits",
        ));
    }
    Ok(())
}

fn validate_schema_migration_sql(sql: &str) -> EngineResult<()> {
    if sql.is_empty() {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "schema migration SQL cannot be empty",
        ));
    }
    if sql.len() > MAX_SCHEMA_MIGRATION_SQL_BYTES {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            format!("schema migration SQL exceeds the {MAX_SCHEMA_MIGRATION_SQL_BYTES}-byte limit"),
        ));
    }
    if sql.as_bytes().contains(&0) {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "schema migration SQL cannot contain a NUL byte",
        ));
    }
    Ok(())
}

fn schema_migration_digest(sql: &str) -> [u8; 32] {
    *blake3::hash(sql.as_bytes()).as_bytes()
}

pub(super) fn schema_migration_id(sql: &str) -> EngineResult<[u8; 32]> {
    validate_schema_migration_sql(sql)?;
    Ok(schema_migration_digest(sql))
}

fn encoded_generated_table_ddl_dialect(dialect: SqlDialect) -> i64 {
    match dialect {
        SqlDialect::Sqlite => GENERATED_TABLE_DDL_SQLITE,
        SqlDialect::PostgreSql => GENERATED_TABLE_DDL_POSTGRESQL,
        SqlDialect::MySql => GENERATED_TABLE_DDL_MYSQL,
    }
}

fn decode_generated_table_ddl_dialect(code: i64) -> EngineResult<SqlDialect> {
    match code {
        GENERATED_TABLE_DDL_SQLITE => Ok(SqlDialect::Sqlite),
        GENERATED_TABLE_DDL_POSTGRESQL => Ok(SqlDialect::PostgreSql),
        GENERATED_TABLE_DDL_MYSQL => Ok(SqlDialect::MySql),
        _ => Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "generated-table DDL bridge uses a newer source dialect encoding",
        )),
    }
}

fn generated_table_ddl_logical_id(
    source_dialect: SqlDialect,
    source_sql: &str,
) -> EngineResult<[u8; 32]> {
    validate_schema_migration_sql(source_sql).map_err(|error| {
        EngineError::from_source(
            error.kind(),
            "generated-table source SQL violates its storage limits",
            error,
        )
    })?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(GENERATED_TABLE_DDL_DIGEST_DOMAIN);
    hasher.update(&GENERATED_TABLE_DDL_DIGEST_VERSION.to_le_bytes());
    hasher.update(&encoded_generated_table_ddl_dialect(source_dialect).to_le_bytes());
    hasher.update(&GENERATED_TABLE_DDL_TRANSLATION_VERSION.to_le_bytes());
    hash_manifest_name(&mut hasher, source_sql.as_bytes());
    Ok(*hasher.finalize().as_bytes())
}

fn generated_table_ddl_declaration(
    database_id: LogicalDatabaseId,
    table_name: String,
    generated_column: String,
) -> EngineResult<TableDeclaration> {
    TableDeclaration::sharded(
        database_id,
        table_name,
        ShardKeyMetadata::new(&generated_column, ShardKeyType::Int64)?,
    )?
    .with_generated_id_policy(GeneratedIdPolicy::native_range_v1(generated_column)?)
}

fn validate_generated_table_ddl_declaration(declaration: &TableDeclaration) -> EngineResult<&str> {
    let (TablePlacement::Sharded(shard_key), GeneratedIdPolicy::NativeRangeV1 { column }) =
        (declaration.placement(), declaration.generated_id_policy())
    else {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "generated-table DDL requires one native_range_v1 Sharded declaration",
        ));
    };
    if shard_key.key_type() != ShardKeyType::Int64 || shard_key.column() != column {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "generated-table DDL generated column must be its Int64 shard key",
        ));
    }
    Ok(column)
}

fn validate_logical_databases(
    connection: &Connection,
    default_database_id: u64,
) -> EngineResult<Box<[LogicalDatabaseMetadata]>> {
    let mut statement = connection
        .prepare(
            "SELECT database_id, database_name
             FROM briskdb_logical_databases
             ORDER BY database_id
             LIMIT ?1",
        )
        .map_err(|error| manifest_read_error(error, "failed to read logical database catalog"))?;
    let limit = i64::try_from(MAX_LOGICAL_DATABASES + 1)
        .expect("logical database inspection limit fits in SQLite");
    let rows = statement
        .query_map([limit], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| manifest_read_error(error, "failed to read logical database catalog"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| manifest_read_error(error, "failed to read logical database catalog"))?;
    if rows.is_empty() || rows.len() > MAX_LOGICAL_DATABASES {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "logical database catalog must contain between 1 and {MAX_LOGICAL_DATABASES} rows"
            ),
        ));
    }

    let mut databases = Vec::with_capacity(rows.len());
    for (stored_id, name) in rows {
        let id = u64::try_from(stored_id).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::DataCorruption,
                "logical database ID is outside the supported numeric range",
                error,
            )
        })?;
        if id == 0 {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "logical database IDs must be positive",
            ));
        }
        if !validate_catalog_identifier(&name) {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("logical database {id} has an invalid catalog name"),
            ));
        }
        databases.push(LogicalDatabaseMetadata::from_validated(id, name));
    }

    let default = databases
        .iter()
        .find(|database| database.id().get() == default_database_id)
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                "logical database catalog is missing its configured default",
            )
        })?;
    if default.name() != DEFAULT_LOGICAL_DATABASE_NAME {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("default logical database must be named {DEFAULT_LOGICAL_DATABASE_NAME}"),
        ));
    }

    Ok(databases.into_boxed_slice())
}

fn validate_table_metadata(
    connection: &Connection,
    databases: &[LogicalDatabaseMetadata],
    generated_ids: bool,
    manifest_version: u32,
) -> EngineResult<Box<[TableMetadata]>> {
    let sql = if generated_ids {
        "SELECT tables.table_id,
                tables.database_id,
                tables.table_name,
                tables.placement,
                tables.shard_key_column,
                tables.shard_key_type,
                generated.policy,
                generated.generated_column,
                generated.encoding_version
         FROM briskdb_tables AS tables
         LEFT JOIN briskdb_generated_ids AS generated
           ON generated.table_id = tables.table_id
         ORDER BY tables.database_id, tables.table_name, tables.table_id
         LIMIT ?1"
    } else {
        "SELECT table_id,
                database_id,
                table_name,
                placement,
                shard_key_column,
                shard_key_type,
                0 AS policy,
                NULL AS generated_column,
                NULL AS encoding_version
         FROM briskdb_tables
         ORDER BY database_id, table_name, table_id
         LIMIT ?1"
    };
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| manifest_read_error(error, "failed to read table metadata catalog"))?;
    let limit = i64::try_from(MAX_TABLES + 1).expect("table inspection limit fits in SQLite");
    let rows = statement
        .query_map([limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<i64>>(8)?,
            ))
        })
        .map_err(|error| manifest_read_error(error, "failed to read table metadata catalog"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| manifest_read_error(error, "failed to read table metadata catalog"))?;
    if rows.len() > MAX_TABLES {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("table metadata catalog exceeds its {MAX_TABLES}-row limit"),
        ));
    }

    let mut tables = Vec::with_capacity(rows.len());
    for (
        stored_table_id,
        stored_database_id,
        name,
        placement,
        column,
        key_type,
        generated_policy,
        generated_column,
        generated_encoding_version,
    ) in rows
    {
        let table_id = positive_catalog_id(stored_table_id, "table")?;
        let database_id = positive_catalog_id(stored_database_id, "logical database")?;
        if databases
            .binary_search_by_key(&database_id, |database| database.id().get())
            .is_err()
        {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("table {table_id} references unknown logical database {database_id}"),
            ));
        }
        if !validate_catalog_identifier(&name) {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("table {table_id} has an invalid catalog name"),
            ));
        }

        let placement = match (placement, column, key_type) {
            (SHARDED_PLACEMENT, Some(column), Some(key_type)) => {
                if !validate_catalog_identifier(&column) {
                    return Err(EngineError::new(
                        EngineErrorKind::DataCorruption,
                        format!("table {table_id} has an invalid shard-key column"),
                    ));
                }
                TablePlacement::Sharded(ShardKeyMetadata::from_validated(
                    column,
                    decode_shard_key_type(key_type, table_id)?,
                ))
            }
            (GLOBAL_PLACEMENT, None, None) => TablePlacement::Global,
            (CATALOG_PLACEMENT, None, None) => TablePlacement::Catalog,
            (placement, _, _) if !matches!(placement, 1..=3) => {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!("table {table_id} has unsupported placement code {placement}"),
                ));
            }
            _ => {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!("table {table_id} has inconsistent placement and shard-key metadata"),
                ));
            }
        };
        let generated_id_policy = decode_generated_id_policy(
            table_id,
            &placement,
            manifest_version,
            generated_policy,
            generated_column,
            generated_encoding_version,
        )?;
        tables.push(TableMetadata::from_validated_with_generated_id_policy(
            table_id,
            database_id,
            name,
            placement,
            generated_id_policy,
        ));
    }

    Ok(tables.into_boxed_slice())
}

fn decode_generated_id_policy(
    table_id: u64,
    placement: &TablePlacement,
    manifest_version: u32,
    policy: Option<i64>,
    column: Option<String>,
    encoding_version: Option<i64>,
) -> EngineResult<GeneratedIdPolicy> {
    match (policy, column, encoding_version) {
        (Some(GENERATED_ID_POLICY_NONE), None, None) => Ok(GeneratedIdPolicy::None),
        (Some(GENERATED_ID_POLICY_NATIVE_RANGE_V1), Some(column), Some(version)) => {
            if !validate_catalog_identifier(&column) {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!("table {table_id} has an invalid generated-ID column"),
                ));
            }
            if version <= 0 {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!("table {table_id} has an invalid generated-ID encoding version"),
                ));
            }
            if version > i64::from(NATIVE_RANGE_V1_ENCODING_VERSION) {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!("table {table_id} uses a newer generated-ID encoding version"),
                ));
            }
            let TablePlacement::Sharded(shard_key) = placement else {
                return Err(inconsistent_generated_id_policy(table_id));
            };
            if shard_key.key_type() != ShardKeyType::Int64 || shard_key.column() != column {
                return Err(inconsistent_generated_id_policy(table_id));
            }
            Ok(GeneratedIdPolicy::native_range_v1_from_validated(column))
        }
        (Some(GENERATED_ID_POLICY_HILO_V1), _, _) if manifest_version < V11_SCHEMA_VERSION => {
            Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!("table {table_id} uses a newer generated-ID policy"),
            ))
        }
        (Some(GENERATED_ID_POLICY_HILO_V1), Some(column), Some(version)) => {
            if !validate_catalog_identifier(&column) {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!("table {table_id} has an invalid generated-ID column"),
                ));
            }
            if version <= 0 {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!("table {table_id} has an invalid generated-ID encoding version"),
                ));
            }
            if version > i64::from(HILO_V1_ENCODING_VERSION) {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!("table {table_id} uses a newer generated-ID encoding version"),
                ));
            }
            let TablePlacement::Sharded(shard_key) = placement else {
                return Err(inconsistent_generated_id_policy(table_id));
            };
            if shard_key.key_type() != ShardKeyType::Int64 || shard_key.column() != column {
                return Err(inconsistent_generated_id_policy(table_id));
            }
            Ok(GeneratedIdPolicy::hilo_v1_from_validated(column))
        }
        (Some(policy), _, _) if policy > GENERATED_ID_POLICY_HILO_V1 => Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("table {table_id} uses a newer generated-ID policy"),
        )),
        (Some(policy), _, _) if policy < GENERATED_ID_POLICY_NONE => Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("table {table_id} has an invalid generated-ID policy code"),
        )),
        _ => Err(inconsistent_generated_id_policy(table_id)),
    }
}

fn inconsistent_generated_id_policy(table_id: u64) -> EngineError {
    EngineError::new(
        EngineErrorKind::DataCorruption,
        format!("table {table_id} has inconsistent generated-ID policy metadata"),
    )
}

fn positive_catalog_id(value: i64, entity: &str) -> EngineResult<u64> {
    let id = u64::try_from(value).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            format!("{entity} ID is outside the supported numeric range"),
            error,
        )
    })?;
    if id == 0 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("{entity} IDs must be positive"),
        ));
    }
    Ok(id)
}

fn decode_shard_key_type(code: i64, table_id: u64) -> EngineResult<ShardKeyType> {
    match code {
        INT64_SHARD_KEY_TYPE => Ok(ShardKeyType::Int64),
        TEXT_SHARD_KEY_TYPE => Ok(ShardKeyType::Text),
        BINARY_SHARD_KEY_TYPE => Ok(ShardKeyType::Binary),
        code => Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("table {table_id} has unsupported shard-key type code {code}"),
        )),
    }
}

fn validate_manifest_configuration(
    connection: &Connection,
    requested_shards: u16,
) -> EngineResult<u16> {
    let mut config_statement = connection
        .prepare("SELECT singleton, shard_count FROM briskdb_manifest ORDER BY singleton LIMIT 3")
        .map_err(|error| manifest_read_error(error, "failed to read manifest configuration"))?;
    let config = config_statement
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .map_err(|error| manifest_read_error(error, "failed to read manifest configuration"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| manifest_read_error(error, "failed to read manifest configuration"))?;
    if config.len() != 1 || config[0].0 != 1 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest configuration must contain exactly its singleton row",
        ));
    }
    let shard_count = u16::try_from(config[0].1).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "manifest shard count is outside the supported numeric range",
            error,
        )
    })?;
    validate_shard_range(shard_count)?;
    ensure_requested_shards(shard_count, requested_shards)?;

    Ok(shard_count)
}

fn validate_downgrade_fence(connection: &Connection, expected_version: u32) -> EngineResult<()> {
    let mut fence_statement = connection
        .prepare("SELECT requires_manifest_version FROM briskdb_metadata ORDER BY rowid LIMIT 3")
        .map_err(|error| manifest_read_error(error, "failed to read manifest downgrade fence"))?;
    let fence = fence_statement
        .query_map([], |row| row.get::<_, i64>(0))
        .map_err(|error| manifest_read_error(error, "failed to read manifest downgrade fence"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| manifest_read_error(error, "failed to read manifest downgrade fence"))?;
    if fence != [i64::from(expected_version)] {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest downgrade fence does not match its schema version",
        ));
    }
    Ok(())
}

fn validate_routing_configuration(connection: &Connection) -> EngineResult<RoutingConfiguration> {
    let mut statement = connection
        .prepare(
            "SELECT singleton,
                    hash_version,
                    key_encoding_version,
                    bucket_algorithm_version,
                    virtual_bucket_count,
                    map_generation
             FROM briskdb_routing
             ORDER BY singleton
             LIMIT 3",
        )
        .map_err(|error| manifest_read_error(error, "failed to read routing configuration"))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })
        .map_err(|error| manifest_read_error(error, "failed to read routing configuration"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| manifest_read_error(error, "failed to read routing configuration"))?;
    if rows.len() != 1 || rows[0].0 != 1 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "routing configuration must contain exactly its singleton row",
        ));
    }

    let (
        _,
        hash_version,
        key_encoding_version,
        bucket_algorithm_version,
        bucket_count,
        map_generation,
    ) = rows[0];
    if hash_version != i64::from(HASH_VERSION) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("manifest has unsupported hash version {hash_version}"),
        ));
    }
    if key_encoding_version != i64::from(KEY_ENCODING_VERSION) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("manifest has unsupported key-encoding version {key_encoding_version}"),
        ));
    }
    if bucket_algorithm_version != i64::from(BUCKET_ALGORITHM_VERSION) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("manifest has unsupported bucket-algorithm version {bucket_algorithm_version}"),
        ));
    }
    if bucket_count != i64::from(VIRTUAL_BUCKET_COUNT) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("manifest has unsupported virtual-bucket count {bucket_count}"),
        ));
    }
    let map_generation = u64::try_from(map_generation).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "manifest map generation is outside the supported numeric range",
            error,
        )
    })?;
    if map_generation != INITIAL_MAP_GENERATION {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("manifest has unsupported map generation {map_generation}"),
        ));
    }
    Ok(RoutingConfiguration {
        hash_version: HASH_VERSION,
        key_encoding_version: KEY_ENCODING_VERSION,
        bucket_algorithm_version: BUCKET_ALGORITHM_VERSION,
        map_generation,
    })
}

fn validate_physical_shards(connection: &Connection, shard_count: u16) -> EngineResult<()> {
    let mut statement = connection
        .prepare(
            "SELECT shard_id, lifecycle_state
             FROM briskdb_physical_shards
             ORDER BY shard_id
             LIMIT 65",
        )
        .map_err(|error| manifest_read_error(error, "failed to read physical shard catalog"))?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| manifest_read_error(error, "failed to read physical shard catalog"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| manifest_read_error(error, "failed to read physical shard catalog"))?;
    if rows.len() != usize::from(shard_count) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "physical shard catalog has {} rows, expected {shard_count}",
                rows.len()
            ),
        ));
    }
    for (expected, (stored, state)) in (0..shard_count).zip(rows) {
        if stored != i64::from(expected) {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "physical shard IDs must be contiguous from zero",
            ));
        }
        if state != ACTIVE_LIFECYCLE_STATE {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("physical shard {expected} has unsupported lifecycle state {state}"),
            ));
        }
    }
    Ok(())
}

fn validate_virtual_buckets(connection: &Connection, shard_count: u16) -> EngineResult<Box<[u16]>> {
    let mut statement = connection
        .prepare(
            "SELECT bucket_id, physical_shard_id
             FROM briskdb_virtual_buckets
             ORDER BY bucket_id
             LIMIT 4097",
        )
        .map_err(|error| manifest_read_error(error, "failed to read virtual bucket map"))?;
    let rows = statement
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .map_err(|error| manifest_read_error(error, "failed to read virtual bucket map"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| manifest_read_error(error, "failed to read virtual bucket map"))?;
    if rows.len() != usize::from(VIRTUAL_BUCKET_COUNT) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "virtual bucket map has {} rows, expected {VIRTUAL_BUCKET_COUNT}",
                rows.len()
            ),
        ));
    }

    let mut assignments = vec![0_u16; usize::from(shard_count)];
    let mut buckets = Vec::with_capacity(usize::from(VIRTUAL_BUCKET_COUNT));
    for (expected, (stored_bucket, stored_shard)) in (0..VIRTUAL_BUCKET_COUNT).zip(rows) {
        if stored_bucket != i64::from(expected) {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "virtual bucket IDs must be contiguous from zero",
            ));
        }
        let stored_shard = u16::try_from(stored_shard).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::DataCorruption,
                format!("virtual bucket {expected} has an invalid physical shard ID"),
                error,
            )
        })?;
        if stored_shard >= shard_count {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("virtual bucket {expected} references unknown shard {stored_shard}"),
            ));
        }
        if stored_shard != initial_physical_shard(expected, shard_count) {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("virtual bucket {expected} disagrees with the generation-1 map"),
            ));
        }
        assignments[usize::from(stored_shard)] += 1;
        buckets.push(stored_shard);
    }
    if let Some(unassigned) = assignments.iter().position(|count| *count == 0) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("active physical shard {unassigned} has no virtual buckets"),
        ));
    }
    Ok(buckets.into_boxed_slice())
}

fn validate_foreign_keys(connection: &Connection) -> EngineResult<()> {
    let violation = connection
        .query_row(
            "SELECT 1 FROM pragma_foreign_key_check LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(Some)
        .or_else(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            error => Err(error),
        })
        .map_err(|error| manifest_read_error(error, "failed to validate manifest foreign keys"))?;
    if violation.is_some() {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest contains a foreign-key violation",
        ));
    }
    Ok(())
}

fn validate_shard_range(shard_count: u16) -> EngineResult<()> {
    if !(2..=64).contains(&shard_count) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("manifest shard count {shard_count} is outside the supported range"),
        ));
    }
    Ok(())
}

fn ensure_requested_shards(stored: u16, requested: u16) -> EngineResult<()> {
    if stored != requested {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("database was created with {stored} shards, but {requested} were requested"),
        ));
    }
    Ok(())
}

fn manifest_read_error(error: rusqlite::Error, diagnostic: &'static str) -> EngineError {
    let classified = sqlite_error::storage(error);
    if matches!(
        classified.kind(),
        EngineErrorKind::Busy
            | EngineErrorKind::Cancelled
            | EngineErrorKind::PermissionDenied
            | EngineErrorKind::ReadOnly
            | EngineErrorKind::StorageFull
            | EngineErrorKind::OutOfMemory
            | EngineErrorKind::StorageUnavailable
    ) {
        classified.context(diagnostic)
    } else {
        EngineError::from_source(EngineErrorKind::DataCorruption, diagnostic, classified)
    }
}

#[cfg(test)]
pub(super) fn create_v4_fixture(connection: &mut Connection, shard_count: u16) {
    let transaction = connection
        .transaction()
        .expect("v4 fixture transaction starts");
    create_v4_schema(&transaction, shard_count).expect("v4 fixture schema is valid");
    set_identity(&transaction, V4_SCHEMA_VERSION).expect("v4 fixture identity is valid");
    transaction.commit().expect("v4 fixture commits");
}

#[cfg(test)]
pub(super) fn create_v5_fixture(connection: &mut Connection, shard_count: u16) {
    let transaction = connection
        .transaction()
        .expect("v5 fixture transaction starts");
    create_v5_schema(&transaction, shard_count).expect("v5 fixture schema is valid");
    transaction
        .execute(
            "UPDATE briskdb_shard_layout SET layout_state = ?1 WHERE singleton = 1",
            [ShardLayoutState::Ready.code()],
        )
        .expect("v5 fixture layout becomes ready");
    set_identity(&transaction, V5_SCHEMA_VERSION).expect("v5 fixture identity is valid");
    validate_v5(
        &transaction,
        shard_count,
        &schema_objects(&transaction).unwrap(),
    )
    .expect("v5 fixture validates");
    transaction.commit().expect("v5 fixture commits");
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{Arc, Barrier, mpsc},
        thread,
        time::Duration,
    };

    use rusqlite::OptionalExtension;

    use crate::core::generated_id::AllocationOwnerSlot;

    use super::*;

    type StoredTableMetadataRow = (i64, i64, String, i64, Option<String>, Option<i64>);
    type StoredGeneratedIdRow = (i64, i64, Option<String>, Option<i64>);

    const V8_PLAN: MigrationPlan<'static> = MigrationPlan {
        current_version: V8_SCHEMA_VERSION,
        migrations: MIGRATIONS,
        initialize_current: create_v8_schema,
        initialize_interrupted_legacy: migrate_interrupted_legacy_to_v8,
    };

    const V10_PLAN: MigrationPlan<'static> = MigrationPlan {
        current_version: V10_SCHEMA_VERSION,
        migrations: MIGRATIONS,
        initialize_current: create_v10_schema,
        initialize_interrupted_legacy: migrate_interrupted_legacy_to_v10,
    };

    fn create_legacy_manifest(connection: &Connection, shards: u16, version: u32) {
        create_empty_legacy_manifest(connection);
        connection
            .execute(
                "INSERT INTO briskdb_metadata (key, value) VALUES ('shard_count', ?1)",
                [shards.to_string()],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO briskdb_metadata (key, value) VALUES ('schema_version', '1')",
                [],
            )
            .unwrap();
        connection
            .pragma_update(None, "user_version", version)
            .unwrap();
    }

    fn create_empty_legacy_manifest(connection: &Connection) {
        connection
            .execute_batch(
                "CREATE TABLE briskdb_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );",
            )
            .unwrap();
    }

    fn create_v2_manifest(connection: &mut Connection, shards: u16) {
        let transaction = connection.transaction().unwrap();
        create_v2_schema(&transaction, shards).unwrap();
        set_identity(&transaction, V2_SCHEMA_VERSION).unwrap();
        transaction.commit().unwrap();
    }

    fn create_v3_manifest(connection: &mut Connection, shards: u16) {
        let transaction = connection.transaction().unwrap();
        create_v3_schema(&transaction, shards).unwrap();
        set_identity(&transaction, V3_SCHEMA_VERSION).unwrap();
        transaction.commit().unwrap();
    }

    fn create_v4_manifest(connection: &mut Connection, shards: u16) {
        create_v4_fixture(connection, shards);
    }

    fn create_v5_manifest(connection: &mut Connection, shards: u16) {
        create_v5_fixture(connection, shards);
    }

    fn create_ready_current_manifest(connection: &mut Connection, shards: u16) {
        let (_, creating) = load_or_create_manifest(connection, shards)
            .unwrap()
            .into_parts();
        if creating.state() != ShardLayoutState::Ready {
            mark_shard_layout_ready(connection, shards, &creating).unwrap();
        }
        seal_verified_schema(connection, shards, [0x5a; 32]).unwrap();
    }

    fn create_ready_v10_manifest(connection: &mut Connection, shards: u16) {
        let transaction = connection.transaction().unwrap();
        create_v10_schema(&transaction, shards).unwrap();
        transaction
            .execute(
                "UPDATE briskdb_shard_layout SET layout_state = ?1 WHERE singleton = 1",
                [ShardLayoutState::Ready.code()],
            )
            .unwrap();
        transaction
            .execute(
                "UPDATE briskdb_integrity
                 SET database_state = ?1,
                     committed_schema_digest = ?2,
                     target_schema_digest = NULL
                 WHERE singleton = 1",
                rusqlite::params![DATABASE_STATE_READY, [0x5a_u8; 32].as_slice()],
            )
            .unwrap();
        set_identity(&transaction, V10_SCHEMA_VERSION).unwrap();
        refresh_manifest_digest(&transaction).unwrap();
        validate_v10(&transaction, shards, &schema_objects(&transaction).unwrap()).unwrap();
        transaction.commit().unwrap();
    }

    fn activate_hilo_table(connection: &mut Connection, shards: u16) -> TableId {
        let database = crate::core::LogicalDatabaseId::new(1).unwrap();
        let declarations = vec![
            TableDeclaration::sharded(
                database,
                "events",
                ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
            )
            .unwrap()
            .with_generated_id_policy(GeneratedIdPolicy::hilo_v1("id").unwrap())
            .unwrap(),
        ];
        let active = match begin_native_table_provisioning(
            connection,
            shards,
            declarations,
            [0x5a; 32],
            || {},
        )
        .unwrap()
        {
            NativeTableProvisioningClassification::Active(active) => active,
            other => panic!("unexpected hi/lo provisioning classification: {other:?}"),
        };
        assert_eq!(active.next_shard(), shards);
        let catalog =
            finalize_native_table_provisioning(connection, shards, &active, || {}).unwrap();
        let table = catalog
            .logical()
            .table("default", "events")
            .unwrap()
            .unwrap();
        assert_eq!(catalog.active_hilo_id_table_ids(), [table.id()]);
        assert!(catalog.active_native_id_table_ids().is_empty());
        table.id()
    }

    fn create_ready_v7_manifest(connection: &mut Connection, shards: u16) {
        let transaction = connection.transaction().unwrap();
        create_v7_schema(&transaction, shards).unwrap();
        transaction
            .execute(
                "UPDATE briskdb_shard_layout
                 SET layout_state = ?1
                 WHERE singleton = 1",
                [ShardLayoutState::Ready.code()],
            )
            .unwrap();
        transaction
            .execute(
                "UPDATE briskdb_integrity
                 SET database_state = ?1,
                     committed_schema_digest = ?2,
                     target_schema_digest = NULL
                 WHERE singleton = 1",
                rusqlite::params![DATABASE_STATE_READY, [0x5a_u8; 32].as_slice()],
            )
            .unwrap();
        set_identity(&transaction, V7_SCHEMA_VERSION).unwrap();
        refresh_manifest_digest(&transaction).unwrap();
        validate_v7(&transaction, shards, &schema_objects(&transaction).unwrap()).unwrap();
        transaction.commit().unwrap();
    }

    fn create_ready_v8_manifest(connection: &mut Connection, shards: u16) {
        let transaction = connection.transaction().unwrap();
        create_v8_schema(&transaction, shards).unwrap();
        transaction
            .execute(
                "UPDATE briskdb_shard_layout
                 SET layout_state = ?1
                 WHERE singleton = 1",
                [ShardLayoutState::Ready.code()],
            )
            .unwrap();
        transaction
            .execute(
                "UPDATE briskdb_integrity
                 SET database_state = ?1,
                     committed_schema_digest = ?2,
                     target_schema_digest = NULL
                 WHERE singleton = 1",
                rusqlite::params![DATABASE_STATE_READY, [0x5a_u8; 32].as_slice()],
            )
            .unwrap();
        set_identity(&transaction, V8_SCHEMA_VERSION).unwrap();
        refresh_manifest_digest(&transaction).unwrap();
        validate_v8(&transaction, shards, &schema_objects(&transaction).unwrap()).unwrap();
        transaction.commit().unwrap();
    }

    fn complete_manifest_migration(
        connection: &mut Connection,
        shards: u16,
        expected_source: u64,
        sql: &str,
    ) -> SchemaMigration {
        let mut migration =
            begin_schema_migration(connection, shards, expected_source, sql).unwrap();
        while migration.next_shard() < migration.shard_count() {
            let next = migration.next_shard() + 1;
            migration = advance_schema_migration(connection, shards, &migration, next).unwrap();
        }
        finalize_schema_migration(connection, shards, &migration).unwrap()
    }

    fn shard_layout_row(connection: &Connection) -> (Vec<u8>, i64, i64, i64) {
        connection
            .query_row(
                "SELECT layout_id,
                        shard_application_id,
                        shard_metadata_version,
                        layout_state
                 FROM briskdb_shard_layout
                 WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap()
    }

    fn routing_configuration(connection: &Connection) -> (i64, i64, i64, i64, i64, i64) {
        connection
            .query_row(
                "SELECT singleton,
                        hash_version,
                        key_encoding_version,
                        bucket_algorithm_version,
                        virtual_bucket_count,
                        map_generation
                 FROM briskdb_routing",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .unwrap()
    }

    fn physical_shards(connection: &Connection) -> Vec<(i64, String)> {
        let mut statement = connection
            .prepare(
                "SELECT shard_id, lifecycle_state
                 FROM briskdb_physical_shards
                 ORDER BY shard_id",
            )
            .unwrap();
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn virtual_buckets(connection: &Connection) -> Vec<(i64, i64)> {
        let mut statement = connection
            .prepare(
                "SELECT bucket_id, physical_shard_id
                 FROM briskdb_virtual_buckets
                 ORDER BY bucket_id",
            )
            .unwrap();
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn schema_catalog_configuration(connection: &Connection) -> (i64, i64, i64, i64) {
        connection
            .query_row(
                "SELECT singleton,
                        identifier_encoding_version,
                        schema_generation,
                        default_database_id
                 FROM briskdb_schema_catalog",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap()
    }

    fn logical_databases(connection: &Connection) -> Vec<(i64, String)> {
        let mut statement = connection
            .prepare(
                "SELECT database_id, database_name
                 FROM briskdb_logical_databases
                 ORDER BY database_id",
            )
            .unwrap();
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn table_metadata_rows(connection: &Connection) -> Vec<StoredTableMetadataRow> {
        let mut statement = connection
            .prepare(
                "SELECT table_id,
                        database_id,
                        table_name,
                        placement,
                        shard_key_column,
                        shard_key_type
                 FROM briskdb_tables
                 ORDER BY database_id, table_name, table_id",
            )
            .unwrap();
        statement
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn generated_id_rows(connection: &Connection) -> Vec<StoredGeneratedIdRow> {
        let mut statement = connection
            .prepare(
                "SELECT table_id, policy, generated_column, encoding_version
                 FROM briskdb_generated_ids
                 ORDER BY table_id",
            )
            .unwrap();
        statement
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn allocation_owner_rows(connection: &Connection) -> Vec<(i64, i64)> {
        let mut statement = connection
            .prepare(
                "SELECT owner_slot, physical_shard_id
                 FROM briskdb_allocation_owners
                 ORDER BY owner_slot",
            )
            .unwrap();
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn stored_manifest_digest(connection: &Connection) -> [u8; 32] {
        let digest = connection
            .query_row(
                "SELECT manifest_digest FROM briskdb_integrity WHERE singleton = 1",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .unwrap();
        digest.try_into().unwrap()
    }

    fn insert_valid_table_catalog(connection: &Connection) {
        connection
            .execute_batch(
                "INSERT INTO briskdb_logical_databases VALUES (9, 'tenant');
                 INSERT INTO briskdb_tables VALUES (3, 1, 'accounts', 1, 'tenant_id', 2);
                 INSERT INTO briskdb_tables VALUES (8, 1, 'countries', 2, NULL, NULL);
                 INSERT INTO briskdb_tables VALUES (21, 9, 'audit_log', 3, NULL, NULL);
                 INSERT INTO briskdb_tables VALUES (34, 9, 'binary_keys', 1, 'key_bytes', 3);
                 INSERT INTO briskdb_tables VALUES (55, 9, 'counters', 1, 'counter_id', 1);",
            )
            .unwrap();
        let has_generated_ids = connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sqlite_schema
                     WHERE type = 'table' AND name = 'briskdb_generated_ids'
                 )",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap();
        if has_generated_ids {
            let has_activation = connection
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM pragma_table_xinfo('briskdb_generated_ids')
                        WHERE name = 'activation_state'
                     )",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap();
            if has_activation {
                connection
                    .execute(
                        "INSERT INTO briskdb_generated_ids (
                            table_id,
                            policy,
                            generated_column,
                            encoding_version,
                            activation_state
                         )
                         SELECT table_id, ?1, NULL, NULL, ?2
                         FROM briskdb_tables
                         ORDER BY table_id",
                        rusqlite::params![GENERATED_ID_POLICY_NONE, GENERATED_ID_INACTIVE],
                    )
                    .unwrap();
            } else {
                connection
                    .execute(
                        "INSERT INTO briskdb_generated_ids (
                            table_id,
                            policy,
                            generated_column,
                            encoding_version
                         )
                         SELECT table_id, ?1, NULL, NULL
                         FROM briskdb_tables
                         ORDER BY table_id",
                        [GENERATED_ID_POLICY_NONE],
                    )
                    .unwrap();
            }
        }
        refresh_manifest_digest_if_checksummed(connection).unwrap();
    }

    fn assert_generation_one_catalog(connection: &Connection, shard_count: u16) {
        assert_eq!(
            identity(connection),
            (MANIFEST_APPLICATION_ID, i64::from(CURRENT_SCHEMA_VERSION))
        );
        assert_eq!(schema_objects(connection).unwrap(), v12_objects());
        assert_eq!(
            connection
                .query_row(
                    "SELECT requires_manifest_version FROM briskdb_metadata",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            i64::from(CURRENT_SCHEMA_VERSION)
        );
        assert_eq!(
            routing_configuration(connection),
            (
                1,
                i64::from(HASH_VERSION),
                i64::from(KEY_ENCODING_VERSION),
                i64::from(BUCKET_ALGORITHM_VERSION),
                i64::from(VIRTUAL_BUCKET_COUNT),
                i64::try_from(INITIAL_MAP_GENERATION).unwrap(),
            )
        );
        assert_eq!(
            physical_shards(connection),
            (0..shard_count)
                .map(|shard| (i64::from(shard), ACTIVE_LIFECYCLE_STATE.to_owned()))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            allocation_owner_rows(connection),
            (0..shard_count)
                .map(|shard| (i64::from(shard), i64::from(shard)))
                .collect::<Vec<_>>()
        );

        let buckets = virtual_buckets(connection);
        assert_eq!(buckets.len(), usize::from(VIRTUAL_BUCKET_COUNT));
        let mut assignments = vec![0_u16; usize::from(shard_count)];
        for (expected, (bucket_id, physical_shard)) in (0..VIRTUAL_BUCKET_COUNT).zip(buckets) {
            assert_eq!(bucket_id, i64::from(expected));
            assert_eq!(
                physical_shard,
                i64::from(initial_physical_shard(expected, shard_count))
            );
            assignments[usize::try_from(physical_shard).unwrap()] += 1;
        }
        let smallest = assignments.iter().min().unwrap();
        let largest = assignments.iter().max().unwrap();
        assert!(*smallest > 0);
        assert!(*largest - *smallest <= 1);
        assert_eq!(schema_catalog_configuration(connection), (1, 1, 0, 1));
        assert_eq!(
            logical_databases(connection),
            [(1, DEFAULT_LOGICAL_DATABASE_NAME.to_owned())]
        );
        assert!(table_metadata_rows(connection).is_empty());
        assert!(generated_id_rows(connection).is_empty());
        assert_eq!(
            connection
                .query_row(
                    "SELECT manifest_digest_version
                     FROM briskdb_integrity
                     WHERE singleton = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            i64::from(V5_MANIFEST_DIGEST_VERSION)
        );
        let (layout_id, application_id, metadata_version, state) = shard_layout_row(connection);
        assert_eq!(layout_id.len(), 16);
        assert_eq!(application_id, SHARD_APPLICATION_ID);
        assert_eq!(metadata_version, i64::from(SHARD_METADATA_VERSION));
        assert!(matches!(state, 1..=3));
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM briskdb_schema_migrations",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        validate_foreign_keys(connection).unwrap();
    }

    fn identity(connection: &Connection) -> (i64, i64) {
        read_identity(connection).unwrap()
    }

    fn current_shard_count(connection: &Connection) -> i64 {
        connection
            .query_row("SELECT shard_count FROM briskdb_manifest", [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    fn quick_check(connection: &Connection) -> String {
        connection
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .unwrap()
    }

    fn legacy_open(connection: &Connection, requested_shards: u16) -> rusqlite::Result<()> {
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS briskdb_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );",
        )?;
        let stored: Option<String> = connection
            .query_row(
                "SELECT value FROM briskdb_metadata WHERE key = 'shard_count'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if stored.as_deref() == Some(&requested_shards.to_string()) {
            Ok(())
        } else {
            Err(rusqlite::Error::InvalidQuery)
        }
    }

    #[test]
    fn migration_registry_is_contiguous_and_reaches_current_version() {
        let mut version = LEGACY_SCHEMA_VERSION;
        for migration in MIGRATIONS {
            assert_eq!(migration.from, version);
            assert!(migration.to > migration.from);
            assert!(!migration.name.is_empty());
            version = migration.to;
        }
        assert_eq!(version, CURRENT_SCHEMA_VERSION);
    }

    #[test]
    fn chained_migrations_commit_each_step_and_resume_at_the_last_version() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_legacy_manifest(&connection, 4, 0);
        let mut first_attempt = Vec::new();
        let error = load_or_create_with_hook(&mut connection, 4, |point| {
            if point.phase == MigrationPhase::AfterVersionStamp {
                first_attempt.push((point.from, point.to));
                if point.from == V2_SCHEMA_VERSION {
                    return Err(EngineError::new(
                        EngineErrorKind::Internal,
                        "injected catalog migration failure",
                    ));
                }
            }
            Ok(())
        })
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert_eq!(first_attempt, [(1, 2), (2, 3)]);
        assert_eq!(identity(&connection), (MANIFEST_APPLICATION_ID, 2));
        assert_eq!(schema_objects(&connection).unwrap(), v2_objects());
        assert_eq!(quick_check(&connection), "ok");

        let mut resumed_steps = Vec::new();
        assert_eq!(
            load_or_create_with_hook(&mut connection, 4, |point| {
                if point.phase == MigrationPhase::AfterVersionStamp {
                    resumed_steps.push((point.from, point.to));
                }
                Ok(())
            })
            .unwrap(),
            4
        );
        assert_eq!(
            resumed_steps,
            [
                (2, 3),
                (3, 4),
                (4, 5),
                (5, 6),
                (6, 7),
                (7, 8),
                (8, 9),
                (9, 10),
                (10, 11),
                (11, 12),
            ]
        );
        assert_generation_one_catalog(&connection, 4);
        assert_eq!(quick_check(&connection), "ok");
    }

    #[test]
    fn creates_and_idempotently_reopens_the_current_manifest() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let mut connection = Connection::open(&path).unwrap();

        assert_eq!(load_or_create(&mut connection, 4).unwrap(), 4);
        assert_eq!(current_shard_count(&connection), 4);
        assert_generation_one_catalog(&connection, 4);
        assert_eq!(quick_check(&connection), "ok");

        let observer = Connection::open(&path).unwrap();
        let before: i64 = observer
            .pragma_query_value(None, "data_version", |row| row.get(0))
            .unwrap();
        assert_eq!(load_or_create(&mut connection, 4).unwrap(), 4);
        let after: i64 = observer
            .pragma_query_value(None, "data_version", |row| row.get(0))
            .unwrap();
        assert_eq!(before, after, "a current-version reopen must not write");
    }

    #[test]
    fn v10_to_v11_is_atomic_retryable_and_fences_v10_readers() {
        for failing_phase in [
            MigrationPhase::AfterSchemaChange,
            MigrationPhase::AfterVersionStamp,
        ] {
            for inject_panic in [false, true] {
                let mut connection = Connection::open_in_memory().unwrap();
                create_ready_v10_manifest(&mut connection, 4);
                let objects_before = schema_objects(&connection).unwrap();
                let root_before = stored_manifest_digest(&connection);

                let attempt = catch_unwind(AssertUnwindSafe(|| {
                    load_or_create_with_hook(&mut connection, 4, |point| {
                        if point.from == V10_SCHEMA_VERSION && point.phase == failing_phase {
                            if inject_panic {
                                panic!("injected v10 to v11 migration panic");
                            }
                            return Err(EngineError::new(
                                EngineErrorKind::Internal,
                                "injected v10 to v11 migration failure",
                            ));
                        }
                        Ok(())
                    })
                }));
                if inject_panic {
                    assert!(attempt.is_err());
                } else {
                    assert_eq!(
                        attempt.unwrap().unwrap_err().kind(),
                        EngineErrorKind::Internal
                    );
                }
                assert_eq!(identity(&connection).1, i64::from(V10_SCHEMA_VERSION));
                assert_eq!(schema_objects(&connection).unwrap(), objects_before);
                assert_eq!(stored_manifest_digest(&connection), root_before);
                assert_eq!(manifest_semantic_digest(&connection).unwrap(), root_before);
                assert_eq!(quick_check(&connection), "ok");

                load_or_create_manifest(&mut connection, 4).unwrap();
                assert_eq!(identity(&connection).1, i64::from(CURRENT_SCHEMA_VERSION));
                assert_eq!(schema_objects(&connection).unwrap(), v12_objects());
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT manifest_digest_version FROM briskdb_integrity",
                            [],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    i64::from(V5_MANIFEST_DIGEST_VERSION)
                );
                assert_eq!(
                    connection
                        .query_row("SELECT COUNT(*) FROM briskdb_hilo_leases", [], |row| {
                            row.get::<_, i64>(0)
                        })
                        .unwrap(),
                    0
                );
                let identity_before = identity(&connection);
                let root_before = stored_manifest_digest(&connection);
                assert_eq!(
                    inspect_with_plan(&connection, 4, V10_PLAN)
                        .unwrap_err()
                        .kind(),
                    EngineErrorKind::FailedPrecondition
                );
                assert_eq!(identity(&connection), identity_before);
                assert_eq!(stored_manifest_digest(&connection), root_before);
            }
        }
    }

    #[test]
    fn hilo_activation_atomically_installs_one_global_lease_head() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let table_id = activate_hilo_table(&mut connection, 4);
        assert_eq!(
            connection
                .query_row(
                    "SELECT table_id, block_size, next_sequence, fence_token,
                            last_owner_id, last_first_sequence, last_last_sequence
                     FROM briskdb_hilo_leases",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, Option<Vec<u8>>>(4)?,
                            row.get::<_, Option<i64>>(5)?,
                            row.get::<_, Option<i64>>(6)?,
                        ))
                    },
                )
                .unwrap(),
            (
                i64::try_from(table_id.get()).unwrap(),
                i64::try_from(HILO_V1_BLOCK_SIZE).unwrap(),
                1,
                0,
                None,
                None,
                None,
            )
        );
        assert_eq!(
            manifest_semantic_digest(&connection).unwrap(),
            stored_manifest_digest(&connection)
        );
    }

    #[test]
    fn hilo_lease_state_is_clock_independent_and_has_no_expiry_fields() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let table_id = activate_hilo_table(&mut connection, 4);

        let columns = connection
            .prepare("SELECT name FROM pragma_table_xinfo('briskdb_hilo_leases') ORDER BY cid")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            columns,
            [
                "table_id",
                "block_size",
                "next_sequence",
                "fence_token",
                "last_owner_id",
                "last_first_sequence",
                "last_last_sequence",
            ]
        );
        assert!(columns.iter().all(|column| {
            let column = column.to_ascii_lowercase();
            !column.contains("time")
                && !column.contains("clock")
                && !column.contains("expire")
                && !column.contains("ttl")
        }));

        let first = reserve_hilo_v1_block(&mut connection, 4, table_id, [0x71; 32]).unwrap();
        thread::sleep(Duration::from_millis(2));
        let second = reserve_hilo_v1_block(&mut connection, 4, table_id, [0x71; 32]).unwrap();
        assert_eq!(
            first,
            DurableHiloLease::new(table_id, [0x71; 32], 1, 1, 4096)
        );
        assert_eq!(
            second,
            DurableHiloLease::new(table_id, [0x71; 32], 2, 4097, 8192)
        );
    }

    #[test]
    fn hilo_reservations_are_durable_fenced_and_part_of_the_semantic_root() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let table_id = activate_hilo_table(&mut connection, 4);
        let initial_root = stored_manifest_digest(&connection);
        let first_owner = [0x11; 32];
        let second_owner = [0x22; 32];

        let first = reserve_hilo_v1_block(&mut connection, 4, table_id, first_owner).unwrap();
        assert_eq!(
            first,
            DurableHiloLease::new(table_id, first_owner, 1, 1, HILO_V1_BLOCK_SIZE)
        );
        let first_root = stored_manifest_digest(&connection);
        assert_ne!(first_root, initial_root);
        let second = reserve_hilo_v1_block(&mut connection, 4, table_id, second_owner).unwrap();
        assert_eq!(
            second,
            DurableHiloLease::new(
                table_id,
                second_owner,
                2,
                HILO_V1_BLOCK_SIZE + 1,
                HILO_V1_BLOCK_SIZE * 2,
            )
        );
        assert_ne!(stored_manifest_digest(&connection), first_root);
        assert_eq!(
            manifest_semantic_digest(&connection).unwrap(),
            stored_manifest_digest(&connection)
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT next_sequence, fence_token, last_owner_id,
                            last_first_sequence, last_last_sequence
                     FROM briskdb_hilo_leases WHERE table_id = ?1",
                    [i64::try_from(table_id.get()).unwrap()],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, i64>(4)?,
                        ))
                    },
                )
                .unwrap(),
            (8193, 2, second_owner.to_vec(), 4097, 8192)
        );
    }

    #[test]
    fn hilo_reservation_returns_a_partial_final_block_then_reports_both_limits() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let table_id = activate_hilo_table(&mut connection, 4);
        let owner = [0x33; 32];
        let final_first = MAX_HILO_V1_SEQUENCE - 2;
        let previous_first = final_first - HILO_V1_BLOCK_SIZE;
        connection
            .execute(
                "UPDATE briskdb_hilo_leases
                 SET next_sequence = ?1, fence_token = 1, last_owner_id = ?2,
                     last_first_sequence = ?3, last_last_sequence = ?4
                 WHERE table_id = ?5",
                rusqlite::params![
                    i64::try_from(final_first).unwrap(),
                    [0x22_u8; 32].as_slice(),
                    i64::try_from(previous_first).unwrap(),
                    i64::try_from(final_first - 1).unwrap(),
                    i64::try_from(table_id.get()).unwrap(),
                ],
            )
            .unwrap();
        refresh_manifest_digest(&connection).unwrap();
        assert_eq!(
            reserve_hilo_v1_block(&mut connection, 4, table_id, owner).unwrap(),
            DurableHiloLease::new(table_id, owner, 2, final_first, MAX_HILO_V1_SEQUENCE)
        );
        assert_eq!(
            reserve_hilo_v1_block(&mut connection, 4, table_id, owner)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );

        connection
            .execute(
                "UPDATE briskdb_hilo_leases
                 SET next_sequence = 8193, fence_token = ?1, last_owner_id = ?2,
                     last_first_sequence = 4097, last_last_sequence = 8192
                 WHERE table_id = ?3",
                rusqlite::params![
                    i64::MAX,
                    owner.as_slice(),
                    i64::try_from(table_id.get()).unwrap()
                ],
            )
            .unwrap();
        refresh_manifest_digest(&connection).unwrap();
        assert_eq!(
            reserve_hilo_v1_block(&mut connection, 4, table_id, owner)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
    }

    #[test]
    fn hilo_lease_tampering_and_resealed_cardinality_loss_are_detected() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let table_id = activate_hilo_table(&mut connection, 4);
        reserve_hilo_v1_block(&mut connection, 4, table_id, [0x44; 32]).unwrap();
        connection
            .execute(
                "UPDATE briskdb_hilo_leases SET last_owner_id = ?1 WHERE table_id = ?2",
                rusqlite::params![
                    [0x45_u8; 32].as_slice(),
                    i64::try_from(table_id.get()).unwrap()
                ],
            )
            .unwrap();
        assert_eq!(
            current_manifest_snapshot(&connection, 4)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
        refresh_manifest_digest(&connection).unwrap();
        connection
            .execute(
                "DELETE FROM briskdb_hilo_leases WHERE table_id = ?1",
                [i64::try_from(table_id.get()).unwrap()],
            )
            .unwrap();
        refresh_manifest_digest(&connection).unwrap();
        assert_eq!(
            current_manifest_snapshot(&connection, 4)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn independent_connections_serialize_hilo_reservations_without_overlap() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let mut setup = Connection::open(&path).unwrap();
        setup.busy_timeout(Duration::from_secs(5)).unwrap();
        create_ready_current_manifest(&mut setup, 4);
        let table_id = activate_hilo_table(&mut setup, 4);
        drop(setup);

        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for owner in [[0x51_u8; 32], [0x52_u8; 32]] {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            workers.push(thread::spawn(move || {
                let mut connection = Connection::open(path).unwrap();
                connection.busy_timeout(Duration::from_secs(5)).unwrap();
                barrier.wait();
                reserve_hilo_v1_block(&mut connection, 4, table_id, owner).unwrap()
            }));
        }
        barrier.wait();
        let leases = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        let owner_51_first = DurableHiloLease::new(table_id, [0x51; 32], 1, 1, 4096);
        let owner_51_second = DurableHiloLease::new(table_id, [0x51; 32], 2, 4097, 8192);
        let owner_52_first = DurableHiloLease::new(table_id, [0x52; 32], 1, 1, 4096);
        let owner_52_second = DurableHiloLease::new(table_id, [0x52; 32], 2, 4097, 8192);
        assert!(
            (leases.contains(&owner_51_first) && leases.contains(&owner_52_second))
                || (leases.contains(&owner_52_first) && leases.contains(&owner_51_second))
        );
        let mut observer = Connection::open(&path).unwrap();
        let third = reserve_hilo_v1_block(&mut observer, 4, table_id, [0x53; 32]).unwrap();
        assert_eq!(
            third,
            DurableHiloLease::new(table_id, [0x53; 32], 3, 8193, 12288)
        );
    }

    #[test]
    fn manifest_write_lock_contention_is_busy_and_retryable() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let mut setup = Connection::open(&path).unwrap();
        create_ready_current_manifest(&mut setup, 4);
        let table_id = activate_hilo_table(&mut setup, 4);
        drop(setup);

        let mut holder = Connection::open(&path).unwrap();
        let held = holder
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let mut contender = Connection::open(&path).unwrap();
        contender.busy_timeout(Duration::ZERO).unwrap();
        assert_eq!(
            reserve_hilo_v1_block(&mut contender, 4, table_id, [0x61; 32])
                .unwrap_err()
                .kind(),
            EngineErrorKind::Busy
        );
        held.rollback().unwrap();
        let lease = reserve_hilo_v1_block(&mut contender, 4, table_id, [0x61; 32]).unwrap();
        assert_eq!(
            lease,
            DurableHiloLease::new(table_id, [0x61; 32], 1, 1, 4096)
        );
    }

    #[test]
    fn active_provisioning_journal_survives_degradation_and_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let mut connection = Connection::open(&path).unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let database = crate::core::LogicalDatabaseId::new(1).unwrap();
        let declarations = vec![
            TableDeclaration::sharded(
                database,
                "events",
                ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
            )
            .unwrap()
            .with_generated_id_policy(GeneratedIdPolicy::hilo_v1("id").unwrap())
            .unwrap(),
        ];
        let active = match begin_native_table_provisioning(
            &mut connection,
            4,
            declarations,
            [0x5a; 32],
            || {},
        )
        .unwrap()
        {
            NativeTableProvisioningClassification::Active(active) => active,
            other => panic!("unexpected provisioning classification: {other:?}"),
        };
        let layout = current_manifest_snapshot(&connection, 4)
            .unwrap()
            .shard_layout
            .unwrap();
        mark_degraded(&mut connection, 4, &layout).unwrap();
        assert_eq!(
            current_integrity(&connection, 4).unwrap().state(),
            DatabaseIntegrityState::Degraded
        );
        drop(connection);

        let mut reopened = Connection::open(&path).unwrap();
        let loaded = load_or_create_manifest(&mut reopened, 4).unwrap();
        let (_, _, _, integrity, _, _, provisioning, generated_ddl) =
            loaded.into_parts_with_recovery();
        assert_eq!(integrity.state(), DatabaseIntegrityState::Degraded);
        assert_eq!(provisioning, Some(active));
        assert_eq!(generated_ddl, None);
        assert_eq!(
            manifest_semantic_digest(&reopened).unwrap(),
            stored_manifest_digest(&reopened)
        );
    }

    #[test]
    fn v8_upgrade_clears_advisory_rows_and_fences_out_v7_readers() {
        const V7_PLAN: MigrationPlan<'static> = MigrationPlan {
            current_version: V7_SCHEMA_VERSION,
            migrations: MIGRATIONS,
            initialize_current: create_v7_schema,
            initialize_interrupted_legacy: migrate_interrupted_legacy_to_v7,
        };

        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_v7_manifest(&mut connection, 4);
        insert_valid_table_catalog(&connection);
        assert_eq!(table_metadata_rows(&connection).len(), 5);
        assert_eq!(logical_databases(&connection).len(), 2);

        let mut no_hook = |_| Ok(());
        let snapshot =
            load_or_create_snapshot_with_plan(&mut connection, 4, V8_PLAN, true, &mut no_hook)
                .unwrap();
        let loaded = catalog_snapshot_from_manifest(snapshot).unwrap();
        assert!(loaded.logical().tables().is_empty());
        assert_eq!(loaded.logical().logical_databases().len(), 2);
        assert_eq!(
            identity(&connection),
            (MANIFEST_APPLICATION_ID, i64::from(V8_SCHEMA_VERSION))
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT requires_manifest_version FROM briskdb_metadata",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            i64::from(V8_SCHEMA_VERSION)
        );
        let root = stored_manifest_digest(&connection);
        assert_eq!(manifest_semantic_digest(&connection).unwrap(), root);

        let identity_before = identity(&connection);
        let error = inspect_with_plan(&connection, 4, V7_PLAN).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&connection), identity_before);
        assert_eq!(stored_manifest_digest(&connection), root);
    }

    #[test]
    fn v7_to_v8_upgrade_errors_and_panics_restore_the_exact_v7_catalog() {
        for failing_phase in [
            MigrationPhase::AfterSchemaChange,
            MigrationPhase::AfterVersionStamp,
        ] {
            for inject_panic in [false, true] {
                let mut connection = Connection::open_in_memory().unwrap();
                create_ready_v7_manifest(&mut connection, 4);
                insert_valid_table_catalog(&connection);
                let original_root = stored_manifest_digest(&connection);
                let original_tables = table_metadata_rows(&connection);
                let original_databases = logical_databases(&connection);
                let original_objects = schema_objects(&connection).unwrap();

                let attempt = catch_unwind(AssertUnwindSafe(|| {
                    let mut hook = |point: MigrationPoint| {
                        if point.from == V7_SCHEMA_VERSION && point.phase == failing_phase {
                            if inject_panic {
                                panic!("injected v7 to v8 migration panic");
                            }
                            return Err(EngineError::new(
                                EngineErrorKind::Internal,
                                "injected v7 to v8 migration failure",
                            ));
                        }
                        Ok(())
                    };
                    load_or_create_snapshot_with_plan(&mut connection, 4, V8_PLAN, true, &mut hook)
                }));
                if inject_panic {
                    assert!(attempt.is_err());
                } else {
                    assert_eq!(
                        attempt.unwrap().unwrap_err().kind(),
                        EngineErrorKind::Internal
                    );
                }

                assert_eq!(
                    identity(&connection),
                    (MANIFEST_APPLICATION_ID, i64::from(V7_SCHEMA_VERSION))
                );
                assert_eq!(schema_objects(&connection).unwrap(), original_objects);
                assert_eq!(table_metadata_rows(&connection), original_tables);
                assert_eq!(logical_databases(&connection), original_databases);
                assert_eq!(stored_manifest_digest(&connection), original_root);
                assert_eq!(
                    manifest_semantic_digest(&connection).unwrap(),
                    original_root
                );
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT requires_manifest_version FROM briskdb_metadata",
                            [],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    i64::from(V7_SCHEMA_VERSION)
                );

                let mut no_hook = |_| Ok(());
                let snapshot = load_or_create_snapshot_with_plan(
                    &mut connection,
                    4,
                    V8_PLAN,
                    true,
                    &mut no_hook,
                )
                .unwrap();
                let loaded = catalog_snapshot_from_manifest(snapshot).unwrap();
                assert!(loaded.logical().tables().is_empty());
                assert_eq!(
                    identity(&connection),
                    (MANIFEST_APPLICATION_ID, i64::from(V8_SCHEMA_VERSION))
                );
            }
        }
    }

    #[test]
    fn v8_to_current_persists_explicit_none_policies_and_owner_slots() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_v8_manifest(&mut connection, 4);
        insert_valid_table_catalog(&connection);
        let tables_before = table_metadata_rows(&connection);
        let databases_before = logical_databases(&connection);
        let routing_before = routing_configuration(&connection);
        let physical_shards_before = physical_shards(&connection);
        let virtual_buckets_before = virtual_buckets(&connection);
        let layout_before = shard_layout_row(&connection);
        assert_eq!(
            connection
                .query_row(
                    "SELECT manifest_digest_version
                     FROM briskdb_integrity
                     WHERE singleton = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            i64::from(V1_MANIFEST_DIGEST_VERSION)
        );

        let loaded = load_or_create_catalog(&mut connection, 4).unwrap();

        assert_eq!(
            identity(&connection),
            (MANIFEST_APPLICATION_ID, i64::from(CURRENT_SCHEMA_VERSION))
        );
        assert_eq!(schema_objects(&connection).unwrap(), v12_objects());
        assert_eq!(table_metadata_rows(&connection), tables_before);
        assert_eq!(logical_databases(&connection), databases_before);
        assert_eq!(routing_configuration(&connection), routing_before);
        assert_eq!(physical_shards(&connection), physical_shards_before);
        assert_eq!(virtual_buckets(&connection), virtual_buckets_before);
        assert_eq!(shard_layout_row(&connection), layout_before);
        assert_eq!(
            generated_id_rows(&connection),
            tables_before
                .iter()
                .map(|row| (row.0, GENERATED_ID_POLICY_NONE, None, None))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            allocation_owner_rows(&connection),
            [(0, 0), (1, 1), (2, 2), (3, 3)]
        );
        assert_eq!(
            loaded
                .allocation_owners()
                .unwrap()
                .pairs()
                .collect::<Vec<_>>(),
            [(0, 0), (1, 1), (2, 2), (3, 3)]
        );
        assert!(
            loaded
                .logical()
                .tables()
                .iter()
                .all(|table| table.generated_id_policy() == &GeneratedIdPolicy::None)
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT manifest_digest_version
                     FROM briskdb_integrity
                     WHERE singleton = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            i64::from(V5_MANIFEST_DIGEST_VERSION)
        );
        assert_eq!(
            manifest_semantic_digest(&connection).unwrap(),
            stored_manifest_digest(&connection)
        );

        let identity_before = identity(&connection);
        let root_before = stored_manifest_digest(&connection);
        assert_eq!(
            inspect_with_plan(&connection, 4, V8_PLAN)
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(identity(&connection), identity_before);
        assert_eq!(stored_manifest_digest(&connection), root_before);
    }

    #[test]
    fn v9_native_policy_migrates_inactive_without_losing_catalog_metadata() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let database = crate::core::LogicalDatabaseId::new(1).unwrap();
        let declarations = vec![
            TableDeclaration::sharded(
                database,
                "events",
                ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
            )
            .unwrap()
            .with_generated_id_policy(GeneratedIdPolicy::native_range_v1("id").unwrap())
            .unwrap(),
        ];
        let ready_digest = [0x5a; 32];
        let active = match begin_native_table_provisioning(
            &mut connection,
            4,
            declarations.clone(),
            ready_digest,
            || {},
        )
        .unwrap()
        {
            NativeTableProvisioningClassification::Active(active) => active,
            other => panic!("unexpected provisioning classification: {other:?}"),
        };
        let mut progress = active;
        for next in 1..=4 {
            progress =
                advance_native_table_provisioning(&mut connection, 4, &progress, next).unwrap();
        }
        finalize_native_table_provisioning(&mut connection, 4, &progress, || {}).unwrap();

        connection
            .execute(
                "UPDATE briskdb_generated_ids SET activation_state = 0 WHERE table_id = 1",
                [],
            )
            .unwrap();
        connection
            .execute_batch(
                "DROP TABLE briskdb_generated_table_ddl;
                 DROP TABLE briskdb_hilo_leases;
                 DROP TABLE briskdb_table_provisioning_declarations;
                 DROP TABLE briskdb_table_provisioning;
                 DROP INDEX briskdb_one_active_owner_per_shard;
                 ALTER TABLE briskdb_generated_ids RENAME TO briskdb_generated_ids_v10;
                 ALTER TABLE briskdb_allocation_owners RENAME TO briskdb_allocation_owners_v10;",
            )
            .unwrap();
        connection
            .execute_batch(V9_GENERATED_IDS_TABLE_SQL)
            .unwrap();
        connection
            .execute(
                "INSERT INTO briskdb_generated_ids
                 SELECT table_id, policy, generated_column, encoding_version
                 FROM briskdb_generated_ids_v10",
                [],
            )
            .unwrap();
        connection
            .execute_batch(V9_ALLOCATION_OWNERS_TABLE_SQL)
            .unwrap();
        connection
            .execute(
                "INSERT INTO briskdb_allocation_owners
                 SELECT owner_slot, physical_shard_id
                 FROM briskdb_allocation_owners_v10",
                [],
            )
            .unwrap();
        connection
            .execute_batch(
                "DROP TABLE briskdb_generated_ids_v10;
                 DROP TABLE briskdb_allocation_owners_v10;
                 DROP TABLE briskdb_metadata;",
            )
            .unwrap();
        connection.execute_batch(V9_DOWNGRADE_FENCE_SQL).unwrap();
        connection
            .execute("INSERT INTO briskdb_metadata VALUES (9)", [])
            .unwrap();
        connection
            .execute(
                "UPDATE briskdb_integrity SET manifest_digest_version = 2",
                [],
            )
            .unwrap();
        set_identity(&connection, V9_SCHEMA_VERSION).unwrap();
        refresh_manifest_digest(&connection).unwrap();

        let loaded = load_or_create_manifest(&mut connection, 4).unwrap();
        let table = loaded
            .catalog
            .logical()
            .table("default", "events")
            .unwrap()
            .unwrap();
        assert_eq!(
            table.generated_id_policy(),
            &GeneratedIdPolicy::native_range_v1("id").unwrap()
        );
        assert!(loaded.catalog.active_native_id_table_ids().is_empty());
        assert_eq!(identity(&connection).1, i64::from(CURRENT_SCHEMA_VERSION));
    }

    #[test]
    fn table_provisioning_journal_is_exact_monotonic_and_atomic() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let database = crate::core::LogicalDatabaseId::new(1).unwrap();
        let declarations = vec![
            TableDeclaration::global(database, "countries").unwrap(),
            TableDeclaration::sharded(
                database,
                "events",
                ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
            )
            .unwrap()
            .with_generated_id_policy(GeneratedIdPolicy::native_range_v1("id").unwrap())
            .unwrap(),
        ];
        let schema_digest = [0x5a; 32];

        let active = match begin_native_table_provisioning(
            &mut connection,
            4,
            declarations.clone(),
            schema_digest,
            || {},
        )
        .unwrap()
        {
            NativeTableProvisioningClassification::Active(active) => active,
            other => panic!("unexpected provisioning classification: {other:?}"),
        };
        assert_eq!(active.next_shard(), 0);
        assert_eq!(active.declarations(), declarations.as_slice());
        assert_eq!(active.committed_schema_digest(), schema_digest);
        assert_eq!(
            active.provisioning_id(),
            [
                0x2a, 0x5e, 0xc9, 0x09, 0x53, 0x3f, 0xa2, 0x27, 0x8e, 0xaa, 0x36, 0xee, 0xf0, 0x1a,
                0x24, 0xba, 0xc5, 0xe9, 0xf2, 0x25, 0x54, 0x9c, 0xf6, 0xf4, 0xa8, 0x9b, 0x3d, 0xfa,
                0xe7, 0x57, 0xda, 0x2c,
            ]
        );
        assert_eq!(
            begin_native_table_provisioning(
                &mut connection,
                4,
                declarations.clone(),
                schema_digest,
                || {},
            )
            .unwrap(),
            NativeTableProvisioningClassification::Active(active.clone())
        );
        let conflict = vec![
            TableDeclaration::sharded(
                database,
                "other_events",
                ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
            )
            .unwrap()
            .with_generated_id_policy(GeneratedIdPolicy::native_range_v1("id").unwrap())
            .unwrap(),
        ];
        assert_eq!(
            begin_native_table_provisioning(&mut connection, 4, conflict, schema_digest, || {})
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(
            advance_native_table_provisioning(&mut connection, 4, &active, 2)
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
        assert_eq!(
            finalize_native_table_provisioning(&mut connection, 4, &active, || {})
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );

        let mut progress = active;
        for next in 1..=4 {
            progress =
                advance_native_table_provisioning(&mut connection, 4, &progress, next).unwrap();
            assert_eq!(progress.next_shard(), next);
        }
        let mut commit_attempted = false;
        let catalog = finalize_native_table_provisioning(&mut connection, 4, &progress, || {
            commit_attempted = true;
        })
        .unwrap();
        assert!(commit_attempted);
        assert_eq!(catalog.logical().tables().len(), 2);
        assert_eq!(catalog.active_native_id_table_ids().len(), 1);
        assert!(
            load_or_create_manifest(&mut connection, 4)
                .unwrap()
                .active_table_provisioning()
                .is_none()
        );
        assert_eq!(
            classify_native_table_provisioning(
                &mut connection,
                4,
                declarations.clone(),
                schema_digest,
            )
            .unwrap(),
            NativeTableProvisioningClassification::Complete
        );
        assert_eq!(
            finalize_native_table_provisioning(&mut connection, 4, &progress, || {}).unwrap(),
            catalog
        );
    }

    #[test]
    fn retired_owners_route_history_while_replacement_owners_allocate() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        connection
            .execute(
                "UPDATE briskdb_allocation_owners
                 SET owner_state = ?1
                 WHERE owner_slot = 0",
                [ALLOCATION_OWNER_RETIRED],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO briskdb_allocation_owners (
                    owner_slot, physical_shard_id, owner_state
                 ) VALUES (100, 0, ?1)",
                [ALLOCATION_OWNER_ACTIVE],
            )
            .unwrap();
        refresh_manifest_digest(&connection).unwrap();
        let catalog = load_or_create_catalog(&mut connection, 4).unwrap();
        let owners = catalog.allocation_owners().unwrap();
        assert_eq!(
            owners.physical_shard(AllocationOwnerSlot::from_validated(0)),
            Some(0)
        );
        assert_eq!(
            owners.owner_for_physical_shard(0),
            Some(AllocationOwnerSlot::from_validated(100))
        );
        assert_eq!(
            owners
                .assignments()
                .filter(|(_, shard, _)| *shard == 0)
                .collect::<Vec<_>>(),
            [
                (0, 0, AllocationOwnerState::Retired),
                (100, 0, AllocationOwnerState::Active),
            ]
        );
    }

    #[cfg(feature = "experimental-vtab")]
    #[test]
    fn allocation_owner_lifecycle_rejects_a_lower_successor_atomically() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        replace_allocation_owner_for_test(&mut connection, 4, 0, 100, 0).unwrap();
        let root_before = stored_manifest_digest(&connection);
        let owners_before = allocation_owner_rows(&connection);

        let error = replace_allocation_owner_for_test(&mut connection, 4, 100, 50, 0).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
        assert!(error.diagnostic().contains("must be greater"));

        assert_eq!(stored_manifest_digest(&connection), root_before);
        assert_eq!(allocation_owner_rows(&connection), owners_before);
        let owners = load_or_create_catalog(&mut connection, 4)
            .unwrap()
            .allocation_owners()
            .unwrap()
            .clone();
        assert_eq!(
            owners.owner_for_physical_shard(0),
            Some(AllocationOwnerSlot::from_validated(100))
        );
        assert!(owners.owner_is_active(AllocationOwnerSlot::from_validated(100)));
    }

    #[test]
    fn v8_to_v9_failures_and_panics_roll_back_exactly_and_retry() {
        for failing_phase in [
            MigrationPhase::AfterSchemaChange,
            MigrationPhase::AfterVersionStamp,
        ] {
            for inject_panic in [false, true] {
                let mut connection = Connection::open_in_memory().unwrap();
                create_ready_v8_manifest(&mut connection, 4);
                insert_valid_table_catalog(&connection);
                let root_before = stored_manifest_digest(&connection);
                let tables_before = table_metadata_rows(&connection);
                let objects_before = schema_objects(&connection).unwrap();

                let attempt = catch_unwind(AssertUnwindSafe(|| {
                    load_or_create_with_hook(&mut connection, 4, |point| {
                        if point.from == V8_SCHEMA_VERSION && point.phase == failing_phase {
                            if inject_panic {
                                panic!("injected v8 to v9 migration panic");
                            }
                            return Err(EngineError::new(
                                EngineErrorKind::Internal,
                                "injected v8 to v9 migration failure",
                            ));
                        }
                        Ok(())
                    })
                }));
                if inject_panic {
                    assert!(attempt.is_err());
                } else {
                    assert_eq!(
                        attempt.unwrap().unwrap_err().kind(),
                        EngineErrorKind::Internal
                    );
                }

                assert_eq!(
                    identity(&connection),
                    (MANIFEST_APPLICATION_ID, i64::from(V8_SCHEMA_VERSION))
                );
                assert_eq!(schema_objects(&connection).unwrap(), objects_before);
                assert_eq!(table_metadata_rows(&connection), tables_before);
                assert_eq!(stored_manifest_digest(&connection), root_before);
                assert_eq!(manifest_semantic_digest(&connection).unwrap(), root_before);
                assert_eq!(quick_check(&connection), "ok");

                let loaded = load_or_create_catalog(&mut connection, 4).unwrap();
                assert!(
                    loaded
                        .logical()
                        .tables()
                        .iter()
                        .all(|table| table.generated_id_policy() == &GeneratedIdPolicy::None)
                );
                assert_eq!(
                    allocation_owner_rows(&connection),
                    [(0, 0), (1, 1), (2, 2), (3, 3)]
                );
                assert_eq!(generated_id_rows(&connection).len(), tables_before.len());
            }
        }
    }

    #[test]
    fn v9_to_v10_failures_and_panics_roll_back_exactly_and_retry() {
        for failing_phase in [
            MigrationPhase::AfterSchemaChange,
            MigrationPhase::AfterVersionStamp,
        ] {
            for inject_panic in [false, true] {
                let mut connection = Connection::open_in_memory().unwrap();
                create_ready_current_manifest(&mut connection, 4);
                let database = crate::core::LogicalDatabaseId::new(1).unwrap();
                let declarations = vec![
                    TableDeclaration::sharded(
                        database,
                        "events",
                        ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
                    )
                    .unwrap()
                    .with_generated_id_policy(GeneratedIdPolicy::native_range_v1("id").unwrap())
                    .unwrap(),
                ];
                install_v9_native_catalog_for_test(&mut connection, 4, &declarations).unwrap();

                let root_before = stored_manifest_digest(&connection);
                let databases_before = logical_databases(&connection);
                let tables_before = table_metadata_rows(&connection);
                let generated_ids_before = generated_id_rows(&connection);
                let owners_before = allocation_owner_rows(&connection);
                let objects_before = schema_objects(&connection).unwrap();
                let fence_before = connection
                    .query_row(
                        "SELECT requires_manifest_version FROM briskdb_metadata",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap();
                let digest_version_before = connection
                    .query_row(
                        "SELECT manifest_digest_version
                         FROM briskdb_integrity
                         WHERE singleton = 1",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap();

                let attempt = catch_unwind(AssertUnwindSafe(|| {
                    load_or_create_with_hook(&mut connection, 4, |point| {
                        if point.from == V9_SCHEMA_VERSION && point.phase == failing_phase {
                            if inject_panic {
                                panic!("injected v9 to v10 migration panic");
                            }
                            return Err(EngineError::new(
                                EngineErrorKind::Internal,
                                "injected v9 to v10 migration failure",
                            ));
                        }
                        Ok(())
                    })
                }));
                if inject_panic {
                    assert!(attempt.is_err());
                } else {
                    assert_eq!(
                        attempt.unwrap().unwrap_err().kind(),
                        EngineErrorKind::Internal
                    );
                }

                assert_eq!(
                    identity(&connection),
                    (MANIFEST_APPLICATION_ID, i64::from(V9_SCHEMA_VERSION))
                );
                assert_eq!(schema_objects(&connection).unwrap(), objects_before);
                assert_eq!(logical_databases(&connection), databases_before);
                assert_eq!(table_metadata_rows(&connection), tables_before);
                assert_eq!(generated_id_rows(&connection), generated_ids_before);
                assert_eq!(allocation_owner_rows(&connection), owners_before);
                assert_eq!(stored_manifest_digest(&connection), root_before);
                assert_eq!(manifest_semantic_digest(&connection).unwrap(), root_before);
                assert_eq!(quick_check(&connection), "ok");
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT requires_manifest_version FROM briskdb_metadata",
                            [],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    fence_before
                );
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT manifest_digest_version
                             FROM briskdb_integrity
                             WHERE singleton = 1",
                            [],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    digest_version_before
                );

                let loaded = load_or_create_catalog(&mut connection, 4).unwrap();
                let table = loaded
                    .logical()
                    .table("default", "events")
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    table.generated_id_policy(),
                    &GeneratedIdPolicy::native_range_v1("id").unwrap()
                );
                assert!(loaded.active_native_id_table_ids().is_empty());
                assert_eq!(generated_id_rows(&connection), generated_ids_before);
                assert_eq!(allocation_owner_rows(&connection), owners_before);
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT COUNT(*)
                             FROM briskdb_generated_ids
                             WHERE activation_state != ?1",
                            [GENERATED_ID_INACTIVE],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    0
                );
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT COUNT(*)
                             FROM briskdb_allocation_owners
                             WHERE owner_state != ?1",
                            [ALLOCATION_OWNER_ACTIVE],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    0
                );
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT (SELECT COUNT(*) FROM briskdb_table_provisioning) +
                                    (SELECT COUNT(*) FROM briskdb_table_provisioning_declarations)",
                            [],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    0
                );
                assert_eq!(
                    identity(&connection),
                    (MANIFEST_APPLICATION_ID, i64::from(CURRENT_SCHEMA_VERSION))
                );
                assert_eq!(schema_objects(&connection).unwrap(), v12_objects());
                assert_eq!(
                    manifest_semantic_digest(&connection).unwrap(),
                    stored_manifest_digest(&connection)
                );
            }
        }
    }

    #[test]
    fn authoritative_catalog_registration_is_atomic_exact_and_idempotent() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let database = crate::core::LogicalDatabaseId::new(1).unwrap();
        let declarations = vec![
            TableDeclaration::catalog(database, "internal_catalog").unwrap(),
            TableDeclaration::global(database, "countries").unwrap(),
            TableDeclaration::sharded(
                database,
                "events",
                ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
            )
            .unwrap()
            .with_generated_id_policy(GeneratedIdPolicy::native_range_v1("id").unwrap())
            .unwrap(),
            TableDeclaration::sharded(
                database,
                "accounts",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ];

        let mut commit_attempted = false;
        let registered = register_table_catalog(&mut connection, 4, declarations.clone(), || {
            commit_attempted = true
        })
        .unwrap();
        assert!(commit_attempted);
        assert_eq!(
            registered
                .logical()
                .tables()
                .iter()
                .map(|table| (table.id().get(), table.name()))
                .collect::<Vec<_>>(),
            [
                (1, "accounts"),
                (2, "countries"),
                (3, "events"),
                (4, "internal_catalog")
            ]
        );
        assert_eq!(
            table_metadata_rows(&connection),
            [
                (
                    1,
                    1,
                    "accounts".to_owned(),
                    SHARDED_PLACEMENT,
                    Some("tenant_id".to_owned()),
                    Some(TEXT_SHARD_KEY_TYPE),
                ),
                (2, 1, "countries".to_owned(), GLOBAL_PLACEMENT, None, None,),
                (
                    3,
                    1,
                    "events".to_owned(),
                    SHARDED_PLACEMENT,
                    Some("id".to_owned()),
                    Some(INT64_SHARD_KEY_TYPE),
                ),
                (
                    4,
                    1,
                    "internal_catalog".to_owned(),
                    CATALOG_PLACEMENT,
                    None,
                    None,
                ),
            ]
        );
        assert_eq!(
            generated_id_rows(&connection),
            [
                (1, GENERATED_ID_POLICY_NONE, None, None),
                (2, GENERATED_ID_POLICY_NONE, None, None),
                (
                    3,
                    GENERATED_ID_POLICY_NATIVE_RANGE_V1,
                    Some("id".to_owned()),
                    Some(i64::from(NATIVE_RANGE_V1_ENCODING_VERSION)),
                ),
                (4, GENERATED_ID_POLICY_NONE, None, None),
            ]
        );
        assert_eq!(
            registered
                .logical()
                .table("default", "events")
                .unwrap()
                .unwrap()
                .generated_id_policy(),
            &GeneratedIdPolicy::native_range_v1("id").unwrap()
        );
        let registered_root = stored_manifest_digest(&connection);
        assert_eq!(
            manifest_semantic_digest(&connection).unwrap(),
            registered_root
        );

        let mut repeated_commit = false;
        let repeated = register_table_catalog(&mut connection, 4, declarations.clone(), || {
            repeated_commit = true
        })
        .unwrap();
        assert!(!repeated_commit);
        assert_eq!(repeated, registered);
        assert_eq!(stored_manifest_digest(&connection), registered_root);

        let policy_conflict = vec![
            TableDeclaration::sharded(
                database,
                "accounts",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
            TableDeclaration::global(database, "countries").unwrap(),
            TableDeclaration::sharded(
                database,
                "events",
                ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
            )
            .unwrap(),
            TableDeclaration::catalog(database, "internal_catalog").unwrap(),
        ];
        let error = register_table_catalog(&mut connection, 4, policy_conflict, || {}).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(stored_manifest_digest(&connection), registered_root);

        let conflict = vec![
            TableDeclaration::global(database, "accounts").unwrap(),
            TableDeclaration::global(database, "countries").unwrap(),
            TableDeclaration::catalog(database, "internal_catalog").unwrap(),
        ];
        let mut conflicting_commit = false;
        let error = register_table_catalog(&mut connection, 4, conflict, || {
            conflicting_commit = true;
        })
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(!conflicting_commit);
        assert_eq!(stored_manifest_digest(&connection), registered_root);
        assert_eq!(
            manifest_semantic_digest(&connection).unwrap(),
            registered_root
        );
    }

    #[test]
    fn registration_commit_boundary_rolls_back_before_sqlite_commit() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let original_root = stored_manifest_digest(&connection);
        let database = crate::core::LogicalDatabaseId::new(1).unwrap();
        let declaration = TableDeclaration::global(database, "countries").unwrap();

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = register_table_catalog(&mut connection, 4, vec![declaration], || {
                panic!("injected interruption immediately before COMMIT");
            });
        }));
        assert!(panic.is_err());
        assert!(table_metadata_rows(&connection).is_empty());
        assert_eq!(stored_manifest_digest(&connection), original_root);
        assert_eq!(
            manifest_semantic_digest(&connection).unwrap(),
            original_root
        );
        assert!(
            load_or_create_catalog(&mut connection, 4)
                .unwrap()
                .logical()
                .tables()
                .is_empty()
        );
    }

    #[test]
    fn current_integrity_root_is_deterministic_across_reopen_checkpoint_and_vacuum() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let mut connection = Connection::open(&path).unwrap();
        create_ready_current_manifest(&mut connection, 4);
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        let expected = stored_manifest_digest(&connection);
        assert_eq!(manifest_semantic_digest(&connection).unwrap(), expected);

        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); VACUUM;")
            .unwrap();
        assert_eq!(manifest_semantic_digest(&connection).unwrap(), expected);
        drop(connection);

        let mut reopened = Connection::open(&path).unwrap();
        let loaded = load_or_create_manifest(&mut reopened, 4).unwrap();
        assert_eq!(loaded.integrity.state(), DatabaseIntegrityState::Ready);
        assert_eq!(stored_manifest_digest(&reopened), expected);
        assert_eq!(manifest_semantic_digest(&reopened).unwrap(), expected);
    }

    #[test]
    fn manifest_semantic_digest_v1_has_a_frozen_golden_vector() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_v8_manifest(&mut connection, 4);
        connection
            .execute(
                "UPDATE briskdb_shard_layout
                 SET layout_id = x'000102030405060708090a0b0c0d0e0f'
                 WHERE singleton = 1",
                [],
            )
            .unwrap();
        insert_valid_table_catalog(&connection);
        assert_eq!(
            connection
                .query_row(
                    "SELECT manifest_digest_version
                     FROM briskdb_integrity
                     WHERE singleton = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            i64::from(V1_MANIFEST_DIGEST_VERSION)
        );
        let digest = refresh_manifest_digest(&connection).unwrap();
        assert_eq!(
            digest,
            [
                0x7b, 0xe1, 0x4b, 0x4f, 0x0a, 0xf4, 0xd0, 0x41, 0x79, 0x9b, 0xe8, 0xd2, 0x19, 0xe5,
                0x5a, 0xdd, 0x13, 0x38, 0x29, 0x62, 0x3c, 0x2b, 0xef, 0xce, 0xfb, 0x5c, 0x4d, 0xc9,
                0xe9, 0xff, 0x5c, 0xe0,
            ]
        );
        assert_eq!(manifest_semantic_digest(&connection).unwrap(), digest);
    }

    #[test]
    fn manifest_semantic_digest_v3_orders_catalog_rows_by_frozen_keys() {
        let mut forward = Connection::open_in_memory().unwrap();
        let mut reverse = Connection::open_in_memory().unwrap();
        for connection in [&mut forward, &mut reverse] {
            create_ready_current_manifest(connection, 4);
            connection
                .execute(
                    "UPDATE briskdb_shard_layout
                     SET layout_id = x'000102030405060708090a0b0c0d0e0f'
                     WHERE singleton = 1",
                    [],
                )
                .unwrap();
        }
        insert_valid_table_catalog(&forward);
        reverse
            .execute_batch(
                "INSERT INTO briskdb_logical_databases VALUES (9, 'tenant');
                 INSERT INTO briskdb_tables VALUES (55, 9, 'counters', 1, 'counter_id', 1);
                 INSERT INTO briskdb_tables VALUES (34, 9, 'binary_keys', 1, 'key_bytes', 3);
                 INSERT INTO briskdb_tables VALUES (21, 9, 'audit_log', 3, NULL, NULL);
                 INSERT INTO briskdb_tables VALUES (8, 1, 'countries', 2, NULL, NULL);
                 INSERT INTO briskdb_tables VALUES (3, 1, 'accounts', 1, 'tenant_id', 2);
                 INSERT INTO briskdb_generated_ids VALUES (55, 0, NULL, NULL, 0);
                 INSERT INTO briskdb_generated_ids VALUES (34, 0, NULL, NULL, 0);
                 INSERT INTO briskdb_generated_ids VALUES (21, 0, NULL, NULL, 0);
                 INSERT INTO briskdb_generated_ids VALUES (8, 0, NULL, NULL, 0);
                 INSERT INTO briskdb_generated_ids VALUES (3, 0, NULL, NULL, 0);",
            )
            .unwrap();

        let digest = refresh_manifest_digest(&forward).unwrap();
        assert_eq!(digest, refresh_manifest_digest(&reverse).unwrap());
    }

    #[test]
    fn manifest_semantic_digest_v3_has_a_frozen_golden_vector() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        connection
            .execute(
                "UPDATE briskdb_shard_layout
                 SET layout_id = x'000102030405060708090a0b0c0d0e0f'
                 WHERE singleton = 1",
                [],
            )
            .unwrap();
        refresh_manifest_digest(&connection).unwrap();
        let database = crate::core::LogicalDatabaseId::new(1).unwrap();
        let declarations = vec![
            TableDeclaration::global(database, "countries").unwrap(),
            TableDeclaration::sharded(
                database,
                "events",
                ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
            )
            .unwrap()
            .with_generated_id_policy(GeneratedIdPolicy::native_range_v1("id").unwrap())
            .unwrap(),
        ];
        let active =
            begin_native_table_provisioning(&mut connection, 4, declarations, [0x5a; 32], || {})
                .unwrap();
        assert!(matches!(
            active,
            NativeTableProvisioningClassification::Active(_)
        ));
        downgrade_v11_manifest_to_v10_for_test(&connection, 4).unwrap();

        let digest = manifest_semantic_digest(&connection).unwrap();
        assert_eq!(
            digest,
            [
                0x14, 0xd3, 0xd7, 0x26, 0x2d, 0x98, 0x5b, 0x0a, 0x6d, 0xe3, 0x57, 0x23, 0xe8, 0xa6,
                0x21, 0xec, 0x49, 0xf9, 0x81, 0x52, 0xc4, 0xa7, 0xa8, 0xaf, 0xcf, 0xee, 0x50, 0xb9,
                0x98, 0x34, 0xd1, 0x5c,
            ]
        );
        assert_eq!(stored_manifest_digest(&connection), digest);
    }

    #[test]
    fn manifest_semantic_digest_v4_has_a_frozen_hilo_lease_vector() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        connection
            .execute(
                "UPDATE briskdb_shard_layout
                 SET layout_id = x'000102030405060708090a0b0c0d0e0f'
                 WHERE singleton = 1",
                [],
            )
            .unwrap();
        refresh_manifest_digest(&connection).unwrap();
        let table_id = activate_hilo_table(&mut connection, 4);
        reserve_hilo_v1_block(&mut connection, 4, table_id, [0x6b; 32]).unwrap();
        downgrade_v12_manifest_to_v11_for_test(&connection, 4).unwrap();

        let digest = manifest_semantic_digest(&connection).unwrap();
        assert_eq!(
            digest,
            [
                0x5e, 0x6d, 0x41, 0xf6, 0x02, 0xcf, 0xaf, 0x77, 0x41, 0x4a, 0x90, 0x59, 0x5a, 0x7e,
                0xa0, 0x37, 0x8e, 0x4e, 0x83, 0x07, 0x07, 0x06, 0xd6, 0x86, 0xed, 0x1f, 0x2b, 0xa7,
                0xed, 0x67, 0x5d, 0xe6,
            ]
        );
        assert_eq!(stored_manifest_digest(&connection), digest);
    }

    #[test]
    fn semantic_root_covers_every_authoritative_manifest_table_and_integrity_state() {
        let mutations = [
            "UPDATE briskdb_manifest SET singleton = 2 WHERE singleton = 1",
            "UPDATE briskdb_metadata SET requires_manifest_version = 13",
            "UPDATE briskdb_routing SET hash_version = 2 WHERE singleton = 1",
            "UPDATE briskdb_physical_shards SET lifecycle_state = 'retired' WHERE shard_id = 0",
            "UPDATE briskdb_allocation_owners SET owner_slot = 100 WHERE owner_slot = 0",
            "UPDATE briskdb_virtual_buckets SET physical_shard_id = 1 WHERE bucket_id = 0",
            "UPDATE briskdb_logical_databases SET database_name = 'primary' WHERE database_id = 1",
            "UPDATE briskdb_schema_catalog SET identifier_encoding_version = 2 WHERE singleton = 1",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'widgets', 2, NULL, NULL)",
            "INSERT INTO briskdb_generated_ids VALUES (1, 0, NULL, NULL, 0)",
            "INSERT INTO briskdb_generated_table_ddl VALUES (1, zeroblob(32), 1, 1, 1, 'x', zeroblob(32), 'x', 1, 'events', 'id', 1, 1, 1, NULL, NULL, NULL)",
            "INSERT INTO briskdb_hilo_leases VALUES (1, 4096, 1, 0, NULL, NULL, NULL)",
            "UPDATE briskdb_shard_layout SET layout_id = randomblob(16) WHERE singleton = 1",
            "INSERT INTO briskdb_schema_migrations VALUES (1, 0, randomblob(32), 1, 'SELECT 1', 4, 2, 4)",
            "INSERT INTO briskdb_table_provisioning VALUES (1, zeroblob(32), 1, 1, zeroblob(32), 4, 1, 0)",
            "INSERT INTO briskdb_table_provisioning_declarations VALUES (1, 0, 1, 'events', 1, 'id', 1, 1, 'id', 1)",
            "UPDATE briskdb_integrity SET database_state = 4 WHERE singleton = 1",
            "UPDATE briskdb_integrity SET committed_schema_digest = randomblob(32) WHERE singleton = 1",
        ];

        for mutation in mutations {
            let mut connection = Connection::open_in_memory().unwrap();
            create_ready_current_manifest(&mut connection, 4);
            let trusted = stored_manifest_digest(&connection);
            connection
                .execute_batch(
                    "PRAGMA foreign_keys = OFF;
                     PRAGMA ignore_check_constraints = ON;",
                )
                .unwrap();
            connection.execute_batch(mutation).unwrap();
            connection
                .execute_batch("PRAGMA ignore_check_constraints = OFF;")
                .unwrap();

            assert_ne!(
                manifest_semantic_digest(&connection).unwrap(),
                trusted,
                "{mutation}"
            );
            assert_eq!(stored_manifest_digest(&connection), trusted, "{mutation}");
            assert_eq!(
                load_or_create_manifest(&mut connection, 4)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::DataCorruption,
                "{mutation}"
            );
            assert_eq!(stored_manifest_digest(&connection), trusted, "{mutation}");
        }
    }

    #[test]
    fn generated_id_future_fields_require_a_valid_v2_root_before_compatibility_errors() {
        for (mutation, expected_diagnostic) in [
            (
                "UPDATE briskdb_generated_ids
                 SET policy = 3, generated_column = 'tenant_id', encoding_version = 1
                 WHERE table_id = 3",
                "table 3 uses a newer generated-ID policy",
            ),
            (
                "UPDATE briskdb_generated_ids
                 SET policy = 1, generated_column = 'tenant_id', encoding_version = 2
                 WHERE table_id = 3",
                "table 3 uses a newer generated-ID encoding version",
            ),
        ] {
            let mut unsealed = Connection::open_in_memory().unwrap();
            create_ready_current_manifest(&mut unsealed, 4);
            insert_valid_table_catalog(&unsealed);
            unsealed.execute_batch(mutation).unwrap();
            let error = load_or_create_manifest(&mut unsealed, 4).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption, "{mutation}");
            assert_eq!(
                error.diagnostic(),
                "manifest semantic checksum does not match its authoritative contents",
                "{mutation}"
            );

            let mut sealed = Connection::open_in_memory().unwrap();
            create_ready_current_manifest(&mut sealed, 4);
            insert_valid_table_catalog(&sealed);
            sealed.execute_batch(mutation).unwrap();
            refresh_manifest_digest(&sealed).unwrap();
            let error = load_or_create_manifest(&mut sealed, 4).unwrap_err();
            assert_eq!(
                error.kind(),
                EngineErrorKind::FailedPrecondition,
                "{mutation}"
            );
            assert_eq!(error.diagnostic(), expected_diagnostic, "{mutation}");
        }
    }

    #[test]
    fn resealed_generated_id_relational_tampering_fails_closed() {
        for mutation in [
            "DELETE FROM briskdb_generated_ids WHERE table_id = 3",
            "INSERT INTO briskdb_generated_ids VALUES (999, 0, NULL, NULL, 0)",
            "UPDATE briskdb_generated_ids
             SET policy = 1, generated_column = 'id', encoding_version = 1
             WHERE table_id = 8",
            "UPDATE briskdb_generated_ids
             SET policy = 1, generated_column = 'tenant_id', encoding_version = 1
             WHERE table_id = 3",
            "UPDATE briskdb_generated_ids
             SET policy = 1, generated_column = 'wrong_id', encoding_version = 1
             WHERE table_id = 55",
            "UPDATE briskdb_generated_ids
             SET policy = 1, generated_column = NULL, encoding_version = 1
             WHERE table_id = 55",
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            create_ready_current_manifest(&mut connection, 4);
            insert_valid_table_catalog(&connection);
            connection
                .execute_batch(
                    "PRAGMA foreign_keys = OFF;
                     PRAGMA ignore_check_constraints = ON;",
                )
                .unwrap();
            connection.execute_batch(mutation).unwrap();
            connection
                .execute_batch("PRAGMA ignore_check_constraints = OFF;")
                .unwrap();
            refresh_manifest_digest(&connection).unwrap();

            let error = load_or_create_manifest(&mut connection, 4).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption, "{mutation}");
        }
    }

    #[test]
    fn resealed_allocation_owner_relational_tampering_fails_closed() {
        for mutation in [
            "DELETE FROM briskdb_allocation_owners WHERE owner_slot = 0",
            "UPDATE briskdb_allocation_owners SET owner_state = 2 WHERE owner_slot = 0",
            "UPDATE briskdb_allocation_owners SET physical_shard_id = 63 WHERE owner_slot = 0",
            "UPDATE briskdb_allocation_owners
             SET owner_slot = 100, owner_state = 2
             WHERE owner_slot = 0;
             INSERT INTO briskdb_allocation_owners (
                 owner_slot, physical_shard_id, owner_state
             ) VALUES (50, 0, 1)",
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            create_ready_current_manifest(&mut connection, 4);
            connection
                .execute_batch(
                    "PRAGMA foreign_keys = OFF;
                     PRAGMA ignore_check_constraints = ON;",
                )
                .unwrap();
            connection.execute_batch(mutation).unwrap();
            connection
                .execute_batch("PRAGMA ignore_check_constraints = OFF;")
                .unwrap();
            refresh_manifest_digest(&connection).unwrap();

            let error = load_or_create_manifest(&mut connection, 4).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption, "{mutation}");
        }
    }

    #[test]
    fn integrity_versions_lengths_and_forged_state_invariants_fail_closed() {
        for (version_column, unsupported_version) in
            [("manifest_digest_version", 6), ("schema_digest_version", 2)]
        {
            let mut unsupported = Connection::open_in_memory().unwrap();
            create_ready_current_manifest(&mut unsupported, 4);
            unsupported
                .execute(
                    &format!(
                        "UPDATE briskdb_integrity
                         SET {version_column} = {unsupported_version}
                         WHERE singleton = 1"
                    ),
                    [],
                )
                .unwrap();
            if version_column == "schema_digest_version" {
                refresh_manifest_digest(&unsupported).unwrap();
            }
            assert_eq!(
                load_or_create_manifest(&mut unsupported, 4)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::FailedPrecondition,
                "{version_column}"
            );

            let mut invalid = Connection::open_in_memory().unwrap();
            create_ready_current_manifest(&mut invalid, 4);
            invalid
                .pragma_update(None, "ignore_check_constraints", "ON")
                .unwrap();
            invalid
                .execute(
                    &format!(
                        "UPDATE briskdb_integrity SET {version_column} = 0 WHERE singleton = 1"
                    ),
                    [],
                )
                .unwrap();
            invalid
                .pragma_update(None, "ignore_check_constraints", "OFF")
                .unwrap();
            assert_eq!(
                load_or_create_manifest(&mut invalid, 4).unwrap_err().kind(),
                EngineErrorKind::DataCorruption,
                "{version_column}"
            );
        }

        let mut malformed = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut malformed, 4);
        malformed
            .pragma_update(None, "ignore_check_constraints", "ON")
            .unwrap();
        malformed
            .execute(
                "UPDATE briskdb_integrity SET committed_schema_digest = x'01' WHERE singleton = 1",
                [],
            )
            .unwrap();
        malformed
            .pragma_update(None, "ignore_check_constraints", "OFF")
            .unwrap();
        assert_eq!(
            load_or_create_manifest(&mut malformed, 4)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );

        let mut forged = Connection::open_in_memory().unwrap();
        load_or_create_manifest(&mut forged, 4).unwrap();
        forged
            .pragma_update(None, "ignore_check_constraints", "ON")
            .unwrap();
        forged
            .execute(
                "UPDATE briskdb_integrity
                 SET database_state = 2, committed_schema_digest = NULL
                 WHERE singleton = 1",
                [],
            )
            .unwrap();
        refresh_manifest_digest(&forged).unwrap();
        forged
            .pragma_update(None, "ignore_check_constraints", "OFF")
            .unwrap();
        assert_eq!(
            load_or_create_manifest(&mut forged, 4).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn migration_and_degraded_transitions_reseal_without_rebaselining() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let ready_root = stored_manifest_digest(&connection);

        let mut migration =
            begin_schema_migration(&mut connection, 4, 0, "CREATE TABLE widgets(id INTEGER)")
                .unwrap();
        let journal_root = stored_manifest_digest(&connection);
        assert_ne!(journal_root, ready_root);
        assert_eq!(
            current_integrity(&connection, 4).unwrap().state(),
            DatabaseIntegrityState::Migrating
        );

        migration = advance_schema_migration(&mut connection, 4, &migration, 1).unwrap();
        let progress_root = stored_manifest_digest(&connection);
        assert_ne!(progress_root, journal_root);
        while migration.next_shard() < migration.shard_count() {
            let next = migration.next_shard() + 1;
            migration = advance_schema_migration(&mut connection, 4, &migration, next).unwrap();
        }
        finalize_schema_migration(&mut connection, 4, &migration).unwrap();
        let completed_root = stored_manifest_digest(&connection);
        assert_ne!(completed_root, progress_root);
        assert_eq!(
            manifest_semantic_digest(&connection).unwrap(),
            completed_root
        );
        assert_eq!(
            current_integrity(&connection, 4).unwrap().state(),
            DatabaseIntegrityState::Ready
        );

        let layout = current_manifest_snapshot(&connection, 4)
            .unwrap()
            .shard_layout
            .unwrap();
        mark_degraded(&mut connection, 4, &layout).unwrap();
        let degraded = current_integrity(&connection, 4).unwrap();
        assert_eq!(degraded.state(), DatabaseIntegrityState::Degraded);
        assert_eq!(
            seal_verified_schema(&mut connection, 4, [0x11; 32])
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
        assert_eq!(
            current_integrity(&connection, 4).unwrap().state(),
            DatabaseIntegrityState::Degraded
        );
        assert_eq!(
            seal_verified_schema(
                &mut connection,
                4,
                degraded.committed_schema_digest().unwrap(),
            )
            .unwrap_err()
            .kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn fresh_manifest_persists_one_layout_identity_and_ready_transition() {
        let mut connection = Connection::open_in_memory().unwrap();
        let (_, creating) = load_or_create_manifest(&mut connection, 4)
            .unwrap()
            .into_parts();

        assert_eq!(creating.state(), ShardLayoutState::Creating);
        assert_eq!(creating.expected_application_id(), SHARD_APPLICATION_ID);
        assert_eq!(creating.metadata_version(), SHARD_METADATA_VERSION);

        let (_, reopened) = load_or_create_manifest(&mut connection, 4)
            .unwrap()
            .into_parts();
        assert_eq!(reopened, creating);

        mark_shard_layout_ready(&mut connection, 4, &creating).unwrap();
        let (_, ready) = load_or_create_manifest(&mut connection, 4)
            .unwrap()
            .into_parts();
        assert_eq!(ready.layout_id(), creating.layout_id());
        assert_eq!(ready.state(), ShardLayoutState::Ready);

        mark_shard_layout_ready(&mut connection, 4, &ready).unwrap();
        assert_eq!(
            shard_layout_row(&connection).3,
            ShardLayoutState::Ready.code()
        );
    }

    #[test]
    fn lagging_reconciler_rereads_ready_instead_of_using_stale_creating_state() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let mut first = Connection::open(&path).unwrap();
        let (_, first_creating) = load_or_create_manifest(&mut first, 4).unwrap().into_parts();
        let mut lagging = Connection::open(&path).unwrap();
        let (_, stale_creating) = load_or_create_manifest(&mut lagging, 4)
            .unwrap()
            .into_parts();
        assert_eq!(first_creating, stale_creating);

        let ready = reconcile_shard_layout(&mut first, 4, &first_creating, |_| Ok(())).unwrap();
        assert_eq!(ready.state(), ShardLayoutState::Ready);

        let mut state_under_lock = None;
        let observed = reconcile_shard_layout(&mut lagging, 4, &stale_creating, |locked| {
            state_under_lock = Some(locked.state());
            Ok(())
        })
        .unwrap();
        assert_eq!(state_under_lock, Some(ShardLayoutState::Ready));
        assert_eq!(observed, ready);
    }

    #[test]
    fn version_four_upgrade_enters_adopting_and_clears_advisory_catalog_rows() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_v4_manifest(&mut connection, 4);
        insert_valid_table_catalog(&connection);

        let (catalog, layout) = load_or_create_manifest(&mut connection, 4)
            .unwrap()
            .into_parts();

        assert_eq!(layout.state(), ShardLayoutState::Adopting);
        assert_eq!(catalog.logical().logical_databases().len(), 2);
        assert!(catalog.logical().tables().is_empty());
        assert_eq!(
            identity(&connection),
            (MANIFEST_APPLICATION_ID, i64::from(CURRENT_SCHEMA_VERSION))
        );
        assert_eq!(schema_objects(&connection).unwrap(), v12_objects());
        assert_eq!(
            shard_layout_row(&connection).3,
            ShardLayoutState::Adopting.code()
        );
        assert_eq!(quick_check(&connection), "ok");
    }

    #[test]
    fn version_five_upgrade_preserves_layout_and_clears_advisory_catalog_rows() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_v5_manifest(&mut connection, 4);
        insert_valid_table_catalog(&connection);
        let layout_before = shard_layout_row(&connection);
        let databases_before = logical_databases(&connection);

        let loaded = load_or_create_manifest(&mut connection, 4).unwrap();
        assert!(loaded.active_migration().is_none());
        let (catalog, layout, active, integrity) = loaded.into_parts_with_migration();

        assert_eq!(
            identity(&connection),
            (MANIFEST_APPLICATION_ID, i64::from(CURRENT_SCHEMA_VERSION))
        );
        assert_eq!(schema_objects(&connection).unwrap(), v12_objects());
        assert_eq!(layout.state(), ShardLayoutState::Ready);
        assert_eq!(shard_layout_row(&connection), layout_before);
        assert_eq!(catalog.logical().schema_generation(), 0);
        assert_eq!(logical_databases(&connection), databases_before);
        assert!(table_metadata_rows(&connection).is_empty());
        assert!(active.is_none());
        assert_eq!(integrity.state(), DatabaseIntegrityState::Verifying);
        assert_eq!(integrity.committed_schema_digest(), None);
        assert_eq!(integrity.target_schema_digest(), None);
        assert_eq!(
            manifest_semantic_digest(&connection).unwrap(),
            stored_manifest_digest(&connection)
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM briskdb_schema_migrations",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(quick_check(&connection), "ok");
    }

    #[test]
    fn version_six_upgrade_enters_verifying_and_clears_advisory_catalog_rows() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_v5_manifest(&mut connection, 4);
        insert_valid_table_catalog(&connection);
        let mut no_hook = |_| Ok(());
        let v6 = load_or_create_snapshot_with_plan(&mut connection, 4, V6_PLAN, true, &mut no_hook)
            .unwrap();
        assert_eq!(identity(&connection), (MANIFEST_APPLICATION_ID, 6));
        let databases_before = logical_databases(&connection);
        assert!(v6.active_migration.is_none());

        let loaded = load_or_create_manifest(&mut connection, 4).unwrap();
        assert_eq!(loaded.integrity.state(), DatabaseIntegrityState::Verifying);
        assert_eq!(loaded.integrity.committed_schema_digest(), None);
        assert_eq!(logical_databases(&connection), databases_before);
        assert!(table_metadata_rows(&connection).is_empty());
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM briskdb_schema_migrations",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            manifest_semantic_digest(&connection).unwrap(),
            stored_manifest_digest(&connection)
        );
    }

    #[test]
    fn version_six_migration_error_and_panic_restore_exact_v5_then_retry() {
        for failing_phase in [
            MigrationPhase::AfterSchemaChange,
            MigrationPhase::AfterVersionStamp,
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            create_v5_manifest(&mut connection, 4);
            let layout_before = shard_layout_row(&connection);

            let error = load_or_create_with_hook(&mut connection, 4, |point| {
                if point.from == V5_SCHEMA_VERSION && point.phase == failing_phase {
                    Err(EngineError::new(
                        EngineErrorKind::Internal,
                        "injected schema journal migration failure",
                    ))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Internal);
            assert_eq!(identity(&connection), (MANIFEST_APPLICATION_ID, 5));
            assert_eq!(schema_objects(&connection).unwrap(), v5_objects());
            assert_eq!(shard_layout_row(&connection), layout_before);
            assert_eq!(quick_check(&connection), "ok");

            load_or_create_manifest(&mut connection, 4).unwrap();
            assert_eq!(
                identity(&connection),
                (MANIFEST_APPLICATION_ID, i64::from(CURRENT_SCHEMA_VERSION))
            );
            assert_eq!(schema_objects(&connection).unwrap(), v12_objects());
        }

        let mut connection = Connection::open_in_memory().unwrap();
        create_v5_manifest(&mut connection, 4);
        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = load_or_create_with_hook(&mut connection, 4, |point| {
                if point.from == V5_SCHEMA_VERSION
                    && point.phase == MigrationPhase::AfterVersionStamp
                {
                    panic!("injected schema journal migration panic");
                }
                Ok(())
            });
        }));
        assert!(panic.is_err());
        assert_eq!(identity(&connection), (MANIFEST_APPLICATION_ID, 5));
        assert_eq!(schema_objects(&connection).unwrap(), v5_objects());
        load_or_create_manifest(&mut connection, 4).unwrap();
        assert_eq!(
            identity(&connection),
            (MANIFEST_APPLICATION_ID, i64::from(CURRENT_SCHEMA_VERSION))
        );
    }

    #[test]
    fn schema_migration_journal_is_exact_idempotent_and_monotonic() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let first_sql = "CREATE TABLE widgets (id INTEGER PRIMARY KEY)";

        assert_eq!(
            classify_schema_migration(&mut connection, 4, first_sql).unwrap(),
            SchemaMigrationClassification::Absent
        );
        let active = begin_schema_migration(&mut connection, 4, 0, first_sql).unwrap();
        assert_eq!(active.source_generation(), 0);
        assert_eq!(active.target_generation(), 1);
        assert_eq!(
            active.migration_id(),
            schema_migration_id(first_sql).unwrap()
        );
        assert_eq!(active.sql_text(), first_sql);
        assert_eq!(active.shard_count(), 4);
        assert_eq!(active.next_shard(), 0);
        assert!(active.is_applying());
        assert_eq!(
            load_active_schema_migration(&mut connection, 4).unwrap(),
            Some(active.clone())
        );
        assert_eq!(
            begin_schema_migration(&mut connection, 4, 0, first_sql).unwrap(),
            active
        );
        assert_eq!(
            classify_schema_migration(&mut connection, 4, first_sql).unwrap(),
            SchemaMigrationClassification::Active(active.clone())
        );

        let conflict =
            begin_schema_migration(&mut connection, 4, 0, "CREATE TABLE different (id INTEGER)")
                .unwrap_err();
        assert_eq!(conflict.kind(), EngineErrorKind::FailedPrecondition);
        let skipped = advance_schema_migration(&mut connection, 4, &active, 2).unwrap_err();
        assert_eq!(skipped.kind(), EngineErrorKind::FailedPrecondition);
        let premature = finalize_schema_migration(&mut connection, 4, &active).unwrap_err();
        assert_eq!(premature.kind(), EngineErrorKind::FailedPrecondition);

        let one = advance_schema_migration(&mut connection, 4, &active, 1).unwrap();
        assert_eq!(one.next_shard(), 1);
        assert_eq!(
            advance_schema_migration(&mut connection, 4, &one, 1).unwrap(),
            one
        );
        let two = advance_schema_migration(&mut connection, 4, &one, 2).unwrap();
        let three = advance_schema_migration(&mut connection, 4, &two, 3).unwrap();
        let four = advance_schema_migration(&mut connection, 4, &three, 4).unwrap();
        let complete = finalize_schema_migration(&mut connection, 4, &four).unwrap();
        assert!(complete.is_complete());
        assert_eq!(complete.next_shard(), 4);
        assert!(
            load_active_schema_migration(&mut connection, 4)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            classify_schema_migration(&mut connection, 4, first_sql).unwrap(),
            SchemaMigrationClassification::Complete(complete.clone())
        );
        assert_eq!(
            begin_schema_migration(&mut connection, 4, 0, first_sql).unwrap(),
            complete
        );
        assert_eq!(
            load_or_create_manifest(&mut connection, 4)
                .unwrap()
                .into_parts()
                .0
                .logical()
                .schema_generation(),
            1
        );

        let stale = begin_schema_migration(
            &mut connection,
            4,
            0,
            "CREATE INDEX widget_ids ON widgets(id)",
        )
        .unwrap_err();
        assert_eq!(stale.kind(), EngineErrorKind::FailedPrecondition);
        let second = complete_manifest_migration(
            &mut connection,
            4,
            1,
            "CREATE INDEX widget_ids ON widgets(id)",
        );
        assert_eq!(second.target_generation(), 2);
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM briskdb_schema_migrations",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            2
        );
        assert_eq!(
            load_or_create_manifest(&mut connection, 4)
                .unwrap()
                .into_parts()
                .0
                .logical()
                .schema_generation(),
            2
        );
    }

    #[test]
    fn exact_sql_bytes_define_the_bounded_migration_identity() {
        assert_eq!(
            schema_migration_id("").unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
        assert_eq!(
            schema_migration_id("SELECT 1\0SELECT 2")
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
        let at_limit = "x".repeat(MAX_SCHEMA_MIGRATION_SQL_BYTES);
        assert_eq!(
            schema_migration_id(&at_limit).unwrap(),
            schema_migration_digest(&at_limit)
        );
        assert_eq!(
            schema_migration_id(&format!("{at_limit}x"))
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
        assert_ne!(
            schema_migration_id("CREATE TABLE t(id INTEGER)").unwrap(),
            schema_migration_id("CREATE TABLE t (id INTEGER)").unwrap()
        );
    }

    #[test]
    fn a_different_active_migration_takes_precedence_over_history_and_absence() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let completed_sql = "CREATE TABLE completed_marker (id INTEGER)";
        complete_manifest_migration(&mut connection, 4, 0, completed_sql);
        let active_sql = "CREATE TABLE active_marker (id INTEGER)";
        let active = begin_schema_migration(&mut connection, 4, 1, active_sql).unwrap();

        assert_eq!(
            classify_schema_migration(&mut connection, 4, active_sql).unwrap(),
            SchemaMigrationClassification::Active(active)
        );
        for conflicting_sql in [
            completed_sql,
            "CREATE TABLE previously_absent_marker (id INTEGER)",
        ] {
            let error = classify_schema_migration(&mut connection, 4, conflicting_sql).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
            assert_eq!(
                error.to_string(),
                "a different schema migration is already active"
            );
            let error = begin_schema_migration(&mut connection, 4, 1, conflicting_sql).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
            assert_eq!(
                error.to_string(),
                "a different schema migration is already active"
            );
        }
    }

    #[test]
    fn held_transaction_journal_mutations_roll_back_on_error_and_panic() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let sql = "CREATE TABLE rollback_marker (id INTEGER)";

        {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            let active = begin_schema_migration_in_transaction(&transaction, 4, 0, sql).unwrap();
            assert_eq!(active.next_shard(), 0);
            drop(transaction);
        }
        assert!(matches!(
            classify_schema_migration(&mut connection, 4, sql).unwrap(),
            SchemaMigrationClassification::Absent
        ));

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            begin_schema_migration_in_transaction(&transaction, 4, 0, sql).unwrap();
            panic!("injected journal creation panic");
        }));
        assert!(panic.is_err());
        assert!(
            load_active_schema_migration(&mut connection, 4)
                .unwrap()
                .is_none()
        );

        let active = begin_schema_migration(&mut connection, 4, 0, sql).unwrap();
        {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            let advanced =
                advance_schema_migration_in_transaction(&transaction, 4, &active, 1).unwrap();
            assert_eq!(advanced.next_shard(), 1);
            drop(transaction);
        }
        assert_eq!(
            load_active_schema_migration(&mut connection, 4)
                .unwrap()
                .unwrap()
                .next_shard(),
            0
        );

        let mut active = active;
        for next in 1..=4 {
            active = advance_schema_migration(&mut connection, 4, &active, next).unwrap();
        }
        {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            let complete =
                finalize_schema_migration_in_transaction(&transaction, 4, &active).unwrap();
            assert!(complete.is_complete());
            drop(transaction);
        }
        let still_active = load_active_schema_migration(&mut connection, 4)
            .unwrap()
            .unwrap();
        assert!(still_active.is_applying());
        assert_eq!(
            load_or_create_manifest(&mut connection, 4)
                .unwrap()
                .into_parts()
                .0
                .logical()
                .schema_generation(),
            0
        );
        finalize_schema_migration(&mut connection, 4, &still_active).unwrap();
    }

    #[test]
    fn non_ready_layouts_require_an_empty_schema_migration_history() {
        let mut creating = Connection::open_in_memory().unwrap();
        let loaded = load_or_create_manifest(&mut creating, 4).unwrap();
        assert_eq!(loaded.into_parts().1.state(), ShardLayoutState::Creating);
        let error = begin_schema_migration(
            &mut creating,
            4,
            0,
            "CREATE TABLE cannot_start (id INTEGER)",
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);

        creating
            .execute(
                "INSERT INTO briskdb_schema_migrations VALUES (
                    1, 0, ?1, 1, ?2, 4, 1, 0
                 )",
                rusqlite::params![
                    schema_migration_digest("CREATE TABLE injected (id INTEGER)").as_slice(),
                    "CREATE TABLE injected (id INTEGER)"
                ],
            )
            .unwrap();
        assert_eq!(
            load_or_create_manifest(&mut creating, 4)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );

        let mut adopting = Connection::open_in_memory().unwrap();
        create_v4_manifest(&mut adopting, 4);
        let loaded = load_or_create_manifest(&mut adopting, 4).unwrap();
        assert_eq!(loaded.into_parts().1.state(), ShardLayoutState::Adopting);
        assert!(
            load_active_schema_migration(&mut adopting, 4)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn schema_migration_history_corruption_fails_closed() {
        for mutation in [
            "UPDATE briskdb_schema_migrations SET migration_id = zeroblob(32)",
            "UPDATE briskdb_schema_migrations SET digest_version = 2",
            "UPDATE briskdb_schema_migrations SET source_generation = 7",
            "UPDATE briskdb_schema_migrations SET migration_state = 1",
            "UPDATE briskdb_schema_migrations SET next_shard = 3",
            "UPDATE briskdb_schema_migrations SET shard_count = 3",
            "UPDATE briskdb_schema_migrations SET sql_text = sql_text || char(0)",
            "UPDATE briskdb_schema_catalog SET schema_generation = 2",
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            create_ready_current_manifest(&mut connection, 4);
            complete_manifest_migration(
                &mut connection,
                4,
                0,
                "CREATE TABLE corruption_target (id INTEGER)",
            );
            connection
                .execute_batch("PRAGMA ignore_check_constraints = ON;")
                .unwrap();
            connection.execute_batch(mutation).unwrap();
            connection
                .execute_batch("PRAGMA ignore_check_constraints = OFF;")
                .unwrap();

            let error = load_or_create_manifest(&mut connection, 4).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption, "{mutation}");
        }

        let mut invalid_text = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut invalid_text, 4);
        complete_manifest_migration(
            &mut invalid_text,
            4,
            0,
            "CREATE TABLE invalid_text_target (id INTEGER)",
        );
        invalid_text
            .execute_batch(
                "PRAGMA ignore_check_constraints = ON;
                 UPDATE briskdb_schema_migrations SET sql_text = CAST(x'80' AS TEXT);
                 PRAGMA ignore_check_constraints = OFF;",
            )
            .unwrap();
        assert_eq!(
            load_or_create_manifest(&mut invalid_text, 4)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn concurrent_journal_creators_converge_or_reject_a_conflict() {
        fn run(sqls: [&'static str; 2]) -> Vec<EngineResult<SchemaMigration>> {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("manifest.sqlite");
            let mut setup = Connection::open(&path).unwrap();
            create_ready_current_manifest(&mut setup, 4);
            drop(setup);

            let barrier = Arc::new(Barrier::new(2));
            let workers = sqls.map(|sql| {
                let path = path.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let mut connection = Connection::open(path).unwrap();
                    connection.busy_timeout(Duration::from_secs(5)).unwrap();
                    barrier.wait();
                    begin_schema_migration(&mut connection, 4, 0, sql)
                })
            });
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect()
        }

        let identical = run([
            "CREATE TABLE concurrent_same (id INTEGER)",
            "CREATE TABLE concurrent_same (id INTEGER)",
        ]);
        assert!(identical.iter().all(Result::is_ok));
        assert_eq!(
            identical[0].as_ref().unwrap().migration_id(),
            identical[1].as_ref().unwrap().migration_id()
        );

        let conflicting = run([
            "CREATE TABLE concurrent_first (id INTEGER)",
            "CREATE TABLE concurrent_second (id INTEGER)",
        ]);
        assert_eq!(
            conflicting.iter().filter(|result| result.is_ok()).count(),
            1
        );
        assert_eq!(
            conflicting
                .iter()
                .find_map(|result| result.as_ref().err())
                .unwrap()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
    }

    #[test]
    fn ready_transition_rejects_different_layout_identity_or_state_without_mutation() {
        let mut connection = Connection::open_in_memory().unwrap();
        let (_, expected) = load_or_create_manifest(&mut connection, 4)
            .unwrap()
            .into_parts();
        let wrong_state = ShardLayout::from_validated_parts(
            expected.layout_id(),
            expected.expected_application_id(),
            expected.metadata_version(),
            ShardLayoutState::Adopting,
        );
        let error = mark_shard_layout_ready(&mut connection, 4, &wrong_state).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);

        let mut wrong_id = expected.layout_id();
        wrong_id[0] ^= 0xff;
        let wrong = ShardLayout::from_validated_parts(
            wrong_id,
            expected.expected_application_id(),
            expected.metadata_version(),
            expected.state(),
        );

        let error = mark_shard_layout_ready(&mut connection, 4, &wrong).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        let (_, observed) = load_or_create_manifest(&mut connection, 4)
            .unwrap()
            .into_parts();
        assert_eq!(observed, expected);
    }

    #[test]
    fn fresh_initialization_is_rejected_beside_an_existing_layout() {
        let mut empty = Connection::open_in_memory().unwrap();
        let error = load_or_create_manifest_with_fresh_layout(&mut empty, 4, false).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&empty), (0, 0));
        assert!(schema_objects(&empty).unwrap().is_empty());

        let mut interrupted = Connection::open_in_memory().unwrap();
        create_empty_legacy_manifest(&interrupted);
        let error =
            load_or_create_manifest_with_fresh_layout(&mut interrupted, 4, false).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&interrupted), (0, 0));
        assert_eq!(schema_objects(&interrupted).unwrap(), legacy_objects());
    }

    #[test]
    fn rejects_corrupt_physical_layout_rows() {
        for mutation in [
            "DELETE FROM briskdb_shard_layout",
            "UPDATE briskdb_shard_layout SET layout_id = x'01'",
            "UPDATE briskdb_shard_layout SET shard_application_id = 7",
            "UPDATE briskdb_shard_layout SET shard_metadata_version = 2",
            "UPDATE briskdb_shard_layout SET layout_state = 9",
            "INSERT INTO briskdb_shard_layout VALUES (2, randomblob(16), 1112691528, 1, 1)",
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            load_or_create(&mut connection, 4).unwrap();
            connection
                .execute_batch("PRAGMA ignore_check_constraints = ON;")
                .unwrap();
            connection.execute_batch(mutation).unwrap();
            connection
                .execute_batch("PRAGMA ignore_check_constraints = OFF;")
                .unwrap();

            let error = load_or_create(&mut connection, 4).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption, "{mutation}");
        }
    }

    #[test]
    fn valid_logical_database_and_table_metadata_round_trips_all_supported_codes() {
        let mut connection = Connection::open_in_memory().unwrap();
        load_or_create(&mut connection, 4).unwrap();
        insert_valid_table_catalog(&connection);

        let first = load_or_create_catalog(&mut connection, 4).unwrap();
        let catalog = first.logical();
        assert_eq!(catalog.identifier_encoding_version(), 1);
        assert_eq!(catalog.schema_generation(), 0);
        assert_eq!(catalog.default_database().name(), "default");
        assert_eq!(catalog.logical_databases().len(), 2);
        assert_eq!(catalog.tables().len(), 5);
        assert_eq!(
            catalog
                .table("default", "accounts")
                .unwrap()
                .unwrap()
                .id()
                .get(),
            3
        );
        assert!(matches!(
            catalog
                .table("default", "countries")
                .unwrap()
                .unwrap()
                .placement(),
            TablePlacement::Global
        ));
        assert!(matches!(
            catalog
                .table("tenant", "audit_log")
                .unwrap()
                .unwrap()
                .placement(),
            TablePlacement::Catalog
        ));
        for (table, expected_type) in [
            ("binary_keys", ShardKeyType::Binary),
            ("counters", ShardKeyType::Int64),
        ] {
            match catalog.table("tenant", table).unwrap().unwrap().placement() {
                TablePlacement::Sharded(shard_key) => {
                    assert_eq!(shard_key.key_type(), expected_type);
                }
                placement => panic!("unexpected placement {placement:?}"),
            }
        }

        let reopened = load_or_create_catalog(&mut connection, 4).unwrap();
        assert_eq!(first, reopened);
        assert_eq!(quick_check(&connection), "ok");
    }

    #[test]
    fn logical_catalog_sql_constraints_reject_invalid_rows() {
        for invalid_insert in [
            "INSERT INTO briskdb_logical_databases VALUES (2, 'Tenant')",
            "INSERT INTO briskdb_logical_databases VALUES (2, '9tenant')",
            "INSERT INTO briskdb_logical_databases VALUES (2, 'briskdb_private')",
            "INSERT INTO briskdb_logical_databases VALUES (2, 'sqlite_private')",
            "INSERT INTO briskdb_logical_databases VALUES (2, 'a' || char(0) || 'UPPER')",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'Widgets', 2, NULL, NULL)",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'a' || char(0) || 'UPPER', 2, NULL, NULL)",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'widgets', 4, NULL, NULL)",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'widgets', 1, NULL, NULL)",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'widgets', 2, 'tenant_id', 2)",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'widgets', 1, 'TenantId', 2)",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'widgets', 1, 'a' || char(0) || 'UPPER', 2)",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'widgets', 1, 'tenant_id', 4)",
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            load_or_create(&mut connection, 4).unwrap();

            assert!(
                connection.execute_batch(invalid_insert).is_err(),
                "SQLite accepted invalid catalog row: {invalid_insert}"
            );
            assert_generation_one_catalog(&connection, 4);
        }

        let mut foreign_key = Connection::open_in_memory().unwrap();
        foreign_key
            .pragma_update(None, "foreign_keys", "ON")
            .unwrap();
        load_or_create(&mut foreign_key, 4).unwrap();
        assert!(
            foreign_key
                .execute_batch(
                    "INSERT INTO briskdb_tables
                     VALUES (1, 9, 'widgets', 2, NULL, NULL)"
                )
                .is_err()
        );
        assert_generation_one_catalog(&foreign_key, 4);
    }

    #[test]
    fn fresh_v2_and_v3_upgraded_catalogs_are_identical_for_every_shard_count() {
        for shard_count in 2..=64 {
            let mut fresh = Connection::open_in_memory().unwrap();
            let fresh_catalog = load_or_create_catalog(&mut fresh, shard_count).unwrap();
            assert_eq!(fresh_catalog.routing().shard_count(), shard_count);
            assert_generation_one_catalog(&fresh, shard_count);
            assert_eq!(
                shard_layout_row(&fresh).3,
                ShardLayoutState::Creating.code()
            );

            let mut upgraded = Connection::open_in_memory().unwrap();
            create_v2_manifest(&mut upgraded, shard_count);
            let upgraded_catalog = load_or_create_catalog(&mut upgraded, shard_count).unwrap();
            assert_eq!(upgraded_catalog.routing().shard_count(), shard_count);
            assert_generation_one_catalog(&upgraded, shard_count);
            assert_eq!(
                shard_layout_row(&upgraded).3,
                ShardLayoutState::Adopting.code()
            );
            assert_eq!(fresh_catalog, upgraded_catalog);
            for key in [
                b"".as_slice(),
                b"customer-42".as_slice(),
                b"a\0b".as_slice(),
                [0_u8, 1, 2, 0xff].as_slice(),
                "snowman-☃".as_bytes(),
            ] {
                assert_eq!(
                    fresh_catalog.routing().shard_for_key(key),
                    upgraded_catalog.routing().shard_for_key(key)
                );
            }
            assert_eq!(
                routing_configuration(&fresh),
                routing_configuration(&upgraded)
            );
            assert_eq!(physical_shards(&fresh), physical_shards(&upgraded));
            assert_eq!(virtual_buckets(&fresh), virtual_buckets(&upgraded));

            let mut version_three = Connection::open_in_memory().unwrap();
            create_v3_manifest(&mut version_three, shard_count);
            let version_three_catalog =
                load_or_create_catalog(&mut version_three, shard_count).unwrap();
            assert_eq!(fresh_catalog, version_three_catalog);
            assert_generation_one_catalog(&version_three, shard_count);
            assert_eq!(
                shard_layout_row(&version_three).3,
                ShardLayoutState::Adopting.code()
            );
        }
    }

    #[test]
    fn generation_one_bucket_ranges_can_preserve_legacy_modulo_placement() {
        let hashes = [
            0,
            1,
            2,
            4_095,
            4_096,
            65_535,
            0x0123_4567_89ab_cdef,
            u64::MAX,
        ];
        for shard_count in 2..=64_u16 {
            let base_size = u64::from(VIRTUAL_BUCKET_COUNT / shard_count);
            let wider_shards = u64::from(VIRTUAL_BUCKET_COUNT % shard_count);
            for hash in hashes {
                let legacy_shard = hash % u64::from(shard_count);
                let group_size = base_size + u64::from(legacy_shard < wider_shards);
                let offset = legacy_shard * base_size + legacy_shard.min(wider_shards);
                let bucket = offset + (hash / u64::from(shard_count)) % group_size;
                let bucket = u16::try_from(bucket).unwrap();

                assert!(bucket < VIRTUAL_BUCKET_COUNT);
                assert_eq!(
                    initial_physical_shard(bucket, shard_count),
                    u16::try_from(legacy_shard).unwrap()
                );
            }
        }
    }

    #[test]
    fn rejects_a_later_map_generation_until_the_manifest_format_supports_it() {
        let mut connection = Connection::open_in_memory().unwrap();
        load_or_create(&mut connection, 4).unwrap();
        connection
            .execute_batch(
                "PRAGMA ignore_check_constraints = ON;
                 BEGIN IMMEDIATE;
                 UPDATE briskdb_virtual_buckets
                 SET physical_shard_id = 1
                 WHERE bucket_id = 0;
                 UPDATE briskdb_virtual_buckets
                 SET physical_shard_id = 0
                 WHERE bucket_id = 1024;
                 UPDATE briskdb_routing SET map_generation = 2;
                 COMMIT;
                 PRAGMA ignore_check_constraints = OFF;",
            )
            .unwrap();
        refresh_manifest_digest(&connection).unwrap();

        let error = load_or_create(&mut connection, 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(
            error.to_string(),
            "manifest has unsupported map generation 2"
        );
        assert_eq!(routing_configuration(&connection).5, 2);
    }

    #[test]
    fn upgrades_unversioned_and_explicitly_versioned_legacy_manifests() {
        for legacy_header in [0, 1] {
            let mut connection = Connection::open_in_memory().unwrap();
            create_legacy_manifest(&connection, 4, legacy_header);

            assert_eq!(load_or_create(&mut connection, 4).unwrap(), 4);
            assert_eq!(current_shard_count(&connection), 4);
            assert_generation_one_catalog(&connection, 4);
            assert_eq!(
                shard_layout_row(&connection).3,
                ShardLayoutState::Adopting.code()
            );
            assert_eq!(quick_check(&connection), "ok");
        }
    }

    #[test]
    fn recovers_the_exact_empty_table_left_by_interrupted_legacy_initialization() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_empty_legacy_manifest(&connection);

        assert_eq!(load_or_create(&mut connection, 8).unwrap(), 8);
        assert_eq!(current_shard_count(&connection), 8);
        assert_generation_one_catalog(&connection, 8);
        assert_eq!(
            shard_layout_row(&connection).3,
            ShardLayoutState::Creating.code()
        );
    }

    #[test]
    fn rejects_partial_and_noncanonical_legacy_metadata() {
        let mutations = [
            "INSERT INTO briskdb_metadata VALUES ('schema_version', '1')",
            "INSERT INTO briskdb_metadata VALUES ('shard_count', '4')",
            "INSERT INTO briskdb_metadata VALUES ('unexpected', 'value')",
        ];
        for mutation in mutations {
            let mut connection = Connection::open_in_memory().unwrap();
            create_empty_legacy_manifest(&connection);
            connection.execute(mutation, []).unwrap();

            let error = load_or_create(&mut connection, 4).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
            assert_eq!(identity(&connection), (0, 0));
        }

        let mut connection = Connection::open_in_memory().unwrap();
        create_legacy_manifest(&connection, 4, 0);
        connection
            .execute(
                "UPDATE briskdb_metadata SET value = '04' WHERE key = 'shard_count'",
                [],
            )
            .unwrap();
        let error = load_or_create(&mut connection, 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);

        for mutation in [
            "UPDATE briskdb_metadata SET value = '2' WHERE key = 'schema_version'",
            "UPDATE briskdb_metadata SET value = 'not-a-number' WHERE key = 'shard_count'",
            "UPDATE briskdb_metadata SET value = '1' WHERE key = 'shard_count'",
            "INSERT INTO briskdb_metadata VALUES ('unexpected', 'value')",
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            create_legacy_manifest(&connection, 4, 0);
            connection.execute(mutation, []).unwrap();
            let error = load_or_create(&mut connection, 4).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        }

        let mut altered_definition = Connection::open_in_memory().unwrap();
        altered_definition
            .execute_batch(
                "CREATE TABLE briskdb_metadata (
                    key TEXT COLLATE NOCASE PRIMARY KEY,
                    value TEXT NOT NULL
                 );
                 INSERT INTO briskdb_metadata VALUES ('schema_version', '1');
                 INSERT INTO briskdb_metadata VALUES ('shard_count', '4');",
            )
            .unwrap();
        let error = load_or_create(&mut altered_definition, 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
    }

    #[test]
    fn shard_mismatch_does_not_upgrade_the_legacy_manifest() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_legacy_manifest(&connection, 4, 0);

        let error = load_or_create(&mut connection, 8).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&connection), (0, 0));
        assert_eq!(schema_objects(&connection).unwrap(), legacy_objects());
        assert_eq!(
            connection
                .query_row(
                    "SELECT value FROM briskdb_metadata WHERE key = 'shard_count'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "4"
        );
    }

    #[test]
    fn shard_mismatch_does_not_upgrade_a_version_two_manifest() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_v2_manifest(&mut connection, 3);

        let error = load_or_create(&mut connection, 5).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&connection), (MANIFEST_APPLICATION_ID, 2));
        assert_eq!(schema_objects(&connection).unwrap(), v2_objects());
        assert_eq!(quick_check(&connection), "ok");
    }

    #[test]
    fn failures_before_and_after_version_stamping_roll_back_and_retry() {
        for failing_phase in [
            MigrationPhase::AfterSchemaChange,
            MigrationPhase::AfterVersionStamp,
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            create_legacy_manifest(&connection, 4, 0);

            let error = load_or_create_with_hook(&mut connection, 4, |point| {
                if point.phase == failing_phase {
                    Err(EngineError::new(
                        EngineErrorKind::Internal,
                        "injected migration failure",
                    ))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Internal);
            assert_eq!(identity(&connection), (0, 0));
            assert_eq!(schema_objects(&connection).unwrap(), legacy_objects());
            assert_eq!(quick_check(&connection), "ok");

            assert_eq!(load_or_create(&mut connection, 4).unwrap(), 4);
            assert_generation_one_catalog(&connection, 4);
        }
    }

    #[test]
    fn catalog_migration_failures_roll_back_to_exact_v2_and_retry() {
        for failing_phase in [
            MigrationPhase::AfterSchemaChange,
            MigrationPhase::AfterVersionStamp,
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            create_v2_manifest(&mut connection, 5);

            let error = load_or_create_with_hook(&mut connection, 5, |point| {
                if point.from == V2_SCHEMA_VERSION && point.phase == failing_phase {
                    Err(EngineError::new(
                        EngineErrorKind::Internal,
                        "injected catalog migration failure",
                    ))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Internal);
            assert_eq!(identity(&connection), (MANIFEST_APPLICATION_ID, 2));
            assert_eq!(schema_objects(&connection).unwrap(), v2_objects());
            assert_eq!(
                connection
                    .query_row(
                        "SELECT requires_manifest_version FROM briskdb_metadata",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                2
            );
            assert_eq!(quick_check(&connection), "ok");

            assert_eq!(load_or_create(&mut connection, 5).unwrap(), 5);
            assert_generation_one_catalog(&connection, 5);
        }
    }

    #[test]
    fn panic_during_catalog_migration_rolls_back_to_v2_and_retry_succeeds() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_v2_manifest(&mut connection, 3);

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = load_or_create_with_hook(&mut connection, 3, |point| {
                if point.from == V2_SCHEMA_VERSION
                    && point.phase == MigrationPhase::AfterVersionStamp
                {
                    panic!("injected catalog migration panic");
                }
                Ok(())
            });
        }));
        assert!(panic.is_err());
        assert_eq!(identity(&connection), (MANIFEST_APPLICATION_ID, 2));
        assert_eq!(schema_objects(&connection).unwrap(), v2_objects());
        assert_eq!(quick_check(&connection), "ok");

        assert_eq!(load_or_create(&mut connection, 3).unwrap(), 3);
        assert_generation_one_catalog(&connection, 3);
    }

    #[test]
    fn logical_catalog_migration_failures_roll_back_to_exact_v3_and_retry() {
        for failing_phase in [
            MigrationPhase::AfterSchemaChange,
            MigrationPhase::AfterVersionStamp,
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            create_v3_manifest(&mut connection, 5);
            let routing_before = routing_configuration(&connection);
            let shards_before = physical_shards(&connection);
            let buckets_before = virtual_buckets(&connection);

            let error = load_or_create_with_hook(&mut connection, 5, |point| {
                if point.from == V3_SCHEMA_VERSION && point.phase == failing_phase {
                    Err(EngineError::new(
                        EngineErrorKind::Internal,
                        "injected logical catalog migration failure",
                    ))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Internal);
            assert_eq!(
                identity(&connection),
                (MANIFEST_APPLICATION_ID, i64::from(V3_SCHEMA_VERSION))
            );
            assert_eq!(schema_objects(&connection).unwrap(), v3_objects());
            assert_eq!(routing_configuration(&connection), routing_before);
            assert_eq!(physical_shards(&connection), shards_before);
            assert_eq!(virtual_buckets(&connection), buckets_before);
            assert_eq!(quick_check(&connection), "ok");

            assert_eq!(load_or_create(&mut connection, 5).unwrap(), 5);
            assert_generation_one_catalog(&connection, 5);
        }
    }

    #[test]
    fn panics_on_both_sides_of_the_logical_catalog_stamp_roll_back_and_retry() {
        for failing_phase in [
            MigrationPhase::AfterSchemaChange,
            MigrationPhase::AfterVersionStamp,
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            create_v3_manifest(&mut connection, 3);
            let buckets_before = virtual_buckets(&connection);

            let panic = catch_unwind(AssertUnwindSafe(|| {
                let _ = load_or_create_with_hook(&mut connection, 3, |point| {
                    if point.from == V3_SCHEMA_VERSION && point.phase == failing_phase {
                        panic!("injected logical catalog migration panic");
                    }
                    Ok(())
                });
            }));
            assert!(panic.is_err());
            assert_eq!(
                identity(&connection),
                (MANIFEST_APPLICATION_ID, i64::from(V3_SCHEMA_VERSION))
            );
            assert_eq!(schema_objects(&connection).unwrap(), v3_objects());
            assert_eq!(virtual_buckets(&connection), buckets_before);
            assert_eq!(quick_check(&connection), "ok");

            assert_eq!(load_or_create(&mut connection, 3).unwrap(), 3);
            assert_generation_one_catalog(&connection, 3);
        }
    }

    #[test]
    fn physical_layout_migration_failures_roll_back_to_exact_v4_and_retry() {
        for failing_phase in [
            MigrationPhase::AfterSchemaChange,
            MigrationPhase::AfterVersionStamp,
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            create_v4_manifest(&mut connection, 4);
            insert_valid_table_catalog(&connection);
            let databases_before = logical_databases(&connection);
            let tables_before = table_metadata_rows(&connection);

            let error = load_or_create_with_hook(&mut connection, 4, |point| {
                if point.from == V4_SCHEMA_VERSION && point.phase == failing_phase {
                    Err(EngineError::new(
                        EngineErrorKind::Internal,
                        "injected physical layout migration failure",
                    ))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Internal);
            assert_eq!(
                identity(&connection),
                (MANIFEST_APPLICATION_ID, i64::from(V4_SCHEMA_VERSION))
            );
            assert_eq!(schema_objects(&connection).unwrap(), v4_objects());
            assert_eq!(logical_databases(&connection), databases_before);
            assert_eq!(table_metadata_rows(&connection), tables_before);
            assert_eq!(quick_check(&connection), "ok");

            let (_, layout) = load_or_create_manifest(&mut connection, 4)
                .unwrap()
                .into_parts();
            assert_eq!(layout.state(), ShardLayoutState::Adopting);
        }
    }

    #[test]
    fn physical_layout_migration_panics_roll_back_to_exact_v4_and_retry() {
        for failing_phase in [
            MigrationPhase::AfterSchemaChange,
            MigrationPhase::AfterVersionStamp,
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            create_v4_manifest(&mut connection, 3);

            let panic = catch_unwind(AssertUnwindSafe(|| {
                let _ = load_or_create_with_hook(&mut connection, 3, |point| {
                    if point.from == V4_SCHEMA_VERSION && point.phase == failing_phase {
                        panic!("injected physical layout migration panic");
                    }
                    Ok(())
                });
            }));
            assert!(panic.is_err());
            assert_eq!(
                identity(&connection),
                (MANIFEST_APPLICATION_ID, i64::from(V4_SCHEMA_VERSION))
            );
            assert_eq!(schema_objects(&connection).unwrap(), v4_objects());
            assert_eq!(quick_check(&connection), "ok");

            let (_, layout) = load_or_create_manifest(&mut connection, 3)
                .unwrap()
                .into_parts();
            assert_eq!(layout.state(), ShardLayoutState::Adopting);
        }
    }

    #[test]
    fn reconciliation_error_and_panic_leave_provisioning_state_resumable() {
        let mut connection = Connection::open_in_memory().unwrap();
        let (_, creating) = load_or_create_manifest(&mut connection, 4)
            .unwrap()
            .into_parts();

        let error = reconcile_shard_layout(&mut connection, 4, &creating, |locked| {
            assert_eq!(locked.state(), ShardLayoutState::Creating);
            Err(EngineError::new(
                EngineErrorKind::Internal,
                "injected shard reconciliation failure",
            ))
        })
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert_eq!(
            shard_layout_row(&connection).3,
            ShardLayoutState::Creating.code()
        );

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = reconcile_shard_layout(&mut connection, 4, &creating, |_| {
                panic!("injected shard reconciliation panic");
            });
        }));
        assert!(panic.is_err());
        assert_eq!(
            shard_layout_row(&connection).3,
            ShardLayoutState::Creating.code()
        );

        let ready = reconcile_shard_layout(&mut connection, 4, &creating, |_| Ok(())).unwrap();
        assert_eq!(ready.state(), ShardLayoutState::Ready);
    }

    #[test]
    fn panic_during_fresh_initialization_rolls_back_and_retry_succeeds() {
        let mut connection = Connection::open_in_memory().unwrap();

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = load_or_create_with_hook(&mut connection, 4, |point| {
                if point.phase == MigrationPhase::AfterVersionStamp {
                    panic!("injected initialization panic");
                }
                Ok(())
            });
        }));
        assert!(panic.is_err());
        assert_eq!(identity(&connection), (0, 0));
        assert!(schema_objects(&connection).unwrap().is_empty());
        assert_eq!(quick_check(&connection), "ok");

        assert_eq!(load_or_create(&mut connection, 4).unwrap(), 4);
    }

    #[test]
    fn an_observer_never_sees_a_partially_migrated_schema() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let mut connection = Connection::open(&path).unwrap();
        create_v2_manifest(&mut connection, 4);
        drop(connection);

        let (paused_tx, paused_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let migration_path = path.clone();
        let worker = thread::spawn(move || {
            let mut connection = Connection::open(migration_path).unwrap();
            load_or_create_with_hook(&mut connection, 4, |point| {
                if point.from == V2_SCHEMA_VERSION
                    && point.phase == MigrationPhase::AfterVersionStamp
                {
                    paused_tx.send(()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                }
                Ok(())
            })
            .unwrap();
        });

        paused_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let observer = Connection::open(&path).unwrap();
        assert_eq!(identity(&observer), (MANIFEST_APPLICATION_ID, 2));
        assert_eq!(schema_objects(&observer).unwrap(), v2_objects());
        resume_tx.send(()).unwrap();
        worker.join().unwrap();

        assert_generation_one_catalog(&observer, 4);
    }

    #[test]
    fn an_observer_sees_exact_v3_until_the_logical_catalog_commits() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let mut connection = Connection::open(&path).unwrap();
        create_v3_manifest(&mut connection, 4);
        let buckets_before = virtual_buckets(&connection);
        drop(connection);

        let (paused_tx, paused_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let migration_path = path.clone();
        let worker = thread::spawn(move || {
            let mut connection = Connection::open(migration_path).unwrap();
            load_or_create_with_hook(&mut connection, 4, |point| {
                if point.from == V3_SCHEMA_VERSION
                    && point.phase == MigrationPhase::AfterVersionStamp
                {
                    paused_tx.send(()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                }
                Ok(())
            })
            .unwrap();
        });

        paused_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let observer = Connection::open(&path).unwrap();
        assert_eq!(
            identity(&observer),
            (MANIFEST_APPLICATION_ID, i64::from(V3_SCHEMA_VERSION))
        );
        assert_eq!(schema_objects(&observer).unwrap(), v3_objects());
        assert_eq!(virtual_buckets(&observer), buckets_before);
        resume_tx.send(()).unwrap();
        worker.join().unwrap();

        assert_generation_one_catalog(&observer, 4);
    }

    #[test]
    fn concurrent_legacy_openers_serialize_and_both_succeed() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let connection = Connection::open(&path).unwrap();
        create_legacy_manifest(&connection, 4, 0);
        drop(connection);

        let barrier = Arc::new(Barrier::new(4));
        let workers = (0..4)
            .map(|_| {
                let path = path.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let mut connection = Connection::open(path).unwrap();
                    connection
                        .busy_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                    barrier.wait();
                    load_or_create(&mut connection, 4)
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            assert_eq!(worker.join().unwrap().unwrap(), 4);
        }

        let connection = Connection::open(path).unwrap();
        assert_eq!(current_shard_count(&connection), 4);
        assert_generation_one_catalog(&connection, 4);
    }

    #[test]
    fn concurrent_version_two_openers_create_one_complete_catalog() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let mut connection = Connection::open(&path).unwrap();
        create_v2_manifest(&mut connection, 5);
        drop(connection);

        let barrier = Arc::new(Barrier::new(4));
        let workers = (0..4)
            .map(|_| {
                let path = path.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let mut connection = Connection::open(path).unwrap();
                    connection
                        .busy_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                    barrier.wait();
                    load_or_create(&mut connection, 5)
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            assert_eq!(worker.join().unwrap().unwrap(), 5);
        }

        let connection = Connection::open(path).unwrap();
        assert_generation_one_catalog(&connection, 5);
        assert_eq!(quick_check(&connection), "ok");
    }

    #[test]
    fn concurrent_version_three_openers_share_one_complete_logical_catalog() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let mut connection = Connection::open(&path).unwrap();
        create_v3_manifest(&mut connection, 5);
        drop(connection);

        let barrier = Arc::new(Barrier::new(4));
        let workers = (0..4)
            .map(|_| {
                let path = path.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let mut connection = Connection::open(path).unwrap();
                    connection
                        .busy_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                    barrier.wait();
                    load_or_create_catalog(&mut connection, 5)
                })
            })
            .collect::<Vec<_>>();
        let snapshots = workers
            .into_iter()
            .map(|worker| worker.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        assert!(snapshots.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(snapshots[0].logical().default_database().name(), "default");
        assert!(snapshots[0].logical().tables().is_empty());

        let connection = Connection::open(path).unwrap();
        assert_generation_one_catalog(&connection, 5);
        assert_eq!(quick_check(&connection), "ok");
    }

    #[test]
    fn v3_upgrade_lock_contention_is_retryable_and_leaves_v3_unchanged() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let mut owner = Connection::open(&path).unwrap();
        create_v3_manifest(&mut owner, 4);
        owner.execute_batch("BEGIN IMMEDIATE;").unwrap();

        let mut contender = Connection::open(&path).unwrap();
        contender.busy_timeout(Duration::ZERO).unwrap();
        let error = load_or_create(&mut contender, 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Busy);
        owner.execute_batch("ROLLBACK;").unwrap();

        assert_eq!(
            identity(&contender),
            (MANIFEST_APPLICATION_ID, i64::from(V3_SCHEMA_VERSION))
        );
        assert_eq!(schema_objects(&contender).unwrap(), v3_objects());
        assert_eq!(load_or_create(&mut contender, 4).unwrap(), 4);
        assert_generation_one_catalog(&contender, 4);
    }

    #[test]
    fn concurrent_initializers_choose_exactly_one_shard_count() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let barrier = Arc::new(Barrier::new(2));
        let workers = [4_u16, 8_u16]
            .into_iter()
            .map(|requested| {
                let path = path.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let mut connection = Connection::open(path).unwrap();
                    connection
                        .busy_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                    barrier.wait();
                    (requested, load_or_create(&mut connection, requested))
                })
            })
            .collect::<Vec<_>>();
        let results = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            results.iter().filter(|(_, result)| result.is_ok()).count(),
            1
        );
        let winner = results
            .iter()
            .find_map(|(requested, result)| result.is_ok().then_some(*requested))
            .unwrap();
        let loser = results
            .into_iter()
            .find_map(|(_, result)| result.err())
            .unwrap();
        assert_eq!(loser.kind(), EngineErrorKind::FailedPrecondition);

        let connection = Connection::open(path).unwrap();
        assert_eq!(current_shard_count(&connection), i64::from(winner));
        assert_generation_one_catalog(&connection, winner);
    }

    #[test]
    fn downgrade_fence_rejects_the_exact_legacy_open_sequence() {
        let mut connection = Connection::open_in_memory().unwrap();
        load_or_create(&mut connection, 4).unwrap();

        let error = legacy_open(&connection, 4).unwrap_err();
        assert!(matches!(
            error,
            rusqlite::Error::SqliteFailure(_, _) | rusqlite::Error::SqlInputError { .. }
        ));
        assert_eq!(
            identity(&connection),
            (MANIFEST_APPLICATION_ID, i64::from(CURRENT_SCHEMA_VERSION))
        );
        assert_eq!(current_shard_count(&connection), 4);
    }

    #[test]
    fn version_two_reader_rejects_current_manifest_without_mutating_it() {
        const OLD_MIGRATIONS: &[Migration] = &[Migration {
            from: LEGACY_SCHEMA_VERSION,
            to: V2_SCHEMA_VERSION,
            name: "typed_manifest_and_downgrade_fence",
            apply: migrate_v1_to_v2,
            validate: validate_v2,
        }];
        const OLD_PLAN: MigrationPlan<'static> = MigrationPlan {
            current_version: V2_SCHEMA_VERSION,
            migrations: OLD_MIGRATIONS,
            initialize_current: create_v2_schema,
            initialize_interrupted_legacy: migrate_v1_to_v2,
        };

        let mut connection = Connection::open_in_memory().unwrap();
        load_or_create(&mut connection, 4).unwrap();
        let before = virtual_buckets(&connection);
        let error = inspect_with_plan(&connection, 4, OLD_PLAN).unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_generation_one_catalog(&connection, 4);
        assert_eq!(virtual_buckets(&connection), before);
    }

    #[test]
    fn version_three_reader_rejects_current_manifest_without_mutating_it() {
        const OLD_MIGRATIONS: &[Migration] = &[
            Migration {
                from: LEGACY_SCHEMA_VERSION,
                to: V2_SCHEMA_VERSION,
                name: "typed_manifest_and_downgrade_fence",
                apply: migrate_v1_to_v2,
                validate: validate_v2,
            },
            Migration {
                from: V2_SCHEMA_VERSION,
                to: V3_SCHEMA_VERSION,
                name: "durable_shard_catalog",
                apply: migrate_v2_to_v3,
                validate: validate_v3,
            },
        ];
        const OLD_PLAN: MigrationPlan<'static> = MigrationPlan {
            current_version: V3_SCHEMA_VERSION,
            migrations: OLD_MIGRATIONS,
            initialize_current: create_v3_schema,
            initialize_interrupted_legacy: migrate_interrupted_legacy_to_v3,
        };

        let mut connection = Connection::open_in_memory().unwrap();
        load_or_create(&mut connection, 4).unwrap();
        let objects = schema_objects(&connection).unwrap();
        let databases = logical_databases(&connection);
        let error = inspect_with_plan(&connection, 4, OLD_PLAN).unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(schema_objects(&connection).unwrap(), objects);
        assert_eq!(logical_databases(&connection), databases);
        assert_generation_one_catalog(&connection, 4);
    }

    #[test]
    fn version_four_reader_rejects_version_five_without_mutating_it() {
        const OLD_MIGRATIONS: &[Migration] = &[
            Migration {
                from: LEGACY_SCHEMA_VERSION,
                to: V2_SCHEMA_VERSION,
                name: "typed_manifest_and_downgrade_fence",
                apply: migrate_v1_to_v2,
                validate: validate_v2,
            },
            Migration {
                from: V2_SCHEMA_VERSION,
                to: V3_SCHEMA_VERSION,
                name: "durable_shard_catalog",
                apply: migrate_v2_to_v3,
                validate: validate_v3,
            },
            Migration {
                from: V3_SCHEMA_VERSION,
                to: V4_SCHEMA_VERSION,
                name: "logical_database_and_table_catalog",
                apply: migrate_v3_to_v4,
                validate: validate_v4,
            },
        ];
        const OLD_PLAN: MigrationPlan<'static> = MigrationPlan {
            current_version: V4_SCHEMA_VERSION,
            migrations: OLD_MIGRATIONS,
            initialize_current: create_v4_schema,
            initialize_interrupted_legacy: migrate_interrupted_legacy_to_v4,
        };

        let mut connection = Connection::open_in_memory().unwrap();
        load_or_create(&mut connection, 4).unwrap();
        let identity_before = identity(&connection);
        let objects_before = schema_objects(&connection).unwrap();
        let layout_before = shard_layout_row(&connection);

        let error = inspect_with_plan(&connection, 4, OLD_PLAN).unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&connection), identity_before);
        assert_eq!(schema_objects(&connection).unwrap(), objects_before);
        assert_eq!(shard_layout_row(&connection), layout_before);
    }

    #[test]
    fn version_five_reader_rejects_current_manifest_without_mutating_it() {
        const OLD_MIGRATIONS: &[Migration] = &[
            Migration {
                from: LEGACY_SCHEMA_VERSION,
                to: V2_SCHEMA_VERSION,
                name: "typed_manifest_and_downgrade_fence",
                apply: migrate_v1_to_v2,
                validate: validate_v2,
            },
            Migration {
                from: V2_SCHEMA_VERSION,
                to: V3_SCHEMA_VERSION,
                name: "durable_shard_catalog",
                apply: migrate_v2_to_v3,
                validate: validate_v3,
            },
            Migration {
                from: V3_SCHEMA_VERSION,
                to: V4_SCHEMA_VERSION,
                name: "logical_database_and_table_catalog",
                apply: migrate_v3_to_v4,
                validate: validate_v4,
            },
            Migration {
                from: V4_SCHEMA_VERSION,
                to: V5_SCHEMA_VERSION,
                name: "validated_physical_shard_layout",
                apply: migrate_v4_to_v5,
                validate: validate_v5,
            },
        ];
        const OLD_PLAN: MigrationPlan<'static> = MigrationPlan {
            current_version: V5_SCHEMA_VERSION,
            migrations: OLD_MIGRATIONS,
            initialize_current: create_v5_schema,
            initialize_interrupted_legacy: migrate_interrupted_legacy_to_v5,
        };

        let mut connection = Connection::open_in_memory().unwrap();
        load_or_create(&mut connection, 4).unwrap();
        let identity_before = identity(&connection);
        let objects_before = schema_objects(&connection).unwrap();

        let error = inspect_with_plan(&connection, 4, OLD_PLAN).unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&connection), identity_before);
        assert_eq!(schema_objects(&connection).unwrap(), objects_before);
        assert_eq!(
            connection
                .query_row(
                    "SELECT requires_manifest_version FROM briskdb_metadata",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            i64::from(CURRENT_SCHEMA_VERSION)
        );
    }

    #[test]
    fn version_six_reader_rejects_version_seven_without_mutating_it() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let identity_before = identity(&connection);
        let objects_before = schema_objects(&connection).unwrap();
        let root_before = stored_manifest_digest(&connection);

        let error = inspect_with_plan(&connection, 4, V6_PLAN).unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&connection), identity_before);
        assert_eq!(schema_objects(&connection).unwrap(), objects_before);
        assert_eq!(stored_manifest_digest(&connection), root_before);
        assert_eq!(manifest_semantic_digest(&connection).unwrap(), root_before);
    }

    #[test]
    fn rejects_future_and_foreign_manifests_without_mutating_them() {
        let mut future = Connection::open_in_memory().unwrap();
        load_or_create(&mut future, 4).unwrap();
        future
            .pragma_update(None, "user_version", CURRENT_SCHEMA_VERSION + 1)
            .unwrap();
        let objects = schema_objects(&future).unwrap();
        let error = load_or_create(&mut future, 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(
            identity(&future),
            (
                MANIFEST_APPLICATION_ID,
                i64::from(CURRENT_SCHEMA_VERSION + 1)
            )
        );
        assert_eq!(schema_objects(&future).unwrap(), objects);

        let mut foreign = Connection::open_in_memory().unwrap();
        foreign
            .execute_batch("CREATE TABLE foreign_data (id INTEGER);")
            .unwrap();
        foreign
            .pragma_update(None, "application_id", 0x1234)
            .unwrap();
        let error = load_or_create(&mut foreign, 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&foreign), (0x1234, 0));
        assert_eq!(
            schema_objects(&foreign).unwrap(),
            [SchemaObject {
                object_type: "table".to_owned(),
                name: "foreign_data".to_owned(),
            }]
        );
    }

    #[test]
    fn rejects_inconsistent_or_tampered_current_manifests() {
        for mutation in [
            "DELETE FROM briskdb_metadata",
            "DELETE FROM briskdb_manifest",
            "INSERT INTO briskdb_metadata VALUES (13)",
            "DELETE FROM briskdb_routing",
            "DELETE FROM briskdb_virtual_buckets WHERE bucket_id = 4095",
            "DELETE FROM briskdb_physical_shards WHERE shard_id = 3",
            "UPDATE briskdb_virtual_buckets
             SET physical_shard_id = 1
             WHERE bucket_id = 0",
            "CREATE TABLE unexpected (value TEXT)",
            "DROP TABLE briskdb_manifest;
             CREATE TABLE briskdb_manifest (
                singleton INTEGER PRIMARY KEY,
                shard_count INTEGER NOT NULL
             ) STRICT;
             INSERT INTO briskdb_manifest VALUES (1, 4);",
            "DROP TABLE briskdb_metadata;
             CREATE TABLE briskdb_metadata (
                requires_manifest_version INTEGER NOT NULL
             ) STRICT;
             INSERT INTO briskdb_metadata VALUES (3);",
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            load_or_create(&mut connection, 4).unwrap();
            connection
                .execute_batch("PRAGMA foreign_keys = OFF;")
                .unwrap();
            connection.execute_batch(mutation).unwrap();
            let error = load_or_create(&mut connection, 4).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        }

        let mut missing_identity = Connection::open_in_memory().unwrap();
        load_or_create(&mut missing_identity, 4).unwrap();
        missing_identity
            .pragma_update(None, "application_id", 0)
            .unwrap();
        let error = load_or_create(&mut missing_identity, 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);

        let mut missing_version = Connection::open_in_memory().unwrap();
        load_or_create(&mut missing_version, 4).unwrap();
        missing_version
            .pragma_update(None, "user_version", 0)
            .unwrap();
        let error = load_or_create(&mut missing_version, 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(identity(&missing_version), (MANIFEST_APPLICATION_ID, 0));

        let mut mismatched_legacy = Connection::open_in_memory().unwrap();
        create_legacy_manifest(&mismatched_legacy, 4, 0);
        mismatched_legacy
            .pragma_update(None, "application_id", MANIFEST_APPLICATION_ID)
            .unwrap();
        mismatched_legacy
            .pragma_update(None, "user_version", V2_SCHEMA_VERSION)
            .unwrap();
        let error = load_or_create(&mut mismatched_legacy, 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(
            schema_objects(&mismatched_legacy).unwrap(),
            legacy_objects()
        );
    }

    #[test]
    fn rejects_catalog_values_that_bypass_sql_constraints() {
        for mutation in [
            "UPDATE briskdb_routing SET singleton = 2",
            "UPDATE briskdb_routing SET hash_version = 2",
            "UPDATE briskdb_routing SET key_encoding_version = 2",
            "UPDATE briskdb_routing SET bucket_algorithm_version = 2",
            "UPDATE briskdb_routing SET virtual_bucket_count = 4095",
            "UPDATE briskdb_routing SET map_generation = 0",
            "UPDATE briskdb_physical_shards
             SET lifecycle_state = 'retired'
             WHERE shard_id = 0",
            "UPDATE briskdb_physical_shards SET shard_id = 7 WHERE shard_id = 3",
            "UPDATE briskdb_virtual_buckets
             SET physical_shard_id = 63
             WHERE bucket_id = 0",
            "UPDATE briskdb_virtual_buckets SET bucket_id = 4096 WHERE bucket_id = 4095",
            "INSERT INTO briskdb_virtual_buckets VALUES (4096, 0)",
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            load_or_create(&mut connection, 4).unwrap();
            connection
                .execute_batch(
                    "PRAGMA foreign_keys = OFF;
                     PRAGMA ignore_check_constraints = ON;",
                )
                .unwrap();
            connection.execute_batch(mutation).unwrap();
            connection
                .execute_batch("PRAGMA ignore_check_constraints = OFF;")
                .unwrap();

            let error = load_or_create(&mut connection, 4).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption, "{mutation}");
        }
    }

    #[test]
    fn rejects_invalid_logical_catalog_values_when_sql_checks_are_bypassed() {
        for mutation in [
            "UPDATE briskdb_schema_catalog SET singleton = 2",
            "UPDATE briskdb_schema_catalog SET identifier_encoding_version = 2",
            "UPDATE briskdb_schema_catalog SET schema_generation = 1",
            "UPDATE briskdb_schema_catalog SET default_database_id = 2",
            "DELETE FROM briskdb_logical_databases WHERE database_id = 1",
            "UPDATE briskdb_logical_databases SET database_name = 'Default'",
            "UPDATE briskdb_logical_databases SET database_name = 'a' || char(0) || 'UPPER'",
            "UPDATE briskdb_logical_databases SET database_name = 'briskdb_internal'",
            "INSERT INTO briskdb_logical_databases VALUES (0, 'zero')",
            "INSERT INTO briskdb_logical_databases VALUES (2, CAST(x'80' AS TEXT))",
            "INSERT INTO briskdb_tables VALUES (0, 1, 'widgets', 2, NULL, NULL)",
            "INSERT INTO briskdb_tables VALUES (1, 9, 'widgets', 2, NULL, NULL)",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'Widgets', 2, NULL, NULL)",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'a' || char(0) || 'UPPER', 2, NULL, NULL)",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'widgets', 4, NULL, NULL)",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'widgets', 1, NULL, NULL)",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'widgets', 2, 'tenant_id', 2)",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'widgets', 1, 'TenantId', 2)",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'widgets', 1, 'a' || char(0) || 'UPPER', 2)",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'widgets', 1, 'tenant_id', 4)",
            "INSERT INTO briskdb_tables VALUES (1, 1, CAST(x'80' AS TEXT), 2, NULL, NULL)",
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            load_or_create(&mut connection, 4).unwrap();
            connection
                .execute_batch(
                    "PRAGMA foreign_keys = OFF;
                     PRAGMA ignore_check_constraints = ON;",
                )
                .unwrap();
            connection.execute_batch(mutation).unwrap();
            connection
                .execute_batch("PRAGMA ignore_check_constraints = OFF;")
                .unwrap();

            let error = load_or_create(&mut connection, 4).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption, "{mutation}");
        }
    }

    #[test]
    fn logical_database_and_table_catalog_limits_are_exact_and_bounded() {
        let mut databases = Connection::open_in_memory().unwrap();
        load_or_create(&mut databases, 4).unwrap();
        {
            let transaction = databases.transaction().unwrap();
            let mut insert = transaction
                .prepare(
                    "INSERT INTO briskdb_logical_databases (database_id, database_name)
                     VALUES (?1, ?2)",
                )
                .unwrap();
            for id in 2..=MAX_LOGICAL_DATABASES {
                insert
                    .execute(rusqlite::params![
                        i64::try_from(id).unwrap(),
                        format!("db_{id:02}")
                    ])
                    .unwrap();
            }
            drop(insert);
            transaction.commit().unwrap();
        }
        refresh_manifest_digest(&databases).unwrap();
        assert_eq!(
            load_or_create_catalog(&mut databases, 4)
                .unwrap()
                .logical()
                .logical_databases()
                .len(),
            MAX_LOGICAL_DATABASES
        );
        databases
            .execute(
                "INSERT INTO briskdb_logical_databases (database_id, database_name)
                 VALUES (?1, ?2)",
                rusqlite::params![
                    i64::try_from(MAX_LOGICAL_DATABASES + 1).unwrap(),
                    "one_too_many"
                ],
            )
            .unwrap();
        assert_eq!(
            load_or_create(&mut databases, 4).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );

        let mut tables = Connection::open_in_memory().unwrap();
        load_or_create(&mut tables, 4).unwrap();
        {
            let transaction = tables.transaction().unwrap();
            let mut insert = transaction
                .prepare(
                    "INSERT INTO briskdb_tables (
                        table_id,
                        database_id,
                        table_name,
                        placement,
                        shard_key_column,
                        shard_key_type
                     ) VALUES (?1, 1, ?2, 2, NULL, NULL)",
                )
                .unwrap();
            for id in 1..=MAX_TABLES {
                insert
                    .execute(rusqlite::params![
                        i64::try_from(id).unwrap(),
                        format!("table_{id:04}")
                    ])
                    .unwrap();
            }
            drop(insert);
            transaction
                .execute(
                    "INSERT INTO briskdb_generated_ids (
                        table_id,
                        policy,
                        generated_column,
                        encoding_version,
                        activation_state
                     )
                     SELECT table_id, 0, NULL, NULL, 0
                     FROM briskdb_tables
                     ORDER BY table_id",
                    [],
                )
                .unwrap();
            transaction.commit().unwrap();
        }
        refresh_manifest_digest(&tables).unwrap();
        assert_eq!(
            load_or_create_catalog(&mut tables, 4)
                .unwrap()
                .logical()
                .tables()
                .len(),
            MAX_TABLES
        );
        tables
            .execute(
                "INSERT INTO briskdb_tables VALUES (?1, 1, ?2, 2, NULL, NULL)",
                rusqlite::params![i64::try_from(MAX_TABLES + 1).unwrap(), "one_too_many"],
            )
            .unwrap();
        assert_eq!(
            load_or_create(&mut tables, 4).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn rejects_altered_catalog_table_definitions() {
        for mutation in [
            "DROP TABLE briskdb_routing;
             CREATE TABLE briskdb_routing (
                singleton INTEGER PRIMARY KEY,
                hash_version INTEGER NOT NULL,
                key_encoding_version INTEGER NOT NULL,
                virtual_bucket_count INTEGER NOT NULL,
                map_generation INTEGER NOT NULL
             ) STRICT;
             INSERT INTO briskdb_routing VALUES (1, 1, 1, 4096, 1);",
            "DROP TABLE briskdb_virtual_buckets;
             CREATE TABLE briskdb_virtual_buckets (
                bucket_id INTEGER PRIMARY KEY,
                physical_shard_id INTEGER NOT NULL
             ) STRICT;",
            "DROP TABLE briskdb_physical_shards;
             CREATE TABLE briskdb_physical_shards (
                shard_id INTEGER PRIMARY KEY,
                lifecycle_state TEXT NOT NULL
             ) STRICT;",
            "DROP TABLE briskdb_logical_databases;
             CREATE TABLE briskdb_logical_databases (
                database_id INTEGER PRIMARY KEY,
                database_name TEXT NOT NULL UNIQUE
             ) STRICT;",
            "DROP TABLE briskdb_schema_catalog;
             CREATE TABLE briskdb_schema_catalog (
                singleton INTEGER PRIMARY KEY,
                identifier_encoding_version INTEGER NOT NULL,
                schema_generation INTEGER NOT NULL,
                default_database_id INTEGER NOT NULL
             ) STRICT;",
            "DROP TABLE briskdb_tables;
             CREATE TABLE briskdb_tables (
                table_id INTEGER PRIMARY KEY,
                database_id INTEGER NOT NULL,
                table_name TEXT NOT NULL,
                placement INTEGER NOT NULL,
                shard_key_column TEXT,
                shard_key_type INTEGER
             ) STRICT;",
            "DROP TABLE briskdb_shard_layout;
             CREATE TABLE briskdb_shard_layout (
                singleton INTEGER PRIMARY KEY,
                layout_id BLOB NOT NULL,
                shard_application_id INTEGER NOT NULL,
                shard_metadata_version INTEGER NOT NULL,
                layout_state INTEGER NOT NULL
             ) STRICT;
             INSERT INTO briskdb_shard_layout
             VALUES (1, randomblob(16), 1112691528, 1, 1);",
            "DROP TABLE briskdb_schema_migrations;
             CREATE TABLE briskdb_schema_migrations (
                target_generation INTEGER PRIMARY KEY,
                source_generation INTEGER NOT NULL,
                migration_id BLOB NOT NULL,
                digest_version INTEGER NOT NULL,
                sql_text TEXT NOT NULL,
                shard_count INTEGER NOT NULL,
                migration_state INTEGER NOT NULL,
                next_shard INTEGER NOT NULL
             ) STRICT;",
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            load_or_create(&mut connection, 4).unwrap();
            connection
                .execute_batch("PRAGMA foreign_keys = OFF;")
                .unwrap();
            connection.execute_batch(mutation).unwrap();

            let error = load_or_create(&mut connection, 4).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption, "{mutation}");
        }
    }

    #[test]
    fn unrecognized_unversioned_sqlite_database_is_not_adopted() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch("CREATE TABLE unrelated (value TEXT);")
            .unwrap();

        let error = load_or_create(&mut connection, 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&connection), (0, 0));
        assert_eq!(schema_objects(&connection).unwrap()[0].name, "unrelated");
    }

    fn generated_events_declaration() -> TableDeclaration {
        let database = LogicalDatabaseId::new(DEFAULT_LOGICAL_DATABASE_ID).unwrap();
        TableDeclaration::sharded(
            database,
            "events",
            ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
        )
        .unwrap()
        .with_generated_id_policy(GeneratedIdPolicy::native_range_v1("id").unwrap())
        .unwrap()
    }

    fn create_ready_v11_manifest(connection: &mut Connection, shards: u16) {
        let transaction = connection.transaction().unwrap();
        create_v11_schema(&transaction, shards).unwrap();
        transaction
            .execute(
                "UPDATE briskdb_shard_layout SET layout_state = ?1 WHERE singleton = 1",
                [ShardLayoutState::Ready.code()],
            )
            .unwrap();
        transaction
            .execute(
                "UPDATE briskdb_integrity
                 SET database_state = ?1,
                     committed_schema_digest = ?2,
                     target_schema_digest = NULL
                 WHERE singleton = 1",
                rusqlite::params![DATABASE_STATE_READY, [0x5a_u8; 32].as_slice()],
            )
            .unwrap();
        set_identity(&transaction, V11_SCHEMA_VERSION).unwrap();
        refresh_manifest_digest(&transaction).unwrap();
        validate_v11(&transaction, shards, &schema_objects(&transaction).unwrap()).unwrap();
        transaction.commit().unwrap();
    }

    #[test]
    fn v11_to_v12_upgrade_is_atomic_checksummed_and_downgrade_fenced() {
        const V11_PLAN: MigrationPlan<'static> = MigrationPlan {
            current_version: V11_SCHEMA_VERSION,
            migrations: MIGRATIONS,
            initialize_current: create_v11_schema,
            initialize_interrupted_legacy: migrate_interrupted_legacy_to_v11,
        };
        for phase in [
            MigrationPhase::AfterSchemaChange,
            MigrationPhase::AfterVersionStamp,
        ] {
            let mut interrupted = Connection::open_in_memory().unwrap();
            create_ready_v11_manifest(&mut interrupted, 4);
            let root = stored_manifest_digest(&interrupted);
            let error = load_or_create_with_hook(&mut interrupted, 4, |point| {
                if point.from == V11_SCHEMA_VERSION && point.phase == phase {
                    Err(EngineError::new(
                        EngineErrorKind::Internal,
                        "injected v11 to v12 failure",
                    ))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Internal);
            assert_eq!(identity(&interrupted).1, i64::from(V11_SCHEMA_VERSION));
            assert_eq!(schema_objects(&interrupted).unwrap(), v11_objects());
            assert_eq!(stored_manifest_digest(&interrupted), root);
        }

        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_v11_manifest(&mut connection, 4);
        load_or_create_manifest(&mut connection, 4).unwrap();
        assert_eq!(identity(&connection).1, i64::from(V12_SCHEMA_VERSION));
        assert_eq!(schema_objects(&connection).unwrap(), v12_objects());
        assert_eq!(
            connection
                .query_row(
                    "SELECT requires_manifest_version FROM briskdb_metadata",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            i64::from(V12_SCHEMA_VERSION)
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT manifest_digest_version FROM briskdb_integrity",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            i64::from(V5_MANIFEST_DIGEST_VERSION)
        );
        assert_eq!(
            manifest_semantic_digest(&connection).unwrap(),
            stored_manifest_digest(&connection)
        );
        assert_eq!(
            inspect_with_plan(&connection, 4, V11_PLAN)
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
    }

    #[test]
    fn generated_table_ddl_bridge_is_exact_checksummed_and_lifecycle_complete() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let source = "CREATE TABLE events (id BIGSERIAL PRIMARY KEY, payload TEXT NOT NULL)";
        let physical =
            "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL)";
        let declaration = generated_events_declaration();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let (ddl, mut migration) = begin_generated_table_ddl_in_transaction(
            &transaction,
            4,
            0,
            SqlDialect::PostgreSql,
            source,
            physical,
            declaration.clone(),
            [0x5a; 32],
            [0x6b; 32],
        )
        .unwrap();
        assert_eq!(
            ddl.lifecycle(),
            GeneratedTableDdlLifecycle::ApplyingPhysical
        );
        assert_ne!(ddl.logical_id(), ddl.physical_migration_id());
        transaction.commit().unwrap();

        assert!(matches!(
            classify_generated_table_ddl(
                &mut connection,
                4,
                SqlDialect::PostgreSql,
                source,
                physical,
                &declaration,
            )
            .unwrap(),
            GeneratedTableDdlClassification::Existing(_)
        ));
        let conflict = classify_generated_table_ddl(
            &mut connection,
            4,
            SqlDialect::MySql,
            source,
            physical,
            &declaration,
        )
        .unwrap_err();
        assert_eq!(conflict.kind(), EngineErrorKind::FailedPrecondition);

        while migration.next_shard() < migration.shard_count() {
            let next = migration.next_shard() + 1;
            migration = advance_schema_migration(&mut connection, 4, &migration, next).unwrap();
        }
        migration = finalize_schema_migration(&mut connection, 4, &migration).unwrap();
        assert!(migration.is_complete());
        let ddl = mark_generated_table_ddl_provisioning(&mut connection, 4, &ddl, || {}).unwrap();
        assert_eq!(ddl.lifecycle(), GeneratedTableDdlLifecycle::Provisioning);
        let provisioning_id = ddl.provisioning_id().unwrap();
        assert_eq!(ddl.provisioning_schema_digest(), Some([0x6b; 32]));

        let active = match begin_native_table_provisioning(
            &mut connection,
            4,
            vec![declaration.clone()],
            [0x6b; 32],
            || {},
        )
        .unwrap()
        {
            NativeTableProvisioningClassification::Active(active) => active,
            other => panic!("expected active provisioning, got {other:?}"),
        };
        assert_eq!(active.provisioning_id(), provisioning_id);
        let mut active = active;
        while active.next_shard() < active.shard_count() {
            let next = active.next_shard() + 1;
            active = advance_native_table_provisioning(&mut connection, 4, &active, next).unwrap();
        }
        let (catalog, completed) =
            finalize_generated_table_ddl_provisioning(&mut connection, 4, &ddl, &active, || {})
                .unwrap();
        assert_eq!(completed.lifecycle(), GeneratedTableDdlLifecycle::Complete);
        assert_eq!(completed.provisioning_id(), Some(provisioning_id));
        assert_eq!(completed.provisioning_schema_digest(), Some([0x6b; 32]));
        assert_eq!(
            completed.table_id(),
            Some(catalog.logical().tables()[0].id())
        );
        assert_eq!(completed.source_dialect(), SqlDialect::PostgreSql);
        assert_eq!(
            completed.translation_version(),
            GENERATED_TABLE_DDL_TRANSLATION_VERSION
        );
        assert_eq!(completed.source_sql(), source);
        assert_eq!(completed.physical_sql(), physical);
        assert_eq!(completed.declaration(), &declaration);
        assert_eq!(
            manifest_semantic_digest(&connection).unwrap(),
            stored_manifest_digest(&connection)
        );

        let reopened = load_or_create_manifest(&mut connection, 4).unwrap();
        assert_eq!(reopened.generated_table_ddl(), Some(&completed));
    }

    #[test]
    fn generated_table_ddl_bridge_schema_and_checksum_fail_closed() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut connection, 4);
        let source = "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT)";
        let physical = source;
        let declaration = generated_events_declaration();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        begin_generated_table_ddl_in_transaction(
            &transaction,
            4,
            0,
            SqlDialect::Sqlite,
            source,
            physical,
            declaration,
            [0x5a; 32],
            [0x6b; 32],
        )
        .unwrap();
        transaction.commit().unwrap();

        connection
            .execute(
                "UPDATE briskdb_generated_table_ddl SET source_sql = source_sql || ' '",
                [],
            )
            .unwrap();
        let error = load_or_create_manifest(&mut connection, 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);

        let mut schema = Connection::open_in_memory().unwrap();
        create_ready_current_manifest(&mut schema, 4);
        schema.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        schema
            .execute_batch(
                "ALTER TABLE briskdb_generated_table_ddl
                 RENAME TO briskdb_generated_table_ddl_old;
                 CREATE TABLE briskdb_generated_table_ddl (singleton INTEGER PRIMARY KEY) STRICT;
                 DROP TABLE briskdb_generated_table_ddl_old;",
            )
            .unwrap();
        let error = load_or_create_manifest(&mut schema, 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
    }
}
