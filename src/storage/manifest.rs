//! Version detection and transactional upgrades for `manifest.sqlite`.

use rusqlite::{Connection, Transaction, TransactionBehavior, types::ValueRef};

#[cfg(test)]
use std::cell::Cell;

use crate::{
    core::{
        BUCKET_ALGORITHM_VERSION, Catalog, CatalogSnapshot, DEFAULT_LOGICAL_DATABASE_ID,
        DEFAULT_LOGICAL_DATABASE_NAME, EngineError, EngineErrorKind, EngineResult, HASH_VERSION,
        IDENTIFIER_ENCODING_VERSION, INITIAL_MAP_GENERATION, KEY_ENCODING_VERSION,
        LogicalDatabaseMetadata, MAX_LOGICAL_DATABASES, MAX_TABLES, RoutingCatalog,
        ShardKeyMetadata, ShardKeyType, TableDeclaration, TableMetadata, TablePlacement,
        VIRTUAL_BUCKET_COUNT, initial_physical_shard, validate_catalog_identifier,
    },
    sqlite_error,
};

use super::shard::{SHARD_APPLICATION_ID, SHARD_METADATA_VERSION, ShardLayout, ShardLayoutState};

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
pub(super) const CURRENT_SCHEMA_VERSION: u32 = V8_SCHEMA_VERSION;
const MAX_TABLE_SQL_BYTES: i64 = 4_096;

pub(super) const MAX_SCHEMA_MIGRATION_SQL_BYTES: usize = 65_536;
pub(super) const MAX_SCHEMA_GENERATION: u64 = i32::MAX as u64;
const SCHEMA_MIGRATION_DIGEST_VERSION: u32 = 1;
const SCHEMA_MIGRATION_APPLYING: i64 = 1;
const SCHEMA_MIGRATION_COMPLETE: i64 = 2;
const MANIFEST_DIGEST_VERSION: u32 = 1;
pub(super) const SCHEMA_DIGEST_VERSION: u32 = 1;
const MANIFEST_DIGEST_DOMAIN: &[u8] = b"briskdb.manifest.semantic-root.v1\0";

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LoadedManifest {
    catalog: CatalogSnapshot,
    shard_layout: ShardLayout,
    active_migration: Option<SchemaMigration>,
    integrity: ManifestIntegrity,
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
    initialize_current: create_v8_schema,
    initialize_interrupted_legacy: migrate_interrupted_legacy_to_v8,
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
    let snapshot = load_or_create_snapshot_with_plan(
        connection,
        requested_shards,
        CURRENT_PLAN,
        fresh_layout_allowed,
        &mut |_| Ok(()),
    )?;
    let routing = snapshot.routing_catalog.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "current manifest validation did not produce a routing catalog",
        )
    })?;
    let logical = snapshot.logical_catalog.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "current manifest validation did not produce a logical catalog",
        )
    })?;
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
        catalog: CatalogSnapshot::from_validated_parts(routing, logical),
        shard_layout,
        active_migration: snapshot.active_migration,
        integrity,
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
    refresh_manifest_digest_if_v7(transaction)?;
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

/// Atomically install the complete authoritative table catalog.
///
/// Physical-table and emptiness validation belongs to the storage coordinator,
/// which holds the root schema gate before entering this manifest transaction.
/// This layer revalidates the checksummed v8 manifest under its write lock,
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
    ensure_table_registration_ready(&current)?;
    let current_catalog = current.logical_catalog.as_ref().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "current manifest validation omitted its logical catalog",
        )
    })?;
    for (database_id, table_name, _) in &declarations {
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
        for (index, (database_id, table_name, placement)) in declarations.iter().enumerate() {
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
    let routing = snapshot.routing_catalog.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "current manifest validation omitted its routing catalog",
        )
    })?;
    let logical = snapshot.logical_catalog.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "current manifest validation omitted its logical catalog",
        )
    })?;
    Ok(CatalogSnapshot::from_validated_parts(routing, logical))
}

fn declarations_match_catalog(
    declarations: &[(crate::core::LogicalDatabaseId, String, TablePlacement)],
    catalog: &Catalog,
) -> bool {
    declarations.len() == catalog.tables().len()
        && declarations.iter().zip(catalog.tables()).all(
            |((database_id, name, placement), table)| {
                *database_id == table.database_id()
                    && name == table.name()
                    && placement == table.placement()
            },
        )
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
        refresh_manifest_digest_if_v7(&transaction)?;

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
                MANIFEST_DIGEST_VERSION,
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

fn validate_integrity_manifest(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
    version: u32,
    downgrade_fence_sql: &str,
) -> EngineResult<ManifestSnapshot> {
    let mut snapshot = validate_catalog_manifest(
        connection,
        requested_shards,
        objects,
        CatalogManifestDefinition {
            version,
            downgrade_fence_sql,
            expected_objects: &v7_objects(),
            schema_catalog_sql: V6_SCHEMA_CATALOG_TABLE_SQL,
            generation_policy: SchemaGenerationPolicy::Journaled,
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
    let integrity = validate_manifest_integrity(connection, &layout, active.as_ref())?;
    snapshot.shard_layout = Some(layout);
    snapshot.active_migration = active;
    snapshot.integrity = Some(integrity);
    Ok(snapshot)
}

struct ManifestDigestQuery {
    table: &'static str,
    columns: &'static [&'static str],
    sql: &'static str,
}

const MANIFEST_DIGEST_QUERIES: &[ManifestDigestQuery] = &[
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

fn manifest_semantic_digest(connection: &Connection) -> EngineResult<[u8; 32]> {
    let (application_id, user_version) = read_identity(connection)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(MANIFEST_DIGEST_DOMAIN);
    hash_manifest_name(&mut hasher, b"application_id");
    hash_manifest_value(&mut hasher, ValueRef::Integer(application_id))?;
    hash_manifest_name(&mut hasher, b"user_version");
    hash_manifest_value(&mut hasher, ValueRef::Integer(user_version))?;

    for query in MANIFEST_DIGEST_QUERIES {
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

fn refresh_manifest_digest_if_v7(connection: &Connection) -> EngineResult<()> {
    let (application_id, version) = read_identity(connection)?;
    if application_id == MANIFEST_APPLICATION_ID
        && matches!(
            u32::try_from(version),
            Ok(V7_SCHEMA_VERSION | V8_SCHEMA_VERSION)
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
    if *manifest_version > i64::from(MANIFEST_DIGEST_VERSION) {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "manifest checksum version is newer than this BriskDB build supports",
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
    if manifest_semantic_digest(connection)? != stored_root {
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

    let catalog_configuration =
        validate_schema_catalog_configuration(connection, definition.generation_policy)?;
    let databases =
        validate_logical_databases(connection, catalog_configuration.default_database_id)?;
    let tables = validate_table_metadata(connection, &databases)?;
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
) -> EngineResult<Box<[TableMetadata]>> {
    let mut statement = connection
        .prepare(
            "SELECT table_id,
                    database_id,
                    table_name,
                    placement,
                    shard_key_column,
                    shard_key_type
             FROM briskdb_tables
             ORDER BY database_id, table_name, table_id
             LIMIT ?1",
        )
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
    for (stored_table_id, stored_database_id, name, placement, column, key_type) in rows {
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
        tables.push(TableMetadata::from_validated(
            table_id,
            database_id,
            name,
            placement,
        ));
    }

    Ok(tables.into_boxed_slice())
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

    use super::*;

    type StoredTableMetadataRow = (i64, i64, String, i64, Option<String>, Option<i64>);

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
        refresh_manifest_digest_if_v7(connection).unwrap();
    }

    fn assert_generation_one_catalog(connection: &Connection, shard_count: u16) {
        assert_eq!(
            identity(connection),
            (MANIFEST_APPLICATION_ID, i64::from(CURRENT_SCHEMA_VERSION))
        );
        assert_eq!(schema_objects(connection).unwrap(), v7_objects());
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
            [(2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8)]
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

        let loaded = load_or_create_catalog(&mut connection, 4).unwrap();
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
                    load_or_create_with_hook(&mut connection, 4, |point| {
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

                let loaded = load_or_create_catalog(&mut connection, 4).unwrap();
                assert!(loaded.logical().tables().is_empty());
                assert_eq!(
                    identity(&connection),
                    (MANIFEST_APPLICATION_ID, i64::from(V8_SCHEMA_VERSION))
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
            [(1, "accounts"), (2, "countries"), (3, "internal_catalog")]
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
                    "internal_catalog".to_owned(),
                    CATALOG_PLACEMENT,
                    None,
                    None,
                ),
            ]
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
    fn v7_integrity_root_is_deterministic_across_reopen_checkpoint_and_vacuum() {
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
        create_ready_current_manifest(&mut connection, 4);
        connection
            .execute(
                "UPDATE briskdb_shard_layout
                 SET layout_id = x'000102030405060708090a0b0c0d0e0f'
                 WHERE singleton = 1",
                [],
            )
            .unwrap();
        insert_valid_table_catalog(&connection);
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
    fn manifest_semantic_digest_orders_catalog_rows_by_frozen_keys() {
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
                 INSERT INTO briskdb_tables VALUES (3, 1, 'accounts', 1, 'tenant_id', 2);",
            )
            .unwrap();

        assert_eq!(
            refresh_manifest_digest(&forward).unwrap(),
            refresh_manifest_digest(&reverse).unwrap()
        );
    }

    #[test]
    fn semantic_root_covers_every_authoritative_manifest_table_and_integrity_state() {
        let mutations = [
            "UPDATE briskdb_manifest SET singleton = 2 WHERE singleton = 1",
            "UPDATE briskdb_metadata SET requires_manifest_version = 9",
            "UPDATE briskdb_routing SET hash_version = 2 WHERE singleton = 1",
            "UPDATE briskdb_physical_shards SET lifecycle_state = 'retired' WHERE shard_id = 0",
            "UPDATE briskdb_virtual_buckets SET physical_shard_id = 1 WHERE bucket_id = 0",
            "UPDATE briskdb_logical_databases SET database_name = 'primary' WHERE database_id = 1",
            "UPDATE briskdb_schema_catalog SET identifier_encoding_version = 2 WHERE singleton = 1",
            "INSERT INTO briskdb_tables VALUES (1, 1, 'widgets', 2, NULL, NULL)",
            "UPDATE briskdb_shard_layout SET layout_id = randomblob(16) WHERE singleton = 1",
            "INSERT INTO briskdb_schema_migrations VALUES (1, 0, randomblob(32), 1, 'SELECT 1', 4, 2, 4)",
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
    fn integrity_versions_lengths_and_forged_state_invariants_fail_closed() {
        for version_column in ["manifest_digest_version", "schema_digest_version"] {
            let mut unsupported = Connection::open_in_memory().unwrap();
            create_ready_current_manifest(&mut unsupported, 4);
            unsupported
                .execute(
                    &format!(
                        "UPDATE briskdb_integrity SET {version_column} = 2 WHERE singleton = 1"
                    ),
                    [],
                )
                .unwrap();
            refresh_manifest_digest(&unsupported).unwrap();
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
            refresh_manifest_digest(&invalid).unwrap();
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
        assert_eq!(schema_objects(&connection).unwrap(), v7_objects());
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
        assert_eq!(schema_objects(&connection).unwrap(), v7_objects());
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
            assert_eq!(schema_objects(&connection).unwrap(), v7_objects());
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
            "INSERT INTO briskdb_metadata VALUES (8)",
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
}
