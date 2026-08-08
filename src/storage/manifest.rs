//! Version detection and transactional upgrades for `manifest.sqlite`.

use rusqlite::{Connection, Transaction, TransactionBehavior};

use crate::{
    core::{EngineError, EngineErrorKind, EngineResult},
    sqlite_error,
};

/// `BRDB` encoded as SQLite's 32-bit application identifier.
pub(super) const MANIFEST_APPLICATION_ID: i64 = 0x4252_4442;
const LEGACY_SCHEMA_VERSION: u32 = 1;
const V2_SCHEMA_VERSION: u32 = 2;
const V3_SCHEMA_VERSION: u32 = 3;
pub(super) const CURRENT_SCHEMA_VERSION: u32 = V3_SCHEMA_VERSION;
const MAX_TABLE_SQL_BYTES: i64 = 4_096;

pub(super) const HASH_VERSION: u32 = 1;
pub(super) const KEY_ENCODING_VERSION: u32 = 1;
pub(super) const BUCKET_ALGORITHM_VERSION: u32 = 1;
pub(super) const VIRTUAL_BUCKET_COUNT: u16 = 4_096;
pub(super) const INITIAL_MAP_GENERATION: u64 = 1;

const ACTIVE_LIFECYCLE_STATE: &str = "active";

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

#[derive(Clone, Copy)]
struct Migration {
    from: u32,
    to: u32,
    name: &'static str,
    apply: fn(&Transaction<'_>, u16) -> EngineResult<()>,
    validate: fn(&Connection, u16, &[SchemaObject]) -> EngineResult<u16>,
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
];

#[derive(Clone, Copy)]
struct MigrationPlan<'a> {
    current_version: u32,
    migrations: &'a [Migration],
    initialize_current: fn(&Transaction<'_>, u16) -> EngineResult<()>,
}

const CURRENT_PLAN: MigrationPlan<'static> = MigrationPlan {
    current_version: CURRENT_SCHEMA_VERSION,
    migrations: MIGRATIONS,
    initialize_current: create_v3_schema,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManifestState {
    Empty,
    LegacyUninitialized,
    LegacyV1 { shard_count: u16 },
    Versioned { version: u32, shard_count: u16 },
}

/// Initialize or advance the manifest under an immediate transaction.
///
/// The state is inspected again after the write lock is acquired, so two
/// concurrent openers cannot both act on a stale version. Each numbered
/// migration owns its transaction and stamps the new version last.
pub(super) fn load_or_create(
    connection: &mut Connection,
    requested_shards: u16,
) -> EngineResult<u16> {
    load_or_create_with_hook(connection, requested_shards, |_| Ok(()))
}

fn load_or_create_with_hook<F>(
    connection: &mut Connection,
    requested_shards: u16,
    mut hook: F,
) -> EngineResult<u16>
where
    F: FnMut(MigrationPoint) -> EngineResult<()>,
{
    load_or_create_with_plan(connection, requested_shards, CURRENT_PLAN, &mut hook)
}

fn load_or_create_with_plan<F>(
    connection: &mut Connection,
    requested_shards: u16,
    plan: MigrationPlan<'_>,
    hook: &mut F,
) -> EngineResult<u16>
where
    F: FnMut(MigrationPoint) -> EngineResult<()>,
{
    loop {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error::storage)?;

        let (from, shard_count) = match inspect_with_plan(&transaction, requested_shards, plan)? {
            ManifestState::Versioned {
                version,
                shard_count,
            } if version == plan.current_version => {
                transaction.commit().map_err(sqlite_error::storage)?;
                return Ok(shard_count);
            }
            ManifestState::Empty => {
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
            ManifestState::LegacyUninitialized => (LEGACY_SCHEMA_VERSION, requested_shards),
            ManifestState::LegacyV1 { shard_count } => (LEGACY_SCHEMA_VERSION, shard_count),
            ManifestState::Versioned {
                version,
                shard_count,
            } => (version, shard_count),
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
) -> EngineResult<u16>
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
    hook(MigrationPoint {
        from: change.from,
        to: change.to,
        phase: MigrationPhase::AfterVersionStamp,
    })?;

    match inspect_with_plan(&transaction, shard_count, plan)? {
        ManifestState::Versioned {
            version,
            shard_count: validated,
        } if version == change.to => {
            transaction.commit().map_err(sqlite_error::storage)?;
            Ok(validated)
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

/// Partition the virtual bucket space into deterministic contiguous ranges.
///
/// Hash version 1 will choose a range using the legacy `hash % shard_count`
/// result, then choose a bucket within that range. Consequently, activating
/// catalog lookup can preserve every existing placement even when the shard
/// count does not divide 4,096.
fn initial_physical_shard(bucket_id: u16, shard_count: u16) -> u16 {
    debug_assert!((2..=64).contains(&shard_count));
    debug_assert!(bucket_id < VIRTUAL_BUCKET_COUNT);

    let bucket_count = u32::from(VIRTUAL_BUCKET_COUNT);
    let shard_count = u32::from(shard_count);
    let bucket_id = u32::from(bucket_id);
    let base_size = bucket_count / shard_count;
    let wider_shards = bucket_count % shard_count;
    let wider_span = (base_size + 1) * wider_shards;
    let shard = if bucket_id < wider_span {
        bucket_id / (base_size + 1)
    } else {
        wider_shards + (bucket_id - wider_span) / base_size
    };
    u16::try_from(shard).expect("a virtual bucket maps to a supported shard")
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
        return validator(connection, requested_shards, &objects).map(|shard_count| {
            ManifestState::Versioned {
                version,
                shard_count,
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
) -> EngineResult<u16> {
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
    Ok(shard_count)
}

fn validate_v3(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
) -> EngineResult<u16> {
    if objects != v3_objects() {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest schema version 3 has unexpected database objects",
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
    validate_table_sql(connection, "briskdb_metadata", V3_DOWNGRADE_FENCE_SQL)?;
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
    validate_downgrade_fence(connection, V3_SCHEMA_VERSION)?;
    validate_routing_configuration(connection)?;
    validate_physical_shards(connection, shard_count)?;
    validate_virtual_buckets(connection, shard_count)?;
    validate_foreign_keys(connection)?;
    Ok(shard_count)
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

fn validate_routing_configuration(connection: &Connection) -> EngineResult<()> {
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
    Ok(())
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

fn validate_virtual_buckets(connection: &Connection, shard_count: u16) -> EngineResult<()> {
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
    }
    if let Some(unassigned) = assignments.iter().position(|count| *count == 0) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("active physical shard {unassigned} has no virtual buckets"),
        ));
    }
    Ok(())
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
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{Arc, Barrier, mpsc},
        thread,
        time::Duration,
    };

    use rusqlite::OptionalExtension;

    use super::*;

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

    fn assert_generation_one_catalog(connection: &Connection, shard_count: u16) {
        assert_eq!(
            identity(connection),
            (MANIFEST_APPLICATION_ID, i64::from(CURRENT_SCHEMA_VERSION))
        );
        assert_eq!(schema_objects(connection).unwrap(), v3_objects());
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
        assert_eq!(resumed_steps, [(2, 3)]);
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
    fn fresh_and_v2_upgraded_catalogs_are_identical_for_every_shard_count() {
        for shard_count in 2..=64 {
            let mut fresh = Connection::open_in_memory().unwrap();
            assert_eq!(
                load_or_create(&mut fresh, shard_count).unwrap(),
                shard_count
            );
            assert_generation_one_catalog(&fresh, shard_count);

            let mut upgraded = Connection::open_in_memory().unwrap();
            create_v2_manifest(&mut upgraded, shard_count);
            assert_eq!(
                load_or_create(&mut upgraded, shard_count).unwrap(),
                shard_count
            );
            assert_generation_one_catalog(&upgraded, shard_count);
            assert_eq!(
                routing_configuration(&fresh),
                routing_configuration(&upgraded)
            );
            assert_eq!(physical_shards(&fresh), physical_shards(&upgraded));
            assert_eq!(virtual_buckets(&fresh), virtual_buckets(&upgraded));
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
    fn version_two_reader_rejects_version_three_without_mutating_it() {
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
            "INSERT INTO briskdb_metadata VALUES (3)",
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
