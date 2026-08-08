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
pub(super) const CURRENT_SCHEMA_VERSION: u32 = V2_SCHEMA_VERSION;
const MAX_TABLE_SQL_BYTES: i64 = 4_096;

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

#[derive(Clone, Copy)]
struct Migration {
    from: u32,
    to: u32,
    name: &'static str,
    apply: fn(&Transaction<'_>, u16) -> EngineResult<()>,
    validate: fn(&Connection, u16, &[SchemaObject]) -> EngineResult<u16>,
}

const MIGRATIONS: &[Migration] = &[Migration {
    from: LEGACY_SCHEMA_VERSION,
    to: V2_SCHEMA_VERSION,
    name: "typed_manifest_and_downgrade_fence",
    apply: migrate_v1_to_v2,
    validate: validate_v2,
}];

#[derive(Clone, Copy)]
struct MigrationPlan<'a> {
    current_version: u32,
    migrations: &'a [Migration],
    initialize_current: fn(&Transaction<'_>, u16) -> EngineResult<()>,
}

const CURRENT_PLAN: MigrationPlan<'static> = MigrationPlan {
    current_version: CURRENT_SCHEMA_VERSION,
    migrations: MIGRATIONS,
    initialize_current: create_v2_schema,
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
        #[cfg(test)]
        "briskdb_v3_marker" => {
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_v3_marker') LIMIT ?1"
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

    let mut fence_statement = connection
        .prepare("SELECT requires_manifest_version FROM briskdb_metadata ORDER BY rowid LIMIT 3")
        .map_err(|error| manifest_read_error(error, "failed to read manifest downgrade fence"))?;
    let fence = fence_statement
        .query_map([], |row| row.get::<_, i64>(0))
        .map_err(|error| manifest_read_error(error, "failed to read manifest downgrade fence"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| manifest_read_error(error, "failed to read manifest downgrade fence"))?;
    if fence != [i64::from(V2_SCHEMA_VERSION)] {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest downgrade fence does not match its schema version",
        ));
    }
    Ok(shard_count)
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

    const SYNTHETIC_V3_SCHEMA_VERSION: u32 = 3;
    const SYNTHETIC_MIGRATIONS: &[Migration] = &[
        Migration {
            from: LEGACY_SCHEMA_VERSION,
            to: V2_SCHEMA_VERSION,
            name: "typed_manifest_and_downgrade_fence",
            apply: migrate_v1_to_v2,
            validate: validate_v2,
        },
        Migration {
            from: V2_SCHEMA_VERSION,
            to: SYNTHETIC_V3_SCHEMA_VERSION,
            name: "synthetic_v3",
            apply: migrate_v2_to_synthetic_v3,
            validate: validate_synthetic_v3,
        },
    ];
    const SYNTHETIC_PLAN: MigrationPlan<'static> = MigrationPlan {
        current_version: SYNTHETIC_V3_SCHEMA_VERSION,
        migrations: SYNTHETIC_MIGRATIONS,
        initialize_current: initialize_synthetic_v3,
    };

    fn migrate_v2_to_synthetic_v3(
        transaction: &Transaction<'_>,
        _shard_count: u16,
    ) -> EngineResult<()> {
        transaction
            .execute_batch(
                "CREATE TABLE briskdb_v3_marker (value INTEGER NOT NULL) STRICT;
                 INSERT INTO briskdb_v3_marker VALUES (42);
                 UPDATE briskdb_metadata SET requires_manifest_version = 3;",
            )
            .map_err(sqlite_error::storage)
    }

    fn initialize_synthetic_v3(
        transaction: &Transaction<'_>,
        shard_count: u16,
    ) -> EngineResult<()> {
        create_v2_schema(transaction, shard_count)?;
        migrate_v2_to_synthetic_v3(transaction, shard_count)
    }

    fn validate_synthetic_v3(
        connection: &Connection,
        requested_shards: u16,
        objects: &[SchemaObject],
    ) -> EngineResult<u16> {
        let expected_objects = [
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
                name: "briskdb_v3_marker".to_owned(),
            },
        ];
        if objects != expected_objects {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "synthetic manifest schema version 3 has unexpected objects",
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
        validate_table(
            connection,
            "briskdb_v3_marker",
            &[TableColumn::expected(0, "value", "INTEGER", true, 0)],
            true,
        )?;

        let (singleton, stored_shards): (i64, i64) = connection
            .query_row(
                "SELECT singleton, shard_count FROM briskdb_manifest",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|error| {
                manifest_read_error(error, "failed to read synthetic manifest configuration")
            })?;
        let fence: i64 = connection
            .query_row(
                "SELECT requires_manifest_version FROM briskdb_metadata",
                [],
                |row| row.get(0),
            )
            .map_err(|error| {
                manifest_read_error(error, "failed to read synthetic manifest fence")
            })?;
        let marker: i64 = connection
            .query_row("SELECT value FROM briskdb_v3_marker", [], |row| row.get(0))
            .map_err(|error| {
                manifest_read_error(error, "failed to read synthetic manifest marker")
            })?;
        let shard_count = u16::try_from(stored_shards).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::DataCorruption,
                "synthetic manifest shard count is invalid",
                error,
            )
        })?;
        if singleton != 1 || fence != 3 || marker != 42 {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "synthetic manifest version 3 has invalid rows",
            ));
        }
        validate_shard_range(shard_count)?;
        ensure_requested_shards(shard_count, requested_shards)?;
        Ok(shard_count)
    }

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
        let error = load_or_create_with_plan(&mut connection, 4, SYNTHETIC_PLAN, &mut |point| {
            if point.phase == MigrationPhase::AfterVersionStamp {
                first_attempt.push((point.from, point.to));
                if point.from == V2_SCHEMA_VERSION {
                    return Err(EngineError::new(
                        EngineErrorKind::Internal,
                        "injected synthetic v3 failure",
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
            load_or_create_with_plan(&mut connection, 4, SYNTHETIC_PLAN, &mut |point| {
                if point.phase == MigrationPhase::AfterVersionStamp {
                    resumed_steps.push((point.from, point.to));
                }
                Ok(())
            },)
            .unwrap(),
            4
        );
        assert_eq!(resumed_steps, [(2, 3)]);
        assert_eq!(identity(&connection), (MANIFEST_APPLICATION_ID, 3));
        assert_eq!(
            connection
                .query_row("SELECT value FROM briskdb_v3_marker", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            42
        );
        assert_eq!(quick_check(&connection), "ok");
    }

    #[test]
    fn creates_and_idempotently_reopens_the_current_manifest() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let mut connection = Connection::open(&path).unwrap();

        assert_eq!(load_or_create(&mut connection, 4).unwrap(), 4);
        assert_eq!(identity(&connection), (MANIFEST_APPLICATION_ID, 2));
        assert_eq!(current_shard_count(&connection), 4);
        assert_eq!(schema_objects(&connection).unwrap(), v2_objects());
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
    fn upgrades_unversioned_and_explicitly_versioned_legacy_manifests() {
        for legacy_header in [0, 1] {
            let mut connection = Connection::open_in_memory().unwrap();
            create_legacy_manifest(&connection, 4, legacy_header);

            assert_eq!(load_or_create(&mut connection, 4).unwrap(), 4);
            assert_eq!(identity(&connection), (MANIFEST_APPLICATION_ID, 2));
            assert_eq!(current_shard_count(&connection), 4);
            assert_eq!(quick_check(&connection), "ok");
        }
    }

    #[test]
    fn recovers_the_exact_empty_table_left_by_interrupted_legacy_initialization() {
        let mut connection = Connection::open_in_memory().unwrap();
        create_empty_legacy_manifest(&connection);

        assert_eq!(load_or_create(&mut connection, 8).unwrap(), 8);
        assert_eq!(identity(&connection), (MANIFEST_APPLICATION_ID, 2));
        assert_eq!(current_shard_count(&connection), 8);
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
            assert_eq!(identity(&connection), (MANIFEST_APPLICATION_ID, 2));
        }
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
        let connection = Connection::open(&path).unwrap();
        create_legacy_manifest(&connection, 4, 0);
        drop(connection);

        let (paused_tx, paused_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let migration_path = path.clone();
        let worker = thread::spawn(move || {
            let mut connection = Connection::open(migration_path).unwrap();
            load_or_create_with_hook(&mut connection, 4, |point| {
                if point.phase == MigrationPhase::AfterVersionStamp {
                    paused_tx.send(()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                }
                Ok(())
            })
            .unwrap();
        });

        paused_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let observer = Connection::open(&path).unwrap();
        assert_eq!(identity(&observer), (0, 0));
        assert_eq!(schema_objects(&observer).unwrap(), legacy_objects());
        resume_tx.send(()).unwrap();
        worker.join().unwrap();

        assert_eq!(identity(&observer), (MANIFEST_APPLICATION_ID, 2));
        assert_eq!(schema_objects(&observer).unwrap(), v2_objects());
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
        assert_eq!(identity(&connection), (MANIFEST_APPLICATION_ID, 2));
        assert_eq!(current_shard_count(&connection), 4);
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
        assert_eq!(identity(&connection), (MANIFEST_APPLICATION_ID, 2));
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
        assert_eq!(identity(&connection), (MANIFEST_APPLICATION_ID, 2));
        assert_eq!(current_shard_count(&connection), 4);
    }

    #[test]
    fn rejects_future_and_foreign_manifests_without_mutating_them() {
        let mut future = Connection::open_in_memory().unwrap();
        load_or_create(&mut future, 4).unwrap();
        future.pragma_update(None, "user_version", 3).unwrap();
        let objects = schema_objects(&future).unwrap();
        let error = load_or_create(&mut future, 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&future), (MANIFEST_APPLICATION_ID, 3));
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
            "INSERT INTO briskdb_metadata VALUES (2)",
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
             INSERT INTO briskdb_metadata VALUES (2);",
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            load_or_create(&mut connection, 4).unwrap();
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
