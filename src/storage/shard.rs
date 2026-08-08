//! Physical-shard identity, provisioning, and strict reopen validation.

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use rusqlite::{
    Connection, MAIN_DB, OpenFlags, TransactionBehavior,
    hooks::{AuthAction, AuthContext, Authorization},
};

use crate::{
    core::{EngineError, EngineErrorKind, EngineResult},
    sqlite_error,
};

use super::CONNECTION_BUSY_TIMEOUT;

/// `BRSH` encoded as SQLite's 32-bit application identifier.
pub(super) const SHARD_APPLICATION_ID: i64 = 0x4252_5348;
/// Version of the storage-owned shard metadata table.
pub(super) const SHARD_METADATA_VERSION: u32 = 1;

const SHARD_METADATA_TABLE: &str = "briskdb_shard_metadata";
const MAX_SHARDS: u16 = 64;
const MAX_DIRECTORY_ENTRIES: usize = 512;
const MAX_SCHEMA_SQL_BYTES: usize = 4_096;

const SHARD_METADATA_TABLE_SQL: &str = "CREATE TABLE briskdb_shard_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    layout_id BLOB NOT NULL
        CHECK (typeof(layout_id) = 'blob' AND length(layout_id) = 16),
    shard_id INTEGER NOT NULL CHECK (shard_id BETWEEN 0 AND 63)
) STRICT";

/// Durable state of physical-shard layout preparation in the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub(super) enum ShardLayoutState {
    Creating = 1,
    Adopting = 2,
    Ready = 3,
}

impl ShardLayoutState {
    pub(super) const fn code(self) -> i64 {
        self as i64
    }

    pub(super) fn from_code(code: i64) -> EngineResult<Self> {
        match code {
            1 => Ok(Self::Creating),
            2 => Ok(Self::Adopting),
            3 => Ok(Self::Ready),
            _ => Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("manifest has unsupported shard-layout state {code}"),
            )),
        }
    }
}

/// Validated physical-shard format expectations loaded from the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ShardLayout {
    layout_id: [u8; 16],
    expected_application_id: i64,
    metadata_version: u32,
    state: ShardLayoutState,
}

impl ShardLayout {
    pub(super) fn from_validated_parts(
        layout_id: [u8; 16],
        expected_application_id: i64,
        metadata_version: u32,
        state: ShardLayoutState,
    ) -> Self {
        debug_assert_eq!(expected_application_id, SHARD_APPLICATION_ID);
        debug_assert_eq!(metadata_version, SHARD_METADATA_VERSION);
        Self {
            layout_id,
            expected_application_id,
            metadata_version,
            state,
        }
    }

    pub(super) const fn layout_id(self) -> [u8; 16] {
        self.layout_id
    }

    pub(super) const fn expected_application_id(self) -> i64 {
        self.expected_application_id
    }

    pub(super) const fn metadata_version(self) -> u32 {
        self.metadata_version
    }

    pub(super) const fn state(self) -> ShardLayoutState {
        self.state
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreflightState {
    Missing,
    Empty,
    Legacy,
    Exact,
}

#[derive(Debug)]
struct PreflightShard {
    shard_id: u16,
    path: PathBuf,
    state: PreflightState,
}

/// Which side of one journaled schema migration a strictly validated shard is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SchemaMigrationShardState {
    Source,
    Target,
}

/// Whether this invocation committed a shard migration or observed an earlier commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SchemaMigrationShardOutcome {
    Applied,
    AlreadyApplied,
}

/// Durable boundaries exposed to storage migration failure-injection tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SchemaMigrationPoint {
    SqlApplied,
    GenerationStamped,
    Committed,
}

/// Immutable identity and SQL for one shard's step in a journaled migration.
#[derive(Debug, Clone, Copy)]
pub(super) struct SchemaMigrationShard<'a> {
    path: &'a Path,
    shard_id: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &'a ShardLayout,
    sql: &'a str,
}

impl<'a> SchemaMigrationShard<'a> {
    pub(super) const fn new(
        path: &'a Path,
        shard_id: u16,
        source_generation: u64,
        target_generation: u64,
        layout: &'a ShardLayout,
        sql: &'a str,
    ) -> Self {
        Self {
            path,
            shard_id,
            source_generation,
            target_generation,
            layout,
            sql,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ReservedSchemaObject {
    object_type: String,
    name: String,
    table_name: String,
    sql: Option<String>,
}

/// Preflight every expected file before changing any shard, provision only the
/// eligible states, then perform one strict no-create validation pass.
pub(super) fn prepare_layout(
    shards_dir: &Path,
    shard_count: u16,
    schema_generation: u64,
    layout: &ShardLayout,
) -> EngineResult<()> {
    prepare_layout_with_hook(shards_dir, shard_count, schema_generation, layout, |_| {
        Ok(())
    })
}

pub(super) fn prepare_layout_with_hook<F>(
    shards_dir: &Path,
    shard_count: u16,
    schema_generation: u64,
    layout: &ShardLayout,
    mut hook: F,
) -> EngineResult<()>
where
    F: FnMut(u16) -> EngineResult<()>,
{
    validate_inputs(shard_count, schema_generation, layout)?;
    let preflight = preflight_all(shards_dir, shard_count, schema_generation, layout)?;

    if preflight
        .iter()
        .any(|shard| shard.state == PreflightState::Missing)
    {
        create_shards_directory(shards_dir, shard_count)?;
    }

    for shard in &preflight {
        match shard.state {
            PreflightState::Missing | PreflightState::Empty | PreflightState::Legacy => {
                provision_shard(
                    &shard.path,
                    shard.shard_id,
                    schema_generation,
                    layout,
                    |_| {},
                )?;
            }
            PreflightState::Exact => {}
        }
        hook(shard.shard_id)?;
    }

    // Re-scan after provisioning so a concurrent unexpected file or a partial
    // SQLite state cannot be certified by the manifest caller.
    validate_directory(shards_dir, shard_count, false)?;
    for shard_id in 0..shard_count {
        let path = shard_path(shards_dir, shard_id);
        drop(open_existing(&path, shard_id, schema_generation, layout)?);
    }
    Ok(())
}

/// Open a required shard without create or symlink traversal and return it only
/// after its persistent identity, generation, metadata, and WAL mode validate.
pub(super) fn open_existing(
    path: &Path,
    shard_id: u16,
    schema_generation: u64,
    layout: &ShardLayout,
) -> EngineResult<Connection> {
    let connection = open_required_file(path)?;
    configure_busy_timeout(&connection)?;
    validate_open_connection(&connection, path, shard_id, schema_generation, layout)?;
    Ok(connection)
}

/// Open the required path with strict filesystem and SQLite no-create/no-follow
/// semantics, but leave database validation to the caller. The controlled pool
/// path uses this split to install cancellation hooks before validation can
/// wait on SQLite locks.
pub(super) fn open_required_file(path: &Path) -> EngineResult<Connection> {
    validate_existing_file(path)?;
    open_existing_connection(path)
}

/// Validate and configure an already-open required shard connection without
/// replacing its busy handler or progress hook.
pub(super) fn validate_open_connection(
    connection: &Connection,
    path: &Path,
    shard_id: u16,
    schema_generation: u64,
    layout: &ShardLayout,
) -> EngineResult<()> {
    validate_shard_id(shard_id)?;
    let expected_user_version = expected_user_version(schema_generation)?;
    require_writable(connection)?;
    validate_exact_shard(connection, path, shard_id, expected_user_version, layout)?;
    configure_connection_pragmas(connection)
}

/// Strictly validate a dedicated migration connection while accepting only
/// the journal's exact source or target application-schema generation.
///
/// This does not replace the connection's busy handler or progress callback,
/// allowing the coordinator to install request cancellation before validation
/// can wait on a real SQLite lock. The connection must be dedicated to the
/// migration because the apply path temporarily installs its own authorizer.
pub(super) fn validate_schema_migration_connection(
    connection: &Connection,
    path: &Path,
    shard_id: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &ShardLayout,
) -> EngineResult<SchemaMigrationShardState> {
    validate_shard_id(shard_id)?;
    let (source_user_version, target_user_version) =
        validate_schema_migration_inputs(source_generation, target_generation, layout)?;
    require_writable(connection)?;
    let state = classify_schema_migration_shard(
        connection,
        path,
        shard_id,
        source_user_version,
        target_user_version,
        layout,
    )?;
    configure_connection_pragmas(connection)?;
    Ok(state)
}

/// Validate the exact durable prefix described by a migration journal.
///
/// Shards before `next_shard` must be at the target generation, the current
/// shard may be at source or target to cover a commit-before-acknowledgement
/// crash, and every later shard must remain at source. `None` means the journal
/// points one past the final shard and every shard validated at target.
pub(super) fn validate_schema_migration_prefix(
    shards_dir: &Path,
    shard_count: u16,
    next_shard: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &ShardLayout,
) -> EngineResult<Option<SchemaMigrationShardState>> {
    validate_schema_migration_prefix_with(
        shards_dir,
        shard_count,
        next_shard,
        source_generation,
        target_generation,
        layout,
        |path, shard_id| {
            let connection = open_required_file(path)?;
            configure_busy_timeout(&connection)?;
            validate_schema_migration_connection(
                &connection,
                path,
                shard_id,
                source_generation,
                target_generation,
                layout,
            )
        },
    )
}

/// Validate a migration prefix while delegating each SQLite connection to a
/// coordinator-provided, optionally cancellation-aware validator.
pub(super) fn validate_schema_migration_prefix_with<F>(
    shards_dir: &Path,
    shard_count: u16,
    next_shard: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &ShardLayout,
    mut validate: F,
) -> EngineResult<Option<SchemaMigrationShardState>>
where
    F: FnMut(&Path, u16) -> EngineResult<SchemaMigrationShardState>,
{
    validate_inputs(shard_count, source_generation, layout)?;
    validate_schema_migration_inputs(source_generation, target_generation, layout)?;
    if next_shard > shard_count {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("schema migration next shard {next_shard} exceeds shard count {shard_count}"),
        ));
    }
    validate_directory(shards_dir, shard_count, false)?;

    let mut current = None;
    for shard_id in 0..shard_count {
        let path = shard_path(shards_dir, shard_id);
        let state = validate(&path, shard_id)?;
        let expected = if shard_id < next_shard {
            SchemaMigrationShardState::Target
        } else if shard_id > next_shard {
            SchemaMigrationShardState::Source
        } else {
            current = Some(state);
            continue;
        };
        if state != expected {
            let expected_name = match expected {
                SchemaMigrationShardState::Source => "source",
                SchemaMigrationShardState::Target => "target",
            };
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "schema migration shard {shard_id} is not at its journaled {expected_name} generation"
                ),
            ));
        }
    }
    Ok(current)
}

/// Execute a migration batch in an immediate transaction and always roll it
/// back. A target-generation shard is strictly validated and skipped.
pub(super) fn preflight_schema_migration(
    path: &Path,
    shard_id: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &ShardLayout,
    sql: &str,
) -> EngineResult<SchemaMigrationShardState> {
    let mut connection = open_required_file(path)?;
    configure_busy_timeout(&connection)?;
    preflight_schema_migration_on_connection(
        &mut connection,
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
        sql,
    )
}

/// Connection-level preflight for a coordinator-owned, cancellation-aware handle.
pub(super) fn preflight_schema_migration_on_connection(
    connection: &mut Connection,
    path: &Path,
    shard_id: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &ShardLayout,
    sql: &str,
) -> EngineResult<SchemaMigrationShardState> {
    let initial = validate_schema_migration_connection(
        connection,
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
    )?;
    if initial == SchemaMigrationShardState::Target {
        return Ok(initial);
    }

    let (source_user_version, target_user_version) =
        validate_schema_migration_inputs(source_generation, target_generation, layout)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let locked = classify_schema_migration_shard(
        &transaction,
        path,
        shard_id,
        source_user_version,
        target_user_version,
        layout,
    )?;
    if locked == SchemaMigrationShardState::Target {
        transaction.rollback().map_err(sqlite_error::storage)?;
        return validate_schema_migration_connection(
            connection,
            path,
            shard_id,
            source_generation,
            target_generation,
            layout,
        );
    }

    let reserved_before = reserved_schema_snapshot(&transaction)?;
    execute_schema_migration_batch(&transaction, sql)?;
    ensure_reserved_schema_unchanged(&reserved_before, &transaction)?;
    ensure_no_foreign_key_violations(&transaction)?;
    transaction.rollback().map_err(sqlite_error::storage)?;

    let state = validate_schema_migration_connection(
        connection,
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
    )?;
    if state != SchemaMigrationShardState::Source {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            format!("schema migration preflight changed shard {shard_id} generation"),
        ));
    }
    Ok(state)
}

/// Atomically apply one journaled batch and its target generation, or strictly
/// validate and skip a shard already committed at the target generation.
pub(super) fn apply_schema_migration(
    path: &Path,
    shard_id: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &ShardLayout,
    sql: &str,
) -> EngineResult<SchemaMigrationShardOutcome> {
    let mut connection = open_required_file(path)?;
    configure_busy_timeout(&connection)?;
    apply_schema_migration_on_connection(
        &mut connection,
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
        sql,
    )
}

/// Connection-level apply for a coordinator-owned, cancellation-aware handle.
pub(super) fn apply_schema_migration_on_connection(
    connection: &mut Connection,
    path: &Path,
    shard_id: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &ShardLayout,
    sql: &str,
) -> EngineResult<SchemaMigrationShardOutcome> {
    let migration = SchemaMigrationShard::new(
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
        sql,
    );
    apply_schema_migration_on_connection_inner(connection, migration, |_| Ok(()))
}

/// Test seam for injecting errors, panics, and process termination at the
/// shard transaction's persistence boundaries.
#[cfg(test)]
pub(super) fn apply_schema_migration_on_connection_with_hook<F>(
    connection: &mut Connection,
    migration: SchemaMigrationShard<'_>,
    hook: F,
) -> EngineResult<SchemaMigrationShardOutcome>
where
    F: FnMut(SchemaMigrationPoint) -> EngineResult<()>,
{
    apply_schema_migration_on_connection_inner(connection, migration, hook)
}

fn apply_schema_migration_on_connection_inner<F>(
    connection: &mut Connection,
    migration: SchemaMigrationShard<'_>,
    mut hook: F,
) -> EngineResult<SchemaMigrationShardOutcome>
where
    F: FnMut(SchemaMigrationPoint) -> EngineResult<()>,
{
    let SchemaMigrationShard {
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
        sql,
    } = migration;
    let initial = validate_schema_migration_connection(
        connection,
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
    )?;
    if initial == SchemaMigrationShardState::Target {
        return Ok(SchemaMigrationShardOutcome::AlreadyApplied);
    }

    let (source_user_version, target_user_version) =
        validate_schema_migration_inputs(source_generation, target_generation, layout)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let locked = classify_schema_migration_shard(
        &transaction,
        path,
        shard_id,
        source_user_version,
        target_user_version,
        layout,
    )?;
    if locked == SchemaMigrationShardState::Target {
        transaction.rollback().map_err(sqlite_error::storage)?;
        validate_schema_migration_connection(
            connection,
            path,
            shard_id,
            source_generation,
            target_generation,
            layout,
        )?;
        return Ok(SchemaMigrationShardOutcome::AlreadyApplied);
    }

    let reserved_before = reserved_schema_snapshot(&transaction)?;
    execute_schema_migration_batch(&transaction, sql)?;
    hook(SchemaMigrationPoint::SqlApplied)?;
    ensure_reserved_schema_unchanged(&reserved_before, &transaction)?;

    transaction
        .pragma_update(None, "user_version", target_user_version)
        .map_err(sqlite_error::storage)?;
    hook(SchemaMigrationPoint::GenerationStamped)?;
    if classify_schema_migration_shard(
        &transaction,
        path,
        shard_id,
        source_user_version,
        target_user_version,
        layout,
    )? != SchemaMigrationShardState::Target
    {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            format!("schema migration did not stamp shard {shard_id} target generation"),
        ));
    }

    transaction.commit().map_err(sqlite_error::storage)?;
    hook(SchemaMigrationPoint::Committed)?;
    if validate_schema_migration_connection(
        connection,
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
    )? != SchemaMigrationShardState::Target
    {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            format!("committed schema migration did not persist on shard {shard_id}"),
        ));
    }
    Ok(SchemaMigrationShardOutcome::Applied)
}

fn validate_schema_migration_inputs(
    source_generation: u64,
    target_generation: u64,
    layout: &ShardLayout,
) -> EngineResult<(i64, i64)> {
    let expected_target = source_generation.checked_add(1).ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "schema migration source generation cannot be incremented",
        )
    })?;
    if target_generation != expected_target {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "schema migration target must immediately follow its source generation",
        ));
    }
    if layout.state() != ShardLayoutState::Ready
        || layout.expected_application_id() != SHARD_APPLICATION_ID
        || layout.metadata_version() != SHARD_METADATA_VERSION
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "schema migration requires a ready supported shard layout",
        ));
    }
    Ok((
        expected_user_version(source_generation)?,
        expected_user_version(target_generation)?,
    ))
}

fn classify_schema_migration_shard(
    connection: &Connection,
    path: &Path,
    shard_id: u16,
    source_user_version: i64,
    target_user_version: i64,
    layout: &ShardLayout,
) -> EngineResult<SchemaMigrationShardState> {
    let (application_id, user_version) = read_identity(connection)?;
    if application_id != layout.expected_application_id() {
        return if application_id == 0 {
            Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("schema-migration shard {shard_id} is missing its BriskDB application ID"),
            ))
        } else {
            Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "schema-migration shard {shard_id} has foreign application identifier {application_id:#010x}"
                ),
            ))
        };
    }
    let state = if user_version == source_user_version {
        SchemaMigrationShardState::Source
    } else if user_version == target_user_version {
        SchemaMigrationShardState::Target
    } else if user_version > target_user_version {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "shard {shard_id} schema generation {user_version} is newer than migration target {target_user_version}"
            ),
        ));
    } else {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "shard {shard_id} schema generation {user_version} is neither migration source {source_user_version} nor target {target_user_version}"
            ),
        ));
    };
    require_wal(connection, path)?;
    validate_metadata(connection, shard_id, layout.layout_id())?;
    Ok(state)
}

fn execute_schema_migration_batch(connection: &Connection, sql: &str) -> EngineResult<()> {
    connection
        .authorizer(Some(|context: AuthContext<'_>| {
            if denies_schema_migration_action(context) {
                Authorization::Deny
            } else {
                Authorization::Allow
            }
        }))
        .map_err(sqlite_error::storage)?;
    let executed = connection
        .execute_batch(sql)
        .map_err(sqlite_error::statement);
    let cleared = connection
        .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
        .map_err(sqlite_error::storage);
    cleared?;
    executed
}

fn reserved_schema_snapshot(connection: &Connection) -> EngineResult<Vec<ReservedSchemaObject>> {
    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, sql
             FROM sqlite_schema
             ORDER BY type, name, tbl_name",
        )
        .map_err(sqlite_error::storage)?;
    let rows = statement
        .query_map([], |row| {
            Ok(ReservedSchemaObject {
                object_type: row.get(0)?,
                name: row.get(1)?,
                table_name: row.get(2)?,
                sql: row.get(3)?,
            })
        })
        .map_err(sqlite_error::storage)?;
    let mut reserved = Vec::new();
    for row in rows {
        let row = row.map_err(sqlite_error::storage)?;
        if is_reserved_name(&row.name) || is_reserved_name(&row.table_name) {
            reserved.push(row);
        }
    }
    Ok(reserved)
}

fn ensure_reserved_schema_unchanged(
    before: &[ReservedSchemaObject],
    connection: &Connection,
) -> EngineResult<()> {
    if reserved_schema_snapshot(connection)? == before {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::PermissionDenied,
            "schema migration attempted to change the reserved BriskDB namespace",
        ))
    }
}

fn ensure_no_foreign_key_violations(connection: &Connection) -> EngineResult<()> {
    let mut statement = connection
        .prepare("PRAGMA main.foreign_key_check")
        .map_err(sqlite_error::storage)?;
    let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
    if rows.next().map_err(sqlite_error::storage)?.is_some() {
        return Err(EngineError::new(
            EngineErrorKind::ForeignKeyViolation,
            "schema migration preflight found a foreign-key violation",
        ));
    }
    Ok(())
}

fn preflight_all(
    shards_dir: &Path,
    shard_count: u16,
    schema_generation: u64,
    layout: &ShardLayout,
) -> EngineResult<Vec<PreflightShard>> {
    let directory_exists = validate_directory(
        shards_dir,
        shard_count,
        layout.state() == ShardLayoutState::Creating,
    )?;
    let expected_user_version = expected_user_version(schema_generation)?;
    let mut shards = Vec::with_capacity(usize::from(shard_count));

    for shard_id in 0..shard_count {
        let path = shard_path(shards_dir, shard_id);
        let state = if !directory_exists || !path_exists(&path)? {
            if layout.state() == ShardLayoutState::Creating {
                PreflightState::Missing
            } else {
                return Err(missing_shard(&path, shard_id));
            }
        } else {
            validate_existing_file(&path)?;
            let connection = open_existing_connection(&path)?;
            configure_connection_safety(&connection)?;
            classify_shard(&connection, &path, shard_id, expected_user_version, layout)?
        };
        shards.push(PreflightShard {
            shard_id,
            path,
            state,
        });
    }
    Ok(shards)
}

fn validate_inputs(
    shard_count: u16,
    schema_generation: u64,
    layout: &ShardLayout,
) -> EngineResult<()> {
    if !(2..=MAX_SHARDS).contains(&shard_count) {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "validated shard layout has an invalid shard count",
        ));
    }
    expected_user_version(schema_generation)?;
    if layout.expected_application_id() != SHARD_APPLICATION_ID
        || layout.metadata_version() != SHARD_METADATA_VERSION
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest shard-layout format is unsupported",
        ));
    }
    Ok(())
}

fn validate_shard_id(shard_id: u16) -> EngineResult<()> {
    if shard_id < MAX_SHARDS {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::Internal,
            format!("shard {shard_id} is outside the supported range"),
        ))
    }
}

fn expected_user_version(schema_generation: u64) -> EngineResult<i64> {
    i32::try_from(schema_generation)
        .map(i64::from)
        .map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::FailedPrecondition,
                "catalog schema generation does not fit SQLite user_version",
                error,
            )
        })
}

fn validate_directory(
    shards_dir: &Path,
    shard_count: u16,
    missing_allowed: bool,
) -> EngineResult<bool> {
    let metadata = match fs::symlink_metadata(shards_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && missing_allowed => {
            return Ok(false);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(EngineError::from_source(
                EngineErrorKind::DataCorruption,
                format!(
                    "required shard directory {} is missing",
                    shards_dir.display()
                ),
                error,
            ));
        }
        Err(error) => {
            return Err(sqlite_error::storage_io(
                error,
                format!("failed to inspect {}", shards_dir.display()),
            ));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "shard path {} is not a real directory",
                shards_dir.display()
            ),
        ));
    }

    let expected = (0..shard_count).map(shard_filename).collect::<HashSet<_>>();
    let mut entries = fs::read_dir(shards_dir).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!("failed to enumerate {}", shards_dir.display()),
        )
    })?;
    for index in 0..=MAX_DIRECTORY_ENTRIES {
        let Some(entry) = entries.next() else {
            return Ok(true);
        };
        if index == MAX_DIRECTORY_ENTRIES {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!(
                    "shard directory {} exceeds its bounded entry limit",
                    shards_dir.display()
                ),
            ));
        }
        let entry = entry.map_err(|error| {
            sqlite_error::storage_io(
                error,
                format!("failed to enumerate {}", shards_dir.display()),
            )
        })?;
        let name = entry.file_name().into_string().map_err(|_| {
            EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "shard directory contains a non-UTF-8 entry name",
            )
        })?;
        let file_type = entry.file_type().map_err(|error| {
            sqlite_error::storage_io(
                error,
                format!("failed to inspect {}", entry.path().display()),
            )
        })?;
        if file_type.is_symlink() {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "shard directory entry {} is a symbolic link",
                    entry.path().display()
                ),
            ));
        }
        if expected.contains(&name) {
            if !file_type.is_file() {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "required shard {} is not a regular file",
                        entry.path().display()
                    ),
                ));
            }
            continue;
        }
        if is_expected_sidecar(&name, &expected) {
            if !file_type.is_file() {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "SQLite sidecar {} is not a regular file",
                        entry.path().display()
                    ),
                ));
            }
            continue;
        }
        if is_canonical_shard_filename(&name) {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!("shard directory contains unexpected database file {name}"),
            ));
        }
    }
    unreachable!("bounded directory loop always returns")
}

fn is_expected_sidecar(name: &str, expected: &HashSet<String>) -> bool {
    ["-wal", "-shm", "-journal"].iter().any(|suffix| {
        name.strip_suffix(suffix)
            .is_some_and(|base| expected.contains(base))
    })
}

fn is_canonical_shard_filename(name: &str) -> bool {
    let Some(shard_id) = name.strip_suffix(".sqlite") else {
        return false;
    };
    shard_id.len() == 4 && shard_id.bytes().all(|byte| byte.is_ascii_digit())
}

fn path_exists(path: &Path) -> EngineResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(sqlite_error::storage_io(
            error,
            format!("failed to inspect {}", path.display()),
        )),
    }
}

fn validate_existing_file(path: &Path) -> EngineResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            EngineError::from_source(
                EngineErrorKind::DataCorruption,
                format!("required shard {} is missing", path.display()),
                error,
            )
        } else {
            sqlite_error::storage_io(error, format!("failed to inspect {}", path.display()))
        }
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("required shard {} is not a real file", path.display()),
        ));
    }
    Ok(())
}

fn create_shards_directory(shards_dir: &Path, shard_count: u16) -> EngineResult<()> {
    match fs::create_dir(shards_dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            validate_directory(shards_dir, shard_count, false).map(|_| ())
        }
        Err(error) => Err(sqlite_error::storage_io(
            error,
            format!("failed to create {}", shards_dir.display()),
        )),
    }
}

fn shard_filename(shard_id: u16) -> String {
    format!("{shard_id:04}.sqlite")
}

fn shard_path(shards_dir: &Path, shard_id: u16) -> PathBuf {
    shards_dir.join(shard_filename(shard_id))
}

fn open_existing_connection(path: &Path) -> EngineResult<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_EXRESCODE;
    let open_path = canonical_open_path(path)?;
    Connection::open_with_flags(open_path, flags).map_err(|error| {
        sqlite_error::storage(error).context(format!("failed to open shard {}", path.display()))
    })
}

fn open_creating_connection(path: &Path) -> EngineResult<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_EXRESCODE;
    let open_path = canonical_open_path(path)?;
    Connection::open_with_flags(open_path, flags).map_err(|error| {
        sqlite_error::storage(error).context(format!("failed to create shard {}", path.display()))
    })
}

// SQLite's NOFOLLOW flag rejects a path containing any symlink component. On
// macOS, tempfile paths commonly begin with `/var`, which is itself a system
// symlink. Resolve only the already-validated parent and retain the final shard
// component so NOFOLLOW still protects the database file from replacement.
fn canonical_open_path(path: &Path) -> EngineResult<PathBuf> {
    let parent = path.parent().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("shard path {} has no parent directory", path.display()),
        )
    })?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let parent_metadata = fs::symlink_metadata(parent).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!("failed to inspect shard directory {}", parent.display()),
        )
    })?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("shard path {} is not a real directory", parent.display()),
        ));
    }
    let file_name = path.file_name().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("shard path {} has no file name", path.display()),
        )
    })?;
    let canonical_parent = fs::canonicalize(parent).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!("failed to resolve shard directory {}", parent.display()),
        )
    })?;
    Ok(canonical_parent.join(file_name))
}

fn configure_connection_safety(connection: &Connection) -> EngineResult<()> {
    configure_busy_timeout(connection)?;
    require_writable(connection)
}

fn configure_busy_timeout(connection: &Connection) -> EngineResult<()> {
    connection
        .busy_timeout(CONNECTION_BUSY_TIMEOUT)
        .map_err(sqlite_error::storage)
}

fn require_writable(connection: &Connection) -> EngineResult<()> {
    if connection
        .is_readonly(MAIN_DB)
        .map_err(sqlite_error::storage)?
    {
        return Err(EngineError::new(
            EngineErrorKind::ReadOnly,
            "required shard opened read-only",
        ));
    }
    Ok(())
}

fn configure_connection_pragmas(connection: &Connection) -> EngineResult<()> {
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(sqlite_error::storage)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn classify_shard(
    connection: &Connection,
    path: &Path,
    shard_id: u16,
    expected_user_version: i64,
    layout: &ShardLayout,
) -> EngineResult<PreflightState> {
    let (application_id, user_version) = read_identity(connection)?;
    if application_id == layout.expected_application_id() {
        if user_version > expected_user_version {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "shard {shard_id} schema generation {user_version} is newer than the catalog generation {expected_user_version}"
                ),
            ));
        }
        if user_version != expected_user_version {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "shard {shard_id} schema generation {user_version} does not match catalog generation {expected_user_version}"
                ),
            ));
        }
        require_wal(connection, path)?;
        validate_metadata(connection, shard_id, layout.layout_id())?;
        return Ok(PreflightState::Exact);
    }

    if application_id != 0 || user_version != 0 {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "shard {shard_id} has foreign identity application_id={application_id:#010x}, user_version={user_version}"
            ),
        ));
    }
    if layout.state() == ShardLayoutState::Ready {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("ready shard {shard_id} is missing its BriskDB identity"),
        ));
    }
    if has_metadata_object(connection)? {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("shard {shard_id} has a conflicting {SHARD_METADATA_TABLE} object"),
        ));
    }

    match layout.state() {
        ShardLayoutState::Creating => {
            if has_application_schema_objects(connection)? {
                Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "new shard {} is not an exact empty SQLite database",
                        path.display()
                    ),
                ))
            } else {
                Ok(PreflightState::Empty)
            }
        }
        ShardLayoutState::Adopting => {
            require_wal(connection, path)?;
            Ok(PreflightState::Legacy)
        }
        ShardLayoutState::Ready => unreachable!("ready legacy state returned above"),
    }
}

fn provision_shard<F>(
    path: &Path,
    shard_id: u16,
    schema_generation: u64,
    layout: &ShardLayout,
    mut hook: F,
) -> EngineResult<()>
where
    F: FnMut(ProvisionPoint),
{
    let expected_user_version = expected_user_version(schema_generation)?;
    let mut connection = if path_exists(path)? {
        validate_existing_file(path)?;
        open_existing_connection(path)?
    } else if layout.state() == ShardLayoutState::Creating {
        open_creating_connection(path)?
    } else {
        return Err(missing_shard(path, shard_id));
    };
    configure_connection_safety(&connection)?;

    // Reclassify after opening so replacement between preflight and provisioning
    // cannot be overwritten as if it were the previously inspected file.
    let state = classify_shard(&connection, path, shard_id, expected_user_version, layout)?;
    if state == PreflightState::Exact {
        return Ok(());
    }
    configure_connection_pragmas(&connection)?;
    if state == PreflightState::Empty {
        enable_wal(&connection, path)?;
        hook(ProvisionPoint::WalPersisted);
    }

    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    // A second startup may have completed this shard while this connection
    // waited for the write lock. Reclassifying under that lock makes an exact
    // concurrent result idempotent and prevents a CREATE TABLE race.
    let locked_state = classify_shard(&transaction, path, shard_id, expected_user_version, layout)?;
    if locked_state == PreflightState::Exact {
        return Ok(());
    }
    transaction
        .execute_batch(SHARD_METADATA_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_shard_metadata (singleton, layout_id, shard_id)
             VALUES (1, ?1, ?2)",
            rusqlite::params![layout.layout_id().as_slice(), i64::from(shard_id)],
        )
        .map_err(sqlite_error::storage)?;
    hook(ProvisionPoint::MetadataWritten);
    transaction
        .pragma_update(None, "application_id", layout.expected_application_id())
        .map_err(sqlite_error::storage)?;
    transaction
        .pragma_update(None, "user_version", expected_user_version)
        .map_err(sqlite_error::storage)?;
    hook(ProvisionPoint::IdentityWritten);
    validate_exact_shard(&transaction, path, shard_id, expected_user_version, layout)?;
    transaction.commit().map_err(sqlite_error::storage)?;
    validate_exact_shard(&connection, path, shard_id, expected_user_version, layout)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProvisionPoint {
    WalPersisted,
    MetadataWritten,
    IdentityWritten,
}

fn validate_exact_shard(
    connection: &Connection,
    path: &Path,
    shard_id: u16,
    expected_user_version: i64,
    layout: &ShardLayout,
) -> EngineResult<()> {
    let (application_id, user_version) = read_identity(connection)?;
    if application_id != layout.expected_application_id() {
        return if application_id == 0 {
            Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("ready shard {shard_id} is missing its BriskDB application ID"),
            ))
        } else {
            Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "shard {shard_id} has foreign application identifier {application_id:#010x}"
                ),
            ))
        };
    }
    if user_version > expected_user_version {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("shard {shard_id} was written by a newer schema generation"),
        ));
    }
    if user_version != expected_user_version {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("shard {shard_id} schema generation does not match its catalog"),
        ));
    }
    require_wal(connection, path)?;
    validate_metadata(connection, shard_id, layout.layout_id())
}

fn read_identity(connection: &Connection) -> EngineResult<(i64, i64)> {
    let application_id = connection
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .map_err(|error| shard_read_error(error, "failed to read shard application ID"))?;
    let user_version = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|error| shard_read_error(error, "failed to read shard schema generation"))?;
    Ok((application_id, user_version))
}

fn journal_mode(connection: &Connection) -> EngineResult<String> {
    connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .map_err(|error| shard_read_error(error, "failed to read shard journal mode"))
}

fn require_wal(connection: &Connection, path: &Path) -> EngineResult<()> {
    let mode = journal_mode(connection)?;
    if mode.eq_ignore_ascii_case("wal") {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "shard {} uses journal mode {mode}, expected WAL",
                path.display()
            ),
        ))
    }
}

fn enable_wal(connection: &Connection, path: &Path) -> EngineResult<()> {
    let mode = connection
        .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))
        .map_err(sqlite_error::storage)?;
    if mode.eq_ignore_ascii_case("wal") {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "SQLite retained journal mode {mode} instead of enabling WAL for {}",
                path.display()
            ),
        ))
    }
}

fn has_application_schema_objects(connection: &Connection) -> EngineResult<bool> {
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_schema
                 WHERE name NOT LIKE 'sqlite_%'
                 LIMIT 1
             )",
            [],
            |row| row.get(0),
        )
        .map_err(|error| shard_read_error(error, "failed to inspect new shard schema"))
}

fn has_metadata_object(connection: &Connection) -> EngineResult<bool> {
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_schema
                 WHERE name = ?1 COLLATE NOCASE
                 LIMIT 1
             )",
            [SHARD_METADATA_TABLE],
            |row| row.get(0),
        )
        .map_err(|error| shard_read_error(error, "failed to inspect shard metadata objects"))
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

fn validate_metadata(
    connection: &Connection,
    expected_shard_id: u16,
    expected_layout_id: [u8; 16],
) -> EngineResult<()> {
    let objects = connection
        .prepare(
            "SELECT type, name, sql
             FROM sqlite_schema
             WHERE name = 'briskdb_shard_metadata' COLLATE NOCASE
             LIMIT 2",
        )
        .and_then(|mut statement| {
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| shard_read_error(error, "failed to inspect shard metadata schema"))?;
    if objects.len() != 1
        || objects[0].0 != "table"
        || objects[0].1 != SHARD_METADATA_TABLE
        || objects[0].2.as_deref().is_none_or(|sql| {
            sql.len() > MAX_SCHEMA_SQL_BYTES
                || normalize_schema_sql(sql) != normalize_schema_sql(SHARD_METADATA_TABLE_SQL)
        })
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "shard metadata table has an incompatible schema",
        ));
    }

    let columns = connection
        .prepare(
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_shard_metadata')
             LIMIT 4",
        )
        .and_then(|mut statement| {
            statement
                .query_map([], |row| {
                    Ok(TableColumn {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        declared_type: row.get(2)?,
                        not_null: row.get::<_, i64>(3)? != 0,
                        default_value: row.get(4)?,
                        primary_key_position: row.get(5)?,
                        hidden: row.get(6)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| shard_read_error(error, "failed to inspect shard metadata columns"))?;
    let expected_columns = [
        TableColumn::expected(0, "singleton", "INTEGER", false, 1),
        TableColumn::expected(1, "layout_id", "BLOB", true, 0),
        TableColumn::expected(2, "shard_id", "INTEGER", true, 0),
    ];
    if columns != expected_columns {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "shard metadata table has incompatible columns",
        ));
    }
    let strict: Option<i64> = connection
        .query_row(
            "SELECT strict
             FROM pragma_table_list
             WHERE schema = 'main' AND name = 'briskdb_shard_metadata'",
            [],
            |row| row.get(0),
        )
        .map(Some)
        .or_else(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            error => Err(error),
        })
        .map_err(|error| shard_read_error(error, "failed to inspect shard metadata flags"))?;
    if strict != Some(1) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "shard metadata table is not STRICT",
        ));
    }

    let rows = connection
        .prepare(
            "SELECT singleton, layout_id, shard_id
             FROM briskdb_shard_metadata
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
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| shard_read_error(error, "failed to read shard metadata"))?;
    if rows.len() != 1 || rows[0].0 != 1 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "shard metadata must contain exactly its singleton row",
        ));
    }
    if rows[0].1.as_slice() != expected_layout_id {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "shard belongs to a different BriskDB layout",
        ));
    }
    if rows[0].2 != i64::from(expected_shard_id) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "shard metadata identifies physical shard {}, expected {expected_shard_id}",
                rows[0].2
            ),
        ));
    }
    Ok(())
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

fn missing_shard(path: &Path, shard_id: u16) -> EngineError {
    EngineError::new(
        EngineErrorKind::DataCorruption,
        format!("required shard {shard_id} is missing at {}", path.display()),
    )
}

fn shard_read_error(error: rusqlite::Error, diagnostic: &'static str) -> EngineError {
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

/// The migration connection is the only SQL surface allowed to change the
/// persistent application schema. Keep it inside `main`, prevent the batch
/// from escaping BriskDB's transaction, and protect every storage-owned name
/// and control. ALTER destinations are validated by comparing the reserved
/// schema before and after the batch because SQLite reports only its source.
fn denies_schema_migration_action(context: AuthContext<'_>) -> bool {
    if context
        .database_name
        .is_some_and(|database| !database.eq_ignore_ascii_case("main"))
    {
        return true;
    }

    match context.action {
        AuthAction::Unknown { .. }
        | AuthAction::Transaction { .. }
        | AuthAction::Savepoint { .. }
        | AuthAction::Attach { .. }
        | AuthAction::Detach { .. }
        | AuthAction::CreateTempIndex { .. }
        | AuthAction::CreateTempTable { .. }
        | AuthAction::CreateTempTrigger { .. }
        | AuthAction::CreateTempView { .. }
        | AuthAction::DropTempIndex { .. }
        | AuthAction::DropTempTable { .. }
        | AuthAction::DropTempTrigger { .. }
        | AuthAction::DropTempView { .. }
        | AuthAction::CreateVtable { .. }
        | AuthAction::DropVtable { .. } => true,
        AuthAction::Pragma {
            pragma_name,
            pragma_value,
        } => pragma_value.is_some() || matches_persistent_pragma(pragma_name),
        AuthAction::Insert { table_name } | AuthAction::Delete { table_name } => {
            is_reserved_name(table_name)
        }
        AuthAction::Update {
            table_name,
            column_name: _,
        } => is_reserved_name(table_name),
        AuthAction::Read {
            table_name,
            column_name: _,
        } => is_metadata_table(table_name),
        AuthAction::CreateTable { table_name } | AuthAction::DropTable { table_name } => {
            is_reserved_name(table_name)
        }
        AuthAction::CreateIndex {
            index_name,
            table_name,
        }
        | AuthAction::DropIndex {
            index_name,
            table_name,
        } => is_reserved_name(index_name) || is_reserved_name(table_name),
        AuthAction::CreateTrigger {
            trigger_name,
            table_name,
        }
        | AuthAction::DropTrigger {
            trigger_name,
            table_name,
        } => is_reserved_name(trigger_name) || is_reserved_name(table_name),
        AuthAction::CreateView { view_name } | AuthAction::DropView { view_name } => {
            is_reserved_name(view_name)
        }
        AuthAction::AlterTable {
            database_name,
            table_name,
        } => !database_name.eq_ignore_ascii_case("main") || is_reserved_name(table_name),
        AuthAction::Reindex { index_name } => is_reserved_name(index_name),
        AuthAction::Analyze { table_name } => is_reserved_name(table_name),
        AuthAction::Select | AuthAction::Function { .. } | AuthAction::Recursive => false,
        _ => false,
    }
}

/// Return whether a client statement action would mutate storage-owned shard
/// identity, durability configuration, or the reserved metadata namespace.
pub(super) fn denies_client_action(action: AuthAction<'_>) -> bool {
    match action {
        AuthAction::Pragma {
            pragma_name,
            pragma_value: Some(_),
        } => matches_persistent_pragma(pragma_name),
        AuthAction::Insert { table_name } | AuthAction::Delete { table_name } => {
            is_metadata_table(table_name)
        }
        AuthAction::Update {
            table_name,
            column_name: _,
        }
        | AuthAction::Read {
            table_name,
            column_name: _,
        } => is_metadata_table(table_name),
        // Every persistent application-schema change must go through the
        // journaled migration connection. Temp objects remain connection-local
        // and are retired by the pool's hygiene boundary.
        AuthAction::AlterTable { .. }
        | AuthAction::CreateTable { .. }
        | AuthAction::CreateIndex { .. }
        | AuthAction::CreateTrigger { .. }
        | AuthAction::CreateView { .. }
        | AuthAction::CreateVtable { .. }
        | AuthAction::DropTable { .. }
        | AuthAction::DropIndex { .. }
        | AuthAction::DropTrigger { .. }
        | AuthAction::DropView { .. }
        | AuthAction::DropVtable { .. } => true,
        AuthAction::CreateTempTable { table_name } => is_reserved_name(table_name),
        AuthAction::CreateTempIndex {
            index_name,
            table_name,
        } => is_reserved_name(index_name) || is_metadata_table(table_name),
        AuthAction::CreateTempTrigger {
            trigger_name,
            table_name,
        } => is_reserved_name(trigger_name) || is_metadata_table(table_name),
        AuthAction::CreateTempView { view_name } => is_reserved_name(view_name),
        AuthAction::DropTempIndex {
            index_name,
            table_name,
        } => is_reserved_name(index_name) || is_metadata_table(table_name),
        AuthAction::DropTempTrigger {
            trigger_name,
            table_name,
        } => is_reserved_name(trigger_name) || is_metadata_table(table_name),
        AuthAction::DropTempTable { table_name } => is_reserved_name(table_name),
        AuthAction::DropTempView { view_name } => is_reserved_name(view_name),
        AuthAction::Reindex { index_name } => is_reserved_name(index_name),
        AuthAction::Analyze { table_name } => is_metadata_table(table_name),
        _ => false,
    }
}

fn matches_persistent_pragma(name: &str) -> bool {
    [
        "application_id",
        "user_version",
        "journal_mode",
        "writable_schema",
        "schema_version",
    ]
    .iter()
    .any(|protected| name.eq_ignore_ascii_case(protected))
}

fn is_metadata_table(name: &str) -> bool {
    name.eq_ignore_ascii_case(SHARD_METADATA_TABLE)
}

fn is_reserved_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("briskdb")
        || name
            .as_bytes()
            .get(.."briskdb_".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"briskdb_"))
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        process::Command,
        sync::Arc,
        thread,
    };

    use rusqlite::hooks::TransactionOperation;

    use super::*;

    const LAYOUT_ID: [u8; 16] = *b"brisk-layout-001";
    const SHARD_CRASH_SQL: &str = "\
        CREATE TABLE shard_crash_marker (id INTEGER PRIMARY KEY, value TEXT NOT NULL);\
        INSERT INTO shard_crash_marker (id, value) VALUES (1, 'persisted');";

    fn layout(state: ShardLayoutState) -> ShardLayout {
        ShardLayout::from_validated_parts(
            LAYOUT_ID,
            SHARD_APPLICATION_ID,
            SHARD_METADATA_VERSION,
            state,
        )
    }

    fn create_legacy(path: &Path, wal: bool, schema: &str) {
        let connection = Connection::open(path).unwrap();
        connection.execute_batch(schema).unwrap();
        if wal {
            enable_wal(&connection, path).unwrap();
        }
    }

    fn identity(path: &Path) -> (i64, i64) {
        let connection = Connection::open(path).unwrap();
        read_identity(&connection).unwrap()
    }

    fn has_metadata(path: &Path) -> bool {
        let connection = Connection::open(path).unwrap();
        has_metadata_object(&connection).unwrap()
    }

    fn create_ready_layout(shard_count: u16) -> (tempfile::TempDir, PathBuf, ShardLayout) {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        prepare_layout(&shards, shard_count, 0, &layout(ShardLayoutState::Creating)).unwrap();
        (temp, shards, layout(ShardLayoutState::Ready))
    }

    fn schema_object_exists(path: &Path, name: &str) -> bool {
        Connection::open(path)
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = ?1)",
                [name],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    fn layout_codes_parts_and_accessors_are_exact() {
        assert_eq!(ShardLayoutState::Creating.code(), 1);
        assert_eq!(ShardLayoutState::Adopting.code(), 2);
        assert_eq!(ShardLayoutState::Ready.code(), 3);
        for state in [
            ShardLayoutState::Creating,
            ShardLayoutState::Adopting,
            ShardLayoutState::Ready,
        ] {
            assert_eq!(ShardLayoutState::from_code(state.code()).unwrap(), state);
            let layout = layout(state);
            assert_eq!(layout.layout_id(), LAYOUT_ID);
            assert_eq!(layout.expected_application_id(), SHARD_APPLICATION_ID);
            assert_eq!(layout.metadata_version(), SHARD_METADATA_VERSION);
            assert_eq!(layout.state(), state);
        }
        assert_eq!(
            ShardLayoutState::from_code(4).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn creating_provisions_exact_wal_shards_and_ready_reopens_without_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        prepare_layout(&shards, 4, 0, &layout(ShardLayoutState::Creating)).unwrap();

        for shard_id in 0..4 {
            let path = shard_path(&shards, shard_id);
            let connection =
                open_existing(&path, shard_id, 0, &layout(ShardLayoutState::Ready)).unwrap();
            assert_eq!(
                journal_mode(&connection).unwrap().to_ascii_lowercase(),
                "wal"
            );
            assert_eq!(
                read_identity(&connection).unwrap(),
                (SHARD_APPLICATION_ID, 0)
            );
        }
    }

    #[test]
    fn unrelated_files_are_ignored_but_extra_canonical_shards_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        fs::write(shards.join("operator-notes.sqlite"), b"not a shard").unwrap();
        fs::write(shards.join("README"), b"layout notes").unwrap();
        prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap();

        fs::copy(shard_path(&shards, 0), shard_path(&shards, 2)).unwrap();
        let error = prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Ready)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
    }

    #[test]
    fn preflight_rejects_a_late_foreign_shard_before_stamping_any_eligible_shard() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        let first = shard_path(&shards, 0);
        let second = shard_path(&shards, 1);
        create_legacy(&first, true, "CREATE TABLE user_data (id INTEGER);");
        create_legacy(&second, true, "CREATE TABLE user_data (id INTEGER);");
        let foreign = Connection::open(&second).unwrap();
        foreign
            .pragma_update(None, "application_id", 0x1234)
            .unwrap();
        drop(foreign);

        let error = prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Adopting)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&first), (0, 0));
        assert!(!has_metadata(&first));
    }

    #[test]
    fn adopting_preserves_legacy_schema_and_is_idempotent_after_partial_work() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        for shard_id in 0..2 {
            create_legacy(
                &shard_path(&shards, shard_id),
                true,
                "CREATE TABLE widgets (id INTEGER PRIMARY KEY, value TEXT);",
            );
        }
        let adopting = layout(ShardLayoutState::Adopting);
        provision_shard(&shard_path(&shards, 0), 0, 0, &adopting, |_| {}).unwrap();
        prepare_layout(&shards, 2, 0, &adopting).unwrap();

        for shard_id in 0..2 {
            let connection = open_existing(
                &shard_path(&shards, shard_id),
                shard_id,
                0,
                &layout(ShardLayoutState::Ready),
            )
            .unwrap();
            assert_eq!(
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'widgets'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                1
            );
        }
    }

    #[test]
    fn adopting_and_ready_never_create_a_missing_shard() {
        for state in [ShardLayoutState::Adopting, ShardLayoutState::Ready] {
            let temp = tempfile::tempdir().unwrap();
            let shards = temp.path().join("shards");
            fs::create_dir(&shards).unwrap();
            let missing = shard_path(&shards, 1);
            create_legacy(&shard_path(&shards, 0), true, "");

            let error = prepare_layout(&shards, 2, 0, &layout(state)).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
            assert!(!missing.exists());
        }
    }

    #[test]
    fn only_creating_may_enable_wal() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        for shard_id in 0..2 {
            create_legacy(&shard_path(&shards, shard_id), false, "");
        }
        let error = prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Adopting)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(
            journal_mode(&Connection::open(shard_path(&shards, 0)).unwrap())
                .unwrap()
                .to_ascii_lowercase(),
            "delete"
        );

        prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap();
        assert_eq!(
            journal_mode(&Connection::open(shard_path(&shards, 0)).unwrap())
                .unwrap()
                .to_ascii_lowercase(),
            "wal"
        );
    }

    #[test]
    fn ready_validation_never_repairs_a_changed_journal_mode() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap();
        let path = shard_path(&shards, 0);
        let connection = Connection::open(&path).unwrap();
        let mode = connection
            .pragma_update_and_check(None, "journal_mode", "DELETE", |row| {
                row.get::<_, String>(0)
            })
            .unwrap();
        assert_eq!(mode.to_ascii_lowercase(), "delete");
        drop(connection);

        let error = open_existing(&path, 0, 0, &layout(ShardLayoutState::Ready)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(
            journal_mode(&Connection::open(path).unwrap())
                .unwrap()
                .to_ascii_lowercase(),
            "delete"
        );
    }

    #[test]
    fn creating_rejects_a_nonempty_unmarked_database() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        create_legacy(
            &shard_path(&shards, 0),
            false,
            "CREATE TABLE foreign_data (id INTEGER);",
        );
        create_legacy(&shard_path(&shards, 1), false, "");

        let error = prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&shard_path(&shards, 1)), (0, 0));
    }

    #[test]
    fn exact_validation_rejects_foreign_future_layout_and_shard_identity() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap();
        let ready = layout(ShardLayoutState::Ready);

        let path = shard_path(&shards, 0);
        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "application_id", 0x1234)
            .unwrap();
        drop(connection);
        assert_eq!(
            open_existing(&path, 0, 0, &ready).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(identity(&path), (0x1234, 0));

        let connection = Connection::open(&path).unwrap();
        connection.pragma_update(None, "application_id", 0).unwrap();
        drop(connection);
        assert_eq!(
            open_existing(&path, 0, 0, &ready).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
        assert_eq!(identity(&path), (0, 0));

        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "application_id", SHARD_APPLICATION_ID)
            .unwrap();
        drop(connection);
        let connection = Connection::open(&path).unwrap();
        connection.pragma_update(None, "user_version", 1).unwrap();
        drop(connection);
        assert_eq!(
            open_existing(&path, 0, 0, &ready).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );

        let connection = Connection::open(&path).unwrap();
        connection.pragma_update(None, "user_version", 0).unwrap();
        connection
            .execute("UPDATE briskdb_shard_metadata SET shard_id = 1", [])
            .unwrap();
        drop(connection);
        assert_eq!(
            open_existing(&path, 0, 0, &ready).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );

        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "UPDATE briskdb_shard_metadata SET shard_id = 0, layout_id = zeroblob(16)",
                [],
            )
            .unwrap();
        drop(connection);
        assert_eq!(
            open_existing(&path, 0, 0, &ready).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn altered_metadata_schema_and_conflicting_legacy_object_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        for shard_id in 0..2 {
            create_legacy(&shard_path(&shards, shard_id), true, "");
        }
        Connection::open(shard_path(&shards, 0))
            .unwrap()
            .execute_batch("CREATE TABLE briskdb_shard_metadata (shard_id INTEGER);")
            .unwrap();
        let error = prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Adopting)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);

        let other = tempfile::tempdir().unwrap();
        let other_shards = other.path().join("shards");
        prepare_layout(&other_shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap();
        let path = shard_path(&other_shards, 0);
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(
            "DROP TABLE briskdb_shard_metadata;
             CREATE TABLE briskdb_shard_metadata (
                 singleton INTEGER PRIMARY KEY,
                 layout_id BLOB NOT NULL,
                 shard_id INTEGER NOT NULL
             ) STRICT;
             INSERT INTO briskdb_shard_metadata VALUES (1, x'627269736b2d6c61796f75742d303031', 0);",
        ).unwrap();
        drop(connection);
        assert_eq!(
            open_existing(&path, 0, 0, &layout(ShardLayoutState::Ready))
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_shards_and_nonfiles_are_rejected_without_following() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        let target = temp.path().join("target.sqlite");
        create_legacy(&target, true, "");
        symlink(&target, shard_path(&shards, 0)).unwrap();
        fs::create_dir(shard_path(&shards, 1)).unwrap();

        let error = prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Adopting)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&target), (0, 0));
    }

    #[cfg(unix)]
    #[test]
    fn runtime_open_rejects_a_symlinked_shard_directory() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let real_shards = temp.path().join("real-shards");
        prepare_layout(&real_shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap();
        let linked_shards = temp.path().join("linked-shards");
        symlink(&real_shards, &linked_shards).unwrap();

        let error = open_existing(
            &shard_path(&linked_shards, 0),
            0,
            0,
            &layout(ShardLayoutState::Ready),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
    }

    #[test]
    fn corrupt_required_shard_is_reported_without_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        let path = shard_path(&shards, 0);
        fs::write(&path, b"not a sqlite database").unwrap();

        let error = open_existing(&path, 0, 0, &layout(ShardLayoutState::Ready)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(fs::read(path).unwrap(), b"not a sqlite database");
    }

    #[test]
    fn provisioning_panics_roll_back_and_retry_in_creating_and_adopting() {
        for state in [ShardLayoutState::Creating, ShardLayoutState::Adopting] {
            let points: &[ProvisionPoint] = if state == ShardLayoutState::Creating {
                &[
                    ProvisionPoint::WalPersisted,
                    ProvisionPoint::MetadataWritten,
                    ProvisionPoint::IdentityWritten,
                ]
            } else {
                &[
                    ProvisionPoint::MetadataWritten,
                    ProvisionPoint::IdentityWritten,
                ]
            };
            for &point in points {
                let temp = tempfile::tempdir().unwrap();
                let shards = temp.path().join("shards");
                fs::create_dir(&shards).unwrap();
                let path = shard_path(&shards, 0);
                create_legacy(&path, state == ShardLayoutState::Adopting, "");

                let panic = catch_unwind(AssertUnwindSafe(|| {
                    let _ = provision_shard(&path, 0, 0, &layout(state), |seen| {
                        if seen == point {
                            panic!("injected shard provisioning panic");
                        }
                    });
                }));
                assert!(panic.is_err());
                assert_eq!(identity(&path), (0, 0));
                assert!(!has_metadata(&path));
                assert_eq!(
                    journal_mode(&Connection::open(&path).unwrap())
                        .unwrap()
                        .to_ascii_lowercase(),
                    "wal"
                );

                provision_shard(&path, 0, 0, &layout(state), |_| {}).unwrap();
                open_existing(&path, 0, 0, &layout(ShardLayoutState::Ready)).unwrap();
            }
        }
    }

    #[test]
    fn migration_preflight_rolls_back_and_apply_commits_sql_with_generation_once() {
        let (_temp, shards, ready) = create_ready_layout(2);
        let path = shard_path(&shards, 0);
        let sql = "CREATE TABLE migrated_widgets (
                       id INTEGER PRIMARY KEY,
                       value TEXT NOT NULL
                   );
                   INSERT INTO migrated_widgets (id, value) VALUES (1, 'once');";

        assert_eq!(
            preflight_schema_migration(&path, 0, 0, 1, &ready, sql).unwrap(),
            SchemaMigrationShardState::Source
        );
        assert_eq!(identity(&path), (SHARD_APPLICATION_ID, 0));
        assert!(!schema_object_exists(&path, "migrated_widgets"));

        assert_eq!(
            apply_schema_migration(&path, 0, 0, 1, &ready, sql).unwrap(),
            SchemaMigrationShardOutcome::Applied
        );
        assert_eq!(identity(&path), (SHARD_APPLICATION_ID, 1));
        assert_eq!(
            Connection::open(&path)
                .unwrap()
                .query_row("SELECT value FROM migrated_widgets", [], |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "once"
        );

        // The original SQL is intentionally not idempotent. A retry must
        // recognize the target generation and skip it rather than execute it.
        assert_eq!(
            preflight_schema_migration(&path, 0, 0, 1, &ready, sql).unwrap(),
            SchemaMigrationShardState::Target
        );
        assert_eq!(
            apply_schema_migration(&path, 0, 0, 1, &ready, sql).unwrap(),
            SchemaMigrationShardOutcome::AlreadyApplied
        );
        assert_eq!(
            Connection::open(&path)
                .unwrap()
                .query_row("SELECT COUNT(*) FROM migrated_widgets", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );
        open_existing(&path, 0, 1, &ready).unwrap();
    }

    #[test]
    fn migration_batch_failure_rolls_back_earlier_statements_and_generation() {
        for preflight_only in [true, false] {
            let (_temp, shards, ready) = create_ready_layout(2);
            let path = shard_path(&shards, 0);
            let sql = "CREATE TABLE must_rollback (id INTEGER);
                       INSERT INTO missing_table VALUES (1);";
            let error = if preflight_only {
                preflight_schema_migration(&path, 0, 0, 1, &ready, sql).unwrap_err()
            } else {
                apply_schema_migration(&path, 0, 0, 1, &ready, sql).unwrap_err()
            };
            assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
            assert_eq!(identity(&path), (SHARD_APPLICATION_ID, 0));
            assert!(!schema_object_exists(&path, "must_rollback"));
            open_existing(&path, 0, 0, &ready).unwrap();
        }
    }

    #[test]
    fn migration_allows_main_ddl_dml_and_alter_without_touching_reserved_schema() {
        let (_temp, shards, ready) = create_ready_layout(2);
        let path = shard_path(&shards, 0);
        Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TABLE widgets (
                     id INTEGER PRIMARY KEY,
                     value TEXT NOT NULL
                 );",
            )
            .unwrap();
        let sql = "ALTER TABLE widgets ADD COLUMN note TEXT;
                   INSERT INTO widgets (id, value, note) VALUES (1, 'first', 'created');
                   UPDATE widgets SET value = upper(value) WHERE id = 1;
                   CREATE INDEX widgets_value_idx ON widgets (value);
                   CREATE VIEW widget_notes AS SELECT id, note FROM widgets;
                   CREATE TRIGGER widgets_note_default
                   AFTER INSERT ON widgets WHEN NEW.note IS NULL
                   BEGIN
                       UPDATE widgets SET note = 'default' WHERE id = NEW.id;
                   END;";

        assert_eq!(
            apply_schema_migration(&path, 0, 0, 1, &ready, sql).unwrap(),
            SchemaMigrationShardOutcome::Applied
        );
        let connection = Connection::open(&path).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT value, note FROM widgets", [], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap(),
            ("FIRST".to_owned(), "created".to_owned())
        );
        validate_metadata(&connection, 0, LAYOUT_ID).unwrap();
    }

    #[test]
    fn migration_authorizer_denies_escape_and_reserved_surfaces() {
        let denied = [
            "SAVEPOINT escaped",
            "ATTACH DATABASE ':memory:' AS auxiliary",
            "CREATE TEMP TABLE temporary_escape (id INTEGER)",
            "CREATE VIRTUAL TABLE virtual_escape USING fts5(value)",
            "PRAGMA user_version = 7",
            "CREATE TABLE briskdb (id INTEGER)",
            "CREATE TABLE BRISKDB (id INTEGER)",
            "CREATE TABLE briskdb_private (id INTEGER)",
            "SELECT * FROM briskdb_shard_metadata",
        ];
        for sql in denied {
            let (_temp, shards, ready) = create_ready_layout(2);
            let path = shard_path(&shards, 0);
            let error = preflight_schema_migration(&path, 0, 0, 1, &ready, sql).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::PermissionDenied, "{sql}");
            assert_eq!(identity(&path), (SHARD_APPLICATION_ID, 0));
            validate_metadata(&Connection::open(&path).unwrap(), 0, LAYOUT_ID).unwrap();
        }
    }

    #[test]
    fn migration_postcheck_catches_an_alter_destination_in_reserved_namespace() {
        let (_temp, shards, ready) = create_ready_layout(2);
        let path = shard_path(&shards, 0);
        Connection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE widgets (id INTEGER PRIMARY KEY);")
            .unwrap();

        for destination in ["briskdb", "BRISKDB", "briskdb_hidden"] {
            let sql = format!("ALTER TABLE widgets RENAME TO {destination}");
            let error = preflight_schema_migration(&path, 0, 0, 1, &ready, &sql).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::PermissionDenied);
            assert!(schema_object_exists(&path, "widgets"));
            assert!(!schema_object_exists(&path, destination));
            assert_eq!(identity(&path), (SHARD_APPLICATION_ID, 0));
        }
    }

    #[test]
    fn migration_error_hooks_prove_every_transaction_persistence_boundary() {
        for point in [
            SchemaMigrationPoint::SqlApplied,
            SchemaMigrationPoint::GenerationStamped,
            SchemaMigrationPoint::Committed,
        ] {
            let (_temp, shards, ready) = create_ready_layout(2);
            let path = shard_path(&shards, 0);
            let mut connection = open_required_file(&path).unwrap();
            configure_busy_timeout(&connection).unwrap();
            let error = apply_schema_migration_on_connection_with_hook(
                &mut connection,
                SchemaMigrationShard::new(
                    &path,
                    0,
                    0,
                    1,
                    &ready,
                    "CREATE TABLE error_boundary (id INTEGER)",
                ),
                |seen| {
                    if seen == point {
                        Err(EngineError::new(
                            EngineErrorKind::Internal,
                            "injected migration boundary error",
                        ))
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Internal);
            drop(connection);

            let committed = point == SchemaMigrationPoint::Committed;
            assert_eq!(
                identity(&path),
                (SHARD_APPLICATION_ID, i64::from(committed))
            );
            assert_eq!(schema_object_exists(&path, "error_boundary"), committed);
            assert_eq!(
                apply_schema_migration(
                    &path,
                    0,
                    0,
                    1,
                    &ready,
                    "CREATE TABLE error_boundary (id INTEGER)",
                )
                .unwrap(),
                if committed {
                    SchemaMigrationShardOutcome::AlreadyApplied
                } else {
                    SchemaMigrationShardOutcome::Applied
                }
            );
        }
    }

    #[test]
    fn migration_panics_prove_every_transaction_persistence_boundary() {
        for point in [
            SchemaMigrationPoint::SqlApplied,
            SchemaMigrationPoint::GenerationStamped,
            SchemaMigrationPoint::Committed,
        ] {
            let (_temp, shards, ready) = create_ready_layout(2);
            let path = shard_path(&shards, 0);
            let panic = catch_unwind(AssertUnwindSafe(|| {
                let mut connection = open_required_file(&path).unwrap();
                configure_busy_timeout(&connection).unwrap();
                let _ = apply_schema_migration_on_connection_with_hook(
                    &mut connection,
                    SchemaMigrationShard::new(
                        &path,
                        0,
                        0,
                        1,
                        &ready,
                        "CREATE TABLE panic_boundary (id INTEGER)",
                    ),
                    |seen| {
                        if seen == point {
                            panic!("injected migration boundary panic");
                        }
                        Ok(())
                    },
                );
            }));
            assert!(panic.is_err());

            let committed = point == SchemaMigrationPoint::Committed;
            assert_eq!(
                identity(&path),
                (SHARD_APPLICATION_ID, i64::from(committed))
            );
            assert_eq!(schema_object_exists(&path, "panic_boundary"), committed);
            assert_eq!(
                apply_schema_migration(
                    &path,
                    0,
                    0,
                    1,
                    &ready,
                    "CREATE TABLE panic_boundary (id INTEGER)",
                )
                .unwrap(),
                if committed {
                    SchemaMigrationShardOutcome::AlreadyApplied
                } else {
                    SchemaMigrationShardOutcome::Applied
                }
            );
        }
    }

    #[test]
    fn schema_migration_shard_crash_child() {
        let Ok(path) = std::env::var("BRISKDB_SHARD_CRASH_PATH") else {
            return;
        };
        let crash_point = std::env::var("BRISKDB_SHARD_CRASH_POINT").unwrap();
        let ready = layout(ShardLayoutState::Ready);
        let mut connection = open_required_file(Path::new(&path)).unwrap();
        configure_busy_timeout(&connection).unwrap();
        let result = apply_schema_migration_on_connection_with_hook(
            &mut connection,
            SchemaMigrationShard::new(Path::new(&path), 0, 0, 1, &ready, SHARD_CRASH_SQL),
            |point| {
                let point_name = match point {
                    SchemaMigrationPoint::SqlApplied => "sql-applied",
                    SchemaMigrationPoint::GenerationStamped => "generation-stamped",
                    SchemaMigrationPoint::Committed => "committed",
                };
                if crash_point == point_name {
                    std::process::abort();
                }
                Ok(())
            },
        );
        panic!("child did not reach requested crash point {crash_point}: {result:?}");
    }

    #[test]
    fn real_process_abort_before_shard_commit_rolls_back_sql_and_generation() {
        for crash_point in ["sql-applied", "generation-stamped"] {
            let (_temp, shards, ready) = create_ready_layout(2);
            let path = shard_path(&shards, 0);
            let status = Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("storage::shard::tests::schema_migration_shard_crash_child")
                .arg("--nocapture")
                .env("BRISKDB_SHARD_CRASH_PATH", &path)
                .env("BRISKDB_SHARD_CRASH_POINT", crash_point)
                .status()
                .unwrap();
            assert!(!status.success(), "child did not abort at {crash_point}");

            assert_eq!(identity(&path), (SHARD_APPLICATION_ID, 0));
            assert!(!schema_object_exists(&path, "shard_crash_marker"));
            assert_eq!(
                apply_schema_migration(&path, 0, 0, 1, &ready, SHARD_CRASH_SQL).unwrap(),
                SchemaMigrationShardOutcome::Applied
            );
            assert_eq!(identity(&path), (SHARD_APPLICATION_ID, 1));
            assert!(schema_object_exists(&path, "shard_crash_marker"));
            assert_eq!(
                Connection::open(&path)
                    .unwrap()
                    .query_row(
                        "SELECT value FROM shard_crash_marker WHERE id = 1",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .unwrap(),
                "persisted"
            );
        }
    }

    #[test]
    fn migration_prefix_allows_only_the_single_commit_before_acknowledgement_slot() {
        let (_temp, shards, ready) = create_ready_layout(4);
        let sql = "CREATE TABLE prefix_marker (id INTEGER)";
        assert_eq!(
            validate_schema_migration_prefix(&shards, 4, 0, 0, 1, &ready).unwrap(),
            Some(SchemaMigrationShardState::Source)
        );

        apply_schema_migration(&shard_path(&shards, 0), 0, 0, 1, &ready, sql).unwrap();
        assert_eq!(
            validate_schema_migration_prefix(&shards, 4, 0, 0, 1, &ready).unwrap(),
            Some(SchemaMigrationShardState::Target)
        );
        assert_eq!(
            validate_schema_migration_prefix(&shards, 4, 1, 0, 1, &ready).unwrap(),
            Some(SchemaMigrationShardState::Source)
        );

        apply_schema_migration(&shard_path(&shards, 2), 2, 0, 1, &ready, sql).unwrap();
        assert_eq!(
            validate_schema_migration_prefix(&shards, 4, 1, 0, 1, &ready)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );

        let (_temp, shards, ready) = create_ready_layout(2);
        for shard_id in 0..2 {
            apply_schema_migration(&shard_path(&shards, shard_id), shard_id, 0, 1, &ready, sql)
                .unwrap();
        }
        assert_eq!(
            validate_schema_migration_prefix(&shards, 2, 2, 0, 1, &ready).unwrap(),
            None
        );
    }

    #[test]
    fn migration_prefix_regression_is_corruption_and_future_generation_is_newer() {
        let (_temp, shards, ready) = create_ready_layout(2);
        let first = shard_path(&shards, 0);
        apply_schema_migration(
            &first,
            0,
            0,
            1,
            &ready,
            "CREATE TABLE regression_marker (id INTEGER)",
        )
        .unwrap();
        Connection::open(&first)
            .unwrap()
            .pragma_update(None, "user_version", 0)
            .unwrap();
        assert_eq!(
            validate_schema_migration_prefix(&shards, 2, 1, 0, 1, &ready)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );

        Connection::open(&first)
            .unwrap()
            .pragma_update(None, "user_version", 2)
            .unwrap();
        assert_eq!(
            validate_schema_migration_prefix(&shards, 2, 0, 0, 1, &ready)
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
    }

    #[test]
    fn migration_validation_rejects_nonadjacent_overflow_and_nonready_inputs() {
        let (_temp, shards, ready) = create_ready_layout(2);
        let path = shard_path(&shards, 0);
        for (source, target, layout, expected) in [
            (0, 2, ready, EngineErrorKind::FailedPrecondition),
            (u64::MAX, 0, ready, EngineErrorKind::FailedPrecondition),
            (
                0,
                1,
                layout(ShardLayoutState::Adopting),
                EngineErrorKind::DataCorruption,
            ),
        ] {
            assert_eq!(
                validate_schema_migration_connection(
                    &Connection::open(&path).unwrap(),
                    &path,
                    0,
                    source,
                    target,
                    &layout,
                )
                .unwrap_err()
                .kind(),
                expected
            );
        }
        assert_eq!(
            validate_schema_migration_connection(
                &Connection::open(&path).unwrap(),
                &path,
                0,
                i32::MAX as u64,
                i32::MAX as u64 + 1,
                &ready,
            )
            .unwrap_err()
            .kind(),
            EngineErrorKind::FailedPrecondition
        );
    }

    #[test]
    fn connection_level_migration_keeps_the_callers_progress_handler() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let (_temp, shards, ready) = create_ready_layout(2);
        let path = shard_path(&shards, 0);
        let mut connection = open_required_file(&path).unwrap();
        configure_busy_timeout(&connection).unwrap();
        let cancelled = Arc::new(AtomicBool::new(false));
        let callback_cancelled = Arc::clone(&cancelled);
        connection
            .progress_handler(1, Some(move || callback_cancelled.load(Ordering::Relaxed)))
            .unwrap();
        assert_eq!(
            validate_schema_migration_connection(&connection, &path, 0, 0, 1, &ready).unwrap(),
            SchemaMigrationShardState::Source
        );

        cancelled.store(true, Ordering::Relaxed);
        let error = apply_schema_migration_on_connection(
            &mut connection,
            &path,
            0,
            0,
            1,
            &ready,
            "CREATE TABLE cancellation_marker (id INTEGER)",
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        drop(connection);
        assert_eq!(identity(&path), (SHARD_APPLICATION_ID, 0));
        assert!(!schema_object_exists(&path, "cancellation_marker"));
    }

    #[test]
    fn strict_existing_opens_are_deterministic_in_parallel() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap();
        let path = Arc::new(shard_path(&shards, 0));
        let workers = (0..8)
            .map(|_| {
                let path = Arc::clone(&path);
                thread::spawn(move || {
                    for _ in 0..100 {
                        let connection =
                            open_existing(path.as_ref(), 0, 0, &layout(ShardLayoutState::Ready))
                                .unwrap();
                        assert_eq!(
                            read_identity(&connection).unwrap(),
                            (SHARD_APPLICATION_ID, 0)
                        );
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
    }

    #[test]
    fn client_authorizer_denies_every_persistent_ddl_and_allows_application_dml() {
        for pragma in [
            "application_id",
            "USER_VERSION",
            "journal_mode",
            "writable_schema",
            "schema_version",
        ] {
            assert!(denies_client_action(AuthAction::Pragma {
                pragma_name: pragma,
                pragma_value: Some("1"),
            }));
            assert!(!denies_client_action(AuthAction::Pragma {
                pragma_name: pragma,
                pragma_value: None,
            }));
        }
        assert!(denies_client_action(AuthAction::Insert {
            table_name: "BRISKDB_SHARD_METADATA",
        }));
        assert!(denies_client_action(AuthAction::Update {
            table_name: SHARD_METADATA_TABLE,
            column_name: "shard_id",
        }));
        assert!(denies_client_action(AuthAction::DropTable {
            table_name: SHARD_METADATA_TABLE,
        }));
        assert!(denies_client_action(AuthAction::CreateTable {
            table_name: "briskdb_future",
        }));
        assert!(denies_client_action(AuthAction::CreateTable {
            table_name: "widgets",
        }));
        assert!(denies_client_action(AuthAction::CreateIndex {
            index_name: "widgets_idx",
            table_name: "widgets",
        }));
        assert!(denies_client_action(AuthAction::CreateTrigger {
            trigger_name: "widgets_trigger",
            table_name: "widgets",
        }));
        assert!(denies_client_action(AuthAction::CreateView {
            view_name: "widget_view",
        }));
        assert!(denies_client_action(AuthAction::CreateVtable {
            table_name: "widget_search",
            module_name: "fts5",
        }));
        assert!(denies_client_action(AuthAction::DropIndex {
            index_name: "widgets_idx",
            table_name: "widgets",
        }));
        assert!(denies_client_action(AuthAction::DropTrigger {
            trigger_name: "widgets_trigger",
            table_name: "widgets",
        }));
        assert!(denies_client_action(AuthAction::DropView {
            view_name: "widget_view",
        }));
        assert!(denies_client_action(AuthAction::DropVtable {
            table_name: "widget_search",
            module_name: "fts5",
        }));
        assert!(denies_client_action(AuthAction::AlterTable {
            database_name: "main",
            table_name: "widgets",
        }));
        assert!(denies_client_action(AuthAction::Read {
            table_name: SHARD_METADATA_TABLE,
            column_name: "shard_id",
        }));
        assert!(!denies_client_action(AuthAction::Read {
            table_name: "widgets",
            column_name: "value",
        }));
        assert!(!denies_client_action(AuthAction::Insert {
            table_name: "widgets",
        }));
        assert!(!denies_client_action(AuthAction::Update {
            table_name: "widgets",
            column_name: "value",
        }));
        assert!(!denies_client_action(AuthAction::Delete {
            table_name: "widgets",
        }));
        assert!(!denies_client_action(AuthAction::CreateTempTable {
            table_name: "temporary_widgets",
        }));
        assert!(!denies_client_action(AuthAction::Transaction {
            operation: TransactionOperation::Begin,
        }));
    }

    #[test]
    fn migration_authorizer_action_matrix_is_fail_closed() {
        let context = |action, database_name| AuthContext {
            action,
            database_name,
            accessor: None,
        };
        assert!(!denies_schema_migration_action(context(
            AuthAction::CreateTable {
                table_name: "widgets",
            },
            Some("main"),
        )));
        assert!(!denies_schema_migration_action(context(
            AuthAction::AlterTable {
                database_name: "main",
                table_name: "widgets",
            },
            Some("main"),
        )));
        assert!(!denies_schema_migration_action(context(
            AuthAction::Insert {
                table_name: "widgets",
            },
            Some("main"),
        )));
        for table_name in ["briskdb", "BRISKDB", "briskdb_private"] {
            assert!(denies_schema_migration_action(context(
                AuthAction::CreateTable { table_name },
                Some("main"),
            )));
        }
        assert!(denies_schema_migration_action(context(
            AuthAction::Read {
                table_name: SHARD_METADATA_TABLE,
                column_name: "shard_id",
            },
            Some("main"),
        )));
        assert!(denies_schema_migration_action(context(
            AuthAction::Pragma {
                pragma_name: "user_version",
                pragma_value: None,
            },
            None,
        )));
        assert!(denies_schema_migration_action(context(
            AuthAction::Pragma {
                pragma_name: "cache_size",
                pragma_value: Some("20"),
            },
            None,
        )));
        assert!(denies_schema_migration_action(context(
            AuthAction::Transaction {
                operation: TransactionOperation::Begin,
            },
            None,
        )));
        assert!(denies_schema_migration_action(context(
            AuthAction::Savepoint {
                operation: TransactionOperation::Begin,
                savepoint_name: "escaped",
            },
            None,
        )));
        assert!(denies_schema_migration_action(context(
            AuthAction::Attach {
                filename: "other.sqlite",
            },
            None,
        )));
        assert!(denies_schema_migration_action(context(
            AuthAction::CreateTempTable {
                table_name: "temporary_widgets",
            },
            Some("temp"),
        )));
        assert!(denies_schema_migration_action(context(
            AuthAction::CreateVtable {
                table_name: "widget_search",
                module_name: "fts5",
            },
            Some("main"),
        )));
        assert!(denies_schema_migration_action(context(
            AuthAction::Select,
            Some("auxiliary"),
        )));
        assert!(denies_schema_migration_action(context(
            AuthAction::Unknown {
                code: -1,
                arg1: None,
                arg2: None,
            },
            None,
        )));
    }
}
