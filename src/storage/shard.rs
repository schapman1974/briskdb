//! Physical-shard identity, provisioning, and strict reopen validation.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    path::{Path, PathBuf},
};

use rusqlite::{
    Connection, MAIN_DB, OpenFlags, OptionalExtension, TransactionBehavior,
    hooks::{AuthAction, AuthContext, Authorization},
};

use crate::{
    core::{
        Catalog, EngineError, EngineErrorKind, EngineResult, GeneratedIdPolicy, ShardKeyType,
        TableDeclaration, TablePlacement,
    },
    sqlite_error,
};

use super::CONNECTION_BUSY_TIMEOUT;

/// `BRSH` encoded as SQLite's 32-bit application identifier.
pub(super) const SHARD_APPLICATION_ID: i64 = 0x4252_5348;
/// Version of the storage-owned shard metadata table.
pub(super) const SHARD_METADATA_VERSION: u32 = 1;
/// Version of the canonical application-schema fingerprint encoding.
pub(super) const SHARD_SCHEMA_DIGEST_VERSION: u32 = 1;

const SHARD_METADATA_TABLE: &str = "briskdb_shard_metadata";
const SHARD_SCHEMA_DIGEST_DOMAIN: &[u8] = b"briskdb.shard.application-schema.v1\0";
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

/// Canonical BLAKE3 fingerprint of one generation's persistent application schema.
pub(super) type SchemaDigest = [u8; 32];

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

/// Open and validate a required shard through an OS-level read-only SQLite
/// handle. This retains the same no-create, no-follow, identity, generation,
/// metadata, WAL-mode, and schema-integrity preconditions as a writable open.
#[cfg(feature = "experimental-vtab")]
pub(super) fn open_existing_read_only(
    path: &Path,
    shard_id: u16,
    schema_generation: u64,
    layout: &ShardLayout,
) -> EngineResult<Connection> {
    let connection = open_required_file_read_only(path)?;
    configure_busy_timeout(&connection)?;
    validate_open_read_only_connection(&connection, path, shard_id, schema_generation, layout)?;
    Ok(connection)
}

/// Open a required shard through an OS-level read-only handle while leaving
/// validation to the caller. The controlled virtual-table bootstrap path uses
/// this split so it can install cancellation hooks before validation touches
/// SQLite schema state that may be locked by another process.
#[cfg(feature = "experimental-vtab")]
pub(super) fn open_required_file_read_only(path: &Path) -> EngineResult<Connection> {
    validate_existing_file(path)?;
    open_existing_read_only_connection(path)
}

/// Validate an already-open read-only shard without replacing its busy handler
/// or progress hook.
#[cfg(feature = "experimental-vtab")]
pub(super) fn validate_open_read_only_connection(
    connection: &Connection,
    path: &Path,
    shard_id: u16,
    schema_generation: u64,
    layout: &ShardLayout,
) -> EngineResult<()> {
    configure_cell_size_check(connection)?;
    validate_shard_id(shard_id)?;
    let expected_user_version = expected_user_version(schema_generation)?;
    require_read_only(connection)?;
    validate_exact_shard(connection, path, shard_id, expected_user_version, layout)?;
    Ok(())
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
    configure_cell_size_check(connection)?;
    validate_shard_id(shard_id)?;
    let expected_user_version = expected_user_version(schema_generation)?;
    require_writable(connection)?;
    validate_exact_shard(connection, path, shard_id, expected_user_version, layout)?;
    configure_connection_pragmas(connection)
}

/// Calculate the canonical fingerprint of the persistent application schema.
///
/// The encoding is domain-separated and generation-bound. Rows are streamed in
/// binary order from `main.sqlite_schema`; SQLite-owned `sqlite_*` objects and
/// the one storage-owned shard-metadata table are omitted. Every text field is
/// length-prefixed, including the nullable SQL field, so no tuple ambiguity is
/// possible. Application data, root pages, shard identity, and WAL state do not
/// participate in the fingerprint.
pub(super) fn calculate_schema_digest(
    connection: &Connection,
    schema_generation: u64,
) -> EngineResult<SchemaDigest> {
    configure_cell_size_check(connection)?;
    expected_user_version(schema_generation)?;

    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, sql
             FROM main.sqlite_schema
             ORDER BY
                 type COLLATE BINARY,
                 name COLLATE BINARY,
                 tbl_name COLLATE BINARY,
                 sql COLLATE BINARY",
        )
        .map_err(|error| shard_read_error(error, "failed to inspect shard application schema"))?;
    let mut rows = statement
        .query([])
        .map_err(|error| shard_read_error(error, "failed to inspect shard application schema"))?;

    let mut hasher = blake3::Hasher::new();
    hasher.update(SHARD_SCHEMA_DIGEST_DOMAIN);
    hasher.update(&SHARD_SCHEMA_DIGEST_VERSION.to_le_bytes());
    hasher.update(&schema_generation.to_le_bytes());

    while let Some(row) = rows
        .next()
        .map_err(|error| shard_read_error(error, "failed to read shard application schema"))?
    {
        let object_type = row
            .get::<_, String>(0)
            .map_err(|error| shard_read_error(error, "failed to read shard application schema"))?;
        let name = row
            .get::<_, String>(1)
            .map_err(|error| shard_read_error(error, "failed to read shard application schema"))?;
        let table_name = row
            .get::<_, String>(2)
            .map_err(|error| shard_read_error(error, "failed to read shard application schema"))?;
        let sql = row
            .get::<_, Option<String>>(3)
            .map_err(|error| shard_read_error(error, "failed to read shard application schema"))?;

        if is_sqlite_schema_name(&name) {
            continue;
        }
        if is_exact_metadata_schema_object(&object_type, &name, &table_name, sql.as_deref()) {
            continue;
        }
        if is_reserved_name(&name) || is_reserved_name(&table_name) {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "shard application schema contains an unexpected reserved object",
            ));
        }

        hasher.update(&[1]);
        hash_schema_text(&mut hasher, &object_type)?;
        hash_schema_text(&mut hasher, &name)?;
        hash_schema_text(&mut hasher, &table_name)?;
        match sql {
            Some(sql) => {
                hasher.update(&[1]);
                hash_schema_text(&mut hasher, &sql)?;
            }
            None => {
                hasher.update(&[0]);
            }
        }
    }
    hasher.update(&[0]);
    Ok(*hasher.finalize().as_bytes())
}

/// Require the current persistent application schema to match a trusted digest.
pub(super) fn verify_schema_digest(
    connection: &Connection,
    schema_generation: u64,
    expected: &SchemaDigest,
) -> EngineResult<()> {
    if calculate_schema_digest(connection, schema_generation)? == *expected {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "shard application schema does not match its trusted fingerprint",
        ))
    }
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
    configure_cell_size_check(connection)?;
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
#[cfg(test)]
pub(super) fn preflight_schema_migration(
    path: &Path,
    shard_id: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &ShardLayout,
    sql: &str,
) -> EngineResult<SchemaMigrationShardState> {
    preflight_schema_migration_with_digest(
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
        sql,
    )
    .map(|(state, _)| state)
}

/// Preflight a migration and return its generation-bound target-schema digest.
#[cfg(test)]
pub(super) fn preflight_schema_migration_with_digest(
    path: &Path,
    shard_id: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &ShardLayout,
    sql: &str,
) -> EngineResult<(SchemaMigrationShardState, SchemaDigest)> {
    let mut connection = open_required_file(path)?;
    configure_busy_timeout(&connection)?;
    preflight_schema_migration_on_connection_with_digest(
        &mut connection,
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
        sql,
    )
}

/// Connection-level digesting preflight for a cancellation-aware coordinator.
#[cfg(test)]
pub(super) fn preflight_schema_migration_on_connection_with_digest(
    connection: &mut Connection,
    path: &Path,
    shard_id: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &ShardLayout,
    sql: &str,
) -> EngineResult<(SchemaMigrationShardState, SchemaDigest)> {
    preflight_schema_migration_on_connection_with_digest_inner(
        connection,
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
        sql,
        None,
    )
}

/// Connection-level digesting preflight that also preserves the complete
/// authoritative table catalog. The catalog check runs inside the rollback
/// transaction after the SQL batch, before its target digest is trusted.
#[allow(clippy::too_many_arguments)]
pub(super) fn preflight_schema_migration_on_connection_with_digest_and_catalog(
    connection: &mut Connection,
    path: &Path,
    shard_id: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &ShardLayout,
    sql: &str,
    catalog: &Catalog,
) -> EngineResult<(SchemaMigrationShardState, SchemaDigest)> {
    preflight_schema_migration_on_connection_with_digest_inner(
        connection,
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
        sql,
        Some(catalog),
    )
}

#[allow(clippy::too_many_arguments)]
fn preflight_schema_migration_on_connection_with_digest_inner(
    connection: &mut Connection,
    path: &Path,
    shard_id: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &ShardLayout,
    sql: &str,
    catalog: Option<&Catalog>,
) -> EngineResult<(SchemaMigrationShardState, SchemaDigest)> {
    if catalog.is_some_and(|catalog| !catalog.tables().is_empty()) {
        crate::sql::validate_authoritative_schema_migration(sql)?;
    }
    let initial = validate_schema_migration_connection(
        connection,
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
    )?;
    if initial == SchemaMigrationShardState::Target {
        if let Some(catalog) = catalog {
            validate_registered_table_schema(connection, catalog)?;
        }
        let digest = calculate_schema_digest(connection, target_generation)?;
        return Ok((initial, digest));
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
        let state = validate_schema_migration_connection(
            connection,
            path,
            shard_id,
            source_generation,
            target_generation,
            layout,
        )?;
        if let Some(catalog) = catalog {
            validate_registered_table_schema(connection, catalog)?;
        }
        let digest = calculate_schema_digest(connection, target_generation)?;
        return Ok((state, digest));
    }

    let reserved_before = reserved_schema_snapshot(&transaction)?;
    execute_schema_migration_batch(&transaction, sql)?;
    ensure_reserved_schema_unchanged(&reserved_before, &transaction)?;
    if let Some(catalog) = catalog {
        validate_registered_table_schema(&transaction, catalog)?;
    }
    ensure_no_foreign_key_violations(&transaction)?;
    let target_digest = calculate_schema_digest(&transaction, target_generation)?;
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
    Ok((state, target_digest))
}

/// Verify that one physical shard still implements the complete authoritative
/// logical table catalog after a tentative schema migration.
///
/// An empty table catalog predates authoritative registration and deliberately
/// retains the unrestricted migration behavior. Once any table is registered,
/// every physical application table must be declared Sharded or Global, every
/// such declaration must have a physical table, and Catalog declarations must
/// remain manifest-only. Sharded keys additionally retain the representation
/// required by deterministic routing.
pub(super) fn validate_registered_table_schema(
    connection: &Connection,
    catalog: &Catalog,
) -> EngineResult<()> {
    if catalog.tables().is_empty() {
        return Ok(());
    }

    let has_application_trigger = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM main.sqlite_schema
                 WHERE type = 'trigger' AND name NOT GLOB 'sqlite_*'
             )",
            [],
            |row| row.get::<_, bool>(0),
        )
        .map_err(sqlite_error::storage)?;
    if has_application_trigger {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "schema migration violates the authoritative table catalog: application triggers are not supported",
        ));
    }
    let has_application_virtual_table = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM pragma_table_list
                 WHERE schema = 'main' AND type = 'virtual'
             )",
            [],
            |row| row.get::<_, bool>(0),
        )
        .map_err(sqlite_error::storage)?;
    if has_application_virtual_table {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "schema migration violates the authoritative table catalog: virtual tables are not supported",
        ));
    }
    validate_stateless_catalog_schema(connection)?;

    let expected = catalog
        .tables()
        .iter()
        .filter(|table| {
            matches!(
                table.placement(),
                TablePlacement::Sharded(_) | TablePlacement::Global
            )
        })
        .map(|table| table.name().to_owned())
        .collect::<BTreeSet<_>>();
    let observed = application_table_names(connection)?;
    if observed != expected {
        let missing = expected.difference(&observed).next().map(String::as_str);
        let unexpected = observed.difference(&expected).next().map(String::as_str);
        let detail = match (missing, unexpected) {
            (Some(missing), Some(unexpected)) => {
                format!("missing table {missing} and found undeclared table {unexpected}")
            }
            (Some(missing), None) => format!("missing table {missing}"),
            (None, Some(unexpected)) => format!("found undeclared table {unexpected}"),
            (None, None) => "physical table set differs".to_owned(),
        };
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("schema migration violates the authoritative table catalog: {detail}"),
        ));
    }

    for table in catalog.tables() {
        if matches!(table.placement(), TablePlacement::Catalog) {
            let has_physical_shadow = connection
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM main.sqlite_schema
                         WHERE name = ?1 COLLATE NOCASE
                           AND type IN ('table', 'view')
                     )",
                    [table.name()],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(sqlite_error::storage)?;
            if has_physical_shadow {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "schema migration violates the authoritative table catalog: catalog table {} has a physical shadow",
                        table.name()
                    ),
                ));
            }
            continue;
        }
        let TablePlacement::Sharded(shard_key) = table.placement() else {
            validate_authoritative_table_constraints(
                connection,
                table.name(),
                AuthoritativeTableConstraints::new(table.placement(), table.generated_id_policy()),
                |parent| {
                    catalog
                        .tables()
                        .iter()
                        .find(|candidate| {
                            candidate.database_id() == table.database_id()
                                && candidate.name().eq_ignore_ascii_case(parent)
                        })
                        .map(|candidate| {
                            AuthoritativeTableConstraints::new(
                                candidate.placement(),
                                candidate.generated_id_policy(),
                            )
                        })
                },
            )?;
            continue;
        };
        let mut statement = connection
            .prepare(
                "SELECT name, type, \"notnull\", pk, hidden
                 FROM pragma_table_xinfo(?1)
                 ORDER BY cid",
            )
            .map_err(sqlite_error::storage)?;
        let columns = statement
            .query_map([table.name()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .map_err(sqlite_error::storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_error::storage)?;
        let Some((_, declared_type, not_null, primary_key, hidden)) = columns
            .iter()
            .find(|(name, _, _, _, _)| name == shard_key.column())
        else {
            return Err(registered_shard_key_error(
                table.name(),
                shard_key.column(),
                "is missing",
            ));
        };
        if *hidden != 0
            || !registered_shard_key_is_non_null(
                connection,
                table.name(),
                declared_type,
                *not_null,
                *primary_key,
            )?
        {
            return Err(registered_shard_key_error(
                table.name(),
                shard_key.column(),
                "must remain a visible physically non-null column",
            ));
        }
        if !shard_key_affinity_is_compatible(shard_key.key_type(), declared_type) {
            return Err(registered_shard_key_error(
                table.name(),
                shard_key.column(),
                "has an incompatible SQLite declared type",
            ));
        }
        if matches!(shard_key.key_type(), ShardKeyType::Text)
            && !shard_key_uses_binary_collation(connection, table.name(), shard_key.column())?
        {
            return Err(registered_shard_key_error(
                table.name(),
                shard_key.column(),
                "must retain SQLite BINARY collation",
            ));
        }
        validate_authoritative_table_constraints(
            connection,
            table.name(),
            AuthoritativeTableConstraints::new(table.placement(), table.generated_id_policy()),
            |parent| {
                catalog
                    .tables()
                    .iter()
                    .find(|candidate| {
                        candidate.database_id() == table.database_id()
                            && candidate.name().eq_ignore_ascii_case(parent)
                    })
                    .map(|candidate| {
                        AuthoritativeTableConstraints::new(
                            candidate.placement(),
                            candidate.generated_id_policy(),
                        )
                    })
            },
        )?;
    }
    Ok(())
}

/// Validate one registration candidate against the complete authoritative
/// declaration set. Foreign-key safety depends on both sides' placements, so
/// validating declarations independently would admit unresolved relationships.
pub(super) fn validate_declared_table_constraints(
    connection: &Connection,
    declaration: &TableDeclaration,
    declarations: &[TableDeclaration],
) -> EngineResult<()> {
    validate_authoritative_table_constraints(
        connection,
        declaration.name(),
        AuthoritativeTableConstraints::new(
            declaration.placement(),
            declaration.generated_id_policy(),
        ),
        |parent| {
            declarations
                .iter()
                .find(|candidate| {
                    candidate.database_id() == declaration.database_id()
                        && candidate.name().eq_ignore_ascii_case(parent)
                })
                .map(|candidate| {
                    AuthoritativeTableConstraints::new(
                        candidate.placement(),
                        candidate.generated_id_policy(),
                    )
                })
        },
    )
}

#[derive(Debug, Clone, Copy)]
struct AuthoritativeTableConstraints<'a> {
    placement: &'a TablePlacement,
    generated_id_policy: &'a GeneratedIdPolicy,
}

impl<'a> AuthoritativeTableConstraints<'a> {
    const fn new(
        placement: &'a TablePlacement,
        generated_id_policy: &'a GeneratedIdPolicy,
    ) -> Self {
        Self {
            placement,
            generated_id_policy,
        }
    }
}

/// Enforce constraints that SQLite can only check inside one physical file.
/// Every unique key of a Sharded table must contain its routing key using
/// BINARY collation so two different owners cannot both accept the same value.
/// Foreign keys are accepted only when authoritative placements prove that the
/// referenced parent row is present in the same physical file.
fn validate_authoritative_table_constraints<'a, T>(
    connection: &Connection,
    table: &str,
    authoritative: AuthoritativeTableConstraints<'_>,
    authoritative_table: T,
) -> EngineResult<()>
where
    T: Fn(&str) -> Option<AuthoritativeTableConstraints<'a>>,
{
    require_foreign_keys_enabled(connection)?;
    validate_authoritative_foreign_keys(connection, table, authoritative, authoritative_table)?;

    let TablePlacement::Sharded(shard_key) = authoritative.placement else {
        return Ok(());
    };
    validate_authoritative_unique_constraints(connection, table, shard_key)
}

fn require_foreign_keys_enabled(connection: &Connection) -> EngineResult<()> {
    let enabled = connection
        .pragma_query_value(None, "foreign_keys", |row| row.get::<_, i64>(0))
        .map_err(sqlite_error::storage)?;
    if enabled != 1 {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "SQLite foreign-key enforcement is not enabled on a validated shard connection",
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct ForeignKeyBuilder {
    parent_table: String,
    on_update: String,
    on_delete: String,
    match_name: String,
    terms: Vec<ForeignKeyTerm>,
}

#[derive(Debug)]
struct ForeignKeyTerm {
    sequence: i64,
    child_column: String,
    parent_column: Option<String>,
}

fn validate_authoritative_foreign_keys<'a, T>(
    connection: &Connection,
    child_table: &str,
    child: AuthoritativeTableConstraints<'_>,
    authoritative_table: T,
) -> EngineResult<()>
where
    T: Fn(&str) -> Option<AuthoritativeTableConstraints<'a>>,
{
    let mut statement = connection
        .prepare(
            "SELECT id, seq, \"table\", \"from\", \"to\", on_update, on_delete, \"match\"
             FROM pragma_foreign_key_list(?1)
             ORDER BY id, seq",
        )
        .map_err(sqlite_error::storage)?;
    let rows = statement
        .query_map([child_table], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
            ))
        })
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)?;

    let mut foreign_keys = BTreeMap::<i64, ForeignKeyBuilder>::new();
    for (
        id,
        sequence,
        parent_table,
        child_column,
        parent_column,
        on_update,
        on_delete,
        match_name,
    ) in rows
    {
        let foreign_key = foreign_keys.entry(id).or_insert_with(|| ForeignKeyBuilder {
            parent_table: parent_table.clone(),
            on_update: on_update.clone(),
            on_delete: on_delete.clone(),
            match_name: match_name.clone(),
            terms: Vec::new(),
        });
        if !foreign_key.parent_table.eq_ignore_ascii_case(&parent_table)
            || !foreign_key.on_update.eq_ignore_ascii_case(&on_update)
            || !foreign_key.on_delete.eq_ignore_ascii_case(&on_delete)
            || !foreign_key.match_name.eq_ignore_ascii_case(&match_name)
        {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("foreign-key metadata for table {child_table} is inconsistent"),
            ));
        }
        foreign_key.terms.push(ForeignKeyTerm {
            sequence,
            child_column,
            parent_column,
        });
    }

    let has_foreign_keys = !foreign_keys.is_empty();
    for (id, mut foreign_key) in foreign_keys {
        if !foreign_key.match_name.eq_ignore_ascii_case("NONE") {
            return Err(foreign_key_precondition(
                child_table,
                id,
                format!("uses unsupported MATCH mode {}", foreign_key.match_name),
            ));
        }
        foreign_key.terms.sort_by_key(|term| term.sequence);
        if foreign_key
            .terms
            .iter()
            .enumerate()
            .any(|(expected, term)| term.sequence != expected as i64)
        {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("foreign-key metadata for table {child_table} has invalid term order"),
            ));
        }

        let Some(parent) = authoritative_table(&foreign_key.parent_table) else {
            return Err(foreign_key_precondition(
                child_table,
                id,
                format!(
                    "references missing authoritative table {}",
                    foreign_key.parent_table
                ),
            ));
        };
        if matches!(parent.placement, TablePlacement::Catalog) {
            return Err(foreign_key_precondition(
                child_table,
                id,
                format!("references catalog-only table {}", foreign_key.parent_table),
            ));
        }

        resolve_implicit_parent_columns(
            connection,
            child_table,
            &foreign_key.parent_table,
            &mut foreign_key.terms,
        )?;
        validate_foreign_key_placement(child_table, id, child, parent, &foreign_key)?;
    }
    if has_foreign_keys {
        validate_sqlite_foreign_key_schema(connection, child_table)?;
    }
    Ok(())
}

/// Ask SQLite to compile, but not execute, DML against a foreign-key child.
/// SQLite resolves every referenced parent key while building this program, so
/// malformed parent columns, missing UNIQUE keys, and incompatible parent-index
/// collations fail here instead of surfacing later as generic `SQLITE_ERROR`.
fn validate_sqlite_foreign_key_schema(
    connection: &Connection,
    child_table: &str,
) -> EngineResult<()> {
    let quoted_table = format!("\"{}\"", child_table.replace('"', "\"\""));
    let sql = format!("EXPLAIN DELETE FROM {quoted_table} WHERE 0");
    match connection.prepare(&sql) {
        Ok(_) => Ok(()),
        Err(error) => {
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
                    | EngineErrorKind::DataCorruption
            ) {
                return Err(classified.context(format!(
                    "failed to validate foreign-key schema involving table {child_table}"
                )));
            }
            Err(EngineError::from_source(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "foreign-key schema involving table {child_table} cannot be enforced by SQLite"
                ),
                classified,
            ))
        }
    }
}

fn resolve_implicit_parent_columns(
    connection: &Connection,
    child_table: &str,
    parent_table: &str,
    terms: &mut [ForeignKeyTerm],
) -> EngineResult<()> {
    if terms.iter().all(|term| term.parent_column.is_some()) {
        return Ok(());
    }
    if terms.iter().any(|term| term.parent_column.is_some()) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "foreign key from {child_table} to {parent_table} mixes explicit and implicit parent columns"
            ),
        ));
    }

    let mut statement = connection
        .prepare("SELECT name FROM pragma_table_xinfo(?1) WHERE pk <> 0 ORDER BY pk")
        .map_err(sqlite_error::storage)?;
    let parent_key = statement
        .query_map([parent_table], |row| row.get::<_, String>(0))
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)?;
    if parent_key.is_empty() || parent_key.len() != terms.len() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "foreign key from {child_table} to {parent_table} omits referenced columns, but the parent has no matching resolvable primary key"
            ),
        ));
    }
    for (term, parent_column) in terms.iter_mut().zip(parent_key) {
        term.parent_column = Some(parent_column);
    }
    Ok(())
}

fn validate_foreign_key_placement(
    child_table: &str,
    id: i64,
    child: AuthoritativeTableConstraints<'_>,
    parent: AuthoritativeTableConstraints<'_>,
    foreign_key: &ForeignKeyBuilder,
) -> EngineResult<()> {
    #[allow(unreachable_patterns)]
    match (child.placement, parent.placement) {
        (TablePlacement::Sharded(child_key), TablePlacement::Sharded(parent_key)) => {
            if child_key.key_type() != parent_key.key_type() {
                return Err(foreign_key_precondition(
                    child_table,
                    id,
                    "maps shard keys with different authoritative types",
                ));
            }
            if !generated_id_routing_domains_match(
                child.generated_id_policy,
                parent.generated_id_policy,
            ) {
                return Err(foreign_key_precondition(
                    child_table,
                    id,
                    "maps shard keys with different generated-ID routing domains",
                ));
            }
            let mapped_terms = foreign_key
                .terms
                .iter()
                .filter(|term| term.child_column.eq_ignore_ascii_case(child_key.column()))
                .collect::<Vec<_>>();
            let parent_key_terms = foreign_key
                .terms
                .iter()
                .filter(|term| {
                    term.parent_column
                        .as_deref()
                        .is_some_and(|column| column.eq_ignore_ascii_case(parent_key.column()))
                })
                .count();
            if mapped_terms.len() != 1
                || parent_key_terms != 1
                || !mapped_terms[0]
                    .parent_column
                    .as_deref()
                    .is_some_and(|column| column.eq_ignore_ascii_case(parent_key.column()))
            {
                return Err(foreign_key_precondition(
                    child_table,
                    id,
                    format!(
                        "does not map child shard key {} exactly once to parent shard key {}",
                        child_key.column(),
                        parent_key.column()
                    ),
                ));
            }
            validate_shard_key_foreign_key_actions(child_table, id, child_key.column(), foreign_key)
        }
        (TablePlacement::Sharded(child_key), TablePlacement::Global) => {
            if foreign_key
                .terms
                .iter()
                .any(|term| term.child_column.eq_ignore_ascii_case(child_key.column()))
            {
                validate_shard_key_foreign_key_actions(
                    child_table,
                    id,
                    child_key.column(),
                    foreign_key,
                )?;
            }
            Ok(())
        }
        (TablePlacement::Global, TablePlacement::Global) => Ok(()),
        (TablePlacement::Global, TablePlacement::Sharded(_)) => Err(foreign_key_precondition(
            child_table,
            id,
            "a Global child cannot reference a Sharded parent",
        )),
        (TablePlacement::Catalog, _) => Err(foreign_key_precondition(
            child_table,
            id,
            "a Catalog child cannot have a physical foreign key",
        )),
        (_, TablePlacement::Catalog) => Err(foreign_key_precondition(
            child_table,
            id,
            "a physical child cannot reference a Catalog parent",
        )),
        (_, _) => Err(foreign_key_precondition(
            child_table,
            id,
            "uses an unsupported authoritative placement relationship",
        )),
    }
}

fn generated_id_routing_domains_match(
    child: &GeneratedIdPolicy,
    parent: &GeneratedIdPolicy,
) -> bool {
    matches!(
        (child, parent),
        (GeneratedIdPolicy::None, GeneratedIdPolicy::None)
            | (
                GeneratedIdPolicy::NativeRangeV1 { .. },
                GeneratedIdPolicy::NativeRangeV1 { .. }
            )
    )
}

fn validate_shard_key_foreign_key_actions(
    child_table: &str,
    id: i64,
    shard_key: &str,
    foreign_key: &ForeignKeyBuilder,
) -> EngineResult<()> {
    if !foreign_key.on_update.eq_ignore_ascii_case("NO ACTION")
        && !foreign_key.on_update.eq_ignore_ascii_case("RESTRICT")
    {
        return Err(foreign_key_precondition(
            child_table,
            id,
            format!(
                "uses ON UPDATE {} on shard key {shard_key}",
                foreign_key.on_update
            ),
        ));
    }
    if foreign_key.on_delete.eq_ignore_ascii_case("SET NULL")
        || foreign_key.on_delete.eq_ignore_ascii_case("SET DEFAULT")
    {
        return Err(foreign_key_precondition(
            child_table,
            id,
            format!(
                "uses ON DELETE {} on shard key {shard_key}",
                foreign_key.on_delete
            ),
        ));
    }
    Ok(())
}

fn foreign_key_precondition(
    child_table: &str,
    id: i64,
    detail: impl std::fmt::Display,
) -> EngineError {
    EngineError::new(
        EngineErrorKind::FailedPrecondition,
        format!("foreign key {id} on table {child_table} {detail}"),
    )
}

fn validate_authoritative_unique_constraints(
    connection: &Connection,
    table: &str,
    shard_key: &crate::core::ShardKeyMetadata,
) -> EngineResult<()> {
    let mut statement = connection
        .prepare(
            "SELECT name, origin
             FROM pragma_index_list(?1)
             WHERE \"unique\" <> 0
             ORDER BY seq",
        )
        .map_err(sqlite_error::storage)?;
    let unique_indexes = statement
        .query_map([table], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)?;
    let has_primary_key_index = unique_indexes.iter().any(|(_, origin)| origin == "pk");

    for (index, _) in &unique_indexes {
        let mut columns = connection
            .prepare(
                "SELECT name, coll
                 FROM pragma_index_xinfo(?1)
                 WHERE key = 1
                 ORDER BY seqno",
            )
            .map_err(sqlite_error::storage)?;
        let indexed_columns = columns
            .query_map([index], |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                ))
            })
            .map_err(sqlite_error::storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_error::storage)?;
        let has_shard_term = indexed_columns
            .iter()
            .any(|(column, _)| column.as_deref() == Some(shard_key.column()));
        if !has_shard_term {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "unique index {index} on sharded table {table} does not include shard key {}",
                    shard_key.column()
                ),
            ));
        }
        let has_binary_shard_term = indexed_columns.iter().any(|(column, collation)| {
            column.as_deref() == Some(shard_key.column())
                && collation
                    .as_deref()
                    .is_some_and(|collation| collation.eq_ignore_ascii_case("BINARY"))
        });
        if !has_binary_shard_term {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "unique index {index} on sharded table {table} must use BINARY collation for shard key {}",
                    shard_key.column()
                ),
            ));
        }
    }

    if !has_primary_key_index {
        let rowid_primary_key = connection
            .query_row(
                "SELECT name
                 FROM pragma_table_xinfo(?1)
                 WHERE pk <> 0
                 ORDER BY pk
                 LIMIT 1",
                [table],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sqlite_error::storage)?;
        if rowid_primary_key
            .as_deref()
            .is_some_and(|column| column != shard_key.column())
        {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "primary key on sharded table {table} does not include shard key {}",
                    shard_key.column()
                ),
            ));
        }
    }
    Ok(())
}

/// Reject persistent expressions that could observe a previous stateless
/// catalog write through SQLite's connection-local counters.
pub(super) fn validate_stateless_catalog_schema(connection: &Connection) -> EngineResult<()> {
    let mut statement = connection
        .prepare(
            "SELECT type, name, sql
             FROM main.sqlite_schema
             WHERE type IN ('table', 'index') AND sql IS NOT NULL
               AND name NOT GLOB 'sqlite_*'
             ORDER BY type, name COLLATE BINARY",
        )
        .map_err(sqlite_error::storage)?;
    let objects = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)?;

    for (object_type, name, sql) in objects {
        crate::sql::validate_stateless_catalog_schema_sql(&sql).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::FailedPrecondition,
                format!("{object_type} {name} cannot participate in stateless catalog write reuse"),
                error,
            )
        })?;
    }
    Ok(())
}

pub(super) fn shard_key_uses_binary_collation(
    connection: &Connection,
    table: &str,
    column: &str,
) -> EngineResult<bool> {
    let (_, collation, _, _, _) = connection
        .column_metadata(Some("main"), table, column)
        .map_err(sqlite_error::storage)?;
    Ok(collation.is_some_and(|name| name.to_bytes().eq_ignore_ascii_case(b"BINARY")))
}

fn registered_shard_key_is_non_null(
    connection: &Connection,
    table: &str,
    declared_type: &str,
    not_null: i64,
    primary_key: i64,
) -> EngineResult<bool> {
    if not_null != 0 {
        return Ok(true);
    }
    if primary_key == 0 || !declared_type.trim().eq_ignore_ascii_case("INTEGER") {
        return Ok(false);
    }
    let has_primary_key_index = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM pragma_index_list(?1) WHERE origin = 'pk'
             )",
            [table],
            |row| row.get::<_, bool>(0),
        )
        .map_err(sqlite_error::storage)?;
    Ok(!has_primary_key_index)
}

fn application_table_names(connection: &Connection) -> EngineResult<BTreeSet<String>> {
    let mut statement = connection
        .prepare(
            "SELECT name FROM pragma_table_list
             WHERE schema = 'main' AND type IN ('table', 'virtual')
               AND name NOT GLOB 'sqlite_*'
               AND name <> 'briskdb_shard_metadata'
             ORDER BY name COLLATE BINARY",
        )
        .map_err(sqlite_error::storage)?;
    statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sqlite_error::storage)?
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(sqlite_error::storage)
}

fn registered_shard_key_error(table: &str, column: &str, detail: &str) -> EngineError {
    EngineError::new(
        EngineErrorKind::FailedPrecondition,
        format!(
            "schema migration violates the authoritative table catalog: shard key {column} on table {table} {detail}"
        ),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqliteAffinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
}

fn shard_key_affinity_is_compatible(key_type: ShardKeyType, declared_type: &str) -> bool {
    let declared_type = declared_type.to_ascii_uppercase();
    let affinity = if declared_type.contains("INT") {
        SqliteAffinity::Integer
    } else if declared_type.contains("CHAR")
        || declared_type.contains("CLOB")
        || declared_type.contains("TEXT")
    {
        SqliteAffinity::Text
    } else if declared_type.contains("BLOB") || declared_type.is_empty() {
        SqliteAffinity::Blob
    } else if declared_type.contains("REAL")
        || declared_type.contains("FLOA")
        || declared_type.contains("DOUB")
    {
        SqliteAffinity::Real
    } else {
        SqliteAffinity::Numeric
    };
    matches!(
        (key_type, affinity),
        (ShardKeyType::Int64, SqliteAffinity::Integer)
            | (ShardKeyType::Text, SqliteAffinity::Text)
            | (ShardKeyType::Binary, SqliteAffinity::Blob)
    )
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

/// Apply one migration while requiring its exact trusted target fingerprint.
pub(super) fn apply_schema_migration_with_digest(
    path: &Path,
    shard_id: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &ShardLayout,
    sql: &str,
    expected_target_digest: &SchemaDigest,
) -> EngineResult<SchemaMigrationShardOutcome> {
    let mut connection = open_required_file(path)?;
    configure_busy_timeout(&connection)?;
    apply_schema_migration_on_connection_with_digest(
        &mut connection,
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
        sql,
        expected_target_digest,
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
    apply_schema_migration_on_connection_inner(connection, migration, None, |_| Ok(()))
}

/// Connection-level digest-verifying apply for a cancellation-aware coordinator.
#[allow(clippy::too_many_arguments)]
pub(super) fn apply_schema_migration_on_connection_with_digest(
    connection: &mut Connection,
    path: &Path,
    shard_id: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &ShardLayout,
    sql: &str,
    expected_target_digest: &SchemaDigest,
) -> EngineResult<SchemaMigrationShardOutcome> {
    let migration = SchemaMigrationShard::new(
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
        sql,
    );
    apply_schema_migration_on_connection_inner(
        connection,
        migration,
        Some(expected_target_digest),
        |_| Ok(()),
    )
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
    apply_schema_migration_on_connection_inner(connection, migration, None, hook)
}

fn apply_schema_migration_on_connection_inner<F>(
    connection: &mut Connection,
    migration: SchemaMigrationShard<'_>,
    expected_target_digest: Option<&SchemaDigest>,
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
        if let Some(expected) = expected_target_digest {
            verify_schema_digest(connection, target_generation, expected)?;
        }
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
        if let Some(expected) = expected_target_digest {
            verify_schema_digest(connection, target_generation, expected)?;
        }
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
    if let Some(expected) = expected_target_digest {
        verify_schema_digest(&transaction, target_generation, expected)?;
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
    if let Some(expected) = expected_target_digest {
        verify_schema_digest(connection, target_generation, expected)?;
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
    let connection = Connection::open_with_flags(open_path, flags).map_err(|error| {
        sqlite_error::storage(error).context(format!("failed to open shard {}", path.display()))
    })?;
    configure_cell_size_check(&connection)?;
    Ok(connection)
}

#[cfg(feature = "experimental-vtab")]
fn open_existing_read_only_connection(path: &Path) -> EngineResult<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_EXRESCODE;
    let open_path = canonical_open_path(path)?;
    let connection = Connection::open_with_flags(open_path, flags).map_err(|error| {
        sqlite_error::storage(error)
            .context(format!("failed to open read-only shard {}", path.display()))
    })?;
    Ok(connection)
}

fn open_creating_connection(path: &Path) -> EngineResult<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_EXRESCODE;
    let open_path = canonical_open_path(path)?;
    let connection = Connection::open_with_flags(open_path, flags).map_err(|error| {
        sqlite_error::storage(error).context(format!("failed to create shard {}", path.display()))
    })?;
    configure_cell_size_check(&connection)?;
    Ok(connection)
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

fn configure_cell_size_check(connection: &Connection) -> EngineResult<()> {
    connection
        .pragma_update(None, "cell_size_check", "ON")
        .map_err(sqlite_error::storage)?;
    let enabled = connection
        .pragma_query_value(None, "cell_size_check", |row| row.get::<_, i64>(0))
        .map_err(sqlite_error::storage)?;
    if enabled == 1 {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "SQLite did not enable required shard cell-size checks",
        ))
    }
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

#[cfg(feature = "experimental-vtab")]
fn require_read_only(connection: &Connection) -> EngineResult<()> {
    if connection
        .is_readonly(MAIN_DB)
        .map_err(sqlite_error::storage)?
    {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::Internal,
            "read-only shard unexpectedly opened through a writable SQLite handle",
        ))
    }
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
    validate_metadata_integrity(connection)
}

fn validate_metadata_integrity(connection: &Connection) -> EngineResult<()> {
    let mut statement = connection
        .prepare("PRAGMA main.integrity_check('briskdb_shard_metadata')")
        .map_err(|error| shard_read_error(error, "failed to check shard metadata integrity"))?;
    let mut rows = statement
        .query([])
        .map_err(|error| shard_read_error(error, "failed to check shard metadata integrity"))?;
    let first = rows
        .next()
        .map_err(|error| shard_read_error(error, "failed to check shard metadata integrity"))?
        .map(|row| row.get::<_, String>(0))
        .transpose()
        .map_err(|error| shard_read_error(error, "failed to check shard metadata integrity"))?;
    let has_additional = rows
        .next()
        .map_err(|error| shard_read_error(error, "failed to check shard metadata integrity"))?
        .is_some();
    require_single_ok_integrity_result(first.as_deref(), has_additional)
}

fn require_single_ok_integrity_result(
    first: Option<&str>,
    has_additional: bool,
) -> EngineResult<()> {
    if first == Some("ok") && !has_additional {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "shard metadata integrity check failed",
        ))
    }
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

fn hash_schema_text(hasher: &mut blake3::Hasher, value: &str) -> EngineResult<()> {
    let length = u64::try_from(value.len()).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::LimitExceeded,
            "shard schema field length exceeds its canonical encoding",
            error,
        )
    })?;
    hasher.update(&length.to_le_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

fn is_sqlite_schema_name(name: &str) -> bool {
    name.as_bytes()
        .get(.."sqlite_".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"sqlite_"))
}

fn is_exact_metadata_schema_object(
    object_type: &str,
    name: &str,
    table_name: &str,
    sql: Option<&str>,
) -> bool {
    object_type == "table"
        && name == SHARD_METADATA_TABLE
        && table_name == SHARD_METADATA_TABLE
        && sql.is_some_and(|sql| {
            sql.len() <= MAX_SCHEMA_SQL_BYTES
                && normalize_schema_sql(sql) == normalize_schema_sql(SHARD_METADATA_TABLE_SQL)
        })
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
        } => {
            (pragma_value.is_some() && !pragma_name.eq_ignore_ascii_case("quick_check"))
                || matches_persistent_pragma(pragma_name)
        }
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
    use crate::core::{
        IDENTIFIER_ENCODING_VERSION, LogicalDatabaseMetadata, ShardKeyMetadata, TableMetadata,
    };

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

    fn catalog_with_registered_tables() -> Catalog {
        Catalog::from_validated_parts(
            IDENTIFIER_ENCODING_VERSION,
            0,
            1,
            vec![LogicalDatabaseMetadata::from_validated(
                1,
                "default".to_owned(),
            )]
            .into_boxed_slice(),
            vec![
                TableMetadata::from_validated(
                    1,
                    1,
                    "accounts".to_owned(),
                    TablePlacement::Sharded(ShardKeyMetadata::from_validated(
                        "id".to_owned(),
                        ShardKeyType::Int64,
                    )),
                ),
                TableMetadata::from_validated(
                    2,
                    1,
                    "audit_catalog".to_owned(),
                    TablePlacement::Catalog,
                ),
                TableMetadata::from_validated(3, 1, "countries".to_owned(), TablePlacement::Global),
            ]
            .into_boxed_slice(),
        )
    }

    fn empty_catalog() -> Catalog {
        Catalog::from_validated_parts(
            IDENTIFIER_ENCODING_VERSION,
            0,
            1,
            vec![LogicalDatabaseMetadata::from_validated(
                1,
                "default".to_owned(),
            )]
            .into_boxed_slice(),
            Vec::new().into_boxed_slice(),
        )
    }

    fn preflight_with_catalog(
        path: &Path,
        ready: &ShardLayout,
        sql: &str,
        catalog: &Catalog,
    ) -> EngineResult<(SchemaMigrationShardState, SchemaDigest)> {
        let mut connection = open_required_file(path)?;
        configure_busy_timeout(&connection)?;
        preflight_schema_migration_on_connection_with_digest_and_catalog(
            &mut connection,
            path,
            0,
            0,
            1,
            ready,
            sql,
            catalog,
        )
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

    fn schema_fixture(schema: &str) -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(SHARD_METADATA_TABLE_SQL).unwrap();
        connection.execute_batch(schema).unwrap();
        connection
    }

    fn cell_size_check(connection: &Connection) -> i64 {
        connection
            .pragma_query_value(None, "cell_size_check", |row| row.get(0))
            .unwrap()
    }

    #[test]
    fn internal_shard_opens_enable_and_read_back_cell_size_checks() {
        let temp = tempfile::tempdir().unwrap();
        let creating_path = temp.path().join("creating.sqlite");
        let creating = open_creating_connection(&creating_path).unwrap();
        assert_eq!(cell_size_check(&creating), 1);
        drop(creating);

        let (_temp, shards, ready) = create_ready_layout(2);
        let path = shard_path(&shards, 0);
        let required = open_required_file(&path).unwrap();
        assert_eq!(cell_size_check(&required), 1);
        drop(required);

        let borrowed = Connection::open(&path).unwrap();
        borrowed
            .pragma_update(None, "cell_size_check", "OFF")
            .unwrap();
        assert_eq!(cell_size_check(&borrowed), 0);
        validate_open_connection(&borrowed, &path, 0, 0, &ready).unwrap();
        assert_eq!(cell_size_check(&borrowed), 1);

        let migration = Connection::open(&path).unwrap();
        migration
            .pragma_update(None, "cell_size_check", "OFF")
            .unwrap();
        assert_eq!(cell_size_check(&migration), 0);
        validate_schema_migration_connection(&migration, &path, 0, 0, 1, &ready).unwrap();
        assert_eq!(cell_size_check(&migration), 1);
    }

    #[test]
    fn metadata_integrity_requires_exactly_one_ok_row_and_maps_failures_to_corruption() {
        assert!(require_single_ok_integrity_result(Some("ok"), false).is_ok());
        for (first, additional) in [
            (None, false),
            (Some(""), false),
            (Some("malformed"), false),
            (Some("ok"), true),
        ] {
            assert_eq!(
                require_single_ok_integrity_result(first, additional)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::DataCorruption
            );
        }

        let (_temp, shards, _) = create_ready_layout(2);
        validate_metadata_integrity(&Connection::open(shard_path(&shards, 0)).unwrap()).unwrap();
    }

    #[test]
    fn schema_digest_has_a_frozen_golden_vector() {
        let connection = schema_fixture(
            "CREATE TABLE widgets (
                 id INTEGER PRIMARY KEY,
                 value TEXT NOT NULL
             ) STRICT;
             CREATE INDEX widgets_value_idx ON widgets (value);",
        );
        let digest = calculate_schema_digest(&connection, 7).unwrap();
        assert_eq!(
            blake3::Hash::from_bytes(digest).to_hex().as_str(),
            "5199ef8f79db275ed5b2ff06ef85c2138f6ddf714b0843300356b9efcfb078aa"
        );
    }

    #[test]
    fn schema_digest_ignores_shard_identity_rows_and_application_data() {
        let (_temp, shards, _) = create_ready_layout(2);
        let mut digests = Vec::new();
        for shard_id in 0..2 {
            let connection = Connection::open(shard_path(&shards, shard_id)).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE widgets (
                         id INTEGER PRIMARY KEY AUTOINCREMENT,
                         value TEXT NOT NULL
                     ) STRICT;
                     CREATE INDEX widgets_value_idx ON widgets (value);",
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO widgets (value) VALUES (?1)",
                    [format!("shard-{shard_id}")],
                )
                .unwrap();
            digests.push(calculate_schema_digest(&connection, 0).unwrap());
        }
        assert_eq!(digests[0], digests[1]);

        let first = Connection::open(shard_path(&shards, 0)).unwrap();
        verify_schema_digest(&first, 0, &digests[0]).unwrap();
        let wrong = [0x5a; 32];
        assert_eq!(
            verify_schema_digest(&first, 0, &wrong).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn schema_digest_is_stable_across_dml_checkpoint_and_vacuum() {
        let (_temp, shards, _) = create_ready_layout(2);
        let path = shard_path(&shards, 0);
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE events (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     value TEXT NOT NULL
                 ) STRICT;
                 CREATE INDEX events_value_idx ON events (value);",
            )
            .unwrap();
        let baseline = calculate_schema_digest(&connection, 0).unwrap();

        connection
            .execute_batch(
                "INSERT INTO events (value) VALUES ('one'), ('two'), ('three');
                 UPDATE events SET value = upper(value) WHERE id = 2;
                 DELETE FROM events WHERE id = 1;",
            )
            .unwrap();
        assert_eq!(calculate_schema_digest(&connection, 0).unwrap(), baseline);

        connection
            .query_row("PRAGMA main.wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .unwrap();
        assert_eq!(calculate_schema_digest(&connection, 0).unwrap(), baseline);

        connection.execute_batch("VACUUM main").unwrap();
        assert_eq!(calculate_schema_digest(&connection, 0).unwrap(), baseline);
    }

    #[test]
    fn every_persistent_application_object_changes_the_schema_digest() {
        let connection = schema_fixture("");
        let mut seen = HashSet::new();
        assert!(seen.insert(calculate_schema_digest(&connection, 0).unwrap()));

        for sql in [
            "CREATE TABLE widgets (id INTEGER PRIMARY KEY, value TEXT NOT NULL) STRICT;",
            "CREATE INDEX widgets_value_idx ON widgets (value);",
            "CREATE VIEW widget_values AS SELECT id, value FROM widgets;",
            "CREATE TRIGGER widgets_after_insert AFTER INSERT ON widgets BEGIN UPDATE widgets SET value = upper(value) WHERE id = NEW.id; END;",
        ] {
            connection.execute_batch(sql).unwrap();
            assert!(seen.insert(calculate_schema_digest(&connection, 0).unwrap()));
        }
        assert_eq!(seen.len(), 5);
    }

    #[test]
    fn schema_digest_order_is_binary_and_independent_of_creation_order() {
        let first = schema_fixture(
            "CREATE TABLE alpha (id INTEGER PRIMARY KEY, value TEXT) STRICT;
             CREATE TABLE beta (id INTEGER PRIMARY KEY, value TEXT) STRICT;
             CREATE INDEX alpha_value_idx ON alpha (value);
             CREATE INDEX beta_value_idx ON beta (value);",
        );
        let second = schema_fixture(
            "CREATE TABLE beta (id INTEGER PRIMARY KEY, value TEXT) STRICT;
             CREATE TABLE alpha (id INTEGER PRIMARY KEY, value TEXT) STRICT;
             CREATE INDEX beta_value_idx ON beta (value);
             CREATE INDEX alpha_value_idx ON alpha (value);",
        );
        assert_eq!(
            calculate_schema_digest(&first, 4).unwrap(),
            calculate_schema_digest(&second, 4).unwrap()
        );
    }

    #[test]
    fn schema_digest_is_generation_bound_and_rejects_unencodable_generations() {
        let connection = schema_fixture("CREATE TABLE widgets (id INTEGER PRIMARY KEY) STRICT;");
        assert_ne!(
            calculate_schema_digest(&connection, 0).unwrap(),
            calculate_schema_digest(&connection, 1).unwrap()
        );
        assert_eq!(
            calculate_schema_digest(&connection, i32::MAX as u64 + 1)
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
    }

    #[test]
    fn schema_digest_rejects_unexpected_or_incompatible_reserved_objects() {
        for schema in [
            "CREATE TABLE briskdb_private (id INTEGER);",
            "CREATE INDEX metadata_tamper_idx ON briskdb_shard_metadata (shard_id);",
        ] {
            let connection = schema_fixture(schema);
            assert_eq!(
                calculate_schema_digest(&connection, 0).unwrap_err().kind(),
                EngineErrorKind::DataCorruption,
                "{schema}"
            );
        }

        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch("CREATE TABLE briskdb_shard_metadata (singleton INTEGER);")
            .unwrap();
        assert_eq!(
            calculate_schema_digest(&connection, 0).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn migration_preflight_fingerprints_rollback_target_and_apply_verifies_before_commit() {
        let (_temp, shards, ready) = create_ready_layout(2);
        let sql = "CREATE TABLE migrated_widgets (
                       id INTEGER PRIMARY KEY,
                       value TEXT NOT NULL
                   ) STRICT";
        let mut target = None;
        for shard_id in 0..2 {
            let path = shard_path(&shards, shard_id);
            let (state, digest) =
                preflight_schema_migration_with_digest(&path, shard_id, 0, 1, &ready, sql).unwrap();
            assert_eq!(state, SchemaMigrationShardState::Source);
            assert!(!schema_object_exists(&path, "migrated_widgets"));
            assert_eq!(identity(&path), (SHARD_APPLICATION_ID, 0));
            if let Some(expected) = target {
                assert_eq!(digest, expected);
            } else {
                target = Some(digest);
            }
        }
        let target = target.unwrap();

        let first = shard_path(&shards, 0);
        assert_eq!(
            apply_schema_migration_with_digest(&first, 0, 0, 1, &ready, sql, &target).unwrap(),
            SchemaMigrationShardOutcome::Applied
        );
        verify_schema_digest(&Connection::open(&first).unwrap(), 1, &target).unwrap();
        assert_eq!(
            apply_schema_migration_with_digest(&first, 0, 0, 1, &ready, sql, &target).unwrap(),
            SchemaMigrationShardOutcome::AlreadyApplied
        );

        let second = shard_path(&shards, 1);
        let wrong = [0x5a; 32];
        assert_eq!(
            apply_schema_migration_with_digest(&second, 1, 0, 1, &ready, sql, &wrong)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
        assert_eq!(identity(&second), (SHARD_APPLICATION_ID, 0));
        assert!(!schema_object_exists(&second, "migrated_widgets"));
    }

    #[test]
    fn migration_preflight_preserves_authoritative_table_catalog_and_shard_keys() {
        let (_temp, shards, ready) = create_ready_layout(2);
        let path = shard_path(&shards, 0);
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE countries (
                     code TEXT NOT NULL,
                     display_name TEXT NOT NULL
                 ) STRICT;
                 CREATE UNIQUE INDEX countries_code_unique ON countries(code);
                 CREATE TABLE accounts (
                     id INTEGER PRIMARY KEY,
                     display_name TEXT NOT NULL,
                     country_code TEXT REFERENCES countries(code)
                 ) STRICT;",
            )
            .unwrap();
        drop(connection);
        let catalog = catalog_with_registered_tables();

        let (state, _) = preflight_with_catalog(
            &path,
            &ready,
            "CREATE INDEX accounts_display_name_idx ON accounts (display_name)",
            &catalog,
        )
        .unwrap();
        assert_eq!(state, SchemaMigrationShardState::Source);
        assert!(!schema_object_exists(&path, "accounts_display_name_idx"));

        let (state, _) = preflight_with_catalog(
            &path,
            &ready,
            "ALTER TABLE accounts ADD COLUMN billing_country TEXT REFERENCES countries(code)",
            &catalog,
        )
        .unwrap();
        assert_eq!(state, SchemaMigrationShardState::Source);
        let connection = Connection::open(&path).unwrap();
        assert!(
            !connection
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM pragma_table_xinfo('accounts')
                         WHERE name = 'billing_country'
                     )",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        );
        drop(connection);

        for (sql, transient_object) in [
            (
                "INSERT INTO accounts (id, display_name) VALUES (1, 'duplicate')",
                None,
            ),
            (
                "UPDATE accounts SET display_name = upper(display_name)",
                None,
            ),
            ("DELETE FROM accounts", None),
            ("DROP TABLE accounts", None),
            (
                "DROP TABLE accounts;
                 CREATE TABLE accounts (
                     account_id INTEGER PRIMARY KEY,
                     display_name TEXT NOT NULL
                 ) STRICT",
                None,
            ),
            (
                "DROP TABLE accounts;
                 CREATE TABLE accounts (
                     id TEXT PRIMARY KEY,
                     display_name TEXT NOT NULL
                 ) STRICT",
                None,
            ),
            (
                "DROP TABLE accounts;
                 CREATE TABLE accounts (
                     id INTEGER PRIMARY KEY DESC,
                     display_name TEXT NOT NULL
                 )",
                None,
            ),
            (
                "DROP TABLE accounts;
                 CREATE TABLE accounts (
                     row_id INTEGER PRIMARY KEY,
                     id INTEGER,
                     display_name TEXT NOT NULL
                 ) STRICT",
                None,
            ),
            (
                "CREATE TABLE undeclared (id INTEGER PRIMARY KEY) STRICT",
                Some("undeclared"),
            ),
            (
                "CREATE TABLE copied_accounts AS SELECT * FROM accounts",
                Some("copied_accounts"),
            ),
            (
                "CREATE UNIQUE INDEX accounts_display_unique ON accounts (display_name)",
                Some("accounts_display_unique"),
            ),
            ("DROP INDEX countries_code_unique", None),
            (
                "CREATE TRIGGER accounts_move AFTER INSERT ON accounts
                 BEGIN DELETE FROM accounts WHERE id = NEW.id; END",
                Some("accounts_move"),
            ),
            (
                "CREATE VIEW Audit_Catalog AS SELECT 1 AS id",
                Some("Audit_Catalog"),
            ),
            (
                "CREATE VIEW audit_catalog AS SELECT id FROM accounts",
                Some("audit_catalog"),
            ),
        ] {
            let error = preflight_with_catalog(&path, &ready, sql, &catalog).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition, "{sql}");
            assert!(schema_object_exists(&path, "accounts"), "{sql}");
            assert!(schema_object_exists(&path, "countries"), "{sql}");
            if let Some(transient_object) = transient_object {
                assert!(!schema_object_exists(&path, transient_object), "{sql}");
            }
            assert_eq!(identity(&path), (SHARD_APPLICATION_ID, 0), "{sql}");
        }
        assert!(schema_object_exists(&path, "countries_code_unique"));

        let empty = empty_catalog();
        let (state, _) = preflight_with_catalog(
            &path,
            &ready,
            "CREATE TABLE legacy_unregistered (id INTEGER PRIMARY KEY) STRICT",
            &empty,
        )
        .unwrap();
        assert_eq!(state, SchemaMigrationShardState::Source);
        assert!(!schema_object_exists(&path, "legacy_unregistered"));
    }

    #[test]
    fn catalog_aware_schema_validation_accepts_only_colocated_foreign_keys() {
        let catalog = catalog_with_registered_tables();
        let safe = Connection::open_in_memory().unwrap();
        safe.pragma_update(None, "foreign_keys", "ON").unwrap();
        safe.execute_batch(
            "CREATE TABLE countries (
                 code TEXT PRIMARY KEY,
                 display_name TEXT NOT NULL
             ) STRICT;
             CREATE TABLE accounts (
                 id INTEGER PRIMARY KEY,
                 display_name TEXT NOT NULL,
                 country_code TEXT REFERENCES countries
             ) STRICT;",
        )
        .unwrap();
        validate_registered_table_schema(&safe, &catalog).unwrap();

        let unsafe_catalog_parent = Connection::open_in_memory().unwrap();
        unsafe_catalog_parent
            .pragma_update(None, "foreign_keys", "ON")
            .unwrap();
        unsafe_catalog_parent
            .execute_batch(
                "CREATE TABLE countries (
                     code TEXT PRIMARY KEY,
                     display_name TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE accounts (
                     id INTEGER PRIMARY KEY,
                     display_name TEXT NOT NULL,
                     catalog_id INTEGER REFERENCES audit_catalog(id)
                 ) STRICT;",
            )
            .unwrap();
        let error = validate_registered_table_schema(&unsafe_catalog_parent, &catalog).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(error.diagnostic().contains("catalog-only table"));
    }

    #[test]
    fn declared_foreign_keys_must_have_a_sqlite_enforceable_parent_key() {
        for schema in [
            "CREATE TABLE countries (code TEXT NOT NULL);
             CREATE TABLE children (
                 tenant_id TEXT PRIMARY KEY NOT NULL,
                 country_code TEXT REFERENCES countries(code)
             );",
            "CREATE TABLE countries (code TEXT PRIMARY KEY);
             CREATE TABLE children (
                 tenant_id TEXT PRIMARY KEY NOT NULL,
                 country_code TEXT REFERENCES countries(missing_code)
             );",
            "CREATE TABLE countries (code TEXT COLLATE NOCASE NOT NULL);
             CREATE UNIQUE INDEX countries_code_unique
                 ON countries(code COLLATE BINARY);
             CREATE TABLE children (
                 tenant_id TEXT PRIMARY KEY NOT NULL,
                 country_code TEXT REFERENCES countries(code)
             );",
        ] {
            let connection = Connection::open_in_memory().unwrap();
            connection
                .pragma_update(None, "foreign_keys", "ON")
                .unwrap();
            connection.execute_batch(schema).unwrap();
            let database = crate::core::LogicalDatabaseId::new(1).unwrap();
            let child = TableDeclaration::sharded(
                database,
                "children",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap();
            let declarations = [
                child.clone(),
                TableDeclaration::global(database, "countries").unwrap(),
            ];

            let error = validate_declared_table_constraints(&connection, &child, &declarations)
                .unwrap_err();
            assert_eq!(
                error.kind(),
                EngineErrorKind::FailedPrecondition,
                "{schema}"
            );
            assert!(
                error.diagnostic().contains("cannot be enforced by SQLite"),
                "{schema}: {}",
                error.diagnostic()
            );
        }
    }

    #[test]
    fn authoritative_unique_index_accepts_a_later_binary_shard_key_term() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE records (
                     tenant_id TEXT NOT NULL COLLATE BINARY,
                     email TEXT NOT NULL
                 );
                 CREATE UNIQUE INDEX records_unique ON records (
                     tenant_id COLLATE NOCASE,
                     tenant_id COLLATE BINARY,
                     email
                 );",
            )
            .unwrap();
        let shard_key =
            ShardKeyMetadata::from_validated("tenant_id".to_owned(), ShardKeyType::Text);

        validate_authoritative_unique_constraints(&connection, "records", &shard_key).unwrap();
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
        assert!(!denies_schema_migration_action(context(
            AuthAction::Pragma {
                pragma_name: "quick_check",
                pragma_value: Some("widgets"),
            },
            Some("main"),
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

    #[test]
    fn migration_authorizer_allows_sqlites_strict_table_quick_check_for_a_new_foreign_key() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .unwrap();
        connection.execute_batch(SHARD_METADATA_TABLE_SQL).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE parents (id INTEGER PRIMARY KEY) STRICT;
                 CREATE TABLE children (id INTEGER PRIMARY KEY) STRICT;",
            )
            .unwrap();

        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        execute_schema_migration_batch(
            &transaction,
            "ALTER TABLE children ADD COLUMN parent_id INTEGER REFERENCES parents(id)",
        )
        .unwrap();
        transaction.rollback().unwrap();
        assert!(
            !connection
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM pragma_table_xinfo('children')
                         WHERE name = 'parent_id'
                     )",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        );
    }
}
