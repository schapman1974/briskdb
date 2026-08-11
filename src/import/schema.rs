//! Read-only source-schema preflight for the offline SQLite importer.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    str,
};

use rusqlite::{Connection, OpenFlags, OptionalExtension, limits::Limit, types::ValueRef};
use same_file::Handle;

use super::{
    MAX_SQLITE_IMPORT_ROW_BYTES, OmittedForeignKey, SQLITE_IMPORT_PLAN_VERSION,
    SqliteForeignKeyPolicy, SqliteImportKeyType, SqliteImportPlacement, SqliteImportPlan,
    SqliteShardKeyPlan, SqliteTableImportPlan,
};
use crate::{
    core::{
        CancellationToken, EngineError, EngineErrorKind, EngineResult, LogicalDatabaseId,
        MAX_TABLES, ShardKeyMetadata, ShardKeyType, TableDeclaration,
    },
    sqlite_error,
};

/// A long-lived, transactionally consistent view of one immutable import source.
#[derive(Debug)]
pub(super) struct SourceSnapshot {
    connection: Connection,
    _source_identity: Handle,
    tables: Vec<SourceTable>,
    explicit_indexes: Vec<SourceIndex>,
    sequences: Vec<SourceSequence>,
    omitted_foreign_keys: Vec<OmittedForeignKey>,
    schema_digest: [u8; 32],
}

/// One validated ordinary source table and its resolved import policy.
#[derive(Debug, Clone)]
pub(super) struct SourceTable {
    name: String,
    source_create_sql: String,
    staged_create_sql: String,
    source_rows: u64,
    placement: SqliteImportPlacement,
    columns: Vec<SourceColumn>,
    shard_key: Option<SourceShardKey>,
    rowid_projection: Option<String>,
    without_rowid: bool,
    strict: bool,
}

/// Exact source-column metadata returned by `pragma_table_xinfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SourceColumn {
    cid: i32,
    name: String,
    declared_type: String,
    not_null: bool,
    default_value: Option<String>,
    primary_key_ordinal: i32,
    hidden: i32,
}

/// A Sharded table's schema- and data-validated routing key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SourceShardKey {
    writable_column_index: usize,
    column: String,
    key_type: SqliteImportKeyType,
}

/// One exact, explicit `CREATE INDEX` object from the source schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SourceIndex {
    name: String,
    table: String,
    create_sql: String,
}

/// One exact `sqlite_sequence` high-water mark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SourceSequence {
    table: String,
    seq: i64,
}

#[derive(Debug)]
struct RawTable {
    name: String,
    create_sql: String,
    without_rowid: bool,
    strict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexSignature {
    name: String,
    unique: bool,
    origin: String,
    partial: bool,
    terms: Vec<IndexTerm>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexTerm {
    cid: i32,
    name: Option<String>,
    descending: bool,
    collation: Option<String>,
    key: bool,
}

#[derive(Debug)]
struct ForeignKeyBuilder {
    referenced_table: String,
    columns: Vec<(i64, String)>,
    referenced_columns: Vec<(i64, Option<String>)>,
    on_update: String,
    on_delete: String,
    match_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqliteAffinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
}

impl SourceSnapshot {
    /// Open `path` without write/create/follow privileges and complete every
    /// source-only validation while one read transaction pins the snapshot.
    #[cfg(test)]
    pub(super) fn open(path: &Path, plan: &SqliteImportPlan) -> EngineResult<Self> {
        Self::open_with_cancellation(path, plan, &CancellationToken::new())
    }

    pub(super) fn open_with_cancellation(
        path: &Path,
        plan: &SqliteImportPlan,
        cancellation: &CancellationToken,
    ) -> EngineResult<Self> {
        ensure_preflight_not_cancelled(cancellation)?;
        if plan.version() != SQLITE_IMPORT_PLAN_VERSION {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "SQLite import plan version {} is unsupported; expected {}",
                    plan.version(),
                    SQLITE_IMPORT_PLAN_VERSION
                ),
            ));
        }

        let (connection, source_path, source_identity) = open_read_only_source(path)?;
        connection
            .set_limit(Limit::SQLITE_LIMIT_LENGTH, MAX_SQLITE_IMPORT_ROW_BYTES)
            .map_err(sqlite_error::storage)?;
        let progress_cancellation = cancellation.clone();
        connection
            .progress_handler(1_000, Some(move || progress_cancellation.is_cancelled()))
            .map_err(sqlite_error::storage)?;
        connection
            .execute_batch("PRAGMA trusted_schema = OFF; PRAGMA query_only = ON; BEGIN DEFERRED")
            .map_err(|error| {
                sqlite_error::storage(error)
                    .context("failed to begin a read-only SQLite source snapshot")
            })?;
        verify_quick_check(&connection)?;
        reject_unsupported_schema_objects(&connection)?;

        let raw_tables = inventory_raw_tables(&connection)?;
        if raw_tables.is_empty() {
            return Err(precondition(
                "SQLite import source has no ordinary application tables",
            ));
        }
        if raw_tables.len() > MAX_TABLES {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!("SQLite import source exceeds the authoritative {MAX_TABLES}-table limit"),
            ));
        }
        let planned_tables = validate_exact_plan_coverage(plan, &raw_tables)?;
        let schema_digest = application_schema_digest(&connection)?;
        let explicit_indexes = inventory_explicit_indexes(&connection, &raw_tables)?;

        let mut tables = Vec::with_capacity(raw_tables.len());
        let mut omitted_foreign_keys = Vec::new();
        for raw in raw_tables {
            ensure_preflight_not_cancelled(cancellation)?;
            let table_plan = planned_tables
                .get(raw.name.as_str())
                .copied()
                .expect("exact coverage was validated");
            let columns = inventory_columns(&connection, &raw.name)?;
            let foreign_keys = inventory_foreign_keys(&connection, &raw.name)?;
            let staged_create_sql = match (foreign_keys.is_empty(), table_plan.foreign_key_policy())
            {
                (true, _) => raw.create_sql.clone(),
                (false, SqliteForeignKeyPolicy::Reject) => {
                    return Err(precondition(format!(
                        "source table {} declares {} foreign-key constraint(s); the import plan must explicitly choose foreign_keys=omit",
                        raw.name,
                        foreign_keys.len()
                    )));
                }
                (false, SqliteForeignKeyPolicy::Omit) => {
                    let (rewritten, removed) = remove_table_foreign_key_clauses(&raw.create_sql)?;
                    if removed != foreign_keys.len() {
                        return Err(precondition(format!(
                            "source table {} has {} foreign-key constraint(s), but only {removed} conservative table-level clause(s) can be omitted",
                            raw.name,
                            foreign_keys.len()
                        )));
                    }
                    omitted_foreign_keys.extend(foreign_keys);
                    rewritten
                }
            };
            let source_rows = table_row_count(&connection, &raw.name)?;
            let rowid_projection = resolve_rowid_projection(&connection, &raw, &columns)?;
            let shard_key = match table_plan.placement() {
                SqliteImportPlacement::Global => None,
                SqliteImportPlacement::Sharded { shard_key } => {
                    Some(resolve_shard_key(&connection, &raw, &columns, shard_key)?)
                }
            };

            tables.push(SourceTable {
                name: raw.name,
                source_create_sql: raw.create_sql,
                staged_create_sql,
                source_rows,
                placement: table_plan.placement().clone(),
                columns,
                shard_key,
                rowid_projection,
                without_rowid: raw.without_rowid,
                strict: raw.strict,
            });
        }

        let sequences = inventory_sequences(&connection, &tables)?;
        verify_staged_schema(&connection, &tables, &explicit_indexes)?;
        ensure_preflight_not_cancelled(cancellation)?;
        ensure_source_identity(&source_path, &source_identity)?;

        Ok(Self {
            connection,
            _source_identity: source_identity,
            tables,
            explicit_indexes,
            sequences,
            omitted_foreign_keys,
            schema_digest,
        })
    }

    pub(super) const fn connection(&self) -> &Connection {
        &self.connection
    }

    pub(super) fn tables(&self) -> &[SourceTable] {
        &self.tables
    }

    pub(super) fn explicit_indexes(&self) -> &[SourceIndex] {
        &self.explicit_indexes
    }

    pub(super) fn sequences(&self) -> &[SourceSequence] {
        &self.sequences
    }

    pub(super) fn omitted_foreign_keys(&self) -> &[OmittedForeignKey] {
        &self.omitted_foreign_keys
    }

    /// Digest of every non-internal `sqlite_schema` row in binary
    /// `(name, type, sql)` order. The encoding is domain-separated, followed
    /// by a little-endian `u64` row count and three length-prefixed fields per
    /// row. A NULL SQL value uses the reserved length `u64::MAX`.
    pub(super) const fn schema_digest(&self) -> [u8; 32] {
        self.schema_digest
    }

    /// Return migration-sized batches without ever splitting one schema object.
    /// All table definitions precede all explicit index definitions.
    pub(super) fn schema_batches(&self, max_bytes: usize) -> EngineResult<Vec<String>> {
        if max_bytes == 0 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "SQLite import schema batch size must be positive",
            ));
        }
        let objects = self
            .tables
            .iter()
            .map(|table| table.staged_create_sql())
            .chain(self.explicit_indexes().iter().map(SourceIndex::create_sql));
        let mut batches = Vec::new();
        let mut batch = String::new();
        for sql in objects {
            let object = terminated_schema_object(sql);
            if object.len() > max_bytes {
                return Err(EngineError::new(
                    EngineErrorKind::LimitExceeded,
                    format!(
                        "one SQLite schema object is {} bytes, exceeding the {max_bytes}-byte migration limit",
                        object.len()
                    ),
                ));
            }
            if !batch.is_empty() && batch.len() + object.len() > max_bytes {
                batches.push(std::mem::take(&mut batch));
            }
            batch.push_str(&object);
        }
        if !batch.is_empty() {
            batches.push(batch);
        }
        Ok(batches)
    }

    pub(super) fn table_declarations(
        &self,
        database_id: LogicalDatabaseId,
    ) -> EngineResult<Vec<TableDeclaration>> {
        self.tables
            .iter()
            .map(|table| match table.shard_key() {
                Some(key) => TableDeclaration::sharded(
                    database_id,
                    table.name(),
                    ShardKeyMetadata::new(key.column(), core_key_type(key.key_type()))?,
                ),
                None => TableDeclaration::global(database_id, table.name()),
            })
            .collect()
    }
}

impl SourceTable {
    pub(super) fn name(&self) -> &str {
        &self.name
    }

    pub(super) fn source_create_sql(&self) -> &str {
        &self.source_create_sql
    }

    pub(super) fn staged_create_sql(&self) -> &str {
        &self.staged_create_sql
    }

    pub(super) const fn source_rows(&self) -> u64 {
        self.source_rows
    }

    pub(super) const fn placement(&self) -> &SqliteImportPlacement {
        &self.placement
    }

    pub(super) fn columns(&self) -> &[SourceColumn] {
        &self.columns
    }

    pub(super) const fn shard_key(&self) -> Option<&SourceShardKey> {
        self.shard_key.as_ref()
    }

    /// Unshadowed SQLite magic name used to preserve an implicit rowid.
    /// `None` means this is a WITHOUT ROWID table or its exact `INTEGER
    /// PRIMARY KEY` column already aliases and preserves the rowid.
    pub(super) fn rowid_projection(&self) -> Option<&str> {
        self.rowid_projection.as_deref()
    }

    pub(super) const fn without_rowid(&self) -> bool {
        self.without_rowid
    }

    pub(super) const fn strict(&self) -> bool {
        self.strict
    }
}

impl SourceColumn {
    pub(super) fn name(&self) -> &str {
        &self.name
    }

    pub(super) const fn hidden(&self) -> i32 {
        self.hidden
    }

    pub(super) const fn writable(&self) -> bool {
        self.hidden() == 0
    }
}

impl SourceShardKey {
    /// Index in the table's writable-column projection, not raw `xinfo` order.
    pub(super) const fn column_index(&self) -> usize {
        self.writable_column_index
    }

    pub(super) fn column(&self) -> &str {
        &self.column
    }

    pub(super) const fn key_type(&self) -> SqliteImportKeyType {
        self.key_type
    }
}

impl SourceIndex {
    pub(super) fn name(&self) -> &str {
        &self.name
    }

    pub(super) fn table(&self) -> &str {
        &self.table
    }

    pub(super) fn create_sql(&self) -> &str {
        &self.create_sql
    }
}

impl SourceSequence {
    pub(super) fn table(&self) -> &str {
        &self.table
    }

    pub(super) const fn seq(&self) -> i64 {
        self.seq
    }
}

fn ensure_preflight_not_cancelled(cancellation: &CancellationToken) -> EngineResult<()> {
    if cancellation.is_cancelled() {
        Err(EngineError::new(
            EngineErrorKind::Cancelled,
            "SQLite import was cancelled during source preflight",
        ))
    } else {
        Ok(())
    }
}

fn open_read_only_source(path: &Path) -> EngineResult<(Connection, PathBuf, Handle)> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!("failed to inspect SQLite import source {}", path.display()),
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(precondition(format!(
            "SQLite import source {} must be a real regular file",
            path.display()
        )));
    }
    let open_path = canonical_open_path(path)?;
    let source_identity = Handle::from_path(&open_path).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!(
                "failed to retain SQLite import source identity {}",
                path.display()
            ),
        )
    })?;
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_EXRESCODE;
    let connection = Connection::open_with_flags(&open_path, flags).map_err(|error| {
        sqlite_error::storage(error).context(format!(
            "failed to open SQLite import source {} read-only",
            path.display()
        ))
    })?;
    ensure_source_identity(&open_path, &source_identity)?;
    Ok((connection, open_path, source_identity))
}

fn ensure_source_identity(path: &Path, expected: &Handle) -> EngineResult<()> {
    let current = Handle::from_path(path).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!(
                "failed to recheck SQLite import source identity {}",
                path.display()
            ),
        )
    })?;
    if &current == expected {
        Ok(())
    } else {
        Err(precondition(format!(
            "SQLite import source {} was replaced during preflight",
            path.display()
        )))
    }
}

fn canonical_open_path(path: &Path) -> EngineResult<PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        precondition(format!(
            "SQLite import source {} has no file name",
            path.display()
        ))
    })?;
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = fs::canonicalize(parent).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!(
                "failed to resolve SQLite import source directory {}",
                parent.display()
            ),
        )
    })?;
    Ok(parent.join(file_name))
}

fn verify_quick_check(connection: &Connection) -> EngineResult<()> {
    let mut statement = connection
        .prepare("PRAGMA quick_check")
        .map_err(sqlite_error::storage)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)?;
    if rows == ["ok"] {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "SQLite import source failed quick_check with {} diagnostic row(s)",
                rows.len()
            ),
        ))
    }
}

fn reject_unsupported_schema_objects(connection: &Connection) -> EngineResult<()> {
    let unknown_reserved = connection
        .query_row(
            "SELECT type, name
             FROM main.sqlite_schema
             WHERE name LIKE 'sqlite\\_%' ESCAPE '\\'
               AND NOT (type = 'table' AND name = 'sqlite_sequence')
               AND NOT (
                   type = 'index'
                   AND name GLOB 'sqlite_autoindex_*'
                   AND sql IS NULL
               )
             ORDER BY type COLLATE BINARY, name COLLATE BINARY
             LIMIT 1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(sqlite_error::storage)?;
    if let Some((kind, name)) = unknown_reserved {
        return Err(precondition(format!(
            "SQLite import source contains unsupported reserved {kind} object {name}"
        )));
    }

    let unsupported = connection
        .query_row(
            "SELECT type, name
             FROM main.sqlite_schema
             WHERE type IN ('view', 'trigger')
             ORDER BY type COLLATE BINARY, name COLLATE BINARY
             LIMIT 1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(sqlite_error::storage)?;
    if let Some((kind, name)) = unsupported {
        return Err(precondition(format!(
            "SQLite import source contains unsupported {kind} object {name}"
        )));
    }

    let non_ordinary = connection
        .query_row(
            "SELECT name, type
             FROM pragma_table_list
             WHERE schema = 'main'
               AND type <> 'table'
               AND type <> 'view'
             ORDER BY name COLLATE BINARY
             LIMIT 1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(sqlite_error::storage)?;
    if let Some((name, kind)) = non_ordinary {
        return Err(precondition(format!(
            "SQLite import source table {name} has unsupported SQLite table type {kind}"
        )));
    }
    Ok(())
}

fn inventory_raw_tables(connection: &Connection) -> EngineResult<Vec<RawTable>> {
    let mut statement = connection
        .prepare(
            "SELECT s.name, s.sql, p.wr, p.strict
             FROM main.sqlite_schema AS s
             JOIN pragma_table_list AS p
               ON p.schema = 'main' AND p.name = s.name
             WHERE s.type = 'table'
               AND s.name <> 'sqlite_sequence'
               AND p.type = 'table'
             ORDER BY s.name COLLATE BINARY",
        )
        .map_err(sqlite_error::storage)?;
    statement
        .query_map([], |row| {
            Ok(RawTable {
                name: row.get(0)?,
                create_sql: row.get(1)?,
                without_rowid: row.get::<_, i64>(2)? != 0,
                strict: row.get::<_, i64>(3)? != 0,
            })
        })
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)
}

fn validate_exact_plan_coverage<'a>(
    plan: &'a SqliteImportPlan,
    raw_tables: &[RawTable],
) -> EngineResult<BTreeMap<&'a str, &'a SqliteTableImportPlan>> {
    let source_names = raw_tables
        .iter()
        .map(|table| table.name.as_str())
        .collect::<BTreeSet<_>>();
    for table in raw_tables {
        ensure_catalog_identifier(&table.name, "source table")?;
    }

    let mut planned = BTreeMap::new();
    for table in plan.tables() {
        ensure_catalog_identifier(table.name(), "import-plan table")?;
        if planned.insert(table.name(), table).is_some() {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!(
                    "SQLite import plan declares table {} more than once",
                    table.name()
                ),
            ));
        }
    }

    let planned_names = planned.keys().copied().collect::<BTreeSet<_>>();
    let missing = source_names
        .difference(&planned_names)
        .copied()
        .collect::<Vec<_>>();
    let unknown = planned_names
        .difference(&source_names)
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() || !unknown.is_empty() {
        return Err(precondition(format!(
            "SQLite import plan does not exactly cover ordinary source tables (missing: {}; unknown: {})",
            display_names(&missing),
            display_names(&unknown)
        )));
    }
    Ok(planned)
}

fn inventory_columns(connection: &Connection, table: &str) -> EngineResult<Vec<SourceColumn>> {
    let mut statement = connection
        .prepare(
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo(?1)
             ORDER BY cid",
        )
        .map_err(sqlite_error::storage)?;
    let columns = statement
        .query_map([table], |row| {
            Ok(SourceColumn {
                cid: row.get(0)?,
                name: row.get(1)?,
                declared_type: row.get(2)?,
                not_null: row.get::<_, i64>(3)? != 0,
                default_value: row.get(4)?,
                primary_key_ordinal: row.get(5)?,
                hidden: row.get(6)?,
            })
        })
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)?;
    if columns.is_empty() {
        return Err(precondition(format!(
            "ordinary source table {table} has no columns"
        )));
    }
    Ok(columns)
}

fn inventory_explicit_indexes(
    connection: &Connection,
    tables: &[RawTable],
) -> EngineResult<Vec<SourceIndex>> {
    let table_names = tables
        .iter()
        .map(|table| table.name.as_str())
        .collect::<BTreeSet<_>>();
    let mut statement = connection
        .prepare(
            "SELECT name, tbl_name, sql
             FROM main.sqlite_schema
             WHERE type = 'index' AND sql IS NOT NULL
               AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
             ORDER BY name COLLATE BINARY",
        )
        .map_err(sqlite_error::storage)?;
    let indexes = statement
        .query_map([], |row| {
            Ok(SourceIndex {
                name: row.get(0)?,
                table: row.get(1)?,
                create_sql: row.get(2)?,
            })
        })
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)?;
    for index in &indexes {
        if !table_names.contains(index.table()) {
            return Err(precondition(format!(
                "explicit index {} targets non-ordinary table {}",
                index.name,
                index.table()
            )));
        }
    }
    Ok(indexes)
}

fn table_row_count(connection: &Connection, table: &str) -> EngineResult<u64> {
    let sql = format!("SELECT count(*) FROM {}", quote_identifier(table));
    let count = connection
        .query_row(&sql, [], |row| row.get::<_, i64>(0))
        .map_err(sqlite_error::storage)?;
    u64::try_from(count).map_err(|_| {
        EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("source table {table} returned an invalid negative row count"),
        )
    })
}

fn resolve_shard_key(
    connection: &Connection,
    table: &RawTable,
    columns: &[SourceColumn],
    plan: &SqliteShardKeyPlan,
) -> EngineResult<SourceShardKey> {
    let (column, key_type) = match plan {
        SqliteShardKeyPlan::PrimaryKey => {
            let primary_key = columns
                .iter()
                .filter(|column| column.primary_key_ordinal > 0)
                .collect::<Vec<_>>();
            if primary_key.len() != 1 || primary_key[0].primary_key_ordinal != 1 {
                return Err(precondition(format!(
                    "source table {} needs an explicit shard key because its primary key has {} columns",
                    table.name,
                    primary_key.len()
                )));
            }
            let column = primary_key[0];
            let key_type =
                import_key_type_for_affinity(&column.declared_type).ok_or_else(|| {
                    precondition(format!(
                        "primary-key column {}.{} has unsupported SQLite {} affinity",
                        table.name,
                        column.name,
                        affinity_name(sqlite_affinity(&column.declared_type))
                    ))
                })?;
            (column, key_type)
        }
        SqliteShardKeyPlan::Column { column, key_type } => {
            ensure_catalog_identifier(column, "shard-key column")?;
            let column = columns
                .iter()
                .find(|candidate| candidate.name == *column)
                .ok_or_else(|| {
                    precondition(format!(
                        "source table {} does not contain declared shard-key column {column}",
                        table.name
                    ))
                })?;
            (column, *key_type)
        }
    };

    ensure_catalog_identifier(&column.name, "shard-key column")?;
    if column.hidden != 0 {
        return Err(precondition(format!(
            "shard-key column {}.{} must be a visible writable column",
            table.name, column.name
        )));
    }
    if !column_is_physically_non_null(connection, &table.name, column)? {
        return Err(precondition(format!(
            "shard-key column {}.{} must be physically NOT NULL",
            table.name, column.name
        )));
    }
    let actual_affinity = sqlite_affinity(&column.declared_type);
    if !key_type_matches_affinity(key_type, actual_affinity) {
        return Err(precondition(format!(
            "shard-key column {}.{} has SQLite {} affinity, incompatible with import key type {}",
            table.name,
            column.name,
            affinity_name(actual_affinity),
            key_type_name(key_type)
        )));
    }
    if matches!(key_type, SqliteImportKeyType::Text)
        && !column_uses_binary_collation(connection, &table.name, &column.name)?
    {
        return Err(precondition(format!(
            "text shard-key column {}.{} must use SQLite BINARY collation",
            table.name, column.name
        )));
    }
    validate_unique_locality(connection, &table.name, &column.name)?;
    validate_runtime_key_values(connection, &table.name, &column.name, key_type)?;

    let writable_column_index = columns
        .iter()
        .filter(|candidate| candidate.writable())
        .position(|candidate| candidate.name == column.name)
        .expect("a visible shard key is in the writable projection");
    Ok(SourceShardKey {
        writable_column_index,
        column: column.name.clone(),
        key_type,
    })
}

fn resolve_rowid_projection(
    connection: &Connection,
    table: &RawTable,
    columns: &[SourceColumn],
) -> EngineResult<Option<String>> {
    if table.without_rowid {
        return Ok(None);
    }
    let primary_key = columns
        .iter()
        .filter(|column| column.primary_key_ordinal > 0)
        .collect::<Vec<_>>();
    if primary_key.len() == 1
        && primary_key[0]
            .declared_type
            .trim()
            .eq_ignore_ascii_case("INTEGER")
    {
        let has_primary_key_index = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_index_list(?1) WHERE origin = 'pk')",
                [table.name.as_str()],
                |row| row.get::<_, bool>(0),
            )
            .map_err(sqlite_error::storage)?;
        if !has_primary_key_index {
            return Ok(None);
        }
    }

    for alias in ["_rowid_", "rowid", "oid"] {
        if !columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(alias))
        {
            return Ok(Some(alias.to_owned()));
        }
    }
    Err(precondition(format!(
        "rowid source table {} has no INTEGER PRIMARY KEY alias and shadows _rowid_, rowid, and oid",
        table.name
    )))
}

fn column_is_physically_non_null(
    connection: &Connection,
    table: &str,
    column: &SourceColumn,
) -> EngineResult<bool> {
    if column.not_null {
        return Ok(true);
    }
    if column.primary_key_ordinal == 0
        || !column.declared_type.trim().eq_ignore_ascii_case("INTEGER")
    {
        return Ok(false);
    }
    let has_primary_key_index = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_index_list(?1) WHERE origin = 'pk')",
            [table],
            |row| row.get::<_, bool>(0),
        )
        .map_err(sqlite_error::storage)?;
    Ok(!has_primary_key_index)
}

fn column_uses_binary_collation(
    connection: &Connection,
    table: &str,
    column: &str,
) -> EngineResult<bool> {
    let (_, collation, _, _, _) = connection
        .column_metadata(Some("main"), table, column)
        .map_err(sqlite_error::storage)?;
    Ok(collation.is_some_and(|name| name.to_bytes().eq_ignore_ascii_case(b"BINARY")))
}

fn validate_unique_locality(connection: &Connection, table: &str, key: &str) -> EngineResult<()> {
    let mut statement = connection
        .prepare(
            "SELECT name, origin
             FROM pragma_index_list(?1)
             WHERE \"unique\" <> 0
             ORDER BY seq",
        )
        .map_err(sqlite_error::storage)?;
    let indexes = statement
        .query_map([table], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)?;
    let has_primary_key_index = indexes.iter().any(|(_, origin)| origin == "pk");

    for (index, _) in &indexes {
        let mut columns = connection
            .prepare(
                "SELECT name, coll
                 FROM pragma_index_xinfo(?1)
                 WHERE key = 1
                 ORDER BY seqno",
            )
            .map_err(sqlite_error::storage)?;
        let terms = columns
            .query_map([index], |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                ))
            })
            .map_err(sqlite_error::storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_error::storage)?;
        if !terms.iter().any(|(name, _)| name.as_deref() == Some(key)) {
            return Err(precondition(format!(
                "unique index {index} on source table {table} does not include shard key {key}"
            )));
        }
        if !terms.iter().any(|(name, collation)| {
            name.as_deref() == Some(key)
                && collation
                    .as_deref()
                    .is_some_and(|value| value.eq_ignore_ascii_case("BINARY"))
        }) {
            return Err(precondition(format!(
                "unique index {index} on source table {table} does not compare shard key {key} with BINARY collation"
            )));
        }
    }

    if !has_primary_key_index {
        let rowid_primary_key = connection
            .query_row(
                "SELECT name FROM pragma_table_xinfo(?1) WHERE pk <> 0 ORDER BY pk LIMIT 1",
                [table],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sqlite_error::storage)?;
        if rowid_primary_key
            .as_deref()
            .is_some_and(|column| column != key)
        {
            return Err(precondition(format!(
                "rowid primary key on source table {table} does not include shard key {key}"
            )));
        }
    }
    Ok(())
}

fn validate_runtime_key_values(
    connection: &Connection,
    table: &str,
    column: &str,
    key_type: SqliteImportKeyType,
) -> EngineResult<()> {
    let expected = match key_type {
        SqliteImportKeyType::Int64 => "integer",
        SqliteImportKeyType::Text => "text",
        SqliteImportKeyType::Binary => "blob",
    };
    let identifier = quote_identifier(column);
    let sql = format!(
        "SELECT typeof({identifier}) FROM {} WHERE {identifier} IS NULL OR typeof({identifier}) <> ?1 LIMIT 1",
        quote_identifier(table)
    );
    let observed = connection
        .query_row(&sql, [expected], |row| row.get::<_, String>(0))
        .optional()
        .map_err(sqlite_error::storage)?;
    if let Some(observed) = observed {
        return Err(precondition(format!(
            "shard-key column {table}.{column} contains runtime SQLite type {observed}; expected {expected}"
        )));
    }

    if matches!(key_type, SqliteImportKeyType::Text) {
        let sql = format!("SELECT {identifier} FROM {}", quote_identifier(table));
        let mut statement = connection.prepare(&sql).map_err(sqlite_error::storage)?;
        let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
        let mut row_number = 0_u64;
        while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
            row_number += 1;
            match row.get_ref(0).map_err(sqlite_error::storage)? {
                ValueRef::Text(bytes) if str::from_utf8(bytes).is_ok() => {}
                ValueRef::Text(_) => {
                    return Err(EngineError::new(
                        EngineErrorKind::InvalidTextEncoding,
                        format!(
                            "text shard-key column {table}.{column} contains invalid UTF-8 at source row {row_number}"
                        ),
                    ));
                }
                _ => unreachable!("runtime storage classes were validated before UTF-8"),
            }
        }
    }
    Ok(())
}

fn inventory_foreign_keys(
    connection: &Connection,
    table: &str,
) -> EngineResult<Vec<OmittedForeignKey>> {
    let mut statement = connection
        .prepare(
            "SELECT id, seq, \"table\", \"from\", \"to\", on_update, on_delete, \"match\"
             FROM pragma_foreign_key_list(?1)
             ORDER BY id, seq",
        )
        .map_err(sqlite_error::storage)?;
    let rows = statement
        .query_map([table], |row| {
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

    let mut grouped = BTreeMap::<i64, ForeignKeyBuilder>::new();
    for (id, seq, referenced_table, column, referenced_column, on_update, on_delete, match_name) in
        rows
    {
        let entry = grouped.entry(id).or_insert_with(|| ForeignKeyBuilder {
            referenced_table: referenced_table.clone(),
            columns: Vec::new(),
            referenced_columns: Vec::new(),
            on_update: on_update.clone(),
            on_delete: on_delete.clone(),
            match_name: match_name.clone(),
        });
        if entry.referenced_table != referenced_table
            || entry.on_update != on_update
            || entry.on_delete != on_delete
            || entry.match_name != match_name
        {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("foreign-key metadata for source table {table} is inconsistent"),
            ));
        }
        entry.columns.push((seq, column));
        entry.referenced_columns.push((seq, referenced_column));
    }

    let mut foreign_keys = Vec::with_capacity(grouped.len());
    for (_, mut foreign_key) in grouped {
        if !foreign_key.match_name.eq_ignore_ascii_case("NONE") {
            return Err(precondition(format!(
                "source table {table} uses unsupported audited foreign-key MATCH mode {}",
                foreign_key.match_name
            )));
        }
        foreign_key.columns.sort_by_key(|(seq, _)| *seq);
        foreign_key.referenced_columns.sort_by_key(|(seq, _)| *seq);
        let referenced_columns = resolve_referenced_columns(
            connection,
            table,
            &foreign_key.referenced_table,
            foreign_key.referenced_columns,
        )?;
        foreign_keys.push(OmittedForeignKey {
            table: table.to_owned(),
            columns: foreign_key
                .columns
                .into_iter()
                .map(|(_, column)| column)
                .collect(),
            referenced_table: foreign_key.referenced_table,
            referenced_columns,
            on_update: foreign_key.on_update,
            on_delete: foreign_key.on_delete,
        });
    }
    Ok(foreign_keys)
}

fn resolve_referenced_columns(
    connection: &Connection,
    child_table: &str,
    parent_table: &str,
    columns: Vec<(i64, Option<String>)>,
) -> EngineResult<Vec<String>> {
    if columns.iter().all(|(_, column)| column.is_some()) {
        return Ok(columns
            .into_iter()
            .map(|(_, column)| column.expect("checked above"))
            .collect());
    }
    if columns.iter().any(|(_, column)| column.is_some()) {
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
    if parent_key.len() != columns.len() || parent_key.is_empty() {
        return Err(precondition(format!(
            "foreign key from {child_table} omits referenced columns, but parent table {parent_table} has no matching resolvable primary key"
        )));
    }
    Ok(parent_key)
}

fn inventory_sequences(
    connection: &Connection,
    tables: &[SourceTable],
) -> EngineResult<Vec<SourceSequence>> {
    let has_sequence = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM main.sqlite_schema WHERE type = 'table' AND name = 'sqlite_sequence')",
            [],
            |row| row.get::<_, bool>(0),
        )
        .map_err(sqlite_error::storage)?;
    if !has_sequence {
        return Ok(Vec::new());
    }
    let mut statement = connection
        .prepare("SELECT name, seq FROM main.sqlite_sequence ORDER BY name COLLATE BINARY, rowid")
        .map_err(sqlite_error::storage)?;
    let sequences = statement
        .query_map([], |row| {
            Ok(SourceSequence {
                table: row.get(0)?,
                seq: row.get(1)?,
            })
        })
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)?;
    let table_names = tables
        .iter()
        .map(SourceTable::name)
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    for sequence in &sequences {
        if !table_names.contains(sequence.table()) {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "sqlite_sequence contains unknown application table {}",
                    sequence.table
                ),
            ));
        }
        if !seen.insert(sequence.table.as_str()) {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "sqlite_sequence contains duplicate rows for application table {}",
                    sequence.table
                ),
            ));
        }
    }
    Ok(sequences)
}

fn verify_staged_schema(
    source: &Connection,
    tables: &[SourceTable],
    indexes: &[SourceIndex],
) -> EngineResult<()> {
    let verification = Connection::open_in_memory().map_err(sqlite_error::storage)?;
    verification
        .execute_batch("PRAGMA trusted_schema = OFF; PRAGMA foreign_keys = OFF")
        .map_err(sqlite_error::storage)?;
    for table in tables {
        if table.source_create_sql().trim().is_empty() {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("source table {} has empty CREATE TABLE SQL", table.name()),
            ));
        }
        verification
            .execute(table.staged_create_sql(), [])
            .map_err(|error| {
                sqlite_error::storage(error).context(format!(
                    "failed to verify staged CREATE TABLE for {}",
                    table.name()
                ))
            })?;
    }
    for index in indexes {
        verification
            .execute(index.create_sql(), [])
            .map_err(|error| {
                sqlite_error::storage(error).context(format!(
                    "failed to verify staged CREATE INDEX {}",
                    index.name()
                ))
            })?;
    }

    for table in tables {
        let staged_columns = inventory_columns(&verification, table.name())?;
        if staged_columns != table.columns {
            return Err(precondition(format!(
                "staged CREATE TABLE verification changed column metadata for {}",
                table.name()
            )));
        }
        let (without_rowid, strict) = table_flags(&verification, table.name())?;
        if without_rowid != table.without_rowid() || strict != table.strict() {
            return Err(precondition(format!(
                "staged CREATE TABLE verification changed table flags for {}",
                table.name()
            )));
        }
        let remaining_foreign_keys = verification
            .query_row(
                "SELECT count(DISTINCT id) FROM pragma_foreign_key_list(?1)",
                [table.name()],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sqlite_error::storage)?;
        if remaining_foreign_keys != 0 {
            return Err(precondition(format!(
                "staged CREATE TABLE verification retained a foreign key on {}",
                table.name()
            )));
        }
        let source_indexes = inventory_index_signatures(source, table.name())?;
        let staged_indexes = inventory_index_signatures(&verification, table.name())?;
        if staged_indexes != source_indexes {
            return Err(precondition(format!(
                "staged CREATE TABLE verification changed index or uniqueness metadata for {}",
                table.name()
            )));
        }
    }
    Ok(())
}

fn inventory_index_signatures(
    connection: &Connection,
    table: &str,
) -> EngineResult<Vec<IndexSignature>> {
    let mut statement = connection
        .prepare(
            "SELECT name, \"unique\", origin, partial
             FROM pragma_index_list(?1)
             ORDER BY name COLLATE BINARY",
        )
        .map_err(sqlite_error::storage)?;
    let indexes = statement
        .query_map([table], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)? != 0,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)? != 0,
            ))
        })
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)?;
    indexes
        .into_iter()
        .map(|(name, unique, origin, partial)| {
            let mut terms_statement = connection
                .prepare(
                    "SELECT cid, name, \"desc\", coll, key
                     FROM pragma_index_xinfo(?1)
                     ORDER BY seqno",
                )
                .map_err(sqlite_error::storage)?;
            let terms = terms_statement
                .query_map([name.as_str()], |row| {
                    Ok(IndexTerm {
                        cid: row.get(0)?,
                        name: row.get(1)?,
                        descending: row.get::<_, i64>(2)? != 0,
                        collation: row.get(3)?,
                        key: row.get::<_, i64>(4)? != 0,
                    })
                })
                .map_err(sqlite_error::storage)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sqlite_error::storage)?;
            Ok(IndexSignature {
                name,
                unique,
                origin,
                partial,
                terms,
            })
        })
        .collect()
}

fn table_flags(connection: &Connection, table: &str) -> EngineResult<(bool, bool)> {
    connection
        .query_row(
            "SELECT wr, strict FROM pragma_table_list WHERE schema = 'main' AND name = ?1 AND type = 'table'",
            [table],
            |row| Ok((row.get::<_, i64>(0)? != 0, row.get::<_, i64>(1)? != 0)),
        )
        .map_err(sqlite_error::storage)
}

fn application_schema_digest(connection: &Connection) -> EngineResult<[u8; 32]> {
    let mut statement = connection
        .prepare(
            "SELECT name, type, sql
             FROM main.sqlite_schema
             WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
             ORDER BY name COLLATE BINARY, type COLLATE BINARY, sql COLLATE BINARY",
        )
        .map_err(sqlite_error::storage)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"briskdb-sqlite-application-schema-v1\0");
    hasher.update(&(rows.len() as u64).to_le_bytes());
    for (name, object_type, sql) in rows {
        hash_field(&mut hasher, Some(name.as_bytes()));
        hash_field(&mut hasher, Some(object_type.as_bytes()));
        hash_field(&mut hasher, sql.as_deref().map(str::as_bytes));
    }
    Ok(*hasher.finalize().as_bytes())
}

fn hash_field(hasher: &mut blake3::Hasher, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            hasher.update(&(value.len() as u64).to_le_bytes());
            hasher.update(value);
        }
        None => {
            hasher.update(&u64::MAX.to_le_bytes());
        }
    }
}

fn remove_table_foreign_key_clauses(sql: &str) -> EngineResult<(String, usize)> {
    let (open, close) = outer_table_parentheses(sql)
        .ok_or_else(|| precondition("cannot conservatively locate the CREATE TABLE column list"))?;
    let body = &sql[open + 1..close];
    let ranges = top_level_element_ranges(body)?;
    let mut kept = Vec::with_capacity(ranges.len());
    let mut removed = 0;
    for (start, end) in ranges {
        let element = &body[start..end];
        if is_table_foreign_key_clause(element)? {
            removed += 1;
        } else {
            kept.push(element);
        }
    }
    if kept.is_empty() {
        return Err(precondition(
            "foreign-key omission would leave an empty CREATE TABLE definition",
        ));
    }
    let mut rewritten = String::with_capacity(sql.len());
    rewritten.push_str(&sql[..open + 1]);
    for (index, element) in kept.into_iter().enumerate() {
        if index != 0 {
            rewritten.push(',');
        }
        rewritten.push_str(element);
    }
    rewritten.push_str(&sql[close..]);
    Ok((rewritten, removed))
}

fn outer_table_parentheses(sql: &str) -> Option<(usize, usize)> {
    let mut scanner = SqlScanner::new(sql);
    let mut open = None;
    let mut depth = 0_usize;
    while let Some((offset, byte)) = scanner.next_normal_byte() {
        match byte {
            b'(' => {
                if open.is_none() {
                    open = Some(offset);
                }
                depth += 1;
            }
            b')' if open.is_some() => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some((open?, offset));
                }
            }
            _ => {}
        }
    }
    None
}

fn top_level_element_ranges(body: &str) -> EngineResult<Vec<(usize, usize)>> {
    let mut scanner = SqlScanner::new(body);
    let mut depth = 0_i64;
    let mut start = 0;
    let mut ranges = Vec::new();
    while let Some((offset, byte)) = scanner.next_normal_byte() {
        match byte {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth < 0 {
                    return Err(precondition("malformed CREATE TABLE parentheses"));
                }
            }
            b',' if depth == 0 => {
                ranges.push((start, offset));
                start = offset + 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(precondition("malformed CREATE TABLE parentheses"));
    }
    ranges.push((start, body.len()));
    Ok(ranges)
}

fn is_table_foreign_key_clause(element: &str) -> EngineResult<bool> {
    let mut lexer = PrefixLexer::new(element);
    let Some(first) = lexer.next_token()? else {
        return Err(precondition("CREATE TABLE contains an empty table element"));
    };
    let keyword = if first.is_word("CONSTRAINT") {
        if lexer.next_token()?.is_none() {
            return Err(precondition("CREATE TABLE has CONSTRAINT without a name"));
        }
        lexer.next_token()?
    } else {
        Some(first)
    };
    let Some(keyword) = keyword else {
        return Ok(false);
    };
    if !keyword.is_word("FOREIGN") {
        return Ok(false);
    }
    Ok(lexer
        .next_token()?
        .is_some_and(|token| token.is_word("KEY")))
}

struct SqlScanner<'a> {
    bytes: &'a [u8],
    offset: usize,
    state: ScanState,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ScanState {
    Normal,
    SingleQuote,
    DoubleQuote,
    Backtick,
    Bracket,
    LineComment,
    BlockComment,
}

impl<'a> SqlScanner<'a> {
    fn new(sql: &'a str) -> Self {
        Self {
            bytes: sql.as_bytes(),
            offset: 0,
            state: ScanState::Normal,
        }
    }

    fn next_normal_byte(&mut self) -> Option<(usize, u8)> {
        while self.offset < self.bytes.len() {
            let offset = self.offset;
            let byte = self.bytes[offset];
            let next = self.bytes.get(offset + 1).copied();
            self.offset += 1;
            match self.state {
                ScanState::Normal => match (byte, next) {
                    (b'\'', _) => self.state = ScanState::SingleQuote,
                    (b'"', _) => self.state = ScanState::DoubleQuote,
                    (b'`', _) => self.state = ScanState::Backtick,
                    (b'[', _) => self.state = ScanState::Bracket,
                    (b'-', Some(b'-')) => {
                        self.offset += 1;
                        self.state = ScanState::LineComment;
                    }
                    (b'/', Some(b'*')) => {
                        self.offset += 1;
                        self.state = ScanState::BlockComment;
                    }
                    _ => return Some((offset, byte)),
                },
                ScanState::SingleQuote => {
                    if byte == b'\'' {
                        if next == Some(b'\'') {
                            self.offset += 1;
                        } else {
                            self.state = ScanState::Normal;
                        }
                    }
                }
                ScanState::DoubleQuote => {
                    if byte == b'"' {
                        if next == Some(b'"') {
                            self.offset += 1;
                        } else {
                            self.state = ScanState::Normal;
                        }
                    }
                }
                ScanState::Backtick => {
                    if byte == b'`' {
                        if next == Some(b'`') {
                            self.offset += 1;
                        } else {
                            self.state = ScanState::Normal;
                        }
                    }
                }
                ScanState::Bracket => {
                    if byte == b']' {
                        self.state = ScanState::Normal;
                    }
                }
                ScanState::LineComment => {
                    if matches!(byte, b'\n' | b'\r') {
                        self.state = ScanState::Normal;
                    }
                }
                ScanState::BlockComment => {
                    if byte == b'*' && next == Some(b'/') {
                        self.offset += 1;
                        self.state = ScanState::Normal;
                    }
                }
            }
        }
        None
    }
}

#[derive(Clone, Copy)]
struct PrefixToken<'a> {
    text: &'a str,
    word: bool,
}

impl PrefixToken<'_> {
    fn is_word(self, expected: &str) -> bool {
        self.word && self.text.eq_ignore_ascii_case(expected)
    }
}

struct PrefixLexer<'a> {
    source: &'a str,
    offset: usize,
}

impl<'a> PrefixLexer<'a> {
    fn new(source: &'a str) -> Self {
        Self { source, offset: 0 }
    }

    fn next_token(&mut self) -> EngineResult<Option<PrefixToken<'a>>> {
        self.skip_trivia()?;
        let bytes = self.source.as_bytes();
        if self.offset == bytes.len() {
            return Ok(None);
        }
        let start = self.offset;
        let byte = bytes[self.offset];
        if byte.is_ascii_alphabetic() || byte == b'_' {
            self.offset += 1;
            while self.offset < bytes.len()
                && (bytes[self.offset].is_ascii_alphanumeric() || bytes[self.offset] == b'_')
            {
                self.offset += 1;
            }
            return Ok(Some(PrefixToken {
                text: &self.source[start..self.offset],
                word: true,
            }));
        }
        if matches!(byte, b'\'' | b'"' | b'`' | b'[') {
            let closing = if byte == b'[' { b']' } else { byte };
            self.offset += 1;
            while self.offset < bytes.len() {
                if bytes[self.offset] == closing {
                    self.offset += 1;
                    if closing != b']' && self.offset < bytes.len() && bytes[self.offset] == closing
                    {
                        self.offset += 1;
                        continue;
                    }
                    return Ok(Some(PrefixToken {
                        text: &self.source[start..self.offset],
                        word: false,
                    }));
                }
                self.offset += 1;
            }
            return Err(precondition("unterminated quoted token in CREATE TABLE"));
        }
        self.offset += 1;
        Ok(Some(PrefixToken {
            text: &self.source[start..self.offset],
            word: false,
        }))
    }

    fn skip_trivia(&mut self) -> EngineResult<()> {
        let bytes = self.source.as_bytes();
        loop {
            while self.offset < bytes.len() && bytes[self.offset].is_ascii_whitespace() {
                self.offset += 1;
            }
            if bytes.get(self.offset..self.offset + 2) == Some(b"--") {
                self.offset += 2;
                while self.offset < bytes.len() && !matches!(bytes[self.offset], b'\n' | b'\r') {
                    self.offset += 1;
                }
                continue;
            }
            if bytes.get(self.offset..self.offset + 2) == Some(b"/*") {
                self.offset += 2;
                let Some(relative_end) = self.source[self.offset..].find("*/") else {
                    return Err(precondition("unterminated comment in CREATE TABLE"));
                };
                self.offset += relative_end + 2;
                continue;
            }
            return Ok(());
        }
    }
}

fn sqlite_affinity(declared_type: &str) -> SqliteAffinity {
    let declared_type = declared_type.to_ascii_uppercase();
    if declared_type.contains("INT") {
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
    }
}

fn import_key_type_for_affinity(declared_type: &str) -> Option<SqliteImportKeyType> {
    match sqlite_affinity(declared_type) {
        SqliteAffinity::Integer => Some(SqliteImportKeyType::Int64),
        SqliteAffinity::Text => Some(SqliteImportKeyType::Text),
        SqliteAffinity::Blob => Some(SqliteImportKeyType::Binary),
        SqliteAffinity::Real | SqliteAffinity::Numeric => None,
    }
}

fn key_type_matches_affinity(key_type: SqliteImportKeyType, affinity: SqliteAffinity) -> bool {
    matches!(
        (key_type, affinity),
        (SqliteImportKeyType::Int64, SqliteAffinity::Integer)
            | (SqliteImportKeyType::Text, SqliteAffinity::Text)
            | (SqliteImportKeyType::Binary, SqliteAffinity::Blob)
    )
}

fn affinity_name(affinity: SqliteAffinity) -> &'static str {
    match affinity {
        SqliteAffinity::Integer => "INTEGER",
        SqliteAffinity::Text => "TEXT",
        SqliteAffinity::Blob => "BLOB",
        SqliteAffinity::Real => "REAL",
        SqliteAffinity::Numeric => "NUMERIC",
    }
}

fn key_type_name(key_type: SqliteImportKeyType) -> &'static str {
    match key_type {
        SqliteImportKeyType::Int64 => "int64",
        SqliteImportKeyType::Text => "text",
        SqliteImportKeyType::Binary => "binary",
    }
}

fn core_key_type(key_type: SqliteImportKeyType) -> ShardKeyType {
    match key_type {
        SqliteImportKeyType::Int64 => ShardKeyType::Int64,
        SqliteImportKeyType::Text => ShardKeyType::Text,
        SqliteImportKeyType::Binary => ShardKeyType::Binary,
    }
}

fn ensure_catalog_identifier(identifier: &str, context: &str) -> EngineResult<()> {
    if crate::core::validate_catalog_identifier(identifier) {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            format!(
                "{context} identifier {identifier:?} must be 1 to 63 bytes of canonical lowercase ASCII and may not use a reserved prefix"
            ),
        ))
    }
}

fn display_names(names: &[&str]) -> String {
    if names.is_empty() {
        "none".to_owned()
    } else {
        names.join(", ")
    }
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn terminated_schema_object(sql: &str) -> String {
    let sql = sql.trim_end();
    let mut object = String::with_capacity(sql.len() + 3);
    object.push_str(sql);
    if !sql.ends_with(';') {
        // A source object may end in a line comment. Start the terminator on a
        // fresh line so that comment cannot swallow the object boundary.
        object.push('\n');
        object.push(';');
    }
    object.push('\n');
    object
}

fn precondition(diagnostic: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::FailedPrecondition, diagnostic)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use rusqlite::Connection;

    use super::*;

    fn create_source(sql: &str) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        let connection = Connection::open(file.path()).unwrap();
        connection.execute_batch(sql).unwrap();
        drop(connection);
        file
    }

    fn sharded_primary(name: &str) -> SqliteTableImportPlan {
        SqliteTableImportPlan::sharded_by_primary_key(name)
    }

    #[test]
    fn inventories_exact_schema_counts_indexes_sequences_and_generated_columns() {
        let source = create_source(
            "CREATE TABLE records (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 tenant_id TEXT NOT NULL COLLATE BINARY,
                 amount INTEGER NOT NULL DEFAULT 7,
                 doubled INTEGER GENERATED ALWAYS AS (amount * 2) STORED,
                 UNIQUE (id, tenant_id)
             ) STRICT;
             CREATE INDEX records_amount ON records(amount) WHERE amount > 0;
             INSERT INTO records(tenant_id, amount) VALUES ('north', 11), ('south', 13);
             DELETE FROM records WHERE id = 2;",
        );
        let plan = SqliteImportPlan::new(vec![sharded_primary("records")]);
        let snapshot = SourceSnapshot::open(source.path(), &plan).unwrap();

        assert_eq!(snapshot.tables().len(), 1);
        let table = &snapshot.tables()[0];
        assert_eq!(table.name(), "records");
        assert_eq!(table.source_rows(), 1);
        assert!(table.strict());
        assert!(!table.without_rowid());
        assert_eq!(table.source_create_sql(), table.staged_create_sql());
        assert_eq!(table.columns().len(), 4);
        assert_eq!(table.columns()[3].name(), "doubled");
        assert_eq!(table.columns()[3].hidden(), 3);
        assert!(!table.columns()[3].writable());
        assert_eq!(table.shard_key().unwrap().column(), "id");
        assert_eq!(table.shard_key().unwrap().column_index(), 0);
        assert_eq!(
            table.shard_key().unwrap().key_type(),
            SqliteImportKeyType::Int64
        );
        assert_eq!(snapshot.explicit_indexes().len(), 1);
        assert_eq!(snapshot.explicit_indexes()[0].name(), "records_amount");
        assert_eq!(snapshot.explicit_indexes()[0].table(), "records");
        assert_eq!(snapshot.sequences()[0].table(), "records");
        assert_eq!(snapshot.sequences()[0].seq(), 2);
        assert!(snapshot.omitted_foreign_keys().is_empty());
        assert_ne!(snapshot.schema_digest(), [0; 32]);
        assert_eq!(
            snapshot
                .connection()
                .query_row("SELECT count(*) FROM records", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn exact_plan_coverage_rejects_missing_unknown_duplicate_and_invalid_names() {
        let source = create_source(
            "CREATE TABLE alpha(id INTEGER PRIMARY KEY);
             CREATE TABLE beta(id INTEGER PRIMARY KEY);",
        );
        for (plan, expected) in [
            (
                SqliteImportPlan::new(vec![sharded_primary("alpha")]),
                "missing: beta",
            ),
            (
                SqliteImportPlan::new(vec![sharded_primary("alpha"), sharded_primary("gamma")]),
                "unknown: gamma",
            ),
            (
                SqliteImportPlan::new(vec![
                    sharded_primary("alpha"),
                    sharded_primary("alpha"),
                    sharded_primary("beta"),
                ]),
                "more than once",
            ),
        ] {
            let error = SourceSnapshot::open(source.path(), &plan).unwrap_err();
            assert!(error.diagnostic().contains(expected), "{error}");
        }

        let invalid = create_source("CREATE TABLE \"MixedCase\"(id INTEGER PRIMARY KEY);");
        let error = SourceSnapshot::open(
            invalid.path(),
            &SqliteImportPlan::new(vec![sharded_primary("MixedCase")]),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
    }

    #[test]
    fn rejects_views_triggers_and_virtual_tables_before_staging() {
        let view = create_source(
            "CREATE TABLE records(id INTEGER PRIMARY KEY);
             CREATE VIEW record_view AS SELECT * FROM records;",
        );
        let error = SourceSnapshot::open(
            view.path(),
            &SqliteImportPlan::new(vec![sharded_primary("records")]),
        )
        .unwrap_err();
        assert!(error.diagnostic().contains("unsupported view"));

        let trigger = create_source(
            "CREATE TABLE records(id INTEGER PRIMARY KEY);
             CREATE TRIGGER records_ai AFTER INSERT ON records BEGIN SELECT 1; END;",
        );
        let error = SourceSnapshot::open(
            trigger.path(),
            &SqliteImportPlan::new(vec![sharded_primary("records")]),
        )
        .unwrap_err();
        assert!(error.diagnostic().contains("unsupported trigger"));

        let virtual_table = tempfile::NamedTempFile::new().unwrap();
        let connection = Connection::open(virtual_table.path()).unwrap();
        if connection
            .execute_batch("CREATE VIRTUAL TABLE spatial USING rtree(id, min_x, max_x)")
            .is_ok()
        {
            drop(connection);
            let error = SourceSnapshot::open(
                virtual_table.path(),
                &SqliteImportPlan::new(vec![sharded_primary("spatial")]),
            )
            .unwrap_err();
            assert!(
                error
                    .diagnostic()
                    .contains("unsupported SQLite table type virtual")
            );
        }
    }

    #[test]
    fn rejects_unknown_reserved_schema_objects_instead_of_silently_omitting_them() {
        let source = create_source(
            "CREATE TABLE ordinary(id INTEGER PRIMARY KEY);
             INSERT INTO ordinary VALUES (1);
             PRAGMA writable_schema = ON;
             UPDATE sqlite_schema
             SET name = 'sqlite_hidden',
                 tbl_name = 'sqlite_hidden',
                 sql = 'CREATE TABLE sqlite_hidden(id INTEGER PRIMARY KEY)'
             WHERE type = 'table' AND name = 'ordinary';
             PRAGMA writable_schema = OFF;
             PRAGMA schema_version = 2;",
        );
        let connection = Connection::open(source.path()).unwrap();
        assert_eq!(
            connection
                .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        drop(connection);

        let error =
            SourceSnapshot::open(source.path(), &SqliteImportPlan::new(Vec::new())).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(
            error
                .diagnostic()
                .contains("unsupported reserved table object sqlite_hidden")
        );
    }

    #[test]
    fn resolves_supported_primary_key_affinities_and_requires_explicit_composite_key() {
        let integer = create_source("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT);");
        let snapshot = SourceSnapshot::open(
            integer.path(),
            &SqliteImportPlan::new(vec![sharded_primary("items")]),
        )
        .unwrap();
        assert_eq!(
            snapshot.tables()[0].shard_key().unwrap().key_type(),
            SqliteImportKeyType::Int64
        );

        let text = create_source("CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY);");
        let snapshot = SourceSnapshot::open(
            text.path(),
            &SqliteImportPlan::new(vec![sharded_primary("items")]),
        )
        .unwrap();
        assert_eq!(
            snapshot.tables()[0].shard_key().unwrap().key_type(),
            SqliteImportKeyType::Text
        );

        let binary = create_source("CREATE TABLE items(id BLOB NOT NULL PRIMARY KEY);");
        let snapshot = SourceSnapshot::open(
            binary.path(),
            &SqliteImportPlan::new(vec![sharded_primary("items")]),
        )
        .unwrap();
        assert_eq!(
            snapshot.tables()[0].shard_key().unwrap().key_type(),
            SqliteImportKeyType::Binary
        );

        let composite = create_source(
            "CREATE TABLE items(tenant TEXT NOT NULL, id INTEGER NOT NULL, PRIMARY KEY(tenant,id));",
        );
        let error = SourceSnapshot::open(
            composite.path(),
            &SqliteImportPlan::new(vec![sharded_primary("items")]),
        )
        .unwrap_err();
        assert!(error.diagnostic().contains("primary key has 2 columns"));
        let snapshot = SourceSnapshot::open(
            composite.path(),
            &SqliteImportPlan::new(vec![SqliteTableImportPlan::sharded(
                "items",
                "tenant",
                SqliteImportKeyType::Text,
            )]),
        )
        .unwrap();
        assert_eq!(snapshot.tables()[0].shard_key().unwrap().column(), "tenant");

        let noncanonical =
            create_source("CREATE TABLE items(\"Bad-Key\" TEXT NOT NULL PRIMARY KEY, value TEXT);");
        let error = SourceSnapshot::open(
            noncanonical.path(),
            &SqliteImportPlan::new(vec![sharded_primary("items")]),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
        assert!(error.diagnostic().contains("shard-key column"));
    }

    #[test]
    fn exposes_an_unshadowed_implicit_rowid_and_rejects_total_shadowing() {
        let source = create_source(
            "CREATE TABLE lookup(code TEXT NOT NULL PRIMARY KEY, value TEXT);
             INSERT INTO lookup(rowid, code, value) VALUES (41, 'a', 'one');",
        );
        let snapshot = SourceSnapshot::open(
            source.path(),
            &SqliteImportPlan::new(vec![sharded_primary("lookup")]),
        )
        .unwrap();
        assert_eq!(snapshot.tables()[0].rowid_projection(), Some("_rowid_"));

        let integer = create_source("CREATE TABLE lookup(id INTEGER PRIMARY KEY, value TEXT);");
        let snapshot = SourceSnapshot::open(
            integer.path(),
            &SqliteImportPlan::new(vec![sharded_primary("lookup")]),
        )
        .unwrap();
        assert_eq!(snapshot.tables()[0].rowid_projection(), None);

        let shadowed = create_source(
            "CREATE TABLE lookup(
                 code TEXT NOT NULL PRIMARY KEY,
                 rowid TEXT,
                 _rowid_ TEXT,
                 oid TEXT
             );",
        );
        let error = SourceSnapshot::open(
            shadowed.path(),
            &SqliteImportPlan::new(vec![sharded_primary("lookup")]),
        )
        .unwrap_err();
        assert!(
            error
                .diagnostic()
                .contains("shadows _rowid_, rowid, and oid")
        );
    }

    #[test]
    fn rejects_nullable_wrong_affinity_wrong_storage_and_nonbinary_text_keys() {
        let nullable = create_source("CREATE TABLE items(id TEXT PRIMARY KEY);");
        let error = SourceSnapshot::open(
            nullable.path(),
            &SqliteImportPlan::new(vec![sharded_primary("items")]),
        )
        .unwrap_err();
        assert!(error.diagnostic().contains("physically NOT NULL"));

        let affinity = create_source("CREATE TABLE items(id REAL NOT NULL PRIMARY KEY);");
        let error = SourceSnapshot::open(
            affinity.path(),
            &SqliteImportPlan::new(vec![sharded_primary("items")]),
        )
        .unwrap_err();
        assert!(
            error
                .diagnostic()
                .contains("unsupported SQLite REAL affinity")
        );

        let runtime = create_source(
            "CREATE TABLE items(
                 tenant_id INTEGER NOT NULL,
                 id INTEGER NOT NULL,
                 PRIMARY KEY(tenant_id, id)
             );
             INSERT INTO items VALUES ('not-an-integer', 1);",
        );
        let error = SourceSnapshot::open(
            runtime.path(),
            &SqliteImportPlan::new(vec![SqliteTableImportPlan::sharded(
                "items",
                "tenant_id",
                SqliteImportKeyType::Int64,
            )]),
        )
        .unwrap_err();
        assert!(error.diagnostic().contains("runtime SQLite type text"));

        let collation = create_source(
            "CREATE TABLE items(id TEXT NOT NULL COLLATE NOCASE PRIMARY KEY);
             INSERT INTO items VALUES ('alpha');",
        );
        let error = SourceSnapshot::open(
            collation.path(),
            &SqliteImportPlan::new(vec![sharded_primary("items")]),
        )
        .unwrap_err();
        assert!(error.diagnostic().contains("BINARY collation"));
    }

    #[test]
    fn rejects_invalid_utf8_text_keys_and_independent_unique_domains() {
        let source = create_source(
            "CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY);
             INSERT INTO items VALUES (CAST(x'80' AS TEXT));",
        );
        let error = SourceSnapshot::open(
            source.path(),
            &SqliteImportPlan::new(vec![sharded_primary("items")]),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidTextEncoding);

        let source = create_source(
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE);",
        );
        let error = SourceSnapshot::open(
            source.path(),
            &SqliteImportPlan::new(vec![sharded_primary("users")]),
        )
        .unwrap_err();
        assert!(error.diagnostic().contains("does not include shard key id"));
        SourceSnapshot::open(
            source.path(),
            &SqliteImportPlan::new(vec![SqliteTableImportPlan::global("users")]),
        )
        .unwrap();
    }

    #[test]
    fn foreign_keys_default_reject_and_table_level_omit_is_audited_and_verified() {
        let source = create_source(
            "CREATE TABLE parent(id INTEGER PRIMARY KEY);
             CREATE TABLE child(
                 id INTEGER PRIMARY KEY,
                 parent_id INTEGER NOT NULL DEFAULT 1,
                 CHECK(parent_id > 0),
                 UNIQUE(id, parent_id),
                 CONSTRAINT child_parent
                     FOREIGN KEY(parent_id) REFERENCES parent(id)
                     ON UPDATE CASCADE ON DELETE RESTRICT
             );",
        );
        let reject =
            SqliteImportPlan::new(vec![sharded_primary("child"), sharded_primary("parent")]);
        let error = SourceSnapshot::open(source.path(), &reject).unwrap_err();
        assert!(error.diagnostic().contains("foreign-key constraint"));

        let omit = SqliteImportPlan::new(vec![
            sharded_primary("child").with_foreign_key_policy(SqliteForeignKeyPolicy::Omit),
            sharded_primary("parent"),
        ]);
        let snapshot = SourceSnapshot::open(source.path(), &omit).unwrap();
        let child = snapshot
            .tables()
            .iter()
            .find(|table| table.name() == "child")
            .unwrap();
        assert!(child.staged_create_sql().contains("CHECK(parent_id > 0)"));
        assert!(child.staged_create_sql().contains("UNIQUE(id, parent_id)"));
        assert!(!child.staged_create_sql().contains("FOREIGN KEY"));
        assert_eq!(snapshot.omitted_foreign_keys().len(), 1);
        assert_eq!(snapshot.omitted_foreign_keys()[0].table, "child");
        assert_eq!(snapshot.omitted_foreign_keys()[0].columns, ["parent_id"]);
        assert_eq!(
            snapshot.omitted_foreign_keys()[0].referenced_table,
            "parent"
        );
        assert_eq!(
            snapshot.omitted_foreign_keys()[0].referenced_columns,
            ["id"]
        );
        assert_eq!(snapshot.omitted_foreign_keys()[0].on_update, "CASCADE");
        assert_eq!(snapshot.omitted_foreign_keys()[0].on_delete, "RESTRICT");
    }

    #[test]
    fn omission_conservatively_rejects_inline_foreign_keys() {
        let source = create_source(
            "CREATE TABLE parent(id INTEGER PRIMARY KEY);
             CREATE TABLE child(
                 id INTEGER PRIMARY KEY,
                 parent_id INTEGER REFERENCES parent(id)
             );",
        );
        let plan = SqliteImportPlan::new(vec![
            sharded_primary("child").with_foreign_key_policy(SqliteForeignKeyPolicy::Omit),
            sharded_primary("parent"),
        ]);
        let error = SourceSnapshot::open(source.path(), &plan).unwrap_err();
        assert!(
            error
                .diagnostic()
                .contains("only 0 conservative table-level")
        );
    }

    #[test]
    fn schema_batches_split_only_at_objects_and_declarations_match_placement() {
        let source = create_source(
            "CREATE TABLE alpha(id INTEGER PRIMARY KEY);
             CREATE TABLE beta(id INTEGER PRIMARY KEY, value TEXT UNIQUE);
             CREATE INDEX alpha_id_copy ON alpha(id);",
        );
        let snapshot = SourceSnapshot::open(
            source.path(),
            &SqliteImportPlan::new(vec![
                sharded_primary("alpha"),
                SqliteTableImportPlan::global("beta"),
            ]),
        )
        .unwrap();
        let exact = snapshot.schema_batches(usize::MAX).unwrap();
        assert_eq!(exact.len(), 1);
        assert!(
            exact[0].find("CREATE TABLE alpha").unwrap() < exact[0].find("CREATE INDEX").unwrap()
        );

        let largest = snapshot
            .tables()
            .iter()
            .map(|table| terminated_schema_object(table.staged_create_sql()).len())
            .chain(
                snapshot
                    .explicit_indexes()
                    .iter()
                    .map(|index| terminated_schema_object(index.create_sql()).len()),
            )
            .max()
            .unwrap();
        let split = snapshot.schema_batches(largest).unwrap();
        assert!(split.len() >= 2);
        assert!(split.iter().all(|batch| batch.len() <= largest));
        let error = snapshot.schema_batches(largest - 1).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);

        let declarations = snapshot
            .table_declarations(LogicalDatabaseId::new(1).unwrap())
            .unwrap();
        assert_eq!(declarations.len(), 2);
        assert_eq!(declarations[0].name(), "alpha");
        assert_eq!(declarations[1].name(), "beta");
    }

    #[test]
    fn digest_is_deterministic_and_changes_with_application_schema() {
        let source = create_source("CREATE TABLE records(id INTEGER PRIMARY KEY);");
        let plan = SqliteImportPlan::new(vec![sharded_primary("records")]);
        let first = SourceSnapshot::open(source.path(), &plan)
            .unwrap()
            .schema_digest();
        let second = SourceSnapshot::open(source.path(), &plan)
            .unwrap()
            .schema_digest();
        assert_eq!(first, second);

        Connection::open(source.path())
            .unwrap()
            .execute("CREATE INDEX records_id_copy ON records(id)", [])
            .unwrap();
        let changed = SourceSnapshot::open(source.path(), &plan)
            .unwrap()
            .schema_digest();
        assert_ne!(first, changed);
    }

    #[test]
    fn read_snapshot_is_consistent_and_main_file_is_not_modified() {
        let source = create_source(
            "PRAGMA journal_mode = WAL;
             CREATE TABLE records(id INTEGER PRIMARY KEY);
             INSERT INTO records VALUES (1);",
        );
        let before = fs::read(source.path()).unwrap();
        let plan = SqliteImportPlan::new(vec![sharded_primary("records")]);
        let snapshot = SourceSnapshot::open(source.path(), &plan).unwrap();
        let writer = Connection::open(source.path()).unwrap();
        writer
            .execute("INSERT INTO records VALUES (2)", [])
            .unwrap();
        assert_eq!(snapshot.tables()[0].source_rows(), 1);
        assert_eq!(
            snapshot
                .connection()
                .query_row("SELECT count(*) FROM records", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        drop(writer);
        drop(snapshot);
        assert_eq!(fs::read(source.path()).unwrap(), before);
    }

    #[cfg(unix)]
    #[test]
    fn source_symlink_is_rejected() {
        use std::os::unix::fs::symlink;

        let source = create_source("CREATE TABLE records(id INTEGER PRIMARY KEY);");
        let directory = tempfile::tempdir().unwrap();
        let link = directory.path().join("source.sqlite");
        symlink(source.path(), &link).unwrap();
        let error = SourceSnapshot::open(
            &link,
            &SqliteImportPlan::new(vec![sharded_primary("records")]),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(error.diagnostic().contains("real regular file"));
    }

    #[test]
    fn table_level_fk_remover_ignores_commas_and_keywords_in_nested_sql() {
        let sql = "CREATE TABLE child(\n\
            id INTEGER PRIMARY KEY,\n\
            note TEXT DEFAULT('FOREIGN, KEY'),\n\
            value INTEGER CHECK(value IN (1,2,3)),\n\
            /* retained comment */ CONSTRAINT uq UNIQUE(id,value),\n\
            CONSTRAINT fk FOREIGN KEY(value) REFERENCES parent(id)\n\
        ) STRICT";
        let (rewritten, removed) = remove_table_foreign_key_clauses(sql).unwrap();
        assert_eq!(removed, 1);
        assert!(rewritten.contains("DEFAULT('FOREIGN, KEY')"));
        assert!(rewritten.contains("CHECK(value IN (1,2,3))"));
        assert!(rewritten.contains("CONSTRAINT uq UNIQUE(id,value)"));
        assert!(!rewritten.contains("CONSTRAINT fk"));
        assert!(rewritten.ends_with(") STRICT"));
    }

    #[test]
    #[ignore = "requires BRISKDB_LARGE_DATA_DB to name the external SQLite fixture"]
    fn external_large_data_fixture_passes_the_explicit_normalized_plan() {
        let path = std::env::var_os("BRISKDB_LARGE_DATA_DB")
            .map(PathBuf::from)
            .expect("set BRISKDB_LARGE_DATA_DB to the read-only fixture path");
        let sharded = |name| SqliteTableImportPlan::sharded_by_primary_key(name);
        let sharded_omitting_foreign_keys = |name| {
            SqliteTableImportPlan::sharded_by_primary_key(name)
                .with_foreign_key_policy(SqliteForeignKeyPolicy::Omit)
        };
        let plan = SqliteImportPlan::new(vec![
            sharded("account_codes"),
            sharded("accounting_data"),
            SqliteTableImportPlan::global("accounts_payable"),
            SqliteTableImportPlan::global("accounts_receivable"),
            SqliteTableImportPlan::global("activation_keys"),
            sharded("cb_accounts"),
            SqliteTableImportPlan::global("check_layout"),
            sharded_omitting_foreign_keys("cust_contacts"),
            sharded_omitting_foreign_keys("customer_contacts"),
            sharded("customers"),
            sharded_omitting_foreign_keys("customers_vehicles"),
            sharded("drawers"),
            SqliteTableImportPlan::global("employees"),
            sharded("integrations"),
            SqliteTableImportPlan::global("inv_matrix"),
            sharded_omitting_foreign_keys("inv_matrix_tier"),
            sharded("inventory_groups"),
            sharded("inventory_main"),
            sharded("job_codes"),
            SqliteTableImportPlan::global("job_kits"),
            sharded("locations"),
            SqliteTableImportPlan::global("maintenance"),
            SqliteTableImportPlan::sharded(
                "maintenance_schedules",
                "service_name",
                SqliteImportKeyType::Text,
            ),
            sharded_omitting_foreign_keys("payments"),
            sharded("setup"),
            sharded("stores"),
            sharded("vehicles"),
            sharded("work_order_items"),
            sharded("work_order_job_codes"),
            SqliteTableImportPlan::global("work_order_kits"),
            sharded("work_orders"),
        ]);

        let snapshot = SourceSnapshot::open(&path, &plan).unwrap();
        assert_eq!(snapshot.tables().len(), 31);
        assert_eq!(
            snapshot
                .tables()
                .iter()
                .map(SourceTable::source_rows)
                .sum::<u64>(),
            1_536_282
        );
        assert_eq!(snapshot.omitted_foreign_keys().len(), 5);
        assert_eq!(snapshot.sequences().len(), 4);
    }

    #[allow(dead_code)]
    fn assert_path(_: &Path) {}
}
