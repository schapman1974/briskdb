//! Explicitly opt-in SQL mode: native ISAM catalog, immutable indexed SQLite
//! base files on shared storage, and atomic S3/Parquet delta publication.
//!
//! Normal `Engine`/`BriskDb` opens never select this mode. Its synchronous API
//! must run on an ordinary OS thread outside any entered Tokio runtime.
//! Queries use SQLite's
//! SQL engine and virtual tables; changes become visible on the next statement.
//! Each modifying statement is atomic within one table/key partition. A
//! statement spanning partitions is rejected before publication, not partially
//! committed. Online DDL, cross-partition transactions, generated IDs, foreign
//! keys and global unique indexes are deliberately not advertised.

#[cfg(feature = "s3-overlay-cli")]
pub mod cli;
mod cloud;
#[cfg(feature = "experimental-duckdb-reader")]
mod duckdb;
#[cfg(feature = "experimental-duckdb-reader")]
pub use duckdb::DuckDbReadOptions;
#[cfg(test)]
mod fault_tests;
mod file_index;
mod metadata;
mod parquet;
#[cfg(test)]
mod pruning_tests;
mod registry;
mod schema;
#[cfg(test)]
mod tests;
mod vtab;

pub use cloud::s3_store;
pub use object_store::ObjectStore;
pub use registry::{CompactionResult, ReadStats, WriteResult};
pub use schema::{Cell, Column, ColumnType, Row, Table};

use crate::{EngineError, EngineErrorKind};
use bytes::Bytes;
use object_store::PutMode;
use rusqlite::{
    Connection, OpenFlags,
    hooks::{AuthAction, AuthContext, Authorization},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, File, OpenOptions as FileOpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub type Result<T> = crate::EngineResult<T>;
pub(crate) const MAX_ROWS: usize = 100_000;
pub(crate) const MAX_BYTES: usize = 64 * 1024 * 1024;

pub(crate) fn invalid(message: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::InvalidArgument, message)
}
pub(crate) fn corrupt(message: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::DataCorruption, message)
}
pub(crate) fn limit(message: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::LimitExceeded, message)
}
pub(crate) fn storage_error(error: impl std::error::Error + Send + Sync + 'static) -> EngineError {
    let kind = match (&error as &dyn std::any::Any).downcast_ref::<rusqlite::Error>() {
        Some(rusqlite::Error::SqliteFailure(code, _)) => match code.extended_code {
            rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
            | rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE => EngineErrorKind::UniqueViolation,
            rusqlite::ffi::SQLITE_CONSTRAINT_NOTNULL => EngineErrorKind::NotNullViolation,
            rusqlite::ffi::SQLITE_AUTH => EngineErrorKind::PermissionDenied,
            rusqlite::ffi::SQLITE_INTERRUPT => EngineErrorKind::DeadlineExceeded,
            _ => EngineErrorKind::StorageUnavailable,
        },
        _ => EngineErrorKind::StorageUnavailable,
    };
    EngineError::from_source(kind, error.to_string(), error)
}
pub(crate) fn nonce() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| invalid(e.to_string()))?;
    Ok(bytes.iter().map(|v| format!("{v:02x}")).collect())
}
pub(crate) fn is_nonce(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// Persisted, credential-free configuration. Changes require a new database;
/// callers must not edit the catalog or immutable base files themselves.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub format: u32,
    pub database_id: String,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    pub shards: u16,
    pub partitions: u16,
    pub tables: Vec<Table>,
    pub max_pending_files: usize,
    pub compact_after_files: usize,
    pub write_retry_ms: u64,
}

impl Config {
    pub fn new(
        bucket: impl Into<String>,
        region: impl Into<String>,
        prefix: impl Into<String>,
        tables: Vec<Table>,
    ) -> Result<Self> {
        let config = Self {
            format: 1,
            database_id: nonce()?,
            bucket: bucket.into(),
            region: region.into(),
            prefix: prefix.into().trim_end_matches('/').to_owned(),
            shards: 4,
            partitions: 64,
            tables,
            max_pending_files: 64,
            compact_after_files: 32,
            write_retry_ms: 60_000,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.format != 1
            || !is_nonce(&self.database_id)
            || self.bucket.is_empty()
            || self.region.is_empty()
            || self.prefix.is_empty()
            || self.prefix.len() > 512
            || self.prefix.split('/').any(|v| {
                v.is_empty()
                    || v == "."
                    || v == ".."
                    || !v
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            })
            || !(2..=64).contains(&self.shards)
            || !(2..=256).contains(&self.partitions)
            || self.partitions % self.shards != 0
            || !(1..=64).contains(&self.tables.len())
            || !(2..=128).contains(&self.max_pending_files)
            || self.compact_after_files == 0
            || self.compact_after_files > self.max_pending_files
            || !(1..=120_000).contains(&self.write_retry_ms)
        {
            return Err(invalid("invalid overlay configuration bounds"));
        }
        let mut names = HashSet::new();
        for table in &self.tables {
            table.validate()?;
            if !names.insert(table.name.to_ascii_lowercase()) {
                return Err(invalid("duplicate overlay table"));
            }
        }
        Ok(())
    }

    pub(crate) fn namespace(&self) -> String {
        format!("{}/databases/{}", self.prefix, self.database_id)
    }
    pub(crate) fn partition(&self, key: &Cell) -> Result<u16> {
        // The same canonical encoding and BLAKE3 hash as BriskDB generation 1.
        let bytes = crate::core::canonical_shard_key_bytes(match key {
            Cell::Integer(v) => crate::core::CanonicalShardKeyRef::Int64(*v),
            Cell::Text(v) => crate::core::CanonicalShardKeyRef::Text(v),
            Cell::Blob(v) => crate::core::CanonicalShardKeyRef::Binary(v),
            _ => {
                return Err(invalid(
                    "routing keys must be non-null INTEGER, TEXT or BLOB",
                ));
            }
        });
        let hash = u64::from_le_bytes(blake3::hash(&bytes).as_bytes()[..8].try_into().unwrap());
        Ok((hash % u64::from(self.partitions)) as u16)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

/// Per-handle flags, not persisted credentials or a change to database identity.
/// Selecting `s3_overlay::Database` is the explicit storage-mode opt-in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OpenOptions {
    /// Use and publish advisory ISAM primary-key Parquet summaries.
    pub parquet_pruning: bool,
    /// Refuse modifying SQL and compaction before any payload publication.
    pub read_only: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            parquet_pruning: true,
            read_only: false,
        }
    }
}

/// A distinct SQL connection. All request-scoped snapshots and pending-file
/// caches are discarded between statements; reopening does not warm the root.
pub struct Database {
    connection: Connection,
    registry: Arc<registry::Registry>,
    last_read_stats: ReadStats,
    options: OpenOptions,
}

impl Database {
    /// Create a NEW database, optionally seeding immutable bases. Never converts
    /// or mutates an existing normal BriskDB database. No network/SQL writes are
    /// acknowledged until their Parquet object and conditional head are durable.
    pub fn create(
        root: impl AsRef<Path>,
        config: Config,
        store: Arc<dyn ObjectStore>,
        seed: BTreeMap<String, Vec<Row>>,
    ) -> Result<Self> {
        config.validate()?;
        if seed
            .keys()
            .any(|name| !config.tables.iter().any(|t| t.name == *name))
        {
            return Err(invalid("seed names an unknown table"));
        }
        // Validate all rows before touching either local or remote storage.
        for table in &config.tables {
            let mut keys = HashSet::new();
            for row in seed.get(&table.name).into_iter().flatten() {
                table.check_row(row)?;
                if !keys.insert(table.key(row)) {
                    return Err(invalid("duplicate primary key in seed"));
                }
            }
        }
        let cloud = Arc::new(cloud::Cloud::new(store)?);
        fs::create_dir(root.as_ref()).map_err(storage_error)?;
        let root = fs::canonicalize(root.as_ref()).map_err(storage_error)?;
        // The creation marker also keeps legacy/default engines from adopting
        // an interrupted overlay initialization as a fresh normal database.
        let mut marker = FileOpenOptions::new()
            .create_new(true)
            .write(true)
            .open(root.join("overlay.mode"))
            .map_err(storage_error)?;
        marker
            .write_all(b"briskdb-s3-overlay-v1\n")
            .map_err(storage_error)?;
        marker.sync_all().map_err(storage_error)?;
        File::open(&root)
            .and_then(|f| f.sync_all())
            .map_err(storage_error)?;
        if let Some(parent) = root.parent() {
            File::open(parent)
                .and_then(|f| f.sync_all())
                .map_err(storage_error)?;
        }
        for (table_number, table) in config.tables.iter().enumerate() {
            let mut partitions: BTreeMap<u16, Vec<Row>> = BTreeMap::new();
            for row in seed.get(&table.name).into_iter().flatten() {
                partitions
                    .entry(config.partition(&row[table.routing_column()])?)
                    .or_default()
                    .push(row.clone());
            }
            for partition in 0..config.partitions {
                let base = match partitions.get(&partition) {
                    Some(rows) => Some(create_base(&root, &config, table_number, partition, rows)?),
                    None => None,
                };
                let head = registry::Head::new(&config, table_number, partition, base);
                cloud
                    .put(
                        &registry::head_key(&config, table_number, partition),
                        Bytes::from(serde_json::to_vec(&head).map_err(storage_error)?),
                        PutMode::Create,
                    )
                    .map_err(storage_error)?;
            }
        }
        metadata::create(&root, &config)?;
        Self::connect(root, config, cloud)
    }

    pub fn create_s3(
        root: impl AsRef<Path>,
        config: Config,
        seed: BTreeMap<String, Vec<Row>>,
    ) -> Result<Self> {
        config.validate()?;
        let store = s3_store(&config.bucket, &config.region)?;
        Self::create(root, config, store, seed)
    }

    pub fn open(root: impl AsRef<Path>, store: Arc<dyn ObjectStore>) -> Result<Self> {
        let root = fs::canonicalize(root).map_err(storage_error)?;
        let config = metadata::open(&root)?;
        Self::connect(root, config, Arc::new(cloud::Cloud::new(store)?))
    }

    pub fn open_s3(root: impl AsRef<Path>) -> Result<Self> {
        Self::open_s3_with_options(root, OpenOptions::default())
    }

    pub fn open_s3_with_options(root: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let root = fs::canonicalize(root).map_err(storage_error)?;
        let config = metadata::open(&root)?;
        let cloud = Arc::new(cloud::Cloud::new(s3_store(
            &config.bucket,
            &config.region,
        )?)?);
        let mut database = Self::connect(root, config, cloud)?;
        database.apply_options(options);
        Ok(database)
    }

    pub fn open_with_options(
        root: impl AsRef<Path>,
        store: Arc<dyn ObjectStore>,
        options: OpenOptions,
    ) -> Result<Self> {
        let mut database = Self::open(root, store)?;
        database.apply_options(options);
        Ok(database)
    }

    pub fn create_s3_with_options(
        root: impl AsRef<Path>,
        config: Config,
        seed: BTreeMap<String, Vec<Row>>,
        options: OpenOptions,
    ) -> Result<Self> {
        if options.read_only {
            return Err(invalid("read-only overlay cannot create a database"));
        }
        let mut database = Self::create_s3(root, config, seed)?;
        database.apply_options(options);
        Ok(database)
    }

    pub fn create_with_options(
        root: impl AsRef<Path>,
        config: Config,
        store: Arc<dyn ObjectStore>,
        seed: BTreeMap<String, Vec<Row>>,
        options: OpenOptions,
    ) -> Result<Self> {
        if options.read_only {
            return Err(invalid("read-only overlay cannot create a database"));
        }
        let mut database = Self::create(root, config, store, seed)?;
        database.apply_options(options);
        Ok(database)
    }

    fn connect(root: PathBuf, config: Config, cloud: Arc<cloud::Cloud>) -> Result<Self> {
        let registry = Arc::new(registry::Registry {
            root,
            config,
            cloud,
            state: Mutex::new(Default::default()),
            pruning: std::sync::atomic::AtomicBool::new(true),
        });
        let connection = Connection::open_in_memory().map_err(storage_error)?;
        connection
            .pragma_update(None, "trusted_schema", "OFF")
            .map_err(storage_error)?;
        connection
            .pragma_update(None, "temp_store", "MEMORY")
            .map_err(storage_error)?;
        connection
            .create_module(
                "brisk_s3",
                rusqlite::vtab::update_module::<vtab::OverlayTable>(),
                Some(Arc::clone(&registry)),
            )
            .map_err(storage_error)?;
        for (i, table) in registry.config.tables.iter().enumerate() {
            connection
                .execute_batch(&format!(
                    "CREATE VIRTUAL TABLE {} USING brisk_s3({i})",
                    schema::quote(&table.name)
                ))
                .map_err(storage_error)?;
        }
        let tables = registry
            .config
            .tables
            .iter()
            .map(|t| t.name.clone())
            .collect::<HashSet<_>>();
        connection
            .authorizer(Some(move |context: AuthContext<'_>| match context.action {
                AuthAction::Select | AuthAction::Recursive => Authorization::Allow,
                AuthAction::Function { function_name }
                    if !matches!(
                        function_name.to_ascii_lowercase().as_str(),
                        "load_extension" | "readfile" | "writefile"
                    ) =>
                {
                    Authorization::Allow
                }
                AuthAction::Read { table_name, .. }
                | AuthAction::Insert { table_name }
                | AuthAction::Delete { table_name }
                | AuthAction::Update { table_name, .. }
                    if context.database_name == Some("main") && tables.contains(table_name) =>
                {
                    Authorization::Allow
                }
                _ => Authorization::Deny,
            }))
            .map_err(storage_error)?;
        let progress = Arc::clone(&registry);
        connection
            .progress_handler(
                10_000,
                Some(move || progress.state.lock().map_or(true, |state| state.expired())),
            )
            .map_err(storage_error)?;
        Ok(Self {
            connection,
            registry,
            last_read_stats: ReadStats::default(),
            options: OpenOptions::default(),
        })
    }

    pub fn config(&self) -> &Config {
        &self.registry.config
    }

    /// Advisory primary-key file pruning. Disable for diagnosis/comparison;
    /// normal BriskDB databases and the optional DuckDB reader are unaffected.
    pub fn set_parquet_pruning(&mut self, enabled: bool) {
        self.registry
            .pruning
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
        self.options.parquet_pruning = enabled;
    }

    pub fn options(&self) -> OpenOptions {
        self.options
    }

    fn apply_options(&mut self, options: OpenOptions) {
        self.options = options;
        self.set_parquet_pruning(options.parquet_pruning);
    }

    fn require_writable(&self) -> Result<()> {
        if self.options.read_only {
            return Err(EngineError::new(
                EngineErrorKind::ReadOnly,
                "overlay was opened read-only",
            ));
        }
        Ok(())
    }

    pub fn read_stats(&self) -> &ReadStats {
        &self.last_read_stats
    }

    fn finish_statement(&mut self) -> Result<()> {
        self.last_read_stats = self
            .registry
            .state
            .lock()
            .map_err(|_| corrupt("overlay request state poisoned"))?
            .stats
            .clone();
        self.registry.reset(false)
    }

    /// Materialized SQL reads, including joins across base and pending rows.
    pub fn query(&mut self, sql: &str, params: &[Cell]) -> Result<QueryResult> {
        self.registry.reset(false)?;
        let result = (|| {
            validate_statement(sql, false)?;
            let mut statement = self.connection.prepare(sql).map_err(storage_error)?;
            if !statement.readonly() {
                return Err(invalid("query accepts read-only SELECT statements"));
            }
            let columns = statement
                .column_names()
                .iter()
                .map(|v| (*v).to_owned())
                .collect::<Vec<_>>();
            let mut cursor = statement
                .query(rusqlite::params_from_iter(params))
                .map_err(storage_error)?;
            let mut rows = Vec::new();
            let mut bytes = 0;
            while let Some(row) = cursor.next().map_err(storage_error)? {
                let row = (0..columns.len())
                    .map(|i| Cell::from_sql(row.get_ref(i)?))
                    .collect::<rusqlite::Result<Row>>()
                    .map_err(storage_error)?;
                bytes += row.iter().map(Cell::size).sum::<usize>();
                if rows.len() >= MAX_ROWS || bytes > MAX_BYTES {
                    return Err(limit("query result exceeds overlay limits"));
                }
                rows.push(row);
            }
            Ok(QueryResult { columns, rows })
        })();
        self.finish_statement()?;
        result
    }

    /// Execute one atomic modifying statement. SQL is never replayed following
    /// an uncertain commit. Independent inserts can rebase a failed conditional
    /// publication only after rechecking primary-key absence on the new head.
    pub fn execute(&mut self, sql: &str, params: &[Cell]) -> Result<WriteResult> {
        self.require_writable()?;
        let insert = validate_statement(sql, true)?;
        self.registry.reset(insert)?;
        let result = (|| {
            let rows = self
                .connection
                .execute(sql, rusqlite::params_from_iter(params))
                .map_err(storage_error)?;
            self.registry.commit(rows as u64)
        })();
        self.finish_statement()?;
        result
    }

    /// Merge one partition into a NEW indexed SQLite snapshot. A conditional
    /// S3 publication switches base+pending atomically. Old files are retained
    /// so already-started readers cannot lose their snapshot.
    pub fn compact(&mut self, table: &str, partition: u16) -> Result<CompactionResult> {
        self.require_writable()?;
        let table = self
            .registry
            .config
            .tables
            .iter()
            .position(|v| v.name == table)
            .ok_or_else(|| invalid("unknown table"))?;
        if partition >= self.registry.config.partitions {
            return Err(invalid("partition out of range"));
        }
        self.registry.reset(false)?;
        self.registry.compact(table, partition)
    }

    /// Callable by a scheduler or administrative request; no background process
    /// is assumed to survive a Lambda response.
    pub fn compact_all(&mut self) -> Result<Vec<CompactionResult>> {
        self.require_writable()?;
        let mut results = Vec::new();
        for table in self
            .registry
            .config
            .tables
            .iter()
            .map(|v| v.name.clone())
            .collect::<Vec<_>>()
        {
            for partition in 0..self.registry.config.partitions {
                results.push(self.compact(&table, partition)?);
            }
        }
        Ok(results)
    }
}

fn validate_statement(sql: &str, writing: bool) -> Result<bool> {
    use sqlparser::{ast::Statement, dialect::SQLiteDialect, parser::Parser};
    let statements = Parser::parse_sql(&SQLiteDialect {}, sql).map_err(storage_error)?;
    if statements.len() != 1 {
        return Err(invalid("exactly one SQL statement is required"));
    }
    match (&statements[0], writing) {
        (Statement::Query(_), false) => Ok(false),
        (Statement::Insert(_), true) => Ok(true),
        (Statement::Update { .. } | Statement::Delete(_), true) => Ok(false),
        _ => Err(invalid(
            "overlay supports SELECT and single-partition INSERT/UPDATE/DELETE; explicit transactions/DDL are not supported",
        )),
    }
}

pub(crate) fn base_directory(
    root: &Path,
    config: &Config,
    table: usize,
    partition: u16,
) -> PathBuf {
    root.join("shards")
        .join(format!("{:04}", partition % config.shards))
        .join(&config.tables[table].name)
        .join(format!("{partition:04}"))
}

pub(crate) fn create_base(
    root: &Path,
    config: &Config,
    table: usize,
    partition: u16,
    rows: &[Row],
) -> Result<String> {
    let directory = base_directory(root, config, table, partition);
    fs::create_dir_all(&directory).map_err(storage_error)?;
    let id = nonce()?;
    let path = directory.join(format!("base-{id}.sqlite"));
    FileOpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .map_err(storage_error)?;
    let schema = &config.tables[table];
    let mut connection = Connection::open(&path).map_err(storage_error)?;
    connection
        .execute_batch(
            "PRAGMA journal_mode=DELETE; PRAGMA synchronous=EXTRA; PRAGMA trusted_schema=OFF;",
        )
        .map_err(storage_error)?;
    let transaction = connection.transaction().map_err(storage_error)?;
    transaction
        .execute_batch(&schema.create_sql())
        .map_err(storage_error)?;
    {
        let markers = vec!["?"; schema.columns.len()].join(",");
        let mut statement = transaction
            .prepare(&format!(
                "INSERT INTO {} VALUES ({markers})",
                schema::quote(&schema.name)
            ))
            .map_err(storage_error)?;
        for row in rows {
            schema.check_row(row)?;
            statement
                .execute(rusqlite::params_from_iter(row))
                .map_err(storage_error)?;
        }
    }
    for (index, columns) in schema.indexes.iter().enumerate() {
        transaction
            .execute_batch(&format!(
                "CREATE INDEX idx_{index} ON {} ({})",
                schema::quote(&schema.name),
                columns
                    .iter()
                    .map(|v| schema::quote(v))
                    .collect::<Vec<_>>()
                    .join(",")
            ))
            .map_err(storage_error)?;
    }
    transaction.commit().map_err(storage_error)?;
    let integrity: String = connection
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .map_err(storage_error)?;
    if integrity != "ok" {
        return Err(corrupt("new SQLite snapshot failed integrity check"));
    }
    connection.close().map_err(|(_, e)| storage_error(e))?;
    File::open(&path)
        .and_then(|f| f.sync_all())
        .map_err(storage_error)?;
    // Persist each newly created ancestor before S3 can publish a reference.
    let mut parent = Some(directory.as_path());
    while let Some(path) = parent {
        File::open(path)
            .and_then(|f| f.sync_all())
            .map_err(storage_error)?;
        if path == root {
            break;
        }
        parent = path.parent();
    }
    Ok(id)
}

pub(crate) fn open_base(
    root: &Path,
    config: &Config,
    table: usize,
    partition: u16,
    id: &str,
) -> Result<Connection> {
    if !is_nonce(id) {
        return Err(corrupt("invalid immutable base identity"));
    }
    let path = base_directory(root, config, table, partition).join(format!("base-{id}.sqlite"));
    let encoded = path
        .as_os_str()
        .as_encoded_bytes()
        .iter()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"/-_.~".contains(b) {
                (*b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect::<String>();
    // Safe ONLY because published base files are never changed in place and
    // neither compaction nor cleanup removes files under active readers.
    Connection::open_with_flags(
        format!("file:{encoded}?mode=ro&immutable=1"),
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(storage_error)
}
