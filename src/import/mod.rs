//! Offline import of one standard SQLite database into a new BriskDB layout.

mod copy;
mod schema;
mod staging;

use std::{
    fmt,
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Write},
    path::Path,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::{
    core::{CancellationToken, Database, EngineError, EngineErrorKind, EngineResult},
    storage::{MAX_SCHEMA_MIGRATION_SQL_BYTES, Storage},
};

/// The only import-plan format understood by this release.
pub const SQLITE_IMPORT_PLAN_VERSION: u32 = 1;

/// Maximum accepted serialized import-plan size.
pub const MAX_SQLITE_IMPORT_PLAN_BYTES: usize = 1_048_576;

/// Maximum SQLite byte length of one imported row or individual TEXT/BLOB
/// value. The finite limit prevents one source cell from exhausting the
/// importer process before it can publish or clean staging.
pub const MAX_SQLITE_IMPORT_ROW_BYTES: i32 = 64 * 1024 * 1024;

/// Durable receipt format written inside a successfully imported layout.
pub const SQLITE_IMPORT_RECEIPT_VERSION: u32 = 2;

/// Declared storage type of a sharded source column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqliteImportKeyType {
    /// A SQLite integer stored losslessly as a signed 64-bit value.
    Int64,
    /// Valid UTF-8 SQLite text compared with `BINARY` collation.
    Text,
    /// An exact SQLite blob.
    Binary,
}

/// How an importer chooses a Sharded table's required key column and type.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case", deny_unknown_fields)]
pub enum SqliteShardKeyPlan {
    /// Use the table's sole primary-key column when its schema satisfies every
    /// authoritative-key rule. Composite or nullable legacy keys are rejected.
    #[default]
    PrimaryKey,
    /// Use one explicitly named column and declared storage type.
    Column {
        /// Exact canonical lowercase source column name.
        column: String,
        /// Required runtime SQLite storage type.
        key_type: SqliteImportKeyType,
    },
}

/// Explicit row-placement policy for one source table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "placement", rename_all = "snake_case", deny_unknown_fields)]
pub enum SqliteImportPlacement {
    /// Store every row on exactly one owner selected from its typed key.
    Sharded {
        /// The omitted JSON field uses the sole valid primary key.
        #[serde(default)]
        shard_key: SqliteShardKeyPlan,
    },
    /// Replicate every row to every shard by explicit request.
    Global,
}

/// Policy for source foreign-key clauses, which authoritative catalogs do not
/// yet support.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqliteForeignKeyPolicy {
    /// Reject the table before staging is created.
    #[default]
    Reject,
    /// Omit its foreign-key clauses from the staged schema and record each one
    /// in the durable import receipt and returned report.
    Omit,
}

/// Complete explicit import declaration for one source table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteTableImportPlan {
    name: String,
    placement: SqliteImportPlacement,
    foreign_keys: SqliteForeignKeyPolicy,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SqliteImportPlacementName {
    Sharded,
    Global,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SqliteTableImportPlanWire {
    name: String,
    placement: SqliteImportPlacementName,
    #[serde(default)]
    shard_key: Option<SqliteShardKeyPlan>,
    #[serde(default)]
    foreign_keys: SqliteForeignKeyPolicy,
}

#[derive(Serialize)]
struct SqliteTableImportPlanRef<'a> {
    name: &'a str,
    placement: SqliteImportPlacementName,
    #[serde(skip_serializing_if = "Option::is_none")]
    shard_key: Option<&'a SqliteShardKeyPlan>,
    #[serde(skip_serializing_if = "foreign_key_policy_is_reject")]
    foreign_keys: SqliteForeignKeyPolicy,
}

const fn foreign_key_policy_is_reject(policy: &SqliteForeignKeyPolicy) -> bool {
    matches!(policy, SqliteForeignKeyPolicy::Reject)
}

impl<'de> Deserialize<'de> for SqliteTableImportPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SqliteTableImportPlanWire::deserialize(deserializer)?;
        let placement = match (wire.placement, wire.shard_key) {
            (SqliteImportPlacementName::Sharded, shard_key) => SqliteImportPlacement::Sharded {
                shard_key: shard_key.unwrap_or_default(),
            },
            (SqliteImportPlacementName::Global, None) => SqliteImportPlacement::Global,
            (SqliteImportPlacementName::Global, Some(_)) => {
                return Err(D::Error::custom(
                    "a Global SQLite import table cannot declare a shard_key",
                ));
            }
        };
        Ok(Self {
            name: wire.name,
            placement,
            foreign_keys: wire.foreign_keys,
        })
    }
}

impl Serialize for SqliteTableImportPlan {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let (placement, shard_key) = match &self.placement {
            SqliteImportPlacement::Sharded { shard_key } => {
                (SqliteImportPlacementName::Sharded, Some(shard_key))
            }
            SqliteImportPlacement::Global => (SqliteImportPlacementName::Global, None),
        };
        SqliteTableImportPlanRef {
            name: &self.name,
            placement,
            shard_key,
            foreign_keys: self.foreign_keys,
        }
        .serialize(serializer)
    }
}

impl SqliteTableImportPlan {
    /// Declare a table Sharded by an explicit column and storage type.
    pub fn sharded(
        name: impl Into<String>,
        column: impl Into<String>,
        key_type: SqliteImportKeyType,
    ) -> Self {
        Self {
            name: name.into(),
            placement: SqliteImportPlacement::Sharded {
                shard_key: SqliteShardKeyPlan::Column {
                    column: column.into(),
                    key_type,
                },
            },
            foreign_keys: SqliteForeignKeyPolicy::Reject,
        }
    }

    /// Declare a table Sharded by its sole physically valid primary key.
    pub fn sharded_by_primary_key(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            placement: SqliteImportPlacement::Sharded {
                shard_key: SqliteShardKeyPlan::PrimaryKey,
            },
            foreign_keys: SqliteForeignKeyPolicy::Reject,
        }
    }

    /// Declare an explicitly replicated Global table.
    pub fn global(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            placement: SqliteImportPlacement::Global,
            foreign_keys: SqliteForeignKeyPolicy::Reject,
        }
    }

    /// Replace the source foreign-key policy for this table.
    #[must_use]
    pub fn with_foreign_key_policy(mut self, policy: SqliteForeignKeyPolicy) -> Self {
        self.foreign_keys = policy;
        self
    }

    /// Return the exact declared source table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the declared row-placement policy.
    pub const fn placement(&self) -> &SqliteImportPlacement {
        &self.placement
    }

    /// Return the source foreign-key policy.
    pub const fn foreign_key_policy(&self) -> SqliteForeignKeyPolicy {
        self.foreign_keys
    }
}

/// Versioned, complete import plan for every application table in one source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteImportPlan {
    version: u32,
    tables: Vec<SqliteTableImportPlan>,
}

impl SqliteImportPlan {
    /// Construct a version-1 plan. Exact source coverage is checked during
    /// preflight against the consistent source snapshot.
    pub fn new(tables: Vec<SqliteTableImportPlan>) -> Self {
        Self {
            version: SQLITE_IMPORT_PLAN_VERSION,
            tables,
        }
    }

    /// Parse a bounded-by-input JSON plan from a file.
    pub fn from_json_file(path: impl AsRef<Path>) -> EngineResult<Self> {
        let path = path.as_ref();
        let file = File::open(path).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::FailedPrecondition,
                format!("failed to open SQLite import plan {}", path.display()),
                error,
            )
        })?;
        let mut bytes = Vec::new();
        file.take(u64::try_from(MAX_SQLITE_IMPORT_PLAN_BYTES + 1).expect("plan limit fits in u64"))
            .read_to_end(&mut bytes)
            .map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::FailedPrecondition,
                    format!("failed to read SQLite import plan {}", path.display()),
                    error,
                )
            })?;
        if bytes.len() > MAX_SQLITE_IMPORT_PLAN_BYTES {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!("SQLite import plan exceeds its {MAX_SQLITE_IMPORT_PLAN_BYTES}-byte limit"),
            ));
        }
        let plan: Self = serde_json::from_slice(&bytes).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::InvalidArgument,
                "SQLite import plan is not valid versioned JSON",
                error,
            )
        })?;
        if plan.version != SQLITE_IMPORT_PLAN_VERSION {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "SQLite import plan version {} is unsupported; expected {}",
                    plan.version, SQLITE_IMPORT_PLAN_VERSION
                ),
            ));
        }
        Ok(plan)
    }

    /// Return the serialized plan format version.
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Return declarations in their source file order.
    pub fn tables(&self) -> &[SqliteTableImportPlan] {
        &self.tables
    }
}

/// Runtime controls for one synchronous offline import.
#[derive(Clone)]
pub struct SqliteImportOptions {
    shard_count: u16,
    cancellation: CancellationToken,
}

impl SqliteImportOptions {
    /// Create options for a new layout with the requested fixed shard count.
    pub fn new(shard_count: u16) -> EngineResult<Self> {
        crate::storage::validate_shard_count(shard_count)?;
        Ok(Self {
            shard_count,
            cancellation: CancellationToken::new(),
        })
    }

    /// Replace the sticky cancellation signal observed before publication.
    #[must_use]
    pub fn with_cancellation_token(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    /// Return the target layout's fixed shard count.
    pub const fn shard_count(&self) -> u16 {
        self.shard_count
    }

    /// Return a clone of the import cancellation signal.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

impl fmt::Debug for SqliteImportOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteImportOptions")
            .field("shard_count", &self.shard_count)
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish()
    }
}

/// One foreign-key clause deliberately omitted by an explicit import plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmittedForeignKey {
    /// Source child table.
    pub table: String,
    /// Ordered source child columns.
    pub columns: Vec<String>,
    /// Referenced source table, even when it does not exist.
    pub referenced_table: String,
    /// Ordered referenced columns.
    pub referenced_columns: Vec<String>,
    /// Source `ON UPDATE` action.
    pub on_update: String,
    /// Source `ON DELETE` action.
    pub on_delete: String,
}

/// Verified row cardinality for one imported logical table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteImportTableReport {
    /// Canonical logical table name.
    pub table: String,
    /// Committed placement.
    pub placement: SqliteImportPlacement,
    /// Rows in the consistent source snapshot.
    pub source_rows: u64,
    /// Rows stored on each physical shard in ascending shard order.
    pub physical_rows: Vec<u64>,
    /// Lowercase BLAKE3 digest of the verified logical SQLite row multiset.
    pub logical_contents_blake3: String,
    /// Exact source `sqlite_sequence` high-water mark, when one exists.
    pub sqlite_sequence: Option<i64>,
}

/// Successful, fully validated SQLite import result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteImportReport {
    /// Durable receipt format version.
    pub receipt_version: u32,
    /// Fixed physical shard count.
    pub shard_count: u16,
    /// Persisted routing hash implementation version.
    pub hash_version: u32,
    /// Persisted canonical shard-key encoding version.
    pub key_encoding_version: u32,
    /// Persisted virtual-bucket assignment algorithm version.
    pub bucket_algorithm_version: u32,
    /// Persisted virtual-bucket map generation used by the import.
    pub map_generation: u64,
    /// Lowercase BLAKE3 digest of the source application-schema snapshot.
    pub source_schema_blake3: String,
    /// Lowercase BLAKE3 digest of the normalized explicit plan.
    pub plan_blake3: String,
    /// Per-table verified logical and physical row counts.
    pub tables: Vec<SqliteImportTableReport>,
    /// Explicit schema normalizations, empty for a lossless-schema import.
    pub omitted_foreign_keys: Vec<OmittedForeignKey>,
}

const IMPORT_RECEIPT_FILE: &str = "briskdb-import-receipt.json";

/// Import one consistent standard-SQLite snapshot into a new BriskDB layout.
///
/// All source and plan validation happens before staging. The destination must
/// not exist and is published only after row, routing, schema, integrity,
/// sequence, and normal-reopen verification succeeds.
pub fn import_sqlite_database(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    plan: &SqliteImportPlan,
    options: SqliteImportOptions,
) -> EngineResult<SqliteImportReport> {
    import_sqlite_database_inner(
        source.as_ref(),
        destination.as_ref(),
        plan,
        options,
        ImportFault::None,
    )
}

#[derive(Debug, Clone, Copy)]
enum ImportFault {
    None,
    #[cfg(test)]
    FailAfterShardCommits(usize),
}

impl ImportFault {
    fn after_shard_commit(self, committed: usize) -> EngineResult<()> {
        #[cfg(test)]
        if matches!(self, Self::FailAfterShardCommits(expected) if expected == committed) {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                format!("injected import failure after {committed} target shard commits"),
            ));
        }
        #[cfg(not(test))]
        let _ = (self, committed);
        Ok(())
    }
}

fn import_sqlite_database_inner(
    source: &Path,
    destination: &Path,
    plan: &SqliteImportPlan,
    options: SqliteImportOptions,
    fault: ImportFault,
) -> EngineResult<SqliteImportReport> {
    let cancellation = options.cancellation_token();
    copy::ensure_not_cancelled(&cancellation)?;

    let snapshot = schema::SourceSnapshot::open_with_cancellation(source, plan, &cancellation)?;
    let schema_batches = snapshot.schema_batches(MAX_SCHEMA_MIGRATION_SQL_BYTES)?;
    let source_schema_blake3 = hex_digest(snapshot.schema_digest());
    let plan_blake3 = hex_digest(normalized_plan_digest(plan)?);
    copy::ensure_not_cancelled(&cancellation)?;

    let mut staging = staging::StagingLayout::create(source, destination)?;
    let mut database = Database::open(staging.path(), options.shard_count())?;
    for batch in schema_batches {
        copy::ensure_not_cancelled(&cancellation)?;
        database.broadcast(&batch)?;
    }
    let declarations = snapshot.table_declarations(database.catalog().default_database().id())?;
    copy::ensure_not_cancelled(&cancellation)?;
    database.register_tables(declarations)?;
    drop(database);

    let table_reports = {
        let storage = Storage::open(staging.path(), options.shard_count())?;
        copy::copy_and_verify(&snapshot, &storage, &cancellation, fault)?
    };

    // Exercise the exact ordinary startup path before the directory can gain
    // its final name. Copy verification has already checked every row.
    let reopened = Database::open(staging.path(), options.shard_count())?;
    if reopened.catalog().tables().len() != snapshot.tables().len() {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "reopened SQLite import catalog lost a declared table",
        ));
    }
    let (hash_version, key_encoding_version, bucket_algorithm_version, map_generation) =
        reopened.routing_provenance();
    drop(reopened);

    let report = SqliteImportReport {
        receipt_version: SQLITE_IMPORT_RECEIPT_VERSION,
        shard_count: options.shard_count(),
        hash_version,
        key_encoding_version,
        bucket_algorithm_version,
        map_generation,
        source_schema_blake3,
        plan_blake3,
        tables: table_reports,
        omitted_foreign_keys: snapshot.omitted_foreign_keys().to_vec(),
    };
    write_receipt(staging.path(), &report)?;
    staging.sync_layout(options.shard_count(), &cancellation)?;
    staging.publish(&cancellation)?;
    Ok(report)
}

fn normalized_plan_digest(plan: &SqliteImportPlan) -> EngineResult<[u8; 32]> {
    let mut normalized = plan.clone();
    normalized
        .tables
        .sort_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
    let bytes = serde_json::to_vec(&normalized).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::Internal,
            "failed to encode normalized SQLite import plan",
            error,
        )
    })?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

fn write_receipt(root: &Path, report: &SqliteImportReport) -> EngineResult<()> {
    let path = root.join(IMPORT_RECEIPT_FILE);
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|error| {
            crate::sqlite_error::storage_io(
                error,
                format!("failed to create SQLite import receipt {}", path.display()),
            )
        })?;
    let receipt = serde_json::to_vec_pretty(report).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::Internal,
            "failed to encode SQLite import receipt",
            error,
        )
    })?;
    let mut writer = BufWriter::new(file);
    writer.write_all(&receipt).map_err(|error| {
        crate::sqlite_error::storage_io(error, "failed to write SQLite import receipt")
    })?;
    writer.write_all(b"\n").map_err(|error| {
        crate::sqlite_error::storage_io(error, "failed to finish SQLite import receipt")
    })?;
    writer.flush().map_err(|error| {
        crate::sqlite_error::storage_io(error, "failed to flush SQLite import receipt")
    })?;
    writer.get_ref().sync_all().map_err(|error| {
        crate::sqlite_error::storage_io(error, "failed to synchronize SQLite import receipt")
    })
}

fn hex_digest(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_data_plan_is_complete_versioned_and_explicit() {
        let plan: SqliteImportPlan =
            serde_json::from_str(include_str!("../../examples/LARGE_Data.import.json")).unwrap();
        assert_eq!(plan.version(), SQLITE_IMPORT_PLAN_VERSION);
        assert_eq!(plan.tables().len(), 31);
        assert_eq!(
            plan.tables()
                .iter()
                .filter(|table| matches!(table.placement(), SqliteImportPlacement::Sharded { .. }))
                .count(),
            21
        );
        assert_eq!(
            plan.tables()
                .iter()
                .filter(|table| matches!(table.placement(), SqliteImportPlacement::Global))
                .count(),
            10
        );
        assert_eq!(
            plan.tables()
                .iter()
                .filter(|table| { table.foreign_key_policy() == SqliteForeignKeyPolicy::Omit })
                .count(),
            5
        );
        let accounts = plan
            .tables()
            .iter()
            .find(|table| table.name() == "cb_accounts")
            .unwrap();
        assert!(matches!(
            accounts.placement(),
            SqliteImportPlacement::Sharded {
                shard_key: SqliteShardKeyPlan::Column {
                    column,
                    key_type: SqliteImportKeyType::Int64,
                }
            } if column == "id"
        ));
    }

    #[test]
    fn sharded_json_defaults_only_the_key_strategy_and_rejects_unknown_fields() {
        let table: SqliteTableImportPlan =
            serde_json::from_str(r#"{"name":"events","placement":"sharded"}"#).unwrap();
        assert_eq!(
            table.placement(),
            &SqliteImportPlacement::Sharded {
                shard_key: SqliteShardKeyPlan::PrimaryKey
            }
        );

        assert!(
            serde_json::from_str::<SqliteTableImportPlan>(
                r#"{"name":"events","placement":"global","guess_small":true}"#
            )
            .is_err()
        );
    }

    #[test]
    fn import_options_validate_shards_and_redact_no_hidden_state() {
        assert_eq!(
            SqliteImportOptions::new(1).unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
        let options = SqliteImportOptions::new(2).unwrap();
        assert_eq!(options.shard_count(), 2);
        assert!(!options.cancellation_token().is_cancelled());
        assert_eq!(
            format!("{options:?}"),
            "SqliteImportOptions { shard_count: 2, cancelled: false }"
        );
    }

    #[test]
    fn failure_after_one_private_shard_commit_cleans_stage_and_retry_succeeds() {
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source.sqlite");
        let destination = temporary.path().join("imported");
        let connection = rusqlite::Connection::open(&source).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE records(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO records VALUES (1, 'one'), (2, 'two'), (3, 'three');",
            )
            .unwrap();
        connection.close().unwrap();
        let source_before = std::fs::read(&source).unwrap();
        let plan = SqliteImportPlan::new(vec![SqliteTableImportPlan::sharded_by_primary_key(
            "records",
        )]);

        let error = import_sqlite_database_inner(
            &source,
            &destination,
            &plan,
            SqliteImportOptions::new(3).unwrap(),
            ImportFault::FailAfterShardCommits(1),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert!(error.diagnostic().contains("after 1 target shard commits"));
        assert!(!destination.exists());
        assert_eq!(std::fs::read(&source).unwrap(), source_before);
        let unpublished_stages = std::fs::read_dir(temporary.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name.to_string_lossy().starts_with(".briskdb-import-stage-"))
            .collect::<Vec<_>>();
        assert!(unpublished_stages.is_empty());

        let report = import_sqlite_database(
            &source,
            &destination,
            &plan,
            SqliteImportOptions::new(3).unwrap(),
        )
        .unwrap();
        assert_eq!(report.tables.len(), 1);
        assert_eq!(report.tables[0].source_rows, 3);
        assert_eq!(report.tables[0].physical_rows.iter().sum::<u64>(), 3);
        assert!(destination.join("manifest.sqlite").is_file());
    }

    #[test]
    fn import_rejects_connection_local_stored_expressions_before_publish() {
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source.sqlite");
        let destination = temporary.path().join("imported");
        let connection = rusqlite::Connection::open(&source).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE records (
                     id INTEGER PRIMARY KEY,
                     observed INTEGER DEFAULT (total_changes())
                 );
                 INSERT INTO records (id) VALUES (1);",
            )
            .unwrap();
        connection.close().unwrap();
        let plan = SqliteImportPlan::new(vec![SqliteTableImportPlan::sharded_by_primary_key(
            "records",
        )]);

        let error = import_sqlite_database(
            &source,
            &destination,
            &plan,
            SqliteImportOptions::new(2).unwrap(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(
            error
                .diagnostic()
                .contains("cannot participate in stateless catalog write reuse"),
            "{}",
            error.diagnostic()
        );
        assert!(!destination.exists());
        assert!(std::fs::read_dir(temporary.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".briskdb-import-stage-")
        }));
    }
}
