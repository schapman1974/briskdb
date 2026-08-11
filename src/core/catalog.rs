//! Protocol-neutral logical database and table metadata.

use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};

use super::{EngineError, EngineErrorKind, EngineResult, RoutingCatalog};

pub(crate) const IDENTIFIER_ENCODING_VERSION: u32 = 1;
pub(crate) const DEFAULT_LOGICAL_DATABASE_ID: u64 = 1;
pub(crate) const DEFAULT_LOGICAL_DATABASE_NAME: &str = "default";
pub(crate) const MAX_CATALOG_IDENTIFIER_BYTES: usize = 63;
pub(crate) const MAX_LOGICAL_DATABASES: usize = 64;
pub(crate) const MAX_TABLES: usize = 4_096;

/// Stable identity of a logical database in the manifest catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogicalDatabaseId(u64);

impl LogicalDatabaseId {
    /// Construct a positive stable logical-database identity.
    pub fn new(value: u64) -> EngineResult<Self> {
        if value == 0 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "logical database IDs must be positive",
            ));
        }
        Ok(Self(value))
    }

    pub(crate) const fn from_validated(value: u64) -> Self {
        debug_assert!(value > 0);
        Self(value)
    }

    /// Return the persisted numeric identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for LogicalDatabaseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable identity of a table in the manifest catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableId(u64);

impl TableId {
    /// Construct a positive stable table identity.
    pub fn new(value: u64) -> EngineResult<Self> {
        if value == 0 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "table IDs must be positive",
            ));
        }
        Ok(Self(value))
    }

    pub(crate) const fn from_validated(value: u64) -> Self {
        debug_assert!(value > 0);
        Self(value)
    }

    /// Return the persisted numeric identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Logical database metadata loaded from the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalDatabaseMetadata {
    id: LogicalDatabaseId,
    name: String,
}

impl LogicalDatabaseMetadata {
    pub(crate) fn from_validated(id: u64, name: String) -> Self {
        debug_assert!(id > 0);
        debug_assert!(validate_catalog_identifier(&name));
        Self {
            id: LogicalDatabaseId::from_validated(id),
            name,
        }
    }

    /// Return the stable catalog identity.
    pub const fn id(&self) -> LogicalDatabaseId {
        self.id
    }

    /// Return the canonical lowercase catalog name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Versioned declared type of a non-null shard-key column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ShardKeyType {
    /// Signed 64-bit integer.
    Int64,
    /// UTF-8 text routed as exact bytes and compared with SQLite `BINARY`
    /// collation, without Unicode normalization.
    Text,
    /// Arbitrary bytes.
    Binary,
}

/// Single-column shard-key declaration for a sharded table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardKeyMetadata {
    column: String,
    key_type: ShardKeyType,
}

impl ShardKeyMetadata {
    /// Construct a validated single-column shard key.
    pub fn new(column: impl Into<String>, key_type: ShardKeyType) -> EngineResult<Self> {
        let column = column.into();
        ensure_catalog_identifier(&column)?;
        Ok(Self { column, key_type })
    }

    pub(crate) fn from_validated(column: String, key_type: ShardKeyType) -> Self {
        debug_assert!(validate_catalog_identifier(&column));
        Self { column, key_type }
    }

    /// Return the canonical lowercase column name.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Return the declared shard-key type.
    pub const fn key_type(&self) -> ShardKeyType {
        self.key_type
    }
}

/// Authoritative physical placement of a registered logical table.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TablePlacement {
    /// The schema exists on every shard and each row belongs to one key-selected
    /// owner. Every local unique key must include the shard key.
    Sharded(ShardKeyMetadata),
    /// Intended for a small lookup table replicated to every shard.
    Global,
    /// Intended for manifest-owned metadata rather than a user shard table.
    Catalog,
}

/// One validated table declaration to register in an empty logical catalog.
///
/// Registration assigns a stable table ID after sorting declarations by
/// logical database and canonical table name. The declaration itself contains
/// no physical rows and can be safely prepared before storage is opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDeclaration {
    database_id: LogicalDatabaseId,
    name: String,
    placement: TablePlacement,
}

impl TableDeclaration {
    /// Declare a key-routed table whose schema exists on every shard.
    pub fn sharded(
        database_id: LogicalDatabaseId,
        name: impl Into<String>,
        shard_key: ShardKeyMetadata,
    ) -> EngineResult<Self> {
        Self::new(database_id, name.into(), TablePlacement::Sharded(shard_key))
    }

    /// Declare a table whose explicitly replicated rows exist on every shard.
    pub fn global(database_id: LogicalDatabaseId, name: impl Into<String>) -> EngineResult<Self> {
        Self::new(database_id, name.into(), TablePlacement::Global)
    }

    /// Declare a manifest-owned table that is never an application SQL target.
    pub fn catalog(database_id: LogicalDatabaseId, name: impl Into<String>) -> EngineResult<Self> {
        Self::new(database_id, name.into(), TablePlacement::Catalog)
    }

    fn new(
        database_id: LogicalDatabaseId,
        name: String,
        placement: TablePlacement,
    ) -> EngineResult<Self> {
        ensure_catalog_identifier(&name)?;
        Ok(Self {
            database_id,
            name,
            placement,
        })
    }

    /// Return the owning logical database.
    pub const fn database_id(&self) -> LogicalDatabaseId {
        self.database_id
    }

    /// Return the canonical lowercase table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the declared physical placement.
    pub const fn placement(&self) -> &TablePlacement {
        &self.placement
    }

    pub(crate) fn into_parts(self) -> (LogicalDatabaseId, String, TablePlacement) {
        (self.database_id, self.name, self.placement)
    }
}

/// Logical table metadata loaded from the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMetadata {
    id: TableId,
    database_id: LogicalDatabaseId,
    name: String,
    placement: TablePlacement,
}

impl TableMetadata {
    pub(crate) fn from_validated(
        id: u64,
        database_id: u64,
        name: String,
        placement: TablePlacement,
    ) -> Self {
        debug_assert!(id > 0);
        debug_assert!(database_id > 0);
        debug_assert!(validate_catalog_identifier(&name));
        Self {
            id: TableId::from_validated(id),
            database_id: LogicalDatabaseId::from_validated(database_id),
            name,
            placement,
        }
    }

    /// Return the stable catalog identity.
    pub const fn id(&self) -> TableId {
        self.id
    }

    /// Return the owning logical database identity.
    pub const fn database_id(&self) -> LogicalDatabaseId {
        self.database_id
    }

    /// Return the canonical lowercase table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the table placement and any shard-key declaration.
    pub const fn placement(&self) -> &TablePlacement {
        &self.placement
    }
}

/// Stable logical schema metadata loaded atomically with routing state.
///
/// Database and table entries remain immutable for the lifetime of this view.
/// Its application-schema generation advances in place only after the durable
/// migration coordinator commits a new generation, so existing `&Catalog`
/// references observe publication without replacing the catalog allocation.
pub struct Catalog {
    identifier_encoding_version: u32,
    schema_generation: AtomicU64,
    default_database_id: LogicalDatabaseId,
    databases: Box<[LogicalDatabaseMetadata]>,
    tables: Box<[TableMetadata]>,
}

impl fmt::Debug for Catalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Catalog")
            .field(
                "identifier_encoding_version",
                &self.identifier_encoding_version,
            )
            .field("schema_generation", &self.schema_generation())
            .field("default_database_id", &self.default_database_id)
            .field("databases", &self.databases)
            .field("tables", &self.tables)
            .finish()
    }
}

impl Clone for Catalog {
    fn clone(&self) -> Self {
        Self {
            identifier_encoding_version: self.identifier_encoding_version,
            // Catalog clones retain value semantics: a clone is an independent
            // metadata snapshot at the generation observed here. Shared live
            // views use the existing Arc<CatalogSnapshot> storage boundary.
            schema_generation: AtomicU64::new(self.schema_generation()),
            default_database_id: self.default_database_id,
            databases: self.databases.clone(),
            tables: self.tables.clone(),
        }
    }
}

impl PartialEq for Catalog {
    fn eq(&self, other: &Self) -> bool {
        if std::ptr::eq(self, other) {
            return true;
        }
        self.identifier_encoding_version == other.identifier_encoding_version
            && self.schema_generation() == other.schema_generation()
            && self.default_database_id == other.default_database_id
            && self.databases == other.databases
            && self.tables == other.tables
    }
}

impl Eq for Catalog {}

impl Catalog {
    pub(crate) fn from_validated_parts(
        identifier_encoding_version: u32,
        schema_generation: u64,
        default_database_id: u64,
        databases: Box<[LogicalDatabaseMetadata]>,
        tables: Box<[TableMetadata]>,
    ) -> Self {
        debug_assert_eq!(identifier_encoding_version, IDENTIFIER_ENCODING_VERSION);
        debug_assert!(!databases.is_empty());
        debug_assert!(databases.len() <= MAX_LOGICAL_DATABASES);
        debug_assert!(tables.len() <= MAX_TABLES);
        debug_assert!(databases.windows(2).all(|rows| rows[0].id < rows[1].id));
        debug_assert!(tables.windows(2).all(|rows| {
            (rows[0].database_id, rows[0].name.as_str())
                < (rows[1].database_id, rows[1].name.as_str())
        }));
        debug_assert!(databases.iter().any(|database| {
            database.id.get() == default_database_id
                && database.name == DEFAULT_LOGICAL_DATABASE_NAME
        }));
        debug_assert!(tables.iter().all(|table| {
            databases
                .binary_search_by_key(&table.database_id, |database| database.id)
                .is_ok()
        }));
        Self {
            identifier_encoding_version,
            schema_generation: AtomicU64::new(schema_generation),
            default_database_id: LogicalDatabaseId::from_validated(default_database_id),
            databases,
            tables,
        }
    }

    /// Return the persisted identifier-encoding version.
    pub const fn identifier_encoding_version(&self) -> u32 {
        self.identifier_encoding_version
    }

    /// Return the latest durably published application-schema generation.
    pub fn schema_generation(&self) -> u64 {
        self.schema_generation.load(Ordering::Acquire)
    }

    /// Publish one committed application-schema generation in place.
    ///
    /// Re-publishing the already visible target is idempotent. Every other
    /// stale, skipped, or regressing transition is an internal coordination
    /// error: manifest validation is responsible for establishing the durable
    /// source and target before this in-memory publication occurs.
    pub(crate) fn publish_schema_generation(
        &self,
        expected_generation: u64,
        target_generation: u64,
    ) -> EngineResult<()> {
        if expected_generation.checked_add(1) != Some(target_generation) {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                format!(
                    "invalid in-memory schema-generation transition {expected_generation} -> {target_generation}"
                ),
            ));
        }

        match self.schema_generation.compare_exchange(
            expected_generation,
            target_generation,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(observed) if observed == target_generation => Ok(()),
            Err(observed) => Err(EngineError::new(
                EngineErrorKind::Internal,
                format!(
                    "cannot publish schema generation {target_generation}; in-memory catalog is at generation {observed}"
                ),
            )),
        }
    }

    /// Return the storage-default logical database.
    pub fn default_database(&self) -> &LogicalDatabaseMetadata {
        self.database_by_id(self.default_database_id)
            .expect("the validated catalog contains its default database")
    }

    /// Return logical databases in stable numeric-ID order.
    pub fn logical_databases(&self) -> &[LogicalDatabaseMetadata] {
        &self.databases
    }

    /// Return tables in logical-database-ID then canonical-name order.
    pub fn tables(&self) -> &[TableMetadata] {
        &self.tables
    }

    /// Look up a logical database by stable ID.
    pub fn database_by_id(&self, id: LogicalDatabaseId) -> Option<&LogicalDatabaseMetadata> {
        self.databases
            .binary_search_by_key(&id, |database| database.id)
            .ok()
            .map(|index| &self.databases[index])
    }

    /// Look up a logical database by canonical name.
    pub fn database(&self, name: &str) -> EngineResult<Option<&LogicalDatabaseMetadata>> {
        ensure_catalog_identifier(name)?;
        Ok(self.databases.iter().find(|database| database.name == name))
    }

    /// Look up a table by logical database and canonical table name.
    pub fn table(&self, database: &str, table: &str) -> EngineResult<Option<&TableMetadata>> {
        let Some(database) = self.database(database)? else {
            ensure_catalog_identifier(table)?;
            return Ok(None);
        };
        ensure_catalog_identifier(table)?;
        Ok(self
            .tables
            .binary_search_by(|metadata| {
                (metadata.database_id, metadata.name.as_str()).cmp(&(database.id, table))
            })
            .ok()
            .map(|index| &self.tables[index]))
    }

    /// Look up a table by stable ID.
    pub fn table_by_id(&self, id: TableId) -> Option<&TableMetadata> {
        self.tables.iter().find(|table| table.id == id)
    }
}

/// Routing and logical metadata loaded from one committed manifest snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogSnapshot {
    routing: RoutingCatalog,
    logical: Catalog,
}

impl CatalogSnapshot {
    pub(crate) fn from_validated_parts(routing: RoutingCatalog, logical: Catalog) -> Self {
        Self { routing, logical }
    }

    pub(crate) const fn routing(&self) -> &RoutingCatalog {
        &self.routing
    }

    pub(crate) const fn logical(&self) -> &Catalog {
        &self.logical
    }
}

pub(crate) fn validate_catalog_identifier(identifier: &str) -> bool {
    let bytes = identifier.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_CATALOG_IDENTIFIER_BYTES {
        return false;
    }
    if !matches!(bytes[0], b'a'..=b'z' | b'_')
        || !bytes
            .iter()
            .all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_'))
    {
        return false;
    }
    identifier != "briskdb"
        && !identifier.starts_with("briskdb_")
        && !identifier.starts_with("sqlite_")
}

fn ensure_catalog_identifier(identifier: &str) -> EngineResult<()> {
    if validate_catalog_identifier(identifier) {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "catalog identifiers must be 1 to 63 bytes of lowercase ASCII, start with a letter or underscore, and not use a reserved prefix",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;

    fn sample_catalog() -> Catalog {
        Catalog::from_validated_parts(
            1,
            7,
            1,
            vec![
                LogicalDatabaseMetadata::from_validated(1, "default".to_owned()),
                LogicalDatabaseMetadata::from_validated(9, "tenant".to_owned()),
            ]
            .into_boxed_slice(),
            vec![
                TableMetadata::from_validated(
                    3,
                    1,
                    "accounts".to_owned(),
                    TablePlacement::Sharded(ShardKeyMetadata::from_validated(
                        "tenant_id".to_owned(),
                        ShardKeyType::Text,
                    )),
                ),
                TableMetadata::from_validated(8, 1, "countries".to_owned(), TablePlacement::Global),
                TableMetadata::from_validated(
                    21,
                    9,
                    "accounts".to_owned(),
                    TablePlacement::Catalog,
                ),
            ]
            .into_boxed_slice(),
        )
    }

    #[test]
    fn identifiers_have_one_narrow_protocol_neutral_contract() {
        for valid in [
            "a",
            "_",
            "_9",
            "briskdbx",
            "sqlite",
            "tenant_42",
            &"a".repeat(63),
        ] {
            assert!(validate_catalog_identifier(valid), "{valid}");
        }
        for invalid in [
            "",
            "9tenant",
            "Tenant",
            "two-words",
            "a\0b",
            "snowman_☃",
            "briskdb",
            "briskdb_tables",
            "sqlite_master",
            &"a".repeat(64),
        ] {
            assert!(!validate_catalog_identifier(invalid), "{invalid}");
        }
    }

    #[test]
    fn table_declarations_are_validated_owned_and_database_scoped() {
        let database = LogicalDatabaseId::new(9).unwrap();
        let shard_key = ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap();
        let declaration =
            TableDeclaration::sharded(database, "accounts", shard_key.clone()).unwrap();
        assert_eq!(declaration.database_id(), database);
        assert_eq!(declaration.name(), "accounts");
        assert_eq!(declaration.placement(), &TablePlacement::Sharded(shard_key));

        assert_eq!(
            TableDeclaration::global(database, "countries")
                .unwrap()
                .placement(),
            &TablePlacement::Global
        );
        assert_eq!(
            TableDeclaration::catalog(database, "audit_catalog")
                .unwrap()
                .placement(),
            &TablePlacement::Catalog
        );

        for invalid in ["", "Accounts", "two-words", "briskdb_tables"] {
            assert_eq!(
                TableDeclaration::global(database, invalid)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::InvalidArgument
            );
            assert_eq!(
                ShardKeyMetadata::new(invalid, ShardKeyType::Int64)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::InvalidArgument
            );
        }
    }

    #[test]
    fn metadata_accessors_and_lookups_are_stable_and_database_scoped() {
        let catalog = sample_catalog();
        assert_eq!(LogicalDatabaseId::new(9).unwrap().get(), 9);
        assert_eq!(TableId::new(8).unwrap().get(), 8);
        assert_eq!(
            LogicalDatabaseId::new(0).unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
        assert_eq!(
            TableId::new(0).unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
        assert_eq!(catalog.identifier_encoding_version(), 1);
        assert_eq!(catalog.schema_generation(), 7);
        assert_eq!(catalog.default_database().id().get(), 1);
        assert_eq!(catalog.default_database().name(), "default");
        assert_eq!(catalog.logical_databases().len(), 2);
        assert_eq!(catalog.tables().len(), 3);
        assert_eq!(catalog.database("missing").unwrap(), None);
        assert_eq!(catalog.database("tenant").unwrap().unwrap().id().get(), 9);
        assert_eq!(
            catalog
                .table("default", "accounts")
                .unwrap()
                .unwrap()
                .id()
                .get(),
            3
        );
        assert_eq!(
            catalog
                .table("tenant", "accounts")
                .unwrap()
                .unwrap()
                .id()
                .get(),
            21
        );
        assert_eq!(catalog.table("tenant", "missing").unwrap(), None);
        assert_eq!(
            catalog
                .table_by_id(TableId::new(8).unwrap())
                .unwrap()
                .name(),
            "countries"
        );

        let accounts = catalog.table("default", "accounts").unwrap().unwrap();
        assert_eq!(accounts.database_id().get(), 1);
        match accounts.placement() {
            TablePlacement::Sharded(shard_key) => {
                assert_eq!(shard_key.column(), "tenant_id");
                assert_eq!(shard_key.key_type(), ShardKeyType::Text);
            }
            placement => panic!("unexpected placement {placement:?}"),
        }
    }

    #[test]
    fn malformed_lookup_identifiers_are_distinct_from_unknown_names() {
        let catalog = sample_catalog();
        for invalid in ["", "Tenant", "two-words", "sqlite_master"] {
            assert_eq!(
                catalog.database(invalid).unwrap_err().kind(),
                EngineErrorKind::InvalidArgument
            );
            assert_eq!(
                catalog.table("default", invalid).unwrap_err().kind(),
                EngineErrorKind::InvalidArgument
            );
        }
        assert_eq!(catalog.database("unknown").unwrap(), None);
        assert_eq!(catalog.table("unknown", "accounts").unwrap(), None);
    }

    #[test]
    fn public_catalog_metadata_is_send_sync_and_owned() {
        fn assert_send_sync_static<T: Send + Sync + 'static>() {}
        assert_send_sync_static::<Catalog>();
        assert_send_sync_static::<LogicalDatabaseMetadata>();
        assert_send_sync_static::<TableMetadata>();
        assert_send_sync_static::<TablePlacement>();
        assert_send_sync_static::<TableDeclaration>();
        assert_send_sync_static::<ShardKeyMetadata>();
        assert_send_sync_static::<ShardKeyType>();
    }

    #[test]
    fn schema_generation_publication_is_monotonic_idempotent_and_visible_in_place() {
        let catalog = sample_catalog();
        let retained_reference = &catalog;

        catalog.publish_schema_generation(7, 8).unwrap();
        assert_eq!(retained_reference.schema_generation(), 8);
        catalog.publish_schema_generation(7, 8).unwrap();
        assert_eq!(catalog.schema_generation(), 8);

        for (source, target) in [(8, 8), (8, 7), (8, 10), (u64::MAX, 0)] {
            let error = catalog
                .publish_schema_generation(source, target)
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Internal);
            assert_eq!(catalog.schema_generation(), 8);
        }

        catalog.publish_schema_generation(7, 8).unwrap();
        assert_eq!(catalog.schema_generation(), 8);
        assert_eq!(
            catalog.publish_schema_generation(9, 10).unwrap_err().kind(),
            EngineErrorKind::Internal
        );
    }

    #[test]
    fn clone_debug_and_equality_keep_snapshot_value_semantics() {
        let catalog = sample_catalog();
        assert_eq!(catalog, catalog);
        let cloned = catalog.clone();
        assert_eq!(catalog, cloned);
        assert_eq!(format!("{catalog:?}"), format!("{cloned:?}"));

        catalog.publish_schema_generation(7, 8).unwrap();
        assert_eq!(catalog.schema_generation(), 8);
        assert_eq!(cloned.schema_generation(), 7);
        assert_ne!(catalog, cloned);
        assert!(format!("{catalog:?}").contains("schema_generation: 8"));
        assert!(format!("{cloned:?}").contains("schema_generation: 7"));
    }

    #[test]
    fn concurrent_publishers_and_readers_never_observe_a_generation_regression() {
        let catalog = Arc::new(sample_catalog());
        let publishers = 8;
        let publish_barrier = Arc::new(Barrier::new(publishers + 1));
        let mut publish_threads = Vec::new();
        for _ in 0..publishers {
            let catalog = Arc::clone(&catalog);
            let barrier = Arc::clone(&publish_barrier);
            publish_threads.push(std::thread::spawn(move || {
                barrier.wait();
                catalog.publish_schema_generation(7, 8)
            }));
        }
        publish_barrier.wait();
        for thread in publish_threads {
            thread.join().unwrap().unwrap();
        }
        assert_eq!(catalog.schema_generation(), 8);

        let readers = 8;
        let read_barrier = Arc::new(Barrier::new(readers + 1));
        let finished = Arc::new(AtomicBool::new(false));
        let mut read_threads = Vec::new();
        for _ in 0..readers {
            let catalog = Arc::clone(&catalog);
            let barrier = Arc::clone(&read_barrier);
            let finished = Arc::clone(&finished);
            read_threads.push(std::thread::spawn(move || {
                barrier.wait();
                let mut observed = catalog.schema_generation();
                while !finished.load(Ordering::Acquire) {
                    let next = catalog.schema_generation();
                    assert!(next >= observed);
                    observed = next;
                    std::hint::spin_loop();
                }
                assert!(catalog.schema_generation() >= observed);
            }));
        }

        read_barrier.wait();
        for generation in 9..=1_024 {
            catalog
                .publish_schema_generation(generation - 1, generation)
                .unwrap();
        }
        finished.store(true, Ordering::Release);
        for thread in read_threads {
            thread.join().unwrap();
        }
        assert_eq!(catalog.schema_generation(), 1_024);
    }
}
