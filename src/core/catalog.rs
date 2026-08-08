//! Protocol-neutral logical database and table metadata.

use std::fmt;

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
///
/// Manifest v4 exposes this as read-only metadata. Routed execution continues
/// to accept the caller's opaque routing-key bytes and does not yet encode keys
/// from table values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ShardKeyType {
    /// Signed 64-bit integer.
    Int64,
    /// UTF-8 text, declared without Unicode normalization.
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

/// Declared physical placement of a logical table.
///
/// These declarations are advisory in manifest v4. Schema-journal work will
/// make them authoritative for physical DDL and query planning.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TablePlacement {
    /// Intended for the same logical schema on every shard and key-routed rows.
    Sharded(ShardKeyMetadata),
    /// Intended for a small lookup table replicated to every shard.
    Global,
    /// Intended for manifest-owned metadata rather than a user shard table.
    Catalog,
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

/// Immutable logical schema metadata loaded atomically with routing state.
///
/// The manifest v4 catalog is read-only and advisory. Existing raw SQLite
/// tables are not inferred or adopted, and current execution behavior remains
/// unchanged until cataloged DDL and schema journaling are implemented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    identifier_encoding_version: u32,
    schema_generation: u64,
    default_database_id: LogicalDatabaseId,
    databases: Box<[LogicalDatabaseMetadata]>,
    tables: Box<[TableMetadata]>,
}

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
            schema_generation,
            default_database_id: LogicalDatabaseId::from_validated(default_database_id),
            databases,
            tables,
        }
    }

    /// Return the persisted identifier-encoding version.
    pub const fn identifier_encoding_version(&self) -> u32 {
        self.identifier_encoding_version
    }

    /// Return the cataloged application-schema generation.
    pub const fn schema_generation(&self) -> u64 {
        self.schema_generation
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
        assert_send_sync_static::<ShardKeyMetadata>();
        assert_send_sync_static::<ShardKeyType>();
    }
}
