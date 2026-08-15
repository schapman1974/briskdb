//! Durable, protocol-neutral global-index catalog metadata.

use std::fmt;

use super::{
    CanonicalIndexKey, EngineError, EngineErrorKind, EngineResult, INDEX_KEY_ENCODING_VERSION,
    IndexKeyCollation, IndexKeyOrder, IndexNullOrder, TableId, UniqueNullSemantics,
    validate_catalog_identifier,
};

pub(crate) const MAX_GLOBAL_INDEXES: usize = 4_096;
pub(crate) const MAX_GLOBAL_INDEX_PARTS: usize = 16;
pub(crate) const MAX_GLOBAL_INDEX_SQL_BYTES: usize = 4_096;

/// Version-1 comparison and migration target for hash-partitioned index storage.
pub const HASH_PARTITIONED_GLOBAL_INDEX_PARTITIONS_V1: u16 = 16;

const PARTITION_ROUTING_DOMAIN_V1: &[u8] = b"briskdb.global-index.partition.v1\0";

/// Stable identity of one durable global index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GlobalIndexId(u64);

impl GlobalIndexId {
    /// Construct a positive stable global-index identity.
    pub fn new(value: u64) -> EngineResult<Self> {
        if value == 0 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "global-index IDs must be positive",
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

impl fmt::Display for GlobalIndexId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Durable lifecycle of a global-index definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GlobalIndexLifecycle {
    /// Metadata exists, but index data must not be used by queries.
    Creating,
    /// Index data is complete and eligible for its documented use.
    Ready,
    /// Validation failed or compatibility was lost; the index is fenced.
    Invalid,
    /// Replacement index data is being constructed and is not yet published.
    Rebuilding,
    /// The definition and any physical artifacts are being removed.
    Dropping,
}

impl GlobalIndexLifecycle {
    /// Return whether one durable lifecycle transition is legal.
    pub fn can_transition_to(self, target: Self) -> bool {
        if self == target {
            return true;
        }
        matches!(
            (self, target),
            (Self::Creating, Self::Ready | Self::Invalid | Self::Dropping)
                | (
                    Self::Ready,
                    Self::Invalid | Self::Rebuilding | Self::Dropping
                )
                | (Self::Invalid, Self::Rebuilding | Self::Dropping)
                | (
                    Self::Rebuilding,
                    Self::Ready | Self::Invalid | Self::Dropping
                )
        )
    }
}

/// Durable physical-layout choice for global-index data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GlobalIndexStorageTopology {
    /// No physical layout has been selected yet.
    Unassigned,
    /// One shared global-index SQLite file.
    SharedSqliteV1,
    /// Canonical keys are hash-partitioned across multiple SQLite files.
    HashPartitionedSqliteV1 { partitions: u16 },
}

impl GlobalIndexStorageTopology {
    /// Return the selected initial topology.
    pub const fn selected_v1() -> Self {
        Self::SharedSqliteV1
    }

    /// Construct a validated version-1 hash-partitioned topology.
    pub fn hash_partitioned_sqlite_v1(partitions: u16) -> EngineResult<Self> {
        if !(2..=256).contains(&partitions) || !partitions.is_power_of_two() {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "global-index partition count must be a power of two between 2 and 256",
            ));
        }
        Ok(Self::HashPartitionedSqliteV1 { partitions })
    }

    pub(crate) fn from_validated_parts(kind: i64, version: i64, partitions: i64) -> Self {
        match (kind, version, partitions) {
            (0, 0, 0) => Self::Unassigned,
            (1, 1, 1) => Self::SharedSqliteV1,
            (2, 1, partitions) => Self::HashPartitionedSqliteV1 {
                partitions: partitions as u16,
            },
            _ => unreachable!("validated global-index topology"),
        }
    }

    pub(crate) const fn persisted_parts(self) -> (i64, i64, i64) {
        match self {
            Self::Unassigned => (0, 0, 0),
            Self::SharedSqliteV1 => (1, 1, 1),
            Self::HashPartitionedSqliteV1 { partitions } => (2, 1, partitions as i64),
        }
    }

    /// Return the number of physical index databases in this topology.
    pub const fn partition_count(self) -> u16 {
        match self {
            Self::Unassigned => 0,
            Self::SharedSqliteV1 => 1,
            Self::HashPartitionedSqliteV1 { partitions } => partitions,
        }
    }

    /// Route a canonical key to its one authoritative index partition.
    ///
    /// Version 1 hashes the stable index ID and exact canonical key bytes with
    /// a domain-separated BLAKE3 digest, then masks the low 64-bit word. The
    /// partition count is constrained to a power of two, so this mapping is
    /// deterministic on every supported architecture.
    pub fn partition_for_key(
        self,
        index_id: GlobalIndexId,
        key: &CanonicalIndexKey,
    ) -> EngineResult<u16> {
        match self {
            Self::Unassigned => Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "global-index storage topology is not assigned",
            )),
            Self::SharedSqliteV1 => Ok(0),
            Self::HashPartitionedSqliteV1 { partitions } => {
                let mut hasher = blake3::Hasher::new();
                hasher.update(PARTITION_ROUTING_DOMAIN_V1);
                hasher.update(&index_id.get().to_le_bytes());
                hasher.update(key.as_bytes());
                let word = u64::from_le_bytes(
                    hasher.finalize().as_bytes()[..size_of::<u64>()]
                        .try_into()
                        .expect("BLAKE3 digest contains one routing word"),
                );
                Ok((word & u64::from(partitions - 1)) as u16)
            }
        }
    }
}

/// Declared logical type of one encoded global-index key component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GlobalIndexKeyType {
    Boolean,
    Int64,
    UInt64,
    Float64,
    Date,
    Timestamp,
    Text,
    Binary,
}

/// Source expression for one global-index key component.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GlobalIndexKeySource {
    /// A canonical catalog column name.
    Column(String),
    /// Exact canonical SQLite expression text retained for later evaluation.
    Expression(String),
}

impl GlobalIndexKeySource {
    /// Construct a validated column source.
    pub fn column(name: impl Into<String>) -> EngineResult<Self> {
        let name = name.into();
        ensure_identifier(&name, "global-index column")?;
        Ok(Self::Column(name))
    }

    /// Construct a bounded, NUL-free expression source.
    pub fn expression(sql: impl Into<String>) -> EngineResult<Self> {
        let sql = sql.into();
        ensure_sql_fragment(&sql, "global-index expression")?;
        Ok(Self::Expression(sql))
    }

    pub(crate) fn from_validated(kind: i64, source: String) -> Self {
        match kind {
            1 => Self::Column(source),
            2 => Self::Expression(source),
            _ => unreachable!("validated global-index key source"),
        }
    }

    pub(crate) const fn kind_code(&self) -> i64 {
        match self {
            Self::Column(_) => 1,
            Self::Expression(_) => 2,
        }
    }

    /// Return the canonical column name or exact expression text.
    pub fn source(&self) -> &str {
        match self {
            Self::Column(source) | Self::Expression(source) => source,
        }
    }
}

/// Frozen definition of one component in a compound global-index key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GlobalIndexKeyPart {
    source: GlobalIndexKeySource,
    key_type: GlobalIndexKeyType,
    order: IndexKeyOrder,
    null_order: IndexNullOrder,
    collation: IndexKeyCollation,
}

impl GlobalIndexKeyPart {
    /// Construct an ascending, NULLS FIRST, BINARY key component.
    pub const fn new(source: GlobalIndexKeySource, key_type: GlobalIndexKeyType) -> Self {
        Self {
            source,
            key_type,
            order: IndexKeyOrder::Ascending,
            null_order: IndexNullOrder::First,
            collation: IndexKeyCollation::Binary,
        }
    }

    pub const fn with_order(mut self, order: IndexKeyOrder) -> Self {
        self.order = order;
        self
    }

    pub const fn with_null_order(mut self, null_order: IndexNullOrder) -> Self {
        self.null_order = null_order;
        self
    }

    pub const fn with_collation(mut self, collation: IndexKeyCollation) -> Self {
        self.collation = collation;
        self
    }

    pub const fn source(&self) -> &GlobalIndexKeySource {
        &self.source
    }

    pub const fn key_type(&self) -> GlobalIndexKeyType {
        self.key_type
    }

    pub const fn order(&self) -> IndexKeyOrder {
        self.order
    }

    pub const fn null_order(&self) -> IndexNullOrder {
        self.null_order
    }

    pub const fn collation(&self) -> IndexKeyCollation {
        self.collation
    }

    pub(crate) const fn from_validated(
        source: GlobalIndexKeySource,
        key_type: GlobalIndexKeyType,
        order: IndexKeyOrder,
        null_order: IndexNullOrder,
        collation: IndexKeyCollation,
    ) -> Self {
        Self {
            source,
            key_type,
            order,
            null_order,
            collation,
        }
    }
}

/// Validated request to add one durable global-index definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexDeclaration {
    table_id: TableId,
    name: String,
    key_parts: Box<[GlobalIndexKeyPart]>,
    unique: bool,
    null_semantics: UniqueNullSemantics,
    predicate: Option<String>,
    topology: GlobalIndexStorageTopology,
}

impl GlobalIndexDeclaration {
    /// Define a non-unique global index in the unassigned topology.
    pub fn new(
        table_id: TableId,
        name: impl Into<String>,
        key_parts: Vec<GlobalIndexKeyPart>,
    ) -> EngineResult<Self> {
        let name = name.into();
        ensure_identifier(&name, "global-index name")?;
        ensure_key_parts(&key_parts)?;
        Ok(Self {
            table_id,
            name,
            key_parts: key_parts.into_boxed_slice(),
            unique: false,
            null_semantics: UniqueNullSemantics::Distinct,
            predicate: None,
            topology: GlobalIndexStorageTopology::Unassigned,
        })
    }

    /// Mark this definition unique and freeze its NULL semantics.
    pub const fn unique(mut self, null_semantics: UniqueNullSemantics) -> Self {
        self.unique = true;
        self.null_semantics = null_semantics;
        self
    }

    /// Attach an exact bounded predicate used for a partial index.
    pub fn with_predicate(mut self, predicate: impl Into<String>) -> EngineResult<Self> {
        let predicate = predicate.into();
        ensure_sql_fragment(&predicate, "global-index predicate")?;
        self.predicate = Some(predicate);
        Ok(self)
    }

    /// Select the durable storage topology. `Ready` publication remains a
    /// separate lifecycle transition.
    pub const fn with_topology(mut self, topology: GlobalIndexStorageTopology) -> Self {
        self.topology = topology;
        self
    }

    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key_parts(&self) -> &[GlobalIndexKeyPart] {
        &self.key_parts
    }

    pub const fn is_unique(&self) -> bool {
        self.unique
    }

    pub const fn null_semantics(&self) -> UniqueNullSemantics {
        self.null_semantics
    }

    pub fn predicate(&self) -> Option<&str> {
        self.predicate.as_deref()
    }

    pub const fn topology(&self) -> GlobalIndexStorageTopology {
        self.topology
    }
}

/// Fully validated read-only global-index metadata loaded from the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexMetadata {
    id: GlobalIndexId,
    table_id: TableId,
    name: String,
    key_parts: Box<[GlobalIndexKeyPart]>,
    unique: bool,
    null_semantics: UniqueNullSemantics,
    predicate: Option<String>,
    lifecycle: GlobalIndexLifecycle,
    key_encoding_version: u32,
    schema_generation: u64,
    topology: GlobalIndexStorageTopology,
}

impl GlobalIndexMetadata {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_validated(
        id: u64,
        table_id: u64,
        name: String,
        key_parts: Box<[GlobalIndexKeyPart]>,
        unique: bool,
        null_semantics: UniqueNullSemantics,
        predicate: Option<String>,
        lifecycle: GlobalIndexLifecycle,
        schema_generation: u64,
        topology: GlobalIndexStorageTopology,
    ) -> Self {
        Self {
            id: GlobalIndexId::from_validated(id),
            table_id: TableId::from_validated(table_id),
            name,
            key_parts,
            unique,
            null_semantics,
            predicate,
            lifecycle,
            key_encoding_version: INDEX_KEY_ENCODING_VERSION,
            schema_generation,
            topology,
        }
    }

    pub const fn id(&self) -> GlobalIndexId {
        self.id
    }

    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key_parts(&self) -> &[GlobalIndexKeyPart] {
        &self.key_parts
    }

    pub const fn is_unique(&self) -> bool {
        self.unique
    }

    pub const fn null_semantics(&self) -> UniqueNullSemantics {
        self.null_semantics
    }

    pub fn predicate(&self) -> Option<&str> {
        self.predicate.as_deref()
    }

    pub const fn lifecycle(&self) -> GlobalIndexLifecycle {
        self.lifecycle
    }

    pub const fn key_encoding_version(&self) -> u32 {
        self.key_encoding_version
    }

    pub const fn schema_generation(&self) -> u64 {
        self.schema_generation
    }

    pub const fn topology(&self) -> GlobalIndexStorageTopology {
        self.topology
    }
}

fn ensure_identifier(value: &str, description: &str) -> EngineResult<()> {
    if validate_catalog_identifier(value) {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            format!("{description} must use canonical catalog spelling"),
        ))
    }
}

fn ensure_sql_fragment(value: &str, description: &str) -> EngineResult<()> {
    if value.is_empty() || value.len() > MAX_GLOBAL_INDEX_SQL_BYTES || value.as_bytes().contains(&0)
    {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            format!(
                "{description} must contain 1 through {MAX_GLOBAL_INDEX_SQL_BYTES} UTF-8 bytes without NUL"
            ),
        ));
    }
    Ok(())
}

fn ensure_key_parts(parts: &[GlobalIndexKeyPart]) -> EngineResult<()> {
    if parts.is_empty() || parts.len() > MAX_GLOBAL_INDEX_PARTS {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            format!("a global index requires 1 through {MAX_GLOBAL_INDEX_PARTS} key components"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_id() -> TableId {
        TableId::new(7).unwrap()
    }

    fn part() -> GlobalIndexKeyPart {
        GlobalIndexKeyPart::new(
            GlobalIndexKeySource::column("email").unwrap(),
            GlobalIndexKeyType::Text,
        )
    }

    #[test]
    fn lifecycle_transitions_are_explicit_and_idempotent() {
        use GlobalIndexLifecycle as State;
        assert!(State::Creating.can_transition_to(State::Ready));
        assert!(State::Ready.can_transition_to(State::Rebuilding));
        assert!(State::Rebuilding.can_transition_to(State::Invalid));
        assert!(State::Invalid.can_transition_to(State::Dropping));
        assert!(State::Dropping.can_transition_to(State::Dropping));
        assert!(!State::Dropping.can_transition_to(State::Ready));
        assert!(!State::Invalid.can_transition_to(State::Ready));
    }

    #[test]
    fn declarations_validate_identifiers_sql_and_compound_limits() {
        let declaration = GlobalIndexDeclaration::new(table_id(), "users_email", vec![part()])
            .unwrap()
            .unique(UniqueNullSemantics::NotDistinct)
            .with_predicate("active = 1")
            .unwrap()
            .with_topology(GlobalIndexStorageTopology::SharedSqliteV1);
        assert!(declaration.is_unique());
        assert_eq!(declaration.predicate(), Some("active = 1"));

        assert!(GlobalIndexDeclaration::new(table_id(), "Bad Name", vec![part()]).is_err());
        assert!(GlobalIndexDeclaration::new(table_id(), "empty", vec![]).is_err());
        assert!(GlobalIndexKeySource::expression("\0").is_err());
        assert!(GlobalIndexStorageTopology::hash_partitioned_sqlite_v1(3).is_err());
        assert!(GlobalIndexStorageTopology::hash_partitioned_sqlite_v1(16).is_ok());
    }

    #[test]
    fn partition_routing_has_frozen_cross_architecture_vectors() {
        let topology = GlobalIndexStorageTopology::HashPartitionedSqliteV1 {
            partitions: HASH_PARTITIONED_GLOBAL_INDEX_PARTITIONS_V1,
        };
        let vectors = [
            (1, "alpha", 0_u16),
            (1, "beta", 0_u16),
            (7, "alpha", 0_u16),
            (u64::MAX, "", 0_u16),
        ];
        let observed = vectors
            .into_iter()
            .map(|(id, value, _)| {
                let key = CanonicalIndexKey::encode_values(&[value.into()]).unwrap();
                topology
                    .partition_for_key(GlobalIndexId::new(id).unwrap(), &key)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(observed, [2, 5, 1, 12]);
        assert_eq!(
            topology.partition_count(),
            HASH_PARTITIONED_GLOBAL_INDEX_PARTITIONS_V1
        );
        assert_eq!(
            GlobalIndexStorageTopology::selected_v1(),
            GlobalIndexStorageTopology::SharedSqliteV1
        );
        assert!(
            GlobalIndexStorageTopology::Unassigned
                .partition_for_key(
                    GlobalIndexId::new(1).unwrap(),
                    &CanonicalIndexKey::encode_values(&["alpha".into()]).unwrap(),
                )
                .is_err()
        );
    }
}
