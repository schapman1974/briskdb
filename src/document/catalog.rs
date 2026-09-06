//! Versioned document namespace, collection, placement, and index metadata.

use super::{BsonDocument, BsonErrorContext, encode_document};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

/// Durable document-catalog representation understood by this release.
pub const DOCUMENT_CATALOG_VERSION: u32 = 1;
/// Physical shard-table representation understood by this release.
pub const DOCUMENT_STORAGE_FORMAT_VERSION: u32 = 1;
/// Stored BSON document schema version understood by this release.
pub const DOCUMENT_SCHEMA_VERSION: u32 = 1;
/// Built-in and declared document-index metadata representation.
pub const DOCUMENT_INDEX_FORMAT_VERSION: u32 = 1;

/// Maximum UTF-8 byte length of a logical Mongo database name.
pub const MAX_DOCUMENT_DATABASE_NAME_BYTES: usize = 63;
/// Maximum UTF-8 byte length of a full `database.collection` namespace.
pub const MAX_DOCUMENT_NAMESPACE_BYTES: usize = 255;

/// Stable identity of one logical document database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DocumentDatabaseId(u64);

impl DocumentDatabaseId {
    pub(crate) fn from_validated(value: u64) -> Self {
        debug_assert!(value > 0);
        Self(value)
    }

    /// Return the durable positive integer identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Stable identity of one logical document collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DocumentCollectionId(u64);

impl DocumentCollectionId {
    pub(crate) fn from_validated(value: u64) -> Self {
        debug_assert!(value > 0);
        Self(value)
    }

    /// Return the durable positive integer identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Immutable placement rule for documents in one collection.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DocumentPlacement {
    /// Route the canonical BSON identity of `_id` through BriskDB's persisted
    /// hash/bucket map. Equal IDs therefore always reach the same shard.
    HashByIdV1,
}

impl DocumentPlacement {
    /// Return the stable persisted policy code.
    pub const fn code(self) -> u32 {
        match self {
            Self::HashByIdV1 => 1,
        }
    }

    /// Return the version of this placement algorithm.
    pub const fn version(self) -> u32 {
        match self {
            Self::HashByIdV1 => 1,
        }
    }
}

/// Whether an index definition already has authoritative physical coverage.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DocumentIndexLifecycle {
    /// The index is authoritative for writes and reads.
    Ready,
    /// Metadata is retained for a later crash-safe physical build.
    PendingBuild,
}

impl DocumentIndexLifecycle {
    pub(crate) fn from_code(code: u32) -> EngineResult<Self> {
        match code {
            1 => Ok(Self::Ready),
            2 => Ok(Self::PendingBuild),
            _ => Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "document index has an unsupported lifecycle",
            )),
        }
    }
}

/// Exact persisted options supplied when a collection is declared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentCollectionOptions {
    document: BsonDocument,
}

impl DocumentCollectionOptions {
    /// Construct an empty option document.
    pub const fn empty() -> Self {
        Self {
            document: BsonDocument::new(),
        }
    }

    /// Validate and retain an ordered BSON option document.
    pub fn new(document: BsonDocument) -> EngineResult<Self> {
        encode_document(&document)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        Ok(Self { document })
    }

    /// Return the ordered option document.
    pub const fn document(&self) -> &BsonDocument {
        &self.document
    }
}

impl Default for DocumentCollectionOptions {
    fn default() -> Self {
        Self::empty()
    }
}

/// Durable metadata for one document index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentIndexMetadata {
    name: String,
    specification: BsonDocument,
    unique: bool,
    built_in: bool,
    lifecycle: DocumentIndexLifecycle,
}

impl DocumentIndexMetadata {
    pub(crate) fn from_validated_parts(
        name: String,
        specification: BsonDocument,
        unique: bool,
        built_in: bool,
        lifecycle: DocumentIndexLifecycle,
    ) -> Self {
        Self {
            name,
            specification,
            unique,
            built_in,
            lifecycle,
        }
    }

    /// Return the exact index name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the ordered BSON index specification.
    pub const fn specification(&self) -> &BsonDocument {
        &self.specification
    }

    /// Return whether duplicate semantic keys are forbidden.
    pub const fn is_unique(&self) -> bool {
        self.unique
    }

    /// Return whether this is storage's mandatory `_id_` index.
    pub const fn is_built_in(&self) -> bool {
        self.built_in
    }

    /// Return the physical-build lifecycle.
    pub const fn lifecycle(&self) -> DocumentIndexLifecycle {
        self.lifecycle
    }
}

/// Durable metadata for one logical collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentCollectionMetadata {
    id: DocumentCollectionId,
    database_id: DocumentDatabaseId,
    database_name: String,
    name: String,
    options: DocumentCollectionOptions,
    placement: DocumentPlacement,
    indexes: Box<[DocumentIndexMetadata]>,
}

impl DocumentCollectionMetadata {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_validated_parts(
        id: DocumentCollectionId,
        database_id: DocumentDatabaseId,
        database_name: String,
        name: String,
        options: DocumentCollectionOptions,
        placement: DocumentPlacement,
        indexes: Box<[DocumentIndexMetadata]>,
    ) -> Self {
        Self {
            id,
            database_id,
            database_name,
            name,
            options,
            placement,
            indexes,
        }
    }

    pub const fn id(&self) -> DocumentCollectionId {
        self.id
    }

    pub const fn database_id(&self) -> DocumentDatabaseId {
        self.database_id
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn options(&self) -> &DocumentCollectionOptions {
        &self.options
    }

    pub const fn placement(&self) -> DocumentPlacement {
        self.placement
    }

    pub fn indexes(&self) -> &[DocumentIndexMetadata] {
        &self.indexes
    }

    /// Return the full Mongo namespace without normalizing either component.
    pub fn namespace(&self) -> String {
        format!("{}.{}", self.database_name, self.name)
    }
}

/// Validated document-catalog snapshot in stable ID order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DocumentCatalog {
    collections: Box<[DocumentCollectionMetadata]>,
}

impl DocumentCatalog {
    pub(crate) fn from_validated_collections(
        collections: Box<[DocumentCollectionMetadata]>,
    ) -> Self {
        Self { collections }
    }

    pub fn collections(&self) -> &[DocumentCollectionMetadata] {
        &self.collections
    }

    pub fn collection(
        &self,
        database: &str,
        collection: &str,
    ) -> Option<&DocumentCollectionMetadata> {
        self.collections
            .iter()
            .find(|metadata| metadata.database_name == database && metadata.name == collection)
    }

    pub fn collection_by_id(
        &self,
        id: DocumentCollectionId,
    ) -> Option<&DocumentCollectionMetadata> {
        self.collections.iter().find(|metadata| metadata.id == id)
    }
}

pub(crate) fn validate_namespace(database: &str, collection: &str) -> EngineResult<()> {
    if database.is_empty()
        || database.len() > MAX_DOCUMENT_DATABASE_NAME_BYTES
        || database.contains('\0')
    {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "document database name must contain 1 to 63 UTF-8 bytes and no NUL",
        ));
    }
    let namespace_len = database
        .len()
        .checked_add(1)
        .and_then(|length| length.checked_add(collection.len()))
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                "document namespace length overflowed",
            )
        })?;
    if collection.is_empty()
        || collection.contains('\0')
        || namespace_len > MAX_DOCUMENT_NAMESPACE_BYTES
    {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "document namespace must contain at most 255 UTF-8 bytes and no NUL",
        ));
    }
    Ok(())
}
