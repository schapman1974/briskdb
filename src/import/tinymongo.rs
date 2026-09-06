//! Strict, read-only import planning for TinyMongo v1.3 SQLite stores.
//!
//! TinyMongo has shipped three unrelated SQLite representations under the
//! same backend name: a TinyDB-compatible single JSON blob, one SQL table per
//! collection, and a manifest plus several table-native SQLite shards.  This
//! reader recognizes those representations only after the caller supplies an
//! exact logical database name and collection allowlist.  In particular, the
//! unsharded table-native format has no application id, user version, or
//! collection catalog, so discovering document collections from arbitrary SQL
//! tables would be unsafe.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt, fs,
    fs::OpenOptions,
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use rusqlite::{Connection, OpenFlags, OptionalExtension, limits::Limit, types::ValueRef};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

use super::{IMPORT_RECEIPT_FILE, MAX_SQLITE_IMPORT_ROW_BYTES, staging::StagingLayout};
use crate::{
    core::{CancellationToken, EngineError, EngineErrorKind, EngineResult},
    document::{
        BsonBinary, BsonDateTime, BsonDecimal128, BsonDocument, BsonErrorContext, BsonJavaScript,
        BsonObjectId, BsonRegex, BsonTimestamp, BsonUuid, BsonValue, CanonicalBsonKey,
        DocumentCollectionOptions, DocumentIndexLifecycle, DocumentPlacement,
        MAX_DOCUMENT_DATABASE_NAME_BYTES, MAX_DOCUMENT_NAMESPACE_BYTES, UuidRepresentation,
        encode_document,
    },
    sqlite_error,
    storage::Storage,
};

/// Frozen source format understood by this reader.
pub const TINYMONGO_IMPORT_FORMAT_VERSION: u32 = 1;
/// Durable receipt format written into a published TinyMongo import.
pub const TINYMONGO_IMPORT_RECEIPT_VERSION: u32 = 1;
/// Largest explicit collection allowlist accepted by one source plan.
pub const MAX_TINYMONGO_IMPORT_COLLECTIONS: usize = 4_096;
/// Largest total target index catalog, including one built-in index per collection.
pub const MAX_TINYMONGO_IMPORT_INDEXES: usize = 65_536;
/// Maximum UTF-8 byte length accepted for one custom index name.
pub const MAX_TINYMONGO_IMPORT_INDEX_NAME_BYTES: usize = 255;
/// Maximum documents retained by this source-only planning API.
pub const MAX_TINYMONGO_IMPORT_DOCUMENTS: usize = 1_000_000;
/// Maximum encoded BSON retained by this source-only planning API.
pub const MAX_TINYMONGO_IMPORT_BSON_BYTES: usize = 512 * 1024 * 1024;
/// Maximum aggregate BSON retained for destination catalog metadata.
pub const MAX_TINYMONGO_IMPORT_METADATA_BSON_BYTES: usize = 64 * 1024 * 1024;
/// Maximum aggregate physical-ID and order-token bytes retained during preflight.
pub const MAX_TINYMONGO_IMPORT_SOURCE_METADATA_BYTES: usize = 512 * 1024 * 1024;

const TYPE_MARKER: &str = "__tinymongo_type_v1__";
const VALUE_MARKER: &str = "value";
const LEGACY_BLOB_TABLE: &str = "tinydb";
const INDEX_CATALOG_TABLE: &str = "__tinymongo_indexes";
const SHARD_CONFIG_TABLE: &str = "__tinymongo_config";
const SHARD_COLLECTION_TABLE: &str = "__tinymongo_collections";
const SHARD_IDENTITY_TABLE: &str = "__tinymongo_shard";
const SHARD_ORDER_COLUMN: &str = "__tinymongo_order";
const SHARDED_FORMAT_VERSION: i64 = 1;
const SHARDED_HASH_ALGORITHM: &str = "physical-id-sha256-mod-v1";
const MIN_SHARD_COUNT: usize = 2;
const MAX_SHARD_COUNT: usize = 64;
const PHYSICAL_ID_PREFIX: &str = "__tinymongo_id_v2__:";
const MAX_TINYMONGO_SOURCE_TABLES: usize = MAX_TINYMONGO_IMPORT_COLLECTIONS + 3;

/// Exact TinyMongo SQLite representation found at the source path.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TinyMongoSourceVariant {
    /// Historical TinyDB root stored as JSON in `tinydb(id, data)` row 1.
    LegacySingleRow,
    /// One ordinary SQLite table per collection.
    TableNative,
    /// Version-one manifest and two to sixty-four table-native shards.
    ShardedV1,
}

/// Explicit declaration of the TinyMongo database and collections to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TinyMongoImportPlan {
    database_name: String,
    collections: Box<[String]>,
}

impl TinyMongoImportPlan {
    /// Construct a plan without inferring collection names from SQLite tables.
    pub fn new<I, S>(database_name: impl Into<String>, collections: I) -> EngineResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let database_name = database_name.into();
        validate_database_name(&database_name)?;
        let mut unique = BTreeSet::new();
        for collection in collections {
            let collection = collection.into();
            validate_collection_name(&database_name, &collection)?;
            if !unique.insert(collection.clone()) {
                return Err(invalid_argument(format!(
                    "TinyMongo import collection allowlist contains duplicate {collection:?}"
                )));
            }
            if unique.len() > MAX_TINYMONGO_IMPORT_COLLECTIONS {
                return Err(EngineError::new(
                    EngineErrorKind::LimitExceeded,
                    format!(
                        "TinyMongo import collection allowlist exceeds {MAX_TINYMONGO_IMPORT_COLLECTIONS} entries"
                    ),
                ));
            }
        }
        if unique.is_empty() {
            return Err(invalid_argument(
                "TinyMongo import requires at least one explicitly allowlisted collection",
            ));
        }
        Ok(Self {
            database_name,
            collections: unique.into_iter().collect::<Vec<_>>().into_boxed_slice(),
        })
    }

    /// Logical database name assigned to every imported collection.
    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    /// Exact, sorted allowlist; no other physical table is interpreted as data.
    pub fn collections(&self) -> &[String] {
        &self.collections
    }
}

/// Runtime controls for one atomic TinyMongo import.
#[derive(Clone)]
pub struct TinyMongoImportOptions {
    shard_count: u16,
    cancellation: CancellationToken,
}

impl TinyMongoImportOptions {
    /// Create options for a new destination with a fixed shard count.
    pub fn new(shard_count: u16) -> EngineResult<Self> {
        crate::storage::validate_shard_count(shard_count)?;
        Ok(Self {
            shard_count,
            cancellation: CancellationToken::new(),
        })
    }

    /// Replace the sticky signal checked throughout preflight and staging.
    #[must_use]
    pub fn with_cancellation_token(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    /// Return the destination's fixed physical shard count.
    pub const fn shard_count(&self) -> u16 {
        self.shard_count
    }

    /// Return a clone of the import cancellation signal.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

impl fmt::Debug for TinyMongoImportOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TinyMongoImportOptions")
            .field("shard_count", &self.shard_count)
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish()
    }
}

/// One validated user-created TinyMongo index declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TinyMongoImportIndex {
    name: String,
    specification: BsonDocument,
    unique: bool,
    source_pending: bool,
}

impl TinyMongoImportIndex {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Ordered version-two TinyMongo metadata (`v`, `name`, `key`, options).
    pub const fn specification(&self) -> &BsonDocument {
        &self.specification
    }

    pub const fn is_unique(&self) -> bool {
        self.unique
    }

    /// A source manifest may contain an interrupted, recoverable index build.
    pub const fn was_pending_in_source(&self) -> bool {
        self.source_pending
    }

    /// Source physical expression indexes are never authoritative in BriskDB.
    pub const fn target_lifecycle(&self) -> DocumentIndexLifecycle {
        DocumentIndexLifecycle::PendingBuild
    }
}

/// Documents and logical index declarations for one allowlisted collection.
#[derive(Debug, Clone)]
pub struct TinyMongoImportCollection {
    name: String,
    documents: Box<[BsonDocument]>,
    indexes: Box<[TinyMongoImportIndex]>,
}

impl TinyMongoImportCollection {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn documents(&self) -> &[BsonDocument] {
        &self.documents
    }

    pub fn indexes(&self) -> &[TinyMongoImportIndex] {
        &self.indexes
    }
}

/// Fully validated, source-only import material.
#[derive(Debug, Clone)]
pub struct TinyMongoImportSource {
    variant: TinyMongoSourceVariant,
    database_name: String,
    source_path: PathBuf,
    source_shards: usize,
    legacy_physical_ids: u64,
    encoded_bson_bytes: u64,
    collections: Box<[TinyMongoImportCollection]>,
}

impl TinyMongoImportSource {
    pub const fn variant(&self) -> TinyMongoSourceVariant {
        self.variant
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn source_path(&self) -> &Path {
        &self.source_path
    }

    pub const fn source_shards(&self) -> usize {
        self.source_shards
    }

    pub const fn legacy_physical_ids(&self) -> u64 {
        self.legacy_physical_ids
    }

    pub const fn encoded_bson_bytes(&self) -> u64 {
        self.encoded_bson_bytes
    }

    pub fn collections(&self) -> &[TinyMongoImportCollection] {
        &self.collections
    }

    pub fn report(&self) -> TinyMongoImportReport {
        TinyMongoImportReport {
            receipt_version: TINYMONGO_IMPORT_RECEIPT_VERSION,
            target_shards: None,
            variant: self.variant,
            collections: self.collections.len() as u64,
            documents: self
                .collections
                .iter()
                .map(|collection| collection.documents.len() as u64)
                .sum(),
            custom_indexes: self
                .collections
                .iter()
                .map(|collection| collection.indexes.len() as u64)
                .sum(),
            source_shards: self.source_shards as u64,
            legacy_physical_ids: self.legacy_physical_ids,
            encoded_bson_bytes: self.encoded_bson_bytes,
        }
    }
}

/// Bounded counts suitable for a reviewed destination-staging plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TinyMongoImportReport {
    receipt_version: u32,
    target_shards: Option<u16>,
    variant: TinyMongoSourceVariant,
    collections: u64,
    documents: u64,
    custom_indexes: u64,
    source_shards: u64,
    legacy_physical_ids: u64,
    encoded_bson_bytes: u64,
}

impl TinyMongoImportReport {
    pub const fn receipt_version(self) -> u32 {
        self.receipt_version
    }
    /// Destination shard count after publication; absent on source preflight.
    pub const fn target_shards(self) -> Option<u16> {
        self.target_shards
    }
    pub const fn variant(self) -> TinyMongoSourceVariant {
        self.variant
    }
    pub const fn collections(self) -> u64 {
        self.collections
    }
    pub const fn documents(self) -> u64 {
        self.documents
    }
    pub const fn custom_indexes(self) -> u64 {
        self.custom_indexes
    }
    pub const fn source_shards(self) -> u64 {
        self.source_shards
    }
    pub const fn legacy_physical_ids(self) -> u64 {
        self.legacy_physical_ids
    }
    pub const fn encoded_bson_bytes(self) -> u64 {
        self.encoded_bson_bytes
    }
}

/// Read and validate one offline TinyMongo v1.3 SQLite source.
///
/// This operation never creates, migrates, checkpoints, or repairs the source.
/// Destination staging and atomic catalog publication intentionally live at a
/// later storage boundary.
pub fn read_tinymongo_source(
    path: impl AsRef<Path>,
    plan: &TinyMongoImportPlan,
) -> EngineResult<TinyMongoImportSource> {
    read_tinymongo_source_with_cancellation(path.as_ref(), plan, &CancellationToken::new())
}

fn read_tinymongo_source_with_cancellation(
    path: &Path,
    plan: &TinyMongoImportPlan,
    cancellation: &CancellationToken,
) -> EngineResult<TinyMongoImportSource> {
    ensure_import_not_cancelled(cancellation)?;
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        crate::sqlite_error::storage_io(
            error,
            format!("TinyMongo import source is unavailable: {}", path.display()),
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(failed_precondition(
            "TinyMongo import source must not be a symbolic link",
        ));
    }
    let source = if metadata.is_file() {
        read_single_file(path, plan, cancellation)
    } else if metadata.is_dir() {
        read_sharded(path, plan, cancellation)
    } else {
        Err(failed_precondition(
            "TinyMongo import source must be a regular SQLite file or sharded directory",
        ))
    }?;
    ensure_import_not_cancelled(cancellation)?;
    validate_target_catalog_budget(&source)?;
    Ok(source)
}

/// Validate a TinyMongo source, build a private BriskDB layout, verify it by
/// ordinary reopen, and atomically publish the absent destination.
pub fn import_tinymongo_database(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    plan: &TinyMongoImportPlan,
    options: TinyMongoImportOptions,
) -> EngineResult<TinyMongoImportReport> {
    import_tinymongo_database_inner(
        source.as_ref(),
        destination.as_ref(),
        plan,
        options,
        TinyMongoImportFault::None,
    )
}

#[derive(Debug, Clone, Copy)]
enum TinyMongoImportFault {
    None,
    #[cfg(test)]
    FailAfterDocuments(usize),
}

impl TinyMongoImportFault {
    fn requires_per_document_insertion(self) -> bool {
        #[cfg(test)]
        {
            matches!(self, Self::FailAfterDocuments(_))
        }
        #[cfg(not(test))]
        {
            let _ = self;
            false
        }
    }

    fn after_document(self, inserted: usize) -> EngineResult<()> {
        #[cfg(test)]
        if matches!(self, Self::FailAfterDocuments(expected) if expected == inserted) {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                format!("injected TinyMongo import failure after {inserted} documents"),
            ));
        }
        #[cfg(not(test))]
        let _ = (self, inserted);
        Ok(())
    }
}

fn import_tinymongo_database_inner(
    source_path: &Path,
    destination: &Path,
    plan: &TinyMongoImportPlan,
    options: TinyMongoImportOptions,
    fault: TinyMongoImportFault,
) -> EngineResult<TinyMongoImportReport> {
    let cancellation = options.cancellation_token();
    ensure_import_not_cancelled(&cancellation)?;

    // This retained snapshot is also the complete preflight boundary. No
    // destination path exists until every source file, catalog, document, ID,
    // and index declaration has passed validation.
    let source = read_tinymongo_source_with_cancellation(source_path, plan, &cancellation)?;
    ensure_import_not_cancelled(&cancellation)?;

    let mut staging = StagingLayout::create(source_path, destination)?;
    let storage = Storage::open(staging.path(), options.shard_count())?;
    let mut inserted = 0_usize;
    for collection in source.collections() {
        ensure_import_not_cancelled(&cancellation)?;
        let metadata = storage.create_document_collection(
            source.database_name(),
            collection.name(),
            &DocumentCollectionOptions::empty(),
        )?;
        if fault.requires_per_document_insertion() {
            for document in collection.documents() {
                ensure_import_not_cancelled(&cancellation)?;
                storage.insert_document(metadata.id(), document)?;
                inserted = inserted.checked_add(1).ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::LimitExceeded,
                        "TinyMongo destination document count overflowed",
                    )
                })?;
                fault.after_document(inserted)?;
            }
        } else {
            storage.insert_documents(metadata.id(), collection.documents(), &cancellation)?;
            inserted = inserted
                .checked_add(collection.documents().len())
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::LimitExceeded,
                        "TinyMongo destination document count overflowed",
                    )
                })?;
        }
        for index in collection.indexes() {
            ensure_import_not_cancelled(&cancellation)?;
            storage.declare_document_index(
                metadata.id(),
                index.name(),
                index.specification(),
                index.is_unique(),
            )?;
        }
    }
    drop(storage);

    // Exercise startup validation and independently compare the catalog and
    // exact BSON representation of every record before publication.
    let reopened = Storage::open(staging.path(), options.shard_count())?;
    verify_imported_destination(&source, &reopened)?;
    drop(reopened);
    ensure_import_not_cancelled(&cancellation)?;

    let mut report = source.report();
    report.target_shards = Some(options.shard_count());
    write_tinymongo_receipt(staging.path(), &source, &report)?;
    staging.sync_layout(options.shard_count(), &cancellation)?;
    staging.publish(&cancellation)?;
    Ok(report)
}

fn validate_target_catalog_budget(source: &TinyMongoImportSource) -> EngineResult<()> {
    let custom_indexes = source
        .collections()
        .iter()
        .try_fold(0_usize, |total, collection| {
            total.checked_add(collection.indexes().len())
        })
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                "TinyMongo target index count overflowed",
            )
        })?;
    let target_indexes = custom_indexes
        .checked_add(source.collections().len())
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                "TinyMongo target index count overflowed",
            )
        })?;
    if target_indexes > MAX_TINYMONGO_IMPORT_INDEXES {
        return Err(EngineError::new(
            EngineErrorKind::LimitExceeded,
            format!(
                "TinyMongo target catalog exceeds {MAX_TINYMONGO_IMPORT_INDEXES} total indexes"
            ),
        ));
    }

    let empty_options = encode_document(&BsonDocument::new())
        .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
    let id_key = BsonDocument::from_entries([("_id", BsonValue::Int32(1))])
        .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
    let id_specification = BsonDocument::from_entries([
        ("v", BsonValue::Int32(2)),
        ("name", BsonValue::String("_id_".to_owned())),
        ("key", BsonValue::Document(id_key)),
        ("unique", BsonValue::Boolean(true)),
    ])
    .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
    let id_specification = encode_document(&id_specification)
        .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;

    let mut metadata_bytes = 0_usize;
    for collection in source.collections() {
        metadata_bytes = metadata_bytes
            .checked_add(empty_options.len())
            .and_then(|total| total.checked_add(id_specification.len()))
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::LimitExceeded,
                    "TinyMongo target metadata BSON size overflowed",
                )
            })?;
        for index in collection.indexes() {
            let bytes = encode_document(index.specification())
                .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
            metadata_bytes = metadata_bytes.checked_add(bytes.len()).ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::LimitExceeded,
                    "TinyMongo target metadata BSON size overflowed",
                )
            })?;
        }
        if metadata_bytes > MAX_TINYMONGO_IMPORT_METADATA_BSON_BYTES {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!(
                    "TinyMongo target catalog exceeds {MAX_TINYMONGO_IMPORT_METADATA_BSON_BYTES} metadata BSON bytes"
                ),
            ));
        }
    }
    Ok(())
}

fn verify_imported_destination(
    source: &TinyMongoImportSource,
    storage: &Storage,
) -> EngineResult<()> {
    let catalog = storage.document_catalog()?;
    if catalog.collections().len() != source.collections().len() {
        return Err(data_corruption(
            "reopened TinyMongo import catalog has an unexpected collection count",
        ));
    }
    let mut verified_bytes = 0_u64;
    for collection in source.collections() {
        let metadata = catalog
            .collection(source.database_name(), collection.name())
            .ok_or_else(|| {
                data_corruption(format!(
                    "reopened TinyMongo import lost collection {:?}",
                    collection.name()
                ))
            })?;
        if metadata.placement() != DocumentPlacement::HashByIdV1
            || !metadata.options().document().is_empty()
            || metadata.indexes().len() != collection.indexes().len() + 1
        {
            return Err(data_corruption(format!(
                "reopened TinyMongo collection {:?} has unexpected catalog metadata",
                collection.name()
            )));
        }
        let built_in = metadata
            .indexes()
            .iter()
            .filter(|index| index.is_built_in())
            .collect::<Vec<_>>();
        if built_in.len() != 1
            || built_in[0].name() != "_id_"
            || !built_in[0].is_unique()
            || built_in[0].lifecycle() != DocumentIndexLifecycle::Ready
        {
            return Err(data_corruption(format!(
                "reopened TinyMongo collection {:?} has invalid built-in _id metadata",
                collection.name()
            )));
        }
        for expected in collection.indexes() {
            let actual = metadata
                .indexes()
                .iter()
                .find(|index| index.name() == expected.name())
                .ok_or_else(|| {
                    data_corruption(format!(
                        "reopened TinyMongo collection {:?} lost index {:?}",
                        collection.name(),
                        expected.name()
                    ))
                })?;
            if actual.is_built_in()
                || actual.is_unique() != expected.is_unique()
                || actual.lifecycle() != DocumentIndexLifecycle::PendingBuild
                || !actual
                    .specification()
                    .representation_eq(expected.specification())
            {
                return Err(data_corruption(format!(
                    "reopened TinyMongo index {:?} changed its declaration",
                    expected.name()
                )));
            }
        }

        let expected_documents = document_representation_map(collection.documents())?;
        let restored_in_order = storage.scan_documents(metadata.id())?;
        if restored_in_order.len() != collection.documents().len() {
            return Err(data_corruption(format!(
                "reopened TinyMongo collection {:?} has an unexpected ordered document count",
                collection.name()
            )));
        }
        for (actual, expected) in restored_in_order.iter().zip(collection.documents().iter()) {
            if encode_document(actual)
                .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?
                != encode_document(expected)
                    .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?
            {
                return Err(data_corruption(format!(
                    "reopened TinyMongo collection {:?} changed natural document order",
                    collection.name()
                )));
            }
        }
        for expected in collection.documents() {
            let identifier = expected
                .get_unique("_id")
                .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?
                .ok_or_else(|| data_corruption("verified TinyMongo document has no _id"))?;
            let restored = storage
                .get_document(metadata.id(), identifier)?
                .ok_or_else(|| {
                    data_corruption(format!(
                        "reopened TinyMongo collection {:?} lost a routed document",
                        collection.name()
                    ))
                })?;
            if encode_document(&restored)
                .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?
                != encode_document(expected)
                    .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?
            {
                return Err(data_corruption(format!(
                    "reopened TinyMongo collection {:?} changed a routed document",
                    collection.name()
                )));
            }
        }
        if storage.document_count(metadata.id())?
            != u64::try_from(expected_documents.len()).expect("bounded import count fits u64")
        {
            return Err(data_corruption(format!(
                "reopened TinyMongo collection {:?} has an unexpected document count",
                collection.name()
            )));
        }
        verified_bytes = verified_bytes
            .checked_add(
                expected_documents
                    .values()
                    .try_fold(0_u64, |total, document| {
                        total.checked_add(document.len() as u64)
                    })
                    .ok_or_else(|| {
                        EngineError::new(
                            EngineErrorKind::LimitExceeded,
                            "verified TinyMongo BSON byte count overflowed",
                        )
                    })?,
            )
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::LimitExceeded,
                    "verified TinyMongo BSON byte count overflowed",
                )
            })?;
    }
    if verified_bytes != source.encoded_bson_bytes() {
        return Err(data_corruption(
            "reopened TinyMongo import has an unexpected encoded BSON byte count",
        ));
    }
    Ok(())
}

fn document_representation_map(
    documents: &[BsonDocument],
) -> EngineResult<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut result = BTreeMap::new();
    for document in documents {
        let identifier = document
            .get_unique("_id")
            .map_err(|error| {
                error.into_engine_error(crate::document::BsonErrorContext::StoredData)
            })?
            .ok_or_else(|| data_corruption("verified TinyMongo document has no _id"))?;
        let key = CanonicalBsonKey::encode(identifier).map_err(|error| {
            error.into_engine_error(crate::document::BsonErrorContext::StoredData)
        })?;
        let encoded = encode_document(document).map_err(|error| {
            error.into_engine_error(crate::document::BsonErrorContext::StoredData)
        })?;
        if result.insert(key.into_bytes(), encoded).is_some() {
            return Err(data_corruption(
                "verified TinyMongo document set has duplicate semantic IDs",
            ));
        }
    }
    Ok(result)
}

fn write_tinymongo_receipt(
    root: &Path,
    source: &TinyMongoImportSource,
    report: &TinyMongoImportReport,
) -> EngineResult<()> {
    let path = root.join(IMPORT_RECEIPT_FILE);
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|error| {
            sqlite_error::storage_io(
                error,
                format!(
                    "failed to create TinyMongo import receipt {}",
                    path.display()
                ),
            )
        })?;
    let variant = match report.variant() {
        TinyMongoSourceVariant::LegacySingleRow => "legacy_single_row",
        TinyMongoSourceVariant::TableNative => "table_native",
        TinyMongoSourceVariant::ShardedV1 => "sharded_v1",
    };
    let collections = source
        .collections()
        .iter()
        .map(|collection| {
            serde_json::json!({
                "name": collection.name(),
                "documents": collection.documents().len(),
                "custom_indexes": collection.indexes().len(),
            })
        })
        .collect::<Vec<_>>();
    let receipt = serde_json::json!({
        "receipt_version": report.receipt_version(),
        "source_format_version": TINYMONGO_IMPORT_FORMAT_VERSION,
        "source_variant": variant,
        "database_name": source.database_name(),
        "target_shards": report.target_shards(),
        "source_shards": report.source_shards(),
        "collections": collections,
        "documents": report.documents(),
        "custom_indexes": report.custom_indexes(),
        "legacy_physical_ids": report.legacy_physical_ids(),
        "encoded_bson_bytes": report.encoded_bson_bytes(),
    });
    let bytes = serde_json::to_vec_pretty(&receipt).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::Internal,
            "failed to encode TinyMongo import receipt",
            error,
        )
    })?;
    let mut writer = BufWriter::new(file);
    writer.write_all(&bytes).map_err(|error| {
        sqlite_error::storage_io(error, "failed to write TinyMongo import receipt")
    })?;
    writer.write_all(b"\n").map_err(|error| {
        sqlite_error::storage_io(error, "failed to finish TinyMongo import receipt")
    })?;
    writer.flush().map_err(|error| {
        sqlite_error::storage_io(error, "failed to flush TinyMongo import receipt")
    })?;
    writer.get_ref().sync_all().map_err(|error| {
        sqlite_error::storage_io(error, "failed to synchronize TinyMongo import receipt")
    })
}

fn ensure_import_not_cancelled(cancellation: &CancellationToken) -> EngineResult<()> {
    if cancellation.is_cancelled() {
        Err(EngineError::new(
            EngineErrorKind::Cancelled,
            "TinyMongo import was cancelled before publication",
        ))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LogicalIndex {
    name: String,
    keys: Vec<(String, i32)>,
    unique: bool,
    sparse: bool,
    partial_filter: Option<BsonDocument>,
    source_pending: bool,
}

#[derive(Debug)]
struct PendingDocument {
    document: BsonDocument,
    retained_bson_bytes: usize,
    physical_id: Option<String>,
    order_token: Option<String>,
    source_shard: usize,
    rowid: i64,
}

#[derive(Default)]
struct RetainedBudget {
    documents: usize,
    bson_bytes: usize,
    source_metadata_bytes: usize,
}

impl RetainedBudget {
    fn retain(&mut self, document: &BsonDocument) -> EngineResult<usize> {
        let bytes = encode_document(document).map_err(|error| {
            error.into_engine_error(crate::document::BsonErrorContext::StoredData)
        })?;
        self.documents = self.documents.checked_add(1).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                "TinyMongo import document count overflowed",
            )
        })?;
        self.bson_bytes = self.bson_bytes.checked_add(bytes.len()).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                "TinyMongo import retained BSON size overflowed",
            )
        })?;
        if self.documents > MAX_TINYMONGO_IMPORT_DOCUMENTS {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!(
                    "TinyMongo source exceeds {MAX_TINYMONGO_IMPORT_DOCUMENTS} retained documents"
                ),
            ));
        }
        if self.bson_bytes > MAX_TINYMONGO_IMPORT_BSON_BYTES {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!(
                    "TinyMongo source exceeds {MAX_TINYMONGO_IMPORT_BSON_BYTES} retained BSON bytes"
                ),
            ));
        }
        Ok(bytes.len())
    }

    fn replace_document_size(
        &mut self,
        previous_bytes: usize,
        document: &BsonDocument,
    ) -> EngineResult<()> {
        let bytes = encode_document(document).map_err(|error| {
            error.into_engine_error(crate::document::BsonErrorContext::StoredData)
        })?;
        let without_previous = self.bson_bytes.checked_sub(previous_bytes).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                "TinyMongo retained BSON accounting underflowed",
            )
        })?;
        let replacement_total = without_previous.checked_add(bytes.len()).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                "TinyMongo import retained BSON size overflowed",
            )
        })?;
        if replacement_total > MAX_TINYMONGO_IMPORT_BSON_BYTES {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!(
                    "TinyMongo source exceeds {MAX_TINYMONGO_IMPORT_BSON_BYTES} retained BSON bytes"
                ),
            ));
        }
        self.bson_bytes = replacement_total;
        Ok(())
    }

    fn retain_source_metadata(
        &mut self,
        lengths: impl IntoIterator<Item = usize>,
    ) -> EngineResult<()> {
        for length in lengths {
            self.source_metadata_bytes = self
                .source_metadata_bytes
                .checked_add(length)
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::LimitExceeded,
                        "TinyMongo retained source metadata size overflowed",
                    )
                })?;
        }
        if self.source_metadata_bytes > MAX_TINYMONGO_IMPORT_SOURCE_METADATA_BYTES {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!(
                    "TinyMongo source exceeds {MAX_TINYMONGO_IMPORT_SOURCE_METADATA_BYTES} retained physical metadata bytes"
                ),
            ));
        }
        Ok(())
    }
}

fn read_single_file(
    path: &Path,
    plan: &TinyMongoImportPlan,
    cancellation: &CancellationToken,
) -> EngineResult<TinyMongoImportSource> {
    let connection = open_source(path, cancellation)?;
    let tables = ordinary_tables(&connection)?;
    let legacy =
        tables.iter().any(|name| name == LEGACY_BLOB_TABLE) && is_legacy_blob_schema(&connection)?;
    if legacy {
        let unexpected: Vec<_> = tables
            .iter()
            .filter(|name| name.as_str() != LEGACY_BLOB_TABLE)
            .cloned()
            .collect();
        if !unexpected.is_empty() {
            return Err(data_corruption(format!(
                "TinyMongo SQLite source mixes legacy tinydb storage with table-native tables: {unexpected:?}"
            )));
        }
        read_legacy_blob(path, plan, &connection, cancellation)
    } else {
        read_table_native(path, plan, &connection, cancellation)
    }
}

fn read_legacy_blob(
    path: &Path,
    plan: &TinyMongoImportPlan,
    connection: &Connection,
    cancellation: &CancellationToken,
) -> EngineResult<TinyMongoImportSource> {
    ensure_import_not_cancelled(cancellation)?;
    validate_legacy_blob_schema(connection)?;
    let mut statement = connection
        .prepare("SELECT id, data FROM tinydb ORDER BY id")
        .map_err(sqlite_error::storage)?;
    let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
    let Some(row) = rows.next().map_err(sqlite_error::storage)? else {
        return Err(data_corruption(
            "TinyMongo legacy tinydb table has no row 1",
        ));
    };
    let id = row.get::<_, i64>(0).map_err(sqlite_error::storage)?;
    let payload = text_column(
        row.get_ref(1).map_err(sqlite_error::storage)?,
        "tinydb.data",
    )?
    .to_owned();
    if id != 1 {
        return Err(data_corruption(
            "TinyMongo legacy tinydb table does not begin with row id 1",
        ));
    }
    if rows.next().map_err(sqlite_error::storage)?.is_some() {
        return Err(data_corruption(
            "TinyMongo legacy tinydb table contains unexpected rows besides id 1",
        ));
    }
    drop(rows);
    drop(statement);

    let root = parse_json(&payload, "legacy tinydb row 1")?;
    let root = json_object(&root, "TinyMongo legacy root")?;
    let logical_indexes = parse_legacy_index_catalog(root, cancellation)?;
    let mut budget = RetainedBudget::default();
    let mut collections = Vec::with_capacity(plan.collections.len());
    for name in plan.collections.iter() {
        ensure_import_not_cancelled(cancellation)?;
        let raw_collection = root.get(name).ok_or_else(|| {
            data_corruption(format!(
                "allowlisted TinyMongo collection {name:?} is absent from the legacy root"
            ))
        })?;
        let raw_documents = json_object(raw_collection, "TinyMongo legacy collection")?;
        let mut documents = Vec::with_capacity(raw_documents.len());
        let mut identities = HashSet::new();
        for (entry_id, value) in raw_documents {
            ensure_import_not_cancelled(cancellation)?;
            let document = json_document(
                value,
                &format!("legacy collection {name:?} entry {entry_id:?}"),
            )?;
            validate_document_identity(name, &document, &mut identities)?;
            let _ = budget.retain(&document)?;
            documents.push(document);
        }
        collections.push(TinyMongoImportCollection {
            name: name.clone(),
            documents: documents.into_boxed_slice(),
            indexes: import_indexes(
                logical_indexes.get(name).cloned().unwrap_or_default(),
                cancellation,
            )?,
        });
    }
    Ok(TinyMongoImportSource {
        variant: TinyMongoSourceVariant::LegacySingleRow,
        database_name: plan.database_name.clone(),
        source_path: path.to_path_buf(),
        source_shards: 1,
        legacy_physical_ids: 0,
        encoded_bson_bytes: budget.bson_bytes as u64,
        collections: collections.into_boxed_slice(),
    })
}

fn read_table_native(
    path: &Path,
    plan: &TinyMongoImportPlan,
    connection: &Connection,
    cancellation: &CancellationToken,
) -> EngineResult<TinyMongoImportSource> {
    if table_exists(connection, LEGACY_BLOB_TABLE)? && is_legacy_blob_schema(connection)? {
        return Err(data_corruption(
            "TinyMongo SQLite source contains a legacy blob alongside table-native state",
        ));
    }
    let indexes = read_sql_index_catalog(connection, cancellation)?;
    let mut budget = RetainedBudget::default();
    let mut legacy_physical_ids = 0_u64;
    let mut collections = Vec::with_capacity(plan.collections.len());
    for name in plan.collections.iter() {
        ensure_import_not_cancelled(cancellation)?;
        validate_collection_schema(connection, name, false)?;
        let pending = read_collection_rows(connection, name, 0, false, &mut budget, cancellation)?;
        let (documents, legacy) =
            finish_collection(name, pending, None, &mut budget, cancellation)?;
        legacy_physical_ids = legacy_physical_ids.saturating_add(legacy);
        collections.push(TinyMongoImportCollection {
            name: name.clone(),
            documents: documents.into_boxed_slice(),
            indexes: import_indexes(indexes.get(name).cloned().unwrap_or_default(), cancellation)?,
        });
    }
    Ok(TinyMongoImportSource {
        variant: TinyMongoSourceVariant::TableNative,
        database_name: plan.database_name.clone(),
        source_path: path.to_path_buf(),
        source_shards: 1,
        legacy_physical_ids,
        encoded_bson_bytes: budget.bson_bytes as u64,
        collections: collections.into_boxed_slice(),
    })
}

fn read_sharded(
    path: &Path,
    plan: &TinyMongoImportPlan,
    cancellation: &CancellationToken,
) -> EngineResult<TinyMongoImportSource> {
    ensure_import_not_cancelled(cancellation)?;
    let manifest_path = path.join("manifest.sqlite");
    require_regular_nonsymlink(&manifest_path, "TinyMongo sharded manifest")?;
    let manifest = open_source(&manifest_path, cancellation)?;
    require_wal(&manifest_path, "TinyMongo sharded manifest")?;
    validate_manifest_schema(&manifest)?;
    let config = read_manifest_config(&manifest)?;
    validate_shard_tree(path, config.shard_count, cancellation)?;
    let collection_states = read_manifest_collections(&manifest, cancellation)?;
    if let Some((name, state)) = collection_states
        .iter()
        .find(|(_, state)| *state != "ready")
    {
        return Err(failed_precondition(format!(
            "TinyMongo collection {name:?} has partial manifest state {state:?}; recover it with pinned TinyMongo v1.3 before import"
        )));
    }
    for name in plan.collections.iter() {
        if !collection_states.contains_key(name) {
            return Err(data_corruption(format!(
                "allowlisted TinyMongo collection {name:?} is absent from the sharded manifest"
            )));
        }
    }
    let manifest_indexes = read_manifest_indexes(&manifest, cancellation)?;
    if let Some(collection) = manifest_indexes
        .keys()
        .find(|collection| !collection_states.contains_key(*collection))
    {
        return Err(data_corruption(format!(
            "TinyMongo sharded manifest has indexes for missing collection {collection:?}"
        )));
    }
    let mut shard_connections = Vec::with_capacity(config.shard_count);
    for shard in 0..config.shard_count {
        ensure_import_not_cancelled(cancellation)?;
        let shard_path = shard_path(path, shard);
        require_regular_nonsymlink(&shard_path, "TinyMongo shard")?;
        let connection = open_source(&shard_path, cancellation)?;
        require_wal(&shard_path, "TinyMongo shard")?;
        validate_shard_identity(&connection, &config, shard)?;
        let child_tables = ordinary_tables(&connection)?;
        let mut expected_tables: BTreeSet<_> = collection_states.keys().cloned().collect();
        expected_tables.insert(SHARD_IDENTITY_TABLE.to_owned());
        if table_exists(&connection, INDEX_CATALOG_TABLE)? {
            expected_tables.insert(INDEX_CATALOG_TABLE.to_owned());
        }
        if child_tables.into_iter().collect::<BTreeSet<_>>() != expected_tables {
            return Err(data_corruption(format!(
                "TinyMongo shard {shard} has an unexpected table inventory"
            )));
        }
        for name in collection_states.keys() {
            validate_collection_schema(&connection, name, true)?;
        }
        let child_indexes = read_sql_index_catalog(&connection, cancellation)?;
        if let Some(name) = child_indexes
            .keys()
            .find(|name| !collection_states.contains_key(*name))
        {
            return Err(data_corruption(format!(
                "TinyMongo shard {shard} has indexes for undeclared collection {name:?}"
            )));
        }
        for name in collection_states.keys() {
            let declared = manifest_indexes.get(name).cloned().unwrap_or_default();
            let actual = child_indexes.get(name).cloned().unwrap_or_default();
            // Ready declarations must exist identically on every shard. A
            // pending build may be present on an arbitrary shard prefix after
            // interruption, but any present copy still has to match exactly.
            let missing_ready = declared
                .iter()
                .filter(|index| !index.source_pending)
                .any(|expected| !actual.contains(expected));
            let undeclared_or_changed = actual.iter().any(|found| {
                !declared
                    .iter()
                    .any(|expected| index_specs_equal(found, expected))
            });
            if missing_ready || undeclared_or_changed {
                return Err(data_corruption(format!(
                    "TinyMongo logical index catalog mismatch for collection {name:?} on shard {shard}"
                )));
            }
        }
        shard_connections.push(connection);
    }

    let mut budget = RetainedBudget::default();
    let mut collections = Vec::with_capacity(plan.collections.len());
    for name in plan.collections.iter() {
        ensure_import_not_cancelled(cancellation)?;
        let mut pending = Vec::new();
        for (shard, connection) in shard_connections.iter().enumerate() {
            ensure_import_not_cancelled(cancellation)?;
            pending.extend(read_collection_rows(
                connection,
                name,
                shard,
                true,
                &mut budget,
                cancellation,
            )?);
        }
        let (documents, legacy) = finish_collection(
            name,
            pending,
            Some(config.shard_count),
            &mut budget,
            cancellation,
        )?;
        if legacy != 0 {
            return Err(data_corruption(format!(
                "TinyMongo sharded collection {name:?} contains legacy physical identifiers"
            )));
        }
        collections.push(TinyMongoImportCollection {
            name: name.clone(),
            documents: documents.into_boxed_slice(),
            indexes: import_indexes(
                manifest_indexes.get(name).cloned().unwrap_or_default(),
                cancellation,
            )?,
        });
    }
    Ok(TinyMongoImportSource {
        variant: TinyMongoSourceVariant::ShardedV1,
        database_name: plan.database_name.clone(),
        source_path: path.to_path_buf(),
        source_shards: config.shard_count,
        legacy_physical_ids: 0,
        encoded_bson_bytes: budget.bson_bytes as u64,
        collections: collections.into_boxed_slice(),
    })
}

fn index_specs_equal(left: &LogicalIndex, right: &LogicalIndex) -> bool {
    left.name == right.name
        && left.keys == right.keys
        && left.unique == right.unique
        && left.sparse == right.sparse
        && left.partial_filter == right.partial_filter
}

fn finish_collection(
    collection: &str,
    mut pending: Vec<PendingDocument>,
    shard_count: Option<usize>,
    budget: &mut RetainedBudget,
    cancellation: &CancellationToken,
) -> EngineResult<(Vec<BsonDocument>, u64)> {
    if shard_count.is_some() {
        pending.sort_by(|left, right| {
            (
                left.order_token.is_none(),
                left.order_token.as_deref().unwrap_or(""),
                left.source_shard,
                left.rowid,
            )
                .cmp(&(
                    right.order_token.is_none(),
                    right.order_token.as_deref().unwrap_or(""),
                    right.source_shard,
                    right.rowid,
                ))
        });
    }
    let mut identities = HashSet::new();
    let mut legacy = 0_u64;
    let mut order_tokens = HashSet::new();
    let mut documents = Vec::with_capacity(pending.len());
    for item in pending {
        ensure_import_not_cancelled(cancellation)?;
        let PendingDocument {
            mut document,
            retained_bson_bytes,
            physical_id,
            order_token,
            source_shard,
            rowid: _,
        } = item;
        if let Some(physical_id) = physical_id.as_deref() {
            if physical_id.starts_with(PHYSICAL_ID_PREFIX) {
                // The frozen v2 digest is SHA-256 over TinyMongo's private
                // type-specific canonical JSON, including exact Decimal128
                // rationalization. Reimplementing only part of that encoder
                // would falsely reject legal IDs. Validate its complete wire
                // shape and routing here; semantic uniqueness is checked from
                // logical `_id`, and the destination always rekeys from that
                // logical value. Consequently this reader does not diagnose a
                // syntactically valid digest that disagrees with its row's
                // logical `_id` while still landing on the same source shard.
                validate_physical_id(physical_id)?;
                if let Some(count) = shard_count {
                    let routed = physical_id_route(physical_id, count)?;
                    if routed != source_shard {
                        return Err(data_corruption(format!(
                            "TinyMongo row in collection {collection:?} is stored on shard {} but physical identifier routes to shard {routed}",
                            source_shard
                        )));
                    }
                }
            } else {
                document =
                    restore_and_validate_legacy_document_id(collection, physical_id, document)?;
                budget.replace_document_size(retained_bson_bytes, &document)?;
                legacy = legacy.saturating_add(1);
            }
        }
        validate_document_identity(collection, &document, &mut identities)?;
        if let Some(token) = order_token.as_deref() {
            validate_order_token(token)?;
            if !order_tokens.insert(token.to_owned()) {
                return Err(data_corruption(format!(
                    "TinyMongo collection {collection:?} contains duplicate natural-order token"
                )));
            }
        }
        documents.push(document);
    }
    Ok((documents, legacy))
}

const MAX_LEGACY_PHYSICAL_ID_BYTES: usize = 16 * 1024 * 1024;
const MAX_LEGACY_LITERAL_VALUES: usize = 1_000_000;

fn restore_and_validate_legacy_document_id(
    collection: &str,
    physical_id: &str,
    document: BsonDocument,
) -> EngineResult<BsonDocument> {
    if physical_id.len() > MAX_LEGACY_PHYSICAL_ID_BYTES {
        return Err(EngineError::new(
            EngineErrorKind::LimitExceeded,
            format!(
                "TinyMongo legacy physical identifier in collection {collection:?} exceeds {MAX_LEGACY_PHYSICAL_ID_BYTES} bytes"
            ),
        ));
    }
    let logical_id = document
        .get_unique("_id")
        .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?
        .ok_or_else(|| {
            data_corruption(format!(
                "TinyMongo document in collection {collection:?} has no logical _id"
            ))
        })?;

    if matches!(logical_id, BsonValue::Document(_) | BsonValue::Array(_)) {
        let candidate = LegacyLiteralParser::parse(physical_id).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Unsupported,
                format!(
                    "TinyMongo legacy container _id in collection {collection:?} cannot be parsed losslessly"
                ),
            )
        })?;
        if !matches!(candidate, BsonValue::Document(_) | BsonValue::Array(_))
            || !legacy_id_values_equal(&candidate, logical_id)
        {
            return Err(data_corruption(format!(
                "TinyMongo legacy physical identifier disagrees with logical _id in collection {collection:?}"
            )));
        }
        return replace_document_id(document, candidate);
    }

    let matches = legacy_scalar_id_matches(physical_id, logical_id)?.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Unsupported,
            format!(
                "TinyMongo legacy physical identifier for this BSON _id family cannot be validated losslessly in collection {collection:?}"
            ),
        )
    })?;
    if !matches {
        return Err(data_corruption(format!(
            "TinyMongo legacy physical identifier disagrees with logical _id in collection {collection:?}"
        )));
    }
    Ok(document)
}

fn replace_document_id(
    document: BsonDocument,
    replacement: BsonValue,
) -> EngineResult<BsonDocument> {
    let mut replacement = Some(replacement);
    BsonDocument::from_entries(document.into_entries().into_iter().map(|(name, value)| {
        if name == "_id" {
            (
                name,
                replacement
                    .take()
                    .expect("validated document contains one _id"),
            )
        } else {
            (name, value)
        }
    }))
    .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))
}

fn legacy_id_values_equal(left: &BsonValue, right: &BsonValue) -> bool {
    match (left, right) {
        (BsonValue::Document(left), BsonValue::Document(right)) => {
            left.len() == right.len()
                && left.iter().all(|(name, left_value)| {
                    right
                        .get_unique(name)
                        .ok()
                        .flatten()
                        .is_some_and(|right_value| legacy_id_values_equal(left_value, right_value))
                })
        }
        (BsonValue::Array(left), BsonValue::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| legacy_id_values_equal(left, right))
        }
        _ => left == right,
    }
}

fn legacy_scalar_id_matches(
    physical_id: &str,
    logical_id: &BsonValue,
) -> EngineResult<Option<bool>> {
    let matches = match logical_id {
        BsonValue::String(value) => physical_id == value,
        BsonValue::Boolean(value) => physical_id == if *value { "True" } else { "False" },
        BsonValue::Null => physical_id == "None",
        BsonValue::Int32(value) => legacy_integer_id_matches(physical_id, i64::from(*value)),
        BsonValue::Int64(value) => legacy_integer_id_matches(physical_id, *value),
        BsonValue::Double(value) => return Ok(legacy_double_id_matches(physical_id, *value)),
        BsonValue::Binary(value) if value.subtype() == 0 => {
            legacy_binary_id_matches(physical_id, value)
        }
        BsonValue::Uuid(value) => physical_id == format_uuid(value.bytes()).as_str(),
        BsonValue::ObjectId(value) => physical_id == value.to_hex(),
        BsonValue::JavaScript(value) if value.scope().is_none() => physical_id == value.code(),
        BsonValue::JavaScript(_) => return Ok(None),
        BsonValue::Decimal128(value) => {
            let rendered = value.to_string();
            let Some(reparsed) = crate::document::BsonDecimal128::parse(&rendered).ok() else {
                return Ok(None);
            };
            if !reparsed.representation_eq(value) {
                return Ok(None);
            }
            physical_id == rendered
        }
        BsonValue::Timestamp(value) => {
            physical_id == format!("Timestamp({}, {})", value.time(), value.increment())
        }
        BsonValue::MinKey => physical_id == "MinKey()",
        BsonValue::MaxKey => physical_id == "MaxKey()",
        BsonValue::Binary(_)
        | BsonValue::DateTime(_)
        | BsonValue::RegularExpression(_)
        | BsonValue::Document(_)
        | BsonValue::Array(_) => return Ok(None),
    };
    Ok(Some(matches))
}

fn legacy_binary_id_matches(physical_id: &str, value: &BsonBinary) -> bool {
    if let Some(inner) = physical_id
        .strip_prefix("bytearray(")
        .and_then(|value| value.strip_suffix(')'))
    {
        return inner == canonical_python_bytes(value.bytes());
    }
    LegacyLiteralParser::parse(physical_id)
        .is_some_and(|candidate| candidate == BsonValue::Binary(BsonBinary::new(0, value.bytes())))
}

fn legacy_integer_id_matches(physical_id: &str, value: i64) -> bool {
    if physical_id == value.to_string() {
        return true;
    }
    let as_double = value as f64;
    (as_double.is_finite()
        && BsonValue::Double(as_double) == BsonValue::Int64(value)
        && provable_python_float_text(as_double).is_some_and(|rendered| physical_id == rendered))
        || (value == 0 && physical_id == "-0.0")
}

fn legacy_double_id_matches(physical_id: &str, value: f64) -> Option<bool> {
    if value.is_nan() {
        return Some(physical_id == "nan");
    }
    if value == f64::INFINITY {
        return Some(physical_id == "inf");
    }
    if value == f64::NEG_INFINITY {
        return Some(physical_id == "-inf");
    }
    if value == 0.0 && matches!(physical_id, "0" | "0.0" | "-0.0") {
        return Some(true);
    }
    if value.fract() == 0.0 && value >= i64::MIN as f64 && value < -(i64::MIN as f64) {
        let integer = value as i64;
        if integer as f64 == value {
            let integer_text = integer.to_string();
            if physical_id == integer_text {
                return Some(true);
            }
            return provable_python_float_text(value).map(|float_text| physical_id == float_text);
        }
    }
    None
}

fn provable_python_float_text(value: f64) -> Option<String> {
    if value.is_nan() {
        return Some("nan".to_owned());
    }
    if value == f64::INFINITY {
        return Some("inf".to_owned());
    }
    if value == f64::NEG_INFINITY {
        return Some("-inf".to_owned());
    }
    if value == 0.0 {
        return Some(if value.is_sign_negative() {
            "-0.0".to_owned()
        } else {
            "0.0".to_owned()
        });
    }
    if value.fract() != 0.0 || value.abs() >= 10_000_000_000_000_000.0 {
        return None;
    }
    let integer = value as i64;
    (integer as f64 == value).then(|| format!("{integer}.0"))
}

fn format_uuid(bytes: [u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(36);
    for (index, byte) in bytes.into_iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            output.push('-');
        }
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

struct LegacyLiteralParser<'a> {
    input: &'a str,
    offset: usize,
    values: usize,
}

impl<'a> LegacyLiteralParser<'a> {
    fn parse(input: &'a str) -> Option<BsonValue> {
        if input.len() > MAX_LEGACY_PHYSICAL_ID_BYTES {
            return None;
        }
        let mut parser = Self {
            input,
            offset: 0,
            values: 0,
        };
        let value = parser.parse_value(1)?;
        (parser.offset == input.len()).then_some(value)
    }

    fn parse_value(&mut self, depth: usize) -> Option<BsonValue> {
        if depth > 100 || self.values >= MAX_LEGACY_LITERAL_VALUES {
            return None;
        }
        self.values += 1;
        if matches!(self.peek(), Some('b' | 'B')) && self.remaining()[1..].starts_with(['\'', '"'])
        {
            return self
                .parse_bytes()
                .map(|bytes| BsonValue::Binary(BsonBinary::new(0, bytes)));
        }
        match self.peek()? {
            '\'' | '"' => self.parse_string().map(BsonValue::String),
            '{' => self.parse_document(depth),
            '[' => self.parse_list(depth),
            '(' => self.parse_tuple(depth),
            _ if self.take_keyword("True") => Some(BsonValue::Boolean(true)),
            _ if self.take_keyword("False") => Some(BsonValue::Boolean(false)),
            _ if self.take_keyword("None") => Some(BsonValue::Null),
            _ => self.parse_number(),
        }
    }

    fn parse_document(&mut self, depth: usize) -> Option<BsonValue> {
        self.take_expected('{')?;
        let mut document = BsonDocument::new();
        if self.take_if('}') {
            return Some(BsonValue::Document(document));
        }
        loop {
            let BsonValue::String(name) = self.parse_value(depth + 1)? else {
                return None;
            };
            self.take_exact(": ")?;
            let value = self.parse_value(depth + 1)?;
            document.push(name, value).ok()?;
            if self.take_if('}') {
                break;
            }
            self.take_exact(", ")?;
            if self.peek() == Some('}') {
                return None;
            }
        }
        Some(BsonValue::Document(document))
    }

    fn parse_list(&mut self, depth: usize) -> Option<BsonValue> {
        self.take_expected('[')?;
        let mut values = Vec::new();
        if self.take_if(']') {
            return Some(BsonValue::Array(values));
        }
        loop {
            values.push(self.parse_value(depth + 1)?);
            if self.take_if(']') {
                break;
            }
            self.take_exact(", ")?;
            if self.peek() == Some(']') {
                return None;
            }
        }
        Some(BsonValue::Array(values))
    }

    fn parse_tuple(&mut self, depth: usize) -> Option<BsonValue> {
        self.take_expected('(')?;
        if self.take_if(')') {
            return Some(BsonValue::Array(Vec::new()));
        }
        let first = self.parse_value(depth + 1)?;
        self.take_expected(',')?;
        let mut values = vec![first];
        if self.take_if(')') {
            return Some(BsonValue::Array(values));
        }
        self.take_expected(' ')?;
        loop {
            values.push(self.parse_value(depth + 1)?);
            if self.take_if(')') {
                break;
            }
            self.take_exact(", ")?;
            if self.peek() == Some(')') {
                return None;
            }
        }
        Some(BsonValue::Array(values))
    }

    fn parse_number(&mut self) -> Option<BsonValue> {
        let start = self.offset;
        while let Some(character) = self.peek() {
            if character.is_whitespace() || matches!(character, ',' | ']' | ')' | '}' | ':') {
                break;
            }
            self.take_char()?;
        }
        let token = &self.input[start..self.offset];
        if token.is_empty() {
            return None;
        }
        if !token.contains(['.', 'e', 'E']) {
            if let Ok(value) = token.parse::<i64>() {
                if token != value.to_string() {
                    return None;
                }
                return Some(if let Ok(value) = i32::try_from(value) {
                    BsonValue::Int32(value)
                } else {
                    BsonValue::Int64(value)
                });
            }
        }
        let value = token.parse::<f64>().ok()?;
        let canonical = provable_python_float_text(value)?;
        (token == canonical).then_some(BsonValue::Double(value))
    }

    fn parse_string(&mut self) -> Option<String> {
        let start = self.offset;
        let quote = self.take_char()?;
        let mut output = String::new();
        loop {
            let character = self.take_char()?;
            if character == quote {
                let canonical = canonical_python_string(&output)?;
                return (self.input[start..self.offset] == canonical).then_some(output);
            }
            if character == '\\' {
                output.push(self.parse_string_escape()?);
            } else if character.is_control() {
                return None;
            } else {
                output.push(character);
            }
        }
    }

    fn parse_string_escape(&mut self) -> Option<char> {
        let escaped = self.take_char()?;
        match escaped {
            '\\' | '\'' | '"' => Some(escaped),
            'a' => Some('\u{7}'),
            'b' => Some('\u{8}'),
            'f' => Some('\u{c}'),
            'n' => Some('\n'),
            'r' => Some('\r'),
            't' => Some('\t'),
            'v' => Some('\u{b}'),
            'x' => char::from_u32(self.take_hex(2)?),
            'u' => char::from_u32(self.take_hex(4)?),
            'U' => char::from_u32(self.take_hex(8)?),
            '0'..='7' => char::from_u32(self.take_octal(escaped)?),
            _ => None,
        }
    }

    fn parse_bytes(&mut self) -> Option<Vec<u8>> {
        let start = self.offset;
        if !matches!(self.take_char()?, 'b' | 'B') {
            return None;
        }
        let quote = self.take_char()?;
        if !matches!(quote, '\'' | '"') {
            return None;
        }
        let mut output = Vec::new();
        loop {
            let character = self.take_char()?;
            if character == quote {
                let canonical = canonical_python_bytes(&output);
                return (self.input[start..self.offset] == canonical).then_some(output);
            }
            if character == '\\' {
                let escaped = self.take_char()?;
                let value = match escaped {
                    '\\' | '\'' | '"' => escaped as u8,
                    'a' => 7,
                    'b' => 8,
                    'f' => 12,
                    'n' => b'\n',
                    'r' => b'\r',
                    't' => b'\t',
                    'v' => 11,
                    'x' => u8::try_from(self.take_hex(2)?).ok()?,
                    '0'..='7' => u8::try_from(self.take_octal(escaped)?).ok()?,
                    _ => return None,
                };
                output.push(value);
            } else if character.is_ascii() && !character.is_control() {
                output.push(character as u8);
            } else {
                return None;
            }
        }
    }

    fn take_hex(&mut self, count: usize) -> Option<u32> {
        let mut value = 0_u32;
        for _ in 0..count {
            value = value.checked_mul(16)?;
            value = value.checked_add(self.take_char()?.to_digit(16)?)?;
        }
        Some(value)
    }

    fn take_octal(&mut self, first: char) -> Option<u32> {
        let mut value = first.to_digit(8)?;
        for _ in 0..2 {
            let Some(character) = self.peek() else {
                break;
            };
            let Some(digit) = character.to_digit(8) else {
                break;
            };
            self.take_char()?;
            value = value.checked_mul(8)?.checked_add(digit)?;
        }
        Some(value)
    }

    fn take_keyword(&mut self, keyword: &str) -> bool {
        if !self.remaining().starts_with(keyword) {
            return false;
        }
        let end = self.offset + keyword.len();
        if self.input[end..]
            .chars()
            .next()
            .is_some_and(|character| character.is_alphanumeric() || character == '_')
        {
            return false;
        }
        self.offset = end;
        true
    }

    fn remaining(&self) -> &'a str {
        &self.input[self.offset..]
    }

    fn peek(&self) -> Option<char> {
        self.remaining().chars().next()
    }

    fn take_char(&mut self) -> Option<char> {
        let character = self.peek()?;
        self.offset += character.len_utf8();
        Some(character)
    }

    fn take_expected(&mut self, expected: char) -> Option<()> {
        (self.take_char()? == expected).then_some(())
    }

    fn take_exact(&mut self, expected: &str) -> Option<()> {
        if !self.remaining().starts_with(expected) {
            return None;
        }
        self.offset += expected.len();
        Some(())
    }

    fn take_if(&mut self, expected: char) -> bool {
        if self.peek() == Some(expected) {
            self.take_char();
            true
        } else {
            false
        }
    }
}

fn canonical_python_string(value: &str) -> Option<String> {
    value.is_ascii().then(|| {
        let quote = if value.contains('\'') && !value.contains('"') {
            '"'
        } else {
            '\''
        };
        let mut output = String::with_capacity(value.len() + 2);
        output.push(quote);
        for byte in value.bytes() {
            push_python_repr_byte(&mut output, byte, quote);
        }
        output.push(quote);
        output
    })
}

fn canonical_python_bytes(value: &[u8]) -> String {
    let quote = if value.contains(&b'\'') && !value.contains(&b'"') {
        '"'
    } else {
        '\''
    };
    let mut output = String::with_capacity(value.len() + 3);
    output.push('b');
    output.push(quote);
    for &byte in value {
        push_python_repr_byte(&mut output, byte, quote);
    }
    output.push(quote);
    output
}

fn push_python_repr_byte(output: &mut String, byte: u8, quote: char) {
    match byte {
        b'\\' => output.push_str("\\\\"),
        b'\t' => output.push_str("\\t"),
        b'\n' => output.push_str("\\n"),
        b'\r' => output.push_str("\\r"),
        byte if byte == quote as u8 => {
            output.push('\\');
            output.push(quote);
        }
        0x20..=0x7e => output.push(char::from(byte)),
        _ => {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            output.push_str("\\x");
            output.push(char::from(HEX[usize::from(byte >> 4)]));
            output.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
}

fn validate_document_identity(
    collection: &str,
    document: &BsonDocument,
    identities: &mut HashSet<CanonicalBsonKey>,
) -> EngineResult<()> {
    let identifier = document
        .get_unique("_id")
        .map_err(|error| error.into_engine_error(crate::document::BsonErrorContext::StoredData))?;
    let identifier = identifier.ok_or_else(|| {
        data_corruption(format!(
            "TinyMongo document in collection {collection:?} has no logical _id"
        ))
    })?;
    let key = CanonicalBsonKey::encode(identifier)
        .map_err(|error| error.into_engine_error(crate::document::BsonErrorContext::StoredData))?;
    if !identities.insert(key) {
        return Err(data_corruption(format!(
            "TinyMongo collection {collection:?} contains duplicate semantic _id values"
        )));
    }
    Ok(())
}

fn read_collection_rows(
    connection: &Connection,
    collection: &str,
    shard: usize,
    sharded: bool,
    budget: &mut RetainedBudget,
    cancellation: &CancellationToken,
) -> EngineResult<Vec<PendingDocument>> {
    let table = quote_identifier(collection);
    let sql = if sharded {
        format!(
            "SELECT rowid, _id, data, {} FROM {table} ORDER BY rowid",
            quote_identifier(SHARD_ORDER_COLUMN)
        )
    } else {
        format!("SELECT rowid, _id, data, NULL FROM {table} ORDER BY rowid")
    };
    let mut statement = connection.prepare(&sql).map_err(sqlite_error::storage)?;
    let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
    let mut documents = Vec::new();
    while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
        ensure_import_not_cancelled(cancellation)?;
        let rowid = row.get::<_, i64>(0).map_err(sqlite_error::storage)?;
        let physical_id = text_column(
            row.get_ref(1).map_err(sqlite_error::storage)?,
            "collection _id",
        )?
        .to_owned();
        let payload = text_column(
            row.get_ref(2).map_err(sqlite_error::storage)?,
            "collection data",
        )?;
        let order_token = match row.get_ref(3).map_err(sqlite_error::storage)? {
            ValueRef::Null => None,
            value => Some(text_column(value, "__tinymongo_order")?.to_owned()),
        };
        budget.retain_source_metadata([
            physical_id.len(),
            order_token.as_deref().map_or(0, str::len),
        ])?;
        let document = json_document(
            &parse_json(payload, &format!("collection {collection:?} row {rowid}"))?,
            &format!("collection {collection:?} row {rowid}"),
        )?;
        let retained_bson_bytes = budget.retain(&document)?;
        documents.push(PendingDocument {
            document,
            retained_bson_bytes,
            physical_id: Some(physical_id),
            order_token,
            source_shard: shard,
            rowid,
        });
    }
    Ok(documents)
}

fn parse_json(value: &str, context: &str) -> EngineResult<JsonValue> {
    serde_json::from_str(value)
        .map_err(|error| data_corruption(format!("invalid TinyMongo JSON in {context}: {error}")))
}

fn json_document(value: &JsonValue, context: &str) -> EngineResult<BsonDocument> {
    let JsonValue::Object(object) = value else {
        return Err(data_corruption(format!(
            "TinyMongo {context} must be a JSON object"
        )));
    };
    json_object_to_document(object, true, context)
}

fn json_object_to_document(
    object: &JsonMap<String, JsonValue>,
    interpret_tags: bool,
    context: &str,
) -> EngineResult<BsonDocument> {
    let mut document = BsonDocument::new();
    for (name, value) in object {
        let value = if interpret_tags {
            json_to_bson(value, context)?
        } else {
            json_to_plain_bson(value, context)?
        };
        document.push(name.clone(), value).map_err(|error| {
            error.into_engine_error(crate::document::BsonErrorContext::StoredData)
        })?;
    }
    Ok(document)
}

fn json_to_bson(value: &JsonValue, context: &str) -> EngineResult<BsonValue> {
    match value {
        JsonValue::Null => Ok(BsonValue::Null),
        JsonValue::Bool(value) => Ok(BsonValue::Boolean(*value)),
        JsonValue::Number(value) => json_number_to_bson(value, context),
        JsonValue::String(value) => Ok(BsonValue::String(value.clone())),
        JsonValue::Array(values) => values
            .iter()
            .map(|value| json_to_bson(value, context))
            .collect::<EngineResult<Vec<_>>>()
            .map(BsonValue::Array),
        JsonValue::Object(object) => {
            if object.len() == 2
                && object.contains_key(TYPE_MARKER)
                && object.contains_key(VALUE_MARKER)
            {
                if let Some(value) = decode_tag(object, context)? {
                    return Ok(value);
                }
                return json_object_to_document(object, false, context).map(BsonValue::Document);
            }
            json_object_to_document(object, true, context).map(BsonValue::Document)
        }
    }
}

fn json_to_plain_bson(value: &JsonValue, context: &str) -> EngineResult<BsonValue> {
    match value {
        JsonValue::Null => Ok(BsonValue::Null),
        JsonValue::Bool(value) => Ok(BsonValue::Boolean(*value)),
        JsonValue::Number(value) => json_number_to_bson(value, context),
        JsonValue::String(value) => Ok(BsonValue::String(value.clone())),
        JsonValue::Array(values) => values
            .iter()
            .map(|value| json_to_plain_bson(value, context))
            .collect::<EngineResult<Vec<_>>>()
            .map(BsonValue::Array),
        JsonValue::Object(object) => {
            json_object_to_document(object, false, context).map(BsonValue::Document)
        }
    }
}

fn json_number_to_bson(number: &JsonNumber, context: &str) -> EngineResult<BsonValue> {
    let spelling = number.to_string();
    let is_float = spelling
        .bytes()
        .any(|byte| matches!(byte, b'.' | b'e' | b'E'));
    if !is_float {
        let integer = number.as_i64().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                format!("TinyMongo integer in {context} is outside BSON int64 range"),
            )
        })?;
        return Ok(i32::try_from(integer).map_or(BsonValue::Int64(integer), BsonValue::Int32));
    }
    let value = number
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                format!("TinyMongo JSON number in {context} is not an exact finite f64"),
            )
        })?;
    Ok(BsonValue::Double(value))
}

fn decode_tag(
    object: &JsonMap<String, JsonValue>,
    context: &str,
) -> EngineResult<Option<BsonValue>> {
    let Some(kind) = object.get(TYPE_MARKER).and_then(JsonValue::as_str) else {
        return Ok(None);
    };
    let payload = &object[VALUE_MARKER];
    let decoded = match kind {
        "float" => match payload.as_str() {
            Some("nan") => Some(BsonValue::Double(f64::NAN)),
            Some("-infinity") => Some(BsonValue::Double(f64::NEG_INFINITY)),
            Some("infinity") => Some(BsonValue::Double(f64::INFINITY)),
            _ => None,
        },
        "objectid" => payload
            .as_str()
            .and_then(|value| BsonObjectId::from_hex(value).ok())
            .map(BsonValue::ObjectId),
        "datetime" => payload
            .as_str()
            .and_then(parse_tinymongo_datetime)
            .map(BsonDateTime::from_millis)
            .map(BsonValue::DateTime),
        "binary" => decode_binary_tag(payload)?.map(BsonValue::Binary),
        "decimal128" => payload
            .as_str()
            .and_then(decode_fixed_hex::<16>)
            .map(BsonDecimal128::from_bid)
            .map(BsonValue::Decimal128),
        "minkey" if json_is_integer(payload, 1) => Some(BsonValue::MinKey),
        "maxkey" if json_is_integer(payload, 1) => Some(BsonValue::MaxKey),
        "timestamp" => decode_timestamp_tag(payload)?.map(BsonValue::Timestamp),
        "code" => decode_code_tag(payload, context)?.map(BsonValue::JavaScript),
        "uuid" => payload
            .as_str()
            .and_then(parse_uuid)
            .map(|bytes| BsonValue::Uuid(BsonUuid::new(bytes, UuidRepresentation::Standard))),
        "regex" => decode_regex_tag(payload)?.map(BsonValue::RegularExpression),
        "mapping" => decode_mapping_tag(payload, context)?.map(BsonValue::Document),
        _ => None,
    };
    Ok(decoded)
}

fn decode_binary_tag(payload: &JsonValue) -> EngineResult<Option<BsonBinary>> {
    let JsonValue::Object(payload) = payload else {
        return Ok(None);
    };
    if payload.len() != 2 || !payload.contains_key("base64") || !payload.contains_key("subtype") {
        return Ok(None);
    }
    let Some(encoded) = payload.get("base64").and_then(JsonValue::as_str) else {
        return Ok(None);
    };
    let Some(subtype) = exact_u64(&payload["subtype"]) else {
        return Ok(None);
    };
    let Ok(subtype) = u8::try_from(subtype) else {
        return Ok(None);
    };
    let Some(bytes) = decode_base64(encoded) else {
        return Ok(None);
    };
    Ok(Some(BsonBinary::new(subtype, bytes)))
}

fn decode_timestamp_tag(payload: &JsonValue) -> EngineResult<Option<BsonTimestamp>> {
    let JsonValue::Object(payload) = payload else {
        return Ok(None);
    };
    if payload.len() != 2 || !payload.contains_key("time") || !payload.contains_key("inc") {
        return Ok(None);
    }
    let (Some(time), Some(increment)) = (exact_u64(&payload["time"]), exact_u64(&payload["inc"]))
    else {
        return Ok(None);
    };
    let (Ok(time), Ok(increment)) = (u32::try_from(time), u32::try_from(increment)) else {
        return Ok(None);
    };
    Ok(Some(BsonTimestamp::new(time, increment)))
}

fn decode_code_tag(payload: &JsonValue, context: &str) -> EngineResult<Option<BsonJavaScript>> {
    let JsonValue::Object(payload) = payload else {
        return Ok(None);
    };
    if payload.len() != 2 || !payload.contains_key("code") || !payload.contains_key("scope") {
        return Ok(None);
    }
    let Some(code) = payload.get("code").and_then(JsonValue::as_str) else {
        return Ok(None);
    };
    match &payload["scope"] {
        JsonValue::Null => Ok(Some(BsonJavaScript::new(code))),
        JsonValue::Object(scope) => Ok(Some(BsonJavaScript::with_scope(
            code,
            json_object_to_document(scope, true, context)?,
        ))),
        _ => Ok(None),
    }
}

fn decode_regex_tag(payload: &JsonValue) -> EngineResult<Option<BsonRegex>> {
    let JsonValue::Object(payload) = payload else {
        return Ok(None);
    };
    let expected = ["pattern", "flags", "representation", "pattern_type"];
    if payload.len() != expected.len() || expected.iter().any(|key| !payload.contains_key(*key)) {
        return Ok(None);
    }
    let (Some(pattern), Some(flags), Some(representation), Some(pattern_type)) = (
        payload["pattern"].as_str(),
        exact_i64(&payload["flags"]),
        payload["representation"].as_str(),
        payload["pattern_type"].as_str(),
    ) else {
        return Ok(None);
    };
    if !matches!(representation, "python" | "bson")
        || !matches!(pattern_type, "string" | "bytes")
        || flags < 0
        || pattern.contains('\0')
    {
        return Ok(None);
    }
    if representation != "bson" || pattern_type != "string" {
        return Err(EngineError::new(
            EngineErrorKind::Unsupported,
            "TinyMongo regex representation cannot be preserved as BSON",
        ));
    }
    if flags & !126 != 0 {
        return Err(EngineError::new(
            EngineErrorKind::Unsupported,
            "TinyMongo regex uses Python-only flags that have no BSON representation",
        ));
    }
    let mut options = String::new();
    for (bit, option) in [
        (2, 'i'),
        (4, 'l'),
        (8, 'm'),
        (16, 's'),
        (32, 'u'),
        (64, 'x'),
    ] {
        if flags & bit != 0 {
            options.push(option);
        }
    }
    Ok(Some(BsonRegex::new(pattern, options).map_err(|error| {
        error.into_engine_error(crate::document::BsonErrorContext::StoredData)
    })?))
}

fn decode_mapping_tag(payload: &JsonValue, context: &str) -> EngineResult<Option<BsonDocument>> {
    let JsonValue::Array(entries) = payload else {
        return Ok(None);
    };
    let mut names = HashSet::new();
    let mut document = BsonDocument::new();
    for entry in entries {
        let JsonValue::Array(pair) = entry else {
            return Ok(None);
        };
        if pair.len() != 2 {
            return Ok(None);
        }
        let Some(name) = pair[0].as_str() else {
            return Ok(None);
        };
        if !names.insert(name) {
            return Err(data_corruption(
                "TinyMongo escaped mapping contains duplicate field names",
            ));
        }
        document
            .push(name, json_to_bson(&pair[1], context)?)
            .map_err(|error| {
                error.into_engine_error(crate::document::BsonErrorContext::StoredData)
            })?;
    }
    Ok(Some(document))
}

fn json_object<'a>(
    value: &'a JsonValue,
    context: &str,
) -> EngineResult<&'a JsonMap<String, JsonValue>> {
    value
        .as_object()
        .ok_or_else(|| data_corruption(format!("{context} must be a JSON object")))
}

fn exact_i64(value: &JsonValue) -> Option<i64> {
    let number = value.as_number()?;
    let spelling = number.to_string();
    (!spelling
        .bytes()
        .any(|byte| matches!(byte, b'.' | b'e' | b'E')))
    .then(|| number.as_i64())
    .flatten()
}

fn exact_u64(value: &JsonValue) -> Option<u64> {
    let value = exact_i64(value)?;
    u64::try_from(value).ok()
}

fn json_is_integer(value: &JsonValue, expected: i64) -> bool {
    exact_i64(value) == Some(expected)
}

fn parse_tinymongo_datetime(value: &str) -> Option<i64> {
    let bytes = value.as_bytes();
    if bytes.len() < 19
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b'T' | b' ')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let year = decimal_digits(&bytes[0..4])? as i64;
    let month = decimal_digits(&bytes[5..7])? as i64;
    let day = decimal_digits(&bytes[8..10])? as i64;
    let hour = decimal_digits(&bytes[11..13])? as i64;
    let minute = decimal_digits(&bytes[14..16])? as i64;
    let second = decimal_digits(&bytes[17..19])? as i64;
    let mut cursor = 19;
    let mut micros = 0_i64;
    if bytes
        .get(cursor)
        .is_some_and(|byte| matches!(byte, b'.' | b','))
    {
        cursor += 1;
        let fraction_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        let fraction = bytes.get(fraction_start..cursor)?;
        if fraction.is_empty() {
            return None;
        }
        // Python's datetime parser retains microseconds and ignores additional
        // fractional precision. TinyMongo then stores only whole milliseconds.
        for (place, digit) in fraction.iter().take(6).enumerate() {
            micros += i64::from(*digit - b'0') * 10_i64.pow((5 - place) as u32);
        }
    }
    let offset_seconds = match bytes.get(cursor..) {
        Some([]) => 0_i64,
        Some([b'Z']) => 0_i64,
        Some(zone) if zone.len() == 6 && matches!(zone[0], b'+' | b'-') && zone[3] == b':' => {
            let offset_hour = decimal_digits(&zone[1..3])? as i64;
            let offset_minute = decimal_digits(&zone[4..6])? as i64;
            if offset_hour >= 24 || offset_minute >= 60 {
                return None;
            }
            let magnitude = offset_hour * 3_600 + offset_minute * 60;
            if zone[0] == b'-' {
                -magnitude
            } else {
                magnitude
            }
        }
        _ => return None,
    };
    if year == 0
        || !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour >= 24
        || minute >= 60
        || second >= 60
    {
        return None;
    }
    let days = days_from_civil(year, month, day);
    let milliseconds = days
        .checked_mul(86_400_000)?
        .checked_add(hour * 3_600_000 + minute * 60_000 + second * 1_000 + micros / 1_000)?
        .checked_sub(offset_seconds.checked_mul(1_000)?)?;
    let minimum = days_from_civil(1, 1, 1) * 86_400_000;
    let maximum = (days_from_civil(10_000, 1, 1) * 86_400_000) - 1;
    (minimum..=maximum)
        .contains(&milliseconds)
        .then_some(milliseconds)
}

fn decimal_digits(bytes: &[u8]) -> Option<u32> {
    bytes.iter().try_fold(0_u32, |value, byte| {
        byte.is_ascii_digit()
            .then(|| value * 10 + u32::from(*byte - b'0'))
    })
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => 0,
    }
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let adjusted_year = year - i64::from(month <= 2);
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn parse_uuid(value: &str) -> Option<[u8; 16]> {
    let bytes = value.as_bytes();
    if bytes.len() != 36 || [8, 13, 18, 23].iter().any(|index| bytes[*index] != b'-') {
        return None;
    }
    let mut compact = String::with_capacity(32);
    for (index, byte) in bytes.iter().enumerate() {
        if !matches!(index, 8 | 13 | 18 | 23) {
            compact.push(char::from(*byte));
        }
    }
    decode_fixed_hex::<16>(&compact)
}

fn decode_fixed_hex<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() != N * 2 {
        return None;
    }
    let mut output = [0_u8; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Some(output)
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn decode_base64(value: &str) -> Option<Vec<u8>> {
    let bytes = value.as_bytes();
    if bytes.len() % 4 != 0 {
        return None;
    }
    let mut output = Vec::with_capacity(bytes.len() / 4 * 3);
    for (chunk_index, chunk) in bytes.chunks_exact(4).enumerate() {
        let last = chunk_index + 1 == bytes.len() / 4;
        let a = base64_value(chunk[0])?;
        let b = base64_value(chunk[1])?;
        let c = if chunk[2] == b'=' {
            64
        } else {
            base64_value(chunk[2])?
        };
        let d = if chunk[3] == b'=' {
            64
        } else {
            base64_value(chunk[3])?
        };
        if !last && (c == 64 || d == 64) {
            return None;
        }
        if c == 64 && d != 64 {
            return None;
        }
        output.push((a << 2) | (b >> 4));
        if c != 64 {
            output.push((b << 4) | (c >> 2));
            if d != 64 {
                output.push((c << 6) | d);
            } else if c & 0b11 != 0 {
                return None;
            }
        } else if b & 0b1111 != 0 {
            return None;
        }
    }
    Some(output)
}

fn base64_value(value: u8) -> Option<u8> {
    match value {
        b'A'..=b'Z' => Some(value - b'A'),
        b'a'..=b'z' => Some(value - b'a' + 26),
        b'0'..=b'9' => Some(value - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

fn validate_database_name(name: &str) -> EngineResult<()> {
    if name.is_empty() || name.len() > MAX_DOCUMENT_DATABASE_NAME_BYTES || name.contains('\0') {
        return Err(invalid_argument(
            "TinyMongo import database name must contain 1 to 63 UTF-8 bytes and no NUL",
        ));
    }
    Ok(())
}

fn validate_collection_name(database: &str, name: &str) -> EngineResult<()> {
    if name.is_empty()
        || name.contains('\0')
        || name == "_default"
        || name.starts_with("__tinymongo_")
    {
        return Err(invalid_argument(format!(
            "TinyMongo import collection name {name:?} is empty or storage-reserved"
        )));
    }
    let namespace = database
        .len()
        .checked_add(1)
        .and_then(|length| length.checked_add(name.len()))
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                "TinyMongo namespace length overflowed",
            )
        })?;
    if namespace > MAX_DOCUMENT_NAMESPACE_BYTES {
        return Err(invalid_argument(format!(
            "TinyMongo namespace {database}.{name} exceeds {MAX_DOCUMENT_NAMESPACE_BYTES} UTF-8 bytes"
        )));
    }
    Ok(())
}

fn open_source(path: &Path, cancellation: &CancellationToken) -> EngineResult<Connection> {
    ensure_import_not_cancelled(cancellation)?;
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_EXRESCODE
        | OpenFlags::SQLITE_OPEN_URI;
    // SQLite's NOFOLLOW flag rejects any symlink component, including macOS's
    // system `/var` link. Resolve the already-inspected parent while retaining
    // the final component so replacement of the database itself is rejected.
    let parent = path.parent().ok_or_else(|| {
        failed_precondition(format!(
            "TinyMongo source path {} has no parent directory",
            path.display()
        ))
    })?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let parent_metadata = fs::symlink_metadata(parent).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!(
                "failed to inspect TinyMongo source directory {}",
                parent.display()
            ),
        )
    })?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(failed_precondition(format!(
            "TinyMongo source parent {} must be a real directory",
            parent.display()
        )));
    }
    let file_name = path.file_name().ok_or_else(|| {
        failed_precondition(format!(
            "TinyMongo source path {} has no file name",
            path.display()
        ))
    })?;
    let canonical_parent = fs::canonicalize(parent).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!(
                "failed to resolve TinyMongo source directory {}",
                parent.display()
            ),
        )
    })?;
    let open_path = canonical_parent.join(file_name);
    reject_source_sidecars(&open_path)?;
    let uri = immutable_sqlite_uri(&open_path)?;
    let connection = Connection::open_with_flags(uri, flags).map_err(sqlite_error::storage)?;
    connection
        .set_limit(Limit::SQLITE_LIMIT_LENGTH, MAX_SQLITE_IMPORT_ROW_BYTES)
        .map_err(sqlite_error::storage)?;
    let progress_cancellation = cancellation.clone();
    connection
        .progress_handler(1_000, Some(move || progress_cancellation.is_cancelled()))
        .map_err(sqlite_error::storage)?;
    connection
        .execute_batch("PRAGMA trusted_schema = OFF; PRAGMA query_only = ON; BEGIN DEFERRED")
        .map_err(sqlite_error::storage)?;
    verify_source_quick_check(&connection)?;
    reject_source_sidecars(&open_path)?;
    ensure_import_not_cancelled(cancellation)?;
    Ok(connection)
}

fn immutable_sqlite_uri(path: &Path) -> EngineResult<String> {
    #[cfg(unix)]
    let bytes = {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes().to_vec()
    };
    #[cfg(windows)]
    let bytes = path
        .to_str()
        .ok_or_else(|| failed_precondition("TinyMongo source path is not valid UTF-8"))?
        .replace('\\', "/")
        .into_bytes();
    #[cfg(not(any(unix, windows)))]
    let bytes = path
        .to_str()
        .ok_or_else(|| failed_precondition("TinyMongo source path is not valid UTF-8"))?
        .as_bytes()
        .to_vec();

    let mut uri = String::with_capacity(bytes.len().saturating_mul(3).saturating_add(25));
    uri.push_str("file:");
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in bytes {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b':' | b'-' | b'.' | b'_' | b'~') {
            uri.push(char::from(byte));
        } else {
            uri.push('%');
            uri.push(char::from(HEX[usize::from(byte >> 4)]));
            uri.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    uri.push_str("?mode=ro&immutable=1");
    Ok(uri)
}

fn reject_source_sidecars(path: &Path) -> EngineResult<()> {
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut sidecar = path.as_os_str().to_os_string();
        sidecar.push(suffix);
        let sidecar = PathBuf::from(sidecar);
        match fs::symlink_metadata(&sidecar) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => {
                return Err(failed_precondition(format!(
                    "TinyMongo source {} has an active SQLite sidecar; stop and checkpoint the source before import",
                    path.display()
                )));
            }
            Err(error) => {
                return Err(sqlite_error::storage_io(
                    error,
                    format!(
                        "failed to inspect TinyMongo source sidecars for {}",
                        path.display()
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn verify_source_quick_check(connection: &Connection) -> EngineResult<()> {
    let mut statement = connection
        .prepare("PRAGMA quick_check")
        .map_err(sqlite_error::storage)?;
    let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
    let Some(row) = rows.next().map_err(sqlite_error::storage)? else {
        return Err(data_corruption(
            "TinyMongo source quick_check returned no result",
        ));
    };
    let result = row.get::<_, String>(0).map_err(sqlite_error::storage)?;
    if result != "ok" || rows.next().map_err(sqlite_error::storage)?.is_some() {
        return Err(data_corruption(
            "TinyMongo source failed SQLite quick_check",
        ));
    }
    Ok(())
}

fn ordinary_tables(connection: &Connection) -> EngineResult<Vec<String>> {
    let mut statement = connection
        .prepare(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name",
        )
        .map_err(sqlite_error::storage)?;
    let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
    let mut names = Vec::new();
    let mut name_bytes = 0_usize;
    while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
        let name = row.get::<_, String>(0).map_err(sqlite_error::storage)?;
        if names.len() >= MAX_TINYMONGO_SOURCE_TABLES {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!("TinyMongo source exceeds {MAX_TINYMONGO_SOURCE_TABLES} physical tables"),
            ));
        }
        charge_source_metadata(
            &mut name_bytes,
            [name.len()],
            MAX_TINYMONGO_IMPORT_METADATA_BSON_BYTES,
            "physical table-name inventory",
        )?;
        names.push(name);
    }
    Ok(names)
}

#[derive(Debug, PartialEq, Eq)]
struct ColumnShape {
    cid: i64,
    name: String,
    declared_type: String,
    not_null: bool,
    default_value: Option<String>,
    primary_key: i64,
    hidden: i64,
}

fn table_shape(connection: &Connection, table: &str) -> EngineResult<Vec<ColumnShape>> {
    let mut statement = connection
        .prepare(
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden FROM pragma_table_xinfo(?) ORDER BY cid",
        )
        .map_err(sqlite_error::storage)?;
    statement
        .query_map([table], |row| {
            Ok(ColumnShape {
                cid: row.get(0)?,
                name: row.get(1)?,
                declared_type: row.get(2)?,
                not_null: row.get::<_, i64>(3)? != 0,
                default_value: row.get(4)?,
                primary_key: row.get(5)?,
                hidden: row.get(6)?,
            })
        })
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)
}

fn table_exists(connection: &Connection, table: &str) -> EngineResult<bool> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?",
            [table],
            |_| Ok(()),
        )
        .optional()
        .map(|value| value.is_some())
        .map_err(sqlite_error::storage)
}

fn is_legacy_blob_schema(connection: &Connection) -> EngineResult<bool> {
    if !table_exists(connection, LEGACY_BLOB_TABLE)? {
        return Ok(false);
    }
    let shape = table_shape(connection, LEGACY_BLOB_TABLE)?;
    Ok(shape.len() == 2
        && shape[0].name == "id"
        && shape[0].declared_type.eq_ignore_ascii_case("INTEGER")
        && shape[0].primary_key == 1
        && shape[1].name == "data"
        && shape[1].declared_type.eq_ignore_ascii_case("TEXT"))
}

fn validate_legacy_blob_schema(connection: &Connection) -> EngineResult<()> {
    if !is_legacy_blob_schema(connection)? {
        return Err(data_corruption(
            "TinyMongo legacy tinydb table has an unsupported schema",
        ));
    }
    let shape = table_shape(connection, LEGACY_BLOB_TABLE)?;
    if shape[0].cid != 0
        || shape[0].not_null
        || shape[0].hidden != 0
        || shape[0].default_value.is_some()
        || shape[1].cid != 1
        || shape[1].not_null
        || shape[1].primary_key != 0
        || shape[1].hidden != 0
        || shape[1].default_value.is_some()
    {
        return Err(data_corruption(
            "TinyMongo legacy tinydb table has hidden/default columns",
        ));
    }
    Ok(())
}

fn validate_collection_schema(
    connection: &Connection,
    collection: &str,
    sharded: bool,
) -> EngineResult<()> {
    if !table_exists(connection, collection)? {
        return Err(data_corruption(format!(
            "allowlisted TinyMongo collection table {collection:?} does not exist"
        )));
    }
    let shape = table_shape(connection, collection)?;
    let expected_len = if sharded { 3 } else { 2 };
    if shape.len() != expected_len
        || shape[0].cid != 0
        || shape[0].name != "_id"
        || !shape[0].declared_type.eq_ignore_ascii_case("TEXT")
        || shape[0].not_null
        || shape[0].primary_key != 1
        || shape[0].hidden != 0
        || shape[0].default_value.is_some()
        || shape[1].cid != 1
        || shape[1].name != "data"
        || !shape[1].declared_type.eq_ignore_ascii_case("TEXT")
        || !shape[1].not_null
        || shape[1].primary_key != 0
        || shape[1].hidden != 0
        || shape[1].default_value.is_some()
        || (sharded
            && (shape[2].cid != 2
                || shape[2].name != SHARD_ORDER_COLUMN
                || !shape[2].declared_type.eq_ignore_ascii_case("TEXT")
                || shape[2].not_null
                || shape[2].primary_key != 0
                || shape[2].hidden != 0
                || shape[2].default_value.is_some()))
    {
        return Err(data_corruption(format!(
            "TinyMongo collection table {collection:?} has an unsupported physical schema"
        )));
    }
    let triggers: i64 = connection
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'trigger' AND tbl_name = ?",
            [collection],
            |row| row.get(0),
        )
        .map_err(sqlite_error::storage)?;
    if triggers != 0 {
        return Err(data_corruption(format!(
            "TinyMongo collection table {collection:?} has external triggers"
        )));
    }
    Ok(())
}

fn read_sql_index_catalog(
    connection: &Connection,
    cancellation: &CancellationToken,
) -> EngineResult<BTreeMap<String, Vec<LogicalIndex>>> {
    if !table_exists(connection, INDEX_CATALOG_TABLE)? {
        return Ok(BTreeMap::new());
    }
    let shape = table_shape(connection, INDEX_CATALOG_TABLE)?;
    let names: Vec<_> = shape.iter().map(|column| column.name.as_str()).collect();
    let has_token = names.get(4) == Some(&"token_version");
    let has_spec = names.get(5) == Some(&"spec_json");
    let allowed = matches!(
        names.as_slice(),
        ["collection_name", "index_name", "field_name", "unique_flag"]
            | [
                "collection_name",
                "index_name",
                "field_name",
                "unique_flag",
                "token_version"
            ]
            | [
                "collection_name",
                "index_name",
                "field_name",
                "unique_flag",
                "token_version",
                "spec_json"
            ]
    );
    if !allowed {
        return Err(data_corruption(
            "TinyMongo SQLite index catalog has an unsupported schema",
        ));
    }
    let common_shape_is_exact = shape.iter().take(4).enumerate().all(|(index, column)| {
        let expected = [
            ("collection_name", "TEXT", 1_i64),
            ("index_name", "TEXT", 2_i64),
            ("field_name", "TEXT", 0_i64),
            ("unique_flag", "INTEGER", 0_i64),
        ][index];
        column.cid == index as i64
            && column.name == expected.0
            && column.declared_type.eq_ignore_ascii_case(expected.1)
            && column.not_null
            && column.primary_key == expected.2
            && column.hidden == 0
            && column.default_value.is_none()
    });
    let token_shape_is_exact = !has_token || {
        let column = &shape[4];
        column.cid == 4
            && column.name == "token_version"
            && column.declared_type.eq_ignore_ascii_case("INTEGER")
            && column.not_null
            && column.primary_key == 0
            && column.hidden == 0
            && matches!(column.default_value.as_deref(), Some("1" | "2" | "3"))
    };
    let spec_shape_is_exact = !has_spec || {
        let column = &shape[5];
        column.cid == 5
            && column.name == "spec_json"
            && column.declared_type.eq_ignore_ascii_case("TEXT")
            && !column.not_null
            && column.primary_key == 0
            && column.hidden == 0
            && column.default_value.is_none()
    };
    if !common_shape_is_exact || !token_shape_is_exact || !spec_shape_is_exact {
        return Err(data_corruption(
            "TinyMongo SQLite index catalog has an unsupported physical schema",
        ));
    }
    let sql = format!(
        "SELECT collection_name, index_name, field_name, unique_flag, {}, {} FROM {} ORDER BY collection_name, index_name",
        if has_token { "token_version" } else { "1" },
        if has_spec { "spec_json" } else { "NULL" },
        quote_identifier(INDEX_CATALOG_TABLE),
    );
    let mut statement = connection.prepare(&sql).map_err(sqlite_error::storage)?;
    let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
    let mut result: BTreeMap<String, Vec<LogicalIndex>> = BTreeMap::new();
    let mut index_count = 0_usize;
    let mut metadata_bytes = 0_usize;
    while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
        ensure_import_not_cancelled(cancellation)?;
        let collection: String = row.get(0).map_err(sqlite_error::storage)?;
        let name: String = row.get(1).map_err(sqlite_error::storage)?;
        let field: String = row.get(2).map_err(sqlite_error::storage)?;
        let unique_flag: i64 = row.get(3).map_err(sqlite_error::storage)?;
        let token_version: i64 = row.get(4).map_err(sqlite_error::storage)?;
        let spec_json = match row.get_ref(5).map_err(sqlite_error::storage)? {
            ValueRef::Null => None,
            value => Some(text_column(value, "index spec_json")?.to_owned()),
        };
        charge_source_index_metadata(
            &mut metadata_bytes,
            [
                collection.len(),
                name.len(),
                field.len(),
                spec_json.as_deref().map_or(0, str::len),
            ],
        )?;
        if collection == "_default" || collection.starts_with("__tinymongo_") {
            return Err(data_corruption(
                "TinyMongo index catalog references a storage-reserved collection",
            ));
        }
        if !matches!(unique_flag, 0 | 1) || !(1..=3).contains(&token_version) {
            return Err(data_corruption(format!(
                "TinyMongo index {name:?} has invalid unique/token metadata"
            )));
        }
        if !table_exists(connection, &collection)? {
            return Err(data_corruption(format!(
                "TinyMongo index {name:?} references missing collection {collection:?}"
            )));
        }
        let mut index = if let Some(spec_json) = spec_json.filter(|value| !value.is_empty()) {
            parse_index_metadata(&parse_json(&spec_json, "SQLite index catalog")?, false)?
        } else {
            legacy_index(name.clone(), field.clone(), unique_flag == 1)?
        };
        if index.name != name
            || index.keys.first().map(|key| key.0.as_str()) != Some(field.as_str())
            || index.unique != (unique_flag == 1)
        {
            return Err(data_corruption(format!(
                "TinyMongo index catalog columns disagree with spec_json for {name:?}"
            )));
        }
        index.source_pending = false;
        result.entry(collection).or_default().push(index);
        index_count = index_count.checked_add(1).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                "TinyMongo custom index count overflowed",
            )
        })?;
        require_index_count(index_count)?;
    }
    for (collection, indexes) in &result {
        reject_duplicate_index_names(collection, indexes)?;
    }
    Ok(result)
}

fn parse_legacy_index_catalog(
    root: &JsonMap<String, JsonValue>,
    cancellation: &CancellationToken,
) -> EngineResult<BTreeMap<String, Vec<LogicalIndex>>> {
    let Some(catalog) = root.get(INDEX_CATALOG_TABLE) else {
        return Ok(BTreeMap::new());
    };
    let catalog = json_object(catalog, "legacy TinyMongo index catalog")?;
    let mut result: BTreeMap<String, Vec<LogicalIndex>> = BTreeMap::new();
    for (index_count, entry) in catalog.values().enumerate() {
        ensure_import_not_cancelled(cancellation)?;
        require_index_count(index_count + 1)?;
        let entry = json_object(entry, "legacy TinyMongo index entry")?;
        let required = ["_id", "collection", "spec"];
        if entry.len() != required.len() || required.iter().any(|key| !entry.contains_key(*key)) {
            return Err(data_corruption(
                "legacy TinyMongo index entry has an unexpected shape",
            ));
        }
        let collection = entry["collection"]
            .as_str()
            .ok_or_else(|| data_corruption("legacy TinyMongo index collection must be text"))?;
        if collection == "_default" || collection.starts_with("__tinymongo_") {
            return Err(data_corruption(
                "legacy TinyMongo index references a storage-reserved collection",
            ));
        }
        let index = parse_index_metadata(&entry["spec"], false)?;
        let expected_id = serde_json::to_string(&(collection, index.name.as_str()))
            .map_err(|error| EngineError::new(EngineErrorKind::Internal, error.to_string()))?;
        if entry["_id"].as_str() != Some(expected_id.as_str()) {
            return Err(data_corruption(
                "legacy TinyMongo index entry has an invalid catalog identity",
            ));
        }
        result.entry(collection.to_owned()).or_default().push(index);
    }
    for (collection, indexes) in &result {
        if !root.contains_key(collection) {
            return Err(data_corruption(format!(
                "legacy TinyMongo indexes reference missing collection {collection:?}"
            )));
        }
        reject_duplicate_index_names(collection, indexes)?;
    }
    Ok(result)
}

fn legacy_index(name: String, field: String, unique: bool) -> EngineResult<LogicalIndex> {
    validate_index_name(&name)?;
    validate_index_field(&field)?;
    Ok(LogicalIndex {
        name,
        keys: vec![(field, 1)],
        unique,
        sparse: false,
        partial_filter: None,
        source_pending: false,
    })
}

fn parse_index_metadata(value: &JsonValue, source_pending: bool) -> EngineResult<LogicalIndex> {
    let object = json_object(value, "TinyMongo index metadata")?;
    let version = object
        .get("v")
        .and_then(exact_i64)
        .ok_or_else(|| data_corruption("TinyMongo index metadata has no integer version"))?;
    let required_v1: BTreeSet<_> = ["v", "name", "key", "unique"].into_iter().collect();
    let required_v2: BTreeSet<_> = [
        "v",
        "name",
        "key",
        "unique",
        "sparse",
        "partialFilterExpression",
    ]
    .into_iter()
    .collect();
    let actual: BTreeSet<_> = object.keys().map(String::as_str).collect();
    if (version == 1 && actual != required_v1)
        || (version == 2 && actual != required_v2)
        || !matches!(version, 1 | 2)
    {
        return Err(data_corruption(
            "TinyMongo index metadata has an unsupported version or field set",
        ));
    }
    let name = object["name"]
        .as_str()
        .ok_or_else(|| data_corruption("TinyMongo index name must be text"))?
        .to_owned();
    validate_index_name(&name)?;
    let keys = object["key"]
        .as_array()
        .ok_or_else(|| data_corruption("TinyMongo index key must be an array"))?;
    if keys.is_empty() {
        return Err(data_corruption("TinyMongo index key must not be empty"));
    }
    let mut seen = HashSet::new();
    let mut parsed_keys = Vec::with_capacity(keys.len());
    for key in keys {
        let pair = key
            .as_array()
            .filter(|pair| pair.len() == 2)
            .ok_or_else(|| {
                data_corruption("TinyMongo index key entries must be two-item arrays")
            })?;
        let field = pair[0]
            .as_str()
            .ok_or_else(|| data_corruption("TinyMongo index field must be text"))?
            .to_owned();
        if !json_is_integer(&pair[1], 1) {
            return Err(data_corruption(
                "TinyMongo supports only ascending index direction 1",
            ));
        }
        validate_index_field(&field)?;
        if !seen.insert(field.clone()) {
            return Err(data_corruption("TinyMongo compound index repeats a field"));
        }
        parsed_keys.push((field, 1));
    }
    let unique = object["unique"]
        .as_bool()
        .ok_or_else(|| data_corruption("TinyMongo unique index option must be boolean"))?;
    let sparse = if version == 2 {
        object["sparse"]
            .as_bool()
            .ok_or_else(|| data_corruption("TinyMongo sparse index option must be boolean"))?
    } else {
        false
    };
    let partial_filter = if version == 2 {
        match &object["partialFilterExpression"] {
            JsonValue::Null => None,
            JsonValue::Object(filter) if !filter.is_empty() => {
                validate_partial_filter(filter)?;
                Some(json_object_to_document(
                    filter,
                    true,
                    "partialFilterExpression",
                )?)
            }
            _ => {
                return Err(data_corruption(
                    "TinyMongo partialFilterExpression must be a non-empty object or null",
                ));
            }
        }
    } else {
        None
    };
    if sparse && partial_filter.is_some() {
        return Err(data_corruption(
            "TinyMongo index cannot combine sparse and partialFilterExpression",
        ));
    }
    Ok(LogicalIndex {
        name,
        keys: parsed_keys,
        unique,
        sparse,
        partial_filter,
        source_pending,
    })
}

fn validate_index_name(name: &str) -> EngineResult<()> {
    if name.is_empty()
        || name.len() > MAX_TINYMONGO_IMPORT_INDEX_NAME_BYTES
        || name.contains('\0')
        || matches!(name, "_id" | "_id_")
    {
        return Err(data_corruption(
            "TinyMongo custom index name is invalid or reserved",
        ));
    }
    Ok(())
}

fn validate_index_field(field: &str) -> EngineResult<()> {
    if field.is_empty()
        || field.contains('\0')
        || field
            .split('.')
            .any(|part| part.is_empty() || part.starts_with('$'))
    {
        return Err(data_corruption("TinyMongo index field path is invalid"));
    }
    Ok(())
}

fn validate_partial_filter(filter: &JsonMap<String, JsonValue>) -> EngineResult<()> {
    if filter.is_empty() {
        return Err(data_corruption(
            "TinyMongo partial filter must not be empty",
        ));
    }
    const FIELD_OPERATORS: &[&str] = &[
        "$eq", "$exists", "$gt", "$gte", "$in", "$lt", "$lte", "$type",
    ];
    for (field, condition) in filter {
        if matches!(field.as_str(), "$and" | "$or") {
            let children = condition
                .as_array()
                .filter(|children| !children.is_empty())
                .ok_or_else(|| {
                    data_corruption("TinyMongo partial logical operator requires a non-empty array")
                })?;
            for child in children {
                validate_partial_filter(json_object(child, "partial filter child")?)?;
            }
            continue;
        }
        validate_index_field(field)?;
        let JsonValue::Object(operators) = condition else {
            continue;
        };
        let operator_names: Vec<_> = operators
            .keys()
            .filter(|name| name.starts_with('$'))
            .collect();
        if operator_names.is_empty() {
            continue;
        }
        if operator_names.len() != operators.len()
            || operator_names
                .iter()
                .any(|name| !FIELD_OPERATORS.contains(&name.as_str()))
            || operators
                .get("$exists")
                .is_some_and(|value| value != &JsonValue::Bool(true))
            || operators.get("$in").is_some_and(|value| !value.is_array())
        {
            return Err(data_corruption(
                "TinyMongo partial filter uses unsupported operators",
            ));
        }
    }
    Ok(())
}

fn import_indexes(
    indexes: Vec<LogicalIndex>,
    cancellation: &CancellationToken,
) -> EngineResult<Box<[TinyMongoImportIndex]>> {
    let mut imported = Vec::with_capacity(indexes.len());
    for index in indexes {
        ensure_import_not_cancelled(cancellation)?;
        let key = BsonDocument::from_entries(
            index
                .keys
                .iter()
                .map(|(field, direction)| (field.clone(), BsonValue::Int32(*direction))),
        )
        .map_err(|error| error.into_engine_error(crate::document::BsonErrorContext::StoredData))?;
        let partial = index
            .partial_filter
            .clone()
            .map(BsonValue::Document)
            .unwrap_or(BsonValue::Null);
        let specification = BsonDocument::from_entries([
            ("v", BsonValue::Int32(2)),
            ("name", BsonValue::String(index.name.clone())),
            ("key", BsonValue::Document(key)),
            ("unique", BsonValue::Boolean(index.unique)),
            ("sparse", BsonValue::Boolean(index.sparse)),
            ("partialFilterExpression", partial),
        ])
        .map_err(|error| error.into_engine_error(crate::document::BsonErrorContext::StoredData))?;
        encode_document(&specification).map_err(|error| {
            error.into_engine_error(crate::document::BsonErrorContext::StoredData)
        })?;
        imported.push(TinyMongoImportIndex {
            name: index.name,
            specification,
            unique: index.unique,
            source_pending: index.source_pending,
        });
    }
    Ok(imported.into_boxed_slice())
}

fn reject_duplicate_index_names(collection: &str, indexes: &[LogicalIndex]) -> EngineResult<()> {
    let mut names = HashSet::new();
    if indexes
        .iter()
        .any(|index| !names.insert(index.name.as_str()))
    {
        return Err(data_corruption(format!(
            "TinyMongo collection {collection:?} contains duplicate custom index names"
        )));
    }
    Ok(())
}

#[derive(Debug)]
struct ShardedConfig {
    shard_count: usize,
    database_id: String,
}

fn validate_manifest_schema(connection: &Connection) -> EngineResult<()> {
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(sqlite_error::storage)?;
    if version != SHARDED_FORMAT_VERSION {
        return Err(failed_precondition(format!(
            "unsupported TinyMongo sharded manifest version {version}"
        )));
    }
    let tables = ordinary_tables(connection)?;
    let expected: BTreeSet<_> = [
        SHARD_CONFIG_TABLE,
        SHARD_COLLECTION_TABLE,
        INDEX_CATALOG_TABLE,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    if tables.into_iter().collect::<BTreeSet<_>>() != expected {
        return Err(data_corruption(
            "TinyMongo sharded manifest has an unexpected table inventory",
        ));
    }
    require_columns(
        connection,
        SHARD_CONFIG_TABLE,
        &[
            ("format_version", "INTEGER", true, 0),
            ("shard_count", "INTEGER", true, 0),
            ("hash_algorithm", "TEXT", true, 0),
            ("database_id", "TEXT", true, 0),
            ("state", "TEXT", true, 0),
        ],
    )?;
    require_columns(
        connection,
        SHARD_COLLECTION_TABLE,
        &[
            ("collection_name", "TEXT", false, 1),
            ("state", "TEXT", true, 0),
        ],
    )?;
    require_columns(
        connection,
        INDEX_CATALOG_TABLE,
        &[
            ("collection_name", "TEXT", true, 1),
            ("index_name", "TEXT", true, 2),
            ("spec_json", "TEXT", true, 0),
            ("state", "TEXT", true, 0),
        ],
    )?;
    Ok(())
}

fn require_columns(
    connection: &Connection,
    table: &str,
    expected: &[(&str, &str, bool, i64)],
) -> EngineResult<()> {
    let shape = table_shape(connection, table)?;
    let exact = shape.len() == expected.len()
        && shape.iter().zip(expected).enumerate().all(
            |(index, (column, (name, declared_type, not_null, primary_key)))| {
                column.cid == index as i64
                    && column.name == *name
                    && column.declared_type.eq_ignore_ascii_case(declared_type)
                    && column.not_null == *not_null
                    && column.primary_key == *primary_key
                    && column.hidden == 0
                    && column.default_value.is_none()
            },
        );
    if !exact {
        return Err(data_corruption(format!(
            "TinyMongo metadata table {table:?} has an unsupported schema"
        )));
    }
    Ok(())
}

fn read_manifest_config(connection: &Connection) -> EngineResult<ShardedConfig> {
    let mut statement = connection
        .prepare("SELECT format_version, shard_count, hash_algorithm, database_id, state FROM __tinymongo_config")
        .map_err(sqlite_error::storage)?;
    let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
    let Some(row) = rows.next().map_err(sqlite_error::storage)? else {
        return Err(data_corruption(
            "TinyMongo sharded manifest has no configuration row",
        ));
    };
    let version: i64 = row.get(0).map_err(sqlite_error::storage)?;
    let shard_count: i64 = row.get(1).map_err(sqlite_error::storage)?;
    let hash_algorithm: String = row.get(2).map_err(sqlite_error::storage)?;
    let database_id: String = row.get(3).map_err(sqlite_error::storage)?;
    let state: String = row.get(4).map_err(sqlite_error::storage)?;
    if rows.next().map_err(sqlite_error::storage)?.is_some() {
        return Err(data_corruption(
            "TinyMongo sharded manifest has multiple configuration rows",
        ));
    }
    let shard_count = usize::try_from(shard_count)
        .ok()
        .filter(|count| (MIN_SHARD_COUNT..=MAX_SHARD_COUNT).contains(count))
        .ok_or_else(|| data_corruption("TinyMongo sharded manifest has an invalid shard count"))?;
    if version != SHARDED_FORMAT_VERSION
        || hash_algorithm != SHARDED_HASH_ALGORITHM
        || database_id.is_empty()
    {
        return Err(data_corruption(
            "TinyMongo sharded manifest identity is invalid",
        ));
    }
    if state != "ready" {
        return Err(failed_precondition(format!(
            "TinyMongo sharded manifest is {state:?}; recover it with pinned TinyMongo v1.3 before import"
        )));
    }
    Ok(ShardedConfig {
        shard_count,
        database_id,
    })
}

fn read_manifest_collections(
    connection: &Connection,
    cancellation: &CancellationToken,
) -> EngineResult<BTreeMap<String, String>> {
    let mut statement = connection
        .prepare(
            "SELECT collection_name, state FROM __tinymongo_collections ORDER BY collection_name",
        )
        .map_err(sqlite_error::storage)?;
    let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
    let mut result = BTreeMap::new();
    let mut metadata_bytes = 0_usize;
    while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
        ensure_import_not_cancelled(cancellation)?;
        let name: String = row.get(0).map_err(sqlite_error::storage)?;
        let state: String = row.get(1).map_err(sqlite_error::storage)?;
        if result.len() >= MAX_TINYMONGO_IMPORT_COLLECTIONS {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!(
                    "TinyMongo source exceeds {MAX_TINYMONGO_IMPORT_COLLECTIONS} manifest collections"
                ),
            ));
        }
        charge_source_metadata(
            &mut metadata_bytes,
            [name.len(), state.len()],
            MAX_TINYMONGO_IMPORT_METADATA_BSON_BYTES,
            "collection catalog",
        )?;
        if name.is_empty()
            || name.contains('\0')
            || name == "_default"
            || name.starts_with("__tinymongo_")
            || !matches!(state.as_str(), "ready" | "dropping")
            || result.insert(name.clone(), state).is_some()
        {
            return Err(data_corruption(
                "TinyMongo sharded collection catalog is invalid",
            ));
        }
    }
    Ok(result)
}

fn read_manifest_indexes(
    connection: &Connection,
    cancellation: &CancellationToken,
) -> EngineResult<BTreeMap<String, Vec<LogicalIndex>>> {
    let mut statement = connection
        .prepare("SELECT collection_name, index_name, spec_json, state FROM __tinymongo_indexes ORDER BY collection_name, index_name")
        .map_err(sqlite_error::storage)?;
    let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
    let mut result: BTreeMap<String, Vec<LogicalIndex>> = BTreeMap::new();
    let mut index_count = 0_usize;
    let mut metadata_bytes = 0_usize;
    while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
        ensure_import_not_cancelled(cancellation)?;
        index_count = index_count.checked_add(1).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                "TinyMongo custom index count overflowed",
            )
        })?;
        require_index_count(index_count)?;
        let collection: String = row.get(0).map_err(sqlite_error::storage)?;
        let name: String = row.get(1).map_err(sqlite_error::storage)?;
        let spec_json: String = row.get(2).map_err(sqlite_error::storage)?;
        let state: String = row.get(3).map_err(sqlite_error::storage)?;
        charge_source_index_metadata(
            &mut metadata_bytes,
            [collection.len(), name.len(), spec_json.len(), state.len()],
        )?;
        if !matches!(state.as_str(), "ready" | "pending") {
            return Err(data_corruption(
                "TinyMongo sharded index has an invalid lifecycle state",
            ));
        }
        let mut index = parse_index_metadata(
            &parse_json(&spec_json, "sharded manifest index")?,
            state == "pending",
        )?;
        if index.name != name {
            return Err(data_corruption(
                "TinyMongo manifest index name disagrees with spec_json",
            ));
        }
        index.source_pending = state == "pending";
        result.entry(collection).or_default().push(index);
    }
    for (collection, indexes) in &result {
        reject_duplicate_index_names(collection, indexes)?;
    }
    Ok(result)
}

fn require_index_count(count: usize) -> EngineResult<()> {
    if count > MAX_TINYMONGO_IMPORT_INDEXES {
        Err(EngineError::new(
            EngineErrorKind::LimitExceeded,
            format!("TinyMongo source exceeds {MAX_TINYMONGO_IMPORT_INDEXES} custom indexes"),
        ))
    } else {
        Ok(())
    }
}

fn charge_source_index_metadata(
    total: &mut usize,
    lengths: impl IntoIterator<Item = usize>,
) -> EngineResult<()> {
    charge_source_metadata(
        total,
        lengths,
        MAX_TINYMONGO_IMPORT_METADATA_BSON_BYTES,
        "index metadata",
    )
}

fn charge_source_metadata(
    total: &mut usize,
    lengths: impl IntoIterator<Item = usize>,
    limit: usize,
    label: &str,
) -> EngineResult<()> {
    for length in lengths {
        *total = total.checked_add(length).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!("TinyMongo source {label} size overflowed"),
            )
        })?;
    }
    if *total > limit {
        return Err(EngineError::new(
            EngineErrorKind::LimitExceeded,
            format!("TinyMongo source {label} exceeds {limit} bytes"),
        ));
    }
    Ok(())
}

fn validate_shard_identity(
    connection: &Connection,
    config: &ShardedConfig,
    shard: usize,
) -> EngineResult<()> {
    require_columns(
        connection,
        SHARD_IDENTITY_TABLE,
        &[
            ("format_version", "INTEGER", true, 0),
            ("database_id", "TEXT", true, 0),
            ("shard_index", "INTEGER", true, 0),
            ("shard_count", "INTEGER", true, 0),
            ("hash_algorithm", "TEXT", true, 0),
        ],
    )?;
    let mut statement = connection
        .prepare("SELECT format_version, database_id, shard_index, shard_count, hash_algorithm FROM __tinymongo_shard")
        .map_err(sqlite_error::storage)?;
    let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
    let Some(row) = rows.next().map_err(sqlite_error::storage)? else {
        return Err(data_corruption(format!(
            "TinyMongo shard {shard} has no identity row"
        )));
    };
    let actual = (
        row.get::<_, i64>(0).map_err(sqlite_error::storage)?,
        row.get::<_, String>(1).map_err(sqlite_error::storage)?,
        row.get::<_, i64>(2).map_err(sqlite_error::storage)?,
        row.get::<_, i64>(3).map_err(sqlite_error::storage)?,
        row.get::<_, String>(4).map_err(sqlite_error::storage)?,
    );
    if rows.next().map_err(sqlite_error::storage)?.is_some()
        || actual
            != (
                SHARDED_FORMAT_VERSION,
                config.database_id.clone(),
                shard as i64,
                config.shard_count as i64,
                SHARDED_HASH_ALGORITHM.to_owned(),
            )
    {
        return Err(data_corruption(format!(
            "TinyMongo shard {shard} identity does not match its manifest"
        )));
    }
    Ok(())
}

fn validate_shard_tree(
    path: &Path,
    shard_count: usize,
    cancellation: &CancellationToken,
) -> EngineResult<()> {
    let shards = path.join("shards");
    let metadata = fs::symlink_metadata(&shards).map_err(|error| {
        crate::sqlite_error::storage_io(error, "TinyMongo shards directory is unavailable")
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(failed_precondition(
            "TinyMongo shards path must be a real directory",
        ));
    }
    let mut expected = BTreeSet::new();
    for shard in 0..shard_count {
        expected.insert(format!("{shard:03}"));
    }
    let entries = fs::read_dir(&shards)
        .map_err(|error| crate::sqlite_error::storage_io(error, "cannot list TinyMongo shards"))?;
    let mut actual = BTreeSet::new();
    for entry in entries {
        ensure_import_not_cancelled(cancellation)?;
        if actual.len() >= shard_count {
            return Err(data_corruption(
                "TinyMongo shard directory contains more entries than its manifest declares",
            ));
        }
        let entry = entry.map_err(|error| {
            crate::sqlite_error::storage_io(error, "cannot inspect TinyMongo shard entry")
        })?;
        let file_type = entry.file_type().map_err(|error| {
            crate::sqlite_error::storage_io(error, "cannot inspect TinyMongo shard entry type")
        })?;
        if file_type.is_symlink() || !file_type.is_dir() {
            return Err(data_corruption(
                "TinyMongo shard inventory contains an entry that is not a real directory",
            ));
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| data_corruption("TinyMongo shard directory name is not UTF-8"))?;
        if !expected.contains(&name) {
            return Err(data_corruption(format!(
                "TinyMongo shard directory inventory contains undeclared entry {name:?}"
            )));
        }
        actual.insert(name);
    }
    if actual != expected {
        return Err(data_corruption(format!(
            "TinyMongo shard directory inventory mismatch: expected {expected:?}, found {actual:?}"
        )));
    }
    Ok(())
}

fn shard_path(root: &Path, shard: usize) -> PathBuf {
    root.join("shards")
        .join(format!("{shard:03}"))
        .join("data.sqlite")
}

fn require_regular_nonsymlink(path: &Path, label: &str) -> EngineResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        crate::sqlite_error::storage_io(
            error,
            format!("{label} is unavailable: {}", path.display()),
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(failed_precondition(format!(
            "{label} must be a regular non-symlink file"
        )));
    }
    Ok(())
}

fn require_wal(path: &Path, label: &str) -> EngineResult<()> {
    // An immutable read-only SQLite connection deliberately reports `delete`
    // for `PRAGMA journal_mode`, even when the durable database header records
    // WAL mode. Inspect the two format-version bytes in the checkpointed main
    // file instead (SQLite file-header offsets 18 and 19).
    let mut header = [0_u8; 20];
    fs::File::open(path)
        .and_then(|mut source| source.read_exact(&mut header))
        .map_err(|error| {
            sqlite_error::storage_io(
                error,
                format!("failed to read {label} header at {}", path.display()),
            )
        })?;
    if &header[..16] != b"SQLite format 3\0" || header[18] != 2 || header[19] != 2 {
        return Err(failed_precondition(format!(
            "{label} is not in WAL journal mode"
        )));
    }
    Ok(())
}

fn validate_physical_id(value: &str) -> EngineResult<()> {
    let digest = value
        .strip_prefix(PHYSICAL_ID_PREFIX)
        .ok_or_else(|| data_corruption("TinyMongo physical identifier has no v2 prefix"))?;
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(data_corruption(
            "TinyMongo v2 physical identifier has an invalid SHA-256 digest",
        ));
    }
    Ok(())
}

fn physical_id_route(value: &str, shard_count: usize) -> EngineResult<usize> {
    validate_physical_id(value)?;
    let suffix = &value[value.len() - 16..];
    let hash = u64::from_str_radix(suffix, 16)
        .map_err(|_| data_corruption("TinyMongo physical identifier suffix is invalid"))?;
    Ok((hash % shard_count as u64) as usize)
}

fn validate_order_token(value: &str) -> EngineResult<()> {
    let Some((timestamp, nonce)) = value.split_once(':') else {
        return Err(data_corruption(
            "TinyMongo natural-order token is malformed",
        ));
    };
    if timestamp.len() < 20
        || !timestamp.bytes().all(|byte| byte.is_ascii_digit())
        || nonce.len() != 12
        || !nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(data_corruption(
            "TinyMongo natural-order token is malformed",
        ));
    }
    Ok(())
}

fn text_column<'a>(value: ValueRef<'a>, label: &str) -> EngineResult<&'a str> {
    match value {
        ValueRef::Text(value) => std::str::from_utf8(value).map_err(|_| {
            EngineError::new(
                EngineErrorKind::InvalidTextEncoding,
                format!("TinyMongo {label} is not valid UTF-8"),
            )
        }),
        _ => Err(data_corruption(format!(
            "TinyMongo {label} must use SQLite TEXT storage"
        ))),
    }
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn invalid_argument(diagnostic: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::InvalidArgument, diagnostic)
}

fn failed_precondition(diagnostic: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::FailedPrecondition, diagnostic)
}

fn data_corruption(diagnostic: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::DataCorruption, diagnostic)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use tempfile::TempDir;

    fn plan(collections: &[&str]) -> TinyMongoImportPlan {
        TinyMongoImportPlan::new("app", collections.iter().copied()).unwrap()
    }

    fn create_collection(connection: &Connection, name: &str, sharded: bool) {
        connection
            .execute_batch(&format!(
                "CREATE TABLE {} (_id TEXT PRIMARY KEY, data TEXT NOT NULL){}",
                quote_identifier(name),
                if sharded {
                    format!(
                        "; ALTER TABLE {} ADD COLUMN {} TEXT",
                        quote_identifier(name),
                        quote_identifier(SHARD_ORDER_COLUMN)
                    )
                } else {
                    String::new()
                }
            ))
            .unwrap();
    }

    fn physical_id_for_route(shard: usize) -> String {
        format!("{PHYSICAL_ID_PREFIX}{:064x}", shard)
    }

    fn initialize_manifest(root: &Path, shard_count: usize) {
        fs::create_dir_all(root.join("shards")).unwrap();
        let manifest = Connection::open(root.join("manifest.sqlite")).unwrap();
        manifest.pragma_update(None, "journal_mode", "WAL").unwrap();
        manifest.pragma_update(None, "user_version", 1).unwrap();
        manifest
            .execute_batch(
                "CREATE TABLE __tinymongo_config (
                    format_version INTEGER NOT NULL,
                    shard_count INTEGER NOT NULL,
                    hash_algorithm TEXT NOT NULL,
                    database_id TEXT NOT NULL,
                    state TEXT NOT NULL);
                 CREATE TABLE __tinymongo_collections (
                    collection_name TEXT PRIMARY KEY,
                    state TEXT NOT NULL);
                 CREATE TABLE __tinymongo_indexes (
                    collection_name TEXT NOT NULL,
                    index_name TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    PRIMARY KEY (collection_name, index_name));",
            )
            .unwrap();
        manifest
            .execute(
                "INSERT INTO __tinymongo_config VALUES (1, ?, ?, 'database-id', 'ready')",
                params![shard_count as i64, SHARDED_HASH_ALGORITHM],
            )
            .unwrap();
        manifest
            .execute(
                "INSERT INTO __tinymongo_collections VALUES ('users', 'ready')",
                [],
            )
            .unwrap();
        for shard in 0..shard_count {
            let directory = root.join("shards").join(format!("{shard:03}"));
            fs::create_dir(&directory).unwrap();
            let connection = Connection::open(directory.join("data.sqlite")).unwrap();
            connection
                .pragma_update(None, "journal_mode", "WAL")
                .unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE __tinymongo_shard (
                        format_version INTEGER NOT NULL,
                        database_id TEXT NOT NULL,
                        shard_index INTEGER NOT NULL,
                        shard_count INTEGER NOT NULL,
                        hash_algorithm TEXT NOT NULL);",
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO __tinymongo_shard VALUES (1, 'database-id', ?, ?, ?)",
                    params![shard as i64, shard_count as i64, SHARDED_HASH_ALGORITHM],
                )
                .unwrap();
            create_collection(&connection, "users", true);
        }
    }

    #[test]
    fn plan_requires_explicit_non_reserved_unique_names() {
        let plan = TinyMongoImportPlan::new("app", ["z", "a"]).unwrap();
        assert_eq!(plan.collections(), &["a".to_owned(), "z".to_owned()]);
        assert_eq!(
            TinyMongoImportPlan::new("app", ["users", "users"])
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
        assert_eq!(
            TinyMongoImportPlan::new("app", ["__tinymongo_indexes"])
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
        assert_eq!(
            TinyMongoImportPlan::new("app", std::iter::empty::<&str>())
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
    }

    #[test]
    fn import_options_validate_shards_and_report_cancellation() {
        assert_eq!(
            TinyMongoImportOptions::new(1).unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
        let options = TinyMongoImportOptions::new(2).unwrap();
        assert_eq!(options.shard_count(), 2);
        assert!(!options.cancellation_token().is_cancelled());
        assert_eq!(
            format!("{options:?}"),
            "TinyMongoImportOptions { shard_count: 2, cancelled: false }"
        );
    }

    #[test]
    fn tagged_json_restores_bson_and_keeps_field_order() {
        let raw = serde_json::from_str(
            r#"{
                "z":1,
                "oid":{"__tinymongo_type_v1__":"objectid","value":"000000000000000000000001"},
                "date":{"__tinymongo_type_v1__":"datetime","value":"1969-12-31T23:59:59.999"},
                "offset_date":{"__tinymongo_type_v1__":"datetime","value":"2026-07-29T08:30:00.123999-04:00"},
                "binary":{"__tinymongo_type_v1__":"binary","value":{"base64":"AAEC/w==","subtype":4}},
                "decimal":{"__tinymongo_type_v1__":"decimal128","value":"3d0c0000000000000000000000003e30"},
                "timestamp":{"__tinymongo_type_v1__":"timestamp","value":{"time":2,"inc":3}},
                "uuid":{"__tinymongo_type_v1__":"uuid","value":"00112233-4455-6677-8899-aabbccddeeff"},
                "regex":{"__tinymongo_type_v1__":"regex","value":{"pattern":"a+","flags":42,"representation":"bson","pattern_type":"string"}},
                "min":{"__tinymongo_type_v1__":"minkey","value":1},
                "max":{"__tinymongo_type_v1__":"maxkey","value":1},
                "escaped":{"__tinymongo_type_v1__":"mapping","value":[["__tinymongo_type_v1__","objectid"],["value","literal"]]}
            }"#,
        )
        .unwrap();
        let document = json_document(&raw, "test").unwrap();
        assert_eq!(
            document.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            [
                "z",
                "oid",
                "date",
                "offset_date",
                "binary",
                "decimal",
                "timestamp",
                "uuid",
                "regex",
                "min",
                "max",
                "escaped"
            ]
        );
        assert_eq!(
            document.get_unique("date").unwrap(),
            Some(&BsonValue::DateTime(BsonDateTime::from_millis(-1)))
        );
        assert_eq!(
            document.get_unique("offset_date").unwrap(),
            Some(&BsonValue::DateTime(BsonDateTime::from_millis(
                1_785_328_200_123
            )))
        );
        assert!(matches!(
            document.get_unique("uuid").unwrap(),
            Some(BsonValue::Uuid(_))
        ));
        assert!(matches!(
            document.get_unique("escaped").unwrap(),
            Some(BsonValue::Document(_))
        ));
    }

    #[test]
    fn malformed_or_unknown_tags_remain_literal_documents() {
        let raw = serde_json::json!({
            "_id": 1,
            "unknown": {TYPE_MARKER: "future", VALUE_MARKER: {TYPE_MARKER: "minkey", VALUE_MARKER: 1}},
            "bad_binary": {TYPE_MARKER: "binary", VALUE_MARKER: {"base64": "!", "subtype": 0}}
        });
        let document = json_document(&raw, "test").unwrap();
        let Some(BsonValue::Document(unknown)) = document.get_unique("unknown").unwrap() else {
            panic!("unknown tag must remain a document");
        };
        assert!(matches!(
            unknown.get_unique(VALUE_MARKER).unwrap(),
            Some(BsonValue::Document(_))
        ));
        assert!(matches!(
            document.get_unique("bad_binary").unwrap(),
            Some(BsonValue::Document(_))
        ));

        let python_only_regex = serde_json::json!({
            "_id": 1,
            "value": {
                TYPE_MARKER: "regex",
                VALUE_MARKER: {
                    "pattern": "x",
                    "flags": 256,
                    "representation": "python",
                    "pattern_type": "string"
                }
            }
        });
        assert_eq!(
            json_document(&python_only_regex, "test")
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );

        for (representation, pattern_type) in
            [("python", "string"), ("python", "bytes"), ("bson", "bytes")]
        {
            let lossy_regex = serde_json::json!({
                "_id": 1,
                "value": {
                    TYPE_MARKER: "regex",
                    VALUE_MARKER: {
                        "pattern": "x",
                        "flags": 2,
                        "representation": representation,
                        "pattern_type": pattern_type
                    }
                }
            });
            assert_eq!(
                json_document(&lossy_regex, "test").unwrap_err().kind(),
                EngineErrorKind::Unsupported
            );
        }
    }

    #[test]
    fn legacy_blob_imports_documents_and_logical_index() {
        let temporary = TempDir::new().unwrap();
        let path = temporary.path().join("app.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("CREATE TABLE tinydb(id INTEGER PRIMARY KEY, data TEXT)")
            .unwrap();
        let payload = r#"{
            "users":{"1":{"_id":1,"name":"Ada"}},
            "__tinymongo_indexes":{"1":{
                "_id":"[\"users\",\"email_1\"]",
                "collection":"users",
                "spec":{"v":2,"name":"email_1","key":[["email",1]],"unique":true,"sparse":false,"partialFilterExpression":null}
            }}
        }"#;
        connection
            .execute("INSERT INTO tinydb VALUES (1, ?)", [payload])
            .unwrap();
        drop(connection);

        let source = read_tinymongo_source(&path, &plan(&["users"])).unwrap();
        assert_eq!(source.variant(), TinyMongoSourceVariant::LegacySingleRow);
        assert_eq!(source.report().documents(), 1);
        assert_eq!(source.collections()[0].indexes()[0].name(), "email_1");
        assert!(source.collections()[0].indexes()[0].is_unique());
        assert_eq!(
            source.collections()[0].indexes()[0].target_lifecycle(),
            DocumentIndexLifecycle::PendingBuild
        );
    }

    #[test]
    fn mixed_legacy_and_table_native_layout_is_rejected() {
        let temporary = TempDir::new().unwrap();
        let path = temporary.path().join("app.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE tinydb(id INTEGER PRIMARY KEY, data TEXT);
                 INSERT INTO tinydb VALUES(1, '{\"users\":{}}');
                 CREATE TABLE users (_id TEXT PRIMARY KEY, data TEXT NOT NULL);",
            )
            .unwrap();
        drop(connection);
        assert_eq!(
            read_tinymongo_source(&path, &plan(&["users"]))
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn table_native_reads_only_allowlisted_exact_schema() {
        let temporary = TempDir::new().unwrap();
        let path = temporary.path().join("app.sqlite");
        let connection = Connection::open(&path).unwrap();
        create_collection(&connection, "users", false);
        create_collection(&connection, "ordinary_sql_table", false);
        connection
            .execute(
                "INSERT INTO users VALUES ('user-1', ?)",
                [r#"{"_id":"user-1","last":2,"first":1}"#],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO ordinary_sql_table VALUES ('ordinary', '{\"_id\":\"must-not-import\"}')",
                [],
            )
            .unwrap();
        drop(connection);

        let source = read_tinymongo_source(&path, &plan(&["users"])).unwrap();
        assert_eq!(source.variant(), TinyMongoSourceVariant::TableNative);
        assert_eq!(source.collections().len(), 1);
        assert_eq!(source.legacy_physical_ids(), 1);
        assert_eq!(
            source.collections()[0].documents()[0]
                .iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            ["_id", "last", "first"]
        );
    }

    #[test]
    fn legacy_physical_ids_are_validated_and_restore_container_order() {
        let temporary = TempDir::new().unwrap();
        let path = temporary.path().join("app.sqlite");
        let connection = Connection::open(&path).unwrap();
        create_collection(&connection, "users", false);
        connection
            .execute(
                "INSERT INTO users VALUES (?1, ?2), (?3, ?4), (?5, ?6)",
                params![
                    "{'b': 2, 'a': 1}",
                    r#"{"_id":{"a":1.0,"b":2.0},"kind":"object"}"#,
                    "(1, {'b': 2, 'a': 1})",
                    r#"{"_id":[1,{"a":1,"b":2}],"kind":"array"}"#,
                    "[2, {'b': 2, 'a': 1}]",
                    r#"{"_id":[2,{"a":1,"b":2}],"kind":"list"}"#,
                ],
            )
            .unwrap();
        drop(connection);

        let source = read_tinymongo_source(&path, &plan(&["users"])).unwrap();
        assert_eq!(source.legacy_physical_ids(), 3);
        let object_id = source.collections()[0].documents()[0]
            .get_unique("_id")
            .unwrap()
            .unwrap();
        let BsonValue::Document(object_id) = object_id else {
            panic!("legacy mapping ID must remain a document");
        };
        assert_eq!(
            object_id.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            ["b", "a"]
        );
        assert!(matches!(
            object_id.get_unique("b").unwrap(),
            Some(BsonValue::Int32(2))
        ));
        let array_id = source.collections()[0].documents()[1]
            .get_unique("_id")
            .unwrap()
            .unwrap();
        let BsonValue::Array(array_id) = array_id else {
            panic!("legacy list ID must remain an array");
        };
        let BsonValue::Document(nested) = &array_id[1] else {
            panic!("nested legacy mapping ID must remain a document");
        };
        assert_eq!(
            nested.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            ["b", "a"]
        );
        let list_id = source.collections()[0].documents()[2]
            .get_unique("_id")
            .unwrap()
            .unwrap();
        let BsonValue::Array(list_id) = list_id else {
            panic!("legacy list ID must remain an array");
        };
        let BsonValue::Document(nested) = &list_id[1] else {
            panic!("nested legacy mapping ID must remain a document");
        };
        assert_eq!(
            nested.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            ["b", "a"]
        );
        let encoded_bson_bytes = source
            .collections()
            .iter()
            .flat_map(TinyMongoImportCollection::documents)
            .map(|document| encode_document(document).unwrap().len() as u64)
            .sum::<u64>();
        assert_eq!(source.encoded_bson_bytes(), encoded_bson_bytes);

        let connection = Connection::open(&path).unwrap();
        connection.execute("DELETE FROM users", []).unwrap();
        connection
            .execute(
                "INSERT INTO users VALUES ('different', '{\"_id\":\"user-1\"}')",
                [],
            )
            .unwrap();
        drop(connection);
        assert_eq!(
            read_tinymongo_source(&path, &plan(&["users"]))
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn legacy_container_ids_reject_noncanonical_python_spellings() {
        let temporary = TempDir::new().unwrap();
        for (index, physical_id) in [
            r#"{"b": 2, "a": 1}"#,
            "{'b':2, 'a': 1}",
            "{'b': 2, 'a': 1,}",
            " {'b': 2, 'a': 1}",
        ]
        .into_iter()
        .enumerate()
        {
            let path = temporary.path().join(format!("app-{index}.sqlite"));
            let connection = Connection::open(&path).unwrap();
            create_collection(&connection, "users", false);
            connection
                .execute(
                    "INSERT INTO users VALUES (?1, ?2)",
                    params![physical_id, r#"{"_id":{"a":1,"b":2}}"#],
                )
                .unwrap();
            drop(connection);
            assert_eq!(
                read_tinymongo_source(&path, &plan(&["users"]))
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::Unsupported
            );
        }
    }

    #[test]
    fn legacy_scalar_spellings_follow_tinymongo_numeric_and_binary_identity() {
        assert_eq!(provable_python_float_text(1.0).as_deref(), Some("1.0"));
        assert_eq!(provable_python_float_text(1e20), None);
        assert_eq!(provable_python_float_text(1e-7), None);
        assert_eq!(provable_python_float_text(-0.0).as_deref(), Some("-0.0"));
        assert!(legacy_integer_id_matches("1", 1));
        assert!(legacy_integer_id_matches("1.0", 1));
        assert!(legacy_integer_id_matches("-0.0", 0));
        assert_eq!(legacy_double_id_matches("1", 1.0), Some(true));
        assert_eq!(legacy_double_id_matches("0", -0.0), Some(true));
        assert_eq!(
            legacy_double_id_matches("10000000000000000", 1e16),
            Some(true)
        );
        assert_eq!(legacy_double_id_matches("1e+16", 1e16), None);
        assert_eq!(legacy_double_id_matches("0.5", 0.5), None);
        assert!(!legacy_integer_id_matches("1", 2));
        assert_eq!(
            LegacyLiteralParser::parse(r"b'\x00\xff'"),
            Some(BsonValue::Binary(BsonBinary::new(0, [0, 255])))
        );
        assert_eq!(LegacyLiteralParser::parse("bytearray(b'x')"), None);
        assert_eq!(
            legacy_scalar_id_matches(
                "bytearray(b'x')",
                &BsonValue::Binary(BsonBinary::new(0, b"x")),
            )
            .unwrap(),
            Some(true)
        );
        assert_eq!(
            legacy_scalar_id_matches(
                "bytearray(b\"x\")",
                &BsonValue::Binary(BsonBinary::new(0, b"x")),
            )
            .unwrap(),
            Some(false)
        );
        assert_eq!(
            legacy_scalar_id_matches("1", &BsonValue::Boolean(true)).unwrap(),
            Some(false)
        );

        let canonical_decimal = crate::document::BsonDecimal128::parse("1.00").unwrap();
        assert_eq!(
            legacy_scalar_id_matches("1.00", &BsonValue::Decimal128(canonical_decimal),).unwrap(),
            Some(true)
        );

        // The Rust BSON codec and pinned PyMongo oracle render some legal raw
        // BID values differently. Treat those representations as unsupported
        // instead of falsely diagnosing a valid TinyMongo row as corrupt.
        let divergent_bid = [
            0x86, 0xcb, 0xed, 0xa6, 0xdc, 0xed, 0x38, 0x1a, 0xf1, 0xf0, 0x7e, 0x46, 0x0a, 0xfd,
            0x6f, 0x23,
        ];
        let divergent = crate::document::BsonDecimal128::from_bid(divergent_bid);
        assert_eq!(divergent.to_string(), "0E-1641");
        assert_eq!(
            legacy_scalar_id_matches(
                "1.032456058729699916549522757607719E-1607",
                &BsonValue::Decimal128(divergent),
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn source_reads_are_immutable_and_active_sqlite_sidecars_fail_closed() {
        let temporary = TempDir::new().unwrap();
        let path = temporary.path().join("app ?#%.sqlite");
        let connection = Connection::open(&path).unwrap();
        create_collection(&connection, "users", false);
        connection
            .execute("INSERT INTO users VALUES ('one', '{\"_id\":\"one\"}')", [])
            .unwrap();
        drop(connection);

        let before = fs::read_dir(temporary.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<BTreeSet<_>>();
        let source = read_tinymongo_source(&path, &plan(&["users"])).unwrap();
        assert_eq!(source.report().documents(), 1);
        let after = fs::read_dir(temporary.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<BTreeSet<_>>();
        assert_eq!(after, before);

        let active_path = temporary.path().join("active.sqlite");
        let active = Connection::open(&active_path).unwrap();
        active.pragma_update(None, "journal_mode", "WAL").unwrap();
        create_collection(&active, "users", false);
        active
            .execute("INSERT INTO users VALUES ('one', '{\"_id\":\"one\"}')", [])
            .unwrap();
        assert!(PathBuf::from(format!("{}-wal", active_path.display())).exists());
        assert_eq!(
            read_tinymongo_source(&active_path, &plan(&["users"]))
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        drop(active);
    }

    #[test]
    fn cancelled_preflight_opens_no_source_or_destination() {
        let temporary = TempDir::new().unwrap();
        let path = temporary.path().join("app.sqlite");
        let connection = Connection::open(&path).unwrap();
        create_collection(&connection, "users", false);
        drop(connection);
        let cancellation = CancellationToken::new();
        assert!(cancellation.cancel());
        assert_eq!(
            read_tinymongo_source_with_cancellation(&path, &plan(&["users"]), &cancellation)
                .unwrap_err()
                .kind(),
            EngineErrorKind::Cancelled
        );
    }

    #[test]
    fn atomic_import_cleans_failed_stage_and_retry_publishes_verified_receipt() {
        let temporary = TempDir::new().unwrap();
        let source_path = temporary.path().join("app.sqlite");
        let destination = temporary.path().join("imported");
        let connection = Connection::open(&source_path).unwrap();
        create_collection(&connection, "users", false);
        connection
            .execute_batch(
                "CREATE TABLE __tinymongo_indexes (
                    collection_name TEXT NOT NULL,
                    index_name TEXT NOT NULL,
                    field_name TEXT NOT NULL,
                    unique_flag INTEGER NOT NULL,
                    token_version INTEGER NOT NULL DEFAULT 3,
                    spec_json TEXT,
                    PRIMARY KEY (collection_name, index_name));",
            )
            .unwrap();
        let spec = r#"{"v":2,"name":"email_1","key":[["email",1]],"unique":true,"sparse":false,"partialFilterExpression":null}"#;
        connection
            .execute(
                "INSERT INTO __tinymongo_indexes VALUES ('users', 'email_1', 'email', 1, 3, ?)",
                [spec],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO users VALUES
                    ('one', '{\"_id\":\"one\",\"email\":\"one@example.test\"}'),
                    ('two', '{\"_id\":\"two\",\"email\":\"two@example.test\"}')",
                [],
            )
            .unwrap();
        drop(connection);
        let source_before = fs::read(&source_path).unwrap();
        let import_plan = plan(&["users"]);

        let error = import_tinymongo_database_inner(
            &source_path,
            &destination,
            &import_plan,
            TinyMongoImportOptions::new(3).unwrap(),
            TinyMongoImportFault::FailAfterDocuments(1),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert!(error.diagnostic().contains("after 1 documents"));
        assert!(!destination.exists());
        assert_eq!(fs::read(&source_path).unwrap(), source_before);
        let unpublished_stages = fs::read_dir(temporary.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name.to_string_lossy().starts_with(".briskdb-import-stage-"))
            .collect::<Vec<_>>();
        assert!(unpublished_stages.is_empty());

        let report = import_tinymongo_database(
            &source_path,
            &destination,
            &import_plan,
            TinyMongoImportOptions::new(3).unwrap(),
        )
        .unwrap();
        assert_eq!(report.receipt_version(), TINYMONGO_IMPORT_RECEIPT_VERSION);
        assert_eq!(report.target_shards(), Some(3));
        assert_eq!(report.documents(), 2);
        assert_eq!(report.custom_indexes(), 1);
        assert!(destination.join("manifest.sqlite").is_file());
        let receipt: JsonValue =
            serde_json::from_slice(&fs::read(destination.join(IMPORT_RECEIPT_FILE)).unwrap())
                .unwrap();
        assert_eq!(receipt["database_name"], "app");
        assert_eq!(receipt["target_shards"], 3);
        assert_eq!(receipt["documents"], 2);
    }

    #[test]
    fn semantic_duplicate_ids_and_missing_ids_fail_closed() {
        let temporary = TempDir::new().unwrap();
        let path = temporary.path().join("app.sqlite");
        let connection = Connection::open(&path).unwrap();
        create_collection(&connection, "users", false);
        connection
            .execute(
                "INSERT INTO users VALUES ('one', '{\"_id\":1}'), ('double', '{\"_id\":1.0}')",
                [],
            )
            .unwrap();
        drop(connection);
        assert_eq!(
            read_tinymongo_source(&path, &plan(&["users"]))
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );

        let connection = Connection::open(&path).unwrap();
        connection.execute("DELETE FROM users", []).unwrap();
        connection
            .execute(
                "INSERT INTO users VALUES ('missing', '{\"name\":\"Ada\"}')",
                [],
            )
            .unwrap();
        drop(connection);
        assert_eq!(
            read_tinymongo_source(&path, &plan(&["users"]))
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn arbitrary_precision_integer_is_rejected_instead_of_becoming_double() {
        let raw = serde_json::from_str::<JsonValue>(
            r#"{"_id":1234567890123456789012345678901234567890}"#,
        )
        .unwrap();
        assert_eq!(
            json_document(&raw, "test").unwrap_err().kind(),
            EngineErrorKind::NumericOutOfRange
        );
    }

    #[test]
    fn sharded_layout_validates_identity_route_and_global_order() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path().join("app.sqlite-sharded");
        initialize_manifest(&root, 2);
        for shard in 0..2 {
            let connection = Connection::open(shard_path(&root, shard)).unwrap();
            connection
                .execute(
                    "INSERT INTO users (_id, data, __tinymongo_order) VALUES (?, ?, ?)",
                    params![
                        physical_id_for_route(shard),
                        format!("{{\"_id\":\"user-{shard}\"}}"),
                        format!("{:020}:abcdef012345", 2 - shard)
                    ],
                )
                .unwrap();
        }

        let source = read_tinymongo_source(&root, &plan(&["users"])).unwrap();
        assert_eq!(source.variant(), TinyMongoSourceVariant::ShardedV1);
        assert_eq!(source.source_shards(), 2);
        assert_eq!(
            source.collections()[0].documents()[0]
                .get_unique("_id")
                .unwrap(),
            Some(&BsonValue::String("user-1".to_owned()))
        );

        let connection = Connection::open(shard_path(&root, 0)).unwrap();
        connection
            .execute("UPDATE users SET _id = ?", [physical_id_for_route(1)])
            .unwrap();
        drop(connection);
        assert_eq!(
            read_tinymongo_source(&root, &plan(&["users"]))
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn sharded_directory_inventory_is_exact_and_rejects_extra_entries_early() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path().join("app.sqlite-sharded");
        initialize_manifest(&root, 2);
        fs::create_dir(root.join("shards/extra")).unwrap();

        assert_eq!(
            read_tinymongo_source(&root, &plan(&["users"]))
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn sharded_partial_collection_and_swapped_identity_fail_closed() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path().join("app.sqlite-sharded");
        initialize_manifest(&root, 2);
        let manifest = Connection::open(root.join("manifest.sqlite")).unwrap();
        manifest
            .execute("UPDATE __tinymongo_collections SET state = 'dropping'", [])
            .unwrap();
        drop(manifest);
        assert_eq!(
            read_tinymongo_source(&root, &plan(&["users"]))
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );

        let manifest = Connection::open(root.join("manifest.sqlite")).unwrap();
        manifest
            .execute("UPDATE __tinymongo_collections SET state = 'ready'", [])
            .unwrap();
        drop(manifest);
        let shard = Connection::open(shard_path(&root, 0)).unwrap();
        shard
            .execute("UPDATE __tinymongo_shard SET shard_index = 1", [])
            .unwrap();
        drop(shard);
        assert_eq!(
            read_tinymongo_source(&root, &plan(&["users"]))
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn sharded_pending_index_may_exist_on_only_some_shards() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path().join("app.sqlite-sharded");
        initialize_manifest(&root, 2);
        let spec = r#"{"v":2,"name":"email_1","key":[["email",1]],"unique":false,"sparse":false,"partialFilterExpression":null}"#;
        let manifest = Connection::open(root.join("manifest.sqlite")).unwrap();
        manifest
            .execute(
                "INSERT INTO __tinymongo_indexes VALUES ('users', 'email_1', ?, 'pending')",
                [spec],
            )
            .unwrap();
        drop(manifest);
        let shard = Connection::open(shard_path(&root, 0)).unwrap();
        shard
            .execute_batch(
                "CREATE TABLE __tinymongo_indexes (
                    collection_name TEXT NOT NULL,
                    index_name TEXT NOT NULL,
                    field_name TEXT NOT NULL,
                    unique_flag INTEGER NOT NULL,
                    token_version INTEGER NOT NULL DEFAULT 3,
                    spec_json TEXT,
                    PRIMARY KEY (collection_name, index_name));",
            )
            .unwrap();
        shard
            .execute(
                "INSERT INTO __tinymongo_indexes VALUES ('users', 'email_1', 'email', 0, 3, ?)",
                [spec],
            )
            .unwrap();
        drop(shard);

        let source = read_tinymongo_source(&root, &plan(&["users"])).unwrap();
        let index = &source.collections()[0].indexes()[0];
        assert_eq!(index.name(), "email_1");
        assert!(index.was_pending_in_source());
        assert_eq!(
            index.target_lifecycle(),
            DocumentIndexLifecycle::PendingBuild
        );
    }

    #[test]
    fn sharded_orphan_index_and_non_exact_metadata_schema_fail_closed() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path().join("app.sqlite-sharded");
        initialize_manifest(&root, 2);
        let spec = r#"{"v":2,"name":"email_1","key":[["email",1]],"unique":false,"sparse":false,"partialFilterExpression":null}"#;
        let manifest = Connection::open(root.join("manifest.sqlite")).unwrap();
        manifest
            .execute(
                "INSERT INTO __tinymongo_indexes VALUES ('missing', 'email_1', ?, 'ready')",
                [spec],
            )
            .unwrap();
        drop(manifest);
        assert_eq!(
            read_tinymongo_source(&root, &plan(&["users"]))
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );

        let manifest = Connection::open(root.join("manifest.sqlite")).unwrap();
        manifest
            .execute("DELETE FROM __tinymongo_indexes", [])
            .unwrap();
        manifest
            .execute_batch(
                "ALTER TABLE __tinymongo_collections RENAME TO old_collections;
                 CREATE TABLE __tinymongo_collections (
                    collection_name TEXT PRIMARY KEY,
                    state TEXT);
                 INSERT INTO __tinymongo_collections SELECT * FROM old_collections;
                 DROP TABLE old_collections;",
            )
            .unwrap();
        drop(manifest);
        assert_eq!(
            read_tinymongo_source(&root, &plan(&["users"]))
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }
}
