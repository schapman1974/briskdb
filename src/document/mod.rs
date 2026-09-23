//! BSON values, wire encoding, comparison, and canonical identity keys.
//!
//! This module is available with the non-default `documents` feature. It is
//! protocol-neutral: the MongoDB listener, embedded document API, and storage
//! layer all consume the same value and codec rules.

mod aggregation;
mod aggregation_expression;
mod aggregation_group;
mod aggregation_numeric;
mod aggregation_transform;
mod catalog;
mod codec;
mod command;
mod distinct;
mod error;
mod index_definition;
mod index_keys;
mod key;
mod matcher;
mod memory;
mod number;
mod options;
mod plan;
mod projection;
mod result;
mod sorting;
mod update;
mod value;

pub use aggregation::{DocumentAggregationStream, DocumentAggregator};
pub(crate) use catalog::validate_namespace;
pub use catalog::{
    DOCUMENT_CATALOG_VERSION, DOCUMENT_INDEX_FORMAT_VERSION, DOCUMENT_SCHEMA_VERSION,
    DOCUMENT_STORAGE_FORMAT_VERSION, DocumentCatalog, DocumentCollectionId,
    DocumentCollectionMetadata, DocumentCollectionOptions, DocumentDatabaseId, DocumentIndexId,
    DocumentIndexLifecycle, DocumentIndexMetadata, DocumentPlacement,
    MAX_DOCUMENT_DATABASE_NAME_BYTES, MAX_DOCUMENT_NAMESPACE_BYTES,
};
pub use codec::{
    BSON_MAX_DECODED_BYTES, BSON_MAX_DOCUMENT_BYTES, BSON_MAX_NESTING_DEPTH, BsonCodecOptions,
    DuplicateFieldPolicy, decode_document, decode_document_batch_with_options,
    decode_document_with_options, encode_document, encode_document_with_options,
};
pub use command::{
    DocumentAggregateRequest, DocumentBuildIndexRequest, DocumentCollectionExistsRequest,
    DocumentCommand, DocumentCommandKind, DocumentContinueCursorRequest, DocumentCountRequest,
    DocumentCreateCollectionRequest, DocumentCreateIndexRequest, DocumentCursorId,
    DocumentDeleteRequest, DocumentDistinctRequest, DocumentDropCollectionRequest,
    DocumentDropDatabaseRequest, DocumentDropIndexRequest, DocumentFindOneAndDeleteRequest,
    DocumentFindOneAndReplaceRequest, DocumentFindOneAndUpdateRequest, DocumentFindRequest,
    DocumentIndexRequest, DocumentInsertRequest, DocumentKillCursorRequest,
    DocumentListCollectionMetadataRequest, DocumentListCollectionsRequest,
    DocumentListDatabaseNamesRequest, DocumentListIndexMetadataRequest, DocumentListIndexesRequest,
    DocumentMutationScope, DocumentNamespace, DocumentReplaceRequest, DocumentRequest,
    DocumentRequestId, DocumentUpdateRequest, MAX_DOCUMENT_INDEX_NAME_BYTES,
};
pub use distinct::DocumentDistinct;
pub(crate) use error::DocumentWriteRollback;
pub use error::{
    BsonError, BsonErrorContext, BsonErrorKind, BsonResult, DocumentCursorError,
    DocumentIndexError, DocumentMutationError,
};
pub use index_definition::DocumentIndexDefinition;
pub(crate) use index_definition::{normalize_index_definition, normalize_index_request};
pub use index_keys::{
    DOCUMENT_INDEX_KEY_ENCODING_VERSION, DocumentIndexKey, DocumentIndexKeyGenerator,
    DocumentIndexPreparation, MAX_DOCUMENT_INDEX_KEY_BYTES, MAX_DOCUMENT_PREPARED_INDEXES,
    PreparedDocumentIndexEntries, PreparedDocumentIndexKeys,
};
pub use key::{BSON_KEY_ENCODING_VERSION, BSON_MAX_CANONICAL_KEY_BYTES, CanonicalBsonKey};
pub use matcher::{DocumentMatcher, DocumentQueryError};
pub use options::{
    DEFAULT_DOCUMENT_BATCH_SIZE, DocumentFilter, DocumentPipeline, DocumentProjection,
    DocumentReadOptions, DocumentSort, DocumentUpdate, DocumentWriteOptions,
    MAX_DOCUMENT_BATCH_SIZE, MAX_DOCUMENT_REQUEST_BYTES,
};
pub use plan::{DocumentPlan, DocumentPointPlan, DocumentScatterPlan};
pub use projection::DocumentProjector;
pub use result::{
    DocumentCursorBatch, DocumentDeleteResult, DocumentExecution, DocumentInsertResult,
    DocumentResult, DocumentResultKind, DocumentUpdateResult, DocumentUpsertedDocument,
    DocumentWriteError,
};
pub use sorting::{DocumentSortKey, DocumentSorter};
pub use update::{DocumentUpdateError, DocumentUpdater};
pub use value::{
    BsonBinary, BsonDateTime, BsonDecimal128, BsonDocument, BsonJavaScript, BsonObjectId,
    BsonRegex, BsonTimestamp, BsonUuid, BsonValue, UuidRepresentation,
};
