//! BSON values, wire encoding, comparison, and canonical identity keys.
//!
//! This module is available with the non-default `documents` feature. It is
//! protocol-neutral: the MongoDB listener, embedded document API, and storage
//! layer all consume the same value and codec rules.

mod catalog;
mod codec;
mod command;
mod error;
mod key;
mod number;
mod options;
mod plan;
mod result;
mod value;

pub(crate) use catalog::validate_namespace;
pub use catalog::{
    DOCUMENT_CATALOG_VERSION, DOCUMENT_INDEX_FORMAT_VERSION, DOCUMENT_SCHEMA_VERSION,
    DOCUMENT_STORAGE_FORMAT_VERSION, DocumentCatalog, DocumentCollectionId,
    DocumentCollectionMetadata, DocumentCollectionOptions, DocumentDatabaseId,
    DocumentIndexLifecycle, DocumentIndexMetadata, DocumentPlacement,
    MAX_DOCUMENT_DATABASE_NAME_BYTES, MAX_DOCUMENT_NAMESPACE_BYTES,
};
pub use codec::{
    BSON_MAX_DECODED_BYTES, BSON_MAX_DOCUMENT_BYTES, BSON_MAX_NESTING_DEPTH, BsonCodecOptions,
    DuplicateFieldPolicy, decode_document, decode_document_with_options, encode_document,
    encode_document_with_options,
};
pub use command::{
    DocumentAggregateRequest, DocumentCommand, DocumentCommandKind, DocumentContinueCursorRequest,
    DocumentCountRequest, DocumentCreateCollectionRequest, DocumentCreateIndexRequest,
    DocumentCursorId, DocumentDeleteRequest, DocumentDistinctRequest,
    DocumentDropCollectionRequest, DocumentDropIndexRequest, DocumentFindRequest,
    DocumentIndexRequest, DocumentInsertRequest, DocumentKillCursorRequest,
    DocumentListCollectionsRequest, DocumentListIndexesRequest, DocumentMutationScope,
    DocumentNamespace, DocumentReplaceRequest, DocumentRequest, DocumentRequestId,
    DocumentUpdateRequest, MAX_DOCUMENT_INDEX_NAME_BYTES,
};
pub use error::{BsonError, BsonErrorContext, BsonErrorKind, BsonResult};
pub use key::{BSON_KEY_ENCODING_VERSION, BSON_MAX_CANONICAL_KEY_BYTES, CanonicalBsonKey};
pub use options::{
    DEFAULT_DOCUMENT_BATCH_SIZE, DocumentFilter, DocumentPipeline, DocumentProjection,
    DocumentReadOptions, DocumentSort, DocumentUpdate, DocumentWriteOptions,
    MAX_DOCUMENT_BATCH_SIZE, MAX_DOCUMENT_REQUEST_BYTES,
};
pub use plan::{DocumentPlan, DocumentPointPlan, DocumentScatterPlan};
pub use result::{
    DocumentCursorBatch, DocumentDeleteResult, DocumentExecution, DocumentInsertResult,
    DocumentResult, DocumentResultKind, DocumentUpdateResult,
};
pub use value::{
    BsonBinary, BsonDateTime, BsonDecimal128, BsonDocument, BsonJavaScript, BsonObjectId,
    BsonRegex, BsonTimestamp, BsonUuid, BsonValue, UuidRepresentation,
};
