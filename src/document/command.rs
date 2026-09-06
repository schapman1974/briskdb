//! Owned commands accepted by the protocol-neutral document engine.

use std::fmt;

use crate::core::{EngineError, EngineErrorKind, EngineResult, RequestContext};

use super::{
    BsonDocument, BsonErrorContext, DocumentCollectionOptions, DocumentFilter, DocumentPipeline,
    DocumentReadOptions, DocumentUpdate, DocumentWriteOptions, MAX_DOCUMENT_BATCH_SIZE,
    MAX_DOCUMENT_REQUEST_BYTES, encode_document, validate_namespace,
};

/// Maximum UTF-8 byte length of a user-defined document index name.
pub const MAX_DOCUMENT_INDEX_NAME_BYTES: usize = 255;

fn invalid_argument(message: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::InvalidArgument, message)
}

fn validate_document(document: &BsonDocument) -> EngineResult<()> {
    encode_document(document)
        .map(|_| ())
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))
}

fn validate_database_name(database: &str) -> EngineResult<()> {
    validate_namespace(database, "_")
}

fn validate_index_name(name: &str) -> EngineResult<()> {
    if name.is_empty() || name.len() > MAX_DOCUMENT_INDEX_NAME_BYTES || name.contains('\0') {
        return Err(invalid_argument(format!(
            "document index name must contain 1 to {MAX_DOCUMENT_INDEX_NAME_BYTES} UTF-8 bytes and no NUL"
        )));
    }
    Ok(())
}

fn validate_field_path(path: &str) -> EngineResult<()> {
    if path.is_empty() || path.contains('\0') {
        return Err(invalid_argument(
            "document field path must be nonempty and contain no NUL",
        ));
    }
    Ok(())
}

/// Exact, case-sensitive identity of one logical document collection.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DocumentNamespace {
    database: String,
    collection: String,
}

impl DocumentNamespace {
    /// Validate and own one database and collection name.
    pub fn new(database: impl Into<String>, collection: impl Into<String>) -> EngineResult<Self> {
        let database = database.into();
        let collection = collection.into();
        validate_namespace(&database, &collection)?;
        Ok(Self {
            database,
            collection,
        })
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn collection(&self) -> &str {
        &self.collection
    }

    pub fn into_parts(self) -> (String, String) {
        (self.database, self.collection)
    }
}

impl fmt::Display for DocumentNamespace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}", self.database, self.collection)
    }
}

/// Caller-owned identity of one document request.
///
/// A retry can reuse the same identity for logs and observability. This type
/// alone does not promise write idempotency.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DocumentRequestId([u8; 16]);

impl DocumentRequestId {
    /// Validate a nonzero 128-bit request identity.
    pub fn new(value: [u8; 16]) -> EngineResult<Self> {
        if value == [0; 16] {
            return Err(invalid_argument(
                "document request IDs must not be all zero",
            ));
        }
        Ok(Self(value))
    }

    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for DocumentRequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("DocumentRequestId")
            .field(&format_args!("{:02x}{:02x}…", self.0[0], self.0[1]))
            .finish()
    }
}

/// Opaque identity of a live engine-owned document cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DocumentCursorId(u64);

impl DocumentCursorId {
    pub fn new(value: u64) -> EngineResult<Self> {
        if value == 0 {
            return Err(invalid_argument("document cursor IDs must be positive"));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Whether a mutation stops after its first match or visits every match.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DocumentMutationScope {
    One,
    Many,
}

/// Owned declaration for one document index.
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentIndexRequest {
    keys: BsonDocument,
    name: Option<String>,
    unique: bool,
    sparse: bool,
    partial_filter: Option<DocumentFilter>,
}

impl DocumentIndexRequest {
    /// Validate and own an ordered BSON key specification.
    pub fn new(keys: BsonDocument) -> EngineResult<Self> {
        validate_document(&keys)?;
        Ok(Self {
            keys,
            name: None,
            unique: false,
            sparse: false,
            partial_filter: None,
        })
    }

    pub fn with_name(mut self, name: impl Into<String>) -> EngineResult<Self> {
        let name = name.into();
        validate_index_name(&name)?;
        self.name = Some(name);
        Ok(self)
    }

    #[must_use]
    pub const fn with_unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }

    #[must_use]
    pub const fn with_sparse(mut self, sparse: bool) -> Self {
        self.sparse = sparse;
        self
    }

    #[must_use]
    pub fn with_partial_filter(mut self, filter: DocumentFilter) -> Self {
        self.partial_filter = Some(filter);
        self
    }

    pub const fn keys(&self) -> &BsonDocument {
        &self.keys
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub const fn unique(&self) -> bool {
        self.unique
    }

    pub const fn sparse(&self) -> bool {
        self.sparse
    }

    pub const fn partial_filter(&self) -> Option<&DocumentFilter> {
        self.partial_filter.as_ref()
    }

    pub fn into_parts(
        self,
    ) -> (
        BsonDocument,
        Option<String>,
        bool,
        bool,
        Option<DocumentFilter>,
    ) {
        (
            self.keys,
            self.name,
            self.unique,
            self.sparse,
            self.partial_filter,
        )
    }
}

impl fmt::Debug for DocumentIndexRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentIndexRequest")
            .field("keys", &"<redacted>")
            .field("name", &self.name)
            .field("unique", &self.unique)
            .field("sparse", &self.sparse)
            .field(
                "partial_filter",
                &self.partial_filter.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Request to create one collection if its namespace is absent.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentCreateCollectionRequest {
    namespace: DocumentNamespace,
    options: DocumentCollectionOptions,
    write_options: DocumentWriteOptions,
}

impl DocumentCreateCollectionRequest {
    pub fn new(
        namespace: DocumentNamespace,
        options: DocumentCollectionOptions,
        write_options: DocumentWriteOptions,
    ) -> Self {
        Self {
            namespace,
            options,
            write_options,
        }
    }

    pub const fn namespace(&self) -> &DocumentNamespace {
        &self.namespace
    }

    pub const fn options(&self) -> &DocumentCollectionOptions {
        &self.options
    }

    pub const fn write_options(&self) -> DocumentWriteOptions {
        self.write_options
    }

    pub fn into_parts(
        self,
    ) -> (
        DocumentNamespace,
        DocumentCollectionOptions,
        DocumentWriteOptions,
    ) {
        (self.namespace, self.options, self.write_options)
    }
}

impl fmt::Debug for DocumentCreateCollectionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentCreateCollectionRequest")
            .field("namespace", &self.namespace)
            .field("options", &"<redacted>")
            .field("write_options", &self.write_options)
            .finish()
    }
}

/// Request to list collections in one validated database name.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentListCollectionsRequest {
    database: String,
    read_options: DocumentReadOptions,
}

impl DocumentListCollectionsRequest {
    pub fn new(
        database: impl Into<String>,
        read_options: DocumentReadOptions,
    ) -> EngineResult<Self> {
        let database = database.into();
        validate_database_name(&database)?;
        Ok(Self {
            database,
            read_options,
        })
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub const fn read_options(&self) -> &DocumentReadOptions {
        &self.read_options
    }

    pub fn into_parts(self) -> (String, DocumentReadOptions) {
        (self.database, self.read_options)
    }
}

macro_rules! namespace_options_request {
    (
        $(#[$meta:meta])*
        $name:ident,
        $options_ty:ty,
        $field:ident,
        $getter:ident
    ) => {
        $(#[$meta])*
        #[non_exhaustive]
        #[derive(Clone, PartialEq, Eq)]
        pub struct $name {
            namespace: DocumentNamespace,
            $field: $options_ty,
        }

        impl $name {
            pub fn new(namespace: DocumentNamespace, $field: $options_ty) -> Self {
                Self { namespace, $field }
            }

            pub const fn namespace(&self) -> &DocumentNamespace {
                &self.namespace
            }

            pub const fn $getter(&self) -> &$options_ty {
                &self.$field
            }

            pub fn into_parts(self) -> (DocumentNamespace, $options_ty) {
                (self.namespace, self.$field)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("namespace", &self.namespace)
                    .field(stringify!($field), &self.$field)
                    .finish()
            }
        }
    };
}

namespace_options_request!(
    /// Request to drop one collection.
    DocumentDropCollectionRequest,
    DocumentWriteOptions,
    write_options,
    write_options
);
namespace_options_request!(
    /// Request to list index metadata for one collection.
    DocumentListIndexesRequest,
    DocumentReadOptions,
    read_options,
    read_options
);

macro_rules! filter_read_request {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[non_exhaustive]
        #[derive(Clone, PartialEq, Eq)]
        pub struct $name {
            namespace: DocumentNamespace,
            filter: DocumentFilter,
            read_options: DocumentReadOptions,
        }

        impl $name {
            pub fn new(
                namespace: DocumentNamespace,
                filter: DocumentFilter,
                read_options: DocumentReadOptions,
            ) -> Self {
                Self {
                    namespace,
                    filter,
                    read_options,
                }
            }

            pub const fn namespace(&self) -> &DocumentNamespace {
                &self.namespace
            }

            pub const fn filter(&self) -> &DocumentFilter {
                &self.filter
            }

            pub const fn read_options(&self) -> &DocumentReadOptions {
                &self.read_options
            }

            pub fn into_parts(
                self,
            ) -> (DocumentNamespace, DocumentFilter, DocumentReadOptions) {
                (self.namespace, self.filter, self.read_options)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("namespace", &self.namespace)
                    .field("filter", &"<redacted>")
                    .field("read_options", &self.read_options)
                    .finish()
            }
        }
    };
}

filter_read_request!(
    /// Request to read an ordered batch of matching documents.
    DocumentFindRequest
);
filter_read_request!(
    /// Request to count matching documents.
    DocumentCountRequest
);

/// Request to run an owned aggregation pipeline.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentAggregateRequest {
    namespace: DocumentNamespace,
    pipeline: DocumentPipeline,
    read_options: DocumentReadOptions,
}

impl DocumentAggregateRequest {
    pub fn new(
        namespace: DocumentNamespace,
        pipeline: DocumentPipeline,
        read_options: DocumentReadOptions,
    ) -> EngineResult<Self> {
        let mut encoded_bytes = 0_usize;
        for stage in pipeline.stages() {
            add_request_payload_bytes(&mut encoded_bytes, stage)?;
        }
        if let Some(projection) = read_options.projection() {
            add_request_payload_bytes(&mut encoded_bytes, projection.document())?;
        }
        if let Some(sort) = read_options.sort() {
            add_request_payload_bytes(&mut encoded_bytes, sort.document())?;
        }
        Ok(Self {
            namespace,
            pipeline,
            read_options,
        })
    }

    pub const fn namespace(&self) -> &DocumentNamespace {
        &self.namespace
    }

    pub const fn pipeline(&self) -> &DocumentPipeline {
        &self.pipeline
    }

    pub const fn read_options(&self) -> &DocumentReadOptions {
        &self.read_options
    }

    pub fn into_parts(self) -> (DocumentNamespace, DocumentPipeline, DocumentReadOptions) {
        (self.namespace, self.pipeline, self.read_options)
    }
}

fn add_request_payload_bytes(total: &mut usize, document: &BsonDocument) -> EngineResult<()> {
    let encoded = encode_document(document)
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    add_request_payload_len(total, encoded.len())
}

fn add_request_payload_len(total: &mut usize, length: usize) -> EngineResult<()> {
    *total = total
        .checked_add(length)
        .filter(|bytes| *bytes <= MAX_DOCUMENT_REQUEST_BYTES)
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!("document request exceeds {MAX_DOCUMENT_REQUEST_BYTES} encoded BSON bytes"),
            )
        })?;
    Ok(())
}

impl fmt::Debug for DocumentAggregateRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentAggregateRequest")
            .field("namespace", &self.namespace)
            .field("pipeline", &"<redacted>")
            .field("read_options", &self.read_options)
            .finish()
    }
}

/// Request to return distinct values for one field path.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentDistinctRequest {
    namespace: DocumentNamespace,
    field: String,
    filter: DocumentFilter,
    read_options: DocumentReadOptions,
}

impl DocumentDistinctRequest {
    pub fn new(
        namespace: DocumentNamespace,
        field: impl Into<String>,
        filter: DocumentFilter,
        read_options: DocumentReadOptions,
    ) -> EngineResult<Self> {
        let field = field.into();
        validate_field_path(&field)?;
        Ok(Self {
            namespace,
            field,
            filter,
            read_options,
        })
    }

    pub const fn namespace(&self) -> &DocumentNamespace {
        &self.namespace
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub const fn filter(&self) -> &DocumentFilter {
        &self.filter
    }

    pub const fn read_options(&self) -> &DocumentReadOptions {
        &self.read_options
    }

    pub fn into_parts(
        self,
    ) -> (
        DocumentNamespace,
        String,
        DocumentFilter,
        DocumentReadOptions,
    ) {
        (self.namespace, self.field, self.filter, self.read_options)
    }
}

impl fmt::Debug for DocumentDistinctRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentDistinctRequest")
            .field("namespace", &self.namespace)
            .field("field", &self.field)
            .field("filter", &"<redacted>")
            .field("read_options", &self.read_options)
            .finish()
    }
}

/// Request to insert one or more BSON documents in input order.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentInsertRequest {
    namespace: DocumentNamespace,
    documents: Box<[BsonDocument]>,
    write_options: DocumentWriteOptions,
}

impl DocumentInsertRequest {
    pub fn new(
        namespace: DocumentNamespace,
        documents: impl Into<Vec<BsonDocument>>,
        write_options: DocumentWriteOptions,
    ) -> EngineResult<Self> {
        let documents = documents.into();
        if documents.is_empty() {
            return Err(invalid_argument(
                "document insert request must contain at least one document",
            ));
        }
        if documents.len() as u64 > MAX_DOCUMENT_BATCH_SIZE {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!(
                    "document insert request must not exceed {MAX_DOCUMENT_BATCH_SIZE} documents"
                ),
            ));
        }
        let mut encoded_bytes = 0_usize;
        for document in &documents {
            let encoded = encode_document(document)
                .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
            encoded_bytes = encoded_bytes
                .checked_add(encoded.len())
                .filter(|bytes| *bytes <= MAX_DOCUMENT_REQUEST_BYTES)
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::LimitExceeded,
                        format!(
                            "document insert request exceeds {MAX_DOCUMENT_REQUEST_BYTES} encoded BSON bytes"
                        ),
                    )
                })?;
        }
        Ok(Self {
            namespace,
            documents: documents.into_boxed_slice(),
            write_options,
        })
    }

    pub const fn namespace(&self) -> &DocumentNamespace {
        &self.namespace
    }

    pub fn documents(&self) -> &[BsonDocument] {
        &self.documents
    }

    pub const fn write_options(&self) -> DocumentWriteOptions {
        self.write_options
    }

    pub fn into_parts(self) -> (DocumentNamespace, Vec<BsonDocument>, DocumentWriteOptions) {
        (
            self.namespace,
            self.documents.into_vec(),
            self.write_options,
        )
    }
}

impl fmt::Debug for DocumentInsertRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentInsertRequest")
            .field("namespace", &self.namespace)
            .field("document_count", &self.documents.len())
            .field("documents", &"<redacted>")
            .field("write_options", &self.write_options)
            .finish()
    }
}

/// Request to apply an update expression to one or many matching documents.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentUpdateRequest {
    namespace: DocumentNamespace,
    filter: DocumentFilter,
    update: DocumentUpdate,
    scope: DocumentMutationScope,
    write_options: DocumentWriteOptions,
}

impl DocumentUpdateRequest {
    pub fn new(
        namespace: DocumentNamespace,
        filter: DocumentFilter,
        update: DocumentUpdate,
        scope: DocumentMutationScope,
        write_options: DocumentWriteOptions,
    ) -> Self {
        Self {
            namespace,
            filter,
            update,
            scope,
            write_options,
        }
    }

    pub const fn namespace(&self) -> &DocumentNamespace {
        &self.namespace
    }

    pub const fn filter(&self) -> &DocumentFilter {
        &self.filter
    }

    pub const fn update(&self) -> &DocumentUpdate {
        &self.update
    }

    pub const fn scope(&self) -> DocumentMutationScope {
        self.scope
    }

    pub const fn write_options(&self) -> DocumentWriteOptions {
        self.write_options
    }

    pub fn into_parts(
        self,
    ) -> (
        DocumentNamespace,
        DocumentFilter,
        DocumentUpdate,
        DocumentMutationScope,
        DocumentWriteOptions,
    ) {
        (
            self.namespace,
            self.filter,
            self.update,
            self.scope,
            self.write_options,
        )
    }
}

impl fmt::Debug for DocumentUpdateRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentUpdateRequest")
            .field("namespace", &self.namespace)
            .field("filter", &"<redacted>")
            .field("update", &"<redacted>")
            .field("scope", &self.scope)
            .field("write_options", &self.write_options)
            .finish()
    }
}

/// Request to replace the first matching document.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentReplaceRequest {
    namespace: DocumentNamespace,
    filter: DocumentFilter,
    replacement: BsonDocument,
    write_options: DocumentWriteOptions,
}

impl DocumentReplaceRequest {
    pub fn new(
        namespace: DocumentNamespace,
        filter: DocumentFilter,
        replacement: BsonDocument,
        write_options: DocumentWriteOptions,
    ) -> EngineResult<Self> {
        validate_document(&replacement)?;
        Ok(Self {
            namespace,
            filter,
            replacement,
            write_options,
        })
    }

    pub const fn namespace(&self) -> &DocumentNamespace {
        &self.namespace
    }

    pub const fn filter(&self) -> &DocumentFilter {
        &self.filter
    }

    pub const fn replacement(&self) -> &BsonDocument {
        &self.replacement
    }

    pub const fn write_options(&self) -> DocumentWriteOptions {
        self.write_options
    }

    pub fn into_parts(
        self,
    ) -> (
        DocumentNamespace,
        DocumentFilter,
        BsonDocument,
        DocumentWriteOptions,
    ) {
        (
            self.namespace,
            self.filter,
            self.replacement,
            self.write_options,
        )
    }
}

impl fmt::Debug for DocumentReplaceRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentReplaceRequest")
            .field("namespace", &self.namespace)
            .field("filter", &"<redacted>")
            .field("replacement", &"<redacted>")
            .field("write_options", &self.write_options)
            .finish()
    }
}

/// Request to delete one or many matching documents.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentDeleteRequest {
    namespace: DocumentNamespace,
    filter: DocumentFilter,
    scope: DocumentMutationScope,
    write_options: DocumentWriteOptions,
}

impl DocumentDeleteRequest {
    pub fn new(
        namespace: DocumentNamespace,
        filter: DocumentFilter,
        scope: DocumentMutationScope,
        write_options: DocumentWriteOptions,
    ) -> Self {
        Self {
            namespace,
            filter,
            scope,
            write_options,
        }
    }

    pub const fn namespace(&self) -> &DocumentNamespace {
        &self.namespace
    }

    pub const fn filter(&self) -> &DocumentFilter {
        &self.filter
    }

    pub const fn scope(&self) -> DocumentMutationScope {
        self.scope
    }

    pub const fn write_options(&self) -> DocumentWriteOptions {
        self.write_options
    }

    pub fn into_parts(
        self,
    ) -> (
        DocumentNamespace,
        DocumentFilter,
        DocumentMutationScope,
        DocumentWriteOptions,
    ) {
        (self.namespace, self.filter, self.scope, self.write_options)
    }
}

impl fmt::Debug for DocumentDeleteRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentDeleteRequest")
            .field("namespace", &self.namespace)
            .field("filter", &"<redacted>")
            .field("scope", &self.scope)
            .field("write_options", &self.write_options)
            .finish()
    }
}

/// Request to declare one index.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentCreateIndexRequest {
    namespace: DocumentNamespace,
    index: DocumentIndexRequest,
    write_options: DocumentWriteOptions,
}

impl DocumentCreateIndexRequest {
    pub fn new(
        namespace: DocumentNamespace,
        index: DocumentIndexRequest,
        write_options: DocumentWriteOptions,
    ) -> Self {
        Self {
            namespace,
            index,
            write_options,
        }
    }

    pub const fn namespace(&self) -> &DocumentNamespace {
        &self.namespace
    }

    pub const fn index(&self) -> &DocumentIndexRequest {
        &self.index
    }

    pub const fn write_options(&self) -> DocumentWriteOptions {
        self.write_options
    }

    pub fn into_parts(
        self,
    ) -> (
        DocumentNamespace,
        DocumentIndexRequest,
        DocumentWriteOptions,
    ) {
        (self.namespace, self.index, self.write_options)
    }
}

impl fmt::Debug for DocumentCreateIndexRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentCreateIndexRequest")
            .field("namespace", &self.namespace)
            .field("index", &self.index)
            .field("write_options", &self.write_options)
            .finish()
    }
}

/// Request to drop one named index.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentDropIndexRequest {
    namespace: DocumentNamespace,
    name: String,
    write_options: DocumentWriteOptions,
}

impl DocumentDropIndexRequest {
    pub fn new(
        namespace: DocumentNamespace,
        name: impl Into<String>,
        write_options: DocumentWriteOptions,
    ) -> EngineResult<Self> {
        let name = name.into();
        validate_index_name(&name)?;
        Ok(Self {
            namespace,
            name,
            write_options,
        })
    }

    pub const fn namespace(&self) -> &DocumentNamespace {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn write_options(&self) -> DocumentWriteOptions {
        self.write_options
    }

    pub fn into_parts(self) -> (DocumentNamespace, String, DocumentWriteOptions) {
        (self.namespace, self.name, self.write_options)
    }
}

/// Request the next batch from one cursor.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentContinueCursorRequest {
    namespace: DocumentNamespace,
    cursor_id: DocumentCursorId,
    read_options: DocumentReadOptions,
}

impl DocumentContinueCursorRequest {
    pub fn new(
        namespace: DocumentNamespace,
        cursor_id: DocumentCursorId,
        read_options: DocumentReadOptions,
    ) -> Self {
        Self {
            namespace,
            cursor_id,
            read_options,
        }
    }

    pub const fn namespace(&self) -> &DocumentNamespace {
        &self.namespace
    }

    pub const fn cursor_id(&self) -> DocumentCursorId {
        self.cursor_id
    }

    pub const fn read_options(&self) -> &DocumentReadOptions {
        &self.read_options
    }

    pub fn into_parts(self) -> (DocumentNamespace, DocumentCursorId, DocumentReadOptions) {
        (self.namespace, self.cursor_id, self.read_options)
    }
}

/// Request to discard one live cursor.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentKillCursorRequest {
    namespace: DocumentNamespace,
    cursor_id: DocumentCursorId,
    write_options: DocumentWriteOptions,
}

impl DocumentKillCursorRequest {
    pub fn new(
        namespace: DocumentNamespace,
        cursor_id: DocumentCursorId,
        write_options: DocumentWriteOptions,
    ) -> Self {
        Self {
            namespace,
            cursor_id,
            write_options,
        }
    }

    pub const fn namespace(&self) -> &DocumentNamespace {
        &self.namespace
    }

    pub const fn cursor_id(&self) -> DocumentCursorId {
        self.cursor_id
    }

    pub const fn write_options(&self) -> DocumentWriteOptions {
        self.write_options
    }

    pub fn into_parts(self) -> (DocumentNamespace, DocumentCursorId, DocumentWriteOptions) {
        (self.namespace, self.cursor_id, self.write_options)
    }
}

/// Stable, payload-free classification of a document command.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DocumentCommandKind {
    CreateCollection,
    ListCollections,
    DropCollection,
    Find,
    Aggregate,
    Count,
    Distinct,
    Insert,
    Update,
    Replace,
    Delete,
    CreateIndex,
    DropIndex,
    ListIndexes,
    ContinueCursor,
    KillCursor,
}

/// One protocol-neutral document command with all payloads owned.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub enum DocumentCommand {
    CreateCollection(DocumentCreateCollectionRequest),
    ListCollections(DocumentListCollectionsRequest),
    DropCollection(DocumentDropCollectionRequest),
    Find(DocumentFindRequest),
    Aggregate(DocumentAggregateRequest),
    Count(DocumentCountRequest),
    Distinct(DocumentDistinctRequest),
    Insert(DocumentInsertRequest),
    Update(DocumentUpdateRequest),
    Replace(DocumentReplaceRequest),
    Delete(DocumentDeleteRequest),
    CreateIndex(DocumentCreateIndexRequest),
    DropIndex(DocumentDropIndexRequest),
    ListIndexes(DocumentListIndexesRequest),
    ContinueCursor(DocumentContinueCursorRequest),
    KillCursor(DocumentKillCursorRequest),
}

impl DocumentCommand {
    pub const fn kind(&self) -> DocumentCommandKind {
        match self {
            Self::CreateCollection(_) => DocumentCommandKind::CreateCollection,
            Self::ListCollections(_) => DocumentCommandKind::ListCollections,
            Self::DropCollection(_) => DocumentCommandKind::DropCollection,
            Self::Find(_) => DocumentCommandKind::Find,
            Self::Aggregate(_) => DocumentCommandKind::Aggregate,
            Self::Count(_) => DocumentCommandKind::Count,
            Self::Distinct(_) => DocumentCommandKind::Distinct,
            Self::Insert(_) => DocumentCommandKind::Insert,
            Self::Update(_) => DocumentCommandKind::Update,
            Self::Replace(_) => DocumentCommandKind::Replace,
            Self::Delete(_) => DocumentCommandKind::Delete,
            Self::CreateIndex(_) => DocumentCommandKind::CreateIndex,
            Self::DropIndex(_) => DocumentCommandKind::DropIndex,
            Self::ListIndexes(_) => DocumentCommandKind::ListIndexes,
            Self::ContinueCursor(_) => DocumentCommandKind::ContinueCursor,
            Self::KillCursor(_) => DocumentCommandKind::KillCursor,
        }
    }
}

impl fmt::Debug for DocumentCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentCommand")
            .field("kind", &self.kind())
            .field("payload", &"<redacted>")
            .finish()
    }
}

/// One document command together with its identity, cancellation, deadline,
/// and result-limit controls.
#[derive(Clone)]
pub struct DocumentRequest {
    request_id: DocumentRequestId,
    context: RequestContext,
    command: DocumentCommand,
}

impl DocumentRequest {
    pub fn new(
        request_id: DocumentRequestId,
        context: RequestContext,
        command: DocumentCommand,
    ) -> Self {
        Self {
            request_id,
            context,
            command,
        }
    }

    pub const fn request_id(&self) -> DocumentRequestId {
        self.request_id
    }

    pub const fn context(&self) -> &RequestContext {
        &self.context
    }

    pub const fn command(&self) -> &DocumentCommand {
        &self.command
    }

    pub fn into_parts(self) -> (DocumentRequestId, RequestContext, DocumentCommand) {
        (self.request_id, self.context, self.command)
    }
}

impl fmt::Debug for DocumentRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentRequest")
            .field("request_id", &self.request_id)
            .field("context", &self.context)
            .field("command_kind", &self.command.kind())
            .field("command", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::super::BsonValue;
    use super::*;

    fn namespace() -> DocumentNamespace {
        DocumentNamespace::new("database", "records").unwrap()
    }

    fn secret_document() -> BsonDocument {
        BsonDocument::from_entries([("token", BsonValue::from("private-token"))]).unwrap()
    }

    #[test]
    fn namespace_and_request_ids_are_validated() {
        assert!(DocumentNamespace::new("", "records").is_err());
        assert!(DocumentNamespace::new("database", "").is_err());
        let namespace = namespace();
        assert_eq!(namespace.to_string(), "database.records");
        assert!(DocumentRequestId::new([0; 16]).is_err());
        assert!(DocumentCursorId::new(0).is_err());
        assert_eq!(DocumentCursorId::new(7).unwrap().get(), 7);
    }

    #[test]
    fn request_owns_controls_and_redacts_command_payload() {
        fn assert_owned<T: Clone + Send + Sync + 'static>() {}
        assert_owned::<DocumentRequest>();

        let insert = DocumentInsertRequest::new(
            namespace(),
            vec![secret_document()],
            DocumentWriteOptions::new(),
        )
        .unwrap();
        let request = DocumentRequest::new(
            DocumentRequestId::new([9; 16]).unwrap(),
            RequestContext::new(),
            DocumentCommand::Insert(insert),
        );
        assert_eq!(request.command().kind(), DocumentCommandKind::Insert);
        let debug = format!("{request:?} {:?}", request.command());
        assert!(debug.contains("Insert"));
        assert!(!debug.contains("private-token"));
    }

    #[test]
    fn aggregate_request_payload_accounting_is_whole_request_bounded() {
        let mut total = 0;
        add_request_payload_len(&mut total, MAX_DOCUMENT_REQUEST_BYTES).unwrap();
        assert_eq!(total, MAX_DOCUMENT_REQUEST_BYTES);
        assert!(add_request_payload_len(&mut total, 1).is_err());
    }

    #[test]
    fn typed_payloads_round_trip_through_request_parts() {
        let filter = DocumentFilter::new(secret_document()).unwrap();
        let update = DocumentUpdate::new(secret_document()).unwrap();
        let command = DocumentUpdateRequest::new(
            namespace(),
            filter,
            update,
            DocumentMutationScope::Many,
            DocumentWriteOptions::new().with_upsert(true),
        );
        let (_, filter, update, scope, options) = command.into_parts();
        assert_eq!(
            filter.document().get_first("token"),
            Some(&BsonValue::from("private-token"))
        );
        assert_eq!(
            update.document().get_first("token"),
            Some(&BsonValue::from("private-token"))
        );
        assert_eq!(scope, DocumentMutationScope::Many);
        assert!(options.upsert());
    }

    #[test]
    fn insert_batch_and_index_names_are_bounded() {
        assert!(
            DocumentInsertRequest::new(namespace(), Vec::new(), DocumentWriteOptions::new())
                .is_err()
        );
        assert!(
            DocumentIndexRequest::new(secret_document())
                .unwrap()
                .with_name("")
                .is_err()
        );
        assert!(
            DocumentDropIndexRequest::new(namespace(), "bad\0name", DocumentWriteOptions::new())
                .is_err()
        );
    }
}
