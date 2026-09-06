//! BSON-preserving results returned by the protocol-neutral document engine.

use std::fmt;

use crate::core::{EngineError, EngineErrorKind, EngineResult};

use super::{
    BsonDocument, BsonErrorContext, BsonValue, CanonicalBsonKey, DocumentCollectionMetadata,
    DocumentCursorId, DocumentIndexMetadata, DocumentNamespace, DocumentPlan, DocumentRequestId,
    MAX_DOCUMENT_BATCH_SIZE, encode_document,
};

fn validate_result_document(document: &BsonDocument) -> EngineResult<()> {
    encode_document(document)
        .map(|_| ())
        .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))
}

/// One ordered batch of independently owned BSON documents.
///
/// `cursor_id == None` means this batch exhausted the cursor. A live cursor ID
/// may accompany an empty batch.
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentCursorBatch {
    namespace: DocumentNamespace,
    cursor_id: Option<DocumentCursorId>,
    documents: Box<[BsonDocument]>,
}

impl DocumentCursorBatch {
    pub fn new(
        namespace: DocumentNamespace,
        cursor_id: Option<DocumentCursorId>,
        documents: impl Into<Vec<BsonDocument>>,
    ) -> EngineResult<Self> {
        let documents = documents.into();
        if documents.len() as u64 > MAX_DOCUMENT_BATCH_SIZE {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!("document cursor batch exceeds {MAX_DOCUMENT_BATCH_SIZE} documents"),
            ));
        }
        for document in &documents {
            validate_result_document(document)?;
        }
        Ok(Self {
            namespace,
            cursor_id,
            documents: documents.into_boxed_slice(),
        })
    }

    pub(crate) fn from_validated(
        namespace: DocumentNamespace,
        cursor_id: Option<DocumentCursorId>,
        documents: Vec<BsonDocument>,
    ) -> Self {
        debug_assert!(documents.len() as u64 <= MAX_DOCUMENT_BATCH_SIZE);
        Self {
            namespace,
            cursor_id,
            documents: documents.into_boxed_slice(),
        }
    }

    pub const fn namespace(&self) -> &DocumentNamespace {
        &self.namespace
    }

    pub const fn cursor_id(&self) -> Option<DocumentCursorId> {
        self.cursor_id
    }

    pub fn documents(&self) -> &[BsonDocument] {
        &self.documents
    }

    pub const fn is_exhausted(&self) -> bool {
        self.cursor_id.is_none()
    }

    pub fn into_parts(
        self,
    ) -> (
        DocumentNamespace,
        Option<DocumentCursorId>,
        Vec<BsonDocument>,
    ) {
        (self.namespace, self.cursor_id, self.documents.into_vec())
    }
}

impl fmt::Debug for DocumentCursorBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentCursorBatch")
            .field("namespace", &self.namespace)
            .field("cursor_id", &self.cursor_id)
            .field("document_count", &self.documents.len())
            .field("documents", &"<redacted>")
            .finish()
    }
}

/// Result of inserting one or more documents in input order.
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentInsertResult {
    inserted_ids: Box<[BsonValue]>,
    acknowledged: bool,
}

impl DocumentInsertResult {
    pub fn new(inserted_ids: impl Into<Vec<BsonValue>>) -> EngineResult<Self> {
        let inserted_ids = inserted_ids.into();
        if inserted_ids.is_empty() {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "document insert result must contain at least one inserted ID",
            ));
        }
        if inserted_ids.len() as u64 > MAX_DOCUMENT_BATCH_SIZE {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!("document insert result exceeds {MAX_DOCUMENT_BATCH_SIZE} IDs"),
            ));
        }
        for id in &inserted_ids {
            CanonicalBsonKey::encode(id)
                .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
        }
        Ok(Self {
            inserted_ids: inserted_ids.into_boxed_slice(),
            acknowledged: true,
        })
    }

    pub(crate) fn from_validated(inserted_ids: Vec<BsonValue>) -> Self {
        debug_assert!(!inserted_ids.is_empty());
        debug_assert!(inserted_ids.len() as u64 <= MAX_DOCUMENT_BATCH_SIZE);
        Self {
            inserted_ids: inserted_ids.into_boxed_slice(),
            acknowledged: true,
        }
    }

    pub fn inserted_ids(&self) -> &[BsonValue] {
        &self.inserted_ids
    }

    pub const fn acknowledged(&self) -> bool {
        self.acknowledged
    }

    pub fn into_inserted_ids(self) -> Vec<BsonValue> {
        self.inserted_ids.into_vec()
    }
}

impl fmt::Debug for DocumentInsertResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentInsertResult")
            .field("inserted_count", &self.inserted_ids.len())
            .field("inserted_ids", &"<redacted>")
            .field("acknowledged", &self.acknowledged)
            .finish()
    }
}

/// Result of one logical update or replacement command.
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentUpdateResult {
    matched_count: u64,
    modified_count: u64,
    upserted_id: Option<BsonValue>,
    acknowledged: bool,
}

impl DocumentUpdateResult {
    pub fn new(
        matched_count: u64,
        modified_count: u64,
        upserted_id: Option<BsonValue>,
    ) -> EngineResult<Self> {
        if modified_count > matched_count {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "document modified count cannot exceed matched count",
            ));
        }
        if let Some(id) = &upserted_id {
            CanonicalBsonKey::encode(id)
                .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
        }
        Ok(Self {
            matched_count,
            modified_count,
            upserted_id,
            acknowledged: true,
        })
    }

    pub const fn matched_count(&self) -> u64 {
        self.matched_count
    }

    pub const fn modified_count(&self) -> u64 {
        self.modified_count
    }

    pub const fn upserted_id(&self) -> Option<&BsonValue> {
        self.upserted_id.as_ref()
    }

    pub const fn acknowledged(&self) -> bool {
        self.acknowledged
    }

    pub fn into_parts(self) -> (u64, u64, Option<BsonValue>) {
        (self.matched_count, self.modified_count, self.upserted_id)
    }
}

impl fmt::Debug for DocumentUpdateResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentUpdateResult")
            .field("matched_count", &self.matched_count)
            .field("modified_count", &self.modified_count)
            .field(
                "upserted_id",
                &self.upserted_id.as_ref().map(|_| "<redacted>"),
            )
            .field("acknowledged", &self.acknowledged)
            .finish()
    }
}

/// Result of one logical delete command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DocumentDeleteResult {
    deleted_count: u64,
    acknowledged: bool,
}

impl DocumentDeleteResult {
    pub const fn new(deleted_count: u64) -> Self {
        Self {
            deleted_count,
            acknowledged: true,
        }
    }

    pub const fn deleted_count(self) -> u64 {
        self.deleted_count
    }

    pub const fn acknowledged(self) -> bool {
        self.acknowledged
    }
}

/// Payload-free classification of a document result.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DocumentResultKind {
    Acknowledged,
    Collection,
    Collections,
    Document,
    Cursor,
    Count,
    Distinct,
    Insert,
    Update,
    Delete,
    IndexName,
    Indexes,
    CursorKilled,
}

/// Result of one protocol-neutral document command.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub enum DocumentResult {
    Acknowledged(bool),
    Collection(DocumentCollectionMetadata),
    Collections(Box<[DocumentCollectionMetadata]>),
    Document(Option<BsonDocument>),
    Cursor(DocumentCursorBatch),
    Count(u64),
    Distinct(Box<[BsonValue]>),
    Insert(DocumentInsertResult),
    Update(DocumentUpdateResult),
    Delete(DocumentDeleteResult),
    IndexName(String),
    Indexes(Box<[DocumentIndexMetadata]>),
    CursorKilled(bool),
}

impl DocumentResult {
    pub const fn kind(&self) -> DocumentResultKind {
        match self {
            Self::Acknowledged(_) => DocumentResultKind::Acknowledged,
            Self::Collection(_) => DocumentResultKind::Collection,
            Self::Collections(_) => DocumentResultKind::Collections,
            Self::Document(_) => DocumentResultKind::Document,
            Self::Cursor(_) => DocumentResultKind::Cursor,
            Self::Count(_) => DocumentResultKind::Count,
            Self::Distinct(_) => DocumentResultKind::Distinct,
            Self::Insert(_) => DocumentResultKind::Insert,
            Self::Update(_) => DocumentResultKind::Update,
            Self::Delete(_) => DocumentResultKind::Delete,
            Self::IndexName(_) => DocumentResultKind::IndexName,
            Self::Indexes(_) => DocumentResultKind::Indexes,
            Self::CursorKilled(_) => DocumentResultKind::CursorKilled,
        }
    }
}

impl fmt::Debug for DocumentResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("DocumentResult");
        debug.field("kind", &self.kind());
        match self {
            Self::Acknowledged(value) | Self::CursorKilled(value) => {
                debug.field("value", value);
            }
            Self::Collections(values) => {
                debug.field("collection_count", &values.len());
            }
            Self::Document(value) => {
                debug.field("present", &value.is_some());
                debug.field("document", &"<redacted>");
            }
            Self::Cursor(value) => {
                debug.field("cursor", value);
            }
            Self::Count(value) => {
                debug.field("count", value);
            }
            Self::Distinct(values) => {
                debug.field("value_count", &values.len());
                debug.field("values", &"<redacted>");
            }
            Self::Insert(value) => {
                debug.field("result", value);
            }
            Self::Update(value) => {
                debug.field("result", value);
            }
            Self::Delete(value) => {
                debug.field("result", value);
            }
            Self::Indexes(values) => {
                debug.field("index_count", &values.len());
            }
            Self::Collection(_) | Self::IndexName(_) => {
                debug.field("payload", &"<redacted>");
            }
        }
        debug.finish()
    }
}

/// Completed document request paired with its route and stable identity.
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentExecution {
    request_id: DocumentRequestId,
    plan: Option<DocumentPlan>,
    result: DocumentResult,
}

impl DocumentExecution {
    pub fn new(
        request_id: DocumentRequestId,
        plan: Option<DocumentPlan>,
        result: DocumentResult,
    ) -> Self {
        Self {
            request_id,
            plan,
            result,
        }
    }

    pub const fn request_id(&self) -> DocumentRequestId {
        self.request_id
    }

    pub const fn plan(&self) -> Option<&DocumentPlan> {
        self.plan.as_ref()
    }

    pub const fn result(&self) -> &DocumentResult {
        &self.result
    }

    pub fn into_parts(self) -> (DocumentRequestId, Option<DocumentPlan>, DocumentResult) {
        (self.request_id, self.plan, self.result)
    }
}

impl fmt::Debug for DocumentExecution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentExecution")
            .field("request_id", &self.request_id)
            .field("plan", &self.plan)
            .field("result_kind", &self.result.kind())
            .field("result", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn namespace() -> DocumentNamespace {
        DocumentNamespace::new("database", "records").unwrap()
    }

    fn secret_document() -> BsonDocument {
        BsonDocument::from_entries([("secret", BsonValue::from("hidden-value"))]).unwrap()
    }

    #[test]
    fn cursor_batch_preserves_bson_order_and_redacts_debug() {
        let batch = DocumentCursorBatch::new(namespace(), None, vec![secret_document()]).unwrap();
        assert!(batch.is_exhausted());
        assert_eq!(batch.documents()[0].iter().next().unwrap().0, "secret");
        let debug = format!("{batch:?}");
        assert!(debug.contains("document_count"));
        assert!(!debug.contains("hidden-value"));
    }

    #[test]
    fn write_results_preserve_exact_bson_ids_and_validate_counts() {
        let id = BsonValue::Int64(42);
        let inserted = DocumentInsertResult::new(vec![id.clone()]).unwrap();
        assert!(inserted.acknowledged());
        assert!(inserted.inserted_ids()[0].representation_eq(&id));
        assert!(DocumentUpdateResult::new(1, 2, None).is_err());
        let updated = DocumentUpdateResult::new(0, 0, Some(id.clone())).unwrap();
        assert!(updated.upserted_id().unwrap().representation_eq(&id));
        assert_eq!(DocumentDeleteResult::new(3).deleted_count(), 3);
    }

    #[test]
    fn execution_carries_identity_and_redacts_result_payload() {
        fn assert_owned<T: Clone + Send + Sync + 'static>() {}
        assert_owned::<DocumentExecution>();
        let request_id = DocumentRequestId::new([5; 16]).unwrap();
        let execution = DocumentExecution::new(
            request_id,
            None,
            DocumentResult::Document(Some(secret_document())),
        );
        assert_eq!(execution.request_id(), request_id);
        assert_eq!(execution.result().kind(), DocumentResultKind::Document);
        let debug = format!("{execution:?} {:?}", execution.result());
        assert!(!debug.contains("hidden-value"));
        assert!(debug.contains("Document"));
    }
}
