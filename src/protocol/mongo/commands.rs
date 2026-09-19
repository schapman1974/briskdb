//! Mongo command translation over the shared, host-enabled document engine.

use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use super::{Request, wire};
use crate::{
    BriskDb, CancellationToken, DocumentSupport,
    core::{EngineError, EngineErrorKind, RequestContext, ResultLimits, Session},
    document::{
        BsonCodecOptions, BsonDocument, BsonValue, DocumentCollectionOptions, DocumentCommand,
        DocumentCreateCollectionRequest, DocumentFilter, DocumentFindRequest,
        DocumentInsertRequest, DocumentListCollectionsRequest, DocumentNamespace,
        DocumentReadOptions, DocumentRequest, DocumentRequestId, DocumentResult,
        DocumentWriteOptions, encode_document_with_options,
    },
};

type Result<T> = std::result::Result<T, CommandError>;

pub(super) struct CommandError {
    code: i32,
    name: &'static str,
    message: &'static str,
}

impl CommandError {
    fn new(code: i32, name: &'static str, message: &'static str) -> Self {
        Self {
            code,
            name,
            message,
        }
    }

    fn invalid() -> Self {
        Self::new(2, "BadValue", "invalid Mongo command argument")
    }

    fn unsupported() -> Self {
        Self::new(
            115,
            "CommandNotSupported",
            "command shape is not implemented",
        )
    }

    fn options() -> Self {
        Self::new(
            72,
            "InvalidOptions",
            "unsupported command option or option type",
        )
    }

    pub(super) fn document(&self) -> BsonDocument {
        fields([
            ("ok", BsonValue::Double(0.0)),
            ("code", BsonValue::Int32(self.code)),
            ("codeName", BsonValue::from(self.name)),
            ("errmsg", BsonValue::from(self.message)),
        ])
    }

    fn write_document(&self) -> BsonDocument {
        fields([
            ("index", BsonValue::Int32(0)),
            ("code", BsonValue::Int32(self.code)),
            ("codeName", BsonValue::from(self.name)),
            ("errmsg", BsonValue::from(self.message)),
        ])
    }
}

impl From<EngineError> for CommandError {
    fn from(error: EngineError) -> Self {
        // Diagnostics can contain SQL, paths, and BSON. Only fixed mappings
        // cross the protocol boundary.
        let (code, name, message) = match error.kind() {
            EngineErrorKind::UniqueViolation => (11000, "DuplicateKey", "duplicate document key"),
            EngineErrorKind::Unsupported => return Self::unsupported(),
            EngineErrorKind::InvalidArgument
            | EngineErrorKind::InvalidQuery
            | EngineErrorKind::InvalidTextEncoding
            | EngineErrorKind::NumericOutOfRange => return Self::invalid(),
            EngineErrorKind::TypeMismatch => (14, "TypeMismatch", "invalid value type"),
            EngineErrorKind::PermissionDenied | EngineErrorKind::ReadOnly => {
                (13, "Unauthorized", "operation is not permitted")
            }
            EngineErrorKind::DeadlineExceeded => {
                (50, "MaxTimeMSExpired", "command deadline exceeded")
            }
            EngineErrorKind::Cancelled => (11601, "Interrupted", "command interrupted"),
            EngineErrorKind::ShuttingDown => {
                (91, "ShutdownInProgress", "database is shutting down")
            }
            EngineErrorKind::Busy => (112, "WriteConflict", "database is busy"),
            EngineErrorKind::LimitExceeded | EngineErrorKind::OutOfMemory => (
                10334,
                "BSONObjectTooLarge",
                "command resource limit exceeded",
            ),
            EngineErrorKind::FailedPrecondition => {
                (20, "IllegalOperation", "operation precondition failed")
            }
            _ => (1, "InternalError", "database operation failed"),
        };
        Self::new(code, name, message)
    }
}

fn fields<const N: usize>(entries: [(&str, BsonValue); N]) -> BsonDocument {
    BsonDocument::from_entries(entries).expect("static Mongo result field names")
}

pub(super) enum Command {
    Insert(DocumentInsertRequest),
    Find(DocumentFindRequest),
}

pub(super) struct Prepared {
    command: Command,
    timeout: Duration,
}

/// Called on the bounded blocking parser, before any engine work is admitted.
pub(super) fn prepare(request: &Request) -> Option<Result<Prepared>> {
    let (name, value) = request.body.iter().next()?;
    if !matches!(name, "insert" | "find") {
        return None;
    }
    Some((|| {
        let BsonValue::String(collection) = value else {
            return Err(CommandError::invalid());
        };
        let namespace = DocumentNamespace::new(&request.database, collection)?;
        let mut timeout = Duration::from_secs(15);
        for (field, value) in request.body.iter().skip(1) {
            let valid = match field {
                "$db" => matches!(value, BsonValue::String(_)),
                "$readPreference" => matches!(value, BsonValue::Document(doc)
                    if doc.len() == 1 && matches!(doc.get_first("mode"), Some(BsonValue::String(mode))
                        if ["primary", "primaryPreferred", "secondary", "secondaryPreferred", "nearest"].contains(&mode.as_str()))),
                "maxTimeMS" => {
                    let millis = unsigned(value)?;
                    if millis > 0 {
                        timeout = timeout.min(Duration::from_millis(millis));
                    }
                    true
                }
                "documents" if name == "insert" => matches!(value, BsonValue::Array(_)),
                "ordered" if name == "insert" => matches!(value, BsonValue::Boolean(true)),
                "bypassDocumentValidation" if name == "insert" => {
                    matches!(value, BsonValue::Boolean(false))
                }
                "writeConcern" if name == "insert" => valid_write_concern(value),
                "filter" if name == "find" => matches!(value, BsonValue::Document(_)),
                "limit" | "skip" | "batchSize" if name == "find" => {
                    unsigned(value)?;
                    true
                }
                "singleBatch" if name == "find" => matches!(value, BsonValue::Boolean(_)),
                _ => false,
            };
            if !valid {
                return Err(CommandError::options());
            }
        }
        let command = if name == "insert" {
            let document = single_insert(request)?;
            // PyMongo generates missing IDs before sending insert_one. Server
            // ID generation and bulk-write outcomes remain the insert milestone.
            if document.get_first("_id").is_none() {
                return Err(CommandError::invalid());
            }
            Command::Insert(DocumentInsertRequest::new(
                namespace,
                vec![document],
                DocumentWriteOptions::new(),
            )?)
        } else {
            if !request.sequences.is_empty() {
                return Err(CommandError::options());
            }
            let Some(BsonValue::Document(filter)) = request.body.get_first("filter") else {
                return Err(CommandError::unsupported());
            };
            // Restrict this checkpoint to point reads, including on a missing
            // collection. Never quietly treat an unsupported filter as empty.
            let Some(id) = filter.get_first("_id") else {
                return Err(CommandError::unsupported());
            };
            if filter.len() != 1
                || matches!(id, BsonValue::RegularExpression(_))
                || matches!(id, BsonValue::Document(doc) if doc.iter().any(|(key, _)| key.starts_with('$')))
            {
                return Err(CommandError::unsupported());
            }
            let mut options = DocumentReadOptions::new();
            if let Some(value) = request.body.get_first("skip") {
                options = options.with_skip(unsigned(value)?);
            }
            if let Some(value) = request.body.get_first("limit") {
                let limit = unsigned(value)?;
                if limit != 0 {
                    options = options.with_limit(limit)?;
                }
            }
            if let Some(value) = request.body.get_first("batchSize") {
                let size = unsigned(value)?;
                if size == 0 || size > 1000 {
                    return Err(CommandError::unsupported());
                }
                options = options.with_batch_size(size)?;
            }
            Command::Find(DocumentFindRequest::new(
                namespace,
                DocumentFilter::new(filter.clone())?,
                options,
            ))
        };
        Ok(Prepared { command, timeout })
    })())
}

fn unsigned(value: &BsonValue) -> Result<u64> {
    match value {
        BsonValue::Int32(value) => u64::try_from(*value).map_err(|_| CommandError::invalid()),
        BsonValue::Int64(value) => u64::try_from(*value).map_err(|_| CommandError::invalid()),
        _ => Err(CommandError::invalid()),
    }
}

fn valid_write_concern(value: &BsonValue) -> bool {
    matches!(value, BsonValue::Document(doc) if doc.iter().all(|(key, value)| match key {
        "w" => matches!(value, BsonValue::Int32(0 | 1) | BsonValue::Int64(0 | 1)),
        "j" => matches!(value, BsonValue::Boolean(false)),
        "wtimeout" => matches!(value, BsonValue::Int32(0) | BsonValue::Int64(0)),
        _ => false,
    }))
}

fn single_insert(request: &Request) -> Result<BsonDocument> {
    if !request.sequences.is_empty() {
        if request.sequences.len() != 1 || request.sequences[0].identifier != "documents" {
            return Err(CommandError::options());
        }
        let documents = &request.sequences[0].documents;
        if documents.len() != 1 {
            return Err(CommandError::unsupported());
        }
        return wire::document(&documents[0]).map_err(|_| CommandError::invalid());
    }
    let Some(BsonValue::Array(documents)) = request.body.get_first("documents") else {
        return Err(CommandError::invalid());
    };
    if documents.len() != 1 {
        return Err(CommandError::unsupported());
    }
    let BsonValue::Document(document) = &documents[0] else {
        return Err(CommandError::invalid());
    };
    Ok(document.clone())
}

/// Shared by connections of this listener. Catalog creation still goes through
/// the engine's durable schema gate; no SQLite or routing logic lives here.
pub(super) struct Executor {
    database: BriskDb,
    creation: Mutex<()>,
}

impl Executor {
    pub(super) fn new(database: BriskDb) -> Self {
        Self {
            database,
            creation: Mutex::new(()),
        }
    }

    pub(super) fn session(&self) -> Session {
        self.database.session()
    }

    pub(super) async fn execute(
        &self,
        session: &Session,
        request_id: i32,
        prepared: Prepared,
        shutdown: CancellationToken,
    ) -> BsonDocument {
        let context = RequestContext::new()
            .with_cancellation_token(shutdown)
            .with_deadline(Instant::now() + prepared.timeout)
            // Reserve protocol-envelope space and bound results from documents
            // written through other, less restrictive embedded interfaces too.
            .with_result_limits(
                ResultLimits::new(1000, (wire::MAX_BOOTSTRAP_MESSAGE_BYTES - 4096) as u64)
                    .expect("static result limits"),
            );
        let mut identity = [0u8; 16];
        identity[..8].copy_from_slice(&session.id().get().to_le_bytes());
        identity[8..12].copy_from_slice(&request_id.to_le_bytes());
        identity[12..].copy_from_slice(&1u32.to_le_bytes());
        let identity = DocumentRequestId::new(identity).expect("nonzero request identity");
        match self.run(session, identity, context, prepared.command).await {
            Ok(reply) => reply,
            Err(error) => error.document(),
        }
    }

    async fn call(
        &self,
        session: &Session,
        identity: DocumentRequestId,
        context: &RequestContext,
        command: DocumentCommand,
    ) -> Result<DocumentResult> {
        self.database
            .execute_document(
                session,
                DocumentRequest::new(identity, context.clone(), command),
            )
            .await
            .map(|execution| execution.into_parts().2)
            .map_err(Into::into)
    }

    async fn exists(
        &self,
        session: &Session,
        identity: DocumentRequestId,
        context: &RequestContext,
        namespace: &DocumentNamespace,
    ) -> Result<bool> {
        let command = DocumentCommand::ListCollections(DocumentListCollectionsRequest::new(
            namespace.database(),
            DocumentReadOptions::new(),
        )?);
        match self.call(session, identity, context, command).await? {
            DocumentResult::Collections(collections) => Ok(collections
                .iter()
                .any(|item| item.name() == namespace.collection())),
            _ => Err(CommandError::new(
                1,
                "InternalError",
                "unexpected engine result",
            )),
        }
    }

    async fn ensure_collection(
        &self,
        session: &Session,
        identity: DocumentRequestId,
        context: &RequestContext,
        namespace: &DocumentNamespace,
    ) -> Result<()> {
        let cancellation = context.cancellation_token();
        let deadline = tokio::time::Instant::from_std(context.deadline().expect("Mongo deadline"));
        let _guard = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(CommandError::new(11601, "Interrupted", "command interrupted")),
            guard = tokio::time::timeout_at(deadline, self.creation.lock()) => guard.map_err(|_| CommandError::new(50, "MaxTimeMSExpired", "command deadline exceeded"))?,
        };
        if !self.exists(session, identity, context, namespace).await? {
            let command = DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                namespace.clone(),
                DocumentCollectionOptions::empty(),
                DocumentWriteOptions::new(),
            ));
            self.call(session, identity, context, command).await?;
        }
        Ok(())
    }

    async fn run(
        &self,
        session: &Session,
        identity: DocumentRequestId,
        context: RequestContext,
        command: Command,
    ) -> Result<BsonDocument> {
        if self.database.document_support() != DocumentSupport::Enabled {
            return Err(CommandError::new(
                20,
                "IllegalOperation",
                "document support is disabled by the host",
            ));
        }
        match command {
            Command::Insert(insert) => {
                self.ensure_collection(session, identity, &context, insert.namespace())
                    .await?;
                match self
                    .call(session, identity, &context, DocumentCommand::Insert(insert))
                    .await
                {
                    Ok(DocumentResult::Insert(result)) => Ok(fields([
                        ("ok", BsonValue::Double(1.0)),
                        ("n", BsonValue::Int32(result.inserted_ids().len() as i32)),
                    ])),
                    Err(error) if error.code == 11000 => Ok(fields([
                        ("ok", BsonValue::Double(1.0)),
                        ("n", BsonValue::Int32(0)),
                        (
                            "writeErrors",
                            BsonValue::Array(vec![BsonValue::Document(error.write_document())]),
                        ),
                    ])),
                    Err(error) => Err(error),
                    _ => Err(CommandError::new(
                        1,
                        "InternalError",
                        "unexpected engine result",
                    )),
                }
            }
            Command::Find(find) => {
                let namespace = find.namespace().to_string();
                if !self
                    .exists(session, identity, &context, find.namespace())
                    .await?
                {
                    return Ok(cursor_reply(namespace, Vec::new()));
                }
                match self
                    .call(session, identity, &context, DocumentCommand::Find(find))
                    .await?
                {
                    DocumentResult::Cursor(batch) if batch.is_exhausted() => {
                        Ok(cursor_reply(namespace, batch.into_parts().2))
                    }
                    _ => Err(CommandError::new(
                        1,
                        "InternalError",
                        "unexpected engine result",
                    )),
                }
            }
        }
    }
}

fn cursor_reply(namespace: String, documents: Vec<BsonDocument>) -> BsonDocument {
    fields([
        ("ok", BsonValue::Double(1.0)),
        (
            "cursor",
            BsonValue::Document(fields([
                ("id", BsonValue::Int64(0)),
                ("ns", BsonValue::String(namespace)),
                (
                    "firstBatch",
                    BsonValue::Array(documents.into_iter().map(BsonValue::Document).collect()),
                ),
            ])),
        ),
    ])
}

/// Called on the blocking encoder. Embedded callers may have stored a BSON
/// document larger than this listener advertises; reject it with a command
/// error while keeping the connection usable.
pub(super) fn validate_response(body: &BsonDocument) -> Result<()> {
    if let Some(BsonValue::Document(cursor)) = body.get_first("cursor") {
        if let Some(BsonValue::Array(documents)) = cursor.get_first("firstBatch") {
            let options =
                BsonCodecOptions::new().with_max_document_bytes(wire::MAX_BOOTSTRAP_BSON_BYTES);
            for document in documents {
                if let BsonValue::Document(document) = document {
                    encode_document_with_options(document, &options).map_err(|_| {
                        CommandError::new(
                            10334,
                            "BSONObjectTooLarge",
                            "document exceeds Mongo listener limit",
                        )
                    })?;
                }
            }
        }
    }
    Ok(())
}
