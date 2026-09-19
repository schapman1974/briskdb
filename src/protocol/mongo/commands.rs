//! Mongo command translation over the shared, host-enabled document engine.

use std::{
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

mod cursors;

use tokio::sync::Mutex;

use super::{Request, wire};
use crate::{
    BriskDb, CancellationToken, DocumentSupport,
    core::{EngineError, EngineErrorKind, RequestContext, ResultLimits, Session},
    document::{
        BsonCodecOptions, BsonDocument, BsonValue, DocumentAggregateRequest, DocumentAggregator,
        DocumentCollectionExistsRequest, DocumentCollectionOptions, DocumentCommand,
        DocumentContinueCursorRequest, DocumentCountRequest, DocumentCreateCollectionRequest,
        DocumentCursorError, DocumentCursorId, DocumentDistinctRequest,
        DocumentDropCollectionRequest, DocumentDropDatabaseRequest, DocumentFilter,
        DocumentFindRequest, DocumentInsertRequest, DocumentKillCursorRequest,
        DocumentListCollectionMetadataRequest, DocumentMatcher, DocumentNamespace,
        DocumentPipeline, DocumentProjection, DocumentProjector, DocumentQueryError,
        DocumentReadOptions, DocumentRequest, DocumentRequestId, DocumentResult, DocumentSort,
        DocumentSorter, DocumentWriteOptions, decode_document_batch_with_options,
        encode_document_with_options,
    },
};

type Result<T> = std::result::Result<T, CommandError>;

#[derive(Debug)]
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

    fn write_document(&self, index: usize) -> BsonDocument {
        fields([
            ("index", BsonValue::Int32(index as i32)),
            ("code", BsonValue::Int32(self.code)),
            ("codeName", BsonValue::from(self.name)),
            ("errmsg", BsonValue::from(self.message)),
        ])
    }
}

impl From<EngineError> for CommandError {
    fn from(error: EngineError) -> Self {
        let mut source = error.source();
        while let Some(cause) = source {
            if let Some(cursor) = cause.downcast_ref::<DocumentCursorError>() {
                return Self::new(
                    cursor.mongo_code(),
                    match cursor {
                        DocumentCursorError::NotFound => "CursorNotFound",
                        DocumentCursorError::InUse => "CursorInUse",
                    },
                    "cursor is unavailable",
                );
            }
            if let Some(query) = cause.downcast_ref::<DocumentQueryError>() {
                let code = query.mongo_code();
                let name = match code {
                    14 => "TypeMismatch",
                    9 => "FailedToParse",
                    115 => "CommandNotSupported",
                    _ => "BadValue",
                };
                return Self::new(code, name, "invalid or unsupported document query");
            }
            source = cause.source();
        }
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
    CreateCollection(DocumentCreateCollectionRequest),
    ListCollections(DocumentListCollectionMetadataRequest, Option<Duration>),
    DropCollection(DocumentDropCollectionRequest),
    DropDatabase(DocumentDropDatabaseRequest),
    Insert(DocumentInsertRequest),
    Count(DocumentCountRequest),
    Distinct(DocumentDistinctRequest),
    Find(DocumentFindRequest, bool, Option<Duration>),
    Aggregate(DocumentAggregateRequest, Option<Duration>),
    GetMore(DocumentContinueCursorRequest),
    KillCursors(DocumentNamespace, Vec<DocumentCursorId>),
}

pub(super) struct Prepared {
    command: Command,
    timeout: Duration,
}

/// Called on the bounded blocking parser, before any engine work is admitted.
pub(super) fn prepare(request: &Request) -> Option<Result<Prepared>> {
    let (name, value) = request.body.iter().next()?;
    if !matches!(
        name,
        "insert"
            | "find"
            | "aggregate"
            | "count"
            | "distinct"
            | "getMore"
            | "killCursors"
            | "drop"
            | "dropDatabase"
            | "create"
            | "listCollections"
    ) {
        return None;
    }
    Some((|| {
        let started = Instant::now();
        if request.more_to_come && name != "insert" {
            return Err(CommandError::options());
        }
        let namespace = if matches!(name, "dropDatabase" | "listCollections") {
            if !matches!(value, BsonValue::Int32(1) | BsonValue::Int64(1)) {
                return Err(CommandError::invalid());
            }
            DocumentNamespace::new(
                &request.database,
                if name == "listCollections" {
                    "$cmd.listCollections"
                } else {
                    "_"
                },
            )?
        } else {
            let collection = if name == "getMore" {
                request
                    .body
                    .get_first("collection")
                    .ok_or_else(CommandError::invalid)?
            } else {
                value
            };
            let BsonValue::String(collection) = collection else {
                return Err(CommandError::invalid());
            };
            DocumentNamespace::new(&request.database, collection)?
        };
        let mut timeout = Duration::from_secs(15);
        let mut cursor_budget = None;
        for (field, value) in request.body.iter().skip(1) {
            let valid = match field {
                "$db" => matches!(value, BsonValue::String(_)),
                "$readPreference" => matches!(value, BsonValue::Document(doc)
                    if doc.len() == 1 && matches!(doc.get_first("mode"), Some(BsonValue::String(mode))
                        if ["primary", "primaryPreferred", "secondary", "secondaryPreferred", "nearest"].contains(&mode.as_str()))),
                "maxTimeMS" => {
                    let millis = unsigned(value)?;
                    if millis > 0 {
                        if name == "getMore" {
                            return Err(CommandError::options());
                        }
                        timeout = timeout.min(Duration::from_millis(millis));
                        if matches!(name, "find" | "aggregate" | "listCollections") {
                            cursor_budget = Some(Duration::from_millis(millis));
                        }
                    }
                    true
                }
                "documents" if name == "insert" => matches!(value, BsonValue::Array(_)),
                "ordered" if name == "insert" => matches!(value, BsonValue::Boolean(_)),
                "bypassDocumentValidation" if name == "insert" => {
                    matches!(value, BsonValue::Boolean(false))
                }
                "writeConcern" if name == "insert" => valid_write_concern(value),
                "writeConcern" if matches!(name, "drop" | "dropDatabase" | "create") => {
                    valid_write_concern(value)
                        && matches!(value, BsonValue::Document(doc)
                        if !matches!(doc.get_first("w"), Some(BsonValue::Int32(0) | BsonValue::Int64(0))))
                }
                // PyMongo's drop_database helper always sends its default None.
                "comment"
                    if matches!(name, "drop" | "dropDatabase" | "create" | "listCollections") =>
                {
                    matches!(value, BsonValue::Null)
                }
                "filter" if matches!(name, "find" | "listCollections") => {
                    matches!(value, BsonValue::Document(_))
                }
                "nameOnly" | "authorizedCollections" if name == "listCollections" => {
                    matches!(value, BsonValue::Boolean(_))
                }
                "pipeline" if name == "aggregate" => {
                    if !matches!(value, BsonValue::Array(_)) {
                        return Err(CommandError::new(
                            14,
                            "TypeMismatch",
                            "aggregate pipeline must be an array",
                        ));
                    }
                    true
                }
                "cursor" if matches!(name, "aggregate" | "listCollections") => {
                    if !matches!(value, BsonValue::Document(_)) {
                        return Err(CommandError::new(
                            14,
                            "TypeMismatch",
                            "aggregate cursor must be a document",
                        ));
                    }
                    true
                }
                "allowDiskUse" if name == "aggregate" => matches!(value, BsonValue::Boolean(false)),
                "query" if name == "count" => matches!(value, BsonValue::Document(_)),
                "key" if name == "distinct" => {
                    if !matches!(value, BsonValue::String(_)) {
                        return Err(CommandError::new(
                            14,
                            "TypeMismatch",
                            "distinct key must be a string",
                        ));
                    }
                    true
                }
                "query" if name == "distinct" => {
                    if !matches!(value, BsonValue::Document(_)) {
                        return Err(CommandError::new(
                            14,
                            "TypeMismatch",
                            "distinct query must be a document",
                        ));
                    }
                    true
                }
                "limit" | "skip" if name == "count" => {
                    unsigned(value)?;
                    true
                }
                "projection" | "sort" if name == "find" => matches!(value, BsonValue::Document(_)),
                "limit" | "skip" | "batchSize" if name == "find" => {
                    unsigned(value)?;
                    true
                }
                "singleBatch" if name == "find" => matches!(value, BsonValue::Boolean(_)),
                "collection" if name == "getMore" => matches!(value, BsonValue::String(_)),
                "batchSize" if name == "getMore" => {
                    unsigned(value)?;
                    true
                }
                "cursors" if name == "killCursors" => matches!(value, BsonValue::Array(_)),
                _ => false,
            };
            if !valid {
                return Err(CommandError::options());
            }
        }
        let mut command = if name == "listCollections" {
            if !request.sequences.is_empty() {
                return Err(CommandError::options());
            }
            let filter = match request.body.get_first("filter") {
                Some(BsonValue::Document(filter)) => filter.clone(),
                None => BsonDocument::new(),
                _ => return Err(CommandError::invalid()),
            };
            DocumentMatcher::compile_with_check(&filter, &mut || {
                if started.elapsed() >= timeout {
                    Err(EngineError::deadline_exceeded(
                        "Mongo metadata parsing deadline exceeded",
                    ))
                } else {
                    Ok(())
                }
            })?;
            let mut options = DocumentReadOptions::new();
            if let Some(BsonValue::Document(cursor)) = request.body.get_first("cursor") {
                for (field, value) in cursor.iter() {
                    if field != "batchSize" {
                        return Err(CommandError::options());
                    }
                    let size = unsigned(value)?;
                    if size > 1000 {
                        return Err(CommandError::unsupported());
                    }
                    options = options.with_batch_size(size)?;
                }
            }
            options =
                options.with_batch_byte_limit((wire::MAX_BOOTSTRAP_MESSAGE_BYTES - 8192) as u64)?;
            Command::ListCollections(
                DocumentListCollectionMetadataRequest::new(
                    &request.database,
                    DocumentFilter::new(filter)?,
                    matches!(
                        request.body.get_first("nameOnly"),
                        Some(BsonValue::Boolean(true))
                    ),
                    options,
                )?,
                cursor_budget,
            )
        } else if name == "create" {
            if !request.sequences.is_empty() {
                return Err(CommandError::options());
            }
            Command::CreateCollection(DocumentCreateCollectionRequest::new(
                namespace,
                DocumentCollectionOptions::empty(),
                DocumentWriteOptions::new(),
            ))
        } else if matches!(name, "drop" | "dropDatabase") {
            if !request.sequences.is_empty() {
                return Err(CommandError::options());
            }
            if name == "drop" {
                Command::DropCollection(DocumentDropCollectionRequest::new(
                    namespace,
                    DocumentWriteOptions::new(),
                ))
            } else {
                Command::DropDatabase(DocumentDropDatabaseRequest::new(
                    &request.database,
                    DocumentWriteOptions::new(),
                )?)
            }
        } else if name == "insert" {
            let documents = insert_documents(request)?;
            let ordered = !matches!(
                request.body.get_first("ordered"),
                Some(BsonValue::Boolean(false))
            );
            Command::Insert(DocumentInsertRequest::new(
                namespace,
                documents,
                DocumentWriteOptions::new().with_ordered(ordered),
            )?)
        } else if name == "find" {
            if !request.sequences.is_empty() {
                return Err(CommandError::options());
            }
            let filter = match request.body.get_first("filter") {
                Some(BsonValue::Document(filter)) => filter.clone(),
                None => BsonDocument::new(),
                _ => return Err(CommandError::invalid()),
            };
            // Validate before missing-collection handling or storage admission.
            // The shared engine compiles the same authoritative matcher.
            DocumentMatcher::compile_with_check(&filter, &mut || {
                if started.elapsed() >= timeout {
                    Err(EngineError::deadline_exceeded(
                        "Mongo command parsing deadline exceeded",
                    ))
                } else {
                    Ok(())
                }
            })?;
            let mut options = DocumentReadOptions::new();
            if let Some(BsonValue::Document(projection)) = request.body.get_first("projection") {
                DocumentProjector::compile_with_check(projection, &mut || {
                    if started.elapsed() >= timeout {
                        Err(EngineError::deadline_exceeded(
                            "Mongo projection parsing deadline exceeded",
                        ))
                    } else {
                        Ok(())
                    }
                })?;
                options = options.with_projection(DocumentProjection::new(projection.clone())?);
            }
            if let Some(BsonValue::Document(sort)) = request.body.get_first("sort") {
                if !sort.is_empty() {
                    DocumentSorter::compile_with_check(sort, &mut || {
                        if started.elapsed() >= timeout {
                            Err(EngineError::deadline_exceeded(
                                "Mongo sorting parsing deadline exceeded",
                            ))
                        } else {
                            Ok(())
                        }
                    })?;
                    options = options.with_sort(DocumentSort::new(sort.clone())?);
                }
            }
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
                if size > 1000 {
                    return Err(CommandError::unsupported());
                }
                options = options.with_batch_size(size)?;
            }
            let single_batch = matches!(
                request.body.get_first("singleBatch"),
                Some(BsonValue::Boolean(true))
            );
            if single_batch && options.batch_size() > 0 {
                options = options.clone().with_limit(
                    options
                        .limit()
                        .unwrap_or(u64::MAX)
                        .min(options.batch_size()),
                )?;
            }
            options =
                options.with_batch_byte_limit((wire::MAX_BOOTSTRAP_MESSAGE_BYTES - 8192) as u64)?;
            Command::Find(
                DocumentFindRequest::new(namespace, DocumentFilter::new(filter)?, options),
                single_batch,
                cursor_budget,
            )
        } else if name == "aggregate" {
            if !request.sequences.is_empty() {
                return Err(CommandError::options());
            }
            let Some(BsonValue::Array(stages)) = request.body.get_first("pipeline") else {
                return Err(CommandError::invalid());
            };
            let pipeline = DocumentPipeline::new(
                stages
                    .iter()
                    .map(|stage| match stage {
                        BsonValue::Document(stage) => Ok(stage.clone()),
                        _ => Err(CommandError::invalid()),
                    })
                    .collect::<Result<Vec<_>>>()?,
            )?;
            let Some(BsonValue::Document(cursor)) = request.body.get_first("cursor") else {
                return Err(CommandError::invalid());
            };
            let mut options = DocumentReadOptions::new();
            for (field, value) in cursor.iter() {
                if field != "batchSize" {
                    return Err(CommandError::options());
                }
                let size = unsigned(value)?;
                if size > 1000 {
                    return Err(CommandError::unsupported());
                }
                options = options.with_batch_size(size)?;
            }
            DocumentAggregator::compile_with_check(&pipeline, &mut || {
                if started.elapsed() >= timeout {
                    Err(EngineError::deadline_exceeded(
                        "Mongo aggregate parsing deadline exceeded",
                    ))
                } else {
                    Ok(())
                }
            })?;
            options =
                options.with_batch_byte_limit((wire::MAX_BOOTSTRAP_MESSAGE_BYTES - 8192) as u64)?;
            Command::Aggregate(
                DocumentAggregateRequest::new(namespace, pipeline, options)?,
                cursor_budget,
            )
        } else if name == "distinct" {
            if !request.sequences.is_empty() {
                return Err(CommandError::options());
            }
            let field = match request.body.get_first("key") {
                Some(BsonValue::String(field)) => field.clone(),
                _ => {
                    return Err(CommandError::new(
                        14,
                        "TypeMismatch",
                        "distinct key must be a string",
                    ));
                }
            };
            let filter = match request.body.get_first("query") {
                Some(BsonValue::Document(filter)) => filter.clone(),
                None => BsonDocument::new(),
                _ => return Err(CommandError::invalid()),
            };
            DocumentMatcher::compile_with_check(&filter, &mut || {
                if started.elapsed() >= timeout {
                    Err(EngineError::deadline_exceeded(
                        "Mongo distinct parsing deadline exceeded",
                    ))
                } else {
                    Ok(())
                }
            })?;
            Command::Distinct(DocumentDistinctRequest::new(
                namespace,
                field,
                DocumentFilter::new(filter)?,
                DocumentReadOptions::new(),
            )?)
        } else if name == "count" {
            if !request.sequences.is_empty() {
                return Err(CommandError::options());
            }
            let filter = match request.body.get_first("query") {
                Some(BsonValue::Document(filter)) => filter.clone(),
                None => BsonDocument::new(),
                _ => return Err(CommandError::invalid()),
            };
            DocumentMatcher::compile_with_check(&filter, &mut || {
                if started.elapsed() >= timeout {
                    Err(EngineError::deadline_exceeded(
                        "Mongo count parsing deadline exceeded",
                    ))
                } else {
                    Ok(())
                }
            })?;
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
            Command::Count(DocumentCountRequest::new(
                namespace,
                DocumentFilter::new(filter)?,
                options,
            ))
        } else if name == "getMore" {
            if !request.sequences.is_empty() {
                return Err(CommandError::options());
            }
            let id = cursor_id(value)?;
            let batch = request
                .body
                .get_first("batchSize")
                .map(unsigned)
                .transpose()?
                .unwrap_or(101);
            if batch == 0 || batch > 1000 {
                return Err(CommandError::invalid());
            }
            let options = DocumentReadOptions::new()
                .with_batch_size(batch)?
                .with_batch_byte_limit((wire::MAX_BOOTSTRAP_MESSAGE_BYTES - 8192) as u64)?;
            Command::GetMore(DocumentContinueCursorRequest::new(namespace, id, options))
        } else {
            if !request.sequences.is_empty() {
                return Err(CommandError::options());
            }
            let Some(BsonValue::Array(ids)) = request.body.get_first("cursors") else {
                return Err(CommandError::invalid());
            };
            if ids.len() > 1000 {
                return Err(CommandError::invalid());
            }
            Command::KillCursors(
                namespace,
                ids.iter().map(cursor_id).collect::<Result<Vec<_>>>()?,
            )
        };
        let elapsed = started.elapsed();
        if elapsed >= timeout {
            return Err(CommandError::new(
                50,
                "MaxTimeMSExpired",
                "command deadline exceeded",
            ));
        }
        if let Command::Find(_, _, budget)
        | Command::Aggregate(_, budget)
        | Command::ListCollections(_, budget) = &mut command
        {
            *budget = budget.map(|budget| budget.saturating_sub(elapsed));
        }
        Ok(Prepared {
            command,
            timeout: timeout - elapsed,
        })
    })())
}

fn cursor_id(value: &BsonValue) -> Result<DocumentCursorId> {
    let id = unsigned(value)?;
    if id == 0 || id > i64::MAX as u64 {
        return Err(CommandError::invalid());
    }
    DocumentCursorId::new(id).map_err(Into::into)
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

fn insert_documents(request: &Request) -> Result<Vec<BsonDocument>> {
    let options = BsonCodecOptions::new()
        .with_max_document_bytes(wire::MAX_BOOTSTRAP_BSON_BYTES)
        .with_max_decoded_bytes(wire::MAX_DECODED_DOCUMENT_BYTES);
    let documents = if !request.sequences.is_empty() {
        if request.sequences.len() != 1 || request.sequences[0].identifier != "documents" {
            return Err(CommandError::options());
        }
        let documents = &request.sequences[0].documents;
        if documents.is_empty() || documents.len() > 1000 {
            return Err(CommandError::invalid());
        }
        let raw: Vec<&[u8]> = documents.iter().map(|bytes| bytes.as_ref()).collect();
        decode_document_batch_with_options(&raw, &options).map_err(|_| {
            CommandError::new(
                10334,
                "BSONObjectTooLarge",
                "insert batch exceeds decoded memory limit",
            )
        })?
    } else {
        let Some(BsonValue::Array(documents)) = request.body.get_first("documents") else {
            return Err(CommandError::invalid());
        };
        if documents.is_empty() || documents.len() > 1000 {
            return Err(CommandError::invalid());
        }
        documents
            .iter()
            .map(|value| match value {
                BsonValue::Document(document) => Ok(document.clone()),
                _ => Err(CommandError::invalid()),
            })
            .collect::<Result<Vec<_>>>()?
    };
    // ID generation belongs to the engine. Reserve the exact BSON ObjectId
    // element size here so normalization cannot exceed the advertised limit.
    for document in &documents {
        let encoded = encode_document_with_options(document, &options)
            .map_err(|_| CommandError::invalid())?;
        if encoded.len()
            + if document.get_first("_id").is_none() {
                17
            } else {
                0
            }
            > wire::MAX_BOOTSTRAP_BSON_BYTES
        {
            return Err(CommandError::new(
                10334,
                "BSONObjectTooLarge",
                "generated ID would exceed document limit",
            ));
        }
    }
    Ok(documents)
}

/// Shared by connections of this listener. Catalog creation still goes through
/// the engine's durable schema gate; no SQLite or routing logic lives here.
pub(super) struct Executor {
    database: BriskDb,
    creation: Mutex<()>,
    cursors: Arc<cursors::WireCursors>,
}

impl Executor {
    pub(super) fn new(database: BriskDb) -> Self {
        Self {
            database,
            creation: Mutex::new(()),
            cursors: Arc::new(cursors::WireCursors::default()),
        }
    }

    pub(super) fn session(&self) -> Session {
        self.database.session()
    }

    pub(super) fn connection_cursors(&self, owner: u64) -> cursors::ConnectionCursors {
        self.cursors.connection(owner)
    }

    pub(super) fn prune_cursors(&self) {
        self.cursors.prune();
    }

    pub(super) fn discard_cursor(&self, id: DocumentCursorId) {
        self.cursors.discard(id);
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
        let command = DocumentCommand::CollectionExists(DocumentCollectionExistsRequest::new(
            namespace.clone(),
        ));
        match self.call(session, identity, context, command).await? {
            DocumentResult::CollectionExists(exists) => Ok(exists),
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
            Command::CreateCollection(request) => {
                match self
                    .call(
                        session,
                        identity,
                        &context,
                        DocumentCommand::CreateCollection(request),
                    )
                    .await?
                {
                    DocumentResult::Collection(_) => Ok(fields([("ok", BsonValue::Double(1.0))])),
                    _ => Err(CommandError::unsupported()),
                }
            }
            Command::DropCollection(request) => {
                match self
                    .call(
                        session,
                        identity,
                        &context,
                        DocumentCommand::DropCollection(request),
                    )
                    .await?
                {
                    DocumentResult::NamespaceDropped(true) => {
                        Ok(fields([("ok", BsonValue::Double(1.0))]))
                    }
                    DocumentResult::NamespaceDropped(false) => Err(CommandError::new(
                        26,
                        "NamespaceNotFound",
                        "collection does not exist",
                    )),
                    _ => Err(CommandError::unsupported()),
                }
            }
            Command::DropDatabase(request) => {
                match self
                    .call(
                        session,
                        identity,
                        &context,
                        DocumentCommand::DropDatabase(request),
                    )
                    .await?
                {
                    DocumentResult::NamespaceDropped(_) => {
                        Ok(fields([("ok", BsonValue::Double(1.0))]))
                    }
                    _ => Err(CommandError::unsupported()),
                }
            }
            Command::Insert(insert) => {
                self.ensure_collection(session, identity, &context, insert.namespace())
                    .await?;
                match self
                    .call(session, identity, &context, DocumentCommand::Insert(insert))
                    .await
                {
                    Ok(DocumentResult::Insert(result)) => {
                        let mut body = fields([
                            ("ok", BsonValue::Double(1.0)),
                            ("n", BsonValue::Int32(result.inserted_ids().len() as i32)),
                        ]);
                        if !result.write_errors().is_empty() {
                            let errors = result
                                .write_errors()
                                .iter()
                                .map(|error| {
                                    let mapped = CommandError::from(EngineError::new(
                                        error.kind(),
                                        "batch write failed",
                                    ));
                                    BsonValue::Document(mapped.write_document(error.index()))
                                })
                                .collect();
                            body.push("writeErrors", BsonValue::Array(errors))
                                .expect("static field name");
                        }
                        Ok(body)
                    }
                    Err(error) if error.code == 11000 => Ok(fields([
                        ("ok", BsonValue::Double(1.0)),
                        ("n", BsonValue::Int32(0)),
                        (
                            "writeErrors",
                            BsonValue::Array(vec![BsonValue::Document(error.write_document(0))]),
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
            Command::Distinct(distinct) => {
                if !self
                    .exists(session, identity, &context, distinct.namespace())
                    .await?
                {
                    return Ok(fields([
                        ("ok", BsonValue::Double(1.0)),
                        ("values", BsonValue::Array(Vec::new())),
                    ]));
                }
                match self
                    .call(
                        session,
                        identity,
                        &context,
                        DocumentCommand::Distinct(distinct),
                    )
                    .await?
                {
                    DocumentResult::Distinct(values) => Ok(fields([
                        ("ok", BsonValue::Double(1.0)),
                        ("values", BsonValue::Array(values.into_vec())),
                    ])),
                    _ => Err(CommandError::new(
                        1,
                        "InternalError",
                        "unexpected engine result",
                    )),
                }
            }
            Command::Count(count) => {
                if !self
                    .exists(session, identity, &context, count.namespace())
                    .await?
                {
                    return Ok(fields([
                        ("ok", BsonValue::Double(1.0)),
                        ("n", BsonValue::Int64(0)),
                    ]));
                }
                match self
                    .call(session, identity, &context, DocumentCommand::Count(count))
                    .await?
                {
                    DocumentResult::Count(count) => Ok(fields([
                        ("ok", BsonValue::Double(1.0)),
                        (
                            "n",
                            BsonValue::Int64(
                                i64::try_from(count).map_err(|_| CommandError::invalid())?,
                            ),
                        ),
                    ])),
                    _ => Err(CommandError::new(
                        1,
                        "InternalError",
                        "unexpected engine result",
                    )),
                }
            }
            command @ (Command::Find(..)
            | Command::Aggregate(..)
            | Command::ListCollections(..)) => {
                let started = Instant::now();
                let metadata = matches!(&command, Command::ListCollections(..));
                let (namespace, command, single_batch, empty_single_batch, budget) = match command {
                    Command::Find(find, single_batch, budget) => {
                        let namespace = find.namespace().clone();
                        let empty = single_batch && find.read_options().batch_size() == 0;
                        (
                            namespace,
                            DocumentCommand::Find(find),
                            single_batch,
                            empty,
                            budget,
                        )
                    }
                    Command::Aggregate(aggregate, budget) => (
                        aggregate.namespace().clone(),
                        DocumentCommand::Aggregate(aggregate),
                        false,
                        false,
                        budget,
                    ),
                    Command::ListCollections(request, budget) => (
                        request.namespace().clone(),
                        DocumentCommand::ListCollectionMetadata(request),
                        false,
                        false,
                        budget,
                    ),
                    _ => unreachable!("cursor command"),
                };
                if !metadata && !self.exists(session, identity, &context, &namespace).await? {
                    return Ok(cursor_reply(namespace.to_string(), None, Vec::new(), false));
                }
                if empty_single_batch {
                    return Ok(cursor_reply(namespace.to_string(), None, Vec::new(), false));
                }
                // Mongo drivers may use a different pooled socket for getMore.
                // Retain this cursor's engine ownership separately from TCP.
                let cursor_session = Arc::new(self.session());
                match self
                    .call(&cursor_session, identity, &context, command)
                    .await?
                {
                    DocumentResult::Cursor(batch) => {
                        let (_, id, documents) = batch.into_parts();
                        let id = if single_batch { None } else { id };
                        if let Some(id) = id {
                            self.cursors.register(
                                id,
                                namespace.clone(),
                                cursor_session,
                                session.id().get(),
                                budget.map(|budget| budget.saturating_sub(started.elapsed())),
                            )?;
                        }
                        Ok(cursor_reply(namespace.to_string(), id, documents, false))
                    }
                    _ => Err(CommandError::new(
                        1,
                        "InternalError",
                        "unexpected engine result",
                    )),
                }
            }
            Command::GetMore(next) => {
                let namespace = next.namespace().clone();
                let id = next.cursor_id();
                let lease = self.cursors.lookup(id, &namespace, session.id().get())?;
                let started = Instant::now();
                let context = if let Some(remaining) = lease.remaining {
                    let deadline = context
                        .deadline()
                        .expect("Mongo command deadline")
                        .min(Instant::now() + remaining.min(Duration::from_secs(15)));
                    context.with_deadline(deadline)
                } else {
                    context
                };
                let result = self
                    .call(
                        &lease.session,
                        identity,
                        &context,
                        DocumentCommand::ContinueCursor(next),
                    )
                    .await;
                match result {
                    Ok(DocumentResult::Cursor(batch)) => {
                        let (_, next_id, documents) = batch.into_parts();
                        lease.complete(started.elapsed(), next_id.is_some())?;
                        Ok(cursor_reply(
                            namespace.to_string(),
                            next_id,
                            documents,
                            true,
                        ))
                    }
                    Err(error) => Err(error),
                    _ => Err(CommandError::new(
                        1,
                        "InternalError",
                        "unexpected engine result",
                    )),
                }
            }
            Command::KillCursors(namespace, ids) => {
                let mut killed = Vec::new();
                let mut missing = Vec::new();
                for id in ids {
                    let Some(cursor_session) = self.cursors.take(id, &namespace) else {
                        missing.push(BsonValue::Int64(id.get() as i64));
                        continue;
                    };
                    let command = DocumentCommand::KillCursor(DocumentKillCursorRequest::new(
                        namespace.clone(),
                        id,
                        DocumentWriteOptions::new(),
                    ));
                    match self
                        .call(&cursor_session, identity, &context, command)
                        .await?
                    {
                        DocumentResult::CursorKilled(true) => {
                            killed.push(BsonValue::Int64(id.get() as i64))
                        }
                        DocumentResult::CursorKilled(false) => {
                            missing.push(BsonValue::Int64(id.get() as i64))
                        }
                        _ => {
                            return Err(CommandError::new(
                                1,
                                "InternalError",
                                "unexpected engine result",
                            ));
                        }
                    }
                }
                Ok(fields([
                    ("ok", BsonValue::Double(1.0)),
                    ("cursorsKilled", BsonValue::Array(killed)),
                    ("cursorsNotFound", BsonValue::Array(missing)),
                    ("cursorsAlive", BsonValue::Array(Vec::new())),
                    ("cursorsUnknown", BsonValue::Array(Vec::new())),
                ]))
            }
        }
    }
}

fn cursor_reply(
    namespace: String,
    id: Option<DocumentCursorId>,
    documents: Vec<BsonDocument>,
    continuation: bool,
) -> BsonDocument {
    fields([
        ("ok", BsonValue::Double(1.0)),
        (
            "cursor",
            BsonValue::Document(fields([
                ("id", BsonValue::Int64(id.map_or(0, |id| id.get() as i64))),
                ("ns", BsonValue::String(namespace)),
                (
                    if continuation {
                        "nextBatch"
                    } else {
                        "firstBatch"
                    },
                    BsonValue::Array(documents.into_iter().map(BsonValue::Document).collect()),
                ),
            ])),
        ),
    ])
}

pub(super) fn reply_cursor_id(body: &BsonDocument) -> Option<DocumentCursorId> {
    let BsonValue::Document(cursor) = body.get_first("cursor")? else {
        return None;
    };
    cursor_id(cursor.get_first("id")?).ok()
}

/// Called on the blocking encoder. Embedded callers may have stored a BSON
/// document larger than this listener advertises; reject it with a command
/// error while keeping the connection usable.
pub(super) fn validate_response(body: &BsonDocument) -> Result<()> {
    if let Some(BsonValue::Document(cursor)) = body.get_first("cursor") {
        for name in ["firstBatch", "nextBatch"] {
            if let Some(BsonValue::Array(documents)) = cursor.get_first(name) {
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
    }
    Ok(())
}
