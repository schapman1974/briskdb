pub mod core;
#[cfg(feature = "embedded")]
pub mod embedded;
#[cfg(feature = "sqlite-import")]
pub mod import;
pub mod protocol;
#[cfg(feature = "listeners")]
pub mod server;
pub mod sql;
pub mod storage;

mod sqlite_error;

// Preserve the original public module path while frontends migrate to the
// explicit protocol namespace.
#[cfg(feature = "http")]
pub use protocol::http as api;

pub use core::{
    CancellationToken, CanonicalIndexKey, CheckpointReport, CheckpointShardReport, Column,
    DataType, Decimal, DecodedIndexKeyPart, DescribeTarget, EngineError, EngineErrorKind,
    EngineOptions, EngineResult, EngineState, EngineStatus, Executed, GeneratedKey,
    GlobalIndexDeclaration, GlobalIndexId, GlobalIndexKeyPart, GlobalIndexKeySource,
    GlobalIndexKeyType, GlobalIndexLifecycle, GlobalIndexMetadata, GlobalIndexStorageTopology,
    INDEX_KEY_ENCODING_VERSION, IndexKeyCollation, IndexKeyOrder, IndexKeyPart, IndexKeyValue,
    IndexKeyValueRef, IndexNullOrder, ParseDecimalError, PortalId, PrepareRequest,
    PreparedExecution, PreparedStatementDescription, PreparedStatementId, PreparedStatementLimits,
    RequestContext, ResultLimits, ResultSet, ResultSetShapeError, Routed, Row, Session, SessionId,
    SessionState, ShutdownReport, Statement, UniqueNullSemantics, Value, WriteResult,
};
#[cfg(feature = "embedded")]
pub use embedded::{
    BriskDb, BriskDbBuilder, BriskSession, DEFAULT_EMBEDDED_SHARDS, DocumentSupport,
    RuntimeBehavior,
};
pub use sql::{SqlDialect, SqlTranslationMode, StatementBehavior};
