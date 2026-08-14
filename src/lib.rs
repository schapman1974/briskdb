pub mod core;
pub mod embedded;
pub mod import;
pub mod protocol;
pub mod server;
pub mod sql;
pub mod storage;

mod sqlite_error;

// Preserve the original public module path while frontends migrate to the
// explicit protocol namespace.
pub use protocol::http as api;

pub use core::{
    CancellationToken, CheckpointReport, CheckpointShardReport, Column, DataType, Decimal,
    DescribeTarget, EngineError, EngineErrorKind, EngineOptions, EngineResult, EngineState,
    EngineStatus, Executed, GeneratedKey, ParseDecimalError, PortalId, PrepareRequest,
    PreparedExecution, PreparedStatementDescription, PreparedStatementId, PreparedStatementLimits,
    RequestContext, ResultLimits, ResultSet, ResultSetShapeError, Routed, Row, Session, SessionId,
    SessionState, ShutdownReport, Statement, Value, WriteResult,
};
pub use embedded::{
    BriskDb, BriskDbBuilder, BriskSession, DEFAULT_EMBEDDED_SHARDS, DocumentSupport,
    RuntimeBehavior,
};
pub use sql::{SqlDialect, SqlTranslationMode, StatementBehavior};
