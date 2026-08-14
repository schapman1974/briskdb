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
    CancellationToken, CheckpointReport, CheckpointShardReport, EngineError, EngineErrorKind,
    EngineOptions, EngineResult, EngineState, EngineStatus, Executed, PreparedStatementLimits,
    RequestContext, ResultLimits, ResultSet, Routed, Session, ShutdownReport, Statement, Value,
    WriteResult,
};
pub use embedded::{
    BriskDb, BriskDbBuilder, DEFAULT_EMBEDDED_SHARDS, DocumentSupport, RuntimeBehavior,
};
