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
    EngineOptions, EngineResult, EngineState, EngineStatus, Executed,
    GLOBAL_INDEX_SHARD_SUMMARY_BLOOM_BYTES, GLOBAL_INDEX_SHARD_SUMMARY_FORMAT_VERSION,
    GeneratedKey, GlobalIndexAsyncOptions, GlobalIndexAsyncProcessReport,
    GlobalIndexAsyncShardOutcome, GlobalIndexAsyncShardReport, GlobalIndexAsyncShardStatus,
    GlobalIndexAsyncStatus, GlobalIndexBuildReport, GlobalIndexDeclaration, GlobalIndexId,
    GlobalIndexKeyPart, GlobalIndexKeySource, GlobalIndexKeyType, GlobalIndexLifecycle,
    GlobalIndexMetadata, GlobalIndexOutboxBatch, GlobalIndexOutboxCursor, GlobalIndexOutboxEvent,
    GlobalIndexOutboxEventKind, GlobalIndexOutboxPruneReport, GlobalIndexOutboxShardStatus,
    GlobalIndexOwner, GlobalIndexRepairReport, GlobalIndexRoutingFallback, GlobalIndexRoutingKind,
    GlobalIndexRoutingPlan, GlobalIndexShardSummaryRebuildReport,
    GlobalIndexShardSummaryShardStatus, GlobalIndexShardSummaryState,
    GlobalIndexShardSummaryStatus, GlobalIndexStorageTopology, GlobalIndexValidationIssue,
    GlobalIndexValidationIssueKind, GlobalIndexValidationMode, GlobalIndexValidationOptions,
    GlobalIndexValidationReport, GlobalIndexWorker, GlobalOperationId, GlobalOperationState,
    GlobalUniqueMutation, GlobalUniqueReservation, GlobalValueLease,
    HASH_PARTITIONED_GLOBAL_INDEX_PARTITIONS_V1, INDEX_KEY_ENCODING_VERSION, IndexKeyCollation,
    IndexKeyOrder, IndexKeyPart, IndexKeyValue, IndexKeyValueRef, IndexNullOrder,
    MAX_GLOBAL_INDEX_OUTBOX_BATCH_EVENTS, MAX_GLOBAL_INDEX_OUTBOX_BYTES_PER_SHARD,
    MAX_GLOBAL_INDEX_OUTBOX_EVENTS_PER_SHARD, ParseDecimalError, PortalId, PrepareRequest,
    PreparedExecution, PreparedStatementDescription, PreparedStatementId, PreparedStatementLimits,
    RequestContext, ResultLimits, ResultSet, ResultSetShapeError, Routed, Row, Session, SessionId,
    SessionState, ShardSummaryPredicateKind, ShardSummaryPrunedShard, ShardSummaryPruningReason,
    ShardSummaryRoutingFallback, ShardSummaryRoutingPlan, ShutdownReport, Statement,
    UniqueNullSemantics, Value, WriteResult,
};
#[cfg(feature = "embedded")]
pub use embedded::{
    BriskDb, BriskDbBuilder, BriskSession, DEFAULT_EMBEDDED_SHARDS, DocumentSupport,
    RuntimeBehavior,
};
pub use sql::{SqlDialect, SqlTranslationMode, StatementBehavior};
