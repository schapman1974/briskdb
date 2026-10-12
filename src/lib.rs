pub mod core;
#[cfg(feature = "documents")]
pub mod document;
#[cfg(feature = "embedded")]
pub mod embedded;
#[cfg(feature = "sqlite-import")]
pub mod import;
pub mod protocol;
#[cfg(feature = "listeners")]
pub mod server;
pub mod sql;
pub mod storage;

/// Opt-in SQL database: ISAM catalog, immutable SQLite bases, S3 Parquet changes.
#[cfg(all(unix, feature = "experimental-s3-overlay"))]
pub mod s3_overlay;

/// Original experimental record store. The hybrid metadata adapter also uses
/// these primitives; native application-data SQL/document execution is separate.
#[cfg(all(unix, feature = "experimental-isam"))]
pub use storage::isam;

mod sqlite_error;

// Preserve the original public module path while frontends migrate to the
// explicit protocol namespace.
#[cfg(feature = "http")]
pub use protocol::http as api;

pub use core::{
    CancellationToken, CanonicalIndexKey, CheckpointDatabase, CheckpointDatabaseReport,
    CheckpointReport, CheckpointShardReport, Column, ContentionJitter, ContentionPolicy,
    ContentionStatistics, DataType, Decimal, DecodedIndexKeyPart, DescribeTarget, EngineError,
    EngineErrorKind, EngineOptions, EngineResult, EngineState, EngineStatus, Executed,
    GLOBAL_INDEX_SHARD_SUMMARY_BLOOM_BYTES, GLOBAL_INDEX_SHARD_SUMMARY_FORMAT_VERSION,
    GeneratedKey, GlobalIndexAsyncOptions, GlobalIndexAsyncProcessReport,
    GlobalIndexAsyncShardOutcome, GlobalIndexAsyncShardReport, GlobalIndexAsyncShardStatus,
    GlobalIndexAsyncStatus, GlobalIndexBuildReport, GlobalIndexDeclaration, GlobalIndexHealthState,
    GlobalIndexId, GlobalIndexKeyPart, GlobalIndexKeySource, GlobalIndexKeyType,
    GlobalIndexLifecycle, GlobalIndexMetadata, GlobalIndexOperationalReport,
    GlobalIndexOperationalStatus, GlobalIndexOutboxBatch, GlobalIndexOutboxCursor,
    GlobalIndexOutboxEvent, GlobalIndexOutboxEventKind, GlobalIndexOutboxPruneReport,
    GlobalIndexOutboxShardStatus, GlobalIndexOwner, GlobalIndexRepairReport,
    GlobalIndexRoutingFallback, GlobalIndexRoutingKind, GlobalIndexRoutingPlan,
    GlobalIndexShardSummaryRebuildReport, GlobalIndexShardSummaryShardStatus,
    GlobalIndexShardSummaryState, GlobalIndexShardSummaryStatus, GlobalIndexStorageTopology,
    GlobalIndexValidationIssue, GlobalIndexValidationIssueKind, GlobalIndexValidationMode,
    GlobalIndexValidationOptions, GlobalIndexValidationReport, GlobalIndexWorker,
    GlobalOperationId, GlobalOperationState, GlobalUniqueMutation, GlobalUniqueReservation,
    GlobalValueLease, HASH_PARTITIONED_GLOBAL_INDEX_PARTITIONS_V1, IDEMPOTENCY_FINGERPRINT_VERSION,
    IDEMPOTENCY_LOCK_STRIPES, IDEMPOTENCY_RECEIPT_RETENTION, INDEX_KEY_ENCODING_VERSION,
    IdempotencyKey, IdempotencyStatus, IdempotentWriteResult, IndexKeyCollation, IndexKeyOrder,
    IndexKeyPart, IndexKeyValue, IndexKeyValueRef, IndexNullOrder,
    MAX_GLOBAL_INDEX_OUTBOX_BATCH_EVENTS, MAX_GLOBAL_INDEX_OUTBOX_BYTES_PER_SHARD,
    MAX_GLOBAL_INDEX_OUTBOX_EVENTS_PER_SHARD, MAX_IDEMPOTENCY_RECEIPTS_PER_SHARD, MetadataBackend,
    ParseDecimalError, ParseIdempotencyKeyError, PortalId, PrepareRequest, PreparedExecution,
    PreparedStatementDescription, PreparedStatementId, PreparedStatementLimits, RequestContext,
    ResultLimits, ResultSet, ResultSetShapeError, Routed, Row, Session, SessionId, SessionState,
    ShardSummaryPredicateKind, ShardSummaryPrunedShard, ShardSummaryPruningReason,
    ShardSummaryRoutingFallback, ShardSummaryRoutingPlan, ShutdownReport, Statement,
    StorageProfile, TransactionExecution, UniqueNullSemantics, Value, WriteResult,
};
#[cfg(feature = "embedded")]
pub use embedded::{
    BriskCursor, BriskDb, BriskDbBuilder, BriskSession, BriskTransaction, DEFAULT_EMBEDDED_SHARDS,
    DocumentSupport, RuntimeBehavior,
};
pub use sql::{SqlDialect, SqlTranslationMode, StatementBehavior};
