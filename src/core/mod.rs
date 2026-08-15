//! Protocol-neutral database orchestration.
//!
//! This module owns routing and coordinates storage and SQL execution. It does
//! not depend on a network protocol.

mod catalog;
mod control;
mod engine;
mod error;
pub(crate) mod generated_id;
mod global_index;
mod index_key;
mod lifecycle;
mod options;
mod planner;
mod prepared;
mod routing;
mod scatter;
mod session;
mod stream;
mod types;
pub(crate) mod worker;

pub use catalog::{
    Catalog, GeneratedIdPolicy, LogicalDatabaseId, LogicalDatabaseMetadata, ShardKeyMetadata,
    ShardKeyType, TableDeclaration, TableId, TableMetadata, TablePlacement,
};
pub(crate) use catalog::{
    CatalogSnapshot, DEFAULT_LOGICAL_DATABASE_ID, DEFAULT_LOGICAL_DATABASE_NAME,
    IDENTIFIER_ENCODING_VERSION, MAX_LOGICAL_DATABASES, MAX_TABLES, validate_catalog_identifier,
};
pub(crate) use control::{
    CancelOnDrop, CancellationReason, OperationControl, wait_for_cancellation, wait_pending,
};
pub use control::{CancellationToken, RequestContext};
pub use engine::{CheckpointReport, CheckpointShardReport, Engine, EngineStatus, Statement};
pub use error::{EngineError, EngineErrorKind, EngineResult};
pub(crate) use generated_id::AllocationOwnerMap;
pub use global_index::{
    DEFAULT_GLOBAL_INDEX_ASYNC_BATCH_EVENTS, DEFAULT_GLOBAL_INDEX_ASYNC_LEASE_MS,
    DEFAULT_GLOBAL_INDEX_ASYNC_POLL_MS, GlobalIndexAsyncOptions, GlobalIndexAsyncProcessReport,
    GlobalIndexAsyncShardOutcome, GlobalIndexAsyncShardReport, GlobalIndexAsyncShardStatus,
    GlobalIndexAsyncStatus, GlobalIndexBuildReport, GlobalIndexDeclaration, GlobalIndexId,
    GlobalIndexKeyPart, GlobalIndexKeySource, GlobalIndexKeyType, GlobalIndexLifecycle,
    GlobalIndexMetadata, GlobalIndexOutboxBatch, GlobalIndexOutboxCursor, GlobalIndexOutboxEvent,
    GlobalIndexOutboxEventKind, GlobalIndexOutboxPruneReport, GlobalIndexOutboxShardStatus,
    GlobalIndexOwner, GlobalIndexRepairReport, GlobalIndexStorageTopology,
    GlobalIndexValidationIssue, GlobalIndexValidationIssueKind, GlobalIndexValidationMode,
    GlobalIndexValidationOptions, GlobalIndexValidationReport, GlobalOperationId,
    GlobalOperationState, GlobalUniqueMutation, GlobalUniqueReservation, GlobalValueLease,
    HASH_PARTITIONED_GLOBAL_INDEX_PARTITIONS_V1, MAX_GLOBAL_INDEX_OUTBOX_BATCH_EVENTS,
    MAX_GLOBAL_INDEX_OUTBOX_BYTES_PER_SHARD, MAX_GLOBAL_INDEX_OUTBOX_EVENTS_PER_SHARD,
};
pub(crate) use global_index::{
    GlobalIndexReadResolution, MAX_GLOBAL_INDEX_PARTS, MAX_GLOBAL_INDEX_READ_CANDIDATES,
    MAX_GLOBAL_INDEX_READ_REPAIRS, MAX_GLOBAL_INDEX_SQL_BYTES, MAX_GLOBAL_INDEXES,
    MAX_GLOBAL_VALUE_LEASE_COUNT,
};
pub use index_key::{
    CanonicalIndexKey, DecodedIndexKeyPart, INDEX_KEY_ENCODING_VERSION, IndexKeyCollation,
    IndexKeyOrder, IndexKeyPart, IndexKeyValue, IndexKeyValueRef, IndexNullOrder,
    UniqueNullSemantics,
};
pub use lifecycle::{EngineState, ShutdownReport};
pub(crate) use lifecycle::{Lifecycle, OperationLease};
pub use options::{
    DEFAULT_CONNECTIONS_PER_SHARD, DEFAULT_MAX_PORTALS_PER_SESSION,
    DEFAULT_MAX_PREPARED_STATEMENTS_PER_SESSION, DEFAULT_MAX_RESULT_BYTES, DEFAULT_MAX_RESULT_ROWS,
    DEFAULT_MAX_RETAINED_BOUND_VALUE_BYTES, DEFAULT_QUEUE_CAPACITY_PER_SHARD,
    DEFAULT_REQUEST_TIMEOUT_MS, DEFAULT_SHUTDOWN_GRACE_MS, EngineOptions,
    MAX_CONNECTIONS_PER_SHARD, MAX_PORTALS_PER_SESSION, MAX_PREPARED_STATEMENTS_PER_SESSION,
    MAX_QUEUE_CAPACITY_PER_SHARD, MAX_REQUEST_TIMEOUT_MS, MAX_RESULT_BYTES, MAX_RESULT_ROWS,
    MAX_RETAINED_BOUND_VALUE_BYTES, MAX_SHUTDOWN_GRACE_MS, PreparedStatementLimits, ResultLimits,
};
pub use planner::{
    BoundStatementPlan, GlobalIndexRoutingFallback, GlobalIndexRoutingKind, GlobalIndexRoutingPlan,
    PlannedRoute,
};
#[cfg(any(test, feature = "experimental-vtab", feature = "sqlite-import"))]
pub(crate) use planner::{CanonicalShardKeyRef, canonical_shard_key_bytes};
pub(crate) use prepared::PreparedState;
pub use prepared::{
    DescribeTarget, PortalId, PrepareRequest, PreparedExecution, PreparedStatementDescription,
    PreparedStatementId, TransactionExecution,
};
pub(crate) use routing::{
    BUCKET_ALGORITHM_VERSION, HASH_VERSION, INITIAL_MAP_GENERATION, KEY_ENCODING_VERSION,
    RoutingCatalog, VIRTUAL_BUCKET_COUNT, initial_physical_shard,
};
pub(crate) use scatter::merge_scatter_results;
pub use session::{Session, SessionId, SessionState};
pub(crate) use stream::RowProducer;
pub use stream::{DEFAULT_STREAM_BUFFER_ROWS, RowStream};
pub use types::{
    Column, DataType, Decimal, GeneratedKey, ParseDecimalError, ResultSet, ResultSetShapeError,
    Row, Value, WriteResult,
};
pub(crate) use worker::BlockingPool;

use session::SessionInner;

use std::{
    path::Path,
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};

use crate::{sql, storage::Storage};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RawDataOperation {
    Execute,
    Query,
}

#[derive(Debug)]
pub(crate) struct RawDataPlan {
    pub(crate) target: RawDataTarget,
    pub(crate) table_id: Option<TableId>,
    pub(crate) sqlite_sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RawDataTarget {
    Exact(u16),
    Generated(TableId),
}

#[derive(Debug)]
pub struct Database {
    storage: Storage,
    global_index_worker_id: [u8; 16],
}

/// Caller-owned background maintenance loop for non-unique global indexes.
///
/// Dropping the handle requests shutdown and joins the worker. Multiple
/// processes may run workers against one data directory; durable leases fence
/// every index/shard pair.
pub struct GlobalIndexWorker {
    wake: Arc<(Mutex<bool>, Condvar)>,
    join: Option<thread::JoinHandle<()>>,
}

impl std::fmt::Debug for GlobalIndexWorker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GlobalIndexWorker")
            .field(
                "running",
                &self.join.as_ref().is_some_and(|join| !join.is_finished()),
            )
            .finish()
    }
}

impl GlobalIndexWorker {
    /// Request worker shutdown and wait for its thread. Returns whether it had
    /// already been stopped.
    pub fn stop(&mut self) -> bool {
        let already_stopped = self.join.is_none();
        {
            let (lock, wake) = &*self.wake;
            let mut stopped = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            *stopped = true;
            wake.notify_all();
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        already_stopped
    }

    pub fn is_finished(&self) -> bool {
        self.join
            .as_ref()
            .is_none_or(thread::JoinHandle::is_finished)
    }
}

impl Drop for GlobalIndexWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

fn random_global_index_worker_id() -> EngineResult<[u8; 16]> {
    let mut owner_id = [0_u8; 16];
    getrandom::fill(&mut owner_id).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::StorageUnavailable,
            "could not generate a global-index worker identity",
            error,
        )
    })?;
    if owner_id == [0; 16] {
        owner_id[0] = 1;
    }
    Ok(owner_id)
}

/// Durable identities and catalog result of one generated-table DDL request.
///
/// The logical identity names the exact source dialect and source bytes. The
/// physical identity names the canonical SQLite migration text. They are
/// intentionally distinct even when two source dialects translate to the same
/// physical table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedTableDdlReceipt {
    logical_id: [u8; 32],
    physical_migration_id: [u8; 32],
    provisioning_id: [u8; 32],
    table_id: TableId,
}

impl GeneratedTableDdlReceipt {
    /// Return the version-1 exact logical-source identity.
    pub const fn logical_id(&self) -> [u8; 32] {
        self.logical_id
    }

    /// Return the exact canonical-physical-SQL migration identity.
    pub const fn physical_migration_id(&self) -> [u8; 32] {
        self.physical_migration_id
    }

    /// Return the normalized table-policy provisioning identity.
    pub const fn provisioning_id(&self) -> [u8; 32] {
        self.provisioning_id
    }

    /// Return the stable catalog table identity published by the operation.
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    pub(crate) const fn from_durable_parts(
        logical_id: [u8; 32],
        physical_migration_id: [u8; 32],
        provisioning_id: [u8; 32],
        table_id: TableId,
    ) -> Self {
        Self {
            logical_id,
            physical_migration_id,
            provisioning_id,
            table_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Routed<T> {
    pub shard: u16,
    pub value: T,
}

/// Result of one logical engine operation and the physical shards it visited.
///
/// Shards are unique and sorted in ascending order. Single-owner reads and
/// writes contain one entry; logical Sharded reads may contain several.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Executed<T> {
    pub shards: Vec<u16>,
    pub value: T,
}

impl<T> Executed<T> {
    /// Return the physical shards visited by this logical operation.
    pub fn shards(&self) -> &[u16] {
        &self.shards
    }
}

impl Database {
    pub fn open(root: impl AsRef<Path>, requested_shards: u16) -> EngineResult<Self> {
        Ok(Self {
            storage: Storage::open(root, requested_shards)?,
            global_index_worker_id: random_global_index_worker_id()?,
        })
    }

    /// Inspect durable asynchronous freshness, lag, leases, and poison state.
    pub fn global_index_async_status(
        &self,
        index_id: GlobalIndexId,
    ) -> EngineResult<GlobalIndexAsyncStatus> {
        self.storage.global_index_async_status(index_id)
    }

    /// Apply one bounded batch per shard using this handle's fenced identity.
    pub fn process_global_index_async(
        &self,
        index_id: GlobalIndexId,
        options: GlobalIndexAsyncOptions,
    ) -> EngineResult<GlobalIndexAsyncProcessReport> {
        self.process_global_index_async_with_cancellation(
            index_id,
            options,
            &CancellationToken::new(),
        )
    }

    pub fn process_global_index_async_with_cancellation(
        &self,
        index_id: GlobalIndexId,
        options: GlobalIndexAsyncOptions,
        cancellation: &CancellationToken,
    ) -> EngineResult<GlobalIndexAsyncProcessReport> {
        self.storage.process_global_index_async(
            index_id,
            self.global_index_worker_id,
            options,
            cancellation,
        )
    }

    /// Pause future consumer passes for one non-unique global index.
    pub fn pause_global_index_async(&self, index_id: GlobalIndexId) -> EngineResult<()> {
        self.storage.set_global_index_async_paused(index_id, true)
    }

    /// Resume future consumer passes. A poison or rebuild-required state stays
    /// fenced until the index is rebuilt.
    pub fn resume_global_index_async(&self, index_id: GlobalIndexId) -> EngineResult<()> {
        self.storage.set_global_index_async_paused(index_id, false)
    }

    /// Start a managed background consumer for every ready non-unique index.
    pub fn start_global_index_worker(
        &self,
        options: GlobalIndexAsyncOptions,
    ) -> EngineResult<GlobalIndexWorker> {
        let owner_id = random_global_index_worker_id()?;
        let storage = self.storage.clone();
        let wake = Arc::new((Mutex::new(false), Condvar::new()));
        let worker_wake = Arc::clone(&wake);
        let join = thread::Builder::new()
            .name("briskdb-global-index".to_owned())
            .spawn(move || {
                let cancellation = CancellationToken::new();
                loop {
                    let (lock, _) = &*worker_wake;
                    if *lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) {
                        break;
                    }
                    let mut made_progress = false;
                    for index_id in storage.ready_nonunique_global_indexes() {
                        match storage.process_global_index_async(
                            index_id,
                            owner_id,
                            options,
                            &cancellation,
                        ) {
                            Ok(report) => made_progress |= report.applied_events() != 0,
                            Err(error)
                                if matches!(
                                    error.kind(),
                                    EngineErrorKind::Busy
                                        | EngineErrorKind::Cancelled
                                        | EngineErrorKind::DeadlineExceeded
                                ) => {}
                            Err(error) => {
                                #[cfg(any(feature = "http", feature = "sqlite-import"))]
                                tracing::warn!(index_id = %index_id, error = %error, "global-index worker pass failed");
                                #[cfg(not(any(feature = "http", feature = "sqlite-import")))]
                                let _ = (index_id, error);
                            }
                        }
                    }
                    if made_progress {
                        continue;
                    }
                    let (lock, wake) = &*worker_wake;
                    let stopped = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    if *stopped {
                        break;
                    }
                    let _ = wake
                        .wait_timeout(stopped, Duration::from_millis(options.poll_ms()))
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            })
            .map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::StorageUnavailable,
                    "failed to start the global-index worker",
                    error,
                )
            })?;
        Ok(GlobalIndexWorker {
            wake,
            join: Some(join),
        })
    }

    /// Detect the immutable shard count from an initialized data directory
    /// without creating or upgrading storage.
    pub fn detect_shard_count(root: impl AsRef<Path>) -> EngineResult<u16> {
        crate::storage::detect_shard_count(root)
    }

    /// Validate and inspect global-index definitions without creating or
    /// upgrading storage.
    pub fn inspect_global_indexes(
        root: impl AsRef<Path>,
    ) -> EngineResult<Box<[GlobalIndexMetadata]>> {
        crate::storage::inspect_global_indexes(root)
    }

    pub fn shard_count(&self) -> u16 {
        self.storage.shard_count()
    }

    /// Atomically register the complete logical table catalog for an empty
    /// application schema.
    ///
    /// This initialization-only operation requires exclusive ownership of the
    /// database before it is wrapped in an [`Engine`]. Every declared physical
    /// table must already exist with the same empty schema on all shards.
    /// Sharded text keys use `BINARY` collation, every unique key must include
    /// the shard key, and foreign keys must prove authoritative co-location.
    /// Triggers and virtual tables are not yet supported. The catalog can be
    /// registered exactly once; later table changes belong to a journaled
    /// schema-and-catalog migration.
    pub fn register_tables(&mut self, declarations: Vec<TableDeclaration>) -> EngineResult<()> {
        self.storage.register_tables(declarations)
    }

    /// Atomically add one durable global-index definition in `Creating` state.
    ///
    /// This lifecycle API publishes metadata only. Physical index construction
    /// and query use are implemented by the later global-index build stages.
    pub fn create_global_index(
        &mut self,
        declaration: GlobalIndexDeclaration,
    ) -> EngineResult<GlobalIndexId> {
        self.storage.create_global_index(declaration)
    }

    /// Build one declared global index while holding exclusive maintenance-mode
    /// ownership of the data directory.
    ///
    /// Construction scans every physical shard, checkpoints only complete
    /// shard transactions, revalidates resumed checkpoints against the source,
    /// and publishes `Ready` only after the physical SQLite authority is fully
    /// durable. A cancelled build remains in `Creating` and can be resumed by
    /// calling this method again.
    pub fn build_global_index(
        &mut self,
        index_id: GlobalIndexId,
    ) -> EngineResult<GlobalIndexBuildReport> {
        self.storage
            .build_global_index(index_id, &CancellationToken::new())
    }

    /// Build one declared global index with a caller-owned sticky cancellation
    /// signal.
    pub fn build_global_index_with_cancellation(
        &mut self,
        index_id: GlobalIndexId,
        cancellation: &CancellationToken,
    ) -> EngineResult<GlobalIndexBuildReport> {
        self.storage.build_global_index(index_id, cancellation)
    }

    /// Fully validate one global index against every qualifying source row.
    ///
    /// Validation is an offline maintenance operation. It fences the index out
    /// of `Ready` before scanning and publishes either `Ready` or `Invalid`
    /// after the machine-readable result is durable.
    pub fn validate_global_index(
        &mut self,
        index_id: GlobalIndexId,
    ) -> EngineResult<GlobalIndexValidationReport> {
        self.validate_global_index_with_cancellation(
            index_id,
            GlobalIndexValidationOptions::full(),
            &CancellationToken::new(),
        )
    }

    /// Validate one global index with explicit full/sample bounds and cancellation.
    pub fn validate_global_index_with_cancellation(
        &mut self,
        index_id: GlobalIndexId,
        options: GlobalIndexValidationOptions,
        cancellation: &CancellationToken,
    ) -> EngineResult<GlobalIndexValidationReport> {
        self.storage
            .validate_global_index(index_id, options, cancellation)
    }

    /// Rebuild a `Ready`, `Invalid`, or interrupted `Rebuilding` index offline.
    pub fn rebuild_global_index(
        &mut self,
        index_id: GlobalIndexId,
    ) -> EngineResult<GlobalIndexBuildReport> {
        self.rebuild_global_index_with_cancellation(index_id, &CancellationToken::new())
    }

    /// Rebuild one index with a caller-owned sticky cancellation signal.
    pub fn rebuild_global_index_with_cancellation(
        &mut self,
        index_id: GlobalIndexId,
        cancellation: &CancellationToken,
    ) -> EngineResult<GlobalIndexBuildReport> {
        self.storage.rebuild_global_index(index_id, cancellation)
    }

    /// Repair stale non-unique physical state one affected shard at a time.
    ///
    /// Unique indexes deliberately reject this path because authoritative
    /// uniqueness must be reconstructed from source through a full rebuild.
    pub fn repair_global_index(
        &mut self,
        index_id: GlobalIndexId,
    ) -> EngineResult<GlobalIndexRepairReport> {
        self.repair_global_index_with_cancellation(index_id, &CancellationToken::new())
    }

    /// Repair a non-unique index with caller-owned cancellation.
    pub fn repair_global_index_with_cancellation(
        &mut self,
        index_id: GlobalIndexId,
        cancellation: &CancellationToken,
    ) -> EngineResult<GlobalIndexRepairReport> {
        self.storage.repair_global_index(index_id, cancellation)
    }

    /// Inspect shard-local non-unique global-index event retention and lag.
    pub fn global_index_outbox_status(&self) -> EngineResult<Vec<GlobalIndexOutboxShardStatus>> {
        self.storage.global_index_outbox_status()
    }

    /// Replay a bounded batch after one durable shard cursor.
    pub fn read_global_index_outbox(
        &self,
        index_id: GlobalIndexId,
        shard: u16,
        after: GlobalIndexOutboxCursor,
        limit: usize,
    ) -> EngineResult<GlobalIndexOutboxBatch> {
        self.read_global_index_outbox_with_cancellation(
            index_id,
            shard,
            after,
            limit,
            &CancellationToken::new(),
        )
    }

    pub fn read_global_index_outbox_with_cancellation(
        &self,
        index_id: GlobalIndexId,
        shard: u16,
        after: GlobalIndexOutboxCursor,
        limit: usize,
        cancellation: &CancellationToken,
    ) -> EngineResult<GlobalIndexOutboxBatch> {
        self.storage
            .read_global_index_outbox(index_id, shard, after, limit, cancellation)
    }

    /// Persist one non-unique index consumer's replay position.
    pub fn advance_global_index_outbox(
        &self,
        index_id: GlobalIndexId,
        shard: u16,
        cursor: GlobalIndexOutboxCursor,
    ) -> EngineResult<GlobalIndexOutboxShardStatus> {
        self.advance_global_index_outbox_with_cancellation(
            index_id,
            shard,
            cursor,
            &CancellationToken::new(),
        )
    }

    pub fn advance_global_index_outbox_with_cancellation(
        &self,
        index_id: GlobalIndexId,
        shard: u16,
        cursor: GlobalIndexOutboxCursor,
        cancellation: &CancellationToken,
    ) -> EngineResult<GlobalIndexOutboxShardStatus> {
        self.storage
            .advance_global_index_outbox(index_id, shard, cursor, cancellation)
    }

    /// Delete a bounded prefix acknowledged by every active shard consumer.
    pub fn prune_global_index_outbox(
        &self,
        shard: u16,
        limit: usize,
    ) -> EngineResult<GlobalIndexOutboxPruneReport> {
        self.prune_global_index_outbox_with_cancellation(shard, limit, &CancellationToken::new())
    }

    pub fn prune_global_index_outbox_with_cancellation(
        &self,
        shard: u16,
        limit: usize,
        cancellation: &CancellationToken,
    ) -> EngineResult<GlobalIndexOutboxPruneReport> {
        self.storage
            .prune_global_index_outbox(shard, limit, cancellation)
    }

    /// Durably reserve one unique-key mutation before its shard write commits.
    pub fn reserve_global_unique(
        &self,
        operation_id: GlobalOperationId,
        mutation: &GlobalUniqueMutation,
    ) -> EngineResult<GlobalUniqueReservation> {
        self.reserve_global_unique_with_cancellation(
            operation_id,
            mutation,
            &CancellationToken::new(),
        )
    }

    pub fn reserve_global_unique_with_cancellation(
        &self,
        operation_id: GlobalOperationId,
        mutation: &GlobalUniqueMutation,
        cancellation: &CancellationToken,
    ) -> EngineResult<GlobalUniqueReservation> {
        self.storage
            .reserve_global_unique(operation_id, mutation, cancellation)
    }

    /// Publish the reserved owner after the corresponding shard write commits.
    pub fn finalize_global_unique(
        &self,
        operation_id: GlobalOperationId,
    ) -> EngineResult<GlobalUniqueReservation> {
        self.storage
            .finalize_global_unique(operation_id, &CancellationToken::new())
    }

    pub fn finalize_global_unique_with_cancellation(
        &self,
        operation_id: GlobalOperationId,
        cancellation: &CancellationToken,
    ) -> EngineResult<GlobalUniqueReservation> {
        self.storage
            .finalize_global_unique(operation_id, cancellation)
    }

    /// Release an active reservation when its corresponding shard write fails.
    pub fn rollback_global_unique(
        &self,
        operation_id: GlobalOperationId,
    ) -> EngineResult<GlobalUniqueReservation> {
        self.storage
            .rollback_global_unique(operation_id, &CancellationToken::new())
    }

    pub fn rollback_global_unique_with_cancellation(
        &self,
        operation_id: GlobalOperationId,
        cancellation: &CancellationToken,
    ) -> EngineResult<GlobalUniqueReservation> {
        self.storage
            .rollback_global_unique(operation_id, cancellation)
    }

    /// Irrevocably lease a disjoint range of positive global integer values.
    pub fn lease_global_values(
        &self,
        operation_id: GlobalOperationId,
        index_id: GlobalIndexId,
        count: u32,
    ) -> EngineResult<GlobalValueLease> {
        self.lease_global_values_with_cancellation(
            operation_id,
            index_id,
            count,
            &CancellationToken::new(),
        )
    }

    pub fn lease_global_values_with_cancellation(
        &self,
        operation_id: GlobalOperationId,
        index_id: GlobalIndexId,
        count: u32,
        cancellation: &CancellationToken,
    ) -> EngineResult<GlobalValueLease> {
        self.storage
            .lease_global_values(operation_id, index_id, count, cancellation)
    }

    /// Mark an irrevocable range as successfully consumed.
    pub fn finalize_global_value_lease(
        &self,
        operation_id: GlobalOperationId,
    ) -> EngineResult<GlobalValueLease> {
        self.finalize_global_value_lease_with_cancellation(operation_id, &CancellationToken::new())
    }

    pub fn finalize_global_value_lease_with_cancellation(
        &self,
        operation_id: GlobalOperationId,
        cancellation: &CancellationToken,
    ) -> EngineResult<GlobalValueLease> {
        self.storage
            .transition_global_value_lease(operation_id, true, cancellation)
    }

    /// Abandon an irrevocable range. Its values remain gaps and are never reused.
    pub fn abandon_global_value_lease(
        &self,
        operation_id: GlobalOperationId,
    ) -> EngineResult<GlobalValueLease> {
        self.abandon_global_value_lease_with_cancellation(operation_id, &CancellationToken::new())
    }

    pub fn abandon_global_value_lease_with_cancellation(
        &self,
        operation_id: GlobalOperationId,
        cancellation: &CancellationToken,
    ) -> EngineResult<GlobalValueLease> {
        self.storage
            .transition_global_value_lease(operation_id, false, cancellation)
    }

    /// Atomically apply one legal non-publication global-index lifecycle transition.
    ///
    /// `Ready` is reserved for [`Self::build_global_index`], which proves that
    /// physical storage is complete before atomically publishing the catalog.
    pub fn transition_global_index(
        &mut self,
        index_id: GlobalIndexId,
        target: GlobalIndexLifecycle,
    ) -> EngineResult<()> {
        self.storage.transition_global_index(index_id, target)
    }

    /// Remove a global-index definition after it reaches `Dropping`.
    pub fn remove_global_index(&mut self, index_id: GlobalIndexId) -> EngineResult<()> {
        self.storage.remove_global_index(index_id)
    }

    /// Create and durably register one generated-ID Sharded table from a
    /// documented SQLite, PostgreSQL, or MySQL declaration.
    ///
    /// This initialization-only operation accepts exactly one generated-key
    /// `CREATE TABLE`, translates it to canonical SQLite, installs the physical
    /// schema on every shard, and publishes the matching `native_range_v1`
    /// table policy. Its bridge journal resumes automatically after a process
    /// exit and retains both logical and physical identities for audit.
    pub fn apply_generated_table_ddl(
        &mut self,
        dialect: sql::SqlDialect,
        source: &str,
    ) -> EngineResult<GeneratedTableDdlReceipt> {
        let parsed = sql::parse(dialect, source)?;
        if parsed.statement_count() != 1 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "generated table DDL requires exactly one top-level statement",
            ));
        }
        let common = sql::validate_common_subset(parsed)?;
        let normalized = sql::normalize_placeholders(common)?;
        let translated = sql::translate_sql(normalized, sql::SqlTranslationMode::Compatibility)?;
        let [intent] = translated.generated_table_intents() else {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "generated table DDL requires exactly one generated-key declaration",
            ));
        };
        if intent.statement_index() != 0
            || !validate_catalog_identifier(intent.table())
            || !validate_catalog_identifier(intent.column())
        {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "generated table DDL identifiers must use canonical catalog spelling",
            ));
        }
        let declaration = TableDeclaration::sharded(
            self.catalog().default_database().id(),
            intent.table(),
            ShardKeyMetadata::new(intent.column(), ShardKeyType::Int64)?,
        )?
        .with_generated_id_policy(GeneratedIdPolicy::native_range_v1(intent.column())?)?;
        self.storage.apply_generated_table_ddl(
            dialect,
            translated.source(),
            translated.sqlite_sql(),
            declaration,
        )
    }

    /// Return the immutable logical database and table catalog.
    pub fn catalog(&self) -> &Catalog {
        self.storage.logical_catalog()
    }

    pub fn shard_for_key(&self, key: &[u8]) -> u16 {
        self.storage.shard_for_key(key)
    }

    pub(crate) fn routing_provenance(&self) -> (u32, u32, u32, u64) {
        self.storage.routing_provenance()
    }

    /// Plan a legacy raw statement against the authoritative catalog.
    ///
    /// An empty catalog deliberately returns `None` so pre-catalog callers
    /// retain their existing explicit-route behavior. Once any table has been
    /// registered, every raw data-plane statement must pass the same bounded
    /// SQL frontend, inference, and routing policy as prepared statements.
    pub(crate) fn raw_data_plan(
        &self,
        shard_key: Option<&str>,
        statement: &str,
        params: &[Value],
        operation: RawDataOperation,
    ) -> EngineResult<Option<RawDataPlan>> {
        let catalog = self.catalog();
        if catalog.tables().is_empty() {
            return Ok(None);
        }

        let parsed = sql::parse(sql::SqlDialect::Sqlite, statement)?;
        if parsed.statement_count() != 1 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "catalog-routed raw SQL must contain exactly one top-level statement",
            ));
        }
        let common = sql::validate_common_subset(parsed)?;
        let normalized = sql::normalize_placeholders(common)?;
        let translated = sql::translate_sql(normalized, sql::SqlTranslationMode::StrictSqlite)?;
        let (hash_version, key_encoding_version, bucket_algorithm_version, map_generation) =
            self.routing_provenance();
        let mut plan = planner::plan_bound_statement(
            planner::BoundStatementPlanInput::new(
                catalog,
                catalog.default_database().id(),
                translated.normalized_sql(),
                0,
                params,
                shard_key.map(str::as_bytes),
            )
            .with_allocation_owners(self.storage.allocation_owner_map()),
            planner::RoutingProvenance::new(
                hash_version,
                key_encoding_version,
                bucket_algorithm_version,
                map_generation,
            ),
            |key| self.shard_for_key(key),
        )?;
        let cancellation = CancellationToken::new();
        planner::apply_global_index_routing(
            &mut plan,
            catalog,
            translated.normalized_sql(),
            params,
            self.shard_count(),
            |index_id, keys, predicate, alias, parameters| {
                self.storage.global_index_read_resolution(
                    index_id,
                    keys,
                    predicate,
                    alias,
                    parameters,
                    (&cancellation, None),
                )
            },
        )?;
        let target = raw_data_execution_target(&plan, catalog, operation)?;
        Ok(Some(RawDataPlan {
            target,
            table_id: plan.inference().table_id(),
            sqlite_sql: translated.sqlite_sql().to_owned(),
        }))
    }

    pub fn execute_routed(
        &self,
        shard_key: &str,
        statement: &str,
        params: &[Value],
    ) -> EngineResult<Routed<usize>> {
        let _schema_operation = self.storage.enter_schema_operation()?;
        let plan = self.raw_data_plan(
            Some(shard_key),
            statement,
            params,
            RawDataOperation::Execute,
        )?;
        let shard = plan.as_ref().map_or_else(
            || self.shard_for_key(shard_key.as_bytes()),
            |plan| match plan.target {
                RawDataTarget::Exact(shard) => shard,
                RawDataTarget::Generated(_) => self.shard_for_key(shard_key.as_bytes()),
            },
        );
        if plan
            .as_ref()
            .is_some_and(|plan| matches!(plan.target, RawDataTarget::Generated(_)))
        {
            return Err(EngineError::new(
                EngineErrorKind::Unsupported,
                "generated-key INSERT requires the asynchronous Engine coordinator",
            ));
        }
        if let Some(table_id) = plan.as_ref().and_then(|plan| plan.table_id) {
            if self.catalog().global_indexes().iter().any(|index| {
                index.table_id() == table_id
                    && index.is_unique()
                    && matches!(
                        index.lifecycle(),
                        GlobalIndexLifecycle::Ready | GlobalIndexLifecycle::Invalid
                    )
            }) {
                return Err(EngineError::new(
                    EngineErrorKind::Unsupported,
                    "writes to a ready or invalid globally unique table require the Engine coordinator",
                ));
            }
            self.storage.fence_uncoordinated_nonunique_write(table_id)?;
        }
        let statement = plan
            .as_ref()
            .map_or(statement, |plan| plan.sqlite_sql.as_str());
        let connection = self.storage.open_shard(shard)?;
        let value =
            self.storage
                .fail_closed_on_corruption(sql::execute(&connection, statement, params))?;
        Ok(Routed { shard, value })
    }

    pub fn query_routed(
        &self,
        shard_key: &str,
        statement: &str,
        params: &[Value],
    ) -> EngineResult<Routed<ResultSet>> {
        let _schema_operation = self.storage.enter_schema_operation()?;
        let plan =
            self.raw_data_plan(Some(shard_key), statement, params, RawDataOperation::Query)?;
        let shard = plan.as_ref().map_or_else(
            || self.shard_for_key(shard_key.as_bytes()),
            |plan| match plan.target {
                RawDataTarget::Exact(shard) => shard,
                RawDataTarget::Generated(_) => unreachable!("query planning cannot generate IDs"),
            },
        );
        let statement = plan
            .as_ref()
            .map_or(statement, |plan| plan.sqlite_sql.as_str());
        let connection = self.storage.open_shard(shard)?;
        let value =
            self.storage
                .fail_closed_on_corruption(sql::query(&connection, statement, params))?;
        Ok(Routed { shard, value })
    }

    pub fn execute(
        &self,
        shard_key: &str,
        statement: &str,
        params: &[Value],
    ) -> EngineResult<usize> {
        Ok(self.execute_routed(shard_key, statement, params)?.value)
    }

    pub fn query(
        &self,
        shard_key: &str,
        statement: &str,
        params: &[Value],
    ) -> EngineResult<ResultSet> {
        Ok(self.query_routed(shard_key, statement, params)?.value)
    }

    pub fn broadcast(&self, statement: &str) -> EngineResult<Vec<u16>> {
        let mut migration = self.storage.begin_schema_migration()?;
        migration.wait_for_quiescence_blocking();
        let completed = self
            .storage
            .apply_schema_migration(statement, &mut migration, None)?;
        migration.publish_ready()?;
        Ok(completed)
    }
}

fn raw_data_execution_target(
    plan: &BoundStatementPlan,
    catalog: &Catalog,
    operation: RawDataOperation,
) -> EngineResult<RawDataTarget> {
    let behavior_matches_operation = matches!(
        (operation, plan.behavior()),
        (RawDataOperation::Execute, sql::StatementBehavior::Write(_))
            | (RawDataOperation::Query, sql::StatementBehavior::Read)
    );
    if !behavior_matches_operation {
        return match plan.behavior() {
            sql::StatementBehavior::Schema(_) | sql::StatementBehavior::Session(_) => {
                Err(EngineError::new(
                    EngineErrorKind::Unsupported,
                    "schema and session statements require a dedicated engine operation",
                ))
            }
            sql::StatementBehavior::Read | sql::StatementBehavior::Write(_) => {
                Err(EngineError::new(
                    EngineErrorKind::InvalidQuery,
                    "raw statement behavior does not match the requested operation",
                ))
            }
        };
    }

    let inference = plan.inference();
    let Some(table_id) = inference.table_id() else {
        return if inference.kind() == sql::ShardKeyInferenceKind::NotApplicable
            && plan.behavior() == sql::StatementBehavior::Read
        {
            Ok(RawDataTarget::Exact(0))
        } else {
            Err(EngineError::new(
                EngineErrorKind::Internal,
                "catalog-routed raw planning lost its target table",
            ))
        };
    };
    let table = catalog.table_by_id(table_id).ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "catalog-routed raw planning resolved an unknown table identity",
        )
    })?;

    match table.placement() {
        TablePlacement::Catalog => Err(EngineError::new(
            EngineErrorKind::PermissionDenied,
            "catalog-placed tables cannot execute as client SQL",
        )),
        TablePlacement::Global => match plan.behavior() {
            sql::StatementBehavior::Read => Ok(RawDataTarget::Exact(0)),
            sql::StatementBehavior::Write(_) => Err(EngineError::new(
                EngineErrorKind::Unsupported,
                "raw writes to global tables require an explicit replication operation",
            )),
            sql::StatementBehavior::Schema(_) | sql::StatementBehavior::Session(_) => {
                Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "catalog-routed raw planning assigned non-data behavior to a global table",
                ))
            }
        },
        TablePlacement::Sharded(_) => match inference.kind() {
            sql::ShardKeyInferenceKind::Exact | sql::ShardKeyInferenceKind::Multiple => plan
                .assigned_shard()
                .map(RawDataTarget::Exact)
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::Unsupported,
                        "raw sharded statement does not have one executable physical shard",
                    )
                }),
            sql::ShardKeyInferenceKind::Unconstrained => plan
                .generated_insert()
                .map(|generated| RawDataTarget::Generated(generated.table_id()))
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::Unsupported,
                        "raw sharded statement requires a finite single-shard key constraint",
                    )
                }),
            sql::ShardKeyInferenceKind::Contradiction => Err(EngineError::new(
                EngineErrorKind::Unsupported,
                "raw sharded statement requires a finite single-shard key constraint",
            )),
            sql::ShardKeyInferenceKind::NotApplicable | sql::ShardKeyInferenceKind::NotSharded => {
                Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "catalog-routed raw inference disagrees with sharded table placement",
                ))
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        io::{Seek, SeekFrom, Write},
        sync::{Arc, mpsc},
        thread,
        time::Duration,
    };

    use super::*;

    fn routing_key_for_shard(database: &Database, expected: u16) -> String {
        (0_u64..)
            .map(|value| format!("sync-corruption-{value}"))
            .find(|key| database.shard_for_key(key.as_bytes()) == expected)
            .expect("the finite shard layout has a routing key")
    }

    fn integer_key_for_shard(database: &Database, expected: u16, excluded: Option<i64>) -> i64 {
        (1_i64..)
            .find(|value| {
                Some(*value) != excluded
                    && database.shard_for_key(value.to_string().as_bytes()) == expected
            })
            .expect("the finite shard layout has an integer routing key")
    }

    fn database_with_raw_catalog() -> (tempfile::TempDir, Database) {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 4).unwrap();
        database
            .broadcast(
                "CREATE TABLE events (
                    tenant_id INTEGER NOT NULL,
                    sequence INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, sequence)
                 );
                 CREATE TABLE global_events (code TEXT PRIMARY KEY, label TEXT NOT NULL);
                 CREATE VIEW undeclared_events AS
                 SELECT tenant_id, sequence, payload FROM events;",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical_database,
                    "events",
                    ShardKeyMetadata::new("tenant_id", ShardKeyType::Int64).unwrap(),
                )
                .unwrap(),
                TableDeclaration::global(logical_database, "global_events").unwrap(),
                TableDeclaration::catalog(logical_database, "catalog_records").unwrap(),
            ])
            .unwrap();
        (temp, database)
    }

    fn corrupt_application_table_root(root: &Path, shard: u16, table: &str) {
        let path = root.join(format!("shards/{shard:04}.sqlite"));
        let connection = rusqlite::Connection::open(&path).unwrap();
        let page_size = u64::try_from(
            connection
                .pragma_query_value(None, "page_size", |row| row.get::<_, i64>(0))
                .unwrap(),
        )
        .unwrap();
        let root_page = u64::try_from(
            connection
                .query_row(
                    "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = ?1",
                    [table],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
        )
        .unwrap();
        connection
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .unwrap();
        drop(connection);

        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start((root_page - 1) * page_size))
            .unwrap();
        file.write_all(&[0]).unwrap();
        file.sync_all().unwrap();
    }

    #[test]
    fn routing_is_stable() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 4).unwrap();

        let first = database.shard_for_key(b"customer-42");
        assert_eq!(first, database.shard_for_key(b"customer-42"));
        assert!(first < 4);
    }

    #[test]
    fn catalog_lookup_preserves_legacy_v1_placement() {
        let keys: [&[u8]; 6] = [
            b"",
            b"customer-42",
            b"tenant/alpha",
            b"non-power-of-two",
            &[0, 1, 2, 0xff],
            "snowman-☃".as_bytes(),
        ];
        for shard_count in [3_u16, 5, 6, 10, 63, 64] {
            let temp = tempfile::tempdir().unwrap();
            let database = Database::open(temp.path(), shard_count).unwrap();
            for key in keys {
                let digest = blake3::hash(key);
                let hash = u64::from_le_bytes(digest.as_bytes()[..8].try_into().unwrap());
                assert_eq!(
                    database.shard_for_key(key),
                    (hash % u64::from(shard_count)) as u16
                );
            }
        }
    }

    #[test]
    fn routing_is_stable_across_close_and_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let keys: [&[u8]; 6] = [
            b"",
            b"customer-42",
            b"tenant/alpha",
            b"a\0b",
            &[0, 1, 2, 0xff],
            "snowman-☃".as_bytes(),
        ];
        let before = {
            let database = Database::open(temp.path(), 10).unwrap();
            keys.map(|key| database.shard_for_key(key))
        };

        let reopened = Database::open(temp.path(), 10).unwrap();
        assert_eq!(
            keys.map(|key| reopened.shard_for_key(key)),
            before,
            "reopening must load the same persisted routing snapshot"
        );
    }

    #[test]
    fn an_open_database_routes_from_its_immutable_validated_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 4).unwrap();
        let keys: [&[u8]; 4] = [b"alpha", b"beta", b"a\0b", &[0, 1, 2, 0xff]];
        let expected = keys.map(|key| database.shard_for_key(key));

        let manifest = rusqlite::Connection::open(temp.path().join("manifest.sqlite")).unwrap();
        manifest
            .execute(
                "UPDATE briskdb_virtual_buckets
                 SET physical_shard_id = (physical_shard_id + 1) % 4",
                [],
            )
            .unwrap();
        drop(manifest);

        assert_eq!(keys.map(|key| database.shard_for_key(key)), expected);
        let error = Database::open(temp.path(), 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
    }

    #[test]
    fn routed_execute_and_query_report_the_selected_shard() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 4).unwrap();
        database
            .broadcast("CREATE TABLE widgets (id TEXT PRIMARY KEY, name TEXT NOT NULL);")
            .unwrap();

        let write = database
            .execute_routed(
                "widget-1",
                "INSERT INTO widgets (id, name) VALUES (?1, ?2)",
                &[Value::from("widget-1"), Value::from("First widget")],
            )
            .unwrap();
        let read = database
            .query_routed(
                "widget-1",
                "SELECT id, name FROM widgets WHERE id = ?1",
                &[Value::from("widget-1")],
            )
            .unwrap();

        let expected_shard = database.shard_for_key(b"widget-1");
        assert_eq!(
            write,
            Routed {
                shard: expected_shard,
                value: 1
            }
        );
        assert_eq!(read.shard, expected_shard);
        assert_eq!(
            read.value,
            ResultSet::new(
                vec![
                    Column::new("id", DataType::Text),
                    Column::new("name", DataType::Text),
                ],
                vec![Row::new(vec![
                    Value::from("widget-1"),
                    Value::from("First widget"),
                ])],
            )
            .unwrap()
        );
    }

    #[test]
    fn compatibility_execute_and_query_methods_keep_their_results() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 4).unwrap();
        assert_eq!(
            database
                .broadcast("CREATE TABLE widgets (id TEXT PRIMARY KEY, name TEXT NOT NULL);")
                .unwrap(),
            vec![0, 1, 2, 3]
        );

        assert_eq!(
            database
                .execute(
                    "widget-1",
                    "INSERT INTO widgets (id, name) VALUES (?1, ?2)",
                    &[Value::from("widget-1"), Value::from("First widget")],
                )
                .unwrap(),
            1
        );
        assert_eq!(
            database
                .query(
                    "widget-1",
                    "SELECT id, name FROM widgets WHERE id = ?1",
                    &[Value::from("widget-1")],
                )
                .unwrap(),
            ResultSet::new(
                vec![
                    Column::new("id", DataType::Text),
                    Column::new("name", DataType::Text),
                ],
                vec![Row::new(vec![
                    Value::from("widget-1"),
                    Value::from("First widget"),
                ])],
            )
            .unwrap()
        );
    }

    #[test]
    fn populated_catalog_routes_raw_point_operations_without_replicating_rows() {
        let (_temp, database) = database_with_raw_catalog();
        let first_key = integer_key_for_shard(&database, 0, None);
        let second_key = integer_key_for_shard(&database, 0, Some(first_key));
        let first_route = first_key.to_string();
        let second_route = second_key.to_string();

        let write = database
            .execute_routed(
                &first_route,
                "INSERT INTO events (tenant_id, sequence, payload)
                 VALUES (?1, ?2, ?3), (?4, ?5, ?6)",
                &[
                    Value::from(first_key),
                    Value::from(1_i64),
                    Value::from("first"),
                    Value::from(second_key),
                    Value::from(2_i64),
                    Value::from("second"),
                ],
            )
            .unwrap();
        assert_eq!(write.shard, 0);
        assert_eq!(write.value, 2);

        let read = database
            .query_routed(
                &second_route,
                "SELECT payload FROM events WHERE tenant_id = ?1",
                &[Value::from(second_key)],
            )
            .unwrap();
        assert_eq!(read.shard, 0);
        assert_eq!(read.value.rows().len(), 1);
        assert_eq!(read.value.rows()[0].get(0), Some(&Value::from("second")));

        let same_shard_read = database
            .query_routed(
                &first_route,
                "SELECT payload FROM events
                 WHERE tenant_id = ?1 OR tenant_id = ?2",
                &[Value::from(first_key), Value::from(second_key)],
            )
            .unwrap();
        assert_eq!(same_shard_read.shard, 0);
        assert_eq!(same_shard_read.value.rows().len(), 2);

        for shard in 0..database.shard_count() {
            let connection = database.storage.open_shard(shard).unwrap();
            let row_count: i64 = connection
                .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
                .unwrap();
            assert_eq!(row_count, if shard == 0 { 2 } else { 0 }, "shard {shard}");
        }
    }

    #[test]
    fn populated_catalog_raw_gate_rejects_unsafe_routes_and_targets() {
        let (_temp, database) = database_with_raw_catalog();
        let shard_zero_value = integer_key_for_shard(&database, 0, None);
        let shard_one_value = integer_key_for_shard(&database, 1, None);
        let shard_zero_key = shard_zero_value.to_string();
        let shard_one_key = shard_one_value.to_string();

        let global = database
            .query_routed(
                &shard_one_key,
                "SELECT code FROM global_events WHERE code = ?1",
                &[Value::from("US")],
            )
            .unwrap();
        assert_eq!(global.shard, 0);
        assert!(global.value.rows().is_empty());

        for (error, expected) in [
            (
                database
                    .execute(
                        &shard_zero_key,
                        "INSERT INTO global_events (code, label) VALUES (?1, ?2)",
                        &[Value::from("US"), Value::from("United States")],
                    )
                    .unwrap_err(),
                EngineErrorKind::Unsupported,
            ),
            (
                database
                    .query(&shard_zero_key, "SELECT * FROM catalog_records", &[])
                    .unwrap_err(),
                EngineErrorKind::PermissionDenied,
            ),
            (
                database
                    .query(&shard_zero_key, "SELECT * FROM undeclared_events", &[])
                    .unwrap_err(),
                EngineErrorKind::InvalidQuery,
            ),
            (
                database
                    .query(&shard_zero_key, "SELECT payload FROM events", &[])
                    .unwrap_err(),
                EngineErrorKind::Unsupported,
            ),
            (
                database
                    .execute(
                        &shard_zero_key,
                        "UPDATE events SET payload = ?1",
                        &[Value::from("unsafe")],
                    )
                    .unwrap_err(),
                EngineErrorKind::Unsupported,
            ),
            (
                database
                    .query(
                        &shard_one_key,
                        "SELECT payload FROM events WHERE tenant_id = ?1",
                        &[Value::from(shard_zero_value)],
                    )
                    .unwrap_err(),
                EngineErrorKind::InvalidArgument,
            ),
            (
                database
                    .query(
                        &shard_zero_key,
                        "SELECT payload FROM events
                         WHERE tenant_id = ?1 OR tenant_id = ?2",
                        &[Value::from(shard_zero_value), Value::from(shard_one_value)],
                    )
                    .unwrap_err(),
                EngineErrorKind::InvalidArgument,
            ),
            (
                database
                    .execute(&shard_zero_key, "CREATE TABLE bypass (id INTEGER)", &[])
                    .unwrap_err(),
                EngineErrorKind::Unsupported,
            ),
        ] {
            assert_eq!(error.kind(), expected, "{}", error.diagnostic());
        }
    }

    #[test]
    fn database_preserves_error_kinds_across_the_engine_boundary() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 4).unwrap();

        assert_eq!(
            database
                .query("widget-1", "SELECT * FROM missing_table", &[])
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidQuery
        );
        assert_eq!(
            Database::open(temp.path(), 1).unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
    }

    #[test]
    fn synchronous_query_and_execute_corruption_persist_terminal_degraded_state() {
        for operation in ["query", "execute"] {
            let temp = tempfile::tempdir().unwrap();
            let database = Database::open(temp.path(), 2).unwrap();
            database
                .broadcast(
                    "CREATE TABLE corrupt_application_page (
                         id INTEGER PRIMARY KEY,
                         value TEXT NOT NULL
                     )",
                )
                .unwrap();
            let routing_key = routing_key_for_shard(&database, 0);
            database
                .execute(
                    &routing_key,
                    "INSERT INTO corrupt_application_page(value) VALUES (?1)",
                    &[Value::from("before-corruption")],
                )
                .unwrap();
            corrupt_application_table_root(temp.path(), 0, "corrupt_application_page");

            let error = match operation {
                "query" => database
                    .query(
                        &routing_key,
                        "SELECT value FROM corrupt_application_page",
                        &[],
                    )
                    .map(|_| ()),
                "execute" => database
                    .execute(
                        &routing_key,
                        "INSERT INTO corrupt_application_page(value) VALUES (?1)",
                        &[Value::from("after-corruption")],
                    )
                    .map(|_| ()),
                _ => unreachable!(),
            }
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption, "{operation}");
            assert_eq!(
                database
                    .query(&routing_key, "SELECT 1", &[])
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::DataCorruption,
                "{operation} must leave shared admission fail-closed"
            );

            let manifest = rusqlite::Connection::open(temp.path().join("manifest.sqlite")).unwrap();
            assert_eq!(
                manifest
                    .query_row(
                        "SELECT database_state FROM briskdb_integrity WHERE singleton = 1",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                4,
                "{operation} must persist Degraded"
            );
            drop(manifest);
            drop(database);
            assert_eq!(
                Database::open(temp.path(), 2).unwrap_err().kind(),
                EngineErrorKind::DataCorruption,
                "{operation} restart must remain fail-closed"
            );
        }
    }

    #[test]
    fn public_broadcast_corruption_preserves_checksummed_journal_in_terminal_degraded() {
        const SQL: &str = "CREATE TABLE durable_migration(id INTEGER PRIMARY KEY)";

        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 2).unwrap());
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        database
            .storage
            .install_schema_migration_test_block(
                crate::storage::SchemaMigrationCoordinatorPoint::JournalCommitted,
                started_tx,
                release_rx,
            )
            .unwrap();

        let worker_database = Arc::clone(&database);
        let worker = thread::spawn(move || worker_database.broadcast(SQL));
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("migration should commit its journal before shard application");

        let manifest_path = temp.path().join("manifest.sqlite");
        let manifest = rusqlite::Connection::open(&manifest_path).unwrap();
        let trusted_digests: (Vec<u8>, Vec<u8>) = manifest
            .query_row(
                "SELECT committed_schema_digest, target_schema_digest
                 FROM briskdb_integrity WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(trusted_digests.0.len(), 32);
        assert_eq!(trusted_digests.1.len(), 32);
        drop(manifest);

        rusqlite::Connection::open(temp.path().join("shards/0000.sqlite"))
            .unwrap()
            .execute_batch("CREATE TABLE injected_migration_drift(value TEXT)")
            .unwrap();
        release_tx.send(()).unwrap();
        let error = worker.join().unwrap().unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);

        let manifest = rusqlite::Connection::open(&manifest_path).unwrap();
        let persisted: (i64, Vec<u8>, Vec<u8>, i64, i64) = manifest
            .query_row(
                "SELECT i.database_state,
                        i.committed_schema_digest,
                        i.target_schema_digest,
                        m.migration_state,
                        m.next_shard
                 FROM briskdb_integrity AS i
                 JOIN briskdb_schema_migrations AS m
                   ON m.migration_state = 1
                 WHERE i.singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(persisted.0, 4);
        assert_eq!((persisted.1, persisted.2), trusted_digests);
        assert_eq!((persisted.3, persisted.4), (1, 0));
        drop(manifest);

        assert_eq!(
            database
                .query("blocked", "SELECT 1", &[])
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
        drop(database);
        assert_eq!(
            Database::open(temp.path(), 2).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn broadcast_preflight_failure_changes_no_shard_and_can_retry() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 4).unwrap();
        database
            .broadcast("CREATE TABLE recovery_marker (id INTEGER NOT NULL);")
            .unwrap();
        let shard_one = database.storage.open_shard(1).unwrap();
        shard_one
            .execute_batch("INSERT INTO recovery_marker VALUES (1), (1)")
            .unwrap();

        let error = database
            .broadcast("CREATE UNIQUE INDEX recovery_marker_id ON recovery_marker (id);")
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::UniqueViolation);
        assert_eq!(
            error.to_string(),
            "schema migration preflight failed on shard 1"
        );

        let shard_zero = database.storage.open_shard(0).unwrap();
        let shard_two = database.storage.open_shard(2).unwrap();
        let index_exists = |connection: &rusqlite::Connection| {
            connection
                .query_row(
                    "SELECT EXISTS (
                        SELECT 1 FROM sqlite_schema
                        WHERE type = 'index' AND name = 'recovery_marker_id'
                    )",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        };
        assert!(!index_exists(&shard_zero));
        assert!(!index_exists(&shard_two));

        shard_one
            .execute("DELETE FROM recovery_marker WHERE rowid = 2", [])
            .unwrap();

        assert_eq!(
            database
                .broadcast("CREATE UNIQUE INDEX recovery_marker_id ON recovery_marker (id);")
                .unwrap(),
            [0, 1, 2, 3]
        );
        assert!(index_exists(&shard_zero));
        assert!(index_exists(&shard_two));
    }
}
