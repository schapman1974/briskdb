//! Asynchronous protocol-neutral engine boundary.

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::{
    sync::OwnedMutexGuard,
    task::{JoinHandle, JoinSet},
};

use super::session::TransactionState;
use super::{
    BlockingPool, BoundStatementPlan, CancelOnDrop, CancellationReason, CancellationToken,
    Database, DescribeTarget, EngineError, EngineErrorKind, EngineOptions, EngineResult,
    EngineState, Executed, Lifecycle, LogicalDatabaseId, OperationControl, OperationLease,
    PortalId, PrepareRequest, PreparedExecution, PreparedStatementDescription, PreparedStatementId,
    PreparedStatementLimits, RawDataOperation, RawDataTarget, RequestContext, ResultLimits,
    ResultSet, Routed, Session, SessionInner, ShutdownReport, TablePlacement, TransactionExecution,
    Value, merge_scatter_results, wait_for_cancellation, wait_pending,
};
use crate::{
    sql,
    storage::{ConnectionOwner, ConnectionPools, PooledConnection, SchemaOperationGuard},
};

#[cfg(feature = "experimental-vtab")]
use crate::storage::{RegistrySchemaCache, WriteCoordinator};

static NEXT_ENGINE_ID: AtomicU64 = AtomicU64::new(1);
const MAX_SCATTER_CONCURRENCY: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecuteOwnerPolicy {
    Session,
    #[cfg_attr(not(feature = "http"), allow(dead_code))]
    ReuseValidatedCatalogWrite,
}

#[cfg(feature = "experimental-vtab")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeneratedWriteTarget {
    Exact(super::TableId, u16),
    NativeAuto(super::TableId),
    HiloAuto(super::TableId),
}

#[cfg(feature = "experimental-vtab")]
impl GeneratedWriteTarget {
    const fn admission_shard(self) -> u16 {
        match self {
            Self::Exact(_, shard) => shard,
            // Auto allocation determines its physical shard inside the
            // coordinator. Native range acquires one candidate capacity at a
            // time; hi/lo reserves every candidate because consuming its
            // allocation irrevocably determines the hash route.
            Self::NativeAuto(_) | Self::HiloAuto(_) => 0,
        }
    }
}

/// An owned SQL statement and its protocol-neutral parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct Statement {
    sql: String,
    params: Vec<Value>,
}

impl Statement {
    /// Construct an owned statement suitable for asynchronous execution.
    pub fn new(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }

    /// Return the SQL text.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Return the ordered bound parameters.
    pub fn params(&self) -> &[Value] {
        &self.params
    }

    /// Consume the statement into its SQL text and parameters.
    pub fn into_parts(self) -> (String, Vec<Value>) {
        (self.sql, self.params)
    }
}

/// Read-only engine information returned through the async boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineStatus {
    shard_count: u16,
    max_blocking_workers: usize,
    connections_per_shard: usize,
    queue_capacity_per_shard: usize,
    max_result_rows: u64,
    max_result_bytes: u64,
    prepared_statement_limits: PreparedStatementLimits,
    request_timeout: Option<Duration>,
    shutdown_grace: Duration,
}

/// Result of one passive WAL checkpoint on a physical shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointShardReport {
    shard: u16,
    busy: bool,
    counts_available: bool,
    wal_frames: u64,
    checkpointed_frames: u64,
}

impl CheckpointShardReport {
    /// Return the physical shard that was checkpointed.
    pub const fn shard(self) -> u16 {
        self.shard
    }

    /// Return whether SQLite reported a competing checkpoint operation.
    pub const fn busy(self) -> bool {
        self.busy
    }

    /// Return whether SQLite supplied WAL and checkpointed frame counts.
    ///
    /// A competing checkpoint can make both counts unavailable while still
    /// producing a successful, busy report. In that case the count accessors
    /// return zero and [`CheckpointShardReport::complete`] returns `false`.
    pub const fn counts_available(self) -> bool {
        self.counts_available
    }

    /// Return the number of frames SQLite observed in the WAL, or zero when
    /// [`CheckpointShardReport::counts_available`] is `false`.
    pub const fn wal_frames(self) -> u64 {
        self.wal_frames
    }

    /// Return the number of frames SQLite copied into the database file, or
    /// zero when [`CheckpointShardReport::counts_available`] is `false`.
    pub const fn checkpointed_frames(self) -> u64 {
        self.checkpointed_frames
    }

    /// Return whether every WAL frame observed by this attempt was copied.
    pub const fn complete(self) -> bool {
        self.counts_available && self.checkpointed_frames >= self.wal_frames
    }
}

fn checkpoint_shard_report(
    shard: u16,
    busy: i64,
    wal_frames: i64,
    checkpointed_frames: i64,
) -> EngineResult<CheckpointShardReport> {
    if wal_frames == -1 && checkpointed_frames == -1 && busy != 0 {
        // sqlite3_wal_checkpoint_v2() uses -1 for both output counts when a
        // competing process already owns the checkpoint lock. Preserve the
        // successful retryable report without inventing frame counts.
        return Ok(CheckpointShardReport {
            shard,
            busy: true,
            counts_available: false,
            wal_frames: 0,
            checkpointed_frames: 0,
        });
    }
    let wal_frames = u64::try_from(wal_frames).map_err(|_| {
        EngineError::new(
            EngineErrorKind::DataCorruption,
            "SQLite returned an invalid WAL frame count",
        )
    })?;
    let checkpointed_frames = u64::try_from(checkpointed_frames).map_err(|_| {
        EngineError::new(
            EngineErrorKind::DataCorruption,
            "SQLite returned an invalid checkpointed frame count",
        )
    })?;
    Ok(CheckpointShardReport {
        shard,
        busy: busy != 0,
        counts_available: true,
        wal_frames,
        checkpointed_frames,
    })
}

/// Ordered result of a passive checkpoint across every physical shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointReport {
    shards: Vec<CheckpointShardReport>,
}

impl CheckpointReport {
    /// Return one report for every physical shard, ordered by shard ID.
    pub fn shards(&self) -> &[CheckpointShardReport] {
        &self.shards
    }

    /// Return whether any shard reported incomplete checkpoint progress.
    pub fn busy(&self) -> bool {
        self.shards.iter().any(|shard| shard.busy())
    }

    /// Return whether every shard checkpoint copied all frames it observed.
    pub fn complete(&self) -> bool {
        self.shards.iter().all(|shard| shard.complete())
    }
}

impl EngineStatus {
    /// Return the number of physical shards opened by the engine.
    pub const fn shard_count(&self) -> u16 {
        self.shard_count
    }

    /// Return the maximum number of BriskDB SQLite tasks admitted to Tokio's
    /// blocking workers at once.
    pub const fn max_blocking_workers(&self) -> usize {
        self.max_blocking_workers
    }

    /// Return the maximum active SQLite connections for each shard.
    pub const fn connections_per_shard(&self) -> usize {
        self.connections_per_shard
    }

    /// Return the maximum number of additional queued requests for each shard.
    pub const fn queue_capacity_per_shard(&self) -> usize {
        self.queue_capacity_per_shard
    }

    /// Return the maximum rows retained by one query.
    pub const fn max_result_rows(&self) -> u64 {
        self.max_result_rows
    }

    /// Return the maximum protocol-neutral logical bytes retained by one query.
    pub const fn max_result_bytes(&self) -> u64 {
        self.max_result_bytes
    }

    /// Return the finite per-session prepared-statement and portal limits.
    pub const fn prepared_statement_limits(&self) -> PreparedStatementLimits {
        self.prepared_statement_limits
    }

    /// Return the engine-wide request timeout, if enabled.
    pub const fn request_timeout(&self) -> Option<Duration> {
        self.request_timeout
    }

    /// Return the graceful-shutdown drain period.
    pub const fn shutdown_grace(&self) -> Duration {
        self.shutdown_grace
    }
}

#[derive(Debug)]
struct EngineInner {
    id: u64,
    database: Arc<Database>,
    options: EngineOptions,
    workers: BlockingPool,
    connections: ConnectionPools,
    lifecycle: Arc<Lifecycle>,
    shutdown_cancel: CancellationToken,
    shutdown_gate: Arc<tokio::sync::Mutex<()>>,
    #[cfg(feature = "experimental-vtab")]
    registry_schema_cache: Arc<RegistrySchemaCache>,
    #[cfg(feature = "experimental-vtab")]
    registry_bootstrap_gate: Arc<tokio::sync::Mutex<()>>,
}

struct Operation {
    lease: Option<OperationLease>,
    control: Arc<OperationControl>,
    cancellation: CancellationToken,
    shutdown_cancel: CancellationToken,
    deadline: Option<Instant>,
    result_limits: ResultLimits,
    cancel_on_drop: CancelOnDrop,
}

impl Operation {
    async fn wait_pending<T, F>(&self, future: F) -> EngineResult<T>
    where
        F: std::future::Future<Output = EngineResult<T>>,
    {
        wait_pending(
            future,
            &self.cancellation,
            &self.shutdown_cancel,
            self.deadline,
            &self.control,
        )
        .await
    }

    fn check_before_start(&self) -> EngineResult<()> {
        let reason = if self.cancellation.is_cancelled() || self.shutdown_cancel.is_cancelled() {
            Some(CancellationReason::Cancelled)
        } else if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            Some(CancellationReason::DeadlineExceeded)
        } else {
            None
        };
        if let Some(reason) = reason {
            self.control.request_cancel(reason);
            return Err(reason.error());
        }
        Ok(())
    }

    fn take_lease(&mut self) -> OperationLease {
        self.lease
            .take()
            .expect("an operation moves its lifecycle lease only once")
    }

    fn finish<T>(&mut self, result: EngineResult<T>) -> EngineResult<T> {
        let result = self.control.complete(result);
        self.cancel_on_drop.disarm();
        result
    }

    fn finish_started<T>(&mut self, result: EngineResult<T>) -> EngineResult<T> {
        self.cancel_on_drop.disarm();
        result
    }

    async fn wait_started<T>(&self, mut join: JoinHandle<EngineResult<T>>) -> EngineResult<T>
    where
        T: Send + 'static,
    {
        tokio::select! {
            biased;
            result = &mut join => flatten_join(result),
            reason = wait_for_cancellation(
                &self.cancellation,
                &self.shutdown_cancel,
                self.deadline,
            ) => {
                self.control.request_cancel(reason);
                flatten_join(join.await)
            }
        }
    }
}

/// Shared asynchronous entry point used by every network frontend.
///
/// Clones refer to the same engine identity and database. SQLite remains
/// blocking internally, so operations acquire bounded per-shard admission and
/// connection capacity before handing owned work to Tokio blocking workers.
#[derive(Debug, Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

impl Engine {
    /// Open a database without blocking the async runtime executor.
    pub async fn open(root: impl AsRef<Path>, requested_shards: u16) -> EngineResult<Self> {
        Self::open_with_options(root, requested_shards, EngineOptions::default()).await
    }

    /// Open a database with explicit worker and per-shard pool limits.
    pub async fn open_with_options(
        root: impl AsRef<Path>,
        requested_shards: u16,
        options: EngineOptions,
    ) -> EngineResult<Self> {
        crate::storage::validate_shard_count(requested_shards)?;
        let root = PathBuf::from(root.as_ref());
        let worker_limit = options.worker_limit(requested_shards)?;
        let workers = BlockingPool::new(worker_limit);
        let database = workers
            .run(move || Database::open(root, requested_shards))
            .await?;
        Self::from_parts(Arc::new(database), options, workers)
    }

    /// Detect an initialized database's immutable shard count and open it.
    ///
    /// Detection runs on a blocking worker and does not create a missing data
    /// directory or manifest. The normal open validates the same count again
    /// before establishing pools, closing replacement races safely.
    pub async fn open_detected_with_options(
        root: impl AsRef<Path>,
        options: EngineOptions,
    ) -> EngineResult<Self> {
        let root = PathBuf::from(root.as_ref());
        let detect_root = root.clone();
        let requested_shards =
            tokio::task::spawn_blocking(move || Database::detect_shard_count(detect_root))
                .await
                .map_err(|error| {
                    EngineError::from_source(
                        EngineErrorKind::Internal,
                        "shard-count discovery worker failed",
                        error,
                    )
                })??;
        Self::open_with_options(root, requested_shards, options).await
    }

    /// Wrap the synchronous compatibility API in the shared async boundary.
    pub fn from_database(database: Arc<Database>) -> Self {
        Self::from_database_with_options(database, EngineOptions::default())
            .expect("default engine options are valid for every supported database")
    }

    /// Wrap the synchronous compatibility API with explicit pool limits.
    pub fn from_database_with_options(
        database: Arc<Database>,
        options: EngineOptions,
    ) -> EngineResult<Self> {
        let workers = BlockingPool::new(options.worker_limit(database.shard_count())?);
        Self::from_parts(database, options, workers)
    }

    fn from_parts(
        database: Arc<Database>,
        options: EngineOptions,
        workers: BlockingPool,
    ) -> EngineResult<Self> {
        let connections = ConnectionPools::new(
            database.storage.clone(),
            options.connections_per_shard(),
            options.queue_capacity_per_shard(),
        )?;
        Ok(Self {
            inner: Arc::new(EngineInner {
                id: NEXT_ENGINE_ID.fetch_add(1, Ordering::Relaxed),
                database,
                options,
                workers,
                connections,
                lifecycle: Lifecycle::new(),
                shutdown_cancel: CancellationToken::new(),
                shutdown_gate: Arc::new(tokio::sync::Mutex::new(())),
                #[cfg(feature = "experimental-vtab")]
                registry_schema_cache: Arc::new(RegistrySchemaCache::new()),
                #[cfg(feature = "experimental-vtab")]
                registry_bootstrap_gate: Arc::new(tokio::sync::Mutex::new(())),
            }),
        })
    }

    #[cfg(feature = "experimental-vtab")]
    fn generated_write_target(
        &self,
        table: super::TableId,
        policy: &super::GeneratedIdPolicy,
    ) -> EngineResult<GeneratedWriteTarget> {
        match policy {
            super::GeneratedIdPolicy::NativeRangeV1 { .. } => {
                let owners = self
                    .inner
                    .database
                    .storage
                    .allocation_owner_map()
                    .ok_or_else(|| {
                        EngineError::new(
                            EngineErrorKind::DataCorruption,
                            "native generated INSERT has no allocation-owner map",
                        )
                    })?;
                if (0..self.shard_count())
                    .any(|shard| owners.owner_for_physical_shard(shard).is_some())
                {
                    return Ok(GeneratedWriteTarget::NativeAuto(table));
                }
                Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "native generated INSERT has no active allocation owner",
                ))
            }
            // Hilo allocation itself determines the hash-routed physical
            // shard. Keep that later decision inside the coordinator so the
            // irrevocably consumed lease and write remain one operation.
            super::GeneratedIdPolicy::HiloV1 { .. } => Ok(GeneratedWriteTarget::HiloAuto(table)),
            super::GeneratedIdPolicy::None => Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "generated INSERT reached a table without a generated-ID policy",
            )),
        }
    }

    #[cfg(feature = "experimental-vtab")]
    fn generated_write_target_for_table(
        &self,
        table: super::TableId,
    ) -> EngineResult<GeneratedWriteTarget> {
        let metadata = self.catalog().table_by_id(table).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                "generated INSERT refers to missing catalog metadata",
            )
        })?;
        self.generated_write_target(table, metadata.generated_id_policy())
    }

    /// Create a new frontend-owned session.
    pub fn session(&self) -> Session {
        Session::new(
            self.inner.id,
            self.inner.options.prepared_statement_limits(),
        )
    }

    /// Return the configured physical shard count.
    pub fn shard_count(&self) -> u16 {
        self.inner.database.shard_count()
    }

    /// Return the engine admission bound for concurrently blocking SQLite tasks.
    ///
    /// This is not Tokio's runtime thread count; embedding runtimes configure
    /// their own blocking-thread capacity independently.
    pub(crate) fn blocking_task_admission_limit(&self) -> usize {
        self.inner.workers.limit()
    }

    /// Return the immutable logical database and table catalog.
    pub fn catalog(&self) -> &super::Catalog {
        self.inner.database.catalog()
    }

    #[cfg(test)]
    pub(crate) fn pool_snapshot_for_test(
        &self,
    ) -> EngineResult<crate::storage::pool::PoolSnapshot> {
        self.inner.connections.snapshot()
    }

    /// Plan one normalized statement from its actual bound parameter values.
    ///
    /// `statement_index` is zero-based. Planning infers routes, retains an
    /// optional explicit fallback, applies single-shard write policy, and
    /// returns the assigned physical shard when one is valid. Finite inferred
    /// routes and an explicit route must select the same physical shard.
    /// Planning remains synchronous and does not prepare or execute SQL.
    pub fn plan_bound_statement(
        &self,
        database: LogicalDatabaseId,
        normalized: &sql::NormalizedSql,
        statement_index: usize,
        parameters: &[Value],
        explicit_routing_key: Option<&[u8]>,
    ) -> EngineResult<BoundStatementPlan> {
        let _schema_operation = self.inner.database.storage.enter_schema_operation()?;
        self.plan_bound_statement_admitted(
            database,
            normalized,
            statement_index,
            parameters,
            explicit_routing_key,
        )
    }

    fn plan_bound_statement_admitted(
        &self,
        database: LogicalDatabaseId,
        normalized: &sql::NormalizedSql,
        statement_index: usize,
        parameters: &[Value],
        explicit_routing_key: Option<&[u8]>,
    ) -> EngineResult<BoundStatementPlan> {
        let (hash_version, key_encoding_version, bucket_algorithm_version, map_generation) =
            self.inner.database.routing_provenance();
        super::planner::plan_bound_statement(
            super::planner::BoundStatementPlanInput::new(
                self.catalog(),
                database,
                normalized,
                statement_index,
                parameters,
                explicit_routing_key,
            )
            .with_allocation_owners(self.inner.database.storage.allocation_owner_map()),
            super::planner::RoutingProvenance::new(
                hash_version,
                key_encoding_version,
                bucket_algorithm_version,
                map_generation,
            ),
            |key| self.inner.database.shard_for_key(key),
        )
    }

    fn logical_raw_query_plan(
        &self,
        statement: &str,
        parameters: &[Value],
    ) -> EngineResult<(Vec<u16>, String)> {
        let parsed = sql::parse(sql::SqlDialect::Sqlite, statement)?;
        if parsed.statement_count() != 1 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "logical raw SQL must contain exactly one top-level statement",
            ));
        }
        let common = sql::validate_common_subset(parsed)?;
        let normalized = sql::normalize_placeholders(common)?;
        let translated = sql::translate_sql(normalized, sql::SqlTranslationMode::StrictSqlite)?;
        let plan = self.plan_bound_statement_admitted(
            self.catalog().default_database().id(),
            translated.normalized_sql(),
            0,
            parameters,
            None,
        )?;
        if !matches!(plan.behavior(), sql::StatementBehavior::Read) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidQuery,
                "logical query statements must be read-only",
            ));
        }
        let shards = prepared_execution_shards(&plan, self.catalog(), self.shard_count())?;
        if shards.len() > 1 {
            sql::validate_scatter_safe(&translated)?;
        }
        Ok((shards, translated.sqlite_sql().to_owned()))
    }

    /// Return the engine's immutable pool and admission options.
    pub fn options(&self) -> EngineOptions {
        self.inner.options
    }

    /// Return the lifecycle state shared by every engine clone.
    pub fn state(&self) -> EngineState {
        self.inner.lifecycle.state()
    }

    #[cfg(test)]
    pub(crate) fn active_operations_for_test(&self) -> usize {
        self.inner.lifecycle.active()
    }

    /// Stop admitting new work while allowing already-admitted operations to drain.
    ///
    /// This transition is synchronous, monotonic, and idempotent. Call
    /// [`Engine::shutdown`] to await cleanup of SQLite handles.
    pub fn begin_shutdown(&self) -> EngineState {
        self.inner.lifecycle.begin_shutdown()
    }

    /// Drain admitted operations and close idle SQLite handles.
    ///
    /// If the configured grace period elapses, admitted requests are cancelled
    /// and given one additional grace period to finish SQLite cleanup. A second
    /// timeout leaves the engine safely in `Draining`; a later call resumes the
    /// shutdown attempt.
    pub async fn shutdown(&self) -> EngineResult<ShutdownReport> {
        self.shutdown_with_grace(self.inner.options.shutdown_grace())
            .await
    }

    /// Perform shutdown with an explicit finite grace period.
    pub async fn shutdown_with_grace(&self, grace: Duration) -> EngineResult<ShutdownReport> {
        if grace.is_zero() {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "shutdown grace period must be greater than zero",
            ));
        }

        let shutdown_guard = Arc::clone(&self.inner.shutdown_gate).lock_owned().await;
        if let Some(report) = self.inner.lifecycle.report() {
            return Ok(report);
        }
        self.begin_shutdown();

        let forced_now = if tokio::time::timeout(grace, self.inner.lifecycle.wait_for_drain())
            .await
            .is_ok()
        {
            false
        } else {
            self.inner.lifecycle.mark_forced();
            self.inner.shutdown_cancel.cancel();
            tokio::time::timeout(grace, self.inner.lifecycle.wait_for_drain())
                .await
                .map_err(|_| {
                    EngineError::deadline_exceeded(
                        "engine shutdown timed out while cancelled operations were cleaning up",
                    )
                })?;
            true
        };

        let report = if forced_now || self.inner.lifecycle.was_forced() {
            ShutdownReport::forced_shutdown()
        } else {
            ShutdownReport::graceful()
        };
        let connections = self.inner.connections.clone();
        let lifecycle = Arc::clone(&self.inner.lifecycle);
        self.inner
            .workers
            .run(move || {
                // Once this finalizer starts, the owned gate remains in the
                // blocking closure even if its async caller is cancelled. A
                // later shutdown therefore cannot report Stopped before every
                // SQLite handle from this finalizer has actually closed.
                let _shutdown_guard = shutdown_guard;
                connections.close_idle()?;
                lifecycle.mark_stopped(report);
                Ok(())
            })
            .await?;
        Ok(report)
    }

    fn operation(&self, context: RequestContext) -> EngineResult<Operation> {
        let lease = self.inner.lifecycle.try_acquire()?;
        let cancellation = context.cancellation_token();
        let now = Instant::now();
        let engine_deadline = self
            .inner
            .options
            .request_timeout()
            .and_then(|timeout| now.checked_add(timeout));
        let deadline = match (context.deadline(), engine_deadline) {
            (Some(request), Some(engine)) => Some(request.min(engine)),
            (Some(request), None) => Some(request),
            (None, engine) => engine,
        };
        let configured = self.inner.options.result_limits();
        let requested = context.result_limits().unwrap_or(configured);
        let result_limits = ResultLimits::new(
            configured.max_rows().min(requested.max_rows()),
            configured.max_bytes().min(requested.max_bytes()),
        )
        .expect("the minimum of validated result limits is valid");
        let control = OperationControl::new(deadline);
        let cancel_on_drop = CancelOnDrop::new(Arc::clone(&control));
        let operation = Operation {
            lease: Some(lease),
            control,
            cancellation,
            shutdown_cancel: self.inner.shutdown_cancel.clone(),
            deadline,
            result_limits,
            cancel_on_drop,
        };
        operation.check_before_start()?;
        Ok(operation)
    }

    /// Return engine status after validating the calling session.
    pub async fn status(&self, session: &Session) -> EngineResult<EngineStatus> {
        self.status_with_context(session, RequestContext::new())
            .await
    }

    /// Return engine status with explicit cancellation and deadline controls.
    pub async fn status_with_context(
        &self,
        session: &Session,
        context: RequestContext,
    ) -> EngineResult<EngineStatus> {
        let mut operation = self.operation(context)?;
        let schema_operation = match self.inner.database.storage.enter_schema_operation() {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let result = async {
            let _schema_operation = schema_operation;
            let _guard = operation.wait_pending(self.ready_session(session)).await?;
            operation.check_before_start()?;
            let result_limits = self.inner.options.result_limits();
            Ok(EngineStatus {
                shard_count: self.shard_count(),
                max_blocking_workers: self.blocking_task_admission_limit(),
                connections_per_shard: self.inner.options.connections_per_shard(),
                queue_capacity_per_shard: self.inner.options.queue_capacity_per_shard(),
                max_result_rows: result_limits.max_rows(),
                max_result_bytes: result_limits.max_bytes(),
                prepared_statement_limits: self.inner.options.prepared_statement_limits(),
                request_timeout: self.inner.options.request_timeout(),
                shutdown_grace: self.inner.options.shutdown_grace(),
            })
        }
        .await;
        operation.finish(result)
    }

    /// Ask SQLite to passively checkpoint every shard without blocking writers.
    ///
    /// The operation participates in ordinary engine admission, cancellation,
    /// deadlines, schema coordination, pool limits, and graceful shutdown. A
    /// successful report can still be `busy` when active readers prevent every
    /// eligible WAL frame from being copied; callers may retry later.
    pub async fn checkpoint(&self) -> EngineResult<CheckpointReport> {
        self.checkpoint_with_context(RequestContext::new()).await
    }

    /// Passively checkpoint every shard with host-supplied request controls.
    pub async fn checkpoint_with_context(
        &self,
        context: RequestContext,
    ) -> EngineResult<CheckpointReport> {
        let mut operation = self.operation(context)?;
        let schema_operation = match self.inner.database.storage.enter_schema_operation() {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let permits = match operation
            .wait_pending(
                self.inner
                    .connections
                    .acquire_all_for_owner(ConnectionOwner::stateless_catalog_write()),
            )
            .await
        {
            Ok(permits) => permits,
            Err(error) => return operation.finish(Err(error)),
        };
        let worker = match operation.wait_pending(self.inner.workers.acquire()).await {
            Ok(worker) => worker,
            Err(error) => return operation.finish(Err(error)),
        };
        if let Err(error) = operation.check_before_start() {
            return operation.finish(Err(error));
        }

        let lease = operation.take_lease();
        let control = Arc::clone(&operation.control);
        let worker_control = Arc::clone(&control);
        let join = worker.spawn(move || {
            let _lease = lease;
            let _schema_operation = schema_operation;
            let result = permits
                .into_iter()
                .map(|(shard, permit)| {
                    permit
                        .checkout_controlled(Arc::clone(&worker_control))
                        .and_then(|mut connection| {
                            let result = connection.run_controlled(
                                Arc::clone(&worker_control),
                                |connection| {
                                    let (busy, wal_frames, checkpointed_frames) = connection
                                        .query_row(
                                            "PRAGMA main.wal_checkpoint(PASSIVE)",
                                            [],
                                            |row| {
                                                Ok((
                                                    row.get::<_, i64>(0)?,
                                                    row.get::<_, i64>(1)?,
                                                    row.get::<_, i64>(2)?,
                                                ))
                                            },
                                        )
                                        .map_err(crate::sqlite_error::storage)?;
                                    checkpoint_shard_report(
                                        shard,
                                        busy,
                                        wal_frames,
                                        checkpointed_frames,
                                    )
                                },
                            );
                            retire_if_broken(&mut connection, &result);
                            result
                        })
                })
                .collect::<EngineResult<Vec<_>>>()
                .map(|shards| CheckpointReport { shards });
            worker_control.complete(result)
        });
        let result = operation.wait_started(join).await;
        operation.finish_started(result)
    }

    /// Parse, validate, translate, and transiently compile one prepared statement.
    pub async fn prepare_statement(
        &self,
        session: &Session,
        request: PrepareRequest,
    ) -> EngineResult<PreparedStatementId> {
        self.prepare_statement_with_context(session, request, RequestContext::new())
            .await
    }

    /// Prepare one statement with explicit cancellation and deadline controls.
    pub async fn prepare_statement_with_context(
        &self,
        session: &Session,
        request: PrepareRequest,
        context: RequestContext,
    ) -> EngineResult<PreparedStatementId> {
        let mut operation = self.operation(context)?;
        let (schema_operation, mut guard) =
            match self.session_with_schema(&operation, session).await {
                Ok(admission) => admission,
                Err(error) => return operation.finish(Err(error)),
            };
        if self.catalog().database_by_id(request.database()).is_none() {
            guard.fail_transaction();
            return operation.finish(Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "selected logical database does not exist",
            )));
        }
        let (database, behavior, translated) = match prepare_translated_request(request) {
            Ok(prepared) => prepared,
            Err(error) => {
                guard.fail_transaction();
                return operation.finish(Err(error));
            }
        };
        if let Err(error) = reject_catalog_prepared_target(
            self.catalog(),
            database,
            translated.normalized_sql(),
            translated.statement_parameters()[0].parameter_count(),
        ) {
            guard.fail_transaction();
            return operation.finish(Err(error));
        }
        if let Err(error) = guard.prepared().ensure_statement_capacity() {
            guard.fail_transaction();
            return operation.finish(Err(error));
        }
        let parameter_count = translated.statement_parameters()[0].parameter_count();
        let sqlite_sql = translated.sqlite_sql().to_owned();
        let schema_generation = self.catalog().schema_generation();
        let owner = ConnectionOwner::new(session.id().get());

        if matches!(behavior, sql::StatementBehavior::Session(_)) {
            let result = operation.check_before_start().and_then(|()| {
                let description = PreparedStatementDescription::new(
                    behavior,
                    parameter_count,
                    Vec::new(),
                    schema_generation,
                );
                let mut guard = guard;
                guard
                    .prepared_mut()
                    .insert_statement(database, translated, description)
            });
            drop(schema_operation);
            return operation.finish(result);
        }

        if guard
            .transaction_mut()
            .is_some_and(|transaction| transaction.connection.is_some())
        {
            let result = self
                .run_transaction_prepare(
                    &mut operation,
                    schema_operation,
                    guard,
                    database,
                    translated,
                    behavior,
                    parameter_count,
                    sqlite_sql,
                    schema_generation,
                )
                .await;
            return operation.finish_started(result);
        }

        let result = self
            .run_on_shard(
                &mut operation,
                0,
                owner,
                schema_operation,
                guard,
                move |connection, session, control| {
                    let result = (|| {
                        connection
                            .isolate_foreign_sql_controlled(Arc::clone(&control), &sqlite_sql)?;
                        let metadata = connection.run_controlled(control, |connection| {
                            sql::describe_statement(connection, &sqlite_sql)
                        })?;
                        ensure_parameter_metadata(parameter_count, metadata.parameter_count())?;
                        let description = PreparedStatementDescription::new(
                            behavior,
                            parameter_count,
                            metadata.columns().to_vec(),
                            schema_generation,
                        );
                        session
                            .prepared_mut()
                            .insert_statement(database, translated, description)
                    })();
                    if result.is_err() {
                        session.fail_transaction();
                    }
                    result
                },
            )
            .await;
        operation.finish_started(result)
    }

    /// Bind typed values and the session's current routing context into a portal.
    pub async fn bind_statement(
        &self,
        session: &Session,
        statement: PreparedStatementId,
        parameters: Vec<Value>,
    ) -> EngineResult<PortalId> {
        self.bind_statement_with_context(session, statement, parameters, RequestContext::new())
            .await
    }

    /// Bind a prepared statement with explicit cancellation and deadline controls.
    pub async fn bind_statement_with_context(
        &self,
        session: &Session,
        statement: PreparedStatementId,
        parameters: Vec<Value>,
        context: RequestContext,
    ) -> EngineResult<PortalId> {
        let mut operation = self.operation(context)?;
        let (schema_operation, mut guard) =
            match self.session_with_schema(&operation, session).await {
                Ok(admission) => admission,
                Err(error) => return operation.finish(Err(error)),
            };
        let result = (|| {
            let routing_key = guard.routing_key().map(str::as_bytes);
            let template = guard.prepared().statement(statement)?;
            let parameter_layout = &template.translated().statement_parameters()[0];
            let expected_parameters = parameter_layout.parameter_count();
            if parameters.len() != expected_parameters {
                return Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    format!(
                        "prepared statement requires exactly {expected_parameters} bound parameters"
                    ),
                ));
            }
            sql::validate_parameters(&parameters)?;
            guard
                .prepared()
                .ensure_portal_capacity(&parameters, routing_key)?;
            guard.prepared().ensure_planning_capacity(
                &parameters,
                parameter_layout.parameter_indices(),
                routing_key,
            )?;
            self.plan_bound_statement_admitted(
                template.database(),
                template.translated().normalized_sql(),
                0,
                &parameters,
                routing_key,
            )?;
            operation.check_before_start()?;
            let routing_key = routing_key.map(<[u8]>::to_vec);
            guard
                .prepared_mut()
                .insert_portal(statement, parameters, routing_key)
        })();
        if result.is_err() {
            guard.fail_transaction();
        }
        drop(schema_operation);
        operation.finish(result)
    }

    /// Describe one prepared statement or bound portal.
    pub async fn describe_prepared(
        &self,
        session: &Session,
        target: DescribeTarget,
    ) -> EngineResult<PreparedStatementDescription> {
        self.describe_prepared_with_context(session, target, RequestContext::new())
            .await
    }

    /// Describe a prepared object with explicit cancellation and deadline controls.
    pub async fn describe_prepared_with_context(
        &self,
        session: &Session,
        target: DescribeTarget,
        context: RequestContext,
    ) -> EngineResult<PreparedStatementDescription> {
        let mut operation = self.operation(context)?;
        let (schema_operation, guard) = match self.session_with_schema(&operation, session).await {
            Ok(admission) => admission,
            Err(error) => return operation.finish(Err(error)),
        };
        let statement = match target {
            DescribeTarget::Statement(statement) => statement,
            DescribeTarget::Portal(portal) => match guard.prepared().portal(portal) {
                Ok(portal) => portal.statement(),
                Err(error) => return operation.finish(Err(error)),
            },
        };
        let template = match guard.prepared().statement(statement) {
            Ok(template) => template,
            Err(error) => return operation.finish(Err(error)),
        };
        let schema_generation = self.catalog().schema_generation();
        if template.description().schema_generation() == schema_generation {
            let result = operation
                .check_before_start()
                .map(|()| template.description().clone());
            drop(guard);
            drop(schema_operation);
            return operation.finish(result);
        }

        let parameter_count = template.translated().statement_parameters()[0].parameter_count();
        let behavior = template.description().behavior();
        let sqlite_sql = template.translated().sqlite_sql().to_owned();
        let owner = ConnectionOwner::new(session.id().get());
        let result = self
            .run_on_shard(
                &mut operation,
                0,
                owner,
                schema_operation,
                guard,
                move |connection, session, control| {
                    connection.isolate_foreign_sql_controlled(Arc::clone(&control), &sqlite_sql)?;
                    let metadata = connection.run_controlled(control, |connection| {
                        sql::describe_statement(connection, &sqlite_sql)
                    })?;
                    ensure_parameter_metadata(parameter_count, metadata.parameter_count())?;
                    let description = PreparedStatementDescription::new(
                        behavior,
                        parameter_count,
                        metadata.columns().to_vec(),
                        schema_generation,
                    );
                    session
                        .prepared_mut()
                        .statement_mut(statement)?
                        .replace_description(description.clone());
                    Ok(description)
                },
            )
            .await;
        operation.finish_started(result)
    }

    /// Execute one immutable bound portal on the shard selected by a fresh plan.
    pub async fn execute_portal(
        &self,
        session: &Session,
        portal: PortalId,
    ) -> EngineResult<Routed<PreparedExecution>> {
        self.execute_portal_with_context(session, portal, RequestContext::new())
            .await
    }

    /// Execute a bound portal with explicit request and result-budget controls.
    pub async fn execute_portal_with_context(
        &self,
        session: &Session,
        portal: PortalId,
        context: RequestContext,
    ) -> EngineResult<Routed<PreparedExecution>> {
        let mut operation = self.operation(context)?;
        let (schema_operation, mut guard) =
            match self.session_with_schema(&operation, session).await {
                Ok(admission) => admission,
                Err(error) => return operation.finish(Err(error)),
            };
        let portal_snapshot = match guard.prepared().portal(portal) {
            Ok(portal) => portal.clone(),
            Err(error) => {
                guard.fail_transaction();
                return operation.finish(Err(error));
            }
        };
        let template = match guard.prepared().statement(portal_snapshot.statement()) {
            Ok(template) => template,
            Err(error) => {
                guard.fail_transaction();
                return operation.finish(Err(error));
            }
        };
        let behavior = template.description().behavior();
        if let sql::StatementBehavior::Session(session_behavior) = behavior {
            let _ = session_behavior;
            return operation.finish(Err(EngineError::new(
                EngineErrorKind::Unsupported,
                "transaction control has no Routed physical shard; use logical portal execution",
            )));
        }
        if guard.state() == super::SessionState::FailedTransaction {
            return operation.finish(Err(transaction_aborted()));
        }
        let plan = match self.plan_bound_statement_admitted(
            template.database(),
            template.translated().normalized_sql(),
            0,
            portal_snapshot.parameters(),
            portal_snapshot.routing_key(),
        ) {
            Ok(plan) => plan,
            Err(error) => {
                guard.fail_transaction();
                return operation.finish(Err(error));
            }
        };
        let sqlite_sql = template.translated().sqlite_sql().to_owned();
        let owner = ConnectionOwner::new(session.id().get());
        if guard.state() == super::SessionState::InTransaction && plan.generated_insert().is_some()
        {
            let mut guard = guard;
            guard.fail_transaction();
            return operation.finish(Err(explicit_generated_write_unsupported()));
        }
        if let Some(generated) = plan.generated_insert() {
            #[cfg(feature = "experimental-vtab")]
            {
                if !self.inner.options.experimental_vtab_writes() {
                    return operation.finish(Err(EngineError::new(
                        EngineErrorKind::FailedPrecondition,
                        "experimental virtual-table writes are disabled",
                    )));
                }
                let generated_target =
                    match self.generated_write_target(generated.table_id(), generated.policy()) {
                        Ok(target) => target,
                        Err(error) => return operation.finish(Err(error)),
                    };
                let admission_shard = generated_target.admission_shard();
                let result = self
                    .run_coordinator_write(
                        &mut operation,
                        admission_shard,
                        owner,
                        schema_operation,
                        guard,
                        sqlite_sql,
                        portal_snapshot.parameters().to_vec(),
                        Some(generated_target),
                    )
                    .await
                    .map(|routed| Routed {
                        shard: routed.shard,
                        value: PreparedExecution::GeneratedWrite(routed.value),
                    });
                return operation.finish_started(result);
            }
            #[cfg(not(feature = "experimental-vtab"))]
            {
                let _ = generated;
                return operation.finish(Err(EngineError::new(
                    EngineErrorKind::Unsupported,
                    "generated-key INSERT requires the experimental-vtab feature",
                )));
            }
        }
        let shard = match prepared_execution_shard(&plan, self.catalog()) {
            Ok(shard) => shard,
            Err(error) => {
                guard.fail_transaction();
                return operation.finish(Err(error));
            }
        };
        let behavior = plan.behavior();
        let result_limits = operation.result_limits;
        if guard.state() == super::SessionState::InTransaction {
            let result = self
                .run_transaction_statement(
                    &mut operation,
                    shard,
                    owner,
                    schema_operation,
                    guard,
                    sqlite_sql,
                    portal_snapshot.parameters().to_vec(),
                    behavior,
                    result_limits,
                )
                .await;
            let value = operation.finish_started(result)?;
            return Ok(Routed { shard, value });
        }
        let result = self
            .run_on_shard(
                &mut operation,
                shard,
                owner,
                schema_operation,
                guard,
                move |connection, _session, control| {
                    connection.isolate_foreign_sql_controlled(Arc::clone(&control), &sqlite_sql)?;
                    connection.run_controlled(control, |connection| {
                        sql::execute_statement_with_limits(
                            connection,
                            &sqlite_sql,
                            portal_snapshot.parameters(),
                            result_limits,
                        )
                        .and_then(|execution| prepared_execution(behavior, execution))
                    })
                },
            )
            .await;
        let value = operation.finish_started(result)?;
        Ok(Routed { shard, value })
    }

    /// Execute one immutable bound portal as a logical read across every
    /// metadata-selected physical shard.
    pub async fn execute_portal_logical(
        &self,
        session: &Session,
        portal: PortalId,
    ) -> EngineResult<Executed<PreparedExecution>> {
        self.execute_portal_logical_with_context(session, portal, RequestContext::new())
            .await
    }

    /// Execute a logical bound portal with explicit request and result-budget
    /// controls.
    pub async fn execute_portal_logical_with_context(
        &self,
        session: &Session,
        portal: PortalId,
        context: RequestContext,
    ) -> EngineResult<Executed<PreparedExecution>> {
        let mut operation = self.operation(context)?;
        let (schema_operation, mut guard) =
            match self.session_with_schema(&operation, session).await {
                Ok(admission) => admission,
                Err(error) => return operation.finish(Err(error)),
            };
        let portal_snapshot = match guard.prepared().portal(portal) {
            Ok(portal) => portal.clone(),
            Err(error) => {
                guard.fail_transaction();
                return operation.finish(Err(error));
            }
        };
        let template = match guard.prepared().statement(portal_snapshot.statement()) {
            Ok(template) => template,
            Err(error) => {
                guard.fail_transaction();
                return operation.finish(Err(error));
            }
        };
        let behavior = template.description().behavior();
        if let sql::StatementBehavior::Session(session_behavior) = behavior {
            if !portal_snapshot.parameters().is_empty() {
                return operation.finish(Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    "transaction control statements do not accept parameters",
                )));
            }
            return self
                .execute_transaction_control(
                    &mut operation,
                    session,
                    schema_operation,
                    guard,
                    session_behavior,
                )
                .await
                .map(|value| Executed {
                    shards: Vec::new(),
                    value,
                });
        }
        if guard.state() == super::SessionState::FailedTransaction {
            return operation.finish(Err(transaction_aborted()));
        }
        let explicit_routing_key = if matches!(
            template.description().behavior(),
            sql::StatementBehavior::Read
        ) {
            None
        } else {
            portal_snapshot.routing_key()
        };
        let plan = match self.plan_bound_statement_admitted(
            template.database(),
            template.translated().normalized_sql(),
            0,
            portal_snapshot.parameters(),
            explicit_routing_key,
        ) {
            Ok(plan) => plan,
            Err(error) => {
                guard.fail_transaction();
                return operation.finish(Err(error));
            }
        };
        let sqlite_sql = template.translated().sqlite_sql().to_owned();
        let owner = ConnectionOwner::new(session.id().get());
        if guard.state() == super::SessionState::InTransaction && plan.generated_insert().is_some()
        {
            let mut guard = guard;
            guard.fail_transaction();
            return operation.finish(Err(explicit_generated_write_unsupported()));
        }
        if let Some(generated) = plan.generated_insert() {
            #[cfg(feature = "experimental-vtab")]
            {
                if !self.inner.options.experimental_vtab_writes() {
                    return operation.finish(Err(EngineError::new(
                        EngineErrorKind::FailedPrecondition,
                        "experimental virtual-table writes are disabled",
                    )));
                }
                let generated_target =
                    match self.generated_write_target(generated.table_id(), generated.policy()) {
                        Ok(target) => target,
                        Err(error) => return operation.finish(Err(error)),
                    };
                let admission_shard = generated_target.admission_shard();
                let result = self
                    .run_coordinator_write(
                        &mut operation,
                        admission_shard,
                        owner,
                        schema_operation,
                        guard,
                        sqlite_sql,
                        portal_snapshot.parameters().to_vec(),
                        Some(generated_target),
                    )
                    .await
                    .map(|routed| Executed {
                        shards: vec![routed.shard],
                        value: PreparedExecution::GeneratedWrite(routed.value),
                    });
                return operation.finish_started(result);
            }
            #[cfg(not(feature = "experimental-vtab"))]
            {
                let _ = generated;
                return operation.finish(Err(EngineError::new(
                    EngineErrorKind::Unsupported,
                    "generated-key INSERT requires the experimental-vtab feature",
                )));
            }
        }
        let shards = match prepared_execution_shards(&plan, self.catalog(), self.shard_count()) {
            Ok(shards) => shards,
            Err(error) => {
                guard.fail_transaction();
                return operation.finish(Err(error));
            }
        };
        let behavior = plan.behavior();
        let result_limits = operation.result_limits;
        if guard.state() == super::SessionState::InTransaction {
            if shards.len() != 1 {
                let mut guard = guard;
                guard.fail_transaction();
                return operation.finish(Err(cross_shard_transaction()));
            }
            let shard = shards[0];
            if guard
                .transaction_shard()
                .is_some_and(|pinned| pinned != shard)
            {
                let mut guard = guard;
                guard.fail_transaction();
                return operation.finish(Err(cross_shard_transaction()));
            }
            let result = self
                .run_transaction_statement(
                    &mut operation,
                    shard,
                    owner,
                    schema_operation,
                    guard,
                    sqlite_sql,
                    portal_snapshot.parameters().to_vec(),
                    behavior,
                    result_limits,
                )
                .await;
            let value = operation.finish_started(result)?;
            return Ok(Executed {
                shards: vec![shard],
                value,
            });
        }
        if shards.len() > 1 {
            if let Err(error) = sql::validate_scatter_safe(template.translated()) {
                return operation.finish(Err(error));
            }
        }
        if shards.len() > 1 {
            if !matches!(behavior, sql::StatementBehavior::Read) {
                return operation.finish(Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "write planning produced more than one execution shard",
                )));
            }
            let parameters = portal_snapshot.parameters().to_vec();
            let result = self
                .run_scatter_query(
                    &mut operation,
                    owner,
                    schema_operation,
                    guard,
                    shards.clone(),
                    sqlite_sql,
                    parameters,
                )
                .await
                .map(PreparedExecution::Rows);
            let value = operation.finish_started(result)?;
            return Ok(Executed { shards, value });
        }

        let shard = shards[0];
        let result = self
            .run_on_shard(
                &mut operation,
                shard,
                owner,
                schema_operation,
                guard,
                move |connection, _session, control| {
                    connection.isolate_foreign_sql_controlled(Arc::clone(&control), &sqlite_sql)?;
                    connection.run_controlled(control, |connection| {
                        sql::execute_statement_with_limits(
                            connection,
                            &sqlite_sql,
                            portal_snapshot.parameters(),
                            result_limits,
                        )
                        .and_then(|execution| prepared_execution(behavior, execution))
                    })
                },
            )
            .await;
        let value = operation.finish_started(result)?;
        Ok(Executed { shards, value })
    }

    /// Close a prepared statement and every portal bound from it.
    ///
    /// This in-memory cleanup remains available while the engine is draining.
    pub async fn close_prepared_statement(
        &self,
        session: &Session,
        statement: PreparedStatementId,
    ) -> EngineResult<bool> {
        let mut guard = self.ready_session(session).await?;
        guard.prepared_mut().close_statement(statement)
    }

    /// Close one bound portal without closing its prepared statement.
    ///
    /// This in-memory cleanup remains available while the engine is draining.
    pub async fn close_portal(&self, session: &Session, portal: PortalId) -> EngineResult<bool> {
        let mut guard = self.ready_session(session).await?;
        guard.prepared_mut().close_portal(portal)
    }

    /// Execute a routed statement and return its selected shard.
    ///
    /// This compatibility method returns only the affected-row count. Use
    /// [`Engine::execute_write`] when generated-key result data is needed.
    pub async fn execute(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Routed<usize>> {
        self.execute_write(session, statement)
            .await
            .map(write_rows_affected)
    }

    /// Execute a routed statement with its complete write result.
    ///
    /// A supported single-row INSERT that omits a catalog-declared generated
    /// key returns that key captured by the same committing operation.
    pub async fn execute_write(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Routed<super::WriteResult>> {
        self.execute_write_with_context(session, statement, RequestContext::new())
            .await
    }

    /// Execute one ephemeral HTTP write while allowing safe physical-handle
    /// reuse after authoritative catalog validation.
    ///
    /// An empty catalog retains ordinary unique-session ownership. With a
    /// populated catalog, raw planning proves that the request is one routed
    /// common-subset write before the shared stateless ownership domain is
    /// selected.
    #[cfg(feature = "http")]
    pub(crate) async fn execute_http_request(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Routed<super::WriteResult>> {
        self.execute_with_context_and_owner(
            session,
            statement,
            RequestContext::new(),
            ExecuteOwnerPolicy::ReuseValidatedCatalogWrite,
        )
        .await
    }

    /// Execute a routed statement with explicit request controls.
    ///
    /// This compatibility method returns only the affected-row count. Use
    /// [`Engine::execute_write_with_context`] to retain the complete write
    /// result shape.
    pub async fn execute_with_context(
        &self,
        session: &Session,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Routed<usize>> {
        self.execute_write_with_context(session, statement, context)
            .await
            .map(write_rows_affected)
    }

    /// Execute a routed statement with explicit request controls and the result
    /// shape used for same-operation generated-key capture.
    pub async fn execute_write_with_context(
        &self,
        session: &Session,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Routed<super::WriteResult>> {
        self.execute_with_context_and_owner(
            session,
            statement,
            context,
            ExecuteOwnerPolicy::Session,
        )
        .await
    }

    /// Execute one preflighted generated-ID INSERT on a caller-admitted shard.
    ///
    /// This crate-private exact-target seam remains for focused coordinator
    /// tests. Public omitted-key SQL uses the shared planner and allocator
    /// target selection in [`Engine::execute_write`].
    #[cfg(feature = "experimental-vtab")]
    #[allow(dead_code)]
    pub(crate) async fn execute_generated_write(
        &self,
        session: &Session,
        statement: Statement,
        table: super::TableId,
        target_shard: u16,
    ) -> EngineResult<Routed<super::WriteResult>> {
        self.execute_generated_write_with_context(
            session,
            statement,
            table,
            target_shard,
            RequestContext::new(),
        )
        .await
    }

    /// Execute a preflighted native-ID INSERT with request controls.
    #[cfg(feature = "experimental-vtab")]
    #[allow(dead_code)]
    pub(crate) async fn execute_generated_write_with_context(
        &self,
        session: &Session,
        statement: Statement,
        table: super::TableId,
        target_shard: u16,
        context: RequestContext,
    ) -> EngineResult<Routed<super::WriteResult>> {
        let mut operation = self.operation(context)?;
        let schema_operation = match self.inner.database.storage.enter_schema_operation() {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let guard = match operation.wait_pending(self.ready_session(session)).await {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        if !self.inner.options.experimental_vtab_writes() {
            return operation.finish(Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "experimental virtual-table writes are disabled",
            )));
        }
        if target_shard >= self.shard_count() {
            return operation.finish(Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!(
                    "native generated INSERT target shard {target_shard} is outside shard count {}",
                    self.shard_count()
                ),
            )));
        }
        let Some(table_metadata) = self.catalog().table_by_id(table) else {
            return operation.finish(Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!("native generated INSERT refers to unknown table identity {table}"),
            )));
        };
        if !matches!(
            table_metadata.generated_id_policy(),
            super::GeneratedIdPolicy::NativeRangeV1 { .. }
        ) {
            return operation.finish(Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "registered table {} does not use native_range_v1 generation",
                    table_metadata.name()
                ),
            )));
        }
        if !self
            .inner
            .database
            .storage
            .native_id_policy_is_active(table)
        {
            return operation.finish(Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "native_range_v1 generation is not active for registered table {}",
                    table_metadata.name()
                ),
            )));
        }
        let Some(allocation_owners) = self.inner.database.storage.allocation_owner_map() else {
            return operation.finish(Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "native generated INSERT has no allocation-owner map",
            )));
        };
        if allocation_owners
            .owner_for_physical_shard(target_shard)
            .is_none()
        {
            return operation.finish(Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "native generated INSERT target shard {target_shard} has no active allocation owner"
                ),
            )));
        }

        let (sql, params) = statement.into_parts();
        let value = self
            .run_coordinator_write(
                &mut operation,
                target_shard,
                ConnectionOwner::new(session.id().get()),
                schema_operation,
                guard,
                sql,
                params,
                Some(GeneratedWriteTarget::Exact(table, target_shard)),
            )
            .await;
        operation.finish_started(value)
    }

    async fn execute_with_context_and_owner(
        &self,
        session: &Session,
        statement: Statement,
        context: RequestContext,
        owner_policy: ExecuteOwnerPolicy,
    ) -> EngineResult<Routed<super::WriteResult>> {
        let mut operation = self.operation(context)?;
        let schema_operation = match self.inner.database.storage.enter_schema_operation() {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let guard = match operation.wait_pending(self.ready_session(session)).await {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let routing_key = guard.routing_key().map(str::to_owned);
        let (sql, params) = statement.into_parts();
        let plan = match self.inner.database.raw_data_plan(
            routing_key.as_deref(),
            &sql,
            &params,
            RawDataOperation::Execute,
        ) {
            Ok(plan) => plan,
            Err(error) => return operation.finish(Err(error)),
        };
        let catalog_authoritative = plan.is_some();
        let owner = match (owner_policy, plan.is_some()) {
            (ExecuteOwnerPolicy::ReuseValidatedCatalogWrite, true) => {
                ConnectionOwner::stateless_catalog_write()
            }
            (ExecuteOwnerPolicy::Session | ExecuteOwnerPolicy::ReuseValidatedCatalogWrite, _) => {
                ConnectionOwner::new(session.id().get())
            }
        };
        let (target, sql) = match plan {
            Some(plan) => (plan.target, plan.sqlite_sql),
            None => {
                let Some(routing_key) = routing_key.as_deref() else {
                    return operation.finish(Err(EngineError::new(
                        EngineErrorKind::InvalidArgument,
                        "the session has no routing key",
                    )));
                };
                (
                    RawDataTarget::Exact(self.inner.database.shard_for_key(routing_key.as_bytes())),
                    sql,
                )
            }
        };

        #[cfg(feature = "experimental-vtab")]
        if let RawDataTarget::Generated(table) = target {
            if !self.inner.options.experimental_vtab_writes() {
                return operation.finish(Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "experimental virtual-table writes are disabled",
                )));
            }
            let generated_target = match self.generated_write_target_for_table(table) {
                Ok(target) => target,
                Err(error) => return operation.finish(Err(error)),
            };
            let admission_shard = generated_target.admission_shard();
            let value = self
                .run_coordinator_write(
                    &mut operation,
                    admission_shard,
                    owner,
                    schema_operation,
                    guard,
                    sql,
                    params,
                    Some(generated_target),
                )
                .await;
            return operation.finish_started(value);
        }

        #[cfg(not(feature = "experimental-vtab"))]
        if matches!(target, RawDataTarget::Generated(_)) {
            return operation.finish(Err(EngineError::new(
                EngineErrorKind::Unsupported,
                "generated-key INSERT requires the experimental-vtab feature",
            )));
        }

        let RawDataTarget::Exact(shard) = target else {
            unreachable!("generated writes returned above")
        };

        #[cfg(feature = "experimental-vtab")]
        if catalog_authoritative && self.inner.options.experimental_vtab_writes() {
            let value = self
                .run_coordinator_write(
                    &mut operation,
                    shard,
                    owner,
                    schema_operation,
                    guard,
                    sql,
                    params,
                    None,
                )
                .await;
            return operation.finish_started(value);
        }

        #[cfg(not(feature = "experimental-vtab"))]
        let _ = catalog_authoritative;

        let value = self
            .run_on_shard(
                &mut operation,
                shard,
                owner,
                schema_operation,
                guard,
                move |connection, _session, control| {
                    connection.isolate_foreign_sql_controlled(Arc::clone(&control), &sql)?;
                    connection.run_controlled(control, |connection| {
                        sql::execute(connection, &sql, &params)
                    })
                },
            )
            .await;
        let value = operation.finish_started(value)?;
        let value = super::WriteResult::without_generated_key(value);
        Ok(Routed { shard, value })
    }

    /// Execute one catalog-authoritative autocommit write through the
    /// experimental logical virtual-table facade.
    ///
    /// The pool permit is deliberately retained only as a capacity token. The
    /// coordinator opens and validates its own child SQLite handle, so checking
    /// out a pooled handle here would double-own the same per-shard capacity.
    #[cfg(feature = "experimental-vtab")]
    #[allow(clippy::too_many_arguments)]
    async fn run_coordinator_write(
        &self,
        operation: &mut Operation,
        shard: u16,
        owner: ConnectionOwner,
        schema_operation: SchemaOperationGuard,
        session: OwnedMutexGuard<SessionInner>,
        sql: String,
        params: Vec<Value>,
        generated_target: Option<GeneratedWriteTarget>,
    ) -> EngineResult<Routed<super::WriteResult>> {
        // A cold registry cache briefly owns an unpooled read-only shard-0
        // handle; the physical DML child later owns the target-shard handle.
        // Serialize that one-time discovery, reserve every real handle through
        // the ordinary pool limits, and let warm coordinators use only their
        // target shard. When shard 0 is also the cold target, one permit covers
        // the sequential handles because discovery closes before DML starts.
        let schema_generation = self.inner.database.storage.current_schema_generation();
        let registry_bootstrap_gate = if self
            .inner
            .registry_schema_cache
            .requires_bootstrap(schema_generation)
        {
            let gate = Arc::clone(&self.inner.registry_bootstrap_gate);
            let acquired = match operation
                .wait_pending(async move { Ok(gate.lock_owned().await) })
                .await
            {
                Ok(acquired) => acquired,
                Err(error) => return operation.control.complete(Err(error)),
            };
            if self
                .inner
                .registry_schema_cache
                .requires_bootstrap(schema_generation)
            {
                Some(acquired)
            } else {
                None
            }
        } else {
            None
        };
        let bootstrap_required = registry_bootstrap_gate.is_some();
        // A hi/lo allocation irrevocably determines its physical shard only
        // inside the coordinator, so reserve every possible child slot for
        // that uncommon path. Native range instead acquires only the current
        // round-robin candidate and can release it before trying a
        // non-exhausted fallback owner.
        let native_auto = matches!(generated_target, Some(GeneratedWriteTarget::NativeAuto(_)));
        let hilo_auto = matches!(generated_target, Some(GeneratedWriteTarget::HiloAuto(_)));
        let auto_generated_shard = native_auto || hilo_auto;
        let auto_capacities = if hilo_auto {
            let mut capacities = Vec::with_capacity(usize::from(self.shard_count()));
            for candidate in 0..self.shard_count() {
                match operation
                    .wait_pending(self.inner.connections.acquire_for_owner(candidate, owner))
                    .await
                {
                    Ok(capacity) => capacities.push(capacity),
                    Err(error) => return operation.control.complete(Err(error)),
                }
            }
            Some(capacities)
        } else {
            None
        };
        let shard_zero_capacity = if bootstrap_required && !hilo_auto {
            match operation
                .wait_pending(self.inner.connections.acquire_for_owner(0, owner))
                .await
            {
                Ok(capacity) => Some(capacity),
                Err(error) => return operation.control.complete(Err(error)),
            }
        } else {
            None
        };
        let target_capacity = if auto_generated_shard || (shard == 0 && bootstrap_required) {
            None
        } else {
            match operation
                .wait_pending(self.inner.connections.acquire_for_owner(shard, owner))
                .await
            {
                Ok(capacity) => Some(capacity),
                Err(error) => return operation.control.complete(Err(error)),
            }
        };
        let worker = match operation.wait_pending(self.inner.workers.acquire()).await {
            Ok(worker) => worker,
            Err(error) => return operation.control.complete(Err(error)),
        };
        if let Err(error) = operation.check_before_start() {
            return operation.control.complete(Err(error));
        }

        let lease = operation.take_lease();
        let worker_control = Arc::clone(&operation.control);
        let storage = self.inner.database.storage.clone();
        let storage_for_corruption = storage.clone();
        let registry_schema_cache = Arc::clone(&self.inner.registry_schema_cache);
        let connections = self.inner.connections.clone();
        let join = worker.spawn(move || {
            let _lease = lease;
            let _session = session;
            let mut shard_zero_capacity = shard_zero_capacity;
            let _target_capacity = target_capacity;
            let _auto_capacities = auto_capacities;
            let mut registry_bootstrap_gate = registry_bootstrap_gate;
            let result = (|| {
                let mut coordinator = WriteCoordinator::open_admitted_controlled(
                    storage,
                    schema_operation,
                    Arc::clone(&worker_control),
                    registry_schema_cache,
                )?;

                // A nonzero write no longer owns a shard-0 handle after registry
                // construction. Keep the same permit for shard-0 target writes.
                if auto_generated_shard || shard != 0 {
                    drop(shard_zero_capacity.take());
                }
                drop(registry_bootstrap_gate.take());
                let cancellation = coordinator.cancellation_handle();
                worker_control
                    .arm(Arc::new(move || cancellation.cancel_write_nonblocking()))
                    .map_err(CancellationReason::error)?;
                let mut native_selected_shard = None;
                let result = match generated_target {
                    Some(GeneratedWriteTarget::Exact(table, expected_shard)) => coordinator
                        .execute_generated_dml_values(
                            &sql,
                            &params,
                            table.get(),
                            expected_shard,
                        )?,
                    Some(GeneratedWriteTarget::NativeAuto(table)) => {
                        let selected = Arc::new(AtomicU64::new(u64::MAX));
                        native_selected_shard = Some(Arc::clone(&selected));
                        let retained_capacity = std::sync::Mutex::new(Vec::new());
                        let admission_connections = connections.clone();
                        coordinator.execute_generated_dml_values_auto_admitted(
                            &sql,
                            &params,
                            table.get(),
                            move |candidate| {
                                {
                                    let mut retained = retained_capacity
                                        .lock()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                                    if retained
                                        .first()
                                        .is_some_and(|(shard, _)| *shard == candidate)
                                    {
                                        return Ok(());
                                    }
                                    retained.clear();
                                }
                                let capacity = admission_connections
                                    .try_acquire_for_owner(candidate, owner)?;
                                selected.store(u64::from(candidate), Ordering::Release);
                                retained_capacity
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .push((candidate, capacity));
                                Ok(())
                            },
                        )?
                    }
                    Some(GeneratedWriteTarget::HiloAuto(table)) => coordinator
                        .execute_generated_dml_values_auto(&sql, &params, table.get())?,
                    None => coordinator.execute_dml_values(&sql, &params)?,
                };
                if !auto_generated_shard
                    && result.shard().is_some_and(|actual| actual != shard)
                {
                    return Err(EngineError::new(
                        EngineErrorKind::Internal,
                        format!(
                            "writable coordinator mutated shard {}, but Engine admitted shard {shard}",
                            result.shard().expect("checked as present")
                        ),
                    ));
                }
                let actual_shard = match result.shard() {
                    Some(actual) => actual,
                    // A successful no-op has no mutated child to report. For
                    // ordinary and native-range writes the Engine already
                    // admitted one exact target, so preserve that established
                    // routed result. Only auto-routed hi/lo needs the child to
                    // report the target selected after allocation.
                    None if !auto_generated_shard => shard,
                    None if native_auto => {
                        let selected = native_selected_shard
                            .as_ref()
                            .expect("native auto write records its candidate")
                            .load(Ordering::Acquire);
                        u16::try_from(selected).map_err(|_| {
                            EngineError::new(
                                EngineErrorKind::Internal,
                                "native auto-routed write completed before selecting a physical shard",
                            )
                        })?
                    }
                    None => {
                        return Err(EngineError::new(
                            EngineErrorKind::Internal,
                            "auto-routed generated write completed without a physical shard",
                        ));
                    }
                };
                Ok(Routed {
                    shard: actual_shard,
                    value: super::WriteResult {
                        rows_affected: result.affected_rows(),
                        generated_key: result.generated_key().cloned(),
                    },
                })
            })();
            drop(registry_bootstrap_gate);
            if result
                .as_ref()
                .is_err_and(|error| error.kind() == EngineErrorKind::DataCorruption)
            {
                storage_for_corruption.record_schema_degraded();
            }
            worker_control.complete(result)
        });
        operation.wait_started(join).await
    }

    /// Query a routed statement and return its selected shard and rows.
    pub async fn query(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Routed<ResultSet>> {
        self.query_with_context(session, statement, RequestContext::new())
            .await
    }

    /// Query a routed statement with explicit cancellation, deadline, and
    /// result-budget controls.
    pub async fn query_with_context(
        &self,
        session: &Session,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Routed<ResultSet>> {
        let mut operation = self.operation(context)?;
        let schema_operation = match self.inner.database.storage.enter_schema_operation() {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let guard = match operation.wait_pending(self.ready_session(session)).await {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let owner = ConnectionOwner::new(session.id().get());
        let routing_key = match required_routing_key(&guard) {
            Ok(key) => key.to_owned(),
            Err(error) => return operation.finish(Err(error)),
        };
        let (sql, params) = statement.into_parts();
        let plan = match self.inner.database.raw_data_plan(
            Some(&routing_key),
            &sql,
            &params,
            RawDataOperation::Query,
        ) {
            Ok(plan) => plan,
            Err(error) => return operation.finish(Err(error)),
        };
        let (shard, sql) = match plan {
            Some(plan) => match plan.target {
                RawDataTarget::Exact(shard) => (shard, plan.sqlite_sql),
                RawDataTarget::Generated(_) => {
                    return operation.finish(Err(EngineError::new(
                        EngineErrorKind::Internal,
                        "query planning unexpectedly produced a generated write",
                    )));
                }
            },
            None => (
                self.inner.database.shard_for_key(routing_key.as_bytes()),
                sql,
            ),
        };
        let limits = operation.result_limits;
        let value = self
            .run_on_shard(
                &mut operation,
                shard,
                owner,
                schema_operation,
                guard,
                move |connection, _session, control| {
                    connection.isolate_foreign_sql_controlled(Arc::clone(&control), &sql)?;
                    connection.run_controlled(control, |connection| {
                        sql::query_with_limits(connection, &sql, &params, limits)
                    })
                },
            )
            .await;
        let value = operation.finish_started(value)?;
        Ok(Routed { shard, value })
    }

    /// Query one logical table view, visiting every physical shard selected by
    /// catalog metadata and preserving `UNION ALL` row semantics.
    pub async fn query_logical(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Executed<ResultSet>> {
        self.query_logical_with_context(session, statement, RequestContext::new())
            .await
    }

    /// Query one logical table view with explicit cancellation, deadline, and
    /// result-budget controls.
    pub async fn query_logical_with_context(
        &self,
        session: &Session,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Executed<ResultSet>> {
        // An empty catalog has no placement metadata, so retain the legacy
        // explicitly routed compatibility behavior.
        if self.catalog().tables().is_empty() {
            return self
                .query_with_context(session, statement, context)
                .await
                .map(|routed| Executed {
                    shards: vec![routed.shard],
                    value: routed.value,
                });
        }

        let mut operation = self.operation(context)?;
        let schema_operation = match self.inner.database.storage.enter_schema_operation() {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let guard = match operation.wait_pending(self.ready_session(session)).await {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let (sql, params) = statement.into_parts();
        let (shards, sql) = match self.logical_raw_query_plan(&sql, &params) {
            Ok(plan) => plan,
            Err(error) => return operation.finish(Err(error)),
        };
        let owner = ConnectionOwner::new(session.id().get());
        if shards.len() > 1 {
            let result = self
                .run_scatter_query(
                    &mut operation,
                    owner,
                    schema_operation,
                    guard,
                    shards.clone(),
                    sql,
                    params,
                )
                .await;
            let value = operation.finish_started(result)?;
            return Ok(Executed { shards, value });
        }

        let shard = shards[0];
        let limits = operation.result_limits;
        let value = self
            .run_on_shard(
                &mut operation,
                shard,
                owner,
                schema_operation,
                guard,
                move |connection, _session, control| {
                    connection.isolate_foreign_sql_controlled(Arc::clone(&control), &sql)?;
                    connection.run_controlled(control, |connection| {
                        sql::query_with_limits(connection, &sql, &params, limits)
                    })
                },
            )
            .await;
        let value = operation.finish_started(value)?;
        Ok(Executed { shards, value })
    }

    /// Run one read-only inspection statement on an explicit physical shard.
    ///
    /// This crate-private boundary exists for administrative surfaces that
    /// need to describe one shard honestly rather than manufacturing a routing
    /// key. It retains the ordinary engine lifecycle, schema gate, session,
    /// pool, worker, cancellation, and result-budget behavior. The SQL adapter
    /// verifies that the prepared SQLite statement is read-only before it is
    /// stepped.
    #[cfg(any(feature = "http", test))]
    pub(crate) async fn inspect_shard(
        &self,
        session: &Session,
        shard: u16,
        statement: Statement,
    ) -> EngineResult<ResultSet> {
        self.inspect_shard_with_context(session, shard, statement, RequestContext::new())
            .await
    }

    /// Run an explicit-shard inspection with request-scoped controls.
    #[cfg(any(feature = "http", test))]
    pub(crate) async fn inspect_shard_with_context(
        &self,
        session: &Session,
        shard: u16,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<ResultSet> {
        let mut operation = self.operation(context)?;
        let schema_operation = match self.inner.database.storage.enter_schema_operation() {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let guard = match operation.wait_pending(self.ready_session(session)).await {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        if shard >= self.shard_count() {
            return operation.finish(Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!(
                    "physical shard {shard} is outside the configured range 0..{}",
                    self.shard_count()
                ),
            )));
        }
        let owner = ConnectionOwner::new(session.id().get());
        let (sql, params) = statement.into_parts();
        let limits = operation.result_limits;

        let result = self
            .run_on_shard(
                &mut operation,
                shard,
                owner,
                schema_operation,
                guard,
                move |connection, _session, control| {
                    connection.isolate_foreign_sql_controlled(Arc::clone(&control), &sql)?;
                    connection.run_controlled(control, |connection| {
                        sql::query_with_limits(connection, &sql, &params, limits)
                    })
                },
            )
            .await;
        operation.finish_started(result)
    }

    /// Apply a parameterless SQL migration batch through the durable shard journal.
    pub async fn broadcast(&self, session: &Session, sql: String) -> EngineResult<Vec<u16>> {
        self.broadcast_with_context(session, sql, RequestContext::new())
            .await
    }

    /// Execute a parameterless SQL batch on every shard with explicit request controls.
    ///
    /// Shards are processed in order under a durable journal. Cancellation may
    /// leave resumable progress, which the same SQL or the next startup resumes.
    pub async fn broadcast_with_context(
        &self,
        session: &Session,
        sql: String,
        context: RequestContext,
    ) -> EngineResult<Vec<u16>> {
        let mut operation = self.operation(context)?;
        if session.owner != self.inner.id {
            return operation.finish(Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "the session belongs to a different engine",
            )));
        }
        let mut migration = match self.inner.database.storage.begin_schema_migration() {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let quiesced = operation
            .wait_pending(async {
                migration.wait_for_quiescence().await;
                Ok(())
            })
            .await;
        if let Err(error) = quiesced {
            return operation.finish(Err(error));
        }
        let session_guard = match operation.wait_pending(self.ready_session(session)).await {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let worker = match operation.wait_pending(self.inner.workers.acquire()).await {
            Ok(worker) => worker,
            Err(error) => return operation.finish(Err(error)),
        };
        if let Err(error) = operation.check_before_start() {
            return operation.finish(Err(error));
        }
        let lease = operation.take_lease();
        let worker_control = Arc::clone(&operation.control);
        let storage = self.inner.database.storage.clone();
        let connections = self.inner.connections.clone();
        let join = worker.spawn(move || {
            let _lease = lease;
            let _session = session_guard;
            let result = connections
                .retire_idle_for_schema_migration()
                .and_then(|_| {
                    storage.apply_schema_migration(
                        &sql,
                        &mut migration,
                        Some(Arc::clone(&worker_control)),
                    )
                })
                .and_then(|completed| {
                    migration.publish_ready()?;
                    Ok(completed)
                });
            worker_control.complete(result)
        });
        let result = operation.wait_started(join).await;
        operation.finish_started(result)
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_scatter_query(
        &self,
        operation: &mut Operation,
        owner: ConnectionOwner,
        schema_operation: SchemaOperationGuard,
        session: OwnedMutexGuard<SessionInner>,
        shards: Vec<u16>,
        sql: String,
        params: Vec<Value>,
    ) -> EngineResult<ResultSet> {
        if shards.len() < 2 {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "scatter execution requires at least two physical shards",
            ));
        }
        if let Err(error) = operation.check_before_start() {
            return operation.control.complete(Err(error));
        }

        let cancellation = CancellationToken::new();
        let cancel_scatter = cancellation.clone();
        if let Err(reason) = operation.control.arm(Arc::new(move || {
            cancel_scatter.cancel();
        })) {
            return operation.control.complete(Err(reason.error()));
        }

        let lease = operation.take_lease();
        let worker_control = Arc::clone(&operation.control);
        let engine = self.clone();
        let deadline = operation.deadline;
        let result_limits = operation.result_limits;
        let sql: Arc<str> = Arc::from(sql);
        let params: Arc<[Value]> = Arc::from(params);
        let join = tokio::spawn(async move {
            // These guards cover the complete logical operation. Cancellation
            // cannot release schema, lifecycle, or session ownership until
            // every started physical read has drained and cleaned up.
            let _lease = lease;
            let _schema_operation = schema_operation;
            let _session = session;
            let result = engine
                .coordinate_scatter_query(
                    owner,
                    shards,
                    sql,
                    params,
                    cancellation,
                    deadline,
                    result_limits,
                )
                .await;
            worker_control.complete(result)
        });
        operation.wait_started(join).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn coordinate_scatter_query(
        &self,
        owner: ConnectionOwner,
        shards: Vec<u16>,
        sql: Arc<str>,
        params: Arc<[Value]>,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        result_limits: ResultLimits,
    ) -> EngineResult<ResultSet> {
        let budget = sql::ScatterResultBudget::new(result_limits);
        let mut remaining = shards.iter().copied();
        let mut running = JoinSet::new();
        for shard in remaining.by_ref().take(MAX_SCATTER_CONCURRENCY) {
            self.spawn_scatter_shard(
                &mut running,
                shard,
                owner,
                Arc::clone(&sql),
                Arc::clone(&params),
                cancellation.clone(),
                deadline,
                budget.clone(),
            );
        }

        let mut results = Vec::with_capacity(shards.len());
        let mut first_error = None;
        while let Some(joined) = running.join_next().await {
            match joined {
                Ok((shard, Ok(result))) if first_error.is_none() => {
                    results.push(Routed {
                        shard,
                        value: result,
                    });
                }
                Ok((_, Ok(_))) => {}
                Ok((_, Err(error))) if first_error.is_none() => {
                    first_error = Some(error);
                    cancellation.cancel();
                }
                Ok((_, Err(_))) => {}
                Err(error) if first_error.is_none() => {
                    first_error = Some(EngineError::from_source(
                        EngineErrorKind::Internal,
                        "scatter query task failed",
                        error,
                    ));
                    cancellation.cancel();
                }
                Err(_) => {}
            }

            if first_error.is_none() {
                if let Some(shard) = remaining.next() {
                    self.spawn_scatter_shard(
                        &mut running,
                        shard,
                        owner,
                        Arc::clone(&sql),
                        Arc::clone(&params),
                        cancellation.clone(),
                        deadline,
                        budget.clone(),
                    );
                }
            }
        }

        if let Some(error) = first_error {
            return Err(error);
        }
        merge_scatter_results(results, result_limits)
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_scatter_shard(
        &self,
        running: &mut JoinSet<(u16, EngineResult<ResultSet>)>,
        shard: u16,
        owner: ConnectionOwner,
        sql: Arc<str>,
        params: Arc<[Value]>,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        budget: sql::ScatterResultBudget,
    ) {
        let engine = self.clone();
        running.spawn(async move {
            let result = engine
                .run_scatter_shard(shard, owner, sql, params, cancellation, deadline, budget)
                .await;
            (shard, result)
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_scatter_shard(
        &self,
        shard: u16,
        owner: ConnectionOwner,
        sql: Arc<str>,
        params: Arc<[Value]>,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        budget: sql::ScatterResultBudget,
    ) -> EngineResult<ResultSet> {
        let control = OperationControl::new(deadline);
        let mut cancel_on_drop = CancelOnDrop::new(Arc::clone(&control));
        let shutdown_cancel = self.inner.shutdown_cancel.clone();

        let permit = match wait_pending(
            self.inner.connections.acquire_for_owner(shard, owner),
            &cancellation,
            &shutdown_cancel,
            deadline,
            &control,
        )
        .await
        {
            Ok(permit) => permit,
            Err(error) => {
                let result = control.complete(Err(error));
                cancel_on_drop.disarm();
                return result;
            }
        };
        let worker = match wait_pending(
            self.inner.workers.acquire(),
            &cancellation,
            &shutdown_cancel,
            deadline,
            &control,
        )
        .await
        {
            Ok(worker) => worker,
            Err(error) => {
                let result = control.complete(Err(error));
                cancel_on_drop.disarm();
                return result;
            }
        };

        if let Some(reason) = pending_cancellation_reason(&cancellation, &shutdown_cancel, deadline)
        {
            control.request_cancel(reason);
            let result = control.complete(Err(reason.error()));
            cancel_on_drop.disarm();
            return result;
        }

        let worker_control = Arc::clone(&control);
        let storage = self.inner.database.storage.clone();
        let mut join = worker.spawn(move || {
            let result = permit
                .checkout_controlled(Arc::clone(&worker_control))
                .and_then(|mut connection| {
                    let result = connection
                        .isolate_foreign_sql_controlled(Arc::clone(&worker_control), &sql)
                        .and_then(|()| {
                            connection.run_controlled(Arc::clone(&worker_control), |connection| {
                                sql::query_with_scatter_budget(
                                    connection,
                                    &sql,
                                    params.as_ref(),
                                    &budget,
                                )
                            })
                        });
                    retire_if_broken(&mut connection, &result);
                    result
                });
            if result
                .as_ref()
                .is_err_and(|error| error.kind() == EngineErrorKind::DataCorruption)
            {
                storage.record_schema_degraded();
            }
            worker_control.complete(result)
        });

        let result = tokio::select! {
            biased;
            result = &mut join => flatten_join(result),
            reason = wait_for_cancellation(&cancellation, &shutdown_cancel, deadline) => {
                control.request_cancel(reason);
                flatten_join(join.await)
            }
        };
        let result = control.complete(result);
        cancel_on_drop.disarm();
        result
    }

    async fn execute_transaction_control(
        &self,
        operation: &mut Operation,
        session: &Session,
        schema_operation: SchemaOperationGuard,
        mut guard: OwnedMutexGuard<SessionInner>,
        behavior: sql::SessionBehavior,
    ) -> EngineResult<PreparedExecution> {
        match behavior {
            sql::SessionBehavior::Begin => match guard.state() {
                super::SessionState::Ready => {
                    let lifecycle = match self.inner.lifecycle.try_acquire() {
                        Ok(lifecycle) => lifecycle,
                        Err(error) => return operation.finish(Err(error)),
                    };
                    let transaction = TransactionState::new(lifecycle, schema_operation);
                    let completion = transaction.completion_token();
                    guard.begin_transaction(transaction);
                    drop(guard);
                    self.watch_transaction_shutdown(session, completion);
                    operation.finish(Ok(PreparedExecution::Transaction(
                        TransactionExecution::Started,
                    )))
                }
                super::SessionState::InTransaction => operation.finish(Ok(
                    PreparedExecution::Transaction(TransactionExecution::Started),
                )),
                super::SessionState::FailedTransaction => {
                    operation.finish(Err(transaction_aborted()))
                }
                super::SessionState::Closed => operation.finish(Err(closed_transaction_session())),
            },
            sql::SessionBehavior::Commit | sql::SessionBehavior::Rollback => {
                let state = guard.state();
                if state == super::SessionState::Ready {
                    let outcome = if behavior == sql::SessionBehavior::Commit {
                        TransactionExecution::Committed
                    } else {
                        TransactionExecution::RolledBack
                    };
                    return operation.finish(Ok(PreparedExecution::Transaction(outcome)));
                }
                if state == super::SessionState::Closed {
                    return operation.finish(Err(closed_transaction_session()));
                }

                let commit = behavior == sql::SessionBehavior::Commit
                    && state == super::SessionState::InTransaction;
                let outcome = if commit {
                    TransactionExecution::Committed
                } else {
                    TransactionExecution::RolledBack
                };
                let has_connection = guard
                    .transaction_mut()
                    .is_some_and(|transaction| transaction.connection.is_some());
                if !has_connection {
                    let transaction = guard.finish_transaction().ok_or_else(|| {
                        EngineError::new(
                            EngineErrorKind::Internal,
                            "active session transaction state is missing",
                        )
                    });
                    drop(guard);
                    drop(schema_operation);
                    return operation.finish(transaction.and_then(|transaction| {
                        transaction.finish(commit, None)?;
                        Ok(PreparedExecution::Transaction(outcome))
                    }));
                }

                let worker = match operation.wait_pending(self.inner.workers.acquire()).await {
                    Ok(worker) => worker,
                    Err(error) => return operation.finish(Err(error)),
                };
                if let Err(error) = operation.check_before_start() {
                    return operation.finish(Err(error));
                }
                let lease = operation.take_lease();
                let control = Arc::clone(&operation.control);
                let worker_control = Arc::clone(&control);
                let join = worker.spawn(move || {
                    let _lease = lease;
                    let _schema_operation = schema_operation;
                    let transaction = guard.finish_transaction().ok_or_else(|| {
                        EngineError::new(
                            EngineErrorKind::Internal,
                            "active session transaction state is missing",
                        )
                    });
                    worker_control.complete(transaction.and_then(|transaction| {
                        transaction.finish(commit, Some(Arc::clone(&worker_control)))?;
                        Ok(PreparedExecution::Transaction(outcome))
                    }))
                });
                let result = operation.wait_started(join).await;
                operation.finish_started(result)
            }
        }
    }

    fn watch_transaction_shutdown(&self, session: &Session, completion: CancellationToken) {
        let shutdown = self.inner.shutdown_cancel.clone();
        let inner = Arc::downgrade(&session.inner);
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = completion.cancelled() => return,
                _ = shutdown.cancelled() => {}
            }
            let Some(inner) = inner.upgrade() else {
                return;
            };
            let transaction = {
                let mut guard = inner.lock().await;
                guard.finish_transaction()
            };
            if let Some(transaction) = transaction {
                let _ = tokio::task::spawn_blocking(move || transaction.finish(false, None)).await;
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_transaction_prepare(
        &self,
        operation: &mut Operation,
        schema_operation: SchemaOperationGuard,
        mut session: OwnedMutexGuard<SessionInner>,
        database: LogicalDatabaseId,
        translated: sql::TranslatedSql,
        behavior: sql::StatementBehavior,
        parameter_count: usize,
        sqlite_sql: String,
        schema_generation: u64,
    ) -> EngineResult<PreparedStatementId> {
        let worker = match operation.wait_pending(self.inner.workers.acquire()).await {
            Ok(worker) => worker,
            Err(error) => {
                session.fail_transaction();
                return operation.control.complete(Err(error));
            }
        };
        if let Err(error) = operation.check_before_start() {
            session.fail_transaction();
            return operation.control.complete(Err(error));
        }

        let lease = operation.take_lease();
        let worker_control = Arc::clone(&operation.control);
        let storage = self.inner.database.storage.clone();
        let join = worker.spawn(move || {
            let _lease = lease;
            let _schema_operation = schema_operation;
            let result = (|| {
                let mut connection = session
                    .transaction_mut()
                    .ok_or_else(|| {
                        EngineError::new(
                            EngineErrorKind::Internal,
                            "active session transaction state is missing",
                        )
                    })?
                    .connection
                    .take()
                    .ok_or_else(|| {
                        EngineError::new(
                            EngineErrorKind::Internal,
                            "a pinned transaction lost its SQLite connection",
                        )
                    })?;
                let result = connection
                    .run_controlled(Arc::clone(&worker_control), |connection| {
                        sql::describe_statement(connection, &sqlite_sql)
                    })
                    .and_then(|metadata| {
                        ensure_parameter_metadata(parameter_count, metadata.parameter_count())?;
                        let description = PreparedStatementDescription::new(
                            behavior,
                            parameter_count,
                            metadata.columns().to_vec(),
                            schema_generation,
                        );
                        session
                            .prepared_mut()
                            .insert_statement(database, translated, description)
                    });
                retire_if_broken(&mut connection, &result);
                if let Some(transaction) = session.transaction_mut() {
                    transaction.connection = Some(connection);
                }
                result
            })();
            if result.is_err() {
                session.fail_transaction();
            }
            if result
                .as_ref()
                .is_err_and(|error| error.kind() == EngineErrorKind::DataCorruption)
            {
                storage.record_schema_degraded();
            }
            worker_control.complete(result)
        });
        operation.wait_started(join).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_transaction_statement(
        &self,
        operation: &mut Operation,
        shard: u16,
        owner: ConnectionOwner,
        schema_operation: SchemaOperationGuard,
        mut session: OwnedMutexGuard<SessionInner>,
        sqlite_sql: String,
        parameters: Vec<Value>,
        behavior: sql::StatementBehavior,
        result_limits: ResultLimits,
    ) -> EngineResult<PreparedExecution> {
        if session
            .transaction_shard()
            .is_some_and(|pinned| pinned != shard)
        {
            session.fail_transaction();
            return operation.control.complete(Err(cross_shard_transaction()));
        }
        let needs_connection = session
            .transaction_mut()
            .is_some_and(|transaction| transaction.connection.is_none());
        let permit = if needs_connection {
            match operation
                .wait_pending(self.inner.connections.acquire_for_owner(shard, owner))
                .await
            {
                Ok(permit) => Some(permit),
                Err(error) => {
                    session.fail_transaction();
                    return operation.control.complete(Err(error));
                }
            }
        } else {
            None
        };
        let worker = match operation.wait_pending(self.inner.workers.acquire()).await {
            Ok(worker) => worker,
            Err(error) => {
                session.fail_transaction();
                return operation.control.complete(Err(error));
            }
        };
        if let Err(error) = operation.check_before_start() {
            session.fail_transaction();
            return operation.control.complete(Err(error));
        }

        let lease = operation.take_lease();
        let control = Arc::clone(&operation.control);
        let worker_control = Arc::clone(&control);
        let storage = self.inner.database.storage.clone();
        let join = worker.spawn(move || {
            let _lease = lease;
            let _schema_operation = schema_operation;
            let result = (|| {
                let transaction = session.transaction_mut().ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::Internal,
                        "active session transaction state is missing",
                    )
                })?;
                let first_statement = transaction.pinned_shard.is_none();
                let mut connection = match transaction.connection.take() {
                    Some(connection) => connection,
                    None => permit
                        .ok_or_else(|| {
                            EngineError::new(
                                EngineErrorKind::Internal,
                                "a pinned transaction lost its SQLite connection",
                            )
                        })?
                        .checkout_controlled(Arc::clone(&worker_control))?,
                };

                let result = (|| {
                    if first_statement {
                        connection.isolate_foreign_sql_controlled(
                            Arc::clone(&worker_control),
                            &sqlite_sql,
                        )?;
                        connection.run_controlled(Arc::clone(&worker_control), |connection| {
                            connection.execute_batch("BEGIN DEFERRED").map_err(|error| {
                                crate::sqlite_error::storage(error)
                                    .context("failed to begin the pinned SQLite transaction")
                            })
                        })?;
                        transaction.pinned_shard = Some(shard);
                    }
                    connection.run_controlled(Arc::clone(&worker_control), |connection| {
                        sql::execute_statement_with_limits(
                            connection,
                            &sqlite_sql,
                            &parameters,
                            result_limits,
                        )
                        .and_then(|execution| prepared_execution(behavior, execution))
                    })
                })();
                retire_if_broken(&mut connection, &result);
                transaction.connection = Some(connection);
                result
            })();
            if result.is_err() {
                session.fail_transaction();
            }
            if result
                .as_ref()
                .is_err_and(|error| error.kind() == EngineErrorKind::DataCorruption)
            {
                storage.record_schema_degraded();
            }
            worker_control.complete(result)
        });
        operation.wait_started(join).await
    }

    async fn run_on_shard<T, F>(
        &self,
        operation: &mut Operation,
        shard: u16,
        owner: ConnectionOwner,
        schema_operation: SchemaOperationGuard,
        mut session: OwnedMutexGuard<SessionInner>,
        work: F,
    ) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(
                &mut PooledConnection,
                &mut SessionInner,
                Arc<OperationControl>,
            ) -> EngineResult<T>
            + Send
            + 'static,
    {
        let permit = match operation
            .wait_pending(self.inner.connections.acquire_for_owner(shard, owner))
            .await
        {
            Ok(permit) => permit,
            Err(error) => return operation.control.complete(Err(error)),
        };
        let worker = match operation.wait_pending(self.inner.workers.acquire()).await {
            Ok(worker) => worker,
            Err(error) => return operation.control.complete(Err(error)),
        };
        if let Err(error) = operation.check_before_start() {
            return operation.control.complete(Err(error));
        }
        let lease = operation.take_lease();
        let control = Arc::clone(&operation.control);
        let worker_control = Arc::clone(&control);
        let storage = self.inner.database.storage.clone();
        let join = worker.spawn(move || {
            let _lease = lease;
            let _schema_operation = schema_operation;
            let result = permit
                .checkout_controlled(Arc::clone(&worker_control))
                .and_then(|mut connection| {
                    let result = work(&mut connection, &mut session, Arc::clone(&worker_control));
                    retire_if_broken(&mut connection, &result);
                    result
                });
            if result
                .as_ref()
                .is_err_and(|error| error.kind() == EngineErrorKind::DataCorruption)
            {
                // Query execution can surface corruption outside the schema
                // fingerprint's coverage. Persist terminal Degraded state so a
                // restart cannot reopen admission without a complete restore.
                storage.record_schema_degraded();
            }
            worker_control.complete(result)
        });
        operation.wait_started(join).await
    }

    async fn ready_session(
        &self,
        session: &Session,
    ) -> EngineResult<OwnedMutexGuard<SessionInner>> {
        if session.owner != self.inner.id {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "the session belongs to a different engine",
            ));
        }

        let guard = Arc::clone(&session.inner).lock_owned().await;
        guard.ensure_open()?;
        Ok(guard)
    }

    async fn session_with_schema(
        &self,
        operation: &Operation,
        session: &Session,
    ) -> EngineResult<(SchemaOperationGuard, OwnedMutexGuard<SessionInner>)> {
        match self.inner.database.storage.enter_schema_operation() {
            Ok(schema_operation) => operation
                .wait_pending(self.ready_session(session))
                .await
                .map(|guard| (schema_operation, guard)),
            Err(error)
                if error.kind() == EngineErrorKind::Busy && session.owner == self.inner.id =>
            {
                let guard = operation
                    .wait_pending(async { Ok(Arc::clone(&session.inner).lock_owned().await) })
                    .await?;
                match guard.transaction_schema_operation() {
                    Some(schema_operation) => Ok((schema_operation, guard)),
                    None => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    async fn hold_session_for_test(
        &self,
        session: &Session,
        shard: u16,
        started: tokio::sync::oneshot::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> EngineResult<()> {
        let mut operation = self.operation(RequestContext::new())?;
        let schema_operation = self.inner.database.storage.enter_schema_operation()?;
        let guard = match operation.wait_pending(self.ready_session(session)).await {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let owner = ConnectionOwner::new(session.id().get());
        let result = self
            .run_on_shard(
                &mut operation,
                shard,
                owner,
                schema_operation,
                guard,
                move |_, _, _| {
                    let _ = started.send(());
                    release.recv().expect("test releases the blocking worker");
                    Ok(())
                },
            )
            .await;
        operation.finish_started(result)
    }

    #[cfg(test)]
    async fn panic_worker_for_test(&self, session: &Session, shard: u16) -> EngineResult<()> {
        let mut operation = self.operation(RequestContext::new())?;
        let schema_operation = self.inner.database.storage.enter_schema_operation()?;
        let guard = match operation.wait_pending(self.ready_session(session)).await {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let owner = ConnectionOwner::new(session.id().get());
        let result = self
            .run_on_shard(
                &mut operation,
                shard,
                owner,
                schema_operation,
                guard,
                move |_, _, _| panic!("intentional blocking worker panic"),
            )
            .await;
        operation.finish_started(result)
    }

    #[cfg(test)]
    async fn data_corruption_worker_for_test(
        &self,
        session: &Session,
        shard: u16,
    ) -> EngineResult<()> {
        let mut operation = self.operation(RequestContext::new())?;
        let schema_operation = self.inner.database.storage.enter_schema_operation()?;
        let guard = match operation.wait_pending(self.ready_session(session)).await {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let owner = ConnectionOwner::new(session.id().get());
        let result = self
            .run_on_shard(
                &mut operation,
                shard,
                owner,
                schema_operation,
                guard,
                move |_, _, _| {
                    Err(EngineError::new(
                        EngineErrorKind::DataCorruption,
                        "injected SQLite data corruption",
                    ))
                },
            )
            .await;
        operation.finish_started(result)
    }

    #[cfg(test)]
    async fn panic_controlled_worker_for_test(
        &self,
        session: &Session,
        shard: u16,
    ) -> EngineResult<()> {
        let mut operation = self.operation(RequestContext::new())?;
        let schema_operation = self.inner.database.storage.enter_schema_operation()?;
        let guard = match operation.wait_pending(self.ready_session(session)).await {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let owner = ConnectionOwner::new(session.id().get());
        let result = self
            .run_on_shard(
                &mut operation,
                shard,
                owner,
                schema_operation,
                guard,
                move |connection, _session, control| {
                    connection.run_controlled(control, |_connection| -> EngineResult<()> {
                        panic!("intentional controlled SQLite worker panic")
                    })
                },
            )
            .await;
        operation.finish_started(result)
    }

    #[cfg(test)]
    async fn connection_id_for_test(&self, session: &Session, shard: u16) -> EngineResult<u64> {
        self.connection_id_with_context_for_test(session, shard, RequestContext::new())
            .await
    }

    #[cfg(test)]
    async fn connection_id_with_context_for_test(
        &self,
        session: &Session,
        shard: u16,
        context: RequestContext,
    ) -> EngineResult<u64> {
        let mut operation = self.operation(context)?;
        let schema_operation = self.inner.database.storage.enter_schema_operation()?;
        let guard = match operation.wait_pending(self.ready_session(session)).await {
            Ok(guard) => guard,
            Err(error) => return operation.finish(Err(error)),
        };
        let owner = ConnectionOwner::new(session.id().get());
        let result = self
            .run_on_shard(
                &mut operation,
                shard,
                owner,
                schema_operation,
                guard,
                move |connection, _, _| Ok(connection.connection_id()),
            )
            .await;
        operation.finish_started(result)
    }
}

fn prepare_translated_request(
    request: PrepareRequest,
) -> EngineResult<(
    LogicalDatabaseId,
    sql::StatementBehavior,
    sql::TranslatedSql,
)> {
    let (database, dialect, translation_mode, source) = request.into_parts();
    let parsed = sql::parse(dialect, source)?;
    if parsed.statement_count() != 1 {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "a prepared statement must contain exactly one top-level SQL statement",
        ));
    }
    let common = sql::validate_common_subset(parsed)?;
    let classification = sql::classify_statements(&common)?;
    let behavior = classification.behavior(0).ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "prepared statement classification lost its only statement",
        )
    })?;
    let normalized = sql::normalize_placeholders(common)?;
    let translated = sql::translate_sql(normalized, translation_mode)?;
    Ok((database, behavior, translated))
}

fn ensure_parameter_metadata(expected: usize, actual: usize) -> EngineResult<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::Internal,
            "normalized and SQLite prepared-parameter counts disagree",
        ))
    }
}

fn reject_catalog_prepared_target(
    catalog: &super::Catalog,
    database: LogicalDatabaseId,
    normalized: &sql::NormalizedSql,
    parameter_count: usize,
) -> EngineResult<()> {
    // Resolving a Global or Catalog target does not inspect predicate values.
    // Supplying NULL placeholders therefore lets prepare enforce the Catalog
    // boundary before SQLite tries to compile a manifest-only table name. Any
    // value-dependent inference error belongs to bind-time planning and leaves
    // the existing prepared-statement behavior unchanged.
    let placeholders = vec![Value::Null; parameter_count];
    let Ok(inference) = sql::infer_shard_keys(catalog, database, normalized, 0, &placeholders)
    else {
        return Ok(());
    };
    let Some(table) = inference
        .table_id()
        .and_then(|table| catalog.table_by_id(table))
    else {
        return Ok(());
    };
    if matches!(table.placement(), TablePlacement::Catalog) {
        return Err(catalog_target_denied());
    }
    Ok(())
}

fn catalog_target_denied() -> EngineError {
    EngineError::new(
        EngineErrorKind::PermissionDenied,
        "catalog-placed tables cannot execute as client SQL",
    )
}

fn prepared_execution_shard(
    plan: &BoundStatementPlan,
    catalog: &super::Catalog,
) -> EngineResult<u16> {
    if matches!(
        plan.behavior(),
        sql::StatementBehavior::Schema(_) | sql::StatementBehavior::Session(_)
    ) {
        return Err(unsupported_prepared_behavior());
    }

    let is_global = if plan.inference().kind() == sql::ShardKeyInferenceKind::NotSharded {
        let table = plan
            .inference()
            .table_id()
            .and_then(|table| catalog.table_by_id(table))
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    "non-sharded prepared planning lost its catalog table",
                )
            })?;
        match table.placement() {
            TablePlacement::Catalog => {
                return Err(catalog_target_denied());
            }
            TablePlacement::Global => true,
            TablePlacement::Sharded(_) => {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "non-sharded prepared planning resolved a sharded table",
                ));
            }
        }
    } else {
        false
    };

    match plan.behavior() {
        sql::StatementBehavior::Read => {
            if let Some(shard) = plan.assigned_shard() {
                return Ok(shard);
            }
            match plan.inference().kind() {
                sql::ShardKeyInferenceKind::NotApplicable => Ok(0),
                sql::ShardKeyInferenceKind::NotSharded if is_global => Ok(0),
                sql::ShardKeyInferenceKind::NotSharded
                | sql::ShardKeyInferenceKind::Unconstrained
                | sql::ShardKeyInferenceKind::Contradiction
                | sql::ShardKeyInferenceKind::Exact
                | sql::ShardKeyInferenceKind::Multiple => Err(unassigned_prepared_statement()),
            }
        }
        sql::StatementBehavior::Write(_) => plan
            .assigned_shard()
            .ok_or_else(unassigned_prepared_statement),
        sql::StatementBehavior::Schema(_) | sql::StatementBehavior::Session(_) => {
            Err(unsupported_prepared_behavior())
        }
    }
}

fn prepared_execution_shards(
    plan: &BoundStatementPlan,
    catalog: &super::Catalog,
    shard_count: u16,
) -> EngineResult<Vec<u16>> {
    if matches!(
        plan.behavior(),
        sql::StatementBehavior::Schema(_) | sql::StatementBehavior::Session(_)
    ) {
        return Err(unsupported_prepared_behavior());
    }

    let placement = if plan.inference().kind() == sql::ShardKeyInferenceKind::NotSharded {
        let table = plan
            .inference()
            .table_id()
            .and_then(|table| catalog.table_by_id(table))
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    "non-sharded prepared planning lost its catalog table",
                )
            })?;
        match table.placement() {
            TablePlacement::Catalog => {
                return Err(catalog_target_denied());
            }
            TablePlacement::Global => Some(TablePlacement::Global),
            TablePlacement::Sharded(_) => {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "non-sharded prepared planning resolved a sharded table",
                ));
            }
        }
    } else {
        None
    };

    match plan.behavior() {
        sql::StatementBehavior::Read => {
            let mut shards = match plan.inference().kind() {
                sql::ShardKeyInferenceKind::NotApplicable => vec![0],
                sql::ShardKeyInferenceKind::NotSharded
                    if matches!(placement, Some(TablePlacement::Global)) =>
                {
                    vec![0]
                }
                sql::ShardKeyInferenceKind::Unconstrained => (0..shard_count).collect(),
                sql::ShardKeyInferenceKind::Contradiction => vec![0],
                sql::ShardKeyInferenceKind::Exact | sql::ShardKeyInferenceKind::Multiple => plan
                    .inferred_routes()
                    .iter()
                    .map(super::PlannedRoute::shard)
                    .collect(),
                sql::ShardKeyInferenceKind::NotSharded => {
                    return Err(EngineError::new(
                        EngineErrorKind::Internal,
                        "non-sharded prepared planning lost Global placement",
                    ));
                }
            };
            shards.sort_unstable();
            shards.dedup();
            if shards.is_empty() || shards.iter().any(|&shard| shard >= shard_count) {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "prepared read planning produced invalid physical targets",
                ));
            }
            Ok(shards)
        }
        sql::StatementBehavior::Write(_) => plan
            .assigned_shard()
            .map(|shard| vec![shard])
            .ok_or_else(unassigned_prepared_statement),
        sql::StatementBehavior::Schema(_) | sql::StatementBehavior::Session(_) => {
            Err(unsupported_prepared_behavior())
        }
    }
}

fn prepared_execution(
    behavior: sql::StatementBehavior,
    execution: sql::StatementExecution,
) -> EngineResult<PreparedExecution> {
    match (behavior, execution) {
        (sql::StatementBehavior::Read, sql::StatementExecution::Rows(rows)) => {
            Ok(PreparedExecution::Rows(rows))
        }
        (sql::StatementBehavior::Write(_), sql::StatementExecution::AffectedRows(rows)) => {
            Ok(PreparedExecution::AffectedRows(rows))
        }
        _ => Err(EngineError::new(
            EngineErrorKind::Internal,
            "classified statement behavior disagrees with SQLite execution metadata",
        )),
    }
}

fn unsupported_prepared_behavior() -> EngineError {
    EngineError::new(
        EngineErrorKind::Unsupported,
        "schema and session statements require a dedicated engine operation",
    )
}

fn transaction_aborted() -> EngineError {
    EngineError::new(
        EngineErrorKind::TransactionAborted,
        "the transaction is aborted; roll it back before continuing",
    )
}

fn cross_shard_transaction() -> EngineError {
    EngineError::new(
        EngineErrorKind::FailedPrecondition,
        "the statement cannot run because the transaction is pinned to one physical shard",
    )
}

fn explicit_generated_write_unsupported() -> EngineError {
    EngineError::new(
        EngineErrorKind::Unsupported,
        "generated-key writes are not supported inside an explicit transaction",
    )
}

fn closed_transaction_session() -> EngineError {
    EngineError::new(EngineErrorKind::FailedPrecondition, "the session is closed")
}

fn unassigned_prepared_statement() -> EngineError {
    EngineError::new(
        EngineErrorKind::Unsupported,
        "the bound statement does not have one executable physical shard",
    )
}

fn flatten_join<T>(result: Result<EngineResult<T>, tokio::task::JoinError>) -> EngineResult<T> {
    result.map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::Internal,
            "blocking engine task failed",
            error,
        )
    })?
}

fn pending_cancellation_reason(
    request: &CancellationToken,
    shutdown: &CancellationToken,
    deadline: Option<Instant>,
) -> Option<CancellationReason> {
    if request.is_cancelled() || shutdown.is_cancelled() {
        Some(CancellationReason::Cancelled)
    } else if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        Some(CancellationReason::DeadlineExceeded)
    } else {
        None
    }
}

fn write_rows_affected(result: Routed<super::WriteResult>) -> Routed<usize> {
    Routed {
        shard: result.shard,
        value: result.value.rows_affected,
    }
}

fn required_routing_key(session: &SessionInner) -> EngineResult<&str> {
    session.routing_key().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::InvalidArgument,
            "the session has no routing key",
        )
    })
}

fn retire_if_broken<T>(connection: &mut PooledConnection, result: &EngineResult<T>) {
    if result.as_ref().is_err_and(|error| {
        matches!(
            error.kind(),
            EngineErrorKind::StorageUnavailable
                | EngineErrorKind::DataCorruption
                | EngineErrorKind::OutOfMemory
                | EngineErrorKind::Internal
        )
    }) {
        connection.mark_broken();
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error as _, sync::mpsc, time::Duration};

    use tokio::{sync::oneshot, time::timeout};

    use super::*;
    #[cfg(feature = "experimental-vtab")]
    use crate::core::GeneratedIdPolicy;
    use crate::core::{
        Column, DataType, Row, SessionState, ShardKeyMetadata, ShardKeyType, TableDeclaration,
    };

    #[test]
    fn competing_checkpoint_sentinel_is_a_busy_report_without_invented_counts() {
        let report = checkpoint_shard_report(2, 1, -1, -1).unwrap();

        assert_eq!(report.shard(), 2);
        assert!(report.busy());
        assert!(!report.counts_available());
        assert_eq!(report.wal_frames(), 0);
        assert_eq!(report.checkpointed_frames(), 0);
        assert!(!report.complete());
    }

    #[test]
    fn malformed_checkpoint_counts_remain_fail_closed() {
        for (wal_frames, checkpointed_frames) in [(-2, -2), (-1, 0), (0, -1)] {
            let error = checkpoint_shard_report(0, 1, wal_frames, checkpointed_frames).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        }
        assert_eq!(
            checkpoint_shard_report(0, 0, -1, -1).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }

    fn engine() -> (tempfile::TempDir, Engine) {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 4).unwrap());
        (temp, Engine::from_database(database))
    }

    fn engine_with_options(
        shards: u16,
        connections_per_shard: usize,
        queue_capacity_per_shard: usize,
    ) -> (tempfile::TempDir, Engine) {
        let options = EngineOptions::new(connections_per_shard, queue_capacity_per_shard).unwrap();
        engine_with_engine_options(shards, options)
    }

    fn engine_with_engine_options(
        shards: u16,
        options: EngineOptions,
    ) -> (tempfile::TempDir, Engine) {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), shards).unwrap());
        let engine = Engine::from_database_with_options(database, options).unwrap();
        (temp, engine)
    }

    fn engine_with_sharded_events(
        shards: u16,
        options: EngineOptions,
    ) -> (tempfile::TempDir, Engine) {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), shards).unwrap();
        database
            .broadcast(
                "CREATE TABLE events (
                    tenant_id INTEGER PRIMARY KEY,
                    payload TEXT NOT NULL
                 )",
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
            ])
            .unwrap();
        let engine = Engine::from_database_with_options(Arc::new(database), options).unwrap();
        (temp, engine)
    }

    #[cfg(feature = "experimental-vtab")]
    fn engine_with_native_events(shards: u16) -> (tempfile::TempDir, Engine, crate::core::TableId) {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), shards).unwrap();
        database
            .broadcast(
                "CREATE TABLE native_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL
                 );
                 CREATE TABLE ordinary_events (
                    id INTEGER PRIMARY KEY,
                    payload TEXT NOT NULL
                 )",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical_database,
                    "native_events",
                    ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
                )
                .unwrap()
                .with_generated_id_policy(GeneratedIdPolicy::native_range_v1("id").unwrap())
                .unwrap(),
                TableDeclaration::sharded(
                    logical_database,
                    "ordinary_events",
                    ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
        let table = database
            .catalog()
            .table("default", "native_events")
            .unwrap()
            .unwrap()
            .id();
        let options = EngineOptions::default().with_experimental_vtab_writes(true);
        let engine = Engine::from_database_with_options(Arc::new(database), options).unwrap();
        (temp, engine, table)
    }

    #[cfg(feature = "experimental-vtab")]
    fn engine_with_two_native_tables(shards: u16) -> (tempfile::TempDir, Engine) {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), shards).unwrap();
        database
            .broadcast(
                "CREATE TABLE native_events_a (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL
                 );
                 CREATE TABLE native_events_b (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL
                 )",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(
                ["native_events_a", "native_events_b"]
                    .into_iter()
                    .map(|table| {
                        TableDeclaration::sharded(
                            logical_database,
                            table,
                            ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
                        )
                        .unwrap()
                        .with_generated_id_policy(GeneratedIdPolicy::native_range_v1("id").unwrap())
                        .unwrap()
                    })
                    .collect(),
            )
            .unwrap();
        let options = EngineOptions::default().with_experimental_vtab_writes(true);
        let engine = Engine::from_database_with_options(Arc::new(database), options).unwrap();
        (temp, engine)
    }

    #[cfg(feature = "experimental-vtab")]
    fn engine_with_hilo_events(shards: u16) -> (tempfile::TempDir, Engine) {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), shards).unwrap();
        database
            .broadcast(
                "CREATE TABLE hilo_events (
                    id INTEGER PRIMARY KEY,
                    payload TEXT NOT NULL
                 )",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical_database,
                    "hilo_events",
                    ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
                )
                .unwrap()
                .with_generated_id_policy(GeneratedIdPolicy::hilo_v1("id").unwrap())
                .unwrap(),
            ])
            .unwrap();
        let options = EngineOptions::default().with_experimental_vtab_writes(true);
        let engine = Engine::from_database_with_options(Arc::new(database), options).unwrap();
        (temp, engine)
    }

    fn engine_with_prepared_catalog(
        options: EngineOptions,
    ) -> (tempfile::TempDir, Engine, LogicalDatabaseId) {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 4).unwrap();
        database
            .broadcast(
                "CREATE TABLE events (
                    tenant_id INTEGER PRIMARY KEY,
                    payload TEXT NOT NULL
                 );
                 CREATE TABLE global_events (code INTEGER NOT NULL);
                 CREATE TABLE text_events (
                    tenant_key TEXT PRIMARY KEY NOT NULL,
                    payload TEXT NOT NULL
                 );",
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
                TableDeclaration::sharded(
                    logical_database,
                    "text_events",
                    ShardKeyMetadata::new("tenant_key", ShardKeyType::Text).unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
        let database = Arc::new(database);
        let engine = Engine::from_database_with_options(database, options).unwrap();
        (temp, engine, logical_database)
    }

    fn routing_key_for_shard(engine: &Engine, expected: u16) -> String {
        (0_u64..)
            .map(|value| format!("shard-{value}"))
            .find(|key| engine.inner.database.shard_for_key(key.as_bytes()) == expected)
            .expect("the finite shard layout has a routing key")
    }

    fn integer_key_for_shard(engine: &Engine, expected: u16, excluded: Option<i64>) -> i64 {
        (1_i64..)
            .find(|value| {
                Some(*value) != excluded
                    && engine
                        .inner
                        .database
                        .shard_for_key(value.to_string().as_bytes())
                        == expected
            })
            .expect("the finite shard layout has an integer routing key")
    }

    async fn execute_prepared_sql(
        engine: &Engine,
        session: &Session,
        database: LogicalDatabaseId,
        source: &str,
        parameters: Vec<Value>,
    ) -> EngineResult<Executed<PreparedExecution>> {
        let statement = engine
            .prepare_statement(
                session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    source,
                ),
            )
            .await?;
        let portal = match engine.bind_statement(session, statement, parameters).await {
            Ok(portal) => portal,
            Err(error) => {
                let _ = engine.close_prepared_statement(session, statement).await;
                return Err(error);
            }
        };
        let result = engine.execute_portal_logical(session, portal).await;
        let _ = engine.close_portal(session, portal).await;
        let _ = engine.close_prepared_statement(session, statement).await;
        result
    }

    #[test]
    fn prepared_execution_shape_must_match_classified_behavior() {
        let rows = ResultSet::new(vec![Column::new("value", DataType::Int64)], vec![]).unwrap();
        assert_eq!(
            prepared_execution(
                sql::StatementBehavior::Read,
                sql::StatementExecution::Rows(rows.clone()),
            )
            .unwrap(),
            PreparedExecution::Rows(rows.clone())
        );
        assert_eq!(
            prepared_execution(
                sql::StatementBehavior::Write(sql::WriteBehavior::Update),
                sql::StatementExecution::AffectedRows(2),
            )
            .unwrap(),
            PreparedExecution::AffectedRows(2)
        );

        for error in [
            prepared_execution(
                sql::StatementBehavior::Read,
                sql::StatementExecution::AffectedRows(0),
            )
            .unwrap_err(),
            prepared_execution(
                sql::StatementBehavior::Write(sql::WriteBehavior::Delete),
                sql::StatementExecution::Rows(rows),
            )
            .unwrap_err(),
        ] {
            assert_eq!(error.kind(), EngineErrorKind::Internal);
        }
    }

    async fn wait_for_pool_occupancy(engine: &Engine, shard: u16, active: usize, queued: usize) {
        timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = engine.inner.connections.snapshot().unwrap();
                let shard = snapshot.shards[usize::from(shard)];
                if shard.active == active && shard.queued == queued {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pool occupancy should reach the expected state");
    }

    async fn wait_for_blocking_signal(receiver: mpsc::Receiver<()>, message: &'static str) {
        timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking(move || receiver.recv().unwrap()),
        )
        .await
        .expect(message)
        .unwrap();
    }

    async fn wait_for_worker_capacity(engine: &Engine, expected: usize) {
        timeout(Duration::from_secs(2), async {
            while engine.inner.workers.available_permits() != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocking-worker capacity should be restored");
    }

    fn assert_send_sync<T: Send + Sync>() {}

    fn assert_send<T: Send>(_: T) {}

    fn assert_send_static<T: Send + 'static>(_: T) {}

    #[test]
    fn owned_public_types_have_expected_thread_safety_and_accessors() {
        assert_send_sync::<Engine>();
        assert_send_sync::<Session>();
        assert_send_sync::<PrepareRequest>();
        assert_send_sync::<PreparedStatementId>();
        assert_send_sync::<PortalId>();
        assert_send_sync::<DescribeTarget>();
        assert_send_sync::<PreparedStatementDescription>();
        assert_send_sync::<PreparedExecution>();

        let statement = Statement::new("SELECT ?1", vec![Value::from(42_i64)]);
        assert_eq!(statement.sql(), "SELECT ?1");
        assert_eq!(statement.params(), [Value::from(42_i64)]);
        assert_send_static(statement.clone());
        assert_eq!(
            statement.into_parts(),
            ("SELECT ?1".to_owned(), vec![Value::from(42_i64)])
        );
    }

    #[tokio::test]
    async fn open_and_status_cross_the_async_engine_boundary() {
        let temp = tempfile::tempdir().unwrap();
        let engine = Engine::open(temp.path(), 4).await.unwrap();
        let session = engine.session();

        assert_send(engine.status(&session));
        assert_send(engine.query(&session, Statement::new("SELECT 1", vec![])));
        assert_eq!(engine.shard_count(), 4);
        assert_eq!(engine.options(), EngineOptions::default());
        let status = engine.status(&session).await.unwrap();
        assert_eq!(status.shard_count(), 4);
        assert_eq!(status.max_blocking_workers(), 16);
        assert_eq!(status.connections_per_shard(), 4);
        assert_eq!(status.queue_capacity_per_shard(), 32);
        assert_eq!(
            status.prepared_statement_limits(),
            PreparedStatementLimits::default()
        );
        assert_eq!(session.state().await, SessionState::Ready);
    }

    #[tokio::test]
    async fn status_obeys_migrating_and_degraded_schema_admission_before_session_waits() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let session = Arc::new(engine.session());
        let (holder_started_tx, holder_started_rx) = oneshot::channel();
        let (holder_release_tx, holder_release_rx) = mpsc::channel();
        let holder_engine = engine.clone();
        let holder_session = Arc::clone(&session);
        let holder = tokio::spawn(async move {
            holder_engine
                .hold_session_for_test(&holder_session, 0, holder_started_tx, holder_release_rx)
                .await
        });
        timeout(Duration::from_secs(2), holder_started_rx)
            .await
            .unwrap()
            .unwrap();

        let migration = engine
            .inner
            .database
            .storage
            .begin_schema_migration()
            .unwrap();
        assert_eq!(
            engine.inner.database.storage.schema_gate_snapshot(),
            crate::storage::SchemaGateSnapshot {
                state: crate::storage::SchemaGateState::Migrating,
                active_operations: 1,
            }
        );
        let busy = timeout(Duration::from_secs(2), engine.status(&session))
            .await
            .expect("schema admission should reject status before its busy session is awaited")
            .unwrap_err();
        assert_eq!(busy.kind(), EngineErrorKind::Busy);
        assert!(busy.is_retryable());
        assert_eq!(engine.active_operations_for_test(), 1);

        drop(migration);
        holder_release_tx.send(()).unwrap();
        holder.await.unwrap().unwrap();
        assert_eq!(engine.status(&session).await.unwrap().shard_count(), 2);

        engine.inner.database.storage.mark_schema_degraded();
        let corruption = engine.status(&session).await.unwrap_err();
        assert_eq!(corruption.kind(), EngineErrorKind::DataCorruption);
        assert!(!corruption.is_retryable());
        assert_eq!(session.state().await, SessionState::Ready);
        assert_eq!(engine.active_operations_for_test(), 0);
        assert_eq!(
            engine.inner.database.storage.schema_gate_snapshot(),
            crate::storage::SchemaGateSnapshot {
                state: crate::storage::SchemaGateState::Degraded,
                active_operations: 0,
            }
        );
    }

    #[tokio::test]
    async fn prepared_lifecycle_returns_the_same_typed_result_for_every_sql_dialect() {
        let (_temp, engine, database) = engine_with_prepared_catalog(EngineOptions::default());
        let session = engine.session();

        let insert_requests = [
            (
                sql::SqlDialect::Sqlite,
                sql::SqlTranslationMode::StrictSqlite,
                "INSERT INTO events (tenant_id, payload) VALUES (?1, ?2)",
                7_i64,
                "seven",
            ),
            (
                sql::SqlDialect::PostgreSql,
                sql::SqlTranslationMode::Compatibility,
                "INSERT INTO events (tenant_id, payload) VALUES ($1, $2)",
                8_i64,
                "eight",
            ),
            (
                sql::SqlDialect::MySql,
                sql::SqlTranslationMode::Compatibility,
                "INSERT INTO events (tenant_id, payload) VALUES (?, ?)",
                9_i64,
                "nine",
            ),
        ];
        let mut insert_descriptions = Vec::new();
        for (dialect, mode, source, tenant_id, payload) in insert_requests {
            let statement = engine
                .prepare_statement(
                    &session,
                    PrepareRequest::new(database, dialect, mode, source),
                )
                .await
                .unwrap();
            let description = engine
                .describe_prepared(&session, DescribeTarget::Statement(statement))
                .await
                .unwrap();
            assert_eq!(
                description.parameter_types(),
                [DataType::Unknown, DataType::Unknown]
            );
            assert_eq!(
                description.behavior(),
                sql::StatementBehavior::Write(sql::WriteBehavior::Insert)
            );
            assert!(description.columns().is_empty());
            assert!(!description.returns_rows());
            insert_descriptions.push(description);

            let portal = engine
                .bind_statement(
                    &session,
                    statement,
                    vec![Value::from(tenant_id), Value::from(payload)],
                )
                .await
                .unwrap();
            let inserted = engine.execute_portal(&session, portal).await.unwrap();
            assert_eq!(inserted.value, PreparedExecution::AffectedRows(1));
            assert_eq!(
                inserted.shard,
                engine
                    .inner
                    .database
                    .shard_for_key(tenant_id.to_string().as_bytes())
            );
            assert!(engine.close_portal(&session, portal).await.unwrap());
            assert!(
                engine
                    .close_prepared_statement(&session, statement)
                    .await
                    .unwrap()
            );
        }
        assert!(
            insert_descriptions
                .windows(2)
                .all(|pair| pair[0] == pair[1])
        );

        let requests = [
            (
                sql::SqlDialect::Sqlite,
                sql::SqlTranslationMode::StrictSqlite,
                "SELECT tenant_id, payload FROM events WHERE tenant_id = ?1",
            ),
            (
                sql::SqlDialect::PostgreSql,
                sql::SqlTranslationMode::Compatibility,
                "SELECT tenant_id, payload FROM events WHERE tenant_id = $1",
            ),
            (
                sql::SqlDialect::MySql,
                sql::SqlTranslationMode::Compatibility,
                "SELECT tenant_id, payload FROM events WHERE tenant_id = ?",
            ),
        ];
        let expected = ResultSet::new(
            vec![
                Column::new("tenant_id", DataType::Int64),
                Column::new("payload", DataType::Text),
            ],
            vec![Row::new(vec![Value::from(7_i64), Value::from("seven")])],
        )
        .unwrap();
        let mut observed = Vec::new();
        for (dialect, mode, source) in requests {
            let statement = engine
                .prepare_statement(
                    &session,
                    PrepareRequest::new(database, dialect, mode, source),
                )
                .await
                .unwrap();
            let description = engine
                .describe_prepared(&session, DescribeTarget::Statement(statement))
                .await
                .unwrap();
            assert_eq!(description.behavior(), sql::StatementBehavior::Read);
            assert_eq!(description.parameter_types(), [DataType::Unknown]);
            assert_eq!(description.columns(), expected.columns());
            assert!(description.returns_rows());
            let portal = engine
                .bind_statement(&session, statement, vec![Value::from(7_i64)])
                .await
                .unwrap();
            assert_eq!(
                engine
                    .describe_prepared(&session, DescribeTarget::Portal(portal))
                    .await
                    .unwrap(),
                description
            );
            observed.push(engine.execute_portal(&session, portal).await.unwrap());
            assert!(engine.close_portal(&session, portal).await.unwrap());
            assert!(
                engine
                    .close_prepared_statement(&session, statement)
                    .await
                    .unwrap()
            );
        }
        assert!(observed.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(observed[0].value, PreparedExecution::Rows(expected));
    }

    #[tokio::test]
    async fn prepared_schema_is_blocked_while_transactions_commit_rollback_and_recover() {
        let (temp, engine, database) = engine_with_prepared_catalog(EngineOptions::default());
        let session = engine.session();

        for (source, expected, object_name) in [
            (
                "CREATE TABLE blocked_prepared_schema_table (id INTEGER)",
                sql::SchemaBehavior::CreateTable,
                "blocked_prepared_schema_table",
            ),
            (
                "CREATE INDEX blocked_prepared_schema_index ON events (payload)",
                sql::SchemaBehavior::CreateIndex,
                "blocked_prepared_schema_index",
            ),
        ] {
            let schema_sql = sql::normalize_placeholders(
                sql::validate_common_subset(sql::parse(sql::SqlDialect::Sqlite, source).unwrap())
                    .unwrap(),
            )
            .unwrap();
            let schema_plan = engine
                .plan_bound_statement(database, &schema_sql, 0, &[], None)
                .unwrap();
            assert_eq!(
                schema_plan.behavior(),
                sql::StatementBehavior::Schema(expected)
            );
            assert_eq!(
                prepared_execution_shard(&schema_plan, engine.catalog())
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::Unsupported
            );

            let schema_error = engine
                .prepare_statement(
                    &session,
                    PrepareRequest::new(
                        database,
                        sql::SqlDialect::Sqlite,
                        sql::SqlTranslationMode::StrictSqlite,
                        source,
                    ),
                )
                .await
                .unwrap_err();
            assert_eq!(schema_error.kind(), EngineErrorKind::PermissionDenied);
            assert_eq!(session.inner.lock().await.prepared().statement_count(), 0);
            for shard in 0..engine.shard_count() {
                let connection = rusqlite::Connection::open(
                    temp.path().join(format!("shards/{shard:04}.sqlite")),
                )
                .unwrap();
                let count = connection
                    .query_row(
                        "SELECT COUNT(*) FROM sqlite_schema WHERE name = ?1",
                        [object_name],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap();
                assert_eq!(count, 0);
            }
        }

        let first_key = integer_key_for_shard(&engine, 0, None);
        assert_eq!(
            execute_prepared_sql(&engine, &session, database, "BEGIN", vec![])
                .await
                .unwrap()
                .value,
            PreparedExecution::Transaction(TransactionExecution::Started)
        );
        assert_eq!(session.state().await, SessionState::InTransaction);
        assert_eq!(engine.active_operations_for_test(), 1);

        execute_prepared_sql(
            &engine,
            &session,
            database,
            "INSERT INTO events (tenant_id, payload) VALUES (?, ?)",
            vec![Value::from(first_key), Value::from("uncommitted")],
        )
        .await
        .unwrap();
        let visible = execute_prepared_sql(
            &engine,
            &session,
            database,
            "SELECT payload FROM events WHERE tenant_id = ?",
            vec![Value::from(first_key)],
        )
        .await
        .unwrap();
        assert!(matches!(
            visible.value,
            PreparedExecution::Rows(rows)
                if rows.rows()[0].get(0) == Some(&Value::from("uncommitted"))
        ));
        assert_eq!(
            execute_prepared_sql(&engine, &session, database, "COMMIT", vec![])
                .await
                .unwrap()
                .value,
            PreparedExecution::Transaction(TransactionExecution::Committed)
        );
        assert_eq!(session.state().await, SessionState::Ready);
        assert_eq!(engine.active_operations_for_test(), 0);

        execute_prepared_sql(&engine, &session, database, "BEGIN", vec![])
            .await
            .unwrap();
        execute_prepared_sql(
            &engine,
            &session,
            database,
            "UPDATE events SET payload = ? WHERE tenant_id = ?",
            vec![Value::from("rolled-back"), Value::from(first_key)],
        )
        .await
        .unwrap();
        assert_eq!(
            execute_prepared_sql(&engine, &session, database, "ROLLBACK", vec![])
                .await
                .unwrap()
                .value,
            PreparedExecution::Transaction(TransactionExecution::RolledBack)
        );
        let recovered = execute_prepared_sql(
            &engine,
            &session,
            database,
            "SELECT payload FROM events WHERE tenant_id = ?",
            vec![Value::from(first_key)],
        )
        .await
        .unwrap();
        assert!(matches!(
            recovered.value,
            PreparedExecution::Rows(rows)
                if rows.rows()[0].get(0) == Some(&Value::from("uncommitted"))
        ));
        assert_eq!(session.routing_key().await, None);
    }

    #[tokio::test]
    async fn transaction_pins_one_shard_enters_failed_state_and_commit_rolls_back() {
        let (_temp, engine, database) = engine_with_prepared_catalog(EngineOptions::default());
        let session = engine.session();
        let first_key = integer_key_for_shard(&engine, 0, None);
        let other_key = integer_key_for_shard(&engine, 1, None);

        execute_prepared_sql(&engine, &session, database, "BEGIN", vec![])
            .await
            .unwrap();
        execute_prepared_sql(
            &engine,
            &session,
            database,
            "INSERT INTO events (tenant_id, payload) VALUES (?, ?)",
            vec![Value::from(first_key), Value::from("first")],
        )
        .await
        .unwrap();
        let cross_shard = execute_prepared_sql(
            &engine,
            &session,
            database,
            "INSERT INTO events (tenant_id, payload) VALUES (?, ?)",
            vec![Value::from(other_key), Value::from("other")],
        )
        .await
        .unwrap_err();
        assert_eq!(cross_shard.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(session.state().await, SessionState::FailedTransaction);

        let rejected = execute_prepared_sql(
            &engine,
            &session,
            database,
            "SELECT payload FROM events WHERE tenant_id = ?",
            vec![Value::from(first_key)],
        )
        .await
        .unwrap_err();
        assert_eq!(rejected.kind(), EngineErrorKind::TransactionAborted);
        assert_eq!(
            execute_prepared_sql(&engine, &session, database, "COMMIT", vec![])
                .await
                .unwrap()
                .value,
            PreparedExecution::Transaction(TransactionExecution::RolledBack)
        );
        assert_eq!(session.state().await, SessionState::Ready);

        for key in [first_key, other_key] {
            let rows = execute_prepared_sql(
                &engine,
                &session,
                database,
                "SELECT payload FROM events WHERE tenant_id = ?",
                vec![Value::from(key)],
            )
            .await
            .unwrap();
            assert!(matches!(rows.value, PreparedExecution::Rows(rows) if rows.rows().is_empty()));
        }

        execute_prepared_sql(&engine, &session, database, "BEGIN", vec![])
            .await
            .unwrap();
        let parse_error = execute_prepared_sql(
            &engine,
            &session,
            database,
            "SELECT FROM private_transaction_text",
            vec![],
        )
        .await
        .unwrap_err();
        assert_eq!(parse_error.kind(), EngineErrorKind::InvalidQuery);
        assert_eq!(session.state().await, SessionState::FailedTransaction);
        execute_prepared_sql(&engine, &session, database, "ROLLBACK", vec![])
            .await
            .unwrap();
        assert_eq!(session.state().await, SessionState::Ready);
    }

    #[tokio::test]
    async fn closing_a_session_rolls_back_and_releases_its_pinned_connection() {
        let (_temp, engine, database) = engine_with_prepared_catalog(
            EngineOptions::new(1, 1).expect("one connection and one waiter are valid"),
        );
        let session = engine.session();
        let key = integer_key_for_shard(&engine, 0, None);

        execute_prepared_sql(&engine, &session, database, "BEGIN", vec![])
            .await
            .unwrap();
        execute_prepared_sql(
            &engine,
            &session,
            database,
            "INSERT INTO events (tenant_id, payload) VALUES (?, ?)",
            vec![Value::from(key), Value::from("discarded")],
        )
        .await
        .unwrap();
        assert_eq!(engine.active_operations_for_test(), 1);
        session.close().await.unwrap();
        assert_eq!(session.state().await, SessionState::Closed);
        assert_eq!(engine.active_operations_for_test(), 0);

        let observer = engine.session();
        let rows = execute_prepared_sql(
            &engine,
            &observer,
            database,
            "SELECT payload FROM events WHERE tenant_id = ?",
            vec![Value::from(key)],
        )
        .await
        .unwrap();
        assert!(matches!(rows.value, PreparedExecution::Rows(rows) if rows.rows().is_empty()));
    }

    #[tokio::test]
    async fn pinned_transaction_holds_pool_capacity_until_rollback_then_waiter_runs() {
        let (_temp, engine, database) = engine_with_prepared_catalog(
            EngineOptions::new(1, 1).expect("one connection and one waiter are valid"),
        );
        let transaction = engine.session();
        let waiter = engine.session();
        let first_key = integer_key_for_shard(&engine, 0, None);
        let second_key = integer_key_for_shard(&engine, 0, Some(first_key));

        execute_prepared_sql(&engine, &transaction, database, "BEGIN", vec![])
            .await
            .unwrap();
        execute_prepared_sql(
            &engine,
            &transaction,
            database,
            "INSERT INTO events (tenant_id, payload) VALUES (?, ?)",
            vec![Value::from(first_key), Value::from("transaction")],
        )
        .await
        .unwrap();

        let waiting = execute_prepared_sql(
            &engine,
            &waiter,
            database,
            "INSERT INTO events (tenant_id, payload) VALUES (?, ?)",
            vec![Value::from(second_key), Value::from("waiter")],
        );
        tokio::pin!(waiting);
        assert!(
            timeout(Duration::from_millis(25), &mut waiting)
                .await
                .is_err()
        );
        execute_prepared_sql(&engine, &transaction, database, "ROLLBACK", vec![])
            .await
            .unwrap();
        waiting.await.unwrap();
    }

    #[tokio::test]
    async fn forced_shutdown_rolls_back_an_idle_pinned_transaction_before_stopping() {
        let (temp, engine, database) = engine_with_prepared_catalog(EngineOptions::default());
        let session = engine.session();
        let key = integer_key_for_shard(&engine, 0, None);

        execute_prepared_sql(&engine, &session, database, "BEGIN", vec![])
            .await
            .unwrap();
        execute_prepared_sql(
            &engine,
            &session,
            database,
            "INSERT INTO events (tenant_id, payload) VALUES (?, ?)",
            vec![Value::from(key), Value::from("shutdown")],
        )
        .await
        .unwrap();

        let report = engine
            .shutdown_with_grace(Duration::from_millis(20))
            .await
            .unwrap();
        assert!(report.forced());
        assert_eq!(engine.state(), EngineState::Stopped);
        assert_eq!(engine.active_operations_for_test(), 0);
        assert_eq!(session.state().await, SessionState::Ready);

        let connection =
            rusqlite::Connection::open(temp.path().join("shards/0000.sqlite")).unwrap();
        let count = connection
            .query_row(
                "SELECT COUNT(*) FROM events WHERE tenant_id = ?1",
                [key],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn transaction_can_commit_while_a_schema_migration_waits_for_its_admission() {
        let (_temp, engine, database) = engine_with_prepared_catalog(EngineOptions::default());
        let transaction = engine.session();
        let key = integer_key_for_shard(&engine, 0, None);

        execute_prepared_sql(&engine, &transaction, database, "BEGIN", vec![])
            .await
            .unwrap();
        execute_prepared_sql(
            &engine,
            &transaction,
            database,
            "INSERT INTO events (tenant_id, payload) VALUES (?, ?)",
            vec![Value::from(key), Value::from("committed")],
        )
        .await
        .unwrap();

        let migration_engine = engine.clone();
        let migration_session = migration_engine.session();
        let migration = tokio::spawn(async move {
            migration_engine
                .broadcast(
                    &migration_session,
                    "CREATE INDEX after_transaction ON events(payload)".to_owned(),
                )
                .await
        });
        while engine.inner.database.storage.schema_gate_snapshot().state
            != crate::storage::SchemaGateState::Migrating
        {
            tokio::task::yield_now().await;
        }

        assert_eq!(
            execute_prepared_sql(&engine, &transaction, database, "COMMIT", vec![])
                .await
                .unwrap()
                .value,
            PreparedExecution::Transaction(TransactionExecution::Committed)
        );
        assert_eq!(migration.await.unwrap().unwrap(), vec![0, 1, 2, 3]);
        assert_eq!(
            engine.inner.database.storage.schema_gate_snapshot().state,
            crate::storage::SchemaGateState::Ready
        );
    }

    #[tokio::test]
    async fn prepared_cache_and_portal_limits_are_session_local_without_eviction() {
        let limits = PreparedStatementLimits::new(1, 1, 64).unwrap();
        let options = EngineOptions::default().with_prepared_statement_limits(limits);
        let (_temp, engine, database) = engine_with_prepared_catalog(options);
        let first_session = engine.session();
        let second_session = engine.session();
        let request = || {
            PrepareRequest::new(
                database,
                sql::SqlDialect::Sqlite,
                sql::SqlTranslationMode::StrictSqlite,
                "SELECT payload FROM events WHERE tenant_id = ?1",
            )
        };

        let first = engine
            .prepare_statement(&first_session, request())
            .await
            .unwrap();
        let duplicate_error = engine
            .prepare_statement(&first_session, request())
            .await
            .unwrap_err();
        assert_eq!(duplicate_error.kind(), EngineErrorKind::LimitExceeded);
        assert!(
            engine
                .describe_prepared(&first_session, DescribeTarget::Statement(first))
                .await
                .is_ok()
        );

        let independent = engine
            .prepare_statement(&second_session, request())
            .await
            .unwrap();
        assert_ne!(first, independent);
        assert_eq!(
            engine
                .describe_prepared(&second_session, DescribeTarget::Statement(first))
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );

        let first_portal = engine
            .bind_statement(&first_session, first, vec![Value::from(1_i64)])
            .await
            .unwrap();
        assert_eq!(
            engine
                .bind_statement(&first_session, first, vec![Value::from(2_i64)])
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        assert!(
            engine
                .describe_prepared(&first_session, DescribeTarget::Portal(first_portal))
                .await
                .is_ok()
        );
        assert!(
            engine
                .close_portal(&first_session, first_portal)
                .await
                .unwrap()
        );
        assert!(
            !engine
                .close_portal(&first_session, first_portal)
                .await
                .unwrap()
        );
        let replacement_portal = engine
            .bind_statement(&first_session, first, vec![Value::from(2_i64)])
            .await
            .unwrap();

        assert!(
            engine
                .close_prepared_statement(&first_session, first)
                .await
                .unwrap()
        );
        assert_eq!(
            engine
                .execute_portal(&first_session, replacement_portal)
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert!(
            !engine
                .close_prepared_statement(&first_session, first)
                .await
                .unwrap()
        );
        let replacement = engine
            .prepare_statement(&first_session, request())
            .await
            .unwrap();
        assert!(replacement > first);

        assert!(
            engine
                .close_prepared_statement(&first_session, replacement)
                .await
                .unwrap()
        );
        assert!(
            engine
                .close_prepared_statement(&second_session, independent)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn repeated_markers_are_bounded_before_planning_and_bind_recovers() {
        let limits = PreparedStatementLimits::new(2, 2, 64).unwrap();
        let options = EngineOptions::default().with_prepared_statement_limits(limits);
        let (_temp, engine, database) = engine_with_prepared_catalog(options);
        let session = engine.session();
        let statement = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    "INSERT INTO text_events (tenant_key, payload) VALUES (?1, ?1)",
                ),
            )
            .await
            .unwrap();

        let error = engine
            .bind_statement(
                &session,
                statement,
                vec![Value::from("abcdefghijklmnopqrst")],
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(session.inner.lock().await.prepared().portal_count(), 0);

        let portal = engine
            .bind_statement(&session, statement, vec![Value::from("a")])
            .await
            .unwrap();
        assert_eq!(session.inner.lock().await.prepared().portal_count(), 1);
        assert_eq!(
            engine.execute_portal(&session, portal).await.unwrap().value,
            PreparedExecution::AffectedRows(1)
        );
    }

    #[tokio::test]
    async fn failed_physical_prepare_does_not_consume_statement_capacity() {
        let limits = PreparedStatementLimits::new(1, 1, 64).unwrap();
        let options = EngineOptions::default().with_prepared_statement_limits(limits);
        let (_temp, engine, database) = engine_with_prepared_catalog(options);
        let session = engine.session();

        let error = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    "SELECT * FROM missing_table",
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
        assert_eq!(session.inner.lock().await.prepared().statement_count(), 0);

        engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    "SELECT 1",
                ),
            )
            .await
            .unwrap();
        assert_eq!(session.inner.lock().await.prepared().statement_count(), 1);
    }

    #[tokio::test]
    async fn prepared_errors_are_atomic_and_recover_without_losing_valid_handles() {
        let (temp, engine, database) = engine_with_prepared_catalog(EngineOptions::default());
        let session = engine.session();

        for source in [
            "",
            "SELECT 1; SELECT 2",
            "SELECT 1; PRAGMA user_version",
            "SELECT 1; SELECT ?0",
        ] {
            let error = engine
                .prepare_statement(
                    &session,
                    PrepareRequest::new(
                        database,
                        sql::SqlDialect::Sqlite,
                        sql::SqlTranslationMode::StrictSqlite,
                        source,
                    ),
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
            assert_eq!(
                error.diagnostic(),
                "a prepared statement must contain exactly one top-level SQL statement"
            );
        }
        let unknown_database = LogicalDatabaseId::new(999).unwrap();
        assert_eq!(
            engine
                .prepare_statement(
                    &session,
                    PrepareRequest::new(
                        unknown_database,
                        sql::SqlDialect::Sqlite,
                        sql::SqlTranslationMode::StrictSqlite,
                        "SELECT 1",
                    ),
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );

        let statement = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    "SELECT payload FROM events WHERE tenant_id = ?1 AND payload = ?2",
                ),
            )
            .await
            .unwrap();
        for parameters in [
            vec![Value::from(1_i64)],
            vec![Value::from(1_i64), Value::from("value"), Value::Null],
        ] {
            assert_eq!(
                engine
                    .bind_statement(&session, statement, parameters)
                    .await
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::InvalidArgument
            );
        }
        let too_large = u64::try_from(i64::MAX).unwrap() + 1;
        assert_eq!(
            engine
                .bind_statement(
                    &session,
                    statement,
                    vec![Value::from(1_i64), Value::from(too_large)],
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::NumericOutOfRange
        );
        let portal = engine
            .bind_statement(
                &session,
                statement,
                vec![Value::from(1_i64), Value::from("missing")],
            )
            .await
            .unwrap();
        assert!(matches!(
            engine.execute_portal(&session, portal).await.unwrap().value,
            PreparedExecution::Rows(result) if result.is_empty()
        ));

        let scalar = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    "SELECT 1 AS value",
                ),
            )
            .await
            .unwrap();
        let scalar_portal = engine
            .bind_statement(&session, scalar, vec![])
            .await
            .unwrap();
        let scalar_result = engine
            .execute_portal(&session, scalar_portal)
            .await
            .unwrap();
        assert_eq!(scalar_result.shard, 0);
        assert!(matches!(
            scalar_result.value,
            PreparedExecution::Rows(result)
                if result.rows()[0].get(0) == Some(&Value::from(1_i64))
        ));

        let global = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    "SELECT code FROM global_events ORDER BY code",
                ),
            )
            .await
            .unwrap();
        let global_portal = engine
            .bind_statement(&session, global, vec![])
            .await
            .unwrap();
        let global_result = engine
            .execute_portal(&session, global_portal)
            .await
            .unwrap();
        assert_eq!(global_result.shard, 0);
        assert!(matches!(
            global_result.value,
            PreparedExecution::Rows(result) if result.is_empty()
        ));

        let global_update = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    "UPDATE global_events SET code = 1",
                ),
            )
            .await
            .unwrap();
        let global_update_description = engine
            .describe_prepared(&session, DescribeTarget::Statement(global_update))
            .await
            .unwrap();
        assert_eq!(
            global_update_description.behavior(),
            sql::StatementBehavior::Write(sql::WriteBehavior::Update)
        );
        let global_update_portal = engine
            .bind_statement(&session, global_update, vec![])
            .await
            .unwrap();
        assert_eq!(
            engine
                .execute_portal(&session, global_update_portal)
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
        for shard in 0..engine.shard_count() {
            let connection =
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM global_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                0
            );
        }

        assert_eq!(
            engine
                .prepare_statement(
                    &session,
                    PrepareRequest::new(
                        database,
                        sql::SqlDialect::Sqlite,
                        sql::SqlTranslationMode::StrictSqlite,
                        "SELECT code FROM catalog_records",
                    ),
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::PermissionDenied
        );
        assert_eq!(
            engine
                .prepare_statement(
                    &session,
                    PrepareRequest::new(
                        database,
                        sql::SqlDialect::Sqlite,
                        sql::SqlTranslationMode::StrictSqlite,
                        "UPDATE catalog_records SET code = 1",
                    ),
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::PermissionDenied
        );

        let scatter = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    "SELECT payload FROM events",
                ),
            )
            .await
            .unwrap();
        let scatter_portal = engine
            .bind_statement(&session, scatter, vec![])
            .await
            .unwrap();
        assert_eq!(
            engine
                .execute_portal(&session, scatter_portal)
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
        assert!(
            engine
                .describe_prepared(&session, DescribeTarget::Statement(statement))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn execution_replans_and_description_refreshes_after_schema_migration() {
        let (_temp, engine, database) = engine_with_prepared_catalog(EngineOptions::default());
        let session = engine.session();
        session.set_routing_key("7").await.unwrap();
        engine
            .execute(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, payload) VALUES (?1, ?2)",
                    vec![Value::from(7_i64), Value::from("seven")],
                ),
            )
            .await
            .unwrap();
        session.clear_routing_key().await.unwrap();

        let statement = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    "SELECT * FROM events WHERE tenant_id = ?1",
                ),
            )
            .await
            .unwrap();
        let before = engine
            .describe_prepared(&session, DescribeTarget::Statement(statement))
            .await
            .unwrap();
        assert_eq!(before.behavior(), sql::StatementBehavior::Read);
        assert_eq!(before.columns().len(), 2);
        let portal = engine
            .bind_statement(&session, statement, vec![Value::from(7_i64)])
            .await
            .unwrap();

        engine
            .broadcast(
                &session,
                "ALTER TABLE events ADD COLUMN extra TEXT".to_owned(),
            )
            .await
            .unwrap();

        let executed = engine.execute_portal(&session, portal).await.unwrap();
        assert_eq!(executed.shard, engine.inner.database.shard_for_key(b"7"));
        let PreparedExecution::Rows(rows) = executed.value else {
            panic!("the prepared SELECT must return rows");
        };
        assert_eq!(rows.columns().len(), 3);
        assert_eq!(
            rows.rows()[0].values(),
            [Value::from(7_i64), Value::from("seven"), Value::Null,]
        );

        let after = engine
            .describe_prepared(&session, DescribeTarget::Portal(portal))
            .await
            .unwrap();
        assert_eq!(after.behavior(), sql::StatementBehavior::Read);
        assert_eq!(after.columns().len(), 3);
        assert!(after.schema_generation() > before.schema_generation());
        assert_eq!(rows.columns(), after.columns());
        assert_eq!(
            engine
                .describe_prepared(&session, DescribeTarget::Portal(portal))
                .await
                .unwrap()
                .schema_generation(),
            after.schema_generation()
        );
    }

    #[tokio::test]
    async fn failed_description_refresh_preserves_cached_metadata_and_can_retry() {
        let (_temp, engine, database) = engine_with_prepared_catalog(EngineOptions::default());
        let session = engine.session();
        engine
            .broadcast(
                &session,
                "CREATE VIEW refreshable_events AS
                 SELECT tenant_id, payload FROM events"
                    .to_owned(),
            )
            .await
            .unwrap();
        let statement = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    "SELECT * FROM refreshable_events",
                ),
            )
            .await
            .unwrap();
        let before = engine
            .describe_prepared(&session, DescribeTarget::Statement(statement))
            .await
            .unwrap();
        assert_eq!(before.behavior(), sql::StatementBehavior::Read);
        assert_eq!(before.columns().len(), 2);

        engine
            .broadcast(&session, "DROP VIEW refreshable_events".to_owned())
            .await
            .unwrap();
        let error = engine
            .describe_prepared(&session, DescribeTarget::Statement(statement))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
        let cached = session
            .inner
            .lock()
            .await
            .prepared()
            .statement(statement)
            .unwrap()
            .description()
            .clone();
        assert_eq!(cached, before);

        engine
            .broadcast(
                &session,
                "CREATE VIEW refreshable_events AS
                 SELECT tenant_id, payload, NULL AS restored FROM events"
                    .to_owned(),
            )
            .await
            .unwrap();
        let refreshed = engine
            .describe_prepared(&session, DescribeTarget::Statement(statement))
            .await
            .unwrap();
        assert_eq!(refreshed.behavior(), sql::StatementBehavior::Read);
        assert_eq!(refreshed.columns().len(), 3);
        assert!(refreshed.schema_generation() > before.schema_generation());
    }

    #[tokio::test]
    async fn portal_execution_result_limit_failure_is_retryable_with_the_same_handle() {
        let (temp, engine, database) = engine_with_prepared_catalog(EngineOptions::default());
        let session = engine.session();
        for shard in 0..engine.shard_count() {
            let connection =
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap();
            connection
                .execute_batch("INSERT INTO global_events (code) VALUES (1), (2)")
                .unwrap();
        }
        let statement = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    "SELECT code FROM global_events ORDER BY code",
                ),
            )
            .await
            .unwrap();

        let portal = engine
            .bind_statement(&session, statement, vec![])
            .await
            .unwrap();

        let narrow = RequestContext::new().with_result_limits(ResultLimits::new(1, 1_024).unwrap());
        let error = engine
            .execute_portal_with_context(&session, portal, narrow)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(session.inner.lock().await.prepared().portal_count(), 1);

        let first = engine.execute_portal(&session, portal).await.unwrap();
        let second = engine.execute_portal(&session, portal).await.unwrap();
        assert_eq!(first, second);
        assert!(matches!(
            first.value,
            PreparedExecution::Rows(result)
                if result.rows().len() == 2
                    && result.rows()[0].get(0) == Some(&Value::from(1_i64))
                    && result.rows()[1].get(0) == Some(&Value::from(2_i64))
        ));
    }

    #[tokio::test]
    async fn cancelled_bind_publishes_no_portal_and_normal_bind_recovers() {
        let (_temp, engine, database) = engine_with_prepared_catalog(EngineOptions::default());
        let session = Arc::new(engine.session());
        let statement = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    "SELECT payload FROM events WHERE tenant_id = ?1",
                ),
            )
            .await
            .unwrap();
        let (holder_started_tx, holder_started_rx) = oneshot::channel();
        let (holder_release_tx, holder_release_rx) = mpsc::channel();
        let holder_engine = engine.clone();
        let holder_session = Arc::clone(&session);
        let holder = tokio::spawn(async move {
            holder_engine
                .hold_session_for_test(&holder_session, 0, holder_started_tx, holder_release_rx)
                .await
        });
        holder_started_rx.await.unwrap();

        let cancellation = CancellationToken::new();
        let context = RequestContext::new().with_cancellation_token(cancellation.clone());
        let waiting_engine = engine.clone();
        let waiting_session = Arc::clone(&session);
        let waiting = tokio::spawn(async move {
            waiting_engine
                .bind_statement_with_context(
                    &waiting_session,
                    statement,
                    vec![Value::from(1_i64)],
                    context,
                )
                .await
        });
        timeout(Duration::from_secs(2), async {
            while engine.active_operations_for_test() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancellation.cancel();
        assert_eq!(
            waiting.await.unwrap().unwrap_err().kind(),
            EngineErrorKind::Cancelled
        );
        holder_release_tx.send(()).unwrap();
        holder.await.unwrap().unwrap();
        assert_eq!(session.inner.lock().await.prepared().portal_count(), 0);

        engine
            .bind_statement(&session, statement, vec![Value::from(1_i64)])
            .await
            .unwrap();
        assert_eq!(session.inner.lock().await.prepared().portal_count(), 1);
    }

    #[tokio::test]
    async fn bound_portals_keep_their_routing_snapshot_when_session_context_changes() {
        let (_temp, engine, database) = engine_with_prepared_catalog(EngineOptions::default());
        let session = engine.session();
        session.set_routing_key("7").await.unwrap();
        engine
            .execute(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, payload) VALUES (?1, ?2)",
                    vec![Value::from(7_i64), Value::from("delete-me")],
                ),
            )
            .await
            .unwrap();

        let statement = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    "DELETE FROM events WHERE payload = ?1",
                ),
            )
            .await
            .unwrap();
        let portal = engine
            .bind_statement(&session, statement, vec![Value::from("delete-me")])
            .await
            .unwrap();
        let expected_shard = engine.inner.database.shard_for_key(b"7");
        let different_route = (8_u64..)
            .map(|value| value.to_string())
            .find(|key| engine.inner.database.shard_for_key(key.as_bytes()) != expected_shard)
            .unwrap();
        session
            .set_routing_key(different_route.clone())
            .await
            .unwrap();

        let deleted = engine.execute_portal(&session, portal).await.unwrap();
        assert_eq!(deleted.shard, expected_shard);
        assert_eq!(deleted.value, PreparedExecution::AffectedRows(1));
        session.set_routing_key("7").await.unwrap();
        let remaining = engine
            .query(
                &session,
                Statement::new(
                    "SELECT tenant_id FROM events WHERE tenant_id = ?1",
                    vec![Value::from(7_i64)],
                ),
            )
            .await
            .unwrap();
        assert!(remaining.value.is_empty());

        engine
            .execute(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, payload) VALUES (?1, ?2)",
                    vec![Value::from(7_i64), Value::from("delete-me")],
                ),
            )
            .await
            .unwrap();
        session.set_routing_key(different_route).await.unwrap();
        let deleted = engine
            .execute_portal_logical(&session, portal)
            .await
            .unwrap();
        assert_eq!(deleted.shards, vec![expected_shard]);
        assert_eq!(deleted.value, PreparedExecution::AffectedRows(1));
    }

    #[tokio::test]
    async fn prepared_session_concurrency_cancellation_and_close_are_linearized() {
        let limits = PreparedStatementLimits::new(1, 2, 1_024).unwrap();
        let options = EngineOptions::new(1, 2)
            .unwrap()
            .with_prepared_statement_limits(limits)
            .with_request_timeout(None)
            .unwrap();
        let (_temp, engine, database) = engine_with_prepared_catalog(options);
        let session = Arc::new(engine.session());
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let prepares = ["SELECT 1 FROM events", "SELECT 2 FROM events"]
            .into_iter()
            .map(|source| {
                let engine = engine.clone();
                let session = Arc::clone(&session);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    engine
                        .prepare_statement(
                            &session,
                            PrepareRequest::new(
                                database,
                                sql::SqlDialect::Sqlite,
                                sql::SqlTranslationMode::StrictSqlite,
                                source,
                            ),
                        )
                        .await
                })
            })
            .collect::<Vec<_>>();
        barrier.wait().await;
        let mut prepared = Vec::new();
        let mut failures = Vec::new();
        for task in prepares {
            match task.await.unwrap() {
                Ok(statement) => prepared.push(statement),
                Err(error) => failures.push(error.kind()),
            }
        }
        assert_eq!(prepared.len(), 1);
        assert_eq!(failures, [EngineErrorKind::LimitExceeded]);
        assert!(
            engine
                .close_prepared_statement(&session, prepared[0])
                .await
                .unwrap()
        );

        let (holder_started_tx, holder_started_rx) = oneshot::channel();
        let (holder_release_tx, holder_release_rx) = mpsc::channel();
        let holder_engine = engine.clone();
        let holder_session = Arc::clone(&session);
        let holder = tokio::spawn(async move {
            holder_engine
                .hold_session_for_test(&holder_session, 0, holder_started_tx, holder_release_rx)
                .await
        });
        holder_started_rx.await.unwrap();

        let cancellation = CancellationToken::new();
        let context = RequestContext::new().with_cancellation_token(cancellation.clone());
        let waiting_engine = engine.clone();
        let waiting_session = Arc::clone(&session);
        let waiting = tokio::spawn(async move {
            waiting_engine
                .prepare_statement_with_context(
                    &waiting_session,
                    PrepareRequest::new(
                        database,
                        sql::SqlDialect::Sqlite,
                        sql::SqlTranslationMode::StrictSqlite,
                        "SELECT 3 FROM events",
                    ),
                    context,
                )
                .await
        });
        timeout(Duration::from_secs(2), async {
            while engine.active_operations_for_test() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancellation.cancel();
        assert_eq!(
            waiting.await.unwrap().unwrap_err().kind(),
            EngineErrorKind::Cancelled
        );
        holder_release_tx.send(()).unwrap();
        holder.await.unwrap().unwrap();
        assert_eq!(session.inner.lock().await.prepared().statement_count(), 0);

        let statement = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    "SELECT payload FROM events WHERE tenant_id = ?1",
                ),
            )
            .await
            .unwrap();
        let portal = engine
            .bind_statement(&session, statement, vec![Value::from(1_i64)])
            .await
            .unwrap();
        assert_eq!(session.inner.lock().await.prepared().portal_count(), 1);

        let (close_holder_started_tx, close_holder_started_rx) = oneshot::channel();
        let (close_holder_release_tx, close_holder_release_rx) = mpsc::channel();
        let holder_engine = engine.clone();
        let holder_session = Arc::clone(&session);
        let holder = tokio::spawn(async move {
            holder_engine
                .hold_session_for_test(
                    &holder_session,
                    0,
                    close_holder_started_tx,
                    close_holder_release_rx,
                )
                .await
        });
        close_holder_started_rx.await.unwrap();
        let closing_session = Arc::clone(&session);
        let mut close = tokio::spawn(async move { closing_session.close().await });
        assert!(
            timeout(Duration::from_millis(20), &mut close)
                .await
                .is_err()
        );
        close_holder_release_tx.send(()).unwrap();
        holder.await.unwrap().unwrap();
        close.await.unwrap().unwrap();
        assert_eq!(session.state().await, SessionState::Closed);
        let guard = session.inner.lock().await;
        assert_eq!(guard.prepared().statement_count(), 0);
        assert_eq!(guard.prepared().portal_count(), 0);
        drop(guard);
        assert_eq!(
            engine
                .execute_portal(&session, portal)
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
    }

    #[tokio::test]
    async fn draining_rejects_prepared_work_but_allows_explicit_cleanup() {
        let (_temp, engine, database) = engine_with_prepared_catalog(EngineOptions::default());
        let session = engine.session();
        let request = || {
            PrepareRequest::new(
                database,
                sql::SqlDialect::Sqlite,
                sql::SqlTranslationMode::StrictSqlite,
                "SELECT 1",
            )
        };
        let statement = engine.prepare_statement(&session, request()).await.unwrap();
        let portal = engine
            .bind_statement(&session, statement, vec![])
            .await
            .unwrap();

        assert_eq!(engine.begin_shutdown(), EngineState::Draining);
        assert_eq!(
            engine
                .prepare_statement(&session, request())
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::ShuttingDown
        );
        assert_eq!(
            engine
                .bind_statement(&session, statement, vec![])
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::ShuttingDown
        );
        assert_eq!(
            engine
                .describe_prepared(&session, DescribeTarget::Statement(statement))
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::ShuttingDown
        );
        assert_eq!(
            engine
                .execute_portal(&session, portal)
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::ShuttingDown
        );

        assert!(engine.close_portal(&session, portal).await.unwrap());
        assert!(
            engine
                .close_prepared_statement(&session, statement)
                .await
                .unwrap()
        );
        session.close().await.unwrap();
        engine.shutdown().await.unwrap();
        assert_eq!(engine.state(), EngineState::Stopped);
    }

    #[tokio::test]
    async fn worker_data_corruption_persists_sticky_fail_closed_admission() {
        let (temp, engine) = engine_with_options(2, 1, 1);
        let session = engine.session();
        session.set_routing_key("corrupt-worker").await.unwrap();

        let detected = engine
            .data_corruption_worker_for_test(&session, 0)
            .await
            .unwrap_err();
        assert_eq!(detected.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(detected.to_string(), "injected SQLite data corruption");
        assert_eq!(engine.active_operations_for_test(), 0);
        assert_eq!(
            engine.inner.database.storage.schema_gate_snapshot(),
            crate::storage::SchemaGateSnapshot {
                state: crate::storage::SchemaGateState::Degraded,
                active_operations: 0,
            }
        );

        for error in [
            engine.status(&session).await.unwrap_err(),
            engine
                .execute(
                    &session,
                    Statement::new("CREATE TABLE denied(id INTEGER)", vec![]),
                )
                .await
                .unwrap_err(),
            engine
                .broadcast(
                    &session,
                    "CREATE TABLE denied_everywhere(id INTEGER)".into(),
                )
                .await
                .unwrap_err(),
        ] {
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
            assert!(!error.is_retryable());
        }

        let manifest = rusqlite::Connection::open(temp.path().join("manifest.sqlite")).unwrap();
        let state: i64 = manifest
            .query_row(
                "SELECT database_state FROM briskdb_integrity WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, 4, "runtime corruption must persist Degraded");

        drop(manifest);
        drop(session);
        drop(engine);
        let restart = Database::open(temp.path(), 2).unwrap_err();
        assert_eq!(restart.kind(), EngineErrorKind::DataCorruption);
    }

    #[tokio::test]
    async fn async_open_preserves_shard_validation_before_pool_construction() {
        let temp = tempfile::tempdir().unwrap();

        for requested_shards in [0, 1, 65, u16::MAX] {
            let data_dir = temp.path().join(requested_shards.to_string());
            let error = Engine::open(&data_dir, requested_shards).await.unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
            assert_eq!(error.to_string(), "shard count must be between 2 and 64");
            assert!(!data_dir.exists());
        }
    }

    #[tokio::test]
    async fn broadcast_execute_and_query_share_one_session_and_typed_results() {
        let (_temp, engine) = engine();
        let session = engine.session();
        assert_eq!(
            engine
                .broadcast(
                    &session,
                    "CREATE TABLE widgets (id TEXT PRIMARY KEY, name TEXT NOT NULL)".to_owned(),
                )
                .await
                .unwrap(),
            [0, 1, 2, 3]
        );
        session.set_routing_key("widget-1").await.unwrap();

        let routed_ddl = engine
            .execute(
                &session,
                Statement::new("CREATE TABLE bypassed_migration (id INTEGER)", vec![]),
            )
            .await
            .unwrap_err();
        assert_eq!(routed_ddl.kind(), EngineErrorKind::PermissionDenied);

        let write = engine
            .execute(
                &session,
                Statement::new(
                    "INSERT INTO widgets (id, name) VALUES (?1, ?2)",
                    vec![Value::from("widget-1"), Value::from("First widget")],
                ),
            )
            .await
            .unwrap();
        let read = engine
            .query(
                &session,
                Statement::new(
                    "SELECT id, name FROM widgets WHERE id = ?1",
                    vec![Value::from("widget-1")],
                ),
            )
            .await
            .unwrap();

        assert_eq!(write.shard, read.shard);
        assert_eq!(write.value, 1);
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
        assert_eq!(session.state().await, SessionState::Ready);

        let pools = engine.inner.connections.snapshot().unwrap();
        assert_eq!(pools.pool_size, 4);
        assert_eq!(pools.queue_capacity, 32);
        for shard in &pools.shards {
            assert_eq!(shard.active, 0);
            assert_eq!(shard.queued, 0);
            if shard.shard == write.shard {
                assert_eq!(shard.opened, 1);
                assert_eq!(shard.idle, 1);
                assert_eq!(shard.checkouts, 3);
                assert_eq!(shard.reused, 2);
            } else {
                assert_eq!(shard.opened, 0);
                assert_eq!(shard.idle, 0);
                assert_eq!(shard.checkouts, 0);
                assert_eq!(shard.reused, 0);
            }
            assert_eq!(shard.retired, 0);
        }
    }

    #[tokio::test]
    async fn populated_catalog_gates_async_raw_data_plane_routing() {
        let (_temp, engine, _database) = engine_with_prepared_catalog(EngineOptions::default());
        let session = engine.session();
        let shard_zero_value = integer_key_for_shard(&engine, 0, None);
        let shard_one_value = integer_key_for_shard(&engine, 1, None);
        let shard_zero_key = shard_zero_value.to_string();
        let shard_one_key = shard_one_value.to_string();

        session.set_routing_key(&shard_zero_key).await.unwrap();
        let write = engine
            .execute(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, payload) VALUES (?1, ?2)",
                    vec![Value::from(shard_zero_value), Value::from("owned")],
                ),
            )
            .await
            .unwrap();
        assert_eq!(write.shard, 0);
        assert_eq!(write.value, 1);

        let read = engine
            .query(
                &session,
                Statement::new(
                    "SELECT payload FROM events WHERE tenant_id = ?1",
                    vec![Value::from(shard_zero_value)],
                ),
            )
            .await
            .unwrap();
        assert_eq!(read.shard, 0);
        assert_eq!(read.value.rows()[0].get(0), Some(&Value::from("owned")));

        session.set_routing_key(&shard_one_key).await.unwrap();
        let global = engine
            .query(
                &session,
                Statement::new(
                    "SELECT code FROM global_events WHERE code = ?1",
                    vec![Value::from(7_i64)],
                ),
            )
            .await
            .unwrap();
        assert_eq!(global.shard, 0);

        for (error, expected) in [
            (
                engine
                    .query(
                        &session,
                        Statement::new(
                            "SELECT payload FROM events WHERE tenant_id = ?1",
                            vec![Value::from(shard_zero_value)],
                        ),
                    )
                    .await
                    .unwrap_err(),
                EngineErrorKind::InvalidArgument,
            ),
            (
                engine
                    .query(
                        &session,
                        Statement::new("SELECT payload FROM events", vec![]),
                    )
                    .await
                    .unwrap_err(),
                EngineErrorKind::Unsupported,
            ),
            (
                engine
                    .query(
                        &session,
                        Statement::new(
                            "SELECT payload FROM events
                             WHERE tenant_id = ?1 OR tenant_id = ?2",
                            vec![Value::from(shard_zero_value), Value::from(shard_one_value)],
                        ),
                    )
                    .await
                    .unwrap_err(),
                EngineErrorKind::InvalidArgument,
            ),
            (
                engine
                    .execute(
                        &session,
                        Statement::new(
                            "INSERT INTO global_events (code) VALUES (?1)",
                            vec![Value::from(1_i64)],
                        ),
                    )
                    .await
                    .unwrap_err(),
                EngineErrorKind::Unsupported,
            ),
            (
                engine
                    .query(
                        &session,
                        Statement::new("SELECT code FROM catalog_records", vec![]),
                    )
                    .await
                    .unwrap_err(),
                EngineErrorKind::PermissionDenied,
            ),
            (
                engine
                    .query(
                        &session,
                        Statement::new("SELECT * FROM undeclared_events", vec![]),
                    )
                    .await
                    .unwrap_err(),
                EngineErrorKind::InvalidQuery,
            ),
        ] {
            assert_eq!(error.kind(), expected, "{}", error.diagnostic());
        }
        assert_eq!(session.state().await, SessionState::Ready);
        assert_eq!(engine.active_operations_for_test(), 0);
    }

    #[tokio::test]
    async fn explicit_shard_inspection_reads_only_the_selected_physical_shard() {
        let (_temp, engine) = engine_with_options(2, 2, 2);
        let setup = engine.session();
        engine
            .broadcast(
                &setup,
                "CREATE TABLE inspection_rows (id INTEGER PRIMARY KEY, label TEXT NOT NULL)"
                    .to_owned(),
            )
            .await
            .unwrap();

        let writer = engine.session();
        for shard in 0..engine.shard_count() {
            writer
                .set_routing_key(routing_key_for_shard(&engine, shard))
                .await
                .unwrap();
            let written = engine
                .execute(
                    &writer,
                    Statement::new(
                        "INSERT INTO inspection_rows (id, label) VALUES (?1, ?2)",
                        vec![
                            Value::from(i64::from(shard)),
                            Value::from(format!("shard-{shard}")),
                        ],
                    ),
                )
                .await
                .unwrap();
            assert_eq!(written.shard, shard);
        }

        let inspector = engine.session();
        for shard in 0..engine.shard_count() {
            let result = engine
                .inspect_shard(
                    &inspector,
                    shard,
                    Statement::new("SELECT id, label FROM inspection_rows ORDER BY id", vec![]),
                )
                .await
                .unwrap();
            assert_eq!(
                result,
                ResultSet::new(
                    vec![
                        Column::new("id", DataType::Int64),
                        Column::new("label", DataType::Text),
                    ],
                    vec![Row::new(vec![
                        Value::from(i64::from(shard)),
                        Value::from(format!("shard-{shard}")),
                    ])],
                )
                .unwrap()
            );
        }
        assert_eq!(inspector.routing_key().await, None);
        assert_eq!(inspector.state().await, SessionState::Ready);

        let pools = engine.inner.connections.snapshot().unwrap();
        assert!(
            pools
                .shards
                .iter()
                .all(|shard| shard.active == 0 && shard.queued == 0 && shard.checkouts >= 2)
        );
    }

    #[tokio::test]
    async fn explicit_shard_inspection_rejects_invalid_shards_without_pool_work_and_recovers() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let session = engine.session();

        let (_other_temp, other_engine) = engine_with_options(2, 1, 1);
        let foreign_session = other_engine.session();
        let foreign_error = engine
            .inspect_shard(
                &foreign_session,
                u16::MAX,
                Statement::new("SELECT 1", vec![]),
            )
            .await
            .unwrap_err();
        assert_eq!(foreign_error.kind(), EngineErrorKind::FailedPrecondition);

        for invalid in [engine.shard_count(), u16::MAX] {
            let error = engine
                .inspect_shard(&session, invalid, Statement::new("SELECT 1", vec![]))
                .await
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
            assert_eq!(
                error.to_string(),
                format!(
                    "physical shard {invalid} is outside the configured range 0..{}",
                    engine.shard_count()
                )
            );
        }
        assert!(
            engine
                .inner
                .connections
                .snapshot()
                .unwrap()
                .shards
                .iter()
                .all(|shard| shard.checkouts == 0)
        );

        let recovered = engine
            .inspect_shard(
                &session,
                0,
                Statement::new("SELECT ?1 AS value", vec![Value::from(7_i64)]),
            )
            .await
            .unwrap();
        assert_eq!(recovered.rows()[0].get(0), Some(&Value::from(7_i64)));
        assert_eq!(session.state().await, SessionState::Ready);
    }

    #[tokio::test]
    async fn explicit_shard_inspection_rejects_writes_and_result_overflow_without_poisoning() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let setup = engine.session();
        engine
            .broadcast(
                &setup,
                "CREATE TABLE inspected_write (id INTEGER PRIMARY KEY)".to_owned(),
            )
            .await
            .unwrap();
        let session = engine.session();

        let write_error = engine
            .inspect_shard(
                &session,
                0,
                Statement::new("INSERT INTO inspected_write (id) VALUES (1)", vec![]),
            )
            .await
            .unwrap_err();
        assert_eq!(write_error.kind(), EngineErrorKind::InvalidQuery);

        let narrow = RequestContext::new().with_result_limits(ResultLimits::new(1, 1_024).unwrap());
        let limit_error = engine
            .inspect_shard_with_context(
                &session,
                0,
                Statement::new("SELECT 1 AS value UNION ALL SELECT 2", vec![]),
                narrow,
            )
            .await
            .unwrap_err();
        assert_eq!(limit_error.kind(), EngineErrorKind::LimitExceeded);

        let recovered = engine
            .inspect_shard(
                &session,
                0,
                Statement::new("SELECT COUNT(*) AS count FROM inspected_write", vec![]),
            )
            .await
            .unwrap();
        assert_eq!(recovered.rows()[0].get(0), Some(&Value::from(0_i64)));
        assert_eq!(session.state().await, SessionState::Ready);
        let shard = engine.inner.connections.snapshot().unwrap().shards[0];
        assert_eq!(shard.active, 0);
        assert_eq!(shard.queued, 0);
    }

    #[tokio::test]
    async fn missing_routing_context_and_query_errors_leave_session_reusable() {
        let (_temp, engine) = engine();
        let session = engine.session();

        assert_eq!(
            engine
                .query(&session, Statement::new("SELECT 1", vec![]))
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
        assert_eq!(session.state().await, SessionState::Ready);

        session.set_routing_key("widget-1").await.unwrap();
        let invalid = engine
            .query(
                &session,
                Statement::new("SELECT * FROM missing_table", vec![]),
            )
            .await
            .unwrap_err();
        assert_eq!(invalid.kind(), EngineErrorKind::InvalidQuery);
        assert!(invalid.source().is_some());
        assert_eq!(session.state().await, SessionState::Ready);

        let recovered = engine
            .query(&session, Statement::new("SELECT 42", vec![]))
            .await
            .unwrap();
        assert_eq!(recovered.value.rows()[0].get(0), Some(&Value::from(42_i64)));

        let shard = engine.inner.database.shard_for_key(b"widget-1");
        let snapshot = engine.inner.connections.snapshot().unwrap().shards[usize::from(shard)];
        assert_eq!(snapshot.opened, 1);
        assert_eq!(snapshot.checkouts, 2);
        assert_eq!(snapshot.reused, 1);
        assert_eq!(snapshot.retired, 0);
    }

    #[tokio::test]
    async fn foreign_and_closed_sessions_are_rejected_before_work() {
        let (_first_temp, first) = engine();
        let (_second_temp, second) = engine();
        let foreign = first.session();

        assert_eq!(
            second.status(&foreign).await.unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        foreign.set_routing_key("widget-1").await.unwrap();
        assert_eq!(
            second
                .query(&foreign, Statement::new("SELECT 1", vec![]))
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );

        foreign.close().await.unwrap();
        assert_eq!(
            first.status(&foreign).await.unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(
            first
                .broadcast(&foreign, "SELECT 1".to_owned())
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(
            first
                .execute(&foreign, Statement::new("SELECT 1", vec![]))
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(
            first
                .query(&foreign, Statement::new("SELECT 1", vec![]))
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );

        for engine in [&first, &second] {
            let pools = engine.inner.connections.snapshot().unwrap();
            assert!(pools.shards.iter().all(|shard| {
                shard.active == 0 && shard.queued == 0 && shard.opened == 0 && shard.checkouts == 0
            }));
            assert_eq!(
                engine.inner.workers.available_permits(),
                engine.inner.workers.limit()
            );
        }
    }

    #[tokio::test]
    async fn failed_migration_preflight_is_atomic_and_retryable_through_the_engine() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 4).unwrap());
        database
            .broadcast("CREATE TABLE marker (id INTEGER NOT NULL)")
            .unwrap();
        let shard_one = database.storage.open_shard(1).unwrap();
        shard_one
            .execute_batch("INSERT INTO marker VALUES (1), (1)")
            .unwrap();
        let engine = Engine::from_database(database);
        let session = engine.session();

        let error = engine
            .broadcast(
                &session,
                "CREATE UNIQUE INDEX marker_id ON marker (id)".to_owned(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::UniqueViolation);
        assert_eq!(session.state().await, SessionState::Ready);
        for shard in 0..4 {
            let connection = engine.inner.database.storage.open_shard(shard).unwrap();
            assert!(
                !connection
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'marker_id')",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap()
            );
        }

        shard_one
            .execute("DELETE FROM marker WHERE rowid = 2", [])
            .unwrap();

        assert_eq!(
            engine
                .broadcast(
                    &session,
                    "CREATE UNIQUE INDEX marker_id ON marker (id)".to_owned(),
                )
                .await
                .unwrap(),
            [0, 1, 2, 3]
        );
    }

    #[tokio::test]
    async fn migration_waits_for_in_flight_work_and_rejects_a_second_coordinator() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let holder_session = Arc::new(engine.session());
        let (holder_started_tx, holder_started_rx) = oneshot::channel();
        let (holder_release_tx, holder_release_rx) = mpsc::channel();
        let holder_engine = engine.clone();
        let holder_session_for_task = Arc::clone(&holder_session);
        let holder = tokio::spawn(async move {
            holder_engine
                .hold_session_for_test(
                    &holder_session_for_task,
                    1,
                    holder_started_tx,
                    holder_release_rx,
                )
                .await
        });
        timeout(Duration::from_secs(2), holder_started_rx)
            .await
            .unwrap()
            .unwrap();

        let first_session = Arc::new(engine.session());
        let first_engine = engine.clone();
        let first_session_for_task = Arc::clone(&first_session);
        let first = tokio::spawn(async move {
            first_engine
                .broadcast(
                    &first_session_for_task,
                    "CREATE TABLE IF NOT EXISTS broadcast_marker (id INTEGER)".to_owned(),
                )
                .await
        });
        timeout(Duration::from_secs(2), async {
            while engine.inner.database.storage.schema_gate_snapshot().state
                != crate::storage::SchemaGateState::Migrating
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first migration should own schema admission");

        let second_session = engine.session();
        let second_error = engine
            .broadcast(
                &second_session,
                "CREATE TABLE IF NOT EXISTS broadcast_marker (id INTEGER)".to_owned(),
            )
            .await
            .unwrap_err();
        assert_eq!(second_error.kind(), EngineErrorKind::Busy);
        assert!(second_error.is_retryable());
        wait_for_pool_occupancy(&engine, 1, 1, 0).await;

        holder_release_tx.send(()).unwrap();
        holder.await.unwrap().unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), first)
                .await
                .expect("first ordered broadcast should complete")
                .unwrap()
                .unwrap(),
            [0, 1]
        );
        assert_eq!(
            engine
                .broadcast(
                    &second_session,
                    "CREATE TABLE IF NOT EXISTS broadcast_marker (id INTEGER)".to_owned(),
                )
                .await
                .unwrap(),
            [0, 1]
        );

        for shard in 0..2 {
            wait_for_pool_occupancy(&engine, shard, 0, 0).await;
        }
        assert_eq!(engine.inner.workers.available_permits(), 2);
    }

    #[tokio::test]
    async fn independent_engines_share_schema_exclusion_publication_and_pool_retirement() {
        let temp = tempfile::tempdir().unwrap();
        let options = EngineOptions::new(1, 1).unwrap();
        let first = Engine::from_database_with_options(
            Arc::new(Database::open(temp.path(), 2).unwrap()),
            options,
        )
        .unwrap();
        let second = Engine::from_database_with_options(
            Arc::new(Database::open(temp.path(), 2).unwrap()),
            options,
        )
        .unwrap();
        let holder_session = Arc::new(first.session());
        let original_connection = first
            .connection_id_for_test(&holder_session, 0)
            .await
            .unwrap();

        let (holder_started_tx, holder_started_rx) = oneshot::channel();
        let (holder_release_tx, holder_release_rx) = mpsc::channel();
        let holder_engine = first.clone();
        let held_session = Arc::clone(&holder_session);
        let holder = tokio::spawn(async move {
            holder_engine
                .hold_session_for_test(&held_session, 0, holder_started_tx, holder_release_rx)
                .await
        });
        timeout(Duration::from_secs(2), holder_started_rx)
            .await
            .unwrap()
            .unwrap();

        let migration_session = Arc::new(second.session());
        let migration_engine = second.clone();
        let migration_session_for_task = Arc::clone(&migration_session);
        let migration = tokio::spawn(async move {
            migration_engine
                .broadcast(
                    &migration_session_for_task,
                    "CREATE TABLE shared_schema (id INTEGER PRIMARY KEY)".to_owned(),
                )
                .await
        });
        timeout(Duration::from_secs(2), async {
            while first.inner.database.storage.schema_gate_snapshot().state
                != crate::storage::SchemaGateState::Migrating
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the second engine should exclude work admitted through the first");

        let rejected_session = first.session();
        rejected_session
            .set_routing_key(&routing_key_for_shard(&first, 0))
            .await
            .unwrap();
        let error = first
            .query(&rejected_session, Statement::new("SELECT 1", vec![]))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Busy);

        holder_release_tx.send(()).unwrap();
        holder.await.unwrap().unwrap();
        assert_eq!(migration.await.unwrap().unwrap(), [0, 1]);
        assert_eq!(first.catalog().schema_generation(), 1);
        assert_eq!(second.catalog().schema_generation(), 1);

        let replacement_connection = first
            .connection_id_for_test(&holder_session, 0)
            .await
            .unwrap();
        assert_ne!(replacement_connection, original_connection);
        let pool = first.inner.connections.snapshot().unwrap().shards[0];
        assert_eq!(pool.retired, 1);

        let result = first
            .query(
                &rejected_session,
                Statement::new("SELECT COUNT(*) FROM shared_schema", vec![]),
            )
            .await
            .unwrap();
        assert_eq!(result.value.rows()[0].get(0), Some(&Value::from(0_i64)));
    }

    #[tokio::test]
    async fn aborting_migration_preflight_cleans_up_and_changes_no_shard() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let blocker = engine.inner.database.storage.open_shard(1).unwrap();
        blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();

        let session = Arc::new(engine.session());
        let broadcast_engine = engine.clone();
        let broadcast_session = Arc::clone(&session);
        let broadcast = tokio::spawn(async move {
            broadcast_engine
                .broadcast(
                    &broadcast_session,
                    "CREATE TABLE abort_marker (id INTEGER)".to_owned(),
                )
                .await
        });

        timeout(Duration::from_secs(2), async {
            while engine.inner.database.storage.schema_gate_snapshot().state
                != crate::storage::SchemaGateState::Migrating
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("migration should own schema admission while preflight is blocked");

        broadcast.abort();
        assert!(broadcast.await.unwrap_err().is_cancelled());
        wait_for_pool_occupancy(&engine, 1, 0, 0).await;
        wait_for_worker_capacity(&engine, 2).await;

        let status = timeout(Duration::from_secs(2), engine.status(&session))
            .await
            .expect("detached broadcast should release its session")
            .unwrap();
        assert_eq!(status.shard_count(), 2);
        blocker.execute_batch("COMMIT").unwrap();
        for shard in 0..2 {
            let connection = engine.inner.database.storage.open_shard(shard).unwrap();
            assert!(
                !connection
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'abort_marker')",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap()
            );
        }
        assert_eq!(
            engine
                .broadcast(
                    &session,
                    "CREATE TABLE abort_marker (id INTEGER)".to_owned(),
                )
                .await
                .unwrap(),
            [0, 1]
        );
        for shard in 0..2 {
            wait_for_pool_occupancy(&engine, shard, 0, 0).await;
        }
        assert_eq!(engine.inner.workers.available_permits(), 2);
    }

    #[tokio::test]
    async fn cancellation_after_durable_journal_waits_for_cleanup_and_exact_retry_recovers() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let sql = "CREATE TABLE durable_cancel_marker (id INTEGER)";
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        engine
            .inner
            .database
            .storage
            .install_schema_migration_test_block(
                crate::storage::SchemaMigrationCoordinatorPoint::JournalCommitted,
                started_tx,
                release_rx,
            )
            .unwrap();

        let token = CancellationToken::new();
        let session = Arc::new(engine.session());
        let broadcast_engine = engine.clone();
        let broadcast_session = Arc::clone(&session);
        let context = RequestContext::new().with_cancellation_token(token.clone());
        let mut broadcast = tokio::spawn(async move {
            broadcast_engine
                .broadcast_with_context(&broadcast_session, sql.to_owned(), context)
                .await
        });
        wait_for_blocking_signal(started_rx, "migration journal should become durable").await;
        assert_eq!(engine.catalog().schema_generation(), 0);
        assert_eq!(
            engine.inner.database.storage.schema_gate_snapshot().state,
            crate::storage::SchemaGateState::Migrating
        );

        assert!(token.cancel());
        assert!(
            timeout(Duration::from_millis(20), &mut broadcast)
                .await
                .is_err(),
            "the public future must await blocking-worker cleanup"
        );
        release_tx.send(()).unwrap();
        let error = timeout(Duration::from_secs(2), broadcast)
            .await
            .expect("cancelled migration cleanup should finish")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        assert_eq!(session.state().await, SessionState::Ready);
        assert_eq!(engine.active_operations_for_test(), 0);
        assert_eq!(engine.inner.workers.available_permits(), 2);
        assert_eq!(
            engine.inner.database.storage.schema_gate_snapshot().state,
            crate::storage::SchemaGateState::Pending
        );

        let ordinary = engine.session();
        ordinary.set_routing_key("pending-work").await.unwrap();
        assert_eq!(
            engine
                .query(&ordinary, Statement::new("SELECT 1", vec![]))
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );

        let retry = engine.session();
        assert_eq!(
            engine.broadcast(&retry, sql.to_owned()).await.unwrap(),
            [0, 1]
        );
        assert_eq!(engine.catalog().schema_generation(), 1);
        assert_eq!(
            engine.inner.database.storage.schema_gate_snapshot().state,
            crate::storage::SchemaGateState::Ready
        );
        let recovered = engine
            .query(
                &ordinary,
                Statement::new("SELECT COUNT(*) FROM durable_cancel_marker", vec![]),
            )
            .await
            .unwrap();
        assert_eq!(recovered.value.rows()[0].get(0), Some(&Value::from(0_i64)));
    }

    #[tokio::test]
    async fn connection_local_sql_is_allowed_then_retired_without_session_leakage() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let first = engine.session();
        first.set_routing_key("tenant-state").await.unwrap();
        let shard = engine.inner.database.shard_for_key(b"tenant-state");

        let created = engine
            .execute(
                &first,
                Statement::new("CREATE TEMP TABLE session_temp (id INTEGER)", vec![]),
            )
            .await
            .unwrap();
        assert_eq!(created.value, 0);
        let after_stateful =
            engine.inner.connections.snapshot().unwrap().shards[usize::from(shard)];
        assert_eq!(after_stateful.opened, 1);
        assert_eq!(after_stateful.retired, 1);
        assert_eq!(after_stateful.idle, 0);

        let second = engine.session();
        second.set_routing_key("tenant-state").await.unwrap();
        let temp_objects = engine
            .query(
                &second,
                Statement::new(
                    "SELECT name FROM sqlite_temp_master WHERE name = 'session_temp'",
                    vec![],
                ),
            )
            .await
            .unwrap();
        assert!(temp_objects.value.rows().is_empty());

        let after_replacement =
            engine.inner.connections.snapshot().unwrap().shards[usize::from(shard)];
        assert_eq!(after_replacement.opened, 2);
        assert_eq!(after_replacement.checkouts, 2);
        assert_eq!(after_replacement.retired, 1);
        assert_eq!(after_replacement.idle, 1);
    }

    #[tokio::test]
    async fn pragma_data_version_never_exposes_a_foreign_pooled_handles_history() {
        let (_temp, engine) = engine_with_options(2, 2, 1);
        let shard = 0;
        let schema = engine.session();
        engine
            .broadcast(
                &schema,
                "CREATE TABLE data_version_marker (id INTEGER PRIMARY KEY)".to_owned(),
            )
            .await
            .unwrap();
        let routing_key = (0_u32..10_000)
            .map(|candidate| format!("data-version-{candidate}"))
            .find(|candidate| engine.inner.database.shard_for_key(candidate.as_bytes()) == shard)
            .expect("a deterministic key should route to shard zero");

        let holder_session = Arc::new(engine.session());
        let (holder_started_tx, holder_started_rx) = oneshot::channel();
        let (holder_release_tx, holder_release_rx) = mpsc::channel();
        let holder_engine = engine.clone();
        let holder_session_for_task = Arc::clone(&holder_session);
        let holder = tokio::spawn(async move {
            holder_engine
                .hold_session_for_test(
                    &holder_session_for_task,
                    shard,
                    holder_started_tx,
                    holder_release_rx,
                )
                .await
        });
        timeout(Duration::from_secs(2), holder_started_rx)
            .await
            .unwrap()
            .unwrap();

        let writer = engine.session();
        writer.set_routing_key(routing_key.clone()).await.unwrap();
        engine
            .execute(
                &writer,
                Statement::new("INSERT INTO data_version_marker VALUES (1)", vec![]),
            )
            .await
            .unwrap();

        let fresh_control = engine.inner.database.storage.open_shard(shard).unwrap();
        let fresh_data_version = fresh_control
            .query_row("PRAGMA data_version", [], |row| row.get::<_, i64>(0))
            .unwrap();

        holder_release_tx.send(()).unwrap();
        holder.await.unwrap().unwrap();

        let observer = engine.session();
        observer.set_routing_key(routing_key).await.unwrap();
        engine
            .query(&observer, Statement::new("SELECT 1", vec![]))
            .await
            .unwrap();
        let result = engine
            .query(&observer, Statement::new("PRAGMA data_version", vec![]))
            .await
            .unwrap();
        assert_eq!(
            result.value.rows()[0].get(0),
            Some(&Value::from(fresh_data_version))
        );

        let snapshot = engine.inner.connections.snapshot().unwrap().shards[usize::from(shard)];
        assert_eq!(snapshot.opened, 3);
        assert_eq!(snapshot.checkouts, 4);
        assert_eq!(snapshot.reused, 2);
        assert_eq!(snapshot.retired, 2);
        assert_eq!(snapshot.idle, 1);
    }

    #[tokio::test]
    async fn cross_owner_probe_never_replaces_public_statement_errors() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let routing_key = "probe-error-key";
        let first = engine.session();
        first.set_routing_key(routing_key).await.unwrap();
        engine
            .query(&first, Statement::new("SELECT 1", vec![]))
            .await
            .unwrap();

        let missing_table = engine.session();
        missing_table.set_routing_key(routing_key).await.unwrap();
        assert_eq!(
            engine
                .query(
                    &missing_table,
                    Statement::new("SELECT * FROM missing_table", vec![]),
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidQuery
        );

        let wrong_parameters = engine.session();
        wrong_parameters.set_routing_key(routing_key).await.unwrap();
        assert_eq!(
            engine
                .query(&wrong_parameters, Statement::new("SELECT ?1", vec![]),)
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );

        let multiple_statements = engine.session();
        multiple_statements
            .set_routing_key(routing_key)
            .await
            .unwrap();
        assert_eq!(
            engine
                .query(
                    &multiple_statements,
                    Statement::new("SELECT 1; SELECT 2", vec![]),
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidQuery
        );
    }

    #[tokio::test]
    async fn migration_batches_preflight_then_execute_once_per_shard() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let first = engine.session();
        assert_eq!(
            engine
                .broadcast(&first, "SELECT 1".to_owned())
                .await
                .unwrap(),
            [0, 1]
        );

        let second = engine.session();
        assert_eq!(
            engine
                .broadcast(
                    &second,
                    "CREATE TABLE batch_marker (id INTEGER PRIMARY KEY); \
                     INSERT INTO batch_marker (id) VALUES (1);"
                        .to_owned(),
                )
                .await
                .unwrap(),
            [0, 1]
        );

        for shard in 0..2 {
            let connection = engine.inner.database.storage.open_shard(shard).unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM batch_marker", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                1
            );
        }
    }

    #[tokio::test]
    async fn write_counters_remain_visible_to_the_writer_but_never_cross_sessions() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let schema = engine.session();
        engine
            .broadcast(
                &schema,
                "CREATE TABLE write_state (id INTEGER PRIMARY KEY)".to_owned(),
            )
            .await
            .unwrap();

        let writer = engine.session();
        writer.set_routing_key("write-state-key").await.unwrap();
        let shard = engine
            .execute(
                &writer,
                Statement::new("INSERT INTO write_state (id) VALUES (1)", vec![]),
            )
            .await
            .unwrap()
            .shard;
        let writer_state = engine
            .query(
                &writer,
                Statement::new(
                    "SELECT last_insert_rowid(), changes(), total_changes()",
                    vec![],
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            writer_state.value.rows()[0].values(),
            [Value::from(1_i64), Value::from(1_i64), Value::from(1_i64)]
        );

        let observer = engine.session();
        observer.set_routing_key("write-state-key").await.unwrap();
        let observer_state = engine
            .query(
                &observer,
                Statement::new(
                    "SELECT last_insert_rowid(), changes(), total_changes(), \
                     (SELECT COUNT(*) FROM write_state)",
                    vec![],
                ),
            )
            .await
            .unwrap();
        assert_eq!(observer_state.shard, shard);
        assert_eq!(
            observer_state.value.rows()[0].values(),
            [
                Value::from(0_i64),
                Value::from(0_i64),
                Value::from(0_i64),
                Value::from(1_i64),
            ]
        );

        let snapshot = engine.inner.connections.snapshot().unwrap().shards[usize::from(shard)];
        assert_eq!(snapshot.opened, 2);
        assert_eq!(snapshot.retired, 1);
        assert_eq!(snapshot.idle, 1);
    }

    #[tokio::test]
    async fn exact_per_shard_active_and_queue_limits_return_retryable_busy_and_recover() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let first_session = Arc::new(engine.session());
        let second_session = Arc::new(engine.session());

        let (first_started_tx, first_started_rx) = oneshot::channel();
        let (first_release_tx, first_release_rx) = mpsc::channel();
        let first_engine = engine.clone();
        let first_session_for_task = Arc::clone(&first_session);
        let first = tokio::spawn(async move {
            first_engine
                .hold_session_for_test(
                    &first_session_for_task,
                    0,
                    first_started_tx,
                    first_release_rx,
                )
                .await
        });
        timeout(Duration::from_secs(2), first_started_rx)
            .await
            .unwrap()
            .unwrap();

        let (second_started_tx, mut second_started_rx) = oneshot::channel();
        let (second_release_tx, second_release_rx) = mpsc::channel();
        let second_engine = engine.clone();
        let second_session_for_task = Arc::clone(&second_session);
        let second = tokio::spawn(async move {
            second_engine
                .hold_session_for_test(
                    &second_session_for_task,
                    0,
                    second_started_tx,
                    second_release_rx,
                )
                .await
        });
        wait_for_pool_occupancy(&engine, 0, 1, 1).await;
        assert!(
            timeout(Duration::from_millis(50), &mut second_started_rx)
                .await
                .is_err()
        );

        let overflow_session = engine.session();
        let (overflow_started_tx, overflow_started_rx) = oneshot::channel();
        let (_overflow_release_tx, overflow_release_rx) = mpsc::channel();
        let overflow = timeout(
            Duration::from_secs(2),
            engine.hold_session_for_test(
                &overflow_session,
                0,
                overflow_started_tx,
                overflow_release_rx,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(overflow.kind(), EngineErrorKind::Busy);
        assert!(overflow.is_retryable());
        assert!(overflow_started_rx.await.is_err());

        first_release_tx.send(()).unwrap();
        timeout(Duration::from_secs(2), &mut second_started_rx)
            .await
            .unwrap()
            .unwrap();
        second_release_tx.send(()).unwrap();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        wait_for_pool_occupancy(&engine, 0, 0, 0).await;

        let shard = engine.inner.connections.snapshot().unwrap().shards[0];
        assert_eq!(shard.opened, 1);
        assert_eq!(shard.checkouts, 2);
        assert_eq!(shard.reused, 1);
    }

    #[tokio::test]
    async fn aborting_queued_engine_work_removes_it_before_sqlite_and_restores_capacity() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let first_session = Arc::new(engine.session());
        let queued_session = Arc::new(engine.session());
        let (first_started_tx, first_started_rx) = oneshot::channel();
        let (first_release_tx, first_release_rx) = mpsc::channel();
        let first_engine = engine.clone();
        let first_session_for_task = Arc::clone(&first_session);
        let first = tokio::spawn(async move {
            first_engine
                .hold_session_for_test(
                    &first_session_for_task,
                    0,
                    first_started_tx,
                    first_release_rx,
                )
                .await
        });
        timeout(Duration::from_secs(2), first_started_rx)
            .await
            .unwrap()
            .unwrap();

        let (queued_started_tx, queued_started_rx) = oneshot::channel();
        let (_queued_release_tx, queued_release_rx) = mpsc::channel();
        let queued_engine = engine.clone();
        let queued_session_for_task = Arc::clone(&queued_session);
        let queued = tokio::spawn(async move {
            queued_engine
                .hold_session_for_test(
                    &queued_session_for_task,
                    0,
                    queued_started_tx,
                    queued_release_rx,
                )
                .await
        });
        wait_for_pool_occupancy(&engine, 0, 1, 1).await;
        queued.abort();
        assert!(queued.await.unwrap_err().is_cancelled());
        assert!(queued_started_rx.await.is_err());
        wait_for_pool_occupancy(&engine, 0, 1, 0).await;

        let replacement_session = Arc::new(engine.session());
        let (replacement_started_tx, replacement_started_rx) = oneshot::channel();
        let (replacement_release_tx, replacement_release_rx) = mpsc::channel();
        let replacement_engine = engine.clone();
        let replacement_session_for_task = Arc::clone(&replacement_session);
        let replacement = tokio::spawn(async move {
            replacement_engine
                .hold_session_for_test(
                    &replacement_session_for_task,
                    0,
                    replacement_started_tx,
                    replacement_release_rx,
                )
                .await
        });
        wait_for_pool_occupancy(&engine, 0, 1, 1).await;

        first_release_tx.send(()).unwrap();
        timeout(Duration::from_secs(2), replacement_started_rx)
            .await
            .unwrap()
            .unwrap();
        replacement_release_tx.send(()).unwrap();
        first.await.unwrap().unwrap();
        replacement.await.unwrap().unwrap();

        let shard = engine.inner.connections.snapshot().unwrap().shards[0];
        assert_eq!(shard.active, 0);
        assert_eq!(shard.queued, 0);
        assert_eq!(shard.checkouts, 2);
        assert_eq!(shard.opened, 1);
        assert_eq!(shard.reused, 1);
    }

    #[tokio::test]
    async fn a_hot_shard_queue_does_not_consume_workers_or_capacity_from_another_shard() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let first_session = Arc::new(engine.session());
        let queued_session = Arc::new(engine.session());
        let free_session = Arc::new(engine.session());

        let (first_started_tx, first_started_rx) = oneshot::channel();
        let (first_release_tx, first_release_rx) = mpsc::channel();
        let first_engine = engine.clone();
        let first_session_for_task = Arc::clone(&first_session);
        let first = tokio::spawn(async move {
            first_engine
                .hold_session_for_test(
                    &first_session_for_task,
                    0,
                    first_started_tx,
                    first_release_rx,
                )
                .await
        });
        timeout(Duration::from_secs(2), first_started_rx)
            .await
            .unwrap()
            .unwrap();

        let (queued_started_tx, mut queued_started_rx) = oneshot::channel();
        let (queued_release_tx, queued_release_rx) = mpsc::channel();
        let queued_engine = engine.clone();
        let queued_session_for_task = Arc::clone(&queued_session);
        let queued = tokio::spawn(async move {
            queued_engine
                .hold_session_for_test(
                    &queued_session_for_task,
                    0,
                    queued_started_tx,
                    queued_release_rx,
                )
                .await
        });
        wait_for_pool_occupancy(&engine, 0, 1, 1).await;
        assert_eq!(engine.inner.workers.available_permits(), 1);

        let (free_started_tx, free_started_rx) = oneshot::channel();
        let (free_release_tx, free_release_rx) = mpsc::channel();
        let free_engine = engine.clone();
        let free_session_for_task = Arc::clone(&free_session);
        let free = tokio::spawn(async move {
            free_engine
                .hold_session_for_test(&free_session_for_task, 1, free_started_tx, free_release_rx)
                .await
        });
        timeout(Duration::from_secs(2), free_started_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(engine.inner.workers.available_permits(), 0);
        assert!(
            timeout(Duration::from_millis(50), &mut queued_started_rx)
                .await
                .is_err()
        );

        free_release_tx.send(()).unwrap();
        free.await.unwrap().unwrap();
        first_release_tx.send(()).unwrap();
        timeout(Duration::from_secs(2), &mut queued_started_rx)
            .await
            .unwrap()
            .unwrap();
        queued_release_tx.send(()).unwrap();
        first.await.unwrap().unwrap();
        queued.await.unwrap().unwrap();

        let snapshot = engine.inner.connections.snapshot().unwrap();
        assert_eq!(snapshot.shards[0].opened, 1);
        assert_eq!(snapshot.shards[0].checkouts, 2);
        assert_eq!(snapshot.shards[1].opened, 1);
        assert_eq!(snapshot.shards[1].checkouts, 1);
        assert_eq!(engine.inner.workers.available_permits(), 2);
    }

    #[tokio::test]
    async fn a_worker_panic_is_internal_and_releases_the_session() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let session = engine.session();
        session
            .set_routing_key(routing_key_for_shard(&engine, 0))
            .await
            .unwrap();
        let original_id = engine.connection_id_for_test(&session, 0).await.unwrap();

        let error = engine.panic_worker_for_test(&session, 0).await.unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert_eq!(error.to_string(), "blocking engine task failed");
        assert!(error.source().is_some());
        let after_panic = engine.inner.connections.snapshot().unwrap().shards[0];
        assert_eq!(after_panic.opened, 1);
        assert_eq!(after_panic.checkouts, 2);
        assert_eq!(after_panic.reused, 1);
        assert_eq!(after_panic.retired, 1);
        assert_eq!(after_panic.idle, 0);

        let replacement_id = engine.connection_id_for_test(&session, 0).await.unwrap();
        assert_ne!(replacement_id, original_id);
        let after_replacement = engine.inner.connections.snapshot().unwrap().shards[0];
        assert_eq!(after_replacement.opened, 2);
        assert_eq!(after_replacement.retired, 1);
        assert_eq!(after_replacement.idle, 1);
        assert_eq!(engine.status(&session).await.unwrap().shard_count(), 2);
        assert_eq!(session.state().await, SessionState::Ready);
    }

    #[tokio::test]
    async fn panic_inside_controlled_sql_cleans_hooks_tls_and_retires_the_handle() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let session = engine.session();
        session
            .set_routing_key(routing_key_for_shard(&engine, 0))
            .await
            .unwrap();
        let original_id = engine.connection_id_for_test(&session, 0).await.unwrap();

        let error = engine
            .panic_controlled_worker_for_test(&session, 0)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        let after_panic = engine.inner.connections.snapshot().unwrap().shards[0];
        assert_eq!(after_panic.retired, 1);
        assert_eq!(after_panic.idle, 0);
        assert_eq!(after_panic.active, 0);

        let replacement_id = engine.connection_id_for_test(&session, 0).await.unwrap();
        assert_ne!(replacement_id, original_id);
        let recovered = engine
            .query(&session, Statement::new("SELECT 9", vec![]))
            .await
            .unwrap();
        assert_eq!(recovered.value.rows()[0].get(0), Some(&Value::from(9_i64)));
        assert_eq!(engine.inner.workers.available_permits(), 2);
        assert_eq!(session.state().await, SessionState::Ready);
    }

    #[tokio::test]
    async fn same_session_calls_wait_without_starting_another_blocking_worker() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let session = Arc::new(engine.session());
        let (first_started_tx, first_started_rx) = oneshot::channel();
        let (first_release_tx, first_release_rx) = mpsc::channel();
        let first_engine = engine.clone();
        let first_session = Arc::clone(&session);
        let first = tokio::spawn(async move {
            first_engine
                .hold_session_for_test(&first_session, 0, first_started_tx, first_release_rx)
                .await
        });
        timeout(Duration::from_secs(2), first_started_rx)
            .await
            .unwrap()
            .unwrap();

        let (second_started_tx, mut second_started_rx) = oneshot::channel();
        let (second_release_tx, second_release_rx) = mpsc::channel();
        let second_engine = engine.clone();
        let second_session = Arc::clone(&session);
        let second = tokio::spawn(async move {
            second_engine
                .hold_session_for_test(&second_session, 0, second_started_tx, second_release_rx)
                .await
        });

        assert!(
            timeout(Duration::from_millis(50), &mut second_started_rx)
                .await
                .is_err()
        );
        let waiting = engine.inner.connections.snapshot().unwrap().shards[0];
        assert_eq!(waiting.active, 1);
        assert_eq!(waiting.queued, 0);
        assert_eq!(engine.inner.workers.available_permits(), 1);
        first_release_tx.send(()).unwrap();
        timeout(Duration::from_secs(2), &mut second_started_rx)
            .await
            .unwrap()
            .unwrap();
        second_release_tx.send(()).unwrap();

        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        let completed = engine.inner.connections.snapshot().unwrap().shards[0];
        assert_eq!(completed.checkouts, 2);
        assert_eq!(completed.opened, 1);
        assert_eq!(completed.reused, 1);
    }

    #[tokio::test]
    async fn aborting_an_outer_future_does_not_release_an_in_flight_session() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let session = Arc::new(engine.session());
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker_engine = engine.clone();
        let worker_session = Arc::clone(&session);
        let worker = tokio::spawn(async move {
            worker_engine
                .hold_session_for_test(&worker_session, 0, started_tx, release_rx)
                .await
        });
        timeout(Duration::from_secs(2), started_rx)
            .await
            .unwrap()
            .unwrap();
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        let detached = engine.inner.connections.snapshot().unwrap().shards[0];
        assert_eq!(detached.active, 1);
        assert_eq!(detached.queued, 0);
        assert_eq!(engine.inner.workers.available_permits(), 1);

        let status_engine = engine.clone();
        let status_session = Arc::clone(&session);
        let mut status = tokio::spawn(async move { status_engine.status(&status_session).await });
        assert!(
            timeout(Duration::from_millis(50), &mut status)
                .await
                .is_err()
        );

        release_tx.send(()).unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), status)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .shard_count(),
            2
        );
        wait_for_pool_occupancy(&engine, 0, 0, 0).await;
        wait_for_worker_capacity(&engine, 2).await;
    }

    #[tokio::test]
    async fn independent_sessions_can_enter_blocking_workers_concurrently() {
        let (_temp, engine) = engine();
        let first_session = Arc::new(engine.session());
        let second_session = Arc::new(engine.session());
        let (first_started_tx, first_started_rx) = oneshot::channel();
        let (second_started_tx, second_started_rx) = oneshot::channel();
        let (first_release_tx, first_release_rx) = mpsc::channel();
        let (second_release_tx, second_release_rx) = mpsc::channel();

        let first_engine = engine.clone();
        let first_session_for_task = Arc::clone(&first_session);
        let first = tokio::spawn(async move {
            first_engine
                .hold_session_for_test(
                    &first_session_for_task,
                    0,
                    first_started_tx,
                    first_release_rx,
                )
                .await
        });
        let second_engine = engine.clone();
        let second_session_for_task = Arc::clone(&second_session);
        let second = tokio::spawn(async move {
            second_engine
                .hold_session_for_test(
                    &second_session_for_task,
                    0,
                    second_started_tx,
                    second_release_rx,
                )
                .await
        });

        timeout(Duration::from_secs(2), first_started_rx)
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(2), second_started_rx)
            .await
            .unwrap()
            .unwrap();
        first_release_tx.send(()).unwrap();
        second_release_tx.send(()).unwrap();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn independent_sessions_keep_routing_and_values_isolated() {
        let (_temp, engine) = engine();
        let first = Arc::new(engine.session());
        let second = Arc::new(engine.session());
        first.set_routing_key("tenant-a").await.unwrap();
        second.set_routing_key("tenant-b").await.unwrap();

        let first_engine = engine.clone();
        let first_session = Arc::clone(&first);
        let first_query = tokio::spawn(async move {
            first_engine
                .query(
                    &first_session,
                    Statement::new("SELECT ?1", vec![Value::from("first")]),
                )
                .await
        });
        let second_engine = engine.clone();
        let second_session = Arc::clone(&second);
        let second_query = tokio::spawn(async move {
            second_engine
                .query(
                    &second_session,
                    Statement::new("SELECT ?1", vec![Value::from("second")]),
                )
                .await
        });

        let first_result = first_query.await.unwrap().unwrap();
        let second_result = second_query.await.unwrap().unwrap();
        assert_eq!(
            first_result.value.rows()[0].get(0),
            Some(&Value::from("first"))
        );
        assert_eq!(
            second_result.value.rows()[0].get(0),
            Some(&Value::from("second"))
        );
        assert_eq!(first.routing_key().await.as_deref(), Some("tenant-a"));
        assert_eq!(second.routing_key().await.as_deref(), Some("tenant-b"));
    }

    #[tokio::test]
    async fn explicit_cancellation_interrupts_running_sql_and_retires_only_that_handle() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap();
        let (_temp, engine) = engine_with_engine_options(2, options);
        let session = Arc::new(engine.session());
        session.set_routing_key("cancel-query").await.unwrap();
        let shard = engine.inner.database.shard_for_key(b"cancel-query");
        let original_id = engine
            .connection_id_for_test(&session, shard)
            .await
            .unwrap();
        let token = CancellationToken::new();
        let context = RequestContext::new().with_cancellation_token(token.clone());
        let query_engine = engine.clone();
        let query_session = Arc::clone(&session);
        let query = tokio::spawn(async move {
            query_engine
                .query_with_context(
                    &query_session,
                    Statement::new(
                        "WITH RECURSIVE numbers(value) AS (\
                         VALUES(0) UNION ALL SELECT value + 1 FROM numbers \
                         WHERE value < 1000000000) SELECT sum(value) FROM numbers",
                        vec![],
                    ),
                    context,
                )
                .await
        });
        wait_for_pool_occupancy(&engine, shard, 1, 0).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(token.cancel());

        let error = timeout(Duration::from_secs(2), query)
            .await
            .expect("SQLite interrupt should bound cancellation cleanup")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        wait_for_pool_occupancy(&engine, shard, 0, 0).await;

        let replacement_id = engine
            .connection_id_for_test(&session, shard)
            .await
            .unwrap();
        assert_ne!(replacement_id, original_id);
        let recovered = engine
            .query(&session, Statement::new("SELECT 42", vec![]))
            .await
            .unwrap();
        assert_eq!(recovered.value.rows()[0].get(0), Some(&Value::from(42_i64)));
        let snapshot = engine.inner.connections.snapshot().unwrap().shards[usize::from(shard)];
        assert_eq!(snapshot.retired, 1);
        assert_eq!(snapshot.active, 0);
        assert_eq!(snapshot.queued, 0);
    }

    #[tokio::test]
    async fn request_deadline_interrupts_running_sql_with_its_distinct_kind() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap();
        let (_temp, engine) = engine_with_engine_options(2, options);
        let session = engine.session();
        session.set_routing_key("deadline-query").await.unwrap();
        let context = RequestContext::new()
            .with_timeout(Duration::from_millis(10))
            .unwrap();

        let error = timeout(
            Duration::from_secs(2),
            engine.query_with_context(
                &session,
                Statement::new(
                    "WITH RECURSIVE numbers(value) AS (\
                     VALUES(0) UNION ALL SELECT value + 1 FROM numbers \
                     WHERE value < 1000000000) SELECT sum(value) FROM numbers",
                    vec![],
                ),
                context,
            ),
        )
        .await
        .expect("deadline should interrupt SQLite")
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);

        let recovered = engine
            .query(&session, Statement::new("SELECT 7", vec![]))
            .await
            .unwrap();
        assert_eq!(recovered.value.rows()[0].get(0), Some(&Value::from(7_i64)));
    }

    #[tokio::test]
    async fn cancelling_atomic_insert_select_returns_only_after_rollback() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap();
        let (_temp, engine) = engine_with_engine_options(2, options);
        let setup = engine.session();
        engine
            .broadcast(
                &setup,
                "CREATE TABLE cancellation_rows (id INTEGER PRIMARY KEY)".to_owned(),
            )
            .await
            .unwrap();

        let session = Arc::new(engine.session());
        session.set_routing_key("cancel-write").await.unwrap();
        let shard = engine.inner.database.shard_for_key(b"cancel-write");
        let token = CancellationToken::new();
        let context = RequestContext::new().with_cancellation_token(token.clone());
        let write_engine = engine.clone();
        let write_session = Arc::clone(&session);
        let write = tokio::spawn(async move {
            write_engine
                .execute_with_context(
                    &write_session,
                    Statement::new(
                        "WITH RECURSIVE numbers(value) AS (\
                         VALUES(1) UNION ALL SELECT value + 1 FROM numbers \
                         WHERE value < 100000000) \
                         INSERT INTO cancellation_rows SELECT value FROM numbers",
                        vec![],
                    ),
                    context,
                )
                .await
        });
        wait_for_pool_occupancy(&engine, shard, 1, 0).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        token.cancel();
        let error = timeout(Duration::from_secs(2), write)
            .await
            .expect("cancelled write should finish rollback")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);

        for _ in 0..2 {
            let count = engine
                .query(
                    &session,
                    Statement::new("SELECT COUNT(*) FROM cancellation_rows", vec![]),
                )
                .await
                .unwrap();
            assert_eq!(count.value.rows()[0].get(0), Some(&Value::from(0_i64)));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn aborting_atomic_write_future_rolls_back_before_resources_are_reused() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap();
        let (_temp, engine) = engine_with_engine_options(2, options);
        let setup = engine.session();
        engine
            .broadcast(
                &setup,
                "CREATE TABLE aborted_rows (id INTEGER PRIMARY KEY)".to_owned(),
            )
            .await
            .unwrap();

        let session = Arc::new(engine.session());
        session.set_routing_key("abort-write").await.unwrap();
        let shard = engine.inner.database.shard_for_key(b"abort-write");
        let write_engine = engine.clone();
        let write_session = Arc::clone(&session);
        let write = tokio::spawn(async move {
            write_engine
                .execute(
                    &write_session,
                    Statement::new(
                        "WITH RECURSIVE numbers(value) AS (\
                         VALUES(1) UNION ALL SELECT value + 1 FROM numbers \
                         WHERE value < 100000000) \
                         INSERT INTO aborted_rows SELECT value FROM numbers",
                        vec![],
                    ),
                )
                .await
        });
        wait_for_pool_occupancy(&engine, shard, 1, 0).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        write.abort();
        assert!(write.await.unwrap_err().is_cancelled());

        timeout(
            Duration::from_secs(2),
            engine.inner.lifecycle.wait_for_drain(),
        )
        .await
        .expect("the detached write should finish rollback");
        wait_for_pool_occupancy(&engine, shard, 0, 0).await;
        wait_for_worker_capacity(&engine, 2).await;
        assert_eq!(engine.inner.lifecycle.active(), 0);

        for _ in 0..2 {
            let count = engine
                .query(
                    &session,
                    Statement::new("SELECT COUNT(*) FROM aborted_rows", vec![]),
                )
                .await
                .unwrap();
            assert_eq!(count.value.rows()[0].get(0), Some(&Value::from(0_i64)));
        }
        assert_eq!(session.state().await, SessionState::Ready);
    }

    #[tokio::test]
    async fn queued_token_cancellation_removes_admission_without_starting_sql() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let holder_session = Arc::new(engine.session());
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let holder_engine = engine.clone();
        let holder_session_for_task = Arc::clone(&holder_session);
        let holder = tokio::spawn(async move {
            holder_engine
                .hold_session_for_test(&holder_session_for_task, 0, started_tx, release_rx)
                .await
        });
        timeout(Duration::from_secs(2), started_rx)
            .await
            .unwrap()
            .unwrap();

        let queued_session = Arc::new(engine.session());
        queued_session
            .set_routing_key(routing_key_for_shard(&engine, 0))
            .await
            .unwrap();
        let token = CancellationToken::new();
        let context = RequestContext::new().with_cancellation_token(token.clone());
        let queued_engine = engine.clone();
        let queued_session_for_task = Arc::clone(&queued_session);
        let queued = tokio::spawn(async move {
            queued_engine
                .execute_with_context(
                    &queued_session_for_task,
                    Statement::new("CREATE TABLE must_not_start (id INTEGER)", vec![]),
                    context,
                )
                .await
        });
        wait_for_pool_occupancy(&engine, 0, 1, 1).await;
        token.cancel();
        let error = timeout(Duration::from_secs(1), queued)
            .await
            .expect("queued cancellation must not wait for a connection")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        wait_for_pool_occupancy(&engine, 0, 1, 0).await;

        release_tx.send(()).unwrap();
        holder.await.unwrap().unwrap();
        let shard = engine.inner.database.storage.open_shard(0).unwrap();
        assert!(
            !shard
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'must_not_start')",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        );
    }

    #[tokio::test]
    async fn queued_deadline_expires_without_waiting_for_a_connection_or_running_sql() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap();
        let (_temp, engine) = engine_with_engine_options(2, options);
        let holder_session = Arc::new(engine.session());
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let holder_engine = engine.clone();
        let holder_session_for_task = Arc::clone(&holder_session);
        let holder = tokio::spawn(async move {
            holder_engine
                .hold_session_for_test(&holder_session_for_task, 0, started_tx, release_rx)
                .await
        });
        timeout(Duration::from_secs(2), started_rx)
            .await
            .unwrap()
            .unwrap();

        let queued = engine.session();
        queued
            .set_routing_key(routing_key_for_shard(&engine, 0))
            .await
            .unwrap();
        let context = RequestContext::new()
            .with_timeout(Duration::from_millis(10))
            .unwrap();
        let error = timeout(
            Duration::from_secs(1),
            engine.execute_with_context(
                &queued,
                Statement::new("CREATE TABLE deadline_must_not_start (id INTEGER)", vec![]),
                context,
            ),
        )
        .await
        .expect("queued deadline must expire independently of the held connection")
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
        wait_for_pool_occupancy(&engine, 0, 1, 0).await;

        release_tx.send(()).unwrap();
        holder.await.unwrap().unwrap();
        let shard = engine.inner.database.storage.open_shard(0).unwrap();
        assert!(
            !shard
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = \
                 'deadline_must_not_start')",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        );
    }

    #[tokio::test]
    async fn late_cancellation_cannot_interrupt_a_reused_connection() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let session = engine.session();
        session.set_routing_key("late-cancel").await.unwrap();
        let shard = engine.inner.database.shard_for_key(b"late-cancel");
        let token = CancellationToken::new();
        let first = engine
            .query_with_context(
                &session,
                Statement::new("SELECT 1", vec![]),
                RequestContext::new().with_cancellation_token(token.clone()),
            )
            .await
            .unwrap();
        assert_eq!(first.value.rows()[0].get(0), Some(&Value::from(1_i64)));
        let first_id = engine
            .connection_id_for_test(&session, shard)
            .await
            .unwrap();

        token.cancel();
        let second = engine
            .query(&session, Statement::new("SELECT 2", vec![]))
            .await
            .unwrap();
        assert_eq!(second.value.rows()[0].get(0), Some(&Value::from(2_i64)));
        let second_id = engine
            .connection_id_for_test(&session, shard)
            .await
            .unwrap();
        assert_eq!(second_id, first_id);
        assert_eq!(
            engine.inner.connections.snapshot().unwrap().shards[usize::from(shard)].retired,
            0
        );
    }

    #[tokio::test]
    async fn cancellation_during_hook_teardown_retires_before_immediate_reuse() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap();
        let (_temp, engine) = engine_with_engine_options(2, options);
        let session = Arc::new(engine.session());
        session.set_routing_key("teardown-race").await.unwrap();
        let shard = engine.inner.database.shard_for_key(b"teardown-race");
        let original_id = engine
            .connection_id_for_test(&session, shard)
            .await
            .unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (_release_tx, release_rx) = mpsc::channel();
        engine
            .inner
            .connections
            .block_next_control_teardown(shard, started_tx, release_rx)
            .unwrap();
        let token = CancellationToken::new();
        let request_engine = engine.clone();
        let request_session = Arc::clone(&session);
        let request_token = token.clone();
        let request = tokio::spawn(async move {
            request_engine
                .query_with_context(
                    &request_session,
                    Statement::new("SELECT 1", vec![]),
                    RequestContext::new().with_cancellation_token(request_token),
                )
                .await
        });
        wait_for_blocking_signal(started_rx, "hook teardown should reach its race barrier").await;
        assert!(token.cancel());
        let completed = timeout(Duration::from_secs(1), request)
            .await
            .expect("teardown cancellation should finish promptly")
            .unwrap()
            .unwrap();
        assert_eq!(completed.value.rows()[0].get(0), Some(&Value::from(1_i64)));

        let replacement_id = engine
            .connection_id_for_test(&session, shard)
            .await
            .unwrap();
        assert_ne!(replacement_id, original_id);
        let recovered = engine
            .query(&session, Statement::new("SELECT 2", vec![]))
            .await
            .unwrap();
        assert_eq!(recovered.value.rows()[0].get(0), Some(&Value::from(2_i64)));
        let snapshot = engine.inner.connections.snapshot().unwrap().shards[usize::from(shard)];
        assert_eq!(snapshot.retired, 1);
        assert_eq!(snapshot.active, 0);
    }

    #[tokio::test]
    async fn deadline_interrupts_lazy_connection_setup_and_capacity_recovers() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap();
        let (_temp, engine) = engine_with_engine_options(2, options);
        let session = Arc::new(engine.session());
        let key = routing_key_for_shard(&engine, 0);
        session.set_routing_key(key).await.unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (_release_tx, release_rx) = mpsc::channel();
        engine
            .inner
            .connections
            .block_next_connection_setup(0, started_tx, release_rx)
            .unwrap();

        let request_engine = engine.clone();
        let request_session = Arc::clone(&session);
        let request = tokio::spawn(async move {
            request_engine
                .query_with_context(
                    &request_session,
                    Statement::new("SELECT 1", vec![]),
                    RequestContext::new()
                        .with_timeout(Duration::from_millis(20))
                        .unwrap(),
                )
                .await
        });
        wait_for_blocking_signal(started_rx, "connection setup should become active").await;
        let error = timeout(Duration::from_secs(1), request)
            .await
            .expect("the deadline should interrupt connection setup")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
        assert_eq!(engine.inner.lifecycle.active(), 0);
        assert_eq!(engine.inner.workers.available_permits(), 2);
        let failed = engine.inner.connections.snapshot().unwrap().shards[0];
        assert_eq!(failed.active, 0);
        assert_eq!(failed.opened, 0);

        let recovered = engine
            .query(&session, Statement::new("SELECT 2", vec![]))
            .await
            .unwrap();
        assert_eq!(recovered.value.rows()[0].get(0), Some(&Value::from(2_i64)));
    }

    #[tokio::test]
    async fn deadline_interrupts_a_real_lock_during_lazy_connection_configuration() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap();
        let (_temp, engine) = engine_with_engine_options(2, options);
        let blocker = engine.inner.database.storage.open_shard(0).unwrap();
        blocker
            .execute_batch("PRAGMA locking_mode = EXCLUSIVE; BEGIN EXCLUSIVE")
            .unwrap();
        let session = engine.session();
        let context = RequestContext::new()
            .with_timeout(Duration::from_millis(20))
            .unwrap();

        let started = std::time::Instant::now();
        let error = timeout(
            Duration::from_secs(1),
            engine.connection_id_with_context_for_test(&session, 0, context),
        )
        .await
        .expect("the request deadline must preempt SQLite's normal five-second busy wait")
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(engine.inner.lifecycle.active(), 0);
        wait_for_worker_capacity(&engine, 2).await;

        blocker.execute_batch("COMMIT").unwrap();
        drop(blocker);
        assert!(engine.connection_id_for_test(&session, 0).await.unwrap() > 0);
    }

    #[tokio::test]
    async fn cancellation_interrupts_foreign_owner_probe_and_retires_the_handle() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap();
        let (_temp, engine) = engine_with_engine_options(2, options);
        let key = routing_key_for_shard(&engine, 0);
        let first = engine.session();
        first.set_routing_key(key.clone()).await.unwrap();
        engine
            .query(&first, Statement::new("SELECT 1", vec![]))
            .await
            .unwrap();

        let second = Arc::new(engine.session());
        second.set_routing_key(key).await.unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (_release_tx, release_rx) = mpsc::channel();
        engine
            .inner
            .connections
            .block_next_foreign_probe(0, started_tx, release_rx)
            .unwrap();
        let token = CancellationToken::new();
        let request_engine = engine.clone();
        let request_session = Arc::clone(&second);
        let request_token = token.clone();
        let request = tokio::spawn(async move {
            request_engine
                .query_with_context(
                    &request_session,
                    Statement::new("SELECT 2", vec![]),
                    RequestContext::new().with_cancellation_token(request_token),
                )
                .await
        });
        wait_for_blocking_signal(started_rx, "foreign-owner probe should become active").await;
        assert!(token.cancel());
        let error = timeout(Duration::from_secs(1), request)
            .await
            .expect("cancellation should interrupt the foreign-owner probe")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        let cancelled = engine.inner.connections.snapshot().unwrap().shards[0];
        assert_eq!(cancelled.active, 0);
        assert_eq!(cancelled.retired, 1);
        assert_eq!(engine.inner.workers.available_permits(), 2);

        let recovered = engine
            .query(&second, Statement::new("SELECT 3", vec![]))
            .await
            .unwrap();
        assert_eq!(recovered.value.rows()[0].get(0), Some(&Value::from(3_i64)));
    }

    #[tokio::test]
    async fn per_request_result_limits_can_narrow_but_never_widen_engine_limits() {
        let engine_limits = ResultLimits::new(2, 1_024).unwrap();
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_result_limits(engine_limits);
        let (_temp, engine) = engine_with_engine_options(2, options);
        let session = engine.session();
        session.set_routing_key("result-budget").await.unwrap();

        let wider =
            RequestContext::new().with_result_limits(ResultLimits::new(100, 1_000_000).unwrap());
        let error = engine
            .query_with_context(
                &session,
                Statement::new("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3", vec![]),
                wider,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);

        let narrower =
            RequestContext::new().with_result_limits(ResultLimits::new(1, 1_024).unwrap());
        let error = engine
            .query_with_context(
                &session,
                Statement::new("SELECT 1 UNION ALL SELECT 2", vec![]),
                narrower,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        let recovered = engine
            .query(&session, Statement::new("SELECT 1", vec![]))
            .await
            .unwrap();
        assert_eq!(recovered.value.rows().len(), 1);

        let configured_bytes = ResultLimits::new(100, 50).unwrap();
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_result_limits(configured_bytes);
        let (_byte_temp, byte_engine) = engine_with_engine_options(2, options);
        let byte_session = byte_engine.session();
        byte_session
            .set_routing_key("configured-bytes")
            .await
            .unwrap();
        let wider_bytes =
            RequestContext::new().with_result_limits(ResultLimits::new(100, 100).unwrap());
        let error = byte_engine
            .query_with_context(
                &byte_session,
                Statement::new("SELECT 1 AS v", vec![]),
                wider_bytes,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);

        let request_options = EngineOptions::new(1, 1)
            .unwrap()
            .with_result_limits(ResultLimits::new(100, 100).unwrap());
        let (_request_temp, request_engine) = engine_with_engine_options(2, request_options);
        let request_session = request_engine.session();
        request_session
            .set_routing_key("request-bytes")
            .await
            .unwrap();
        let narrower_bytes =
            RequestContext::new().with_result_limits(ResultLimits::new(100, 50).unwrap());
        let error = request_engine
            .query_with_context(
                &request_session,
                Statement::new("SELECT 1 AS v", vec![]),
                narrower_bytes,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);

        let exact = RequestContext::new().with_result_limits(ResultLimits::new(100, 51).unwrap());
        let exact_result = request_engine
            .query_with_context(
                &request_session,
                Statement::new("SELECT 1 AS v", vec![]),
                exact,
            )
            .await
            .unwrap();
        assert_eq!(
            exact_result.value.rows()[0].get(0),
            Some(&Value::from(1_i64))
        );
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_admitted_work_rejects_new_work_and_is_idempotent() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let session = Arc::new(engine.session());
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let active_engine = engine.clone();
        let active_session = Arc::clone(&session);
        let active = tokio::spawn(async move {
            active_engine
                .hold_session_for_test(&active_session, 0, started_tx, release_rx)
                .await
        });
        timeout(Duration::from_secs(2), started_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(engine.begin_shutdown(), EngineState::Draining);
        assert_eq!(engine.state(), EngineState::Draining);
        let rejected = engine.status(&engine.session()).await.unwrap_err();
        assert_eq!(rejected.kind(), EngineErrorKind::ShuttingDown);

        let shutdown_engine = engine.clone();
        let mut shutdown = tokio::spawn(async move { shutdown_engine.shutdown().await });
        assert!(
            timeout(Duration::from_millis(20), &mut shutdown)
                .await
                .is_err()
        );
        release_tx.send(()).unwrap();
        active.await.unwrap().unwrap();
        let report = shutdown.await.unwrap().unwrap();
        assert!(!report.forced());
        assert_eq!(engine.state(), EngineState::Stopped);
        assert_eq!(engine.shutdown().await.unwrap(), report);
        let snapshot = engine.inner.connections.snapshot().unwrap();
        assert!(snapshot.shards.iter().all(|shard| shard.idle == 0));
    }

    #[tokio::test]
    async fn shutdown_drains_work_already_waiting_on_the_same_session() {
        let (_temp, engine) = engine_with_options(2, 1, 1);
        let session = Arc::new(engine.session());
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let first_engine = engine.clone();
        let first_session = Arc::clone(&session);
        let first = tokio::spawn(async move {
            first_engine
                .hold_session_for_test(&first_session, 0, started_tx, release_rx)
                .await
        });
        timeout(Duration::from_secs(2), started_rx)
            .await
            .unwrap()
            .unwrap();

        let second_engine = engine.clone();
        let second_session = Arc::clone(&session);
        let second = tokio::spawn(async move { second_engine.status(&second_session).await });
        timeout(Duration::from_secs(2), async {
            while engine.inner.lifecycle.active() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        engine.begin_shutdown();
        let shutdown_engine = engine.clone();
        let shutdown = tokio::spawn(async move { shutdown_engine.shutdown().await });

        release_tx.send(()).unwrap();
        first.await.unwrap().unwrap();
        assert_eq!(second.await.unwrap().unwrap().shard_count(), 2);
        assert!(!shutdown.await.unwrap().unwrap().forced());
        assert_eq!(engine.state(), EngineState::Stopped);
    }

    #[tokio::test]
    async fn shutdown_force_cancels_running_sql_then_waits_for_cleanup() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap()
            .with_shutdown_grace(Duration::from_millis(10))
            .unwrap();
        let (_temp, engine) = engine_with_engine_options(2, options);
        let session = Arc::new(engine.session());
        session.set_routing_key("shutdown-cancel").await.unwrap();
        let shard = engine.inner.database.shard_for_key(b"shutdown-cancel");
        let query_engine = engine.clone();
        let query_session = Arc::clone(&session);
        let query = tokio::spawn(async move {
            query_engine
                .query(
                    &query_session,
                    Statement::new(
                        "WITH RECURSIVE numbers(value) AS (\
                         VALUES(0) UNION ALL SELECT value + 1 FROM numbers \
                         WHERE value < 1000000000) SELECT sum(value) FROM numbers",
                        vec![],
                    ),
                )
                .await
        });
        wait_for_pool_occupancy(&engine, shard, 1, 0).await;

        let report = timeout(Duration::from_secs(2), engine.shutdown())
            .await
            .expect("forced shutdown should interrupt SQLite")
            .unwrap();
        assert!(report.forced());
        assert_eq!(engine.state(), EngineState::Stopped);
        let error = query.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        assert_eq!(engine.inner.lifecycle.active(), 0);
        assert_eq!(engine.inner.workers.available_permits(), 2);
    }

    #[tokio::test]
    async fn dropped_started_request_is_counted_until_cleanup_and_shutdown_can_resume() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap()
            .with_shutdown_grace(Duration::from_millis(10))
            .unwrap();
        let (_temp, engine) = engine_with_engine_options(2, options);
        let session = Arc::new(engine.session());
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let task_engine = engine.clone();
        let task_session = Arc::clone(&session);
        let task = tokio::spawn(async move {
            task_engine
                .hold_session_for_test(&task_session, 0, started_tx, release_rx)
                .await
        });
        timeout(Duration::from_secs(2), started_rx)
            .await
            .unwrap()
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(engine.inner.lifecycle.active(), 1);

        let error = engine.shutdown().await.unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
        assert_eq!(engine.state(), EngineState::Draining);
        assert_eq!(engine.inner.lifecycle.active(), 1);

        release_tx.send(()).unwrap();
        timeout(
            Duration::from_secs(2),
            engine.inner.lifecycle.wait_for_drain(),
        )
        .await
        .unwrap();
        let report = engine.shutdown().await.unwrap();
        assert!(report.forced());
        assert_eq!(engine.state(), EngineState::Stopped);
    }

    #[tokio::test]
    async fn cancelling_one_shutdown_waiter_does_not_strand_later_shutdown() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap()
            .with_shutdown_grace(Duration::from_secs(1))
            .unwrap();
        let (_temp, engine) = engine_with_engine_options(2, options);
        let session = Arc::new(engine.session());
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let active_engine = engine.clone();
        let active_session = Arc::clone(&session);
        let active = tokio::spawn(async move {
            active_engine
                .hold_session_for_test(&active_session, 0, started_tx, release_rx)
                .await
        });
        timeout(Duration::from_secs(2), started_rx)
            .await
            .unwrap()
            .unwrap();

        let first_engine = engine.clone();
        let first = tokio::spawn(async move { first_engine.shutdown().await });
        timeout(Duration::from_secs(1), async {
            while engine.state() != EngineState::Draining {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        release_tx.send(()).unwrap();
        active.await.unwrap().unwrap();
        let report = engine.shutdown().await.unwrap();
        assert!(!report.forced());
        assert_eq!(engine.state(), EngineState::Stopped);
    }

    #[tokio::test]
    async fn cancelled_finalizer_keeps_shutdown_gate_until_handles_are_closed() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap()
            .with_shutdown_grace(Duration::from_secs(1))
            .unwrap();
        let (_temp, engine) = engine_with_engine_options(2, options);
        let session = engine.session();
        session
            .set_routing_key("open-an-idle-handle")
            .await
            .unwrap();
        engine
            .query(&session, Statement::new("SELECT 1", vec![]))
            .await
            .unwrap();

        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        engine
            .inner
            .connections
            .block_next_close_idle(started_tx, release_rx);

        let first_engine = engine.clone();
        let first = tokio::spawn(async move { first_engine.shutdown().await });
        wait_for_blocking_signal(started_rx, "the first shutdown finalizer should start").await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        let second_engine = engine.clone();
        let mut second = tokio::spawn(async move { second_engine.shutdown().await });
        assert!(
            timeout(Duration::from_millis(20), &mut second)
                .await
                .is_err(),
            "a later shutdown must wait for the detached finalizer"
        );
        assert_eq!(engine.state(), EngineState::Draining);

        release_tx.send(()).unwrap();
        let report = timeout(Duration::from_secs(2), second)
            .await
            .expect("the later shutdown should observe finalization")
            .unwrap()
            .unwrap();
        assert!(!report.forced());
        assert_eq!(engine.state(), EngineState::Stopped);
        let snapshot = engine.inner.connections.snapshot().unwrap();
        assert!(snapshot.shards.iter().all(|shard| shard.idle == 0));
    }

    #[tokio::test]
    async fn logical_scatter_starts_no_more_than_eight_physical_reads_at_once() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap();
        let (_temp, engine) = engine_with_sharded_events(9, options);
        let mut started = Vec::new();
        let mut release = Vec::new();
        for shard in 0..9 {
            let (started_tx, started_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            engine
                .inner
                .connections
                .block_next_connection_setup(shard, started_tx, release_rx)
                .unwrap();
            started.push(Some(started_rx));
            release.push(release_tx);
        }

        let query_engine = engine.clone();
        let query = tokio::spawn(async move {
            let session = query_engine.session();
            query_engine
                .query_logical(
                    &session,
                    Statement::new("SELECT tenant_id FROM events", vec![]),
                )
                .await
        });

        for (shard, receiver) in started.iter_mut().enumerate().take(8) {
            wait_for_blocking_signal(
                receiver.take().unwrap(),
                "one of the first eight scatter reads should start",
            )
            .await;
            assert_eq!(
                engine.inner.connections.snapshot().unwrap().shards[shard].active,
                1
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(matches!(
            started[8].as_ref().unwrap().try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        release[0].send(()).unwrap();
        wait_for_blocking_signal(
            started[8].take().unwrap(),
            "the ninth scatter read should start after one bounded slot is released",
        )
        .await;
        for sender in release.iter().skip(1) {
            sender.send(()).unwrap();
        }

        let result = timeout(Duration::from_secs(2), query)
            .await
            .expect("the bounded scatter query should complete")
            .unwrap()
            .unwrap();
        assert_eq!(result.shards, (0_u16..9).collect::<Vec<_>>());
        assert!(result.value.is_empty());
        wait_for_worker_capacity(&engine, 9).await;
        assert!(
            engine
                .inner
                .connections
                .snapshot()
                .unwrap()
                .shards
                .iter()
                .all(|shard| shard.active == 0 && shard.queued == 0)
        );
    }

    #[tokio::test]
    async fn logical_scatter_deadline_cancels_and_drains_every_started_shard() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap();
        let (_temp, engine) = engine_with_sharded_events(4, options);
        let (started_tx, started_rx) = mpsc::channel();
        let (_release_tx, release_rx) = mpsc::channel();
        engine
            .inner
            .connections
            .block_next_connection_setup(0, started_tx, release_rx)
            .unwrap();

        let context = RequestContext::new()
            .with_timeout(Duration::from_secs(1))
            .unwrap();
        let session = Arc::new(engine.session());
        let query_engine = engine.clone();
        let query_session = Arc::clone(&session);
        let query = tokio::spawn(async move {
            query_engine
                .query_logical_with_context(
                    &query_session,
                    Statement::new("SELECT tenant_id FROM events", vec![]),
                    context,
                )
                .await
        });
        wait_for_blocking_signal(started_rx, "one scatter shard should enter SQLite setup").await;

        let error = timeout(Duration::from_secs(2), query)
            .await
            .expect("the shared deadline should interrupt the blocked shard")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
        assert_eq!(engine.active_operations_for_test(), 0);
        wait_for_worker_capacity(&engine, 4).await;
        assert!(
            engine
                .inner
                .connections
                .snapshot()
                .unwrap()
                .shards
                .iter()
                .all(|shard| shard.active == 0 && shard.queued == 0)
        );

        let recovered = engine
            .query_logical(
                &session,
                Statement::new("SELECT tenant_id FROM events", vec![]),
            )
            .await
            .unwrap();
        assert_eq!(recovered.shards, vec![0, 1, 2, 3]);
        assert!(recovered.value.is_empty());
    }

    #[tokio::test]
    async fn writer_on_another_sqlite_file_progresses_while_scatter_is_in_flight() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap();
        let (_temp, engine) = engine_with_sharded_events(4, options);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        engine
            .inner
            .connections
            .block_next_connection_setup(0, started_tx, release_rx)
            .unwrap();

        let query_engine = engine.clone();
        let scatter = tokio::spawn(async move {
            let session = query_engine.session();
            query_engine
                .query_logical(
                    &session,
                    Statement::new("SELECT tenant_id, payload FROM events", vec![]),
                )
                .await
        });
        wait_for_blocking_signal(started_rx, "scatter should remain active on shard zero").await;

        let key = integer_key_for_shard(&engine, 1, None);
        let writer = engine.session();
        writer.set_routing_key(key.to_string()).await.unwrap();
        let inserted = timeout(
            Duration::from_secs(2),
            engine.execute(
                &writer,
                Statement::new(
                    "INSERT INTO events (tenant_id, payload) VALUES (?1, ?2)",
                    vec![Value::Int64(key), Value::from("written independently")],
                ),
            ),
        )
        .await
        .expect("a writer on shard one must not wait for shard zero")
        .unwrap();
        assert_eq!(inserted.shard, 1);
        assert_eq!(inserted.value, 1);

        release_tx.send(()).unwrap();
        timeout(Duration::from_secs(2), scatter)
            .await
            .expect("scatter should finish after the held shard is released")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn aborting_logical_scatter_retains_guards_until_detached_cleanup_finishes() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap();
        let (_temp, engine) = engine_with_sharded_events(4, options);
        let (started_tx, started_rx) = mpsc::channel();
        let (_release_tx, release_rx) = mpsc::channel();
        engine
            .inner
            .connections
            .block_next_connection_setup(0, started_tx, release_rx)
            .unwrap();

        let session = Arc::new(engine.session());
        let query_engine = engine.clone();
        let query_session = Arc::clone(&session);
        let query = tokio::spawn(async move {
            query_engine
                .query_logical(
                    &query_session,
                    Statement::new("SELECT tenant_id FROM events", vec![]),
                )
                .await
        });
        wait_for_blocking_signal(started_rx, "one scatter child should enter SQLite setup").await;
        assert_eq!(engine.active_operations_for_test(), 1);

        query.abort();
        assert!(query.await.unwrap_err().is_cancelled());
        timeout(Duration::from_secs(2), async {
            while engine.active_operations_for_test() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the detached scatter coordinator should finish child cleanup");
        wait_for_worker_capacity(&engine, 4).await;
        assert!(
            engine
                .inner
                .connections
                .snapshot()
                .unwrap()
                .shards
                .iter()
                .all(|shard| shard.active == 0 && shard.queued == 0)
        );

        let recovered = engine
            .query_logical(
                &session,
                Statement::new("SELECT tenant_id FROM events", vec![]),
            )
            .await
            .unwrap();
        assert_eq!(recovered.shards, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn one_scatter_failure_cancels_and_drains_a_blocked_sibling_without_partial_rows() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap();
        let (_temp, engine) = engine_with_sharded_events(4, options);
        let session = Arc::new(engine.session());
        let first = integer_key_for_shard(&engine, 0, None);
        let second = integer_key_for_shard(&engine, 0, Some(first));
        session.set_routing_key(first.to_string()).await.unwrap();
        engine
            .execute(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, payload) VALUES (?1, 'one'), (?2, 'two')",
                    vec![Value::Int64(first), Value::Int64(second)],
                ),
            )
            .await
            .unwrap();

        let (failure_started_tx, failure_started_rx) = mpsc::channel();
        let (failure_release_tx, failure_release_rx) = mpsc::channel();
        engine
            .inner
            .connections
            .block_next_control_teardown(0, failure_started_tx, failure_release_rx)
            .unwrap();
        let (sibling_started_tx, sibling_started_rx) = mpsc::channel();
        let (_sibling_release_tx, sibling_release_rx) = mpsc::channel();
        engine
            .inner
            .connections
            .block_next_connection_setup(3, sibling_started_tx, sibling_release_rx)
            .unwrap();

        let context = RequestContext::new()
            .with_result_limits(ResultLimits::new(1, ResultLimits::default().max_bytes()).unwrap());
        let query_engine = engine.clone();
        let query_session = Arc::clone(&session);
        let query = tokio::spawn(async move {
            query_engine
                .query_logical_with_context(
                    &query_session,
                    Statement::new("SELECT tenant_id FROM events", vec![]),
                    context,
                )
                .await
        });
        wait_for_blocking_signal(
            sibling_started_rx,
            "a sibling shard should be blocked before the first error is published",
        )
        .await;
        wait_for_blocking_signal(
            failure_started_rx,
            "the row-limit failure should reach controlled SQLite cleanup",
        )
        .await;
        failure_release_tx.send(()).unwrap();

        let error = timeout(Duration::from_secs(2), query)
            .await
            .expect("the first shard failure should cancel and drain its sibling")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert!(error.diagnostic().contains("row limit"));
        assert_eq!(engine.active_operations_for_test(), 0);
        wait_for_worker_capacity(&engine, 4).await;
        assert!(
            engine
                .inner
                .connections
                .snapshot()
                .unwrap()
                .shards
                .iter()
                .all(|shard| shard.active == 0 && shard.queued == 0)
        );

        let recovered = engine
            .query_logical(
                &session,
                Statement::new("SELECT tenant_id FROM events", vec![]),
            )
            .await
            .unwrap();
        assert_eq!(recovered.value.len(), 2);
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn experimental_vtab_engine_writes_mutate_only_the_metadata_selected_shard() {
        let options = EngineOptions::default().with_experimental_vtab_writes(true);
        let (temp, engine) = engine_with_sharded_events(4, options);
        let session = engine.session();

        let keys = (0..4)
            .map(|shard| integer_key_for_shard(&engine, shard, None))
            .collect::<Vec<_>>();

        session.set_routing_key(keys[1].to_string()).await.unwrap();
        let mismatch = engine
            .execute(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, payload) VALUES (?1, ?2)",
                    vec![Value::Int64(keys[0]), Value::from("wrong route")],
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(mismatch.kind(), EngineErrorKind::InvalidArgument);

        for (shard, key) in keys.iter().copied().enumerate() {
            session.set_routing_key(key.to_string()).await.unwrap();
            let inserted = engine
                .execute(
                    &session,
                    Statement::new(
                        "INSERT INTO events (tenant_id, payload) VALUES (?1, ?2)",
                        vec![Value::Int64(key), Value::from(format!("event-{shard}"))],
                    ),
                )
                .await
                .unwrap();
            assert_eq!(inserted.shard, u16::try_from(shard).unwrap());
            assert_eq!(inserted.value, 1);
        }

        for (owner, key) in keys.iter().copied().enumerate() {
            for shard in 0..4 {
                let connection = rusqlite::Connection::open(
                    temp.path().join(format!("shards/{shard:04}.sqlite")),
                )
                .unwrap();
                let count = connection
                    .query_row(
                        "SELECT COUNT(*) FROM events WHERE tenant_id = ?1",
                        [key],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap();
                assert_eq!(count, i64::from(shard == owner));
            }
        }

        let key = keys[2];
        session.set_routing_key(key.to_string()).await.unwrap();
        assert_eq!(
            engine
                .execute(
                    &session,
                    Statement::new(
                        "UPDATE events SET payload = ?2 WHERE tenant_id = ?1",
                        vec![Value::Int64(key), Value::from("updated")],
                    ),
                )
                .await
                .unwrap()
                .value,
            1
        );
        assert_eq!(
            engine
                .execute(
                    &session,
                    Statement::new(
                        "DELETE FROM events WHERE tenant_id = ?1",
                        vec![Value::Int64(key)],
                    ),
                )
                .await
                .unwrap()
                .value,
            1
        );
        assert_eq!(
            engine
                .execute(
                    &session,
                    Statement::new(
                        "DELETE FROM events WHERE tenant_id = ?1",
                        vec![Value::Int64(key)],
                    ),
                )
                .await
                .unwrap()
                .value,
            0
        );

        let extra_zero = integer_key_for_shard(&engine, 0, Some(keys[0]));
        let extra_one = integer_key_for_shard(&engine, 1, Some(keys[1]));
        session
            .set_routing_key(extra_zero.to_string())
            .await
            .unwrap();
        let multiple_owners = engine
            .execute(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, payload) VALUES (?1, 'zero'), (?2, 'one')",
                    vec![Value::Int64(extra_zero), Value::Int64(extra_one)],
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(multiple_owners.kind(), EngineErrorKind::InvalidArgument);
        for shard in 0..4 {
            let connection =
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap();
            let count = connection
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE tenant_id IN (?1, ?2)",
                    [extra_zero, extra_one],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap();
            assert_eq!(count, 0);
        }

        let pools = engine.pool_snapshot_for_test().unwrap();
        assert!(pools.shards.iter().all(|shard| {
            shard.active == 0 && shard.queued == 0 && shard.opened == 0 && shard.checkouts == 0
        }));
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn experimental_vtab_nonzero_write_accounts_for_shard_zero_bootstrap_capacity() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap()
            .with_experimental_vtab_writes(true);
        let (_temp, engine) = engine_with_sharded_events(4, options);
        let held = engine
            .inner
            .connections
            .acquire_for_owner(0, ConnectionOwner::new(u64::MAX))
            .await
            .unwrap();
        let available_workers = engine.inner.workers.available_permits();
        let key = integer_key_for_shard(&engine, 1, None);
        let session = Arc::new(engine.session());
        session.set_routing_key(key.to_string()).await.unwrap();

        let write_engine = engine.clone();
        let write_session = Arc::clone(&session);
        let write = tokio::spawn(async move {
            write_engine
                .execute(
                    &write_session,
                    Statement::new(
                        "INSERT INTO events (tenant_id, payload) VALUES (?1, 'accounted')",
                        vec![Value::Int64(key)],
                    ),
                )
                .await
        });

        wait_for_pool_occupancy(&engine, 0, 1, 1).await;
        assert_eq!(engine.inner.workers.available_permits(), available_workers);
        let target = engine.pool_snapshot_for_test().unwrap().shards[1];
        assert_eq!(target.active, 0);
        assert_eq!(target.queued, 0);

        drop(held);
        let inserted = timeout(Duration::from_secs(2), write)
            .await
            .expect("releasing shard-zero capacity should admit registry bootstrap")
            .unwrap()
            .unwrap();
        assert_eq!(inserted.shard, 1);
        assert_eq!(inserted.value, 1);
        for shard in 0..4 {
            wait_for_pool_occupancy(&engine, shard, 0, 0).await;
        }
        wait_for_worker_capacity(&engine, available_workers).await;
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn experimental_vtab_deadline_interrupts_locked_registry_bootstrap_promptly() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap()
            .with_experimental_vtab_writes(true);
        let (_temp, engine) = engine_with_sharded_events(4, options);
        let blocker = engine.inner.database.storage.open_shard(0).unwrap();
        blocker
            .execute_batch("PRAGMA locking_mode = EXCLUSIVE; BEGIN EXCLUSIVE")
            .unwrap();
        let available_workers = engine.inner.workers.available_permits();
        let key = integer_key_for_shard(&engine, 1, None);
        let session = engine.session();
        session.set_routing_key(key.to_string()).await.unwrap();
        let context = RequestContext::new()
            .with_timeout(Duration::from_millis(75))
            .unwrap();

        let started = std::time::Instant::now();
        let error = timeout(
            Duration::from_secs(1),
            engine.execute_with_context(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, payload) VALUES (?1, 'cancelled bootstrap')",
                    vec![Value::Int64(key)],
                ),
                context,
            ),
        )
        .await
        .expect("the deadline must preempt SQLite's normal five-second bootstrap wait")
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
        assert!(started.elapsed() < Duration::from_secs(1));

        blocker.execute_batch("COMMIT").unwrap();
        drop(blocker);
        for shard in 0..4 {
            wait_for_pool_occupancy(&engine, shard, 0, 0).await;
        }
        wait_for_worker_capacity(&engine, available_workers).await;
        assert_eq!(engine.active_operations_for_test(), 0);

        assert_eq!(
            engine
                .execute(
                    &session,
                    Statement::new(
                        "INSERT INTO events (tenant_id, payload) VALUES (?1, 'recovered')",
                        vec![Value::Int64(key)],
                    ),
                )
                .await
                .unwrap()
                .value,
            1
        );
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn experimental_vtab_explicit_cancellation_interrupts_a_locked_child_write() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap()
            .with_experimental_vtab_writes(true);
        let (temp, engine) = engine_with_sharded_events(4, options);
        let key = integer_key_for_shard(&engine, 1, None);
        let session = Arc::new(engine.session());
        session.set_routing_key(key.to_string()).await.unwrap();
        let blocker = rusqlite::Connection::open(temp.path().join("shards/0001.sqlite")).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        let token = CancellationToken::new();
        let context = RequestContext::new().with_cancellation_token(token.clone());
        let child_busy_gate = engine.inner.registry_schema_cache.install_child_busy_gate();
        let cancellation_observer = engine
            .inner
            .registry_schema_cache
            .install_cancellation_observer();

        let write_engine = engine.clone();
        let write_session = Arc::clone(&session);
        let write = tokio::spawn(async move {
            write_engine
                .execute_with_context(
                    &write_session,
                    Statement::new(
                        "INSERT INTO events (tenant_id, payload) VALUES (?1, 'cancelled')",
                        vec![Value::Int64(key)],
                    ),
                    context,
                )
                .await
        });
        let mut child_busy_gate = timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking(move || {
                child_busy_gate.wait_until_started();
                child_busy_gate
            }),
        )
        .await
        .expect("the target child must reach a real SQLite busy result")
        .unwrap();
        assert!(token.cancel());
        timeout(Duration::from_secs(2), async {
            while !cancellation_observer.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the Engine cancellation callback must reach the active coordinator");
        child_busy_gate.release();
        let error = timeout(Duration::from_secs(2), write)
            .await
            .expect("explicit cancellation should interrupt the child lock wait")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);

        blocker.execute_batch("ROLLBACK").unwrap();
        drop(blocker);
        for shard in 0..4 {
            wait_for_pool_occupancy(&engine, shard, 0, 0).await;
        }
        assert_eq!(engine.active_operations_for_test(), 0);
        assert_eq!(
            engine
                .inner
                .database
                .storage
                .open_shard(1)
                .unwrap()
                .query_row("SELECT COUNT(*) FROM events", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn experimental_vtab_known_commit_wins_a_late_engine_cancellation() {
        let options = EngineOptions::default()
            .with_request_timeout(None)
            .unwrap()
            .with_experimental_vtab_writes(true);
        let (temp, engine) = engine_with_sharded_events(2, options);
        let key = integer_key_for_shard(&engine, 1, None);
        let session = Arc::new(engine.session());
        session.set_routing_key(key.to_string()).await.unwrap();
        let token = CancellationToken::new();
        let context = RequestContext::new().with_cancellation_token(token.clone());
        let commit_gate = engine.inner.registry_schema_cache.install_commit_gate();
        let cancellation_observer = engine
            .inner
            .registry_schema_cache
            .install_cancellation_observer();

        let write_engine = engine.clone();
        let write_session = Arc::clone(&session);
        let write = tokio::spawn(async move {
            write_engine
                .execute_with_context(
                    &write_session,
                    Statement::new(
                        "INSERT INTO events (tenant_id, payload) VALUES (?1, 'committed')",
                        vec![Value::Int64(key)],
                    ),
                    context,
                )
                .await
        });
        let mut commit_gate = timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking(move || {
                commit_gate.wait_until_started();
                commit_gate
            }),
        )
        .await
        .expect("the child finalizer must claim the commit decision")
        .unwrap();

        assert!(token.cancel());
        timeout(Duration::from_secs(2), async {
            while !cancellation_observer.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("late cancellation must reach the commit linearization point");
        commit_gate.release();

        let inserted = timeout(Duration::from_secs(2), write)
            .await
            .expect("the known commit must finish after cancellation")
            .unwrap()
            .unwrap();
        assert_eq!(inserted.shard, 1);
        assert_eq!(inserted.value, 1);
        assert_eq!(
            rusqlite::Connection::open(temp.path().join("shards/0001.sqlite"))
                .unwrap()
                .query_row(
                    "SELECT payload FROM events WHERE tenant_id = ?1",
                    [key],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "committed"
        );
        assert_eq!(engine.active_operations_for_test(), 0);
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn experimental_vtab_registry_cache_rebuilds_after_schema_generation_changes() {
        let options = EngineOptions::default().with_experimental_vtab_writes(true);
        let (temp, engine) = engine_with_sharded_events(2, options);
        let key = integer_key_for_shard(&engine, 1, None);
        let session = engine.session();
        session.set_routing_key(key.to_string()).await.unwrap();

        engine
            .execute(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, payload) VALUES (?1, 'warm')",
                    vec![Value::Int64(key)],
                ),
            )
            .await
            .unwrap();
        engine
            .broadcast(
                &session,
                "ALTER TABLE events ADD COLUMN note TEXT".to_owned(),
            )
            .await
            .unwrap();

        let next_key = integer_key_for_shard(&engine, 1, Some(key));
        session.set_routing_key(next_key.to_string()).await.unwrap();
        let inserted = engine
            .execute(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, payload, note) VALUES (?1, 'new', 'fresh')",
                    vec![Value::Int64(next_key)],
                ),
            )
            .await
            .unwrap();
        assert_eq!(inserted.shard, 1);
        assert_eq!(inserted.value, 1);
        assert_eq!(
            rusqlite::Connection::open(temp.path().join("shards/0001.sqlite"))
                .unwrap()
                .query_row(
                    "SELECT note FROM events WHERE tenant_id = ?1",
                    [next_key],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "fresh"
        );
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn experimental_vtab_engine_routes_explicit_native_ids_to_their_owner() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 4).unwrap();
        database
            .broadcast(
                "CREATE TABLE native_events (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     payload TEXT NOT NULL
                 )",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical_database,
                    "native_events",
                    ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
                )
                .unwrap()
                .with_generated_id_policy(GeneratedIdPolicy::native_range_v1("id").unwrap())
                .unwrap(),
            ])
            .unwrap();

        let (owner_shard, native_id, reserved_floor) = {
            let owners = database.storage.allocation_owner_map().unwrap();
            (0..database.shard_count())
                .find_map(|owner_shard| {
                    let owner = owners.owner_for_physical_shard(owner_shard).unwrap();
                    (1..=256_u64).find_map(|local_sequence| {
                        let native_id =
                            crate::core::generated_id::NativeRangeV1Id::new(owner, local_sequence)
                                .unwrap()
                                .encode();
                        (database.shard_for_key(native_id.to_string().as_bytes()) != owner_shard)
                            .then(|| {
                                (
                                    owner_shard,
                                    native_id,
                                    crate::core::generated_id::native_range_v1_sequence_floor(
                                        owner,
                                    ),
                                )
                            })
                    })
                })
                .expect("a native owner route must differ from an ordinary hash route")
        };
        assert_ne!(
            database.shard_for_key(native_id.to_string().as_bytes()),
            owner_shard,
            "the regression key must expose owner-map versus hash routing"
        );

        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_experimental_vtab_writes(true);
        let engine = Engine::from_database_with_options(Arc::new(database), options).unwrap();
        let session = engine.session();

        // The planner, pool admission, coordinator callback, and reported
        // shard must all use the persisted allocation owner rather than the
        // ordinary hash route for the decimal routing-context bytes.
        session
            .set_routing_key(native_id.to_string())
            .await
            .unwrap();
        let inserted = engine
            .execute_write(
                &session,
                Statement::new(
                    "INSERT INTO native_events (id, payload) VALUES (?1, 'owner-routed')",
                    vec![Value::Int64(native_id)],
                ),
            )
            .await
            .unwrap();
        assert_eq!(inserted.shard, owner_shard);
        assert_eq!(inserted.value.rows_affected, 1);
        assert_eq!(inserted.value.generated_key, None);

        // A sequence floor has native marker bits but is not a valid row ID.
        // Reject it as caller input without degrading authoritative storage.
        session
            .set_routing_key(reserved_floor.to_string())
            .await
            .unwrap();
        let error = engine
            .execute(
                &session,
                Statement::new(
                    "INSERT INTO native_events (id, payload) VALUES (?1, 'reserved')",
                    vec![Value::Int64(reserved_floor)],
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
        assert_eq!(session.state().await, SessionState::Ready);
        assert_eq!(
            engine.inner.database.storage.schema_gate_snapshot().state,
            crate::storage::SchemaGateState::Ready
        );

        for shard in 0..engine.shard_count() {
            assert_eq!(
                engine
                    .inner
                    .database
                    .storage
                    .open_shard(shard)
                    .unwrap()
                    .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                if shard == owner_shard { 1 } else { 0 },
                "shard {shard}"
            );
        }
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn experimental_vtab_engine_generated_write_returns_the_routed_physical_key() {
        let (temp, engine, table) = engine_with_native_events(2);
        let session = engine.session();

        // The lower exact-target seam accepts only an already-admitted decision
        // and does not require a session routing-key surrogate.
        let inserted = engine
            .execute_generated_write(
                &session,
                Statement::new(
                    "INSERT INTO native_events (payload) VALUES (?1)",
                    vec![Value::from("engine-generated")],
                ),
                table,
                1,
            )
            .await
            .unwrap();

        assert_eq!(inserted.shard, 1);
        assert_eq!(inserted.value.rows_affected, 1);
        let generated = inserted.value.generated_key.unwrap();
        assert_eq!(generated.column, "id");
        let Value::Int64(generated_id) = generated.value else {
            panic!("native_range_v1 must return an Int64 generated key");
        };
        let decoded = crate::core::generated_id::NativeRangeV1Id::decode(generated_id).unwrap();
        assert_eq!(
            engine
                .inner
                .database
                .storage
                .allocation_owner_map()
                .unwrap()
                .physical_shard(decoded.owner()),
            Some(1)
        );
        assert_eq!(
            rusqlite::Connection::open(temp.path().join("shards/0001.sqlite"))
                .unwrap()
                .query_row(
                    "SELECT payload FROM native_events WHERE id = ?1",
                    [generated_id],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "engine-generated"
        );
        assert_eq!(
            rusqlite::Connection::open(temp.path().join("shards/0000.sqlite"))
                .unwrap()
                .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn public_generated_insert_rejects_sqlite_quoted_alias_before_mutation() {
        let (temp, engine, _table) = engine_with_native_events(4);
        let session = engine.session();
        let owner = engine
            .inner
            .database
            .storage
            .allocation_owner_map()
            .unwrap()
            .owner_for_physical_shard(0)
            .unwrap();
        let explicit_id = crate::core::generated_id::NativeRangeV1Id::new(owner, 41)
            .unwrap()
            .encode();
        session
            .set_routing_key(explicit_id.to_string())
            .await
            .unwrap();

        let error = engine
            .execute_write(
                &session,
                Statement::new(
                    "INSERT INTO native_events (\"ID\", payload) VALUES (NULL, ?1)",
                    vec![Value::from("must-not-generate")],
                ),
            )
            .await
            .unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
        let error = engine
            .execute_write(
                &session,
                Statement::new(
                    "INSERT INTO native_events (id, \"ID\", payload) VALUES (?1, NULL, ?2)",
                    vec![Value::Int64(explicit_id), Value::from("mixed-alias")],
                ),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                error.kind(),
                EngineErrorKind::InvalidQuery | EngineErrorKind::Unsupported
            ),
            "the mixed alias must fail during SQL validation or generated-key planning"
        );
        for shard in 0..engine.shard_count() {
            let connection =
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                0,
                "quoted explicit NULL must not mutate physical shard {shard}"
            );
        }
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn public_omitted_key_insert_uses_allocator_route_and_returns_its_key() {
        let (temp, engine, _table) = engine_with_native_events(4);
        let session = engine.session();

        let inserted = engine
            .execute_write(
                &session,
                Statement::new(
                    "INSERT INTO native_events (payload) VALUES (?1)",
                    vec![Value::from("public-generated")],
                ),
            )
            .await
            .unwrap();

        assert_eq!(inserted.value.rows_affected, 1);
        let generated = inserted.value.generated_key.unwrap();
        assert_eq!(generated.column, "id");
        let Value::Int64(id) = generated.value else {
            panic!("native_range_v1 must return an Int64 generated key");
        };
        let decoded = crate::core::generated_id::NativeRangeV1Id::decode(id).unwrap();
        assert_eq!(
            engine
                .inner
                .database
                .storage
                .allocation_owner_map()
                .unwrap()
                .physical_shard(decoded.owner()),
            Some(inserted.shard)
        );
        for shard in 0..engine.shard_count() {
            let connection =
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap();
            assert_eq!(
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM native_events WHERE id = ?1 AND payload = 'public-generated'",
                        [id],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                i64::from(shard == inserted.shard),
                "physical shard {shard}"
            );
        }
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn public_omitted_key_inserts_admit_and_mutate_each_selected_owner_shard() {
        let (temp, engine, _table) = engine_with_native_events(4);
        let session = engine.session();
        let mut observed_shards = Vec::new();
        let mut observed_ids = std::collections::BTreeSet::new();

        for ordinal in 0..8_i64 {
            let inserted = engine
                .execute_write(
                    &session,
                    Statement::new(
                        "INSERT INTO native_events (payload) VALUES (?1)",
                        vec![Value::from(format!("generated-{ordinal}"))],
                    ),
                )
                .await
                .unwrap();
            assert_eq!(inserted.value.rows_affected, 1);
            let generated = inserted.value.generated_key.unwrap();
            let Value::Int64(id) = generated.value else {
                panic!("native_range_v1 must return an Int64 generated key");
            };
            assert!(observed_ids.insert(id), "generated IDs must be unique");
            let decoded = crate::core::generated_id::NativeRangeV1Id::decode(id).unwrap();
            assert_eq!(
                engine
                    .inner
                    .database
                    .storage
                    .allocation_owner_map()
                    .unwrap()
                    .physical_shard(decoded.owner()),
                Some(inserted.shard)
            );
            observed_shards.push(inserted.shard);
        }

        assert_eq!(observed_shards, vec![0, 1, 2, 3, 0, 1, 2, 3]);
        for shard in 0..engine.shard_count() {
            assert_eq!(
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap()
                    .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                2,
                "physical shard {shard}"
            );
        }
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn native_generated_round_robin_is_independent_per_table() {
        let (temp, engine) = engine_with_two_native_tables(4);
        let session = engine.session();
        let mut observed_a = Vec::new();
        let mut observed_b = Vec::new();

        for ordinal in 0..4_i64 {
            for (table, observed) in [
                ("native_events_a", &mut observed_a),
                ("native_events_b", &mut observed_b),
            ] {
                let inserted = engine
                    .execute_write(
                        &session,
                        Statement::new(
                            format!("INSERT INTO {table} (payload) VALUES (?1)"),
                            vec![Value::from(format!("generated-{ordinal}"))],
                        ),
                    )
                    .await
                    .unwrap();
                observed.push(inserted.shard);
            }
        }

        assert_eq!(observed_a, vec![0, 1, 2, 3]);
        assert_eq!(observed_b, vec![0, 1, 2, 3]);
        for shard in 0..engine.shard_count() {
            let connection =
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap();
            for table in ["native_events_a", "native_events_b"] {
                assert_eq!(
                    connection
                        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                            row.get::<_, i64>(0)
                        })
                        .unwrap(),
                    1,
                    "{table} on physical shard {shard}"
                );
            }
        }
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn native_generated_public_write_skips_an_exhausted_owner() {
        let (temp, engine, _table) = engine_with_native_events(2);
        let owner = engine
            .inner
            .database
            .storage
            .allocation_owner_map()
            .unwrap()
            .owner_for_physical_shard(0)
            .unwrap();
        let ceiling = crate::core::generated_id::native_range_v1_sequence_ceiling(owner);
        engine
            .inner
            .database
            .storage
            .open_shard(0)
            .unwrap()
            .execute(
                "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'native_events'",
                [ceiling],
            )
            .unwrap();

        let inserted = engine
            .execute_write(
                &engine.session(),
                Statement::new(
                    "INSERT INTO native_events (payload) VALUES ('fallback')",
                    vec![],
                ),
            )
            .await
            .unwrap();
        assert_eq!(inserted.shard, 1);
        assert_eq!(inserted.value.rows_affected, 1);
        assert!(inserted.value.generated_key.is_some());
        for shard in 0..2_u16 {
            assert_eq!(
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap()
                    .query_row(
                        "SELECT COUNT(*) FROM native_events WHERE payload = 'fallback'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                i64::from(shard == 1),
                "physical shard {shard}"
            );
        }
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn native_generated_public_write_never_waits_for_a_busy_candidate_after_worker_admission()
    {
        let (_temp, default_engine, table) = engine_with_native_events(2);
        let database = Arc::clone(&default_engine.inner.database);
        drop(default_engine);
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_experimental_vtab_writes(true);
        let engine = Engine::from_database_with_options(database, options).unwrap();
        let session = engine.session();
        engine
            .execute_generated_write(
                &session,
                Statement::new(
                    "INSERT INTO native_events (payload) VALUES ('warm-cache')",
                    vec![],
                ),
                table,
                1,
            )
            .await
            .unwrap();
        let occupied = engine
            .inner
            .connections
            .acquire_for_owner(0, ConnectionOwner::new(u64::MAX))
            .await
            .unwrap();

        let inserted = timeout(
            Duration::from_secs(2),
            engine.execute_write(
                &session,
                Statement::new(
                    "INSERT INTO native_events (payload) VALUES ('busy-fallback')",
                    vec![],
                ),
            ),
        )
        .await
        .expect("native fallback must not wait for capacity while retaining a worker")
        .unwrap();
        assert_eq!(inserted.shard, 1);
        drop(occupied);
        for shard in 0..engine.shard_count() {
            wait_for_pool_occupancy(&engine, shard, 0, 0).await;
        }
        wait_for_worker_capacity(&engine, usize::from(engine.shard_count())).await;
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn hilo_public_omitted_key_insert_reports_its_allocator_route() {
        let (temp, engine) = engine_with_hilo_events(4);
        let inserted = engine
            .execute_write(
                &engine.session(),
                Statement::new(
                    "INSERT INTO hilo_events (payload) VALUES ('hilo-generated')",
                    vec![],
                ),
            )
            .await
            .unwrap();
        assert_eq!(inserted.value.rows_affected, 1);
        let generated = inserted.value.generated_key.unwrap();
        assert_eq!(generated.column, "id");
        let Value::Int64(id) = generated.value else {
            panic!("hilo_v1 must return an Int64 generated key");
        };
        let decoded = crate::core::generated_id::HiloV1Id::decode(id).unwrap();
        assert_eq!(decoded.sequence(), 1);
        assert_eq!(
            inserted.shard,
            engine
                .inner
                .database
                .shard_for_key(&crate::core::canonical_shard_key_bytes(
                    crate::core::CanonicalShardKeyRef::Int64(id)
                ))
        );
        for shard in 0..engine.shard_count() {
            assert_eq!(
                rusqlite::Connection::open(
                    temp.path().join(format!("shards/{shard:04}.sqlite"))
                )
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM hilo_events WHERE id = ?1 AND payload = 'hilo-generated'",
                    [id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
                i64::from(shard == inserted.shard),
                "physical shard {shard}"
            );
        }
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn public_omitted_key_insert_respects_the_disabled_runtime_gate_before_mutation() {
        let (temp, enabled_engine, _table) = engine_with_native_events(2);
        let database = Arc::clone(&enabled_engine.inner.database);
        drop(enabled_engine);
        let engine =
            Engine::from_database_with_options(database, EngineOptions::default()).unwrap();
        let session = engine.session();
        let before = (0..engine.shard_count())
            .map(|shard| {
                let connection = engine.inner.database.storage.open_shard(shard).unwrap();
                let count = connection
                    .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap();
                let sequence = connection
                    .query_row(
                        "SELECT seq FROM sqlite_sequence WHERE name = 'native_events'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap();
                (count, sequence)
            })
            .collect::<Vec<_>>();

        let error = engine
            .execute_write(
                &session,
                Statement::new(
                    "INSERT INTO native_events (payload) VALUES ('disabled')",
                    vec![],
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(error.diagnostic().contains("writes are disabled"));
        assert_eq!(session.state().await, SessionState::Ready);

        for (shard, expected) in before.into_iter().enumerate() {
            let connection =
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                expected.0,
                "physical shard {shard}"
            );
            assert_eq!(
                connection
                    .query_row(
                        "SELECT seq FROM sqlite_sequence WHERE name = 'native_events'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                expected.1,
                "physical shard {shard}"
            );
        }
        assert_eq!(engine.active_operations_for_test(), 0);
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn public_omitted_key_insert_survives_reopen_with_a_new_unique_id() {
        let (temp, engine, _table) = engine_with_native_events(2);
        let first_session = engine.session();
        let first = engine
            .execute_write(
                &first_session,
                Statement::new(
                    "INSERT INTO native_events (payload) VALUES ('before-reopen')",
                    vec![],
                ),
            )
            .await
            .unwrap();
        let first_shard = first.shard;
        let Value::Int64(first_id) = first.value.generated_key.unwrap().value else {
            panic!("native_range_v1 must return an Int64 generated key");
        };
        drop(first_session);
        drop(engine);

        let options = EngineOptions::default().with_experimental_vtab_writes(true);
        let reopened = Engine::open_with_options(temp.path(), 2, options)
            .await
            .unwrap();
        let reopened_session = reopened.session();
        let second = reopened
            .execute_write(
                &reopened_session,
                Statement::new(
                    "INSERT INTO native_events (payload) VALUES ('after-reopen')",
                    vec![],
                ),
            )
            .await
            .unwrap();
        let second_shard = second.shard;
        let Value::Int64(second_id) = second.value.generated_key.unwrap().value else {
            panic!("native_range_v1 must return an Int64 generated key");
        };

        assert_ne!(first_id, second_id);
        for (shard, id) in [(first_shard, first_id), (second_shard, second_id)] {
            let decoded = crate::core::generated_id::NativeRangeV1Id::decode(id).unwrap();
            assert_eq!(
                reopened
                    .inner
                    .database
                    .storage
                    .allocation_owner_map()
                    .unwrap()
                    .physical_shard(decoded.owner()),
                Some(shard)
            );
        }
        assert_eq!(
            (0..reopened.shard_count())
                .map(|shard| {
                    reopened
                        .inner
                        .database
                        .storage
                        .open_shard(shard)
                        .unwrap()
                        .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                            row.get::<_, i64>(0)
                        })
                        .unwrap()
                })
                .sum::<i64>(),
            2
        );
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn public_omitted_key_constraint_failure_rolls_back_without_exposing_a_key() {
        let (temp, engine, _table) = engine_with_native_events(2);
        let session = engine.session();
        let before = (0..engine.shard_count())
            .map(|shard| {
                engine
                    .inner
                    .database
                    .storage
                    .open_shard(shard)
                    .unwrap()
                    .query_row(
                        "SELECT seq FROM sqlite_sequence WHERE name = 'native_events'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let error = engine
            .execute_write(
                &session,
                Statement::new(
                    "INSERT INTO native_events (payload) VALUES (?1)",
                    vec![Value::Null],
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::NotNullViolation);
        assert_eq!(session.state().await, SessionState::Ready);
        for (shard, expected_sequence) in before.into_iter().enumerate() {
            let connection =
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                0,
                "physical shard {shard}"
            );
            assert_eq!(
                connection
                    .query_row(
                        "SELECT seq FROM sqlite_sequence WHERE name = 'native_events'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                expected_sequence,
                "physical shard {shard}"
            );
        }

        let recovered = engine
            .execute_write(
                &session,
                Statement::new(
                    "INSERT INTO native_events (payload) VALUES ('recovered')",
                    vec![],
                ),
            )
            .await
            .unwrap();
        assert_eq!(recovered.value.rows_affected, 1);
        let Value::Int64(recovered_id) = recovered.value.generated_key.unwrap().value else {
            panic!("native_range_v1 must return an Int64 generated key");
        };
        assert_eq!(
            engine
                .inner
                .database
                .storage
                .open_shard(recovered.shard)
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM native_events WHERE id = ?1 AND payload = 'recovered'",
                    [recovered_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn concurrent_public_omitted_key_writes_are_unique_and_spread_across_owners() {
        const WRITE_COUNT: usize = 16;

        let (temp, engine, _table) = engine_with_native_events(4);
        let barrier = Arc::new(tokio::sync::Barrier::new(WRITE_COUNT + 1));
        let mut writes = Vec::with_capacity(WRITE_COUNT);
        for ordinal in 0..WRITE_COUNT {
            let write_engine = engine.clone();
            let barrier = Arc::clone(&barrier);
            writes.push(tokio::spawn(async move {
                let session = write_engine.session();
                barrier.wait().await;
                write_engine
                    .execute_write(
                        &session,
                        Statement::new(
                            "INSERT INTO native_events (payload) VALUES (?1)",
                            vec![Value::from(format!("concurrent-{ordinal}"))],
                        ),
                    )
                    .await
            }));
        }
        barrier.wait().await;

        let mut ids = std::collections::BTreeSet::new();
        let mut writes_per_shard = [0_usize; 4];
        for write in writes {
            let inserted = timeout(Duration::from_secs(5), write)
                .await
                .expect("a simultaneous native write must finish")
                .unwrap()
                .unwrap();
            assert_eq!(inserted.value.rows_affected, 1);
            let Value::Int64(id) = inserted.value.generated_key.unwrap().value else {
                panic!("native_range_v1 must return an Int64 generated key");
            };
            assert!(ids.insert(id), "generated IDs must be globally unique");
            let decoded = crate::core::generated_id::NativeRangeV1Id::decode(id).unwrap();
            assert_eq!(
                engine
                    .inner
                    .database
                    .storage
                    .allocation_owner_map()
                    .unwrap()
                    .physical_shard(decoded.owner()),
                Some(inserted.shard)
            );
            writes_per_shard[usize::from(inserted.shard)] += 1;
        }

        assert_eq!(ids.len(), WRITE_COUNT);
        assert_eq!(writes_per_shard, [4, 4, 4, 4]);
        for shard in 0..engine.shard_count() {
            assert_eq!(
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap()
                    .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                4,
                "physical shard {shard}"
            );
        }
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn public_omitted_key_cancellation_interrupts_a_locked_owner_and_recovers() {
        let (temp, engine, _table) = engine_with_native_events(2);
        let blocker = rusqlite::Connection::open(temp.path().join("shards/0000.sqlite")).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        let token = CancellationToken::new();
        let context = RequestContext::new().with_cancellation_token(token.clone());
        let child_busy_gate = engine.inner.registry_schema_cache.install_child_busy_gate();
        let cancellation_observer = engine
            .inner
            .registry_schema_cache
            .install_cancellation_observer();
        let session = Arc::new(engine.session());
        let write_engine = engine.clone();
        let write_session = Arc::clone(&session);
        let write = tokio::spawn(async move {
            write_engine
                .execute_write_with_context(
                    &write_session,
                    Statement::new(
                        "INSERT INTO native_events (payload) VALUES ('cancelled')",
                        vec![],
                    ),
                    context,
                )
                .await
        });
        let mut child_busy_gate = timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking(move || {
                child_busy_gate.wait_until_started();
                child_busy_gate
            }),
        )
        .await
        .expect("the generated write must reach a real SQLite busy result")
        .unwrap();

        assert!(token.cancel());
        timeout(Duration::from_secs(2), async {
            while !cancellation_observer.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Engine cancellation must reach the generated coordinator");
        child_busy_gate.release();
        let error = timeout(Duration::from_secs(2), write)
            .await
            .expect("the locked generated write must observe cancellation")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        assert_eq!(session.state().await, SessionState::Ready);

        blocker.execute_batch("ROLLBACK").unwrap();
        drop(blocker);
        for shard in 0..engine.shard_count() {
            wait_for_pool_occupancy(&engine, shard, 0, 0).await;
            assert_eq!(
                engine
                    .inner
                    .database
                    .storage
                    .open_shard(shard)
                    .unwrap()
                    .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                0,
                "physical shard {shard}"
            );
        }
        assert_eq!(engine.active_operations_for_test(), 0);

        let recovered = engine
            .execute_write(
                &session,
                Statement::new(
                    "INSERT INTO native_events (payload) VALUES ('recovered')",
                    vec![],
                ),
            )
            .await
            .unwrap();
        assert_eq!(recovered.shard, 1);
        assert_eq!(recovered.value.rows_affected, 1);
        assert!(recovered.value.generated_key.is_some());
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn prepared_omitted_key_insert_returns_protocol_neutral_generated_write() {
        let (temp, engine, _table) = engine_with_native_events(4);
        let database = engine.catalog().default_database().id();
        let session = engine.session();
        let statement = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::PostgreSql,
                    sql::SqlTranslationMode::Compatibility,
                    "INSERT INTO native_events (payload) VALUES ($1)",
                ),
            )
            .await
            .unwrap();
        let portal = engine
            .bind_statement(&session, statement, vec![Value::from("prepared-generated")])
            .await
            .unwrap();

        let routed = engine.execute_portal(&session, portal).await.unwrap();
        assert_eq!(routed.shard, 0);
        let PreparedExecution::GeneratedWrite(write) = routed.value else {
            panic!("omitted generated key must use PreparedExecution::GeneratedWrite");
        };
        assert_eq!(write.rows_affected, 1);
        let Value::Int64(first_id) = write.generated_key.unwrap().value else {
            panic!("native_range_v1 must return an Int64 generated key");
        };

        let logical = engine
            .execute_portal_logical(&session, portal)
            .await
            .unwrap();
        assert_eq!(logical.shards, vec![1]);
        let PreparedExecution::GeneratedWrite(write) = logical.value else {
            panic!("logical portal execution must retain the generated result");
        };
        assert_eq!(write.rows_affected, 1);
        let Value::Int64(second_id) = write.generated_key.unwrap().value else {
            panic!("native_range_v1 must return an Int64 generated key");
        };
        assert_ne!(first_id, second_id);

        for (shard, id) in [(0_u16, first_id), (1_u16, second_id)] {
            assert_eq!(
                rusqlite::Connection::open(
                    temp.path().join(format!("shards/{shard:04}.sqlite"))
                )
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM native_events WHERE id = ?1 AND payload = 'prepared-generated'",
                    [id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
                1
            );
        }
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn prepared_quoted_case_variant_does_not_enable_generated_write() {
        let (temp, engine, _table) = engine_with_native_events(4);
        let database = engine.catalog().default_database().id();
        let session = engine.session();
        let owner = engine
            .inner
            .database
            .storage
            .allocation_owner_map()
            .unwrap()
            .owner_for_physical_shard(0)
            .unwrap();
        let explicit_id = crate::core::generated_id::NativeRangeV1Id::new(owner, 42)
            .unwrap()
            .encode();
        session
            .set_routing_key(explicit_id.to_string())
            .await
            .unwrap();
        let statement = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::PostgreSql,
                    sql::SqlTranslationMode::Compatibility,
                    "INSERT INTO native_events (\"ID\", payload) VALUES (NULL, $1)",
                ),
            )
            .await
            .unwrap();
        let error = engine
            .bind_statement(&session, statement, vec![Value::from("must-not-generate")])
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
        for shard in 0..engine.shard_count() {
            let connection =
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                0,
                "quoted explicit NULL must not mutate physical shard {shard}"
            );
        }
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn prepared_explicit_native_key_keeps_affected_rows_compatibility() {
        let (_temp, engine, _table) = engine_with_native_events(2);
        let database = engine.catalog().default_database().id();
        let session = engine.session();
        let owner = engine
            .inner
            .database
            .storage
            .allocation_owner_map()
            .unwrap()
            .owner_for_physical_shard(1)
            .unwrap();
        let id = crate::core::generated_id::NativeRangeV1Id::new(owner, 73)
            .unwrap()
            .encode();
        let statement = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    database,
                    sql::SqlDialect::MySql,
                    sql::SqlTranslationMode::Compatibility,
                    "INSERT INTO native_events (id, payload) VALUES (?, ?)",
                ),
            )
            .await
            .unwrap();
        let portal = engine
            .bind_statement(
                &session,
                statement,
                vec![Value::Int64(id), Value::from("explicit")],
            )
            .await
            .unwrap();

        let inserted = engine.execute_portal(&session, portal).await.unwrap();
        assert_eq!(inserted.shard, 1);
        assert_eq!(inserted.value, PreparedExecution::AffectedRows(1));
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn public_omitted_multirow_insert_rejects_before_any_allocator_mutation() {
        let (temp, engine, _table) = engine_with_native_events(2);
        let session = engine.session();
        let before = (0..engine.shard_count())
            .map(|shard| {
                let connection = rusqlite::Connection::open(
                    temp.path().join(format!("shards/{shard:04}.sqlite")),
                )
                .unwrap();
                connection
                    .query_row(
                        "SELECT seq FROM sqlite_sequence WHERE name = 'native_events'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let error = engine
            .execute_write(
                &session,
                Statement::new(
                    "INSERT INTO native_events (payload) VALUES ('first'), ('second')",
                    vec![],
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Unsupported);

        for (shard, expected_sequence) in before.into_iter().enumerate() {
            let connection =
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                0
            );
            assert_eq!(
                connection
                    .query_row(
                        "SELECT seq FROM sqlite_sequence WHERE name = 'native_events'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                expected_sequence
            );
        }
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn experimental_vtab_generated_known_commit_wins_a_late_engine_cancellation() {
        let (temp, engine, table) = engine_with_native_events(2);
        let session = Arc::new(engine.session());
        let token = CancellationToken::new();
        let context = RequestContext::new().with_cancellation_token(token.clone());
        let commit_gate = engine.inner.registry_schema_cache.install_commit_gate();
        let cancellation_observer = engine
            .inner
            .registry_schema_cache
            .install_cancellation_observer();

        let write_engine = engine.clone();
        let write_session = Arc::clone(&session);
        let write = tokio::spawn(async move {
            write_engine
                .execute_generated_write_with_context(
                    &write_session,
                    Statement::new(
                        "INSERT INTO native_events (payload) VALUES (?1)",
                        vec![Value::from("committed-after-cancellation")],
                    ),
                    table,
                    1,
                    context,
                )
                .await
        });
        let mut commit_gate = timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking(move || {
                commit_gate.wait_until_started();
                commit_gate
            }),
        )
        .await
        .expect("the generated child finalizer must claim the commit decision")
        .unwrap();

        assert!(token.cancel());
        timeout(Duration::from_secs(2), async {
            while !cancellation_observer.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("late cancellation must reach the generated commit linearization point");
        commit_gate.release();

        let inserted = timeout(Duration::from_secs(2), write)
            .await
            .expect("the known generated commit must finish after cancellation")
            .unwrap()
            .unwrap();
        assert_eq!(inserted.shard, 1);
        assert_eq!(inserted.value.rows_affected, 1);
        let generated = inserted.value.generated_key.unwrap();
        assert_eq!(generated.column, "id");
        let Value::Int64(generated_id) = generated.value else {
            panic!("native_range_v1 must return an Int64 generated key");
        };
        let decoded = crate::core::generated_id::NativeRangeV1Id::decode(generated_id).unwrap();
        assert_eq!(
            engine
                .inner
                .database
                .storage
                .allocation_owner_map()
                .unwrap()
                .physical_shard(decoded.owner()),
            Some(1)
        );
        assert_eq!(
            rusqlite::Connection::open(temp.path().join("shards/0001.sqlite"))
                .unwrap()
                .query_row(
                    "SELECT payload FROM native_events WHERE id = ?1",
                    [generated_id],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "committed-after-cancellation"
        );
        assert_eq!(session.state().await, SessionState::Ready);
        assert_eq!(
            engine.inner.database.storage.schema_gate_snapshot().state,
            crate::storage::SchemaGateState::Ready
        );
        for shard in 0..engine.shard_count() {
            wait_for_pool_occupancy(&engine, shard, 0, 0).await;
        }
        assert_eq!(engine.active_operations_for_test(), 0);
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn experimental_vtab_engine_generated_write_rejects_multirow_without_mutation() {
        let (temp, engine, table) = engine_with_native_events(2);
        let session = engine.session();

        let error = engine
            .execute_generated_write(
                &session,
                Statement::new(
                    "INSERT INTO native_events (payload) VALUES ('first'), ('second')",
                    vec![],
                ),
                table,
                0,
            )
            .await
            .unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
        assert!(
            error
                .diagnostic()
                .contains("multi-row native generated INSERT")
        );
        assert_eq!(session.state().await, SessionState::Ready);
        for shard in 0..engine.shard_count() {
            assert_eq!(
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap()
                    .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                0,
                "shard {shard}"
            );
        }
        assert_eq!(engine.active_operations_for_test(), 0);
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn experimental_vtab_engine_generated_write_validates_admission_before_mutation() {
        let (temp, engine, native_table) = engine_with_native_events(2);
        let session = engine.session();
        let ordinary_table = engine
            .catalog()
            .table("default", "ordinary_events")
            .unwrap()
            .unwrap()
            .id();

        let cases = [
            (native_table, 2, EngineErrorKind::InvalidArgument),
            (ordinary_table, 0, EngineErrorKind::FailedPrecondition),
            (
                crate::core::TableId::new(u64::MAX).unwrap(),
                0,
                EngineErrorKind::InvalidArgument,
            ),
        ];
        for (table, shard, expected) in cases {
            let error = engine
                .execute_generated_write(
                    &session,
                    Statement::new(
                        "INSERT INTO native_events (payload) VALUES ('must-not-run')",
                        vec![],
                    ),
                    table,
                    shard,
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind(), expected);
            assert_eq!(session.state().await, SessionState::Ready);
        }

        let cancellation = CancellationToken::new();
        assert!(cancellation.cancel());
        let error = engine
            .execute_generated_write_with_context(
                &session,
                Statement::new(
                    "INSERT INTO native_events (payload) VALUES ('cancelled')",
                    vec![],
                ),
                native_table,
                0,
                RequestContext::new().with_cancellation_token(cancellation),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);

        for shard in 0..engine.shard_count() {
            let connection =
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                0,
                "shard {shard}"
            );
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM ordinary_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                0,
                "shard {shard}"
            );
        }
        assert_eq!(engine.active_operations_for_test(), 0);
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn experimental_vtab_deadline_interrupts_a_locked_write_and_releases_capacity() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap()
            .with_experimental_vtab_writes(true);
        let (temp, engine) = engine_with_sharded_events(4, options);
        let key = integer_key_for_shard(&engine, 1, None);
        let session = engine.session();
        session.set_routing_key(key.to_string()).await.unwrap();

        // Warm registry metadata first so the lock below targets the physical
        // child-open window rather than cold shard-zero discovery.
        let warm_key = integer_key_for_shard(&engine, 0, None);
        session.set_routing_key(warm_key.to_string()).await.unwrap();
        engine
            .execute(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, payload) VALUES (?1, 'warm')",
                    vec![Value::Int64(warm_key)],
                ),
            )
            .await
            .unwrap();
        session.set_routing_key(key.to_string()).await.unwrap();

        let lock = rusqlite::Connection::open(temp.path().join("shards/0001.sqlite")).unwrap();
        lock.execute_batch("PRAGMA locking_mode = EXCLUSIVE; BEGIN EXCLUSIVE")
            .unwrap();
        let context = RequestContext::new()
            .with_timeout(Duration::from_millis(75))
            .unwrap();
        let error = engine
            .execute_with_context(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, payload) VALUES (?1, ?2)",
                    vec![Value::Int64(key), Value::from("cancelled")],
                ),
                context,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
        lock.execute_batch("COMMIT").unwrap();
        drop(lock);

        let shard = engine.pool_snapshot_for_test().unwrap().shards[1];
        assert_eq!(shard.active, 0);
        assert_eq!(shard.queued, 0);
        assert_eq!(engine.active_operations_for_test(), 0);

        let inserted = engine
            .execute(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, payload) VALUES (?1, ?2)",
                    vec![Value::Int64(key), Value::from("recovered")],
                ),
            )
            .await
            .unwrap();
        assert_eq!(inserted.shard, 1);
        assert_eq!(inserted.value, 1);
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn experimental_vtab_engine_preserves_constraint_kinds_and_session_recovery() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 2).unwrap();
        database
            .broadcast(
                "CREATE TABLE parents (
                     tenant_id INTEGER NOT NULL,
                     parent_id INTEGER NOT NULL,
                     label TEXT NOT NULL,
                     PRIMARY KEY (tenant_id, parent_id)
                 );
                 CREATE TABLE items (
                     tenant_id INTEGER NOT NULL,
                     item_id INTEGER NOT NULL,
                     parent_id INTEGER NOT NULL,
                     code TEXT NOT NULL,
                     quantity INTEGER NOT NULL CHECK (quantity > 0),
                     PRIMARY KEY (tenant_id, item_id),
                     UNIQUE (tenant_id, code),
                     FOREIGN KEY (tenant_id, parent_id)
                         REFERENCES parents (tenant_id, parent_id)
                 );",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical_database,
                    "parents",
                    ShardKeyMetadata::new("tenant_id", ShardKeyType::Int64).unwrap(),
                )
                .unwrap(),
                TableDeclaration::sharded(
                    logical_database,
                    "items",
                    ShardKeyMetadata::new("tenant_id", ShardKeyType::Int64).unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
        let key = (1_i64..)
            .find(|key| database.shard_for_key(key.to_string().as_bytes()) == 0)
            .unwrap();
        let options = EngineOptions::default().with_experimental_vtab_writes(true);
        let engine = Engine::from_database_with_options(Arc::new(database), options).unwrap();
        let session = engine.session();
        session.set_routing_key(key.to_string()).await.unwrap();

        assert_eq!(
            engine
                .execute(
                    &session,
                    Statement::new(
                        "INSERT INTO parents (tenant_id, parent_id, label) VALUES (?1, 1, 'parent')",
                        vec![Value::Int64(key)],
                    ),
                )
                .await
                .unwrap()
                .value,
            1
        );
        assert_eq!(
            engine
                .execute(
                    &session,
                    Statement::new(
                        "INSERT INTO items
                         (tenant_id, item_id, parent_id, code, quantity)
                         VALUES (?1, 1, 1, 'first', 5)",
                        vec![Value::Int64(key)],
                    ),
                )
                .await
                .unwrap()
                .value,
            1
        );

        for (sql, expected) in [
            (
                "INSERT INTO items
                 (tenant_id, item_id, parent_id, code, quantity)
                 VALUES (?1, 1, 1, 'duplicate', 5)",
                EngineErrorKind::UniqueViolation,
            ),
            (
                "INSERT INTO items
                 (tenant_id, item_id, parent_id, code, quantity)
                 VALUES (?1, 2, 1, NULL, 5)",
                EngineErrorKind::NotNullViolation,
            ),
            (
                "INSERT INTO items
                 (tenant_id, item_id, parent_id, code, quantity)
                 VALUES (?1, 2, 1, 'bad-check', 0)",
                EngineErrorKind::CheckViolation,
            ),
            (
                "INSERT INTO items
                 (tenant_id, item_id, parent_id, code, quantity)
                 VALUES (?1, 2, 999, 'bad-parent', 5)",
                EngineErrorKind::ForeignKeyViolation,
            ),
        ] {
            let error = engine
                .execute(&session, Statement::new(sql, vec![Value::Int64(key)]))
                .await
                .unwrap_err();
            assert_eq!(error.kind(), expected);
            assert_eq!(session.state().await, SessionState::Ready);
        }

        let recovered = engine
            .execute(
                &session,
                Statement::new(
                    "INSERT INTO items
                     (tenant_id, item_id, parent_id, code, quantity)
                     VALUES (?1, 2, 1, 'second', 5)",
                    vec![Value::Int64(key)],
                ),
            )
            .await
            .unwrap();
        assert_eq!(recovered.value, 1);
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn experimental_vtab_writer_on_another_shard_progresses_while_one_is_locked() {
        let options = EngineOptions::new(1, 2)
            .unwrap()
            .with_request_timeout(None)
            .unwrap()
            .with_experimental_vtab_writes(true);
        let (temp, engine) = engine_with_sharded_events(2, options);
        let key_zero = integer_key_for_shard(&engine, 0, None);
        let key_one = integer_key_for_shard(&engine, 1, None);

        let lock = rusqlite::Connection::open(temp.path().join("shards/0000.sqlite")).unwrap();
        lock.execute_batch("BEGIN IMMEDIATE").unwrap();
        let child_busy_gate = engine.inner.registry_schema_cache.install_child_busy_gate();
        let blocked_engine = engine.clone();
        let blocked = tokio::spawn(async move {
            let session = blocked_engine.session();
            session.set_routing_key(key_zero.to_string()).await.unwrap();
            blocked_engine
                .execute(
                    &session,
                    Statement::new(
                        "INSERT INTO events (tenant_id, payload) VALUES (?1, 'blocked')",
                        vec![Value::Int64(key_zero)],
                    ),
                )
                .await
        });
        let mut child_busy_gate = timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking(move || {
                child_busy_gate.wait_until_started();
                child_busy_gate
            }),
        )
        .await
        .expect("the shard-zero writer must reach a real SQLite busy result")
        .unwrap();

        let independent = engine.session();
        independent
            .set_routing_key(key_one.to_string())
            .await
            .unwrap();
        let inserted = timeout(
            Duration::from_secs(1),
            engine.execute(
                &independent,
                Statement::new(
                    "INSERT INTO events (tenant_id, payload) VALUES (?1, 'independent')",
                    vec![Value::Int64(key_one)],
                ),
            ),
        )
        .await
        .expect("a writer on shard one must not wait for shard zero")
        .unwrap();
        assert_eq!(inserted.shard, 1);
        assert_eq!(inserted.value, 1);

        lock.execute_batch("ROLLBACK").unwrap();
        child_busy_gate.release();
        let released = timeout(Duration::from_secs(2), blocked)
            .await
            .expect("the blocked writer should finish after its shard lock is released")
            .unwrap()
            .unwrap();
        assert_eq!(released.shard, 0);
        assert_eq!(released.value, 1);
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn experimental_vtab_shutdown_cancels_and_drains_an_inflight_coordinator() {
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap()
            .with_shutdown_grace(Duration::from_millis(100))
            .unwrap()
            .with_experimental_vtab_writes(true);
        let (temp, engine) = engine_with_sharded_events(2, options);
        let key = integer_key_for_shard(&engine, 0, None);
        let lock = rusqlite::Connection::open(temp.path().join("shards/0000.sqlite")).unwrap();
        lock.execute_batch("BEGIN IMMEDIATE").unwrap();

        let writer_engine = engine.clone();
        let writer = tokio::spawn(async move {
            let session = writer_engine.session();
            session.set_routing_key(key.to_string()).await.unwrap();
            writer_engine
                .execute(
                    &session,
                    Statement::new(
                        "INSERT INTO events (tenant_id, payload) VALUES (?1, 'shutdown')",
                        vec![Value::Int64(key)],
                    ),
                )
                .await
        });
        timeout(Duration::from_secs(1), async {
            while engine.active_operations_for_test() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the coordinator write should enter Engine lifecycle accounting");

        let report = timeout(Duration::from_secs(2), engine.shutdown())
            .await
            .expect("shutdown should cancel and drain the locked coordinator")
            .unwrap();
        assert!(report.forced());
        let error = writer.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        assert_eq!(engine.active_operations_for_test(), 0);
        assert_eq!(engine.state(), EngineState::Stopped);
        let shard = engine.pool_snapshot_for_test().unwrap().shards[0];
        assert_eq!(shard.active, 0);
        assert_eq!(shard.queued, 0);
        lock.execute_batch("ROLLBACK").unwrap();
    }
}
