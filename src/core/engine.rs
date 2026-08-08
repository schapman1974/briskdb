//! Asynchronous protocol-neutral engine boundary.

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::{sync::OwnedMutexGuard, task::JoinHandle};

use super::{
    BlockingPool, CancelOnDrop, CancellationReason, CancellationToken, Database, EngineError,
    EngineErrorKind, EngineOptions, EngineResult, EngineState, Lifecycle, OperationControl,
    OperationLease, RequestContext, ResultLimits, ResultSet, Routed, Session, SessionInner,
    ShutdownReport, Value, wait_for_cancellation, wait_pending,
};
use crate::{
    sql,
    storage::{ConnectionOwner, ConnectionPools, PooledConnection, SchemaOperationGuard},
};

static NEXT_ENGINE_ID: AtomicU64 = AtomicU64::new(1);

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
    request_timeout: Option<Duration>,
    shutdown_grace: Duration,
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
            }),
        })
    }

    /// Create a new frontend-owned session.
    pub fn session(&self) -> Session {
        Session::new(self.inner.id)
    }

    /// Return the configured physical shard count.
    pub fn shard_count(&self) -> u16 {
        self.inner.database.shard_count()
    }

    /// Return the immutable logical database and table catalog.
    pub fn catalog(&self) -> &super::Catalog {
        self.inner.database.catalog()
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
        let result = async {
            let _guard = operation.wait_pending(self.ready_session(session)).await?;
            operation.check_before_start()?;
            let result_limits = self.inner.options.result_limits();
            Ok(EngineStatus {
                shard_count: self.shard_count(),
                max_blocking_workers: self.inner.workers.limit(),
                connections_per_shard: self.inner.options.connections_per_shard(),
                queue_capacity_per_shard: self.inner.options.queue_capacity_per_shard(),
                max_result_rows: result_limits.max_rows(),
                max_result_bytes: result_limits.max_bytes(),
                request_timeout: self.inner.options.request_timeout(),
                shutdown_grace: self.inner.options.shutdown_grace(),
            })
        }
        .await;
        operation.finish(result)
    }

    /// Execute a routed statement and return its selected shard.
    pub async fn execute(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Routed<usize>> {
        self.execute_with_context(session, statement, RequestContext::new())
            .await
    }

    /// Execute a routed statement with explicit request controls.
    pub async fn execute_with_context(
        &self,
        session: &Session,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Routed<usize>> {
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
        let shard = self.inner.database.shard_for_key(routing_key.as_bytes());
        let (sql, params) = statement.into_parts();

        let value = self
            .run_on_shard(
                &mut operation,
                shard,
                owner,
                schema_operation,
                guard,
                move |connection, control| {
                    connection.isolate_foreign_sql_controlled(Arc::clone(&control), &sql)?;
                    connection.run_controlled(control, |connection| {
                        sql::execute(connection, &sql, &params)
                    })
                },
            )
            .await;
        let value = operation.finish_started(value)?;
        Ok(Routed { shard, value })
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
        let shard = self.inner.database.shard_for_key(routing_key.as_bytes());
        let (sql, params) = statement.into_parts();
        let limits = operation.result_limits;

        let value = self
            .run_on_shard(
                &mut operation,
                shard,
                owner,
                schema_operation,
                guard,
                move |connection, control| {
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

    async fn run_on_shard<T, F>(
        &self,
        operation: &mut Operation,
        shard: u16,
        owner: ConnectionOwner,
        schema_operation: SchemaOperationGuard,
        session: OwnedMutexGuard<SessionInner>,
        work: F,
    ) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut PooledConnection, Arc<OperationControl>) -> EngineResult<T> + Send + 'static,
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
        let join = worker.spawn(move || {
            let _lease = lease;
            let _session = session;
            let _schema_operation = schema_operation;
            let result = permit
                .checkout_controlled(Arc::clone(&worker_control))
                .and_then(|mut connection| {
                    let result = work(&mut connection, Arc::clone(&worker_control));
                    retire_if_broken(&mut connection, &result);
                    result
                });
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
        guard.ensure_ready()?;
        Ok(guard)
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
                move |_, _| {
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
                move |_, _| panic!("intentional blocking worker panic"),
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
                move |connection, control| {
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
                move |connection, _| Ok(connection.connection_id()),
            )
            .await;
        operation.finish_started(result)
    }
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
    use crate::core::{Column, DataType, Row, SessionState};

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

    fn routing_key_for_shard(engine: &Engine, expected: u16) -> String {
        (0_u64..)
            .map(|value| format!("shard-{value}"))
            .find(|key| engine.inner.database.shard_for_key(key.as_bytes()) == expected)
            .expect("the finite shard layout has a routing key")
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
        assert_eq!(session.state().await, SessionState::Ready);
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
                    Column::new("id", DataType::Unknown),
                    Column::new("name", DataType::Unknown),
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
}
