//! Asynchronous protocol-neutral engine boundary.

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::sync::OwnedMutexGuard;

use super::{
    BlockingPool, Database, EngineError, EngineErrorKind, EngineOptions, EngineResult, ResultSet,
    Routed, Session, SessionInner, Value,
};
use crate::{
    sql,
    storage::{ConnectionOwner, ConnectionPools, PooledConnection},
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
}

#[derive(Debug)]
struct EngineInner {
    id: u64,
    database: Arc<Database>,
    options: EngineOptions,
    workers: BlockingPool,
    connections: ConnectionPools,
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

    /// Return the engine's immutable pool and admission options.
    pub fn options(&self) -> EngineOptions {
        self.inner.options
    }

    /// Return engine status after validating the calling session.
    pub async fn status(&self, session: &Session) -> EngineResult<EngineStatus> {
        let _guard = self.ready_session(session).await?;
        Ok(EngineStatus {
            shard_count: self.shard_count(),
            max_blocking_workers: self.inner.workers.limit(),
            connections_per_shard: self.inner.options.connections_per_shard(),
            queue_capacity_per_shard: self.inner.options.queue_capacity_per_shard(),
        })
    }

    /// Execute a routed statement and return its selected shard.
    pub async fn execute(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Routed<usize>> {
        let guard = self.ready_session(session).await?;
        let owner = ConnectionOwner::new(session.id().get());
        let routing_key = required_routing_key(&guard)?.to_owned();
        let shard = self.inner.database.shard_for_key(routing_key.as_bytes());
        let (sql, params) = statement.into_parts();

        let value = self
            .run_on_shard(shard, owner, guard, move |connection| {
                connection.isolate_foreign_sql(&sql)?;
                sql::execute(connection, &sql, &params)
            })
            .await?;
        Ok(Routed { shard, value })
    }

    /// Query a routed statement and return its selected shard and rows.
    pub async fn query(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Routed<ResultSet>> {
        let guard = self.ready_session(session).await?;
        let owner = ConnectionOwner::new(session.id().get());
        let routing_key = required_routing_key(&guard)?.to_owned();
        let shard = self.inner.database.shard_for_key(routing_key.as_bytes());
        let (sql, params) = statement.into_parts();

        let value = self
            .run_on_shard(shard, owner, guard, move |connection| {
                connection.isolate_foreign_sql(&sql)?;
                sql::query(connection, &sql, &params)
            })
            .await?;
        Ok(Routed { shard, value })
    }

    /// Execute a parameterless SQL batch sequentially on every shard.
    pub async fn broadcast(&self, session: &Session, sql: String) -> EngineResult<Vec<u16>> {
        let guard = self.ready_session(session).await?;
        let owner = ConnectionOwner::new(session.id().get());
        let permits = self.inner.connections.acquire_all_for_owner(owner).await?;
        let workers = self.inner.workers.clone();

        workers
            .run(move || {
                let _guard = guard;
                let mut completed = Vec::with_capacity(permits.len());
                for (shard, permit) in permits {
                    let mut connection = permit.checkout().map_err(|error| {
                        error.context(format!("broadcast failed to open shard {shard}"))
                    })?;
                    connection.ensure_owner_local().map_err(|error| {
                        error.context(format!("broadcast failed to isolate shard {shard}"))
                    })?;
                    let result = sql::execute_batch(&connection, &sql);
                    retire_if_broken(&mut connection, &result);
                    result.map_err(|error| {
                        error.context(format!("broadcast failed on shard {shard}"))
                    })?;
                    completed.push(shard);
                }
                Ok(completed)
            })
            .await
    }

    async fn run_on_shard<T, F>(
        &self,
        shard: u16,
        owner: ConnectionOwner,
        session: OwnedMutexGuard<SessionInner>,
        work: F,
    ) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut PooledConnection) -> EngineResult<T> + Send + 'static,
    {
        let permit = self
            .inner
            .connections
            .acquire_for_owner(shard, owner)
            .await?;
        self.inner
            .workers
            .run(move || {
                let _session = session;
                let mut connection = permit.checkout()?;
                let result = work(&mut connection);
                retire_if_broken(&mut connection, &result);
                result
            })
            .await
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
        let guard = self.ready_session(session).await?;
        let owner = ConnectionOwner::new(session.id().get());
        self.run_on_shard(shard, owner, guard, move |_| {
            let _ = started.send(());
            release.recv().expect("test releases the blocking worker");
            Ok(())
        })
        .await
    }

    #[cfg(test)]
    async fn panic_worker_for_test(&self, session: &Session, shard: u16) -> EngineResult<()> {
        let guard = self.ready_session(session).await?;
        let owner = ConnectionOwner::new(session.id().get());
        self.run_on_shard(shard, owner, guard, move |_| {
            panic!("intentional blocking worker panic")
        })
        .await
    }

    #[cfg(test)]
    async fn connection_id_for_test(&self, session: &Session, shard: u16) -> EngineResult<u64> {
        let guard = self.ready_session(session).await?;
        let owner = ConnectionOwner::new(session.id().get());
        self.run_on_shard(shard, owner, guard, move |connection| {
            Ok(connection.connection_id())
        })
        .await
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
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), shards).unwrap());
        let options = EngineOptions::new(connections_per_shard, queue_capacity_per_shard).unwrap();
        let engine = Engine::from_database_with_options(database, options).unwrap();
        (temp, engine)
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
            assert_eq!(shard.opened, 1);
            assert_eq!(shard.idle, 1);
            if shard.shard == write.shard {
                assert_eq!(shard.checkouts, 3);
                assert_eq!(shard.reused, 2);
            } else {
                assert_eq!(shard.checkouts, 1);
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
    async fn partial_broadcast_failure_can_recover_through_the_engine() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 4).unwrap());
        let shard_one = database.storage.open_shard(1).unwrap();
        crate::sql::execute_batch(&shard_one, "CREATE TABLE marker (id INTEGER)").unwrap();
        let engine = Engine::from_database(database);
        let session = engine.session();

        let error = engine
            .broadcast(&session, "CREATE TABLE marker (id INTEGER)".to_owned())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
        assert!(error.source().is_some());
        assert_eq!(session.state().await, SessionState::Ready);

        assert_eq!(
            engine
                .broadcast(
                    &session,
                    "CREATE TABLE IF NOT EXISTS marker (id INTEGER)".to_owned(),
                )
                .await
                .unwrap(),
            [0, 1, 2, 3]
        );
    }

    #[tokio::test]
    async fn concurrent_broadcasts_use_ordered_pool_acquisition_without_deadlock() {
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
        wait_for_pool_occupancy(&engine, 0, 1, 0).await;
        wait_for_pool_occupancy(&engine, 1, 1, 1).await;

        let second_session = Arc::new(engine.session());
        let second_engine = engine.clone();
        let second_session_for_task = Arc::clone(&second_session);
        let second = tokio::spawn(async move {
            second_engine
                .broadcast(
                    &second_session_for_task,
                    "CREATE TABLE IF NOT EXISTS broadcast_marker (id INTEGER)".to_owned(),
                )
                .await
        });
        wait_for_pool_occupancy(&engine, 0, 1, 1).await;
        assert_eq!(engine.inner.workers.available_permits(), 1);

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
            timeout(Duration::from_secs(2), second)
                .await
                .expect("second ordered broadcast should complete")
                .unwrap()
                .unwrap(),
            [0, 1]
        );

        for shard in 0..2 {
            wait_for_pool_occupancy(&engine, shard, 0, 0).await;
        }
        assert_eq!(engine.inner.workers.available_permits(), 2);
    }

    #[tokio::test]
    async fn aborting_a_dispatched_broadcast_does_not_stop_later_shards() {
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

        let shard_zero = engine.inner.database.storage.open_shard(0).unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                let exists = shard_zero
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'abort_marker')",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap();
                if exists {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("broadcast should complete shard zero before blocking on shard one");

        broadcast.abort();
        assert!(broadcast.await.unwrap_err().is_cancelled());
        assert_eq!(
            engine.inner.connections.snapshot().unwrap().shards[1].active,
            1
        );
        assert_eq!(engine.inner.workers.available_permits(), 1);

        blocker.execute_batch("COMMIT").unwrap();
        let status = timeout(Duration::from_secs(2), engine.status(&session))
            .await
            .expect("detached broadcast should release its session")
            .unwrap();
        assert_eq!(status.shard_count(), 2);
        assert!(
            blocker
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'abort_marker')",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        );
        for shard in 0..2 {
            wait_for_pool_occupancy(&engine, shard, 0, 0).await;
        }
        assert_eq!(engine.inner.workers.available_permits(), 2);
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
                Statement::new(
                    "CREATE TABLE data_version_marker (id INTEGER PRIMARY KEY)",
                    vec![],
                ),
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
    async fn foreign_owner_broadcast_batches_execute_once_without_statement_preflight() {
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
        assert_eq!(snapshot.opened, 3);
        assert_eq!(snapshot.retired, 2);
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
        assert_eq!(engine.inner.workers.available_permits(), 2);
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
}
