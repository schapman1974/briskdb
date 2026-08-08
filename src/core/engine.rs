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
    Database, EngineError, EngineErrorKind, EngineResult, ResultSet, Routed, Session, SessionInner,
    Value,
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
}

impl EngineStatus {
    /// Return the number of physical shards opened by the engine.
    pub const fn shard_count(&self) -> u16 {
        self.shard_count
    }
}

#[derive(Debug)]
struct EngineInner {
    id: u64,
    database: Arc<Database>,
}

/// Shared asynchronous entry point used by every network frontend.
///
/// Clones refer to the same engine identity and database. SQLite remains
/// blocking internally, so operations hand owned work to Tokio blocking
/// workers until the bounded pool abstraction is introduced.
#[derive(Debug, Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

impl Engine {
    /// Open a database without blocking the async runtime executor.
    pub async fn open(root: impl AsRef<Path>, requested_shards: u16) -> EngineResult<Self> {
        let root = PathBuf::from(root.as_ref());
        let database = run_blocking(move || Database::open(root, requested_shards)).await?;
        Ok(Self::from_database(Arc::new(database)))
    }

    /// Wrap the synchronous compatibility API in the shared async boundary.
    pub fn from_database(database: Arc<Database>) -> Self {
        Self {
            inner: Arc::new(EngineInner {
                id: NEXT_ENGINE_ID.fetch_add(1, Ordering::Relaxed),
                database,
            }),
        }
    }

    /// Create a new frontend-owned session.
    pub fn session(&self) -> Session {
        Session::new(self.inner.id)
    }

    /// Return the configured physical shard count.
    pub fn shard_count(&self) -> u16 {
        self.inner.database.shard_count()
    }

    /// Return engine status after validating the calling session.
    pub async fn status(&self, session: &Session) -> EngineResult<EngineStatus> {
        let _guard = self.ready_session(session).await?;
        Ok(EngineStatus {
            shard_count: self.shard_count(),
        })
    }

    /// Execute a routed statement and return its selected shard.
    pub async fn execute(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Routed<usize>> {
        let guard = self.ready_session(session).await?;
        let routing_key = required_routing_key(&guard)?.to_owned();
        let database = Arc::clone(&self.inner.database);
        let (sql, params) = statement.into_parts();

        run_blocking(move || {
            let _guard = guard;
            database.execute_routed(&routing_key, &sql, &params)
        })
        .await
    }

    /// Query a routed statement and return its selected shard and rows.
    pub async fn query(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Routed<ResultSet>> {
        let guard = self.ready_session(session).await?;
        let routing_key = required_routing_key(&guard)?.to_owned();
        let database = Arc::clone(&self.inner.database);
        let (sql, params) = statement.into_parts();

        run_blocking(move || {
            let _guard = guard;
            database.query_routed(&routing_key, &sql, &params)
        })
        .await
    }

    /// Execute a parameterless SQL batch sequentially on every shard.
    pub async fn broadcast(&self, session: &Session, sql: String) -> EngineResult<Vec<u16>> {
        let guard = self.ready_session(session).await?;
        let database = Arc::clone(&self.inner.database);

        run_blocking(move || {
            let _guard = guard;
            database.broadcast(&sql)
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
        started: tokio::sync::oneshot::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> EngineResult<()> {
        let guard = self.ready_session(session).await?;
        run_blocking(move || {
            let _guard = guard;
            let _ = started.send(());
            release.recv().expect("test releases the blocking worker");
            Ok(())
        })
        .await
    }

    #[cfg(test)]
    async fn panic_worker_for_test(&self, session: &Session) -> EngineResult<()> {
        let guard = self.ready_session(session).await?;
        run_blocking(move || {
            let _guard = guard;
            panic!("intentional blocking worker panic")
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

async fn run_blocking<T, F>(work: F) -> EngineResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> EngineResult<T> + Send + 'static,
{
    tokio::task::spawn_blocking(work).await.map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::Internal,
            "blocking engine task failed",
            error,
        )
    })?
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
        assert_eq!(engine.status(&session).await.unwrap().shard_count(), 4);
        assert_eq!(session.state().await, SessionState::Ready);
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
    async fn a_worker_panic_is_internal_and_releases_the_session() {
        let (_temp, engine) = engine();
        let session = engine.session();

        let error = engine.panic_worker_for_test(&session).await.unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert_eq!(error.to_string(), "blocking engine task failed");
        assert!(error.source().is_some());
        assert_eq!(engine.status(&session).await.unwrap().shard_count(), 4);
        assert_eq!(session.state().await, SessionState::Ready);
    }

    #[tokio::test]
    async fn same_session_calls_wait_without_starting_another_blocking_worker() {
        let (_temp, engine) = engine();
        let session = Arc::new(engine.session());
        let (first_started_tx, first_started_rx) = oneshot::channel();
        let (first_release_tx, first_release_rx) = mpsc::channel();
        let first_engine = engine.clone();
        let first_session = Arc::clone(&session);
        let first = tokio::spawn(async move {
            first_engine
                .hold_session_for_test(&first_session, first_started_tx, first_release_rx)
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
                .hold_session_for_test(&second_session, second_started_tx, second_release_rx)
                .await
        });

        assert!(
            timeout(Duration::from_millis(50), &mut second_started_rx)
                .await
                .is_err()
        );
        first_release_tx.send(()).unwrap();
        timeout(Duration::from_secs(2), &mut second_started_rx)
            .await
            .unwrap()
            .unwrap();
        second_release_tx.send(()).unwrap();

        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn aborting_an_outer_future_does_not_release_an_in_flight_session() {
        let (_temp, engine) = engine();
        let session = Arc::new(engine.session());
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker_engine = engine.clone();
        let worker_session = Arc::clone(&session);
        let worker = tokio::spawn(async move {
            worker_engine
                .hold_session_for_test(&worker_session, started_tx, release_rx)
                .await
        });
        timeout(Duration::from_secs(2), started_rx)
            .await
            .unwrap()
            .unwrap();
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());

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
            4
        );
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
                .hold_session_for_test(&first_session_for_task, first_started_tx, first_release_rx)
                .await
        });
        let second_engine = engine.clone();
        let second_session_for_task = Arc::clone(&second_session);
        let second = tokio::spawn(async move {
            second_engine
                .hold_session_for_test(
                    &second_session_for_task,
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
