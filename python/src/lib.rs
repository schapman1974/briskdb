mod error;
mod value;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use briskdb::{
    BriskDb, BriskSession, CheckpointReport, EngineOptions, EngineState, EngineStatus,
    PreparedStatementLimits, ResultLimits, SessionState, Statement,
};
use pyo3::{
    prelude::*,
    types::{PyDict, PyModule},
};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

use crate::{
    error::{NativeError, NativeResult, run_native},
    value::{
        extract_params, logical_result_to_python, routed_result_to_python, write_result_to_python,
    },
};

struct RuntimeOwner {
    runtime: Runtime,
}

struct DatabaseShared {
    database: Mutex<Option<BriskDb>>,
    runtime: Arc<RuntimeOwner>,
    root: PathBuf,
    config: Config,
}

impl DatabaseShared {
    fn database(&self) -> NativeResult<BriskDb> {
        self.database
            .lock()?
            .clone()
            .ok_or(NativeError::Closed("database"))
    }
}

impl Drop for DatabaseShared {
    fn drop(&mut self) {
        if let Ok(slot) = self.database.get_mut() {
            if let Some(database) = slot.take() {
                database.begin_close();
            }
        }
    }
}

#[pyclass(module = "briskdb._briskdb", frozen, get_all, skip_from_py_object)]
#[derive(Clone, Debug)]
struct Config {
    shards: u16,
    connections_per_shard: usize,
    queue_capacity_per_shard: usize,
    max_result_rows: u64,
    max_result_bytes: u64,
    max_prepared_statements_per_session: usize,
    max_portals_per_session: usize,
    max_retained_bound_value_bytes: u64,
    request_timeout_ms: u64,
    shutdown_grace_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            shards: briskdb::DEFAULT_EMBEDDED_SHARDS,
            connections_per_shard: briskdb::core::DEFAULT_CONNECTIONS_PER_SHARD,
            queue_capacity_per_shard: briskdb::core::DEFAULT_QUEUE_CAPACITY_PER_SHARD,
            max_result_rows: briskdb::core::DEFAULT_MAX_RESULT_ROWS,
            max_result_bytes: briskdb::core::DEFAULT_MAX_RESULT_BYTES,
            max_prepared_statements_per_session:
                briskdb::core::DEFAULT_MAX_PREPARED_STATEMENTS_PER_SESSION,
            max_portals_per_session: briskdb::core::DEFAULT_MAX_PORTALS_PER_SESSION,
            max_retained_bound_value_bytes: briskdb::core::DEFAULT_MAX_RETAINED_BOUND_VALUE_BYTES,
            request_timeout_ms: briskdb::core::DEFAULT_REQUEST_TIMEOUT_MS,
            shutdown_grace_ms: briskdb::core::DEFAULT_SHUTDOWN_GRACE_MS,
        }
    }
}

impl Config {
    fn engine_options(&self) -> NativeResult<EngineOptions> {
        let result_limits = ResultLimits::new(self.max_result_rows, self.max_result_bytes)?;
        let prepared_statement_limits = PreparedStatementLimits::new(
            self.max_prepared_statements_per_session,
            self.max_portals_per_session,
            self.max_retained_bound_value_bytes,
        )?;
        let request_timeout =
            (self.request_timeout_ms != 0).then(|| Duration::from_millis(self.request_timeout_ms));
        let options =
            EngineOptions::new(self.connections_per_shard, self.queue_capacity_per_shard)?
                .with_result_limits(result_limits)
                .with_prepared_statement_limits(prepared_statement_limits)
                .with_request_timeout(request_timeout)?
                .with_shutdown_grace(Duration::from_millis(self.shutdown_grace_ms))?;
        options.validate_for_shards(self.shards)?;
        Ok(options)
    }
}

#[pymethods]
impl Config {
    #[new]
    #[pyo3(signature = (
        *,
        shards = briskdb::DEFAULT_EMBEDDED_SHARDS,
        connections_per_shard = briskdb::core::DEFAULT_CONNECTIONS_PER_SHARD,
        queue_capacity_per_shard = briskdb::core::DEFAULT_QUEUE_CAPACITY_PER_SHARD,
        max_result_rows = briskdb::core::DEFAULT_MAX_RESULT_ROWS,
        max_result_bytes = briskdb::core::DEFAULT_MAX_RESULT_BYTES,
        max_prepared_statements_per_session = briskdb::core::DEFAULT_MAX_PREPARED_STATEMENTS_PER_SESSION,
        max_portals_per_session = briskdb::core::DEFAULT_MAX_PORTALS_PER_SESSION,
        max_retained_bound_value_bytes = briskdb::core::DEFAULT_MAX_RETAINED_BOUND_VALUE_BYTES,
        request_timeout_ms = briskdb::core::DEFAULT_REQUEST_TIMEOUT_MS,
        shutdown_grace_ms = briskdb::core::DEFAULT_SHUTDOWN_GRACE_MS,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        shards: u16,
        connections_per_shard: usize,
        queue_capacity_per_shard: usize,
        max_result_rows: u64,
        max_result_bytes: u64,
        max_prepared_statements_per_session: usize,
        max_portals_per_session: usize,
        max_retained_bound_value_bytes: u64,
        request_timeout_ms: u64,
        shutdown_grace_ms: u64,
    ) -> PyResult<Self> {
        let config = Self {
            shards,
            connections_per_shard,
            queue_capacity_per_shard,
            max_result_rows,
            max_result_bytes,
            max_prepared_statements_per_session,
            max_portals_per_session,
            max_retained_bound_value_bytes,
            request_timeout_ms,
            shutdown_grace_ms,
        };
        config.engine_options()?;
        Ok(config)
    }

    fn __repr__(&self) -> String {
        format!(
            "Config(shards={}, connections_per_shard={}, queue_capacity_per_shard={})",
            self.shards, self.connections_per_shard, self.queue_capacity_per_shard
        )
    }
}

struct SessionShared {
    session: Mutex<Option<BriskSession>>,
    runtime: Arc<RuntimeOwner>,
}

impl SessionShared {
    fn session(&self) -> NativeResult<BriskSession> {
        self.session
            .lock()?
            .clone()
            .ok_or(NativeError::Closed("session"))
    }
}

#[pyclass(module = "briskdb._briskdb", frozen)]
struct Database {
    shared: Arc<DatabaseShared>,
}

impl Database {
    fn create(py: Python<'_>, root: PathBuf, config: Config) -> PyResult<Self> {
        run_native(py, move || {
            let engine_options = config.engine_options()?;
            let runtime = RuntimeBuilder::new_multi_thread()
                .enable_all()
                .thread_name("briskdb-python")
                .build()
                .map_err(|error| NativeError::Runtime(error.to_string()))?;
            let database = runtime.block_on(
                BriskDb::builder(&root)
                    .with_shard_count(config.shards)
                    .with_engine_options(engine_options)
                    .open(),
            )?;
            let runtime = Arc::new(RuntimeOwner { runtime });
            Ok(Self {
                shared: Arc::new(DatabaseShared {
                    database: Mutex::new(Some(database)),
                    runtime,
                    root,
                    config,
                }),
            })
        })
    }
}

#[pymethods]
impl Database {
    #[new]
    #[pyo3(signature = (path, *, shards = None, config = None))]
    fn new(
        py: Python<'_>,
        path: PathBuf,
        shards: Option<u16>,
        config: Option<PyRef<'_, Config>>,
    ) -> PyResult<Self> {
        let config = resolve_config(shards, config.as_deref())?;
        Self::create(py, path, config)
    }

    #[getter]
    fn path(&self) -> PathBuf {
        self.shared.root.clone()
    }

    #[getter]
    fn shard_count(&self) -> u16 {
        self.shared.config.shards
    }

    #[getter]
    fn config(&self) -> Config {
        self.shared.config.clone()
    }

    #[getter]
    fn closed(&self) -> PyResult<bool> {
        Ok(self
            .shared
            .database
            .lock()
            .map_err(NativeError::from)?
            .is_none())
    }

    #[getter]
    fn state(&self) -> PyResult<&'static str> {
        let state = self
            .shared
            .database
            .lock()
            .map_err(NativeError::from)?
            .as_ref()
            .map_or(EngineState::Stopped, BriskDb::state);
        Ok(engine_state_name(state))
    }

    #[pyo3(signature = (*, routing_key = None))]
    fn session(&self, py: Python<'_>, routing_key: Option<String>) -> PyResult<Session> {
        let shared = Arc::clone(&self.shared);
        run_native(py, move || {
            let session = shared.database()?.owned_session();
            if let Some(routing_key) = routing_key {
                shared
                    .runtime
                    .runtime
                    .block_on(session.set_routing_key(routing_key))?;
            }
            Ok(Session {
                shared: Arc::new(SessionShared {
                    session: Mutex::new(Some(session)),
                    runtime: Arc::clone(&shared.runtime),
                }),
            })
        })
    }

    fn checkpoint(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let shared = Arc::clone(&self.shared);
        let report = run_native(py, move || {
            let database = shared.database()?;
            Ok(shared.runtime.runtime.block_on(database.checkpoint())?)
        })?;
        checkpoint_to_python(py, report)
    }

    fn close(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let shared = Arc::clone(&self.shared);
        let report = run_native(py, move || {
            let database = shared.database.lock()?.take();
            match database {
                Some(database) => Ok(Some(shared.runtime.runtime.block_on(database.close())?)),
                None => Ok(None),
            }
        })?;
        let output = PyDict::new(py);
        output.set_item("already_closed", report.is_none())?;
        output.set_item("forced", report.is_some_and(|report| report.forced()))?;
        Ok(output.into_any().unbind())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "Database(path={:?}, shards={}, state={:?})",
            self.shared.root,
            self.shared.config.shards,
            self.state()?
        ))
    }
}

#[pyclass(module = "briskdb._briskdb", frozen)]
struct Session {
    shared: Arc<SessionShared>,
}

#[pymethods]
impl Session {
    #[getter]
    fn closed(&self) -> PyResult<bool> {
        Ok(self
            .shared
            .session
            .lock()
            .map_err(NativeError::from)?
            .is_none())
    }

    #[getter]
    fn state(&self, py: Python<'_>) -> PyResult<&'static str> {
        let shared = Arc::clone(&self.shared);
        run_native(py, move || {
            let session = match shared.session.lock()?.clone() {
                Some(session) => session,
                None => return Ok("closed"),
            };
            let state = shared.runtime.runtime.block_on(session.state());
            Ok(session_state_name(state))
        })
    }

    #[getter]
    fn database_state(&self) -> PyResult<&'static str> {
        let session = self.shared.session()?;
        Ok(engine_state_name(session.database_state()))
    }

    #[getter]
    fn routing_key(&self, py: Python<'_>) -> PyResult<Option<String>> {
        let shared = Arc::clone(&self.shared);
        run_native(py, move || {
            let session = shared.session()?;
            Ok(shared.runtime.runtime.block_on(session.routing_key()))
        })
    }

    fn set_routing_key(&self, py: Python<'_>, routing_key: String) -> PyResult<()> {
        let shared = Arc::clone(&self.shared);
        run_native(py, move || {
            let session = shared.session()?;
            Ok(shared
                .runtime
                .runtime
                .block_on(session.set_routing_key(routing_key))?)
        })
    }

    fn clear_routing_key(&self, py: Python<'_>) -> PyResult<()> {
        let shared = Arc::clone(&self.shared);
        run_native(py, move || {
            let session = shared.session()?;
            Ok(shared
                .runtime
                .runtime
                .block_on(session.clear_routing_key())?)
        })
    }

    fn migrate(&self, py: Python<'_>, sql: String) -> PyResult<Vec<u16>> {
        let shared = Arc::clone(&self.shared);
        run_native(py, move || {
            let session = shared.session()?;
            Ok(shared.runtime.runtime.block_on(session.migrate(sql))?)
        })
    }

    #[pyo3(signature = (sql, params = None))]
    fn execute(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<Vec<Py<PyAny>>>,
    ) -> PyResult<Py<PyAny>> {
        let params = extract_params(py, params)?;
        let shared = Arc::clone(&self.shared);
        let result = run_native(py, move || {
            let session = shared.session()?;
            Ok(shared
                .runtime
                .runtime
                .block_on(session.execute_write(Statement::new(sql, params)))?)
        })?;
        write_result_to_python(py, result)
    }

    #[pyo3(signature = (sql, params = None))]
    fn query(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<Vec<Py<PyAny>>>,
    ) -> PyResult<Py<PyAny>> {
        let params = extract_params(py, params)?;
        let shared = Arc::clone(&self.shared);
        let result = run_native(py, move || {
            let session = shared.session()?;
            Ok(shared
                .runtime
                .runtime
                .block_on(session.query(Statement::new(sql, params)))?)
        })?;
        routed_result_to_python(py, result)
    }

    #[pyo3(signature = (sql, params = None))]
    fn query_logical(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<Vec<Py<PyAny>>>,
    ) -> PyResult<Py<PyAny>> {
        let params = extract_params(py, params)?;
        let shared = Arc::clone(&self.shared);
        let result = run_native(py, move || {
            let session = shared.session()?;
            Ok(shared
                .runtime
                .runtime
                .block_on(session.query_logical(Statement::new(sql, params)))?)
        })?;
        logical_result_to_python(py, result)
    }

    fn status(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let shared = Arc::clone(&self.shared);
        let status = run_native(py, move || {
            let session = shared.session()?;
            Ok(shared.runtime.runtime.block_on(session.status())?)
        })?;
        status_to_python(py, status)
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let shared = Arc::clone(&self.shared);
        run_native(py, move || {
            let session = shared.session.lock()?.take();
            if let Some(session) = session {
                shared.runtime.runtime.block_on(session.close())?;
            }
            Ok(())
        })
    }

    fn __repr__(&self) -> PyResult<String> {
        let state = if self.closed()? { "closed" } else { "ready" };
        Ok(format!("Session(state={state:?})"))
    }
}

#[pyfunction(name = "open", signature = (path, *, shards = None, config = None))]
fn open_database(
    py: Python<'_>,
    path: PathBuf,
    shards: Option<u16>,
    config: Option<PyRef<'_, Config>>,
) -> PyResult<Database> {
    let config = resolve_config(shards, config.as_deref())?;
    Database::create(py, path, config)
}

fn resolve_config(shards: Option<u16>, config: Option<&Config>) -> PyResult<Config> {
    match (shards, config) {
        (Some(_), Some(_)) => Err(crate::error::invalid_value(
            "pass either shards or config to open a database, not both",
        )),
        (Some(shards), None) => {
            let config = Config {
                shards,
                ..Config::default()
            };
            config.engine_options()?;
            Ok(config)
        }
        (None, Some(config)) => Ok(config.clone()),
        (None, None) => Ok(Config::default()),
    }
}

fn engine_state_name(state: EngineState) -> &'static str {
    match state {
        EngineState::Running => "running",
        EngineState::Draining => "draining",
        EngineState::Stopped => "stopped",
        _ => "unknown",
    }
}

fn session_state_name(state: SessionState) -> &'static str {
    match state {
        SessionState::Ready => "ready",
        SessionState::Closed => "closed",
        _ => "unknown",
    }
}

fn status_to_python(py: Python<'_>, status: EngineStatus) -> PyResult<Py<PyAny>> {
    let output = PyDict::new(py);
    output.set_item("shards", status.shard_count())?;
    output.set_item("max_blocking_workers", status.max_blocking_workers())?;
    output.set_item("connections_per_shard", status.connections_per_shard())?;
    output.set_item(
        "queue_capacity_per_shard",
        status.queue_capacity_per_shard(),
    )?;
    output.set_item("max_result_rows", status.max_result_rows())?;
    output.set_item("max_result_bytes", status.max_result_bytes())?;
    output.set_item(
        "request_timeout_ms",
        status
            .request_timeout()
            .map(|timeout| u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)),
    )?;
    output.set_item(
        "shutdown_grace_ms",
        u64::try_from(status.shutdown_grace().as_millis()).unwrap_or(u64::MAX),
    )?;
    Ok(output.into_any().unbind())
}

fn checkpoint_to_python(py: Python<'_>, report: CheckpointReport) -> PyResult<Py<PyAny>> {
    let output = PyDict::new(py);
    output.set_item("busy", report.busy())?;
    output.set_item("complete", report.complete())?;
    let shards = report
        .shards()
        .iter()
        .map(|shard| {
            let item = PyDict::new(py);
            item.set_item("shard", shard.shard())?;
            item.set_item("busy", shard.busy())?;
            item.set_item("wal_frames", shard.wal_frames())?;
            item.set_item("checkpointed_frames", shard.checkpointed_frames())?;
            item.set_item("complete", shard.complete())?;
            Ok(item.into_any().unbind())
        })
        .collect::<PyResult<Vec<_>>>()?;
    output.set_item("shards", shards)?;
    Ok(output.into_any().unbind())
}

#[pymodule]
fn _briskdb(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Config>()?;
    module.add_class::<Database>()?;
    module.add_class::<Session>()?;
    module.add_function(wrap_pyfunction!(open_database, module)?)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
