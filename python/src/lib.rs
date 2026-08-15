mod error;
mod value;

use std::{
    collections::VecDeque,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use briskdb::{
    BriskDb, BriskSession, CancellationToken as EngineCancellationToken, CheckpointReport, Column,
    EngineOptions, EngineState, EngineStatus, Executed, PreparedStatementLimits, RequestContext,
    ResultLimits, ResultSet, Routed, SessionState, Statement, Value,
    protocol::postgres::SecurityConfig as PostgresSecurityConfig,
    server::{AttachedServer, ListenerAddresses, ListenerConfig},
};
use pyo3::{
    prelude::*,
    types::{PyAny, PyDict, PyList, PyModule, PyTuple},
};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

use crate::{
    error::{NativeError, NativeResult, listener_error, run_native},
    value::{
        data_type_name, extract_params, logical_result_to_python, routed_result_to_python,
        value_to_python, write_result_to_python,
    },
};

struct RuntimeOwner {
    runtime: Runtime,
}

struct ServerShared {
    server: Mutex<Option<AttachedServer>>,
    runtime: Arc<RuntimeOwner>,
    addresses: ListenerAddresses,
}

impl ServerShared {
    fn begin_close(&self) -> NativeResult<()> {
        if let Some(server) = self.server.lock()?.as_mut() {
            server.begin_close();
        }
        Ok(())
    }

    fn close_native(&self) -> NativeResult<bool> {
        let server = self.server.lock()?.take();
        let Some(mut server) = server else {
            return Ok(true);
        };
        self.runtime
            .runtime
            .block_on(server.close())
            .map_err(listener_error)
    }

    fn is_closed(&self) -> NativeResult<bool> {
        Ok(self
            .server
            .lock()?
            .as_ref()
            .is_none_or(AttachedServer::is_closed))
    }
}

impl Drop for ServerShared {
    fn drop(&mut self) {
        if let Ok(slot) = self.server.get_mut() {
            if let Some(server) = slot.as_mut() {
                server.begin_close();
            }
        }
    }
}

struct DatabaseShared {
    database: Mutex<Option<BriskDb>>,
    servers: Mutex<Vec<Weak<ServerShared>>>,
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
        if let Ok(servers) = self.servers.get_mut() {
            for server in servers.iter().filter_map(Weak::upgrade) {
                let _ = server.begin_close();
            }
        }
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
    shards: Option<u16>,
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
            shards: None,
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
        if let Some(shards) = self.shards {
            options.validate_for_shards(shards)?;
        }
        Ok(options)
    }
}

#[pymethods]
impl Config {
    #[new]
    #[pyo3(signature = (
        *,
        shards = None,
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
        shards: Option<u16>,
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
        let shards = self
            .shards
            .map_or_else(|| "None".to_owned(), |shards| shards.to_string());
        format!(
            "Config(shards={shards}, connections_per_shard={}, queue_capacity_per_shard={})",
            self.connections_per_shard, self.queue_capacity_per_shard
        )
    }
}

#[pyclass(module = "briskdb._briskdb", frozen, skip_from_py_object)]
#[derive(Clone, Debug)]
struct CancellationToken {
    inner: EngineCancellationToken,
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self {
            inner: EngineCancellationToken::new(),
        }
    }
}

#[pymethods]
impl CancellationToken {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn cancel(&self) -> bool {
        self.inner.cancel()
    }

    #[getter]
    fn cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    fn __repr__(&self) -> String {
        format!("CancellationToken(cancelled={})", self.cancelled())
    }
}

struct CursorState {
    rows: VecDeque<Vec<Value>>,
    closed: bool,
}

#[pyclass(module = "briskdb._briskdb", frozen)]
struct Cursor {
    shards: Vec<u16>,
    columns: Vec<(String, &'static str)>,
    batch_size: usize,
    state: Mutex<CursorState>,
}

impl Cursor {
    fn from_routed(result: Routed<ResultSet>, batch_size: usize) -> PyResult<Self> {
        Self::from_parts(vec![result.shard], result.value, batch_size)
    }

    fn from_logical(result: Executed<ResultSet>, batch_size: usize) -> PyResult<Self> {
        Self::from_parts(result.shards, result.value, batch_size)
    }

    fn from_parts(shards: Vec<u16>, result: ResultSet, batch_size: usize) -> PyResult<Self> {
        if batch_size == 0 {
            return Err(crate::error::invalid_value(
                "cursor batch_size must be at least 1",
            ));
        }
        let (columns, rows) = result.into_parts();
        Ok(Self {
            shards,
            columns: cursor_columns(columns),
            batch_size,
            state: Mutex::new(CursorState {
                rows: rows.into_iter().map(briskdb::Row::into_values).collect(),
                closed: false,
            }),
        })
    }

    fn take_rows(&self, count: usize) -> PyResult<Vec<Vec<Value>>> {
        let mut state = self.state.lock().map_err(NativeError::from)?;
        if state.closed {
            return Err(NativeError::Closed("cursor").into());
        }
        let count = count.min(state.rows.len());
        Ok(state.rows.drain(..count).collect())
    }
}

#[pymethods]
impl Cursor {
    #[getter]
    fn shards(&self) -> Vec<u16> {
        self.shards.clone()
    }

    #[getter]
    fn columns(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let output = PyList::empty(py);
        for (name, data_type) in &self.columns {
            let column = PyDict::new(py);
            column.set_item("name", name)?;
            column.set_item("type", data_type)?;
            output.append(column)?;
        }
        Ok(output.into_any().unbind())
    }

    #[getter]
    fn closed(&self) -> PyResult<bool> {
        Ok(self.state.lock().map_err(NativeError::from)?.closed)
    }

    #[getter]
    fn remaining(&self) -> PyResult<usize> {
        let state = self.state.lock().map_err(NativeError::from)?;
        if state.closed {
            return Err(NativeError::Closed("cursor").into());
        }
        Ok(state.rows.len())
    }

    fn fetchone(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.take_rows(1)?
            .pop()
            .map(|row| cursor_row_to_python(py, row))
            .transpose()
    }

    #[pyo3(signature = (size = None))]
    fn fetchmany(&self, py: Python<'_>, size: Option<usize>) -> PyResult<Py<PyAny>> {
        let size = size.unwrap_or(self.batch_size);
        if size == 0 {
            return Err(crate::error::invalid_value(
                "cursor fetch size must be at least 1",
            ));
        }
        cursor_rows_to_python(py, self.take_rows(size)?)
    }

    fn fetchall(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        cursor_rows_to_python(py, self.take_rows(usize::MAX)?)
    }

    fn close(&self) -> PyResult<()> {
        let mut state = self.state.lock().map_err(NativeError::from)?;
        state.rows.clear();
        state.closed = true;
        Ok(())
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.fetchone(py)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        _exception_type: Option<&Bound<'_, PyAny>>,
        _exception: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }

    fn __repr__(&self) -> PyResult<String> {
        let state = self.state.lock().map_err(NativeError::from)?;
        Ok(format!(
            "Cursor(columns={}, remaining={}, closed={})",
            self.columns.len(),
            state.rows.len(),
            state.closed
        ))
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
            let builder = BriskDb::builder(&root).with_engine_options(engine_options);
            let builder = match config.shards {
                Some(shards) => builder.with_shard_count(shards),
                None => builder,
            };
            let database = runtime.block_on(builder.open())?;
            let mut config = config;
            config.shards = Some(database.shard_count());
            let runtime = Arc::new(RuntimeOwner { runtime });
            Ok(Self {
                shared: Arc::new(DatabaseShared {
                    database: Mutex::new(Some(database)),
                    servers: Mutex::new(Vec::new()),
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
        self.shared
            .config
            .shards
            .expect("an opened database always has a resolved shard count")
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

    #[pyo3(signature = (*, timeout_ms = None, cancellation = None))]
    fn checkpoint(
        &self,
        py: Python<'_>,
        timeout_ms: Option<u64>,
        cancellation: Option<PyRef<'_, CancellationToken>>,
    ) -> PyResult<Py<PyAny>> {
        let context = request_context(timeout_ms, cancellation.as_deref())?;
        let shared = Arc::clone(&self.shared);
        let report = run_native(py, move || {
            let database = shared.database()?;
            Ok(shared
                .runtime
                .runtime
                .block_on(database.checkpoint_with_context(context))?)
        })?;
        checkpoint_to_python(py, report)
    }

    #[pyo3(signature = (
        *,
        http = "127.0.0.1:0",
        postgres = None,
        postgres_tls_cert = None,
        postgres_tls_key = None,
        postgres_user = "briskdb",
        postgres_password_file = None,
    ))]
    // These are separate keyword-only arguments in the stable Python API.
    #[allow(clippy::too_many_arguments)]
    fn serve(
        &self,
        py: Python<'_>,
        http: &str,
        postgres: Option<&str>,
        postgres_tls_cert: Option<PathBuf>,
        postgres_tls_key: Option<PathBuf>,
        postgres_user: &str,
        postgres_password_file: Option<PathBuf>,
    ) -> PyResult<Server> {
        let http_listen = parse_listener_address(http, "HTTP")?;
        let postgres_listen = postgres
            .map(|address| parse_listener_address(address, "PostgreSQL"))
            .transpose()?;
        validate_python_listener_address(http_listen, "HTTP")?;
        let postgres_security = match (postgres_tls_cert, postgres_tls_key, postgres_password_file)
        {
            (None, None, None) => None,
            (Some(certificate), Some(private_key), Some(password_file)) => Some(
                PostgresSecurityConfig::new(certificate, private_key, postgres_user, password_file)
                    .map_err(listener_error)?,
            ),
            _ => {
                return Err(crate::error::invalid_value(
                    "postgres_tls_cert, postgres_tls_key, and postgres_password_file must be set together",
                ));
            }
        };
        if postgres_security.is_none() {
            if let Some(address) = postgres_listen {
                validate_python_listener_address(address, "PostgreSQL")?;
            }
        }
        let shared = Arc::clone(&self.shared);
        run_native(py, move || {
            // Holding the database slot across bind and registration makes
            // listener start atomic with respect to Database.close().
            let database_slot = shared.database.lock()?;
            let database = database_slot
                .as_ref()
                .cloned()
                .ok_or(NativeError::Closed("database"))?;
            let listener_config = ListenerConfig {
                http_listen,
                postgres_listen,
            };
            let attached = match postgres_security {
                Some(security) => shared
                    .runtime
                    .runtime
                    .block_on(AttachedServer::start_secure(
                        &database,
                        listener_config,
                        security,
                    )),
                None => shared
                    .runtime
                    .runtime
                    .block_on(AttachedServer::start(&database, listener_config)),
            }
            .map_err(listener_error)?;
            let server = Arc::new(ServerShared {
                addresses: attached.addresses(),
                server: Mutex::new(Some(attached)),
                runtime: Arc::clone(&shared.runtime),
            });
            let mut servers = shared.servers.lock()?;
            servers.retain(|server| server.strong_count() > 0);
            servers.push(Arc::downgrade(&server));
            drop(servers);
            drop(database_slot);
            Ok(Server { shared: server })
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let shared = Arc::clone(&self.shared);
        let report = run_native(py, move || {
            let (database, servers) = {
                let mut database_slot = shared.database.lock()?;
                let Some(database) = database_slot.take() else {
                    return Ok(None);
                };
                let mut registry = shared.servers.lock()?;
                let servers = registry
                    .iter()
                    .filter_map(Weak::upgrade)
                    .collect::<Vec<_>>();
                registry.clear();
                (database, servers)
            };
            let mut listener_failure = None;
            for server in servers {
                if let Err(error) = server.close_native() {
                    listener_failure.get_or_insert(error);
                }
            }
            let report = shared.runtime.runtime.block_on(database.close())?;
            if let Some(error) = listener_failure {
                return Err(error);
            }
            Ok(Some(report))
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
            self.shard_count(),
            self.state()?
        ))
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        _exception_type: Option<&Bound<'_, PyAny>>,
        _exception: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }
}

#[pyclass(module = "briskdb._briskdb", frozen)]
struct Server {
    shared: Arc<ServerShared>,
}

#[pymethods]
impl Server {
    #[getter]
    fn http_address(&self) -> String {
        self.shared.addresses.http().to_string()
    }

    #[getter]
    fn postgres_address(&self) -> Option<String> {
        self.shared
            .addresses
            .postgres()
            .map(|address| address.to_string())
    }

    #[getter]
    fn closed(&self) -> PyResult<bool> {
        self.shared.is_closed().map_err(PyErr::from)
    }

    fn close(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let shared = Arc::clone(&self.shared);
        let already_closed = run_native(py, move || shared.close_native())?;
        let output = PyDict::new(py);
        output.set_item("already_closed", already_closed)?;
        Ok(output.into_any().unbind())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "Server(http_address={:?}, postgres_address={:?}, closed={})",
            self.http_address(),
            self.postgres_address(),
            self.closed()?
        ))
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        _exception_type: Option<&Bound<'_, PyAny>>,
        _exception: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
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

    #[pyo3(signature = (sql, *, timeout_ms = None, cancellation = None))]
    fn migrate(
        &self,
        py: Python<'_>,
        sql: String,
        timeout_ms: Option<u64>,
        cancellation: Option<PyRef<'_, CancellationToken>>,
    ) -> PyResult<Vec<u16>> {
        let context = request_context(timeout_ms, cancellation.as_deref())?;
        let shared = Arc::clone(&self.shared);
        run_native(py, move || {
            let session = shared.session()?;
            Ok(shared
                .runtime
                .runtime
                .block_on(session.migrate_with_context(sql, context))?)
        })
    }

    #[pyo3(signature = (sql, params = None, *, timeout_ms = None, cancellation = None))]
    fn execute(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<Vec<Py<PyAny>>>,
        timeout_ms: Option<u64>,
        cancellation: Option<PyRef<'_, CancellationToken>>,
    ) -> PyResult<Py<PyAny>> {
        let params = extract_params(py, params)?;
        let context = request_context(timeout_ms, cancellation.as_deref())?;
        let shared = Arc::clone(&self.shared);
        let result = run_native(py, move || {
            let session = shared.session()?;
            Ok(shared.runtime.runtime.block_on(
                session.execute_write_with_context(Statement::new(sql, params), context),
            )?)
        })?;
        write_result_to_python(py, result)
    }

    #[pyo3(signature = (sql, params = None, *, timeout_ms = None, cancellation = None))]
    fn query(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<Vec<Py<PyAny>>>,
        timeout_ms: Option<u64>,
        cancellation: Option<PyRef<'_, CancellationToken>>,
    ) -> PyResult<Py<PyAny>> {
        let params = extract_params(py, params)?;
        let context = request_context(timeout_ms, cancellation.as_deref())?;
        let shared = Arc::clone(&self.shared);
        let result = run_native(py, move || {
            let session = shared.session()?;
            Ok(shared
                .runtime
                .runtime
                .block_on(session.query_with_context(Statement::new(sql, params), context))?)
        })?;
        routed_result_to_python(py, result)
    }

    #[pyo3(signature = (sql, params = None, *, timeout_ms = None, cancellation = None))]
    fn query_logical(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<Vec<Py<PyAny>>>,
        timeout_ms: Option<u64>,
        cancellation: Option<PyRef<'_, CancellationToken>>,
    ) -> PyResult<Py<PyAny>> {
        let params = extract_params(py, params)?;
        let context = request_context(timeout_ms, cancellation.as_deref())?;
        let shared = Arc::clone(&self.shared);
        let result = run_native(py, move || {
            let session = shared.session()?;
            Ok(shared.runtime.runtime.block_on(
                session.query_logical_with_context(Statement::new(sql, params), context),
            )?)
        })?;
        logical_result_to_python(py, result)
    }

    #[pyo3(signature = (sql, params = None, *, batch_size = 1_000, timeout_ms = None, cancellation = None))]
    #[allow(clippy::too_many_arguments)]
    fn cursor(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<Vec<Py<PyAny>>>,
        batch_size: usize,
        timeout_ms: Option<u64>,
        cancellation: Option<PyRef<'_, CancellationToken>>,
    ) -> PyResult<Cursor> {
        let params = extract_params(py, params)?;
        let context = request_context(timeout_ms, cancellation.as_deref())?;
        let shared = Arc::clone(&self.shared);
        let result = run_native(py, move || {
            let session = shared.session()?;
            Ok(shared
                .runtime
                .runtime
                .block_on(session.query_with_context(Statement::new(sql, params), context))?)
        })?;
        Cursor::from_routed(result, batch_size)
    }

    #[pyo3(signature = (sql, params = None, *, batch_size = 1_000, timeout_ms = None, cancellation = None))]
    #[allow(clippy::too_many_arguments)]
    fn logical_cursor(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<Vec<Py<PyAny>>>,
        batch_size: usize,
        timeout_ms: Option<u64>,
        cancellation: Option<PyRef<'_, CancellationToken>>,
    ) -> PyResult<Cursor> {
        let params = extract_params(py, params)?;
        let context = request_context(timeout_ms, cancellation.as_deref())?;
        let shared = Arc::clone(&self.shared);
        let result = run_native(py, move || {
            let session = shared.session()?;
            Ok(shared.runtime.runtime.block_on(
                session.query_logical_with_context(Statement::new(sql, params), context),
            )?)
        })?;
        Cursor::from_logical(result, batch_size)
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

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        _exception_type: Option<&Bound<'_, PyAny>>,
        _exception: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
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
                shards: Some(shards),
                ..Config::default()
            };
            config.engine_options()?;
            Ok(config)
        }
        (None, Some(config)) => Ok(config.clone()),
        (None, None) => Ok(Config::default()),
    }
}

fn parse_listener_address(value: &str, label: &str) -> PyResult<SocketAddr> {
    value.parse().map_err(|_| {
        crate::error::invalid_value(format!(
            "{label} listener must be an IP socket address such as 127.0.0.1:0"
        ))
    })
}

fn validate_python_listener_address(address: SocketAddr, label: &str) -> PyResult<()> {
    if address.ip().is_loopback() {
        Ok(())
    } else {
        Err(crate::error::invalid_value(format!(
            "{label} listener must use a loopback address; received {address}"
        )))
    }
}

fn request_context(
    timeout_ms: Option<u64>,
    cancellation: Option<&CancellationToken>,
) -> PyResult<RequestContext> {
    let cancellation = cancellation
        .map(|cancellation| cancellation.inner.clone())
        .unwrap_or_default();
    let context = RequestContext::new().with_cancellation_token(cancellation);
    match timeout_ms {
        Some(timeout_ms) => context
            .with_timeout(Duration::from_millis(timeout_ms))
            .map_err(NativeError::from)
            .map_err(PyErr::from),
        None => Ok(context),
    }
}

fn cursor_columns(columns: Vec<Column>) -> Vec<(String, &'static str)> {
    columns
        .into_iter()
        .map(|column| (column.name, data_type_name(column.data_type)))
        .collect()
}

fn cursor_row_to_python(py: Python<'_>, row: Vec<Value>) -> PyResult<Py<PyAny>> {
    let values = row
        .into_iter()
        .map(|value| value_to_python(py, value))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(PyTuple::new(py, values)?.into_any().unbind())
}

fn cursor_rows_to_python(py: Python<'_>, rows: Vec<Vec<Value>>) -> PyResult<Py<PyAny>> {
    let output = PyList::empty(py);
    for row in rows {
        output.append(cursor_row_to_python(py, row)?)?;
    }
    Ok(output.into_any().unbind())
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
            item.set_item("counts_available", shard.counts_available())?;
            item.set_item("wal_frames", shard.wal_frames())?;
            item.set_item("checkpointed_frames", shard.checkpointed_frames())?;
            item.set_item("complete", shard.complete())?;
            Ok(item.into_any().unbind())
        })
        .collect::<PyResult<Vec<_>>>()?;
    output.set_item("shards", shards)?;
    Ok(output.into_any().unbind())
}

fn python_distribution_version(version: &str) -> String {
    for (semver, pep440) in [("-alpha.", "a"), ("-beta.", "b"), ("-rc.", "rc")] {
        if let Some((release, prerelease)) = version.split_once(semver) {
            return format!("{release}{pep440}{prerelease}");
        }
    }
    version.to_owned()
}

#[pymodule]
fn _briskdb(module: &Bound<'_, PyModule>) -> PyResult<()> {
    error::register(module)?;
    module.add_class::<CancellationToken>()?;
    module.add_class::<Config>()?;
    module.add_class::<Cursor>()?;
    module.add_class::<Database>()?;
    module.add_class::<Server>()?;
    module.add_class::<Session>()?;
    module.add_function(wrap_pyfunction!(open_database, module)?)?;
    module.add(
        "__version__",
        python_distribution_version(env!("CARGO_PKG_VERSION")),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use briskdb::core::{
        Database as CoreDatabase, GlobalIndexDeclaration, GlobalIndexKeyPart, GlobalIndexKeySource,
        GlobalIndexKeyType, GlobalIndexStorageTopology, ShardKeyMetadata, ShardKeyType,
        TableDeclaration, UniqueNullSemantics,
    };

    use super::{Config, Database, error::UniqueViolationError, python_distribution_version};

    #[test]
    fn cargo_prereleases_match_python_distribution_versions() {
        assert_eq!(python_distribution_version("1.2.3"), "1.2.3");
        assert_eq!(python_distribution_version("1.2.3-alpha.4"), "1.2.3a4");
        assert_eq!(python_distribution_version("1.2.3-beta.5"), "1.2.3b5");
        assert_eq!(python_distribution_version("1.2.3-rc.6"), "1.2.3rc6");
    }

    #[test]
    fn python_session_writes_use_global_unique_authority() {
        let root = tempfile::tempdir().unwrap();
        let mut core = CoreDatabase::open(root.path(), 2).unwrap();
        core.broadcast(
            "CREATE TABLE accounts (
                tenant_id TEXT PRIMARY KEY NOT NULL,
                email TEXT NOT NULL
             )",
        )
        .unwrap();
        let logical = core.catalog().default_database().id();
        core.register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "accounts",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
        let table_id = core
            .catalog()
            .table("default", "accounts")
            .unwrap()
            .unwrap()
            .id();
        let declaration = GlobalIndexDeclaration::new(
            table_id,
            "accounts_email_unique",
            vec![GlobalIndexKeyPart::new(
                GlobalIndexKeySource::column("email").unwrap(),
                GlobalIndexKeyType::Text,
            )],
        )
        .unwrap()
        .unique(UniqueNullSemantics::Distinct)
        .with_topology(GlobalIndexStorageTopology::selected_v1());
        let index_id = core.create_global_index(declaration).unwrap();
        core.build_global_index(index_id).unwrap();
        drop(core);

        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let database = Database::create(py, root.path().to_path_buf(), Config::default())
                .expect("open Python database wrapper");
            let first = database
                .session(py, Some("python-index-a".to_owned()))
                .unwrap();
            first
                .execute(
                    py,
                    "INSERT INTO accounts (tenant_id, email) VALUES ('python-index-a', 'python-global@example.test')"
                        .to_owned(),
                    None,
                    None,
                    None,
                )
                .unwrap();
            first.close(py).unwrap();

            let duplicate = database
                .session(py, Some("python-index-b".to_owned()))
                .unwrap();
            let error = duplicate
                .execute(
                    py,
                    "INSERT INTO accounts (tenant_id, email) VALUES ('python-index-b', 'python-global@example.test')"
                        .to_owned(),
                    None,
                    None,
                    None,
                )
                .unwrap_err();
            assert!(error.is_instance_of::<UniqueViolationError>(py));
            duplicate.close(py).unwrap();
            database.close(py).unwrap();
        });

        let mut core = CoreDatabase::open(root.path(), 2).unwrap();
        assert!(core.validate_global_index(index_id).unwrap().is_valid());
    }
}
