//! Stable, listener-free entry point for embedding BriskDB.
//!
//! Opening this API never binds a socket, installs a signal handler or tracing
//! subscriber, or otherwise changes process-global state. The caller owns the
//! Tokio runtime and should explicitly call [`BriskDb::close`] before dropping
//! its last database handle.

use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::core::{
    CancellationToken, Catalog, CheckpointReport, Column, DescribeTarget, Engine, EngineOptions,
    EngineResult, EngineState, EngineStatus, Executed, GlobalIndexAsyncOptions, GlobalIndexWorker,
    PortalId, PrepareRequest, PreparedExecution, PreparedStatementDescription, PreparedStatementId,
    RequestContext, ResultSet, Routed, Row, RowStream, Session, SessionId, SessionState,
    ShutdownReport, Statement, TransactionExecution, Value, WriteResult,
};
use crate::{EngineError, EngineErrorKind};
use crate::{SqlDialect, SqlTranslationMode};

/// Recommended number of physical shards for small embedded deployments.
///
/// New databases still require callers to select a count explicitly; this
/// constant is retained as a convenient documented choice.
pub const DEFAULT_EMBEDDED_SHARDS: u16 = 4;

/// Tokio runtime ownership selected for an embedded database.
///
/// The initial embedded API is intentionally async and uses the runtime that
/// polls [`BriskDbBuilder::open`]. A dedicated runtime is reserved for future
/// synchronous foreign-language wrappers and is rejected before storage opens.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RuntimeBehavior {
    /// Use the embedding application's current Tokio runtime.
    #[default]
    CallerManaged,
    /// Reserve a dedicated BriskDB runtime owned by the handle.
    Dedicated,
}

/// Optional native document-command surface for an embedded database.
///
/// Document support is an explicit configuration choice so enabling it later
/// cannot silently change a SQL-only application's storage contract. The
/// current build rejects [`DocumentSupport::Enabled`] before touching storage.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DocumentSupport {
    /// Open only the established SQL engine.
    #[default]
    Disabled,
    /// Enable the native BSON/document engine when that feature is available.
    Enabled,
}

/// Validated configuration for opening one listener-free BriskDB instance.
#[derive(Debug, Clone)]
#[must_use = "a builder must be opened to create a BriskDB instance"]
pub struct BriskDbBuilder {
    root: PathBuf,
    shard_count: Option<u16>,
    engine_options: EngineOptions,
    runtime_behavior: RuntimeBehavior,
    document_support: DocumentSupport,
}

impl BriskDbBuilder {
    /// Create a builder that detects existing storage and uses documented
    /// resource defaults.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            shard_count: None,
            engine_options: EngineOptions::default(),
            runtime_behavior: RuntimeBehavior::default(),
            document_support: DocumentSupport::default(),
        }
    }

    /// Return the configured data-directory path.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Return the requested count, or `None` when existing data will decide.
    pub const fn shard_count(&self) -> Option<u16> {
        self.shard_count
    }

    /// Set the fixed physical shard count used for a new database.
    pub const fn with_shard_count(mut self, shard_count: u16) -> Self {
        self.shard_count = Some(shard_count);
        self
    }

    /// Return the configured engine resource limits.
    pub const fn engine_options(&self) -> EngineOptions {
        self.engine_options
    }

    /// Replace the complete validated engine resource-limit set.
    pub const fn with_engine_options(mut self, engine_options: EngineOptions) -> Self {
        self.engine_options = engine_options;
        self
    }

    /// Return the configured runtime ownership behavior.
    pub const fn runtime_behavior(&self) -> RuntimeBehavior {
        self.runtime_behavior
    }

    /// Select how the asynchronous runtime is owned.
    pub const fn with_runtime_behavior(mut self, runtime_behavior: RuntimeBehavior) -> Self {
        self.runtime_behavior = runtime_behavior;
        self
    }

    /// Return the configured native document support.
    pub const fn document_support(&self) -> DocumentSupport {
        self.document_support
    }

    /// Enable or disable native document support explicitly.
    pub const fn with_document_support(mut self, document_support: DocumentSupport) -> Self {
        self.document_support = document_support;
        self
    }

    /// Validate the complete configuration without touching the filesystem.
    pub fn validate(&self) -> EngineResult<()> {
        if self.root.as_os_str().is_empty() {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "the embedded data-directory path must not be empty",
            ));
        }
        if let Some(shard_count) = self.shard_count {
            self.engine_options.validate_for_shards(shard_count)?;
        }
        if self.runtime_behavior != RuntimeBehavior::CallerManaged {
            return Err(EngineError::new(
                EngineErrorKind::Unsupported,
                "a dedicated embedded runtime is not implemented; use CallerManaged",
            ));
        }
        if self.document_support != DocumentSupport::Disabled {
            return Err(EngineError::new(
                EngineErrorKind::Unsupported,
                "native embedded document support is not implemented in this build",
            ));
        }
        Ok(())
    }

    /// Validate and open one embedded database on the caller's Tokio runtime.
    pub async fn open(self) -> EngineResult<BriskDb> {
        self.validate()?;
        let engine = match self.shard_count {
            Some(shard_count) => {
                Engine::open_with_options(&self.root, shard_count, self.engine_options).await?
            }
            None => Engine::open_detected_with_options(&self.root, self.engine_options).await?,
        };
        Ok(BriskDb {
            engine,
            root: self.root,
            runtime_behavior: self.runtime_behavior,
            document_support: self.document_support,
        })
    }
}

/// A cloneable, listener-free handle to one BriskDB engine.
///
/// Clones share lifecycle and connection pools. Different calls to
/// [`BriskDb::open`] create independent instances when given different data
/// directories.
#[derive(Debug, Clone)]
pub struct BriskDb {
    engine: Engine,
    root: PathBuf,
    runtime_behavior: RuntimeBehavior,
    document_support: DocumentSupport,
}

/// Cloneable, database-owning session handle for embedded applications.
///
/// Every clone shares one serialized session state, including routing context,
/// prepared statements, and terminal close. The retained [`BriskDb`] handle
/// keeps the owning engine identity available, but cannot resurrect it after
/// database shutdown.
#[derive(Clone)]
pub struct BriskSession {
    database: BriskDb,
    session: Arc<Session>,
}

/// An owned, single-session transaction for embedded applications.
///
/// The handle uses the same transaction state machine as every protocol
/// adapter. Its first routed statement pins one physical shard, later work on
/// another shard fails the transaction, and committing a failed transaction
/// rolls it back. Dropping an unfinished handle drops its private session; pool
/// hygiene then rolls back any pinned SQLite transaction before the connection
/// can be reused.
#[must_use = "an embedded transaction must be committed, rolled back, or dropped"]
pub struct BriskTransaction {
    session: BriskSession,
}

/// A bounded, asynchronous cursor returned by embedded prepared reads.
///
/// Dropping a cursor cancels its unfinished query and interrupts the leased
/// SQLite connection. The prepared statement and portal remain owned by the
/// session and can be closed with [`BriskSession::close_prepared`] or
/// [`BriskSession::close_bound`].
#[must_use = "dropping a cursor cancels its unfinished query"]
pub struct BriskCursor {
    shards: Vec<u16>,
    stream: RowStream,
}

impl fmt::Debug for BriskSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BriskSession")
            .field("id", &self.id())
            .field("database_state", &self.database.state())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for BriskTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BriskTransaction")
            .field("session", &self.session.id())
            .field("database_state", &self.session.database_state())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for BriskCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BriskCursor")
            .field("shards", &self.shards)
            .field("stream", &self.stream)
            .finish()
    }
}

impl BriskDb {
    /// Start a builder for one data directory.
    pub fn builder(root: impl Into<PathBuf>) -> BriskDbBuilder {
        BriskDbBuilder::new(root)
    }

    /// Open an initialized database by detecting its physical shard count.
    pub async fn open(root: impl Into<PathBuf>) -> EngineResult<Self> {
        Self::builder(root).open().await
    }

    /// Return the data-directory path used to open this database.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Return the selected runtime behavior.
    pub const fn runtime_behavior(&self) -> RuntimeBehavior {
        self.runtime_behavior
    }

    /// Return the selected document-support behavior.
    pub const fn document_support(&self) -> DocumentSupport {
        self.document_support
    }

    /// Borrow the protocol-neutral engine for advanced APIs.
    pub const fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Return the immutable engine resource and lifecycle options.
    pub fn options(&self) -> EngineOptions {
        self.engine.options()
    }

    /// Start a caller-owned background consumer for ready non-unique indexes.
    pub fn start_global_index_worker(
        &self,
        options: GlobalIndexAsyncOptions,
    ) -> EngineResult<GlobalIndexWorker> {
        self.engine.start_global_index_worker(options)
    }

    /// Create an independent frontend session owned by this database.
    pub fn session(&self) -> Session {
        self.engine.session()
    }

    /// Create a cloneable session that retains its owning database handle.
    ///
    /// This is the preferred session form for foreign-language wrappers and
    /// application components that need to move or clone owned handles.
    pub fn owned_session(&self) -> BriskSession {
        BriskSession {
            database: self.clone(),
            session: Arc::new(self.session()),
        }
    }

    /// Begin an owned transaction on a new private embedded session.
    pub async fn begin_transaction(&self) -> EngineResult<BriskTransaction> {
        self.begin_transaction_with_context(RequestContext::new())
            .await
    }

    /// Begin an owned transaction with host-supplied request controls.
    pub async fn begin_transaction_with_context(
        &self,
        context: RequestContext,
    ) -> EngineResult<BriskTransaction> {
        let session = self.owned_session();
        let outcome = session.transaction_control("BEGIN", context).await?;
        debug_assert_eq!(outcome, TransactionExecution::Started);
        Ok(BriskTransaction { session })
    }

    /// Return immutable engine and resource-limit status.
    pub async fn status(&self, session: &Session) -> EngineResult<EngineStatus> {
        self.engine.status(session).await
    }

    /// Execute one routed write and return only its affected-row count.
    pub async fn execute(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Routed<usize>> {
        self.engine.execute(session, statement).await
    }

    /// Execute one routed write with host-supplied request controls.
    pub async fn execute_with_context(
        &self,
        session: &Session,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Routed<usize>> {
        self.engine
            .execute_with_context(session, statement, context)
            .await
    }

    /// Execute one routed write and return the selected shard and write result.
    pub async fn execute_write(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Routed<WriteResult>> {
        self.engine.execute_write(session, statement).await
    }

    /// Execute one routed write with request controls and generated-key data.
    pub async fn execute_write_with_context(
        &self,
        session: &Session,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Routed<WriteResult>> {
        self.engine
            .execute_write_with_context(session, statement, context)
            .await
    }

    /// Query one routed physical owner.
    pub async fn query(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Routed<ResultSet>> {
        self.engine.query(session, statement).await
    }

    /// Query one routed owner with cancellation, deadline, and result controls.
    pub async fn query_with_context(
        &self,
        session: &Session,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Routed<ResultSet>> {
        self.engine
            .query_with_context(session, statement, context)
            .await
    }

    /// Query the logical table view selected by catalog metadata.
    pub async fn query_logical(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Executed<ResultSet>> {
        self.engine.query_logical(session, statement).await
    }

    /// Query the logical table view with host-supplied request controls.
    pub async fn query_logical_with_context(
        &self,
        session: &Session,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Executed<ResultSet>> {
        self.engine
            .query_logical_with_context(session, statement, context)
            .await
    }

    /// Parse, validate, translate, and compile one prepared SQL statement.
    pub async fn prepare(
        &self,
        session: &Session,
        request: PrepareRequest,
    ) -> EngineResult<PreparedStatementId> {
        self.engine.prepare_statement(session, request).await
    }

    /// Prepare one statement with host-supplied request controls.
    pub async fn prepare_with_context(
        &self,
        session: &Session,
        request: PrepareRequest,
        context: RequestContext,
    ) -> EngineResult<PreparedStatementId> {
        self.engine
            .prepare_statement_with_context(session, request, context)
            .await
    }

    /// Bind typed values and the session's current route into a portal.
    pub async fn bind(
        &self,
        session: &Session,
        statement: PreparedStatementId,
        parameters: Vec<Value>,
    ) -> EngineResult<PortalId> {
        self.engine
            .bind_statement(session, statement, parameters)
            .await
    }

    /// Bind a prepared statement with host-supplied request controls.
    pub async fn bind_with_context(
        &self,
        session: &Session,
        statement: PreparedStatementId,
        parameters: Vec<Value>,
        context: RequestContext,
    ) -> EngineResult<PortalId> {
        self.engine
            .bind_statement_with_context(session, statement, parameters, context)
            .await
    }

    /// Describe a prepared statement or bound portal.
    pub async fn describe(
        &self,
        session: &Session,
        target: DescribeTarget,
    ) -> EngineResult<PreparedStatementDescription> {
        self.engine.describe_prepared(session, target).await
    }

    /// Describe a prepared object with host-supplied request controls.
    pub async fn describe_with_context(
        &self,
        session: &Session,
        target: DescribeTarget,
        context: RequestContext,
    ) -> EngineResult<PreparedStatementDescription> {
        self.engine
            .describe_prepared_with_context(session, target, context)
            .await
    }

    /// Execute one immutable bound portal on its selected physical owner.
    pub async fn execute_bound(
        &self,
        session: &Session,
        portal: PortalId,
    ) -> EngineResult<Routed<PreparedExecution>> {
        self.engine.execute_portal(session, portal).await
    }

    /// Execute a bound portal with host-supplied request controls.
    pub async fn execute_bound_with_context(
        &self,
        session: &Session,
        portal: PortalId,
        context: RequestContext,
    ) -> EngineResult<Routed<PreparedExecution>> {
        self.engine
            .execute_portal_with_context(session, portal, context)
            .await
    }

    /// Execute a bound portal through logical point/scatter planning.
    pub async fn execute_bound_logical(
        &self,
        session: &Session,
        portal: PortalId,
    ) -> EngineResult<Executed<PreparedExecution>> {
        self.engine.execute_portal_logical(session, portal).await
    }

    /// Execute a logical bound portal with host-supplied request controls.
    pub async fn execute_bound_logical_with_context(
        &self,
        session: &Session,
        portal: PortalId,
        context: RequestContext,
    ) -> EngineResult<Executed<PreparedExecution>> {
        self.engine
            .execute_portal_logical_with_context(session, portal, context)
            .await
    }

    /// Stream one immutable read portal through logical point/scatter planning.
    pub async fn stream_bound_logical(
        &self,
        session: &Session,
        portal: PortalId,
    ) -> EngineResult<BriskCursor> {
        self.stream_bound_logical_with_context(session, portal, RequestContext::new())
            .await
    }

    /// Stream one logical read portal with host-supplied request controls.
    pub async fn stream_bound_logical_with_context(
        &self,
        session: &Session,
        portal: PortalId,
        context: RequestContext,
    ) -> EngineResult<BriskCursor> {
        let Executed { shards, value } = self
            .engine
            .stream_portal_logical_with_context(session, portal, context)
            .await?;
        Ok(BriskCursor {
            shards,
            stream: value,
        })
    }

    /// Close a prepared statement and every portal bound from it.
    pub async fn close_prepared(
        &self,
        session: &Session,
        statement: PreparedStatementId,
    ) -> EngineResult<bool> {
        self.engine
            .close_prepared_statement(session, statement)
            .await
    }

    /// Close one bound portal while retaining its prepared statement.
    pub async fn close_bound(&self, session: &Session, portal: PortalId) -> EngineResult<bool> {
        self.engine.close_portal(session, portal).await
    }

    /// Apply one durable parameterless schema batch to every shard.
    pub async fn migrate(
        &self,
        session: &Session,
        sql: impl Into<String>,
    ) -> EngineResult<Vec<u16>> {
        self.engine.broadcast(session, sql.into()).await
    }

    /// Apply one durable schema batch with host-supplied request controls.
    pub async fn migrate_with_context(
        &self,
        session: &Session,
        sql: impl Into<String>,
        context: RequestContext,
    ) -> EngineResult<Vec<u16>> {
        self.engine
            .broadcast_with_context(session, sql.into(), context)
            .await
    }

    /// Ask SQLite to passively checkpoint every physical shard.
    pub async fn checkpoint(&self) -> EngineResult<CheckpointReport> {
        self.engine.checkpoint().await
    }

    /// Passively checkpoint every shard with host-supplied request controls.
    pub async fn checkpoint_with_context(
        &self,
        context: RequestContext,
    ) -> EngineResult<CheckpointReport> {
        self.engine.checkpoint_with_context(context).await
    }

    /// Return the immutable logical database and table catalog.
    pub fn catalog(&self) -> &Catalog {
        self.engine.catalog()
    }

    /// Return the configured physical shard count.
    pub fn shard_count(&self) -> u16 {
        self.engine.shard_count()
    }

    /// Return the engine lifecycle state shared by every clone.
    pub fn state(&self) -> EngineState {
        self.engine.state()
    }

    /// Stop admitting new work without waiting for active work to drain.
    ///
    /// This is synchronous and idempotent. Call [`BriskDb::close`] or
    /// [`BriskDb::close_with_grace`] afterward to finish cleanup.
    pub fn begin_close(&self) -> EngineState {
        self.engine.begin_shutdown()
    }

    /// Explicitly drain work and close idle SQLite handles.
    pub async fn close(&self) -> EngineResult<ShutdownReport> {
        self.engine.shutdown().await
    }

    /// Explicitly close using a host-selected finite grace period.
    pub async fn close_with_grace(
        &self,
        grace: std::time::Duration,
    ) -> EngineResult<ShutdownReport> {
        self.engine.shutdown_with_grace(grace).await
    }

    /// Wait for a host-owned cancellation token, then close explicitly.
    ///
    /// Dropping this future before cancellation has no side effects. This lets
    /// an embedding host compose its own signal, task, or service lifecycle
    /// without BriskDB installing a process signal handler.
    pub async fn close_when_cancelled(
        &self,
        cancellation: CancellationToken,
    ) -> EngineResult<ShutdownReport> {
        cancellation.cancelled().await;
        self.close().await
    }
}

impl BriskSession {
    async fn execute_prepared_statement(
        &self,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Executed<PreparedExecution>> {
        let (sql, parameters) = statement.into_parts();
        let request = PrepareRequest::new(
            self.database.catalog().default_database().id(),
            SqlDialect::Sqlite,
            SqlTranslationMode::StrictSqlite,
            sql,
        );
        let prepared = self.prepare_with_context(request, context.clone()).await?;
        let portal = match self
            .bind_with_context(prepared, parameters, context.clone())
            .await
        {
            Ok(portal) => portal,
            Err(error) => {
                let _ = self.close_prepared(prepared).await;
                return Err(error);
            }
        };
        let execution = self
            .execute_bound_logical_with_context(portal, context)
            .await;
        let cleanup = self.close_prepared(prepared).await;
        let execution = execution?;
        cleanup?;
        Ok(execution)
    }

    async fn transaction_control(
        &self,
        sql: &'static str,
        context: RequestContext,
    ) -> EngineResult<TransactionExecution> {
        let execution = self
            .execute_prepared_statement(Statement::new(sql, Vec::new()), context)
            .await;
        let execution = execution?;
        match execution.value {
            PreparedExecution::Transaction(outcome) => Ok(outcome),
            _ => Err(EngineError::new(
                EngineErrorKind::Internal,
                "transaction control returned a non-transaction result",
            )),
        }
    }

    /// Return the process-unique session identity.
    pub fn id(&self) -> SessionId {
        self.session.id()
    }

    /// Return the lifecycle state shared by every clone of this handle.
    pub async fn state(&self) -> SessionState {
        self.session.state().await
    }

    /// Return the owning database lifecycle state.
    pub fn database_state(&self) -> EngineState {
        self.database.state()
    }

    /// Return a copy of the current explicit route, if one is set.
    pub async fn routing_key(&self) -> Option<String> {
        self.session.routing_key().await
    }

    /// Replace the explicit route used by subsequent operations.
    pub async fn set_routing_key(&self, routing_key: impl Into<String>) -> EngineResult<()> {
        self.session.set_routing_key(routing_key).await
    }

    /// Clear the explicit route used by subsequent operations.
    pub async fn clear_routing_key(&self) -> EngineResult<()> {
        self.session.clear_routing_key().await
    }

    /// Return immutable engine and resource-limit status.
    pub async fn status(&self) -> EngineResult<EngineStatus> {
        self.database.status(self.session.as_ref()).await
    }

    /// Execute one routed write and return only its affected-row count.
    pub async fn execute(&self, statement: Statement) -> EngineResult<Routed<usize>> {
        self.database
            .execute(self.session.as_ref(), statement)
            .await
    }

    /// Execute one routed write with host-supplied request controls.
    pub async fn execute_with_context(
        &self,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Routed<usize>> {
        self.database
            .execute_with_context(self.session.as_ref(), statement, context)
            .await
    }

    /// Execute one routed write with complete generated-key data.
    pub async fn execute_write(&self, statement: Statement) -> EngineResult<Routed<WriteResult>> {
        self.database
            .execute_write(self.session.as_ref(), statement)
            .await
    }

    /// Execute one routed write with request controls and generated-key data.
    pub async fn execute_write_with_context(
        &self,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Routed<WriteResult>> {
        self.database
            .execute_write_with_context(self.session.as_ref(), statement, context)
            .await
    }

    /// Query one routed physical owner.
    pub async fn query(&self, statement: Statement) -> EngineResult<Routed<ResultSet>> {
        self.database.query(self.session.as_ref(), statement).await
    }

    /// Query one routed owner with host-supplied request controls.
    pub async fn query_with_context(
        &self,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Routed<ResultSet>> {
        self.database
            .query_with_context(self.session.as_ref(), statement, context)
            .await
    }

    /// Query the logical table view through point/scatter planning.
    pub async fn query_logical(&self, statement: Statement) -> EngineResult<Executed<ResultSet>> {
        self.database
            .query_logical(self.session.as_ref(), statement)
            .await
    }

    /// Query the logical table view with host-supplied request controls.
    pub async fn query_logical_with_context(
        &self,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Executed<ResultSet>> {
        self.database
            .query_logical_with_context(self.session.as_ref(), statement, context)
            .await
    }

    /// Compile one protocol-neutral prepared SQL statement.
    pub async fn prepare(&self, request: PrepareRequest) -> EngineResult<PreparedStatementId> {
        self.database.prepare(self.session.as_ref(), request).await
    }

    /// Prepare one statement with host-supplied request controls.
    pub async fn prepare_with_context(
        &self,
        request: PrepareRequest,
        context: RequestContext,
    ) -> EngineResult<PreparedStatementId> {
        self.database
            .prepare_with_context(self.session.as_ref(), request, context)
            .await
    }

    /// Bind typed values and the current route into an immutable portal.
    pub async fn bind(
        &self,
        statement: PreparedStatementId,
        parameters: Vec<Value>,
    ) -> EngineResult<PortalId> {
        self.database
            .bind(self.session.as_ref(), statement, parameters)
            .await
    }

    /// Bind a prepared statement with host-supplied request controls.
    pub async fn bind_with_context(
        &self,
        statement: PreparedStatementId,
        parameters: Vec<Value>,
        context: RequestContext,
    ) -> EngineResult<PortalId> {
        self.database
            .bind_with_context(self.session.as_ref(), statement, parameters, context)
            .await
    }

    /// Describe a prepared statement or bound portal.
    pub async fn describe(
        &self,
        target: DescribeTarget,
    ) -> EngineResult<PreparedStatementDescription> {
        self.database.describe(self.session.as_ref(), target).await
    }

    /// Execute one immutable bound portal on its selected owner.
    pub async fn execute_bound(&self, portal: PortalId) -> EngineResult<Routed<PreparedExecution>> {
        self.database
            .execute_bound(self.session.as_ref(), portal)
            .await
    }

    /// Execute a bound portal through logical point/scatter planning.
    pub async fn execute_bound_logical(
        &self,
        portal: PortalId,
    ) -> EngineResult<Executed<PreparedExecution>> {
        self.database
            .execute_bound_logical(self.session.as_ref(), portal)
            .await
    }

    /// Execute a logical bound portal with host-supplied request controls.
    pub async fn execute_bound_logical_with_context(
        &self,
        portal: PortalId,
        context: RequestContext,
    ) -> EngineResult<Executed<PreparedExecution>> {
        self.database
            .execute_bound_logical_with_context(self.session.as_ref(), portal, context)
            .await
    }

    /// Stream one immutable read portal through logical point/scatter planning.
    pub async fn stream_bound_logical(&self, portal: PortalId) -> EngineResult<BriskCursor> {
        self.database
            .stream_bound_logical(self.session.as_ref(), portal)
            .await
    }

    /// Stream one logical read portal with host-supplied request controls.
    pub async fn stream_bound_logical_with_context(
        &self,
        portal: PortalId,
        context: RequestContext,
    ) -> EngineResult<BriskCursor> {
        self.database
            .stream_bound_logical_with_context(self.session.as_ref(), portal, context)
            .await
    }

    /// Close a prepared statement and every bound portal derived from it.
    pub async fn close_prepared(&self, statement: PreparedStatementId) -> EngineResult<bool> {
        self.database
            .close_prepared(self.session.as_ref(), statement)
            .await
    }

    /// Close one bound portal while retaining its prepared statement.
    pub async fn close_bound(&self, portal: PortalId) -> EngineResult<bool> {
        self.database
            .close_bound(self.session.as_ref(), portal)
            .await
    }

    /// Apply one durable parameterless schema batch to every shard.
    pub async fn migrate(&self, sql: impl Into<String>) -> EngineResult<Vec<u16>> {
        self.database.migrate(self.session.as_ref(), sql).await
    }

    /// Apply one durable schema batch with host-supplied request controls.
    pub async fn migrate_with_context(
        &self,
        sql: impl Into<String>,
        context: RequestContext,
    ) -> EngineResult<Vec<u16>> {
        self.database
            .migrate_with_context(self.session.as_ref(), sql, context)
            .await
    }

    /// Close this session terminally and clear all retained session state.
    ///
    /// Closing one clone closes every clone. It remains available while the
    /// database is draining and is deterministic and idempotent.
    pub async fn close(&self) -> EngineResult<()> {
        self.session.close().await
    }
}

impl BriskTransaction {
    /// Return the current transaction/session state.
    pub async fn state(&self) -> SessionState {
        self.session.state().await
    }

    /// Set the route used by subsequent statements until it is replaced.
    pub async fn set_routing_key(&self, routing_key: impl Into<String>) -> EngineResult<()> {
        self.session.set_routing_key(routing_key).await
    }

    /// Execute one routed write inside the transaction.
    pub async fn execute_write(&self, statement: Statement) -> EngineResult<Executed<WriteResult>> {
        self.execute_write_with_context(statement, RequestContext::new())
            .await
    }

    /// Execute one routed write with host-supplied request controls.
    pub async fn execute_write_with_context(
        &self,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Executed<WriteResult>> {
        let execution = self
            .session
            .execute_prepared_statement(statement, context)
            .await?;
        let value = match execution.value {
            PreparedExecution::AffectedRows(rows) => WriteResult::without_generated_key(rows),
            PreparedExecution::GeneratedWrite(write) => write,
            _ => {
                return Err(EngineError::new(
                    EngineErrorKind::InvalidQuery,
                    "transaction write returned a non-write result",
                ));
            }
        };
        Ok(Executed {
            shards: execution.shards,
            value,
        })
    }

    /// Query one routed physical owner inside the transaction.
    pub async fn query(&self, statement: Statement) -> EngineResult<Executed<ResultSet>> {
        self.query_with_context(statement, RequestContext::new())
            .await
    }

    /// Query one routed owner with host-supplied request controls.
    pub async fn query_with_context(
        &self,
        statement: Statement,
        context: RequestContext,
    ) -> EngineResult<Executed<ResultSet>> {
        let execution = self
            .session
            .execute_prepared_statement(statement, context)
            .await?;
        let PreparedExecution::Rows(value) = execution.value else {
            return Err(EngineError::new(
                EngineErrorKind::InvalidQuery,
                "transaction query returned a non-row result",
            ));
        };
        Ok(Executed {
            shards: execution.shards,
            value,
        })
    }

    /// Compile one protocol-neutral prepared statement inside the transaction.
    pub async fn prepare(&self, request: PrepareRequest) -> EngineResult<PreparedStatementId> {
        self.session.prepare(request).await
    }

    /// Bind typed values and the transaction's current route into a portal.
    pub async fn bind(
        &self,
        statement: PreparedStatementId,
        parameters: Vec<Value>,
    ) -> EngineResult<PortalId> {
        self.session.bind(statement, parameters).await
    }

    /// Describe a prepared statement or bound portal.
    pub async fn describe(
        &self,
        target: DescribeTarget,
    ) -> EngineResult<PreparedStatementDescription> {
        self.session.describe(target).await
    }

    /// Stream a prepared read inside the transaction.
    pub async fn stream_bound_logical(&self, portal: PortalId) -> EngineResult<BriskCursor> {
        self.session.stream_bound_logical(portal).await
    }

    /// Stream a prepared read with host-supplied request controls.
    pub async fn stream_bound_logical_with_context(
        &self,
        portal: PortalId,
        context: RequestContext,
    ) -> EngineResult<BriskCursor> {
        self.session
            .stream_bound_logical_with_context(portal, context)
            .await
    }

    /// Close a prepared statement and every portal derived from it.
    pub async fn close_prepared(&self, statement: PreparedStatementId) -> EngineResult<bool> {
        self.session.close_prepared(statement).await
    }

    /// Close one bound portal while retaining its prepared statement.
    pub async fn close_bound(&self, portal: PortalId) -> EngineResult<bool> {
        self.session.close_bound(portal).await
    }

    /// Commit the transaction and terminally close its private session.
    pub async fn commit(self) -> EngineResult<TransactionExecution> {
        self.commit_with_context(RequestContext::new()).await
    }

    /// Commit the transaction with host-supplied request controls.
    pub async fn commit_with_context(
        self,
        context: RequestContext,
    ) -> EngineResult<TransactionExecution> {
        let outcome = self.session.transaction_control("COMMIT", context).await?;
        self.session.close().await?;
        Ok(outcome)
    }

    /// Roll back the transaction and terminally close its private session.
    pub async fn rollback(self) -> EngineResult<TransactionExecution> {
        self.rollback_with_context(RequestContext::new()).await
    }

    /// Roll back the transaction with host-supplied request controls.
    pub async fn rollback_with_context(
        self,
        context: RequestContext,
    ) -> EngineResult<TransactionExecution> {
        let outcome = self
            .session
            .transaction_control("ROLLBACK", context)
            .await?;
        self.session.close().await?;
        Ok(outcome)
    }
}

impl BriskCursor {
    /// Return the physical shards selected for this logical stream.
    pub fn shards(&self) -> &[u16] {
        &self.shards
    }

    /// Return stable column metadata published before the first row.
    pub fn columns(&self) -> &[Column] {
        self.stream.columns()
    }

    /// Wait for the next row, terminal query error, or clean end of stream.
    pub async fn next_row(&mut self) -> Option<EngineResult<Row>> {
        self.stream.next_row().await
    }
}
