//! Stable, listener-free entry point for embedding BriskDB.
//!
//! Opening this API never binds a socket, installs a signal handler or tracing
//! subscriber, or otherwise changes process-global state. The caller owns the
//! Tokio runtime and should explicitly call [`BriskDb::close`] before dropping
//! its last database handle.

use std::path::{Path, PathBuf};

use crate::core::{
    Catalog, Engine, EngineOptions, EngineResult, EngineState, EngineStatus, Executed, ResultSet,
    Routed, Session, ShutdownReport, Statement, WriteResult,
};
use crate::{EngineError, EngineErrorKind};

/// Default number of physical shards created by the embedded convenience API.
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
    shard_count: u16,
    engine_options: EngineOptions,
    runtime_behavior: RuntimeBehavior,
    document_support: DocumentSupport,
}

impl BriskDbBuilder {
    /// Create a builder with documented embedded defaults.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            shard_count: DEFAULT_EMBEDDED_SHARDS,
            engine_options: EngineOptions::default(),
            runtime_behavior: RuntimeBehavior::default(),
            document_support: DocumentSupport::default(),
        }
    }

    /// Return the configured data-directory path.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Return the configured physical shard count.
    pub const fn shard_count(&self) -> u16 {
        self.shard_count
    }

    /// Set the fixed physical shard count used for a new database.
    pub const fn with_shard_count(mut self, shard_count: u16) -> Self {
        self.shard_count = shard_count;
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
        self.engine_options.validate_for_shards(self.shard_count)?;
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
        let engine =
            Engine::open_with_options(&self.root, self.shard_count, self.engine_options).await?;
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

impl BriskDb {
    /// Start a builder for one data directory.
    pub fn builder(root: impl Into<PathBuf>) -> BriskDbBuilder {
        BriskDbBuilder::new(root)
    }

    /// Open one database with the documented embedded defaults.
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

    /// Create an independent frontend session owned by this database.
    pub fn session(&self) -> Session {
        self.engine.session()
    }

    /// Return immutable engine and resource-limit status.
    pub async fn status(&self, session: &Session) -> EngineResult<EngineStatus> {
        self.engine.status(session).await
    }

    /// Execute one routed write and return the selected shard and write result.
    pub async fn execute_write(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Routed<WriteResult>> {
        self.engine.execute_write(session, statement).await
    }

    /// Query one routed physical owner.
    pub async fn query(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Routed<ResultSet>> {
        self.engine.query(session, statement).await
    }

    /// Query the logical table view selected by catalog metadata.
    pub async fn query_logical(
        &self,
        session: &Session,
        statement: Statement,
    ) -> EngineResult<Executed<ResultSet>> {
        self.engine.query_logical(session, statement).await
    }

    /// Apply one durable parameterless schema batch to every shard.
    pub async fn migrate(
        &self,
        session: &Session,
        sql: impl Into<String>,
    ) -> EngineResult<Vec<u16>> {
        self.engine.broadcast(session, sql.into()).await
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

    /// Explicitly drain work and close idle SQLite handles.
    pub async fn close(&self) -> EngineResult<ShutdownReport> {
        self.engine.shutdown().await
    }
}
