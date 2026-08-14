//! Protocol-neutral frontend session state.

use std::{
    fmt,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

use super::{
    CancellationToken, EngineError, EngineErrorKind, EngineResult, OperationControl,
    OperationLease, PreparedState, PreparedStatementLimits,
};
use crate::storage::{PooledConnection, SchemaOperationGuard};
use tokio::sync::Mutex;

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

/// A process-unique identifier for a frontend session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionId(u64);

impl SessionId {
    /// Return the numeric session identifier.
    pub const fn get(self) -> u64 {
        self.0
    }

    #[cfg(test)]
    pub(crate) const fn for_test(value: u64) -> Self {
        Self(value)
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// The lifecycle state of a protocol-neutral session.
///
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionState {
    /// The session can accept requests.
    Ready,
    /// The session owns an explicit transaction that can still execute work.
    InTransaction,
    /// The session transaction rejected or failed a statement; subsequent SQL
    /// execution accepts only transaction termination.
    FailedTransaction,
    /// The session was closed and cannot be reopened.
    Closed,
}

pub(crate) struct SessionInner {
    state: SessionState,
    routing_key: Option<String>,
    prepared: PreparedState,
    transaction: Option<TransactionState>,
}

#[derive(Debug)]
pub(crate) struct TransactionState {
    pub(crate) pinned_shard: Option<u16>,
    pub(crate) connection: Option<PooledConnection>,
    done: CancellationToken,
    _lifecycle: OperationLease,
    schema_operation: SchemaOperationGuard,
}

impl TransactionState {
    pub(crate) fn new(lifecycle: OperationLease, schema: SchemaOperationGuard) -> Self {
        Self {
            pinned_shard: None,
            connection: None,
            done: CancellationToken::new(),
            _lifecycle: lifecycle,
            schema_operation: schema,
        }
    }

    pub(crate) fn completion_token(&self) -> CancellationToken {
        self.done.clone()
    }

    pub(crate) fn finish(
        mut self,
        commit: bool,
        control: Option<Arc<OperationControl>>,
    ) -> EngineResult<()> {
        self.done.cancel();
        let Some(mut connection) = self.connection.take() else {
            return Ok(());
        };
        let sql = if commit { "COMMIT" } else { "ROLLBACK" };
        let execute = |connection: &mut PooledConnection| {
            connection.execute_batch(sql).map_err(|error| {
                crate::sqlite_error::storage(error)
                    .context(format!("failed to {sql} the pinned SQLite transaction"))
            })
        };
        let result = match control {
            Some(control) => connection.run_controlled(control, execute),
            None => execute(&mut connection),
        };
        if result.is_err() {
            connection.mark_broken();
        }
        result
    }
}

impl Drop for TransactionState {
    fn drop(&mut self) {
        self.done.cancel();
    }
}

impl SessionInner {
    pub(crate) fn ensure_open(&self) -> EngineResult<()> {
        if self.state != SessionState::Closed {
            Ok(())
        } else {
            Err(closed_session_error())
        }
    }

    pub(crate) const fn state(&self) -> SessionState {
        self.state
    }

    pub(crate) fn begin_transaction(&mut self, transaction: TransactionState) {
        debug_assert_eq!(self.state, SessionState::Ready);
        debug_assert!(self.transaction.is_none());
        self.transaction = Some(transaction);
        self.state = SessionState::InTransaction;
    }

    pub(crate) fn fail_transaction(&mut self) {
        if self.state == SessionState::InTransaction {
            self.state = SessionState::FailedTransaction;
        }
    }

    pub(crate) fn transaction_mut(&mut self) -> Option<&mut TransactionState> {
        self.transaction.as_mut()
    }

    pub(crate) fn transaction_shard(&self) -> Option<u16> {
        self.transaction
            .as_ref()
            .and_then(|transaction| transaction.pinned_shard)
    }

    pub(crate) fn transaction_schema_operation(&self) -> Option<SchemaOperationGuard> {
        self.transaction
            .as_ref()
            .map(|transaction| transaction.schema_operation.clone())
    }

    pub(crate) fn take_transaction(&mut self) -> Option<TransactionState> {
        self.transaction.take()
    }

    pub(crate) fn finish_transaction(&mut self) -> Option<TransactionState> {
        let transaction = self.transaction.take();
        if self.state != SessionState::Closed {
            self.state = SessionState::Ready;
        }
        transaction
    }

    pub(crate) fn routing_key(&self) -> Option<&str> {
        self.routing_key.as_deref()
    }

    pub(crate) const fn prepared(&self) -> &PreparedState {
        &self.prepared
    }

    pub(crate) fn prepared_mut(&mut self) -> &mut PreparedState {
        &mut self.prepared
    }
}

impl fmt::Debug for SessionInner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionInner")
            .field("state", &self.state)
            .field("has_routing_key", &self.routing_key.is_some())
            .field("prepared", &self.prepared)
            .field(
                "transaction_shard",
                &self
                    .transaction
                    .as_ref()
                    .and_then(|transaction| transaction.pinned_shard),
            )
            .finish()
    }
}

/// Mutable state owned by one protocol frontend connection or request.
///
/// A `Session` deliberately does not implement [`Clone`]. Multiple operations
/// may borrow the same session concurrently, but the engine serializes them.
#[must_use = "a session must be passed to engine operations"]
pub struct Session {
    id: SessionId,
    pub(crate) owner: u64,
    pub(crate) inner: Arc<Mutex<SessionInner>>,
}

impl Session {
    pub(crate) fn new(owner: u64, prepared_limits: PreparedStatementLimits) -> Self {
        let id = SessionId(NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed));
        Self {
            id,
            owner,
            inner: Arc::new(Mutex::new(SessionInner {
                state: SessionState::Ready,
                routing_key: None,
                prepared: PreparedState::new(id, prepared_limits),
                transaction: None,
            })),
        }
    }

    /// Return this session's process-unique identifier.
    pub const fn id(&self) -> SessionId {
        self.id
    }

    /// Return the current lifecycle state.
    pub async fn state(&self) -> SessionState {
        self.inner.lock().await.state
    }

    /// Return a copy of the current explicit routing key, if one is set.
    pub async fn routing_key(&self) -> Option<String> {
        self.inner.lock().await.routing_key.clone()
    }

    /// Set the explicit routing key used by subsequent routed operations.
    pub async fn set_routing_key(&self, routing_key: impl Into<String>) -> EngineResult<()> {
        let routing_key = routing_key.into();
        let mut inner = self.inner.lock().await;
        inner.ensure_open()?;
        inner.routing_key = Some(routing_key);
        Ok(())
    }

    /// Clear the explicit routing key used by routed operations.
    pub async fn clear_routing_key(&self) -> EngineResult<()> {
        let mut inner = self.inner.lock().await;
        inner.ensure_open()?;
        inner.routing_key = None;
        Ok(())
    }

    /// Close the session.
    ///
    /// Closing is terminal and idempotent. It also clears routing context,
    /// prepared statements, and bound portals.
    pub async fn close(&self) -> EngineResult<()> {
        let transaction = {
            let mut inner = self.inner.lock().await;
            inner.state = SessionState::Closed;
            inner.routing_key = None;
            inner.prepared.clear();
            inner.take_transaction()
        };
        if let Some(transaction) = transaction {
            tokio::task::spawn_blocking(move || transaction.finish(false, None))
                .await
                .map_err(|error| {
                    EngineError::from_source(
                        EngineErrorKind::Internal,
                        "transaction cleanup task failed",
                        error,
                    )
                })??;
        }
        Ok(())
    }

    /// Mark an active transaction failed after a protocol-layer error.
    pub async fn fail_transaction(&self) {
        self.inner.lock().await.fail_transaction();
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Session")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

fn closed_session_error() -> EngineError {
    EngineError::new(EngineErrorKind::FailedPrecondition, "the session is closed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sessions_have_unique_ids_and_start_ready_without_routing_context() {
        let first = Session::new(7, PreparedStatementLimits::default());
        let second = Session::new(7, PreparedStatementLimits::default());

        assert_ne!(first.id(), second.id());
        assert!(first.id().get() > 0);
        assert_eq!(first.id().to_string(), first.id().get().to_string());
        assert_eq!(first.state().await, SessionState::Ready);
        assert_eq!(first.routing_key().await, None);
    }

    #[tokio::test]
    async fn routing_context_can_be_set_replaced_and_cleared() {
        let session = Session::new(7, PreparedStatementLimits::default());

        session.set_routing_key("tenant-a").await.unwrap();
        assert_eq!(session.routing_key().await.as_deref(), Some("tenant-a"));
        session.set_routing_key("tenant-b").await.unwrap();
        assert_eq!(session.routing_key().await.as_deref(), Some("tenant-b"));
        session.clear_routing_key().await.unwrap();
        assert_eq!(session.routing_key().await, None);
        assert_eq!(session.state().await, SessionState::Ready);
    }

    #[tokio::test]
    async fn close_is_idempotent_terminal_and_clears_routing_context() {
        let session = Session::new(7, PreparedStatementLimits::default());
        session.set_routing_key("tenant-a").await.unwrap();

        session.close().await.unwrap();
        session.close().await.unwrap();

        assert_eq!(session.state().await, SessionState::Closed);
        assert_eq!(session.routing_key().await, None);
        assert_eq!(
            session
                .set_routing_key("tenant-b")
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(
            session.clear_routing_key().await.unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(session.state().await, SessionState::Closed);
    }
}
