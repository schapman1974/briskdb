//! Protocol-neutral frontend session state.

use std::{
    fmt,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

use tokio::sync::Mutex;

use super::{EngineError, EngineErrorKind, EngineResult, PreparedState, PreparedStatementLimits};

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
/// Transaction state will be added with the wire-protocol transaction work;
/// these variants currently describe only whether the session can accept
/// requests.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionState {
    /// The session can accept requests.
    Ready,
    /// The session was closed and cannot be reopened.
    Closed,
}

pub(crate) struct SessionInner {
    state: SessionState,
    routing_key: Option<String>,
    prepared: PreparedState,
}

impl SessionInner {
    pub(crate) fn ensure_ready(&self) -> EngineResult<()> {
        if self.state == SessionState::Ready {
            Ok(())
        } else {
            Err(closed_session_error())
        }
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
        inner.ensure_ready()?;
        inner.routing_key = Some(routing_key);
        Ok(())
    }

    /// Clear the explicit routing key used by routed operations.
    pub async fn clear_routing_key(&self) -> EngineResult<()> {
        let mut inner = self.inner.lock().await;
        inner.ensure_ready()?;
        inner.routing_key = None;
        Ok(())
    }

    /// Close the session.
    ///
    /// Closing is terminal and idempotent. It also clears routing context,
    /// prepared statements, and bound portals.
    pub async fn close(&self) -> EngineResult<()> {
        let mut inner = self.inner.lock().await;
        inner.state = SessionState::Closed;
        inner.routing_key = None;
        inner.prepared.clear();
        Ok(())
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
