//! Bounded host observation, independent of protocol commands and socket probes.

use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

use crate::{
    DocumentSupport, EngineState,
    core::{ReadinessSnapshot, SchemaState},
};

/// Lifecycle of this listener, not of other listeners borrowing the same engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MongoListenerState {
    Running,
    Closing,
    Closed,
    Failed,
}

impl MongoListenerState {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Closing => "closing",
            Self::Closed => "closed",
            Self::Failed => "failed",
        }
    }
}

/// Security policy enforced at bind time. Readiness is not network certification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MongoSecurityMode {
    /// Loopback only; no Mongo authentication or TLS. Trust local processes.
    AnonymousLoopback,
}

impl MongoSecurityMode {
    pub const fn code(self) -> &'static str {
        match self {
            Self::AnonymousLoopback => "anonymous_loopback",
        }
    }
}

/// Fixed, payload-free reason ordinary Mongo document work is not ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MongoReadinessReason {
    ListenerClosing,
    ListenerClosed,
    ListenerFailed,
    DocumentsDisabled,
    EngineUnavailable,
    EngineDraining,
    EngineStopped,
    SchemaMigrating,
    SchemaPending,
    SchemaDegraded,
}

impl MongoReadinessReason {
    pub const fn code(self) -> &'static str {
        match self {
            Self::ListenerClosing => "listener_closing",
            Self::ListenerClosed => "listener_closed",
            Self::ListenerFailed => "listener_failed",
            Self::DocumentsDisabled => "documents_disabled",
            Self::EngineUnavailable => "engine_unavailable",
            Self::EngineDraining => "engine_draining",
            Self::EngineStopped => "engine_stopped",
            Self::SchemaMigrating => "schema_migrating",
            Self::SchemaPending => "schema_pending",
            Self::SchemaDegraded => "schema_degraded",
        }
    }
}

/// Neighboring live observations, not a reservation or an atomic system snapshot.
/// No I/O, sessions, query admission, retries or background polling are required.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MongoReadinessSnapshot {
    pub listener: MongoListenerState,
    /// None after the last engine owner is released. This snapshot owns no engine.
    pub engine: Option<ReadinessSnapshot>,
    pub document_support: DocumentSupport,
    pub security: MongoSecurityMode,
}

impl MongoReadinessSnapshot {
    /// Local document admission only. Does not promise spare connection capacity,
    /// deep on-disk integrity, global-index health, authentication or TLS.
    pub const fn ready(self) -> bool {
        self.reason().is_none()
    }

    /// Primary reason, ordered listener > document support > engine > schema.
    /// Inspect the fields for concurrent secondary conditions. Detected catalog
    /// or shard corruption is reflected by the shared schema-degraded state;
    /// this cheap observation does not itself detect new on-disk corruption.
    pub const fn reason(self) -> Option<MongoReadinessReason> {
        use MongoReadinessReason as Reason;
        match self.listener {
            MongoListenerState::Closing => return Some(Reason::ListenerClosing),
            MongoListenerState::Closed => return Some(Reason::ListenerClosed),
            MongoListenerState::Failed => return Some(Reason::ListenerFailed),
            MongoListenerState::Running => {}
        }
        if !matches!(self.document_support, DocumentSupport::Enabled) {
            return Some(Reason::DocumentsDisabled);
        }
        let Some(engine) = self.engine else {
            return Some(Reason::EngineUnavailable);
        };
        match engine.lifecycle_state() {
            EngineState::Draining => return Some(Reason::EngineDraining),
            EngineState::Stopped => return Some(Reason::EngineStopped),
            EngineState::Running => {}
        }
        match engine.schema_state() {
            SchemaState::Ready => None,
            SchemaState::Migrating => Some(Reason::SchemaMigrating),
            SchemaState::Pending => Some(Reason::SchemaPending),
            SchemaState::Degraded => Some(Reason::SchemaDegraded),
        }
    }
}

#[derive(Default)]
pub(super) struct ListenerHealth(AtomicU8);

impl ListenerHealth {
    pub(super) fn state(&self, cancelled: bool) -> MongoListenerState {
        match self.0.load(Ordering::Acquire) {
            0 if !cancelled => MongoListenerState::Running,
            0 | 1 => MongoListenerState::Closing,
            2 => MongoListenerState::Closed,
            _ => MongoListenerState::Failed,
        }
    }

    pub(super) fn begin_close(&self) {
        let _ = self
            .0
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire);
    }

    pub(super) fn guard(self: &Arc<Self>) -> ListenerGuard {
        ListenerGuard(Arc::clone(self))
    }
}

pub(super) struct ListenerGuard(Arc<ListenerHealth>);

impl ListenerGuard {
    pub(super) fn finish(self, success: bool) {
        self.0
            .0
            .store(if success { 2 } else { 3 }, Ordering::Release);
    }
}

impl Drop for ListenerGuard {
    fn drop(&mut self) {
        // The future owns this guard even before its first poll. Aborts and
        // unwinds must never leave a dead listener reporting Running/Closing.
        let _ = self
            .0
            .0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state < 2).then_some(3)
            });
    }
}

#[cfg(test)]
mod tests;
