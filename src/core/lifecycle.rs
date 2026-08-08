//! Shared engine admission and graceful-shutdown lifecycle.

use std::{
    fmt,
    sync::{Arc, Mutex},
};

use tokio::sync::Notify;

use super::{EngineError, EngineResult};

/// The monotonic lifecycle state shared by every clone of an [`Engine`](super::Engine).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EngineState {
    /// New operations are admitted.
    Running,
    /// Shutdown has begun; admitted operations are draining and new work is rejected.
    Draining,
    /// Every admitted operation has finished and idle SQLite handles were closed.
    Stopped,
}

/// Outcome of a completed graceful shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownReport {
    forced: bool,
}

impl ShutdownReport {
    pub(crate) const fn graceful() -> Self {
        Self { forced: false }
    }

    pub(crate) const fn forced_shutdown() -> Self {
        Self { forced: true }
    }

    /// Return whether the grace period expired and admitted work was cancelled.
    pub const fn forced(self) -> bool {
        self.forced
    }
}

#[derive(Debug)]
struct LifecycleState {
    phase: EngineState,
    active: usize,
    forced: bool,
    report: Option<ShutdownReport>,
}

/// One mutex protects both admission state and the active count. That makes
/// admission atomic with respect to shutdown observing an empty engine.
pub(crate) struct Lifecycle {
    state: Mutex<LifecycleState>,
    changed: Notify,
}

impl Lifecycle {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(LifecycleState {
                phase: EngineState::Running,
                active: 0,
                forced: false,
                report: None,
            }),
            changed: Notify::new(),
        })
    }

    pub(crate) fn state(&self) -> EngineState {
        self.lock_state().phase
    }

    pub(crate) fn report(&self) -> Option<ShutdownReport> {
        self.lock_state().report
    }

    pub(crate) fn mark_forced(&self) {
        self.lock_state().forced = true;
    }

    pub(crate) fn was_forced(&self) -> bool {
        self.lock_state().forced
    }

    pub(crate) fn try_acquire(self: &Arc<Self>) -> EngineResult<OperationLease> {
        let mut state = self.lock_state();
        if state.phase != EngineState::Running {
            return Err(EngineError::shutting_down(
                "the engine is draining and no longer accepts operations",
            ));
        }
        state.active = state.active.checked_add(1).ok_or_else(|| {
            EngineError::shutting_down("the engine operation counter is exhausted")
        })?;
        Ok(OperationLease {
            lifecycle: Arc::clone(self),
        })
    }

    pub(crate) fn begin_shutdown(&self) -> EngineState {
        let mut state = self.lock_state();
        if state.phase == EngineState::Running {
            state.phase = EngineState::Draining;
            self.changed.notify_waiters();
        }
        state.phase
    }

    pub(crate) async fn wait_for_drain(&self) {
        loop {
            // Register before inspecting the count to avoid losing the final
            // lease's notification.
            let changed = self.changed.notified();
            if self.lock_state().active == 0 {
                return;
            }
            changed.await;
        }
    }

    pub(crate) fn mark_stopped(&self, report: ShutdownReport) {
        let mut state = self.lock_state();
        debug_assert_eq!(state.active, 0);
        state.phase = EngineState::Stopped;
        state.report = Some(report);
        self.changed.notify_waiters();
    }

    #[cfg(test)]
    pub(crate) fn active(&self) -> usize {
        self.lock_state().active
    }

    fn release(&self) {
        let mut state = self.lock_state();
        debug_assert!(state.active > 0);
        state.active = state.active.saturating_sub(1);
        if state.active == 0 {
            self.changed.notify_waiters();
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, LifecycleState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl fmt::Debug for Lifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Lifecycle")
            .field("state", &*self.lock_state())
            .finish_non_exhaustive()
    }
}

/// Counts an admitted operation until all of its resources are cleaned up.
#[derive(Debug)]
pub(crate) struct OperationLease {
    lifecycle: Arc<Lifecycle>,
}

impl Drop for OperationLease {
    fn drop(&mut self) {
        self.lifecycle.release();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::core::EngineErrorKind;

    #[tokio::test]
    async fn admission_and_shutdown_are_one_atomic_monotonic_state_machine() {
        let lifecycle = Lifecycle::new();
        let first = lifecycle.try_acquire().unwrap();
        let second = lifecycle.try_acquire().unwrap();
        assert_eq!(lifecycle.active(), 2);

        assert_eq!(lifecycle.begin_shutdown(), EngineState::Draining);
        assert_eq!(
            lifecycle.try_acquire().unwrap_err().kind(),
            EngineErrorKind::ShuttingDown
        );
        assert_eq!(lifecycle.begin_shutdown(), EngineState::Draining);

        let waiter = {
            let lifecycle = Arc::clone(&lifecycle);
            tokio::spawn(async move { lifecycle.wait_for_drain().await })
        };
        drop(first);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), waiter)
                .await
                .is_err()
        );
        drop(second);
        tokio::time::timeout(Duration::from_secs(1), lifecycle.wait_for_drain())
            .await
            .unwrap();

        lifecycle.mark_stopped(ShutdownReport::graceful());
        assert_eq!(lifecycle.state(), EngineState::Stopped);
        assert_eq!(lifecycle.report(), Some(ShutdownReport::graceful()));
        assert_eq!(lifecycle.begin_shutdown(), EngineState::Stopped);
    }

    #[tokio::test]
    async fn drain_wait_has_no_lost_wakeup_when_already_empty() {
        let lifecycle = Lifecycle::new();
        lifecycle.begin_shutdown();
        tokio::time::timeout(Duration::from_millis(20), lifecycle.wait_for_drain())
            .await
            .expect("an empty lifecycle drains immediately");
    }
}
