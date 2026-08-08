//! In-process admission and quiescence for application-schema migrations.

use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use tokio::sync::Notify;

use crate::core::{EngineError, EngineErrorKind, EngineResult};

/// Current application-schema admission state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SchemaGateState {
    /// Ordinary operations may be admitted.
    Ready,
    /// A migration guard is excluding new ordinary operations while existing
    /// operations drain or migration work runs.
    Migrating,
    /// A durable partial migration must be resumed before ordinary work can run.
    Pending,
}

/// One deterministic view of schema admission and current occupancy.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SchemaGateSnapshot {
    pub(crate) state: SchemaGateState,
    pub(crate) active_operations: usize,
}

#[derive(Debug)]
struct GateData {
    state: SchemaGateState,
    active_operations: usize,
}

#[derive(Debug)]
struct SchemaGateInner {
    data: Mutex<GateData>,
    blocking_quiesced: Condvar,
    async_quiesced: Notify,
}

impl SchemaGateInner {
    fn lock(&self) -> MutexGuard<'_, GateData> {
        self.data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn release_operation(&self) {
        let quiesced = {
            let mut data = self.lock();
            data.active_operations = data
                .active_operations
                .checked_sub(1)
                .expect("a schema operation guard releases exactly one admission");
            data.active_operations == 0
        };

        if quiesced {
            self.blocking_quiesced.notify_all();
            self.async_quiesced.notify_waiters();
        }
    }
}

/// Shared application-schema admission gate.
#[derive(Debug, Clone)]
pub(crate) struct SchemaGate {
    inner: Arc<SchemaGateInner>,
}

impl SchemaGate {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(SchemaGateInner {
                data: Mutex::new(GateData {
                    state: SchemaGateState::Ready,
                    active_operations: 0,
                }),
                blocking_quiesced: Condvar::new(),
                async_quiesced: Notify::new(),
            }),
        }
    }

    /// Return a coherent snapshot for status and diagnostics.
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> SchemaGateSnapshot {
        let data = self.inner.lock();
        SchemaGateSnapshot {
            state: data.state,
            active_operations: data.active_operations,
        }
    }

    /// Admit one ordinary operation only while the application schema is ready.
    pub(crate) fn try_acquire_operation(&self) -> EngineResult<SchemaOperationGuard> {
        let mut data = self.inner.lock();
        match data.state {
            SchemaGateState::Ready => {
                data.active_operations =
                    data.active_operations.checked_add(1).ok_or_else(|| {
                        EngineError::new(
                            EngineErrorKind::Internal,
                            "application-schema operation count overflowed",
                        )
                    })?;
                Ok(SchemaOperationGuard {
                    inner: Arc::clone(&self.inner),
                })
            }
            SchemaGateState::Migrating => Err(EngineError::new(
                EngineErrorKind::Busy,
                "an application-schema migration is in progress",
            )),
            SchemaGateState::Pending => Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "a partial application-schema migration must be resumed",
            )),
        }
    }

    /// Exclude new ordinary work and create the sole migration coordinator.
    ///
    /// A ready gate begins new work and therefore restores `Ready` if the
    /// guard is cancelled before a durable journal is published. A pending
    /// gate begins recovery and restores `Pending` on cancellation.
    pub(crate) fn begin_migration(&self) -> EngineResult<SchemaMigrationGuard> {
        let mut data = self.inner.lock();
        let restore_state = match data.state {
            SchemaGateState::Ready => SchemaGateState::Ready,
            SchemaGateState::Pending => SchemaGateState::Pending,
            SchemaGateState::Migrating => {
                return Err(EngineError::new(
                    EngineErrorKind::Busy,
                    "an application-schema migration is already in progress",
                ));
            }
        };
        data.state = SchemaGateState::Migrating;
        Ok(SchemaMigrationGuard {
            inner: Arc::clone(&self.inner),
            restore_state,
            finished: false,
        })
    }
}

impl Default for SchemaGate {
    fn default() -> Self {
        Self::new()
    }
}

/// Owned admission retained for the complete lifetime of one ordinary operation.
#[derive(Debug)]
#[must_use = "dropping the guard releases schema-operation admission"]
pub(crate) struct SchemaOperationGuard {
    inner: Arc<SchemaGateInner>,
}

impl Drop for SchemaOperationGuard {
    fn drop(&mut self) {
        self.inner.release_operation();
    }
}

/// Exclusive coordinator for one new or resumed schema migration.
#[derive(Debug)]
#[must_use = "dropping an unfinished migration guard restores safe schema admission state"]
pub(crate) struct SchemaMigrationGuard {
    inner: Arc<SchemaGateInner>,
    restore_state: SchemaGateState,
    finished: bool,
}

impl SchemaMigrationGuard {
    /// Wait asynchronously until every operation admitted before migration has left.
    pub(crate) async fn wait_for_quiescence(&self) {
        loop {
            // Register before checking the count. This closes the gap where
            // the final operation exits between the check and the await, while
            // `notify_waiters` also permits more than one diagnostic waiter.
            let notified = self.inner.async_quiesced.notified();
            tokio::pin!(notified);
            let _ = notified.as_mut().enable();
            if self.inner.lock().active_operations == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Wait on a condition variable until every previously admitted operation exits.
    pub(crate) fn wait_for_quiescence_blocking(&self) {
        let mut data = self.inner.lock();
        while data.active_operations != 0 {
            data = self
                .inner
                .blocking_quiesced
                .wait(data)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    /// Make cancellation or unwinding restore `Pending` after the durable
    /// migration journal has been created.
    pub(crate) fn mark_pending_on_drop(&mut self) {
        self.restore_state = SchemaGateState::Pending;
    }

    /// Publish a successfully completed migration and admit ordinary work again.
    pub(crate) fn publish_ready(mut self) -> EngineResult<()> {
        self.publish(SchemaGateState::Ready)
    }

    /// Publish durable partial progress that only another migration may resume.
    #[cfg(test)]
    pub(crate) fn publish_pending(mut self) -> EngineResult<()> {
        self.publish(SchemaGateState::Pending)
    }

    fn publish(&mut self, target: SchemaGateState) -> EngineResult<()> {
        {
            let mut data = self.inner.lock();
            if data.state != SchemaGateState::Migrating {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "application-schema migration guard lost exclusive ownership",
                ));
            }
            if data.active_operations != 0 {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "application-schema migration was published before operations quiesced",
                ));
            }
            data.state = target;
        }
        self.finished = true;
        Ok(())
    }
}

impl Drop for SchemaMigrationGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }

        let mut data = self.inner.lock();
        if data.state == SchemaGateState::Migrating {
            data.state = self.restore_state;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier, mpsc};

    use super::*;

    #[test]
    fn ready_admits_owned_operations_and_drop_releases_exactly_once() {
        let gate = SchemaGate::new();
        let first = gate.try_acquire_operation().unwrap();
        let second = gate.try_acquire_operation().unwrap();
        assert_eq!(
            gate.snapshot(),
            SchemaGateSnapshot {
                state: SchemaGateState::Ready,
                active_operations: 2,
            }
        );

        drop(first);
        assert_eq!(gate.snapshot().active_operations, 1);
        drop(second);
        assert_eq!(gate.snapshot().active_operations, 0);
    }

    #[test]
    fn migrating_is_retryable_busy_and_pending_is_a_failed_precondition() {
        let gate = SchemaGate::new();
        let migration = gate.begin_migration().unwrap();
        let busy = gate.try_acquire_operation().unwrap_err();
        assert_eq!(busy.kind(), EngineErrorKind::Busy);
        assert!(busy.is_retryable());

        migration.publish_pending().unwrap();
        let pending = gate.try_acquire_operation().unwrap_err();
        assert_eq!(pending.kind(), EngineErrorKind::FailedPrecondition);
        assert!(!pending.is_retryable());
    }

    #[test]
    fn only_one_migration_guard_exists_and_pending_can_be_resumed() {
        let gate = SchemaGate::new();
        let migration = gate.begin_migration().unwrap();
        assert_eq!(
            gate.begin_migration().unwrap_err().kind(),
            EngineErrorKind::Busy
        );
        migration.publish_pending().unwrap();

        let resumed = gate.begin_migration().unwrap();
        assert_eq!(gate.snapshot().state, SchemaGateState::Migrating);
        resumed.publish_ready().unwrap();
        assert_eq!(gate.snapshot().state, SchemaGateState::Ready);
    }

    #[test]
    fn guard_drop_restores_ready_before_durability_and_pending_after_it() {
        let gate = SchemaGate::new();
        drop(gate.begin_migration().unwrap());
        assert_eq!(gate.snapshot().state, SchemaGateState::Ready);

        let mut durable = gate.begin_migration().unwrap();
        durable.mark_pending_on_drop();
        drop(durable);
        assert_eq!(gate.snapshot().state, SchemaGateState::Pending);

        drop(gate.begin_migration().unwrap());
        assert_eq!(gate.snapshot().state, SchemaGateState::Pending);
    }

    #[tokio::test]
    async fn async_quiescence_wait_completes_after_all_owned_operations_drop() {
        let gate = SchemaGate::new();
        let first = gate.try_acquire_operation().unwrap();
        let second = gate.try_acquire_operation().unwrap();
        let migration = gate.begin_migration().unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        let waiter = tokio::spawn(async move {
            let _ = started_tx.send(());
            migration.wait_for_quiescence().await;
            migration.publish_ready()
        });
        started_rx.await.unwrap();
        assert_eq!(gate.snapshot().active_operations, 2);
        drop(first);
        assert_eq!(gate.snapshot().active_operations, 1);
        drop(second);

        waiter.await.unwrap().unwrap();
        assert_eq!(gate.snapshot().state, SchemaGateState::Ready);
    }

    #[tokio::test]
    async fn cancelling_an_async_waiter_restores_its_safe_drop_state() {
        let gate = SchemaGate::new();
        let operation = gate.try_acquire_operation().unwrap();
        let migration = gate.begin_migration().unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(async move {
            let _ = started_tx.send(());
            migration.wait_for_quiescence().await;
            migration.publish_ready()
        });
        started_rx.await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert_eq!(gate.snapshot().state, SchemaGateState::Ready);
        drop(operation);

        let operation = gate.try_acquire_operation().unwrap();
        let mut migration = gate.begin_migration().unwrap();
        migration.mark_pending_on_drop();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(async move {
            let _ = started_tx.send(());
            migration.wait_for_quiescence().await;
            migration.publish_ready()
        });
        started_rx.await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert_eq!(gate.snapshot().state, SchemaGateState::Pending);
        drop(operation);
    }

    #[test]
    fn blocking_quiescence_wait_uses_the_same_operation_count() {
        let gate = SchemaGate::new();
        let operation = gate.try_acquire_operation().unwrap();
        let migration = gate.begin_migration().unwrap();
        let rendezvous = Arc::new(Barrier::new(2));
        let thread_rendezvous = Arc::clone(&rendezvous);
        let (complete_tx, complete_rx) = mpsc::channel();

        let waiter = std::thread::spawn(move || {
            thread_rendezvous.wait();
            migration.wait_for_quiescence_blocking();
            migration.publish_ready().unwrap();
            complete_tx.send(()).unwrap();
        });
        rendezvous.wait();
        assert!(complete_rx.try_recv().is_err());
        drop(operation);
        complete_rx.recv().unwrap();
        waiter.join().unwrap();
        assert_eq!(gate.snapshot().state, SchemaGateState::Ready);
    }

    #[test]
    fn publishing_before_quiescence_fails_and_drop_restores_the_origin() {
        let gate = SchemaGate::new();
        let operation = gate.try_acquire_operation().unwrap();
        let error = gate.begin_migration().unwrap().publish_ready().unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert_eq!(gate.snapshot().state, SchemaGateState::Ready);
        assert_eq!(gate.snapshot().active_operations, 1);
        drop(operation);
    }

    #[test]
    fn gate_guards_are_send_sync_and_owned() {
        fn assert_send_sync_static<T: Send + Sync + 'static>() {}
        assert_send_sync_static::<SchemaGate>();
        assert_send_sync_static::<SchemaOperationGuard>();
        assert_send_sync_static::<SchemaMigrationGuard>();
    }
}
