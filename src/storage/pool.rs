//! Bounded per-shard SQLite connection pools.

use std::{
    cell::RefCell,
    ops::Deref,
    sync::{Arc, Mutex, MutexGuard, atomic::Ordering},
    time::{Duration, Instant},
};

#[cfg(test)]
use std::sync::atomic::AtomicU64;

use rusqlite::Connection;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::{
    core::{EngineError, EngineErrorKind, EngineResult, OperationControl},
    sqlite_error,
};

use super::{CONNECTION_BUSY_TIMEOUT, ConnectionHygiene, Storage};

thread_local! {
    static BUSY_OPERATION: RefCell<Option<BusyOperation>> = const { RefCell::new(None) };
}

struct BusyOperation {
    control: Arc<OperationControl>,
    started: Instant,
}

struct BusyOperationGuard;

impl BusyOperationGuard {
    fn install(control: Arc<OperationControl>) -> Self {
        BUSY_OPERATION.with(|operation| {
            let previous = operation.replace(Some(BusyOperation {
                control,
                started: Instant::now(),
            }));
            debug_assert!(previous.is_none(), "SQLite busy controls cannot be nested");
        });
        Self
    }
}

impl Drop for BusyOperationGuard {
    fn drop(&mut self) {
        BUSY_OPERATION.with(|operation| {
            operation.replace(None);
        });
    }
}

fn cancellable_busy_handler(attempt: i32) -> bool {
    BUSY_OPERATION.with(|operation| {
        let operation = operation.borrow();
        let Some(operation) = operation.as_ref() else {
            return false;
        };
        if operation.control.should_stop() || operation.started.elapsed() >= CONNECTION_BUSY_TIMEOUT
        {
            return false;
        }

        // Poll cancellation frequently without turning a locked database into
        // a spin loop. The total wait remains bounded by the configured
        // five-second connection timeout.
        let delay = match attempt {
            ..=9 => Duration::from_millis(1),
            10..=19 => Duration::from_millis(5),
            _ => Duration::from_millis(10),
        };
        std::thread::sleep(
            delay.min(CONNECTION_BUSY_TIMEOUT.saturating_sub(operation.started.elapsed())),
        );
        !operation.control.should_stop() && operation.started.elapsed() < CONNECTION_BUSY_TIMEOUT
    })
}

/// Identity of the engine session allowed to observe a connection's write-local state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConnectionOwner(u64);

impl ConnectionOwner {
    pub(crate) const fn new(session_id: u64) -> Self {
        Self(session_id)
    }
}

/// The bounded pools for every physical shard.
#[derive(Debug, Clone)]
pub(crate) struct ConnectionPools {
    shards: Vec<ShardPool>,
    #[cfg(test)]
    pool_size: usize,
    #[cfg(test)]
    queue_capacity: usize,
    #[cfg(test)]
    close_idle_hook: Arc<Mutex<Option<CloseIdleHook>>>,
}

#[cfg(test)]
#[derive(Debug)]
struct CloseIdleHook {
    started: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
#[derive(Debug)]
struct ControlledBarrier {
    started: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
impl ControlledBarrier {
    fn wait(self, control: &OperationControl) {
        let _ = self.started.send(());
        loop {
            if control.should_stop() {
                return;
            }
            match self.release.recv_timeout(Duration::from_millis(1)) {
                Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            }
        }
    }
}

impl ConnectionPools {
    pub(crate) fn new(
        storage: Storage,
        pool_size: usize,
        queue_capacity: usize,
    ) -> EngineResult<Self> {
        if pool_size == 0 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "connections per shard must be at least 1",
            ));
        }
        let admission_capacity = pool_size
            .checked_add(queue_capacity)
            .filter(|&capacity| capacity <= Semaphore::MAX_PERMITS)
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    "connection and queue capacity exceeds the semaphore limit",
                )
            })?;

        let shards = (0..storage.shard_count())
            .map(|shard| {
                ShardPool::new(
                    storage.clone(),
                    shard,
                    pool_size,
                    queue_capacity,
                    admission_capacity,
                )
            })
            .collect();
        Ok(Self {
            shards,
            #[cfg(test)]
            pool_size,
            #[cfg(test)]
            queue_capacity,
            #[cfg(test)]
            close_idle_hook: Arc::new(Mutex::new(None)),
        })
    }

    /// Reserve bounded admission and one active connection slot for a shard.
    ///
    /// Admission is attempted before waiting for a connection. Once the active
    /// and queued capacity is exhausted, this returns `Busy` immediately.
    pub(crate) async fn acquire_for_owner(
        &self,
        shard: u16,
        owner: ConnectionOwner,
    ) -> EngineResult<PoolPermit> {
        self.shard(shard)?.acquire(owner).await
    }

    /// Reserve one slot on every shard in deterministic shard order.
    pub(crate) async fn acquire_all_for_owner(
        &self,
        owner: ConnectionOwner,
    ) -> EngineResult<Vec<(u16, PoolPermit)>> {
        let mut permits = Vec::with_capacity(self.shards.len());
        for shard in 0..self.shards.len() {
            let shard = u16::try_from(shard).expect("the storage shard count fits in u16");
            permits.push((shard, self.acquire_for_owner(shard, owner).await?));
        }
        Ok(permits)
    }

    /// Drain and close every currently idle SQLite handle.
    ///
    /// The engine calls this on a blocking worker only after the lifecycle's
    /// active-operation count reaches zero, so no checked-out connection can
    /// race back into these idle vectors.
    pub(crate) fn close_idle(&self) -> EngineResult<usize> {
        #[cfg(test)]
        if let Some(hook) = self
            .close_idle_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = hook.started.send(());
            hook.release
                .recv()
                .expect("the shutdown-finalizer test releases idle closing");
        }

        let mut closing = Vec::new();
        for shard in &self.shards {
            let mut idle = shard.inner.lock_idle()?;
            closing.append(&mut idle);
        }
        let closed = closing.len();
        drop(closing);
        Ok(closed)
    }

    #[cfg(test)]
    pub(crate) fn block_next_close_idle(
        &self,
        started: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) {
        let previous = self
            .close_idle_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .replace(CloseIdleHook { started, release });
        assert!(previous.is_none(), "only one close-idle hook may be armed");
    }

    #[cfg(test)]
    pub(crate) fn block_next_connection_setup(
        &self,
        shard: u16,
        started: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> EngineResult<()> {
        let previous = self
            .shard(shard)?
            .inner
            .setup_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .replace(ControlledBarrier { started, release });
        assert!(previous.is_none(), "only one setup hook may be armed");
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn block_next_foreign_probe(
        &self,
        shard: u16,
        started: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> EngineResult<()> {
        let previous = self
            .shard(shard)?
            .inner
            .probe_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .replace(ControlledBarrier { started, release });
        assert!(previous.is_none(), "only one probe hook may be armed");
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn block_next_control_teardown(
        &self,
        shard: u16,
        started: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> EngineResult<()> {
        let previous = self
            .shard(shard)?
            .inner
            .teardown_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .replace(ControlledBarrier { started, release });
        assert!(previous.is_none(), "only one teardown hook may be armed");
        Ok(())
    }

    #[cfg(test)]
    async fn acquire(&self, shard: u16) -> EngineResult<PoolPermit> {
        self.acquire_for_owner(shard, ConnectionOwner::new(1)).await
    }

    #[cfg(test)]
    async fn acquire_all(&self) -> EngineResult<Vec<(u16, PoolPermit)>> {
        self.acquire_all_for_owner(ConnectionOwner::new(1)).await
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> EngineResult<PoolSnapshot> {
        Ok(PoolSnapshot {
            pool_size: self.pool_size,
            queue_capacity: self.queue_capacity,
            shards: self
                .shards
                .iter()
                .map(ShardPool::snapshot)
                .collect::<EngineResult<_>>()?,
        })
    }

    fn shard(&self, shard: u16) -> EngineResult<&ShardPool> {
        self.shards.get(usize::from(shard)).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                format!("shard {shard} is outside the configured pool range"),
            )
        })
    }
}

#[derive(Debug, Clone)]
struct ShardPool {
    inner: Arc<ShardPoolInner>,
}

impl ShardPool {
    fn new(
        storage: Storage,
        shard: u16,
        pool_size: usize,
        _queue_capacity: usize,
        admission_capacity: usize,
    ) -> Self {
        Self {
            inner: Arc::new(ShardPoolInner {
                storage,
                shard,
                #[cfg(test)]
                pool_size,
                #[cfg(test)]
                queue_capacity: _queue_capacity,
                admission: Arc::new(Semaphore::new(admission_capacity)),
                connections: Arc::new(Semaphore::new(pool_size)),
                idle: Mutex::new(Vec::with_capacity(pool_size)),
                #[cfg(test)]
                next_connection_id: AtomicU64::new(1),
                #[cfg(test)]
                opened: AtomicU64::new(0),
                #[cfg(test)]
                checkouts: AtomicU64::new(0),
                #[cfg(test)]
                reused: AtomicU64::new(0),
                #[cfg(test)]
                retired: AtomicU64::new(0),
                #[cfg(test)]
                setup_hook: Mutex::new(None),
                #[cfg(test)]
                probe_hook: Mutex::new(None),
                #[cfg(test)]
                teardown_hook: Mutex::new(None),
            }),
        }
    }

    async fn acquire(&self, owner: ConnectionOwner) -> EngineResult<PoolPermit> {
        let admission = match Arc::clone(&self.inner.admission).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                return Err(EngineError::new(
                    EngineErrorKind::Busy,
                    format!("shard {} connection queue is full", self.inner.shard),
                ));
            }
            Err(TryAcquireError::Closed) => {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    format!("shard {} admission semaphore is closed", self.inner.shard),
                ));
            }
        };
        let connection = Arc::clone(&self.inner.connections)
            .acquire_owned()
            .await
            .map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::Internal,
                    format!("shard {} connection semaphore is closed", self.inner.shard),
                    error,
                )
            })?;

        Ok(PoolPermit {
            pool: self.clone(),
            owner,
            admission,
            connection,
        })
    }

    #[cfg(test)]
    fn snapshot(&self) -> EngineResult<ShardPoolSnapshot> {
        let admitted = self
            .inner
            .pool_size
            .saturating_add(self.inner.queue_capacity)
            .saturating_sub(self.inner.admission.available_permits());
        let active = self
            .inner
            .pool_size
            .saturating_sub(self.inner.connections.available_permits());
        let idle = self.inner.lock_idle()?.len();

        Ok(ShardPoolSnapshot {
            shard: self.inner.shard,
            active,
            queued: admitted.saturating_sub(active),
            idle,
            opened: self.inner.opened.load(Ordering::Relaxed),
            checkouts: self.inner.checkouts.load(Ordering::Relaxed),
            reused: self.inner.reused.load(Ordering::Relaxed),
            retired: self.inner.retired.load(Ordering::Relaxed),
        })
    }
}

#[derive(Debug)]
struct ShardPoolInner {
    storage: Storage,
    shard: u16,
    #[cfg(test)]
    pool_size: usize,
    #[cfg(test)]
    queue_capacity: usize,
    admission: Arc<Semaphore>,
    connections: Arc<Semaphore>,
    idle: Mutex<Vec<ManagedConnection>>,
    #[cfg(test)]
    next_connection_id: AtomicU64,
    #[cfg(test)]
    opened: AtomicU64,
    #[cfg(test)]
    checkouts: AtomicU64,
    #[cfg(test)]
    reused: AtomicU64,
    #[cfg(test)]
    retired: AtomicU64,
    #[cfg(test)]
    setup_hook: Mutex<Option<ControlledBarrier>>,
    #[cfg(test)]
    probe_hook: Mutex<Option<ControlledBarrier>>,
    #[cfg(test)]
    teardown_hook: Mutex<Option<ControlledBarrier>>,
}

impl ShardPoolInner {
    fn lock_idle(&self) -> EngineResult<MutexGuard<'_, Vec<ManagedConnection>>> {
        self.idle.lock().map_err(|error| {
            EngineError::new(
                EngineErrorKind::Internal,
                format!(
                    "shard {} idle connection pool is poisoned: {error}",
                    self.shard
                ),
            )
        })
    }

    fn checkout(
        &self,
        owner: ConnectionOwner,
        control: Option<Arc<OperationControl>>,
    ) -> EngineResult<ManagedConnection> {
        let (reusable, foreign_write_connection) = {
            let mut idle = self.lock_idle()?;
            let reusable_index = idle
                .iter()
                .position(|connection| connection.write_owner == Some(owner))
                .or_else(|| {
                    idle.iter().position(|connection| {
                        connection.write_owner.is_none() && connection.origin_owner == Some(owner)
                    })
                })
                .or_else(|| {
                    idle.iter()
                        .position(|connection| connection.write_owner.is_none())
                });
            match reusable_index {
                Some(index) => (Some(idle.swap_remove(index)), None),
                None => (None, idle.pop()),
            }
        };

        if let Some(connection) = foreign_write_connection {
            self.retire(connection);
        }

        if let Some(connection) = reusable {
            connection.hygiene.wrote.store(false, Ordering::Relaxed);
            #[cfg(test)]
            self.reused.fetch_add(1, Ordering::Relaxed);
            return Ok(connection);
        }

        match control {
            Some(control) => self.open_connection_controlled(control),
            None => self.open_connection(),
        }
    }

    fn open_connection(&self) -> EngineResult<ManagedConnection> {
        let (connection, hygiene) = self.storage.open_pooled_shard(self.shard)?;
        Ok(self.managed_connection(connection, hygiene))
    }

    fn open_connection_controlled(
        &self,
        control: Arc<OperationControl>,
    ) -> EngineResult<ManagedConnection> {
        let mut connection = self.storage.open_unconfigured_shard(self.shard)?;
        configure_connection_controlled(
            &mut connection,
            Arc::clone(&control),
            &self.storage,
            self.shard,
            #[cfg(test)]
            self.take_setup_hook(),
        )?;
        let (connection, hygiene) = self.storage.attach_pool_hygiene(connection)?;
        Ok(self.managed_connection(connection, hygiene))
    }

    #[cfg(test)]
    fn take_setup_hook(&self) -> Option<ControlledBarrier> {
        self.setup_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }

    #[cfg(test)]
    fn take_probe_hook(&self) -> Option<ControlledBarrier> {
        self.probe_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }

    #[cfg(test)]
    fn take_teardown_hook(&self) -> Option<ControlledBarrier> {
        self.teardown_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }

    fn managed_connection(
        &self,
        connection: Connection,
        hygiene: ConnectionHygiene,
    ) -> ManagedConnection {
        #[cfg(test)]
        let id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        self.opened.fetch_add(1, Ordering::Relaxed);
        ManagedConnection {
            #[cfg(test)]
            id,
            connection,
            hygiene,
            write_owner: None,
            origin_owner: None,
        }
    }

    fn retire(&self, connection: ManagedConnection) {
        #[cfg(test)]
        self.retired.fetch_add(1, Ordering::Relaxed);
        drop(connection);
    }

    fn check_in(&self, mut connection: ManagedConnection, owner: ConnectionOwner, broken: bool) {
        let panicking = std::thread::panicking();
        let was_in_transaction = !connection.connection.is_autocommit();
        let mut cleanup_failed = false;
        if was_in_transaction && connection.connection.execute_batch("ROLLBACK").is_err() {
            cleanup_failed = true;
        }
        let tainted = connection.hygiene.tainted.load(Ordering::Relaxed);

        if broken || panicking || was_in_transaction || cleanup_failed || tainted {
            self.retire(connection);
            return;
        }

        if connection.hygiene.wrote.load(Ordering::Relaxed) {
            connection.write_owner = Some(owner);
        }
        if connection.origin_owner.is_none() {
            connection.origin_owner = Some(owner);
        }

        match self.idle.lock() {
            Ok(mut idle) => idle.push(connection),
            Err(_) => self.retire(connection),
        }
    }
}

#[derive(Debug)]
struct ManagedConnection {
    #[cfg(test)]
    id: u64,
    connection: Connection,
    hygiene: ConnectionHygiene,
    write_owner: Option<ConnectionOwner>,
    origin_owner: Option<ConnectionOwner>,
}

/// Admission plus an active connection slot reserved outside a blocking task.
#[derive(Debug)]
pub(crate) struct PoolPermit {
    pool: ShardPool,
    owner: ConnectionOwner,
    admission: OwnedSemaphorePermit,
    connection: OwnedSemaphorePermit,
}

impl PoolPermit {
    /// Checkout or lazily open a connection. Call this on the blocking worker.
    #[cfg(test)]
    pub(crate) fn checkout(self) -> EngineResult<PooledConnection> {
        self.checkout_inner(None)
    }

    /// Checkout while making lazy SQLite configuration cancellable.
    pub(crate) fn checkout_controlled(
        self,
        control: Arc<OperationControl>,
    ) -> EngineResult<PooledConnection> {
        self.checkout_inner(Some(control))
    }

    fn checkout_inner(
        self,
        control: Option<Arc<OperationControl>>,
    ) -> EngineResult<PooledConnection> {
        let PoolPermit {
            pool,
            owner,
            admission,
            connection,
        } = self;
        let managed = pool.inner.checkout(owner, control)?;
        let borrowed_from_other_owner = managed
            .origin_owner
            .is_some_and(|origin_owner| origin_owner != owner);
        #[cfg(test)]
        pool.inner.checkouts.fetch_add(1, Ordering::Relaxed);
        Ok(PooledConnection {
            pool,
            owner,
            managed: Some(managed),
            broken: false,
            borrowed_from_other_owner,
            operation: None,
            _admission: admission,
            _connection: connection,
        })
    }
}

/// A checked-out SQLite connection that returns clean state to its shard pool.
#[derive(Debug)]
pub(crate) struct PooledConnection {
    pool: ShardPool,
    owner: ConnectionOwner,
    managed: Option<ManagedConnection>,
    broken: bool,
    borrowed_from_other_owner: bool,
    operation: Option<Arc<OperationControl>>,
    _admission: OwnedSemaphorePermit,
    _connection: OwnedSemaphorePermit,
}

impl PooledConnection {
    #[cfg(test)]
    pub(crate) fn connection_id(&self) -> u64 {
        self.managed
            .as_ref()
            .expect("a live pooled connection owns its SQLite handle")
            .id
    }

    /// Retire this physical connection instead of returning it to the pool.
    pub(crate) fn mark_broken(&mut self) {
        self.broken = true;
    }

    /// Run work while cancellation is armed against exactly this leased handle.
    ///
    /// The progress hook closes the small race between starting SQLite and an
    /// interrupt reaching the connection. It is always removed before the
    /// handle can return to the pool. Interrupted handles are conservatively
    /// retired so a late SQLite interrupt cannot affect the next owner.
    pub(crate) fn run_controlled<T, F>(
        &mut self,
        control: Arc<OperationControl>,
        work: F,
    ) -> EngineResult<T>
    where
        F: FnOnce(&mut Self) -> EngineResult<T>,
    {
        debug_assert!(self.operation.is_none());
        let _busy_operation = BusyOperationGuard::install(Arc::clone(&control));
        if let Err(error) = self.busy_handler(Some(cancellable_busy_handler)) {
            self.mark_broken();
            return Err(sqlite_error::storage(error)
                .context("failed to install the SQLite cancellable busy handler"));
        }
        let progress_control = Arc::clone(&control);
        if let Err(error) =
            self.progress_handler(1_000, Some(move || progress_control.should_stop()))
        {
            let _ = self.busy_timeout(CONNECTION_BUSY_TIMEOUT);
            self.mark_broken();
            return Err(sqlite_error::storage(error)
                .context("failed to install the SQLite request progress hook"));
        }

        let interrupt_handle = self.get_interrupt_handle();
        if let Err(reason) = control.arm(Arc::new(move || interrupt_handle.interrupt())) {
            if self.progress_handler(0, None::<fn() -> bool>).is_err() {
                self.mark_broken();
            }
            if self.busy_timeout(CONNECTION_BUSY_TIMEOUT).is_err() {
                self.mark_broken();
            }
            return Err(reason.error());
        }
        self.operation = Some(control);

        let result = work(self);
        self.finish_controlled(result)
    }

    fn finish_controlled<T>(&mut self, result: EngineResult<T>) -> EngineResult<T> {
        let control = self
            .operation
            .take()
            .expect("a controlled SQLite operation is armed");
        let cleanup = self
            .progress_handler(0, None::<fn() -> bool>)
            .map_err(sqlite_error::storage);
        let busy_cleanup = self
            .busy_timeout(CONNECTION_BUSY_TIMEOUT)
            .map_err(sqlite_error::storage);
        #[cfg(test)]
        if let Some(barrier) = self.pool.inner.take_teardown_hook() {
            barrier.wait(&control);
        }
        if control.disarm().is_some() {
            self.mark_broken();
        }
        match (result, cleanup, busy_cleanup) {
            (Ok(_), Err(error), _) => {
                self.mark_broken();
                Err(error.context("failed to remove the SQLite request progress hook"))
            }
            (Ok(_), Ok(()), Err(error)) => {
                self.mark_broken();
                Err(error.context("failed to restore the SQLite busy timeout"))
            }
            (result, _, _) => result,
        }
    }

    /// Ensure foreign SQL cannot inherit connection-local history or write on a
    /// handle whose physical history began under another session.
    ///
    /// SQLite reports authorizer actions while preparing a statement. For a
    /// clean handle borrowed from another owner, probe once in an authorizer
    /// mode that denies the first connection-local or write action before it can
    /// have a prepare-time effect. If the probe finds either, replace the handle
    /// before the real execution. Any probe error also fails closed to a fresh
    /// handle. The probe error itself is never exposed; opening the replacement
    /// can return a storage error, and otherwise the real statement boundary is
    /// authoritative for the SQL result.
    #[cfg(test)]
    pub(crate) fn isolate_foreign_sql(&mut self, sql: &str) -> EngineResult<()> {
        if !self.borrowed_from_other_owner {
            return Ok(());
        }

        let requires_fresh = self.foreign_sql_requires_fresh(sql);
        if requires_fresh {
            self.replace_with_fresh()?;
        }
        Ok(())
    }

    /// Probe foreign-owner SQL under the same cancellation hooks as execution.
    pub(crate) fn isolate_foreign_sql_controlled(
        &mut self,
        control: Arc<OperationControl>,
        sql: &str,
    ) -> EngineResult<()> {
        if !self.borrowed_from_other_owner {
            return Ok(());
        }
        let requires_fresh = self.run_controlled(Arc::clone(&control), |connection| {
            Ok(connection.foreign_sql_requires_fresh(sql))
        })?;
        if requires_fresh {
            self.replace_with_fresh_controlled(control)?;
        }
        Ok(())
    }

    fn foreign_sql_requires_fresh(&self, sql: &str) -> bool {
        let managed = self
            .managed
            .as_ref()
            .expect("a live pooled connection owns its SQLite handle");
        let probe = managed.hygiene.begin_probe();
        #[cfg(test)]
        if let Some(barrier) = self.pool.inner.take_probe_hook() {
            barrier.wait(
                self.operation
                    .as_deref()
                    .expect("the foreign-owner probe is cancellation-controlled"),
            );
        }
        let prepared = managed.connection.prepare(sql);
        let preparation_failed = prepared.is_err();
        drop(prepared);
        preparation_failed || probe.requires_fresh_connection()
    }

    pub(crate) fn ensure_owner_local_controlled(
        &mut self,
        control: Arc<OperationControl>,
    ) -> EngineResult<()> {
        if self.borrowed_from_other_owner {
            self.replace_with_fresh_controlled(control)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn replace_with_fresh(&mut self) -> EngineResult<()> {
        let previous = self
            .managed
            .take()
            .expect("a live pooled connection owns its SQLite handle");
        self.pool.inner.retire(previous);
        self.managed = Some(self.pool.inner.open_connection()?);
        self.borrowed_from_other_owner = false;
        Ok(())
    }

    fn replace_with_fresh_controlled(
        &mut self,
        control: Arc<OperationControl>,
    ) -> EngineResult<()> {
        let previous = self
            .managed
            .take()
            .expect("a live pooled connection owns its SQLite handle");
        self.pool.inner.retire(previous);
        self.managed = Some(self.pool.inner.open_connection_controlled(control)?);
        self.borrowed_from_other_owner = false;
        Ok(())
    }
}

fn configure_connection_controlled(
    connection: &mut Connection,
    control: Arc<OperationControl>,
    storage: &Storage,
    shard: u16,
    #[cfg(test)] barrier: Option<ControlledBarrier>,
) -> EngineResult<()> {
    let _busy_operation = BusyOperationGuard::install(Arc::clone(&control));
    connection
        .busy_handler(Some(cancellable_busy_handler))
        .map_err(sqlite_error::storage)?;
    let progress_control = Arc::clone(&control);
    if let Err(error) =
        connection.progress_handler(1_000, Some(move || progress_control.should_stop()))
    {
        let _ = connection.busy_timeout(CONNECTION_BUSY_TIMEOUT);
        return Err(sqlite_error::storage(error).context(
            "failed to install the SQLite request progress hook during connection setup",
        ));
    }
    let interrupt_handle = connection.get_interrupt_handle();
    if let Err(reason) = control.arm(Arc::new(move || interrupt_handle.interrupt())) {
        let _ = connection.progress_handler(0, None::<fn() -> bool>);
        let _ = connection.busy_timeout(CONNECTION_BUSY_TIMEOUT);
        return Err(reason.error());
    }

    #[cfg(test)]
    if let Some(barrier) = barrier {
        barrier.wait(&control);
    }

    // Do not call the ordinary configuration wrapper here: its fixed busy
    // timeout would replace the cancellable handler before these pragmas can
    // encounter a SQLite lock.
    let result = storage.validate_unconfigured_shard(connection, shard);
    let progress_cleanup = connection
        .progress_handler(0, None::<fn() -> bool>)
        .map_err(sqlite_error::storage);
    let busy_cleanup = connection
        .busy_timeout(CONNECTION_BUSY_TIMEOUT)
        .map_err(sqlite_error::storage);
    let reason = control.disarm();
    match (result, progress_cleanup, busy_cleanup, reason) {
        (Err(_), _, _, Some(reason)) => Err(reason.error()),
        (Err(error), _, _, None) => Err(error),
        (Ok(()), _, _, Some(reason)) => Err(reason.error()),
        (Ok(()), Err(error), _, None) => {
            Err(error.context("failed to remove the SQLite request progress hook after setup"))
        }
        (Ok(()), Ok(()), Err(error), None) => {
            Err(error.context("failed to restore the SQLite busy timeout after setup"))
        }
        (Ok(()), Ok(()), Ok(()), None) => Ok(()),
    }
}

impl Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self
            .managed
            .as_ref()
            .expect("a live pooled connection owns its SQLite handle")
            .connection
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(control) = self.operation.take() {
            let _ = self.progress_handler(0, None::<fn() -> bool>);
            let _ = self.busy_timeout(CONNECTION_BUSY_TIMEOUT);
            control.disarm();
            self.broken = true;
        }
        if let Some(connection) = self.managed.take() {
            self.pool
                .inner
                .check_in(connection, self.owner, self.broken);
        }
    }
}

/// A deterministic snapshot of configured pool capacity and shard counters.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PoolSnapshot {
    pub(crate) pool_size: usize,
    pub(crate) queue_capacity: usize,
    pub(crate) shards: Vec<ShardPoolSnapshot>,
}

/// Counters and current occupancy for one shard pool.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ShardPoolSnapshot {
    pub(crate) shard: u16,
    pub(crate) active: usize,
    pub(crate) queued: usize,
    pub(crate) idle: usize,
    pub(crate) opened: u64,
    pub(crate) checkouts: u64,
    pub(crate) reused: u64,
    pub(crate) retired: u64,
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::Arc,
        time::Duration,
    };

    use rusqlite::hooks::{AuthAction, TransactionOperation};
    use tokio::time::timeout;

    use super::*;
    use crate::storage::{action_taints_connection, action_writes_connection};

    fn pools(pool_size: usize, queue_capacity: usize) -> (tempfile::TempDir, ConnectionPools) {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let pools = ConnectionPools::new(storage, pool_size, queue_capacity).unwrap();
        (temp, pools)
    }

    async fn wait_for_occupancy(pools: &ConnectionPools, shard: u16, active: usize, queued: usize) {
        timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = pools.snapshot().unwrap().shards[usize::from(shard)];
                if snapshot.active == active && snapshot.queued == queued {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn validates_capacity_before_constructing_semaphores() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();

        let zero = ConnectionPools::new(storage.clone(), 0, 1).unwrap_err();
        assert_eq!(zero.kind(), EngineErrorKind::InvalidArgument);
        let excessive = ConnectionPools::new(storage, Semaphore::MAX_PERMITS, 1).unwrap_err();
        assert_eq!(excessive.kind(), EngineErrorKind::InvalidArgument);
    }

    #[tokio::test]
    async fn rejects_a_shard_outside_the_pool_layout() {
        let (_temp, pools) = pools(1, 0);
        let error = pools.acquire(2).await.unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert_eq!(
            error.to_string(),
            "shard 2 is outside the configured pool range"
        );
    }

    #[tokio::test]
    async fn exact_active_and_queue_capacity_apply_backpressure_and_recover() {
        let (_temp, pools) = pools(1, 1);
        let first = pools.acquire(0).await.unwrap().checkout().unwrap();

        let queued_pools = pools.clone();
        let queued = tokio::spawn(async move { queued_pools.acquire(0).await });
        wait_for_occupancy(&pools, 0, 1, 1).await;

        let overflow = pools.acquire(0).await.unwrap_err();
        assert_eq!(overflow.kind(), EngineErrorKind::Busy);
        assert!(overflow.is_retryable());

        drop(first);
        let queued = queued.await.unwrap().unwrap().checkout().unwrap();
        drop(queued);
        wait_for_occupancy(&pools, 0, 0, 0).await;

        let snapshot = pools.snapshot().unwrap();
        assert_eq!(snapshot.pool_size, 1);
        assert_eq!(snapshot.queue_capacity, 1);
        assert_eq!(snapshot.shards[0].opened, 1);
        assert_eq!(snapshot.shards[0].checkouts, 2);
        assert_eq!(snapshot.shards[0].reused, 1);
    }

    #[tokio::test]
    async fn cancelling_a_queued_acquire_releases_its_admission_slot() {
        let (_temp, pools) = pools(1, 1);
        let first = pools.acquire(0).await.unwrap().checkout().unwrap();

        let queued_pools = pools.clone();
        let queued = tokio::spawn(async move { queued_pools.acquire(0).await });
        wait_for_occupancy(&pools, 0, 1, 1).await;
        queued.abort();
        assert!(queued.await.unwrap_err().is_cancelled());
        wait_for_occupancy(&pools, 0, 1, 0).await;

        let replacement_pools = pools.clone();
        let replacement = tokio::spawn(async move { replacement_pools.acquire(0).await });
        wait_for_occupancy(&pools, 0, 1, 1).await;
        drop(first);
        drop(replacement.await.unwrap().unwrap().checkout().unwrap());
        wait_for_occupancy(&pools, 0, 0, 0).await;
    }

    #[tokio::test]
    async fn connections_open_lazily_and_keep_stable_ids_when_reused() {
        let (_temp, pools) = pools(2, 0);
        assert_eq!(pools.snapshot().unwrap().shards[0].opened, 0);

        let first = pools.acquire(0).await.unwrap().checkout().unwrap();
        let first_id = first.connection_id();
        drop(first);
        let second = pools.acquire(0).await.unwrap().checkout().unwrap();
        assert_eq!(second.connection_id(), first_id);
        drop(second);

        let snapshot = pools.snapshot().unwrap().shards[0];
        assert_eq!(snapshot.opened, 1);
        assert_eq!(snapshot.idle, 1);
        assert_eq!(snapshot.checkouts, 2);
        assert_eq!(snapshot.reused, 1);
        assert_eq!(snapshot.retired, 0);
    }

    #[tokio::test]
    async fn lazy_open_preserves_storage_error_classification() {
        let (temp, pools) = pools(1, 0);
        fs::write(
            temp.path().join("shards/0000.sqlite"),
            b"this is not a SQLite database",
        )
        .unwrap();

        let error = pools.acquire(0).await.unwrap().checkout().unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        let snapshot = pools.snapshot().unwrap().shards[0];
        assert_eq!(snapshot.active, 0);
        assert_eq!(snapshot.opened, 0);
        assert_eq!(snapshot.checkouts, 0);
    }

    #[tokio::test]
    async fn ordinary_implicit_dml_can_reuse_a_connection_within_one_owner() {
        let (_temp, pools) = pools(1, 0);
        let mut expected_id = None;
        for sql in [
            "CREATE TABLE widgets (id INTEGER PRIMARY KEY, value INTEGER)",
            "INSERT INTO widgets (id, value) VALUES (1, 10)",
            "UPDATE widgets SET value = 20 WHERE id = 1",
            "DELETE FROM widgets WHERE id = 1",
        ] {
            let connection = pools.acquire(0).await.unwrap().checkout().unwrap();
            let id = connection.connection_id();
            assert_eq!(*expected_id.get_or_insert(id), id);
            connection.execute_batch(sql).unwrap();
            drop(connection);
        }

        let snapshot = pools.snapshot().unwrap().shards[0];
        assert_eq!(snapshot.opened, 1);
        assert_eq!(snapshot.reused, 3);
        assert_eq!(snapshot.retired, 0);
        assert_eq!(snapshot.idle, 1);
    }

    #[tokio::test]
    async fn write_local_sqlite_state_never_crosses_connection_owners() {
        let (_temp, pools) = pools(1, 0);
        let first_owner = ConnectionOwner::new(11);
        let second_owner = ConnectionOwner::new(22);
        let third_owner = ConnectionOwner::new(33);

        let first = pools
            .acquire_for_owner(0, first_owner)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        first
            .execute_batch(
                "CREATE TABLE widgets (id INTEGER PRIMARY KEY);
                 INSERT INTO widgets (id) VALUES (1);",
            )
            .unwrap();
        let first_id = first.connection_id();
        assert_eq!(
            first
                .query_row(
                    "SELECT last_insert_rowid(), changes(), total_changes()",
                    [],
                    |row| Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?
                    )),
                )
                .unwrap(),
            (1, 1, 1)
        );
        drop(first);

        let second = pools
            .acquire_for_owner(0, second_owner)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        let second_id = second.connection_id();
        assert_ne!(second_id, first_id);
        assert_eq!(
            second
                .query_row(
                    "SELECT last_insert_rowid(), changes(), total_changes()",
                    [],
                    |row| Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?
                    )),
                )
                .unwrap(),
            (0, 0, 0)
        );
        assert_eq!(
            second
                .query_row("SELECT COUNT(*) FROM widgets", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        drop(second);

        let third = pools
            .acquire_for_owner(0, third_owner)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        assert_eq!(third.connection_id(), second_id);
        drop(third);

        let snapshot = pools.snapshot().unwrap().shards[0];
        assert_eq!(snapshot.opened, 2);
        assert_eq!(snapshot.checkouts, 3);
        assert_eq!(snapshot.reused, 1);
        assert_eq!(snapshot.retired, 1);
        assert_eq!(snapshot.idle, 1);
    }

    #[tokio::test]
    async fn a_foreign_write_moves_to_a_fresh_handle_and_keeps_its_own_counters() {
        let (_temp, pools) = pools(1, 0);
        let setup = pools.shards[0].inner.storage.open_shard(0).unwrap();
        setup
            .execute_batch("CREATE TABLE widgets (id INTEGER PRIMARY KEY)")
            .unwrap();
        drop(setup);

        let first_owner = ConnectionOwner::new(11);
        let writer = ConnectionOwner::new(22);
        let first = pools
            .acquire_for_owner(0, first_owner)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        first.query_row("SELECT 1", [], |_| Ok(())).unwrap();
        let first_id = first.connection_id();
        drop(first);

        let mut write = pools
            .acquire_for_owner(0, writer)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        assert_eq!(write.connection_id(), first_id);
        write
            .isolate_foreign_sql("INSERT INTO widgets (id) VALUES (1)")
            .unwrap();
        let writer_id = write.connection_id();
        assert_ne!(writer_id, first_id);
        assert_eq!(
            write
                .execute("INSERT INTO widgets (id) VALUES (1)", [])
                .unwrap(),
            1
        );
        assert_eq!(
            write
                .query_row(
                    "SELECT last_insert_rowid(), changes(), total_changes()",
                    [],
                    |row| Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?
                    )),
                )
                .unwrap(),
            (1, 1, 1)
        );
        drop(write);

        let writer_again = pools
            .acquire_for_owner(0, writer)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        assert_eq!(writer_again.connection_id(), writer_id);
        assert!(!writer_again.borrowed_from_other_owner);
        assert_eq!(
            writer_again
                .query_row(
                    "SELECT last_insert_rowid(), changes(), total_changes()",
                    [],
                    |row| Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?
                    )),
                )
                .unwrap(),
            (1, 1, 1)
        );
        drop(writer_again);

        let snapshot = pools.snapshot().unwrap().shards[0];
        assert_eq!(snapshot.opened, 2);
        assert_eq!(snapshot.checkouts, 3);
        assert_eq!(snapshot.reused, 2);
        assert_eq!(snapshot.retired, 1);
        assert_eq!(snapshot.idle, 1);
    }

    #[tokio::test]
    async fn connection_local_sql_on_a_foreign_read_handle_moves_to_a_fresh_connection() {
        let (_temp, pools) = pools(2, 0);
        let first_owner = ConnectionOwner::new(11);
        let writer = ConnectionOwner::new(22);
        let observer = ConnectionOwner::new(33);

        let first = pools
            .acquire_for_owner(0, first_owner)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        first.query_row("SELECT 1", [], |_| Ok(())).unwrap();
        let first_id = first.connection_id();

        let writer_connection = pools
            .acquire_for_owner(0, writer)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        writer_connection
            .execute_batch("CREATE TABLE data_version_marker (id INTEGER PRIMARY KEY)")
            .unwrap();
        drop(writer_connection);
        drop(first);

        let fresh_control = pools.shards[0].inner.storage.open_shard(0).unwrap();
        let fresh_data_version = fresh_control
            .query_row("PRAGMA data_version", [], |row| row.get::<_, i64>(0))
            .unwrap();

        let mut observed = pools
            .acquire_for_owner(0, observer)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        assert_eq!(observed.connection_id(), first_id);
        assert!(observed.borrowed_from_other_owner);

        observed.isolate_foreign_sql("PRAGMA data_version").unwrap();
        assert_ne!(observed.connection_id(), first_id);
        assert!(!observed.borrowed_from_other_owner);
        let data_version = observed
            .query_row("PRAGMA data_version", [], |row| row.get::<_, i64>(0))
            .unwrap();
        assert_eq!(data_version, fresh_data_version);
        drop(observed);

        let snapshot = pools.snapshot().unwrap().shards[0];
        assert_eq!(snapshot.opened, 3);
        assert_eq!(snapshot.checkouts, 3);
        assert_eq!(snapshot.reused, 1);
        assert_eq!(snapshot.retired, 2);
        assert_eq!(snapshot.idle, 1);
    }

    #[tokio::test]
    async fn an_ordinary_foreign_read_never_claims_the_handles_local_history() {
        let (_temp, pools) = pools(1, 0);
        let first_owner = ConnectionOwner::new(11);
        let second_owner = ConnectionOwner::new(22);

        let first = pools
            .acquire_for_owner(0, first_owner)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        first.query_row("SELECT 1", [], |_| Ok(())).unwrap();
        let original_id = first.connection_id();
        drop(first);

        let mut ordinary_read = pools
            .acquire_for_owner(0, second_owner)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        assert_eq!(ordinary_read.connection_id(), original_id);
        assert!(ordinary_read.borrowed_from_other_owner);
        ordinary_read.isolate_foreign_sql("SELECT 1").unwrap();
        ordinary_read.query_row("SELECT 1", [], |_| Ok(())).unwrap();
        drop(ordinary_read);

        let mut local_observer = pools
            .acquire_for_owner(0, second_owner)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        assert_eq!(local_observer.connection_id(), original_id);
        assert!(local_observer.borrowed_from_other_owner);
        local_observer
            .isolate_foreign_sql("PRAGMA data_version")
            .unwrap();
        assert_ne!(local_observer.connection_id(), original_id);
        local_observer
            .query_row("PRAGMA data_version", [], |_| Ok(()))
            .unwrap();
        drop(local_observer);

        let snapshot = pools.snapshot().unwrap().shards[0];
        assert_eq!(snapshot.opened, 2);
        assert_eq!(snapshot.checkouts, 3);
        assert_eq!(snapshot.reused, 2);
        assert_eq!(snapshot.retired, 2);
        assert_eq!(snapshot.idle, 0);
    }

    #[tokio::test]
    async fn storage_owned_pragmas_remain_denied_after_probe_isolation() {
        let (temp, pools) = pools(1, 0);
        let first_owner = ConnectionOwner::new(11);
        let second_owner = ConnectionOwner::new(22);

        let first = pools
            .acquire_for_owner(0, first_owner)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        first.query_row("SELECT 1", [], |_| Ok(())).unwrap();
        drop(first);

        let control = Connection::open(temp.path().join("shards/0000.sqlite")).unwrap();
        assert_eq!(
            control
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );

        let mut second = pools
            .acquire_for_owner(0, second_owner)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        second
            .isolate_foreign_sql("PRAGMA user_version = 7")
            .unwrap();
        assert_eq!(
            control
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0,
            "the authorizer probe must deny the PRAGMA before any prepare-time effect"
        );

        let error = second.execute_batch("PRAGMA user_version = 7").unwrap_err();
        assert!(matches!(
            error,
            rusqlite::Error::SqliteFailure(_, _) | rusqlite::Error::SqlInputError { .. }
        ));
        assert_eq!(
            control
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        drop(second);

        let snapshot = pools.snapshot().unwrap().shards[0];
        assert_eq!(snapshot.opened, 2);
        assert_eq!(snapshot.retired, 1);
        assert_eq!(snapshot.idle, 1);
    }

    #[tokio::test]
    async fn every_probe_error_fails_closed_to_a_fresh_handle() {
        let (_temp, pools) = pools(1, 0);
        let first_owner = ConnectionOwner::new(11);
        let second_owner = ConnectionOwner::new(22);

        let first = pools
            .acquire_for_owner(0, first_owner)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        first.query_row("SELECT 1", [], |_| Ok(())).unwrap();
        let first_id = first.connection_id();
        drop(first);

        let mut second = pools
            .acquire_for_owner(0, second_owner)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        assert_eq!(second.connection_id(), first_id);
        second.isolate_foreign_sql("this is not valid SQL").unwrap();
        assert_ne!(second.connection_id(), first_id);
        assert!(second.prepare("this is not valid SQL").is_err());
        drop(second);

        let snapshot = pools.snapshot().unwrap().shards[0];
        assert_eq!(snapshot.opened, 2);
        assert_eq!(snapshot.reused, 1);
        assert_eq!(snapshot.retired, 1);
        assert_eq!(snapshot.idle, 1);
    }

    #[tokio::test]
    async fn failed_fresh_replacement_preserves_storage_error_and_recovers_capacity() {
        let (temp, pools) = pools(1, 0);
        let first_owner = ConnectionOwner::new(11);
        let second_owner = ConnectionOwner::new(22);

        let first = pools
            .acquire_for_owner(0, first_owner)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        first.query_row("SELECT 1", [], |_| Ok(())).unwrap();
        drop(first);

        let shard_path = temp.path().join("shards/0000.sqlite");
        let backup_path = temp.path().join("shards/0000.sqlite.backup");
        fs::rename(&shard_path, &backup_path).unwrap();
        fs::create_dir(&shard_path).unwrap();

        let mut second = pools
            .acquire_for_owner(0, second_owner)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        let error = second
            .isolate_foreign_sql("PRAGMA data_version")
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        drop(second);

        let failed = pools.snapshot().unwrap().shards[0];
        assert_eq!(failed.active, 0);
        assert_eq!(failed.queued, 0);
        assert_eq!(failed.idle, 0);
        assert_eq!(failed.opened, 1);
        assert_eq!(failed.retired, 1);

        fs::remove_dir(&shard_path).unwrap();
        fs::rename(&backup_path, &shard_path).unwrap();

        let recovered = pools
            .acquire_for_owner(0, second_owner)
            .await
            .unwrap()
            .checkout()
            .unwrap();
        recovered.query_row("SELECT 1", [], |_| Ok(())).unwrap();
        drop(recovered);

        let recovered = pools.snapshot().unwrap().shards[0];
        assert_eq!(recovered.active, 0);
        assert_eq!(recovered.queued, 0);
        assert_eq!(recovered.idle, 1);
        assert_eq!(recovered.opened, 2);
        assert_eq!(recovered.retired, 1);
    }

    #[tokio::test]
    async fn ordinary_sql_errors_return_a_clean_connection_to_the_pool() {
        let (_temp, pools) = pools(1, 0);
        let connection = pools.acquire(0).await.unwrap().checkout().unwrap();
        connection
            .execute_batch("CREATE TABLE widgets (id INTEGER PRIMARY KEY)")
            .unwrap();
        connection
            .execute("INSERT INTO widgets (id) VALUES (1)", [])
            .unwrap();
        let original_id = connection.connection_id();
        let error = connection
            .execute("INSERT INTO widgets (id) VALUES (1)", [])
            .unwrap_err();
        assert_eq!(
            error.sqlite_error_code(),
            Some(rusqlite::ErrorCode::ConstraintViolation)
        );
        drop(connection);

        let replacement = pools.acquire(0).await.unwrap().checkout().unwrap();
        assert_eq!(replacement.connection_id(), original_id);
        drop(replacement);
        let snapshot = pools.snapshot().unwrap().shards[0];
        assert_eq!(snapshot.opened, 1);
        assert_eq!(snapshot.reused, 1);
        assert_eq!(snapshot.retired, 0);
    }

    #[tokio::test]
    async fn non_autocommit_state_is_rolled_back_before_a_tainted_connection_retires() {
        let (_temp, pools) = pools(1, 0);
        let connection = pools.acquire(0).await.unwrap().checkout().unwrap();
        connection
            .execute_batch("CREATE TABLE widgets (id INTEGER PRIMARY KEY)")
            .unwrap();
        let original_id = connection.connection_id();
        drop(connection);

        let connection = pools.acquire(0).await.unwrap().checkout().unwrap();
        assert_eq!(connection.connection_id(), original_id);
        connection
            .execute_batch("BEGIN; INSERT INTO widgets (id) VALUES (1)")
            .unwrap();
        assert!(!connection.is_autocommit());
        drop(connection);

        let replacement = pools.acquire(0).await.unwrap().checkout().unwrap();
        assert_ne!(replacement.connection_id(), original_id);
        assert_eq!(
            replacement
                .query_row("SELECT COUNT(*) FROM widgets", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        drop(replacement);
        let snapshot = pools.snapshot().unwrap().shards[0];
        assert_eq!(snapshot.opened, 2);
        assert_eq!(snapshot.retired, 1);
    }

    #[test]
    fn authorizer_taints_every_connection_local_or_unknown_action() {
        let actions = [
            AuthAction::Unknown {
                code: -1,
                arg1: None,
                arg2: None,
            },
            AuthAction::CreateTempIndex {
                index_name: "i",
                table_name: "t",
            },
            AuthAction::CreateTempTable { table_name: "t" },
            AuthAction::CreateTempTrigger {
                trigger_name: "tr",
                table_name: "t",
            },
            AuthAction::CreateTempView { view_name: "v" },
            AuthAction::DropTempIndex {
                index_name: "i",
                table_name: "t",
            },
            AuthAction::DropTempTable { table_name: "t" },
            AuthAction::DropTempTrigger {
                trigger_name: "tr",
                table_name: "t",
            },
            AuthAction::DropTempView { view_name: "v" },
            AuthAction::Pragma {
                pragma_name: "cache_size",
                pragma_value: None,
            },
            AuthAction::Transaction {
                operation: TransactionOperation::Begin,
            },
            AuthAction::Attach {
                filename: "other.sqlite",
            },
            AuthAction::Detach {
                database_name: "other",
            },
            AuthAction::CreateVtable {
                table_name: "vt",
                module_name: "module",
            },
            AuthAction::DropVtable {
                table_name: "vt",
                module_name: "module",
            },
            AuthAction::Savepoint {
                operation: TransactionOperation::Begin,
                savepoint_name: "s",
            },
        ];

        assert!(actions.into_iter().all(action_taints_connection));
        assert!(!action_taints_connection(AuthAction::Select));
        assert!(!action_taints_connection(AuthAction::Insert {
            table_name: "widgets"
        }));
        assert!(action_writes_connection(AuthAction::Insert {
            table_name: "widgets"
        }));
        assert!(action_writes_connection(AuthAction::Update {
            table_name: "widgets",
            column_name: "value",
        }));
        assert!(action_writes_connection(AuthAction::Delete {
            table_name: "widgets"
        }));
        assert!(!action_writes_connection(AuthAction::Select));
    }

    #[tokio::test]
    async fn table_valued_data_version_uses_the_same_connection_local_authorizer_boundary() {
        let (_temp, pools) = pools(1, 0);
        let connection = pools.acquire(0).await.unwrap().checkout().unwrap();
        connection
            .query_row("SELECT * FROM pragma_data_version", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        assert!(
            connection
                .managed
                .as_ref()
                .unwrap()
                .hygiene
                .tainted
                .load(Ordering::Relaxed)
        );
    }

    #[tokio::test]
    async fn stateful_sql_runs_normally_for_one_lease_then_retires_the_connection() {
        let (_temp, pools) = pools(1, 0);
        let connection = pools.acquire(0).await.unwrap().checkout().unwrap();
        let id = connection.connection_id();
        assert_eq!(
            connection
                .query_row("PRAGMA busy_timeout", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            5_000
        );
        drop(connection);

        let replacement = pools.acquire(0).await.unwrap().checkout().unwrap();
        assert_ne!(replacement.connection_id(), id);
        drop(replacement);
        assert_eq!(pools.snapshot().unwrap().shards[0].retired, 1);
    }

    #[tokio::test]
    async fn every_new_pooled_connection_has_the_storage_pragmas() {
        let (_temp, pools) = pools(1, 0);
        let connection = pools.acquire(0).await.unwrap().checkout().unwrap();

        assert_eq!(
            connection
                .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "wal"
        );
        assert_eq!(
            connection
                .query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            connection
                .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            connection
                .query_row("PRAGMA busy_timeout", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            5_000
        );
    }

    #[tokio::test]
    async fn explicit_broken_and_panicking_connections_are_discarded() {
        let (_temp, pools) = pools(1, 0);
        let mut broken = pools.acquire(0).await.unwrap().checkout().unwrap();
        let broken_id = broken.connection_id();
        broken.mark_broken();
        drop(broken);

        let permit = pools.acquire(0).await.unwrap();
        let panicking_id = Arc::new(AtomicU64::new(0));
        let observed_id = Arc::clone(&panicking_id);
        let panic = catch_unwind(AssertUnwindSafe(|| {
            let connection = permit.checkout().unwrap();
            observed_id.store(connection.connection_id(), Ordering::Relaxed);
            panic!("intentional pooled worker panic");
        }));
        assert!(panic.is_err());

        let replacement = pools.acquire(0).await.unwrap().checkout().unwrap();
        assert_ne!(replacement.connection_id(), broken_id);
        assert_ne!(
            replacement.connection_id(),
            panicking_id.load(Ordering::Relaxed)
        );
        drop(replacement);
        let snapshot = pools.snapshot().unwrap().shards[0];
        assert_eq!(snapshot.retired, 2);
        assert_eq!(snapshot.opened, 3);
    }

    #[tokio::test]
    async fn acquire_all_reserves_one_permit_for_each_shard() {
        let (_temp, pools) = pools(1, 0);
        let permits = pools.acquire_all().await.unwrap();
        assert_eq!(
            permits.iter().map(|(shard, _)| *shard).collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(pools.snapshot().unwrap().shards[0].active, 1);
        assert_eq!(pools.snapshot().unwrap().shards[1].active, 1);
        drop(permits);
        wait_for_occupancy(&pools, 0, 0, 0).await;
        wait_for_occupancy(&pools, 1, 0, 0).await;
    }
}
