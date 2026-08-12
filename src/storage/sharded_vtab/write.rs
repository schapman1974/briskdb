//! Writable coordinator execution and one-shard transaction state.

use std::{
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use rusqlite::{
    Connection, InterruptHandle, Params, Result as SqliteResult, types::ValueRef,
    vtab::ConflictMode,
};

use super::{
    ALLOCATION_OVERHEAD_BYTES, CoordinatorCancellation, CursorLimits, ROW_ACCOUNTING_BYTES,
    RawCell, Registry, TableSpec, VALUE_ACCOUNTING_BYTES, allocation_error,
    attach_writable_coordinator_authorizer, bootstrap_coordinator_schema, cancelled_error,
    limit_error, locator, module_v2,
};
#[cfg(test)]
use super::{TestChildScanControl, TestChildScanGate};
use crate::{
    core::{EngineError, EngineErrorKind, EngineResult, Value},
    sqlite_error,
    storage::{CONNECTION_BUSY_TIMEOUT, SchemaOperationGuard, Storage},
};

const CANCELLABLE_BUSY_SLICE: Duration = Duration::from_millis(25);

#[derive(Debug, PartialEq)]
pub(crate) struct CoordinatorWriteResult {
    pub(crate) affected_rows: usize,
    pub(crate) explicit_key: Option<Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SavepointOperation {
    Savepoint,
    Release,
    RollbackTo,
}

impl SavepointOperation {
    const fn name(self) -> &'static str {
        match self {
            Self::Savepoint => "SAVEPOINT",
            Self::Release => "RELEASE",
            Self::RollbackTo => "ROLLBACK TO",
        }
    }
}

fn injected_savepoint_error(operation: SavepointOperation, kind: EngineErrorKind) -> EngineError {
    EngineError::new(
        kind,
        format!("injected physical child {} failure", operation.name()),
    )
}

/// The only supported owner of a writable coordinator connection.
///
/// The underlying SQLite handle is intentionally private: every statement has
/// to pass through reconciliation so a fallible physical child commit is
/// observed before success is acknowledged.
pub(crate) struct WriteCoordinator {
    connection: Connection,
    registry: Arc<Registry>,
    cancellation: CoordinatorCancellation,
    allow_transaction_control: Arc<AtomicBool>,
    broken: bool,
    #[cfg(test)]
    statement_arm_gate: Mutex<Option<TestChildScanGate>>,
}

impl WriteCoordinator {
    pub(crate) fn open(storage: Storage) -> EngineResult<Self> {
        let bootstrap_operation = storage.enter_schema_operation()?;
        let connection = Connection::open_in_memory().map_err(sqlite_error::storage)?;
        let registry = Registry::build_writable(storage, CursorLimits::default())?;
        module_v2::register_module(&connection, Arc::clone(&registry))
            .map_err(sqlite_error::storage)?;
        let bootstrap_epoch = registry.cancellation_epoch.load(Ordering::Acquire);
        let bootstrap_arm = registry.write_state().arm_statement(bootstrap_epoch)?;
        bootstrap_coordinator_schema(&connection, &registry)?;

        connection
            .pragma_update(None, "trusted_schema", "OFF")
            .map_err(sqlite_error::storage)?;

        // CREATE VIRTUAL TABLE can enlist the module in the coordinator's
        // bootstrap transaction. Drain that callback-only, childless state
        // before exposing the execution wrapper.
        match registry.write_state().finalize_terminal(&registry)? {
            FinalizedWrite::None | FinalizedWrite::Committed(_) | FinalizedWrite::RolledBack => {}
        }
        drop(bootstrap_arm);

        let allow_transaction_control = Arc::new(AtomicBool::new(false));
        attach_writable_coordinator_authorizer(
            &connection,
            &registry,
            Arc::clone(&allow_transaction_control),
        )?;
        if registry.storage.current_schema_generation() != registry.schema_generation {
            return Err(EngineError::new(
                EngineErrorKind::Busy,
                "application schema changed while the writable brisk_shard coordinator was opening",
            ));
        }
        drop(bootstrap_operation);

        let cancellation = CoordinatorCancellation {
            epoch: Arc::clone(&registry.cancellation_epoch),
            active_child_scans: Arc::clone(&registry.active_child_scans),
            interrupt: Arc::new(connection.get_interrupt_handle()),
            write_state: registry.write_state.clone(),
        };
        Ok(Self {
            connection,
            registry,
            cancellation,
            allow_transaction_control,
            broken: false,
            #[cfg(test)]
            statement_arm_gate: Mutex::new(None),
        })
    }

    pub(crate) fn execute_dml<P: Params>(
        &mut self,
        sql: &str,
        parameters: P,
    ) -> EngineResult<CoordinatorWriteResult> {
        self.ensure_usable()?;
        let epoch = self.registry.cancellation_epoch.load(Ordering::Acquire);
        let statement_arm = self.registry.write_state().arm_statement(epoch)?;
        self.registry.write_state().reset_statement_outcome()?;

        let execution = (|| {
            #[cfg(test)]
            self.wait_after_statement_arm_for_test()?;
            let mut statement = self
                .connection
                .prepare(sql)
                .map_err(sqlite_error::statement)?;
            if statement.readonly() {
                return Err(EngineError::new(
                    EngineErrorKind::Unsupported,
                    "writable brisk_shard execution accepts one INSERT, UPDATE, or DELETE statement",
                ));
            }
            if statement.column_count() != 0 {
                return Err(EngineError::new(
                    EngineErrorKind::Unsupported,
                    "RETURNING is not supported by the experimental writable brisk_shard facade",
                ));
            }
            statement
                .execute(parameters)
                .map_err(sqlite_error::statement)
        })();

        let result = self.reconcile_statement(execution);
        drop(statement_arm);
        result
    }

    pub(crate) fn begin(&mut self) -> EngineResult<()> {
        self.ensure_usable()?;
        if !self.connection.is_autocommit() {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "writable brisk_shard coordinator already has an explicit transaction",
            ));
        }
        self.execute_transaction_control("BEGIN")?;
        if self.connection.is_autocommit() {
            self.broken = true;
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "SQLite did not enter the requested coordinator transaction",
            ));
        }
        Ok(())
    }

    pub(crate) fn commit(&mut self) -> EngineResult<()> {
        self.ensure_usable()?;
        if self.connection.is_autocommit() {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "writable brisk_shard coordinator has no explicit transaction to commit",
            ));
        }

        if let Err(error) = self.execute_transaction_control("COMMIT") {
            if self.registry.write_state().abort_required(&self.registry) {
                self.abort_explicit_transaction();
            }
            return Err(error);
        }
        if !self.connection.is_autocommit() {
            self.broken = true;
            self.abort_explicit_transaction();
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "SQLite coordinator commit completed without leaving transaction mode",
            ));
        }

        match self
            .registry
            .write_state()
            .finalize_terminal(&self.registry)
        {
            Ok(FinalizedWrite::None | FinalizedWrite::Committed(_)) => Ok(()),
            Ok(FinalizedWrite::RolledBack) => Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "writable brisk_shard transaction was rolled back instead of committed",
            )),
            Err(error) => {
                self.broken = true;
                Err(error)
            }
        }
    }

    pub(crate) fn rollback(&mut self) -> EngineResult<()> {
        if self.connection.is_autocommit() {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "writable brisk_shard coordinator has no explicit transaction to roll back",
            ));
        }
        let outer = self.execute_transaction_control("ROLLBACK");
        let child = self
            .registry
            .write_state()
            .finalize_terminal(&self.registry);
        match (outer, child) {
            (Ok(()), Ok(FinalizedWrite::None | FinalizedWrite::RolledBack)) => Ok(()),
            (Ok(()), Ok(FinalizedWrite::Committed(_))) => {
                self.broken = true;
                Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "writable brisk_shard rollback unexpectedly committed its physical child",
                ))
            }
            (Err(error), _) | (_, Err(error)) => {
                self.broken = true;
                let _ = self.registry.write_state().force_rollback(&self.registry);
                Err(error)
            }
        }
    }

    pub(crate) fn savepoint(&mut self, name: &str) -> EngineResult<()> {
        self.ensure_explicit_transaction()?;
        let name = transaction_identifier(name)?;
        self.execute_savepoint_control(&format!("SAVEPOINT {name}"))
    }

    pub(crate) fn release(&mut self, name: &str) -> EngineResult<()> {
        self.ensure_explicit_transaction()?;
        let name = transaction_identifier(name)?;
        self.execute_savepoint_control(&format!("RELEASE SAVEPOINT {name}"))
    }

    pub(crate) fn rollback_to(&mut self, name: &str) -> EngineResult<()> {
        self.ensure_explicit_transaction()?;
        let name = transaction_identifier(name)?;
        self.execute_savepoint_control(&format!("ROLLBACK TO SAVEPOINT {name}"))
    }

    pub(crate) fn in_transaction(&self) -> bool {
        !self.connection.is_autocommit()
    }

    pub(crate) fn cancellation_handle(&self) -> CoordinatorCancellation {
        self.cancellation.clone()
    }

    #[cfg(test)]
    pub(super) fn write_state_for_test(&self) -> Arc<WriteState> {
        Arc::clone(self.registry.write_state())
    }

    #[cfg(test)]
    pub(super) fn install_statement_arm_gate_for_test(&self) -> TestChildScanControl {
        let (gate, control) = TestChildScanGate::channel();
        *self
            .statement_arm_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(gate);
        control
    }

    #[cfg(test)]
    pub(super) fn fail_next_commit_for_test(&self) {
        self.registry.write_state().fail_next_commit();
    }

    #[cfg(test)]
    pub(super) fn fail_next_write_corruption_for_test(&self) {
        self.registry.write_state().fail_next_write_corruption();
    }

    #[cfg(test)]
    pub(super) fn fail_next_commit_corruption_for_test(&self) {
        self.registry.write_state().fail_next_commit_corruption();
    }

    #[cfg(test)]
    pub(super) fn fail_next_savepoint_operation_for_test(&self, operation: SavepointOperation) {
        self.registry
            .write_state()
            .fail_next_savepoint_operation(operation, EngineErrorKind::StorageUnavailable);
    }

    #[cfg(test)]
    pub(super) fn fail_next_savepoint_corruption_for_test(&self, operation: SavepointOperation) {
        self.registry
            .write_state()
            .fail_next_savepoint_operation(operation, EngineErrorKind::DataCorruption);
    }

    fn reconcile_statement(
        &mut self,
        execution: EngineResult<usize>,
    ) -> EngineResult<CoordinatorWriteResult> {
        if self.connection.is_autocommit() {
            let finalized = self
                .registry
                .write_state()
                .finalize_terminal(&self.registry);
            return match finalized {
                Err(error) => {
                    self.broken = true;
                    Err(error)
                }
                Ok(FinalizedWrite::RolledBack) => execution.and_then(|_| {
                    Err(EngineError::new(
                        EngineErrorKind::FailedPrecondition,
                        "writable brisk_shard statement rolled back",
                    ))
                }),
                Ok(FinalizedWrite::None) => execution.map(|_| CoordinatorWriteResult {
                    affected_rows: 0,
                    explicit_key: None,
                }),
                Ok(FinalizedWrite::Committed(outcome)) => {
                    execution.map(|_| CoordinatorWriteResult {
                        affected_rows: outcome.affected_rows,
                        explicit_key: outcome.explicit_key,
                    })
                }
            };
        }

        if self.registry.write_state().has_terminal_state() {
            self.broken = true;
            self.abort_explicit_transaction();
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "writable brisk_shard callback reached a terminal child state inside an outer transaction",
            ));
        }
        if self.registry.write_state().abort_required(&self.registry) {
            let original = execution.err().unwrap_or_else(|| {
                EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "writable brisk_shard transaction was aborted",
                )
            });
            self.abort_explicit_transaction();
            return Err(original);
        }
        let outcome = self.registry.write_state().take_statement_outcome()?;
        execution.map(|_| CoordinatorWriteResult {
            affected_rows: outcome.affected_rows,
            explicit_key: outcome.explicit_key,
        })
    }

    fn execute_transaction_control(&self, sql: &str) -> EngineResult<()> {
        let epoch = self.registry.cancellation_epoch.load(Ordering::Acquire);
        let _statement_arm = self.registry.write_state().arm_statement(epoch)?;
        let _authorization = TransactionAuthorization::enter(&self.allow_transaction_control);
        self.connection
            .execute_batch(sql)
            .map_err(sqlite_error::statement)
    }

    fn execute_savepoint_control(&mut self, sql: &str) -> EngineResult<()> {
        let result = self.execute_transaction_control(sql);
        if result.is_err() && self.registry.write_state().abort_required(&self.registry) {
            // A failed xSavepoint/xRelease/xRollbackTo may leave SQLite's
            // coordinator stack and the physical child stack out of sync.
            // Once the child reports that uncertainty, abandon both sides
            // before allowing this wrapper to execute another statement.
            self.abort_explicit_transaction();
        }
        result
    }

    fn ensure_usable(&self) -> EngineResult<()> {
        if self.broken {
            Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "writable brisk_shard coordinator must be reopened after a reconciliation failure",
            ))
        } else {
            Ok(())
        }
    }

    fn ensure_explicit_transaction(&self) -> EngineResult<()> {
        self.ensure_usable()?;
        if self.connection.is_autocommit() {
            Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "writable brisk_shard savepoints require an explicit transaction",
            ))
        } else {
            Ok(())
        }
    }

    fn abort_explicit_transaction(&mut self) {
        if !self.connection.is_autocommit() {
            let _authorization = TransactionAuthorization::enter(&self.allow_transaction_control);
            let _ = self.connection.execute_batch("ROLLBACK");
        }
        if !self.connection.is_autocommit() {
            self.broken = true;
        }
        if self
            .registry
            .write_state()
            .force_rollback(&self.registry)
            .is_err()
        {
            self.broken = true;
        }
    }

    #[cfg(test)]
    fn wait_after_statement_arm_for_test(&self) -> EngineResult<()> {
        let gate = self
            .statement_arm_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if gate.is_some_and(|gate| !gate.wait_for_release()) {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "writable statement-arm test gate timed out or disconnected",
            ));
        }
        Ok(())
    }
}

impl Drop for WriteCoordinator {
    fn drop(&mut self) {
        self.abort_explicit_transaction();
    }
}

struct TransactionAuthorization<'a> {
    flag: &'a AtomicBool,
}

impl<'a> TransactionAuthorization<'a> {
    fn enter(flag: &'a AtomicBool) -> Self {
        let was_enabled = flag.swap(true, Ordering::AcqRel);
        debug_assert!(!was_enabled, "transaction authorization cannot be nested");
        Self { flag }
    }
}

impl Drop for TransactionAuthorization<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

fn transaction_identifier(name: &str) -> EngineResult<String> {
    if name.is_empty()
        || name.len() > 63
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "savepoint names must be 1 to 63 ASCII letters, digits, or underscores",
        ));
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

pub(super) struct WriteState {
    inner: Mutex<WriteTransactionState>,
    armed_statement_epoch: Mutex<Option<u64>>,
    active_interrupt: Mutex<Option<Arc<InterruptHandle>>>,
    commit_linearization: Mutex<()>,
    #[cfg(test)]
    fail_next_commit: AtomicBool,
    #[cfg(test)]
    fail_next_write_corruption: AtomicBool,
    #[cfg(test)]
    fail_next_commit_corruption: AtomicBool,
    #[cfg(test)]
    fail_next_savepoint_operation: Mutex<Option<(SavepointOperation, EngineErrorKind)>>,
}

impl WriteState {
    pub(super) fn new() -> Self {
        Self {
            inner: Mutex::new(WriteTransactionState::Idle),
            armed_statement_epoch: Mutex::new(None),
            active_interrupt: Mutex::new(None),
            commit_linearization: Mutex::new(()),
            #[cfg(test)]
            fail_next_commit: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_write_corruption: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_commit_corruption: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_savepoint_operation: Mutex::new(None),
        }
    }

    fn lock(&self) -> MutexGuard<'_, WriteTransactionState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn arm_statement(self: &Arc<Self>, epoch: u64) -> EngineResult<StatementEpochArm> {
        let mut armed = self
            .armed_statement_epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if armed.is_some() {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "writable brisk_shard statement cancellation epoch is already armed",
            ));
        }
        *armed = Some(epoch);
        Ok(StatementEpochArm {
            state: Arc::clone(self),
            epoch,
        })
    }

    fn statement_epoch(&self, registry: &Registry) -> EngineResult<u64> {
        let epoch = self
            .armed_statement_epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    "writable brisk_shard callback ran without an armed statement epoch",
                )
            })?;
        if registry.cancelled(epoch) {
            return Err(cancelled_error());
        }
        Ok(epoch)
    }

    fn clear_statement_epoch(&self, epoch: u64) {
        let mut armed = self
            .armed_statement_epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *armed == Some(epoch) {
            *armed = None;
        }
    }

    pub(super) fn lock_commit_linearization(&self) -> MutexGuard<'_, ()> {
        self.commit_linearization
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn interrupt_child(&self) {
        let interrupt = self
            .active_interrupt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(interrupt) = interrupt {
            interrupt.interrupt();
        }
    }

    #[cfg(test)]
    pub(super) fn has_active_child(&self) -> bool {
        self.active_interrupt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    #[cfg(test)]
    pub(super) fn fail_next_commit(&self) {
        self.fail_next_commit.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn fail_next_write_corruption(&self) {
        self.fail_next_write_corruption
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn fail_next_commit_corruption(&self) {
        self.fail_next_commit_corruption
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn fail_next_savepoint_operation(
        &self,
        operation: SavepointOperation,
        kind: EngineErrorKind,
    ) {
        *self
            .fail_next_savepoint_operation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((operation, kind));
    }

    pub(super) fn begin(&self, registry: &Registry) -> EngineResult<()> {
        let mut state = self.lock();
        self.ensure_active(&mut state, registry).map(|_| ())
    }

    pub(super) fn savepoint(&self, registry: &Registry, number: i32) -> EngineResult<()> {
        let result = {
            let mut state = self.lock();
            let transaction = self.ensure_active(&mut state, registry)?;
            transaction.ensure_healthy(registry)?;
            let result = match self.take_savepoint_failure(SavepointOperation::Savepoint) {
                Some(kind) => Err(injected_savepoint_error(
                    SavepointOperation::Savepoint,
                    kind,
                )),
                None => transaction.savepoint(number),
            };
            transaction.poison_savepoint_failure(SavepointOperation::Savepoint, &result);
            result
        };
        registry.storage.fail_closed_on_corruption(result)
    }

    pub(super) fn release(&self, registry: &Registry, number: i32) -> EngineResult<()> {
        let result = {
            let mut state = self.lock();
            let WriteTransactionState::Active(transaction) = &mut *state else {
                return Ok(());
            };
            transaction.ensure_healthy(registry)?;
            let result = match self.take_savepoint_failure(SavepointOperation::Release) {
                Some(kind) => Err(injected_savepoint_error(SavepointOperation::Release, kind)),
                None => transaction.release(number),
            };
            transaction.poison_savepoint_failure(SavepointOperation::Release, &result);
            result
        };
        registry.storage.fail_closed_on_corruption(result)
    }

    pub(super) fn rollback_to(&self, registry: &Registry, number: i32) -> EngineResult<()> {
        let result = {
            let mut state = self.lock();
            let WriteTransactionState::Active(transaction) = &mut *state else {
                return Ok(());
            };
            transaction.ensure_healthy(registry)?;
            let result = match self.take_savepoint_failure(SavepointOperation::RollbackTo) {
                Some(kind) => Err(injected_savepoint_error(
                    SavepointOperation::RollbackTo,
                    kind,
                )),
                None => transaction.rollback_to(number),
            };
            transaction.poison_savepoint_failure(SavepointOperation::RollbackTo, &result);
            result
        };
        registry.storage.fail_closed_on_corruption(result)
    }

    pub(super) fn sync(&self, registry: &Registry) -> EngineResult<()> {
        let state = self.lock();
        match &*state {
            WriteTransactionState::Idle => Ok(()),
            WriteTransactionState::Active(transaction) => {
                transaction.ensure_healthy(registry)?;
                if transaction
                    .child
                    .as_ref()
                    .is_some_and(|child| child.connection.is_autocommit())
                {
                    return Err(EngineError::new(
                        EngineErrorKind::Internal,
                        "brisk_shard child transaction ended before coordinator sync",
                    ));
                }
                Ok(())
            }
            WriteTransactionState::PendingCommit(_) | WriteTransactionState::PendingRollback(_) => {
                Ok(())
            }
        }
    }

    pub(super) fn mark_commit(&self, registry: &Registry) -> EngineResult<()> {
        let mut state = self.lock();
        match std::mem::replace(&mut *state, WriteTransactionState::Idle) {
            WriteTransactionState::Idle => Ok(()),
            WriteTransactionState::PendingCommit(transaction) => {
                *state = WriteTransactionState::PendingCommit(transaction);
                Ok(())
            }
            WriteTransactionState::PendingRollback(transaction) => {
                *state = WriteTransactionState::PendingRollback(transaction);
                Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "brisk_shard transaction is already rolling back",
                ))
            }
            WriteTransactionState::Active(mut transaction) => {
                if let Err(error) = transaction.ensure_healthy(registry) {
                    transaction.poison(error.to_string());
                    *state = WriteTransactionState::PendingRollback(transaction);
                    return Err(error);
                }
                *state = WriteTransactionState::PendingCommit(transaction);
                Ok(())
            }
        }
    }

    pub(super) fn mark_rollback(&self) {
        let mut state = self.lock();
        match std::mem::replace(&mut *state, WriteTransactionState::Idle) {
            WriteTransactionState::Idle => {}
            WriteTransactionState::Active(transaction)
            | WriteTransactionState::PendingCommit(transaction)
            | WriteTransactionState::PendingRollback(transaction) => {
                *state = WriteTransactionState::PendingRollback(transaction);
            }
        }
    }

    pub(super) fn finalize_terminal(&self, registry: &Registry) -> EngineResult<FinalizedWrite> {
        let _linearization = self.lock_commit_linearization();
        let state = {
            let mut state = self.lock();
            std::mem::replace(&mut *state, WriteTransactionState::Idle)
        };
        let result = match state {
            WriteTransactionState::Idle => Ok(FinalizedWrite::None),
            WriteTransactionState::PendingRollback(mut transaction) => {
                transaction.rollback().map(|()| FinalizedWrite::RolledBack)
            }
            WriteTransactionState::PendingCommit(mut transaction) => {
                if let Err(error) = transaction.ensure_healthy(registry) {
                    let _ = transaction.rollback();
                    Err(error)
                } else if self.take_commit_corruption_for_test() {
                    let _ = transaction.rollback();
                    Err(EngineError::new(
                        EngineErrorKind::DataCorruption,
                        "injected writable child commit corruption",
                    ))
                } else if self.take_commit_failure_for_test() {
                    let _ = transaction.rollback();
                    Err(EngineError::new(
                        EngineErrorKind::StorageUnavailable,
                        "injected writable child commit failure",
                    ))
                } else {
                    transaction.commit().map(FinalizedWrite::Committed)
                }
            }
            WriteTransactionState::Active(mut transaction) => {
                transaction.rollback().and_then(|()| {
                    Err(EngineError::new(
                        EngineErrorKind::Internal,
                        "brisk_shard child remained active after the outer transaction ended",
                    ))
                })
            }
        };
        self.clear_active_interrupt();
        registry.storage.fail_closed_on_corruption(result)
    }

    pub(super) fn force_rollback(&self, registry: &Registry) -> EngineResult<()> {
        let _linearization = self.lock_commit_linearization();
        let state = {
            let mut state = self.lock();
            std::mem::replace(&mut *state, WriteTransactionState::Idle)
        };
        let result = match state {
            WriteTransactionState::Idle => Ok(()),
            WriteTransactionState::Active(mut transaction)
            | WriteTransactionState::PendingCommit(mut transaction)
            | WriteTransactionState::PendingRollback(mut transaction) => transaction.rollback(),
        };
        self.clear_active_interrupt();
        registry.storage.fail_closed_on_corruption(result)
    }

    pub(super) fn reset_statement_outcome(&self) -> EngineResult<()> {
        let mut state = self.lock();
        match &mut *state {
            WriteTransactionState::Idle => Ok(()),
            WriteTransactionState::Active(transaction) => {
                transaction.affected_rows = 0;
                transaction.explicit_key = None;
                Ok(())
            }
            WriteTransactionState::PendingCommit(_) | WriteTransactionState::PendingRollback(_) => {
                Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "brisk_shard cannot start a statement while child finalization is pending",
                ))
            }
        }
    }

    pub(super) fn take_statement_outcome(&self) -> EngineResult<WriteOutcome> {
        let mut state = self.lock();
        match &mut *state {
            WriteTransactionState::Idle => Ok(WriteOutcome::default()),
            WriteTransactionState::Active(transaction) => Ok(WriteOutcome {
                affected_rows: std::mem::take(&mut transaction.affected_rows),
                explicit_key: transaction.explicit_key.take(),
            }),
            WriteTransactionState::PendingCommit(_) | WriteTransactionState::PendingRollback(_) => {
                Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "brisk_shard statement outcome is unavailable during child finalization",
                ))
            }
        }
    }

    pub(super) fn has_terminal_state(&self) -> bool {
        matches!(
            &*self.lock(),
            WriteTransactionState::PendingCommit(_) | WriteTransactionState::PendingRollback(_)
        )
    }

    pub(super) fn abort_required(&self, registry: &Registry) -> bool {
        match &*self.lock() {
            WriteTransactionState::Idle => false,
            WriteTransactionState::Active(transaction)
            | WriteTransactionState::PendingCommit(transaction) => {
                transaction.ensure_healthy(registry).is_err()
            }
            WriteTransactionState::PendingRollback(_) => true,
        }
    }

    pub(super) fn execute_insert(
        &self,
        registry: &Registry,
        spec: &TableSpec,
        values: &[ValueRef<'_>],
        conflict: ConflictMode,
    ) -> EngineResult<i64> {
        self.with_active(registry, |transaction| {
            transaction.insert(registry, spec, values, conflict, &self.active_interrupt)
        })
    }

    pub(super) fn execute_delete(
        &self,
        registry: &Registry,
        spec: &TableSpec,
        locator: ValueRef<'_>,
    ) -> EngineResult<()> {
        self.with_active(registry, |transaction| {
            transaction.delete(registry, spec, locator, &self.active_interrupt)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_update(
        &self,
        registry: &Registry,
        spec: &TableSpec,
        old_locator: ValueRef<'_>,
        new_locator: ValueRef<'_>,
        values: &[ValueRef<'_>],
        no_change: &[bool],
        conflict: ConflictMode,
    ) -> EngineResult<()> {
        self.with_active(registry, |transaction| {
            transaction.update(
                registry,
                spec,
                old_locator,
                new_locator,
                values,
                no_change,
                conflict,
                &self.active_interrupt,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn read_shard_rows(
        &self,
        registry: &Registry,
        spec: &TableSpec,
        shard: u16,
        equality: Option<&RawCell>,
        scan_epoch: u64,
        remaining_rows: usize,
        remaining_bytes: usize,
    ) -> EngineResult<(Vec<Vec<RawCell>>, usize)> {
        let mut state = self.lock();
        let transaction = self.ensure_active(&mut state, registry)?;
        transaction.read_rows(
            registry,
            spec,
            shard,
            equality,
            scan_epoch,
            remaining_rows,
            remaining_bytes,
            &self.active_interrupt,
        )
    }

    fn with_active<T>(
        &self,
        registry: &Registry,
        operation: impl FnOnce(&mut WriteTransaction) -> EngineResult<T>,
    ) -> EngineResult<T> {
        let result = (|| {
            let mut state = self.lock();
            let transaction = self.ensure_active(&mut state, registry)?;
            transaction.ensure_healthy(registry)?;
            let result = if self.take_write_corruption_for_test() {
                Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    "injected writable child operation corruption",
                ))
            } else {
                operation(transaction)
            };
            if let Err(error) = &result {
                if error.kind() == EngineErrorKind::DataCorruption {
                    transaction.poison(error.to_string());
                }
            }
            result
        })();
        registry.storage.fail_closed_on_corruption(result)
    }

    fn ensure_active<'a>(
        &self,
        state: &'a mut WriteTransactionState,
        registry: &Registry,
    ) -> EngineResult<&'a mut WriteTransaction> {
        if matches!(state, WriteTransactionState::Idle) {
            let operation = registry.storage.enter_schema_operation()?;
            if registry.storage.current_schema_generation() != registry.schema_generation {
                return Err(EngineError::new(
                    EngineErrorKind::Busy,
                    "brisk_shard writable coordinator schema is stale; reopen the coordinator",
                ));
            }
            let epoch = self.statement_epoch(registry)?;
            *state = WriteTransactionState::Active(WriteTransaction::new(operation, epoch));
        }
        match state {
            WriteTransactionState::Active(transaction) => Ok(transaction),
            WriteTransactionState::Idle => unreachable!(),
            WriteTransactionState::PendingCommit(_) | WriteTransactionState::PendingRollback(_) => {
                Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "brisk_shard write callback ran while child finalization was pending",
                ))
            }
        }
    }

    fn clear_active_interrupt(&self) {
        *self
            .active_interrupt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    fn take_commit_failure_for_test(&self) -> bool {
        #[cfg(test)]
        {
            self.fail_next_commit.swap(false, Ordering::AcqRel)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    fn take_write_corruption_for_test(&self) -> bool {
        #[cfg(test)]
        {
            self.fail_next_write_corruption
                .swap(false, Ordering::AcqRel)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    fn take_commit_corruption_for_test(&self) -> bool {
        #[cfg(test)]
        {
            self.fail_next_commit_corruption
                .swap(false, Ordering::AcqRel)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    fn take_savepoint_failure(&self, operation: SavepointOperation) -> Option<EngineErrorKind> {
        #[cfg(test)]
        {
            let mut pending = self
                .fail_next_savepoint_operation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if pending
                .as_ref()
                .is_some_and(|(pending_operation, _)| *pending_operation == operation)
            {
                let (_, kind) = pending.take().expect("matched pending savepoint failure");
                return Some(kind);
            }
        }
        #[cfg(not(test))]
        let _ = operation;
        None
    }
}

struct StatementEpochArm {
    state: Arc<WriteState>,
    epoch: u64,
}

impl Drop for StatementEpochArm {
    fn drop(&mut self) {
        self.state.clear_statement_epoch(self.epoch);
    }
}

impl Drop for WriteState {
    fn drop(&mut self) {
        let state = self
            .inner
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match state {
            WriteTransactionState::Idle => {}
            WriteTransactionState::Active(transaction)
            | WriteTransactionState::PendingCommit(transaction)
            | WriteTransactionState::PendingRollback(transaction) => {
                let _ = transaction.rollback();
            }
        }
    }
}

enum WriteTransactionState {
    Idle,
    Active(WriteTransaction),
    PendingCommit(WriteTransaction),
    PendingRollback(WriteTransaction),
}

struct WriteTransaction {
    _operation: SchemaOperationGuard,
    epoch: u64,
    child: Option<WriteChild>,
    savepoints: Vec<SavepointMark>,
    poison: Option<String>,
    affected_rows: usize,
    explicit_key: Option<Value>,
}

struct SavepointMark {
    number: i32,
    affected_rows: usize,
    explicit_key: Option<Value>,
}

impl WriteTransaction {
    fn new(operation: SchemaOperationGuard, epoch: u64) -> Self {
        Self {
            _operation: operation,
            epoch,
            child: None,
            savepoints: Vec::new(),
            poison: None,
            affected_rows: 0,
            explicit_key: None,
        }
    }

    fn ensure_healthy(&self, registry: &Registry) -> EngineResult<()> {
        if registry.cancelled(self.epoch) {
            return Err(cancelled_error());
        }
        if registry.storage.current_schema_generation() != registry.schema_generation {
            return Err(EngineError::new(
                EngineErrorKind::Busy,
                "brisk_shard writable coordinator schema became stale",
            ));
        }
        if let Some(reason) = &self.poison {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!("brisk_shard transaction is aborted: {reason}"),
            ));
        }
        Ok(())
    }

    fn poison(&mut self, reason: impl Into<String>) {
        if self.poison.is_none() {
            self.poison = Some(reason.into());
        }
    }

    fn record_physical_changes(&mut self, changed: usize) -> EngineResult<()> {
        let Some(affected_rows) = self.affected_rows.checked_add(changed) else {
            self.poison("physical affected-row count overflowed");
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                "brisk_shard physical affected-row count overflowed",
            ));
        };
        self.affected_rows = affected_rows;
        Ok(())
    }

    fn poison_savepoint_failure(
        &mut self,
        operation: SavepointOperation,
        result: &EngineResult<()>,
    ) {
        if let Err(error) = result {
            self.poison(format!(
                "physical child {} failed: {error}",
                operation.name()
            ));
        }
    }

    fn pin<'a>(
        &'a mut self,
        registry: &Registry,
        shard: u16,
        active_interrupt: &Mutex<Option<Arc<InterruptHandle>>>,
    ) -> EngineResult<&'a mut WriteChild> {
        self.ensure_healthy(registry)?;
        if self
            .child
            .as_ref()
            .is_some_and(|child| child.shard != shard)
        {
            self.poison("a second physical shard was requested");
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "brisk_shard transaction cannot write more than one physical shard",
            ));
        }
        if self.child.is_none() {
            let connection = registry.storage.open_shard(shard)?;
            connection
                .busy_timeout(CANCELLABLE_BUSY_SLICE)
                .map_err(sqlite_error::storage)?;
            let cancellation_epoch = Arc::clone(&registry.cancellation_epoch);
            let epoch = self.epoch;
            connection
                .progress_handler(
                    64,
                    Some(move || cancellation_epoch.load(Ordering::Acquire) != epoch),
                )
                .map_err(sqlite_error::storage)?;
            let interrupt = Arc::new(connection.get_interrupt_handle());
            *active_interrupt
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&interrupt));

            let begin_deadline = Instant::now()
                .checked_add(CONNECTION_BUSY_TIMEOUT)
                .unwrap_or_else(Instant::now);
            let begin_result = loop {
                if registry.cancelled(self.epoch) {
                    break Err(cancelled_error());
                }
                match connection.execute_batch("BEGIN IMMEDIATE") {
                    Ok(()) => break Ok(()),
                    Err(error) => {
                        let error = sqlite_error::statement(error);
                        if error.kind() == EngineErrorKind::Busy && Instant::now() < begin_deadline
                        {
                            continue;
                        }
                        break Err(error);
                    }
                }
            };
            if let Err(error) = begin_result {
                *active_interrupt
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                return Err(error);
            }

            let foreign_keys = connection
                .pragma_query_value(None, "foreign_keys", |row| row.get::<_, i64>(0))
                .map_err(sqlite_error::storage)?;
            if foreign_keys != 1 {
                let _ = connection.execute_batch("ROLLBACK");
                *active_interrupt
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "SQLite foreign-key enforcement is not enabled on a writable shard child",
                ));
            }
            for savepoint in &self.savepoints {
                if let Err(error) = connection
                    .execute_batch(&format!("SAVEPOINT brisk_vtab_{}", savepoint.number))
                    .map_err(sqlite_error::statement)
                {
                    let _ = connection.execute_batch("ROLLBACK");
                    *active_interrupt
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                    self.poison(format!("physical child SAVEPOINT replay failed: {error}"));
                    return Err(error);
                }
            }
            if registry.cancelled(self.epoch) {
                let _ = connection.execute_batch("ROLLBACK");
                *active_interrupt
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                return Err(cancelled_error());
            }
            self.child = Some(WriteChild {
                shard,
                connection,
                _interrupt: interrupt,
            });
        }
        Ok(self.child.as_mut().expect("write child was installed"))
    }

    fn savepoint(&mut self, number: i32) -> EngineResult<()> {
        if number < 0 {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "brisk_shard received a negative savepoint number",
            ));
        }
        if self.savepoints.iter().any(|saved| saved.number == number) {
            return Ok(());
        }
        let first_missing = match self.savepoints.last() {
            Some(saved) => saved.number.checked_add(1).ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::LimitExceeded,
                    "brisk_shard savepoint number overflowed",
                )
            })?,
            None => 0,
        };
        if first_missing > number {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "brisk_shard received an invalid savepoint sequence",
            ));
        }

        // A virtual table first enlisted below existing SQLite savepoints
        // receives only xSavepoint(N), not callbacks for 0..N-1. No writes
        // through this shared state happened before enlistment, so all of
        // those missing levels have the same baseline. Materializing every
        // level lets a later RELEASE of N followed by ROLLBACK TO an outer
        // level still reach the correct physical boundary.
        let affected_rows = self.affected_rows;
        let explicit_key = self.explicit_key.clone();
        for missing in first_missing..=number {
            if let Some(child) = &self.child {
                child
                    .connection
                    .execute_batch(&format!("SAVEPOINT brisk_vtab_{missing}"))
                    .map_err(sqlite_error::statement)?;
            }
            self.savepoints.push(SavepointMark {
                number: missing,
                affected_rows,
                explicit_key: explicit_key.clone(),
            });
        }
        Ok(())
    }

    fn release(&mut self, number: i32) -> EngineResult<()> {
        let Some(position) = self
            .savepoints
            .iter()
            .position(|saved| saved.number == number)
        else {
            // Multiple participating virtual tables receive the same callback.
            return Ok(());
        };
        if let Some(child) = &self.child {
            child
                .connection
                .execute_batch(&format!("RELEASE SAVEPOINT brisk_vtab_{number}"))
                .map_err(sqlite_error::statement)?;
        }
        self.savepoints.truncate(position);
        Ok(())
    }

    fn rollback_to(&mut self, number: i32) -> EngineResult<()> {
        let Some(position) = self
            .savepoints
            .iter()
            .position(|saved| saved.number == number)
        else {
            return Ok(());
        };
        if let Some(child) = &self.child {
            child
                .connection
                .execute_batch(&format!("ROLLBACK TO SAVEPOINT brisk_vtab_{number}"))
                .map_err(sqlite_error::statement)?;
        }
        self.affected_rows = self.savepoints[position].affected_rows;
        self.explicit_key = self.savepoints[position].explicit_key.clone();
        self.savepoints.truncate(position + 1);
        Ok(())
    }

    fn insert(
        &mut self,
        registry: &Registry,
        spec: &TableSpec,
        values: &[ValueRef<'_>],
        conflict: ConflictMode,
        active_interrupt: &Mutex<Option<Arc<InterruptHandle>>>,
    ) -> EngineResult<i64> {
        spec.ensure_writable()?;
        let shard_key = spec.write_shard_key(values)?;
        let shard = registry.write_target(spec, shard_key)?;
        let sql = spec.insert_sql(conflict)?;
        let parameters = values
            .iter()
            .copied()
            .map(RawCell::try_copy_from)
            .collect::<EngineResult<Vec<_>>>()?;
        let explicit_key = RawCell::try_copy_from(shard_key)?;
        let child = self.pin(registry, shard, active_interrupt)?;
        let changed = match child
            .connection
            .execute(&sql, rusqlite::params_from_iter(&parameters))
            .map_err(sqlite_error::statement)
        {
            Ok(changed) => changed,
            Err(error) => {
                self.poison_if_uncertain(&error);
                return Err(error);
            }
        };
        if changed != 1 {
            self.poison("physical INSERT did not report exactly one changed row");
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "brisk_shard physical INSERT did not change exactly one row",
            ));
        }
        self.record_physical_changes(changed)?;
        let rowid = match &explicit_key {
            RawCell::Integer(value) => *value,
            _ => 0,
        };
        self.explicit_key = Some(raw_cell_to_value(explicit_key)?);
        self.ensure_healthy(registry)?;
        Ok(rowid)
    }

    fn delete(
        &mut self,
        registry: &Registry,
        spec: &TableSpec,
        locator_value: ValueRef<'_>,
        active_interrupt: &Mutex<Option<Arc<InterruptHandle>>>,
    ) -> EngineResult<()> {
        let decoded = spec.decode_locator(locator_value)?;
        let child = self.pin(registry, decoded.shard, active_interrupt)?;
        let changed = match child.connection.execute(
            &spec.delete_sql()?,
            rusqlite::params_from_iter(&decoded.values),
        ) {
            Ok(changed) => changed,
            Err(error) => {
                let error = sqlite_error::statement(error);
                self.poison_if_uncertain(&error);
                return Err(error);
            }
        };
        if changed > 1 {
            self.poison("physical DELETE locator identified more than one row");
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "brisk_shard DELETE locator identified more than one physical row",
            ));
        }
        // SQLite materializes every target before calling xUpdate. An
        // earlier REPLACE or foreign-key action may legitimately remove this
        // row before its callback arrives, in which case native SQLite skips
        // the stale identity and reports no additional direct change.
        self.record_physical_changes(changed)?;
        self.ensure_healthy(registry)
    }

    #[allow(clippy::too_many_arguments)]
    fn update(
        &mut self,
        registry: &Registry,
        spec: &TableSpec,
        old_locator: ValueRef<'_>,
        new_locator: ValueRef<'_>,
        values: &[ValueRef<'_>],
        no_change: &[bool],
        conflict: ConflictMode,
        active_interrupt: &Mutex<Option<Arc<InterruptHandle>>>,
    ) -> EngineResult<()> {
        match (old_locator, new_locator) {
            (ValueRef::Blob(old), ValueRef::Blob(new)) if old == new => {}
            _ => {
                self.poison("the opaque row locator was changed");
                return Err(EngineError::new(
                    EngineErrorKind::ReadOnly,
                    "brisk_shard opaque row locator cannot be changed",
                ));
            }
        }
        let decoded = spec.decode_locator(old_locator)?;
        let shard_key = spec.shard_key.as_ref().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Unsupported,
                format!("registered table {} has no writable shard key", spec.name),
            )
        })?;
        let shard_key_index = usize::try_from(shard_key.column_index).map_err(|_| {
            EngineError::new(
                EngineErrorKind::Internal,
                format!(
                    "registered table {} has an invalid shard-key index",
                    spec.name
                ),
            )
        })?;
        let new_shard = if no_change.get(shard_key_index).copied().unwrap_or(false) {
            decoded.shard
        } else {
            registry.write_target(spec, spec.write_shard_key(values)?)?
        };
        if decoded.shard != new_shard {
            self.poison("a shard-key update attempted to move a row to another file");
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "brisk_shard shard-key updates cannot move a row between physical shards",
            ));
        }
        let (sql, mut parameters) = spec.update_sql_and_values(values, no_change, conflict)?;
        parameters.extend(decoded.values);
        let child = self.pin(registry, decoded.shard, active_interrupt)?;
        let changed = match child
            .connection
            .execute(&sql, rusqlite::params_from_iter(&parameters))
            .map_err(sqlite_error::statement)
        {
            Ok(changed) => changed,
            Err(error) => {
                self.poison_if_uncertain(&error);
                return Err(error);
            }
        };
        if changed > 1 {
            self.poison("physical UPDATE locator identified more than one row");
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "brisk_shard UPDATE locator identified more than one physical row",
            ));
        }
        // A prior callback in this materialized UPDATE can replace, cascade
        // away, or relocate the row. Treat its old locator as SQLite treats a
        // stale rowid: a successful zero-row no-op.
        self.record_physical_changes(changed)?;
        self.ensure_healthy(registry)
    }

    #[allow(clippy::too_many_arguments)]
    fn read_rows(
        &mut self,
        registry: &Registry,
        spec: &TableSpec,
        shard: u16,
        equality: Option<&RawCell>,
        scan_epoch: u64,
        remaining_rows: usize,
        remaining_bytes: usize,
        active_interrupt: &Mutex<Option<Arc<InterruptHandle>>>,
    ) -> EngineResult<(Vec<Vec<RawCell>>, usize)> {
        spec.ensure_writable()?;
        if scan_epoch != self.epoch || registry.cancelled(scan_epoch) {
            return Err(cancelled_error());
        }
        let child = self.pin(registry, shard, active_interrupt)?;
        let select_sql = equality
            .and(spec.locator_point_select_sql.as_deref())
            .or(spec.locator_select_sql.as_deref())
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::Unsupported,
                    format!(
                        "registered table {} has no writable locator scan",
                        spec.name
                    ),
                )
            })?;
        let mut statement = child
            .connection
            .prepare(select_sql)
            .map_err(sqlite_error::statement)?;
        let mut sqlite_rows = match equality {
            Some(value) => statement.query([value]).map_err(sqlite_error::statement)?,
            None => statement.query([]).map_err(sqlite_error::statement)?,
        };
        let locator_spec = spec.locator.as_ref().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Unsupported,
                format!("registered table {} has no writable row locator", spec.name),
            )
        })?;
        let result = (|| {
            let mut rows = Vec::new();
            let mut used_bytes = 0_usize;
            while let Some(row) = sqlite_rows.next().map_err(sqlite_error::statement)? {
                if registry.cancelled(scan_epoch) {
                    return Err(cancelled_error());
                }
                if rows.len() == remaining_rows {
                    return Err(limit_error("row", registry.limits.rows));
                }
                let returned_cells = spec
                    .column_count
                    .checked_add(1)
                    .ok_or_else(|| limit_error("byte", registry.limits.bytes))?;
                let mut row_bytes = returned_cells
                    .checked_mul(VALUE_ACCOUNTING_BYTES)
                    .and_then(|bytes| bytes.checked_add(ROW_ACCOUNTING_BYTES))
                    .ok_or_else(|| limit_error("byte", registry.limits.bytes))?;
                if used_bytes
                    .checked_add(row_bytes)
                    .is_none_or(|bytes| bytes > remaining_bytes)
                {
                    return Err(limit_error("byte", registry.limits.bytes));
                }
                let mut cells = Vec::new();
                cells
                    .try_reserve_exact(returned_cells)
                    .map_err(allocation_error)?;
                for column in 0..spec.column_count {
                    let value = row.get_ref(column).map_err(sqlite_error::statement)?;
                    row_bytes = row_bytes
                        .checked_add(RawCell::accounted_payload_bytes(value))
                        .ok_or_else(|| limit_error("byte", registry.limits.bytes))?;
                    if used_bytes
                        .checked_add(row_bytes)
                        .is_none_or(|bytes| bytes > remaining_bytes)
                    {
                        return Err(limit_error("byte", registry.limits.bytes));
                    }
                    cells.push(RawCell::try_copy_from(value)?);
                }
                let identity_count = locator_spec.value_count();
                let mut identity = Vec::new();
                identity
                    .try_reserve_exact(identity_count)
                    .map_err(allocation_error)?;
                for offset in 0..identity_count {
                    let value = row
                        .get_ref(spec.column_count + offset)
                        .map_err(sqlite_error::statement)?;
                    row_bytes = row_bytes
                        .checked_add(RawCell::accounted_payload_bytes(value))
                        .and_then(|bytes| bytes.checked_add(VALUE_ACCOUNTING_BYTES))
                        .ok_or_else(|| limit_error("byte", registry.limits.bytes))?;
                    if used_bytes
                        .checked_add(row_bytes)
                        .is_none_or(|bytes| bytes > remaining_bytes)
                    {
                        return Err(limit_error("byte", registry.limits.bytes));
                    }
                    identity.push(RawCell::try_copy_from(value)?);
                }
                let encoded = locator::encode(spec.id, shard, &identity)?;
                row_bytes = row_bytes
                    .checked_add(encoded.len())
                    .and_then(|bytes| bytes.checked_add(ALLOCATION_OVERHEAD_BYTES))
                    .ok_or_else(|| limit_error("byte", registry.limits.bytes))?;
                if used_bytes
                    .checked_add(row_bytes)
                    .is_none_or(|bytes| bytes > remaining_bytes)
                {
                    return Err(limit_error("byte", registry.limits.bytes));
                }
                cells.push(RawCell::Blob(encoded));
                used_bytes = used_bytes
                    .checked_add(row_bytes)
                    .ok_or_else(|| limit_error("byte", registry.limits.bytes))?;
                rows.try_reserve(1).map_err(allocation_error)?;
                rows.push(cells);
            }
            if registry.cancelled(scan_epoch) {
                return Err(cancelled_error());
            }
            Ok((rows, used_bytes))
        })();
        registry.storage.fail_closed_on_corruption(result)
    }

    fn poison_if_uncertain(&mut self, error: &EngineError) {
        if !matches!(
            error.kind(),
            EngineErrorKind::ConstraintViolation
                | EngineErrorKind::UniqueViolation
                | EngineErrorKind::NotNullViolation
                | EngineErrorKind::ForeignKeyViolation
                | EngineErrorKind::CheckViolation
                | EngineErrorKind::Busy
                | EngineErrorKind::TypeMismatch
                | EngineErrorKind::NumericOutOfRange
        ) {
            self.poison(error.to_string());
        }
    }

    fn commit(&mut self) -> EngineResult<WriteOutcome> {
        if let Some(child) = self.child.take() {
            if let Err(error) = child
                .connection
                .execute_batch("COMMIT")
                .map_err(sqlite_error::statement)
            {
                if !child.connection.is_autocommit() {
                    let _ = child.connection.execute_batch("ROLLBACK");
                }
                return Err(error);
            }
        }
        Ok(WriteOutcome {
            affected_rows: std::mem::take(&mut self.affected_rows),
            explicit_key: self.explicit_key.take(),
        })
    }

    fn rollback(&mut self) -> EngineResult<()> {
        if let Some(child) = self.child.take() {
            if !child.connection.is_autocommit() {
                child
                    .connection
                    .execute_batch("ROLLBACK")
                    .map_err(sqlite_error::statement)?;
            }
        }
        Ok(())
    }
}

struct WriteChild {
    shard: u16,
    connection: Connection,
    _interrupt: Arc<InterruptHandle>,
}

#[derive(Debug, Default)]
pub(super) struct WriteOutcome {
    pub(super) affected_rows: usize,
    pub(super) explicit_key: Option<Value>,
}

pub(super) enum FinalizedWrite {
    None,
    Committed(WriteOutcome),
    RolledBack,
}

fn raw_cell_to_value(value: RawCell) -> EngineResult<Value> {
    Ok(match value {
        RawCell::Null => Value::Null,
        RawCell::Integer(value) => Value::Int64(value),
        RawCell::Real(value) => Value::Float64(value),
        RawCell::Text(value) => match String::from_utf8(value) {
            Ok(value) => Value::Text(value),
            Err(error) => Value::InvalidText(error.into_bytes()),
        },
        RawCell::Blob(value) => Value::Binary(value),
    })
}

pub(super) fn map_callback(result: EngineResult<()>) -> SqliteResult<()> {
    result.map_err(super::vtab_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_identifier_is_bounded_and_injection_safe() {
        assert_eq!(transaction_identifier("save_1").unwrap(), "\"save_1\"");
        for invalid in ["", "bad-name", "x;ROLLBACK", "space name"] {
            assert_eq!(
                transaction_identifier(invalid).unwrap_err().kind(),
                EngineErrorKind::InvalidArgument
            );
        }
        assert_eq!(
            transaction_identifier(&"x".repeat(64)).unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
    }

    #[test]
    fn coordinator_result_is_protocol_neutral() {
        let result = CoordinatorWriteResult {
            affected_rows: 1,
            explicit_key: Some(Value::Int64(7)),
        };
        assert_eq!(result.affected_rows, 1);
        assert_eq!(result.explicit_key, Some(Value::Int64(7)));
        assert_eq!(super::super::MODULE_NAME, "brisk_shard");
    }
}
