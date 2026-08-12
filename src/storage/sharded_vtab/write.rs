//! Writable coordinator execution and one-shard transaction state.

use std::{
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
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
    RawCell, Registry, RegistrySchemaCache, TableSpec, VALUE_ACCOUNTING_BYTES, allocation_error,
    attach_writable_coordinator_authorizer, bootstrap_coordinator_schema, cancelled_error,
    limit_error, locator, module_v2,
};
#[cfg(test)]
use super::{TestChildScanControl, TestChildScanGate};
use crate::{
    core::{
        EngineError, EngineErrorKind, EngineResult, GeneratedIdPolicy, GeneratedKey,
        OperationControl, Value,
        generated_id::{
            AllocationOwnerSlot, GeneratedIdClassification, classify_generated_id,
            native_range_v1_sequence_ceiling, native_range_v1_sequence_floor,
        },
    },
    sqlite_error,
    storage::{CONNECTION_BUSY_TIMEOUT, SchemaOperationGuard, Storage},
};

const CANCELLABLE_BUSY_SLICE: Duration = Duration::from_millis(25);

fn validate_allocation_sequence_capacity(
    connection: &Connection,
    table: &str,
    owner: AllocationOwnerSlot,
) -> EngineResult<()> {
    let sequence = read_allocation_sequence(connection, table)?;
    let floor = native_range_v1_sequence_floor(owner);
    let ceiling = native_range_v1_sequence_ceiling(owner);
    if sequence < floor || sequence > ceiling {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "native generated table {table} sequence {sequence} is outside owner {} range",
                owner.get()
            ),
        ));
    }
    if sequence == ceiling {
        return Err(EngineError::new(
            EngineErrorKind::LimitExceeded,
            format!(
                "native generated table {table} exhausted allocation owner {}",
                owner.get()
            ),
        ));
    }
    Ok(())
}

fn read_allocation_sequence(connection: &Connection, table: &str) -> EngineResult<i64> {
    let mut statement = connection
        .prepare("SELECT seq FROM main.sqlite_sequence WHERE name = ?1 COLLATE BINARY")
        .map_err(|error| allocation_sequence_read_error(error, table))?;
    let mut rows = statement
        .query([table])
        .map_err(|error| allocation_sequence_read_error(error, table))?;
    let sequence = match rows
        .next()
        .map_err(|error| allocation_sequence_read_error(error, table))?
    {
        Some(row) => match row
            .get_ref(0)
            .map_err(|error| allocation_sequence_read_error(error, table))?
        {
            ValueRef::Integer(sequence) => sequence,
            _ => {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!("native generated table {table} has a non-integer SQLite sequence"),
                ));
            }
        },
        None => {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("native generated table {table} is missing its SQLite sequence"),
            ));
        }
    };
    if rows
        .next()
        .map_err(|error| allocation_sequence_read_error(error, table))?
        .is_some()
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("native generated table {table} has duplicate SQLite sequences"),
        ));
    }

    Ok(sequence)
}

fn allocation_sequence_read_error(error: rusqlite::Error, table: &str) -> EngineError {
    let classified = sqlite_error::storage(error);
    let diagnostic = format!("failed to read native generated sequence for table {table}");
    if matches!(
        classified.kind(),
        EngineErrorKind::Busy
            | EngineErrorKind::Cancelled
            | EngineErrorKind::PermissionDenied
            | EngineErrorKind::ReadOnly
            | EngineErrorKind::StorageFull
            | EngineErrorKind::OutOfMemory
            | EngineErrorKind::StorageUnavailable
    ) {
        classified.context(diagnostic)
    } else {
        EngineError::from_source(EngineErrorKind::DataCorruption, diagnostic, classified)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct CoordinatorWriteResult {
    pub(crate) affected_rows: usize,
    pub(crate) shard: Option<u16>,
    pub(crate) explicit_key: Option<Value>,
    pub(crate) generated_key: Option<GeneratedKey>,
}

impl CoordinatorWriteResult {
    /// Return the direct physical rows changed by this statement.
    pub(crate) const fn affected_rows(&self) -> usize {
        self.affected_rows
    }

    /// Return the one physical shard mutated by this statement.
    ///
    /// A successful no-op (for example, an ignored constraint) has no shard
    /// result. The writable coordinator still refuses to mutate two shards in
    /// one transaction.
    pub(crate) const fn shard(&self) -> Option<u16> {
        self.shard
    }

    /// Return the caller-supplied key reported for a successful INSERT.
    ///
    /// UPDATE and DELETE do not produce a key. Automatically allocated values
    /// are reported separately by [`Self::generated_key`].
    pub(crate) const fn explicit_key(&self) -> Option<&Value> {
        self.explicit_key.as_ref()
    }

    /// Return the SQLite-allocated key captured by the physical INSERT.
    ///
    /// Caller-supplied IDs are deliberately reported only by
    /// [`Self::explicit_key`]; adapters can therefore distinguish generation
    /// from an explicit value without inspecting SQL text.
    pub(crate) const fn generated_key(&self) -> Option<&GeneratedKey> {
        self.generated_key.as_ref()
    }
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

enum CoordinatorAdmission {
    Standalone(SchemaOperationGuard),
    Retained {
        operation: SchemaOperationGuard,
        controlled: Option<(Arc<OperationControl>, Arc<RegistrySchemaCache>)>,
    },
}

impl WriteCoordinator {
    pub(crate) fn open(storage: Storage) -> EngineResult<Self> {
        let bootstrap_operation = storage.enter_schema_operation()?;
        Self::open_with_admission(
            storage,
            CoordinatorAdmission::Standalone(bootstrap_operation),
        )
    }

    /// Open an ephemeral coordinator under schema admission already owned by
    /// the Engine operation.
    ///
    /// The supplied guard is retained through bootstrap, statement execution,
    /// child cleanup, and coordinator drop. Neither writable callbacks nor
    /// cursors try to re-enter schema admission while a migration is waiting
    /// for this already-admitted operation to drain.
    pub(crate) fn open_admitted(
        storage: Storage,
        operation: SchemaOperationGuard,
    ) -> EngineResult<Self> {
        Self::open_with_admission(
            storage,
            CoordinatorAdmission::Retained {
                operation,
                controlled: None,
            },
        )
    }

    /// Open an Engine-admitted coordinator with cancellation installed before
    /// registry discovery can wait on shard-0 schema state.
    pub(crate) fn open_admitted_controlled(
        storage: Storage,
        operation: SchemaOperationGuard,
        control: Arc<OperationControl>,
        registry_cache: Arc<RegistrySchemaCache>,
    ) -> EngineResult<Self> {
        Self::open_with_admission(
            storage,
            CoordinatorAdmission::Retained {
                operation,
                controlled: Some((control, registry_cache)),
            },
        )
    }

    fn open_with_admission(
        storage: Storage,
        admission: CoordinatorAdmission,
    ) -> EngineResult<Self> {
        let connection = Connection::open_in_memory().map_err(sqlite_error::storage)?;
        let (registry, bootstrap_operation, test_registry_cache) = match admission {
            CoordinatorAdmission::Standalone(operation) => (
                Registry::build_writable(storage, CursorLimits::default())?,
                Some(operation),
                None,
            ),
            CoordinatorAdmission::Retained {
                operation,
                controlled: Some((control, registry_cache)),
            } => (
                Registry::build_writable_cached(
                    storage,
                    CursorLimits::default(),
                    operation,
                    control,
                    &registry_cache,
                )?,
                None,
                Some(registry_cache),
            ),
            CoordinatorAdmission::Retained {
                operation,
                controlled: None,
            } => (
                Registry::build_writable_admitted(
                    storage,
                    CursorLimits::default(),
                    operation,
                    None,
                )?,
                None,
                None,
            ),
        };
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

        #[cfg(test)]
        if let Some(cache) = test_registry_cache {
            registry.install_write_test_controls(&cache);
        }
        #[cfg(not(test))]
        let _ = test_registry_cache;

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
        self.execute_dml_inner(sql, parameters, None)
    }

    /// Execute one preflighted single-row INSERT whose native generated key
    /// was omitted by the caller.
    ///
    /// This is intentionally a narrow seam for the shared SQL planner added
    /// by issue #130: the caller must identify both the catalog table and its
    /// already-selected physical shard. Merely supplying NULL through
    /// [`Self::execute_dml`] never enables allocation.
    pub(crate) fn execute_generated_dml<P: Params>(
        &mut self,
        sql: &str,
        parameters: P,
        table_id: u64,
        expected_shard: u16,
    ) -> EngineResult<CoordinatorWriteResult> {
        self.execute_dml_inner(
            sql,
            parameters,
            Some(GeneratedInsertRequest {
                table_id,
                expected_shard: Some(expected_shard),
            }),
        )
    }

    /// Execute one generated INSERT and let the writable registry choose an
    /// active, non-exhausted owner using a per-table round-robin cursor.
    pub(crate) fn execute_generated_dml_auto<P: Params>(
        &mut self,
        sql: &str,
        parameters: P,
        table_id: u64,
    ) -> EngineResult<CoordinatorWriteResult> {
        self.execute_dml_inner(
            sql,
            parameters,
            Some(GeneratedInsertRequest {
                table_id,
                expected_shard: None,
            }),
        )
    }

    fn execute_dml_inner<P: Params>(
        &mut self,
        sql: &str,
        parameters: P,
        generated: Option<GeneratedInsertRequest>,
    ) -> EngineResult<CoordinatorWriteResult> {
        self.ensure_usable()?;
        let epoch = self.registry.cancellation_epoch.load(Ordering::Acquire);
        let statement_arm = self.registry.write_state().arm_statement(epoch)?;
        let generated_arm = generated
            .map(|request| {
                self.registry.validate_generated_insert_request(request)?;
                self.registry.write_state().arm_generated_insert(request)
            })
            .transpose()?;
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

        let execution = match (&generated_arm, execution) {
            (Some(arm), Ok(changed)) => arm.require_consumed().map(|()| changed),
            (_, execution) => execution,
        };
        let result = self.reconcile_statement(execution);
        drop(generated_arm);
        drop(statement_arm);
        result
    }

    /// Execute DML using protocol-neutral values from the Engine boundary.
    ///
    /// Conversion happens before the coordinator arms a statement, so a value
    /// without a lossless SQLite representation cannot begin or disturb a
    /// physical transaction. The generic [`Self::execute_dml`] entry point is
    /// retained for storage-local callers and focused callback tests.
    pub(crate) fn execute_dml_values(
        &mut self,
        sql: &str,
        parameters: &[Value],
    ) -> EngineResult<CoordinatorWriteResult> {
        let parameters = crate::sql::sqlite_parameters(parameters)?;
        self.execute_dml(sql, rusqlite::params_from_iter(parameters))
    }

    /// Protocol-neutral counterpart to [`Self::execute_generated_dml`].
    pub(crate) fn execute_generated_dml_values(
        &mut self,
        sql: &str,
        parameters: &[Value],
        table_id: u64,
        expected_shard: u16,
    ) -> EngineResult<CoordinatorWriteResult> {
        let parameters = crate::sql::sqlite_parameters(parameters)?;
        self.execute_generated_dml(
            sql,
            rusqlite::params_from_iter(parameters),
            table_id,
            expected_shard,
        )
    }

    /// Protocol-neutral counterpart to [`Self::execute_generated_dml_auto`].
    pub(crate) fn execute_generated_dml_values_auto(
        &mut self,
        sql: &str,
        parameters: &[Value],
        table_id: u64,
    ) -> EngineResult<CoordinatorWriteResult> {
        let parameters = crate::sql::sqlite_parameters(parameters)?;
        self.execute_generated_dml_auto(sql, rusqlite::params_from_iter(parameters), table_id)
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
    pub(super) fn last_insert_rowid_for_test(&self) -> i64 {
        self.connection.last_insert_rowid()
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
    pub(super) fn install_generated_target_gate_for_test(&self) -> TestChildScanControl {
        let (gate, control) = TestChildScanGate::channel();
        *self
            .registry
            .generated_target_gate
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
                    shard: None,
                    explicit_key: None,
                    generated_key: None,
                }),
                Ok(FinalizedWrite::Committed(outcome)) => {
                    execution.map(|_| CoordinatorWriteResult {
                        affected_rows: outcome.affected_rows,
                        shard: outcome.shard,
                        explicit_key: outcome.explicit_key,
                        generated_key: outcome.generated_key,
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
            shard: outcome.shard,
            explicit_key: outcome.explicit_key,
            generated_key: outcome.generated_key,
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
    retained_schema_operation: Option<SchemaOperationGuard>,
    armed_statement_epoch: Mutex<Option<u64>>,
    generated_insert: Mutex<Option<GeneratedInsertIntent>>,
    active_interrupt: Mutex<Option<Arc<InterruptHandle>>>,
    commit_linearization: Mutex<()>,
    nonblocking_cancel_requested: AtomicBool,
    #[cfg(test)]
    fail_next_commit: AtomicBool,
    #[cfg(test)]
    fail_next_write_corruption: AtomicBool,
    #[cfg(test)]
    fail_next_commit_corruption: AtomicBool,
    #[cfg(test)]
    fail_next_savepoint_operation: Mutex<Option<(SavepointOperation, EngineErrorKind)>>,
    #[cfg(test)]
    commit_gate: Mutex<Option<TestChildScanGate>>,
    #[cfg(test)]
    cancellation_observer: Mutex<Option<Arc<AtomicBool>>>,
}

impl WriteState {
    pub(super) fn new() -> Self {
        Self::new_with_admission(None)
    }

    pub(super) fn new_admitted(operation: SchemaOperationGuard) -> Self {
        Self::new_with_admission(Some(operation))
    }

    fn new_with_admission(retained_schema_operation: Option<SchemaOperationGuard>) -> Self {
        Self {
            inner: Mutex::new(WriteTransactionState::Idle),
            retained_schema_operation,
            armed_statement_epoch: Mutex::new(None),
            generated_insert: Mutex::new(None),
            active_interrupt: Mutex::new(None),
            commit_linearization: Mutex::new(()),
            nonblocking_cancel_requested: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_commit: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_write_corruption: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_commit_corruption: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_savepoint_operation: Mutex::new(None),
            #[cfg(test)]
            commit_gate: Mutex::new(None),
            #[cfg(test)]
            cancellation_observer: Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub(super) fn install_test_controls(
        &self,
        commit_gate: Option<TestChildScanGate>,
        cancellation_observer: Option<Arc<AtomicBool>>,
    ) {
        *self
            .commit_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = commit_gate;
        *self
            .cancellation_observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = cancellation_observer;
    }

    fn wait_before_commit_for_test(&self) -> EngineResult<()> {
        #[cfg(test)]
        {
            let gate = self
                .commit_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if gate.is_some_and(|gate| !gate.wait_for_release()) {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "writable commit test gate timed out or disconnected",
                ));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn observe_nonblocking_cancellation_for_test(&self) {
        if let Some(observer) = self
            .cancellation_observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            observer.store(true, Ordering::Release);
        }
    }

    pub(super) const fn has_retained_schema_admission(&self) -> bool {
        self.retained_schema_operation.is_some()
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
        if self.nonblocking_cancel_requested.load(Ordering::Acquire) {
            return Err(cancelled_error());
        }
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

    fn arm_generated_insert(
        self: &Arc<Self>,
        request: GeneratedInsertRequest,
    ) -> EngineResult<GeneratedInsertArm> {
        let mut generated = self
            .generated_insert
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if generated.is_some() {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "native generated INSERT intent is already armed",
            ));
        }
        *generated = Some(GeneratedInsertIntent {
            request,
            consumed: false,
        });
        Ok(GeneratedInsertArm {
            state: Arc::clone(self),
            request,
        })
    }

    fn generated_insert_target(
        &self,
        registry: &Registry,
        spec: &TableSpec,
        shard_key: ValueRef<'_>,
    ) -> EngineResult<Option<GeneratedInsertTargets>> {
        let mut generated = self
            .generated_insert
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(intent) = generated.as_mut() else {
            return Ok(None);
        };
        if intent.request.table_id != spec.id {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "native generated INSERT was preflighted for a different table than {}",
                    spec.name
                ),
            ));
        }
        if !matches!(shard_key, ValueRef::Null) {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "native generated INSERT for {} unexpectedly supplied an explicit key",
                    spec.name
                ),
            ));
        }
        if intent.consumed {
            return Err(EngineError::new(
                EngineErrorKind::Unsupported,
                "multi-row native generated INSERT is not supported until it can reserve a safe batch",
            ));
        }
        intent.consumed = true;
        let targets = match intent.request.expected_shard {
            Some(shard) => GeneratedInsertTargets::Exact(shard),
            None => GeneratedInsertTargets::Auto(registry.generated_insert_targets(spec)?),
        };
        registry.wait_after_generated_target_selection_for_test()?;
        Ok(Some(targets))
    }

    fn reject_generated_non_insert(&self) -> EngineResult<()> {
        if self
            .generated_insert
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
        {
            Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "native generated INSERT intent reached a non-INSERT callback",
            ))
        } else {
            Ok(())
        }
    }

    pub(super) fn lock_commit_linearization(&self) -> MutexGuard<'_, ()> {
        self.commit_linearization
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn try_lock_commit_linearization(&self) -> Option<MutexGuard<'_, ()>> {
        match self.commit_linearization.try_lock() {
            Ok(guard) => Some(guard),
            Err(TryLockError::Poisoned(error)) => Some(error.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        }
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
                } else if let Err(error) = self.wait_before_commit_for_test() {
                    let _ = transaction.rollback();
                    Err(error)
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
                transaction.statement_shard = None;
                transaction.explicit_key = None;
                transaction.generated_key = None;
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
                shard: transaction.statement_shard.take(),
                explicit_key: transaction.explicit_key.take(),
                generated_key: transaction.generated_key.take(),
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
            let shard_key = spec.write_shard_key(values)?;
            let generated_shard = self.generated_insert_target(registry, spec, shard_key)?;
            transaction.insert(
                registry,
                spec,
                values,
                conflict,
                generated_shard,
                &self.active_interrupt,
            )
        })
    }

    pub(super) fn execute_delete(
        &self,
        registry: &Registry,
        spec: &TableSpec,
        locator: ValueRef<'_>,
    ) -> EngineResult<()> {
        self.reject_generated_non_insert()?;
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
        self.reject_generated_non_insert()?;
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
            let operation = if self.has_retained_schema_admission() {
                None
            } else {
                Some(registry.storage.enter_schema_operation()?)
            };
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

impl CoordinatorCancellation {
    /// Request cancellation without waiting behind durable child finalization.
    ///
    /// The commit-linearization mutex remains the ordering point. Acquiring it
    /// means cancellation won before finalization, so advancing the epoch and
    /// interrupting SQLite force the child transaction to roll back. If the
    /// mutex is already held, finalization won; waiting for its potentially
    /// slow `COMMIT` would block an async runtime thread and interrupting it
    /// could make the durable outcome ambiguous.
    pub(crate) fn cancel_write_nonblocking(&self) {
        let Some(write_state) = &self.write_state else {
            debug_assert!(
                false,
                "write cancellation requires writable coordinator state"
            );
            return;
        };
        #[cfg(test)]
        write_state.observe_nonblocking_cancellation_for_test();
        let Some(_commit) = write_state.try_lock_commit_linearization() else {
            return;
        };

        // Publish the one-shot request before advancing the reusable epoch.
        // Otherwise a statement starting in this narrow window could capture
        // the new epoch and mistake a pre-start cancellation for fresh state.
        write_state
            .nonblocking_cancel_requested
            .store(true, Ordering::Release);
        self.epoch.fetch_add(1, Ordering::AcqRel);
        write_state.interrupt_child();
        self.interrupt.interrupt();
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GeneratedInsertRequest {
    table_id: u64,
    expected_shard: Option<u16>,
}

#[derive(Debug)]
enum GeneratedInsertTargets {
    Exact(u16),
    Auto(Box<[u16]>),
}

impl GeneratedInsertTargets {
    fn candidates(&self) -> &[u16] {
        match self {
            Self::Exact(shard) => std::slice::from_ref(shard),
            Self::Auto(shards) => shards,
        }
    }

    const fn permits_capacity_fallback(&self) -> bool {
        matches!(self, Self::Auto(_))
    }
}

impl Registry {
    fn generated_insert_targets(&self, spec: &TableSpec) -> EngineResult<Box<[u16]>> {
        if spec.targets.is_empty() {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "native generated INSERT for {} has no eligible physical shard",
                    spec.name
                ),
            ));
        }
        let owners = spec.allocation_owners.as_ref().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "registered native-ID table {} has no allocation-owner map",
                    spec.name
                ),
            )
        })?;
        let target_count = u64::try_from(spec.targets.len()).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::LimitExceeded,
                "native generated target count does not fit the selection cursor",
                error,
            )
        })?;
        let start = spec.generated_shard_cursor.fetch_add(1, Ordering::Relaxed) % target_count;
        let start = usize::try_from(start).expect("target cursor modulo length fits usize");
        let mut eligible = Vec::with_capacity(spec.targets.len());
        for offset in 0..spec.targets.len() {
            let shard = spec.targets[(start + offset) % spec.targets.len()];
            if owners.owner_for_physical_shard(shard).is_some() {
                eligible.push(shard);
            }
        }
        if eligible.is_empty() {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "native generated INSERT for {} has no active allocation owner",
                    spec.name
                ),
            ));
        }
        Ok(eligible.into_boxed_slice())
    }

    fn validate_generated_insert_request(
        &self,
        request: GeneratedInsertRequest,
    ) -> EngineResult<()> {
        let spec = self.tables.get(&request.table_id).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "native generated INSERT refers to unknown table identity {}",
                    request.table_id
                ),
            )
        })?;
        spec.ensure_writable()?;
        if !spec.native_id_policy_active {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "native generated INSERT for {} is unavailable until its allocation policy is activated",
                    spec.name
                ),
            ));
        }
        let GeneratedIdPolicy::NativeRangeV1 { column } = &spec.generated_id_policy else {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "registered table {} does not use native_range_v1 generation",
                    spec.name
                ),
            ));
        };
        let shard_key = spec.shard_key.as_ref().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "registered native-ID table {} has no shard-key descriptor",
                    spec.name
                ),
            )
        })?;
        let key_index = usize::try_from(shard_key.column_index).map_err(|_| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "registered native-ID table {} has an invalid key index",
                    spec.name
                ),
            )
        })?;
        if shard_key.key_type != crate::core::ShardKeyType::Int64
            || spec
                .columns
                .get(key_index)
                .map(|column| column.name.as_str())
                != Some(column.as_str())
        {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "registered native-ID policy for {} does not match its Int64 shard key",
                    spec.name
                ),
            ));
        }
        let owners = spec.allocation_owners.as_ref().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "registered native-ID table {} has no allocation-owner map",
                    spec.name
                ),
            )
        })?;
        if let Some(expected_shard) = request.expected_shard {
            if !spec.targets.contains(&expected_shard) {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "native generated INSERT target shard {expected_shard} is not eligible for {}",
                        spec.name
                    ),
                ));
            }
            if owners.owner_for_physical_shard(expected_shard).is_none() {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "native generated INSERT target shard {expected_shard} has no active allocation owner"
                    ),
                ));
            }
        } else if !spec
            .targets
            .iter()
            .any(|shard| owners.owner_for_physical_shard(*shard).is_some())
        {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "native generated INSERT for {} has no active allocation owner",
                    spec.name
                ),
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct GeneratedInsertIntent {
    request: GeneratedInsertRequest,
    consumed: bool,
}

struct GeneratedInsertArm {
    state: Arc<WriteState>,
    request: GeneratedInsertRequest,
}

impl GeneratedInsertArm {
    fn require_consumed(&self) -> EngineResult<()> {
        let generated = self
            .state
            .generated_insert
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match generated.as_ref() {
            Some(intent) if intent.request == self.request && intent.consumed => Ok(()),
            Some(intent) if intent.request == self.request => Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "native generated INSERT completed without allocating a physical row",
            )),
            _ => Err(EngineError::new(
                EngineErrorKind::Internal,
                "native generated INSERT intent changed during statement execution",
            )),
        }
    }
}

impl Drop for GeneratedInsertArm {
    fn drop(&mut self) {
        let mut generated = self
            .state
            .generated_insert
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if generated
            .as_ref()
            .is_some_and(|intent| intent.request == self.request)
        {
            *generated = None;
        }
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
    _operation: Option<SchemaOperationGuard>,
    epoch: u64,
    child: Option<WriteChild>,
    savepoints: Vec<SavepointMark>,
    poison: Option<String>,
    affected_rows: usize,
    statement_shard: Option<u16>,
    explicit_key: Option<Value>,
    generated_key: Option<GeneratedKey>,
}

struct SavepointMark {
    number: i32,
    affected_rows: usize,
    statement_shard: Option<u16>,
    explicit_key: Option<Value>,
    generated_key: Option<GeneratedKey>,
}

impl WriteTransaction {
    fn new(operation: Option<SchemaOperationGuard>, epoch: u64) -> Self {
        Self {
            _operation: operation,
            epoch,
            child: None,
            savepoints: Vec::new(),
            poison: None,
            affected_rows: 0,
            statement_shard: None,
            explicit_key: None,
            generated_key: None,
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

    fn record_write_shard(&mut self, shard: u16) -> EngineResult<()> {
        if self.statement_shard.is_some_and(|current| current != shard) {
            self.poison("one statement reported mutations on two physical shards");
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "brisk_shard statement mutated more than one physical shard",
            ));
        }
        self.statement_shard = Some(shard);
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
            let connection = registry.storage.open_shard_write_cancellable(
                shard,
                Arc::clone(&registry.cancellation_epoch),
                self.epoch,
            )?;
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
                            registry.wait_after_child_busy_for_test()?;
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

    /// Drop a capacity probe that acquired a physical writer lock but made no
    /// change, so an auto-selected generated INSERT can try its next owner.
    fn release_unmutated_generated_candidate(
        &mut self,
        active_interrupt: &Mutex<Option<Arc<InterruptHandle>>>,
    ) -> EngineResult<()> {
        if self.affected_rows != 0
            || self.statement_shard.is_some()
            || self.explicit_key.is_some()
            || self.generated_key.is_some()
        {
            self.poison("generated allocation fallback followed a physical mutation");
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "native generated allocation cannot retry after a physical mutation",
            ));
        }
        let Some(child) = self.child.take() else {
            self.poison("generated allocation fallback lost its physical child");
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "native generated allocation retry has no physical child",
            ));
        };
        let rollback = child
            .connection
            .execute_batch("ROLLBACK")
            .map_err(sqlite_error::statement);
        *active_interrupt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        if let Err(error) = rollback {
            self.poison(format!(
                "generated allocation capacity-probe rollback failed: {error}"
            ));
            return Err(error);
        }
        Ok(())
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
        let statement_shard = self.statement_shard;
        let explicit_key = self.explicit_key.clone();
        let generated_key = self.generated_key.clone();
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
                statement_shard,
                explicit_key: explicit_key.clone(),
                generated_key: generated_key.clone(),
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
        self.statement_shard = self.savepoints[position].statement_shard;
        self.explicit_key = self.savepoints[position].explicit_key.clone();
        self.generated_key = self.savepoints[position].generated_key.clone();
        self.savepoints.truncate(position + 1);
        Ok(())
    }

    fn insert(
        &mut self,
        registry: &Registry,
        spec: &TableSpec,
        values: &[ValueRef<'_>],
        conflict: ConflictMode,
        generated_targets: Option<GeneratedInsertTargets>,
        active_interrupt: &Mutex<Option<Arc<InterruptHandle>>>,
    ) -> EngineResult<i64> {
        spec.ensure_writable()?;
        let shard_key = spec.write_shard_key(values)?;
        if let Some(targets) = generated_targets {
            return self.insert_generated(
                registry,
                spec,
                values,
                conflict,
                targets,
                active_interrupt,
            );
        }
        let shard = registry.insert_target(spec, shard_key)?;
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
        self.record_write_shard(shard)?;
        let rowid = match &explicit_key {
            RawCell::Integer(value) => *value,
            _ => 0,
        };
        self.explicit_key = Some(raw_cell_to_value(explicit_key)?);
        self.ensure_healthy(registry)?;
        Ok(rowid)
    }

    fn insert_generated(
        &mut self,
        registry: &Registry,
        spec: &TableSpec,
        values: &[ValueRef<'_>],
        conflict: ConflictMode,
        targets: GeneratedInsertTargets,
        active_interrupt: &Mutex<Option<Arc<InterruptHandle>>>,
    ) -> EngineResult<i64> {
        let owners = spec.allocation_owners.as_ref().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "registered native-ID table {} has no allocation-owner map",
                    spec.name
                ),
            )
        })?;
        let (sql, parameters, generated_column) =
            spec.generated_insert_sql_and_values(values, conflict)?;

        let pinned_shard = self.child.as_ref().map(|child| child.shard);
        let may_fallback = targets.permits_capacity_fallback() && pinned_shard.is_none();
        let mut selected = None;
        let candidates = targets.candidates();
        for &shard in candidates {
            if pinned_shard.is_some_and(|pinned| pinned != shard) {
                continue;
            }
            let owner = owners.owner_for_physical_shard(shard).ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "native generated INSERT target shard {shard} has no active allocation owner"
                    ),
                )
            })?;
            let capacity = {
                let child = self.pin(registry, shard, active_interrupt)?;
                validate_allocation_sequence_capacity(&child.connection, &spec.name, owner)
            };
            match capacity {
                Ok(()) => {
                    selected = Some((shard, owner));
                    break;
                }
                Err(error) if may_fallback && error.kind() == EngineErrorKind::LimitExceeded => {
                    self.release_unmutated_generated_candidate(active_interrupt)?;
                }
                Err(error) => return Err(error),
            }
        }
        let Some((shard, owner)) = selected else {
            return Err(EngineError::new(
                if pinned_shard.is_some() {
                    EngineErrorKind::FailedPrecondition
                } else {
                    EngineErrorKind::LimitExceeded
                },
                if pinned_shard.is_some() {
                    format!(
                        "native generated INSERT for {} cannot move an existing transaction to another shard",
                        spec.name
                    )
                } else {
                    format!(
                        "native generated table {} exhausted every active allocation owner",
                        spec.name
                    )
                },
            ));
        };
        let insertion = (|| {
            let child = self.pin(registry, shard, active_interrupt)?;
            let mut statement = child
                .connection
                .prepare(&sql)
                .map_err(sqlite_error::statement)?;
            let mut rows = statement
                .query(rusqlite::params_from_iter(&parameters))
                .map_err(sqlite_error::statement)?;
            let generated = match rows.next().map_err(sqlite_error::statement)? {
                Some(row) => row.get::<_, i64>(0).map_err(sqlite_error::statement)?,
                // INSERT OR IGNORE can legitimately report no generated row.
                None => return Ok(None),
            };
            if rows.next().map_err(sqlite_error::statement)?.is_some() {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "native generated INSERT returned more than one generated key",
                ));
            }
            drop(rows);
            drop(statement);
            let sequence = read_allocation_sequence(&child.connection, &spec.name)?;
            if sequence != generated {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!(
                        "native generated table {} returned ID {generated} but SQLite recorded sequence {sequence}",
                        spec.name
                    ),
                ));
            }
            let changed = usize::try_from(child.connection.changes()).map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::LimitExceeded,
                    "SQLite generated INSERT change count does not fit usize",
                    error,
                )
            })?;
            Ok(Some((generated, changed)))
        })();
        let (generated, changed) = match insertion {
            Ok(Some(result)) => result,
            Ok(None) => {
                self.ensure_healthy(registry)?;
                return Ok(0);
            }
            Err(error) => {
                self.poison_if_uncertain(&error);
                return Err(error);
            }
        };

        let decoded = match classify_generated_id(&spec.generated_id_policy, generated)? {
            GeneratedIdClassification::NativeRangeV1(decoded) => decoded,
            GeneratedIdClassification::Legacy(_) => {
                self.poison("SQLite allocated a legacy value for a native generated ID");
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!(
                        "SQLite allocated a non-native ID for registered table {}",
                        spec.name
                    ),
                ));
            }
        };
        if decoded.owner() != owner || owners.physical_shard(decoded.owner()) != Some(shard) {
            self.poison("SQLite allocated a generated ID outside its shard owner range");
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "SQLite allocated native ID {generated} outside shard {shard}'s owner range"
                ),
            ));
        }
        if changed != 1 {
            self.poison("physical generated INSERT did not report exactly one changed row");
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "brisk_shard physical generated INSERT did not change exactly one row",
            ));
        }
        self.record_physical_changes(changed)?;
        self.record_write_shard(shard)?;
        self.generated_key = Some(GeneratedKey::new(generated_column, Value::Int64(generated)));
        self.ensure_healthy(registry)?;
        Ok(generated)
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
        if changed != 0 {
            self.record_write_shard(decoded.shard)?;
        }
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
        if changed != 0 {
            self.record_write_shard(decoded.shard)?;
        }
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
            shard: self.statement_shard.take(),
            explicit_key: self.explicit_key.take(),
            generated_key: self.generated_key.take(),
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
    pub(super) shard: Option<u16>,
    pub(super) explicit_key: Option<Value>,
    pub(super) generated_key: Option<GeneratedKey>,
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
    use std::{
        sync::{atomic::AtomicU64, mpsc},
        thread,
    };

    use super::*;

    fn write_cancellation_fixture() -> (
        Connection,
        CoordinatorCancellation,
        Arc<WriteState>,
        Arc<AtomicU64>,
    ) {
        let connection = Connection::open_in_memory().unwrap();
        let epoch = Arc::new(AtomicU64::new(0));
        let write_state = Arc::new(WriteState::new());
        let cancellation = CoordinatorCancellation {
            epoch: Arc::clone(&epoch),
            active_child_scans: Arc::new(Mutex::new(0)),
            interrupt: Arc::new(connection.get_interrupt_handle()),
            write_state: Some(Arc::clone(&write_state)),
        };
        (connection, cancellation, write_state, epoch)
    }

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
            shard: Some(0),
            explicit_key: Some(Value::Int64(7)),
            generated_key: None,
        };
        assert_eq!(result.affected_rows(), 1);
        assert_eq!(result.shard(), Some(0));
        assert_eq!(result.explicit_key(), Some(&Value::Int64(7)));
        assert_eq!(result.generated_key(), None);
        assert_eq!(super::super::MODULE_NAME, "brisk_shard");
    }

    #[test]
    fn sqlite_returning_and_last_insert_rowid_match_on_the_same_connection() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE allocated (id INTEGER PRIMARY KEY AUTOINCREMENT);
                 CREATE TABLE unrelated (id INTEGER PRIMARY KEY AUTOINCREMENT);",
            )
            .unwrap();
        let owner = AllocationOwnerSlot::new(3).unwrap();
        let floor = native_range_v1_sequence_floor(owner);
        connection
            .execute(
                "INSERT INTO sqlite_sequence (name, seq) VALUES ('allocated', ?1)",
                [floor],
            )
            .unwrap();

        let captured = connection
            .query_row(
                "INSERT INTO allocated DEFAULT VALUES RETURNING id",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(
            captured,
            crate::core::generated_id::native_range_v1_first_id(owner)
        );
        assert_eq!(connection.last_insert_rowid(), captured);
        assert_eq!(
            connection
                .query_row(
                    "SELECT seq FROM sqlite_sequence WHERE name = 'allocated'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            captured
        );

        // The captured RETURNING value is stable, while a later operation on
        // the same handle demonstrates why adapters must not consult the
        // ambient last_insert_rowid after releasing or reusing a connection.
        connection
            .execute("INSERT INTO unrelated DEFAULT VALUES", [])
            .unwrap();
        assert_ne!(connection.last_insert_rowid(), captured);
        assert_eq!(
            connection
                .query_row("SELECT id FROM allocated", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            captured
        );
    }

    #[test]
    fn nonblocking_write_cancellation_wins_before_child_finalization() {
        let (connection, cancellation, write_state, epoch) = write_cancellation_fixture();

        cancellation.cancel_write_nonblocking();

        assert_eq!(epoch.load(Ordering::Acquire), 1);
        let arm_error = match write_state.arm_statement(epoch.load(Ordering::Acquire)) {
            Ok(_) => panic!("a pre-start cancellation was lost at statement arm"),
            Err(error) => error,
        };
        assert_eq!(arm_error.kind(), EngineErrorKind::Cancelled);
        let _commit = write_state.lock_commit_linearization();
        assert_eq!(
            connection.query_row("SELECT 1", [], |row| row.get::<_, i64>(0)),
            Ok(1)
        );
    }

    #[test]
    fn nonblocking_write_cancellation_does_not_wait_after_finalization_wins() {
        let (_connection, cancellation, write_state, epoch) = write_cancellation_fixture();
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let finalizer_state = Arc::clone(&write_state);
        let finalizer = thread::spawn(move || {
            let _commit = finalizer_state.lock_commit_linearization();
            locked_tx.send(()).unwrap();
            let _ = release_rx.recv();
        });
        locked_rx.recv().unwrap();

        let (returned_tx, returned_rx) = mpsc::channel();
        let canceller = thread::spawn(move || {
            cancellation.cancel_write_nonblocking();
            returned_tx.send(()).unwrap();
        });
        let returned_before_release = returned_rx.recv_timeout(Duration::from_secs(1)).is_ok();

        release_tx.send(()).unwrap();
        finalizer.join().unwrap();
        canceller.join().unwrap();

        assert!(
            returned_before_release,
            "write cancellation waited behind child finalization"
        );
        assert_eq!(epoch.load(Ordering::Acquire), 0);
    }
}
