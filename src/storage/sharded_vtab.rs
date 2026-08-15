//! Experimental, statically registered SQLite virtual-table facade.
//!
//! This module is deliberately isolated behind `experimental-vtab`. It proves
//! the no-fork boundary without replacing the existing scatter/gather path or
//! changing any physical shard schema.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::c_int,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

#[cfg(test)]
use std::{sync::mpsc, time::Duration};

use rusqlite::{
    Connection, Error as SqliteError, InterruptHandle, Result as SqliteResult, ffi,
    hooks::{AuthAction, AuthContext, Authorization},
    types::{ToSql, ToSqlOutput, ValueRef},
    vtab::{
        ConflictMode, Context, CreateVTab, Filters, IndexConstraintOp, IndexInfo, Inserts,
        TransactionVTab, UpdateVTab, Updates, VTab, VTabConfig, VTabConnection, VTabCursor,
        VTabKind, read_only_module,
    },
};

use super::{
    SchemaOperationGuard, SqliteAffinity, Storage, quote_identifier, shard, sqlite_affinity,
};
use crate::{
    core::generated_id::{
        GeneratedIdClassification, classify_caller_generated_id, classify_generated_id,
    },
    core::{
        AllocationOwnerMap, CanonicalShardKeyRef, EngineError, EngineErrorKind, EngineResult,
        GeneratedIdPolicy, GlobalIndexKeySource, GlobalIndexLifecycle, GlobalIndexMetadata,
        OperationControl, ShardKeyType, TableMetadata, TablePlacement, canonical_shard_key_bytes,
    },
    sqlite_error,
};

const MODULE_NAME: &str = "brisk_shard";
const MAX_CURSOR_ROWS: usize = 65_536;
const MAX_CURSOR_BYTES: usize = 64 * 1024 * 1024;
const ALLOCATION_OVERHEAD_BYTES: usize = 16;
const ROW_ACCOUNTING_BYTES: usize = size_of::<Vec<RawCell>>() * 4 + ALLOCATION_OVERHEAD_BYTES;
const VALUE_ACCOUNTING_BYTES: usize = size_of::<RawCell>();
const SCAN_PLAN: c_int = 0;
const SHARD_KEY_EQUALITY_PLAN: c_int = 1;
const LOCATOR_COLUMN_NAME: &str = shard::WRITABLE_LOCATOR_COLUMN_NAME;

mod locator;
mod module_v2;
mod write;

#[allow(unused_imports)]
pub(crate) use write::{CoordinatorWriteResult, WriteCoordinator};

/// Engine-local cache of immutable physical table descriptors for one schema
/// generation. Each coordinator still owns independent transaction and
/// cancellation state; only the validated registry blueprint is shared.
#[derive(Debug, Default)]
pub(crate) struct RegistrySchemaCache {
    inner: Mutex<Option<Arc<RegistrySchema>>>,
    #[cfg(test)]
    child_busy_gate: Mutex<Option<TestChildScanGate>>,
    #[cfg(test)]
    commit_gate: Mutex<Option<TestChildScanGate>>,
    #[cfg(test)]
    cancellation_observer: Mutex<Option<Arc<std::sync::atomic::AtomicBool>>>,
}

impl RegistrySchemaCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn requires_bootstrap(&self, schema_generation: u64) -> bool {
        self.current(schema_generation).is_none()
    }

    fn current(&self, schema_generation: u64) -> Option<Arc<RegistrySchema>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|schema| schema.schema_generation == schema_generation)
            .cloned()
    }

    fn publish(&self, schema: Arc<RegistrySchema>) {
        *self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(schema);
    }

    #[cfg(test)]
    pub(crate) fn install_child_busy_gate(&self) -> TestChildScanControl {
        let (gate, control) = TestChildScanGate::channel();
        *self
            .child_busy_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(gate);
        control
    }

    #[cfg(test)]
    pub(crate) fn install_commit_gate(&self) -> TestChildScanControl {
        let (gate, control) = TestChildScanGate::channel();
        *self
            .commit_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(gate);
        control
    }

    #[cfg(test)]
    pub(crate) fn install_cancellation_observer(&self) -> Arc<std::sync::atomic::AtomicBool> {
        let observer = Arc::new(std::sync::atomic::AtomicBool::new(false));
        *self
            .cancellation_observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&observer));
        observer
    }

    #[cfg(test)]
    fn take_write_test_controls(
        &self,
    ) -> (
        Option<TestChildScanGate>,
        Option<TestChildScanGate>,
        Option<Arc<std::sync::atomic::AtomicBool>>,
    ) {
        let child_busy = self
            .child_busy_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let commit = self
            .commit_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let cancellation = self
            .cancellation_observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        (child_busy, commit, cancellation)
    }
}

/// A separate SQLite coordinator whose logical tables are backed by BriskDB
/// shard files. The coordinator never attaches those files to its own schema.
///
/// SQLite invokes virtual-table callbacks synchronously on the connection's
/// owning thread. Each cursor therefore opens and closes its own validated
/// child handle and never recursively calls the coordinator connection.
pub(crate) struct ReadCoordinator {
    connection: Connection,
    registry: Arc<Registry>,
    cancellation: CoordinatorCancellation,
}

impl ReadCoordinator {
    /// Open an ephemeral coordinator. Physical files and the manifest remain
    /// untouched; only the in-memory coordinator contains virtual tables.
    pub(crate) fn open(storage: Storage) -> EngineResult<Self> {
        let connection = Connection::open_in_memory().map_err(sqlite_error::storage)?;
        Self::open_connection(storage, connection)
    }

    /// Open a durable coordinator schema for lifecycle tests. The production
    /// spike is intentionally ephemeral until coordinator-file identity and
    /// ownership become a supported storage contract.
    #[cfg(test)]
    fn open_at(storage: Storage, path: impl AsRef<std::path::Path>) -> EngineResult<Self> {
        let connection = Connection::open(path).map_err(sqlite_error::storage)?;
        Self::open_connection(storage, connection)
    }

    fn open_connection(storage: Storage, connection: Connection) -> EngineResult<Self> {
        Self::open_connection_with_limits(storage, connection, CursorLimits::default())
    }

    #[cfg(test)]
    fn open_with_limits(storage: Storage, limits: CursorLimits) -> EngineResult<Self> {
        let connection = Connection::open_in_memory().map_err(sqlite_error::storage)?;
        Self::open_connection_with_limits(storage, connection, limits)
    }

    fn open_connection_with_limits(
        storage: Storage,
        connection: Connection,
        limits: CursorLimits,
    ) -> EngineResult<Self> {
        // Hold schema admission through discovery and coordinator bootstrap so
        // open cannot return an already-stale declaration.
        let bootstrap_operation = storage.enter_schema_operation()?;
        let registry = Registry::build_admitted_with_limits(storage, limits)?;
        register_module(&connection, Arc::clone(&registry)).map_err(sqlite_error::storage)?;
        bootstrap_coordinator_schema(&connection, &registry)?;

        connection
            .pragma_update(None, "trusted_schema", "OFF")
            .map_err(sqlite_error::storage)?;
        connection
            .pragma_update(None, "query_only", "ON")
            .map_err(sqlite_error::storage)?;

        attach_coordinator_authorizer(&connection)?;
        if registry.storage.current_schema_generation() != registry.schema_generation {
            return Err(EngineError::new(
                EngineErrorKind::Busy,
                "application schema changed while the brisk_shard coordinator was opening",
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
        })
    }

    pub(crate) const fn connection(&self) -> &Connection {
        &self.connection
    }

    pub(crate) fn cancellation_handle(&self) -> CoordinatorCancellation {
        self.cancellation.clone()
    }

    #[cfg(test)]
    fn lifecycle(&self) -> Arc<LifecycleCounters> {
        Arc::clone(&self.registry.lifecycle)
    }

    #[cfg(test)]
    fn take_opened_shards(&self) -> Vec<u16> {
        std::mem::take(
            &mut *self
                .registry
                .opened_shards
                .lock()
                .expect("virtual-table shard diagnostics are not poisoned"),
        )
    }

    #[cfg(test)]
    fn install_child_scan_gate(&self) -> TestChildScanControl {
        let (gate, control) = TestChildScanGate::channel();
        *self
            .registry
            .child_scan_gate
            .lock()
            .expect("child-scan test gate is not poisoned") = Some(gate);
        control
    }

    #[cfg(test)]
    fn install_child_scan_complete_gate(&self) -> TestChildScanControl {
        let (gate, control) = TestChildScanGate::channel();
        *self
            .registry
            .child_scan_complete_gate
            .lock()
            .expect("child-scan completion test gate is not poisoned") = Some(gate);
        control
    }
}

/// Cancels a child-shard scan currently inside an `xFilter`/`xNext` callback.
/// Incrementing the epoch does not poison later queries; each new filter
/// captures the then-current epoch. SQLite defines an interrupt issued while
/// the coordinator is idle as harmless to statements started later, so every
/// cancellation also interrupts stock-SQLite work above the virtual table.
#[derive(Clone)]
pub(crate) struct CoordinatorCancellation {
    epoch: Arc<AtomicU64>,
    active_child_scans: Arc<Mutex<usize>>,
    interrupt: Arc<InterruptHandle>,
    write_state: Option<Arc<write::WriteState>>,
}

impl CoordinatorCancellation {
    pub(crate) fn cancel(&self) {
        // Child COMMIT is the durability linearization point: cancellation
        // either wins this mutex before COMMIT and forces rollback through the
        // epoch check, or waits until a completed COMMIT has won. Interrupting
        // an in-flight durable commit could only turn a known result into an
        // ambiguous one, so that narrow finalization window is deliberately
        // non-cancellable.
        let _commit = self
            .write_state
            .as_ref()
            .map(|state| state.lock_commit_linearization());
        let _active_child_scans = self
            .active_child_scans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.epoch.fetch_add(1, Ordering::AcqRel);
        if let Some(write_state) = &self.write_state {
            write_state.cancel_authority();
            write_state.interrupt_child();
        }
        self.interrupt.interrupt();
    }
}

fn bootstrap_coordinator_schema(connection: &Connection, registry: &Registry) -> EngineResult<()> {
    let expected = registry
        .tables
        .values()
        .map(|spec| {
            (
                spec.name.clone(),
                ("table".to_owned(), Some(spec.create_virtual_table_sql())),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let observed = coordinator_schema_inventory(connection)?;
    if observed
        .iter()
        .any(|(name, definition)| expected.get(name) != Some(definition))
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "coordinator schema contains an unexpected or shadowing object",
        ));
    }

    for spec in registry.tables.values() {
        if !observed.contains_key(&spec.name) {
            connection
                .execute_batch(&spec.create_virtual_table_sql())
                .map_err(sqlite_error::storage)?;
        }
    }

    if coordinator_schema_inventory(connection)? != expected {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "coordinator schema does not exactly match the brisk_shard registry",
        ));
    }
    Ok(())
}

fn coordinator_schema_inventory(
    connection: &Connection,
) -> EngineResult<BTreeMap<String, (String, Option<String>)>> {
    let mut statement = connection
        .prepare(
            "SELECT name, type, sql
             FROM main.sqlite_schema
             WHERE name NOT GLOB 'sqlite_*'
             ORDER BY name COLLATE BINARY",
        )
        .map_err(sqlite_error::storage)?;
    statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                (row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?),
            ))
        })
        .map_err(sqlite_error::storage)?
        .collect::<Result<BTreeMap<_, _>, _>>()
        .map_err(sqlite_error::storage)
}

fn attach_coordinator_authorizer(connection: &Connection) -> EngineResult<()> {
    connection
        .authorizer(Some(|context: AuthContext<'_>| match context.action {
            AuthAction::Select | AuthAction::Read { .. } | AuthAction::Recursive => {
                Authorization::Allow
            }
            AuthAction::Function { function_name }
                if !function_name.eq_ignore_ascii_case("load_extension") =>
            {
                Authorization::Allow
            }
            AuthAction::Pragma {
                pragma_name,
                pragma_value: None,
            } if pragma_name.eq_ignore_ascii_case("query_only")
                || pragma_name.eq_ignore_ascii_case("database_list") =>
            {
                Authorization::Allow
            }
            _ => Authorization::Deny,
        }))
        .map_err(sqlite_error::storage)
}

fn attach_writable_coordinator_authorizer(
    connection: &Connection,
    registry: &Registry,
    allow_transaction_control: Arc<std::sync::atomic::AtomicBool>,
) -> EngineResult<()> {
    let registered_tables = registry
        .tables
        .values()
        .map(|table| table.name.clone())
        .collect::<BTreeSet<_>>();
    connection
        .authorizer(Some(move |context: AuthContext<'_>| {
            if context
                .database_name
                .is_some_and(|database| !database.eq_ignore_ascii_case("main"))
                || context.accessor.is_some()
            {
                return Authorization::Deny;
            }
            match context.action {
                AuthAction::Select | AuthAction::Recursive => Authorization::Allow,
                AuthAction::Read { table_name, .. } => {
                    if registered_tables.contains(table_name) {
                        Authorization::Allow
                    } else {
                        Authorization::Deny
                    }
                }
                AuthAction::Insert { table_name }
                | AuthAction::Delete { table_name }
                | AuthAction::Update { table_name, .. } => {
                    if registered_tables.contains(table_name) {
                        Authorization::Allow
                    } else {
                        Authorization::Deny
                    }
                }
                AuthAction::Function { function_name }
                    if !function_name.eq_ignore_ascii_case("load_extension") =>
                {
                    Authorization::Allow
                }
                AuthAction::Transaction { .. } | AuthAction::Savepoint { .. }
                    if allow_transaction_control.load(Ordering::Acquire) =>
                {
                    Authorization::Allow
                }
                AuthAction::Pragma {
                    pragma_name,
                    pragma_value: None,
                } if pragma_name.eq_ignore_ascii_case("database_list") => Authorization::Allow,
                _ => Authorization::Deny,
            }
        }))
        .map_err(sqlite_error::storage)
}

fn register_module(connection: &Connection, registry: Arc<Registry>) -> SqliteResult<()> {
    connection.create_module(
        c"brisk_shard",
        read_only_module::<BriskShardTable>(),
        Some(registry),
    )
}

struct Registry {
    storage: Storage,
    schema_generation: u64,
    tables: Arc<BTreeMap<u64, Arc<TableSpec>>>,
    mode: CoordinatorMode,
    write_state: Option<Arc<write::WriteState>>,
    limits: CursorLimits,
    cancellation_epoch: Arc<AtomicU64>,
    active_child_scans: Arc<Mutex<usize>>,
    lifecycle: Arc<LifecycleCounters>,
    generated_shard_admission: Mutex<Option<Arc<GeneratedShardAdmission>>>,
    #[cfg(test)]
    opened_shards: Mutex<Vec<u16>>,
    #[cfg(test)]
    child_scan_gate: Mutex<Option<TestChildScanGate>>,
    #[cfg(test)]
    child_scan_complete_gate: Mutex<Option<TestChildScanGate>>,
    #[cfg(test)]
    write_child_busy_gate: Mutex<Option<TestChildScanGate>>,
    #[cfg(test)]
    generated_target_gate: Mutex<Option<TestChildScanGate>>,
}

type GeneratedShardAdmission = dyn Fn(u16) -> EngineResult<()> + Send + Sync;

#[derive(Debug)]
struct RegistrySchema {
    schema_generation: u64,
    tables: Arc<BTreeMap<u64, Arc<TableSpec>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoordinatorMode {
    ReadOnly,
    Writable,
}

#[derive(Debug, Clone, Copy)]
struct CursorLimits {
    rows: usize,
    bytes: usize,
}

impl Default for CursorLimits {
    fn default() -> Self {
        Self {
            rows: MAX_CURSOR_ROWS,
            bytes: MAX_CURSOR_BYTES,
        }
    }
}

impl Registry {
    fn build_admitted(storage: Storage) -> EngineResult<Arc<Self>> {
        Self::build_admitted_with_limits_and_mode(
            storage,
            CursorLimits::default(),
            CoordinatorMode::ReadOnly,
            None,
            None,
        )
    }

    fn build_admitted_with_limits(
        storage: Storage,
        limits: CursorLimits,
    ) -> EngineResult<Arc<Self>> {
        Self::build_admitted_with_limits_and_mode(
            storage,
            limits,
            CoordinatorMode::ReadOnly,
            None,
            None,
        )
    }

    fn build_writable(storage: Storage, limits: CursorLimits) -> EngineResult<Arc<Self>> {
        Self::build_admitted_with_limits_and_mode(
            storage,
            limits,
            CoordinatorMode::Writable,
            None,
            None,
        )
    }

    fn build_writable_admitted(
        storage: Storage,
        limits: CursorLimits,
        operation: SchemaOperationGuard,
        control: Option<Arc<OperationControl>>,
    ) -> EngineResult<Arc<Self>> {
        Self::build_admitted_with_limits_and_mode(
            storage,
            limits,
            CoordinatorMode::Writable,
            Some(operation),
            control,
        )
    }

    fn build_writable_cached(
        storage: Storage,
        limits: CursorLimits,
        operation: SchemaOperationGuard,
        control: Arc<OperationControl>,
        cache: &RegistrySchemaCache,
    ) -> EngineResult<Arc<Self>> {
        Self::validate_limits(limits)?;
        let schema_generation = storage.current_schema_generation();
        let schema = match cache.current(schema_generation) {
            Some(schema) => schema,
            None => {
                let schema = Arc::new(Self::discover_schema(
                    &storage,
                    schema_generation,
                    Some(control),
                )?);
                cache.publish(Arc::clone(&schema));
                schema
            }
        };
        Self::from_schema(
            storage,
            limits,
            CoordinatorMode::Writable,
            Some(operation),
            schema,
        )
    }

    fn build_admitted_with_limits_and_mode(
        storage: Storage,
        limits: CursorLimits,
        mode: CoordinatorMode,
        admitted_operation: Option<SchemaOperationGuard>,
        bootstrap_control: Option<Arc<OperationControl>>,
    ) -> EngineResult<Arc<Self>> {
        Self::validate_limits(limits)?;
        let schema_generation = storage.current_schema_generation();
        let schema = Arc::new(Self::discover_schema(
            &storage,
            schema_generation,
            bootstrap_control,
        )?);
        Self::from_schema(storage, limits, mode, admitted_operation, schema)
    }

    fn validate_limits(limits: CursorLimits) -> EngineResult<()> {
        if limits.rows == 0 || limits.bytes == 0 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "brisk_shard result limits must be non-zero",
            ));
        }
        Ok(())
    }

    fn discover_schema(
        storage: &Storage,
        schema_generation: u64,
        bootstrap_control: Option<Arc<OperationControl>>,
    ) -> EngineResult<RegistrySchema> {
        let allocation_owners = storage.allocation_owner_map().cloned().map(Arc::new);
        let tables = match bootstrap_control {
            Some(control) => storage.with_shard_read_only_controlled(0, control, |shard| {
                shard
                    .pragma_update(None, "query_only", "ON")
                    .map_err(sqlite_error::storage)?;
                Self::discover_tables(storage, shard, allocation_owners.as_ref())
            })?,
            None => {
                let shard = storage.open_shard_read_only(0)?;
                shard
                    .pragma_update(None, "query_only", "ON")
                    .map_err(sqlite_error::storage)?;
                Self::discover_tables(storage, &shard, allocation_owners.as_ref())?
            }
        };
        Ok(RegistrySchema {
            schema_generation,
            tables: Arc::new(tables),
        })
    }

    fn from_schema(
        storage: Storage,
        limits: CursorLimits,
        mode: CoordinatorMode,
        admitted_operation: Option<SchemaOperationGuard>,
        schema: Arc<RegistrySchema>,
    ) -> EngineResult<Arc<Self>> {
        if schema.schema_generation != storage.current_schema_generation() {
            return Err(EngineError::new(
                EngineErrorKind::Busy,
                "cached brisk_shard registry belongs to a stale schema generation",
            ));
        }
        let write_state = matches!(mode, CoordinatorMode::Writable).then(|| {
            Arc::new(
                admitted_operation
                    .map_or_else(write::WriteState::new, write::WriteState::new_admitted),
            )
        });
        Ok(Arc::new(Self {
            storage,
            schema_generation: schema.schema_generation,
            tables: Arc::clone(&schema.tables),
            mode,
            write_state,
            limits,
            cancellation_epoch: Arc::new(AtomicU64::new(0)),
            active_child_scans: Arc::new(Mutex::new(0)),
            lifecycle: Arc::new(LifecycleCounters::default()),
            generated_shard_admission: Mutex::new(None),
            #[cfg(test)]
            opened_shards: Mutex::new(Vec::new()),
            #[cfg(test)]
            child_scan_gate: Mutex::new(None),
            #[cfg(test)]
            child_scan_complete_gate: Mutex::new(None),
            #[cfg(test)]
            write_child_busy_gate: Mutex::new(None),
            #[cfg(test)]
            generated_target_gate: Mutex::new(None),
        }))
    }

    fn wait_after_child_busy_for_test(&self) -> EngineResult<()> {
        #[cfg(test)]
        {
            let gate = self
                .write_child_busy_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if gate.is_some_and(|gate| !gate.wait_for_release()) {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "writable child-busy test gate timed out or disconnected",
                ));
            }
        }
        Ok(())
    }

    fn wait_after_generated_target_selection_for_test(&self) -> EngineResult<()> {
        #[cfg(test)]
        {
            let gate = self
                .generated_target_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if gate.is_some_and(|gate| !gate.wait_for_release()) {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "native generated target-selection test gate timed out or disconnected",
                ));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn install_write_test_controls(&self, cache: &RegistrySchemaCache) {
        let (child_busy, commit, cancellation) = cache.take_write_test_controls();
        *self
            .write_child_busy_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = child_busy;
        self.write_state()
            .install_test_controls(commit, cancellation);
    }

    fn discover_tables(
        storage: &Storage,
        shard: &Connection,
        allocation_owners: Option<&Arc<AllocationOwnerMap>>,
    ) -> EngineResult<BTreeMap<u64, Arc<TableSpec>>> {
        let mut tables = BTreeMap::new();
        for table in storage.logical_catalog().tables() {
            #[allow(unreachable_patterns)]
            let targets = match table.placement() {
                TablePlacement::Sharded(_) => (0..storage.shard_count()).collect::<Vec<_>>(),
                // Global rows are replicated physically but exposed once from
                // their canonical read owner.
                TablePlacement::Global => vec![0],
                TablePlacement::Catalog => continue,
                _ => {
                    return Err(EngineError::new(
                        EngineErrorKind::Unsupported,
                        format!(
                            "registered table {} has an unsupported placement",
                            table.name()
                        ),
                    ));
                }
            };
            let spec = TableSpec::from_physical_table(
                shard,
                table,
                storage.logical_catalog().global_indexes(),
                storage.generated_id_policy_is_active(table.id()),
                allocation_owners.cloned(),
                targets.into_boxed_slice(),
            )?;
            tables.insert(table.id().get(), Arc::new(spec));
        }
        Ok(tables)
    }

    fn table(&self, id: u64) -> Option<Arc<TableSpec>> {
        self.tables.get(&id).cloned()
    }

    fn table_named(&self, name: &str) -> Option<&Arc<TableSpec>> {
        self.tables.values().find(|spec| spec.name == name)
    }

    fn write_state(&self) -> &Arc<write::WriteState> {
        self.write_state
            .as_ref()
            .expect("writable coordinator registry has shared write state")
    }

    fn has_retained_schema_admission(&self) -> bool {
        self.write_state
            .as_ref()
            .is_some_and(|state| state.has_retained_schema_admission())
    }

    fn equality_scan(
        &self,
        spec: &TableSpec,
        value: ValueRef<'_>,
    ) -> EngineResult<(RawCell, Box<[u16]>, usize)> {
        let equality_bytes = RawCell::accounted_payload_bytes(value)
            .checked_add(VALUE_ACCOUNTING_BYTES)
            .and_then(|bytes| bytes.checked_add(ALLOCATION_OVERHEAD_BYTES))
            .ok_or_else(|| limit_error("byte", self.limits.bytes))?;
        if equality_bytes > self.limits.bytes {
            return Err(limit_error("byte", self.limits.bytes));
        }
        let equality = RawCell::try_copy_from(value)?;
        let targets = match self.equality_targets(spec, value)? {
            EqualityTargets::Empty => Box::default(),
            EqualityTargets::One(shard) => {
                if !spec.targets.contains(&shard) {
                    return Err(EngineError::new(
                        EngineErrorKind::DataCorruption,
                        format!(
                            "registered table {} routed outside its declared target set",
                            spec.name
                        ),
                    ));
                }
                vec![shard].into_boxed_slice()
            }
            EqualityTargets::All => spec.targets.clone(),
        };
        Ok((equality, targets, equality_bytes))
    }

    fn equality_targets(
        &self,
        spec: &TableSpec,
        value: ValueRef<'_>,
    ) -> EngineResult<EqualityTargets> {
        let Some(shard_key) = &spec.shard_key else {
            return Ok(EqualityTargets::All);
        };
        if matches!(value, ValueRef::Null) {
            return Ok(EqualityTargets::Empty);
        }

        #[allow(unreachable_patterns)]
        let target = match (shard_key.key_type, value) {
            (ShardKeyType::Int64, ValueRef::Integer(value)) => {
                match classify_generated_id(&spec.generated_id_policy, value)? {
                    GeneratedIdClassification::Legacy(value) => Some(self.storage.shard_for_key(
                        &canonical_shard_key_bytes(CanonicalShardKeyRef::Int64(value)),
                    )),
                    GeneratedIdClassification::NativeRangeV1(id) => {
                        let owners = spec.allocation_owners.as_ref().ok_or_else(|| {
                            EngineError::new(
                                EngineErrorKind::DataCorruption,
                                format!(
                                    "registered native-ID table {} has no allocation-owner map",
                                    spec.name
                                ),
                            )
                        })?;
                        return Ok(owners
                            .physical_shard(id.owner())
                            .map_or(EqualityTargets::Empty, EqualityTargets::One));
                    }
                    GeneratedIdClassification::HiloV1(id) => Some(self.storage.shard_for_key(
                        &canonical_shard_key_bytes(CanonicalShardKeyRef::Int64(id.encode())),
                    )),
                }
            }
            (ShardKeyType::Text, ValueRef::Text(value)) => {
                std::str::from_utf8(value).ok().map(|value| {
                    self.storage.shard_for_key(&canonical_shard_key_bytes(
                        CanonicalShardKeyRef::Text(value),
                    ))
                })
            }
            (ShardKeyType::Binary, ValueRef::Blob(value)) => Some(self.storage.shard_for_key(
                &canonical_shard_key_bytes(CanonicalShardKeyRef::Binary(value)),
            )),
            (ShardKeyType::Int64, _) | (ShardKeyType::Text, _) | (ShardKeyType::Binary, _) => {
                return Ok(EqualityTargets::All);
            }
            // ShardKeyType is non-exhaustive. A future type is not safe to
            // single-route until this feature learns its canonical encoding.
            (_, _) => return Ok(EqualityTargets::All),
        };
        Ok(target.map_or(EqualityTargets::All, EqualityTargets::One))
    }

    fn write_target(&self, spec: &TableSpec, value: ValueRef<'_>) -> EngineResult<u16> {
        let shard_key = spec.shard_key.as_ref().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Unsupported,
                format!("registered table {} has no writable shard key", spec.name),
            )
        })?;
        #[allow(unreachable_patterns)]
        let target = match (shard_key.key_type, value) {
            (_, ValueRef::Null) => {
                return Err(EngineError::new(
                    EngineErrorKind::NotNullViolation,
                    format!("registered table {} shard key cannot be NULL", spec.name),
                ));
            }
            (ShardKeyType::Int64, ValueRef::Integer(value)) => {
                match classify_caller_generated_id(&spec.generated_id_policy, value)? {
                    GeneratedIdClassification::Legacy(value) => self.storage.shard_for_key(
                        &canonical_shard_key_bytes(CanonicalShardKeyRef::Int64(value)),
                    ),
                    GeneratedIdClassification::NativeRangeV1(id) => spec
                        .allocation_owners
                        .as_ref()
                        .ok_or_else(|| {
                            EngineError::new(
                                EngineErrorKind::DataCorruption,
                                format!(
                                    "registered native-ID table {} has no allocation-owner map",
                                    spec.name
                                ),
                            )
                        })?
                        .physical_shard(id.owner())
                        .ok_or_else(|| {
                            EngineError::new(
                                EngineErrorKind::FailedPrecondition,
                                format!(
                                    "native ID for {} refers to an unassigned allocation owner",
                                    spec.name
                                ),
                            )
                        })?,
                    GeneratedIdClassification::HiloV1(id) => self.storage.shard_for_key(
                        &canonical_shard_key_bytes(CanonicalShardKeyRef::Int64(id.encode())),
                    ),
                }
            }
            (ShardKeyType::Text, ValueRef::Text(value)) => {
                let value = std::str::from_utf8(value).map_err(|error| {
                    EngineError::from_source(
                        EngineErrorKind::InvalidTextEncoding,
                        format!(
                            "registered table {} shard key is not valid UTF-8",
                            spec.name
                        ),
                        error,
                    )
                })?;
                self.storage
                    .shard_for_key(&canonical_shard_key_bytes(CanonicalShardKeyRef::Text(
                        value,
                    )))
            }
            (ShardKeyType::Binary, ValueRef::Blob(value)) => self.storage.shard_for_key(
                &canonical_shard_key_bytes(CanonicalShardKeyRef::Binary(value)),
            ),
            (ShardKeyType::Int64, _) | (ShardKeyType::Text, _) | (ShardKeyType::Binary, _) => {
                return Err(EngineError::new(
                    EngineErrorKind::TypeMismatch,
                    format!(
                        "registered table {} shard key has the wrong SQLite storage class",
                        spec.name
                    ),
                ));
            }
            (_, _) => {
                return Err(EngineError::new(
                    EngineErrorKind::Unsupported,
                    format!(
                        "registered table {} uses an unsupported shard-key type",
                        spec.name
                    ),
                ));
            }
        };
        if !spec.targets.contains(&target) {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "registered table {} routed outside its declared writable target set",
                    spec.name
                ),
            ));
        }
        Ok(target)
    }

    /// Resolve a caller-supplied key for INSERT or a key-changing UPDATE while
    /// preventing new rows from being introduced into allocator-owned or
    /// retired namespaces. Historical IDs remain routable through
    /// `write_target` for lookup and unchanged-key mutation semantics.
    fn caller_new_key_target(&self, spec: &TableSpec, value: ValueRef<'_>) -> EngineResult<u16> {
        if let (GeneratedIdPolicy::HiloV1 { .. }, ValueRef::Integer(value)) =
            (&spec.generated_id_policy, value)
        {
            if matches!(
                classify_caller_generated_id(&spec.generated_id_policy, value)?,
                GeneratedIdClassification::HiloV1(_)
            ) {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "hilo_v1 ID for {} is allocator-owned and cannot be supplied explicitly",
                        spec.name
                    ),
                ));
            }
        }
        if let (GeneratedIdPolicy::NativeRangeV1 { .. }, ValueRef::Integer(value)) =
            (&spec.generated_id_policy, value)
        {
            if let GeneratedIdClassification::NativeRangeV1(id) =
                classify_caller_generated_id(&spec.generated_id_policy, value)?
            {
                if let Some(owners) = spec.allocation_owners.as_ref() {
                    if owners.physical_shard(id.owner()).is_some()
                        && !owners.owner_is_active(id.owner())
                    {
                        return Err(EngineError::new(
                            EngineErrorKind::FailedPrecondition,
                            format!(
                                "native ID for {} uses retired allocation owner {}",
                                spec.name,
                                id.owner().get()
                            ),
                        ));
                    }
                }
            }
        }
        self.write_target(spec, value)
    }

    fn cancelled(&self, scan_epoch: u64) -> bool {
        self.cancellation_epoch.load(Ordering::Acquire) != scan_epoch
    }

    fn read_shard_rows(
        self: &Arc<Self>,
        spec: &TableSpec,
        shard_id: u16,
        equality: Option<&RawCell>,
        scan_epoch: u64,
        remaining_rows: usize,
        remaining_bytes: usize,
    ) -> EngineResult<(Vec<Vec<RawCell>>, usize)> {
        if self.cancelled(scan_epoch) {
            return Err(cancelled_error());
        }
        if self.mode == CoordinatorMode::Writable {
            return self.write_state().read_shard_rows(
                self,
                spec,
                shard_id,
                equality,
                scan_epoch,
                remaining_rows,
                remaining_bytes,
            );
        }

        let connection = self.storage.open_shard_read_only(shard_id)?;
        let connection =
            TrackedChildConnection::new(connection, Arc::clone(&self.active_child_scans))?;
        connection
            .connection()
            .pragma_update(None, "query_only", "ON")
            .map_err(sqlite_error::storage)?;
        #[cfg(test)]
        self.opened_shards
            .lock()
            .map_err(|error| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    format!("virtual-table shard diagnostics are poisoned: {error}"),
                )
            })?
            .push(shard_id);

        let cancellation_epoch = Arc::clone(&self.cancellation_epoch);
        #[cfg(test)]
        let mut child_scan_gate = self
            .child_scan_gate
            .lock()
            .map_err(|error| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    format!("child-scan test gate is poisoned: {error}"),
                )
            })?
            .take();
        #[cfg(test)]
        let progress_interval = if child_scan_gate.is_some() { 1 } else { 128 };
        #[cfg(not(test))]
        let progress_interval = 128;
        connection
            .connection()
            .progress_handler(
                progress_interval,
                Some(move || {
                    #[cfg(test)]
                    if let Some(gate) = child_scan_gate.take() {
                        if !gate.wait_for_release() {
                            return true;
                        }
                    }
                    cancellation_epoch.load(Ordering::Acquire) != scan_epoch
                }),
            )
            .map_err(sqlite_error::storage)?;

        let result = (|| {
            let select_sql = equality
                .and(spec.point_select_sql.as_deref())
                .unwrap_or(&spec.select_sql);
            let mut statement = connection
                .connection()
                .prepare(select_sql)
                .map_err(sqlite_error::statement)?;
            let mut sqlite_rows = match equality {
                Some(value) => statement.query([value]).map_err(sqlite_error::statement)?,
                None => statement.query([]).map_err(sqlite_error::statement)?,
            };
            let mut rows = Vec::new();
            let mut used_bytes = 0_usize;

            while let Some(row) = sqlite_rows.next().map_err(sqlite_error::statement)? {
                if self.cancelled(scan_epoch) {
                    return Err(cancelled_error());
                }
                if rows.len() == remaining_rows {
                    return Err(limit_error("row", self.limits.rows));
                }

                let mut row_bytes = spec
                    .column_count
                    .checked_mul(VALUE_ACCOUNTING_BYTES)
                    .and_then(|bytes| bytes.checked_add(ROW_ACCOUNTING_BYTES))
                    .ok_or_else(|| limit_error("byte", self.limits.bytes))?;
                if used_bytes
                    .checked_add(row_bytes)
                    .is_none_or(|bytes| bytes > remaining_bytes)
                {
                    return Err(limit_error("byte", self.limits.bytes));
                }
                let mut cells = Vec::new();
                cells
                    .try_reserve_exact(spec.column_count)
                    .map_err(allocation_error)?;
                for column in 0..spec.column_count {
                    let value = row.get_ref(column).map_err(sqlite_error::statement)?;
                    row_bytes = row_bytes
                        .checked_add(RawCell::accounted_payload_bytes(value))
                        .ok_or_else(|| limit_error("byte", self.limits.bytes))?;
                    let projected_bytes = used_bytes
                        .checked_add(row_bytes)
                        .ok_or_else(|| limit_error("byte", self.limits.bytes))?;
                    if projected_bytes > remaining_bytes {
                        return Err(limit_error("byte", self.limits.bytes));
                    }
                    cells.push(RawCell::try_copy_from(value)?);
                }
                used_bytes = used_bytes
                    .checked_add(row_bytes)
                    .filter(|bytes| *bytes <= remaining_bytes)
                    .ok_or_else(|| limit_error("byte", self.limits.bytes))?;
                rows.try_reserve(1).map_err(allocation_error)?;
                rows.push(cells);
            }
            #[cfg(test)]
            if let Some(gate) = self
                .child_scan_complete_gate
                .lock()
                .map_err(|error| {
                    EngineError::new(
                        EngineErrorKind::Internal,
                        format!("child-scan completion test gate is poisoned: {error}"),
                    )
                })?
                .take()
            {
                if !gate.wait_for_release() {
                    return Err(EngineError::new(
                        EngineErrorKind::Internal,
                        "child-scan completion test gate timed out or disconnected",
                    ));
                }
            }
            // The progress callback is deliberately coarse in production. A
            // short or empty query can complete without invoking it, so close
            // that cancellation window before publishing a successful batch.
            if self.cancelled(scan_epoch) {
                return Err(cancelled_error());
            }
            Ok((rows, used_bytes))
        })();

        self.storage.fail_closed_on_corruption(result)
    }
}

enum EqualityTargets {
    Empty,
    One(u16),
    All,
}

struct TrackedChildConnection {
    connection: Option<Connection>,
    active_child_scans: Arc<Mutex<usize>>,
}

impl TrackedChildConnection {
    fn new(connection: Connection, active_child_scans: Arc<Mutex<usize>>) -> EngineResult<Self> {
        {
            let mut active = active_child_scans.lock().map_err(|error| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    format!("active child-scan state is poisoned: {error}"),
                )
            })?;
            *active = active.checked_add(1).ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    "active child-scan count overflowed",
                )
            })?;
        }
        Ok(Self {
            connection: Some(connection),
            active_child_scans,
        })
    }

    fn connection(&self) -> &Connection {
        self.connection
            .as_ref()
            .expect("tracked child connection is live")
    }
}

impl Drop for TrackedChildConnection {
    fn drop(&mut self) {
        // Close the SQLite handle before publishing that the active child scan
        // has drained.
        drop(self.connection.take());
        let mut active = self
            .active_child_scans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(*active > 0);
        *active = active.saturating_sub(1);
    }
}

#[derive(Debug)]
struct TableSpec {
    id: u64,
    name: String,
    read_declared_schema: String,
    write_declared_schema: String,
    select_sql: String,
    point_select_sql: Option<String>,
    locator_select_sql: Option<String>,
    locator_point_select_sql: Option<String>,
    columns: Box<[PhysicalColumnSpec]>,
    locator: Option<PhysicalLocatorSpec>,
    global_unique_indexes: Box<[GlobalUniqueIndexSpec]>,
    write_unsupported: Option<String>,
    column_count: usize,
    targets: Box<[u16]>,
    shard_key: Option<ShardKeySpec>,
    generated_id_policy: GeneratedIdPolicy,
    generated_id_policy_active: bool,
    allocation_owners: Option<Arc<AllocationOwnerMap>>,
    generated_shard_cursor: AtomicU64,
}

#[derive(Debug, Clone)]
struct PhysicalColumnSpec {
    name: String,
    affinity: SqliteAffinity,
    collation: String,
    generated: bool,
}

#[derive(Debug, Clone)]
struct GlobalUniqueIndexSpec {
    metadata: GlobalIndexMetadata,
    evaluation_sql: String,
}

#[derive(Debug, Clone)]
enum PhysicalLocatorSpec {
    Rowid {
        expression: String,
    },
    PrimaryKey {
        columns: Box<[String]>,
        column_indices: Box<[usize]>,
    },
}

impl PhysicalLocatorSpec {
    fn expressions(&self) -> Box<[String]> {
        match self {
            Self::Rowid { expression } => vec![expression.clone()].into_boxed_slice(),
            Self::PrimaryKey { columns, .. } => columns.clone(),
        }
    }

    fn value_count(&self) -> usize {
        match self {
            Self::Rowid { .. } => 1,
            Self::PrimaryKey { columns, .. } => columns.len(),
        }
    }

    fn predicate_sql(&self) -> String {
        match self {
            Self::Rowid { expression } => format!("{expression} = ?"),
            Self::PrimaryKey { columns, .. } => columns
                .iter()
                .map(|column| format!("{} IS ?", quote_identifier(column)))
                .collect::<Vec<_>>()
                .join(" AND "),
        }
    }
}

#[derive(Debug, Clone)]
struct ShardKeySpec {
    column_index: c_int,
    key_type: ShardKeyType,
}

impl TableSpec {
    fn from_physical_table(
        connection: &Connection,
        table: &TableMetadata,
        global_indexes: &[GlobalIndexMetadata],
        generated_id_policy_active: bool,
        allocation_owners: Option<Arc<AllocationOwnerMap>>,
        targets: Box<[u16]>,
    ) -> EngineResult<Self> {
        let id = table.id().get();
        let name = table.name();
        let (strict, without_rowid) = connection
            .query_row(
                "SELECT strict, wr
                 FROM pragma_table_list
                 WHERE schema = 'main' AND name = ?1 COLLATE BINARY AND type = 'table'",
                [name],
                |row| Ok((row.get::<_, bool>(0)?, row.get::<_, bool>(1)?)),
            )
            .map_err(sqlite_error::storage)?;
        let mut statement = connection
            .prepare(
                "SELECT name, type, \"notnull\", dflt_value, pk, hidden
                 FROM pragma_table_xinfo(?1)
                 ORDER BY cid",
            )
            .map_err(sqlite_error::storage)?;
        let columns = statement
            .query_map([name], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })
            .map_err(sqlite_error::storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_error::storage)?;
        if columns.is_empty() {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("registered table {name} has no physical columns"),
            ));
        }
        if columns.iter().any(|(_, _, _, _, _, hidden)| *hidden == 1) {
            return Err(EngineError::new(
                EngineErrorKind::Unsupported,
                format!("registered table {name} has a hidden virtual-table column"),
            ));
        }

        let physical_columns = columns
            .iter()
            .map(|(column, declared_type, not_null, default_sql, _, _)| {
                let (_, collation, _, _, _) = connection
                    .column_metadata(None::<&str>, name, column)
                    .map_err(sqlite_error::storage)?;
                let collation = collation
                    .ok_or_else(|| {
                        EngineError::new(
                            EngineErrorKind::DataCorruption,
                            format!("registered column {name}.{column} has no collation metadata"),
                        )
                    })?
                    .to_str()
                    .map_err(|error| {
                        EngineError::from_source(
                            EngineErrorKind::DataCorruption,
                            format!(
                                "registered column {name}.{column} has invalid collation metadata"
                            ),
                            error,
                        )
                    })?;
                let affinity = if strict && declared_type.eq_ignore_ascii_case("ANY") {
                    // STRICT ANY preserves the incoming storage class. BLOB is
                    // SQLite's no-preference affinity in a virtual declaration.
                    SqliteAffinity::Blob
                } else {
                    sqlite_affinity(declared_type)
                };
                let mut declaration = format!(
                    "{} {} COLLATE {}",
                    quote_identifier(column),
                    affinity_name(affinity),
                    quote_identifier(collation)
                );
                if *not_null != 0 {
                    declaration.push_str(" NOT NULL");
                }
                if let Some(default_sql) = default_sql {
                    declaration.push_str(" DEFAULT (");
                    declaration.push_str(default_sql);
                    declaration.push(')');
                }
                Ok((
                    PhysicalColumnSpec {
                        name: column.clone(),
                        affinity,
                        collation: collation.to_owned(),
                        generated: false,
                    },
                    declaration,
                ))
            })
            .collect::<EngineResult<Vec<_>>>()?;
        let declared_columns = physical_columns
            .iter()
            .map(|(_, declaration)| declaration.as_str())
            .collect::<Vec<_>>();
        let read_declared_schema = format!("CREATE TABLE x({})", declared_columns.join(", "));
        let write_declared_schema = format!(
            "CREATE TABLE x({}, {} BLOB HIDDEN PRIMARY KEY NOT NULL) WITHOUT ROWID",
            declared_columns.join(", "),
            quote_identifier(LOCATOR_COLUMN_NAME)
        );
        let projected_columns = columns
            .iter()
            .map(|(column, _, _, _, _, _)| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");

        #[allow(unreachable_patterns)]
        let shard_key = match table.placement() {
            TablePlacement::Sharded(metadata) => {
                let column_index = columns
                    .iter()
                    .position(|(column, _, _, _, _, _)| column == metadata.column())
                    .ok_or_else(|| {
                        EngineError::new(
                            EngineErrorKind::DataCorruption,
                            format!(
                                "registered shard key {}.{} is absent from the physical schema",
                                name,
                                metadata.column()
                            ),
                        )
                    })?;
                let column_index = c_int::try_from(column_index).map_err(|error| {
                    EngineError::from_source(
                        EngineErrorKind::LimitExceeded,
                        format!("registered table {name} has too many columns"),
                        error,
                    )
                })?;
                Some(ShardKeySpec {
                    column_index,
                    key_type: metadata.key_type(),
                })
            }
            TablePlacement::Global | TablePlacement::Catalog => None,
            _ => {
                return Err(EngineError::new(
                    EngineErrorKind::Unsupported,
                    format!("registered table {name} has an unsupported placement"),
                ));
            }
        };
        let point_select_sql = match table.placement() {
            TablePlacement::Sharded(metadata) => Some(format!(
                "SELECT {projected_columns} FROM main.{} WHERE {} = ?1",
                quote_identifier(name),
                quote_identifier(metadata.column())
            )),
            _ => None,
        };

        let physical_columns = physical_columns
            .into_iter()
            .zip(columns.iter())
            .map(|((mut column, _), (_, _, _, _, _, hidden))| {
                column.generated = matches!(*hidden, 2 | 3);
                column
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let locator = if without_rowid {
            let mut primary_key = columns
                .iter()
                .filter(|(_, _, _, _, primary_key, _)| *primary_key > 0)
                .map(|(column, _, _, _, primary_key, _)| (*primary_key, column.clone()))
                .collect::<Vec<_>>();
            primary_key.sort_unstable_by_key(|(position, _)| *position);
            (!primary_key.is_empty()).then(|| {
                let primary_key = primary_key
                    .into_iter()
                    .map(|(_, column)| column)
                    .collect::<Vec<_>>();
                let column_indices = primary_key
                    .iter()
                    .map(|primary| {
                        physical_columns
                            .iter()
                            .position(|column| column.name == *primary)
                            .expect("primary-key column was discovered from this schema")
                    })
                    .collect::<Vec<_>>();
                PhysicalLocatorSpec::PrimaryKey {
                    columns: primary_key.into_boxed_slice(),
                    column_indices: column_indices.into_boxed_slice(),
                }
            })
        } else {
            let physical_names = columns
                .iter()
                .map(|(column, _, _, _, _, _)| column.as_str())
                .collect::<Vec<_>>();
            ["rowid", "_rowid_", "oid"]
                .into_iter()
                .find(|candidate| {
                    physical_names
                        .iter()
                        .all(|column| !column.eq_ignore_ascii_case(candidate))
                })
                .map(|expression| PhysicalLocatorSpec::Rowid {
                    expression: expression.to_owned(),
                })
        };

        let write_unsupported = if !matches!(table.placement(), TablePlacement::Sharded(_)) {
            Some(format!(
                "registered table {name} is not Sharded; the writable facade does not mutate replicated or catalog placement"
            ))
        } else if global_indexes.iter().any(|index| {
            index.table_id() == table.id()
                && index.is_unique()
                && index.lifecycle() == GlobalIndexLifecycle::Invalid
        }) {
            Some(format!(
                "registered table {name} has an invalid authoritative global index; rebuild it before writing"
            ))
        } else {
            shard::writable_table_unsupported_reason(connection, name)?
        };

        let global_unique_indexes = global_indexes
            .iter()
            .filter(|index| {
                index.table_id() == table.id()
                    && index.is_unique()
                    && index.lifecycle() == GlobalIndexLifecycle::Ready
            })
            .map(|metadata| GlobalUniqueIndexSpec {
                evaluation_sql: global_unique_evaluation_sql(metadata, &physical_columns),
                metadata: metadata.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let (locator_select_sql, locator_point_select_sql) = locator
            .as_ref()
            .map(|locator| {
                let locator_columns = locator.expressions().join(", ");
                let select = format!(
                    "SELECT {projected_columns}, {locator_columns} FROM main.{}",
                    quote_identifier(name)
                );
                let point = match table.placement() {
                    TablePlacement::Sharded(metadata) => Some(format!(
                        "SELECT {projected_columns}, {locator_columns} FROM main.{} WHERE {} = ?1",
                        quote_identifier(name),
                        quote_identifier(metadata.column())
                    )),
                    _ => None,
                };
                (select, point)
            })
            .map_or((None, None), |(select, point)| (Some(select), point));

        Ok(Self {
            id,
            name: name.to_owned(),
            read_declared_schema,
            write_declared_schema,
            select_sql: format!(
                "SELECT {projected_columns} FROM main.{}",
                quote_identifier(name)
            ),
            point_select_sql,
            locator_select_sql,
            locator_point_select_sql,
            columns: physical_columns,
            locator,
            global_unique_indexes,
            write_unsupported,
            column_count: columns.len(),
            targets,
            shard_key,
            generated_id_policy: table.generated_id_policy().clone(),
            generated_id_policy_active,
            allocation_owners,
            generated_shard_cursor: AtomicU64::new(0),
        })
    }

    fn create_virtual_table_sql(&self) -> String {
        format!(
            "CREATE VIRTUAL TABLE {} USING {MODULE_NAME}({})",
            quote_identifier(&self.name),
            self.id
        )
    }

    fn ensure_writable(&self) -> EngineResult<()> {
        if let Some(reason) = &self.write_unsupported {
            Err(EngineError::new(
                EngineErrorKind::Unsupported,
                reason.clone(),
            ))
        } else {
            Ok(())
        }
    }

    fn write_shard_key<'a>(&self, values: &'a [ValueRef<'a>]) -> EngineResult<ValueRef<'a>> {
        self.ensure_writable()?;
        if values.len() != self.column_count {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                format!(
                    "brisk_shard received {} values for {} physical columns on {}",
                    values.len(),
                    self.column_count,
                    self.name
                ),
            ));
        }
        let shard_key = self.shard_key.as_ref().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Unsupported,
                format!("registered table {} has no writable shard key", self.name),
            )
        })?;
        values
            .get(usize::try_from(shard_key.column_index).map_err(|_| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    format!(
                        "registered table {} has an invalid shard-key index",
                        self.name
                    ),
                )
            })?)
            .copied()
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    format!("registered table {} shard-key value is missing", self.name),
                )
            })
    }

    fn insert_sql(&self, conflict: ConflictMode) -> EngineResult<String> {
        self.ensure_writable()?;
        let columns = self
            .columns
            .iter()
            .map(|column| quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = (1..=self.column_count)
            .map(|position| format!("?{position}"))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "INSERT{} INTO main.{} ({columns}) VALUES ({placeholders})",
            write_conflict_clause(conflict),
            quote_identifier(&self.name)
        ))
    }

    fn generated_insert_sql_and_values(
        &self,
        values: &[ValueRef<'_>],
        conflict: ConflictMode,
    ) -> EngineResult<(String, Vec<RawCell>, String)> {
        self.ensure_writable()?;
        if values.len() != self.column_count {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                format!(
                    "brisk_shard received an invalid generated INSERT shape for {}",
                    self.name
                ),
            ));
        }
        let generated_column = self.generated_id_policy.column().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!("registered table {} has no generated-ID policy", self.name),
            )
        })?;
        let generated_index = self
            .columns
            .iter()
            .position(|column| column.name == generated_column)
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!(
                        "registered generated column {}.{} is absent from the physical schema",
                        self.name, generated_column
                    ),
                )
            })?;
        if !matches!(values[generated_index], ValueRef::Null) {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "generated INSERT for {} supplied an explicit {} value",
                    self.name, generated_column
                ),
            ));
        }

        let mut columns = Vec::with_capacity(self.column_count.saturating_sub(1));
        let mut parameters = Vec::with_capacity(self.column_count.saturating_sub(1));
        for (index, (column, value)) in self.columns.iter().zip(values).enumerate() {
            if index == generated_index {
                continue;
            }
            columns.push(quote_identifier(&column.name));
            parameters.push(RawCell::try_copy_from(*value)?);
        }
        let placeholders = (1..=parameters.len())
            .map(|position| format!("?{position}"))
            .collect::<Vec<_>>()
            .join(", ");
        let insert = if columns.is_empty() {
            format!(
                "INSERT{} INTO main.{} DEFAULT VALUES RETURNING {}",
                write_conflict_clause(conflict),
                quote_identifier(&self.name),
                quote_identifier(generated_column),
            )
        } else {
            format!(
                "INSERT{} INTO main.{} ({}) VALUES ({placeholders}) RETURNING {}",
                write_conflict_clause(conflict),
                quote_identifier(&self.name),
                columns.join(", "),
                quote_identifier(generated_column),
            )
        };
        Ok((insert, parameters, generated_column.to_owned()))
    }

    fn hilo_insert_sql_and_values(
        &self,
        values: &[ValueRef<'_>],
        conflict: ConflictMode,
        generated_id: i64,
    ) -> EngineResult<(String, Vec<RawCell>, String)> {
        self.ensure_writable()?;
        if values.len() != self.column_count {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                format!(
                    "brisk_shard received an invalid hilo_v1 INSERT shape for {}",
                    self.name
                ),
            ));
        }
        let GeneratedIdPolicy::HiloV1 {
            column: generated_column,
        } = &self.generated_id_policy
        else {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!("registered table {} does not use hilo_v1", self.name),
            ));
        };
        let generated_index = self
            .columns
            .iter()
            .position(|column| column.name == *generated_column)
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!(
                        "registered generated column {}.{} is absent from the physical schema",
                        self.name, generated_column
                    ),
                )
            })?;
        if !matches!(values[generated_index], ValueRef::Null) {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "hilo_v1 INSERT for {} unexpectedly supplied an explicit {} value",
                    self.name, generated_column
                ),
            ));
        }

        let columns = self
            .columns
            .iter()
            .map(|column| quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let mut parameters = Vec::with_capacity(self.column_count);
        for (index, value) in values.iter().enumerate() {
            parameters.push(if index == generated_index {
                RawCell::Integer(generated_id)
            } else {
                RawCell::try_copy_from(*value)?
            });
        }
        let placeholders = (1..=parameters.len())
            .map(|position| format!("?{position}"))
            .collect::<Vec<_>>()
            .join(", ");
        Ok((
            format!(
                "INSERT{} INTO main.{} ({columns}) VALUES ({placeholders}) RETURNING {}",
                write_conflict_clause(conflict),
                quote_identifier(&self.name),
                quote_identifier(generated_column),
            ),
            parameters,
            generated_column.to_owned(),
        ))
    }

    fn delete_sql(&self) -> EngineResult<String> {
        self.ensure_writable()?;
        let locator = self.locator.as_ref().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Unsupported,
                format!("registered table {} has no writable row locator", self.name),
            )
        })?;
        Ok(format!(
            "DELETE FROM main.{} WHERE {}",
            quote_identifier(&self.name),
            locator.predicate_sql()
        ))
    }

    fn update_sql_and_values(
        &self,
        values: &[ValueRef<'_>],
        no_change: &[bool],
        conflict: ConflictMode,
    ) -> EngineResult<(String, Vec<RawCell>)> {
        self.ensure_writable()?;
        if values.len() != self.column_count || no_change.len() != self.column_count {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                format!(
                    "brisk_shard received an invalid update shape for {}",
                    self.name
                ),
            ));
        }
        let mut assignments = Vec::new();
        let mut parameters = Vec::new();
        for ((column, value), unchanged) in self.columns.iter().zip(values).zip(no_change) {
            if *unchanged {
                continue;
            }
            if self.generated_id_policy.column() == Some(column.name.as_str()) {
                return Err(EngineError::new(
                    EngineErrorKind::ReadOnly,
                    format!(
                        "native generated identity {}.{} cannot be updated",
                        self.name, column.name
                    ),
                ));
            }
            if column.generated {
                return Err(EngineError::new(
                    EngineErrorKind::ReadOnly,
                    format!(
                        "generated column {}.{} is read-only through brisk_shard",
                        self.name, column.name
                    ),
                ));
            }
            parameters.push(RawCell::try_copy_from(*value)?);
            assignments.push(format!(
                "{} = ?{}",
                quote_identifier(&column.name),
                parameters.len()
            ));
        }
        if assignments.is_empty() {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                format!("brisk_shard update for {} changed no columns", self.name),
            ));
        }
        let locator = self.locator.as_ref().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Unsupported,
                format!("registered table {} has no writable row locator", self.name),
            )
        })?;
        Ok((
            format!(
                "UPDATE{} main.{} SET {} WHERE {}",
                write_conflict_clause(conflict),
                quote_identifier(&self.name),
                assignments.join(", "),
                locator.predicate_sql()
            ),
            parameters,
        ))
    }

    fn decode_locator(&self, value: ValueRef<'_>) -> EngineResult<locator::DecodedLocator> {
        self.ensure_writable()?;
        let ValueRef::Blob(bytes) = value else {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "brisk_shard row locator must be an opaque BLOB",
            ));
        };
        let decoded = locator::decode(self.id, bytes)?;
        let expected_values = self
            .locator
            .as_ref()
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::Unsupported,
                    format!("registered table {} has no writable row locator", self.name),
                )
            })?
            .value_count();
        if decoded.values.len() != expected_values || !self.targets.contains(&decoded.shard) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "brisk_shard row locator does not match the registered physical table",
            ));
        }
        Ok(decoded)
    }
}

const fn write_conflict_clause(conflict: ConflictMode) -> &'static str {
    if matches!(conflict, ConflictMode::Replace) {
        " OR REPLACE"
    } else {
        // The physical schema may declare its own ON CONFLICT policy. Force
        // each child operation to ABORT so the coordinator's conflict mode,
        // reported through sqlite3_vtab_on_conflict(), remains authoritative.
        " OR ABORT"
    }
}

const fn affinity_name(affinity: SqliteAffinity) -> &'static str {
    match affinity {
        SqliteAffinity::Integer => "INTEGER",
        SqliteAffinity::Text => "TEXT",
        SqliteAffinity::Blob => "BLOB",
        SqliteAffinity::Real => "REAL",
        SqliteAffinity::Numeric => "NUMERIC",
    }
}

fn global_unique_evaluation_sql(
    index: &GlobalIndexMetadata,
    columns: &[PhysicalColumnSpec],
) -> String {
    let input = columns
        .iter()
        .enumerate()
        .map(|(ordinal, column)| {
            let parameter = ordinal + 1;
            let value = if column.affinity == SqliteAffinity::Blob {
                format!("?{parameter}")
            } else {
                format!("CAST(?{parameter} AS {})", affinity_name(column.affinity))
            };
            format!(
                "({value} COLLATE {}) AS {}",
                quote_identifier(&column.collation),
                quote_identifier(&column.name)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let keys = index
        .key_parts()
        .iter()
        .map(|part| match part.source() {
            GlobalIndexKeySource::Column(column) => quote_identifier(column),
            GlobalIndexKeySource::Expression(expression) => format!("({expression})"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!("SELECT {keys} FROM (SELECT {input})");
    if let Some(predicate) = index.predicate() {
        sql.push_str(" WHERE (");
        sql.push_str(predicate);
        sql.push(')');
    }
    sql
}

#[derive(Default)]
struct LifecycleCounters {
    creates: AtomicUsize,
    connects: AtomicUsize,
    disconnects: AtomicUsize,
    destroys: AtomicUsize,
    opens: AtomicUsize,
    closes: AtomicUsize,
    filters: AtomicUsize,
    nexts: AtomicUsize,
    eofs: AtomicUsize,
    columns: AtomicUsize,
    rowids: AtomicUsize,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct LifecycleSnapshot {
    creates: usize,
    connects: usize,
    disconnects: usize,
    destroys: usize,
    opens: usize,
    closes: usize,
    filters: usize,
    nexts: usize,
    eofs: usize,
    columns: usize,
    rowids: usize,
}

#[cfg(test)]
impl LifecycleCounters {
    fn snapshot(&self) -> LifecycleSnapshot {
        LifecycleSnapshot {
            creates: self.creates.load(Ordering::Relaxed),
            connects: self.connects.load(Ordering::Relaxed),
            disconnects: self.disconnects.load(Ordering::Relaxed),
            destroys: self.destroys.load(Ordering::Relaxed),
            opens: self.opens.load(Ordering::Relaxed),
            closes: self.closes.load(Ordering::Relaxed),
            filters: self.filters.load(Ordering::Relaxed),
            nexts: self.nexts.load(Ordering::Relaxed),
            eofs: self.eofs.load(Ordering::Relaxed),
            columns: self.columns.load(Ordering::Relaxed),
            rowids: self.rowids.load(Ordering::Relaxed),
        }
    }
}

/// An instance of one catalog-authoritative logical table.
#[repr(C)]
struct BriskShardTable {
    // SQLite requires its base object to be the first field.
    _base: ffi::sqlite3_vtab,
    database_handle: *mut ffi::sqlite3,
    registry: Arc<Registry>,
    spec: Arc<TableSpec>,
}

unsafe impl<'vtab> VTab<'vtab> for BriskShardTable {
    type Aux = Arc<Registry>;
    type Cursor = BriskShardCursor;

    fn connect(
        database: &mut VTabConnection,
        auxiliary: Option<&Self::Aux>,
        args: &[&[u8]],
    ) -> SqliteResult<(String, Self)> {
        let registry = auxiliary
            .cloned()
            .ok_or_else(|| module_error("missing brisk_shard registry"))?;
        if args.len() != 4 {
            return Err(module_error(
                "brisk_shard requires exactly one catalog table ID",
            ));
        }
        let id = std::str::from_utf8(args[3])
            .map_err(|error| module_error(format!("invalid brisk_shard table ID: {error}")))?
            .parse::<u64>()
            .map_err(|error| module_error(format!("invalid brisk_shard table ID: {error}")))?;
        let spec = registry
            .table(id)
            .ok_or_else(|| module_error(format!("unknown brisk_shard table ID {id}")))?;

        database.config(VTabConfig::DirectOnly)?;
        if registry.mode == CoordinatorMode::Writable {
            // rusqlite's safe wrapper omits the required third vararg for
            // SQLITE_VTAB_CONSTRAINT_SUPPORT. Use stock SQLite's supported C
            // API directly so conflict modes can be delegated correctly.
            let result_code = unsafe {
                ffi::sqlite3_vtab_config(
                    database.handle(),
                    ffi::SQLITE_VTAB_CONSTRAINT_SUPPORT,
                    1_i32,
                )
            };
            if result_code != ffi::SQLITE_OK {
                return Err(SqliteError::SqliteFailure(
                    ffi::Error::new(result_code),
                    Some("could not enable brisk_shard constraint support".to_owned()),
                ));
            }
        }
        registry.lifecycle.connects.fetch_add(1, Ordering::Relaxed);
        let database_handle = unsafe { database.handle() };
        Ok((
            match registry.mode {
                CoordinatorMode::ReadOnly => spec.read_declared_schema.clone(),
                CoordinatorMode::Writable => spec.write_declared_schema.clone(),
            },
            Self {
                _base: ffi::sqlite3_vtab::default(),
                database_handle,
                registry,
                spec,
            },
        ))
    }

    fn best_index(&self, information: &mut IndexInfo) -> SqliteResult<()> {
        let mut shard_key_equality = None;
        if let Some(shard_key) = &self.spec.shard_key {
            for (index, constraint) in information.constraints().enumerate() {
                if constraint.is_usable()
                    && constraint.column() == shard_key.column_index
                    && constraint.operator() == IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ
                    && !information.is_in_constraint(index)?
                    && (shard_key.key_type != ShardKeyType::Text
                        || information.collation(index)?.eq_ignore_ascii_case("binary"))
                {
                    shard_key_equality = Some(index);
                    break;
                }
            }
        }

        if let Some(index) = shard_key_equality {
            let mut usage = information.constraint_usage(index);
            usage.set_argv_index(1);
            // The child receives the same equality, but the coordinator keeps
            // SQLite's outer comparison as the final affinity/collation check.
            usage.set_omit(false);
            information.set_idx_num(SHARD_KEY_EQUALITY_PLAN);
            information.set_estimated_cost(10.0);
            information.set_estimated_rows(100);
        } else {
            information.set_idx_num(SCAN_PLAN);
            information.set_estimated_cost(1_000_000.0);
            information.set_estimated_rows(1_000_000);
        }
        Ok(())
    }

    fn open(&'vtab mut self) -> SqliteResult<Self::Cursor> {
        self.registry
            .lifecycle
            .opens
            .fetch_add(1, Ordering::Relaxed);
        Ok(BriskShardCursor::new(
            Arc::clone(&self.registry),
            Arc::clone(&self.spec),
        ))
    }
}

impl<'vtab> CreateVTab<'vtab> for BriskShardTable {
    const KIND: VTabKind = VTabKind::Default;

    fn create(
        database: &mut VTabConnection,
        auxiliary: Option<&Self::Aux>,
        args: &[&[u8]],
    ) -> SqliteResult<(String, Self)> {
        let lifecycle = auxiliary
            .map(|registry| Arc::clone(&registry.lifecycle))
            .ok_or_else(|| module_error("missing brisk_shard registry"))?;
        lifecycle.creates.fetch_add(1, Ordering::Relaxed);
        Self::connect(database, auxiliary, args)
    }

    fn destroy(&self) -> SqliteResult<()> {
        self.registry
            .lifecycle
            .destroys
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

impl<'vtab> UpdateVTab<'vtab> for BriskShardTable {
    fn delete(&mut self, locator: ValueRef<'_>) -> SqliteResult<()> {
        self.registry
            .write_state()
            .execute_delete(&self.registry, &self.spec, locator)
            .map_err(vtab_error)
    }

    fn insert(&mut self, arguments: &Inserts<'_>) -> SqliteResult<i64> {
        let expected = self
            .spec
            .column_count
            .checked_add(3)
            .ok_or_else(|| module_error("brisk_shard writable column count overflowed"))?;
        let values = arguments.iter().collect::<Vec<_>>();
        if values.len() != expected || !matches!(values[0], ValueRef::Null) {
            return Err(module_error(format!(
                "brisk_shard INSERT received an invalid argument shape for {}",
                self.spec.name
            )));
        }
        if !matches!(values[1], ValueRef::Null)
            || !matches!(values[2 + self.spec.column_count], ValueRef::Null)
        {
            return Err(vtab_error(EngineError::new(
                EngineErrorKind::ReadOnly,
                "brisk_shard opaque row locator cannot be supplied by INSERT",
            )));
        }
        let conflict = unsafe { arguments.on_conflict(self.database_handle) };
        self.registry
            .write_state()
            .execute_insert(
                &self.registry,
                &self.spec,
                &values[2..2 + self.spec.column_count],
                conflict,
            )
            .map_err(vtab_error)
    }

    fn update(&mut self, arguments: &Updates<'_>) -> SqliteResult<()> {
        let expected = self
            .spec
            .column_count
            .checked_add(3)
            .ok_or_else(|| module_error("brisk_shard writable column count overflowed"))?;
        let values = arguments.iter().collect::<Vec<_>>();
        if values.len() != expected || matches!(values[0], ValueRef::Null) {
            return Err(module_error(format!(
                "brisk_shard UPDATE received an invalid argument shape for {}",
                self.spec.name
            )));
        }
        let hidden_value = values[2 + self.spec.column_count];
        if !matches!(
            (values[0], values[1], hidden_value),
            (ValueRef::Blob(old), ValueRef::Blob(new), ValueRef::Blob(hidden))
                if old == new && old == hidden
        ) {
            return Err(vtab_error(EngineError::new(
                EngineErrorKind::ReadOnly,
                "brisk_shard opaque row locator cannot be changed",
            )));
        }
        let no_change = (0..self.spec.column_count)
            .map(|column| arguments.no_change(column + 2))
            .collect::<Vec<_>>();
        let conflict = unsafe { arguments.on_conflict(self.database_handle) };
        self.registry
            .write_state()
            .execute_update(
                &self.registry,
                &self.spec,
                values[0],
                values[1],
                &values[2..2 + self.spec.column_count],
                &no_change,
                conflict,
            )
            .map_err(vtab_error)
    }
}

impl<'vtab> TransactionVTab<'vtab> for BriskShardTable {
    fn begin(&mut self) -> SqliteResult<()> {
        write::map_callback(self.registry.write_state().begin(&self.registry))
    }

    fn sync(&mut self) -> SqliteResult<()> {
        write::map_callback(self.registry.write_state().sync(&self.registry))
    }

    fn commit(&mut self) -> SqliteResult<()> {
        write::map_callback(self.registry.write_state().mark_commit(&self.registry))
    }

    fn rollback(&mut self) -> SqliteResult<()> {
        self.registry.write_state().mark_rollback();
        Ok(())
    }
}

impl BriskShardTable {
    fn savepoint(&mut self, number: c_int) -> SqliteResult<()> {
        write::map_callback(
            self.registry
                .write_state()
                .savepoint(&self.registry, number),
        )
    }

    fn release(&mut self, number: c_int) -> SqliteResult<()> {
        write::map_callback(self.registry.write_state().release(&self.registry, number))
    }

    fn rollback_to(&mut self, number: c_int) -> SqliteResult<()> {
        write::map_callback(
            self.registry
                .write_state()
                .rollback_to(&self.registry, number),
        )
    }
}

impl Drop for BriskShardTable {
    fn drop(&mut self) {
        self.registry
            .lifecycle
            .disconnects
            .fetch_add(1, Ordering::Relaxed);
    }
}

#[repr(C)]
struct BriskShardCursor {
    // SQLite requires its cursor base object to be the first field.
    _base: ffi::sqlite3_vtab_cursor,
    registry: Arc<Registry>,
    spec: Arc<TableSpec>,
    operation: Option<SchemaOperationGuard>,
    rows: Vec<Vec<RawCell>>,
    equality: Option<RawCell>,
    targets: Box<[u16]>,
    row_index: usize,
    next_target: usize,
    row_id: i64,
    scan_epoch: u64,
    total_rows: usize,
    total_bytes: usize,
    eof: bool,
}

impl BriskShardCursor {
    fn new(registry: Arc<Registry>, spec: Arc<TableSpec>) -> Self {
        Self {
            _base: ffi::sqlite3_vtab_cursor::default(),
            registry,
            spec,
            operation: None,
            rows: Vec::new(),
            equality: None,
            targets: Box::default(),
            row_index: 0,
            next_target: 0,
            row_id: 0,
            scan_epoch: 0,
            total_rows: 0,
            total_bytes: 0,
            eof: true,
        }
    }

    fn begin_scan(&mut self) -> SqliteResult<()> {
        if self.registry.mode == CoordinatorMode::Writable {
            self.spec.ensure_writable().map_err(vtab_error)?;
            if self.targets.len() != 1 {
                return Err(vtab_error(EngineError::new(
                    EngineErrorKind::Unsupported,
                    "writable brisk_shard UPDATE and DELETE require an exact shard-key equality",
                )));
            }
        }
        let operation = if self.registry.has_retained_schema_admission() {
            None
        } else {
            Some(
                self.registry
                    .storage
                    .enter_schema_operation()
                    .map_err(vtab_error)?,
            )
        };
        if self.registry.storage.current_schema_generation() != self.registry.schema_generation {
            return Err(module_error(
                "brisk_shard coordinator schema is stale; reopen the coordinator",
            ));
        }
        self.operation = operation;
        self.scan_epoch = self.registry.cancellation_epoch.load(Ordering::Acquire);
        if self.registry.cancelled(self.scan_epoch) {
            return Err(vtab_error(cancelled_error()));
        }
        self.advance_to_nonempty_shard()
    }

    fn advance_to_nonempty_shard(&mut self) -> SqliteResult<()> {
        // Drop the exhausted shard batch before allocating the next one.
        self.rows = Vec::new();
        self.row_index = 0;
        while let Some(&shard_id) = self.targets.get(self.next_target) {
            self.next_target += 1;
            let remaining_rows = self.registry.limits.rows.saturating_sub(self.total_rows);
            let remaining_bytes = self.registry.limits.bytes.saturating_sub(self.total_bytes);
            let (rows, used_bytes) = self
                .registry
                .read_shard_rows(
                    &self.spec,
                    shard_id,
                    self.equality.as_ref(),
                    self.scan_epoch,
                    remaining_rows,
                    remaining_bytes,
                )
                .map_err(vtab_error)?;
            self.total_rows += rows.len();
            self.total_bytes += used_bytes;
            if !rows.is_empty() {
                self.rows = rows;
                self.row_index = 0;
                self.eof = false;
                return Ok(());
            }
        }

        self.finish();
        Ok(())
    }

    fn finish(&mut self) {
        self.rows = Vec::new();
        self.equality = None;
        self.targets = Box::default();
        self.row_index = 0;
        self.eof = true;
        self.operation = None;
    }

    fn fail(&mut self, error: SqliteError) -> SqliteError {
        self.finish();
        error
    }
}

unsafe impl VTabCursor for BriskShardCursor {
    fn filter(
        &mut self,
        index_number: c_int,
        _index_string: Option<&str>,
        arguments: &Filters<'_>,
    ) -> SqliteResult<()> {
        self.registry
            .lifecycle
            .filters
            .fetch_add(1, Ordering::Relaxed);
        self.finish();
        self.next_target = 0;
        self.row_id = 1;
        self.total_rows = 0;
        self.total_bytes = 0;
        match index_number {
            SCAN_PLAN => {
                if !arguments.is_empty() {
                    return Err(module_error(
                        "brisk_shard scan plan received unexpected filter arguments",
                    ));
                }
                self.targets = self.spec.targets.clone();
            }
            SHARD_KEY_EQUALITY_PLAN => {
                let mut values = arguments.iter();
                let value = values.next().ok_or_else(|| {
                    module_error("brisk_shard equality plan is missing its filter argument")
                })?;
                if values.next().is_some() {
                    return Err(module_error(
                        "brisk_shard equality plan received too many filter arguments",
                    ));
                }
                let (equality, targets, equality_bytes) = self
                    .registry
                    .equality_scan(&self.spec, value)
                    .map_err(vtab_error)?;
                self.equality = Some(equality);
                self.targets = targets;
                self.total_bytes = equality_bytes;
            }
            other => {
                return Err(module_error(format!(
                    "unknown brisk_shard filter plan {other}"
                )));
            }
        }
        self.begin_scan().map_err(|error| self.fail(error))
    }

    fn next(&mut self) -> SqliteResult<()> {
        self.registry
            .lifecycle
            .nexts
            .fetch_add(1, Ordering::Relaxed);
        if self.eof {
            return Ok(());
        }
        if self.registry.cancelled(self.scan_epoch) {
            return Err(self.fail(vtab_error(cancelled_error())));
        }
        self.row_id = self
            .row_id
            .checked_add(1)
            .ok_or_else(|| module_error("brisk_shard synthetic rowid overflow"))?;
        if self.row_index + 1 < self.rows.len() {
            self.row_index += 1;
            return Ok(());
        }
        self.advance_to_nonempty_shard()
            .map_err(|error| self.fail(error))
    }

    fn eof(&self) -> bool {
        self.registry.lifecycle.eofs.fetch_add(1, Ordering::Relaxed);
        self.eof
    }

    fn column(&self, context: &mut Context, column: c_int) -> SqliteResult<()> {
        self.registry
            .lifecycle
            .columns
            .fetch_add(1, Ordering::Relaxed);
        let column = usize::try_from(column)
            .map_err(|_| module_error("negative brisk_shard column index"))?;
        if self.registry.mode == CoordinatorMode::Writable
            && column < self.spec.column_count
            && context.no_change()
        {
            // Leaving the result unset is the signal that lets SQLite mark
            // this argv slot with sqlite3_value_nochange() for xUpdate.
            return Ok(());
        }
        let value = self
            .rows
            .get(self.row_index)
            .and_then(|row| row.get(column))
            .ok_or_else(|| module_error(format!("brisk_shard column {column} is out of bounds")))?;
        context.set_result(value)
    }

    fn rowid(&self) -> SqliteResult<i64> {
        self.registry
            .lifecycle
            .rowids
            .fetch_add(1, Ordering::Relaxed);
        if self.eof {
            return Err(module_error("brisk_shard rowid requested at EOF"));
        }
        Ok(self.row_id)
    }
}

impl Drop for BriskShardCursor {
    fn drop(&mut self) {
        self.registry
            .lifecycle
            .closes
            .fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
enum RawCell {
    Null,
    Integer(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl RawCell {
    fn try_copy_from(value: ValueRef<'_>) -> EngineResult<Self> {
        Ok(match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(value) => Self::Integer(value),
            ValueRef::Real(value) => Self::Real(value),
            ValueRef::Text(value) => Self::Text(copy_bytes(value)?),
            ValueRef::Blob(value) => Self::Blob(copy_bytes(value)?),
        })
    }

    fn accounted_payload_bytes(value: ValueRef<'_>) -> usize {
        match value {
            ValueRef::Null => 0,
            ValueRef::Integer(_) | ValueRef::Real(_) => size_of::<u64>(),
            ValueRef::Text(value) | ValueRef::Blob(value) => {
                value.len().saturating_add(ALLOCATION_OVERHEAD_BYTES)
            }
        }
    }

    fn as_value_ref(&self) -> ValueRef<'_> {
        match self {
            Self::Null => ValueRef::Null,
            Self::Integer(value) => ValueRef::Integer(*value),
            Self::Real(value) => ValueRef::Real(*value),
            Self::Text(value) => ValueRef::Text(value),
            Self::Blob(value) => ValueRef::Blob(value),
        }
    }
}

fn copy_bytes(bytes: &[u8]) -> EngineResult<Vec<u8>> {
    let mut copy = Vec::new();
    copy.try_reserve_exact(bytes.len())
        .map_err(allocation_error)?;
    copy.extend_from_slice(bytes);
    Ok(copy)
}

fn allocation_error(error: std::collections::TryReserveError) -> EngineError {
    EngineError::from_source(
        EngineErrorKind::OutOfMemory,
        "brisk_shard could not reserve bounded result memory",
        error,
    )
}

impl ToSql for RawCell {
    fn to_sql(&self) -> SqliteResult<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(self.as_value_ref()))
    }
}

fn module_error(message: impl Into<String>) -> SqliteError {
    SqliteError::ModuleError(message.into())
}

fn vtab_error(error: EngineError) -> SqliteError {
    let result_code = match error.kind() {
        EngineErrorKind::NumericOutOfRange => Some(ffi::SQLITE_RANGE),
        EngineErrorKind::InvalidTextEncoding | EngineErrorKind::TypeMismatch => {
            Some(ffi::SQLITE_MISMATCH)
        }
        EngineErrorKind::ConstraintViolation => Some(ffi::SQLITE_CONSTRAINT),
        EngineErrorKind::UniqueViolation => Some(ffi::SQLITE_CONSTRAINT_UNIQUE),
        EngineErrorKind::NotNullViolation => Some(ffi::SQLITE_CONSTRAINT_NOTNULL),
        EngineErrorKind::ForeignKeyViolation => Some(ffi::SQLITE_CONSTRAINT_FOREIGNKEY),
        EngineErrorKind::CheckViolation => Some(ffi::SQLITE_CONSTRAINT_CHECK),
        EngineErrorKind::PermissionDenied => Some(ffi::SQLITE_AUTH),
        EngineErrorKind::ReadOnly => Some(ffi::SQLITE_READONLY),
        EngineErrorKind::Busy => Some(ffi::SQLITE_BUSY),
        EngineErrorKind::Cancelled | EngineErrorKind::DeadlineExceeded => {
            Some(ffi::SQLITE_INTERRUPT)
        }
        EngineErrorKind::LimitExceeded => Some(ffi::SQLITE_TOOBIG),
        EngineErrorKind::Unsupported if write::is_global_index_write_unsupported(&error) => {
            Some(ffi::SQLITE_NOLFS)
        }
        EngineErrorKind::ShuttingDown => Some(ffi::SQLITE_ABORT),
        EngineErrorKind::StorageFull => Some(ffi::SQLITE_FULL),
        EngineErrorKind::OutOfMemory => Some(ffi::SQLITE_NOMEM),
        EngineErrorKind::StorageUnavailable => Some(ffi::SQLITE_IOERR),
        EngineErrorKind::DataCorruption => Some(ffi::SQLITE_CORRUPT),
        EngineErrorKind::Internal => Some(ffi::SQLITE_INTERNAL),
        _ => None,
    };
    result_code.map_or_else(
        || module_error(error.to_string()),
        |result_code| {
            SqliteError::SqliteFailure(ffi::Error::new(result_code), Some(error.to_string()))
        },
    )
}

fn cancelled_error() -> EngineError {
    EngineError::new(EngineErrorKind::Cancelled, "brisk_shard scan was cancelled")
}

fn limit_error(resource: &str, limit: usize) -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        format!("brisk_shard {resource} materialization exceeds its {limit} limit"),
    )
}

#[cfg(test)]
const TEST_SYNC_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct TestChildScanGate {
    started: mpsc::SyncSender<()>,
    release: mpsc::Receiver<()>,
}

#[cfg(test)]
impl TestChildScanGate {
    fn channel() -> (Self, TestChildScanControl) {
        let (started_sender, started) = mpsc::sync_channel(1);
        let (release_sender, release) = mpsc::sync_channel(1);
        (
            Self {
                started: started_sender,
                release,
            },
            TestChildScanControl {
                started,
                release: TestRelease::new(release_sender),
            },
        )
    }

    fn wait_for_release(self) -> bool {
        self.started.send(()).is_ok() && self.release.recv_timeout(TEST_SYNC_TIMEOUT).is_ok()
    }
}

#[cfg(test)]
pub(crate) struct TestChildScanControl {
    started: mpsc::Receiver<()>,
    release: TestRelease,
}

#[cfg(test)]
impl TestChildScanControl {
    pub(crate) fn wait_until_started(&self) {
        self.started
            .recv_timeout(TEST_SYNC_TIMEOUT)
            .expect("child scan did not reach its test gate before the timeout");
    }

    pub(crate) fn release(&mut self) {
        self.release.signal();
    }
}

#[cfg(test)]
struct TestRelease {
    sender: Option<mpsc::SyncSender<()>>,
}

#[cfg(test)]
impl TestRelease {
    const fn new(sender: mpsc::SyncSender<()>) -> Self {
        Self {
            sender: Some(sender),
        }
    }

    fn signal(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(());
        }
    }
}

#[cfg(test)]
impl Drop for TestRelease {
    fn drop(&mut self) {
        self.signal();
    }
}

#[cfg(test)]
mod benchmarks;

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{Arc, mpsc},
        thread,
        time::Instant,
    };

    use proptest::prelude::*;
    use rusqlite::{ErrorCode, MAIN_DB, params, types::ValueRef};

    use super::*;
    use crate::core::{
        Database, Engine, GeneratedIdPolicy, ShardKeyMetadata, ShardKeyType, Statement,
        TableDeclaration, Value,
        generated_id::{AllocationOwnerSlot, NativeRangeV1Id, native_range_v1_sequence_floor},
    };

    struct ReapedTestChild {
        child: Option<std::process::Child>,
    }

    impl ReapedTestChild {
        const fn new(child: std::process::Child) -> Self {
            Self { child: Some(child) }
        }

        fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
            self.child
                .as_mut()
                .expect("test child was already reaped")
                .try_wait()
        }

        fn terminate_with_output(&mut self) -> std::io::Result<std::process::Output> {
            let mut child = self.child.take().expect("test child was already reaped");
            let status_error = match child.try_wait() {
                Ok(Some(_)) => None,
                Ok(None) => child.kill().err(),
                Err(error) => {
                    let _ = child.kill();
                    Some(error)
                }
            };
            let output = child.wait_with_output();
            match (output, status_error) {
                (Err(wait_error), _) => Err(wait_error),
                (Ok(_), Some(status_error)) => Err(status_error),
                (Ok(output), None) => Ok(output),
            }
        }
    }

    impl Drop for ReapedTestChild {
        fn drop(&mut self) {
            if let Some(mut child) = self.child.take() {
                if child.try_wait().ok().flatten().is_none() {
                    let _ = child.kill();
                }
                let _ = child.wait();
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum ParityCell {
        Null,
        Integer(i64),
        Real(u64),
        Text(Vec<u8>),
        Blob(Vec<u8>),
    }

    fn sqlite_parity_cell(value: ValueRef<'_>) -> ParityCell {
        match value {
            ValueRef::Null => ParityCell::Null,
            ValueRef::Integer(value) => ParityCell::Integer(value),
            ValueRef::Real(value) => ParityCell::Real(value.to_bits()),
            ValueRef::Text(value) => ParityCell::Text(value.to_vec()),
            ValueRef::Blob(value) => ParityCell::Blob(value.to_vec()),
        }
    }

    fn engine_parity_cell(value: &Value) -> ParityCell {
        match value {
            Value::Null => ParityCell::Null,
            Value::Int64(value) => ParityCell::Integer(*value),
            Value::Float64(value) => ParityCell::Real(value.to_bits()),
            Value::Text(value) => ParityCell::Text(value.as_bytes().to_vec()),
            Value::InvalidText(value) => ParityCell::Text(value.clone()),
            Value::Binary(value) => ParityCell::Blob(value.clone()),
            value => panic!("unexpected Engine value in SQLite parity fixture: {value:?}"),
        }
    }

    fn sort_parity_rows(rows: &mut [Vec<ParityCell>]) {
        rows.sort_unstable_by_key(|row| match row.first() {
            Some(ParityCell::Integer(value)) => *value,
            value => panic!("parity row has an invalid tenant key: {value:?}"),
        });
    }

    struct Fixture {
        temp: tempfile::TempDir,
        storage: Storage,
        keys: [i64; 2],
    }

    struct WritableFixture {
        temp: tempfile::TempDir,
        storage: Storage,
        keys: [i64; 2],
    }

    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let mut storage = Storage::open(temp.path(), 2).unwrap();
            let mut migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            storage
                .apply_schema_migration(
                    "CREATE TABLE events (
                        tenant_id INTEGER NOT NULL,
                        event_id INTEGER NOT NULL,
                        payload TEXT NOT NULL,
                        amount REAL,
                        raw BLOB,
                        optional TEXT,
                        category TEXT COLLATE NOCASE NOT NULL,
                        PRIMARY KEY (tenant_id, event_id)
                     );
                     CREATE TABLE countries (
                        code TEXT PRIMARY KEY,
                        label TEXT NOT NULL
                     );",
                    &mut migration,
                    None,
                )
                .unwrap();
            migration.publish_ready().unwrap();

            let database_id = storage.logical_catalog().default_database().id();
            storage
                .register_tables(vec![
                    TableDeclaration::sharded(
                        database_id,
                        "events",
                        ShardKeyMetadata::new("tenant_id", ShardKeyType::Int64).unwrap(),
                    )
                    .unwrap(),
                    TableDeclaration::global(database_id, "countries").unwrap(),
                    TableDeclaration::catalog(database_id, "internal_catalog").unwrap(),
                ])
                .unwrap();

            let keys = std::array::from_fn(|expected| {
                (1_i64..)
                    .find(|key| {
                        storage.shard_for_key(key.to_string().as_bytes()) == expected as u16
                    })
                    .unwrap()
            });
            storage
                .open_shard(0)
                .unwrap()
                .execute(
                    "INSERT INTO events
                     VALUES (?1, 1, 'zero', 1.5, x'0001', CAST(x'80FF' AS TEXT), 'Zulu')",
                    [keys[0]],
                )
                .unwrap();
            storage
                .open_shard(1)
                .unwrap()
                .execute(
                    "INSERT INTO events
                     VALUES (?1, 1, 'one', 2.5, x'FEFF', NULL, 'alpha')",
                    [keys[1]],
                )
                .unwrap();
            for shard in 0..2 {
                storage
                    .open_shard(shard)
                    .unwrap()
                    .execute("INSERT INTO countries VALUES ('US', 'United States')", [])
                    .unwrap();
            }

            Self {
                temp,
                storage,
                keys,
            }
        }

        fn physical_row_count(&self) -> i64 {
            (0..2)
                .map(|shard| {
                    self.storage
                        .open_shard(shard)
                        .unwrap()
                        .query_row("SELECT COUNT(*) FROM events", [], |row| {
                            row.get::<_, i64>(0)
                        })
                        .unwrap()
                })
                .sum()
        }
    }

    impl WritableFixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let mut storage = Storage::open(temp.path(), 2).unwrap();
            let mut migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            storage
                .apply_schema_migration(
                    "CREATE TABLE parents (
                         tenant_id INTEGER NOT NULL,
                         parent_id INTEGER NOT NULL,
                         label TEXT NOT NULL,
                         PRIMARY KEY (tenant_id, parent_id)
                     );
                     CREATE TABLE items (
                         tenant_id INTEGER NOT NULL,
                         item_id INTEGER NOT NULL,
                         parent_id INTEGER NOT NULL,
                         code TEXT NOT NULL,
                         quantity INTEGER NOT NULL CHECK (quantity > 0),
                         PRIMARY KEY (tenant_id, item_id),
                         UNIQUE (tenant_id, code),
                         FOREIGN KEY (tenant_id, parent_id)
                             REFERENCES parents (tenant_id, parent_id)
                     );
                     CREATE INDEX items_quantity_idx
                         ON items (tenant_id, quantity);",
                    &mut migration,
                    None,
                )
                .unwrap();
            migration.publish_ready().unwrap();
            let database_id = storage.logical_catalog().default_database().id();
            storage
                .register_tables(vec![
                    TableDeclaration::sharded(
                        database_id,
                        "parents",
                        ShardKeyMetadata::new("tenant_id", ShardKeyType::Int64).unwrap(),
                    )
                    .unwrap(),
                    TableDeclaration::sharded(
                        database_id,
                        "items",
                        ShardKeyMetadata::new("tenant_id", ShardKeyType::Int64).unwrap(),
                    )
                    .unwrap(),
                ])
                .unwrap();
            let keys = std::array::from_fn(|expected| {
                (1_i64..)
                    .find(|key| {
                        storage.shard_for_key(key.to_string().as_bytes()) == expected as u16
                    })
                    .unwrap()
            });
            for (shard, key) in keys.into_iter().enumerate() {
                storage
                    .open_shard(u16::try_from(shard).unwrap())
                    .unwrap()
                    .execute(
                        "INSERT INTO parents VALUES (?1, 1, ?2)",
                        params![key, format!("parent-{shard}")],
                    )
                    .unwrap();
            }
            Self {
                temp,
                storage,
                keys,
            }
        }

        fn item_count(&self) -> i64 {
            (0..2)
                .map(|shard| {
                    self.storage
                        .open_shard(shard)
                        .unwrap()
                        .query_row("SELECT COUNT(*) FROM items", [], |row| row.get::<_, i64>(0))
                        .unwrap()
                })
                .sum()
        }
    }

    struct TypedRoutingFixture {
        _temp: tempfile::TempDir,
        storage: Storage,
        int_keys: Vec<i64>,
        text_keys: Vec<String>,
        blob_keys: Vec<Vec<u8>>,
        native_ids: Vec<i64>,
    }

    struct HiloFixture {
        _temp: tempfile::TempDir,
        storage: Storage,
        table_id: u64,
    }

    impl HiloFixture {
        fn new(shard_count: u16) -> Self {
            let temp = tempfile::tempdir().unwrap();
            let mut storage = Storage::open(temp.path(), shard_count).unwrap();
            let mut migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            storage
                .apply_schema_migration(
                    "CREATE TABLE hilo_events (
                         id INTEGER PRIMARY KEY,
                         payload TEXT NOT NULL CHECK (payload <> 'reject')
                     ) STRICT;",
                    &mut migration,
                    None,
                )
                .unwrap();
            migration.publish_ready().unwrap();
            let database_id = storage.logical_catalog().default_database().id();
            storage
                .register_tables(vec![
                    TableDeclaration::sharded(
                        database_id,
                        "hilo_events",
                        ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
                    )
                    .unwrap()
                    .with_generated_id_policy(GeneratedIdPolicy::hilo_v1("id").unwrap())
                    .unwrap(),
                ])
                .unwrap();
            let table_id = storage
                .logical_catalog()
                .table("default", "hilo_events")
                .unwrap()
                .unwrap()
                .id()
                .get();
            Self {
                _temp: temp,
                storage,
                table_id,
            }
        }

        fn row_count(&self) -> i64 {
            (0..self.storage.shard_count())
                .map(|shard| {
                    self.storage
                        .open_shard(shard)
                        .unwrap()
                        .query_row("SELECT COUNT(*) FROM hilo_events", [], |row| {
                            row.get::<_, i64>(0)
                        })
                        .unwrap()
                })
                .sum()
        }
    }

    struct ScaleFixture {
        temp: tempfile::TempDir,
        storage: Storage,
        keys: Vec<i64>,
    }

    impl ScaleFixture {
        fn new(shard_count: u16) -> Self {
            let temp = tempfile::tempdir().unwrap();
            let mut storage = Storage::open(temp.path(), shard_count).unwrap();
            let mut migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            storage
                .apply_schema_migration(
                    "CREATE TABLE scale_events (
                         id INTEGER PRIMARY KEY,
                         payload TEXT NOT NULL
                     ) STRICT;",
                    &mut migration,
                    None,
                )
                .unwrap();
            migration.publish_ready().unwrap();
            let database_id = storage.logical_catalog().default_database().id();
            storage
                .register_tables(vec![
                    TableDeclaration::sharded(
                        database_id,
                        "scale_events",
                        ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
                    )
                    .unwrap(),
                ])
                .unwrap();
            let keys = keys_for_shards(&storage, shard_count, |candidate| {
                candidate.to_string().into_bytes()
            });
            for shard in 0..shard_count {
                storage
                    .open_shard(shard)
                    .unwrap()
                    .execute(
                        "INSERT INTO scale_events VALUES (?1, ?2)",
                        params![keys[usize::from(shard)], format!("shard-{shard}")],
                    )
                    .unwrap();
            }
            Self {
                temp,
                storage,
                keys,
            }
        }

        fn physical_rows(&self) -> Vec<(i64, String)> {
            let mut rows = Vec::new();
            for shard in 0..self.storage.shard_count() {
                let connection = self.storage.open_shard(shard).unwrap();
                let mut statement = connection
                    .prepare("SELECT id, payload FROM scale_events")
                    .unwrap();
                rows.extend(
                    statement
                        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                        .unwrap()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap(),
                );
            }
            rows.sort_unstable();
            rows
        }
    }

    impl TypedRoutingFixture {
        fn new(shard_count: u16) -> Self {
            let temp = tempfile::tempdir().unwrap();
            let mut storage = Storage::open(temp.path(), shard_count).unwrap();
            let mut migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            storage
                .apply_schema_migration(
                    "CREATE TABLE int_events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
                     CREATE TABLE text_events (
                         id TEXT PRIMARY KEY COLLATE BINARY,
                         payload TEXT NOT NULL
                     ) WITHOUT ROWID;
                     CREATE TABLE blob_events (
                         id BLOB PRIMARY KEY,
                         payload TEXT NOT NULL
                     ) WITHOUT ROWID;
                     CREATE TABLE native_events (
                         id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                         payload TEXT NOT NULL
                     ) STRICT;
                     CREATE TABLE native_id_only (
                         id INTEGER PRIMARY KEY AUTOINCREMENT
                     ) STRICT;",
                    &mut migration,
                    None,
                )
                .unwrap();
            migration.publish_ready().unwrap();

            let database_id = storage.logical_catalog().default_database().id();
            storage
                .register_tables(vec![
                    TableDeclaration::sharded(
                        database_id,
                        "int_events",
                        ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
                    )
                    .unwrap(),
                    TableDeclaration::sharded(
                        database_id,
                        "text_events",
                        ShardKeyMetadata::new("id", ShardKeyType::Text).unwrap(),
                    )
                    .unwrap(),
                    TableDeclaration::sharded(
                        database_id,
                        "blob_events",
                        ShardKeyMetadata::new("id", ShardKeyType::Binary).unwrap(),
                    )
                    .unwrap(),
                    TableDeclaration::sharded(
                        database_id,
                        "native_events",
                        ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
                    )
                    .unwrap()
                    .with_generated_id_policy(GeneratedIdPolicy::native_range_v1("id").unwrap())
                    .unwrap(),
                    TableDeclaration::sharded(
                        database_id,
                        "native_id_only",
                        ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
                    )
                    .unwrap()
                    .with_generated_id_policy(GeneratedIdPolicy::native_range_v1("id").unwrap())
                    .unwrap(),
                ])
                .unwrap();

            let int_keys = keys_for_shards(&storage, shard_count, |candidate| {
                candidate.to_string().into_bytes()
            });
            let text_keys = keys_for_shards(&storage, shard_count, |candidate| {
                format!("tenant-{candidate}").into_bytes()
            })
            .into_iter()
            .map(|candidate| format!("tenant-{candidate}"))
            .collect::<Vec<_>>();
            let blob_candidates = keys_for_shards(&storage, shard_count, |candidate| {
                candidate.to_be_bytes().to_vec()
            });
            let blob_keys = blob_candidates
                .iter()
                .map(|candidate| candidate.to_be_bytes().to_vec())
                .collect::<Vec<_>>();
            let owners = storage.allocation_owner_map().unwrap();
            let native_ids = (0..shard_count)
                .map(|shard| {
                    NativeRangeV1Id::new(owners.owner_for_physical_shard(shard).unwrap(), 1)
                        .unwrap()
                        .encode()
                })
                .collect::<Vec<_>>();

            for shard in 0..shard_count {
                let connection = storage.open_shard(shard).unwrap();
                let index = usize::from(shard);
                connection
                    .execute(
                        "INSERT INTO int_events VALUES (?1, ?2)",
                        params![int_keys[index], format!("int-{shard}")],
                    )
                    .unwrap();
                connection
                    .execute(
                        "INSERT INTO text_events VALUES (?1, ?2)",
                        params![text_keys[index], format!("text-{shard}")],
                    )
                    .unwrap();
                connection
                    .execute(
                        "INSERT INTO blob_events VALUES (?1, ?2)",
                        params![blob_keys[index], format!("blob-{shard}")],
                    )
                    .unwrap();
                connection
                    .execute(
                        "INSERT INTO native_events VALUES (?1, ?2)",
                        params![native_ids[index], format!("native-{shard}")],
                    )
                    .unwrap();
            }

            Self {
                _temp: temp,
                storage,
                int_keys,
                text_keys,
                blob_keys,
                native_ids,
            }
        }

        fn native_table_id(&self) -> u64 {
            self.storage
                .logical_catalog()
                .table("default", "native_events")
                .unwrap()
                .unwrap()
                .id()
                .get()
        }

        fn native_id_only_table_id(&self) -> u64 {
            self.storage
                .logical_catalog()
                .table("default", "native_id_only")
                .unwrap()
                .unwrap()
                .id()
                .get()
        }
    }

    fn keys_for_shards(
        storage: &Storage,
        shard_count: u16,
        encoded: impl Fn(i64) -> Vec<u8>,
    ) -> Vec<i64> {
        let mut keys = vec![None; usize::from(shard_count)];
        for candidate in 1_i64.. {
            let shard = usize::from(storage.shard_for_key(&encoded(candidate)));
            if keys[shard].is_none() {
                keys[shard] = Some(candidate);
                if keys.iter().all(Option::is_some) {
                    break;
                }
            }
        }
        keys.into_iter().map(Option::unwrap).collect()
    }

    fn assert_persistent_sqlite_integrity(root: &std::path::Path, shard_count: u16) {
        let paths = std::iter::once(root.join("manifest.sqlite"))
            .chain((0..shard_count).map(|shard| root.join(format!("shards/{shard:04}.sqlite"))));
        for path in paths {
            let connection = Connection::open(&path).unwrap();
            assert_eq!(
                connection
                    .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok",
                "{} failed SQLite integrity_check",
                path.display()
            );
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                0,
                "{} failed SQLite foreign_key_check",
                path.display()
            );
        }
    }

    fn native_event_rows(storage: &Storage) -> Vec<(u16, i64, String)> {
        let mut rows = Vec::new();
        for shard in 0..storage.shard_count() {
            let connection = storage.open_shard(shard).unwrap();
            rows.extend(
                connection
                    .prepare("SELECT id, payload FROM native_events")
                    .unwrap()
                    .query_map([], |row| Ok((shard, row.get(0)?, row.get(1)?)))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
            );
        }
        rows.sort_unstable_by_key(|row| row.1);
        rows
    }

    fn assert_native_event_invariants(storage: &Storage) -> Vec<(u16, i64, String)> {
        let owners = storage.allocation_owner_map().unwrap();
        let rows = native_event_rows(storage);
        assert_eq!(
            rows.iter().map(|row| row.1).collect::<BTreeSet<_>>().len(),
            rows.len(),
            "native generated IDs must be globally unique"
        );
        for (shard, id, _) in &rows {
            let decoded = NativeRangeV1Id::decode(*id).unwrap();
            assert_eq!(owners.physical_shard(decoded.owner()), Some(*shard));
        }
        for shard in 0..storage.shard_count() {
            let maximum = rows
                .iter()
                .filter_map(|(row_shard, id, _)| (*row_shard == shard).then_some(*id))
                .max()
                .expect("the native generated fixture seeds every physical shard");
            assert_eq!(
                storage
                    .open_shard(shard)
                    .unwrap()
                    .query_row(
                        "SELECT seq FROM sqlite_sequence WHERE name = 'native_events'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                maximum,
                "shard {shard} SQLite sequence must equal its greatest durable generated ID"
            );
        }
        rows
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 24,
            max_shrink_iters: 1_024,
            ..ProptestConfig::default()
        })]

        #[test]
        fn randomized_write_sequences_match_the_model_physical_union_and_facade(
            operations in proptest::collection::vec(
                (0_u8..5, any::<bool>(), 2_i64..32, "[a-z0-9]{0,24}"),
                1..40,
            ),
        ) {
            let fixture = Fixture::new();
            let mut expected = BTreeMap::from([
                ((fixture.keys[0], 1_i64), "zero".to_owned()),
                ((fixture.keys[1], 1_i64), "one".to_owned()),
            ]);
            let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

            for (operation, second_shard, event_id, payload) in operations {
                let tenant_id = fixture.keys[usize::from(second_shard)];
                let identity = (tenant_id, event_id);
                let existed = expected.contains_key(&identity);
                match operation {
                    0 => {
                        let affected = if existed {
                            coordinator
                                .execute_dml(
                                    "UPDATE events SET payload = ?3
                                     WHERE tenant_id = ?1 AND event_id = ?2",
                                    params![tenant_id, event_id, payload],
                                )
                                .unwrap()
                                .affected_rows()
                        } else {
                            coordinator
                                .execute_dml(
                                    "INSERT INTO events
                                     (tenant_id, event_id, payload, amount, raw, optional, category)
                                     VALUES (?1, ?2, ?3, 1.0, x'00', NULL, 'property')",
                                    params![tenant_id, event_id, payload],
                                )
                                .unwrap()
                                .affected_rows()
                        };
                        prop_assert_eq!(affected, 1);
                        expected.insert(identity, payload);
                    }
                    1 => {
                        let affected = coordinator
                            .execute_dml(
                                "UPDATE events SET payload = ?3
                                 WHERE tenant_id = ?1 AND event_id = ?2",
                                params![tenant_id, event_id, payload],
                            )
                            .unwrap()
                            .affected_rows();
                        prop_assert_eq!(affected, usize::from(existed));
                        if existed {
                            expected.insert(identity, payload);
                        }
                    }
                    2 => {
                        let affected = coordinator
                            .execute_dml(
                                "DELETE FROM events WHERE tenant_id = ?1 AND event_id = ?2",
                                params![tenant_id, event_id],
                            )
                            .unwrap()
                            .affected_rows();
                        prop_assert_eq!(affected, usize::from(existed));
                        expected.remove(&identity);
                    }
                    3 | 4 => {
                        coordinator.begin().unwrap();
                        let affected = if existed {
                            coordinator
                                .execute_dml(
                                    "UPDATE events SET payload = ?3
                                     WHERE tenant_id = ?1 AND event_id = ?2",
                                    params![tenant_id, event_id, payload],
                                )
                                .unwrap()
                                .affected_rows()
                        } else {
                            coordinator
                                .execute_dml(
                                    "INSERT INTO events
                                     (tenant_id, event_id, payload, amount, raw, optional, category)
                                     VALUES (?1, ?2, ?3, 1.0, x'00', NULL, 'property')",
                                    params![tenant_id, event_id, payload],
                                )
                                .unwrap()
                                .affected_rows()
                        };
                        prop_assert_eq!(affected, 1);
                        if operation == 3 {
                            coordinator.rollback().unwrap();
                        } else {
                            coordinator.commit().unwrap();
                            expected.insert(identity, payload);
                        }
                    }
                    _ => unreachable!(),
                }
            }
            drop(coordinator);

            let mut physical = Vec::new();
            for shard in 0..2_u16 {
                let connection = fixture.storage.open_shard(shard).unwrap();
                let shard_rows = connection
                    .prepare("SELECT tenant_id, event_id, payload FROM events")
                    .unwrap()
                    .query_map([], |row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, String>(2)?))
                    })
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                prop_assert!(
                    shard_rows.iter().all(|row| row.0 == fixture.keys[usize::from(shard)]),
                    "physical shard {shard} contains a row owned by another shard: {shard_rows:?}",
                );
                physical.extend(shard_rows);
            }
            physical.sort_unstable();

            let facade = ReadCoordinator::open(fixture.storage.clone()).unwrap();
            let mut logical = facade
                .connection()
                .prepare("SELECT tenant_id, event_id, payload FROM events")
                .unwrap()
                .query_map([], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, String>(2)?))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            logical.sort_unstable();
            let modeled = expected
                .into_iter()
                .map(|((tenant_id, event_id), payload)| (tenant_id, event_id, payload))
                .collect::<Vec<_>>();

            prop_assert_eq!(&logical, &physical);
            prop_assert_eq!(logical, modeled);
            assert_persistent_sqlite_integrity(fixture.temp.path(), 2);
        }
    }

    #[test]
    fn normal_select_unions_typed_rows_from_two_stock_sqlite_files() {
        let fixture = Fixture::new();
        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        let mut statement = coordinator
            .connection()
            .prepare(
                "SELECT tenant_id, payload, amount, hex(raw), typeof(optional)
                 FROM events
                 WHERE tenant_id IN (?1, ?2)
                 ORDER BY payload",
            )
            .unwrap();
        let rows = statement
            .query_map(params![fixture.keys[0], fixture.keys[1]], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, f64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                (
                    fixture.keys[1],
                    "one".to_owned(),
                    2.5,
                    "FEFF".to_owned(),
                    "null".to_owned()
                ),
                (
                    fixture.keys[0],
                    "zero".to_owned(),
                    1.5,
                    "0001".to_owned(),
                    "text".to_owned()
                ),
            ]
        );

        let raw_text = coordinator
            .connection()
            .query_row(
                "SELECT optional FROM events WHERE payload = 'zero'",
                [],
                |row| match row.get_ref(0)? {
                    ValueRef::Text(bytes) => Ok(bytes.to_vec()),
                    value => panic!("expected raw SQLite text, got {value:?}"),
                },
            )
            .unwrap();
        assert_eq!(raw_text, [0x80, 0xff]);

        // The files remain independently readable by unmodified SQLite.
        for shard in 0..2 {
            let physical = Connection::open(
                fixture
                    .temp
                    .path()
                    .join(format!("shards/{shard:04}.sqlite")),
            )
            .unwrap();
            assert_eq!(
                physical
                    .query_row("SELECT COUNT(*) FROM events", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                1
            );
        }
    }

    #[test]
    fn coordinator_children_are_validated_os_level_read_only_handles() {
        let fixture = Fixture::new();
        for shard in 0..2 {
            let child = fixture.storage.open_shard_read_only(shard).unwrap();
            assert!(child.is_readonly(MAIN_DB).unwrap());
            assert!(
                child
                    .execute_batch("CREATE TABLE forbidden_on_read_only_child(id INTEGER)")
                    .is_err()
            );
        }

        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        assert_eq!(
            coordinator
                .connection()
                .query_row("SELECT COUNT(*) FROM events", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(coordinator.take_opened_shards(), [0, 1]);
    }

    #[tokio::test]
    async fn normal_select_rows_and_storage_types_match_the_engine_scatter_path() {
        let fixture = Fixture::new();
        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        let mut coordinator_rows = coordinator
            .connection()
            .prepare(
                "SELECT tenant_id, event_id, payload, amount, raw, optional
                 FROM events",
            )
            .unwrap()
            .query_map([], |row| {
                (0..6)
                    .map(|column| row.get_ref(column).map(sqlite_parity_cell))
                    .collect::<SqliteResult<Vec<_>>>()
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(coordinator.take_opened_shards(), [0, 1]);

        let database = Arc::new(Database::open(fixture.temp.path(), 2).unwrap());
        let engine = Engine::from_database(database);
        let executed = engine
            .query_logical(
                &engine.session(),
                Statement::new(
                    "SELECT tenant_id, event_id, payload, amount, raw, optional FROM events",
                    vec![],
                ),
            )
            .await
            .unwrap();
        assert_eq!(executed.shards(), [0, 1]);
        let mut engine_rows = executed
            .value
            .rows()
            .iter()
            .map(|row| row.values().iter().map(engine_parity_cell).collect())
            .collect::<Vec<Vec<_>>>();

        sort_parity_rows(&mut coordinator_rows);
        sort_parity_rows(&mut engine_rows);
        assert_eq!(coordinator_rows, engine_rows);
        assert!(
            coordinator_rows
                .iter()
                .flatten()
                .any(|cell| { matches!(cell, ParityCell::Text(bytes) if bytes == &[0x80, 0xff]) })
        );
        assert!(
            coordinator_rows
                .iter()
                .flatten()
                .any(|cell| matches!(cell, ParityCell::Null))
        );
        assert!(
            coordinator_rows.iter().flatten().any(|cell| {
                matches!(cell, ParityCell::Real(bits) if *bits == 1.5_f64.to_bits())
            })
        );
        assert!(
            coordinator_rows
                .iter()
                .flatten()
                .any(|cell| { matches!(cell, ParityCell::Blob(bytes) if bytes == &[0x00, 0x01]) })
        );
    }

    #[test]
    fn exact_typed_equalities_prune_to_one_shard_and_bind_the_child_predicate() {
        let fixture = TypedRoutingFixture::new(10);
        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        for shard in [0_u16, 4, 9] {
            let index = usize::from(shard);
            let cases: [(&str, &dyn ToSql, String); 3] = [
                (
                    "int_events",
                    &fixture.int_keys[index],
                    format!("int-{shard}"),
                ),
                (
                    "text_events",
                    &fixture.text_keys[index],
                    format!("text-{shard}"),
                ),
                (
                    "blob_events",
                    &fixture.blob_keys[index],
                    format!("blob-{shard}"),
                ),
            ];
            for (table, key, expected) in cases {
                let _ = coordinator.take_opened_shards();
                let payload = coordinator
                    .connection()
                    .query_row(
                        &format!("SELECT payload FROM {table} WHERE id = ?1"),
                        [key],
                        |row| row.get::<_, String>(0),
                    )
                    .unwrap();
                assert_eq!(payload, expected);
                assert_eq!(coordinator.take_opened_shards(), [shard]);
            }
        }

        // A second predicate must be evaluated by the indexed child query and
        // then rechecked by the coordinator, yielding no false-positive row.
        let _ = coordinator.take_opened_shards();
        assert_eq!(
            coordinator
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM int_events WHERE id = ?1 AND payload = 'absent'",
                    [fixture.int_keys[4]],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(coordinator.take_opened_shards(), [4]);

        let decoys = (1_000_i64..)
            .filter(|candidate| {
                fixture
                    .storage
                    .shard_for_key(candidate.to_string().as_bytes())
                    == 4
                    && *candidate != fixture.int_keys[4]
            })
            .take(2)
            .collect::<Vec<_>>();
        for decoy in decoys {
            fixture
                .storage
                .open_shard(4)
                .unwrap()
                .execute(
                    "INSERT INTO int_events VALUES (?1, 'same-shard-decoy')",
                    [decoy],
                )
                .unwrap();
        }
        let tight = ReadCoordinator::open_with_limits(
            fixture.storage.clone(),
            CursorLimits {
                rows: 1,
                bytes: MAX_CURSOR_BYTES,
            },
        )
        .unwrap();
        assert_eq!(
            tight
                .connection()
                .query_row(
                    "SELECT payload FROM int_events WHERE id = ?1",
                    [fixture.int_keys[4]],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "int-4"
        );
        assert_eq!(tight.take_opened_shards(), [4]);
    }

    #[test]
    fn null_and_mismatched_equalities_preserve_sqlite_semantics() {
        let fixture = TypedRoutingFixture::new(10);
        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();

        assert_eq!(
            coordinator
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM int_events WHERE id = NULL",
                    [],
                    |row| { row.get::<_, i64>(0) }
                )
                .unwrap(),
            0
        );
        assert!(coordinator.take_opened_shards().is_empty());

        // SQLite may coerce a TEXT RHS using INTEGER affinity. The router must
        // conservatively visit every shard when the incoming storage class is
        // not the catalog's exact shard-key type.
        let key = fixture.int_keys[4].to_string();
        assert_eq!(
            coordinator
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM int_events WHERE id = ?1",
                    [&key],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert_eq!(
            coordinator.take_opened_shards(),
            (0..10).collect::<Vec<_>>()
        );
    }

    #[test]
    fn nonbinary_text_collations_scan_all_shards_before_sqlite_rechecks() {
        let fixture = TypedRoutingFixture::new(10);
        let (stored_case, query_case, case_shard) = (1_i64..)
            .find_map(|candidate| {
                let stored = format!("Case-{candidate}");
                let query = stored.to_ascii_lowercase();
                let stored_shard = fixture.storage.shard_for_key(stored.as_bytes());
                (stored_shard != fixture.storage.shard_for_key(query.as_bytes())).then_some((
                    stored,
                    query,
                    stored_shard,
                ))
            })
            .unwrap();
        fixture
            .storage
            .open_shard(case_shard)
            .unwrap()
            .execute(
                "INSERT INTO text_events VALUES (?1, 'nocase')",
                [&stored_case],
            )
            .unwrap();

        let (stored_trim, query_trim, trim_shard) = (1_i64..)
            .find_map(|candidate| {
                let query = format!("trim-{candidate}");
                let stored = format!("{query}   ");
                let stored_shard = fixture.storage.shard_for_key(stored.as_bytes());
                (stored_shard != fixture.storage.shard_for_key(query.as_bytes())).then_some((
                    stored,
                    query,
                    stored_shard,
                ))
            })
            .unwrap();
        fixture
            .storage
            .open_shard(trim_shard)
            .unwrap()
            .execute(
                "INSERT INTO text_events VALUES (?1, 'rtrim')",
                [&stored_trim],
            )
            .unwrap();

        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        for (collation, query, expected) in [
            ("NOCASE", query_case, "nocase"),
            ("RTRIM", query_trim, "rtrim"),
        ] {
            let payloads = coordinator
                .connection()
                .prepare(&format!(
                    "SELECT payload FROM text_events WHERE id COLLATE {collation} = ?1"
                ))
                .unwrap()
                .query_map([&query], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(payloads, [expected]);
            assert_eq!(
                coordinator.take_opened_shards(),
                (0..10).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn native_ids_route_by_owner_while_legacy_ids_use_the_hash_map() {
        let fixture = TypedRoutingFixture::new(10);
        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        for shard in [0_u16, 4, 9] {
            let id = fixture.native_ids[usize::from(shard)];
            let _ = coordinator.take_opened_shards();
            assert_eq!(
                coordinator
                    .connection()
                    .query_row(
                        "SELECT payload FROM native_events WHERE id = ?1",
                        [id],
                        |row| row.get::<_, String>(0),
                    )
                    .unwrap(),
                format!("native-{shard}")
            );
            assert_eq!(coordinator.take_opened_shards(), [shard]);
        }

        let legacy = fixture.int_keys[4];
        fixture
            .storage
            .open_shard(4)
            .unwrap()
            .execute("INSERT INTO native_events VALUES (?1, 'legacy')", [legacy])
            .unwrap();
        assert_eq!(
            coordinator
                .connection()
                .query_row(
                    "SELECT payload FROM native_events WHERE id = ?1",
                    [legacy],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "legacy"
        );
        assert_eq!(coordinator.take_opened_shards(), [4]);

        let reserved = native_range_v1_sequence_floor(AllocationOwnerSlot::new(0).unwrap());
        let error = coordinator
            .connection()
            .query_row(
                "SELECT payload FROM native_events WHERE id = ?1",
                [reserved],
                |row| row.get::<_, String>(0),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            SqliteError::SqliteFailure(detail, _) if detail.code == ErrorCode::DatabaseCorrupt
        ));
        assert!(coordinator.take_opened_shards().is_empty());

        let unknown_owner = NativeRangeV1Id::new(AllocationOwnerSlot::new(1000).unwrap(), 1)
            .unwrap()
            .encode();
        assert_eq!(
            coordinator
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM native_events WHERE id = ?1",
                    [unknown_owner],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert!(coordinator.take_opened_shards().is_empty());
    }

    #[test]
    fn scans_match_physical_union_all_at_two_ten_and_sixty_four_shards() {
        for shard_count in [2_u16, 10, 64] {
            let fixture = ScaleFixture::new(shard_count);
            let duplicate_id = -9_000_000_000_i64 - i64::from(shard_count);
            for shard in 0..shard_count {
                fixture
                    .storage
                    .open_shard(shard)
                    .unwrap()
                    .execute(
                        "INSERT INTO scale_events VALUES (?1, 'duplicate')",
                        [duplicate_id],
                    )
                    .unwrap();
            }

            let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
            let mut rows = coordinator
                .connection()
                .prepare("SELECT id, payload FROM scale_events")
                .unwrap()
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<Result<Vec<(i64, String)>, _>>()
                .unwrap();
            rows.sort_unstable();
            assert_eq!(rows, fixture.physical_rows());
            assert_eq!(
                rows.iter()
                    .filter(|row| row == &&(duplicate_id, "duplicate".to_owned()))
                    .count(),
                usize::from(shard_count)
            );
            assert_eq!(
                coordinator.take_opened_shards(),
                (0..shard_count).collect::<Vec<_>>()
            );

            let point_shard = shard_count - 1;
            assert_eq!(
                coordinator
                    .connection()
                    .query_row(
                        "SELECT payload FROM scale_events WHERE id = ?1",
                        [fixture.keys[usize::from(point_shard)]],
                        |row| row.get::<_, String>(0),
                    )
                    .unwrap(),
                format!("shard-{point_shard}")
            );
            assert_eq!(coordinator.take_opened_shards(), [point_shard]);

            let empty_shard = if shard_count == 2 { 0 } else { shard_count / 2 };
            fixture
                .storage
                .open_shard(empty_shard)
                .unwrap()
                .execute("DELETE FROM scale_events", [])
                .unwrap();
            let scan_after_empty = coordinator
                .connection()
                .prepare("SELECT payload FROM scale_events")
                .unwrap()
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(
                scan_after_empty.len(),
                fixture.physical_rows().len(),
                "scan must skip an empty middle child and continue to later shards"
            );
            assert!(
                scan_after_empty
                    .iter()
                    .any(|payload| payload == &format!("shard-{}", shard_count - 1)),
                "scan stopped before the last non-empty shard"
            );
            assert_eq!(
                coordinator.take_opened_shards(),
                (0..shard_count).collect::<Vec<_>>()
            );

            for shard in 0..shard_count {
                fixture
                    .storage
                    .open_shard(shard)
                    .unwrap()
                    .execute("DELETE FROM scale_events", [])
                    .unwrap();
            }
            assert_eq!(
                coordinator
                    .connection()
                    .query_row("SELECT COUNT(*) FROM scale_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                0
            );
            assert_eq!(
                coordinator.take_opened_shards(),
                (0..shard_count).collect::<Vec<_>>()
            );
            assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
        }
    }

    #[test]
    fn stale_coordinators_fail_closed_and_fresh_reopens_work_at_scale() {
        for shard_count in [2_u16, 10, 64] {
            let fixture = ScaleFixture::new(shard_count);
            let stale = ReadCoordinator::open(fixture.storage.clone()).unwrap();
            let _ = stale.take_opened_shards();

            let mut migration = fixture.storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            fixture
                .storage
                .apply_schema_migration(
                    "CREATE INDEX scale_events_payload_idx ON scale_events(payload)",
                    &mut migration,
                    None,
                )
                .unwrap();
            migration.publish_ready().unwrap();

            let error = stale
                .connection()
                .query_row("SELECT COUNT(*) FROM scale_events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap_err();
            assert!(error.to_string().contains("coordinator schema is stale"));
            assert!(stale.take_opened_shards().is_empty());

            let fresh = ReadCoordinator::open(fixture.storage.clone()).unwrap();
            assert_eq!(
                fresh
                    .connection()
                    .query_row("SELECT COUNT(*) FROM scale_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                i64::from(shard_count)
            );
            assert_eq!(
                fresh.take_opened_shards(),
                (0..shard_count).collect::<Vec<_>>()
            );
            assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
        }
    }

    #[test]
    fn cancellation_releases_scaled_scans_and_allows_reuse() {
        for shard_count in [2_u16, 10, 64] {
            let fixture = ScaleFixture::new(shard_count);
            let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
            let mut gate = coordinator.install_child_scan_gate();
            let cancellation = coordinator.cancellation_handle();
            let worker = thread::spawn(move || {
                let error = coordinator
                    .connection()
                    .query_row("SELECT COUNT(*) FROM scale_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap_err();
                (coordinator, error)
            });

            gate.wait_until_started();
            cancellation.cancel();
            gate.release();
            let (coordinator, error) = worker.join().unwrap();
            assert!(matches!(
                error,
                SqliteError::SqliteFailure(detail, _)
                    if detail.code == ErrorCode::OperationInterrupted
            ));
            assert_eq!(
                *cancellation
                    .active_child_scans
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
                0
            );
            assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);

            assert_eq!(coordinator.take_opened_shards(), [0]);

            let point_shard = shard_count - 1;
            assert_eq!(
                coordinator
                    .connection()
                    .query_row(
                        "SELECT payload FROM scale_events WHERE id = ?1",
                        [fixture.keys[usize::from(point_shard)]],
                        |row| row.get::<_, String>(0),
                    )
                    .unwrap(),
                format!("shard-{point_shard}")
            );
            assert_eq!(coordinator.take_opened_shards(), [point_shard]);
        }
    }

    #[test]
    fn a_slow_scan_does_not_block_an_independent_point_reader_at_scale() {
        for shard_count in [2_u16, 10, 64] {
            let fixture = ScaleFixture::new(shard_count);
            let slow = ReadCoordinator::open(fixture.storage.clone()).unwrap();
            let mut gate = slow.install_child_scan_gate();
            let worker = thread::spawn(move || {
                let count = slow
                    .connection()
                    .query_row("SELECT COUNT(*) FROM scale_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap();
                (slow, count)
            });

            gate.wait_until_started();
            let point_shard = shard_count - 1;
            let fast = ReadCoordinator::open(fixture.storage.clone()).unwrap();
            assert_eq!(
                fast.connection()
                    .query_row(
                        "SELECT payload FROM scale_events WHERE id = ?1",
                        [fixture.keys[usize::from(point_shard)]],
                        |row| row.get::<_, String>(0),
                    )
                    .unwrap(),
                format!("shard-{point_shard}")
            );
            assert_eq!(fast.take_opened_shards(), [point_shard]);
            gate.release();
            let (_slow, count) = worker.join().unwrap();
            assert_eq!(count, i64::from(shard_count));
            assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
        }
    }

    #[test]
    fn separate_concurrent_readers_are_consistent_at_scale() {
        for shard_count in [2_u16, 10, 64] {
            let fixture = ScaleFixture::new(shard_count);
            let reader_count = if shard_count == 64 { 4 } else { 8 };
            let (ready_sender, ready) = mpsc::sync_channel(reader_count);
            let mut start_releases = Vec::with_capacity(reader_count);
            let mut readers = Vec::new();
            for reader in 0..reader_count {
                let storage = fixture.storage.clone();
                let ready_sender = ready_sender.clone();
                let (start_sender, start) = mpsc::sync_channel(1);
                start_releases.push(TestRelease::new(start_sender));
                let point_key = fixture.keys[usize::from(shard_count - 1)];
                readers.push(thread::spawn(move || {
                    let coordinator = ReadCoordinator::open(storage).unwrap();
                    ready_sender
                        .send(())
                        .expect("concurrent-reader test coordinator dropped its ready receiver");
                    start
                        .recv_timeout(TEST_SYNC_TIMEOUT)
                        .expect("concurrent reader was not released before the timeout");
                    if reader % 2 == 0 {
                        coordinator
                            .connection()
                            .query_row("SELECT COUNT(*) FROM scale_events", [], |row| {
                                row.get::<_, i64>(0)
                            })
                            .unwrap()
                    } else {
                        coordinator
                            .connection()
                            .query_row(
                                "SELECT COUNT(*) FROM scale_events WHERE id = ?1",
                                [point_key],
                                |row| row.get::<_, i64>(0),
                            )
                            .unwrap()
                    }
                }));
            }
            drop(ready_sender);

            let mut ready_error = None;
            for _ in 0..reader_count {
                if let Err(error) = ready.recv_timeout(TEST_SYNC_TIMEOUT) {
                    ready_error = Some(error);
                    break;
                }
            }
            for release in &mut start_releases {
                release.signal();
            }
            let outcomes = readers
                .into_iter()
                .enumerate()
                .map(|(reader, handle)| (reader, handle.join()))
                .collect::<Vec<_>>();

            assert!(
                ready_error.is_none(),
                "concurrent readers did not all become ready before the timeout: {ready_error:?}"
            );
            for (reader, outcome) in outcomes {
                assert_eq!(
                    outcome.unwrap(),
                    if reader % 2 == 0 {
                        i64::from(shard_count)
                    } else {
                        1
                    }
                );
            }
            assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
        }
    }

    #[test]
    fn a_middle_shard_error_stops_later_scans_and_marks_persistent_degradation() {
        for shard_count in [2_u16, 10, 64] {
            let fixture = ScaleFixture::new(shard_count);
            let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
            let failed_shard = shard_count / 2;
            let shard_path = fixture
                .temp
                .path()
                .join(format!("shards/{failed_shard:04}.sqlite"));
            let held_path = fixture
                .temp
                .path()
                .join(format!("shards/{failed_shard:04}.sqlite.held"));
            std::fs::rename(&shard_path, &held_path).unwrap();
            let error = coordinator
                .connection()
                .query_row("SELECT COUNT(*) FROM scale_events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap_err();
            std::fs::rename(&held_path, &shard_path).unwrap();
            assert!(matches!(
                error,
                SqliteError::SqliteFailure(detail, _)
                    if detail.code == ErrorCode::DatabaseCorrupt
            ));
            assert_eq!(
                coordinator.take_opened_shards(),
                (0..failed_shard).collect::<Vec<_>>()
            );
            assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
            let sticky_error = coordinator
                .connection()
                .query_row("SELECT COUNT(*) FROM scale_events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap_err();
            assert!(matches!(
                sticky_error,
                SqliteError::SqliteFailure(detail, _)
                    if detail.code == ErrorCode::DatabaseCorrupt
            ));

            let ScaleFixture {
                temp,
                storage,
                keys: _,
            } = fixture;
            drop(coordinator);
            drop(storage);
            let reopen_error = Storage::open(temp.path(), shard_count).unwrap_err();
            assert_eq!(reopen_error.kind(), EngineErrorKind::DataCorruption);
            assert!(reopen_error.to_string().contains("persistently degraded"));
        }
    }

    #[test]
    fn sqlite_composes_filters_ordering_aggregation_limit_and_rowid_above_facade() {
        let fixture = Fixture::new();
        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        let aggregate = coordinator
            .connection()
            .query_row(
                "SELECT COUNT(*), group_concat(payload, ',')
                 FROM (
                    SELECT payload FROM events
                    WHERE amount >= 1
                    ORDER BY payload
                    LIMIT 2
                 )",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap();
        assert_eq!(aggregate, (2, "one,zero".to_owned()));

        let rowids = coordinator
            .connection()
            .prepare("SELECT rowid, payload FROM events ORDER BY rowid")
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rowids, [(1, "zero".to_owned()), (2, "one".to_owned())]);

        let collated = coordinator
            .connection()
            .prepare("SELECT category FROM events ORDER BY category")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(collated, ["alpha", "Zulu"]);
        assert_eq!(
            coordinator
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE category = 'ALPHA'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );

        let joined = coordinator
            .connection()
            .query_row(
                "SELECT COUNT(*)
                 FROM events AS left_event
                 JOIN events AS right_event
                   ON left_event.tenant_id = right_event.tenant_id",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(joined, 2);
    }

    #[test]
    fn global_rows_are_read_once_and_catalog_placement_is_hidden() {
        let fixture = Fixture::new();
        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        assert_eq!(
            coordinator
                .connection()
                .query_row("SELECT COUNT(*) FROM countries", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert!(
            coordinator
                .connection()
                .prepare("SELECT * FROM internal_catalog")
                .is_err()
        );
    }

    #[test]
    fn coordinator_authorizer_seals_writes_attach_pragmas_and_extension_loading() {
        let fixture = Fixture::new();
        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        let before = fixture.physical_row_count();
        let physical_schema_before = (0..fixture.storage.shard_count())
            .map(|shard| {
                let connection = fixture.storage.open_shard(shard).unwrap();
                (
                    connection
                        .pragma_query_value(None, "schema_version", |row| row.get::<_, i64>(0))
                        .unwrap(),
                    connection
                        .query_row(
                            "SELECT group_concat(sql, char(10))
                             FROM (
                                 SELECT sql FROM sqlite_schema
                                 WHERE sql IS NOT NULL ORDER BY name COLLATE BINARY
                             )",
                            [],
                            |row| row.get::<_, String>(0),
                        )
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        for sql in [
            "INSERT INTO events VALUES (99, 99, 'bad', 0, NULL, NULL, 'bad')",
            "UPDATE events SET payload = 'bad'",
            "DELETE FROM events",
            "CREATE TABLE denied_table(id INTEGER)",
            "ALTER TABLE events ADD COLUMN denied_column TEXT",
            "DROP TABLE events",
            "DETACH DATABASE main",
            "PRAGMA writable_schema = ON",
        ] {
            assert!(coordinator.connection().execute(sql, []).is_err(), "{sql}");
        }
        assert_eq!(fixture.physical_row_count(), before);
        assert!(
            coordinator
                .connection()
                .execute_batch("PRAGMA query_only = OFF")
                .is_err()
        );
        assert!(
            coordinator
                .connection()
                .pragma_query_value(None, "query_only", |row| row.get::<_, bool>(0))
                .unwrap()
        );

        let attach_dir = tempfile::tempdir().unwrap();
        let attached_path = attach_dir.path().join("must-not-exist.sqlite");
        let attached_path_text = attached_path.to_str().unwrap();
        assert!(
            coordinator
                .connection()
                .execute("ATTACH DATABASE ?1 AS foreign_data", [attached_path_text])
                .is_err()
        );
        assert!(!attached_path.exists());
        let databases = coordinator
            .connection()
            .prepare("PRAGMA database_list")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(databases, ["main"]);

        let extension_error = coordinator
            .connection()
            .query_row("SELECT load_extension(NULL)", [], |_| Ok(()))
            .unwrap_err();
        assert!(
            matches!(
                &extension_error,
                SqliteError::SqlInputError { msg, .. }
                    if msg.contains("not authorized to use function: load_extension")
            ),
            "{extension_error:?}"
        );
        let physical_schema_after = (0..fixture.storage.shard_count())
            .map(|shard| {
                let connection = fixture.storage.open_shard(shard).unwrap();
                (
                    connection
                        .pragma_query_value(None, "schema_version", |row| row.get::<_, i64>(0))
                        .unwrap(),
                    connection
                        .query_row(
                            "SELECT group_concat(sql, char(10))
                             FROM (
                                 SELECT sql FROM sqlite_schema
                                 WHERE sql IS NOT NULL ORDER BY name COLLATE BINARY
                             )",
                            [],
                            |row| row.get::<_, String>(0),
                        )
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(physical_schema_after, physical_schema_before);
    }

    #[test]
    fn cursor_holds_schema_admission_until_eof_or_close() {
        let fixture = Fixture::new();
        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);

        let mut statement = coordinator
            .connection()
            .prepare("SELECT payload FROM events")
            .unwrap();
        let mut rows = statement.query([]).unwrap();
        assert!(rows.next().unwrap().is_some());
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 1);
        drop(rows);
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
    }

    #[test]
    fn cancellation_interrupts_child_scan_releases_guard_and_allows_reuse() {
        let fixture = Fixture::new();
        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        let mut gate = coordinator.install_child_scan_gate();
        let cancellation = coordinator.cancellation_handle();

        let worker = thread::spawn(move || {
            let error = coordinator
                .connection()
                .query_row("SELECT COUNT(*) FROM events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap_err();
            (coordinator, error)
        });
        gate.wait_until_started();
        assert_eq!(
            *cancellation
                .active_child_scans
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            1
        );
        cancellation.cancel();
        gate.release();
        let (coordinator, error) = worker.join().unwrap();
        assert!(matches!(
            error,
            SqliteError::SqliteFailure(detail, _)
                if detail.code == ErrorCode::OperationInterrupted
        ));
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
        assert_eq!(
            *cancellation
                .active_child_scans
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            0
        );
        assert_eq!(
            coordinator
                .connection()
                .query_row("SELECT COUNT(*) FROM events", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        cancellation.cancel();
        assert_eq!(
            coordinator
                .connection()
                .query_row("SELECT COUNT(*) FROM events", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[test]
    fn cancellation_between_materialized_rows_releases_guard_and_allows_reuse() {
        let fixture = Fixture::new();
        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        let cancellation = coordinator.cancellation_handle();
        let mut statement = coordinator
            .connection()
            .prepare("SELECT payload FROM events")
            .unwrap();
        let mut rows = statement.query([]).unwrap();
        assert!(rows.next().unwrap().is_some());
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 1);

        cancellation.cancel();
        let error = rows.next().unwrap_err();
        assert!(matches!(
            error,
            SqliteError::SqliteFailure(detail, _)
                if detail.code == ErrorCode::OperationInterrupted
        ));
        drop(rows);
        drop(statement);
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
        assert_eq!(
            coordinator
                .connection()
                .query_row("SELECT COUNT(*) FROM events", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[test]
    fn cancellation_interrupts_coordinator_work_after_the_child_handle_closes() {
        let fixture = Fixture::new();
        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        let mut child_gate = coordinator.install_child_scan_complete_gate();
        let (outer_gate, mut outer_control) = TestChildScanGate::channel();
        let outer_armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let progress_armed = Arc::clone(&outer_armed);
        let mut outer_gate = Some(outer_gate);
        coordinator
            .connection()
            .progress_handler(
                1,
                Some(move || {
                    if progress_armed.load(Ordering::Acquire) {
                        if let Some(gate) = outer_gate.take() {
                            return !gate.wait_for_release();
                        }
                    }
                    false
                }),
            )
            .unwrap();
        let cancellation = coordinator.cancellation_handle();
        let worker = thread::spawn(move || {
            let error = coordinator
                .connection()
                .query_row("SELECT COUNT(*) FROM events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap_err();
            (coordinator, error)
        });

        child_gate.wait_until_started();
        outer_armed.store(true, Ordering::Release);
        child_gate.release();
        outer_control.wait_until_started();
        assert_eq!(
            *cancellation
                .active_child_scans
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            0,
            "the cancellation must occur during coordinator work, not a child callback"
        );
        cancellation.cancel();
        outer_control.release();

        let (coordinator, error) = worker.join().unwrap();
        assert!(matches!(
            error,
            SqliteError::SqliteFailure(detail, _)
                if detail.code == ErrorCode::OperationInterrupted
        ));
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
        coordinator
            .connection()
            .progress_handler(0, None::<fn() -> bool>)
            .unwrap();
        assert_eq!(
            coordinator
                .connection()
                .query_row("SELECT COUNT(*) FROM events", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[test]
    fn cancellation_after_an_empty_child_scan_is_not_lost() {
        let fixture = Fixture::new();
        for shard in 0..fixture.storage.shard_count() {
            fixture
                .storage
                .open_shard(shard)
                .unwrap()
                .execute("DELETE FROM countries", [])
                .unwrap();
        }

        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        let mut gate = coordinator.install_child_scan_complete_gate();
        let cancellation = coordinator.cancellation_handle();

        let worker = thread::spawn(move || {
            let error = coordinator
                .connection()
                .query_row("SELECT COUNT(*) FROM countries", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap_err();
            (coordinator, error)
        });
        gate.wait_until_started();
        // Advance only the epoch so this test proves the post-scan check,
        // independently of both SQLite interrupt handles and progress hooks.
        cancellation.epoch.fetch_add(1, Ordering::AcqRel);
        gate.release();

        let (coordinator, error) = worker.join().unwrap();
        assert!(matches!(
            error,
            SqliteError::SqliteFailure(detail, _)
                if detail.code == ErrorCode::OperationInterrupted
        ));
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
        assert_eq!(
            *cancellation
                .active_child_scans
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            0
        );
        assert_eq!(
            coordinator
                .connection()
                .query_row("SELECT COUNT(*) FROM countries", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn coordinator_reopen_connects_and_teardown_closes_every_cursor_and_table() {
        let fixture = Fixture::new();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("coordinator.sqlite");

        let first = ReadCoordinator::open_at(fixture.storage.clone(), &path).unwrap();
        let first_lifecycle = first.lifecycle();
        let selected = first
            .connection()
            .prepare("SELECT rowid, payload FROM events")
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(selected.len(), 2);
        let first_snapshot = first_lifecycle.snapshot();
        assert_eq!(first_snapshot.creates, 2);
        assert_eq!(first_snapshot.connects, 2);
        assert!(first_snapshot.opens >= 1);
        assert_eq!(first_snapshot.opens, first_snapshot.closes);
        assert!(first_snapshot.filters >= 1);
        assert!(first_snapshot.nexts >= 2);
        assert!(first_snapshot.eofs >= 1);
        assert!(first_snapshot.columns >= 2);
        assert!(first_snapshot.rowids >= 2);
        drop(first);
        assert_eq!(first_lifecycle.snapshot().disconnects, 2);

        let second = ReadCoordinator::open_at(fixture.storage.clone(), &path).unwrap();
        let second_lifecycle = second.lifecycle();
        assert_eq!(second_lifecycle.snapshot().creates, 0);
        assert_eq!(
            second
                .connection()
                .query_row("SELECT COUNT(*) FROM events", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            second
                .connection()
                .query_row("SELECT COUNT(*) FROM countries", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(second_lifecycle.snapshot().connects, 2);
        drop(second);
        assert_eq!(second_lifecycle.snapshot().disconnects, 2);
    }

    #[test]
    fn durable_test_coordinator_rejects_shadow_and_foreign_schema_objects() {
        let fixture = Fixture::new();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("coordinator.sqlite");
        Connection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE events(shadow TEXT)")
            .unwrap();

        let error = match ReadCoordinator::open_at(fixture.storage.clone(), &path) {
            Ok(_) => panic!("shadow coordinator schema must be rejected"),
            Err(error) => error,
        };
        assert!(matches!(
            error.kind(),
            EngineErrorKind::InvalidQuery | EngineErrorKind::FailedPrecondition
        ));
        let connection = Connection::open(&path).unwrap();
        let objects = connection
            .prepare(
                "SELECT name FROM sqlite_schema
                 WHERE name NOT GLOB 'sqlite_*'
                 ORDER BY name COLLATE BINARY",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(objects, ["events".to_owned()]);
    }

    #[test]
    fn malformed_creation_write_and_destroy_paths_are_bounded() {
        let fixture = Fixture::new();
        let operation = fixture.storage.enter_schema_operation().unwrap();
        let registry = Registry::build_admitted(fixture.storage.clone()).unwrap();
        drop(operation);
        let lifecycle = Arc::clone(&registry.lifecycle);
        let connection = Connection::open_in_memory().unwrap();
        register_module(&connection, Arc::clone(&registry)).unwrap();

        assert!(
            connection
                .execute_batch("CREATE VIRTUAL TABLE missing USING brisk_shard")
                .is_err()
        );
        assert!(
            connection
                .execute_batch("CREATE VIRTUAL TABLE unknown USING brisk_shard(999999)")
                .is_err()
        );
        let events_id = fixture
            .storage
            .logical_catalog()
            .tables()
            .iter()
            .find(|table| table.name() == "events")
            .unwrap()
            .id()
            .get();
        connection
            .execute_batch(&format!(
                "CREATE VIRTUAL TABLE facade USING brisk_shard({events_id})"
            ))
            .unwrap();
        assert!(
            connection
                .execute(
                    "INSERT INTO facade VALUES (99, 99, 'bad', 0, NULL, NULL, 'bad')",
                    []
                )
                .is_err()
        );
        connection.execute_batch("DROP TABLE facade").unwrap();
        let snapshot = lifecycle.snapshot();
        assert_eq!(snapshot.destroys, 1);
        assert!(snapshot.disconnects >= 1);
        assert_eq!(fixture.physical_row_count(), 2);
    }

    #[test]
    fn stale_coordinator_fails_before_opening_a_child_shard() {
        let fixture = Fixture::new();
        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        let mut migration = fixture.storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        fixture
            .storage
            .apply_schema_migration(
                "ALTER TABLE events ADD COLUMN added_after_open TEXT",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();

        let error = coordinator
            .connection()
            .query_row("SELECT COUNT(*) FROM events", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap_err();
        assert!(error.to_string().contains("coordinator schema is stale"));
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
    }

    #[test]
    fn later_shard_open_failure_exits_xnext_and_releases_schema_admission() {
        let fixture = Fixture::new();
        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        let shard_path = fixture.temp.path().join("shards/0001.sqlite");
        let held_path = fixture.temp.path().join("shards/0001.sqlite.held");

        let mut statement = coordinator
            .connection()
            .prepare("SELECT payload FROM events")
            .unwrap();
        let mut rows = statement.query([]).unwrap();
        assert_eq!(
            rows.next().unwrap().unwrap().get::<_, String>(0).unwrap(),
            "zero"
        );
        std::fs::rename(&shard_path, &held_path).unwrap();
        let error = rows.next().unwrap_err();
        drop(rows);
        std::fs::rename(&held_path, &shard_path).unwrap();

        assert!(
            matches!(
                &error,
                SqliteError::SqliteFailure(detail, _)
                    if detail.code == ErrorCode::DatabaseCorrupt
            ),
            "{error:?}"
        );
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
    }

    #[test]
    fn materialization_budget_is_checked_before_cell_payload_copy() {
        let fixture = Fixture::new();
        let operation = fixture.storage.enter_schema_operation().unwrap();
        let registry = Registry::build_admitted(fixture.storage.clone()).unwrap();
        drop(operation);
        let spec = registry
            .tables
            .values()
            .find(|spec| spec.name == "events")
            .unwrap();

        let error = registry
            .read_shard_rows(spec, 0, None, 0, MAX_CURSOR_ROWS, 1)
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(
            *registry
                .active_child_scans
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            0
        );
    }

    #[test]
    fn row_and_byte_limits_hold_at_the_boundary_and_recover_after_failure() {
        let fixture = Fixture::new();
        let operation = fixture.storage.enter_schema_operation().unwrap();
        let registry = Registry::build_admitted(fixture.storage.clone()).unwrap();
        drop(operation);
        let spec = registry
            .tables
            .values()
            .find(|spec| spec.name == "events")
            .unwrap();

        let (_, exact_bytes) = registry
            .read_shard_rows(spec, 0, None, 0, 1, MAX_CURSOR_BYTES)
            .unwrap();
        assert_eq!(
            registry
                .read_shard_rows(spec, 0, None, 0, 1, exact_bytes)
                .unwrap()
                .0
                .len(),
            1
        );
        assert_eq!(
            registry
                .read_shard_rows(spec, 0, None, 0, 1, exact_bytes - 1)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );

        let equality_bytes = RawCell::accounted_payload_bytes(ValueRef::Integer(fixture.keys[0]))
            + VALUE_ACCOUNTING_BYTES
            + ALLOCATION_OVERHEAD_BYTES;
        let combined_bytes = equality_bytes + exact_bytes;
        let combined_exact = ReadCoordinator::open_with_limits(
            fixture.storage.clone(),
            CursorLimits {
                rows: 1,
                bytes: combined_bytes,
            },
        )
        .unwrap();
        assert_eq!(
            combined_exact
                .connection()
                .query_row(
                    "SELECT payload FROM events WHERE tenant_id = ?1",
                    [fixture.keys[0]],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "zero"
        );
        let combined_one_under = ReadCoordinator::open_with_limits(
            fixture.storage.clone(),
            CursorLimits {
                rows: 1,
                bytes: combined_bytes - 1,
            },
        )
        .unwrap();
        let error = combined_one_under
            .connection()
            .query_row(
                "SELECT payload FROM events WHERE tenant_id = ?1",
                [fixture.keys[0]],
                |row| row.get::<_, String>(0),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            SqliteError::SqliteFailure(detail, _) if detail.code == ErrorCode::TooBig
        ));
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);

        assert_eq!(
            registry
                .read_shard_rows(spec, 0, None, 0, 0, MAX_CURSOR_BYTES)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );

        let exact = ReadCoordinator::open_with_limits(
            fixture.storage.clone(),
            CursorLimits {
                rows: 2,
                bytes: MAX_CURSOR_BYTES,
            },
        )
        .unwrap();
        assert_eq!(
            exact
                .connection()
                .query_row("SELECT COUNT(*) FROM events", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );

        let one = ReadCoordinator::open_with_limits(
            fixture.storage.clone(),
            CursorLimits {
                rows: 1,
                bytes: MAX_CURSOR_BYTES,
            },
        )
        .unwrap();
        let error = one
            .connection()
            .query_row("SELECT COUNT(*) FROM events", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap_err();
        assert!(matches!(
            error,
            SqliteError::SqliteFailure(detail, _) if detail.code == ErrorCode::TooBig
        ));
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
        assert_eq!(
            one.connection()
                .query_row(
                    "SELECT payload FROM events WHERE tenant_id = ?1",
                    [fixture.keys[0]],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "zero"
        );
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);

        let tiny_bytes = ReadCoordinator::open_with_limits(
            fixture.storage.clone(),
            CursorLimits {
                rows: MAX_CURSOR_ROWS,
                bytes: 64,
            },
        )
        .unwrap();
        let oversized = "x".repeat(65);
        let error = tiny_bytes
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE tenant_id = ?1",
                [&oversized],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            SqliteError::SqliteFailure(detail, _) if detail.code == ErrorCode::TooBig
        ));
        assert!(tiny_bytes.take_opened_shards().is_empty());
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
        assert_eq!(
            tiny_bytes
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE tenant_id = NULL",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert!(tiny_bytes.take_opened_shards().is_empty());
    }

    #[test]
    fn operational_engine_errors_keep_specific_sqlite_result_codes() {
        let cases = [
            (EngineErrorKind::Busy, ffi::SQLITE_BUSY),
            (EngineErrorKind::Cancelled, ffi::SQLITE_INTERRUPT),
            (EngineErrorKind::PermissionDenied, ffi::SQLITE_AUTH),
            (EngineErrorKind::ReadOnly, ffi::SQLITE_READONLY),
            (EngineErrorKind::LimitExceeded, ffi::SQLITE_TOOBIG),
            (EngineErrorKind::StorageFull, ffi::SQLITE_FULL),
            (EngineErrorKind::OutOfMemory, ffi::SQLITE_NOMEM),
            (EngineErrorKind::StorageUnavailable, ffi::SQLITE_IOERR),
            (EngineErrorKind::DataCorruption, ffi::SQLITE_CORRUPT),
            (EngineErrorKind::Internal, ffi::SQLITE_INTERNAL),
        ];
        for (kind, expected) in cases {
            let error = vtab_error(EngineError::new(kind, "test"));
            assert!(matches!(
                error,
                SqliteError::SqliteFailure(ref detail, _) if detail.extended_code == expected
            ));
        }
    }

    #[test]
    fn diagnostics_show_sharded_reads_visit_both_files_and_globals_only_zero() {
        let fixture = Fixture::new();
        let coordinator = ReadCoordinator::open(fixture.storage.clone()).unwrap();
        coordinator
            .connection()
            .query_row("SELECT COUNT(*) FROM events", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        coordinator
            .connection()
            .query_row("SELECT COUNT(*) FROM countries", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        let opened = coordinator
            .registry
            .opened_shards
            .lock()
            .unwrap()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        assert_eq!(opened, BTreeSet::from([0, 1]));
    }

    #[test]
    fn writable_coordinator_inserts_an_explicit_key_on_its_owner_shard() {
        let fixture = Fixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let result = coordinator
            .execute_dml(
                "INSERT INTO events
                 (tenant_id, event_id, payload, amount, raw, optional, category)
                 VALUES (?1, 2, 'written', 3.5, x'01', NULL, 'beta')",
                [fixture.keys[0]],
            )
            .unwrap();
        assert_eq!(result.affected_rows, 1);
        assert_eq!(result.explicit_key, Some(Value::Int64(fixture.keys[0])));
        assert_eq!(fixture.physical_row_count(), 3);
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT payload FROM events WHERE tenant_id = ?1 AND event_id = 2",
                    [fixture.keys[0]],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "written"
        );
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
    }

    #[test]
    fn writable_admitted_coordinator_does_not_reenter_schema_admission() {
        let fixture = Fixture::new();
        let operation = fixture.storage.enter_schema_operation().unwrap();
        let migration = fixture.storage.begin_schema_migration().unwrap();
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 1);

        // Opening and executing after the migration starts proves bootstrap,
        // write-state callbacks, and the UPDATE cursor all reuse the admission
        // that the Engine acquired before the migration closed the gate.
        let mut coordinator =
            WriteCoordinator::open_admitted(fixture.storage.clone(), operation).unwrap();
        let updated = coordinator
            .execute_dml(
                "UPDATE events SET payload = 'admitted'
                 WHERE tenant_id = ?1 AND event_id = 1",
                [fixture.keys[0]],
            )
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 1);

        drop(coordinator);
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
        migration.publish_ready().unwrap();
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT payload FROM events WHERE tenant_id = ?1 AND event_id = 1",
                    [fixture.keys[0]],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "admitted"
        );
    }

    #[test]
    fn writable_coordinator_binds_protocol_neutral_values_without_coercion() {
        let fixture = Fixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let parameters = [
            Value::Int64(fixture.keys[0]),
            Value::Int64(2),
            Value::Text("bound text".to_owned()),
            Value::Float64(3.5),
            Value::Binary(vec![0, 255]),
            Value::Null,
            Value::Text("beta".to_owned()),
        ];

        let result = coordinator
            .execute_dml_values(
                "INSERT INTO events
                 (tenant_id, event_id, payload, amount, raw, optional, category)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                &parameters,
            )
            .unwrap();

        assert_eq!(result.affected_rows(), 1);
        assert_eq!(result.explicit_key(), Some(&Value::Int64(fixture.keys[0])));
        let connection = fixture.storage.open_shard(0).unwrap();
        let persisted = connection
            .query_row(
                "SELECT payload, amount, raw, optional
                 FROM events WHERE tenant_id = ?1 AND event_id = 2",
                [fixture.keys[0]],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            persisted,
            ("bound text".to_owned(), 3.5, vec![0, 255], None)
        );
    }

    #[test]
    fn writable_protocol_value_rejection_precedes_transaction_and_allows_reuse() {
        let fixture = Fixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let too_large = u64::try_from(i64::MAX).unwrap() + 1;

        let error = coordinator
            .execute_dml_values(
                "INSERT INTO events
                 (tenant_id, event_id, payload, amount, raw, optional, category)
                 VALUES (?1, 2, 'rejected', 1.0, x'00', NULL, 'beta')",
                &[Value::UInt64(too_large)],
            )
            .unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::NumericOutOfRange);
        assert!(!coordinator.in_transaction());
        assert_eq!(fixture.physical_row_count(), 2);

        let result = coordinator
            .execute_dml_values(
                "INSERT INTO events
                 (tenant_id, event_id, payload, amount, raw, optional, category)
                 VALUES (?1, 2, 'accepted', 1.0, x'00', NULL, 'beta')",
                &[Value::Int64(fixture.keys[0])],
            )
            .unwrap();
        assert_eq!(result.affected_rows(), 1);
        assert_eq!(fixture.physical_row_count(), 3);
    }

    #[test]
    fn writable_coordinator_updates_and_deletes_through_opaque_locators() {
        let fixture = Fixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let updated = coordinator
            .execute_dml(
                "UPDATE events
                 SET payload = 'updated', optional = 'present'
                 WHERE tenant_id = ?1 AND event_id = 1",
                [fixture.keys[0]],
            )
            .unwrap();
        assert_eq!(updated.affected_rows, 1);
        assert_eq!(updated.explicit_key, None);
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT payload || ':' || optional FROM events
                     WHERE tenant_id = ?1 AND event_id = 1",
                    [fixture.keys[0]],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "updated:present"
        );

        let deleted = coordinator
            .execute_dml(
                "DELETE FROM events WHERE tenant_id = ?1 AND event_id = 1",
                [fixture.keys[0]],
            )
            .unwrap();
        assert_eq!(deleted.affected_rows, 1);
        assert_eq!(fixture.physical_row_count(), 1);
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
    }

    #[test]
    fn writable_explicit_transactions_commit_rollback_and_nested_savepoints() {
        let fixture = Fixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        coordinator.begin().unwrap();
        coordinator
            .execute_dml(
                "INSERT INTO events VALUES (?1, 2, 'kept', 2.0, x'02', NULL, 'beta')",
                [fixture.keys[0]],
            )
            .unwrap();
        coordinator.savepoint("outer").unwrap();
        coordinator
            .execute_dml(
                "INSERT INTO events VALUES (?1, 3, 'discarded', 3.0, x'03', NULL, 'gamma')",
                [fixture.keys[0]],
            )
            .unwrap();
        coordinator.savepoint("inner").unwrap();
        coordinator
            .execute_dml(
                "UPDATE events SET payload = 'also-discarded'
                 WHERE tenant_id = ?1 AND event_id = 1",
                [fixture.keys[0]],
            )
            .unwrap();
        coordinator.rollback_to("outer").unwrap();
        coordinator.release("outer").unwrap();
        coordinator.commit().unwrap();
        assert!(!coordinator.in_transaction());

        let shard = fixture.storage.open_shard(0).unwrap();
        assert_eq!(
            shard
                .query_row(
                    "SELECT GROUP_CONCAT(event_id || ':' || payload, ',')
                     FROM events WHERE tenant_id = ?1 ORDER BY event_id",
                    [fixture.keys[0]],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "1:zero,2:kept"
        );

        coordinator.begin().unwrap();
        coordinator
            .execute_dml(
                "DELETE FROM events WHERE tenant_id = ?1 AND event_id = 2",
                [fixture.keys[0]],
            )
            .unwrap();
        coordinator.rollback().unwrap();
        assert_eq!(fixture.physical_row_count(), 3);
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
    }

    #[test]
    fn writable_late_enlistment_preserves_preexisting_nested_savepoints() {
        let fixture = Fixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        coordinator.begin().unwrap();
        coordinator.savepoint("outer").unwrap();
        coordinator.savepoint("inner").unwrap();
        coordinator
            .execute_dml(
                "INSERT INTO events VALUES
                 (?1, 2, 'rolled-back', 2.0, x'02', NULL, 'beta')",
                [fixture.keys[0]],
            )
            .unwrap();

        // SQLite late-enlists the virtual table at only the deepest level.
        // Releasing that level must not erase the physical boundary for an
        // outer savepoint that existed before the first virtual-table write.
        coordinator.release("inner").unwrap();
        coordinator.rollback_to("outer").unwrap();
        coordinator
            .execute_dml(
                "INSERT INTO events VALUES
                 (?1, 3, 'kept-after-rollback', 3.0, x'03', NULL, 'gamma')",
                [fixture.keys[0]],
            )
            .unwrap();
        coordinator.release("outer").unwrap();
        coordinator.commit().unwrap();

        let shard = fixture.storage.open_shard(0).unwrap();
        assert_eq!(
            shard
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE tenant_id = ?1 AND event_id = 2",
                    [fixture.keys[0]],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            shard
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE tenant_id = ?1 AND event_id = 3",
                    [fixture.keys[0]],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert_eq!(fixture.physical_row_count(), 3);
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
    }

    #[test]
    fn writable_savepoint_callback_failures_abort_both_transaction_layers() {
        for operation in [
            write::SavepointOperation::Savepoint,
            write::SavepointOperation::Release,
            write::SavepointOperation::RollbackTo,
        ] {
            let fixture = Fixture::new();
            let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

            coordinator.begin().unwrap();
            coordinator
                .execute_dml(
                    "INSERT INTO events VALUES
                     (?1, 2, 'must-roll-back', 2.0, x'02', NULL, 'beta')",
                    [fixture.keys[0]],
                )
                .unwrap();
            if operation != write::SavepointOperation::Savepoint {
                coordinator.savepoint("guard").unwrap();
            }

            coordinator.fail_next_savepoint_operation_for_test(operation);
            let error = match operation {
                write::SavepointOperation::Savepoint => coordinator.savepoint("guard"),
                write::SavepointOperation::Release => coordinator.release("guard"),
                write::SavepointOperation::RollbackTo => coordinator.rollback_to("guard"),
            }
            .unwrap_err();

            assert_eq!(error.kind(), EngineErrorKind::StorageUnavailable);
            assert!(!coordinator.in_transaction(), "operation={operation:?}");
            assert!(
                !coordinator.write_state_for_test().has_active_child(),
                "operation={operation:?}"
            );
            assert_eq!(fixture.physical_row_count(), 2, "operation={operation:?}");

            // A proven rollback restores the wrapper to an idle, reusable
            // state; only an unprovable rollback marks it broken.
            coordinator
                .execute_dml(
                    "INSERT INTO events VALUES
                     (?1, 2, 'recovered', 2.0, x'02', NULL, 'beta')",
                    [fixture.keys[0]],
                )
                .unwrap();
            assert_eq!(fixture.physical_row_count(), 3, "operation={operation:?}");
        }
    }

    #[test]
    fn writable_savepoint_corruption_marks_storage_degraded_and_rolls_back() {
        let fixture = Fixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        coordinator.begin().unwrap();
        coordinator
            .execute_dml(
                "INSERT INTO events VALUES
                 (?1, 2, 'must-roll-back', 2.0, x'02', NULL, 'beta')",
                [fixture.keys[0]],
            )
            .unwrap();
        coordinator.savepoint("guard").unwrap();
        coordinator.fail_next_savepoint_corruption_for_test(write::SavepointOperation::Release);

        let error = coordinator.release("guard").unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert!(!coordinator.in_transaction());
        assert_eq!(
            fixture.storage.schema_gate_snapshot().state,
            crate::storage::SchemaGateState::Degraded
        );
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
        assert_eq!(
            Connection::open(fixture.temp.path().join("shards/0000.sqlite"))
                .unwrap()
                .query_row("SELECT COUNT(*) FROM events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );
    }

    #[test]
    fn writable_cross_shard_attempts_roll_back_without_partial_effects() {
        let fixture = Fixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        let error = coordinator
            .execute_dml(
                "INSERT INTO events VALUES
                 (?1, 2, 'first', 1.0, x'01', NULL, 'one'),
                 (?2, 2, 'second', 2.0, x'02', NULL, 'two')",
                params![fixture.keys[0], fixture.keys[1]],
            )
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
        assert_eq!(fixture.physical_row_count(), 2);

        let moved = coordinator
            .execute_dml(
                "UPDATE OR IGNORE events SET tenant_id = ?2
                 WHERE tenant_id = ?1 AND event_id = 1",
                params![fixture.keys[0], fixture.keys[1]],
            )
            .unwrap_err();
        assert_eq!(moved.kind(), EngineErrorKind::InvalidQuery);
        assert_eq!(fixture.physical_row_count(), 2);
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT tenant_id FROM events WHERE event_id = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            fixture.keys[0]
        );

        coordinator.begin().unwrap();
        coordinator
            .execute_dml(
                "INSERT INTO events VALUES (?1, 2, 'first', 1.0, x'01', NULL, 'one')",
                [fixture.keys[0]],
            )
            .unwrap();
        let error = coordinator
            .execute_dml(
                "INSERT INTO events VALUES (?1, 2, 'second', 2.0, x'02', NULL, 'two')",
                [fixture.keys[1]],
            )
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
        assert!(!coordinator.in_transaction());
        assert_eq!(fixture.physical_row_count(), 2);
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
    }

    #[test]
    fn writable_physical_checks_nullability_and_local_foreign_keys_are_authoritative() {
        let fixture = WritableFixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let inserted = coordinator
            .execute_dml(
                "INSERT INTO items VALUES (?1, 1, 1, 'first-code', 5)",
                [fixture.keys[0]],
            )
            .unwrap();
        assert_eq!(inserted.affected_rows, 1);
        assert_eq!(inserted.explicit_key, Some(Value::Int64(fixture.keys[0])));
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT code FROM items WHERE tenant_id = ?1 AND item_id = 1",
                    [fixture.keys[0]],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "first-code"
        );

        let check = coordinator
            .execute_dml(
                "INSERT INTO items VALUES (?1, 2, 1, 'bad-check', 0)",
                [fixture.keys[0]],
            )
            .unwrap_err();
        assert_eq!(check.kind(), EngineErrorKind::CheckViolation);

        let not_null = coordinator
            .execute_dml(
                "INSERT INTO items VALUES (?1, 2, 1, NULL, 1)",
                [fixture.keys[0]],
            )
            .unwrap_err();
        assert_eq!(not_null.kind(), EngineErrorKind::NotNullViolation);

        let foreign_key = coordinator
            .execute_dml(
                "INSERT INTO items VALUES (?1, 2, 999, 'bad-parent', 1)",
                [fixture.keys[0]],
            )
            .unwrap_err();
        assert_eq!(foreign_key.kind(), EngineErrorKind::ForeignKeyViolation);
        assert_eq!(fixture.item_count(), 1);
    }

    #[test]
    fn writable_two_tables_share_one_pinned_child_and_read_uncommitted_writes() {
        let fixture = WritableFixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        coordinator.begin().unwrap();
        coordinator
            .execute_dml(
                "INSERT INTO parents VALUES (?1, 2, 'new-parent')",
                [fixture.keys[0]],
            )
            .unwrap();
        coordinator
            .execute_dml(
                "INSERT INTO items VALUES (?1, 10, 2, 'new-item', 1)",
                [fixture.keys[0]],
            )
            .unwrap();
        coordinator
            .execute_dml(
                "UPDATE items SET quantity = 7
                 WHERE tenant_id = ?1 AND item_id = 10",
                [fixture.keys[0]],
            )
            .unwrap();
        coordinator.commit().unwrap();

        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT quantity FROM items WHERE tenant_id = ?1 AND item_id = 10",
                    [fixture.keys[0]],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            7
        );
    }

    #[test]
    fn writable_conflict_modes_preserve_sqlite_statement_semantics() {
        for (mode, expected_count) in [("ABORT", 1), ("FAIL", 2), ("IGNORE", 2)] {
            let fixture = WritableFixture::new();
            let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
            coordinator
                .execute_dml(
                    "INSERT INTO items VALUES (?1, 1, 1, 'duplicate', 1)",
                    [fixture.keys[0]],
                )
                .unwrap();
            let sql = format!(
                "INSERT OR {mode} INTO items VALUES
                 (?1, 2, 1, 'first-in-statement', 1),
                 (?1, 3, 1, 'duplicate', 1)"
            );
            let result = coordinator.execute_dml(&sql, [fixture.keys[0]]);
            if mode == "IGNORE" {
                assert_eq!(result.unwrap().affected_rows, 1);
            } else {
                assert_eq!(result.unwrap_err().kind(), EngineErrorKind::UniqueViolation);
            }
            assert_eq!(fixture.item_count(), expected_count, "mode={mode}");
        }

        let fixture = WritableFixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        coordinator
            .execute_dml(
                "INSERT INTO items VALUES (?1, 1, 1, 'duplicate', 1)",
                [fixture.keys[0]],
            )
            .unwrap();
        assert_eq!(
            coordinator
                .execute_dml(
                    "INSERT OR REPLACE INTO items VALUES (?1, 2, 1, 'duplicate', 9)",
                    [fixture.keys[0]],
                )
                .unwrap()
                .affected_rows,
            1
        );
        assert_eq!(fixture.item_count(), 1);
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT item_id FROM items WHERE tenant_id = ?1 AND code = 'duplicate'",
                    [fixture.keys[0]],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            2
        );

        let fixture = WritableFixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        coordinator
            .execute_dml(
                "INSERT INTO items VALUES (?1, 1, 1, 'duplicate', 1)",
                [fixture.keys[0]],
            )
            .unwrap();
        coordinator.begin().unwrap();
        coordinator
            .execute_dml(
                "INSERT INTO items VALUES (?1, 2, 1, 'before-rollback', 1)",
                [fixture.keys[0]],
            )
            .unwrap();
        let error = coordinator
            .execute_dml(
                "INSERT OR ROLLBACK INTO items VALUES (?1, 3, 1, 'duplicate', 1)",
                [fixture.keys[0]],
            )
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::UniqueViolation);
        assert!(!coordinator.in_transaction());
        assert_eq!(fixture.item_count(), 1);
    }

    #[test]
    fn writable_child_sql_overrides_physical_schema_conflict_policies() {
        let fixture = WritableFixture::new();
        let operation = fixture.storage.enter_schema_operation().unwrap();
        let registry =
            Registry::build_writable(fixture.storage.clone(), CursorLimits::default()).unwrap();
        drop(operation);
        let spec = registry
            .tables
            .values()
            .find(|spec| spec.name == "items")
            .unwrap();

        let physical = Connection::open_in_memory().unwrap();
        physical
            .execute_batch(
                "CREATE TABLE items (
                     tenant_id INTEGER NOT NULL,
                     item_id INTEGER NOT NULL,
                     parent_id INTEGER NOT NULL,
                     code TEXT NOT NULL,
                     quantity INTEGER NOT NULL,
                     PRIMARY KEY (tenant_id, item_id),
                     UNIQUE (tenant_id, code) ON CONFLICT IGNORE
                 );
                 INSERT INTO items VALUES (1, 1, 1, 'first', 1);",
            )
            .unwrap();

        let insert_abort = spec.insert_sql(ConflictMode::Abort).unwrap();
        assert!(insert_abort.starts_with("INSERT OR ABORT INTO"));
        assert_eq!(
            physical
                .execute(&insert_abort, params![1, 2, 1, "first", 1])
                .unwrap_err()
                .sqlite_error_code(),
            Some(ErrorCode::ConstraintViolation)
        );
        let insert_ignore = spec.insert_sql(ConflictMode::Ignore).unwrap();
        assert_eq!(insert_ignore, insert_abort);
        assert_eq!(
            physical
                .execute(&insert_ignore, params![1, 2, 1, "first", 1])
                .unwrap_err()
                .sqlite_error_code(),
            Some(ErrorCode::ConstraintViolation)
        );
        assert_eq!(
            physical
                .execute(
                    &spec.insert_sql(ConflictMode::Replace).unwrap(),
                    params![1, 2, 1, "first", 1],
                )
                .unwrap(),
            1
        );
        assert_eq!(
            physical
                .query_row("SELECT item_id FROM items", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );

        physical
            .execute_batch(
                "DELETE FROM items;
                 INSERT INTO items VALUES (1, 1, 1, 'first', 1);
                 INSERT INTO items VALUES (1, 2, 1, 'second', 1);",
            )
            .unwrap();
        let second_rowid = physical
            .query_row(
                "SELECT rowid FROM items WHERE tenant_id = 1 AND item_id = 2",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        let update_values = [
            ValueRef::Integer(1),
            ValueRef::Integer(2),
            ValueRef::Integer(1),
            ValueRef::Text(b"first"),
            ValueRef::Integer(1),
        ];
        let no_change = [true, true, true, false, true];
        let (update_abort, mut update_parameters) = spec
            .update_sql_and_values(&update_values, &no_change, ConflictMode::Abort)
            .unwrap();
        assert!(update_abort.starts_with("UPDATE OR ABORT"));
        update_parameters.push(RawCell::Integer(second_rowid));
        assert_eq!(
            physical
                .execute(
                    &update_abort,
                    rusqlite::params_from_iter(&update_parameters),
                )
                .unwrap_err()
                .sqlite_error_code(),
            Some(ErrorCode::ConstraintViolation)
        );

        let (update_replace, mut replace_parameters) = spec
            .update_sql_and_values(&update_values, &no_change, ConflictMode::Replace)
            .unwrap();
        assert!(update_replace.starts_with("UPDATE OR REPLACE"));
        replace_parameters.push(RawCell::Integer(second_rowid));
        assert_eq!(
            physical
                .execute(
                    &update_replace,
                    rusqlite::params_from_iter(&replace_parameters),
                )
                .unwrap(),
            1
        );
        assert_eq!(
            physical
                .query_row("SELECT item_id FROM items", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[test]
    fn writable_surface_rejects_global_multishard_ddl_returning_and_locator_mutation() {
        let fixture = Fixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        let global = coordinator
            .execute_dml("INSERT INTO countries VALUES ('CA', 'Canada')", [])
            .unwrap_err();
        assert_eq!(global.kind(), EngineErrorKind::InvalidQuery);
        for shard in 0..2 {
            assert_eq!(
                fixture
                    .storage
                    .open_shard(shard)
                    .unwrap()
                    .query_row("SELECT COUNT(*) FROM countries", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                1
            );
        }

        let scan = coordinator
            .execute_dml("UPDATE events SET payload = 'unsafe-scan'", [])
            .unwrap_err();
        assert_eq!(scan.kind(), EngineErrorKind::InvalidQuery);
        assert_eq!(fixture.physical_row_count(), 2);

        let returning = coordinator
            .execute_dml(
                "INSERT INTO events VALUES
                 (?1, 2, 'returning', 1.0, x'01', NULL, 'one')
                 RETURNING event_id",
                [fixture.keys[0]],
            )
            .unwrap_err();
        assert_eq!(returning.kind(), EngineErrorKind::Unsupported);
        assert_eq!(fixture.physical_row_count(), 2);

        for sql in [
            "DROP TABLE events",
            "ATTACH DATABASE ':memory:' AS escaped",
            "BEGIN",
        ] {
            assert_eq!(
                coordinator.execute_dml(sql, []).unwrap_err().kind(),
                EngineErrorKind::PermissionDenied,
                "sql={sql}"
            );
        }

        let locator = coordinator
            .execute_dml(
                "INSERT INTO events
                 (tenant_id, event_id, payload, amount, raw, optional, category,
                  __briskdb_locator)
                 VALUES (?1, 2, 'locator', 1.0, x'01', NULL, 'one', x'00')",
                [fixture.keys[0]],
            )
            .unwrap_err();
        assert_eq!(locator.kind(), EngineErrorKind::ReadOnly);
        assert_eq!(fixture.physical_row_count(), 2);

        let null_locator = coordinator
            .execute_dml(
                "UPDATE events
                 SET payload = 'must-not-land', __briskdb_locator = NULL
                 WHERE tenant_id = ?1 AND event_id = 1",
                [fixture.keys[0]],
            )
            .unwrap_err();
        assert_eq!(null_locator.kind(), EngineErrorKind::ReadOnly);
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT payload FROM events WHERE tenant_id = ?1 AND event_id = 1",
                    [fixture.keys[0]],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "zero"
        );

        // Rejections that did not poison a physical transaction leave the
        // wrapper reusable.
        assert_eq!(
            coordinator
                .execute_dml(
                    "INSERT INTO events VALUES
                     (?1, 2, 'after-errors', 1.0, x'01', NULL, 'one')",
                    [fixture.keys[0]],
                )
                .unwrap()
                .affected_rows,
            1
        );
    }

    #[test]
    fn writable_update_conflict_ignore_and_replace_are_delegated_physically() {
        let fixture = WritableFixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        for (item_id, code) in [(1, "first"), (2, "second")] {
            coordinator
                .execute_dml(
                    "INSERT INTO items VALUES (?1, ?2, 1, ?3, 1)",
                    params![fixture.keys[0], item_id, code],
                )
                .unwrap();
        }
        assert_eq!(
            coordinator
                .execute_dml(
                    "UPDATE OR IGNORE items SET code = 'first'
                     WHERE tenant_id = ?1 AND item_id = 2",
                    [fixture.keys[0]],
                )
                .unwrap()
                .affected_rows,
            0
        );
        assert_eq!(fixture.item_count(), 2);

        assert_eq!(
            coordinator
                .execute_dml(
                    "UPDATE OR REPLACE items SET code = 'first'
                     WHERE tenant_id = ?1 AND item_id = 2",
                    [fixture.keys[0]],
                )
                .unwrap()
                .affected_rows,
            1
        );
        assert_eq!(fixture.item_count(), 1);
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT item_id FROM items WHERE tenant_id = ?1 AND code = 'first'",
                    [fixture.keys[0]],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            2
        );
    }

    #[test]
    fn writable_multirow_replace_skips_a_later_invalidated_locator_and_counts_physical_updates() {
        let fixture = WritableFixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        for (item_id, code) in [(1, "first"), (2, "second")] {
            coordinator
                .execute_dml(
                    "INSERT INTO items VALUES (?1, ?2, 1, ?3, 1)",
                    params![fixture.keys[0], item_id, code],
                )
                .unwrap();
        }

        // Whichever materialized row SQLite updates first takes the other
        // row's unique code, so OR REPLACE removes the still-pending row.
        // Native SQLite then treats that later stale row identity as a no-op.
        let updated = coordinator
            .execute_dml(
                "UPDATE OR REPLACE items
                 SET code = CASE item_id
                     WHEN 1 THEN 'second'
                     ELSE 'first'
                 END
                 WHERE tenant_id = ?1 AND item_id IN (1, 2)",
                [fixture.keys[0]],
            )
            .unwrap();

        assert_eq!(updated.affected_rows, 1);
        assert_eq!(fixture.item_count(), 1);
    }

    #[test]
    fn writable_fk_cascade_skips_later_invalidated_locator_and_counts_direct_delete() {
        let temp = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(temp.path(), 2).unwrap();
        let mut migration = storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        storage
            .apply_schema_migration(
                "CREATE TABLE cascade_nodes (
                     tenant_id INTEGER NOT NULL,
                     node_id INTEGER NOT NULL,
                     parent_id INTEGER,
                     PRIMARY KEY (tenant_id, node_id),
                     FOREIGN KEY (tenant_id, parent_id)
                         REFERENCES cascade_nodes (tenant_id, node_id)
                         ON DELETE CASCADE
                 );",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();
        let database_id = storage.logical_catalog().default_database().id();
        storage
            .register_tables(vec![
                TableDeclaration::sharded(
                    database_id,
                    "cascade_nodes",
                    ShardKeyMetadata::new("tenant_id", ShardKeyType::Int64).unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
        let key = (1_i64..)
            .find(|key| storage.shard_for_key(key.to_string().as_bytes()) == 0)
            .unwrap();
        let shard = storage.open_shard(0).unwrap();
        shard
            .execute("INSERT INTO cascade_nodes VALUES (?1, 1, NULL)", [key])
            .unwrap();
        shard
            .execute("INSERT INTO cascade_nodes VALUES (?1, 2, 1)", [key])
            .unwrap();
        shard
            .execute(
                "UPDATE cascade_nodes SET parent_id = 2
                 WHERE tenant_id = ?1 AND node_id = 1",
                [key],
            )
            .unwrap();
        drop(shard);

        let mut coordinator = WriteCoordinator::open(storage.clone()).unwrap();
        let deleted = coordinator
            .execute_dml("DELETE FROM cascade_nodes WHERE tenant_id = ?1", [key])
            .unwrap();

        // The first direct delete cascades to the second materialized row.
        // Like sqlite3_changes(), affected_rows excludes the FK side effect.
        assert_eq!(deleted.affected_rows, 1);
        assert_eq!(
            storage
                .open_shard(0)
                .unwrap()
                .query_row("SELECT COUNT(*) FROM cascade_nodes", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn writable_affected_rows_are_per_statement_for_bulk_single_shard_dml() {
        let fixture = WritableFixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let inserted = coordinator
            .execute_dml(
                "INSERT INTO items VALUES
                 (?1, 1, 1, 'one', 1),
                 (?1, 2, 1, 'two', 2),
                 (?1, 3, 1, 'three', 3)",
                [fixture.keys[0]],
            )
            .unwrap();
        assert_eq!(inserted.affected_rows, 3);
        assert_eq!(inserted.explicit_key, Some(Value::Int64(fixture.keys[0])));
        assert_eq!(
            coordinator
                .execute_dml(
                    "UPDATE items SET quantity = quantity + 10 WHERE tenant_id = ?1",
                    [fixture.keys[0]],
                )
                .unwrap()
                .affected_rows,
            3
        );
        assert_eq!(
            coordinator
                .execute_dml("DELETE FROM items WHERE tenant_id = ?1", [fixture.keys[0]])
                .unwrap()
                .affected_rows,
            3
        );
        assert_eq!(fixture.item_count(), 0);
    }

    #[test]
    fn writable_locators_cover_without_rowid_text_blob_and_explicit_native_ids() {
        let fixture = TypedRoutingFixture::new(2);
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        assert_eq!(
            coordinator
                .execute_dml(
                    "UPDATE text_events SET payload = 'text-updated' WHERE id = ?1",
                    [&fixture.text_keys[0]],
                )
                .unwrap()
                .affected_rows,
            1
        );
        assert_eq!(
            coordinator
                .execute_dml(
                    "DELETE FROM blob_events WHERE id = ?1",
                    [&fixture.blob_keys[1]]
                )
                .unwrap()
                .affected_rows,
            1
        );
        let decoded = NativeRangeV1Id::decode(fixture.native_ids[0]).unwrap();
        let second_native = NativeRangeV1Id::new(decoded.owner(), 2).unwrap().encode();
        let inserted = coordinator
            .execute_dml(
                "INSERT INTO native_events VALUES (?1, 'native-explicit')",
                [second_native],
            )
            .unwrap();
        assert_eq!(inserted.explicit_key, Some(Value::Int64(second_native)));

        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT payload FROM text_events WHERE id = ?1",
                    [&fixture.text_keys[0]],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "text-updated"
        );
        assert_eq!(
            fixture
                .storage
                .open_shard(1)
                .unwrap()
                .query_row("SELECT COUNT(*) FROM blob_events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT payload FROM native_events WHERE id = ?1",
                    [second_native],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "native-explicit"
        );
    }

    #[test]
    fn writable_native_generation_captures_returning_id_on_selected_child() {
        let fixture = TypedRoutingFixture::new(2);
        let table_id = fixture.native_table_id();
        let owner = fixture
            .storage
            .allocation_owner_map()
            .unwrap()
            .owner_for_physical_shard(1)
            .unwrap();
        let expected = NativeRangeV1Id::new(owner, 2).unwrap().encode();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        let result = coordinator
            .execute_generated_dml(
                "INSERT INTO native_events (payload) VALUES (?1)",
                ["generated-on-one"],
                table_id,
                1,
            )
            .unwrap();

        assert_eq!(result.affected_rows(), 1);
        assert_eq!(result.shard(), Some(1));
        assert_eq!(result.explicit_key(), None);
        assert_eq!(
            result.generated_key(),
            Some(&crate::core::GeneratedKey::new(
                "id",
                Value::Int64(expected)
            ))
        );
        assert_eq!(coordinator.last_insert_rowid_for_test(), expected);
        assert_eq!(
            fixture
                .storage
                .open_shard(1)
                .unwrap()
                .query_row(
                    "SELECT payload FROM native_events WHERE id = ?1",
                    [expected],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "generated-on-one"
        );
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );
    }

    #[test]
    fn writable_native_generation_auto_selects_round_robin_active_owners() {
        let fixture = TypedRoutingFixture::new(2);
        let table_id = fixture.native_table_id();
        let owners = fixture.storage.allocation_owner_map().unwrap().clone();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        for expected_shard in 0..2 {
            let result = coordinator
                .execute_generated_dml_auto(
                    "INSERT INTO native_events (payload) VALUES (?1)",
                    [format!("auto-{expected_shard}")],
                    table_id,
                )
                .unwrap();
            assert_eq!(result.shard(), Some(expected_shard));
            let generated = match &result.generated_key().unwrap().value {
                Value::Int64(value) => *value,
                value => panic!("unexpected generated value: {value:?}"),
            };
            let decoded = NativeRangeV1Id::decode(generated).unwrap();
            assert_eq!(owners.physical_shard(decoded.owner()), Some(expected_shard));
            assert!(owners.owner_is_active(decoded.owner()));
        }
    }

    #[test]
    fn writable_hilo_generation_leases_once_and_hash_routes_each_id() {
        let fixture = HiloFixture::new(4);
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let mut generated = Vec::new();
        for index in 0..10 {
            let result = coordinator
                .execute_generated_dml_auto(
                    "INSERT INTO hilo_events (payload) VALUES (?1)",
                    [format!("hilo-{index}")],
                    fixture.table_id,
                )
                .unwrap();
            let id = match &result.generated_key().unwrap().value {
                Value::Int64(id) => *id,
                value => panic!("unexpected generated value: {value:?}"),
            };
            assert_eq!(
                result.shard(),
                Some(
                    fixture
                        .storage
                        .shard_for_key(&canonical_shard_key_bytes(CanonicalShardKeyRef::Int64(id)))
                )
            );
            assert_eq!(
                crate::core::generated_id::HiloV1Id::decode(id)
                    .unwrap()
                    .sequence(),
                u64::try_from(index + 1).unwrap()
            );
            generated.push(id);
        }
        generated.sort_unstable();
        generated.dedup();
        assert_eq!(generated.len(), 10);
        assert_eq!(fixture.row_count(), 10);

        let manifest = Connection::open(fixture.storage.root.join("manifest.sqlite")).unwrap();
        let (next, fence): (i64, i64) = manifest
            .query_row(
                "SELECT next_sequence, fence_token FROM briskdb_hilo_leases WHERE table_id = ?1",
                [i64::try_from(fixture.table_id).unwrap()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(next, 4_097);
        assert_eq!(fence, 1);
    }

    #[test]
    fn hilo_generated_write_process_child() {
        let Ok(root) = std::env::var("BRISKDB_HILO_WRITE_PROCESS_ROOT") else {
            return;
        };
        let storage = Storage::open(root, 4).unwrap();
        let table_id = storage
            .logical_catalog()
            .table("default", "hilo_events")
            .unwrap()
            .unwrap()
            .id()
            .get();
        let ready =
            std::path::PathBuf::from(std::env::var("BRISKDB_HILO_WRITE_PROCESS_READY").unwrap());
        let go = std::path::PathBuf::from(std::env::var("BRISKDB_HILO_WRITE_PROCESS_GO").unwrap());
        std::fs::write(&ready, b"ready").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !go.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(go.exists(), "parent did not release hilo_v1 write child");

        let mut coordinator = WriteCoordinator::open(storage).unwrap();
        let payload = format!("process-{}", std::process::id());
        let result = coordinator
            .execute_generated_dml_auto(
                "INSERT INTO hilo_events (payload) VALUES (?1)",
                [payload],
                table_id,
            )
            .unwrap();
        let id = match &result.generated_key().unwrap().value {
            Value::Int64(value) => *value,
            value => panic!("unexpected generated value: {value:?}"),
        };
        std::fs::write(
            std::env::var("BRISKDB_HILO_WRITE_PROCESS_OUTPUT").unwrap(),
            format!("{id},{}", result.shard().unwrap()),
        )
        .unwrap();
    }

    #[test]
    fn competing_processes_insert_unique_hilo_ids_on_their_hash_routed_shards() {
        let fixture = HiloFixture::new(4);
        let go = fixture._temp.path().join("hilo-write-go");
        let mut children = Vec::new();
        let mut ready_paths = Vec::new();
        let mut output_paths = Vec::new();
        for index in 0..3 {
            let ready = fixture
                ._temp
                .path()
                .join(format!("hilo-write-ready-{index}"));
            let output = fixture
                ._temp
                .path()
                .join(format!("hilo-write-output-{index}"));
            let child = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("storage::sharded_vtab::tests::hilo_generated_write_process_child")
                .arg("--nocapture")
                .env("BRISKDB_HILO_WRITE_PROCESS_ROOT", fixture._temp.path())
                .env("BRISKDB_HILO_WRITE_PROCESS_READY", &ready)
                .env("BRISKDB_HILO_WRITE_PROCESS_GO", &go)
                .env("BRISKDB_HILO_WRITE_PROCESS_OUTPUT", &output)
                .spawn()
                .unwrap();
            children.push(child);
            ready_paths.push(ready);
            output_paths.push(output);
        }

        let deadline = Instant::now() + Duration::from_secs(10);
        while ready_paths.iter().any(|path| !path.exists()) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(ready_paths.iter().all(|path| path.exists()));
        std::fs::write(&go, b"go").unwrap();
        for mut child in children {
            assert!(child.wait().unwrap().success());
        }

        let allocations = output_paths
            .iter()
            .map(|path| {
                let output = std::fs::read_to_string(path).unwrap();
                let (id, shard) = output.split_once(',').unwrap();
                (id.parse::<i64>().unwrap(), shard.parse::<u16>().unwrap())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            allocations
                .iter()
                .map(|(id, _)| *id)
                .collect::<BTreeSet<_>>()
                .len(),
            allocations.len()
        );
        for (id, shard) in allocations {
            assert_eq!(
                shard,
                fixture
                    .storage
                    .shard_for_key(&canonical_shard_key_bytes(CanonicalShardKeyRef::Int64(id)))
            );
            for candidate in 0..4 {
                assert_eq!(
                    fixture
                        .storage
                        .open_shard(candidate)
                        .unwrap()
                        .query_row(
                            "SELECT COUNT(*) FROM hilo_events WHERE id = ?1",
                            [id],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    i64::from(candidate == shard)
                );
            }
        }
        assert_eq!(fixture.row_count(), 3);
    }

    #[test]
    fn writable_hilo_constraint_rollback_and_ignore_burn_ids_without_reuse() {
        let fixture = HiloFixture::new(2);
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let first = coordinator
            .execute_generated_dml_auto(
                "INSERT INTO hilo_events (payload) VALUES ('first')",
                [],
                fixture.table_id,
            )
            .unwrap();
        let first = match &first.generated_key().unwrap().value {
            Value::Int64(value) => *value,
            value => panic!("unexpected generated value: {value:?}"),
        };

        let ignored = coordinator
            .execute_generated_dml_auto(
                "INSERT OR IGNORE INTO hilo_events (payload) VALUES ('reject')",
                [],
                fixture.table_id,
            )
            .unwrap();
        assert_eq!(ignored.affected_rows(), 0);
        assert_eq!(ignored.generated_key(), None);

        let error = coordinator
            .execute_generated_dml_auto(
                "INSERT INTO hilo_events (payload) VALUES ('reject')",
                [],
                fixture.table_id,
            )
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::CheckViolation);

        let next = coordinator
            .execute_generated_dml_auto(
                "INSERT INTO hilo_events (payload) VALUES ('next')",
                [],
                fixture.table_id,
            )
            .unwrap();
        let next = match &next.generated_key().unwrap().value {
            Value::Int64(value) => *value,
            value => panic!("unexpected generated value: {value:?}"),
        };
        let first_sequence = crate::core::generated_id::HiloV1Id::decode(first)
            .unwrap()
            .sequence();
        let next_sequence = crate::core::generated_id::HiloV1Id::decode(next)
            .unwrap()
            .sequence();
        assert_eq!(next_sequence, first_sequence + 3);
        assert_eq!(fixture.row_count(), 2);
    }

    #[test]
    fn writable_hilo_cancellation_after_allocation_burns_the_id_without_reuse() {
        let fixture = HiloFixture::new(2);
        let coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let cancellation = coordinator.cancellation_handle();
        let mut gate = coordinator.install_generated_target_gate_for_test();
        let table_id = fixture.table_id;
        let worker = thread::spawn(move || {
            let mut coordinator = coordinator;
            let result = coordinator.execute_generated_dml_auto(
                "INSERT INTO hilo_events (payload) VALUES ('cancelled')",
                [],
                table_id,
            );
            (coordinator, result)
        });

        gate.wait_until_started();
        cancellation.cancel();
        gate.release();

        let (mut coordinator, result) = worker.join().unwrap();
        let error = result.unwrap_err();
        assert_eq!(
            error.kind(),
            EngineErrorKind::Cancelled,
            "unexpected cancellation error: {error}"
        );
        assert_eq!(fixture.row_count(), 0);

        let after = coordinator
            .execute_generated_dml_auto(
                "INSERT INTO hilo_events (payload) VALUES ('after-cancel')",
                [],
                fixture.table_id,
            )
            .unwrap();
        let generated = match &after.generated_key().unwrap().value {
            Value::Int64(value) => *value,
            value => panic!("unexpected generated value: {value:?}"),
        };
        assert_eq!(
            crate::core::generated_id::HiloV1Id::decode(generated)
                .unwrap()
                .sequence(),
            2
        );
        assert_eq!(fixture.row_count(), 1);
    }

    #[test]
    fn writable_hilo_rejects_multirow_and_explicit_allocator_ids_without_mutation() {
        let fixture = HiloFixture::new(2);
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let error = coordinator
            .execute_generated_dml_auto(
                "INSERT INTO hilo_events (payload) VALUES ('one'), ('two')",
                [],
                fixture.table_id,
            )
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            EngineErrorKind::InvalidQuery | EngineErrorKind::Unsupported
        ));
        assert_eq!(fixture.row_count(), 0);
        drop(coordinator);

        let explicit = crate::core::generated_id::HiloV1Id::new(99)
            .unwrap()
            .encode();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let error = coordinator
            .execute_dml(
                "INSERT INTO hilo_events VALUES (?1, 'explicit')",
                [explicit],
            )
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            EngineErrorKind::InvalidQuery | EngineErrorKind::FailedPrecondition
        ));
        assert_eq!(fixture.row_count(), 0);
    }

    #[test]
    fn writable_hilo_rejects_caller_namespaces_without_degrading_storage() {
        let fixture = HiloFixture::new(2);
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        coordinator
            .execute_dml("INSERT INTO hilo_events VALUES (1, 'legacy')", [])
            .unwrap();
        drop(coordinator);

        for malformed in [
            crate::core::generated_id::HiloV1Id::new(99)
                .unwrap()
                .encode(),
            crate::core::generated_id::HILO_V1_FORMAT_MARKER as i64,
            crate::core::generated_id::NATIVE_RANGE_V1_FORMAT_MARKER as i64,
            i64::MAX,
        ] {
            let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
            let error = coordinator
                .execute_dml(
                    "INSERT INTO hilo_events VALUES (?1, 'malformed')",
                    [malformed],
                )
                .unwrap_err();
            assert_ne!(error.kind(), EngineErrorKind::DataCorruption);
            drop(coordinator);

            let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
            let error = coordinator
                .execute_dml("UPDATE hilo_events SET id = ?1 WHERE id = 1", [malformed])
                .unwrap_err();
            assert_ne!(error.kind(), EngineErrorKind::DataCorruption);
        }

        assert_eq!(fixture.row_count(), 1);
        let reopened = Storage::open(&fixture.storage.root, 2).unwrap();
        assert!(
            reopened.generated_id_policy_is_active(
                crate::core::TableId::new(fixture.table_id).unwrap()
            )
        );
    }

    #[test]
    fn writable_native_generation_auto_skips_exhausted_owners() {
        let fixture = TypedRoutingFixture::new(2);
        let owners = fixture.storage.allocation_owner_map().unwrap();
        let exhausted_owner = owners.owner_for_physical_shard(0).unwrap();
        let ceiling = crate::core::generated_id::native_range_v1_sequence_ceiling(exhausted_owner);
        fixture
            .storage
            .open_shard(0)
            .unwrap()
            .execute(
                "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'native_events'",
                [ceiling],
            )
            .unwrap();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        let result = coordinator
            .execute_generated_dml_auto(
                "INSERT INTO native_events (payload) VALUES ('fallback')",
                [],
                fixture.native_table_id(),
            )
            .unwrap();
        assert_eq!(result.shard(), Some(1));
        let generated = match &result.generated_key().unwrap().value {
            Value::Int64(value) => *value,
            value => panic!("unexpected generated value: {value:?}"),
        };
        assert_eq!(
            owners.physical_shard(NativeRangeV1Id::decode(generated).unwrap().owner()),
            Some(1)
        );
    }

    #[test]
    fn writable_native_generation_auto_retries_when_selected_owner_exhausts_before_lock() {
        let fixture = TypedRoutingFixture::new(2);
        let owners = fixture.storage.allocation_owner_map().unwrap().clone();
        let first_owner = owners.owner_for_physical_shard(0).unwrap();
        let ceiling = crate::core::generated_id::native_range_v1_sequence_ceiling(first_owner);
        fixture
            .storage
            .open_shard(0)
            .unwrap()
            .execute(
                "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'native_events'",
                [ceiling - 1],
            )
            .unwrap();

        let coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let mut gate = coordinator.install_generated_target_gate_for_test();
        let table_id = fixture.native_table_id();
        let worker = thread::spawn(move || {
            let mut coordinator = coordinator;
            coordinator.execute_generated_dml_auto(
                "INSERT INTO native_events (payload) VALUES ('raced-fallback')",
                [],
                table_id,
            )
        });

        gate.wait_until_started();
        let competing = fixture
            .storage
            .open_shard(0)
            .unwrap()
            .query_row(
                "INSERT INTO native_events (payload) VALUES ('last-on-zero') RETURNING id",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(competing, ceiling);
        gate.release();

        let result = worker.join().unwrap().unwrap();
        assert_eq!(result.affected_rows(), 1);
        assert_eq!(result.shard(), Some(1));
        let generated = match &result.generated_key().unwrap().value {
            Value::Int64(value) => *value,
            value => panic!("unexpected generated value: {value:?}"),
        };
        assert_eq!(
            owners.physical_shard(NativeRangeV1Id::decode(generated).unwrap().owner()),
            Some(1)
        );
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM native_events WHERE payload = 'raced-fallback'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            fixture
                .storage
                .open_shard(1)
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM native_events WHERE payload = 'raced-fallback'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn writable_native_generation_auto_reports_all_active_owners_exhausted() {
        let fixture = TypedRoutingFixture::new(2);
        for shard in 0..2 {
            let owner = fixture
                .storage
                .allocation_owner_map()
                .unwrap()
                .owner_for_physical_shard(shard)
                .unwrap();
            let ceiling = crate::core::generated_id::native_range_v1_sequence_ceiling(owner);
            fixture
                .storage
                .open_shard(shard)
                .unwrap()
                .execute(
                    "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'native_events'",
                    [ceiling],
                )
                .unwrap();
        }
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        let error = coordinator
            .execute_generated_dml_auto(
                "INSERT INTO native_events (payload) VALUES ('all-exhausted')",
                [],
                fixture.native_table_id(),
            )
            .unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert!(error.diagnostic().contains("exhausted every active"));
        for shard in 0..2 {
            assert_eq!(
                fixture
                    .storage
                    .open_shard(shard)
                    .unwrap()
                    .query_row(
                        "SELECT COUNT(*) FROM native_events WHERE payload = 'all-exhausted'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                0
            );
        }
    }

    #[test]
    fn writable_native_generation_requires_an_explicit_intent() {
        let fixture = TypedRoutingFixture::new(2);
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        let error = coordinator
            .execute_dml(
                "INSERT INTO native_events (payload) VALUES ('not-authorized')",
                [],
            )
            .unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::NotNullViolation);
        assert_eq!(
            (0..2)
                .map(|shard| fixture
                    .storage
                    .open_shard(shard)
                    .unwrap()
                    .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap())
                .sum::<i64>(),
            2
        );
    }

    #[test]
    fn writable_native_generation_rejects_multiple_callbacks_and_rolls_back() {
        let fixture = TypedRoutingFixture::new(2);
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        let error = coordinator
            .execute_generated_dml(
                "INSERT INTO native_events (payload) VALUES ('first'), ('second')",
                [],
                fixture.native_table_id(),
                0,
            )
            .unwrap_err();

        // The virtual-table module preserves the rejection diagnostic; the
        // outer SQLite statement classifies a module callback rejection as an
        // invalid query.
        assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
        assert!(
            error
                .to_string()
                .contains("multi-row native generated INSERT")
        );
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );
    }

    #[test]
    fn writable_native_generation_rollback_discards_row_and_result_state() {
        let fixture = TypedRoutingFixture::new(2);
        let table_id = fixture.native_table_id();
        let owner = fixture
            .storage
            .allocation_owner_map()
            .unwrap()
            .owner_for_physical_shard(0)
            .unwrap();
        let rolled_back_id = NativeRangeV1Id::new(owner, 2).unwrap().encode();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        coordinator.begin().unwrap();
        let pending = coordinator
            .execute_generated_dml(
                "INSERT INTO native_events (payload) VALUES ('rolled-back')",
                [],
                table_id,
                0,
            )
            .unwrap();
        assert_eq!(
            pending.generated_key().unwrap().value,
            Value::Int64(rolled_back_id)
        );
        coordinator.rollback().unwrap();

        let committed = coordinator
            .execute_generated_dml(
                "INSERT INTO native_events (payload) VALUES ('committed')",
                [],
                table_id,
                0,
            )
            .unwrap();
        assert_eq!(
            committed.generated_key().unwrap().value,
            Value::Int64(rolled_back_id)
        );
        let connection = fixture.storage.open_shard(0).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM native_events WHERE payload = 'rolled-back'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM native_events WHERE payload = 'committed'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn writable_native_generation_supports_id_only_default_values() {
        let fixture = TypedRoutingFixture::new(2);
        let owner = fixture
            .storage
            .allocation_owner_map()
            .unwrap()
            .owner_for_physical_shard(0)
            .unwrap();
        let expected = NativeRangeV1Id::new(owner, 1).unwrap().encode();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        let result = coordinator
            .execute_generated_dml(
                "INSERT INTO native_id_only DEFAULT VALUES",
                [],
                fixture.native_id_only_table_id(),
                0,
            )
            .unwrap();

        assert_eq!(result.generated_key().unwrap().column, "id");
        assert_eq!(
            result.generated_key().unwrap().value,
            Value::Int64(expected)
        );
        assert_eq!(result.shard(), Some(0));
    }

    #[test]
    fn writable_native_generation_constraint_failure_does_not_leak_a_key() {
        let fixture = TypedRoutingFixture::new(2);
        let table_id = fixture.native_table_id();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        let error = coordinator
            .execute_generated_dml(
                "INSERT INTO native_events (payload) VALUES (NULL)",
                [],
                table_id,
                0,
            )
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::NotNullViolation);

        let successful = coordinator
            .execute_generated_dml(
                "INSERT INTO native_events (payload) VALUES ('after-constraint')",
                [],
                table_id,
                0,
            )
            .unwrap();
        assert!(successful.generated_key().is_some());
        assert_eq!(successful.affected_rows(), 1);
    }

    #[test]
    fn writable_native_generation_commit_failure_never_acknowledges_the_key() {
        let fixture = TypedRoutingFixture::new(2);
        let table_id = fixture.native_table_id();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        coordinator.fail_next_commit_for_test();

        let error = coordinator
            .execute_generated_dml(
                "INSERT INTO native_events (payload) VALUES ('failed-commit')",
                [],
                table_id,
                0,
            )
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::StorageUnavailable);
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM native_events WHERE payload = 'failed-commit'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );

        let recovered = WriteCoordinator::open(fixture.storage.clone())
            .unwrap()
            .execute_generated_dml(
                "INSERT INTO native_events (payload) VALUES ('after-failed-commit')",
                [],
                table_id,
                0,
            )
            .unwrap();
        assert!(recovered.generated_key().is_some());
    }

    #[test]
    fn writable_native_generation_or_ignore_exposes_no_key_and_rolls_back_its_sequence_attempt() {
        let TypedRoutingFixture { _temp, storage, .. } = TypedRoutingFixture::new(2);
        let table_id = storage
            .logical_catalog()
            .table("default", "native_events")
            .unwrap()
            .unwrap()
            .id()
            .get();
        let owner = storage
            .allocation_owner_map()
            .unwrap()
            .owner_for_physical_shard(0)
            .unwrap();
        let committed_id = NativeRangeV1Id::new(owner, 1).unwrap().encode();
        let next_id = NativeRangeV1Id::new(owner, 2).unwrap().encode();
        let mut coordinator = WriteCoordinator::open(storage.clone()).unwrap();

        let ignored = coordinator
            .execute_generated_dml(
                "INSERT OR IGNORE INTO native_events (payload) VALUES (NULL)",
                [],
                table_id,
                0,
            )
            .unwrap();

        assert_eq!(ignored.affected_rows(), 0);
        assert_eq!(ignored.shard(), None);
        assert_eq!(ignored.generated_key(), None);
        assert_eq!(ignored.explicit_key(), None);
        drop(coordinator);

        let sequence = storage
            .open_shard(0)
            .unwrap()
            .query_row(
                "SELECT seq FROM sqlite_sequence WHERE name = 'native_events'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        // The facade delegates non-REPLACE physical conflicts as OR ABORT and
        // lets the outer virtual-table statement apply IGNORE. The failed
        // child statement therefore rolls back its sequence attempt instead
        // of committing the gap that direct SQLite OR IGNORE would retain.
        assert_eq!(sequence, committed_id);
        assert_eq!(
            storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM native_events WHERE id = ?1",
                    [next_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );

        drop(storage);
        let reopened = Storage::open(_temp.path(), 2).unwrap();
        assert_eq!(
            reopened
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT seq FROM sqlite_sequence WHERE name = 'native_events'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            committed_id
        );
        let inserted = WriteCoordinator::open(reopened.clone())
            .unwrap()
            .execute_generated_dml(
                "INSERT INTO native_events (payload) VALUES ('after-ignored-gap')",
                [],
                table_id,
                0,
            )
            .unwrap();
        assert_eq!(
            inserted.generated_key().unwrap().value,
            Value::Int64(next_id)
        );
        assert_ne!(next_id, committed_id);
        let connection = reopened.open_shard(0).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT payload FROM native_events WHERE id = ?1",
                    [next_id],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "after-ignored-gap"
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT seq FROM sqlite_sequence WHERE name = 'native_events'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            next_id
        );
    }

    #[test]
    fn writable_native_generation_preflight_mismatch_does_not_mutate() {
        let fixture = TypedRoutingFixture::new(2);
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        let outside = coordinator
            .execute_generated_dml(
                "INSERT INTO native_events (payload) VALUES ('outside')",
                [],
                fixture.native_table_id(),
                2,
            )
            .unwrap_err();
        assert_eq!(outside.kind(), EngineErrorKind::FailedPrecondition);

        let explicit = coordinator
            .execute_generated_dml(
                "INSERT INTO native_events (id, payload) VALUES (?1, 'explicit')",
                [fixture.native_ids[0]],
                fixture.native_table_id(),
                0,
            )
            .unwrap_err();
        assert_eq!(explicit.kind(), EngineErrorKind::InvalidQuery);
        assert!(
            explicit
                .to_string()
                .contains("unexpectedly supplied an explicit key")
        );
        assert_eq!(
            (0..2)
                .map(|shard| fixture
                    .storage
                    .open_shard(shard)
                    .unwrap()
                    .query_row(
                        "SELECT COUNT(*) FROM native_events WHERE payload IN ('outside', 'explicit')",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap())
                .sum::<i64>(),
            0
        );
    }

    #[test]
    fn writable_native_generated_identity_cannot_be_updated_even_in_place() {
        let fixture = TypedRoutingFixture::new(2);
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        let error = coordinator
            .execute_dml(
                "UPDATE native_events SET id = id WHERE id = ?1",
                [fixture.native_ids[0]],
            )
            .unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::ReadOnly);
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM native_events WHERE id = ?1",
                    [fixture.native_ids[0]],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn writable_native_generation_reopens_with_the_persisted_sequence() {
        let TypedRoutingFixture { _temp, storage, .. } = TypedRoutingFixture::new(2);
        let table_id = storage
            .logical_catalog()
            .table("default", "native_events")
            .unwrap()
            .unwrap()
            .id()
            .get();
        let owner = storage
            .allocation_owner_map()
            .unwrap()
            .owner_for_physical_shard(1)
            .unwrap();
        let first_expected = NativeRangeV1Id::new(owner, 2).unwrap().encode();
        let second_expected = NativeRangeV1Id::new(owner, 3).unwrap().encode();

        let first = WriteCoordinator::open(storage.clone())
            .unwrap()
            .execute_generated_dml(
                "INSERT INTO native_events (payload) VALUES ('before-reopen')",
                [],
                table_id,
                1,
            )
            .unwrap();
        assert_eq!(
            first.generated_key().unwrap().value,
            Value::Int64(first_expected)
        );
        drop(storage);

        let reopened = Storage::open(_temp.path(), 2).unwrap();
        assert_eq!(
            reopened
                .logical_catalog()
                .table("default", "native_events")
                .unwrap()
                .unwrap()
                .id()
                .get(),
            table_id
        );
        let second = WriteCoordinator::open(reopened.clone())
            .unwrap()
            .execute_generated_dml(
                "INSERT INTO native_events (payload) VALUES ('after-reopen')",
                [],
                table_id,
                1,
            )
            .unwrap();
        assert_eq!(
            second.generated_key().unwrap().value,
            Value::Int64(second_expected)
        );
        assert_eq!(
            reopened
                .open_shard(1)
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM native_events
                     WHERE payload IN ('before-reopen', 'after-reopen')",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            2
        );
    }

    #[test]
    fn writable_native_generation_stops_at_the_owner_ceiling() {
        let fixture = TypedRoutingFixture::new(2);
        let owner = fixture
            .storage
            .allocation_owner_map()
            .unwrap()
            .owner_for_physical_shard(0)
            .unwrap();
        let ceiling = crate::core::generated_id::native_range_v1_sequence_ceiling(owner);
        fixture
            .storage
            .open_shard(0)
            .unwrap()
            .execute(
                "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'native_events'",
                [ceiling],
            )
            .unwrap();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();

        let error = coordinator
            .execute_generated_dml(
                "INSERT INTO native_events (payload) VALUES ('outside-range')",
                [],
                fixture.native_table_id(),
                0,
            )
            .unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM native_events WHERE payload = 'outside-range'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn writable_native_auto_generation_is_concurrent_unique_and_uses_every_wal() {
        const ROUNDS_PER_SHARD: usize = 3;

        for shard_count in [2_u16, 4, 8, 10] {
            let fixture = TypedRoutingFixture::new(shard_count);
            let table_id = fixture.native_table_id();

            // Pin a pre-write read snapshot on every physical database. This
            // prevents a closing writer from resetting its WAL before the
            // assertions below can prove that each independent file received
            // frames from this concurrent workload.
            let mut wal_readers = Vec::with_capacity(usize::from(shard_count));
            for shard in 0..shard_count {
                let path = fixture
                    ._temp
                    .path()
                    .join("shards")
                    .join(format!("{shard:04}.sqlite"));
                let reader = Connection::open(path).unwrap();
                reader
                    .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                    .unwrap();
                reader.execute_batch("BEGIN DEFERRED").unwrap();
                reader
                    .query_row("SELECT COUNT(*) FROM native_events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap();
                wal_readers.push(reader);
            }

            let worker_count = usize::from(shard_count);
            let inserts_per_worker = worker_count * ROUNDS_PER_SHARD;
            let barrier = Arc::new(std::sync::Barrier::new(worker_count));
            let mut workers = Vec::with_capacity(worker_count);
            for worker in 0..worker_count {
                let storage = fixture.storage.clone();
                let barrier = Arc::clone(&barrier);
                workers.push(thread::spawn(move || {
                    let mut coordinator = WriteCoordinator::open(storage).unwrap();
                    barrier.wait();
                    (0..inserts_per_worker)
                        .map(|insert| {
                            let result = coordinator
                                .execute_generated_dml_auto(
                                    "INSERT INTO native_events (payload) VALUES (?1)",
                                    [format!("auto-concurrent-{worker}-{insert}")],
                                    table_id,
                                )
                                .unwrap();
                            let shard = result.shard().expect("generated write selected a shard");
                            let generated = match &result.generated_key().unwrap().value {
                                Value::Int64(value) => *value,
                                value => panic!("unexpected generated value: {value:?}"),
                            };
                            (shard, generated)
                        })
                        .collect::<Vec<_>>()
                }));
            }

            let results = workers
                .into_iter()
                .flat_map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>();
            let expected_total = worker_count * inserts_per_worker;
            assert_eq!(results.len(), expected_total);

            let owners = fixture.storage.allocation_owner_map().unwrap();
            let mut unique_ids = BTreeSet::new();
            let mut seen_shards = BTreeSet::new();
            let mut rows_per_shard = vec![0_usize; worker_count];
            let mut maximum_per_shard = vec![i64::MIN; worker_count];
            for (shard, generated) in results {
                assert!(
                    unique_ids.insert(generated),
                    "duplicate generated ID {generated} with {shard_count} shards"
                );
                seen_shards.insert(shard);
                rows_per_shard[usize::from(shard)] += 1;
                maximum_per_shard[usize::from(shard)] =
                    maximum_per_shard[usize::from(shard)].max(generated);

                let decoded = NativeRangeV1Id::decode(generated).unwrap();
                assert_eq!(owners.physical_shard(decoded.owner()), Some(shard));
                assert!(owners.owner_is_active(decoded.owner()));
            }
            assert_eq!(unique_ids.len(), expected_total);
            assert_eq!(seen_shards, (0..shard_count).collect::<BTreeSet<_>>());

            let expected_per_shard = worker_count * ROUNDS_PER_SHARD;
            for shard in 0..shard_count {
                let index = usize::from(shard);
                assert_eq!(rows_per_shard[index], expected_per_shard);
                let connection = fixture.storage.open_shard(shard).unwrap();
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT COUNT(*) FROM native_events
                             WHERE payload LIKE 'auto-concurrent-%'",
                            [],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    i64::try_from(expected_per_shard).unwrap(),
                    "shard count {shard_count}, shard {shard}"
                );
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT seq FROM sqlite_sequence WHERE name = 'native_events'",
                            [],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    maximum_per_shard[index],
                    "shard count {shard_count}, shard {shard}"
                );
                assert_eq!(
                    connection
                        .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                        .unwrap(),
                    "wal"
                );
                let wal = fixture
                    ._temp
                    .path()
                    .join("shards")
                    .join(format!("{shard:04}.sqlite-wal"));
                assert!(
                    std::fs::metadata(&wal).unwrap().len() > 32,
                    "shard count {shard_count}, shard {shard} did not retain a WAL frame"
                );
            }
            drop(wal_readers);
        }
    }

    #[test]
    fn writable_native_generation_is_concurrent_and_unique_across_supported_shard_counts() {
        for shard_count in [2_u16, 4, 8, 10] {
            let fixture = TypedRoutingFixture::new(shard_count);
            let table_id = fixture.native_table_id();
            let barrier = Arc::new(std::sync::Barrier::new(usize::from(shard_count)));
            let mut workers = Vec::new();
            for shard in 0..shard_count {
                let storage = fixture.storage.clone();
                let barrier = Arc::clone(&barrier);
                workers.push(thread::spawn(move || {
                    let mut coordinator = WriteCoordinator::open(storage).unwrap();
                    barrier.wait();
                    let result = coordinator
                        .execute_generated_dml(
                            "INSERT INTO native_events (payload) VALUES (?1)",
                            [format!("concurrent-{shard}")],
                            table_id,
                            shard,
                        )
                        .unwrap();
                    let generated = match &result.generated_key().unwrap().value {
                        Value::Int64(value) => *value,
                        value => panic!("unexpected generated value: {value:?}"),
                    };
                    (shard, result.shard(), generated)
                }));
            }

            let results = workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>();
            let unique = results
                .iter()
                .map(|(_, _, generated)| *generated)
                .collect::<BTreeSet<_>>();
            assert_eq!(unique.len(), usize::from(shard_count));
            let owners = fixture.storage.allocation_owner_map().unwrap();
            for (expected_shard, actual_shard, generated) in results {
                assert_eq!(actual_shard, Some(expected_shard));
                let decoded = NativeRangeV1Id::decode(generated).unwrap();
                assert_eq!(owners.physical_shard(decoded.owner()), Some(expected_shard));
                assert_eq!(decoded.local_sequence(), 2);
                let connection = fixture.storage.open_shard(expected_shard).unwrap();
                assert_eq!(
                    connection
                        .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                        .unwrap(),
                    "wal"
                );
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT COUNT(*) FROM native_events WHERE id = ?1",
                            [generated],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    1
                );
            }
        }
    }

    #[test]
    fn writable_native_generation_cancellation_before_callback_exposes_no_key() {
        let fixture = TypedRoutingFixture::new(2);
        let table_id = fixture.native_table_id();
        let coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let cancellation = coordinator.cancellation_handle();
        let mut gate = coordinator.install_statement_arm_gate_for_test();
        let worker = thread::spawn(move || {
            let mut coordinator = coordinator;
            let result = coordinator.execute_generated_dml(
                "INSERT INTO native_events (payload) VALUES ('cancelled-generated')",
                [],
                table_id,
                0,
            );
            (coordinator, result)
        });

        gate.wait_until_started();
        cancellation.cancel();
        gate.release();

        let (mut coordinator, result) = worker.join().unwrap();
        assert_eq!(result.unwrap_err().kind(), EngineErrorKind::Cancelled);
        assert_eq!(
            fixture
                .storage
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM native_events WHERE payload = 'cancelled-generated'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        let after = coordinator
            .execute_generated_dml(
                "INSERT INTO native_events (payload) VALUES ('after-cancel')",
                [],
                table_id,
                0,
            )
            .unwrap();
        assert!(after.generated_key().is_some());
    }

    #[test]
    fn writable_distinct_shards_progress_while_same_shard_writer_is_locked() {
        let fixture = Fixture::new();
        let blocker = fixture.storage.open_shard(0).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

        let (completed_tx, completed_rx) = mpsc::channel();
        let mut workers = Vec::new();
        for shard in 0..2_u16 {
            let storage = fixture.storage.clone();
            let key = fixture.keys[usize::from(shard)];
            let completed = completed_tx.clone();
            workers.push(thread::spawn(move || {
                let mut coordinator = WriteCoordinator::open(storage).unwrap();
                let result = coordinator.execute_dml(
                    "INSERT INTO events VALUES
                     (?1, 2, 'concurrent', 1.0, x'01', NULL, 'one')",
                    [key],
                );
                completed.send((shard, result)).unwrap();
            }));
        }
        drop(completed_tx);

        let (first_shard, first_result) = completed_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("the unlocked shard writer did not complete independently");
        assert_eq!(first_shard, 1);
        assert_eq!(first_result.unwrap().affected_rows, 1);

        blocker.execute_batch("COMMIT").unwrap();
        let (second_shard, second_result) = completed_rx
            .recv_timeout(TEST_SYNC_TIMEOUT)
            .expect("the blocked same-shard writer did not resume");
        assert_eq!(second_shard, 0);
        assert_eq!(second_result.unwrap().affected_rows, 1);
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(fixture.physical_row_count(), 4);
    }

    #[test]
    fn writable_cancellation_interrupts_lock_wait_rolls_back_and_allows_reuse() {
        let fixture = Fixture::new();
        let blocker = fixture.storage.open_shard(0).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

        let coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let cancellation = coordinator.cancellation_handle();
        let write_state = coordinator.write_state_for_test();
        let key = fixture.keys[0];
        let worker = thread::spawn(move || {
            let mut coordinator = coordinator;
            let result = coordinator.execute_dml(
                "INSERT INTO events VALUES
                 (?1, 2, 'cancelled', 1.0, x'01', NULL, 'one')",
                [key],
            );
            (coordinator, result)
        });

        let deadline = Instant::now() + TEST_SYNC_TIMEOUT;
        while !write_state.has_active_child() {
            assert!(
                Instant::now() < deadline,
                "writable child did not reach its cancellable lock wait"
            );
            thread::sleep(Duration::from_millis(5));
        }
        cancellation.cancel();
        let (mut coordinator, result) = worker.join().unwrap();
        assert_eq!(result.unwrap_err().kind(), EngineErrorKind::Cancelled);
        assert_eq!(fixture.physical_row_count(), 2);

        blocker.execute_batch("COMMIT").unwrap();
        assert_eq!(
            coordinator
                .execute_dml(
                    "INSERT INTO events VALUES
                     (?1, 2, 'after-cancel', 1.0, x'01', NULL, 'one')",
                    [fixture.keys[0]],
                )
                .unwrap()
                .affected_rows,
            1
        );
        assert_eq!(fixture.physical_row_count(), 3);
    }

    #[test]
    fn writable_cancellation_after_statement_arm_cannot_be_lost_before_first_callback() {
        let fixture = Fixture::new();
        let coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let cancellation = coordinator.cancellation_handle();
        let mut gate = coordinator.install_statement_arm_gate_for_test();
        let key = fixture.keys[0];
        let worker = thread::spawn(move || {
            let mut coordinator = coordinator;
            let result = coordinator.execute_dml(
                "INSERT INTO events VALUES
                 (?1, 2, 'cancelled-before-callback', 1.0, x'01', NULL, 'one')",
                [key],
            );
            (coordinator, result)
        });

        gate.wait_until_started();
        cancellation.cancel();
        gate.release();

        let (mut coordinator, result) = worker.join().unwrap();
        assert_eq!(result.unwrap_err().kind(), EngineErrorKind::Cancelled);
        assert_eq!(fixture.physical_row_count(), 2);

        assert_eq!(
            coordinator
                .execute_dml(
                    "INSERT INTO events VALUES
                     (?1, 2, 'after-cancel', 1.0, x'01', NULL, 'one')",
                    [fixture.keys[0]],
                )
                .unwrap()
                .affected_rows,
            1
        );
        assert_eq!(fixture.physical_row_count(), 3);
    }

    #[test]
    fn writable_drop_rolls_back_connection_loss_and_commits_survive_reopen() {
        let fixture = Fixture::new();
        {
            let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
            coordinator.begin().unwrap();
            coordinator
                .execute_dml(
                    "INSERT INTO events VALUES
                     (?1, 2, 'lost-connection', 1.0, x'01', NULL, 'one')",
                    [fixture.keys[0]],
                )
                .unwrap();
            // Dropping the only wrapper is the supported connection-loss path.
        }
        assert_eq!(fixture.physical_row_count(), 2);

        {
            let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
            coordinator
                .execute_dml(
                    "INSERT INTO events VALUES
                     (?1, 2, 'durable', 1.0, x'01', NULL, 'one')",
                    [fixture.keys[0]],
                )
                .unwrap();
        }
        let reopened = Storage::open(fixture.temp.path(), 2).unwrap();
        assert_eq!(
            reopened
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT payload FROM events WHERE tenant_id = ?1 AND event_id = 2",
                    [fixture.keys[0]],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "durable"
        );
    }

    #[test]
    fn writable_forced_termination_process_child() {
        let Ok(root) = std::env::var("BRISKDB_VTAB_TERMINATION_ROOT") else {
            return;
        };
        let storage = Storage::open(root, 4).unwrap();
        let table_id = storage
            .logical_catalog()
            .table("default", "native_events")
            .unwrap()
            .unwrap()
            .id()
            .get();
        let mut coordinator = WriteCoordinator::open(storage).unwrap();

        let committed = coordinator
            .execute_generated_dml_auto(
                "INSERT INTO native_events (payload) VALUES ('committed-before-kill')",
                [],
                table_id,
            )
            .unwrap();
        let committed_id = match &committed.generated_key().unwrap().value {
            Value::Int64(value) => *value,
            value => panic!("unexpected generated value: {value:?}"),
        };
        let committed_shard = committed.shard().unwrap();

        coordinator.begin().unwrap();
        let pending = coordinator
            .execute_generated_dml_auto(
                "INSERT INTO native_events (payload) VALUES ('uncommitted-at-kill')",
                [],
                table_id,
            )
            .unwrap();
        let pending_id = match &pending.generated_key().unwrap().value {
            Value::Int64(value) => *value,
            value => panic!("unexpected generated value: {value:?}"),
        };
        std::fs::write(
            std::env::var("BRISKDB_VTAB_TERMINATION_OUTPUT").unwrap(),
            format!(
                "{committed_id},{committed_shard},{pending_id},{}",
                pending.shard().unwrap()
            ),
        )
        .unwrap();
        std::fs::write(
            std::env::var("BRISKDB_VTAB_TERMINATION_READY").unwrap(),
            b"ready",
        )
        .unwrap();

        thread::sleep(Duration::from_secs(30));
        panic!("forced-termination child was not terminated by its parent");
    }

    #[test]
    fn forced_termination_recovers_wal_without_lost_acknowledged_or_duplicate_generated_rows() {
        let TypedRoutingFixture {
            _temp,
            storage,
            int_keys: _,
            text_keys: _,
            blob_keys: _,
            native_ids: _,
        } = TypedRoutingFixture::new(4);
        let root = _temp.path();
        let before = assert_native_event_invariants(&storage);
        drop(storage);
        let ready = root.join("forced-termination-ready");
        let output = root.join("forced-termination-output");
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("storage::sharded_vtab::tests::writable_forced_termination_process_child")
            .arg("--nocapture")
            .env("BRISKDB_VTAB_TERMINATION_ROOT", root)
            .env("BRISKDB_VTAB_TERMINATION_READY", &ready)
            .env("BRISKDB_VTAB_TERMINATION_OUTPUT", &output)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut child = ReapedTestChild::new(child);

        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.exists() && Instant::now() < deadline {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        if !ready.exists() {
            let child_output = child.terminate_with_output().unwrap();
            panic!(
                "termination child did not reach its write boundary (status {}): stdout={} stderr={}",
                child_output.status,
                String::from_utf8_lossy(&child_output.stdout),
                String::from_utf8_lossy(&child_output.stderr),
            );
        }
        let child_output = child.terminate_with_output().unwrap();
        assert!(
            !child_output.status.success(),
            "termination child unexpectedly succeeded: stdout={} stderr={}",
            String::from_utf8_lossy(&child_output.stdout),
            String::from_utf8_lossy(&child_output.stderr),
        );

        let result = std::fs::read_to_string(output).unwrap();
        let values = result
            .split(',')
            .map(|value| value.parse::<i64>().unwrap())
            .collect::<Vec<_>>();
        let [committed_id, committed_shard, pending_id, _pending_shard] = values.as_slice() else {
            panic!("unexpected forced-termination child output: {result}");
        };

        let reopened = Storage::open(root, 4).unwrap();
        let recovered = assert_native_event_invariants(&reopened);
        assert_eq!(recovered.len(), before.len() + 1);
        assert_eq!(
            recovered
                .iter()
                .filter(|(shard, id, payload)| {
                    i64::from(*shard) == *committed_shard
                        && *id == *committed_id
                        && payload == "committed-before-kill"
                })
                .count(),
            1,
            "the acknowledged autocommit row must survive exactly once"
        );
        assert!(
            recovered
                .iter()
                .all(|(_, id, payload)| *id != *pending_id && payload != "uncommitted-at-kill"),
            "the killed outer transaction must leave no row or generated ID"
        );
        assert_persistent_sqlite_integrity(root, 4);

        let table_id = reopened
            .logical_catalog()
            .table("default", "native_events")
            .unwrap()
            .unwrap()
            .id()
            .get();
        let retried = WriteCoordinator::open(reopened.clone())
            .unwrap()
            .execute_generated_dml_auto(
                "INSERT INTO native_events (payload) VALUES ('retry-after-kill')",
                [],
                table_id,
            )
            .unwrap();
        let retried_id = match &retried.generated_key().unwrap().value {
            Value::Int64(value) => *value,
            value => panic!("unexpected generated value: {value:?}"),
        };
        let final_rows = assert_native_event_invariants(&reopened);
        assert_eq!(final_rows.len(), before.len() + 2);
        assert_eq!(
            final_rows
                .iter()
                .filter(|(_, id, payload)| *id == retried_id && payload == "retry-after-kill")
                .count(),
            1
        );
        assert_persistent_sqlite_integrity(root, 4);
    }

    #[test]
    fn writable_child_commit_failure_is_surfaced_before_acknowledgement_and_recovers_on_reopen() {
        let fixture = Fixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        coordinator.fail_next_commit_for_test();
        let error = coordinator
            .execute_dml(
                "INSERT INTO events VALUES
                 (?1, 2, 'must-not-commit', 1.0, x'01', NULL, 'one')",
                [fixture.keys[0]],
            )
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::StorageUnavailable);
        assert_eq!(fixture.physical_row_count(), 2);
        assert_eq!(
            coordinator
                .execute_dml(
                    "INSERT INTO events VALUES
                     (?1, 2, 'not-reusable', 1.0, x'01', NULL, 'one')",
                    [fixture.keys[0]],
                )
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        drop(coordinator);

        let mut reopened = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        assert_eq!(
            reopened
                .execute_dml(
                    "INSERT INTO events VALUES
                     (?1, 2, 'recovered', 1.0, x'01', NULL, 'one')",
                    [fixture.keys[0]],
                )
                .unwrap()
                .affected_rows,
            1
        );
        assert_eq!(fixture.physical_row_count(), 3);
    }

    #[test]
    fn writable_sqlite_full_rolls_back_and_exact_retry_preserves_invariants() {
        let fixture = TypedRoutingFixture::new(4);
        let table_id = fixture.native_table_id();
        let before = assert_native_event_invariants(&fixture.storage);
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        coordinator.force_next_child_sqlite_full_for_test();
        let oversized_payload = format!("retry-after-full:{}", "x".repeat(256 * 1024));

        let error = coordinator
            .execute_generated_dml_auto(
                "INSERT INTO native_events (payload) VALUES (?1)",
                params![oversized_payload],
                table_id,
            )
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::StorageFull);
        drop(coordinator);
        assert_eq!(assert_native_event_invariants(&fixture.storage), before);
        assert_persistent_sqlite_integrity(fixture._temp.path(), 4);

        let reopened = Storage::open(fixture._temp.path(), 4).unwrap();
        let retried = WriteCoordinator::open(reopened.clone())
            .unwrap()
            .execute_generated_dml_auto(
                "INSERT INTO native_events (payload) VALUES (?1)",
                params![oversized_payload],
                table_id,
            )
            .unwrap();
        let retried_id = match &retried.generated_key().unwrap().value {
            Value::Int64(value) => *value,
            value => panic!("unexpected generated value: {value:?}"),
        };
        let after = assert_native_event_invariants(&reopened);
        assert_eq!(after.len(), before.len() + 1);
        assert_eq!(
            after
                .iter()
                .filter(|(_, id, payload)| { *id == retried_id && payload == &oversized_payload })
                .count(),
            1
        );
        assert_persistent_sqlite_integrity(fixture._temp.path(), 4);
    }

    #[test]
    fn writable_child_operation_corruption_marks_storage_degraded() {
        let fixture = Fixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        coordinator.begin().unwrap();
        coordinator.fail_next_write_corruption_for_test();
        let error = coordinator
            .execute_dml(
                "INSERT INTO events VALUES
                 (?1, 2, 'must-not-land', 1.0, x'01', NULL, 'one')",
                [fixture.keys[0]],
            )
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert!(!coordinator.in_transaction());
        assert_eq!(
            fixture.storage.schema_gate_snapshot().state,
            crate::storage::SchemaGateState::Degraded
        );
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
        assert_eq!(
            Connection::open(fixture.temp.path().join("shards/0000.sqlite"))
                .unwrap()
                .query_row("SELECT COUNT(*) FROM events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );
    }

    #[test]
    fn writable_child_commit_corruption_marks_storage_degraded_before_acknowledgement() {
        let fixture = Fixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        coordinator.fail_next_commit_corruption_for_test();
        let error = coordinator
            .execute_dml(
                "INSERT INTO events VALUES
                 (?1, 2, 'must-roll-back', 1.0, x'01', NULL, 'one')",
                [fixture.keys[0]],
            )
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(
            fixture.storage.schema_gate_snapshot().state,
            crate::storage::SchemaGateState::Degraded
        );
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
        assert_eq!(
            Connection::open(fixture.temp.path().join("shards/0000.sqlite"))
                .unwrap()
                .query_row("SELECT COUNT(*) FROM events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );
    }

    #[test]
    fn writable_local_foreign_key_does_not_accept_a_parent_on_the_wrong_shard() {
        let fixture = WritableFixture::new();
        fixture
            .storage
            .open_shard(0)
            .unwrap()
            .execute(
                "DELETE FROM parents WHERE tenant_id = ?1",
                [fixture.keys[0]],
            )
            .unwrap();
        fixture
            .storage
            .open_shard(1)
            .unwrap()
            .execute(
                "INSERT INTO parents VALUES (?1, 1, 'wrong-owner')",
                [fixture.keys[0]],
            )
            .unwrap();

        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let error = coordinator
            .execute_dml(
                "INSERT INTO items VALUES (?1, 1, 1, 'must-fail', 1)",
                [fixture.keys[0]],
            )
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::ForeignKeyViolation);
        assert_eq!(fixture.item_count(), 0);
    }

    #[test]
    fn writable_defaults_and_generated_columns_fail_closed_until_their_issues_land() {
        let temp = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(temp.path(), 2).unwrap();
        let mut migration = storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        storage
            .apply_schema_migration(
                "CREATE TABLE default_events (
                     id INTEGER PRIMARY KEY,
                     payload TEXT NOT NULL DEFAULT 'defaulted'
                 );
                 CREATE TABLE generated_events (
                     id INTEGER PRIMARY KEY,
                     base INTEGER NOT NULL,
                     doubled INTEGER GENERATED ALWAYS AS (base * 2) STORED
                 );",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();
        let database_id = storage.logical_catalog().default_database().id();
        storage
            .register_tables(vec![
                TableDeclaration::sharded(
                    database_id,
                    "default_events",
                    ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
                )
                .unwrap(),
                TableDeclaration::sharded(
                    database_id,
                    "generated_events",
                    ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
        let key = (1_i64..)
            .find(|key| storage.shard_for_key(key.to_string().as_bytes()) == 0)
            .unwrap();
        let mut coordinator = WriteCoordinator::open(storage.clone()).unwrap();

        for (sql, parameters) in [
            (
                "INSERT INTO default_events (id, payload) VALUES (?1, 'explicit')",
                [key],
            ),
            (
                "INSERT INTO generated_events (id, base) VALUES (?1, 3)",
                [key],
            ),
        ] {
            assert_eq!(
                coordinator.execute_dml(sql, parameters).unwrap_err().kind(),
                EngineErrorKind::InvalidQuery,
                "sql={sql}"
            );
        }
        for table in ["default_events", "generated_events"] {
            assert_eq!(
                storage
                    .open_shard(0)
                    .unwrap()
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                0
            );
        }
    }

    #[test]
    fn writable_stale_coordinator_fails_before_opening_or_mutating_a_child() {
        let fixture = Fixture::new();
        let mut coordinator = WriteCoordinator::open(fixture.storage.clone()).unwrap();
        let mut migration = fixture.storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        fixture
            .storage
            .apply_schema_migration(
                "ALTER TABLE events ADD COLUMN note TEXT",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();

        let error = coordinator
            .execute_dml(
                "INSERT INTO events
                 (tenant_id, event_id, payload, amount, raw, optional, category)
                 VALUES (?1, 2, 'stale', 1.0, x'01', NULL, 'one')",
                [fixture.keys[0]],
            )
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Busy);
        assert_eq!(fixture.physical_row_count(), 2);
        assert_eq!(fixture.storage.schema_gate_snapshot().active_operations, 0);
    }
}
