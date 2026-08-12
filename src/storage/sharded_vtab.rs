//! Experimental, statically registered SQLite virtual-table facade.
//!
//! This module is deliberately isolated behind `experimental-vtab`. It proves
//! the no-fork boundary without replacing the existing scatter/gather path or
//! changing any physical shard schema.

use std::{
    collections::BTreeMap,
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
        Context, CreateVTab, Filters, IndexConstraintOp, IndexInfo, VTab, VTabConfig,
        VTabConnection, VTabCursor, VTabKind, read_only_module,
    },
};

use super::{SchemaOperationGuard, SqliteAffinity, Storage, quote_identifier, sqlite_affinity};
use crate::{
    core::generated_id::{GeneratedIdClassification, classify_generated_id},
    core::{
        AllocationOwnerMap, CanonicalShardKeyRef, EngineError, EngineErrorKind, EngineResult,
        GeneratedIdPolicy, ShardKeyType, TableMetadata, TablePlacement, canonical_shard_key_bytes,
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
}

impl CoordinatorCancellation {
    pub(crate) fn cancel(&self) {
        let _active_child_scans = self
            .active_child_scans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.epoch.fetch_add(1, Ordering::AcqRel);
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
    tables: BTreeMap<u64, Arc<TableSpec>>,
    limits: CursorLimits,
    cancellation_epoch: Arc<AtomicU64>,
    active_child_scans: Arc<Mutex<usize>>,
    lifecycle: Arc<LifecycleCounters>,
    #[cfg(test)]
    opened_shards: Mutex<Vec<u16>>,
    #[cfg(test)]
    child_scan_gate: Mutex<Option<TestChildScanGate>>,
    #[cfg(test)]
    child_scan_complete_gate: Mutex<Option<TestChildScanGate>>,
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
        Self::build_admitted_with_limits(storage, CursorLimits::default())
    }

    fn build_admitted_with_limits(
        storage: Storage,
        limits: CursorLimits,
    ) -> EngineResult<Arc<Self>> {
        if limits.rows == 0 || limits.bytes == 0 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "brisk_shard result limits must be non-zero",
            ));
        }
        let schema_generation = storage.current_schema_generation();
        let shard = storage.open_shard_read_only(0)?;
        shard
            .pragma_update(None, "query_only", "ON")
            .map_err(sqlite_error::storage)?;

        let allocation_owners = storage.allocation_owner_map().cloned().map(Arc::new);
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
                &shard,
                table,
                allocation_owners.clone(),
                targets.into_boxed_slice(),
            )?;
            tables.insert(table.id().get(), Arc::new(spec));
        }

        Ok(Arc::new(Self {
            storage,
            schema_generation,
            tables,
            limits,
            cancellation_epoch: Arc::new(AtomicU64::new(0)),
            active_child_scans: Arc::new(Mutex::new(0)),
            lifecycle: Arc::new(LifecycleCounters::default()),
            #[cfg(test)]
            opened_shards: Mutex::new(Vec::new()),
            #[cfg(test)]
            child_scan_gate: Mutex::new(None),
            #[cfg(test)]
            child_scan_complete_gate: Mutex::new(None),
        }))
    }

    fn table(&self, id: u64) -> Option<Arc<TableSpec>> {
        self.tables.get(&id).cloned()
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

struct TableSpec {
    id: u64,
    name: String,
    declared_schema: String,
    select_sql: String,
    point_select_sql: Option<String>,
    column_count: usize,
    targets: Box<[u16]>,
    shard_key: Option<ShardKeySpec>,
    generated_id_policy: GeneratedIdPolicy,
    allocation_owners: Option<Arc<AllocationOwnerMap>>,
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
        allocation_owners: Option<Arc<AllocationOwnerMap>>,
        targets: Box<[u16]>,
    ) -> EngineResult<Self> {
        let id = table.id().get();
        let name = table.name();
        let strict = connection
            .query_row(
                "SELECT strict
                 FROM pragma_table_list
                 WHERE schema = 'main' AND name = ?1 COLLATE BINARY AND type = 'table'",
                [name],
                |row| row.get::<_, bool>(0),
            )
            .map_err(sqlite_error::storage)?;
        let mut statement = connection
            .prepare(
                "SELECT name, type, hidden
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
        if columns.iter().any(|(_, _, hidden)| *hidden == 1) {
            return Err(EngineError::new(
                EngineErrorKind::Unsupported,
                format!("registered table {name} has a hidden virtual-table column"),
            ));
        }

        let declared_columns = columns
            .iter()
            .map(|(column, declared_type, _)| {
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
                Ok(format!(
                    "{} {} COLLATE {}",
                    quote_identifier(column),
                    affinity_name(affinity),
                    quote_identifier(collation)
                ))
            })
            .collect::<EngineResult<Vec<_>>>()?
            .join(", ");
        let projected_columns = columns
            .iter()
            .map(|(column, _, _)| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");

        #[allow(unreachable_patterns)]
        let shard_key = match table.placement() {
            TablePlacement::Sharded(metadata) => {
                let column_index = columns
                    .iter()
                    .position(|(column, _, _)| column == metadata.column())
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

        Ok(Self {
            id,
            name: name.to_owned(),
            declared_schema: format!("CREATE TABLE x({declared_columns})"),
            select_sql: format!(
                "SELECT {projected_columns} FROM main.{}",
                quote_identifier(name)
            ),
            point_select_sql,
            column_count: columns.len(),
            targets,
            shard_key,
            generated_id_policy: table.generated_id_policy().clone(),
            allocation_owners,
        })
    }

    fn create_virtual_table_sql(&self) -> String {
        format!(
            "CREATE VIRTUAL TABLE {} USING {MODULE_NAME}({})",
            quote_identifier(&self.name),
            self.id
        )
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
        registry.lifecycle.connects.fetch_add(1, Ordering::Relaxed);
        Ok((
            spec.declared_schema.clone(),
            Self {
                _base: ffi::sqlite3_vtab::default(),
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
        let operation = self
            .registry
            .storage
            .enter_schema_operation()
            .map_err(vtab_error)?;
        if self.registry.storage.current_schema_generation() != self.registry.schema_generation {
            return Err(module_error(
                "brisk_shard coordinator schema is stale; reopen the coordinator",
            ));
        }
        self.operation = Some(operation);
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

#[derive(Debug)]
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
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Integer(value) => ValueRef::Integer(*value),
            Self::Real(value) => ValueRef::Real(*value),
            Self::Text(value) => ValueRef::Text(value),
            Self::Blob(value) => ValueRef::Blob(value),
        }))
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
struct TestChildScanGate {
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
struct TestChildScanControl {
    started: mpsc::Receiver<()>,
    release: TestRelease,
}

#[cfg(test)]
impl TestChildScanControl {
    fn wait_until_started(&self) {
        self.started
            .recv_timeout(TEST_SYNC_TIMEOUT)
            .expect("child scan did not reach its test gate before the timeout");
    }

    fn release(&mut self) {
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
        collections::BTreeSet,
        sync::{Arc, mpsc},
        thread,
    };

    use rusqlite::{ErrorCode, MAIN_DB, params, types::ValueRef};

    use super::*;
    use crate::core::{
        Database, Engine, GeneratedIdPolicy, ShardKeyMetadata, ShardKeyType, Statement,
        TableDeclaration, Value,
        generated_id::{AllocationOwnerSlot, NativeRangeV1Id, native_range_v1_sequence_floor},
    };

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

    struct TypedRoutingFixture {
        _temp: tempfile::TempDir,
        storage: Storage,
        int_keys: Vec<i64>,
        text_keys: Vec<String>,
        blob_keys: Vec<Vec<u8>>,
        native_ids: Vec<i64>,
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
                         id INTEGER PRIMARY KEY NOT NULL,
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
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
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
}
