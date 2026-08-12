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

use rusqlite::{
    Connection, Error as SqliteError, InterruptHandle, Result as SqliteResult, ffi,
    hooks::{AuthAction, AuthContext, Authorization},
    types::{ToSql, ToSqlOutput, ValueRef},
    vtab::{
        Context, CreateVTab, Filters, IndexInfo, VTab, VTabConfig, VTabConnection, VTabCursor,
        VTabKind, read_only_module,
    },
};

use super::{SchemaOperationGuard, SqliteAffinity, Storage, quote_identifier, sqlite_affinity};
use crate::{
    core::{EngineError, EngineErrorKind, EngineResult, TablePlacement},
    sqlite_error,
};

const MODULE_NAME: &str = "brisk_shard";
const MAX_CURSOR_ROWS: usize = 65_536;
const MAX_CURSOR_BYTES: usize = 64 * 1024 * 1024;
const ALLOCATION_OVERHEAD_BYTES: usize = 16;
const ROW_ACCOUNTING_BYTES: usize = size_of::<Vec<RawCell>>() * 4 + ALLOCATION_OVERHEAD_BYTES;
const VALUE_ACCOUNTING_BYTES: usize = size_of::<RawCell>();

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
        // Hold schema admission through discovery and coordinator bootstrap so
        // open cannot return an already-stale declaration.
        let bootstrap_operation = storage.enter_schema_operation()?;
        let registry = Registry::build_admitted(storage)?;
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
    fn install_child_scan_gate(
        &self,
        started: Arc<std::sync::Barrier>,
        release: Arc<std::sync::Barrier>,
    ) {
        *self
            .registry
            .child_scan_gate
            .lock()
            .expect("child-scan test gate is not poisoned") =
            Some(TestChildScanGate { started, release });
    }

    #[cfg(test)]
    fn install_child_scan_complete_gate(
        &self,
        started: Arc<std::sync::Barrier>,
        release: Arc<std::sync::Barrier>,
    ) {
        *self
            .registry
            .child_scan_complete_gate
            .lock()
            .expect("child-scan completion test gate is not poisoned") =
            Some(TestChildScanGate { started, release });
    }
}

/// Cancels a child-shard scan currently inside an `xFilter`/`xNext` callback.
/// Incrementing the epoch does not poison later queries; each new filter
/// captures the then-current epoch. The active-child mutex closes the race in
/// which a late coordinator interrupt could otherwise hit the next query.
#[derive(Clone)]
pub(crate) struct CoordinatorCancellation {
    epoch: Arc<AtomicU64>,
    active_child_scans: Arc<Mutex<usize>>,
    interrupt: Arc<InterruptHandle>,
}

impl CoordinatorCancellation {
    pub(crate) fn cancel(&self) {
        let active_child_scans = self
            .active_child_scans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.epoch.fetch_add(1, Ordering::AcqRel);
        if *active_child_scans != 0 {
            self.interrupt.interrupt();
        }
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
            } if pragma_name.eq_ignore_ascii_case("query_only") => Authorization::Allow,
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

impl Registry {
    fn build_admitted(storage: Storage) -> EngineResult<Arc<Self>> {
        let schema_generation = storage.current_schema_generation();
        let shard = storage.open_shard(0)?;
        shard
            .pragma_update(None, "query_only", "ON")
            .map_err(sqlite_error::storage)?;

        let mut tables = BTreeMap::new();
        for table in storage.logical_catalog().tables() {
            let targets = match table.placement() {
                TablePlacement::Sharded(_) => (0..storage.shard_count()).collect::<Vec<_>>(),
                // Global rows are replicated physically but exposed once from
                // their canonical read owner.
                TablePlacement::Global => vec![0],
                TablePlacement::Catalog => continue,
            };
            let spec = TableSpec::from_physical_table(
                &shard,
                table.id().get(),
                table.name(),
                targets.into_boxed_slice(),
            )?;
            tables.insert(table.id().get(), Arc::new(spec));
        }

        Ok(Arc::new(Self {
            storage,
            schema_generation,
            tables,
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

    fn cancelled(&self, scan_epoch: u64) -> bool {
        self.cancellation_epoch.load(Ordering::Acquire) != scan_epoch
    }

    fn read_shard_rows(
        self: &Arc<Self>,
        spec: &TableSpec,
        shard_id: u16,
        scan_epoch: u64,
        remaining_rows: usize,
        remaining_bytes: usize,
    ) -> EngineResult<(Vec<Vec<RawCell>>, usize)> {
        if self.cancelled(scan_epoch) {
            return Err(cancelled_error());
        }

        let connection = self.storage.open_shard(shard_id)?;
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
        connection
            .connection()
            .progress_handler(
                if cfg!(test) { 1 } else { 128 },
                Some(move || {
                    #[cfg(test)]
                    if let Some(gate) = child_scan_gate.take() {
                        gate.started.wait();
                        gate.release.wait();
                    }
                    cancellation_epoch.load(Ordering::Acquire) != scan_epoch
                }),
            )
            .map_err(sqlite_error::storage)?;

        let result = (|| {
            let mut statement = connection
                .connection()
                .prepare(&spec.select_sql)
                .map_err(sqlite_error::statement)?;
            let mut sqlite_rows = statement.query([]).map_err(sqlite_error::statement)?;
            let mut rows = Vec::new();
            let mut used_bytes = 0_usize;

            while let Some(row) = sqlite_rows.next().map_err(sqlite_error::statement)? {
                if self.cancelled(scan_epoch) {
                    return Err(cancelled_error());
                }
                if rows.len() == remaining_rows {
                    return Err(limit_error("row", MAX_CURSOR_ROWS));
                }

                let mut row_bytes = spec
                    .column_count
                    .checked_mul(VALUE_ACCOUNTING_BYTES)
                    .and_then(|bytes| bytes.checked_add(ROW_ACCOUNTING_BYTES))
                    .ok_or_else(|| limit_error("byte", MAX_CURSOR_BYTES))?;
                if used_bytes
                    .checked_add(row_bytes)
                    .is_none_or(|bytes| bytes > remaining_bytes)
                {
                    return Err(limit_error("byte", MAX_CURSOR_BYTES));
                }
                let mut cells = Vec::new();
                cells
                    .try_reserve_exact(spec.column_count)
                    .map_err(allocation_error)?;
                for column in 0..spec.column_count {
                    let value = row.get_ref(column).map_err(sqlite_error::statement)?;
                    row_bytes = row_bytes
                        .checked_add(RawCell::accounted_payload_bytes(value))
                        .ok_or_else(|| limit_error("byte", MAX_CURSOR_BYTES))?;
                    let projected_bytes = used_bytes
                        .checked_add(row_bytes)
                        .ok_or_else(|| limit_error("byte", MAX_CURSOR_BYTES))?;
                    if projected_bytes > remaining_bytes {
                        return Err(limit_error("byte", MAX_CURSOR_BYTES));
                    }
                    cells.push(RawCell::try_copy_from(value)?);
                }
                used_bytes = used_bytes
                    .checked_add(row_bytes)
                    .filter(|bytes| *bytes <= remaining_bytes)
                    .ok_or_else(|| limit_error("byte", MAX_CURSOR_BYTES))?;
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
                gate.started.wait();
                gate.release.wait();
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
    column_count: usize,
    targets: Box<[u16]>,
}

impl TableSpec {
    fn from_physical_table(
        connection: &Connection,
        id: u64,
        name: &str,
        targets: Box<[u16]>,
    ) -> EngineResult<Self> {
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

        Ok(Self {
            id,
            name: name.to_owned(),
            declared_schema: format!("CREATE TABLE x({declared_columns})"),
            select_sql: format!(
                "SELECT {projected_columns} FROM main.{}",
                quote_identifier(name)
            ),
            column_count: columns.len(),
            targets,
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
        // This boundary spike deliberately consumes no constraints. SQLite
        // rechecks every predicate. Routing and pushdown belong to issue #126.
        information.set_idx_num(0);
        information.set_estimated_cost(1_000_000.0);
        information.set_estimated_rows(1_000_000);
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
        while let Some(&shard_id) = self.spec.targets.get(self.next_target) {
            self.next_target += 1;
            let remaining_rows = MAX_CURSOR_ROWS.saturating_sub(self.total_rows);
            let remaining_bytes = MAX_CURSOR_BYTES.saturating_sub(self.total_bytes);
            let (rows, used_bytes) = self
                .registry
                .read_shard_rows(
                    &self.spec,
                    shard_id,
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
        _index_number: c_int,
        _index_string: Option<&str>,
        _arguments: &Filters<'_>,
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
struct TestChildScanGate {
    started: Arc<std::sync::Barrier>,
    release: Arc<std::sync::Barrier>,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::{Arc, Barrier},
        thread,
    };

    use rusqlite::{ErrorCode, params, types::ValueRef};

    use super::*;
    use crate::core::{ShardKeyMetadata, ShardKeyType, TableDeclaration};

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
        for sql in [
            "INSERT INTO events VALUES (99, 99, 'bad', 0, NULL, NULL, 'bad')",
            "UPDATE events SET payload = 'bad'",
            "DELETE FROM events",
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
        let started = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        coordinator.install_child_scan_gate(Arc::clone(&started), Arc::clone(&release));
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
        started.wait();
        assert_eq!(
            *cancellation
                .active_child_scans
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            1
        );
        cancellation.cancel();
        release.wait();
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
        let started = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        coordinator.install_child_scan_complete_gate(Arc::clone(&started), Arc::clone(&release));
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
        started.wait();
        // Advance only the epoch so this test proves the post-scan check,
        // independently of both SQLite interrupt handles and progress hooks.
        cancellation.epoch.fetch_add(1, Ordering::AcqRel);
        release.wait();

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
            .read_shard_rows(spec, 0, 0, MAX_CURSOR_ROWS, 1)
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
