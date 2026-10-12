use super::{Cell, Row, registry::Registry};
use rusqlite::{
    ffi,
    types::ValueRef,
    vtab::{
        ConflictMode, Context, CreateVTab, Filters, IndexConstraintOp, IndexInfo, Inserts,
        UpdateVTab, Updates, VTab, VTabConfig, VTabConnection, VTabCursor, VTabKind,
    },
};
use std::{ffi::c_int, sync::Arc};

pub(crate) fn error(message: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::ModuleError(message.into())
}
fn map(error: crate::EngineError) -> rusqlite::Error {
    let code = match error.kind() {
        crate::EngineErrorKind::UniqueViolation => ffi::SQLITE_CONSTRAINT_PRIMARYKEY,
        crate::EngineErrorKind::Busy => ffi::SQLITE_BUSY,
        _ => ffi::SQLITE_ERROR,
    };
    rusqlite::Error::SqliteFailure(ffi::Error::new(code), Some(error.to_string()))
}

#[repr(C)]
pub(crate) struct OverlayTable {
    base: ffi::sqlite3_vtab,
    registry: Arc<Registry>,
    table: usize,
    handle: *mut ffi::sqlite3,
}

// SAFETY: SQLite's C base is the first field; rusqlite owns allocations and
// invokes all callbacks on the owning connection's thread.
unsafe impl<'a> VTab<'a> for OverlayTable {
    type Aux = Arc<Registry>;
    type Cursor = Cursor;
    fn connect(
        connection: &mut VTabConnection,
        auxiliary: Option<&Self::Aux>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, Self)> {
        let registry = auxiliary
            .cloned()
            .ok_or_else(|| error("missing overlay registry"))?;
        if args.len() != 4 {
            return Err(error("expected a catalog table number"));
        }
        let table = std::str::from_utf8(args[3])
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v < registry.config.tables.len())
            .ok_or_else(|| error("invalid catalog table number"))?;
        connection.config(VTabConfig::DirectOnly)?;
        let schema = format!(
            "CREATE TABLE x ({})",
            registry.config.tables[table].columns_sql()
        );
        // SAFETY: only stored for on_conflict during this live connection's callbacks.
        let handle = unsafe { connection.handle() };
        Ok((
            schema,
            Self {
                base: ffi::sqlite3_vtab::default(),
                registry,
                table,
                handle,
            },
        ))
    }
    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        let count = self.registry.config.tables[self.table].columns.len();
        let mut columns = Vec::new();
        let selected = info
            .constraints()
            .enumerate()
            .filter_map(|(i, c)| {
                (c.is_usable()
                    && c.column() >= 0
                    && (c.column() as usize) < count
                    && c.operator() == IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ)
                    .then_some((i, c.column()))
            })
            .collect::<Vec<_>>();
        for (i, column) in selected {
            if info.is_in_constraint(i)? || !info.collation(i)?.eq_ignore_ascii_case("BINARY") {
                continue;
            }
            columns.push(column.to_string());
            let mut usage = info.constraint_usage(i);
            usage.set_argv_index(columns.len() as i32);
            usage.set_omit(false);
        }
        let routed = columns.iter().any(|c| {
            *c == self.registry.config.tables[self.table]
                .routing_column()
                .to_string()
        });
        info.set_idx_str(&columns.join(","));
        info.set_estimated_cost(if routed { 10.0 } else { 1_000_000.0 });
        info.set_estimated_rows(if routed { 100 } else { 100_000 });
        Ok(())
    }
    fn open(&'a mut self) -> rusqlite::Result<Cursor> {
        Ok(Cursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            registry: Arc::clone(&self.registry),
            table: self.table,
            rows: Vec::new(),
            position: 0,
        })
    }
}

impl<'a> CreateVTab<'a> for OverlayTable {
    const KIND: VTabKind = VTabKind::Default;
}

impl<'a> UpdateVTab<'a> for OverlayTable {
    fn delete(&mut self, old: ValueRef<'_>) -> rusqlite::Result<()> {
        let ValueRef::Integer(rowid) = old else {
            return Err(error("invalid row locator"));
        };
        self.registry.delete(self.table, rowid).map_err(map)
    }
    fn insert(&mut self, args: &Inserts<'_>) -> rusqlite::Result<i64> {
        // SAFETY: handle belongs to this live virtual-table connection.
        if unsafe { args.on_conflict(self.handle) } != ConflictMode::Abort {
            return Err(error("overlay supports default conflict handling only"));
        }
        let values = args.iter().collect::<Vec<_>>();
        if values.len() != self.registry.config.tables[self.table].columns.len() + 2
            || !matches!(values[0], ValueRef::Null)
            || !matches!(values[1], ValueRef::Null)
        {
            return Err(error(
                "explicit rowid is unsupported; supply the declared primary key",
            ));
        }
        let row = values[2..]
            .iter()
            .map(|v| Cell::from_sql(*v))
            .collect::<rusqlite::Result<Row>>()?;
        self.registry.insert(self.table, row).map_err(map)
    }
    fn update(&mut self, args: &Updates<'_>) -> rusqlite::Result<()> {
        // SAFETY: handle belongs to this live virtual-table connection.
        if unsafe { args.on_conflict(self.handle) } != ConflictMode::Abort {
            return Err(error("overlay supports default conflict handling only"));
        }
        let values = args.iter().collect::<Vec<_>>();
        if values.len() != self.registry.config.tables[self.table].columns.len() + 2 {
            return Err(error("invalid update shape"));
        }
        let (ValueRef::Integer(old), ValueRef::Integer(new)) = (values[0], values[1]) else {
            return Err(error("invalid row locator"));
        };
        if old != new {
            return Err(error("rowid cannot be changed"));
        }
        let row = values[2..]
            .iter()
            .map(|v| Cell::from_sql(*v))
            .collect::<rusqlite::Result<Row>>()?;
        self.registry.update(self.table, old, row).map_err(map)
    }
}

#[repr(C)]
pub(crate) struct Cursor {
    base: ffi::sqlite3_vtab_cursor,
    registry: Arc<Registry>,
    table: usize,
    rows: Vec<(i64, Row)>,
    position: usize,
}

// SAFETY: SQLite's C cursor base is first; cursor is owned by rusqlite and
// no references into transient SQLite argv/results survive a callback.
unsafe impl VTabCursor for Cursor {
    fn filter(
        &mut self,
        _number: c_int,
        plan: Option<&str>,
        args: &Filters<'_>,
    ) -> rusqlite::Result<()> {
        let columns = plan
            .unwrap_or("")
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.parse::<usize>().map_err(|_| error("invalid scan plan")))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let values = args
            .iter()
            .map(Cell::from_sql)
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if columns.len() != values.len() {
            return Err(error("scan arguments differ from plan"));
        }
        self.rows = self
            .registry
            .scan(
                self.table,
                &columns.into_iter().zip(values).collect::<Vec<_>>(),
            )
            .map_err(map)?;
        self.position = 0;
        Ok(())
    }
    fn next(&mut self) -> rusqlite::Result<()> {
        self.position += 1;
        Ok(())
    }
    fn eof(&self) -> bool {
        self.position >= self.rows.len()
    }
    fn column(&self, context: &mut Context, column: c_int) -> rusqlite::Result<()> {
        let value = self
            .rows
            .get(self.position)
            .and_then(|(_, r)| r.get(column as usize))
            .ok_or_else(|| error("column out of range"))?;
        context.set_result(value)
    }
    fn rowid(&self) -> rusqlite::Result<i64> {
        self.rows
            .get(self.position)
            .map(|v| v.0)
            .ok_or_else(|| error("rowid at EOF"))
    }
}
