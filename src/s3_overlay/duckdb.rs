//! Optional native DuckDB SQL reader, preserving BriskDB snapshot authority.
//! DuckDB scans SQLite files directly; never copies a database to local disk.
//! Pending Parquet is loaded/verified by the SAME bounded BriskDB reader and
//! exposed as typed pending rows. This is not a DuckDB S3/httpfs benchmark.
use super::{
    Cell, ColumnType, Database, MAX_BYTES, MAX_ROWS, QueryResult, Result, invalid, limit,
    schema::quote, storage_error,
};
use serde::{Deserialize, Serialize};
use std::{
    ffi::{CStr, CString, c_char, c_void},
    path::PathBuf,
    ptr,
    sync::mpsc,
    thread,
    time::Duration,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuckDbReadOptions {
    /// Trusted DuckDB 1.5.6 shared library; never supplied by an untrusted request.
    pub library: PathBuf,
    /// Matching signed SQLite extension, provisioned BEFORE a Lambda invocation.
    pub sqlite_extension: PathBuf,
    pub threads: u16,
    #[serde(default = "default_memory")]
    pub memory_mb: u16,
}
fn default_memory() -> u16 {
    256
}

type Handle = *mut c_void;
#[repr(C)]
#[derive(Clone, Copy)]
struct RawResult {
    columns: u64,
    rows: u64,
    changed: u64,
    column_ptr: Handle,
    error_ptr: Handle,
    internal: Handle,
}
impl Default for RawResult {
    fn default() -> Self {
        // Raw pointers do not implement Default on our Rust 1.85 MSRV.
        Self {
            columns: 0,
            rows: 0,
            changed: 0,
            column_ptr: ptr::null_mut(),
            error_ptr: ptr::null_mut(),
            internal: ptr::null_mut(),
        }
    }
}
#[repr(C)]
struct RawString {
    length: u32,
    prefix: [u8; 4],
    // Covers both the pointer and the final eight inline bytes.
    tail: u64,
}

macro_rules! api {
    ($($name:ident: ($($arg:ty),*) -> $ret:ty),* $(,)?) => {
        struct Api { _library: libloading::Library, $( $name: unsafe extern "C" fn($($arg),*) -> $ret, )* }
        impl Api {
            fn load(path: &std::path::Path) -> Result<Self> {
                // SAFETY: caller explicitly opts into loading this trusted native
                // library. All signatures/layouts match the pinned 1.5.6 C header.
                unsafe {
                    let library = libloading::Library::new(path).map_err(storage_error)?;
                    $(let $name = *library.get::<unsafe extern "C" fn($($arg),*) -> $ret>(concat!(stringify!($name),"\0").as_bytes()).map_err(storage_error)?;)*
                    Ok(Self { _library: library, $($name,)* })
                }
            }
        }
    }
}
api! {
    duckdb_library_version: () -> *const c_char,
    duckdb_create_config: (*mut Handle) -> u32,
    duckdb_set_config: (Handle, *const c_char, *const c_char) -> u32,
    duckdb_destroy_config: (*mut Handle) -> (),
    duckdb_open_ext: (*const c_char, *mut Handle, Handle, *mut *mut c_char) -> u32,
    duckdb_close: (*mut Handle) -> (),
    duckdb_connect: (Handle, *mut Handle) -> u32,
    duckdb_disconnect: (*mut Handle) -> (),
    duckdb_interrupt: (Handle) -> (),
    duckdb_query: (Handle, *const c_char, *mut RawResult) -> u32,
    duckdb_destroy_result: (*mut RawResult) -> (),
    duckdb_result_error: (*mut RawResult) -> *const c_char,
    duckdb_column_count: (*mut RawResult) -> u64,
    duckdb_column_name: (*mut RawResult, u64) -> *const c_char,
    duckdb_column_type: (*mut RawResult, u64) -> u32,
    duckdb_result_chunk_count: (RawResult) -> u64,
    duckdb_result_get_chunk: (RawResult, u64) -> Handle,
    duckdb_destroy_data_chunk: (*mut Handle) -> (),
    duckdb_data_chunk_get_size: (Handle) -> u64,
    duckdb_data_chunk_get_vector: (Handle, u64) -> Handle,
    duckdb_vector_get_data: (Handle) -> Handle,
    duckdb_vector_get_validity: (Handle) -> *const u64,
    duckdb_string_t_data: (*const RawString) -> *const u8,
    duckdb_free: (Handle) -> (),
    duckdb_prepare: (Handle, *const c_char, *mut Handle) -> u32,
    duckdb_prepare_error: (Handle) -> *const c_char,
    duckdb_destroy_prepare: (*mut Handle) -> (),
    duckdb_nparams: (Handle) -> u64,
    duckdb_bind_null: (Handle, u64) -> u32,
    duckdb_bind_int64: (Handle, u64, i64) -> u32,
    duckdb_bind_double: (Handle, u64, f64) -> u32,
    duckdb_bind_varchar_length: (Handle, u64, *const c_char, u64) -> u32,
    duckdb_bind_blob: (Handle, u64, *const c_void, u64) -> u32,
    duckdb_execute_prepared: (Handle, *mut RawResult) -> u32,
}
fn cstring(value: &str) -> Result<CString> {
    CString::new(value).map_err(|_| invalid("embedded NUL in SQL/path"))
}
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
unsafe fn message(value: *const c_char) -> String {
    if value.is_null() {
        "DuckDB failed without a diagnostic".into()
    } else {
        unsafe { CStr::from_ptr(value) }
            .to_string_lossy()
            .into_owned()
    }
}

struct Duck {
    api: Api,
    db: Handle,
    connection: Handle,
    stop: Option<mpsc::Sender<()>>,
    watchdog: Option<thread::JoinHandle<()>>,
}
impl Drop for Duck {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(watchdog) = self.watchdog.take() {
            let _ = watchdog.join();
        }
        // SAFETY: watchdog stopped, all owned results/prepares destroyed before
        // connection/database teardown, library remains loaded until last.
        unsafe {
            if !self.connection.is_null() {
                (self.api.duckdb_disconnect)(&mut self.connection);
            }
            if !self.db.is_null() {
                (self.api.duckdb_close)(&mut self.db);
            }
        }
    }
}
impl Duck {
    fn open(options: &DuckDbReadOptions) -> Result<Self> {
        if !(1..=16).contains(&options.threads)
            || !(64..=1024).contains(&options.memory_mb)
            || !options.library.is_absolute()
            || !options.sqlite_extension.is_absolute()
        {
            return Err(invalid(
                "DuckDB requires trusted absolute library/extension paths, 1..16 threads, 64..1024 MiB",
            ));
        }
        let api = Api::load(&options.library)?;
        let mut duck = Self {
            api,
            db: ptr::null_mut(),
            connection: ptr::null_mut(),
            stop: None,
            watchdog: None,
        };
        // SAFETY: all handles/output pointers are initialized; config freed on
        // every path. Duck owns partially initialized db/connection on failures.
        unsafe {
            if message((duck.api.duckdb_library_version)()) != "v1.5.6" {
                return Err(invalid("this experimental reader requires DuckDB v1.5.6"));
            }
            let mut config = ptr::null_mut();
            if (duck.api.duckdb_create_config)(&mut config) != 0 {
                return Err(invalid("cannot create DuckDB config"));
            }
            let configured = (|| {
                for (key, value) in [
                    ("threads", options.threads.to_string()),
                    ("memory_limit", format!("{}MB", options.memory_mb)),
                    ("autoload_known_extensions", "false".into()),
                    ("autoinstall_known_extensions", "false".into()),
                    ("allow_unsigned_extensions", "false".into()),
                    ("temp_directory", "".into()),
                ] {
                    if (duck.api.duckdb_set_config)(
                        config,
                        cstring(key)?.as_ptr(),
                        cstring(&value)?.as_ptr(),
                    ) != 0
                    {
                        return Err(invalid(format!("unsupported DuckDB configuration: {key}")));
                    }
                }
                let mut error = ptr::null_mut();
                let status = (duck.api.duckdb_open_ext)(
                    c":memory:".as_ptr(),
                    &mut duck.db,
                    config,
                    &mut error,
                );
                let diagnostic = if status != 0 {
                    Some(message(error))
                } else {
                    None
                };
                if !error.is_null() {
                    (duck.api.duckdb_free)(error.cast());
                }
                diagnostic.map_or(Ok(()), |v| Err(invalid(v)))
            })();
            (duck.api.duckdb_destroy_config)(&mut config);
            configured?;
            if (duck.api.duckdb_connect)(duck.db, &mut duck.connection) != 0 {
                return Err(invalid("cannot connect DuckDB"));
            }
        }
        let (stop, wait) = mpsc::channel();
        let connection = duck.connection as usize;
        let interrupt = duck.api.duckdb_interrupt;
        duck.stop = Some(stop);
        duck.watchdog = Some(thread::spawn(move || {
            if wait.recv_timeout(Duration::from_secs(120)) == Err(mpsc::RecvTimeoutError::Timeout) {
                // SAFETY: DuckDB documents interrupt as callable from another
                // thread; owner joins us before closing the live connection.
                unsafe {
                    interrupt(connection as Handle);
                }
            }
        }));
        duck.exec(&format!(
            "LOAD {}",
            literal(
                options
                    .sqlite_extension
                    .to_str()
                    .ok_or_else(|| invalid("extension path must be UTF-8"))?
            )
        ))?;
        if duck.query("SELECT current_setting('threads')", &[])?.rows
            != vec![vec![Cell::Integer(i64::from(options.threads))]]
        {
            return Err(invalid("DuckDB thread setting was not applied"));
        }
        Ok(duck)
    }

    fn exec(&self, sql: &str) -> Result<()> {
        let sql = cstring(sql)?;
        let mut result = RawResult::default();
        // SAFETY: valid connection, NUL-terminated SQL, initialized result. C
        // result must be destroyed on error as well as success.
        unsafe {
            let status = (self.api.duckdb_query)(self.connection, sql.as_ptr(), &mut result);
            let error = (status != 0)
                .then(|| invalid(message((self.api.duckdb_result_error)(&mut result))));
            (self.api.duckdb_destroy_result)(&mut result);
            error.map_or(Ok(()), Err)
        }
    }

    fn query(&self, sql: &str, params: &[Cell]) -> Result<QueryResult> {
        let sql = cstring(sql)?;
        let mut prepared = ptr::null_mut();
        let mut result = RawResult::default();
        // SAFETY: owned live connection, stable parameter buffers during bind,
        // and both C output handles destroyed unconditionally below.
        unsafe {
            let answer = (|| {
                if (self.api.duckdb_prepare)(self.connection, sql.as_ptr(), &mut prepared) != 0 {
                    return Err(invalid(message((self.api.duckdb_prepare_error)(prepared))));
                }
                if (self.api.duckdb_nparams)(prepared) != params.len() as u64 {
                    return Err(invalid("DuckDB parameter count mismatch"));
                }
                for (index, value) in params.iter().enumerate() {
                    let index = index as u64 + 1;
                    let status = match value {
                        Cell::Null => (self.api.duckdb_bind_null)(prepared, index),
                        Cell::Integer(v) => (self.api.duckdb_bind_int64)(prepared, index, *v),
                        Cell::Real(v) if v.is_finite() => {
                            (self.api.duckdb_bind_double)(prepared, index, *v)
                        }
                        Cell::Text(v) => (self.api.duckdb_bind_varchar_length)(
                            prepared,
                            index,
                            v.as_ptr().cast(),
                            v.len() as u64,
                        ),
                        Cell::Blob(v) => (self.api.duckdb_bind_blob)(
                            prepared,
                            index,
                            v.as_ptr().cast(),
                            v.len() as u64,
                        ),
                        _ => return Err(invalid("non-finite numeric parameter")),
                    };
                    if status != 0 {
                        return Err(invalid("DuckDB parameter binding failed"));
                    }
                }
                if (self.api.duckdb_execute_prepared)(prepared, &mut result) != 0 {
                    return Err(invalid(message((self.api.duckdb_result_error)(
                        &mut result,
                    ))));
                }
                self.read_result(&mut result)
            })();
            (self.api.duckdb_destroy_result)(&mut result);
            if !prepared.is_null() {
                (self.api.duckdb_destroy_prepare)(&mut prepared);
            }
            answer
        }
    }

    unsafe fn read_result(&self, raw: &mut RawResult) -> Result<QueryResult> {
        // SAFETY: caller keeps this successful materialized result live. Each
        // chunk is destroyed after copying its flat vectors. Do not combine this
        // interface with deprecated value getters: they truncate embedded NULs.
        unsafe {
            let count = (self.api.duckdb_column_count)(raw);
            if count > 256 {
                return Err(limit("DuckDB result exceeds column limit"));
            }
            let columns = (0..count)
                .map(|i| message((self.api.duckdb_column_name)(raw, i)))
                .collect();
            let mut rows = Vec::new();
            let mut bytes = 0;
            for chunk_index in 0..(self.api.duckdb_result_chunk_count)(*raw) {
                let mut chunk = (self.api.duckdb_result_get_chunk)(*raw, chunk_index);
                if chunk.is_null() {
                    return Err(invalid("DuckDB failed fetching result chunk"));
                }
                let copied: Result<()> = (|| {
                    let length = (self.api.duckdb_data_chunk_get_size)(chunk) as usize;
                    if length > MAX_ROWS.saturating_sub(rows.len()) {
                        return Err(limit("DuckDB result exceeds row limit"));
                    }
                    let mut batch = vec![Vec::with_capacity(count as usize); length];
                    for col in 0..count {
                        let kind = (self.api.duckdb_column_type)(raw, col);
                        let vector = (self.api.duckdb_data_chunk_get_vector)(chunk, col);
                        let data = (self.api.duckdb_vector_get_data)(vector);
                        let validity = (self.api.duckdb_vector_get_validity)(vector);
                        for (index, row) in batch.iter_mut().enumerate() {
                            let valid = validity.is_null()
                                || (*validity.add(index / 64) & (1u64 << (index % 64))) != 0;
                            let value = if !valid {
                                Cell::Null
                            } else if data.is_null() {
                                return Err(invalid("missing DuckDB vector data"));
                            } else {
                                match kind {
                                    1 => Cell::Integer(i64::from(*data.cast::<bool>().add(index))),
                                    2 => Cell::Integer(i64::from(*data.cast::<i8>().add(index))),
                                    3 => Cell::Integer(i64::from(*data.cast::<i16>().add(index))),
                                    4 => Cell::Integer(i64::from(*data.cast::<i32>().add(index))),
                                    5 => Cell::Integer(*data.cast::<i64>().add(index)),
                                    10 | 11 => {
                                        let value = if kind == 10 {
                                            f64::from(*data.cast::<f32>().add(index))
                                        } else {
                                            *data.cast::<f64>().add(index)
                                        };
                                        if !value.is_finite() {
                                            return Err(invalid("non-finite DuckDB result"));
                                        }
                                        Cell::Real(value)
                                    }
                                    17 | 18 => {
                                        let string = data.cast::<RawString>().add(index);
                                        let size = (*string).length as usize;
                                        if size > MAX_BYTES.saturating_sub(bytes) {
                                            return Err(limit("DuckDB result exceeds byte limit"));
                                        }
                                        let buffer = (self.api.duckdb_string_t_data)(string);
                                        let copied = if size == 0 {
                                            Vec::new()
                                        } else if buffer.is_null() {
                                            return Err(invalid("missing DuckDB string data"));
                                        } else {
                                            std::slice::from_raw_parts(buffer, size).to_vec()
                                        };
                                        if kind == 17 {
                                            Cell::Text(
                                                String::from_utf8(copied).map_err(storage_error)?,
                                            )
                                        } else {
                                            Cell::Blob(copied)
                                        }
                                    }
                                    _ => {
                                        return Err(invalid(format!(
                                            "DuckDB result type {kind} is outside the overlay scalar contract; cast it explicitly"
                                        )));
                                    }
                                }
                            };
                            bytes += value.size();
                            if bytes > MAX_BYTES {
                                return Err(limit("DuckDB result exceeds byte limit"));
                            }
                            row.push(value);
                        }
                    }
                    rows.extend(batch);
                    Ok(())
                })();
                (self.api.duckdb_destroy_data_chunk)(&mut chunk);
                copied?;
            }
            Ok(QueryResult { columns, rows })
        }
    }
}

impl Database {
    /// Experimental read over one table/key partition. Unlike `query`, this
    /// uses DuckDB SQL semantics. Callers MUST supply the intended routing key;
    /// rows in other partitions are intentionally outside this API's scope.
    /// Library/extension paths are trusted application configuration, not SQL.
    pub fn query_partition_duckdb(
        &mut self,
        table: &str,
        routing_key: &Cell,
        sql: &str,
        params: &[Cell],
        options: &DuckDbReadOptions,
    ) -> Result<QueryResult> {
        super::validate_statement(sql, false)?;
        // Reuse SQLite's existing authorizer as a fail-closed preflight: queries
        // cannot use external file/SQL table functions absent from this catalog.
        let preflight = self.connection.prepare(sql).map_err(storage_error)?;
        if !preflight.readonly() {
            return Err(invalid("DuckDB reader accepts SELECT only"));
        }
        drop(preflight);
        let table_number = self
            .config()
            .tables
            .iter()
            .position(|v| v.name == table)
            .ok_or_else(|| invalid("unknown table"))?;
        let schema = &self.config().tables[table_number];
        let kind = schema.columns[schema.routing_column()].kind;
        if !matches!(
            (kind, routing_key),
            (ColumnType::Integer, Cell::Integer(_))
                | (ColumnType::Text, Cell::Text(_))
                | (ColumnType::Blob, Cell::Blob(_))
        ) {
            return Err(invalid("routing key type differs from catalog"));
        }
        let partition = self.config().partition(routing_key)?;
        let (base, pending) = self.registry.duck_snapshot(table_number, partition)?;
        let duck = Duck::open(options)?;
        let definitions = schema
            .columns
            .iter()
            .map(|c| {
                format!(
                    "{} {}",
                    quote(&c.name),
                    match c.kind {
                        ColumnType::Integer => "BIGINT",
                        ColumnType::Real => "DOUBLE",
                        ColumnType::Text => "VARCHAR",
                        ColumnType::Blob => "BLOB",
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let base_sql = match base {
            Some(path) => format!(
                "SELECT {} FROM sqlite_scan({}, {})",
                schema.select_columns(),
                literal(
                    path.to_str()
                        .ok_or_else(|| invalid("base path must be UTF-8"))?
                ),
                literal(&schema.name)
            ),
            None => {
                duck.exec(&format!("CREATE TEMP TABLE __brisk_empty ({definitions})"))?;
                "SELECT * FROM __brisk_empty".into()
            }
        };
        let view_sql = if pending.is_empty() {
            base_sql
        } else {
            duck.exec(&format!(
                "CREATE TEMP TABLE __brisk_pending ({definitions}, __brisk_deleted BOOLEAN)"
            ))?;
            for (key, value) in pending {
                let deleted = value.is_none();
                let mut row = value.unwrap_or_else(|| vec![Cell::Null; schema.columns.len()]);
                if deleted {
                    let keys: Vec<Cell> = serde_json::from_slice(&key).map_err(storage_error)?;
                    if keys.len() != schema.primary_key.len() {
                        return Err(super::corrupt("invalid tombstone primary key"));
                    }
                    for (name, value) in schema.primary_key.iter().zip(keys) {
                        row[schema.columns.iter().position(|c| c.name == *name).unwrap()] = value;
                    }
                }
                row.push(Cell::Integer(i64::from(deleted)));
                duck.query(
                    &format!(
                        "INSERT INTO __brisk_pending VALUES ({})",
                        vec!["?"; row.len()].join(",")
                    ),
                    &row,
                )?;
            }
            let keys = schema
                .primary_key
                .iter()
                .map(|k| format!("b.{0}=p.{0}", quote(k)))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!(
                "SELECT b.* FROM ({base_sql}) b WHERE NOT EXISTS (SELECT 1 FROM __brisk_pending p WHERE {keys}) UNION ALL SELECT {} FROM __brisk_pending WHERE NOT __brisk_deleted",
                schema.select_columns()
            )
        };
        duck.exec(&format!(
            "CREATE TEMP VIEW {} AS {view_sql}",
            quote(&schema.name)
        ))?;
        duck.query(sql, params)
    }
}

#[cfg(test)]
mod tests {
    use super::RawResult;

    #[test]
    fn raw_result_starts_with_zero_counts_and_null_handles() {
        let result = RawResult::default();
        assert_eq!((result.columns, result.rows, result.changed), (0, 0, 0));
        assert!(result.column_ptr.is_null());
        assert!(result.error_ptr.is_null());
        assert!(result.internal.is_null());
    }
}
