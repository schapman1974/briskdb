//! Stock-SQLite virtual-table module bridge with transaction savepoints.
//!
//! rusqlite exposes writable virtual-table transaction callbacks through
//! a version-1 `sqlite3_module`, but does not expose SQLite's version-2
//! `xSavepoint`, `xRelease`, or `xRollbackTo` slots. This bridge preserves
//! rusqlite's callback implementation and ownership model while filling only
//! those three supported SQLite API slots. It does not patch or fork SQLite.

use std::{
    ffi::{CStr, c_int, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
    sync::{Arc, OnceLock},
};

use rusqlite::{Connection, Error as SqliteError, Result as SqliteResult, ffi};

use super::{BriskShardTable, Registry};

const MAX_VTAB_ERROR_MESSAGE_BYTES: usize = 4 * 1024;

/// Register the writable `brisk_shard` module through SQLite's supported
/// version-2 module API.
pub(super) fn register_module(
    connection: &Connection,
    registry: Arc<Registry>,
) -> SqliteResult<()> {
    let registry = Box::into_raw(Box::new(registry)).cast::<c_void>();
    // SAFETY: `Connection::handle` is valid for the lifetime of `connection`.
    // `writable_module_v2` has static storage, the module name is NUL
    // terminated, and `registry` is a boxed `Arc<Registry>`. SQLite owns that
    // box after this call and invokes `drop_registry` both on registration
    // failure and when the module is no longer needed.
    let result_code = unsafe {
        ffi::sqlite3_create_module_v2(
            connection.handle(),
            c"brisk_shard".as_ptr(),
            writable_module_v2(),
            registry,
            Some(drop_registry),
        )
    };
    if result_code == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(connection_error(connection, result_code))
    }
}

fn writable_module_v2() -> &'static ffi::sqlite3_module {
    static MODULE: OnceLock<ffi::sqlite3_module> = OnceLock::new();
    MODULE.get_or_init(|| {
        let rusqlite_module = rusqlite::vtab::update_module_with_tx::<BriskShardTable>();
        // SAFETY: rusqlite documents `Module` as `#[repr(transparent)]` over
        // `ffi::sqlite3_module`. The returned module is static and its raw
        // callback table is `Copy`; copying it preserves every stock rusqlite
        // callback and its ABI.
        let mut module = unsafe { *ptr::from_ref(rusqlite_module).cast::<ffi::sqlite3_module>() };
        module.iVersion = 2;
        module.xSavepoint = Some(savepoint_callback);
        module.xRelease = Some(release_callback);
        module.xRollbackTo = Some(rollback_to_callback);
        module
    })
}

unsafe extern "C" fn drop_registry(registry: *mut c_void) {
    if !registry.is_null() {
        // SAFETY: `register_module` created exactly one `Box<Arc<Registry>>`
        // for this pointer and SQLite invokes this destructor at most once.
        drop(unsafe { Box::from_raw(registry.cast::<Arc<Registry>>()) });
    }
}

unsafe extern "C" fn savepoint_callback(table: *mut ffi::sqlite3_vtab, savepoint: c_int) -> c_int {
    run_callback(table, |table| table.savepoint(savepoint))
}

unsafe extern "C" fn release_callback(table: *mut ffi::sqlite3_vtab, savepoint: c_int) -> c_int {
    run_callback(table, |table| table.release(savepoint))
}

unsafe extern "C" fn rollback_to_callback(
    table: *mut ffi::sqlite3_vtab,
    savepoint: c_int,
) -> c_int {
    run_callback(table, |table| table.rollback_to(savepoint))
}

fn run_callback(
    table: *mut ffi::sqlite3_vtab,
    callback: impl FnOnce(&mut BriskShardTable) -> SqliteResult<()>,
) -> c_int {
    if table.is_null() {
        return ffi::SQLITE_MISUSE;
    }

    // A panic must never cross SQLite's C ABI boundary. The table allocation
    // is a `BriskShardTable` because every other callback in this module table
    // is the stock rusqlite callback specialized for that same type.
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: SQLite passes the live virtual-table object created by
        // rusqlite. `BriskShardTable` has `sqlite3_vtab` as its first field and
        // the callback is serialized by the owning SQLite connection.
        let result = callback(unsafe { &mut *table.cast::<BriskShardTable>() });
        callback_result(table, result)
    }));
    match result {
        Ok(result_code) => result_code,
        Err(_) => {
            // SAFETY: `table` was checked non-null and is live for this
            // callback. Allocation uses SQLite's allocator as required for
            // `sqlite3_vtab.zErrMsg`.
            unsafe {
                set_error_message(table, "brisk_shard savepoint callback panicked");
            }
            ffi::SQLITE_ABORT
        }
    }
}

fn callback_result(table: *mut ffi::sqlite3_vtab, result: SqliteResult<()>) -> c_int {
    match result {
        Ok(()) => ffi::SQLITE_OK,
        Err(SqliteError::SqliteFailure(error, message)) => {
            if let Some(message) = message {
                // SAFETY: `run_callback` validated the live table pointer.
                unsafe {
                    set_error_message(table, &message);
                }
            }
            if error.extended_code == ffi::SQLITE_OK {
                ffi::SQLITE_ERROR
            } else {
                error.extended_code
            }
        }
        Err(error) => {
            // SAFETY: `run_callback` validated the live table pointer.
            unsafe {
                set_error_message(table, &error.to_string());
            }
            ffi::SQLITE_ERROR
        }
    }
}

unsafe fn set_error_message(table: *mut ffi::sqlite3_vtab, message: &str) {
    let mut length = message.len().min(MAX_VTAB_ERROR_MESSAGE_BYTES);
    while !message.is_char_boundary(length) {
        length -= 1;
    }
    let allocation_size = u64::try_from(length.saturating_add(1))
        .expect("the bounded virtual-table error allocation fits in u64");
    // SAFETY: SQLite owns and later frees `zErrMsg`, so it must be allocated
    // with SQLite's allocator. The bounded size always fits in `u64`.
    let allocation = unsafe { ffi::sqlite3_malloc64(allocation_size) }.cast::<u8>();
    if allocation.is_null() {
        // Preserve no stale diagnostic when replacing the message fails.
        if !unsafe { (*table).zErrMsg }.is_null() {
            // SAFETY: an existing zErrMsg is owned by SQLite's allocator.
            unsafe { ffi::sqlite3_free((*table).zErrMsg.cast::<c_void>()) };
            unsafe { (*table).zErrMsg = ptr::null_mut() };
        }
        return;
    }

    for (index, byte) in message.as_bytes()[..length].iter().copied().enumerate() {
        // Interior NUL bytes would truncate SQLite's diagnostic. Rust strings
        // can contain NUL, so replace them with a harmless printable byte.
        unsafe {
            allocation
                .add(index)
                .write(if byte == 0 { b'?' } else { byte })
        };
    }
    unsafe { allocation.add(length).write(0) };

    if !unsafe { (*table).zErrMsg }.is_null() {
        // SAFETY: an existing zErrMsg is owned by SQLite's allocator.
        unsafe { ffi::sqlite3_free((*table).zErrMsg.cast::<c_void>()) };
    }
    unsafe { (*table).zErrMsg = allocation.cast() };
}

fn connection_error(connection: &Connection, result_code: c_int) -> SqliteError {
    // SAFETY: the connection remains live after a failed module registration.
    let handle = unsafe { connection.handle() };
    let extended_code = unsafe { ffi::sqlite3_extended_errcode(handle) };
    let effective_code = if extended_code == ffi::SQLITE_OK {
        result_code
    } else {
        extended_code
    };
    let message = unsafe {
        let message = ffi::sqlite3_errmsg(handle);
        (!message.is_null()).then(|| CStr::from_ptr(message).to_string_lossy().into_owned())
    };
    SqliteError::SqliteFailure(ffi::Error::new(effective_code), message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_preserves_writable_callbacks_and_adds_only_v2_savepoints() {
        let base = rusqlite::vtab::update_module_with_tx::<BriskShardTable>();
        // SAFETY: the same repr-transparent invariant used by the bridge.
        let base = unsafe { &*ptr::from_ref(base).cast::<ffi::sqlite3_module>() };
        let bridged = writable_module_v2();

        assert_eq!(base.iVersion, 1);
        assert!(base.xUpdate.is_some());
        assert!(base.xBegin.is_some());
        assert!(base.xSync.is_some());
        assert!(base.xCommit.is_some());
        assert!(base.xRollback.is_some());
        assert!(base.xSavepoint.is_none());
        assert!(base.xRelease.is_none());
        assert!(base.xRollbackTo.is_none());

        assert_eq!(bridged.iVersion, 2);
        assert!(bridged.xUpdate.is_some());
        assert!(bridged.xBegin.is_some());
        assert!(bridged.xSync.is_some());
        assert!(bridged.xCommit.is_some());
        assert!(bridged.xRollback.is_some());
        assert!(bridged.xSavepoint.is_some());
        assert!(bridged.xRelease.is_some());
        assert!(bridged.xRollbackTo.is_some());
    }

    #[test]
    fn error_messages_use_sqlite_ownership_and_sanitize_nul_bytes() {
        let mut table = ffi::sqlite3_vtab::default();
        unsafe {
            set_error_message(&mut table, "first\0message");
        }
        assert!(!table.zErrMsg.is_null());
        let first = unsafe { CStr::from_ptr(table.zErrMsg) };
        assert_eq!(first.to_bytes(), b"first?message");

        unsafe {
            set_error_message(&mut table, "replacement");
        }
        assert!(!table.zErrMsg.is_null());
        let replacement = unsafe { CStr::from_ptr(table.zErrMsg) };
        assert_eq!(replacement.to_bytes(), b"replacement");

        unsafe {
            ffi::sqlite3_free(table.zErrMsg.cast::<c_void>());
        }
    }

    #[test]
    fn null_table_callback_fails_without_invoking_rust() {
        let invoked = std::cell::Cell::new(false);
        let result = run_callback(ptr::null_mut(), |_| {
            invoked.set(true);
            Ok(())
        });
        assert_eq!(result, ffi::SQLITE_MISUSE);
        assert!(!invoked.get());
    }
}
