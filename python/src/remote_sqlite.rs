use std::ffi::{c_char, c_int, c_void};

unsafe extern "C" {
    fn briskdb_remote_init(db: *mut c_void, error: *mut *mut c_char, api: *const c_void) -> c_int;
}

// SQLite derives this name from _briskdb.abi3.so. Keep the ABI completely
// separate from libsqlite3_sys: db belongs to Python's host SQLite library.
#[unsafe(no_mangle)]
unsafe extern "C" fn sqlite3_briskdb_init(
    db: *mut c_void,
    error: *mut *mut c_char,
    api: *const c_void,
) -> c_int {
    unsafe { briskdb_remote_init(db, error, api) }
}
