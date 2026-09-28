//! Bounded reuse of private manifest read handles, not cached catalog authority.

use std::{path::Path, sync::Mutex};

use rusqlite::{Connection, MAIN_DB, ffi};

use super::*;

const MAX_IDLE_READERS: usize = 2;

#[derive(Debug, Default)]
pub(super) struct ManifestReaders {
    idle: Mutex<Vec<Connection>>,
    #[cfg(test)]
    opened: std::sync::atomic::AtomicUsize,
}

impl ManifestReaders {
    /// Call only for storage-owned reads while schema admission is held.
    /// Handles remain read/write-capable for SQLite hot-journal recovery, as
    /// before, but never escape to public SQL or execute user statements.
    pub(super) fn read<T>(
        &self,
        path: &Path,
        control: Arc<OperationControl>,
        read: impl FnOnce(&mut Connection) -> EngineResult<T>,
    ) -> EngineResult<T> {
        check_active(&control)?;
        let cached = self.lock()?.pop();
        let mut connection = match cached {
            Some(connection)
                if connection.is_autocommit() && file_is_current(&connection, path)? =>
            {
                connection
            }
            _ => {
                let connection = open_existing_manifest(path)?;
                #[cfg(test)]
                self.opened
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                connection
            }
        };
        check_active(&control)?;
        // Installs and removes this request's busy/progress/interrupt hooks.
        // Any error (including cleanup failure) or unwind drops the handle.
        let value = pool::run_dedicated_connection_controlled(
            &mut connection,
            Arc::clone(&control),
            |connection| {
                configure_manifest_connection_after_busy_setup(connection)?;
                read(connection)
            },
        )?;
        check_active(&control)?;
        if connection.is_autocommit() {
            let mut idle = self.lock()?;
            if idle.len() < MAX_IDLE_READERS && idle.try_reserve(1).is_ok() {
                idle.push(connection);
            }
        }
        Ok(value)
    }

    fn lock(&self) -> EngineResult<MutexGuard<'_, Vec<Connection>>> {
        self.idle.lock().map_err(|_| {
            EngineError::new(
                EngineErrorKind::Internal,
                "manifest reader pool is poisoned",
            )
        })
    }

    pub(super) fn close_idle(&self) -> EngineResult<usize> {
        let closing = std::mem::take(&mut *self.lock()?);
        let count = closing.len();
        drop(closing);
        Ok(count)
    }
}

fn check_active(control: &OperationControl) -> EngineResult<()> {
    match control.reason() {
        Some(reason) => Err(reason.error()),
        None => Ok(()),
    }
}

fn file_is_current(connection: &Connection, path: &Path) -> EngineResult<bool> {
    validate_existing_manifest_file(path)?;
    canonical_manifest_open_path(path)?;
    let mut moved: std::ffi::c_int = 0;
    // SAFETY: this live handle is exclusively checked out. SQLite borrows the
    // database name and writes only the supplied integer; no pointer escapes.
    let code = unsafe {
        ffi::sqlite3_file_control(
            connection.handle(),
            MAIN_DB.as_ptr(),
            ffi::SQLITE_FCNTL_HAS_MOVED,
            std::ptr::from_mut(&mut moved).cast(),
        )
    };
    interpret_identity_probe(code, moved)
}

fn interpret_identity_probe(code: i32, moved: i32) -> EngineResult<bool> {
    match code {
        ffi::SQLITE_OK if moved == 0 => Ok(true),
        ffi::SQLITE_OK => Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "pooled manifest file was moved, removed, or replaced",
        )),
        // Unsupported probes never grant reuse authority. Open and validate a
        // fresh no-create/no-follow connection instead.
        ffi::SQLITE_NOTFOUND => Ok(false),
        code => Err(sqlite_error::storage(rusqlite::Error::SqliteFailure(
            ffi::Error::new(code),
            None,
        ))
        .context("failed to validate pooled manifest file identity")),
    }
}

#[cfg(test)]
mod tests;
