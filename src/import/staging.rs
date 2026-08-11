//! Private staging and atomic publication for offline imports.

use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    time::Duration,
};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::ffi::CString;

use rusqlite::{Connection, OpenFlags};
use same_file::Handle;

use crate::{
    core::{CancellationToken, EngineError, EngineErrorKind, EngineResult},
    sqlite_error,
};

const RECEIPT_FILE_NAME: &str = "briskdb-import-receipt.json";
const RANDOM_STAGE_ATTEMPTS: usize = 128;
const CHECKPOINT_BUSY_TIMEOUT: Duration = Duration::from_millis(250);

/// An unpublished sibling layout held under a destination-specific lock.
///
/// The destination must be absent. Dropping the guard before publication
/// removes only the randomly named staging directory on a best-effort basis.
/// `publish` consumes the guard so the target lock covers the rename commit.
#[derive(Debug)]
pub(super) struct StagingLayout {
    parent: PathBuf,
    destination: PathBuf,
    stage: PathBuf,
    stage_identity: Handle,
    _target_lock: TargetLock,
    layout_synced: bool,
    published: bool,
}

impl StagingLayout {
    /// Reserve an absent destination and create a hidden sibling directory.
    pub(super) fn create(source: &Path, destination: &Path) -> EngineResult<Self> {
        let source = canonical_source(source)?;
        let (parent, destination) = canonical_destination(destination)?;
        reject_existing_destination(&source, &destination)?;

        let target_lock = TargetLock::acquire(&parent, &destination)?;
        // The first inspection intentionally happened before creating the
        // persistent lock file. Repeat it while cooperative importers are
        // serialized; publication's no-replace rename closes the race with
        // every non-cooperating process.
        reject_existing_destination(&source, &destination)?;
        let stage = create_random_stage(&parent, &source)?;
        let stage_identity = open_stage_identity(&stage)?;

        Ok(Self {
            parent,
            destination,
            stage,
            stage_identity,
            _target_lock: target_lock,
            layout_synced: false,
            published: false,
        })
    }

    /// Return the private root in which the importer builds the new layout.
    pub(super) fn path(&self) -> &Path {
        &self.stage
    }

    /// Return the canonical final path reserved by this guard.
    #[cfg(test)]
    fn destination(&self) -> &Path {
        &self.destination
    }

    /// Checkpoint and durably sync every known file in a completed layout.
    ///
    /// All importer-owned SQLite connections must be closed first. The receipt
    /// must already exist, and no layout mutation may follow this call before
    /// `publish`.
    pub(super) fn sync_layout(
        &mut self,
        shard_count: u16,
        cancellation: &CancellationToken,
    ) -> EngineResult<()> {
        crate::storage::validate_shard_count(shard_count)?;
        self.layout_synced = false;
        ensure_sync_not_cancelled(cancellation)?;
        self.ensure_stage_identity()?;

        let manifest = self.stage.join("manifest.sqlite");
        let receipt = self.stage.join(RECEIPT_FILE_NAME);
        let shards = self.stage.join("shards");
        require_regular_file(&manifest)?;
        require_regular_file(&receipt)?;
        require_real_directory(&shards)?;

        let mut databases = Vec::with_capacity(usize::from(shard_count) + 1);
        databases.push(manifest);
        for shard_id in 0..shard_count {
            let shard = shards.join(format!("{shard_id:04}.sqlite"));
            require_regular_file(&shard)?;
            databases.push(shard);
        }

        for database in &databases {
            ensure_sync_not_cancelled(cancellation)?;
            checkpoint_wal(database)?;
        }
        for database in &databases {
            ensure_sync_not_cancelled(cancellation)?;
            sync_regular_file(database)?;
            sync_optional_regular_file(&wal_path(database))?;
        }
        ensure_sync_not_cancelled(cancellation)?;
        sync_regular_file(&receipt)?;
        sync_directory(&shards)?;
        sync_directory(&self.stage)?;
        sync_directory(&self.parent)?;
        self.ensure_stage_identity()?;
        self.layout_synced = true;
        Ok(())
    }

    /// Atomically publish the staged layout without replacing any path.
    ///
    /// Cancellation is observed immediately before the rename. A successful
    /// rename is the commit point: later cancellation or a parent-directory
    /// sync error cannot turn publication into a retryable failure.
    pub(super) fn publish(mut self, cancellation: &CancellationToken) -> EngineResult<()> {
        if !self.layout_synced {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "SQLite import staging layout must be synced before publication",
            ));
        }
        self.ensure_stage_identity()?;
        require_real_directory(&self.stage)?;
        require_absent_destination(&self.destination)?;
        if cancellation.is_cancelled() {
            return Err(EngineError::new(
                EngineErrorKind::Cancelled,
                "SQLite import was cancelled before publication",
            ));
        }

        atomic_rename_noreplace(&self.stage, &self.destination).map_err(|error| {
            sqlite_error::storage_io(
                error,
                format!(
                    "failed to publish SQLite import at {}",
                    self.destination.display()
                ),
            )
        })?;
        self.published = true;
        match open_stage_identity(&self.destination) {
            Ok(published_identity) if published_identity == self.stage_identity => {}
            Ok(_) => tracing::error!(
                destination = %self.destination.display(),
                "published SQLite import no longer has its verified staging identity"
            ),
            Err(error) => tracing::warn!(
                destination = %self.destination.display(),
                error = %error,
                "published SQLite import could not be identity-checked after commit"
            ),
        }

        if let Err(error) = sync_directory(&self.parent) {
            // The final name is already committed. Reporting an ordinary
            // failure would encourage a retry against a complete destination.
            tracing::warn!(
                destination = %self.destination.display(),
                error = %error,
                "SQLite import was published but its parent directory sync failed"
            );
        }
        Ok(())
    }

    fn ensure_stage_identity(&self) -> EngineResult<()> {
        let current = open_stage_identity(&self.stage)?;
        if current == self.stage_identity {
            Ok(())
        } else {
            Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "SQLite import staging directory was replaced before publication",
            ))
        }
    }
}

impl Drop for StagingLayout {
    fn drop(&mut self) {
        if !self.published && stage_matches_identity(&self.stage, &self.stage_identity) {
            cleanup_unpublished_stage(&self.stage);
        }
    }
}

fn open_stage_identity(path: &Path) -> EngineResult<Handle> {
    Handle::from_path(path).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!(
                "failed to inspect SQLite import staging identity {}",
                path.display()
            ),
        )
    })
}

fn stage_matches_identity(path: &Path, expected: &Handle) -> bool {
    Handle::from_path(path).is_ok_and(|current| &current == expected)
}

fn ensure_sync_not_cancelled(cancellation: &CancellationToken) -> EngineResult<()> {
    if cancellation.is_cancelled() {
        Err(EngineError::new(
            EngineErrorKind::Cancelled,
            "SQLite import was cancelled while synchronizing its staging layout",
        ))
    } else {
        Ok(())
    }
}

fn canonical_source(source: &Path) -> EngineResult<PathBuf> {
    let source = fs::canonicalize(source).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::FailedPrecondition,
            format!(
                "failed to resolve SQLite import source {}",
                source.display()
            ),
            error,
        )
    })?;
    let metadata = fs::metadata(&source).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!(
                "failed to inspect SQLite import source {}",
                source.display()
            ),
        )
    })?;
    if !metadata.is_file() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "SQLite import source {} is not a regular file",
                source.display()
            ),
        ));
    }
    Ok(source)
}

fn canonical_destination(destination: &Path) -> EngineResult<(PathBuf, PathBuf)> {
    let file_name = destination.file_name().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::InvalidArgument,
            "SQLite import destination must name a new directory",
        )
    })?;
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::FailedPrecondition,
            format!(
                "failed to resolve SQLite import destination parent {}",
                parent.display()
            ),
            error,
        )
    })?;
    require_real_directory(&parent)?;
    let destination = parent.join(file_name);
    Ok((parent, destination))
}

fn reject_existing_destination(source: &Path, destination: &Path) -> EngineResult<()> {
    match fs::symlink_metadata(destination) {
        Ok(metadata) => {
            let aliases_source = fs::canonicalize(destination)
                .ok()
                .is_some_and(|existing| existing == source)
                || same_file_identity(source, destination);
            if aliases_source {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "SQLite import source and destination resolve to the same file",
                ));
            }
            let kind = if metadata.file_type().is_symlink() {
                "a symbolic link"
            } else {
                "an existing path"
            };
            Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "SQLite import destination {} is {kind}",
                    destination.display()
                ),
            ))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if source == destination {
                Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "SQLite import source and destination resolve to the same file",
                ))
            } else {
                Ok(())
            }
        }
        Err(error) => Err(sqlite_error::storage_io(
            error,
            format!(
                "failed to inspect SQLite import destination {}",
                destination.display()
            ),
        )),
    }
}

fn require_absent_destination(destination: &Path) -> EngineResult<()> {
    match fs::symlink_metadata(destination) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "SQLite import destination {} appeared before publication",
                destination.display()
            ),
        )),
        Err(error) => Err(sqlite_error::storage_io(
            error,
            format!(
                "failed to recheck SQLite import destination {}",
                destination.display()
            ),
        )),
    }
}

#[cfg(unix)]
fn same_file_identity(first: &Path, second: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;

    let Ok(first) = fs::metadata(first) else {
        return false;
    };
    let Ok(second) = fs::metadata(second) else {
        return false;
    };
    first.dev() == second.dev() && first.ino() == second.ino()
}

#[cfg(not(unix))]
fn same_file_identity(_first: &Path, _second: &Path) -> bool {
    false
}

fn create_random_stage(parent: &Path, source: &Path) -> EngineResult<PathBuf> {
    for _ in 0..RANDOM_STAGE_ATTEMPTS {
        let mut random = [0_u8; 16];
        getrandom::fill(&mut random).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::StorageUnavailable,
                "failed to generate a private SQLite import staging name",
                error,
            )
        })?;
        let suffix = hex_bytes(&random);
        let stage = parent.join(format!(".briskdb-import-stage-{suffix}"));
        if stage == source {
            continue;
        }

        let result = create_private_directory(&stage);
        match result {
            Ok(()) => return Ok(stage),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(sqlite_error::storage_io(
                    error,
                    format!(
                        "failed to create SQLite import staging directory in {}",
                        parent.display()
                    ),
                ));
            }
        }
    }
    Err(EngineError::new(
        EngineErrorKind::StorageUnavailable,
        "failed to reserve a unique SQLite import staging directory",
    ))
}

#[cfg(unix)]
fn create_private_directory(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700).create(path)
}

#[cfg(not(unix))]
fn create_private_directory(path: &Path) -> io::Result<()> {
    fs::create_dir(path)
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn checkpoint_wal(path: &Path) -> EngineResult<()> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_EXRESCODE;
    let connection = Connection::open_with_flags(path, flags).map_err(sqlite_error::storage)?;
    connection
        .busy_timeout(CHECKPOINT_BUSY_TIMEOUT)
        .map_err(sqlite_error::storage)?;
    let journal_mode = connection
        .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
        .map_err(sqlite_error::storage)?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "SQLite import layout file {} is not in WAL mode",
                path.display()
            ),
        ));
    }
    let (busy, log_frames, checkpointed_frames) = connection
        .query_row("PRAGMA main.wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(sqlite_error::storage)?;
    if busy != 0 || log_frames != checkpointed_frames {
        return Err(EngineError::new(
            EngineErrorKind::Busy,
            format!(
                "SQLite import layout file {} could not be fully checkpointed",
                path.display()
            ),
        ));
    }
    connection
        .close()
        .map_err(|(_, error)| sqlite_error::storage(error))?;
    Ok(())
}

fn require_regular_file(path: &Path) -> EngineResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!(
                "failed to inspect SQLite import layout file {}",
                path.display()
            ),
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "SQLite import layout path {} is not a regular file",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn require_real_directory(path: &Path) -> EngineResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!(
                "failed to inspect SQLite import directory {}",
                path.display()
            ),
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "SQLite import path {} is not a real directory",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn sync_regular_file(path: &Path) -> EngineResult<()> {
    require_regular_file(path)?;
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| {
            sqlite_error::storage_io(
                error,
                format!(
                    "failed to sync SQLite import layout file {}",
                    path.display()
                ),
            )
        })
}

fn sync_optional_regular_file(path: &Path) -> EngineResult<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) => sync_regular_file(path),
        Err(error) => Err(sqlite_error::storage_io(
            error,
            format!("failed to inspect SQLite import sidecar {}", path.display()),
        )),
    }
}

fn sync_directory(path: &Path) -> EngineResult<()> {
    require_real_directory(path)?;
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            sqlite_error::storage_io(
                error,
                format!("failed to sync SQLite import directory {}", path.display()),
            )
        })
}

fn wal_path(database: &Path) -> PathBuf {
    let mut path = database.as_os_str().to_os_string();
    path.push("-wal");
    PathBuf::from(path)
}

fn cleanup_unpublished_stage(stage: &Path) {
    match fs::symlink_metadata(stage) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            let _ = fs::remove_file(stage);
        }
        Ok(metadata) if metadata.is_dir() => {
            let _ = fs::remove_dir_all(stage);
        }
        Ok(_) => {
            let _ = fs::remove_file(stage);
        }
        Err(_) => {}
    }
}

#[derive(Debug)]
struct TargetLock {
    _file: File,
}

impl TargetLock {
    fn acquire(parent: &Path, destination: &Path) -> EngineResult<Self> {
        let digest = blake3::hash(destination.as_os_str().as_encoded_bytes());
        let lock_path = parent.join(format!(
            ".briskdb-import-lock-{}",
            hex_bytes(digest.as_bytes())
        ));
        reject_lock_symlink(&lock_path)?;
        let file = open_lock_file(&lock_path).map_err(|error| {
            sqlite_error::storage_io(
                error,
                format!(
                    "failed to open SQLite import destination lock {}",
                    lock_path.display()
                ),
            )
        })?;
        lock_exclusive_nonblocking(&file).map_err(|error| {
            if error.kind() == io::ErrorKind::WouldBlock {
                EngineError::from_source(
                    EngineErrorKind::Busy,
                    format!(
                        "another SQLite importer owns destination {}",
                        destination.display()
                    ),
                    error,
                )
            } else {
                sqlite_error::storage_io(
                    error,
                    format!(
                        "failed to lock SQLite import destination {}",
                        destination.display()
                    ),
                )
            }
        })?;
        Ok(Self { _file: file })
    }
}

fn reject_lock_symlink(path: &Path) -> EngineResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "SQLite import destination lock {} is not a regular file",
                    path.display()
                ),
            ))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(sqlite_error::storage_io(
            error,
            format!(
                "failed to inspect SQLite import destination lock {}",
                path.display()
            ),
        )),
    }
}

#[cfg(unix)]
fn open_lock_file(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(path)
}

#[cfg(windows)]
fn open_lock_file(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;

    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .share_mode(0)
        .open(path)
}

#[cfg(not(any(unix, windows)))]
fn open_lock_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
}

#[cfg(unix)]
fn lock_exclusive_nonblocking(file: &File) -> io::Result<()> {
    use std::os::{raw::c_int, unix::io::AsRawFd};

    const LOCK_EX: c_int = 2;
    const LOCK_NB: c_int = 4;
    unsafe extern "C" {
        #[link_name = "flock"]
        fn c_flock(file_descriptor: c_int, operation: c_int) -> c_int;
    }

    // SAFETY: `file` owns a live descriptor for this call and `flock` neither
    // retains the descriptor nor dereferences a Rust pointer.
    let result = unsafe { c_flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(windows)]
fn lock_exclusive_nonblocking(_file: &File) -> io::Result<()> {
    // `share_mode(0)` on the retained handle is the Windows lock.
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn lock_exclusive_nonblocking(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn path_c_string(path: &Path) -> io::Result<CString> {
    use std::os::unix::ffi::OsStrExt;

    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "SQLite import path contains a NUL byte",
        )
    })
}

#[cfg(target_os = "linux")]
fn atomic_rename_noreplace(source: &Path, destination: &Path) -> io::Result<()> {
    use std::os::raw::{c_char, c_int, c_uint};

    const AT_FDCWD: c_int = -100;
    const RENAME_NOREPLACE: c_uint = 1;
    unsafe extern "C" {
        fn renameat2(
            old_directory: c_int,
            old_path: *const c_char,
            new_directory: c_int,
            new_path: *const c_char,
            flags: c_uint,
        ) -> c_int;
    }

    let source = path_c_string(source)?;
    let destination = path_c_string(destination)?;
    // SAFETY: both C strings are NUL-terminated and remain live for the call;
    // the function retains neither pointer. Both directory descriptors use the
    // documented `AT_FDCWD` sentinel.
    let result = unsafe {
        renameat2(
            AT_FDCWD,
            source.as_ptr(),
            AT_FDCWD,
            destination.as_ptr(),
            RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "macos")]
fn atomic_rename_noreplace(source: &Path, destination: &Path) -> io::Result<()> {
    use std::os::raw::{c_char, c_int, c_uint};

    const RENAME_EXCL: c_uint = 0x0000_0004;
    unsafe extern "C" {
        fn renamex_np(old_path: *const c_char, new_path: *const c_char, flags: c_uint) -> c_int;
    }

    let source = path_c_string(source)?;
    let destination = path_c_string(destination)?;
    // SAFETY: both C strings are NUL-terminated and remain live for the call;
    // `renamex_np` retains neither pointer.
    let result = unsafe { renamex_np(source.as_ptr(), destination.as_ptr(), RENAME_EXCL) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(windows)]
fn atomic_rename_noreplace(source: &Path, destination: &Path) -> io::Result<()> {
    // Windows' rename primitive fails when the destination already exists.
    fs::rename(source, destination)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn atomic_rename_noreplace(_source: &Path, _destination: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic no-replace directory publication is unsupported on this target",
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        process::{Command, Stdio},
        sync::{Arc, Barrier},
        thread,
        time::{Duration, Instant},
    };

    use tempfile::TempDir;

    use super::*;

    const CHILD_SOURCE_ENV: &str = "BRISKDB_STAGING_TEST_SOURCE";
    const CHILD_DESTINATION_ENV: &str = "BRISKDB_STAGING_TEST_DESTINATION";
    const CHILD_READY_ENV: &str = "BRISKDB_STAGING_TEST_READY";
    const CHILD_RELEASE_ENV: &str = "BRISKDB_STAGING_TEST_RELEASE";

    fn source_file(root: &Path) -> PathBuf {
        let source = root.join("source.sqlite");
        fs::write(&source, b"source remains untouched").unwrap();
        source
    }

    fn create_wal_database(path: &Path) {
        let connection = Connection::open(path).unwrap();
        let mode = connection
            .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))
            .unwrap();
        assert!(mode.eq_ignore_ascii_case("wal"));
        connection
            .execute_batch(
                "CREATE TABLE marker(id INTEGER PRIMARY KEY); INSERT INTO marker VALUES(1);",
            )
            .unwrap();
        connection.close().unwrap();
    }

    fn complete_layout(layout: &mut StagingLayout, shard_count: u16) {
        create_wal_database(&layout.path().join("manifest.sqlite"));
        fs::write(
            layout.path().join(RECEIPT_FILE_NAME),
            b"{\"complete\":true}\n",
        )
        .unwrap();
        fs::create_dir(layout.path().join("shards")).unwrap();
        for shard_id in 0..shard_count {
            create_wal_database(
                &layout
                    .path()
                    .join("shards")
                    .join(format!("{shard_id:04}.sqlite")),
            );
        }
        layout
            .sync_layout(shard_count, &CancellationToken::new())
            .unwrap();
    }

    #[test]
    fn creates_a_private_random_sibling_and_drop_cleans_only_it() {
        let temp = TempDir::new().unwrap();
        let source = source_file(temp.path());
        let destination = temp.path().join("database");
        let layout = StagingLayout::create(&source, &destination).unwrap();
        let stage = layout.path().to_owned();
        let canonical_destination = fs::canonicalize(temp.path()).unwrap().join("database");
        assert_eq!(layout.destination(), canonical_destination);
        assert_eq!(stage.parent(), canonical_destination.parent());
        assert!(
            stage
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(".briskdb-import-stage-")
        );
        assert!(stage.is_dir());
        drop(layout);
        assert!(!stage.exists());
        assert_eq!(fs::read(source).unwrap(), b"source remains untouched");
    }

    #[test]
    fn rejects_existing_alias_and_symbolic_link_destinations() {
        let temp = TempDir::new().unwrap();
        let source = source_file(temp.path());
        let alias = StagingLayout::create(&source, &source).unwrap_err();
        assert_eq!(alias.kind(), EngineErrorKind::FailedPrecondition);

        let existing = temp.path().join("existing");
        fs::create_dir(&existing).unwrap();
        let error = StagingLayout::create(&source, &existing).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let link = temp.path().join("link");
            symlink(temp.path().join("missing-target"), &link).unwrap();
            let error = StagingLayout::create(&source, &link).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        }
    }

    #[test]
    fn target_lock_serializes_live_guards_and_releases_on_drop() {
        let temp = TempDir::new().unwrap();
        let source = source_file(temp.path());
        let destination = temp.path().join("database");
        let first = StagingLayout::create(&source, &destination).unwrap();
        let busy = StagingLayout::create(&source, &destination).unwrap_err();
        assert_eq!(busy.kind(), EngineErrorKind::Busy);
        drop(first);
        drop(StagingLayout::create(&source, &destination).unwrap());
    }

    #[test]
    fn cancellation_before_rename_cleans_stage_and_publishes_nothing() {
        let temp = TempDir::new().unwrap();
        let source = source_file(temp.path());
        let destination = temp.path().join("database");
        let mut layout = StagingLayout::create(&source, &destination).unwrap();
        let stage = layout.path().to_owned();
        complete_layout(&mut layout, 2);
        let cancellation = CancellationToken::new();
        assert!(cancellation.cancel());
        let error = layout.publish(&cancellation).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        assert!(!stage.exists());
        assert!(!destination.exists());
    }

    #[test]
    fn cancellation_before_layout_sync_cleans_stage_and_publishes_nothing() {
        let temp = TempDir::new().unwrap();
        let source = source_file(temp.path());
        let destination = temp.path().join("database");
        let mut layout = StagingLayout::create(&source, &destination).unwrap();
        let stage = layout.path().to_owned();
        create_wal_database(&layout.path().join("manifest.sqlite"));
        fs::write(layout.path().join(RECEIPT_FILE_NAME), b"{}\n").unwrap();
        fs::create_dir(layout.path().join("shards")).unwrap();
        create_wal_database(&layout.path().join("shards/0000.sqlite"));
        create_wal_database(&layout.path().join("shards/0001.sqlite"));

        let cancellation = CancellationToken::new();
        assert!(cancellation.cancel());
        let error = layout.sync_layout(2, &cancellation).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        drop(layout);
        assert!(!stage.exists());
        assert!(!destination.exists());
    }

    #[test]
    fn publish_is_no_replace_when_a_destination_appears() {
        let temp = TempDir::new().unwrap();
        let source = source_file(temp.path());
        let destination = temp.path().join("database");
        let mut layout = StagingLayout::create(&source, &destination).unwrap();
        let stage = layout.path().to_owned();
        complete_layout(&mut layout, 2);

        fs::create_dir(&destination).unwrap();
        fs::write(destination.join("winner"), b"untouched").unwrap();
        let error = layout.publish(&CancellationToken::new()).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(fs::read(destination.join("winner")).unwrap(), b"untouched");
        assert!(!stage.exists());
    }

    #[test]
    fn a_replaced_stage_is_neither_published_nor_recursively_cleaned() {
        let temp = TempDir::new().unwrap();
        let source = source_file(temp.path());
        let destination = temp.path().join("database");
        let mut layout = StagingLayout::create(&source, &destination).unwrap();
        complete_layout(&mut layout, 2);

        let stage = layout.path().to_owned();
        let original = temp.path().join("original-stage");
        fs::rename(&stage, &original).unwrap();
        fs::create_dir(&stage).unwrap();
        fs::write(stage.join("replacement"), b"must remain").unwrap();

        let error = layout.publish(&CancellationToken::new()).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(fs::read(stage.join("replacement")).unwrap(), b"must remain");
        assert!(original.join(RECEIPT_FILE_NAME).is_file());
        assert!(!destination.exists());
    }

    #[test]
    fn complete_layout_is_published_and_reopenable() {
        let temp = TempDir::new().unwrap();
        let source = source_file(temp.path());
        let destination = temp.path().join("database");
        let mut layout = StagingLayout::create(&source, &destination).unwrap();
        complete_layout(&mut layout, 2);
        layout.publish(&CancellationToken::new()).unwrap();

        assert!(destination.join(RECEIPT_FILE_NAME).is_file());
        for path in [
            destination.join("manifest.sqlite"),
            destination.join("shards/0000.sqlite"),
            destination.join("shards/0001.sqlite"),
        ] {
            let connection = Connection::open(path).unwrap();
            let check = connection
                .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
                .unwrap();
            assert_eq!(check, "ok");
        }
        assert_eq!(fs::read(source).unwrap(), b"source remains untouched");
    }

    #[test]
    fn sync_requires_receipt_known_shards_and_wal_mode() {
        let temp = TempDir::new().unwrap();
        let source = source_file(temp.path());
        let destination = temp.path().join("database");
        let mut layout = StagingLayout::create(&source, &destination).unwrap();
        create_wal_database(&layout.path().join("manifest.sqlite"));
        fs::create_dir(layout.path().join("shards")).unwrap();
        create_wal_database(&layout.path().join("shards/0000.sqlite"));
        create_wal_database(&layout.path().join("shards/0001.sqlite"));
        let missing_receipt = layout
            .sync_layout(2, &CancellationToken::new())
            .unwrap_err();
        assert!(matches!(
            missing_receipt.kind(),
            EngineErrorKind::StorageUnavailable | EngineErrorKind::FailedPrecondition
        ));

        fs::write(layout.path().join(RECEIPT_FILE_NAME), b"{}").unwrap();
        fs::remove_file(layout.path().join("shards/0001.sqlite")).unwrap();
        let missing_shard = layout
            .sync_layout(2, &CancellationToken::new())
            .unwrap_err();
        assert!(matches!(
            missing_shard.kind(),
            EngineErrorKind::StorageUnavailable | EngineErrorKind::FailedPrecondition
        ));

        let non_wal = Connection::open(layout.path().join("shards/0001.sqlite")).unwrap();
        non_wal
            .execute("CREATE TABLE marker(id INTEGER PRIMARY KEY)", [])
            .unwrap();
        non_wal.close().unwrap();
        let wrong_journal = layout
            .sync_layout(2, &CancellationToken::new())
            .unwrap_err();
        assert_eq!(wrong_journal.kind(), EngineErrorKind::FailedPrecondition);
        assert!(wrong_journal.diagnostic().contains("not in WAL mode"));
    }

    #[test]
    fn atomic_no_replace_has_exactly_one_race_winner() {
        const RACERS: usize = 12;

        let temp = TempDir::new().unwrap();
        let destination = Arc::new(temp.path().join("published"));
        let barrier = Arc::new(Barrier::new(RACERS));
        let mut racers = Vec::new();
        for index in 0..RACERS {
            let source = temp.path().join(format!("candidate-{index}"));
            fs::create_dir(&source).unwrap();
            fs::write(source.join("identity"), index.to_string()).unwrap();
            let destination = Arc::clone(&destination);
            let barrier = Arc::clone(&barrier);
            racers.push(thread::spawn(move || {
                barrier.wait();
                atomic_rename_noreplace(&source, &destination)
            }));
        }
        let results = racers
            .into_iter()
            .map(|racer| racer.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert!(destination.join("identity").is_file());
    }

    #[test]
    fn target_lock_is_exclusive_across_processes() {
        let temp = TempDir::new().unwrap();
        let source = source_file(temp.path());
        let destination = temp.path().join("database");
        let ready = temp.path().join("ready");
        let release = temp.path().join("release");
        let child = Command::new(env::current_exe().unwrap())
            .arg("--exact")
            .arg("import::staging::tests::subprocess_lock_holder")
            .arg("--nocapture")
            .env(CHILD_SOURCE_ENV, &source)
            .env(CHILD_DESTINATION_ENV, &destination)
            .env(CHILD_READY_ENV, &ready)
            .env(CHILD_RELEASE_ENV, &release)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "child did not acquire its target lock");
        let busy = StagingLayout::create(&source, &destination).unwrap_err();
        assert_eq!(busy.kind(), EngineErrorKind::Busy);
        fs::write(&release, b"release").unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "child failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        drop(StagingLayout::create(&source, &destination).unwrap());
    }

    #[test]
    fn subprocess_lock_holder() {
        let Ok(source) = env::var(CHILD_SOURCE_ENV) else {
            return;
        };
        let destination = PathBuf::from(env::var(CHILD_DESTINATION_ENV).unwrap());
        let ready = PathBuf::from(env::var(CHILD_READY_ENV).unwrap());
        let release = PathBuf::from(env::var(CHILD_RELEASE_ENV).unwrap());
        let _layout = StagingLayout::create(Path::new(&source), &destination).unwrap();
        fs::write(ready, b"ready").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !release.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(release.exists(), "parent did not release child lock holder");
    }
}
