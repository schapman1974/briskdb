//! Opt-in durable catalog storage; not an active engine or listener policy.
//!
//! The host must retain the store ID in its trusted root configuration and must
//! integrate durable publication with runtime admission. This separate store
//! does not modify `manifest.sqlite`, enable authentication, or prevent an older
//! engine from opening a database root. Do not expose it as a secure listener yet.
//!
//! Paths require an existing owner-only directory and owner-only regular file.
//! Unix ownership/mode checks are supported; other platforms fail closed until
//! equivalent ACL validation is implemented. No existing permissions are changed.

use std::{
    fmt,
    fs::File,
    path::{Path, PathBuf},
    time::Duration,
};

#[cfg(unix)]
use std::fs;

use rusqlite::{Connection, OpenFlags, Transaction, TransactionBehavior};

use crate::core::{
    EngineError, EngineErrorKind, EngineResult,
    security_catalog::{MAX_SECURITY_CATALOG_RECORD_BYTES, SecurityCatalog},
};

const APPLICATION_ID: i64 = 0x4253_4341; // BSCA
const FORMAT_VERSION: i64 = 1;
const TABLE_SQL: &str = "CREATE TABLE briskdb_security_state (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    store_id BLOB NOT NULL CHECK (length(store_id) = 16),
    revision INTEGER NOT NULL CHECK (revision > 0),
    record BLOB NOT NULL CHECK (length(record) BETWEEN 52 AND 134217728)
) STRICT";

/// Trusted root binding, not a credential. Hosts must not recycle IDs for unrelated
/// stores. A copied/restored file needs an explicit host-side recovery decision.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SecurityStoreId([u8; 16]);

impl SecurityStoreId {
    pub fn generate() -> EngineResult<Self> {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes).map_err(|_| {
            failure(
                EngineErrorKind::Internal,
                "security store ID generation failed",
            )
        })?;
        Self::from_bytes(bytes)
    }

    pub fn from_bytes(bytes: [u8; 16]) -> EngineResult<Self> {
        if bytes == [0; 16] {
            return Err(failure(
                EngineErrorKind::InvalidArgument,
                "security store ID is invalid",
            ));
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for SecurityStoreId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecurityStoreId { .. }")
    }
}

/// One fully validated snapshot. Loading creates a fresh runtime incarnation;
/// this object is not a live role cache or an engine-session authorization layer.
#[derive(Debug)]
pub struct StoredSecurityCatalog {
    revision: u64,
    catalog: SecurityCatalog,
}

impl StoredSecurityCatalog {
    pub const fn revision(&self) -> u64 {
        self.revision
    }
    pub fn into_parts(self) -> (u64, SecurityCatalog) {
        (self.revision, self.catalog)
    }
}

/// Dedicated, revision-checked SQLite store. It has no plaintext-password API.
/// All operations require exclusive Rust access; SQLite serializes peer writers.
/// An ambiguous write/commit failure fences this handle until explicitly reopened.
///
/// ```no_run
/// use briskdb::core::security_catalog::SecurityCatalog;
/// use briskdb::storage::security_catalog::{SecurityCatalogStore, SecurityStoreId};
/// let id = SecurityStoreId::generate()?; // Retain in trusted host configuration.
/// let catalog = SecurityCatalog::new();
/// let mut store = SecurityCatalogStore::create("/existing/private/directory/security.sqlite", id, &catalog)?;
/// let (revision, catalog) = store.load()?.into_parts();
/// let next = store.replace(revision, &catalog)?;
/// assert_eq!(next, revision + 1);
/// # Ok::<_, briskdb::EngineError>(())
/// ```
pub struct SecurityCatalogStore {
    connection: Connection,
    path: PathBuf,
    identity_file: File,
    id: SecurityStoreId,
    observed_revision: u64,
    fenced: bool,
}

impl fmt::Debug for SecurityCatalogStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecurityCatalogStore")
            .field("observed_revision", &self.observed_revision)
            .field("fenced", &self.fenced)
            .finish_non_exhaustive()
    }
}

impl SecurityCatalogStore {
    /// Create a new file only. The caller retains `id` even if initialization has
    /// an uncertain outcome, and can explicitly reopen to inspect that outcome.
    /// Never truncates, adopts or repairs an existing file.
    pub fn create(
        path: impl AsRef<Path>,
        id: SecurityStoreId,
        catalog: &SecurityCatalog,
    ) -> EngineResult<Self> {
        let record = catalog.to_record()?;
        let path = private_path(path.as_ref())?;
        let identity_file = private_file(&path, true)?;
        let mut connection = open_connection(&path)?;
        check_file_identity(&connection, &path, &identity_file)?;
        configure(&connection)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        transaction
            .execute_batch(TABLE_SQL)
            .map_err(storage_error)?;
        transaction
            .pragma_update(None, "application_id", APPLICATION_ID)
            .map_err(storage_error)?;
        transaction
            .pragma_update(None, "user_version", FORMAT_VERSION)
            .map_err(storage_error)?;
        transaction
            .execute(
                "INSERT INTO briskdb_security_state VALUES (1, ?1, 1, ?2)",
                rusqlite::params![id.as_bytes().as_slice(), record.as_bytes()],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        check_file_identity(&connection, &path, &identity_file)?;
        identity_file.sync_all().map_err(|_| io_failure())?;
        File::open(path.parent().expect("canonical parent"))
            .and_then(|directory| directory.sync_all())
            .map_err(|_| io_failure())?;
        Ok(Self {
            connection,
            path,
            identity_file,
            id,
            observed_revision: 1,
            fenced: false,
        })
    }

    /// Open only an existing, structurally valid store matching a trusted ID.
    /// Wrong IDs/versions, incomplete creation and unrelated SQLite files fail
    /// closed; this does not silently initialize or upgrade anything.
    pub fn open(path: impl AsRef<Path>, expected_id: SecurityStoreId) -> EngineResult<Self> {
        let path = private_path(path.as_ref())?;
        let identity_file = private_file(&path, false)?;
        let connection = open_connection(&path)?;
        check_file_identity(&connection, &path, &identity_file)?;
        configure(&connection)?;
        let mut store = Self {
            connection,
            path,
            identity_file,
            id: expected_id,
            observed_revision: 0,
            fenced: false,
        };
        store.load()?;
        Ok(store)
    }

    pub const fn id(&self) -> SecurityStoreId {
        self.id
    }

    pub fn load(&mut self) -> EngineResult<StoredSecurityCatalog> {
        self.check()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(storage_error)?;
        let snapshot = read_snapshot(&transaction, self.id, self.observed_revision)?;
        check_file_identity(&transaction, &self.path, &self.identity_file)?;
        transaction.commit().map_err(storage_error)?;
        self.observed_revision = snapshot.revision;
        Ok(snapshot)
    }

    /// Compare-and-swap a whole validated catalog. A stale revision never writes.
    /// Return the new revision only after commit. This publishes bytes, not live
    /// session authority; the host must couple that boundary to runtime admission.
    /// Edit a loaded catalog: replacements cannot reset the user-ID allocator,
    /// reuse identities, rewind credential generations or change verifier bytes
    /// without a generation advance. Restore to an earlier history requires an
    /// explicit separate store/root recovery decision, not a normal replacement.
    pub fn replace(
        &mut self,
        expected_revision: u64,
        catalog: &SecurityCatalog,
    ) -> EngineResult<u64> {
        self.replace_with_hook(expected_revision, catalog, |_| Ok(()))
    }

    fn replace_with_hook(
        &mut self,
        expected_revision: u64,
        catalog: &SecurityCatalog,
        hook: impl FnOnce(&Transaction<'_>) -> EngineResult<()>,
    ) -> EngineResult<u64> {
        self.check()?;
        if expected_revision == 0 || expected_revision >= i64::MAX as u64 {
            return Err(failure(
                EngineErrorKind::LimitExceeded,
                "security store revision is invalid or exhausted",
            ));
        }
        let record = catalog.to_record()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let current = read_snapshot(&transaction, self.id, self.observed_revision)?;
        self.observed_revision = current.revision;
        if current.revision != expected_revision {
            return Err(failure(
                EngineErrorKind::FailedPrecondition,
                "security store revision conflict",
            ));
        }
        current.catalog.validate_successor(catalog)?;
        let next = expected_revision + 1;
        // From this point an I/O error may have an uncertain durable outcome.
        // Fence first; only a confirmed complete commit clears the fence.
        self.fenced = true;
        transaction
            .execute(
                "UPDATE briskdb_security_state SET revision = ?1, record = ?2 WHERE singleton = 1",
                rusqlite::params![next as i64, record.as_bytes()],
            )
            .map_err(storage_error)?;
        hook(&transaction)?;
        check_file_identity(&transaction, &self.path, &self.identity_file)?;
        transaction.commit().map_err(storage_error)?;
        check_file_identity(&self.connection, &self.path, &self.identity_file)?;
        self.observed_revision = next;
        self.fenced = false;
        Ok(next)
    }

    pub(crate) const fn is_fenced(&self) -> bool {
        self.fenced
    }

    pub(crate) const fn observed_revision(&self) -> u64 {
        self.observed_revision
    }

    fn check(&self) -> EngineResult<()> {
        if self.fenced {
            return Err(failure(
                EngineErrorKind::FailedPrecondition,
                "security store requires explicit reopen",
            ));
        }
        check_file_identity(&self.connection, &self.path, &self.identity_file)
    }
}

fn open_connection(path: &Path) -> EngineResult<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_EXRESCODE;
    Connection::open_with_flags(path, flags).map_err(storage_error)
}

fn configure(connection: &Connection) -> EngineResult<()> {
    // SQLite materializes row values before Rust can inspect length(record).
    // Cap that allocation too, allowing only small fixed row/schema overhead.
    connection
        .set_limit(
            rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH,
            (MAX_SECURITY_CATALOG_RECORD_BYTES + 4096) as i32,
        )
        .map_err(storage_error)?;
    connection
        .busy_timeout(Duration::from_secs(2))
        .map_err(storage_error)?;
    // Connection-only settings: do not change or repair an existing file's mode.
    connection.execute_batch("PRAGMA trusted_schema=OFF; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON; PRAGMA temp_store=MEMORY;").map_err(storage_error)
}

fn read_snapshot(
    connection: &Connection,
    expected_id: SecurityStoreId,
    observed: u64,
) -> EngineResult<StoredSecurityCatalog> {
    let application: i64 = connection
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .map_err(storage_error)?;
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(storage_error)?;
    let journal: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .map_err(storage_error)?;
    if application != APPLICATION_ID || version != FORMAT_VERSION || journal != "delete" {
        return Err(corrupt());
    }
    let mut schema = connection.prepare("SELECT type, name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 2").map_err(storage_error)?;
    let mut rows = schema.query([]).map_err(storage_error)?;
    let row = rows.next().map_err(storage_error)?.ok_or_else(corrupt)?;
    if row
        .get_ref(0)
        .map_err(storage_error)?
        .as_str()
        .map_err(|_| corrupt())?
        != "table"
        || row
            .get_ref(1)
            .map_err(storage_error)?
            .as_str()
            .map_err(|_| corrupt())?
            != "briskdb_security_state"
        || row
            .get_ref(2)
            .map_err(storage_error)?
            .as_str()
            .map_err(|_| corrupt())?
            != TABLE_SQL
        || rows.next().map_err(storage_error)?.is_some()
    {
        return Err(corrupt());
    }
    let mut statement = connection.prepare("SELECT singleton, store_id, revision, length(record), record FROM briskdb_security_state LIMIT 2").map_err(storage_error)?;
    let mut rows = statement.query([]).map_err(storage_error)?;
    let row = rows.next().map_err(storage_error)?.ok_or_else(corrupt)?;
    let singleton: i64 = row.get(0).map_err(|_| corrupt())?;
    let id = row
        .get_ref(1)
        .map_err(storage_error)?
        .as_blob()
        .map_err(|_| corrupt())?;
    let revision: i64 = row.get(2).map_err(|_| corrupt())?;
    let length: i64 = row.get(3).map_err(|_| corrupt())?;
    if singleton != 1
        || id != expected_id.as_bytes()
        || revision <= 0
        || (revision as u64) < observed
        || !(52..=MAX_SECURITY_CATALOG_RECORD_BYTES as i64).contains(&length)
    {
        return Err(corrupt());
    }
    let bytes = row
        .get_ref(4)
        .map_err(storage_error)?
        .as_blob()
        .map_err(|_| corrupt())?;
    let catalog = SecurityCatalog::from_record(bytes).map_err(|_| corrupt())?;
    if rows.next().map_err(storage_error)?.is_some() {
        return Err(corrupt());
    }
    Ok(StoredSecurityCatalog {
        revision: revision as u64,
        catalog,
    })
}

#[cfg(unix)]
pub(super) fn private_path(path: &Path) -> EngineResult<PathBuf> {
    use std::os::unix::fs::MetadataExt;
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let metadata = fs::symlink_metadata(parent).map_err(|_| io_failure())?;
    // SAFETY: geteuid has no preconditions and returns the process effective UID.
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        return Err(private_access_required());
    }
    let name = path.file_name().ok_or_else(private_access_required)?;
    Ok(fs::canonicalize(parent)
        .map_err(|_| io_failure())?
        .join(name))
}

#[cfg(unix)]
fn private_file(path: &Path, create: bool) -> EngineResult<File> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(create)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| io_failure())?;
    let metadata = file.metadata().map_err(|_| io_failure())?;
    // SAFETY: geteuid has no preconditions and returns the process effective UID.
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o177 != 0
        || metadata.mode() & 0o600 != 0o600
    {
        return Err(private_access_required());
    }
    Ok(file)
}

#[cfg(not(unix))]
pub(super) fn private_path(_: &Path) -> EngineResult<PathBuf> {
    Err(failure(
        EngineErrorKind::Unsupported,
        "security store requires platform access-control validation",
    ))
}

#[cfg(not(unix))]
fn private_file(_: &Path, _: bool) -> EngineResult<File> {
    Err(failure(
        EngineErrorKind::Unsupported,
        "security store requires platform access-control validation",
    ))
}

fn check_file_identity(connection: &Connection, path: &Path, expected: &File) -> EngineResult<()> {
    private_path(path)?;
    let current = private_file(path, false)?;
    let expected = same_file::Handle::from_file(expected.try_clone().map_err(|_| io_failure())?)
        .map_err(|_| io_failure())?;
    let current = same_file::Handle::from_file(current).map_err(|_| io_failure())?;
    if expected != current
        || !super::shard::pooled_file_is_current(connection, path).map_err(|_| corrupt())?
    {
        return Err(corrupt());
    }
    Ok(())
}

fn failure(kind: EngineErrorKind, message: &'static str) -> EngineError {
    EngineError::new(kind, message)
}
fn storage_error(error: rusqlite::Error) -> EngineError {
    failure(
        crate::sqlite_error::storage(error).kind(),
        "security catalog storage operation failed",
    )
}
fn io_failure() -> EngineError {
    failure(
        EngineErrorKind::StorageUnavailable,
        "security catalog file access failed",
    )
}
fn corrupt() -> EngineError {
    failure(
        EngineErrorKind::DataCorruption,
        "security catalog store validation failed",
    )
}
#[cfg(unix)]
fn private_access_required() -> EngineError {
    failure(
        EngineErrorKind::PermissionDenied,
        "security catalog requires private owned storage",
    )
}

#[cfg(all(test, unix))]
mod tests;
