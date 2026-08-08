//! Physical-shard identity, provisioning, and strict reopen validation.

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use rusqlite::{Connection, MAIN_DB, OpenFlags, TransactionBehavior, hooks::AuthAction};

use crate::{
    core::{EngineError, EngineErrorKind, EngineResult},
    sqlite_error,
};

use super::CONNECTION_BUSY_TIMEOUT;

/// `BRSH` encoded as SQLite's 32-bit application identifier.
pub(super) const SHARD_APPLICATION_ID: i64 = 0x4252_5348;
/// Version of the storage-owned shard metadata table.
pub(super) const SHARD_METADATA_VERSION: u32 = 1;

const SHARD_METADATA_TABLE: &str = "briskdb_shard_metadata";
const MAX_SHARDS: u16 = 64;
const MAX_DIRECTORY_ENTRIES: usize = 512;
const MAX_SCHEMA_SQL_BYTES: usize = 4_096;

const SHARD_METADATA_TABLE_SQL: &str = "CREATE TABLE briskdb_shard_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    layout_id BLOB NOT NULL
        CHECK (typeof(layout_id) = 'blob' AND length(layout_id) = 16),
    shard_id INTEGER NOT NULL CHECK (shard_id BETWEEN 0 AND 63)
) STRICT";

/// Durable state of physical-shard layout preparation in the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub(super) enum ShardLayoutState {
    Creating = 1,
    Adopting = 2,
    Ready = 3,
}

impl ShardLayoutState {
    pub(super) const fn code(self) -> i64 {
        self as i64
    }

    pub(super) fn from_code(code: i64) -> EngineResult<Self> {
        match code {
            1 => Ok(Self::Creating),
            2 => Ok(Self::Adopting),
            3 => Ok(Self::Ready),
            _ => Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("manifest has unsupported shard-layout state {code}"),
            )),
        }
    }
}

/// Validated physical-shard format expectations loaded from the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ShardLayout {
    layout_id: [u8; 16],
    expected_application_id: i64,
    metadata_version: u32,
    state: ShardLayoutState,
}

impl ShardLayout {
    pub(super) fn from_validated_parts(
        layout_id: [u8; 16],
        expected_application_id: i64,
        metadata_version: u32,
        state: ShardLayoutState,
    ) -> Self {
        debug_assert_eq!(expected_application_id, SHARD_APPLICATION_ID);
        debug_assert_eq!(metadata_version, SHARD_METADATA_VERSION);
        Self {
            layout_id,
            expected_application_id,
            metadata_version,
            state,
        }
    }

    pub(super) const fn layout_id(self) -> [u8; 16] {
        self.layout_id
    }

    pub(super) const fn expected_application_id(self) -> i64 {
        self.expected_application_id
    }

    pub(super) const fn metadata_version(self) -> u32 {
        self.metadata_version
    }

    pub(super) const fn state(self) -> ShardLayoutState {
        self.state
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreflightState {
    Missing,
    Empty,
    Legacy,
    Exact,
}

#[derive(Debug)]
struct PreflightShard {
    shard_id: u16,
    path: PathBuf,
    state: PreflightState,
}

/// Preflight every expected file before changing any shard, provision only the
/// eligible states, then perform one strict no-create validation pass.
pub(super) fn prepare_layout(
    shards_dir: &Path,
    shard_count: u16,
    schema_generation: u64,
    layout: &ShardLayout,
) -> EngineResult<()> {
    prepare_layout_with_hook(shards_dir, shard_count, schema_generation, layout, |_| {
        Ok(())
    })
}

pub(super) fn prepare_layout_with_hook<F>(
    shards_dir: &Path,
    shard_count: u16,
    schema_generation: u64,
    layout: &ShardLayout,
    mut hook: F,
) -> EngineResult<()>
where
    F: FnMut(u16) -> EngineResult<()>,
{
    validate_inputs(shard_count, schema_generation, layout)?;
    let preflight = preflight_all(shards_dir, shard_count, schema_generation, layout)?;

    if preflight
        .iter()
        .any(|shard| shard.state == PreflightState::Missing)
    {
        create_shards_directory(shards_dir, shard_count)?;
    }

    for shard in &preflight {
        match shard.state {
            PreflightState::Missing | PreflightState::Empty | PreflightState::Legacy => {
                provision_shard(
                    &shard.path,
                    shard.shard_id,
                    schema_generation,
                    layout,
                    |_| {},
                )?;
            }
            PreflightState::Exact => {}
        }
        hook(shard.shard_id)?;
    }

    // Re-scan after provisioning so a concurrent unexpected file or a partial
    // SQLite state cannot be certified by the manifest caller.
    validate_directory(shards_dir, shard_count, false)?;
    for shard_id in 0..shard_count {
        let path = shard_path(shards_dir, shard_id);
        drop(open_existing(&path, shard_id, schema_generation, layout)?);
    }
    Ok(())
}

/// Open a required shard without create or symlink traversal and return it only
/// after its persistent identity, generation, metadata, and WAL mode validate.
pub(super) fn open_existing(
    path: &Path,
    shard_id: u16,
    schema_generation: u64,
    layout: &ShardLayout,
) -> EngineResult<Connection> {
    let connection = open_required_file(path)?;
    configure_busy_timeout(&connection)?;
    validate_open_connection(&connection, path, shard_id, schema_generation, layout)?;
    Ok(connection)
}

/// Open the required path with strict filesystem and SQLite no-create/no-follow
/// semantics, but leave database validation to the caller. The controlled pool
/// path uses this split to install cancellation hooks before validation can
/// wait on SQLite locks.
pub(super) fn open_required_file(path: &Path) -> EngineResult<Connection> {
    validate_existing_file(path)?;
    open_existing_connection(path)
}

/// Validate and configure an already-open required shard connection without
/// replacing its busy handler or progress hook.
pub(super) fn validate_open_connection(
    connection: &Connection,
    path: &Path,
    shard_id: u16,
    schema_generation: u64,
    layout: &ShardLayout,
) -> EngineResult<()> {
    validate_shard_id(shard_id)?;
    let expected_user_version = expected_user_version(schema_generation)?;
    require_writable(connection)?;
    validate_exact_shard(connection, path, shard_id, expected_user_version, layout)?;
    configure_connection_pragmas(connection)
}

fn preflight_all(
    shards_dir: &Path,
    shard_count: u16,
    schema_generation: u64,
    layout: &ShardLayout,
) -> EngineResult<Vec<PreflightShard>> {
    let directory_exists = validate_directory(
        shards_dir,
        shard_count,
        layout.state() == ShardLayoutState::Creating,
    )?;
    let expected_user_version = expected_user_version(schema_generation)?;
    let mut shards = Vec::with_capacity(usize::from(shard_count));

    for shard_id in 0..shard_count {
        let path = shard_path(shards_dir, shard_id);
        let state = if !directory_exists || !path_exists(&path)? {
            if layout.state() == ShardLayoutState::Creating {
                PreflightState::Missing
            } else {
                return Err(missing_shard(&path, shard_id));
            }
        } else {
            validate_existing_file(&path)?;
            let connection = open_existing_connection(&path)?;
            configure_connection_safety(&connection)?;
            classify_shard(&connection, &path, shard_id, expected_user_version, layout)?
        };
        shards.push(PreflightShard {
            shard_id,
            path,
            state,
        });
    }
    Ok(shards)
}

fn validate_inputs(
    shard_count: u16,
    schema_generation: u64,
    layout: &ShardLayout,
) -> EngineResult<()> {
    if !(2..=MAX_SHARDS).contains(&shard_count) {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "validated shard layout has an invalid shard count",
        ));
    }
    expected_user_version(schema_generation)?;
    if layout.expected_application_id() != SHARD_APPLICATION_ID
        || layout.metadata_version() != SHARD_METADATA_VERSION
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest shard-layout format is unsupported",
        ));
    }
    Ok(())
}

fn validate_shard_id(shard_id: u16) -> EngineResult<()> {
    if shard_id < MAX_SHARDS {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::Internal,
            format!("shard {shard_id} is outside the supported range"),
        ))
    }
}

fn expected_user_version(schema_generation: u64) -> EngineResult<i64> {
    i32::try_from(schema_generation)
        .map(i64::from)
        .map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::FailedPrecondition,
                "catalog schema generation does not fit SQLite user_version",
                error,
            )
        })
}

fn validate_directory(
    shards_dir: &Path,
    shard_count: u16,
    missing_allowed: bool,
) -> EngineResult<bool> {
    let metadata = match fs::symlink_metadata(shards_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && missing_allowed => {
            return Ok(false);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(EngineError::from_source(
                EngineErrorKind::DataCorruption,
                format!(
                    "required shard directory {} is missing",
                    shards_dir.display()
                ),
                error,
            ));
        }
        Err(error) => {
            return Err(sqlite_error::storage_io(
                error,
                format!("failed to inspect {}", shards_dir.display()),
            ));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "shard path {} is not a real directory",
                shards_dir.display()
            ),
        ));
    }

    let expected = (0..shard_count).map(shard_filename).collect::<HashSet<_>>();
    let mut entries = fs::read_dir(shards_dir).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!("failed to enumerate {}", shards_dir.display()),
        )
    })?;
    for index in 0..=MAX_DIRECTORY_ENTRIES {
        let Some(entry) = entries.next() else {
            return Ok(true);
        };
        if index == MAX_DIRECTORY_ENTRIES {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!(
                    "shard directory {} exceeds its bounded entry limit",
                    shards_dir.display()
                ),
            ));
        }
        let entry = entry.map_err(|error| {
            sqlite_error::storage_io(
                error,
                format!("failed to enumerate {}", shards_dir.display()),
            )
        })?;
        let name = entry.file_name().into_string().map_err(|_| {
            EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "shard directory contains a non-UTF-8 entry name",
            )
        })?;
        let file_type = entry.file_type().map_err(|error| {
            sqlite_error::storage_io(
                error,
                format!("failed to inspect {}", entry.path().display()),
            )
        })?;
        if file_type.is_symlink() {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "shard directory entry {} is a symbolic link",
                    entry.path().display()
                ),
            ));
        }
        if expected.contains(&name) {
            if !file_type.is_file() {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "required shard {} is not a regular file",
                        entry.path().display()
                    ),
                ));
            }
            continue;
        }
        if is_expected_sidecar(&name, &expected) {
            if !file_type.is_file() {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "SQLite sidecar {} is not a regular file",
                        entry.path().display()
                    ),
                ));
            }
            continue;
        }
        if is_canonical_shard_filename(&name) {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!("shard directory contains unexpected database file {name}"),
            ));
        }
    }
    unreachable!("bounded directory loop always returns")
}

fn is_expected_sidecar(name: &str, expected: &HashSet<String>) -> bool {
    ["-wal", "-shm", "-journal"].iter().any(|suffix| {
        name.strip_suffix(suffix)
            .is_some_and(|base| expected.contains(base))
    })
}

fn is_canonical_shard_filename(name: &str) -> bool {
    let Some(shard_id) = name.strip_suffix(".sqlite") else {
        return false;
    };
    shard_id.len() == 4 && shard_id.bytes().all(|byte| byte.is_ascii_digit())
}

fn path_exists(path: &Path) -> EngineResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(sqlite_error::storage_io(
            error,
            format!("failed to inspect {}", path.display()),
        )),
    }
}

fn validate_existing_file(path: &Path) -> EngineResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            EngineError::from_source(
                EngineErrorKind::DataCorruption,
                format!("required shard {} is missing", path.display()),
                error,
            )
        } else {
            sqlite_error::storage_io(error, format!("failed to inspect {}", path.display()))
        }
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("required shard {} is not a real file", path.display()),
        ));
    }
    Ok(())
}

fn create_shards_directory(shards_dir: &Path, shard_count: u16) -> EngineResult<()> {
    match fs::create_dir(shards_dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            validate_directory(shards_dir, shard_count, false).map(|_| ())
        }
        Err(error) => Err(sqlite_error::storage_io(
            error,
            format!("failed to create {}", shards_dir.display()),
        )),
    }
}

fn shard_filename(shard_id: u16) -> String {
    format!("{shard_id:04}.sqlite")
}

fn shard_path(shards_dir: &Path, shard_id: u16) -> PathBuf {
    shards_dir.join(shard_filename(shard_id))
}

fn open_existing_connection(path: &Path) -> EngineResult<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_EXRESCODE;
    let open_path = canonical_open_path(path)?;
    Connection::open_with_flags(open_path, flags).map_err(|error| {
        sqlite_error::storage(error).context(format!("failed to open shard {}", path.display()))
    })
}

fn open_creating_connection(path: &Path) -> EngineResult<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_EXRESCODE;
    let open_path = canonical_open_path(path)?;
    Connection::open_with_flags(open_path, flags).map_err(|error| {
        sqlite_error::storage(error).context(format!("failed to create shard {}", path.display()))
    })
}

// SQLite's NOFOLLOW flag rejects a path containing any symlink component. On
// macOS, tempfile paths commonly begin with `/var`, which is itself a system
// symlink. Resolve only the already-validated parent and retain the final shard
// component so NOFOLLOW still protects the database file from replacement.
fn canonical_open_path(path: &Path) -> EngineResult<PathBuf> {
    let parent = path.parent().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("shard path {} has no parent directory", path.display()),
        )
    })?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let parent_metadata = fs::symlink_metadata(parent).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!("failed to inspect shard directory {}", parent.display()),
        )
    })?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("shard path {} is not a real directory", parent.display()),
        ));
    }
    let file_name = path.file_name().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("shard path {} has no file name", path.display()),
        )
    })?;
    let canonical_parent = fs::canonicalize(parent).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!("failed to resolve shard directory {}", parent.display()),
        )
    })?;
    Ok(canonical_parent.join(file_name))
}

fn configure_connection_safety(connection: &Connection) -> EngineResult<()> {
    configure_busy_timeout(connection)?;
    require_writable(connection)
}

fn configure_busy_timeout(connection: &Connection) -> EngineResult<()> {
    connection
        .busy_timeout(CONNECTION_BUSY_TIMEOUT)
        .map_err(sqlite_error::storage)
}

fn require_writable(connection: &Connection) -> EngineResult<()> {
    if connection
        .is_readonly(MAIN_DB)
        .map_err(sqlite_error::storage)?
    {
        return Err(EngineError::new(
            EngineErrorKind::ReadOnly,
            "required shard opened read-only",
        ));
    }
    Ok(())
}

fn configure_connection_pragmas(connection: &Connection) -> EngineResult<()> {
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(sqlite_error::storage)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn classify_shard(
    connection: &Connection,
    path: &Path,
    shard_id: u16,
    expected_user_version: i64,
    layout: &ShardLayout,
) -> EngineResult<PreflightState> {
    let (application_id, user_version) = read_identity(connection)?;
    if application_id == layout.expected_application_id() {
        if user_version > expected_user_version {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "shard {shard_id} schema generation {user_version} is newer than the catalog generation {expected_user_version}"
                ),
            ));
        }
        if user_version != expected_user_version {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "shard {shard_id} schema generation {user_version} does not match catalog generation {expected_user_version}"
                ),
            ));
        }
        require_wal(connection, path)?;
        validate_metadata(connection, shard_id, layout.layout_id())?;
        return Ok(PreflightState::Exact);
    }

    if application_id != 0 || user_version != 0 {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "shard {shard_id} has foreign identity application_id={application_id:#010x}, user_version={user_version}"
            ),
        ));
    }
    if layout.state() == ShardLayoutState::Ready {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("ready shard {shard_id} is missing its BriskDB identity"),
        ));
    }
    if has_metadata_object(connection)? {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("shard {shard_id} has a conflicting {SHARD_METADATA_TABLE} object"),
        ));
    }

    match layout.state() {
        ShardLayoutState::Creating => {
            if has_application_schema_objects(connection)? {
                Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "new shard {} is not an exact empty SQLite database",
                        path.display()
                    ),
                ))
            } else {
                Ok(PreflightState::Empty)
            }
        }
        ShardLayoutState::Adopting => {
            require_wal(connection, path)?;
            Ok(PreflightState::Legacy)
        }
        ShardLayoutState::Ready => unreachable!("ready legacy state returned above"),
    }
}

fn provision_shard<F>(
    path: &Path,
    shard_id: u16,
    schema_generation: u64,
    layout: &ShardLayout,
    mut hook: F,
) -> EngineResult<()>
where
    F: FnMut(ProvisionPoint),
{
    let expected_user_version = expected_user_version(schema_generation)?;
    let mut connection = if path_exists(path)? {
        validate_existing_file(path)?;
        open_existing_connection(path)?
    } else if layout.state() == ShardLayoutState::Creating {
        open_creating_connection(path)?
    } else {
        return Err(missing_shard(path, shard_id));
    };
    configure_connection_safety(&connection)?;

    // Reclassify after opening so replacement between preflight and provisioning
    // cannot be overwritten as if it were the previously inspected file.
    let state = classify_shard(&connection, path, shard_id, expected_user_version, layout)?;
    if state == PreflightState::Exact {
        return Ok(());
    }
    configure_connection_pragmas(&connection)?;
    if state == PreflightState::Empty {
        enable_wal(&connection, path)?;
        hook(ProvisionPoint::WalPersisted);
    }

    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    // A second startup may have completed this shard while this connection
    // waited for the write lock. Reclassifying under that lock makes an exact
    // concurrent result idempotent and prevents a CREATE TABLE race.
    let locked_state = classify_shard(&transaction, path, shard_id, expected_user_version, layout)?;
    if locked_state == PreflightState::Exact {
        return Ok(());
    }
    transaction
        .execute_batch(SHARD_METADATA_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_shard_metadata (singleton, layout_id, shard_id)
             VALUES (1, ?1, ?2)",
            rusqlite::params![layout.layout_id().as_slice(), i64::from(shard_id)],
        )
        .map_err(sqlite_error::storage)?;
    hook(ProvisionPoint::MetadataWritten);
    transaction
        .pragma_update(None, "application_id", layout.expected_application_id())
        .map_err(sqlite_error::storage)?;
    transaction
        .pragma_update(None, "user_version", expected_user_version)
        .map_err(sqlite_error::storage)?;
    hook(ProvisionPoint::IdentityWritten);
    validate_exact_shard(&transaction, path, shard_id, expected_user_version, layout)?;
    transaction.commit().map_err(sqlite_error::storage)?;
    validate_exact_shard(&connection, path, shard_id, expected_user_version, layout)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProvisionPoint {
    WalPersisted,
    MetadataWritten,
    IdentityWritten,
}

fn validate_exact_shard(
    connection: &Connection,
    path: &Path,
    shard_id: u16,
    expected_user_version: i64,
    layout: &ShardLayout,
) -> EngineResult<()> {
    let (application_id, user_version) = read_identity(connection)?;
    if application_id != layout.expected_application_id() {
        return if application_id == 0 {
            Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("ready shard {shard_id} is missing its BriskDB application ID"),
            ))
        } else {
            Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "shard {shard_id} has foreign application identifier {application_id:#010x}"
                ),
            ))
        };
    }
    if user_version > expected_user_version {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("shard {shard_id} was written by a newer schema generation"),
        ));
    }
    if user_version != expected_user_version {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("shard {shard_id} schema generation does not match its catalog"),
        ));
    }
    require_wal(connection, path)?;
    validate_metadata(connection, shard_id, layout.layout_id())
}

fn read_identity(connection: &Connection) -> EngineResult<(i64, i64)> {
    let application_id = connection
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .map_err(|error| shard_read_error(error, "failed to read shard application ID"))?;
    let user_version = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|error| shard_read_error(error, "failed to read shard schema generation"))?;
    Ok((application_id, user_version))
}

fn journal_mode(connection: &Connection) -> EngineResult<String> {
    connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .map_err(|error| shard_read_error(error, "failed to read shard journal mode"))
}

fn require_wal(connection: &Connection, path: &Path) -> EngineResult<()> {
    let mode = journal_mode(connection)?;
    if mode.eq_ignore_ascii_case("wal") {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "shard {} uses journal mode {mode}, expected WAL",
                path.display()
            ),
        ))
    }
}

fn enable_wal(connection: &Connection, path: &Path) -> EngineResult<()> {
    let mode = connection
        .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))
        .map_err(sqlite_error::storage)?;
    if mode.eq_ignore_ascii_case("wal") {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "SQLite retained journal mode {mode} instead of enabling WAL for {}",
                path.display()
            ),
        ))
    }
}

fn has_application_schema_objects(connection: &Connection) -> EngineResult<bool> {
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_schema
                 WHERE name NOT LIKE 'sqlite_%'
                 LIMIT 1
             )",
            [],
            |row| row.get(0),
        )
        .map_err(|error| shard_read_error(error, "failed to inspect new shard schema"))
}

fn has_metadata_object(connection: &Connection) -> EngineResult<bool> {
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_schema
                 WHERE name = ?1 COLLATE NOCASE
                 LIMIT 1
             )",
            [SHARD_METADATA_TABLE],
            |row| row.get(0),
        )
        .map_err(|error| shard_read_error(error, "failed to inspect shard metadata objects"))
}

#[derive(Debug, PartialEq, Eq)]
struct TableColumn {
    id: i64,
    name: String,
    declared_type: String,
    not_null: bool,
    default_value: Option<String>,
    primary_key_position: i64,
    hidden: i64,
}

impl TableColumn {
    fn expected(
        id: i64,
        name: &str,
        declared_type: &str,
        not_null: bool,
        primary_key_position: i64,
    ) -> Self {
        Self {
            id,
            name: name.to_owned(),
            declared_type: declared_type.to_owned(),
            not_null,
            default_value: None,
            primary_key_position,
            hidden: 0,
        }
    }
}

fn validate_metadata(
    connection: &Connection,
    expected_shard_id: u16,
    expected_layout_id: [u8; 16],
) -> EngineResult<()> {
    let objects = connection
        .prepare(
            "SELECT type, name, sql
             FROM sqlite_schema
             WHERE name = 'briskdb_shard_metadata' COLLATE NOCASE
             LIMIT 2",
        )
        .and_then(|mut statement| {
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| shard_read_error(error, "failed to inspect shard metadata schema"))?;
    if objects.len() != 1
        || objects[0].0 != "table"
        || objects[0].1 != SHARD_METADATA_TABLE
        || objects[0].2.as_deref().is_none_or(|sql| {
            sql.len() > MAX_SCHEMA_SQL_BYTES
                || normalize_schema_sql(sql) != normalize_schema_sql(SHARD_METADATA_TABLE_SQL)
        })
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "shard metadata table has an incompatible schema",
        ));
    }

    let columns = connection
        .prepare(
            "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
             FROM pragma_table_xinfo('briskdb_shard_metadata')
             LIMIT 4",
        )
        .and_then(|mut statement| {
            statement
                .query_map([], |row| {
                    Ok(TableColumn {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        declared_type: row.get(2)?,
                        not_null: row.get::<_, i64>(3)? != 0,
                        default_value: row.get(4)?,
                        primary_key_position: row.get(5)?,
                        hidden: row.get(6)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| shard_read_error(error, "failed to inspect shard metadata columns"))?;
    let expected_columns = [
        TableColumn::expected(0, "singleton", "INTEGER", false, 1),
        TableColumn::expected(1, "layout_id", "BLOB", true, 0),
        TableColumn::expected(2, "shard_id", "INTEGER", true, 0),
    ];
    if columns != expected_columns {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "shard metadata table has incompatible columns",
        ));
    }
    let strict: Option<i64> = connection
        .query_row(
            "SELECT strict
             FROM pragma_table_list
             WHERE schema = 'main' AND name = 'briskdb_shard_metadata'",
            [],
            |row| row.get(0),
        )
        .map(Some)
        .or_else(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            error => Err(error),
        })
        .map_err(|error| shard_read_error(error, "failed to inspect shard metadata flags"))?;
    if strict != Some(1) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "shard metadata table is not STRICT",
        ));
    }

    let rows = connection
        .prepare(
            "SELECT singleton, layout_id, shard_id
             FROM briskdb_shard_metadata
             ORDER BY singleton
             LIMIT 3",
        )
        .and_then(|mut statement| {
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| shard_read_error(error, "failed to read shard metadata"))?;
    if rows.len() != 1 || rows[0].0 != 1 {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "shard metadata must contain exactly its singleton row",
        ));
    }
    if rows[0].1.as_slice() != expected_layout_id {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "shard belongs to a different BriskDB layout",
        ));
    }
    if rows[0].2 != i64::from(expected_shard_id) {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "shard metadata identifies physical shard {}, expected {expected_shard_id}",
                rows[0].2
            ),
        ));
    }
    Ok(())
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

fn missing_shard(path: &Path, shard_id: u16) -> EngineError {
    EngineError::new(
        EngineErrorKind::DataCorruption,
        format!("required shard {shard_id} is missing at {}", path.display()),
    )
}

fn shard_read_error(error: rusqlite::Error, diagnostic: &'static str) -> EngineError {
    let classified = sqlite_error::storage(error);
    if matches!(
        classified.kind(),
        EngineErrorKind::Busy
            | EngineErrorKind::Cancelled
            | EngineErrorKind::PermissionDenied
            | EngineErrorKind::ReadOnly
            | EngineErrorKind::StorageFull
            | EngineErrorKind::OutOfMemory
            | EngineErrorKind::StorageUnavailable
    ) {
        classified.context(diagnostic)
    } else {
        EngineError::from_source(EngineErrorKind::DataCorruption, diagnostic, classified)
    }
}

/// Return whether a client statement action would mutate storage-owned shard
/// identity, durability configuration, or the reserved metadata namespace.
pub(super) fn denies_client_action(action: AuthAction<'_>) -> bool {
    match action {
        AuthAction::Pragma {
            pragma_name,
            pragma_value: Some(_),
        } => matches_persistent_pragma(pragma_name),
        AuthAction::Insert { table_name }
        | AuthAction::Delete { table_name }
        | AuthAction::DropTable { table_name }
        | AuthAction::DropVtable {
            table_name,
            module_name: _,
        } => is_metadata_table(table_name),
        AuthAction::Update {
            table_name,
            column_name: _,
        }
        | AuthAction::Read {
            table_name,
            column_name: _,
        } => is_metadata_table(table_name),
        // SQLite's authorizer reports the source table for ALTER TABLE but
        // does not expose a RENAME TO destination. Deny the whole operation
        // so a client cannot move an application table into the reserved
        // namespace without the authorizer seeing the new name.
        AuthAction::AlterTable {
            database_name: _,
            table_name: _,
        } => true,
        AuthAction::CreateTable { table_name }
        | AuthAction::CreateTempTable { table_name }
        | AuthAction::CreateVtable {
            table_name,
            module_name: _,
        } => is_reserved_name(table_name),
        AuthAction::CreateIndex {
            index_name,
            table_name,
        }
        | AuthAction::CreateTempIndex {
            index_name,
            table_name,
        } => is_reserved_name(index_name) || is_metadata_table(table_name),
        AuthAction::CreateTrigger {
            trigger_name,
            table_name,
        }
        | AuthAction::CreateTempTrigger {
            trigger_name,
            table_name,
        } => is_reserved_name(trigger_name) || is_metadata_table(table_name),
        AuthAction::CreateView { view_name } | AuthAction::CreateTempView { view_name } => {
            is_reserved_name(view_name)
        }
        AuthAction::DropIndex {
            index_name,
            table_name,
        }
        | AuthAction::DropTempIndex {
            index_name,
            table_name,
        } => is_reserved_name(index_name) || is_metadata_table(table_name),
        AuthAction::DropTrigger {
            trigger_name,
            table_name,
        }
        | AuthAction::DropTempTrigger {
            trigger_name,
            table_name,
        } => is_reserved_name(trigger_name) || is_metadata_table(table_name),
        AuthAction::Reindex { index_name } => is_reserved_name(index_name),
        AuthAction::Analyze { table_name } => is_metadata_table(table_name),
        _ => false,
    }
}

fn matches_persistent_pragma(name: &str) -> bool {
    [
        "application_id",
        "user_version",
        "journal_mode",
        "writable_schema",
        "schema_version",
    ]
    .iter()
    .any(|protected| name.eq_ignore_ascii_case(protected))
}

fn is_metadata_table(name: &str) -> bool {
    name.eq_ignore_ascii_case(SHARD_METADATA_TABLE)
}

fn is_reserved_name(name: &str) -> bool {
    name.as_bytes()
        .get(.."briskdb_".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"briskdb_"))
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::Arc,
        thread,
    };

    use rusqlite::hooks::TransactionOperation;

    use super::*;

    const LAYOUT_ID: [u8; 16] = *b"brisk-layout-001";

    fn layout(state: ShardLayoutState) -> ShardLayout {
        ShardLayout::from_validated_parts(
            LAYOUT_ID,
            SHARD_APPLICATION_ID,
            SHARD_METADATA_VERSION,
            state,
        )
    }

    fn create_legacy(path: &Path, wal: bool, schema: &str) {
        let connection = Connection::open(path).unwrap();
        connection.execute_batch(schema).unwrap();
        if wal {
            enable_wal(&connection, path).unwrap();
        }
    }

    fn identity(path: &Path) -> (i64, i64) {
        let connection = Connection::open(path).unwrap();
        read_identity(&connection).unwrap()
    }

    fn has_metadata(path: &Path) -> bool {
        let connection = Connection::open(path).unwrap();
        has_metadata_object(&connection).unwrap()
    }

    #[test]
    fn layout_codes_parts_and_accessors_are_exact() {
        assert_eq!(ShardLayoutState::Creating.code(), 1);
        assert_eq!(ShardLayoutState::Adopting.code(), 2);
        assert_eq!(ShardLayoutState::Ready.code(), 3);
        for state in [
            ShardLayoutState::Creating,
            ShardLayoutState::Adopting,
            ShardLayoutState::Ready,
        ] {
            assert_eq!(ShardLayoutState::from_code(state.code()).unwrap(), state);
            let layout = layout(state);
            assert_eq!(layout.layout_id(), LAYOUT_ID);
            assert_eq!(layout.expected_application_id(), SHARD_APPLICATION_ID);
            assert_eq!(layout.metadata_version(), SHARD_METADATA_VERSION);
            assert_eq!(layout.state(), state);
        }
        assert_eq!(
            ShardLayoutState::from_code(4).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn creating_provisions_exact_wal_shards_and_ready_reopens_without_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        prepare_layout(&shards, 4, 0, &layout(ShardLayoutState::Creating)).unwrap();

        for shard_id in 0..4 {
            let path = shard_path(&shards, shard_id);
            let connection =
                open_existing(&path, shard_id, 0, &layout(ShardLayoutState::Ready)).unwrap();
            assert_eq!(
                journal_mode(&connection).unwrap().to_ascii_lowercase(),
                "wal"
            );
            assert_eq!(
                read_identity(&connection).unwrap(),
                (SHARD_APPLICATION_ID, 0)
            );
        }
    }

    #[test]
    fn unrelated_files_are_ignored_but_extra_canonical_shards_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        fs::write(shards.join("operator-notes.sqlite"), b"not a shard").unwrap();
        fs::write(shards.join("README"), b"layout notes").unwrap();
        prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap();

        fs::copy(shard_path(&shards, 0), shard_path(&shards, 2)).unwrap();
        let error = prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Ready)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
    }

    #[test]
    fn preflight_rejects_a_late_foreign_shard_before_stamping_any_eligible_shard() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        let first = shard_path(&shards, 0);
        let second = shard_path(&shards, 1);
        create_legacy(&first, true, "CREATE TABLE user_data (id INTEGER);");
        create_legacy(&second, true, "CREATE TABLE user_data (id INTEGER);");
        let foreign = Connection::open(&second).unwrap();
        foreign
            .pragma_update(None, "application_id", 0x1234)
            .unwrap();
        drop(foreign);

        let error = prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Adopting)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&first), (0, 0));
        assert!(!has_metadata(&first));
    }

    #[test]
    fn adopting_preserves_legacy_schema_and_is_idempotent_after_partial_work() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        for shard_id in 0..2 {
            create_legacy(
                &shard_path(&shards, shard_id),
                true,
                "CREATE TABLE widgets (id INTEGER PRIMARY KEY, value TEXT);",
            );
        }
        let adopting = layout(ShardLayoutState::Adopting);
        provision_shard(&shard_path(&shards, 0), 0, 0, &adopting, |_| {}).unwrap();
        prepare_layout(&shards, 2, 0, &adopting).unwrap();

        for shard_id in 0..2 {
            let connection = open_existing(
                &shard_path(&shards, shard_id),
                shard_id,
                0,
                &layout(ShardLayoutState::Ready),
            )
            .unwrap();
            assert_eq!(
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'widgets'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                1
            );
        }
    }

    #[test]
    fn adopting_and_ready_never_create_a_missing_shard() {
        for state in [ShardLayoutState::Adopting, ShardLayoutState::Ready] {
            let temp = tempfile::tempdir().unwrap();
            let shards = temp.path().join("shards");
            fs::create_dir(&shards).unwrap();
            let missing = shard_path(&shards, 1);
            create_legacy(&shard_path(&shards, 0), true, "");

            let error = prepare_layout(&shards, 2, 0, &layout(state)).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
            assert!(!missing.exists());
        }
    }

    #[test]
    fn only_creating_may_enable_wal() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        for shard_id in 0..2 {
            create_legacy(&shard_path(&shards, shard_id), false, "");
        }
        let error = prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Adopting)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(
            journal_mode(&Connection::open(shard_path(&shards, 0)).unwrap())
                .unwrap()
                .to_ascii_lowercase(),
            "delete"
        );

        prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap();
        assert_eq!(
            journal_mode(&Connection::open(shard_path(&shards, 0)).unwrap())
                .unwrap()
                .to_ascii_lowercase(),
            "wal"
        );
    }

    #[test]
    fn ready_validation_never_repairs_a_changed_journal_mode() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap();
        let path = shard_path(&shards, 0);
        let connection = Connection::open(&path).unwrap();
        let mode = connection
            .pragma_update_and_check(None, "journal_mode", "DELETE", |row| {
                row.get::<_, String>(0)
            })
            .unwrap();
        assert_eq!(mode.to_ascii_lowercase(), "delete");
        drop(connection);

        let error = open_existing(&path, 0, 0, &layout(ShardLayoutState::Ready)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(
            journal_mode(&Connection::open(path).unwrap())
                .unwrap()
                .to_ascii_lowercase(),
            "delete"
        );
    }

    #[test]
    fn creating_rejects_a_nonempty_unmarked_database() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        create_legacy(
            &shard_path(&shards, 0),
            false,
            "CREATE TABLE foreign_data (id INTEGER);",
        );
        create_legacy(&shard_path(&shards, 1), false, "");

        let error = prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&shard_path(&shards, 1)), (0, 0));
    }

    #[test]
    fn exact_validation_rejects_foreign_future_layout_and_shard_identity() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap();
        let ready = layout(ShardLayoutState::Ready);

        let path = shard_path(&shards, 0);
        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "application_id", 0x1234)
            .unwrap();
        drop(connection);
        assert_eq!(
            open_existing(&path, 0, 0, &ready).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(identity(&path), (0x1234, 0));

        let connection = Connection::open(&path).unwrap();
        connection.pragma_update(None, "application_id", 0).unwrap();
        drop(connection);
        assert_eq!(
            open_existing(&path, 0, 0, &ready).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
        assert_eq!(identity(&path), (0, 0));

        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "application_id", SHARD_APPLICATION_ID)
            .unwrap();
        drop(connection);
        let connection = Connection::open(&path).unwrap();
        connection.pragma_update(None, "user_version", 1).unwrap();
        drop(connection);
        assert_eq!(
            open_existing(&path, 0, 0, &ready).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );

        let connection = Connection::open(&path).unwrap();
        connection.pragma_update(None, "user_version", 0).unwrap();
        connection
            .execute("UPDATE briskdb_shard_metadata SET shard_id = 1", [])
            .unwrap();
        drop(connection);
        assert_eq!(
            open_existing(&path, 0, 0, &ready).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );

        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "UPDATE briskdb_shard_metadata SET shard_id = 0, layout_id = zeroblob(16)",
                [],
            )
            .unwrap();
        drop(connection);
        assert_eq!(
            open_existing(&path, 0, 0, &ready).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn altered_metadata_schema_and_conflicting_legacy_object_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        for shard_id in 0..2 {
            create_legacy(&shard_path(&shards, shard_id), true, "");
        }
        Connection::open(shard_path(&shards, 0))
            .unwrap()
            .execute_batch("CREATE TABLE briskdb_shard_metadata (shard_id INTEGER);")
            .unwrap();
        let error = prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Adopting)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);

        let other = tempfile::tempdir().unwrap();
        let other_shards = other.path().join("shards");
        prepare_layout(&other_shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap();
        let path = shard_path(&other_shards, 0);
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(
            "DROP TABLE briskdb_shard_metadata;
             CREATE TABLE briskdb_shard_metadata (
                 singleton INTEGER PRIMARY KEY,
                 layout_id BLOB NOT NULL,
                 shard_id INTEGER NOT NULL
             ) STRICT;
             INSERT INTO briskdb_shard_metadata VALUES (1, x'627269736b2d6c61796f75742d303031', 0);",
        ).unwrap();
        drop(connection);
        assert_eq!(
            open_existing(&path, 0, 0, &layout(ShardLayoutState::Ready))
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_shards_and_nonfiles_are_rejected_without_following() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        let target = temp.path().join("target.sqlite");
        create_legacy(&target, true, "");
        symlink(&target, shard_path(&shards, 0)).unwrap();
        fs::create_dir(shard_path(&shards, 1)).unwrap();

        let error = prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Adopting)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(identity(&target), (0, 0));
    }

    #[cfg(unix)]
    #[test]
    fn runtime_open_rejects_a_symlinked_shard_directory() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let real_shards = temp.path().join("real-shards");
        prepare_layout(&real_shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap();
        let linked_shards = temp.path().join("linked-shards");
        symlink(&real_shards, &linked_shards).unwrap();

        let error = open_existing(
            &shard_path(&linked_shards, 0),
            0,
            0,
            &layout(ShardLayoutState::Ready),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
    }

    #[test]
    fn corrupt_required_shard_is_reported_without_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        fs::create_dir(&shards).unwrap();
        let path = shard_path(&shards, 0);
        fs::write(&path, b"not a sqlite database").unwrap();

        let error = open_existing(&path, 0, 0, &layout(ShardLayoutState::Ready)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(fs::read(path).unwrap(), b"not a sqlite database");
    }

    #[test]
    fn provisioning_panics_roll_back_and_retry_in_creating_and_adopting() {
        for state in [ShardLayoutState::Creating, ShardLayoutState::Adopting] {
            let points: &[ProvisionPoint] = if state == ShardLayoutState::Creating {
                &[
                    ProvisionPoint::WalPersisted,
                    ProvisionPoint::MetadataWritten,
                    ProvisionPoint::IdentityWritten,
                ]
            } else {
                &[
                    ProvisionPoint::MetadataWritten,
                    ProvisionPoint::IdentityWritten,
                ]
            };
            for &point in points {
                let temp = tempfile::tempdir().unwrap();
                let shards = temp.path().join("shards");
                fs::create_dir(&shards).unwrap();
                let path = shard_path(&shards, 0);
                create_legacy(&path, state == ShardLayoutState::Adopting, "");

                let panic = catch_unwind(AssertUnwindSafe(|| {
                    let _ = provision_shard(&path, 0, 0, &layout(state), |seen| {
                        if seen == point {
                            panic!("injected shard provisioning panic");
                        }
                    });
                }));
                assert!(panic.is_err());
                assert_eq!(identity(&path), (0, 0));
                assert!(!has_metadata(&path));
                assert_eq!(
                    journal_mode(&Connection::open(&path).unwrap())
                        .unwrap()
                        .to_ascii_lowercase(),
                    "wal"
                );

                provision_shard(&path, 0, 0, &layout(state), |_| {}).unwrap();
                open_existing(&path, 0, 0, &layout(ShardLayoutState::Ready)).unwrap();
            }
        }
    }

    #[test]
    fn strict_existing_opens_are_deterministic_in_parallel() {
        let temp = tempfile::tempdir().unwrap();
        let shards = temp.path().join("shards");
        prepare_layout(&shards, 2, 0, &layout(ShardLayoutState::Creating)).unwrap();
        let path = Arc::new(shard_path(&shards, 0));
        let workers = (0..8)
            .map(|_| {
                let path = Arc::clone(&path);
                thread::spawn(move || {
                    for _ in 0..100 {
                        let connection =
                            open_existing(path.as_ref(), 0, 0, &layout(ShardLayoutState::Ready))
                                .unwrap();
                        assert_eq!(
                            read_identity(&connection).unwrap(),
                            (SHARD_APPLICATION_ID, 0)
                        );
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
    }

    #[test]
    fn protected_client_actions_are_exact_and_application_reads_remain_allowed() {
        for pragma in [
            "application_id",
            "USER_VERSION",
            "journal_mode",
            "writable_schema",
            "schema_version",
        ] {
            assert!(denies_client_action(AuthAction::Pragma {
                pragma_name: pragma,
                pragma_value: Some("1"),
            }));
            assert!(!denies_client_action(AuthAction::Pragma {
                pragma_name: pragma,
                pragma_value: None,
            }));
        }
        assert!(denies_client_action(AuthAction::Insert {
            table_name: "BRISKDB_SHARD_METADATA",
        }));
        assert!(denies_client_action(AuthAction::Update {
            table_name: SHARD_METADATA_TABLE,
            column_name: "shard_id",
        }));
        assert!(denies_client_action(AuthAction::DropTable {
            table_name: SHARD_METADATA_TABLE,
        }));
        assert!(denies_client_action(AuthAction::CreateTable {
            table_name: "briskdb_future",
        }));
        assert!(denies_client_action(AuthAction::AlterTable {
            database_name: "main",
            table_name: "widgets",
        }));
        assert!(denies_client_action(AuthAction::Read {
            table_name: SHARD_METADATA_TABLE,
            column_name: "shard_id",
        }));
        assert!(!denies_client_action(AuthAction::Read {
            table_name: "widgets",
            column_name: "value",
        }));
        assert!(!denies_client_action(AuthAction::Transaction {
            operation: TransactionOperation::Begin,
        }));
    }
}
