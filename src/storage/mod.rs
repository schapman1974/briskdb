//! SQLite file layout, versioned manifest management, and connection configuration.

mod manifest;

pub(crate) mod pool;
pub(crate) use pool::{ConnectionOwner, ConnectionPools, PooledConnection};

use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use rusqlite::{
    Connection,
    hooks::{AuthAction, AuthContext, Authorization},
};

pub use crate::core::Database;
use crate::{
    core::{Catalog, CatalogSnapshot, EngineError, EngineErrorKind, EngineResult},
    sqlite_error,
};

pub(crate) const CONNECTION_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Clone)]
pub(crate) struct Storage {
    root: PathBuf,
    catalog: Arc<CatalogSnapshot>,
}

impl Storage {
    pub(crate) fn open(root: impl AsRef<Path>, requested_shards: u16) -> EngineResult<Self> {
        validate_shard_count(requested_shards)?;

        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|error| {
            sqlite_error::storage_io(error, format!("failed to create {}", root.display()))
        })?;
        let shards_dir = root.join("shards");
        let manifest_path = root.join("manifest.sqlite");
        let mut manifest = Connection::open(&manifest_path).map_err(|error| {
            sqlite_error::storage(error)
                .context(format!("failed to open {}", manifest_path.display()))
        })?;
        configure_manifest_connection(&manifest)?;
        let catalog = Arc::new(manifest::load_or_create_catalog(
            &mut manifest,
            requested_shards,
        )?);
        configure_journal_mode(&manifest)?;

        fs::create_dir_all(&shards_dir).map_err(|error| {
            sqlite_error::storage_io(error, format!("failed to create {}", shards_dir.display()))
        })?;

        let storage = Self { root, catalog };
        for shard in 0..storage.shard_count() {
            storage.open_shard(shard)?;
        }
        Ok(storage)
    }

    pub(crate) fn shard_count(&self) -> u16 {
        self.catalog.routing().shard_count()
    }

    pub(crate) fn shard_for_key(&self, key: &[u8]) -> u16 {
        self.catalog.routing().shard_for_key(key)
    }

    pub(crate) fn logical_catalog(&self) -> &Catalog {
        self.catalog.logical()
    }

    fn shard_path(&self, shard: u16) -> PathBuf {
        self.root.join("shards").join(format!("{shard:04}.sqlite"))
    }

    pub(crate) fn open_shard(&self, shard: u16) -> EngineResult<Connection> {
        let connection = self.open_unconfigured_shard(shard)?;
        configure_connection(&connection)?;
        Ok(connection)
    }

    fn open_unconfigured_shard(&self, shard: u16) -> EngineResult<Connection> {
        if shard >= self.shard_count() {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                format!("shard {shard} is outside the configured range"),
            ));
        }
        let path = self.shard_path(shard);
        let connection = Connection::open(&path).map_err(|error| {
            sqlite_error::storage(error).context(format!("failed to open shard {}", path.display()))
        })?;
        Ok(connection)
    }

    pub(crate) fn open_pooled_shard(
        &self,
        shard: u16,
    ) -> EngineResult<(Connection, ConnectionHygiene)> {
        let connection = self.open_shard(shard)?;
        self.attach_pool_hygiene(connection)
    }

    fn attach_pool_hygiene(
        &self,
        connection: Connection,
    ) -> EngineResult<(Connection, ConnectionHygiene)> {
        let tainted = Arc::new(AtomicBool::new(false));
        let wrote = Arc::new(AtomicBool::new(false));
        let probing = Arc::new(AtomicBool::new(false));
        let probe_tainted = Arc::new(AtomicBool::new(false));
        let probe_wrote = Arc::new(AtomicBool::new(false));
        let authorizer_taint = Arc::clone(&tainted);
        let authorizer_wrote = Arc::clone(&wrote);
        let authorizer_probing = Arc::clone(&probing);
        let authorizer_probe_tainted = Arc::clone(&probe_tainted);
        let authorizer_probe_wrote = Arc::clone(&probe_wrote);
        connection
            .authorizer(Some(move |context: AuthContext<'_>| {
                if action_taints_connection(context.action) {
                    if authorizer_probing.load(Ordering::Relaxed) {
                        authorizer_probe_tainted.store(true, Ordering::Relaxed);
                        return Authorization::Deny;
                    }
                    authorizer_taint.store(true, Ordering::Relaxed);
                }
                if action_writes_connection(context.action) {
                    if authorizer_probing.load(Ordering::Relaxed) {
                        authorizer_probe_wrote.store(true, Ordering::Relaxed);
                        return Authorization::Deny;
                    }
                    authorizer_wrote.store(true, Ordering::Relaxed);
                }
                Authorization::Allow
            }))
            .map_err(sqlite_error::storage)?;
        Ok((
            connection,
            ConnectionHygiene {
                tainted,
                wrote,
                probing,
                probe_tainted,
                probe_wrote,
            },
        ))
    }
}

#[derive(Debug)]
pub(crate) struct ConnectionHygiene {
    pub(crate) tainted: Arc<AtomicBool>,
    pub(crate) wrote: Arc<AtomicBool>,
    probing: Arc<AtomicBool>,
    probe_tainted: Arc<AtomicBool>,
    probe_wrote: Arc<AtomicBool>,
}

impl ConnectionHygiene {
    /// Enter a non-executing authorizer probe. Tainting and write actions are
    /// denied at prepare time so they cannot affect a foreign physical handle.
    pub(crate) fn begin_probe(&self) -> ConnectionProbe<'_> {
        self.probe_tainted.store(false, Ordering::Relaxed);
        self.probe_wrote.store(false, Ordering::Relaxed);
        let was_probing = self.probing.swap(true, Ordering::Relaxed);
        debug_assert!(!was_probing, "connection hygiene probes cannot be nested");
        ConnectionProbe { hygiene: self }
    }
}

/// Restores the normal authorizer mode even if statement preparation unwinds.
pub(crate) struct ConnectionProbe<'a> {
    hygiene: &'a ConnectionHygiene,
}

impl ConnectionProbe<'_> {
    pub(crate) fn requires_fresh_connection(&self) -> bool {
        self.hygiene.probe_tainted.load(Ordering::Relaxed)
            || self.hygiene.probe_wrote.load(Ordering::Relaxed)
    }
}

impl Drop for ConnectionProbe<'_> {
    fn drop(&mut self) {
        self.hygiene.probing.store(false, Ordering::Relaxed);
    }
}

pub(crate) fn validate_shard_count(requested_shards: u16) -> EngineResult<()> {
    if (2..=64).contains(&requested_shards) {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "shard count must be between 2 and 64",
        ))
    }
}

fn action_taints_connection(action: AuthAction<'_>) -> bool {
    matches!(
        action,
        AuthAction::Unknown { .. }
            | AuthAction::CreateTempIndex { .. }
            | AuthAction::CreateTempTable { .. }
            | AuthAction::CreateTempTrigger { .. }
            | AuthAction::CreateTempView { .. }
            | AuthAction::DropTempIndex { .. }
            | AuthAction::DropTempTable { .. }
            | AuthAction::DropTempTrigger { .. }
            | AuthAction::DropTempView { .. }
            | AuthAction::Pragma { .. }
            | AuthAction::Transaction { .. }
            | AuthAction::Attach { .. }
            | AuthAction::Detach { .. }
            | AuthAction::CreateVtable { .. }
            | AuthAction::DropVtable { .. }
            | AuthAction::Savepoint { .. }
    )
}

fn action_writes_connection(action: AuthAction<'_>) -> bool {
    matches!(
        action,
        AuthAction::Insert { .. } | AuthAction::Update { .. } | AuthAction::Delete { .. }
    )
}

fn configure_connection(connection: &Connection) -> EngineResult<()> {
    connection
        .busy_timeout(CONNECTION_BUSY_TIMEOUT)
        .map_err(sqlite_error::storage)?;
    configure_connection_pragmas(connection)
}

fn configure_manifest_connection(connection: &Connection) -> EngineResult<()> {
    connection
        .busy_timeout(CONNECTION_BUSY_TIMEOUT)
        .map_err(sqlite_error::storage)?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(sqlite_error::storage)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn configure_journal_mode(connection: &Connection) -> EngineResult<()> {
    let mode = connection
        .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))
        .map_err(sqlite_error::storage)?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("SQLite retained journal mode {mode} instead of enabling WAL"),
        ));
    }
    Ok(())
}

fn configure_connection_pragmas(connection: &Connection) -> EngineResult<()> {
    configure_journal_mode(connection)?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(sqlite_error::storage)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(sqlite_error::storage)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error as _,
        sync::{Arc, Barrier},
        thread,
    };

    use super::*;

    #[test]
    fn creates_layout_and_reopens_with_the_same_shard_count() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 4).unwrap();

        assert_eq!(storage.shard_count(), 4);
        assert!(temp.path().join("manifest.sqlite").exists());
        assert!(temp.path().join("shards/0003.sqlite").exists());
        assert!(Storage::open(temp.path(), 4).is_ok());

        let manifest = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
        assert_eq!(
            manifest
                .query_row(
                    "SELECT shard_count FROM briskdb_manifest WHERE singleton = 1",
                    [],
                    |row| row.get::<_, u16>(0),
                )
                .unwrap(),
            4
        );
        assert_eq!(
            manifest
                .pragma_query_value(None, "application_id", |row| row.get::<_, i64>(0))
                .unwrap(),
            manifest::MANIFEST_APPLICATION_ID
        );
        assert_eq!(
            manifest
                .pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
                .unwrap(),
            manifest::CURRENT_SCHEMA_VERSION
        );
    }

    #[test]
    fn rejects_invalid_or_changed_shard_counts() {
        let temp = tempfile::tempdir().unwrap();
        let too_few = Storage::open(temp.path(), 1).unwrap_err();
        assert_eq!(too_few.kind(), EngineErrorKind::InvalidArgument);
        assert_eq!(too_few.to_string(), "shard count must be between 2 and 64");

        let too_many = Storage::open(temp.path(), 65).unwrap_err();
        assert_eq!(too_many.kind(), EngineErrorKind::InvalidArgument);
        assert_eq!(too_many.to_string(), "shard count must be between 2 and 64");

        Storage::open(temp.path(), 4).unwrap();
        let changed = Storage::open(temp.path(), 8).unwrap_err();
        assert_eq!(changed.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(
            changed.to_string(),
            "database was created with 4 shards, but 8 were requested"
        );
    }

    #[test]
    fn rejects_opening_a_shard_outside_the_layout() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 4).unwrap();

        let error = storage.open_shard(4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert_eq!(error.to_string(), "shard 4 is outside the configured range");
    }

    #[test]
    fn rejects_an_incompatible_current_manifest_definition_as_data_corruption() {
        let temp = tempfile::tempdir().unwrap();
        Storage::open(temp.path(), 4).unwrap();
        let manifest = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
        manifest
            .execute_batch(
                "DROP TABLE briskdb_manifest;
                 CREATE TABLE briskdb_manifest (
                    singleton INTEGER PRIMARY KEY,
                    shard_count TEXT NOT NULL
                 ) STRICT;
                 INSERT INTO briskdb_manifest VALUES (1, '4');",
            )
            .unwrap();

        let error = Storage::open(temp.path(), 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(
            error.to_string(),
            "manifest table briskdb_manifest has an incompatible definition"
        );
    }

    #[test]
    fn rejects_impossible_numeric_manifest_counts_as_data_corruption() {
        for invalid in [0_i64, 1, 65, 65_535] {
            let temp = tempfile::tempdir().unwrap();
            Storage::open(temp.path(), 4).unwrap();
            let manifest = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
            manifest
                .pragma_update(None, "ignore_check_constraints", "ON")
                .unwrap();
            manifest
                .execute(
                    "UPDATE briskdb_manifest SET shard_count = ?1 WHERE singleton = 1",
                    [invalid],
                )
                .unwrap();

            let error = Storage::open(temp.path(), 4).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
            assert_eq!(
                error.to_string(),
                format!("manifest shard count {invalid} is outside the supported range")
            );
        }
    }

    #[test]
    fn rejects_a_future_manifest_before_enabling_wal_or_creating_shards() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let mut connection = Connection::open(&path).unwrap();
        manifest::load_or_create(&mut connection, 4).unwrap();
        connection
            .pragma_update(None, "user_version", manifest::CURRENT_SCHEMA_VERSION + 1)
            .unwrap();
        assert_eq!(
            connection
                .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                .unwrap(),
            "delete"
        );
        drop(connection);

        let error = Storage::open(temp.path(), 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(!temp.path().join("shards").exists());

        let connection = Connection::open(path).unwrap();
        assert_eq!(
            connection
                .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                .unwrap(),
            "delete"
        );
        assert_eq!(
            connection
                .pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
                .unwrap(),
            manifest::CURRENT_SCHEMA_VERSION + 1
        );
    }

    #[test]
    fn rejects_a_foreign_manifest_before_enabling_wal_or_creating_shards() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("manifest.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("CREATE TABLE foreign_data (id INTEGER PRIMARY KEY);")
            .unwrap();
        connection
            .pragma_update(None, "application_id", 0x1234)
            .unwrap();
        drop(connection);

        let error = Storage::open(temp.path(), 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(!temp.path().join("shards").exists());

        let connection = Connection::open(path).unwrap();
        assert_eq!(
            connection
                .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                .unwrap(),
            "delete"
        );
        assert_eq!(
            connection
                .pragma_query_value(None, "application_id", |row| row.get::<_, i64>(0))
                .unwrap(),
            0x1234
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM sqlite_schema WHERE name = 'foreign_data'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn concurrent_storage_openers_complete_one_wal_shard_layout() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let barrier = Arc::new(Barrier::new(4));
        let workers = (0..4)
            .map(|_| {
                let root = root.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    Storage::open(root, 4)
                })
            })
            .collect::<Vec<_>>();

        for worker in workers {
            assert_eq!(worker.join().unwrap().unwrap().shard_count(), 4);
        }
        for shard in 0..4 {
            assert!(
                root.join("shards")
                    .join(format!("{shard:04}.sqlite"))
                    .exists()
            );
        }
        let manifest = Connection::open(root.join("manifest.sqlite")).unwrap();
        assert_eq!(
            manifest
                .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                .unwrap(),
            "wal"
        );
        assert_eq!(
            manifest
                .query_row("SELECT shard_count FROM briskdb_manifest", [], |row| {
                    row.get::<_, u16>(0)
                })
                .unwrap(),
            4
        );
    }

    #[test]
    fn rejects_a_corrupt_manifest_database() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("manifest.sqlite"),
            b"not a sqlite database",
        )
        .unwrap();

        let error = Storage::open(temp.path(), 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert!(error.source().is_some());
    }

    #[test]
    fn classifies_invalid_storage_path_shape_as_a_failed_precondition() {
        let root_file = tempfile::NamedTempFile::new().unwrap();
        let error = Storage::open(root_file.path(), 4).unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(!error.is_retryable());
        assert!(error.source().is_some());
    }

    #[test]
    fn shard_connections_keep_the_durability_configuration() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 4).unwrap();
        let connection = storage.open_shard(0).unwrap();

        assert_eq!(
            connection
                .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "wal"
        );
        assert_eq!(
            connection
                .query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            connection
                .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            connection
                .query_row("PRAGMA busy_timeout", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            5_000
        );
    }
}
