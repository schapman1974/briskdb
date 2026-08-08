//! SQLite file layout, versioned manifest management, and connection configuration.

mod manifest;
mod migration;
mod schema_gate;
mod shard;

pub(crate) mod pool;
pub(crate) use pool::{ConnectionOwner, ConnectionPools, PooledConnection};

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use rusqlite::{
    Connection, OpenFlags,
    hooks::{AuthAction, AuthContext, Authorization},
};

#[cfg(test)]
pub(crate) use migration::SchemaMigrationCoordinatorPoint;
#[cfg(test)]
pub(crate) use schema_gate::{SchemaGateSnapshot, SchemaGateState};
pub(crate) use schema_gate::{SchemaMigrationGuard, SchemaOperationGuard};

pub use crate::core::Database;
use crate::{
    core::{Catalog, CatalogSnapshot, EngineError, EngineErrorKind, EngineResult},
    sqlite_error,
};

pub(crate) const CONNECTION_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug)]
struct RootSchemaCoordination {
    gate: schema_gate::SchemaGate,
    catalogs: Mutex<Vec<Weak<CatalogSnapshot>>>,
}

impl RootSchemaCoordination {
    fn new() -> Self {
        Self {
            gate: schema_gate::SchemaGate::new(),
            catalogs: Mutex::new(Vec::new()),
        }
    }

    fn register_catalog(&self, loaded: CatalogSnapshot) -> EngineResult<Arc<CatalogSnapshot>> {
        let loaded = Arc::new(loaded);
        let mut catalogs = self.catalogs.lock().map_err(|error| {
            EngineError::new(
                EngineErrorKind::Internal,
                format!("root schema catalog coordination is poisoned: {error}"),
            )
        })?;
        catalogs.retain(|catalog| catalog.strong_count() != 0);
        catalogs.push(Arc::downgrade(&loaded));
        Ok(loaded)
    }

    fn publish_schema_generation(
        &self,
        expected_generation: u64,
        target_generation: u64,
    ) -> EngineResult<()> {
        let mut catalogs = self.catalogs.lock().map_err(|error| {
            EngineError::new(
                EngineErrorKind::Internal,
                format!("root schema catalog coordination is poisoned: {error}"),
            )
        })?;
        catalogs.retain(|catalog| catalog.strong_count() != 0);
        let live = catalogs
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();

        if let Some(observed) = live
            .iter()
            .map(|catalog| catalog.logical().schema_generation())
            .find(|generation| {
                *generation != expected_generation && *generation != target_generation
            })
        {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                format!(
                    "cannot publish schema generation {target_generation}; a live catalog is at generation {observed}"
                ),
            ));
        }

        for catalog in live {
            catalog
                .logical()
                .publish_schema_generation(expected_generation, target_generation)?;
        }
        Ok(())
    }

    fn reconcile_validated_catalog_generation(
        &self,
        validated: &CatalogSnapshot,
    ) -> EngineResult<()> {
        let target_generation = validated.logical().schema_generation();
        let mut catalogs = self.catalogs.lock().map_err(|error| {
            EngineError::new(
                EngineErrorKind::Internal,
                format!("root schema catalog coordination is poisoned: {error}"),
            )
        })?;
        catalogs.retain(|catalog| catalog.strong_count() != 0);
        let live = catalogs
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();

        if live
            .iter()
            .all(|catalog| catalog.logical().schema_generation() == target_generation)
        {
            return Ok(());
        }
        let source_generation = target_generation.checked_sub(1).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                "validated manifest generation conflicts with a live catalog",
            )
        })?;
        if live.iter().any(|catalog| {
            !matches!(
                catalog.logical().schema_generation(),
                generation if generation == source_generation || generation == target_generation
            )
        }) {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "validated manifest generation conflicts with a live catalog",
            ));
        }

        for catalog in live {
            catalog
                .logical()
                .publish_schema_generation(source_generation, target_generation)?;
        }
        Ok(())
    }
}

static ROOT_SCHEMA_COORDINATIONS: OnceLock<Mutex<HashMap<PathBuf, Weak<RootSchemaCoordination>>>> =
    OnceLock::new();

fn root_schema_coordination(root: &Path) -> EngineResult<Arc<RootSchemaCoordination>> {
    let registry = ROOT_SCHEMA_COORDINATIONS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry.lock().map_err(|error| {
        EngineError::new(
            EngineErrorKind::Internal,
            format!("root schema coordination registry is poisoned: {error}"),
        )
    })?;
    registry.retain(|_, coordination| coordination.strong_count() != 0);
    if let Some(coordination) = registry.get(root).and_then(Weak::upgrade) {
        return Ok(coordination);
    }
    let coordination = Arc::new(RootSchemaCoordination::new());
    registry.insert(root.to_path_buf(), Arc::downgrade(&coordination));
    Ok(coordination)
}

fn begin_startup_coordination(
    coordination: &RootSchemaCoordination,
) -> EngineResult<SchemaMigrationGuard> {
    let started = Instant::now();
    loop {
        match coordination.gate.begin_migration() {
            Ok(guard) => return Ok(guard),
            Err(error)
                if error.kind() == EngineErrorKind::Busy
                    && started.elapsed() < CONNECTION_BUSY_TIMEOUT =>
            {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(error) => return Err(error),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Storage {
    root: PathBuf,
    catalog: Arc<CatalogSnapshot>,
    shard_layout: shard::ShardLayout,
    schema_coordination: Arc<RootSchemaCoordination>,
}

impl Storage {
    pub(crate) fn open(root: impl AsRef<Path>, requested_shards: u16) -> EngineResult<Self> {
        validate_shard_count(requested_shards)?;

        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|error| {
            sqlite_error::storage_io(error, format!("failed to create {}", root.display()))
        })?;
        let root = fs::canonicalize(&root).map_err(|error| {
            sqlite_error::storage_io(error, format!("failed to resolve {}", root.display()))
        })?;
        let schema_coordination = root_schema_coordination(&root)?;
        let mut startup = begin_startup_coordination(&schema_coordination)?;
        startup.wait_for_quiescence_blocking();
        let shards_dir = root.join("shards");
        let fresh_layout_allowed = physical_layout_is_empty(&shards_dir)?;
        let manifest_path = root.join("manifest.sqlite");
        let mut manifest = open_manifest_for_startup(&manifest_path)?;
        configure_manifest_connection(&manifest)?;
        let loaded = manifest::load_or_create_manifest_with_fresh_layout(
            &mut manifest,
            requested_shards,
            fresh_layout_allowed,
        )?;
        configure_journal_mode(&manifest)?;
        let (catalog, shard_layout, active_migration) = loaded.into_parts_with_migration();
        let catalog = schema_coordination.register_catalog(catalog)?;

        if let Some(active_migration) = active_migration {
            startup.mark_pending_on_drop();
            migration::resume_schema_migration_on_startup(
                &root,
                &mut manifest,
                requested_shards,
                &catalog,
                &shard_layout,
                &schema_coordination,
                active_migration,
            )?;
        }
        let schema_generation = catalog.logical().schema_generation();

        let ready_layout = manifest::reconcile_shard_layout(
            &mut manifest,
            requested_shards,
            &shard_layout,
            |locked_layout| {
                shard::prepare_layout(
                    &shards_dir,
                    catalog.routing().shard_count(),
                    schema_generation,
                    locked_layout,
                )
            },
        )?;

        let storage = Self {
            root,
            catalog,
            shard_layout: ready_layout,
            schema_coordination,
        };
        for shard in 0..storage.shard_count() {
            storage.open_shard(shard)?;
        }
        storage
            .schema_coordination
            .reconcile_validated_catalog_generation(&storage.catalog)?;
        startup.publish_ready()?;
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

    pub(crate) fn current_schema_generation(&self) -> u64 {
        self.catalog.logical().schema_generation()
    }

    pub(crate) fn enter_schema_operation(&self) -> EngineResult<SchemaOperationGuard> {
        self.schema_coordination.gate.try_acquire_operation()
    }

    pub(crate) fn begin_schema_migration(&self) -> EngineResult<SchemaMigrationGuard> {
        self.schema_coordination.gate.begin_migration()
    }

    #[cfg(test)]
    pub(crate) fn install_schema_migration_test_block(
        &self,
        point: SchemaMigrationCoordinatorPoint,
        started: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> EngineResult<()> {
        migration::install_schema_migration_test_block(&self.root, point, started, release)
    }

    fn publish_schema_generation(
        &self,
        expected_generation: u64,
        target_generation: u64,
    ) -> EngineResult<()> {
        self.schema_coordination
            .publish_schema_generation(expected_generation, target_generation)
    }

    #[cfg(test)]
    pub(crate) fn schema_gate_snapshot(&self) -> SchemaGateSnapshot {
        self.schema_coordination.gate.snapshot()
    }

    pub(crate) fn apply_schema_migration(
        &self,
        sql: &str,
        guard: &mut SchemaMigrationGuard,
        control: Option<Arc<crate::core::OperationControl>>,
    ) -> EngineResult<Vec<u16>> {
        migration::apply_schema_migration(self, sql, guard, control)
    }

    fn shard_path(&self, shard: u16) -> PathBuf {
        self.root.join("shards").join(format!("{shard:04}.sqlite"))
    }

    pub(crate) fn open_shard(&self, shard: u16) -> EngineResult<Connection> {
        self.ensure_shard_in_range(shard)?;
        let path = self.shard_path(shard);
        let connection = shard::open_existing(
            &path,
            shard,
            self.catalog.logical().schema_generation(),
            &self.shard_layout,
        )?;
        attach_storage_authorizer(&connection)?;
        Ok(connection)
    }

    fn open_unconfigured_shard(&self, shard: u16) -> EngineResult<Connection> {
        self.ensure_shard_in_range(shard)?;
        shard::open_required_file(&self.shard_path(shard))
    }

    fn validate_unconfigured_shard(&self, connection: &Connection, shard: u16) -> EngineResult<()> {
        self.ensure_shard_in_range(shard)?;
        shard::validate_open_connection(
            connection,
            &self.shard_path(shard),
            shard,
            self.catalog.logical().schema_generation(),
            &self.shard_layout,
        )
    }

    fn ensure_shard_in_range(&self, shard: u16) -> EngineResult<()> {
        if shard >= self.shard_count() {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                format!("shard {shard} is outside the configured range"),
            ));
        }
        Ok(())
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
                if shard::denies_client_action(context.action) {
                    return Authorization::Deny;
                }
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

fn physical_layout_is_empty(shards_dir: &Path) -> EngineResult<bool> {
    let metadata = match fs::symlink_metadata(shards_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
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
    let mut entries = fs::read_dir(shards_dir).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!("failed to enumerate {}", shards_dir.display()),
        )
    })?;
    match entries.next() {
        None => Ok(true),
        Some(Ok(_)) => Ok(false),
        Some(Err(error)) => Err(sqlite_error::storage_io(
            error,
            format!("failed to enumerate {}", shards_dir.display()),
        )),
    }
}

fn attach_storage_authorizer(connection: &Connection) -> EngineResult<()> {
    connection
        .authorizer(Some(|context: AuthContext<'_>| {
            if shard::denies_client_action(context.action) {
                Authorization::Deny
            } else {
                Authorization::Allow
            }
        }))
        .map_err(sqlite_error::storage)
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

fn open_manifest_for_startup(path: &Path) -> EngineResult<Connection> {
    validate_optional_manifest_file(path)?;
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_EXRESCODE;
    let open_path = canonical_manifest_open_path(path)?;
    let connection = Connection::open_with_flags(open_path, flags).map_err(|error| {
        sqlite_error::storage(error).context(format!("failed to open {}", path.display()))
    })?;
    validate_existing_manifest_file(path)?;
    Ok(connection)
}

pub(super) fn open_existing_manifest(path: &Path) -> EngineResult<Connection> {
    validate_existing_manifest_file(path)?;
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_EXRESCODE;
    let open_path = canonical_manifest_open_path(path)?;
    let connection = Connection::open_with_flags(open_path, flags).map_err(|error| {
        sqlite_error::storage(error).context(format!("failed to open {}", path.display()))
    })?;
    validate_existing_manifest_file(path)?;
    Ok(connection)
}

fn validate_optional_manifest_file(path: &Path) -> EngineResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_symlink() && metadata.is_file() => Ok(()),
        Ok(_) => Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("manifest {} is not a real file", path.display()),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(sqlite_error::storage_io(
            error,
            format!("failed to inspect {}", path.display()),
        )),
    }
}

fn validate_existing_manifest_file(path: &Path) -> EngineResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            EngineError::from_source(
                EngineErrorKind::DataCorruption,
                format!("required manifest {} is missing", path.display()),
                error,
            )
        } else {
            sqlite_error::storage_io(error, format!("failed to inspect {}", path.display()))
        }
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("manifest {} is not a real file", path.display()),
        ));
    }
    Ok(())
}

fn canonical_manifest_open_path(path: &Path) -> EngineResult<PathBuf> {
    let parent = path.parent().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("manifest path {} has no parent directory", path.display()),
        )
    })?;
    let parent_metadata = fs::symlink_metadata(parent).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!("failed to inspect manifest directory {}", parent.display()),
        )
    })?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("manifest path {} is not a real directory", parent.display()),
        ));
    }
    let file_name = path.file_name().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("manifest path {} has no file name", path.display()),
        )
    })?;
    let canonical_parent = fs::canonicalize(parent).map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!("failed to resolve manifest directory {}", parent.display()),
        )
    })?;
    Ok(canonical_parent.join(file_name))
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

#[cfg(test)]
mod tests {
    use std::{
        error::Error as _,
        sync::{Arc, Barrier},
        thread,
    };

    use super::*;

    fn shard_file(root: &Path, shard: u16) -> PathBuf {
        root.join("shards").join(format!("{shard:04}.sqlite"))
    }

    fn manifest_layout_row(root: &Path) -> (Vec<u8>, i64, i64, i64) {
        Connection::open(root.join("manifest.sqlite"))
            .unwrap()
            .query_row(
                "SELECT layout_id,
                        shard_application_id,
                        shard_metadata_version,
                        layout_state
                 FROM briskdb_shard_layout
                 WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap()
    }

    fn create_v4_layout(root: &Path, shard_count: u16) {
        let mut manifest = Connection::open(root.join("manifest.sqlite")).unwrap();
        manifest::create_v4_fixture(&mut manifest, shard_count);
        fs::create_dir(root.join("shards")).unwrap();
        for shard in 0..shard_count {
            let connection = Connection::open(shard_file(root, shard)).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE legacy_rows (
                        shard_id INTEGER PRIMARY KEY,
                        value TEXT NOT NULL
                     );",
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO legacy_rows (shard_id, value) VALUES (?1, ?2)",
                    rusqlite::params![i64::from(shard), format!("legacy-{shard}")],
                )
                .unwrap();
            let mode = connection
                .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))
                .unwrap();
            assert_eq!(mode.to_ascii_lowercase(), "wal");
        }
    }

    #[test]
    fn creates_layout_and_reopens_with_the_same_shard_count() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 4).unwrap();

        assert_eq!(storage.shard_count(), 4);
        assert!(temp.path().join("manifest.sqlite").exists());
        assert!(temp.path().join("shards/0003.sqlite").exists());

        let observer_paths = std::iter::once(temp.path().join("manifest.sqlite"))
            .chain((0..4).map(|shard| shard_file(temp.path(), shard)))
            .collect::<Vec<_>>();
        let observers = observer_paths
            .iter()
            .map(|path| Connection::open(path).unwrap())
            .collect::<Vec<_>>();
        let data_versions_before = observers
            .iter()
            .map(|connection| {
                connection
                    .pragma_query_value(None, "data_version", |row| row.get::<_, i64>(0))
                    .unwrap()
            })
            .collect::<Vec<_>>();

        drop(Storage::open(temp.path(), 4).unwrap());

        for (index, (connection, before)) in observers
            .iter()
            .zip(data_versions_before.iter())
            .enumerate()
        {
            let after = connection
                .pragma_query_value(None, "data_version", |row| row.get::<_, i64>(0))
                .unwrap();
            assert_eq!(
                *before,
                after,
                "ready reopen wrote to {}",
                observer_paths[index].display()
            );
        }

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
        let (layout_id, application_id, metadata_version, state) = manifest_layout_row(temp.path());
        assert_eq!(layout_id.len(), 16);
        assert_eq!(application_id, shard::SHARD_APPLICATION_ID);
        assert_eq!(metadata_version, i64::from(shard::SHARD_METADATA_VERSION));
        assert_eq!(state, shard::ShardLayoutState::Ready.code());
        for shard_id in 0..4 {
            let connection = Connection::open(shard_file(temp.path(), shard_id)).unwrap();
            assert_eq!(
                connection
                    .pragma_query_value(None, "application_id", |row| row.get::<_, i64>(0))
                    .unwrap(),
                shard::SHARD_APPLICATION_ID
            );
            assert_eq!(
                connection
                    .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                    .unwrap(),
                0
            );
            assert_eq!(
                connection
                    .query_row(
                        "SELECT layout_id, shard_id FROM briskdb_shard_metadata",
                        [],
                        |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, u16>(1)?)),
                    )
                    .unwrap(),
                (layout_id.clone(), shard_id)
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn canonical_and_symlink_roots_share_migration_coordination() {
        let temp = tempfile::tempdir().unwrap();
        let original = Storage::open(temp.path(), 2).unwrap();
        let alias_parent = tempfile::tempdir().unwrap();
        let alias_path = alias_parent.path().join("database-alias");
        std::os::unix::fs::symlink(temp.path(), &alias_path).unwrap();
        let alias = Storage::open(&alias_path, 2).unwrap();

        assert!(Arc::ptr_eq(
            &original.schema_coordination,
            &alias.schema_coordination
        ));
        let mut migration = alias.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        assert_eq!(
            original.enter_schema_operation().unwrap_err().kind(),
            EngineErrorKind::Busy
        );
        assert_eq!(
            alias
                .apply_schema_migration(
                    "CREATE TABLE canonical_root_marker (id INTEGER)",
                    &mut migration,
                    None,
                )
                .unwrap(),
            [0, 1]
        );
        migration.publish_ready().unwrap();

        assert_eq!(original.current_schema_generation(), 1);
        assert_eq!(alias.current_schema_generation(), 1);
        for shard_id in 0..2 {
            assert!(
                original
                    .open_shard(shard_id)
                    .unwrap()
                    .query_row(
                        "SELECT EXISTS(
                            SELECT 1 FROM sqlite_schema WHERE name = 'canonical_root_marker'
                         )",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap()
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn startup_rejects_a_symlinked_manifest_without_mutating_its_target() {
        let target_root = tempfile::tempdir().unwrap();
        drop(Storage::open(target_root.path(), 2).unwrap());
        let target_manifest = target_root.path().join("manifest.sqlite");
        let before = fs::read(&target_manifest).unwrap();

        let victim_root = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(&target_manifest, victim_root.path().join("manifest.sqlite"))
            .unwrap();
        let error = Storage::open(victim_root.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(!victim_root.path().join("shards").exists());
        assert_eq!(fs::read(&target_manifest).unwrap(), before);
        assert_eq!(
            Connection::open(&target_manifest)
                .unwrap()
                .query_row(
                    "SELECT schema_generation FROM briskdb_schema_catalog WHERE singleton = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn version_four_layout_is_adopted_without_changing_application_data() {
        let temp = tempfile::tempdir().unwrap();
        create_v4_layout(temp.path(), 4);

        let storage = Storage::open(temp.path(), 4).unwrap();
        assert_eq!(storage.shard_count(), 4);
        assert_eq!(
            manifest_layout_row(temp.path()).3,
            shard::ShardLayoutState::Ready.code()
        );
        for shard_id in 0..4 {
            let connection = storage.open_shard(shard_id).unwrap();
            assert_eq!(
                connection
                    .query_row(
                        "SELECT value FROM legacy_rows WHERE shard_id = ?1",
                        [shard_id],
                        |row| row.get::<_, String>(0),
                    )
                    .unwrap(),
                format!("legacy-{shard_id}")
            );
        }
    }

    #[test]
    fn failed_version_four_preflight_stays_adopting_and_changes_no_legacy_shard() {
        let temp = tempfile::tempdir().unwrap();
        create_v4_layout(temp.path(), 2);
        let first = shard_file(temp.path(), 0);
        let missing = shard_file(temp.path(), 1);
        fs::remove_file(&missing).unwrap();

        let error = Storage::open(temp.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert!(!missing.exists());
        assert_eq!(
            manifest_layout_row(temp.path()).3,
            shard::ShardLayoutState::Adopting.code()
        );
        let connection = Connection::open(first).unwrap();
        assert_eq!(
            connection
                .pragma_query_value(None, "application_id", |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .query_row("SELECT value FROM legacy_rows", [], |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "legacy-0"
        );
    }

    #[test]
    fn partial_creating_layout_resumes_through_storage_open_and_publishes_ready() {
        let temp = tempfile::tempdir().unwrap();
        let shards_dir = temp.path().join("shards");
        let mut manifest = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
        configure_manifest_connection(&manifest).unwrap();
        let loaded =
            manifest::load_or_create_manifest_with_fresh_layout(&mut manifest, 2, true).unwrap();
        configure_journal_mode(&manifest).unwrap();
        let (catalog, layout) = loaded.into_parts();

        let error = manifest::reconcile_shard_layout(&mut manifest, 2, &layout, |locked| {
            shard::prepare_layout_with_hook(
                &shards_dir,
                2,
                catalog.logical().schema_generation(),
                locked,
                |shard_id| {
                    if shard_id == 0 {
                        Err(EngineError::new(
                            EngineErrorKind::Internal,
                            "injected failure after first fresh shard",
                        ))
                    } else {
                        Ok(())
                    }
                },
            )
        })
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        drop(manifest);
        assert_eq!(
            manifest_layout_row(temp.path()).3,
            shard::ShardLayoutState::Creating.code()
        );
        assert!(shard_file(temp.path(), 0).exists());
        assert!(!shard_file(temp.path(), 1).exists());

        drop(Storage::open(temp.path(), 2).unwrap());
        assert_eq!(
            manifest_layout_row(temp.path()).3,
            shard::ShardLayoutState::Ready.code()
        );
        assert!(shard_file(temp.path(), 1).exists());
    }

    #[test]
    fn partial_adopting_layout_resumes_through_storage_open_and_preserves_data() {
        let temp = tempfile::tempdir().unwrap();
        create_v4_layout(temp.path(), 2);
        let shards_dir = temp.path().join("shards");
        let mut manifest = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
        configure_manifest_connection(&manifest).unwrap();
        let loaded =
            manifest::load_or_create_manifest_with_fresh_layout(&mut manifest, 2, false).unwrap();
        configure_journal_mode(&manifest).unwrap();
        let (catalog, layout) = loaded.into_parts();
        assert_eq!(layout.state(), shard::ShardLayoutState::Adopting);

        let error = manifest::reconcile_shard_layout(&mut manifest, 2, &layout, |locked| {
            shard::prepare_layout_with_hook(
                &shards_dir,
                2,
                catalog.logical().schema_generation(),
                locked,
                |shard_id| {
                    if shard_id == 0 {
                        Err(EngineError::new(
                            EngineErrorKind::Internal,
                            "injected failure after first adopted shard",
                        ))
                    } else {
                        Ok(())
                    }
                },
            )
        })
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        drop(manifest);
        assert_eq!(
            manifest_layout_row(temp.path()).3,
            shard::ShardLayoutState::Adopting.code()
        );
        let first = Connection::open(shard_file(temp.path(), 0)).unwrap();
        let second = Connection::open(shard_file(temp.path(), 1)).unwrap();
        assert_eq!(
            first
                .pragma_query_value(None, "application_id", |row| row.get::<_, i64>(0))
                .unwrap(),
            shard::SHARD_APPLICATION_ID
        );
        assert_eq!(
            second
                .pragma_query_value(None, "application_id", |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        drop((first, second));

        let storage = Storage::open(temp.path(), 2).unwrap();
        assert_eq!(
            manifest_layout_row(temp.path()).3,
            shard::ShardLayoutState::Ready.code()
        );
        for shard_id in 0..2 {
            let value = storage
                .open_shard(shard_id)
                .unwrap()
                .query_row("SELECT value FROM legacy_rows", [], |row| {
                    row.get::<_, String>(0)
                })
                .unwrap();
            assert_eq!(value, format!("legacy-{shard_id}"));
        }
    }

    #[test]
    fn ready_layout_never_recreates_a_missing_shard_at_startup_or_runtime() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let missing = shard_file(temp.path(), 1);
        fs::remove_file(&missing).unwrap();

        let runtime = storage.open_shard(1).unwrap_err();
        assert_eq!(runtime.kind(), EngineErrorKind::DataCorruption);
        assert!(!missing.exists());
        let pooled = storage.open_pooled_shard(1).unwrap_err();
        assert_eq!(pooled.kind(), EngineErrorKind::DataCorruption);
        assert!(!missing.exists());
        drop(storage);

        let startup = Storage::open(temp.path(), 2).unwrap_err();
        assert_eq!(startup.kind(), EngineErrorKind::DataCorruption);
        assert!(!missing.exists());
        assert_eq!(
            manifest_layout_row(temp.path()).3,
            shard::ShardLayoutState::Ready.code()
        );
    }

    #[test]
    fn startup_rejects_swapped_and_cross_layout_shards() {
        let swapped = tempfile::tempdir().unwrap();
        drop(Storage::open(swapped.path(), 2).unwrap());
        let first = shard_file(swapped.path(), 0);
        let second = shard_file(swapped.path(), 1);
        let temporary = swapped.path().join("shards/swap.tmp");
        fs::rename(&first, &temporary).unwrap();
        fs::rename(&second, &first).unwrap();
        fs::rename(&temporary, &second).unwrap();
        let error = Storage::open(swapped.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);

        let source = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        drop(Storage::open(source.path(), 2).unwrap());
        drop(Storage::open(target.path(), 2).unwrap());
        fs::copy(shard_file(source.path(), 0), shard_file(target.path(), 0)).unwrap();
        let error = Storage::open(target.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
    }

    #[test]
    fn startup_rejects_header_generation_and_wal_tampering_without_repair() {
        let foreign = tempfile::tempdir().unwrap();
        drop(Storage::open(foreign.path(), 2).unwrap());
        Connection::open(shard_file(foreign.path(), 0))
            .unwrap()
            .pragma_update(None, "application_id", 0x1234)
            .unwrap();
        let error = Storage::open(foreign.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);

        let future = tempfile::tempdir().unwrap();
        drop(Storage::open(future.path(), 2).unwrap());
        Connection::open(shard_file(future.path(), 0))
            .unwrap()
            .pragma_update(None, "user_version", 1)
            .unwrap();
        let error = Storage::open(future.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);

        let non_wal = tempfile::tempdir().unwrap();
        drop(Storage::open(non_wal.path(), 2).unwrap());
        let path = shard_file(non_wal.path(), 0);
        let connection = Connection::open(&path).unwrap();
        let mode = connection
            .pragma_update_and_check(None, "journal_mode", "DELETE", |row| {
                row.get::<_, String>(0)
            })
            .unwrap();
        assert_eq!(mode.to_ascii_lowercase(), "delete");
        drop(connection);
        let error = Storage::open(non_wal.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(
            Connection::open(path)
                .unwrap()
                .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                .unwrap()
                .to_ascii_lowercase(),
            "delete"
        );
    }

    #[test]
    fn unrelated_files_are_tolerated_but_extra_canonical_shards_fail_startup() {
        let temp = tempfile::tempdir().unwrap();
        drop(Storage::open(temp.path(), 2).unwrap());
        fs::write(
            temp.path().join("shards/operator-notes.sqlite"),
            b"not a canonical shard",
        )
        .unwrap();
        drop(Storage::open(temp.path(), 2).unwrap());

        fs::copy(
            shard_file(temp.path(), 0),
            temp.path().join("shards/0002.sqlite"),
        )
        .unwrap();
        let error = Storage::open(temp.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
    }

    #[cfg(unix)]
    #[test]
    fn runtime_open_rejects_replacing_the_shards_directory_with_a_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let shards = temp.path().join("shards");
        let moved = temp.path().join("moved-shards");
        fs::rename(&shards, &moved).unwrap();
        symlink(&moved, &shards).unwrap();

        let error = storage.open_shard(0).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
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
    fn fresh_manifest_is_not_initialized_beside_unexplained_physical_files() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir(temp.path().join("shards")).unwrap();
        fs::write(temp.path().join("shards/operator-note"), b"do not adopt").unwrap();

        let error = Storage::open(temp.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(!shard_file(temp.path(), 0).exists());
        let manifest = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
        assert_eq!(
            manifest
                .pragma_query_value(None, "application_id", |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            manifest
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
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

    #[test]
    fn every_connection_surface_denies_storage_owned_sql_and_preserves_identity() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let mut migration = storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        storage
            .apply_schema_migration(
                "CREATE TABLE application_rows (id INTEGER PRIMARY KEY)",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();

        for connection in [
            storage.open_shard(0).unwrap(),
            storage.open_pooled_shard(0).unwrap().0,
        ] {
            connection
                .execute_batch("INSERT OR IGNORE INTO application_rows VALUES (1);")
                .unwrap();
            for statement in [
                "PRAGMA application_id = 7",
                "PRAGMA user_version = 7",
                "PRAGMA journal_mode = DELETE",
                "PRAGMA writable_schema = ON",
                "UPDATE briskdb_shard_metadata SET shard_id = 1",
                "SELECT * FROM briskdb_shard_metadata",
                "DROP TABLE briskdb_shard_metadata",
                "CREATE TABLE briskdb_future (id INTEGER)",
                "CREATE TABLE denied_application_table (id INTEGER)",
                "DROP TABLE application_rows",
                "ALTER TABLE application_rows RENAME TO briskdb_future",
            ] {
                assert!(
                    connection.execute_batch(statement).is_err(),
                    "storage-owned statement was authorized: {statement}"
                );
            }
            assert_eq!(
                connection
                    .query_row(
                        "SELECT count(*) FROM sqlite_schema
                         WHERE type = 'table' AND name = 'application_rows'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                1
            );
        }

        let raw = Connection::open(shard_file(temp.path(), 0)).unwrap();
        assert_eq!(
            raw.pragma_query_value(None, "application_id", |row| row.get::<_, i64>(0))
                .unwrap(),
            shard::SHARD_APPLICATION_ID
        );
        assert_eq!(
            raw.pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            i64::try_from(storage.current_schema_generation()).unwrap()
        );
        assert_eq!(
            raw.pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                .unwrap()
                .to_ascii_lowercase(),
            "wal"
        );
    }

    #[test]
    fn protocol_neutral_database_surfaces_report_storage_denials() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 2).unwrap();
        assert_eq!(
            database
                .broadcast("CREATE TABLE migration_only (id INTEGER PRIMARY KEY)")
                .unwrap(),
            [0, 1]
        );

        let pragma = database
            .execute("tenant", "PRAGMA user_version = 7", &[])
            .unwrap_err();
        assert_eq!(pragma.kind(), EngineErrorKind::PermissionDenied);
        let metadata = database
            .query("tenant", "SELECT shard_id FROM briskdb_shard_metadata", &[])
            .unwrap_err();
        assert_eq!(metadata.kind(), EngineErrorKind::PermissionDenied);
        let routed_ddl = database
            .execute(
                "tenant",
                "CREATE TABLE bypassed_migration (id INTEGER)",
                &[],
            )
            .unwrap_err();
        assert_eq!(routed_ddl.kind(), EngineErrorKind::PermissionDenied);
        let broadcast = database
            .broadcast("UPDATE briskdb_shard_metadata SET shard_id = 1")
            .unwrap_err();
        assert_eq!(broadcast.kind(), EngineErrorKind::PermissionDenied);
        for shard_id in 0..2 {
            let shard = Connection::open(shard_file(temp.path(), shard_id)).unwrap();
            assert!(
                shard
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'migration_only')",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap()
            );
            assert!(
                !shard
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'bypassed_migration')",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap()
            );
        }
    }
}
