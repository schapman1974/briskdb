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
    schema_digests: Mutex<RuntimeSchemaDigests>,
}

#[derive(Debug, Default, Clone, Copy)]
struct RuntimeSchemaDigests {
    committed: Option<[u8; 32]>,
    target: Option<[u8; 32]>,
}

impl RootSchemaCoordination {
    fn new() -> Self {
        Self {
            gate: schema_gate::SchemaGate::new(),
            catalogs: Mutex::new(Vec::new()),
            schema_digests: Mutex::new(RuntimeSchemaDigests::default()),
        }
    }

    fn mark_degraded(&self) {
        self.gate.mark_degraded();
    }

    fn publish_schema_digests(
        &self,
        committed: Option<[u8; 32]>,
        target: Option<[u8; 32]>,
    ) -> EngineResult<()> {
        let mut digests = self.schema_digests.lock().map_err(|error| {
            EngineError::new(
                EngineErrorKind::Internal,
                format!("root schema checksum coordination is poisoned: {error}"),
            )
        })?;
        *digests = RuntimeSchemaDigests { committed, target };
        Ok(())
    }

    fn committed_schema_digest(&self) -> EngineResult<[u8; 32]> {
        self.schema_digests
            .lock()
            .map_err(|error| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    format!("root schema checksum coordination is poisoned: {error}"),
                )
            })?
            .committed
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "application schema has not completed integrity verification",
                )
            })
    }

    fn target_schema_digest(&self) -> EngineResult<Option<[u8; 32]>> {
        Ok(self
            .schema_digests
            .lock()
            .map_err(|error| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    format!("root schema checksum coordination is poisoned: {error}"),
                )
            })?
            .target)
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

fn validate_schema_migration_checksum_prefix(
    shards_dir: &Path,
    shard_count: u16,
    layout: &shard::ShardLayout,
    migration: &manifest::SchemaMigration,
    integrity: manifest::ManifestIntegrity,
) -> EngineResult<()> {
    if integrity.state() != manifest::DatabaseIntegrityState::Migrating {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "active schema migration has an inconsistent database integrity state",
        ));
    }
    let source_digest = integrity.committed_schema_digest().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::DataCorruption,
            "active schema migration is missing its source checksum",
        )
    })?;
    let target_digest = integrity.target_schema_digest().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::DataCorruption,
            "active schema migration is missing its target checksum",
        )
    })?;
    shard::validate_schema_migration_prefix_with(
        shards_dir,
        shard_count,
        migration.next_shard(),
        migration.source_generation(),
        migration.target_generation(),
        layout,
        |path, shard_id| {
            let connection = shard::open_required_file(path)?;
            connection
                .busy_timeout(CONNECTION_BUSY_TIMEOUT)
                .map_err(sqlite_error::storage)?;
            let state = shard::validate_schema_migration_connection(
                &connection,
                path,
                shard_id,
                migration.source_generation(),
                migration.target_generation(),
                layout,
            )?;
            let (generation, expected) = match state {
                shard::SchemaMigrationShardState::Source => {
                    (migration.source_generation(), &source_digest)
                }
                shard::SchemaMigrationShardState::Target => {
                    (migration.target_generation(), &target_digest)
                }
            };
            shard::verify_schema_digest(&connection, generation, expected)?;
            Ok(state)
        },
    )?;
    Ok(())
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
        if let Err(error) = configure_manifest_connection(&manifest) {
            if error.kind() == EngineErrorKind::DataCorruption {
                schema_coordination.mark_degraded();
            }
            return Err(error);
        }
        let v6_active = match manifest::load_v6_active_migration(&manifest, requested_shards) {
            Ok(active) => active,
            Err(error) => {
                if error.kind() == EngineErrorKind::DataCorruption {
                    schema_coordination.mark_degraded();
                }
                return Err(error);
            }
        };
        if let Some(v6_active) = v6_active {
            configure_journal_mode(&manifest)?;
            let (v6_catalog, v6_layout, active_migration) = v6_active.into_parts();
            let v6_catalog = schema_coordination.register_catalog(v6_catalog)?;
            startup.mark_pending_on_drop();
            if let Err(error) = migration::resume_schema_migration_on_startup(
                &root,
                &mut manifest,
                requested_shards,
                &v6_catalog,
                &v6_layout,
                &schema_coordination,
                active_migration,
            ) {
                if error.kind() == EngineErrorKind::DataCorruption {
                    schema_coordination.mark_degraded();
                }
                return Err(error);
            }
        }

        let loaded = match manifest::load_or_create_manifest_with_fresh_layout(
            &mut manifest,
            requested_shards,
            fresh_layout_allowed,
        ) {
            Ok(loaded) => loaded,
            Err(error) => {
                if error.kind() == EngineErrorKind::DataCorruption {
                    schema_coordination.mark_degraded();
                }
                return Err(error);
            }
        };
        let (catalog, shard_layout, active_migration, mut integrity) =
            loaded.into_parts_with_migration();
        let catalog = schema_coordination.register_catalog(catalog)?;
        schema_coordination.publish_schema_digests(
            integrity.committed_schema_digest(),
            integrity.target_schema_digest(),
        )?;

        if integrity.state() == manifest::DatabaseIntegrityState::Degraded {
            schema_coordination.mark_degraded();
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "database is persistently degraded and requires a complete known-good restore",
            ));
        }
        configure_journal_mode(&manifest)?;

        if let Some(active_migration) = active_migration {
            startup.mark_pending_on_drop();
            let recovery = (|| {
                validate_schema_migration_checksum_prefix(
                    &root.join("shards"),
                    requested_shards,
                    &shard_layout,
                    &active_migration,
                    integrity,
                )?;
                migration::resume_schema_migration_on_startup(
                    &root,
                    &mut manifest,
                    requested_shards,
                    &catalog,
                    &shard_layout,
                    &schema_coordination,
                    active_migration,
                )
            })();
            if let Err(error) = recovery {
                if error.kind() == EngineErrorKind::DataCorruption {
                    schema_coordination.mark_degraded();
                    let _ = manifest::mark_degraded(&mut manifest, requested_shards, &shard_layout);
                }
                return Err(error);
            }
            integrity = manifest::current_integrity(&manifest, requested_shards)?;
        }
        let schema_generation = catalog.logical().schema_generation();

        let ready_layout = match manifest::reconcile_shard_layout(
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
        ) {
            Ok(layout) => layout,
            Err(error) => {
                if error.kind() == EngineErrorKind::DataCorruption {
                    schema_coordination.mark_degraded();
                    let _ = manifest::mark_degraded(&mut manifest, requested_shards, &shard_layout);
                }
                return Err(error);
            }
        };

        let storage = Self {
            root,
            catalog,
            shard_layout: ready_layout,
            schema_coordination,
        };
        let verification = storage.verify_current_schema_consensus(integrity);
        let observed_digest = match verification {
            Ok(digest) => digest,
            Err(error) => {
                if error.kind() == EngineErrorKind::DataCorruption {
                    storage.mark_schema_degraded();
                    let _ = manifest::mark_degraded(
                        &mut manifest,
                        requested_shards,
                        &storage.shard_layout,
                    );
                }
                return Err(error);
            }
        };
        let sealed = match manifest::seal_verified_schema(
            &mut manifest,
            requested_shards,
            observed_digest,
        ) {
            Ok(sealed) => sealed,
            Err(error) => {
                if error.kind() == EngineErrorKind::DataCorruption {
                    storage.mark_schema_degraded();
                    let _ = manifest::mark_degraded(
                        &mut manifest,
                        requested_shards,
                        &storage.shard_layout,
                    );
                }
                return Err(error);
            }
        };
        storage.schema_coordination.publish_schema_digests(
            sealed.committed_schema_digest(),
            sealed.target_schema_digest(),
        )?;
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

    pub(crate) fn routing_provenance(&self) -> (u32, u32, u32, u64) {
        let routing = self.catalog.routing();
        (
            routing.hash_version(),
            routing.key_encoding_version(),
            routing.bucket_algorithm_version(),
            routing.map_generation(),
        )
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

    pub(crate) fn mark_schema_degraded(&self) {
        self.schema_coordination.mark_degraded();
    }

    /// Fail closed immediately and make one zero-busy-wait best-effort attempt
    /// to persist terminal `Degraded` state in an already-trusted manifest.
    pub(crate) fn record_schema_degraded(&self) {
        self.mark_schema_degraded();
        if let Ok(mut manifest_connection) =
            open_existing_manifest(&self.root.join("manifest.sqlite"))
        {
            let configured = manifest_connection
                .busy_timeout(std::time::Duration::ZERO)
                .map_err(sqlite_error::storage)
                .and_then(|_| configure_manifest_connection_after_busy_setup(&manifest_connection));
            if configured.is_ok() {
                let _ = manifest::mark_degraded(
                    &mut manifest_connection,
                    self.catalog.routing().shard_count(),
                    &self.shard_layout,
                );
            }
        }
    }

    pub(crate) fn fail_closed_on_corruption<T>(&self, result: EngineResult<T>) -> EngineResult<T> {
        if result
            .as_ref()
            .is_err_and(|error| error.kind() == EngineErrorKind::DataCorruption)
        {
            self.record_schema_degraded();
        }
        result
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
        let result = (|| {
            self.ensure_shard_in_range(shard)?;
            let path = self.shard_path(shard);
            let connection = shard::open_existing(
                &path,
                shard,
                self.catalog.logical().schema_generation(),
                &self.shard_layout,
            )?;
            let expected_digest = self.schema_coordination.committed_schema_digest()?;
            shard::verify_schema_digest(
                &connection,
                self.catalog.logical().schema_generation(),
                &expected_digest,
            )?;
            attach_storage_authorizer(&connection)?;
            Ok(connection)
        })();
        self.fail_closed_on_corruption(result)
    }

    fn verify_current_schema_consensus(
        &self,
        integrity: manifest::ManifestIntegrity,
    ) -> EngineResult<[u8; 32]> {
        if matches!(
            integrity.state(),
            manifest::DatabaseIntegrityState::Migrating
        ) {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "database remained in migrating state after startup recovery",
            ));
        }
        let generation = self.catalog.logical().schema_generation();
        let trusted = integrity.committed_schema_digest();
        let mut consensus = None;
        for shard_id in 0..self.shard_count() {
            let path = self.shard_path(shard_id);
            let connection = shard::open_existing(&path, shard_id, generation, &self.shard_layout)?;
            let observed = shard::calculate_schema_digest(&connection, generation)?;
            if trusted.is_some_and(|expected| expected != observed) {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    "a shard application schema does not match the trusted checksum",
                ));
            }
            if consensus.is_some_and(|expected| expected != observed) {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    "shard application schemas do not have one consistent fingerprint",
                ));
            }
            consensus = Some(observed);
        }
        consensus.ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                "schema verification requires at least one configured shard",
            )
        })
    }

    fn open_unconfigured_shard(&self, shard: u16) -> EngineResult<Connection> {
        self.ensure_shard_in_range(shard)?;
        shard::open_required_file(&self.shard_path(shard))
    }

    fn validate_unconfigured_shard(&self, connection: &Connection, shard: u16) -> EngineResult<()> {
        let result = (|| {
            self.ensure_shard_in_range(shard)?;
            shard::validate_open_connection(
                connection,
                &self.shard_path(shard),
                shard,
                self.catalog.logical().schema_generation(),
                &self.shard_layout,
            )?;
            let expected_digest = self.schema_coordination.committed_schema_digest()?;
            shard::verify_schema_digest(
                connection,
                self.catalog.logical().schema_generation(),
                &expected_digest,
            )
        })();
        self.fail_closed_on_corruption(result)
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
    configure_manifest_connection_after_busy_setup(connection)
}

fn configure_manifest_connection_after_busy_setup(connection: &Connection) -> EngineResult<()> {
    connection
        .pragma_update(None, "cell_size_check", "ON")
        .map_err(sqlite_error::storage)?;
    let cell_size_check = connection
        .pragma_query_value(None, "cell_size_check", |row| row.get::<_, i64>(0))
        .map_err(sqlite_error::storage)?;
    if cell_size_check != 1 {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "SQLite did not enable manifest b-tree cell-size checking",
        ));
    }
    validate_manifest_integrity_check(connection)?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(sqlite_error::storage)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn validate_manifest_integrity_check(connection: &Connection) -> EngineResult<()> {
    let mut statement = connection
        .prepare("PRAGMA main.integrity_check(1)")
        .map_err(sqlite_error::storage)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)?;
    if rows == ["ok"] {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "SQLite manifest integrity verification failed",
        ))
    }
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

    #[test]
    fn manifest_configuration_enables_and_reads_back_cell_size_checks() {
        let connection = Connection::open_in_memory().unwrap();
        assert_eq!(
            connection
                .pragma_query_value(None, "cell_size_check", |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        configure_manifest_connection(&connection).unwrap();
        assert_eq!(
            connection
                .pragma_query_value(None, "cell_size_check", |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
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
    fn canonical_and_symlink_roots_share_sticky_degraded_admission() {
        let temp = tempfile::tempdir().unwrap();
        let original = Storage::open(temp.path(), 2).unwrap();
        let alias_parent = tempfile::tempdir().unwrap();
        let alias_path = alias_parent.path().join("database-alias");
        std::os::unix::fs::symlink(temp.path(), &alias_path).unwrap();
        let alias = Storage::open(&alias_path, 2).unwrap();

        alias.mark_schema_degraded();

        for storage in [&original, &alias] {
            assert_eq!(
                storage.schema_gate_snapshot(),
                SchemaGateSnapshot {
                    state: SchemaGateState::Degraded,
                    active_operations: 0,
                }
            );
            assert_eq!(
                storage.enter_schema_operation().unwrap_err().kind(),
                EngineErrorKind::DataCorruption
            );
            assert_eq!(
                storage.begin_schema_migration().unwrap_err().kind(),
                EngineErrorKind::DataCorruption
            );
        }

        assert_eq!(
            Storage::open(temp.path(), 2).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn runtime_schema_drift_is_sticky_and_persisted_until_a_complete_restore() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let shard_path = temp.path().join("shards/0000.sqlite");
        Connection::open(&shard_path)
            .unwrap()
            .execute_batch("CREATE TABLE unexpected_drift(id INTEGER PRIMARY KEY)")
            .unwrap();

        let detected = storage.open_shard(0).unwrap_err();
        assert_eq!(detected.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(
            storage.schema_gate_snapshot().state,
            SchemaGateState::Degraded
        );
        assert_eq!(
            storage.enter_schema_operation().unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
        assert_eq!(
            Connection::open(temp.path().join("manifest.sqlite"))
                .unwrap()
                .query_row(
                    "SELECT database_state FROM briskdb_integrity WHERE singleton = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            4
        );

        Connection::open(&shard_path)
            .unwrap()
            .execute_batch("DROP TABLE unexpected_drift")
            .unwrap();
        assert_eq!(
            storage.enter_schema_operation().unwrap_err().kind(),
            EngineErrorKind::DataCorruption,
            "repair without releasing the live canonical-root coordination must stay degraded"
        );
        drop(storage);

        let restart = Storage::open(temp.path(), 2).unwrap_err();
        assert_eq!(restart.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(
            Connection::open(temp.path().join("manifest.sqlite"))
                .unwrap()
                .query_row(
                    "SELECT database_state FROM briskdb_integrity WHERE singleton = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            4
        );
    }

    #[test]
    fn terminal_degraded_startup_does_not_change_manifest_journal_mode_or_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let layout = storage.shard_layout;
        drop(storage);
        let manifest_path = temp.path().join("manifest.sqlite");
        let mut manifest_connection = Connection::open(&manifest_path).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        manifest::mark_degraded(&mut manifest_connection, 2, &layout).unwrap();
        manifest_connection
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .unwrap();
        assert_eq!(
            manifest_connection
                .pragma_update_and_check(None, "journal_mode", "DELETE", |row| {
                    row.get::<_, String>(0)
                })
                .unwrap()
                .to_ascii_lowercase(),
            "delete"
        );
        drop(manifest_connection);
        let before = fs::read(&manifest_path).unwrap();

        let error = Storage::open(temp.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(fs::read(&manifest_path).unwrap(), before);
        let observed_mode = Connection::open(&manifest_path)
            .unwrap()
            .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
            .unwrap();
        assert_eq!(observed_mode.to_ascii_lowercase(), "delete");
        assert!(!temp.path().join("manifest.sqlite-wal").exists());
    }

    #[test]
    fn failed_emergency_marker_write_still_leaves_live_admission_degraded() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let manifest_path = temp.path().join("manifest.sqlite");
        let owner = Connection::open(&manifest_path).unwrap();
        owner
            .execute_batch(
                "PRAGMA wal_checkpoint(TRUNCATE);
                 PRAGMA journal_mode = DELETE;
                 BEGIN EXCLUSIVE;",
            )
            .unwrap();

        let started = Instant::now();
        storage.record_schema_degraded();
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "best-effort degradation persistence must not inherit the normal busy timeout"
        );
        assert_eq!(
            storage.enter_schema_operation().unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
        owner.execute_batch("ROLLBACK").unwrap();
        assert_eq!(
            Connection::open(manifest_path)
                .unwrap()
                .query_row(
                    "SELECT database_state FROM briskdb_integrity WHERE singleton = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            2,
            "the blocked emergency write must not claim it committed Degraded"
        );
    }

    #[test]
    fn identical_offline_schema_drift_on_every_shard_is_not_rebaselined() {
        let temp = tempfile::tempdir().unwrap();
        drop(Storage::open(temp.path(), 2).unwrap());
        for shard_id in 0..2 {
            Connection::open(temp.path().join(format!("shards/{shard_id:04}.sqlite")))
                .unwrap()
                .execute_batch("CREATE TABLE coordinated_drift(id INTEGER PRIMARY KEY)")
                .unwrap();
        }

        let error = Storage::open(temp.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(
            Connection::open(temp.path().join("manifest.sqlite"))
                .unwrap()
                .query_row(
                    "SELECT database_state FROM briskdb_integrity WHERE singleton = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            4
        );
    }

    #[test]
    fn a_manifest_root_mismatch_degrades_live_aliases_without_signing_the_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let manifest_path = temp.path().join("manifest.sqlite");
        let manifest = Connection::open(&manifest_path).unwrap();
        let trusted_root: Vec<u8> = manifest
            .query_row(
                "SELECT manifest_digest FROM briskdb_integrity WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        manifest
            .execute(
                "UPDATE briskdb_virtual_buckets
                 SET physical_shard_id = 1 - physical_shard_id
                 WHERE bucket_id = 0",
                [],
            )
            .unwrap();
        drop(manifest);

        let error = Storage::open(temp.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(
            storage.enter_schema_operation().unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
        let manifest = Connection::open(manifest_path).unwrap();
        assert_eq!(
            manifest
                .query_row(
                    "SELECT manifest_digest FROM briskdb_integrity WHERE singleton = 1",
                    [],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .unwrap(),
            trusted_root
        );
        assert_eq!(
            manifest
                .query_row(
                    "SELECT database_state FROM briskdb_integrity WHERE singleton = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            2,
            "an untrusted manifest must never write or reseal its own degraded marker"
        );
    }

    #[test]
    fn every_v6_active_prefix_finishes_before_v7_checksum_bootstrap() {
        const SQL: &str = "CREATE TABLE recovered_v6(id INTEGER PRIMARY KEY)";
        for (target_shards, acknowledged) in [(0_u16, 0_u16), (1, 0), (1, 1), (2, 1), (2, 2)] {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            let layout = storage.shard_layout;
            drop(storage);

            let manifest_path = temp.path().join("manifest.sqlite");
            let mut manifest = Connection::open(&manifest_path).unwrap();
            manifest
                .execute_batch(
                    "BEGIN IMMEDIATE;
                     DROP TABLE briskdb_integrity;
                     DROP TABLE briskdb_metadata;
                     CREATE TABLE briskdb_metadata (
                         requires_manifest_version INTEGER NOT NULL
                             CHECK (requires_manifest_version >= 6)
                     ) STRICT;
                     INSERT INTO briskdb_metadata VALUES (6);
                     PRAGMA user_version = 6;
                     COMMIT;",
                )
                .unwrap();
            let mut migration = manifest::begin_schema_migration(&mut manifest, 2, 0, SQL).unwrap();
            for shard_id in 0..target_shards {
                shard::apply_schema_migration(
                    &temp.path().join(format!("shards/{shard_id:04}.sqlite")),
                    shard_id,
                    0,
                    1,
                    &layout,
                    SQL,
                )
                .unwrap();
            }
            while migration.next_shard() < acknowledged {
                let next = migration.next_shard() + 1;
                migration =
                    manifest::advance_schema_migration(&mut manifest, 2, &migration, next).unwrap();
            }
            drop(manifest);

            let recovered = Storage::open(temp.path(), 2).unwrap();
            assert_eq!(recovered.current_schema_generation(), 1);
            for shard_id in 0..2 {
                let connection = recovered.open_shard(shard_id).unwrap();
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT COUNT(*) FROM sqlite_schema
                             WHERE type = 'table' AND name = 'recovered_v6'",
                            [],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    1
                );
            }
            let manifest = Connection::open(manifest_path).unwrap();
            assert_eq!(
                manifest
                    .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                    .unwrap(),
                i64::from(manifest::CURRENT_SCHEMA_VERSION)
            );
            assert_eq!(
                manifest
                    .query_row(
                        "SELECT database_state,
                                length(committed_schema_digest),
                                target_schema_digest IS NULL
                         FROM briskdb_integrity",
                        [],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, bool>(2)?,
                            ))
                        },
                    )
                    .unwrap(),
                (2, 32, true),
                "shape targets={target_shards} acknowledged={acknowledged}"
            );
        }
    }

    #[test]
    fn file_backed_v7_verifying_interruption_reopens_to_ready_without_metadata_or_schema_drift() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let catalog_before = storage.catalog.as_ref().clone();
        let layout_before = storage.shard_layout;
        let schema_generation = storage.current_schema_generation();
        let schema_digests_before: [[u8; 32]; 2] = std::array::from_fn(|shard_id| {
            let connection = storage
                .open_shard(u16::try_from(shard_id).unwrap())
                .unwrap();
            shard::calculate_schema_digest(&connection, schema_generation).unwrap()
        });
        assert_eq!(schema_digests_before[0], schema_digests_before[1]);
        drop(storage);

        let manifest_path = temp.path().join("manifest.sqlite");
        Connection::open(&manifest_path)
            .unwrap()
            .execute_batch(
                "BEGIN IMMEDIATE;
                 DROP TABLE briskdb_integrity;
                 DROP TABLE briskdb_metadata;
                 CREATE TABLE briskdb_metadata (
                     requires_manifest_version INTEGER NOT NULL
                         CHECK (requires_manifest_version >= 6)
                 ) STRICT;
                 INSERT INTO briskdb_metadata VALUES (6);
                 PRAGMA user_version = 6;
                 COMMIT;",
            )
            .unwrap();

        let mut manifest_connection = Connection::open(&manifest_path).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        let verifying =
            manifest::load_or_create_manifest_with_fresh_layout(&mut manifest_connection, 2, false)
                .unwrap();
        assert!(manifest_connection.is_autocommit());
        let (verifying_catalog, verifying_layout, active_migration, verifying_integrity) =
            verifying.into_parts_with_migration();
        assert_eq!(
            verifying_catalog, catalog_before,
            "the committed v7 Verifying upgrade changed catalog or routing metadata"
        );
        assert_eq!(verifying_layout, layout_before);
        assert!(active_migration.is_none());
        assert_eq!(
            verifying_integrity.state(),
            manifest::DatabaseIntegrityState::Verifying
        );
        assert_eq!(verifying_integrity.committed_schema_digest(), None);
        assert_eq!(verifying_integrity.target_schema_digest(), None);
        assert_eq!(
            manifest::current_integrity(&manifest_connection, 2)
                .unwrap()
                .state(),
            manifest::DatabaseIntegrityState::Verifying,
            "the file-backed v7 upgrade must be durably valid before restart verification"
        );
        drop(manifest_connection);

        let reopened = Storage::open(temp.path(), 2).unwrap();
        assert_eq!(
            reopened.catalog.as_ref(),
            &catalog_before,
            "restart verification changed catalog or routing metadata"
        );
        assert_eq!(reopened.shard_layout, layout_before);
        assert_eq!(reopened.current_schema_generation(), schema_generation);
        let schema_digests_after: [[u8; 32]; 2] = std::array::from_fn(|shard_id| {
            let connection = reopened
                .open_shard(u16::try_from(shard_id).unwrap())
                .unwrap();
            shard::calculate_schema_digest(&connection, schema_generation).unwrap()
        });
        assert_eq!(
            schema_digests_after, schema_digests_before,
            "restart verification changed an application schema"
        );

        let manifest_connection = Connection::open(manifest_path).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        let ready = manifest::current_integrity(&manifest_connection, 2).unwrap();
        assert_eq!(ready.state(), manifest::DatabaseIntegrityState::Ready);
        assert_eq!(
            ready.committed_schema_digest(),
            Some(schema_digests_before[0])
        );
        assert_eq!(ready.target_schema_digest(), None);
        assert_eq!(
            manifest_connection
                .query_row(
                    "SELECT COUNT(*) FROM briskdb_schema_migrations",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn divergent_v6_bootstrap_persists_terminal_degraded_without_a_new_baseline() {
        let temp = tempfile::tempdir().unwrap();
        drop(Storage::open(temp.path(), 2).unwrap());

        let manifest_path = temp.path().join("manifest.sqlite");
        Connection::open(&manifest_path)
            .unwrap()
            .execute_batch(
                "BEGIN IMMEDIATE;
                 DROP TABLE briskdb_integrity;
                 DROP TABLE briskdb_metadata;
                 CREATE TABLE briskdb_metadata (
                     requires_manifest_version INTEGER NOT NULL
                         CHECK (requires_manifest_version >= 6)
                 ) STRICT;
                 INSERT INTO briskdb_metadata VALUES (6);
                 PRAGMA user_version = 6;
                 COMMIT;",
            )
            .unwrap();
        Connection::open(temp.path().join("shards/0000.sqlite"))
            .unwrap()
            .execute_batch("CREATE TABLE divergent_bootstrap(id INTEGER PRIMARY KEY)")
            .unwrap();

        let first = Storage::open(temp.path(), 2).unwrap_err();
        assert_eq!(first.kind(), EngineErrorKind::DataCorruption);
        let mut manifest_connection = Connection::open(&manifest_path).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        let degraded = manifest::current_integrity(&manifest_connection, 2).unwrap();
        assert_eq!(degraded.state(), manifest::DatabaseIntegrityState::Degraded);
        assert_eq!(degraded.committed_schema_digest(), None);
        drop(manifest_connection);

        Connection::open(temp.path().join("shards/0001.sqlite"))
            .unwrap()
            .execute_batch("CREATE TABLE divergent_bootstrap(id INTEGER PRIMARY KEY)")
            .unwrap();
        let second = Storage::open(temp.path(), 2).unwrap_err();
        assert_eq!(second.kind(), EngineErrorKind::DataCorruption);

        manifest_connection = Connection::open(manifest_path).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        let still_degraded = manifest::current_integrity(&manifest_connection, 2).unwrap();
        assert_eq!(
            still_degraded.state(),
            manifest::DatabaseIntegrityState::Degraded
        );
        assert_eq!(still_degraded.committed_schema_digest(), None);
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
                "SQLite manifest integrity verification failed",
                "invalid shard count {invalid}"
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
