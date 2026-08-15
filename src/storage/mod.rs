//! SQLite file layout, versioned manifest management, and connection configuration.

mod global_index;
mod global_index_async;
mod hilo;
mod index_outbox;
mod manifest;
mod migration;
mod process_lock;
mod schema_gate;
mod shard;
#[cfg(feature = "experimental-vtab")]
#[allow(dead_code)]
mod sharded_vtab;
#[cfg(feature = "experimental-vtab")]
pub(crate) use sharded_vtab::{RegistrySchemaCache, WriteCoordinator};

pub(crate) mod pool;
pub(crate) use pool::{ConnectionOwner, ConnectionPools, PooledConnection};

use std::{
    collections::{BTreeSet, HashMap},
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, TransactionBehavior,
    hooks::{AuthAction, AuthContext, Authorization},
};

#[cfg(test)]
pub(crate) use migration::SchemaMigrationCoordinatorPoint;
use schema_gate::SchemaMigrationGuard as LocalSchemaMigrationGuard;
pub(crate) use schema_gate::SchemaOperationGuard;
#[cfg(test)]
pub(crate) use schema_gate::{SchemaGateSnapshot, SchemaGateState};

pub use crate::core::Database;
use crate::core::TableId;
use crate::{
    core::{
        Catalog, CatalogSnapshot, EngineError, EngineErrorKind, EngineResult, GeneratedIdPolicy,
        GeneratedTableDdlReceipt, GlobalIndexAsyncOptions, GlobalIndexAsyncProcessReport,
        GlobalIndexAsyncStatus, GlobalIndexBuildReport, GlobalIndexDeclaration, GlobalIndexId,
        GlobalIndexKeyType, GlobalIndexLifecycle, GlobalIndexMetadata, GlobalIndexOutboxBatch,
        GlobalIndexOutboxCursor, GlobalIndexOutboxPruneReport, GlobalIndexOutboxShardStatus,
        GlobalIndexRepairReport, GlobalIndexValidationMode, GlobalIndexValidationOptions,
        GlobalIndexValidationReport, GlobalOperationId, GlobalUniqueMutation,
        GlobalUniqueReservation, GlobalValueLease, IndexKeyValue, MAX_TABLES, ShardKeyType,
        TableDeclaration, TablePlacement,
        generated_id::{
            NATIVE_RANGE_V1_FORMAT_MARKER, native_range_v1_sequence_ceiling,
            native_range_v1_sequence_floor,
        },
    },
    sql::SqlDialect,
    sqlite_error,
};

use crate::core::AllocationOwnerMap;

pub(crate) const CONNECTION_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
#[cfg(feature = "sqlite-import")]
pub(crate) const MAX_SCHEMA_MIGRATION_SQL_BYTES: usize = manifest::MAX_SCHEMA_MIGRATION_SQL_BYTES;

#[derive(Debug)]
struct RootSchemaCoordination {
    gate: schema_gate::SchemaGate,
    process_lease: process_lock::RootProcessLease,
    catalogs: Mutex<Vec<Weak<CatalogSnapshot>>>,
    schema_digests: Mutex<RuntimeSchemaDigests>,
    #[cfg_attr(not(feature = "experimental-vtab"), allow(dead_code))]
    hilo_allocator: hilo::HiloAllocator,
}

struct CatalogReplacementGuard<'a> {
    catalogs: MutexGuard<'a, Vec<Weak<CatalogSnapshot>>>,
}

impl CatalogReplacementGuard<'_> {
    fn publish(
        mut self,
        current: &Arc<CatalogSnapshot>,
        replacement: CatalogSnapshot,
    ) -> EngineResult<Arc<CatalogSnapshot>> {
        if current.routing() != replacement.routing()
            || current.logical().schema_generation() != replacement.logical().schema_generation()
            || current.allocation_owners() != replacement.allocation_owners()
        {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "table registration changed routing, allocation-owner, or schema-generation metadata",
            ));
        }
        let replacement = Arc::new(replacement);
        self.catalogs.clear();
        self.catalogs.push(Arc::downgrade(&replacement));
        Ok(replacement)
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct RuntimeSchemaDigests {
    committed: Option<[u8; 32]>,
    target: Option<[u8; 32]>,
}

impl RootSchemaCoordination {
    fn new(root: &Path) -> EngineResult<Self> {
        Ok(Self {
            gate: schema_gate::SchemaGate::new(),
            process_lease: process_lock::RootProcessLease::acquire(root)?,
            catalogs: Mutex::new(Vec::new()),
            schema_digests: Mutex::new(RuntimeSchemaDigests::default()),
            hilo_allocator: hilo::HiloAllocator::new()?,
        })
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
        let mut catalogs = self.catalogs.lock().map_err(|error| {
            EngineError::new(
                EngineErrorKind::Internal,
                format!("root schema catalog coordination is poisoned: {error}"),
            )
        })?;
        catalogs.retain(|catalog| catalog.strong_count() != 0);
        if catalogs
            .iter()
            .filter_map(Weak::upgrade)
            .any(|live| !immutable_catalog_metadata_matches(&live, &loaded))
        {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "the committed catalog conflicts with a live database handle; close stale handles before reopening",
            ));
        }
        let loaded = Arc::new(loaded);
        catalogs.push(Arc::downgrade(&loaded));
        Ok(loaded)
    }

    fn reserve_catalog_replacement<'a>(
        &'a self,
        current: &Arc<CatalogSnapshot>,
    ) -> EngineResult<CatalogReplacementGuard<'a>> {
        if Arc::strong_count(current) != 1 {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "catalog mutation requires one exclusively owned database handle",
            ));
        }
        let mut catalogs = self.catalogs.lock().map_err(|error| {
            EngineError::new(
                EngineErrorKind::Internal,
                format!("root schema catalog coordination is poisoned: {error}"),
            )
        })?;
        catalogs.retain(|catalog| catalog.strong_count() != 0);
        let current = Arc::downgrade(current);
        let mut live = catalogs
            .iter()
            .filter(|catalog| catalog.strong_count() != 0);
        if live.next().is_none_or(|catalog| !catalog.ptr_eq(&current)) || live.next().is_some() {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "catalog mutation requires every other database handle to be closed",
            ));
        }
        Ok(CatalogReplacementGuard { catalogs })
    }

    fn ensure_exclusive_catalog(&self, current: &Arc<CatalogSnapshot>) -> EngineResult<()> {
        // The schema gate prevents a new handle from completing startup while
        // the caller's migration guard is live. Dropping this reservation is
        // therefore safe and avoids holding the catalog mutex while physical
        // migration generation is published through that same coordinator.
        drop(self.reserve_catalog_replacement(current)?);
        Ok(())
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
            .any(|catalog| !immutable_catalog_metadata_matches(catalog, validated))
        {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "the validated catalog conflicts with a live database handle",
            ));
        }

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

/// Compare the routing and logical metadata that cannot change while a handle
/// remains live. Schema generation is deliberately excluded: crash recovery
/// may publish one validated generation transition into existing snapshots.
fn immutable_catalog_metadata_matches(left: &CatalogSnapshot, right: &CatalogSnapshot) -> bool {
    let left_logical = left.logical();
    let right_logical = right.logical();
    left.routing() == right.routing()
        && left.allocation_owners() == right.allocation_owners()
        && left.active_native_id_table_ids() == right.active_native_id_table_ids()
        && left.active_hilo_id_table_ids() == right.active_hilo_id_table_ids()
        && left_logical.identifier_encoding_version() == right_logical.identifier_encoding_version()
        && left_logical.default_database().id() == right_logical.default_database().id()
        && left_logical.logical_databases() == right_logical.logical_databases()
        && left_logical.tables() == right_logical.tables()
        && left_logical.global_indexes() == right_logical.global_indexes()
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
    let coordination = Arc::new(RootSchemaCoordination::new(root)?);
    registry.insert(root.to_path_buf(), Arc::downgrade(&coordination));
    Ok(coordination)
}

fn begin_startup_coordination(
    coordination: &RootSchemaCoordination,
) -> EngineResult<LocalSchemaMigrationGuard> {
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

/// In-process schema exclusion composed with optional sole-process ownership.
#[derive(Debug)]
pub(crate) struct SchemaMigrationGuard {
    local: LocalSchemaMigrationGuard,
    process: Option<process_lock::RootMutationGuard>,
}

impl SchemaMigrationGuard {
    fn new(local: LocalSchemaMigrationGuard) -> Self {
        Self {
            local,
            process: None,
        }
    }

    pub(crate) async fn wait_for_quiescence(&self) {
        self.local.wait_for_quiescence().await;
    }

    pub(crate) fn wait_for_quiescence_blocking(&self) {
        self.local.wait_for_quiescence_blocking();
    }

    pub(crate) fn mark_pending_on_drop(&mut self) {
        self.local.mark_pending_on_drop();
    }

    fn acquire_process_ownership(
        &mut self,
        lease: &process_lock::RootProcessLease,
    ) -> EngineResult<()> {
        if self.process.is_some() {
            return Ok(());
        }
        // Callers exclude new local operations and drain admitted work first.
        // The nonblocking process upgrade is last, so competing processes do
        // not wait on one another while holding another cross-process lock.
        self.process = Some(lease.try_acquire_exclusive()?);
        Ok(())
    }

    pub(crate) fn publish_ready(mut self) -> EngineResult<()> {
        self.local.publish_ready()?;
        if let Some(process) = self.process.take() {
            process.downgrade()?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn publish_pending(mut self) -> EngineResult<()> {
        self.local.publish_pending()?;
        if let Some(process) = self.process.take() {
            process.downgrade()?;
        }
        Ok(())
    }
}

#[cfg(test)]
fn abort_generated_table_ddl_at_test_boundary(boundary: &str) {
    if std::env::var("BRISKDB_GENERATED_DDL_ABORT_POINT").as_deref() == Ok(boundary) {
        std::process::abort();
    }
}

#[cfg(test)]
fn abort_global_index_recovery_at_test_boundary(boundary: &str) {
    if std::env::var("BRISKDB_GLOBAL_INDEX_RECOVERY_ABORT_POINT").as_deref() == Ok(boundary) {
        std::process::abort();
    }
}

#[cfg(not(test))]
fn abort_global_index_recovery_at_test_boundary(_boundary: &str) {}

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

#[derive(Debug)]
pub(crate) struct GlobalWriteReservationGuard {
    operation_id: GlobalOperationId,
    _lease: process_lock::GlobalWriteOperationLease,
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
        // Lock order is startup serialization, process-local schema admission,
        // then (only for initialization/upgrade/recovery) sole-process root
        // ownership. A queued opener cannot add a shared lease while recovery
        // is deciding whether its exclusive upgrade is safe.
        let _process_startup =
            process_lock::RootStartupGuard::acquire(&root, CONNECTION_BUSY_TIMEOUT)?;
        let schema_coordination = root_schema_coordination(&root)?;
        let mut startup = begin_startup_coordination(&schema_coordination)?;
        startup.wait_for_quiescence_blocking();
        let manifest_path = root.join("manifest.sqlite");
        let requires_exclusive = startup_requires_exclusive_ownership(
            &manifest_path,
            requested_shards,
        )
        .and_then(|manifest_requires_exclusive| {
            global_index::startup_requires_upgrade(&root).map(|global_index_requires_exclusive| {
                manifest_requires_exclusive || global_index_requires_exclusive
            })
        });
        let requires_exclusive = match requires_exclusive {
            Ok(requires_exclusive) => requires_exclusive,
            Err(error) => {
                if error.kind() == EngineErrorKind::DataCorruption {
                    schema_coordination.mark_degraded();
                }
                return Err(error);
            }
        };
        let process_mutation = if requires_exclusive {
            Some(schema_coordination.process_lease.try_acquire_exclusive()?)
        } else {
            None
        };
        if let Err(error) = global_index::upgrade_if_needed(&root) {
            if error.kind() == EngineErrorKind::DataCorruption {
                schema_coordination.mark_degraded();
            }
            return Err(error);
        }
        let shards_dir = root.join("shards");
        let fresh_layout_allowed = physical_layout_is_empty(&shards_dir)?;
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
        let (
            catalog,
            shard_layout,
            active_migration,
            mut integrity,
            active_native_id_table_ids,
            active_hilo_id_table_ids,
            mut active_table_provisioning,
            generated_table_ddl,
        ) = loaded.into_parts_with_recovery();
        let catalog = catalog.with_active_native_id_table_ids(active_native_id_table_ids);
        let catalog = catalog.with_active_hilo_id_table_ids(active_hilo_id_table_ids);
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
            let recovery: EngineResult<()> = (|| {
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

        let mut storage = Self {
            root,
            catalog,
            shard_layout: ready_layout,
            schema_coordination,
        };
        if let Some(mut ddl) = generated_table_ddl {
            startup.mark_pending_on_drop();
            let recovery: EngineResult<()> = (|| {
                if ddl.lifecycle() == manifest::GeneratedTableDdlLifecycle::ApplyingPhysical {
                    ddl = manifest::mark_generated_table_ddl_provisioning(
                        &mut manifest,
                        requested_shards,
                        &ddl,
                        || {},
                    )?;
                }
                if ddl.lifecycle() == manifest::GeneratedTableDdlLifecycle::Complete {
                    if active_table_provisioning.is_some() {
                        return Err(EngineError::new(
                            EngineErrorKind::DataCorruption,
                            "complete generated-table DDL retains active table provisioning",
                        ));
                    }
                    return Ok(());
                }
                if ddl.lifecycle() != manifest::GeneratedTableDdlLifecycle::Provisioning {
                    return Err(EngineError::new(
                        EngineErrorKind::DataCorruption,
                        "generated-table DDL recovery found an unsupported lifecycle",
                    ));
                }
                let mut provisioning = match active_table_provisioning.take() {
                    Some(provisioning) => provisioning,
                    None => {
                        let committed_schema_digest =
                            storage.schema_coordination.committed_schema_digest()?;
                        match manifest::begin_native_table_provisioning(
                            &mut manifest,
                            requested_shards,
                            vec![ddl.declaration().clone()],
                            committed_schema_digest,
                            || {},
                        )? {
                            manifest::NativeTableProvisioningClassification::Active(
                                provisioning,
                            ) => provisioning,
                            manifest::NativeTableProvisioningClassification::Complete => {
                                return Err(EngineError::new(
                                    EngineErrorKind::DataCorruption,
                                    "generated-table DDL catalog completed outside its atomic bridge finalization",
                                ));
                            }
                            manifest::NativeTableProvisioningClassification::Absent => {
                                return Err(EngineError::new(
                                    EngineErrorKind::Internal,
                                    "generated-table DDL recovery did not create its provisioning journal",
                                ));
                            }
                        }
                    }
                };
                storage.validate_empty_table_declarations(provisioning.declarations())?;
                while provisioning.next_shard() < provisioning.shard_count() {
                    let shard_id = provisioning.next_shard();
                    storage.seed_native_range_v1_sequences_on_shard(
                        provisioning.declarations(),
                        shard_id,
                    )?;
                    provisioning = manifest::advance_native_table_provisioning(
                        &mut manifest,
                        requested_shards,
                        &provisioning,
                        shard_id + 1,
                    )?;
                }
                let replacement_guard = storage
                    .schema_coordination
                    .reserve_catalog_replacement(&storage.catalog)?;
                let (replacement, _completed) =
                    manifest::finalize_generated_table_ddl_provisioning(
                        &mut manifest,
                        requested_shards,
                        &ddl,
                        &provisioning,
                        || {},
                    )?;
                storage.catalog = replacement_guard.publish(&storage.catalog, replacement)?;
                Ok::<(), EngineError>(())
            })();
            if let Err(error) = recovery {
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
        } else if let Some(mut provisioning) = active_table_provisioning {
            startup.mark_pending_on_drop();
            let recovery: EngineResult<()> = (|| {
                storage.validate_empty_table_declarations(provisioning.declarations())?;
                while provisioning.next_shard() < provisioning.shard_count() {
                    let shard_id = provisioning.next_shard();
                    storage.seed_native_range_v1_sequences_on_shard(
                        provisioning.declarations(),
                        shard_id,
                    )?;
                    provisioning = manifest::advance_native_table_provisioning(
                        &mut manifest,
                        requested_shards,
                        &provisioning,
                        shard_id + 1,
                    )?;
                }
                let replacement_guard = storage
                    .schema_coordination
                    .reserve_catalog_replacement(&storage.catalog)?;
                let replacement = manifest::finalize_native_table_provisioning(
                    &mut manifest,
                    requested_shards,
                    &provisioning,
                    || {},
                )?;
                storage.catalog = replacement_guard.publish(&storage.catalog, replacement)?;
                Ok::<(), EngineError>(())
            })();
            if let Err(error) = recovery {
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
        }
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
        if let Some(process_mutation) = process_mutation {
            process_mutation.downgrade()?;
        }
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

    pub(crate) fn allocation_owner_map(&self) -> Option<&AllocationOwnerMap> {
        self.catalog.allocation_owners()
    }

    #[cfg(any(feature = "experimental-vtab", test))]
    pub(crate) fn native_id_policy_is_active(&self, table_id: TableId) -> bool {
        self.catalog.native_id_policy_is_active(table_id)
    }

    #[cfg(any(feature = "experimental-vtab", test))]
    #[cfg_attr(not(feature = "experimental-vtab"), allow(dead_code))]
    pub(crate) fn generated_id_policy_is_active(&self, table_id: TableId) -> bool {
        self.catalog.native_id_policy_is_active(table_id)
            || self.catalog.hilo_id_policy_is_active(table_id)
    }

    #[cfg(any(feature = "experimental-vtab", test))]
    #[cfg_attr(not(feature = "experimental-vtab"), allow(dead_code))]
    pub(crate) fn allocate_hilo_v1(&self, table_id: TableId) -> EngineResult<hilo::HiloAllocation> {
        if !self.catalog.hilo_id_policy_is_active(table_id) {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!("hilo_v1 generation is not active for table {table_id}"),
            ));
        }
        self.schema_coordination
            .hilo_allocator
            .allocate(table_id, |owner_id| {
                let mut manifest_connection =
                    open_existing_manifest(&self.root.join("manifest.sqlite"))?;
                configure_manifest_connection(&manifest_connection)?;
                manifest::reserve_hilo_v1_block(
                    &mut manifest_connection,
                    self.shard_count(),
                    table_id,
                    owner_id,
                )
            })
    }

    #[cfg(any(feature = "experimental-vtab", test))]
    #[cfg_attr(not(feature = "experimental-vtab"), allow(dead_code))]
    pub(crate) fn hilo_owner_id(&self) -> [u8; 32] {
        self.schema_coordination.hilo_allocator.owner_id()
    }

    pub(crate) fn apply_generated_table_ddl(
        &mut self,
        source_dialect: SqlDialect,
        source_sql: &str,
        physical_sql: &str,
        declaration: TableDeclaration,
    ) -> EngineResult<GeneratedTableDdlReceipt> {
        let result = self.apply_generated_table_ddl_inner(
            source_dialect,
            source_sql,
            physical_sql,
            declaration,
        );
        self.fail_closed_on_corruption(result)
    }

    fn apply_generated_table_ddl_inner(
        &mut self,
        source_dialect: SqlDialect,
        source_sql: &str,
        physical_sql: &str,
        declaration: TableDeclaration,
    ) -> EngineResult<GeneratedTableDdlReceipt> {
        self.validate_table_declaration_request(std::slice::from_ref(&declaration))?;
        let mut migration =
            SchemaMigrationGuard::new(self.schema_coordination.gate.begin_new_migration()?);
        migration.wait_for_quiescence_blocking();
        migration.acquire_process_ownership(&self.schema_coordination.process_lease)?;
        let operation = (|| {
            self.schema_coordination
                .ensure_exclusive_catalog(&self.catalog)?;
            let manifest_path = self.root.join("manifest.sqlite");
            let mut manifest_connection = open_existing_manifest(&manifest_path)?;
            configure_manifest_connection(&manifest_connection)?;
            configure_journal_mode(&manifest_connection)?;

            let classification = manifest::classify_generated_table_ddl(
                &mut manifest_connection,
                self.shard_count(),
                source_dialect,
                source_sql,
                physical_sql,
                &declaration,
            )?;
            let mut ddl = match classification {
                manifest::GeneratedTableDdlClassification::Existing(existing)
                    if existing.lifecycle() == manifest::GeneratedTableDdlLifecycle::Complete =>
                {
                    let receipt = generated_table_ddl_receipt(&existing)?;
                    if !declarations_match_catalog(
                        self.catalog.logical(),
                        std::slice::from_ref(&declaration),
                    ) || !self.catalog.native_id_policy_is_active(receipt.table_id())
                    {
                        return Err(EngineError::new(
                            EngineErrorKind::FailedPrecondition,
                            "generated-table DDL is complete; reopen this stale database handle",
                        ));
                    }
                    return Ok(receipt);
                }
                manifest::GeneratedTableDdlClassification::Existing(existing) => {
                    if existing.lifecycle()
                        == manifest::GeneratedTableDdlLifecycle::ApplyingPhysical
                    {
                        self.apply_schema_migration(existing.physical_sql(), &mut migration, None)?;
                    }
                    existing
                }
                manifest::GeneratedTableDdlClassification::Absent => {
                    if !self.catalog.logical().tables().is_empty() {
                        return Err(EngineError::new(
                            EngineErrorKind::FailedPrecondition,
                            "generated-table DDL bridge currently requires an empty authoritative catalog",
                        ));
                    }
                    self.preflight_generated_table_ddl(physical_sql, &declaration)?;
                    migration::apply_generated_table_ddl_migration(
                        self,
                        source_dialect,
                        source_sql,
                        physical_sql,
                        declaration.clone(),
                        &mut migration,
                    )
                    .inspect(|_| {
                        #[cfg(test)]
                        abort_generated_table_ddl_at_test_boundary("physical-complete");
                    })?
                }
            };

            if ddl.lifecycle() == manifest::GeneratedTableDdlLifecycle::ApplyingPhysical {
                ddl = manifest::mark_generated_table_ddl_provisioning(
                    &mut manifest_connection,
                    self.shard_count(),
                    &ddl,
                    || {
                        migration.mark_pending_on_drop();
                        #[cfg(test)]
                        abort_generated_table_ddl_at_test_boundary("mark-before-commit");
                    },
                )?;
                #[cfg(test)]
                abort_generated_table_ddl_at_test_boundary("mark-after-commit");
            }
            if ddl.lifecycle() != manifest::GeneratedTableDdlLifecycle::Provisioning {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "generated-table DDL did not reach its provisioning phase",
                ));
            }

            self.validate_empty_table_declarations(std::slice::from_ref(&declaration))?;
            let committed_schema_digest = self.schema_coordination.committed_schema_digest()?;
            let classification = manifest::begin_native_table_provisioning(
                &mut manifest_connection,
                self.shard_count(),
                vec![declaration.clone()],
                committed_schema_digest,
                || {
                    migration.mark_pending_on_drop();
                    #[cfg(test)]
                    abort_generated_table_ddl_at_test_boundary("provisioning-before-commit");
                },
            )?;
            #[cfg(test)]
            abort_generated_table_ddl_at_test_boundary("provisioning-after-commit");
            let mut provisioning = match classification {
                manifest::NativeTableProvisioningClassification::Active(provisioning) => {
                    provisioning
                }
                manifest::NativeTableProvisioningClassification::Complete => {
                    return Err(EngineError::new(
                        EngineErrorKind::DataCorruption,
                        "generated-table DDL catalog completed outside its atomic bridge finalization",
                    ));
                }
                manifest::NativeTableProvisioningClassification::Absent => {
                    return Err(EngineError::new(
                        EngineErrorKind::Internal,
                        "generated-table DDL provisioning did not create its durable journal",
                    ));
                }
            };
            while provisioning.next_shard() < provisioning.shard_count() {
                let shard_id = provisioning.next_shard();
                self.seed_native_range_v1_sequences_on_shard(
                    provisioning.declarations(),
                    shard_id,
                )?;
                #[cfg(test)]
                if shard_id == 0 {
                    abort_generated_table_ddl_at_test_boundary("seed-0-before-progress");
                }
                provisioning = manifest::advance_native_table_provisioning(
                    &mut manifest_connection,
                    self.shard_count(),
                    &provisioning,
                    shard_id + 1,
                )?;
                #[cfg(test)]
                if shard_id == 0 {
                    abort_generated_table_ddl_at_test_boundary("seed-0-progress");
                }
            }

            let replacement_guard = self
                .schema_coordination
                .reserve_catalog_replacement(&self.catalog)?;
            let (replacement, completed) = manifest::finalize_generated_table_ddl_provisioning(
                &mut manifest_connection,
                self.shard_count(),
                &ddl,
                &provisioning,
                || {
                    migration.mark_pending_on_drop();
                    #[cfg(test)]
                    abort_generated_table_ddl_at_test_boundary("complete-before-commit");
                },
            )?;
            #[cfg(test)]
            abort_generated_table_ddl_at_test_boundary("complete-after-commit");
            self.catalog = replacement_guard.publish(&self.catalog, replacement)?;
            generated_table_ddl_receipt(&completed)
        })();
        if operation
            .as_ref()
            .is_err_and(|error: &EngineError| error.kind() == EngineErrorKind::DataCorruption)
        {
            self.record_schema_degraded();
        }
        let receipt = operation?;
        migration.publish_ready()?;
        Ok(receipt)
    }

    pub(crate) fn register_tables(
        &mut self,
        declarations: Vec<TableDeclaration>,
    ) -> EngineResult<()> {
        let result = self.register_tables_inner(declarations);
        self.fail_closed_on_corruption(result)
    }

    fn register_tables_inner(&mut self, declarations: Vec<TableDeclaration>) -> EngineResult<()> {
        self.validate_table_declaration_request(&declarations)?;
        let catalog_is_empty = self.catalog.logical().tables().is_empty();
        if !catalog_is_empty && !declarations_match_catalog(self.catalog.logical(), &declarations) {
            let _operation = self.enter_schema_operation()?;
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "the authoritative table catalog is already registered",
            ));
        }
        let has_generated_policy = declarations.iter().any(|declaration| {
            !matches!(declaration.generated_id_policy(), GeneratedIdPolicy::None)
        });
        let every_generated_policy_is_active = !has_generated_policy
            || declarations
                .iter()
                .all(|declaration| match declaration.generated_id_policy() {
                    GeneratedIdPolicy::None => true,
                    GeneratedIdPolicy::NativeRangeV1 { .. } => self
                        .catalog
                        .logical()
                        .table("default", declaration.name())
                        .ok()
                        .flatten()
                        .is_some_and(|table| self.catalog.native_id_policy_is_active(table.id())),
                    GeneratedIdPolicy::HiloV1 { .. } => self
                        .catalog
                        .logical()
                        .table("default", declaration.name())
                        .ok()
                        .flatten()
                        .is_some_and(|table| self.catalog.hilo_id_policy_is_active(table.id())),
                });
        if !catalog_is_empty && every_generated_policy_is_active {
            let _operation = self.enter_schema_operation()?;
            return Ok(());
        }
        let mut migration =
            SchemaMigrationGuard::new(self.schema_coordination.gate.begin_new_migration()?);
        migration.wait_for_quiescence_blocking();
        migration.acquire_process_ownership(&self.schema_coordination.process_lease)?;
        let registration = (|| {
            self.validate_empty_table_declarations(&declarations)?;
            let replacement_guard = self
                .schema_coordination
                .reserve_catalog_replacement(&self.catalog)?;

            let manifest_path = self.root.join("manifest.sqlite");
            let mut manifest_connection = open_existing_manifest(&manifest_path)?;
            configure_manifest_connection(&manifest_connection)?;
            let replacement = if has_generated_policy {
                let committed_schema_digest = self.schema_coordination.committed_schema_digest()?;
                let classification = manifest::begin_native_table_provisioning(
                    &mut manifest_connection,
                    self.shard_count(),
                    declarations.clone(),
                    committed_schema_digest,
                    || migration.mark_pending_on_drop(),
                )?;
                let mut provisioning = match classification {
                    manifest::NativeTableProvisioningClassification::Active(provisioning) => {
                        provisioning
                    }
                    manifest::NativeTableProvisioningClassification::Complete => {
                        return Err(EngineError::new(
                            EngineErrorKind::FailedPrecondition,
                            "table provisioning is already complete in the manifest; reopen this stale handle",
                        ));
                    }
                    manifest::NativeTableProvisioningClassification::Absent => {
                        return Err(EngineError::new(
                            EngineErrorKind::Internal,
                            "table provisioning did not create its durable journal",
                        ));
                    }
                };
                while provisioning.next_shard() < provisioning.shard_count() {
                    let shard_id = provisioning.next_shard();
                    self.seed_native_range_v1_sequences_on_shard(
                        provisioning.declarations(),
                        shard_id,
                    )?;
                    provisioning = manifest::advance_native_table_provisioning(
                        &mut manifest_connection,
                        self.shard_count(),
                        &provisioning,
                        shard_id + 1,
                    )?;
                }
                manifest::finalize_native_table_provisioning(
                    &mut manifest_connection,
                    self.shard_count(),
                    &provisioning,
                    || migration.mark_pending_on_drop(),
                )?
            } else {
                manifest::register_table_catalog(
                    &mut manifest_connection,
                    self.shard_count(),
                    declarations,
                    || migration.mark_pending_on_drop(),
                )?
            };
            let replacement = replacement_guard.publish(&self.catalog, replacement)?;
            self.catalog = replacement;
            Ok(())
        })();
        if registration
            .as_ref()
            .is_err_and(|error: &EngineError| error.kind() == EngineErrorKind::DataCorruption)
        {
            // Keep the exclusive migration guard live while degradation is
            // published so no concurrent operation observes a transient Ready
            // state between error propagation and fail-closed handling.
            self.record_schema_degraded();
        }
        registration?;
        migration.publish_ready()
    }

    pub(crate) fn create_global_index(
        &mut self,
        declaration: GlobalIndexDeclaration,
    ) -> EngineResult<GlobalIndexId> {
        let result = self.create_global_index_inner(declaration);
        self.fail_closed_on_corruption(result)
    }

    fn create_global_index_inner(
        &mut self,
        declaration: GlobalIndexDeclaration,
    ) -> EngineResult<GlobalIndexId> {
        let mut mutation =
            SchemaMigrationGuard::new(self.schema_coordination.gate.begin_new_migration()?);
        mutation.wait_for_quiescence_blocking();
        mutation.acquire_process_ownership(&self.schema_coordination.process_lease)?;
        let replacement_guard = self
            .schema_coordination
            .reserve_catalog_replacement(&self.catalog)?;
        let mut manifest_connection = open_existing_manifest(&self.root.join("manifest.sqlite"))?;
        configure_manifest_connection(&manifest_connection)?;
        let (replacement, index_id) = manifest::create_global_index(
            &mut manifest_connection,
            self.shard_count(),
            &declaration,
            || mutation.mark_pending_on_drop(),
        )?;
        self.catalog = replacement_guard.publish(&self.catalog, replacement)?;
        mutation.publish_ready()?;
        Ok(index_id)
    }

    pub(crate) fn transition_global_index(
        &mut self,
        index_id: GlobalIndexId,
        target: GlobalIndexLifecycle,
    ) -> EngineResult<()> {
        if target == GlobalIndexLifecycle::Ready {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!("global index {index_id} can become Ready only through build_global_index"),
            ));
        }
        let result = self.transition_global_index_inner(index_id, target);
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn build_global_index(
        &mut self,
        index_id: GlobalIndexId,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalIndexBuildReport> {
        let result = self.build_global_index_inner(index_id, cancellation);
        self.fail_closed_on_corruption(result)
    }

    fn build_global_index_inner(
        &mut self,
        index_id: GlobalIndexId,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalIndexBuildReport> {
        let mut mutation =
            SchemaMigrationGuard::new(self.schema_coordination.gate.begin_new_migration()?);
        mutation.wait_for_quiescence_blocking();
        mutation.acquire_process_ownership(&self.schema_coordination.process_lease)?;
        let index = self
            .catalog
            .logical()
            .global_index_by_id(index_id)
            .cloned()
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    format!("global index {index_id} does not exist"),
                )
            })?;

        if index.lifecycle() == GlobalIndexLifecycle::Ready {
            let report = global_index::validate_ready(self, &index, cancellation)?;
            mutation.publish_ready()?;
            return Ok(report);
        }

        let replacement_guard = self
            .schema_coordination
            .reserve_catalog_replacement(&self.catalog)?;
        let report = global_index::build(self, &index, cancellation)?;
        if cancellation.is_cancelled() {
            return Err(EngineError::new(
                EngineErrorKind::Cancelled,
                format!("global-index build {index_id} was cancelled before publication"),
            ));
        }
        let mut manifest_connection = open_existing_manifest(&self.root.join("manifest.sqlite"))?;
        configure_manifest_connection(&manifest_connection)?;
        let replacement = manifest::transition_global_index(
            &mut manifest_connection,
            self.shard_count(),
            index_id,
            GlobalIndexLifecycle::Ready,
            || mutation.mark_pending_on_drop(),
        )?;
        self.catalog = replacement_guard.publish(&self.catalog, replacement)?;
        mutation.publish_ready()?;
        Ok(report)
    }

    pub(crate) fn validate_global_index(
        &mut self,
        index_id: GlobalIndexId,
        options: GlobalIndexValidationOptions,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalIndexValidationReport> {
        let result = self.validate_global_index_inner(index_id, options, cancellation);
        self.fail_closed_on_corruption(result)
    }

    fn validate_global_index_inner(
        &mut self,
        index_id: GlobalIndexId,
        options: GlobalIndexValidationOptions,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalIndexValidationReport> {
        let mut mutation =
            SchemaMigrationGuard::new(self.schema_coordination.gate.begin_new_migration()?);
        mutation.wait_for_quiescence_blocking();
        mutation.acquire_process_ownership(&self.schema_coordination.process_lease)?;
        let original = self.global_index_metadata(index_id)?;
        match original.lifecycle() {
            GlobalIndexLifecycle::Ready | GlobalIndexLifecycle::Invalid => {
                self.publish_global_index_lifecycle(
                    index_id,
                    GlobalIndexLifecycle::Rebuilding,
                    &mut mutation,
                )?;
            }
            GlobalIndexLifecycle::Rebuilding => {}
            lifecycle => {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "global index {index_id} cannot be validated while its lifecycle is {lifecycle:?}"
                    ),
                ));
            }
        }
        abort_global_index_recovery_at_test_boundary("validation-fenced");
        let fenced = self.global_index_metadata(index_id)?;
        let outcome = match global_index::validate_index(self, &fenced, cancellation, options) {
            Ok(outcome) => outcome,
            Err(error) => {
                mutation.publish_ready()?;
                return Err(error);
            }
        };
        abort_global_index_recovery_at_test_boundary("validation-complete");
        let can_publish_ready = outcome.is_valid()
            && (original.lifecycle() == GlobalIndexLifecycle::Ready
                || (options.mode() == GlobalIndexValidationMode::Full
                    && (original.lifecycle() == GlobalIndexLifecycle::Rebuilding
                        || !original.is_unique())));
        let lifecycle_after = if can_publish_ready {
            GlobalIndexLifecycle::Ready
        } else {
            GlobalIndexLifecycle::Invalid
        };
        self.publish_global_index_lifecycle(index_id, lifecycle_after, &mut mutation)?;
        abort_global_index_recovery_at_test_boundary("validation-published");
        mutation.publish_ready()?;
        Ok(outcome.into_report(index_id, original.lifecycle(), lifecycle_after))
    }

    pub(crate) fn rebuild_global_index(
        &mut self,
        index_id: GlobalIndexId,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalIndexBuildReport> {
        let result = self.rebuild_global_index_inner(index_id, cancellation);
        self.fail_closed_on_corruption(result)
    }

    fn rebuild_global_index_inner(
        &mut self,
        index_id: GlobalIndexId,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalIndexBuildReport> {
        let mut mutation =
            SchemaMigrationGuard::new(self.schema_coordination.gate.begin_new_migration()?);
        mutation.wait_for_quiescence_blocking();
        mutation.acquire_process_ownership(&self.schema_coordination.process_lease)?;
        let original = self.global_index_metadata(index_id)?;
        match original.lifecycle() {
            GlobalIndexLifecycle::Ready | GlobalIndexLifecycle::Invalid => {
                self.publish_global_index_lifecycle(
                    index_id,
                    GlobalIndexLifecycle::Rebuilding,
                    &mut mutation,
                )?;
            }
            GlobalIndexLifecycle::Rebuilding => {}
            lifecycle => {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "global index {index_id} cannot be rebuilt while its lifecycle is {lifecycle:?}"
                    ),
                ));
            }
        }
        abort_global_index_recovery_at_test_boundary("rebuild-fenced");
        let fenced = self.global_index_metadata(index_id)?;
        let report = match global_index::rebuild(self, &fenced, cancellation) {
            Ok(report) => report,
            Err(error) => {
                mutation.publish_ready()?;
                return Err(error);
            }
        };
        if cancellation.is_cancelled() {
            mutation.publish_ready()?;
            return Err(EngineError::new(
                EngineErrorKind::Cancelled,
                format!("global-index rebuild {index_id} was cancelled before publication"),
            ));
        }
        abort_global_index_recovery_at_test_boundary("rebuild-complete");
        self.publish_global_index_lifecycle(index_id, GlobalIndexLifecycle::Ready, &mut mutation)?;
        abort_global_index_recovery_at_test_boundary("rebuild-published");
        mutation.publish_ready()?;
        Ok(report)
    }

    pub(crate) fn repair_global_index(
        &mut self,
        index_id: GlobalIndexId,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalIndexRepairReport> {
        let result = self.repair_global_index_inner(index_id, cancellation);
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn reserve_global_unique(
        &self,
        operation_id: GlobalOperationId,
        mutation: &GlobalUniqueMutation,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalUniqueReservation> {
        let result = (|| {
            let _operation = self.enter_schema_operation()?;
            let index = self.validate_authority_index(mutation.index_id(), true, false)?;
            for (_, owner) in [mutation.previous_entry(), mutation.new_entry()]
                .into_iter()
                .flatten()
            {
                if owner.source_shard() >= self.shard_count() {
                    return Err(EngineError::new(
                        EngineErrorKind::InvalidArgument,
                        format!(
                            "global-index owner shard {} is outside 0..{}",
                            owner.source_shard(),
                            self.shard_count()
                        ),
                    ));
                }
            }
            for (key, _) in [mutation.previous_entry(), mutation.new_entry()]
                .into_iter()
                .flatten()
            {
                self.validate_authority_key(index, key)?;
            }
            debug_assert_eq!(index.id(), mutation.index_id());
            global_index::reserve_unique(
                &self.root,
                operation_id,
                mutation,
                index,
                self.shard_count(),
                cancellation,
            )
        })();
        self.fail_closed_on_corruption(result)
    }

    /// Reserve one coordinator-owned unique mutation while holding an exact
    /// process-liveness lease used by crash recovery.
    pub(crate) fn reserve_global_unique_write(
        &self,
        mutation: &GlobalUniqueMutation,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalWriteReservationGuard> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::StorageUnavailable,
                "could not generate a global-index write operation ID",
                error,
            )
        })?;
        if bytes == [0; 16] {
            bytes[0] = 1;
        }
        let operation_id = GlobalOperationId::new(bytes)?;
        let lease = process_lock::GlobalWriteOperationLease::acquire(&self.root, operation_id)?;
        if let Err(error) = self.reserve_global_unique(operation_id, mutation, cancellation) {
            process_lock::remove_global_write_marker(&self.root, operation_id);
            return Err(error);
        }
        Ok(GlobalWriteReservationGuard {
            operation_id,
            _lease: lease,
        })
    }

    pub(crate) fn finalize_global_unique_write(
        &self,
        reservation: &GlobalWriteReservationGuard,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalUniqueReservation> {
        let result =
            global_index::finalize_unique_write(self, reservation.operation_id, cancellation);
        if result.is_ok() {
            process_lock::remove_global_write_marker(&self.root, reservation.operation_id);
        }
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn rollback_global_unique_write(
        &self,
        reservation: &GlobalWriteReservationGuard,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalUniqueReservation> {
        let result = self.rollback_global_unique(reservation.operation_id, cancellation);
        if result.is_ok() {
            process_lock::remove_global_write_marker(&self.root, reservation.operation_id);
        }
        result
    }

    pub(crate) fn refresh_global_unique_write_indexes(
        &self,
        index_ids: &[GlobalIndexId],
        shard: u16,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<()> {
        let result =
            global_index::refresh_unique_write_indexes(self, index_ids, shard, cancellation);
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn global_index_read_resolution(
        &self,
        index_id: GlobalIndexId,
        keys: &[crate::core::CanonicalIndexKey],
        query_predicate_sql: &str,
        query_table_alias: Option<&str>,
        parameters: &[crate::core::Value],
        read_control: (&crate::core::CancellationToken, Option<Instant>),
    ) -> EngineResult<crate::core::GlobalIndexReadResolution> {
        (|| {
            let index = self.validate_authority_index(index_id, false, false)?;
            for key in keys {
                self.validate_authority_key(index, key)?;
            }
            if index.is_unique() {
                global_index::lookup_authoritative_owners(self, index, keys)
                    .map(crate::core::GlobalIndexReadResolution::authoritative)
            } else {
                global_index::verify_nonunique_candidates(
                    self,
                    index,
                    keys,
                    query_predicate_sql,
                    query_table_alias,
                    parameters,
                    read_control,
                )
            }
        })()
    }

    /// Recover only coordinator-owned active reservations whose OS lease is
    /// no longer held by a live writer.
    pub(crate) fn recover_global_unique_writes(
        &self,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<usize> {
        let result = (|| {
            let mut recovered = 0_usize;
            for (operation_id, mutation) in global_index::active_unique_mutations(&self.root)? {
                let Some(_lease) = process_lock::GlobalWriteOperationLease::try_acquire_orphan(
                    &self.root,
                    operation_id,
                )?
                else {
                    continue;
                };
                match global_index::decide_orphaned_unique_write(self, &mutation, cancellation)? {
                    global_index::OrphanedUniqueWriteDecision::Finalize => {
                        global_index::finalize_unique_write(self, operation_id, cancellation)?;
                    }
                    global_index::OrphanedUniqueWriteDecision::RollBack => {
                        global_index::rollback_unique(&self.root, operation_id, cancellation)?;
                    }
                }
                process_lock::remove_global_write_marker(&self.root, operation_id);
                recovered = recovered.checked_add(1).ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::LimitExceeded,
                        "global-index recovered operation count overflowed",
                    )
                })?;
            }
            Ok(recovered)
        })();
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn finalize_global_unique(
        &self,
        operation_id: GlobalOperationId,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalUniqueReservation> {
        let result = (|| {
            let _operation = self.enter_schema_operation()?;
            global_index::finalize_unique(&self.root, operation_id, cancellation)
        })();
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn rollback_global_unique(
        &self,
        operation_id: GlobalOperationId,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalUniqueReservation> {
        let result = (|| {
            let _operation = self.enter_schema_operation()?;
            global_index::rollback_unique(&self.root, operation_id, cancellation)
        })();
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn lease_global_values(
        &self,
        operation_id: GlobalOperationId,
        index_id: GlobalIndexId,
        count: u32,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalValueLease> {
        let result = (|| {
            let _operation = self.enter_schema_operation()?;
            let index = self.validate_authority_index(index_id, true, true)?;
            global_index::lease_values(
                &self.root,
                operation_id,
                index,
                self.shard_count(),
                count,
                cancellation,
            )
        })();
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn transition_global_value_lease(
        &self,
        operation_id: GlobalOperationId,
        finalize: bool,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalValueLease> {
        let result = (|| {
            let _operation = self.enter_schema_operation()?;
            global_index::transition_value_lease(&self.root, operation_id, finalize, cancellation)
        })();
        self.fail_closed_on_corruption(result)
    }

    fn validate_authority_index(
        &self,
        index_id: GlobalIndexId,
        require_unique: bool,
        require_integer: bool,
    ) -> EngineResult<&GlobalIndexMetadata> {
        let index = self
            .catalog
            .logical()
            .global_index_by_id(index_id)
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    format!("global index {index_id} does not exist"),
                )
            })?;
        if index.lifecycle() != GlobalIndexLifecycle::Ready {
            return Err(EngineError::new(
                EngineErrorKind::Busy,
                format!(
                    "global index {index_id} is not authoritative while {:?}",
                    index.lifecycle()
                ),
            ));
        }
        if require_unique && !index.is_unique() {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!("global index {index_id} is not unique"),
            ));
        }
        if require_integer
            && (index.key_parts().len() != 1
                || !matches!(
                    index.key_parts()[0].key_type(),
                    GlobalIndexKeyType::Int64 | GlobalIndexKeyType::UInt64
                ))
        {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!("global index {index_id} is not a single integer value authority"),
            ));
        }
        Ok(index)
    }

    fn validate_authority_key(
        &self,
        index: &GlobalIndexMetadata,
        key: &crate::core::CanonicalIndexKey,
    ) -> EngineResult<()> {
        let parts = key.decode()?;
        if parts.len() != index.key_parts().len() {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!(
                    "global index {} requires {} key parts, not {}",
                    index.id(),
                    index.key_parts().len(),
                    parts.len()
                ),
            ));
        }
        for (ordinal, (part, expected)) in parts.iter().zip(index.key_parts()).enumerate() {
            let type_matches = matches!(
                (expected.key_type(), part.value()),
                (_, IndexKeyValue::Null)
                    | (GlobalIndexKeyType::Boolean, IndexKeyValue::Boolean(_))
                    | (GlobalIndexKeyType::Int64, IndexKeyValue::Int64(_))
                    | (GlobalIndexKeyType::UInt64, IndexKeyValue::UInt64(_))
                    | (GlobalIndexKeyType::Float64, IndexKeyValue::Float64(_))
                    | (GlobalIndexKeyType::Date, IndexKeyValue::Date(_))
                    | (GlobalIndexKeyType::Timestamp, IndexKeyValue::Timestamp(_))
                    | (GlobalIndexKeyType::Text, IndexKeyValue::Text(_))
                    | (GlobalIndexKeyType::Binary, IndexKeyValue::Binary(_))
            );
            if !type_matches
                || part.order() != expected.order()
                || part.null_order() != expected.null_order()
                || part.collation() != expected.collation()
            {
                return Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    format!(
                        "global index {} key part {ordinal} does not match its definition",
                        index.id()
                    ),
                ));
            }
            if matches!(part.value(), IndexKeyValue::Null)
                && index.null_semantics() == crate::core::UniqueNullSemantics::Distinct
            {
                return Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    format!(
                        "global index {} uses distinct NULL semantics and must not reserve NULL keys",
                        index.id()
                    ),
                ));
            }
        }
        Ok(())
    }

    fn repair_global_index_inner(
        &mut self,
        index_id: GlobalIndexId,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalIndexRepairReport> {
        let mut mutation =
            SchemaMigrationGuard::new(self.schema_coordination.gate.begin_new_migration()?);
        mutation.wait_for_quiescence_blocking();
        mutation.acquire_process_ownership(&self.schema_coordination.process_lease)?;
        let original = self.global_index_metadata(index_id)?;
        if original.is_unique() {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "global index {index_id} is authoritative for uniqueness and must be rebuilt"
                ),
            ));
        }
        match original.lifecycle() {
            GlobalIndexLifecycle::Ready | GlobalIndexLifecycle::Invalid => {
                self.publish_global_index_lifecycle(
                    index_id,
                    GlobalIndexLifecycle::Rebuilding,
                    &mut mutation,
                )?;
            }
            GlobalIndexLifecycle::Rebuilding => {}
            lifecycle => {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "global index {index_id} cannot be repaired while its lifecycle is {lifecycle:?}"
                    ),
                ));
            }
        }
        abort_global_index_recovery_at_test_boundary("repair-fenced");
        let fenced = self.global_index_metadata(index_id)?;
        let initial = match global_index::validate_index(
            self,
            &fenced,
            cancellation,
            GlobalIndexValidationOptions::full(),
        ) {
            Ok(outcome) => outcome,
            Err(error) => {
                mutation.publish_ready()?;
                return Err(error);
            }
        };
        let repair_shards = initial.repair_shards(self.shard_count());
        let (repaired_shards, indexed_rows) = if initial.is_valid() {
            let report = global_index::validate_ready(self, &fenced, cancellation)?;
            (Vec::new(), report.indexed_rows())
        } else {
            match global_index::repair_non_unique(self, &fenced, cancellation, repair_shards) {
                Ok(report) => report,
                Err(error) => {
                    mutation.publish_ready()?;
                    return Err(error);
                }
            }
        };
        abort_global_index_recovery_at_test_boundary("repair-complete");
        let final_outcome = match global_index::validate_index(
            self,
            &fenced,
            cancellation,
            GlobalIndexValidationOptions::full(),
        ) {
            Ok(outcome) => outcome,
            Err(error) => {
                mutation.publish_ready()?;
                return Err(error);
            }
        };
        let lifecycle_after = if final_outcome.is_valid() {
            GlobalIndexLifecycle::Ready
        } else {
            GlobalIndexLifecycle::Invalid
        };
        self.publish_global_index_lifecycle(index_id, lifecycle_after, &mut mutation)?;
        abort_global_index_recovery_at_test_boundary("repair-published");
        mutation.publish_ready()?;
        let validation = final_outcome.into_report(index_id, original.lifecycle(), lifecycle_after);
        Ok(GlobalIndexRepairReport::from_validated(
            index_id,
            repaired_shards,
            indexed_rows,
            validation,
        ))
    }

    fn global_index_metadata(&self, index_id: GlobalIndexId) -> EngineResult<GlobalIndexMetadata> {
        self.catalog
            .logical()
            .global_index_by_id(index_id)
            .cloned()
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    format!("global index {index_id} does not exist"),
                )
            })
    }

    pub(crate) fn global_index_outbox_status(
        &self,
    ) -> EngineResult<Vec<GlobalIndexOutboxShardStatus>> {
        let result = index_outbox::inspect(self);
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn read_global_index_outbox(
        &self,
        index_id: GlobalIndexId,
        shard: u16,
        after: GlobalIndexOutboxCursor,
        limit: usize,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalIndexOutboxBatch> {
        let result = (|| {
            self.validate_nonunique_outbox_index(index_id)?;
            index_outbox::read_batch(self, index_id, shard, after.get(), limit, cancellation)
        })();
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn advance_global_index_outbox(
        &self,
        index_id: GlobalIndexId,
        shard: u16,
        cursor: GlobalIndexOutboxCursor,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalIndexOutboxShardStatus> {
        let result = (|| {
            self.validate_nonunique_outbox_index(index_id)?;
            index_outbox::advance_consumer(self, index_id, shard, cursor.get(), cancellation)
        })();
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn prune_global_index_outbox(
        &self,
        shard: u16,
        limit: usize,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalIndexOutboxPruneReport> {
        let result = index_outbox::prune(self, shard, limit, cancellation);
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn global_index_async_status(
        &self,
        index_id: GlobalIndexId,
    ) -> EngineResult<GlobalIndexAsyncStatus> {
        let result = (|| {
            let index = self.global_index_metadata(index_id)?;
            global_index_async::status(self, &index)
        })();
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn process_global_index_async(
        &self,
        index_id: GlobalIndexId,
        owner_id: [u8; 16],
        options: GlobalIndexAsyncOptions,
        cancellation: &crate::core::CancellationToken,
    ) -> EngineResult<GlobalIndexAsyncProcessReport> {
        let result = (|| {
            let _schema = self.enter_schema_operation()?;
            let index = self.global_index_metadata(index_id)?;
            if index.lifecycle() != GlobalIndexLifecycle::Ready {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!("global index {index_id} is not ready for asynchronous maintenance"),
                ));
            }
            global_index_async::process_index(self, &index, owner_id, options, cancellation)
        })();
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn set_global_index_async_paused(
        &self,
        index_id: GlobalIndexId,
        paused: bool,
    ) -> EngineResult<()> {
        let result = (|| {
            let _schema = self.enter_schema_operation()?;
            let index = self.global_index_metadata(index_id)?;
            global_index_async::set_paused(self, &index, paused)
        })();
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn ready_nonunique_global_indexes(&self) -> Vec<GlobalIndexId> {
        self.catalog
            .logical()
            .global_indexes()
            .iter()
            .filter(|index| !index.is_unique() && index.lifecycle() == GlobalIndexLifecycle::Ready)
            .map(GlobalIndexMetadata::id)
            .collect()
    }

    pub(crate) fn fence_uncoordinated_nonunique_write(
        &self,
        table_id: TableId,
    ) -> EngineResult<()> {
        for index_id in self
            .catalog
            .logical()
            .global_indexes()
            .iter()
            .filter(|index| {
                index.table_id() == table_id
                    && !index.is_unique()
                    && index.lifecycle() == GlobalIndexLifecycle::Ready
            })
            .map(GlobalIndexMetadata::id)
        {
            global_index_async::mark_rebuild_required(self, index_id)?;
        }
        Ok(())
    }

    fn validate_nonunique_outbox_index(&self, index_id: GlobalIndexId) -> EngineResult<()> {
        let index = self.global_index_metadata(index_id)?;
        if index.is_unique() {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!("global index {index_id} is unique and does not use the shard outbox"),
            ));
        }
        Ok(())
    }

    fn publish_global_index_lifecycle(
        &mut self,
        index_id: GlobalIndexId,
        target: GlobalIndexLifecycle,
        mutation: &mut SchemaMigrationGuard,
    ) -> EngineResult<()> {
        let replacement_guard = self
            .schema_coordination
            .reserve_catalog_replacement(&self.catalog)?;
        let mut manifest_connection = open_existing_manifest(&self.root.join("manifest.sqlite"))?;
        configure_manifest_connection(&manifest_connection)?;
        let replacement = manifest::transition_global_index(
            &mut manifest_connection,
            self.shard_count(),
            index_id,
            target,
            || mutation.mark_pending_on_drop(),
        )?;
        self.catalog = replacement_guard.publish(&self.catalog, replacement)?;
        Ok(())
    }

    fn transition_global_index_inner(
        &mut self,
        index_id: GlobalIndexId,
        target: GlobalIndexLifecycle,
    ) -> EngineResult<()> {
        let mut mutation =
            SchemaMigrationGuard::new(self.schema_coordination.gate.begin_new_migration()?);
        mutation.wait_for_quiescence_blocking();
        mutation.acquire_process_ownership(&self.schema_coordination.process_lease)?;
        let replacement_guard = self
            .schema_coordination
            .reserve_catalog_replacement(&self.catalog)?;
        let mut manifest_connection = open_existing_manifest(&self.root.join("manifest.sqlite"))?;
        configure_manifest_connection(&manifest_connection)?;
        let replacement = manifest::transition_global_index(
            &mut manifest_connection,
            self.shard_count(),
            index_id,
            target,
            || mutation.mark_pending_on_drop(),
        )?;
        self.catalog = replacement_guard.publish(&self.catalog, replacement)?;
        mutation.publish_ready()
    }

    pub(crate) fn remove_global_index(&mut self, index_id: GlobalIndexId) -> EngineResult<()> {
        let result = self.remove_global_index_inner(index_id);
        self.fail_closed_on_corruption(result)
    }

    fn remove_global_index_inner(&mut self, index_id: GlobalIndexId) -> EngineResult<()> {
        let mut mutation =
            SchemaMigrationGuard::new(self.schema_coordination.gate.begin_new_migration()?);
        mutation.wait_for_quiescence_blocking();
        mutation.acquire_process_ownership(&self.schema_coordination.process_lease)?;
        let replacement_guard = self
            .schema_coordination
            .reserve_catalog_replacement(&self.catalog)?;
        let index = self
            .catalog
            .logical()
            .global_index_by_id(index_id)
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    format!("global index {index_id} does not exist"),
                )
            })?;
        if index.lifecycle() != GlobalIndexLifecycle::Dropping {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!("global index {index_id} must enter Dropping before removal"),
            ));
        }
        if !index.is_unique() {
            index_outbox::deactivate_index(self, index_id)?;
        }
        global_index::remove_artifacts(&self.root, index_id)?;
        let mut manifest_connection = open_existing_manifest(&self.root.join("manifest.sqlite"))?;
        configure_manifest_connection(&manifest_connection)?;
        let replacement = manifest::remove_global_index(
            &mut manifest_connection,
            self.shard_count(),
            index_id,
            || mutation.mark_pending_on_drop(),
        )?;
        self.catalog = replacement_guard.publish(&self.catalog, replacement)?;
        mutation.publish_ready()
    }

    fn validate_table_declaration_request(
        &self,
        declarations: &[TableDeclaration],
    ) -> EngineResult<()> {
        if declarations.is_empty() {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "table registration requires at least one declaration",
            ));
        }
        if declarations.len() > MAX_TABLES {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!("table registration exceeds its {MAX_TABLES}-table limit"),
            ));
        }
        let default_database_id = self.catalog.logical().default_database().id();
        let mut names = BTreeSet::new();
        for declaration in declarations {
            if declaration.database_id() != default_database_id {
                return Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    format!(
                        "table {} must use the storage-default logical database",
                        declaration.name()
                    ),
                ));
            }
            if !names.insert(declaration.name()) {
                return Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    format!("table {} is declared more than once", declaration.name()),
                ));
            }
        }
        Ok(())
    }

    /// Seed every native table's shard-local AUTOINCREMENT source before the
    /// authoritative catalog can publish the policy.
    ///
    /// Each shard is committed independently, so a process exit can leave a
    /// prefix seeded. Publication is deliberately last, and retries install at
    /// least the same durable owner floor without lowering a valid same-owner
    /// high-water mark left by earlier committed-and-deleted rows.
    #[cfg(test)]
    fn seed_native_range_v1_sequences(
        &self,
        declarations: &[TableDeclaration],
    ) -> EngineResult<()> {
        for shard_id in 0..self.shard_count() {
            self.seed_native_range_v1_sequences_on_shard(declarations, shard_id)?;
        }
        Ok(())
    }

    fn seed_native_range_v1_sequences_on_shard(
        &self,
        declarations: &[TableDeclaration],
        shard_id: u16,
    ) -> EngineResult<()> {
        let native_tables = declarations
            .iter()
            .filter_map(|declaration| match declaration.generated_id_policy() {
                GeneratedIdPolicy::NativeRangeV1 { column } => {
                    Some((declaration.name(), column.as_str()))
                }
                GeneratedIdPolicy::None | GeneratedIdPolicy::HiloV1 { .. } => None,
            })
            .collect::<Vec<_>>();
        if native_tables.is_empty() {
            return Ok(());
        }
        let allocation_owners = self.catalog.allocation_owners().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                "native_range_v1 registration requires a durable allocation-owner map",
            )
        })?;

        let owner = allocation_owners
            .owner_for_physical_shard(shard_id)
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!(
                        "physical shard {shard_id} has no durable native-range allocation owner"
                    ),
                )
            })?;
        let floor = native_range_v1_sequence_floor(owner);
        let ceiling = native_range_v1_sequence_ceiling(owner);
        let marker = i64::try_from(NATIVE_RANGE_V1_FORMAT_MARKER)
            .expect("the native-range marker reserves the signed high bit");
        let mut connection = self.open_shard(shard_id)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error::storage)?;
        for &(table, column) in &native_tables {
            let quoted_table = quote_identifier(table);
            let has_rows = transaction
                .query_row(
                    &format!("SELECT EXISTS(SELECT 1 FROM {quoted_table} LIMIT 1)"),
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(sqlite_error::storage)?;
            if has_rows != 0 {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "table {table} must remain empty on physical shard {shard_id} while native_range_v1 is provisioned"
                    ),
                ));
            }
            if !shard::native_generated_column_is_exact(&transaction, table, column)? {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "generated column {column} on table {table} must be exactly INTEGER PRIMARY KEY AUTOINCREMENT on physical shard {shard_id}"
                    ),
                ));
            }
            let (row_count, integer_count) = transaction
                .query_row(
                    "SELECT COUNT(*), COALESCE(SUM(typeof(seq) = 'integer'), 0)
                         FROM main.sqlite_sequence
                         WHERE name = ?1 COLLATE BINARY",
                    [table],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .map_err(sqlite_error::storage)?;
            match (row_count, integer_count) {
                (0, 0) => {
                    transaction
                        .execute(
                            "INSERT INTO main.sqlite_sequence(name, seq) VALUES (?1, ?2)",
                            rusqlite::params![table, floor],
                        )
                        .map_err(sqlite_error::storage)?;
                }
                (1, 1) => {
                    let sequence = transaction
                        .query_row(
                            "SELECT seq FROM main.sqlite_sequence
                                 WHERE name = ?1 COLLATE BINARY",
                            [table],
                            |row| row.get::<_, i64>(0),
                        )
                        .map_err(sqlite_error::storage)?;
                    if sequence < marker {
                        transaction
                            .execute(
                                "UPDATE main.sqlite_sequence SET seq = ?2
                                     WHERE name = ?1 COLLATE BINARY",
                                rusqlite::params![table, floor],
                            )
                            .map_err(sqlite_error::storage)?;
                    } else if !(floor..=ceiling).contains(&sequence) {
                        return Err(EngineError::new(
                            EngineErrorKind::FailedPrecondition,
                            format!(
                                "table {table} on physical shard {shard_id} has sqlite_sequence value {sequence} in a conflicting native allocation-owner range"
                            ),
                        ));
                    }
                    // Preserve a same-owner high-water mark. Lowering it,
                    // even while the table is empty, could reuse an ID
                    // that was committed and later deleted.
                }
                _ => {
                    return Err(EngineError::new(
                        EngineErrorKind::FailedPrecondition,
                        format!(
                            "table {table} on physical shard {shard_id} must have at most one integer sqlite_sequence row before native_range_v1 registration"
                        ),
                    ));
                }
            }
        }
        transaction.commit().map_err(sqlite_error::storage)
    }

    fn validate_empty_table_declarations(
        &self,
        declarations: &[TableDeclaration],
    ) -> EngineResult<()> {
        let mut physical_declarations = BTreeSet::new();
        let mut catalog_declarations = BTreeSet::new();
        for declaration in declarations {
            if matches!(declaration.placement(), TablePlacement::Catalog) {
                catalog_declarations.insert(declaration.name().to_owned());
            } else {
                physical_declarations.insert(declaration.name().to_owned());
            }
        }

        for shard_id in 0..self.shard_count() {
            let connection = self.open_shard(shard_id)?;
            let has_application_trigger = connection
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM main.sqlite_schema
                         WHERE type = 'trigger' AND name NOT GLOB 'sqlite_*'
                     )",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(sqlite_error::storage)?;
            if has_application_trigger {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "application triggers are not supported with authoritative placement on physical shard {shard_id}"
                    ),
                ));
            }
            let has_application_virtual_table = connection
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM pragma_table_list
                         WHERE schema = 'main' AND type = 'virtual'
                     )",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(sqlite_error::storage)?;
            if has_application_virtual_table {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "virtual tables are not supported with authoritative placement on physical shard {shard_id}"
                    ),
                ));
            }
            shard::validate_stateless_catalog_schema(&connection)?;
            let physical_names = application_table_names(&connection)?;
            let relation_names = application_relation_names(&connection)?;
            if let Some(shadow) = catalog_declarations.iter().find(|name| {
                relation_names
                    .iter()
                    .any(|relation| relation.eq_ignore_ascii_case(name))
            }) {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "catalog table {shadow} has an application-table shadow on physical shard {shard_id}"
                    ),
                ));
            }
            if physical_declarations != physical_names {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "the declared physical tables do not exactly match physical shard {shard_id}"
                    ),
                ));
            }
            for declaration in declarations {
                if !matches!(declaration.placement(), TablePlacement::Catalog) {
                    validate_empty_table_declaration(
                        &connection,
                        shard_id,
                        declaration,
                        declarations,
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Prove a generated-table candidate can become both authoritative and
    /// writable while every change still belongs to a rollback-only SQLite
    /// transaction. This runs before the durable DDL bridge or schema-migration
    /// journal is created.
    fn preflight_generated_table_ddl(
        &self,
        physical_sql: &str,
        declaration: &TableDeclaration,
    ) -> EngineResult<()> {
        let expected_tables = BTreeSet::from([declaration.name().to_owned()]);
        for shard_id in 0..self.shard_count() {
            let mut connection = self.open_shard(shard_id)?;
            let existing_object = connection
                .query_row(
                    "SELECT name
                     FROM main.sqlite_schema
                     WHERE name NOT GLOB 'sqlite_*'
                       AND name <> 'briskdb_shard_metadata'
                       AND tbl_name <> 'briskdb_shard_metadata'
                     ORDER BY name COLLATE BINARY
                     LIMIT 1",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(sqlite_error::storage)?;
            if let Some(existing_object) = existing_object {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "generated-table DDL requires an empty physical application schema; physical shard {shard_id} already contains {existing_object}"
                    ),
                ));
            }

            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error::storage)?;
            let validation = (|| {
                shard::execute_schema_migration_batch(&transaction, physical_sql)?;
                if application_table_names(&transaction)? != expected_tables {
                    return Err(EngineError::new(
                        EngineErrorKind::FailedPrecondition,
                        format!(
                            "generated-table DDL did not produce exactly its declared table on physical shard {shard_id}"
                        ),
                    ));
                }
                validate_empty_table_declaration(
                    &transaction,
                    shard_id,
                    declaration,
                    std::slice::from_ref(declaration),
                )?;
                if let Some(reason) =
                    shard::writable_table_unsupported_reason(&transaction, declaration.name())?
                {
                    return Err(EngineError::new(
                        EngineErrorKind::FailedPrecondition,
                        format!(
                            "generated-table DDL cannot publish a writable table on physical shard {shard_id}: {reason}"
                        ),
                    ));
                }
                Ok(())
            })();
            transaction.rollback().map_err(sqlite_error::storage)?;
            validation?;
        }
        Ok(())
    }

    pub(crate) fn current_schema_generation(&self) -> u64 {
        self.catalog.logical().schema_generation()
    }

    pub(crate) fn enter_schema_operation(&self) -> EngineResult<SchemaOperationGuard> {
        self.schema_coordination.gate.try_acquire_operation()
    }

    pub(crate) fn begin_schema_migration(&self) -> EngineResult<SchemaMigrationGuard> {
        self.schema_coordination
            .gate
            .begin_migration()
            .map(SchemaMigrationGuard::new)
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
        if !self.catalog.logical().global_indexes().is_empty() {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "application-schema migration is fenced while global-index definitions exist; drop them before migrating",
            ));
        }
        guard.acquire_process_ownership(&self.schema_coordination.process_lease)?;
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
            self.validate_native_range_v1_state(&connection, shard)?;
            attach_storage_authorizer(&connection)?;
            Ok(connection)
        })();
        self.fail_closed_on_corruption(result)
    }

    /// Open and validate a writable virtual-table child while polling the
    /// coordinator's cancellation epoch before an interrupt handle exists.
    #[cfg(feature = "experimental-vtab")]
    pub(crate) fn open_shard_write_cancellable(
        &self,
        shard: u16,
        cancellation_epoch: Arc<std::sync::atomic::AtomicU64>,
        expected_epoch: u64,
    ) -> EngineResult<Connection> {
        let result = (|| {
            self.ensure_shard_in_range(shard)?;
            let connection = self.open_unconfigured_shard(shard)?;
            connection
                .busy_timeout(std::time::Duration::from_millis(25))
                .map_err(sqlite_error::storage)?;
            let progress_epoch = Arc::clone(&cancellation_epoch);
            connection
                .progress_handler(
                    64,
                    Some(move || progress_epoch.load(Ordering::Acquire) != expected_epoch),
                )
                .map_err(sqlite_error::storage)?;

            let deadline = std::time::Instant::now()
                .checked_add(CONNECTION_BUSY_TIMEOUT)
                .unwrap_or_else(std::time::Instant::now);
            loop {
                if cancellation_epoch.load(Ordering::Acquire) != expected_epoch {
                    return Err(EngineError::new(
                        EngineErrorKind::Cancelled,
                        "the writable shard child was cancelled while opening",
                    ));
                }
                match self.validate_unconfigured_shard(&connection, shard) {
                    Ok(()) => break,
                    Err(error)
                        if error.kind() == EngineErrorKind::Busy
                            && std::time::Instant::now() < deadline =>
                    {
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
            if cancellation_epoch.load(Ordering::Acquire) != expected_epoch {
                return Err(EngineError::new(
                    EngineErrorKind::Cancelled,
                    "the writable shard child was cancelled while opening",
                ));
            }
            attach_storage_authorizer(&connection)?;
            Ok(connection)
        })();
        self.fail_closed_on_corruption(result)
    }

    #[cfg(feature = "experimental-vtab")]
    pub(crate) fn open_shard_read_only(&self, shard: u16) -> EngineResult<Connection> {
        let result = (|| {
            self.ensure_shard_in_range(shard)?;
            let path = self.shard_path(shard);
            let connection = shard::open_existing_read_only(
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
            self.validate_native_range_v1_state(&connection, shard)?;
            attach_storage_authorizer(&connection)?;
            Ok(connection)
        })();
        self.fail_closed_on_corruption(result)
    }

    /// Validate and inspect one read-only shard while cancellation remains
    /// installed across the entire virtual-table registry discovery.
    #[cfg(feature = "experimental-vtab")]
    pub(crate) fn with_shard_read_only_controlled<T>(
        &self,
        shard: u16,
        control: Arc<crate::core::OperationControl>,
        inspect: impl FnOnce(&Connection) -> EngineResult<T>,
    ) -> EngineResult<T> {
        // This controlled path is non-terminal: its caller still owns the
        // cancellation linearization point. Let that wrapper distinguish an
        // interrupted validation from authoritative corruption before the
        // Engine applies terminal fail-closed policy.
        (|| {
            self.ensure_shard_in_range(shard)?;
            let mut connection = shard::open_required_file_read_only(&self.shard_path(shard))?;
            pool::with_read_only_connection_controlled(
                &mut connection,
                control,
                self,
                shard,
                inspect,
            )
        })()
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
        let mut verified_connections = Vec::with_capacity(usize::from(self.shard_count()));
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
            verified_connections.push(connection);
        }
        let consensus = consensus.ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                "schema verification requires at least one configured shard",
            )
        })?;

        // Establish trusted and cross-shard digest agreement everywhere before
        // enforcing compatibility policy. Otherwise a policy error on an early
        // shard could mask later corruption and prevent terminal degradation.
        for (shard_id, connection) in verified_connections.iter().enumerate() {
            shard::validate_registered_table_schema_with_active_generated_ids(
                connection,
                self.catalog.logical(),
                self.catalog.active_native_id_table_ids(),
                self.catalog.active_hilo_id_table_ids(),
            )?;
            self.validate_native_range_v1_state(
                connection,
                u16::try_from(shard_id).expect("the validated shard set fits u16"),
            )?;
        }
        Ok(consensus)
    }

    fn open_unconfigured_shard(&self, shard: u16) -> EngineResult<Connection> {
        self.ensure_shard_in_range(shard)?;
        shard::open_required_file(&self.shard_path(shard))
    }

    fn validate_unconfigured_shard(&self, connection: &Connection, shard: u16) -> EngineResult<()> {
        let result = self.validate_unconfigured_shard_nonterminal(connection, shard);
        self.fail_closed_on_corruption(result)
    }

    /// Validate a handle owned by a cancellation-controlled worker without
    /// changing persistent admission state. Its controlled wrapper resolves an
    /// interrupted validation to cancellation; the Engine then persists any
    /// DataCorruption that remains authoritative.
    fn validate_unconfigured_shard_nonterminal(
        &self,
        connection: &Connection,
        shard: u16,
    ) -> EngineResult<()> {
        (|| {
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
            )?;
            self.validate_native_range_v1_state(connection, shard)
        })()
    }

    #[cfg(feature = "experimental-vtab")]
    fn validate_unconfigured_shard_read_only_nonterminal(
        &self,
        connection: &Connection,
        shard: u16,
    ) -> EngineResult<()> {
        (|| {
            self.ensure_shard_in_range(shard)?;
            shard::validate_open_read_only_connection(
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
            )?;
            self.validate_native_range_v1_state(connection, shard)?;
            attach_storage_authorizer(connection)
        })()
    }

    fn validate_native_range_v1_state(
        &self,
        connection: &Connection,
        physical_shard: u16,
    ) -> EngineResult<()> {
        if self.catalog.active_native_id_table_ids().is_empty() {
            return Ok(());
        }
        let allocation_owners = self.catalog.allocation_owners().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                "native_range_v1 catalog is missing its durable allocation-owner map",
            )
        })?;
        shard::validate_native_range_v1_state_with_active_ids(
            connection,
            self.catalog.logical(),
            allocation_owners,
            physical_shard,
            Some(self.catalog.active_native_id_table_ids()),
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

/// Detect the immutable physical shard count without creating or upgrading
/// any database files.
pub(crate) fn detect_shard_count(root: impl AsRef<Path>) -> EngineResult<u16> {
    inspect_manifest_snapshot(root.as_ref(), manifest::detect_shard_count)
}

/// Validate and inspect durable global-index definitions without creating or
/// upgrading any database files.
pub(crate) fn inspect_global_indexes(
    root: impl AsRef<Path>,
) -> EngineResult<Box<[GlobalIndexMetadata]>> {
    inspect_manifest_snapshot(root.as_ref(), manifest::inspect_global_indexes)
}

fn inspect_manifest_snapshot<T>(
    root: &Path,
    inspect: impl FnOnce(&Connection) -> EngineResult<T>,
) -> EngineResult<T> {
    let connection = open_manifest_for_read_only_inspection(root)?;
    connection
        .execute_batch("BEGIN DEFERRED TRANSACTION")
        .map_err(sqlite_error::storage)?;
    match inspect(&connection) {
        Ok(value) => {
            connection
                .execute_batch("COMMIT")
                .map_err(sqlite_error::storage)?;
            Ok(value)
        }
        Err(error) => {
            // Preserve the validation error. A read-only rollback is best-effort
            // cleanup and cannot make the original manifest diagnosis clearer.
            let _ = connection.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

fn open_manifest_for_read_only_inspection(root: &Path) -> EngineResult<Connection> {
    let metadata = fs::symlink_metadata(root).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            EngineError::from_source(
                EngineErrorKind::FailedPrecondition,
                "data directory has no initialized BriskDB manifest; set a shard count to create it",
                error,
            )
        } else {
            sqlite_error::storage_io(error, format!("failed to inspect {}", root.display()))
        }
    })?;
    if !metadata.is_dir() && !metadata.file_type().is_symlink() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("data path {} is not a directory", root.display()),
        ));
    }
    let root = fs::canonicalize(root).map_err(|error| {
        sqlite_error::storage_io(error, format!("failed to resolve {}", root.display()))
    })?;
    if !fs::metadata(&root)
        .map_err(|error| {
            sqlite_error::storage_io(error, format!("failed to inspect {}", root.display()))
        })?
        .is_dir()
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("data path {} is not a directory", root.display()),
        ));
    }
    let manifest_path = root.join("manifest.sqlite");
    match fs::symlink_metadata(&manifest_path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "data directory has no initialized BriskDB manifest; set a shard count to create it",
            ));
        }
        Err(error) => {
            return Err(sqlite_error::storage_io(
                error,
                format!("failed to inspect {}", manifest_path.display()),
            ));
        }
    }
    let connection = open_existing_manifest_read_only(&manifest_path)?;
    configure_manifest_connection(&connection)?;
    Ok(connection)
}

fn declarations_match_catalog(catalog: &Catalog, declarations: &[TableDeclaration]) -> bool {
    if catalog.tables().len() != declarations.len() {
        return false;
    }
    let mut declarations = declarations.iter().collect::<Vec<_>>();
    declarations.sort_by_key(|declaration| (declaration.database_id(), declaration.name()));
    catalog
        .tables()
        .iter()
        .zip(declarations)
        .all(|(table, declaration)| {
            table.database_id() == declaration.database_id()
                && table.name() == declaration.name()
                && table.placement() == declaration.placement()
                && table.generated_id_policy() == declaration.generated_id_policy()
        })
}

fn generated_table_ddl_receipt(
    ddl: &manifest::GeneratedTableDdl,
) -> EngineResult<GeneratedTableDdlReceipt> {
    if ddl.lifecycle() != manifest::GeneratedTableDdlLifecycle::Complete {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "generated-table DDL receipt requires a complete durable bridge",
        ));
    }
    let provisioning_id = ddl.provisioning_id().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "complete generated-table DDL is missing its provisioning identity",
        )
    })?;
    let table_id = ddl.table_id().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "complete generated-table DDL is missing its catalog table identity",
        )
    })?;
    Ok(GeneratedTableDdlReceipt::from_durable_parts(
        ddl.logical_id(),
        ddl.physical_migration_id(),
        provisioning_id,
        table_id,
    ))
}

fn application_table_names(connection: &Connection) -> EngineResult<BTreeSet<String>> {
    let mut statement = connection
        .prepare(
            "SELECT name FROM pragma_table_list
             WHERE schema = 'main' AND type IN ('table', 'virtual')
               AND name NOT GLOB 'sqlite_*'
               AND name <> 'briskdb_shard_metadata'
             ORDER BY name COLLATE BINARY",
        )
        .map_err(sqlite_error::storage)?;
    statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sqlite_error::storage)?
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(sqlite_error::storage)
}

fn application_relation_names(connection: &Connection) -> EngineResult<BTreeSet<String>> {
    let mut statement = connection
        .prepare(
            "SELECT name FROM main.sqlite_schema
             WHERE type IN ('table', 'view')
             ORDER BY name COLLATE BINARY",
        )
        .map_err(sqlite_error::storage)?;
    statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sqlite_error::storage)?
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(sqlite_error::storage)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqliteAffinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
}

fn sqlite_affinity(declared_type: &str) -> SqliteAffinity {
    let declared_type = declared_type.to_ascii_uppercase();
    if declared_type.contains("INT") {
        SqliteAffinity::Integer
    } else if declared_type.contains("CHAR")
        || declared_type.contains("CLOB")
        || declared_type.contains("TEXT")
    {
        SqliteAffinity::Text
    } else if declared_type.contains("BLOB") || declared_type.is_empty() {
        SqliteAffinity::Blob
    } else if declared_type.contains("REAL")
        || declared_type.contains("FLOA")
        || declared_type.contains("DOUB")
    {
        SqliteAffinity::Real
    } else {
        SqliteAffinity::Numeric
    }
}

fn validate_empty_table_declaration(
    connection: &Connection,
    shard_id: u16,
    declaration: &TableDeclaration,
    declarations: &[TableDeclaration],
) -> EngineResult<()> {
    let quoted_table = quote_identifier(declaration.name());
    let has_rows = connection
        .query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM {quoted_table} LIMIT 1)"),
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sqlite_error::storage)?;
    if has_rows != 0 {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "table {} must be empty on physical shard {shard_id} before registration",
                declaration.name()
            ),
        ));
    }

    let TablePlacement::Sharded(shard_key) = declaration.placement() else {
        shard::validate_declared_table_constraints(connection, declaration, declarations)?;
        return Ok(());
    };
    let mut statement = connection
        .prepare(
            "SELECT name, type, \"notnull\", pk, hidden
             FROM pragma_table_xinfo(?1)
             ORDER BY cid",
        )
        .map_err(sqlite_error::storage)?;
    let columns = statement
        .query_map([declaration.name()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)?;
    let Some((_, declared_type, not_null, primary_key, hidden)) = columns
        .iter()
        .find(|(name, _, _, _, _)| name == shard_key.column())
    else {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "shard key {} is missing from table {} on physical shard {shard_id}",
                shard_key.column(),
                declaration.name()
            ),
        ));
    };
    if *hidden != 0
        || !shard_key_is_non_null(
            connection,
            declaration.name(),
            declared_type,
            *not_null,
            *primary_key,
        )?
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "shard key {} on table {} must be a visible physically non-null column",
                shard_key.column(),
                declaration.name()
            ),
        ));
    }
    if let GeneratedIdPolicy::NativeRangeV1 { column } = declaration.generated_id_policy() {
        if !shard::native_generated_column_is_exact(connection, declaration.name(), column)? {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "generated column {column} on table {} must be exactly INTEGER PRIMARY KEY AUTOINCREMENT on physical shard {shard_id}",
                    declaration.name()
                ),
            ));
        }
    }
    if let GeneratedIdPolicy::HiloV1 { column } = declaration.generated_id_policy() {
        if !shard::hilo_generated_column_is_exact(connection, declaration.name(), column)? {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "generated column {column} on table {} must be exactly INTEGER PRIMARY KEY without AUTOINCREMENT on physical shard {shard_id}",
                    declaration.name()
                ),
            ));
        }
    }
    let affinity = sqlite_affinity(declared_type);
    let compatible = matches!(
        (shard_key.key_type(), affinity),
        (ShardKeyType::Int64, SqliteAffinity::Integer)
            | (ShardKeyType::Text, SqliteAffinity::Text)
            | (ShardKeyType::Binary, SqliteAffinity::Blob)
    );
    if !compatible {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "shard key {} on table {} has an incompatible SQLite declared type",
                shard_key.column(),
                declaration.name()
            ),
        ));
    }
    if matches!(shard_key.key_type(), ShardKeyType::Text)
        && !shard::shard_key_uses_binary_collation(
            connection,
            declaration.name(),
            shard_key.column(),
        )?
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "shard key {} on table {} must use SQLite BINARY collation",
                shard_key.column(),
                declaration.name()
            ),
        ));
    }
    shard::validate_declared_table_constraints(connection, declaration, declarations)?;
    Ok(())
}

fn shard_key_is_non_null(
    connection: &Connection,
    table: &str,
    declared_type: &str,
    not_null: i64,
    primary_key: i64,
) -> EngineResult<bool> {
    if not_null != 0 {
        return Ok(true);
    }
    if primary_key == 0 || !declared_type.trim().eq_ignore_ascii_case("INTEGER") {
        return Ok(false);
    }
    let has_primary_key_index = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM pragma_index_list(?1) WHERE origin = 'pk'
             )",
            [table],
            |row| row.get::<_, bool>(0),
        )
        .map_err(sqlite_error::storage)?;
    // An exact INTEGER PRIMARY KEY without a separate primary-key index is the
    // rowid alias. SQLite always materializes a non-null integer for it. Other
    // rowid-table PRIMARY KEY forms retain SQLite's legacy NULL exception.
    Ok(!has_primary_key_index)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
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

pub(super) fn attach_storage_authorizer(connection: &Connection) -> EngineResult<()> {
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
    match action {
        // SQLite reports table-valued PRAGMAs through the authorizer as a
        // PRAGMA action. `table_list` is a read-only schema inventory with no
        // connection-local state, so it is safe for the admin inspector to use
        // without forcing a pooled handle to retire after every page.
        AuthAction::Pragma {
            pragma_name,
            pragma_value,
        } => pragma_value.is_some() || !pragma_name.eq_ignore_ascii_case("table_list"),
        _ => matches!(
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
                | AuthAction::Transaction { .. }
                | AuthAction::Attach { .. }
                | AuthAction::Detach { .. }
                | AuthAction::CreateVtable { .. }
                | AuthAction::DropVtable { .. }
                | AuthAction::Savepoint { .. }
        ),
    }
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

fn startup_requires_exclusive_ownership(path: &Path, requested_shards: u16) -> EngineResult<bool> {
    validate_optional_manifest_file(path)?;
    if !path.exists() {
        return Ok(true);
    }
    let connection = open_existing_manifest(path)?;
    configure_manifest_connection(&connection)?;
    manifest::startup_requires_exclusive_ownership(&connection, requested_shards)
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

fn open_existing_manifest_read_only(path: &Path) -> EngineResult<Connection> {
    validate_existing_manifest_file(path)?;
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_EXRESCODE;
    let open_path = canonical_manifest_open_path(path)?;
    let connection = Connection::open_with_flags(open_path, flags).map_err(|error| {
        sqlite_error::storage(error).context(format!("failed to open {} read-only", path.display()))
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
        process::Command,
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
    fn nonterminal_validation_defers_fail_closed_state_until_completion() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let shard_path = temp.path().join("shards/0000.sqlite");
        Connection::open(&shard_path)
            .unwrap()
            .execute_batch("CREATE TABLE unexpected_drift(id INTEGER PRIMARY KEY)")
            .unwrap();
        let connection = storage.open_unconfigured_shard(0).unwrap();

        let nonterminal = storage
            .validate_unconfigured_shard_nonterminal(&connection, 0)
            .unwrap_err();
        assert_eq!(nonterminal.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(storage.schema_gate_snapshot().state, SchemaGateState::Ready);

        let terminal = storage
            .validate_unconfigured_shard(&connection, 0)
            .unwrap_err();
        assert_eq!(terminal.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(
            storage.schema_gate_snapshot().state,
            SchemaGateState::Degraded
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
                     DROP TABLE briskdb_global_index_parts;
                     DROP TABLE briskdb_global_indexes;
                     DROP TABLE briskdb_generated_table_ddl;
                     DROP TABLE briskdb_hilo_leases;
                     DROP TABLE briskdb_table_provisioning_declarations;
                     DROP TABLE briskdb_table_provisioning;
                     DROP TABLE briskdb_generated_ids;
                     DROP INDEX briskdb_one_active_owner_per_shard;
                     DROP TABLE briskdb_allocation_owners;
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
                 DROP TABLE briskdb_global_index_parts;
                 DROP TABLE briskdb_global_indexes;
                 DROP TABLE briskdb_generated_table_ddl;
                 DROP TABLE briskdb_hilo_leases;
                 DROP TABLE briskdb_table_provisioning_declarations;
                 DROP TABLE briskdb_table_provisioning;
                 DROP TABLE briskdb_generated_ids;
                 DROP INDEX briskdb_one_active_owner_per_shard;
                 DROP TABLE briskdb_allocation_owners;
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
                 DROP TABLE briskdb_global_index_parts;
                 DROP TABLE briskdb_global_indexes;
                 DROP TABLE briskdb_generated_table_ddl;
                 DROP TABLE briskdb_hilo_leases;
                 DROP TABLE briskdb_table_provisioning_declarations;
                 DROP TABLE briskdb_table_provisioning;
                 DROP TABLE briskdb_generated_ids;
                 DROP INDEX briskdb_one_active_owner_per_shard;
                 DROP TABLE briskdb_allocation_owners;
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
            "manifest semantic checksum does not match its authoritative contents"
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

    fn registered_table_declarations(database: &Database) -> Vec<TableDeclaration> {
        let logical_database = database.catalog().default_database().id();
        vec![
            TableDeclaration::global(logical_database, "countries").unwrap(),
            TableDeclaration::sharded(
                logical_database,
                "events",
                crate::core::ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ]
    }

    fn create_registered_table_schema(database: &Database) {
        database
            .broadcast(
                "CREATE TABLE events (
                    id INTEGER NOT NULL,
                    tenant_id TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    PRIMARY KEY (tenant_id, id)
                 );
                 CREATE TABLE countries (
                    code TEXT PRIMARY KEY,
                    name TEXT NOT NULL
                 );",
            )
            .unwrap();
    }

    fn native_events_declaration(database_id: crate::core::LogicalDatabaseId) -> TableDeclaration {
        TableDeclaration::sharded(
            database_id,
            "events",
            crate::core::ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
        )
        .unwrap()
        .with_generated_id_policy(crate::core::GeneratedIdPolicy::native_range_v1("id").unwrap())
        .unwrap()
    }

    fn create_registered_native_storage(root: &Path, shard_count: u16) -> Storage {
        let mut storage = Storage::open(root, shard_count).unwrap();
        let mut migration = storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        storage
            .apply_schema_migration(
                "CREATE TABLE events (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     payload BLOB
                 )",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();
        let declaration =
            native_events_declaration(storage.logical_catalog().default_database().id());
        storage.register_tables(vec![declaration]).unwrap();
        storage
    }

    fn create_registered_hilo_storage(root: &Path, shard_count: u16) -> Storage {
        let mut storage = Storage::open(root, shard_count).unwrap();
        let mut migration = storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        storage
            .apply_schema_migration(
                "CREATE TABLE events (
                     id INTEGER PRIMARY KEY,
                     payload BLOB
                 ) STRICT;",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();
        let declaration = TableDeclaration::sharded(
            storage.logical_catalog().default_database().id(),
            "events",
            crate::core::ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
        )
        .unwrap()
        .with_generated_id_policy(crate::core::GeneratedIdPolicy::hilo_v1("id").unwrap())
        .unwrap();
        storage.register_tables(vec![declaration]).unwrap();
        storage
    }

    fn hilo_events_table_id(storage: &Storage) -> TableId {
        storage
            .logical_catalog()
            .table("default", "events")
            .unwrap()
            .unwrap()
            .id()
    }

    #[test]
    fn degradation_preserves_active_native_provisioning_and_startup_does_not_replay_it() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let mut migration = storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        storage
            .apply_schema_migration(
                "CREATE TABLE events (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     payload BLOB
                 )",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();

        let declaration =
            native_events_declaration(storage.logical_catalog().default_database().id());
        let committed_schema_digest = storage
            .schema_coordination
            .committed_schema_digest()
            .unwrap();
        let mut manifest_connection =
            open_existing_manifest(&temp.path().join("manifest.sqlite")).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        let active = match manifest::begin_native_table_provisioning(
            &mut manifest_connection,
            2,
            vec![declaration],
            committed_schema_digest,
            || {},
        )
        .unwrap()
        {
            manifest::NativeTableProvisioningClassification::Active(active) => active,
            classification => panic!("unexpected classification: {classification:?}"),
        };
        let provisioning_id = active.provisioning_id();

        manifest::mark_degraded(&mut manifest_connection, 2, &storage.shard_layout).unwrap();
        assert_eq!(
            manifest::current_integrity(&manifest_connection, 2)
                .unwrap()
                .state(),
            manifest::DatabaseIntegrityState::Degraded
        );
        assert_eq!(
            manifest_connection
                .query_row(
                    "SELECT provisioning_id, next_shard
                     FROM briskdb_table_provisioning
                     WHERE singleton = 1",
                    [],
                    |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, u16>(1)?)),
                )
                .unwrap(),
            (provisioning_id.to_vec(), 0)
        );
        assert_eq!(
            manifest_connection
                .query_row(
                    "SELECT COUNT(*) FROM briskdb_table_provisioning_declarations",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        drop(manifest_connection);
        drop(storage);

        for shard in 0..2 {
            assert_eq!(
                Connection::open(shard_file(temp.path(), shard))
                    .unwrap()
                    .query_row(
                        "SELECT COUNT(*) FROM sqlite_sequence WHERE name = 'events'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                0
            );
        }

        let reopen_error = Storage::open(temp.path(), 2).unwrap_err();
        assert_eq!(reopen_error.kind(), EngineErrorKind::DataCorruption);
        assert!(reopen_error.to_string().contains("persistently degraded"));

        let manifest_connection =
            open_existing_manifest(&temp.path().join("manifest.sqlite")).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        assert_eq!(
            manifest::current_integrity(&manifest_connection, 2)
                .unwrap()
                .state(),
            manifest::DatabaseIntegrityState::Degraded
        );
        assert_eq!(
            manifest_connection
                .query_row(
                    "SELECT provisioning_id, next_shard
                     FROM briskdb_table_provisioning
                     WHERE singleton = 1",
                    [],
                    |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, u16>(1)?)),
                )
                .unwrap(),
            (provisioning_id.to_vec(), 0)
        );
        assert_eq!(
            manifest_connection
                .query_row(
                    "SELECT COUNT(*) FROM briskdb_table_provisioning_declarations",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        drop(manifest_connection);
        for shard in 0..2 {
            assert_eq!(
                Connection::open(shard_file(temp.path(), shard))
                    .unwrap()
                    .query_row(
                        "SELECT COUNT(*) FROM sqlite_sequence WHERE name = 'events'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                0
            );
        }
    }

    #[test]
    fn startup_recovers_every_native_provisioning_commit_boundary() {
        use crate::core::generated_id::native_range_v1_sequence_floor;

        // `seeded` may exceed the acknowledged prefix by one to model the
        // crash window after a shard commit and before journal advancement.
        for (acknowledged, seeded) in [(0_u16, 0_u16), (0, 1), (2, 3), (4, 4)] {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 4).unwrap();
            let mut migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            storage
                .apply_schema_migration(
                    "CREATE TABLE events (
                         id INTEGER PRIMARY KEY AUTOINCREMENT,
                         payload BLOB
                     )",
                    &mut migration,
                    None,
                )
                .unwrap();
            migration.publish_ready().unwrap();
            let declaration =
                native_events_declaration(storage.logical_catalog().default_database().id());
            let committed_schema_digest = storage
                .schema_coordination
                .committed_schema_digest()
                .unwrap();
            let mut manifest_connection =
                open_existing_manifest(&temp.path().join("manifest.sqlite")).unwrap();
            configure_manifest_connection(&manifest_connection).unwrap();
            let mut provisioning = match manifest::begin_native_table_provisioning(
                &mut manifest_connection,
                4,
                vec![declaration],
                committed_schema_digest,
                || {},
            )
            .unwrap()
            {
                manifest::NativeTableProvisioningClassification::Active(provisioning) => {
                    provisioning
                }
                classification => panic!("unexpected classification: {classification:?}"),
            };
            for shard in 0..seeded {
                storage
                    .seed_native_range_v1_sequences_on_shard(provisioning.declarations(), shard)
                    .unwrap();
                if shard < acknowledged {
                    provisioning = manifest::advance_native_table_provisioning(
                        &mut manifest_connection,
                        4,
                        &provisioning,
                        shard + 1,
                    )
                    .unwrap();
                }
            }
            assert_eq!(provisioning.next_shard(), acknowledged);
            drop(manifest_connection);
            drop(storage);

            let reopened = Storage::open(temp.path(), 4).unwrap();
            let table = reopened
                .logical_catalog()
                .table("default", "events")
                .unwrap()
                .unwrap();
            assert!(reopened.native_id_policy_is_active(table.id()));
            for shard in 0..4 {
                let owner = reopened
                    .allocation_owner_map()
                    .unwrap()
                    .owner_for_physical_shard(shard)
                    .unwrap();
                assert_eq!(
                    reopened
                        .open_shard(shard)
                        .unwrap()
                        .query_row(
                            "SELECT seq FROM sqlite_sequence WHERE name = 'events'",
                            [],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    native_range_v1_sequence_floor(owner)
                );
            }
            let mut manifest_connection =
                open_existing_manifest(&temp.path().join("manifest.sqlite")).unwrap();
            assert!(
                manifest::classify_native_table_provisioning(
                    &mut manifest_connection,
                    4,
                    vec![native_events_declaration(
                        reopened.logical_catalog().default_database().id(),
                    )],
                    reopened
                        .schema_coordination
                        .committed_schema_digest()
                        .unwrap(),
                )
                .unwrap()
                    == manifest::NativeTableProvisioningClassification::Complete
            );
            drop(manifest_connection);
            drop(reopened);

            let second_reopen = Storage::open(temp.path(), 4).unwrap();
            let table = second_reopen
                .logical_catalog()
                .table("default", "events")
                .unwrap()
                .unwrap();
            assert!(second_reopen.native_id_policy_is_active(table.id()));
        }
    }

    #[test]
    fn finalized_native_provisioning_stays_pending_until_stale_handle_closes() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let mut schema = storage.begin_schema_migration().unwrap();
        schema.wait_for_quiescence_blocking();
        storage
            .apply_schema_migration(
                "CREATE TABLE events (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     payload BLOB
                 )",
                &mut schema,
                None,
            )
            .unwrap();
        schema.publish_ready().unwrap();

        let declaration =
            native_events_declaration(storage.logical_catalog().default_database().id());
        let committed_schema_digest = storage
            .schema_coordination
            .committed_schema_digest()
            .unwrap();
        let mut registration = storage.begin_schema_migration().unwrap();
        registration.wait_for_quiescence_blocking();
        let mut manifest_connection =
            open_existing_manifest(&temp.path().join("manifest.sqlite")).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        let mut provisioning = match manifest::begin_native_table_provisioning(
            &mut manifest_connection,
            2,
            vec![declaration],
            committed_schema_digest,
            || registration.mark_pending_on_drop(),
        )
        .unwrap()
        {
            manifest::NativeTableProvisioningClassification::Active(provisioning) => provisioning,
            classification => panic!("unexpected classification: {classification:?}"),
        };
        while provisioning.next_shard() < provisioning.shard_count() {
            let shard = provisioning.next_shard();
            storage
                .seed_native_range_v1_sequences_on_shard(provisioning.declarations(), shard)
                .unwrap();
            provisioning = manifest::advance_native_table_provisioning(
                &mut manifest_connection,
                2,
                &provisioning,
                shard + 1,
            )
            .unwrap();
        }

        // Simulate process loss after manifest finalization but before the
        // returned catalog can be published into the live Storage handle.
        let durable_replacement = manifest::finalize_native_table_provisioning(
            &mut manifest_connection,
            2,
            &provisioning,
            || registration.mark_pending_on_drop(),
        )
        .unwrap();
        assert_eq!(durable_replacement.active_native_id_table_ids().len(), 1);
        drop(durable_replacement);
        drop(manifest_connection);
        drop(registration);

        assert!(storage.logical_catalog().tables().is_empty());
        assert_eq!(
            storage.schema_gate_snapshot().state,
            SchemaGateState::Pending
        );
        assert_eq!(
            storage.enter_schema_operation().unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        let conflict = Storage::open(temp.path(), 2).unwrap_err();
        assert_eq!(conflict.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(
            storage.schema_gate_snapshot().state,
            SchemaGateState::Pending
        );
        drop(storage);

        let reopened = Storage::open(temp.path(), 2).unwrap();
        let table = reopened
            .logical_catalog()
            .table("default", "events")
            .unwrap()
            .unwrap();
        assert!(reopened.native_id_policy_is_active(table.id()));
        assert_eq!(
            reopened.schema_gate_snapshot().state,
            SchemaGateState::Ready
        );
    }

    #[test]
    fn table_registration_is_authoritative_persistent_and_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 3).unwrap();
        create_registered_table_schema(&database);
        let declarations = registered_table_declarations(&database);

        database.register_tables(declarations.clone()).unwrap();
        let tables = database.catalog().tables().to_vec();
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name(), "countries");
        assert_eq!(tables[0].placement(), &TablePlacement::Global);
        assert_eq!(tables[1].name(), "events");
        assert_eq!(tables[1].placement(), declarations[1].placement());

        database
            .execute(
                "tenant-one",
                "INSERT INTO events (id, tenant_id, payload) VALUES (?1, ?2, ?3)",
                &[
                    crate::core::Value::from(1_i64),
                    crate::core::Value::from("tenant-one"),
                    crate::core::Value::from(vec![1_u8, 2, 3]),
                ],
            )
            .unwrap();
        database.register_tables(declarations).unwrap();
        drop(database);

        let reopened = Database::open(temp.path(), 3).unwrap();
        assert_eq!(reopened.catalog().tables(), tables.as_slice());
        assert_eq!(
            reopened
                .catalog()
                .table("default", "events")
                .unwrap()
                .unwrap()
                .placement(),
            &TablePlacement::Sharded(
                crate::core::ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap()
            )
        );
    }

    #[test]
    fn native_generated_id_policy_registers_and_reopens_exactly() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 3).unwrap();
        database
            .broadcast(
                "CREATE TABLE events (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     payload BLOB
                 )",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        let policy = crate::core::GeneratedIdPolicy::native_range_v1("id").unwrap();
        let declaration = TableDeclaration::sharded(
            logical_database,
            "events",
            crate::core::ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
        )
        .unwrap()
        .with_generated_id_policy(policy.clone())
        .unwrap();

        database.register_tables(vec![declaration.clone()]).unwrap();
        assert_eq!(
            database
                .catalog()
                .table("default", "events")
                .unwrap()
                .unwrap()
                .generated_id_policy(),
            &policy
        );
        database.register_tables(vec![declaration]).unwrap();
        drop(database);

        let reopened = Database::open(temp.path(), 3).unwrap();
        let reopened_table = reopened
            .catalog()
            .table("default", "events")
            .unwrap()
            .unwrap();
        assert_eq!(reopened_table.generated_id_policy(), &policy);
        drop(reopened);

        let reopened = Storage::open(temp.path(), 3).unwrap();
        let reopened_table = reopened
            .logical_catalog()
            .table("default", "events")
            .unwrap()
            .unwrap();
        assert!(reopened.native_id_policy_is_active(reopened_table.id()));
        for shard in 0..3 {
            reopened.open_shard(shard).unwrap();
        }
    }

    #[test]
    fn file_backed_v9_native_policy_upgrades_inactive_without_autoincrement() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let mut migration = storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        storage
            .apply_schema_migration(
                "CREATE TABLE events (
                     id INTEGER PRIMARY KEY,
                     payload BLOB
                 )",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();
        let declaration =
            native_events_declaration(storage.logical_catalog().default_database().id());
        let mut manifest_connection =
            open_existing_manifest(&temp.path().join("manifest.sqlite")).unwrap();
        manifest::install_v9_native_catalog_for_test(
            &mut manifest_connection,
            2,
            std::slice::from_ref(&declaration),
        )
        .unwrap();
        manifest::inspect_with_v9_plan_for_test(&manifest_connection, 2).unwrap();
        drop(manifest_connection);
        drop(storage);

        let mut upgraded = Storage::open(temp.path(), 2).unwrap();
        let table = upgraded
            .logical_catalog()
            .table("default", "events")
            .unwrap()
            .unwrap();
        assert_eq!(
            table.generated_id_policy(),
            declaration.generated_id_policy()
        );
        assert!(!upgraded.native_id_policy_is_active(table.id()));
        for shard in 0..2 {
            let connection = upgraded.open_shard(shard).unwrap();
            assert!(
                !connection
                    .query_row(
                        "SELECT EXISTS(
                         SELECT 1 FROM sqlite_schema
                         WHERE type = 'table' AND name = 'sqlite_sequence'
                     )",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap()
            );
        }

        // Activation checks the physical shape before publishing a journal;
        // the old v9 policy remains readable and ordinary admission remains ready.
        let error = upgraded.register_tables(vec![declaration]).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(error.diagnostic().contains("AUTOINCREMENT"));
        assert_eq!(
            upgraded.schema_gate_snapshot().state,
            SchemaGateState::Ready
        );

        let manifest_connection =
            open_existing_manifest(&temp.path().join("manifest.sqlite")).unwrap();
        let identity_before = manifest_connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .unwrap();
        let root_before = manifest_connection
            .query_row(
                "SELECT manifest_digest FROM briskdb_integrity WHERE singleton = 1",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .unwrap();
        let error = manifest::inspect_with_v9_plan_for_test(&manifest_connection, 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(
            manifest_connection
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            identity_before
        );
        assert_eq!(
            manifest_connection
                .query_row(
                    "SELECT manifest_digest FROM briskdb_integrity WHERE singleton = 1",
                    [],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .unwrap(),
            root_before
        );
        drop(manifest_connection);
        drop(upgraded);

        let reopened = Storage::open(temp.path(), 2).unwrap();
        let table = reopened
            .logical_catalog()
            .table("default", "events")
            .unwrap()
            .unwrap();
        assert!(!reopened.native_id_policy_is_active(table.id()));

        #[cfg(feature = "experimental-vtab")]
        {
            let mut coordinator = WriteCoordinator::open(reopened.clone()).unwrap();
            let error = coordinator
                .execute_generated_dml_auto(
                    "INSERT INTO events (payload) VALUES (x'01')",
                    [],
                    table.id().get(),
                )
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
            assert_eq!(
                reopened
                    .open_shard(0)
                    .unwrap()
                    .query_row("SELECT COUNT(*) FROM events", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                0
            );
        }
    }

    #[test]
    fn native_generated_id_policy_requires_exact_autoincrement_storage() {
        for schema in [
            "CREATE TABLE events (id INTEGER PRIMARY KEY, payload BLOB)",
            "CREATE TABLE events (id INTEGER NOT NULL UNIQUE, payload BLOB)",
            "CREATE TABLE events (
                 id INTEGER PRIMARY KEY AUTOINCREMENT DEFAULT 7,
                 payload BLOB
             )",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let mut database = Database::open(temp.path(), 2).unwrap();
            database.broadcast(schema).unwrap();
            let declaration = native_events_declaration(database.catalog().default_database().id());

            let error = database.register_tables(vec![declaration]).unwrap_err();
            assert_eq!(
                error.kind(),
                EngineErrorKind::FailedPrecondition,
                "{schema}"
            );
            assert!(
                error
                    .diagnostic()
                    .contains("INTEGER PRIMARY KEY AUTOINCREMENT"),
                "{}",
                error.diagnostic()
            );
            assert!(database.catalog().tables().is_empty());
        }
    }

    #[test]
    fn native_sequence_provisioning_is_idempotent_for_2_4_8_and_10_shards() {
        use crate::core::generated_id::{
            AllocationOwnerSlot, native_range_v1_first_id, native_range_v1_sequence_floor,
        };

        for shard_count in [2, 4, 8, 10] {
            let temp = tempfile::tempdir().unwrap();
            let mut storage = Storage::open(temp.path(), shard_count).unwrap();
            let mut migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            storage
                .apply_schema_migration(
                    "CREATE TABLE events (
                         id INTEGER PRIMARY KEY AUTOINCREMENT,
                         payload BLOB
                     )",
                    &mut migration,
                    None,
                )
                .unwrap();
            migration.publish_ready().unwrap();
            let declaration =
                native_events_declaration(storage.logical_catalog().default_database().id());

            storage
                .seed_native_range_v1_sequences(std::slice::from_ref(&declaration))
                .unwrap();
            storage
                .seed_native_range_v1_sequences(std::slice::from_ref(&declaration))
                .unwrap();
            storage.register_tables(vec![declaration.clone()]).unwrap();
            storage.register_tables(vec![declaration]).unwrap();

            for shard_id in 0..shard_count {
                let owner = AllocationOwnerSlot::new(shard_id).unwrap();
                let connection = Connection::open(shard_file(temp.path(), shard_id)).unwrap();
                let rows = connection
                    .query_row(
                        "SELECT COUNT(*), MIN(seq), MAX(seq)
                         FROM sqlite_sequence WHERE name = 'events'",
                        [],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, i64>(2)?,
                            ))
                        },
                    )
                    .unwrap();
                assert_eq!(
                    rows,
                    (
                        1,
                        native_range_v1_sequence_floor(owner),
                        native_range_v1_sequence_floor(owner)
                    ),
                    "shard count {shard_count}, shard {shard_id}"
                );
                let allocated = connection
                    .query_row(
                        "INSERT INTO events (payload) VALUES (x'01') RETURNING id",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap();
                assert_eq!(allocated, native_range_v1_first_id(owner));
            }
            drop(storage);
            Storage::open(temp.path(), shard_count).unwrap();
        }
    }

    #[test]
    fn native_sequence_provisioning_never_lowers_a_deleted_same_owner_high_water() {
        use crate::core::generated_id::{AllocationOwnerSlot, native_range_v1_first_id};

        let temp = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(temp.path(), 2).unwrap();
        let mut migration = storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        storage
            .apply_schema_migration(
                "CREATE TABLE events (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     payload BLOB
                 )",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();

        let owner = AllocationOwnerSlot::new(0).unwrap();
        let committed_high_water = native_range_v1_first_id(owner) + 24;
        let connection = storage.open_shard(0).unwrap();
        connection
            .execute(
                "INSERT INTO events (id, payload) VALUES (?1, x'01')",
                [committed_high_water],
            )
            .unwrap();
        connection
            .execute("DELETE FROM events WHERE id = ?1", [committed_high_water])
            .unwrap();
        drop(connection);

        let declaration =
            native_events_declaration(storage.logical_catalog().default_database().id());
        storage.register_tables(vec![declaration]).unwrap();

        let connection = storage.open_shard(0).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT seq FROM sqlite_sequence WHERE name = 'events'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            committed_high_water
        );
        let next = connection
            .query_row(
                "INSERT INTO events (payload) VALUES (x'02') RETURNING id",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(next, committed_high_water + 1);
    }

    #[test]
    fn native_sequence_provisioning_rejects_a_conflicting_owner_high_water() {
        use crate::core::generated_id::{AllocationOwnerSlot, native_range_v1_first_id};

        let temp = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(temp.path(), 2).unwrap();
        let mut migration = storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        storage
            .apply_schema_migration(
                "CREATE TABLE events (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     payload BLOB
                 )",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();

        let conflicting = native_range_v1_first_id(AllocationOwnerSlot::new(0).unwrap()) + 9;
        let connection = storage.open_shard(1).unwrap();
        connection
            .execute(
                "INSERT INTO events (id, payload) VALUES (?1, x'01')",
                [conflicting],
            )
            .unwrap();
        connection
            .execute("DELETE FROM events WHERE id = ?1", [conflicting])
            .unwrap();
        drop(connection);

        let declaration =
            native_events_declaration(storage.logical_catalog().default_database().id());
        let error = storage.register_tables(vec![declaration]).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(
            error
                .diagnostic()
                .contains("conflicting native allocation-owner")
        );
        assert!(storage.logical_catalog().tables().is_empty());
        assert_eq!(
            storage.schema_gate_snapshot().state,
            SchemaGateState::Pending
        );
        assert_eq!(
            storage.enter_schema_operation().unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );

        let connection = Connection::open(shard_file(temp.path(), 1)).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT seq FROM sqlite_sequence WHERE name = 'events'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            conflicting
        );
    }

    #[test]
    fn native_allocator_state_tampering_fails_closed() {
        use crate::core::generated_id::{
            AllocationOwnerSlot, native_range_v1_first_id, native_range_v1_sequence_ceiling,
            native_range_v1_sequence_floor,
        };

        enum Tamper {
            Missing,
            Duplicate,
            NonInteger,
            BelowFloor,
            AboveCeiling,
            SequenceBehindRow,
            ForeignOwnerRow,
        }

        for tamper in [
            Tamper::Missing,
            Tamper::Duplicate,
            Tamper::NonInteger,
            Tamper::BelowFloor,
            Tamper::AboveCeiling,
            Tamper::SequenceBehindRow,
            Tamper::ForeignOwnerRow,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let storage = create_registered_native_storage(temp.path(), 2);
            let physical_shard = if matches!(tamper, Tamper::ForeignOwnerRow) {
                1
            } else {
                0
            };
            let owner = AllocationOwnerSlot::new(physical_shard).unwrap();
            let floor = native_range_v1_sequence_floor(owner);
            let ceiling = native_range_v1_sequence_ceiling(owner);
            let connection = Connection::open(shard_file(temp.path(), physical_shard)).unwrap();
            match tamper {
                Tamper::Missing => {
                    connection
                        .execute("DELETE FROM sqlite_sequence WHERE name = 'events'", [])
                        .unwrap();
                }
                Tamper::Duplicate => {
                    connection
                        .execute(
                            "INSERT INTO sqlite_sequence(name, seq) VALUES ('events', ?1)",
                            [floor],
                        )
                        .unwrap();
                }
                Tamper::NonInteger => {
                    connection
                        .execute(
                            "UPDATE sqlite_sequence SET seq = 'invalid' WHERE name = 'events'",
                            [],
                        )
                        .unwrap();
                }
                Tamper::BelowFloor => {
                    connection
                        .execute(
                            "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'events'",
                            [floor - 1],
                        )
                        .unwrap();
                }
                Tamper::AboveCeiling => {
                    connection
                        .execute(
                            "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'events'",
                            [ceiling + 1],
                        )
                        .unwrap();
                }
                Tamper::SequenceBehindRow => {
                    connection
                        .execute(
                            "INSERT INTO events (id, payload) VALUES (?1, x'01')",
                            [native_range_v1_first_id(owner) + 5],
                        )
                        .unwrap();
                    connection
                        .execute(
                            "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'events'",
                            [floor],
                        )
                        .unwrap();
                }
                Tamper::ForeignOwnerRow => {
                    let foreign = native_range_v1_first_id(AllocationOwnerSlot::new(0).unwrap());
                    connection
                        .execute(
                            "INSERT INTO events (id, payload) VALUES (?1, x'01')",
                            [foreign],
                        )
                        .unwrap();
                    connection
                        .execute(
                            "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'events'",
                            [floor],
                        )
                        .unwrap();
                }
            }
            drop(connection);

            let error = storage.open_shard(physical_shard).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
            assert!(error.diagnostic().contains("native_range_v1"));
        }
    }

    #[test]
    fn native_allocator_accepts_an_exhausted_owner_at_its_exact_ceiling() {
        use crate::core::generated_id::{AllocationOwnerSlot, native_range_v1_sequence_ceiling};

        let temp = tempfile::tempdir().unwrap();
        let storage = create_registered_native_storage(temp.path(), 2);
        let ceiling = native_range_v1_sequence_ceiling(AllocationOwnerSlot::new(0).unwrap());
        let connection = Connection::open(shard_file(temp.path(), 0)).unwrap();
        connection
            .execute(
                "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'events'",
                [ceiling],
            )
            .unwrap();
        drop(connection);

        storage.open_shard(0).unwrap();
    }

    #[cfg(feature = "experimental-vtab")]
    #[test]
    fn retired_owner_rows_survive_reopen_while_new_allocation_uses_replacement_owner() {
        use crate::core::generated_id::{
            AllocationOwnerSlot, NativeRangeV1Id, native_range_v1_first_id,
            native_range_v1_sequence_floor,
        };

        let temp = tempfile::tempdir().unwrap();
        let storage = create_registered_native_storage(temp.path(), 2);
        let old_owner = storage
            .allocation_owner_map()
            .unwrap()
            .owner_for_physical_shard(0)
            .unwrap();
        let old_id = NativeRangeV1Id::new(old_owner, 5).unwrap().encode();
        storage
            .open_shard(0)
            .unwrap()
            .execute(
                "INSERT INTO events (id, payload) VALUES (?1, x'6f6c64')",
                [old_id],
            )
            .unwrap();
        drop(storage);

        let replacement_owner = AllocationOwnerSlot::new(100).unwrap();
        let mut manifest_connection =
            open_existing_manifest(&temp.path().join("manifest.sqlite")).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        manifest::replace_allocation_owner_for_test(
            &mut manifest_connection,
            2,
            old_owner.get(),
            replacement_owner.get(),
            0,
        )
        .unwrap();
        drop(manifest_connection);
        Connection::open(shard_file(temp.path(), 0))
            .unwrap()
            .execute(
                "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'events'",
                [native_range_v1_sequence_floor(replacement_owner)],
            )
            .unwrap();

        let storage = Storage::open(temp.path(), 2).unwrap();
        let owners = storage.allocation_owner_map().unwrap();
        assert_eq!(owners.physical_shard(old_owner), Some(0));
        assert!(!owners.owner_is_active(old_owner));
        assert_eq!(owners.owner_for_physical_shard(0), Some(replacement_owner));
        for shard in 0..2 {
            storage.open_shard(shard).unwrap();
        }

        let reader = sharded_vtab::ReadCoordinator::open(storage.clone()).unwrap();
        assert_eq!(
            reader
                .connection()
                .query_row(
                    "SELECT payload FROM events WHERE id = ?1",
                    [old_id],
                    |row| { row.get::<_, Vec<u8>>(0) }
                )
                .unwrap(),
            b"old"
        );
        drop(reader);

        let retired_candidate = NativeRangeV1Id::new(old_owner, 6).unwrap().encode();
        let error = WriteCoordinator::open(storage.clone())
            .unwrap()
            .execute_dml(
                "INSERT INTO events (id, payload) VALUES (?1, x'6e6577')",
                [retired_candidate],
            )
            .unwrap_err();
        // SQLite surfaces an xUpdate module rejection as SQLITE_ERROR; the
        // shared planner preserves FailedPrecondition before this callback.
        assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
        assert!(error.diagnostic().contains("retired allocation owner"));

        let table_id = storage
            .logical_catalog()
            .table("default", "events")
            .unwrap()
            .unwrap()
            .id()
            .get();
        let generated = WriteCoordinator::open(storage.clone())
            .unwrap()
            .execute_generated_dml(
                "INSERT INTO events (payload) VALUES (x'6e6577')",
                [],
                table_id,
                0,
            )
            .unwrap();
        assert_eq!(generated.shard(), Some(0));
        assert_eq!(
            generated.generated_key().unwrap().value,
            crate::core::Value::Int64(native_range_v1_first_id(replacement_owner))
        );

        let deleted = WriteCoordinator::open(storage.clone())
            .unwrap()
            .execute_dml("DELETE FROM events WHERE id = ?1", [old_id])
            .unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        drop(storage);

        let reopened = Storage::open(temp.path(), 2).unwrap();
        assert_eq!(
            reopened
                .open_shard(0)
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE id = ?1",
                    [old_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            reopened
                .allocation_owner_map()
                .unwrap()
                .physical_shard(old_owner),
            Some(0)
        );
    }

    #[cfg(feature = "experimental-vtab")]
    #[test]
    fn lower_owner_replacement_is_rejected_after_a_deleted_committed_high_water() {
        use crate::core::generated_id::{
            AllocationOwnerSlot, NativeRangeV1Id, native_range_v1_sequence_floor,
        };

        let temp = tempfile::tempdir().unwrap();
        let storage = create_registered_native_storage(temp.path(), 2);
        let original_owner = storage
            .allocation_owner_map()
            .unwrap()
            .owner_for_physical_shard(0)
            .unwrap();
        drop(storage);

        let active_owner = AllocationOwnerSlot::new(100).unwrap();
        let mut manifest_connection =
            open_existing_manifest(&temp.path().join("manifest.sqlite")).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        manifest::replace_allocation_owner_for_test(
            &mut manifest_connection,
            2,
            original_owner.get(),
            active_owner.get(),
            0,
        )
        .unwrap();
        drop(manifest_connection);

        Connection::open(shard_file(temp.path(), 0))
            .unwrap()
            .execute(
                "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'events'",
                [native_range_v1_sequence_floor(active_owner)],
            )
            .unwrap();

        let storage = Storage::open(temp.path(), 2).unwrap();
        let connection = storage.open_shard(0).unwrap();
        let deleted_high_water = connection
            .query_row(
                "INSERT INTO events (payload) VALUES (x'01') RETURNING id",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(
            NativeRangeV1Id::decode(deleted_high_water).unwrap().owner(),
            active_owner
        );
        connection
            .execute("DELETE FROM events WHERE id = ?1", [deleted_high_water])
            .unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE id = ?1",
                    [deleted_high_water],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT seq FROM sqlite_sequence WHERE name = 'events'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            deleted_high_water
        );
        drop(connection);
        drop(storage);

        let mut manifest_connection =
            open_existing_manifest(&temp.path().join("manifest.sqlite")).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        let error = manifest::replace_allocation_owner_for_test(
            &mut manifest_connection,
            2,
            active_owner.get(),
            50,
            0,
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
        assert!(error.diagnostic().contains("must be greater"));
        drop(manifest_connection);

        let reopened = Storage::open(temp.path(), 2).unwrap();
        assert_eq!(
            reopened
                .allocation_owner_map()
                .unwrap()
                .owner_for_physical_shard(0),
            Some(active_owner)
        );
        let connection = reopened.open_shard(0).unwrap();
        let next = connection
            .query_row(
                "INSERT INTO events (payload) VALUES (x'02') RETURNING id",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(next, deleted_high_water + 1);
        assert_eq!(NativeRangeV1Id::decode(next).unwrap().owner(), active_owner);
    }

    #[test]
    fn native_generated_id_policy_rejects_a_nullable_physical_key() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 2).unwrap();
        database
            .broadcast("CREATE TABLE events (id INTEGER, payload BLOB)")
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        let declaration = TableDeclaration::sharded(
            logical_database,
            "events",
            crate::core::ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
        )
        .unwrap()
        .with_generated_id_policy(crate::core::GeneratedIdPolicy::native_range_v1("id").unwrap())
        .unwrap();

        let error = database.register_tables(vec![declaration]).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(database.catalog().tables().is_empty());
    }

    #[test]
    fn table_registration_rejects_incomplete_or_invalid_physical_metadata_atomically() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 2).unwrap();
        create_registered_table_schema(&database);
        let logical_database = database.catalog().default_database().id();

        let incomplete = vec![
            TableDeclaration::sharded(
                logical_database,
                "events",
                crate::core::ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ];
        assert_eq!(
            database.register_tables(incomplete).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert!(database.catalog().tables().is_empty());

        let wrong_type = vec![
            TableDeclaration::global(logical_database, "countries").unwrap(),
            TableDeclaration::sharded(
                logical_database,
                "events",
                crate::core::ShardKeyMetadata::new("tenant_id", ShardKeyType::Int64).unwrap(),
            )
            .unwrap(),
        ];
        assert_eq!(
            database.register_tables(wrong_type).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert!(database.catalog().tables().is_empty());

        let missing_key = vec![
            TableDeclaration::global(logical_database, "countries").unwrap(),
            TableDeclaration::sharded(
                logical_database,
                "events",
                crate::core::ShardKeyMetadata::new("missing_key", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ];
        assert_eq!(
            database.register_tables(missing_key).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert!(database.catalog().tables().is_empty());

        database
            .broadcast("CREATE VIEW Audit_Catalog AS SELECT 1 AS id")
            .unwrap();
        let mut catalog_shadow = registered_table_declarations(&database);
        catalog_shadow.push(TableDeclaration::catalog(logical_database, "audit_catalog").unwrap());
        assert_eq!(
            database.register_tables(catalog_shadow).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert!(database.catalog().tables().is_empty());
        database.broadcast("DROP VIEW Audit_Catalog").unwrap();

        database
            .register_tables(registered_table_declarations(&database))
            .unwrap();
        drop(database);
        assert_eq!(
            Database::open(temp.path(), 2)
                .unwrap()
                .catalog()
                .tables()
                .len(),
            2
        );
    }

    #[test]
    fn first_table_registration_requires_exclusive_database_ownership() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 2).unwrap();
        create_registered_table_schema(&database);
        let observer = Database::open(temp.path(), 2).unwrap();
        let declarations = registered_table_declarations(&database);

        assert_eq!(
            database
                .register_tables(declarations.clone())
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert!(database.catalog().tables().is_empty());
        assert!(observer.catalog().tables().is_empty());

        drop(observer);
        database.register_tables(declarations).unwrap();
        assert_eq!(database.catalog().tables().len(), 2);
    }

    #[test]
    fn malformed_or_nonempty_registration_changes_no_catalog_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 2).unwrap();
        create_registered_table_schema(&database);
        let manifest_path = temp.path().join("manifest.sqlite");
        let manifest_root = || {
            Connection::open(&manifest_path)
                .unwrap()
                .query_row(
                    "SELECT manifest_digest FROM briskdb_integrity WHERE singleton = 1",
                    [],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .unwrap()
        };
        let original_root = manifest_root();
        let declarations = registered_table_declarations(&database);
        assert_eq!(
            database
                .register_tables(vec![declarations[0].clone(), declarations[0].clone()])
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
        let unknown_database = crate::core::LogicalDatabaseId::new(99).unwrap();
        assert_eq!(
            database
                .register_tables(vec![
                    TableDeclaration::global(unknown_database, "countries").unwrap()
                ])
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
        assert_eq!(manifest_root(), original_root);

        database
            .execute(
                "tenant-one",
                "INSERT INTO events (id, tenant_id, payload) VALUES (?1, ?2, ?3)",
                &[
                    crate::core::Value::from(1_i64),
                    crate::core::Value::from("tenant-one"),
                    crate::core::Value::from(vec![1_u8]),
                ],
            )
            .unwrap();
        assert_eq!(
            database.register_tables(declarations).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert!(database.catalog().tables().is_empty());
        assert_eq!(manifest_root(), original_root);
        assert_eq!(
            Connection::open(&manifest_path)
                .unwrap()
                .query_row("SELECT COUNT(*) FROM briskdb_tables", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
        drop(database);
        assert!(
            Database::open(temp.path(), 2)
                .unwrap()
                .catalog()
                .tables()
                .is_empty()
        );
    }

    #[test]
    fn table_registration_crash_child() {
        let Ok(root) = std::env::var("BRISKDB_TABLE_REGISTRATION_ABORT_ROOT") else {
            return;
        };
        let mut database = Database::open(&root, 2).unwrap();
        create_registered_table_schema(&database);
        let declarations = registered_table_declarations(&database);
        let result = database.register_tables(declarations);
        panic!("child did not reach requested table-registration boundary: {result:?}");
    }

    #[test]
    fn real_process_abort_leaves_exactly_the_old_or_new_table_catalog() {
        for (boundary, expected_tables) in [("before-commit", 0), ("after-commit", 2)] {
            let temp = tempfile::tempdir().unwrap();
            let status = Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("storage::tests::table_registration_crash_child")
                .arg("--nocapture")
                .env("BRISKDB_TABLE_REGISTRATION_ABORT_ROOT", temp.path())
                .env("BRISKDB_TABLE_REGISTRATION_ABORT_POINT", boundary)
                .status()
                .unwrap();
            assert!(!status.success(), "child did not abort {boundary}");

            let mut reopened = Database::open(temp.path(), 2).unwrap();
            assert_eq!(reopened.catalog().tables().len(), expected_tables);
            let declarations = registered_table_declarations(&reopened);
            reopened.register_tables(declarations).unwrap();
            assert_eq!(reopened.catalog().tables().len(), 2);
        }
    }

    fn global_index_test_declaration(database: &Database) -> GlobalIndexDeclaration {
        let table_id = database
            .catalog()
            .table("default", "events")
            .unwrap()
            .unwrap()
            .id();
        GlobalIndexDeclaration::new(
            table_id,
            "events_payload_global",
            vec![crate::core::GlobalIndexKeyPart::new(
                crate::core::GlobalIndexKeySource::column("payload").unwrap(),
                crate::core::GlobalIndexKeyType::Binary,
            )],
        )
        .unwrap()
        .with_topology(crate::core::GlobalIndexStorageTopology::SharedSqliteV1)
    }

    fn setup_global_index_root(root: &Path) -> Database {
        let mut database = Database::open(root, 2).unwrap();
        create_registered_table_schema(&database);
        database
            .register_tables(registered_table_declarations(&database))
            .unwrap();
        database
    }

    #[test]
    fn global_index_catalog_crash_child() {
        let Ok(root) = std::env::var("BRISKDB_GLOBAL_INDEX_ABORT_ROOT") else {
            return;
        };
        let mode = std::env::var("BRISKDB_GLOBAL_INDEX_ABORT_MODE").unwrap();
        let mut database = Database::open(&root, 2).unwrap();
        let result = match mode.as_str() {
            "create" => {
                let declaration = global_index_test_declaration(&database);
                database.create_global_index(declaration).map(|_| ())
            }
            "transition" => {
                let id = GlobalIndexId::new(
                    std::env::var("BRISKDB_GLOBAL_INDEX_ABORT_ID")
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
                .unwrap();
                database.transition_global_index(id, GlobalIndexLifecycle::Invalid)
            }
            "remove" => {
                let id = GlobalIndexId::new(
                    std::env::var("BRISKDB_GLOBAL_INDEX_ABORT_ID")
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
                .unwrap();
                database.remove_global_index(id)
            }
            "build" => {
                let id = GlobalIndexId::new(
                    std::env::var("BRISKDB_GLOBAL_INDEX_ABORT_ID")
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
                .unwrap();
                database.build_global_index(id).map(|_| ())
            }
            "validate" => {
                let id = GlobalIndexId::new(
                    std::env::var("BRISKDB_GLOBAL_INDEX_ABORT_ID")
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
                .unwrap();
                database.validate_global_index(id).map(|_| ())
            }
            "rebuild" => {
                let id = GlobalIndexId::new(
                    std::env::var("BRISKDB_GLOBAL_INDEX_ABORT_ID")
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
                .unwrap();
                database.rebuild_global_index(id).map(|_| ())
            }
            "repair" => {
                let id = GlobalIndexId::new(
                    std::env::var("BRISKDB_GLOBAL_INDEX_ABORT_ID")
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
                .unwrap();
                database.repair_global_index(id).map(|_| ())
            }
            "read-repair" => {
                let logical_database = database.catalog().default_database().id();
                let parsed = crate::sql::parse(
                    crate::sql::SqlDialect::Sqlite,
                    "SELECT id FROM events WHERE payload = ?1",
                )
                .unwrap();
                let common = crate::sql::validate_common_subset(parsed).unwrap();
                let normalized = crate::sql::normalize_placeholders(common).unwrap();
                let engine = crate::core::Engine::from_database(Arc::new(database));
                engine
                    .plan_bound_statement(
                        logical_database,
                        &normalized,
                        0,
                        &[crate::core::Value::from(vec![1_u8, 2, 3])],
                        None,
                    )
                    .map(|_| ())
            }
            "authority-reserve" => {
                let (id, operation, mutation) = authority_crash_request();
                debug_assert_eq!(id, mutation.index_id());
                database
                    .reserve_global_unique(operation, &mutation)
                    .map(|_| ())
            }
            "authority-finalize" => {
                let (_, operation, _) = authority_crash_request();
                database.finalize_global_unique(operation).map(|_| ())
            }
            "authority-rollback" => {
                let (_, operation, _) = authority_crash_request();
                database.rollback_global_unique(operation).map(|_| ())
            }
            "authority-write-finalize" => {
                let id = GlobalIndexId::new(
                    std::env::var("BRISKDB_GLOBAL_INDEX_ABORT_ID")
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
                .unwrap();
                let shard = database.shard_for_key(&crate::core::canonical_shard_key_bytes(
                    crate::core::CanonicalShardKeyRef::Text("crash-writer"),
                ));
                let locator =
                    global_index::encode_locator(&[rusqlite::types::ValueRef::Integer(1)]).unwrap();
                let mutation = crate::core::GlobalUniqueMutation::claim(
                    id,
                    crate::core::CanonicalIndexKey::encode_values(&[crate::core::Value::from(
                        vec![1_u8, 2, 3],
                    )])
                    .unwrap(),
                    crate::core::GlobalIndexOwner::new(shard, locator).unwrap(),
                );
                let cancellation = crate::core::CancellationToken::new();
                let storage = Storage::open(&root, 2).unwrap();
                let reservation = storage
                    .reserve_global_unique_write(&mutation, &cancellation)
                    .unwrap();
                let connection = storage.open_shard(shard).unwrap();
                crate::sql::execute(
                    &connection,
                    "INSERT INTO events (id, tenant_id, payload) VALUES (1, ?1, ?2)",
                    &[
                        crate::core::Value::from("crash-writer"),
                        crate::core::Value::from(vec![1_u8, 2, 3]),
                    ],
                )
                .unwrap();
                storage
                    .finalize_global_unique_write(&reservation, &cancellation)
                    .map(|_| ())
            }
            "authority-coordinator-write" => {
                drop(database);
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async {
                    let brisk = crate::BriskDb::open(&root).await?;
                    let session = brisk.session();
                    session.set_routing_key("crash-writer").await?;
                    brisk
                        .execute_write(
                            &session,
                            crate::Statement::new(
                                "INSERT INTO events (id, tenant_id, payload) VALUES (1, ?1, ?2)",
                                vec![
                                    crate::core::Value::from("crash-writer"),
                                    crate::core::Value::from(vec![1_u8, 2, 3]),
                                ],
                            ),
                        )
                        .await?;
                    Ok(())
                })
            }
            "authority-lease" => {
                let (id, operation, _) = authority_crash_request();
                database.lease_global_values(operation, id, 7).map(|_| ())
            }
            "authority-value-finalize" => {
                let (_, operation, _) = authority_crash_request();
                database.finalize_global_value_lease(operation).map(|_| ())
            }
            "authority-value-abandon" => {
                let (_, operation, _) = authority_crash_request();
                database.abandon_global_value_lease(operation).map(|_| ())
            }
            unexpected => panic!("unexpected global-index crash mode: {unexpected}"),
        };
        panic!("child did not reach requested global-index boundary: {result:?}");
    }

    fn authority_crash_request() -> (
        crate::core::GlobalIndexId,
        crate::core::GlobalOperationId,
        crate::core::GlobalUniqueMutation,
    ) {
        let id = crate::core::GlobalIndexId::new(
            std::env::var("BRISKDB_GLOBAL_INDEX_ABORT_ID")
                .unwrap()
                .parse()
                .unwrap(),
        )
        .unwrap();
        authority_request(id)
    }

    fn authority_request(
        id: crate::core::GlobalIndexId,
    ) -> (
        crate::core::GlobalIndexId,
        crate::core::GlobalOperationId,
        crate::core::GlobalUniqueMutation,
    ) {
        let operation = crate::core::GlobalOperationId::new([7; 16]).unwrap();
        let key = crate::core::CanonicalIndexKey::encode_values(&[crate::core::Value::from(vec![
            1_u8, 2, 3,
        ])])
        .unwrap();
        let owner = crate::core::GlobalIndexOwner::new(0, b"authority-row".to_vec()).unwrap();
        let mutation = crate::core::GlobalUniqueMutation::claim(id, key, owner);
        (id, operation, mutation)
    }

    fn abort_global_index_child(
        root: &Path,
        mode: &str,
        boundary: &str,
        id: Option<GlobalIndexId>,
    ) {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .arg("--exact")
            .arg("storage::tests::global_index_catalog_crash_child")
            .arg("--nocapture")
            .env("BRISKDB_GLOBAL_INDEX_ABORT_ROOT", root)
            .env("BRISKDB_GLOBAL_INDEX_ABORT_MODE", mode)
            .env("BRISKDB_GLOBAL_INDEX_ABORT_POINT", boundary)
            .env("BRISKDB_GLOBAL_INDEX_AUTHORITY_ABORT_POINT", boundary);
        if let Some(id) = id {
            command.env("BRISKDB_GLOBAL_INDEX_ABORT_ID", id.get().to_string());
        }
        let status = command.status().unwrap();
        assert!(!status.success(), "child did not abort at {boundary}");
    }

    fn setup_authority_crash_root(root: &Path) -> (GlobalIndexId, GlobalIndexId) {
        let mut database = setup_global_index_root(root);
        let unique = global_index_test_declaration(&database)
            .unique(crate::core::UniqueNullSemantics::Distinct);
        let unique_id = database.create_global_index(unique).unwrap();
        database.build_global_index(unique_id).unwrap();
        let table_id = database
            .catalog()
            .table("default", "events")
            .unwrap()
            .unwrap()
            .id();
        let value = crate::core::GlobalIndexDeclaration::new(
            table_id,
            "events_id_global_value",
            vec![crate::core::GlobalIndexKeyPart::new(
                crate::core::GlobalIndexKeySource::column("id").unwrap(),
                crate::core::GlobalIndexKeyType::Int64,
            )],
        )
        .unwrap()
        .unique(crate::core::UniqueNullSemantics::NotDistinct)
        .with_topology(crate::core::GlobalIndexStorageTopology::SharedSqliteV1);
        let value_id = database.create_global_index(value).unwrap();
        database.build_global_index(value_id).unwrap();
        (unique_id, value_id)
    }

    fn authority_operation_state(root: &Path) -> Option<i64> {
        Connection::open(root.join("global-indexes/global.sqlite"))
            .unwrap()
            .query_row(
                "SELECT operation_state FROM briskdb_global_operations
                 WHERE operation_id = ?1",
                [&[7_u8; 16][..]],
                |row| row.get(0),
            )
            .optional()
            .unwrap()
    }

    #[test]
    fn global_authority_retries_converge_after_real_process_abort_at_every_boundary() {
        for (boundary, durable) in [
            ("unique-reserve-before-commit", false),
            ("unique-reserve-after-commit", true),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let (unique_id, _) = setup_authority_crash_root(temp.path());
            abort_global_index_child(temp.path(), "authority-reserve", boundary, Some(unique_id));
            assert_eq!(authority_operation_state(temp.path()).is_some(), durable);
            let database = Database::open(temp.path(), 2).unwrap();
            let (_, operation, mutation) = authority_request(unique_id);
            assert_eq!(
                database
                    .reserve_global_unique(operation, &mutation)
                    .unwrap()
                    .state(),
                crate::core::GlobalOperationState::Active
            );
        }

        for (mode, boundaries, target) in [
            (
                "authority-finalize",
                [
                    "unique-finalize-before-commit",
                    "unique-finalize-after-commit",
                ],
                crate::core::GlobalOperationState::Finalized,
            ),
            (
                "authority-rollback",
                [
                    "unique-rollback-before-commit",
                    "unique-rollback-after-commit",
                ],
                crate::core::GlobalOperationState::RolledBack,
            ),
        ] {
            for (offset, boundary) in boundaries.into_iter().enumerate() {
                let temp = tempfile::tempdir().unwrap();
                let (unique_id, _) = setup_authority_crash_root(temp.path());
                let database = Database::open(temp.path(), 2).unwrap();
                let (_, operation, mutation) = authority_request(unique_id);
                database
                    .reserve_global_unique(operation, &mutation)
                    .unwrap();
                drop(database);
                abort_global_index_child(temp.path(), mode, boundary, Some(unique_id));
                assert_eq!(
                    authority_operation_state(temp.path()),
                    Some(if offset == 0 { 1 } else { target_code(target) })
                );
                let database = Database::open(temp.path(), 2).unwrap();
                let report = if target == crate::core::GlobalOperationState::Finalized {
                    database.finalize_global_unique(operation).unwrap()
                } else {
                    database.rollback_global_unique(operation).unwrap()
                };
                assert_eq!(report.state(), target);
            }
        }

        for (boundary, durable) in [
            ("value-lease-before-commit", false),
            ("value-lease-after-commit", true),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let (_, value_id) = setup_authority_crash_root(temp.path());
            abort_global_index_child(temp.path(), "authority-lease", boundary, Some(value_id));
            assert_eq!(authority_operation_state(temp.path()).is_some(), durable);
            let database = Database::open(temp.path(), 2).unwrap();
            let (_, operation, _) = authority_request(value_id);
            let lease = database
                .lease_global_values(operation, value_id, 7)
                .unwrap();
            assert_eq!((lease.first(), lease.last()), (1, 7));
        }

        for (mode, boundaries, target) in [
            (
                "authority-value-finalize",
                [
                    "value-finalize-before-commit",
                    "value-finalize-after-commit",
                ],
                crate::core::GlobalOperationState::Finalized,
            ),
            (
                "authority-value-abandon",
                ["value-abandon-before-commit", "value-abandon-after-commit"],
                crate::core::GlobalOperationState::RolledBack,
            ),
        ] {
            for (offset, boundary) in boundaries.into_iter().enumerate() {
                let temp = tempfile::tempdir().unwrap();
                let (_, value_id) = setup_authority_crash_root(temp.path());
                let database = Database::open(temp.path(), 2).unwrap();
                let (_, operation, _) = authority_request(value_id);
                database
                    .lease_global_values(operation, value_id, 7)
                    .unwrap();
                drop(database);
                abort_global_index_child(temp.path(), mode, boundary, Some(value_id));
                assert_eq!(
                    authority_operation_state(temp.path()),
                    Some(if offset == 0 { 1 } else { target_code(target) })
                );
                let database = Database::open(temp.path(), 2).unwrap();
                let lease = if target == crate::core::GlobalOperationState::Finalized {
                    database.finalize_global_value_lease(operation).unwrap()
                } else {
                    database.abandon_global_value_lease(operation).unwrap()
                };
                assert_eq!(lease.state(), target);
            }
        }
    }

    #[test]
    fn indexed_write_recovery_converges_after_real_process_abort() {
        for (boundary, expected_recovered) in [
            ("unique-write-finalize-before-commit", 1_usize),
            ("unique-write-finalize-after-commit", 0_usize),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let mut database = setup_global_index_root(temp.path());
            let declaration = global_index_test_declaration(&database)
                .unique(crate::core::UniqueNullSemantics::Distinct);
            let index_id = database.create_global_index(declaration).unwrap();
            database.build_global_index(index_id).unwrap();
            drop(database);

            abort_global_index_child(
                temp.path(),
                "authority-write-finalize",
                boundary,
                Some(index_id),
            );
            let storage = Storage::open(temp.path(), 2).unwrap();
            assert_eq!(
                storage
                    .recover_global_unique_writes(&crate::core::CancellationToken::new())
                    .unwrap(),
                expected_recovered
            );
            drop(storage);
            let mut database = Database::open(temp.path(), 2).unwrap();
            let report = database.validate_global_index(index_id).unwrap();
            assert!(report.is_valid(), "{report:?}");
        }
    }

    #[test]
    fn indexed_write_recovers_across_the_physical_commit_boundary() {
        for boundary in [
            "unique-write-physical-before-commit",
            "unique-write-physical-after-commit",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let mut database = setup_global_index_root(temp.path());
            let declaration = global_index_test_declaration(&database)
                .unique(crate::core::UniqueNullSemantics::Distinct);
            let index_id = database.create_global_index(declaration).unwrap();
            database.build_global_index(index_id).unwrap();
            drop(database);

            abort_global_index_child(
                temp.path(),
                "authority-coordinator-write",
                boundary,
                Some(index_id),
            );
            let storage = Storage::open(temp.path(), 2).unwrap();
            assert_eq!(
                storage
                    .recover_global_unique_writes(&crate::core::CancellationToken::new())
                    .unwrap(),
                1
            );
            drop(storage);
            let mut database = Database::open(temp.path(), 2).unwrap();
            let report = database.validate_global_index(index_id).unwrap();
            assert!(report.is_valid(), "{report:?}");
            assert_eq!(
                database
                    .query(
                        "crash-writer",
                        "SELECT COUNT(*) FROM events WHERE id = 1 AND tenant_id = ?1",
                        &[crate::core::Value::from("crash-writer")],
                    )
                    .unwrap()
                    .rows()[0]
                    .get(0)
                    .unwrap()
                    .as_i64(),
                Some(if boundary.ends_with("after-commit") {
                    1
                } else {
                    0
                })
            );
        }
    }

    const fn target_code(state: crate::core::GlobalOperationState) -> i64 {
        match state {
            crate::core::GlobalOperationState::Active => 1,
            crate::core::GlobalOperationState::Finalized => 2,
            crate::core::GlobalOperationState::RolledBack => 3,
        }
    }

    fn setup_upgrade_test_root(root: &Path) {
        let mut database = setup_global_index_root(root);
        let id = database
            .create_global_index(global_index_test_declaration(&database))
            .unwrap();
        database.build_global_index(id).unwrap();
        drop(database);
        global_index::downgrade_to_v1_for_test(root);
    }

    #[test]
    fn global_authority_format_upgrade_recovers_from_real_process_abort() {
        for (boundary, expected_version) in [
            ("upgrade-before-commit", 1_i64),
            ("upgrade-after-commit", 4_i64),
        ] {
            let temp = tempfile::tempdir().unwrap();
            setup_upgrade_test_root(temp.path());
            let status = Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("storage::tests::global_index_catalog_crash_child")
                .arg("--nocapture")
                .env("BRISKDB_GLOBAL_INDEX_ABORT_ROOT", temp.path())
                .env("BRISKDB_GLOBAL_INDEX_ABORT_MODE", "create")
                .env("BRISKDB_GLOBAL_INDEX_AUTHORITY_ABORT_POINT", boundary)
                .status()
                .unwrap();
            assert!(!status.success(), "child did not abort at {boundary}");
            assert_eq!(
                Connection::open(temp.path().join("global-indexes/global.sqlite"))
                    .unwrap()
                    .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                expected_version
            );
            drop(Database::open(temp.path(), 2).unwrap());
            let canonical = fs::canonicalize(temp.path()).unwrap();
            assert!(!global_index::startup_requires_upgrade(&canonical).unwrap());
        }
    }

    #[test]
    fn global_authority_v2_upgrade_preserves_ready_index_data() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = setup_global_index_root(temp.path());
        database
            .execute(
                "v2-upgrade-route",
                "INSERT INTO events (id, tenant_id, payload) VALUES (1, ?1, ?2)",
                &[
                    crate::core::Value::from("v2-upgrade-route"),
                    crate::core::Value::from(vec![1_u8, 2, 3]),
                ],
            )
            .unwrap();
        let index = database
            .create_global_index(global_index_test_declaration(&database))
            .unwrap();
        database.build_global_index(index).unwrap();
        drop(database);
        global_index::downgrade_to_v2_for_test(temp.path());

        let mut database = Database::open(temp.path(), 2).unwrap();
        assert!(database.validate_global_index(index).unwrap().is_valid());
        let authority = Connection::open(temp.path().join("global-indexes/global.sqlite")).unwrap();
        assert_eq!(
            authority
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            4
        );
        assert_eq!(
            authority
                .query_row(
                    "SELECT COUNT(*) FROM briskdb_global_index_entries WHERE index_id = ?1",
                    [i64::try_from(index.get()).unwrap()],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert!(
            authority
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_schema
                         WHERE name = 'briskdb_global_index_read_repairs'
                     )",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        );
    }

    #[test]
    fn global_authority_v3_upgrade_preserves_entries_and_requires_async_rebuild() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = setup_global_index_root(temp.path());
        database
            .execute(
                "v3-upgrade-route",
                "INSERT INTO events (id, tenant_id, payload) VALUES (1, ?1, ?2)",
                &[
                    crate::core::Value::from("v3-upgrade-route"),
                    crate::core::Value::from(vec![4_u8, 5, 6]),
                ],
            )
            .unwrap();
        let index = database
            .create_global_index(global_index_test_declaration(&database))
            .unwrap();
        database.build_global_index(index).unwrap();
        drop(database);
        global_index::downgrade_to_v3_for_test(temp.path());

        let database = Database::open(temp.path(), 2).unwrap();
        let status = database.global_index_async_status(index).unwrap();
        assert!(status.rebuild_required());
        let authority = Connection::open(temp.path().join("global-indexes/global.sqlite")).unwrap();
        assert_eq!(
            authority
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            4
        );
        assert_eq!(
            authority
                .query_row(
                    "SELECT COUNT(*) FROM briskdb_global_index_entries WHERE index_id = ?1",
                    [i64::try_from(index.get()).unwrap()],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }

    fn setup_stale_read_repair_root(root: &Path) -> GlobalIndexId {
        let mut database = setup_global_index_root(root);
        database
            .execute(
                "read-repair-route",
                "INSERT INTO events (id, tenant_id, payload) VALUES (1, ?1, ?2)",
                &[
                    crate::core::Value::from("read-repair-route"),
                    crate::core::Value::from(vec![1_u8, 2, 3]),
                ],
            )
            .unwrap();
        let index = database
            .create_global_index(global_index_test_declaration(&database))
            .unwrap();
        database.build_global_index(index).unwrap();
        let shard = database.shard_for_key(&crate::core::canonical_shard_key_bytes(
            crate::core::CanonicalShardKeyRef::Text("read-repair-route"),
        ));
        drop(database);
        Connection::open(shard_file(root, shard))
            .unwrap()
            .execute(
                "DELETE FROM events WHERE tenant_id = ?1 AND id = 1",
                ["read-repair-route"],
            )
            .unwrap();
        index
    }

    fn plan_stale_read_repair(root: &Path) -> crate::core::GlobalIndexRoutingPlan {
        let database = Arc::new(Database::open(root, 2).unwrap());
        let logical_database = database.catalog().default_database().id();
        let engine = crate::core::Engine::from_database(database);
        let parsed = crate::sql::parse(
            crate::sql::SqlDialect::Sqlite,
            "SELECT id FROM events WHERE payload = ?1",
        )
        .unwrap();
        let common = crate::sql::validate_common_subset(parsed).unwrap();
        let normalized = crate::sql::normalize_placeholders(common).unwrap();
        engine
            .plan_bound_statement(
                logical_database,
                &normalized,
                0,
                &[crate::core::Value::from(vec![1_u8, 2, 3])],
                None,
            )
            .unwrap()
            .global_index_routing()
            .clone()
    }

    #[test]
    fn global_index_read_repair_recovers_at_every_transaction_boundary() {
        for (boundary, expected_state) in [
            ("read-repair-before-enqueue-commit", None),
            ("read-repair-after-enqueue-commit", Some(1_i64)),
            ("read-repair-before-apply-commit", Some(1_i64)),
            ("read-repair-after-apply-commit", Some(2_i64)),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let index = setup_stale_read_repair_root(temp.path());
            abort_global_index_child(temp.path(), "read-repair", boundary, Some(index));
            let authority =
                Connection::open(temp.path().join("global-indexes/global.sqlite")).unwrap();
            let state = authority
                .query_row(
                    "SELECT repair_state FROM briskdb_global_index_read_repairs
                     WHERE index_id = ?1",
                    [i64::try_from(index.get()).unwrap()],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .unwrap();
            assert_eq!(state, expected_state, "boundary {boundary}");
            drop(authority);

            let recovered = plan_stale_read_repair(temp.path());
            assert_eq!(
                recovered.fallback_reason(),
                Some(crate::core::GlobalIndexRoutingFallback::FreshnessUnproven)
            );
            let authority =
                Connection::open(temp.path().join("global-indexes/global.sqlite")).unwrap();
            assert_eq!(
                authority
                    .query_row(
                        "SELECT COUNT(*) FROM briskdb_global_index_read_repairs
                         WHERE index_id = ?1 AND repair_state = 2",
                        [i64::try_from(index.get()).unwrap()],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                1,
                "boundary {boundary} did not converge"
            );
            drop(authority);

            let retry = plan_stale_read_repair(temp.path());
            assert_eq!(retry.candidate_count(), 0);
            assert_eq!(retry.stale_candidate_count(), 0);
            assert_eq!(retry.repairs_queued(), 0);
        }
    }

    #[test]
    fn global_authority_format_upgrade_requires_sole_process_ownership() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = setup_global_index_root(temp.path());
        let id = database
            .create_global_index(global_index_test_declaration(&database))
            .unwrap();
        database.build_global_index(id).unwrap();
        drop(database);
        let (peer, release) = spawn_shared_root_peer(temp.path(), "global-authority-upgrade");
        global_index::downgrade_to_v1_for_test(temp.path());
        let error = Database::open(temp.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Busy);
        assert!(error.is_retryable());
        let canonical = fs::canonicalize(temp.path()).unwrap();
        assert!(global_index::startup_requires_upgrade(&canonical).unwrap());
        release_shared_root_peer(peer, &release);
        drop(Database::open(temp.path(), 2).unwrap());
        assert!(!global_index::startup_requires_upgrade(&canonical).unwrap());
    }

    #[test]
    fn global_index_catalog_recovers_at_every_transaction_boundary() {
        for (boundary, expected_count) in [("create-before-commit", 0), ("create-after-commit", 1)]
        {
            let temp = tempfile::tempdir().unwrap();
            drop(setup_global_index_root(temp.path()));
            abort_global_index_child(temp.path(), "create", boundary, None);
            assert_eq!(
                Database::inspect_global_indexes(temp.path()).unwrap().len(),
                expected_count,
                "boundary {boundary}"
            );
            drop(Database::open(temp.path(), 2).unwrap());
        }

        for (boundary, expected) in [
            ("transition-before-commit", GlobalIndexLifecycle::Creating),
            ("transition-after-commit", GlobalIndexLifecycle::Invalid),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let mut database = setup_global_index_root(temp.path());
            let declaration = global_index_test_declaration(&database);
            let id = database.create_global_index(declaration).unwrap();
            drop(database);
            abort_global_index_child(temp.path(), "transition", boundary, Some(id));
            assert_eq!(
                Database::inspect_global_indexes(temp.path()).unwrap()[0].lifecycle(),
                expected,
                "boundary {boundary}"
            );
            drop(Database::open(temp.path(), 2).unwrap());
        }

        for (boundary, expected_count) in [("remove-before-commit", 1), ("remove-after-commit", 0)]
        {
            let temp = tempfile::tempdir().unwrap();
            let mut database = setup_global_index_root(temp.path());
            let declaration = global_index_test_declaration(&database);
            let id = database.create_global_index(declaration).unwrap();
            database
                .transition_global_index(id, GlobalIndexLifecycle::Dropping)
                .unwrap();
            drop(database);
            abort_global_index_child(temp.path(), "remove", boundary, Some(id));
            assert_eq!(
                Database::inspect_global_indexes(temp.path()).unwrap().len(),
                expected_count,
                "boundary {boundary}"
            );
            drop(Database::open(temp.path(), 2).unwrap());
        }
    }

    #[test]
    fn global_index_build_resumes_after_real_process_abort_at_every_durable_phase() {
        let boundaries = [
            ("initialized", 0_u16, false),
            ("shard-0-before-commit", 0, false),
            ("shard-0-after-commit", 1, false),
            ("shard-1-before-commit", 1, false),
            ("shard-1-after-commit", 2, false),
            ("complete-before-commit", 2, false),
            ("complete-after-commit", 2, false),
            ("transition-before-commit", 2, true),
            ("transition-after-commit", 2, true),
        ];
        for (boundary, expected_resume, manifest_boundary) in boundaries {
            let temp = tempfile::tempdir().unwrap();
            let mut database = setup_global_index_root(temp.path());
            for shard in 0..2_u16 {
                for row in 0..2_i64 {
                    let tenant = (0..10_000)
                        .map(|candidate| format!("tenant-{shard}-{candidate}"))
                        .find(|candidate| database.shard_for_key(candidate.as_bytes()) == shard)
                        .unwrap();
                    database
                        .execute(
                            &tenant,
                            "INSERT INTO events (id, tenant_id, payload)
                             VALUES (?1, ?2, ?3)",
                            &[
                                crate::core::Value::from(i64::from(shard) * 10 + row),
                                crate::core::Value::from(tenant.as_str()),
                                crate::core::Value::from(
                                    format!("payload-{shard}-{row}").into_bytes(),
                                ),
                            ],
                        )
                        .unwrap();
                }
            }
            let declaration = global_index_test_declaration(&database);
            let id = database.create_global_index(declaration).unwrap();
            drop(database);

            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .arg("--exact")
                .arg("storage::tests::global_index_catalog_crash_child")
                .arg("--nocapture")
                .env("BRISKDB_GLOBAL_INDEX_ABORT_ROOT", temp.path())
                .env("BRISKDB_GLOBAL_INDEX_ABORT_MODE", "build")
                .env("BRISKDB_GLOBAL_INDEX_ABORT_ID", id.get().to_string());
            if manifest_boundary {
                command.env("BRISKDB_GLOBAL_INDEX_ABORT_POINT", boundary);
            } else {
                command.env("BRISKDB_GLOBAL_INDEX_BUILD_ABORT_POINT", boundary);
            }
            let status = command.status().unwrap();
            assert!(!status.success(), "child did not abort at {boundary}");

            let mut reopened = Database::open(temp.path(), 2).unwrap();
            let report = reopened.build_global_index(id).unwrap();
            assert_eq!(
                report.resumed_from_shard(),
                expected_resume,
                "boundary {boundary}"
            );
            assert_eq!(report.indexed_rows(), 4, "boundary {boundary}");
            assert_eq!(
                reopened
                    .catalog()
                    .global_index_by_id(id)
                    .unwrap()
                    .lifecycle(),
                GlobalIndexLifecycle::Ready,
                "boundary {boundary}"
            );
            let physical =
                Connection::open(temp.path().join("global-indexes").join("global.sqlite")).unwrap();
            assert_eq!(
                physical
                    .query_row(
                        "SELECT COUNT(*) FROM briskdb_global_index_entries
                         WHERE index_id = ?1",
                        [i64::try_from(id.get()).unwrap()],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                4,
                "boundary {boundary}"
            );
            assert_eq!(
                physical
                    .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok",
                "boundary {boundary}"
            );
        }
    }

    fn setup_recovery_global_index(root: &Path) -> GlobalIndexId {
        let mut database = setup_global_index_root(root);
        for shard in 0..2_u16 {
            let tenant = (0..10_000)
                .map(|candidate| format!("recovery-tenant-{shard}-{candidate}"))
                .find(|candidate| database.shard_for_key(candidate.as_bytes()) == shard)
                .unwrap();
            database
                .execute(
                    &tenant,
                    "INSERT INTO events (id, tenant_id, payload) VALUES (?1, ?2, ?3)",
                    &[
                        crate::core::Value::from(i64::from(shard)),
                        crate::core::Value::from(tenant.as_str()),
                        crate::core::Value::from(format!("recovery-payload-{shard}").into_bytes()),
                    ],
                )
                .unwrap();
        }
        let id = database
            .create_global_index(global_index_test_declaration(&database))
            .unwrap();
        database.build_global_index(id).unwrap();
        id
    }

    fn abort_global_index_recovery_child(
        root: &Path,
        mode: &str,
        boundary: &str,
        id: GlobalIndexId,
    ) {
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("storage::tests::global_index_catalog_crash_child")
            .arg("--nocapture")
            .env("BRISKDB_GLOBAL_INDEX_ABORT_ROOT", root)
            .env("BRISKDB_GLOBAL_INDEX_ABORT_MODE", mode)
            .env("BRISKDB_GLOBAL_INDEX_ABORT_ID", id.get().to_string())
            .env("BRISKDB_GLOBAL_INDEX_RECOVERY_ABORT_POINT", boundary)
            .status()
            .unwrap();
        assert!(
            !status.success(),
            "child did not abort at {mode}:{boundary}"
        );
    }

    #[test]
    fn global_index_recovery_survives_real_process_abort_without_partial_publication() {
        for (mode, boundary) in [
            ("validate", "validation-fenced"),
            ("validate", "validation-complete"),
            ("validate", "validation-published"),
            ("rebuild", "rebuild-fenced"),
            ("rebuild", "rebuild-complete"),
            ("rebuild", "rebuild-published"),
            ("repair", "repair-fenced"),
            ("repair", "repair-shard-0-before-commit"),
            ("repair", "repair-shard-0-after-commit"),
            ("repair", "repair-complete"),
            ("repair", "repair-published"),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let id = setup_recovery_global_index(temp.path());
            if mode == "repair" {
                Connection::open(temp.path().join("global-indexes/global.sqlite"))
                    .unwrap()
                    .execute(
                        "DELETE FROM briskdb_global_index_entries
                         WHERE index_id = ?1 AND source_shard = 0",
                        [i64::try_from(id.get()).unwrap()],
                    )
                    .unwrap();
            }

            abort_global_index_recovery_child(temp.path(), mode, boundary, id);

            let mut reopened = Database::open(temp.path(), 2).unwrap();
            match mode {
                "validate" => {
                    reopened.validate_global_index(id).unwrap();
                }
                "rebuild" => {
                    reopened.rebuild_global_index(id).unwrap();
                }
                "repair" => {
                    reopened.repair_global_index(id).unwrap();
                }
                _ => unreachable!(),
            }
            let report = reopened.validate_global_index(id).unwrap();
            assert!(report.is_valid(), "recovery failed after {mode}:{boundary}");
            assert_eq!(report.physical_entries_examined(), 2);
            assert_eq!(
                reopened
                    .catalog()
                    .global_index_by_id(id)
                    .unwrap()
                    .lifecycle(),
                GlobalIndexLifecycle::Ready
            );
        }
    }

    #[test]
    fn global_index_resume_restarts_if_a_checkpointed_source_shard_changed() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = setup_global_index_root(temp.path());
        let tenant = (0..10_000)
            .map(|candidate| format!("checkpoint-tenant-{candidate}"))
            .find(|candidate| database.shard_for_key(candidate.as_bytes()) == 0)
            .unwrap();
        database
            .execute(
                &tenant,
                "INSERT INTO events (id, tenant_id, payload) VALUES (?1, ?2, ?3)",
                &[
                    crate::core::Value::from(1_i64),
                    crate::core::Value::from(tenant.as_str()),
                    crate::core::Value::from(b"old".to_vec()),
                ],
            )
            .unwrap();
        let declaration = global_index_test_declaration(&database);
        let id = database.create_global_index(declaration).unwrap();
        drop(database);

        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("storage::tests::global_index_catalog_crash_child")
            .arg("--nocapture")
            .env("BRISKDB_GLOBAL_INDEX_ABORT_ROOT", temp.path())
            .env("BRISKDB_GLOBAL_INDEX_ABORT_MODE", "build")
            .env("BRISKDB_GLOBAL_INDEX_ABORT_ID", id.get().to_string())
            .env(
                "BRISKDB_GLOBAL_INDEX_BUILD_ABORT_POINT",
                "shard-0-after-commit",
            )
            .status()
            .unwrap();
        assert!(!status.success());

        let mut reopened = Database::open(temp.path(), 2).unwrap();
        reopened
            .execute(
                &tenant,
                "INSERT INTO events (id, tenant_id, payload) VALUES (?1, ?2, ?3)",
                &[
                    crate::core::Value::from(2_i64),
                    crate::core::Value::from(tenant.as_str()),
                    crate::core::Value::from(b"new".to_vec()),
                ],
            )
            .unwrap();
        let report = reopened.build_global_index(id).unwrap();
        assert_eq!(report.resumed_from_shard(), 0);
        assert_eq!(report.indexed_rows(), 2);
    }

    const GENERATED_DDL_CRASH_SOURCE: &str =
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL)";

    #[test]
    fn generated_table_ddl_crash_child() {
        let Ok(root) = std::env::var("BRISKDB_GENERATED_DDL_ABORT_ROOT") else {
            return;
        };
        let mut database = Database::open(root, 2).unwrap();
        let result =
            database.apply_generated_table_ddl(SqlDialect::Sqlite, GENERATED_DDL_CRASH_SOURCE);
        panic!("child did not reach requested generated-DDL boundary: {result:?}");
    }

    #[test]
    fn generated_table_ddl_resumes_after_real_process_abort_at_every_durable_phase() {
        for boundary in [
            "journal",
            "shard-0",
            "progress-0-prepared",
            "progress-0",
            "physical-finalization-prepared",
            "physical-finalization-committed",
            "physical-complete",
            "mark-before-commit",
            "mark-after-commit",
            "provisioning-before-commit",
            "provisioning-after-commit",
            "seed-0-before-progress",
            "seed-0-progress",
            "complete-before-commit",
            "complete-after-commit",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let status = Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("storage::tests::generated_table_ddl_crash_child")
                .arg("--nocapture")
                .env("BRISKDB_GENERATED_DDL_ABORT_ROOT", temp.path())
                .env("BRISKDB_GENERATED_DDL_ABORT_POINT", boundary)
                .status()
                .unwrap();
            assert!(!status.success(), "child did not abort at {boundary}");

            let mut reopened = Database::open(temp.path(), 2).unwrap();
            let receipt = reopened
                .apply_generated_table_ddl(SqlDialect::Sqlite, GENERATED_DDL_CRASH_SOURCE)
                .unwrap();
            let table = reopened
                .catalog()
                .table("default", "events")
                .unwrap()
                .unwrap();
            assert_eq!(table.id(), receipt.table_id(), "boundary {boundary}");
            assert_eq!(
                table.generated_id_policy(),
                &GeneratedIdPolicy::native_range_v1("id").unwrap(),
                "boundary {boundary}"
            );

            let manifest = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
            assert_eq!(
                manifest
                    .query_row(
                        "SELECT lifecycle_state FROM briskdb_generated_table_ddl",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                3,
                "boundary {boundary}"
            );
            assert_eq!(
                manifest
                    .query_row(
                        "SELECT COUNT(*) FROM briskdb_table_provisioning",
                        [],
                        |row| { row.get::<_, i64>(0) }
                    )
                    .unwrap(),
                0,
                "boundary {boundary}"
            );
            drop(manifest);

            for shard_id in 0..2_u16 {
                let shard = Connection::open(shard_file(temp.path(), shard_id)).unwrap();
                assert_eq!(
                    shard
                        .query_row(
                            "SELECT seq FROM sqlite_sequence WHERE name = 'events'",
                            [],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    native_range_v1_sequence_floor(
                        crate::core::generated_id::AllocationOwnerSlot::new(shard_id).unwrap(),
                    ),
                    "boundary {boundary}, shard {shard_id}"
                );
                assert_eq!(
                    shard
                        .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
                        .unwrap(),
                    "ok",
                    "boundary {boundary}, shard {shard_id}"
                );
            }
        }
    }

    #[test]
    fn shared_root_peer_process_child() {
        let Ok(root) = std::env::var("BRISKDB_SHARED_ROOT_PEER_ROOT") else {
            return;
        };
        let ready = PathBuf::from(std::env::var("BRISKDB_SHARED_ROOT_PEER_READY").unwrap());
        let release = PathBuf::from(std::env::var("BRISKDB_SHARED_ROOT_PEER_RELEASE").unwrap());
        let _database = Database::open(root, 2).unwrap();
        fs::write(&ready, b"ready").unwrap();
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        while !release.exists() && Instant::now() < deadline {
            thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(release.exists(), "parent did not release shared-root peer");
    }

    fn spawn_shared_root_peer(root: &Path, label: &str) -> (std::process::Child, PathBuf) {
        let ready = root.join(format!("peer-{label}-ready"));
        let release = root.join(format!("peer-{label}-release"));
        let child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("storage::tests::shared_root_peer_process_child")
            .arg("--nocapture")
            .env("BRISKDB_SHARED_ROOT_PEER_ROOT", root)
            .env("BRISKDB_SHARED_ROOT_PEER_READY", &ready)
            .env("BRISKDB_SHARED_ROOT_PEER_RELEASE", &release)
            .spawn()
            .unwrap();
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        while !ready.exists() && Instant::now() < deadline {
            thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(ready.exists(), "shared-root peer did not open the database");
        (child, release)
    }

    fn release_shared_root_peer(mut child: std::process::Child, release: &Path) {
        fs::write(release, b"release").unwrap();
        assert!(child.wait().unwrap().success());
    }

    #[test]
    fn peer_fences_schema_migration_and_catalog_registration_before_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let setup = Database::open(temp.path(), 2).unwrap();
        create_registered_table_schema(&setup);
        drop(setup);

        let (peer, release) = spawn_shared_root_peer(temp.path(), "catalog");
        let mut database = Database::open(temp.path(), 2).unwrap();
        let manifest_path = temp.path().join("manifest.sqlite");
        let before = Connection::open(&manifest_path)
            .unwrap()
            .query_row(
                "SELECT schema_generation,
                        (SELECT COUNT(*) FROM briskdb_schema_migrations),
                        (SELECT COUNT(*) FROM briskdb_tables),
                        manifest_digest
                 FROM briskdb_schema_catalog
                 JOIN briskdb_integrity ON briskdb_integrity.singleton = 1
                 WHERE briskdb_schema_catalog.singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                    ))
                },
            )
            .unwrap();

        let migration = database
            .broadcast("CREATE INDEX events_payload_idx ON events(payload)")
            .unwrap_err();
        assert_eq!(migration.kind(), EngineErrorKind::Busy);
        assert!(migration.is_retryable());
        let registration = database
            .register_tables(registered_table_declarations(&database))
            .unwrap_err();
        assert_eq!(registration.kind(), EngineErrorKind::Busy);
        assert!(registration.is_retryable());

        let after = Connection::open(&manifest_path)
            .unwrap()
            .query_row(
                "SELECT schema_generation,
                        (SELECT COUNT(*) FROM briskdb_schema_migrations),
                        (SELECT COUNT(*) FROM briskdb_tables),
                        manifest_digest
                 FROM briskdb_schema_catalog
                 JOIN briskdb_integrity ON briskdb_integrity.singleton = 1
                 WHERE briskdb_schema_catalog.singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(after, before);
        for shard_id in 0..2 {
            assert!(
                !Connection::open(shard_file(temp.path(), shard_id))
                    .unwrap()
                    .query_row(
                        "SELECT EXISTS(
                            SELECT 1 FROM sqlite_schema
                            WHERE name = 'events_payload_idx'
                         )",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap()
            );
        }

        release_shared_root_peer(peer, &release);
        let declarations = registered_table_declarations(&database);
        database.register_tables(declarations).unwrap();
        database
            .broadcast("CREATE INDEX events_payload_idx ON events(payload)")
            .unwrap();
    }

    #[test]
    fn peer_fences_generated_table_ddl_and_guard_downgrades_after_retry() {
        let temp = tempfile::tempdir().unwrap();
        drop(Database::open(temp.path(), 2).unwrap());
        let (peer, release) = spawn_shared_root_peer(temp.path(), "generated");
        let mut database = Database::open(temp.path(), 2).unwrap();

        let error = database
            .apply_generated_table_ddl(SqlDialect::Sqlite, GENERATED_DDL_CRASH_SOURCE)
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Busy);
        assert!(error.is_retryable());
        let manifest = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
        assert_eq!(
            manifest
                .query_row(
                    "SELECT COUNT(*) FROM briskdb_generated_table_ddl",
                    [],
                    |row| { row.get::<_, i64>(0) }
                )
                .unwrap(),
            0
        );
        drop(manifest);
        for shard_id in 0..2 {
            assert!(
                !Connection::open(shard_file(temp.path(), shard_id))
                    .unwrap()
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'events')",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap()
            );
        }

        release_shared_root_peer(peer, &release);
        database
            .apply_generated_table_ddl(SqlDialect::Sqlite, GENERATED_DDL_CRASH_SOURCE)
            .unwrap();

        // The successful exclusive operation must downgrade to the lifetime
        // shared lease so a fresh independent process can join immediately.
        let (peer, release) = spawn_shared_root_peer(temp.path(), "after-retry");
        release_shared_root_peer(peer, &release);
    }

    #[test]
    fn hilo_lease_process_child() {
        let Ok(root) = std::env::var("BRISKDB_HILO_PROCESS_ROOT") else {
            return;
        };
        let storage = Storage::open(root, 2).unwrap();
        let table_id = hilo_events_table_id(&storage);
        if let Ok(ready) = std::env::var("BRISKDB_HILO_PROCESS_READY") {
            fs::write(&ready, b"ready").unwrap();
            let go = PathBuf::from(std::env::var("BRISKDB_HILO_PROCESS_GO").unwrap());
            let deadline = Instant::now() + std::time::Duration::from_secs(10);
            while !go.exists() && Instant::now() < deadline {
                thread::sleep(std::time::Duration::from_millis(5));
            }
            assert!(go.exists(), "parent did not release hilo_v1 child");
        }
        let allocation = storage.allocate_hilo_v1(table_id).unwrap();
        if let Ok(output) = std::env::var("BRISKDB_HILO_PROCESS_OUTPUT") {
            fs::write(output, allocation.id().to_string()).unwrap();
        }
    }

    #[test]
    fn hilo_restart_burns_the_unconsumed_tail_of_the_committed_block() {
        let temp = tempfile::tempdir().unwrap();
        let storage = create_registered_hilo_storage(temp.path(), 2);
        let table_id = hilo_events_table_id(&storage);
        let first = storage.allocate_hilo_v1(table_id).unwrap().id();
        assert_eq!(
            crate::core::generated_id::HiloV1Id::decode(first)
                .unwrap()
                .sequence(),
            1
        );
        drop(storage);

        let reopened = Storage::open(temp.path(), 2).unwrap();
        let next = reopened.allocate_hilo_v1(table_id).unwrap().id();
        assert_eq!(
            crate::core::generated_id::HiloV1Id::decode(next)
                .unwrap()
                .sequence(),
            manifest::HILO_V1_BLOCK_SIZE + 1
        );
    }

    #[test]
    fn hilo_allocator_reserves_manifest_state_once_per_block() {
        let temp = tempfile::tempdir().unwrap();
        let storage = create_registered_hilo_storage(temp.path(), 4);
        let table_id = hilo_events_table_id(&storage);

        for expected_sequence in 1..=manifest::HILO_V1_BLOCK_SIZE + 1 {
            let allocation = storage.allocate_hilo_v1(table_id).unwrap();
            assert_eq!(
                crate::core::generated_id::HiloV1Id::decode(allocation.id())
                    .unwrap()
                    .sequence(),
                expected_sequence
            );
        }

        let connection = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
        let (next_sequence, fence_token) = connection
            .query_row(
                "SELECT next_sequence, fence_token
                 FROM briskdb_hilo_leases
                 WHERE table_id = ?1",
                [i64::try_from(table_id.get()).unwrap()],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .unwrap();
        assert_eq!(
            next_sequence,
            i64::try_from(manifest::HILO_V1_BLOCK_SIZE * 2 + 1).unwrap()
        );
        assert_eq!(fence_token, 2);
    }

    #[test]
    fn real_process_abort_before_or_after_hilo_lease_commit_never_reuses_a_block() {
        for (boundary, expected_sequence) in [
            ("before-commit", 1),
            ("after-commit", manifest::HILO_V1_BLOCK_SIZE + 1),
        ] {
            let temp = tempfile::tempdir().unwrap();
            drop(create_registered_hilo_storage(temp.path(), 2));
            let status = Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("storage::tests::hilo_lease_process_child")
                .arg("--nocapture")
                .env("BRISKDB_HILO_PROCESS_ROOT", temp.path())
                .env("BRISKDB_HILO_LEASE_ABORT_POINT", boundary)
                .status()
                .unwrap();
            assert!(!status.success(), "child did not abort at {boundary}");

            let storage = Storage::open(temp.path(), 2).unwrap();
            let allocation = storage
                .allocate_hilo_v1(hilo_events_table_id(&storage))
                .unwrap();
            assert_eq!(
                crate::core::generated_id::HiloV1Id::decode(allocation.id())
                    .unwrap()
                    .sequence(),
                expected_sequence,
                "{boundary}"
            );
        }
    }

    #[test]
    fn competing_processes_receive_disjoint_hilo_blocks() {
        let temp = tempfile::tempdir().unwrap();
        drop(create_registered_hilo_storage(temp.path(), 2));
        let go = temp.path().join("go");
        let mut children = Vec::new();
        let mut ready_paths = Vec::new();
        let mut output_paths = Vec::new();
        for index in 0..2 {
            let ready = temp.path().join(format!("ready-{index}"));
            let output = temp.path().join(format!("allocation-{index}"));
            let child = Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("storage::tests::hilo_lease_process_child")
                .arg("--nocapture")
                .env("BRISKDB_HILO_PROCESS_ROOT", temp.path())
                .env("BRISKDB_HILO_PROCESS_READY", &ready)
                .env("BRISKDB_HILO_PROCESS_GO", &go)
                .env("BRISKDB_HILO_PROCESS_OUTPUT", &output)
                .spawn()
                .unwrap();
            children.push(child);
            ready_paths.push(ready);
            output_paths.push(output);
        }
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        while ready_paths.iter().any(|path| !path.exists()) && Instant::now() < deadline {
            thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(ready_paths.iter().all(|path| path.exists()));
        fs::write(&go, b"go").unwrap();
        for mut child in children {
            assert!(child.wait().unwrap().success());
        }
        let mut sequences = output_paths
            .iter()
            .map(|path| {
                let id = fs::read_to_string(path).unwrap().parse::<i64>().unwrap();
                crate::core::generated_id::HiloV1Id::decode(id)
                    .unwrap()
                    .sequence()
            })
            .collect::<Vec<_>>();
        sequences.sort_unstable();
        assert_eq!(sequences, [1, manifest::HILO_V1_BLOCK_SIZE + 1]);

        let storage = Storage::open(temp.path(), 2).unwrap();
        let next = storage
            .allocate_hilo_v1(hilo_events_table_id(&storage))
            .unwrap();
        assert_eq!(
            crate::core::generated_id::HiloV1Id::decode(next.id())
                .unwrap()
                .sequence(),
            manifest::HILO_V1_BLOCK_SIZE * 2 + 1
        );
    }

    #[test]
    fn post_commit_registration_ambiguity_stays_pending_until_stale_handle_closes() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 2).unwrap();
        create_registered_table_schema(&database);
        let declarations = registered_table_declarations(&database);
        let canonical_root = fs::canonicalize(temp.path()).unwrap();
        let coordination = root_schema_coordination(&canonical_root).unwrap();

        manifest::fail_next_table_registration_post_commit_for_test();
        let error = database.register_tables(declarations).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert!(database.catalog().tables().is_empty());
        assert_eq!(coordination.gate.snapshot().state, SchemaGateState::Pending);

        let conflict = Database::open(temp.path(), 2).unwrap_err();
        assert_eq!(conflict.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(coordination.gate.snapshot().state, SchemaGateState::Pending);
        assert_eq!(
            database
                .execute("tenant-one", "SELECT 1", &[])
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );

        drop(database);
        let reopened = Database::open(temp.path(), 2).unwrap();
        assert_eq!(reopened.catalog().tables().len(), 2);
        assert_eq!(coordination.gate.snapshot().state, SchemaGateState::Ready);
    }

    #[test]
    fn registration_manifest_corruption_degrades_every_live_alias() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 2).unwrap();
        create_registered_table_schema(&database);
        let alias = Database::open(temp.path(), 2).unwrap();
        let canonical_root = fs::canonicalize(temp.path()).unwrap();
        let coordination = root_schema_coordination(&canonical_root).unwrap();
        Connection::open(temp.path().join("manifest.sqlite"))
            .unwrap()
            .execute(
                "UPDATE briskdb_virtual_buckets
                 SET physical_shard_id = 1 - physical_shard_id
                 WHERE bucket_id = 0",
                [],
            )
            .unwrap();

        // Close the observer so exclusivity does not mask the checksummed
        // manifest read performed by the registration transaction.
        drop(alias);
        let error = database
            .register_tables(registered_table_declarations(&database))
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(
            coordination.gate.snapshot().state,
            SchemaGateState::Degraded
        );
        assert_eq!(
            database
                .execute("tenant-one", "SELECT 1", &[])
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn table_registration_rejects_sqlite_nullable_primary_key_forms() {
        for definition in [
            "key TEXT PRIMARY KEY",
            "key BIGINT PRIMARY KEY",
            "key INTEGER PRIMARY KEY DESC",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let mut database = Database::open(temp.path(), 2).unwrap();
            database
                .broadcast(&format!("CREATE TABLE nullable_keys ({definition})"))
                .unwrap();
            let logical_database = database.catalog().default_database().id();
            let key_type = if definition.starts_with("key TEXT") {
                ShardKeyType::Text
            } else {
                ShardKeyType::Int64
            };
            let declaration = TableDeclaration::sharded(
                logical_database,
                "nullable_keys",
                crate::core::ShardKeyMetadata::new("key", key_type).unwrap(),
            )
            .unwrap();

            assert_eq!(
                database
                    .register_tables(vec![declaration])
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::FailedPrecondition,
                "{definition}"
            );
            assert!(database.catalog().tables().is_empty());
        }
    }

    #[test]
    fn table_registration_rejects_nonlocal_constraints_collations_and_triggers() {
        for schema in [
            "CREATE TABLE records (
                 id INTEGER PRIMARY KEY,
                 tenant_id TEXT NOT NULL
             )",
            "CREATE TABLE records (
                 tenant_id TEXT COLLATE NOCASE PRIMARY KEY NOT NULL,
                 payload TEXT
             )",
            "CREATE TABLE records (
                 tenant_id TEXT PRIMARY KEY NOT NULL,
                 email TEXT NOT NULL UNIQUE
             )",
            "CREATE TABLE records (
                 tenant_id TEXT PRIMARY KEY NOT NULL,
                 email TEXT,
                 UNIQUE (tenant_id COLLATE NOCASE, email)
             )",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let mut database = Database::open(temp.path(), 2).unwrap();
            database.broadcast(schema).unwrap();
            let logical_database = database.catalog().default_database().id();
            let declaration = TableDeclaration::sharded(
                logical_database,
                "records",
                crate::core::ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap();
            assert_eq!(
                database
                    .register_tables(vec![declaration])
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::FailedPrecondition,
                "{schema}"
            );
            assert!(database.catalog().tables().is_empty());
        }

        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 2).unwrap();
        create_registered_table_schema(&database);
        database
            .broadcast(
                "CREATE TRIGGER events_copy AFTER INSERT ON events
                 BEGIN UPDATE events SET payload = NEW.payload
                 WHERE tenant_id = NEW.tenant_id AND id = NEW.id; END",
            )
            .unwrap();
        assert_eq!(
            database
                .register_tables(registered_table_declarations(&database))
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
    }

    #[test]
    fn registration_accepts_colocated_foreign_keys_and_sqlite_enforces_them_locally() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 2).unwrap();
        database
            .broadcast(
                "CREATE TABLE countries (
                     code TEXT PRIMARY KEY,
                     label TEXT NOT NULL
                 );
                 CREATE TABLE regions (
                     code TEXT PRIMARY KEY,
                     country_code TEXT NOT NULL REFERENCES countries(code)
                 );
                 CREATE TABLE parents (
                     tenant_id TEXT PRIMARY KEY NOT NULL
                 );
                 CREATE TABLE children (
                     tenant_id TEXT PRIMARY KEY NOT NULL,
                     country_code TEXT NOT NULL REFERENCES countries(code),
                     FOREIGN KEY (tenant_id) REFERENCES parents
                 );",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        let text_key =
            || crate::core::ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap();
        database
            .register_tables(vec![
                TableDeclaration::global(logical_database, "countries").unwrap(),
                TableDeclaration::sharded(logical_database, "children", text_key()).unwrap(),
                TableDeclaration::sharded(logical_database, "parents", text_key()).unwrap(),
                TableDeclaration::global(logical_database, "regions").unwrap(),
            ])
            .unwrap();

        let tenant = (0_u64..)
            .map(|candidate| format!("tenant-{candidate}"))
            .find(|candidate| database.shard_for_key(candidate.as_bytes()) == 0)
            .unwrap();
        for shard in 0..2 {
            let physical = Connection::open(shard_file(temp.path(), shard)).unwrap();
            physical.pragma_update(None, "foreign_keys", "ON").unwrap();
            assert_eq!(
                physical
                    .pragma_query_value(None, "foreign_keys", |row| row.get::<_, i64>(0))
                    .unwrap(),
                1
            );
            physical
                .execute("INSERT INTO countries VALUES ('US', 'United States')", [])
                .unwrap();
        }
        Connection::open(shard_file(temp.path(), 1))
            .unwrap()
            .execute("INSERT INTO parents VALUES (?1)", [&tenant])
            .unwrap();

        let values = [
            crate::core::Value::from(tenant.clone()),
            crate::core::Value::from("US"),
        ];
        let error = database
            .execute(
                &tenant,
                "INSERT INTO children (tenant_id, country_code) VALUES (?1, ?2)",
                &values,
            )
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::ForeignKeyViolation);
        assert_eq!(
            Connection::open(shard_file(temp.path(), 0))
                .unwrap()
                .query_row("SELECT COUNT(*) FROM children", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );

        database
            .execute(
                &tenant,
                "INSERT INTO parents (tenant_id) VALUES (?1)",
                &[crate::core::Value::from(tenant.clone())],
            )
            .unwrap();
        assert_eq!(
            database
                .execute(
                    &tenant,
                    "INSERT INTO children (tenant_id, country_code) VALUES (?1, ?2)",
                    &values,
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn registration_rejects_colocated_foreign_keys_with_different_generated_id_routing_domains() {
        for native_child in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let mut database = Database::open(temp.path(), 2).unwrap();
            database
                .broadcast(
                    "CREATE TABLE parents (id INTEGER PRIMARY KEY AUTOINCREMENT);
                     CREATE TABLE children (
                         id INTEGER PRIMARY KEY AUTOINCREMENT REFERENCES parents(id)
                     );",
                )
                .unwrap();

            // A native ID owned by shard 0 need not hash to shard 0 under the
            // legacy None policy. Equal SQLite values therefore prove
            // co-location only when both tables use the same routing domain.
            let owner = crate::core::generated_id::AllocationOwnerSlot::new(0).unwrap();
            let divergent = (1_u64..10_000)
                .map(|sequence| {
                    crate::core::generated_id::NativeRangeV1Id::new(owner, sequence)
                        .unwrap()
                        .encode()
                })
                .find(|id| database.shard_for_key(id.to_string().as_bytes()) != 0)
                .unwrap();
            assert_ne!(database.shard_for_key(divergent.to_string().as_bytes()), 0);

            let logical_database = database.catalog().default_database().id();
            let int_key = || crate::core::ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap();
            let mut child =
                TableDeclaration::sharded(logical_database, "children", int_key()).unwrap();
            let mut parent =
                TableDeclaration::sharded(logical_database, "parents", int_key()).unwrap();
            if native_child {
                child = child
                    .with_generated_id_policy(
                        crate::core::GeneratedIdPolicy::native_range_v1("id").unwrap(),
                    )
                    .unwrap();
            } else {
                parent = parent
                    .with_generated_id_policy(
                        crate::core::GeneratedIdPolicy::native_range_v1("id").unwrap(),
                    )
                    .unwrap();
            }

            let error = database.register_tables(vec![child, parent]).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
            assert!(
                error
                    .diagnostic()
                    .contains("different generated-ID routing domains"),
                "{}",
                error.diagnostic()
            );
            assert!(database.catalog().tables().is_empty());
        }
    }

    #[test]
    fn registration_rejects_foreign_keys_without_a_safe_placement_proof() {
        #[derive(Clone, Copy)]
        enum Placement<'a> {
            Sharded(&'a str, ShardKeyType),
            Global,
            Catalog,
        }

        let scenarios = [
            (
                "CREATE TABLE parents (
                     tenant_id TEXT NOT NULL,
                     parent_id INTEGER NOT NULL,
                     PRIMARY KEY (tenant_id, parent_id)
                 );
                 CREATE TABLE children (
                     tenant_id TEXT PRIMARY KEY NOT NULL,
                     parent_tenant TEXT NOT NULL,
                     parent_id INTEGER NOT NULL,
                     FOREIGN KEY (parent_tenant, parent_id)
                         REFERENCES parents (tenant_id, parent_id)
                 );",
                vec![
                    Placement::Sharded("tenant_id", ShardKeyType::Text),
                    Placement::Sharded("tenant_id", ShardKeyType::Text),
                ],
                vec!["children", "parents"],
                "does not map child shard key",
            ),
            (
                "CREATE TABLE parents (id INTEGER PRIMARY KEY);
                 CREATE TABLE children (
                     tenant_id TEXT PRIMARY KEY NOT NULL,
                     FOREIGN KEY (tenant_id) REFERENCES parents(id)
                 );",
                vec![
                    Placement::Sharded("tenant_id", ShardKeyType::Text),
                    Placement::Sharded("id", ShardKeyType::Int64),
                ],
                vec!["children", "parents"],
                "different authoritative types",
            ),
            (
                "CREATE TABLE parents (tenant_id TEXT PRIMARY KEY NOT NULL);
                 CREATE TABLE children (
                     id INTEGER PRIMARY KEY,
                     tenant_id TEXT NOT NULL REFERENCES parents(tenant_id)
                 );",
                vec![
                    Placement::Global,
                    Placement::Sharded("tenant_id", ShardKeyType::Text),
                ],
                vec!["children", "parents"],
                "Global child cannot reference a Sharded parent",
            ),
            (
                "CREATE TABLE children (
                     tenant_id TEXT PRIMARY KEY NOT NULL,
                     FOREIGN KEY (tenant_id) REFERENCES missing_parent(tenant_id)
                 );",
                vec![Placement::Sharded("tenant_id", ShardKeyType::Text)],
                vec!["children"],
                "missing authoritative table",
            ),
            (
                "CREATE TABLE children (
                     tenant_id TEXT PRIMARY KEY NOT NULL,
                     FOREIGN KEY (tenant_id) REFERENCES audit_catalog(id)
                 );",
                vec![
                    Placement::Catalog,
                    Placement::Sharded("tenant_id", ShardKeyType::Text),
                ],
                vec!["audit_catalog", "children"],
                "catalog-only table",
            ),
            (
                "CREATE TABLE parents (tenant_id TEXT PRIMARY KEY NOT NULL);
                 CREATE TABLE children (
                     tenant_id TEXT PRIMARY KEY NOT NULL,
                     FOREIGN KEY (tenant_id) REFERENCES parents(tenant_id)
                         ON UPDATE CASCADE
                 );",
                vec![
                    Placement::Sharded("tenant_id", ShardKeyType::Text),
                    Placement::Sharded("tenant_id", ShardKeyType::Text),
                ],
                vec!["children", "parents"],
                "ON UPDATE CASCADE",
            ),
        ];

        for (schema, placements, names, diagnostic) in scenarios {
            let temp = tempfile::tempdir().unwrap();
            let mut database = Database::open(temp.path(), 2).unwrap();
            database.broadcast(schema).unwrap();
            let logical_database = database.catalog().default_database().id();
            let declarations = names
                .into_iter()
                .zip(placements)
                .map(|(name, placement)| match placement {
                    Placement::Sharded(column, key_type) => TableDeclaration::sharded(
                        logical_database,
                        name,
                        crate::core::ShardKeyMetadata::new(column, key_type).unwrap(),
                    )
                    .unwrap(),
                    Placement::Global => TableDeclaration::global(logical_database, name).unwrap(),
                    Placement::Catalog => {
                        TableDeclaration::catalog(logical_database, name).unwrap()
                    }
                })
                .collect();

            let error = database.register_tables(declarations).unwrap_err();
            assert_eq!(
                error.kind(),
                EngineErrorKind::FailedPrecondition,
                "{schema}"
            );
            assert!(
                error.diagnostic().contains(diagnostic),
                "{schema}: {}",
                error.diagnostic()
            );
            assert!(database.catalog().tables().is_empty());
        }
    }

    #[test]
    fn table_registration_rejects_connection_local_functions_in_stored_expressions() {
        for expression in [
            "value INTEGER DEFAULT (total_changes())",
            "value INTEGER CHECK (changes() >= 0)",
            "value INTEGER CHECK (last_insert_rowid() >= 0)",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let mut database = Database::open(temp.path(), 2).unwrap();
            database
                .broadcast(&format!(
                    "CREATE TABLE records (
                         tenant_id TEXT NOT NULL PRIMARY KEY,
                         {expression}
                     )"
                ))
                .unwrap();
            let logical_database = database.catalog().default_database().id();
            let declaration = TableDeclaration::sharded(
                logical_database,
                "records",
                crate::core::ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap();

            let error = database.register_tables(vec![declaration]).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
            assert!(
                error
                    .diagnostic()
                    .contains("cannot participate in stateless catalog write reuse"),
                "{expression}: {}",
                error.diagnostic()
            );
            assert!(database.catalog().tables().is_empty());
        }
    }

    #[test]
    fn startup_rejects_a_preexisting_catalog_with_connection_local_schema_functions() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 2).unwrap();
        database
            .broadcast(
                "CREATE TABLE records (
                     tenant_id TEXT NOT NULL PRIMARY KEY,
                     value INTEGER CHECK(total_changes() >= 0)
                 )",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        let declaration = TableDeclaration::sharded(
            logical_database,
            "records",
            crate::core::ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
        )
        .unwrap();
        drop(database);

        let mut manifest_connection =
            open_existing_manifest(&temp.path().join("manifest.sqlite")).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        let simulated_older_catalog =
            manifest::register_table_catalog(&mut manifest_connection, 2, vec![declaration], || {})
                .unwrap();
        drop(simulated_older_catalog);
        drop(manifest_connection);

        let error = Database::open(temp.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(
            error
                .diagnostic()
                .contains("cannot participate in stateless catalog write reuse"),
            "{}",
            error.diagnostic()
        );
    }

    #[test]
    fn later_schema_corruption_is_not_masked_by_an_earlier_unsafe_legacy_schema() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 2).unwrap();
        database
            .broadcast(
                "CREATE TABLE records (
                     tenant_id TEXT NOT NULL PRIMARY KEY,
                     value INTEGER CHECK(total_changes() >= 0)
                 )",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        let declaration = TableDeclaration::sharded(
            logical_database,
            "records",
            crate::core::ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
        )
        .unwrap();
        drop(database);

        let manifest_path = temp.path().join("manifest.sqlite");
        let mut manifest_connection = open_existing_manifest(&manifest_path).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        let simulated_older_catalog =
            manifest::register_table_catalog(&mut manifest_connection, 2, vec![declaration], || {})
                .unwrap();
        drop(simulated_older_catalog);
        drop(manifest_connection);

        Connection::open(temp.path().join("shards/0001.sqlite"))
            .unwrap()
            .execute_batch("CREATE TABLE later_shard_tamper(id INTEGER PRIMARY KEY)")
            .unwrap();

        let error = Database::open(temp.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(
            Connection::open(manifest_path)
                .unwrap()
                .query_row(
                    "SELECT database_state FROM briskdb_integrity WHERE singleton = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            4,
            "trusted later-shard corruption must persist terminal Degraded state"
        );
    }

    #[test]
    fn virtual_tables_are_never_classified_as_ordinary_physical_tables() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch("CREATE VIRTUAL TABLE search_records USING fts5(body)")
            .unwrap();
        let names = application_table_names(&connection).unwrap();
        assert!(names.contains("search_records"));
        assert!(!names.contains("search_records_data"));
    }
}
