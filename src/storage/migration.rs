//! Crash-resumable application-schema migration coordination.

use std::{
    cell::RefCell,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(test)]
use std::cell::Cell;
#[cfg(test)]
use std::{
    collections::{HashMap, hash_map::Entry},
    sync::{Mutex, OnceLock, mpsc},
};

use rusqlite::Connection;

use crate::{
    core::{
        CancellationReason, CatalogSnapshot, EngineError, EngineErrorKind, EngineResult,
        OperationControl,
    },
    sqlite_error,
};

use super::{
    CONNECTION_BUSY_TIMEOUT, RootSchemaCoordination, SchemaMigrationGuard, Storage,
    configure_journal_mode, configure_manifest_connection,
    configure_manifest_connection_after_busy_setup, manifest,
    manifest::{SchemaMigration, SchemaMigrationClassification},
    open_existing_manifest, shard,
};

thread_local! {
    static MIGRATION_BUSY_OPERATION: RefCell<Option<MigrationBusyOperation>> = const {
        RefCell::new(None)
    };
}

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_MANIFEST_COMMIT_CLEANUP: Cell<bool> = const { Cell::new(false) };
    static CANCEL_AFTER_CONTROLLED_CORRUPTION: Cell<bool> = const { Cell::new(false) };
}

struct MigrationBusyOperation {
    control: Arc<OperationControl>,
    started: Instant,
}

struct MigrationBusyGuard;

impl MigrationBusyGuard {
    fn install(control: Arc<OperationControl>) -> Self {
        MIGRATION_BUSY_OPERATION.with(|operation| {
            let previous = operation.replace(Some(MigrationBusyOperation {
                control,
                started: Instant::now(),
            }));
            debug_assert!(
                previous.is_none(),
                "migration SQLite controls cannot be nested"
            );
        });
        Self
    }
}

impl Drop for MigrationBusyGuard {
    fn drop(&mut self) {
        MIGRATION_BUSY_OPERATION.with(|operation| {
            operation.replace(None);
        });
    }
}

fn cancellable_busy_handler(attempt: i32) -> bool {
    MIGRATION_BUSY_OPERATION.with(|operation| {
        let operation = operation.borrow();
        let Some(operation) = operation.as_ref() else {
            return false;
        };
        if operation.control.should_stop() || operation.started.elapsed() >= CONNECTION_BUSY_TIMEOUT
        {
            return false;
        }
        let delay = match attempt {
            ..=9 => Duration::from_millis(1),
            10..=19 => Duration::from_millis(5),
            _ => Duration::from_millis(10),
        };
        std::thread::sleep(
            delay.min(CONNECTION_BUSY_TIMEOUT.saturating_sub(operation.started.elapsed())),
        );
        !operation.control.should_stop() && operation.started.elapsed() < CONNECTION_BUSY_TIMEOUT
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SchemaMigrationCoordinatorPoint {
    JournalCommitted,
    ShardCommitted(u16),
    ProgressPrepared(u16),
    ProgressCommitted(u16),
    FinalizationPrepared,
    FinalizationCommitted,
}

#[cfg(test)]
struct SchemaMigrationTestBlock {
    point: SchemaMigrationCoordinatorPoint,
    started: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

#[cfg(test)]
static SCHEMA_MIGRATION_TEST_BLOCKS: OnceLock<Mutex<HashMap<PathBuf, SchemaMigrationTestBlock>>> =
    OnceLock::new();

#[cfg(test)]
pub(super) fn install_schema_migration_test_block(
    root: &Path,
    point: SchemaMigrationCoordinatorPoint,
    started: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
) -> EngineResult<()> {
    let blocks = SCHEMA_MIGRATION_TEST_BLOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut blocks = blocks.lock().map_err(|error| {
        EngineError::new(
            EngineErrorKind::Internal,
            format!("schema-migration test coordination is poisoned: {error}"),
        )
    })?;
    match blocks.entry(root.to_path_buf()) {
        Entry::Vacant(entry) => {
            entry.insert(SchemaMigrationTestBlock {
                point,
                started,
                release,
            });
            Ok(())
        }
        Entry::Occupied(_) => Err(EngineError::new(
            EngineErrorKind::Internal,
            "a schema-migration test block is already installed for this root",
        )),
    }
}

#[cfg(test)]
fn run_schema_migration_test_block(
    root: &Path,
    point: SchemaMigrationCoordinatorPoint,
) -> EngineResult<()> {
    let Some(blocks) = SCHEMA_MIGRATION_TEST_BLOCKS.get() else {
        return Ok(());
    };
    let block = {
        let mut blocks = blocks.lock().map_err(|error| {
            EngineError::new(
                EngineErrorKind::Internal,
                format!("schema-migration test coordination is poisoned: {error}"),
            )
        })?;
        if blocks.get(root).is_some_and(|block| block.point == point) {
            blocks.remove(root)
        } else {
            None
        }
    };
    let Some(block) = block else {
        return Ok(());
    };
    block.started.send(()).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::Internal,
            "failed to signal the schema-migration test block",
            error,
        )
    })?;
    block.release.recv().map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::Internal,
            "failed to release the schema-migration test block",
            error,
        )
    })
}

struct SchemaMigrationCoordinator<'a> {
    root: &'a Path,
    manifest_connection: &'a mut Connection,
    requested_shards: u16,
    catalog: &'a CatalogSnapshot,
    schema_coordination: &'a RootSchemaCoordination,
    layout: &'a shard::ShardLayout,
    control: Option<Arc<OperationControl>>,
}

pub(super) fn resume_schema_migration_on_startup(
    root: &Path,
    manifest_connection: &mut Connection,
    requested_shards: u16,
    catalog: &CatalogSnapshot,
    layout: &shard::ShardLayout,
    schema_coordination: &RootSchemaCoordination,
    migration: SchemaMigration,
) -> EngineResult<()> {
    let mut no_hook = |_| Ok(());
    resume_active_schema_migration(
        SchemaMigrationCoordinator {
            root,
            manifest_connection,
            requested_shards,
            catalog,
            schema_coordination,
            layout,
            control: None,
        },
        migration,
        &mut no_hook,
    )
}

pub(super) fn apply_schema_migration(
    storage: &Storage,
    sql: &str,
    guard: &mut SchemaMigrationGuard,
    control: Option<Arc<OperationControl>>,
) -> EngineResult<Vec<u16>> {
    #[cfg(test)]
    let result = {
        apply_schema_migration_with_hook(storage, sql, guard, control, |point| {
            run_schema_migration_test_block(&storage.root, point)
        })
    };
    #[cfg(not(test))]
    let result = { apply_schema_migration_with_hook(storage, sql, guard, control, |_| Ok(())) };
    if result
        .as_ref()
        .is_err_and(|error| error.kind() == EngineErrorKind::DataCorruption)
    {
        storage.record_schema_degraded();
    }
    result
}

fn apply_schema_migration_with_hook<F>(
    storage: &Storage,
    sql: &str,
    guard: &mut SchemaMigrationGuard,
    control: Option<Arc<OperationControl>>,
    mut hook: F,
) -> EngineResult<Vec<u16>>
where
    F: FnMut(SchemaMigrationCoordinatorPoint) -> EngineResult<()>,
{
    check_cancelled(control.as_deref())?;
    // Validate public input before touching any shard or creating a journal.
    let _ = manifest::schema_migration_id(sql)?;

    let manifest_path = storage.root.join("manifest.sqlite");
    let mut manifest_connection = open_existing_manifest(&manifest_path)?;
    match control.as_ref() {
        Some(control) => run_connection_controlled(
            &mut manifest_connection,
            Arc::clone(control),
            |connection| {
                configure_manifest_connection_after_busy_setup(connection)?;
                configure_journal_mode(connection)
            },
        )?,
        None => {
            configure_manifest_connection(&manifest_connection)?;
            configure_journal_mode(&manifest_connection)?;
        }
    }

    match classify_schema_migration(
        &mut manifest_connection,
        storage.shard_count(),
        &storage.shard_layout,
        sql,
        control.as_ref(),
    )? {
        SchemaMigrationClassification::Complete(migration) => {
            if let Some(integrity) = current_integrity_optional_controlled(
                &mut manifest_connection,
                storage.shard_count(),
                control.as_ref(),
            )? {
                storage.schema_coordination.publish_schema_digests(
                    integrity.committed_schema_digest(),
                    integrity.target_schema_digest(),
                )?;
            }
            finish_completed_schema_migration(storage, &migration, control.as_ref())?;
            return Ok(all_shards(storage.shard_count()));
        }
        SchemaMigrationClassification::Active(migration) => {
            if let Some(integrity) = current_integrity_optional_controlled(
                &mut manifest_connection,
                storage.shard_count(),
                control.as_ref(),
            )? {
                storage.schema_coordination.publish_schema_digests(
                    integrity.committed_schema_digest(),
                    integrity.target_schema_digest(),
                )?;
            }
            guard.mark_pending_on_drop();
            resume_active_schema_migration(
                SchemaMigrationCoordinator {
                    root: &storage.root,
                    manifest_connection: &mut manifest_connection,
                    requested_shards: storage.shard_count(),
                    catalog: &storage.catalog,
                    schema_coordination: &storage.schema_coordination,
                    layout: &storage.shard_layout,
                    control,
                },
                migration,
                &mut hook,
            )?;
            return Ok(all_shards(storage.shard_count()));
        }
        SchemaMigrationClassification::Absent => {}
    }

    let source_generation = storage.current_schema_generation();
    let target_generation = source_generation.checked_add(1).ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "schema migration generation is exhausted",
        )
    })?;
    let shards_dir = storage.root.join("shards");
    let source_digest = storage.schema_coordination.committed_schema_digest()?;
    let mut target_digest = None;
    for shard_id in 0..storage.shard_count() {
        check_cancelled(control.as_deref())?;
        let path = shard_path(&shards_dir, shard_id);
        let (state, observed_target_digest) = preflight_one_shard(
            ShardMigrationRequest {
                path: &path,
                shard_id,
                source_generation,
                target_generation,
                layout: &storage.shard_layout,
                sql,
                control: control.as_ref(),
            },
            &source_digest,
        )
        .map_err(|error| sanitized_shard_error(error, "preflight", shard_id))?;
        if state != shard::SchemaMigrationShardState::Source {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "schema migration preflight found shard {shard_id} at the target generation without an active journal"
                ),
            ));
        }
        if target_digest.is_some_and(|expected| expected != observed_target_digest) {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "schema migration produced inconsistent target fingerprints across shards",
            ));
        }
        target_digest = Some(observed_target_digest);
    }
    check_cancelled(control.as_deref())?;
    let target_digest = target_digest.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "schema migration preflight did not inspect any shards",
        )
    })?;

    let migration = begin_schema_migration(
        &mut manifest_connection,
        BeginSchemaMigrationRequest {
            requested_shards: storage.shard_count(),
            layout: &storage.shard_layout,
            source_generation,
            sql,
            source_digest,
            target_digest,
            control: control.as_ref(),
        },
        guard,
    )?;
    storage
        .schema_coordination
        .publish_schema_digests(Some(source_digest), Some(target_digest))?;
    if migration.is_complete() {
        finish_completed_schema_migration(storage, &migration, control.as_ref())?;
        return Ok(all_shards(storage.shard_count()));
    }
    hook(SchemaMigrationCoordinatorPoint::JournalCommitted)?;
    resume_active_schema_migration(
        SchemaMigrationCoordinator {
            root: &storage.root,
            manifest_connection: &mut manifest_connection,
            requested_shards: storage.shard_count(),
            catalog: &storage.catalog,
            schema_coordination: &storage.schema_coordination,
            layout: &storage.shard_layout,
            control,
        },
        migration,
        &mut hook,
    )?;
    Ok(all_shards(storage.shard_count()))
}

fn classify_schema_migration(
    connection: &mut Connection,
    requested_shards: u16,
    layout: &shard::ShardLayout,
    sql: &str,
    control: Option<&Arc<OperationControl>>,
) -> EngineResult<SchemaMigrationClassification> {
    let mut transaction = ManifestTransaction::begin(connection, control)?;
    let classification = transaction.run(|connection| {
        manifest::ensure_schema_migration_layout(connection, requested_shards, layout)?;
        manifest::classify_schema_migration_in_transaction(connection, requested_shards, sql)
    })?;
    transaction.commit()?;
    Ok(classification)
}

fn current_integrity_optional_controlled(
    connection: &mut Connection,
    requested_shards: u16,
    control: Option<&Arc<OperationControl>>,
) -> EngineResult<Option<manifest::ManifestIntegrity>> {
    match control {
        Some(control) => run_connection_controlled(connection, Arc::clone(control), |connection| {
            manifest::current_integrity_optional(connection, requested_shards)
        }),
        None => manifest::current_integrity_optional(connection, requested_shards),
    }
}

struct BeginSchemaMigrationRequest<'a> {
    requested_shards: u16,
    layout: &'a shard::ShardLayout,
    source_generation: u64,
    sql: &'a str,
    source_digest: [u8; 32],
    target_digest: [u8; 32],
    control: Option<&'a Arc<OperationControl>>,
}

fn begin_schema_migration(
    connection: &mut Connection,
    request: BeginSchemaMigrationRequest<'_>,
    guard: &mut SchemaMigrationGuard,
) -> EngineResult<SchemaMigration> {
    let BeginSchemaMigrationRequest {
        requested_shards,
        layout,
        source_generation,
        sql,
        source_digest,
        target_digest,
        control,
    } = request;
    let mut transaction = ManifestTransaction::begin(connection, control)?;
    let migration = transaction.run(|connection| {
        manifest::ensure_schema_migration_layout(connection, requested_shards, layout)?;
        manifest::begin_schema_migration_with_digests_in_transaction(
            connection,
            requested_shards,
            source_generation,
            sql,
            source_digest,
            target_digest,
        )
    })?;
    transaction.commit_with_durability_boundary(|| guard.mark_pending_on_drop())?;
    Ok(migration)
}

struct ManifestTransaction<'a> {
    connection: &'a mut Connection,
    control: Option<Arc<OperationControl>>,
    active: bool,
}

impl<'a> ManifestTransaction<'a> {
    fn begin(
        connection: &'a mut Connection,
        control: Option<&Arc<OperationControl>>,
    ) -> EngineResult<Self> {
        check_cancelled(control.map(Arc::as_ref))?;
        match control {
            Some(control) => {
                run_connection_controlled(connection, Arc::clone(control), |connection| {
                    connection
                        .execute_batch("BEGIN IMMEDIATE")
                        .map_err(sqlite_error::storage)
                })?
            }
            None => connection
                .execute_batch("BEGIN IMMEDIATE")
                .map_err(sqlite_error::storage)?,
        }
        Ok(Self {
            connection,
            control: control.cloned(),
            active: true,
        })
    }

    fn run<T>(&mut self, work: impl FnOnce(&Connection) -> EngineResult<T>) -> EngineResult<T> {
        match self.control.as_ref() {
            Some(control) => {
                let control = Arc::clone(control);
                run_connection_controlled(self.connection, control, |connection| work(connection))
            }
            None => work(self.connection),
        }
    }

    fn commit(self) -> EngineResult<()> {
        self.commit_with_durability_boundary(|| {})
    }

    fn commit_with_durability_boundary(
        mut self,
        on_commit_attempted: impl FnOnce(),
    ) -> EngineResult<()> {
        let mut committed = false;
        let result = self.run(|connection| {
            // Once COMMIT is attempted, an I/O error can be durability-
            // ambiguous. Publish the caller's fail-closed state immediately
            // before entering SQLite, but only after request controls have
            // allowed the work closure to start.
            on_commit_attempted();
            connection
                .execute_batch("COMMIT")
                .map_err(sqlite_error::storage)?;
            committed = true;
            Ok(())
        });
        #[cfg(test)]
        let result = {
            let inject = FAIL_NEXT_MANIFEST_COMMIT_CLEANUP.with(|fail| fail.replace(false));
            if inject && result.is_ok() {
                Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "injected manifest post-commit cleanup failure",
                ))
            } else {
                result
            }
        };
        if committed {
            self.active = false;
        }
        result
    }

    fn rollback(mut self) -> EngineResult<()> {
        let result = self
            .connection
            .execute_batch("ROLLBACK")
            .map_err(sqlite_error::storage);
        if result.is_ok() {
            self.active = false;
        }
        result
    }
}

impl Drop for ManifestTransaction<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self.connection.execute_batch("ROLLBACK");
        }
    }
}

fn resume_active_schema_migration<F>(
    coordinator: SchemaMigrationCoordinator<'_>,
    mut migration: SchemaMigration,
    hook: &mut F,
) -> EngineResult<()>
where
    F: FnMut(SchemaMigrationCoordinatorPoint) -> EngineResult<()>,
{
    let SchemaMigrationCoordinator {
        root,
        manifest_connection,
        requested_shards,
        catalog,
        schema_coordination,
        layout,
        control,
    } = coordinator;
    ensure_active_matches_catalog(&migration, catalog, requested_shards)?;
    let shards_dir = root.join("shards");
    let integrity = current_integrity_optional_controlled(
        manifest_connection,
        requested_shards,
        control.as_ref(),
    )?;
    let schema_digests = if let Some(integrity) = integrity {
        if integrity.state() != manifest::DatabaseIntegrityState::Migrating {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "active schema migration is not in its durable migrating state",
            ));
        }
        let source = integrity.committed_schema_digest().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                "active schema migration is missing its source fingerprint",
            )
        })?;
        let target = integrity.target_schema_digest().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                "active schema migration is missing its target fingerprint",
            )
        })?;
        schema_coordination.publish_schema_digests(Some(source), Some(target))?;
        let target = schema_coordination.target_schema_digest()?.ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::Internal,
                "root coordination did not publish the migration target fingerprint",
            )
        })?;
        validate_schema_migration_prefix_with_digests(
            &shards_dir,
            requested_shards,
            migration.next_shard(),
            migration.source_generation(),
            migration.target_generation(),
            layout,
            &source,
            &target,
            control.as_ref(),
        )?;
        Some((source, target))
    } else {
        validate_schema_migration_prefix(
            &shards_dir,
            requested_shards,
            migration.next_shard(),
            migration.source_generation(),
            migration.target_generation(),
            layout,
            control.as_ref(),
        )?;
        None
    };

    while migration.next_shard() < requested_shards {
        check_cancelled(control.as_deref())?;
        let mut transaction = ManifestTransaction::begin(manifest_connection, control.as_ref())?;
        let Some(locked) = transaction.run(|connection| {
            manifest::load_active_schema_migration_in_transaction(connection, requested_shards)
        })?
        else {
            transaction.rollback()?;
            return finish_completed_by_sql(
                manifest_connection,
                requested_shards,
                schema_coordination,
                layout,
                &shards_dir,
                migration.sql_text(),
                control.as_ref(),
            );
        };
        ensure_same_migration(&migration, &locked)?;
        migration = locked;
        if migration.next_shard() == requested_shards {
            transaction.commit()?;
            break;
        }

        let shard_id = migration.next_shard();
        let path = shard_path(&shards_dir, shard_id);
        apply_one_shard(
            ShardMigrationRequest {
                path: &path,
                shard_id,
                source_generation: migration.source_generation(),
                target_generation: migration.target_generation(),
                layout,
                sql: migration.sql_text(),
                control: control.as_ref(),
            },
            schema_digests.as_ref().map(|(_, target)| target),
        )
        .map_err(|error| sanitized_shard_error(error, "apply", shard_id))?;
        hook(SchemaMigrationCoordinatorPoint::ShardCommitted(shard_id))?;
        check_cancelled(control.as_deref())?;

        let next_shard = shard_id + 1;
        migration = transaction.run(|connection| {
            manifest::advance_schema_migration_in_transaction(
                connection,
                requested_shards,
                &migration,
                next_shard,
            )
        })?;
        hook(SchemaMigrationCoordinatorPoint::ProgressPrepared(shard_id))?;
        transaction.commit()?;
        hook(SchemaMigrationCoordinatorPoint::ProgressCommitted(shard_id))?;
    }

    check_cancelled(control.as_deref())?;
    if let Some((source_digest, target_digest)) = schema_digests.as_ref() {
        validate_schema_migration_prefix_with_digests(
            &shards_dir,
            requested_shards,
            requested_shards,
            migration.source_generation(),
            migration.target_generation(),
            layout,
            source_digest,
            target_digest,
            control.as_ref(),
        )?;
    } else {
        validate_schema_migration_prefix(
            &shards_dir,
            requested_shards,
            requested_shards,
            migration.source_generation(),
            migration.target_generation(),
            layout,
            control.as_ref(),
        )?;
    }

    let mut transaction = ManifestTransaction::begin(manifest_connection, control.as_ref())?;
    let Some(locked) = transaction.run(|connection| {
        manifest::load_active_schema_migration_in_transaction(connection, requested_shards)
    })?
    else {
        transaction.rollback()?;
        return finish_completed_by_sql(
            manifest_connection,
            requested_shards,
            schema_coordination,
            layout,
            &shards_dir,
            migration.sql_text(),
            control.as_ref(),
        );
    };
    ensure_same_migration(&migration, &locked)?;
    let completed = transaction.run(|connection| {
        manifest::finalize_schema_migration_in_transaction(connection, requested_shards, &locked)
    })?;
    hook(SchemaMigrationCoordinatorPoint::FinalizationPrepared)?;
    transaction.commit()?;
    hook(SchemaMigrationCoordinatorPoint::FinalizationCommitted)?;
    schema_coordination
        .publish_schema_generation(completed.source_generation(), completed.target_generation())?;
    if let Some((_, target_digest)) = schema_digests {
        schema_coordination.publish_schema_digests(Some(target_digest), None)?;
    }
    Ok(())
}

fn finish_completed_by_sql(
    manifest_connection: &mut Connection,
    requested_shards: u16,
    schema_coordination: &RootSchemaCoordination,
    layout: &shard::ShardLayout,
    shards_dir: &Path,
    sql: &str,
    control: Option<&Arc<OperationControl>>,
) -> EngineResult<()> {
    let completed = match classify_schema_migration(
        manifest_connection,
        requested_shards,
        layout,
        sql,
        control,
    )? {
        SchemaMigrationClassification::Complete(completed) => completed,
        SchemaMigrationClassification::Active(_) | SchemaMigrationClassification::Absent => {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "schema migration changed while it was being applied",
            ));
        }
    };
    if let Some(integrity) =
        current_integrity_optional_controlled(manifest_connection, requested_shards, control)?
    {
        let target_digest = integrity.committed_schema_digest().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                "completed schema migration is missing its committed fingerprint",
            )
        })?;
        validate_schema_migration_prefix_with_digests(
            shards_dir,
            requested_shards,
            requested_shards,
            completed.source_generation(),
            completed.target_generation(),
            layout,
            &target_digest,
            &target_digest,
            control,
        )?;
        schema_coordination.publish_schema_digests(Some(target_digest), None)?;
    } else {
        validate_schema_migration_prefix(
            shards_dir,
            requested_shards,
            requested_shards,
            completed.source_generation(),
            completed.target_generation(),
            layout,
            control,
        )?;
    }
    schema_coordination
        .publish_schema_generation(completed.source_generation(), completed.target_generation())
}

fn finish_completed_schema_migration(
    storage: &Storage,
    migration: &SchemaMigration,
    control: Option<&Arc<OperationControl>>,
) -> EngineResult<()> {
    let current = storage.current_schema_generation();
    if current <= migration.target_generation() {
        if current != migration.source_generation() && current != migration.target_generation() {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "completed schema migration does not match the loaded catalog generation",
            ));
        }
        let target_digest = storage.schema_coordination.committed_schema_digest()?;
        validate_schema_migration_prefix_with_digests(
            &storage.root.join("shards"),
            storage.shard_count(),
            storage.shard_count(),
            migration.source_generation(),
            migration.target_generation(),
            &storage.shard_layout,
            &target_digest,
            &target_digest,
            control,
        )?;
        return storage.publish_schema_generation(
            migration.source_generation(),
            migration.target_generation(),
        );
    }

    // A retained older history row remains idempotent after later migrations.
    // Validate every shard at the live generation rather than requiring it to
    // regress to this row's historical target.
    for shard_id in 0..storage.shard_count() {
        check_cancelled(control.map(Arc::as_ref))?;
        match control {
            None => drop(storage.open_shard(shard_id)?),
            Some(control) => {
                let mut connection = storage.open_unconfigured_shard(shard_id)?;
                run_connection_controlled(&mut connection, Arc::clone(control), |connection| {
                    storage.validate_unconfigured_shard(connection, shard_id)
                })?;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_schema_migration_prefix(
    shards_dir: &Path,
    shard_count: u16,
    next_shard: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &shard::ShardLayout,
    control: Option<&Arc<OperationControl>>,
) -> EngineResult<Option<shard::SchemaMigrationShardState>> {
    let Some(control) = control else {
        return shard::validate_schema_migration_prefix(
            shards_dir,
            shard_count,
            next_shard,
            source_generation,
            target_generation,
            layout,
        );
    };
    shard::validate_schema_migration_prefix_with(
        shards_dir,
        shard_count,
        next_shard,
        source_generation,
        target_generation,
        layout,
        |path, shard_id| {
            let mut connection = shard::open_required_file(path)?;
            run_connection_controlled(&mut connection, Arc::clone(control), |connection| {
                shard::validate_schema_migration_connection(
                    connection,
                    path,
                    shard_id,
                    source_generation,
                    target_generation,
                    layout,
                )
            })
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn validate_schema_migration_prefix_with_digests(
    shards_dir: &Path,
    shard_count: u16,
    next_shard: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &shard::ShardLayout,
    source_digest: &[u8; 32],
    target_digest: &[u8; 32],
    control: Option<&Arc<OperationControl>>,
) -> EngineResult<Option<shard::SchemaMigrationShardState>> {
    shard::validate_schema_migration_prefix_with(
        shards_dir,
        shard_count,
        next_shard,
        source_generation,
        target_generation,
        layout,
        |path, shard_id| {
            let mut connection = shard::open_required_file(path)?;
            let validate = |connection: &mut Connection| {
                let state = shard::validate_schema_migration_connection(
                    connection,
                    path,
                    shard_id,
                    source_generation,
                    target_generation,
                    layout,
                )?;
                let (generation, expected) = match state {
                    shard::SchemaMigrationShardState::Source => (source_generation, source_digest),
                    shard::SchemaMigrationShardState::Target => (target_generation, target_digest),
                };
                shard::verify_schema_digest(connection, generation, expected)?;
                Ok(state)
            };
            match control {
                Some(control) => {
                    run_connection_controlled(&mut connection, Arc::clone(control), validate)
                }
                None => {
                    connection
                        .busy_timeout(CONNECTION_BUSY_TIMEOUT)
                        .map_err(sqlite_error::storage)?;
                    validate(&mut connection)
                }
            }
        },
    )
}

struct ShardMigrationRequest<'a> {
    path: &'a Path,
    shard_id: u16,
    source_generation: u64,
    target_generation: u64,
    layout: &'a shard::ShardLayout,
    sql: &'a str,
    control: Option<&'a Arc<OperationControl>>,
}

fn preflight_one_shard(
    request: ShardMigrationRequest<'_>,
    expected_source_digest: &[u8; 32],
) -> EngineResult<(shard::SchemaMigrationShardState, [u8; 32])> {
    let ShardMigrationRequest {
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
        sql,
        control,
    } = request;
    match control {
        None => {
            let mut connection = shard::open_required_file(path)?;
            connection
                .busy_timeout(CONNECTION_BUSY_TIMEOUT)
                .map_err(sqlite_error::storage)?;
            shard::validate_schema_migration_connection(
                &connection,
                path,
                shard_id,
                source_generation,
                target_generation,
                layout,
            )?;
            shard::verify_schema_digest(&connection, source_generation, expected_source_digest)?;
            shard::preflight_schema_migration_on_connection_with_digest(
                &mut connection,
                path,
                shard_id,
                source_generation,
                target_generation,
                layout,
                sql,
            )
        }
        Some(control) => {
            let mut connection = shard::open_required_file(path)?;
            run_connection_controlled(&mut connection, Arc::clone(control), |connection| {
                shard::validate_schema_migration_connection(
                    connection,
                    path,
                    shard_id,
                    source_generation,
                    target_generation,
                    layout,
                )?;
                shard::verify_schema_digest(connection, source_generation, expected_source_digest)?;
                shard::preflight_schema_migration_on_connection_with_digest(
                    connection,
                    path,
                    shard_id,
                    source_generation,
                    target_generation,
                    layout,
                    sql,
                )
            })
        }
    }
}

fn apply_one_shard(
    request: ShardMigrationRequest<'_>,
    expected_target_digest: Option<&[u8; 32]>,
) -> EngineResult<()> {
    let ShardMigrationRequest {
        path,
        shard_id,
        source_generation,
        target_generation,
        layout,
        sql,
        control,
    } = request;
    match control {
        None => match expected_target_digest {
            Some(expected) => shard::apply_schema_migration_with_digest(
                path,
                shard_id,
                source_generation,
                target_generation,
                layout,
                sql,
                expected,
            )
            .map(|_| ()),
            None => shard::apply_schema_migration(
                path,
                shard_id,
                source_generation,
                target_generation,
                layout,
                sql,
            )
            .map(|_| ()),
        },
        Some(control) => {
            let mut connection = shard::open_required_file(path)?;
            run_connection_controlled(&mut connection, Arc::clone(control), |connection| {
                match expected_target_digest {
                    Some(expected) => shard::apply_schema_migration_on_connection_with_digest(
                        connection,
                        path,
                        shard_id,
                        source_generation,
                        target_generation,
                        layout,
                        sql,
                        expected,
                    )
                    .map(|_| ()),
                    None => shard::apply_schema_migration_on_connection(
                        connection,
                        path,
                        shard_id,
                        source_generation,
                        target_generation,
                        layout,
                        sql,
                    )
                    .map(|_| ()),
                }
            })
        }
    }
}

fn run_connection_controlled<T, F>(
    connection: &mut Connection,
    control: Arc<OperationControl>,
    work: F,
) -> EngineResult<T>
where
    F: FnOnce(&mut Connection) -> EngineResult<T>,
{
    let _busy_operation = MigrationBusyGuard::install(Arc::clone(&control));
    connection
        .busy_handler(Some(cancellable_busy_handler))
        .map_err(sqlite_error::storage)?;
    let progress_control = Arc::clone(&control);
    if let Err(error) =
        connection.progress_handler(1_000, Some(move || progress_control.should_stop()))
    {
        let _ = connection.busy_timeout(CONNECTION_BUSY_TIMEOUT);
        return Err(sqlite_error::storage(error)
            .context("failed to install the schema-migration progress hook"));
    }
    let interrupt_handle = connection.get_interrupt_handle();
    if let Err(reason) = control.arm(Arc::new(move || interrupt_handle.interrupt())) {
        let _ = connection.progress_handler(0, None::<fn() -> bool>);
        let _ = connection.busy_timeout(CONNECTION_BUSY_TIMEOUT);
        return Err(reason.error());
    }

    let outcome = catch_unwind(AssertUnwindSafe(|| work(connection)));
    #[cfg(test)]
    if matches!(
        &outcome,
        Ok(Err(error)) if error.kind() == EngineErrorKind::DataCorruption
    ) && CANCEL_AFTER_CONTROLLED_CORRUPTION.with(|cancel| cancel.replace(false))
    {
        control.request_cancel(CancellationReason::Cancelled);
    }
    let progress_cleanup = connection
        .progress_handler(0, None::<fn() -> bool>)
        .map_err(sqlite_error::storage);
    let busy_cleanup = connection
        .busy_timeout(CONNECTION_BUSY_TIMEOUT)
        .map_err(sqlite_error::storage);
    let reason = control.disarm();

    let result = match outcome {
        Ok(result) => result,
        Err(payload) => {
            let _ = progress_cleanup;
            let _ = busy_cleanup;
            resume_unwind(payload)
        }
    };
    match (result, progress_cleanup, busy_cleanup, reason) {
        (Ok(_), Err(error), _, _) => {
            Err(error.context("failed to remove the schema-migration progress hook"))
        }
        (Ok(_), Ok(()), Err(error), _) => {
            Err(error.context("failed to restore the schema-migration busy timeout"))
        }
        (Ok(value), Ok(()), Ok(()), _) => Ok(value),
        (Err(error), _, _, _) if error.kind() == EngineErrorKind::DataCorruption => Err(error),
        (Err(_), _, _, Some(reason)) => Err(reason.error()),
        (Err(error), _, _, None) => Err(error),
    }
}

fn ensure_active_matches_catalog(
    migration: &SchemaMigration,
    catalog: &CatalogSnapshot,
    requested_shards: u16,
) -> EngineResult<()> {
    if !migration.is_applying()
        || migration.shard_count() != requested_shards
        || migration.source_generation() != catalog.logical().schema_generation()
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "active schema migration does not match the loaded catalog",
        ));
    }
    Ok(())
}

fn ensure_same_migration(
    expected: &SchemaMigration,
    observed: &SchemaMigration,
) -> EngineResult<()> {
    if expected.source_generation() != observed.source_generation()
        || expected.target_generation() != observed.target_generation()
        || expected.migration_id() != observed.migration_id()
        || expected.sql_text() != observed.sql_text()
        || expected.shard_count() != observed.shard_count()
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "schema migration identity changed while it was being applied",
        ));
    }
    Ok(())
}

fn check_cancelled(control: Option<&OperationControl>) -> EngineResult<()> {
    match control.and_then(OperationControl::reason) {
        Some(CancellationReason::Cancelled) => Err(CancellationReason::Cancelled.error()),
        Some(CancellationReason::DeadlineExceeded) => {
            Err(CancellationReason::DeadlineExceeded.error())
        }
        None => Ok(()),
    }
}

fn sanitized_shard_error(error: EngineError, phase: &str, shard_id: u16) -> EngineError {
    EngineError::new(
        error.kind(),
        format!("schema migration {phase} failed on shard {shard_id}"),
    )
}

fn shard_path(shards_dir: &Path, shard_id: u16) -> PathBuf {
    shards_dir.join(format!("{shard_id:04}.sqlite"))
}

fn all_shards(shard_count: u16) -> Vec<u16> {
    (0..shard_count).collect()
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        panic::{AssertUnwindSafe, catch_unwind},
        process::Command,
    };

    use rusqlite::OptionalExtension;

    use super::*;
    use crate::storage::SchemaGateState;

    const MIGRATION_SQL: &str =
        "CREATE TABLE migration_marker (id INTEGER PRIMARY KEY, value TEXT NOT NULL)";

    #[derive(Debug, Clone, Copy)]
    struct ExpectedBoundary {
        point: SchemaMigrationCoordinatorPoint,
        manifest_generation: i64,
        state: i64,
        next_shard: i64,
        shard_generations: [i64; 2],
    }

    const BOUNDARIES: &[ExpectedBoundary] = &[
        ExpectedBoundary {
            point: SchemaMigrationCoordinatorPoint::JournalCommitted,
            manifest_generation: 0,
            state: 1,
            next_shard: 0,
            shard_generations: [0, 0],
        },
        ExpectedBoundary {
            point: SchemaMigrationCoordinatorPoint::ShardCommitted(0),
            manifest_generation: 0,
            state: 1,
            next_shard: 0,
            shard_generations: [1, 0],
        },
        ExpectedBoundary {
            point: SchemaMigrationCoordinatorPoint::ProgressPrepared(0),
            manifest_generation: 0,
            state: 1,
            next_shard: 0,
            shard_generations: [1, 0],
        },
        ExpectedBoundary {
            point: SchemaMigrationCoordinatorPoint::ProgressCommitted(0),
            manifest_generation: 0,
            state: 1,
            next_shard: 1,
            shard_generations: [1, 0],
        },
        ExpectedBoundary {
            point: SchemaMigrationCoordinatorPoint::ShardCommitted(1),
            manifest_generation: 0,
            state: 1,
            next_shard: 1,
            shard_generations: [1, 1],
        },
        ExpectedBoundary {
            point: SchemaMigrationCoordinatorPoint::ProgressPrepared(1),
            manifest_generation: 0,
            state: 1,
            next_shard: 1,
            shard_generations: [1, 1],
        },
        ExpectedBoundary {
            point: SchemaMigrationCoordinatorPoint::ProgressCommitted(1),
            manifest_generation: 0,
            state: 1,
            next_shard: 2,
            shard_generations: [1, 1],
        },
        ExpectedBoundary {
            point: SchemaMigrationCoordinatorPoint::FinalizationPrepared,
            manifest_generation: 0,
            state: 1,
            next_shard: 2,
            shard_generations: [1, 1],
        },
        ExpectedBoundary {
            point: SchemaMigrationCoordinatorPoint::FinalizationCommitted,
            manifest_generation: 1,
            state: 2,
            next_shard: 2,
            shard_generations: [1, 1],
        },
    ];

    fn run_with_hook<F>(storage: &Storage, hook: F) -> EngineResult<Vec<u16>>
    where
        F: FnMut(SchemaMigrationCoordinatorPoint) -> EngineResult<()>,
    {
        run_sql_with_hook(storage, MIGRATION_SQL, hook)
    }

    fn run_sql_with_hook<F>(storage: &Storage, sql: &str, hook: F) -> EngineResult<Vec<u16>>
    where
        F: FnMut(SchemaMigrationCoordinatorPoint) -> EngineResult<()>,
    {
        let mut guard = storage.begin_schema_migration()?;
        guard.wait_for_quiescence_blocking();
        let completed = apply_schema_migration_with_hook(storage, sql, &mut guard, None, hook)?;
        guard.publish_ready()?;
        Ok(completed)
    }

    fn fail_at(
        expected: SchemaMigrationCoordinatorPoint,
    ) -> impl FnMut(SchemaMigrationCoordinatorPoint) -> EngineResult<()> {
        move |observed| {
            if observed == expected {
                Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "injected coordinator failure",
                ))
            } else {
                Ok(())
            }
        }
    }

    fn journal_snapshot(root: &Path) -> (i64, i64, i64) {
        Connection::open(root.join("manifest.sqlite"))
            .unwrap()
            .query_row(
                "SELECT c.schema_generation, m.migration_state, m.next_shard
                 FROM briskdb_schema_catalog AS c
                 JOIN briskdb_schema_migrations AS m
                 WHERE c.singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
    }

    fn active_journal_snapshot(root: &Path) -> Option<(i64, i64, i64)> {
        Connection::open(root.join("manifest.sqlite"))
            .unwrap()
            .query_row(
                "SELECT c.schema_generation, m.migration_state, m.next_shard
                 FROM briskdb_schema_catalog AS c
                 JOIN briskdb_schema_migrations AS m
                 WHERE c.singleton = 1 AND m.migration_state = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .unwrap()
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct MigratingManifestSnapshot {
        catalog_generation: i64,
        target_generation: i64,
        source_generation: i64,
        migration_id: Vec<u8>,
        digest_version: i64,
        sql_text: String,
        shard_count: i64,
        migration_state: i64,
        next_shard: i64,
        committed_schema_digest: Vec<u8>,
        target_schema_digest: Vec<u8>,
    }

    fn migrating_manifest_snapshot(root: &Path) -> MigratingManifestSnapshot {
        Connection::open(root.join("manifest.sqlite"))
            .unwrap()
            .query_row(
                "SELECT c.schema_generation,
                        m.target_generation,
                        m.source_generation,
                        m.migration_id,
                        m.digest_version,
                        m.sql_text,
                        m.shard_count,
                        m.migration_state,
                        m.next_shard,
                        i.committed_schema_digest,
                        i.target_schema_digest
                 FROM briskdb_schema_catalog AS c
                 JOIN briskdb_schema_migrations AS m ON m.migration_state = 1
                 JOIN briskdb_integrity AS i ON i.singleton = 1
                 WHERE c.singleton = 1",
                [],
                |row| {
                    Ok(MigratingManifestSnapshot {
                        catalog_generation: row.get(0)?,
                        target_generation: row.get(1)?,
                        source_generation: row.get(2)?,
                        migration_id: row.get(3)?,
                        digest_version: row.get(4)?,
                        sql_text: row.get(5)?,
                        shard_count: row.get(6)?,
                        migration_state: row.get(7)?,
                        next_shard: row.get(8)?,
                        committed_schema_digest: row.get(9)?,
                        target_schema_digest: row.get(10)?,
                    })
                },
            )
            .unwrap()
    }

    fn manifest_generation_and_history(root: &Path, file_name: &str) -> (i64, i64) {
        Connection::open(root.join(file_name))
            .unwrap()
            .query_row(
                "SELECT c.schema_generation,
                        (SELECT COUNT(*) FROM briskdb_schema_migrations)
                 FROM briskdb_schema_catalog AS c
                 WHERE c.singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }

    fn shard_generations(root: &Path) -> [i64; 2] {
        std::array::from_fn(|shard| {
            Connection::open(root.join("shards").join(format!("{shard:04}.sqlite")))
                .unwrap()
                .pragma_query_value(None, "user_version", |row| row.get(0))
                .unwrap()
        })
    }

    fn assert_boundary(root: &Path, expected: ExpectedBoundary) {
        assert_eq!(
            journal_snapshot(root),
            (
                expected.manifest_generation,
                expected.state,
                expected.next_shard,
            ),
            "unexpected manifest state at {:?}",
            expected.point
        );
        assert_eq!(
            shard_generations(root),
            expected.shard_generations,
            "unexpected shard prefix at {:?}",
            expected.point
        );
    }

    fn assert_complete(storage: &Storage, root: &Path) {
        assert_eq!(storage.current_schema_generation(), 1);
        assert_eq!(journal_snapshot(root), (1, 2, 2));
        assert_eq!(shard_generations(root), [1, 1]);
        for shard in 0..2 {
            assert!(
                storage
                    .open_shard(shard)
                    .unwrap()
                    .query_row(
                        "SELECT EXISTS(
                        SELECT 1 FROM sqlite_schema
                        WHERE type = 'table' AND name = 'migration_marker'
                     )",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap()
            );
        }
    }

    fn assert_file_backed_migrating_startup_rejects_schema_tamper(
        tampered_shard: u16,
        drift_table: &str,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let failure = run_with_hook(
            &storage,
            fail_at(SchemaMigrationCoordinatorPoint::ShardCommitted(0)),
        )
        .unwrap_err();
        assert_eq!(failure.kind(), EngineErrorKind::Internal);
        assert_eq!(
            storage.schema_gate_snapshot().state,
            SchemaGateState::Pending
        );
        assert_eq!(active_journal_snapshot(temp.path()), Some((0, 1, 0)));
        assert_eq!(shard_generations(temp.path()), [1, 0]);

        let trusted = migrating_manifest_snapshot(temp.path());
        assert_eq!(trusted.catalog_generation, 0);
        assert_eq!(
            (trusted.source_generation, trusted.target_generation),
            (0, 1)
        );
        assert_eq!((trusted.migration_state, trusted.next_shard), (1, 0));
        assert_eq!(trusted.committed_schema_digest.len(), 32);
        assert_eq!(trusted.target_schema_digest.len(), 32);
        assert_eq!(
            Connection::open(temp.path().join("manifest.sqlite"))
                .unwrap()
                .query_row(
                    "SELECT database_state FROM briskdb_integrity WHERE singleton = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            3
        );
        drop(storage);

        Connection::open(
            temp.path()
                .join("shards")
                .join(format!("{tampered_shard:04}.sqlite")),
        )
        .unwrap()
        .execute_batch(&format!(
            "CREATE TABLE {drift_table}(id INTEGER PRIMARY KEY)"
        ))
        .unwrap();

        let error = Storage::open(temp.path(), 2).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(
            migrating_manifest_snapshot(temp.path()),
            trusted,
            "startup must preserve the complete active journal and both trusted digests"
        );
        assert_eq!(active_journal_snapshot(temp.path()), Some((0, 1, 0)));
        assert_eq!(
            shard_generations(temp.path()),
            [1, 0],
            "startup must not acknowledge or apply another shard"
        );

        let manifest_path = temp.path().join("manifest.sqlite");
        let manifest_connection = Connection::open(&manifest_path).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        let degraded = manifest::current_integrity(&manifest_connection, 2).unwrap();
        assert_eq!(degraded.state(), manifest::DatabaseIntegrityState::Degraded);
        assert_eq!(
            degraded.committed_schema_digest().unwrap().as_slice(),
            trusted.committed_schema_digest.as_slice()
        );
        assert_eq!(
            degraded.target_schema_digest().unwrap().as_slice(),
            trusted.target_schema_digest.as_slice()
        );
        drop(manifest_connection);

        for shard_id in 0..2 {
            let connection = Connection::open(
                temp.path()
                    .join("shards")
                    .join(format!("{shard_id:04}.sqlite")),
            )
            .unwrap();
            assert_eq!(
                connection
                    .query_row(
                        "SELECT EXISTS(
                            SELECT 1 FROM sqlite_schema
                            WHERE type = 'table' AND name = 'migration_marker'
                         )",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap(),
                shard_id == 0,
                "startup advanced the commit-before-ack migration prefix"
            );
            assert_eq!(
                connection
                    .query_row(
                        "SELECT EXISTS(
                            SELECT 1 FROM sqlite_schema
                            WHERE type = 'table' AND name = ?1
                         )",
                        [drift_table],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap(),
                shard_id == tampered_shard,
                "startup changed the injected schema drift"
            );
        }

        let terminal = Storage::open(temp.path(), 2).unwrap_err();
        assert_eq!(terminal.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(migrating_manifest_snapshot(temp.path()), trusted);
        assert_eq!(shard_generations(temp.path()), [1, 0]);
    }

    #[test]
    fn every_coordinator_error_boundary_is_exact_pending_and_idempotently_resumable() {
        for expected in BOUNDARIES {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            let error = run_with_hook(&storage, fail_at(expected.point)).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Internal);
            assert_eq!(
                storage.schema_gate_snapshot().state,
                SchemaGateState::Pending,
                "gate was not pending at {:?}",
                expected.point
            );
            assert_boundary(temp.path(), *expected);
            assert_eq!(
                storage.enter_schema_operation().unwrap_err().kind(),
                EngineErrorKind::FailedPrecondition
            );

            assert_eq!(run_with_hook(&storage, |_| Ok(())).unwrap(), [0, 1]);
            assert_eq!(storage.schema_gate_snapshot().state, SchemaGateState::Ready);
            assert_complete(&storage, temp.path());
            // Exact SQL is the durable idempotency identity.
            assert_eq!(run_with_hook(&storage, |_| Ok(())).unwrap(), [0, 1]);
        }
    }

    #[test]
    fn every_coordinator_panic_boundary_rolls_back_or_retains_only_durable_progress() {
        for expected in BOUNDARIES {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            let panic = catch_unwind(AssertUnwindSafe(|| {
                run_with_hook(&storage, |observed| {
                    if observed == expected.point {
                        panic!("injected coordinator panic at {observed:?}");
                    }
                    Ok(())
                })
            }));
            assert!(panic.is_err(), "hook did not panic at {:?}", expected.point);
            assert_eq!(
                storage.schema_gate_snapshot().state,
                SchemaGateState::Pending
            );
            assert_boundary(temp.path(), *expected);

            assert_eq!(run_with_hook(&storage, |_| Ok(())).unwrap(), [0, 1]);
            assert_complete(&storage, temp.path());
        }
    }

    #[test]
    fn startup_resumes_every_durable_partial_shape_before_returning_ready() {
        for expected in BOUNDARIES {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            run_with_hook(&storage, fail_at(expected.point)).unwrap_err();
            assert_boundary(temp.path(), *expected);
            drop(storage);

            let reopened = Storage::open(temp.path(), 2).unwrap();
            assert_eq!(
                reopened.schema_gate_snapshot().state,
                SchemaGateState::Ready
            );
            assert_complete(&reopened, temp.path());
        }
    }

    #[test]
    fn file_backed_migrating_startup_degrades_without_advancing_on_source_schema_tamper() {
        assert_file_backed_migrating_startup_rejects_schema_tamper(
            1,
            "injected_source_schema_drift",
        );
    }

    #[test]
    fn file_backed_migrating_startup_degrades_without_advancing_on_target_schema_tamper() {
        assert_file_backed_migrating_startup_rejects_schema_tamper(
            0,
            "injected_target_schema_drift",
        );
    }

    #[test]
    fn cancellation_after_journal_commit_leaves_pending_and_retry_completes() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let control = OperationControl::new(None);
        let mut guard = storage.begin_schema_migration().unwrap();
        guard.wait_for_quiescence_blocking();
        let cancellation = Arc::clone(&control);
        let error = apply_schema_migration_with_hook(
            &storage,
            MIGRATION_SQL,
            &mut guard,
            Some(control),
            move |point| {
                if point == SchemaMigrationCoordinatorPoint::JournalCommitted {
                    cancellation.request_cancel(CancellationReason::Cancelled);
                }
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        drop(guard);
        assert_eq!(
            storage.schema_gate_snapshot().state,
            SchemaGateState::Pending
        );
        assert_eq!(journal_snapshot(temp.path()), (0, 1, 0));
        assert_eq!(shard_generations(temp.path()), [0, 0]);

        assert_eq!(run_with_hook(&storage, |_| Ok(())).unwrap(), [0, 1]);
        assert_complete(&storage, temp.path());
    }

    #[test]
    fn cancellation_cannot_mask_corruption_before_terminal_degradation_is_recorded() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        Connection::open(temp.path().join("shards/0000.sqlite"))
            .unwrap()
            .execute_batch("CREATE TABLE injected_checksum_drift(value TEXT)")
            .unwrap();

        CANCEL_AFTER_CONTROLLED_CORRUPTION.with(|cancel| cancel.set(true));
        let control = OperationControl::new(None);
        let mut guard = storage.begin_schema_migration().unwrap();
        guard.wait_for_quiescence_blocking();
        let result = apply_schema_migration(
            &storage,
            MIGRATION_SQL,
            &mut guard,
            Some(Arc::clone(&control)),
        );
        assert_eq!(
            result.as_ref().unwrap_err().kind(),
            EngineErrorKind::DataCorruption,
            "the storage wrapper must observe corruption before client cancellation mapping"
        );
        drop(guard);
        assert_eq!(
            storage.schema_gate_snapshot().state,
            SchemaGateState::Degraded
        );
        assert_eq!(
            control.complete(result).unwrap_err().kind(),
            EngineErrorKind::Cancelled,
            "the outer request boundary may still report the accepted cancellation"
        );

        let manifest_connection = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
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
                    "SELECT COUNT(*) FROM briskdb_schema_migrations",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0,
            "preflight corruption must not publish a migration journal"
        );
    }

    #[test]
    fn an_older_exact_sql_identity_remains_idempotent_after_later_migrations() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        assert_eq!(run_with_hook(&storage, |_| Ok(())).unwrap(), [0, 1]);

        let mut second = storage.begin_schema_migration().unwrap();
        second.wait_for_quiescence_blocking();
        assert_eq!(
            apply_schema_migration_with_hook(
                &storage,
                "ALTER TABLE migration_marker ADD COLUMN revision INTEGER NOT NULL DEFAULT 0",
                &mut second,
                None,
                |_| Ok(()),
            )
            .unwrap(),
            [0, 1]
        );
        second.publish_ready().unwrap();
        assert_eq!(storage.current_schema_generation(), 2);

        assert_eq!(run_with_hook(&storage, |_| Ok(())).unwrap(), [0, 1]);
        assert_eq!(storage.current_schema_generation(), 2);
        for shard in 0..2 {
            assert_eq!(
                storage
                    .open_shard(shard)
                    .unwrap()
                    .query_row(
                        "SELECT COUNT(*) FROM pragma_table_info('migration_marker')",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                3
            );
        }
    }

    #[test]
    fn manifest_lock_wait_observes_deadline_without_the_fixed_busy_timeout_delay() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let owner = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
        owner
            .execute_batch(
                "PRAGMA wal_checkpoint(TRUNCATE);
                 PRAGMA journal_mode = DELETE;
                 BEGIN EXCLUSIVE;",
            )
            .unwrap();

        let control = OperationControl::new(Some(Instant::now() + Duration::from_millis(50)));
        let mut guard = storage.begin_schema_migration().unwrap();
        guard.wait_for_quiescence_blocking();
        let started = Instant::now();
        let error = apply_schema_migration_with_hook(
            &storage,
            MIGRATION_SQL,
            &mut guard,
            Some(control),
            |_| Ok(()),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "manifest lock ignored the request deadline"
        );
        drop(guard);
        assert_eq!(storage.schema_gate_snapshot().state, SchemaGateState::Ready);
        assert_eq!(
            owner
                .query_row(
                    "SELECT COUNT(*) FROM briskdb_schema_migrations",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );

        owner.execute_batch("ROLLBACK").unwrap();
        assert_eq!(run_with_hook(&storage, |_| Ok(())).unwrap(), [0, 1]);
        assert_complete(&storage, temp.path());
    }

    #[test]
    fn post_commit_journal_cleanup_failure_keeps_gate_pending_and_retry_heals() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let mut guard = storage.begin_schema_migration().unwrap();
        guard.wait_for_quiescence_blocking();
        let mut manifest_connection =
            open_existing_manifest(&temp.path().join("manifest.sqlite")).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        configure_journal_mode(&manifest_connection).unwrap();
        let control = OperationControl::new(None);
        FAIL_NEXT_MANIFEST_COMMIT_CLEANUP.with(|fail| fail.set(true));
        let source_digest = storage
            .schema_coordination
            .committed_schema_digest()
            .unwrap();
        let (_, target_digest) = shard::preflight_schema_migration_with_digest(
            &temp.path().join("shards/0000.sqlite"),
            0,
            0,
            1,
            &storage.shard_layout,
            MIGRATION_SQL,
        )
        .unwrap();

        let error = begin_schema_migration(
            &mut manifest_connection,
            BeginSchemaMigrationRequest {
                requested_shards: 2,
                layout: &storage.shard_layout,
                source_generation: 0,
                sql: MIGRATION_SQL,
                source_digest,
                target_digest,
                control: Some(&control),
            },
            &mut guard,
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert!(manifest_connection.is_autocommit());
        drop(manifest_connection);
        drop(guard);

        assert_eq!(
            storage.schema_gate_snapshot().state,
            SchemaGateState::Pending
        );
        assert_eq!(active_journal_snapshot(temp.path()), Some((0, 1, 0)));
        assert_eq!(shard_generations(temp.path()), [0, 0]);
        assert_eq!(run_with_hook(&storage, |_| Ok(())).unwrap(), [0, 1]);
        assert_eq!(storage.schema_gate_snapshot().state, SchemaGateState::Ready);
        assert_complete(&storage, temp.path());
    }

    #[test]
    fn manifest_history_scan_observes_deadline_and_rolls_back_its_transaction() {
        const HISTORY_ROWS: i64 = 20_000;

        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let layout = storage.shard_layout;
        let mut manifest_connection =
            Connection::open(temp.path().join("manifest.sqlite")).unwrap();
        configure_manifest_connection(&manifest_connection).unwrap();
        manifest_connection
            .execute_batch("BEGIN IMMEDIATE")
            .unwrap();
        {
            let mut insert = manifest_connection
                .prepare(
                    "INSERT INTO briskdb_schema_migrations (
                         target_generation,
                         source_generation,
                         migration_id,
                         digest_version,
                         sql_text,
                         shard_count,
                         migration_state,
                         next_shard
                     ) VALUES (?1, ?2, ?3, 1, ?4, 2, 2, 2)",
                )
                .unwrap();
            for target_generation in 1..=HISTORY_ROWS {
                let sql = format!("SELECT {target_generation}");
                let migration_id = manifest::schema_migration_id(&sql).unwrap();
                insert
                    .execute(rusqlite::params![
                        target_generation,
                        target_generation - 1,
                        migration_id.as_slice(),
                        sql,
                    ])
                    .unwrap();
            }
        }
        manifest_connection
            .execute(
                "UPDATE briskdb_schema_catalog SET schema_generation = ?1 WHERE singleton = 1",
                [HISTORY_ROWS],
            )
            .unwrap();
        manifest_connection.execute_batch("COMMIT").unwrap();
        manifest::reseal_manifest_for_test(&manifest_connection).unwrap();

        let control = OperationControl::new(Some(Instant::now() + Duration::from_millis(5)));
        let started = Instant::now();
        let error = classify_schema_migration(
            &mut manifest_connection,
            2,
            &layout,
            "SELECT absent_migration",
            Some(&control),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(manifest_connection.is_autocommit());

        let integrity_control =
            OperationControl::new(Some(Instant::now() + Duration::from_millis(5)));
        let integrity_started = Instant::now();
        let integrity_error = current_integrity_optional_controlled(
            &mut manifest_connection,
            2,
            Some(&integrity_control),
        )
        .unwrap_err();
        assert_eq!(integrity_error.kind(), EngineErrorKind::DeadlineExceeded);
        assert!(integrity_started.elapsed() < Duration::from_secs(1));
        assert!(manifest_connection.is_autocommit());

        let public_control = OperationControl::new(Some(Instant::now() + Duration::from_millis(5)));
        let mut guard = storage.begin_schema_migration().unwrap();
        guard.wait_for_quiescence_blocking();
        let public_started = Instant::now();
        let public_error = apply_schema_migration_with_hook(
            &storage,
            "SELECT absent_migration",
            &mut guard,
            Some(public_control),
            |_| Ok(()),
        )
        .unwrap_err();
        assert_eq!(public_error.kind(), EngineErrorKind::DeadlineExceeded);
        assert!(public_started.elapsed() < Duration::from_secs(1));
        drop(guard);
        assert_eq!(storage.schema_gate_snapshot().state, SchemaGateState::Ready);

        assert_eq!(
            manifest::classify_schema_migration(
                &mut manifest_connection,
                2,
                "SELECT absent_migration",
            )
            .unwrap(),
            SchemaMigrationClassification::Absent
        );
    }

    #[test]
    fn active_prefix_validation_observes_deadline_on_a_real_shard_lock() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        run_with_hook(
            &storage,
            fail_at(SchemaMigrationCoordinatorPoint::JournalCommitted),
        )
        .unwrap_err();
        assert_eq!(
            storage.schema_gate_snapshot().state,
            SchemaGateState::Pending
        );

        let blocker = Connection::open(temp.path().join("shards/0000.sqlite")).unwrap();
        blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
        let control = OperationControl::new(Some(Instant::now() + Duration::from_millis(50)));
        let mut guard = storage.begin_schema_migration().unwrap();
        guard.wait_for_quiescence_blocking();
        let started = Instant::now();
        let error = apply_schema_migration_with_hook(
            &storage,
            MIGRATION_SQL,
            &mut guard,
            Some(control),
            |_| Ok(()),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(guard);
        assert_eq!(
            storage.schema_gate_snapshot().state,
            SchemaGateState::Pending
        );

        blocker.execute_batch("ROLLBACK").unwrap();
        assert_eq!(run_with_hook(&storage, |_| Ok(())).unwrap(), [0, 1]);
        assert_complete(&storage, temp.path());
    }

    #[test]
    fn same_process_reopen_recovers_finalization_commit_before_publication() {
        let temp = tempfile::tempdir().unwrap();
        let original = Storage::open(temp.path(), 2).unwrap();
        let error = run_with_hook(
            &original,
            fail_at(SchemaMigrationCoordinatorPoint::FinalizationCommitted),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert_eq!(original.current_schema_generation(), 0);
        assert_eq!(
            original.schema_gate_snapshot().state,
            SchemaGateState::Pending
        );
        assert_boundary(
            temp.path(),
            ExpectedBoundary {
                point: SchemaMigrationCoordinatorPoint::FinalizationCommitted,
                manifest_generation: 1,
                state: 2,
                next_shard: 2,
                shard_generations: [1, 1],
            },
        );

        let reopened = Storage::open(temp.path(), 2).unwrap();
        assert_eq!(original.current_schema_generation(), 1);
        assert_eq!(reopened.current_schema_generation(), 1);
        assert_eq!(
            original.schema_gate_snapshot().state,
            SchemaGateState::Ready
        );
        for shard_id in 0..2 {
            drop(original.open_shard(shard_id).unwrap());
            drop(reopened.open_shard(shard_id).unwrap());
        }
    }

    #[test]
    fn historical_retry_cannot_publish_ready_over_a_different_active_journal() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        assert_eq!(run_with_hook(&storage, |_| Ok(())).unwrap(), [0, 1]);
        let active_sql = "CREATE TABLE second_migration_marker (id INTEGER)";
        let error = run_sql_with_hook(
            &storage,
            active_sql,
            fail_at(SchemaMigrationCoordinatorPoint::JournalCommitted),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert_eq!(storage.current_schema_generation(), 1);
        assert_eq!(
            storage.schema_gate_snapshot().state,
            SchemaGateState::Pending
        );
        assert_eq!(active_journal_snapshot(temp.path()), Some((1, 1, 0)));
        assert_eq!(shard_generations(temp.path()), [1, 1]);

        let started = Instant::now();
        let conflict = run_with_hook(&storage, |_| Ok(())).unwrap_err();
        assert_eq!(conflict.kind(), EngineErrorKind::FailedPrecondition);
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(
            storage.schema_gate_snapshot().state,
            SchemaGateState::Pending
        );
        assert_eq!(active_journal_snapshot(temp.path()), Some((1, 1, 0)));
        assert_eq!(shard_generations(temp.path()), [1, 1]);

        assert_eq!(
            run_sql_with_hook(&storage, active_sql, |_| Ok(())).unwrap(),
            [0, 1]
        );
        assert_eq!(storage.current_schema_generation(), 2);
        assert_eq!(storage.schema_gate_snapshot().state, SchemaGateState::Ready);
    }

    #[test]
    fn absent_migration_rejects_unjournaled_target_generation_shards() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let advanced = Connection::open(temp.path().join("shards/0000.sqlite")).unwrap();
        advanced
            .execute_batch(
                "CREATE TABLE unrelated_schema (id INTEGER);
                 PRAGMA user_version = 1;",
            )
            .unwrap();
        drop(advanced);

        let error = run_with_hook(&storage, |_| Ok(())).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(storage.current_schema_generation(), 0);
        assert_eq!(storage.schema_gate_snapshot().state, SchemaGateState::Ready);
        let manifest = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
        assert_eq!(
            manifest
                .query_row(
                    "SELECT schema_generation FROM briskdb_schema_catalog WHERE singleton = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            manifest
                .query_row(
                    "SELECT COUNT(*) FROM briskdb_schema_migrations",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        for (shard_id, expected_generation) in [(0, 1), (1, 0)] {
            let shard = Connection::open(
                temp.path()
                    .join("shards")
                    .join(format!("{shard_id:04}.sqlite")),
            )
            .unwrap();
            assert_eq!(
                shard
                    .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                    .unwrap(),
                expected_generation
            );
            assert!(
                !shard
                    .query_row(
                        "SELECT EXISTS(
                            SELECT 1 FROM sqlite_schema WHERE name = 'migration_marker'
                         )",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap()
            );
            assert_eq!(
                shard
                    .query_row(
                        "SELECT EXISTS(
                            SELECT 1 FROM sqlite_schema WHERE name = 'unrelated_schema'
                         )",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap(),
                shard_id == 0
            );
        }
    }

    #[test]
    fn deferred_foreign_key_failure_is_rejected_before_journaling_or_shard_commit() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let baseline_sql = "CREATE TABLE parent (id INTEGER PRIMARY KEY);
                            CREATE TABLE selector (value INTEGER NOT NULL);";
        assert_eq!(
            run_sql_with_hook(&storage, baseline_sql, |_| Ok(())).unwrap(),
            [0, 1]
        );
        storage
            .open_shard(1)
            .unwrap()
            .execute("INSERT INTO selector VALUES (999)", [])
            .unwrap();

        let deferred_sql = "CREATE TABLE child (
                                parent_id INTEGER REFERENCES parent(id)
                                    DEFERRABLE INITIALLY DEFERRED
                            );
                            INSERT INTO child SELECT value FROM selector;";
        let error = run_sql_with_hook(&storage, deferred_sql, |_| Ok(())).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::ForeignKeyViolation);
        assert_eq!(storage.current_schema_generation(), 1);
        assert_eq!(storage.schema_gate_snapshot().state, SchemaGateState::Ready);
        let manifest = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
        assert_eq!(
            manifest
                .query_row(
                    "SELECT COUNT(*) FROM briskdb_schema_migrations",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert!(active_journal_snapshot(temp.path()).is_none());
        assert_eq!(shard_generations(temp.path()), [1, 1]);
        for shard_id in 0..2 {
            let shard = storage.open_shard(shard_id).unwrap();
            assert!(
                !shard
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'child')",
                        [],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap()
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn runtime_manifest_symlink_and_cross_layout_replacement_fail_before_mutation() {
        let first_root = tempfile::tempdir().unwrap();
        let second_root = tempfile::tempdir().unwrap();
        let first = Storage::open(first_root.path(), 2).unwrap();
        let second = Storage::open(second_root.path(), 2).unwrap();
        let manifest_path = first_root.path().join("manifest.sqlite");
        let original_manifest_path = first_root.path().join("original-manifest.sqlite");
        fs::rename(&manifest_path, &original_manifest_path).unwrap();
        std::os::unix::fs::symlink(second_root.path().join("manifest.sqlite"), &manifest_path)
            .unwrap();

        let symlink_error = run_with_hook(&first, |_| Ok(())).unwrap_err();
        assert_eq!(symlink_error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(first.schema_gate_snapshot().state, SchemaGateState::Ready);
        assert_eq!(first.current_schema_generation(), 0);
        assert_eq!(second.current_schema_generation(), 0);
        assert_eq!(
            manifest_generation_and_history(first_root.path(), "original-manifest.sqlite"),
            (0, 0)
        );
        assert_eq!(
            manifest_generation_and_history(second_root.path(), "manifest.sqlite"),
            (0, 0)
        );

        fs::remove_file(&manifest_path).unwrap();
        fs::copy(second_root.path().join("manifest.sqlite"), &manifest_path).unwrap();
        let replacement_integrity_before: (i64, Vec<u8>) = Connection::open(&manifest_path)
            .unwrap()
            .query_row(
                "SELECT database_state, manifest_digest
                 FROM briskdb_integrity WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let mut guard = first.begin_schema_migration().unwrap();
        guard.wait_for_quiescence_blocking();
        let replacement_error = first
            .apply_schema_migration(MIGRATION_SQL, &mut guard, None)
            .unwrap_err();
        assert_eq!(replacement_error.kind(), EngineErrorKind::DataCorruption);
        drop(guard);
        assert_eq!(
            first.schema_gate_snapshot().state,
            SchemaGateState::Degraded
        );
        assert_eq!(
            Connection::open(&manifest_path)
                .unwrap()
                .query_row(
                    "SELECT database_state, manifest_digest
                     FROM briskdb_integrity WHERE singleton = 1",
                    [],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
                )
                .unwrap(),
            replacement_integrity_before,
            "a rejected foreign manifest must not be marked or resealed"
        );
        assert_eq!(
            manifest_generation_and_history(first_root.path(), "manifest.sqlite"),
            (0, 0)
        );
        assert_eq!(
            manifest_generation_and_history(second_root.path(), "manifest.sqlite"),
            (0, 0)
        );
        assert_eq!(shard_generations(first_root.path()), [0, 0]);
        assert_eq!(shard_generations(second_root.path()), [0, 0]);
        for root in [first_root.path(), second_root.path()] {
            for shard_id in 0..2 {
                assert!(
                    !Connection::open(root.join("shards").join(format!("{shard_id:04}.sqlite")))
                        .unwrap()
                        .query_row(
                            "SELECT EXISTS(
                                SELECT 1 FROM sqlite_schema WHERE name = 'migration_marker'
                             )",
                            [],
                            |row| row.get::<_, bool>(0),
                        )
                        .unwrap()
                );
            }
        }
    }

    #[test]
    fn schema_migration_crash_child() {
        let Ok(root) = std::env::var("BRISKDB_SCHEMA_CRASH_ROOT") else {
            return;
        };
        let crash_point = std::env::var("BRISKDB_SCHEMA_CRASH_POINT").unwrap();
        let storage = Storage::open(root, 2).unwrap();
        let mut guard = storage.begin_schema_migration().unwrap();
        guard.wait_for_quiescence_blocking();
        let _ =
            apply_schema_migration_with_hook(&storage, MIGRATION_SQL, &mut guard, None, |point| {
                if crash_point == point_name(point) {
                    std::process::abort();
                }
                Ok(())
            });
        panic!("child did not reach requested crash point {crash_point}");
    }

    #[test]
    fn real_process_abort_is_resumed_at_key_durable_boundaries() {
        for crash_point in [
            "journal",
            "shard-0",
            "progress-0",
            "finalization-prepared",
            "finalization-committed",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let status = Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("storage::migration::tests::schema_migration_crash_child")
                .arg("--nocapture")
                .env("BRISKDB_SCHEMA_CRASH_ROOT", temp.path())
                .env("BRISKDB_SCHEMA_CRASH_POINT", crash_point)
                .status()
                .unwrap();
            assert!(!status.success(), "child did not abort at {crash_point}");

            let reopened = Storage::open(temp.path(), 2).unwrap();
            assert_complete(&reopened, temp.path());
        }
    }

    fn point_name(point: SchemaMigrationCoordinatorPoint) -> &'static str {
        match point {
            SchemaMigrationCoordinatorPoint::JournalCommitted => "journal",
            SchemaMigrationCoordinatorPoint::ShardCommitted(0) => "shard-0",
            SchemaMigrationCoordinatorPoint::ProgressCommitted(0) => "progress-0",
            SchemaMigrationCoordinatorPoint::FinalizationPrepared => "finalization-prepared",
            SchemaMigrationCoordinatorPoint::FinalizationCommitted => "finalization-committed",
            SchemaMigrationCoordinatorPoint::ShardCommitted(_)
            | SchemaMigrationCoordinatorPoint::ProgressPrepared(_)
            | SchemaMigrationCoordinatorPoint::ProgressCommitted(_) => "other",
        }
    }
}
