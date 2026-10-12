//! Hybrid engine integration. Shared root/schema admission remains the same
//! as SQLite mode; only metadata persistence and its transaction journal differ.
use super::native_manifest::{FILE_NAME, NativeManifest, Root, unsupported};
use super::shard::{SHARD_APPLICATION_ID, SHARD_METADATA_VERSION};
use super::*;

impl Storage {
    pub(super) fn open_native_metadata(
        root: &Path,
        shards: u16,
        security_store_id: Option<[u8; 16]>,
        shared_control: Option<&Arc<OperationControl>>,
        profile: crate::StorageProfile,
    ) -> EngineResult<Self> {
        profile.require_available()?;
        Self::open_native_metadata_inner(root, shards, security_store_id, shared_control, profile)
    }

    /// Test-only rollback profile entry; never used by a public builder/open.
    #[cfg(test)]
    pub(super) fn open_native_metadata_qualification(
        root: &Path,
        shards: u16,
        control: Option<&Arc<OperationControl>>,
    ) -> EngineResult<Self> {
        contention::with_control(control.cloned(), || {
            Self::open_native_metadata_inner(
                root,
                shards,
                None,
                control,
                crate::StorageProfile::Nfs,
            )
        })
    }

    fn open_native_metadata_inner(
        root: &Path,
        shards: u16,
        security_store_id: Option<[u8; 16]>,
        shared_control: Option<&Arc<OperationControl>>,
        profile: crate::StorageProfile,
    ) -> EngineResult<Self> {
        let control = shared_control.map(Arc::as_ref);
        validate_shard_count(shards)?;
        if security_store_id.is_some() {
            return Err(unsupported());
        }
        check_startup_control(control)?;
        metadata::validate_selection(root, crate::MetadataBackend::Isam)?;
        // A missing manifest beside surviving shards is damage, not a request
        // to invent a new database identity or adopt arbitrary SQLite files.
        let exists = metadata::present(&root.join(FILE_NAME))?;
        if !exists && !physical_layout_is_empty(&root.join("shards"))? {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "ISAM metadata manifest is missing beside existing shards",
            ));
        }
        for file in ["security.sqlite", "global-indexes/global.sqlite"] {
            if metadata::present(&root.join(file))? {
                return Err(unsupported());
            }
        }
        fs::create_dir_all(root)
            .map_err(|e| sqlite_error::storage_io(e, "cannot create native metadata root"))?;
        let root = fs::canonicalize(root)
            .map_err(|e| sqlite_error::storage_io(e, "cannot resolve native metadata root"))?;
        let journal = match profile {
            crate::StorageProfile::Local => journal::JournalPolicy::LOCAL,
            crate::StorageProfile::Nfs => journal::JournalPolicy::NFS_PERSIST,
        };
        let root = profile::StorageRoot::new(root, journal).with_native_metadata();
        let _startup_process = process_lock::RootStartupGuard::acquire_controlled(
            &root,
            CONNECTION_BUSY_TIMEOUT,
            control,
        )?;
        // Recheck after obtaining the same startup lock used by SQLite mode.
        metadata::validate_selection(&root, crate::MetadataBackend::Isam)?;
        let coordination = root_schema_coordination(&root)?;
        let mut startup = begin_startup_coordination(&coordination, control)?;
        match control {
            Some(control) => startup.wait_for_quiescence_controlled(control)?,
            None => startup.wait_for_quiescence_blocking(),
        }
        let path = root.join(FILE_NAME);
        let existing = metadata::present(&path)?;
        let inspection = if existing {
            Some(NativeManifest::open(&path, true)?.root()?.1)
        } else {
            None
        };
        if inspection.as_ref().is_some_and(|r| r.journal() != journal) {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "native metadata journal profile differs; implicit conversion is not supported",
            ));
        }
        if inspection.as_ref().is_some_and(|r| r.shards != shards) {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "requested shard count differs from ISAM metadata",
            ));
        }
        if inspection.as_ref().is_some_and(|r| r.degraded) {
            coordination.mark_degraded();
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "ISAM metadata is persistently degraded; restore a known-good database",
            ));
        }
        let needs_ownership = inspection
            .as_ref()
            .is_none_or(|r| !r.ready || r.active.is_some());
        let mutation = if needs_ownership {
            Some(coordination.process_lease.try_acquire_exclusive()?)
        } else {
            None
        };
        let mut native = if existing {
            NativeManifest::open(&path, false)?
        } else {
            if !physical_layout_is_empty(&root.join("shards"))? {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    "cannot recreate missing ISAM metadata",
                ));
            }
            if profile == crate::StorageProfile::Local {
                NativeManifest::create(&path, shards)?
            } else {
                NativeManifest::create_with_profile(&path, shards, true)?
            }
        };
        let result = (|| {
            let mut state = native.root()?.1;
            if state.active.is_some() {
                startup.mark_pending_on_drop();
                resume(&root, &mut native, &state, shared_control)?;
                state = native.root()?.1;
            }
            shard::prepare_layout(
                &root.join("shards"),
                shards,
                state.generation,
                &state.layout(),
            )?;
            let catalog = coordination.register_catalog(native.catalog()?)?;
            let storage = Self {
                root: root.clone(),
                catalog,
                shard_layout: state.layout(),
                native_manifest: Some(Arc::new(Mutex::new(native))),
                #[cfg(feature = "documents")]
                document_peer_readers: Arc::new(document::PeerReaders::default()),
                #[cfg(feature = "documents")]
                manifest_readers: Arc::new(manifest_readers::ManifestReaders::new(journal)),
                schema_coordination: Arc::clone(&coordination),
            };
            let digest = storage.verify_current_schema_consensus(
                manifest::ManifestIntegrity::native_ready(state.digest),
            )?;
            if !state.ready {
                let mut native =
                    contention::lock(storage.native_manifest.as_ref().unwrap(), "native metadata")?;
                let (version, mut state) = native.root()?;
                state.ready = true;
                state.digest = Some(digest);
                native.publish(version, &state, Vec::new())?;
            }
            coordination.publish_schema_digests(Some(digest), None)?;
            coordination.reconcile_validated_catalog_generation(&storage.catalog)?;
            Ok(storage)
        })();
        if result
            .as_ref()
            .is_err_and(|e: &EngineError| e.kind() == EngineErrorKind::DataCorruption)
        {
            coordination.mark_degraded();
            if let Ok(mut native) = NativeManifest::open(&path, false) {
                if let Ok((version, mut state)) = native.root() {
                    state.degraded = true;
                    let _ = native.publish(version, &state, Vec::new());
                }
            }
        }
        let mut storage = result?;
        storage.shard_layout = shard::ShardLayout::from_validated_parts(
            storage.shard_layout.layout_id(),
            SHARD_APPLICATION_ID,
            SHARD_METADATA_VERSION,
            shard::ShardLayoutState::Ready,
        )
        .with_journal(journal);
        startup.publish_ready()?;
        if let Some(mutation) = mutation {
            mutation.downgrade()?;
        }
        Ok(storage)
    }

    pub(super) fn register_native_metadata(
        &mut self,
        declarations: Vec<TableDeclaration>,
    ) -> EngineResult<()> {
        if declarations.len() >= isam::MAX_BATCH_RECORDS {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                "ISAM metadata supports at most 4095 tables",
            ));
        }
        if declarations
            .iter()
            .any(|d| d.generated_id_policy() != &GeneratedIdPolicy::None)
        {
            return Err(unsupported());
        }
        if !self.catalog.logical().tables().is_empty() {
            let _operation = self.enter_schema_operation()?;
            return if declarations_match_catalog(self.catalog.logical(), &declarations) {
                Ok(())
            } else {
                Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "table catalog already registered",
                ))
            };
        }
        let mut guard =
            SchemaMigrationGuard::new(self.schema_coordination.gate.begin_new_migration()?);
        guard.wait_for_quiescence_blocking();
        guard.acquire_process_ownership(&self.schema_coordination.process_lease)?;
        self.validate_empty_table_declarations(&declarations)?;
        let replacement_guard = self
            .schema_coordination
            .reserve_catalog_replacement(&self.catalog)?;
        // Pending on uncertain publication; never leave a possibly stale catalog usable.
        guard.mark_pending_on_drop();
        let replacement =
            contention::lock(self.native_manifest.as_ref().unwrap(), "native metadata")?
                .register(&declarations)?;
        self.catalog = replacement_guard.publish(&self.catalog, replacement)?;
        guard.publish_ready()
    }

    pub(super) fn migrate_native_metadata(
        &self,
        sql: &str,
        guard: &mut SchemaMigrationGuard,
        control: Option<Arc<OperationControl>>,
    ) -> EngineResult<Vec<u16>> {
        let _ = manifest::schema_migration_id(sql)?;
        check_startup_control(control.as_deref())?;
        guard.acquire_process_ownership(&self.schema_coordination.process_lease)?;
        let mut native =
            contention::lock(self.native_manifest.as_ref().unwrap(), "native metadata")?;
        let state = native.root()?.1;
        if let Some(active) = &state.active {
            if native.migration_sql(active)? != sql {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "another native metadata migration is pending",
                ));
            }
        } else {
            if native.completed_sql(sql)? {
                if state.generation != self.current_schema_generation() {
                    return Err(EngineError::new(
                        EngineErrorKind::FailedPrecondition,
                        "reopen this stale handle to reconcile committed native metadata",
                    ));
                }
                self.verify_current_schema_consensus(manifest::ManifestIntegrity::native_ready(
                    state.digest,
                ))?;
                return Ok((0..state.shards).collect());
            }
            let (generation, source, target) =
                migration::preflight_new_schema_migration(self, sql, control.as_ref())?;
            if generation != state.generation {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "stale native metadata schema generation",
                ));
            }
            guard.mark_pending_on_drop();
            native.begin_migration(sql, source, target)?;
            failpoint("journal");
        }
        let state = native.root()?.1;
        guard.mark_pending_on_drop();
        let outcome = resume(&self.root, &mut native, &state, control.as_ref());
        // Drop the metadata mutex before degradation or runtime publication.
        let current = if outcome.is_ok() {
            Some(native.root()?.1)
        } else {
            None
        };
        drop(native);
        outcome?;
        let current = current.unwrap();
        self.schema_coordination
            .publish_schema_digests(current.digest, None)?;
        self.schema_coordination
            .publish_schema_generation(state.generation, current.generation)?;
        Ok((0..state.shards).collect())
    }
}

fn resume(
    root: &Path,
    native: &mut NativeManifest,
    state: &Root,
    control: Option<&Arc<OperationControl>>,
) -> EngineResult<()> {
    let active = state
        .active
        .as_ref()
        .ok_or_else(|| EngineError::new(EngineErrorKind::Internal, "missing migration"))?;
    let layout = state.layout();
    let sql = native.migration_sql(active)?;
    // Validate every shard BEFORE mutating any: committed prefix must be at
    // target, at most the next shard may be ahead of its journal acknowledgement.
    for id in 0..state.shards {
        check_startup_control(control.map(Arc::as_ref))?;
        let path = root.join("shards").join(format!("{id:04}.sqlite"));
        let connection = shard::open_required_file(&path)?;
        contention::configure(&connection, CONNECTION_BUSY_TIMEOUT)?;
        let observed = shard::validate_schema_migration_connection(
            &connection,
            &path,
            id,
            active.source,
            active.target,
            &layout,
        )?;
        let target = observed == shard::SchemaMigrationShardState::Target;
        if (id < active.next_shard && !target) || (id > active.next_shard && target) {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "native migration shard prefix is inconsistent",
            ));
        }
        shard::verify_schema_digest(
            &connection,
            if target { active.target } else { active.source },
            if target {
                &active.target_digest
            } else {
                &active.source_digest
            },
        )?;
    }
    for id in active.next_shard..state.shards {
        check_startup_control(control.map(Arc::as_ref))?;
        let path = root.join("shards").join(format!("{id:04}.sqlite"));
        migration::apply_native_metadata_shard(
            &path,
            id,
            active.source,
            active.target,
            &layout,
            &sql,
            &active.target_digest,
            control,
        )?;
        if id == 0 {
            failpoint("shard");
        }
        native.advance(id + 1)?;
        if id == 0 {
            failpoint("progress");
        }
    }
    native.finish()?;
    failpoint("complete");
    Ok(())
}

fn failpoint(_point: &str) {
    #[cfg(test)]
    if std::env::var("BRISKDB_NATIVE_METADATA_TEST_EXIT").as_deref() == Ok(_point) {
        // Test-only immediate exit: no destructors, no accidental graceful commit.
        std::process::exit(97);
    }
}

#[cfg(test)]
mod tests {
    use crate::{MetadataBackend, core::Database};
    const SQL: &str = "CREATE TABLE recovery_marker(id INTEGER PRIMARY KEY)";

    #[test]
    fn crash_child() {
        let Ok(root) = std::env::var("BRISKDB_NATIVE_METADATA_TEST_ROOT") else {
            return;
        };
        let db = Database::open_with_metadata_backend(root, 2, MetadataBackend::Isam).unwrap();
        db.broadcast(SQL).unwrap();
        panic!("requested crash boundary was not reached");
    }

    #[test]
    fn native_metadata_recovers_each_durable_migration_boundary() {
        for point in ["journal", "shard", "progress", "complete"] {
            let dir = tempfile::tempdir().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "storage::native_metadata::tests::crash_child",
                    "--nocapture",
                ])
                .env("BRISKDB_NATIVE_METADATA_TEST_ROOT", dir.path())
                .env("BRISKDB_NATIVE_METADATA_TEST_EXIT", point)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(97),
                "{point}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let db =
                Database::open_with_metadata_backend(dir.path(), 2, MetadataBackend::Isam).unwrap();
            assert_eq!(db.catalog().schema_generation(), 1, "{point}");
            db.broadcast(SQL).unwrap();
            assert_eq!(
                db.query("key", "SELECT count(*) FROM recovery_marker", &[])
                    .unwrap()
                    .rows()
                    .len(),
                1
            );
            assert!(!dir.path().join("manifest.sqlite").exists());
        }
    }
}
