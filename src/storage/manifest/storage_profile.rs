//! Fenced NFS manifest format and explicit, no-conversion initialization.
//!
//! Local roots continue to use v22. Internal storage can exercise this format;
//! public NFS openers remain unavailable pending cross-host safety qualification.

use super::*;
use crate::core::StorageProfile;

pub(super) const NFS_SCHEMA_VERSION: u32 = 23;
const PROFILE_VERSION: i64 = 1;
const NFS_MODE: i64 = 2;
const DELETE_JOURNAL: i64 = 1;
const PERSIST_JOURNAL: i64 = 2;
const EXTRA_SYNCHRONOUS: i64 = 3;

const DOWNGRADE_FENCE_SQL: &str = "CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 23)
) STRICT";

const PROFILE_TABLE_SQL: &str = "CREATE TABLE briskdb_storage_profile (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    profile_version INTEGER NOT NULL CHECK (profile_version > 0),
    storage_mode INTEGER NOT NULL CHECK (storage_mode > 0),
    journal_mode INTEGER NOT NULL CHECK (journal_mode > 0),
    synchronous INTEGER NOT NULL CHECK (synchronous > 0)
) STRICT";

pub(super) const PROFILE_DIGEST_QUERY: ManifestDigestQuery = ManifestDigestQuery {
    table: "briskdb_storage_profile",
    columns: &[
        "singleton",
        "profile_version",
        "storage_mode",
        "journal_mode",
        "synchronous",
    ],
    sql: "SELECT singleton, profile_version, storage_mode, journal_mode, synchronous FROM briskdb_storage_profile ORDER BY singleton",
};

// Used only for inspection in production. Both initializer slots explicitly
// refuse mutation, even if a future caller accidentally uses it as an open plan.
const INSPECTION_PLAN: MigrationPlan<'static> = MigrationPlan {
    current_version: NFS_SCHEMA_VERSION,
    // Keep this separate from the contiguous local upgrade registry, whose
    // last entry must remain v22. This plan only inspects NFS roots, never
    // upgrades a historical local root into the reserved profile.
    migrations: &[Migration {
        from: V22_SCHEMA_VERSION,
        to: NFS_SCHEMA_VERSION,
        name: "explicit_nfs_profile_no_automatic_conversion",
        apply: reject_conversion,
        validate: validate_nfs,
    }],
    initialize_current: reject_conversion,
    initialize_interrupted_legacy: reject_conversion,
};

const INITIALIZATION_PLAN: MigrationPlan<'static> = MigrationPlan {
    initialize_current: create_nfs_schema,
    ..INSPECTION_PLAN
};

pub(super) fn requested_plan(
    connection: &Connection,
    profile: StorageProfile,
) -> EngineResult<MigrationPlan<'static>> {
    let identity = read_identity(connection)?;
    match profile {
        StorageProfile::Local
            if identity != (MANIFEST_APPLICATION_ID, i64::from(NFS_SCHEMA_VERSION)) =>
        {
            Ok(CURRENT_PLAN)
        }
        StorageProfile::Nfs
            if matches!(identity, (0, 0))
                || identity == (MANIFEST_APPLICATION_ID, i64::from(NFS_SCHEMA_VERSION)) =>
        {
            Ok(INITIALIZATION_PLAN)
        }
        _ => Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "storage profile mismatch; local/NFS conversion is not supported; export/import into a new compatible root instead",
        )),
    }
}

pub(super) fn existing_plan(connection: &Connection) -> EngineResult<MigrationPlan<'static>> {
    if read_identity(connection)? == (MANIFEST_APPLICATION_ID, i64::from(NFS_SCHEMA_VERSION)) {
        Ok(INSPECTION_PLAN)
    } else {
        Ok(CURRENT_PLAN)
    }
}

pub(super) const fn is_current(version: u32) -> bool {
    version == CURRENT_SCHEMA_VERSION || version == NFS_SCHEMA_VERSION
}

fn create_nfs_schema(transaction: &Transaction<'_>, shards: u16) -> EngineResult<()> {
    create_v22_schema(transaction, shards)?;
    transaction
        .execute_batch(PROFILE_TABLE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_storage_profile VALUES (1, ?1, ?2, ?3, ?4)",
            rusqlite::params![
                PROFILE_VERSION,
                NFS_MODE,
                PERSIST_JOURNAL,
                EXTRA_SYNCHRONOUS
            ],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch("DROP TABLE briskdb_metadata")
        .map_err(sqlite_error::storage)?;
    transaction
        .execute_batch(DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_metadata VALUES (?1)",
            [NFS_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "UPDATE briskdb_integrity SET manifest_digest_version=?1",
            [V15_MANIFEST_DIGEST_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn reject_conversion(_: &Transaction<'_>, _: u16) -> EngineResult<()> {
    Err(EngineError::new(
        EngineErrorKind::FailedPrecondition,
        "local/NFS storage profile conversion is not supported; retain a complete stopped-owner backup and export/import into a compatible new root once that profile is enabled",
    ))
}

fn objects() -> Vec<SchemaObject> {
    let mut objects = v22_objects();
    objects.push(SchemaObject {
        object_type: "table".into(),
        name: "briskdb_storage_profile".into(),
    });
    objects.sort_by(|left, right| {
        (&left.object_type, &left.name).cmp(&(&right.object_type, &right.name))
    });
    objects
}

fn profile_row(connection: &Connection) -> EngineResult<(i64, i64, i64, i64)> {
    validate_table(
        connection,
        "briskdb_storage_profile",
        &[
            TableColumn::expected(0, "singleton", "INTEGER", false, 1),
            TableColumn::expected(1, "profile_version", "INTEGER", true, 0),
            TableColumn::expected(2, "storage_mode", "INTEGER", true, 0),
            TableColumn::expected(3, "journal_mode", "INTEGER", true, 0),
            TableColumn::expected(4, "synchronous", "INTEGER", true, 0),
        ],
        true,
    )?;
    validate_table_sql(connection, "briskdb_storage_profile", PROFILE_TABLE_SQL)?;
    let mut statement = connection
        .prepare("SELECT singleton, profile_version, storage_mode, journal_mode, synchronous FROM briskdb_storage_profile LIMIT 2")
        .map_err(|error| manifest_read_error(error, "failed to read storage profile"))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
        .map_err(|error| manifest_read_error(error, "invalid storage profile record"))?;
    match rows.as_slice() {
        [(1, version, mode, journal, synchronous)]
            if *version > 0 && *mode > 0 && *journal > 0 && *synchronous > 0 =>
        {
            Ok((*version, *mode, *journal, *synchronous))
        }
        _ => Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest storage profile must contain its canonical singleton row",
        )),
    }
}

fn validate_nfs(
    connection: &Connection,
    requested_shards: u16,
    schema: &[SchemaObject],
) -> EngineResult<ManifestSnapshot> {
    // Bound and type-check new fields before the generic checksum reader. A
    // correctly sealed future policy is unsupported, an unsealed edit corrupt.
    let row = profile_row(connection)?;
    let security_store_id = validate_security_binding(connection)?;
    let mut snapshot = validate_v12_features(
        connection,
        requested_shards,
        schema,
        IntegrityManifestDefinition {
            version: NFS_SCHEMA_VERSION,
            downgrade_fence_sql: DOWNGRADE_FENCE_SQL,
            expected_objects: &objects(),
            expected_manifest_digest_version: V15_MANIFEST_DIGEST_VERSION,
            generated_ids: true,
        },
    )?;
    if !matches!(
        row,
        (
            PROFILE_VERSION,
            NFS_MODE,
            DELETE_JOURNAL | PERSIST_JOURNAL,
            EXTRA_SYNCHRONOUS
        )
    ) {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "manifest storage profile version or durability policy is not supported by this build",
        ));
    }
    let catalog = snapshot.logical_catalog.take().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "storage profile validation omitted the catalog",
        )
    })?;
    let indexes = validate_global_indexes(connection, &catalog)?;
    snapshot.logical_catalog = Some(catalog.with_global_indexes(indexes));
    validate_document_catalog(connection, snapshot.shard_count)?;
    snapshot.security_store_id = security_store_id;
    if snapshot
        .shard_layout
        .is_some_and(|layout| layout.state() == ShardLayoutState::Adopting)
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "NFS cannot adopt a legacy local shard layout",
        ));
    }
    snapshot.shard_layout = snapshot.shard_layout.map(|layout| {
        layout.with_journal(if row.2 == PERSIST_JOURNAL {
            super::super::journal::JournalPolicy::NFS_PERSIST
        } else {
            super::super::journal::JournalPolicy::NFS_DELETE
        })
    });
    Ok(snapshot)
}

pub(in crate::storage) fn detect_storage_profile(
    connection: &Connection,
) -> EngineResult<StorageProfile> {
    let (application_id, version) = read_identity(connection)?;
    if application_id == MANIFEST_APPLICATION_ID && version == i64::from(NFS_SCHEMA_VERSION) {
        let shards = read_manifest_shard_count(connection)?;
        inspect_with_plan(connection, shards, INSPECTION_PLAN)?;
        let mode: String = connection
            .pragma_query_value(Some("main"), "journal_mode", |row| row.get(0))
            .map_err(|error| manifest_read_error(error, "failed to inspect NFS journal mode"))?;
        // PERSIST is connection-local; a read-only opener normally reports
        // DELETE. Neither is a shared-memory WAL database. Do not change it.
        if !mode.eq_ignore_ascii_case("delete") && !mode.eq_ignore_ascii_case("persist") {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "NFS manifest requires rollback journaling, not WAL or an unsafe journal mode",
            ));
        }
        Ok(StorageProfile::Nfs)
    } else {
        // This preserves complete historical validation and newer-version
        // rejection instead of guessing local from an absent profile table.
        detect_shard_count(connection)?;
        Ok(StorageProfile::Local)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_nfs_with_hook(
        connection: &mut Connection,
        hook: &mut impl FnMut(MigrationPoint) -> EngineResult<()>,
    ) -> EngineResult<ManifestSnapshot> {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error::storage)?;
        if read_identity(&transaction)? != (0, 0)
            || !matches!(
                inspect_with_plan(&transaction, 4, INSPECTION_PLAN)?,
                ManifestState::Empty
            )
        {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "NFS initialization requires a new empty manifest; conversion is not supported",
            ));
        }
        apply_schema_change(
            transaction,
            4,
            SchemaChange {
                from: 0,
                to: NFS_SCHEMA_VERSION,
                apply: create_nfs_schema,
            },
            INSPECTION_PLAN,
            hook,
        )
    }

    fn fixture() -> (tempfile::NamedTempFile, Connection) {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut connection = Connection::open(file.path()).unwrap();
        create_nfs_with_hook(&mut connection, &mut |_| Ok(())).unwrap();
        (file, connection)
    }

    #[test]
    fn nfs_profile_is_checksummed_and_detected_without_journal_conversion() {
        let (file, connection) = fixture();
        assert_eq!(
            detect_storage_profile(&connection).unwrap(),
            StorageProfile::Nfs
        );
        assert_eq!(profile_row(&connection).unwrap(), (1, 2, 2, 3));
        let bytes = std::fs::read(file.path()).unwrap();
        drop(connection);
        let readonly =
            Connection::open_with_flags(file.path(), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        assert_eq!(
            detect_storage_profile(&readonly).unwrap(),
            StorageProfile::Nfs
        );
        assert_eq!(std::fs::read(file.path()).unwrap(), bytes);
        assert_eq!(
            readonly
                .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                .unwrap(),
            "delete"
        );
    }

    #[test]
    fn local_format_stays_v22_and_local_openers_reject_nfs_before_mutation() {
        let (file, mut nfs) = fixture();
        let bytes = std::fs::read(file.path()).unwrap();
        assert_eq!(
            load_or_create_manifest(&mut nfs, 4).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(
            startup_requires_exclusive_ownership(&nfs, 4)
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(std::fs::read(file.path()).unwrap(), bytes);
        let mut local = Connection::open_in_memory().unwrap();
        load_or_create_manifest(&mut local, 4).unwrap();
        assert_eq!(read_identity(&local).unwrap().1, 22);
        assert_eq!(
            detect_storage_profile(&local).unwrap(),
            StorageProfile::Local
        );
        assert_eq!(
            create_nfs_with_hook(&mut local, &mut |_| Ok(()))
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(read_identity(&local).unwrap().1, 22);
    }

    #[test]
    fn initialization_rolls_back_at_each_format_publication_boundary() {
        for phase in [
            MigrationPhase::AfterSchemaChange,
            MigrationPhase::AfterVersionStamp,
        ] {
            let file = tempfile::NamedTempFile::new().unwrap();
            let mut connection = Connection::open(file.path()).unwrap();
            let error = create_nfs_with_hook(&mut connection, &mut |point| {
                if point.phase == phase {
                    Err(EngineError::new(
                        EngineErrorKind::Internal,
                        "injected profile initialization failure",
                    ))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Internal);
            assert!(schema_objects(&connection).unwrap().is_empty());
            assert_eq!(read_identity(&connection).unwrap(), (0, 0));
            create_nfs_with_hook(&mut connection, &mut |_| Ok(())).unwrap();
            assert_eq!(
                detect_storage_profile(&connection).unwrap(),
                StorageProfile::Nfs
            );
        }
    }

    #[test]
    fn storage_profile_public_inspection_is_read_only_and_local_open_is_fenced() {
        let directory = tempfile::tempdir().unwrap();
        let manifest = directory.path().join("manifest.sqlite");
        let mut connection = Connection::open(&manifest).unwrap();
        create_nfs_with_hook(&mut connection, &mut |_| Ok(())).unwrap();
        drop(connection);
        let bytes = std::fs::read(&manifest).unwrap();
        assert_eq!(
            crate::core::Database::detect_storage_profile(directory.path()).unwrap(),
            StorageProfile::Nfs
        );
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
        assert_eq!(std::fs::read(&manifest).unwrap(), bytes);
        assert_eq!(
            crate::core::Database::open(directory.path(), 4)
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(std::fs::read(&manifest).unwrap(), bytes);
        assert!(!directory.path().join("shards").exists());
        assert!(!directory.path().join("global-indexes").exists());
    }

    #[test]
    fn older_header_cannot_hide_storage_profile_downgrade_fence() {
        let (_file, mut connection) = fixture();
        connection
            .pragma_update(None, "user_version", V22_SCHEMA_VERSION)
            .unwrap();
        assert!(load_or_create_manifest(&mut connection, 4).is_err());
        assert!(detect_storage_profile(&connection).is_err());
        assert_eq!(profile_row(&connection).unwrap(), (1, 2, 2, 3));
        assert_eq!(
            connection
                .query_row(
                    "SELECT requires_manifest_version FROM briskdb_metadata",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            i64::from(NFS_SCHEMA_VERSION)
        );
    }

    #[test]
    fn unsealed_policy_changes_are_corruption_and_sealed_future_policies_are_unsupported() {
        for (column, value) in [
            ("profile_version", 2),
            ("storage_mode", 3),
            ("journal_mode", 3),
            ("synchronous", 2),
        ] {
            let (_file, connection) = fixture();
            connection
                .execute(
                    &format!("UPDATE briskdb_storage_profile SET {column}=?1"),
                    [value],
                )
                .unwrap();
            assert_eq!(
                detect_storage_profile(&connection).unwrap_err().kind(),
                EngineErrorKind::DataCorruption
            );
            refresh_manifest_digest(&connection).unwrap();
            assert_eq!(
                detect_storage_profile(&connection).unwrap_err().kind(),
                EngineErrorKind::FailedPrecondition
            );
        }
    }

    #[test]
    fn missing_profile_and_weakened_fence_are_rejected() {
        for sql in [
            "DELETE FROM briskdb_storage_profile",
            "DROP TABLE briskdb_storage_profile",
            "PRAGMA ignore_check_constraints=ON; UPDATE briskdb_metadata SET requires_manifest_version=22",
        ] {
            let (_file, connection) = fixture();
            connection.execute_batch(sql).unwrap();
            assert_eq!(
                detect_storage_profile(&connection).unwrap_err().kind(),
                EngineErrorKind::DataCorruption
            );
        }
    }

    #[test]
    fn rollback_candidates_are_sealed_but_wal_is_never_repaired_by_inspection() {
        let (_file, connection) = fixture();
        connection
            .execute(
                "UPDATE briskdb_storage_profile SET journal_mode=?1",
                [DELETE_JOURNAL],
            )
            .unwrap();
        refresh_manifest_digest(&connection).unwrap();
        assert_eq!(
            detect_storage_profile(&connection).unwrap(),
            StorageProfile::Nfs
        );
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        assert_eq!(
            detect_storage_profile(&connection).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(
            connection
                .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                .unwrap(),
            "wal"
        );
    }
}
