//! Local-only capability fence for maintained ordered secondary indexes.
//! Existing Ready indexes remain unchanged until explicitly rebuilt; migration
//! neither scans their BSON nor turns a shared create-index no-op into DDL.

use super::*;

const TABLE_SQL: &str = "CREATE TABLE briskdb_document_index_ordering (
    index_id INTEGER PRIMARY KEY CHECK (index_id > 0),
    key_format_version INTEGER NOT NULL CHECK (key_format_version = 1),
    FOREIGN KEY (index_id)
        REFERENCES briskdb_document_index_identities (index_id)
        ON DELETE CASCADE
) STRICT";

const DOWNGRADE_FENCE_SQL: &str = "CREATE TABLE briskdb_metadata (
    requires_manifest_version INTEGER NOT NULL
        CHECK (requires_manifest_version >= 24)
) STRICT";

pub(super) const DIGEST_QUERY: ManifestDigestQuery = ManifestDigestQuery {
    table: "briskdb_document_index_ordering",
    columns: &["index_id", "key_format_version"],
    sql: "SELECT index_id, key_format_version FROM briskdb_document_index_ordering ORDER BY index_id",
};

pub(super) fn migrate_v22_to_v24(transaction: &Transaction<'_>, _: u16) -> EngineResult<()> {
    transaction
        .execute_batch(TABLE_SQL)
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
            [V24_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "UPDATE briskdb_integrity SET manifest_digest_version = ?1 WHERE singleton = 1",
            [V16_MANIFEST_DIGEST_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

pub(super) fn objects() -> Vec<SchemaObject> {
    let mut objects = v22_objects();
    objects.push(SchemaObject {
        object_type: "table".into(),
        name: "briskdb_document_index_ordering".into(),
    });
    objects.sort_by(|left, right| {
        (&left.object_type, &left.name).cmp(&(&right.object_type, &right.name))
    });
    objects
}

pub(super) fn validate_v24(
    connection: &Connection,
    requested_shards: u16,
    objects: &[SchemaObject],
) -> EngineResult<ManifestSnapshot> {
    let security_store_id = validate_security_binding(connection)?;
    validate_capabilities(connection)?;
    let mut snapshot = validate_v12_features(
        connection,
        requested_shards,
        objects,
        IntegrityManifestDefinition {
            version: V24_SCHEMA_VERSION,
            downgrade_fence_sql: DOWNGRADE_FENCE_SQL,
            expected_objects: &self::objects(),
            expected_manifest_digest_version: V16_MANIFEST_DIGEST_VERSION,
            generated_ids: true,
        },
    )?;
    let catalog = snapshot.logical_catalog.take().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            "ordered-index validation omitted the logical catalog",
        )
    })?;
    let indexes = validate_global_indexes(connection, &catalog)?;
    snapshot.logical_catalog = Some(catalog.with_global_indexes(indexes));
    validate_document_catalog(connection, snapshot.shard_count)?;
    snapshot.security_store_id = security_store_id;
    Ok(snapshot)
}

fn validate_capabilities(connection: &Connection) -> EngineResult<()> {
    validate_table(
        connection,
        "briskdb_document_index_ordering",
        &[
            TableColumn::expected(0, "index_id", "INTEGER", false, 1),
            TableColumn::expected(1, "key_format_version", "INTEGER", true, 0),
        ],
        true,
    )?;
    validate_table_sql(connection, "briskdb_document_index_ordering", TABLE_SQL)?;
    let invalid: bool = connection
        .query_row(
            "SELECT EXISTS (
            SELECT 1 FROM briskdb_document_index_ordering AS o
            LEFT JOIN briskdb_document_index_identities AS i ON i.index_id = o.index_id
            LEFT JOIN briskdb_document_indexes AS d
                ON d.collection_id = i.collection_id AND d.index_name = i.index_name
            WHERE typeof(o.index_id) <> 'integer' OR o.index_id <= 0
                OR typeof(o.key_format_version) <> 'integer' OR o.key_format_version <> 1
                OR i.index_id IS NULL OR d.is_builtin IS NULL OR d.is_builtin <> 0
        ) OR (SELECT count(*) FROM briskdb_document_index_ordering) > ?1",
            [i64::try_from(MAX_DOCUMENT_INDEXES).expect("index catalog bound fits i64")],
            |row| row.get(0),
        )
        .map_err(|error| {
            manifest_read_error(error, "failed to validate ordered-index capabilities")
        })?;
    if invalid {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest ordered-index capability is invalid",
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn downgrade_to_v22_for_test(connection: &Connection, shards: u16) -> EngineResult<()> {
    let active: bool = connection
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM briskdb_document_index_ordering)",
            [],
            |row| row.get(0),
        )
        .map_err(sqlite_error::storage)?;
    if active {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "cannot downgrade a fixture with ordered-index capabilities",
        ));
    }
    connection
        .execute_batch("DROP TABLE briskdb_document_index_ordering; DROP TABLE briskdb_metadata;")
        .map_err(sqlite_error::storage)?;
    connection
        .execute_batch(V22_DOWNGRADE_FENCE_SQL)
        .map_err(sqlite_error::storage)?;
    connection
        .execute(
            "INSERT INTO briskdb_metadata VALUES (?1)",
            [V22_SCHEMA_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    connection
        .execute(
            "UPDATE briskdb_integrity SET manifest_digest_version = ?1 WHERE singleton = 1",
            [V14_MANIFEST_DIGEST_VERSION],
        )
        .map_err(sqlite_error::storage)?;
    set_identity(connection, V22_SCHEMA_VERSION)?;
    refresh_manifest_digest(connection)?;
    validate_v22(connection, shards, &schema_objects(connection)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const OLD_PLAN: MigrationPlan<'static> = MigrationPlan {
        current_version: V22_SCHEMA_VERSION,
        migrations: MIGRATIONS,
        initialize_current: create_v22_schema,
        initialize_interrupted_legacy: migrate_interrupted_legacy_to_v22,
    };

    fn at_v22() -> Connection {
        let mut connection = Connection::open_in_memory().unwrap();
        load_or_create_snapshot_with_plan(&mut connection, 2, OLD_PLAN, true, &mut |_| Ok(()))
            .unwrap();
        connection
    }

    #[test]
    fn local_upgrade_skips_reserved_nfs_format_without_scanning_or_certifying_indexes() {
        let mut connection = at_v22();
        let before = validate_v22(&connection, 2, &schema_objects(&connection).unwrap()).unwrap();
        load_or_create_manifest(&mut connection, 2).unwrap();
        let after = validate_v24(&connection, 2, &schema_objects(&connection).unwrap()).unwrap();
        assert_eq!(before.logical_catalog, after.logical_catalog);
        assert_eq!(before.routing_catalog, after.routing_catalog);
        assert_eq!(before.shard_layout, after.shard_layout);
        assert_eq!(before.security_store_id, after.security_store_id);
        assert_eq!(
            read_identity(&connection).unwrap().1,
            i64::from(V24_SCHEMA_VERSION)
        );
        assert_eq!(
            detect_storage_profile(&connection).unwrap(),
            crate::core::StorageProfile::Local
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM briskdb_document_index_ordering",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM sqlite_schema WHERE name='briskdb_storage_profile'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        let before_digest = manifest_semantic_digest(&connection).unwrap();
        assert_eq!(
            inspect_with_plan(&connection, 2, OLD_PLAN)
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(
            manifest_semantic_digest(&connection).unwrap(),
            before_digest
        );
        assert!(
            storage_profile::requested_plan(&connection, crate::core::StorageProfile::Nfs).is_err()
        );
    }

    #[test]
    fn capability_upgrade_rolls_back_errors_and_panics_at_both_boundaries() {
        for phase in [
            MigrationPhase::AfterSchemaChange,
            MigrationPhase::AfterVersionStamp,
        ] {
            for panic in [false, true] {
                let mut connection = at_v22();
                let before = manifest_semantic_digest(&connection).unwrap();
                let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    load_or_create_with_hook(&mut connection, 2, |point| {
                        if point.from == V22_SCHEMA_VERSION && point.phase == phase {
                            if panic {
                                panic!("injected ordered-index upgrade panic");
                            }
                            return Err(EngineError::new(
                                EngineErrorKind::Cancelled,
                                "injected ordered-index upgrade cancellation",
                            ));
                        }
                        Ok(())
                    })
                }));
                if panic {
                    assert!(attempt.is_err());
                } else {
                    assert_eq!(
                        attempt.unwrap().unwrap_err().kind(),
                        EngineErrorKind::Cancelled
                    );
                }
                assert_eq!(
                    read_identity(&connection).unwrap().1,
                    i64::from(V22_SCHEMA_VERSION)
                );
                assert_eq!(schema_objects(&connection).unwrap(), v22_objects());
                assert_eq!(manifest_semantic_digest(&connection).unwrap(), before);
                load_or_create_manifest(&mut connection, 2).unwrap();
                validate_v24(&connection, 2, &schema_objects(&connection).unwrap()).unwrap();
            }
        }
    }

    #[test]
    fn resealed_orphan_capabilities_fail_closed() {
        for mutation in [
            "INSERT INTO briskdb_document_index_ordering VALUES (999,1)",
            "INSERT INTO briskdb_document_index_ordering VALUES (0,1)",
            "INSERT INTO briskdb_document_index_ordering VALUES (1,2)",
        ] {
            let mut connection = at_v22();
            load_or_create_manifest(&mut connection, 2).unwrap();
            connection
                .execute_batch("PRAGMA foreign_keys=OFF; PRAGMA ignore_check_constraints=ON;")
                .unwrap();
            connection.execute_batch(mutation).unwrap();
            refresh_manifest_digest(&connection).unwrap();
            assert_eq!(
                validate_v24(&connection, 2, &schema_objects(&connection).unwrap())
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::DataCorruption
            );
        }
    }
}
