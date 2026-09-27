use super::*;

const V21_PLAN: MigrationPlan<'static> = MigrationPlan {
    current_version: V21_SCHEMA_VERSION,
    migrations: MIGRATIONS,
    initialize_current: create_v21_schema,
    initialize_interrupted_legacy: migrate_interrupted_legacy_to_v21,
};

fn at_v21() -> Connection {
    let mut connection = Connection::open_in_memory().unwrap();
    load_or_create_snapshot_with_plan(&mut connection, 2, V21_PLAN, true, &mut |_| Ok(())).unwrap();
    connection
}

fn bind_fixture(connection: &mut Connection) {
    let transaction = connection.transaction().unwrap();
    transaction
        .execute(
            "INSERT INTO briskdb_security_binding VALUES (1, 1, ?1)",
            [vec![7_u8; 16]],
        )
        .unwrap();
    refresh_manifest_digest(&transaction).unwrap();
    transaction.commit().unwrap();
}

#[test]
fn v22_upgrade_preserves_catalog_and_starts_without_a_security_binding() {
    let mut connection = at_v21();
    let before = validate_v21(&connection, 2, &schema_objects(&connection).unwrap()).unwrap();
    load_or_create_manifest(&mut connection, 2).unwrap();
    let after = validate_v22(&connection, 2, &schema_objects(&connection).unwrap()).unwrap();
    assert_eq!(before.logical_catalog, after.logical_catalog);
    assert_eq!(before.routing_catalog, after.routing_catalog);
    assert_eq!(before.shard_layout, after.shard_layout);
    assert_eq!(after.security_store_id, None);
    assert_eq!(
        read_identity(&connection).unwrap().1,
        i64::from(V22_SCHEMA_VERSION)
    );
    assert_eq!(schema_objects(&connection).unwrap(), v22_objects());
    assert_eq!(
        connection
            .query_row(
                "SELECT manifest_digest_version FROM briskdb_integrity",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        14
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM briskdb_security_binding", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
}

#[test]
fn v22_migration_rolls_back_at_each_injected_boundary_and_retries_cleanly() {
    for (phase, inject_panic) in [
        MigrationPhase::AfterSchemaChange,
        MigrationPhase::AfterVersionStamp,
    ]
    .into_iter()
    .flat_map(|phase| [false, true].map(|panic| (phase, panic)))
    {
        let mut connection = at_v21();
        let before = manifest_semantic_digest(&connection).unwrap();
        let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            load_or_create_with_hook(&mut connection, 2, |point| {
                if point.from == V21_SCHEMA_VERSION && point.phase == phase {
                    if inject_panic {
                        panic!("injected v22 rollback panic");
                    }
                    return Err(EngineError::new(
                        EngineErrorKind::Cancelled,
                        "injected v22 rollback",
                    ));
                }
                Ok(())
            })
        }));
        if inject_panic {
            assert!(attempt.is_err());
        } else {
            assert_eq!(
                attempt.unwrap().unwrap_err().kind(),
                EngineErrorKind::Cancelled
            );
        }
        assert_eq!(
            read_identity(&connection).unwrap().1,
            i64::from(V21_SCHEMA_VERSION)
        );
        assert_eq!(schema_objects(&connection).unwrap(), v19_objects());
        assert_eq!(manifest_semantic_digest(&connection).unwrap(), before);
        load_or_create_manifest(&mut connection, 2).unwrap();
        assert_eq!(
            read_identity(&connection).unwrap().1,
            i64::from(V22_SCHEMA_VERSION)
        );
    }
}

#[test]
fn v21_reader_and_tampered_version_cannot_ignore_the_v22_binding_table() {
    let mut connection = at_v21();
    load_or_create_manifest(&mut connection, 2).unwrap();
    assert_eq!(
        inspect_with_plan(&connection, 2, V21_PLAN)
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
    bind_fixture(&mut connection);
    assert_eq!(
        inspect_with_plan(&connection, 2, V21_PLAN)
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
    connection
        .pragma_update(None, "user_version", V21_SCHEMA_VERSION)
        .unwrap();
    assert!(inspect_with_plan(&connection, 2, V21_PLAN).is_err());
    assert_eq!(
        connection
            .query_row(
                "SELECT requires_manifest_version FROM briskdb_metadata",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        22
    );
}

#[test]
fn bound_manifest_rejects_every_ordinary_startup_path_even_with_auth_feature() {
    let mut connection = at_v21();
    load_or_create_manifest(&mut connection, 2).unwrap();
    bind_fixture(&mut connection);
    let snapshot = validate_v22(&connection, 2, &schema_objects(&connection).unwrap()).unwrap();
    assert_eq!(snapshot.security_store_id, Some([7; 16]));
    assert_eq!(
        startup_requires_exclusive_ownership(&connection, 2)
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
    assert_eq!(
        load_or_create_manifest(&mut connection, 2)
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
    assert!(downgrade_v22_manifest_to_v21_for_test(&connection, 2).is_err());
}

#[test]
fn security_binding_is_checksum_covered_and_malformed_rows_fail_closed() {
    let mut connection = at_v21();
    load_or_create_manifest(&mut connection, 2).unwrap();
    bind_fixture(&mut connection);
    connection
        .execute(
            "UPDATE briskdb_security_binding SET store_id = ?1",
            [vec![8_u8; 16]],
        )
        .unwrap();
    assert_eq!(
        validate_v22(&connection, 2, &schema_objects(&connection).unwrap())
            .unwrap_err()
            .kind(),
        EngineErrorKind::DataCorruption
    );
    refresh_manifest_digest(&connection).unwrap();
    assert_eq!(
        validate_security_binding(&connection).unwrap(),
        Some([8; 16])
    );
    for sql in [
        "UPDATE briskdb_security_binding SET store_id = zeroblob(16)",
        "UPDATE briskdb_security_binding SET store_id = zeroblob(15)",
        "UPDATE briskdb_security_binding SET store_id = zeroblob(17)",
        "UPDATE briskdb_security_binding SET store_id = zeroblob(1048576)",
        "UPDATE briskdb_security_binding SET format_version = 2",
        "UPDATE briskdb_security_binding SET singleton = 2, format_version = 1",
        "INSERT INTO briskdb_security_binding VALUES (2, 1, CAST('abcdefghijklmnop' AS BLOB))",
    ] {
        connection
            .execute_batch("PRAGMA ignore_check_constraints=ON")
            .unwrap();
        connection.execute_batch("DELETE FROM briskdb_security_binding; INSERT INTO briskdb_security_binding VALUES (1, 1, CAST('abcdefghijklmnop' AS BLOB));").unwrap();
        connection.execute_batch(sql).unwrap();
        assert_eq!(
            validate_security_binding(&connection).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }
}

#[test]
fn real_root_bound_startup_does_not_change_manifest_or_open_anonymous_storage() {
    let temp = tempfile::tempdir().unwrap();
    drop(crate::core::Database::open(temp.path(), 2).unwrap());
    let path = temp.path().join("manifest.sqlite");
    let mut connection = Connection::open(&path).unwrap();
    bind_fixture(&mut connection);
    drop(connection);
    let before = std::fs::read(&path).unwrap();
    let error = crate::core::Database::open(temp.path(), 2).unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
    assert!(error.to_string().contains("authenticated engine startup"));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(!temp.path().join("security").exists());
}
