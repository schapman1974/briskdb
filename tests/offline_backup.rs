use std::{fs, path::Path, sync::Arc};

use briskdb::core::{
    CanonicalIndexKey, CheckpointDatabase, Database, Engine, GlobalIndexDeclaration,
    GlobalIndexHealthState, GlobalIndexKeyPart, GlobalIndexKeySource, GlobalIndexKeyType,
    GlobalIndexOwner, GlobalIndexStorageTopology, GlobalOperationId, GlobalUniqueMutation,
    ShardKeyMetadata, ShardKeyType, Statement, TableDeclaration, UniqueNullSemantics, Value,
};

const SHARDS: u16 = 4;

fn copy_directory(source: &Path, destination: &Path) {
    fs::create_dir(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_directory(&source_path, &destination_path);
        } else {
            fs::copy(&source_path, &destination_path).unwrap();
        }
    }
}

fn one_key_per_shard(database: &Database) -> Vec<String> {
    let mut keys = vec![None; usize::from(SHARDS)];
    for candidate in 0..10_000 {
        let key = format!("backup-key-{candidate}");
        let shard = usize::from(database.shard_for_key(key.as_bytes()));
        keys[shard].get_or_insert(key);
        if keys.iter().all(Option::is_some) {
            return keys.into_iter().map(Option::unwrap).collect();
        }
    }
    panic!("failed to find one routed key for every shard");
}

#[tokio::test]
async fn stopped_server_backup_restores_schema_and_rows_from_every_shard() {
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live");
    let backup = directory.path().join("backup");
    let restored = directory.path().join("restored");

    let mut database = Database::open(&live, SHARDS).unwrap();
    database
        .broadcast(
            "CREATE TABLE backup_items (
                id TEXT NOT NULL PRIMARY KEY,
                shard_number INTEGER NOT NULL
             )",
        )
        .unwrap();
    let logical = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "backup_items",
                ShardKeyMetadata::new("id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    let keys = one_key_per_shard(&database);
    drop(database);

    let engine = Engine::open(&live, SHARDS).await.unwrap();
    for (expected_shard, key) in keys.iter().enumerate() {
        let session = engine.session();
        session.set_routing_key(key).await.unwrap();
        let inserted = engine
            .execute(
                &session,
                Statement::new(
                    "INSERT INTO backup_items (id, shard_number) VALUES (?1, ?2)",
                    vec![Value::from(key.clone()), Value::from(expected_shard as i64)],
                ),
            )
            .await
            .unwrap();
        assert_eq!(usize::from(inserted.shard), expected_shard);
        assert_eq!(inserted.value, 1);
    }
    engine.shutdown().await.unwrap();
    drop(engine);

    let mut database = Database::open(&live, SHARDS).unwrap();
    let table_id = database
        .catalog()
        .table("default", "backup_items")
        .unwrap()
        .unwrap()
        .id();
    let declaration = GlobalIndexDeclaration::new(
        table_id,
        "backup_items_id_global",
        vec![GlobalIndexKeyPart::new(
            GlobalIndexKeySource::column("id").unwrap(),
            GlobalIndexKeyType::Text,
        )],
    )
    .unwrap()
    .with_topology(GlobalIndexStorageTopology::selected_v1());
    let index_id = database.create_global_index(declaration).unwrap();
    assert_eq!(
        database
            .build_global_index(index_id)
            .unwrap()
            .indexed_rows(),
        u64::from(SHARDS)
    );
    let unique_id = database
        .create_global_index(
            GlobalIndexDeclaration::new(
                table_id,
                "backup_items_id_unique",
                vec![GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::column("id").unwrap(),
                    GlobalIndexKeyType::Text,
                )],
            )
            .unwrap()
            .unique(UniqueNullSemantics::Distinct)
            .with_topology(GlobalIndexStorageTopology::selected_v1()),
        )
        .unwrap();
    assert_eq!(
        database
            .build_global_index(unique_id)
            .unwrap()
            .indexed_rows(),
        u64::from(SHARDS)
    );
    let value_id = database
        .create_global_index(
            GlobalIndexDeclaration::new(
                table_id,
                "backup_items_global_value_unique",
                vec![GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::column("shard_number").unwrap(),
                    GlobalIndexKeyType::Int64,
                )],
            )
            .unwrap()
            .unique(UniqueNullSemantics::NotDistinct)
            .with_topology(GlobalIndexStorageTopology::selected_v1()),
        )
        .unwrap();
    assert_eq!(
        database
            .build_global_index(value_id)
            .unwrap()
            .indexed_rows(),
        u64::from(SHARDS)
    );

    let lease_operation = GlobalOperationId::new(100_u128.to_le_bytes()).unwrap();
    let lease = database
        .lease_global_values(lease_operation, value_id, 3)
        .unwrap();
    assert_eq!((lease.first(), lease.last()), (1, 3));
    database
        .finalize_global_value_lease(lease_operation)
        .unwrap();

    let reservation_operation = GlobalOperationId::new(200_u128.to_le_bytes()).unwrap();
    let reserved_key =
        CanonicalIndexKey::encode_values(&[Value::from("reserved-after-backup")]).unwrap();
    let reservation = GlobalUniqueMutation::claim(
        unique_id,
        reserved_key,
        GlobalIndexOwner::new(0, b"reserved-row".to_vec()).unwrap(),
    );
    database
        .reserve_global_unique(reservation_operation, &reservation)
        .unwrap();

    let database = Arc::new(database);
    let engine = Engine::from_database(Arc::clone(&database));
    let extra_key = (10_000..)
        .map(|candidate| format!("backup-extra-{candidate}"))
        .find(|candidate| !keys.contains(candidate))
        .unwrap();
    let session = engine.session();
    session.set_routing_key(&extra_key).await.unwrap();
    engine
        .execute_write(
            &session,
            Statement::new(
                "INSERT INTO backup_items (id, shard_number) VALUES (?1, ?2)",
                vec![Value::from(extra_key.clone()), Value::from(99_i64)],
            ),
        )
        .await
        .unwrap();
    session.close().await.unwrap();

    let before = engine.global_index_operational_report().await.unwrap();
    assert_eq!(before.state(), GlobalIndexHealthState::Degraded);
    assert_eq!(before.retained_outbox_events(), 1);
    let non_unique = before
        .indexes()
        .iter()
        .find(|status| status.index_id() == index_id)
        .unwrap();
    assert_eq!(non_unique.async_lag(), 1);
    let unique = before
        .indexes()
        .iter()
        .find(|status| status.index_id() == unique_id)
        .unwrap();
    assert_eq!(unique.unique_keys(), u64::from(SHARDS) + 1);
    assert_eq!(unique.active_operations(), 1);
    assert_eq!(unique.active_unique_reservations(), 1);

    let checkpoint = engine.checkpoint().await.unwrap();
    assert_eq!(checkpoint.databases().len(), 2);
    assert_eq!(
        checkpoint
            .databases()
            .iter()
            .map(|report| report.database())
            .collect::<Vec<_>>(),
        vec![
            CheckpointDatabase::Manifest,
            CheckpointDatabase::GlobalIndex
        ]
    );
    assert!(checkpoint.complete());
    engine.shutdown().await.unwrap();
    drop(engine);
    drop(database);

    copy_directory(&live, &backup);
    copy_directory(&backup, &restored);

    let mut restored_database = Database::open(&restored, SHARDS).unwrap();
    let restored_status = restored_database.global_index_operational_report().unwrap();
    assert_eq!(restored_status.retained_outbox_events(), 1);
    assert_eq!(
        restored_status
            .indexes()
            .iter()
            .find(|status| status.index_id() == index_id)
            .unwrap()
            .async_lag(),
        1
    );
    let restored_unique = restored_status
        .indexes()
        .iter()
        .find(|status| status.index_id() == unique_id)
        .unwrap();
    assert_eq!(restored_unique.unique_keys(), u64::from(SHARDS) + 1);
    assert_eq!(restored_unique.active_unique_reservations(), 1);
    restored_database
        .rollback_global_unique(reservation_operation)
        .unwrap();
    let restored_lease_operation = GlobalOperationId::new(201_u128.to_le_bytes()).unwrap();
    assert_eq!(
        restored_database
            .lease_global_values(restored_lease_operation, value_id, 2)
            .unwrap()
            .first(),
        4
    );
    restored_database
        .finalize_global_value_lease(restored_lease_operation)
        .unwrap();
    restored_database
        .process_global_index_async(index_id, Default::default())
        .unwrap();
    for index in [index_id, unique_id, value_id] {
        let rebuilt = restored_database.rebuild_global_index(index).unwrap();
        assert_eq!(rebuilt.indexed_rows(), u64::from(SHARDS) + 1);
        let validation = restored_database.validate_global_index(index).unwrap();
        assert!(
            validation.is_valid(),
            "index {index} failed: {validation:?}"
        );
        assert_eq!(validation.source_rows_examined(), u64::from(SHARDS) + 1);
        assert_eq!(
            validation.physical_entries_examined(),
            u64::from(SHARDS) + 1
        );
    }
    drop(restored_database);
    let restored_engine = Engine::open(&restored, SHARDS).await.unwrap();
    assert_eq!(restored_engine.catalog().schema_generation(), 1);
    for (expected_shard, key) in keys.iter().enumerate() {
        let session = restored_engine.session();
        session.set_routing_key(key).await.unwrap();
        let result = restored_engine
            .query(
                &session,
                Statement::new(
                    "SELECT id, shard_number FROM backup_items WHERE id = ?1",
                    vec![Value::from(key.clone())],
                ),
            )
            .await
            .unwrap();
        assert_eq!(usize::from(result.shard), expected_shard);
        assert_eq!(result.value.rows().len(), 1);
        assert_eq!(
            result.value.rows()[0].get(0),
            Some(&Value::from(key.clone()))
        );
        assert_eq!(
            result.value.rows()[0].get(1),
            Some(&Value::from(expected_shard as i64))
        );
    }
    let session = restored_engine.session();
    session.set_routing_key(&extra_key).await.unwrap();
    let extra = restored_engine
        .query(
            &session,
            Statement::new(
                "SELECT shard_number FROM backup_items WHERE id = ?1",
                vec![Value::from(extra_key)],
            ),
        )
        .await
        .unwrap();
    assert_eq!(extra.value.rows()[0].get(0), Some(&Value::from(99_i64)));
    restored_engine.shutdown().await.unwrap();
}
