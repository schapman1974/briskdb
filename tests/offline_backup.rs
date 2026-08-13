use std::{fs, path::Path};

use briskdb::core::{Database, Engine, Statement, Value};

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

    let database = Database::open(&live, SHARDS).unwrap();
    database
        .broadcast(
            "CREATE TABLE backup_items (
                id TEXT PRIMARY KEY,
                shard_number INTEGER NOT NULL
             )",
        )
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

    copy_directory(&live, &backup);
    copy_directory(&backup, &restored);

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
    restored_engine.shutdown().await.unwrap();
}
