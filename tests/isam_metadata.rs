#![cfg(all(unix, feature = "experimental-isam"))]

use briskdb::core::{Database, ShardKeyMetadata, ShardKeyType, TableDeclaration};
use briskdb::{EngineErrorKind, MetadataBackend, Value};

fn open(path: &std::path::Path) -> Database {
    Database::open_with_metadata_backend(path, 2, MetadataBackend::Isam).unwrap()
}

#[test]
fn hybrid_metadata_runs_real_sqlite_shards_and_reopens_without_sqlite_manifest() {
    for backend in [MetadataBackend::Sqlite, MetadataBackend::Isam] {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open_with_metadata_backend(directory.path(), 2, backend).unwrap();
        if backend == MetadataBackend::Isam {
            let metadata =
                briskdb::isam::Store::open_read_only(directory.path().join("manifest.isam"))
                    .unwrap();
            assert_eq!(metadata.format_version(), 4);
        }
        let ddl = "CREATE TABLE widgets (id TEXT PRIMARY KEY, name TEXT NOT NULL);";
        database.broadcast(ddl).unwrap();
        database.broadcast(ddl).unwrap(); // durable DDL receipt, not a second CREATE
        database
            .execute(
                "key",
                "INSERT INTO widgets VALUES (?1, ?2)",
                &[Value::from("key"), Value::from("first")],
            )
            .unwrap();
        assert_eq!(
            database
                .query("key", "SELECT name FROM widgets", &[])
                .unwrap()
                .rows()
                .len(),
            1
        );
        let generation = database.catalog().schema_generation();
        assert_eq!(generation, 1);
        drop(database);
        let database = Database::open_with_metadata_backend(directory.path(), 2, backend).unwrap();
        assert_eq!(database.catalog().schema_generation(), generation);
        assert_eq!(database.metadata_backend(), backend);
        assert_eq!(
            database
                .query("key", "SELECT name FROM widgets", &[])
                .unwrap()
                .rows()
                .len(),
            1
        );
        database
            .execute("key", "UPDATE widgets SET name = 'second'", &[])
            .unwrap();
        database.execute("key", "DELETE FROM widgets", &[]).unwrap();
        assert!(
            database
                .query("key", "SELECT * FROM widgets", &[])
                .unwrap()
                .rows()
                .is_empty()
        );
        assert_eq!(
            directory.path().join("manifest.sqlite").exists(),
            backend == MetadataBackend::Sqlite
        );
        assert_eq!(
            directory.path().join("manifest.isam").exists(),
            backend == MetadataBackend::Isam
        );
        assert!(directory.path().join("shards/0000.sqlite").exists());
    }
}

#[test]
fn native_table_registration_survives_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let mut database = open(directory.path());
    database
        .broadcast("CREATE TABLE widgets (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    let declaration = TableDeclaration::sharded(
        database.catalog().default_database().id(),
        "widgets",
        ShardKeyMetadata::new("id", ShardKeyType::Text).unwrap(),
    )
    .unwrap();
    database.register_tables(vec![declaration.clone()]).unwrap();
    database.register_tables(vec![declaration]).unwrap();
    assert_eq!(database.catalog().tables().len(), 1);
    drop(database);
    let database = open(directory.path());
    assert_eq!(database.catalog().tables()[0].name(), "widgets");
    assert!(!directory.path().join("manifest.sqlite").exists());
}

#[test]
fn backend_mismatch_never_converts_or_creates_other_manifest() {
    for backend in [MetadataBackend::Sqlite, MetadataBackend::Isam] {
        let directory = tempfile::tempdir().unwrap();
        drop(Database::open_with_metadata_backend(directory.path(), 2, backend).unwrap());
        let name = if backend == MetadataBackend::Sqlite {
            "manifest.sqlite"
        } else {
            "manifest.isam"
        };
        let before = std::fs::read(directory.path().join(name)).unwrap();
        let other = if backend == MetadataBackend::Sqlite {
            MetadataBackend::Isam
        } else {
            MetadataBackend::Sqlite
        };
        let error = Database::open_with_metadata_backend(directory.path(), 2, other).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(before, std::fs::read(directory.path().join(name)).unwrap());
        assert!(
            !directory
                .path()
                .join(if name == "manifest.sqlite" {
                    "manifest.isam"
                } else {
                    "manifest.sqlite"
                })
                .exists()
        );
    }
}

#[test]
fn missing_or_wrong_count_native_metadata_is_not_reinitialized() {
    let directory = tempfile::tempdir().unwrap();
    drop(open(directory.path()));
    assert_eq!(
        Database::open_with_metadata_backend(directory.path(), 3, MetadataBackend::Isam)
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
    std::fs::rename(
        directory.path().join("manifest.isam"),
        directory.path().join("saved.isam"),
    )
    .unwrap();
    assert_eq!(
        Database::open_with_metadata_backend(directory.path(), 2, MetadataBackend::Isam)
            .unwrap_err()
            .kind(),
        EngineErrorKind::DataCorruption
    );
    assert!(!directory.path().join("manifest.isam").exists());
}

#[test]
fn failed_ddl_preflight_leaves_native_catalog_usable() {
    let directory = tempfile::tempdir().unwrap();
    let database = open(directory.path());
    assert!(database.broadcast("CREATE TABLE invalid(").is_err());
    assert_eq!(database.catalog().schema_generation(), 0);
    database
        .broadcast("CREATE TABLE valid(id INTEGER PRIMARY KEY)")
        .unwrap();
    assert_eq!(database.catalog().schema_generation(), 1);
}

#[test]
fn independent_handles_can_read_and_write_sqlite_shards_with_native_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let database = open(directory.path());
    database
        .broadcast("CREATE TABLE events (id TEXT PRIMARY KEY, value INTEGER NOT NULL)")
        .unwrap();
    let ready = std::sync::Arc::new(std::sync::Barrier::new(4));
    let workers: Vec<_> = (0..4)
        .map(|worker| {
            let root = directory.path().to_path_buf();
            let ready = std::sync::Arc::clone(&ready);
            std::thread::spawn(move || {
                let db = open(&root);
                // Startup owns schema admission in both backends. Measure steady
                // operations only after all independent handles finish opening.
                ready.wait();
                for number in 0..25 {
                    let id = format!("{worker}:{number}");
                    db.execute(
                        &id,
                        "INSERT INTO events VALUES (?1, ?2)",
                        &[Value::from(id.clone()), Value::from(7_i64)],
                    )
                    .unwrap();
                    assert_eq!(
                        db.query(
                            &id,
                            "SELECT value FROM events WHERE id = ?1",
                            &[Value::from(id.clone())]
                        )
                        .unwrap()
                        .rows()
                        .len(),
                        1
                    );
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    assert!(!directory.path().join("manifest.sqlite").exists());
}

#[test]
fn native_metadata_detects_schema_damage_and_remains_degraded() {
    let directory = tempfile::tempdir().unwrap();
    let database = open(directory.path());
    database
        .broadcast("CREATE TABLE good(id INTEGER PRIMARY KEY)")
        .unwrap();
    drop(database);
    let shard = rusqlite::Connection::open(directory.path().join("shards/0000.sqlite")).unwrap();
    shard
        .execute_batch("CREATE TABLE unexpected(id INTEGER)")
        .unwrap();
    drop(shard);
    for _ in 0..2 {
        assert_eq!(
            Database::open_with_metadata_backend(directory.path(), 2, MetadataBackend::Isam)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }
    assert!(!directory.path().join("manifest.sqlite").exists());
}

#[test]
fn native_metadata_rejects_manifest_symlinks() {
    let directory = tempfile::tempdir().unwrap();
    drop(open(directory.path()));
    std::fs::rename(
        directory.path().join("manifest.isam"),
        directory.path().join("saved.isam"),
    )
    .unwrap();
    std::os::unix::fs::symlink(
        directory.path().join("saved.isam"),
        directory.path().join("manifest.isam"),
    )
    .unwrap();
    assert_eq!(
        Database::open_with_metadata_backend(directory.path(), 2, MetadataBackend::Isam)
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
}

#[cfg(feature = "embedded")]
#[tokio::test]
async fn builder_can_discover_isam_metadata_and_wrap_native_database() {
    let directory = tempfile::tempdir().unwrap();
    let database = open(directory.path());
    let engine = briskdb::core::Engine::from_database(std::sync::Arc::new(database));
    drop(engine);
    let database = briskdb::BriskDb::builder(directory.path())
        .with_metadata_backend(MetadataBackend::Isam)
        .open()
        .await
        .unwrap();
    database.close().await.unwrap();
}
