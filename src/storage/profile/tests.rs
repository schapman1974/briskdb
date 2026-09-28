use super::*;
use crate::{
    core::{EngineErrorKind, StorageProfile},
    storage::Storage,
};
use rusqlite::Connection;

fn nfs(root: &Path) -> Storage {
    // Internal profile integration, not a public mount-qualification bypass.
    Storage::open_with_profile_control(root, 2, None, None, StorageProfile::Nfs).unwrap()
}

fn assert_rollback(connection: &Connection) {
    let mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .unwrap();
    assert_eq!(mode, "persist");
    assert_eq!(
        connection
            .pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))
            .unwrap(),
        3
    );
    assert_eq!(
        connection
            .pragma_query_value(None, "locking_mode", |row| row.get::<_, String>(0))
            .unwrap(),
        "normal"
    );
}

#[test]
fn nfs_profile_initializes_migrates_reopens_and_never_enables_wal() {
    let root = tempfile::tempdir().unwrap();
    let storage = nfs(root.path());
    assert_eq!(
        crate::storage::detect_storage_profile(root.path()).unwrap(),
        StorageProfile::Nfs
    );
    assert_eq!(
        crate::storage::detect_shard_count_with_profile(root.path(), StorageProfile::Nfs).unwrap(),
        2
    );
    assert!(crate::storage::detect_shard_count(root.path()).is_err());
    {
        let mut migration = storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        storage
            .apply_schema_migration(
                "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();
    }
    for shard in 0..2 {
        let connection = storage.open_shard(shard).unwrap();
        assert_rollback(&connection);
        connection
            .execute("INSERT INTO items VALUES (?1, 'value')", [i64::from(shard)])
            .unwrap();
    }
    let manifest = Connection::open(root.path().join("manifest.sqlite")).unwrap();
    storage.configure_manifest_connection(&manifest).unwrap();
    assert_rollback(&manifest);
    assert_eq!(
        manifest
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        23
    );
    drop(manifest);
    assert_eq!(
        storage.checkpoint_auxiliary_databases().unwrap_err().kind(),
        EngineErrorKind::Unsupported
    );
    drop(storage);
    let reopened = nfs(root.path());
    for shard in 0..2 {
        let connection = reopened.open_shard(shard).unwrap();
        assert_rollback(&connection);
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM items", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
    for file in [
        "manifest.sqlite",
        "shards/0000.sqlite",
        "shards/0001.sqlite",
    ] {
        for suffix in ["-wal", "-shm"] {
            assert!(!root.path().join(format!("{file}{suffix}")).exists());
        }
    }
}

#[test]
fn nfs_profile_mismatches_fail_without_conversion() {
    for original in [StorageProfile::Local, StorageProfile::Nfs] {
        let root = tempfile::tempdir().unwrap();
        drop(Storage::open_with_profile_control(root.path(), 2, None, None, original).unwrap());
        let manifest = root.path().join("manifest.sqlite");
        let before = std::fs::read(&manifest).unwrap();
        let other = if original == StorageProfile::Local {
            StorageProfile::Nfs
        } else {
            StorageProfile::Local
        };
        assert_eq!(
            Storage::open_with_profile_control(root.path(), 2, None, None, other)
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(std::fs::read(&manifest).unwrap(), before);
        assert_eq!(
            crate::storage::detect_storage_profile(root.path()).unwrap(),
            original
        );
    }
}

#[test]
fn nfs_profile_rejects_wal_in_manifest_or_shard_without_repair() {
    for file in ["manifest.sqlite", "shards/0000.sqlite"] {
        let root = tempfile::tempdir().unwrap();
        drop(nfs(root.path()));
        let path = root.path().join(file);
        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        drop(connection);
        assert_eq!(
            Storage::open_with_profile_control(root.path(), 2, None, None, StorageProfile::Nfs)
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        let raw = Connection::open(&path).unwrap();
        assert_eq!(
            raw.pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                .unwrap(),
            "wal"
        );
    }
}

#[cfg(all(feature = "auth-scram", unix))]
#[test]
fn nfs_profile_security_activation_reopen_and_old_store_fence() {
    use crate::storage::{security_catalog::SecurityCatalogStore, security_root};
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    drop(nfs(root.path()));
    let (catalog, _, _) = crate::core::security_catalog::tests::setup();
    // Ordinary local activation cannot change the profile or create a store.
    assert!(security_root::provision(root.path(), 2, &catalog).is_err());
    assert!(!root.path().join("security.sqlite").exists());
    let id = security_root::provision_with_profile(root.path(), 2, &catalog, StorageProfile::Nfs)
        .unwrap();
    let authority = security_root::open_with_profile(root.path(), 2, StorageProfile::Nfs).unwrap();
    assert_eq!(authority.store_id(), id);
    assert_eq!(authority.revision(), 1);
    drop(authority);
    let path = root.path().join("security.sqlite");
    let before = std::fs::read(&path).unwrap();
    assert!(SecurityCatalogStore::open(&path, id).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let mut store =
        SecurityCatalogStore::open_with_journal(&path, id, JournalPolicy::NFS_PERSIST).unwrap();
    let (revision, catalog) = store.load().unwrap().into_parts();
    assert_eq!(store.replace(revision, &catalog).unwrap(), 2);
    drop(store);
    assert_eq!(
        security_root::open_with_profile(root.path(), 2, StorageProfile::Nfs)
            .unwrap()
            .revision(),
        2
    );
    let storage = Storage::open_with_profile_control(
        root.path(),
        2,
        Some(*id.as_bytes()),
        None,
        StorageProfile::Nfs,
    )
    .unwrap();
    assert_rollback(&storage.open_shard(0).unwrap());
    assert!(!root.path().join("security.sqlite-wal").exists());
    assert!(!root.path().join("security.sqlite-shm").exists());
}

#[cfg(feature = "documents")]
#[test]
fn nfs_profile_document_rows_indexes_and_pooled_metadata_reopen() {
    use crate::{
        core::OperationControl,
        document::{BsonDocument, BsonValue, DocumentCollectionOptions, DocumentNamespace},
    };
    let root = tempfile::tempdir().unwrap();
    let storage = nfs(root.path());
    let collection = storage
        .create_document_collection("app", "items", &DocumentCollectionOptions::empty())
        .unwrap();
    let row =
        BsonDocument::from_entries([("_id", BsonValue::Int32(1)), ("value", BsonValue::Int32(7))])
            .unwrap();
    storage.insert_document(collection.id(), &row).unwrap();
    let migration = storage.begin_schema_migration().unwrap();
    migration.wait_for_quiescence_blocking();
    storage
        .create_built_document_index_controlled(
            &DocumentNamespace::new("app", "items").unwrap(),
            "value",
            &BsonDocument::from_entries([("value", BsonValue::Int32(1))]).unwrap(),
            false,
            migration,
            OperationControl::new(None),
        )
        .unwrap();
    // Exercise the manifest-reader pool, not only directly opened connections.
    for _ in 0..2 {
        let catalog = storage
            .document_catalog_controlled(OperationControl::new(None))
            .unwrap();
        assert_eq!(
            catalog.collection("app", "items").unwrap().id(),
            collection.id()
        );
    }
    drop(storage);
    let reopened = nfs(root.path());
    assert!(
        reopened
            .get_document(collection.id(), &BsonValue::Int32(1))
            .unwrap()
            .is_some()
    );
    let migration = reopened.begin_schema_migration().unwrap();
    migration.wait_for_quiescence_blocking();
    reopened
        .drop_built_document_index_controlled(
            "app",
            "items",
            "value",
            migration,
            OperationControl::new(None),
        )
        .unwrap();
    for shard in 0..2 {
        assert_rollback(&reopened.open_shard(shard).unwrap());
    }
}

#[test]
fn nfs_profile_global_index_build_validate_and_reopen() {
    use crate::core::{
        CancellationToken, GlobalIndexDeclaration, GlobalIndexKeyPart, GlobalIndexKeySource,
        GlobalIndexKeyType, GlobalIndexStorageTopology, ShardKeyMetadata, ShardKeyType,
        TableDeclaration,
    };
    let root = tempfile::tempdir().unwrap();
    let mut storage = nfs(root.path());
    let mut migration = storage.begin_schema_migration().unwrap();
    migration.wait_for_quiescence_blocking();
    storage
        .apply_schema_migration(
            "CREATE TABLE items(tenant TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL)",
            &mut migration,
            None,
        )
        .unwrap();
    migration.publish_ready().unwrap();
    let logical = storage.catalog.logical().default_database().id();
    storage
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "items",
                ShardKeyMetadata::new("tenant", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    let table = storage
        .catalog
        .logical()
        .table("default", "items")
        .unwrap()
        .unwrap()
        .id();
    storage
        .open_shard(storage.shard_for_key(b"a"))
        .unwrap()
        .execute("INSERT INTO items VALUES ('a', 'value')", [])
        .unwrap();
    let index = storage
        .create_global_index(
            GlobalIndexDeclaration::new(
                table,
                "items_value",
                vec![GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::column("value").unwrap(),
                    GlobalIndexKeyType::Text,
                )],
            )
            .unwrap()
            .with_topology(GlobalIndexStorageTopology::selected_v1()),
        )
        .unwrap();
    storage
        .build_global_index(index, &CancellationToken::new())
        .unwrap();
    drop(storage);
    let mut reopened = nfs(root.path());
    reopened
        .build_global_index(index, &CancellationToken::new())
        .unwrap();
    for entry in std::fs::read_dir(root.path()).unwrap() {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        assert!(!name.ends_with("-wal") && !name.ends_with("-shm"), "{name}");
    }
}
