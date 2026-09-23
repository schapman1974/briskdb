use std::path::Path;

use rusqlite::{Connection, types::Value};

use super::*;
use crate::{
    core::{EngineErrorKind, OperationControl},
    document::{BsonDocument, BsonValue, DocumentCollectionOptions, DocumentIndexLifecycle},
    storage::{Storage, attach_storage_authorizer, manifest},
};

fn shard_connection(root: &Path, shard: u16) -> Connection {
    Connection::open(root.join("shards").join(format!("{shard:04}.sqlite"))).unwrap()
}

fn manifest_connection(root: &Path) -> Connection {
    Connection::open(root.join("manifest.sqlite")).unwrap()
}

fn rows(connection: &Connection, sql: &str) -> Vec<Vec<Value>> {
    let mut statement = connection.prepare(sql).unwrap();
    let columns = statement.column_count();
    statement
        .query_map([], |row| (0..columns).map(|i| row.get(i)).collect())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn record_snapshot(root: &Path, count: u16) -> Vec<Vec<Vec<Value>>> {
    (0..count)
        .map(|shard| {
            rows(
                &shard_connection(root, shard),
                "SELECT * FROM briskdb_documents_v1 ORDER BY collection_id, id_key",
            )
        })
        .collect()
}

fn catalog_snapshot(root: &Path) -> Vec<Vec<Vec<Value>>> {
    let connection = manifest_connection(root);
    [
        "SELECT * FROM briskdb_document_databases ORDER BY database_id",
        "SELECT * FROM briskdb_document_collections ORDER BY collection_id",
        "SELECT * FROM briskdb_document_indexes ORDER BY collection_id, index_name COLLATE BINARY",
        "SELECT * FROM briskdb_document_index_identities ORDER BY index_id",
        "SELECT * FROM briskdb_document_index_allocator ORDER BY singleton",
        "SELECT * FROM briskdb_document_identities ORDER BY singleton",
    ]
    .iter()
    .map(|sql| rows(&connection, sql))
    .collect()
}

fn document(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(entries).unwrap()
}

fn populate(root: &Path, count: u16) {
    let storage = Storage::open(root, count).unwrap();
    let collection = storage
        .create_document_collection("app", "one", &DocumentCollectionOptions::empty())
        .unwrap();
    storage
        .declare_document_index(
            collection.id(),
            "pending",
            &document([("a", BsonValue::Double(-1.0))]),
            true,
        )
        .unwrap();
    // Historical opaque envelopes are deliberately not interpreted by a
    // physical-schema-only upgrade.
    storage
        .declare_document_index(
            collection.id(),
            "opaque",
            &document([(
                "key",
                BsonValue::Document(document([("a", BsonValue::Int64(1))])),
            )]),
            false,
        )
        .unwrap();
    for id in 0..3 {
        storage
            .insert_document(
                collection.id(),
                &document([
                    ("tail_first", BsonValue::Int64(id.into())),
                    ("_id", BsonValue::Int32(id)),
                    ("a", BsonValue::Double(-0.0)),
                ]),
            )
            .unwrap();
    }
}

fn downgrade(root: &Path, count: u16) {
    // Only test-owned roots are reconstructed; production has no downgrade.
    for shard in 0..count {
        shard_connection(root, shard)
            .execute_batch("DROP TABLE IF EXISTS briskdb_document_index_entries_v1")
            .unwrap();
    }
    manifest::downgrade_v18_manifest_to_v17_for_test(&manifest_connection(root), count).unwrap();
}

fn assert_ready(root: &Path, count: u16, populated: bool) {
    let connection = manifest_connection(root);
    let layout: (i64, i64, i64, i64) = connection
        .query_row(
            "SELECT entry_format_version, lifecycle_state, shard_count, next_shard
         FROM briskdb_document_index_storage",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(layout, (1, 1, count.into(), count.into()));
    manifest::current_integrity(&connection, count).unwrap();
    for shard in 0..count {
        let connection = shard_connection(root, shard);
        assert_eq!(validate_optional_schema(&connection).unwrap(), populated);
        assert_eq!(
            super::super::validate_optional_schema(&connection).unwrap(),
            populated
        );
        if populated {
            require_empty(&connection).unwrap();
        }
    }
}

#[test]
fn upgrades_v17_without_rewriting_bson_catalog_or_index_identities() {
    for count in [2, 4] {
        let temp = tempfile::tempdir().unwrap();
        populate(temp.path(), count);
        let records = record_snapshot(temp.path(), count);
        let catalog = catalog_snapshot(temp.path());
        downgrade(temp.path(), count);
        let storage = Storage::open(temp.path(), count).unwrap();
        assert_ready(temp.path(), count, true);
        assert_eq!(record_snapshot(temp.path(), count), records);
        assert_eq!(catalog_snapshot(temp.path()), catalog);
        let metadata = storage.document_catalog().unwrap();
        let collection = metadata.collection("app", "one").unwrap();
        assert_eq!(storage.document_count(collection.id()).unwrap(), 3);
        for index in collection
            .indexes()
            .iter()
            .filter(|index| !index.is_built_in())
        {
            assert_eq!(index.lifecycle(), DocumentIndexLifecycle::PendingBuild);
        }
        drop(storage);
        drop(Storage::open(temp.path(), count).unwrap());
        assert_eq!(record_snapshot(temp.path(), count), records);
        assert_eq!(catalog_snapshot(temp.path()), catalog);
    }
}

#[test]
fn index_storage_crash_child() {
    let Ok(root) = std::env::var("BRISKDB_TEST_DOCUMENT_INDEX_STORAGE_ROOT") else {
        return;
    };
    let count = std::env::var("BRISKDB_TEST_DOCUMENT_INDEX_STORAGE_SHARDS")
        .unwrap()
        .parse()
        .unwrap();
    Storage::open(root, count).unwrap();
    panic!("configured index storage crash boundary was not reached");
}

#[test]
fn every_index_storage_upgrade_commit_boundary_recovers() {
    for count in [2, 4] {
        let mut checkpoints = vec![
            "after-intent:0".to_owned(),
            "before-completion:0".to_owned(),
            "after-completion:0".to_owned(),
        ];
        for shard in 0..count {
            for point in [
                "before-shard-commit",
                "after-shard-commit",
                "before-cursor-commit",
                "after-cursor-commit",
            ] {
                checkpoints.push(format!("{point}:{shard}"));
            }
        }
        for checkpoint in checkpoints {
            let temp = tempfile::tempdir().unwrap();
            populate(temp.path(), count);
            let records = record_snapshot(temp.path(), count);
            let catalog = catalog_snapshot(temp.path());
            downgrade(temp.path(), count);
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "storage::document::index_storage::tests::index_storage_crash_child",
                    "--nocapture",
                ])
                .env("BRISKDB_TEST_DOCUMENT_INDEX_STORAGE_ROOT", temp.path())
                .env(
                    "BRISKDB_TEST_DOCUMENT_INDEX_STORAGE_SHARDS",
                    count.to_string(),
                )
                .env("BRISKDB_TEST_DOCUMENT_INDEX_STORAGE_CRASH", &checkpoint)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(74),
                "{count} shards, {checkpoint}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            drop(Storage::open(temp.path(), count).unwrap());
            assert_ready(temp.path(), count, true);
            assert_eq!(record_snapshot(temp.path(), count), records, "{checkpoint}");
            assert_eq!(catalog_snapshot(temp.path()), catalog, "{checkpoint}");
        }
    }
}

#[test]
fn fresh_and_upgraded_empty_roots_provision_and_remove_index_storage_with_namespaces() {
    for upgrade in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        drop(Storage::open(temp.path(), 2).unwrap());
        if upgrade {
            downgrade(temp.path(), 2);
        }
        let storage = Storage::open(temp.path(), 2).unwrap();
        assert_ready(temp.path(), 2, false);
        let one = storage
            .create_document_collection("app", "one", &DocumentCollectionOptions::empty())
            .unwrap();
        assert_ready(temp.path(), 2, true);
        let two = storage
            .create_document_collection("app", "two", &DocumentCollectionOptions::empty())
            .unwrap();
        storage
            .insert_document(two.id(), &document([("_id", BsonValue::Int32(5))]))
            .unwrap();
        for name in [one.name(), two.name()] {
            let migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            assert!(
                storage
                    .drop_document_namespace_controlled(
                        "app",
                        Some(name),
                        migration,
                        OperationControl::new(None)
                    )
                    .unwrap()
            );
            assert_ready(temp.path(), 2, name == one.name());
            if name == one.name() {
                assert_eq!(storage.document_count(two.id()).unwrap(), 1);
            }
        }
        drop(storage);
        drop(Storage::open(temp.path(), 2).unwrap());
        assert_ready(temp.path(), 2, false);
    }
}

#[test]
fn ready_roots_reject_missing_or_malformed_entry_schema_without_repair() {
    for mutation in [
        "DROP TABLE briskdb_document_index_entries_v1",
        "DROP INDEX briskdb_document_index_entries_by_record_v1",
        "DROP INDEX briskdb_document_index_entries_by_record_v1;
         CREATE INDEX briskdb_document_index_entries_by_record_v1 ON briskdb_document_index_entries_v1 (index_id, id_key)",
        "DROP TABLE briskdb_document_index_entries_v1;
         CREATE TABLE briskdb_document_index_entries_v1 (collection_id INTEGER)",
    ] {
        let temp = tempfile::tempdir().unwrap();
        populate(temp.path(), 2);
        let connection = shard_connection(temp.path(), 0);
        connection.execute_batch(mutation).unwrap();
        let schema = rows(&connection, "SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY name");
        let records = record_snapshot(temp.path(), 2);
        assert_eq!(Storage::open(temp.path(), 2).unwrap_err().kind(), EngineErrorKind::DataCorruption);
        assert_eq!(rows(&connection, "SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY name"), schema);
        assert_eq!(record_snapshot(temp.path(), 2), records);
    }
}

#[test]
fn orphan_index_schema_is_rejected() {
    let connection = Connection::open_in_memory().unwrap();
    connection.execute_batch(ENTRIES_SQL).unwrap();
    connection.execute_batch(BY_RECORD_SQL).unwrap();
    assert_eq!(
        super::super::validate_optional_schema(&connection)
            .unwrap_err()
            .kind(),
        EngineErrorKind::DataCorruption
    );
}

#[test]
fn v17_deletion_journals_recover_before_index_layout_upgrade() {
    for keep_other in [false, true] {
        for checkpoint in ["after-intent:0", "after-progress:1"] {
            let temp = tempfile::tempdir().unwrap();
            populate(temp.path(), 4);
            if keep_other {
                let storage = Storage::open(temp.path(), 4).unwrap();
                let other = storage
                    .create_document_collection(
                        "other",
                        "keep",
                        &DocumentCollectionOptions::empty(),
                    )
                    .unwrap();
                storage
                    .insert_document(other.id(), &document([("_id", BsonValue::Int32(42))]))
                    .unwrap();
            }
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "storage::document::enabled::tests::document_drop_crash_child",
                    "--nocapture",
                ])
                .env("BRISKDB_TEST_DOCUMENT_DROP_ROOT", temp.path())
                .env("BRISKDB_TEST_DOCUMENT_DROP_MODE", "collection")
                .env("BRISKDB_TEST_DOCUMENT_DROP_CRASH", checkpoint)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(73),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            downgrade(temp.path(), 4);
            let storage = Storage::open(temp.path(), 4).unwrap();
            assert_ready(temp.path(), 4, keep_other);
            let catalog = storage.document_catalog().unwrap();
            assert!(catalog.collection("app", "one").is_none());
            assert_eq!(catalog.collection("other", "keep").is_some(), keep_other);
            if keep_other {
                assert_eq!(
                    storage
                        .document_count(catalog.collection("other", "keep").unwrap().id())
                        .unwrap(),
                    1
                );
            }
        }
    }
}

fn insert_unowned_entry(connection: &Connection) {
    connection
        .execute_batch(
            "INSERT INTO briskdb_document_index_entries_v1
        SELECT collection_id, 999, id_key, zeroblob(13), zeroblob(32), 1
        FROM briskdb_documents_v1 LIMIT 1",
        )
        .unwrap();
}

#[test]
fn unowned_entries_fail_closed_in_ready_and_resuming_roots() {
    for upgrading in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        populate(temp.path(), 2);
        if upgrading {
            let connection = manifest_connection(temp.path());
            connection
                .execute_batch(
                    "UPDATE briskdb_document_index_storage SET lifecycle_state = 2, next_shard = 0",
                )
                .unwrap();
            manifest::refresh_manifest_digest(&connection).unwrap();
        }
        let connection = (0..2)
            .map(|shard| shard_connection(temp.path(), shard))
            .find(|connection| {
                connection
                    .query_row(
                        "SELECT EXISTS (SELECT 1 FROM briskdb_documents_v1)",
                        [],
                        |r| r.get::<_, bool>(0),
                    )
                    .unwrap()
            })
            .unwrap();
        insert_unowned_entry(&connection);
        let before = rows(
            &connection,
            "SELECT * FROM briskdb_document_index_entries_v1",
        );
        assert_eq!(before.len(), 1);
        assert_eq!(
            Storage::open(temp.path(), 2).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
        assert_eq!(
            rows(
                &connection,
                "SELECT * FROM briskdb_document_index_entries_v1"
            ),
            before
        );
    }
}

#[test]
fn physical_entry_keys_are_record_owned_and_hidden_from_sql_clients() {
    let temp = tempfile::tempdir().unwrap();
    populate(temp.path(), 2);
    let connection = (0..2)
        .map(|shard| shard_connection(temp.path(), shard))
        .find(|connection| {
            connection
                .query_row(
                    "SELECT EXISTS (SELECT 1 FROM briskdb_documents_v1)",
                    [],
                    |r| r.get::<_, bool>(0),
                )
                .unwrap()
        })
        .unwrap();
    connection
        .execute_batch("PRAGMA foreign_keys = ON")
        .unwrap();
    insert_unowned_entry(&connection);
    assert!(
        connection
            .execute_batch(
                "INSERT INTO briskdb_document_index_entries_v1 VALUES
        (999, 999, zeroblob(9), zeroblob(13), zeroblob(32), 1)"
            )
            .is_err()
    );
    connection
        .execute_batch("DELETE FROM briskdb_documents_v1")
        .unwrap();
    require_empty(&connection).unwrap();
    attach_storage_authorizer(&connection).unwrap();
    for sql in [
        "SELECT * FROM briskdb_document_index_entries_v1",
        "DELETE FROM briskdb_document_index_entries_v1",
        "UPDATE briskdb_document_index_entries_v1 SET index_id = 8",
        "INSERT INTO briskdb_document_index_entries_v1 VALUES (1, 1, zeroblob(9), zeroblob(13), zeroblob(32), 1)",
        "DROP TABLE briskdb_document_index_entries_v1",
    ] {
        assert!(connection.execute_batch(sql).is_err(), "{sql}");
    }
}
