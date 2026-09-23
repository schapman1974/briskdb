use super::*;
use rusqlite::types::Value;
use std::path::Path;

fn document(id: i32, value: BsonValue) -> BsonDocument {
    BsonDocument::from_entries([("_id", BsonValue::Int32(id)), ("value", value)]).unwrap()
}

fn build(storage: &Storage, name: &str) -> EngineResult<DocumentIndexMetadata> {
    let migration = storage.begin_schema_migration()?;
    migration.wait_for_quiescence_blocking();
    storage.build_document_index_controlled(
        "app",
        "items",
        name,
        migration,
        OperationControl::new(None),
    )
}

fn setup(root: &Path, count: u16) -> (Storage, DocumentCollectionId) {
    let storage = Storage::open(root, count).unwrap();
    let collection = storage
        .create_document_collection("app", "items", &DocumentCollectionOptions::empty())
        .unwrap();
    for id in 0..12 {
        storage
            .insert_document(
                collection.id(),
                &document(
                    id,
                    BsonValue::Array(vec![
                        BsonValue::Int32(id),
                        BsonValue::Int32(id),
                        BsonValue::Int32(99),
                    ]),
                ),
            )
            .unwrap();
    }
    storage
        .declare_document_index(
            collection.id(),
            "value",
            &BsonDocument::from_entries([("value", BsonValue::Int32(1))]).unwrap(),
            false,
        )
        .unwrap();
    (storage, collection.id())
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

fn snapshot(root: &Path, count: u16, table: &str) -> Vec<Vec<Vec<Value>>> {
    (0..count)
        .map(|shard| {
            rows(
                &Connection::open(root.join("shards").join(format!("{shard:04}.sqlite"))).unwrap(),
                &format!("SELECT * FROM {table} ORDER BY 1, 2, 3"),
            )
        })
        .collect()
}

#[test]
fn builds_nonunique_multikey_indexes_idempotently_without_record_rewrites() {
    for count in [2, 4] {
        let temp = tempfile::tempdir().unwrap();
        let (storage, collection) = setup(temp.path(), count);
        let peer = Storage::open(temp.path(), count).unwrap();
        let records = snapshot(temp.path(), count, "briskdb_documents_v1");
        let declared = storage
            .document_catalog()
            .unwrap()
            .collection("app", "items")
            .unwrap()
            .indexes()
            .iter()
            .find(|i| i.name() == "value")
            .unwrap()
            .clone();
        let ready = build(&storage, "value").unwrap();
        assert_eq!(ready.id(), declared.id());
        assert_eq!(ready.lifecycle(), DocumentIndexLifecycle::Ready);
        let entries = snapshot(temp.path(), count, "briskdb_document_index_entries_v1");
        assert_eq!(entries.iter().map(Vec::len).sum::<usize>(), 24);
        assert_eq!(build(&storage, "value").unwrap(), ready);
        assert_eq!(
            storage
                .declare_document_index(collection, "value", ready.specification(), false)
                .unwrap(),
            ready
        );
        assert_eq!(
            snapshot(temp.path(), count, "briskdb_document_index_entries_v1"),
            entries
        );
        assert_eq!(
            snapshot(temp.path(), count, "briskdb_documents_v1"),
            records
        );
        // The same-root handle sees newly published authority immediately.
        peer.insert_document(collection, &document(30, BsonValue::Int32(99)))
            .unwrap();
        assert_eq!(
            snapshot(temp.path(), count, "briskdb_document_index_entries_v1")
                .iter()
                .map(Vec::len)
                .sum::<usize>(),
            25
        );
        drop(peer);
        drop(storage);
        drop(Storage::open(temp.path(), count).unwrap());
    }
}

#[test]
fn built_entries_follow_record_transactions_and_unindexed_changes() {
    let temp = tempfile::tempdir().unwrap();
    let (storage, collection) = setup(temp.path(), 2);
    build(&storage, "value").unwrap();
    let cancellation = CancellationToken::new();
    let (key, shard) = storage.prepare_document_id(&BsonValue::Int32(0)).unwrap();
    let mut connection = storage.open_unconfigured_shard(shard).unwrap();
    let before_records = snapshot(temp.path(), 2, "briskdb_documents_v1");
    let before_entries = snapshot(temp.path(), 2, "briskdb_document_index_entries_v1");
    for commit in [false, true] {
        let transaction = connection.transaction().unwrap();
        let record = storage
            .get_document_on_connection(&transaction, collection, shard, &key, &cancellation)
            .unwrap()
            .unwrap();
        let replacement = BsonDocument::from_entries(
            record
                .document()
                .iter()
                .map(|(key, value)| (key, value.clone()))
                .chain([("unindexed", BsonValue::Int32(1))]),
        )
        .unwrap();
        let replacement = storage.prepare_document_write(&replacement).unwrap();
        assert!(
            storage
                .replace_document_on_connection(
                    &transaction,
                    collection,
                    shard,
                    &key,
                    record.natural_order(),
                    &replacement,
                    &cancellation
                )
                .unwrap()
        );
        if commit {
            transaction.commit().unwrap();
        } else {
            transaction.rollback().unwrap();
        }
        if !commit {
            assert_eq!(
                snapshot(temp.path(), 2, "briskdb_documents_v1"),
                before_records
            );
            assert_eq!(
                snapshot(temp.path(), 2, "briskdb_document_index_entries_v1"),
                before_entries
            );
        }
    }
    let changed = snapshot(temp.path(), 2, "briskdb_document_index_entries_v1");
    assert_ne!(changed, before_entries); // checksum changes even when the key does not
    for commit in [false, true] {
        let transaction = connection.transaction().unwrap();
        assert!(
            storage
                .delete_document_on_connection(&transaction, collection, shard, &key, &cancellation)
                .unwrap()
        );
        if commit {
            transaction.commit().unwrap();
        } else {
            transaction.rollback().unwrap();
        }
        if !commit {
            assert_eq!(
                snapshot(temp.path(), 2, "briskdb_document_index_entries_v1"),
                changed
            );
        }
    }
    assert_eq!(
        snapshot(temp.path(), 2, "briskdb_document_index_entries_v1")
            .iter()
            .map(Vec::len)
            .sum::<usize>(),
        22
    );
    drop(connection);
    drop(storage);
    drop(Storage::open(temp.path(), 2).unwrap());
}

#[test]
fn compound_sparse_and_partial_membership_build_and_write_together() {
    use crate::document::{DocumentFilter, DocumentIndexRequest, normalize_index_request};
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::open(temp.path(), 2).unwrap();
    let collection = storage
        .create_document_collection("app", "items", &DocumentCollectionOptions::empty())
        .unwrap()
        .id();
    let keys = BsonDocument::from_entries([
        ("value", BsonValue::Int32(1)),
        ("absent", BsonValue::Int32(-1)),
    ])
    .unwrap();
    let partial =
        DocumentFilter::new(BsonDocument::from_entries([("value", BsonValue::Int32(7))]).unwrap());
    for index in [
        DocumentIndexRequest::new(keys.clone())
            .unwrap()
            .with_sparse(true)
            .with_name("sparse")
            .unwrap(),
        DocumentIndexRequest::new(keys)
            .unwrap()
            .with_partial_filter(partial.unwrap())
            .with_name("partial")
            .unwrap(),
    ] {
        let (spec, name, unique) = normalize_index_request(index, &mut || Ok(())).unwrap();
        storage
            .declare_document_index(collection, &name, &spec, unique)
            .unwrap();
    }
    for id in 0..3 {
        storage
            .insert_document(
                collection,
                &BsonDocument::from_entries([("_id", BsonValue::Int32(id))]).unwrap(),
            )
            .unwrap();
    }
    build(&storage, "sparse").unwrap();
    build(&storage, "partial").unwrap();
    assert!(
        snapshot(temp.path(), 2, "briskdb_document_index_entries_v1")
            .iter()
            .all(Vec::is_empty)
    );
    storage
        .insert_document(collection, &document(10, BsonValue::Int32(7)))
        .unwrap();
    storage
        .insert_document(collection, &document(11, BsonValue::Null))
        .unwrap();
    assert_eq!(
        snapshot(temp.path(), 2, "briskdb_document_index_entries_v1")
            .iter()
            .map(Vec::len)
            .sum::<usize>(),
        3
    );
    drop(storage);
    let storage = Storage::open(temp.path(), 2).unwrap();
    let migration = storage.begin_schema_migration().unwrap();
    migration.wait_for_quiescence_blocking();
    storage
        .drop_document_namespace_controlled(
            "app",
            Some("items"),
            migration,
            OperationControl::new(None),
        )
        .unwrap();
    drop(storage);
    drop(Storage::open(temp.path(), 2).unwrap());
}

#[test]
fn failed_index_write_rolls_back_the_record_and_all_prior_index_entries() {
    let temp = tempfile::tempdir().unwrap();
    let (storage, collection) = setup(temp.path(), 2);
    build(&storage, "value").unwrap();
    let prepared = storage
        .prepare_document_write(&document(40, BsonValue::Int32(7)))
        .unwrap();
    let cancellation = CancellationToken::new();
    let order = storage
        .reserve_document_natural_orders_for_engine(collection, 1, &cancellation)
        .unwrap();
    let mut connection = storage.open_unconfigured_shard(prepared.shard()).unwrap();
    let records = snapshot(temp.path(), 2, "briskdb_documents_v1");
    let entries = snapshot(temp.path(), 2, "briskdb_document_index_entries_v1");
    // A TEMP trigger injects failure into the second statement of one write;
    // it is not persisted schema and does not alter any production catalog.
    connection.execute_batch("CREATE TEMP TRIGGER fail_entry BEFORE INSERT ON briskdb_document_index_entries_v1 BEGIN SELECT RAISE(ABORT, 'injected entry failure'); END;").unwrap();
    let transaction = connection.transaction().unwrap();
    assert!(
        storage
            .insert_prepared_document_on_connection(
                &transaction,
                collection,
                order,
                prepared.shard(),
                &prepared,
                &cancellation
            )
            .is_err()
    );
    transaction.rollback().unwrap();
    assert_eq!(snapshot(temp.path(), 2, "briskdb_documents_v1"), records);
    assert_eq!(
        snapshot(temp.path(), 2, "briskdb_document_index_entries_v1"),
        entries
    );
    drop(connection);
    drop(storage);
    drop(Storage::open(temp.path(), 2).unwrap());
}

#[test]
fn unsupported_unique_opaque_and_combined_budget_fail_before_intent() {
    let temp = tempfile::tempdir().unwrap();
    let (storage, collection) = setup(temp.path(), 2);
    for (name, specification, unique) in [
        (
            "unique",
            BsonDocument::from_entries([("value", BsonValue::Int32(1))]).unwrap(),
            true,
        ),
        (
            "opaque",
            BsonDocument::from_entries([(
                "key",
                BsonValue::Document(
                    BsonDocument::from_entries([("value", BsonValue::Int32(1))]).unwrap(),
                ),
            )])
            .unwrap(),
            false,
        ),
    ] {
        storage
            .declare_document_index(collection, name, &specification, unique)
            .unwrap();
        assert_eq!(
            build(&storage, name).unwrap_err().kind(),
            EngineErrorKind::Unsupported
        );
        drop(storage.enter_schema_operation().unwrap());
    }
    let array = BsonValue::Array((0..9000).map(BsonValue::Int32).collect());
    storage
        .insert_document(collection, &document(40, array))
        .unwrap();
    build(&storage, "value").unwrap();
    storage
        .declare_document_index(
            collection,
            "second",
            &BsonDocument::from_entries([("value", BsonValue::Int32(-1))]).unwrap(),
            false,
        )
        .unwrap();
    let entries = snapshot(temp.path(), 2, "briskdb_document_index_entries_v1");
    assert_eq!(
        build(&storage, "second").unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
    drop(storage.enter_schema_operation().unwrap());
    assert_eq!(
        snapshot(temp.path(), 2, "briskdb_document_index_entries_v1"),
        entries
    );
    assert!(
        load(&Connection::open(temp.path().join("manifest.sqlite")).unwrap())
            .unwrap()
            .is_none()
    );
    drop(storage);
    drop(Storage::open(temp.path(), 2).unwrap());
}

#[test]
fn ready_coverage_corruption_is_rejected_without_repair() {
    for sql in [
        "DELETE FROM briskdb_document_index_entries_v1",
        "UPDATE briskdb_document_index_entries_v1 SET entry_checksum = zeroblob(32)",
        "UPDATE briskdb_document_index_entries_v1 SET index_id = 12345",
        "UPDATE briskdb_document_index_entries_v1 SET index_key = CAST(x'ff' || substr(index_key, 2) AS BLOB)",
        "PRAGMA ignore_check_constraints=ON; UPDATE briskdb_document_index_entries_v1 SET entry_format_version=2",
        "PRAGMA foreign_keys=OFF; DELETE FROM briskdb_documents_v1",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let (storage, _) = setup(temp.path(), 2);
        build(&storage, "value").unwrap();
        for shard in 0..2 {
            storage
                .open_unconfigured_shard(shard)
                .unwrap()
                .execute_batch(sql)
                .unwrap();
        }
        drop(storage);
        let records = snapshot(temp.path(), 2, "briskdb_documents_v1");
        let entries = snapshot(temp.path(), 2, "briskdb_document_index_entries_v1");
        assert_eq!(
            Storage::open(temp.path(), 2).err().unwrap().kind(),
            EngineErrorKind::DataCorruption,
            "{sql}"
        );
        assert_eq!(snapshot(temp.path(), 2, "briskdb_documents_v1"), records);
        assert_eq!(
            snapshot(temp.path(), 2, "briskdb_document_index_entries_v1"),
            entries
        );
    }
}

#[test]
fn index_operation_crash_child() {
    let Ok(root) = std::env::var("BRISKDB_TEST_INDEX_OPERATION_ROOT") else {
        return;
    };
    let count = std::env::var("BRISKDB_TEST_INDEX_OPERATION_SHARDS")
        .unwrap()
        .parse()
        .unwrap();
    let storage = Storage::open(root, count).unwrap();
    build(&storage, "value").unwrap();
    panic!("index operation crash boundary was not reached");
}

fn crash(root: &Path, count: u16, point: &str) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "storage::document::enabled::index_operations::tests::index_operation_crash_child",
            "--nocapture",
        ])
        .env("BRISKDB_TEST_INDEX_OPERATION_ROOT", root)
        .env("BRISKDB_TEST_INDEX_OPERATION_SHARDS", count.to_string())
        .env("BRISKDB_TEST_DOCUMENT_INDEX_OPERATION_CRASH", point)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(75),
        "{point}: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn every_build_commit_boundary_recovers_without_partial_activation() {
    for count in [2, 4] {
        let mut points = vec![
            "before-intent:0".to_owned(),
            "after-intent:0".to_owned(),
            "before-activation:0".to_owned(),
            "after-activation:0".to_owned(),
        ];
        for shard in 0..count {
            for point in [
                "before-shard",
                "after-shard",
                "before-cursor",
                "after-cursor",
            ] {
                points.push(format!("{point}:{shard}"));
            }
        }
        for point in points {
            let temp = tempfile::tempdir().unwrap();
            let (storage, _) = setup(temp.path(), count);
            drop(storage);
            let records = snapshot(temp.path(), count, "briskdb_documents_v1");
            crash(temp.path(), count, &point);
            let reopened = Storage::open(temp.path(), count).unwrap();
            let catalog = reopened.document_catalog().unwrap();
            let index = catalog
                .collection("app", "items")
                .unwrap()
                .indexes()
                .iter()
                .find(|i| i.name() == "value")
                .unwrap();
            let activated = point == "after-activation:0";
            assert_eq!(
                index.lifecycle(),
                if activated {
                    DocumentIndexLifecycle::Ready
                } else {
                    DocumentIndexLifecycle::PendingBuild
                },
                "{point}"
            );
            assert_eq!(
                snapshot(temp.path(), count, "briskdb_documents_v1"),
                records
            );
            assert_eq!(
                snapshot(temp.path(), count, "briskdb_document_index_entries_v1")
                    .iter()
                    .map(Vec::len)
                    .sum::<usize>(),
                if activated { 24 } else { 0 }
            );
            build(&reopened, "value").unwrap();
            drop(reopened);
            drop(Storage::open(temp.path(), count).unwrap());
        }
    }
}

#[test]
fn every_abort_cleanup_boundary_is_idempotent() {
    let count = 2;
    let mut points = vec![
        "cleanup-before-completion:0".to_owned(),
        "cleanup-after-completion:0".to_owned(),
    ];
    for shard in 0..count {
        for point in [
            "cleanup-before-shard",
            "cleanup-after-shard",
            "before-cursor",
            "after-cursor",
        ] {
            points.push(format!("{point}:{shard}"));
        }
    }
    for point in points {
        let temp = tempfile::tempdir().unwrap();
        let (storage, collection) = setup(temp.path(), count);
        storage
            .declare_document_index(
                collection,
                "keep",
                &BsonDocument::from_entries([("value", BsonValue::Int32(-1))]).unwrap(),
                false,
            )
            .unwrap();
        build(&storage, "keep").unwrap();
        let retained = snapshot(temp.path(), count, "briskdb_document_index_entries_v1");
        drop(storage);
        let records = snapshot(temp.path(), count, "briskdb_documents_v1");
        crash(temp.path(), count, "before-activation:0");
        crash(temp.path(), count, &point);
        let storage = Storage::open(temp.path(), count).unwrap();
        assert_eq!(
            snapshot(temp.path(), count, "briskdb_documents_v1"),
            records
        );
        assert_eq!(
            snapshot(temp.path(), count, "briskdb_document_index_entries_v1"),
            retained
        );
        build(&storage, "value").unwrap();
    }
}

#[test]
fn explicit_drop_cleanup_journal_preserves_records_and_allocator() {
    let temp = tempfile::tempdir().unwrap();
    let (storage, _) = setup(temp.path(), 2);
    let target = build(&storage, "value").unwrap();
    let records = snapshot(temp.path(), 2, "briskdb_documents_v1");
    let mut connection = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
    let allocator = rows(
        &connection,
        "SELECT * FROM briskdb_document_index_allocator",
    );
    let transaction = connection.transaction().unwrap();
    transaction
        .execute(
            "UPDATE briskdb_document_indexes SET lifecycle_state=2 WHERE index_name='value'",
            [],
        )
        .unwrap();
    transaction
        .execute(
            "INSERT INTO briskdb_document_index_operation VALUES (1, ?1, 2, zeroblob(32), 2, 0)",
            [target.id().get() as i64],
        )
        .unwrap();
    manifest::refresh_manifest_digest(&transaction).unwrap();
    manifest::current_integrity(&transaction, 2).unwrap();
    transaction.commit().unwrap();
    drop(connection);
    drop(storage);
    let storage = Storage::open(temp.path(), 2).unwrap();
    assert_eq!(
        storage
            .document_catalog()
            .unwrap()
            .collection("app", "items")
            .unwrap()
            .indexes()
            .len(),
        1
    );
    assert_eq!(snapshot(temp.path(), 2, "briskdb_documents_v1"), records);
    assert!(
        snapshot(temp.path(), 2, "briskdb_document_index_entries_v1")
            .iter()
            .all(Vec::is_empty)
    );
    let connection = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
    assert_eq!(
        rows(
            &connection,
            "SELECT * FROM briskdb_document_index_allocator"
        ),
        allocator
    );
    assert!(load(&connection).unwrap().is_none());
}
