use super::*;
use crate::document::{DocumentFilter, DocumentIndexRequest, normalize_index_request};

fn doc(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(entries).unwrap()
}

fn setup(shards: u16) -> (tempfile::TempDir, Storage, DocumentCollectionId) {
    let root = tempfile::tempdir().unwrap();
    let storage = Storage::open(root.path(), shards).unwrap();
    let collection = storage
        .create_document_collection("app", "items", &DocumentCollectionOptions::empty())
        .unwrap()
        .id();
    (root, storage, collection)
}

fn build(
    storage: &Storage,
    collection: DocumentCollectionId,
    index: DocumentIndexRequest,
) -> EngineResult<DocumentIndexMetadata> {
    let (spec, name, unique) = normalize_index_request(index.with_unique(true), &mut || Ok(()))?;
    storage.declare_document_index(collection, &name, &spec, unique)?;
    let migration = storage.begin_schema_migration()?;
    migration.wait_for_quiescence_blocking();
    storage.build_document_index_controlled(
        "app",
        "items",
        &name,
        migration,
        OperationControl::new(None),
    )
}

fn index() -> DocumentIndexRequest {
    DocumentIndexRequest::new(doc([("value", BsonValue::Int32(1))]))
        .unwrap()
        .with_name("value_unique")
        .unwrap()
}

fn ids_on_different_shards(storage: &Storage) -> (i32, i32) {
    let first = storage.prepare_document_id(&BsonValue::Int32(0)).unwrap().1;
    let second = (1..100)
        .find(|id| {
            storage
                .prepare_document_id(&BsonValue::Int32(*id))
                .unwrap()
                .1
                != first
        })
        .unwrap();
    (0, second)
}

fn row(id: i32, value: Option<BsonValue>) -> BsonDocument {
    let mut fields = vec![("_id", BsonValue::Int32(id))];
    if let Some(value) = value {
        fields.push(("value", value));
    }
    doc(fields)
}

#[test]
fn cross_shard_unique_keys_are_typed_multikey_and_persistent() {
    for shards in [2, 4] {
        for (left, right, conflicts) in [
            (Some(BsonValue::Int32(1)), Some(BsonValue::Int64(1)), true),
            (
                Some(BsonValue::Int64(1)),
                Some(BsonValue::Double(1.0)),
                true,
            ),
            (
                Some(BsonValue::Int32(1)),
                Some(BsonValue::Boolean(true)),
                false,
            ),
            (
                Some(BsonValue::Int32(1)),
                Some(BsonValue::String("1".into())),
                false,
            ),
            (None, Some(BsonValue::Null), true),
            (
                Some(BsonValue::Array(vec![
                    BsonValue::Int32(1),
                    BsonValue::Int32(1),
                    BsonValue::Int32(2),
                ])),
                Some(BsonValue::Int64(2)),
                true,
            ),
            (
                Some(BsonValue::Array(vec![])),
                Some(BsonValue::Array(vec![])),
                true,
            ),
            (Some(BsonValue::Array(vec![])), None, false),
        ] {
            let (root, storage, collection) = setup(shards);
            let (first, second) = ids_on_different_shards(&storage);
            let metadata = build(&storage, collection, index()).unwrap();
            assert_eq!(metadata.lifecycle(), DocumentIndexLifecycle::Ready);
            assert!(metadata.is_unique());
            storage
                .insert_document(collection, &row(first, left.clone()))
                .unwrap();
            let result = storage.insert_document(collection, &row(second, right.clone()));
            if conflicts {
                assert_eq!(result.unwrap_err().kind(), EngineErrorKind::UniqueViolation);
            } else {
                result.unwrap();
            }
            drop(storage);
            let storage = Storage::open(root.path(), shards).unwrap();
            assert!(
                storage
                    .get_document(collection, &BsonValue::Int32(first))
                    .unwrap()
                    .unwrap()
                    .representation_eq(&row(first, left))
            );
            assert_eq!(
                storage
                    .get_document(collection, &BsonValue::Int32(second))
                    .unwrap()
                    .is_some(),
                !conflicts
            );
            let retry = storage.insert_document(collection, &row(1000, right));
            assert_eq!(retry.unwrap_err().kind(), EngineErrorKind::UniqueViolation);
        }
    }
}

#[test]
fn unique_build_rejects_existing_cross_shard_duplicates_without_activation() {
    let (root, storage, collection) = setup(4);
    let (first, second) = ids_on_different_shards(&storage);
    storage
        .insert_document(collection, &row(first, Some(BsonValue::Int32(7))))
        .unwrap();
    storage
        .insert_document(collection, &row(second, Some(BsonValue::Double(7.0))))
        .unwrap();
    assert_eq!(
        build(&storage, collection, index()).unwrap_err().kind(),
        EngineErrorKind::UniqueViolation
    );
    assert_eq!(
        storage.document_catalog().unwrap().collections()[0]
            .indexes()
            .iter()
            .find(|index| !index.is_built_in())
            .unwrap()
            .lifecycle(),
        DocumentIndexLifecycle::PendingBuild
    );
    drop(storage.enter_schema_operation().unwrap());
    storage
        .insert_document(collection, &row(1000, Some(BsonValue::Int32(7))))
        .unwrap();
    drop(storage);
    drop(Storage::open(root.path(), 4).unwrap());
}

#[test]
fn sparse_partial_and_compound_membership_preserve_unique_rules() {
    for mode in ["sparse", "partial", "compound"] {
        let (root, storage, collection) = setup(4);
        let (first, second) = ids_on_different_shards(&storage);
        let definition = match mode {
            "sparse" => index().with_sparse(true),
            "partial" => index().with_partial_filter(
                DocumentFilter::new(doc([("enabled", BsonValue::Boolean(true))])).unwrap(),
            ),
            _ => DocumentIndexRequest::new(doc([
                ("tenant", BsonValue::Int32(1)),
                ("value", BsonValue::Int32(-1)),
            ]))
            .unwrap()
            .with_name("compound_unique")
            .unwrap(),
        };
        build(&storage, collection, definition).unwrap();
        for id in [first, second] {
            let row = match mode {
                "sparse" => row(id, None),
                "partial" => doc([
                    ("_id", BsonValue::Int32(id)),
                    ("value", BsonValue::Int32(7)),
                    ("enabled", BsonValue::Boolean(false)),
                ]),
                _ => doc([
                    ("_id", BsonValue::Int32(id)),
                    ("value", BsonValue::Int32(7)),
                    ("tenant", BsonValue::Int32(id)),
                ]),
            };
            storage.insert_document(collection, &row).unwrap();
        }
        let constrained = |id| match mode {
            "sparse" => row(id, Some(BsonValue::Null)),
            "partial" => doc([
                ("_id", BsonValue::Int32(id)),
                ("value", BsonValue::Int32(7)),
                ("enabled", BsonValue::Boolean(true)),
            ]),
            _ => doc([
                ("_id", BsonValue::Int32(id)),
                ("value", BsonValue::Int32(7)),
                ("tenant", BsonValue::Int32(-1)),
            ]),
        };
        storage
            .insert_document(collection, &constrained(1000))
            .unwrap();
        assert_eq!(
            storage
                .insert_document(collection, &constrained(1001))
                .unwrap_err()
                .kind(),
            EngineErrorKind::UniqueViolation
        );
        drop(storage);
        drop(Storage::open(root.path(), 4).unwrap());
    }
}

#[test]
fn concurrent_cross_shard_writers_choose_exactly_one_unique_owner() {
    let (root, storage, collection) = setup(4);
    build(&storage, collection, index()).unwrap();
    let start = Arc::new(std::sync::Barrier::new(16));
    let workers: Vec<_> = (0..16)
        .map(|id| {
            let storage = storage.clone();
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                storage
                    .insert_document(collection, &row(id, Some(BsonValue::Int32(42))))
                    .map_err(|error| error.kind())
            })
        })
        .collect();
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(
        results.iter().filter(|result| result.is_ok()).count(),
        1,
        "{results:?}"
    );
    assert!(
        results
            .iter()
            .all(|result| result.is_ok() || *result == Err(EngineErrorKind::UniqueViolation)),
        "{results:?}"
    );
    drop(storage);
    drop(Storage::open(root.path(), 4).unwrap());
}

#[test]
fn replacement_conflicts_roll_back_and_deletion_and_drop_release_unique_keys() {
    let (root, storage, collection) = setup(4);
    let (first, second) = ids_on_different_shards(&storage);
    build(&storage, collection, index()).unwrap();
    for (id, value) in [(first, 1), (second, 2)] {
        storage
            .insert_document(collection, &row(id, Some(BsonValue::Int32(value))))
            .unwrap();
    }
    let cancellation = CancellationToken::new();
    let (key, shard) = storage
        .prepare_document_id(&BsonValue::Int32(second))
        .unwrap();
    let connection = storage.open_unconfigured_shard(shard).unwrap();
    let replacement = storage
        .prepare_document_write(&row(second, Some(BsonValue::Int64(1))))
        .unwrap();
    let transaction = storage
        .begin_document_write(&connection, collection, shard, &cancellation, None)
        .unwrap();
    let record = storage
        .get_document_on_connection(&transaction, collection, shard, &key, &cancellation)
        .unwrap()
        .unwrap();
    assert_eq!(
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
            .unwrap_err()
            .kind(),
        EngineErrorKind::UniqueViolation
    );
    transaction.rollback().unwrap();
    assert!(
        storage
            .get_document(collection, &BsonValue::Int32(second))
            .unwrap()
            .unwrap()
            .representation_eq(&row(second, Some(BsonValue::Int32(2))))
    );
    let (first_key, first_shard) = storage
        .prepare_document_id(&BsonValue::Int32(first))
        .unwrap();
    let first_connection = storage.open_unconfigured_shard(first_shard).unwrap();
    let transaction = storage
        .begin_document_write(
            &first_connection,
            collection,
            first_shard,
            &cancellation,
            None,
        )
        .unwrap();
    assert!(
        storage
            .delete_document_on_connection(
                &transaction,
                collection,
                first_shard,
                &first_key,
                &cancellation
            )
            .unwrap()
    );
    transaction.commit().unwrap();
    for _ in 0..2 {
        let transaction = storage
            .begin_document_write(&connection, collection, shard, &cancellation, None)
            .unwrap();
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
        transaction.commit().unwrap();
    }
    drop(first_connection);
    drop(connection);
    drop(storage);
    let storage = Storage::open(root.path(), 4).unwrap();
    assert_eq!(
        storage
            .insert_document(collection, &row(first, Some(BsonValue::Double(1.0))))
            .unwrap_err()
            .kind(),
        EngineErrorKind::UniqueViolation
    );
    let migration = storage.begin_schema_migration().unwrap();
    migration.wait_for_quiescence_blocking();
    storage
        .drop_built_document_index_controlled(
            "app",
            "items",
            "value_unique",
            migration,
            OperationControl::new(None),
        )
        .unwrap();
    storage
        .insert_document(collection, &row(first, Some(BsonValue::Double(1.0))))
        .unwrap();
    drop(storage);
    drop(Storage::open(root.path(), 4).unwrap());
}

#[test]
fn damaged_foreign_unique_entries_are_corruption_not_duplicate_errors() {
    let (root, storage, collection) = setup(2);
    let (first, second) = ids_on_different_shards(&storage);
    build(&storage, collection, index()).unwrap();
    let shard = storage
        .insert_document(collection, &row(first, Some(BsonValue::Int32(7))))
        .unwrap();
    let connection = storage.open_unconfigured_shard(shard).unwrap();
    connection
        .execute(
            "UPDATE briskdb_document_index_entries_v1 SET entry_checksum = zeroblob(32)",
            [],
        )
        .unwrap();
    assert_eq!(
        storage
            .insert_document(collection, &row(second, Some(BsonValue::Int32(7))))
            .unwrap_err()
            .kind(),
        EngineErrorKind::DataCorruption
    );
    assert!(storage.enter_schema_operation().is_err());
    // Inspect the private fixture directly: the rejected post-image never
    // committed. Genuine corruption must remain persistently degraded.
    let contender_shard = storage
        .prepare_document_id(&BsonValue::Int32(second))
        .unwrap()
        .1;
    let contender = storage.open_unconfigured_shard(contender_shard).unwrap();
    let count: i64 = contender
        .query_row("SELECT count(*) FROM briskdb_documents_v1", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 0);
    drop(contender);
    drop(connection);
    drop(storage);
    assert_eq!(
        Storage::open(root.path(), 2).unwrap_err().kind(),
        EngineErrorKind::DataCorruption
    );
}

#[test]
fn child_read_cancellation_preserves_precise_corruption_and_root_health() {
    use crate::core::CancellationReason;
    let (_root, storage, _collection) = setup(2);
    let control = OperationControl::new(None);
    control.request_cancel(CancellationReason::DeadlineExceeded);
    let cancellation = CancellationToken::new();
    assert_eq!(
        open_peer(
            &storage,
            1,
            Some(Arc::clone(&control)),
            cancellation.clone()
        )
        .unwrap_err()
        .kind(),
        EngineErrorKind::DeadlineExceeded
    );
    for (code, expected) in [
        (
            rusqlite::ffi::SQLITE_ERROR,
            EngineErrorKind::DeadlineExceeded,
        ),
        (
            rusqlite::ffi::SQLITE_CORRUPT,
            EngineErrorKind::DataCorruption,
        ),
    ] {
        let failure = rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None);
        let error = shard_read_error(failure, "injected validation failure");
        assert_eq!(
            normalize::<()>(Err(error), Some(&control), &cancellation)
                .unwrap_err()
                .kind(),
            expected
        );
    }
    assert_eq!(
        normalize::<()>(
            Err(corrupt("semantic damage")),
            Some(&control),
            &cancellation
        )
        .unwrap_err()
        .kind(),
        EngineErrorKind::DataCorruption
    );
    drop(storage.enter_schema_operation().unwrap());
}

#[test]
fn startup_rejects_cross_shard_duplicate_owners_even_with_valid_entries() {
    let (root, storage, collection) = setup(2);
    let (first, second) = ids_on_different_shards(&storage);
    for id in [first, second] {
        storage
            .insert_document(collection, &row(id, Some(BsonValue::Int32(7))))
            .unwrap();
    }
    let (spec, name, unique) = normalize_index_request(index(), &mut || Ok(())).unwrap();
    storage
        .declare_document_index(collection, &name, &spec, unique)
        .unwrap();
    let migration = storage.begin_schema_migration().unwrap();
    migration.wait_for_quiescence_blocking();
    storage
        .build_document_index_controlled(
            "app",
            "items",
            &name,
            migration,
            OperationControl::new(None),
        )
        .unwrap();
    drop(storage);
    // Forge only the authority flag in this private fixture. The records,
    // canonical keys and entry checksums are otherwise valid, so per-shard
    // coverage alone cannot detect the duplicate ownership.
    let connection = Connection::open(root.path().join("manifest.sqlite")).unwrap();
    connection
        .execute(
            "UPDATE briskdb_document_indexes SET is_unique = 1 WHERE index_name = ?1",
            [name],
        )
        .unwrap();
    manifest::refresh_manifest_digest(&connection).unwrap();
    drop(connection);
    assert_eq!(
        Storage::open(root.path(), 2).unwrap_err().kind(),
        EngineErrorKind::DataCorruption
    );
}

#[test]
fn startup_waits_for_the_unique_writer_fence_before_scanning() {
    let (root, storage, collection) = setup(2);
    build(&storage, collection, index()).unwrap();
    let fence = super::super::write_transaction::acquire_fence(
        &storage,
        collection,
        &CancellationToken::new(),
        None,
    )
    .unwrap();
    let path = root.path().to_owned();
    let (started, started_rx) = std::sync::mpsc::channel();
    let (finished, finished_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        started.send(()).unwrap();
        finished.send(Storage::open(path, 2)).unwrap();
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert!(matches!(
        finished_rx.recv_timeout(std::time::Duration::from_millis(100)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    drop(fence);
    let reopened = finished_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .unwrap()
        .unwrap();
    worker.join().unwrap();
    reopened
        .insert_document(collection, &row(1, Some(BsonValue::Int32(7))))
        .unwrap();
    assert_eq!(
        storage
            .insert_document(collection, &row(2, Some(BsonValue::Int32(7))))
            .unwrap_err()
            .kind(),
        EngineErrorKind::UniqueViolation
    );
}

#[test]
fn unique_process_child() {
    let Ok(root) = std::env::var("BRISKDB_TEST_UNIQUE_PROCESS_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let storage = Storage::open(&root, 4).unwrap();
    let collection = storage.document_catalog().unwrap().collections()[0].id();
    let id: i32 = std::env::var("BRISKDB_TEST_UNIQUE_PROCESS_ID")
        .unwrap()
        .parse()
        .unwrap();
    let mode = std::env::var("BRISKDB_TEST_UNIQUE_PROCESS_MODE").unwrap();
    if mode == "race" {
        std::fs::write(root.join(format!("ready-{id}")), b"ready").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !root.join("go").exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "parent did not release worker"
            );
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let result = storage.insert_document(collection, &row(id, Some(BsonValue::Int32(42))));
        let outcome = match result {
            Ok(_) => "ok",
            Err(error) if error.kind() == EngineErrorKind::UniqueViolation => "duplicate",
            Err(error) => panic!("{error:?}"),
        };
        std::fs::write(root.join(format!("result-{id}")), outcome).unwrap();
    } else {
        let cancellation = CancellationToken::new();
        let prepared = storage
            .prepare_document_write(&row(id, Some(BsonValue::Int32(42))))
            .unwrap();
        let order = storage
            .reserve_document_natural_orders_for_engine(collection, 1, &cancellation)
            .unwrap();
        let connection = storage.open_unconfigured_shard(prepared.shard()).unwrap();
        let transaction = storage
            .begin_document_write(
                &connection,
                collection,
                prepared.shard(),
                &cancellation,
                None,
            )
            .unwrap();
        storage
            .insert_prepared_document_on_connection(
                &transaction,
                collection,
                order,
                prepared.shard(),
                &prepared,
                &cancellation,
            )
            .unwrap();
        if mode == "committed" {
            transaction.commit().unwrap();
        }
        std::process::exit(73);
    }
}

fn child(root: &std::path::Path, id: i32, mode: &str) -> std::process::Child {
    std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("storage::document::enabled::unique::tests::unique_process_child")
        .env("BRISKDB_TEST_UNIQUE_PROCESS_ROOT", root)
        .env("BRISKDB_TEST_UNIQUE_PROCESS_ID", id.to_string())
        .env("BRISKDB_TEST_UNIQUE_PROCESS_MODE", mode)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap()
}

#[test]
fn independent_processes_cannot_commit_the_same_unique_key() {
    let (root, storage, collection) = setup(4);
    let (first, second) = ids_on_different_shards(&storage);
    build(&storage, collection, index()).unwrap();
    drop(storage);
    let workers = [
        child(root.path(), first, "race"),
        child(root.path(), second, "race"),
    ];
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while ![first, second]
        .iter()
        .all(|id| root.path().join(format!("ready-{id}")).exists())
    {
        assert!(
            std::time::Instant::now() < deadline,
            "unique workers did not start"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    std::fs::write(root.path().join("go"), b"go").unwrap();
    for worker in workers {
        let output = worker.wait_with_output().unwrap();
        assert!(output.status.success(), "{output:?}");
    }
    let mut outcomes: Vec<_> = [first, second]
        .into_iter()
        .map(|id| std::fs::read_to_string(root.path().join(format!("result-{id}"))).unwrap())
        .collect();
    outcomes.sort();
    assert_eq!(outcomes, ["duplicate", "ok"]);
    let storage = Storage::open(root.path(), 4).unwrap();
    assert_eq!(
        [first, second]
            .into_iter()
            .filter(|id| storage
                .get_document(collection, &BsonValue::Int32(*id))
                .unwrap()
                .is_some())
            .count(),
        1
    );
}

#[test]
fn unique_record_and_entry_crashes_recover_at_the_same_commit_boundary() {
    for committed in [false, true] {
        let (root, storage, collection) = setup(4);
        let (first, second) = ids_on_different_shards(&storage);
        build(&storage, collection, index()).unwrap();
        drop(storage);
        let output = child(
            root.path(),
            first,
            if committed {
                "committed"
            } else {
                "uncommitted"
            },
        )
        .wait_with_output()
        .unwrap();
        assert_eq!(output.status.code(), Some(73), "{output:?}");
        let storage = Storage::open(root.path(), 4).unwrap();
        assert_eq!(
            storage
                .get_document(collection, &BsonValue::Int32(first))
                .unwrap()
                .is_some(),
            committed
        );
        let result = storage.insert_document(collection, &row(second, Some(BsonValue::Int32(42))));
        if committed {
            assert_eq!(result.unwrap_err().kind(), EngineErrorKind::UniqueViolation);
        } else {
            result.unwrap();
        }
        drop(storage);
        drop(Storage::open(root.path(), 4).unwrap());
    }
}
