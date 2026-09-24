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

fn drop_built(storage: &Storage, name: &str) -> EngineResult<()> {
    let migration = storage.begin_schema_migration()?;
    migration.wait_for_quiescence_blocking();
    storage.drop_built_document_index_controlled(
        "app",
        "items",
        name,
        migration,
        OperationControl::new(None),
    )
}

fn create_built(storage: &Storage, name: &str, field: &str) -> EngineResult<(u64, u64)> {
    let migration = storage.begin_schema_migration()?;
    migration.wait_for_quiescence_blocking();
    storage.create_built_document_index_controlled(
        &DocumentNamespace::new("app", "items").unwrap(),
        name,
        &BsonDocument::from_entries([(field, BsonValue::Int32(1))]).unwrap(),
        false,
        migration,
        OperationControl::new(None),
    )
}

fn high_water(root: &Path) -> i64 {
    Connection::open(root.join("manifest.sqlite"))
        .unwrap()
        .query_row(
            "SELECT index_high_water FROM briskdb_document_index_allocator",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

fn drop_selected(storage: &Storage, name: Option<&str>) -> EngineResult<(u64, u64)> {
    let migration = storage.begin_schema_migration()?;
    migration.wait_for_quiescence_blocking();
    storage.drop_document_indexes_controlled(
        &DocumentNamespace::new("app", "items").unwrap(),
        name,
        migration,
        OperationControl::new(None),
    )
}

#[test]
fn every_drop_batch_boundary_preserves_completed_prefix_and_unstarted_indexes() {
    for count in [2, 4] {
        let mut points = vec![
            "drop-before-intent:0".to_owned(),
            "drop-after-intent:0".to_owned(),
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
            let target = build(&storage, "value").unwrap();
            create_built(&storage, "a", "a").unwrap();
            create_built(&storage, "z", "z").unwrap();
            storage
                .declare_document_index(
                    collection,
                    "!pending",
                    &BsonDocument::from_entries([("pending", BsonValue::Int32(1))]).unwrap(),
                    true,
                )
                .unwrap();
            let catalog = storage.document_catalog().unwrap();
            let first = catalog
                .collection("app", "items")
                .unwrap()
                .indexes()
                .iter()
                .find(|i| i.name() == "a")
                .unwrap()
                .id();
            let high = high_water(temp.path());
            let records = snapshot(temp.path(), count, "briskdb_documents_v1");
            let mut expected_entries =
                snapshot(temp.path(), count, "briskdb_document_index_entries_v1");
            let admitted = point != "drop-before-intent:0";
            for shard in &mut expected_entries {
                shard.retain(|row| {
                    row[1] != Value::Integer(first.get() as i64)
                        && (!admitted || row[1] != Value::Integer(target.id().get() as i64))
                });
            }
            drop(storage);
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "storage::document::enabled::index_operations::tests::index_operation_crash_child", "--nocapture"])
                .env("BRISKDB_TEST_INDEX_OPERATION_ROOT", temp.path())
                .env("BRISKDB_TEST_INDEX_OPERATION_SHARDS", count.to_string())
                .env("BRISKDB_TEST_INDEX_OPERATION_DROP_BATCH", "1")
                .env("BRISKDB_TEST_DOCUMENT_INDEX_OPERATION_ID", target.id().get().to_string())
                .env("BRISKDB_TEST_DOCUMENT_INDEX_OPERATION_CRASH", &point)
                .output().unwrap();
            assert_eq!(
                output.status.code(),
                Some(75),
                "{point}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let storage = Storage::open(temp.path(), count).unwrap();
            assert_eq!(high_water(temp.path()), high);
            assert_eq!(
                snapshot(temp.path(), count, "briskdb_documents_v1"),
                records
            );
            assert_eq!(
                snapshot(temp.path(), count, "briskdb_document_index_entries_v1"),
                expected_entries,
                "{point}"
            );
            let catalog = storage.document_catalog().unwrap();
            let indexes = catalog.collection("app", "items").unwrap().indexes();
            assert!(!indexes.iter().any(|i| matches!(i.name(), "a" | "!pending")));
            assert!(
                indexes
                    .iter()
                    .any(|i| i.name() == "z" && i.lifecycle() == DocumentIndexLifecycle::Ready)
            );
            assert_eq!(indexes.iter().any(|i| i.name() == "value"), !admitted);
            assert_eq!(
                drop_selected(&storage, None).unwrap(),
                (if admitted { 2 } else { 3 }, 1)
            );
            assert!(
                snapshot(temp.path(), count, "briskdb_document_index_entries_v1")
                    .iter()
                    .all(Vec::is_empty)
            );
            assert_eq!(high_water(temp.path()), high);
        }
    }
}

fn create_batch(
    storage: &Storage,
    indexes: Vec<crate::document::DocumentIndexRequest>,
) -> EngineResult<(u64, u64)> {
    let definitions =
        crate::document::normalize_index_batch(indexes.into_boxed_slice(), &mut || Ok(()))?;
    let migration = storage.begin_schema_migration()?;
    migration.wait_for_quiescence_blocking();
    storage.create_document_indexes_controlled(
        &DocumentNamespace::new("app", "items").unwrap(),
        definitions,
        migration,
        OperationControl::new(None),
    )
}

fn batch_index(field: &str, name: &str) -> crate::document::DocumentIndexRequest {
    crate::document::DocumentIndexRequest::new(
        BsonDocument::from_entries([(field, BsonValue::Int32(1))]).unwrap(),
    )
    .unwrap()
    .with_name(name)
    .unwrap()
}

#[test]
fn strict_batch_matches_legacy_envelope_without_rewriting_identity_or_definition() {
    let temp = tempfile::tempdir().unwrap();
    let (storage, collection) = setup(temp.path(), 2);
    let specification = BsonDocument::from_entries([
        ("v", BsonValue::Int32(2)),
        ("name", BsonValue::from("legacy")),
        (
            "key",
            BsonValue::Document(
                BsonDocument::from_entries([("other", BsonValue::Int64(1))]).unwrap(),
            ),
        ),
        ("unique", BsonValue::Boolean(false)),
        ("sparse", BsonValue::Boolean(false)),
        ("partialFilterExpression", BsonValue::Null),
    ])
    .unwrap();
    let original = storage
        .declare_document_index(collection, "legacy", &specification, false)
        .unwrap();
    assert_eq!(
        create_batch(&storage, vec![batch_index("other", "legacy")]).unwrap(),
        (1, 2)
    );
    // Matching name wins even if a permissive legacy API declared a duplicate.
    storage
        .declare_document_index(
            collection,
            "duplicate",
            &BsonDocument::from_entries([("other", BsonValue::Int32(1))]).unwrap(),
            false,
        )
        .unwrap();
    assert_eq!(
        create_batch(&storage, vec![batch_index("other", "legacy")]).unwrap(),
        (2, 2)
    );
    let catalog = storage.document_catalog().unwrap();
    let actual = catalog
        .collection("app", "items")
        .unwrap()
        .indexes()
        .iter()
        .find(|i| i.name() == "legacy")
        .unwrap();
    assert_eq!(actual.id(), original.id());
    assert!(actual.specification().representation_eq(&specification));
    let error = create_batch(&storage, vec![batch_index("other", "new")]).unwrap_err();
    assert_eq!(
        std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<DocumentIndexError>(),
        Some(&DocumentIndexError::OptionsConflict)
    );
}

#[test]
fn batch_crash_preserves_completed_prefix_and_cleans_only_unfinished_entry() {
    for count in [2, 4] {
        let mut points = vec![
            "create-before-intent:0".to_owned(),
            "create-after-intent:0".to_owned(),
            "create-before-activation:0".to_owned(),
            "create-after-activation:0".to_owned(),
        ];
        for shard in 0..count {
            points.push(format!("create-before-shard:{shard}"));
            points.push(format!("create-after-shard:{shard}"));
        }
        for point in points {
            let temp = tempfile::tempdir().unwrap();
            let (storage, _) = setup(temp.path(), count);
            let records = snapshot(temp.path(), count, "briskdb_documents_v1");
            drop(storage);
            crash_operation_mode(temp.path(), count, &point, false, None, true);
            let storage = Storage::open(temp.path(), count).unwrap();
            let catalog = storage.document_catalog().unwrap();
            let indexes = catalog.collection("app", "items").unwrap().indexes();
            assert!(
                indexes
                    .iter()
                    .any(|i| i.name() == "value" && i.lifecycle() == DocumentIndexLifecycle::Ready)
            );
            let finished = point == "create-after-activation:0";
            assert_eq!(indexes.len(), if finished { 3 } else { 2 });
            assert_eq!(indexes.iter().any(|i| i.name() == "other"), finished);
            assert_eq!(
                snapshot(temp.path(), count, "briskdb_documents_v1"),
                records
            );
            let before = if finished { 3 } else { 2 };
            assert_eq!(
                create_batch(
                    &storage,
                    vec![batch_index("value", "value"), batch_index("other", "other")]
                )
                .unwrap(),
                (before, 3)
            );
        }
    }
}

#[test]
fn combined_creation_publishes_entries_counts_and_shared_cache_without_record_rewrites() {
    let temp = tempfile::tempdir().unwrap();
    let (storage, collection) = setup(temp.path(), 2);
    let peer = Storage::open(temp.path(), 2).unwrap();
    let records = snapshot(temp.path(), 2, "briskdb_documents_v1");
    let high = high_water(temp.path());
    assert_eq!(create_built(&storage, "new", "value").unwrap(), (1, 2));
    assert_eq!(high_water(temp.path()), high + 1);
    let entries = snapshot(temp.path(), 2, "briskdb_document_index_entries_v1");
    assert_eq!(entries.iter().map(Vec::len).sum::<usize>(), 24);
    assert_eq!(create_built(&storage, "new", "value").unwrap(), (2, 2));
    assert_eq!(high_water(temp.path()), high + 1);
    assert_eq!(
        snapshot(temp.path(), 2, "briskdb_document_index_entries_v1"),
        entries
    );
    assert_eq!(snapshot(temp.path(), 2, "briskdb_documents_v1"), records);
    assert_eq!(
        create_built(&storage, "new", "other").unwrap_err().kind(),
        EngineErrorKind::FailedPrecondition
    );
    assert_eq!(create_built(&storage, "value", "value").unwrap(), (2, 3));
    assert_eq!(high_water(temp.path()), high + 1); // reuses the existing Pending identity
    assert!(
        peer.document_index_is_ready(&DocumentNamespace::new("app", "items").unwrap(), "new")
            .unwrap()
    );
    peer.insert_document(collection, &document(50, BsonValue::Int32(99)))
        .unwrap();
    assert_eq!(
        snapshot(temp.path(), 2, "briskdb_document_index_entries_v1")
            .iter()
            .map(Vec::len)
            .sum::<usize>(),
        50
    );
    drop(peer);
    drop(storage);
    let storage = Storage::open(temp.path(), 2).unwrap();
    assert_eq!(create_built(&storage, "new", "value").unwrap(), (3, 3));
}

#[test]
fn combined_creation_preflight_failures_leave_catalog_allocator_and_entries_unchanged() {
    let temp = tempfile::tempdir().unwrap();
    let (storage, collection) = setup(temp.path(), 2);
    storage
        .insert_document(
            collection,
            &document(
                50,
                BsonValue::ObjectId(crate::document::BsonObjectId::from_bytes([7; 12])),
            ),
        )
        .unwrap();
    let catalog = storage.document_catalog().unwrap();
    let high = high_water(temp.path());
    assert_eq!(
        create_built(&storage, "new", "value").unwrap_err().kind(),
        EngineErrorKind::Unsupported
    );
    assert_eq!(storage.document_catalog().unwrap(), catalog);
    assert_eq!(high_water(temp.path()), high);
    assert!(
        snapshot(temp.path(), 2, "briskdb_document_index_entries_v1")
            .iter()
            .all(Vec::is_empty)
    );
    // The rejected name remains available, and the root remains usable.
    assert_eq!(create_built(&storage, "new", "other").unwrap(), (1, 2));
    drop(storage);
    drop(Storage::open(temp.path(), 2).unwrap());
}

#[test]
fn combined_creation_combined_key_budget_rejects_before_allocating_identity() {
    let temp = tempfile::tempdir().unwrap();
    let (storage, collection) = setup(temp.path(), 2);
    storage
        .insert_document(
            collection,
            &document(
                50,
                BsonValue::Array((0..9000).map(BsonValue::Int32).collect()),
            ),
        )
        .unwrap();
    build(&storage, "value").unwrap();
    let high = high_water(temp.path());
    let catalog = storage.document_catalog().unwrap();
    let entries = snapshot(temp.path(), 2, "briskdb_document_index_entries_v1");
    assert_eq!(
        create_built(&storage, "new", "value").unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
    assert_eq!(high_water(temp.path()), high);
    assert_eq!(storage.document_catalog().unwrap(), catalog);
    assert_eq!(
        snapshot(temp.path(), 2, "briskdb_document_index_entries_v1"),
        entries
    );
    drop(storage.enter_schema_operation().unwrap());
}

#[test]
fn built_drop_preserves_records_surviving_entries_and_allocator_without_stale_caches() {
    use crate::document::BsonObjectId;
    let temp = tempfile::tempdir().unwrap();
    let (storage, collection) = setup(temp.path(), 2);
    let peer = Storage::open(temp.path(), 2).unwrap();
    let removed = build(&storage, "value").unwrap();
    storage
        .declare_document_index(
            collection,
            "keep",
            &BsonDocument::from_entries([("other", BsonValue::Int32(1))]).unwrap(),
            false,
        )
        .unwrap();
    build(&storage, "keep").unwrap();
    let namespace = DocumentNamespace::new("app", "items").unwrap();
    assert!(peer.document_index_is_ready(&namespace, "value").unwrap());
    let records = snapshot(temp.path(), 2, "briskdb_documents_v1");
    let mut surviving = snapshot(temp.path(), 2, "briskdb_document_index_entries_v1");
    for shard in &mut surviving {
        shard.retain(|row| row[1] != Value::Integer(removed.id().get() as i64));
    }
    drop_built(&storage, "value").unwrap();
    assert!(!peer.document_index_is_ready(&namespace, "value").unwrap());
    assert!(peer.document_index_is_ready(&namespace, "keep").unwrap());
    assert_eq!(snapshot(temp.path(), 2, "briskdb_documents_v1"), records);
    assert_eq!(
        snapshot(temp.path(), 2, "briskdb_document_index_entries_v1"),
        surviving
    );
    // The removed index no longer rejects values outside its supported key subset.
    peer.insert_document(
        collection,
        &document(50, BsonValue::ObjectId(BsonObjectId::from_bytes([7; 12]))),
    )
    .unwrap();
    assert_eq!(
        snapshot(temp.path(), 2, "briskdb_document_index_entries_v1")
            .iter()
            .map(Vec::len)
            .sum::<usize>(),
        13
    );
    let replacement = storage
        .declare_document_index(collection, "value", removed.specification(), false)
        .unwrap();
    assert!(replacement.id().get() > removed.id().get());
    assert_eq!(
        replacement.lifecycle(),
        DocumentIndexLifecycle::PendingBuild
    );
    drop(peer);
    drop(storage);
    drop(Storage::open(temp.path(), 2).unwrap());
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

#[test]
fn equality_candidates_require_current_ready_authority_and_keep_natural_pagination() {
    for count in [2, 4] {
        let root = tempfile::tempdir().unwrap();
        let (storage, collection) = setup(root.path(), count);
        let matcher = DocumentMatcher::compile(
            &BsonDocument::from_entries([("value", BsonValue::Double(99.0))]).unwrap(),
        )
        .unwrap();
        let admission = storage.enter_schema_operation().unwrap();
        assert!(
            storage
                .document_equality_probe(collection, &matcher, &mut || Ok(()))
                .unwrap()
                .is_none()
        );
        drop(admission);
        let metadata = build(&storage, "value").unwrap();
        let admission = storage.enter_schema_operation().unwrap();
        let probe = storage
            .document_equality_probe(collection, &matcher, &mut || Ok(()))
            .unwrap()
            .unwrap();
        assert_eq!(probe.index_id(), metadata.id());
        let token = CancellationToken::new();
        let mut total = 0;
        for shard in 0..count {
            let connection =
                Connection::open(root.path().join(format!("shards/{shard:04}.sqlite"))).unwrap();
            assert_eq!(
                storage
                    .scan_document_candidates_on_connection(
                        &connection,
                        DocumentCollectionId::from_validated(collection.get() + 1),
                        shard,
                        None,
                        1,
                        Some(&probe),
                        &token
                    )
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::FailedPrecondition
            );
            let scan = storage
                .scan_document_shard_on_connection(
                    &connection,
                    collection,
                    shard,
                    None,
                    100,
                    &token,
                )
                .unwrap();
            let mut after = None;
            for expected in scan {
                let page = storage
                    .scan_document_candidates_on_connection(
                        &connection,
                        collection,
                        shard,
                        after,
                        1,
                        Some(&probe),
                        &token,
                    )
                    .unwrap();
                assert_eq!(page.len(), 1);
                assert_eq!(page[0].id_key(), expected.id_key());
                assert_eq!(page[0].natural_order(), expected.natural_order());
                after = Some(page[0].natural_order());
                total += 1;
            }
            assert!(
                storage
                    .scan_document_candidates_on_connection(
                        &connection,
                        collection,
                        shard,
                        after,
                        1,
                        Some(&probe),
                        &token
                    )
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                storage
                    .scan_document_candidates_on_connection(
                        &connection,
                        collection,
                        shard,
                        None,
                        0,
                        Some(&probe),
                        &token
                    )
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::InvalidArgument
            );
            let cancelled = CancellationToken::new();
            cancelled.cancel();
            assert_eq!(
                storage
                    .scan_document_candidates_on_connection(
                        &connection,
                        collection,
                        shard,
                        None,
                        1,
                        Some(&probe),
                        &cancelled
                    )
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::Cancelled
            );
        }
        assert_eq!(total, 12);
        drop(admission);
        drop_built(&storage, "value").unwrap();
        let admission = storage.enter_schema_operation().unwrap();
        assert!(
            storage
                .document_equality_probe(collection, &matcher, &mut || Ok(()))
                .unwrap()
                .is_none()
        );
        drop(admission);
        drop(storage);
        let storage = Storage::open(root.path(), count).unwrap();
        create_built(&storage, "value", "value").unwrap();
        let _admission = storage.enter_schema_operation().unwrap();
        let replacement = storage
            .document_equality_probe(collection, &matcher, &mut || Ok(()))
            .unwrap()
            .unwrap();
        assert_ne!(replacement.index_id(), probe.index_id());
    }
}

#[test]
fn equality_candidates_skip_unselected_bson_but_validate_selected_entry_binding() {
    for damage in ["checksum", "version", "record"] {
        let root = tempfile::tempdir().unwrap();
        let (storage, collection) = setup(root.path(), 2);
        build(&storage, "value").unwrap();
        let _admission = storage.enter_schema_operation().unwrap();
        let matcher = DocumentMatcher::compile(
            &BsonDocument::from_entries([("value", BsonValue::Int32(0))]).unwrap(),
        )
        .unwrap();
        let probe = storage
            .document_equality_probe(collection, &matcher, &mut || Ok(()))
            .unwrap()
            .unwrap();
        let token = CancellationToken::new();
        let mut found = false;
        for shard in 0..2 {
            let connection =
                Connection::open(root.path().join(format!("shards/{shard:04}.sqlite"))).unwrap();
            let selected = storage
                .scan_document_candidates_on_connection(
                    &connection,
                    collection,
                    shard,
                    None,
                    100,
                    Some(&probe),
                    &token,
                )
                .unwrap();
            if let Some(record) = selected.first() {
                assert_eq!(selected.len(), 1);
                found = true;
                // A test-owned, deliberately damaged noncandidate proves that
                // the index restricts BSON decoding, not just post-filtering.
                let changed = connection.execute("UPDATE briskdb_documents_v1 SET document_checksum = zeroblob(32) WHERE id_key != ?1",
                    [record.id_key().as_bytes()]).unwrap();
                assert!(changed > 0);
                assert_eq!(
                    storage
                        .scan_document_candidates_on_connection(
                            &connection,
                            collection,
                            shard,
                            None,
                            100,
                            Some(&probe),
                            &token
                        )
                        .unwrap()
                        .len(),
                    1
                );
                assert_eq!(
                    storage
                        .scan_document_shard_on_connection(
                            &connection,
                            collection,
                            shard,
                            None,
                            100,
                            &token
                        )
                        .unwrap_err()
                        .kind(),
                    EngineErrorKind::DataCorruption
                );
                let sql = match damage {
                    "checksum" => {
                        "UPDATE briskdb_document_index_entries_v1 SET entry_checksum = zeroblob(32) WHERE id_key = ?1"
                    }
                    "version" => {
                        "UPDATE briskdb_document_index_entries_v1 SET entry_format_version = 2 WHERE id_key = ?1"
                    }
                    _ => {
                        "UPDATE briskdb_documents_v1 SET document_checksum = zeroblob(32) WHERE id_key = ?1"
                    }
                };
                connection
                    .execute_batch("PRAGMA ignore_check_constraints = ON")
                    .unwrap();
                connection
                    .execute(sql, [record.id_key().as_bytes()])
                    .unwrap();
                assert_eq!(
                    storage
                        .scan_document_candidates_on_connection(
                            &connection,
                            collection,
                            shard,
                            None,
                            100,
                            Some(&probe),
                            &token
                        )
                        .unwrap_err()
                        .kind(),
                    EngineErrorKind::DataCorruption
                );
            }
        }
        assert!(found);
    }
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
    if std::env::var("BRISKDB_TEST_INDEX_OPERATION_DROP_BATCH").as_deref() == Ok("1") {
        drop_selected(&storage, None).unwrap();
    } else if std::env::var("BRISKDB_TEST_INDEX_OPERATION_BATCH").as_deref() == Ok("1") {
        create_batch(
            &storage,
            vec![batch_index("value", "value"), batch_index("other", "other")],
        )
        .unwrap();
    } else if let Ok(name) = std::env::var("BRISKDB_TEST_INDEX_OPERATION_CREATE") {
        create_built(&storage, &name, "value").unwrap();
    } else if std::env::var("BRISKDB_TEST_INDEX_OPERATION_DROP").as_deref() == Ok("1") {
        drop_built(&storage, "value").unwrap();
    } else {
        build(&storage, "value").unwrap();
    }
    panic!("index operation crash boundary was not reached");
}

fn crash(root: &Path, count: u16, point: &str) {
    crash_mode(root, count, point, false);
}

fn crash_mode(root: &Path, count: u16, point: &str, dropping: bool) {
    crash_operation(root, count, point, dropping, None);
}

fn crash_operation(root: &Path, count: u16, point: &str, dropping: bool, creating: Option<&str>) {
    crash_operation_mode(root, count, point, dropping, creating, false);
}

fn crash_operation_mode(
    root: &Path,
    count: u16,
    point: &str,
    dropping: bool,
    creating: Option<&str>,
    batch: bool,
) {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    child
        .args([
            "--exact",
            "storage::document::enabled::index_operations::tests::index_operation_crash_child",
            "--nocapture",
        ])
        .env("BRISKDB_TEST_INDEX_OPERATION_ROOT", root)
        .env("BRISKDB_TEST_INDEX_OPERATION_SHARDS", count.to_string())
        .env(
            "BRISKDB_TEST_INDEX_OPERATION_BATCH",
            if batch { "1" } else { "0" },
        )
        .env("BRISKDB_TEST_DOCUMENT_INDEX_OPERATION_CRASH", point)
        .env(
            "BRISKDB_TEST_INDEX_OPERATION_DROP",
            if dropping { "1" } else { "0" },
        );
    if let Some(name) = creating {
        child.env("BRISKDB_TEST_INDEX_OPERATION_CREATE", name);
    } else {
        child.env_remove("BRISKDB_TEST_INDEX_OPERATION_CREATE");
    }
    let output = child.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(75),
        "{point}: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn every_combined_creation_boundary_removes_only_unfinished_new_declarations() {
    for count in [2, 4] {
        let mut points = vec![
            "create-before-intent:0".to_owned(),
            "create-after-intent:0".to_owned(),
            "create-before-activation:0".to_owned(),
            "create-after-activation:0".to_owned(),
        ];
        for shard in 0..count {
            for point in ["create-before-shard", "create-after-shard"] {
                points.push(format!("{point}:{shard}"));
            }
        }
        for point in points {
            let temp = tempfile::tempdir().unwrap();
            let (storage, _) = setup(temp.path(), count);
            build(&storage, "value").unwrap();
            let records = snapshot(temp.path(), count, "briskdb_documents_v1");
            let entries = snapshot(temp.path(), count, "briskdb_document_index_entries_v1");
            let high = high_water(temp.path());
            drop(storage);
            crash_operation(temp.path(), count, &point, false, Some("new"));
            let storage = Storage::open(temp.path(), count).unwrap();
            let catalog = storage.document_catalog().unwrap();
            let indexes = catalog.collection("app", "items").unwrap().indexes();
            let activated = point == "create-after-activation:0";
            assert_eq!(indexes.len(), if activated { 3 } else { 2 }, "{point}");
            assert!(
                indexes
                    .iter()
                    .all(|i| i.lifecycle() == DocumentIndexLifecycle::Ready)
            );
            assert_eq!(
                high_water(temp.path()),
                high + i64::from(point != "create-before-intent:0")
            );
            assert_eq!(
                snapshot(temp.path(), count, "briskdb_documents_v1"),
                records
            );
            if !activated {
                assert_eq!(
                    snapshot(temp.path(), count, "briskdb_document_index_entries_v1"),
                    entries
                );
            }
            let before_retry = high_water(temp.path());
            assert_eq!(
                create_built(&storage, "new", "value").unwrap(),
                if activated { (3, 3) } else { (2, 3) }
            );
            assert_eq!(
                high_water(temp.path()),
                before_retry + i64::from(!activated)
            );
            drop(storage);
            drop(Storage::open(temp.path(), count).unwrap());
        }
    }
    // The combined API must preserve an already-declared Pending index on abort.
    let temp = tempfile::tempdir().unwrap();
    let (storage, _) = setup(temp.path(), 2);
    let catalog = storage.document_catalog().unwrap();
    let high = high_water(temp.path());
    drop(storage);
    crash_operation(temp.path(), 2, "before-activation:0", false, Some("value"));
    let storage = Storage::open(temp.path(), 2).unwrap();
    assert_eq!(storage.document_catalog().unwrap(), catalog);
    assert_eq!(high_water(temp.path()), high);
    assert!(
        snapshot(temp.path(), 2, "briskdb_document_index_entries_v1")
            .iter()
            .all(Vec::is_empty)
    );
    assert_eq!(create_built(&storage, "value", "value").unwrap(), (1, 2));
}

#[test]
fn combined_creation_cleanup_can_itself_restart_at_every_commit_boundary() {
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
        let (storage, _) = setup(temp.path(), count);
        build(&storage, "value").unwrap();
        let entries = snapshot(temp.path(), count, "briskdb_document_index_entries_v1");
        let records = snapshot(temp.path(), count, "briskdb_documents_v1");
        let high = high_water(temp.path());
        drop(storage);
        crash_operation(
            temp.path(),
            count,
            "create-before-activation:0",
            false,
            Some("new"),
        );
        crash(temp.path(), count, &point);
        let storage = Storage::open(temp.path(), count).unwrap();
        assert_eq!(
            snapshot(temp.path(), count, "briskdb_document_index_entries_v1"),
            entries
        );
        assert_eq!(
            snapshot(temp.path(), count, "briskdb_documents_v1"),
            records
        );
        assert_eq!(high_water(temp.path()), high + 1);
        assert_eq!(
            storage
                .document_catalog()
                .unwrap()
                .collection("app", "items")
                .unwrap()
                .indexes()
                .len(),
            2
        );
    }
}

#[test]
fn every_built_drop_boundary_recovers_without_rewriting_surviving_authority() {
    for count in [2, 4] {
        let mut points = vec![
            "drop-before-intent:0".to_owned(),
            "drop-after-intent:0".to_owned(),
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
            let removed = build(&storage, "value").unwrap();
            let keep = storage
                .declare_document_index(
                    collection,
                    "keep",
                    &BsonDocument::from_entries([("other", BsonValue::Int32(1))]).unwrap(),
                    false,
                )
                .unwrap();
            build(&storage, "keep").unwrap();
            let records = snapshot(temp.path(), count, "briskdb_documents_v1");
            let mut entries = snapshot(temp.path(), count, "briskdb_document_index_entries_v1");
            if point != "drop-before-intent:0" {
                for shard in &mut entries {
                    shard.retain(|row| row[1] != Value::Integer(removed.id().get() as i64));
                }
            }
            drop(storage);
            crash_mode(temp.path(), count, &point, true);
            let storage = Storage::open(temp.path(), count).unwrap();
            assert_eq!(
                snapshot(temp.path(), count, "briskdb_documents_v1"),
                records,
                "{point}"
            );
            assert_eq!(
                snapshot(temp.path(), count, "briskdb_document_index_entries_v1"),
                entries,
                "{point}"
            );
            let catalog = storage.document_catalog().unwrap();
            let indexes = catalog.collection("app", "items").unwrap().indexes();
            assert_eq!(
                indexes.iter().any(|index| index.id() == removed.id()),
                point == "drop-before-intent:0"
            );
            assert!(indexes.iter().any(|index| index.id() == keep.id()
                && index.lifecycle() == DocumentIndexLifecycle::Ready));
            if point == "drop-before-intent:0" {
                drop_built(&storage, "value").unwrap();
            }
            let next = storage
                .declare_document_index(collection, "value", removed.specification(), false)
                .unwrap();
            assert!(next.id().get() > keep.id().get());
            build(&storage, "value").unwrap();
            drop(storage);
            drop(Storage::open(temp.path(), count).unwrap());
        }
    }
}

#[test]
fn cancellation_during_cleanup_validation_is_not_corruption() {
    use crate::core::CancellationReason;
    use std::sync::Barrier;

    let temp = tempfile::tempdir().unwrap();
    let (storage, _) = setup(temp.path(), 2);
    build(&storage, "value").unwrap();
    let blocker = storage.open_unconfigured_shard(0).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    for attempt in 0..512 {
        let mut connection = storage.open_unconfigured_shard(0).unwrap();
        let control = OperationControl::new(None);
        let observer_control = Arc::clone(&control);
        let ready = Arc::new(Barrier::new(2));
        let observer_ready = Arc::clone(&ready);
        let observer = std::thread::spawn(move || {
            observer_ready.wait();
            std::thread::sleep(std::time::Duration::from_micros((attempt % 256) * 10));
            observer_control.request_cancel(CancellationReason::Cancelled);
        });
        let result: EngineResult<()> =
            run_provisioning_step(&mut connection, Some(&control), |connection| {
                ready.wait();
                storage.validate_unconfigured_shard_nonterminal(connection, 0)?;
                require_schema(connection)?;
                // Keep the operation interruptible even when validation finishes
                // before the observer. No mutation is performed.
                connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sqlite_error::storage)?;
                panic!("the blocker must prevent write admission");
            });
        observer.join().unwrap();
        let error = result.unwrap_err();
        assert_eq!(
            error.kind(),
            EngineErrorKind::Cancelled,
            "attempt {attempt}: {error:?}"
        );
    }
    blocker.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn cancellation_at_each_cleanup_validation_checkpoint_is_not_corruption() {
    use crate::core::CancellationReason;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let temp = tempfile::tempdir().unwrap();
    let (storage, _) = setup(temp.path(), 2);
    build(&storage, "value").unwrap();
    for checkpoint in 1..=10_000 {
        let mut connection = storage.open_unconfigured_shard(0).unwrap();
        let control = OperationControl::new(None);
        let progress_control = Arc::clone(&control);
        let result = run_provisioning_step(&mut connection, Some(&control), |connection| {
            let steps = AtomicUsize::new(0);
            connection
                .progress_handler(
                    1,
                    Some(move || {
                        if steps.fetch_add(1, Ordering::Relaxed) + 1 == checkpoint {
                            progress_control.request_cancel(CancellationReason::Cancelled);
                        }
                        // Exercise the interrupt callback itself. Returning true
                        // would also force SQLITE_INTERRUPT from the outer VM.
                        false
                    }),
                )
                .unwrap();
            storage.validate_unconfigured_shard_nonterminal(connection, 0)?;
            require_schema(connection)
        });
        if control.reason().is_none() {
            result.unwrap();
            assert!(checkpoint > 100, "must cover the complete validation");
            return;
        }
        if let Err(error) = result {
            assert_eq!(
                error.kind(),
                EngineErrorKind::Cancelled,
                "checkpoint {checkpoint}: {error:?}"
            );
        }
    }
    panic!("validation exceeded the bounded checkpoint sweep");
}

#[test]
fn cancelled_admitted_drop_stays_fenced_until_reopen_finishes_cleanup() {
    use crate::core::CancellationReason;
    let temp = tempfile::tempdir().unwrap();
    let (storage, _) = setup(temp.path(), 2);
    build(&storage, "value").unwrap();
    let records = snapshot(temp.path(), 2, "briskdb_documents_v1");
    let blocker = storage.open_unconfigured_shard(0).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let control = OperationControl::new(None);
    let observer_control = Arc::clone(&control);
    let manifest_path = temp.path().join("manifest.sqlite");
    let observer = std::thread::spawn(move || {
        let connection = Connection::open(manifest_path).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            let admitted: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM briskdb_document_index_operation WHERE operation_kind=2)", [], |row| row.get(0)).unwrap();
            if admitted {
                assert!(observer_control.request_cancel(CancellationReason::Cancelled));
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "drop intent was not admitted"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    });
    let migration = storage.begin_schema_migration().unwrap();
    migration.wait_for_quiescence_blocking();
    let error = storage
        .drop_built_document_index_controlled("app", "items", "value", migration, control)
        .unwrap_err();
    observer.join().unwrap();
    assert_eq!(error.kind(), EngineErrorKind::Cancelled, "{error:?}");
    assert!(storage.enter_schema_operation().is_err());
    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
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
}

#[test]
fn cancelled_drop_batch_keeps_completed_prefix_and_fences_current_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let (storage, collection) = setup(temp.path(), 2);
    build(&storage, "value").unwrap();
    storage
        .declare_document_index(
            collection,
            "!pending",
            &BsonDocument::from_entries([("pending", BsonValue::Int32(1))]).unwrap(),
            false,
        )
        .unwrap();
    let records = snapshot(temp.path(), 2, "briskdb_documents_v1");
    let high = high_water(temp.path());
    let blocker = storage.open_unconfigured_shard(0).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let control = OperationControl::new(None);
    let observer_control = Arc::clone(&control);
    let manifest = temp.path().join("manifest.sqlite");
    let observer = std::thread::spawn(move || {
        let connection = Connection::open(manifest).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            let admitted: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM briskdb_document_index_operation WHERE operation_kind=2)", [], |row| row.get(0)).unwrap();
            if admitted {
                let pending_exists: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM briskdb_document_indexes WHERE index_name='!pending')", [], |row| row.get(0)).unwrap();
                assert!(
                    !pending_exists,
                    "completed prefix must already be committed"
                );
                assert!(
                    observer_control.request_cancel(crate::core::CancellationReason::Cancelled)
                );
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "batch drop intent was not admitted"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    });
    let migration = storage.begin_schema_migration().unwrap();
    migration.wait_for_quiescence_blocking();
    let error = storage
        .drop_document_indexes_controlled(
            &DocumentNamespace::new("app", "items").unwrap(),
            None,
            migration,
            control,
        )
        .unwrap_err();
    observer.join().unwrap();
    assert_eq!(error.kind(), EngineErrorKind::Cancelled);
    assert!(storage.enter_schema_operation().is_err());
    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
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
    assert_eq!(high_water(temp.path()), high);
}

#[test]
fn cancelled_admitted_creation_stays_fenced_until_reopen_removes_its_new_declaration() {
    let temp = tempfile::tempdir().unwrap();
    let (storage, _) = setup(temp.path(), 2);
    build(&storage, "value").unwrap();
    let catalog = storage.document_catalog().unwrap();
    let records = snapshot(temp.path(), 2, "briskdb_documents_v1");
    let entries = snapshot(temp.path(), 2, "briskdb_document_index_entries_v1");
    let high = high_water(temp.path());
    let blocker = storage.open_unconfigured_shard(0).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let control = OperationControl::new(None);
    let observer_control = Arc::clone(&control);
    let manifest_path = temp.path().join("manifest.sqlite");
    let observer = std::thread::spawn(move || {
        let connection = Connection::open(manifest_path).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            let admitted: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM briskdb_document_index_operation WHERE operation_kind=2 AND next_shard=0)", [], |row| row.get(0)).unwrap();
            if admitted {
                assert!(
                    observer_control.request_cancel(crate::core::CancellationReason::Cancelled)
                );
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "creation intent was not admitted"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    });
    let migration = storage.begin_schema_migration().unwrap();
    migration.wait_for_quiescence_blocking();
    let error = storage
        .create_built_document_index_controlled(
            &DocumentNamespace::new("app", "items").unwrap(),
            "new",
            &BsonDocument::from_entries([("value", BsonValue::Int32(1))]).unwrap(),
            false,
            migration,
            control,
        )
        .unwrap_err();
    observer.join().unwrap();
    assert_eq!(error.kind(), EngineErrorKind::Cancelled);
    assert!(storage.enter_schema_operation().is_err());
    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
    drop(storage);
    let storage = Storage::open(temp.path(), 2).unwrap();
    assert_eq!(storage.document_catalog().unwrap(), catalog);
    assert_eq!(high_water(temp.path()), high + 1);
    assert_eq!(snapshot(temp.path(), 2, "briskdb_documents_v1"), records);
    assert_eq!(
        snapshot(temp.path(), 2, "briskdb_document_index_entries_v1"),
        entries
    );
    assert_eq!(create_built(&storage, "new", "value").unwrap(), (2, 3));
    assert_eq!(high_water(temp.path()), high + 2);
}

#[test]
fn combined_creation_exhausted_identity_space_fails_before_intent() {
    let temp = tempfile::tempdir().unwrap();
    let (storage, _) = setup(temp.path(), 2);
    drop(storage);
    let mut connection = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
    let transaction = connection.transaction().unwrap();
    transaction
        .execute(
            "UPDATE briskdb_document_index_allocator SET index_high_water=?1",
            [i64::MAX],
        )
        .unwrap();
    manifest::refresh_manifest_digest(&transaction).unwrap();
    transaction.commit().unwrap();
    let storage = Storage::open(temp.path(), 2).unwrap();
    let catalog = storage.document_catalog().unwrap();
    assert_eq!(
        create_built(&storage, "new", "value").unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
    assert_eq!(storage.document_catalog().unwrap(), catalog);
    assert!(load(&connection).unwrap().is_none());
    assert_eq!(high_water(temp.path()), i64::MAX);
    assert_eq!(create_built(&storage, "value", "value").unwrap(), (1, 2));
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
