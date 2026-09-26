//! Real, bounded SQLite allocation failures on owned temporary document shards.

use std::sync::atomic::{AtomicBool, Ordering};

use rusqlite::{
    hooks::{AuthAction, AuthContext, Authorization},
    types::Value,
};

use super::*;
use crate::storage::process_lock::document_write::DocumentWriteFence;

fn doc(id: i32, value: &str, payload: &str) -> BsonDocument {
    BsonDocument::from_entries([
        ("_id", BsonValue::Int32(id)),
        ("value", BsonValue::String(value.into())),
        ("payload", BsonValue::String(payload.into())),
    ])
    .unwrap()
}

fn snapshot(connection: &Connection, sql: &str) -> Vec<Vec<Value>> {
    let mut statement = connection.prepare(sql).unwrap();
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            (0..columns).map(|column| row.get(column)).collect()
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn claim(storage: &Storage, collection: DocumentCollectionId) -> EngineResult<DocumentWriteFence> {
    DocumentWriteFence::try_acquire(
        &storage.root,
        collection,
        Arc::clone(&storage.schema_coordination.document_write_stripes),
    )
}

#[test]
fn sqlite_full_rolls_back_documents_and_unique_entries_then_recovers() {
    const RECORDS: &str = "SELECT * FROM briskdb_documents_v1 ORDER BY collection_id, id_key";
    const ENTRIES: &str = "SELECT * FROM briskdb_document_index_entries_v1
        ORDER BY collection_id, index_id, index_key, id_key";
    for replace in [false, true] {
        for fail_index in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let storage = Storage::open(root.path(), 2).unwrap();
            let collection = storage
                .create_document_collection("app", "items", &DocumentCollectionOptions::empty())
                .unwrap()
                .id();
            let shard = storage.prepare_document_id(&BsonValue::Int32(0)).unwrap().1;
            let ids: Vec<i32> = (0..100)
                .filter(|id| {
                    storage
                        .prepare_document_id(&BsonValue::Int32(*id))
                        .unwrap()
                        .1
                        == shard
                })
                .take(3)
                .collect();
            assert_eq!(ids.len(), 3);
            let original = doc(ids[0], "original-marker", "before");
            let target = doc(ids[1], "original-target", "before");
            storage.insert_document(collection, &original).unwrap();
            storage.insert_document(collection, &target).unwrap();
            storage
                .declare_document_index(
                    collection,
                    "value_unique",
                    &BsonDocument::from_entries([("value", BsonValue::Int32(1))]).unwrap(),
                    true,
                )
                .unwrap();
            let migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            storage
                .build_document_index_controlled(
                    "app",
                    "items",
                    "value_unique",
                    migration,
                    OperationControl::new(None),
                )
                .unwrap();
            let admission = storage.enter_schema_operation().unwrap();
            let cancellation = CancellationToken::new();
            let order = storage
                .reserve_document_natural_orders_for_engine(collection, 1, &cancellation)
                .unwrap();
            let failed_id = ids[if replace { 1 } else { 2 }];
            let large = "x".repeat(1024 * 1024);
            let oversized_document = if fail_index {
                doc(failed_id, &large, "")
            } else {
                doc(failed_id, "failed-value", &large)
            };
            let encoded_bytes = encode_document(&oversized_document).unwrap().len();
            let oversized = storage.prepare_document_write(&oversized_document).unwrap();
            let marker = storage
                .prepare_document_write(&doc(ids[0], "uncommitted-marker", "changed"))
                .unwrap();
            let connection = storage.open_unconfigured_shard(shard).unwrap();
            storage
                .validate_unconfigured_shard(&connection, shard)
                .unwrap();
            let records_before = snapshot(&connection, RECORDS);
            let entries_before = snapshot(&connection, ENTRIES);
            assert!(!entries_before.is_empty());
            let page_count: i64 = connection
                .pragma_query_value(None, "page_count", |row| row.get(0))
                .unwrap();
            let page_size: i64 = connection
                .pragma_query_value(None, "page_size", |row| row.get(0))
                .unwrap();
            // Record failure cannot fit the 1-MiB BSON. Index failure has room
            // for that BSON and small maintenance overhead, but not its second
            // 1-MiB secondary key. No host disk or other connection is limited.
            let headroom = if fail_index {
                i64::try_from(encoded_bytes).unwrap() / page_size + 16
            } else {
                8
            };
            let maximum = page_count + headroom;
            assert_eq!(
                connection
                    .query_row(&format!("PRAGMA max_page_count = {maximum}"), [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                maximum
            );
            let transaction = storage
                .begin_document_write(&connection, collection, shard, &cancellation, None)
                .unwrap();
            assert_eq!(
                claim(&storage, collection).unwrap_err().kind(),
                EngineErrorKind::Busy
            );
            assert!(
                storage
                    .replace_document_on_connection(
                        &transaction,
                        collection,
                        shard,
                        marker.id_key(),
                        1,
                        &marker,
                        &cancellation
                    )
                    .unwrap()
            );
            // Installing this observer invalidates cached prepared statements.
            // Entry INSERT authorization occurs only after the record statement
            // succeeded; it proves which allocation boundary is actually hit.
            let entered_index = Arc::new(AtomicBool::new(false));
            let observed = Arc::clone(&entered_index);
            connection
                .authorizer(Some(move |context: AuthContext<'_>| {
                    if matches!(
                        context.action,
                        AuthAction::Insert {
                            table_name: "briskdb_document_index_entries_v1"
                        }
                    ) {
                        observed.store(true, Ordering::SeqCst);
                    }
                    Authorization::Allow
                }))
                .unwrap();
            let result = if replace {
                storage
                    .replace_document_on_connection(
                        &transaction,
                        collection,
                        shard,
                        oversized.id_key(),
                        2,
                        &oversized,
                        &cancellation,
                    )
                    .map(|_| ())
            } else {
                storage.insert_prepared_document_on_connection(
                    &transaction,
                    collection,
                    order,
                    shard,
                    &oversized,
                    &cancellation,
                )
            };
            assert_eq!(
                result.unwrap_err().kind(),
                EngineErrorKind::StorageFull,
                "replace={replace}, index={fail_index}"
            );
            assert_eq!(entered_index.load(Ordering::SeqCst), fail_index);
            connection
                .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
                .unwrap();
            assert!(
                connection.is_autocommit(),
                "SQLite FULL must end the entire transaction"
            );
            // The wrapper conservatively retains uniqueness authority until
            // cleanup, even after SQLite's automatic rollback.
            assert_eq!(
                claim(&storage, collection).unwrap_err().kind(),
                EngineErrorKind::Busy
            );
            drop(transaction);
            drop(claim(&storage, collection).unwrap());
            drop(storage.enter_schema_operation().unwrap());
            assert_eq!(snapshot(&connection, RECORDS), records_before);
            assert_eq!(snapshot(&connection, ENTRIES), entries_before);
            assert_eq!(
                connection
                    .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );

            // Restore only this owned connection and prove it can write again,
            // including unique-key authority and durable reopen validation.
            let restored = maximum + 4096;
            assert_eq!(
                connection
                    .query_row(&format!("PRAGMA max_page_count = {restored}"), [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                restored
            );
            let recovered = oversized_document;
            let prepared = storage.prepare_document_write(&recovered).unwrap();
            let recovery_order = storage
                .reserve_document_natural_orders_for_engine(collection, 1, &cancellation)
                .unwrap();
            assert!(
                recovery_order > order,
                "failed reservations are never reused"
            );
            let transaction = storage
                .begin_document_write(&connection, collection, shard, &cancellation, None)
                .unwrap();
            if replace {
                assert!(
                    storage
                        .replace_document_on_connection(
                            &transaction,
                            collection,
                            shard,
                            prepared.id_key(),
                            2,
                            &prepared,
                            &cancellation
                        )
                        .unwrap()
                );
            } else {
                storage
                    .insert_prepared_document_on_connection(
                        &transaction,
                        collection,
                        recovery_order,
                        shard,
                        &prepared,
                        &cancellation,
                    )
                    .unwrap();
            }
            transaction.commit().unwrap();
            drop(connection);
            drop(admission);
            drop(storage);
            let storage = Storage::open(root.path(), 2).unwrap();
            assert!(
                storage
                    .get_document(collection, &BsonValue::Int32(ids[0]))
                    .unwrap()
                    .unwrap()
                    .representation_eq(&original)
            );
            assert!(
                storage
                    .get_document(collection, &BsonValue::Int32(failed_id))
                    .unwrap()
                    .unwrap()
                    .representation_eq(&recovered)
            );
            if !replace {
                assert!(
                    storage
                        .get_document(collection, &BsonValue::Int32(ids[1]))
                        .unwrap()
                        .unwrap()
                        .representation_eq(&target)
                );
            }
            let peer_id = (100..200)
                .find(|id| {
                    storage
                        .prepare_document_id(&BsonValue::Int32(*id))
                        .unwrap()
                        .1
                        != shard
                })
                .unwrap();
            assert_eq!(
                storage
                    .insert_document(
                        collection,
                        &doc(
                            peer_id,
                            if fail_index { &large } else { "failed-value" },
                            "duplicate"
                        )
                    )
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::UniqueViolation
            );
            storage
                .insert_document(collection, &doc(peer_id, "new-peer-value", "healthy"))
                .unwrap();
        }
    }
}
