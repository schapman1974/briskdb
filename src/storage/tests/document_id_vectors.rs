//! Frozen BSON identity -> routing vectors, including a validated synthetic
//! v17 document root. This is a schema-upgrade test, not an old-binary fixture.

use std::{collections::BTreeMap, path::Path};

use crate::{
    core::{Engine, RequestContext, Session},
    document::*,
};
use rusqlite::Connection;

use super::super::manifest;
mod golden;
mod values;

fn hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn doc(values: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(values).unwrap()
}

fn namespace() -> DocumentNamespace {
    DocumentNamespace::new("routing_vectors", "items").unwrap()
}

fn exact(id: BsonValue) -> DocumentFilter {
    // Explicit equality also treats regex and operator-named objects literally.
    DocumentFilter::new(doc([("_id", BsonValue::Document(doc([("$eq", id)])))])).unwrap()
}

async fn call(engine: &Engine, session: &Session, command: DocumentCommand) -> DocumentExecution {
    engine
        .execute_document(
            session,
            DocumentRequest::new(
                DocumentRequestId::new([1; 16]).unwrap(),
                RequestContext::new(),
                command,
            ),
        )
        .await
        .unwrap()
}

fn checkouts(engine: &Engine) -> Vec<u64> {
    engine
        .pool_snapshot_for_test()
        .unwrap()
        .shards
        .iter()
        .map(|shard| {
            assert_eq!(shard.active, 0);
            shard.checkouts
        })
        .collect()
}

fn assert_owner(engine: &Engine, before: &[u64], execution: &DocumentExecution, owner: u16) {
    assert!(matches!(execution.plan(), Some(DocumentPlan::Point(plan)) if plan.shard() == owner));
    let after = checkouts(engine);
    let touched: Vec<_> = before
        .iter()
        .zip(after)
        .enumerate()
        .filter_map(|(i, (before, after))| (after > *before).then_some(u16::try_from(i).unwrap()))
        .collect();
    assert_eq!(
        touched,
        vec![owner],
        "actual pool access, not only a planned owner"
    );
}

#[derive(Debug, PartialEq, Eq)]
struct Stored {
    shard: u16,
    key: Vec<u8>,
    order: i64,
    bson: Vec<u8>,
    checksum: Vec<u8>,
}

fn image(root: &Path, shards: u16) -> Vec<Stored> {
    let mut rows = Vec::new();
    for shard in 0..shards {
        let connection = Connection::open(root.join(format!("shards/{shard:04}.sqlite"))).unwrap();
        rows.extend(connection.prepare("SELECT id_key,natural_order,document_bson,document_checksum FROM briskdb_documents_v1 ORDER BY id_key").unwrap()
            .query_map([], |row| Ok(Stored { shard, key: row.get(0)?, order: row.get(1)?, bson: row.get(2)?, checksum: row.get(3)? })).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap());
    }
    rows
}

fn synthetic_v17(root: &Path, shards: u16) {
    // Reuse the schema/digest-validated migration fixture, not a version-number
    // edit. No secondary declarations or entries exist in this fixture.
    let connection = Connection::open(root.join("manifest.sqlite")).unwrap();
    manifest::downgrade_v18_manifest_to_v17_for_test(&connection, shards).unwrap();
    assert_eq!(
        connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
            .unwrap(),
        17
    );
    for shard in 0..shards {
        let connection = Connection::open(root.join(format!("shards/{shard:04}.sqlite"))).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM briskdb_document_index_entries_v1",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        connection
            .execute_batch("DROP TABLE briskdb_document_index_entries_v1")
            .unwrap();
    }
}

#[test]
fn bson_id_routing_v1_golden_bytes_hashes_and_all_initial_owner_counts() {
    use crate::core::{RoutingCatalog, initial_physical_shard};
    let values = values::values();
    assert_eq!(values.len(), 38);
    assert_eq!(values.len(), golden::VECTORS.len());
    for ((name, value), (expected_name, encoded, digest)) in values.iter().zip(golden::VECTORS) {
        assert_eq!(name, expected_name);
        let key = CanonicalBsonKey::encode(value).unwrap();
        assert_eq!(key.encoding_version(), 1);
        assert_eq!(key.as_bytes(), hex(encoded), "{name}");
        assert_eq!(CanonicalBsonKey::from_bytes(&hex(encoded)).unwrap(), key);
        assert_eq!(
            &blake3::hash(key.as_bytes()).as_bytes()[..8],
            digest.to_le_bytes(),
            "{name}"
        );
        for count in 2..=64 {
            let routing = RoutingCatalog::from_validated_parts(
                count,
                1,
                1,
                1,
                1,
                (0..4096)
                    .map(|bucket| initial_physical_shard(bucket, count))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            );
            // Generation 1 preserves hash % initial shard count, including
            // unequal bucket groups. Expected hash is a frozen literal above.
            assert_eq!(
                u64::from(routing.shard_for_key(key.as_bytes())),
                digest % u64::from(count),
                "{name}/{count}"
            );
        }
    }
}

#[tokio::test]
async fn bson_id_routing_vectors_preserve_physical_records_through_v17_upgrade_and_reopen() {
    let values = values::values();
    let mut records = BTreeMap::new();
    for ((name, value), (expected_name, encoded, _)) in values.iter().zip(golden::VECTORS) {
        assert_eq!(name, expected_name);
        records
            .entry(hex(encoded))
            .or_insert_with(|| doc([("_id", value.clone()), ("vector", BsonValue::from(*name))]));
    }
    assert_eq!(
        records.len(),
        32,
        "numeric/UUID aliases share records; typed distinct IDs do not"
    );
    for shards in [3, 8, 64] {
        let root = tempfile::tempdir().unwrap();
        let mut original = None;
        for phase in 0..3 {
            let engine = Engine::open(root.path(), shards).await.unwrap();
            let session = engine.session();
            if phase == 0 {
                call(
                    &engine,
                    &session,
                    DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                        namespace(),
                        DocumentCollectionOptions::empty(),
                        DocumentWriteOptions::new(),
                    )),
                )
                .await;
                let inserted = call(
                    &engine,
                    &session,
                    DocumentCommand::Insert(
                        DocumentInsertRequest::new(
                            namespace(),
                            records.values().cloned().collect::<Vec<_>>(),
                            DocumentWriteOptions::new(),
                        )
                        .unwrap(),
                    ),
                )
                .await;
                let DocumentResult::Insert(inserted) = inserted.result() else {
                    panic!("insert")
                };
                assert!(inserted.write_errors().is_empty());
                assert_eq!(inserted.inserted_ids().len(), records.len());
            }
            for ((name, value), (_, encoded, digest)) in values.iter().zip(golden::VECTORS) {
                let owner = u16::try_from(digest % u64::from(shards)).unwrap();
                let before = checkouts(&engine);
                let execution = call(
                    &engine,
                    &session,
                    DocumentCommand::Find(DocumentFindRequest::new(
                        namespace(),
                        exact(value.clone()),
                        DocumentReadOptions::new().with_execution_stats(true),
                    )),
                )
                .await;
                assert_owner(&engine, &before, &execution, owner);
                let stats = execution.read_stats().unwrap();
                assert_eq!(
                    (stats.storage_reads(), stats.documents_examined()),
                    (1, 1),
                    "{name}/{phase}"
                );
                assert_eq!(stats.shards_read().collect::<Vec<_>>(), vec![owner]);
                let DocumentResult::Cursor(batch) = execution.result() else {
                    panic!("cursor")
                };
                assert!(batch.is_exhausted());
                assert_eq!(batch.documents().len(), 1, "{name}/{phase}");
                assert_eq!(
                    encode_document(&batch.documents()[0]).unwrap(),
                    encode_document(&records[&hex(encoded)]).unwrap(),
                    "{name}/{phase}"
                );
            }
            let current = image(root.path(), shards);
            for row in &current {
                let (_, _, digest) = golden::VECTORS
                    .iter()
                    .find(|(_, key, _)| hex(key) == row.key)
                    .unwrap();
                assert_eq!(u64::from(row.shard), digest % u64::from(shards));
            }
            if let Some(original) = &original {
                assert_eq!(&current, original);
            } else {
                original = Some(current);
            }
            if phase == 2 {
                for record in records.values() {
                    let id = record.get_first("_id").unwrap();
                    let key = CanonicalBsonKey::encode(id).unwrap();
                    let (_, _, digest) = golden::VECTORS
                        .iter()
                        .find(|(_, encoded, _)| hex(encoded) == key.as_bytes())
                        .unwrap();
                    let owner = u16::try_from(digest % u64::from(shards)).unwrap();
                    let before = checkouts(&engine);
                    let updated = call(
                        &engine,
                        &session,
                        DocumentCommand::Update(DocumentUpdateRequest::new(
                            namespace(),
                            exact(id.clone()),
                            DocumentUpdate::new(doc([(
                                "$set",
                                BsonValue::Document(doc([("touched", BsonValue::Boolean(true))])),
                            )]))
                            .unwrap(),
                            DocumentMutationScope::One,
                            DocumentWriteOptions::new(),
                        )),
                    )
                    .await;
                    assert_owner(&engine, &before, &updated, owner);
                    let DocumentResult::Update(updated) = updated.result() else {
                        panic!("update")
                    };
                    assert_eq!((updated.matched_count(), updated.modified_count()), (1, 1));
                    let before = checkouts(&engine);
                    let deleted = call(
                        &engine,
                        &session,
                        DocumentCommand::Delete(DocumentDeleteRequest::new(
                            namespace(),
                            exact(id.clone()),
                            DocumentMutationScope::One,
                            DocumentWriteOptions::new(),
                        )),
                    )
                    .await;
                    assert_owner(&engine, &before, &deleted, owner);
                    let DocumentResult::Delete(deleted) = deleted.result() else {
                        panic!("delete")
                    };
                    assert_eq!(deleted.deleted_count(), 1);
                }
                assert!(image(root.path(), shards).is_empty());
            }
            drop(session);
            engine.shutdown().await.unwrap();
            drop(engine);
            if phase == 0 {
                synthetic_v17(root.path(), shards);
                assert_eq!(&image(root.path(), shards), original.as_ref().unwrap());
            } else {
                let connection = Connection::open(root.path().join("manifest.sqlite")).unwrap();
                assert_eq!(
                    connection
                        .pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
                        .unwrap(),
                    manifest::CURRENT_SCHEMA_VERSION
                );
            }
        }
    }
}
