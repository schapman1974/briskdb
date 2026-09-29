use super::*;

#[test]
fn scoped_orphan_check_has_constant_work_for_an_unrelated_collection() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(super::super::super::RECORDS_SCHEMA_SQL)
        .unwrap();
    connection.execute_batch(super::super::ENTRIES_SQL).unwrap();
    connection
        .execute_batch(super::super::BY_RECORD_SQL)
        .unwrap();
    let steps = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&steps);
    connection
        .progress_handler(
            1,
            Some(move || {
                counted.fetch_add(1, Ordering::Relaxed);
                false
            }),
        )
        .unwrap();
    let target = DocumentCollectionId::from_validated(2);
    require_no_orphans(&connection, Some(target)).unwrap();
    let empty_steps = steps.swap(0, Ordering::Relaxed);
    // Each unrelated record has four entries, resembling the reported store.
    // The check must not step through any of them, regardless of BSON size.
    for id in 1..=512 {
        let key = format!("{id:09}").into_bytes();
        connection.execute(
            "INSERT INTO briskdb_documents_v1 VALUES (1, ?1, ?2, zeroblob(100000), zeroblob(32), 1)",
            params![key, id],
        ).unwrap();
        for index in 1..=4 {
            connection.execute(
                "INSERT INTO briskdb_document_index_entries_v1 VALUES (1, ?1, ?2, zeroblob(13), zeroblob(32), 1)",
                params![index, key],
            ).unwrap();
        }
    }
    steps.store(0, Ordering::Relaxed);
    require_no_orphans(&connection, Some(target)).unwrap();
    let populated_steps = steps.load(Ordering::Relaxed);
    assert!(
        populated_steps <= empty_steps + 20,
        "unrelated entries expanded VM work: {empty_steps} -> {populated_steps}"
    );
    require_no_orphans(&connection, None).unwrap();
    connection
        .execute_batch("PRAGMA foreign_keys=OFF; DELETE FROM briskdb_documents_v1")
        .unwrap();
    require_no_orphans(&connection, Some(target)).unwrap();
    for scope in [None, Some(DocumentCollectionId::from_validated(1))] {
        assert_eq!(
            require_no_orphans(&connection, scope).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }
}

#[test]
fn entry_checksum_has_frozen_bytes_and_binds_every_identity() {
    let collection = DocumentCollectionId::from_validated(7);
    let index = DocumentIndexId::from_validated(13);
    let record = [0x42; 32];
    let digest = checksum(
        collection,
        index,
        3,
        b"canonical-id",
        b"BDIK-tuple",
        &record,
    );
    let hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(
        hex,
        "67e8e7a61c0605511b9e71389967e2243cafc0821c9706a060f6ffcaa1da2dd4"
    );
    for other in [
        checksum(
            DocumentCollectionId::from_validated(8),
            index,
            3,
            b"canonical-id",
            b"BDIK-tuple",
            &record,
        ),
        checksum(
            collection,
            DocumentIndexId::from_validated(14),
            3,
            b"canonical-id",
            b"BDIK-tuple",
            &record,
        ),
        checksum(
            collection,
            index,
            4,
            b"canonical-id",
            b"BDIK-tuple",
            &record,
        ),
        checksum(
            collection,
            index,
            3,
            b"canonical-ie",
            b"BDIK-tuple",
            &record,
        ),
        checksum(
            collection,
            index,
            3,
            b"canonical-id",
            b"BDIK-tuplf",
            &record,
        ),
        checksum(
            collection,
            index,
            3,
            b"canonical-id",
            b"BDIK-tuple",
            &[0x43; 32],
        ),
    ] {
        assert_ne!(other, digest);
    }
}
