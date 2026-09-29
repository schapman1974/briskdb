use super::*;

#[test]
fn startup_audit_ownership_streams_keys_without_record_seeks_or_temporary_sorting() {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(super::super::super::RECORDS_SCHEMA_SQL)
        .unwrap();
    connection.execute_batch(super::super::ENTRIES_SQL).unwrap();
    connection
        .execute_batch(super::super::BY_RECORD_SQL)
        .unwrap();
    for scope in [None, Some(1_i64)] {
        let mut statement = connection
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {}",
                orphan_check_sql(scope.is_some())
            ))
            .unwrap();
        let plan = statement
            .query_map(rusqlite::params_from_iter(scope), |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        // Guard the bundled SQLite's plan; production never parses EXPLAIN.
        assert!(
            plan.iter().any(|step| step.contains("MERGE (EXCEPT)")),
            "{plan:?}"
        );
        assert!(
            plan.iter().all(|step| !step.contains("TEMP B-TREE")),
            "{plan:?}"
        );
        assert!(
            plan.iter().all(|step| !step.contains("id_key=?")),
            "{plan:?}"
        );
    }
}

#[test]
fn startup_audit_ownership_preserves_tuple_identity_and_detects_orphans_at_every_position() {
    for orphan in [b"000000000", b"000000002", b"999999999"] {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
        connection
            .execute_batch(super::super::super::RECORDS_SCHEMA_SQL)
            .unwrap();
        connection.execute_batch(super::super::ENTRIES_SQL).unwrap();
        connection
            .execute_batch(super::super::BY_RECORD_SQL)
            .unwrap();
        for collection in [1, 2] {
            for (order, key) in [b"000000001", b"000000003"].into_iter().enumerate() {
                connection.execute(
                    "INSERT INTO briskdb_documents_v1 VALUES (?1, ?2, ?3, zeroblob(100000), zeroblob(32), 1)",
                    params![collection, key.as_slice(), order as i64 + 1],
                ).unwrap();
                for index in 1..=4 {
                    connection.execute(
                        "INSERT INTO briskdb_document_index_entries_v1 VALUES (?1, ?2, ?3, zeroblob(13), zeroblob(32), 1)",
                        params![collection, index, key.as_slice()],
                    ).unwrap();
                }
            }
        }
        for scope in [None, Some(DocumentCollectionId::from_validated(1))] {
            require_no_orphans(&connection, scope).unwrap();
        }
        connection.execute(
            "INSERT INTO briskdb_document_index_entries_v1 VALUES (1, 1, ?1, zeroblob(13), zeroblob(32), 1)",
            [orphan.as_slice()],
        ).unwrap();
        for scope in [None, Some(DocumentCollectionId::from_validated(1))] {
            assert_eq!(
                require_no_orphans(&connection, scope).unwrap_err().kind(),
                EngineErrorKind::DataCorruption
            );
        }
        require_no_orphans(&connection, Some(DocumentCollectionId::from_validated(2))).unwrap();
        connection
            .execute(
                "DELETE FROM briskdb_document_index_entries_v1 WHERE id_key = ?1",
                [orphan.as_slice()],
            )
            .unwrap();
        // The same key owned by another collection cannot satisfy ownership.
        connection.execute(
            "INSERT INTO briskdb_document_index_entries_v1 VALUES (3, 1, ?1, zeroblob(13), zeroblob(32), 1)",
            [b"000000001".as_slice()],
        ).unwrap();
        assert_eq!(
            require_no_orphans(&connection, None).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
        require_no_orphans(&connection, Some(DocumentCollectionId::from_validated(1))).unwrap();
        assert_eq!(
            require_no_orphans(&connection, Some(DocumentCollectionId::from_validated(3)))
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }
}

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
