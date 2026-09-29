use super::*;
use rusqlite::{Connection, StatementStatus, types::Value};

fn key(value: u8) -> Vec<u8> {
    vec![value; 13]
}
fn seed(connection: &Connection, collection: i64, ordinal: i64, key: &[u8], id_bytes: usize) {
    let mut id = vec![1; id_bytes];
    id[..8].copy_from_slice(&ordinal.to_be_bytes());
    connection
        .execute(
            "INSERT INTO briskdb_documents_v1 VALUES (?1,?2,?3,zeroblob(5),zeroblob(32),1)",
            params![collection, id, ordinal],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO briskdb_document_index_entries_v1 VALUES (?1,1,?2,?3,zeroblob(32),1)",
            params![collection, id, key],
        )
        .unwrap();
}
fn empty() -> Connection {
    let mut connection = Connection::open_in_memory().unwrap();
    crate::storage::document::ensure_schema(&mut connection).unwrap();
    connection
}
fn rows(
    connection: &Connection,
    sql: &str,
    value: &[u8],
    after: i64,
    limit: i64,
) -> Vec<Vec<Value>> {
    rows_with_work(connection, sql, value, after, limit).0
}
fn rows_with_work(
    connection: &Connection,
    sql: &str,
    value: &[u8],
    after: i64,
    limit: i64,
) -> (Vec<Vec<Value>>, i32) {
    let mut statement = connection.prepare(sql).unwrap();
    let result = statement
        .query_map(params![1, after, limit, 1, value, key(255)], |row| {
            (0..8)
                .map(|i| row.get(i))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    (result, statement.get_status(StatementStatus::VmStep))
}
fn adaptive(connection: &Connection, value: &[u8], after: i64, limit: i64) -> Vec<Vec<Value>> {
    adaptive_with_work(connection, value, after, limit).0
}
fn adaptive_with_work(
    connection: &Connection,
    value: &[u8],
    after: i64,
    limit: i64,
) -> (Vec<Vec<Value>>, i32) {
    let mut statement = connection.prepare(SELECTIVITY_SQL).unwrap();
    let (snapshot, admitted) = selective_snapshot(&mut statement, 1, 1, value, &key(255)).unwrap();
    let (result, scan_work) = rows_with_work(
        connection,
        if admitted {
            selective_single()
        } else {
            single()
        },
        value,
        after,
        limit,
    );
    drop(snapshot);
    (
        result,
        scan_work + statement.get_status(StatementStatus::VmStep),
    )
}

fn admits_selective_single(
    connection: &Connection,
    collection: i64,
    index: i64,
    value: &[u8],
    fallback: &[u8],
) -> rusqlite::Result<bool> {
    let mut statement = connection.prepare(SELECTIVITY_SQL)?;
    let (_snapshot, admitted) =
        selective_snapshot(&mut statement, collection, index, value, fallback)?;
    Ok(admitted)
}

#[test]
fn selective_hits_and_misses_seek_independently_of_collection_and_unrelated_rows() {
    let connection = empty();
    seed(&connection, 1, 100_000, &key(1), 9);
    // Count both statements' actual VM work, excluding statement preparation's
    // schema/statistics loading. This is deterministic, not a wall-clock check.
    let measure = |value: &[u8]| adaptive_with_work(&connection, value, 0, 1);
    let hit = measure(&key(1));
    let miss = measure(&key(2));
    for end in [128, 1024] {
        let transaction = connection.unchecked_transaction().unwrap();
        for ordinal in end..end * 2 {
            seed(&transaction, 1, ordinal, &key(3), 9);
            seed(&transaction, 2, ordinal, &key(1), 9);
        }
        transaction.commit().unwrap();
        for analyze in [false, true] {
            if analyze {
                connection.execute_batch("ANALYZE").unwrap();
            }
            for (value, baseline) in [(key(1), &hit), (key(2), &miss)] {
                let actual = measure(&value);
                assert_eq!(actual.0, baseline.0);
                assert!(
                    actual.1 <= baseline.1 + 50,
                    "selective VM work grew at end={end}, analyze={analyze}: {} -> {}; plan={:?}",
                    baseline.1,
                    actual.1,
                    connection
                        .prepare(&format!("EXPLAIN QUERY PLAN {}", selective_single()))
                        .unwrap()
                        .query_map(params![1, 0, 1, 1, value, key(255)], |row| row
                            .get::<_, String>(3))
                        .unwrap()
                        .collect::<rusqlite::Result<Vec<_>>>()
                        .unwrap()
                );
                let (legacy, legacy_work) = rows_with_work(&connection, single(), &value, 0, 1);
                assert_eq!(legacy, baseline.0);
                assert!(
                    legacy_work > actual.1 * 5,
                    "fixture must exercise the legacy scan"
                );
            }
        }
    }
}

#[test]
fn adaptive_pages_preserve_fallback_entries_order_and_checksum_pairing() {
    let connection = empty();
    for ordinal in [9, 1, 7, 3, 5] {
        seed(
            &connection,
            1,
            ordinal,
            &key(if ordinal == 5 { 255 } else { 1 }),
            9,
        );
        connection.execute("UPDATE briskdb_document_index_entries_v1 SET entry_checksum=?1 WHERE id_key IN (SELECT id_key FROM briskdb_documents_v1 WHERE natural_order=?2)",
            params![vec![ordinal as u8;32], ordinal]).unwrap();
    }
    for after in [0, 1, 4, 7, 9] {
        for limit in [1, 2, 32, 100] {
            assert_eq!(
                adaptive(&connection, &key(1), after, limit),
                rows(&connection, single(), &key(1), after, limit)
            );
            assert_eq!(
                adaptive(&connection, &key(2), after, limit),
                rows(&connection, single(), &key(2), after, limit)
            );
        }
    }
}

#[test]
fn entry_count_and_identity_bytes_bound_selective_frontier_retention() {
    let connection = empty();
    for ordinal in 1..=MAX_SELECTIVE_ENTRIES {
        seed(&connection, 1, ordinal, &key(1), 9);
    }
    assert!(admits_selective_single(&connection, 1, 1, &key(1), &key(255)).unwrap());
    seed(&connection, 1, 100, &key(255), 9);
    assert!(
        !admits_selective_single(&connection, 1, 1, &key(1), &key(255)).unwrap(),
        "fallback consumes the same bound"
    );
    assert_eq!(
        adaptive(&connection, &key(1), 0, 3),
        rows(&connection, single(), &key(1), 0, 3)
    );
    let large = empty();
    seed(&large, 1, 1, &key(1), MAX_SELECTIVE_ID_BYTES as usize);
    assert!(admits_selective_single(&large, 1, 1, &key(1), &key(255)).unwrap());
    seed(&large, 1, 2, &key(1), 9);
    assert!(!admits_selective_single(&large, 1, 1, &key(1), &key(255)).unwrap());
    assert_eq!(
        adaptive(&large, &key(1), 0, 1),
        rows(&large, single(), &key(1), 0, 1)
    );
}

#[test]
fn selective_sort_retains_only_identities_not_document_payloads() {
    let connection = empty();
    for ordinal in 1..=MAX_SELECTIVE_ENTRIES {
        seed(&connection, 1, ordinal, &key(1), 9);
    }
    for analyze in [false, true] {
        if analyze {
            connection.execute_batch("ANALYZE").unwrap();
        }
        for limit in [1, 3, MAX_SELECTIVE_ENTRIES, 100] {
            let plan: Vec<String> = connection
                .prepare(&format!("EXPLAIN QUERY PLAN {}", selective_single()))
                .unwrap()
                .query_map(params![1, 0, limit, 1, key(1), key(255)], |row| row.get(3))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            let frontier_end = plan.iter().position(|step| step == "SCAN f").unwrap();
            assert!(
                plan[..frontier_end]
                    .iter()
                    .any(|step| step.contains("TEMP B-TREE")),
                "{plan:?}"
            );
            assert!(
                plan[frontier_end..]
                    .iter()
                    .all(|step| !step.contains("TEMP B-TREE")),
                "payload fetch must stream after the identity sort: {plan:?}"
            );
        }
    }
}

#[test]
fn selective_probe_and_scan_share_a_snapshot_when_a_writer_expands_the_group() {
    for initially_present in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("snapshot.sqlite");
        let mut reader = Connection::open(&path).unwrap();
        reader.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        crate::storage::document::ensure_schema(&mut reader).unwrap();
        if initially_present {
            seed(&reader, 1, 100, &key(1), 9);
        }
        let writer = Connection::open(&path).unwrap();
        let mut statement = reader.prepare(SELECTIVITY_SQL).unwrap();
        let (snapshot, admitted) =
            selective_snapshot(&mut statement, 1, 1, &key(1), &key(255)).unwrap();
        assert!(admitted);
        let write = writer.unchecked_transaction().unwrap();
        for ordinal in 1..=MAX_SELECTIVE_ENTRIES + 1 {
            seed(&write, 1, ordinal, &key(1), 9);
        }
        write.commit().unwrap();
        let result = rows(&reader, selective_single(), &key(1), 0, 100);
        assert_eq!(result.len(), usize::from(initially_present));
        if initially_present {
            assert_eq!(result[0][0], Value::Integer(100));
        }
        drop(snapshot);
        drop(statement);
        assert!(!admits_selective_single(&reader, 1, 1, &key(1), &key(255)).unwrap());
        assert_eq!(
            adaptive(&reader, &key(1), 0, 100).len(),
            33 + usize::from(initially_present)
        );
    }
}
