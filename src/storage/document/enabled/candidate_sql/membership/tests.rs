use super::*;
use rusqlite::{Connection, StatementStatus, params, types::Value};

fn key(value: u8) -> Vec<u8> {
    vec![value; 13]
}

fn empty() -> Connection {
    let mut connection = Connection::open_in_memory().unwrap();
    crate::storage::document::ensure_schema(&mut connection).unwrap();
    connection
}

fn seed(connection: &Connection, collection: i64, ordinal: i64, keys: &[Vec<u8>]) {
    let id = [vec![1], ordinal.to_be_bytes().to_vec()].concat();
    connection
        .execute(
            "INSERT INTO briskdb_documents_v1 VALUES (?1,?2,?3,zeroblob(5),zeroblob(32),1)",
            params![collection, id, ordinal],
        )
        .unwrap();
    for key in keys {
        connection
            .execute(
                "INSERT INTO briskdb_document_index_entries_v1 VALUES (?1,1,?2,?3,?4,1)",
                params![collection, id, key, vec![key[0]; 32]],
            )
            .unwrap();
    }
}

fn parameters(keys: &[Vec<u8>], after: i64, limit: i64) -> Vec<Value> {
    [1, after, limit, 1]
        .into_iter()
        .map(Value::Integer)
        .chain(keys.iter().cloned().map(Value::Blob))
        .chain(std::iter::once(Value::Blob(key(255))))
        .collect()
}

fn rows(
    connection: &Connection,
    sql: &str,
    keys: &[Vec<u8>],
    after: i64,
    limit: i64,
) -> (Vec<Vec<Value>>, i32) {
    let mut statement = connection.prepare(sql).unwrap();
    let rows = statement
        .query_map(params_from_iter(parameters(keys, after, limit)), |row| {
            (0..8)
                .map(|i| row.get(i))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    (rows, statement.get_status(StatementStatus::VmStep))
}

fn adaptive(
    connection: &Connection,
    keys: &[Vec<u8>],
    after: i64,
    limit: i64,
) -> (Vec<Vec<Value>>, i32, bool) {
    let mut statement = connection
        .prepare(&membership_selectivity_sql(keys.len()))
        .unwrap();
    let (snapshot, admitted) =
        selective_membership_snapshot(&mut statement, 1, 1, keys, &key(255)).unwrap();
    let sql = if admitted {
        selective_membership(keys.len())
    } else {
        super::super::membership(keys.len())
    };
    let (rows, work) = rows(connection, &sql, keys, after, limit);
    drop(snapshot);
    (
        rows,
        work + statement.get_status(StatementStatus::VmStep),
        admitted,
    )
}

#[test]
fn selective_membership_hits_and_misses_do_not_walk_unrelated_rows() {
    let connection = empty();
    for (ordinal, value) in [(100_000, 1), (100_001, 2), (100_002, 3)] {
        seed(&connection, 1, ordinal, &[key(value)]);
    }
    let hit_keys = vec![key(1), key(2), key(3)];
    let miss_keys = vec![key(4), key(5), key(6)];
    let hit = adaptive(&connection, &hit_keys, 0, 32);
    let miss = adaptive(&connection, &miss_keys, 0, 32);
    for end in [128, 1024] {
        let transaction = connection.unchecked_transaction().unwrap();
        for ordinal in end..end * 2 {
            seed(&transaction, 1, ordinal, &[key(0)]);
            seed(&transaction, 2, ordinal, &hit_keys);
        }
        transaction.commit().unwrap();
        for analyze in [false, true] {
            if analyze {
                connection.execute_batch("ANALYZE").unwrap();
            }
            for (keys, baseline) in [(&hit_keys, &hit), (&miss_keys, &miss)] {
                let actual = adaptive(&connection, keys, 0, 32);
                assert!(actual.2);
                assert_eq!(actual.0, baseline.0);
                assert!(
                    actual.1 <= baseline.1 + 50,
                    "end={end}, analyze={analyze}: {} -> {}",
                    baseline.1,
                    actual.1
                );
                let legacy = rows(
                    &connection,
                    &super::super::membership(keys.len()),
                    keys,
                    0,
                    32,
                );
                assert_eq!(actual.0, legacy.0);
                assert!(
                    legacy.1 > actual.1 * 2,
                    "fixture must expose the legacy scan"
                );
            }
        }
    }
}

#[test]
fn membership_pages_deduplicate_before_limit_and_pair_the_selected_checksum() {
    let connection = empty();
    seed(&connection, 1, 9, &[key(1), key(2), key(3)]);
    seed(&connection, 1, 1, &[key(2), key(3)]);
    seed(&connection, 1, 5, &[key(255)]);
    seed(&connection, 1, 3, &[key(1)]);
    for count in [2, 3, 128] {
        let keys = (1..=count).map(key).collect::<Vec<_>>();
        for after in [0, 1, 4, 8, 9] {
            for limit in [1, 2, 32, 100] {
                let actual = adaptive(&connection, &keys, after, limit);
                assert!(actual.2);
                let legacy = rows(
                    &connection,
                    &super::super::membership(keys.len()),
                    &keys,
                    after,
                    limit,
                );
                assert_eq!(
                    actual.0.iter().map(|row| &row[..5]).collect::<Vec<_>>(),
                    legacy.0.iter().map(|row| &row[..5]).collect::<Vec<_>>()
                );
                for row in actual.0 {
                    let Value::Blob(ref chosen) = row[7] else {
                        panic!("expected selected key");
                    };
                    assert_eq!(row[5], Value::Blob(vec![chosen[0]; 32]));
                }
            }
        }
    }
}

#[test]
fn membership_entry_count_and_identity_plus_key_bytes_bound_retention() {
    let connection = empty();
    let keys = vec![key(1), key(2), key(3)];
    for ordinal in 1..=MAX_SELECTIVE_ENTRIES * 3 {
        seed(&connection, 1, ordinal, &[key(1)]);
    }
    assert!(adaptive(&connection, &keys, 0, 1).2);
    seed(&connection, 1, 1000, &[key(255)]);
    assert!(
        !adaptive(&connection, &keys, 0, 1).2,
        "fallback counts toward the same limit"
    );
    let large = empty();
    let large_keys = vec![vec![1; MAX_SELECTIVE_ID_BYTES as usize - 9], key(2)];
    seed(&large, 1, 1, &large_keys[..1]);
    assert!(adaptive(&large, &large_keys, 0, 1).2);
    seed(&large, 1, 2, &[key(2)]);
    assert!(
        !adaptive(&large, &large_keys, 0, 1).2,
        "representative keys must be charged too"
    );
}

#[test]
fn membership_sort_groups_only_bounded_metadata_then_streams_bson_fetches() {
    let connection = empty();
    let keys = vec![key(1), key(2), key(3)];
    for ordinal in 1..=16 {
        seed(&connection, 1, ordinal, &keys);
    }
    for analyze in [false, true] {
        if analyze {
            connection.execute_batch("ANALYZE").unwrap();
        }
        let plan = connection
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {}",
                selective_membership(keys.len())
            ))
            .unwrap()
            .query_map(params_from_iter(parameters(&keys, 0, 3)), |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
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
            "BSON fetch must stream: {plan:?}"
        );
    }
}

#[test]
fn membership_probe_and_grouping_share_a_snapshot_across_a_writer() {
    for initial_hit in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("snapshot.sqlite");
        let mut reader = Connection::open(&path).unwrap();
        reader.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        crate::storage::document::ensure_schema(&mut reader).unwrap();
        let keys = vec![key(1), key(2), key(3)];
        if initial_hit {
            seed(&reader, 1, 1000, &keys);
        }
        let writer = Connection::open(&path).unwrap();
        let mut statement = reader
            .prepare(&membership_selectivity_sql(keys.len()))
            .unwrap();
        let (snapshot, admitted) =
            selective_membership_snapshot(&mut statement, 1, 1, &keys, &key(255)).unwrap();
        assert!(admitted);
        let transaction = writer.unchecked_transaction().unwrap();
        for ordinal in 1..=100 {
            seed(&transaction, 1, ordinal, &keys);
        }
        transaction.commit().unwrap();
        let actual = rows(&reader, &selective_membership(keys.len()), &keys, 0, 1000);
        assert_eq!(actual.0.len(), usize::from(initial_hit));
        drop(snapshot);
        assert!(reader.is_autocommit());
        let fresh = adaptive(&reader, &keys, 0, 1000);
        assert!(!fresh.2);
        assert_eq!(fresh.0.len(), 100 + usize::from(initial_hit));
    }
}
