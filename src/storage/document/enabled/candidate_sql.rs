//! Bounded, naturally ordered index candidates for the shard merge frontier.

pub(super) fn single() -> &'static str {
    // Stale statistics can underestimate a large null/missing-key group. Keep
    // the natural-order range outermost instead of sorting the complete group
    // for each one-record frontier. Normal keys and BDIF are mutually exclusive
    // record representations, so a singleton probe needs no extra grouping.
    "SELECT d.natural_order, d.id_key, d.document_bson, d.document_checksum,
            d.storage_format_version, e.entry_checksum, e.entry_format_version,
            e.index_key
     FROM briskdb_documents_v1 AS d
     CROSS JOIN briskdb_document_index_entries_v1 AS e
       ON e.collection_id = d.collection_id AND e.id_key = d.id_key
     WHERE d.collection_id = ?1 AND d.natural_order > ?2
       AND e.index_id = ?4 AND (e.index_key = ?5 OR e.index_key = ?6)
     ORDER BY d.natural_order LIMIT ?3"
}

pub(super) fn membership(key_count: usize) -> String {
    // Only bounded placeholder numbers are generated, never BSON values. The
    // final parameter is the conservative fallback key.
    let placeholders = (5..=5 + key_count)
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(",");
    grouped(&format!(" AND e.index_key IN ({placeholders})"))
}

pub(super) fn sparse() -> String {
    grouped("")
}

fn grouped(key_filter: &str) -> String {
    // CROSS JOIN keeps the document natural-order range as the outer loop even
    // after ANALYZE. An index-first join can materialize every matching entry in
    // a temporary GROUP BY tree for each one-record merge frontier.
    //
    // Group BEFORE LIMIT so a multikey document appears once. SQLite evaluates
    // all bare result columns from the same group row, keeping the selected
    // entry/checksum paired. natural_order is unique in the admitted collection.
    format!(
        "SELECT d.natural_order, d.id_key, d.document_bson, d.document_checksum,
                d.storage_format_version, e.entry_checksum, e.entry_format_version,
                e.index_key
         FROM briskdb_documents_v1 AS d
         CROSS JOIN briskdb_document_index_entries_v1 AS e
           ON e.collection_id = d.collection_id AND e.id_key = d.id_key
         WHERE d.collection_id = ?1 AND d.natural_order > ?2
           AND e.index_id = ?4{key_filter}
         GROUP BY d.natural_order ORDER BY d.natural_order LIMIT ?3"
    )
}

#[cfg(test)]
mod tests {
    use rusqlite::{Connection, params, params_from_iter, types::Value};

    fn key(value: u8) -> Vec<u8> {
        vec![value; 13]
    }

    #[test]
    fn single_key_frontiers_remain_streaming_after_statistics_become_stale() {
        let mut connection = Connection::open_in_memory().unwrap();
        crate::storage::document::ensure_schema(&mut connection).unwrap();
        for hot in [false, true] {
            let transaction = connection.transaction().unwrap();
            for ordinal in if hot { 1_001..6_001_i64 } else { 1..1_001_i64 } {
                let id = [vec![1], ordinal.to_be_bytes().to_vec()].concat();
                let entry = if hot {
                    key(0)
                } else {
                    [vec![1; 5], ordinal.to_be_bytes().to_vec()].concat()
                };
                transaction
                    .execute(
                        "INSERT INTO briskdb_documents_v1 VALUES (1,?1,?2,?3,?4,1)",
                        params![id, ordinal, vec![0_u8; 5], vec![0_u8; 32]],
                    )
                    .unwrap();
                transaction
                    .execute(
                        "INSERT INTO briskdb_document_index_entries_v1 VALUES (1,1,?1,?2,?3,1)",
                        params![id, entry, vec![0_u8; 32]],
                    )
                    .unwrap();
            }
            transaction.commit().unwrap();
            if !hot {
                connection.execute_batch("ANALYZE").unwrap();
            }
        }
        // Statistics describe rare distinct keys, but later ordinary writes
        // have created a large null/missing-key group. LIMIT 1 must not require
        // materializing and sorting that complete group for every frontier.
        for analyzed in [false, true] {
            if analyzed {
                connection.execute_batch("ANALYZE").unwrap();
            }
            let plan: Vec<String> = connection
                .prepare(&format!("EXPLAIN QUERY PLAN {}", super::single()))
                .unwrap()
                .query_map(params![1, 0, 1, 1, key(0), key(255)], |row| row.get(3))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert!(
                plan.iter().all(|step| !step.contains("TEMP B-TREE")),
                "analyzed={analyzed}: {plan:?}"
            );
            assert!(
                plan.first().is_some_and(
                    |step| step.starts_with("SEARCH d ") && step.contains("natural_order>?")
                ),
                "{plan:?}"
            );
            for after in [0, 1_001, 3_500, 6_000] {
                for limit in [1, 3, 17] {
                    let actual: Vec<i64> = connection
                        .prepare(super::single())
                        .unwrap()
                        .query_map(params![1, after, limit, 1, key(0), key(255)], |row| {
                            row.get(0)
                        })
                        .unwrap()
                        .collect::<Result<_, _>>()
                        .unwrap();
                    let expected: Vec<_> = (1_001..6_001)
                        .filter(|value| *value > after)
                        .take(limit as usize)
                        .collect();
                    assert_eq!(actual, expected);
                }
            }
        }
    }

    #[test]
    fn grouped_index_pages_stream_without_temporary_grouping_before_and_after_analyze() {
        let mut connection = Connection::open_in_memory().unwrap();
        crate::storage::document::ensure_schema(&mut connection).unwrap();
        {
            let transaction = connection.transaction().unwrap();
            // Schema-valid raw bytes isolate the SQL planner/row-pairing test
            // from BSON/key encoding, which has separate engine-level oracles.
            for ordinal in 0..1_000_i64 {
                let id = [vec![1], ordinal.to_be_bytes().to_vec()].concat();
                transaction
                    .execute(
                        "INSERT INTO briskdb_documents_v1 VALUES (1,?1,?2,?3,?4,1)",
                        params![id, ordinal + 1, vec![0_u8; 5], vec![0_u8; 32]],
                    )
                    .unwrap();
                // Some records are absent from this sparse-style index.
                if ordinal % 5 == 0 {
                    continue;
                }
                for value in [ordinal % 128, (ordinal + 1) % 128] {
                    transaction
                        .execute(
                            "INSERT INTO briskdb_document_index_entries_v1
                             VALUES (1,1,?1,?2,?3,1)",
                            params![id, key(value as u8), vec![value as u8; 32]],
                        )
                        .unwrap();
                }
            }
            transaction.commit().unwrap();
        }
        for analyzed in [false, true] {
            if analyzed {
                connection.execute_batch("ANALYZE").unwrap();
            }
            for key_count in [Some(2), Some(64), Some(128), None] {
                let sql = key_count.map_or_else(super::sparse, super::membership);
                for (after, limit) in [(0, 1), (0, 3), (17, 7), (998, 3)] {
                    let mut parameters: Vec<_> = [1, after, limit, 1]
                        .into_iter()
                        .map(Value::Integer)
                        .collect();
                    if let Some(key_count) = key_count {
                        parameters
                            .extend((0..key_count).map(|value| Value::Blob(key(value as u8))));
                        parameters.push(Value::Blob(key(255)));
                    }
                    let plan: Vec<String> = connection
                        .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                        .unwrap()
                        .query_map(params_from_iter(&parameters), |row| row.get(3))
                        .unwrap()
                        .collect::<Result<_, _>>()
                        .unwrap();
                    assert!(
                        plan.first().is_some_and(|step| {
                            step.starts_with("SEARCH d ") && step.contains("natural_order>?")
                        }),
                        "document-first ordered range: analyzed={analyzed}, keys={key_count:?}: {plan:?}"
                    );
                    assert!(
                        plan.iter().all(|step| !step.contains("TEMP B-TREE")),
                        "no all-candidate sort/group: analyzed={analyzed}, keys={key_count:?}: {plan:?}"
                    );
                    let rows: Vec<(i64, Vec<u8>, Vec<u8>)> = connection
                        .prepare(&sql)
                        .unwrap()
                        .query_map(params_from_iter(&parameters), |row| {
                            Ok((row.get(0)?, row.get(7)?, row.get(5)?))
                        })
                        .unwrap()
                        .collect::<Result<_, _>>()
                        .unwrap();
                    let expected: Vec<_> = (0..1_000_i64)
                        .filter(|ordinal| {
                            ordinal + 1 > after
                                && ordinal % 5 != 0
                                && key_count.is_none_or(|key_count| {
                                    ordinal % 128 < key_count as i64
                                        || (ordinal + 1) % 128 < key_count as i64
                                })
                        })
                        .take(limit as usize)
                        .map(|ordinal| ordinal + 1)
                        .collect();
                    assert_eq!(rows.iter().map(|row| row.0).collect::<Vec<_>>(), expected);
                    for (natural_order, index_key, checksum) in rows {
                        assert!(
                            key_count.is_none_or(|key_count| (index_key[0] as usize) < key_count)
                        );
                        assert!(
                            index_key[0] == ((natural_order - 1) % 128) as u8
                                || index_key[0] == (natural_order % 128) as u8
                        );
                        assert_eq!(checksum, vec![index_key[0]; 32]);
                    }
                }
            }
        }
    }
}
