use super::*;
use crate::document::{
    BsonDocument, BsonValue, DocumentIndexKeyGenerator, DocumentMatcher, NON_UNIQUE_FALLBACK_KEY,
};
use proptest::prelude::*;
use rusqlite::{Connection, params};

fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}
fn generator() -> DocumentIndexKeyGenerator {
    DocumentIndexKeyGenerator::compile(&doc([("a", BsonValue::Int32(1))]), false, None).unwrap()
}
fn key(value: &str) -> Vec<u8> {
    generator()
        .keys(&doc([("a", BsonValue::from(value))]))
        .unwrap()[0]
        .to_bytes()
        .unwrap()
}
fn connection() -> Connection {
    let mut connection = Connection::open_in_memory().unwrap();
    crate::storage::document::ensure_schema(&mut connection).unwrap();
    connection
}
fn insert(connection: &Connection, ordinal: i64, keys: &[Vec<u8>]) {
    let id = crate::document::CanonicalBsonKey::encode(&BsonValue::Int64(ordinal))
        .unwrap()
        .into_bytes();
    connection
        .execute(
            "INSERT INTO briskdb_documents_v1 VALUES (1,?1,?2,?3,?4,1)",
            params![id, ordinal, vec![0_u8; 5], vec![0_u8; 32]],
        )
        .unwrap();
    for key in keys {
        connection
            .execute(
                "INSERT INTO briskdb_document_index_entries_v1 VALUES (1,1,?1,?2,?3,1)",
                params![id, key, vec![0_u8; 32]],
            )
            .unwrap();
    }
}
fn candidates(
    connection: &Connection,
    bound: &str,
    greater: bool,
    inclusive: bool,
    after: i64,
    limit: i64,
) -> Vec<i64> {
    connection
        .prepare(&string_range(greater, inclusive))
        .unwrap()
        .query_map(
            params![1, after, limit, 1, key(bound), NON_UNIQUE_FALLBACK_KEY],
            |row| row.get(0),
        )
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[test]
fn string_range_sql_matches_encoded_utf8_not_length_prefixes_or_numeric_coercion() {
    let strings = [
        "",
        "\0",
        "a",
        "a\0",
        "a\0z",
        "aa",
        "b",
        "00",
        "1",
        "10",
        "2",
        "'; DROP TABLE x;--",
        "é",
        "λ",
        "中",
        "😀",
        "\u{10ffff}",
    ];
    for string in strings {
        let key = key(string);
        assert_eq!(&key[..13], b"BDIK\0\0\0\x01\0\0\0\x01\x01");
        assert_eq!(&key[17..26], b"BBKY\0\0\0\x01\x03");
        assert_eq!(&key[30..], string.as_bytes());
    }
    let connection = connection();
    for (ordinal, string) in strings.iter().enumerate() {
        insert(&connection, ordinal as i64 + 1, &[key(string)]);
    }
    let numeric = generator()
        .keys(&doc([("a", BsonValue::Int32(10))]))
        .unwrap()[0]
        .to_bytes()
        .unwrap();
    insert(&connection, 100, &[numeric]);
    insert(&connection, 101, &[NON_UNIQUE_FALLBACK_KEY.to_vec()]);
    for analyzed in [false, true] {
        if analyzed {
            connection.execute_batch("ANALYZE").unwrap();
        }
        let plan: Vec<String> = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {}", string_range(true, true)))
            .unwrap()
            .query_map(
                params![1, 0, 1, 1, key("a"), NON_UNIQUE_FALLBACK_KEY],
                |row| row.get(3),
            )
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            plan.first().is_some_and(
                |step| step.starts_with("SEARCH d ") && step.contains("natural_order>?")
            ),
            "{plan:?}"
        );
        assert!(
            plan.iter().all(|step| !step.contains("TEMP B-TREE")),
            "{plan:?}"
        );
    }
    for bound in strings {
        for (greater, inclusive) in [(false, false), (false, true), (true, false), (true, true)] {
            let mut expected: Vec<_> = strings
                .iter()
                .enumerate()
                .filter(|(_, value)| {
                    let comparison = (**value).cmp(bound);
                    if comparison.is_eq() {
                        inclusive
                    } else {
                        comparison.is_gt() == greater
                    }
                })
                .map(|(i, _)| i as i64 + 1)
                .collect();
            expected.push(101);
            assert_eq!(
                candidates(&connection, bound, greater, inclusive, 0, 1000),
                expected
            );
            for after in [0, 5, 101] {
                for limit in [1, 3, 100] {
                    assert_eq!(
                        candidates(&connection, bound, greater, inclusive, after, limit),
                        expected
                            .iter()
                            .copied()
                            .filter(|id| *id > after)
                            .take(limit as usize)
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn string_range_sql_has_no_false_negatives_for_arbitrary_utf8_multikey(
        values in prop::collection::vec(any::<String>(), 0..8),
        bound in any::<String>(), greater in any::<bool>(), inclusive in any::<bool>(),
    ) {
        let record = doc([("a", BsonValue::Array(values.into_iter().map(BsonValue::from).collect()))]);
        let keys = generator().keys(&record).unwrap().into_iter().map(|key| key.to_bytes().unwrap()).collect::<Vec<_>>();
        let connection = connection();
        insert(&connection, 1, &keys);
        let operator = match (greater, inclusive) { (true, false) => "$gt", (true, true) => "$gte", (false, false) => "$lt", (false, true) => "$lte" };
        let query = doc([("a", BsonValue::Document(doc([(operator, BsonValue::from(bound.clone()))])))]);
        let matches = DocumentMatcher::compile(&query).unwrap().matches(&record).unwrap();
        let selected = candidates(&connection, &bound, greater, inclusive, 0, 100);
        prop_assert_eq!(selected, if matches { vec![1] } else { vec![] });
    }
}
