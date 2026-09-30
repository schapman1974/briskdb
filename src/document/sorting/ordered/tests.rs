use super::*;
use crate::{
    core::{EngineError, EngineErrorKind},
    document::{
        BsonBinary, BsonDateTime, BsonDecimal128, BsonJavaScript, BsonObjectId, BsonRegex,
        BsonTimestamp, DocumentSorter,
    },
};
use proptest::prelude::*;

fn doc(fields: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}

fn key(value: BsonValue, descending: bool) -> DocumentSortKey {
    // Bypass array member selection here: compare actual owned key atoms. The
    // separate compiled-sorter tests exercise path/array selection end to end.
    DocumentSortKey {
        components: vec![(descending, Atom::Value(value))],
        retained_bytes: 0,
    }
}

fn scalar() -> impl Strategy<Value = BsonValue> {
    prop_oneof![
        Just(BsonValue::MinKey),
        Just(BsonValue::MaxKey),
        Just(BsonValue::Null),
        any::<i32>().prop_map(BsonValue::Int32),
        any::<i64>().prop_map(BsonValue::Int64),
        any::<u64>().prop_map(|bits| BsonValue::Double(f64::from_bits(bits))),
        any::<[u8; 16]>().prop_map(|bytes| BsonValue::Decimal128(BsonDecimal128::from_bid(bytes))),
        prop::collection::vec(any::<char>(), 0..20)
            .prop_map(|chars| BsonValue::String(chars.into_iter().collect())),
        (any::<u8>(), prop::collection::vec(any::<u8>(), 0..20))
            .prop_map(|(subtype, bytes)| BsonValue::Binary(BsonBinary::new(subtype, bytes))),
        any::<[u8; 12]>().prop_map(|bytes| BsonValue::ObjectId(BsonObjectId::from_bytes(bytes))),
        any::<bool>().prop_map(BsonValue::Boolean),
        any::<i64>().prop_map(|v| BsonValue::DateTime(BsonDateTime::from_millis(v))),
        (any::<u32>(), any::<u32>())
            .prop_map(|(t, i)| BsonValue::Timestamp(BsonTimestamp::new(t, i))),
    ]
}

fn value() -> impl Strategy<Value = BsonValue> {
    scalar().prop_recursive(4, 96, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..6).prop_map(BsonValue::Array),
            prop::collection::vec(("[a-c]{0,3}", inner), 0..6).prop_map(|fields| {
                BsonValue::Document(BsonDocument::from_entries(fields).unwrap())
            }),
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]
    #[test]
    fn bytes_match_bson_order_for_recursive_values(left in value(), right in value(), descending in any::<bool>()) {
        let left = key(left, descending);
        let right = key(right, descending);
        prop_assert_eq!(left.ordered_bytes().unwrap().cmp(&right.ordered_bytes().unwrap()), left.cmp(&right));
    }

    #[test]
    fn exact_numeric_order_and_cohorts_survive_encoding(
        int in any::<i64>(), bits in any::<u64>(), bid in any::<[u8;16]>()
    ) {
        let values = [BsonValue::Int64(int), BsonValue::Double(f64::from_bits(bits)),
            BsonValue::Decimal128(BsonDecimal128::from_bid(bid))];
        for left in &values {
            for right in &values {
                prop_assert_eq!(key(left.clone(), false).ordered_bytes().unwrap()
                    .cmp(&key(right.clone(), false).ordered_bytes().unwrap()), left.cmp(right));
            }
        }
    }
}

#[test]
fn prefix_strings_nuls_extrema_and_all_special_families_order_exactly() {
    let mut values = vec![BsonValue::MinKey, BsonValue::Null, BsonValue::MaxKey];
    for string in ["", "\0", "\0\0", "\0a", "a", "a\0", "a\0b", "aa", "é", "😀"] {
        values.push(BsonValue::String(string.into()));
        values.push(BsonValue::JavaScript(BsonJavaScript::new(string)));
        values.push(BsonValue::JavaScript(BsonJavaScript::with_scope(
            string,
            doc([]),
        )));
    }
    for n in [
        f64::NAN,
        f64::NEG_INFINITY,
        -f64::MAX,
        -1.01,
        -1.0,
        -0.0,
        0.0,
        f64::from_bits(1),
        f64::MIN_POSITIVE,
        0.1,
        1.0,
        1.01,
        f64::MAX,
        f64::INFINITY,
    ] {
        values.push(BsonValue::Double(n));
    }
    for n in [i64::MIN, -1, 0, 1, 10, i64::MAX] {
        values.push(BsonValue::Int64(n));
    }
    for (pattern, flags) in [("", ""), ("a", ""), ("a", "i"), ("aa", "ms")] {
        values.push(BsonValue::RegularExpression(
            BsonRegex::new(pattern, flags).unwrap(),
        ));
    }
    values.extend([
        BsonValue::Array(vec![]),
        BsonValue::Array(vec![BsonValue::MinKey]),
        BsonValue::Document(doc([])),
        BsonValue::Document(doc([("", BsonValue::MinKey)])),
        BsonValue::Document(doc([("z", BsonValue::Int32(1)), ("z", BsonValue::Null)])),
        BsonValue::Document(doc([("a", BsonValue::String("".into()))])),
        BsonValue::Binary(BsonBinary::new(2, [0; 4])),
        BsonValue::Binary(BsonBinary::new(0, [0; 8])),
    ]);
    for descending in [false, true] {
        let mut keys: Vec<_> = values.iter().cloned().map(|v| key(v, descending)).collect();
        keys.push(DocumentSortKey {
            components: vec![(descending, Atom::EmptyArray)],
            retained_bytes: 0,
        });
        for left in &keys {
            for right in &keys {
                assert_eq!(
                    left.ordered_bytes()
                        .unwrap()
                        .cmp(&right.ordered_bytes().unwrap()),
                    left.cmp(right)
                );
            }
        }
    }
}

#[test]
fn compiled_compound_and_array_keys_preserve_direction_selection_and_ties() {
    let documents = [
        doc([("a", BsonValue::Array(vec![])), ("b", BsonValue::Int32(1))]),
        doc([
            (
                "a",
                BsonValue::Array(vec![BsonValue::Int32(2), BsonValue::Int32(1)]),
            ),
            ("b", BsonValue::Int32(2)),
        ]),
        doc([
            ("a", BsonValue::Int32(1)),
            ("b", BsonValue::String("a\0".into())),
        ]),
        doc([
            ("a", BsonValue::Double(1.0)),
            ("b", BsonValue::String("a".into())),
        ]),
        doc([("b", BsonValue::Null)]),
    ];
    let mut keys = Vec::new();
    for a in [1, -1] {
        for b in [1, -1] {
            let sorter = DocumentSorter::compile(&doc([
                ("a", BsonValue::Int32(a)),
                ("b", BsonValue::Int32(b)),
            ]))
            .unwrap();
            keys.extend(
                documents
                    .iter()
                    .map(|document| sorter.key(document).unwrap()),
            );
            let sorter = DocumentSorter::compile(&doc([("a", BsonValue::Int32(a))])).unwrap();
            keys.extend(
                documents
                    .iter()
                    .map(|document| sorter.key(document).unwrap()),
            );
        }
    }
    for left in &keys {
        for right in &keys {
            assert_eq!(
                left.ordered_bytes()
                    .unwrap()
                    .cmp(&right.ordered_bytes().unwrap()),
                left.cmp(right)
            );
        }
    }
    assert_eq!(
        key(BsonValue::Int32(1), false).ordered_bytes().unwrap(),
        key(BsonValue::Double(1.0), false).ordered_bytes().unwrap()
    );
}

#[test]
fn decimal_quantums_and_binary_rounding_keep_exact_numeric_order() {
    let decimal = |coefficient: u128, exponent: i32, negative: bool| {
        let bid =
            coefficient | (((exponent + 6176) as u128) << 113) | (u128::from(negative) << 127);
        BsonValue::Decimal128(BsonDecimal128::from_bid(bid.to_le_bytes()))
    };
    for negative in [false, true] {
        let integer = BsonValue::Int64(if negative { -1 } else { 1 });
        for (coefficient, exponent) in [(1, 0), (10, -1), (100, -2)] {
            assert_eq!(
                key(integer.clone(), false).ordered_bytes().unwrap(),
                key(decimal(coefficient, exponent, negative), false)
                    .ordered_bytes()
                    .unwrap()
            );
        }
    }
    let tenth = decimal(1, -1, false);
    let binary_tenth = BsonValue::Double(0.1);
    assert!(tenth < binary_tenth);
    assert!(
        key(tenth, false).ordered_bytes().unwrap()
            < key(binary_tenth, false).ordered_bytes().unwrap()
    );
    let values = [
        decimal(1, -6176, false),
        BsonValue::Double(f64::from_bits(1)),
        BsonValue::Int64(1),
        BsonValue::Double(f64::MAX),
        decimal(1, 6111, false),
    ];
    for pair in values.windows(2) {
        assert!(pair[0] < pair[1]);
        assert!(
            key(pair[0].clone(), false).ordered_bytes().unwrap()
                < key(pair[1].clone(), false).ordered_bytes().unwrap()
        );
    }
}

#[test]
fn encoding_is_versioned_bounded_cancellable_and_payload_free_on_error() {
    let encoded = key(BsonValue::String("a\0".into()), false)
        .ordered_bytes()
        .unwrap();
    assert_eq!(
        encoded,
        [
            b'B', b'B', b'S', b'O', 0, 0, 0, 1, 1, 0, 5, b'a', 0, 255, 0, 0, 0
        ]
    );
    let large = key(BsonValue::String("secret".repeat(5000)), true);
    let mut checks = 0;
    let error = large
        .ordered_bytes_with_check(&mut || {
            checks += 1;
            if checks >= 12 {
                Err(EngineError::new(
                    EngineErrorKind::Cancelled,
                    "test cancelled",
                ))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Cancelled);
    assert_eq!(checks, 12);
    assert!(!format!("{error:?}").contains("secret"));
    let too_big = key(BsonValue::String("\0".repeat(MAX_BYTES / 2)), false);
    assert_eq!(
        too_big.ordered_bytes().unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
}
