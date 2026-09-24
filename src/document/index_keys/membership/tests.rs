use super::*;
use crate::document::{BsonBinary, BsonDecimal128, BsonObjectId, BsonRegex};

mod absence;
mod logical;

fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}

fn object<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonValue {
    BsonValue::Document(doc(fields))
}

fn member(values: Vec<BsonValue>) -> BsonValue {
    object([("$in", BsonValue::Array(values))])
}

fn generator(compound: bool, sparse: bool) -> DocumentIndexKeyGenerator {
    let mut spec = doc([("a", BsonValue::Int32(1))]);
    if compound {
        spec.push("b", BsonValue::Int32(-1)).unwrap();
    }
    DocumentIndexKeyGenerator::compile(&spec, sparse, None).unwrap()
}

fn keys(
    generator: &DocumentIndexKeyGenerator,
    query: &BsonDocument,
) -> Option<Vec<DocumentIndexKey>> {
    let matcher = DocumentMatcher::compile(query).unwrap();
    generator
        .membership_keys_with_budget(&matcher, &mut Budget::new(&mut || Ok(())))
        .unwrap()
}

#[test]
fn necessary_membership_tuples_have_no_false_negatives_for_scalar_multikey_and_sparse_records() {
    let scalars = [
        BsonValue::Null,
        BsonValue::Boolean(true),
        BsonValue::Int32(1),
        BsonValue::Int64(1),
        BsonValue::Double(1.0),
        BsonValue::Decimal128(BsonDecimal128::parse("1.00").unwrap()),
        BsonValue::Int64(9_007_199_254_740_993),
        BsonValue::from("1"),
        BsonValue::Binary(BsonBinary::new(128, vec![1, 2])),
    ];
    let mut documents = vec![doc([]), doc([("b", BsonValue::Int32(1))])];
    for left in &scalars {
        documents.push(doc([("a", left.clone()), ("b", BsonValue::Int32(1))]));
        for right in &scalars {
            documents.push(doc([
                (
                    "a",
                    BsonValue::Array(vec![left.clone(), right.clone(), left.clone()]),
                ),
                ("b", BsonValue::Int32(1)),
            ]));
        }
    }
    documents.push(doc([
        ("a", BsonValue::Array(vec![])),
        ("b", BsonValue::Int32(1)),
    ]));
    for compound in [false, true] {
        for sparse in [false, true] {
            let generator = generator(compound, sparse);
            for left in &scalars {
                for right in &scalars {
                    let query = doc([(
                        "$and",
                        BsonValue::Array(vec![
                            object([(
                                "a",
                                member(vec![right.clone(), left.clone(), right.clone()]),
                            )]),
                            object([("b", member(vec![BsonValue::Int64(1), BsonValue::Int32(2)]))]),
                        ]),
                    )]);
                    let matcher = DocumentMatcher::compile(&query).unwrap();
                    let Some(probes) = keys(&generator, &query) else {
                        assert!(
                            sparse
                                && !compound
                                && (matches!(left, BsonValue::Null)
                                    || matches!(right, BsonValue::Null))
                        );
                        continue;
                    };
                    assert!(!probes.is_empty() && probes.len() <= 4);
                    assert_eq!(probes.iter().collect::<HashSet<_>>().len(), probes.len());
                    for document in &documents {
                        if matcher.matches(document).unwrap() {
                            let stored = generator.keys(document).unwrap();
                            assert!(
                                stored.iter().any(|key| probes.contains(key)),
                                "query={query:?}, document={document:?}, sparse={sparse}, compound={compound}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn unknown_negative_partial_incomplete_and_sparse_null_shapes_keep_scans() {
    let one = BsonValue::Int32(1);
    for value in [
        member(vec![]),
        member(vec![one.clone(); MAX_PROBE_KEYS + 1]),
        member(vec![BsonValue::Array(vec![one.clone()])]),
        member(vec![object([("value", one.clone())])]),
        member(vec![BsonValue::ObjectId(BsonObjectId::from_bytes([1; 12]))]),
        member(vec![BsonValue::Double(f64::NAN)]),
        member(vec![
            one.clone(),
            BsonValue::RegularExpression(BsonRegex::new("1", "").unwrap()),
        ]),
        object([("$nin", BsonValue::Array(vec![one.clone()]))]),
        object([("$not", member(vec![one.clone()]))]),
    ] {
        assert!(keys(&generator(false, false), &doc([("a", value)])).is_none());
    }
    let clause = object([("a", member(vec![one.clone()]))]);
    for op in ["$or", "$nor"] {
        assert!(
            keys(
                &generator(false, false),
                &doc([(op, BsonValue::Array(vec![clause.clone()]))])
            )
            .is_none()
        );
    }
    assert!(
        keys(
            &generator(true, false),
            &doc([("a", member(vec![one.clone()]))])
        )
        .is_none()
    );
    let nulls = doc([
        ("a", member(vec![BsonValue::Null, one.clone()])),
        ("b", member(vec![BsonValue::Null, one.clone()])),
    ]);
    assert!(keys(&generator(true, true), &nulls).is_none());
    assert_eq!(keys(&generator(true, false), &nulls).unwrap().len(), 4);
    let partial = DocumentIndexKeyGenerator::compile(
        &doc([("a", one.clone())]),
        false,
        Some(&doc([("enabled", BsonValue::Boolean(true))])),
    )
    .unwrap();
    assert!(
        keys(
            &partial,
            &doc([
                ("a", member(vec![one])),
                ("enabled", BsonValue::Boolean(true))
            ])
        )
        .is_none()
    );
}

#[test]
fn equality_precedes_membership_and_numeric_duplicates_do_not_expand_products() {
    let one = BsonValue::Int32(1);
    let alias = BsonValue::Double(1.0);
    let generator = generator(true, false);
    let query = doc([
        ("a", member(vec![one.clone(), alias.clone(), one.clone()])),
        ("b", member(vec![BsonValue::Int32(2), BsonValue::Int64(2)])),
    ]);
    let probes = keys(&generator, &query).unwrap();
    assert_eq!(
        probes,
        generator
            .keys(&doc([("a", one.clone()), ("b", BsonValue::Int32(2))]))
            .unwrap()
    );
    // The public single-equality inference remains unchanged for the frozen
    // probe oracle; only the internal optional planner gets list candidates.
    assert!(
        generator
            .equality_key(&DocumentMatcher::compile(&query).unwrap())
            .unwrap()
            .is_none()
    );
    let query = doc([
        (
            "a",
            object([
                ("$in", BsonValue::Array(vec![alias, BsonValue::Int32(2)])),
                ("$eq", one.clone()),
            ]),
        ),
        ("b", member(vec![BsonValue::Int32(2)])),
    ]);
    assert_eq!(keys(&generator, &query).unwrap(), probes);
}

#[test]
fn repeated_memberships_on_an_array_are_necessary_witnesses_not_intersections() {
    let generator = generator(false, false);
    let query = doc([(
        "$and",
        BsonValue::Array(vec![
            object([("a", member(vec![BsonValue::Int32(1), BsonValue::Int32(2)]))]),
            object([("a", member(vec![BsonValue::Int32(3), BsonValue::Int32(4)]))]),
        ]),
    )]);
    let matching = doc([(
        "a",
        BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(3)]),
    )]);
    let matcher = DocumentMatcher::compile(&query).unwrap();
    assert!(matcher.matches(&matching).unwrap());
    let probes = keys(&generator, &query).unwrap();
    assert!(
        generator
            .keys(&matching)
            .unwrap()
            .iter()
            .any(|key| probes.contains(key))
    );
    assert!(!matcher.matches(&doc([("a", BsonValue::Int32(1))])).unwrap());
}

#[test]
fn total_cartesian_keys_and_encoded_bytes_are_bounded_before_serialization() {
    let generator = generator(true, false);
    for count in [MAX_PROBE_KEYS, MAX_PROBE_KEYS + 1] {
        let query = doc([
            (
                "a",
                member((0..count).map(|i| BsonValue::Int32(i as i32)).collect()),
            ),
            ("b", BsonValue::Int32(1)),
        ]);
        assert_eq!(
            keys(&generator, &query).map(|keys| keys.len()),
            (count == MAX_PROBE_KEYS).then_some(count)
        );
    }
    let product = doc([
        ("a", member((0..16).map(BsonValue::Int32).collect())),
        ("b", member((0..9).map(BsonValue::Int32).collect())),
    ]);
    assert!(keys(&generator, &product).is_none());
    let large = doc([
        ("a", BsonValue::from("x".repeat(MAX_PROBE_BYTES / 16))),
        ("b", member((0..17).map(BsonValue::Int32).collect())),
    ]);
    assert!(keys(&generator, &large).is_none());
}

#[test]
fn cancellation_at_every_membership_checkpoint_discards_the_entire_probe() {
    let generator = generator(true, false);
    let matcher = DocumentMatcher::compile(&doc([
        ("a", member(vec![BsonValue::Int32(1), BsonValue::Int32(2)])),
        (
            "b",
            member(vec![BsonValue::from("x"), BsonValue::from("y")]),
        ),
    ]))
    .unwrap();
    let mut points = 0;
    let expected = generator
        .membership_keys_with_budget(
            &matcher,
            &mut Budget::new(&mut || {
                points += 1;
                Ok(())
            }),
        )
        .unwrap()
        .unwrap();
    for stop in 1..=points {
        let mut seen = 0;
        let error = generator
            .membership_keys_with_budget(
                &matcher,
                &mut Budget::new(&mut || {
                    seen += 1;
                    if seen == stop {
                        Err(EngineError::new(
                            EngineErrorKind::Cancelled,
                            "test cancellation",
                        ))
                    } else {
                        Ok(())
                    }
                }),
            )
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
    }
    assert_eq!(
        generator
            .membership_keys_with_budget(&matcher, &mut Budget::new(&mut || Ok(())))
            .unwrap()
            .unwrap(),
        expected
    );
}
