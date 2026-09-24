use super::*;
use std::error::Error;

fn logical(operator: &str, branches: Vec<BsonDocument>) -> BsonDocument {
    doc([(
        operator,
        BsonValue::Array(branches.into_iter().map(BsonValue::Document).collect()),
    )])
}

fn alternatives(
    generator: &DocumentIndexKeyGenerator,
    query: &BsonDocument,
) -> Option<Vec<DocumentIndexKey>> {
    generator
        .alternative_keys_with_budget(
            &DocumentMatcher::compile(query).unwrap(),
            &mut Budget::new(&mut || Ok(())),
        )
        .unwrap()
}

#[test]
fn logical_union_candidates_have_no_false_negatives_for_typed_compound_and_multikey_records() {
    let values = [
        BsonValue::Null,
        BsonValue::Int32(1),
        BsonValue::Int64(2),
        BsonValue::Double(1.0),
        BsonValue::Decimal128(BsonDecimal128::parse("1.00").unwrap()),
        BsonValue::Boolean(true),
        BsonValue::from("1"),
    ];
    let mut records = vec![doc([]), doc([("b", BsonValue::Int32(1))])];
    for a in &values {
        for b in [BsonValue::Int32(1), BsonValue::Int32(2), BsonValue::Null] {
            records.push(doc([("a", a.clone()), ("b", b.clone())]));
            for other in &values {
                records.push(doc([
                    (
                        "a",
                        BsonValue::Array(vec![a.clone(), other.clone(), a.clone()]),
                    ),
                    ("b", b.clone()),
                ]));
            }
            records.push(doc([
                ("a", BsonValue::Array(vec![a.clone(), object([])])),
                ("b", b),
            ]));
        }
    }
    let mut matches = 0;
    let mut fallbacks = 0;
    for compound in [false, true] {
        for sparse in [false, true] {
            let generator = generator(compound, sparse);
            for left in &values {
                for right in &values {
                    let query = logical(
                        "$or",
                        vec![
                            doc([("a", left.clone()), ("b", BsonValue::Int32(1))]),
                            doc([
                                ("a", member(vec![right.clone(), right.clone()])),
                                ("b", BsonValue::Int32(2)),
                            ]),
                        ],
                    );
                    assert!(
                        keys(&generator, &query).is_none(),
                        "old necessary inference stays unchanged"
                    );
                    let Some(probes) = alternatives(&generator, &query) else {
                        assert!(
                            sparse
                                && !compound
                                && (matches!(left, BsonValue::Null)
                                    || matches!(right, BsonValue::Null))
                        );
                        continue;
                    };
                    assert!(probes.len() <= if compound { 4 } else { 2 });
                    let matcher = DocumentMatcher::compile(&query).unwrap();
                    for record in &records {
                        if matcher.matches(record).unwrap() {
                            matches += 1;
                            match generator.keys(record) {
                                Ok(stored) => assert!(
                                    stored.iter().any(|key| probes.contains(key)),
                                    "{query:?} {record:?}"
                                ),
                                Err(error) => {
                                    assert!(error.source().is_some_and(|source| {
                                        source.downcast_ref::<UnsupportedIndexedValue>().is_some()
                                    }));
                                    fallbacks += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    assert!(matches > 3_000);
    assert!(fallbacks > 100);
}

#[test]
fn logical_proofs_choose_conjunctive_witnesses_without_intersecting_array_values() {
    let query = logical(
        "$or",
        vec![
            logical(
                "$and",
                vec![
                    doc([("a", BsonValue::Int32(1))]),
                    doc([("a", BsonValue::Int32(2))]),
                ],
            ),
            doc([("a", object([("$exists", BsonValue::Boolean(false))]))]),
        ],
    );
    let generator = generator(false, false);
    let probes = alternatives(&generator, &query).unwrap();
    assert_eq!(probes.len(), 2);
    let array = doc([(
        "a",
        BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(2)]),
    )]);
    assert!(
        DocumentMatcher::compile(&query)
            .unwrap()
            .matches(&array)
            .unwrap()
    );
    assert!(
        generator
            .keys(&array)
            .unwrap()
            .iter()
            .any(|key| probes.contains(key))
    );
    assert!(probes.contains(&generator.keys(&doc([])).unwrap()[0]));
    assert!(
        !DocumentMatcher::compile(&query)
            .unwrap()
            .matches(&doc([("a", BsonValue::Null)]))
            .unwrap()
    );

    let mut nested = doc([("a", BsonValue::Int32(1))]);
    for _ in 0..12 {
        nested = logical("$or", vec![nested, doc([("a", BsonValue::Int32(2))])]);
    }
    let nested = logical("$and", vec![nested, doc([("b", BsonValue::Int32(1))])]);
    assert_eq!(
        alternatives(&super::generator(true, true), &nested)
            .unwrap()
            .len(),
        2
    );

    let correlated = logical(
        "$or",
        vec![
            doc([("a", BsonValue::Int32(1)), ("b", BsonValue::Int32(1))]),
            doc([("a", BsonValue::Int32(2)), ("b", BsonValue::Int32(2))]),
        ],
    );
    let compound = super::generator(true, false);
    let probes = alternatives(&compound, &correlated).unwrap();
    let false_positive = doc([("a", BsonValue::Int32(1)), ("b", BsonValue::Int32(2))]);
    assert_eq!(
        probes.len(),
        4,
        "independent unions form a conservative Cartesian superset"
    );
    assert!(probes.contains(&compound.keys(&false_positive).unwrap()[0]));
    assert!(
        !DocumentMatcher::compile(&correlated)
            .unwrap()
            .matches(&false_positive)
            .unwrap()
    );
}

#[test]
fn unbounded_negative_partial_and_sparse_logical_shapes_keep_scans() {
    let one = doc([("a", BsonValue::Int32(1))]);
    for branch in [
        doc([]),
        doc([("other", BsonValue::Int32(1))]),
        doc([("a", object([("$gt", BsonValue::Int32(1))]))]),
        doc([(
            "a",
            object([("$not", object([("$eq", BsonValue::Int32(1))]))]),
        )]),
        logical("$nor", vec![one.clone()]),
        doc([("a", member(vec![]))]),
        doc([(
            "a",
            member(vec![BsonValue::RegularExpression(
                BsonRegex::new("^1", "").unwrap(),
            )]),
        )]),
        doc([("a", BsonValue::Array(vec![BsonValue::Int32(1)]))]),
        doc([("a", BsonValue::ObjectId(BsonObjectId::from_bytes([1; 12])))]),
        doc([("a", BsonValue::Double(f64::NAN))]),
    ] {
        assert!(
            alternatives(
                &generator(false, false),
                &logical("$or", vec![one.clone(), branch])
            )
            .is_none()
        );
    }
    let absence = doc([("a", object([("$exists", BsonValue::Boolean(false))]))]);
    let query = logical("$or", vec![one.clone(), absence]);
    assert!(alternatives(&generator(false, true), &query).is_none());
    let mut compound_query = query.clone();
    compound_query.push("b", BsonValue::Int32(1)).unwrap();
    assert!(alternatives(&generator(true, true), &compound_query).is_some());
    let partial = DocumentIndexKeyGenerator::compile(
        &doc([("a", BsonValue::Int32(1))]),
        false,
        Some(&doc([("enabled", BsonValue::Boolean(true))])),
    )
    .unwrap();
    assert!(alternatives(&partial, &query).is_none());
    let matcher =
        DocumentMatcher::compile(&logical("$nor", vec![logical("$or", vec![one])])).unwrap();
    assert!(!matcher.has_index_alternatives(&mut || Ok(())).unwrap());
}

#[test]
fn logical_probes_bound_raw_operands_tuples_and_expanded_bytes_before_encoding() {
    let branches = |count| {
        (0..count)
            .map(|_| doc([("a", BsonValue::Int32(1))]))
            .collect()
    };
    assert_eq!(
        alternatives(&generator(false, false), &logical("$or", branches(128)))
            .unwrap()
            .len(),
        1
    );
    assert!(alternatives(&generator(false, false), &logical("$or", branches(129))).is_none());
    let lists = |count| {
        logical(
            "$or",
            vec![
                doc([("a", member(vec![BsonValue::Int32(1); 64]))]),
                doc([("a", member(vec![BsonValue::Int32(2); count]))]),
            ],
        )
    };
    assert_eq!(
        alternatives(&generator(false, false), &lists(64))
            .unwrap()
            .len(),
        2
    );
    assert!(alternatives(&generator(false, false), &lists(65)).is_none());
    let mut query = logical(
        "$or",
        vec![doc([("a", member((0..9).map(BsonValue::Int32).collect()))])],
    );
    query
        .push("b", member((0..15).map(BsonValue::Int32).collect()))
        .unwrap();
    assert!(alternatives(&generator(true, false), &query).is_none());
    let mut query = logical(
        "$or",
        vec![doc([("a", BsonValue::from("x".repeat(20_000)))])],
    );
    query
        .push("b", member((0..100).map(BsonValue::Int32).collect()))
        .unwrap();
    assert!(alternatives(&generator(true, false), &query).is_none());

    let failed = logical(
        "$or",
        vec![
            doc([("a", member(vec![BsonValue::Int32(1); 128]))]),
            doc([]),
        ],
    );
    let query = logical("$and", vec![failed, logical("$or", branches(1))]);
    assert!(
        alternatives(&generator(false, false), &query).is_none(),
        "failed alternatives never refund the operand quota"
    );
}

#[test]
fn logical_cancellation_at_every_checkpoint_preserves_reusability() {
    let generator = generator(true, false);
    let query = logical(
        "$or",
        vec![
            doc([("a", BsonValue::Int32(1)), ("b", BsonValue::Int32(2))]),
            doc([
                ("a", member(vec![BsonValue::Int32(2), BsonValue::Int64(1)])),
                ("b", BsonValue::Int32(1)),
            ]),
        ],
    );
    let matcher = DocumentMatcher::compile(&query).unwrap();
    let mut checkpoints = 0;
    let expected = generator
        .alternative_keys_with_budget(
            &matcher,
            &mut Budget::new(&mut || {
                checkpoints += 1;
                Ok(())
            }),
        )
        .unwrap();
    assert!(expected.is_some());
    for stop in 1..=checkpoints {
        let mut seen = 0;
        let error = generator
            .alternative_keys_with_budget(
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
    assert_eq!(alternatives(&generator, &query), expected);
}
