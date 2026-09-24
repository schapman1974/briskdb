use super::*;
use std::error::Error;

fn absent(path: &str, operand: BsonValue) -> BsonDocument {
    doc([(path, object([("$exists", operand)]))])
}

#[test]
fn absence_null_keys_have_no_false_negatives_for_typed_compound_and_array_paths() {
    let values = [
        BsonValue::Null,
        BsonValue::Boolean(false),
        BsonValue::Int32(1),
        BsonValue::Double(f64::NAN),
        BsonValue::from("present"),
        BsonValue::ObjectId(BsonObjectId::from_bytes([1; 12])),
        BsonValue::Array(vec![]),
        BsonValue::Array(vec![BsonValue::Null, BsonValue::Int32(1)]),
        BsonValue::Array(vec![object([]), object([("a", BsonValue::Null)])]),
        BsonValue::Array(vec![BsonValue::Array(vec![])]),
        object([]),
        object([("0", BsonValue::Null)]),
        object([("a", BsonValue::Null)]),
    ];
    let mut records = vec![doc([]), doc([("b", BsonValue::Int32(1))])];
    for value in values {
        for b in [
            BsonValue::Int32(1),
            BsonValue::Int64(2),
            BsonValue::Int32(99),
        ] {
            records.push(doc([("a", value.clone()), ("b", b.clone())]));
            records.push(doc([("nested", value.clone()), ("b", b.clone())]));
            records.push(doc([("nested", object([("a", value.clone())])), ("b", b)]));
        }
    }
    let mut matches = 0;
    let mut fallbacks = 0;
    for path in ["a", "nested.a", "a.0"] {
        for compound in [false, true] {
            for sparse in [false, true] {
                let mut spec = doc([(path, BsonValue::Int32(1))]);
                if compound {
                    spec.push("b", BsonValue::Int32(-1)).unwrap();
                }
                let generator = DocumentIndexKeyGenerator::compile(&spec, sparse, None).unwrap();
                for operand in [BsonValue::Boolean(false), BsonValue::Int32(0)] {
                    let mut query = absent(path, operand);
                    if compound {
                        query
                            .push(
                                "b",
                                member(vec![BsonValue::Int32(1), BsonValue::Double(2.0)]),
                            )
                            .unwrap();
                    }
                    for query in [
                        query.clone(),
                        doc([("$and", BsonValue::Array(vec![BsonValue::Document(query)]))]),
                    ] {
                        let matcher = DocumentMatcher::compile(&query).unwrap();
                        assert!(
                            generator.equality_key(&matcher).unwrap().is_none(),
                            "public inference stays unchanged"
                        );
                        let probes = keys(&generator, &query);
                        if sparse && !compound {
                            assert!(probes.is_none(), "missing sparse records require scanning");
                            continue;
                        }
                        let probes = probes.unwrap();
                        assert_eq!(probes.len(), if compound { 2 } else { 1 });
                        for record in &records {
                            if !matcher.matches(record).unwrap() {
                                continue;
                            }
                            matches += 1;
                            match generator.keys(record) {
                                Ok(stored) => assert!(
                                    stored.iter().any(|key| probes.contains(key)),
                                    "query={query:?}, record={record:?}"
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
    assert!(matches > 500);
    assert!(fallbacks > 20);
}

#[test]
fn explicit_null_is_only_a_candidate_and_sparse_partial_or_unproven_absence_stays_conservative() {
    let ordinary = generator(false, false);
    let query = absent("a", BsonValue::Boolean(false));
    let matcher = DocumentMatcher::compile(&query).unwrap();
    let probes = keys(&ordinary, &query).unwrap();
    assert_eq!(probes, ordinary.keys(&doc([])).unwrap());
    assert_eq!(
        probes,
        ordinary.keys(&doc([("a", BsonValue::Null)])).unwrap()
    );
    assert!(matcher.matches(&doc([])).unwrap());
    assert!(!matcher.matches(&doc([("a", BsonValue::Null)])).unwrap());
    assert!(keys(&generator(true, false), &query).is_none());
    assert!(
        keys(
            &generator(true, true),
            &doc([
                ("a", object([("$exists", BsonValue::Boolean(false))])),
                ("b", object([("$exists", BsonValue::Boolean(false))])),
            ])
        )
        .is_none()
    );
    assert!(
        keys(
            &generator(true, true),
            &doc([
                ("a", object([("$exists", BsonValue::Boolean(false))])),
                ("b", member(vec![BsonValue::Null, BsonValue::Int32(1)])),
            ])
        )
        .is_none()
    );
    let partial = DocumentIndexKeyGenerator::compile(
        &doc([("a", BsonValue::Int32(1))]),
        false,
        Some(&doc([("enabled", BsonValue::Boolean(true))])),
    )
    .unwrap();
    assert!(keys(&partial, &query).is_none());
    for query in [
        absent("a", BsonValue::Boolean(true)),
        absent("other", BsonValue::Boolean(false)),
        doc([(
            "$or",
            BsonValue::Array(vec![BsonValue::Document(query.clone())]),
        )]),
        doc([("$nor", BsonValue::Array(vec![BsonValue::Document(query)]))]),
        doc([(
            "a",
            object([("$not", object([("$exists", BsonValue::Boolean(true))]))]),
        )]),
    ] {
        assert!(keys(&ordinary, &query).is_none(), "{query:?}");
    }
}

#[test]
fn absence_probe_cancellation_at_every_checkpoint_keeps_the_generator_reusable() {
    let generator = generator(true, false);
    let matcher = DocumentMatcher::compile(&doc([
        ("a", object([("$exists", BsonValue::Boolean(false))])),
        ("b", member(vec![BsonValue::Int32(1), BsonValue::Int32(2)])),
    ]))
    .unwrap();
    let mut checkpoints = 0;
    let expected = generator
        .membership_keys_with_budget(
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
            .unwrap(),
        expected
    );
}
