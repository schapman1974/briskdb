use super::*;
use crate::document::{BsonBinary, BsonDateTime, BsonDecimal128, BsonObjectId, BsonRegex};
use std::error::Error;

fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}

fn object<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonValue {
    BsonValue::Document(doc(fields))
}

fn presence(path: &str, value: BsonValue) -> BsonDocument {
    doc([(path, object([("$exists", value)]))])
}

fn selected(generator: &DocumentIndexKeyGenerator, query: &BsonDocument) -> bool {
    generator
        .sparse_presence_with_budget(
            &DocumentMatcher::compile(query).unwrap(),
            &mut Budget::new(&mut || Ok(())),
        )
        .unwrap()
}

#[test]
fn positive_presence_is_a_necessary_sparse_witness_without_false_negatives() {
    let values = [
        BsonValue::Null,
        BsonValue::Boolean(true),
        BsonValue::Int32(1),
        BsonValue::Int64(9_007_199_254_740_993),
        BsonValue::Double(1.0),
        BsonValue::Double(f64::NAN),
        BsonValue::Decimal128(BsonDecimal128::parse("1.00").unwrap()),
        BsonValue::from("present"),
        BsonValue::Binary(BsonBinary::new(128, vec![1, 2])),
        BsonValue::DateTime(BsonDateTime::from_millis(123)),
        BsonValue::ObjectId(BsonObjectId::from_bytes([1; 12])),
        BsonValue::RegularExpression(BsonRegex::new("value", "").unwrap()),
        BsonValue::Array(vec![]),
        BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(2)]),
        BsonValue::Array(vec![object([("a", BsonValue::Null)]), object([])]),
        BsonValue::Array(vec![BsonValue::Array(vec![])]),
        object([("a", BsonValue::Null)]),
        object([("0", BsonValue::Int32(1))]),
    ];
    let mut records = vec![doc([]), doc([("b", BsonValue::Int32(1))])];
    for value in values {
        records.push(doc([("a", value.clone())]));
        records.push(doc([("nested", object([("a", value.clone())]))]));
        records.push(doc([("a", value.clone()), ("b", BsonValue::Int32(1))]));
        records.push(doc([("nested", value)]));
    }
    let mut matches = 0;
    let mut fallbacks = 0;
    for path in ["a", "nested.a", "a.0"] {
        for compound in [false, true] {
            let mut definition = doc([(path, BsonValue::Int32(1))]);
            if compound {
                definition.push("b", BsonValue::Int32(-1)).unwrap();
            }
            let generator = DocumentIndexKeyGenerator::compile(&definition, true, None).unwrap();
            for query in [
                presence(path, BsonValue::Boolean(true)),
                presence(path, BsonValue::Int32(1)),
                doc([(
                    "$and",
                    BsonValue::Array(vec![
                        BsonValue::Document(presence(path, BsonValue::Boolean(true))),
                        object([("b", BsonValue::Int32(1))]),
                    ]),
                )]),
            ] {
                assert!(selected(&generator, &query));
                let matcher = DocumentMatcher::compile(&query).unwrap();
                for record in &records {
                    if !matcher.matches(record).unwrap() {
                        continue;
                    }
                    matches += 1;
                    match generator.keys(record) {
                        Ok(keys) => assert!(!keys.is_empty(), "query={query:?}, record={record:?}"),
                        Err(error) => {
                            // Storage emits a record-bound BDIF entry for these
                            // non-unique shapes; the sparse scan includes it.
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
    assert!(matches > 200);
    assert!(fallbacks > 50);
}

#[test]
fn ordinary_partial_negative_alternative_and_unrelated_paths_do_not_grant_presence() {
    let keys = doc([("a", BsonValue::Int32(1)), ("b", BsonValue::Int32(1))]);
    let ordinary = DocumentIndexKeyGenerator::compile(&keys, false, None).unwrap();
    let sparse = DocumentIndexKeyGenerator::compile(&keys, true, None).unwrap();
    let query = presence("a", BsonValue::Boolean(true));
    assert!(!selected(&ordinary, &query));
    let partial = DocumentIndexKeyGenerator::compile(&keys, false, Some(&query)).unwrap();
    assert!(!selected(&partial, &query));
    assert!(selected(&sparse, &query));
    assert!(selected(&sparse, &presence("b", BsonValue::Boolean(true))));
    for query in [
        presence("a", BsonValue::Boolean(false)),
        presence("a", BsonValue::Int32(0)),
        presence("a.nested", BsonValue::Boolean(true)),
        presence("other", BsonValue::Boolean(true)),
        doc([("a", BsonValue::Int32(1))]),
        doc([(
            "a",
            object([("$not", object([("$exists", BsonValue::Boolean(false))]))]),
        )]),
        doc([(
            "$or",
            BsonValue::Array(vec![BsonValue::Document(query.clone())]),
        )]),
        doc([("$nor", BsonValue::Array(vec![BsonValue::Document(query)]))]),
    ] {
        assert!(!selected(&sparse, &query), "{query:?}");
    }
}

#[test]
fn cancellation_at_every_presence_checkpoint_releases_the_optional_proof() {
    let generator = DocumentIndexKeyGenerator::compile(
        &doc([("other", BsonValue::Int32(1)), ("a", BsonValue::Int32(1))]),
        true,
        None,
    )
    .unwrap();
    let matcher = DocumentMatcher::compile(&doc([(
        "$and",
        BsonValue::Array(vec![
            object([("unrelated", BsonValue::Int32(1))]),
            BsonValue::Document(presence("a", BsonValue::Boolean(true))),
        ]),
    )]))
    .unwrap();
    let mut checkpoints = 0;
    assert!(
        generator
            .sparse_presence_with_budget(
                &matcher,
                &mut Budget::new(&mut || {
                    checkpoints += 1;
                    Ok(())
                })
            )
            .unwrap()
    );
    for stop in 1..=checkpoints {
        let mut seen = 0;
        let error = generator
            .sparse_presence_with_budget(
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
    assert!(
        generator
            .sparse_presence_with_budget(&matcher, &mut Budget::new(&mut || Ok(())))
            .unwrap()
    );
}
