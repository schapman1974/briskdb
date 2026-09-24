use super::*;
use crate::document::{BsonBinary, BsonDecimal128, BsonObjectId};

fn doc(values: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(values).unwrap()
}
fn obj(values: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonValue {
    BsonValue::Document(doc(values))
}
fn logical(kind: &'static str, children: Vec<BsonDocument>) -> BsonDocument {
    doc([(
        kind,
        BsonValue::Array(children.into_iter().map(BsonValue::Document).collect()),
    )])
}
fn proves(partial: &BsonDocument, query: &BsonDocument) -> bool {
    DocumentMatcher::compile(partial)
        .unwrap()
        .is_implied_by_index_query(&DocumentMatcher::compile(query).unwrap(), &mut || Ok(()))
        .unwrap()
}

#[test]
fn partial_proof_requires_necessary_identical_scalar_or_presence_facts() {
    let required = doc([("enabled", BsonValue::Boolean(true))]);
    let a = doc([("a", BsonValue::Int32(1))]);
    for query in [
        required.clone(),
        doc([
            ("enabled", BsonValue::Boolean(true)),
            ("a", BsonValue::Int32(1)),
        ]),
        logical("$and", vec![a.clone(), required.clone()]),
        logical(
            "$or",
            vec![
                logical("$and", vec![required.clone(), a.clone()]),
                required.clone(),
            ],
        ),
    ] {
        assert!(proves(&required, &query), "{query:?}");
    }
    for query in [
        doc([]),
        a.clone(),
        doc([("enabled", BsonValue::Boolean(false))]),
        doc([("enabled", BsonValue::Int32(1))]),
        logical("$or", vec![required.clone(), a.clone()]),
        logical("$nor", vec![required.clone()]),
        doc([(
            "enabled",
            obj([("$not", obj([("$eq", BsonValue::Boolean(true))]))]),
        )]),
        doc([(
            "enabled",
            obj([("$in", BsonValue::Array(vec![BsonValue::Boolean(true)]))]),
        )]),
    ] {
        assert!(!proves(&required, &query), "{query:?}");
    }
    assert!(proves(
        &logical("$or", vec![required.clone(), a.clone()]),
        &required
    ));
    assert!(!proves(
        &logical("$and", vec![required.clone(), a]),
        &required
    ));
    let presence = doc([("enabled", obj([("$exists", BsonValue::Boolean(true))]))]);
    assert!(proves(&presence, &presence));
    assert!(!proves(&presence, &doc([("enabled", BsonValue::Null)])));
    assert!(
        !proves(&presence, &required),
        "nonnull-value implications are outside this proof slice"
    );
    assert!(
        !proves(
            &doc([("a", BsonValue::Int32(1))]),
            &doc([("a", BsonValue::Double(1.0))])
        ),
        "aliases stay conservative"
    );
    for unsupported in [
        obj([("$gt", BsonValue::Int32(0))]),
        obj([("$type", BsonValue::from("number"))]),
        BsonValue::Array(vec![BsonValue::Int32(1)]),
        obj([("nested", BsonValue::Int32(1))]),
    ] {
        let partial = doc([("a", unsupported)]);
        assert!(!proves(&partial, &partial));
    }
}

#[test]
fn partial_proof_differential_never_excludes_a_matching_typed_document() {
    let values = vec![
        BsonValue::Null,
        BsonValue::Boolean(false),
        BsonValue::Boolean(true),
        BsonValue::Int32(0),
        BsonValue::Int32(1),
        BsonValue::Int64(1),
        BsonValue::Double(1.0),
        BsonValue::Decimal128(BsonDecimal128::parse("1.0").unwrap()),
        BsonValue::from("a"),
        BsonValue::from("b"),
        BsonValue::ObjectId(BsonObjectId::from_bytes([1; 12])),
        BsonValue::Binary(BsonBinary::new(0, b"a".to_vec())),
        BsonValue::Array(vec![]),
        BsonValue::Array(vec![BsonValue::Null, BsonValue::Int32(1)]),
        BsonValue::Array(vec![BsonValue::Int32(0), BsonValue::Int32(1)]),
        obj([("x", BsonValue::Int32(1))]),
    ];
    let guard = doc([("g", BsonValue::Boolean(true))]);
    let presence = doc([("a", obj([("$exists", BsonValue::Boolean(true))]))]);
    let mut partials = vec![presence.clone()];
    let mut queries = vec![doc([]), presence];
    let mut documents = vec![doc([]), guard.clone()];
    for value in &values {
        let equality = doc([("a", value.clone())]);
        partials.extend([
            equality.clone(),
            logical("$and", vec![equality.clone(), guard.clone()]),
            logical("$or", vec![equality.clone(), guard.clone()]),
        ]);
        queries.extend([
            equality.clone(),
            logical("$and", vec![equality.clone(), guard.clone()]),
            logical("$or", vec![equality.clone(), guard.clone()]),
            logical("$nor", vec![equality.clone()]),
            logical(
                "$or",
                vec![
                    equality.clone(),
                    logical("$and", vec![equality.clone(), guard.clone()]),
                ],
            ),
            logical("$and", vec![equality, doc([("a", BsonValue::Int32(1))])]),
        ]);
        for enabled in [
            None,
            Some(BsonValue::Boolean(false)),
            Some(BsonValue::Boolean(true)),
        ] {
            let mut entries = vec![("a", value.clone())];
            if let Some(enabled) = enabled {
                entries.push(("g", enabled));
            }
            documents.push(doc(entries));
        }
    }
    let mut proven = 0;
    let mut matched = 0;
    for partial in &partials {
        let membership = DocumentMatcher::compile(partial).unwrap();
        for query in &queries {
            let matcher = DocumentMatcher::compile(query).unwrap();
            if !membership
                .is_implied_by_index_query(&matcher, &mut || Ok(()))
                .unwrap()
            {
                continue;
            }
            proven += 1;
            for document in &documents {
                if matcher.matches(document).unwrap() {
                    matched += 1;
                    assert!(
                        membership.matches(document).unwrap(),
                        "false negative: partial={partial:?}, query={query:?}, document={document:?}"
                    );
                }
            }
        }
    }
    assert!(
        proven > 100 && matched > 500,
        "non-vacuous proof coverage: {proven} / {matched}"
    );
}

#[test]
fn partial_proof_checks_cancellation_and_comparison_work_without_payload_errors() {
    let secret = "private-membership-value".repeat(100);
    let query = logical(
        "$or",
        vec![
            doc([("a", BsonValue::from(secret.clone()))]),
            doc([("a", BsonValue::from(secret.clone()))]),
        ],
    );
    let membership = DocumentMatcher::compile(&doc([("a", BsonValue::from(secret))])).unwrap();
    let matcher = DocumentMatcher::compile(&query).unwrap();
    let mut steps = 0;
    assert!(
        membership
            .is_implied_by_index_query(&matcher, &mut || {
                steps += 1;
                Ok(())
            })
            .unwrap()
    );
    for stop in 1..=steps {
        let mut seen = 0;
        let error = membership
            .is_implied_by_index_query(&matcher, &mut || {
                seen += 1;
                if seen == stop {
                    Err(EngineError::new(
                        EngineErrorKind::Cancelled,
                        "partial proof cancelled",
                    ))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        assert!(!error.to_string().contains("private-membership-value"));
    }
    struct Bounded {
        bytes: usize,
    }
    impl MatchControl for Bounded {
        fn step(&mut self) -> EngineResult<()> {
            Ok(())
        }
        fn comparison_bytes(&mut self, bytes: usize) -> EngineResult<()> {
            self.bytes += bytes;
            if self.bytes > 128 {
                Err(limit())
            } else {
                Ok(())
            }
        }
    }
    assert_eq!(
        membership
            .is_implied_by_index_query(&matcher, &mut Bounded { bytes: 0 })
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    assert!(
        membership
            .is_implied_by_index_query(&matcher, &mut || Ok(()))
            .unwrap()
    );
}
