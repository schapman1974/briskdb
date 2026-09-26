use super::*;

fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}
fn object<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonValue {
    BsonValue::Document(doc(fields))
}
fn query() -> BsonDocument {
    doc([(
        "a",
        object([("$gt", BsonValue::from("z")), ("$lt", BsonValue::from("b"))]),
    )])
}
fn selected(
    generator: &DocumentIndexKeyGenerator,
    query: &BsonDocument,
) -> Option<DocumentIndexSelection> {
    generator
        .string_range_with_budget(
            &DocumentMatcher::compile(query).unwrap(),
            &mut Budget::new(&mut || Ok(())),
        )
        .unwrap()
}

#[test]
fn string_range_uses_one_necessary_bound_not_an_array_intersection() {
    for sparse in [false, true] {
        let generator =
            DocumentIndexKeyGenerator::compile(&doc([("a", BsonValue::Int32(1))]), sparse, None)
                .unwrap();
        let query = query();
        assert!(
            DocumentMatcher::compile(&query)
                .unwrap()
                .matches(&doc([(
                    "a",
                    BsonValue::Array(vec![BsonValue::from("a"), BsonValue::from("zz")])
                )]))
                .unwrap()
        );
        let Some(DocumentIndexSelection::StringRange {
            key,
            greater,
            inclusive,
        }) = selected(&generator, &query)
        else {
            panic!("range")
        };
        assert!(greater);
        assert!(!inclusive);
        assert_eq!(
            key,
            generator.keys(&doc([("a", BsonValue::from("z"))])).unwrap()[0]
                .to_bytes()
                .unwrap()
        );
        assert!(
            selected(
                &generator,
                &doc([(
                    "$and",
                    BsonValue::Array(vec![BsonValue::Document(query.clone())])
                )])
            )
            .is_some()
        );
        for operator in ["$or", "$nor"] {
            assert!(
                selected(
                    &generator,
                    &doc([(
                        operator,
                        BsonValue::Array(vec![BsonValue::Document(query.clone())])
                    )])
                )
                .is_none()
            );
        }
        for value in [
            BsonValue::Null,
            BsonValue::Int64(9_007_199_254_740_993),
            BsonValue::Double(f64::NAN),
            BsonValue::Boolean(true),
            BsonValue::Array(vec![BsonValue::from("a")]),
        ] {
            assert!(selected(&generator, &doc([("a", object([("$gt", value)]))])).is_none());
        }
        assert!(
            selected(
                &generator,
                &doc([("b", object([("$gt", BsonValue::from("a"))]))])
            )
            .is_none()
        );
        assert!(
            selected(
                &generator,
                &doc([(
                    "a",
                    object([("$not", object([("$gt", BsonValue::from("a"))]))])
                )])
            )
            .is_none()
        );
    }
}

#[test]
fn string_range_requires_single_component_and_proven_partial_membership() {
    let compound = DocumentIndexKeyGenerator::compile(
        &doc([("a", BsonValue::Int32(1)), ("b", BsonValue::Int32(1))]),
        false,
        None,
    )
    .unwrap();
    assert!(selected(&compound, &query()).is_none());
    let partial = doc([("enabled", BsonValue::Boolean(true))]);
    let generator = DocumentIndexKeyGenerator::compile(
        &doc([("a", BsonValue::Int32(-1))]),
        false,
        Some(&partial),
    )
    .unwrap();
    assert!(selected(&generator, &query()).is_none());
    let mut eligible = query();
    eligible.push("enabled", BsonValue::Boolean(true)).unwrap();
    assert!(selected(&generator, &eligible).is_some());
}

#[test]
fn string_range_cancellation_at_each_checkpoint_keeps_the_compiler_reusable() {
    let generator =
        DocumentIndexKeyGenerator::compile(&doc([("a", BsonValue::Int32(1))]), false, None)
            .unwrap();
    let matcher = DocumentMatcher::compile(&query()).unwrap();
    let mut steps = 0;
    assert!(
        generator
            .string_range_with_budget(
                &matcher,
                &mut Budget::new(&mut || {
                    steps += 1;
                    Ok(())
                })
            )
            .unwrap()
            .is_some()
    );
    for target in 1..=steps {
        let mut seen = 0;
        let result = generator.string_range_with_budget(
            &matcher,
            &mut Budget::new(&mut || {
                seen += 1;
                if seen == target {
                    Err(EngineError::new(
                        EngineErrorKind::Cancelled,
                        "test cancellation",
                    ))
                } else {
                    Ok(())
                }
            }),
        );
        assert_eq!(result.err().unwrap().kind(), EngineErrorKind::Cancelled);
    }
    assert!(selected(&generator, &query()).is_some());
}
