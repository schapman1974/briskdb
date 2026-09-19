use super::*;
use crate::document::{BsonDecimal128, decode_document};
use bson::{Bson, Document, doc};

#[test]
fn all_in_not_size_type_and_mod_contracts() {
    assert!(matches(
        doc! { "v": [1, 2, 3] },
        doc! { "v": {"$all": [1, 3], "$size": 3} }
    ));
    assert!(!matches(doc! { "v": [1, 2] }, doc! { "v": {"$all": []} }));
    assert!(matches(
        doc! { "v": [{"x": 2}, {"x": 3}] },
        doc! { "v": {"$all": [{"$elemMatch": {"x": 2}}, {"$elemMatch": {"x": 3}}]} }
    ));
    assert!(matches(
        doc! { "v": [1, "x"] },
        doc! { "v": {"$type": ["string", "double"]} }
    ));
    assert!(!matches(
        doc! { "v": true },
        doc! { "v": {"$type": "number"} }
    ));
    assert!(matches(
        doc! { "v": [1, 2] },
        doc! { "v": {"$in": [2], "$nin": [3]} }
    ));
    assert!(matches(doc! {}, doc! { "v": {"$not": {"$eq": "x"}} }));
    assert!(matches(
        doc! { "v": 10.9 },
        doc! { "v": {"$mod": [4.9, 2.9]} }
    ));
    assert!(matches(doc! { "v": -10 }, doc! { "v": {"$mod": [4, -2]} }));
    assert!(!matches(doc! { "v": -10 }, doc! { "v": {"$mod": [4, 2]} }));
    assert!(matches(
        doc! { "v": i64::MIN },
        doc! { "v": {"$mod": [-1, 0]} }
    ));
    assert!(!matches(doc! { "v": "10" }, doc! { "v": {"$mod": [4, 2]} }));
}

#[test]
fn decimal_integer_boundaries_are_exact_and_checked() {
    let decimal = |value: &str| BsonValue::Decimal128(value.parse::<BsonDecimal128>().unwrap());
    assert_eq!(integer(&decimal("10.9"), true), Some(10));
    assert_eq!(integer(&decimal("-10.9"), true), Some(-10));
    assert_eq!(integer(&decimal("10.9"), false), None);
    assert_eq!(
        integer(&decimal("-9223372036854775808.9"), true),
        Some(i64::MIN)
    );
    assert_eq!(integer(&decimal("9223372036854775808"), true), None);
    assert_eq!(integer(&decimal("1E+6000"), true), None);
    assert_eq!(integer(&decimal("1E-6000"), true), Some(0));
    assert_eq!(integer(&decimal("NaN"), true), None);
    assert_eq!(
        integer(&BsonValue::Double(9_223_372_036_854_775_808.0), true),
        None
    );
}

#[test]
fn regex_identity_is_separate_from_predicates_and_options() {
    let regex = Bson::RegularExpression(bson::Regex {
        pattern: "Ab.c".try_into().unwrap(),
        options: "i".try_into().unwrap(),
    });
    assert!(matches(
        doc! {"v": ["no", "abxc"]},
        doc! {"v": regex.clone()}
    ));
    assert!(matches(
        doc! {"v": regex.clone()},
        doc! {"v": {"$regex": "Ab.c", "$options": "i"}}
    ));
    assert!(!matches(
        doc! {"v": "Abxc"},
        doc! {"v": {"$eq": regex.clone()}}
    ));
    assert!(matches(
        doc! {"v": regex.clone()},
        doc! {"v": {"$eq": regex.clone()}}
    ));
    assert!(matches(
        doc! {"v": [["abxc"]]},
        doc! {"v": {"$all": [regex.clone()]}}
    ));
    assert!(matches(
        doc! {"v": "abcabc"},
        doc! {"v": {"$regex": r"^(abc)\1$"}}
    ));
    assert!(matches(
        doc! {"v": "abc"},
        doc! {"v": {"$regex": r"^a(?=bc)"}}
    ));
    assert!(matches(doc! {}, doc! {"v": {"$not": regex}}));
    let invalid_literal = Bson::RegularExpression(bson::Regex {
        pattern: "[".try_into().unwrap(),
        options: "".try_into().unwrap(),
    });
    assert!(matches(
        doc! {"v": invalid_literal.clone()},
        doc! {"v": {"$eq": invalid_literal.clone()}}
    ));
    assert_eq!(code(doc! {"v": invalid_literal}), 51091);
}

#[test]
fn regex_anchors_unicode_classes_and_scoped_flags_follow_the_oracle() {
    for (text, pattern, options, expected) in [
        ("a\n", "^a$", "", true),
        ("a\n\n", "^a$", "", false),
        ("a\n", r"a\Z", "", false),
        ("a\n\n", "^a$", "m", true),
        ("²", r"\w", "", true),
        ("\u{301}", r"\w", "", false),
        ("\u{1c}", r"\s", "", true),
        ("İ", "[a-z]", "i", true),
        ("ı", "[^i]", "i", false),
        ("i", "[^İ]", "i", false),
        ("Ab", "(?i:a)(?-i:b)", "", true),
        ("AB", "(?i:a)(?-i:b)", "", false),
        ("aa", "(?P<word>a)(?P=word)", "", true),
        ("$", r"\$", "", true),
    ] {
        assert_eq!(
            matches(
                doc! {"v": text},
                doc! {"v": {"$regex": pattern, "$options": options}}
            ),
            expected,
            "{pattern:?} / {options} against {text:?}"
        );
    }
    assert_eq!(
        code(doc! {"v": {"$regex": format!("{}a{}", "(".repeat(1000), ")".repeat(1000))}}),
        51091
    );
}

#[test]
fn every_query_branch_is_validated_with_redacted_stable_errors() {
    for filter in [
        doc! {"v": {"$size": "2"}},
        doc! {"v": {"$mod": [0, 1]}},
        doc! {"v": {"$elemMatch": 1}},
        doc! {"v": {"$not": {}}},
        doc! {"v": {"$not": {"literal": 1}}},
        doc! {"v": {"$in": [{"$eq": 1}]}},
        doc! {"v": {"$options": "i"}},
        doc! {"$and": []},
        doc! {"v": {"$gt": 2, "literal": 1}},
        doc! {"v": {"$comment": 1}},
        doc! {"v": {"$elemMatch": {"$unknown": 1}}},
        doc! {"$or": [{}, {"v": {"$size": "invalid"}}]},
    ] {
        assert_eq!(code(filter), 2);
    }
    assert_eq!(code(doc! {"v": {"$type": []}}), 9);
    assert_eq!(code(doc! {"v": {"$type": true}}), 14);
    assert_eq!(code(doc! {"v": {"$regex": "[secret"}}), 51091);
    assert_eq!(code(doc! {"v": {"$regex": "x", "$options": "q"}}), 51108);
    assert_eq!(
        code(
            doc! {"v": {"$regex": Bson::RegularExpression(bson::Regex { pattern: "x".try_into().unwrap(), options: "i".try_into().unwrap() }), "$options": "i"}}
        ),
        51075
    );
    assert_eq!(code(doc! {"$where": "secret"}), 115);
    assert_eq!(code(doc! {"v": {"$unknown": "secret"}}), 115);
    let error = DocumentMatcher::compile(&owned(doc! {"$where": "secret"})).unwrap_err();
    assert!(!format!("{error:?}").contains("secret"));
}

#[test]
fn cancellation_depth_node_and_regex_limits_are_enforced() {
    let query = owned(doc! {"v": {"$regex": "a".repeat(MAX_REGEX_BYTES + 1)}});
    assert_eq!(
        DocumentMatcher::compile(&query).unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
    let filter = BsonDocument::from_entries(
        (0..MAX_QUERY_NODES + 1).map(|index| (format!("k{index}"), BsonValue::Null)),
    )
    .unwrap();
    assert_eq!(
        DocumentMatcher::compile(&filter).unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
    let matcher = DocumentMatcher::compile(&owned(doc! {"v": 1})).unwrap();
    let error = matcher
        .matches_with_check(&owned(doc! {"v": 1}), &mut || {
            Err(EngineError::new(
                EngineErrorKind::Cancelled,
                "test cancellation",
            ))
        })
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Cancelled);
    let too_deep = BsonDocument::from_entries([("x.".repeat(101), BsonValue::Null)]).unwrap();
    assert_eq!(
        DocumentMatcher::compile(&too_deep).unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
    // Disable the seek optimization only in this fixture so the VM reliably
    // exercises the same backtracking limit and runtime error mapping.
    let pattern = r"(?i)(a|b|ab)*(?>c)";
    let regex = CompiledRegex {
        identity: BsonRegex::new(pattern, "").unwrap(),
        expression: Some(
            RegexBuilder::new(pattern)
                .backtrack_limit(REGEX_BACKTRACK_LIMIT)
                .seek(false)
                .build()
                .unwrap(),
        ),
    };
    let value = BsonValue::from("ab".repeat(30));
    let error = regex
        .evaluate(
            Some(&value),
            false,
            &mut Work {
                steps: 0,
                check: &mut || Ok(()),
            },
        )
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
}

fn owned(document: Document) -> BsonDocument {
    decode_document(&document.to_vec().unwrap()).unwrap()
}

fn matches(document: Document, filter: Document) -> bool {
    DocumentMatcher::compile(&owned(filter))
        .unwrap()
        .matches(&owned(document))
        .unwrap()
}

fn code(filter: Document) -> i32 {
    let error = DocumentMatcher::compile(&owned(filter)).unwrap_err();
    error
        .source()
        .unwrap()
        .downcast_ref::<DocumentQueryError>()
        .unwrap()
        .mongo_code()
}

#[test]
fn missing_null_numeric_families_and_exact_id_arrays() {
    assert!(matches(doc! {}, doc! { "v": Bson::Null }));
    assert!(!matches(doc! {}, doc! { "v": { "$ne": Bson::Null } }));
    assert!(matches(doc! { "v": [1, 2] }, doc! { "v": 2.0 }));
    assert!(!matches(doc! { "_id": [1, 2] }, doc! { "_id": 2 }));
    assert!(matches(doc! { "_id": [1, 2] }, doc! { "_id": [1.0, 2] }));
    assert!(!matches(doc! { "v": true }, doc! { "v": 1 }));
    assert!(matches(
        doc! { "v": Bson::Double(f64::NAN) },
        doc! { "v": Bson::Double(f64::NAN) }
    ));
    assert!(!matches(
        doc! { "v": 2 },
        doc! { "v": { "$gt": Bson::Double(f64::NAN) } }
    ));
    assert!(!matches(doc! { "v": "z" }, doc! { "v": { "$gt": 2 } }));
    assert!(matches(doc! {}, doc! { "v": { "$gte": Bson::Null } }));
    assert!(matches(doc! {}, doc! { "v": { "$gt": Bson::MinKey } }));
    assert!(matches(
        doc! { "v": [] },
        doc! { "v": { "$lt": Bson::MaxKey } }
    ));
    assert!(matches(
        doc! { "v": [[1, 2]] },
        doc! { "v": { "$gte": [1, 1] } }
    ));
}

#[test]
fn dotted_paths_keep_missing_and_numeric_endpoints_distinct() {
    let document = doc! { "items": [{"other": 1}, {"score": 2}] };
    assert!(matches(document.clone(), doc! { "items.score": 2 }));
    assert!(matches(
        document.clone(),
        doc! { "items.score": Bson::Null }
    ));
    assert!(!matches(
        document.clone(),
        doc! { "items.score": { "$ne": Bson::Null } }
    ));
    assert!(!matches(
        document,
        doc! { "items.score": { "$exists": false } }
    ));
    let raw = doc! { "items": [[{"score": 2}]] };
    assert!(!matches(raw.clone(), doc! { "items.score": 2 }));
    assert!(matches(
        raw.clone(),
        doc! { "items.score": { "$exists": false } }
    ));
    assert!(matches(raw, doc! { "items.score": { "$ne": Bson::Null } }));
    assert!(matches(
        doc! { "items": [1, {"0": {"score": 3}}] },
        doc! { "items.0.score": 3 }
    ));
    assert!(matches(
        doc! { "items": [1, 2] },
        doc! { "items.0.score": { "$nin": [Bson::Null] } }
    ));
    let nested = doc! { "values": [[1, 2], [3]] };
    for condition in [
        Bson::Int32(1),
        Bson::Document(doc! {"$type": "int"}),
        Bson::Document(doc! {"$gt": 0}),
        Bson::Document(doc! {"$all": [1, 2]}),
    ] {
        assert!(!matches(nested.clone(), doc! { "values.0": condition }));
    }
    for condition in [
        Bson::Array(vec![1.into(), 2.into()]),
        Bson::Document(doc! {"$size": 2}),
        Bson::Document(doc! {"$type": "array"}),
        Bson::Document(doc! {"$elemMatch": {"$gt": 1}}),
    ] {
        assert!(matches(nested.clone(), doc! { "values.0": condition }));
    }
}

#[test]
fn element_match_logical_operators_and_independent_array_predicates() {
    let document = doc! { "values": [2, 7, 12], "items": [{"kind": "quiz", "score": 8}, {"kind": "exam", "score": 10}] };
    assert!(matches(
        document.clone(),
        doc! { "values": { "$elemMatch": {"$gte": 5, "$lt": 10} } }
    ));
    assert!(matches(
        document.clone(),
        doc! { "values": { "$gt": 10, "$lt": 3 } }
    ));
    assert!(!matches(
        document.clone(),
        doc! { "values": { "$elemMatch": {"$gt": 10, "$lt": 3} } }
    ));
    assert!(matches(
        document.clone(),
        doc! { "items": {"$elemMatch": {"kind": "quiz", "score": {"$gte": 8}}} }
    ));
    assert!(!matches(
        document.clone(),
        doc! { "items": {"$elemMatch": {"kind": "quiz", "score": {"$gte": 10}}} }
    ));
    assert!(matches(
        document.clone(),
        doc! { "$and": [{"values": 2}, {"values": 12}], "$comment": "ignored" }
    ));
    assert!(matches(
        document.clone(),
        doc! { "$or": [{"absent": 1}, {"values": 2}] }
    ));
    assert!(matches(
        document,
        doc! { "$nor": [{"values": 99}, {"absent": 1}] }
    ));
    assert!(matches(
        doc! { "values": [[1, 2]] },
        doc! { "values": {"$elemMatch": {"0": 1}} }
    ));
    assert!(matches(
        doc! { "values": [[1, 2]] },
        doc! { "values": {"$elemMatch": {"value": Bson::Null}} }
    ));
    assert!(!matches(
        doc! { "values": [[{"x": 2}]] },
        doc! { "values": {"$elemMatch": {"x": 2}} }
    ));
    assert!(matches(
        doc! { "values": [{"a": [{"b": 1}]}] },
        doc! { "values": {"$elemMatch": {"a.b": 1}} }
    ));
}
