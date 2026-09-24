use super::*;

fn document(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(entries).unwrap()
}

fn model(
    keys: BsonDocument,
    options: impl IntoIterator<Item = (&'static str, BsonValue)>,
) -> BsonDocument {
    document(std::iter::once(("key", BsonValue::Document(keys))).chain(options))
}

fn request(models: Vec<BsonDocument>) -> Request {
    Request {
        request_id: 1,
        database: "compat".into(),
        body: document([
            ("createIndexes", BsonValue::from("items")),
            (
                "indexes",
                BsonValue::Array(models.into_iter().map(BsonValue::Document).collect()),
            ),
            ("$db", BsonValue::from("compat")),
        ]),
        sequences: vec![],
        more_to_come: false,
        legacy_handshake: false,
    }
}

fn plan(models: Vec<BsonDocument>) -> Result<PreparedIndexes> {
    prepare(
        &request(models),
        DocumentNamespace::new("compat", "items").unwrap(),
        Instant::now(),
        Duration::from_secs(10),
    )
}

#[test]
fn compatibility_plans_keep_effective_keys_names_and_explicit_warnings() {
    let original = request(vec![
        model(
            document([
                ("tenant", BsonValue::Int64(-1)),
                ("token", BsonValue::from("hashed")),
            ]),
            [
                ("sparse", BsonValue::Boolean(true)),
                ("expireAfterSeconds", BsonValue::Double(0.5)),
                ("background", BsonValue::Boolean(true)),
            ],
        ),
        model(
            document([("email", BsonValue::Int32(1))]),
            [
                ("unique", BsonValue::Boolean(true)),
                ("background", BsonValue::Boolean(false)),
            ],
        ),
    ]);
    let before = original.body.clone();
    let prepared = prepare(
        &original,
        DocumentNamespace::new("compat", "items").unwrap(),
        Instant::now(),
        Duration::from_secs(10),
    )
    .unwrap();
    assert_eq!(original.body, before);
    let indexes = prepared.request.indexes();
    assert_eq!(indexes[0].name(), Some("tenant_-1_token_hashed"));
    assert_eq!(
        indexes[0].keys(),
        &document([
            ("tenant", BsonValue::Int64(-1)),
            ("token", BsonValue::Int32(1))
        ])
    );
    assert!(indexes[0].sparse());
    assert!(!indexes[0].unique());
    assert!(indexes[1].unique());
    assert_eq!(
        prepared.warnings,
        vec![BsonValue::Document(document([
            ("name", BsonValue::from("tenant_-1_token_hashed")),
            (
                "reducedBehavior",
                BsonValue::Array(vec![
                    BsonValue::from("hashed: ascending equality indexing"),
                    BsonValue::from("ttl: expiration is not performed"),
                    BsonValue::from("background: builds run synchronously"),
                ])
            ),
        ]))]
    );
    assert!(
        reply(1, 3, prepared.warnings)
            .get_first("briskdbIndexWarnings")
            .is_some()
    );
    assert!(
        reply(1, 1, vec![])
            .get_first("briskdbIndexWarnings")
            .is_none()
    );
}

#[test]
fn background_builtin_noops_use_the_canonical_name_without_secondary_name_rules() {
    for name in [None, Some("_id_"), Some("_id_1"), Some("ignored_alias")] {
        let mut options = vec![("background", BsonValue::Boolean(true))];
        if let Some(name) = name {
            options.push(("name", BsonValue::from(name)));
        }
        let prepared = plan(vec![model(
            document([("_id", BsonValue::Int64(1))]),
            options,
        )])
        .unwrap();
        assert_eq!(
            prepared.warnings,
            vec![BsonValue::Document(document([
                ("name", BsonValue::from("_id_")),
                (
                    "reducedBehavior",
                    BsonValue::Array(vec![BsonValue::from(
                        "background: builds run synchronously"
                    )])
                ),
            ]))]
        );
    }
}

#[test]
fn compatibility_never_degrades_unique_or_builtin_key_semantics() {
    for field in ["value", "_id"] {
        for hashed in [false, true] {
            let keys = document([(
                field,
                if hashed {
                    BsonValue::from("hashed")
                } else {
                    BsonValue::Int32(1)
                },
            )]);
            let mut options = vec![("unique", BsonValue::Boolean(true))];
            if !hashed {
                options.push(("expireAfterSeconds", BsonValue::Int32(60)));
            }
            assert_eq!(
                plan(vec![model(keys.clone(), options)]).err().unwrap().code,
                115
            );
            if field == "_id" {
                let options = if hashed {
                    vec![]
                } else {
                    vec![("expireAfterSeconds", BsonValue::Int32(60))]
                };
                assert_eq!(plan(vec![model(keys, options)]).err().unwrap().code, 115);
            }
        }
    }
    assert_eq!(
        plan(vec![model(
            document([("_id", BsonValue::Int32(1))]),
            [("unique", BsonValue::Boolean(false))]
        )])
        .err()
        .unwrap()
        .code,
        197
    );
}

#[test]
fn ttl_options_are_finite_nonnegative_numbers_not_boolean_or_decimal_aliases() {
    for value in [
        BsonValue::Int32(0),
        BsonValue::Int64(i64::MAX),
        BsonValue::Double(0.5),
        BsonValue::Double(-0.0),
    ] {
        assert!(
            plan(vec![model(
                document([("created", BsonValue::Int32(1))]),
                [("expireAfterSeconds", value)]
            )])
            .is_ok()
        );
    }
    for value in [
        BsonValue::Int32(-1),
        BsonValue::Int64(-1),
        BsonValue::Double(-0.1),
        BsonValue::Double(f64::NAN),
        BsonValue::Double(f64::INFINITY),
        BsonValue::Boolean(true),
        BsonValue::Decimal128(crate::document::BsonDecimal128::parse("60").unwrap()),
        BsonValue::Null,
        BsonValue::from("60"),
    ] {
        assert_eq!(
            plan(vec![model(
                document([("created", BsonValue::Int32(1))]),
                [("expireAfterSeconds", value)]
            )])
            .err()
            .unwrap()
            .code,
            72
        );
    }
    assert_eq!(
        plan(vec![model(
            document([("value", BsonValue::Int32(1))]),
            [("background", BsonValue::Int32(1))]
        )])
        .err()
        .unwrap()
        .code,
        72
    );
}

#[test]
fn compatibility_preserves_validation_name_bounds_and_deadlines() {
    let hashed = || model(document([("value", BsonValue::from("hashed"))]), []);
    for bad in [
        model(document([("bad", BsonValue::from("geoHaystack"))]), []),
        model(document([("bad", BsonValue::Boolean(true))]), []),
        model(document([("$bad", BsonValue::from("hashed"))]), []),
        model(
            document([("bad", BsonValue::from("hashed"))]),
            [("name", BsonValue::from("_id_"))],
        ),
        model(
            document([("bad", BsonValue::from("hashed"))]),
            [(
                "partialFilterExpression",
                BsonValue::Document(document([(
                    "v",
                    BsonValue::Document(document([("$unknown", BsonValue::Int32(1))])),
                )])),
            )],
        ),
    ] {
        assert!(plan(vec![hashed(), bad]).is_err());
    }
    let oversized =
        BsonDocument::from_entries([("v".repeat(250), BsonValue::from("hashed"))]).unwrap();
    assert_eq!(plan(vec![model(oversized, [])]).err().unwrap().code, 10334);
    let prepared = prepare(
        &request(vec![hashed()]),
        DocumentNamespace::new("compat", "items").unwrap(),
        Instant::now(),
        Duration::ZERO,
    );
    assert_eq!(prepared.err().unwrap().code, 50);
}

#[test]
fn largest_warning_batch_fits_the_advertised_reply_budget_before_mutation() {
    let prepared = plan(
        (0..1000)
            .map(|number| {
                model(
                    document([("value", BsonValue::from("hashed"))]),
                    [
                        (
                            "name",
                            BsonValue::String(format!("{number:04}{}", "n".repeat(251))),
                        ),
                        ("expireAfterSeconds", BsonValue::Int64(60)),
                        ("background", BsonValue::Boolean(true)),
                    ],
                )
            })
            .collect(),
    )
    .unwrap();
    assert_eq!(prepared.warnings.len(), 1000);
    let options = BsonCodecOptions::new().with_max_document_bytes(wire::MAX_BOOTSTRAP_BSON_BYTES);
    let before =
        encode_document_with_options(&reply(0, 0, prepared.warnings.clone()), &options).unwrap();
    let after = encode_document_with_options(&reply(1, 1001, prepared.warnings), &options).unwrap();
    assert_eq!(before.len(), after.len());
    assert!(after.len() > 255_000);
}

fn text_warning(name: &str) -> BsonValue {
    BsonValue::Document(document([
        ("name", BsonValue::from(name)),
        ("skipped", BsonValue::Boolean(true)),
        (
            "reducedBehavior",
            BsonValue::Array(vec![BsonValue::from(
                "text: entire index is skipped; $text queries are not supported",
            )]),
        ),
    ]))
}

#[test]
fn text_models_skip_the_entire_declaration_without_claiming_partial_builds() {
    let prepared = plan(vec![
        model(
            document([
                ("tenant", BsonValue::Int32(-1)),
                ("body", BsonValue::from("text")),
                ("token", BsonValue::from("hashed")),
            ]),
            [
                ("expireAfterSeconds", BsonValue::Double(0.5)),
                ("background", BsonValue::Boolean(true)),
                ("sparse", BsonValue::Boolean(true)),
            ],
        ),
        model(
            document([("email", BsonValue::Int32(1))]),
            [("unique", BsonValue::Boolean(true))],
        ),
        model(
            document([("title", BsonValue::from("text"))]),
            [("name", BsonValue::from("email_1"))],
        ),
    ])
    .unwrap();
    assert_eq!(prepared.request.indexes().len(), 1);
    assert_eq!(
        prepared.request.indexes()[0].keys(),
        &document([("email", BsonValue::Int32(1))])
    );
    assert!(prepared.request.indexes()[0].unique());
    assert_eq!(
        prepared.warnings,
        vec![
            text_warning("tenant_-1_body_text_token_hashed"),
            text_warning("email_1")
        ]
    );
}

#[test]
fn all_text_batches_use_only_a_builtin_noop_and_do_not_compile_unused_filters() {
    for filter in [
        BsonDocument::new(),
        document([(
            "value",
            BsonValue::Document(document([("$unknown", BsonValue::Int32(1))])),
        )]),
    ] {
        let prepared = plan(vec![model(
            document([("body", BsonValue::from("text"))]),
            [("partialFilterExpression", BsonValue::Document(filter))],
        )])
        .unwrap();
        assert_eq!(prepared.request.indexes().len(), 1);
        let noop = &prepared.request.indexes()[0];
        assert_eq!(noop.keys(), &document([("_id", BsonValue::Int32(1))]));
        assert!(noop.name().is_none());
        assert!(!noop.unique() && !noop.sparse() && noop.partial_filter().is_none());
        assert_eq!(prepared.warnings, vec![text_warning("body_text")]);
    }
}

#[test]
fn skipped_id_text_requests_preserve_requested_names_without_changing_the_builtin() {
    for name in ["_id", "_id_", "_id_text", "alias"] {
        let prepared = plan(vec![model(
            document([("_id", BsonValue::from("text"))]),
            [
                ("name", BsonValue::from(name)),
                ("unique", BsonValue::Boolean(false)),
                ("expireAfterSeconds", BsonValue::Int32(1)),
            ],
        )])
        .unwrap();
        assert_eq!(prepared.warnings, vec![text_warning(name)]);
        assert_eq!(
            prepared.request.indexes()[0].keys(),
            &document([("_id", BsonValue::Int32(1))])
        );
    }
}

#[test]
fn text_skipping_never_bypasses_eager_option_key_name_or_unique_validation() {
    for bad in [
        model(
            document([("body", BsonValue::from("text"))]),
            [("unique", BsonValue::Boolean(true))],
        ),
        model(
            document([("_id", BsonValue::from("text"))]),
            [("unique", BsonValue::Boolean(true))],
        ),
        model(
            document([("body", BsonValue::from("text"))]),
            [("name", BsonValue::from("_id_"))],
        ),
        model(
            document([("body", BsonValue::from("text"))]),
            [
                ("sparse", BsonValue::Boolean(true)),
                (
                    "partialFilterExpression",
                    BsonValue::Document(BsonDocument::new()),
                ),
            ],
        ),
        model(document([("$body", BsonValue::from("text"))]), []),
        model(
            document([
                ("body", BsonValue::from("text")),
                ("tail", BsonValue::Boolean(true)),
            ]),
            [],
        ),
        model(
            document([
                ("body", BsonValue::from("text")),
                ("tail", BsonValue::from("2dsphere")),
            ]),
            [],
        ),
        model(
            document([("body", BsonValue::from("text"))]),
            [("background", BsonValue::Int32(1))],
        ),
        model(
            document([("body", BsonValue::from("text"))]),
            [("expireAfterSeconds", BsonValue::Int32(-1))],
        ),
        model(
            document([("body", BsonValue::from("text"))]),
            [("partialFilterExpression", BsonValue::Int32(1))],
        ),
        model(
            document([("body", BsonValue::from("text"))]),
            [("weights", BsonValue::Document(BsonDocument::new()))],
        ),
    ] {
        assert!(
            plan(vec![
                model(document([("good", BsonValue::Int32(1))]), []),
                bad
            ])
            .is_err()
        );
    }
    let text = || model(document([("body", BsonValue::from("text"))]), []);
    assert!(
        plan(vec![
            text(),
            model(document([("tail", BsonValue::Boolean(true))]), [])
        ])
        .is_err()
    );
    assert!(plan(vec![text(); 1001]).is_err());
    let oversized =
        BsonDocument::from_entries([("v".repeat(251), BsonValue::from("text"))]).unwrap();
    assert_eq!(plan(vec![model(oversized, [])]).err().unwrap().code, 10334);
    let many_keys = BsonDocument::from_entries(
        (0..33).map(|number| (format!("field{number}"), BsonValue::from("text"))),
    )
    .unwrap();
    assert!(
        plan(vec![model(
            many_keys,
            [("name", BsonValue::from("bounded"))]
        )])
        .is_err()
    );
    assert_eq!(
        prepare(
            &request(vec![text()]),
            DocumentNamespace::new("compat", "items").unwrap(),
            Instant::now(),
            Duration::ZERO
        )
        .err()
        .unwrap()
        .code,
        50
    );
}

#[test]
fn maximum_skipped_text_batch_preserves_order_and_fits_the_reply_budget() {
    let names: Vec<_> = (0..1000)
        .map(|number| format!("{number:04}{}", "n".repeat(251)))
        .collect();
    let prepared = plan(
        names
            .iter()
            .map(|name| {
                model(
                    document([("body", BsonValue::from("text"))]),
                    [("name", BsonValue::from(name.as_str()))],
                )
            })
            .collect(),
    )
    .unwrap();
    assert_eq!(prepared.request.indexes().len(), 1);
    assert_eq!(
        prepared.warnings,
        names
            .iter()
            .map(|name| text_warning(name))
            .collect::<Vec<_>>()
    );
    assert!(
        encode_document_with_options(
            &reply(1, 1, prepared.warnings),
            &BsonCodecOptions::new().with_max_document_bytes(wire::MAX_BOOTSTRAP_BSON_BYTES)
        )
        .is_ok()
    );
}
