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
        model(document([("bad", BsonValue::from("text"))]), []),
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
