use super::*;

fn input(value: BsonValue) -> Request {
    Request {
        request_id: 1,
        database: "accounts".into(),
        body: fields([("usersInfo", value), ("$db", BsonValue::from("accounts"))]),
        sequences: vec![],
        more_to_come: false,
        legacy_handshake: false,
    }
}

fn check(request: &Request) -> Result<Prepared> {
    prepare(
        request,
        Instant::now(),
        crate::protocol::mongo::MongoResourceLimits::default(),
    )
}

#[test]
fn user_info_accepts_only_bounded_exact_selectors_and_credential_free_options() {
    let named = BsonValue::Document(fields([
        ("user", BsonValue::from("secret-user")),
        ("db", BsonValue::from("elsewhere")),
    ]));
    for value in [
        BsonValue::Int32(1),
        BsonValue::Int64(1),
        BsonValue::from("secret-user"),
        named.clone(),
        BsonValue::Array(vec![named.clone(), BsonValue::from("another")]),
        BsonValue::Array(vec![]),
    ] {
        assert!(check(&input(value)).is_ok());
    }
    for (key, value) in [
        ("showCredentials", BsonValue::Boolean(true)),
        ("showPrivileges", BsonValue::Boolean(true)),
        ("showAuthenticationRestrictions", BsonValue::Boolean(true)),
        ("showCustomData", BsonValue::Int32(1)),
        (
            "filter",
            BsonValue::Document(fields([("user", BsonValue::from("secret-user"))])),
        ),
        ("comment", BsonValue::from("private-comment")),
        ("usersInfo", BsonValue::from("duplicate")),
    ] {
        let mut request = input(named.clone());
        request.body.push(key, value).unwrap();
        let error = check(&request)
            .err()
            .expect("unsupported or duplicated option");
        assert!(!format!("{:?}", error.document()).contains("secret"));
    }
    let mut valid = input(named.clone());
    for key in [
        "showCredentials",
        "showPrivileges",
        "showAuthenticationRestrictions",
        "showCustomData",
    ] {
        valid.body.push(key, BsonValue::Boolean(false)).unwrap();
    }
    valid
        .body
        .push("filter", BsonValue::Document(BsonDocument::new()))
        .unwrap();
    assert!(check(&valid).is_ok());
    for value in [
        BsonValue::Null,
        BsonValue::Int32(0),
        BsonValue::Boolean(true),
        BsonValue::Array(vec![named; MAX_USER_INFO_SELECTORS + 1]),
        BsonValue::Document(fields([("forAllDBs", BsonValue::Boolean(true))])),
    ] {
        assert!(check(&input(value)).is_err());
    }
    let mut duplicate = fields([("user", BsonValue::from("a")), ("db", BsonValue::from("b"))]);
    duplicate.push("db", BsonValue::from("c")).unwrap();
    assert!(check(&input(BsonValue::Document(duplicate))).is_err());
    let mut one_way = input(BsonValue::Int32(1));
    one_way.more_to_come = true;
    assert!(check(&one_way).is_err());
}
