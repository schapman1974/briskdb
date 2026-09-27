use super::*;

fn request(value: BsonValue) -> Request {
    Request {
        request_id: 1,
        database: "app".into(),
        body: fields([("rolesInfo", value), ("$db", BsonValue::from("app"))]),
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
fn role_info_accepts_only_bounded_selectors_and_explicitly_unexpanded_metadata() {
    let named = BsonValue::Document(fields([
        ("role", BsonValue::from("private-role")),
        ("db", BsonValue::from("other")),
    ]));
    for value in [
        BsonValue::Int32(1),
        BsonValue::Int64(1),
        BsonValue::from("private-role"),
        named.clone(),
        BsonValue::Array(vec![named.clone(), BsonValue::from("reader")]),
        BsonValue::Array(vec![]),
    ] {
        assert!(check(&request(value)).is_ok());
    }
    for field in [
        "showPrivileges",
        "showBuiltinRoles",
        "showAuthenticationRestrictions",
    ] {
        let mut valid = request(named.clone());
        valid.body.push(field, BsonValue::Boolean(false)).unwrap();
        assert!(check(&valid).is_ok());
        for value in [
            BsonValue::Boolean(true),
            BsonValue::Int32(0),
            BsonValue::from("asUserFragment"),
        ] {
            let mut invalid = request(named.clone());
            invalid.body.push(field, value).unwrap();
            assert!(check(&invalid).is_err());
        }
    }
    for (field, value) in [
        ("rolesInfo", named.clone()),
        ("comment", BsonValue::from("private-comment")),
        ("filter", BsonValue::Document(BsonDocument::new())),
    ] {
        let mut invalid = request(named.clone());
        invalid.body.push(field, value).unwrap();
        let error = check(&invalid).err().unwrap();
        assert!(!format!("{:?}", error.document()).contains("private"));
    }
    for value in [
        BsonValue::Null,
        BsonValue::Boolean(true),
        BsonValue::Double(1.0),
        BsonValue::Int32(0),
        BsonValue::Array(vec![named; MAX_ROLE_INFO_SELECTORS + 1]),
        BsonValue::Document(fields([("forAllDBs", BsonValue::Boolean(true))])),
    ] {
        assert!(check(&request(value)).is_err());
    }
    let mut duplicate = fields([("role", BsonValue::from("a")), ("db", BsonValue::from("b"))]);
    duplicate.push("role", BsonValue::from("c")).unwrap();
    assert!(check(&request(BsonValue::Document(duplicate))).is_err());
    let mut one_way = request(BsonValue::Int32(1));
    one_way.more_to_come = true;
    assert!(check(&one_way).is_err());
    one_way.more_to_come = false;
    one_way.legacy_handshake = true;
    assert!(check(&one_way).is_err());
}
