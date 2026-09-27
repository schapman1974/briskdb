use super::*;

fn request(command: &str, fields: &[(&str, BsonValue)]) -> Request {
    let mut body =
        BsonDocument::from_entries([(command, BsonValue::from("private-user"))]).unwrap();
    for (key, value) in fields {
        body.push(*key, value.clone()).unwrap();
    }
    body.push("$db", BsonValue::from("admin")).unwrap();
    Request {
        request_id: 1,
        database: "admin".into(),
        body,
        sequences: vec![],
        more_to_come: false,
        legacy_handshake: false,
    }
}

fn prepare_request(request: &Request) -> Result<Prepared> {
    prepare(
        request,
        Instant::now(),
        crate::protocol::mongo::MongoResourceLimits::default(),
    )
}

#[test]
fn user_commands_accept_only_bounded_unambiguous_implemented_fields() {
    let fields = [
        ("pwd", BsonValue::from("private-password")),
        ("roles", BsonValue::Array(vec![BsonValue::from("reader")])),
    ];
    assert!(prepare_request(&request("createUser", &fields)).is_ok());
    let cross_realm = BsonDocument::from_entries([
        ("role", BsonValue::from("reader")),
        ("db", BsonValue::from("app")),
    ])
    .unwrap();
    assert!(
        prepare_request(&request(
            "grantRolesToUser",
            &[(
                "roles",
                BsonValue::Array(vec![BsonValue::Document(cross_realm.clone())])
            )]
        ))
        .is_ok()
    );
    for extra in [
        ("pwd", BsonValue::from("duplicate")),
        (
            "mechanisms",
            BsonValue::Array(vec![BsonValue::from("SCRAM-SHA-1")]),
        ),
        ("digestPassword", BsonValue::Boolean(false)),
        ("customData", BsonValue::Document(BsonDocument::new())),
        (
            "writeConcern",
            BsonValue::Document(BsonDocument::from_entries([("w", BsonValue::Int32(0))]).unwrap()),
        ),
        ("roles", BsonValue::Array(vec![])),
        ("comment", BsonValue::from("private-comment")),
    ] {
        let mut input = request("createUser", &fields);
        input.body.push(extra.0, extra.1).unwrap();
        let error = prepare_request(&input)
            .err()
            .expect("unsupported/duplicate option");
        assert!(!format!("{:?}", error.document()).contains("private"));
    }
    for mutation in 0..3 {
        let mut input = request("createUser", &fields);
        match mutation {
            0 => input.more_to_come = true,
            1 => input.legacy_handshake = true,
            _ => input.database = "local".into(),
        }
        assert!(prepare_request(&input).is_err());
    }
    for roles in [
        vec![],
        vec![BsonValue::from("reader"); MAX_POLICY_ROLES + 1],
        vec![BsonValue::Int32(1)],
    ] {
        assert!(
            prepare_request(&request(
                "grantRolesToUser",
                &[("roles", BsonValue::Array(roles))]
            ))
            .is_err()
        );
    }
    let mut duplicate_role = cross_realm;
    duplicate_role
        .push("db", BsonValue::from("elsewhere"))
        .unwrap();
    assert!(
        prepare_request(&request(
            "revokeRolesFromUser",
            &[(
                "roles",
                BsonValue::Array(vec![BsonValue::Document(duplicate_role)])
            )]
        ))
        .is_err()
    );
    assert!(
        prepare_request(&request(
            "updateUser",
            &[("roles", BsonValue::Array(vec![]))]
        ))
        .is_err()
    );
    assert!(
        prepare_request(&request(
            "updateUser",
            &[("pwd", BsonValue::from("password"))]
        ))
        .is_ok()
    );
}
