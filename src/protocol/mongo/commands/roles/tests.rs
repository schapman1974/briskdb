use super::*;
use crate::protocol::mongo::MongoResourceLimits;

#[test]
fn missing_roles_have_a_typed_mapping_not_a_generic_precondition_alias() {
    let mut catalog = crate::core::security_catalog::SecurityCatalog::new();
    let error = catalog
        .drop_role(&SecurityName::new("admin", "private-role").unwrap())
        .unwrap_err();
    let reply = CommandError::from(error);
    assert_eq!(reply.code, 31);
    assert_eq!(reply.name, "RoleNotFound");
    assert!(!format!("{:?}", reply.document()).contains("private"));
    // A revision conflict or invalid session must not be reported as absent.
    let error = EngineError::new(
        EngineErrorKind::FailedPrecondition,
        "private revision conflict",
    );
    assert_eq!(CommandError::from(error).code, 20);
}

fn request(value: BsonValue) -> Request {
    Request {
        request_id: 1,
        database: "admin".into(),
        body: fields([("dropRole", value), ("$db", BsonValue::from("admin"))]),
        sequences: vec![],
        more_to_come: false,
        legacy_handshake: false,
    }
}

fn prepared(input: &Request) -> Result<Prepared> {
    prepare_drop(input, Instant::now(), MongoResourceLimits::default())
}

#[test]
fn drop_role_accepts_only_bounded_names_and_acknowledged_local_options() {
    for concern in [
        None,
        Some(BsonDocument::new()),
        Some(fields([("w", BsonValue::Int64(1))])),
    ] {
        let mut input = request(BsonValue::from("private-role"));
        if let Some(concern) = concern {
            input
                .body
                .push("writeConcern", BsonValue::Document(concern))
                .unwrap();
        }
        let Command::DropRole(name) = prepared(&input).unwrap().command else {
            panic!("wrong command")
        };
        assert_eq!(name, SecurityName::new("admin", "private-role").unwrap());
        assert!(super::super::prepare(&input, false).unwrap().is_ok());
    }
    for value in [
        BsonValue::Int32(1),
        BsonValue::Null,
        BsonValue::from(""),
        BsonValue::from("x".repeat(1024)),
        BsonValue::from("bad\0name"),
    ] {
        assert!(prepared(&request(value)).is_err());
    }
    for (key, value) in [
        ("dropRole", BsonValue::from("duplicate-private-role")),
        ("$db", BsonValue::from("other")),
        ("comment", BsonValue::from("private-comment")),
        ("roles", BsonValue::Array(vec![])),
        ("writeConcern", BsonValue::Int32(1)),
        (
            "writeConcern",
            BsonValue::Document(fields([("w", BsonValue::Int32(0))])),
        ),
        (
            "writeConcern",
            BsonValue::Document(fields([("w", BsonValue::from("majority"))])),
        ),
        (
            "writeConcern",
            BsonValue::Document(fields([
                ("w", BsonValue::Int32(1)),
                ("w", BsonValue::Int32(1)),
            ])),
        ),
        (
            "writeConcern",
            BsonValue::Document(fields([("j", BsonValue::Boolean(true))])),
        ),
    ] {
        let mut input = request(BsonValue::from("private-role"));
        input.body.push(key, value).unwrap();
        let error = prepared(&input).err().expect("invalid option accepted");
        assert!(!format!("{:?}", error.document()).contains("private"));
    }
    let mut input = request(BsonValue::from("role"));
    input
        .body
        .push("writeConcern", BsonValue::Document(BsonDocument::new()))
        .unwrap();
    input
        .body
        .push("writeConcern", BsonValue::Document(BsonDocument::new()))
        .unwrap();
    assert!(prepared(&input).is_err());
    for variant in 0..4 {
        let mut input = request(BsonValue::from("role"));
        match variant {
            0 => input.more_to_come = true,
            1 => input.legacy_handshake = true,
            2 => input.database = "local".into(),
            _ => input.sequences.push(wire::DocumentSequence {
                identifier: "private-sequence".into(),
                documents: vec![],
            }),
        }
        assert!(prepared(&input).is_err());
    }
    assert_eq!(
        prepare_drop(
            &request(BsonValue::from("role")),
            Instant::now() - Duration::from_secs(30),
            MongoResourceLimits::default()
        )
        .err()
        .unwrap()
        .code,
        50
    );
}
