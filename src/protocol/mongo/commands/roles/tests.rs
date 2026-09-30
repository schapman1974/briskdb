use super::*;
use crate::protocol::mongo::MongoResourceLimits;

fn revoking(privileges: Vec<BsonValue>) -> Request {
    let mut input = creation(vec![]);
    input.body = fields([
        ("revokePrivilegesFromRole", BsonValue::from("private-role")),
        ("privileges", BsonValue::Array(privileges)),
        ("$db", BsonValue::from("app")),
    ]);
    input
}

#[test]
fn revoke_role_privileges_preserves_exact_pairs_without_expanding_admission_grants() {
    use crate::core::authorization::{Action, DataDomain, Resource};
    let prepare =
        |input: &Request| prepare_revoke(input, Instant::now(), MongoResourceLimits::default());
    for privileges in [
        vec![],
        vec![privilege("app", "posts", &["find", "createCollection"])],
        vec![privilege("app", "", &["find"; 256])],
    ] {
        let input = revoking(privileges);
        let Command::RevokeRolePrivileges(name, policy) = prepare(&input).unwrap().command else {
            panic!("wrong command")
        };
        assert_eq!(name, SecurityName::new("app", "private-role").unwrap());
        let db = Resource::database(DataDomain::Document, "app").unwrap();
        assert!(!policy.allows(Action::ConnectDatabase, &db));
        assert!(!policy.allows(Action::CreateDatabase, &db));
        assert!(super::super::prepare(&input, false).unwrap().is_ok());
    }
    for privileges in [
        vec![privilege("other", "", &["find"])],
        vec![privilege("", "", &["find"])],
        vec![privilege("app", "", &["revokeRole"])],
        vec![privilege("app", "posts", &["connect_database"])],
        vec![privilege("app", "posts", &["find"; 257])],
        vec![privilege("app", "posts", &[])],
        vec![BsonValue::Null],
    ] {
        assert!(prepare(&revoking(privileges)).is_err());
    }
    for (field, value) in [
        ("revokePrivilegesFromRole", BsonValue::from("duplicate")),
        ("privileges", BsonValue::Array(vec![])),
        ("roles", BsonValue::Array(vec![])),
        ("createRole", BsonValue::from("private-role")),
        ("grantPrivilegesToRole", BsonValue::from("private-role")),
        ("$db", BsonValue::from("other")),
        ("comment", BsonValue::from("private-comment")),
        (
            "writeConcern",
            BsonValue::Document(fields([("w", BsonValue::Int32(0))])),
        ),
    ] {
        let mut input = revoking(vec![]);
        input.body.push(field, value).unwrap();
        let error = prepare(&input).err().unwrap();
        assert!(!format!("{:?}", error.document()).contains("private"));
    }
    for variant in 0..4 {
        let mut input = revoking(vec![]);
        match variant {
            0 => input.more_to_come = true,
            1 => input.legacy_handshake = true,
            2 => input.database = "local".into(),
            _ => input.sequences.push(wire::DocumentSequence {
                identifier: "private".into(),
                documents: vec![],
            }),
        }
        assert!(prepare(&input).is_err());
    }
    assert_eq!(
        prepare_revoke(
            &revoking(vec![]),
            Instant::now() - Duration::from_secs(30),
            MongoResourceLimits::default()
        )
        .err()
        .unwrap()
        .code,
        50
    );
    let mut catalog = crate::core::security_catalog::SecurityCatalog::new();
    let missing = catalog
        .revoke_document_role_privileges(
            &SecurityName::new("app", "private-role").unwrap(),
            crate::core::authorization::Policy::default(),
        )
        .unwrap_err();
    assert_eq!(CommandError::from(missing).code, 31);
}

fn granting(privileges: Vec<BsonValue>) -> Request {
    let mut input = creation(vec![]);
    input.body = fields([
        ("grantPrivilegesToRole", BsonValue::from("private-role")),
        ("privileges", BsonValue::Array(privileges)),
        ("$db", BsonValue::from("app")),
    ]);
    input
}

fn granted(input: &Request) -> Result<Prepared> {
    prepare_grant(input, Instant::now(), MongoResourceLimits::default())
}

#[test]
fn grant_role_privileges_reuses_bounded_data_policies_without_accepting_creation_fields() {
    for privileges in [vec![], vec![privilege("app", "posts", &["find", "insert"])]] {
        let input = granting(privileges);
        let Command::GrantRolePrivileges(name, _) = granted(&input).unwrap().command else {
            panic!("wrong command")
        };
        assert_eq!(name, SecurityName::new("app", "private-role").unwrap());
        assert!(super::super::prepare(&input, false).unwrap().is_ok());
    }
    for privileges in [
        vec![privilege("other", "", &["find"])],
        vec![privilege("", "", &["find"])],
        vec![privilege("app", "", &["grantRole"])],
        vec![privilege("app", "posts", &["find"; 256])],
        vec![BsonValue::Null],
    ] {
        assert!(granted(&granting(privileges)).is_err());
    }
    for (field, value) in [
        ("grantPrivilegesToRole", BsonValue::from("duplicate")),
        ("privileges", BsonValue::Array(vec![])),
        ("roles", BsonValue::Array(vec![])),
        ("createRole", BsonValue::from("private-role")),
        ("$db", BsonValue::from("other")),
        ("comment", BsonValue::from("private-comment")),
        (
            "writeConcern",
            BsonValue::Document(fields([("w", BsonValue::Int32(0))])),
        ),
    ] {
        let mut input = granting(vec![]);
        input.body.push(field, value).unwrap();
        let error = granted(&input).err().unwrap();
        assert!(!format!("{:?}", error.document()).contains("private"));
    }
    for variant in 0..4 {
        let mut input = granting(vec![]);
        match variant {
            0 => input.more_to_come = true,
            1 => input.legacy_handshake = true,
            2 => input.database = "local".into(),
            _ => input.sequences.push(wire::DocumentSequence {
                identifier: "private".into(),
                documents: vec![],
            }),
        }
        assert!(granted(&input).is_err());
    }
    assert_eq!(
        prepare_grant(
            &granting(vec![]),
            Instant::now() - Duration::from_secs(30),
            MongoResourceLimits::default()
        )
        .err()
        .unwrap()
        .code,
        50
    );
    let mut catalog = crate::core::security_catalog::SecurityCatalog::new();
    let missing = catalog
        .grant_document_role_privileges(
            &SecurityName::new("app", "private-role").unwrap(),
            crate::core::authorization::Policy::default(),
        )
        .unwrap_err();
    assert_eq!(CommandError::from(missing).code, 31);
}

fn creation(privileges: Vec<BsonValue>) -> Request {
    let mut request = request(BsonValue::from("unused"));
    request.database = "app".into();
    request.body = fields([
        ("createRole", BsonValue::from("private-custom")),
        ("privileges", BsonValue::Array(privileges)),
        ("roles", BsonValue::Array(vec![])),
        ("$db", BsonValue::from("app")),
    ]);
    request
}

fn privilege(database: &str, collection: &str, actions: &[&str]) -> BsonValue {
    BsonValue::Document(fields([
        (
            "resource",
            BsonValue::Document(fields([
                ("db", BsonValue::from(database)),
                ("collection", BsonValue::from(collection)),
            ])),
        ),
        (
            "actions",
            BsonValue::Array(
                actions
                    .iter()
                    .map(|value| BsonValue::from(*value))
                    .collect(),
            ),
        ),
    ]))
}

fn created(input: &Request) -> Result<Prepared> {
    prepare_create(input, Instant::now(), MongoResourceLimits::default())
}

#[test]
fn custom_data_roles_map_only_explicit_actions_and_never_expand_collection_scopes() {
    use crate::core::authorization::{Action, DataDomain, Resource};
    let input = creation(vec![
        privilege("app", "", &["find"]),
        privilege(
            "app",
            "posts",
            &["insert", "createCollection", "listIndexes"],
        ),
    ]);
    let Command::CreateRole(name, policy) = created(&input).unwrap().command else {
        panic!("wrong command")
    };
    assert_eq!(name, SecurityName::new("app", "private-custom").unwrap());
    assert!(super::super::prepare(&input, false).unwrap().is_ok());
    let object = |db, collection| Resource::object(DataDomain::Document, db, collection).unwrap();
    assert!(policy.allows(Action::ReadData, &object("app", "any")));
    assert!(!policy.allows(Action::ReadData, &object("app", "system.js")));
    assert!(!policy.allows(Action::ReadData, &object("other", "posts")));
    assert!(policy.allows(Action::InsertData, &object("app", "posts")));
    assert!(!policy.allows(Action::InsertData, &object("app", "other")));
    assert!(!policy.allows(Action::UpdateData, &object("app", "posts")));
    assert!(policy.allows(Action::CreateObject, &object("app", "posts")));
    assert!(!policy.allows(Action::CreateObject, &object("app", "other")));
    assert!(policy.allows(
        Action::CreateDatabase,
        &Resource::database(DataDomain::Document, "app").unwrap()
    ));
    assert!(!policy.allows(Action::GrantRole, &Resource::security_realm("app").unwrap()));
    let Command::CreateRole(_, empty) = created(&creation(vec![])).unwrap().command else {
        panic!("wrong command")
    };
    assert_eq!(empty.privilege_count(), 0);
    for action in [
        "find",
        "insert",
        "update",
        "remove",
        "createCollection",
        "dropCollection",
        "createIndex",
        "dropIndex",
        "listIndexes",
        "listCollections",
        "dropDatabase",
    ] {
        assert!(
            created(&creation(vec![privilege("app", "", &[action])])).is_ok(),
            "{action}"
        );
    }
}

#[test]
fn custom_roles_reject_unsupported_resources_options_and_unbounded_input() {
    // Expansion reserves room for database admission and counts duplicate inputs.
    assert!(created(&creation(vec![privilege("app", "posts", &["find"; 255])])).is_ok());
    assert!(created(&creation(vec![privilege("app", "posts", &["find"; 256])])).is_err());
    assert!(
        created(&creation(vec![privilege(
            "app",
            "posts",
            &["createCollection"; 127]
        )]))
        .is_ok()
    );
    assert!(
        created(&creation(vec![privilege(
            "app",
            "posts",
            &["createCollection"; 128]
        )]))
        .is_err()
    );
    for grants in [
        vec![privilege("other", "posts", &["find"])],
        vec![privilege("", "posts", &["find"])],
        vec![privilege("app", "posts", &["grantRole"])],
        vec![privilege("app", "posts", &["listCollections"])],
        vec![privilege("app", "posts", &["dropDatabase"])],
        vec![privilege("app", "posts", &[])],
        vec![BsonValue::Null],
        vec![privilege("app", "posts", &["find"]); 257],
        vec![privilege("app", "posts", &["find"; 257])],
    ] {
        assert!(created(&creation(grants)).is_err());
    }
    for (key, value) in [
        ("createRole", BsonValue::from("duplicate")),
        ("privileges", BsonValue::Array(vec![])),
        ("roles", BsonValue::Array(vec![])),
        ("$db", BsonValue::from("app")),
        ("authenticationRestrictions", BsonValue::Array(vec![])),
        ("comment", BsonValue::from("private-comment")),
        (
            "writeConcern",
            BsonValue::Document(fields([("w", BsonValue::Int32(0))])),
        ),
    ] {
        let mut input = creation(vec![]);
        input.body.push(key, value).unwrap();
        let error = created(&input).err().unwrap();
        assert!(!format!("{:?}", error.document()).contains("private"));
    }
    for body in [
        fields([
            ("createRole", BsonValue::from("x")),
            ("roles", BsonValue::Array(vec![])),
        ]),
        fields([
            ("createRole", BsonValue::from("x")),
            ("privileges", BsonValue::Array(vec![])),
        ]),
        fields([
            ("createRole", BsonValue::from("x")),
            ("roles", BsonValue::Array(vec![BsonValue::from("read")])),
            ("privileges", BsonValue::Array(vec![])),
        ]),
    ] {
        let mut input = creation(vec![]);
        input.body = body;
        assert!(created(&input).is_err());
    }
    for variant in 0..4 {
        let mut input = creation(vec![]);
        match variant {
            0 => input.more_to_come = true,
            1 => input.legacy_handshake = true,
            2 => input.database = "local".into(),
            _ => input.sequences.push(wire::DocumentSequence {
                identifier: "private".into(),
                documents: vec![],
            }),
        }
        assert!(created(&input).is_err());
    }
    assert_eq!(
        prepare_create(
            &creation(vec![]),
            Instant::now() - Duration::from_secs(30),
            MongoResourceLimits::default()
        )
        .err()
        .unwrap()
        .code,
        50
    );
    // Nested duplicate/unknown keys must not select an arbitrary first value.
    for resource in [
        fields([
            ("db", BsonValue::from("app")),
            ("db", BsonValue::from("other")),
            ("collection", BsonValue::from("")),
        ]),
        fields([("cluster", BsonValue::Boolean(true))]),
        fields([("anyResource", BsonValue::Boolean(true))]),
    ] {
        let grant = fields([
            ("resource", BsonValue::Document(resource)),
            ("actions", BsonValue::Array(vec![BsonValue::from("find")])),
        ]);
        assert!(created(&creation(vec![BsonValue::Document(grant)])).is_err());
    }
}

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
