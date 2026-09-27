use super::*;

fn object(domain: DataDomain, database: &str, name: &str) -> Resource {
    Resource::object(domain, database, name).unwrap()
}

fn grant(action: Action, scope: Scope) -> Privilege {
    Privilege::new(action, scope).unwrap()
}

fn read_policy(domain: DataDomain, database: &str) -> Policy {
    Policy::new([grant(
        Action::ReadData,
        Scope::database(domain, database).unwrap(),
    )])
    .unwrap()
}

fn resources() -> Vec<Resource> {
    vec![
        Resource::server(),
        Resource::security_realm("admin").unwrap(),
        Resource::data_domain(DataDomain::Relational),
        Resource::data_domain(DataDomain::Document),
        Resource::database(DataDomain::Relational, "app").unwrap(),
        Resource::database(DataDomain::Document, "app").unwrap(),
        object(DataDomain::Relational, "app", "users"),
        object(DataDomain::Document, "app", "users"),
    ]
}

#[test]
fn empty_policy_and_every_misscoped_action_deny_by_default() {
    let empty = Policy::default();
    for resource in resources() {
        for &action in Action::ALL {
            assert!(!empty.allows(action, &resource));
            assert_eq!(
                empty.authorize(action, &resource).unwrap_err().kind(),
                EngineErrorKind::PermissionDenied
            );
            let exact = Privilege::new(action, Scope::exact(resource.clone()));
            if action.resource_kind() == resource.kind() {
                assert!(exact.is_ok());
            } else {
                assert_eq!(exact.unwrap_err().kind(), EngineErrorKind::InvalidArgument);
            }
        }
    }
    assert!(empty.authorize_all([]).is_err());
    assert_eq!(empty, Policy::new([]).unwrap());
    assert_eq!(empty, Policy::combine([]).unwrap());
}

#[test]
fn complete_exact_action_resource_matrix_has_no_implicit_privileges() {
    let resources = resources();
    for source in &resources {
        for &action in Action::ALL {
            if action.resource_kind() != source.kind() {
                continue;
            }
            let policy = Policy::new([grant(action, Scope::exact(source.clone()))]).unwrap();
            for candidate in &resources {
                for &other_action in Action::ALL {
                    assert_eq!(
                        policy.allows(other_action, candidate),
                        action == other_action && source == candidate
                    );
                }
            }
        }
    }
}

#[test]
fn database_scopes_do_not_cross_domains_realms_names_or_prefixes() {
    let policy = read_policy(DataDomain::Document, "app");
    assert!(policy.allows(
        Action::ReadData,
        &object(DataDomain::Document, "app", "users")
    ));
    for target in [
        object(DataDomain::Relational, "app", "users"),
        object(DataDomain::Document, "app2", "users"),
        object(DataDomain::Document, "App", "users"),
        Resource::database(DataDomain::Document, "app").unwrap(),
        Resource::data_domain(DataDomain::Document),
        Resource::security_realm("app").unwrap(),
        Resource::server(),
    ] {
        assert!(!policy.allows(Action::ReadData, &target));
    }
    let target = object(DataDomain::Document, "app", "users");
    for &action in Action::ALL {
        assert_eq!(policy.allows(action, &target), action == Action::ReadData);
    }
    assert!(
        Privilege::new(
            Action::CreateUser,
            Scope::database(DataDomain::Document, "app").unwrap()
        )
        .is_err()
    );
}

#[test]
fn exact_database_and_descendant_scopes_are_intentionally_distinct() {
    let database = Resource::database(DataDomain::Document, "app").unwrap();
    assert!(Privilege::new(Action::ReadData, Scope::exact(database.clone())).is_err());
    let connect = Policy::new([grant(
        Action::ConnectDatabase,
        Scope::exact(database.clone()),
    )])
    .unwrap();
    assert!(connect.allows(Action::ConnectDatabase, &database));
    assert!(!connect.allows(
        Action::ReadData,
        &object(DataDomain::Document, "app", "users")
    ));
    assert!(!connect.allows(
        Action::ConnectDatabase,
        &Resource::database(DataDomain::Document, "other").unwrap()
    ));
    assert!(read_policy(DataDomain::Document, "app").allows(
        Action::ReadData,
        &object(DataDomain::Document, "app", "a_new_collection")
    ));
}

#[test]
fn broad_data_scopes_never_grant_user_management_or_server_administration() {
    let data = Policy::new([
        grant(Action::ReadData, Scope::all_databases(DataDomain::Document)),
        grant(
            Action::ListDatabases,
            Scope::all_databases(DataDomain::Document),
        ),
    ])
    .unwrap();
    for database in ["app", "other", "admin"] {
        assert!(data.allows(
            Action::ReadData,
            &object(DataDomain::Document, database, "users")
        ));
        assert!(!data.allows(
            Action::ReadData,
            &object(DataDomain::Relational, database, "users")
        ));
        for &action in Action::ALL {
            assert!(!data.allows(action, &Resource::security_realm(database).unwrap()));
        }
    }
    assert!(data.allows(
        Action::ListDatabases,
        &Resource::data_domain(DataDomain::Document)
    ));
    assert!(!data.allows(
        Action::ListDatabases,
        &Resource::data_domain(DataDomain::Relational)
    ));
    for &action in Action::ALL {
        assert!(!data.allows(action, &Resource::server()));
        if matches!(
            action.resource_kind(),
            ResourceKind::SecurityRealm | ResourceKind::Server
        ) {
            assert!(Privilege::new(action, Scope::all_databases(DataDomain::Document)).is_err());
        }
    }
}

#[test]
fn realm_privileges_never_imply_data_server_or_other_identity_actions() {
    let role_grant = Policy::new([grant(Action::GrantRole, Scope::all_security_realms())]).unwrap();
    for realm in ["admin", "app"] {
        let resource = Resource::security_realm(realm).unwrap();
        assert!(role_grant.allows(Action::GrantRole, &resource));
        for &action in Action::ALL {
            assert_eq!(
                role_grant.allows(action, &resource),
                action == Action::GrantRole
            );
        }
    }
    let exact = Policy::new([grant(
        Action::RotateCredentials,
        Scope::exact(Resource::security_realm("app").unwrap()),
    )])
    .unwrap();
    assert!(exact.allows(
        Action::RotateCredentials,
        &Resource::security_realm("app").unwrap()
    ));
    assert!(!exact.allows(
        Action::RotateCredentials,
        &Resource::security_realm("App").unwrap()
    ));
    assert!(!role_grant.allows(
        Action::ReadData,
        &object(DataDomain::Document, "admin", "system.users")
    ));
    assert!(!role_grant.allows(Action::ManageServer, &Resource::server()));
    assert!(Privilege::new(Action::ReadData, Scope::all_security_realms()).is_err());
}

#[test]
fn document_names_use_exact_utf8_without_globs_or_normalization() {
    let exact = object(DataDomain::Document, "App", "events.*");
    let policy = Policy::new([grant(Action::ReadData, Scope::exact(exact.clone()))]).unwrap();
    assert!(policy.allows(Action::ReadData, &exact));
    assert!(!policy.allows(
        Action::ReadData,
        &object(DataDomain::Document, "App", "events.users")
    ));
    assert!(!policy.allows(
        Action::ReadData,
        &object(DataDomain::Document, "app", "events.*")
    ));
    let unicode = object(DataDomain::Document, "é", "users");
    let policy = Policy::new([grant(Action::ReadData, Scope::exact(unicode.clone()))]).unwrap();
    assert!(policy.allows(Action::ReadData, &unicode));
    assert!(!policy.allows(
        Action::ReadData,
        &object(DataDomain::Document, "e\u{301}", "users")
    ));
    let dotted = object(DataDomain::Document, "a.b", "c");
    let policy = Policy::new([grant(Action::ReadData, Scope::exact(dotted))]).unwrap();
    assert!(!policy.allows(Action::ReadData, &object(DataDomain::Document, "a", "b.c")));
}

#[cfg(feature = "documents")]
#[test]
fn document_resource_validation_matches_the_existing_catalog_contract() {
    for database in [
        "app".to_owned(),
        "Case".to_owned(),
        "é".to_owned(),
        String::new(),
        "secret\0name".to_owned(),
        "d".repeat(63),
        "d".repeat(64),
    ] {
        for collection in [
            "users".to_owned(),
            "system.users".to_owned(),
            "a.b".to_owned(),
            String::new(),
            "secret\0name".to_owned(),
            "c".repeat(191),
            "c".repeat(192),
            "é".repeat(128),
        ] {
            assert_eq!(
                Resource::object(DataDomain::Document, &database, &collection).is_ok(),
                crate::document::DocumentNamespace::new(database.clone(), collection).is_ok()
            );
        }
    }
}

#[test]
fn constructors_bound_names_and_reject_noncanonical_or_internal_sql_objects() {
    for name in [
        "".to_owned(),
        "x".repeat(64),
        "secret\0name".to_owned(),
        "🦀".repeat(16),
    ] {
        assert!(Resource::database(DataDomain::Document, &name).is_err());
        assert!(Resource::security_realm(&name).is_err());
        assert!(Scope::database(DataDomain::Document, &name).is_err());
    }
    let database = "d".repeat(63);
    assert!(Resource::database(DataDomain::Document, &database).is_ok());
    assert!(Resource::object(DataDomain::Document, &database, &"x".repeat(191)).is_ok());
    assert!(Resource::object(DataDomain::Document, &database, &"x".repeat(192)).is_err());
    for name in [
        "",
        "secret\0name",
        "Upper",
        "with.dot",
        "briskdb",
        "briskdb_users",
        "sqlite_master",
    ] {
        assert!(Resource::object(DataDomain::Relational, "app", name).is_err());
        assert!(Resource::database(DataDomain::Relational, name).is_err());
    }
    assert!(Resource::object(DataDomain::Relational, &database, &"t".repeat(63)).is_ok());
    assert!(Resource::object(DataDomain::Relational, "app", &"t".repeat(64)).is_err());
    assert!(Resource::object(DataDomain::Document, "app", "").is_err());
    assert!(Resource::object(DataDomain::Document, "app", "secret\0name").is_err());
}

#[test]
fn known_action_codes_are_unique_round_trip_and_never_accept_aliases() {
    let mut seen = BTreeSet::new();
    for &action in Action::ALL {
        assert!(seen.insert(action.code()));
        assert_eq!(Action::from_code(action.code()).unwrap(), action);
    }
    assert_eq!(seen.len(), 26);
    for invalid_code in ["*", "all", "root", "ReadData", "read_data ", "read_data\0"] {
        assert_eq!(
            Action::from_code(invalid_code).unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
    }
}

#[test]
fn role_union_is_order_independent_deduplicated_and_non_mutating() {
    let target = object(DataDomain::Document, "app", "users");
    let read = read_policy(DataDomain::Document, "app");
    let write = Policy::new([grant(Action::InsertData, Scope::exact(target.clone()))]).unwrap();
    let first = Policy::combine([&read, &write, &read]).unwrap();
    let second = Policy::combine([&write, &read]).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.privilege_count(), 2);
    assert!(first.allows(Action::ReadData, &target));
    assert!(first.allows(Action::InsertData, &target));
    assert!(!read.allows(Action::InsertData, &target));
    assert!(!write.allows(Action::ReadData, &target));
    assert!(!first.allows(Action::DropObject, &target));
}

fn full_policy() -> Policy {
    Policy::new((0..MAX_POLICY_PRIVILEGES).map(|number| {
        grant(
            Action::ReadData,
            Scope::exact(object(DataDomain::Document, "app", &format!("c{number}"))),
        )
    }))
    .unwrap()
}

#[test]
fn policy_input_role_union_and_requirement_iteration_are_bounded() {
    let full = full_policy();
    assert_eq!(full.privilege_count(), MAX_POLICY_PRIVILEGES);
    assert_eq!(
        Policy::combine(std::iter::repeat_n(&full, MAX_POLICY_ROLES)).unwrap(),
        full
    );
    assert_eq!(
        Policy::combine(std::iter::repeat(&Policy::default()))
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    let extra = Policy::new([grant(
        Action::ReadData,
        Scope::exact(object(DataDomain::Document, "other", "c")),
    )])
    .unwrap();
    assert_eq!(
        Policy::combine([&full, &extra]).unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
    assert_eq!(full.privilege_count(), MAX_POLICY_PRIVILEGES);

    let target = object(DataDomain::Document, "app", "c0");
    let privilege = grant(Action::ReadData, Scope::exact(target.clone()));
    let mut visited = 0;
    assert_eq!(
        Policy::new(std::iter::repeat(privilege).inspect(|_| visited += 1))
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    assert_eq!(visited, MAX_POLICY_PRIVILEGES + 1);
    full.authorize_all(std::iter::repeat_n(
        (Action::ReadData, &target),
        MAX_AUTHORIZATION_REQUIREMENTS,
    ))
    .unwrap();
    assert_eq!(
        full.authorize_all(std::iter::repeat((Action::ReadData, &target)))
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
}

#[test]
fn multi_resource_requests_require_every_grant_and_reject_empty_work() {
    let source = object(DataDomain::Document, "app", "source");
    let target = object(DataDomain::Document, "app", "target");
    let source_only = Policy::new([grant(Action::ReadData, Scope::exact(source.clone()))]).unwrap();
    assert_eq!(
        source_only
            .authorize_all([(Action::ReadData, &source), (Action::ReadData, &target)])
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    let both = read_policy(DataDomain::Document, "app");
    both.authorize_all([(Action::ReadData, &source), (Action::ReadData, &target)])
        .unwrap();
    assert_eq!(
        both.authorize_all([(Action::ReadData, &source), (Action::DeleteData, &target)])
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    assert_eq!(
        both.authorize_all([]).unwrap_err().kind(),
        EngineErrorKind::PermissionDenied
    );
}

#[test]
fn decisions_debug_and_errors_do_not_expose_resource_names() {
    fn thread_safe<T: Send + Sync>() {}
    thread_safe::<Policy>();
    let target = object(DataDomain::Document, "secret_tenant", "private_collection");
    let privilege = grant(Action::ReadData, Scope::exact(target.clone()));
    let scope = Scope::database(DataDomain::Document, "secret_tenant").unwrap();
    let policy = Policy::new([privilege.clone()]).unwrap();
    for text in [
        format!("{target:?}"),
        format!("{privilege:?}"),
        format!("{scope:?}"),
        format!("{policy:?}"),
        policy
            .authorize(Action::DeleteData, &target)
            .unwrap_err()
            .to_string(),
    ] {
        assert!(!text.contains("secret_tenant"));
        assert!(!text.contains("private_collection"));
    }
    assert_eq!(
        policy
            .authorize(Action::DeleteData, &target)
            .unwrap_err()
            .to_string(),
        "operation is not authorized"
    );
}
