use super::*;

fn exact_grant(collection: &str) -> Privilege {
    Privilege::new(
        Action::ReadData,
        Scope::exact(Resource::object(DataDomain::Document, "app", collection).unwrap()),
    )
    .unwrap()
}

#[test]
fn document_role_grants_preserve_membership_existing_policy_and_duplicate_unions() {
    use super::super::tests::{credential, login};
    let mut catalog = SecurityCatalog::new();
    let role = SecurityName::new("app", "custom").unwrap();
    let user = SecurityName::new("accounts", "alice").unwrap();
    let original = Privilege::new(
        Action::ReadData,
        Scope::exact(Resource::object(DataDomain::Relational, "app", "legacy").unwrap()),
    )
    .unwrap();
    catalog
        .create_role(role.clone(), Policy::new([original.clone()]).unwrap())
        .unwrap();
    catalog
        .create_user(user.clone(), credential(), [role.clone()])
        .unwrap();
    let principal = login(&catalog, &user);
    let additions = Policy::new([exact_grant("posts")]).unwrap();
    catalog
        .grant_document_role_privileges(&role, additions.clone())
        .unwrap();
    let before = catalog.to_record().unwrap();
    catalog
        .grant_document_role_privileges(&role, additions)
        .unwrap();
    catalog
        .grant_document_role_privileges(&role, Policy::default())
        .unwrap();
    assert_eq!(before.as_bytes(), catalog.to_record().unwrap().as_bytes());
    assert_eq!(catalog.roles[&role].privilege_count(), 2);
    assert!(
        catalog.roles[&role]
            .privileges()
            .any(|grant| grant == &original)
    );
    assert_eq!(catalog.users[&user].roles, BTreeSet::from([role.clone()]));
    catalog
        .authorize(
            &principal,
            Action::ReadData,
            &Resource::object(DataDomain::Document, "app", "posts").unwrap(),
        )
        .unwrap();
    let forbidden = Policy::new([original]).unwrap();
    assert_eq!(
        catalog
            .grant_document_role_privileges(&role, forbidden)
            .unwrap_err()
            .kind(),
        EngineErrorKind::InvalidArgument
    );
    assert_eq!(before.as_bytes(), catalog.to_record().unwrap().as_bytes());
    let missing = catalog
        .grant_document_role_privileges(
            &SecurityName::new("app", "missing").unwrap(),
            Policy::default(),
        )
        .unwrap_err();
    assert!(
        std::error::Error::source(&missing)
            .unwrap()
            .is::<RoleNotFound>()
    );
    assert_eq!(before.as_bytes(), catalog.to_record().unwrap().as_bytes());
    let restored = SecurityCatalog::from_record(before.as_bytes()).unwrap();
    let principal = login(&restored, &user);
    restored
        .authorize(
            &principal,
            Action::ReadData,
            &Resource::object(DataDomain::Document, "app", "posts").unwrap(),
        )
        .unwrap();
}

#[test]
fn document_role_grants_reject_role_and_affected_user_union_overflow_atomically() {
    use super::super::tests::credential;
    for user_union in [false, true] {
        let mut catalog = SecurityCatalog::new();
        let role = SecurityName::new("app", "target").unwrap();
        let other = SecurityName::new("app", "other").unwrap();
        let full = Policy::new((0..256).map(|n| exact_grant(&format!("c{n}")))).unwrap();
        if user_union {
            catalog
                .create_role(role.clone(), Policy::default())
                .unwrap();
            catalog.create_role(other.clone(), full).unwrap();
            catalog
                .create_user(
                    SecurityName::new("accounts", "alice").unwrap(),
                    credential(),
                    [role.clone(), other],
                )
                .unwrap();
        } else {
            catalog.create_role(role.clone(), full).unwrap();
        }
        let before = catalog.to_record().unwrap();
        assert_eq!(
            catalog
                .grant_document_role_privileges(&role, Policy::new([exact_grant("extra")]).unwrap())
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        assert_eq!(before.as_bytes(), catalog.to_record().unwrap().as_bytes());
        // Duplicate additions at the limit succeed without changing authority.
        catalog
            .grant_document_role_privileges(&role, Policy::new([exact_grant("c0")]).unwrap())
            .unwrap();
    }
}

#[test]
fn mongo_data_role_profiles_have_exact_action_and_namespace_boundaries() {
    for database in ["app", "local", "Local", "é.*"] {
        for profile in [MongoDataRole::Read, MongoDataRole::ReadWrite] {
            let policy = profile.policy(database).unwrap();
            let writable = profile == MongoDataRole::ReadWrite;
            for &action in Action::ALL {
                let allowed_object = matches!(action, Action::ReadData | Action::ListIndexes)
                    || writable
                        && matches!(
                            action,
                            Action::InsertData
                                | Action::UpdateData
                                | Action::DeleteData
                                | Action::CreateObject
                                | Action::DropObject
                                | Action::CreateIndex
                                | Action::DropIndex
                        );
                for collection in ["items", "system.js", "systemx.users", "SYSTEM.users"] {
                    assert_eq!(
                        policy.allows(
                            action,
                            &Resource::object(DataDomain::Document, database, collection).unwrap()
                        ),
                        allowed_object
                    );
                }
                for collection in [
                    "system.users",
                    "system.roles",
                    "system.profile",
                    "system.views",
                    "system.buckets.items",
                    "system.js.child",
                ] {
                    assert!(!policy.allows(
                        action,
                        &Resource::object(DataDomain::Document, database, collection).unwrap()
                    ));
                }
                assert_eq!(
                    policy.allows(
                        action,
                        &Resource::object(DataDomain::Document, database, "replset.config")
                            .unwrap()
                    ),
                    allowed_object && database != "local"
                );
                assert_eq!(
                    policy.allows(
                        action,
                        &Resource::database(DataDomain::Document, database).unwrap()
                    ),
                    matches!(action, Action::ConnectDatabase | Action::ListObjects)
                        || writable && action == Action::CreateDatabase
                );
                for other in [
                    Resource::object(DataDomain::Document, "other", "items").unwrap(),
                    Resource::database(DataDomain::Document, "other").unwrap(),
                    Resource::object(DataDomain::Relational, "app", "items").unwrap(),
                    Resource::data_domain(DataDomain::Document),
                    Resource::security_realm(database).unwrap(),
                    Resource::server(),
                ] {
                    assert!(!policy.allows(action, &other));
                }
            }
        }
    }
}

#[test]
fn mongo_data_role_provisioning_never_overwrites_collisions_or_partially_publishes() {
    for existing in ["read", "readWrite"] {
        let mut catalog = SecurityCatalog::new();
        catalog
            .create_role(
                SecurityName::new("app", existing).unwrap(),
                Policy::default(),
            )
            .unwrap();
        let before = catalog.to_record().unwrap();
        assert_eq!(
            catalog
                .provision_mongo_data_roles("app")
                .unwrap_err()
                .kind(),
            EngineErrorKind::UniqueViolation
        );
        assert_eq!(catalog.to_record().unwrap().as_bytes(), before.as_bytes());
    }
    let mut catalog = SecurityCatalog::new();
    for index in 0..MAX_SECURITY_ROLES - 1 {
        catalog
            .create_role(
                SecurityName::new("existing", &index.to_string()).unwrap(),
                Policy::default(),
            )
            .unwrap();
    }
    let before = catalog.to_record().unwrap();
    assert_eq!(
        catalog
            .provision_mongo_data_roles("app")
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    assert_eq!(catalog.to_record().unwrap().as_bytes(), before.as_bytes());
    catalog
        .drop_role(&SecurityName::new("existing", "0").unwrap())
        .unwrap();
    catalog.provision_mongo_data_roles("app").unwrap();
    assert_eq!(catalog.role_count(), MAX_SECURITY_ROLES);
    assert_eq!(catalog.user_count(), 0);
    let before = catalog.to_record().unwrap();
    assert!(catalog.provision_mongo_data_roles("app").is_err());
    assert_eq!(catalog.to_record().unwrap().as_bytes(), before.as_bytes());
}

#[test]
fn mongo_data_role_profiles_validate_names_and_survive_records_without_implicit_assignment() {
    use super::super::tests::{credential, login};
    let mut catalog = SecurityCatalog::new();
    let before = catalog.to_record().unwrap();
    for database in ["", "bad\0name", &"x".repeat(64)] {
        assert!(catalog.provision_mongo_data_roles(database).is_err());
        assert_eq!(catalog.to_record().unwrap().as_bytes(), before.as_bytes());
    }
    catalog.provision_mongo_data_roles("app").unwrap();
    let user = SecurityName::new("accounts", "alice").unwrap();
    catalog.create_user(user.clone(), credential(), []).unwrap();
    let target = Resource::object(DataDomain::Document, "app", "items").unwrap();
    let principal = login(&catalog, &user);
    assert!(
        catalog
            .authorize(&principal, Action::ReadData, &target)
            .is_err()
    );
    let role = SecurityName::new("app", "readWrite").unwrap();
    catalog.grant_user_roles(&user, [role.clone()]).unwrap();
    catalog
        .authorize(&principal, Action::InsertData, &target)
        .unwrap();
    let record = catalog.to_record().unwrap();
    assert_eq!(&record.as_bytes()[..8], b"BRKSEC02");
    let mut restored = SecurityCatalog::from_record(record.as_bytes()).unwrap();
    assert_eq!(restored.to_record().unwrap().as_bytes(), record.as_bytes());
    let principal = login(&restored, &user);
    restored
        .authorize(&principal, Action::InsertData, &target)
        .unwrap();
    // These are explicitly stored profiles, not protected/implicit built-ins.
    restored.replace_role(&role, Policy::default()).unwrap();
    assert!(
        restored
            .authorize(&principal, Action::ReadData, &target)
            .is_err()
    );
    restored.drop_role(&role).unwrap();
    restored
        .create_role(role, MongoDataRole::ReadWrite.policy("app").unwrap())
        .unwrap();
    assert!(
        restored
            .authorize(&principal, Action::ReadData, &target)
            .is_err()
    );
}
