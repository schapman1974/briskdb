use super::*;

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
