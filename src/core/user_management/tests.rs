use super::*;
use crate::core::{
    authorization::{DataDomain, Policy, Privilege, Scope},
    security_catalog::tests as fixtures,
};

fn name(realm: &str, name: &str) -> SecurityName {
    SecurityName::new(realm, name).unwrap()
}

#[test]
fn commands_derive_every_realm_requirement_without_accepting_caller_permissions() {
    let user = name("accounts", "private-user");
    let role = name("data", "reader");
    let account = Resource::security_realm("accounts").unwrap();
    let data = Resource::security_realm("data").unwrap();
    let command =
        UserManagementCommand::create(user.clone(), "private-password", [role.clone()]).unwrap();
    assert_eq!(
        command.requirements().unwrap(),
        [
            (Action::CreateUser, account.clone()),
            (Action::GrantRole, data.clone())
        ]
    );
    assert_eq!(
        UserManagementCommand::change_password(user.clone(), "password")
            .unwrap()
            .requirements()
            .unwrap(),
        [(Action::RotateCredentials, account.clone())]
    );
    assert_eq!(
        UserManagementCommand::drop_user(user.clone())
            .requirements()
            .unwrap(),
        [(Action::DropUser, account)]
    );
    assert_eq!(
        UserManagementCommand::grant_roles(user.clone(), [role.clone()])
            .unwrap()
            .requirements()
            .unwrap(),
        [(Action::GrantRole, data.clone())]
    );
    assert_eq!(
        UserManagementCommand::revoke_roles(user.clone(), [role.clone()])
            .unwrap()
            .requirements()
            .unwrap(),
        [(Action::RevokeRole, data)]
    );
    for secret in ["private-user", "private-password", "accounts", "reader"] {
        assert!(!format!("{command:?}").contains(secret));
    }
    assert!(UserManagementCommand::create(user.clone(), "", []).is_err());
    assert!(
        UserManagementCommand::create(user.clone(), &"x".repeat(MAX_SCRAM_PASSWORD_BYTES + 1), [])
            .is_err()
    );
    assert!(
        UserManagementCommand::create(user.clone(), "password", std::iter::repeat(role.clone()))
            .is_err()
    );
    assert!(UserManagementCommand::grant_roles(user.clone(), []).is_err());
    assert!(UserManagementCommand::revoke_roles(user, []).is_err());
}

#[test]
fn membership_union_and_removal_are_bounded_atomic_and_keep_existing_grants() {
    let (mut catalog, user, role) = fixtures::setup();
    let writer = name("app", "writer");
    let resource = Resource::object(DataDomain::Document, "app", "items").unwrap();
    catalog
        .create_role(
            writer.clone(),
            Policy::new([
                Privilege::new(Action::InsertData, Scope::exact(resource.clone())).unwrap(),
            ])
            .unwrap(),
        )
        .unwrap();
    let attempt = catalog.begin_scram(&user).unwrap();
    let (_, message, proof) = fixtures::exchange(&attempt, fixtures::PASSWORD);
    let principal = catalog
        .complete_scram(attempt, message.as_bytes(), &proof)
        .unwrap()
        .into_parts()
        .0;
    catalog
        .grant_user_roles(&user, [writer.clone(), writer.clone()])
        .unwrap();
    catalog
        .authorize(&principal, Action::ReadData, &resource)
        .unwrap();
    catalog
        .authorize(&principal, Action::InsertData, &resource)
        .unwrap();
    let before = catalog.to_record().unwrap();
    assert!(
        catalog
            .grant_user_roles(&user, [name("app", "missing")])
            .is_err()
    );
    assert!(
        catalog
            .revoke_user_roles(&user, std::iter::repeat(role.clone()))
            .is_err()
    );
    assert_eq!(before.as_bytes(), catalog.to_record().unwrap().as_bytes());
    catalog
        .revoke_user_roles(&user, [role, name("app", "missing")])
        .unwrap();
    assert!(
        catalog
            .authorize(&principal, Action::ReadData, &resource)
            .is_err()
    );
    catalog
        .authorize(&principal, Action::InsertData, &resource)
        .unwrap();
}
