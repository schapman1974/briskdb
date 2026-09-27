use super::*;
use crate::core::{
    authorization::{Privilege, Scope},
    security_catalog::tests as fixtures,
};

#[test]
fn role_info_assigned_names_are_visible_but_other_names_and_realms_require_view_roles() {
    let (mut catalog, user, role) = fixtures::setup();
    let principal = fixtures::login(&catalog, &user);
    let own = RoleInfoRequest::names([role.clone()]).unwrap();
    let rows = catalog
        .role_info(&principal, &own, ResultLimits::default())
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name(), &role);
    for secret in [role.name(), role.realm(), fixtures::PASSWORD] {
        assert!(!format!("{own:?}{rows:?}").contains(secret));
    }
    for request in [
        RoleInfoRequest::realm(role.realm()).unwrap(),
        RoleInfoRequest::realm("empty").unwrap(),
        RoleInfoRequest::names([
            role.clone(),
            SecurityName::new(role.realm(), "missing").unwrap(),
        ])
        .unwrap(),
        RoleInfoRequest::names([role.clone(), SecurityName::new("other", "missing").unwrap()])
            .unwrap(),
    ] {
        assert_eq!(
            catalog
                .role_info(&principal, &request, ResultLimits::new(1, 1).unwrap())
                .unwrap_err()
                .kind(),
            EngineErrorKind::PermissionDenied
        );
    }
    assert!(
        catalog
            .role_info(
                &principal,
                &RoleInfoRequest::names([]).unwrap(),
                ResultLimits::default()
            )
            .unwrap()
            .is_empty()
    );
    catalog.set_user_roles(&user, []).unwrap();
    assert!(
        catalog
            .role_info(&principal, &own, ResultLimits::default())
            .is_err()
    );
    catalog.set_user_roles(&user, [role.clone()]).unwrap();
    assert!(
        catalog
            .role_info(&principal, &own, ResultLimits::default())
            .is_ok()
    );
    catalog.drop_role(&role).unwrap();
    catalog
        .create_role(role.clone(), Policy::default())
        .unwrap();
    assert!(
        catalog
            .role_info(&principal, &own, ResultLimits::default())
            .is_err()
    );
    catalog.set_user_roles(&user, [role]).unwrap();
    catalog
        .rotate_credentials(&user, fixtures::credential())
        .unwrap();
    for request in [own, RoleInfoRequest::names([]).unwrap()] {
        assert!(
            catalog
                .role_info(&principal, &request, ResultLimits::default())
                .is_err()
        );
    }
}

#[test]
fn role_info_is_bounded_canonical_read_only_and_checks_current_authority_before_lookup() {
    let (mut catalog, user, role) = fixtures::setup();
    let reader = SecurityName::new("app", "another").unwrap();
    catalog
        .create_role(reader.clone(), Policy::default())
        .unwrap();
    catalog
        .replace_role(
            &role,
            Policy::new([Privilege::new(
                Action::ViewRoles,
                Scope::exact(Resource::security_realm("app").unwrap()),
            )
            .unwrap()])
            .unwrap(),
        )
        .unwrap();
    let principal = fixtures::login(&catalog, &user);
    let request = RoleInfoRequest::realm("app").unwrap();
    let before = catalog.to_record().unwrap();
    let rows = catalog
        .role_info(&principal, &request, ResultLimits::default())
        .unwrap();
    let mut expected = vec![role.clone(), reader.clone()];
    expected.sort();
    assert_eq!(
        rows.iter()
            .map(|row| row.name().clone())
            .collect::<Vec<_>>(),
        expected
    );
    let selection = RoleInfoRequest::names([
        role.clone(),
        reader.clone(),
        reader,
        SecurityName::new("app", "missing").unwrap(),
    ])
    .unwrap();
    assert_eq!(
        catalog
            .role_info(&principal, &selection, ResultLimits::default())
            .unwrap(),
        rows
    );
    for limits in [
        ResultLimits::new(1, 100_000).unwrap(),
        ResultLimits::new(10, 400).unwrap(),
    ] {
        assert_eq!(
            catalog
                .role_info(&principal, &request, limits)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
    }
    assert!(
        catalog
            .role_info(
                &principal,
                &RoleInfoRequest::names([]).unwrap(),
                ResultLimits::new(1, 1).unwrap()
            )
            .is_err()
    );
    assert_eq!(catalog.to_record().unwrap().as_bytes(), before.as_bytes());
    assert!(RoleInfoRequest::names(std::iter::repeat(role.clone())).is_err());
    assert!(RoleInfoRequest::realm("").is_err());
    assert!(RoleInfoRequest::realm(&"x".repeat(64)).is_err());
    let restored = SecurityCatalog::from_record(before.as_bytes()).unwrap();
    assert!(
        restored
            .role_info(&principal, &request, ResultLimits::default())
            .is_err()
    );
    catalog.replace_role(&role, Policy::default()).unwrap();
    assert!(
        catalog
            .role_info(&principal, &request, ResultLimits::default())
            .is_err()
    );
    catalog.drop_user(&user).unwrap();
    catalog
        .create_user(user, fixtures::credential(), [role])
        .unwrap();
    assert!(
        catalog
            .role_info(&principal, &selection, ResultLimits::default())
            .is_err()
    );
}
