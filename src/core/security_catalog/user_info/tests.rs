use super::*;
use crate::core::{
    authorization::{Privilege, Scope},
    security_catalog::tests as fixtures,
};

#[test]
fn exact_self_lookup_is_credential_free_but_never_grants_realm_or_other_user_discovery() {
    let (mut catalog, user, role) = fixtures::setup();
    let principal = fixtures::login(&catalog, &user);
    let own = UserInfoRequest::names([user.clone()]).unwrap();
    let info = catalog
        .user_info(&principal, &own, ResultLimits::default())
        .unwrap();
    assert_eq!(info.len(), 1);
    assert_eq!(info[0].name(), &user);
    assert_eq!(info[0].roles(), &[role]);
    for secret in [user.name(), user.realm(), fixtures::PASSWORD] {
        assert!(!format!("{own:?}{info:?}").contains(secret));
    }
    for request in [
        UserInfoRequest::realm(user.realm()).unwrap(),
        UserInfoRequest::realm("empty-realm").unwrap(),
        UserInfoRequest::names([
            user.clone(),
            SecurityName::new("elsewhere", "missing").unwrap(),
        ])
        .unwrap(),
    ] {
        assert_eq!(
            catalog
                .user_info(&principal, &request, ResultLimits::new(1, 1).unwrap())
                .unwrap_err()
                .kind(),
            EngineErrorKind::PermissionDenied
        );
    }
    assert!(
        catalog
            .user_info(
                &principal,
                &UserInfoRequest::names([]).unwrap(),
                ResultLimits::default()
            )
            .unwrap()
            .is_empty()
    );
    catalog
        .rotate_credentials(&user, fixtures::credential())
        .unwrap();
    for request in [own, UserInfoRequest::names([]).unwrap()] {
        assert_eq!(
            catalog
                .user_info(&principal, &request, ResultLimits::default())
                .unwrap_err()
                .kind(),
            EngineErrorKind::PermissionDenied
        );
    }
}

#[test]
fn realm_permissions_bounds_and_canonical_selection_precede_metadata_publication() {
    let (mut catalog, user, role) = fixtures::setup();
    let bob = SecurityName::new("app", "bob").unwrap();
    catalog
        .create_user(bob.clone(), fixtures::credential(), [])
        .unwrap();
    let principal = fixtures::login(&catalog, &user);
    catalog
        .replace_role(
            &role,
            Policy::new([Privilege::new(
                Action::ViewUsers,
                Scope::exact(Resource::security_realm("app").unwrap()),
            )
            .unwrap()])
            .unwrap(),
        )
        .unwrap();
    let before = catalog.to_record().unwrap();
    let all = UserInfoRequest::realm("app").unwrap();
    let result = catalog
        .user_info(&principal, &all, ResultLimits::default())
        .unwrap();
    assert_eq!(
        result.iter().map(UserInfo::name).collect::<Vec<_>>(),
        [&user, &bob]
    );
    let selected = UserInfoRequest::names([
        bob.clone(),
        user.clone(),
        bob.clone(),
        SecurityName::new("app", "missing").unwrap(),
    ])
    .unwrap();
    assert_eq!(
        catalog
            .user_info(&principal, &selected, ResultLimits::default())
            .unwrap(),
        result
    );
    for limits in [
        ResultLimits::new(1, 1_000_000).unwrap(),
        ResultLimits::new(10, 400).unwrap(),
    ] {
        assert_eq!(
            catalog
                .user_info(&principal, &all, limits)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
    }
    assert_eq!(before.as_bytes(), catalog.to_record().unwrap().as_bytes());
    assert!(UserInfoRequest::names(std::iter::repeat(user.clone())).is_err());
    assert!(UserInfoRequest::realm("").is_err());
    assert!(UserInfoRequest::realm(&"a".repeat(64)).is_err());
    let mut other = SecurityCatalog::from_record(before.as_bytes()).unwrap();
    assert!(
        other
            .user_info(&principal, &all, ResultLimits::default())
            .is_err()
    );
    catalog.replace_role(&role, Policy::default()).unwrap();
    assert!(
        catalog
            .user_info(&principal, &all, ResultLimits::default())
            .is_err()
    );
    other.drop_user(&user).unwrap();
    other.create_user(user, fixtures::credential(), []).unwrap();
    assert!(
        other
            .user_info(&principal, &all, ResultLimits::default())
            .is_err()
    );
}
