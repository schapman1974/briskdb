use super::*;
use crate::core::{
    authorization::{Privilege, Scope},
    security_catalog::tests as fixtures,
};

#[test]
fn privilege_projection_obeys_wire_input_bounds_and_rejects_mixed_exports() {
    use crate::core::authorization::DataDomain;
    let (mut catalog, user, role) = fixtures::setup();
    let principal = fixtures::login(&catalog, &user);
    let request = RoleInfoRequest::names([role.clone()])
        .unwrap()
        .with_document_privileges(true);
    let db = Resource::database(DataDomain::Document, role.realm()).unwrap();
    for count in [127, 128] {
        let mut grants = vec![
            Privilege::new(Action::ConnectDatabase, Scope::exact(db.clone())).unwrap(),
            Privilege::new(Action::CreateDatabase, Scope::exact(db.clone())).unwrap(),
        ];
        grants.extend((0..count).map(|n| {
            Privilege::new(
                Action::CreateObject,
                Scope::exact(
                    Resource::object(DataDomain::Document, role.realm(), &format!("c{n}")).unwrap(),
                ),
            )
            .unwrap()
        }));
        catalog
            .replace_role(&role, Policy::new(grants).unwrap())
            .unwrap();
        let rows = catalog.role_info(&principal, &request, ResultLimits::default());
        if count == 127 {
            assert_eq!(rows.unwrap()[0].document_privileges().unwrap().len(), count);
        } else {
            assert_eq!(rows.unwrap_err().kind(), EngineErrorKind::Unsupported);
        }
    }
    let valid = SecurityName::new(role.realm(), "aaa_valid").unwrap();
    catalog
        .create_role(valid.clone(), Policy::default())
        .unwrap();
    catalog
        .set_user_roles(&user, [role.clone(), valid.clone()])
        .unwrap();
    let mixed = RoleInfoRequest::names([valid, role.clone()])
        .unwrap()
        .with_document_privileges(true);
    assert_eq!(
        catalog
            .role_info(&principal, &mixed, ResultLimits::default())
            .unwrap_err()
            .kind(),
        EngineErrorKind::Unsupported
    );
    // Authorization precedes even unsupported-policy inspection.
    let unauthorized =
        RoleInfoRequest::names([role.clone(), SecurityName::new("other", "missing").unwrap()])
            .unwrap()
            .with_document_privileges(true);
    assert_eq!(
        catalog
            .role_info(&principal, &unauthorized, ResultLimits::default())
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
}

#[test]
fn document_privilege_inspection_is_opt_in_lossless_bounded_and_current() {
    use crate::core::authorization::DataDomain;
    let (mut catalog, user, role) = fixtures::setup();
    let db = Resource::database(DataDomain::Document, role.realm()).unwrap();
    let read = Privilege::new(
        Action::ReadData,
        Scope::exact(
            Resource::object(DataDomain::Document, role.realm(), "private_posts").unwrap(),
        ),
    )
    .unwrap();
    let admission = Privilege::new(Action::ConnectDatabase, Scope::exact(db.clone())).unwrap();
    let valid = Policy::new([read.clone(), admission.clone()]).unwrap();
    catalog.replace_role(&role, valid.clone()).unwrap();
    let principal = fixtures::login(&catalog, &user);
    let names = RoleInfoRequest::names([role.clone()]).unwrap();
    let expanded = names.clone().with_document_privileges(true);
    let before = catalog.to_record().unwrap();
    assert!(
        catalog
            .role_info(&principal, &names, ResultLimits::default())
            .unwrap()[0]
            .document_privileges()
            .is_none()
    );
    let rows = catalog
        .role_info(&principal, &expanded, ResultLimits::default())
        .unwrap();
    let grants = rows[0].document_privileges().unwrap();
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].database(), role.realm());
    assert_eq!(grants[0].collection(), "private_posts");
    assert_eq!(grants[0].action(), "find");
    assert!(!format!("{grants:?}").contains("private_posts"));
    assert_eq!(
        catalog
            .role_info(&principal, &expanded, ResultLimits::new(10, 500).unwrap())
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    assert_eq!(before.as_bytes(), catalog.to_record().unwrap().as_bytes());
    // Neither missing nor extra admission can be silently invented/omitted.
    for invalid in [
        Policy::new([read]).unwrap(),
        Policy::new([admission.clone()]).unwrap(),
        Policy::combine([
            &valid,
            &Policy::new([Privilege::new(Action::CreateDatabase, Scope::exact(db)).unwrap()])
                .unwrap(),
        ])
        .unwrap(),
        Policy::new([
            admission,
            Privilege::new(
                Action::ReadData,
                Scope::database(DataDomain::Document, role.realm()).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap(),
    ] {
        catalog.replace_role(&role, invalid).unwrap();
        assert_eq!(
            catalog
                .role_info(&principal, &expanded, ResultLimits::default())
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
        assert!(
            catalog
                .role_info(&principal, &names, ResultLimits::default())
                .is_ok()
        );
    }
    catalog.replace_role(&role, Policy::default()).unwrap();
    assert_eq!(
        catalog
            .role_info(&principal, &expanded, ResultLimits::default())
            .unwrap()[0]
            .document_privileges(),
        Some([].as_slice())
    );
    catalog.set_user_roles(&user, []).unwrap();
    assert_eq!(
        catalog
            .role_info(&principal, &expanded, ResultLimits::default())
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
}

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
