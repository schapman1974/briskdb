use super::*;
use crate::core::authorization::Resource;

fn target() -> SecurityName {
    SecurityName::new("team", "custom").unwrap()
}
fn data(action: Action) -> Policy {
    Policy::new([Privilege::new(
        action,
        Scope::exact(Resource::object(DataDomain::Document, "team", "posts").unwrap()),
    )
    .unwrap()])
    .unwrap()
}
async fn enable(engine: &Engine, action: Action) {
    engine
        .update_security_catalog(move |catalog| {
            catalog.replace_role(
                &role(),
                Policy::new([Privilege::new(
                    action,
                    Scope::exact(Resource::security_realm("team")?),
                )?])?,
            )
        })
        .await
        .unwrap();
}
async fn authorized(engine: &Engine, actor: &Session, action: Action) -> EngineResult<()> {
    let principal = actor.principal.clone().unwrap();
    engine
        .security_call(move |authority| {
            authority.authorize_all(
                &principal,
                [(
                    action,
                    &Resource::object(DataDomain::Document, "team", "posts")?,
                )],
            )
        })
        .await
}

#[tokio::test]
async fn role_privilege_revocation_requires_current_revoke_authority_and_refreshes_members() {
    let (root, engine) = secure(&[]).await;
    engine
        .update_security_catalog(|catalog| {
            catalog.create_role(
                target(),
                Policy::combine([&data(Action::ReadData), &data(Action::InsertData)])?,
            )?;
            catalog.set_user_roles(&user(), [role(), target()])
        })
        .await
        .unwrap();
    let actor = login(&engine).await;
    enable(&engine, Action::GrantRole).await;
    assert_eq!(
        engine
            .revoke_document_role_privileges(
                &actor,
                RequestContext::new(),
                target(),
                data(Action::ReadData)
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    enable(&engine, Action::RevokeRole).await;
    let peer = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    let peer_actor = login(&peer).await;
    authorized(&peer, &peer_actor, Action::ReadData)
        .await
        .unwrap();
    assert!(
        peer.revoke_document_role_privileges(
            &actor,
            RequestContext::new(),
            target(),
            Policy::default()
        )
        .await
        .is_err()
    );
    assert_eq!(
        engine
            .revoke_document_role_privileges(
                &engine.session(),
                RequestContext::new(),
                target(),
                Policy::default()
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    assert_eq!(
        engine
            .revoke_document_role_privileges(
                &actor,
                RequestContext::new(),
                role(),
                Policy::default()
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    for removals in [
        data(Action::ReadData),
        data(Action::ReadData),
        Policy::default(),
    ] {
        engine
            .revoke_document_role_privileges(&actor, RequestContext::new(), target(), removals)
            .await
            .unwrap();
    }
    assert_eq!(
        authorized(&peer, &peer_actor, Action::ReadData)
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    authorized(&peer, &peer_actor, Action::InsertData)
        .await
        .unwrap();
    let memberships = engine
        .user_info(
            &actor,
            RequestContext::new(),
            crate::core::security_catalog::UserInfoRequest::names([user()]).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(memberships[0].roles(), &[role(), target()]);
    let missing = SecurityName::new("team", "missing").unwrap();
    assert_eq!(
        engine
            .revoke_document_role_privileges(
                &actor,
                RequestContext::new(),
                missing.clone(),
                Policy::default()
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
    peer.update_security_catalog(|catalog| catalog.replace_role(&role(), Policy::default()))
        .await
        .unwrap();
    for name in [target(), missing] {
        assert_eq!(
            engine
                .revoke_document_role_privileges(
                    &actor,
                    RequestContext::new(),
                    name,
                    Policy::default()
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::PermissionDenied
        );
    }
    peer.shutdown().await.unwrap();
    engine.shutdown().await.unwrap();
    drop(peer);
    drop(engine);
    let reopened = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    let actor = login(&reopened).await;
    assert!(
        authorized(&reopened, &actor, Action::ReadData)
            .await
            .is_err()
    );
    authorized(&reopened, &actor, Action::InsertData)
        .await
        .unwrap();
    assert_eq!(
        reopened
            .revoke_document_role_privileges(
                &actor,
                RequestContext::new(),
                target(),
                data(Action::InsertData)
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn role_privilege_revocation_checks_inputs_wait_budget_and_session_lifecycle() {
    let (root, engine) = secure(&[]).await;
    engine
        .update_security_catalog(|catalog| catalog.create_role(target(), data(Action::ReadData)))
        .await
        .unwrap();
    enable(&engine, Action::RevokeRole).await;
    let actor = login(&engine).await;
    let before = fs::read(root.path().join("security.sqlite")).unwrap();
    for (action, scope) in [
        (Action::ReadData, Scope::all_databases(DataDomain::Document)),
        (
            Action::ReadData,
            Scope::non_system_document_collections("other").unwrap(),
        ),
        (
            Action::ReadData,
            Scope::exact(Resource::object(DataDomain::Relational, "team", "posts").unwrap()),
        ),
        (
            Action::RevokeRole,
            Scope::exact(Resource::security_realm("team").unwrap()),
        ),
        (
            Action::ConnectDatabase,
            Scope::exact(Resource::database(DataDomain::Document, "team").unwrap()),
        ),
        (
            Action::CreateDatabase,
            Scope::exact(Resource::database(DataDomain::Document, "team").unwrap()),
        ),
    ] {
        assert_eq!(
            engine
                .revoke_document_role_privileges(
                    &actor,
                    RequestContext::new(),
                    target(),
                    Policy::new([Privilege::new(action, scope).unwrap()]).unwrap()
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
    }
    let guard = actor.inner.lock().await;
    assert_eq!(
        engine
            .revoke_document_role_privileges(
                &actor,
                RequestContext::new()
                    .with_timeout(Duration::from_millis(20))
                    .unwrap(),
                target(),
                data(Action::ReadData)
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::DeadlineExceeded
    );
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        engine
            .revoke_document_role_privileges(
                &actor,
                RequestContext::new().with_cancellation_token(token),
                target(),
                data(Action::ReadData)
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::Cancelled
    );
    drop(guard);
    assert_eq!(
        before,
        fs::read(root.path().join("security.sqlite")).unwrap()
    );
    engine
        .update_security_catalog(|catalog| {
            catalog.rotate_credentials(&user(), fixtures::credential())
        })
        .await
        .unwrap();
    assert_eq!(
        engine
            .revoke_document_role_privileges(
                &actor,
                RequestContext::new(),
                target(),
                data(Action::ReadData)
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    let actor = login(&engine).await;
    actor.close().await.unwrap();
    assert!(
        engine
            .revoke_document_role_privileges(
                &actor,
                RequestContext::new(),
                target(),
                data(Action::ReadData)
            )
            .await
            .is_err()
    );
    let actor = login(&engine).await;
    engine
        .revoke_document_role_privileges(
            &actor,
            RequestContext::new(),
            target(),
            data(Action::ReadData),
        )
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    assert!(
        engine
            .revoke_document_role_privileges(
                &actor,
                RequestContext::new(),
                target(),
                Policy::default()
            )
            .await
            .is_err()
    );
}
