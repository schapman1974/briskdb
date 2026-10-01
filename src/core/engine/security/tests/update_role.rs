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

#[tokio::test]
async fn role_replacement_checks_scope_wait_budget_and_session_lifecycle() {
    let (root, engine) = secure(&[]).await;
    engine
        .update_security_catalog(|catalog| {
            catalog.create_role(target(), data(Action::ReadData))?;
            catalog.replace_role(
                &role(),
                Policy::new([
                    Privilege::new(Action::RevokeRole, Scope::all_security_realms())?,
                    Privilege::new(
                        Action::GrantRole,
                        Scope::exact(Resource::security_realm("team")?),
                    )?,
                ])?,
            )
        })
        .await
        .unwrap();
    let actor = login(&engine).await;
    let before = fs::read(root.path().join("security.sqlite")).unwrap();
    for scope in [
        Scope::all_databases(DataDomain::Document),
        Scope::non_system_document_collections("other").unwrap(),
        Scope::exact(Resource::object(DataDomain::Relational, "team", "posts").unwrap()),
    ] {
        let replacement = Policy::new([Privilege::new(Action::ReadData, scope).unwrap()]).unwrap();
        assert_eq!(
            engine
                .update_document_role(&actor, RequestContext::new(), target(), Some(replacement))
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
    }
    assert_eq!(
        engine
            .update_document_role(&engine.session(), RequestContext::new(), target(), None)
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    let peer = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    assert!(
        peer.update_document_role(&actor, RequestContext::new(), target(), None)
            .await
            .is_err()
    );
    peer.shutdown().await.unwrap();
    assert_eq!(
        engine
            .update_document_role(
                &actor,
                RequestContext::new(),
                SecurityName::new("other", "missing").unwrap(),
                Some(Policy::default())
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    let guard = actor.inner.lock().await;
    assert_eq!(
        engine
            .update_document_role(
                &actor,
                RequestContext::new()
                    .with_timeout(Duration::from_millis(20))
                    .unwrap(),
                target(),
                Some(Policy::default())
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
            .update_document_role(
                &actor,
                RequestContext::new().with_cancellation_token(token),
                target(),
                Some(Policy::default())
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
            .update_document_role(&actor, RequestContext::new(), target(), None)
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    let actor = login(&engine).await;
    actor.close().await.unwrap();
    assert!(
        engine
            .update_document_role(&actor, RequestContext::new(), target(), None)
            .await
            .is_err()
    );
    let actor = login(&engine).await;
    engine
        .update_document_role(
            &actor,
            RequestContext::new(),
            target(),
            Some(Policy::default()),
        )
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    assert!(
        engine
            .update_document_role(&actor, RequestContext::new(), target(), None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn role_replacement_requires_all_realm_revocation_and_preserves_membership() {
    let (root, engine) = secure(&[]).await;
    engine
        .update_security_catalog(|catalog| {
            catalog.create_role(target(), data(Action::ReadData))?;
            catalog.set_user_roles(&user(), [role(), target()])
        })
        .await
        .unwrap();
    let actor = login(&engine).await;
    // Exact authority over all currently relevant realms is still not an
    // explicit grant covering every realm, including future ones.
    engine
        .update_security_catalog(|catalog| {
            catalog.replace_role(
                &role(),
                Policy::new([
                    Privilege::new(
                        Action::RevokeRole,
                        Scope::exact(Resource::security_realm("team")?),
                    )?,
                    Privilege::new(
                        Action::RevokeRole,
                        Scope::exact(Resource::security_realm("admin")?),
                    )?,
                    Privilege::new(
                        Action::GrantRole,
                        Scope::exact(Resource::security_realm("team")?),
                    )?,
                ])?,
            )
        })
        .await
        .unwrap();
    for name in [target(), SecurityName::new("team", "missing").unwrap()] {
        assert_eq!(
            engine
                .update_document_role(
                    &actor,
                    RequestContext::new(),
                    name,
                    Some(data(Action::InsertData))
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::PermissionDenied
        );
    }
    engine
        .update_security_catalog(|catalog| {
            catalog.replace_role(
                &role(),
                Policy::new([Privilege::new(
                    Action::RevokeRole,
                    Scope::all_security_realms(),
                )?])?,
            )
        })
        .await
        .unwrap();
    // Clearing empty inheritance alone preserves privileges without grant authority.
    engine
        .update_document_role(&actor, RequestContext::new(), target(), None)
        .await
        .unwrap();
    assert_eq!(
        engine
            .update_document_role(
                &actor,
                RequestContext::new(),
                target(),
                Some(Policy::default())
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    engine
        .update_security_catalog(|catalog| {
            catalog.replace_role(
                &role(),
                Policy::new([
                    Privilege::new(Action::RevokeRole, Scope::all_security_realms())?,
                    Privilege::new(
                        Action::GrantRole,
                        Scope::exact(Resource::security_realm("team")?),
                    )?,
                ])?,
            )
        })
        .await
        .unwrap();
    engine
        .update_document_role(
            &actor,
            RequestContext::new(),
            target(),
            Some(data(Action::InsertData)),
        )
        .await
        .unwrap();
    let principal = actor.principal.clone().unwrap();
    engine
        .security_call(move |authority| {
            let resource = Resource::object(DataDomain::Document, "team", "posts")?;
            assert_eq!(
                authority
                    .authorize_all(&principal, [(Action::ReadData, &resource)])
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::PermissionDenied
            );
            authority.authorize_all(&principal, [(Action::InsertData, &resource)])
        })
        .await
        .unwrap();
    // A peer and a fresh login see the replacement through the same membership.
    let peer = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    let peer_actor = login(&peer).await;
    let principal = peer_actor.principal.clone().unwrap();
    peer.security_call(move |authority| {
        authority.authorize_all(
            &principal,
            [(
                Action::InsertData,
                &Resource::object(DataDomain::Document, "team", "posts")?,
            )],
        )
    })
    .await
    .unwrap();
    engine
        .update_security_catalog(|catalog| catalog.replace_role(&role(), Policy::default()))
        .await
        .unwrap();
    assert_eq!(
        engine
            .update_document_role(&actor, RequestContext::new(), target(), None)
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    peer.shutdown().await.unwrap();
    engine.shutdown().await.unwrap();
}
