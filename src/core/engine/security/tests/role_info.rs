use super::*;
use crate::core::{ResultLimits, authorization::Resource, security_catalog::RoleInfoRequest};

#[tokio::test]
async fn role_info_obeys_live_membership_realm_revocation_ownership_and_reopen() {
    let (root, engine) = secure(&[]).await;
    let actor = login(&engine).await;
    let own = RoleInfoRequest::names([role()]).unwrap();
    let before = fs::read(root.path().join("security.sqlite")).unwrap();
    let rows = engine
        .role_info(&actor, RequestContext::new(), own.clone())
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name(), &role());
    assert_eq!(
        before,
        fs::read(root.path().join("security.sqlite")).unwrap()
    );
    let all = RoleInfoRequest::realm("team").unwrap();
    assert_eq!(
        engine
            .role_info(&actor, RequestContext::new(), all.clone())
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    engine
        .update_security_catalog(|catalog| {
            catalog.replace_role(
                &role(),
                Policy::new([Privilege::new(
                    Action::ViewRoles,
                    Scope::exact(Resource::security_realm("team")?),
                )?])?,
            )?;
            catalog.create_role(SecurityName::new("team", "member")?, Policy::default())
        })
        .await
        .unwrap();
    let roles = engine
        .role_info(&actor, RequestContext::new(), all.clone())
        .await
        .unwrap();
    assert_eq!(
        roles[0].name(),
        &SecurityName::new("team", "member").unwrap()
    );
    let peer = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    assert!(
        peer.role_info(&actor, RequestContext::new(), own.clone())
            .await
            .is_err()
    );
    let peer_actor = login(&peer).await;
    assert_eq!(
        peer.role_info(&peer_actor, RequestContext::new(), all.clone())
            .await
            .unwrap(),
        roles
    );
    peer.update_security_catalog(|catalog| catalog.set_user_roles(&user(), []))
        .await
        .unwrap();
    for request in [own.clone(), all.clone()] {
        assert_eq!(
            engine
                .role_info(&actor, RequestContext::new(), request)
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::PermissionDenied
        );
    }
    peer.update_security_catalog(|catalog| catalog.set_user_roles(&user(), [role()]))
        .await
        .unwrap();
    assert!(
        engine
            .role_info(&actor, RequestContext::new(), own.clone())
            .await
            .is_ok()
    );
    peer.update_security_catalog(|catalog| {
        catalog.rotate_credentials(&user(), fixtures::credential())
    })
    .await
    .unwrap();
    assert!(
        engine
            .role_info(
                &actor,
                RequestContext::new(),
                RoleInfoRequest::names([]).unwrap()
            )
            .await
            .is_err()
    );
    assert!(
        engine
            .role_info(&engine.session(), RequestContext::new(), own.clone())
            .await
            .is_err()
    );
    peer.shutdown().await.unwrap();
    engine.shutdown().await.unwrap();
    drop(peer_actor);
    drop(actor);
    drop(peer);
    drop(engine);
    let reopened = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    let actor = login(&reopened).await;
    assert_eq!(
        reopened
            .role_info(&actor, RequestContext::new(), all)
            .await
            .unwrap(),
        roles
    );
    assert!(
        reopened
            .role_info(&actor, RequestContext::new(), own.clone())
            .await
            .is_ok()
    );
    actor.close().await.unwrap();
    assert!(
        reopened
            .role_info(&actor, RequestContext::new(), own)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn role_info_applies_result_limits_and_queued_controls_without_catalog_edits() {
    let (root, engine) = secure(&[]).await;
    let actor = login(&engine).await;
    let own = RoleInfoRequest::names([role()]).unwrap();
    let before = fs::read(root.path().join("security.sqlite")).unwrap();
    assert_eq!(
        engine
            .role_info(
                &actor,
                RequestContext::new().with_result_limits(ResultLimits::new(1, 64).unwrap()),
                own.clone()
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    let guard = actor.inner.lock().await;
    let context = RequestContext::new()
        .with_timeout(std::time::Duration::from_millis(20))
        .unwrap();
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            engine.role_info(&actor, context, own.clone())
        )
        .await
        .unwrap()
        .unwrap_err()
        .kind(),
        EngineErrorKind::DeadlineExceeded
    );
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        engine
            .role_info(
                &actor,
                RequestContext::new().with_cancellation_token(token),
                own
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
}
