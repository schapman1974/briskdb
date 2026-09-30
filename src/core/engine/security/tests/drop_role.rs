use super::*;
use crate::core::{
    authorization::Resource,
    security_catalog::{RoleInfoRequest, UserInfoRequest},
};

fn target() -> SecurityName {
    SecurityName::new("team", "reader").unwrap()
}

async fn enable(engine: &Engine) {
    engine
        .update_security_catalog(|catalog| {
            catalog.replace_role(
                &role(),
                Policy::new([Privilege::new(
                    Action::DropRole,
                    Scope::exact(Resource::security_realm("team")?),
                )?])?,
            )?;
            catalog.create_role(target(), Policy::default())?;
            catalog.set_user_roles(&user(), [role(), target()])
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn drop_role_checks_current_authority_before_existence_and_never_revives_memberships() {
    let (root, engine) = secure(&[]).await;
    let actor = login(&engine).await;
    assert_eq!(
        engine
            .drop_role(&actor, RequestContext::new(), target())
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    enable(&engine).await;
    let peer = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    assert!(
        peer.drop_role(&actor, RequestContext::new(), target())
            .await
            .is_err()
    );
    let peer_actor = login(&peer).await;
    let own_roles = RoleInfoRequest::names([target()]).unwrap();
    assert_eq!(
        peer.role_info(&peer_actor, RequestContext::new(), own_roles.clone())
            .await
            .unwrap()
            .len(),
        1
    );
    // A grant on one realm cannot delete an identically named role elsewhere.
    assert_eq!(
        engine
            .drop_role(&actor, RequestContext::new(), role())
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    assert_eq!(
        engine
            .drop_role(&engine.session(), RequestContext::new(), target())
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    engine
        .drop_role(&actor, RequestContext::new(), target())
        .await
        .unwrap();
    assert_eq!(
        engine
            .drop_role(&actor, RequestContext::new(), target())
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
    assert_eq!(
        peer.role_info(&peer_actor, RequestContext::new(), own_roles.clone())
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    peer.update_security_catalog(|catalog| catalog.create_role(target(), Policy::default()))
        .await
        .unwrap();
    assert_eq!(
        engine
            .role_info(&actor, RequestContext::new(), own_roles)
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    let users = engine
        .user_info(
            &actor,
            RequestContext::new(),
            UserInfoRequest::names([user()]).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(users[0].roles(), &[role()]);
    // Revocation from a different engine must precede the next deletion decision.
    peer.update_security_catalog(|catalog| catalog.replace_role(&role(), Policy::default()))
        .await
        .unwrap();
    assert_eq!(
        engine
            .drop_role(&actor, RequestContext::new(), target())
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
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
    let users = reopened
        .user_info(
            &actor,
            RequestContext::new(),
            UserInfoRequest::names([user()]).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(users[0].roles(), &[role()]);
    assert_eq!(
        reopened
            .drop_role(&actor, RequestContext::new(), target())
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn drop_role_honors_queued_controls_closed_sessions_and_rotated_credentials() {
    let (root, engine) = secure(&[]).await;
    enable(&engine).await;
    let actor = login(&engine).await;
    let before = fs::read(root.path().join("security.sqlite")).unwrap();
    let guard = actor.inner.lock().await;
    let context = RequestContext::new()
        .with_timeout(Duration::from_millis(20))
        .unwrap();
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(1),
            engine.drop_role(&actor, context, target())
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
            .drop_role(
                &actor,
                RequestContext::new().with_cancellation_token(token),
                target()
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
            .drop_role(&actor, RequestContext::new(), target())
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    let actor = login(&engine).await;
    actor.close().await.unwrap();
    assert!(
        engine
            .drop_role(&actor, RequestContext::new(), target())
            .await
            .is_err()
    );
    let actor = login(&engine).await;
    engine
        .drop_role(&actor, RequestContext::new(), target())
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    assert!(
        engine
            .drop_role(&actor, RequestContext::new(), role())
            .await
            .is_err()
    );
}
