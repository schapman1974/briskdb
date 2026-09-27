use super::*;
use crate::core::{ResultLimits, authorization::Resource, security_catalog::UserInfoRequest};

#[tokio::test]
async fn user_info_is_read_only_current_permission_checked_and_session_owned() {
    let (root, engine) = secure(&[]).await;
    let actor = login(&engine).await;
    let own = UserInfoRequest::names([user()]).unwrap();
    let before = fs::read(root.path().join("security.sqlite")).unwrap();
    let result = engine
        .user_info(&actor, RequestContext::new(), own.clone())
        .await
        .unwrap();
    assert_eq!(result[0].name(), &user());
    assert_eq!(result[0].roles(), &[role()]);
    assert_eq!(
        before,
        fs::read(root.path().join("security.sqlite")).unwrap()
    );
    let selected = UserInfoRequest::realm("team").unwrap();
    assert_eq!(
        engine
            .user_info(&actor, RequestContext::new(), selected.clone())
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
                    Action::ViewUsers,
                    Scope::exact(Resource::security_realm("team")?),
                )?])?,
            )?;
            catalog.create_user(
                SecurityName::new("team", "bob")?,
                fixtures::credential(),
                [],
            )
        })
        .await
        .unwrap();
    let rows = engine
        .user_info(&actor, RequestContext::new(), selected.clone())
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name(), &SecurityName::new("team", "bob").unwrap());
    let peer = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    assert!(
        peer.user_info(&actor, RequestContext::new(), own.clone())
            .await
            .is_err()
    );
    let peer_actor = login(&peer).await;
    assert_eq!(
        peer.user_info(&peer_actor, RequestContext::new(), selected.clone())
            .await
            .unwrap(),
        rows
    );
    peer.update_security_catalog(|catalog| catalog.replace_role(&role(), Policy::default()))
        .await
        .unwrap();
    assert_eq!(
        engine
            .user_info(&actor, RequestContext::new(), selected)
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    // Current self-inspection remains valid without a role grant, not after rotation.
    assert!(
        engine
            .user_info(&actor, RequestContext::new(), own.clone())
            .await
            .is_ok()
    );
    peer.update_security_catalog(|catalog| {
        catalog.rotate_credentials(&user(), fixtures::credential())
    })
    .await
    .unwrap();
    assert_eq!(
        engine
            .user_info(&actor, RequestContext::new(), own.clone())
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    assert_eq!(
        engine
            .user_info(&engine.session(), RequestContext::new(), own)
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
}

#[tokio::test]
async fn user_info_honors_metadata_limits_and_queued_cancellation_without_catalog_edits() {
    let (root, engine) = secure(&[]).await;
    let actor = login(&engine).await;
    let request = UserInfoRequest::names([user()]).unwrap();
    let before = fs::read(root.path().join("security.sqlite")).unwrap();
    let result = engine
        .user_info(
            &actor,
            RequestContext::new().with_result_limits(ResultLimits::new(1, 64).unwrap()),
            request.clone(),
        )
        .await;
    assert_eq!(result.unwrap_err().kind(), EngineErrorKind::LimitExceeded);
    let guard = actor.inner.lock().await;
    let context = RequestContext::new()
        .with_timeout(std::time::Duration::from_millis(20))
        .unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        engine.user_info(&actor, context, request.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        result.unwrap_err().kind(),
        EngineErrorKind::DeadlineExceeded
    );
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        engine
            .user_info(
                &actor,
                RequestContext::new().with_cancellation_token(token),
                request
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
