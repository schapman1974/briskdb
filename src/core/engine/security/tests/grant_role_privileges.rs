use super::*;
use crate::core::authorization::Resource;

fn target() -> SecurityName {
    SecurityName::new("team", "custom").unwrap()
}

fn additions() -> Policy {
    Policy::new([Privilege::new(
        Action::ReadData,
        Scope::exact(Resource::object(DataDomain::Document, "team", "posts").unwrap()),
    )
    .unwrap()])
    .unwrap()
}

async fn enable(engine: &Engine) {
    engine
        .update_security_catalog(|catalog| {
            catalog.replace_role(
                &role(),
                Policy::new([Privilege::new(
                    Action::GrantRole,
                    Scope::exact(Resource::security_realm("team")?),
                )?])?,
            )
        })
        .await
        .unwrap();
}

async fn readable(engine: &Engine, actor: &Session) -> EngineResult<()> {
    let principal = actor.principal.clone().unwrap();
    engine
        .security_call(move |authority| {
            authority.authorize_all(
                &principal,
                [(
                    Action::ReadData,
                    &Resource::object(DataDomain::Document, "team", "posts")?,
                )],
            )
        })
        .await
}

#[tokio::test]
async fn role_privilege_grants_require_current_authority_and_refresh_members_across_engines() {
    let (root, engine) = secure(&[]).await;
    engine
        .update_security_catalog(|catalog| {
            catalog.create_role(target(), Policy::default())?;
            catalog.set_user_roles(&user(), [role(), target()])
        })
        .await
        .unwrap();
    let actor = login(&engine).await;
    assert_eq!(
        engine
            .grant_document_role_privileges(&actor, RequestContext::new(), target(), additions())
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    enable(&engine).await;
    let peer = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    let peer_actor = login(&peer).await;
    assert!(readable(&peer, &peer_actor).await.is_err());
    assert!(
        peer.grant_document_role_privileges(&actor, RequestContext::new(), target(), additions())
            .await
            .is_err()
    );
    assert_eq!(
        engine
            .grant_document_role_privileges(
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
    assert_eq!(
        engine
            .grant_document_role_privileges(
                &engine.session(),
                RequestContext::new(),
                target(),
                additions()
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    engine
        .grant_document_role_privileges(&actor, RequestContext::new(), target(), additions())
        .await
        .unwrap();
    engine
        .grant_document_role_privileges(&actor, RequestContext::new(), target(), additions())
        .await
        .unwrap();
    engine
        .grant_document_role_privileges(&actor, RequestContext::new(), target(), Policy::default())
        .await
        .unwrap();
    readable(&peer, &peer_actor).await.unwrap();
    let memberships = engine
        .user_info(
            &actor,
            RequestContext::new(),
            crate::core::security_catalog::UserInfoRequest::names([user()]).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(memberships[0].roles(), &[role(), target()]);
    assert_eq!(
        engine
            .grant_document_role_privileges(
                &actor,
                RequestContext::new(),
                SecurityName::new("team", "missing").unwrap(),
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
    for name in [target(), SecurityName::new("team", "missing").unwrap()] {
        assert_eq!(
            engine
                .grant_document_role_privileges(
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
    readable(&reopened, &actor).await.unwrap();
    assert_eq!(
        reopened
            .grant_document_role_privileges(&actor, RequestContext::new(), target(), additions())
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn role_privilege_grants_reject_broad_additions_cancelled_waits_and_stale_sessions() {
    let (root, engine) = secure(&[]).await;
    engine
        .update_security_catalog(|catalog| catalog.create_role(target(), Policy::default()))
        .await
        .unwrap();
    enable(&engine).await;
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
            Action::GrantRole,
            Scope::exact(Resource::security_realm("team").unwrap()),
        ),
    ] {
        assert_eq!(
            engine
                .grant_document_role_privileges(
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
            .grant_document_role_privileges(
                &actor,
                RequestContext::new()
                    .with_timeout(Duration::from_millis(20))
                    .unwrap(),
                target(),
                additions()
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
            .grant_document_role_privileges(
                &actor,
                RequestContext::new().with_cancellation_token(token),
                target(),
                additions()
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
            .grant_document_role_privileges(&actor, RequestContext::new(), target(), additions())
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    let actor = login(&engine).await;
    actor.close().await.unwrap();
    assert!(
        engine
            .grant_document_role_privileges(&actor, RequestContext::new(), target(), additions())
            .await
            .is_err()
    );
    let actor = login(&engine).await;
    engine
        .grant_document_role_privileges(&actor, RequestContext::new(), target(), additions())
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    assert!(
        engine
            .grant_document_role_privileges(&actor, RequestContext::new(), target(), additions())
            .await
            .is_err()
    );
}
