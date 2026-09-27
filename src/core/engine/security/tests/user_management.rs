use super::*;
use crate::core::{authorization::Resource, user_management::UserManagementCommand as Command};
use ring::{digest, hmac, pbkdf2};
use std::num::NonZeroU32;

fn target() -> SecurityName {
    SecurityName::new("team", "bob").unwrap()
}
fn role_in_app() -> SecurityName {
    SecurityName::new("app", "readers").unwrap()
}
fn administration() -> Policy {
    Policy::new(
        [
            (Action::CreateUser, "team"),
            (Action::DropUser, "team"),
            (Action::RotateCredentials, "team"),
            (Action::GrantRole, "app"),
            (Action::RevokeRole, "app"),
        ]
        .into_iter()
        .map(|(action, realm)| {
            Privilege::new(
                action,
                Scope::exact(Resource::security_realm(realm).unwrap()),
            )
            .unwrap()
        }),
    )
    .unwrap()
}

async fn login_named(engine: &Engine, name: SecurityName, password: &str) -> Session {
    let attempt = engine.begin_authentication(name).await.unwrap();
    let (expected_signature, message, proof) = exchange(&attempt, password);
    let (session, signature) = engine
        .complete_authentication(attempt, message.into_bytes(), proof)
        .await
        .unwrap();
    assert_eq!(signature.as_slice(), expected_signature);
    session
}

// postgres-protocol's client fixture deliberately caps iterations below our
// production password cost. Keep that independent client for low-cost fixtures;
// this engine-only helper supports production-created users. The wire gate also
// verifies their proofs and server signatures with real PyMongo.
fn exchange(attempt: &ScramAttempt, password: &str) -> (Vec<u8>, String, Vec<u8>) {
    let mut salted = [0; 32];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        NonZeroU32::new(attempt.iterations()).unwrap(),
        attempt.salt(),
        stringprep::saslprep(password).unwrap().as_bytes(),
        &mut salted,
    );
    let mac = |key: &[u8], value: &[u8]| {
        hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), value)
            .as_ref()
            .to_vec()
    };
    let message = format!(
        "n=test,r=client,r=clientserver,s={},i={},c=biws,r=clientserver",
        STANDARD.encode(attempt.salt()),
        attempt.iterations()
    );
    let key = mac(&salted, b"Client Key");
    let stored = digest::digest(&digest::SHA256, &key);
    let signature = mac(stored.as_ref(), message.as_bytes());
    let proof = key
        .iter()
        .zip(signature)
        .map(|(key, sig)| key ^ sig)
        .collect();
    let server = mac(&mac(&salted, b"Server Key"), message.as_bytes());
    (server, message, proof)
}

async fn enable(engine: &Engine) {
    engine
        .update_security_catalog(|catalog| {
            catalog.replace_role(&role(), administration())?;
            catalog.create_role(role_in_app(), Policy::default())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn user_management_requires_current_permissions_before_hashing_or_existence_checks() {
    let (_root, engine) = secure(&[]).await;
    let actor = login(&engine).await;
    // The prohibited password would fail normalization if hashing ran first.
    let command = Command::create(target(), "\u{7}", []).unwrap();
    assert_eq!(
        engine
            .execute_user_management(&actor, RequestContext::new(), command)
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    enable(&engine).await;
    let command = Command::create(target(), "\u{7}", []).unwrap();
    assert_eq!(
        engine
            .execute_user_management(&actor, RequestContext::new(), command)
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::InvalidArgument
    );
    assert!(engine.begin_authentication(target()).await.is_err());
    // Creation and role-grant scopes are independent, including missing roles.
    let foreign = SecurityName::new("other", "privileged").unwrap();
    let command = Command::create(target(), fixtures::PASSWORD, [foreign]).unwrap();
    assert_eq!(
        engine
            .execute_user_management(&actor, RequestContext::new(), command)
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    assert!(engine.begin_authentication(target()).await.is_err());
    let command = Command::create(target(), fixtures::PASSWORD, [role_in_app()]).unwrap();
    engine
        .execute_user_management(&actor, RequestContext::new(), command)
        .await
        .unwrap();
    let bob = login_named(&engine, target(), fixtures::PASSWORD).await;
    assert_eq!(
        engine
            .execute_user_management(&bob, RequestContext::new(), Command::drop_user(target()))
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    // There is no implicit self-service password grant.
    assert_eq!(
        engine
            .execute_user_management(
                &bob,
                RequestContext::new(),
                Command::change_password(target(), "new").unwrap()
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    engine
        .update_security_catalog(|catalog| catalog.replace_role(&role(), Policy::default()))
        .await
        .unwrap();
    assert_eq!(
        engine
            .execute_user_management(&actor, RequestContext::new(), Command::drop_user(target()))
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
}

#[tokio::test]
async fn authorized_password_membership_and_drop_commands_survive_reopen_and_revoke_sessions() {
    let (root, engine) = secure(&[]).await;
    enable(&engine).await;
    let actor = login(&engine).await;
    engine
        .execute_user_management(
            &actor,
            RequestContext::new(),
            Command::create(target(), fixtures::PASSWORD, []).unwrap(),
        )
        .await
        .unwrap();
    engine
        .execute_user_management(
            &actor,
            RequestContext::new(),
            Command::grant_roles(target(), [role_in_app()]).unwrap(),
        )
        .await
        .unwrap();
    engine
        .execute_user_management(
            &actor,
            RequestContext::new(),
            Command::revoke_roles(target(), [role_in_app()]).unwrap(),
        )
        .await
        .unwrap();
    let old_attempt = engine.begin_authentication(target()).await.unwrap();
    let (_, message, proof) = exchange(&old_attempt, fixtures::PASSWORD);
    engine
        .execute_user_management(
            &actor,
            RequestContext::new(),
            Command::change_password(target(), "changed password").unwrap(),
        )
        .await
        .unwrap();
    assert!(
        engine
            .complete_authentication(old_attempt, message.into_bytes(), proof)
            .await
            .is_err()
    );
    drop(login_named(&engine, target(), "changed password").await);
    let peer = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    drop(login_named(&peer, target(), "changed password").await);
    engine
        .execute_user_management(&actor, RequestContext::new(), Command::drop_user(target()))
        .await
        .unwrap();
    assert!(peer.begin_authentication(target()).await.is_err());
    assert!(
        peer.execute_user_management(&actor, RequestContext::new(), Command::drop_user(target()))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn queued_session_admission_honors_cancellation_and_deadlines_without_edits() {
    let (_root, engine) = secure(&[]).await;
    enable(&engine).await;
    let actor = login(&engine).await;
    let guard = actor.inner.lock().await;
    let deadline = RequestContext::new()
        .with_timeout(std::time::Duration::from_millis(20))
        .unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        engine.execute_user_management(
            &actor,
            deadline,
            Command::create(target(), fixtures::PASSWORD, []).unwrap(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        result.unwrap_err().kind(),
        EngineErrorKind::DeadlineExceeded
    );
    let token = CancellationToken::new();
    token.cancel();
    let result = engine
        .execute_user_management(
            &actor,
            RequestContext::new().with_cancellation_token(token),
            Command::create(target(), fixtures::PASSWORD, []).unwrap(),
        )
        .await;
    assert_eq!(result.unwrap_err().kind(), EngineErrorKind::Cancelled);
    drop(guard);
    assert!(engine.begin_authentication(target()).await.is_err());
}
