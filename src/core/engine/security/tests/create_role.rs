use super::*;
use crate::core::{authorization::Resource, security_catalog::RoleInfoRequest};

#[cfg(feature = "mongo")]
#[tokio::test]
async fn collection_probes_check_the_original_command_without_granting_metadata_access() {
    use crate::document::*;
    let (root, engine) = secure(&[
        Action::ConnectDatabase,
        Action::ReadData,
        Action::UpdateData,
    ])
    .await;
    let actor = login(&engine).await;
    let id = DocumentRequestId::new([1; 16]).unwrap();
    let namespace = DocumentNamespace::new("app", "missing").unwrap();
    let source = DocumentCommand::Count(DocumentCountRequest::new(
        namespace.clone(),
        DocumentFilter::empty(),
        DocumentReadOptions::new(),
    ));
    let result = engine
        .execute_document_collection_probe(&actor, id, RequestContext::new(), &source)
        .await
        .unwrap();
    assert!(matches!(
        result.into_parts().2,
        DocumentResult::CollectionExists(false)
    ));
    let metadata =
        DocumentCommand::CollectionExists(DocumentCollectionExistsRequest::new(namespace.clone()));
    assert_eq!(
        engine
            .execute_document(
                &actor,
                DocumentRequest::new(id, RequestContext::new(), metadata.clone())
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    assert_eq!(
        engine
            .execute_document_collection_probe(&actor, id, RequestContext::new(), &metadata)
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::InvalidArgument
    );
    let update = DocumentCommand::Update(DocumentUpdateRequest::new(
        namespace,
        DocumentFilter::empty(),
        DocumentUpdate::new(
            BsonDocument::from_entries([(
                "$set",
                BsonValue::Document(
                    BsonDocument::from_entries([("n", BsonValue::Int32(1))]).unwrap(),
                ),
            )])
            .unwrap(),
        )
        .unwrap(),
        DocumentMutationScope::One,
        DocumentWriteOptions::new().with_upsert(true),
    ));
    // Update permission alone is not the complete upsert requirement.
    assert_eq!(
        engine
            .execute_document_collection_probe(&actor, id, RequestContext::new(), &update)
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    let peer = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    assert!(
        peer.execute_document_collection_probe(&actor, id, RequestContext::new(), &source)
            .await
            .is_err()
    );
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        engine
            .execute_document_collection_probe(
                &actor,
                id,
                RequestContext::new().with_cancellation_token(token),
                &source
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::Cancelled
    );
    peer.update_security_catalog(|catalog| catalog.replace_role(&role(), Policy::default()))
        .await
        .unwrap();
    assert_eq!(
        engine
            .execute_document_collection_probe(&actor, id, RequestContext::new(), &source)
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    actor.close().await.unwrap();
    assert!(
        engine
            .execute_document_collection_probe(&actor, id, RequestContext::new(), &source)
            .await
            .is_err()
    );
    peer.shutdown().await.unwrap();
    engine.shutdown().await.unwrap();
}

fn target(value: &str) -> SecurityName {
    SecurityName::new("team", value).unwrap()
}

fn data_policy() -> Policy {
    Policy::new([Privilege::new(
        Action::ReadData,
        Scope::non_system_document_collections("team").unwrap(),
    )
    .unwrap()])
    .unwrap()
}

async fn enable(engine: &Engine, actions: &[Action]) {
    let policy = Policy::new(actions.iter().map(|action| {
        Privilege::new(
            *action,
            Scope::exact(Resource::security_realm("team").unwrap()),
        )
        .unwrap()
    }))
    .unwrap();
    engine
        .update_security_catalog(move |catalog| catalog.replace_role(&role(), policy))
        .await
        .unwrap();
}

#[tokio::test]
async fn create_document_role_requires_both_current_realm_grants_and_persists_without_assignment() {
    let (root, engine) = secure(&[]).await;
    let actor = login(&engine).await;
    for actions in [vec![], vec![Action::CreateRole], vec![Action::GrantRole]] {
        enable(&engine, &actions).await;
        assert_eq!(
            engine
                .create_document_role(
                    &actor,
                    RequestContext::new(),
                    target("custom"),
                    data_policy()
                )
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::PermissionDenied
        );
    }
    enable(
        &engine,
        &[Action::CreateRole, Action::GrantRole, Action::ViewRoles],
    )
    .await;
    engine
        .create_document_role(
            &actor,
            RequestContext::new(),
            target("custom"),
            data_policy(),
        )
        .await
        .unwrap();
    assert!(
        engine
            .create_document_role(
                &actor,
                RequestContext::new(),
                target("custom"),
                Policy::default()
            )
            .await
            .is_err()
    );
    assert_eq!(
        engine
            .create_document_role(
                &actor,
                RequestContext::new(),
                SecurityName::new("other", "custom").unwrap(),
                Policy::default()
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    assert_eq!(
        engine
            .create_document_role(
                &engine.session(),
                RequestContext::new(),
                target("anonymous"),
                Policy::default()
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    // A created role is not implicitly assigned, including to its creator.
    let users = engine
        .user_info(
            &actor,
            RequestContext::new(),
            crate::core::security_catalog::UserInfoRequest::names([user()]).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(users[0].roles(), &[role()]);
    let peer = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    assert!(
        peer.create_document_role(
            &actor,
            RequestContext::new(),
            target("foreign"),
            Policy::default()
        )
        .await
        .is_err()
    );
    let peer_actor = login(&peer).await;
    assert_eq!(
        peer.role_info(
            &peer_actor,
            RequestContext::new(),
            RoleInfoRequest::names([target("custom")]).unwrap()
        )
        .await
        .unwrap()
        .len(),
        1
    );
    // Revoke grant authority from an independent engine. Neither an existing
    // name nor an absent one may leak past the new permission decision.
    enable(&peer, &[Action::CreateRole, Action::ViewRoles]).await;
    for value in ["custom", "absent"] {
        assert_eq!(
            engine
                .create_document_role(&actor, RequestContext::new(), target(value), data_policy())
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
    assert_eq!(
        reopened
            .role_info(
                &actor,
                RequestContext::new(),
                RoleInfoRequest::names([target("custom")]).unwrap()
            )
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        reopened
            .role_info(
                &actor,
                RequestContext::new(),
                RoleInfoRequest::names([target("absent")]).unwrap()
            )
            .await
            .unwrap()
            .len(),
        0
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn create_document_role_rejects_scope_escalation_and_honors_session_and_request_controls() {
    let (root, engine) = secure(&[]).await;
    enable(&engine, &[Action::CreateRole, Action::GrantRole]).await;
    let actor = login(&engine).await;
    let before = fs::read(root.path().join("security.sqlite")).unwrap();
    for (action, scope) in [
        (Action::ReadData, Scope::all_databases(DataDomain::Document)),
        (
            Action::ReadData,
            Scope::database(DataDomain::Document, "team").unwrap(),
        ),
        (
            Action::ReadData,
            Scope::non_system_document_collections("other").unwrap(),
        ),
        (
            Action::ReadData,
            Scope::exact(Resource::object(DataDomain::Document, "other", "docs").unwrap()),
        ),
        (
            Action::ReadData,
            Scope::exact(Resource::object(DataDomain::Relational, "team", "docs").unwrap()),
        ),
        (
            Action::GrantRole,
            Scope::exact(Resource::security_realm("team").unwrap()),
        ),
        (Action::ManageServer, Scope::exact(Resource::server())),
    ] {
        let policy = Policy::new([Privilege::new(action, scope).unwrap()]).unwrap();
        assert_eq!(
            engine
                .create_document_role(&actor, RequestContext::new(), target("bad"), policy)
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
    }
    let guard = actor.inner.lock().await;
    assert_eq!(
        engine
            .create_document_role(
                &actor,
                RequestContext::new()
                    .with_timeout(Duration::from_millis(20))
                    .unwrap(),
                target("expired"),
                data_policy()
            )
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::DeadlineExceeded
    );
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert_eq!(
        engine
            .create_document_role(
                &actor,
                RequestContext::new().with_cancellation_token(cancel),
                target("cancelled"),
                data_policy()
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
            .create_document_role(
                &actor,
                RequestContext::new(),
                target("rotated"),
                data_policy()
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
            .create_document_role(
                &actor,
                RequestContext::new(),
                target("closed"),
                data_policy()
            )
            .await
            .is_err()
    );
    let actor = login(&engine).await;
    engine
        .create_document_role(
            &actor,
            RequestContext::new(),
            target("empty"),
            Policy::default(),
        )
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    assert!(
        engine
            .create_document_role(
                &actor,
                RequestContext::new(),
                target("shutdown"),
                data_policy()
            )
            .await
            .is_err()
    );
}
