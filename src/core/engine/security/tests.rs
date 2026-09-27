use super::*;
use crate::core::{
    authorization::{Action, DataDomain, Policy, Privilege, Scope},
    security_catalog::tests as fixtures,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::{fs, os::unix::fs::PermissionsExt};

mod user_info;
mod user_management;

fn user() -> SecurityName {
    SecurityName::new("admin", "alice").unwrap()
}
fn role() -> SecurityName {
    SecurityName::new("admin", "reader").unwrap()
}
fn policy(actions: &[Action]) -> Policy {
    Policy::new(
        actions.iter().map(|action| {
            Privilege::new(*action, Scope::all_databases(DataDomain::Document)).unwrap()
        }),
    )
    .unwrap()
}

fn catalog(actions: &[Action]) -> SecurityCatalog {
    let mut catalog = SecurityCatalog::new();
    catalog.create_role(role(), policy(actions)).unwrap();
    catalog
        .create_user(user(), fixtures::credential(), [role()])
        .unwrap();
    catalog
}

async fn secure(actions: &[Action]) -> (tempfile::TempDir, Engine) {
    let temp = tempfile::tempdir().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    drop(Database::open(temp.path(), 2).unwrap());
    Engine::provision_security(temp.path(), 2, catalog(actions))
        .await
        .unwrap();
    let engine = Engine::open_authenticated(temp.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    (temp, engine)
}

async fn login(engine: &Engine) -> Session {
    let attempt = engine.begin_authentication(user()).await.unwrap();
    let (mut client, transcript, proof) = fixtures::exchange(&attempt, fixtures::PASSWORD);
    let (session, signature) = engine
        .complete_authentication(attempt, transcript.into_bytes(), proof)
        .await
        .unwrap();
    client
        .finish(format!("v={}", STANDARD.encode(signature)).as_bytes())
        .unwrap();
    session
}

#[tokio::test]
async fn security_is_explicit_and_never_falls_back_to_anonymous_startup() {
    let temp = tempfile::tempdir().unwrap();
    assert!(
        Engine::open_authenticated(temp.path(), 2, EngineOptions::default())
            .await
            .is_err()
    );
    assert!(!temp.path().join("manifest.sqlite").exists());
    let ordinary = Engine::open(temp.path(), 2).await.unwrap();
    assert!(!ordinary.security_enabled());
    assert!(ordinary.begin_authentication(user()).await.is_err());
    assert!(
        Engine::provision_security(temp.path(), 2, catalog(&[]))
            .await
            .is_err()
    );
    assert!(!temp.path().join(security_root::FILE_NAME).exists());
    drop(ordinary);
    let (_, engine) = secure(&[]).await;
    assert!(engine.security_enabled());
}

#[cfg(feature = "mongo")]
#[tokio::test]
async fn retained_cursor_sessions_copy_only_a_current_identity_from_the_same_engine() {
    let (root, engine) = secure(&[Action::ReadData]).await;
    let session = login(&engine).await;
    let cursor = engine.cursor_session(&session).await.unwrap();
    assert_ne!(cursor.id(), session.id());
    assert!(cursor.same_authentication(&session));
    assert!(engine.cursor_session(&engine.session()).await.is_err());
    let peer = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    assert!(peer.cursor_session(&session).await.is_err());
    engine
        .update_security_catalog(|catalog| {
            catalog.rotate_credentials(&user(), fixtures::credential())
        })
        .await
        .unwrap();
    assert!(engine.cursor_session(&session).await.is_err());
    assert!(engine.cursor_session(&cursor).await.is_err());
    let replacement = login(&engine).await;
    assert!(!replacement.same_authentication(&cursor));
}

#[tokio::test]
async fn authenticated_identity_cannot_unlock_unimplemented_sql_or_admin_paths() {
    let (_temp, engine) = secure(&[Action::ReadData]).await;
    let session = login(&engine).await;
    for result in [
        engine.status(&session).await.map(|_| ()),
        engine.shard_status().await.map(|_| ()),
        engine.checkpoint().await.map(|_| ()),
        engine.migration_summary().await.map(|_| ()),
        engine
            .query(&session, Statement::new("SELECT 1", vec![]))
            .await
            .map(|_| ()),
        engine
            .execute(&session, Statement::new("CREATE TABLE bypass(id)", vec![]))
            .await
            .map(|_| ()),
        engine
            .migrate(&session, "CREATE TABLE bypass(id)".to_owned())
            .await
            .map(|_| ()),
    ] {
        assert_eq!(
            result.unwrap_err().kind(),
            EngineErrorKind::PermissionDenied
        );
    }
    engine.shutdown().await.unwrap();
    assert!(engine.begin_authentication(user()).await.is_err());
}

#[tokio::test]
async fn stale_pending_proofs_and_cross_engine_attempts_never_produce_sessions() {
    let (temp, engine) = secure(&[Action::ReadData]).await;
    let attempt = engine.begin_authentication(user()).await.unwrap();
    let (_, transcript, proof) = fixtures::exchange(&attempt, fixtures::PASSWORD);
    let peer = Engine::open_authenticated(temp.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    assert!(
        peer.complete_authentication(attempt, transcript.into_bytes(), proof)
            .await
            .is_err()
    );
    let attempt = engine.begin_authentication(user()).await.unwrap();
    let (_, transcript, proof) = fixtures::exchange(&attempt, fixtures::PASSWORD);
    peer.update_security_catalog(|catalog| {
        catalog.rotate_credentials(&user(), fixtures::credential())
    })
    .await
    .unwrap();
    assert!(
        engine
            .complete_authentication(attempt, transcript.into_bytes(), proof)
            .await
            .is_err()
    );
    drop(login(&engine).await);
}

#[tokio::test]
async fn callback_panic_fails_closed_and_does_not_publish_an_edit() {
    let (temp, engine) = secure(&[Action::ReadData]).await;
    assert!(
        engine
            .update_security_catalog::<(), _>(|catalog| {
                catalog.drop_user(&user())?;
                panic!("injected host edit panic")
            })
            .await
            .is_err()
    );
    assert!(engine.begin_authentication(user()).await.is_err());
    let reopened = Engine::open_authenticated(temp.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    drop(login(&reopened).await);
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_adapter_cannot_select_a_secured_database() {
    let (_temp, engine) = secure(&[]).await;
    let adapter = crate::protocol::postgres::Adapter::new(engine);
    assert!(adapter.open_connection_for("alice", "briskdb").is_err());
    assert!(adapter.open_connection().status().await.is_err());
}

#[cfg(feature = "http")]
#[tokio::test]
async fn legacy_http_routers_reject_every_path_without_metadata_or_admin_side_effects() {
    use tower::ServiceExt;
    let (_temp, engine) = secure(&[]).await;
    for router in [
        crate::protocol::http::router_with_engine(engine.clone()),
        crate::protocol::http::data_router_with_engine(engine.clone()),
        crate::protocol::http::admin_router_with_engine(engine.clone()),
    ] {
        for path in [
            "/",
            "/health",
            "/ready",
            "/metrics",
            "/v1",
            "/v1/query",
            "/admin",
        ] {
            let response = router
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri(path)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), axum::http::StatusCode::FORBIDDEN);
        }
    }
    let config = crate::protocol::sqlite_remote::Config::new(
        "a-long-enough-private-token-for-tests",
        vec!["items".to_owned()],
    )
    .unwrap();
    assert!(crate::protocol::sqlite_remote::router(engine, config).is_err());
}

#[cfg(feature = "documents")]
mod documents {
    use super::*;
    use crate::document::*;

    fn ns() -> DocumentNamespace {
        DocumentNamespace::new("app", "items").unwrap()
    }
    fn request(command: DocumentCommand) -> DocumentRequest {
        DocumentRequest::new(
            DocumentRequestId::new([1; 16]).unwrap(),
            RequestContext::new(),
            command,
        )
    }
    fn find() -> DocumentCommand {
        DocumentCommand::Find(DocumentFindRequest::new(
            ns(),
            DocumentFilter::empty(),
            DocumentReadOptions::new().with_batch_size(1).unwrap(),
        ))
    }
    fn create() -> DocumentCommand {
        DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
            ns(),
            DocumentCollectionOptions::empty(),
            DocumentWriteOptions::new(),
        ))
    }
    fn insert() -> DocumentCommand {
        DocumentCommand::Insert(
            DocumentInsertRequest::new(
                ns(),
                (1..=3)
                    .map(|id| BsonDocument::from_entries([("_id", BsonValue::Int32(id))]).unwrap())
                    .collect::<Vec<_>>(),
                DocumentWriteOptions::new(),
            )
            .unwrap(),
        )
    }
    async fn seeded() -> (tempfile::TempDir, Engine, Session) {
        let (temp, engine) = secure(&[
            Action::ConnectDatabase,
            Action::CreateDatabase,
            Action::CreateObject,
            Action::InsertData,
            Action::ReadData,
        ])
        .await;
        let session = login(&engine).await;
        engine
            .execute_document(&session, request(create()))
            .await
            .unwrap();
        engine
            .execute_document(&session, request(insert()))
            .await
            .unwrap();
        (temp, engine, session)
    }
    async fn cursor(engine: &Engine, session: &Session) -> DocumentCursorId {
        let execution = engine
            .execute_document(session, request(find()))
            .await
            .unwrap();
        let DocumentResult::Cursor(batch) = execution.into_parts().2 else {
            panic!("cursor");
        };
        assert_eq!(batch.documents().len(), 1);
        batch.cursor_id().unwrap()
    }
    fn more(id: DocumentCursorId) -> DocumentCommand {
        DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
            ns(),
            id,
            DocumentReadOptions::new().with_batch_size(1).unwrap(),
        ))
    }

    #[tokio::test]
    async fn real_document_reads_writes_and_reopen_require_current_permissions() {
        let (temp, engine, session) = seeded().await;
        assert_eq!(
            engine
                .execute_document(&engine.session(), request(find()))
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::PermissionDenied
        );
        engine
            .update_security_catalog(|catalog| {
                catalog.replace_role(
                    &role(),
                    policy(&[Action::ConnectDatabase, Action::ReadData]),
                )
            })
            .await
            .unwrap();
        engine
            .execute_document(&session, request(find()))
            .await
            .unwrap();
        assert_eq!(
            engine
                .execute_document(&session, request(insert()))
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::PermissionDenied
        );
        drop(session);
        engine.shutdown().await.unwrap();
        drop(engine);
        assert!(Engine::open(temp.path(), 2).await.is_err());
        let reopened = Engine::open_authenticated(temp.path(), 2, EngineOptions::default())
            .await
            .unwrap();
        let session = login(&reopened).await;
        reopened
            .execute_document(&session, request(find()))
            .await
            .unwrap();
        assert!(
            reopened
                .execute_document(&session, request(insert()))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn retained_cursors_recheck_revocation_and_cannot_cross_sessions_or_engines() {
        let (temp, engine, session) = seeded().await;
        let id = cursor(&engine, &session).await;
        let other = login(&engine).await;
        assert!(
            engine
                .execute_document(&other, request(more(id)))
                .await
                .is_err()
        );
        let peer = Engine::open_authenticated(temp.path(), 2, EngineOptions::default())
            .await
            .unwrap();
        assert!(
            peer.execute_document(&session, request(find()))
                .await
                .is_err()
        );
        peer.update_security_catalog(|catalog| catalog.set_user_roles(&user(), []))
            .await
            .unwrap();
        assert_eq!(
            engine
                .execute_document(&session, request(more(id)))
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::PermissionDenied
        );
        peer.update_security_catalog(|catalog| catalog.set_user_roles(&user(), [role()]))
            .await
            .unwrap();
        engine
            .execute_document(&session, request(more(id)))
            .await
            .unwrap();
        peer.update_security_catalog(|catalog| {
            catalog.rotate_credentials(&user(), fixtures::credential())
        })
        .await
        .unwrap();
        assert!(
            engine
                .execute_document(&session, request(more(id)))
                .await
                .is_err()
        );
        let fresh = login(&engine).await;
        assert!(
            engine
                .execute_document(&fresh, request(more(id)))
                .await
                .is_err()
        );
        fresh.close().await.unwrap();
        assert!(
            engine
                .execute_document(&fresh, request(find()))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn every_side_of_returning_upserts_is_authorized_before_writing() {
        let (_temp, engine, session) = seeded().await;
        let update = || {
            DocumentUpdateRequest::new(
                ns(),
                DocumentFilter::empty(),
                DocumentUpdate::new(
                    BsonDocument::from_entries([(
                        "$set",
                        BsonValue::Document(
                            BsonDocument::from_entries([("changed", BsonValue::Boolean(true))])
                                .unwrap(),
                        ),
                    )])
                    .unwrap(),
                )
                .unwrap(),
                DocumentMutationScope::One,
                DocumentWriteOptions::new().with_upsert(true),
            )
        };
        for actions in [
            vec![Action::ConnectDatabase, Action::UpdateData],
            vec![
                Action::ConnectDatabase,
                Action::UpdateData,
                Action::InsertData,
            ],
            vec![Action::UpdateData, Action::InsertData, Action::ReadData],
        ] {
            engine
                .update_security_catalog(move |catalog| {
                    catalog.replace_role(&role(), policy(&actions))
                })
                .await
                .unwrap();
            let command = DocumentCommand::FindOneAndUpdate(DocumentFindOneAndUpdateRequest::new(
                update(),
                DocumentReadOptions::new(),
            ));
            assert_eq!(
                engine
                    .execute_document(&session, request(command))
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
                    policy(&[Action::ConnectDatabase, Action::ReadData]),
                )
            })
            .await
            .unwrap();
        let execution = engine
            .execute_document(&session, request(find()))
            .await
            .unwrap();
        let DocumentResult::Cursor(batch) = execution.into_parts().2 else {
            panic!("cursor");
        };
        assert!(
            batch
                .documents()
                .iter()
                .all(|row| row.get_first("changed").is_none())
        );
    }

    #[tokio::test]
    async fn metadata_cursors_recheck_their_real_permissions_not_data_read_privileges() {
        let (_temp, engine, session) = seeded().await;
        for name in ["second", "third"] {
            let command = DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                DocumentNamespace::new("app", name).unwrap(),
                DocumentCollectionOptions::empty(),
                DocumentWriteOptions::new(),
            ));
            engine
                .execute_document(&session, request(command))
                .await
                .unwrap();
        }
        engine
            .update_security_catalog(|catalog| {
                catalog.replace_role(
                    &role(),
                    policy(&[
                        Action::ConnectDatabase,
                        Action::ListObjects,
                        Action::CreateIndex,
                        Action::ListIndexes,
                    ]),
                )
            })
            .await
            .unwrap();
        for field in ["x", "y"] {
            let index = DocumentIndexRequest::new(
                BsonDocument::from_entries([(field, BsonValue::Int32(1))]).unwrap(),
            )
            .unwrap();
            engine
                .execute_document(
                    &session,
                    request(DocumentCommand::CreateBuiltIndex(
                        DocumentCreateIndexRequest::new(ns(), index, DocumentWriteOptions::new()),
                    )),
                )
                .await
                .unwrap();
        }
        let reads = || DocumentReadOptions::new().with_batch_size(1).unwrap();
        for (command, permission) in [
            (
                DocumentCommand::ListCollectionMetadata(
                    DocumentListCollectionMetadataRequest::new(
                        "app",
                        DocumentFilter::empty(),
                        true,
                        reads(),
                    )
                    .unwrap(),
                ),
                Action::ListObjects,
            ),
            (
                DocumentCommand::ListIndexMetadata(DocumentListIndexMetadataRequest::new(
                    ns(),
                    reads(),
                )),
                Action::ListIndexes,
            ),
        ] {
            let execution = engine
                .execute_document(&session, request(command))
                .await
                .unwrap();
            let DocumentResult::Cursor(batch) = execution.into_parts().2 else {
                panic!("cursor");
            };
            let cursor_id = batch.cursor_id().expect("paged metadata");
            let namespace = batch.namespace().clone();
            // ReadData alone must not authorize metadata continuation.
            engine
                .update_security_catalog(|catalog| {
                    catalog.replace_role(
                        &role(),
                        policy(&[Action::ConnectDatabase, Action::ReadData]),
                    )
                })
                .await
                .unwrap();
            let more = || {
                DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
                    namespace.clone(),
                    cursor_id,
                    reads(),
                ))
            };
            assert_eq!(
                engine
                    .execute_document(&session, request(more()))
                    .await
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::PermissionDenied
            );
            engine
                .update_security_catalog(move |catalog| {
                    catalog.replace_role(&role(), policy(&[Action::ConnectDatabase, permission]))
                })
                .await
                .unwrap();
            engine
                .execute_document(&session, request(more()))
                .await
                .unwrap();
            // Restore both metadata privileges before opening the next kind.
            engine
                .update_security_catalog(|catalog| {
                    catalog.replace_role(
                        &role(),
                        policy(&[
                            Action::ConnectDatabase,
                            Action::ListObjects,
                            Action::ListIndexes,
                        ]),
                    )
                })
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn corrupt_or_missing_authority_cannot_use_cached_roles() {
        let (temp, engine, session) = seeded().await;
        let path = temp.path().join(security_root::FILE_NAME);
        fs::rename(&path, path.with_extension("retained")).unwrap();
        assert!(
            engine
                .execute_document(&session, request(find()))
                .await
                .is_err()
        );
        fs::rename(path.with_extension("retained"), &path).unwrap();
        assert!(
            engine
                .execute_document(&session, request(find()))
                .await
                .is_err()
        );
        assert!(engine.begin_authentication(user()).await.is_err());
    }
}
