use super::*;
use crate::{core::authorization::Resource, document::*};

fn request(command: DocumentCommand) -> DocumentRequest {
    DocumentRequest::new(
        DocumentRequestId::new([11; 16]).unwrap(),
        RequestContext::new(),
        command,
    )
}

fn namespace(database: &str, collection: &str) -> DocumentNamespace {
    DocumentNamespace::new(database, collection).unwrap()
}

fn find(database: &str, collection: &str) -> DocumentCommand {
    DocumentCommand::Find(DocumentFindRequest::new(
        namespace(database, collection),
        DocumentFilter::empty(),
        DocumentReadOptions::new().with_batch_size(1).unwrap(),
    ))
}

fn scoped_policy() -> Policy {
    let mut grants = Vec::new();
    for database in ["app", "local"] {
        grants.push(
            Privilege::new(
                Action::ConnectDatabase,
                Scope::exact(Resource::database(DataDomain::Document, database).unwrap()),
            )
            .unwrap(),
        );
        grants.push(
            Privilege::new(
                Action::ReadData,
                Scope::non_system_document_collections(database).unwrap(),
            )
            .unwrap(),
        );
    }
    grants.push(
        Privilege::new(
            Action::ReadData,
            Scope::exact(Resource::object(DataDomain::Document, "app", "system.js").unwrap()),
        )
        .unwrap(),
    );
    Policy::new(grants).unwrap()
}

#[tokio::test]
async fn non_system_scope_survives_durable_refresh_reopen_and_denies_retained_reserved_cursors() {
    let (root, engine) = secure(&[
        Action::ConnectDatabase,
        Action::CreateDatabase,
        Action::CreateObject,
        Action::InsertData,
        Action::ReadData,
    ])
    .await;
    let actor = login(&engine).await;
    for (database, collection) in [
        ("app", "items"),
        ("app", "system.js"),
        ("app", "system.users"),
        ("local", "items"),
        ("local", "replset.config"),
    ] {
        let ns = namespace(database, collection);
        engine
            .execute_document(
                &actor,
                request(DocumentCommand::CreateCollection(
                    DocumentCreateCollectionRequest::new(
                        ns.clone(),
                        DocumentCollectionOptions::empty(),
                        DocumentWriteOptions::new(),
                    ),
                )),
            )
            .await
            .unwrap();
        let rows = (1..=3)
            .map(|id| BsonDocument::from_entries([("_id", BsonValue::Int32(id))]).unwrap())
            .collect::<Vec<_>>();
        engine
            .execute_document(
                &actor,
                request(DocumentCommand::Insert(
                    DocumentInsertRequest::new(ns, rows, DocumentWriteOptions::new()).unwrap(),
                )),
            )
            .await
            .unwrap();
    }
    let before = engine
        .execute_document(&actor, request(find("app", "system.users")))
        .await
        .unwrap();
    let DocumentResult::Cursor(batch) = before.into_parts().2 else {
        panic!("cursor");
    };
    let cursor = batch.cursor_id().unwrap();
    let peer = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    peer.update_security_catalog(|catalog| catalog.replace_role(&role(), scoped_policy()))
        .await
        .unwrap();
    let more = DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
        namespace("app", "system.users"),
        cursor,
        DocumentReadOptions::new(),
    ));
    assert_eq!(
        engine
            .execute_document(&actor, request(more))
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    for (database, collection, allowed) in [
        ("app", "items", true),
        ("app", "system.js", true),
        ("app", "system.users", false),
        ("local", "items", true),
        ("local", "replset.config", false),
    ] {
        let result = engine
            .execute_document(&actor, request(find(database, collection)))
            .await;
        if allowed {
            assert!(result.is_ok());
        } else {
            assert_eq!(
                result.unwrap_err().kind(),
                EngineErrorKind::PermissionDenied
            );
        }
    }
    peer.shutdown().await.unwrap();
    engine.shutdown().await.unwrap();
    drop(actor);
    drop(peer);
    drop(engine);
    let reopened = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    let actor = login(&reopened).await;
    assert!(
        reopened
            .execute_document(&actor, request(find("app", "items")))
            .await
            .is_ok()
    );
    assert!(
        reopened
            .execute_document(&actor, request(find("app", "system.js")))
            .await
            .is_ok()
    );
    assert_eq!(
        reopened
            .execute_document(&actor, request(find("app", "system.users")))
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    assert_eq!(
        reopened
            .execute_document(&actor, request(find("local", "replset.config")))
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    // Explicitly restoring the old broad custom grant remains a trusted choice.
    reopened
        .update_security_catalog(|catalog| {
            catalog.replace_role(
                &role(),
                policy(&[Action::ConnectDatabase, Action::ReadData]),
            )
        })
        .await
        .unwrap();
    assert!(
        reopened
            .execute_document(&actor, request(find("app", "system.users")))
            .await
            .is_ok()
    );
}
