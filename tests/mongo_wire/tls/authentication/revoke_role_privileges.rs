use super::*;
use briskdb::core::authorization::Resource;

#[tokio::test]
async fn revoke_role_privileges_reject_anonymous_administration_without_breaking_ping() {
    let (_root, database) = super::super::database().await;
    let mut server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let mut socket = TcpStream::connect(server.address()).await.unwrap();
    let body = BsonDocument::from_entries([
        ("revokePrivilegesFromRole", BsonValue::from("private-role")),
        ("privileges", BsonValue::Array(vec![])),
        ("$db", BsonValue::from("app")),
    ])
    .unwrap();
    socket.write_all(&packet(&body, 1, 0)).await.unwrap();
    let (_, reply) = response(&mut socket).await;
    assert_eq!(reply.get_first("code"), Some(&BsonValue::Int32(115)));
    assert!(!format!("{reply:?}").contains("private"));
    socket
        .write_all(&packet(&command("ping"), 2, 0))
        .await
        .unwrap();
    assert_eq!(
        response(&mut socket).await.1.get_first("ok"),
        Some(&BsonValue::Double(1.0))
    );
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires pinned PyMongo; run explicitly with BRISKDB_MONGO_WIRE_PYTHON"]
async fn real_pymongo_revoke_role_privileges_revokes_cursors_and_preserves_other_permissions() {
    let (root, database) = secured().await;
    database
        .engine()
        .update_security_catalog(|catalog| {
            let operator = name("revoke_operator");
            catalog.create_role(
                operator.clone(),
                Policy::new([Privilege::new(
                    Action::RevokeRole,
                    Scope::exact(Resource::security_realm("app")?),
                )?])?,
            )?;
            catalog.create_user(
                operator.clone(),
                ScramSha256Verifier::from_password_with_iterations(PASSWORD, 4096)?,
                [operator],
            )?;
            let role = SecurityName::new("app", "custom")?;
            let backup = SecurityName::new("app", "backup")?;
            let posts = Scope::exact(Resource::object(DataDomain::Document, "app", "posts")?);
            let mut grants = vec![
                Privilege::new(
                    Action::ConnectDatabase,
                    Scope::exact(Resource::database(DataDomain::Document, "app")?),
                )?,
                Privilege::new(
                    Action::CreateDatabase,
                    Scope::exact(Resource::database(DataDomain::Document, "app")?),
                )?,
                Privilege::new(
                    Action::ReadData,
                    Scope::non_system_document_collections("app")?,
                )?,
                Privilege::new(Action::ReadData, posts.clone())?,
                Privilege::new(Action::InsertData, posts.clone())?,
                Privilege::new(Action::UpdateData, posts.clone())?,
            ];
            for collection in ["scratch", "other_new"] {
                for action in [Action::CreateObject, Action::InsertData] {
                    grants.push(Privilege::new(
                        action,
                        Scope::exact(Resource::object(DataDomain::Document, "app", collection)?),
                    )?);
                }
            }
            catalog.create_role(role.clone(), Policy::new(grants)?)?;
            catalog.create_role(
                backup.clone(),
                Policy::new([Privilege::new(Action::ReadData, posts)?])?,
            )?;
            catalog.create_user(
                name("dynamic_a"),
                ScramSha256Verifier::from_password_with_iterations(PASSWORD, 4096)?,
                [role.clone()],
            )?;
            catalog.create_user(
                name("dynamic_b"),
                ScramSha256Verifier::from_password_with_iterations(PASSWORD, 4096)?,
                [role, backup],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    run_client(&database, "initial").await;
    database
        .engine()
        .update_security_catalog(|catalog| {
            catalog.replace_role(&name("revoke_operator"), Policy::default())
        })
        .await
        .unwrap();
    database.close().await.unwrap();
    drop(database);
    let reopened = BriskDb::builder(root.path())
        .with_shard_count(2)
        .with_document_support(DocumentSupport::Enabled)
        .with_authenticated_root()
        .open()
        .await
        .unwrap();
    run_client(&reopened, "reopened").await;
    reopened.close().await.unwrap();
}

async fn run_client(database: &BriskDb, phase: &'static str) {
    let secrets = tempfile::tempdir().unwrap();
    let identity = config(secrets.path());
    let certificate = identity.certificate().to_path_buf();
    let mut server = MongoServer::start_tls(database, "127.0.0.1:0".parse().unwrap(), identity)
        .await
        .unwrap();
    let port = server.address().port();
    let directory = secrets.path().to_path_buf();
    let python = std::env::var("BRISKDB_MONGO_WIRE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = tokio::task::spawn_blocking(move || {
        Command::new(python)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/mongo_revoke_role_privileges_client.py"
            ))
            .arg(port.to_string())
            .arg(certificate)
            .arg(directory)
            .arg(phase)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    server.close().await.unwrap();
    let metrics = server.metrics();
    assert_eq!(metrics.active_connections, 0);
    let command =
        metrics.command(briskdb::protocol::mongo::MongoCommandKind::RevokePrivilegesFromRole);
    assert!(command.started > 0);
    assert_eq!(command.in_flight, 0);
}
