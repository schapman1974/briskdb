use super::*;
use briskdb::core::authorization::Resource;

#[tokio::test]
async fn grant_role_privileges_reject_anonymous_administration_without_breaking_ping() {
    let (_root, database) = super::super::database().await;
    let mut server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let mut socket = TcpStream::connect(server.address()).await.unwrap();
    let body = BsonDocument::from_entries([
        ("grantPrivilegesToRole", BsonValue::from("private-role")),
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
async fn real_pymongo_grant_role_privileges_preserves_members_and_survives_reopen() {
    let (root, database) = secured().await;
    database
        .engine()
        .update_security_catalog(|catalog| {
            let operator = name("privilege_operator");
            catalog.create_role(
                operator.clone(),
                Policy::new([Privilege::new(
                    Action::GrantRole,
                    Scope::exact(Resource::security_realm("app")?),
                )?])?,
            )?;
            catalog.create_user(
                operator.clone(),
                ScramSha256Verifier::from_password_with_iterations(PASSWORD, 4096)?,
                [operator],
            )?;
            let role = SecurityName::new("app", "custom")?;
            catalog.create_role(
                role.clone(),
                Policy::new([
                    Privilege::new(
                        Action::ConnectDatabase,
                        Scope::exact(Resource::database(DataDomain::Document, "app")?),
                    )?,
                    Privilege::new(
                        Action::ReadData,
                        Scope::exact(Resource::object(DataDomain::Document, "app", "posts")?),
                    )?,
                ])?,
            )?;
            for user in ["dynamic_a", "dynamic_b"] {
                catalog.create_user(
                    name(user),
                    ScramSha256Verifier::from_password_with_iterations(PASSWORD, 4096)?,
                    [role.clone()],
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
    run_client(&database, "initial").await;
    database
        .engine()
        .update_security_catalog(|catalog| {
            catalog.replace_role(&name("privilege_operator"), Policy::default())
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
                "/tests/mongo_grant_role_privileges_client.py"
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
    assert_eq!(server.metrics().active_connections, 0);
    let metrics = server.metrics();
    let command =
        metrics.command(briskdb::protocol::mongo::MongoCommandKind::GrantPrivilegesToRole);
    assert!(command.started > 0);
    assert_eq!(command.in_flight, 0);
}
