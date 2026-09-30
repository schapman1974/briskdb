use super::*;
use briskdb::core::authorization::Resource;

#[tokio::test]
async fn create_role_cannot_enable_administration_on_an_anonymous_root() {
    let (_root, database) = super::super::database().await;
    let mut server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let mut socket = TcpStream::connect(server.address()).await.unwrap();
    let body = BsonDocument::from_entries([
        ("createRole", BsonValue::from("private-role")),
        ("privileges", BsonValue::Array(vec![])),
        ("roles", BsonValue::Array(vec![])),
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
async fn real_pymongo_create_role_is_database_confined_and_survives_reopen() {
    let (root, database) = secured().await;
    database
        .engine()
        .update_security_catalog(|catalog| {
            for (username, actions) in [
                (
                    "role_creator",
                    vec![
                        Action::CreateRole,
                        Action::GrantRole,
                        Action::ViewRoles,
                        Action::CreateUser,
                        Action::DropRole,
                    ],
                ),
                ("create_only", vec![Action::CreateRole]),
                ("grant_only", vec![Action::GrantRole]),
            ] {
                let role = name(username);
                catalog.create_role(
                    role.clone(),
                    Policy::new(actions.into_iter().map(|action| {
                        Privilege::new(
                            action,
                            Scope::exact(Resource::security_realm("app").unwrap()),
                        )
                        .unwrap()
                    }))?,
                )?;
                catalog.create_user(
                    role.clone(),
                    ScramSha256Verifier::from_password_with_iterations(PASSWORD, 4096)?,
                    [role],
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
    run_client(&database, "initial").await;
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
                "/tests/mongo_create_role_client.py"
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
    assert!(
        server
            .metrics()
            .command(briskdb::protocol::mongo::MongoCommandKind::CreateRole)
            .completed
            > 0
    );
}
