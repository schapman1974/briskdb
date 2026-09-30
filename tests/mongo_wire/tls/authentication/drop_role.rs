use super::*;
use briskdb::core::authorization::Resource;

#[tokio::test]
async fn drop_role_cannot_enable_administration_on_an_anonymous_root() {
    let (_root, database) = super::super::database().await;
    let mut server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let mut socket = TcpStream::connect(server.address()).await.unwrap();
    let body = BsonDocument::from_entries([
        ("dropRole", BsonValue::from("private-role")),
        ("$db", BsonValue::from("admin")),
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
async fn real_pymongo_drop_role_revokes_live_cursors_and_survives_recreation_and_reopen() {
    let (root, database) = secured().await;
    database
        .engine()
        .update_security_catalog(|catalog| {
            let operator = name("role_operator");
            catalog.create_role(
                operator.clone(),
                Policy::new([Action::DropRole, Action::ViewRoles].map(|action| {
                    Privilege::new(
                        action,
                        Scope::exact(Resource::security_realm("admin").unwrap()),
                    )
                    .unwrap()
                }))?,
            )?;
            catalog.create_user(
                operator.clone(),
                ScramSha256Verifier::from_password_with_iterations(PASSWORD, 4096)?,
                [operator],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    run_client(&database, "initial").await;
    // Recreating a deleted name is not permission to restore old memberships.
    database
        .engine()
        .update_security_catalog(|catalog| {
            catalog.create_role(
                name("reader"),
                policy(&[Action::ConnectDatabase, Action::ReadData]),
            )
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
                "/tests/mongo_drop_role_client.py"
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
            .command(briskdb::protocol::mongo::MongoCommandKind::DropRole)
            .completed
            > 0
    );
}
