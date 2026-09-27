use super::*;
use briskdb::core::authorization::Resource;

#[tokio::test]
#[ignore = "requires pinned PyMongo; run explicitly with BRISKDB_MONGO_WIRE_PYTHON"]
async fn real_pymongo_provisioned_read_readwrite_profiles_enforce_least_privilege_after_reopen() {
    let (root, database) = secured().await;
    database
        .engine()
        .update_security_catalog(|catalog| {
            for database in ["app", "local", "config"] {
                catalog.provision_mongo_data_roles(database)?;
            }
            let grants = [
                (Action::CreateUser, "accounts"),
                (Action::ViewUsers, "accounts"),
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
            });
            let operator = name("profile_operator");
            catalog.create_role(operator.clone(), Policy::new(grants)?)?;
            let verifier = ScramSha256Verifier::from_password_with_iterations(PASSWORD, 4096)?;
            catalog.create_user(operator.clone(), verifier.clone(), [operator])?;
            for (user, role) in [("profile_reader", "read"), ("profile_writer", "readWrite")] {
                catalog.create_user(
                    name(user),
                    verifier.clone(),
                    ["app", "local", "config"]
                        .into_iter()
                        .map(|database| SecurityName::new(database, role).unwrap()),
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
    database.close().await.unwrap();
    drop(database);
    let database = BriskDb::builder(root.path())
        .with_shard_count(2)
        .with_document_support(DocumentSupport::Enabled)
        .with_authenticated_root()
        .open()
        .await
        .unwrap();
    let secrets = tempfile::tempdir().unwrap();
    let identity = config(secrets.path());
    let certificate = identity.certificate().to_path_buf();
    let mut server = MongoServer::start_tls(&database, "127.0.0.1:0".parse().unwrap(), identity)
        .await
        .unwrap();
    let port = server.address().port();
    let directory = secrets.path().to_path_buf();
    let python = std::env::var("BRISKDB_MONGO_WIRE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = tokio::task::spawn_blocking(move || {
        Command::new(python)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/mongo_data_roles_client.py"
            ))
            .arg(port.to_string())
            .arg(certificate)
            .arg(directory)
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
    database.close().await.unwrap();
}
