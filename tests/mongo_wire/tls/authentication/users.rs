use super::*;
use briskdb::core::authorization::Resource;

#[tokio::test]
#[ignore = "requires pinned PyMongo; run explicitly with BRISKDB_MONGO_WIRE_PYTHON"]
async fn real_pymongo_user_management_enforces_scopes_rotation_and_atomic_role_updates() {
    let (_root, database) = secured().await;
    database
        .engine()
        .update_security_catalog(|catalog| {
            let role = name("account_operator");
            let policy = Policy::new(
                [
                    (Action::CreateUser, "accounts"),
                    (Action::DropUser, "accounts"),
                    (Action::RotateCredentials, "accounts"),
                    (Action::GrantRole, "admin"),
                    (Action::RevokeRole, "admin"),
                ]
                .into_iter()
                .map(|(action, realm)| {
                    Privilege::new(
                        action,
                        Scope::exact(Resource::security_realm(realm).unwrap()),
                    )
                    .unwrap()
                }),
            )?;
            catalog.create_role(role.clone(), policy)?;
            catalog.create_user(
                name("operator"),
                ScramSha256Verifier::from_password_with_iterations(PASSWORD, 4096)?,
                [role],
            )
        })
        .await
        .unwrap();
    let secrets = tempfile::tempdir().unwrap();
    let identity = config(secrets.path());
    let certificate = identity.certificate().to_path_buf();
    let capture = capture::Capture::default();
    let mut server = MongoServer::start_tls(&database, "127.0.0.1:0".parse().unwrap(), identity)
        .with_subscriber(capture.clone())
        .await
        .unwrap();
    let port = server.address().port();
    let directory = secrets.path().to_path_buf();
    let python = std::env::var("BRISKDB_MONGO_WIRE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = tokio::task::spawn_blocking(move || {
        Command::new(python)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/mongo_user_management_client.py"
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
    let recorded = {
        let captured = capture.0.lock().unwrap();
        assert!(!captured.events.is_empty());
        assert!(captured.live.is_empty());
        format!("{:?}{:?}", captured.spans, captured.events)
    };
    for secret in [
        PASSWORD,
        "changed-account-password",
        "accounts",
        "operator",
        "managed-user",
        "private-admin-comment",
    ] {
        assert!(
            !recorded.contains(secret),
            "sensitive command data appeared in trace"
        );
    }
    assert_eq!(server.metrics().active_connections, 0);
    database.close().await.unwrap();
}
