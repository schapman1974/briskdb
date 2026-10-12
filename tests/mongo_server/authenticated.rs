use super::*;
use briskdb::{
    BriskDb, DocumentSupport,
    core::{
        Engine,
        authentication::ScramSha256Verifier,
        authorization::{Action, DataDomain, Policy, Privilege, Scope},
        security_catalog::{SecurityCatalog, SecurityName},
    },
};
use std::os::unix::fs::PermissionsExt;

#[test]
fn authenticated_process_verifies_tls_and_reopens_after_both_shutdown_signals() {
    exercise(false, false);
}

#[test]
#[ignore = "requires pinned PyMongo; run explicitly with BRISKDB_MONGO_WIRE_PYTHON"]
fn authenticated_process_sync_async_least_privilege_and_persistence() {
    exercise(true, false);
}

#[test]
fn authenticated_process_reload_preserves_failed_identity_and_existing_sockets() {
    exercise(false, true);
}

#[test]
#[ignore = "requires pinned PyMongo; run explicitly with BRISKDB_MONGO_WIRE_PYTHON"]
fn authenticated_process_reload_keeps_authenticated_sync_async_sessions() {
    exercise(true, true);
}

fn exercise(driver: bool, reload: bool) {
    let root = tempfile::tempdir().unwrap();
    let secrets = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let database = BriskDb::builder(root.path())
            .with_shard_count(2)
            .with_document_support(DocumentSupport::Enabled)
            .open()
            .await
            .unwrap();
        database.close().await.unwrap();
        drop(database);
        let mut catalog = SecurityCatalog::new();
        for (name, actions) in [
            ("reader", vec![Action::ConnectDatabase, Action::ReadData]),
            (
                "writer",
                vec![
                    Action::ConnectDatabase,
                    Action::ReadData,
                    Action::CreateDatabase,
                    Action::CreateObject,
                    Action::InsertData,
                ],
            ),
        ] {
            let role = SecurityName::new("admin", name).unwrap();
            catalog
                .create_role(
                    role.clone(),
                    Policy::new(actions.into_iter().map(|action| {
                        Privilege::new(
                            action,
                            Scope::database(DataDomain::Document, "app").unwrap(),
                        )
                        .unwrap()
                    }))
                    .unwrap(),
                )
                .unwrap();
            catalog
                .create_user(
                    role.clone(),
                    ScramSha256Verifier::from_password_with_iterations("test-only-password", 4096)
                        .unwrap(),
                    [role],
                )
                .unwrap();
        }
        Engine::provision_security(root.path(), 2, catalog)
            .await
            .unwrap();
    });
    let (certificate, key) = tls::identity(secrets.path(), false);
    for signal in [libc::SIGTERM, libc::SIGINT] {
        let log = secrets.path().join(format!("daemon-{signal}.log"));
        let output = File::create(&log).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_briskdb-mongo"));
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("BRISKDB_") {
                command.env_remove(key);
            }
        }
        command
            .args([
                "--shards",
                "2",
                "--mongo-listen",
                "127.0.0.1:0",
                "--data-dir",
            ])
            .arg(root.path())
            .arg("--mongo-tls-cert")
            .arg(&certificate)
            .arg("--mongo-tls-key")
            .arg(&key)
            .env("NO_COLOR", "1")
            .env("RUST_LOG", "info")
            .stdout(output.try_clone().unwrap())
            .stderr(output);
        if reload {
            command.arg("--reload-on-sighup");
        }
        let mut process = Process(command.spawn().unwrap());
        let text = ready(&mut process, &log);
        assert!(text.contains("Authenticated Mongo-only BriskDB is ready"));
        assert!(!text.contains("http_listen="));
        assert!(!text.contains("admin_listen="));
        assert!(!text.contains("postgres_listen="));
        let address = bound_address(&text, "mongo_listen");
        if driver {
            let python =
                std::env::var("BRISKDB_MONGO_WIRE_PYTHON").unwrap_or_else(|_| "python3".into());
            let mut driver_command = Command::new(python);
            driver_command
                .arg(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/mongo_authenticated_daemon_client.py"
                ))
                .arg(address.port().to_string())
                .arg(&certificate)
                .arg(if signal == libc::SIGTERM { "0" } else { "1" });
            if reload {
                driver_command
                    .arg(process.0.id().to_string())
                    .arg(&key)
                    .arg(&log);
            }
            let output = driver_command.output().unwrap();
            assert!(
                output.status.success(),
                "driver stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        assert!(tls::connect(address, None, "localhost").is_err());
        assert!(tls::connect(address, Some(&certificate), "wrong.example").is_err());
        // Keep a verified TLS socket open while shutdown drains it.
        let mut socket = tls::connect(address, Some(&certificate), "localhost").unwrap();
        if reload && !driver {
            use briskdb::document::{BsonDocument, BsonValue};
            let old_trust = secrets.path().join("old-ca.crt");
            fs::copy(&certificate, &old_trust).unwrap();
            tls::identity(secrets.path(), true);
            tls::reload(&process);
            tls::wait_message(&mut process, &log, "listener security reloaded", 1);
            assert!(tls::connect(address, Some(&old_trust), "localhost").is_err());
            assert!(tls::connect(address, Some(&certificate), "localhost").is_ok());
            let reply = enabled::exchange_stream(
                &mut socket,
                BsonDocument::from_entries([
                    ("ping", BsonValue::Int32(1)),
                    ("$db", BsonValue::from("admin")),
                ])
                .unwrap(),
            );
            assert_eq!(reply.get_first("ok"), Some(&BsonValue::Double(1.0)));
            fs::write(&key, include_bytes!("../fixtures/postgres-tls/server.key")).unwrap();
            tls::reload(&process);
            tls::wait_message(&mut process, &log, "listener security reload rejected", 1);
            assert!(tls::connect(address, Some(&certificate), "localhost").is_ok());
            tls::identity(secrets.path(), false);
            tls::reload(&process);
            tls::wait_message(&mut process, &log, "listener security reloaded", 2);
            assert!(tls::connect(address, Some(&old_trust), "localhost").is_ok());
            for _ in 0..100 {
                tls::reload(&process);
            }
        }
        assert_eq!(
            unsafe { libc::kill(process.0.id() as libc::pid_t, signal) },
            0
        );
        assert!(process.wait().success());
        drop(socket);
        let released = std::net::TcpListener::bind(address).unwrap();
        drop(released);
    }
    runtime.block_on(async {
        let database = BriskDb::builder(root.path())
            .with_shard_count(2)
            .with_document_support(DocumentSupport::Enabled)
            .with_authenticated_root()
            .open()
            .await
            .unwrap();
        database.close().await.unwrap();
    });
}
