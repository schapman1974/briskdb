use super::*;
use briskdb::core::{
    Engine,
    authentication::ScramSha256Verifier,
    authorization::{Action, DataDomain, Policy, Privilege, Scope},
    security_catalog::{SecurityCatalog, SecurityName},
};
use std::os::unix::fs::PermissionsExt;
use tracing::instrument::WithSubscriber;

mod capture {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/mongo_trace_capture.rs"
    ));
}

const PASSWORD: &str = "private test password";
const REPLACEMENT: &str = "rotated test password";

fn name(value: &str) -> SecurityName {
    SecurityName::new("admin", value).unwrap()
}
fn policy(actions: &[Action]) -> Policy {
    Policy::new(actions.iter().map(|action| {
        Privilege::new(
            *action,
            Scope::database(DataDomain::Document, "app").unwrap(),
        )
        .unwrap()
    }))
    .unwrap()
}

async fn secured() -> (tempfile::TempDir, BriskDb) {
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let ordinary = BriskDb::builder(root.path())
        .with_shard_count(2)
        .with_document_support(DocumentSupport::Enabled)
        .open()
        .await
        .unwrap();
    ordinary.close().await.unwrap();
    drop(ordinary);
    let mut catalog = SecurityCatalog::new();
    catalog
        .create_role(
            name("writer"),
            policy(&[
                Action::ConnectDatabase,
                Action::ListObjects,
                Action::CreateDatabase,
                Action::CreateObject,
                Action::DropObject,
                Action::ReadData,
                Action::InsertData,
                Action::UpdateData,
                Action::DeleteData,
                Action::ListIndexes,
                Action::CreateIndex,
                Action::DropIndex,
            ]),
        )
        .unwrap();
    catalog
        .create_role(
            name("reader"),
            policy(&[
                Action::ConnectDatabase,
                Action::ReadData,
                Action::ListObjects,
                Action::ListIndexes,
            ]),
        )
        .unwrap();
    catalog
        .create_role(
            name("metadata"),
            policy(&[
                Action::ConnectDatabase,
                Action::ListObjects,
                Action::CreateObject,
                Action::CreateDatabase,
            ]),
        )
        .unwrap();
    let credential = ScramSha256Verifier::from_password_with_iterations(PASSWORD, 4096).unwrap();
    catalog
        .create_user(name("alice"), credential.clone(), [name("writer")])
        .unwrap();
    catalog
        .create_user(name("bob"), credential.clone(), [name("reader")])
        .unwrap();
    catalog
        .create_user(name("a,b=c"), credential.clone(), [name("reader")])
        .unwrap();
    catalog
        .create_user(name("metadata"), credential, [name("metadata")])
        .unwrap();
    Engine::provision_security(root.path(), 2, catalog)
        .await
        .unwrap();
    let database = BriskDb::builder(root.path())
        .with_shard_count(2)
        .with_document_support(DocumentSupport::Enabled)
        .with_authenticated_root()
        .open()
        .await
        .unwrap();
    (root, database)
}

#[tokio::test]
async fn bound_security_requires_tls_and_gates_anonymous_data_but_not_monitoring() {
    let (_root, database) = secured().await;
    assert!(
        MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
            .await
            .is_err()
    );
    let secrets = tempfile::tempdir().unwrap();
    // Explicit authentication + TLS is the only way to bind off loopback.
    let mut remote = MongoServer::start_tls(
        &database,
        "0.0.0.0:0".parse().unwrap(),
        config(secrets.path()),
    )
    .await
    .unwrap();
    assert_eq!(
        remote.readiness().security,
        MongoSecurityMode::AuthenticatedTls
    );
    remote.close().await.unwrap();
    let mut server = MongoServer::start_tls(
        &database,
        "127.0.0.1:0".parse().unwrap(),
        config(secrets.path()),
    )
    .await
    .unwrap();
    let mut socket = connect(&server, true, "localhost").await.unwrap();
    let hello = BsonDocument::from_entries([
        ("hello", BsonValue::Int32(1)),
        ("saslSupportedMechs", BsonValue::from("admin.missing")),
        (
            "speculativeAuthenticate",
            BsonValue::Document(command("saslStart")),
        ),
        ("$db", BsonValue::from("admin")),
    ])
    .unwrap();
    socket.write_all(&packet(&hello, 1, 0)).await.unwrap();
    let (_, reply) = response(&mut socket).await;
    assert_eq!(
        reply.get_first("saslSupportedMechs"),
        Some(&BsonValue::Array(vec![BsonValue::from("SCRAM-SHA-256")]))
    );
    assert!(reply.get_first("speculativeAuthenticate").is_none());
    let find = BsonDocument::from_entries([
        ("find", BsonValue::from("items")),
        ("$db", BsonValue::from("app")),
    ])
    .unwrap();
    socket.write_all(&packet(&find, 2, 0)).await.unwrap();
    assert_eq!(
        response(&mut socket).await.1.get_first("code"),
        Some(&BsonValue::Int32(13))
    );
    socket
        .write_all(&packet(&command("ping"), 3, 0))
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
async fn real_pymongo_scram_tls_roles_pooling_and_live_credential_rotation() {
    let capture = capture::Capture::default();
    let (_root, database) = secured().await;
    let secrets = tempfile::tempdir().unwrap();
    let identity = config(secrets.path());
    let certificate = identity.certificate().to_path_buf();
    let mut server = MongoServer::start_tls(&database, "127.0.0.1:0".parse().unwrap(), identity)
        .with_subscriber(capture.clone())
        .await
        .unwrap();
    let port = server.address().port();
    let python = std::env::var("BRISKDB_MONGO_WIRE_PYTHON").unwrap_or_else(|_| "python3".into());
    let markers = secrets.path().to_path_buf();
    let directory = markers.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(python)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/mongo_auth_client.py"
            ))
            .arg(port.to_string())
            .arg(certificate)
            .arg(directory)
            .output()
            .unwrap()
    });
    let stage = timeout(Duration::from_secs(40), async {
        while !markers.join("rotate.ready").exists() {
            if output.is_finished() {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        true
    })
    .await
    .unwrap();
    if stage {
        // Transport identity reload does not clear live authenticated identities
        // or bypass the subsequent authority refresh on those sockets.
        server.reload_tls(config(secrets.path())).await.unwrap();
        database
            .engine()
            .update_security_catalog(|catalog| {
                catalog.rotate_credentials(
                    &name("alice"),
                    ScramSha256Verifier::from_password_with_iterations(REPLACEMENT, 4096)?,
                )?;
                catalog.replace_role(&name("reader"), policy(&[Action::ConnectDatabase]))
            })
            .await
            .unwrap();
        fs::write(markers.join("rotate.done"), []).unwrap();
    }
    let output = timeout(Duration::from_secs(40), output)
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stage, "client did not reach live rotation check");
    wait_for(&server, |metrics| metrics.active_connections == 0).await;
    let traces = {
        let traces = capture.0.lock().unwrap();
        assert!(!traces.events.is_empty());
        assert!(traces.live.is_empty());
        let mut sequences = std::collections::BTreeMap::new();
        for event in &traces.events {
            assert_eq!(event.len(), 11);
            let sequence = sequences.entry(&event["connection_id"]).or_insert(0u64);
            *sequence += 1;
            assert_eq!(
                event["sequence"],
                sequence.to_string(),
                "authentication must preserve connection correlation"
            );
        }
        format!("{:?}{:?}", traces.events, traces.spans)
    };
    let observations = format!(
        "{:?} {:?} {traces}",
        server.metrics(),
        server.client_metadata()
    );
    for secret in [
        PASSWORD,
        REPLACEMENT,
        "alice",
        "bob",
        "private test",
        "SCRAM",
        "client-nonce",
    ] {
        assert!(!observations.contains(secret));
    }
    server.close().await.unwrap();
    assert_eq!(server.metrics().cursors.active, 0);
    database.close().await.unwrap();
}

#[tokio::test]
async fn abandoned_authentication_expires_without_occupying_a_socket_until_idle_timeout() {
    use briskdb::document::BsonBinary;
    let (_root, database) = secured().await;
    let secrets = tempfile::tempdir().unwrap();
    let mut server = MongoServer::start_tls(
        &database,
        "127.0.0.1:0".parse().unwrap(),
        config(secrets.path()),
    )
    .await
    .unwrap();
    let mut socket = connect(&server, true, "localhost").await.unwrap();
    let start = BsonDocument::from_entries([
        ("saslStart", BsonValue::Int32(1)),
        ("mechanism", BsonValue::from("SCRAM-SHA-256")),
        (
            "payload",
            BsonValue::Binary(BsonBinary::new(0, b"n,,n=alice,r=abandoned")),
        ),
        ("$db", BsonValue::from("admin")),
    ])
    .unwrap();
    socket.write_all(&packet(&start, 1, 0)).await.unwrap();
    assert_eq!(
        response(&mut socket).await.1.get_first("done"),
        Some(&BsonValue::Boolean(false))
    );
    let mut byte = [0];
    // rustls reports unclean EOF as an error; either result means no Mongo data.
    if let Ok(length) = timeout(Duration::from_secs(12), socket.read(&mut byte))
        .await
        .unwrap()
    {
        assert_eq!(length, 0);
    }
    wait_for(&server, |metrics| metrics.active_connections == 0).await;
    assert!(server.metrics().transport_failures.timed_out > 0);
    server.close().await.unwrap();
    database.close().await.unwrap();
}
