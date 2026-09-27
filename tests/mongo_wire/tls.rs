use super::*;
use briskdb::protocol::mongo::{MongoResourceLimits, MongoSecurityMode, MongoTlsConfig};
use std::{fs, path::Path, sync::Arc};
use tokio_rustls::{TlsConnector, rustls};

const CERT: &[u8] = include_bytes!("../fixtures/postgres-tls/server.crt");
const KEY: &[u8] = include_bytes!("../fixtures/postgres-tls/server.key");

fn config(directory: &Path) -> MongoTlsConfig {
    let certificate = directory.join("server.crt");
    let key = directory.join("server.key");
    fs::write(&certificate, CERT).unwrap();
    fs::write(&key, KEY).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
    }
    MongoTlsConfig::new(certificate, key)
}

async fn database() -> (tempfile::TempDir, BriskDb) {
    let root = tempfile::tempdir().unwrap();
    let database = BriskDb::builder(root.path())
        .with_shard_count(2)
        .with_document_support(DocumentSupport::Enabled)
        .open()
        .await
        .unwrap();
    (root, database)
}

async fn connect(
    server: &MongoServer,
    trust: bool,
    name: &'static str,
) -> io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let mut roots = rustls::RootCertStore::empty();
    if trust {
        let mut pem = CERT;
        for certificate in rustls_pemfile::certs(&mut pem) {
            roots.add(certificate.unwrap()).unwrap();
        }
    }
    let connector = TlsConnector::from(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ));
    let stream = TcpStream::connect(server.address()).await?;
    connector
        .connect(
            rustls::pki_types::ServerName::try_from(name).unwrap(),
            stream,
        )
        .await
}

async fn wait_for(
    server: &MongoServer,
    condition: impl Fn(&briskdb::protocol::mongo::MongoMetricsSnapshot) -> bool,
) {
    timeout(Duration::from_secs(3), async {
        while !condition(&server.metrics()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn tls_handshake_budget_has_finite_inclusive_boundaries() {
    let config = MongoTlsConfig::new("certificate", "key");
    assert_eq!(config.certificate(), Path::new("certificate"));
    assert_eq!(config.private_key(), Path::new("key"));
    assert_eq!(config.handshake_timeout(), Duration::from_secs(15));
    assert!(
        config
            .clone()
            .with_handshake_timeout(Duration::ZERO)
            .is_err()
    );
    assert!(
        config
            .clone()
            .with_handshake_timeout(Duration::from_secs(15) + Duration::from_nanos(1))
            .is_err()
    );
    assert!(
        config
            .clone()
            .with_handshake_timeout(Duration::from_nanos(1))
            .is_ok()
    );
    assert!(
        config
            .with_handshake_timeout(Duration::from_secs(15))
            .is_ok()
    );
}

#[tokio::test]
async fn encrypted_mongo_preserves_certificate_validation_and_rejects_plaintext() {
    let (_root, database) = database().await;
    let secrets = tempfile::tempdir().unwrap();
    let mut server = MongoServer::start_tls(
        &database,
        "127.0.0.1:0".parse().unwrap(),
        config(secrets.path()),
    )
    .await
    .unwrap();
    assert_eq!(
        server.readiness().security,
        MongoSecurityMode::AnonymousTlsLoopback
    );
    assert_eq!(server.readiness().security.code(), "anonymous_tls_loopback");
    assert!(server.readiness().ready());
    assert!(connect(&server, false, "localhost").await.is_err());
    assert!(connect(&server, true, "wrong.invalid").await.is_err());

    let mut plaintext = TcpStream::connect(server.address()).await.unwrap();
    plaintext
        .write_all(&packet(&command("ping"), 1, 0))
        .await
        .unwrap();
    let mut bytes = Vec::new();
    let _ = timeout(Duration::from_secs(3), plaintext.read_to_end(&mut bytes))
        .await
        .unwrap();
    assert!(
        bytes.is_empty() || bytes[0] == 21,
        "at most a TLS alert, never a Mongo reply"
    );
    wait_for(&server, |metrics| metrics.active_connections == 0).await;
    assert!(server.client_metadata().is_empty());
    assert!(
        server
            .metrics()
            .commands()
            .iter()
            .all(|command| command.started == 0)
    );
    let mut client = connect(&server, true, "localhost").await.unwrap();
    client
        .write_all(&packet(&command("ping"), 7, 0))
        .await
        .unwrap();
    let (frame, body) = response(&mut client).await;
    assert_eq!(frame.response_to, 7);
    assert_eq!(body.get_first("ok"), Some(&BsonValue::Double(1.0)));
    assert!(client.get_ref().1.alpn_protocol().is_none());
    server.close().await.unwrap();
    assert_eq!(server.metrics().active_connections, 0);
    database.close().await.unwrap();
}

#[tokio::test]
async fn tls_never_permits_remote_binding_or_invalid_identity_material() {
    let (_root, database) = database().await;
    for address in ["0.0.0.0:0", "[::]:0"] {
        let error = MongoServer::start_tls(
            &database,
            address.parse().unwrap(),
            MongoTlsConfig::new("missing", "missing"),
        )
        .await
        .err()
        .unwrap();
        assert!(error.to_string().contains("requires loopback"));
    }
    let secrets = tempfile::tempdir().unwrap();
    let identity = config(secrets.path());
    fs::write(
        identity.private_key(),
        include_bytes!("../fixtures/postgres-tls/rotated.key"),
    )
    .unwrap();
    assert!(
        MongoServer::start_tls(&database, "127.0.0.1:0".parse().unwrap(), identity)
            .await
            .is_err()
    );
    let identity = config(secrets.path());
    let mut server =
        MongoServer::start_tls(&database, "127.0.0.1:0".parse().unwrap(), identity.clone())
            .await
            .unwrap();
    server.close().await.unwrap();
    database.close().await.unwrap();
    assert!(
        MongoServer::start_tls(&database, "127.0.0.1:0".parse().unwrap(), identity)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("running engine")
    );
}

#[tokio::test]
async fn tls_handshake_slots_time_out_and_shutdown_without_allocating_command_state() {
    let (_root, database) = database().await;
    let secrets = tempfile::tempdir().unwrap();
    let limits = MongoResourceLimits::new(1, Duration::from_secs(1)).unwrap();
    let identity = config(secrets.path())
        .with_handshake_timeout(Duration::from_millis(500))
        .unwrap();
    let mut server = MongoServer::start_tls_with_limits(
        &database,
        "127.0.0.1:0".parse().unwrap(),
        identity,
        limits,
    )
    .await
    .unwrap();
    let mut stalled = TcpStream::connect(server.address()).await.unwrap();
    wait_for(&server, |metrics| metrics.active_connections == 1).await;
    let mut rejected = TcpStream::connect(server.address()).await.unwrap();
    disconnected(&mut rejected).await;
    wait_for(&server, |metrics| metrics.rejected_connections == 1).await;
    assert!(server.client_metadata().is_empty());
    disconnected(&mut stalled).await;
    wait_for(&server, |metrics| metrics.active_connections == 0).await;
    assert_eq!(server.metrics().transport_failures.timed_out, 1);
    assert!(
        server
            .metrics()
            .commands()
            .iter()
            .all(|command| command.started == 0)
    );

    let mut client = connect(&server, true, "localhost").await.unwrap();
    client
        .write_all(&packet(&command("ping"), 3, 0))
        .await
        .unwrap();
    assert_eq!(
        response(&mut client).await.1.get_first("ok"),
        Some(&BsonValue::Double(1.0))
    );
    drop(client);
    wait_for(&server, |metrics| metrics.active_connections == 0).await;
    server.close().await.unwrap();
    // The next listener uses the full 15-second deadline, so the one-second
    // close bound below proves cancellation rather than natural timeout.
    let mut server = MongoServer::start_tls_with_limits(
        &database,
        "127.0.0.1:0".parse().unwrap(),
        config(secrets.path()),
        limits,
    )
    .await
    .unwrap();
    let mut partial = TcpStream::connect(server.address()).await.unwrap();
    partial.write_all(&[22, 3, 3]).await.unwrap();
    wait_for(&server, |metrics| metrics.active_connections == 1).await;
    timeout(Duration::from_secs(1), server.close())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(server.metrics().active_connections, 0);
    assert_eq!(
        server.metrics().admitted_connections,
        server.metrics().closed_connections
    );
    database.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires pinned PyMongo; run explicitly with BRISKDB_MONGO_WIRE_PYTHON"]
async fn real_pymongo_sync_async_tls_validation_and_crud() {
    let (_root, database) = database().await;
    let secrets = tempfile::tempdir().unwrap();
    let identity = config(secrets.path());
    let certificate = identity.certificate().to_path_buf();
    let mut server = MongoServer::start_tls(&database, "127.0.0.1:0".parse().unwrap(), identity)
        .await
        .unwrap();
    let port = server.address().port();
    let python = std::env::var("BRISKDB_MONGO_WIRE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = tokio::task::spawn_blocking(move || {
        Command::new(python)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/mongo_tls_client.py"
            ))
            .arg(port.to_string())
            .arg(certificate)
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
