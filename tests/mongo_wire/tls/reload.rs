use super::*;

const ROTATED_CERT: &[u8] = include_bytes!("../../fixtures/postgres-tls/rotated.crt");
const ROTATED_KEY: &[u8] = include_bytes!("../../fixtures/postgres-tls/rotated.key");

fn rotated_config(directory: &Path) -> MongoTlsConfig {
    let config = config(directory);
    fs::write(config.certificate(), ROTATED_CERT).unwrap();
    fs::write(config.private_key(), ROTATED_KEY).unwrap();
    config
}

async fn handshake(
    stream: TcpStream,
    mut certificate: &[u8],
) -> io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in rustls_pemfile::certs(&mut certificate) {
        roots.add(certificate.unwrap()).unwrap();
    }
    let connector = TlsConnector::from(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ));
    timeout(
        Duration::from_secs(3),
        connector.connect(
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            stream,
        ),
    )
    .await
    .unwrap()
}

async fn ping(client: &mut tokio_rustls::client::TlsStream<TcpStream>) {
    client
        .write_all(&packet(&command("ping"), 19, 0))
        .await
        .unwrap();
    let (frame, body) = response(client).await;
    assert_eq!(frame.response_to, 19);
    assert_eq!(body.get_first("ok"), Some(&BsonValue::Double(1.0)));
}

#[tokio::test]
async fn rotation_preserves_established_and_admitted_handshakes_and_rejects_bad_replacements() {
    let (_root, database) = database().await;
    let old_files = tempfile::tempdir().unwrap();
    let new_files = tempfile::tempdir().unwrap();
    let original = config(old_files.path());
    let next = rotated_config(new_files.path());
    let mut server =
        MongoServer::start_tls(&database, "127.0.0.1:0".parse().unwrap(), original.clone())
            .await
            .unwrap();
    let address = server.address();
    let mut established = connect(&server, true, "localhost").await.unwrap();
    let pending = TcpStream::connect(address).await.unwrap();
    wait_for(&server, |metrics| metrics.active_connections == 2).await;
    server.reload_tls(next.clone()).await.unwrap();
    assert_eq!(server.address(), address);
    ping(&mut established).await;
    // No TLS bytes were sent before reload, but admission already froze the old identity.
    let mut pending = handshake(pending, CERT).await.unwrap();
    ping(&mut pending).await;
    assert!(connect(&server, true, "localhost").await.is_err());
    let mut new = handshake(TcpStream::connect(address).await.unwrap(), ROTATED_CERT)
        .await
        .unwrap();
    ping(&mut new).await;
    fs::write(next.private_key(), KEY).unwrap();
    assert!(server.reload_tls(next).await.is_err());
    let mut after_failure = handshake(TcpStream::connect(address).await.unwrap(), ROTATED_CERT)
        .await
        .unwrap();
    ping(&mut after_failure).await;
    server.reload_tls(original).await.unwrap();
    ping(&mut connect(&server, true, "localhost").await.unwrap()).await;
    // Reload is not revocation: the replaced generation still works on its existing socket.
    ping(&mut new).await;
    server.close().await.unwrap();
    assert_eq!(server.metrics().active_connections, 0);
    database.close().await.unwrap();
}

#[tokio::test]
async fn admitted_handshakes_keep_their_old_budget_after_reload() {
    let (_root, database) = database().await;
    let files = tempfile::tempdir().unwrap();
    let original = config(files.path());
    let mut server = MongoServer::start_tls(
        &database,
        "127.0.0.1:0".parse().unwrap(),
        original
            .clone()
            .with_handshake_timeout(Duration::from_millis(800))
            .unwrap(),
    )
    .await
    .unwrap();
    let mut old = TcpStream::connect(server.address()).await.unwrap();
    wait_for(&server, |metrics| metrics.active_connections == 1).await;
    server.reload_tls(original).await.unwrap(); // New sockets get the full 15s.
    let _new = TcpStream::connect(server.address()).await.unwrap();
    wait_for(&server, |metrics| metrics.active_connections == 2).await;
    disconnected(&mut old).await;
    wait_for(&server, |metrics| metrics.active_connections == 1).await;
    assert_eq!(server.metrics().transport_failures.timed_out, 1);
    timeout(Duration::from_secs(1), server.close())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(server.metrics().active_connections, 0);
    database.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires pinned PyMongo; run explicitly with BRISKDB_MONGO_WIRE_PYTHON"]
async fn real_pymongo_uses_rotated_certificate_with_full_validation() {
    let (_root, database) = database().await;
    let old_files = tempfile::tempdir().unwrap();
    let new_files = tempfile::tempdir().unwrap();
    let mut server = MongoServer::start_tls(
        &database,
        "127.0.0.1:0".parse().unwrap(),
        config(old_files.path()),
    )
    .await
    .unwrap();
    let replacement = rotated_config(new_files.path());
    server.reload_tls(replacement.clone()).await.unwrap();
    let port = server.address().port();
    let python = std::env::var("BRISKDB_MONGO_WIRE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = tokio::task::spawn_blocking(move || {
        Command::new(python)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/mongo_tls_client.py"
            ))
            .arg(port.to_string())
            .arg(replacement.certificate())
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
