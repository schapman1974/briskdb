use super::*;
use crate::{
    BriskDb,
    server::{AttachedServer, ListenerConfig},
};

const ROTATED_CERT: &[u8] = include_bytes!("../../../../tests/fixtures/postgres-tls/rotated.crt");
const ROTATED_KEY: &[u8] = include_bytes!("../../../../tests/fixtures/postgres-tls/rotated.key");

fn listener_config() -> ListenerConfig {
    ListenerConfig {
        http_listen: "127.0.0.1:0".parse().unwrap(),
        admin_listen: None,
        postgres_listen: Some("127.0.0.1:0".parse().unwrap()),
    }
}

fn rotated_config(directory: &Path) -> SecurityConfig {
    let old = test_security_config(directory);
    std::fs::write(old.certificate(), ROTATED_CERT).unwrap();
    std::fs::write(old.private_key(), ROTATED_KEY).unwrap();
    std::fs::write(old.password_file(), b"rotated-secret\n").unwrap();
    SecurityConfig::new(
        old.certificate(),
        old.private_key(),
        "rotated",
        old.password_file(),
    )
    .unwrap()
}

async fn tls_only(address: SocketAddr, certificate: &[u8]) -> io::Result<TestTlsStream> {
    let mut tcp = TcpStream::connect(address).await?;
    tcp.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await?;
    let mut response = [0; 1];
    tcp.read_exact(&mut response).await?;
    assert_eq!(response, *b"S");
    test_tls_connector_for(certificate)
        .connect(
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            tcp,
        )
        .await
}

async fn query_and_close(mut client: TestTlsStream) {
    client
        .write_all(&typed_packet(b'Q', b"SELECT 17\0"))
        .await
        .unwrap();
    let frames = read_until_ready(&mut client).await;
    assert!(
        frames
            .iter()
            .any(|(kind, body)| *kind == b'D' && data_row(body) == vec![Some(b"17".to_vec())])
    );
    client.write_all(&typed_packet(b'X', &[])).await.unwrap();
}

#[tokio::test]
async fn reload_rotates_certificate_key_and_credentials_without_mixing_connections() {
    let data = tempfile::tempdir().unwrap();
    let original_files = tempfile::tempdir().unwrap();
    let rotated_files = tempfile::tempdir().unwrap();
    let db = BriskDb::builder(data.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let original = test_security_config(original_files.path());
    let mut server = AttachedServer::start_secure(&db, listener_config(), original.clone())
        .await
        .unwrap();
    let address = server.addresses().postgres().unwrap();
    let established = secure_startup(address, "briskdb", "correct horse battery staple")
        .await
        .unwrap();
    // This connection has negotiated TLS but has not begun authentication.
    let in_progress = tls_only(address, &std::fs::read(original.certificate()).unwrap())
        .await
        .unwrap();
    let next = rotated_config(rotated_files.path());
    server.reload_postgres_security(next.clone()).await.unwrap();
    assert_eq!(server.addresses().postgres(), Some(address));
    query_and_close(established).await;
    query_and_close(
        authenticate_tls(in_progress, "briskdb", "correct horse battery staple")
            .await
            .unwrap(),
    )
    .await;

    // Fresh clients must trust the replacement cert, not just know a new password.
    assert!(
        tls_only(address, &std::fs::read(original.certificate()).unwrap())
            .await
            .is_err()
    );
    for (user, password) in [
        ("briskdb", "correct horse battery staple"),
        ("rotated", "wrong"),
    ] {
        let tls = tls_only(address, ROTATED_CERT).await.unwrap();
        let error = authenticate_tls(tls, user, password).await.unwrap_err();
        assert_eq!(error.get(&b'C').map(String::as_str), Some("28P01"));
    }
    let (one, two) = tokio::join!(
        tls_only(address, ROTATED_CERT),
        tls_only(address, ROTATED_CERT)
    );
    for tls in [one.unwrap(), two.unwrap()] {
        query_and_close(
            authenticate_tls(tls, "rotated", "rotated-secret")
                .await
                .unwrap(),
        )
        .await;
    }
    // A mismatched certificate/key must not displace the last valid identity.
    std::fs::write(
        next.private_key(),
        include_bytes!("../../../../tests/fixtures/postgres-tls/server.key"),
    )
    .unwrap();
    let error = server.reload_postgres_security(next).await.unwrap_err();
    assert!(!format!("{error:#}").contains("rotated-secret"));
    query_and_close(
        authenticate_tls(
            tls_only(address, ROTATED_CERT).await.unwrap(),
            "rotated",
            "rotated-secret",
        )
        .await
        .unwrap(),
    )
    .await;

    server.reload_postgres_security(original).await.unwrap();
    query_and_close(
        secure_startup(address, "briskdb", "correct horse battery staple")
            .await
            .unwrap(),
    )
    .await;
    server.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn reload_rejects_anonymous_closing_and_closed_servers_and_never_enables_security() {
    let data = tempfile::tempdir().unwrap();
    let secrets = tempfile::tempdir().unwrap();
    let db = BriskDb::builder(data.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let config = test_security_config(secrets.path());
    let mut anonymous = AttachedServer::start(&db, listener_config()).await.unwrap();
    assert!(
        anonymous
            .reload_postgres_security(config.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("already-secure")
    );
    anonymous.close().await.unwrap();
    let mut secure = AttachedServer::start_secure(&db, listener_config(), config.clone())
        .await
        .unwrap();
    secure.begin_close();
    assert!(
        secure
            .reload_postgres_security(config.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("running")
    );
    secure.close().await.unwrap();
    assert!(
        secure
            .reload_postgres_security(config)
            .await
            .unwrap_err()
            .to_string()
            .contains("running")
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn wire_connections_retain_one_snapshot_across_multiple_publications() {
    let (original_files, engine) = engine(2).await;
    let rotated_files = tempfile::tempdir().unwrap();
    let original = test_security_config(original_files.path()).load().unwrap();
    let source = ReloadableSecurity::new(original);
    let adapter = Adapter::with_reloadable_security(engine.clone(), source.clone());
    let old = adapter.wire_connection();
    let old_security = old.state.security.as_ref().unwrap().clone();
    let config = rotated_config(rotated_files.path());
    for _ in 0..8 {
        source.replace(config.load().unwrap());
        let new = adapter.wire_connection();
        assert!(Arc::ptr_eq(
            old.state.security.as_ref().unwrap(),
            &old_security
        ));
        assert_eq!(old_security.credentials.user.as_ref(), "briskdb");
        assert_eq!(
            new.state
                .security
                .as_ref()
                .unwrap()
                .credentials
                .user
                .as_ref(),
            "rotated"
        );
        assert!(!Arc::ptr_eq(
            new.state.security.as_ref().unwrap(),
            &old_security
        ));
    }
    assert!(!format!("{source:?}").contains("rotated"));
    drop(old);
    drop(adapter);
    engine.shutdown().await.unwrap();
}

#[test]
fn concurrent_publications_never_mix_certificate_and_credential_generations() {
    let original_files = tempfile::tempdir().unwrap();
    let rotated_files = tempfile::tempdir().unwrap();
    let original = test_security_config(original_files.path()).load().unwrap();
    let rotated = rotated_config(rotated_files.path()).load().unwrap();
    let original_cert = original.certificate_pem.clone();
    let source = ReloadableSecurity::new(original.clone());
    std::thread::scope(|scope| {
        for index in 0..4 {
            let source = source.clone();
            let identity = if index % 2 == 0 {
                original.clone()
            } else {
                rotated.clone()
            };
            let original_cert = original_cert.clone();
            scope.spawn(move || {
                for _ in 0..500 {
                    source.replace(identity.clone());
                    let current = source.snapshot();
                    match current.credentials.user.as_ref() {
                        "briskdb" => {
                            assert_eq!(current.certificate_pem.as_ref(), original_cert.as_ref())
                        }
                        "rotated" => assert_eq!(current.certificate_pem.as_ref(), ROTATED_CERT),
                        _ => panic!("unexpected security identity"),
                    }
                }
            });
        }
    });
}
