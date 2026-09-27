use super::*;
use crate::server::{
    AttachedServer, AttachedServerOptions, BoundListeners, EngineShutdown, ListenerConfig,
    serve_listeners_with_shutdown_mode,
};
use crate::{BriskDb, EngineOptions, EngineState, Statement, Value};
use std::{fs, net::SocketAddr};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};
use tokio_rustls::{TlsConnector, rustls};

pub(super) fn identity(root: &Path, rotated: bool) -> HttpTlsConfig {
    let certificate = root.join("server.crt");
    let key = root.join("server.key");
    let (cert_bytes, key_bytes): (&[u8], &[u8]) = if rotated {
        (
            include_bytes!("../../../tests/fixtures/postgres-tls/rotated.crt"),
            include_bytes!("../../../tests/fixtures/postgres-tls/rotated.key"),
        )
    } else {
        (
            include_bytes!("../../../tests/fixtures/postgres-tls/server.crt"),
            include_bytes!("../../../tests/fixtures/postgres-tls/server.key"),
        )
    };
    fs::write(&certificate, cert_bytes).unwrap();
    fs::write(&key, key_bytes).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
    }
    HttpTlsConfig::new(certificate, key)
}

pub(super) fn config() -> ListenerConfig {
    ListenerConfig {
        http_listen: "127.0.0.1:0".parse().unwrap(),
        admin_listen: Some("127.0.0.1:0".parse().unwrap()),
        postgres_listen: None,
    }
}

pub(super) async fn database() -> (tempfile::TempDir, BriskDb) {
    let root = tempfile::tempdir().unwrap();
    let db = BriskDb::builder(root.path())
        .with_shard_count(2)
        .with_engine_options(
            EngineOptions::default()
                .with_shutdown_grace(Duration::from_millis(50))
                .unwrap(),
        )
        .open()
        .await
        .unwrap();
    (root, db)
}

pub(super) async fn tls(
    address: SocketAddr,
    certificate: Option<&Path>,
    name: &'static str,
) -> io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let stream = timeout(Duration::from_secs(3), TcpStream::connect(address))
        .await
        .unwrap()?;
    tls_stream(stream, certificate, name).await
}

pub(super) async fn tls_stream(
    stream: TcpStream,
    certificate: Option<&Path>,
    name: &'static str,
) -> io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let mut roots = rustls::RootCertStore::empty();
    if let Some(path) = certificate {
        let bytes = fs::read(path).unwrap();
        for certificate in rustls_pemfile::certs(&mut bytes.as_slice()) {
            roots.add(certificate.unwrap()).unwrap();
        }
    }
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let connector = TlsConnector::from(Arc::new(config));
    timeout(Duration::from_secs(3), async {
        connector.connect(name.try_into().unwrap(), stream).await
    })
    .await
    .unwrap()
}

pub(super) async fn request(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    path: &str,
    headers: &str,
) -> String {
    timeout(Duration::from_secs(3), async {
        stream
            .write_all(
                format!(
                    "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{headers}\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut bytes = Vec::new();
        stream.take(65536).read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.len() < 65536);
        String::from_utf8(bytes).unwrap()
    })
    .await
    .unwrap()
}

pub(super) async fn closed(stream: &mut (impl AsyncRead + Unpin)) {
    match timeout(Duration::from_secs(3), stream.read(&mut [0]))
        .await
        .unwrap()
    {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::UnexpectedEof
            ) => {}
        result => panic!("expected a closed socket, not a timeout or data: {result:?}"),
    }
}

pub(super) async fn permits(slots: &Semaphore, expected: usize) {
    timeout(Duration::from_secs(3), async {
        while slots.available_permits() != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn identity_limits_and_options_are_explicit_and_redacted() {
    let config = HttpTlsConfig::new("hidden-certificate", "hidden-key");
    assert_eq!(config.certificate(), Path::new("hidden-certificate"));
    assert_eq!(config.private_key(), Path::new("hidden-key"));
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
    let options = AttachedServerOptions::new()
        .with_http_tls(config.clone())
        .with_admin_tls(config);
    let rendered = format!("{options:?}");
    assert!(rendered.contains("http_tls: true"));
    assert!(rendered.contains("admin_tls: true"));
    assert!(!rendered.contains("hidden"));
    assert!(AttachedServerOptions::new().http_tls.is_none());
    assert!(AttachedServerOptions::new().admin_tls.is_none());
    let slots = Slots::default();
    assert_eq!(slots.data.available_permits(), 256);
    assert_eq!(slots.admin.available_permits(), 256);
}

#[tokio::test]
async fn independent_verified_tls_planes_preserve_routes_and_reject_untrusted_or_plain_clients() {
    let (_root, db) = database().await;
    let data_files = tempfile::tempdir().unwrap();
    let admin_files = tempfile::tempdir().unwrap();
    let data = identity(data_files.path(), false);
    let admin = identity(admin_files.path(), true);
    let mut server = AttachedServer::start_with_options(
        &db,
        config(),
        AttachedServerOptions::new()
            .with_http_tls(data.clone())
            .with_admin_tls(admin.clone()),
    )
    .await
    .unwrap();
    let addresses = server.addresses();
    for (address, trust, wrong_trust, path, forbidden) in [
        (
            addresses.http(),
            data.certificate(),
            admin.certificate(),
            "/v1",
            "/health",
        ),
        (
            addresses.admin().unwrap(),
            admin.certificate(),
            data.certificate(),
            "/health",
            "/v1",
        ),
    ] {
        assert!(tls(address, None, "localhost").await.is_err());
        assert!(tls(address, Some(trust), "wrong.invalid").await.is_err());
        assert!(tls(address, Some(wrong_trust), "localhost").await.is_err());
        let mut client = tls(address, Some(trust), "localhost").await.unwrap();
        assert_eq!(
            client.get_ref().1.alpn_protocol(),
            Some(b"http/1.1".as_slice())
        );
        assert!(
            request(&mut client, path, "")
                .await
                .starts_with("HTTP/1.1 200")
        );
        let mut client = tls(address, Some(trust), "localhost").await.unwrap();
        assert!(
            request(&mut client, forbidden, "")
                .await
                .starts_with("HTTP/1.1 404")
        );
        let mut plaintext = TcpStream::connect(address).await.unwrap();
        plaintext
            .write_all(b"GET /v1 HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut reply = [0; 32];
        match timeout(Duration::from_secs(3), plaintext.read(&mut reply))
            .await
            .unwrap()
        {
            Ok(size) => assert!(
                size == 0 || reply[0] == 21,
                "never return HTTP on a TLS socket"
            ),
            Err(error) => assert!(matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
            )),
        }
    }
    server.close().await.unwrap();
    assert_eq!(db.state(), EngineState::Running);
    db.close().await.unwrap();
}

#[tokio::test]
async fn preflight_and_invalid_identities_fail_before_binding_and_leave_database_running() {
    let (_root, db) = database().await;
    let missing = || HttpTlsConfig::new("missing-certificate", "missing-key");
    for admin in [false, true] {
        let mut listeners = config();
        if admin {
            listeners.admin_listen = Some("0.0.0.0:0".parse().unwrap());
        } else {
            listeners.http_listen = "0.0.0.0:0".parse().unwrap();
        }
        let error = AttachedServer::start_with_options(
            &db,
            listeners,
            AttachedServerOptions::new()
                .with_http_tls(missing())
                .with_admin_tls(missing()),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("loopback"));
    }
    let mut disabled = config();
    disabled.admin_listen = None;
    let error = AttachedServer::start_with_options(
        &db,
        disabled,
        AttachedServerOptions::new().with_admin_tls(missing()),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("enabled admin"));
    let files = tempfile::tempdir().unwrap();
    let identity = identity(files.path(), false);
    for invalid_admin in [false, true] {
        let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = reservation.local_addr().unwrap();
        drop(reservation);
        let mut listeners = config();
        listeners.http_listen = address;
        let options = if invalid_admin {
            AttachedServerOptions::new()
                .with_http_tls(identity.clone())
                .with_admin_tls(missing())
        } else {
            AttachedServerOptions::new().with_http_tls(missing())
        };
        assert!(
            AttachedServer::start_with_options(&db, listeners, options)
                .await
                .is_err()
        );
        let rebound = TcpListener::bind(address).await.unwrap();
        drop(rebound);
        assert_eq!(db.state(), EngineState::Running);
    }
    fs::write(
        identity.private_key(),
        include_bytes!("../../../tests/fixtures/postgres-tls/rotated.key"),
    )
    .unwrap();
    assert!(
        AttachedServer::start_with_options(
            &db,
            config(),
            AttachedServerOptions::new().with_http_tls(identity)
        )
        .await
        .is_err()
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn late_bind_failure_releases_tls_data_socket_and_mixed_planes_can_retry() {
    let (_root, db) = database().await;
    let files = tempfile::tempdir().unwrap();
    let identity = identity(files.path(), false);
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let listeners = ListenerConfig {
        http_listen: address,
        admin_listen: Some(occupied.local_addr().unwrap()),
        postgres_listen: None,
    };
    assert!(
        AttachedServer::start_with_options(
            &db,
            listeners,
            AttachedServerOptions::new().with_http_tls(identity.clone()),
        )
        .await
        .is_err()
    );
    let rebound = TcpListener::bind(address).await.unwrap();
    drop(rebound);
    assert_eq!(db.state(), EngineState::Running);
    drop(occupied);
    for encrypted_admin in [false, true] {
        let options = if encrypted_admin {
            AttachedServerOptions::new().with_admin_tls(identity.clone())
        } else {
            AttachedServerOptions::new().with_http_tls(identity.clone())
        };
        let mut server = AttachedServer::start_with_options(&db, listeners, options)
            .await
            .unwrap();
        let addresses = server.addresses();
        for (is_admin, address, path) in [
            (false, addresses.http(), "/v1"),
            (true, addresses.admin().unwrap(), "/health"),
        ] {
            let response = if is_admin == encrypted_admin {
                let mut client = tls(address, Some(identity.certificate()), "localhost")
                    .await
                    .unwrap();
                request(&mut client, path, "").await
            } else {
                let mut client = TcpStream::connect(address).await.unwrap();
                request(&mut client, path, "").await
            };
            assert!(response.starts_with("HTTP/1.1 200"));
        }
        server.close().await.unwrap();
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn encrypted_and_plain_admission_are_finite_and_admin_slots_stay_independent() {
    for encrypted in [false, true] {
        let (_root, db) = database().await;
        let files = tempfile::tempdir().unwrap();
        let identity = identity(files.path(), false);
        let mut listeners = BoundListeners::bind(&config()).await.unwrap();
        listeners.http_slots = Slots::new(2);
        let data_slots = listeners.http_slots.data.clone();
        let admin_slots = listeners.http_slots.admin.clone();
        if encrypted {
            listeners.http_tls.data = Some(Reloadable::new(identity.clone().load().unwrap()));
            listeners.http_tls.admin = Some(Reloadable::new(identity.clone().load().unwrap()));
        }
        let addresses = listeners.addresses().unwrap();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(serve_listeners_with_shutdown_mode(
            listeners,
            db.engine().clone(),
            async {
                let _ = stopped.await;
            },
            None,
            EngineShutdown::Borrowed,
            None,
            None,
        ));
        let mut first = TcpStream::connect(addresses.http()).await.unwrap();
        let mut second = TcpStream::connect(addresses.http()).await.unwrap();
        let partial: &[u8] = if encrypted {
            &[22, 3, 3]
        } else {
            b"GET /v1 HTTP/1.1\r\nHost:"
        };
        first.write_all(partial).await.unwrap();
        second.write_all(partial).await.unwrap();
        permits(&data_slots, 0).await;
        let mut excess = TcpStream::connect(addresses.http()).await.unwrap();
        closed(&mut excess).await;
        let reply = if encrypted {
            let mut admin = tls(
                addresses.admin().unwrap(),
                Some(identity.certificate()),
                "localhost",
            )
            .await
            .unwrap();
            request(&mut admin, "/health", "").await
        } else {
            let mut admin = TcpStream::connect(addresses.admin().unwrap())
                .await
                .unwrap();
            request(&mut admin, "/health", "").await
        };
        assert!(reply.starts_with("HTTP/1.1 200"));
        permits(&admin_slots, 2).await;
        drop(first);
        drop(second);
        permits(&data_slots, 2).await;
        let reply = if encrypted {
            let mut client = tls(addresses.http(), Some(identity.certificate()), "localhost")
                .await
                .unwrap();
            request(&mut client, "/v1", "").await
        } else {
            let mut client = TcpStream::connect(addresses.http()).await.unwrap();
            request(&mut client, "/v1", "").await
        };
        assert!(reply.starts_with("HTTP/1.1 200"));
        stop.send(()).unwrap();
        timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        permits(&data_slots, 2).await;
        assert_eq!(db.state(), EngineState::Running);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn handshake_deadline_and_server_close_or_drop_release_every_socket() {
    let (_root, db) = database().await;
    let files = tempfile::tempdir().unwrap();
    let identity = identity(files.path(), false);
    let mut server = AttachedServer::start_with_options(
        &db,
        config(),
        AttachedServerOptions::new().with_http_tls(
            identity
                .clone()
                .with_handshake_timeout(Duration::from_millis(100))
                .unwrap(),
        ),
    )
    .await
    .unwrap();
    let mut pending = TcpStream::connect(server.addresses().http()).await.unwrap();
    pending.write_all(&[22, 3, 3]).await.unwrap();
    closed(&mut pending).await;
    let mut recovered = tls(
        server.addresses().http(),
        Some(identity.certificate()),
        "localhost",
    )
    .await
    .unwrap();
    assert!(
        request(&mut recovered, "/v1", "")
            .await
            .starts_with("HTTP/1.1 200")
    );
    server.close().await.unwrap();
    for dropping in [false, true] {
        let mut server = AttachedServer::start_with_options(
            &db,
            config(),
            AttachedServerOptions::new()
                .with_http_tls(identity.clone())
                .with_admin_tls(identity.clone()),
        )
        .await
        .unwrap();
        let addresses = server.addresses();
        let mut pending = TcpStream::connect(addresses.admin().unwrap())
            .await
            .unwrap();
        pending.write_all(&[22, 3, 3]).await.unwrap();
        let mut established = tls(addresses.http(), Some(identity.certificate()), "localhost")
            .await
            .unwrap();
        established
            .write_all(b"GET /v1 HTTP/1.1\r\nHost:")
            .await
            .unwrap();
        if dropping {
            drop(server);
        } else {
            server.close().await.unwrap();
        }
        closed(&mut pending).await;
        closed(&mut established).await;
        assert_eq!(db.state(), EngineState::Running);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn tls_sqlite_remote_keeps_bearer_auth_and_admin_route_isolation() {
    const TOKEN: &str = "http-tls-remote-test-token-0123456789";
    let (_root, db) = database().await;
    let files = tempfile::tempdir().unwrap();
    let identity = identity(files.path(), false);
    let session = db.session();
    session.set_routing_key("http-tls").await.unwrap();
    db.migrate(&session, "CREATE TABLE users (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    db.execute_write(
        &session,
        Statement::new("INSERT INTO users VALUES (?1)", vec![Value::Int64(7)]),
    )
    .await
    .unwrap();
    session.close().await.unwrap();
    let remote = crate::protocol::sqlite_remote::Config::new(TOKEN, vec!["users".into()])
        .unwrap()
        .with_legacy_routing_key("http-tls".into())
        .unwrap();
    let mut server = AttachedServer::start_with_options(
        &db,
        config(),
        AttachedServerOptions::new()
            .with_http_tls(identity.clone())
            .with_admin_tls(identity.clone())
            .with_sqlite_remote(remote),
    )
    .await
    .unwrap();
    let mut client = tls(
        server.addresses().http(),
        Some(identity.certificate()),
        "localhost",
    )
    .await
    .unwrap();
    assert!(
        request(&mut client, "/sqlite/v1/catalog", "")
            .await
            .starts_with("HTTP/1.1 401")
    );
    let mut client = tls(
        server.addresses().http(),
        Some(identity.certificate()),
        "localhost",
    )
    .await
    .unwrap();
    let reply = request(
        &mut client,
        "/sqlite/v1/catalog",
        &format!("Authorization: Bearer {TOKEN}\r\n"),
    )
    .await;
    assert!(reply.starts_with("HTTP/1.1 200"));
    assert!(reply.contains("users"));
    let mut admin = tls(
        server.addresses().admin().unwrap(),
        Some(identity.certificate()),
        "localhost",
    )
    .await
    .unwrap();
    assert!(
        request(
            &mut admin,
            "/sqlite/v1/catalog",
            &format!("Authorization: Bearer {TOKEN}\r\n")
        )
        .await
        .starts_with("HTTP/1.1 404")
    );
    server.close().await.unwrap();
    db.close().await.unwrap();
}
