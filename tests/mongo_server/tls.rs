use super::*;
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::Arc,
};
use tokio_rustls::rustls;

pub(super) fn identity(root: &Path, rotated: bool) -> (PathBuf, PathBuf) {
    let certificate = root.join("server.crt");
    let key = root.join("server.key");
    let (cert_bytes, key_bytes): (&[u8], &[u8]) = if rotated {
        (
            include_bytes!("../fixtures/postgres-tls/rotated.crt"),
            include_bytes!("../fixtures/postgres-tls/rotated.key"),
        )
    } else {
        (
            include_bytes!("../fixtures/postgres-tls/server.crt"),
            include_bytes!("../fixtures/postgres-tls/server.key"),
        )
    };
    fs::write(&certificate, cert_bytes).unwrap();
    fs::write(&key, key_bytes).unwrap();
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
    (certificate, key)
}

fn tls_command(data: &Path, log: &Path, address: &str, certificate: &Path, key: &Path) -> Command {
    let mut child = command(data, log, Some(address));
    child
        .arg("--mongo-tls-cert")
        .arg(certificate)
        .arg("--mongo-tls-key")
        .arg(key);
    child
}

fn address(text: &str) -> SocketAddr {
    assert!(text.contains("mongo_secure=true"), "{text}");
    text.split("mongo_listen=Some(")
        .nth(1)
        .expect(text)
        .split(')')
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

pub(super) fn connect(
    address: SocketAddr,
    certificate: Option<&Path>,
    name: &'static str,
) -> std::io::Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>> {
    let mut roots = rustls::RootCertStore::empty();
    if let Some(path) = certificate {
        let bytes = fs::read(path).unwrap();
        for certificate in rustls_pemfile::certs(&mut bytes.as_slice()) {
            roots.add(certificate.unwrap()).unwrap();
        }
    }
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let socket = TcpStream::connect_timeout(&address, Duration::from_secs(3))?;
    socket.set_read_timeout(Some(Duration::from_secs(3)))?;
    socket.set_write_timeout(Some(Duration::from_secs(3)))?;
    let mut stream = rustls::StreamOwned::new(
        rustls::ClientConnection::new(Arc::new(config), name.try_into().unwrap()).unwrap(),
        socket,
    );
    while stream.conn.is_handshaking() {
        stream.conn.complete_io(&mut stream.sock)?;
    }
    Ok(stream)
}

#[test]
fn daemon_tls_validates_certificates_drains_and_restarts_with_persistent_data() {
    let root = tempfile::tempdir().unwrap();
    let secrets = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    for initial in [true, false] {
        let (certificate, key) = identity(secrets.path(), !initial);
        let log = root.path().join("daemon.log");
        let mut child = Process(
            tls_command(&data, &log, "127.0.0.1:0", &certificate, &key)
                .spawn()
                .unwrap(),
        );
        let address = address(&ready(&mut child, &log));
        assert!(connect(address, None, "localhost").is_err());
        assert!(connect(address, Some(&certificate), "wrong.invalid").is_err());
        let mut encrypted = connect(address, Some(&certificate), "localhost").unwrap();
        assert!(encrypted.conn.alpn_protocol().is_none());
        enabled::check_data_with(
            |command| enabled::exchange_stream(&mut encrypted, command),
            initial,
        );
        let mut partial = TcpStream::connect(address).unwrap();
        partial
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        partial.write_all(&[22, 3, 3]).unwrap();
        encrypted.write_all(&[1, 2]).unwrap();
        terminate(&mut child);
        for peer in [&mut partial as &mut dyn Read, &mut encrypted] {
            match peer.read(&mut [0]) {
                Ok(0) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::ConnectionAborted
                            | std::io::ErrorKind::UnexpectedEof
                    ) => {}
                result => panic!("TLS/partial peer was not drained: {result:?}"),
            }
        }
        assert!(TcpStream::connect(address).is_err());
    }
}

#[test]
fn daemon_tls_preflight_rejects_remote_and_invalid_identity_before_database_creation() {
    let root = tempfile::tempdir().unwrap();
    let secrets = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    let log = root.path().join("daemon.log");
    let (certificate, key) = identity(secrets.path(), false);
    for address in ["0.0.0.0:0", "[::]:0"] {
        let mut child = Process(
            tls_command(
                &data,
                &log,
                address,
                Path::new("missing.crt"),
                Path::new("missing.key"),
            )
            .spawn()
            .unwrap(),
        );
        assert!(!child.wait().success());
        assert!(
            fs::read_to_string(&log)
                .unwrap()
                .contains("Mongo startup requires a loopback")
        );
        assert!(!data.exists());
    }
    for mode in ["missing", "mismatched", "public"] {
        let invalid_key = if mode == "missing" {
            secrets.path().join("missing.key")
        } else {
            key.clone()
        };
        if mode == "mismatched" {
            fs::write(&key, include_bytes!("../fixtures/postgres-tls/rotated.key")).unwrap();
        } else if mode == "public" {
            identity(secrets.path(), false);
            fs::set_permissions(&key, fs::Permissions::from_mode(0o644)).unwrap();
        }
        let mut child = Process(
            tls_command(&data, &log, "127.0.0.1:0", &certificate, &invalid_key)
                .spawn()
                .unwrap(),
        );
        assert!(!child.wait().success());
        let text = fs::read_to_string(&log).unwrap();
        assert!(text.contains("failed to prepare Mongo TLS"), "{text}");
        assert!(!text.contains("BEGIN PRIVATE KEY"));
        assert!(!text.contains("BriskDB is ready"));
        assert!(!data.exists());
    }
}

#[test]
fn daemon_tls_bind_failure_releases_http_and_database_for_retry() {
    let root = tempfile::tempdir().unwrap();
    let secrets = tempfile::tempdir().unwrap();
    let (certificate, key) = identity(secrets.path(), false);
    let data = root.path().join("data");
    let log = root.path().join("daemon.log");
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http = reservation.local_addr().unwrap();
    drop(reservation);
    let mut child = Process(
        command_with_http(
            &data,
            &log,
            Some(&occupied.local_addr().unwrap().to_string()),
            &http.to_string(),
        )
        .arg("--mongo-tls-cert")
        .arg(&certificate)
        .arg("--mongo-tls-key")
        .arg(&key)
        .spawn()
        .unwrap(),
    );
    assert!(!child.wait().success());
    let text = fs::read_to_string(&log).unwrap();
    assert!(text.contains("failed to bind Mongo listener"), "{text}");
    let rebound = std::net::TcpListener::bind(http).unwrap();
    drop(rebound);
    let mut retry = Process(
        tls_command(&data, &log, "127.0.0.1:0", &certificate, &key)
            .spawn()
            .unwrap(),
    );
    let bound = address(&ready(&mut retry, &log));
    let mut stream = connect(bound, Some(&certificate), "localhost").unwrap();
    enabled::check_data_with(
        |command| enabled::exchange_stream(&mut stream, command),
        true,
    );
    terminate(&mut retry);
}

#[test]
#[ignore = "requires pinned PyMongo; run explicitly with BRISKDB_MONGO_WIRE_PYTHON"]
fn real_pymongo_uses_daemon_tls_and_reopens_persisted_collections() {
    let root = tempfile::tempdir().unwrap();
    let secrets = tempfile::tempdir().unwrap();
    let (certificate, key) = identity(secrets.path(), false);
    let data = root.path().join("data");
    let log = root.path().join("daemon.log");
    let python = std::env::var("BRISKDB_MONGO_WIRE_PYTHON").unwrap_or_else(|_| "python3".into());
    for mode in ["initial", "reopened"] {
        // Exercise environment configuration through the actual daemon too.
        let mut child = Process(
            command(&data, &log, Some("127.0.0.1:0"))
                .env("BRISKDB_MONGO_TLS_CERT", &certificate)
                .env("BRISKDB_MONGO_TLS_KEY", &key)
                .spawn()
                .unwrap(),
        );
        let bound = address(&ready(&mut child, &log));
        let output = Command::new(&python)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/mongo_daemon_tls_client.py"
            ))
            .arg(bound.port().to_string())
            .arg(&certificate)
            .arg(mode)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        terminate(&mut child);
    }
}

fn wait_message(child: &mut Process, log: &Path, message: &str, count: usize) {
    let until = Instant::now() + DEADLINE;
    loop {
        let text = fs::read_to_string(log).unwrap();
        if text.matches(message).count() >= count {
            return;
        }
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "daemon exited: {text}"
        );
        assert!(Instant::now() < until, "missing reload result: {text}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn reload(child: &Process) {
    assert_eq!(
        unsafe { libc::kill(child.0.id() as libc::pid_t, libc::SIGHUP) },
        0
    );
}

#[test]
fn daemon_sighup_rotates_mongo_preserves_failed_reloads_and_drains_signal_bursts() {
    let root = tempfile::tempdir().unwrap();
    let secrets = tempfile::tempdir().unwrap();
    let (certificate, key) = identity(secrets.path(), false);
    let old_trust = root.path().join("original-ca.crt");
    fs::copy(&certificate, &old_trust).unwrap();
    let data = root.path().join("data");
    let log = root.path().join("daemon.log");
    let mut child = Process(
        tls_command(&data, &log, "127.0.0.1:0", &certificate, &key)
            .arg("--reload-on-sighup")
            .spawn()
            .unwrap(),
    );
    let text = ready(&mut child, &log);
    assert!(text.contains("security_reload_on_sighup=true"));
    let bound = address(&text);
    let mut original = connect(bound, Some(&old_trust), "localhost").unwrap();
    enabled::check_data_with(
        |command| enabled::exchange_stream(&mut original, command),
        true,
    );
    identity(secrets.path(), true);
    reload(&child);
    wait_message(&mut child, &log, "listener security reloaded", 1);
    assert!(connect(bound, Some(&old_trust), "localhost").is_err());
    let mut current = connect(bound, Some(&certificate), "localhost").unwrap();
    enabled::check_data_with(
        |command| enabled::exchange_stream(&mut current, command),
        false,
    );
    enabled::check_data_with(
        |command| enabled::exchange_stream(&mut original, command),
        false,
    );
    fs::write(&key, include_bytes!("../fixtures/postgres-tls/server.key")).unwrap();
    reload(&child);
    wait_message(&mut child, &log, "listener security reload rejected", 1);
    assert!(connect(bound, Some(&certificate), "localhost").is_ok());
    identity(secrets.path(), false);
    reload(&child);
    wait_message(&mut child, &log, "listener security reloaded", 2);
    assert!(connect(bound, Some(&old_trust), "localhost").is_ok());
    for _ in 0..100 {
        reload(&child);
    }
    terminate(&mut child);
    assert!(TcpStream::connect(bound).is_err());
    let text = fs::read_to_string(log).unwrap();
    assert!(!text.contains("BEGIN PRIVATE KEY"));
}
