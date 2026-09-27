//! Real process checks use native, verifying HTTPS clients; no Python dependency.
use super::*;
use std::{
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::Arc,
};
use tokio_rustls::rustls;

fn certificate(rotated: bool) -> &'static [u8] {
    if rotated {
        include_bytes!("../fixtures/postgres-tls/rotated.crt")
    } else {
        include_bytes!("../fixtures/postgres-tls/server.crt")
    }
}

fn key(rotated: bool) -> &'static [u8] {
    if rotated {
        include_bytes!("../fixtures/postgres-tls/rotated.key")
    } else {
        include_bytes!("../fixtures/postgres-tls/server.key")
    }
}

struct Identity {
    _root: tempfile::TempDir,
    certificate: PathBuf,
    key: PathBuf,
}

impl Identity {
    fn new(rotated: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let value = Self {
            certificate: root.path().join("server.crt"),
            key: root.path().join("server.key"),
            _root: root,
        };
        value.rotate(rotated);
        value
    }

    fn rotate(&self, rotated: bool) {
        fs::write(&self.certificate, certificate(rotated)).unwrap();
        fs::write(&self.key, key(rotated)).unwrap();
        fs::set_permissions(&self.key, fs::Permissions::from_mode(0o600)).unwrap();
    }

    fn configure(&self, command: &mut Command, plane: &str) {
        command
            .arg(format!("--{plane}-tls-cert"))
            .arg(&self.certificate)
            .arg(format!("--{plane}-tls-key"))
            .arg(&self.key);
    }
}

fn reserve() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap()
}

fn tcp(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&address, Duration::from_secs(3)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
}

type Client = rustls::StreamOwned<rustls::ClientConnection, TcpStream>;

fn connect(address: SocketAddr, trust: Option<bool>, name: &'static str) -> io::Result<Client> {
    let mut roots = rustls::RootCertStore::empty();
    if let Some(rotated) = trust {
        for certificate in rustls_pemfile::certs(&mut certificate(rotated)) {
            roots.add(certificate.unwrap()).unwrap();
        }
    }
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let mut connection =
        rustls::ClientConnection::new(Arc::new(config), name.try_into().unwrap()).unwrap();
    let mut stream = tcp(address);
    while connection.is_handshaking() {
        connection.complete_io(&mut stream)?;
    }
    assert_eq!(connection.alpn_protocol(), Some(b"http/1.1".as_slice()));
    Ok(rustls::StreamOwned::new(connection, stream))
}

fn request(stream: &mut (impl Read + Write), path: &str) -> String {
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .unwrap();
    stream.flush().unwrap();
    let mut result = Vec::new();
    let mut bytes = [0; 4096];
    loop {
        match stream.read(&mut bytes) {
            Ok(0) => break,
            Ok(size) => result.extend_from_slice(&bytes[..size]),
            // A closed TLS transport may omit close_notify, but it must still
            // have delivered a complete HTTP response for the assertions below.
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => panic!("HTTP request failed: {error}"),
        }
        assert!(result.len() < 65536);
    }
    let result = String::from_utf8(result).unwrap();
    let (headers, body) = result.split_once("\r\n\r\n").unwrap();
    let length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|value| value.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    assert_eq!(body.len(), length, "never accept a truncated response");
    result
}

struct Host {
    _root: tempfile::TempDir,
    process: Process,
    log: PathBuf,
    http: SocketAddr,
    admin: SocketAddr,
}

fn start(http: Option<&Identity>, admin: Option<&Identity>, reload: bool) -> Host {
    let root = tempfile::tempdir().unwrap();
    let log = root.path().join("daemon.log");
    let data = root.path().join("data");
    let data_reservation = reserve();
    let admin_reservation = reserve();
    let http_address = data_reservation.local_addr().unwrap();
    let admin_address = admin_reservation.local_addr().unwrap();
    let mut command = command_with_listeners(
        &data,
        &log,
        None,
        &http_address.to_string(),
        &admin_address.to_string(),
        "disabled",
    );
    command.args(["--shutdown-grace-ms", "100"]);
    if let Some(identity) = http {
        identity.configure(&mut command, "http");
    }
    if let Some(identity) = admin {
        identity.configure(&mut command, "admin");
    }
    if reload {
        command.arg("--reload-on-sighup");
    }
    drop(data_reservation);
    drop(admin_reservation);
    let mut process = Process(command.spawn().unwrap());
    let text = ready(&mut process, &log);
    assert!(text.contains(&format!("http_secure={}", http.is_some())));
    assert!(text.contains(&format!("admin_secure={}", admin.is_some())));
    assert!(text.contains(&format!("security_reload_on_sighup={reload}")));
    Host {
        _root: root,
        process,
        log,
        http: http_address,
        admin: admin_address,
    }
}

fn signal(host: &mut Host, success: bool, expected_count: usize) {
    assert_eq!(
        unsafe { libc::kill(host.process.0.id() as libc::pid_t, libc::SIGHUP) },
        0
    );
    let needle = if success {
        "listener security reloaded"
    } else {
        "listener security reload rejected"
    };
    let until = Instant::now() + DEADLINE;
    loop {
        let text = fs::read_to_string(&host.log).unwrap();
        if text.matches(needle).count() >= expected_count {
            return;
        }
        assert!(host.process.0.try_wait().unwrap().is_none(), "{text}");
        assert!(Instant::now() < until, "reload outcome not logged: {text}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn closed(stream: &mut impl Read) {
    let mut buffer = [0; 1024];
    let mut total = 0;
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => return,
            Ok(size) => {
                total += size;
                assert!(total < 4096);
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::UnexpectedEof
                ) =>
            {
                return;
            }
            result => panic!("expected bounded socket closure, got {result:?}"),
        }
    }
}

#[test]
fn daemon_https_planes_validate_trust_and_routes_and_close_partial_connections() {
    let data = Identity::new(false);
    let admin = Identity::new(true);
    for (data_tls, admin_tls) in [(true, false), (false, true), (true, true)] {
        let mut host = start(
            data_tls.then_some(&data),
            admin_tls.then_some(&admin),
            false,
        );
        for (encrypted, trust, address, path, forbidden) in [
            (data_tls, false, host.http, "/v1", "/health"),
            (admin_tls, true, host.admin, "/health", "/v1"),
        ] {
            if encrypted {
                assert!(connect(address, None, "localhost").is_err());
                assert!(connect(address, Some(!trust), "localhost").is_err());
                assert!(connect(address, Some(trust), "wrong.invalid").is_err());
                assert!(
                    request(
                        &mut connect(address, Some(trust), "localhost").unwrap(),
                        path
                    )
                    .starts_with("HTTP/1.1 200")
                );
                assert!(
                    request(
                        &mut connect(address, Some(trust), "localhost").unwrap(),
                        forbidden
                    )
                    .starts_with("HTTP/1.1 404")
                );
                let mut plaintext = tcp(address);
                plaintext
                    .write_all(b"GET /v1 HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    .unwrap();
                let mut response = [0; 32];
                match plaintext.read(&mut response) {
                    Ok(size) => assert!(size == 0 || response[0] == 21),
                    Err(error) => assert!(matches!(
                        error.kind(),
                        io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
                    )),
                }
            } else {
                assert!(request(&mut tcp(address), path).starts_with("HTTP/1.1 200"));
                assert!(request(&mut tcp(address), forbidden).starts_with("HTTP/1.1 404"));
            }
        }
        let (address, trust) = if data_tls {
            (host.http, false)
        } else {
            (host.admin, true)
        };
        let mut pending = tcp(address);
        pending.write_all(&[22, 3, 3]).unwrap();
        let mut established = connect(address, Some(trust), "localhost").unwrap();
        established.write_all(b"GET /v1 HTTP/1.1\r\nHost:").unwrap();
        established.flush().unwrap();
        terminate(&mut host.process);
        closed(&mut pending);
        closed(&mut established);
    }
}

#[test]
fn daemon_https_sighup_validates_both_planes_before_publication_and_preserves_live_clients() {
    let data = Identity::new(false);
    let admin = Identity::new(false);
    let mut host = start(Some(&data), Some(&admin), true);
    let mut established_data = connect(host.http, Some(false), "localhost").unwrap();
    let mut established_admin = connect(host.admin, Some(false), "localhost").unwrap();
    data.rotate(true);
    fs::write(&admin.key, b"not-a-private-key-or-a-secret-to-log").unwrap();
    signal(&mut host, false, 1);
    for (address, path) in [(host.http, "/v1"), (host.admin, "/health")] {
        assert!(
            request(
                &mut connect(address, Some(false), "localhost").unwrap(),
                path
            )
            .starts_with("HTTP/1.1 200")
        );
        assert!(connect(address, Some(true), "localhost").is_err());
    }
    admin.rotate(true);
    signal(&mut host, true, 1);
    for (address, path) in [(host.http, "/v1"), (host.admin, "/health")] {
        assert!(
            request(
                &mut connect(address, Some(true), "localhost").unwrap(),
                path
            )
            .starts_with("HTTP/1.1 200")
        );
        assert!(connect(address, Some(false), "localhost").is_err());
    }
    assert!(request(&mut established_data, "/v1").starts_with("HTTP/1.1 200"));
    assert!(request(&mut established_admin, "/health").starts_with("HTTP/1.1 200"));
    admin.rotate(false);
    fs::write(&data.key, b"bad-data-key-should-not-be-logged").unwrap();
    signal(&mut host, false, 2);
    for (address, path) in [(host.http, "/v1"), (host.admin, "/health")] {
        assert!(
            request(
                &mut connect(address, Some(true), "localhost").unwrap(),
                path
            )
            .starts_with("HTTP/1.1 200")
        );
    }
    data.rotate(false);
    signal(&mut host, true, 2);
    for (address, path) in [(host.http, "/v1"), (host.admin, "/health")] {
        assert!(
            request(
                &mut connect(address, Some(false), "localhost").unwrap(),
                path
            )
            .starts_with("HTTP/1.1 200")
        );
        assert!(connect(address, Some(true), "localhost").is_err());
    }
    terminate(&mut host.process);
    let text = fs::read_to_string(&host.log).unwrap();
    for secret in [
        "not-a-private-key-or-a-secret-to-log",
        "bad-data-key-should-not-be-logged",
        "BEGIN PRIVATE KEY",
        "BEGIN CERTIFICATE",
    ] {
        assert!(!text.contains(secret));
    }
}

#[test]
fn daemon_sighup_can_rotate_either_http_plane_without_other_secure_listeners() {
    for admin in [false, true] {
        let identity = Identity::new(false);
        let mut host = start(
            (!admin).then_some(&identity),
            admin.then_some(&identity),
            true,
        );
        let (encrypted, path, plaintext, plain_path) = if admin {
            (host.admin, "/health", host.http, "/v1")
        } else {
            (host.http, "/v1", host.admin, "/health")
        };
        identity.rotate(true);
        signal(&mut host, true, 1);
        assert!(connect(encrypted, Some(false), "localhost").is_err());
        assert!(
            request(
                &mut connect(encrypted, Some(true), "localhost").unwrap(),
                path
            )
            .starts_with("HTTP/1.1 200")
        );
        assert!(request(&mut tcp(plaintext), plain_path).starts_with("HTTP/1.1 200"));
        terminate(&mut host.process);
    }
}

#[test]
fn daemon_http_tls_preflight_fails_before_database_creation() {
    for (http, admin, flags, expected) in [
        (
            "127.0.0.1:0",
            "127.0.0.1:0",
            vec!["--http-tls-cert", "missing"],
            "must be set together",
        ),
        (
            "127.0.0.1:0",
            "127.0.0.1:0",
            vec!["--admin-tls-key", "missing"],
            "must be set together",
        ),
        (
            "127.0.0.1:0",
            "disabled",
            vec!["--admin-tls-cert", "missing", "--admin-tls-key", "missing"],
            "enabled --admin-listen",
        ),
        (
            "0.0.0.0:0",
            "127.0.0.1:0",
            vec!["--http-tls-cert", "missing", "--http-tls-key", "missing"],
            "loopback",
        ),
        (
            "127.0.0.1:0",
            "0.0.0.0:0",
            vec!["--admin-tls-cert", "missing", "--admin-tls-key", "missing"],
            "loopback",
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let log = root.path().join("daemon.log");
        let mut process = Process(
            command_with_listeners(&data, &log, None, http, admin, "disabled")
                .args(flags)
                .spawn()
                .unwrap(),
        );
        assert!(!process.wait().success());
        assert!(!data.exists());
        let text = fs::read_to_string(log).unwrap();
        assert!(text.contains(expected), "{text}");
        assert!(!text.contains("BriskDB is ready"));
    }
    for plane in ["http", "admin"] {
        let identity = Identity::new(false);
        fs::write(&identity.key, key(true)).unwrap();
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let log = root.path().join("daemon.log");
        let mut command =
            command_with_listeners(&data, &log, None, "127.0.0.1:0", "127.0.0.1:0", "disabled");
        identity.configure(&mut command, plane);
        let mut process = Process(command.spawn().unwrap());
        assert!(!process.wait().success());
        assert!(!data.exists());
        assert!(
            !fs::read_to_string(log)
                .unwrap()
                .contains("BriskDB is ready")
        );
    }
}

#[test]
fn daemon_http_tls_late_bind_failure_releases_the_data_socket_and_database() {
    let identity = Identity::new(false);
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    let log = root.path().join("daemon.log");
    let reservation = reserve();
    let occupied = reserve();
    let http = reservation.local_addr().unwrap();
    let admin = occupied.local_addr().unwrap();
    drop(reservation);
    let mut command = command_with_listeners(
        &data,
        &log,
        None,
        &http.to_string(),
        &admin.to_string(),
        "disabled",
    );
    identity.configure(&mut command, "http");
    identity.configure(&mut command, "admin");
    let mut process = Process(command.spawn().unwrap());
    assert!(!process.wait().success());
    assert!(
        !fs::read_to_string(&log)
            .unwrap()
            .contains("BriskDB is ready")
    );
    let rebound = TcpListener::bind(http).unwrap();
    drop(rebound);
    drop(occupied);
    let mut process = Process(command.spawn().unwrap());
    ready(&mut process, &log);
    assert!(
        request(&mut connect(http, Some(false), "localhost").unwrap(), "/v1")
            .starts_with("HTTP/1.1 200")
    );
    assert!(
        request(
            &mut connect(admin, Some(false), "localhost").unwrap(),
            "/health"
        )
        .starts_with("HTTP/1.1 200")
    );
    terminate(&mut process);
}
