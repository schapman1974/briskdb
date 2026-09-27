use super::*;
use std::os::unix::fs::PermissionsExt;

fn files(root: &Path) {
    fs::write(
        root.join("server.crt"),
        include_bytes!("../fixtures/postgres-tls/server.crt"),
    )
    .unwrap();
    fs::write(
        root.join("server.key"),
        include_bytes!("../fixtures/postgres-tls/server.key"),
    )
    .unwrap();
    fs::write(root.join("password"), b"daemon-original-test-secret\n").unwrap();
    for name in ["server.key", "password"] {
        fs::set_permissions(root.join(name), fs::Permissions::from_mode(0o600)).unwrap();
    }
}

fn check(mongo: bool) {
    let root = tempfile::tempdir().unwrap();
    let postgres = tempfile::tempdir().unwrap();
    let mongo_files = tempfile::tempdir().unwrap();
    files(postgres.path());
    files(mongo_files.path());
    let log = root.path().join("daemon.log");
    let data = root.path().join("data");
    let mut command = command_with_postgres(
        &data,
        &log,
        mongo.then_some("127.0.0.1:0"),
        "127.0.0.1:0",
        "127.0.0.1:0",
    );
    command
        .arg("--postgres-tls-cert")
        .arg(postgres.path().join("server.crt"))
        .arg("--postgres-tls-key")
        .arg(postgres.path().join("server.key"))
        .arg("--postgres-password-file")
        .arg(postgres.path().join("password"))
        .env("BRISKDB_RELOAD_ON_SIGHUP", "true");
    if mongo {
        command
            .arg("--mongo-tls-cert")
            .arg(mongo_files.path().join("server.crt"))
            .arg("--mongo-tls-key")
            .arg(mongo_files.path().join("server.key"));
    }
    let mut child = Process(command.spawn().unwrap());
    let text = ready(&mut child, &log);
    let pg_address = bound_address(&text, "postgres_listen");
    assert!(text.contains("security_reload_on_sighup=true"));
    let mongo_port = if mongo {
        text.split("mongo_listen=Some(")
            .nth(1)
            .expect(&text)
            .split(')')
            .next()
            .unwrap()
            .parse::<std::net::SocketAddr>()
            .unwrap()
            .port()
            .to_string()
    } else {
        "disabled".into()
    };
    let python = std::env::var("BRISKDB_MONGO_WIRE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/daemon_reload_client.py"
        ))
        .arg(child.0.id().to_string())
        .arg(&log)
        .arg(pg_address.port().to_string())
        .arg(mongo_port)
        .arg(postgres.path())
        .arg(mongo_files.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}\ndaemon:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
        fs::read_to_string(&log).unwrap()
    );
    terminate(&mut child);
}

#[test]
#[ignore = "requires pinned psycopg; run explicitly with BRISKDB_MONGO_WIRE_PYTHON"]
fn real_postgres_only_sighup_rotates_tls_and_scram() {
    check(false);
}

#[cfg(feature = "mongo-tls")]
#[test]
#[ignore = "requires pinned PyMongo and psycopg; run explicitly with BRISKDB_MONGO_WIRE_PYTHON"]
fn real_combined_sighup_validates_every_identity_before_publication() {
    check(true);
}
