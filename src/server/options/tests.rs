use super::*;
use crate::{DocumentSupport, EngineState, Statement, Value, protocol::mongo::MongoTlsConfig};
use std::path::Path;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_rustls::{TlsConnector, rustls};

const CERT: &[u8] = include_bytes!("../../../tests/fixtures/postgres-tls/server.crt");
const KEY: &[u8] = include_bytes!("../../../tests/fixtures/postgres-tls/server.key");
const TOKEN: &str = "composed_sqlite_test_token_0123456789";

fn identity(root: &Path) -> (MongoTlsConfig, postgres::SecurityConfig) {
    let certificate = root.join("server.crt");
    let key = root.join("server.key");
    let password = root.join("password");
    std::fs::write(&certificate, CERT).unwrap();
    std::fs::write(&key, KEY).unwrap();
    std::fs::write(&password, b"composed-test-secret\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for file in [&key, &password] {
            std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
    }
    (
        MongoTlsConfig::new(&certificate, &key),
        postgres::SecurityConfig::new(certificate, key, "briskdb", password).unwrap(),
    )
}

fn config() -> ListenerConfig {
    ListenerConfig {
        http_listen: "127.0.0.1:0".parse().unwrap(),
        admin_listen: Some("127.0.0.1:0".parse().unwrap()),
        postgres_listen: Some("127.0.0.1:0".parse().unwrap()),
    }
}

async fn database(documents: DocumentSupport) -> (tempfile::TempDir, BriskDb) {
    let root = tempfile::tempdir().unwrap();
    let db = BriskDb::builder(root.path())
        .with_shard_count(2)
        .with_document_support(documents)
        .open()
        .await
        .unwrap();
    (root, db)
}

async fn closed(address: SocketAddr) {
    timeout(Duration::from_secs(3), async {
        while TcpStream::connect(address).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

async fn tls(address: SocketAddr) -> tokio_rustls::client::TlsStream<TcpStream> {
    let mut roots = rustls::RootCertStore::empty();
    let mut pem = CERT;
    for certificate in rustls_pemfile::certs(&mut pem) {
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
            TcpStream::connect(address).await.unwrap(),
        ),
    )
    .await
    .unwrap()
    .unwrap()
}

#[test]
fn composed_options_preserve_tls_when_changing_addresses_and_redact_credentials() {
    let files = tempfile::tempdir().unwrap();
    let (mongo, postgres) = identity(files.path());
    let options = AttachedServerOptions::new()
        .with_mongo_tls("127.0.0.1:0".parse().unwrap(), mongo.clone())
        .with_mongo("[::1]:0".parse().unwrap())
        .with_postgres_security(postgres)
        .with_sqlite_remote(
            crate::protocol::sqlite_remote::Config::new(TOKEN, vec!["users".into()]).unwrap(),
        );
    assert_eq!(options.mongo_tls, Some(mongo));
    let rendered = format!("{options:?}");
    for hidden in [
        TOKEN,
        "composed-test-secret",
        "server.key",
        "server.crt",
        "password",
    ] {
        assert!(!rendered.contains(hidden));
    }
    assert!(rendered.contains("mongo_tls: true"));
}

#[tokio::test]
async fn preflight_rejects_remote_collision_disabled_documents_before_identity_io() {
    let (_root, database) = database(DocumentSupport::Enabled).await;
    let missing = || MongoTlsConfig::new("missing-cert", "missing-key");
    for address in ["0.0.0.0:0", "[::]:0"] {
        let error = AttachedServer::start_with_options(
            &database,
            config(),
            AttachedServerOptions::new().with_mongo_tls(address.parse().unwrap(), missing()),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("loopback"));
    }
    let busy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = busy.local_addr().unwrap();
    let mut collision = config();
    collision.http_listen = address;
    let error = AttachedServer::start_with_options(
        &database,
        collision,
        AttachedServerOptions::new().with_mongo_tls(address, missing()),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("distinct addresses"));
    database.close().await.unwrap();
    let error = AttachedServer::start_with_options(
        &database,
        config(),
        AttachedServerOptions::new().with_mongo_tls("127.0.0.1:0".parse().unwrap(), missing()),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("running database"));
    let (_root, disabled) = self::database(DocumentSupport::Disabled).await;
    let error = AttachedServer::start_with_options(
        &disabled,
        config(),
        AttachedServerOptions::new().with_mongo_tls("127.0.0.1:0".parse().unwrap(), missing()),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("enabled document support"));
    disabled.close().await.unwrap();
}

#[tokio::test]
async fn identity_and_late_bind_failures_release_sockets_and_allow_retry() {
    let (_root, database) = database(DocumentSupport::Enabled).await;
    let files = tempfile::tempdir().unwrap();
    let (mongo, postgres) = identity(files.path());
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut listeners = config();
    listeners.http_listen = reserved.local_addr().unwrap();
    drop(reserved);
    let error = AttachedServer::start_with_options(
        &database,
        listeners,
        AttachedServerOptions::new()
            .with_postgres_security(postgres.clone())
            .with_mongo_tls(
                "127.0.0.1:0".parse().unwrap(),
                MongoTlsConfig::new("missing", "missing"),
            ),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("Mongo TLS"));
    drop(TcpListener::bind(listeners.http_listen).await.unwrap());
    let busy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mongo_address = busy.local_addr().unwrap();
    let error = AttachedServer::start_with_options(
        &database,
        listeners,
        AttachedServerOptions::new()
            .with_postgres_security(postgres.clone())
            .with_mongo_tls(mongo_address, mongo.clone()),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("bind Mongo"));
    drop(TcpListener::bind(listeners.http_listen).await.unwrap());
    assert_eq!(database.state(), EngineState::Running);
    drop(busy);
    let mut server = AttachedServer::start_with_options(
        &database,
        listeners,
        AttachedServerOptions::new()
            .with_postgres_security(postgres)
            .with_mongo_tls(mongo_address, mongo),
    )
    .await
    .unwrap();
    drop(tls(server.addresses().mongo().unwrap()).await);
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn attached_tls_close_and_drop_drain_encrypted_and_partial_handshakes() {
    for drop_handle in [false, true] {
        let (_root, database) = database(DocumentSupport::Enabled).await;
        let files = tempfile::tempdir().unwrap();
        let (mongo, _) = identity(files.path());
        let mut server = AttachedServer::start_with_options(
            &database,
            config(),
            AttachedServerOptions::new().with_mongo_tls("127.0.0.1:0".parse().unwrap(), mongo),
        )
        .await
        .unwrap();
        let addresses = server.addresses();
        let mut established = tls(addresses.mongo().unwrap()).await;
        assert!(established.get_ref().1.alpn_protocol().is_none());
        let mut pending = TcpStream::connect(addresses.mongo().unwrap())
            .await
            .unwrap();
        pending.write_all(&[22, 3, 3]).await.unwrap();
        if drop_handle {
            drop(server);
        } else {
            timeout(Duration::from_secs(2), server.close())
                .await
                .unwrap()
                .unwrap();
            server.close().await.unwrap();
        }
        for address in [
            Some(addresses.http()),
            addresses.admin(),
            addresses.postgres(),
            addresses.mongo(),
        ]
        .into_iter()
        .flatten()
        {
            closed(address).await;
        }
        let mut byte = [0];
        assert!(matches!(
            timeout(Duration::from_secs(2), established.read(&mut byte))
                .await
                .unwrap(),
            Ok(0) | Err(_)
        ));
        assert!(matches!(
            timeout(Duration::from_secs(2), pending.read(&mut byte))
                .await
                .unwrap(),
            Ok(0) | Err(_)
        ));
        assert_eq!(database.state(), EngineState::Running);
        database.close().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires pinned PyMongo and psycopg; use BRISKDB_MONGO_WIRE_PYTHON"]
async fn real_clients_compose_mongo_tls_postgres_scram_and_sqlite_remote() {
    let (_root, database) = database(DocumentSupport::Enabled).await;
    let files = tempfile::tempdir().unwrap();
    let (mongo, postgres) = identity(files.path());
    let certificate = mongo.certificate().to_owned();
    let session = database.session();
    session.set_routing_key("composed-sql").await.unwrap();
    database
        .migrate(&session, "CREATE TABLE users (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    database
        .execute_write(
            &session,
            Statement::new("INSERT INTO users VALUES (?1)", vec![Value::Int64(7)]),
        )
        .await
        .unwrap();
    session.close().await.unwrap();
    let remote = crate::protocol::sqlite_remote::Config::new(TOKEN, vec!["users".into()])
        .unwrap()
        .with_legacy_routing_key("composed-sql".into())
        .unwrap();
    let mut server = AttachedServer::start_with_options(
        &database,
        config(),
        AttachedServerOptions::new()
            .with_postgres_security(postgres)
            .with_sqlite_remote(remote)
            .with_mongo_tls("127.0.0.1:0".parse().unwrap(), mongo),
    )
    .await
    .unwrap();
    let addresses = server.addresses();
    let python = std::env::var("BRISKDB_MONGO_WIRE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(python)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/mongo_composed_tls_client.py"
            ))
            .arg(addresses.mongo().unwrap().port().to_string())
            .arg(addresses.postgres().unwrap().port().to_string())
            .arg(format!("http://{}", addresses.http()))
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
    assert_eq!(database.state(), EngineState::Running);
    database.close().await.unwrap();
}
