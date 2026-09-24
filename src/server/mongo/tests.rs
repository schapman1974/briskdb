use std::{net::SocketAddr, time::Duration};

use tokio::{io::AsyncWriteExt, net::TcpStream, sync::oneshot, time::timeout};

use crate::{BriskDb, CancellationToken, DocumentSupport, EngineState};

use super::super::{
    AttachedServer, EngineShutdown, ListenerConfig, bind_configured_listeners,
    serve_listeners_with_shutdown_mode,
};

fn config() -> ListenerConfig {
    ListenerConfig {
        http_listen: "127.0.0.1:0".parse().unwrap(),
        admin_listen: Some("127.0.0.1:0".parse().unwrap()),
        postgres_listen: Some("127.0.0.1:0".parse().unwrap()),
    }
}

async fn database(root: &std::path::Path) -> BriskDb {
    BriskDb::builder(root)
        .with_shard_count(2)
        .with_document_support(DocumentSupport::Enabled)
        .open()
        .await
        .unwrap()
}

async fn assert_closed(address: SocketAddr) {
    timeout(Duration::from_secs(3), async {
        while TcpStream::connect(address).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn validates_mongo_loopback_fixed_collisions_and_independent_ephemeral_ports() {
    for address in ["127.0.0.1:0", "[::1]:0", "127.0.0.1:27017", "[::1]:27017"] {
        super::validate_address(&config(), address.parse().unwrap()).unwrap();
    }
    for address in ["0.0.0.0:0", "[::]:27017", "192.0.2.1:27017"] {
        assert!(super::validate_address(&config(), address.parse().unwrap()).is_err());
    }
    let address = "127.0.0.1:27017".parse().unwrap();
    for listener in 0..3 {
        let mut config = config();
        match listener {
            0 => config.http_listen = address,
            1 => config.admin_listen = Some(address),
            _ => config.postgres_listen = Some(address),
        }
        assert!(
            super::validate_address(&config, address)
                .unwrap_err()
                .to_string()
                .contains("distinct addresses")
        );
    }
}

#[tokio::test]
async fn attached_mongo_requires_documents_and_running_engine_without_changing_defaults() {
    let root = tempfile::tempdir().unwrap();
    let db = BriskDb::builder(root.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let error = AttachedServer::start_with_mongo(&db, config(), "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("explicitly enabled document support")
    );
    assert_eq!(db.state(), EngineState::Running);
    let mut default = AttachedServer::start(&db, config()).await.unwrap();
    assert_eq!(default.addresses().mongo(), None);
    default.close().await.unwrap();
    db.close().await.unwrap();

    let db = database(root.path()).await;
    db.close().await.unwrap();
    let error = AttachedServer::start_with_mongo(&db, config(), "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("running database"));
}

#[tokio::test]
async fn mongo_bind_failure_releases_other_sockets_and_preserves_borrowed_engine() {
    let root = tempfile::tempdir().unwrap();
    let db = database(root.path()).await;
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reserved = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = reserved.local_addr().unwrap();
    drop(reserved);
    let mut config = config();
    config.http_listen = http;
    let error = AttachedServer::start_with_mongo(&db, config, occupied.local_addr().unwrap())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("failed to bind Mongo listener"));
    let _rebound = tokio::net::TcpListener::bind(http).await.unwrap();
    assert_eq!(db.state(), EngineState::Running);
    db.close().await.unwrap();
}

#[cfg(feature = "server")]
#[tokio::test]
async fn owned_mongo_preflight_creates_no_files_and_bind_failure_releases_engine() {
    use super::super::{Config, run_with_mongo};
    let root = tempfile::tempdir().unwrap();
    let data_dir = root.path().join("data");
    let config = Config {
        listen: "127.0.0.1:0".parse().unwrap(),
        admin_listen: None,
        postgres_listen: None,
        postgres_security: None,
        data_dir: data_dir.clone(),
        shards: 2,
    };
    let error = run_with_mongo(
        config.clone(),
        Default::default(),
        "0.0.0.0:0".parse().unwrap(),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Mongo startup requires a loopback")
    );
    assert!(!data_dir.exists());

    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let error = run_with_mongo(config, Default::default(), occupied.local_addr().unwrap())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("failed to bind Mongo listener"));
    // The failed owned startup must release the database for another owner.
    let reopened = database(&data_dir).await;
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn mongo_owned_shutdown_and_abort_close_all_listeners_and_allow_reopen() {
    for abort in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path()).await;
        let listeners =
            bind_configured_listeners(&config(), &db, Some("127.0.0.1:0".parse().unwrap()))
                .await
                .unwrap();
        let addresses = listeners.addresses().unwrap();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(serve_listeners_with_shutdown_mode(
            listeners,
            db.engine().clone(),
            async {
                let _ = stopped.await;
            },
            None,
            EngineShutdown::Owned,
            None,
            None,
        ));
        // Ensure the serving future (including its owned shutdown guard) runs.
        let mut mongo = TcpStream::connect(addresses.mongo().unwrap())
            .await
            .unwrap();
        mongo.write_all(&[1, 2]).await.unwrap();
        let mut http = TcpStream::connect(addresses.http()).await.unwrap();
        http.write_all(b"GET /v1 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        use tokio::io::AsyncReadExt;
        let mut reply = [0; 12];
        timeout(Duration::from_secs(3), http.read_exact(&mut reply))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&reply, b"HTTP/1.1 200");
        if abort {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert_eq!(db.state(), EngineState::Draining);
            db.close().await.unwrap();
        } else {
            stop.send(()).unwrap();
            timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        assert_eq!(db.state(), EngineState::Stopped);
        for address in [
            Some(addresses.http()),
            addresses.admin(),
            addresses.postgres(),
            addresses.mongo(),
        ]
        .into_iter()
        .flatten()
        {
            assert_closed(address).await;
        }
        let mut byte = [0];
        assert!(matches!(
            timeout(Duration::from_secs(3), mongo.read(&mut byte))
                .await
                .unwrap(),
            Ok(0) | Err(_)
        ));
        let reopened = database(root.path()).await;
        reopened.close().await.unwrap();
    }
}

#[tokio::test]
async fn unexpected_mongo_stop_drains_primary_and_is_not_success() {
    let root = tempfile::tempdir().unwrap();
    let db = database(root.path()).await;
    let mongo = crate::protocol::mongo::MongoServer::start(&db, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = mongo.address();
    let (stop, stopped) = oneshot::channel();
    let (drained, completion) = oneshot::channel();
    mongo.begin_close();
    let error = super::coordinate(
        mongo,
        async {
            stopped.await.unwrap();
            drained.send(()).unwrap();
            Ok(())
        },
        stop,
        CancellationToken::new(),
        db.engine().clone(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("stopped unexpectedly"));
    completion.await.unwrap();
    assert_closed(address).await;
    assert_eq!(db.state(), EngineState::Running);
    db.close().await.unwrap();
}

#[tokio::test]
async fn primary_failure_closes_mongo_and_cancelled_wait_can_still_be_joined() {
    let root = tempfile::tempdir().unwrap();
    let db = database(root.path()).await;
    let mut mongo = crate::protocol::mongo::MongoServer::start(&db, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = mongo.address();
    assert!(
        timeout(Duration::from_millis(10), mongo.wait())
            .await
            .is_err()
    );
    let (stop, _stopped) = oneshot::channel();
    let error = super::coordinate(
        mongo,
        async { anyhow::bail!("primary failed") },
        stop,
        CancellationToken::new(),
        db.engine().clone(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.to_string(), "primary failed");
    assert_closed(address).await;
    assert_eq!(db.state(), EngineState::Running);
    db.close().await.unwrap();
}

#[tokio::test]
async fn cancelled_attached_close_retains_join_handle() {
    let root = tempfile::tempdir().unwrap();
    let db = database(root.path()).await;
    let mut server =
        AttachedServer::start_with_mongo(&db, config(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
    let addresses = server.addresses();
    // Deterministic pending task instead of relying on cleanup scheduling speed.
    let actual = server.task.take().unwrap();
    let (release, released) = oneshot::channel();
    server.task = Some(tokio::spawn(async move {
        let outcome = actual.await.unwrap();
        released.await.unwrap();
        outcome
    }));
    assert!(
        timeout(Duration::from_millis(10), server.close())
            .await
            .is_err()
    );
    assert!(server.task.is_some());
    release.send(()).unwrap();
    assert!(!server.close().await.unwrap());
    assert!(server.close().await.unwrap());
    assert_closed(addresses.mongo().unwrap()).await;
    assert_eq!(db.state(), EngineState::Running);
    db.close().await.unwrap();
}

#[tokio::test]
async fn requested_mongo_stop_is_success_even_when_it_finishes_first() {
    let root = tempfile::tempdir().unwrap();
    let db = database(root.path()).await;
    let mongo = crate::protocol::mongo::MongoServer::start(&db, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let requested = CancellationToken::new();
    requested.cancel();
    mongo.begin_close();
    let (stop, stopped) = oneshot::channel();
    super::coordinate(
        mongo,
        async {
            stopped.await.unwrap();
            Ok(())
        },
        stop,
        requested,
        db.engine().clone(),
    )
    .await
    .unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn dropping_attached_mongo_stops_sockets_without_stopping_database() {
    let root = tempfile::tempdir().unwrap();
    let db = database(root.path()).await;
    let server = AttachedServer::start_with_mongo(&db, config(), "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = server.addresses().mongo().unwrap();
    drop(server);
    assert_closed(address).await;
    assert_eq!(db.state(), EngineState::Running);
    db.close().await.unwrap();
}
