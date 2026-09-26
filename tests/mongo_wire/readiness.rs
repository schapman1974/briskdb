use super::*;
use briskdb::{
    EngineErrorKind,
    core::SchemaState,
    protocol::mongo::{MongoListenerState, MongoReadinessReason, MongoSecurityMode},
};

#[tokio::test]
async fn readiness_tracks_listener_engine_and_release_without_owning_the_database() {
    let (root, database, mut server) = setup().await;
    let mut other = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let ready = server.readiness();
    assert!(ready.ready());
    assert_eq!(ready.listener, MongoListenerState::Running);
    assert_eq!(ready.security, MongoSecurityMode::AnonymousLoopback);
    assert_eq!(ready.engine.unwrap(), database.engine().readiness());
    server.begin_close();
    assert_eq!(
        server.readiness().reason(),
        Some(MongoReadinessReason::ListenerClosing)
    );
    server.close().await.unwrap();
    assert_eq!(
        server.readiness().reason(),
        Some(MongoReadinessReason::ListenerClosed)
    );
    assert!(other.readiness().ready());
    assert!(database.engine().readiness().ready());
    // A closed listener cannot become ready again or shut down another listener.
    server.begin_close();
    assert_eq!(server.readiness().listener, MongoListenerState::Closed);
    database.begin_close();
    let draining = other.readiness();
    assert!(!draining.ready());
    assert_eq!(
        draining.engine.unwrap().lifecycle_state(),
        EngineState::Draining
    );
    other.close().await.unwrap();
    database.close().await.unwrap();
    assert_eq!(
        other.readiness().engine.unwrap().lifecycle_state(),
        EngineState::Stopped
    );
    drop(database);
    assert!(server.readiness().engine.is_none());
    assert!(other.readiness().engine.is_none());
    // Both closed host handles remain alive across opening a new engine identity.
    let reopened = BriskDb::builder(root.path())
        .with_document_support(DocumentSupport::Enabled)
        .open()
        .await
        .unwrap();
    let mut fresh = MongoServer::start(&reopened, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    assert!(fresh.readiness().ready());
    assert!(server.readiness().engine.is_none());
    fresh.close().await.unwrap();
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn discovery_can_work_while_document_readiness_explicitly_reports_disabled() {
    let root = tempfile::tempdir().unwrap();
    let database = BriskDb::builder(root.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let mut server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    assert_eq!(
        server.readiness().reason(),
        Some(MongoReadinessReason::DocumentsDisabled)
    );
    assert!(!server.readiness().ready());
    assert_eq!(server.readiness().security.code(), "anonymous_loopback");
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let reply = send_command(&mut stream, &command("ping")).await;
    assert!(matches!(reply.get_first("ok"), Some(BsonValue::Double(value)) if *value == 1.0));
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn readiness_reports_detected_catalog_and_shard_failures_without_leaking_details() {
    for catalog in [true, false] {
        let (root, database, mut server) = setup().await;
        let target = if catalog {
            "manifest.sqlite"
        } else {
            "shards/0001.sqlite"
        };
        // Inject corruption only in this test-owned disposable database.
        if catalog {
            std::fs::remove_file(root.path().join(target)).unwrap();
        } else {
            let shard = rusqlite::Connection::open(root.path().join(target)).unwrap();
            shard
                .execute(
                    "UPDATE briskdb_shard_metadata SET shard_id = 0 WHERE singleton = 1",
                    [],
                )
                .unwrap();
        }
        // Observation does not secretly perform filesystem validation.
        assert!(server.readiness().ready());
        let error = if catalog {
            database.engine().migration_summary().await.unwrap_err()
        } else {
            database.engine().shard_status().await.unwrap_err()
        };
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        let status = server.readiness();
        assert!(!status.ready());
        assert_eq!(status.reason(), Some(MongoReadinessReason::SchemaDegraded));
        assert_eq!(status.engine.unwrap().schema_state(), SchemaState::Degraded);
        let formatted = format!("{status:?}");
        assert!(!formatted.contains(root.path().to_str().unwrap()));
        assert!(!formatted.contains(target));
        server.close().await.unwrap();
        database.close().await.unwrap();
    }
}
