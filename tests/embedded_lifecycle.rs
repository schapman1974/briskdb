use std::time::Duration;

use briskdb::{
    BriskDb, CancellationToken, CheckpointDatabase, EngineErrorKind, EngineState, Statement, Value,
};

async fn open_two_shards(path: &std::path::Path) -> BriskDb {
    BriskDb::builder(path)
        .with_shard_count(2)
        .open()
        .await
        .unwrap()
}

#[test]
fn embedded_module_has_no_process_or_listener_assembly_dependencies() {
    let source = include_str!("../src/embedded.rs");
    for forbidden in [
        "tokio::net",
        "tokio::signal",
        "tracing_subscriber",
        "std::process",
        "crate::server",
    ] {
        assert!(
            !source.contains(forbidden),
            "embedded lifecycle must not depend on {forbidden}"
        );
    }

    let server = include_str!("../src/server/mod.rs");
    assert!(server.contains("BriskDb::builder"));
}

#[tokio::test]
async fn host_cancellation_and_grace_control_explicit_shutdown() {
    let temp = tempfile::tempdir().unwrap();
    let database = open_two_shards(temp.path()).await;

    let invalid = database.close_with_grace(Duration::ZERO).await.unwrap_err();
    assert_eq!(invalid.kind(), EngineErrorKind::InvalidArgument);
    assert_eq!(database.state(), EngineState::Running);

    let cancellation = CancellationToken::new();
    let waiter_database = database.clone();
    let waiter_token = cancellation.clone();
    let mut waiter =
        tokio::spawn(async move { waiter_database.close_when_cancelled(waiter_token).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiter)
            .await
            .is_err()
    );
    assert_eq!(database.state(), EngineState::Running);

    assert!(cancellation.cancel());
    let report = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!report.forced());
    assert_eq!(database.state(), EngineState::Stopped);
}

#[tokio::test]
async fn passive_checkpoint_is_ordered_bounded_and_host_cancellable() {
    let temp = tempfile::tempdir().unwrap();
    let database = open_two_shards(temp.path()).await;
    let session = database.session();
    session.set_routing_key("checkpoint-owner").await.unwrap();
    database
        .migrate(
            &session,
            "CREATE TABLE events (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
        )
        .await
        .unwrap();
    database
        .execute_write(
            &session,
            Statement::new(
                "INSERT INTO events (id, body) VALUES (?1, ?2)",
                vec![Value::from(1_i64), Value::from("checkpoint")],
            ),
        )
        .await
        .unwrap();

    let report = database.checkpoint().await.unwrap();
    assert_eq!(
        report
            .shards()
            .iter()
            .map(|report| report.shard())
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    for shard in report.shards() {
        assert!(shard.counts_available());
        assert!(shard.checkpointed_frames() <= shard.wal_frames());
        assert_eq!(
            shard.complete(),
            shard.checkpointed_frames() == shard.wal_frames()
        );
    }
    assert_eq!(report.databases().len(), 1);
    assert_eq!(
        report.databases()[0].database(),
        CheckpointDatabase::Manifest
    );
    assert!(report.databases()[0].counts_available());
    assert_eq!(
        report.complete(),
        report.shards().iter().all(|s| s.complete())
            && report
                .databases()
                .iter()
                .all(|database| database.complete())
    );

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let error = database
        .checkpoint_with_context(briskdb::RequestContext::new().with_cancellation_token(cancelled))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Cancelled);

    session.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn repeated_cold_warm_drop_and_close_cycles_remain_reopenable() {
    let temp = tempfile::tempdir().unwrap();

    let dropped = open_two_shards(temp.path()).await;
    drop(dropped);

    for _ in 0..8 {
        let database = open_two_shards(temp.path()).await;
        assert_eq!(database.state(), EngineState::Running);
        assert_eq!(database.shard_count(), 2);
        assert!(!database.close().await.unwrap().forced());
        assert_eq!(database.state(), EngineState::Stopped);
    }
}
