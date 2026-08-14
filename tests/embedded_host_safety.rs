use std::time::Duration;

use briskdb::{
    BriskDb, BriskSession, EngineErrorKind, EngineOptions, EngineState, SessionState, Statement,
    Value,
};

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

async fn open(path: &std::path::Path) -> BriskDb {
    BriskDb::builder(path)
        .with_shard_count(2)
        .with_engine_options(EngineOptions::new(1, 2).unwrap())
        .open()
        .await
        .unwrap()
}

#[test]
fn owned_session_is_send_sync_cloneable_and_runtime_teardown_safe() {
    assert_send_sync_static::<BriskSession>();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().to_path_buf();

    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let database = open(&path).await;
            let session = database.owned_session();
            session.set_routing_key("runtime-owner").await.unwrap();
            session
                .migrate("CREATE TABLE events (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
                .await
                .unwrap();
            session.close().await.unwrap();
            database.close().await.unwrap();
        });
    })
    .join()
    .unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let reopened = open(temp.path()).await;
        assert_eq!(reopened.state(), EngineState::Running);
        reopened.close().await.unwrap();
    });
}

#[tokio::test]
async fn clones_share_terminal_session_state_and_cannot_resurrect_database() {
    let temp = tempfile::tempdir().unwrap();
    let database = open(temp.path()).await;
    let session = database.owned_session();
    let clone = session.clone();
    assert_eq!(session.id(), clone.id());
    session.set_routing_key("shared-session").await.unwrap();
    assert_eq!(clone.routing_key().await.as_deref(), Some("shared-session"));

    clone.close().await.unwrap();
    assert_eq!(session.state().await, SessionState::Closed);
    let closed = session
        .query(Statement::new("SELECT 1", Vec::new()))
        .await
        .unwrap_err();
    assert_eq!(closed.kind(), EngineErrorKind::FailedPrecondition);

    let leaked = database.owned_session();
    leaked.set_routing_key("leaked").await.unwrap();
    database.close().await.unwrap();
    drop(database);
    assert_eq!(leaked.database_state(), EngineState::Stopped);
    let stopped = leaked
        .query(Statement::new("SELECT 1", Vec::new()))
        .await
        .unwrap_err();
    assert_eq!(stopped.kind(), EngineErrorKind::ShuttingDown);
    leaked.close().await.unwrap();
}

#[tokio::test]
async fn shared_session_concurrency_serializes_and_concurrent_close_is_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    let database = open(temp.path()).await;
    let session = database.owned_session();
    session.set_routing_key("concurrent-owner").await.unwrap();
    session
        .migrate("CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
        .await
        .unwrap();
    session
        .execute_write(Statement::new(
            "INSERT INTO counters (id, value) VALUES (?1, ?2)",
            vec![Value::from(1_i64), Value::from(0_i64)],
        ))
        .await
        .unwrap();

    let mut work = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let session = session.clone();
        work.spawn(async move {
            session
                .execute_write(Statement::new(
                    "UPDATE counters SET value = value + 1 WHERE id = ?1",
                    vec![Value::from(1_i64)],
                ))
                .await
        });
    }
    while let Some(result) = work.join_next().await {
        assert_eq!(result.unwrap().unwrap().value.rows_affected, 1);
    }
    let rows = session
        .query(Statement::new(
            "SELECT value FROM counters WHERE id = ?1",
            vec![Value::from(1_i64)],
        ))
        .await
        .unwrap();
    assert_eq!(rows.value.rows()[0].get(0), Some(&Value::from(16_i64)));

    session.close().await.unwrap();
    let first = database.clone();
    let second = database.clone();
    let (first, second) = tokio::join!(first.close(), second.close());
    assert_eq!(first.unwrap(), second.unwrap());
    assert_eq!(database.state(), EngineState::Stopped);
}

#[tokio::test]
async fn request_cancellation_reaches_owned_session_commands() {
    let temp = tempfile::tempdir().unwrap();
    let database = open(temp.path()).await;
    let session = database.owned_session();
    session.set_routing_key("cancel-owner").await.unwrap();
    let cancellation = briskdb::CancellationToken::new();
    cancellation.cancel();
    let error = session
        .query_with_context(
            Statement::new("SELECT 1", Vec::new()),
            briskdb::RequestContext::new().with_cancellation_token(cancellation),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Cancelled);

    assert_eq!(
        session.status().await.unwrap().queue_capacity_per_shard(),
        2
    );
    session.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), database.close())
        .await
        .unwrap()
        .unwrap();
}
