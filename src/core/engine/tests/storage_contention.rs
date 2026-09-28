use super::*;
use crate::core::{ContentionJitter, ContentionPolicy};
use rusqlite::Connection;

fn indexed_engine(policy: ContentionPolicy) -> (tempfile::TempDir, Engine) {
    let root = tempfile::tempdir().unwrap();
    let mut database = Database::open(root.path(), 2).unwrap();
    database
        .broadcast(
            "CREATE TABLE indexed_items (tenant TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL)",
        )
        .unwrap();
    let logical = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "indexed_items",
                ShardKeyMetadata::new("tenant", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    let table = database
        .catalog()
        .table("default", "indexed_items")
        .unwrap()
        .unwrap()
        .id();
    let index = database
        .create_global_index(
            GlobalIndexDeclaration::new(
                table,
                "by_value",
                vec![GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::column("value").unwrap(),
                    GlobalIndexKeyType::Text,
                )],
            )
            .unwrap()
            .with_topology(crate::GlobalIndexStorageTopology::selected_v1()),
        )
        .unwrap();
    database.build_global_index(index).unwrap();
    let engine = Engine::from_database_with_options(
        Arc::new(database),
        EngineOptions::default().with_contention_policy(Some(policy)),
    )
    .unwrap();
    (root, engine)
}

fn exclusive_index(root: &std::path::Path) -> Connection {
    let connection = Connection::open(root.join("global-indexes/global.sqlite")).unwrap();
    connection
        .execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE")
        .unwrap();
    connection
}

fn backoff(retries: u32) -> ContentionPolicy {
    ContentionPolicy::new(
        Duration::from_millis(500),
        Duration::from_millis(500),
        1,
        ContentionJitter::None,
        retries,
        Duration::from_secs(10),
    )
    .unwrap()
}

async fn wait_retry(engine: &Engine, retries: u64) {
    timeout(Duration::from_secs(2), async {
        while engine.contention_statistics().retries_scheduled() < retries {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn metadata_planning_backoff_does_not_block_runtime_or_swallow_cancellation() {
    for bound in [false, true] {
        let (root, engine) = indexed_engine(backoff(10));
        let session = engine.session();
        let prepared = engine
            .prepare_statement(
                &session,
                PrepareRequest::new(
                    engine.catalog().default_database().id(),
                    sql::SqlDialect::Sqlite,
                    sql::SqlTranslationMode::StrictSqlite,
                    "SELECT tenant FROM indexed_items WHERE value = ?1",
                ),
            )
            .await
            .unwrap();
        let blocker = exclusive_index(root.path());
        let token = CancellationToken::new();
        let context = RequestContext::new().with_cancellation_token(token.clone());
        let worker = engine.clone();
        let call = tokio::spawn(async move {
            if bound {
                worker
                    .bind_statement_with_context(&session, prepared, vec!["one".into()], context)
                    .await
                    .map(|_| ())
            } else {
                worker
                    .query_logical_with_context(
                        &session,
                        Statement::new(
                            "SELECT tenant FROM indexed_items WHERE value = ?1",
                            vec!["one".into()],
                        ),
                        context,
                    )
                    .await
                    .map(|_| ())
            }
        });
        wait_retry(&engine, 1).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        token.cancel();
        assert_eq!(
            timeout(Duration::from_secs(2), call)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err()
                .kind(),
            EngineErrorKind::Cancelled
        );
        assert_eq!(engine.contention_statistics().exhausted_budgets(), 0);
        blocker.execute_batch("ROLLBACK").unwrap();
        drop(blocker);
        assert!(
            engine
                .query_logical(
                    &engine.session(),
                    Statement::new(
                        "SELECT tenant FROM indexed_items WHERE value = ?1",
                        vec!["one".into()]
                    )
                )
                .await
                .unwrap()
                .value
                .is_empty()
        );
    }
}

#[tokio::test]
async fn dropped_planning_future_retains_worker_lifecycle_and_schema_admission() {
    let (_root, engine) = engine_with_options(2, 1, 1);
    let worker = engine.clone();
    let (started, started_rx) = oneshot::channel();
    let (release, released) = mpsc::channel();
    let call = tokio::spawn(async move {
        let mut operation = worker.operation(RequestContext::new()).unwrap();
        let schema = worker
            .inner
            .database
            .storage
            .enter_schema_operation()
            .unwrap();
        worker
            .run_storage_planning(&mut operation, &schema, move |_, _, _| {
                started.send(()).unwrap();
                released.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(())
            })
            .await
    });
    timeout(Duration::from_secs(2), started_rx)
        .await
        .unwrap()
        .unwrap();
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    assert_eq!(engine.inner.lifecycle.active(), 1);
    assert_eq!(
        engine
            .inner
            .database
            .storage
            .schema_gate_snapshot()
            .active_operations,
        1
    );
    release.send(()).unwrap();
    timeout(Duration::from_secs(2), async {
        while engine.inner.lifecycle.active() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        engine
            .inner
            .database
            .storage
            .schema_gate_snapshot()
            .active_operations,
        0
    );
}

#[tokio::test]
async fn managed_index_worker_inherits_policy_and_stop_wakes_configured_backoff() {
    let policy = ContentionPolicy::new(
        Duration::from_secs(5),
        Duration::from_secs(5),
        1,
        ContentionJitter::None,
        10,
        Duration::from_secs(60),
    )
    .unwrap();
    let (root, engine) = indexed_engine(policy);
    let blocker = exclusive_index(root.path());
    let mut worker = engine
        .start_global_index_worker(crate::GlobalIndexAsyncOptions::new(64, 500, 5).unwrap())
        .unwrap();
    wait_retry(&engine, 1).await;
    let started = Instant::now();
    assert!(!worker.stop());
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(worker.is_finished());
    assert_eq!(engine.contention_statistics().exhausted_budgets(), 0);
    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
    engine.global_index_operational_report().await.unwrap();
}

#[cfg(feature = "experimental-vtab")]
fn hilo_engine(policy: ContentionPolicy) -> (tempfile::TempDir, Engine) {
    let (root, original) = engine_with_hilo_events(2);
    let engine = Engine::from_database_with_options(
        Arc::clone(&original.inner.database),
        original.options().with_contention_policy(Some(policy)),
    )
    .unwrap();
    (root, engine)
}

#[cfg(feature = "experimental-vtab")]
fn block_children(root: &std::path::Path) -> Vec<Connection> {
    (0..2)
        .map(|shard| {
            let connection =
                Connection::open(root.join(format!("shards/{shard:04}.sqlite"))).unwrap();
            connection.execute_batch("BEGIN IMMEDIATE").unwrap();
            connection
        })
        .collect()
}

#[cfg(feature = "experimental-vtab")]
fn release_children(blockers: Vec<Connection>) {
    for connection in blockers {
        connection.execute_batch("ROLLBACK").unwrap();
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM hilo_events", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
}

#[cfg(feature = "experimental-vtab")]
fn insert() -> Statement {
    Statement::new("INSERT INTO hilo_events (payload) VALUES ('once')", vec![])
}

#[cfg(feature = "experimental-vtab")]
#[tokio::test]
async fn vtab_children_fail_fast_without_legacy_five_second_retry_or_replay() {
    let (root, engine) = hilo_engine(ContentionPolicy::fail_fast());
    let blockers = block_children(root.path());
    let error = timeout(
        Duration::from_secs(2),
        engine.execute_write(&engine.session(), insert()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy, "{error}");
    assert_eq!(engine.contention_statistics().exhausted_budgets(), 1);
    assert_eq!(engine.contention_statistics().retries_scheduled(), 0);
    release_children(blockers);
    assert_eq!(
        engine
            .execute_write(&engine.session(), insert())
            .await
            .unwrap()
            .value
            .rows_affected,
        1
    );
}

#[cfg(feature = "experimental-vtab")]
#[tokio::test]
async fn vtab_manifest_and_child_share_one_retry_budget() {
    let (root, engine) = hilo_engine(backoff(1));
    let manifest = Connection::open(root.path().join("manifest.sqlite")).unwrap();
    manifest.execute_batch("BEGIN IMMEDIATE").unwrap();
    let blockers = block_children(root.path());
    let worker = engine.clone();
    let call = tokio::spawn(async move { worker.execute_write(&worker.session(), insert()).await });
    wait_retry(&engine, 1).await;
    manifest.execute_batch("ROLLBACK").unwrap();
    let error = timeout(Duration::from_secs(2), call)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy, "{error}");
    assert_eq!(engine.contention_statistics().retries_scheduled(), 1);
    assert_eq!(engine.contention_statistics().exhausted_budgets(), 1);
    release_children(blockers);
}

#[cfg(feature = "experimental-vtab")]
#[tokio::test]
async fn vtab_child_wait_cancels_on_single_thread_runtime_without_replay() {
    for deadline in [false, true] {
        let (root, engine) = hilo_engine(backoff(10));
        let blockers = block_children(root.path());
        let token = CancellationToken::new();
        let context = if deadline {
            RequestContext::new().with_deadline(Instant::now() + Duration::from_millis(300))
        } else {
            RequestContext::new().with_cancellation_token(token.clone())
        };
        let worker = engine.clone();
        let call = tokio::spawn(async move {
            worker
                .execute_write_with_context(&worker.session(), insert(), context)
                .await
        });
        wait_retry(&engine, 1).await;
        if !deadline {
            token.cancel();
        }
        let error = timeout(Duration::from_secs(2), call)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.kind(),
            if deadline {
                EngineErrorKind::DeadlineExceeded
            } else {
                EngineErrorKind::Cancelled
            },
            "{error}"
        );
        release_children(blockers);
        assert_eq!(
            engine
                .execute_write(&engine.session(), insert())
                .await
                .unwrap()
                .value
                .rows_affected,
            1
        );
    }
}

#[tokio::test]
async fn auxiliary_checkpoint_fail_fast_and_metadata_report_backoff_are_controlled() {
    let (root, engine) = engine_with_engine_options(
        2,
        EngineOptions::default().with_contention_policy(Some(ContentionPolicy::fail_fast())),
    );
    let manifest = Connection::open(root.path().join("manifest.sqlite")).unwrap();
    manifest
        .execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE")
        .unwrap();
    let error = timeout(Duration::from_secs(2), engine.checkpoint())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy, "{error}");
    manifest.execute_batch("ROLLBACK").unwrap();
    drop(manifest);
    engine.checkpoint().await.unwrap();
    engine.shutdown().await.unwrap();
    let engine = Engine::from_database_with_options(
        Arc::clone(&engine.inner.database),
        engine.options().with_contention_policy(Some(backoff(2))),
    )
    .unwrap();
    let blocker = Connection::open(root.path().join("shards/0000.sqlite")).unwrap();
    blocker
        .execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE")
        .unwrap();
    let worker = engine.clone();
    let call = tokio::spawn(async move { worker.global_index_operational_report().await });
    wait_retry(&engine, 1).await;
    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
    timeout(Duration::from_secs(2), call)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(engine.contention_statistics().retries_scheduled(), 1);
}
