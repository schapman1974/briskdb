use super::*;
use crate::core::{ContentionJitter, ContentionMetrics, ContentionPolicy};
use std::sync::atomic::{AtomicUsize, Ordering};

fn policy(retries: u32, delay: Duration) -> ContentionPolicy {
    ContentionPolicy::new(
        delay,
        delay,
        1,
        ContentionJitter::None,
        retries,
        Duration::from_secs(5),
    )
    .unwrap()
}

fn control(
    policy: ContentionPolicy,
    deadline: Option<Instant>,
) -> (Arc<OperationControl>, Arc<ContentionMetrics>) {
    let metrics = Arc::new(ContentionMetrics::default());
    (
        OperationControl::with_contention_metrics(deadline, Some(policy), Arc::clone(&metrics)),
        metrics,
    )
}

fn connections(path: &Path, mode: &str) -> (Connection, Connection) {
    let owner = Connection::open(path).unwrap();
    owner.pragma_update(None, "journal_mode", mode).unwrap();
    owner
        .execute_batch("CREATE TABLE existing(value INTEGER)")
        .unwrap();
    let contender = Connection::open(path).unwrap();
    contender.pragma_update(None, "journal_mode", mode).unwrap();
    (owner, contender)
}

fn assert_clean(connection: &Connection) {
    assert!(connection.is_autocommit());
    assert_eq!(
        connection
            .pragma_query_value(None, "busy_timeout", |row| row.get::<_, i64>(0))
            .unwrap(),
        CONNECTION_BUSY_TIMEOUT.as_millis() as i64
    );
    assert!(MIGRATION_BUSY_OPERATION.with(|operation| operation.borrow().is_none()));
}

#[test]
fn fail_fast_migration_busy_never_replays_work_or_changes_schema() {
    // These are local journal-mode tests, not NFS qualification.
    for mode in ["WAL", "DELETE", "PERSIST"] {
        let root = tempfile::tempdir().unwrap();
        let (owner, mut contender) = connections(&root.path().join("test.sqlite"), mode);
        owner.execute_batch("BEGIN IMMEDIATE").unwrap();
        let (control, metrics) = control(ContentionPolicy::fail_fast(), None);
        let calls = AtomicUsize::new(0);
        let error = run_connection_controlled(&mut contender, control, |connection| {
            calls.fetch_add(1, Ordering::SeqCst);
            connection
                .execute_batch("CREATE TABLE must_not_exist(value INTEGER)")
                .map_err(sqlite_error::storage)
        })
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Busy, "{mode}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.snapshot().retries_scheduled(), 0);
        assert_eq!(metrics.snapshot().exhausted_budgets(), 1, "{mode}");
        assert_clean(&contender);
        owner.execute_batch("ROLLBACK").unwrap();
        assert!(
            !contender
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='must_not_exist')",
                    [],
                    |row| row.get::<_, bool>(0)
                )
                .unwrap()
        );
    }
}

#[test]
fn sequential_migration_handles_share_one_retry_budget_and_uncontended_work_still_runs() {
    let root = tempfile::tempdir().unwrap();
    let (first_owner, mut first) = connections(&root.path().join("first.sqlite"), "DELETE");
    let (second_owner, mut second) = connections(&root.path().join("second.sqlite"), "DELETE");
    first_owner.execute_batch("BEGIN IMMEDIATE").unwrap();
    second_owner.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (control, metrics) = control(policy(2, Duration::from_millis(1)), None);
    let calls = AtomicUsize::new(0);
    for connection in [&mut first, &mut second] {
        let error = run_connection_controlled(connection, Arc::clone(&control), |connection| {
            calls.fetch_add(1, Ordering::SeqCst);
            connection
                .execute("INSERT INTO existing VALUES (1)", [])
                .map_err(sqlite_error::storage)
        })
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Busy);
        assert_eq!(metrics.snapshot().retries_scheduled(), 2);
        assert_eq!(metrics.snapshot().exhausted_budgets(), 1);
        assert_clean(connection);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    first_owner.execute_batch("ROLLBACK").unwrap();
    run_connection_controlled(&mut first, control, |connection| {
        calls.fetch_add(1, Ordering::SeqCst);
        connection
            .execute("INSERT INTO existing VALUES (1)", [])
            .map_err(sqlite_error::storage)
    })
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        first
            .query_row("SELECT COUNT(*) FROM existing", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(metrics.snapshot().retries_scheduled(), 2);
    assert_eq!(metrics.snapshot().exhausted_budgets(), 1);
    second_owner.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn a_released_migration_lock_completes_within_one_window_without_replaying_work() {
    let root = tempfile::tempdir().unwrap();
    let (owner, mut contender) = connections(&root.path().join("test.sqlite"), "DELETE");
    owner.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (control, metrics) = control(policy(2, Duration::from_millis(500)), None);
    let observed = Arc::clone(&metrics);
    let holder = std::thread::spawn(move || {
        let until = Instant::now() + Duration::from_secs(2);
        while observed.snapshot().retries_scheduled() == 0 && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(1));
        }
        owner.execute_batch("ROLLBACK").unwrap();
        assert_eq!(observed.snapshot().retries_scheduled(), 1);
    });
    let calls = AtomicUsize::new(0);
    run_connection_controlled(&mut contender, control, |connection| {
        calls.fetch_add(1, Ordering::SeqCst);
        connection
            .execute("INSERT INTO existing VALUES (1)", [])
            .map_err(sqlite_error::storage)
    })
    .unwrap();
    holder.join().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.snapshot().retries_scheduled(), 1);
    assert_eq!(metrics.snapshot().exhausted_budgets(), 0);
    assert_clean(&contender);
}

#[tokio::test]
async fn engine_broadcast_uses_configured_policy_before_durable_migration_work() {
    use crate::core::{Engine, EngineOptions, RequestContext};

    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open_with_options(
        root.path(),
        2,
        EngineOptions::default().with_contention_policy(Some(ContentionPolicy::fail_fast())),
    )
    .await
    .unwrap();
    let session = engine.session();
    let blocker = Connection::open(root.path().join("manifest.sqlite")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let sql = "CREATE TABLE policy_wired(value INTEGER)";
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        engine.broadcast_with_context(&session, sql.to_owned(), RequestContext::new()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    assert_eq!(engine.contention_statistics().exhausted_budgets(), 1);
    assert_eq!(engine.contention_statistics().retries_scheduled(), 0);
    blocker.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        blocker
            .query_row(
                "SELECT COUNT(*) FROM briskdb_schema_migrations",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    assert_eq!(
        engine.broadcast(&session, sql.to_owned()).await.unwrap(),
        [0, 1]
    );
    engine.shutdown().await.unwrap();
}

#[test]
fn cancellation_and_deadline_stop_migration_backoff_without_schema_mutation() {
    for cancelled in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let (owner, mut contender) = connections(&root.path().join("test.sqlite"), "DELETE");
        owner.execute_batch("BEGIN IMMEDIATE").unwrap();
        let deadline = (!cancelled).then(|| Instant::now() + Duration::from_millis(20));
        let (control, metrics) = control(policy(10, Duration::from_millis(500)), deadline);
        let canceller = cancelled.then(|| {
            let control = Arc::clone(&control);
            let metrics = Arc::clone(&metrics);
            std::thread::spawn(move || {
                let until = Instant::now() + Duration::from_secs(2);
                while metrics.snapshot().retries_scheduled() == 0 && Instant::now() < until {
                    std::thread::sleep(Duration::from_millis(1));
                }
                control.request_cancel(CancellationReason::Cancelled);
                assert_eq!(metrics.snapshot().retries_scheduled(), 1);
            })
        });
        let error = run_connection_controlled(&mut contender, control, |connection| {
            connection
                .execute_batch("CREATE TABLE must_not_exist(value INTEGER)")
                .map_err(sqlite_error::storage)
        })
        .unwrap_err();
        if let Some(canceller) = canceller {
            canceller.join().unwrap();
        }
        assert_eq!(
            error.kind(),
            if cancelled {
                EngineErrorKind::Cancelled
            } else {
                EngineErrorKind::DeadlineExceeded
            }
        );
        assert_eq!(metrics.snapshot().exhausted_budgets(), 0);
        assert_clean(&contender);
        owner.execute_batch("ROLLBACK").unwrap();
        assert!(
            !contender
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='must_not_exist')",
                    [],
                    |row| row.get::<_, bool>(0)
                )
                .unwrap()
        );
    }
}

#[test]
fn migration_controls_cleanup_after_panic_and_preserve_known_commit() {
    let mut connection = Connection::open_in_memory().unwrap();
    let (cancelled, metrics) = control(ContentionPolicy::fail_fast(), None);
    let observed = Arc::clone(&cancelled);
    let result = run_connection_controlled(&mut connection, cancelled, |connection| {
        connection
            .execute_batch(
                "CREATE TABLE committed(value INTEGER); INSERT INTO committed VALUES (7)",
            )
            .map_err(sqlite_error::storage)?;
        observed.request_cancel(CancellationReason::Cancelled);
        Ok(7)
    })
    .unwrap();
    assert_eq!(result, 7);
    assert_clean(&connection);
    assert_eq!(metrics.snapshot().exhausted_budgets(), 0);
    let (control, _) = control(ContentionPolicy::fail_fast(), None);
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _: EngineResult<()> = run_connection_controlled(&mut connection, control, |_| {
            panic!("migration request cleanup test");
        });
    }));
    assert!(panic.is_err());
    assert_clean(&connection);
    // A cancelled request's progress hook cannot interrupt later use.
    assert_eq!(
        connection
            .query_row(
                "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<2000)
         SELECT (SELECT value FROM committed) + MAX(x) FROM n",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        2007
    );
}

#[test]
fn contention_after_one_shard_commit_retains_progress_and_explicit_resume_does_not_replay() {
    const SQL: &str =
        "CREATE TABLE migration_once(value INTEGER); INSERT INTO migration_once VALUES (7)";
    let root = tempfile::tempdir().unwrap();
    let storage = Storage::open(root.path(), 2).unwrap();
    let blocker = Connection::open(root.path().join("shards/0001.sqlite")).unwrap();
    let (control, metrics) = control(ContentionPolicy::fail_fast(), None);
    let mut commits = [0, 0];
    let mut guard = storage.begin_schema_migration().unwrap();
    guard.wait_for_quiescence_blocking();
    let error =
        apply_schema_migration_with_hook(&storage, SQL, &mut guard, Some(control), |point| {
            if let SchemaMigrationCoordinatorPoint::ShardCommitted(shard) = point {
                commits[usize::from(shard)] += 1;
                if shard == 0 {
                    blocker
                        .execute_batch("BEGIN IMMEDIATE")
                        .map_err(sqlite_error::storage)?;
                }
            }
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    assert_eq!(metrics.snapshot().exhausted_budgets(), 1);
    assert_eq!(commits, [1, 0]);
    drop(guard);
    assert_eq!(
        storage.schema_gate_snapshot().state,
        super::super::SchemaGateState::Pending
    );
    blocker.execute_batch("ROLLBACK").unwrap();
    let (control, _) = self::control(ContentionPolicy::fail_fast(), None);
    let mut guard = storage.begin_schema_migration().unwrap();
    guard.wait_for_quiescence_blocking();
    assert_eq!(
        apply_schema_migration_with_hook(&storage, SQL, &mut guard, Some(control), |point| {
            if let SchemaMigrationCoordinatorPoint::ShardCommitted(shard) = point {
                commits[usize::from(shard)] += 1;
            }
            Ok(())
        })
        .unwrap(),
        [0, 1]
    );
    guard.publish_ready().unwrap();
    assert_eq!(commits, [1, 1]);
    assert_eq!(storage.current_schema_generation(), 1);
    for shard in 0..2 {
        assert_eq!(
            storage
                .open_shard(shard)
                .unwrap()
                .query_row(
                    "SELECT COUNT(*), MIN(value) FROM migration_once",
                    [],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                )
                .unwrap(),
            (1, 7)
        );
    }
}

#[test]
fn configured_startup_recovery_preserves_partial_progress_when_a_shard_is_busy() {
    const SQL: &str =
        "CREATE TABLE startup_once(value INTEGER); INSERT INTO startup_once VALUES (7)";
    let root = tempfile::tempdir().unwrap();
    let storage = Storage::open(root.path(), 2).unwrap();
    let blocker = Connection::open(root.path().join("shards/0001.sqlite")).unwrap();
    let (migration_control, _) = control(ContentionPolicy::fail_fast(), None);
    let mut guard = storage.begin_schema_migration().unwrap();
    guard.wait_for_quiescence_blocking();
    let error = apply_schema_migration_with_hook(
        &storage,
        SQL,
        &mut guard,
        Some(migration_control),
        |point| {
            if matches!(point, SchemaMigrationCoordinatorPoint::ShardCommitted(0)) {
                blocker
                    .execute_batch("BEGIN IMMEDIATE")
                    .map_err(sqlite_error::storage)?;
            }
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    drop(guard);
    drop(storage);
    let (startup_control, metrics) = control(ContentionPolicy::fail_fast(), None);
    let error = Storage::open_with_startup_control(root.path(), 2, None, Some(&startup_control))
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    assert_eq!(metrics.snapshot().exhausted_budgets(), 1);
    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
    let (startup_control, _) = control(ContentionPolicy::fail_fast(), None);
    let recovered =
        Storage::open_with_startup_control(root.path(), 2, None, Some(&startup_control)).unwrap();
    assert_eq!(recovered.current_schema_generation(), 1);
    for shard in 0..2 {
        assert_eq!(
            recovered
                .open_shard(shard)
                .unwrap()
                .query_row("SELECT COUNT(*), MIN(value) FROM startup_once", [], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                })
                .unwrap(),
            (1, 7)
        );
    }
}
