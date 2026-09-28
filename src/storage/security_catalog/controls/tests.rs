use super::*;
use crate::core::{CancellationReason, ContentionJitter, ContentionMetrics, ContentionPolicy};
use std::sync::atomic::{AtomicUsize, Ordering};

fn control(policy: Option<ContentionPolicy>) -> (Arc<OperationControl>, Arc<ContentionMetrics>) {
    let metrics = Arc::new(ContentionMetrics::default());
    (
        OperationControl::with_contention_metrics(None, policy, Arc::clone(&metrics)),
        metrics,
    )
}

fn assert_clean(connection: &Connection) {
    assert!(connection.is_autocommit());
    assert_eq!(
        connection
            .pragma_query_value(None, "busy_timeout", |row| row.get::<_, i64>(0))
            .unwrap(),
        2000
    );
    assert!(AUTHORITY_CONTROL.with(|slot| slot.borrow().is_none()));
}

#[test]
fn sequential_security_connections_share_one_budget_and_never_replay_work() {
    let root = tempfile::tempdir().unwrap();
    let mut connections = Vec::new();
    for name in ["first", "second"] {
        let path = root.path().join(name);
        let owner = Connection::open(&path).unwrap();
        owner
            .execute_batch("CREATE TABLE items(value INTEGER); BEGIN IMMEDIATE")
            .unwrap();
        connections.push((owner, Connection::open(&path).unwrap()));
    }
    let policy = ContentionPolicy::new(
        Duration::from_millis(1),
        Duration::from_millis(1),
        1,
        ContentionJitter::None,
        2,
        Duration::from_secs(1),
    )
    .unwrap();
    let (control, metrics) = control(Some(policy));
    let calls = AtomicUsize::new(0);
    for (_, connection) in &mut connections {
        let error = with_operation_control(Arc::clone(&control), || {
            with_connection(connection, |connection| {
                calls.fetch_add(1, Ordering::SeqCst);
                connection
                    .execute("INSERT INTO items VALUES (1)", [])
                    .map_err(storage_error)
            })
        })
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Busy);
        assert_eq!(metrics.snapshot().retries_scheduled(), 2);
        assert_eq!(metrics.snapshot().exhausted_budgets(), 1);
        assert_clean(connection);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let (owner, connection) = &mut connections[0];
    owner.execute_batch("ROLLBACK").unwrap();
    with_operation_control(control, || {
        with_connection(connection, |connection| {
            connection
                .execute("INSERT INTO items VALUES (1)", [])
                .map_err(storage_error)
        })
    })
    .unwrap();
    assert_clean(connection);
}

#[test]
fn panic_restores_busy_policy_tls_and_rolls_back_open_transaction() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE items(value INTEGER)")
        .unwrap();
    let (control, _) = control(Some(ContentionPolicy::fail_fast()));
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        with_operation_control(control, || {
            with_connection(&mut connection, |connection| -> EngineResult<()> {
                let transaction = connection.transaction().unwrap();
                transaction
                    .execute("INSERT INTO items VALUES (1)", [])
                    .unwrap();
                panic!("intentional security cleanup test");
            })
        })
    }));
    assert!(outcome.is_err());
    assert_clean(&connection);
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM items", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn known_commit_survives_late_cancel_and_corruption_is_not_masked() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE items(value INTEGER)")
        .unwrap();
    let (control, _) = control(Some(ContentionPolicy::fail_fast()));
    let observed = Arc::clone(&control);
    assert_eq!(
        with_operation_control(Arc::clone(&control), || with_connection(
            &mut connection,
            |connection| {
                let changed = connection
                    .execute("INSERT INTO items VALUES (1)", [])
                    .map_err(storage_error)?;
                observed.request_cancel(CancellationReason::Cancelled);
                Ok(changed)
            }
        ))
        .unwrap(),
        1
    );
    assert_clean(&connection);
    let error = with_operation_control(control, || {
        with_connection(&mut connection, |_| {
            Err::<(), _>(failure(EngineErrorKind::DataCorruption, "test corruption"))
        })
    })
    .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
    assert_clean(&connection);
}

#[test]
fn legacy_calls_preserve_native_timeout_and_nested_scopes_restore_the_parent() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection.busy_timeout(SECURITY_BUSY_TIMEOUT).unwrap();
    let (parent, _) = control(Some(ContentionPolicy::fail_fast()));
    let (legacy, _) = control(None);
    with_operation_control(Arc::clone(&parent), || {
        with_operation_control(legacy, || {
            with_connection(&mut connection, |connection| {
                assert_clean(connection);
                Ok(())
            })
        })?;
        assert!(
            AUTHORITY_CONTROL.with(|slot| Arc::ptr_eq(slot.borrow().as_ref().unwrap(), &parent))
        );
        Ok(())
    })
    .unwrap();
    assert_clean(&connection);
}
