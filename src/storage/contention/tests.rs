use super::*;
use crate::core::{
    CancellationReason, ContentionJitter, ContentionMetrics, ContentionPolicy, EngineError,
};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    time::Instant,
};

fn policy() -> ContentionPolicy {
    ContentionPolicy::new(
        Duration::from_millis(1),
        Duration::from_millis(1),
        1,
        ContentionJitter::None,
        2,
        Duration::from_secs(10),
    )
    .unwrap()
}

#[test]
fn sequential_handles_share_exact_budget_and_work_is_never_replayed() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("busy.sqlite");
    let blocker = Connection::open(&path).unwrap();
    blocker
        .execute_batch("CREATE TABLE t (id INTEGER); BEGIN EXCLUSIVE")
        .unwrap();
    let metrics = Arc::new(ContentionMetrics::default());
    let control =
        OperationControl::with_contention_metrics(None, Some(policy()), Arc::clone(&metrics));
    let mut calls = 0;
    with_control(Some(Arc::clone(&control)), || {
        // A previous layer spent one of this command's two retries.
        assert_eq!(control.wait_for_contention(None), Some(true));
        for _ in 0..2 {
            let connection = Connection::open(&path).unwrap();
            configure(&connection, Duration::from_secs(5))?;
            calls += 1;
            let error = connection
                .execute("INSERT INTO t VALUES (1)", [])
                .map_err(sqlite_error::storage)
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Busy);
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(calls, 2);
    assert_eq!(metrics.snapshot().retries_scheduled(), 2);
    assert_eq!(metrics.snapshot().exhausted_budgets(), 1);
    blocker.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        blocker
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert!(!is_configured());
}

#[test]
fn nested_scopes_and_panics_restore_legacy_native_timeout() {
    let control =
        OperationControl::with_contention_policy(None, Some(ContentionPolicy::fail_fast()));
    with_control(Some(Arc::clone(&control)), || {
        assert!(is_configured());
        let panic = catch_unwind(AssertUnwindSafe(|| {
            with_control(None, || -> EngineResult<()> {
                assert!(!is_configured());
                let connection = Connection::open_in_memory().unwrap();
                configure(&connection, Duration::from_secs(5)).unwrap();
                assert_eq!(
                    connection
                        .pragma_query_value(None, "busy_timeout", |r| r.get::<_, i64>(0))
                        .unwrap(),
                    5000
                );
                panic!("test scope cleanup");
            })
        }));
        assert!(panic.is_err());
        assert!(Arc::ptr_eq(&current().unwrap(), &control));
        Ok(())
    })
    .unwrap();
    assert!(!is_configured());
}

#[test]
fn cancellation_maps_only_busy_not_known_commit_or_corruption() {
    for deadline in [false, true] {
        let control =
            OperationControl::with_contention_policy(deadline.then(Instant::now), Some(policy()));
        if !deadline {
            control.request_cancel(CancellationReason::Cancelled);
        }
        let error = with_control(Some(Arc::clone(&control)), || {
            assert!(!busy_handler(0));
            Err::<(), _>(EngineError::new(EngineErrorKind::Busy, "busy"))
        })
        .unwrap_err();
        assert_eq!(
            error.kind(),
            if deadline {
                EngineErrorKind::DeadlineExceeded
            } else {
                EngineErrorKind::Cancelled
            }
        );
        assert_eq!(
            with_control(Some(Arc::clone(&control)), || Ok(7)).unwrap(),
            7
        );
        assert_eq!(
            with_control(Some(control), || Err::<(), _>(EngineError::new(
                EngineErrorKind::DataCorruption,
                "corrupt"
            )))
            .unwrap_err()
            .kind(),
            EngineErrorKind::DataCorruption
        );
    }
}
