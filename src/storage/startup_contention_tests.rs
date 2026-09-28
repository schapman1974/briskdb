//! Local admission tests, not cross-host NFS qualification.

use super::*;
use crate::core::{
    CancellationReason, ContentionJitter, ContentionMetrics, ContentionPolicy, Engine,
    EngineOptions,
};
use std::time::Duration;

fn policy(retries: u32) -> ContentionPolicy {
    ContentionPolicy::new(
        Duration::from_millis(500),
        Duration::from_millis(500),
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

fn wait_until(mut ready: impl FnMut() -> bool) {
    let until = Instant::now() + Duration::from_secs(2);
    while !ready() {
        assert!(
            Instant::now() < until,
            "startup test synchronization timed out"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[tokio::test]
async fn engine_fail_fast_stops_before_manifest_work() {
    let root = tempfile::tempdir().unwrap();
    let storage = Storage::open(root.path(), 2).unwrap();
    let before = fs::read(root.path().join("manifest.sqlite")).unwrap();
    let held = process_lock::RootStartupGuard::acquire(root.path(), Duration::ZERO).unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        Engine::open_with_options(
            root.path(),
            2,
            EngineOptions::default().with_contention_policy(Some(ContentionPolicy::fail_fast())),
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    assert_eq!(storage.schema_gate_snapshot().state, SchemaGateState::Ready);
    assert_eq!(
        fs::read(root.path().join("manifest.sqlite")).unwrap(),
        before
    );
    drop(held);
    let engine = Engine::open_with_options(
        root.path(),
        2,
        EngineOptions::default().with_contention_policy(Some(ContentionPolicy::fail_fast())),
    )
    .await
    .unwrap();
    assert_eq!(engine.contention_statistics().exhausted_budgets(), 0);
    engine.shutdown().await.unwrap();
}

#[test]
fn startup_lock_schema_gate_and_quiescence_share_one_budget() {
    let root = tempfile::tempdir().unwrap();
    let storage = Storage::open(root.path(), 2).unwrap();
    let active = storage
        .schema_coordination
        .gate
        .try_acquire_operation()
        .unwrap();
    let migrating = storage.schema_coordination.gate.begin_migration().unwrap();
    let held = process_lock::RootStartupGuard::acquire(root.path(), Duration::ZERO).unwrap();
    let before = fs::read(root.path().join("manifest.sqlite")).unwrap();
    let (control, metrics) = control(policy(2), None);
    let path = root.path().to_path_buf();
    let opener = std::thread::spawn(move || {
        Storage::open_with_startup_control(path, 2, None, Some(&control))
    });
    wait_until(|| metrics.snapshot().retries_scheduled() == 1);
    drop(held);
    wait_until(|| metrics.snapshot().retries_scheduled() == 2);
    drop(migrating);
    assert_eq!(
        opener.join().unwrap().unwrap_err().kind(),
        EngineErrorKind::Busy
    );
    assert_eq!(metrics.snapshot().retries_scheduled(), 2);
    assert_eq!(metrics.snapshot().exhausted_budgets(), 1);
    assert_eq!(storage.schema_gate_snapshot().state, SchemaGateState::Ready);
    assert_eq!(storage.schema_gate_snapshot().active_operations, 1);
    assert_eq!(
        fs::read(root.path().join("manifest.sqlite")).unwrap(),
        before
    );
    drop(active);
    Storage::open(root.path(), 2).unwrap();
}

#[test]
fn released_startup_lock_completes_without_replaying_initialization() {
    let root = tempfile::tempdir().unwrap();
    let held = process_lock::RootStartupGuard::acquire(root.path(), Duration::ZERO).unwrap();
    let (control, metrics) = control(policy(2), None);
    let path = root.path().to_path_buf();
    let opener = std::thread::spawn(move || {
        Storage::open_with_startup_control(path, 2, None, Some(&control))
    });
    wait_until(|| metrics.snapshot().retries_scheduled() == 1);
    assert!(!root.path().join("manifest.sqlite").exists());
    drop(held);
    let storage = opener.join().unwrap().unwrap();
    assert_eq!(metrics.snapshot().retries_scheduled(), 1);
    assert_eq!(metrics.snapshot().exhausted_budgets(), 0);
    assert_eq!(storage.schema_gate_snapshot().state, SchemaGateState::Ready);
    Storage::open(root.path(), 2).unwrap();
}

#[test]
fn cancellation_and_deadline_interrupt_startup_lock_backoff() {
    for cancelled in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let _held = process_lock::RootStartupGuard::acquire(root.path(), Duration::ZERO).unwrap();
        let (control, metrics) = control(
            policy(10),
            (!cancelled).then(|| Instant::now() + Duration::from_millis(20)),
        );
        let child_control = Arc::clone(&control);
        let path = root.path().to_path_buf();
        let opener = std::thread::spawn(move || {
            Storage::open_with_startup_control(path, 2, None, Some(&child_control))
        });
        if cancelled {
            wait_until(|| metrics.snapshot().retries_scheduled() == 1);
            assert!(control.request_cancel(CancellationReason::Cancelled));
        }
        assert_eq!(
            opener.join().unwrap().unwrap_err().kind(),
            if cancelled {
                EngineErrorKind::Cancelled
            } else {
                EngineErrorKind::DeadlineExceeded
            }
        );
        assert!(!root.path().join("manifest.sqlite").exists());
        assert_eq!(metrics.snapshot().exhausted_budgets(), 0);
    }
}

#[tokio::test]
async fn dropping_configured_open_releases_schema_and_root_admission() {
    let root = tempfile::tempdir().unwrap();
    let storage = Storage::open(root.path(), 2).unwrap();
    let active = storage
        .schema_coordination
        .gate
        .try_acquire_operation()
        .unwrap();
    let path = root.path().to_path_buf();
    let opener = tokio::spawn(Engine::open_with_options(
        path,
        2,
        EngineOptions::default().with_contention_policy(Some(policy(10))),
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        while storage.schema_gate_snapshot().state != SchemaGateState::Migrating {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    opener.abort();
    assert!(opener.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(2), async {
        while storage.schema_gate_snapshot().state != SchemaGateState::Ready {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        loop {
            match process_lock::RootStartupGuard::acquire(root.path(), Duration::ZERO) {
                Ok(guard) => break drop(guard),
                Err(error) => assert_eq!(error.kind(), EngineErrorKind::Busy),
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(storage.schema_gate_snapshot().active_operations, 1);
    drop(active);
    Storage::open(root.path(), 2).unwrap();
}

#[test]
fn cancelled_startup_does_not_create_a_root() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("absent");
    let (control, _) = control(policy(1), None);
    control.request_cancel(CancellationReason::Cancelled);
    assert_eq!(
        Storage::open_with_startup_control(&root, 2, None, Some(&control))
            .unwrap_err()
            .kind(),
        EngineErrorKind::Cancelled
    );
    assert!(!root.exists());
}

#[test]
fn exhausted_quiescence_restores_pending_not_ready() {
    let gate = schema_gate::SchemaGate::new();
    let active = gate.try_acquire_operation().unwrap();
    let mut first = gate.begin_migration().unwrap();
    first.mark_pending_on_drop();
    drop(first);
    let (control, metrics) = control(ContentionPolicy::fail_fast(), None);
    let pending = gate.begin_migration().unwrap();
    assert_eq!(
        pending
            .wait_for_quiescence_controlled(&control)
            .unwrap_err()
            .kind(),
        EngineErrorKind::Busy
    );
    drop(pending);
    assert_eq!(gate.snapshot().state, SchemaGateState::Pending);
    assert_eq!(metrics.snapshot().exhausted_budgets(), 1);
    drop(active);
}
