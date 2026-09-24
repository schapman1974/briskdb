use super::*;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::sync::Barrier;

#[derive(Default)]
struct Probe {
    started: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
    closed: AtomicUsize,
}

struct Active(Arc<Probe>);
impl Probe {
    fn enter(self: &Arc<Self>) -> Active {
        self.started.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        Active(Arc::clone(self))
    }
}
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.closed.fetch_add(1, Ordering::SeqCst);
    }
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .unwrap()
}

#[tokio::test]
async fn frontier_coordinator_runs_eight_children_and_preserves_physical_identities() {
    let probe = Arc::new(Probe::default());
    let gate = Arc::new(Barrier::new(9));
    let work_probe = Arc::clone(&probe);
    let work_gate = Arc::clone(&gate);
    let parent = CancellationToken::new();
    let task = tokio::spawn(coordinate(
        (0..64).collect(),
        parent.clone(),
        CancellationToken::new(),
        None,
        move |shard, _| {
            let probe = Arc::clone(&work_probe);
            let gate = Arc::clone(&work_gate);
            async move {
                let _active = probe.enter();
                if shard < 8 {
                    gate.wait().await;
                }
                tokio::task::yield_now().await;
                Ok(shard * 10)
            }
        },
    ));
    bounded(gate.wait()).await;
    let mut results = bounded(task).await.unwrap().unwrap();
    results.sort_unstable();
    assert_eq!(
        results,
        (0..64).map(|shard| (shard, shard * 10)).collect::<Vec<_>>()
    );
    assert_eq!(probe.peak.load(Ordering::SeqCst), 8);
    assert_eq!(probe.started.load(Ordering::SeqCst), 64);
    assert_eq!(probe.closed.load(Ordering::SeqCst), 64);
    assert_eq!(probe.active.load(Ordering::SeqCst), 0);
    assert!(!parent.is_cancelled());
}

#[tokio::test]
async fn frontier_errors_and_panics_cancel_and_drain_peers_without_poisoning_parent() {
    for panic in [false, true] {
        let probe = Arc::new(Probe::default());
        let gate = Arc::new(Barrier::new(9));
        let work_probe = Arc::clone(&probe);
        let work_gate = Arc::clone(&gate);
        let parent = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(coordinate(
            (0..64).collect(),
            parent.clone(),
            shutdown.clone(),
            None,
            move |shard, child| {
                let probe = Arc::clone(&work_probe);
                let gate = Arc::clone(&work_gate);
                async move {
                    let _active = probe.enter();
                    gate.wait().await;
                    if shard == 0 {
                        assert!(!panic, "injected child failure");
                        return Err::<(), _>(limit_exceeded("injected frontier budget"));
                    }
                    child.cancelled().await;
                    Err(EngineError::new(
                        EngineErrorKind::Cancelled,
                        "child canceled",
                    ))
                }
            },
        ));
        bounded(gate.wait()).await;
        let error = bounded(task).await.unwrap().unwrap_err();
        if panic {
            assert_eq!(error.kind(), EngineErrorKind::Internal);
        } else {
            assert!(error.to_string().contains("injected frontier budget"));
        }
        assert_eq!(probe.started.load(Ordering::SeqCst), 8);
        assert_eq!(probe.closed.load(Ordering::SeqCst), 8);
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        assert!(!parent.is_cancelled());
        assert!(!shutdown.is_cancelled());
        assert_eq!(
            coordinate(vec![2], parent, shutdown, None, |shard, _| async move {
                Ok(shard)
            })
            .await
            .unwrap(),
            vec![(2, 2)]
        );
    }
}

#[tokio::test]
async fn parent_and_shutdown_cancellation_drain_started_frontiers_before_return() {
    for close in [false, true] {
        let probe = Arc::new(Probe::default());
        let gate = Arc::new(Barrier::new(9));
        let work_probe = Arc::clone(&probe);
        let work_gate = Arc::clone(&gate);
        let parent = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(coordinate(
            (0..64).collect(),
            parent.clone(),
            shutdown.clone(),
            None,
            move |_, child| {
                let probe = Arc::clone(&work_probe);
                let gate = Arc::clone(&work_gate);
                async move {
                    let _active = probe.enter();
                    gate.wait().await;
                    child.cancelled().await;
                    Ok(())
                }
            },
        ));
        bounded(gate.wait()).await;
        if close {
            shutdown.cancel();
        } else {
            parent.cancel();
        }
        let error = bounded(task).await.unwrap().unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        assert_eq!(probe.started.load(Ordering::SeqCst), 8);
        assert_eq!(probe.closed.load(Ordering::SeqCst), 8);
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        assert_eq!(parent.is_cancelled(), !close);
    }
}

#[tokio::test]
async fn expired_or_canceled_frontiers_admit_no_children() {
    let started = AtomicUsize::new(0);
    let parent = CancellationToken::new();
    let shutdown = CancellationToken::new();
    for expired in [true, false] {
        if !expired {
            parent.cancel();
        }
        let result = coordinate(
            vec![0, 1],
            parent.clone(),
            shutdown.clone(),
            expired.then(|| Instant::now() - Duration::from_secs(1)),
            |_, _| {
                started.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            },
        )
        .await;
        let expected = if expired {
            EngineErrorKind::DeadlineExceeded
        } else {
            EngineErrorKind::Cancelled
        };
        assert_eq!(result.unwrap_err().kind(), expected);
    }
    assert_eq!(started.load(Ordering::SeqCst), 0);
    assert!(
        coordinate(
            Vec::<u16>::new(),
            CancellationToken::new(),
            shutdown,
            None,
            |_, _| async { Ok(()) }
        )
        .await
        .unwrap()
        .is_empty()
    );
}

#[tokio::test]
async fn live_frontier_deadline_cancels_and_drains_started_children() {
    let probe = Arc::new(Probe::default());
    let work_probe = Arc::clone(&probe);
    let parent = CancellationToken::new();
    let error = coordinate(
        (0..64).collect(),
        parent.clone(),
        CancellationToken::new(),
        Some(Instant::now() + Duration::from_secs(1)),
        move |_, child| {
            let probe = Arc::clone(&work_probe);
            async move {
                let _active = probe.enter();
                child.cancelled().await;
                Ok(())
            }
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
    assert_eq!(probe.started.load(Ordering::SeqCst), 8);
    assert_eq!(probe.closed.load(Ordering::SeqCst), 8);
    assert_eq!(probe.active.load(Ordering::SeqCst), 0);
    assert!(!parent.is_cancelled());
}

#[test]
fn frontier_budget_is_shared_checked_and_never_partially_charges_rejection() {
    let budget = FrontierBudget {
        retained: AtomicU64::new(0),
        limit: 1024,
    };
    std::thread::scope(|scope| {
        let results: Vec<_> = (0..8)
            .map(|_| scope.spawn(|| budget.reserve(256)))
            .collect();
        assert_eq!(
            results
                .into_iter()
                .map(|result| result.join().unwrap())
                .filter(|result| result.is_ok())
                .count(),
            4
        );
    });
    assert_eq!(budget.retained.load(Ordering::Acquire), 1024);
    assert!(budget.reserve(1).is_err());
    assert_eq!(budget.retained.load(Ordering::Acquire), 1024);
    let overflow = FrontierBudget {
        retained: AtomicU64::new(u64::MAX),
        limit: u64::MAX,
    };
    assert!(overflow.reserve(1).is_err());
    assert_eq!(overflow.retained.load(Ordering::Acquire), u64::MAX);
}
