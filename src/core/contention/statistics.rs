//! Fixed-size, saturating diagnostics; no request identities or data labels.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// Engine-lifetime observations of explicitly configured contention policies.
///
/// Legacy waits and not-yet-integrated internal paths are excluded. Counters
/// saturate at `u64::MAX` and reset when a new engine is created. Concurrent
/// updates can occur between field reads; this is not a transactional snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ContentionStatistics {
    retries_scheduled: u64,
    wait_nanos: u64,
    exhausted_budgets: u64,
}

impl ContentionStatistics {
    /// Reserved retry/backoff windows, excluding initial acquisition attempts.
    /// Admission may complete or cancellation may occur before a window ends.
    pub const fn retries_scheduled(self) -> u64 {
        self.retries_scheduled
    }

    /// Summed time in completed application contention-wait windows.
    ///
    /// Concurrent child waits add independently. This includes early completion,
    /// cancellation and scheduler delay, not SQL execution or NFS kernel I/O.
    /// Active waits contribute when they finish, including on future drop.
    pub const fn wait_nanos(self) -> u64 {
        self.wait_nanos
    }

    /// Budgets observed refusing further contention waiting, once per shared
    /// budget, including fail-fast. This is not a count of final request errors.
    pub const fn exhausted_budgets(self) -> u64 {
        self.exhausted_budgets
    }
}

#[derive(Debug, Default)]
pub(crate) struct ContentionMetrics {
    retries_scheduled: AtomicU64,
    wait_nanos: AtomicU64,
    exhausted_budgets: AtomicU64,
}

impl ContentionMetrics {
    pub(crate) fn snapshot(&self) -> ContentionStatistics {
        ContentionStatistics {
            retries_scheduled: self.retries_scheduled.load(Ordering::Relaxed),
            wait_nanos: self.wait_nanos.load(Ordering::Relaxed),
            exhausted_budgets: self.exhausted_budgets.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn scheduled(&self) {
        saturating_add(&self.retries_scheduled, 1);
    }

    pub(crate) fn exhausted(&self) {
        saturating_add(&self.exhausted_budgets, 1);
    }

    fn waited(&self, elapsed: Duration) {
        saturating_add(
            &self.wait_nanos,
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
        );
    }

    pub(crate) fn start_wait(self: &Arc<Self>) -> ContentionWait {
        ContentionWait {
            metrics: Arc::clone(self),
            started: Instant::now(),
        }
    }
}

// The renamed Atomic::try_update is unavailable on our Rust 1.85 MSRV.
#[allow(deprecated)]
fn saturating_add(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(amount))
    });
}

pub(crate) struct ContentionWait {
    metrics: Arc<ContentionMetrics>,
    started: Instant,
}

impl Drop for ContentionWait {
    fn drop(&mut self) {
        self.metrics.waited(self.started.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_saturate_without_wrapping_and_sum_concurrent_updates() {
        let metrics = Arc::new(ContentionMetrics::default());
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let metrics = Arc::clone(&metrics);
                scope.spawn(move || {
                    for _ in 0..100 {
                        metrics.scheduled();
                        metrics.waited(Duration::from_nanos(2));
                        metrics.exhausted();
                    }
                });
            }
        });
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.retries_scheduled(), 800);
        assert_eq!(snapshot.wait_nanos(), 1600);
        assert_eq!(snapshot.exhausted_budgets(), 800);
        for counter in [&metrics.retries_scheduled, &metrics.exhausted_budgets] {
            counter.store(u64::MAX - 1, Ordering::Relaxed);
            saturating_add(counter, 3);
            saturating_add(counter, 1);
            assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
        }
        metrics.waited(Duration::MAX);
        metrics.waited(Duration::from_nanos(1));
        assert_eq!(metrics.snapshot().wait_nanos(), u64::MAX);
    }

    #[test]
    fn wait_timer_records_only_on_drop_including_unwind() {
        let metrics = Arc::new(ContentionMetrics::default());
        let result = std::panic::catch_unwind({
            let metrics = Arc::clone(&metrics);
            move || {
                let mut timer = metrics.start_wait();
                timer.started = Instant::now() - Duration::from_millis(2);
                assert_eq!(metrics.snapshot().wait_nanos(), 0);
                panic!("test unwind");
            }
        });
        assert!(result.is_err());
        assert!(metrics.snapshot().wait_nanos() >= 2_000_000);
        assert_eq!(metrics.snapshot().retries_scheduled(), 0);
        assert_eq!(metrics.snapshot().exhausted_budgets(), 0);
    }
}
