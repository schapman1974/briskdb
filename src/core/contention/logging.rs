//! Rate-limited, data-free events. The embedding host owns the subscriber.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

static NEXT_BUDGET: AtomicU64 = AtomicU64::new(1);
const LOG_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(super) struct ContentionLog {
    id: u64,
    last_warning: Duration,
    dispatch: Option<tracing::Dispatch>,
}

impl ContentionLog {
    pub(super) fn new() -> Self {
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        Self {
            id: NEXT_BUDGET.fetch_add(1, Ordering::Relaxed),
            last_warning: Duration::ZERO,
            dispatch: (!dispatch.is::<tracing::subscriber::NoSubscriber>()).then_some(dispatch),
        }
    }

    pub(super) fn retry(
        &mut self,
        attempt: u32,
        elapsed: Duration,
        remaining: Duration,
        delay: Duration,
    ) {
        // Carry the host's subscriber onto blocking workers. Never touch a
        // callsite from an unsubscribed request: that can poison cached
        // interest for the locked tracing version (see Mongo RequestTrace).
        let Some(dispatch) = &self.dispatch else {
            return;
        };
        tracing::dispatcher::with_default(dispatch, || {
            if elapsed.saturating_sub(self.last_warning) >= LOG_INTERVAL {
                self.last_warning = elapsed;
                tracing::warn!(
                    target: "briskdb::contention",
                    budget_id = self.id, attempt,
                    elapsed_ms = elapsed.as_millis() as u64,
                    remaining_ms = remaining.as_millis() as u64,
                    retry_delay_ms = delay.as_millis() as u64,
                    "still waiting for lock or admission; application work is not replayed"
                );
            } else if attempt == 1 {
                tracing::debug!(
                    target: "briskdb::contention",
                    budget_id = self.id, attempt,
                    remaining_ms = remaining.as_millis() as u64,
                    retry_delay_ms = delay.as_millis() as u64,
                    "contention backoff started"
                );
            }
        });
    }

    pub(super) fn exhausted(&self, attempts: u32, elapsed: Duration, budget: Duration) {
        let Some(dispatch) = &self.dispatch else {
            return;
        };
        tracing::dispatcher::with_default(dispatch, || {
            tracing::warn!(
                target: "briskdb::contention",
                budget_id = self.id, attempts,
                elapsed_ms = elapsed.as_millis() as u64,
                max_elapsed_ms = budget.as_millis() as u64,
                "contention budget exhausted; no further lock retries"
            );
        });
    }
}

#[cfg(all(test, feature = "server-cli"))]
mod tests {
    use super::*;
    use crate::core::contention::ContentionBudget;
    use crate::{ContentionJitter, ContentionPolicy};
    use std::{
        io::Write,
        sync::{Arc, Mutex},
        time::Instant,
    };

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn logs_are_rate_limited_and_exhaustion_is_reported_once() {
        let output = Capture::default();
        let writer = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        // An unsubscribed first use must not suppress a later host's events.
        let _ = ContentionBudget::new(ContentionPolicy::fail_fast()).next_delay(Instant::now(), 0);
        let mut budget = tracing::dispatcher::with_default(&dispatch, || {
            let policy = ContentionPolicy::new(
                Duration::from_millis(1),
                Duration::from_millis(1),
                1,
                ContentionJitter::None,
                1000,
                Duration::from_secs(11),
            )
            .unwrap();
            ContentionBudget::new(policy)
        });
        std::thread::spawn(move || {
            let start = Instant::now();
            for millis in (0..=10_000).step_by(100) {
                assert!(
                    budget
                        .next_delay(start + Duration::from_millis(millis), 0)
                        .is_some()
                );
            }
            for _ in 0..3 {
                assert!(budget.expired(start + Duration::from_secs(11)));
                assert!(
                    budget
                        .next_delay(start + Duration::from_secs(11), 0)
                        .is_none()
                );
            }
        })
        .join()
        .unwrap();
        let logs = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
        assert_eq!(logs.matches("contention backoff started").count(), 1);
        assert_eq!(
            logs.matches("still waiting for lock or admission").count(),
            2
        );
        assert_eq!(logs.matches("contention budget exhausted").count(), 1);
        for field in [
            "budget_id=",
            "attempt=",
            "elapsed_ms=5000",
            "remaining_ms=6000",
            "retry_delay_ms=1",
            "max_elapsed_ms=11000",
        ] {
            assert!(logs.contains(field), "missing {field}: {logs}");
        }
    }
}
