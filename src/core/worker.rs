//! Bounded admission for blocking engine work.

use std::sync::Arc;

use tokio::sync::Semaphore;

use super::{EngineError, EngineErrorKind, EngineResult};

/// A cloneable concurrency bound around Tokio's blocking workers.
#[derive(Debug, Clone)]
pub(crate) struct BlockingPool {
    limit: usize,
    permits: Arc<Semaphore>,
}

impl BlockingPool {
    /// Construct a blocking pool with an already-validated concurrency bound.
    pub(crate) fn new(max_active: usize) -> Self {
        assert!(max_active > 0, "blocking pool must have active capacity");
        Self {
            limit: max_active,
            permits: Arc::new(Semaphore::new(max_active)),
        }
    }

    /// Return the configured maximum number of active blocking tasks.
    pub(crate) const fn limit(&self) -> usize {
        self.limit
    }

    /// Return the number of permits not currently held by admitted work.
    #[cfg(test)]
    pub(crate) fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }

    /// Wait for capacity, then execute blocking work without exceeding the
    /// configured concurrency bound.
    pub(crate) async fn run<T, F>(&self, work: F) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> EngineResult<T> + Send + 'static,
    {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::Internal,
                    "blocking engine pool closed unexpectedly",
                    error,
                )
            })?;

        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work()
        })
        .await
        .map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::Internal,
                "blocking engine task failed",
                error,
            )
        })?
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, time::Duration};

    use tokio::{sync::oneshot, time::timeout};

    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    fn assert_send<T: Send>(_: T) {}

    async fn wait_for_started(started: oneshot::Receiver<()>) {
        timeout(Duration::from_secs(2), started)
            .await
            .expect("blocking work should start")
            .expect("blocking worker should report that it started");
    }

    fn held_work(
        started: oneshot::Sender<()>,
        release: mpsc::Receiver<()>,
    ) -> impl FnOnce() -> EngineResult<()> + Send + 'static {
        move || {
            let _ = started.send(());
            release.recv().expect("test releases the blocking work");
            Ok(())
        }
    }

    #[tokio::test]
    async fn public_future_and_pool_are_send_and_pool_is_sync() {
        assert_send_sync::<BlockingPool>();
        let pool = BlockingPool::new(1);
        assert_eq!(pool.limit(), 1);
        assert_eq!(pool.available_permits(), 1);
        assert_send(pool.run(|| Ok::<_, EngineError>(())))
    }

    #[tokio::test]
    async fn active_blocking_work_never_exceeds_the_exact_bound() {
        let pool = BlockingPool::new(2);
        let (first_started_tx, first_started_rx) = oneshot::channel();
        let (second_started_tx, second_started_rx) = oneshot::channel();
        let (third_started_tx, mut third_started_rx) = oneshot::channel();
        let (first_release_tx, first_release_rx) = mpsc::channel();
        let (second_release_tx, second_release_rx) = mpsc::channel();
        let (third_release_tx, third_release_rx) = mpsc::channel();

        let first_pool = pool.clone();
        let first = tokio::spawn(async move {
            first_pool
                .run(held_work(first_started_tx, first_release_rx))
                .await
        });
        let second_pool = pool.clone();
        let second = tokio::spawn(async move {
            second_pool
                .run(held_work(second_started_tx, second_release_rx))
                .await
        });
        wait_for_started(first_started_rx).await;
        wait_for_started(second_started_rx).await;
        assert_eq!(pool.available_permits(), 0);

        let third_pool = pool.clone();
        let third = tokio::spawn(async move {
            third_pool
                .run(held_work(third_started_tx, third_release_rx))
                .await
        });
        assert!(
            timeout(Duration::from_millis(50), &mut third_started_rx)
                .await
                .is_err(),
            "third worker started while both permits were held"
        );

        first_release_tx.send(()).unwrap();
        wait_for_started(third_started_rx).await;
        assert_eq!(pool.available_permits(), 0);
        second_release_tx.send(()).unwrap();
        third_release_tx.send(()).unwrap();

        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        third.await.unwrap().unwrap();
        assert_eq!(pool.available_permits(), 2);
    }

    #[tokio::test]
    async fn a_worker_panic_is_internal_and_returns_its_permit() {
        let pool = BlockingPool::new(1);
        let error = pool
            .run(|| -> EngineResult<()> { panic!("intentional worker panic") })
            .await
            .unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert_eq!(error.to_string(), "blocking engine task failed");
        timeout(Duration::from_secs(2), pool.run(|| Ok(())))
            .await
            .expect("panic must release the blocking permit")
            .unwrap();
        assert_eq!(pool.available_permits(), 1);
    }

    #[tokio::test]
    async fn aborting_the_outer_future_keeps_the_permit_until_work_finishes() {
        let pool = BlockingPool::new(1);
        let (first_started_tx, first_started_rx) = oneshot::channel();
        let (first_release_tx, first_release_rx) = mpsc::channel();
        let first_pool = pool.clone();
        let first = tokio::spawn(async move {
            first_pool
                .run(held_work(first_started_tx, first_release_rx))
                .await
        });
        wait_for_started(first_started_rx).await;
        assert_eq!(pool.available_permits(), 0);
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        let (second_started_tx, mut second_started_rx) = oneshot::channel();
        let (second_release_tx, second_release_rx) = mpsc::channel();
        let second_pool = pool.clone();
        let second = tokio::spawn(async move {
            second_pool
                .run(held_work(second_started_tx, second_release_rx))
                .await
        });
        assert!(
            timeout(Duration::from_millis(50), &mut second_started_rx)
                .await
                .is_err(),
            "detached blocking work released its permit before finishing"
        );

        first_release_tx.send(()).unwrap();
        wait_for_started(second_started_rx).await;
        assert_eq!(pool.available_permits(), 0);
        second_release_tx.send(()).unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(pool.available_permits(), 1);
    }
}
