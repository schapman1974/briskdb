//! Protocol-neutral request cancellation and deadline controls.

use std::{
    fmt,
    future::Future,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::sync::Notify;

use super::{
    ContentionJitter, ContentionPolicy,
    contention::{ContentionBudget, ContentionMetrics, ContentionWait},
};
use super::{EngineError, EngineErrorKind, EngineResult, ResultLimits};

/// A cloneable, sticky request-cancellation signal.
///
/// Cancellation is idempotent. Once cancelled, all current and future waiters
/// observe the signal. A token controls only requests whose
/// [`RequestContext`] contains that token; it is never stored on a session.
#[derive(Clone, Default)]
pub struct CancellationToken {
    inner: Arc<CancellationInner>,
}

#[derive(Default)]
struct CancellationInner {
    cancelled: AtomicBool,
    notify: Notify,
}

impl CancellationToken {
    /// Create a token in the non-cancelled state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancel every request currently observing this token.
    ///
    /// Returns `true` only for the call that changed the token's state.
    pub fn cancel(&self) -> bool {
        let changed = !self.inner.cancelled.swap(true, Ordering::AcqRel);
        if changed {
            self.inner.notify.notify_waiters();
        }
        changed
    }

    /// Return whether cancellation has already been requested.
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    /// Wait until cancellation is requested.
    pub async fn cancelled(&self) {
        loop {
            // Register before checking the sticky bit so cancellation cannot
            // land in a lost-wakeup window between the two operations.
            let notified = self.inner.notify.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

/// Per-request controls consumed by the protocol-neutral engine boundary.
///
/// A frontend can use the same context for one operation. Reusing a cancelled
/// context intentionally cancels the later operation as well. Result limits in
/// a context may narrow, but never widen, the engine-wide limits.
#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    cancellation: CancellationToken,
    deadline: Option<Instant>,
    result_limits: Option<ResultLimits>,
}

impl RequestContext {
    /// Create a request with a fresh cancellation token and no explicit
    /// deadline or result-limit override.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the cancellation token observed by this request.
    #[must_use]
    pub fn with_cancellation_token(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    /// Set an absolute monotonic deadline.
    #[must_use]
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Set a deadline relative to the current monotonic time.
    pub fn with_timeout(mut self, timeout: Duration) -> EngineResult<Self> {
        if timeout.is_zero() {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "request timeout must be greater than zero",
            ));
        }
        self.deadline = Some(Instant::now().checked_add(timeout).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::InvalidArgument,
                "request timeout is too large for the monotonic clock",
            )
        })?);
        Ok(self)
    }

    /// Narrow the configured query-result limits for this request.
    #[must_use]
    pub fn with_result_limits(mut self, result_limits: ResultLimits) -> Self {
        self.result_limits = Some(result_limits);
        self
    }

    /// Return a clone of the request cancellation token.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Return the caller-supplied absolute deadline, if any.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Return the caller-supplied result-limit override, if any.
    pub fn result_limits(&self) -> Option<ResultLimits> {
        self.result_limits
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CancellationReason {
    Cancelled,
    DeadlineExceeded,
}

impl CancellationReason {
    pub(crate) fn error(self) -> EngineError {
        match self {
            Self::Cancelled => EngineError::new(
                EngineErrorKind::Cancelled,
                "the request was cancelled before the operation completed",
            ),
            Self::DeadlineExceeded => {
                EngineError::deadline_exceeded("the request deadline elapsed")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationPhase {
    Pending,
    Running,
    Finished,
}

struct OperationState {
    phase: OperationPhase,
    reason: Option<CancellationReason>,
    interrupt: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
}

/// Race-safe state shared by the async caller and one leased SQLite handle.
///
/// The mutex is the linearization point between cancellation and the start of
/// SQLite execution. A pending cancellation prevents SQL from starting; a
/// running cancellation interrupts the exact currently leased handle.
pub(crate) struct OperationControl {
    state: Mutex<OperationState>,
    deadline: Option<Instant>,
    contention: Option<Arc<Mutex<ContentionBudget>>>,
    wake_contention: Condvar,
}

/// Request-local state propagated to physical child tasks. Clones share only
/// the retry budget and cancellation signal, never a SQLite interrupt slot or
/// operation phase. Public cancellation tokens can be reused across requests
/// without accidentally reusing their budgets.
#[derive(Clone)]
pub(crate) struct RequestScope {
    cancellation: CancellationToken,
    contention: Option<Arc<Mutex<ContentionBudget>>>,
}

impl RequestScope {
    pub(crate) fn new(cancellation: CancellationToken, parent: &OperationControl) -> Self {
        Self {
            cancellation,
            contention: parent.contention.clone(),
        }
    }

    pub(crate) fn child_control(&self, deadline: Option<Instant>) -> Arc<OperationControl> {
        OperationControl::with_contention_budget(deadline, self.contention.clone())
    }

    /// A fanout error cancels its siblings, not its caller, while all of them
    /// still consume the original logical request's contention budget.
    #[cfg(feature = "documents")]
    pub(crate) fn fork_cancellation(&self) -> Self {
        Self {
            cancellation: CancellationToken::new(),
            contention: self.contention.clone(),
        }
    }
}

// CPU/storage helpers only need the token. Async coordinators carry the scope
// itself so creating another physical task cannot silently reset its budget.
impl std::ops::Deref for RequestScope {
    type Target = CancellationToken;

    fn deref(&self) -> &Self::Target {
        &self.cancellation
    }
}

impl OperationControl {
    #[cfg(any(feature = "tinymongo-import", test))]
    pub(crate) fn new(deadline: Option<Instant>) -> Arc<Self> {
        Self::with_contention_policy(deadline, None)
    }

    #[cfg(any(feature = "tinymongo-import", test))]
    pub(crate) fn with_contention_policy(
        deadline: Option<Instant>,
        policy: Option<ContentionPolicy>,
    ) -> Arc<Self> {
        Self::with_contention_metrics(deadline, policy, Arc::new(ContentionMetrics::default()))
    }

    pub(crate) fn with_contention_metrics(
        deadline: Option<Instant>,
        policy: Option<ContentionPolicy>,
        metrics: Arc<ContentionMetrics>,
    ) -> Arc<Self> {
        Self::with_contention_budget(
            deadline,
            policy.map(|policy| {
                Arc::new(Mutex::new(ContentionBudget::with_metrics(policy, metrics)))
            }),
        )
    }

    fn with_contention_budget(
        deadline: Option<Instant>,
        contention: Option<Arc<Mutex<ContentionBudget>>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(OperationState {
                phase: OperationPhase::Pending,
                reason: None,
                interrupt: None,
            }),
            deadline,
            contention,
            wake_contention: Condvar::new(),
        })
    }

    #[cfg(feature = "auth-scram")]
    pub(crate) fn has_contention_policy(&self) -> bool {
        self.contention.is_some()
    }

    /// A blocking-worker-only lock wait, never a retry of application work.
    /// `None` keeps the caller's legacy policy. Reusing this control across
    /// connection setup, collection fences and SQL shares one finite budget.
    pub(crate) fn wait_for_contention(
        &self,
        cancellation: Option<&CancellationToken>,
    ) -> Option<bool> {
        let budget = self.contention.as_ref()?;
        if self.should_stop() || cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Some(false);
        }
        let delay = self.next_contention_delay();
        let Some(delay) = delay else {
            return Some(false);
        };
        let _wait = self.start_contention_wait();
        let until = Instant::now() + delay;
        loop {
            if self.should_stop() || cancellation.is_some_and(CancellationToken::is_cancelled) {
                return Some(false);
            }
            let now = Instant::now();
            if now >= until {
                return Some(
                    !budget
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .expired(now),
                );
            }
            let mut remaining = until.duration_since(now);
            if let Some(deadline) = self.deadline {
                remaining = remaining.min(deadline.saturating_duration_since(now));
            }
            // Poll standalone tokens too; async callers normally signal the
            // control directly and wake the condition variable immediately.
            remaining = remaining.min(Duration::from_millis(10));
            let state = self.lock_state();
            if state.reason.is_some() {
                return Some(false);
            }
            drop(
                self.wake_contention
                    .wait_timeout(state, remaining)
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        }
    }

    /// Reserve one retry from the shared logical-request budget. Called only
    /// after a lock or admission future has actually reported contention.
    fn next_contention_delay(&self) -> Option<Duration> {
        let mut budget = self
            .contention
            .as_ref()?
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut random = [0; 8];
        if budget.jitter() == ContentionJitter::Full && getrandom::fill(&mut random).is_err() {
            // This can run inside SQLite's C callback: never panic or silently
            // disable the finite policy when entropy is unavailable.
            return None;
        }
        budget.next_delay(Instant::now(), u64::from_le_bytes(random))
    }

    fn start_contention_wait(&self) -> Option<ContentionWait> {
        self.contention.as_ref().map(|budget| {
            budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .start_wait()
        })
    }

    fn contention_expired(&self) -> bool {
        self.contention.as_ref().is_some_and(|budget| {
            budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .expired(Instant::now())
        })
    }

    pub(crate) fn request_cancel(&self, reason: CancellationReason) -> bool {
        let interrupt = {
            let mut state = self.lock_state();
            if state.phase == OperationPhase::Finished || state.reason.is_some() {
                return false;
            }
            state.reason = Some(reason);
            state.interrupt.clone()
        };
        self.wake_contention.notify_all();
        if let Some(interrupt) = interrupt {
            interrupt();
        }
        true
    }

    pub(crate) fn arm(
        &self,
        interrupt: Arc<dyn Fn() + Send + Sync + 'static>,
    ) -> Result<(), CancellationReason> {
        self.expire_deadline();
        let mut state = self.lock_state();
        if let Some(reason) = state.reason {
            return Err(reason);
        }
        debug_assert_eq!(state.phase, OperationPhase::Pending);
        state.phase = OperationPhase::Running;
        state.interrupt = Some(interrupt);
        Ok(())
    }

    pub(crate) fn should_stop(&self) -> bool {
        self.expire_deadline();
        self.lock_state().reason.is_some()
    }

    pub(crate) fn disarm(&self) -> Option<CancellationReason> {
        let mut state = self.lock_state();
        state.interrupt = None;
        if state.phase == OperationPhase::Running {
            state.phase = OperationPhase::Pending;
        }
        state.reason
    }

    pub(crate) fn complete<T>(&self, result: EngineResult<T>) -> EngineResult<T> {
        let reason = {
            let mut state = self.lock_state();
            state.phase = OperationPhase::Finished;
            state.interrupt = None;
            state.reason
        };

        // A completed SQLite operation wins a very close cancellation race.
        // In particular, never report cancellation for a write known to have
        // committed successfully. Interrupted failures use the first accepted
        // cancellation reason so deadlines remain distinguishable.
        match (result, reason) {
            (Ok(value), _) => Ok(value),
            (Err(_), Some(reason)) => Err(reason.error()),
            (Err(error), None) => Err(error),
        }
    }

    pub(crate) fn reason(&self) -> Option<CancellationReason> {
        self.expire_deadline();
        self.lock_state().reason
    }

    fn expire_deadline(&self) {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.request_cancel(CancellationReason::DeadlineExceeded);
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, OperationState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl fmt::Debug for OperationControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.lock_state();
        formatter
            .debug_struct("OperationControl")
            .field("phase", &state.phase)
            .field("reason", &state.reason)
            .field("interrupt_armed", &state.interrupt.is_some())
            .field("deadline", &self.deadline)
            .finish()
    }
}

/// Cancel a started operation if its public future is dropped.
pub(crate) struct CancelOnDrop {
    control: Arc<OperationControl>,
    armed: bool,
}

impl CancelOnDrop {
    pub(crate) fn new(control: Arc<OperationControl>) -> Self {
        Self {
            control,
            armed: true,
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.control.request_cancel(CancellationReason::Cancelled);
        }
    }
}

pub(crate) async fn wait_for_cancellation(
    request: &CancellationToken,
    shutdown: &CancellationToken,
    deadline: Option<Instant>,
) -> CancellationReason {
    let deadline_wait = async move {
        match deadline {
            Some(deadline) => {
                tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await
            }
            None => std::future::pending::<()>().await,
        }
    };
    tokio::select! {
        _ = request.cancelled() => CancellationReason::Cancelled,
        _ = shutdown.cancelled() => CancellationReason::Cancelled,
        _ = deadline_wait => CancellationReason::DeadlineExceeded,
    }
}

/// Wait only for cancellation-safe resource admission, never a started task.
/// The future is retained, not replayed, across contention-delay windows.
pub(crate) async fn wait_admission<T, F>(
    future: F,
    request: &CancellationToken,
    shutdown: &CancellationToken,
    deadline: Option<Instant>,
    control: &OperationControl,
) -> EngineResult<T>
where
    F: Future<Output = EngineResult<T>>,
{
    tokio::pin!(future);
    if control.contention.is_some() {
        // Poll once even for fail-fast/exhausted budgets: uncontended admission
        // is always allowed. Retain this exact future (including its queue
        // position) thereafter; never recreate or replay application work.
        if let std::task::Poll::Ready(result) =
            std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx))).await
        {
            return result;
        }
        loop {
            let reason = if request.is_cancelled() || shutdown.is_cancelled() {
                Some(CancellationReason::Cancelled)
            } else if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                Some(CancellationReason::DeadlineExceeded)
            } else {
                control.reason()
            };
            if let Some(reason) = reason {
                control.request_cancel(reason);
                return Err(reason.error());
            }
            let Some(delay) = control.next_contention_delay() else {
                return Err(EngineError::new(
                    EngineErrorKind::Busy,
                    "the request contention budget was exhausted while waiting for admission",
                ));
            };
            let _wait = control.start_contention_wait();
            tokio::select! {
                biased;
                reason = wait_for_cancellation(request, shutdown, deadline) => {
                    control.request_cancel(reason);
                    return Err(reason.error());
                }
                _ = tokio::time::sleep(delay) => {}
                result = &mut future => {
                    if result.is_ok() && control.contention_expired() {
                        return Err(EngineError::new(
                            EngineErrorKind::Busy,
                            "the request contention budget expired while waiting for admission",
                        ));
                    }
                    return result;
                }
            }
        }
    }
    wait_pending(future, request, shutdown, deadline, control).await
}

pub(crate) async fn wait_pending<T, F>(
    future: F,
    request: &CancellationToken,
    shutdown: &CancellationToken,
    deadline: Option<Instant>,
    control: &OperationControl,
) -> EngineResult<T>
where
    F: Future<Output = EngineResult<T>>,
{
    tokio::pin!(future);
    tokio::select! {
        biased;
        result = &mut future => result,
        reason = wait_for_cancellation(request, shutdown, deadline) => {
            control.request_cancel(reason);
            Err(reason.error())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    fn short_contention_policy(retries: u32) -> ContentionPolicy {
        ContentionPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(1),
            1,
            ContentionJitter::None,
            retries,
            Duration::from_secs(5),
        )
        .unwrap()
    }

    #[test]
    fn contention_scope_shares_one_atomic_retry_count_across_parallel_children() {
        let metrics = Arc::new(ContentionMetrics::default());
        let parent = OperationControl::with_contention_metrics(
            None,
            Some(short_contention_policy(7)),
            Arc::clone(&metrics),
        );
        let scope = RequestScope::new(CancellationToken::new(), &parent);
        let admitted = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::new();
        for _ in 0..16 {
            let control = scope.clone().child_control(None);
            let admitted = Arc::clone(&admitted);
            workers.push(std::thread::spawn(move || {
                while control.next_contention_delay().is_some() {
                    admitted.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(admitted.load(Ordering::SeqCst), 7);
        assert_eq!(parent.next_contention_delay(), None);
        assert_eq!(metrics.snapshot().retries_scheduled(), 7);
        assert_eq!(metrics.snapshot().exhausted_budgets(), 1);
        assert_eq!(metrics.snapshot().wait_nanos(), 0);
    }

    #[test]
    fn contention_diagnostics_measure_blocking_waits_but_not_pre_cancelled_or_legacy_calls() {
        let metrics = Arc::new(ContentionMetrics::default());
        let legacy = OperationControl::with_contention_metrics(None, None, Arc::clone(&metrics));
        assert_eq!(legacy.wait_for_contention(None), None);
        let cancelled = OperationControl::with_contention_metrics(
            None,
            Some(short_contention_policy(1)),
            Arc::clone(&metrics),
        );
        cancelled.request_cancel(CancellationReason::Cancelled);
        assert_eq!(cancelled.wait_for_contention(None), Some(false));
        assert_eq!(
            metrics.snapshot(),
            super::super::ContentionStatistics::default()
        );
        let active = OperationControl::with_contention_metrics(
            None,
            Some(short_contention_policy(1)),
            Arc::clone(&metrics),
        );
        assert_eq!(active.wait_for_contention(None), Some(true));
        assert_eq!(active.wait_for_contention(None), Some(false));
        assert_eq!(metrics.snapshot().retries_scheduled(), 1);
        assert!(metrics.snapshot().wait_nanos() >= 1_000_000);
        assert_eq!(metrics.snapshot().exhausted_budgets(), 1);
    }

    #[tokio::test]
    async fn contention_diagnostics_record_dropped_admission_wait_without_false_exhaustion() {
        let metrics = Arc::new(ContentionMetrics::default());
        let control = OperationControl::with_contention_metrics(
            None,
            Some(slow_contention_policy()),
            Arc::clone(&metrics),
        );
        let request = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let mut waiting = Box::pin(wait_admission(
            std::future::pending::<EngineResult<()>>(),
            &request,
            &shutdown,
            None,
            &control,
        ));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(waiting.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        assert_eq!(metrics.snapshot().retries_scheduled(), 1);
        assert_eq!(metrics.snapshot().wait_nanos(), 0);
        drop(waiting);
        assert!(metrics.snapshot().wait_nanos() > 0);
        assert_eq!(metrics.snapshot().exhausted_budgets(), 0);
        let before = metrics.snapshot();
        wait_admission(async { Ok(()) }, &request, &shutdown, None, &control)
            .await
            .unwrap();
        assert_eq!(metrics.snapshot(), before); // Ready admission is not a wait.
    }

    #[test]
    fn contention_scope_never_shares_interrupt_slots_or_public_token_budgets() {
        let token = CancellationToken::new();
        let parent =
            OperationControl::with_contention_policy(None, Some(short_contention_policy(1)));
        let scope = RequestScope::new(token.clone(), &parent);
        let first = scope.child_control(None);
        let second = scope.child_control(None);
        let interrupted = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&interrupted);
        first
            .arm(Arc::new(move || {
                observed.fetch_add(1, Ordering::SeqCst);
            }))
            .unwrap();
        second
            .arm(Arc::new(|| {
                panic!("a sibling interrupt slot must stay independent")
            }))
            .unwrap();
        first.request_cancel(CancellationReason::Cancelled);
        assert_eq!(interrupted.load(Ordering::SeqCst), 1);
        assert_eq!(second.reason(), None);
        assert_eq!(parent.reason(), None);
        assert!(!token.is_cancelled());
        assert!(second.next_contention_delay().is_some());
        assert_eq!(scope.child_control(None).next_contention_delay(), None);
        // The same public cancellation token is legal in a later request.
        let fresh =
            OperationControl::with_contention_policy(None, Some(short_contention_policy(1)));
        assert!(
            RequestScope::new(token, &fresh)
                .child_control(None)
                .next_contention_delay()
                .is_some()
        );
    }

    #[tokio::test]
    async fn contention_admission_fail_fast_releases_queue_and_allows_ready_resources() {
        let control =
            OperationControl::with_contention_policy(None, Some(ContentionPolicy::fail_fast()));
        let request = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let semaphore = tokio::sync::Semaphore::new(0);
        let error = wait_admission(
            async { Ok(semaphore.acquire().await.unwrap()) },
            &request,
            &shutdown,
            None,
            &control,
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Busy);
        semaphore.add_permits(1);
        let permit = wait_admission(
            async { Ok(semaphore.acquire().await.unwrap()) },
            &request,
            &shutdown,
            None,
            &control,
        )
        .await
        .unwrap();
        drop(permit);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn contention_admission_and_storage_share_budget_without_recreating_future() {
        let control =
            OperationControl::with_contention_policy(None, Some(short_contention_policy(1)));
        let request = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let mut polls = 0;
        let future = std::future::poll_fn(|_| {
            polls += 1;
            if polls == 1 {
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(Ok(42))
            }
        });
        assert_eq!(
            wait_admission(future, &request, &shutdown, None, &control)
                .await
                .unwrap(),
            42
        );
        assert_eq!(polls, 2);
        let child = RequestScope::new(request.clone(), &control).child_control(None);
        assert_eq!(child.wait_for_contention(None), Some(false));
        assert_eq!(
            wait_admission(
                std::future::pending::<EngineResult<()>>(),
                &request,
                &shutdown,
                None,
                &child
            )
            .await
            .unwrap_err()
            .kind(),
            EngineErrorKind::Busy
        );
    }

    #[tokio::test]
    async fn contention_admission_is_bounded_and_cancellation_wakes_long_async_backoff() {
        let request = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let control =
            OperationControl::with_contention_policy(None, Some(short_contention_policy(2)));
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            wait_admission(
                std::future::pending::<EngineResult<()>>(),
                &request,
                &shutdown,
                None,
                &control,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Busy);
        assert_eq!(control.next_contention_delay(), None);
        let control =
            OperationControl::with_contention_policy(None, Some(slow_contention_policy()));
        let (result, ()) = tokio::join!(
            wait_admission(
                std::future::pending::<EngineResult<()>>(),
                &request,
                &shutdown,
                None,
                &control
            ),
            async {
                tokio::task::yield_now().await;
                request.cancel();
            },
        );
        assert_eq!(result.unwrap_err().kind(), EngineErrorKind::Cancelled);
    }

    #[tokio::test]
    async fn contention_admission_deadline_precedes_exhaustion_and_legacy_is_unchanged() {
        for deadline in [
            Instant::now() - Duration::from_millis(1),
            Instant::now() + Duration::from_millis(20),
        ] {
            let control = OperationControl::with_contention_policy(
                Some(deadline),
                Some(slow_contention_policy()),
            );
            let error = wait_admission(
                std::future::pending::<EngineResult<()>>(),
                &CancellationToken::new(),
                &CancellationToken::new(),
                Some(deadline),
                &control,
            )
            .await
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
        }
        let control = OperationControl::new(None);
        assert_eq!(
            wait_admission(
                async {
                    tokio::task::yield_now().await;
                    Ok(42)
                },
                &CancellationToken::new(),
                &CancellationToken::new(),
                None,
                &control
            )
            .await
            .unwrap(),
            42
        );
        assert!(control.contention.is_none());
    }

    fn slow_contention_policy() -> ContentionPolicy {
        ContentionPolicy::new(
            Duration::from_secs(5),
            Duration::from_secs(5),
            1,
            ContentionJitter::None,
            10,
            Duration::from_secs(60),
        )
        .unwrap()
    }

    #[test]
    fn contention_policy_is_opt_in_and_shared_by_repeated_handle_arms() {
        assert_eq!(OperationControl::new(None).wait_for_contention(None), None);
        let policy = ContentionPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(1),
            1,
            ContentionJitter::None,
            2,
            Duration::from_secs(5),
        )
        .unwrap();
        let control = OperationControl::with_contention_policy(None, Some(policy));
        for _ in 0..2 {
            control.arm(Arc::new(|| {})).unwrap();
            assert_eq!(control.wait_for_contention(None), Some(true));
            assert_eq!(control.disarm(), None);
        }
        control.arm(Arc::new(|| {})).unwrap();
        assert_eq!(control.wait_for_contention(None), Some(false));
        assert_eq!(control.disarm(), None);
    }

    #[test]
    fn contention_deadline_wins_and_successful_commit_is_not_reported_as_cancelled() {
        let control = OperationControl::with_contention_policy(
            Some(Instant::now() + Duration::from_millis(20)),
            Some(slow_contention_policy()),
        );
        let started = Instant::now();
        assert_eq!(control.wait_for_contention(None), Some(false));
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(control.reason(), Some(CancellationReason::DeadlineExceeded));
        // Exhaustion/deadline never reinterpret an already-known commit.
        assert_eq!(control.complete(Ok(42)).unwrap(), 42);
    }

    #[test]
    fn cancellation_wakes_a_long_backoff_and_standalone_tokens_are_polled() {
        for standalone in [false, true] {
            let metrics = Arc::new(ContentionMetrics::default());
            let control = OperationControl::with_contention_metrics(
                None,
                Some(slow_contention_policy()),
                Arc::clone(&metrics),
            );
            let token = CancellationToken::new();
            let waiter_control = Arc::clone(&control);
            let waiter_token = token.clone();
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let waiter = std::thread::spawn(move || {
                done_tx
                    .send(waiter_control.wait_for_contention(Some(&waiter_token)))
                    .unwrap();
            });
            let started = Instant::now();
            while !control
                .contention
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .started()
            {
                assert!(
                    started.elapsed() < Duration::from_secs(2),
                    "waiter must reserve a retry before cancellation"
                );
                std::thread::yield_now();
            }
            if standalone {
                token.cancel();
            } else {
                control.request_cancel(CancellationReason::Cancelled);
            }
            assert_eq!(
                done_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
                Some(false)
            );
            waiter.join().unwrap();
            assert_eq!(metrics.snapshot().retries_scheduled(), 1);
            assert!(metrics.snapshot().wait_nanos() > 0);
            assert_eq!(metrics.snapshot().exhausted_budgets(), 0);
        }
    }

    #[test]
    fn public_control_types_are_send_sync_and_have_stable_accessors() {
        assert_send_sync::<CancellationToken>();
        assert_send_sync::<RequestContext>();

        let token = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(1);
        let limits = ResultLimits::new(7, 512).unwrap();
        let context = RequestContext::new()
            .with_cancellation_token(token.clone())
            .with_deadline(deadline)
            .with_result_limits(limits);

        assert!(!context.cancellation_token().is_cancelled());
        assert_eq!(context.deadline(), Some(deadline));
        assert_eq!(context.result_limits(), Some(limits));
    }

    #[tokio::test]
    async fn cancellation_is_sticky_idempotent_and_wakes_every_waiter() {
        let token = CancellationToken::new();
        let observed = Arc::new(AtomicUsize::new(0));
        let mut waiters = Vec::new();
        for _ in 0..8 {
            let token = token.clone();
            let observed = Arc::clone(&observed);
            waiters.push(tokio::spawn(async move {
                token.cancelled().await;
                observed.fetch_add(1, Ordering::SeqCst);
            }));
        }

        assert!(token.cancel());
        assert!(!token.cancel());
        for waiter in waiters {
            waiter.await.unwrap();
        }
        assert_eq!(observed.load(Ordering::SeqCst), 8);
        tokio::time::timeout(Duration::from_millis(10), token.cancelled())
            .await
            .expect("late waiter must observe sticky cancellation");
    }

    #[tokio::test]
    async fn already_elapsed_deadline_wins_a_pending_wait() {
        let request = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let deadline = Instant::now() - Duration::from_millis(1);
        let control = OperationControl::new(Some(deadline));

        let error = wait_pending(
            std::future::pending::<EngineResult<()>>(),
            &request,
            &shutdown,
            Some(deadline),
            &control,
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DeadlineExceeded);
        assert_eq!(control.reason(), Some(CancellationReason::DeadlineExceeded));
    }

    #[test]
    fn timeout_builder_rejects_zero_and_preserves_a_relative_deadline() {
        assert_eq!(
            RequestContext::new()
                .with_timeout(Duration::ZERO)
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );

        let before = Instant::now();
        let context = RequestContext::new()
            .with_timeout(Duration::from_millis(50))
            .unwrap();
        let deadline = context.deadline().unwrap();
        assert!(deadline >= before + Duration::from_millis(50));
    }
}
