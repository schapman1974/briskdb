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

use super::{ContentionJitter, ContentionPolicy, contention::ContentionBudget};
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
    contention: Option<Mutex<ContentionBudget>>,
    wake_contention: Condvar,
}

impl OperationControl {
    pub(crate) fn new(deadline: Option<Instant>) -> Arc<Self> {
        Self::with_contention_policy(deadline, None)
    }

    pub(crate) fn with_contention_policy(
        deadline: Option<Instant>,
        policy: Option<ContentionPolicy>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(OperationState {
                phase: OperationPhase::Pending,
                reason: None,
                interrupt: None,
            }),
            deadline,
            contention: policy.map(|policy| Mutex::new(ContentionBudget::new(policy))),
            wake_contention: Condvar::new(),
        })
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
        let delay = {
            let mut budget = budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut random = [0; 8];
            if budget.jitter() == ContentionJitter::Full && getrandom::fill(&mut random).is_err() {
                // Entropy failure must not panic inside SQLite's C callback or
                // silently disable the finite policy: stop retrying instead.
                return Some(false);
            }
            budget.next_delay(Instant::now(), u64::from_le_bytes(random))
        };
        let Some(delay) = delay else {
            return Some(false);
        };
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
            let control =
                OperationControl::with_contention_policy(None, Some(slow_contention_policy()));
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
