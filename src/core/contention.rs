//! Bounded lock-acquisition backoff. This never replays application work.

use std::time::{Duration, Instant};

use super::{EngineError, EngineErrorKind, EngineResult};

const MAX_RETRY_TIME: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_RETRIES: u32 = 1_000_000;

/// Randomization applied to a contention delay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentionJitter {
    /// Use the capped exponential delay unchanged.
    None,
    /// Choose a whole-millisecond delay from 1 through the exponential cap.
    Full,
}

/// Validated, opt-in retry policy for controlled locks and resource admission.
///
/// Retries exclude the initial lock attempt. This policy does not authorize
/// replaying SQL, transactions, bulk commands, or writes with unknown outcomes.
/// The default engine retains its existing contention behavior until explicitly
/// configured with [`super::EngineOptions::with_contention_policy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentionPolicy {
    initial_delay: Duration,
    max_delay: Duration,
    multiplier: u32,
    jitter: ContentionJitter,
    max_retries: u32,
    max_elapsed: Duration,
}

impl ContentionPolicy {
    /// Construct a finite exponential backoff policy.
    ///
    /// Durations must be whole milliseconds between 1 ms and 24 hours;
    /// `initial_delay <= max_delay <= max_elapsed`. The integer multiplier must
    /// be 1..=1024 and retries 1..=1,000,000. Use [`Self::fail_fast`] for zero
    /// retries. Growth saturates at `max_delay`, never overflows.
    pub fn new(
        initial_delay: Duration,
        max_delay: Duration,
        multiplier: u32,
        jitter: ContentionJitter,
        max_retries: u32,
        max_elapsed: Duration,
    ) -> EngineResult<Self> {
        if [initial_delay, max_delay, max_elapsed]
            .into_iter()
            .any(|delay| {
                delay < Duration::from_millis(1)
                    || delay > MAX_RETRY_TIME
                    || delay.subsec_nanos() % 1_000_000 != 0
            })
            || initial_delay > max_delay
            || max_delay > max_elapsed
            || !(1..=1024).contains(&multiplier)
            || !(1..=MAX_RETRIES).contains(&max_retries)
        {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "contention policy requires whole milliseconds in 1..=86400000, \
                 initial delay <= maximum delay <= elapsed limit, multiplier in \
                 1..=1024 and retries in 1..=1000000; use fail_fast for no retries",
            ));
        }
        Ok(Self {
            initial_delay,
            max_delay,
            multiplier,
            jitter,
            max_retries,
            max_elapsed,
        })
    }

    /// Return `Busy` immediately when a controlled lock cannot be acquired.
    pub const fn fail_fast() -> Self {
        Self {
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            multiplier: 1,
            jitter: ContentionJitter::None,
            max_retries: 0,
            max_elapsed: Duration::ZERO,
        }
    }

    /// Return the first retry's delay before jitter.
    pub const fn initial_delay(self) -> Duration {
        self.initial_delay
    }
    /// Return the cap applied before jitter.
    pub const fn max_delay(self) -> Duration {
        self.max_delay
    }
    /// Return the integer exponential multiplier.
    pub const fn multiplier(self) -> u32 {
        self.multiplier
    }
    /// Return the delay randomization strategy.
    pub const fn jitter(self) -> ContentionJitter {
        self.jitter
    }
    /// Return the maximum retries, excluding the initial acquisition attempt.
    pub const fn max_retries(self) -> u32 {
        self.max_retries
    }
    /// Return the wall-clock budget beginning with the first contention.
    pub const fn max_elapsed(self) -> Duration {
        self.max_elapsed
    }
}

/// One budget survives admission, sequential handles, and parallel child tasks.
/// Keep scheduling separate from sleeping so boundary tests need no real clock.
#[derive(Debug)]
pub(crate) struct ContentionBudget {
    policy: ContentionPolicy,
    started: Option<Instant>,
    next_delay: Duration,
    retries: u32,
}

impl ContentionBudget {
    pub(crate) fn new(policy: ContentionPolicy) -> Self {
        Self {
            policy,
            started: None,
            next_delay: policy.initial_delay,
            retries: 0,
        }
    }

    pub(crate) fn next_delay(&mut self, now: Instant, random: u64) -> Option<Duration> {
        let started = *self.started.get_or_insert(now);
        if self.retries >= self.policy.max_retries {
            return None;
        }
        let remaining = self
            .policy
            .max_elapsed
            .saturating_sub(now.duration_since(started));
        let delay = match self.policy.jitter {
            ContentionJitter::None => self.next_delay,
            ContentionJitter::Full => {
                let cap_ms = self.next_delay.as_millis() as u64;
                Duration::from_millis(1 + random % cap_ms)
            }
        };
        // No shortened final sleep followed by an attempt at/after expiry.
        if delay >= remaining {
            return None;
        }
        self.retries += 1;
        self.next_delay = self
            .next_delay
            .saturating_mul(self.policy.multiplier)
            .min(self.policy.max_delay);
        Some(delay)
    }

    pub(crate) fn expired(&self, now: Instant) -> bool {
        self.started
            .is_some_and(|started| now.duration_since(started) >= self.policy.max_elapsed)
    }

    pub(crate) fn jitter(&self) -> ContentionJitter {
        self.policy.jitter
    }

    #[cfg(test)]
    pub(crate) fn started(&self) -> bool {
        self.started.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> ContentionPolicy {
        ContentionPolicy::new(
            Duration::from_millis(2),
            Duration::from_millis(5),
            2,
            ContentionJitter::None,
            4,
            Duration::from_millis(100),
        )
        .unwrap()
    }

    #[test]
    fn capped_growth_counts_only_retries_and_fail_fast_never_waits() {
        let mut budget = ContentionBudget::new(policy());
        let mut now = Instant::now();
        for ms in [2, 4, 5, 5] {
            let delay = budget.next_delay(now, 0).unwrap();
            assert_eq!(delay, Duration::from_millis(ms));
            now += delay;
        }
        assert_eq!(budget.next_delay(now, 0), None);
        assert_eq!(
            ContentionBudget::new(ContentionPolicy::fail_fast()).next_delay(now, 0),
            None
        );
    }

    #[test]
    fn elapsed_time_includes_gaps_and_does_not_reset_after_successful_attempts() {
        let start = Instant::now();
        let mut budget = ContentionBudget::new(policy());
        assert!(budget.next_delay(start, 0).is_some());
        assert!(
            budget
                .next_delay(start + Duration::from_millis(95), 0)
                .is_some()
        );
        assert_eq!(
            budget.next_delay(start + Duration::from_millis(96), 0),
            None
        );
        assert!(!budget.expired(start + Duration::from_millis(99)));
        assert!(budget.expired(start + Duration::from_millis(100)));
    }

    #[test]
    fn jitter_has_positive_inclusive_bounds_and_never_overflows() {
        let mut config = policy();
        config.jitter = ContentionJitter::Full;
        let start = Instant::now();
        for (sample, expected_ms) in [(0, 1), (1, 2), (u64::MAX, 2)] {
            assert_eq!(
                ContentionBudget::new(config).next_delay(start, sample),
                Some(Duration::from_millis(expected_ms))
            );
        }
        let config = ContentionPolicy::new(
            Duration::from_millis(1),
            Duration::from_secs(60),
            1024,
            ContentionJitter::None,
            100,
            MAX_RETRY_TIME,
        )
        .unwrap();
        let mut budget = ContentionBudget::new(config);
        for _ in 0..100 {
            assert!(budget.next_delay(start, 0).unwrap() <= Duration::from_secs(60));
        }
        assert_eq!(budget.next_delay(start, 0), None);
    }

    #[test]
    fn invalid_configuration_is_rejected_without_storage_access() {
        let ms = Duration::from_millis;
        for (initial, max, multiplier, retries, elapsed) in [
            (Duration::ZERO, ms(2), 2, 1, ms(10)),
            (Duration::from_nanos(1_000_001), ms(2), 2, 1, ms(10)),
            (ms(3), ms(2), 2, 1, ms(10)),
            (ms(1), ms(11), 2, 1, ms(10)),
            (ms(1), ms(2), 0, 1, ms(10)),
            (ms(1), ms(2), 1025, 1, ms(10)),
            (ms(1), ms(2), 2, 0, ms(10)),
            (ms(1), ms(2), 2, u32::MAX, ms(10)),
            (ms(1), ms(2), 2, 1, Duration::MAX),
        ] {
            assert_eq!(
                ContentionPolicy::new(
                    initial,
                    max,
                    multiplier,
                    ContentionJitter::None,
                    retries,
                    elapsed
                )
                .unwrap_err()
                .kind(),
                EngineErrorKind::InvalidArgument
            );
        }
    }
}
