use super::{Error, OperationCounters, Result};
use std::{
    fs::File,
    io,
    os::fd::AsRawFd,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

/// Retry only lock admission, with a finite deadline. Defaults to fail-fast.
#[derive(Debug, Clone, Copy)]
pub struct LockPolicy {
    timeout: Duration,
    interval: Duration,
}

impl Default for LockPolicy {
    fn default() -> Self {
        Self {
            timeout: Duration::ZERO,
            interval: Duration::from_millis(5),
        }
    }
}

impl LockPolicy {
    pub fn new(timeout: Duration, interval: Duration) -> Result<Self> {
        if interval.is_zero() || timeout > Duration::from_secs(300) {
            return Err(Error::Invalid(
                "lock interval must be positive and timeout <=300 seconds",
            ));
        }
        Ok(Self { timeout, interval })
    }
}

#[derive(Debug)]
pub(crate) struct Guard<'a> {
    pub(crate) file: &'a File,
    owner_pid: u32,
}

impl<'a> Guard<'a> {
    #[cfg(test)]
    pub(crate) fn acquire(file: &'a File, exclusive: bool, policy: LockPolicy) -> Result<Self> {
        Self::acquire_inner(file, exclusive, policy, None)
    }

    pub(super) fn acquire_counted(
        file: &'a File,
        exclusive: bool,
        policy: LockPolicy,
        counters: &OperationCounters,
    ) -> Result<Self> {
        Self::acquire_inner(file, exclusive, policy, Some(counters))
    }

    fn acquire_inner(
        file: &'a File,
        exclusive: bool,
        policy: LockPolicy,
        counters: Option<&OperationCounters>,
    ) -> Result<Self> {
        let operation = if exclusive {
            libc::LOCK_EX
        } else {
            libc::LOCK_SH
        } | libc::LOCK_NB;
        let started = Instant::now();
        loop {
            if let Some(counters) = counters {
                counters.lock_requests.fetch_add(1, Ordering::Relaxed);
            }
            // SAFETY: the borrowed descriptor is live; flock retains no pointer.
            if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
                if let Some(counters) = counters {
                    counters
                        .lock_wait_ns
                        .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
                return Ok(Self {
                    file,
                    owner_pid: std::process::id(),
                });
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::WouldBlock
                && error.kind() != io::ErrorKind::Interrupted
            {
                return Err(error.into());
            }
            if let Some(counters) = counters {
                counters.lock_retries.fetch_add(1, Ordering::Relaxed);
            }
            let Some(left) = policy.timeout.checked_sub(started.elapsed()) else {
                if let Some(counters) = counters {
                    counters
                        .lock_wait_ns
                        .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
                return Err(Error::Busy);
            };
            if left.is_zero() {
                if let Some(counters) = counters {
                    counters
                        .lock_wait_ns
                        .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
                return Err(Error::Busy);
            }
            std::thread::sleep(left.min(policy.interval));
        }
    }
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        // A forked child must not release its parent's shared open-description lock.
        if self.owner_pid == std::process::id() {
            // SAFETY: the descriptor outlives this guard.
            unsafe {
                libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
            }
        }
    }
}
