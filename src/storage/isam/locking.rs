use super::{Error, Result};
use std::{
    fs::File,
    io,
    os::fd::AsRawFd,
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
    pub(crate) fn acquire(file: &'a File, exclusive: bool, policy: LockPolicy) -> Result<Self> {
        let operation = if exclusive {
            libc::LOCK_EX
        } else {
            libc::LOCK_SH
        } | libc::LOCK_NB;
        let started = Instant::now();
        loop {
            // SAFETY: the borrowed descriptor is live; flock retains no pointer.
            if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
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
            let Some(left) = policy.timeout.checked_sub(started.elapsed()) else {
                return Err(Error::Busy);
            };
            if left.is_zero() {
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
