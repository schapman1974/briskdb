//! Local-filesystem advisory locks shared by independent BriskDB processes.

use std::{
    fs::{File, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

use crate::{
    core::{EngineError, EngineErrorKind, EngineResult},
    sqlite_error,
};

pub(super) const PROCESS_LEASE_FILE_NAME: &str = ".briskdb-process.lock";
pub(super) const STARTUP_LOCK_FILE_NAME: &str = ".briskdb-startup.lock";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseMode {
    Shared,
    Exclusive,
}

#[derive(Debug)]
struct LeaseFile {
    file: File,
    path: PathBuf,
    mode: LeaseMode,
}

/// One process-local lease shared by every handle for a canonical root.
#[derive(Debug)]
pub(super) struct RootProcessLease {
    inner: Mutex<LeaseFile>,
}

impl RootProcessLease {
    pub(super) fn acquire(root: &Path) -> EngineResult<Self> {
        let path = root.join(PROCESS_LEASE_FILE_NAME);
        let file = open_regular_lock_file(&path)?;
        lock_nonblocking(&file, LockRequest::Shared).map_err(|error| {
            map_lock_error(
                error,
                &path,
                "another process is changing the BriskDB data directory",
            )
        })?;
        Ok(Self {
            inner: Mutex::new(LeaseFile {
                file,
                path,
                mode: LeaseMode::Shared,
            }),
        })
    }

    /// Try to become the only live process using this root.
    ///
    /// `flock` upgrades are not atomic on every supported kernel. If the
    /// upgrade loses a race, restore the lifetime shared lease before
    /// returning the retryable contention error.
    #[allow(dead_code)]
    pub(super) fn try_acquire_exclusive(&self) -> EngineResult<RootMutationGuard<'_>> {
        let mut lease = self.lock_inner()?;
        if lease.mode == LeaseMode::Exclusive {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "cross-process root mutation lease was acquired recursively",
            ));
        }
        match lock_nonblocking(&lease.file, LockRequest::Exclusive) {
            Ok(()) => {
                lease.mode = LeaseMode::Exclusive;
                Ok(RootMutationGuard {
                    lease,
                    downgraded: false,
                })
            }
            Err(error) => {
                // A failed conversion may have dropped this descriptor's
                // shared lock. Re-establish it before local operations resume.
                lock_blocking(&lease.file, LockRequest::Shared).map_err(|restore_error| {
                    sqlite_error::storage_io(
                        restore_error,
                        format!(
                            "failed to restore BriskDB process lease {} after exclusive contention",
                            lease.path.display()
                        ),
                    )
                })?;
                Err(map_lock_error(
                    error,
                    &lease.path,
                    "another BriskDB process has this data directory open",
                ))
            }
        }
    }

    fn lock_inner(&self) -> EngineResult<MutexGuard<'_, LeaseFile>> {
        self.inner.lock().map_err(|error| {
            EngineError::new(
                EngineErrorKind::Internal,
                format!("cross-process root lease is poisoned: {error}"),
            )
        })
    }
}

/// Exclusive root ownership for one schema/catalog/layout mutation.
#[derive(Debug)]
#[must_use = "dropping the guard restores the process's shared root lease"]
pub(super) struct RootMutationGuard<'a> {
    lease: MutexGuard<'a, LeaseFile>,
    downgraded: bool,
}

impl RootMutationGuard<'_> {
    #[allow(dead_code)]
    pub(super) fn downgrade(mut self) -> EngineResult<()> {
        self.restore_shared()?;
        self.downgraded = true;
        Ok(())
    }

    fn restore_shared(&mut self) -> EngineResult<()> {
        lock_blocking(&self.lease.file, LockRequest::Shared).map_err(|error| {
            sqlite_error::storage_io(
                error,
                format!(
                    "failed to restore shared BriskDB process lease {}",
                    self.lease.path.display()
                ),
            )
        })?;
        self.lease.mode = LeaseMode::Shared;
        Ok(())
    }
}

impl Drop for RootMutationGuard<'_> {
    fn drop(&mut self) {
        if !self.downgraded {
            // If this unexpectedly fails, retain the descriptor. That fails
            // closed by continuing to exclude peers for this coordinator's
            // lifetime instead of admitting them during uncertain ownership.
            let _ = self.restore_shared();
        }
    }
}

/// Serializes startup inspection and recovery attempts across processes.
#[derive(Debug)]
pub(super) struct RootStartupGuard {
    _file: File,
}

impl RootStartupGuard {
    #[allow(dead_code)]
    pub(super) fn acquire(root: &Path, timeout: Duration) -> EngineResult<Self> {
        let path = root.join(STARTUP_LOCK_FILE_NAME);
        let file = open_regular_lock_file(&path)?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        loop {
            match lock_nonblocking(&file, LockRequest::Exclusive) {
                Ok(()) => return Ok(Self { _file: file }),
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => {
                    return Err(map_lock_error(
                        error,
                        &path,
                        "another BriskDB process is starting or recovering this data directory",
                    ));
                }
            }
        }
    }
}

fn open_regular_lock_file(path: &Path) -> EngineResult<File> {
    let file = open_lock_file(path).map_err(|error| {
        if error.raw_os_error() == Some(libc::ELOOP) {
            EngineError::from_source(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "BriskDB lock {} must not be a symbolic link",
                    path.display()
                ),
                error,
            )
        } else {
            sqlite_error::storage_io(
                error,
                format!("failed to open BriskDB lock {}", path.display()),
            )
        }
    })?;
    let metadata = file.metadata().map_err(|error| {
        sqlite_error::storage_io(
            error,
            format!("failed to inspect BriskDB lock {}", path.display()),
        )
    })?;
    if !metadata.is_file() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("BriskDB lock {} is not a regular file", path.display()),
        ));
    }
    Ok(file)
}

#[cfg(unix)]
fn open_lock_file(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
}

#[cfg(not(unix))]
fn open_lock_file(_path: &Path) -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "cross-process BriskDB root leases require a supported Unix host",
    ))
}

#[derive(Debug, Clone, Copy)]
enum LockRequest {
    Shared,
    Exclusive,
}

#[cfg(unix)]
fn lock_nonblocking(file: &File, request: LockRequest) -> io::Result<()> {
    let operation = match request {
        LockRequest::Shared => libc::LOCK_SH,
        LockRequest::Exclusive => libc::LOCK_EX,
    } | libc::LOCK_NB;
    flock(file, operation)
}

#[cfg(unix)]
fn lock_blocking(file: &File, request: LockRequest) -> io::Result<()> {
    let operation = match request {
        LockRequest::Shared => libc::LOCK_SH,
        LockRequest::Exclusive => libc::LOCK_EX,
    };
    loop {
        match flock(file, operation) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            result => return result,
        }
    }
}

#[cfg(unix)]
fn flock(file: &File, operation: libc::c_int) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: `file` owns a live descriptor for the duration of this call;
    // `flock` retains neither the descriptor nor a Rust pointer.
    if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn lock_nonblocking(_file: &File, _request: LockRequest) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "cross-process BriskDB root leases require a supported Unix host",
    ))
}

#[cfg(not(unix))]
fn lock_blocking(_file: &File, _request: LockRequest) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "cross-process BriskDB root leases require a supported Unix host",
    ))
}

fn map_lock_error(error: io::Error, path: &Path, busy_message: &str) -> EngineError {
    if error.kind() == io::ErrorKind::WouldBlock {
        EngineError::from_source(EngineErrorKind::Busy, busy_message, error)
    } else {
        sqlite_error::storage_io(
            error,
            format!(
                "failed to lock BriskDB coordination file {}",
                path.display()
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        process::{Command, Stdio},
        thread,
    };

    use tempfile::TempDir;

    use super::*;

    const CHILD_ROOT_ENV: &str = "BRISKDB_PROCESS_LOCK_TEST_ROOT";
    const CHILD_READY_ENV: &str = "BRISKDB_PROCESS_LOCK_TEST_READY";
    const CHILD_RELEASE_ENV: &str = "BRISKDB_PROCESS_LOCK_TEST_RELEASE";
    const CHILD_KIND_ENV: &str = "BRISKDB_PROCESS_LOCK_TEST_KIND";

    #[test]
    fn shared_leases_coexist_and_exclusive_ownership_requires_no_peer() {
        let temp = TempDir::new().unwrap();
        let first = RootProcessLease::acquire(temp.path()).unwrap();
        let second = RootProcessLease::acquire(temp.path()).unwrap();
        assert_eq!(
            first.try_acquire_exclusive().unwrap_err().kind(),
            EngineErrorKind::Busy
        );
        drop(second);
        first.try_acquire_exclusive().unwrap().downgrade().unwrap();
        drop(RootProcessLease::acquire(temp.path()).unwrap());
    }

    #[test]
    fn startup_guards_are_exclusive_and_release_on_drop() {
        let temp = TempDir::new().unwrap();
        let first = RootStartupGuard::acquire(temp.path(), Duration::ZERO).unwrap();
        assert_eq!(
            RootStartupGuard::acquire(temp.path(), Duration::ZERO)
                .unwrap_err()
                .kind(),
            EngineErrorKind::Busy
        );
        drop(first);
        drop(RootStartupGuard::acquire(temp.path(), Duration::ZERO).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn lock_paths_reject_symbolic_links_and_non_files() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let target = temp.path().join("target");
        fs::write(&target, b"not a lock").unwrap();
        symlink(&target, temp.path().join(PROCESS_LEASE_FILE_NAME)).unwrap();
        assert_eq!(
            RootProcessLease::acquire(temp.path()).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );

        let other = TempDir::new().unwrap();
        fs::create_dir(other.path().join(PROCESS_LEASE_FILE_NAME)).unwrap();
        assert_eq!(
            RootProcessLease::acquire(other.path()).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
    }

    #[cfg(unix)]
    #[test]
    fn newly_created_lock_is_owner_only_and_close_on_exec() {
        use std::os::{fd::AsRawFd, unix::fs::PermissionsExt};

        let temp = TempDir::new().unwrap();
        let lease = RootProcessLease::acquire(temp.path()).unwrap();
        let path = temp.path().join(PROCESS_LEASE_FILE_NAME);
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let descriptor = lease.inner.lock().unwrap().file.as_raw_fd();
        // SAFETY: the lease retains this live descriptor for the call.
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        assert_ne!(flags, -1);
        assert_ne!(flags & libc::FD_CLOEXEC, 0);
    }

    #[test]
    fn process_lease_is_shared_across_processes_and_released_after_kill() {
        let temp = TempDir::new().unwrap();
        let ready = temp.path().join("ready");
        let release = temp.path().join("release");
        let mut child = spawn_holder(temp.path(), &ready, &release, "lease");
        wait_for_path(&ready);

        let lease = RootProcessLease::acquire(temp.path()).unwrap();
        assert_eq!(
            lease.try_acquire_exclusive().unwrap_err().kind(),
            EngineErrorKind::Busy
        );
        child.kill().unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(!output.status.success());
        lease.try_acquire_exclusive().unwrap().downgrade().unwrap();
    }

    #[test]
    fn startup_lock_is_visible_across_processes() {
        let temp = TempDir::new().unwrap();
        let ready = temp.path().join("ready");
        let release = temp.path().join("release");
        let child = spawn_holder(temp.path(), &ready, &release, "startup");
        wait_for_path(&ready);

        assert_eq!(
            RootStartupGuard::acquire(temp.path(), Duration::from_millis(10))
                .unwrap_err()
                .kind(),
            EngineErrorKind::Busy
        );
        fs::write(&release, b"release").unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "child failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        drop(RootStartupGuard::acquire(temp.path(), Duration::ZERO).unwrap());
    }

    fn spawn_holder(root: &Path, ready: &Path, release: &Path, kind: &str) -> std::process::Child {
        Command::new(env::current_exe().unwrap())
            .arg("--exact")
            .arg("storage::process_lock::tests::subprocess_lock_holder")
            .arg("--nocapture")
            .env(CHILD_ROOT_ENV, root)
            .env(CHILD_READY_ENV, ready)
            .env(CHILD_RELEASE_ENV, release)
            .env(CHILD_KIND_ENV, kind)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    }

    fn wait_for_path(path: &Path) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !path.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(path.exists(), "child did not publish readiness");
    }

    #[test]
    fn subprocess_lock_holder() {
        let Ok(root) = env::var(CHILD_ROOT_ENV) else {
            return;
        };
        let ready = PathBuf::from(env::var(CHILD_READY_ENV).unwrap());
        let release = PathBuf::from(env::var(CHILD_RELEASE_ENV).unwrap());
        let kind = env::var(CHILD_KIND_ENV).unwrap();
        let _lease = if kind == "lease" {
            Some(RootProcessLease::acquire(Path::new(&root)).unwrap())
        } else {
            None
        };
        let _startup = if kind == "startup" {
            Some(RootStartupGuard::acquire(Path::new(&root), Duration::ZERO).unwrap())
        } else {
            None
        };
        assert!(matches!(kind.as_str(), "lease" | "startup"));
        fs::write(ready, b"ready").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !release.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(release.exists(), "parent did not release child lock holder");
    }
}
