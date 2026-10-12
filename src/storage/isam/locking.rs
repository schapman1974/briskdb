use super::{Error, OperationCounters, Result, options};
use std::{
    collections::HashMap,
    fs::File,
    io,
    os::{
        fd::AsRawFd,
        unix::fs::{FileExt, MetadataExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak, atomic::Ordering},
    time::{Duration, Instant},
};

/// Fixed, versioned key-lock stripes. Hash collisions only serialize writers.
pub const KEY_LOCK_STRIPES: usize = 64;
// Reserved byte immediately after the key stripes. The root recheck remains
// mandatory: older writers do not participate in this performance-only gate.
const COMMIT_LOCK_SLOT: usize = KEY_LOCK_STRIPES;
const LOCK_TABLE_OFFSET: i64 = 4096;
const LOCK_HEADER_BYTES: usize = 4096;
const LOCK_MAGIC: &[u8; 8] = b"BRILOCK1";

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

    pub(crate) fn deadline(self) -> LockDeadline {
        LockDeadline(Instant::now() + self.timeout)
    }

    pub(crate) const fn interval(self) -> Duration {
        self.interval
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LockDeadline(Instant);

impl LockDeadline {
    fn remaining(self) -> Option<Duration> {
        self.0.checked_duration_since(Instant::now())
    }

    pub(super) fn wait(self, interval: Duration) -> Result<()> {
        let Some(remaining) = self.remaining() else {
            return Err(Error::Busy);
        };
        if remaining.is_zero() {
            return Err(Error::Busy);
        }
        std::thread::sleep(remaining.min(interval));
        Ok(())
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
        Self::acquire_inner(file, exclusive, policy.deadline(), policy.interval, None)
    }

    pub(super) fn acquire_counted(
        file: &'a File,
        exclusive: bool,
        policy: LockPolicy,
        counters: &OperationCounters,
    ) -> Result<Self> {
        Self::acquire_inner(
            file,
            exclusive,
            policy.deadline(),
            policy.interval(),
            Some(counters),
        )
    }

    pub(super) fn acquire_until(
        file: &'a File,
        exclusive: bool,
        deadline: LockDeadline,
        interval: Duration,
        counters: &OperationCounters,
    ) -> Result<Self> {
        Self::acquire_inner(file, exclusive, deadline, interval, Some(counters))
    }

    fn acquire_inner(
        file: &'a File,
        exclusive: bool,
        deadline: LockDeadline,
        interval: Duration,
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
            if !lock_contended(&error) {
                return Err(error.into());
            }
            if let Some(counters) = counters {
                counters.lock_retries.fetch_add(1, Ordering::Relaxed);
            }
            if let Err(error) = deadline.wait(interval) {
                if let Some(counters) = counters {
                    counters
                        .lock_wait_ns
                        .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
                return Err(error);
            }
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

#[derive(Debug)]
struct LockIdentity {
    device: u64,
    inode: u64,
}

#[derive(Debug)]
pub(super) struct KeyLockFile {
    file: File,
    path: PathBuf,
    identity: LockIdentity,
    owner_pid: u32,
    local: [Mutex<()>; KEY_LOCK_STRIPES],
    commit: Mutex<()>,
    durability: DurabilityBarrier,
    open_counters: Arc<OperationCounters>,
}

static LOCK_FILES: OnceLock<Mutex<HashMap<PathBuf, Weak<KeyLockFile>>>> = OnceLock::new();

impl KeyLockFile {
    pub(super) fn create(path: &Path, counters: Arc<OperationCounters>) -> Result<Arc<Self>> {
        let path = normalize_path(path)?;
        let registry = LOCK_FILES.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry = registry.lock().map_err(|_| Error::LockRegistryPoisoned)?;
        prune_registry(&mut registry);
        if registry.get(&path).and_then(Weak::upgrade).is_some() {
            return Err(Error::Invalid("ISAM lock file is already live"));
        }
        let file = options(true).create_new(true).open(&path)?;
        counters.file_opens.fetch_add(1, Ordering::Relaxed);
        let lock_file = Arc::new(Self::from_new_file(
            path.clone(),
            file,
            Arc::clone(&counters),
        )?);
        registry.insert(path, Arc::downgrade(&lock_file));
        Ok(lock_file)
    }

    pub(super) fn open(
        path: &Path,
        counters: Arc<OperationCounters>,
        deadline: LockDeadline,
        interval: Duration,
    ) -> Result<Arc<Self>> {
        let path = normalize_path(path)?;
        let registry = LOCK_FILES.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry = registry.lock().map_err(|_| Error::LockRegistryPoisoned)?;
        prune_registry(&mut registry);
        match registry.get(&path).and_then(Weak::upgrade) {
            Some(existing) if existing.owner_pid == std::process::id() => {
                existing.validate_path()?;
                return Ok(existing);
            }
            _ => {}
        }

        let file = options(true).open(&path)?;
        counters.file_opens.fetch_add(1, Ordering::Relaxed);
        let lock_file = Arc::new(Self::from_open_file(
            path.clone(),
            file,
            Arc::clone(&counters),
            deadline,
            interval,
            &counters,
        )?);
        registry.insert(path, Arc::downgrade(&lock_file));
        Ok(lock_file)
    }

    pub(super) fn open_or_create(
        path: &Path,
        legacy_writer_file: &File,
        counters: Arc<OperationCounters>,
        deadline: LockDeadline,
        interval: Duration,
    ) -> Result<Arc<Self>> {
        match Self::open(path, Arc::clone(&counters), deadline, interval) {
            Ok(lock_file) => Ok(lock_file),
            Err(Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                let _legacy_fence =
                    Guard::acquire_until(legacy_writer_file, true, deadline, interval, &counters)?;
                match Self::open(path, Arc::clone(&counters), deadline, interval) {
                    Ok(lock_file) => Ok(lock_file),
                    Err(Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                        Self::create(path, counters)
                    }
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    fn from_new_file(path: PathBuf, file: File, counters: Arc<OperationCounters>) -> Result<Self> {
        let identity = check_lock_file(&file, &counters)?;
        let _guard = Guard::acquire_counted(&file, true, LockPolicy::default(), &counters)?;
        let mut header = [0; LOCK_HEADER_BYTES];
        header[..8].copy_from_slice(LOCK_MAGIC);
        header[8..10].copy_from_slice(&1_u16.to_le_bytes());
        header[10..12].copy_from_slice(&(KEY_LOCK_STRIPES as u16).to_le_bytes());
        header[12..16].copy_from_slice(&1_u32.to_le_bytes());
        file.set_len(LOCK_HEADER_BYTES as u64)?;
        file.write_all_at(&header, 0)?;
        counters.sync_requests.fetch_add(1, Ordering::Relaxed);
        file.sync_all()?;
        counters.syncs.fetch_add(1, Ordering::Relaxed);
        drop(_guard);
        Ok(Self {
            file,
            path,
            identity,
            owner_pid: std::process::id(),
            local: std::array::from_fn(|_| Mutex::new(())),
            commit: Mutex::new(()),
            durability: DurabilityBarrier::default(),
            open_counters: counters,
        })
    }

    fn from_open_file(
        path: PathBuf,
        file: File,
        counters: Arc<OperationCounters>,
        deadline: LockDeadline,
        interval: Duration,
        operation_counters: &OperationCounters,
    ) -> Result<Self> {
        let identity = check_lock_file(&file, operation_counters)?;
        let guard = Guard::acquire_until(&file, true, deadline, interval, operation_counters)?;
        let mut header = [0; LOCK_HEADER_BYTES];
        let result = file.read_exact_at(&mut header, 0);
        if let Err(error) = result {
            if error.kind() != io::ErrorKind::UnexpectedEof {
                return Err(error.into());
            }
            if file.metadata()?.len() != 0 {
                return Err(Error::Corrupt("truncated ISAM lock header"));
            }
            header[..8].copy_from_slice(LOCK_MAGIC);
            header[8..10].copy_from_slice(&1_u16.to_le_bytes());
            header[10..12].copy_from_slice(&(KEY_LOCK_STRIPES as u16).to_le_bytes());
            header[12..16].copy_from_slice(&1_u32.to_le_bytes());
            file.set_len(LOCK_HEADER_BYTES as u64)?;
            file.write_all_at(&header, 0)?;
            operation_counters
                .sync_requests
                .fetch_add(1, Ordering::Relaxed);
            file.sync_all()?;
            operation_counters.syncs.fetch_add(1, Ordering::Relaxed);
        }
        operation_counters
            .file_stats
            .fetch_add(1, Ordering::Relaxed);
        if &header[..8] != LOCK_MAGIC
            || u16::from_le_bytes(header[8..10].try_into().unwrap()) != 1
            || u16::from_le_bytes(header[10..12].try_into().unwrap()) as usize != KEY_LOCK_STRIPES
            || u32::from_le_bytes(header[12..16].try_into().unwrap()) != 1
            || header[16..].iter().any(|byte| *byte != 0)
            || file.metadata()?.len() != LOCK_HEADER_BYTES as u64
        {
            return Err(Error::Corrupt("unknown or invalid ISAM lock-table format"));
        }
        drop(guard);
        Ok(Self {
            file,
            path,
            identity,
            owner_pid: std::process::id(),
            local: std::array::from_fn(|_| Mutex::new(())),
            commit: Mutex::new(()),
            durability: DurabilityBarrier::default(),
            open_counters: counters,
        })
    }

    fn validate_path(&self) -> Result<()> {
        self.open_counters
            .file_stats
            .fetch_add(1, Ordering::Relaxed);
        let metadata = std::fs::symlink_metadata(&self.path)?;
        if !metadata.file_type().is_file()
            || metadata.nlink() != 1
            || metadata.dev() != self.identity.device
            || metadata.ino() != self.identity.inode
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(Error::Invalid("ISAM lock file was replaced while live"));
        }
        Ok(())
    }

    /// Coalesce fsync only among descriptors in THIS process, on the same
    /// inode. A ticket is registered after its data/root write completes;
    /// fsync covers only tickets captured before that particular sync began.
    /// This is not a cross-host flush service or an acknowledgement shortcut.
    pub(super) fn synchronize(&self, file: &File, counters: &OperationCounters) -> io::Result<()> {
        counters.sync_requests.fetch_add(1, Ordering::Relaxed);
        self.durability
            .run(|| super::sync_file_physical(file, counters))
    }

    /// Called under the exclusive writer/recovery fence, with no active writes.
    pub(super) fn reset_durability(&self) -> io::Result<()> {
        let mut state = self
            .durability
            .state
            .lock()
            .map_err(|_| io::Error::other("poisoned durability barrier"))?;
        if state.flushing {
            return Err(io::Error::other("durability barrier still active"));
        }
        *state = FlushState::default();
        Ok(())
    }

    /// Coordinate durable publication without excluding snapshot readers or
    /// taking another file descriptor (closing one could release POSIX locks).
    pub(super) fn acquire_commit(
        &self,
        deadline: LockDeadline,
        interval: Duration,
        counters: &OperationCounters,
    ) -> Result<KeyStripeGuard<'_>> {
        if self.owner_pid != std::process::id() {
            return Err(Error::WrongProcess);
        }
        let started = Instant::now();
        let result = (|| {
            let local = loop {
                match self.commit.try_lock() {
                    Ok(guard) => break guard,
                    Err(std::sync::TryLockError::Poisoned(_)) => {
                        return Err(Error::LockRegistryPoisoned);
                    }
                    Err(std::sync::TryLockError::WouldBlock) => {
                        counters.lock_retries.fetch_add(1, Ordering::Relaxed);
                        deadline.wait(interval)?;
                    }
                }
            };
            loop {
                counters.lock_requests.fetch_add(1, Ordering::Relaxed);
                match set_range_lock(&self.file, COMMIT_LOCK_SLOT, true) {
                    Ok(()) => break,
                    Err(error) if lock_contended(&error) => {
                        counters.lock_retries.fetch_add(1, Ordering::Relaxed);
                        deadline.wait(interval)?;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(KeyStripeGuard {
                file: &self.file,
                stripes: vec![COMMIT_LOCK_SLOT],
                _local_guards: vec![local],
                owner_pid: std::process::id(),
            })
        })();
        let elapsed = started.elapsed().as_nanos() as u64;
        counters.lock_wait_ns.fetch_add(elapsed, Ordering::Relaxed);
        counters
            .commit_lock_wait_ns
            .fetch_add(elapsed, Ordering::Relaxed);
        result
    }

    pub(super) fn acquire_stripes<'a>(
        &'a self,
        keys: &[&[u8]],
        deadline: LockDeadline,
        interval: Duration,
        counters: &OperationCounters,
    ) -> Result<KeyStripeGuard<'a>> {
        if self.owner_pid != std::process::id() {
            return Err(Error::WrongProcess);
        }
        let mut stripes: Vec<_> = keys.iter().map(|key| key_lock_stripe(key)).collect();
        stripes.sort_unstable();
        stripes.dedup();

        let started = Instant::now();
        counters.write_lock_batches.fetch_add(1, Ordering::Relaxed);
        counters
            .write_lock_keys
            .fetch_add(keys.len() as u64, Ordering::Relaxed);
        let mut local_guards = Vec::with_capacity(stripes.len());
        let mut local_wait_ns = Vec::with_capacity(stripes.len());
        for stripe in &stripes {
            let stripe_started = Instant::now();
            loop {
                match self.local[*stripe].try_lock() {
                    Ok(guard) => {
                        let waited = stripe_started.elapsed().as_nanos() as u64;
                        local_guards.push(guard);
                        local_wait_ns.push(waited);
                        counters
                            .write_lock_local_wait_ns
                            .fetch_add(waited, Ordering::Relaxed);
                        break;
                    }
                    Err(std::sync::TryLockError::Poisoned(_)) => {
                        return Err(Error::LockRegistryPoisoned);
                    }
                    Err(std::sync::TryLockError::WouldBlock) => {
                        counters.lock_retries.fetch_add(1, Ordering::Relaxed);
                        counters.write_lock_retries.fetch_add(1, Ordering::Relaxed);
                        counters
                            .write_lock_local_retries
                            .fetch_add(1, Ordering::Relaxed);
                        counters.write_lock_stripe_retries[*stripe].fetch_add(1, Ordering::Relaxed);
                        if let Err(error) = deadline.wait(interval) {
                            let waited = stripe_started.elapsed().as_nanos() as u64;
                            counters
                                .lock_wait_ns
                                .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            counters
                                .write_lock_wait_ns
                                .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            counters
                                .write_lock_local_wait_ns
                                .fetch_add(waited, Ordering::Relaxed);
                            counters.write_lock_stripe_wait_ns[*stripe]
                                .fetch_add(waited, Ordering::Relaxed);
                            return Err(error);
                        }
                    }
                }
            }
        }

        let mut held = Vec::with_capacity(stripes.len());
        for (index, stripe) in stripes.iter().enumerate() {
            let range_started = Instant::now();
            loop {
                counters.lock_requests.fetch_add(1, Ordering::Relaxed);
                counters.write_lock_requests.fetch_add(1, Ordering::Relaxed);
                if set_range_lock(&self.file, *stripe, true).is_ok() {
                    let range_waited = range_started.elapsed().as_nanos() as u64;
                    held.push(*stripe);
                    counters
                        .write_lock_stripes_acquired
                        .fetch_add(1, Ordering::Relaxed);
                    counters.write_lock_stripe_acquisitions[*stripe]
                        .fetch_add(1, Ordering::Relaxed);
                    counters
                        .write_lock_range_wait_ns
                        .fetch_add(range_waited, Ordering::Relaxed);
                    counters.write_lock_stripe_wait_ns[*stripe].fetch_add(
                        local_wait_ns[index].saturating_add(range_waited),
                        Ordering::Relaxed,
                    );
                    break;
                }
                let error = io::Error::last_os_error();
                if !lock_contended(&error) {
                    unlock_ranges(&self.file, &held);
                    return Err(error.into());
                }
                counters.lock_retries.fetch_add(1, Ordering::Relaxed);
                counters.write_lock_retries.fetch_add(1, Ordering::Relaxed);
                counters
                    .write_lock_range_retries
                    .fetch_add(1, Ordering::Relaxed);
                counters.write_lock_stripe_retries[*stripe].fetch_add(1, Ordering::Relaxed);
                if let Err(wait_error) = deadline.wait(interval) {
                    unlock_ranges(&self.file, &held);
                    let waited = started.elapsed().as_nanos() as u64;
                    let range_waited = range_started.elapsed().as_nanos() as u64;
                    counters.lock_wait_ns.fetch_add(waited, Ordering::Relaxed);
                    counters
                        .write_lock_wait_ns
                        .fetch_add(waited, Ordering::Relaxed);
                    counters
                        .write_lock_range_wait_ns
                        .fetch_add(range_waited, Ordering::Relaxed);
                    counters.write_lock_stripe_wait_ns[*stripe].fetch_add(
                        local_wait_ns[index].saturating_add(range_waited),
                        Ordering::Relaxed,
                    );
                    return Err(wait_error);
                }
            }
        }
        let waited = started.elapsed().as_nanos() as u64;
        counters.lock_wait_ns.fetch_add(waited, Ordering::Relaxed);
        counters
            .write_lock_wait_ns
            .fetch_add(waited, Ordering::Relaxed);
        Ok(KeyStripeGuard {
            file: &self.file,
            stripes: held,
            _local_guards: local_guards,
            owner_pid: std::process::id(),
        })
    }
}

#[derive(Debug, Default)]
struct FlushState {
    requested: u64,
    completed: u64,
    flushing: bool,
    failed: Option<(io::ErrorKind, String)>,
}

#[derive(Debug, Default)]
struct DurabilityBarrier {
    state: Mutex<FlushState>,
    changed: Condvar,
}

impl DurabilityBarrier {
    fn run(&self, flush: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("poisoned durability barrier"))?;
        state.requested = state
            .requested
            .checked_add(1)
            .ok_or_else(|| io::Error::other("durability ticket exhausted"))?;
        let ticket = state.requested;
        loop {
            if let Some((kind, message)) = &state.failed {
                return Err(io::Error::new(*kind, message.clone()));
            }
            if state.completed >= ticket {
                return Ok(());
            }
            if state.flushing {
                state = self
                    .changed
                    .wait(state)
                    .map_err(|_| io::Error::other("poisoned durability barrier"))?;
                continue;
            }
            let through = state.requested;
            state.flushing = true;
            drop(state);
            let result = flush();
            state = self
                .state
                .lock()
                .map_err(|_| io::Error::other("poisoned durability barrier"))?;
            state.flushing = false;
            match &result {
                Ok(()) => state.completed = through,
                Err(error) => state.failed = Some((error.kind(), error.to_string())),
            }
            self.changed.notify_all();
            return result;
        }
    }
}

impl Drop for KeyLockFile {
    fn drop(&mut self) {
        self.open_counters
            .file_closes
            .fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub(super) struct KeyStripeGuard<'a> {
    file: &'a File,
    stripes: Vec<usize>,
    _local_guards: Vec<MutexGuard<'a, ()>>,
    owner_pid: u32,
}

impl Drop for KeyStripeGuard<'_> {
    fn drop(&mut self) {
        if self.owner_pid == std::process::id() {
            unlock_ranges(self.file, &self.stripes);
        }
    }
}

fn set_range_lock(file: &File, stripe: usize, exclusive: bool) -> io::Result<()> {
    // SAFETY: a zeroed flock is initialized below before passing its pointer.
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = if exclusive {
        libc::F_WRLCK as _
    } else {
        libc::F_RDLCK as _
    };
    lock.l_whence = libc::SEEK_SET as _;
    lock.l_start = LOCK_TABLE_OFFSET + stripe as i64;
    lock.l_len = 1;
    // SAFETY: fcntl reads the initialized structure synchronously.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &lock) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn unlock_ranges(file: &File, stripes: &[usize]) {
    for stripe in stripes.iter().rev() {
        // SAFETY: a zeroed flock is initialized below before passing its pointer.
        let mut lock: libc::flock = unsafe { std::mem::zeroed() };
        lock.l_type = libc::F_UNLCK as _;
        lock.l_whence = libc::SEEK_SET as _;
        lock.l_start = LOCK_TABLE_OFFSET + *stripe as i64;
        lock.l_len = 1;
        // SAFETY: fcntl reads the initialized structure synchronously.
        unsafe {
            libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &lock);
        }
    }
}

pub(super) fn key_lock_stripe(key: &[u8]) -> usize {
    let hash = key.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    });
    hash as usize % KEY_LOCK_STRIPES
}

fn lock_contended(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock
        || error.kind() == io::ErrorKind::Interrupted
        || error.raw_os_error() == Some(libc::EACCES)
}

fn check_lock_file(file: &File, counters: &OperationCounters) -> Result<LockIdentity> {
    counters.file_stats.fetch_add(1, Ordering::Relaxed);
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 || metadata.permissions().mode() & 0o077 != 0 {
        return Err(Error::Invalid(
            "ISAM lock files must be owner-only regular files without aliases",
        ));
    }
    Ok(LockIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

fn normalize_path(path: &Path) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .ok_or(Error::Invalid("missing file name"))?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    Ok(std::fs::canonicalize(parent)?.join(file_name))
}

fn prune_registry(registry: &mut HashMap<PathBuf, Weak<KeyLockFile>>) {
    registry.retain(|_, entry| entry.strong_count() > 0);
}

#[cfg(test)]
mod durability_tests {
    use super::*;
    use std::{
        sync::{atomic::AtomicUsize, mpsc},
        thread,
    };

    fn wait_for_tickets(barrier: &DurabilityBarrier, count: u64) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while barrier.state.lock().unwrap().requested != count {
            assert!(Instant::now() < deadline, "flush waiters did not register");
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn coalescing_never_acks_tickets_registered_after_a_flush_started() {
        let barrier = DurabilityBarrier::default();
        let calls = AtomicUsize::new(0);
        let (started_tx, started_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        thread::scope(|scope| {
            let barrier_ref = &barrier;
            let calls_ref = &calls;
            let leader = scope.spawn(move || {
                barrier_ref.run(|| {
                    calls_ref.fetch_add(1, Ordering::SeqCst);
                    started_tx.send(()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(())
                })
            });
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let followers = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.run(|| {
                            calls.fetch_add(1, Ordering::SeqCst);
                            assert_eq!(barrier.state.lock().unwrap().completed, 1);
                            Ok(())
                        })
                    })
                })
                .collect::<Vec<_>>();
            wait_for_tickets(&barrier, 9);
            assert_eq!(barrier.state.lock().unwrap().completed, 0);
            resume_tx.send(()).unwrap();
            leader.join().unwrap().unwrap();
            for follower in followers {
                follower.join().unwrap().unwrap();
            }
        });
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(barrier.state.lock().unwrap().completed, 9);
    }

    #[test]
    fn shared_flush_failure_wakes_every_waiter_without_acknowledging_any() {
        let barrier = DurabilityBarrier::default();
        let (started_tx, started_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        thread::scope(|scope| {
            let barrier_ref = &barrier;
            let leader = scope.spawn(move || {
                barrier_ref.run(|| {
                    started_tx.send(()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    Err(io::Error::other("injected sync failure"))
                })
            });
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let followers = (0..8)
                .map(|_| scope.spawn(|| barrier.run(|| panic!("must not flush after failure"))))
                .collect::<Vec<_>>();
            wait_for_tickets(&barrier, 9);
            resume_tx.send(()).unwrap();
            assert!(leader.join().unwrap().is_err());
            for follower in followers {
                assert!(follower.join().unwrap().is_err());
            }
        });
        assert_eq!(barrier.state.lock().unwrap().completed, 0);
        assert!(
            barrier
                .run(|| panic!("must stay failed until exclusive recovery"))
                .is_err()
        );
    }
}
