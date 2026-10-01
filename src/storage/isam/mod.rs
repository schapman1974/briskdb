//! Original BriskDB indexed-record storage, not a port of another ISAM library.
//!
//! This Unix-only experiment stores fixed-width byte keys and bounded values in
//! an append-only, copy-on-write B+ tree. Shared read batches and exclusive write
//! batches retain descriptors. Readers briefly share the publication lock to
//! capture a root, then traverse immutable pages concurrently with a writer.
//! Writers serialize on a retained sidecar and exclusively lock the data file
//! only while publishing/synchronizing the new root. A read batch reloads the root;
//! it never caches the entire database in memory.
//!
//! SQLite remains BriskDB's normal backend. These primitives are not yet wired
//! into SQL, documents, catalogs, or the Python/wire interfaces. The format is
//! experimental; there is no migration, compaction, secondary index, or EFS/NFS
//! qualification. Do not use for authoritative data. Never unlink/replace a live
//! file, bypass its locks, or reuse a handle inherited across `fork`.
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use briskdb::isam::{Layout, Mutation, Store};
//! # let directory = tempfile::tempdir()?;
//! let mut store = Store::create(directory.path().join("verses.isam"),
//!                               Layout::new(9, 768)?)?;
//! store.write_batch(&[Mutation::insert(b"JHN003016", b"example verse")])?;
//! let batch = store.read_batch()?;
//! let verses = batch.range(b"JHN003000", Some(b"JHN004000"), 100)?;
//! assert_eq!(verses[0].value, b"example verse");
//! # Ok(()) }
//! ```

#![cfg(unix)]

mod format;
mod locking;
mod tree;

use std::{
    fmt,
    fs::{File, OpenOptions},
    io,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use format::{HEADER_BYTES, Snapshot, read_snapshot, write_snapshot};
use locking::Guard;
pub use locking::LockPolicy;

/// Maximum mutations in a transaction or records returned by one range call.
pub const MAX_BATCH_RECORDS: usize = 4096;

#[derive(Debug, Default)]
struct OperationCounters {
    file_opens: AtomicU64,
    file_closes: AtomicU64,
    file_stats: AtomicU64,
    root_reads: AtomicU64,
    root_writes: AtomicU64,
    page_reads: AtomicU64,
    page_writes: AtomicU64,
    root_read_ns: AtomicU64,
    root_write_ns: AtomicU64,
    page_read_ns: AtomicU64,
    page_write_ns: AtomicU64,
    syncs: AtomicU64,
    sync_ns: AtomicU64,
    publication_ns: AtomicU64,
    lock_requests: AtomicU64,
    lock_retries: AtomicU64,
    lock_wait_ns: AtomicU64,
}

/// Logical operation counts and selected phase timings from this store handle.
///
/// These measure application-level calls and durations, not operating-system
/// syscall totals or NFS RPCs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OperationStats {
    pub file_opens: u64,
    pub file_closes: u64,
    pub file_stats: u64,
    pub root_reads: u64,
    pub root_writes: u64,
    pub page_reads: u64,
    pub page_writes: u64,
    pub root_read_ns: u64,
    pub root_write_ns: u64,
    pub page_read_ns: u64,
    pub page_write_ns: u64,
    pub syncs: u64,
    pub sync_ns: u64,
    pub publication_ns: u64,
    pub lock_requests: u64,
    pub lock_retries: u64,
    pub lock_wait_ns: u64,
}

impl OperationCounters {
    fn snapshot(&self) -> OperationStats {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        OperationStats {
            file_opens: load(&self.file_opens),
            file_closes: load(&self.file_closes),
            file_stats: load(&self.file_stats),
            root_reads: load(&self.root_reads),
            root_writes: load(&self.root_writes),
            page_reads: load(&self.page_reads),
            page_writes: load(&self.page_writes),
            root_read_ns: load(&self.root_read_ns),
            root_write_ns: load(&self.root_write_ns),
            page_read_ns: load(&self.page_read_ns),
            page_write_ns: load(&self.page_write_ns),
            syncs: load(&self.syncs),
            sync_ns: load(&self.sync_ns),
            publication_ns: load(&self.publication_ns),
            lock_requests: load(&self.lock_requests),
            lock_retries: load(&self.lock_retries),
            lock_wait_ns: load(&self.lock_wait_ns),
        }
    }

    fn reset(&self) {
        for counter in [
            &self.file_opens,
            &self.file_closes,
            &self.file_stats,
            &self.root_reads,
            &self.root_writes,
            &self.page_reads,
            &self.page_writes,
            &self.root_read_ns,
            &self.root_write_ns,
            &self.page_read_ns,
            &self.page_write_ns,
            &self.syncs,
            &self.sync_ns,
            &self.publication_ns,
            &self.lock_requests,
            &self.lock_retries,
            &self.lock_wait_ns,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone)]
pub struct OperationStatsHandle(Arc<OperationCounters>);

impl OperationStatsHandle {
    pub fn snapshot(&self) -> OperationStats {
        self.0.snapshot()
    }
}

/// Errors distinguish pre-commit failures from an uncertain publication outcome.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Invalid(&'static str),
    Corrupt(&'static str),
    Busy,
    Duplicate,
    ReadOnly,
    WrongProcess,
    /// Root publication was attempted, but its durable outcome is unknown.
    /// Reopen/reconcile by key; do not blindly retry non-idempotent work.
    CommitUnknown(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "ISAM I/O: {e}"),
            Self::Invalid(s) => write!(f, "invalid ISAM request: {s}"),
            Self::Corrupt(s) => write!(f, "invalid ISAM storage: {s}"),
            Self::Busy => f.write_str("ISAM batch lock deadline reached"),
            Self::Duplicate => f.write_str("ISAM key already exists"),
            Self::ReadOnly => f.write_str("ISAM handle is read-only"),
            Self::WrongProcess => f.write_str("reopen ISAM handles after fork"),
            Self::CommitUnknown(e) => write!(f, "ISAM commit outcome is unknown: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) | Self::CommitUnknown(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Persisted record sizes. Keys sort lexicographically as unsigned bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    key_bytes: u16,
    max_value_bytes: u16,
}

impl Layout {
    pub fn new(key_bytes: u16, max_value_bytes: u16) -> Result<Self> {
        if !(1..=128).contains(&key_bytes) || max_value_bytes > 1024 {
            return Err(Error::Invalid(
                "key width must be 1..=128; value bound must be <=1024",
            ));
        }
        Ok(Self {
            key_bytes,
            max_value_bytes,
        })
    }

    pub const fn key_bytes(self) -> u16 {
        self.key_bytes
    }
    pub const fn max_value_bytes(self) -> u16 {
        self.max_value_bytes
    }

    fn check_key(self, key: &[u8]) -> Result<()> {
        if key.len() != usize::from(self.key_bytes) {
            return Err(Error::Invalid(
                "key does not match the persisted fixed width",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// Operations run in supplied order; a failed batch publishes none of them.
#[derive(Debug, Clone)]
pub enum Mutation {
    Insert(Record),
    Put(Record),
    /// Deleting an absent key is a no-op.
    Delete(Vec<u8>),
}

impl Mutation {
    pub fn insert(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self::Insert(Record {
            key: key.into(),
            value: value.into(),
        })
    }
    pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self::Put(Record {
            key: key.into(),
            value: value.into(),
        })
    }
    pub fn delete(key: impl Into<Vec<u8>>) -> Self {
        Self::Delete(key.into())
    }

    fn key(&self) -> &[u8] {
        match self {
            Self::Insert(r) | Self::Put(r) => &r.key,
            Self::Delete(k) => k,
        }
    }
}

/// One retained descriptor. Operations require `&mut self` so two batches cannot
/// accidentally convert/unlock the same descriptor's lock in separate threads.
#[derive(Debug)]
pub struct Store {
    file: File,
    writer_lock: File,
    layout: Layout,
    policy: LockPolicy,
    writable: bool,
    owner_pid: u32,
    counters: Arc<OperationCounters>,
}

impl Store {
    /// Create a new file; never adopt, truncate, or convert an existing file.
    /// A failed creation may leave an incomplete file; it is not auto-repaired.
    pub fn create(path: impl AsRef<Path>, layout: Layout) -> Result<Self> {
        let path = path.as_ref();
        let counters = Arc::new(OperationCounters::default());
        let file = options(true).create_new(true).open(path)?;
        counters.file_opens.fetch_add(1, Ordering::Relaxed);
        let guard = Guard::acquire_counted(&file, true, LockPolicy::default(), &counters)?;
        let writer_lock = options(true)
            .create_new(true)
            .open(writer_lock_path(path)?)?;
        counters.file_opens.fetch_add(1, Ordering::Relaxed);
        file.set_len(HEADER_BYTES)?;
        let initial = Snapshot {
            layout,
            generation: 1,
            root: 0,
            end: HEADER_BYTES,
        };
        write_snapshot(&file, initial, &counters)?;
        sync_file(&file, &counters)?;
        sync_file(&writer_lock, &counters)?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        sync_file(&File::open(parent)?, &counters)?;
        drop(guard);
        Ok(Self::from_file(file, writer_lock, layout, true, counters))
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_policy(path, LockPolicy::default())
    }
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_read_only_with_policy(path, LockPolicy::default())
    }

    pub fn open_with_policy(path: impl AsRef<Path>, policy: LockPolicy) -> Result<Self> {
        Self::open_inner(path.as_ref(), true, policy)
    }

    pub fn open_read_only_with_policy(path: impl AsRef<Path>, policy: LockPolicy) -> Result<Self> {
        Self::open_inner(path.as_ref(), false, policy)
    }

    fn open_inner(path: &Path, writable: bool, policy: LockPolicy) -> Result<Self> {
        let file = options(writable).open(path)?;
        let counters = Arc::new(OperationCounters::default());
        counters.file_opens.fetch_add(1, Ordering::Relaxed);
        check_regular(&file, &counters)?;
        let writer_lock = options(writable).open(writer_lock_path(path)?)?;
        counters.file_opens.fetch_add(1, Ordering::Relaxed);
        check_regular(&writer_lock, &counters)?;
        counters.file_stats.fetch_add(1, Ordering::Relaxed);
        if writer_lock.metadata()?.len() != 0 {
            return Err(Error::Corrupt("invalid writer lock file"));
        }
        let guard = Guard::acquire_counted(&file, false, policy, &counters)?;
        let snapshot = read_snapshot(&file, Some(&counters))?;
        if snapshot.root != 0 {
            tree::read_node(&file, snapshot, snapshot.root, &counters)?;
        }
        drop(guard);
        let mut store = Self::from_file(file, writer_lock, snapshot.layout, writable, counters);
        store.policy = policy;
        Ok(store)
    }

    fn from_file(
        file: File,
        writer_lock: File,
        layout: Layout,
        writable: bool,
        counters: Arc<OperationCounters>,
    ) -> Self {
        Self {
            file,
            writer_lock,
            layout,
            writable,
            policy: LockPolicy::default(),
            owner_pid: std::process::id(),
            counters,
        }
    }

    pub const fn layout(&self) -> Layout {
        self.layout
    }

    /// Return application-level counts since the last reset.
    pub fn operation_stats(&self) -> OperationStats {
        self.counters.snapshot()
    }

    /// Retain a read-only view of these counters, including after the store closes.
    pub fn operation_stats_handle(&self) -> OperationStatsHandle {
        OperationStatsHandle(Arc::clone(&self.counters))
    }

    /// Reset application-level counts. This does not reset filesystem caches or
    /// any server-side NFS counters.
    pub fn reset_operation_stats(&mut self) {
        self.counters.reset();
    }

    /// Configure bounded admission retries only. Mutations are never retried.
    pub fn set_lock_policy(&mut self, policy: LockPolicy) {
        self.policy = policy;
    }

    fn check_process(&self) -> Result<()> {
        if self.owner_pid != std::process::id() {
            return Err(Error::WrongProcess);
        }
        Ok(())
    }

    pub fn read_batch(&mut self) -> Result<ReadBatch<'_>> {
        self.check_process()?;
        let guard = Guard::acquire_counted(&self.file, false, self.policy, &self.counters)?;
        let snapshot = self.snapshot()?;
        drop(guard);
        Ok(ReadBatch {
            file: &self.file,
            snapshot,
            owner_pid: self.owner_pid,
            counters: Arc::clone(&self.counters),
        })
    }

    fn snapshot(&self) -> Result<Snapshot> {
        let snapshot = read_snapshot(&self.file, Some(&self.counters))?;
        if snapshot.layout != self.layout {
            return Err(Error::Corrupt("record layout changed"));
        }
        Ok(snapshot)
    }

    /// Publish a single atomic root after all new pages have been synchronized.
    /// Old pages are never modified. Space reclamation is not implemented yet.
    pub fn write_batch(&mut self, mutations: &[Mutation]) -> Result<()> {
        self.write_with_hook(mutations, |_| Ok(()))
    }

    fn write_with_hook(
        &mut self,
        mutations: &[Mutation],
        mut hook: impl FnMut(CommitPoint) -> io::Result<()>,
    ) -> Result<()> {
        self.check_process()?;
        if !self.writable {
            return Err(Error::ReadOnly);
        }
        if mutations.len() > MAX_BATCH_RECORDS {
            return Err(Error::Invalid("write batch exceeds record limit"));
        }
        for m in mutations {
            self.layout.check_key(m.key())?;
            if let Mutation::Insert(r) | Mutation::Put(r) = m {
                if r.value.len() > usize::from(self.layout.max_value_bytes) {
                    return Err(Error::Invalid("value exceeds persisted bound"));
                }
            }
        }
        if mutations.is_empty() {
            return Ok(());
        }
        let _writer = Guard::acquire_counted(&self.writer_lock, true, self.policy, &self.counters)?;
        let root_guard = Guard::acquire_counted(&self.file, false, self.policy, &self.counters)?;
        let mut next = self.snapshot()?;
        drop(root_guard);
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or(Error::Invalid("generation exhausted"))?;
        tree::apply_batch(&self.file, &mut next, mutations, &self.counters)?;
        hook(CommitPoint::PagesWritten)?;
        sync_file(&self.file, &self.counters)?;
        hook(CommitPoint::PagesSynced)?;
        let publication_started = std::time::Instant::now();
        let publication_result = (|| {
            let _publication =
                Guard::acquire_counted(&self.file, true, self.policy, &self.counters)?;
            write_snapshot(&self.file, next, &self.counters).map_err(|e| match e {
                Error::Io(e) => Error::CommitUnknown(e),
                other => other,
            })?;
            hook(CommitPoint::RootWritten).map_err(Error::CommitUnknown)?;
            sync_file(&self.file, &self.counters).map_err(Error::CommitUnknown)?;
            Ok(())
        })();
        self.counters.publication_ns.fetch_add(
            publication_started.elapsed().as_nanos() as u64,
            Ordering::Relaxed,
        );
        publication_result
    }
}

fn sync_file(file: &File, counters: &OperationCounters) -> io::Result<()> {
    let started = std::time::Instant::now();
    let result = file.sync_all();
    counters
        .sync_ns
        .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
    if result.is_ok() {
        counters.syncs.fetch_add(1, Ordering::Relaxed);
    }
    result
}

impl Drop for Store {
    fn drop(&mut self) {
        self.counters.file_closes.fetch_add(2, Ordering::Relaxed);
    }
}

fn writer_lock_path(path: &Path) -> Result<PathBuf> {
    let mut name = path
        .file_name()
        .ok_or(Error::Invalid("missing file name"))?
        .to_os_string();
    name.push(".writer.lock");
    Ok(path.with_file_name(name))
}

fn check_regular(file: &File, counters: &OperationCounters) -> Result<()> {
    counters.file_stats.fetch_add(1, Ordering::Relaxed);
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(Error::Invalid(
            "ISAM files must be regular files without hard-link aliases",
        ));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(Error::Invalid("ISAM files must be owner-only"));
    }
    Ok(())
}

fn options(writable: bool) -> OpenOptions {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(writable)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    options
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitPoint {
    PagesWritten,
    PagesSynced,
    RootWritten,
}

/// An immutable read snapshot. Shared publication ownership has already been
/// released: its pages remain valid while subsequent writers append/commit.
#[derive(Debug)]
pub struct ReadBatch<'a> {
    file: &'a File,
    snapshot: Snapshot,
    owner_pid: u32,
    counters: Arc<OperationCounters>,
}

impl ReadBatch<'_> {
    pub const fn generation(&self) -> u64 {
        self.snapshot.generation
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_process()?;
        self.snapshot.layout.check_key(key)?;
        tree::get(self.file, self.snapshot, key, &self.counters)
    }

    /// Ordered half-open range `[start, end)`, with a hard record limit.
    /// `None` means no upper bound. A subsequent call is in the same snapshot.
    pub fn range(&self, start: &[u8], end: Option<&[u8]>, limit: usize) -> Result<Vec<Record>> {
        self.check_process()?;
        self.snapshot.layout.check_key(start)?;
        if let Some(end) = end {
            self.snapshot.layout.check_key(end)?;
            if start > end {
                return Err(Error::Invalid("reversed range"));
            }
        }
        if limit > MAX_BATCH_RECORDS {
            return Err(Error::Invalid("read limit exceeds record limit"));
        }
        tree::range(self.file, self.snapshot, start, end, limit, &self.counters)
    }

    /// Traverse the live tree and check every parent/child ordering boundary.
    /// This is an offline diagnostic, not part of the normal read hot path.
    pub fn verify(&self) -> Result<u64> {
        self.check_process()?;
        tree::verify(self.file, self.snapshot, &self.counters)
    }

    fn check_process(&self) -> Result<()> {
        if self.owner_pid != std::process::id() {
            return Err(Error::WrongProcess);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
