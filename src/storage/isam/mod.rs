//! Original BriskDB indexed-record storage, not a port of another ISAM library.
//!
//! This Unix-only experiment stores fixed-width byte keys and bounded values in
//! an append-only, copy-on-write B+ tree. Shared read batches and exclusive write
//! batches retain descriptors. Readers briefly share the publication lock to
//! capture a root, then traverse immutable pages concurrently with a writer.
//! In v2/v3 disjoint writers prepare in parallel, then retain a commit gate
//! through writing/syncing. Opt-in v4 pipelines durable prefixes: the staging
//! and publication gates are released before either explicit sync, while
//! conflicting key locks remain held. Working-state handoffs also take a shared
//! data-file lock for NFS cache coherence (the kernel may flush on handoff).
//! A read batch reloads the published root;
//! it never caches the entire database in memory.
//!
//! SQLite remains BriskDB's normal backend. An opt-in hybrid metadata adapter
//! uses this store while retaining SQLite application shards. `NativeCatalog`
//! separately provides typed rows and physical secondary indexes; those native
//! application-data operations are not wired into SQL/documents. The format is
//! experimental; there is no migration, compaction, or EFS/NFS qualification.
//! Do not use for authoritative data. Never unlink or replace a live file,
//! bypass its locks, or reuse a handle inherited across `fork`.
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

mod catalog;
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

pub use catalog::{
    CatalogIdentity, ColumnDefinition, ColumnType, IndexDefinition, NativeCatalog, NativeValue,
    TableDefinition,
};
use format::{Snapshot, read_snapshot, write_snapshot};
use locking::{Guard, KeyLockFile};
pub use locking::{KEY_LOCK_STRIPES, LockPolicy};

/// Maximum mutations in a transaction or records returned by one range call.
pub const MAX_BATCH_RECORDS: usize = 4096;

#[derive(Debug)]
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
    sync_requests: AtomicU64,
    sync_ns: AtomicU64,
    publication_ns: AtomicU64,
    preflight_rebases: AtomicU64,
    publication_retries: AtomicU64,
    commit_lock_wait_ns: AtomicU64,
    lock_requests: AtomicU64,
    lock_retries: AtomicU64,
    lock_wait_ns: AtomicU64,
    write_lock_batches: AtomicU64,
    write_lock_keys: AtomicU64,
    write_lock_requests: AtomicU64,
    write_lock_retries: AtomicU64,
    write_lock_wait_ns: AtomicU64,
    write_lock_local_retries: AtomicU64,
    write_lock_local_wait_ns: AtomicU64,
    write_lock_range_retries: AtomicU64,
    write_lock_range_wait_ns: AtomicU64,
    write_lock_stripes_acquired: AtomicU64,
    write_lock_stripe_acquisitions: [AtomicU64; locking::KEY_LOCK_STRIPES],
    write_lock_stripe_retries: [AtomicU64; locking::KEY_LOCK_STRIPES],
    write_lock_stripe_wait_ns: [AtomicU64; locking::KEY_LOCK_STRIPES],
}

impl Default for OperationCounters {
    fn default() -> Self {
        Self {
            file_opens: AtomicU64::new(0),
            file_closes: AtomicU64::new(0),
            file_stats: AtomicU64::new(0),
            root_reads: AtomicU64::new(0),
            root_writes: AtomicU64::new(0),
            page_reads: AtomicU64::new(0),
            page_writes: AtomicU64::new(0),
            root_read_ns: AtomicU64::new(0),
            root_write_ns: AtomicU64::new(0),
            page_read_ns: AtomicU64::new(0),
            page_write_ns: AtomicU64::new(0),
            syncs: AtomicU64::new(0),
            sync_requests: AtomicU64::new(0),
            sync_ns: AtomicU64::new(0),
            publication_ns: AtomicU64::new(0),
            preflight_rebases: AtomicU64::new(0),
            publication_retries: AtomicU64::new(0),
            commit_lock_wait_ns: AtomicU64::new(0),
            lock_requests: AtomicU64::new(0),
            lock_retries: AtomicU64::new(0),
            lock_wait_ns: AtomicU64::new(0),
            write_lock_batches: AtomicU64::new(0),
            write_lock_keys: AtomicU64::new(0),
            write_lock_requests: AtomicU64::new(0),
            write_lock_retries: AtomicU64::new(0),
            write_lock_wait_ns: AtomicU64::new(0),
            write_lock_local_retries: AtomicU64::new(0),
            write_lock_local_wait_ns: AtomicU64::new(0),
            write_lock_range_retries: AtomicU64::new(0),
            write_lock_range_wait_ns: AtomicU64::new(0),
            write_lock_stripes_acquired: AtomicU64::new(0),
            write_lock_stripe_acquisitions: std::array::from_fn(|_| AtomicU64::new(0)),
            write_lock_stripe_retries: std::array::from_fn(|_| AtomicU64::new(0)),
            write_lock_stripe_wait_ns: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

/// Logical operation counts and selected phase timings from this store handle.
///
/// These measure application-level calls and durations, not operating-system
/// syscall totals or NFS RPCs. Write-lock fields separate striped key-lock
/// activity from the aggregate file-lock counters; per-stripe arrays report
/// stable stripe IDs, never record keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// Durability barriers requested. V4 may share a physical sync among
    /// waiting handles in the same process; this is not cross-host coalescing.
    pub sync_requests: u64,
    pub sync_ns: u64,
    pub publication_ns: u64,
    /// Plans discarded before allocating, writing or syncing pages.
    pub preflight_rebases: u64,
    /// Post-sync root conflicts (possible when interoperating with old writers).
    pub publication_retries: u64,
    /// Commit-gate admission; also included in aggregate lock_wait_ns.
    pub commit_lock_wait_ns: u64,
    pub lock_requests: u64,
    pub lock_retries: u64,
    pub lock_wait_ns: u64,
    /// Write-lock stripe admission counts. Stripe IDs are stable hash stripes,
    /// not record keys; collisions intentionally serialize writes.
    pub write_lock_batches: u64,
    pub write_lock_keys: u64,
    pub write_lock_requests: u64,
    pub write_lock_retries: u64,
    pub write_lock_wait_ns: u64,
    pub write_lock_local_retries: u64,
    pub write_lock_local_wait_ns: u64,
    pub write_lock_range_retries: u64,
    pub write_lock_range_wait_ns: u64,
    pub write_lock_stripes_acquired: u64,
    pub write_lock_stripe_acquisitions: [u64; locking::KEY_LOCK_STRIPES],
    pub write_lock_stripe_retries: [u64; locking::KEY_LOCK_STRIPES],
    pub write_lock_stripe_wait_ns: [u64; locking::KEY_LOCK_STRIPES],
}

impl Default for OperationStats {
    fn default() -> Self {
        Self {
            file_opens: 0,
            file_closes: 0,
            file_stats: 0,
            root_reads: 0,
            root_writes: 0,
            page_reads: 0,
            page_writes: 0,
            root_read_ns: 0,
            root_write_ns: 0,
            page_read_ns: 0,
            page_write_ns: 0,
            syncs: 0,
            sync_requests: 0,
            sync_ns: 0,
            publication_ns: 0,
            preflight_rebases: 0,
            publication_retries: 0,
            commit_lock_wait_ns: 0,
            lock_requests: 0,
            lock_retries: 0,
            lock_wait_ns: 0,
            write_lock_batches: 0,
            write_lock_keys: 0,
            write_lock_requests: 0,
            write_lock_retries: 0,
            write_lock_wait_ns: 0,
            write_lock_local_retries: 0,
            write_lock_local_wait_ns: 0,
            write_lock_range_retries: 0,
            write_lock_range_wait_ns: 0,
            write_lock_stripes_acquired: 0,
            write_lock_stripe_acquisitions: [0; locking::KEY_LOCK_STRIPES],
            write_lock_stripe_retries: [0; locking::KEY_LOCK_STRIPES],
            write_lock_stripe_wait_ns: [0; locking::KEY_LOCK_STRIPES],
        }
    }
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
            sync_requests: load(&self.sync_requests),
            sync_ns: load(&self.sync_ns),
            publication_ns: load(&self.publication_ns),
            preflight_rebases: load(&self.preflight_rebases),
            publication_retries: load(&self.publication_retries),
            commit_lock_wait_ns: load(&self.commit_lock_wait_ns),
            lock_requests: load(&self.lock_requests),
            lock_retries: load(&self.lock_retries),
            lock_wait_ns: load(&self.lock_wait_ns),
            write_lock_batches: load(&self.write_lock_batches),
            write_lock_keys: load(&self.write_lock_keys),
            write_lock_requests: load(&self.write_lock_requests),
            write_lock_retries: load(&self.write_lock_retries),
            write_lock_wait_ns: load(&self.write_lock_wait_ns),
            write_lock_local_retries: load(&self.write_lock_local_retries),
            write_lock_local_wait_ns: load(&self.write_lock_local_wait_ns),
            write_lock_range_retries: load(&self.write_lock_range_retries),
            write_lock_range_wait_ns: load(&self.write_lock_range_wait_ns),
            write_lock_stripes_acquired: load(&self.write_lock_stripes_acquired),
            write_lock_stripe_acquisitions: std::array::from_fn(|stripe| {
                load(&self.write_lock_stripe_acquisitions[stripe])
            }),
            write_lock_stripe_retries: std::array::from_fn(|stripe| {
                load(&self.write_lock_stripe_retries[stripe])
            }),
            write_lock_stripe_wait_ns: std::array::from_fn(|stripe| {
                load(&self.write_lock_stripe_wait_ns[stripe])
            }),
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
            &self.sync_requests,
            &self.sync_ns,
            &self.publication_ns,
            &self.preflight_rebases,
            &self.publication_retries,
            &self.commit_lock_wait_ns,
            &self.lock_requests,
            &self.lock_retries,
            &self.lock_wait_ns,
            &self.write_lock_batches,
            &self.write_lock_keys,
            &self.write_lock_requests,
            &self.write_lock_retries,
            &self.write_lock_wait_ns,
            &self.write_lock_local_retries,
            &self.write_lock_local_wait_ns,
            &self.write_lock_range_retries,
            &self.write_lock_range_wait_ns,
            &self.write_lock_stripes_acquired,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
        for stripe in 0..locking::KEY_LOCK_STRIPES {
            self.write_lock_stripe_acquisitions[stripe].store(0, Ordering::Relaxed);
            self.write_lock_stripe_retries[stripe].store(0, Ordering::Relaxed);
            self.write_lock_stripe_wait_ns[stripe].store(0, Ordering::Relaxed);
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
    LockRegistryPoisoned,
    /// Root publication (or v4 speculative staging) was attempted, but the
    /// durable outcome is unknown; a later writer may publish the staged prefix.
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
            Self::LockRegistryPoisoned => f.write_str("ISAM lock registry is poisoned"),
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
    key_locks: Option<Arc<KeyLockFile>>,
    layout: Layout,
    format_version: u16,
    policy: LockPolicy,
    writable: bool,
    owner_pid: u32,
    counters: Arc<OperationCounters>,
}

impl Store {
    /// Create a new file; never adopt, truncate, or convert an existing file.
    /// A failed creation may leave an incomplete file; it is not auto-repaired.
    pub fn create(path: impl AsRef<Path>, layout: Layout) -> Result<Self> {
        Self::create_inner(path.as_ref(), layout, format::FORMAT_VERSION)
    }

    /// Create an opt-in v3 file with variable-length leaf values (no compression).
    /// Existing v2 files remain unchanged; there is no in-place conversion.
    pub fn create_packed(path: impl AsRef<Path>, layout: Layout) -> Result<Self> {
        Self::create_inner(path.as_ref(), layout, format::PACKED_FORMAT_VERSION)
    }

    /// Create an opt-in v4 packed file with pipelined durable commits. The
    /// staging gate does not span either explicit sync; key locks protect conflicts.
    /// Existing files are never converted. Not yet qualified on NFS/EFS.
    pub fn create_pipelined(path: impl AsRef<Path>, layout: Layout) -> Result<Self> {
        Self::create_inner(path.as_ref(), layout, format::PIPELINED_FORMAT_VERSION)
    }

    fn create_inner(path: &Path, layout: Layout, format_version: u16) -> Result<Self> {
        let counters = Arc::new(OperationCounters::default());
        let file = options(true).create_new(true).open(path)?;
        counters.file_opens.fetch_add(1, Ordering::Relaxed);
        let guard = Guard::acquire_counted(&file, true, LockPolicy::default(), &counters)?;
        let writer_lock = options(true)
            .create_new(true)
            .open(writer_lock_path(path)?)?;
        counters.file_opens.fetch_add(1, Ordering::Relaxed);
        let key_locks = KeyLockFile::create(&key_lock_path(path)?, Arc::clone(&counters))?;
        file.set_len(format::data_start(format_version))?;
        let initial = Snapshot {
            format_version,
            layout,
            generation: 1,
            publication: 1,
            root: 0,
            end: format::data_start(format_version),
        };
        write_snapshot(&file, initial, &counters)?;
        if format_version == format::PIPELINED_FORMAT_VERSION {
            format::write_working_snapshot(&file, initial, &counters)?;
        }
        sync_file(&file, &counters)?;
        sync_file(&writer_lock, &counters)?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        sync_file(&File::open(parent)?, &counters)?;
        drop(guard);
        Ok(Self::from_file(
            file,
            writer_lock,
            Some(key_locks),
            initial,
            true,
            counters,
        ))
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
        Self::open_inner_with_snapshot(path, writable, policy, None).map(|(store, _)| store)
    }

    fn open_inner_with_snapshot(
        path: &Path,
        writable: bool,
        policy: LockPolicy,
        cache: Option<&tree::PageCache>,
    ) -> Result<(Self, Snapshot)> {
        let deadline = policy.deadline();
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
        let guard = Guard::acquire_until(&file, false, deadline, policy.interval(), &counters)?;
        let mut snapshot = read_snapshot(&file, Some(&counters))?;
        if snapshot.root != 0 {
            tree::validate_root(&file, snapshot, &counters, cache)?;
        }
        drop(guard);
        let key_locks = if writable {
            Some(KeyLockFile::open_or_create(
                &key_lock_path(path)?,
                &writer_lock,
                Arc::clone(&counters),
                deadline,
                policy.interval(),
            )?)
        } else {
            None
        };
        if writable && snapshot.format_version == format::PIPELINED_FORMAT_VERSION {
            // No active writer may lose its speculative base. This is an
            // exclusive *open/recovery* fence, never the normal commit gate.
            // An unpublished working root is not durable authority, even if
            // it happens to have a valid checksum after a crash.
            let _recovery =
                Guard::acquire_until(&writer_lock, true, deadline, policy.interval(), &counters)?;
            // A sidecar lock does not refresh this inode's NFS cache. Recovery
            // must read the published root and reset working state under the
            // data-file fence, just like normal working-state handoffs.
            let _data = Guard::acquire_until(&file, true, deadline, policy.interval(), &counters)?;
            snapshot = read_snapshot(&file, Some(&counters))?;
            format::write_working_snapshot(&file, snapshot, &counters)?;
            key_locks.as_ref().unwrap().reset_durability()?;
        }
        let mut store = Self::from_file(file, writer_lock, key_locks, snapshot, writable, counters);
        store.policy = policy;
        Ok((store, snapshot))
    }

    /// Consume the snapshot validated during a read-only open, including its
    /// decoded root page. The callback cannot retain a batch past the file's
    /// lifetime. Ordinary `read_batch` still acquires a fresh published root.
    #[cfg(any(feature = "experimental-s3-overlay", test))]
    pub(crate) fn with_open_read_only_snapshot<T>(
        path: impl AsRef<Path>,
        use_snapshot: impl FnOnce(Layout, &ReadBatch<'_>) -> T,
    ) -> Result<(T, OperationStats)> {
        let cache = tree::PageCache::default();
        let (store, snapshot) = Self::open_inner_with_snapshot(
            path.as_ref(),
            false,
            LockPolicy::default(),
            Some(&cache),
        )?;
        let stats = store.operation_stats_handle();
        let read = ReadBatch {
            file: &store.file,
            snapshot,
            owner_pid: store.owner_pid,
            counters: Arc::clone(&store.counters),
            cache,
        };
        let result = use_snapshot(store.layout, &read);
        drop(read);
        drop(store);
        Ok((result, stats.snapshot()))
    }

    fn from_file(
        file: File,
        writer_lock: File,
        key_locks: Option<Arc<KeyLockFile>>,
        snapshot: Snapshot,
        writable: bool,
        counters: Arc<OperationCounters>,
    ) -> Self {
        Self {
            file,
            writer_lock,
            key_locks,
            layout: snapshot.layout,
            format_version: snapshot.format_version,
            writable,
            policy: LockPolicy::default(),
            owner_pid: std::process::id(),
            counters,
        }
    }

    pub const fn layout(&self) -> Layout {
        self.layout
    }

    /// Persisted format: 2 fixed slots, 3 packed, 4 packed/pipelined commits.
    pub const fn format_version(&self) -> u16 {
        self.format_version
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
            cache: tree::PageCache::default(),
        })
    }

    fn snapshot(&self) -> Result<Snapshot> {
        let snapshot = read_snapshot(&self.file, Some(&self.counters))?;
        if snapshot.layout != self.layout || snapshot.format_version != self.format_version {
            return Err(Error::Corrupt("record layout or format changed"));
        }
        Ok(snapshot)
    }

    /// Publish a single atomic root after all new pages have been synchronized.
    /// Old pages are never modified. Space reclamation is not implemented yet.
    pub fn write_batch(&mut self, mutations: &[Mutation]) -> Result<()> {
        self.write_with_hook(mutations, |_| Ok(()))
    }

    /// Internal metadata compare-and-publish. Validation is repeated after
    /// rebasing under commit admission; a stale caller never overwrites authority.
    pub(crate) fn write_batch_at_generation(
        &mut self,
        mutations: &[Mutation],
        generation: u64,
    ) -> Result<bool> {
        self.write_batch_checked(mutations, &[], |read| Ok(read.generation() == generation))
    }

    fn write_with_hook(
        &mut self,
        mutations: &[Mutation],
        mut hook: impl FnMut(CommitPoint) -> io::Result<()>,
    ) -> Result<()> {
        self.write_with_validator(mutations, &[], false, |_| Ok(true), &mut hook)
            .map(|_| ())
    }

    fn write_batch_checked(
        &mut self,
        mutations: &[Mutation],
        lock_keys: &[Vec<u8>],
        mut validate: impl FnMut(&ReadBatch<'_>) -> Result<bool>,
    ) -> Result<bool> {
        self.write_with_validator(mutations, lock_keys, false, &mut validate, |_| Ok(()))
    }

    fn write_batch_checked_exclusive_legacy(
        &mut self,
        mutations: &[Mutation],
        mut validate: impl FnMut(&ReadBatch<'_>) -> Result<bool>,
    ) -> Result<bool> {
        self.write_with_validator(mutations, &[], true, &mut validate, |_| Ok(()))
    }

    fn write_with_validator(
        &mut self,
        mutations: &[Mutation],
        lock_keys: &[Vec<u8>],
        exclusive_legacy: bool,
        mut validate: impl FnMut(&ReadBatch<'_>) -> Result<bool>,
        mut hook: impl FnMut(CommitPoint) -> io::Result<()>,
    ) -> Result<bool> {
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
        for key in lock_keys {
            self.layout.check_key(key)?;
        }
        if mutations.is_empty() {
            return Ok(true);
        }
        let deadline = self.policy.deadline();
        let _legacy_compatibility = Guard::acquire_until(
            &self.writer_lock,
            exclusive_legacy,
            deadline,
            self.policy.interval(),
            &self.counters,
        )?;
        let mut keys: Vec<_> = mutations
            .iter()
            .map(|mutation| mutation.key().to_vec())
            .collect();
        keys.extend(lock_keys.iter().cloned());
        let key_refs: Vec<_> = keys.iter().map(Vec::as_slice).collect();
        let key_locks = self
            .key_locks
            .as_ref()
            .ok_or(Error::Corrupt("writable store has no key-lock table"))?;
        let _key_stripes = key_locks.acquire_stripes(
            &key_refs,
            deadline,
            self.policy.interval(),
            &self.counters,
        )?;
        if self.format_version == format::PIPELINED_FORMAT_VERSION {
            return self.write_pipelined(mutations, deadline, &mut validate, &mut hook);
        }
        // Keep admission across a preflight rebase, so a waiting writer does
        // not repeatedly lose its turn. The first plan is still concurrent.
        let mut commit_guard = None;
        loop {
            let root_guard = Guard::acquire_until(
                &self.file,
                false,
                deadline,
                self.policy.interval(),
                &self.counters,
            )?;
            let base = self.snapshot()?;
            drop(root_guard);
            let read = ReadBatch {
                file: &self.file,
                snapshot: base,
                owner_pid: self.owner_pid,
                counters: Arc::clone(&self.counters),
                cache: tree::PageCache::default(),
            };
            if !validate(&read)? {
                return Ok(false);
            }
            let mut next = base;
            next.generation = next
                .generation
                .checked_add(1)
                .ok_or(Error::Invalid("generation exhausted"))?;
            next.publication = next.generation;
            let plan = tree::prepare_batch(
                &self.file,
                base,
                mutations,
                &self.counters,
                Some(&read.cache),
            )?;
            drop(read);
            hook(CommitPoint::PlanPrepared)?;
            if commit_guard.is_none() {
                commit_guard = Some(key_locks.acquire_commit(
                    deadline,
                    self.policy.interval(),
                    &self.counters,
                )?);
            }
            // Recheck under the allocation lock we already need, avoiding a
            // separate lock round trip and any allocation for a stale plan.
            let Some(start) = self.reserve_page_range(base, plan.page_count(), deadline)? else {
                self.counters
                    .preflight_rebases
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            };
            next = tree::write_plan(&self.file, base, next, start, &plan, &self.counters)?;
            hook(CommitPoint::PagesWritten)?;
            sync_file(&self.file, &self.counters)?;
            hook(CommitPoint::PagesSynced)?;

            let publication_started = std::time::Instant::now();
            let publication_result: Result<bool> = (|| {
                let _publication = Guard::acquire_until(
                    &self.file,
                    true,
                    deadline,
                    self.policy.interval(),
                    &self.counters,
                )?;
                let latest = self.snapshot()?;
                if latest != base {
                    self.counters
                        .publication_retries
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(false);
                }
                write_snapshot(&self.file, next, &self.counters).map_err(|e| match e {
                    Error::Io(e) => Error::CommitUnknown(e),
                    other => other,
                })?;
                hook(CommitPoint::RootWritten).map_err(Error::CommitUnknown)?;
                sync_file(&self.file, &self.counters).map_err(Error::CommitUnknown)?;
                Ok(true)
            })();
            self.counters.publication_ns.fetch_add(
                publication_started.elapsed().as_nanos() as u64,
                Ordering::Relaxed,
            );
            if publication_result? {
                return Ok(true);
            }
        }
    }

    fn write_pipelined(
        &self,
        mutations: &[Mutation],
        deadline: locking::LockDeadline,
        mut validate: impl FnMut(&ReadBatch<'_>) -> Result<bool>,
        mut hook: impl FnMut(CommitPoint) -> io::Result<()>,
    ) -> Result<bool> {
        let staged = {
            let _stage = self.key_locks.as_ref().unwrap().acquire_commit(
                deadline,
                self.policy.interval(),
                &self.counters,
            )?;
            // Lock the inode whose cached bytes we exchange, not just its
            // sidecar. NFS lock acquisition refreshes its cached state; unlock
            // hands off the new working root and appended pages to peers.
            // Shared is enough: the staging gate excludes other working-state
            // writers, and published pages are immutable. Snapshot capture can
            // coexist; only root publication needs exclusive data ownership.
            let _data = Guard::acquire_until(
                &self.file,
                false,
                deadline,
                self.policy.interval(),
                &self.counters,
            )?;
            let mut state = format::read_working_state(&self.file, &self.counters)?;
            let base = state.snapshot;
            if state.failed {
                return Err(Error::Corrupt(
                    "pipeline failed; quiesce writers and reopen",
                ));
            }
            if base.generation - state.durable_generation == 64 {
                return Err(Error::Busy);
            }
            if base.layout != self.layout || base.format_version != self.format_version {
                return Err(Error::Corrupt("working root layout or format changed"));
            }
            let read = ReadBatch {
                file: &self.file,
                snapshot: base,
                owner_pid: self.owner_pid,
                counters: Arc::clone(&self.counters),
                cache: tree::PageCache::default(),
            };
            if !validate(&read)? {
                // A dead writer can leave a speculative row different from
                // the published row. Returning false here would make catalog
                // optimistic retries reread that same old row forever. Do not
                // spin or silently discard a durability hole.
                if self.snapshot()?.generation < base.generation {
                    return Err(Error::Busy);
                }
                return Ok(false);
            }
            let mut next = base;
            next.generation = base
                .generation
                .checked_add(1)
                .ok_or(Error::Invalid("generation exhausted"))?;
            let plan = match tree::prepare_batch(
                &self.file,
                base,
                mutations,
                &self.counters,
                Some(&read.cache),
            ) {
                Ok(plan) => plan,
                Err(Error::Duplicate) => {
                    // Do not report a durable uniqueness conflict solely from
                    // an abandoned, unpublished transaction. A conflict that
                    // also exists in the published tree is a real Duplicate.
                    let published = self.snapshot()?;
                    if published.generation == base.generation {
                        return Err(Error::Duplicate);
                    }
                    match tree::prepare_batch(
                        &self.file,
                        published,
                        mutations,
                        &self.counters,
                        None,
                    ) {
                        Ok(_) => return Err(Error::Busy),
                        Err(error) => return Err(error),
                    }
                }
                Err(error) => return Err(error),
            };
            hook(CommitPoint::PlanPrepared)?;
            // The staging gate owns append allocation. The data fence makes
            // both the physical length and speculative base fresh on NFS.
            let start = self.reserve_physical_pages(base, plan.page_count())?;
            next = tree::write_plan(&self.file, base, next, start, &plan, &self.counters)?;
            hook(CommitPoint::PagesWritten)?;
            // From this point a later writer can include this transaction in
            // its durable prefix, so failures are uncertain, never safe retries.
            state.snapshot = next;
            format::write_working_state(&self.file, state, &self.counters)
                .map_err(uncertain_commit)?;
            next
        }; // Crucially: release the staging gate BEFORE either durable sync.

        let finish = (|| -> Result<()> {
            hook(CommitPoint::WorkingRootWritten)?;
            // Flush this writer's pages, then wait for the earlier writers'
            // explicit completion flags. This does NOT assume fsync on one
            // client flushes another client's dirty cache.
            self.key_locks
                .as_ref()
                .unwrap()
                .synchronize(&self.file, &self.counters)?;
            self.complete_pipeline_data(staged.generation, deadline)?;
            hook(CommitPoint::PagesSynced)?;
            let started = std::time::Instant::now();
            {
                let _publication = Guard::acquire_until(
                    &self.file,
                    true,
                    deadline,
                    self.policy.interval(),
                    &self.counters,
                )?;
                let latest = self.snapshot()?;
                let mut published = if latest.generation < staged.generation {
                    staged
                } else {
                    latest
                };
                published.publication = latest
                    .publication
                    .checked_add(1)
                    .ok_or(Error::Invalid("publication sequence exhausted"))?;
                // Write a root on THIS descriptor even if a later writer has
                // already published our prefix; the following fsync must not
                // depend on flushing another client's dirty root header.
                write_snapshot(&self.file, published, &self.counters)?;
                // A newer published prefix already contains this transaction.
                // Never overwrite it with an out-of-order older completion.
            }
            self.counters
                .publication_ns
                .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
            hook(CommitPoint::RootWritten)?;
            // Publication ownership is also released before this sync. No
            // acknowledgement is returned until the root/prefix is durable.
            self.key_locks
                .as_ref()
                .unwrap()
                .synchronize(&self.file, &self.counters)?;
            Ok(())
        })();
        if let Err(error) = finish {
            // Best effort: prevent new speculation after a failed sync/timeout.
            // A missing completion bit independently prevents unsafe promotion.
            if let Ok(_stage) = self.key_locks.as_ref().unwrap().acquire_commit(
                LockPolicy::default().deadline(),
                self.policy.interval(),
                &self.counters,
            ) {
                if let Ok(_data) = Guard::acquire_until(
                    &self.file,
                    false,
                    LockPolicy::default().deadline(),
                    self.policy.interval(),
                    &self.counters,
                ) {
                    if let Ok(mut state) = format::read_working_state(&self.file, &self.counters) {
                        state.failed = true;
                        let _ = format::write_working_state(&self.file, state, &self.counters);
                    }
                }
            }
            return Err(uncertain_commit(error));
        }
        Ok(true)
    }

    fn complete_pipeline_data(
        &self,
        generation: u64,
        deadline: locking::LockDeadline,
    ) -> Result<()> {
        let mut marked = false;
        loop {
            {
                let _stage = self.key_locks.as_ref().unwrap().acquire_commit(
                    deadline,
                    self.policy.interval(),
                    &self.counters,
                )?;
                let _data = Guard::acquire_until(
                    &self.file,
                    false,
                    deadline,
                    self.policy.interval(),
                    &self.counters,
                )?;
                let mut state = format::read_working_state(&self.file, &self.counters)?;
                if state.failed {
                    return Err(Error::Corrupt(
                        "pipeline failed; quiesce writers and reopen",
                    ));
                }
                if generation > state.snapshot.generation {
                    return Err(Error::Corrupt("pipeline lost staged generation"));
                }
                if !marked {
                    if generation > state.durable_generation {
                        state.ready |= 1 << (generation % 64);
                        while state.durable_generation < state.snapshot.generation {
                            let bit = 1 << ((state.durable_generation + 1) % 64);
                            if state.ready & bit == 0 {
                                break;
                            }
                            state.ready &= !bit;
                            state.durable_generation += 1;
                        }
                    }
                    format::write_working_state(&self.file, state, &self.counters)?;
                    marked = true;
                }
                if state.durable_generation >= generation {
                    return Ok(());
                }
            }
            deadline.wait(self.policy.interval())?;
        }
    }

    fn reserve_page_range(
        &self,
        base: Snapshot,
        pages: usize,
        deadline: locking::LockDeadline,
    ) -> Result<Option<u64>> {
        let _allocation = Guard::acquire_until(
            &self.file,
            true,
            deadline,
            self.policy.interval(),
            &self.counters,
        )?;
        if self.snapshot()? != base {
            return Ok(None);
        }
        self.reserve_physical_pages(base, pages).map(Some)
    }

    fn reserve_physical_pages(&self, base: Snapshot, pages: usize) -> Result<u64> {
        if pages == 0 {
            return Ok(base.end);
        }
        self.counters.file_stats.fetch_add(1, Ordering::Relaxed);
        let physical_end = self.file.metadata()?.len();
        let start = physical_end.max(base.end);
        if start % format::PAGE_BYTES as u64 != 0 {
            return Err(Error::Corrupt("unaligned physical append position"));
        }
        let bytes = (pages as u64)
            .checked_mul(format::PAGE_BYTES as u64)
            .ok_or(Error::Invalid("append reservation overflow"))?;
        let reserved_end = start
            .checked_add(bytes)
            .filter(|end| *end <= i64::MAX as u64)
            .ok_or(Error::Invalid("file size exhausted"))?;
        self.file.set_len(reserved_end)?;
        Ok(start)
    }
}

fn uncertain_commit(error: Error) -> Error {
    match error {
        Error::CommitUnknown(_) => error,
        Error::Io(error) => Error::CommitUnknown(error),
        other => Error::CommitUnknown(io::Error::other(other)),
    }
}

fn sync_file(file: &File, counters: &OperationCounters) -> io::Result<()> {
    counters.sync_requests.fetch_add(1, Ordering::Relaxed);
    sync_file_physical(file, counters)
}

fn sync_file_physical(file: &File, counters: &OperationCounters) -> io::Result<()> {
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

fn key_lock_path(path: &Path) -> Result<PathBuf> {
    let mut name = path
        .file_name()
        .ok_or(Error::Invalid("missing file name"))?
        .to_os_string();
    name.push(".keylocks");
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
    PlanPrepared,
    PagesWritten,
    WorkingRootWritten,
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
    cache: tree::PageCache,
}

impl ReadBatch<'_> {
    pub const fn generation(&self) -> u64 {
        self.snapshot.generation
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_process()?;
        self.snapshot.layout.check_key(key)?;
        tree::get(self.file, self.snapshot, key, &self.counters, &self.cache)
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
        tree::range(
            self.file,
            self.snapshot,
            start,
            end,
            limit,
            &self.counters,
            &self.cache,
        )
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
mod pipelined_tests;
#[cfg(test)]
mod tests;
