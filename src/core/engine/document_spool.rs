//! Bounded external sorting of keys, never document payloads. Anonymous files
//! are removed by the OS on last-close, including process termination. Binary
//! runs are merged lazily at read time when their frontiers fit memory. Only
//! oversized frontier sets require balanced, two-way materialized merge passes.

use std::{
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque},
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::core::{EngineError, EngineErrorKind, EngineResult};

const MAX_KEY_BYTES: usize = 16 * 1024 * 1024;
const CHUNK_BYTES: usize = 16 * 1024 * 1024;
const CHUNK_KEYS: usize = 65_536;
const QUERY_DISK_BYTES: u64 = 256 * 1024 * 1024;
const ENGINE_DISK_BYTES: u64 = 1024 * 1024 * 1024;
const HEADER_BYTES: usize = 4 + 8 + 2 + 32;
const IO_CHUNK: usize = 64 * 1024;
// Large keys bypass BufWriter's small buffer in larger, bounded writes. A
// 64-KiB read buffer still keeps a many-run cursor's retained memory modest.
const FILE_IO_CHUNK: usize = 1024 * 1024;
const MAX_RUNS: usize = 512;
const FRONTIER_RUNS: usize = 64;

fn limit() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "document sort scratch limit exceeded",
    )
}

fn damaged() -> EngineError {
    // An ephemeral sort-file problem is not evidence of damage to the database.
    EngineError::new(
        EngineErrorKind::StorageUnavailable,
        "document sort scratch is incomplete or damaged",
    )
}

fn io(error: std::io::Error) -> EngineError {
    EngineError::from_source(
        EngineErrorKind::StorageUnavailable,
        "document sort scratch I/O failed",
        error,
    )
}

struct Counter {
    used: AtomicU64,
    limit: u64,
}

impl Counter {
    fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicU64::new(0),
            limit,
        })
    }

    fn reserve(&self, bytes: u64) -> EngineResult<()> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.limit)
            })
            .map(|_| ())
            .map_err(|_| limit())
    }
}

pub(super) struct SpoolBudget(Arc<Counter>);

impl Default for SpoolBudget {
    fn default() -> Self {
        Self(Counter::new(ENGINE_DISK_BYTES))
    }
}

#[cfg(test)]
impl SpoolBudget {
    pub fn used_bytes(&self) -> u64 {
        self.0.used.load(Ordering::Acquire)
    }
}

struct Charge {
    local: Arc<Counter>,
    global: Arc<Counter>,
    bytes: u64,
}

impl Charge {
    fn reserve(&mut self, bytes: u64) -> EngineResult<()> {
        self.local.reserve(bytes)?;
        if let Err(error) = self.global.reserve(bytes) {
            self.local.used.fetch_sub(bytes, Ordering::AcqRel);
            return Err(error);
        }
        self.bytes += bytes;
        Ok(())
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.local.used.fetch_sub(self.bytes, Ordering::AcqRel);
        self.global.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct SpoolPosition {
    pub key: Vec<u8>,
    pub natural_order: u64,
    pub shard: u16,
}

impl SpoolPosition {
    pub fn retained_bytes(&self) -> usize {
        self.key.capacity().saturating_add(128)
    }

    fn header(
        &self,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<[u8; HEADER_BYTES]> {
        if !(9..=MAX_KEY_BYTES).contains(&self.key.len())
            || self.natural_order == 0
            || self.shard >= 64
        {
            return Err(damaged());
        }
        let mut header = [0; HEADER_BYTES];
        header[..4].copy_from_slice(&(self.key.len() as u32).to_le_bytes());
        header[4..12].copy_from_slice(&self.natural_order.to_le_bytes());
        header[12..14].copy_from_slice(&self.shard.to_le_bytes());
        let mut hash = blake3::Hasher::new();
        hash.update(b"briskdb.sort-scratch.v1\0");
        hash.update(&header[..14]);
        for chunk in self.key.chunks(IO_CHUNK) {
            check()?;
            hash.update(chunk);
        }
        header[14..].copy_from_slice(hash.finalize().as_bytes());
        Ok(header)
    }
}

struct RunWriter {
    file: BufWriter<File>,
    charge: Charge,
    max_key_bytes: usize,
}

impl RunWriter {
    fn new(local: &Arc<Counter>, global: &Arc<Counter>) -> EngineResult<Self> {
        Ok(Self {
            file: BufWriter::with_capacity(IO_CHUNK, tempfile::tempfile().map_err(io)?),
            charge: Charge {
                local: local.clone(),
                global: global.clone(),
                bytes: 0,
            },
            max_key_bytes: 0,
        })
    }

    fn push(
        &mut self,
        position: &SpoolPosition,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<()> {
        check()?;
        let header = position.header(check)?;
        self.max_key_bytes = self.max_key_bytes.max(position.key.len());
        self.charge
            .reserve((HEADER_BYTES + position.key.len()) as u64)?;
        self.file.write_all(&header).map_err(io)?;
        for chunk in position.key.chunks(FILE_IO_CHUNK) {
            check()?;
            self.file.write_all(chunk).map_err(io)?;
        }
        check()
    }

    fn finish(mut self, check: &mut dyn FnMut() -> EngineResult<()>) -> EngineResult<Run> {
        check()?;
        self.file.flush().map_err(io)?;
        let mut file = self
            .file
            .into_inner()
            .map_err(|error| io(error.into_error()))?;
        file.seek(SeekFrom::Start(0)).map_err(io)?;
        check()?;
        Ok(Run {
            file: BufReader::with_capacity(IO_CHUNK, file),
            read: 0,
            charge: self.charge,
            max_key_bytes: self.max_key_bytes,
        })
    }
}

struct Run {
    file: BufReader<File>,
    read: u64,
    charge: Charge,
    max_key_bytes: usize,
}

impl Run {
    fn next(
        &mut self,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Option<SpoolPosition>> {
        check()?;
        if self.read == self.charge.bytes {
            return Ok(None);
        }
        if self.charge.bytes.saturating_sub(self.read) < HEADER_BYTES as u64 {
            return Err(damaged());
        }
        let mut header = [0; HEADER_BYTES];
        self.file.read_exact(&mut header).map_err(io)?;
        let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        if !(9..=MAX_KEY_BYTES).contains(&len)
            || len > self.max_key_bytes
            || self.charge.bytes - self.read - (HEADER_BYTES as u64) < len as u64
        {
            return Err(damaged());
        }
        let mut key = Vec::new();
        key.try_reserve_exact(len).map_err(|_| limit())?;
        key.resize(len, 0);
        for chunk in key.chunks_mut(FILE_IO_CHUNK) {
            check()?;
            self.file.read_exact(chunk).map_err(io)?;
        }
        let position = SpoolPosition {
            key,
            natural_order: u64::from_le_bytes(header[4..12].try_into().unwrap()),
            shard: u16::from_le_bytes(header[12..14].try_into().unwrap()),
        };
        if position.header(check)? != header {
            return Err(damaged());
        }
        self.read += (HEADER_BYTES + len) as u64;
        Ok(Some(position))
    }
}

fn merge(
    mut left: Run,
    mut right: Run,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<Run> {
    let mut output = RunWriter::new(&left.charge.local, &left.charge.global)?;
    let mut a = left.next(check)?;
    let mut b = right.next(check)?;
    while a.is_some() || b.is_some() {
        check()?;
        if b.is_none() || a.as_ref().is_some_and(|a| a <= b.as_ref().unwrap()) {
            output.push(a.as_ref().unwrap(), check)?;
            a.take();
            a = left.next(check)?;
        } else {
            output.push(b.as_ref().unwrap(), check)?;
            b.take();
            b = right.next(check)?;
        }
    }
    output.finish(check)
}

pub(super) struct SpoolBuilder {
    local: Arc<Counter>,
    global: Arc<Counter>,
    keys: BinaryHeap<Reverse<SpoolPosition>>,
    key_bytes: usize,
    runs: Vec<Run>,
    chunk_bytes: usize,
    chunk_keys: usize,
    frontier_bytes: usize,
    frontier_runs: usize,
}

impl SpoolBuilder {
    pub fn new(budget: &SpoolBudget) -> Self {
        Self {
            local: Counter::new(QUERY_DISK_BYTES),
            global: budget.0.clone(),
            keys: BinaryHeap::new(),
            key_bytes: 0,
            runs: Vec::new(),
            chunk_bytes: CHUNK_BYTES,
            chunk_keys: CHUNK_KEYS,
            frontier_bytes: CHUNK_BYTES,
            frontier_runs: FRONTIER_RUNS,
        }
    }

    pub fn push(
        &mut self,
        position: SpoolPosition,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<()> {
        check()?;
        let bytes = position.retained_bytes();
        if bytes > self.chunk_bytes {
            return Err(limit());
        }
        if self.keys.len() == self.chunk_keys
            || self.key_bytes.saturating_add(bytes) > self.chunk_bytes
        {
            self.spill(check)?;
        }
        self.key_bytes += bytes;
        self.keys.push(Reverse(position));
        Ok(())
    }

    fn spill(&mut self, check: &mut dyn FnMut() -> EngineResult<()>) -> EngineResult<()> {
        if self.keys.is_empty() {
            return check();
        }
        if self.runs.len() == MAX_RUNS {
            return Err(limit());
        }
        let mut writer = RunWriter::new(&self.local, &self.global)?;
        while let Some(Reverse(position)) = self.keys.pop() {
            writer.push(&position, check)?;
        }
        self.keys = BinaryHeap::new();
        self.key_bytes = 0;
        self.runs.push(writer.finish(check)?);
        Ok(())
    }

    pub fn finish(
        mut self,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<SortSpool> {
        self.spill(check)?;
        // Usually each run only needs one small key in the cursor's merge
        // heap. Do not rewrite every byte of every run just to collapse files.
        // When frontiers would exceed the memory/handle cap, merge adjacent
        // pairs in complete balanced passes (never a growing run with each
        // subsequent input). This bounds materialized merge work to O(n log n).
        while self.runs.len() > 1
            && (self.runs.len() > self.frontier_runs
                || self
                    .runs
                    .iter()
                    .map(|run| run.max_key_bytes + 128)
                    .sum::<usize>()
                    > self.frontier_bytes)
        {
            let mut next = Vec::with_capacity(self.runs.len().div_ceil(2));
            let mut runs = self.runs.into_iter();
            while let Some(left) = runs.next() {
                check()?;
                next.push(match runs.next() {
                    Some(right) => merge(left, right, check)?,
                    None => left,
                });
            }
            self.runs = next;
        }
        check()?;
        Ok(SortSpool {
            runs: self.runs,
            frontiers: BinaryHeap::new(),
            initialized: false,
            pending: VecDeque::new(),
        })
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct Frontier {
    position: SpoolPosition,
    run: usize,
}

pub(super) struct SortSpool {
    runs: Vec<Run>,
    frontiers: BinaryHeap<Reverse<Frontier>>,
    initialized: bool,
    pub pending: VecDeque<Arc<SpoolPosition>>,
}

impl SortSpool {
    pub fn advance(&mut self, check: &mut dyn FnMut() -> EngineResult<()>) -> EngineResult<()> {
        if !self.initialized {
            for (index, run) in self.runs.iter_mut().enumerate() {
                if let Some(position) = run.next(check)? {
                    self.frontiers.push(Reverse(Frontier {
                        position,
                        run: index,
                    }));
                }
            }
            self.initialized = true;
        }
        if self.pending.is_empty() {
            let mut bytes = 0;
            while self.pending.len() < 256 && bytes < 1024 * 1024 {
                let Some(Reverse(Frontier { position, run })) = self.frontiers.pop() else {
                    break;
                };
                bytes += position.retained_bytes();
                self.pending.push_back(Arc::new(position));
                if let Some(position) = self.runs[run].next(check)? {
                    self.frontiers.push(Reverse(Frontier { position, run }));
                }
            }
        }
        check()
    }

    pub fn retained_bytes(&self) -> usize {
        self.runs.len() * IO_CHUNK
            + self.runs.capacity() * std::mem::size_of::<Run>()
            + self.frontiers.capacity() * std::mem::size_of::<Frontier>()
            + self
                .frontiers
                .iter()
                .map(|frontier| frontier.0.position.retained_bytes())
                .sum::<usize>()
            + 256
            + self.pending.capacity() * 128
            + self
                .pending
                .iter()
                .map(|position| position.retained_bytes())
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests;
