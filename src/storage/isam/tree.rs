//! An original copy-on-write B+ tree. Branch entries store a child's maximum
//! key, so bounded range traversal needs no mutable links between leaf pages.
use super::{
    Error, Layout, Mutation, OperationCounters, Record, Result,
    format::{self, CHECKSUM_START, PAGE_BYTES, Snapshot, u16_at, u64_at},
};
use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    os::unix::fs::FileExt,
    sync::{Arc, Mutex, atomic::Ordering},
    time::Instant,
};

const PREFIX: usize = 40;
const MAX_LEVEL: u8 = 31;
const MAGIC: &[u8; 8] = b"BRIPAGE1";
const PACKED_MAGIC: &[u8; 8] = b"BRIPAGE2";

const CACHE_PAGES: usize = 64;
const CACHE_BYTES: usize = 256 * 1024;

/// Owned by one immutable ReadBatch, never shared between files/generations.
/// The byte budget covers decoded node/vector storage, not allocator overhead.
#[derive(Debug, Default)]
pub(super) struct PageCache(Mutex<CachedPages>);

#[derive(Debug, Default)]
struct CachedPages {
    pages: HashMap<u64, Arc<Node>>,
    fifo: VecDeque<u64>,
    bytes: usize,
}

/// Validate at open under the shared root fence. A supplied cache belongs only
/// to this snapshot and lets its first lookup reuse the validated root page.
pub(super) fn validate_root(
    file: &File,
    snapshot: Snapshot,
    counters: &OperationCounters,
    cache: Option<&PageCache>,
) -> Result<()> {
    if cache.is_some() {
        cached_node(file, snapshot, snapshot.root, counters, cache)?;
    } else {
        read_node(file, snapshot, snapshot.root, counters)?;
    }
    Ok(())
}

impl Node {
    fn cache_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.entries.capacity() * std::mem::size_of::<Record>()
            + self
                .entries
                .iter()
                .map(|entry| entry.key.capacity() + entry.value.capacity())
                .sum::<usize>()
    }
}

fn cached_node(
    file: &File,
    snapshot: Snapshot,
    offset: u64,
    counters: &OperationCounters,
    cache: Option<&PageCache>,
) -> Result<Arc<Node>> {
    let cached = cache.and_then(|cache| {
        cache
            .0
            .lock()
            .ok()
            .and_then(|pages| pages.pages.get(&offset).cloned())
    });
    if let Some(node) = cached {
        return Ok(node);
    }
    // Validate/checksum before admission. Never cache an error or miss. A
    // poisoned cache merely falls back to the authoritative immutable pages.
    let node = Arc::new(read_node(file, snapshot, offset, counters)?);
    if let Some(mut pages) = cache.and_then(|cache| cache.0.lock().ok()) {
        if let Some(existing) = pages.pages.get(&offset) {
            return Ok(Arc::clone(existing));
        }
        let bytes = node.cache_bytes();
        if bytes <= CACHE_BYTES {
            while pages.pages.len() >= CACHE_PAGES || pages.bytes + bytes > CACHE_BYTES {
                let victim = pages.fifo.pop_front().expect("nonempty bounded page cache");
                let removed = pages.pages.remove(&victim).expect("queued cache page");
                pages.bytes -= removed.cache_bytes();
            }
            pages.bytes += bytes;
            pages.fifo.push_back(offset);
            pages.pages.insert(offset, Arc::clone(&node));
        }
    }
    Ok(node)
}

#[derive(Debug, Clone)]
pub(crate) struct Node {
    level: u8,
    entries: Vec<Record>,
}

fn stride(layout: Layout, level: u8) -> usize {
    usize::from(layout.key_bytes)
        + if level == 0 {
            2 + usize::from(layout.max_value_bytes)
        } else {
            8
        }
}

fn packed_leaf(snapshot: Snapshot, level: u8) -> bool {
    matches!(
        snapshot.format_version,
        format::PACKED_FORMAT_VERSION | format::PIPELINED_FORMAT_VERSION
    ) && level == 0
}

fn page_magic(snapshot: Snapshot) -> &'static [u8; 8] {
    if matches!(
        snapshot.format_version,
        format::PACKED_FORMAT_VERSION | format::PIPELINED_FORMAT_VERSION
    ) {
        PACKED_MAGIC
    } else {
        MAGIC
    }
}

fn capacity(snapshot: Snapshot, level: u8) -> usize {
    let minimum_width = if packed_leaf(snapshot, level) {
        usize::from(snapshot.layout.key_bytes) + 2
    } else {
        stride(snapshot.layout, level)
    };
    (CHECKSUM_START - PREFIX) / minimum_width
}

fn child(entry: &Record) -> u64 {
    u64_at(&entry.value, 0)
}

pub(crate) fn read_node(
    file: &File,
    snapshot: Snapshot,
    offset: u64,
    counters: &OperationCounters,
) -> Result<Node> {
    if offset < format::data_start(snapshot.format_version)
        || offset >= snapshot.end
        || offset % PAGE_BYTES as u64 != 0
    {
        return Err(Error::Corrupt("page pointer outside committed bounds"));
    }
    let mut bytes = [0; PAGE_BYTES];
    counters.page_reads.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let result = file.read_exact_at(&mut bytes, offset);
    counters
        .page_read_ns
        .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
    result.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::Corrupt("truncated tree page")
        } else {
            Error::Io(e)
        }
    })?;
    if !format::checksum_valid(&bytes)
        || &bytes[..8] != page_magic(snapshot)
        || u64_at(&bytes, 8) != offset
    {
        return Err(Error::Corrupt("page checksum, magic, or position mismatch"));
    }
    let generation = u64_at(&bytes, 16);
    let level = bytes[24];
    let count = usize::from(u16_at(&bytes, 26));
    if level > MAX_LEVEL
        || generation == 0
        || generation > snapshot.generation
        || bytes[25] != 0
        || bytes[28..PREFIX].iter().any(|b| *b != 0)
        || count == 0
        || count > capacity(snapshot, level)
    {
        return Err(Error::Corrupt("invalid page generation, level, or count"));
    }
    let width = stride(snapshot.layout, level);
    let key_bytes = usize::from(snapshot.layout.key_bytes);
    let mut entries: Vec<Record> = Vec::with_capacity(count);
    let mut start = PREFIX;
    for _ in 0..count {
        let minimum_width = if packed_leaf(snapshot, level) {
            key_bytes + 2
        } else {
            width
        };
        if start + minimum_width > CHECKSUM_START {
            return Err(Error::Corrupt("page entry exceeds payload bounds"));
        }
        let key = bytes[start..start + key_bytes].to_vec();
        let body = start + key_bytes;
        let mut entry_end = start + width;
        let value = if level == 0 {
            let len = usize::from(u16_at(&bytes, body));
            if packed_leaf(snapshot, level) {
                entry_end = body + 2 + len;
            }
            if len > usize::from(snapshot.layout.max_value_bytes)
                || entry_end > CHECKSUM_START
                || bytes[body + 2 + len..entry_end].iter().any(|b| *b != 0)
            {
                return Err(Error::Corrupt("invalid leaf value length or padding"));
            }
            bytes[body + 2..body + 2 + len].to_vec()
        } else {
            let pointer = u64_at(&bytes, body);
            // Parents are appended after their children. This also rules out cycles.
            if pointer < format::data_start(snapshot.format_version)
                || pointer >= offset
                || pointer % PAGE_BYTES as u64 != 0
            {
                return Err(Error::Corrupt("invalid branch pointer"));
            }
            bytes[body..body + 8].to_vec()
        };
        if entries.last().is_some_and(|previous| previous.key >= key) {
            return Err(Error::Corrupt("page keys are not strictly ordered"));
        }
        entries.push(Record { key, value });
        start = entry_end;
    }
    if bytes[start..CHECKSUM_START].iter().any(|b| *b != 0) {
        return Err(Error::Corrupt("nonzero page tail"));
    }
    Ok(Node { level, entries })
}

fn read_child(
    file: &File,
    snapshot: Snapshot,
    parent: &Node,
    index: usize,
    counters: &OperationCounters,
    cache: Option<&PageCache>,
) -> Result<Arc<Node>> {
    let reference = &parent.entries[index];
    let node = cached_node(file, snapshot, child(reference), counters, cache)?;
    if node.level + 1 != parent.level
        || node.entries.last().unwrap().key != reference.key
        || (index > 0 && node.entries[0].key <= parent.entries[index - 1].key)
    {
        return Err(Error::Corrupt("child level or key fence mismatch"));
    }
    Ok(node)
}

#[derive(Debug, Clone, Copy)]
enum PageRef {
    Existing(u64),
    New(usize),
}

#[derive(Debug)]
enum PlannedValue {
    Inline(Vec<u8>),
    Child(PageRef),
}

#[derive(Debug)]
struct PlannedEntry {
    key: Vec<u8>,
    value: PlannedValue,
}

#[derive(Debug)]
struct PlannedPage {
    level: u8,
    entries: Vec<PlannedEntry>,
}

#[derive(Debug)]
pub(crate) struct BatchPlan {
    pages: Vec<PlannedPage>,
    root: Option<PageRef>,
}

impl BatchPlan {
    pub(crate) fn page_count(&self) -> usize {
        self.pages.len()
    }
}

pub(crate) fn prepare_batch(
    file: &File,
    snapshot: Snapshot,
    mutations: &[Mutation],
    counters: &OperationCounters,
    cache: Option<&PageCache>,
) -> Result<BatchPlan> {
    let root = if snapshot.root == 0 {
        Arc::new(Node {
            level: 0,
            entries: Vec::new(),
        })
    } else {
        cached_node(file, snapshot, snapshot.root, counters, cache)?
    };
    let mut level = root.level;
    let mut ordered: Vec<_> = mutations.iter().collect();
    // Stable ordering preserves insert/put/delete semantics for repeated keys.
    ordered.sort_by(|a, b| a.key().cmp(b.key()));
    let mut pages = Vec::new();
    let mut changed = change_batch(file, snapshot, root, &ordered, &mut pages, counters, cache)?;
    while changed.len() > 1 {
        if level == MAX_LEVEL {
            return Err(Error::Invalid("tree depth exhausted"));
        }
        level += 1;
        changed = emit_pages(snapshot, level, changed, &mut pages)?;
    }
    let root = changed.first().map(|entry| match entry.value {
        PlannedValue::Child(reference) => reference,
        PlannedValue::Inline(_) => unreachable!("leaf entries cannot be tree roots"),
    });
    Ok(BatchPlan { pages, root })
}

fn change_batch(
    file: &File,
    snapshot: Snapshot,
    node: Arc<Node>,
    mutations: &[&Mutation],
    pages: &mut Vec<PlannedPage>,
    counters: &OperationCounters,
    cache: Option<&PageCache>,
) -> Result<Vec<PlannedEntry>> {
    if node.level == 0 {
        let mut records: std::collections::BTreeMap<_, _> = node
            .entries
            .iter()
            .map(|r| (r.key.clone(), r.value.clone()))
            .collect();
        for mutation in mutations {
            match mutation {
                Mutation::Insert(record) => {
                    if records.contains_key(&record.key) {
                        return Err(Error::Duplicate);
                    }
                    records.insert(record.key.clone(), record.value.clone());
                }
                Mutation::Put(record) => {
                    records.insert(record.key.clone(), record.value.clone());
                }
                Mutation::Delete(key) => {
                    records.remove(key);
                }
            }
        }
        return emit_pages(
            snapshot,
            0,
            records
                .into_iter()
                .map(|(key, value)| PlannedEntry {
                    key,
                    value: PlannedValue::Inline(value),
                })
                .collect(),
            pages,
        );
    }

    let mut replacement = Vec::new();
    let mut consumed = 0;
    for position in 0..node.entries.len() {
        let count = if position + 1 == node.entries.len() {
            mutations.len() - consumed
        } else {
            mutations[consumed..]
                .partition_point(|m| m.key() <= node.entries[position].key.as_slice())
        };
        if count == 0 {
            replacement.push(PlannedEntry {
                key: node.entries[position].key.clone(),
                value: PlannedValue::Child(PageRef::Existing(child(&node.entries[position]))),
            });
        } else {
            let descendant = read_child(file, snapshot, &node, position, counters, cache)?;
            replacement.extend(change_batch(
                file,
                snapshot,
                descendant,
                &mutations[consumed..consumed + count],
                pages,
                counters,
                cache,
            )?);
            consumed += count;
        }
    }
    emit_pages(snapshot, node.level, replacement, pages)
}

fn emit_pages(
    snapshot: Snapshot,
    level: u8,
    entries: Vec<PlannedEntry>,
    pages: &mut Vec<PlannedPage>,
) -> Result<Vec<PlannedEntry>> {
    let mut result = Vec::new();
    let mut entries = entries.into_iter().peekable();
    while entries.peek().is_some() {
        let mut chunk = Vec::new();
        let mut used = PREFIX;
        while let Some(entry) = entries.peek() {
            let width = planned_width(snapshot, level, entry)?;
            if used + width > CHECKSUM_START {
                break;
            }
            used += width;
            chunk.push(entries.next().unwrap());
        }
        let key = chunk
            .last()
            .ok_or(Error::Invalid("record exceeds page capacity"))?
            .key
            .clone();
        let page_id = pages.len();
        pages.push(PlannedPage {
            level,
            entries: chunk,
        });
        result.push(PlannedEntry {
            key,
            value: PlannedValue::Child(PageRef::New(page_id)),
        });
    }
    Ok(result)
}

fn planned_width(snapshot: Snapshot, level: u8, entry: &PlannedEntry) -> Result<usize> {
    match &entry.value {
        PlannedValue::Inline(value)
            if level == 0 && value.len() <= usize::from(snapshot.layout.max_value_bytes) =>
        {
            Ok(if packed_leaf(snapshot, level) {
                usize::from(snapshot.layout.key_bytes) + 2 + value.len()
            } else {
                stride(snapshot.layout, level)
            })
        }
        PlannedValue::Child(_) if level != 0 => Ok(stride(snapshot.layout, level)),
        _ => Err(Error::Corrupt(
            "planned tree value at invalid level or size",
        )),
    }
}

pub(crate) fn write_plan(
    file: &File,
    base: Snapshot,
    mut next: Snapshot,
    start: u64,
    plan: &BatchPlan,
    counters: &OperationCounters,
) -> Result<Snapshot> {
    if plan.pages.is_empty() {
        next.root = 0;
        return Ok(next);
    }
    if start < base.end || start % PAGE_BYTES as u64 != 0 {
        return Err(Error::Invalid("invalid append reservation"));
    }
    let pages_bytes = (plan.pages.len() as u64)
        .checked_mul(PAGE_BYTES as u64)
        .ok_or(Error::Invalid("append reservation overflow"))?;
    let end = start
        .checked_add(pages_bytes)
        .filter(|end| *end <= i64::MAX as u64)
        .ok_or(Error::Invalid("file size exhausted"))?;
    let mut offsets = Vec::with_capacity(plan.pages.len());
    for index in 0..plan.pages.len() {
        offsets.push(start + index as u64 * PAGE_BYTES as u64);
    }

    for (index, page) in plan.pages.iter().enumerate() {
        let mut bytes = [0; PAGE_BYTES];
        bytes[..8].copy_from_slice(page_magic(next));
        bytes[8..16].copy_from_slice(&offsets[index].to_le_bytes());
        bytes[16..24].copy_from_slice(&next.generation.to_le_bytes());
        bytes[24] = page.level;
        bytes[26..28].copy_from_slice(&(page.entries.len() as u16).to_le_bytes());
        let key_bytes = usize::from(next.layout.key_bytes);
        let mut entry_start = PREFIX;
        for entry in &page.entries {
            let width = planned_width(next, page.level, entry)?;
            if entry_start + width > CHECKSUM_START || entry.key.len() != key_bytes {
                return Err(Error::Corrupt("planned tree entry exceeds page bounds"));
            }
            bytes[entry_start..entry_start + key_bytes].copy_from_slice(&entry.key);
            let body = entry_start + key_bytes;
            match &entry.value {
                PlannedValue::Inline(value) if page.level == 0 => {
                    bytes[body..body + 2].copy_from_slice(&(value.len() as u16).to_le_bytes());
                    bytes[body + 2..body + 2 + value.len()].copy_from_slice(value);
                }
                PlannedValue::Child(reference) if page.level != 0 => {
                    let child_offset = match reference {
                        PageRef::Existing(offset) => *offset,
                        PageRef::New(page_id) => *offsets
                            .get(*page_id)
                            .ok_or(Error::Corrupt("forward reference in tree plan"))?,
                    };
                    bytes[body..body + 8].copy_from_slice(&child_offset.to_le_bytes());
                }
                _ => return Err(Error::Corrupt("planned tree value at invalid level")),
            }
            entry_start += width;
        }
        format::seal(&mut bytes);
        counters.page_writes.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let result = file.write_all_at(&bytes, offsets[index]);
        counters
            .page_write_ns
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        result?;
    }
    next.root = match plan.root {
        Some(PageRef::Existing(offset)) => offset,
        Some(PageRef::New(page_id)) => *offsets
            .get(page_id)
            .ok_or(Error::Corrupt("tree root reference outside plan"))?,
        None => 0,
    };
    next.end = end;
    Ok(next)
}

pub(crate) fn get(
    file: &File,
    snapshot: Snapshot,
    key: &[u8],
    counters: &OperationCounters,
    cache: &PageCache,
) -> Result<Option<Vec<u8>>> {
    if snapshot.root == 0 {
        return Ok(None);
    }
    let mut node = cached_node(file, snapshot, snapshot.root, counters, Some(cache))?;
    while node.level != 0 {
        let index = node.entries.partition_point(|r| r.key.as_slice() < key);
        if index == node.entries.len() {
            return Ok(None);
        }
        node = read_child(file, snapshot, &node, index, counters, Some(cache))?;
    }
    Ok(node
        .entries
        .binary_search_by(|r| r.key.as_slice().cmp(key))
        .ok()
        .map(|i| node.entries[i].value.clone()))
}

pub(crate) fn range(
    file: &File,
    snapshot: Snapshot,
    start: &[u8],
    end: Option<&[u8]>,
    limit: usize,
    counters: &OperationCounters,
    cache: &PageCache,
) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    if snapshot.root != 0 && limit != 0 && end != Some(start) {
        let node = cached_node(file, snapshot, snapshot.root, counters, Some(cache))?;
        RangeVisitor {
            file,
            snapshot,
            start,
            end,
            limit,
            counters,
            cache,
            records: &mut records,
        }
        .visit(node)?;
    }
    Ok(records)
}

struct RangeVisitor<'a> {
    file: &'a File,
    snapshot: Snapshot,
    start: &'a [u8],
    end: Option<&'a [u8]>,
    limit: usize,
    counters: &'a OperationCounters,
    cache: &'a PageCache,
    records: &'a mut Vec<Record>,
}

impl RangeVisitor<'_> {
    fn visit(&mut self, node: Arc<Node>) -> Result<()> {
        if node.level == 0 {
            for entry in &node.entries {
                if self.end.is_some_and(|end| entry.key.as_slice() >= end)
                    || self.records.len() == self.limit
                {
                    break;
                }
                if entry.key.as_slice() >= self.start {
                    self.records.push(entry.clone());
                }
            }
        } else {
            for i in 0..node.entries.len() {
                if self.records.len() == self.limit
                    || (i > 0
                        && self
                            .end
                            .is_some_and(|end| node.entries[i - 1].key.as_slice() >= end))
                {
                    break;
                }
                if node.entries[i].key.as_slice() >= self.start {
                    let descendant = read_child(
                        self.file,
                        self.snapshot,
                        &node,
                        i,
                        self.counters,
                        Some(self.cache),
                    )?;
                    self.visit(descendant)?;
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn verify(file: &File, snapshot: Snapshot, counters: &OperationCounters) -> Result<u64> {
    if snapshot.root == 0 {
        return Ok(0);
    }
    fn walk(
        file: &File,
        snapshot: Snapshot,
        node: Arc<Node>,
        counters: &OperationCounters,
    ) -> Result<(u64, Vec<u8>)> {
        if node.level == 0 {
            return Ok((node.entries.len() as u64, node.entries[0].key.clone()));
        }
        let mut total: u64 = 0;
        let mut first = Vec::new();
        for i in 0..node.entries.len() {
            let descendant = read_child(file, snapshot, &node, i, counters, None)?;
            let (count, minimum) = walk(file, snapshot, descendant, counters)?;
            if i == 0 {
                first = minimum;
            } else if minimum <= node.entries[i - 1].key {
                return Err(Error::Corrupt("overlapping subtrees"));
            }
            total = total
                .checked_add(count)
                .ok_or(Error::Corrupt("record count overflow"))?;
        }
        Ok((total, first))
    }
    Ok(walk(
        file,
        snapshot,
        Arc::new(read_node(file, snapshot, snapshot.root, counters)?),
        counters,
    )?
    .0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::isam::Store;

    #[test]
    fn cached_children_still_check_the_parent_fence() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = Store::create(
            directory.path().join("fences.isam"),
            Layout::new(2, 1024).unwrap(),
        )
        .unwrap();
        store
            .write_batch(
                &(0..20_u16)
                    .map(|key| Mutation::put(key.to_be_bytes(), b"value"))
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        let read = store.read_batch().unwrap();
        assert_eq!(read.get(&[0, 0]).unwrap(), Some(b"value".to_vec()));
        let mut parent =
            read_node(read.file, read.snapshot, read.snapshot.root, &read.counters).unwrap();
        assert_eq!(parent.level, 1);
        parent.entries[0].key[1] ^= 1;
        let before = read.counters.page_reads.load(Ordering::Relaxed);
        assert!(matches!(
            read_child(
                read.file,
                read.snapshot,
                &parent,
                0,
                &read.counters,
                Some(&read.cache)
            ),
            Err(Error::Corrupt(_))
        ));
        assert_eq!(read.counters.page_reads.load(Ordering::Relaxed), before);
    }

    #[test]
    fn cache_evicts_under_both_page_and_decoded_byte_limits() {
        for (key_width, value_width, count) in [(128, 1024, 240_u16), (2, 8, 6000)] {
            let directory = tempfile::tempdir().unwrap();
            let mut store = Store::create(
                directory.path().join("bounded.isam"),
                Layout::new(key_width, value_width).unwrap(),
            )
            .unwrap();
            let key = |number: u16| {
                let mut key = vec![0; usize::from(key_width)];
                key[..2].copy_from_slice(&number.to_be_bytes());
                key
            };
            let value = vec![7; usize::from(value_width)];
            let mutations: Vec<_> = (0..count)
                .map(|number| Mutation::insert(key(number), value.clone()))
                .collect();
            for chunk in mutations.chunks(3000) {
                store.write_batch(chunk).unwrap();
            }
            let stats = store.operation_stats_handle();
            let batch = store.read_batch().unwrap();
            for number in 0..count {
                assert_eq!(batch.get(&key(number)).unwrap(), Some(value.clone()));
                let pages = batch.cache.0.lock().unwrap();
                assert!(pages.pages.len() <= CACHE_PAGES);
                assert!(pages.bytes <= CACHE_BYTES);
                assert_eq!(pages.fifo.len(), pages.pages.len());
                assert_eq!(
                    pages.bytes,
                    pages
                        .pages
                        .values()
                        .map(|node| node.cache_bytes())
                        .sum::<usize>()
                );
            }
            let before = stats.snapshot().page_reads;
            assert_eq!(batch.get(&key(0)).unwrap(), Some(value));
            assert!(
                stats.snapshot().page_reads > before,
                "oldest leaf must have been evicted"
            );
            assert_eq!(batch.verify().unwrap(), u64::from(count));
        }
    }
}
