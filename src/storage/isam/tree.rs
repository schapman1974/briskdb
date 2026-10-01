//! An original copy-on-write B+ tree. Branch entries store a child's maximum
//! key, so bounded range traversal needs no mutable links between leaf pages.
use super::{
    Error, Layout, Mutation, OperationCounters, Record, Result,
    format::{self, CHECKSUM_START, HEADER_BYTES, PAGE_BYTES, Snapshot, u16_at, u64_at},
};
use std::{fs::File, os::unix::fs::FileExt, sync::atomic::Ordering};

const PREFIX: usize = 40;
const MAX_LEVEL: u8 = 31;
const MAGIC: &[u8; 8] = b"BRIPAGE1";

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

fn capacity(layout: Layout, level: u8) -> usize {
    (CHECKSUM_START - PREFIX) / stride(layout, level)
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
    if offset < HEADER_BYTES || offset >= snapshot.end || offset % PAGE_BYTES as u64 != 0 {
        return Err(Error::Corrupt("page pointer outside committed bounds"));
    }
    let mut bytes = [0; PAGE_BYTES];
    counters.page_reads.fetch_add(1, Ordering::Relaxed);
    file.read_exact_at(&mut bytes, offset).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::Corrupt("truncated tree page")
        } else {
            Error::Io(e)
        }
    })?;
    if !format::checksum_valid(&bytes) || &bytes[..8] != MAGIC || u64_at(&bytes, 8) != offset {
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
        || count > capacity(snapshot.layout, level)
    {
        return Err(Error::Corrupt("invalid page generation, level, or count"));
    }
    let width = stride(snapshot.layout, level);
    let key_bytes = usize::from(snapshot.layout.key_bytes);
    let mut entries: Vec<Record> = Vec::with_capacity(count);
    for i in 0..count {
        let start = PREFIX + i * width;
        let key = bytes[start..start + key_bytes].to_vec();
        let body = start + key_bytes;
        let value = if level == 0 {
            let len = usize::from(u16_at(&bytes, body));
            if len > usize::from(snapshot.layout.max_value_bytes)
                || bytes[body + 2 + len..start + width].iter().any(|b| *b != 0)
            {
                return Err(Error::Corrupt("invalid leaf value length or padding"));
            }
            bytes[body + 2..body + 2 + len].to_vec()
        } else {
            let pointer = u64_at(&bytes, body);
            // Parents are appended after their children. This also rules out cycles.
            if pointer < HEADER_BYTES || pointer >= offset || pointer % PAGE_BYTES as u64 != 0 {
                return Err(Error::Corrupt("invalid branch pointer"));
            }
            bytes[body..body + 8].to_vec()
        };
        if entries.last().is_some_and(|previous| previous.key >= key) {
            return Err(Error::Corrupt("page keys are not strictly ordered"));
        }
        entries.push(Record { key, value });
    }
    if bytes[PREFIX + count * width..CHECKSUM_START]
        .iter()
        .any(|b| *b != 0)
    {
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
) -> Result<Node> {
    let reference = &parent.entries[index];
    let node = read_node(file, snapshot, child(reference), counters)?;
    if node.level + 1 != parent.level
        || node.entries.last().unwrap().key != reference.key
        || (index > 0 && node.entries[0].key <= parent.entries[index - 1].key)
    {
        return Err(Error::Corrupt("child level or key fence mismatch"));
    }
    Ok(node)
}

fn append(
    file: &File,
    snapshot: &mut Snapshot,
    node: &Node,
    counters: &OperationCounters,
) -> Result<Record> {
    let offset = snapshot.end;
    let end = offset
        .checked_add(PAGE_BYTES as u64)
        .filter(|end| *end <= i64::MAX as u64)
        .ok_or(Error::Invalid("file size exhausted"))?;
    let mut bytes = [0; PAGE_BYTES];
    bytes[..8].copy_from_slice(MAGIC);
    bytes[8..16].copy_from_slice(&offset.to_le_bytes());
    bytes[16..24].copy_from_slice(&snapshot.generation.to_le_bytes());
    bytes[24] = node.level;
    bytes[26..28].copy_from_slice(&(node.entries.len() as u16).to_le_bytes());
    let width = stride(snapshot.layout, node.level);
    let key_bytes = usize::from(snapshot.layout.key_bytes);
    for (i, entry) in node.entries.iter().enumerate() {
        let start = PREFIX + i * width;
        bytes[start..start + key_bytes].copy_from_slice(&entry.key);
        let body = start + key_bytes;
        if node.level == 0 {
            bytes[body..body + 2].copy_from_slice(&(entry.value.len() as u16).to_le_bytes());
            bytes[body + 2..body + 2 + entry.value.len()].copy_from_slice(&entry.value);
        } else {
            bytes[body..body + 8].copy_from_slice(&entry.value);
        }
    }
    format::seal(&mut bytes);
    counters.page_writes.fetch_add(1, Ordering::Relaxed);
    file.write_all_at(&bytes, offset)?;
    snapshot.end = end;
    Ok(Record {
        key: node.entries.last().unwrap().key.clone(),
        value: offset.to_le_bytes().to_vec(),
    })
}

fn persist(
    file: &File,
    snapshot: &mut Snapshot,
    node: Node,
    counters: &OperationCounters,
) -> Result<Vec<Record>> {
    let mut references = Vec::new();
    for entries in node.entries.chunks(capacity(snapshot.layout, node.level)) {
        references.push(append(
            file,
            snapshot,
            &Node {
                level: node.level,
                entries: entries.to_vec(),
            },
            counters,
        )?);
    }
    Ok(references)
}

fn change_batch(
    file: &File,
    snapshot: &mut Snapshot,
    mut node: Node,
    mutations: &[&Mutation],
    counters: &OperationCounters,
) -> Result<Vec<Record>> {
    if node.level == 0 {
        // This bounded map contains one leaf plus this batch's changes, never
        // the whole database. Coalesce before writing: one new page per changed
        // leaf, not a rewrite of the index path for every individual record.
        let mut records: std::collections::BTreeMap<_, _> =
            node.entries.into_iter().map(|r| (r.key, r.value)).collect();
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
        node.entries = records
            .into_iter()
            .map(|(key, value)| Record { key, value })
            .collect();
    } else {
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
                replacement.push(node.entries[position].clone());
            } else {
                let descendant = read_child(file, *snapshot, &node, position, counters)?;
                replacement.extend(change_batch(
                    file,
                    snapshot,
                    descendant,
                    &mutations[consumed..consumed + count],
                    counters,
                )?);
                consumed += count;
            }
        }
        node.entries = replacement;
    }
    persist(file, snapshot, node, counters)
}

pub(crate) fn apply_batch(
    file: &File,
    snapshot: &mut Snapshot,
    mutations: &[Mutation],
    counters: &OperationCounters,
) -> Result<()> {
    let root = if snapshot.root == 0 {
        Node {
            level: 0,
            entries: Vec::new(),
        }
    } else {
        read_node(file, *snapshot, snapshot.root, counters)?
    };
    let mut level = root.level;
    let mut ordered: Vec<_> = mutations.iter().collect();
    // Stable ordering preserves insert/put/delete semantics for repeated keys.
    ordered.sort_by(|a, b| a.key().cmp(b.key()));
    let mut changed = change_batch(file, snapshot, root, &ordered, counters)?;
    while changed.len() > 1 {
        if level == MAX_LEVEL {
            return Err(Error::Invalid("tree depth exhausted"));
        }
        level += 1;
        changed = persist(
            file,
            snapshot,
            Node {
                level,
                entries: changed,
            },
            counters,
        )?;
    }
    snapshot.root = changed.first().map(child).unwrap_or(0);
    Ok(())
}

pub(crate) fn get(
    file: &File,
    snapshot: Snapshot,
    key: &[u8],
    counters: &OperationCounters,
) -> Result<Option<Vec<u8>>> {
    if snapshot.root == 0 {
        return Ok(None);
    }
    let mut node = read_node(file, snapshot, snapshot.root, counters)?;
    while node.level != 0 {
        let index = node.entries.partition_point(|r| r.key.as_slice() < key);
        if index == node.entries.len() {
            return Ok(None);
        }
        node = read_child(file, snapshot, &node, index, counters)?;
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
) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    if snapshot.root != 0 && limit != 0 && end != Some(start) {
        let node = read_node(file, snapshot, snapshot.root, counters)?;
        visit(
            file,
            snapshot,
            node,
            start,
            end,
            limit,
            &mut records,
            counters,
        )?;
    }
    Ok(records)
}

fn visit(
    file: &File,
    snapshot: Snapshot,
    node: Node,
    start: &[u8],
    end: Option<&[u8]>,
    limit: usize,
    out: &mut Vec<Record>,
    counters: &OperationCounters,
) -> Result<()> {
    if node.level == 0 {
        for entry in node.entries {
            if end.is_some_and(|end| entry.key.as_slice() >= end) || out.len() == limit {
                break;
            }
            if entry.key.as_slice() >= start {
                out.push(entry);
            }
        }
    } else {
        for i in 0..node.entries.len() {
            if out.len() == limit
                || (i > 0 && end.is_some_and(|end| node.entries[i - 1].key.as_slice() >= end))
            {
                break;
            }
            if node.entries[i].key.as_slice() >= start {
                let descendant = read_child(file, snapshot, &node, i, counters)?;
                visit(file, snapshot, descendant, start, end, limit, out, counters)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn verify(file: &File, snapshot: Snapshot, counters: &OperationCounters) -> Result<u64> {
    if snapshot.root == 0 {
        return Ok(0);
    }
    fn walk(
        file: &File,
        snapshot: Snapshot,
        node: Node,
        counters: &OperationCounters,
    ) -> Result<(u64, Vec<u8>)> {
        if node.level == 0 {
            return Ok((node.entries.len() as u64, node.entries[0].key.clone()));
        }
        let mut total: u64 = 0;
        let mut first = Vec::new();
        for i in 0..node.entries.len() {
            let descendant = read_child(file, snapshot, &node, i, counters)?;
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
        read_node(file, snapshot, snapshot.root, counters)?,
        counters,
    )?
    .0)
}
