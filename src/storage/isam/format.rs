use super::{Error, Layout, OperationCounters, Result};
use std::sync::atomic::Ordering;
use std::{fs::File, os::unix::fs::FileExt};

pub(crate) const PAGE_BYTES: usize = 4096;
pub(crate) const HEADER_BYTES: u64 = 2 * PAGE_BYTES as u64;
pub(crate) const CHECKSUM_START: usize = PAGE_BYTES - 32;
const MAGIC: &[u8; 8] = b"BRISAM01";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub(crate) layout: Layout,
    pub(crate) generation: u64,
    pub(crate) root: u64,
    pub(crate) end: u64,
}

pub(crate) fn seal(bytes: &mut [u8; PAGE_BYTES]) {
    let hash = blake3::hash(&bytes[..CHECKSUM_START]);
    bytes[CHECKSUM_START..].copy_from_slice(hash.as_bytes());
}

pub(crate) fn checksum_valid(bytes: &[u8]) -> bool {
    blake3::hash(&bytes[..CHECKSUM_START]).as_bytes() == &bytes[CHECKSUM_START..]
}

pub(crate) fn u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}
pub(crate) fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

pub(super) fn write_snapshot(
    file: &File,
    snapshot: Snapshot,
    counters: &OperationCounters,
) -> Result<()> {
    let mut bytes = [0; PAGE_BYTES];
    bytes[..8].copy_from_slice(MAGIC);
    bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
    bytes[10..12].copy_from_slice(&snapshot.layout.key_bytes.to_le_bytes());
    bytes[12..14].copy_from_slice(&snapshot.layout.max_value_bytes.to_le_bytes());
    bytes[16..24].copy_from_slice(&snapshot.generation.to_le_bytes());
    bytes[24..32].copy_from_slice(&snapshot.root.to_le_bytes());
    bytes[32..40].copy_from_slice(&snapshot.end.to_le_bytes());
    seal(&mut bytes);
    counters.root_writes.fetch_add(1, Ordering::Relaxed);
    file.write_all_at(&bytes, (snapshot.generation % 2) * PAGE_BYTES as u64)?;
    Ok(())
}

fn decode(bytes: &[u8], slot: u64) -> Result<Option<Snapshot>> {
    // A zero/partially written alternate slot is not a published generation.
    if !checksum_valid(bytes) {
        return Ok(None);
    }
    if &bytes[..8] != MAGIC || u16_at(bytes, 8) != 1 {
        return Err(Error::Corrupt("unknown file magic or format version"));
    }
    if bytes[14..16]
        .iter()
        .chain(bytes[40..CHECKSUM_START].iter())
        .any(|b| *b != 0)
    {
        return Err(Error::Corrupt("nonzero header reserved bytes"));
    }
    let layout = Layout::new(u16_at(bytes, 10), u16_at(bytes, 12))
        .map_err(|_| Error::Corrupt("invalid persisted record layout"))?;
    let s = Snapshot {
        layout,
        generation: u64_at(bytes, 16),
        root: u64_at(bytes, 24),
        end: u64_at(bytes, 32),
    };
    if s.generation == 0
        || s.generation % 2 != slot
        || s.end < HEADER_BYTES
        || s.end % PAGE_BYTES as u64 != 0
        || s.end > i64::MAX as u64
        || (s.root != 0
            && (s.root < HEADER_BYTES || s.root >= s.end || s.root % PAGE_BYTES as u64 != 0))
    {
        return Err(Error::Corrupt("invalid root bounds or generation"));
    }
    Ok(Some(s))
}

/// One root read per batch, not a stat/open/catalog lookup for each record.
pub(super) fn read_snapshot(file: &File, counters: Option<&OperationCounters>) -> Result<Snapshot> {
    if let Some(counters) = counters {
        counters.root_reads.fetch_add(1, Ordering::Relaxed);
    }
    let mut bytes = [0; HEADER_BYTES as usize];
    file.read_exact_at(&mut bytes, 0).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::Corrupt("truncated root headers")
        } else {
            Error::Io(e)
        }
    })?;
    match (
        decode(&bytes[..PAGE_BYTES], 0)?,
        decode(&bytes[PAGE_BYTES..], 1)?,
    ) {
        (Some(a), Some(b)) => {
            if a.layout != b.layout || a.generation.abs_diff(b.generation) != 1 {
                return Err(Error::Corrupt("root slots disagree"));
            }
            Ok(if a.generation > b.generation { a } else { b })
        }
        (Some(s), None) | (None, Some(s)) => Ok(s),
        (None, None) => Err(Error::Corrupt("no valid root header")),
    }
}
