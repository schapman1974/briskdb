use super::{Error, Layout, OperationCounters, Result};
use std::{fs::File, os::unix::fs::FileExt, sync::atomic::Ordering, time::Instant};

pub(crate) const PAGE_BYTES: usize = 4096;
pub(crate) const HEADER_BYTES: u64 = 2 * PAGE_BYTES as u64;
pub(crate) const CHECKSUM_START: usize = PAGE_BYTES - 32;
pub(crate) const FORMAT_VERSION: u16 = 2;
pub(crate) const PACKED_FORMAT_VERSION: u16 = 3;
pub(crate) const PIPELINED_FORMAT_VERSION: u16 = 4;
pub(crate) const WORKING_ROOT_OFFSET: u64 = HEADER_BYTES;
const MAGIC: &[u8; 8] = b"BRISAM02";
const PACKED_MAGIC: &[u8; 8] = b"BRISAM03";
const PIPELINED_MAGIC: &[u8; 8] = b"BRISAM04";

pub(crate) fn data_start(version: u16) -> u64 {
    if version == PIPELINED_FORMAT_VERSION {
        HEADER_BYTES + PAGE_BYTES as u64
    } else {
        HEADER_BYTES
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub(crate) format_version: u16,
    pub(crate) layout: Layout,
    pub(crate) generation: u64,
    // v4 may publish several staged generations at once. Slot alternation is
    // therefore a separate, strictly consecutive publication sequence.
    pub(crate) publication: u64,
    pub(crate) root: u64,
    pub(crate) end: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct WorkingState {
    pub snapshot: Snapshot,
    // A prefix is publishable only after EACH originating writer has synced
    // its pages. One client's fsync must not stand in for another client's.
    pub durable_generation: u64,
    pub ready: u64,
    pub failed: bool,
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
    write_snapshot_at(
        file,
        snapshot,
        (if snapshot.format_version == PIPELINED_FORMAT_VERSION {
            snapshot.publication
        } else {
            snapshot.generation
        } % 2)
            * PAGE_BYTES as u64,
        counters,
        None,
    )
}

pub(super) fn write_working_snapshot(
    file: &File,
    snapshot: Snapshot,
    counters: &OperationCounters,
) -> Result<()> {
    write_working_state(
        file,
        WorkingState {
            snapshot,
            durable_generation: snapshot.generation,
            ready: 0,
            failed: false,
        },
        counters,
    )
}

pub(super) fn write_working_state(
    file: &File,
    state: WorkingState,
    counters: &OperationCounters,
) -> Result<()> {
    if state.snapshot.format_version != PIPELINED_FORMAT_VERSION {
        return Err(Error::Invalid("working root requires pipelined format"));
    }
    write_snapshot_at(
        file,
        state.snapshot,
        WORKING_ROOT_OFFSET,
        counters,
        Some((state.durable_generation, state.ready, state.failed)),
    )
}

fn write_snapshot_at(
    file: &File,
    snapshot: Snapshot,
    offset: u64,
    counters: &OperationCounters,
    working: Option<(u64, u64, bool)>,
) -> Result<()> {
    let mut bytes = [0; PAGE_BYTES];
    let magic = match snapshot.format_version {
        FORMAT_VERSION => MAGIC,
        PACKED_FORMAT_VERSION => PACKED_MAGIC,
        PIPELINED_FORMAT_VERSION => PIPELINED_MAGIC,
        _ => return Err(Error::Invalid("unsupported file format version")),
    };
    bytes[..8].copy_from_slice(magic);
    bytes[8..10].copy_from_slice(&snapshot.format_version.to_le_bytes());
    bytes[10..12].copy_from_slice(&snapshot.layout.key_bytes.to_le_bytes());
    bytes[12..14].copy_from_slice(&snapshot.layout.max_value_bytes.to_le_bytes());
    bytes[16..24].copy_from_slice(&snapshot.generation.to_le_bytes());
    bytes[24..32].copy_from_slice(&snapshot.root.to_le_bytes());
    bytes[32..40].copy_from_slice(&snapshot.end.to_le_bytes());
    if snapshot.format_version == PIPELINED_FORMAT_VERSION {
        bytes[40..48].copy_from_slice(&snapshot.publication.to_le_bytes());
    }
    if let Some((durable, ready, failed)) = working {
        bytes[48..56].copy_from_slice(&durable.to_le_bytes());
        bytes[56..64].copy_from_slice(&ready.to_le_bytes());
        bytes[64..72].copy_from_slice(&u64::from(failed).to_le_bytes());
    }
    seal(&mut bytes);
    counters.root_writes.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let result = file.write_all_at(&bytes, offset);
    counters
        .root_write_ns
        .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
    result?;
    Ok(())
}

fn decode(bytes: &[u8], slot: Option<u64>) -> Result<Option<Snapshot>> {
    // Only the untouched slot in a newly created file may be empty. Any other
    // invalid slot is ambiguous: it could be an interrupted write or corruption
    // of an acknowledged generation, so recovery must not select an older root.
    if !checksum_valid(bytes) {
        return if bytes.iter().all(|byte| *byte == 0) {
            Ok(None)
        } else {
            Err(Error::Corrupt(
                "root slot checksum invalid; recovery is ambiguous",
            ))
        };
    }
    let format_version = u16_at(bytes, 8);
    if !((format_version == FORMAT_VERSION && &bytes[..8] == MAGIC)
        || (format_version == PACKED_FORMAT_VERSION && &bytes[..8] == PACKED_MAGIC)
        || (format_version == PIPELINED_FORMAT_VERSION && &bytes[..8] == PIPELINED_MAGIC))
    {
        return Err(Error::Corrupt("unknown file magic or format version"));
    }
    let reserved_start = if format_version == PIPELINED_FORMAT_VERSION {
        if slot.is_none() { 72 } else { 48 }
    } else {
        40
    };
    if bytes[14..16]
        .iter()
        .chain(bytes[reserved_start..CHECKSUM_START].iter())
        .any(|b| *b != 0)
    {
        return Err(Error::Corrupt("nonzero header reserved bytes"));
    }
    let layout = Layout::new(u16_at(bytes, 10), u16_at(bytes, 12))
        .map_err(|_| Error::Corrupt("invalid persisted record layout"))?;
    let s = Snapshot {
        format_version,
        layout,
        generation: u64_at(bytes, 16),
        publication: if format_version == PIPELINED_FORMAT_VERSION {
            u64_at(bytes, 40)
        } else {
            u64_at(bytes, 16)
        },
        root: u64_at(bytes, 24),
        end: u64_at(bytes, 32),
    };
    if s.generation == 0
        || s.publication == 0
        || s.publication > s.generation
        || slot.is_some_and(|slot| s.publication % 2 != slot)
        || s.end < data_start(format_version)
        || s.end % PAGE_BYTES as u64 != 0
        || s.end > i64::MAX as u64
        || (s.root != 0
            && (s.root < data_start(format_version)
                || s.root >= s.end
                || s.root % PAGE_BYTES as u64 != 0))
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
    let started = Instant::now();
    let result = file.read_exact_at(&mut bytes, 0);
    if let Some(counters) = counters {
        counters
            .root_read_ns
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
    result.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::Corrupt("truncated root headers")
        } else {
            Error::Io(e)
        }
    })?;
    match (
        decode(&bytes[..PAGE_BYTES], Some(0))?,
        decode(&bytes[PAGE_BYTES..], Some(1))?,
    ) {
        (Some(a), Some(b)) => {
            let (newer, older) = if a.publication > b.publication {
                (a, b)
            } else {
                (b, a)
            };
            if a.layout != b.layout
                || a.format_version != b.format_version
                || a.publication.abs_diff(b.publication) != 1
                || newer.generation < older.generation
                || newer.end < older.end
                || (newer.generation == older.generation
                    && (newer.format_version != PIPELINED_FORMAT_VERSION
                        || newer.root != older.root
                        || newer.end != older.end))
            {
                return Err(Error::Corrupt("root slots disagree"));
            }
            Ok(newer)
        }
        (Some(s), None) | (None, Some(s)) if s.publication == 1 && s.generation == 1 => Ok(s),
        (Some(_), None) | (None, Some(_)) => {
            Err(Error::Corrupt("missing root slot after initial generation"))
        }
        (None, None) => Err(Error::Corrupt("no valid root header")),
    }
}

/// Speculative writer state, never a reader/recovery authority. Writable open
/// resets it from the published root only while no writer can still use it.
pub(super) fn read_working_state(
    file: &File,
    counters: &OperationCounters,
) -> Result<WorkingState> {
    let mut bytes = [0; PAGE_BYTES];
    counters.root_reads.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let result = file.read_exact_at(&mut bytes, WORKING_ROOT_OFFSET);
    counters
        .root_read_ns
        .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
    result?;
    let snapshot = decode(&bytes, None)?.ok_or(Error::Corrupt("missing working root"))?;
    if snapshot.format_version != PIPELINED_FORMAT_VERSION {
        return Err(Error::Corrupt("invalid working root format"));
    }
    let durable_generation = u64_at(&bytes, 48);
    let ready = u64_at(&bytes, 56);
    let failed = u64_at(&bytes, 64);
    if durable_generation == 0
        || durable_generation > snapshot.generation
        || snapshot.generation - durable_generation > 64
        || failed > 1
    {
        return Err(Error::Corrupt("invalid pipeline durability window"));
    }
    let mut valid_bits = 0_u64;
    for delta in 1..=snapshot.generation - durable_generation {
        valid_bits |= 1 << ((durable_generation + delta) % 64);
    }
    if ready & !valid_bits != 0 {
        return Err(Error::Corrupt("invalid pipeline completion bits"));
    }
    Ok(WorkingState {
        snapshot,
        durable_generation,
        ready,
        failed: failed != 0,
    })
}
