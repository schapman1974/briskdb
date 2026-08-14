//! Process-local consumption of manifest-leased `hilo_v1` ranges.
//!
//! The manifest is the only durable allocator. This cache never restores a
//! range after process exit and never returns an issued value to a range, so
//! rollback, cancellation, constraint failures, and crashes can create gaps
//! but cannot cause reuse.

#![cfg_attr(not(feature = "experimental-vtab"), allow(dead_code))]

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::core::{
    EngineError, EngineErrorKind, EngineResult, TableId,
    generated_id::{HiloV1Id, MAX_HILO_V1_SEQUENCE},
};

/// One range durably committed by the manifest allocator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DurableHiloLease {
    table_id: TableId,
    owner_id: [u8; 32],
    fence: u64,
    first_sequence: u64,
    last_sequence: u64,
}

impl DurableHiloLease {
    pub(super) const fn new(
        table_id: TableId,
        owner_id: [u8; 32],
        fence: u64,
        first_sequence: u64,
        last_sequence: u64,
    ) -> Self {
        Self {
            table_id,
            owner_id,
            fence,
            first_sequence,
            last_sequence,
        }
    }
}

/// One irrevocably consumed ID and the durable fence that authorized it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HiloAllocation {
    id: i64,
    table_id: TableId,
    owner_id: [u8; 32],
    fence: u64,
}

impl HiloAllocation {
    pub(super) const fn id(self) -> i64 {
        self.id
    }

    pub(super) const fn table_id(self) -> TableId {
        self.table_id
    }

    pub(super) const fn owner_id(self) -> [u8; 32] {
        self.owner_id
    }

    pub(super) const fn fence(self) -> u64 {
        self.fence
    }
}

#[derive(Debug)]
struct CachedRange {
    owner_id: [u8; 32],
    fence: u64,
    next_sequence: u64,
    last_sequence: u64,
}

#[derive(Debug, Default)]
struct TableAllocator {
    range: Mutex<Option<CachedRange>>,
}

/// Shared by every `Storage` handle for one canonical root in this process.
#[derive(Debug)]
pub(super) struct HiloAllocator {
    owner_id: [u8; 32],
    tables: Mutex<HashMap<TableId, Arc<TableAllocator>>>,
}

impl HiloAllocator {
    pub(super) fn new() -> EngineResult<Self> {
        let mut owner_id = [0_u8; 32];
        getrandom::fill(&mut owner_id).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::StorageUnavailable,
                "failed to create the hilo_v1 process-incarnation identity",
                error,
            )
        })?;
        Ok(Self {
            owner_id,
            tables: Mutex::new(HashMap::new()),
        })
    }

    pub(super) const fn owner_id(&self) -> [u8; 32] {
        self.owner_id
    }

    /// Irrevocably consume one sequence, refilling through `reserve` only
    /// after the previous range is empty.
    pub(super) fn allocate<F>(&self, table_id: TableId, reserve: F) -> EngineResult<HiloAllocation>
    where
        F: FnOnce([u8; 32]) -> EngineResult<DurableHiloLease>,
    {
        let table = {
            let mut tables = self.tables.lock().map_err(|error| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    format!("hilo_v1 allocator registry is poisoned: {error}"),
                )
            })?;
            Arc::clone(
                tables
                    .entry(table_id)
                    .or_insert_with(|| Arc::new(TableAllocator::default())),
            )
        };
        let mut range = table.range.lock().map_err(|error| {
            EngineError::new(
                EngineErrorKind::Internal,
                format!("hilo_v1 table allocator is poisoned: {error}"),
            )
        })?;
        if range
            .as_ref()
            .is_none_or(|current| current.next_sequence > current.last_sequence)
        {
            let lease = reserve(self.owner_id)?;
            validate_lease(table_id, self.owner_id, lease)?;
            *range = Some(CachedRange {
                owner_id: lease.owner_id,
                fence: lease.fence,
                next_sequence: lease.first_sequence,
                last_sequence: lease.last_sequence,
            });
        }
        let current = range.as_mut().expect("a validated lease was installed");
        let sequence = current.next_sequence;
        current.next_sequence = sequence
            .checked_add(1)
            .expect("a validated hilo_v1 sequence has a successor sentinel");
        Ok(HiloAllocation {
            id: HiloV1Id::new(sequence)?.encode(),
            table_id,
            owner_id: current.owner_id,
            fence: current.fence,
        })
    }
}

fn validate_lease(
    table_id: TableId,
    owner_id: [u8; 32],
    lease: DurableHiloLease,
) -> EngineResult<()> {
    if lease.table_id != table_id
        || lease.owner_id != owner_id
        || lease.fence == 0
        || lease.first_sequence == 0
        || lease.first_sequence > lease.last_sequence
        || lease.last_sequence > MAX_HILO_V1_SEQUENCE
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "manifest returned an invalid hilo_v1 lease",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn one_durable_lease_serves_every_value_once() {
        let allocator = HiloAllocator::new().unwrap();
        let table = TableId::new(7).unwrap();
        let calls = AtomicUsize::new(0);
        let mut ids = Vec::new();
        for _ in 0..4 {
            ids.push(
                allocator
                    .allocate(table, |owner| {
                        calls.fetch_add(1, Ordering::Relaxed);
                        Ok(DurableHiloLease::new(table, owner, 1, 9, 12))
                    })
                    .unwrap()
                    .id(),
            );
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            ids,
            (9..=12)
                .map(|sequence| HiloV1Id::new(sequence).unwrap().encode())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn exhausted_cache_refills_and_never_reuses_a_value() {
        let allocator = HiloAllocator::new().unwrap();
        let table = TableId::new(1).unwrap();
        let calls = AtomicUsize::new(0);
        let mut ids = Vec::new();
        for _ in 0..3 {
            ids.push(
                allocator
                    .allocate(table, |owner| {
                        let call = calls.fetch_add(1, Ordering::Relaxed);
                        let first = 1 + u64::try_from(call).unwrap() * 2;
                        Ok(DurableHiloLease::new(
                            table,
                            owner,
                            u64::try_from(call + 1).unwrap(),
                            first,
                            first + 1,
                        ))
                    })
                    .unwrap()
                    .id(),
            );
        }
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(ids.len(), 3);
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn malformed_or_cross_table_lease_is_rejected_without_installation() {
        let allocator = HiloAllocator::new().unwrap();
        let table = TableId::new(1).unwrap();
        let other = TableId::new(2).unwrap();
        let error = allocator
            .allocate(table, |owner| {
                Ok(DurableHiloLease::new(other, owner, 1, 1, 4))
            })
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);

        let allocation = allocator
            .allocate(table, |owner| {
                Ok(DurableHiloLease::new(table, owner, 2, 5, 8))
            })
            .unwrap();
        assert_eq!(allocation.id(), HiloV1Id::new(5).unwrap().encode());
    }
}
