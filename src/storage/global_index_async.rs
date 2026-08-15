//! Fenced asynchronous maintenance and freshness watermarks for non-unique indexes.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};

use crate::{
    core::{
        CancellationToken, EngineError, EngineErrorKind, EngineResult, GlobalIndexAsyncOptions,
        GlobalIndexAsyncProcessReport, GlobalIndexAsyncShardOutcome, GlobalIndexAsyncShardReport,
        GlobalIndexAsyncShardStatus, GlobalIndexAsyncStatus, GlobalIndexId, GlobalIndexMetadata,
        GlobalIndexOutboxBatch, GlobalIndexOutboxEvent, GlobalIndexOutboxEventKind,
        GlobalIndexOwner,
    },
    sqlite_error,
};

use super::{Storage, global_index, index_outbox};

const POISON_APPLY_FAILED: i64 = 1;

#[derive(Debug, Clone, Copy)]
struct Lease {
    fence: u64,
}

#[derive(Debug, Clone, Copy)]
struct Watermark {
    cursor: u64,
    poison: Option<u64>,
}

pub(super) fn initialize_index(
    transaction: &Transaction<'_>,
    index: &GlobalIndexMetadata,
    high_waters: &[u64],
) -> EngineResult<()> {
    if index.is_unique() {
        return Ok(());
    }
    transaction
        .execute(
            "INSERT INTO briskdb_global_index_async_controls (
                 index_id, paused, rebuild_required
             ) VALUES (?1, 0, 0)
             ON CONFLICT (index_id) DO UPDATE SET
                 paused = 0, rebuild_required = 0",
            [global_index::to_sqlite_id(index.id())?],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "DELETE FROM briskdb_global_index_async_watermarks WHERE index_id = ?1",
            [global_index::to_sqlite_id(index.id())?],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "DELETE FROM briskdb_global_index_async_leases WHERE index_id = ?1",
            [global_index::to_sqlite_id(index.id())?],
        )
        .map_err(sqlite_error::storage)?;
    let mut insert = transaction
        .prepare_cached(
            "INSERT INTO briskdb_global_index_async_watermarks (
                 index_id, source_shard, applied_cursor, applied_events,
                 failure_count, last_batch_events, last_batch_micros,
                 last_applied_unix_ms, poison_cursor, poison_code
             ) VALUES (?1, ?2, ?3, 0, 0, 0, 0, ?4, NULL, NULL)",
        )
        .map_err(sqlite_error::storage)?;
    let now = now_unix_ms()?;
    for (shard, high_water) in high_waters.iter().copied().enumerate() {
        insert
            .execute(params![
                global_index::to_sqlite_id(index.id())?,
                i64::try_from(shard).map_err(|_| corrupt("source shard overflowed"))?,
                global_index::to_sqlite_u64(high_water, "initial async watermark")?,
                now,
            ])
            .map_err(sqlite_error::storage)?;
    }
    Ok(())
}

pub(super) fn uncertain_shards(
    transaction: &Transaction<'_>,
    index_id: GlobalIndexId,
    requirements: &[u64],
) -> EngineResult<Vec<u16>> {
    let rebuild_required = transaction
        .query_row(
            "SELECT rebuild_required FROM briskdb_global_index_async_controls
             WHERE index_id = ?1",
            [global_index::to_sqlite_id(index_id)?],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(sqlite_error::storage)?
        .is_none_or(|value| value != 0);
    if rebuild_required {
        return (0..requirements.len())
            .map(|shard| u16::try_from(shard).map_err(|_| corrupt("source shard overflowed")))
            .collect();
    }
    let mut statement = transaction
        .prepare_cached(
            "SELECT applied_cursor, poison_cursor
             FROM briskdb_global_index_async_watermarks
             WHERE index_id = ?1 AND source_shard = ?2",
        )
        .map_err(sqlite_error::storage)?;
    let mut uncertain = Vec::new();
    for (shard, required) in requirements.iter().copied().enumerate() {
        let row = statement
            .query_row(
                params![
                    global_index::to_sqlite_id(index_id)?,
                    i64::try_from(shard).map_err(|_| corrupt("source shard overflowed"))?,
                ],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
            )
            .optional()
            .map_err(sqlite_error::storage)?;
        let fresh = match row {
            Some((cursor, None)) => {
                global_index::from_sqlite_u64(cursor, "async watermark")? >= required
            }
            Some((_, Some(poison))) => {
                global_index::from_sqlite_u64(poison, "async poison cursor")?;
                false
            }
            None => false,
        };
        if !fresh {
            uncertain.push(u16::try_from(shard).map_err(|_| corrupt("source shard overflowed"))?);
        }
    }
    Ok(uncertain)
}

pub(super) fn process_index(
    storage: &Storage,
    index: &GlobalIndexMetadata,
    owner_id: [u8; 16],
    options: GlobalIndexAsyncOptions,
    cancellation: &CancellationToken,
) -> EngineResult<GlobalIndexAsyncProcessReport> {
    if index.is_unique() {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            format!(
                "global index {} is unique and has no async consumer",
                index.id()
            ),
        ));
    }
    let snapshot = status(storage, index)?;
    if snapshot.is_paused() || snapshot.rebuild_required() {
        let outcome = if snapshot.is_paused() {
            GlobalIndexAsyncShardOutcome::Paused
        } else {
            GlobalIndexAsyncShardOutcome::RebuildRequired
        };
        return Ok(GlobalIndexAsyncProcessReport::new(
            index.id(),
            snapshot
                .shards()
                .iter()
                .map(|shard| {
                    GlobalIndexAsyncShardReport::new(
                        shard.shard(),
                        outcome,
                        shard.applied().get(),
                        shard.applied().get(),
                        0,
                    )
                })
                .collect(),
        ));
    }
    let mut reports = Vec::with_capacity(usize::from(storage.shard_count()));
    let mut processed = Vec::new();
    for shard_status in snapshot.shards() {
        if cancellation.is_cancelled() {
            return Err(cancelled("while processing a global index"));
        }
        if let Some(poison) = shard_status.poison_cursor() {
            reports.push(GlobalIndexAsyncShardReport::new(
                shard_status.shard(),
                GlobalIndexAsyncShardOutcome::Poisoned,
                shard_status.applied().get(),
                poison.get(),
                0,
            ));
        } else if shard_status.is_fresh() {
            let shard = shard_status.shard();
            let applied = shard_status.applied().get();
            match index_outbox::consumer_cursor(storage, index.id(), shard)? {
                Some(cursor) if cursor == applied => {
                    reports.push(GlobalIndexAsyncShardReport::new(
                        shard,
                        GlobalIndexAsyncShardOutcome::Current,
                        applied,
                        applied,
                        0,
                    ));
                }
                Some(cursor) if cursor < applied => {
                    index_outbox::advance_consumer(
                        storage,
                        index.id(),
                        shard,
                        applied,
                        cancellation,
                    )?;
                    processed.push(shard);
                    reports.push(GlobalIndexAsyncShardReport::new(
                        shard,
                        GlobalIndexAsyncShardOutcome::Current,
                        applied,
                        applied,
                        0,
                    ));
                }
                Some(_) | None => {
                    mark_rebuild_required(storage, index.id())?;
                    reports.push(GlobalIndexAsyncShardReport::new(
                        shard,
                        GlobalIndexAsyncShardOutcome::RebuildRequired,
                        applied,
                        applied,
                        0,
                    ));
                }
            }
        } else {
            processed.push(shard_status.shard());
            reports.push(process_shard(
                storage,
                index,
                shard_status.shard(),
                owner_id,
                options,
                cancellation,
            )?);
        }
    }
    for shard in processed {
        if cancellation.is_cancelled() {
            break;
        }
        let _ = index_outbox::prune(storage, shard, options.batch_events(), cancellation);
    }
    Ok(GlobalIndexAsyncProcessReport::new(index.id(), reports))
}

#[allow(clippy::too_many_arguments)]
fn process_shard(
    storage: &Storage,
    index: &GlobalIndexMetadata,
    shard: u16,
    owner_id: [u8; 16],
    options: GlobalIndexAsyncOptions,
    cancellation: &CancellationToken,
) -> EngineResult<GlobalIndexAsyncShardReport> {
    let watermark = match load_watermark(storage, index, shard)? {
        LoadWatermark::Ready(watermark) => watermark,
        LoadWatermark::Paused => {
            return Ok(GlobalIndexAsyncShardReport::new(
                shard,
                GlobalIndexAsyncShardOutcome::Paused,
                0,
                0,
                0,
            ));
        }
        LoadWatermark::RebuildRequired => {
            return Ok(GlobalIndexAsyncShardReport::new(
                shard,
                GlobalIndexAsyncShardOutcome::RebuildRequired,
                0,
                0,
                0,
            ));
        }
        LoadWatermark::Missing => {
            mark_rebuild_required(storage, index.id())?;
            return Ok(GlobalIndexAsyncShardReport::new(
                shard,
                GlobalIndexAsyncShardOutcome::RebuildRequired,
                0,
                0,
                0,
            ));
        }
    };
    if watermark.poison.is_some() {
        return Ok(GlobalIndexAsyncShardReport::new(
            shard,
            GlobalIndexAsyncShardOutcome::Poisoned,
            watermark.cursor,
            watermark.cursor,
            0,
        ));
    }
    let now = now_unix_ms()?;
    let Some(lease) = acquire_lease(storage, index, shard, owner_id, options.lease_ms(), now)?
    else {
        return Ok(GlobalIndexAsyncShardReport::new(
            shard,
            GlobalIndexAsyncShardOutcome::LeasedElsewhere,
            watermark.cursor,
            watermark.cursor,
            0,
        ));
    };
    let batch = match index_outbox::read_batch(
        storage,
        index.id(),
        shard,
        watermark.cursor,
        options.batch_events(),
        cancellation,
    ) {
        Ok(batch) => batch,
        Err(error) if error.kind() == EngineErrorKind::FailedPrecondition => {
            mark_rebuild_required(storage, index.id())?;
            return Ok(GlobalIndexAsyncShardReport::new(
                shard,
                GlobalIndexAsyncShardOutcome::RebuildRequired,
                watermark.cursor,
                watermark.cursor,
                0,
            ));
        }
        Err(error) => return Err(error),
    };
    let started = Instant::now();
    let applied = match apply_batch(
        storage,
        index,
        owner_id,
        lease,
        &batch,
        options,
        started,
        cancellation,
    ) {
        Ok(applied) => applied,
        Err(error)
            if matches!(
                error.kind(),
                EngineErrorKind::InvalidArgument | EngineErrorKind::LimitExceeded
            ) =>
        {
            let poison_cursor = batch
                .events()
                .first()
                .map(|event| event.cursor().get())
                .unwrap_or_else(|| watermark.cursor.saturating_add(1));
            record_poison(
                storage,
                index,
                shard,
                owner_id,
                lease,
                watermark.cursor,
                poison_cursor,
            )?;
            return Ok(GlobalIndexAsyncShardReport::new(
                shard,
                GlobalIndexAsyncShardOutcome::Poisoned,
                watermark.cursor,
                watermark.cursor,
                0,
            ));
        }
        Err(error) => return Err(error),
    };
    abort_at_test_boundary("consumer-before-advance");
    index_outbox::advance_consumer(storage, index.id(), shard, applied.through, cancellation)?;
    let outcome = if applied.through == watermark.cursor && applied.events == 0 {
        GlobalIndexAsyncShardOutcome::Current
    } else {
        GlobalIndexAsyncShardOutcome::Applied
    };
    Ok(GlobalIndexAsyncShardReport::new(
        shard,
        outcome,
        watermark.cursor,
        applied.through,
        applied.events,
    ))
}

struct AppliedBatch {
    through: u64,
    events: u64,
}

#[allow(clippy::too_many_arguments)]
fn apply_batch(
    storage: &Storage,
    index: &GlobalIndexMetadata,
    owner_id: [u8; 16],
    lease: Lease,
    batch: &GlobalIndexOutboxBatch,
    options: GlobalIndexAsyncOptions,
    started: Instant,
    cancellation: &CancellationToken,
) -> EngineResult<AppliedBatch> {
    if cancellation.is_cancelled() {
        return Err(cancelled("before applying a global-index batch"));
    }
    let (mut connection, _) = global_index::open_existing(&storage.root)?
        .ok_or_else(|| corrupt("ready global index has no physical storage"))?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    global_index::validate_physical_authority(&transaction, index, storage.shard_count())?;
    validate_lease(
        &transaction,
        index.id(),
        batch.shard(),
        owner_id,
        lease.fence,
        now_unix_ms()?,
    )?;
    let current = transaction
        .query_row(
            "SELECT applied_cursor FROM briskdb_global_index_async_watermarks
             WHERE index_id = ?1 AND source_shard = ?2 AND poison_cursor IS NULL",
            params![
                global_index::to_sqlite_id(index.id())?,
                i64::from(batch.shard())
            ],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(sqlite_error::storage)?
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "global-index async watermark is unavailable",
            )
        })?;
    let current = global_index::from_sqlite_u64(current, "async watermark")?;
    if current != batch.after().get() {
        return Err(EngineError::new(
            EngineErrorKind::Busy,
            "global-index async watermark changed during replay",
        ));
    }
    let poison = injected_poison_cursor();
    let mut row_delta = 0_i64;
    for event in batch.events() {
        if cancellation.is_cancelled() {
            return Err(cancelled("while applying a global-index batch"));
        }
        if poison == Some(event.cursor().get()) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "injected global-index poison event",
            ));
        }
        row_delta = row_delta
            .checked_add(apply_event(&transaction, index.id(), event)?)
            .ok_or_else(|| corrupt("global-index entry count overflowed"))?;
    }
    if row_delta != 0 {
        let build_changed = transaction
            .execute(
                "UPDATE briskdb_global_index_builds
                 SET indexed_rows = indexed_rows + ?1
                 WHERE index_id = ?2 AND indexed_rows + ?1 >= 0",
                params![row_delta, global_index::to_sqlite_id(index.id())?],
            )
            .map_err(sqlite_error::storage)?;
        let checkpoint_changed = transaction
            .execute(
                "UPDATE briskdb_global_index_checkpoints
                 SET indexed_rows = indexed_rows + ?1
                 WHERE index_id = ?2 AND source_shard = ?3 AND indexed_rows + ?1 >= 0",
                params![
                    row_delta,
                    global_index::to_sqlite_id(index.id())?,
                    i64::from(batch.shard())
                ],
            )
            .map_err(sqlite_error::storage)?;
        if build_changed != 1 || checkpoint_changed != 1 {
            return Err(corrupt("global-index async row accounting is inconsistent"));
        }
    }
    let through = if batch.events().len() < options.batch_events() {
        batch.high_water().get()
    } else {
        batch
            .events()
            .last()
            .map(|event| event.cursor().get())
            .unwrap_or(batch.high_water().get())
    };
    let events = u64::try_from(batch.events().len())
        .map_err(|_| corrupt("global-index async batch count overflowed"))?;
    let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    abort_at_test_boundary("apply-before-watermark");
    let changed = transaction
        .execute(
            "UPDATE briskdb_global_index_async_watermarks SET
                 applied_cursor = ?1,
                 applied_events = applied_events + ?2,
                 last_batch_events = ?2,
                 last_batch_micros = ?3,
                 last_applied_unix_ms = ?4
             WHERE index_id = ?5 AND source_shard = ?6
               AND applied_cursor = ?7 AND poison_cursor IS NULL",
            params![
                global_index::to_sqlite_u64(through, "async watermark")?,
                global_index::to_sqlite_u64(events, "async applied event count")?,
                global_index::to_sqlite_u64(micros, "async batch duration")?,
                now_unix_ms()?,
                global_index::to_sqlite_id(index.id())?,
                i64::from(batch.shard()),
                global_index::to_sqlite_u64(current, "async watermark")?,
            ],
        )
        .map_err(sqlite_error::storage)?;
    if changed != 1 {
        return Err(EngineError::new(
            EngineErrorKind::Busy,
            "global-index async watermark was fenced",
        ));
    }
    abort_at_test_boundary("apply-before-commit");
    transaction.commit().map_err(sqlite_error::storage)?;
    abort_at_test_boundary("apply-after-commit");
    Ok(AppliedBatch { through, events })
}

fn apply_event(
    transaction: &Transaction<'_>,
    index_id: GlobalIndexId,
    event: &GlobalIndexOutboxEvent,
) -> EngineResult<i64> {
    let mut delta = 0_i64;
    let mut reusable_ordinal = None;
    if let Some(owner) = event.old_owner() {
        let deleted = delete_owner(transaction, index_id, owner)?;
        reusable_ordinal = deleted.ordinal;
        delta -=
            i64::try_from(deleted.rows).map_err(|_| corrupt("deleted row count overflowed"))?;
    }
    match event.kind() {
        GlobalIndexOutboxEventKind::Insert | GlobalIndexOutboxEventKind::Update => {
            let key = event
                .new_key()
                .ok_or_else(|| corrupt("global-index insert event has no new key"))?;
            let owner = event
                .new_owner()
                .ok_or_else(|| corrupt("global-index insert event has no new owner"))?;
            let duplicate = delete_owner(transaction, index_id, owner)?;
            delta -= i64::try_from(duplicate.rows)
                .map_err(|_| corrupt("deleted row count overflowed"))?;
            let ordinal = duplicate
                .ordinal
                .or(reusable_ordinal)
                .map(Ok)
                .unwrap_or_else(|| next_ordinal(transaction, index_id, owner.source_shard()))?;
            transaction
                .execute(
                    "INSERT INTO briskdb_global_index_entries (
                         index_id, encoded_key, source_shard, source_ordinal, source_locator
                     ) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        global_index::to_sqlite_id(index_id)?,
                        key.as_bytes(),
                        i64::from(owner.source_shard()),
                        ordinal,
                        owner.locator(),
                    ],
                )
                .map_err(sqlite_error::storage)?;
            delta += 1;
        }
        GlobalIndexOutboxEventKind::Delete | GlobalIndexOutboxEventKind::Tombstone => {}
    }
    Ok(delta)
}

struct DeletedOwner {
    ordinal: Option<i64>,
    rows: usize,
}

fn delete_owner(
    transaction: &Transaction<'_>,
    index_id: GlobalIndexId,
    owner: &GlobalIndexOwner,
) -> EngineResult<DeletedOwner> {
    let ordinal = transaction
        .query_row(
            "SELECT MIN(source_ordinal) FROM briskdb_global_index_entries
             WHERE index_id = ?1 AND source_shard = ?2 AND source_locator = ?3",
            params![
                global_index::to_sqlite_id(index_id)?,
                i64::from(owner.source_shard()),
                owner.locator(),
            ],
            |row| row.get::<_, Option<i64>>(0),
        )
        .map_err(sqlite_error::storage)?;
    let rows = transaction
        .execute(
            "DELETE FROM briskdb_global_index_entries
             WHERE index_id = ?1 AND source_shard = ?2 AND source_locator = ?3",
            params![
                global_index::to_sqlite_id(index_id)?,
                i64::from(owner.source_shard()),
                owner.locator(),
            ],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "DELETE FROM briskdb_global_index_read_repairs
             WHERE index_id = ?1 AND source_shard = ?2 AND source_locator = ?3",
            params![
                global_index::to_sqlite_id(index_id)?,
                i64::from(owner.source_shard()),
                owner.locator(),
            ],
        )
        .map_err(sqlite_error::storage)?;
    Ok(DeletedOwner { ordinal, rows })
}

fn next_ordinal(
    transaction: &Transaction<'_>,
    index_id: GlobalIndexId,
    shard: u16,
) -> EngineResult<i64> {
    let maximum = transaction
        .query_row(
            "SELECT MAX(source_ordinal) FROM briskdb_global_index_entries
             WHERE index_id = ?1 AND source_shard = ?2",
            params![global_index::to_sqlite_id(index_id)?, i64::from(shard)],
            |row| row.get::<_, Option<i64>>(0),
        )
        .map_err(sqlite_error::storage)?;
    maximum.unwrap_or(-1).checked_add(1).ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::LimitExceeded,
            "global-index source ordinals are exhausted",
        )
    })
}

fn acquire_lease(
    storage: &Storage,
    index: &GlobalIndexMetadata,
    shard: u16,
    owner_id: [u8; 16],
    lease_ms: u64,
    now: i64,
) -> EngineResult<Option<Lease>> {
    let (mut connection, _) = global_index::open_existing(&storage.root)?
        .ok_or_else(|| corrupt("ready global index has no physical storage"))?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    global_index::validate_physical_authority(&transaction, index, storage.shard_count())?;
    let control = transaction
        .query_row(
            "SELECT paused, rebuild_required FROM briskdb_global_index_async_controls
             WHERE index_id = ?1",
            [global_index::to_sqlite_id(index.id())?],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(sqlite_error::storage)?;
    if control.is_some_and(|(paused, rebuild)| paused != 0 || rebuild != 0) {
        transaction.commit().map_err(sqlite_error::storage)?;
        return Ok(None);
    }
    let expires = now
        .checked_add(i64::try_from(lease_ms).map_err(|_| corrupt("lease duration overflowed"))?)
        .ok_or_else(|| corrupt("lease deadline overflowed"))?;
    let existing = transaction
        .query_row(
            "SELECT owner_id, fence_token, expires_unix_ms
             FROM briskdb_global_index_async_leases
             WHERE index_id = ?1 AND source_shard = ?2",
            params![global_index::to_sqlite_id(index.id())?, i64::from(shard)],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error::storage)?;
    let fence = match existing {
        None => {
            transaction
                .execute(
                    "INSERT INTO briskdb_global_index_async_leases (
                         index_id, source_shard, owner_id, fence_token, expires_unix_ms
                     ) VALUES (?1, ?2, ?3, 1, ?4)",
                    params![
                        global_index::to_sqlite_id(index.id())?,
                        i64::from(shard),
                        owner_id.as_slice(),
                        expires,
                    ],
                )
                .map_err(sqlite_error::storage)?;
            1
        }
        Some((owner, fence, _)) if owner.as_slice() == owner_id => {
            transaction
                .execute(
                    "UPDATE briskdb_global_index_async_leases SET expires_unix_ms = ?1
                     WHERE index_id = ?2 AND source_shard = ?3",
                    params![
                        expires,
                        global_index::to_sqlite_id(index.id())?,
                        i64::from(shard)
                    ],
                )
                .map_err(sqlite_error::storage)?;
            global_index::from_sqlite_u64(fence, "async lease fence")?
        }
        Some((_, _, existing_expires)) if existing_expires > now => {
            transaction.commit().map_err(sqlite_error::storage)?;
            return Ok(None);
        }
        Some((_, fence, _)) => {
            let fence = global_index::from_sqlite_u64(fence, "async lease fence")?
                .checked_add(1)
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::LimitExceeded,
                        "global-index async lease fences are exhausted",
                    )
                })?;
            transaction
                .execute(
                    "UPDATE briskdb_global_index_async_leases SET
                         owner_id = ?1, fence_token = ?2, expires_unix_ms = ?3
                     WHERE index_id = ?4 AND source_shard = ?5",
                    params![
                        owner_id.as_slice(),
                        global_index::to_sqlite_u64(fence, "async lease fence")?,
                        expires,
                        global_index::to_sqlite_id(index.id())?,
                        i64::from(shard),
                    ],
                )
                .map_err(sqlite_error::storage)?;
            fence
        }
    };
    abort_at_test_boundary("lease-before-commit");
    transaction.commit().map_err(sqlite_error::storage)?;
    Ok(Some(Lease { fence }))
}

fn validate_lease(
    transaction: &Transaction<'_>,
    index_id: GlobalIndexId,
    shard: u16,
    owner_id: [u8; 16],
    fence: u64,
    now: i64,
) -> EngineResult<()> {
    let valid = transaction
        .query_row(
            "SELECT 1 FROM briskdb_global_index_async_leases
             WHERE index_id = ?1 AND source_shard = ?2 AND owner_id = ?3
               AND fence_token = ?4 AND expires_unix_ms >= ?5",
            params![
                global_index::to_sqlite_id(index_id)?,
                i64::from(shard),
                owner_id.as_slice(),
                global_index::to_sqlite_u64(fence, "async lease fence")?,
                now,
            ],
            |_| Ok(()),
        )
        .optional()
        .map_err(sqlite_error::storage)?
        .is_some();
    if valid {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::Busy,
            "global-index async lease was lost",
        ))
    }
}

enum LoadWatermark {
    Ready(Watermark),
    Paused,
    RebuildRequired,
    Missing,
}

fn load_watermark(
    storage: &Storage,
    index: &GlobalIndexMetadata,
    shard: u16,
) -> EngineResult<LoadWatermark> {
    let (connection, _) = global_index::open_existing(&storage.root)?
        .ok_or_else(|| corrupt("ready global index has no physical storage"))?;
    let control = connection
        .query_row(
            "SELECT paused, rebuild_required FROM briskdb_global_index_async_controls
             WHERE index_id = ?1",
            [global_index::to_sqlite_id(index.id())?],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(sqlite_error::storage)?;
    match control {
        Some((1, _)) => return Ok(LoadWatermark::Paused),
        Some((_, 1)) => return Ok(LoadWatermark::RebuildRequired),
        None => return Ok(LoadWatermark::Missing),
        Some((0, 0)) => {}
        Some(_) => return Err(corrupt("global-index async control is invalid")),
    }
    connection
        .query_row(
            "SELECT applied_cursor, poison_cursor
             FROM briskdb_global_index_async_watermarks
             WHERE index_id = ?1 AND source_shard = ?2",
            params![global_index::to_sqlite_id(index.id())?, i64::from(shard)],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
        )
        .optional()
        .map_err(sqlite_error::storage)?
        .map(|(cursor, poison)| {
            Ok(LoadWatermark::Ready(Watermark {
                cursor: global_index::from_sqlite_u64(cursor, "async watermark")?,
                poison: poison
                    .map(|value| global_index::from_sqlite_u64(value, "async poison cursor"))
                    .transpose()?,
            }))
        })
        .unwrap_or(Ok(LoadWatermark::Missing))
}

pub(super) fn mark_rebuild_required(
    storage: &Storage,
    index_id: GlobalIndexId,
) -> EngineResult<()> {
    let (connection, _) = global_index::open_existing(&storage.root)?
        .ok_or_else(|| corrupt("ready global index has no physical storage"))?;
    connection
        .execute(
            "UPDATE briskdb_global_index_async_controls SET rebuild_required = 1
             WHERE index_id = ?1",
            [global_index::to_sqlite_id(index_id)?],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn record_poison(
    storage: &Storage,
    index: &GlobalIndexMetadata,
    shard: u16,
    owner_id: [u8; 16],
    lease: Lease,
    expected_cursor: u64,
    cursor: u64,
) -> EngineResult<()> {
    let (mut connection, _) = global_index::open_existing(&storage.root)?
        .ok_or_else(|| corrupt("ready global index has no physical storage"))?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    validate_lease(
        &transaction,
        index.id(),
        shard,
        owner_id,
        lease.fence,
        now_unix_ms()?,
    )?;
    let changed = transaction
        .execute(
            "UPDATE briskdb_global_index_async_watermarks SET
                 failure_count = failure_count + 1,
                 poison_cursor = ?1,
                 poison_code = ?2
             WHERE index_id = ?3 AND source_shard = ?4
               AND applied_cursor = ?5 AND poison_cursor IS NULL",
            params![
                global_index::to_sqlite_u64(cursor, "async poison cursor")?,
                POISON_APPLY_FAILED,
                global_index::to_sqlite_id(index.id())?,
                i64::from(shard),
                global_index::to_sqlite_u64(expected_cursor, "async watermark")?,
            ],
        )
        .map_err(sqlite_error::storage)?;
    if changed != 1 {
        return Err(EngineError::new(
            EngineErrorKind::Busy,
            "global-index async poison record was fenced",
        ));
    }
    transaction.commit().map_err(sqlite_error::storage)
}

pub(super) fn set_paused(
    storage: &Storage,
    index: &GlobalIndexMetadata,
    paused: bool,
) -> EngineResult<()> {
    if index.is_unique() {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "unique global indexes do not use asynchronous maintenance",
        ));
    }
    let (connection, _) = global_index::open_existing(&storage.root)?
        .ok_or_else(|| corrupt("ready global index has no physical storage"))?;
    let changed = connection
        .execute(
            "UPDATE briskdb_global_index_async_controls SET paused = ?1
             WHERE index_id = ?2",
            params![i64::from(paused), global_index::to_sqlite_id(index.id())?],
        )
        .map_err(sqlite_error::storage)?;
    if changed != 1 {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "global index has no initialized asynchronous state; rebuild it",
        ));
    }
    Ok(())
}

pub(super) fn status(
    storage: &Storage,
    index: &GlobalIndexMetadata,
) -> EngineResult<GlobalIndexAsyncStatus> {
    if index.is_unique() {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "unique global indexes do not use asynchronous maintenance",
        ));
    }
    let high_waters = index_outbox::snapshot_high_waters(storage)?;
    let (connection, _) = global_index::open_existing(&storage.root)?
        .ok_or_else(|| corrupt("ready global index has no physical storage"))?;
    let control = connection
        .query_row(
            "SELECT paused, rebuild_required FROM briskdb_global_index_async_controls
             WHERE index_id = ?1",
            [global_index::to_sqlite_id(index.id())?],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(sqlite_error::storage)?
        .unwrap_or((0, 1));
    let now = now_unix_ms()?;
    let mut shards = Vec::with_capacity(high_waters.len());
    for (shard, high_water) in high_waters.into_iter().enumerate() {
        let shard = u16::try_from(shard).map_err(|_| corrupt("source shard overflowed"))?;
        let watermark = connection
            .query_row(
                "SELECT applied_cursor, applied_events, failure_count,
                        last_batch_events, last_batch_micros, poison_cursor
                 FROM briskdb_global_index_async_watermarks
                 WHERE index_id = ?1 AND source_shard = ?2",
                params![global_index::to_sqlite_id(index.id())?, i64::from(shard)],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(sqlite_error::storage)?
            .unwrap_or((0, 0, 0, 0, 0, None));
        let lease = connection
            .query_row(
                "SELECT fence_token, expires_unix_ms
                 FROM briskdb_global_index_async_leases
                 WHERE index_id = ?1 AND source_shard = ?2",
                params![global_index::to_sqlite_id(index.id())?, i64::from(shard)],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .map_err(sqlite_error::storage)?
            .unwrap_or((0, 0));
        shards.push(GlobalIndexAsyncShardStatus::new(
            shard,
            global_index::from_sqlite_u64(watermark.0, "async watermark")?,
            high_water,
            global_index::from_sqlite_u64(watermark.1, "async applied count")?,
            global_index::from_sqlite_u64(watermark.2, "async failure count")?,
            global_index::from_sqlite_u64(watermark.3, "async last batch count")?,
            global_index::from_sqlite_u64(watermark.4, "async batch duration")?,
            watermark
                .5
                .map(|value| global_index::from_sqlite_u64(value, "async poison cursor"))
                .transpose()?,
            global_index::from_sqlite_u64(lease.0, "async lease fence")?,
            lease.1 > now,
        ));
    }
    Ok(GlobalIndexAsyncStatus::new(
        index.id(),
        control.0 != 0,
        control.1 != 0,
        shards,
    ))
}

fn now_unix_ms() -> EngineResult<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::StorageUnavailable,
                "system clock is before the Unix epoch",
                error,
            )
        })?;
    i64::try_from(duration.as_millis()).map_err(|_| {
        EngineError::new(
            EngineErrorKind::NumericOutOfRange,
            "system clock cannot be represented by global-index storage",
        )
    })
}

fn cancelled(context: &str) -> EngineError {
    EngineError::new(
        EngineErrorKind::Cancelled,
        format!("operation cancelled {context}"),
    )
}

fn corrupt(diagnostic: impl Into<String>) -> EngineError {
    global_index::corrupt(diagnostic)
}

#[cfg(test)]
fn injected_poison_cursor() -> Option<u64> {
    std::env::var("BRISKDB_GLOBAL_INDEX_ASYNC_POISON_CURSOR")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
}

#[cfg(not(test))]
fn injected_poison_cursor() -> Option<u64> {
    None
}

#[cfg(test)]
fn abort_at_test_boundary(boundary: &str) {
    if std::env::var("BRISKDB_GLOBAL_INDEX_ASYNC_ABORT_POINT").as_deref() == Ok(boundary) {
        std::process::abort();
    }
}

#[cfg(not(test))]
fn abort_at_test_boundary(_boundary: &str) {}

#[cfg(all(test, unix))]
mod tests {
    use std::{path::Path, process::Command, sync::Arc, time::Duration};

    use rusqlite::Connection;

    use crate::{
        Statement, Value,
        core::{
            Database, Engine, GlobalIndexAsyncOptions, GlobalIndexDeclaration, GlobalIndexKeyPart,
            GlobalIndexKeySource, GlobalIndexKeyType, GlobalIndexStorageTopology, ShardKeyMetadata,
            ShardKeyType, TableDeclaration,
        },
    };

    fn setup(root: &Path) -> (crate::GlobalIndexId, String) {
        let mut database = Database::open(root, 2).unwrap();
        database
            .broadcast(
                "CREATE TABLE async_abort (
                     tenant_id TEXT NOT NULL PRIMARY KEY,
                     email TEXT NOT NULL
                 ) STRICT",
            )
            .unwrap();
        let logical = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical,
                    "async_abort",
                    ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
        let table = database
            .catalog()
            .table("default", "async_abort")
            .unwrap()
            .unwrap()
            .id();
        let index = database
            .create_global_index(
                GlobalIndexDeclaration::new(
                    table,
                    "async_abort_email",
                    vec![GlobalIndexKeyPart::new(
                        GlobalIndexKeySource::column("email").unwrap(),
                        GlobalIndexKeyType::Text,
                    )],
                )
                .unwrap()
                .with_topology(GlobalIndexStorageTopology::selected_v1()),
            )
            .unwrap();
        database.build_global_index(index).unwrap();
        let route = (0_u64..100_000)
            .map(|value| format!("async-abort-{value}"))
            .find(|route| database.shard_for_key(route.as_bytes()) == 0)
            .unwrap();
        let database = Arc::new(database);
        let engine = Engine::from_database(Arc::clone(&database));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let session = engine.session();
            session.set_routing_key(&route).await.unwrap();
            engine
                .execute_write(
                    &session,
                    Statement::new(
                        "INSERT INTO async_abort (tenant_id, email) VALUES (?1, ?2)",
                        vec![route.clone().into(), Value::from("abort@example.test")],
                    ),
                )
                .await
                .unwrap();
        });
        (index, route)
    }

    #[test]
    fn async_index_abort_child() {
        let Ok(root) = std::env::var("BRISKDB_ASYNC_ABORT_ROOT") else {
            return;
        };
        let index = crate::GlobalIndexId::new(
            std::env::var("BRISKDB_ASYNC_ABORT_INDEX")
                .unwrap()
                .parse()
                .unwrap(),
        )
        .unwrap();
        let database = Database::open(root, 2).unwrap();
        database
            .process_global_index_async(index, GlobalIndexAsyncOptions::new(64, 100, 5).unwrap())
            .unwrap();
    }

    #[test]
    fn async_index_poison_child() {
        let Ok(root) = std::env::var("BRISKDB_ASYNC_POISON_ROOT") else {
            return;
        };
        let index = crate::GlobalIndexId::new(
            std::env::var("BRISKDB_ASYNC_POISON_INDEX")
                .unwrap()
                .parse()
                .unwrap(),
        )
        .unwrap();
        let database = Database::open(root, 2).unwrap();
        database
            .process_global_index_async(index, GlobalIndexAsyncOptions::new(64, 5_000, 5).unwrap())
            .unwrap();
    }

    #[test]
    fn poison_is_durable_and_an_operator_rebuild_clears_it() {
        let temp = tempfile::tempdir().unwrap();
        let (index, _) = setup(temp.path());
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::global_index_async::tests::async_index_poison_child",
                "--nocapture",
            ])
            .env("BRISKDB_ASYNC_POISON_ROOT", temp.path())
            .env("BRISKDB_ASYNC_POISON_INDEX", index.get().to_string())
            .env("BRISKDB_GLOBAL_INDEX_ASYNC_POISON_CURSOR", "1")
            .status()
            .unwrap();
        assert!(status.success());

        let mut database = Database::open(temp.path(), 2).unwrap();
        let poisoned = database.global_index_async_status(index).unwrap();
        assert_eq!(poisoned.shards()[0].poison_cursor().unwrap().get(), 1);
        assert_eq!(poisoned.shards()[0].failure_count(), 1);
        assert!(!poisoned.is_fresh());

        database.rebuild_global_index(index).unwrap();
        let rebuilt = database.global_index_async_status(index).unwrap();
        assert!(rebuilt.is_fresh());
        assert!(
            rebuilt
                .shards()
                .iter()
                .all(|shard| shard.poison_cursor().is_none())
        );
    }

    #[test]
    fn restart_recovers_every_async_apply_and_watermark_boundary() {
        for boundary in [
            "lease-before-commit",
            "apply-before-watermark",
            "apply-before-commit",
            "apply-after-commit",
            "consumer-before-advance",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let (index, _) = setup(temp.path());
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "storage::global_index_async::tests::async_index_abort_child",
                    "--nocapture",
                ])
                .env("BRISKDB_ASYNC_ABORT_ROOT", temp.path())
                .env("BRISKDB_ASYNC_ABORT_INDEX", index.get().to_string())
                .env("BRISKDB_GLOBAL_INDEX_ASYNC_ABORT_POINT", boundary)
                .status()
                .unwrap();
            assert!(!status.success(), "child did not abort at {boundary}");
            std::thread::sleep(Duration::from_millis(120));
            let database = Database::open(temp.path(), 2).unwrap();
            database
                .process_global_index_async(
                    index,
                    GlobalIndexAsyncOptions::new(64, 100, 5).unwrap(),
                )
                .unwrap();
            let status = database.global_index_async_status(index).unwrap();
            assert!(status.is_fresh(), "boundary {boundary}");
            assert_eq!(
                status
                    .shards()
                    .iter()
                    .map(|shard| shard.applied_events())
                    .sum::<u64>(),
                1,
                "boundary {boundary}"
            );
            assert_eq!(
                database.global_index_outbox_status().unwrap()[0].retained_events(),
                0,
                "boundary {boundary} left an acknowledged event retained"
            );
            let authority =
                Connection::open(temp.path().join("global-indexes/global.sqlite")).unwrap();
            assert_eq!(
                authority
                    .query_row(
                        "SELECT COUNT(*) FROM briskdb_global_index_entries WHERE index_id = ?1",
                        [i64::try_from(index.get()).unwrap()],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                1,
                "boundary {boundary}"
            );
        }
    }
}
