//! Conservative shard-local Bloom and min/max summaries.

use std::cmp::Ordering;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use crate::{
    core::{
        CancellationToken, CanonicalIndexKey, EngineError, EngineErrorKind, EngineResult,
        GLOBAL_INDEX_SHARD_SUMMARY_BLOOM_BYTES, GLOBAL_INDEX_SHARD_SUMMARY_FORMAT_VERSION,
        GlobalIndexId, GlobalIndexMetadata, GlobalIndexShardSummaryPredicate,
        GlobalIndexShardSummaryPrunedShard, GlobalIndexShardSummaryReadResolution,
        GlobalIndexShardSummaryRebuildReport, GlobalIndexShardSummaryShardStatus,
        GlobalIndexShardSummaryState, GlobalIndexShardSummaryStatus, ShardSummaryPruningReason,
    },
    sqlite_error,
};

use super::{Storage, attach_storage_authorizer, global_index};

const TABLE: &str = "briskdb_global_index_shard_summaries";
const BLOOM_HASHES: u32 = 7;
const SATURATION_PERCENT: u64 = 95;
const STATE_BUILDING: i64 = 1;
const STATE_READY: i64 = 2;
const STATE_STALE: i64 = 3;
const BLOOM_DOMAIN: &[u8] = b"briskdb.shard-summary.bloom.v1\0";

const TABLE_SCHEMA_SQL: &str = "CREATE TABLE briskdb_global_index_shard_summaries (
    index_id INTEGER PRIMARY KEY CHECK (index_id > 0),
    format_version INTEGER NOT NULL CHECK (format_version = 1),
    definition_digest BLOB NOT NULL CHECK (
        typeof(definition_digest) = 'blob' AND length(definition_digest) = 32
    ),
    summary_state INTEGER NOT NULL CHECK (summary_state IN (1, 2, 3)),
    bloom_bits BLOB NOT NULL CHECK (
        typeof(bloom_bits) = 'blob' AND length(bloom_bits) = 16384
    ),
    bloom_hashes INTEGER NOT NULL CHECK (bloom_hashes = 7),
    bloom_set_bits INTEGER NOT NULL CHECK (
        bloom_set_bits >= 0 AND bloom_set_bits <= 131072
    ),
    min_key BLOB,
    max_key BLOB,
    observed_rows INTEGER NOT NULL CHECK (observed_rows >= 0),
    additions INTEGER NOT NULL CHECK (additions >= 0),
    saturated INTEGER NOT NULL CHECK (saturated IN (0, 1)),
    CHECK (min_key IS NULL OR (typeof(min_key) = 'blob' AND length(min_key) > 0)),
    CHECK (max_key IS NULL OR (typeof(max_key) = 'blob' AND length(max_key) > 0)),
    CHECK ((min_key IS NULL) = (max_key IS NULL))
) STRICT, WITHOUT ROWID";

const SCHEMA_SQL: &str = "CREATE TABLE briskdb_global_index_shard_summaries (
    index_id INTEGER PRIMARY KEY CHECK (index_id > 0),
    format_version INTEGER NOT NULL CHECK (format_version = 1),
    definition_digest BLOB NOT NULL CHECK (
        typeof(definition_digest) = 'blob' AND length(definition_digest) = 32
    ),
    summary_state INTEGER NOT NULL CHECK (summary_state IN (1, 2, 3)),
    bloom_bits BLOB NOT NULL CHECK (
        typeof(bloom_bits) = 'blob' AND length(bloom_bits) = 16384
    ),
    bloom_hashes INTEGER NOT NULL CHECK (bloom_hashes = 7),
    bloom_set_bits INTEGER NOT NULL CHECK (
        bloom_set_bits >= 0 AND bloom_set_bits <= 131072
    ),
    min_key BLOB,
    max_key BLOB,
    observed_rows INTEGER NOT NULL CHECK (observed_rows >= 0),
    additions INTEGER NOT NULL CHECK (additions >= 0),
    saturated INTEGER NOT NULL CHECK (saturated IN (0, 1)),
    CHECK (min_key IS NULL OR (typeof(min_key) = 'blob' AND length(min_key) > 0)),
    CHECK (max_key IS NULL OR (typeof(max_key) = 'blob' AND length(max_key) > 0)),
    CHECK ((min_key IS NULL) = (max_key IS NULL))
) STRICT, WITHOUT ROWID;";

#[derive(Debug, Clone)]
struct Summary {
    state: i64,
    definition_digest: Vec<u8>,
    bloom: Bloom,
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,
    observed_rows: u64,
    additions: u64,
    saturated: bool,
}

#[derive(Debug, Clone)]
struct Bloom {
    bits: Vec<u8>,
    set_bits: u32,
}

impl Bloom {
    fn empty() -> Self {
        Self {
            bits: vec![0; GLOBAL_INDEX_SHARD_SUMMARY_BLOOM_BYTES],
            set_bits: 0,
        }
    }

    fn from_parts(bits: Vec<u8>, set_bits: u32) -> EngineResult<Self> {
        if bits.len() != GLOBAL_INDEX_SHARD_SUMMARY_BLOOM_BYTES
            || usize::try_from(set_bits).ok().is_none_or(|count| {
                count > GLOBAL_INDEX_SHARD_SUMMARY_BLOOM_BYTES.saturating_mul(8)
            })
        {
            return Err(corrupt("shard summary has invalid Bloom metadata"));
        }
        let observed = bits.iter().map(|byte| byte.count_ones()).sum::<u32>();
        if observed != set_bits {
            return Err(corrupt("shard summary Bloom bit count is inconsistent"));
        }
        Ok(Self { bits, set_bits })
    }

    fn insert(&mut self, key: &[u8]) {
        for position in bloom_positions(key) {
            let byte = position / 8;
            let mask = 1_u8 << (position % 8);
            if self.bits[byte] & mask == 0 {
                self.bits[byte] |= mask;
                self.set_bits += 1;
            }
        }
    }

    fn may_contain(&self, key: &[u8]) -> bool {
        bloom_positions(key).into_iter().all(|position| {
            let byte = position / 8;
            self.bits[byte] & (1_u8 << (position % 8)) != 0
        })
    }

    fn merge(&mut self, other: &Self) {
        for (target, source) in self.bits.iter_mut().zip(&other.bits) {
            *target |= source;
        }
        self.set_bits = self.bits.iter().map(|byte| byte.count_ones()).sum();
    }

    fn saturated(&self) -> bool {
        u64::from(self.set_bits) * 100
            >= (GLOBAL_INDEX_SHARD_SUMMARY_BLOOM_BYTES as u64) * 8 * SATURATION_PERCENT
    }

    fn false_positive_rate_ppm(&self) -> u32 {
        if self.saturated() {
            return 1_000_000;
        }
        let occupied =
            f64::from(self.set_bits) / ((GLOBAL_INDEX_SHARD_SUMMARY_BLOOM_BYTES as f64) * 8.0);
        (occupied.powi(BLOOM_HASHES as i32) * 1_000_000.0)
            .round()
            .clamp(0.0, 1_000_000.0) as u32
    }
}

#[derive(Debug)]
struct SummaryAccumulator {
    bloom: Bloom,
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,
}

impl SummaryAccumulator {
    fn new() -> Self {
        Self {
            bloom: Bloom::empty(),
            min_key: None,
            max_key: None,
        }
    }

    fn insert(&mut self, key: &CanonicalIndexKey) {
        let bytes = key.as_bytes();
        self.bloom.insert(bytes);
        if self
            .min_key
            .as_ref()
            .is_none_or(|current| bytes < current.as_slice())
        {
            self.min_key = Some(bytes.to_vec());
        }
        if self
            .max_key
            .as_ref()
            .is_none_or(|current| bytes > current.as_slice())
        {
            self.max_key = Some(bytes.to_vec());
        }
    }

    fn merge_summary(&mut self, summary: &Summary) {
        self.bloom.merge(&summary.bloom);
        merge_min(&mut self.min_key, summary.min_key.as_deref());
        merge_max(&mut self.max_key, summary.max_key.as_deref());
    }
}

pub(super) fn rebuild(
    storage: &Storage,
    index: &GlobalIndexMetadata,
    cancellation: &CancellationToken,
) -> EngineResult<GlobalIndexShardSummaryRebuildReport> {
    let mut rebuilt_shards = 0_u16;
    let mut observed_rows = 0_u64;
    for shard in 0..storage.shard_count() {
        ensure_not_cancelled(cancellation, "before rebuilding a shard summary")?;
        begin_rebuild(storage, index, shard)?;
        let mut summary = SummaryAccumulator::new();
        let rows = global_index::scan_source_keys(storage, index, shard, cancellation, |key| {
            summary.insert(key);
            Ok(())
        })?;
        ensure_not_cancelled(cancellation, "before publishing a shard summary")?;
        finish_rebuild(storage, index, shard, summary, rows)?;
        rebuilt_shards += 1;
        observed_rows = observed_rows.checked_add(rows).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                "shard-summary observed row count overflowed",
            )
        })?;
    }
    Ok(GlobalIndexShardSummaryRebuildReport::new(
        index.id(),
        rebuilt_shards,
        observed_rows,
    ))
}

fn begin_rebuild(storage: &Storage, index: &GlobalIndexMetadata, shard: u16) -> EngineResult<()> {
    let mut connection = storage.open_shard(shard)?;
    with_internal_authorizer_mut(&mut connection, |connection| {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error::storage)?;
        ensure_schema(&transaction)?;
        transaction
            .execute(
                "INSERT INTO briskdb_global_index_shard_summaries (
                     index_id, format_version, definition_digest, summary_state,
                     bloom_bits, bloom_hashes, bloom_set_bits, min_key, max_key,
                     observed_rows, additions, saturated
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, NULL, NULL, 0, 0, 0)
                 ON CONFLICT (index_id) DO UPDATE SET
                     format_version = excluded.format_version,
                     definition_digest = excluded.definition_digest,
                     summary_state = excluded.summary_state,
                     bloom_bits = excluded.bloom_bits,
                     bloom_hashes = excluded.bloom_hashes,
                     bloom_set_bits = 0,
                     min_key = NULL,
                     max_key = NULL,
                     observed_rows = 0,
                     additions = 0,
                     saturated = 0",
                params![
                    to_sqlite_id(index.id())?,
                    i64::from(GLOBAL_INDEX_SHARD_SUMMARY_FORMAT_VERSION),
                    global_index::definition_digest(index).as_slice(),
                    STATE_BUILDING,
                    Bloom::empty().bits,
                    i64::from(BLOOM_HASHES),
                ],
            )
            .map_err(sqlite_error::storage)?;
        transaction.commit().map_err(sqlite_error::storage)
    })
}

fn finish_rebuild(
    storage: &Storage,
    index: &GlobalIndexMetadata,
    shard: u16,
    mut scanned: SummaryAccumulator,
    observed_rows: u64,
) -> EngineResult<()> {
    let mut connection = storage.open_shard(shard)?;
    with_internal_authorizer_mut(&mut connection, |connection| {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error::storage)?;
        validate_optional_schema(&transaction)?
            .then_some(())
            .ok_or_else(|| corrupt("shard summary disappeared during rebuild"))?;
        let current = read_summary(&transaction, index.id())?
            .ok_or_else(|| corrupt("shard summary disappeared during rebuild"))?;
        if current.state != STATE_BUILDING
            || current.definition_digest.as_slice() != global_index::definition_digest(index)
        {
            return Err(EngineError::new(
                EngineErrorKind::Busy,
                format!(
                    "global-index shard summary {} changed during rebuild",
                    index.id()
                ),
            ));
        }
        scanned.merge_summary(&current);
        let saturated = scanned.bloom.saturated();
        transaction
            .execute(
                "UPDATE briskdb_global_index_shard_summaries SET
                     summary_state = ?1, bloom_bits = ?2, bloom_set_bits = ?3,
                     min_key = ?4, max_key = ?5, observed_rows = ?6,
                     saturated = ?7
                 WHERE index_id = ?8 AND summary_state = ?9",
                params![
                    STATE_READY,
                    scanned.bloom.bits,
                    i64::from(scanned.bloom.set_bits),
                    scanned.min_key,
                    scanned.max_key,
                    to_sqlite_u64(observed_rows, "summary observed rows")?,
                    i64::from(saturated),
                    to_sqlite_id(index.id())?,
                    STATE_BUILDING,
                ],
            )
            .map_err(sqlite_error::storage)?;
        transaction.commit().map_err(sqlite_error::storage)
    })
}

/// Add only new keys. Deletes deliberately leave bits and extrema behind, so
/// they can increase false positives but can never create false negatives.
pub(super) fn record_additions(
    connection: &Connection,
    additions: &[(GlobalIndexId, CanonicalIndexKey)],
) -> EngineResult<()> {
    if additions.is_empty() {
        return Ok(());
    }
    if connection.is_autocommit() {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "shard-summary maintenance requires the physical row transaction",
        ));
    }
    with_internal_authorizer(connection, || {
        if !validate_optional_schema(connection)? {
            return Ok(());
        }
        for (index_id, key) in additions {
            let summary = match read_summary(connection, *index_id) {
                Ok(summary) => summary,
                Err(error) if error.kind() == EngineErrorKind::DataCorruption => {
                    mark_stale_on_connection(connection, *index_id)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let Some(mut summary) = summary else {
                continue;
            };
            if summary.state == STATE_STALE {
                continue;
            }
            if !matches!(summary.state, STATE_BUILDING | STATE_READY) {
                mark_stale_on_connection(connection, *index_id)?;
                continue;
            }
            let mut accumulator = SummaryAccumulator {
                bloom: summary.bloom.clone(),
                min_key: summary.min_key.take(),
                max_key: summary.max_key.take(),
            };
            accumulator.insert(key);
            let additions = summary.additions.checked_add(1).ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::NumericOutOfRange,
                    "shard-summary addition count overflowed",
                )
            })?;
            connection
                .execute(
                    "UPDATE briskdb_global_index_shard_summaries SET
                         bloom_bits = ?1, bloom_set_bits = ?2,
                         min_key = ?3, max_key = ?4, additions = ?5,
                         saturated = ?6
                     WHERE index_id = ?7",
                    params![
                        accumulator.bloom.bits,
                        i64::from(accumulator.bloom.set_bits),
                        accumulator.min_key,
                        accumulator.max_key,
                        to_sqlite_u64(additions, "summary additions")?,
                        i64::from(accumulator.bloom.saturated()),
                        to_sqlite_id(*index_id)?,
                    ],
                )
                .map_err(sqlite_error::storage)?;
        }
        Ok(())
    })
}

pub(super) fn mark_stale(
    storage: &Storage,
    shard: u16,
    index_ids: &[GlobalIndexId],
) -> EngineResult<()> {
    if index_ids.is_empty() {
        return Ok(());
    }
    let mut connection = storage.open_shard(shard)?;
    with_internal_authorizer_mut(&mut connection, |connection| {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error::storage)?;
        if validate_optional_schema(&transaction)? {
            for index_id in index_ids {
                mark_stale_on_connection(&transaction, *index_id)?;
            }
        }
        transaction.commit().map_err(sqlite_error::storage)
    })
}

fn mark_stale_on_connection(connection: &Connection, index_id: GlobalIndexId) -> EngineResult<()> {
    connection
        .execute(
            "UPDATE briskdb_global_index_shard_summaries
             SET summary_state = ?1 WHERE index_id = ?2",
            params![STATE_STALE, to_sqlite_id(index_id)?],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

pub(super) fn remove_index(storage: &Storage, index_id: GlobalIndexId) -> EngineResult<()> {
    for shard in 0..storage.shard_count() {
        let mut connection = storage.open_shard(shard)?;
        with_internal_authorizer_mut(&mut connection, |connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error::storage)?;
            if validate_optional_schema(&transaction)? {
                transaction
                    .execute(
                        "DELETE FROM briskdb_global_index_shard_summaries WHERE index_id = ?1",
                        [to_sqlite_id(index_id)?],
                    )
                    .map_err(sqlite_error::storage)?;
            }
            transaction.commit().map_err(sqlite_error::storage)
        })?;
    }
    Ok(())
}

pub(super) fn status(
    storage: &Storage,
    index: &GlobalIndexMetadata,
) -> EngineResult<GlobalIndexShardSummaryStatus> {
    let mut shards = Vec::with_capacity(usize::from(storage.shard_count()));
    for shard in 0..storage.shard_count() {
        let connection = storage.open_shard(shard)?;
        let inspected = with_internal_authorizer(&connection, || {
            if !validate_optional_schema(&connection)? {
                return Ok(None);
            }
            read_summary(&connection, index.id())
        });
        let status = match inspected {
            Ok(None) => GlobalIndexShardSummaryShardStatus::new(
                shard,
                GlobalIndexShardSummaryState::Missing,
                0,
                0,
                0,
                0,
                false,
                None,
            ),
            Err(error) if error.kind() == EngineErrorKind::DataCorruption => {
                GlobalIndexShardSummaryShardStatus::new(
                    shard,
                    GlobalIndexShardSummaryState::Incompatible,
                    0,
                    0,
                    0,
                    0,
                    false,
                    None,
                )
            }
            Err(error) => return Err(error),
            Ok(Some(summary)) => {
                let compatible =
                    summary.definition_digest.as_slice() == global_index::definition_digest(index);
                let state = if !compatible {
                    GlobalIndexShardSummaryState::Incompatible
                } else {
                    match summary.state {
                        STATE_BUILDING => GlobalIndexShardSummaryState::Building,
                        STATE_READY => GlobalIndexShardSummaryState::Ready,
                        STATE_STALE => GlobalIndexShardSummaryState::Stale,
                        _ => GlobalIndexShardSummaryState::Incompatible,
                    }
                };
                GlobalIndexShardSummaryShardStatus::new(
                    shard,
                    state,
                    GLOBAL_INDEX_SHARD_SUMMARY_BLOOM_BYTES as u32,
                    summary.bloom.set_bits,
                    summary.observed_rows,
                    summary.additions,
                    summary.saturated,
                    (state == GlobalIndexShardSummaryState::Ready)
                        .then(|| summary.bloom.false_positive_rate_ppm()),
                )
            }
        };
        shards.push(status);
    }
    Ok(GlobalIndexShardSummaryStatus::new(index.id(), shards))
}

pub(super) fn resolve(
    storage: &Storage,
    index: &GlobalIndexMetadata,
    predicate: &GlobalIndexShardSummaryPredicate,
    target_shards: &[u16],
) -> EngineResult<GlobalIndexShardSummaryReadResolution> {
    let mut retained = Vec::with_capacity(target_shards.len());
    let mut pruned = Vec::new();
    let mut examined = 0_usize;
    let mut fpr_total = 0_u64;
    let mut fpr_count = 0_u64;
    for &shard in target_shards {
        if shard >= storage.shard_count() {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "shard-summary routing received an out-of-range shard",
            ));
        }
        let connection = storage.open_shard(shard)?;
        let inspected = with_internal_authorizer(&connection, || {
            if !validate_optional_schema(&connection)? {
                return Ok(None);
            }
            read_summary(&connection, index.id())
        });
        let summary = match inspected {
            Ok(Some(summary))
                if summary.state == STATE_READY
                    && summary.definition_digest.as_slice()
                        == global_index::definition_digest(index) =>
            {
                summary
            }
            Ok(_) | Err(_) => {
                retained.push(shard);
                continue;
            }
        };
        examined += 1;
        if matches!(predicate, GlobalIndexShardSummaryPredicate::Equality(_)) {
            fpr_total += u64::from(summary.bloom.false_positive_rate_ppm());
            fpr_count += 1;
        }
        if let Some(reason) = exclusion_reason(&summary, predicate) {
            pruned.push(GlobalIndexShardSummaryPrunedShard::new(shard, reason));
        } else {
            retained.push(shard);
        }
    }
    let estimated_false_positive_rate_ppm =
        (fpr_count != 0).then(|| u32::try_from(fpr_total / fpr_count).unwrap_or(1_000_000));
    Ok(GlobalIndexShardSummaryReadResolution::new(
        retained,
        pruned,
        examined,
        estimated_false_positive_rate_ppm,
    ))
}

fn exclusion_reason(
    summary: &Summary,
    predicate: &GlobalIndexShardSummaryPredicate,
) -> Option<ShardSummaryPruningReason> {
    if summary.observed_rows == 0 && summary.additions == 0 {
        return Some(ShardSummaryPruningReason::EmptySummary);
    }
    match predicate {
        GlobalIndexShardSummaryPredicate::Equality(keys) => {
            if summary.saturated {
                return None;
            }
            (!keys
                .iter()
                .any(|key| summary.bloom.may_contain(key.as_bytes())))
            .then_some(ShardSummaryPruningReason::BloomMiss)
        }
        GlobalIndexShardSummaryPredicate::Range { lower, upper } => {
            let (Some(min_key), Some(max_key)) =
                (summary.min_key.as_deref(), summary.max_key.as_deref())
            else {
                return None;
            };
            if lower.as_ref().is_some_and(|bound| {
                matches!(max_key.cmp(bound.key().as_bytes()), Ordering::Less)
                    || (!bound.inclusive() && max_key == bound.key().as_bytes())
            }) {
                return Some(ShardSummaryPruningReason::MaximumBelowLowerBound);
            }
            if upper.as_ref().is_some_and(|bound| {
                matches!(min_key.cmp(bound.key().as_bytes()), Ordering::Greater)
                    || (!bound.inclusive() && min_key == bound.key().as_bytes())
            }) {
                return Some(ShardSummaryPruningReason::MinimumAboveUpperBound);
            }
            None
        }
    }
}

fn read_summary(connection: &Connection, index_id: GlobalIndexId) -> EngineResult<Option<Summary>> {
    connection
        .query_row(
            "SELECT format_version, definition_digest, summary_state,
                    bloom_bits, bloom_hashes, bloom_set_bits, min_key, max_key,
                    observed_rows, additions, saturated
             FROM briskdb_global_index_shard_summaries WHERE index_id = ?1",
            [to_sqlite_id(index_id)?],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, Option<Vec<u8>>>(6)?,
                    row.get::<_, Option<Vec<u8>>>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, i64>(10)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error::storage)?
        .map(
            |(
                version,
                definition_digest,
                state,
                bits,
                hashes,
                set_bits,
                min_key,
                max_key,
                observed_rows,
                additions,
                saturated,
            )| {
                if version != i64::from(GLOBAL_INDEX_SHARD_SUMMARY_FORMAT_VERSION)
                    || definition_digest.len() != 32
                    || !matches!(state, STATE_BUILDING | STATE_READY | STATE_STALE)
                    || hashes != i64::from(BLOOM_HASHES)
                    || !(0..=i64::try_from(
                        GLOBAL_INDEX_SHARD_SUMMARY_BLOOM_BYTES.saturating_mul(8),
                    )
                    .expect("Bloom size fits SQLite's signed integer range"))
                        .contains(&set_bits)
                    || (min_key.is_some() != max_key.is_some())
                    || min_key.as_ref().is_some_and(Vec::is_empty)
                    || max_key.as_ref().is_some_and(Vec::is_empty)
                    || min_key
                        .as_ref()
                        .zip(max_key.as_ref())
                        .is_some_and(|(min, max)| min > max)
                    || observed_rows < 0
                    || additions < 0
                    || !matches!(saturated, 0 | 1)
                {
                    return Err(corrupt("shard summary row is invalid or incompatible"));
                }
                Ok(Summary {
                    state,
                    definition_digest,
                    bloom: Bloom::from_parts(
                        bits,
                        u32::try_from(set_bits).map_err(|_| {
                            corrupt("shard summary Bloom bit count is out of range")
                        })?,
                    )?,
                    min_key,
                    max_key,
                    observed_rows: observed_rows as u64,
                    additions: additions as u64,
                    saturated: saturated == 1,
                })
            },
        )
        .transpose()
}

fn ensure_schema(connection: &Connection) -> EngineResult<()> {
    if !validate_optional_schema(connection)? {
        connection
            .execute_batch(SCHEMA_SQL)
            .map_err(sqlite_error::storage)?;
    }
    if !validate_optional_schema(connection)? {
        return Err(corrupt("shard-summary schema was not installed"));
    }
    Ok(())
}

pub(super) fn validate_optional_schema(connection: &Connection) -> EngineResult<bool> {
    let object = connection
        .query_row(
            "SELECT type, tbl_name, sql FROM sqlite_schema WHERE name = ?1",
            [TABLE],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error::storage)?;
    let Some((object_type, table_name, sql)) = object else {
        return Ok(false);
    };
    if object_type != "table"
        || table_name != TABLE
        || sql
            .as_deref()
            .is_none_or(|sql| normalize_schema_sql(sql) != normalize_schema_sql(TABLE_SCHEMA_SQL))
    {
        return Err(corrupt(
            "shard-summary schema is incomplete or incompatible",
        ));
    }
    Ok(true)
}

pub(super) fn is_exact_schema_object(
    object_type: &str,
    name: &str,
    table_name: &str,
    sql: Option<&str>,
) -> bool {
    object_type == "table"
        && name == TABLE
        && table_name == TABLE
        && sql
            .is_some_and(|sql| normalize_schema_sql(sql) == normalize_schema_sql(TABLE_SCHEMA_SQL))
}

fn bloom_positions(key: &[u8]) -> [usize; BLOOM_HASHES as usize] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(BLOOM_DOMAIN);
    hasher.update(key);
    let digest = hasher.finalize();
    let bytes = digest.as_bytes();
    let first = u64::from_le_bytes(bytes[..8].try_into().expect("digest contains two words"));
    let second =
        u64::from_le_bytes(bytes[8..16].try_into().expect("digest contains two words")) | 1;
    let bit_count = (GLOBAL_INDEX_SHARD_SUMMARY_BLOOM_BYTES as u64) * 8;
    std::array::from_fn(|ordinal| {
        first
            .wrapping_add((ordinal as u64).wrapping_mul(second))
            .wrapping_rem(bit_count) as usize
    })
}

fn merge_min(target: &mut Option<Vec<u8>>, candidate: Option<&[u8]>) {
    if let Some(candidate) = candidate {
        if target
            .as_ref()
            .is_none_or(|current| candidate < current.as_slice())
        {
            *target = Some(candidate.to_vec());
        }
    }
}

fn merge_max(target: &mut Option<Vec<u8>>, candidate: Option<&[u8]>) {
    if let Some(candidate) = candidate {
        if target
            .as_ref()
            .is_none_or(|current| candidate > current.as_slice())
        {
            *target = Some(candidate.to_vec());
        }
    }
}

fn with_internal_authorizer<T>(
    connection: &Connection,
    action: impl FnOnce() -> EngineResult<T>,
) -> EngineResult<T> {
    connection
        .authorizer(None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>)
        .map_err(sqlite_error::storage)?;
    let result = action();
    attach_storage_authorizer(connection)?;
    result
}

fn with_internal_authorizer_mut<T>(
    connection: &mut Connection,
    action: impl FnOnce(&mut Connection) -> EngineResult<T>,
) -> EngineResult<T> {
    connection
        .authorizer(None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>)
        .map_err(sqlite_error::storage)?;
    let result = action(connection);
    attach_storage_authorizer(connection)?;
    result
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_end_matches(';')
        .to_owned()
}

fn to_sqlite_id(index_id: GlobalIndexId) -> EngineResult<i64> {
    i64::try_from(index_id.get()).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::NumericOutOfRange,
            "global-index ID exceeds SQLite's signed integer range",
            error,
        )
    })
}

fn to_sqlite_u64(value: u64, label: &str) -> EngineResult<i64> {
    i64::try_from(value).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::NumericOutOfRange,
            format!("{label} exceeds SQLite's signed integer range"),
            error,
        )
    })
}

fn ensure_not_cancelled(cancellation: &CancellationToken, context: &str) -> EngineResult<()> {
    if cancellation.is_cancelled() {
        Err(EngineError::new(
            EngineErrorKind::Cancelled,
            format!("global-index shard-summary rebuild was cancelled {context}"),
        ))
    } else {
        Ok(())
    }
}

fn corrupt(message: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::DataCorruption, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Value;

    #[test]
    fn bloom_has_no_false_negatives_before_and_after_saturation() {
        let mut bloom = Bloom::empty();
        let mut keys = Vec::new();
        for value in 0_i64..100_000 {
            let key = CanonicalIndexKey::encode_values(&[Value::Int64(value)]).unwrap();
            bloom.insert(key.as_bytes());
            keys.push(key);
            if bloom.saturated() {
                break;
            }
        }
        assert!(bloom.saturated());
        assert!(keys.iter().all(|key| bloom.may_contain(key.as_bytes())));
        assert_eq!(bloom.false_positive_rate_ppm(), 1_000_000);
    }

    #[test]
    fn exact_schema_and_bit_count_are_compatibility_fenced() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(SCHEMA_SQL).unwrap();
        assert!(validate_optional_schema(&connection).unwrap());
        connection
            .execute_batch(
                "DROP TABLE briskdb_global_index_shard_summaries;
                 CREATE TABLE briskdb_global_index_shard_summaries (index_id INTEGER PRIMARY KEY)",
            )
            .unwrap();
        assert_eq!(
            validate_optional_schema(&connection).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );

        assert_eq!(
            Bloom::from_parts(vec![0; GLOBAL_INDEX_SHARD_SUMMARY_BLOOM_BYTES], 1,)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn persisted_bit_count_cannot_wrap_during_validation() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(SCHEMA_SQL).unwrap();
        connection
            .execute_batch(
                "PRAGMA ignore_check_constraints = ON;
                 INSERT INTO briskdb_global_index_shard_summaries (
                     index_id, format_version, definition_digest, summary_state,
                     bloom_bits, bloom_hashes, bloom_set_bits, min_key, max_key,
                     observed_rows, additions, saturated
                 ) VALUES (
                     1, 1, zeroblob(32), 2, zeroblob(16384), 7,
                     4294967296, NULL, NULL, 1, 0, 0
                 )",
            )
            .unwrap();

        assert_eq!(
            read_summary(&connection, GlobalIndexId::new(1).unwrap())
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }
}
