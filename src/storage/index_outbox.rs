//! Transactional shard-local non-unique global-index outbox.

use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use crate::core::{
    CancellationToken, CanonicalIndexKey, EngineError, EngineErrorKind, EngineResult,
    GlobalIndexId, GlobalIndexOutboxBatch, GlobalIndexOutboxEvent, GlobalIndexOutboxEventKind,
    GlobalIndexOutboxPruneReport, GlobalIndexOutboxShardStatus, GlobalIndexOwner,
    GlobalOperationId, MAX_GLOBAL_INDEX_OUTBOX_BATCH_EVENTS,
    MAX_GLOBAL_INDEX_OUTBOX_BYTES_PER_SHARD, MAX_GLOBAL_INDEX_OUTBOX_EVENTS_PER_SHARD,
};
use crate::sqlite_error;

use super::{Storage, attach_storage_authorizer};

pub(super) const OUTBOX_FORMAT_VERSION: u32 = 1;
const STATE_TABLE: &str = "briskdb_global_index_outbox_state";
const EVENTS_TABLE: &str = "briskdb_global_index_outbox_events";
const CONSUMERS_TABLE: &str = "briskdb_global_index_outbox_consumers";
const INDEX_CURSOR_INDEX: &str = "briskdb_global_index_outbox_events_index_cursor";

const STATE_SCHEMA_SQL: &str = "CREATE TABLE briskdb_global_index_outbox_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    format_version INTEGER NOT NULL CHECK (format_version = 1),
    last_cursor INTEGER NOT NULL CHECK (last_cursor >= 0),
    pruned_through INTEGER NOT NULL CHECK (
        pruned_through >= 0 AND pruned_through <= last_cursor
    ),
    retained_events INTEGER NOT NULL CHECK (retained_events >= 0),
    retained_bytes INTEGER NOT NULL CHECK (retained_bytes >= 0)
) STRICT";

const EVENTS_SCHEMA_SQL: &str = "CREATE TABLE briskdb_global_index_outbox_events (
    cursor INTEGER PRIMARY KEY CHECK (cursor > 0),
    format_version INTEGER NOT NULL CHECK (format_version = 1),
    index_id INTEGER NOT NULL CHECK (index_id > 0),
    source_shard INTEGER NOT NULL CHECK (source_shard BETWEEN 0 AND 63),
    operation_id BLOB NOT NULL CHECK (
        typeof(operation_id) = 'blob' AND length(operation_id) = 16
    ),
    event_kind INTEGER NOT NULL CHECK (event_kind IN (1, 2, 3, 4)),
    old_key BLOB,
    new_key BLOB,
    old_locator BLOB,
    new_locator BLOB,
    payload_bytes INTEGER NOT NULL CHECK (payload_bytes > 0),
    CHECK (old_key IS NULL OR (typeof(old_key) = 'blob' AND length(old_key) > 0)),
    CHECK (new_key IS NULL OR (typeof(new_key) = 'blob' AND length(new_key) > 0)),
    CHECK (
        old_locator IS NULL OR
        (typeof(old_locator) = 'blob' AND length(old_locator) > 0)
    ),
    CHECK (
        new_locator IS NULL OR
        (typeof(new_locator) = 'blob' AND length(new_locator) > 0)
    ),
    CHECK (
        (event_kind = 1 AND old_key IS NULL AND old_locator IS NULL
            AND new_key IS NOT NULL AND new_locator IS NOT NULL) OR
        (event_kind = 2 AND old_key IS NOT NULL AND old_locator IS NOT NULL
            AND new_key IS NOT NULL AND new_locator IS NOT NULL) OR
        (event_kind IN (3, 4) AND old_key IS NOT NULL AND old_locator IS NOT NULL
            AND new_key IS NULL AND new_locator IS NULL)
    )
) STRICT, WITHOUT ROWID";

const CONSUMERS_SCHEMA_SQL: &str = "CREATE TABLE briskdb_global_index_outbox_consumers (
    index_id INTEGER PRIMARY KEY CHECK (index_id > 0),
    durable_cursor INTEGER NOT NULL CHECK (durable_cursor >= 0),
    active INTEGER NOT NULL CHECK (active IN (0, 1))
) STRICT, WITHOUT ROWID";

const INDEX_CURSOR_SCHEMA_SQL: &str = "CREATE INDEX briskdb_global_index_outbox_events_index_cursor
    ON briskdb_global_index_outbox_events (index_id, cursor)";

pub(super) const SCHEMA_SQL: &str = "
CREATE TABLE briskdb_global_index_outbox_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    format_version INTEGER NOT NULL CHECK (format_version = 1),
    last_cursor INTEGER NOT NULL CHECK (last_cursor >= 0),
    pruned_through INTEGER NOT NULL CHECK (
        pruned_through >= 0 AND pruned_through <= last_cursor
    ),
    retained_events INTEGER NOT NULL CHECK (retained_events >= 0),
    retained_bytes INTEGER NOT NULL CHECK (retained_bytes >= 0)
) STRICT;
CREATE TABLE briskdb_global_index_outbox_events (
    cursor INTEGER PRIMARY KEY CHECK (cursor > 0),
    format_version INTEGER NOT NULL CHECK (format_version = 1),
    index_id INTEGER NOT NULL CHECK (index_id > 0),
    source_shard INTEGER NOT NULL CHECK (source_shard BETWEEN 0 AND 63),
    operation_id BLOB NOT NULL CHECK (
        typeof(operation_id) = 'blob' AND length(operation_id) = 16
    ),
    event_kind INTEGER NOT NULL CHECK (event_kind IN (1, 2, 3, 4)),
    old_key BLOB,
    new_key BLOB,
    old_locator BLOB,
    new_locator BLOB,
    payload_bytes INTEGER NOT NULL CHECK (payload_bytes > 0),
    CHECK (old_key IS NULL OR (typeof(old_key) = 'blob' AND length(old_key) > 0)),
    CHECK (new_key IS NULL OR (typeof(new_key) = 'blob' AND length(new_key) > 0)),
    CHECK (
        old_locator IS NULL OR
        (typeof(old_locator) = 'blob' AND length(old_locator) > 0)
    ),
    CHECK (
        new_locator IS NULL OR
        (typeof(new_locator) = 'blob' AND length(new_locator) > 0)
    ),
    CHECK (
        (event_kind = 1 AND old_key IS NULL AND old_locator IS NULL
            AND new_key IS NOT NULL AND new_locator IS NOT NULL) OR
        (event_kind = 2 AND old_key IS NOT NULL AND old_locator IS NOT NULL
            AND new_key IS NOT NULL AND new_locator IS NOT NULL) OR
        (event_kind IN (3, 4) AND old_key IS NOT NULL AND old_locator IS NOT NULL
            AND new_key IS NULL AND new_locator IS NULL)
    )
) STRICT, WITHOUT ROWID;
CREATE TABLE briskdb_global_index_outbox_consumers (
    index_id INTEGER PRIMARY KEY CHECK (index_id > 0),
    durable_cursor INTEGER NOT NULL CHECK (durable_cursor >= 0),
    active INTEGER NOT NULL CHECK (active IN (0, 1))
) STRICT, WITHOUT ROWID;
CREATE INDEX briskdb_global_index_outbox_events_index_cursor
    ON briskdb_global_index_outbox_events (index_id, cursor);
INSERT INTO briskdb_global_index_outbox_state (
    singleton, format_version, last_cursor, pruned_through,
    retained_events, retained_bytes
) VALUES (1, 1, 0, 0, 0, 0);
";

#[derive(Debug, Clone)]
pub(crate) struct PendingGlobalIndexOutboxEvent {
    pub(crate) index_id: GlobalIndexId,
    pub(crate) kind: GlobalIndexOutboxEventKind,
    pub(crate) old_key: Option<CanonicalIndexKey>,
    pub(crate) new_key: Option<CanonicalIndexKey>,
    pub(crate) old_owner: Option<GlobalIndexOwner>,
    pub(crate) new_owner: Option<GlobalIndexOwner>,
}

impl PendingGlobalIndexOutboxEvent {
    fn payload_bytes(&self) -> EngineResult<u64> {
        let bytes = 80_usize
            .checked_add(self.old_key.as_ref().map_or(0, |key| key.as_bytes().len()))
            .and_then(|bytes| {
                bytes.checked_add(self.new_key.as_ref().map_or(0, |key| key.as_bytes().len()))
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    self.old_owner
                        .as_ref()
                        .map_or(0, |owner| owner.locator().len()),
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    self.new_owner
                        .as_ref()
                        .map_or(0, |owner| owner.locator().len()),
                )
            })
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::LimitExceeded,
                    "global-index outbox event size overflowed",
                )
            })?;
        u64::try_from(bytes).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::LimitExceeded,
                "global-index outbox event is too large",
                error,
            )
        })
    }
}

pub(super) fn is_exact_schema_object(
    object_type: &str,
    name: &str,
    table_name: &str,
    sql: Option<&str>,
) -> bool {
    let expected = match name {
        STATE_TABLE if object_type == "table" && table_name == STATE_TABLE => STATE_SCHEMA_SQL,
        EVENTS_TABLE if object_type == "table" && table_name == EVENTS_TABLE => EVENTS_SCHEMA_SQL,
        CONSUMERS_TABLE if object_type == "table" && table_name == CONSUMERS_TABLE => {
            CONSUMERS_SCHEMA_SQL
        }
        INDEX_CURSOR_INDEX if object_type == "index" && table_name == EVENTS_TABLE => {
            INDEX_CURSOR_SCHEMA_SQL
        }
        _ => return false,
    };
    sql.is_some_and(|sql| normalize_schema_sql(sql) == normalize_schema_sql(expected))
}

pub(super) fn validate_optional_schema(connection: &Connection) -> EngineResult<bool> {
    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, sql FROM sqlite_schema
             WHERE name IN (?1, ?2, ?3, ?4)
             ORDER BY name COLLATE BINARY",
        )
        .map_err(sqlite_error::storage)?;
    let objects = statement
        .query_map(
            [
                STATE_TABLE,
                EVENTS_TABLE,
                CONSUMERS_TABLE,
                INDEX_CURSOR_INDEX,
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)?;
    if objects.is_empty() {
        return Ok(false);
    }
    if objects.len() != 4
        || objects.iter().any(|(kind, name, table, sql)| {
            !is_exact_schema_object(kind, name, table, sql.as_deref())
        })
    {
        return Err(corrupt(
            "shard global-index outbox schema is incomplete or incompatible",
        ));
    }
    let state = connection
        .query_row(
            "SELECT format_version, last_cursor, pruned_through,
                    retained_events, retained_bytes
             FROM briskdb_global_index_outbox_state WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error::storage)?;
    let Some((version, last, pruned, events, bytes)) = state else {
        return Err(corrupt("shard global-index outbox state row is missing"));
    };
    if version != i64::from(OUTBOX_FORMAT_VERSION)
        || last < 0
        || pruned < 0
        || pruned > last
        || events < 0
        || bytes < 0
    {
        return Err(corrupt("shard global-index outbox state is invalid"));
    }
    Ok(true)
}

pub(crate) fn append_events(
    connection: &Connection,
    shard: u16,
    operation_id: GlobalOperationId,
    events: &[PendingGlobalIndexOutboxEvent],
) -> EngineResult<usize> {
    if events.is_empty() {
        return Ok(0);
    }
    if events.len() > MAX_GLOBAL_INDEX_OUTBOX_BATCH_EVENTS {
        return Err(EngineError::new(
            EngineErrorKind::LimitExceeded,
            format!(
                "one write produced more than {MAX_GLOBAL_INDEX_OUTBOX_BATCH_EVENTS} global-index outbox events"
            ),
        ));
    }
    if connection.is_autocommit() {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "global-index outbox append requires the physical row transaction",
        ));
    }
    for event in events {
        for owner in event.old_owner.iter().chain(event.new_owner.iter()) {
            if owner.source_shard() != shard {
                return Err(corrupt("global-index outbox event targets the wrong shard"));
            }
        }
    }
    with_internal_authorizer(connection, || {
        if !validate_optional_schema(connection)? {
            connection
                .execute_batch(SCHEMA_SQL)
                .map_err(sqlite_error::storage)?;
            if !validate_optional_schema(connection)? {
                return Err(corrupt("global-index outbox schema was not installed"));
            }
        }
        let (last_cursor, retained_events, retained_bytes) = connection
            .query_row(
                "SELECT last_cursor, retained_events, retained_bytes
                 FROM briskdb_global_index_outbox_state WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .map_err(sqlite_error::storage)?;
        let last_cursor =
            u64::try_from(last_cursor).map_err(|_| corrupt("invalid outbox cursor"))?;
        let retained_events =
            u64::try_from(retained_events).map_err(|_| corrupt("invalid outbox event count"))?;
        let retained_bytes =
            u64::try_from(retained_bytes).map_err(|_| corrupt("invalid outbox byte count"))?;
        let added_events = u64::try_from(events.len()).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::LimitExceeded,
                "global-index outbox batch is too large",
                error,
            )
        })?;
        let payloads = events
            .iter()
            .map(PendingGlobalIndexOutboxEvent::payload_bytes)
            .collect::<EngineResult<Vec<_>>>()?;
        let added_bytes = payloads.iter().try_fold(0_u64, |total, bytes| {
            total.checked_add(*bytes).ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::LimitExceeded,
                    "global-index outbox batch byte size overflowed",
                )
            })
        })?;
        if retained_events
            .checked_add(added_events)
            .is_none_or(|count| count > MAX_GLOBAL_INDEX_OUTBOX_EVENTS_PER_SHARD)
            || retained_bytes
                .checked_add(added_bytes)
                .is_none_or(|bytes| bytes > MAX_GLOBAL_INDEX_OUTBOX_BYTES_PER_SHARD)
        {
            return Err(EngineError::new(
                EngineErrorKind::Busy,
                "global-index outbox is full; advance consumers and prune before retrying the write",
            ));
        }
        let new_last = last_cursor.checked_add(added_events).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                "global-index outbox cursor space is exhausted",
            )
        })?;

        let mut first_seen = BTreeMap::new();
        for event in events {
            first_seen.entry(event.index_id).or_insert(last_cursor);
        }
        for (index_id, initial_cursor) in first_seen {
            connection
                .execute(
                    "INSERT OR IGNORE INTO briskdb_global_index_outbox_consumers (
                         index_id, durable_cursor, active
                     ) VALUES (?1, ?2, 1)",
                    params![
                        to_sqlite_id(index_id)?,
                        to_sqlite_u64(initial_cursor, "cursor")?
                    ],
                )
                .map_err(sqlite_error::storage)?;
        }

        connection
            .execute(
                "UPDATE briskdb_global_index_outbox_state SET
                     last_cursor = ?1,
                     retained_events = retained_events + ?2,
                     retained_bytes = retained_bytes + ?3
                 WHERE singleton = 1",
                params![
                    to_sqlite_u64(new_last, "cursor")?,
                    to_sqlite_u64(added_events, "event count")?,
                    to_sqlite_u64(added_bytes, "byte count")?,
                ],
            )
            .map_err(sqlite_error::storage)?;
        let mut insert = connection
            .prepare_cached(
                "INSERT INTO briskdb_global_index_outbox_events (
                     cursor, format_version, index_id, source_shard, operation_id,
                     event_kind, old_key, new_key, old_locator, new_locator, payload_bytes
                 ) VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            )
            .map_err(sqlite_error::storage)?;
        let operation_bytes = operation_id.as_bytes();
        for (offset, (event, payload_bytes)) in events.iter().zip(payloads).enumerate() {
            let offset = u64::try_from(offset).map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::LimitExceeded,
                    "global-index outbox event offset is too large",
                    error,
                )
            })?;
            let cursor = last_cursor
                .checked_add(offset)
                .and_then(|cursor| cursor.checked_add(1))
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::LimitExceeded,
                        "global-index outbox cursor space is exhausted",
                    )
                })?;
            insert
                .execute(params![
                    to_sqlite_u64(cursor, "cursor")?,
                    to_sqlite_id(event.index_id)?,
                    i64::from(shard),
                    operation_bytes.as_slice(),
                    event.kind.code(),
                    event.old_key.as_ref().map(CanonicalIndexKey::as_bytes),
                    event.new_key.as_ref().map(CanonicalIndexKey::as_bytes),
                    event.old_owner.as_ref().map(GlobalIndexOwner::locator),
                    event.new_owner.as_ref().map(GlobalIndexOwner::locator),
                    to_sqlite_u64(payload_bytes, "event byte count")?,
                ])
                .map_err(sqlite_error::storage)?;
        }
        Ok(events.len())
    })
}

pub(super) fn inspect(storage: &Storage) -> EngineResult<Vec<GlobalIndexOutboxShardStatus>> {
    (0..storage.shard_count())
        .map(|shard| inspect_shard(storage, shard))
        .collect()
}

fn inspect_shard(storage: &Storage, shard: u16) -> EngineResult<GlobalIndexOutboxShardStatus> {
    let connection = storage.open_shard(shard)?;
    with_internal_authorizer(&connection, || {
        if !validate_optional_schema(&connection)? {
            return Ok(GlobalIndexOutboxShardStatus::new(shard, 0, 0, 0, 0, 0, 0));
        }
        let (last, pruned, events, bytes) = connection
            .query_row(
                "SELECT last_cursor, pruned_through, retained_events, retained_bytes
                 FROM briskdb_global_index_outbox_state WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .map_err(sqlite_error::storage)?;
        let (consumers, minimum) = connection
            .query_row(
                "SELECT COUNT(*), MIN(durable_cursor)
                 FROM briskdb_global_index_outbox_consumers WHERE active = 1",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
            )
            .map_err(sqlite_error::storage)?;
        let last = checked_u64(last, "outbox high-water cursor")?;
        Ok(GlobalIndexOutboxShardStatus::new(
            shard,
            last,
            checked_u64(pruned, "outbox pruned cursor")?,
            checked_u64(events, "outbox retained event count")?,
            checked_u64(bytes, "outbox retained byte count")?,
            checked_u64(consumers, "outbox consumer count")?,
            minimum.map_or(Ok(last), |value| checked_u64(value, "consumer cursor"))?,
        ))
    })
}

pub(super) fn read_batch(
    storage: &Storage,
    index_id: GlobalIndexId,
    shard: u16,
    after: u64,
    limit: usize,
    cancellation: &CancellationToken,
) -> EngineResult<GlobalIndexOutboxBatch> {
    validate_batch_limit(limit)?;
    ensure_not_cancelled(cancellation, "before reading the global-index outbox")?;
    let connection = storage.open_shard(shard)?;
    with_internal_authorizer(&connection, || {
        if !validate_optional_schema(&connection)? {
            if after != 0 {
                return Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    "global-index outbox cursor is beyond the empty shard high-water mark",
                ));
            }
            return Ok(GlobalIndexOutboxBatch::new(
                shard,
                index_id,
                after,
                0,
                Vec::new(),
            ));
        }
        let (high_water, pruned_through) = connection
            .query_row(
                "SELECT last_cursor, pruned_through
                 FROM briskdb_global_index_outbox_state WHERE singleton = 1",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(sqlite_error::storage)?;
        let high_water = checked_u64(high_water, "outbox high-water cursor")?;
        let pruned_through = checked_u64(pruned_through, "outbox pruned cursor")?;
        if after < pruned_through {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "global-index outbox cursor {after} was pruned through {pruned_through}; rebuild the consumer"
                ),
            ));
        }
        if after > high_water {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "global-index outbox cursor is beyond the shard high-water mark",
            ));
        }
        let mut statement = connection
            .prepare(
                "SELECT cursor, format_version, source_shard, operation_id, event_kind,
                        old_key, new_key, old_locator, new_locator
                 FROM briskdb_global_index_outbox_events
                 WHERE index_id = ?1 AND cursor > ?2 AND cursor <= ?3
                 ORDER BY cursor LIMIT ?4",
            )
            .map_err(sqlite_error::storage)?;
        let mut rows = statement
            .query(params![
                to_sqlite_id(index_id)?,
                to_sqlite_u64(after, "cursor")?,
                to_sqlite_u64(high_water, "cursor")?,
                i64::try_from(limit).map_err(|_| invalid_batch_limit())?,
            ])
            .map_err(sqlite_error::storage)?;
        let mut events = Vec::new();
        while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
            ensure_not_cancelled(cancellation, "while reading the global-index outbox")?;
            let cursor = checked_u64(
                row.get::<_, i64>(0).map_err(sqlite_error::storage)?,
                "outbox event cursor",
            )?;
            let version = row.get::<_, i64>(1).map_err(sqlite_error::storage)?;
            if version != i64::from(OUTBOX_FORMAT_VERSION) {
                return Err(corrupt(
                    "global-index outbox event has an unsupported format",
                ));
            }
            let source_shard = row.get::<_, i64>(2).map_err(sqlite_error::storage)?;
            if source_shard != i64::from(shard) {
                return Err(corrupt(
                    "global-index outbox event records the wrong source shard",
                ));
            }
            let operation = row
                .get::<_, Vec<u8>>(3)
                .map_err(sqlite_error::storage)?
                .try_into()
                .map_err(|_| corrupt("global-index outbox operation ID has the wrong length"))?;
            let operation_id = GlobalOperationId::new(operation)
                .map_err(|_| corrupt("global-index outbox operation ID is invalid"))?;
            let kind = GlobalIndexOutboxEventKind::from_code(
                row.get::<_, i64>(4).map_err(sqlite_error::storage)?,
            )?;
            let old_key = read_key(
                row.get::<_, Option<Vec<u8>>>(5)
                    .map_err(sqlite_error::storage)?,
            )?;
            let new_key = read_key(
                row.get::<_, Option<Vec<u8>>>(6)
                    .map_err(sqlite_error::storage)?,
            )?;
            let old_owner = read_owner(
                shard,
                row.get::<_, Option<Vec<u8>>>(7)
                    .map_err(sqlite_error::storage)?,
            )?;
            let new_owner = read_owner(
                shard,
                row.get::<_, Option<Vec<u8>>>(8)
                    .map_err(sqlite_error::storage)?,
            )?;
            validate_event_shape(kind, &old_key, &new_key, &old_owner, &new_owner)?;
            events.push(GlobalIndexOutboxEvent::from_validated_parts(
                OUTBOX_FORMAT_VERSION,
                cursor,
                index_id,
                operation_id,
                kind,
                old_key,
                new_key,
                old_owner,
                new_owner,
            ));
        }
        Ok(GlobalIndexOutboxBatch::new(
            shard, index_id, after, high_water, events,
        ))
    })
}

pub(super) fn advance_consumer(
    storage: &Storage,
    index_id: GlobalIndexId,
    shard: u16,
    cursor: u64,
    cancellation: &CancellationToken,
) -> EngineResult<GlobalIndexOutboxShardStatus> {
    ensure_not_cancelled(
        cancellation,
        "before advancing a global-index outbox consumer",
    )?;
    let mut connection = storage.open_shard(shard)?;
    with_internal_authorizer_mut(&mut connection, |connection| {
        if !validate_optional_schema(connection)? {
            return if cursor == 0 {
                Ok(GlobalIndexOutboxShardStatus::new(shard, 0, 0, 0, 0, 0, 0))
            } else {
                Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    "global-index outbox cursor is beyond the empty shard high-water mark",
                ))
            };
        }
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error::storage)?;
        let high_water = transaction
            .query_row(
                "SELECT last_cursor FROM briskdb_global_index_outbox_state WHERE singleton = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sqlite_error::storage)?;
        let high_water = checked_u64(high_water, "outbox high-water cursor")?;
        if cursor > high_water {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "global-index outbox cursor is beyond the shard high-water mark",
            ));
        }
        ensure_not_cancelled(
            cancellation,
            "while advancing a global-index outbox consumer",
        )?;
        let changed = transaction
            .execute(
                "UPDATE briskdb_global_index_outbox_consumers
                 SET durable_cursor = ?1
                 WHERE index_id = ?2 AND active = 1 AND durable_cursor <= ?1",
                params![to_sqlite_u64(cursor, "cursor")?, to_sqlite_id(index_id)?],
            )
            .map_err(sqlite_error::storage)?;
        if changed == 0 {
            let existing = transaction
                .query_row(
                    "SELECT durable_cursor FROM briskdb_global_index_outbox_consumers
                     WHERE index_id = ?1 AND active = 1",
                    [to_sqlite_id(index_id)?],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .map_err(sqlite_error::storage)?;
            if existing.is_none() {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "global index has no active shard-local outbox consumer",
                ));
            }
        }
        abort_at_test_boundary("outbox-cursor-before-commit");
        transaction.commit().map_err(sqlite_error::storage)?;
        abort_at_test_boundary("outbox-cursor-after-commit");
        inspect_shard(storage, shard)
    })
}

pub(super) fn prune(
    storage: &Storage,
    shard: u16,
    limit: usize,
    cancellation: &CancellationToken,
) -> EngineResult<GlobalIndexOutboxPruneReport> {
    validate_batch_limit(limit)?;
    ensure_not_cancelled(cancellation, "before pruning the global-index outbox")?;
    let mut connection = storage.open_shard(shard)?;
    with_internal_authorizer_mut(&mut connection, |connection| {
        if !validate_optional_schema(connection)? {
            return Ok(GlobalIndexOutboxPruneReport::new(shard, 0, 0, 0));
        }
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error::storage)?;
        let (last, current_pruned) = transaction
            .query_row(
                "SELECT last_cursor, pruned_through
                 FROM briskdb_global_index_outbox_state WHERE singleton = 1",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(sqlite_error::storage)?;
        let threshold = transaction
            .query_row(
                "SELECT MIN(durable_cursor)
                 FROM briskdb_global_index_outbox_consumers WHERE active = 1",
                [],
                |row| row.get::<_, Option<i64>>(0),
            )
            .map_err(sqlite_error::storage)?
            .unwrap_or(last);
        let mut statement = transaction
            .prepare(
                "SELECT cursor, payload_bytes
                 FROM briskdb_global_index_outbox_events
                 WHERE cursor <= ?1 ORDER BY cursor LIMIT ?2",
            )
            .map_err(sqlite_error::storage)?;
        let selected = statement
            .query_map(
                params![
                    threshold,
                    i64::try_from(limit).map_err(|_| invalid_batch_limit())?
                ],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(sqlite_error::storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_error::storage)?;
        drop(statement);
        if selected.is_empty() {
            return Ok(GlobalIndexOutboxPruneReport::new(
                shard,
                0,
                0,
                checked_u64(current_pruned, "outbox pruned cursor")?,
            ));
        }
        ensure_not_cancelled(cancellation, "while pruning the global-index outbox")?;
        let through = selected.last().expect("nonempty selection").0;
        let deleted_bytes = selected.iter().try_fold(0_u64, |total, (_, bytes)| {
            total
                .checked_add(checked_u64(*bytes, "outbox event byte count")?)
                .ok_or_else(|| corrupt("global-index outbox byte count overflowed"))
        })?;
        let deleted_events = u64::try_from(selected.len()).map_err(|_| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                "global-index outbox prune count is too large",
            )
        })?;
        let changed = transaction
            .execute(
                "DELETE FROM briskdb_global_index_outbox_events WHERE cursor <= ?1",
                [through],
            )
            .map_err(sqlite_error::storage)?;
        if changed != selected.len() {
            return Err(corrupt(
                "global-index outbox prune changed an unexpected row count",
            ));
        }
        transaction
            .execute(
                "UPDATE briskdb_global_index_outbox_state SET
                     pruned_through = ?1,
                     retained_events = retained_events - ?2,
                     retained_bytes = retained_bytes - ?3
                 WHERE singleton = 1",
                params![
                    through,
                    to_sqlite_u64(deleted_events, "event count")?,
                    to_sqlite_u64(deleted_bytes, "byte count")?,
                ],
            )
            .map_err(sqlite_error::storage)?;
        abort_at_test_boundary("outbox-prune-before-commit");
        transaction.commit().map_err(sqlite_error::storage)?;
        abort_at_test_boundary("outbox-prune-after-commit");
        Ok(GlobalIndexOutboxPruneReport::new(
            shard,
            deleted_events,
            deleted_bytes,
            checked_u64(through, "outbox pruned cursor")?,
        ))
    })
}

pub(super) fn deactivate_index(storage: &Storage, index_id: GlobalIndexId) -> EngineResult<()> {
    for shard in 0..storage.shard_count() {
        let mut connection = storage.open_shard(shard)?;
        with_internal_authorizer_mut(&mut connection, |connection| {
            if !validate_optional_schema(connection)? {
                return Ok(());
            }
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error::storage)?;
            transaction
                .execute(
                    "UPDATE briskdb_global_index_outbox_consumers SET active = 0
                     WHERE index_id = ?1",
                    [to_sqlite_id(index_id)?],
                )
                .map_err(sqlite_error::storage)?;
            transaction.commit().map_err(sqlite_error::storage)
        })?;
    }
    Ok(())
}

fn with_internal_authorizer<T>(
    connection: &Connection,
    action: impl FnOnce() -> EngineResult<T>,
) -> EngineResult<T> {
    connection
        .authorizer(None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>)
        .map_err(sqlite_error::storage)?;
    let result = action();
    let restored = attach_storage_authorizer(connection);
    restored?;
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
    let restored = attach_storage_authorizer(connection);
    restored?;
    result
}

fn read_key(bytes: Option<Vec<u8>>) -> EngineResult<Option<CanonicalIndexKey>> {
    bytes
        .map(|bytes| CanonicalIndexKey::from_bytes(&bytes))
        .transpose()
        .map_err(|_| corrupt("global-index outbox contains an invalid canonical key"))
}

fn read_owner(shard: u16, locator: Option<Vec<u8>>) -> EngineResult<Option<GlobalIndexOwner>> {
    locator
        .map(|locator| GlobalIndexOwner::new(shard, locator))
        .transpose()
        .map_err(|_| corrupt("global-index outbox contains an invalid row locator"))
}

fn validate_event_shape(
    kind: GlobalIndexOutboxEventKind,
    old_key: &Option<CanonicalIndexKey>,
    new_key: &Option<CanonicalIndexKey>,
    old_owner: &Option<GlobalIndexOwner>,
    new_owner: &Option<GlobalIndexOwner>,
) -> EngineResult<()> {
    let valid = match kind {
        GlobalIndexOutboxEventKind::Insert => {
            old_key.is_none() && old_owner.is_none() && new_key.is_some() && new_owner.is_some()
        }
        GlobalIndexOutboxEventKind::Update => {
            old_key.is_some() && old_owner.is_some() && new_key.is_some() && new_owner.is_some()
        }
        GlobalIndexOutboxEventKind::Delete | GlobalIndexOutboxEventKind::Tombstone => {
            old_key.is_some() && old_owner.is_some() && new_key.is_none() && new_owner.is_none()
        }
    };
    if valid {
        Ok(())
    } else {
        Err(corrupt(
            "global-index outbox event has an invalid old/new shape",
        ))
    }
}

fn validate_batch_limit(limit: usize) -> EngineResult<()> {
    if (1..=MAX_GLOBAL_INDEX_OUTBOX_BATCH_EVENTS).contains(&limit) {
        Ok(())
    } else {
        Err(invalid_batch_limit())
    }
}

fn invalid_batch_limit() -> EngineError {
    EngineError::new(
        EngineErrorKind::InvalidArgument,
        format!(
            "global-index outbox batch limit must be in 1..={MAX_GLOBAL_INDEX_OUTBOX_BATCH_EVENTS}"
        ),
    )
}

fn ensure_not_cancelled(cancellation: &CancellationToken, context: &str) -> EngineResult<()> {
    if cancellation.is_cancelled() {
        Err(EngineError::new(
            EngineErrorKind::Cancelled,
            format!("global-index outbox operation was cancelled {context}"),
        ))
    } else {
        Ok(())
    }
}

fn checked_u64(value: i64, field: &str) -> EngineResult<u64> {
    u64::try_from(value).map_err(|_| corrupt(format!("{field} is invalid")))
}

fn to_sqlite_id(id: GlobalIndexId) -> EngineResult<i64> {
    i64::try_from(id.get()).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::NumericOutOfRange,
            "global-index ID does not fit shard-local outbox storage",
            error,
        )
    })
}

fn to_sqlite_u64(value: u64, field: &str) -> EngineResult<i64> {
    i64::try_from(value).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::LimitExceeded,
            format!("global-index outbox {field} does not fit SQLite"),
            error,
        )
    })
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

fn corrupt(diagnostic: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::DataCorruption, diagnostic)
}

#[cfg(test)]
pub(super) fn abort_at_test_boundary(boundary: &str) {
    if std::env::var("BRISKDB_GLOBAL_INDEX_OUTBOX_ABORT_POINT").as_deref() == Ok(boundary) {
        std::process::abort();
    }
}

#[cfg(not(test))]
pub(super) fn abort_at_test_boundary(_boundary: &str) {}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    #[cfg(feature = "embedded")]
    use std::{
        env,
        path::Path,
        process::{Command, Stdio},
    };

    use super::*;
    use crate::core::Value;
    #[cfg(feature = "embedded")]
    use crate::{
        Statement,
        core::{
            Database, GlobalIndexDeclaration, GlobalIndexKeyPart, GlobalIndexKeySource,
            GlobalIndexKeyType, GlobalIndexOutboxCursor, GlobalIndexStorageTopology,
            ShardKeyMetadata, ShardKeyType, TableDeclaration,
        },
        embedded::BriskDb,
    };

    fn insert_event() -> PendingGlobalIndexOutboxEvent {
        PendingGlobalIndexOutboxEvent {
            index_id: GlobalIndexId::new(1).unwrap(),
            kind: GlobalIndexOutboxEventKind::Insert,
            old_key: None,
            new_key: Some(CanonicalIndexKey::encode_values(&[Value::from("key")]).unwrap()),
            old_owner: None,
            new_owner: Some(GlobalIndexOwner::new(2, vec![1, 2, 3]).unwrap()),
        }
    }

    fn operation_id() -> GlobalOperationId {
        GlobalOperationId::new([7; 16]).unwrap()
    }

    #[test]
    fn append_is_atomic_with_its_callers_transaction_and_cursor_is_monotonic() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert_eq!(
            append_events(&connection, 2, operation_id(), &[insert_event()]).unwrap(),
            1
        );
        connection.execute_batch("ROLLBACK").unwrap();
        with_internal_authorizer(&connection, || {
            assert!(!validate_optional_schema(&connection)?);
            Ok(())
        })
        .unwrap();

        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        append_events(
            &connection,
            2,
            operation_id(),
            &[insert_event(), insert_event()],
        )
        .unwrap();
        connection.execute_batch("COMMIT").unwrap();
        with_internal_authorizer(&connection, || {
            assert!(validate_optional_schema(&connection)?);
            let cursors = connection
                .prepare("SELECT cursor FROM briskdb_global_index_outbox_events ORDER BY cursor")
                .map_err(sqlite_error::storage)?
                .query_map([], |row| row.get::<_, i64>(0))
                .map_err(sqlite_error::storage)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sqlite_error::storage)?;
            assert_eq!(cursors, [1, 2]);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn capacity_is_explicit_backpressure_and_does_not_append() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        append_events(&connection, 2, operation_id(), &[insert_event()]).unwrap();
        connection.execute_batch("COMMIT").unwrap();
        with_internal_authorizer(&connection, || {
            connection
                .execute(
                    "UPDATE briskdb_global_index_outbox_state
                     SET retained_events = ?1 WHERE singleton = 1",
                    [to_sqlite_u64(
                        MAX_GLOBAL_INDEX_OUTBOX_EVENTS_PER_SHARD,
                        "event count",
                    )?],
                )
                .map_err(sqlite_error::storage)?;
            Ok(())
        })
        .unwrap();

        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        let error = append_events(&connection, 2, operation_id(), &[insert_event()]).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Busy);
        connection.execute_batch("ROLLBACK").unwrap();
        with_internal_authorizer(&connection, || {
            let events = connection
                .query_row(
                    "SELECT COUNT(*) FROM briskdb_global_index_outbox_events",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(sqlite_error::storage)?;
            assert_eq!(events, 1);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn optional_schema_rejects_partial_or_modified_storage_objects() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(STATE_SCHEMA_SQL).unwrap();
        let error = validate_optional_schema(&connection).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
    }

    #[cfg(feature = "embedded")]
    fn setup_fault_root(root: &Path) -> (GlobalIndexId, String, u16) {
        let mut database = Database::open(root, 2).unwrap();
        database
            .broadcast(
                "CREATE TABLE outbox_fault (
                     tenant_id TEXT PRIMARY KEY NOT NULL,
                     indexed_value TEXT NOT NULL
                 ) STRICT",
            )
            .unwrap();
        let logical = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical,
                    "outbox_fault",
                    ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
        let table = database
            .catalog()
            .table("default", "outbox_fault")
            .unwrap()
            .unwrap()
            .id();
        let index = database
            .create_global_index(
                GlobalIndexDeclaration::new(
                    table,
                    "outbox_fault_value",
                    vec![GlobalIndexKeyPart::new(
                        GlobalIndexKeySource::column("indexed_value").unwrap(),
                        GlobalIndexKeyType::Text,
                    )],
                )
                .unwrap()
                .with_topology(GlobalIndexStorageTopology::selected_v1()),
            )
            .unwrap();
        database.build_global_index(index).unwrap();
        let route = "outbox-fault-route".to_owned();
        let shard = database.shard_for_key(route.as_bytes());
        (index, route, shard)
    }

    #[cfg(feature = "embedded")]
    fn write_fault_row(root: &Path, route: &str) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let db = BriskDb::open(root).await.unwrap();
            let session = db.session();
            session.set_routing_key(route).await.unwrap();
            db.execute_write(
                &session,
                Statement::new(
                    "INSERT INTO outbox_fault (tenant_id, indexed_value) VALUES (?1, 'value')",
                    vec![route.into()],
                ),
            )
            .await
            .unwrap();
            session.close().await.unwrap();
            db.close().await.unwrap();
        });
    }

    #[cfg(feature = "embedded")]
    fn abort_fault_child(
        root: &Path,
        mode: &str,
        boundary: &str,
        index: GlobalIndexId,
        route: &str,
        shard: u16,
    ) {
        let status = Command::new(env::current_exe().unwrap())
            .arg("--exact")
            .arg("storage::index_outbox::tests::outbox_fault_child")
            .arg("--nocapture")
            .env("BRISKDB_OUTBOX_FAULT_ROOT", root)
            .env("BRISKDB_OUTBOX_FAULT_MODE", mode)
            .env("BRISKDB_OUTBOX_FAULT_INDEX", index.get().to_string())
            .env("BRISKDB_OUTBOX_FAULT_ROUTE", route)
            .env("BRISKDB_OUTBOX_FAULT_SHARD", shard.to_string())
            .env("BRISKDB_GLOBAL_INDEX_OUTBOX_ABORT_POINT", boundary)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(!status.success(), "child did not abort at {boundary}");
    }

    #[cfg(feature = "embedded")]
    #[test]
    fn outbox_fault_child() {
        let Ok(root) = env::var("BRISKDB_OUTBOX_FAULT_ROOT") else {
            return;
        };
        let root = Path::new(&root);
        let mode = env::var("BRISKDB_OUTBOX_FAULT_MODE").unwrap();
        let index = GlobalIndexId::new(
            env::var("BRISKDB_OUTBOX_FAULT_INDEX")
                .unwrap()
                .parse()
                .unwrap(),
        )
        .unwrap();
        let route = env::var("BRISKDB_OUTBOX_FAULT_ROUTE").unwrap();
        let shard = env::var("BRISKDB_OUTBOX_FAULT_SHARD")
            .unwrap()
            .parse::<u16>()
            .unwrap();
        match mode.as_str() {
            "write" => write_fault_row(root, &route),
            "cursor" => {
                Database::open(root, 2)
                    .unwrap()
                    .advance_global_index_outbox(index, shard, GlobalIndexOutboxCursor::new(1))
                    .unwrap();
            }
            "prune" => {
                Database::open(root, 2)
                    .unwrap()
                    .prune_global_index_outbox(shard, 16)
                    .unwrap();
            }
            _ => panic!("unknown outbox fault mode"),
        }
    }

    #[cfg(feature = "embedded")]
    #[test]
    fn process_abort_preserves_row_event_cursor_and_prune_atomicity() {
        for (boundary, committed) in [
            ("outbox-physical-before-commit", false),
            ("outbox-physical-after-commit", true),
        ] {
            let root = tempfile::tempdir().unwrap();
            let (index, route, shard) = setup_fault_root(root.path());
            abort_fault_child(root.path(), "write", boundary, index, &route, shard);
            let database = Database::open(root.path(), 2).unwrap();
            let rows = database
                .query(
                    &route,
                    "SELECT tenant_id FROM outbox_fault WHERE tenant_id = ?1",
                    &[route.clone().into()],
                )
                .unwrap();
            assert_eq!(rows.rows().len(), usize::from(committed));
            let events = database
                .read_global_index_outbox(index, shard, GlobalIndexOutboxCursor::new(0), 16)
                .unwrap();
            assert_eq!(events.events().len(), usize::from(committed));
        }

        for (mode, boundaries) in [
            (
                "cursor",
                ["outbox-cursor-before-commit", "outbox-cursor-after-commit"],
            ),
            (
                "prune",
                ["outbox-prune-before-commit", "outbox-prune-after-commit"],
            ),
        ] {
            for (boundary, committed) in boundaries.into_iter().zip([false, true]) {
                let root = tempfile::tempdir().unwrap();
                let (index, route, shard) = setup_fault_root(root.path());
                write_fault_row(root.path(), &route);
                if mode == "prune" {
                    Database::open(root.path(), 2)
                        .unwrap()
                        .advance_global_index_outbox(index, shard, GlobalIndexOutboxCursor::new(1))
                        .unwrap();
                }
                abort_fault_child(root.path(), mode, boundary, index, &route, shard);
                let database = Database::open(root.path(), 2).unwrap();
                let status = database.global_index_outbox_status().unwrap();
                let shard_status = &status[usize::from(shard)];
                if mode == "cursor" {
                    assert_eq!(
                        shard_status.minimum_durable_cursor().get(),
                        u64::from(committed)
                    );
                    assert_eq!(shard_status.retained_events(), 1);
                } else {
                    assert_eq!(shard_status.pruned_through().get(), u64::from(committed));
                    assert_eq!(shard_status.retained_events(), u64::from(!committed));
                }
            }
        }
    }
}
