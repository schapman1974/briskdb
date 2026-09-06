//! Exact shard-local receipts for durable idempotent writes.

use std::fmt;

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use crate::{
    core::{
        EngineError, EngineErrorKind, EngineResult,
        IDEMPOTENCY_RECEIPT_RETENTION_MS as IDEMPOTENCY_RETENTION_MS,
        MAX_IDEMPOTENCY_RECEIPTS_PER_SHARD, OperationControl, Value,
    },
    sqlite_error,
};

use super::pool;

pub(super) const RECEIPTS_TABLE: &str = "briskdb_idempotency_receipts_v1";
const EXPIRED_RECEIPT_CLEANUP_LIMIT: i64 = 64;
const MAX_CREATED_UNIX_MS: i64 = i64::MAX - IDEMPOTENCY_RETENTION_MS;

const RECEIPTS_SCHEMA_SQL: &str = "CREATE TABLE briskdb_idempotency_receipts_v1 (
    key_digest BLOB PRIMARY KEY NOT NULL CHECK (
        typeof(key_digest) = 'blob' AND length(key_digest) = 32
    ),
    request_digest BLOB NOT NULL CHECK (
        typeof(request_digest) = 'blob' AND length(request_digest) = 32
    ),
    target_shard INTEGER NOT NULL CHECK (target_shard BETWEEN 0 AND 63),
    rows_affected INTEGER NOT NULL CHECK (rows_affected >= 0),
    created_unix_ms INTEGER NOT NULL CHECK (
        created_unix_ms BETWEEN 0 AND 9223372036768375807
    ),
    expires_unix_ms INTEGER NOT NULL CHECK (
        expires_unix_ms = created_unix_ms + 86400000
    ),
    format_version INTEGER NOT NULL CHECK (format_version = 1)
) STRICT, WITHOUT ROWID";

/// One checksum-keyed result retained on its authoritative physical shard.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct IdempotencyReceipt {
    key_digest: [u8; 32],
    request_digest: [u8; 32],
    target_shard: u16,
    rows_affected: usize,
    created_unix_ms: i64,
    expires_unix_ms: i64,
}

impl fmt::Debug for IdempotencyReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IdempotencyReceipt")
            .field("key_digest", &"<redacted>")
            .field("request_digest", &"<redacted>")
            .field("target_shard", &self.target_shard)
            .field("rows_affected", &self.rows_affected)
            .field("created_unix_ms", &self.created_unix_ms)
            .field("expires_unix_ms", &self.expires_unix_ms)
            .finish()
    }
}

impl IdempotencyReceipt {
    pub(crate) const fn key_digest(&self) -> [u8; 32] {
        self.key_digest
    }

    pub(crate) const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    pub(crate) const fn target_shard(&self) -> u16 {
        self.target_shard
    }

    pub(crate) const fn rows_affected(&self) -> usize {
        self.rows_affected
    }
}

/// Receipt fields known before the application mutation is stepped.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct NewIdempotencyReceipt {
    key_digest: [u8; 32],
    request_digest: [u8; 32],
    target_shard: u16,
    created_unix_ms: i64,
    expires_unix_ms: i64,
}

impl fmt::Debug for NewIdempotencyReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NewIdempotencyReceipt")
            .field("key_digest", &"<redacted>")
            .field("request_digest", &"<redacted>")
            .field("target_shard", &self.target_shard)
            .field("created_unix_ms", &self.created_unix_ms)
            .field("expires_unix_ms", &self.expires_unix_ms)
            .finish()
    }
}

impl NewIdempotencyReceipt {
    pub(crate) fn new(
        key_digest: [u8; 32],
        request_digest: [u8; 32],
        target_shard: u16,
        created_unix_ms: i64,
    ) -> EngineResult<Self> {
        if target_shard > 63 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "idempotency receipt target shard must be between 0 and 63",
            ));
        }
        if !(0..=MAX_CREATED_UNIX_MS).contains(&created_unix_ms) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "idempotency receipt creation time is outside the supported Unix millisecond range",
            ));
        }
        let expires_unix_ms = created_unix_ms
            .checked_add(IDEMPOTENCY_RETENTION_MS)
            .expect("the validated receipt timestamp leaves room for retention");
        Ok(Self {
            key_digest,
            request_digest,
            target_shard,
            created_unix_ms,
            expires_unix_ms,
        })
    }
}

/// The result of the atomic mutation-and-receipt transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IdempotentExecuteOutcome {
    Executed(IdempotencyReceipt),
    Existing(IdempotencyReceipt),
}

pub(super) fn is_exact_schema_object(
    object_type: &str,
    name: &str,
    table_name: &str,
    sql: Option<&str>,
) -> bool {
    object_type == "table"
        && name == RECEIPTS_TABLE
        && table_name == RECEIPTS_TABLE
        && sql.is_some_and(|sql| {
            normalize_schema_sql(sql) == normalize_schema_sql(RECEIPTS_SCHEMA_SQL)
        })
}

pub(super) fn validate_optional_schema(connection: &Connection) -> EngineResult<bool> {
    let objects = connection
        .prepare(
            "SELECT type, name, tbl_name, sql FROM sqlite_schema
             WHERE name = ?1 COLLATE NOCASE
             ORDER BY type, name, tbl_name LIMIT 2",
        )
        .and_then(|mut statement| {
            statement
                .query_map([RECEIPTS_TABLE], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| shard_read_error(error, "failed to inspect idempotency receipt schema"))?;
    if objects.is_empty() {
        return Ok(false);
    }
    if objects.len() != 1
        || !is_exact_schema_object(
            &objects[0].0,
            &objects[0].1,
            &objects[0].2,
            objects[0].3.as_deref(),
        )
    {
        return Err(corrupt(
            "shard idempotency receipt table has an incompatible schema",
        ));
    }
    Ok(true)
}

/// Validate every persisted row while retaining a hard upper bound on startup
/// work. Honest writers keep the entire table at or below the active-receipt
/// capacity by removing at least one expired row before each replacement.
pub(super) fn validate_optional_state(
    connection: &Connection,
    physical_shard: u16,
) -> EngineResult<bool> {
    if !validate_optional_schema(connection)? {
        return Ok(false);
    }
    let mut statement = connection
        .prepare(
            "SELECT key_digest, request_digest, target_shard, rows_affected,
                    created_unix_ms, expires_unix_ms, format_version
             FROM briskdb_idempotency_receipts_v1
             ORDER BY key_digest
             LIMIT 4097",
        )
        .map_err(|error| shard_read_error(error, "failed to validate idempotency receipts"))?;
    let mut rows = statement
        .query([])
        .map_err(|error| shard_read_error(error, "failed to validate idempotency receipts"))?;
    let mut count = 0_usize;
    while let Some(row) = rows
        .next()
        .map_err(|error| shard_read_error(error, "failed to validate idempotency receipts"))?
    {
        count += 1;
        if count > MAX_IDEMPOTENCY_RECEIPTS_PER_SHARD {
            return Err(corrupt(
                "shard idempotency receipt count exceeds the storage format limit",
            ));
        }
        let receipt = parse_receipt((
            row.get::<_, Vec<u8>>(0).map_err(|error| {
                shard_read_error(error, "failed to validate idempotency receipts")
            })?,
            row.get::<_, Vec<u8>>(1).map_err(|error| {
                shard_read_error(error, "failed to validate idempotency receipts")
            })?,
            row.get::<_, i64>(2).map_err(|error| {
                shard_read_error(error, "failed to validate idempotency receipts")
            })?,
            row.get::<_, i64>(3).map_err(|error| {
                shard_read_error(error, "failed to validate idempotency receipts")
            })?,
            row.get::<_, i64>(4).map_err(|error| {
                shard_read_error(error, "failed to validate idempotency receipts")
            })?,
            row.get::<_, i64>(5).map_err(|error| {
                shard_read_error(error, "failed to validate idempotency receipts")
            })?,
            row.get::<_, i64>(6).map_err(|error| {
                shard_read_error(error, "failed to validate idempotency receipts")
            })?,
        ))?;
        if receipt.target_shard != physical_shard {
            return Err(corrupt(
                "shard idempotency receipt targets a different physical shard",
            ));
        }
    }
    Ok(true)
}

pub(super) fn find_receipt(
    connection: &Connection,
    key_digest: [u8; 32],
    now_unix_ms: i64,
    control: Option<&OperationControl>,
) -> EngineResult<Option<IdempotencyReceipt>> {
    validate_now(now_unix_ms)?;
    if !validate_optional_schema(connection)? {
        return Ok(None);
    }
    let receipt =
        pool::with_idempotency_storage_operation(|| load_receipt(connection, key_digest, None))?;
    let Some(receipt) = receipt else {
        return Ok(None);
    };
    if receipt.expires_unix_ms > now_unix_ms {
        return Ok(Some(receipt));
    }

    check_cancelled(control)?;
    let transaction = Transaction::new_unchecked(connection, TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let current = pool::with_idempotency_storage_operation(|| {
        // Re-read under the write lock before deleting so even a caller that
        // omitted the outer stripe cannot remove a freshly replaced receipt.
        match load_receipt(&transaction, key_digest, None)? {
            Some(receipt) if receipt.expires_unix_ms <= now_unix_ms => {
                delete_expired_key(&transaction, key_digest, now_unix_ms)?;
                Ok(None)
            }
            current => Ok(current),
        }
    })?;
    check_cancelled(control)?;
    transaction.commit().map_err(sqlite_error::storage)?;
    Ok(current)
}

pub(super) fn execute_with_receipt(
    connection: &Connection,
    physical_shard: u16,
    statement: &str,
    parameters: &[Value],
    receipt: NewIdempotencyReceipt,
    control: Option<&OperationControl>,
) -> EngineResult<IdempotentExecuteOutcome> {
    if receipt.target_shard != physical_shard {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "idempotency receipt target does not match the checked-out physical shard",
        ));
    }

    let transaction = Transaction::new_unchecked(connection, TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let preflight = pool::with_idempotency_storage_operation(|| {
        ensure_schema(&transaction)?;
        delete_expired_key(&transaction, receipt.key_digest, receipt.created_unix_ms)?;
        cleanup_expired(&transaction, receipt.created_unix_ms)?;
        if let Some(existing) = load_receipt(&transaction, receipt.key_digest, None)? {
            return Ok(Some(existing));
        }
        ensure_capacity(&transaction, physical_shard, receipt.created_unix_ms)?;
        Ok(None)
    })?;

    if let Some(existing) = preflight {
        check_cancelled(control)?;
        transaction.commit().map_err(sqlite_error::storage)?;
        return Ok(IdempotentExecuteOutcome::Existing(existing));
    }

    check_cancelled(control)?;
    // The storage-operation guard is deliberately absent while client SQL is
    // prepared and stepped, so it cannot use this helper to reach the reserved
    // receipt namespace.
    let rows_affected = crate::sql::execute(&transaction, statement, parameters)?;
    wait_after_application_dml_for_test(receipt.key_digest);
    check_cancelled(control)?;
    let rows_affected_sqlite = i64::try_from(rows_affected).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::LimitExceeded,
            "affected-row count exceeds the idempotency receipt format",
            error,
        )
    })?;
    let stored = IdempotencyReceipt {
        key_digest: receipt.key_digest,
        request_digest: receipt.request_digest,
        target_shard: receipt.target_shard,
        rows_affected,
        created_unix_ms: receipt.created_unix_ms,
        expires_unix_ms: receipt.expires_unix_ms,
    };
    pool::with_idempotency_storage_operation(|| {
        transaction
            .execute(
                "INSERT INTO briskdb_idempotency_receipts_v1 (
                    key_digest,
                    request_digest,
                    target_shard,
                    rows_affected,
                    created_unix_ms,
                    expires_unix_ms,
                    format_version
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)",
                params![
                    &stored.key_digest[..],
                    &stored.request_digest[..],
                    i64::from(stored.target_shard),
                    rows_affected_sqlite,
                    stored.created_unix_ms,
                    stored.expires_unix_ms,
                ],
            )
            .map_err(sqlite_error::storage)?;
        Ok(())
    })?;
    check_cancelled(control)?;
    transaction.commit().map_err(sqlite_error::storage)?;
    wait_after_commit_for_test(stored.key_digest);
    Ok(IdempotentExecuteOutcome::Executed(stored))
}

fn ensure_schema(connection: &Connection) -> EngineResult<()> {
    if !validate_optional_schema(connection)? {
        connection
            .execute_batch(RECEIPTS_SCHEMA_SQL)
            .map_err(sqlite_error::storage)?;
    }
    if validate_optional_schema(connection)? {
        Ok(())
    } else {
        Err(corrupt(
            "idempotency receipt table creation did not produce its exact schema",
        ))
    }
}

fn delete_expired_key(
    connection: &Connection,
    key_digest: [u8; 32],
    now_unix_ms: i64,
) -> EngineResult<()> {
    connection
        .execute(
            "DELETE FROM briskdb_idempotency_receipts_v1
             WHERE key_digest = ?1 AND expires_unix_ms <= ?2",
            params![&key_digest[..], now_unix_ms],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn cleanup_expired(connection: &Connection, now_unix_ms: i64) -> EngineResult<()> {
    connection
        .execute(
            "DELETE FROM briskdb_idempotency_receipts_v1
             WHERE key_digest IN (
                 SELECT key_digest FROM briskdb_idempotency_receipts_v1
                 WHERE expires_unix_ms <= ?1
                 ORDER BY expires_unix_ms, key_digest
                 LIMIT ?2
             )",
            params![now_unix_ms, EXPIRED_RECEIPT_CLEANUP_LIMIT],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn ensure_capacity(
    connection: &Connection,
    physical_shard: u16,
    now_unix_ms: i64,
) -> EngineResult<()> {
    let misplaced = connection
        .query_row(
            "SELECT EXISTS (
                 SELECT 1 FROM briskdb_idempotency_receipts_v1
                 WHERE target_shard <> ?1 LIMIT 1
             )",
            [i64::from(physical_shard)],
            |row| row.get::<_, bool>(0),
        )
        .map_err(sqlite_error::storage)?;
    if misplaced {
        return Err(corrupt(
            "shard idempotency receipt targets a different physical shard",
        ));
    }
    let count = connection
        .query_row(
            "SELECT count(*) FROM briskdb_idempotency_receipts_v1
             WHERE target_shard = ?1 AND expires_unix_ms > ?2",
            params![i64::from(physical_shard), now_unix_ms],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sqlite_error::storage)?;
    let count = usize::try_from(count)
        .map_err(|_| corrupt("shard idempotency receipt count is invalid"))?;
    if count > MAX_IDEMPOTENCY_RECEIPTS_PER_SHARD {
        return Err(corrupt(
            "shard idempotency receipt count exceeds the storage format limit",
        ));
    }
    if count == MAX_IDEMPOTENCY_RECEIPTS_PER_SHARD {
        return Err(EngineError::new(
            EngineErrorKind::LimitExceeded,
            "shard idempotency receipt capacity is exhausted",
        ));
    }
    Ok(())
}

fn load_receipt(
    connection: &Connection,
    key_digest: [u8; 32],
    active_after_unix_ms: Option<i64>,
) -> EngineResult<Option<IdempotencyReceipt>> {
    let mut statement = connection
        .prepare(
            "SELECT key_digest, request_digest, target_shard, rows_affected,
                    created_unix_ms, expires_unix_ms, format_version
             FROM briskdb_idempotency_receipts_v1
             WHERE key_digest = ?1
               AND (?2 IS NULL OR expires_unix_ms > ?2)",
        )
        .map_err(sqlite_error::storage)?;
    let stored = statement
        .query_row(params![&key_digest[..], active_after_unix_ms], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })
        .optional()
        .map_err(|error| shard_read_error(error, "failed to read an idempotency receipt"))?;
    stored.map(parse_receipt).transpose()
}

fn parse_receipt(
    (
        key_digest,
        request_digest,
        target_shard,
        rows_affected,
        created_unix_ms,
        expires_unix_ms,
        format_version,
    ): (Vec<u8>, Vec<u8>, i64, i64, i64, i64, i64),
) -> EngineResult<IdempotencyReceipt> {
    let key_digest = key_digest
        .try_into()
        .map_err(|_| corrupt("idempotency receipt has an invalid key digest"))?;
    let request_digest = request_digest
        .try_into()
        .map_err(|_| corrupt("idempotency receipt has an invalid request digest"))?;
    let target_shard = u16::try_from(target_shard)
        .ok()
        .filter(|target| *target <= 63)
        .ok_or_else(|| corrupt("idempotency receipt has an invalid target shard"))?;
    let rows_affected = usize::try_from(rows_affected)
        .map_err(|_| corrupt("idempotency receipt has an invalid affected-row count"))?;
    let expected_expiration = created_unix_ms
        .checked_add(IDEMPOTENCY_RETENTION_MS)
        .filter(|_| (0..=MAX_CREATED_UNIX_MS).contains(&created_unix_ms));
    if expected_expiration != Some(expires_unix_ms) {
        return Err(corrupt(
            "idempotency receipt has an invalid retention window",
        ));
    }
    if format_version != 1 {
        return Err(corrupt(
            "idempotency receipt has an unsupported format version",
        ));
    }
    Ok(IdempotencyReceipt {
        key_digest,
        request_digest,
        target_shard,
        rows_affected,
        created_unix_ms,
        expires_unix_ms,
    })
}

fn validate_now(now_unix_ms: i64) -> EngineResult<()> {
    if now_unix_ms < 0 {
        Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "idempotency receipt lookup time must be a nonnegative Unix millisecond timestamp",
        ))
    } else {
        Ok(())
    }
}

fn check_cancelled(control: Option<&OperationControl>) -> EngineResult<()> {
    match control.and_then(OperationControl::reason) {
        Some(reason) => Err(reason.error()),
        None => Ok(()),
    }
}

#[cfg(test)]
struct PostApplicationDmlHook {
    key_digest: [u8; 32],
    started: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
static POST_APPLICATION_DML_HOOK: std::sync::OnceLock<
    std::sync::Mutex<Option<PostApplicationDmlHook>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
static POST_APPLICATION_DML_TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
static POST_COMMIT_HOOK: std::sync::OnceLock<std::sync::Mutex<Option<PostApplicationDmlHook>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
fn block_after_next_application_dml(
    key_digest: [u8; 32],
    started: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
) {
    let mut hook = POST_APPLICATION_DML_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(
        hook.replace(PostApplicationDmlHook {
            key_digest,
            started,
            release,
        })
        .is_none(),
        "only one post-application-DML hook may be installed"
    );
}

#[cfg(test)]
fn wait_after_application_dml_for_test(key_digest: [u8; 32]) {
    let hook = {
        let mut installed = POST_APPLICATION_DML_HOOK
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        installed
            .as_ref()
            .is_some_and(|hook| hook.key_digest == key_digest)
            .then(|| {
                installed
                    .take()
                    .expect("the matching test hook is installed")
            })
    };
    if let Some(hook) = hook {
        let _ = hook.started.send(());
        hook.release
            .recv()
            .expect("the idempotency test releases the application DML boundary");
    }
}

#[cfg(not(test))]
fn wait_after_application_dml_for_test(_key_digest: [u8; 32]) {}

#[cfg(test)]
fn block_after_next_commit(
    key_digest: [u8; 32],
    started: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
) {
    let mut hook = POST_COMMIT_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(
        hook.replace(PostApplicationDmlHook {
            key_digest,
            started,
            release,
        })
        .is_none(),
        "only one post-commit hook may be installed"
    );
}

#[cfg(test)]
fn wait_after_commit_for_test(key_digest: [u8; 32]) {
    let hook = {
        let mut installed = POST_COMMIT_HOOK
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        installed
            .as_ref()
            .is_some_and(|hook| hook.key_digest == key_digest)
            .then(|| {
                installed
                    .take()
                    .expect("the matching test hook is installed")
            })
    };
    if let Some(hook) = hook {
        let _ = hook.started.send(());
        hook.release
            .recv()
            .expect("the idempotency test releases the committed boundary");
    }
}

#[cfg(not(test))]
fn wait_after_commit_for_test(_key_digest: [u8; 32]) {}

fn normalize_schema_sql(sql: &str) -> String {
    sql.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

fn corrupt(diagnostic: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::DataCorruption, diagnostic)
}

fn shard_read_error(error: rusqlite::Error, diagnostic: &'static str) -> EngineError {
    let classified = sqlite_error::storage(error);
    if matches!(
        classified.kind(),
        EngineErrorKind::Busy
            | EngineErrorKind::Cancelled
            | EngineErrorKind::PermissionDenied
            | EngineErrorKind::ReadOnly
            | EngineErrorKind::StorageFull
            | EngineErrorKind::OutOfMemory
            | EngineErrorKind::StorageUnavailable
    ) {
        classified.context(diagnostic)
    } else {
        EngineError::from_source(EngineErrorKind::DataCorruption, diagnostic, classified)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        process::{Command, Stdio},
        sync::Arc,
        thread,
        time::{Duration, Instant},
    };

    use super::*;
    use crate::{
        core::{CancellationReason, OperationControl},
        storage::{ConnectionOwner, ConnectionPools, Storage},
    };

    const CRASH_ROOT_ENV: &str = "BRISKDB_IDEMPOTENCY_CRASH_ROOT";
    const CRASH_READY_ENV: &str = "BRISKDB_IDEMPOTENCY_CRASH_READY";
    const CRASH_RELEASE_ENV: &str = "BRISKDB_IDEMPOTENCY_CRASH_RELEASE";
    const CRASH_BOUNDARY_ENV: &str = "BRISKDB_IDEMPOTENCY_CRASH_BOUNDARY";

    #[test]
    fn exact_schema_is_optional_and_near_misses_are_corruption() {
        let connection = Connection::open_in_memory().unwrap();
        assert!(!validate_optional_schema(&connection).unwrap());
        connection.execute_batch(RECEIPTS_SCHEMA_SQL).unwrap();
        assert!(validate_optional_schema(&connection).unwrap());

        let malformed = Connection::open_in_memory().unwrap();
        malformed
            .execute_batch(
                "CREATE TABLE briskdb_idempotency_receipts_v1 (
                    key_digest BLOB PRIMARY KEY,
                    request_digest BLOB
                 ) STRICT, WITHOUT ROWID",
            )
            .unwrap();
        assert_eq!(
            validate_optional_schema(&malformed).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn receipt_debug_output_redacts_both_digests() {
        let seed = NewIdempotencyReceipt::new([1; 32], [2; 32], 3, 10).unwrap();
        let seed_debug = format!("{seed:?}");
        assert_eq!(seed_debug.matches("<redacted>").count(), 2);
        assert!(!seed_debug.contains("[1, 1, 1"));
        assert!(!seed_debug.contains("[2, 2, 2"));

        let receipt = IdempotencyReceipt {
            key_digest: [1; 32],
            request_digest: [2; 32],
            target_shard: 3,
            rows_affected: 1,
            created_unix_ms: 10,
            expires_unix_ms: 10 + IDEMPOTENCY_RETENTION_MS,
        };
        let receipt_debug = format!("{receipt:?}");
        assert_eq!(receipt_debug.matches("<redacted>").count(), 2);
        assert!(!receipt_debug.contains("[1, 1, 1"));
        assert!(!receipt_debug.contains("[2, 2, 2"));
    }

    #[test]
    fn schema_digest_excludes_only_the_exact_receipt_object() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch("CREATE TABLE records (id INTEGER PRIMARY KEY) STRICT")
            .unwrap();
        let baseline = super::super::shard::calculate_schema_digest(&connection, 0).unwrap();
        connection.execute_batch(RECEIPTS_SCHEMA_SQL).unwrap();
        assert_eq!(
            super::super::shard::calculate_schema_digest(&connection, 0).unwrap(),
            baseline
        );

        let malformed = Connection::open_in_memory().unwrap();
        malformed
            .execute_batch(
                "CREATE TABLE briskdb_idempotency_receipts_v1 (
                    key_digest BLOB PRIMARY KEY
                 ) STRICT, WITHOUT ROWID",
            )
            .unwrap();
        assert_eq!(
            super::super::shard::calculate_schema_digest(&malformed, 0)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn failed_mutation_rolls_back_receipt_table_and_application_dml() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch("CREATE TABLE records (id INTEGER PRIMARY KEY)")
            .unwrap();
        let receipt = NewIdempotencyReceipt::new([1; 32], [2; 32], 0, 10).unwrap();
        let error = execute_with_receipt(
            &connection,
            0,
            "INSERT INTO records (id) VALUES (?1)",
            &[Value::Text("wrong type".to_owned())],
            receipt,
            None,
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::TypeMismatch);
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM records", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert!(!validate_optional_schema(&connection).unwrap());
    }

    #[tokio::test]
    async fn pooled_helpers_reach_only_the_exact_reserved_table() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let mut migration = storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        storage
            .apply_schema_migration(
                "CREATE TABLE records (id INTEGER PRIMARY KEY) STRICT",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();
        let pools = ConnectionPools::new(storage.clone(), 1, 0).unwrap();
        let mut connection = pools
            .acquire_for_owner(0, ConnectionOwner::new(1))
            .await
            .unwrap()
            .checkout()
            .unwrap();

        let outcome = connection
            .execute_idempotent(
                "INSERT INTO records (id) VALUES (?1)",
                &[Value::Int64(7)],
                NewIdempotencyReceipt::new([1; 32], [2; 32], 0, 10).unwrap(),
                OperationControl::new(None),
            )
            .unwrap();
        assert!(matches!(outcome, IdempotentExecuteOutcome::Executed(_)));
        assert!(
            connection
                .query_row(
                    "SELECT count(*) FROM briskdb_idempotency_receipts_v1",
                    [],
                    |row| { row.get::<_, i64>(0) }
                )
                .is_err(),
            "ordinary pooled SQL must not read receipt storage"
        );
        assert_eq!(
            connection
                .find_idempotency_receipt([1; 32], 10, OperationControl::new(None))
                .unwrap()
                .unwrap()
                .rows_affected(),
            1
        );

        let denied = connection
            .execute_idempotent(
                "DELETE FROM briskdb_idempotency_receipts_v1",
                &[],
                NewIdempotencyReceipt::new([3; 32], [4; 32], 0, 10).unwrap(),
                OperationControl::new(None),
            )
            .unwrap_err();
        assert_eq!(denied.kind(), EngineErrorKind::PermissionDenied);
        assert!(
            connection
                .find_idempotency_receipt([3; 32], 10, OperationControl::new(None))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM records", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        drop(connection);
        drop(pools);
        Storage::open(temp.path(), 2).unwrap();
    }

    #[tokio::test]
    async fn cancellation_after_application_dml_rolls_back_dml_and_receipt() {
        let _serial = POST_APPLICATION_DML_TEST_SERIAL.lock().await;
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let mut migration = storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        storage
            .apply_schema_migration(
                "CREATE TABLE records (id INTEGER PRIMARY KEY) STRICT",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();
        let pools = ConnectionPools::new(storage, 1, 0).unwrap();
        let connection = pools
            .acquire_for_owner(0, ConnectionOwner::new(1))
            .await
            .unwrap()
            .checkout()
            .unwrap();
        let control = OperationControl::new(None);
        let cancel = Arc::clone(&control);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        block_after_next_application_dml([5; 32], started_tx, release_rx);

        let worker = std::thread::spawn(move || {
            let mut connection = connection;
            let result = connection.execute_idempotent(
                "INSERT INTO records (id) VALUES (7)",
                &[],
                NewIdempotencyReceipt::new([5; 32], [6; 32], 0, 10).unwrap(),
                control,
            );
            (connection, result)
        });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("the application DML must reach the cancellation boundary");
        assert!(cancel.request_cancel(CancellationReason::Cancelled));
        release_tx.send(()).unwrap();
        let (connection, result) = worker.join().unwrap();
        assert_eq!(result.unwrap_err().kind(), EngineErrorKind::Cancelled);
        drop(connection);

        let mut connection = pools
            .acquire_for_owner(0, ConnectionOwner::new(2))
            .await
            .unwrap()
            .checkout()
            .unwrap();
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM records", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert!(
            connection
                .find_idempotency_receipt([5; 32], 10, OperationControl::new(None))
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn process_death_between_dml_and_receipt_commits_neither() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let mut migration = storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        storage
            .apply_schema_migration(
                "CREATE TABLE records (id INTEGER PRIMARY KEY) STRICT",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();
        drop(storage);

        let ready = temp.path().join("crash-ready");
        let release = temp.path().join("crash-release");
        let mut child = Command::new(env::current_exe().unwrap())
            .arg("--exact")
            .arg("storage::idempotency::tests::subprocess_idempotent_write_holder")
            .arg("--nocapture")
            .env(CRASH_ROOT_ENV, temp.path())
            .env(CRASH_READY_ENV, &ready)
            .env(CRASH_RELEASE_ENV, &release)
            .env(CRASH_BOUNDARY_ENV, "precommit")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            ready.exists(),
            "child did not reach the uncommitted DML boundary"
        );
        child.kill().unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(!output.status.success());

        let storage = Storage::open(temp.path(), 2).unwrap();
        let pools = ConnectionPools::new(storage, 1, 0).unwrap();
        let mut connection = pools
            .acquire_for_owner(0, ConnectionOwner::new(3))
            .await
            .unwrap()
            .checkout()
            .unwrap();
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM records", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert!(
            connection
                .find_idempotency_receipt([0x5a; 32], 10, OperationControl::new(None))
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn process_death_after_commit_reopens_and_replays_one_write() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 2).unwrap();
        let mut migration = storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        storage
            .apply_schema_migration(
                "CREATE TABLE records (id INTEGER PRIMARY KEY) STRICT",
                &mut migration,
                None,
            )
            .unwrap();
        migration.publish_ready().unwrap();
        drop(storage);

        let ready = temp.path().join("commit-ready");
        let release = temp.path().join("commit-release");
        let mut child = Command::new(env::current_exe().unwrap())
            .arg("--exact")
            .arg("storage::idempotency::tests::subprocess_idempotent_write_holder")
            .arg("--nocapture")
            .env(CRASH_ROOT_ENV, temp.path())
            .env(CRASH_READY_ENV, &ready)
            .env(CRASH_RELEASE_ENV, &release)
            .env(CRASH_BOUNDARY_ENV, "postcommit")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "child did not reach the committed boundary");
        child.kill().unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(!output.status.success());

        let storage = Storage::open(temp.path(), 2).unwrap();
        let pools = ConnectionPools::new(storage, 1, 0).unwrap();
        let mut connection = pools
            .acquire_for_owner(0, ConnectionOwner::new(5))
            .await
            .unwrap()
            .checkout()
            .unwrap();
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM records", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        let receipt = connection
            .find_idempotency_receipt([0x5a; 32], 10, OperationControl::new(None))
            .unwrap()
            .expect("the committed receipt must survive process death");
        assert_eq!(receipt.rows_affected(), 1);
        let replay = connection
            .execute_idempotent(
                "INSERT INTO records (id) VALUES (8)",
                &[],
                NewIdempotencyReceipt::new([0x5a; 32], [0x6b; 32], 0, 10).unwrap(),
                OperationControl::new(None),
            )
            .unwrap();
        assert!(matches!(replay, IdempotentExecuteOutcome::Existing(_)));
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM records", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn subprocess_idempotent_write_holder() {
        let Ok(root) = env::var(CRASH_ROOT_ENV) else {
            return;
        };
        let ready = std::path::PathBuf::from(env::var(CRASH_READY_ENV).unwrap());
        let release = std::path::PathBuf::from(env::var(CRASH_RELEASE_ENV).unwrap());
        let boundary = env::var(CRASH_BOUNDARY_ENV).unwrap();
        let storage = Storage::open(root, 2).unwrap();
        let pools = ConnectionPools::new(storage, 1, 0).unwrap();
        let connection = pools
            .acquire_for_owner(0, ConnectionOwner::new(4))
            .await
            .unwrap()
            .checkout()
            .unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        match boundary.as_str() {
            "precommit" => block_after_next_application_dml([0x5a; 32], started_tx, release_rx),
            "postcommit" => block_after_next_commit([0x5a; 32], started_tx, release_rx),
            _ => panic!("unsupported idempotency crash boundary"),
        }
        let worker = thread::spawn(move || {
            let mut connection = connection;
            connection.execute_idempotent(
                "INSERT INTO records (id) VALUES (7)",
                &[],
                NewIdempotencyReceipt::new([0x5a; 32], [0x6b; 32], 0, 10).unwrap(),
                OperationControl::new(None),
            )
        });
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("child DML must reach its uncommitted boundary");
        fs::write(&ready, b"ready").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !release.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if release.exists() {
            release_tx.send(()).unwrap();
            worker.join().unwrap().unwrap();
        }
    }

    #[test]
    fn mutation_and_receipt_commit_together_and_replay_skips_dml() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch("CREATE TABLE records (id INTEGER PRIMARY KEY)")
            .unwrap();
        let receipt = NewIdempotencyReceipt::new([1; 32], [2; 32], 0, 10).unwrap();
        let executed = execute_with_receipt(
            &connection,
            0,
            "INSERT INTO records (id) VALUES (?1)",
            &[Value::Int64(7)],
            receipt.clone(),
            None,
        )
        .unwrap();
        let IdempotentExecuteOutcome::Executed(stored) = executed else {
            panic!("the first write must execute");
        };
        assert_eq!(stored.rows_affected(), 1);
        assert_eq!(
            find_receipt(&connection, [1; 32], 10, None).unwrap(),
            Some(stored.clone())
        );

        let replay = execute_with_receipt(
            &connection,
            0,
            "INSERT INTO records (id) VALUES (?1)",
            &[Value::Int64(8)],
            receipt,
            None,
        )
        .unwrap();
        assert_eq!(replay, IdempotentExecuteOutcome::Existing(stored));
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM records", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn expired_receipt_is_replaced_with_a_fresh_atomic_result() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch("CREATE TABLE records (id INTEGER PRIMARY KEY)")
            .unwrap();
        execute_with_receipt(
            &connection,
            0,
            "INSERT INTO records (id) VALUES (1)",
            &[],
            NewIdempotencyReceipt::new([1; 32], [2; 32], 0, 10).unwrap(),
            None,
        )
        .unwrap();
        let replaced = execute_with_receipt(
            &connection,
            0,
            "INSERT INTO records (id) VALUES (2)",
            &[],
            NewIdempotencyReceipt::new([1; 32], [3; 32], 0, 10 + IDEMPOTENCY_RETENTION_MS).unwrap(),
            None,
        )
        .unwrap();
        let IdempotentExecuteOutcome::Executed(replaced) = replaced else {
            panic!("an expired key must be reusable");
        };
        assert_eq!(replaced.request_digest(), [3; 32]);
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM records", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[test]
    fn forward_reuse_then_clock_rollback_cannot_revive_an_old_shard() {
        let first = Connection::open_in_memory().unwrap();
        let second = Connection::open_in_memory().unwrap();
        first
            .execute_batch("CREATE TABLE records (id INTEGER PRIMARY KEY) STRICT")
            .unwrap();
        second
            .execute_batch("CREATE TABLE records (id INTEGER PRIMARY KEY) STRICT")
            .unwrap();
        let key = [7; 32];
        execute_with_receipt(
            &first,
            0,
            "INSERT INTO records (id) VALUES (1)",
            &[],
            NewIdempotencyReceipt::new(key, [1; 32], 0, 10).unwrap(),
            None,
        )
        .unwrap();

        let forward_time = 10 + IDEMPOTENCY_RETENTION_MS;
        assert!(
            find_receipt(&first, key, forward_time, None)
                .unwrap()
                .is_none()
        );
        execute_with_receipt(
            &second,
            1,
            "INSERT INTO records (id) VALUES (2)",
            &[],
            NewIdempotencyReceipt::new(key, [2; 32], 1, forward_time).unwrap(),
            None,
        )
        .unwrap();

        assert!(find_receipt(&first, key, 10, None).unwrap().is_none());
        let current = find_receipt(&second, key, 10, None).unwrap().unwrap();
        assert_eq!(current.target_shard(), 1);
        assert_eq!(current.request_digest(), [2; 32]);
    }

    #[test]
    fn generic_expired_cleanup_deletes_at_most_one_bounded_batch() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(RECEIPTS_SCHEMA_SQL).unwrap();
        connection
            .execute_batch("CREATE TABLE records (id INTEGER PRIMARY KEY) STRICT")
            .unwrap();
        let transaction = connection.unchecked_transaction().unwrap();
        {
            let mut insert = transaction
                .prepare(
                    "INSERT INTO briskdb_idempotency_receipts_v1
                     VALUES (?1, ?2, 0, 1, 0, 86400000, 1)",
                )
                .unwrap();
            for value in 0_u32..100 {
                let mut digest = [0_u8; 32];
                digest[..4].copy_from_slice(&value.to_le_bytes());
                insert
                    .execute(params![&digest[..], &[9_u8; 32][..]])
                    .unwrap();
            }
        }
        transaction.commit().unwrap();

        execute_with_receipt(
            &connection,
            0,
            "INSERT INTO records (id) VALUES (1)",
            &[],
            NewIdempotencyReceipt::new([0xff; 32], [8; 32], 0, IDEMPOTENCY_RETENTION_MS).unwrap(),
            None,
        )
        .unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM briskdb_idempotency_receipts_v1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            37
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM briskdb_idempotency_receipts_v1
                     WHERE expires_unix_ms <= 86400000",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            36
        );
    }

    #[test]
    fn receipt_capacity_fails_before_application_dml() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(RECEIPTS_SCHEMA_SQL).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE records (id INTEGER PRIMARY KEY, touched INTEGER NOT NULL) STRICT;
                 INSERT INTO records (id, touched) VALUES (1, 0)",
            )
            .unwrap();
        let transaction = connection.unchecked_transaction().unwrap();
        {
            let mut insert = transaction
                .prepare(
                    "INSERT INTO briskdb_idempotency_receipts_v1
                     VALUES (?1, ?2, 0, 1, 10, 86400010, 1)",
                )
                .unwrap();
            for value in 0_u32..MAX_IDEMPOTENCY_RECEIPTS_PER_SHARD as u32 {
                let mut digest = [0_u8; 32];
                digest[..4].copy_from_slice(&value.to_le_bytes());
                insert
                    .execute(params![&digest[..], &[9_u8; 32][..]])
                    .unwrap();
            }
        }
        transaction.commit().unwrap();

        let error = execute_with_receipt(
            &connection,
            0,
            "UPDATE records SET touched = 1 WHERE id = 1",
            &[],
            NewIdempotencyReceipt::new([0xff; 32], [8; 32], 0, 10).unwrap(),
            None,
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(
            connection
                .query_row("SELECT touched FROM records WHERE id = 1", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn strict_state_validation_rejects_misplaced_rows() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(RECEIPTS_SCHEMA_SQL).unwrap();
        connection
            .execute(
                "INSERT INTO briskdb_idempotency_receipts_v1
                 VALUES (?1, ?2, 1, 0, 10, 86400010, 1)",
                params![&[1_u8; 32][..], &[2_u8; 32][..]],
            )
            .unwrap();
        assert_eq!(
            validate_optional_state(&connection, 0).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
        assert!(validate_optional_state(&connection, 1).unwrap());
    }
}
