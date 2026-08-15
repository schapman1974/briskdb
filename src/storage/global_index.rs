//! Offline construction and durable storage for global indexes.

use std::{
    collections::BTreeSet,
    fs::{self, File},
    path::{Path, PathBuf},
    str,
};

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
    types::ValueRef,
};

use crate::{
    core::{
        CancellationToken, CanonicalIndexKey, EngineError, EngineErrorKind, EngineResult,
        GlobalIndexBuildReport, GlobalIndexId, GlobalIndexKeySource, GlobalIndexKeyType,
        GlobalIndexLifecycle, GlobalIndexMetadata, GlobalIndexStorageTopology,
        INDEX_KEY_ENCODING_VERSION, IndexKeyOrder, IndexKeyPart, IndexKeyValue,
        UniqueNullSemantics,
    },
    sqlite_error,
};

use super::{CONNECTION_BUSY_TIMEOUT, Storage};

const DIRECTORY_NAME: &str = "global-indexes";
const SHARED_FILE_NAME: &str = "global.sqlite";
const APPLICATION_ID: i32 = 0x4252_4749;
const STORAGE_VERSION: u32 = 1;
const BUILDING: i64 = 1;
const COMPLETE: i64 = 2;
const DEFINITION_DIGEST_DOMAIN: &[u8] = b"briskdb.global-index.definition.v1\0";
const SOURCE_DIGEST_DOMAIN: &[u8] = b"briskdb.global-index.source-shard.v1\0";
const LOCATOR_MAGIC: &[u8; 4] = b"BRIL";
const LOCATOR_VERSION: u32 = 1;

const SCHEMA_SQL: &str = "
CREATE TABLE briskdb_global_index_storage (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    storage_version INTEGER NOT NULL CHECK (storage_version = 1),
    key_encoding_version INTEGER NOT NULL CHECK (key_encoding_version = 1)
) STRICT;

CREATE TABLE briskdb_global_index_builds (
    index_id INTEGER PRIMARY KEY CHECK (index_id > 0),
    definition_digest BLOB NOT NULL CHECK (length(definition_digest) = 32),
    schema_generation INTEGER NOT NULL CHECK (schema_generation >= 0),
    shard_count INTEGER NOT NULL CHECK (shard_count BETWEEN 2 AND 64),
    build_state INTEGER NOT NULL CHECK (build_state IN (1, 2)),
    indexed_rows INTEGER NOT NULL CHECK (indexed_rows >= 0)
) STRICT;

CREATE TABLE briskdb_global_index_checkpoints (
    index_id INTEGER NOT NULL,
    source_shard INTEGER NOT NULL CHECK (source_shard BETWEEN 0 AND 63),
    source_digest BLOB NOT NULL CHECK (length(source_digest) = 32),
    indexed_rows INTEGER NOT NULL CHECK (indexed_rows >= 0),
    unique_rows INTEGER NOT NULL CHECK (unique_rows >= 0),
    PRIMARY KEY (index_id, source_shard),
    FOREIGN KEY (index_id) REFERENCES briskdb_global_index_builds (index_id)
        ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE briskdb_global_index_entries (
    index_id INTEGER NOT NULL,
    encoded_key BLOB NOT NULL,
    source_shard INTEGER NOT NULL CHECK (source_shard BETWEEN 0 AND 63),
    source_ordinal INTEGER NOT NULL CHECK (source_ordinal >= 0),
    source_locator BLOB NOT NULL,
    PRIMARY KEY (index_id, encoded_key, source_shard, source_locator),
    UNIQUE (index_id, source_shard, source_ordinal),
    FOREIGN KEY (index_id) REFERENCES briskdb_global_index_builds (index_id)
        ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE briskdb_global_index_unique_keys (
    index_id INTEGER NOT NULL,
    encoded_key BLOB NOT NULL,
    source_shard INTEGER NOT NULL CHECK (source_shard BETWEEN 0 AND 63),
    source_locator BLOB NOT NULL,
    PRIMARY KEY (index_id, encoded_key),
    FOREIGN KEY (index_id) REFERENCES briskdb_global_index_builds (index_id)
        ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

INSERT INTO briskdb_global_index_storage (
    singleton, storage_version, key_encoding_version
) VALUES (1, 1, 1);
";

const EXPECTED_OBJECTS: &[&str] = &[
    "briskdb_global_index_builds",
    "briskdb_global_index_checkpoints",
    "briskdb_global_index_entries",
    "briskdb_global_index_storage",
    "briskdb_global_index_unique_keys",
];

#[derive(Debug, Clone, PartialEq, Eq)]
struct Checkpoint {
    source_shard: u16,
    source_digest: [u8; 32],
    indexed_rows: u64,
    unique_rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BuildState {
    state: i64,
    indexed_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SourceLocator {
    RowId(String),
    PrimaryKey(Vec<String>),
}

impl SourceLocator {
    fn expressions(&self) -> Vec<String> {
        match self {
            Self::RowId(name) => vec![quote_identifier(name)],
            Self::PrimaryKey(columns) => columns
                .iter()
                .map(|column| quote_identifier(column))
                .collect(),
        }
    }
}

#[derive(Debug)]
struct ScanOutcome {
    source_digest: [u8; 32],
    indexed_rows: u64,
    unique_rows: u64,
}

pub(super) fn build(
    storage: &Storage,
    index: &GlobalIndexMetadata,
    cancellation: &CancellationToken,
) -> EngineResult<GlobalIndexBuildReport> {
    ensure_not_cancelled(cancellation, "before global-index construction")?;
    if index.topology() != GlobalIndexStorageTopology::SharedSqliteV1 {
        return Err(EngineError::new(
            EngineErrorKind::Unsupported,
            format!(
                "global index {} cannot be built with {:?}; the initial builder supports only SharedSqliteV1",
                index.id(),
                index.topology()
            ),
        ));
    }
    match index.lifecycle() {
        GlobalIndexLifecycle::Creating | GlobalIndexLifecycle::Ready => {}
        GlobalIndexLifecycle::Rebuilding => {
            return Err(EngineError::new(
                EngineErrorKind::Unsupported,
                format!(
                    "global index {} replacement construction belongs to the validation and rebuild workflow",
                    index.id()
                ),
            ));
        }
        lifecycle => {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "global index {} cannot be built while its lifecycle is {lifecycle:?}",
                    index.id()
                ),
            ));
        }
    }

    let (mut connection, path) = open_or_create(&storage.root)?;
    cleanup_abandoned(&mut connection, storage.catalog.logical().global_indexes())?;
    let definition_digest = definition_digest(index);
    prepare_build(
        &mut connection,
        index,
        storage.shard_count(),
        &definition_digest,
    )?;
    abort_at_test_boundary("initialized");

    let checkpoints = load_checkpoints(&connection, index.id(), storage.shard_count())?;
    let mut resumed_from_shard = checkpoints.len() as u16;
    for checkpoint in &checkpoints {
        ensure_not_cancelled(cancellation, "while revalidating a build checkpoint")?;
        let observed =
            scan_source_shard(storage, index, checkpoint.source_shard, cancellation, None)?;
        if observed.source_digest != checkpoint.source_digest
            || observed.indexed_rows != checkpoint.indexed_rows
            || observed.unique_rows != checkpoint.unique_rows
        {
            reset_build(
                &mut connection,
                index,
                storage.shard_count(),
                &definition_digest,
            )?;
            resumed_from_shard = 0;
            break;
        }
    }

    for shard in resumed_from_shard..storage.shard_count() {
        ensure_not_cancelled(cancellation, "before scanning a source shard")?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error::storage)?;
        delete_shard_rows(&transaction, index.id(), shard)?;
        let outcome = scan_source_shard(storage, index, shard, cancellation, Some(&transaction))?;
        write_checkpoint(&transaction, index.id(), shard, &outcome)?;
        ensure_not_cancelled(cancellation, "before committing a source-shard checkpoint")?;
        abort_at_test_boundary(&format!("shard-{shard}-before-commit"));
        transaction.commit().map_err(sqlite_error::storage)?;
        abort_at_test_boundary(&format!("shard-{shard}-after-commit"));
    }

    let checkpoints = load_checkpoints(&connection, index.id(), storage.shard_count())?;
    verify_checkpoint_sources(storage, index, cancellation, &checkpoints)?;
    let indexed_rows = checkpoints.iter().try_fold(0_u64, |total, checkpoint| {
        total.checked_add(checkpoint.indexed_rows).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                "global-index row count overflowed",
            )
        })
    })?;
    let unique_rows = checkpoints.iter().try_fold(0_u64, |total, checkpoint| {
        total.checked_add(checkpoint.unique_rows).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                "global-index unique reservation count overflowed",
            )
        })
    })?;
    validate_physical_contents(&connection, index, &checkpoints, indexed_rows, unique_rows)?;

    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "UPDATE briskdb_global_index_builds
             SET build_state = ?1, indexed_rows = ?2
             WHERE index_id = ?3",
            params![
                COMPLETE,
                to_sqlite_u64(indexed_rows, "global-index row count")?,
                to_sqlite_id(index.id())?,
            ],
        )
        .map_err(sqlite_error::storage)?;
    ensure_not_cancelled(
        cancellation,
        "before completing physical global-index storage",
    )?;
    abort_at_test_boundary("complete-before-commit");
    transaction.commit().map_err(sqlite_error::storage)?;
    abort_at_test_boundary("complete-after-commit");

    checkpoint_and_sync(&connection, &path)?;
    Ok(GlobalIndexBuildReport::from_validated(
        index.id(),
        storage.shard_count(),
        resumed_from_shard,
        indexed_rows,
    ))
}

pub(super) fn validate_ready(
    storage: &Storage,
    index: &GlobalIndexMetadata,
    cancellation: &CancellationToken,
) -> EngineResult<GlobalIndexBuildReport> {
    let (connection, _) = open_existing(&storage.root)?.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "ready global index {} has no physical storage file",
                index.id()
            ),
        )
    })?;
    let digest = definition_digest(index);
    let state = load_build_state(&connection, index.id(), storage.shard_count(), &digest)?
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::DataCorruption,
                format!(
                    "ready global index {} has no physical build record",
                    index.id()
                ),
            )
        })?;
    if state.state != COMPLETE {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "ready global index {} is not physically complete",
                index.id()
            ),
        ));
    }
    let checkpoints = load_checkpoints(&connection, index.id(), storage.shard_count())?;
    verify_checkpoint_sources(storage, index, cancellation, &checkpoints)?;
    let unique_rows = checkpoints
        .iter()
        .map(|checkpoint| checkpoint.unique_rows)
        .sum();
    validate_physical_contents(
        &connection,
        index,
        &checkpoints,
        state.indexed_rows,
        unique_rows,
    )?;
    Ok(GlobalIndexBuildReport::from_validated(
        index.id(),
        storage.shard_count(),
        storage.shard_count(),
        state.indexed_rows,
    ))
}

pub(super) fn remove_artifacts(root: &Path, index_id: GlobalIndexId) -> EngineResult<()> {
    let Some((mut connection, path)) = open_existing(root)? else {
        return Ok(());
    };
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "DELETE FROM briskdb_global_index_builds WHERE index_id = ?1",
            [to_sqlite_id(index_id)?],
        )
        .map_err(sqlite_error::storage)?;
    transaction.commit().map_err(sqlite_error::storage)?;
    checkpoint_and_sync(&connection, &path)
}

fn prepare_build(
    connection: &mut Connection,
    index: &GlobalIndexMetadata,
    shard_count: u16,
    definition_digest: &[u8; 32],
) -> EngineResult<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    let existing = transaction
        .query_row(
            "SELECT definition_digest, schema_generation, shard_count
             FROM briskdb_global_index_builds WHERE index_id = ?1",
            [to_sqlite_id(index.id())?],
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
    let expected_generation = to_sqlite_u64(index.schema_generation(), "schema generation")?;
    if existing
        .as_ref()
        .is_some_and(|(digest, generation, shards)| {
            digest.as_slice() != definition_digest
                || *generation != expected_generation
                || *shards != i64::from(shard_count)
        })
    {
        transaction
            .execute(
                "DELETE FROM briskdb_global_index_builds WHERE index_id = ?1",
                [to_sqlite_id(index.id())?],
            )
            .map_err(sqlite_error::storage)?;
    }
    transaction
        .execute(
            "INSERT OR IGNORE INTO briskdb_global_index_builds (
                 index_id, definition_digest, schema_generation, shard_count,
                 build_state, indexed_rows
             ) VALUES (?1, ?2, ?3, ?4, ?5, 0)",
            params![
                to_sqlite_id(index.id())?,
                definition_digest.as_slice(),
                expected_generation,
                i64::from(shard_count),
                BUILDING,
            ],
        )
        .map_err(sqlite_error::storage)?;
    transaction.commit().map_err(sqlite_error::storage)
}

fn reset_build(
    connection: &mut Connection,
    index: &GlobalIndexMetadata,
    shard_count: u16,
    definition_digest: &[u8; 32],
) -> EngineResult<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "DELETE FROM briskdb_global_index_builds WHERE index_id = ?1",
            [to_sqlite_id(index.id())?],
        )
        .map_err(sqlite_error::storage)?;
    transaction
        .execute(
            "INSERT INTO briskdb_global_index_builds (
                 index_id, definition_digest, schema_generation, shard_count,
                 build_state, indexed_rows
             ) VALUES (?1, ?2, ?3, ?4, ?5, 0)",
            params![
                to_sqlite_id(index.id())?,
                definition_digest.as_slice(),
                to_sqlite_u64(index.schema_generation(), "schema generation")?,
                i64::from(shard_count),
                BUILDING,
            ],
        )
        .map_err(sqlite_error::storage)?;
    transaction.commit().map_err(sqlite_error::storage)
}

fn cleanup_abandoned(
    connection: &mut Connection,
    indexes: &[GlobalIndexMetadata],
) -> EngineResult<()> {
    let live = indexes
        .iter()
        .map(|index| to_sqlite_id(index.id()))
        .collect::<EngineResult<BTreeSet<_>>>()?;
    let stored = {
        let mut statement = connection
            .prepare("SELECT index_id FROM briskdb_global_index_builds ORDER BY index_id")
            .map_err(sqlite_error::storage)?;
        statement
            .query_map([], |row| row.get::<_, i64>(0))
            .map_err(sqlite_error::storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_error::storage)?
    };
    let abandoned = stored
        .into_iter()
        .filter(|index_id| !live.contains(index_id))
        .collect::<Vec<_>>();
    if abandoned.is_empty() {
        return Ok(());
    }
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    for index_id in abandoned {
        transaction
            .execute(
                "DELETE FROM briskdb_global_index_builds WHERE index_id = ?1",
                [index_id],
            )
            .map_err(sqlite_error::storage)?;
    }
    transaction.commit().map_err(sqlite_error::storage)
}

fn load_build_state(
    connection: &Connection,
    index_id: GlobalIndexId,
    shard_count: u16,
    definition_digest: &[u8; 32],
) -> EngineResult<Option<BuildState>> {
    connection
        .query_row(
            "SELECT build_state, indexed_rows
             FROM briskdb_global_index_builds
             WHERE index_id = ?1 AND definition_digest = ?2 AND shard_count = ?3",
            params![
                to_sqlite_id(index_id)?,
                definition_digest.as_slice(),
                i64::from(shard_count),
            ],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(sqlite_error::storage)?
        .map(|(state, rows)| {
            Ok(BuildState {
                state,
                indexed_rows: from_sqlite_u64(rows, "global-index row count")?,
            })
        })
        .transpose()
}

fn load_checkpoints(
    connection: &Connection,
    index_id: GlobalIndexId,
    shard_count: u16,
) -> EngineResult<Vec<Checkpoint>> {
    let mut statement = connection
        .prepare(
            "SELECT source_shard, source_digest, indexed_rows, unique_rows
             FROM briskdb_global_index_checkpoints
             WHERE index_id = ?1 ORDER BY source_shard",
        )
        .map_err(sqlite_error::storage)?;
    let rows = statement
        .query_map([to_sqlite_id(index_id)?], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .map_err(sqlite_error::storage)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error::storage)?;
    if rows.len() > usize::from(shard_count) {
        return Err(corrupt(format!(
            "global index {index_id} has more checkpoints than source shards"
        )));
    }
    rows.into_iter()
        .enumerate()
        .map(
            |(expected, (source_shard, digest, indexed_rows, unique_rows))| {
                let source_shard = u16::try_from(source_shard).map_err(|_| {
                    corrupt(format!(
                        "global index {index_id} has an invalid source-shard checkpoint"
                    ))
                })?;
                if usize::from(source_shard) != expected || source_shard >= shard_count {
                    return Err(corrupt(format!(
                        "global index {index_id} checkpoints are not a contiguous shard prefix"
                    )));
                }
                let source_digest: [u8; 32] = digest.try_into().map_err(|_| {
                    corrupt(format!(
                        "global index {index_id} has an invalid checkpoint digest"
                    ))
                })?;
                Ok(Checkpoint {
                    source_shard,
                    source_digest,
                    indexed_rows: from_sqlite_u64(indexed_rows, "checkpoint row count")?,
                    unique_rows: from_sqlite_u64(unique_rows, "checkpoint unique row count")?,
                })
            },
        )
        .collect()
}

fn verify_checkpoint_sources(
    storage: &Storage,
    index: &GlobalIndexMetadata,
    cancellation: &CancellationToken,
    checkpoints: &[Checkpoint],
) -> EngineResult<()> {
    if checkpoints.len() != usize::from(storage.shard_count()) {
        return Err(corrupt(format!(
            "global index {} has an incomplete checkpoint set",
            index.id()
        )));
    }
    for checkpoint in checkpoints {
        ensure_not_cancelled(cancellation, "while validating completed source shards")?;
        let observed =
            scan_source_shard(storage, index, checkpoint.source_shard, cancellation, None)?;
        if observed.source_digest != checkpoint.source_digest
            || observed.indexed_rows != checkpoint.indexed_rows
            || observed.unique_rows != checkpoint.unique_rows
        {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "global index {} source shard {} changed or uses a nondeterministic expression during its offline build",
                    index.id(),
                    checkpoint.source_shard
                ),
            ));
        }
    }
    Ok(())
}

fn scan_source_shard(
    storage: &Storage,
    index: &GlobalIndexMetadata,
    shard: u16,
    cancellation: &CancellationToken,
    target: Option<&Transaction<'_>>,
) -> EngineResult<ScanOutcome> {
    let source = storage.open_shard(shard)?;
    let progress_cancellation = cancellation.clone();
    source
        .progress_handler(1_000, Some(move || progress_cancellation.is_cancelled()))
        .map_err(sqlite_error::storage)?;
    source
        .execute_batch("BEGIN DEFERRED TRANSACTION")
        .map_err(sqlite_error::storage)?;
    let table = storage
        .catalog
        .logical()
        .table_by_id(index.table_id())
        .ok_or_else(|| {
            corrupt(format!(
                "global index {} references a missing table",
                index.id()
            ))
        })?;
    let locator = inspect_source_locator(&source, table.name())?;
    let sql = scan_sql(index, table.name(), &locator);
    let mut statement = source.prepare(&sql).map_err(sqlite_error::statement)?;
    let locator_count = locator.expressions().len();
    let mut rows = statement.query([]).map_err(sqlite_error::statement)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(SOURCE_DIGEST_DOMAIN);
    hasher.update(&index.id().get().to_le_bytes());
    hasher.update(&shard.to_le_bytes());
    let mut indexed_rows = 0_u64;
    let mut unique_rows = 0_u64;
    while let Some(row) = rows.next().map_err(sqlite_error::statement)? {
        ensure_not_cancelled(cancellation, "while scanning a source shard")?;
        let values = index
            .key_parts()
            .iter()
            .enumerate()
            .map(|(ordinal, part)| {
                read_key_value(
                    row.get_ref(ordinal).map_err(sqlite_error::statement)?,
                    part.key_type(),
                    index,
                    shard,
                    ordinal,
                )
            })
            .collect::<EngineResult<Vec<_>>>()?;
        let parts = values
            .iter()
            .zip(index.key_parts())
            .map(|(value, metadata)| {
                let part = match metadata.order() {
                    IndexKeyOrder::Ascending => IndexKeyPart::ascending(value.as_ref()),
                    IndexKeyOrder::Descending => IndexKeyPart::descending(value.as_ref()),
                };
                part.with_null_order(metadata.null_order())
                    .with_collation(metadata.collation())
            })
            .collect::<Vec<_>>();
        let encoded_key = CanonicalIndexKey::encode(&parts)?;
        let locator_values = (0..locator_count)
            .map(|offset| {
                row.get_ref(index.key_parts().len() + offset)
                    .map_err(sqlite_error::statement)
            })
            .collect::<EngineResult<Vec<_>>>()?;
        let encoded_locator = encode_locator(&locator_values)?;
        update_framed(&mut hasher, encoded_key.as_bytes());
        update_framed(&mut hasher, &encoded_locator);

        if let Some(target) = target {
            insert_entry(
                target,
                index,
                shard,
                indexed_rows,
                &encoded_key,
                &encoded_locator,
                &parts,
            )?;
        }
        indexed_rows = indexed_rows.checked_add(1).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                "global-index row count overflowed",
            )
        })?;
        if index.is_unique()
            && !(index.null_semantics() == UniqueNullSemantics::Distinct
                && values
                    .iter()
                    .any(|value| matches!(value, IndexKeyValue::Null)))
        {
            unique_rows = unique_rows.checked_add(1).ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::NumericOutOfRange,
                    "global-index unique reservation count overflowed",
                )
            })?;
        }
    }
    source
        .execute_batch("COMMIT")
        .map_err(sqlite_error::storage)?;
    Ok(ScanOutcome {
        source_digest: *hasher.finalize().as_bytes(),
        indexed_rows,
        unique_rows,
    })
}

fn insert_entry(
    transaction: &Transaction<'_>,
    index: &GlobalIndexMetadata,
    shard: u16,
    source_ordinal: u64,
    encoded_key: &CanonicalIndexKey,
    encoded_locator: &[u8],
    parts: &[IndexKeyPart<'_>],
) -> EngineResult<()> {
    let index_id = to_sqlite_id(index.id())?;
    transaction
        .execute(
            "INSERT INTO briskdb_global_index_entries (
                 index_id, encoded_key, source_shard, source_ordinal, source_locator
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                index_id,
                encoded_key.as_bytes(),
                i64::from(shard),
                to_sqlite_u64(source_ordinal, "source row ordinal")?,
                encoded_locator,
            ],
        )
        .map_err(sqlite_error::storage)?;
    if !index.is_unique()
        || CanonicalIndexKey::encode_unique(parts, index.null_semantics())?.is_none()
    {
        return Ok(());
    }
    let inserted = transaction
        .execute(
            "INSERT OR IGNORE INTO briskdb_global_index_unique_keys (
                 index_id, encoded_key, source_shard, source_locator
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                index_id,
                encoded_key.as_bytes(),
                i64::from(shard),
                encoded_locator,
            ],
        )
        .map_err(sqlite_error::storage)?;
    if inserted == 1 {
        return Ok(());
    }
    let (existing_shard, existing_locator) = transaction
        .query_row(
            "SELECT source_shard, source_locator
             FROM briskdb_global_index_unique_keys
             WHERE index_id = ?1 AND encoded_key = ?2",
            params![index_id, encoded_key.as_bytes()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .map_err(sqlite_error::storage)?;
    Err(EngineError::new(
        EngineErrorKind::UniqueViolation,
        format!(
            "global index {} ({}) found a duplicate unique key at shard {} row {} and shard {} row {}; key bytes are redacted",
            index.id(),
            index.name(),
            existing_shard,
            locator_label(&existing_locator),
            shard,
            locator_label(encoded_locator),
        ),
    ))
}

fn delete_shard_rows(
    transaction: &Transaction<'_>,
    index_id: GlobalIndexId,
    shard: u16,
) -> EngineResult<()> {
    for table in [
        "briskdb_global_index_unique_keys",
        "briskdb_global_index_entries",
        "briskdb_global_index_checkpoints",
    ] {
        transaction
            .execute(
                &format!("DELETE FROM {table} WHERE index_id = ?1 AND source_shard = ?2"),
                params![to_sqlite_id(index_id)?, i64::from(shard)],
            )
            .map_err(sqlite_error::storage)?;
    }
    transaction
        .execute(
            "UPDATE briskdb_global_index_builds
             SET build_state = ?1, indexed_rows = 0 WHERE index_id = ?2",
            params![BUILDING, to_sqlite_id(index_id)?],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn write_checkpoint(
    transaction: &Transaction<'_>,
    index_id: GlobalIndexId,
    shard: u16,
    outcome: &ScanOutcome,
) -> EngineResult<()> {
    transaction
        .execute(
            "INSERT INTO briskdb_global_index_checkpoints (
                 index_id, source_shard, source_digest, indexed_rows, unique_rows
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                to_sqlite_id(index_id)?,
                i64::from(shard),
                outcome.source_digest.as_slice(),
                to_sqlite_u64(outcome.indexed_rows, "checkpoint row count")?,
                to_sqlite_u64(outcome.unique_rows, "checkpoint unique row count")?,
            ],
        )
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn validate_physical_contents(
    connection: &Connection,
    index: &GlobalIndexMetadata,
    checkpoints: &[Checkpoint],
    indexed_rows: u64,
    unique_rows: u64,
) -> EngineResult<()> {
    let index_id = to_sqlite_id(index.id())?;
    let entries = connection
        .query_row(
            "SELECT COUNT(*) FROM briskdb_global_index_entries WHERE index_id = ?1",
            [index_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sqlite_error::storage)?;
    let reservations = connection
        .query_row(
            "SELECT COUNT(*) FROM briskdb_global_index_unique_keys WHERE index_id = ?1",
            [index_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sqlite_error::storage)?;
    if from_sqlite_u64(entries, "physical global-index entry count")? != indexed_rows
        || from_sqlite_u64(reservations, "physical unique reservation count")? != unique_rows
    {
        return Err(corrupt(format!(
            "global index {} physical row counts do not match its checkpoints",
            index.id()
        )));
    }
    for checkpoint in checkpoints {
        let mut statement = connection
            .prepare(
                "SELECT encoded_key, source_locator
                 FROM briskdb_global_index_entries
                 WHERE index_id = ?1 AND source_shard = ?2
                 ORDER BY source_ordinal",
            )
            .map_err(sqlite_error::storage)?;
        let mut rows = statement
            .query(params![index_id, i64::from(checkpoint.source_shard)])
            .map_err(sqlite_error::storage)?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(SOURCE_DIGEST_DOMAIN);
        hasher.update(&index.id().get().to_le_bytes());
        hasher.update(&checkpoint.source_shard.to_le_bytes());
        let mut physical_rows = 0_u64;
        while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
            let key = row.get_ref(0).map_err(sqlite_error::storage)?;
            let locator = row.get_ref(1).map_err(sqlite_error::storage)?;
            let (ValueRef::Blob(key), ValueRef::Blob(locator)) = (key, locator) else {
                return Err(corrupt(format!(
                    "global index {} contains a non-blob physical entry",
                    index.id()
                )));
            };
            CanonicalIndexKey::from_bytes(key)?;
            update_framed(&mut hasher, key);
            update_framed(&mut hasher, locator);
            physical_rows = physical_rows.checked_add(1).ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::NumericOutOfRange,
                    "physical global-index row count overflowed",
                )
            })?;
        }
        if physical_rows != checkpoint.indexed_rows
            || hasher.finalize().as_bytes() != &checkpoint.source_digest
        {
            return Err(corrupt(format!(
                "global index {} physical entries do not match source-shard checkpoint {}",
                index.id(),
                checkpoint.source_shard
            )));
        }
    }
    validate_unique_reservations(connection, index)?;
    Ok(())
}

fn validate_unique_reservations(
    connection: &Connection,
    index: &GlobalIndexMetadata,
) -> EngineResult<()> {
    if !index.is_unique() {
        return Ok(());
    }
    let index_id = to_sqlite_id(index.id())?;
    let mut entry_statement = connection
        .prepare(
            "SELECT encoded_key, source_shard, source_locator
             FROM briskdb_global_index_entries
             WHERE index_id = ?1
             ORDER BY encoded_key, source_shard, source_locator",
        )
        .map_err(sqlite_error::storage)?;
    let mut reservation_statement = connection
        .prepare(
            "SELECT encoded_key, source_shard, source_locator
             FROM briskdb_global_index_unique_keys
             WHERE index_id = ?1 ORDER BY encoded_key",
        )
        .map_err(sqlite_error::storage)?;
    let mut entries = entry_statement
        .query([index_id])
        .map_err(sqlite_error::storage)?;
    let mut reservations = reservation_statement
        .query([index_id])
        .map_err(sqlite_error::storage)?;
    let mut previous_reserved_key: Option<Vec<u8>> = None;
    while let Some(entry) = entries.next().map_err(sqlite_error::storage)? {
        let key = entry.get::<_, Vec<u8>>(0).map_err(sqlite_error::storage)?;
        let canonical = CanonicalIndexKey::from_bytes(&key)?;
        let contains_null = canonical
            .decode()?
            .iter()
            .any(|part| matches!(part.value(), IndexKeyValue::Null));
        if index.null_semantics() == UniqueNullSemantics::Distinct && contains_null {
            continue;
        }
        if previous_reserved_key.as_deref() == Some(key.as_slice()) {
            return Err(corrupt(format!(
                "global index {} contains duplicate physically reserved keys",
                index.id()
            )));
        }
        previous_reserved_key = Some(key.clone());
        let source_shard = entry.get::<_, i64>(1).map_err(sqlite_error::storage)?;
        let source_locator = entry.get::<_, Vec<u8>>(2).map_err(sqlite_error::storage)?;
        let reservation = reservations
            .next()
            .map_err(sqlite_error::storage)?
            .ok_or_else(|| {
                corrupt(format!(
                    "global index {} is missing a unique reservation",
                    index.id()
                ))
            })?;
        if reservation
            .get::<_, Vec<u8>>(0)
            .map_err(sqlite_error::storage)?
            != key
            || reservation
                .get::<_, i64>(1)
                .map_err(sqlite_error::storage)?
                != source_shard
            || reservation
                .get::<_, Vec<u8>>(2)
                .map_err(sqlite_error::storage)?
                != source_locator
        {
            return Err(corrupt(format!(
                "global index {} unique reservation does not match its source entry",
                index.id()
            )));
        }
    }
    if reservations
        .next()
        .map_err(sqlite_error::storage)?
        .is_some()
    {
        return Err(corrupt(format!(
            "global index {} contains an extra unique reservation",
            index.id()
        )));
    }
    Ok(())
}

fn inspect_source_locator(connection: &Connection, table: &str) -> EngineResult<SourceLocator> {
    let without_rowid = connection
        .query_row(
            "SELECT wr FROM pragma_table_list
             WHERE schema = 'main' AND name = ?1 COLLATE BINARY AND type = 'table'",
            [table],
            |row| row.get::<_, bool>(0),
        )
        .optional()
        .map_err(sqlite_error::storage)?
        .ok_or_else(|| {
            corrupt(format!(
                "registered global-index source table {table} is missing"
            ))
        })?;
    let columns = {
        let mut statement = connection
            .prepare(
                "SELECT name, pk
                 FROM pragma_table_xinfo(?1)
                 ORDER BY CASE WHEN pk = 0 THEN 2147483647 ELSE pk END, cid",
            )
            .map_err(sqlite_error::storage)?;
        statement
            .query_map([table], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(sqlite_error::storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_error::storage)?
    };
    if without_rowid {
        let primary_key = columns
            .iter()
            .filter(|(_, ordinal)| *ordinal > 0)
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        if primary_key.is_empty() {
            return Err(corrupt(format!(
                "WITHOUT ROWID source table {table} has no primary-key locator"
            )));
        }
        return Ok(SourceLocator::PrimaryKey(primary_key));
    }
    ["rowid", "_rowid_", "oid"]
        .into_iter()
        .find(|candidate| {
            columns
                .iter()
                .all(|(column, _)| !column.eq_ignore_ascii_case(candidate))
        })
        .map(|candidate| SourceLocator::RowId(candidate.to_owned()))
        .ok_or_else(|| {
            corrupt(format!(
                "rowid source table {table} has no unshadowed physical row locator"
            ))
        })
}

fn scan_sql(index: &GlobalIndexMetadata, table: &str, locator: &SourceLocator) -> String {
    let mut expressions = index
        .key_parts()
        .iter()
        .map(|part| match part.source() {
            GlobalIndexKeySource::Column(column) => quote_identifier(column),
            GlobalIndexKeySource::Expression(expression) => format!("({expression})"),
        })
        .collect::<Vec<_>>();
    let locator_expressions = locator.expressions();
    expressions.extend(locator_expressions.iter().cloned());
    let mut sql = format!(
        "SELECT {} FROM {}",
        expressions.join(", "),
        quote_identifier(table)
    );
    if let Some(predicate) = index.predicate() {
        sql.push_str(" WHERE (");
        sql.push_str(predicate);
        sql.push(')');
    }
    sql.push_str(" ORDER BY ");
    sql.push_str(&locator_expressions.join(", "));
    sql
}

fn read_key_value(
    value: ValueRef<'_>,
    key_type: GlobalIndexKeyType,
    index: &GlobalIndexMetadata,
    shard: u16,
    ordinal: usize,
) -> EngineResult<IndexKeyValue> {
    if matches!(value, ValueRef::Null) {
        return Ok(IndexKeyValue::Null);
    }
    let mismatch = || {
        EngineError::new(
            EngineErrorKind::TypeMismatch,
            format!(
                "global index {} ({}) key part {} has an incompatible SQLite value on source shard {shard}",
                index.id(),
                index.name(),
                ordinal
            ),
        )
    };
    match (key_type, value) {
        (GlobalIndexKeyType::Boolean, ValueRef::Integer(0)) => Ok(IndexKeyValue::Boolean(false)),
        (GlobalIndexKeyType::Boolean, ValueRef::Integer(1)) => Ok(IndexKeyValue::Boolean(true)),
        (GlobalIndexKeyType::Int64, ValueRef::Integer(value)) => Ok(IndexKeyValue::Int64(value)),
        (GlobalIndexKeyType::UInt64, ValueRef::Integer(value)) => u64::try_from(value)
            .map(IndexKeyValue::UInt64)
            .map_err(|_| mismatch()),
        (GlobalIndexKeyType::Float64, ValueRef::Integer(value)) => {
            Ok(IndexKeyValue::Float64(value as f64))
        }
        (GlobalIndexKeyType::Float64, ValueRef::Real(value)) => Ok(IndexKeyValue::Float64(value)),
        (GlobalIndexKeyType::Date, ValueRef::Integer(value)) => i32::try_from(value)
            .map(IndexKeyValue::Date)
            .map_err(|_| mismatch()),
        (GlobalIndexKeyType::Timestamp, ValueRef::Integer(value)) => {
            Ok(IndexKeyValue::Timestamp(value))
        }
        (GlobalIndexKeyType::Text, ValueRef::Text(value)) => str::from_utf8(value)
            .map(|value| IndexKeyValue::Text(value.to_owned()))
            .map_err(|_| {
                EngineError::new(
                    EngineErrorKind::InvalidTextEncoding,
                    format!(
                        "global index {} ({}) key part {} contains invalid UTF-8 on source shard {shard}",
                        index.id(),
                        index.name(),
                        ordinal
                    ),
                )
            }),
        (GlobalIndexKeyType::Binary, ValueRef::Blob(value)) => {
            Ok(IndexKeyValue::Binary(value.to_vec()))
        }
        _ => Err(mismatch()),
    }
}

fn encode_locator(values: &[ValueRef<'_>]) -> EngineResult<Vec<u8>> {
    let count = u16::try_from(values.len()).map_err(|_| {
        EngineError::new(
            EngineErrorKind::LimitExceeded,
            "source row locator has too many components",
        )
    })?;
    let mut output = Vec::with_capacity(16 + values.len() * 9);
    output.extend_from_slice(LOCATOR_MAGIC);
    output.extend_from_slice(&LOCATOR_VERSION.to_be_bytes());
    output.extend_from_slice(&count.to_be_bytes());
    for value in values {
        match value {
            ValueRef::Null => output.push(0),
            ValueRef::Integer(value) => {
                output.push(1);
                output.extend_from_slice(&value.to_be_bytes());
            }
            ValueRef::Real(value) => {
                output.push(2);
                output.extend_from_slice(&value.to_bits().to_be_bytes());
            }
            ValueRef::Text(value) => {
                output.push(3);
                append_locator_bytes(&mut output, value)?;
            }
            ValueRef::Blob(value) => {
                output.push(4);
                append_locator_bytes(&mut output, value)?;
            }
        }
    }
    Ok(output)
}

fn append_locator_bytes(output: &mut Vec<u8>, value: &[u8]) -> EngineResult<()> {
    let length = u32::try_from(value.len()).map_err(|_| {
        EngineError::new(
            EngineErrorKind::LimitExceeded,
            "source row locator component exceeds the version-1 size limit",
        )
    })?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn definition_digest(index: &GlobalIndexMetadata) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DEFINITION_DIGEST_DOMAIN);
    hasher.update(&index.id().get().to_le_bytes());
    hasher.update(&index.table_id().get().to_le_bytes());
    update_framed(&mut hasher, index.name().as_bytes());
    hasher.update(&[u8::from(index.is_unique())]);
    hasher.update(&[match index.null_semantics() {
        UniqueNullSemantics::Distinct => 1,
        UniqueNullSemantics::NotDistinct => 2,
    }]);
    match index.predicate() {
        Some(predicate) => {
            hasher.update(&[1]);
            update_framed(&mut hasher, predicate.as_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
    hasher.update(&index.schema_generation().to_le_bytes());
    hasher.update(&index.key_encoding_version().to_le_bytes());
    let (topology_kind, topology_version, partition_count) = index.topology().persisted_parts();
    hasher.update(&topology_kind.to_le_bytes());
    hasher.update(&topology_version.to_le_bytes());
    hasher.update(&partition_count.to_le_bytes());
    hasher.update(&(index.key_parts().len() as u64).to_le_bytes());
    for part in index.key_parts() {
        hasher.update(&part.source().kind_code().to_le_bytes());
        update_framed(&mut hasher, part.source().source().as_bytes());
        hasher.update(&[key_type_code(part.key_type())]);
        hasher.update(&[match part.order() {
            IndexKeyOrder::Ascending => 1,
            IndexKeyOrder::Descending => 2,
        }]);
        hasher.update(&[match part.null_order() {
            crate::core::IndexNullOrder::First => 1,
            crate::core::IndexNullOrder::Last => 2,
        }]);
        hasher.update(&[match part.collation() {
            crate::core::IndexKeyCollation::Binary => 1,
        }]);
    }
    *hasher.finalize().as_bytes()
}

fn key_type_code(key_type: GlobalIndexKeyType) -> u8 {
    match key_type {
        GlobalIndexKeyType::Boolean => 1,
        GlobalIndexKeyType::Int64 => 2,
        GlobalIndexKeyType::UInt64 => 3,
        GlobalIndexKeyType::Float64 => 4,
        GlobalIndexKeyType::Date => 5,
        GlobalIndexKeyType::Timestamp => 6,
        GlobalIndexKeyType::Text => 7,
        GlobalIndexKeyType::Binary => 8,
    }
}

fn update_framed(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

fn open_or_create(root: &Path) -> EngineResult<(Connection, PathBuf)> {
    let directory = root.join(DIRECTORY_NAME);
    ensure_real_directory(&directory)?;
    let path = directory.join(SHARED_FILE_NAME);
    let exists = ensure_regular_file_or_absent(&path)?;
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | if exists {
            OpenFlags::empty()
        } else {
            OpenFlags::SQLITE_OPEN_CREATE
        };
    let connection = Connection::open_with_flags(&path, flags).map_err(sqlite_error::storage)?;
    configure(&connection)?;
    if exists {
        validate(&connection)?;
    } else {
        initialize(&connection)?;
        sync_directory(&directory)?;
    }
    Ok((connection, path))
}

fn open_existing(root: &Path) -> EngineResult<Option<(Connection, PathBuf)>> {
    let directory = root.join(DIRECTORY_NAME);
    match fs::symlink_metadata(&directory) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(sqlite_error::storage_io(
                error,
                format!("failed to inspect {}", directory.display()),
            ));
        }
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "global-index path {} is not a real directory",
                    directory.display()
                ),
            ));
        }
        Ok(_) => {}
    }
    let path = directory.join(SHARED_FILE_NAME);
    if !ensure_regular_file_or_absent(&path)? {
        return Ok(None);
    }
    let connection = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .map_err(sqlite_error::storage)?;
    configure(&connection)?;
    validate(&connection)?;
    Ok(Some((connection, path)))
}

fn configure(connection: &Connection) -> EngineResult<()> {
    connection
        .busy_timeout(CONNECTION_BUSY_TIMEOUT)
        .map_err(sqlite_error::storage)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(sqlite_error::storage)?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(sqlite_error::storage)?;
    Ok(())
}

fn initialize(connection: &Connection) -> EngineResult<()> {
    connection
        .pragma_update(None, "application_id", APPLICATION_ID)
        .map_err(sqlite_error::storage)?;
    connection
        .pragma_update(None, "user_version", STORAGE_VERSION)
        .map_err(sqlite_error::storage)?;
    let mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .map_err(sqlite_error::storage)?;
    if !mode.eq_ignore_ascii_case("wal") {
        let mode: String = connection
            .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))
            .map_err(sqlite_error::storage)?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(EngineError::new(
                EngineErrorKind::StorageUnavailable,
                "global-index storage did not enter WAL mode",
            ));
        }
    }
    connection
        .execute_batch(SCHEMA_SQL)
        .map_err(sqlite_error::storage)?;
    validate(connection)
}

fn validate(connection: &Connection) -> EngineResult<()> {
    let application_id: i32 = connection
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .map_err(sqlite_error::storage)?;
    let storage_version: u32 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(sqlite_error::storage)?;
    if application_id != APPLICATION_ID {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "global-index SQLite file has a foreign application identity",
        ));
    }
    if storage_version != STORAGE_VERSION {
        return Err(EngineError::new(
            if storage_version > STORAGE_VERSION {
                EngineErrorKind::FailedPrecondition
            } else {
                EngineErrorKind::DataCorruption
            },
            format!(
                "global-index storage version {storage_version} is not supported by this build"
            ),
        ));
    }
    let mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .map_err(sqlite_error::storage)?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(corrupt("global-index storage is not in WAL mode"));
    }
    let quick_check: String = connection
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .map_err(sqlite_error::storage)?;
    if quick_check != "ok" {
        return Err(corrupt("global-index storage failed SQLite quick_check"));
    }
    let objects = {
        let mut statement = connection
            .prepare(
                "SELECT name FROM sqlite_schema
                 WHERE name NOT GLOB 'sqlite_*'
                 ORDER BY name COLLATE BINARY",
            )
            .map_err(sqlite_error::storage)?;
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sqlite_error::storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_error::storage)?
    };
    if objects != EXPECTED_OBJECTS {
        return Err(corrupt("global-index storage schema is not canonical"));
    }
    let metadata = connection
        .query_row(
            "SELECT storage_version, key_encoding_version
             FROM briskdb_global_index_storage WHERE singleton = 1",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(sqlite_error::storage)?;
    if metadata
        != Some((
            i64::from(STORAGE_VERSION),
            i64::from(INDEX_KEY_ENCODING_VERSION),
        ))
    {
        return Err(corrupt("global-index storage metadata is invalid"));
    }
    Ok(())
}

fn ensure_real_directory(path: &Path) -> EngineResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "global-index path {} is not a real directory",
                    path.display()
                ),
            ));
        }
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(sqlite_error::storage_io(
                error,
                format!("failed to inspect {}", path.display()),
            ));
        }
    }
    fs::create_dir(path).map_err(|error| {
        sqlite_error::storage_io(error, format!("failed to create {}", path.display()))
    })?;
    Ok(())
}

fn ensure_regular_file_or_absent(path: &Path) -> EngineResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "global-index storage {} is not a regular file",
                    path.display()
                ),
            ))
        }
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(sqlite_error::storage_io(
            error,
            format!("failed to inspect {}", path.display()),
        )),
    }
}

fn checkpoint_and_sync(connection: &Connection, path: &Path) -> EngineResult<()> {
    let (_, remaining): (i64, i64) = connection
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .map_err(sqlite_error::storage)?;
    if remaining != 0 {
        return Err(EngineError::new(
            EngineErrorKind::Busy,
            "global-index WAL could not be fully checkpointed before publication",
        ));
    }
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| {
            sqlite_error::storage_io(error, format!("failed to synchronize {}", path.display()))
        })?;
    sync_directory(path.parent().expect("global-index file has a parent"))
}

fn sync_directory(path: &Path) -> EngineResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            sqlite_error::storage_io(error, format!("failed to synchronize {}", path.display()))
        })
}

fn ensure_not_cancelled(cancellation: &CancellationToken, context: &str) -> EngineResult<()> {
    if cancellation.is_cancelled() {
        Err(EngineError::new(
            EngineErrorKind::Cancelled,
            format!("global-index build was cancelled {context}"),
        ))
    } else {
        Ok(())
    }
}

fn to_sqlite_id(index_id: GlobalIndexId) -> EngineResult<i64> {
    i64::try_from(index_id.get()).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::NumericOutOfRange,
            "global-index ID does not fit in SQLite",
            error,
        )
    })
}

fn to_sqlite_u64(value: u64, description: &str) -> EngineResult<i64> {
    i64::try_from(value).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::NumericOutOfRange,
            format!("{description} does not fit in SQLite"),
            error,
        )
    })
}

fn from_sqlite_u64(value: i64, description: &str) -> EngineResult<u64> {
    u64::try_from(value).map_err(|_| corrupt(format!("{description} is negative")))
}

fn locator_label(locator: &[u8]) -> String {
    let digest = blake3::hash(locator);
    digest.as_bytes()[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn corrupt(diagnostic: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::DataCorruption, diagnostic)
}

#[cfg(test)]
fn abort_at_test_boundary(boundary: &str) {
    if std::env::var("BRISKDB_GLOBAL_INDEX_BUILD_ABORT_POINT").as_deref() == Ok(boundary) {
        std::process::abort();
    }
}

#[cfg(not(test))]
fn abort_at_test_boundary(_boundary: &str) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_metadata() -> GlobalIndexMetadata {
        GlobalIndexMetadata::from_validated(
            1,
            1,
            "events_email".to_owned(),
            vec![crate::core::GlobalIndexKeyPart::new(
                GlobalIndexKeySource::column("email").unwrap(),
                GlobalIndexKeyType::Text,
            )]
            .into_boxed_slice(),
            true,
            UniqueNullSemantics::Distinct,
            None,
            GlobalIndexLifecycle::Creating,
            1,
            GlobalIndexStorageTopology::SharedSqliteV1,
        )
    }

    #[test]
    fn source_locator_encoding_is_typed_framed_and_stable() {
        let encoded = encode_locator(&[
            ValueRef::Integer(-1),
            ValueRef::Text(b"a"),
            ValueRef::Blob(&[0, 255]),
        ])
        .unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(b"BRIL");
        expected.extend_from_slice(&1_u32.to_be_bytes());
        expected.extend_from_slice(&3_u16.to_be_bytes());
        expected.push(1);
        expected.extend_from_slice(&(-1_i64).to_be_bytes());
        expected.push(3);
        expected.extend_from_slice(&1_u32.to_be_bytes());
        expected.push(b'a');
        expected.push(4);
        expected.extend_from_slice(&2_u32.to_be_bytes());
        expected.extend_from_slice(&[0, 255]);
        assert_eq!(encoded, expected);
    }

    #[test]
    fn checkpoints_must_form_one_contiguous_source_shard_prefix() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(SCHEMA_SQL).unwrap();
        connection
            .execute(
                "INSERT INTO briskdb_global_index_builds VALUES (1, ?1, 0, 4, 1, 0)",
                [&[0_u8; 32][..]],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO briskdb_global_index_checkpoints
                 VALUES (1, 1, ?1, 0, 0)",
                [&[0_u8; 32][..]],
            )
            .unwrap();
        let error = load_checkpoints(&connection, GlobalIndexId::new(1).unwrap(), 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert!(error.diagnostic().contains("contiguous shard prefix"));
    }

    #[test]
    fn duplicate_reservations_report_both_redacted_source_locations() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(SCHEMA_SQL).unwrap();
        connection
            .execute(
                "INSERT INTO briskdb_global_index_builds VALUES (1, ?1, 1, 2, 1, 0)",
                [&[0_u8; 32][..]],
            )
            .unwrap();
        let index = unique_metadata();
        let value = IndexKeyValue::Text("duplicate@example.test".to_owned());
        let parts = [IndexKeyPart::ascending(value.as_ref())];
        let key = CanonicalIndexKey::encode(&parts).unwrap();
        let first_locator = encode_locator(&[ValueRef::Integer(1)]).unwrap();
        let second_locator = encode_locator(&[ValueRef::Integer(2)]).unwrap();
        let transaction = connection.transaction().unwrap();
        insert_entry(&transaction, &index, 0, 0, &key, &first_locator, &parts).unwrap();
        let error =
            insert_entry(&transaction, &index, 1, 0, &key, &second_locator, &parts).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::UniqueViolation);
        assert!(error.diagnostic().contains("shard 0"));
        assert!(error.diagnostic().contains("shard 1"));
        assert!(error.diagnostic().contains("key bytes are redacted"));
        assert!(!error.diagnostic().contains("duplicate@example.test"));
    }
}
