//! Exact-value row streaming and independent staged-layout verification.

use std::{borrow::Cow, str};

use rusqlite::{
    Connection, ToSql, params_from_iter,
    types::{ToSqlOutput, ValueRef},
};

use crate::{
    core::{
        CancellationToken, CanonicalShardKeyRef, EngineError, EngineErrorKind, EngineResult,
        ShardKeyType, TablePlacement, canonical_shard_key_bytes,
    },
    sqlite_error,
    storage::Storage,
};

use super::{
    ImportFault, SqliteImportKeyType, SqliteImportPlacement, SqliteImportTableReport, hex_digest,
    schema::{SourceSequence, SourceSnapshot, SourceTable},
};

const PROGRESS_HANDLER_OPS: i32 = 1_000;
const ROW_DIGEST_DOMAIN: &[u8] = b"briskdb.sqlite-import.row.v1\0";
const TABLE_DIGEST_DOMAIN: &[u8] = b"briskdb.sqlite-import.table-multiset.v1\0";

/// Stream every source row into its declared physical owner(s), commit the
/// staging-only transactions, and independently verify the complete result.
///
/// The caller still owns the staging directory. Returning success means the
/// files are ready for the caller's final pre-publication cancellation check;
/// it does not publish or rename anything itself.
pub(super) fn copy_and_verify(
    source: &SourceSnapshot,
    storage: &Storage,
    cancellation: &CancellationToken,
    fault: ImportFault,
) -> EngineResult<Vec<SqliteImportTableReport>> {
    ensure_not_cancelled(cancellation)?;
    let targets = (0..storage.shard_count())
        .map(|shard| storage.open_shard(shard))
        .collect::<EngineResult<Vec<_>>>()?;
    copy_and_verify_on_connections(source, storage, &targets, cancellation, fault)
}

/// Final sticky cancellation observation used immediately before publication.
pub(super) fn ensure_not_cancelled(cancellation: &CancellationToken) -> EngineResult<()> {
    if cancellation.is_cancelled() {
        Err(EngineError::new(
            EngineErrorKind::Cancelled,
            "SQLite import was cancelled before publication",
        ))
    } else {
        Ok(())
    }
}

fn copy_and_verify_on_connections(
    source: &SourceSnapshot,
    storage: &Storage,
    targets: &[Connection],
    cancellation: &CancellationToken,
    fault: ImportFault,
) -> EngineResult<Vec<SqliteImportTableReport>> {
    if targets.len() != usize::from(storage.shard_count()) {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "SQLite import target connection count disagrees with the persisted shard layout",
        ));
    }
    ensure_not_cancelled(cancellation)?;

    let _source_progress = ProgressHandlerReset::install(source.connection(), cancellation)?;
    let _target_progress = targets
        .iter()
        .map(|connection| ProgressHandlerReset::install(connection, cancellation))
        .collect::<EngineResult<Vec<_>>>()?;

    begin_target_transactions(targets)?;
    let copied = (|| {
        let expectations = copy_tables(source, storage, targets, cancellation)?;
        restore_sequences(source.sequences(), targets, cancellation)?;
        commit_target_transactions(targets, cancellation, fault)?;
        Ok(expectations)
    })();
    let expectations = match copied {
        Ok(expectations) => expectations,
        Err(error) => {
            rollback_target_transactions(targets);
            return Err(error);
        }
    };

    ensure_not_cancelled(cancellation)?;
    let reports = verify_tables(source, storage, targets, &expectations, cancellation)?;
    verify_sequences(source.sequences(), targets, cancellation)?;
    for (shard, connection) in targets.iter().enumerate() {
        ensure_not_cancelled(cancellation)?;
        verify_quick_check(connection, shard)?;
        verify_foreign_keys(connection, shard)?;
    }
    ensure_not_cancelled(cancellation)?;
    Ok(reports)
}

struct ProgressHandlerReset<'a> {
    connection: &'a Connection,
}

impl<'a> ProgressHandlerReset<'a> {
    fn install(connection: &'a Connection, cancellation: &CancellationToken) -> EngineResult<Self> {
        let cancellation = cancellation.clone();
        connection
            .progress_handler(
                PROGRESS_HANDLER_OPS,
                Some(move || cancellation.is_cancelled()),
            )
            .map_err(sqlite_error::storage)?;
        Ok(Self { connection })
    }
}

impl Drop for ProgressHandlerReset<'_> {
    fn drop(&mut self) {
        let _ = self.connection.progress_handler(0, None::<fn() -> bool>);
    }
}

fn begin_target_transactions(targets: &[Connection]) -> EngineResult<()> {
    for (shard, connection) in targets.iter().enumerate() {
        if let Err(error) = connection.execute_batch("BEGIN IMMEDIATE") {
            rollback_target_transactions(&targets[..shard]);
            return Err(sqlite_error::storage(error).context(format!(
                "failed to begin SQLite import transaction on physical shard {shard}"
            )));
        }
    }
    Ok(())
}

fn commit_target_transactions(
    targets: &[Connection],
    cancellation: &CancellationToken,
    fault: ImportFault,
) -> EngineResult<()> {
    for (shard, connection) in targets.iter().enumerate() {
        ensure_not_cancelled(cancellation)?;
        connection.execute_batch("COMMIT").map_err(|error| {
            sqlite_error::storage(error).context(format!(
                "failed to commit SQLite import transaction on physical shard {shard}"
            ))
        })?;
        fault.after_shard_commit(shard + 1)?;
    }
    Ok(())
}

fn rollback_target_transactions(targets: &[Connection]) {
    for connection in targets {
        let _ = connection.execute_batch("ROLLBACK");
    }
}

#[derive(Debug, Clone)]
struct TableExpectation {
    source_rows: u64,
    digest: [u8; 32],
    projection: TableProjection,
}

#[derive(Debug, Clone)]
struct TableProjection {
    rowid_alias: Option<String>,
    writable: Vec<WritableColumn>,
    column_count: usize,
    shard_key_index: Option<usize>,
}

#[derive(Debug, Clone)]
struct WritableColumn {
    value_index: usize,
    name: String,
}

fn copy_tables(
    source: &SourceSnapshot,
    storage: &Storage,
    targets: &[Connection],
    cancellation: &CancellationToken,
) -> EngineResult<Vec<TableExpectation>> {
    let mut expectations = Vec::with_capacity(source.tables().len());
    for table in source.tables() {
        ensure_not_cancelled(cancellation)?;
        validate_committed_placement(storage, table)?;
        expectations.push(copy_table(
            source.connection(),
            table,
            storage,
            targets,
            cancellation,
        )?);
    }
    Ok(expectations)
}

fn validate_committed_placement(storage: &Storage, table: &SourceTable) -> EngineResult<()> {
    let catalog = storage.logical_catalog();
    let database = catalog.default_database().name();
    let committed = catalog.table(database, table.name())?.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::Internal,
            format!(
                "SQLite import source table {} is absent from the committed catalog",
                table.name()
            ),
        )
    })?;
    let matches = match (table.placement(), committed.placement()) {
        (SqliteImportPlacement::Global, TablePlacement::Global) => table.shard_key().is_none(),
        (SqliteImportPlacement::Sharded { .. }, TablePlacement::Sharded(committed_key)) => {
            table.shard_key().is_some_and(|source_key| {
                source_key.column() == committed_key.column()
                    && import_key_type(source_key.key_type()) == committed_key.key_type()
            })
        }
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::Internal,
            format!(
                "SQLite import source table {} disagrees with its committed placement metadata",
                table.name()
            ),
        ))
    }
}

const fn import_key_type(key_type: SqliteImportKeyType) -> ShardKeyType {
    match key_type {
        SqliteImportKeyType::Int64 => ShardKeyType::Int64,
        SqliteImportKeyType::Text => ShardKeyType::Text,
        SqliteImportKeyType::Binary => ShardKeyType::Binary,
    }
}

fn copy_table(
    source: &Connection,
    table: &SourceTable,
    storage: &Storage,
    targets: &[Connection],
    cancellation: &CancellationToken,
) -> EngineResult<TableExpectation> {
    let projection = table_projection(table)?;
    if projection.writable.is_empty() {
        return Err(EngineError::new(
            EngineErrorKind::Unsupported,
            format!(
                "SQLite import table {} has no writable ordinary columns",
                table.name()
            ),
        ));
    }

    let select_sql = select_all_columns_sql(table, &projection);
    let insert_sql = insert_writable_columns_sql(table, &projection.writable);
    let mut insert_statements = targets
        .iter()
        .enumerate()
        .map(|(shard, target)| {
            target.prepare_cached(&insert_sql).map_err(|error| {
                sqlite_error::storage(error).context(format!(
                    "failed to prepare SQLite import INSERT for table {} on physical shard {shard}",
                    table.name()
                ))
            })
        })
        .collect::<EngineResult<Vec<_>>>()?;
    let mut statement = source.prepare(&select_sql).map_err(|error| {
        sqlite_error::storage(error).context(format!(
            "failed to prepare source row scan for SQLite import table {}",
            table.name()
        ))
    })?;
    let mut rows = statement.query([]).map_err(|error| {
        sqlite_error::storage(error).context(format!(
            "failed to start source row scan for SQLite import table {}",
            table.name()
        ))
    })?;
    let mut accumulator = MultisetAccumulator::default();
    let mut source_rows = 0_u64;

    while let Some(row) = rows.next().map_err(|error| {
        sqlite_error::storage(error).context(format!(
            "failed to read source rows for SQLite import table {}",
            table.name()
        ))
    })? {
        ensure_not_cancelled(cancellation)?;
        source_rows = source_rows.checked_add(1).ok_or_else(row_count_overflow)?;
        let values = read_raw_row(row, projection.column_count).map_err(|error| {
            error.context(format!(
                "failed to decode source row {source_rows} for SQLite import table {}",
                table.name()
            ))
        })?;
        accumulator.add(&values)?;

        match table.placement() {
            SqliteImportPlacement::Sharded { .. } => {
                let shard_key = table.shard_key().ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::Internal,
                        "resolved Sharded import table has no shard key",
                    )
                })?;
                let key_index = projection.shard_key_index.ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::Internal,
                        "resolved Sharded import projection has no shard-key index",
                    )
                })?;
                let key = values.get(key_index).ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::Internal,
                        "resolved SQLite import shard-key index is outside its source row",
                    )
                })?;
                let key_bytes =
                    canonical_key_for_import(key, shard_key.key_type(), table.name(), source_rows)?;
                let shard = storage.shard_for_key(key_bytes.as_ref());
                insert_row(
                    &mut insert_statements[usize::from(shard)],
                    &values,
                    &projection.writable,
                    table.name(),
                    source_rows,
                    shard,
                )?;
            }
            SqliteImportPlacement::Global => {
                for (shard, insert) in insert_statements.iter_mut().enumerate() {
                    ensure_not_cancelled(cancellation)?;
                    insert_row(
                        insert,
                        &values,
                        &projection.writable,
                        table.name(),
                        source_rows,
                        u16::try_from(shard).expect("validated shard indexes fit in u16"),
                    )?;
                }
            }
        }
    }

    if source_rows != table.source_rows() {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!(
                "SQLite import source row count changed for table {}; expected {}, scanned {source_rows}",
                table.name(),
                table.source_rows()
            ),
        ));
    }
    Ok(TableExpectation {
        source_rows,
        digest: accumulator.finish(table.name(), projection.column_count)?,
        projection,
    })
}

fn table_projection(table: &SourceTable) -> EngineResult<TableProjection> {
    let rowid_alias = table.rowid_projection().map(str::to_owned);
    let offset = usize::from(rowid_alias.is_some());
    let mut writable = Vec::with_capacity(
        table
            .columns()
            .iter()
            .filter(|column| column.writable())
            .count()
            + offset,
    );
    if let Some(alias) = rowid_alias.as_deref() {
        writable.push(WritableColumn {
            value_index: 0,
            name: alias.to_owned(),
        });
    }
    writable.extend(
        table
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, column)| column.writable())
            .map(|(index, column)| WritableColumn {
                value_index: index + offset,
                name: column.name().to_owned(),
            }),
    );
    let shard_key_index = table
        .shard_key()
        .map(|key| {
            let writable_index = key
                .column_index()
                .checked_add(offset)
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::LimitExceeded,
                        "SQLite import shard-key projection index overflowed",
                    )
                })?;
            writable
                .get(writable_index)
                .filter(|column| column.name == key.column())
                .map(|column| column.value_index)
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::Internal,
                        format!(
                            "resolved shard key {} disagrees with the writable projection for SQLite import table {}",
                            key.column(),
                            table.name()
                        ),
                    )
                })
        })
        .transpose()?;
    Ok(TableProjection {
        rowid_alias,
        writable,
        column_count: table.columns().len().checked_add(offset).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                "SQLite import projection contains too many columns",
            )
        })?,
        shard_key_index,
    })
}

fn insert_row(
    statement: &mut rusqlite::CachedStatement<'_>,
    values: &[RawSqlValue],
    writable: &[WritableColumn],
    table: &str,
    source_row: u64,
    shard: u16,
) -> EngineResult<()> {
    let parameters = writable.iter().map(|column| &values[column.value_index]);
    let changed = statement
        .execute(params_from_iter(parameters))
        .map_err(|error| {
            sqlite_error::statement(error).context(format!(
                "failed to copy source row {source_row} from SQLite import table {table} to physical shard {shard}"
            ))
        })?;
    if changed == 1 {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::Internal,
            format!(
                "SQLite import INSERT for table {table} changed {changed} rows on physical shard {shard}"
            ),
        ))
    }
}

fn canonical_key_for_import<'a>(
    value: &'a RawSqlValue,
    key_type: SqliteImportKeyType,
    table: &str,
    row: u64,
) -> EngineResult<Cow<'a, [u8]>> {
    match (key_type, value) {
        (_, RawSqlValue::Null) => Err(EngineError::new(
            EngineErrorKind::NotNullViolation,
            format!("SQLite import table {table} row {row} has a NULL shard key"),
        )),
        (SqliteImportKeyType::Int64, RawSqlValue::Integer(value)) => Ok(canonical_shard_key_bytes(
            CanonicalShardKeyRef::Int64(*value),
        )),
        (SqliteImportKeyType::Text, RawSqlValue::Text(value)) => {
            let value = str::from_utf8(value).map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::InvalidTextEncoding,
                    format!("SQLite import table {table} row {row} has a non-UTF-8 text shard key"),
                    error,
                )
            })?;
            Ok(canonical_shard_key_bytes(CanonicalShardKeyRef::Text(value)))
        }
        (SqliteImportKeyType::Binary, RawSqlValue::Blob(value)) => Ok(canonical_shard_key_bytes(
            CanonicalShardKeyRef::Binary(value),
        )),
        _ => Err(EngineError::new(
            EngineErrorKind::TypeMismatch,
            format!(
                "SQLite import table {table} row {row} shard key has the wrong SQLite storage class"
            ),
        )),
    }
}

fn restore_sequences(
    sequences: &[SourceSequence],
    targets: &[Connection],
    cancellation: &CancellationToken,
) -> EngineResult<()> {
    for (shard, connection) in targets.iter().enumerate() {
        ensure_not_cancelled(cancellation)?;
        let exists = sqlite_sequence_exists(connection)?;
        if !exists {
            if sequences.is_empty() {
                continue;
            }
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                format!(
                    "physical shard {shard} has no sqlite_sequence table required by the source schema"
                ),
            ));
        }
        connection
            .execute("DELETE FROM main.sqlite_sequence", [])
            .map_err(sqlite_error::storage)?;
        for sequence in sequences {
            ensure_not_cancelled(cancellation)?;
            connection
                .execute(
                    "INSERT INTO main.sqlite_sequence(name, seq) VALUES (?1, ?2)",
                    (sequence.table(), sequence.seq()),
                )
                .map_err(|error| {
                    sqlite_error::storage(error).context(format!(
                        "failed to restore sqlite_sequence on physical shard {shard}"
                    ))
                })?;
        }
    }
    Ok(())
}

fn verify_tables(
    source: &SourceSnapshot,
    storage: &Storage,
    targets: &[Connection],
    expectations: &[TableExpectation],
    cancellation: &CancellationToken,
) -> EngineResult<Vec<SqliteImportTableReport>> {
    if source.tables().len() != expectations.len() {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "SQLite import verification lost a source table expectation",
        ));
    }
    let mut reports = Vec::with_capacity(source.tables().len());
    for (table, expected) in source.tables().iter().zip(expectations) {
        ensure_not_cancelled(cancellation)?;
        let mut physical_rows = Vec::with_capacity(targets.len());
        let mut shard_accumulators = Vec::with_capacity(targets.len());
        for (shard, connection) in targets.iter().enumerate() {
            let shard = u16::try_from(shard).expect("validated shard indexes fit in u16");
            let accumulator = scan_target_table(
                connection,
                table,
                &expected.projection,
                shard,
                storage,
                cancellation,
            )?;
            physical_rows.push(accumulator.rows());
            shard_accumulators.push(accumulator);
        }

        match table.placement() {
            SqliteImportPlacement::Sharded { .. } => {
                let mut combined = MultisetAccumulator::default();
                for accumulator in &shard_accumulators {
                    combined.merge(accumulator)?;
                }
                if combined.rows() != expected.source_rows
                    || combined.finish(table.name(), expected.projection.column_count)?
                        != expected.digest
                {
                    return Err(EngineError::new(
                        EngineErrorKind::DataCorruption,
                        format!(
                            "SQLite import verification found missing, duplicate, or changed values in Sharded table {}",
                            table.name()
                        ),
                    ));
                }
            }
            SqliteImportPlacement::Global => {
                for (shard, accumulator) in shard_accumulators.iter().enumerate() {
                    if accumulator.rows() != expected.source_rows
                        || accumulator.finish(table.name(), expected.projection.column_count)?
                            != expected.digest
                    {
                        return Err(EngineError::new(
                            EngineErrorKind::DataCorruption,
                            format!(
                                "SQLite import verification found missing, duplicate, or changed values in Global table {} on physical shard {shard}",
                                table.name()
                            ),
                        ));
                    }
                }
            }
        }

        reports.push(SqliteImportTableReport {
            table: table.name().to_owned(),
            placement: table.placement().clone(),
            source_rows: expected.source_rows,
            physical_rows,
            logical_contents_blake3: hex_digest(expected.digest),
            sqlite_sequence: source
                .sequences()
                .iter()
                .find(|sequence| sequence.table() == table.name())
                .map(SourceSequence::seq),
        });
    }
    Ok(reports)
}

fn scan_target_table(
    connection: &Connection,
    table: &SourceTable,
    projection: &TableProjection,
    shard: u16,
    storage: &Storage,
    cancellation: &CancellationToken,
) -> EngineResult<MultisetAccumulator> {
    let sql = select_all_columns_sql(table, projection);
    let mut statement = connection.prepare(&sql).map_err(|error| {
        sqlite_error::storage(error).context(format!(
            "failed to prepare verification scan for table {} on physical shard {shard}",
            table.name()
        ))
    })?;
    let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
    let mut accumulator = MultisetAccumulator::default();
    let mut physical_row = 0_u64;
    while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
        ensure_not_cancelled(cancellation)?;
        physical_row = physical_row.checked_add(1).ok_or_else(row_count_overflow)?;
        let values = read_raw_row(row, projection.column_count)?;
        accumulator.add(&values)?;
        if matches!(table.placement(), SqliteImportPlacement::Sharded { .. }) {
            let shard_key = table.shard_key().ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    "resolved Sharded import table has no shard key during verification",
                )
            })?;
            let key_index = projection.shard_key_index.ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    "resolved Sharded import projection has no shard-key index during verification",
                )
            })?;
            let key = values.get(key_index).ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    "resolved SQLite import shard-key index is outside its target row",
                )
            })?;
            let key_bytes =
                canonical_key_for_import(key, shard_key.key_type(), table.name(), physical_row)?;
            if storage.shard_for_key(key_bytes.as_ref()) != shard {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!(
                        "SQLite import verification found a row of table {} on the wrong physical shard {shard}",
                        table.name()
                    ),
                ));
            }
        }
    }
    Ok(accumulator)
}

fn verify_sequences(
    source: &[SourceSequence],
    targets: &[Connection],
    cancellation: &CancellationToken,
) -> EngineResult<()> {
    let mut expected = source
        .iter()
        .map(|sequence| (sequence.table().to_owned(), sequence.seq()))
        .collect::<Vec<_>>();
    expected.sort();
    for (shard, connection) in targets.iter().enumerate() {
        ensure_not_cancelled(cancellation)?;
        if !sqlite_sequence_exists(connection)? {
            if expected.is_empty() {
                continue;
            }
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("physical shard {shard} lost its required sqlite_sequence table"),
            ));
        }
        let mut statement = connection
            .prepare("SELECT name, seq FROM main.sqlite_sequence ORDER BY name COLLATE BINARY, seq")
            .map_err(sqlite_error::storage)?;
        let observed = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(sqlite_error::storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_error::storage)?;
        if observed != expected {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("sqlite_sequence differs on physical shard {shard}"),
            ));
        }
    }
    Ok(())
}

fn sqlite_sequence_exists(connection: &Connection) -> EngineResult<bool> {
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM main.sqlite_schema
                 WHERE type = 'table' AND name = 'sqlite_sequence'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(sqlite_error::storage)
}

fn verify_quick_check(connection: &Connection, shard: usize) -> EngineResult<()> {
    let mut statement = connection
        .prepare("PRAGMA main.quick_check")
        .map_err(sqlite_error::storage)?;
    let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
    let first = rows
        .next()
        .map_err(sqlite_error::storage)?
        .map(|row| row.get::<_, String>(0))
        .transpose()
        .map_err(sqlite_error::storage)?;
    let additional = rows.next().map_err(sqlite_error::storage)?.is_some();
    if first.as_deref() == Some("ok") && !additional {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            format!("SQLite quick_check failed on imported physical shard {shard}"),
        ))
    }
}

fn verify_foreign_keys(connection: &Connection, shard: usize) -> EngineResult<()> {
    let mut statement = connection
        .prepare("PRAGMA main.foreign_key_check")
        .map_err(sqlite_error::storage)?;
    let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
    if rows.next().map_err(sqlite_error::storage)?.is_none() {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::ForeignKeyViolation,
            format!("foreign-key check failed on imported physical shard {shard}"),
        ))
    }
}

fn select_all_columns_sql(table: &SourceTable, projection: &TableProjection) -> String {
    let mut columns = Vec::with_capacity(projection.column_count);
    if let Some(alias) = projection.rowid_alias.as_deref() {
        columns.push(quote_identifier(alias));
    }
    columns.extend(
        table
            .columns()
            .iter()
            .map(|column| quote_identifier(column.name())),
    );
    let columns = columns.join(", ");
    format!("SELECT {columns} FROM {}", quote_identifier(table.name()))
}

fn insert_writable_columns_sql(table: &SourceTable, writable: &[WritableColumn]) -> String {
    let columns = writable
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let parameters = (1..=writable.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({columns}) VALUES ({parameters})",
        quote_identifier(table.name())
    )
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[derive(Debug, Clone, PartialEq)]
enum RawSqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl RawSqlValue {
    fn from_ref(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(value) => Self::Integer(value),
            ValueRef::Real(value) => Self::Real(value),
            ValueRef::Text(value) => Self::Text(value.to_vec()),
            ValueRef::Blob(value) => Self::Blob(value.to_vec()),
        }
    }

    fn hash_into(&self, hasher: &mut blake3::Hasher) -> EngineResult<()> {
        match self {
            Self::Null => hasher.update(&[0]),
            Self::Integer(value) => {
                hasher.update(&[1]);
                hasher.update(&value.to_le_bytes())
            }
            Self::Real(value) => {
                hasher.update(&[2]);
                hasher.update(&value.to_bits().to_le_bytes())
            }
            Self::Text(value) => {
                hasher.update(&[3]);
                hash_length(hasher, value.len())?;
                hasher.update(value)
            }
            Self::Blob(value) => {
                hasher.update(&[4]);
                hash_length(hasher, value.len())?;
                hasher.update(value)
            }
        };
        Ok(())
    }
}

impl ToSql for RawSqlValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Integer(value) => ValueRef::Integer(*value),
            Self::Real(value) => ValueRef::Real(*value),
            Self::Text(value) => ValueRef::Text(value),
            Self::Blob(value) => ValueRef::Blob(value),
        }))
    }
}

fn read_raw_row(row: &rusqlite::Row<'_>, column_count: usize) -> EngineResult<Vec<RawSqlValue>> {
    (0..column_count)
        .map(|index| {
            row.get_ref(index)
                .map(RawSqlValue::from_ref)
                .map_err(sqlite_error::storage)
        })
        .collect()
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct MultisetAccumulator {
    rows: u64,
    lanes: [u64; 4],
}

impl MultisetAccumulator {
    fn rows(&self) -> u64 {
        self.rows
    }

    fn add(&mut self, values: &[RawSqlValue]) -> EngineResult<()> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(ROW_DIGEST_DOMAIN);
        hash_length(&mut hasher, values.len())?;
        for value in values {
            value.hash_into(&mut hasher)?;
        }
        let digest = hasher.finalize();
        for (lane, chunk) in self.lanes.iter_mut().zip(digest.as_bytes().chunks_exact(8)) {
            let bytes: [u8; 8] = chunk
                .try_into()
                .expect("BLAKE3 digest chunks contain exactly eight bytes");
            *lane = lane.wrapping_add(u64::from_le_bytes(bytes));
        }
        self.rows = self.rows.checked_add(1).ok_or_else(row_count_overflow)?;
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> EngineResult<()> {
        self.rows = self
            .rows
            .checked_add(other.rows)
            .ok_or_else(row_count_overflow)?;
        for (left, right) in self.lanes.iter_mut().zip(other.lanes) {
            *left = left.wrapping_add(right);
        }
        Ok(())
    }

    fn finish(&self, table: &str, column_count: usize) -> EngineResult<[u8; 32]> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(TABLE_DIGEST_DOMAIN);
        hash_length(&mut hasher, table.len())?;
        hasher.update(table.as_bytes());
        hash_length(&mut hasher, column_count)?;
        hasher.update(&self.rows.to_le_bytes());
        for lane in self.lanes {
            hasher.update(&lane.to_le_bytes());
        }
        Ok(*hasher.finalize().as_bytes())
    }
}

fn hash_length(hasher: &mut blake3::Hasher, length: usize) -> EngineResult<()> {
    let length = u64::try_from(length).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::LimitExceeded,
            "SQLite import value exceeds its canonical digest length",
            error,
        )
    })?;
    hasher.update(&length.to_le_bytes());
    Ok(())
}

fn row_count_overflow() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "SQLite import row count exceeds its supported representation",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multiset_digest_is_order_independent_and_multiplicity_sensitive() {
        let first = vec![
            RawSqlValue::Integer(1),
            RawSqlValue::Text(b"alpha".to_vec()),
        ];
        let second = vec![RawSqlValue::Real(-0.0), RawSqlValue::Blob(vec![0, 0xff])];

        let mut forward = MultisetAccumulator::default();
        forward.add(&first).unwrap();
        forward.add(&second).unwrap();
        let mut reverse = MultisetAccumulator::default();
        reverse.add(&second).unwrap();
        reverse.add(&first).unwrap();
        assert_eq!(
            forward.finish("events", 2).unwrap(),
            reverse.finish("events", 2).unwrap()
        );

        reverse.add(&first).unwrap();
        assert_ne!(
            forward.finish("events", 2).unwrap(),
            reverse.finish("events", 2).unwrap()
        );
    }

    #[test]
    fn raw_text_binding_preserves_invalid_utf8_and_storage_class() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute("CREATE TABLE exact(value TEXT)", [])
            .unwrap();
        let value = RawSqlValue::Text(vec![0xff, 0, b'a']);
        connection
            .execute("INSERT INTO exact(value) VALUES (?1)", [&value])
            .unwrap();
        let (storage_class, bytes): (String, Vec<u8>) = connection
            .query_row(
                "SELECT typeof(value), CAST(value AS BLOB) FROM exact",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(storage_class, "text");
        assert_eq!(bytes, vec![0xff, 0, b'a']);
    }

    #[test]
    fn exact_digest_distinguishes_every_sqlite_storage_class() {
        let values = [
            RawSqlValue::Null,
            RawSqlValue::Integer(1),
            RawSqlValue::Real(1.0),
            RawSqlValue::Text(vec![1]),
            RawSqlValue::Blob(vec![1]),
        ];
        let mut seen = std::collections::HashSet::new();
        for value in values {
            let mut accumulator = MultisetAccumulator::default();
            accumulator.add(&[value]).unwrap();
            assert!(seen.insert(accumulator.finish("typed", 1).unwrap()));
        }
    }

    #[test]
    fn logical_contents_digest_has_a_frozen_v1_vector() {
        let mut accumulator = MultisetAccumulator::default();
        accumulator
            .add(&[
                RawSqlValue::Null,
                RawSqlValue::Integer(-42),
                RawSqlValue::Real(-0.0),
                RawSqlValue::Text(vec![0xff, 0]),
                RawSqlValue::Blob(vec![0, 0xff]),
            ])
            .unwrap();
        assert_eq!(
            hex_digest(accumulator.finish("typed", 5).unwrap()),
            "94f858b6eae8002d80c7b650f4f069c20b1b8e6837a0044219ca7068d4bff985",
        );
    }
}
