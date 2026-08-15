//! Protocol-neutral SQL parsing, SQLite execution, and value conversion.
//!
//! The parser facade produces a bounded, dialect-explicit opaque AST. The
//! common-subset validator recursively admits only protocol-neutral SQL shapes,
//! the statement classifier identifies protocol-neutral behavior and enforces
//! read-only multi-statement batches, and the placeholder normalizer produces a
//! separate source-preserving SQLite parameter representation for planning.
//! HTTP execution uses these layers whenever an authoritative table catalog is
//! populated; only an empty-catalog compatibility path remains raw SQLite
//! pass-through. The SQL layer itself does not open storage or execute a plan.

mod classifier;
mod dml;
mod generated;
mod global_index;
mod inference;
mod normalizer;
mod parser;
mod scatter;
mod subset;
mod translator;

pub(crate) use classifier::classify_normalized_statements;
pub(crate) use dml::{GeneratedInsertShape, RoutedDml, generated_insert_shape, routed_dml_shape};

pub use classifier::{
    SchemaBehavior, SessionBehavior, StatementBatchClassification, StatementBehavior,
    WriteBehavior, classify_statements,
};
pub use generated::{GeneratedIdPolicyIntent, GeneratedTableIntent};
pub(crate) use global_index::{
    GlobalIndexInferenceFallback, ShardSummaryInferenceFallback, infer_global_index_lookup,
    infer_shard_summary_lookup,
};
pub use inference::{ShardKeyInference, ShardKeyInferenceKind, ShardKeyValue, infer_shard_keys};
pub use normalizer::{
    MAX_SQL_PARAMETERS, NormalizedSql, StatementParameters, normalize_placeholders,
};
pub use parser::{
    MAX_PARSED_SQL_BYTES, MAX_PARSED_SQL_STATEMENTS, ParsedSql, SQL_PARSE_RECURSION_LIMIT,
    SqlDialect, parse,
};
pub(crate) use parser::{
    validate_authoritative_schema_migration, validate_stateless_catalog_schema_sql,
};
pub(crate) use scatter::validate_scatter_safe;
pub use subset::{CommonSql, MAX_COMMON_SQL_EXPRESSION_DEPTH, validate_common_subset};
pub use translator::{SqlTranslationMode, TranslatedSql, translate_sql};

use rusqlite::{
    Connection, Statement as SqlStatement, params_from_iter,
    types::{Value as SqlValue, ValueRef},
};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::{
    core::{
        Column, DataType, EngineError, EngineErrorKind, EngineResult, ResultLimits, ResultSet, Row,
        Value,
    },
    sqlite_error,
};

const RESULT_ENVELOPE_BYTES: u64 = 16;
const TYPE_TAG_BYTES: u64 = 1;
const LENGTH_BYTES: u64 = 8;
const ROW_FRAME_BYTES: u64 = 8;
const FIXED_VALUE_PAYLOAD_BYTES: u64 = 8;

/// One result budget shared by every physical query in a scatter operation.
///
/// Clones refer to the same counters. Column metadata is charged once and must
/// match exactly on every shard; rows and logical bytes are reserved atomically
/// before SQLite values are copied into an owned [`ResultSet`].
#[derive(Debug, Clone)]
pub(crate) struct ScatterResultBudget {
    limits: ResultLimits,
    state: Arc<Mutex<ScatterResultBudgetState>>,
}

#[derive(Debug, Default)]
struct ScatterResultBudgetState {
    columns: Option<Vec<Column>>,
    rows: u64,
    logical_bytes: u64,
}

impl ScatterResultBudget {
    pub(crate) fn new(limits: ResultLimits) -> Self {
        Self {
            limits,
            state: Arc::new(Mutex::new(ScatterResultBudgetState::default())),
        }
    }

    fn register_columns(&self, columns: &[Column]) -> EngineResult<()> {
        let mut state = self.lock_state();
        if let Some(expected) = &state.columns {
            if expected != columns {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "scatter query returned inconsistent column metadata",
                ));
            }
            return Ok(());
        }

        // Calculate against locals so a limit failure cannot partially commit
        // metadata accounting to the shared operation.
        let mut logical_bytes = account_bytes(
            state.logical_bytes,
            RESULT_ENVELOPE_BYTES,
            self.limits.max_bytes(),
        )?;
        for column in columns {
            logical_bytes = account_bytes(logical_bytes, TYPE_TAG_BYTES, self.limits.max_bytes())?;
            logical_bytes = account_bytes(logical_bytes, LENGTH_BYTES, self.limits.max_bytes())?;
            logical_bytes = account_bytes(
                logical_bytes,
                usize_to_u64(column.name.len())?,
                self.limits.max_bytes(),
            )?;
        }

        state.logical_bytes = logical_bytes;
        state.columns = Some(columns.to_vec());
        Ok(())
    }

    fn reserve_sqlite_row(&self, row: &rusqlite::Row<'_>, column_count: usize) -> EngineResult<()> {
        let mut additional_bytes = ROW_FRAME_BYTES;
        for index in 0..column_count {
            let value = row.get_ref(index).map_err(sqlite_error::statement)?;
            additional_bytes = additional_bytes
                .checked_add(logical_value_bytes(value)?)
                .ok_or_else(byte_limit_exceeded)?;
        }

        let mut state = self.lock_state();
        let rows = account_row(state.rows, self.limits.max_rows())?;
        let logical_bytes = account_bytes(
            state.logical_bytes,
            additional_bytes,
            self.limits.max_bytes(),
        )?;

        // Commit both counters together only after both limits pass.
        state.rows = rows;
        state.logical_bytes = logical_bytes;
        Ok(())
    }

    fn lock_state(&self) -> MutexGuard<'_, ScatterResultBudgetState> {
        // A panic while another owner held the mutex must not turn every later
        // request into a second panic. The accounting methods themselves only
        // commit complete transitions, so recovering the guarded state is safe.
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}

/// Owned metadata collected while SQLite transiently prepares a statement.
///
/// SQLite's statement and column metadata borrow their connection. Keeping an
/// owned protocol-neutral copy lets prepared-statement callers release that
/// borrow before storing the description in a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatementMetadata {
    parameter_count: usize,
    columns: Vec<Column>,
    readonly: bool,
}

impl StatementMetadata {
    pub(crate) const fn parameter_count(&self) -> usize {
        self.parameter_count
    }

    pub(crate) fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub(crate) const fn readonly(&self) -> bool {
        self.readonly
    }

    pub(crate) fn produces_columns(&self) -> bool {
        !self.columns.is_empty()
    }
}

/// The result of executing one transiently prepared SQLite statement.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StatementExecution {
    Rows(ResultSet),
    AffectedRows(usize),
}

/// Transiently prepares a statement and copies all metadata needed by the
/// protocol-neutral prepared-statement lifecycle.
pub(crate) fn describe_statement(
    connection: &Connection,
    statement: &str,
) -> EngineResult<StatementMetadata> {
    let statement = connection
        .prepare(statement)
        .map_err(sqlite_error::statement)?;
    Ok(statement_metadata(&statement))
}

/// Transiently prepares and executes exactly one statement.
///
/// Column-producing writes (for example DML with `RETURNING`) are rejected
/// before SQLite is stepped. Supporting those safely requires a result and
/// transaction policy that the protocol-neutral engine does not yet expose.
pub(crate) fn execute_statement_with_limits(
    connection: &Connection,
    statement: &str,
    params: &[Value],
    limits: ResultLimits,
) -> EngineResult<StatementExecution> {
    let params = sqlite_parameters(params)?;
    let mut statement = connection
        .prepare(statement)
        .map_err(sqlite_error::statement)?;
    let metadata = statement_metadata(&statement);

    if metadata.produces_columns() {
        if !metadata.readonly() {
            return Err(EngineError::new(
                EngineErrorKind::InvalidQuery,
                "row-producing write statements are not supported",
            ));
        }
        return materialize_rows(&mut statement, metadata.columns, params, limits)
            .map(StatementExecution::Rows);
    }

    statement
        .execute(params_from_iter(params))
        .map(StatementExecution::AffectedRows)
        .map_err(sqlite_error::statement)
}

/// Validates that every protocol-neutral parameter has a lossless SQLite
/// binding without allocating converted text or binary payloads.
pub(crate) fn validate_parameters(params: &[Value]) -> EngineResult<()> {
    params.iter().try_for_each(validate_parameter)
}

fn validate_parameter(value: &Value) -> EngineResult<()> {
    match value {
        Value::UInt64(value) => i64::try_from(*value).map(|_| ()).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::NumericOutOfRange,
                format!("unsigned integer {value} exceeds SQLite INTEGER range"),
                error,
            )
        }),
        Value::Float64(value) if value.is_nan() => Err(EngineError::new(
            EngineErrorKind::Unsupported,
            "NaN has no lossless SQLite binding because SQLite converts it to NULL",
        )),
        Value::Decimal(value) => Err(EngineError::new(
            EngineErrorKind::Unsupported,
            format!("decimal value {value} has no lossless SQLite binding"),
        )),
        Value::InvalidText(_) => Err(EngineError::new(
            EngineErrorKind::InvalidTextEncoding,
            "non-UTF-8 text has no lossless SQLite binding",
        )),
        Value::Null
        | Value::Boolean(_)
        | Value::Int64(_)
        | Value::Float64(_)
        | Value::Text(_)
        | Value::Binary(_) => Ok(()),
    }
}

fn statement_metadata(statement: &SqlStatement<'_>) -> StatementMetadata {
    StatementMetadata {
        parameter_count: statement.parameter_count(),
        columns: statement
            .columns()
            .into_iter()
            .map(|column| {
                Column::new(
                    column.name(),
                    column
                        .decl_type()
                        .map(declared_data_type)
                        .unwrap_or(DataType::Unknown),
                )
            })
            .collect(),
        readonly: statement.readonly(),
    }
}

fn declared_data_type(declaration: &str) -> DataType {
    let declaration = declaration.trim().to_ascii_uppercase();
    if matches!(declaration.as_str(), "BOOL" | "BOOLEAN") {
        DataType::Boolean
    } else if declaration.contains("INT") {
        DataType::Int64
    } else if declaration.contains("CHAR")
        || declaration.contains("CLOB")
        || declaration.contains("TEXT")
    {
        DataType::Text
    } else if declaration.contains("BLOB") {
        DataType::Binary
    } else if declaration.contains("REAL")
        || declaration.contains("FLOA")
        || declaration.contains("DOUB")
    {
        DataType::Float64
    } else if declaration.contains("DECIMAL") || declaration.contains("NUMERIC") {
        DataType::Decimal
    } else {
        DataType::Unknown
    }
}

pub(crate) fn execute(
    connection: &Connection,
    statement: &str,
    params: &[Value],
) -> EngineResult<usize> {
    let params = sqlite_parameters(params)?;
    connection
        .execute(statement, params_from_iter(params))
        .map_err(sqlite_error::statement)
}

pub(crate) fn query(
    connection: &Connection,
    statement: &str,
    params: &[Value],
) -> EngineResult<ResultSet> {
    query_with_limits(connection, statement, params, ResultLimits::default())
}

pub(crate) fn query_with_limits(
    connection: &Connection,
    statement: &str,
    params: &[Value],
    limits: ResultLimits,
) -> EngineResult<ResultSet> {
    let params = sqlite_parameters(params)?;
    let mut statement = connection
        .prepare(statement)
        .map_err(sqlite_error::statement)?;
    if !statement.readonly() {
        return Err(EngineError::new(
            EngineErrorKind::InvalidQuery,
            "query statements must be read-only",
        ));
    }

    let metadata = statement_metadata(&statement);
    materialize_rows(&mut statement, metadata.columns, params, limits)
}

/// Execute one physical shard query against a budget shared by the complete
/// scatter operation.
///
/// This deliberately parallels [`query_with_limits`] instead of changing its
/// accounting contract: ordinary single-shard queries remain independently
/// bounded, while scatter callers explicitly opt into operation-wide limits.
pub(crate) fn query_with_scatter_budget(
    connection: &Connection,
    statement: &str,
    params: &[Value],
    budget: &ScatterResultBudget,
) -> EngineResult<ResultSet> {
    let params = sqlite_parameters(params)?;
    let mut statement = connection
        .prepare(statement)
        .map_err(sqlite_error::statement)?;
    if !statement.readonly() {
        return Err(EngineError::new(
            EngineErrorKind::InvalidQuery,
            "query statements must be read-only",
        ));
    }

    let metadata = statement_metadata(&statement);
    materialize_rows_with_scatter_budget(&mut statement, metadata.columns, params, budget)
}

/// Step one read-only SQLite statement and publish each owned row through a
/// bounded protocol-neutral sink.
///
/// `publish_columns` runs after successful prepare and budget validation but
/// before SQLite is stepped. Scatter callers share one budget and invoke this
/// once per shard, which also verifies identical column metadata.
pub(crate) fn stream_query_with_budget(
    connection: &Connection,
    statement: &str,
    params: &[Value],
    budget: &ScatterResultBudget,
    publish_columns: impl FnOnce(&[Column]) -> EngineResult<()>,
    mut publish_row: impl FnMut(Row) -> EngineResult<()>,
) -> EngineResult<()> {
    let params = sqlite_parameters(params)?;
    let mut statement = connection
        .prepare(statement)
        .map_err(sqlite_error::statement)?;
    if !statement.readonly() {
        return Err(EngineError::new(
            EngineErrorKind::InvalidQuery,
            "streamed query statements must be read-only",
        ));
    }

    let metadata = statement_metadata(&statement);
    budget.register_columns(&metadata.columns)?;
    publish_columns(&metadata.columns)?;
    let mut sqlite_rows = statement
        .query(params_from_iter(params))
        .map_err(sqlite_error::statement)?;
    while let Some(sqlite_row) = sqlite_rows.next().map_err(sqlite_error::statement)? {
        budget.reserve_sqlite_row(sqlite_row, metadata.columns.len())?;
        let mut values = Vec::with_capacity(metadata.columns.len());
        for index in 0..metadata.columns.len() {
            values.push(sql_to_value(
                sqlite_row.get_ref(index).map_err(sqlite_error::statement)?,
            ));
        }
        publish_row(Row::new(values))?;
    }
    Ok(())
}

fn materialize_rows(
    statement: &mut SqlStatement<'_>,
    columns: Vec<Column>,
    params: Vec<SqlValue>,
    limits: ResultLimits,
) -> EngineResult<ResultSet> {
    let mut logical_bytes = account_bytes(0, RESULT_ENVELOPE_BYTES, limits.max_bytes())?;
    for column in &columns {
        logical_bytes = account_bytes(logical_bytes, TYPE_TAG_BYTES, limits.max_bytes())?;
        logical_bytes = account_bytes(logical_bytes, LENGTH_BYTES, limits.max_bytes())?;
        logical_bytes = account_bytes(
            logical_bytes,
            usize_to_u64(column.name.len())?,
            limits.max_bytes(),
        )?;
    }
    let mut sqlite_rows = statement
        .query(params_from_iter(params))
        .map_err(sqlite_error::statement)?;
    let mut rows = Vec::new();
    let mut row_count = 0_u64;

    while let Some(sqlite_row) = sqlite_rows.next().map_err(sqlite_error::statement)? {
        row_count = account_row(row_count, limits.max_rows())?;
        let mut row_bytes = account_bytes(logical_bytes, ROW_FRAME_BYTES, limits.max_bytes())?;

        // Account every borrowed SQLite value before cloning any payload into
        // the protocol-neutral result. A rejected row is therefore never
        // partially materialized.
        for index in 0..columns.len() {
            let value = sqlite_row.get_ref(index).map_err(sqlite_error::statement)?;
            row_bytes = account_bytes(row_bytes, logical_value_bytes(value)?, limits.max_bytes())?;
        }

        let mut values = Vec::with_capacity(columns.len());
        for index in 0..columns.len() {
            values.push(sql_to_value(
                sqlite_row.get_ref(index).map_err(sqlite_error::statement)?,
            ));
        }
        rows.push(Row::new(values));
        logical_bytes = row_bytes;
    }
    ResultSet::new(columns, rows).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::Internal,
            "SQLite returned rows that do not match their column metadata",
            error,
        )
    })
}

fn materialize_rows_with_scatter_budget(
    statement: &mut SqlStatement<'_>,
    columns: Vec<Column>,
    params: Vec<SqlValue>,
    budget: &ScatterResultBudget,
) -> EngineResult<ResultSet> {
    budget.register_columns(&columns)?;

    let mut sqlite_rows = statement
        .query(params_from_iter(params))
        .map_err(sqlite_error::statement)?;
    let mut rows = Vec::new();

    while let Some(sqlite_row) = sqlite_rows.next().map_err(sqlite_error::statement)? {
        // Reservation examines every borrowed value and atomically commits the
        // full row charge before any payload is cloned into owned memory.
        budget.reserve_sqlite_row(sqlite_row, columns.len())?;

        let mut values = Vec::with_capacity(columns.len());
        for index in 0..columns.len() {
            values.push(sql_to_value(
                sqlite_row.get_ref(index).map_err(sqlite_error::statement)?,
            ));
        }
        rows.push(Row::new(values));
    }

    ResultSet::new(columns, rows).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::Internal,
            "SQLite returned rows that do not match their column metadata",
            error,
        )
    })
}

/// Convert protocol-neutral values into lossless owned SQLite bindings.
///
/// Storage execution boundaries use this shared conversion so direct shard
/// statements and the experimental coordinator reject unsupported values with
/// identical error kinds before SQLite can coerce them.
pub(crate) fn sqlite_parameters(params: &[Value]) -> EngineResult<Vec<SqlValue>> {
    params.iter().map(value_to_sql).collect()
}

fn account_row(current: u64, maximum: u64) -> EngineResult<u64> {
    let next = current.checked_add(1).ok_or_else(row_limit_exceeded)?;
    if next > maximum {
        return Err(row_limit_exceeded());
    }
    Ok(next)
}

fn account_bytes(current: u64, additional: u64, maximum: u64) -> EngineResult<u64> {
    let next = current
        .checked_add(additional)
        .ok_or_else(byte_limit_exceeded)?;
    if next > maximum {
        return Err(byte_limit_exceeded());
    }
    Ok(next)
}

fn logical_value_bytes(value: ValueRef<'_>) -> EngineResult<u64> {
    let payload = match value {
        ValueRef::Null => 0,
        ValueRef::Integer(_) | ValueRef::Real(_) => FIXED_VALUE_PAYLOAD_BYTES,
        ValueRef::Text(value) | ValueRef::Blob(value) => usize_to_u64(value.len())?,
    };
    TYPE_TAG_BYTES
        .checked_add(LENGTH_BYTES)
        .and_then(|framing| framing.checked_add(payload))
        .ok_or_else(byte_limit_exceeded)
}

fn usize_to_u64(value: usize) -> EngineResult<u64> {
    u64::try_from(value).map_err(|_| byte_limit_exceeded())
}

fn row_limit_exceeded() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "query result exceeds the configured row limit",
    )
}

fn byte_limit_exceeded() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "query result exceeds the configured logical byte limit",
    )
}

#[cfg(test)]
pub(crate) fn execute_batch(connection: &Connection, statement: &str) -> EngineResult<()> {
    connection
        .execute_batch(statement)
        .map_err(sqlite_error::statement)
}

fn value_to_sql(value: &Value) -> EngineResult<SqlValue> {
    Ok(match value {
        Value::Null => SqlValue::Null,
        Value::Boolean(value) => SqlValue::Integer(i64::from(*value)),
        Value::Int64(value) => SqlValue::Integer(*value),
        Value::UInt64(value) => SqlValue::Integer(i64::try_from(*value).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::NumericOutOfRange,
                format!("unsigned integer {value} exceeds SQLite INTEGER range"),
                error,
            )
        })?),
        Value::Float64(value) if value.is_nan() => {
            return Err(EngineError::new(
                EngineErrorKind::Unsupported,
                "NaN has no lossless SQLite binding because SQLite converts it to NULL",
            ));
        }
        Value::Float64(value) => SqlValue::Real(*value),
        Value::Decimal(value) => {
            return Err(EngineError::new(
                EngineErrorKind::Unsupported,
                format!("decimal value {value} has no lossless SQLite binding"),
            ));
        }
        Value::Text(value) => SqlValue::Text(value.clone()),
        Value::InvalidText(_) => {
            return Err(EngineError::new(
                EngineErrorKind::InvalidTextEncoding,
                "non-UTF-8 text has no lossless SQLite binding",
            ));
        }
        Value::Binary(value) => SqlValue::Blob(value.clone()),
    })
}

fn sql_to_value(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => Value::Int64(value),
        ValueRef::Real(value) => Value::Float64(value),
        ValueRef::Text(value) => match std::str::from_utf8(value) {
            Ok(value) => Value::Text(value.to_owned()),
            Err(_) => Value::InvalidText(value.to_vec()),
        },
        ValueRef::Blob(value) => Value::Binary(value.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_metadata_preserves_exact_parameter_and_column_layouts() {
        let connection = Connection::open_in_memory().unwrap();
        let metadata = describe_statement(
            &connection,
            "SELECT ?1 AS duplicate,
                    ?3 AS \"\",
                    ? AS middle,
                    :named AS duplicate,
                    :named AS repeated",
        )
        .unwrap();

        // SQLite reports the greatest assigned parameter index. That includes
        // the unused ?2 gap and does not allocate a second slot for a repeated
        // named parameter.
        assert_eq!(metadata.parameter_count(), 5);
        assert_eq!(
            metadata.columns(),
            [
                Column::new("duplicate", DataType::Unknown),
                Column::new("", DataType::Unknown),
                Column::new("middle", DataType::Unknown),
                Column::new("duplicate", DataType::Unknown),
                Column::new("repeated", DataType::Unknown),
            ]
        );
        assert!(metadata.readonly());
        assert!(metadata.produces_columns());
    }

    #[test]
    fn statement_metadata_distinguishes_commands_from_row_producers() {
        let connection = Connection::open_in_memory().unwrap();
        execute_batch(
            &connection,
            "CREATE TABLE metadata_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL);",
        )
        .unwrap();

        let command = describe_statement(
            &connection,
            "INSERT INTO metadata_test (id, value) VALUES (?1, ?2)",
        )
        .unwrap();
        assert_eq!(command.parameter_count(), 2);
        assert!(command.columns().is_empty());
        assert!(!command.readonly());
        assert!(!command.produces_columns());

        let returning = describe_statement(
            &connection,
            "INSERT INTO metadata_test (id, value) VALUES (?1, ?2) RETURNING id, value",
        )
        .unwrap();
        assert_eq!(returning.parameter_count(), 2);
        assert_eq!(
            returning.columns(),
            [
                Column::new("id", DataType::Int64),
                Column::new("value", DataType::Text),
            ]
        );
        assert!(!returning.readonly());
        assert!(returning.produces_columns());

        let selected =
            describe_statement(&connection, "SELECT id, value FROM metadata_test").unwrap();
        assert_eq!(
            selected.columns(),
            [
                Column::new("id", DataType::Int64),
                Column::new("value", DataType::Text),
            ]
        );
        assert!(selected.readonly());
    }

    #[test]
    fn sqlite_declarations_map_to_conservative_protocol_neutral_types() {
        for (declaration, expected) in [
            ("BOOLEAN", DataType::Boolean),
            ("unsigned big int", DataType::Int64),
            ("VARCHAR(255)", DataType::Text),
            ("BLOB", DataType::Binary),
            ("DOUBLE PRECISION", DataType::Float64),
            ("DECIMAL(20, 4)", DataType::Decimal),
            ("DATE", DataType::Unknown),
            ("", DataType::Unknown),
        ] {
            assert_eq!(declared_data_type(declaration), expected, "{declaration}");
        }
    }

    #[test]
    fn transient_execution_returns_rows_with_ordered_typed_values() {
        let connection = Connection::open_in_memory().unwrap();
        let result = execute_statement_with_limits(
            &connection,
            "SELECT ?1 AS duplicate,
                    ?2 AS duplicate,
                    ?3 AS \"\",
                    ?4 AS blob_value,
                    NULL AS optional_value",
            &[
                Value::from(7_i64),
                Value::from(1.5_f64),
                Value::from("text"),
                Value::from(vec![0_u8, 255]),
            ],
            ResultLimits::default(),
        )
        .unwrap();

        let StatementExecution::Rows(result) = result else {
            panic!("SELECT must produce rows");
        };
        assert_eq!(
            result.columns(),
            [
                Column::new("duplicate", DataType::Unknown),
                Column::new("duplicate", DataType::Unknown),
                Column::new("", DataType::Unknown),
                Column::new("blob_value", DataType::Unknown),
                Column::new("optional_value", DataType::Unknown),
            ]
        );
        assert_eq!(
            result.rows(),
            [Row::new(vec![
                Value::from(7_i64),
                Value::from(1.5_f64),
                Value::from("text"),
                Value::from(vec![0_u8, 255]),
                Value::Null,
            ])]
        );
    }

    #[test]
    fn transient_execution_returns_affected_rows_for_commands() {
        let connection = Connection::open_in_memory().unwrap();
        execute_batch(
            &connection,
            "CREATE TABLE execution_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL);",
        )
        .unwrap();

        assert_eq!(
            execute_statement_with_limits(
                &connection,
                "INSERT INTO execution_test (id, value) VALUES (1, 'one'), (2, 'two')",
                &[],
                ResultLimits::default(),
            )
            .unwrap(),
            StatementExecution::AffectedRows(2)
        );
        assert_eq!(
            execute_statement_with_limits(
                &connection,
                "UPDATE execution_test SET value = ?1 WHERE id = ?2",
                &[Value::from("updated"), Value::from(2_i64)],
                ResultLimits::default(),
            )
            .unwrap(),
            StatementExecution::AffectedRows(1)
        );
        assert_eq!(
            connection
                .query_row("SELECT value FROM execution_test WHERE id = 2", [], |row| {
                    row.get::<_, String>(0)
                },)
                .unwrap(),
            "updated"
        );
    }

    #[test]
    fn transient_execution_classifies_arity_and_value_errors_without_mutation() {
        let connection = Connection::open_in_memory().unwrap();
        execute_batch(
            &connection,
            "CREATE TABLE binding_test (id INTEGER PRIMARY KEY, value INTEGER NOT NULL);",
        )
        .unwrap();

        for params in [Vec::new(), vec![Value::from(1_i64), Value::from(2_i64)]] {
            let error = execute_statement_with_limits(
                &connection,
                "INSERT INTO binding_test (id, value) VALUES (1, ?1)",
                &params,
                ResultLimits::default(),
            )
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
        }

        let too_large = u64::try_from(i64::MAX).unwrap() + 1;
        let error = execute_statement_with_limits(
            &connection,
            "INSERT INTO binding_test (id, value) VALUES (1, ?1)",
            &[Value::from(too_large)],
            ResultLimits::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::NumericOutOfRange);

        let error = execute_statement_with_limits(
            &connection,
            "INSERT INTO binding_test (id, value) VALUES (1, ?1)",
            &[Value::from(f64::NAN)],
            ResultLimits::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Unsupported);

        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM binding_test", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn transient_execution_rejects_row_producing_writes_before_mutation() {
        let connection = Connection::open_in_memory().unwrap();
        execute_batch(
            &connection,
            "CREATE TABLE returning_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO returning_test (id, value) VALUES (1, 'original');",
        )
        .unwrap();

        for statement in [
            "INSERT INTO returning_test (id, value) VALUES (2, 'inserted') RETURNING id",
            "UPDATE returning_test SET value = 'updated' WHERE id = 1 RETURNING id",
            "DELETE FROM returning_test WHERE id = 1 RETURNING id",
        ] {
            let error =
                execute_statement_with_limits(&connection, statement, &[], ResultLimits::default())
                    .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
            assert_eq!(
                error.to_string(),
                "row-producing write statements are not supported"
            );
        }

        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*), MIN(value) FROM returning_test",
                    [],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                )
                .unwrap(),
            (1, "original".to_owned())
        );
    }

    #[test]
    fn transient_execution_uses_the_existing_exact_result_limits() {
        let connection = Connection::open_in_memory().unwrap();
        // Metadata is 26 bytes. Each integer row is 25 bytes.
        let exact = ResultLimits::new(2, 76).unwrap();
        let result = execute_statement_with_limits(
            &connection,
            "SELECT 1 AS v UNION ALL SELECT 2",
            &[],
            exact,
        )
        .unwrap();
        assert!(matches!(result, StatementExecution::Rows(result) if result.len() == 2));

        for (limits, message) in [
            (
                ResultLimits::new(1, 76).unwrap(),
                "query result exceeds the configured row limit",
            ),
            (
                ResultLimits::new(2, 75).unwrap(),
                "query result exceeds the configured logical byte limit",
            ),
        ] {
            let error = execute_statement_with_limits(
                &connection,
                "SELECT 1 AS v UNION ALL SELECT 2",
                &[],
                limits,
            )
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn transient_prepare_and_execution_classify_invalid_sql() {
        let connection = Connection::open_in_memory().unwrap();

        for error in [
            describe_statement(&connection, "SELECT FROM").unwrap_err(),
            execute_statement_with_limits(
                &connection,
                "SELECT * FROM missing_table",
                &[],
                ResultLimits::default(),
            )
            .unwrap_err(),
        ] {
            assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
        }
    }

    #[test]
    fn protocol_values_bind_to_the_expected_sqlite_storage_classes() {
        assert_eq!(value_to_sql(&Value::Null).unwrap(), SqlValue::Null);
        assert_eq!(
            value_to_sql(&Value::from(true)).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            value_to_sql(&Value::from(false)).unwrap(),
            SqlValue::Integer(0)
        );
        assert_eq!(
            value_to_sql(&Value::from(42_i64)).unwrap(),
            SqlValue::Integer(42)
        );
        assert_eq!(
            value_to_sql(&Value::from(42_u64)).unwrap(),
            SqlValue::Integer(42)
        );
        assert_eq!(
            value_to_sql(&Value::from(1.5_f64)).unwrap(),
            SqlValue::Real(1.5)
        );
        assert_eq!(
            value_to_sql(&Value::from(f64::INFINITY)).unwrap(),
            SqlValue::Real(f64::INFINITY)
        );
        assert_eq!(
            value_to_sql(&Value::from("text")).unwrap(),
            SqlValue::Text("text".to_owned())
        );
        assert_eq!(
            value_to_sql(&Value::from(vec![0_u8, 255])).unwrap(),
            SqlValue::Blob(vec![0, 255])
        );
    }

    #[test]
    fn parameter_validation_accepts_every_lossless_sqlite_binding() {
        validate_parameters(&[
            Value::Null,
            Value::from(false),
            Value::from(true),
            Value::from(i64::MIN),
            Value::from(i64::MAX),
            Value::from(0_u64),
            Value::from(u64::try_from(i64::MAX).unwrap()),
            Value::from(f64::NEG_INFINITY),
            Value::from(-0.0_f64),
            Value::from(f64::INFINITY),
            Value::from(String::new()),
            Value::from("text"),
            Value::from(Vec::<u8>::new()),
            Value::from(vec![0_u8, 255]),
        ])
        .unwrap();
    }

    #[test]
    fn parameter_validation_preserves_precise_rejection_kinds() {
        let too_large = u64::try_from(i64::MAX).unwrap() + 1;
        let cases = [
            (Value::from(too_large), EngineErrorKind::NumericOutOfRange),
            (
                Value::decimal("12.3400").unwrap(),
                EngineErrorKind::Unsupported,
            ),
            (
                Value::InvalidText(vec![0x80]),
                EngineErrorKind::InvalidTextEncoding,
            ),
            (Value::from(f64::NAN), EngineErrorKind::Unsupported),
        ];

        for (value, expected) in cases {
            assert_eq!(validate_parameters(&[value]).unwrap_err().kind(), expected);
        }
    }

    #[test]
    fn values_without_lossless_sqlite_bindings_are_rejected() {
        let too_large = u64::try_from(i64::MAX).unwrap() + 1;
        let error = value_to_sql(&Value::from(too_large)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::NumericOutOfRange);
        assert_eq!(
            error.to_string(),
            format!("unsigned integer {too_large} exceeds SQLite INTEGER range")
        );

        let error = value_to_sql(&Value::decimal("12.3400").unwrap()).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Unsupported);
        assert_eq!(
            error.to_string(),
            "decimal value 12.3400 has no lossless SQLite binding"
        );

        let error = value_to_sql(&Value::InvalidText(vec![0x80])).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidTextEncoding);
        assert_eq!(
            error.to_string(),
            "non-UTF-8 text has no lossless SQLite binding"
        );

        let error = value_to_sql(&Value::from(f64::NAN)).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Unsupported);
        assert_eq!(
            error.to_string(),
            "NaN has no lossless SQLite binding because SQLite converts it to NULL"
        );
    }

    #[test]
    fn sqlite_storage_classes_convert_without_json_loss() {
        assert_eq!(sql_to_value(ValueRef::Null), Value::Null);
        assert_eq!(sql_to_value(ValueRef::Integer(42)), Value::from(42_i64));
        assert_eq!(sql_to_value(ValueRef::Real(1.5)), Value::from(1.5_f64));
        assert_eq!(sql_to_value(ValueRef::Text(b"text")), Value::from("text"));
        assert_eq!(
            sql_to_value(ValueRef::Blob(&[0, 255])),
            Value::from(vec![0_u8, 255])
        );
    }

    #[test]
    fn sqlite_text_with_invalid_utf8_preserves_its_original_bytes() {
        assert_eq!(
            sql_to_value(ValueRef::Text(&[b'f', 0x80])),
            Value::InvalidText(vec![b'f', 0x80])
        );
    }

    #[test]
    fn query_preserves_invalid_sqlite_text_bytes() {
        let connection = Connection::open_in_memory().unwrap();
        let result = query(&connection, "SELECT CAST(X'6680' AS TEXT) AS value", &[]).unwrap();

        assert_eq!(
            result.rows()[0].get(0),
            Some(&Value::InvalidText(vec![b'f', 0x80]))
        );
    }

    #[test]
    fn query_rejects_nan_before_sqlite_can_convert_it_to_null() {
        let connection = Connection::open_in_memory().unwrap();
        let error = query(&connection, "SELECT ?1 AS value", &[Value::from(f64::NAN)]).unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::Unsupported);
        assert_eq!(
            error.to_string(),
            "NaN has no lossless SQLite binding because SQLite converts it to NULL"
        );
    }

    #[test]
    fn logical_value_accounting_covers_every_sqlite_storage_class() {
        assert_eq!(logical_value_bytes(ValueRef::Null).unwrap(), 9);
        assert_eq!(logical_value_bytes(ValueRef::Integer(42)).unwrap(), 17);
        assert_eq!(logical_value_bytes(ValueRef::Real(1.5)).unwrap(), 17);
        assert_eq!(
            logical_value_bytes(ValueRef::Text("é".as_bytes())).unwrap(),
            11
        );
        assert_eq!(logical_value_bytes(ValueRef::Blob(&[0, 255])).unwrap(), 11);
    }

    #[test]
    fn checked_accounting_accepts_equality_and_classifies_overflow() {
        assert_eq!(account_row(1, 2).unwrap(), 2);
        assert_eq!(account_bytes(75, 1, 76).unwrap(), 76);

        for error in [
            account_row(u64::MAX, u64::MAX).unwrap_err(),
            account_bytes(u64::MAX, 1, u64::MAX).unwrap_err(),
        ] {
            assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        }
    }

    #[test]
    fn metadata_bytes_are_enforced_even_for_an_empty_result() {
        let connection = Connection::open_in_memory().unwrap();
        // Envelope (16) + column type (1) + name length (8) + "v" (1).
        let exact = ResultLimits::new(1, 26).unwrap();
        let result = query_with_limits(&connection, "SELECT 1 AS v WHERE 0", &[], exact).unwrap();
        assert!(result.is_empty());

        let one_byte_short = ResultLimits::new(1, 25).unwrap();
        let error = query_with_limits(&connection, "SELECT 1 AS v WHERE 0", &[], one_byte_short)
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(
            error.to_string(),
            "query result exceeds the configured logical byte limit"
        );
    }

    #[test]
    fn exact_row_and_byte_limits_pass_and_one_over_returns_no_result() {
        let connection = Connection::open_in_memory().unwrap();
        // Metadata is 26 bytes. Each row is a row length (8), value type (1),
        // value length (8), and integer payload (8), for 25 bytes per row.
        let exact = ResultLimits::new(2, 76).unwrap();
        let result =
            query_with_limits(&connection, "SELECT 1 AS v UNION ALL SELECT 2", &[], exact).unwrap();
        assert_eq!(result.rows().len(), 2);

        let row_error = query_with_limits(
            &connection,
            "SELECT 1 AS v UNION ALL SELECT 2",
            &[],
            ResultLimits::new(1, 76).unwrap(),
        )
        .unwrap_err();
        assert_eq!(row_error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(
            row_error.to_string(),
            "query result exceeds the configured row limit"
        );

        let byte_error = query_with_limits(
            &connection,
            "SELECT 1 AS v UNION ALL SELECT 2",
            &[],
            ResultLimits::new(2, 75).unwrap(),
        )
        .unwrap_err();
        assert_eq!(byte_error.kind(), EngineErrorKind::LimitExceeded);

        // A rejected materialization leaves the connection immediately usable.
        assert_eq!(
            query_with_limits(&connection, "SELECT 3 AS v", &[], exact)
                .unwrap()
                .rows()[0]
                .get(0),
            Some(&Value::from(3_i64))
        );
    }

    #[test]
    fn shared_scatter_budget_charges_metadata_once_across_queries() {
        let first = Connection::open_in_memory().unwrap();
        let second = Connection::open_in_memory().unwrap();
        // One shared metadata envelope is 26 bytes and each integer row is 25.
        let budget = ScatterResultBudget::new(ResultLimits::new(2, 76).unwrap());

        let first_result =
            query_with_scatter_budget(&first, "SELECT 1 AS v", &[], &budget).unwrap();
        let second_result =
            query_with_scatter_budget(&second, "SELECT 2 AS v", &[], &budget).unwrap();

        assert_eq!(first_result.rows(), [Row::new(vec![Value::from(1_i64)])]);
        assert_eq!(second_result.rows(), [Row::new(vec![Value::from(2_i64)])]);
        let state = budget.lock_state();
        assert_eq!(state.rows, 2);
        assert_eq!(state.logical_bytes, 76);
    }

    #[test]
    fn shared_scatter_budget_enforces_combined_row_limit_one_over() {
        let first = Connection::open_in_memory().unwrap();
        let second = Connection::open_in_memory().unwrap();
        let budget = ScatterResultBudget::new(ResultLimits::new(1, 76).unwrap());

        query_with_scatter_budget(&first, "SELECT 1 AS v", &[], &budget).unwrap();
        let error = query_with_scatter_budget(&second, "SELECT 2 AS v", &[], &budget).unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(
            error.to_string(),
            "query result exceeds the configured row limit"
        );
        let state = budget.lock_state();
        assert_eq!(state.rows, 1);
        assert_eq!(state.logical_bytes, 51);
    }

    #[test]
    fn shared_scatter_budget_enforces_combined_byte_limit_one_over_atomically() {
        let first = Connection::open_in_memory().unwrap();
        let second = Connection::open_in_memory().unwrap();
        let budget = ScatterResultBudget::new(ResultLimits::new(2, 75).unwrap());

        query_with_scatter_budget(&first, "SELECT 1 AS v", &[], &budget).unwrap();
        let error = query_with_scatter_budget(&second, "SELECT 2 AS v", &[], &budget).unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(
            error.to_string(),
            "query result exceeds the configured logical byte limit"
        );
        // The failed row reserves neither of its counters.
        let state = budget.lock_state();
        assert_eq!(state.rows, 1);
        assert_eq!(state.logical_bytes, 51);
    }

    #[test]
    fn shared_scatter_budget_rejects_metadata_mismatch_without_consuming_budget() {
        let first = Connection::open_in_memory().unwrap();
        let second = Connection::open_in_memory().unwrap();
        let budget = ScatterResultBudget::new(ResultLimits::new(1, 51).unwrap());

        query_with_scatter_budget(&first, "SELECT 1 AS v WHERE 0", &[], &budget).unwrap();
        let error =
            query_with_scatter_budget(&second, "SELECT 1 AS other", &[], &budget).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert_eq!(
            error.to_string(),
            "scatter query returned inconsistent column metadata"
        );

        let result = query_with_scatter_budget(&second, "SELECT 1 AS v", &[], &budget).unwrap();
        assert_eq!(result.rows(), [Row::new(vec![Value::from(1_i64)])]);
    }

    #[test]
    fn cloned_scatter_budgets_serialize_concurrent_row_reservations() {
        use std::sync::Barrier;

        let budget = ScatterResultBudget::new(ResultLimits::new(1, 51).unwrap());
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for value in [1_i64, 2_i64] {
            let worker_budget = budget.clone();
            let worker_barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                let connection = Connection::open_in_memory().unwrap();
                worker_barrier.wait();
                query_with_scatter_budget(
                    &connection,
                    "SELECT ?1 AS v",
                    &[Value::from(value)],
                    &worker_budget,
                )
            }));
        }

        barrier.wait();
        let results: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter_map(|result| result.as_ref().err())
                .map(EngineError::kind)
                .collect::<Vec<_>>(),
            [EngineErrorKind::LimitExceeded]
        );
    }

    #[test]
    fn shared_scatter_budget_recovers_a_poisoned_accounting_lock() {
        let budget = ScatterResultBudget::new(ResultLimits::new(1, 51).unwrap());
        let poisoner = budget.clone();
        let poisoned = std::panic::catch_unwind(move || {
            let _guard = poisoner.state.lock().unwrap();
            panic!("poison shared result accounting for this test");
        });
        assert!(poisoned.is_err());

        let connection = Connection::open_in_memory().unwrap();
        let result = query_with_scatter_budget(&connection, "SELECT 1 AS v", &[], &budget).unwrap();
        assert_eq!(result.rows(), [Row::new(vec![Value::from(1_i64)])]);
    }

    #[test]
    fn logical_bytes_use_utf8_and_blob_lengths_not_character_counts() {
        let connection = Connection::open_in_memory().unwrap();
        // Envelope (16), five one-byte column names (50), row frame (8), then
        // null (9), integer (17), real (17), two-byte UTF-8 text (11), and a
        // two-byte blob (11).
        let exact = ResultLimits::new(1, 139).unwrap();
        let sql = "SELECT NULL AS a, 42 AS b, 1.5 AS c, 'é' AS d, X'00FF' AS e";
        let result = query_with_limits(&connection, sql, &[], exact).unwrap();
        assert_eq!(result.rows().len(), 1);

        assert_eq!(
            query_with_limits(&connection, sql, &[], ResultLimits::new(1, 138).unwrap(),)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );

        // A two-byte UTF-8 column name makes this one-column result 52 bytes.
        assert!(
            query_with_limits(
                &connection,
                "SELECT 1 AS \"é\"",
                &[],
                ResultLimits::new(1, 52).unwrap(),
            )
            .is_ok()
        );
        assert_eq!(
            query_with_limits(
                &connection,
                "SELECT 1 AS \"é\"",
                &[],
                ResultLimits::new(1, 51).unwrap(),
            )
            .unwrap_err()
            .kind(),
            EngineErrorKind::LimitExceeded
        );
    }

    #[test]
    fn query_rejects_write_capable_statements_before_they_can_change_sqlite() {
        let connection = Connection::open_in_memory().unwrap();
        execute_batch(
            &connection,
            "CREATE TABLE writes (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO writes (id, value) VALUES (1, 'original');",
        )
        .unwrap();
        let limits = ResultLimits::default();

        for statement in [
            "INSERT INTO writes (id, value) VALUES (2, 'inserted') RETURNING id",
            "UPDATE writes SET value = 'updated' WHERE id = 1 RETURNING id",
            "DELETE FROM writes WHERE id = 1 RETURNING id",
            "CREATE TABLE unexpected (id INTEGER)",
        ] {
            let error = query_with_limits(&connection, statement, &[], limits).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
            assert_eq!(error.to_string(), "query statements must be read-only");
        }

        assert_eq!(
            connection
                .query_row("SELECT COUNT(*), MIN(value) FROM writes", [], |row| Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?
                )),)
                .unwrap(),
            (1, "original".to_owned())
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'unexpected'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn compatibility_query_is_bounded_by_default_row_limits() {
        let connection = Connection::open_in_memory().unwrap();
        let error = query(
            &connection,
            "WITH RECURSIVE numbers(value) AS (
                 VALUES (1)
                 UNION ALL
                 SELECT value + 1 FROM numbers WHERE value < 10001
             )
             SELECT value AS v FROM numbers",
            &[],
        )
        .unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(
            error.to_string(),
            "query result exceeds the configured row limit"
        );
    }

    #[test]
    fn caller_sql_and_parameter_errors_are_classified_without_message_parsing() {
        let connection = Connection::open_in_memory().unwrap();

        assert_eq!(
            query(&connection, "SELECT * FROM missing_table", &[])
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidQuery
        );
        assert_eq!(
            query(&connection, "SELECT ?1", &[]).unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
        assert_eq!(
            execute(&connection, "SELECT 1", &[]).unwrap_err().kind(),
            EngineErrorKind::InvalidQuery
        );
    }

    #[test]
    fn real_sqlite_constraint_failures_keep_their_precise_kinds() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .unwrap();
        execute_batch(
            &connection,
            "CREATE TABLE parents (id INTEGER PRIMARY KEY);
             CREATE TABLE children (
                 id INTEGER PRIMARY KEY,
                 unique_value TEXT UNIQUE,
                 required_value TEXT NOT NULL,
                 checked_value INTEGER CHECK (checked_value > 0),
                 parent_id INTEGER REFERENCES parents(id)
             );",
        )
        .unwrap();
        execute(
            &connection,
            "INSERT INTO children
             (id, unique_value, required_value, checked_value, parent_id)
             VALUES (1, 'duplicate', 'present', 1, NULL)",
            &[],
        )
        .unwrap();

        let cases = [
            (
                "INSERT INTO children
                 (id, unique_value, required_value, checked_value, parent_id)
                 VALUES (2, 'duplicate', 'present', 1, NULL)",
                EngineErrorKind::UniqueViolation,
            ),
            (
                "INSERT INTO children
                 (id, unique_value, required_value, checked_value, parent_id)
                 VALUES (2, 'other', NULL, 1, NULL)",
                EngineErrorKind::NotNullViolation,
            ),
            (
                "INSERT INTO children
                 (id, unique_value, required_value, checked_value, parent_id)
                 VALUES (2, 'other', 'present', 1, 999)",
                EngineErrorKind::ForeignKeyViolation,
            ),
            (
                "INSERT INTO children
                 (id, unique_value, required_value, checked_value, parent_id)
                 VALUES (2, 'other', 'present', 0, NULL)",
                EngineErrorKind::CheckViolation,
            ),
        ];

        for (statement, expected) in cases {
            assert_eq!(
                execute(&connection, statement, &[]).unwrap_err().kind(),
                expected
            );
        }
    }

    #[test]
    fn busy_writes_are_retryable_after_the_competing_transaction_releases() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let first = Connection::open(temp.path()).unwrap();
        let second = Connection::open(temp.path()).unwrap();
        first.busy_timeout(std::time::Duration::ZERO).unwrap();
        second.busy_timeout(std::time::Duration::ZERO).unwrap();
        execute_batch(&first, "CREATE TABLE locks (id INTEGER PRIMARY KEY);").unwrap();
        execute_batch(&first, "BEGIN IMMEDIATE;").unwrap();
        execute(&first, "INSERT INTO locks (id) VALUES (1)", &[]).unwrap();

        let error = execute(&second, "INSERT INTO locks (id) VALUES (2)", &[]).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Busy);
        assert!(error.is_retryable());

        execute_batch(&first, "COMMIT;").unwrap();
        assert_eq!(
            execute(&second, "INSERT INTO locks (id) VALUES (2)", &[]).unwrap(),
            1
        );
    }

    #[test]
    fn execute_and_query_return_ordered_typed_results() {
        let connection = Connection::open_in_memory().unwrap();
        execute_batch(
            &connection,
            "CREATE TABLE values_table (
                id INTEGER PRIMARY KEY,
                enabled INTEGER NOT NULL,
                ratio REAL NOT NULL,
                text_value TEXT NOT NULL,
                blob_value BLOB NOT NULL,
                optional_value TEXT
            );",
        )
        .unwrap();

        assert_eq!(
            execute(
                &connection,
                "INSERT INTO values_table
                 (id, enabled, ratio, text_value, blob_value, optional_value)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                &[
                    Value::from(7_i64),
                    Value::from(true),
                    Value::from(1.5_f64),
                    Value::from("text"),
                    Value::from(vec![0_u8, 255]),
                    Value::Null,
                ],
            )
            .unwrap(),
            1
        );

        let result = query(
            &connection,
            "SELECT id, enabled, ratio, text_value, blob_value, optional_value
             FROM values_table WHERE id = ?1",
            &[Value::from(7_i64)],
        )
        .unwrap();
        assert_eq!(
            result.columns(),
            vec![
                Column::new("id", DataType::Int64),
                Column::new("enabled", DataType::Int64),
                Column::new("ratio", DataType::Float64),
                Column::new("text_value", DataType::Text),
                Column::new("blob_value", DataType::Binary),
                Column::new("optional_value", DataType::Text),
            ]
        );
        assert_eq!(
            result.rows(),
            vec![Row::new(vec![
                Value::from(7_i64),
                Value::from(1_i64),
                Value::from(1.5_f64),
                Value::from("text"),
                Value::from(vec![0_u8, 255]),
                Value::Null,
            ])]
        );
    }

    #[test]
    fn query_preserves_duplicate_column_names_and_positions() {
        let connection = Connection::open_in_memory().unwrap();
        let result = query(
            &connection,
            "SELECT 1 AS duplicate, 2 AS middle, 3 AS duplicate, 4 AS \"\"",
            &[],
        )
        .unwrap();

        assert_eq!(
            result.columns(),
            [
                Column::new("duplicate", DataType::Unknown),
                Column::new("middle", DataType::Unknown),
                Column::new("duplicate", DataType::Unknown),
                Column::new("", DataType::Unknown),
            ]
        );
        assert_eq!(
            result.rows(),
            [Row::new(vec![
                Value::from(1_i64),
                Value::from(2_i64),
                Value::from(3_i64),
                Value::from(4_i64),
            ])]
        );
    }

    #[test]
    fn empty_results_still_return_ordered_duplicate_column_metadata() {
        let connection = Connection::open_in_memory().unwrap();
        let result = query(
            &connection,
            "SELECT 1 AS duplicate, 2 AS duplicate WHERE 0",
            &[],
        )
        .unwrap();

        assert_eq!(
            result.columns(),
            [
                Column::new("duplicate", DataType::Unknown),
                Column::new("duplicate", DataType::Unknown),
            ]
        );
        assert!(result.is_empty());
    }
}
