//! Protocol-neutral SQL parsing, SQLite execution, and value conversion.
//!
//! The parser facade produces a bounded, dialect-explicit opaque AST. The
//! common-subset validator recursively admits only protocol-neutral SQL shapes,
//! and the placeholder normalizer produces a separate source-preserving SQLite
//! parameter representation for later planning work. Current execution remains
//! deliberate SQLite pass-through; these opt-in layers do not gate, route, or
//! execute statements.

mod dml;
mod inference;
mod normalizer;
mod parser;
mod subset;
mod translator;

pub(crate) use dml::{RoutedDml, routed_dml_shape};

pub use inference::{ShardKeyInference, ShardKeyInferenceKind, ShardKeyValue, infer_shard_keys};
pub use normalizer::{
    MAX_SQL_PARAMETERS, NormalizedSql, StatementParameters, normalize_placeholders,
};
pub use parser::{
    MAX_PARSED_SQL_BYTES, MAX_PARSED_SQL_STATEMENTS, ParsedSql, SQL_PARSE_RECURSION_LIMIT,
    SqlDialect, parse,
};
pub use subset::{CommonSql, MAX_COMMON_SQL_EXPRESSION_DEPTH, validate_common_subset};
pub use translator::{SqlTranslationMode, TranslatedSql, translate_sql};

use rusqlite::{
    Connection, params_from_iter,
    types::{Value as SqlValue, ValueRef},
};

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

pub(crate) fn execute(
    connection: &Connection,
    statement: &str,
    params: &[Value],
) -> EngineResult<usize> {
    let params = params
        .iter()
        .map(value_to_sql)
        .collect::<EngineResult<Vec<_>>>()?;
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
    let params = params
        .iter()
        .map(value_to_sql)
        .collect::<EngineResult<Vec<_>>>()?;
    let mut statement = connection
        .prepare(statement)
        .map_err(sqlite_error::statement)?;
    if !statement.readonly() {
        return Err(EngineError::new(
            EngineErrorKind::InvalidQuery,
            "query statements must be read-only",
        ));
    }

    let column_names = statement.column_names();
    let mut logical_bytes = account_bytes(0, RESULT_ENVELOPE_BYTES, limits.max_bytes())?;
    for name in &column_names {
        logical_bytes = account_bytes(logical_bytes, TYPE_TAG_BYTES, limits.max_bytes())?;
        logical_bytes = account_bytes(logical_bytes, LENGTH_BYTES, limits.max_bytes())?;
        logical_bytes =
            account_bytes(logical_bytes, usize_to_u64(name.len())?, limits.max_bytes())?;
    }
    let columns = column_names
        .into_iter()
        .map(|name| Column::new(name, DataType::Unknown))
        .collect::<Vec<_>>();
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
                Column::new("id", DataType::Unknown),
                Column::new("enabled", DataType::Unknown),
                Column::new("ratio", DataType::Unknown),
                Column::new("text_value", DataType::Unknown),
                Column::new("blob_value", DataType::Unknown),
                Column::new("optional_value", DataType::Unknown),
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
