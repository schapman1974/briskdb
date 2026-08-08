//! SQLite SQL execution and value conversion.
//!
//! SQL remains deliberate SQLite pass-through during this phase. Parsing and
//! dialect normalization are later roadmap work.

use rusqlite::{
    Connection, params_from_iter,
    types::{Value as SqlValue, ValueRef},
};

use crate::{
    core::{Column, DataType, EngineError, EngineErrorKind, EngineResult, ResultSet, Row, Value},
    sqlite_error,
};

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
    let params = params
        .iter()
        .map(value_to_sql)
        .collect::<EngineResult<Vec<_>>>()?;
    let mut statement = connection
        .prepare(statement)
        .map_err(sqlite_error::statement)?;
    let columns = statement
        .column_names()
        .into_iter()
        .map(|name| Column::new(name, DataType::Unknown))
        .collect::<Vec<_>>();
    let mut sqlite_rows = statement
        .query(params_from_iter(params))
        .map_err(sqlite_error::statement)?;
    let mut rows = Vec::new();

    while let Some(sqlite_row) = sqlite_rows.next().map_err(sqlite_error::statement)? {
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
