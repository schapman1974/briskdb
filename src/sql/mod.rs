//! SQLite SQL execution and value conversion.
//!
//! SQL remains deliberate SQLite pass-through during this phase. Parsing and
//! dialect normalization are later roadmap work.

use rusqlite::{
    Connection, params_from_iter,
    types::{Value as SqlValue, ValueRef},
};

use crate::core::{Column, DataType, ResultSet, Row, Value};

pub(crate) fn execute(
    connection: &Connection,
    statement: &str,
    params: &[Value],
) -> anyhow::Result<usize> {
    let params = params
        .iter()
        .map(value_to_sql)
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(connection.execute(statement, params_from_iter(params))?)
}

pub(crate) fn query(
    connection: &Connection,
    statement: &str,
    params: &[Value],
) -> anyhow::Result<ResultSet> {
    let params = params
        .iter()
        .map(value_to_sql)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut statement = connection.prepare(statement)?;
    let columns = statement
        .column_names()
        .into_iter()
        .map(|name| Column::new(name, DataType::Unknown))
        .collect::<Vec<_>>();
    let mut sqlite_rows = statement.query(params_from_iter(params))?;
    let mut rows = Vec::new();

    while let Some(sqlite_row) = sqlite_rows.next()? {
        let mut values = Vec::with_capacity(columns.len());
        for index in 0..columns.len() {
            values.push(sql_to_value(sqlite_row.get_ref(index)?));
        }
        rows.push(Row::new(values));
    }
    Ok(ResultSet::new(columns, rows)?)
}

pub(crate) fn execute_batch(connection: &Connection, statement: &str) -> anyhow::Result<()> {
    connection.execute_batch(statement)?;
    Ok(())
}

fn value_to_sql(value: &Value) -> anyhow::Result<SqlValue> {
    Ok(match value {
        Value::Null => SqlValue::Null,
        Value::Boolean(value) => SqlValue::Integer(i64::from(*value)),
        Value::Int64(value) => SqlValue::Integer(*value),
        Value::UInt64(value) => SqlValue::Integer(i64::try_from(*value).map_err(|_| {
            anyhow::anyhow!("unsigned integer {value} exceeds SQLite INTEGER range")
        })?),
        Value::Float64(value) if value.is_nan() => {
            anyhow::bail!("NaN has no lossless SQLite binding because SQLite converts it to NULL")
        }
        Value::Float64(value) => SqlValue::Real(*value),
        Value::Decimal(value) => {
            anyhow::bail!("decimal value {value} has no lossless SQLite binding")
        }
        Value::Text(value) => SqlValue::Text(value.clone()),
        Value::InvalidText(_) => {
            anyhow::bail!("non-UTF-8 text has no lossless SQLite binding")
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
        assert_eq!(
            value_to_sql(&Value::from(too_large))
                .unwrap_err()
                .to_string(),
            format!("unsigned integer {too_large} exceeds SQLite INTEGER range")
        );
        assert_eq!(
            value_to_sql(&Value::decimal("12.3400").unwrap())
                .unwrap_err()
                .to_string(),
            "decimal value 12.3400 has no lossless SQLite binding"
        );
        assert_eq!(
            value_to_sql(&Value::InvalidText(vec![0x80]))
                .unwrap_err()
                .to_string(),
            "non-UTF-8 text has no lossless SQLite binding"
        );
        assert_eq!(
            value_to_sql(&Value::from(f64::NAN))
                .unwrap_err()
                .to_string(),
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

        assert_eq!(
            error.to_string(),
            "NaN has no lossless SQLite binding because SQLite converts it to NULL"
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
