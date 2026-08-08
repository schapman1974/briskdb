//! SQLite SQL execution and value conversion.
//!
//! SQL remains deliberate SQLite pass-through during this phase. Parsing and
//! dialect normalization are later roadmap work.

use rusqlite::{
    Connection, params_from_iter,
    types::{Value as SqlValue, ValueRef},
};
use serde_json::{Map, Value, json};

pub(crate) fn execute(
    connection: &Connection,
    statement: &str,
    params: &[Value],
) -> anyhow::Result<usize> {
    let params = params.iter().map(json_to_sql).collect::<Vec<_>>();
    Ok(connection.execute(statement, params_from_iter(params))?)
}

pub(crate) fn query(
    connection: &Connection,
    statement: &str,
    params: &[Value],
) -> anyhow::Result<Vec<Value>> {
    let params = params.iter().map(json_to_sql).collect::<Vec<_>>();
    let mut statement = connection.prepare(statement)?;
    let column_names = statement
        .column_names()
        .into_iter()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let mut rows = statement.query(params_from_iter(params))?;
    let mut result = Vec::new();

    while let Some(row) = rows.next()? {
        let mut object = Map::with_capacity(column_names.len());
        for (index, name) in column_names.iter().enumerate() {
            object.insert(name.clone(), sql_to_json(row.get_ref(index)?));
        }
        result.push(Value::Object(object));
    }
    Ok(result)
}

pub(crate) fn execute_batch(connection: &Connection, statement: &str) -> anyhow::Result<()> {
    connection.execute_batch(statement)?;
    Ok(())
}

fn json_to_sql(value: &Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Bool(value) => SqlValue::Integer(i64::from(*value)),
        Value::Number(value) => value
            .as_i64()
            .map(SqlValue::Integer)
            .or_else(|| value.as_f64().map(SqlValue::Real))
            .unwrap_or_else(|| SqlValue::Text(value.to_string())),
        Value::String(value) => SqlValue::Text(value.clone()),
        Value::Array(_) | Value::Object(_) => SqlValue::Text(value.to_string()),
    }
}

fn sql_to_json(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => json!(value),
        ValueRef::Real(value) => json!(value),
        ValueRef::Text(value) => Value::String(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => Value::Array(value.iter().map(|byte| json!(byte)).collect()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_number_conversion_matches_the_http_compatibility_contract() {
        assert_eq!(json_to_sql(&json!(42)), SqlValue::Integer(42));
        assert_eq!(json_to_sql(&json!(1.5)), SqlValue::Real(1.5));

        let above_signed_i64_range = json!(9_223_372_036_854_775_809_u64);
        assert_eq!(
            json_to_sql(&above_signed_i64_range),
            SqlValue::Real(9_223_372_036_854_775_808.0)
        );
        assert!(serde_json::from_str::<Value>("1e400").is_err());
    }

    #[test]
    fn sqlite_text_with_invalid_utf8_is_converted_lossily() {
        assert_eq!(
            sql_to_json(ValueRef::Text(&[b'f', 0x80])),
            Value::String("f\u{fffd}".to_owned())
        );
    }

    #[test]
    fn executes_and_queries_with_json_parameters() {
        let connection = Connection::open_in_memory().unwrap();
        execute_batch(
            &connection,
            "CREATE TABLE values_table (id INTEGER PRIMARY KEY, value TEXT NOT NULL);",
        )
        .unwrap();

        assert_eq!(
            execute(
                &connection,
                "INSERT INTO values_table (id, value) VALUES (?1, ?2)",
                &[json!(7), json!({"nested": true})],
            )
            .unwrap(),
            1
        );
        assert_eq!(
            query(
                &connection,
                "SELECT id, value FROM values_table WHERE id = ?1",
                &[json!(7)],
            )
            .unwrap(),
            vec![json!({"id": 7, "value": "{\"nested\":true}"})]
        );
    }
}
