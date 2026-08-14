use briskdb::{DataType, Decimal, Executed, ResultSet, Routed, Value, WriteResult};
use pyo3::{
    conversion::IntoPyObjectExt,
    prelude::*,
    types::{
        PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyMemoryView, PyString,
        PyTuple,
    },
};

use crate::error::{numeric_out_of_range, type_mismatch, unsupported};

pub(crate) fn extract_params(
    py: Python<'_>,
    params: Option<Vec<Py<PyAny>>>,
) -> PyResult<Vec<Value>> {
    params
        .unwrap_or_default()
        .into_iter()
        .map(|value| extract_value(value.bind(py)))
        .collect()
}

fn extract_value(value: &Bound<'_, PyAny>) -> PyResult<Value> {
    if value.is_none() {
        return Ok(Value::Null);
    }
    if value.is_instance_of::<PyBool>() {
        return value.extract::<bool>().map(Value::Boolean);
    }
    if value.is_instance_of::<PyInt>() {
        if let Ok(integer) = value.extract::<i64>() {
            return Ok(Value::Int64(integer));
        }
        if let Ok(integer) = value.extract::<u64>() {
            return Ok(Value::UInt64(integer));
        }
        return Err(numeric_out_of_range(
            "SQL integer parameters must be between -2^63 and 2^64-1",
        ));
    }
    if value.is_instance_of::<PyFloat>() {
        return value.extract::<f64>().map(Value::Float64);
    }
    if value.is_instance_of::<PyString>() {
        return value.extract::<String>().map(Value::Text);
    }
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Ok(Value::Binary(bytes.as_bytes().to_vec()));
    }
    if let Ok(bytes) = value.cast::<PyByteArray>() {
        return Ok(Value::Binary(bytes.to_vec()));
    }
    if let Ok(memory_view) = value.cast::<PyMemoryView>() {
        let bytes = memory_view.call_method0("tobytes")?;
        return Ok(Value::Binary(
            bytes
                .cast::<PyBytes>()
                .map_err(PyErr::from)?
                .as_bytes()
                .to_vec(),
        ));
    }

    let decimal_type = value.py().import("decimal")?.getattr("Decimal")?;
    if value.is_instance(&decimal_type)? {
        let representation = value.str()?.extract::<String>()?;
        return Decimal::parse(representation)
            .map(Value::Decimal)
            .map_err(|_| {
                unsupported("non-finite Decimal values have no lossless SQL representation")
            });
    }

    let type_name = value
        .get_type()
        .fully_qualified_name()?
        .extract::<String>()?;
    Err(type_mismatch(format!(
        "SQL parameters do not support values of type {type_name}"
    )))
}

pub(crate) fn value_to_python(py: Python<'_>, value: Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Boolean(value) => value.into_py_any(py),
        Value::Int64(value) => value.into_py_any(py),
        Value::UInt64(value) => value.into_py_any(py),
        Value::Float64(value) => value.into_py_any(py),
        Value::Decimal(value) => py
            .import("decimal")?
            .getattr("Decimal")?
            .call1((value.into_string(),))
            .map(|value| value.unbind()),
        Value::Text(value) => value.into_py_any(py),
        Value::InvalidText(value) | Value::Binary(value) => {
            Ok(PyBytes::new(py, &value).into_any().unbind())
        }
    }
}

pub(crate) fn data_type_name(data_type: DataType) -> &'static str {
    match data_type {
        DataType::Unknown => "unknown",
        DataType::Null => "null",
        DataType::Boolean => "boolean",
        DataType::Int64 => "int64",
        DataType::UInt64 => "uint64",
        DataType::Float64 => "float64",
        DataType::Decimal => "decimal",
        DataType::Text => "text",
        DataType::Binary => "binary",
    }
}

fn result_set_to_python(
    py: Python<'_>,
    shards: Vec<u16>,
    result: ResultSet,
) -> PyResult<Py<PyAny>> {
    let output = PyDict::new(py);
    output.set_item("shards", shards)?;

    let (columns, rows) = result.into_parts();
    let python_columns = PyList::empty(py);
    for column in columns {
        let item = PyDict::new(py);
        item.set_item("name", column.name)?;
        item.set_item("type", data_type_name(column.data_type))?;
        python_columns.append(item)?;
    }
    output.set_item("columns", python_columns)?;

    let python_rows = PyList::empty(py);
    for row in rows {
        let values = row
            .into_values()
            .into_iter()
            .map(|value| value_to_python(py, value))
            .collect::<PyResult<Vec<_>>>()?;
        python_rows.append(PyTuple::new(py, values)?)?;
    }
    output.set_item("rows", python_rows)?;
    Ok(output.into_any().unbind())
}

pub(crate) fn routed_result_to_python(
    py: Python<'_>,
    result: Routed<ResultSet>,
) -> PyResult<Py<PyAny>> {
    result_set_to_python(py, vec![result.shard], result.value)
}

pub(crate) fn logical_result_to_python(
    py: Python<'_>,
    result: Executed<ResultSet>,
) -> PyResult<Py<PyAny>> {
    result_set_to_python(py, result.shards, result.value)
}

pub(crate) fn write_result_to_python(
    py: Python<'_>,
    result: Routed<WriteResult>,
) -> PyResult<Py<PyAny>> {
    let output = PyDict::new(py);
    output.set_item("shard", result.shard)?;
    output.set_item("rows_affected", result.value.rows_affected)?;
    match result.value.generated_key {
        Some(key) => {
            let generated = PyDict::new(py);
            generated.set_item("column", key.column)?;
            generated.set_item("value", value_to_python(py, key.value)?)?;
            output.set_item("generated_key", generated)?;
        }
        None => output.set_item("generated_key", py.None())?,
    }
    Ok(output.into_any().unbind())
}

#[cfg(test)]
mod tests {
    use briskdb::CanonicalIndexKey;
    use pyo3::{conversion::IntoPyObjectExt, types::PyBytes};

    use super::*;

    #[test]
    fn python_parameters_use_the_shared_canonical_index_key_encoding() {
        Python::initialize();
        Python::attach(|py| {
            let params = vec![
                true.into_py_any(py).unwrap(),
                (-42_i64).into_py_any(py).unwrap(),
                9_223_372_036_854_775_809_u64.into_py_any(py).unwrap(),
                1.5_f64.into_py_any(py).unwrap(),
                "shared".into_py_any(py).unwrap(),
                PyBytes::new(py, &[0, 255]).into_any().unbind(),
            ];
            let through_python = extract_params(py, Some(params)).unwrap();
            let direct = [
                Value::Boolean(true),
                Value::Int64(-42),
                Value::UInt64(9_223_372_036_854_775_809),
                Value::Float64(1.5),
                Value::Text("shared".to_owned()),
                Value::Binary(vec![0, 255]),
            ];
            assert_eq!(
                CanonicalIndexKey::encode_values(&through_python).unwrap(),
                CanonicalIndexKey::encode_values(&direct).unwrap()
            );
        });
    }
}
