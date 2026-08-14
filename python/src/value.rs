use briskdb::{DataType, Executed, ResultSet, Routed, Value, WriteResult};
use pyo3::{
    conversion::IntoPyObjectExt,
    exceptions::PyTypeError,
    prelude::*,
    types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple},
};

use crate::error::invalid_value;

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
        return value.extract::<i64>().map(Value::Int64).map_err(|_| {
            invalid_value("SQL integer parameters must fit in a signed 64-bit integer")
        });
    }
    if value.is_instance_of::<PyFloat>() {
        let number = value.extract::<f64>()?;
        if !number.is_finite() {
            return Err(invalid_value("SQL float parameters must be finite"));
        }
        return Ok(Value::Float64(number));
    }
    if value.is_instance_of::<PyString>() {
        return value.extract::<String>().map(Value::Text);
    }
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Ok(Value::Binary(bytes.as_bytes().to_vec()));
    }

    Err(PyTypeError::new_err(
        "SQL parameters currently support None, bool, int64, finite float, str, and bytes",
    ))
}

fn value_to_python(py: Python<'_>, value: Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Boolean(value) => value.into_py_any(py),
        Value::Int64(value) => value.into_py_any(py),
        Value::UInt64(value) => value.into_py_any(py),
        Value::Float64(value) => value.into_py_any(py),
        Value::Decimal(value) => value.into_string().into_py_any(py),
        Value::Text(value) => value.into_py_any(py),
        Value::InvalidText(value) | Value::Binary(value) => {
            Ok(PyBytes::new(py, &value).into_any().unbind())
        }
    }
}

fn data_type_name(data_type: DataType) -> &'static str {
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
