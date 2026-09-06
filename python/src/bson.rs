use std::{collections::HashSet, mem::size_of};

use briskdb::document::{
    BSON_MAX_DECODED_BYTES, BSON_MAX_DOCUMENT_BYTES, BSON_MAX_NESTING_DEPTH, BsonBinary,
    BsonDateTime, BsonDecimal128, BsonDocument, BsonJavaScript, BsonObjectId, BsonRegex,
    BsonTimestamp, BsonUuid, BsonValue, DocumentRequestId, UuidRepresentation, encode_document,
};
use pyo3::{
    conversion::IntoPyObjectExt,
    prelude::*,
    types::{
        PyAny, PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyMapping,
        PyMemoryView, PyString, PyStringMethods, PyTuple,
    },
};

use crate::error::{
    bson_error_to_python, invalid_text_encoding, invalid_value, limit_exceeded,
    numeric_out_of_range, type_mismatch, unsupported,
};

const MISSING_BSON: &str = "Python document operations require the optional pymongo package";
const INVALID_BSON_MAPPING: &str = "BSON documents require an ordered mapping with string keys";
const INVALID_BSON_VALUE: &str = "unsupported Python value in BSON document";
const INVALID_BSON_CONTAINER: &str = "unable to read Python BSON container";
const INVALID_BSON_ATTRIBUTE: &str = "unable to read Python BSON value";
const BSON_CYCLE: &str = "BSON documents cannot contain cyclic containers";
const BSON_DEPTH: &str = "BSON container depth exceeds the limit of 100";
const BSON_SIZE: &str = "BSON document exceeds the 16777216-byte limit";
const BSON_HEAP: &str = "decoded BSON exceeds the 67108864-byte allocation budget";
const INVALID_BSON_TEXT: &str =
    "BSON strings and field names must contain valid Unicode scalar values";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PythonUuidRepresentation {
    Unspecified,
    Standard,
    PythonLegacy,
    JavaLegacy,
    CSharpLegacy,
}

impl PythonUuidRepresentation {
    const fn into_brisk(self) -> Option<UuidRepresentation> {
        match self {
            Self::Unspecified => None,
            Self::Standard => Some(UuidRepresentation::Standard),
            Self::PythonLegacy => Some(UuidRepresentation::PythonLegacy),
            Self::JavaLegacy => Some(UuidRepresentation::JavaLegacy),
            Self::CSharpLegacy => Some(UuidRepresentation::CSharpLegacy),
        }
    }
}

pub(crate) fn parse_uuid_representation(value: &str) -> PyResult<PythonUuidRepresentation> {
    match value {
        "unspecified" => Ok(PythonUuidRepresentation::Unspecified),
        "standard" => Ok(PythonUuidRepresentation::Standard),
        "python_legacy" => Ok(PythonUuidRepresentation::PythonLegacy),
        "java_legacy" => Ok(PythonUuidRepresentation::JavaLegacy),
        "csharp_legacy" => Ok(PythonUuidRepresentation::CSharpLegacy),
        _ => Err(crate::error::invalid_value(
            "uuid_representation must be one of: unspecified, standard, python_legacy, java_legacy, csharp_legacy",
        )),
    }
}

pub(crate) fn ensure_bson_available(py: Python<'_>) -> PyResult<PythonBsonOutputTypes> {
    PythonBsonTypes::load(py)?.into_output_types()
}

pub(crate) fn extract_bson_document(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    representation: PythonUuidRepresentation,
) -> PyResult<BsonDocument> {
    InputConverter::new(PythonBsonTypes::load(py)?, representation).document(value)
}

pub(crate) fn bson_document_to_python(
    py: Python<'_>,
    document: &BsonDocument,
    representation: PythonUuidRepresentation,
    types: &PythonBsonOutputTypes,
) -> PyResult<Py<PyAny>> {
    OutputConverter::new(py, types, representation).document(document)
}

pub(crate) fn bson_value_to_python(
    py: Python<'_>,
    value: &BsonValue,
    representation: PythonUuidRepresentation,
    types: &PythonBsonOutputTypes,
) -> PyResult<Py<PyAny>> {
    OutputConverter::new(py, types, representation).value(value)
}

pub(crate) fn extract_request_id(value: &Bound<'_, PyAny>) -> PyResult<DocumentRequestId> {
    DocumentRequestId::new(extract_uuid_bytes(value, "request_id")?)
        .map_err(crate::error::engine_error_to_python)
}

pub(crate) fn extract_uuid_bytes(
    value: &Bound<'_, PyAny>,
    label: &'static str,
) -> PyResult<[u8; 16]> {
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return bytes
            .as_bytes()
            .try_into()
            .map_err(|_| type_mismatch(format!("{label} must be a uuid.UUID or 16-byte value")));
    }
    let uuid = value
        .py()
        .import("uuid")
        .and_then(|module| module.getattr("UUID"))
        .map_err(|_| invalid_value("unable to load Python UUID support"))?;
    if !value
        .is_instance(&uuid)
        .map_err(|_| invalid_value("unable to inspect Python UUID value"))?
    {
        return Err(type_mismatch(format!(
            "{label} must be a uuid.UUID or 16-byte value"
        )));
    }
    required_uuid_bytes(value)
}

pub(crate) fn uuid_bytes_to_python(
    py: Python<'_>,
    bytes: [u8; 16],
    types: &PythonBsonOutputTypes,
) -> PyResult<Py<PyAny>> {
    uuid_to_python(py, types.uuid.bind(py), bytes)
}

struct PythonBsonTypes<'py> {
    binary: Bound<'py, PyAny>,
    code: Bound<'py, PyAny>,
    datetime_ms: Bound<'py, PyAny>,
    decimal128: Bound<'py, PyAny>,
    int64: Bound<'py, PyAny>,
    max_key: Bound<'py, PyAny>,
    min_key: Bound<'py, PyAny>,
    object_id: Bound<'py, PyAny>,
    regex: Bound<'py, PyAny>,
    timestamp: Bound<'py, PyAny>,
    datetime: Bound<'py, PyAny>,
    timedelta: Bound<'py, PyAny>,
    timezone_utc: Bound<'py, PyAny>,
    regex_pattern: Bound<'py, PyAny>,
    uuid: Bound<'py, PyAny>,
}

pub(crate) struct PythonBsonOutputTypes {
    binary: Py<PyAny>,
    code: Py<PyAny>,
    datetime_ms: Py<PyAny>,
    decimal128_from_bid: Py<PyAny>,
    int64: Py<PyAny>,
    max_key: Py<PyAny>,
    min_key: Py<PyAny>,
    object_id: Py<PyAny>,
    regex: Py<PyAny>,
    timestamp: Py<PyAny>,
    datetime: Py<PyAny>,
    timedelta: Py<PyAny>,
    timezone_utc: Py<PyAny>,
    uuid: Py<PyAny>,
}

impl<'py> PythonBsonTypes<'py> {
    fn load(py: Python<'py>) -> PyResult<Self> {
        let bson = py.import("bson").map_err(|_| unsupported(MISSING_BSON))?;
        let attr = |name| bson.getattr(name).map_err(|_| unsupported(MISSING_BSON));
        let datetime_module = py
            .import("datetime")
            .map_err(|_| unsupported(MISSING_BSON))?;
        let re_module = py.import("re").map_err(|_| unsupported(MISSING_BSON))?;
        let uuid_module = py.import("uuid").map_err(|_| unsupported(MISSING_BSON))?;
        Ok(Self {
            binary: attr("Binary")?,
            code: attr("Code")?,
            datetime_ms: attr("DatetimeMS")?,
            decimal128: attr("Decimal128")?,
            int64: attr("Int64")?,
            max_key: attr("MaxKey")?,
            min_key: attr("MinKey")?,
            object_id: attr("ObjectId")?,
            regex: attr("Regex")?,
            timestamp: attr("Timestamp")?,
            datetime: datetime_module
                .getattr("datetime")
                .map_err(|_| unsupported(MISSING_BSON))?,
            timedelta: datetime_module
                .getattr("timedelta")
                .map_err(|_| unsupported(MISSING_BSON))?,
            timezone_utc: datetime_module
                .getattr("timezone")
                .and_then(|timezone| timezone.getattr("utc"))
                .map_err(|_| unsupported(MISSING_BSON))?,
            regex_pattern: re_module
                .getattr("Pattern")
                .map_err(|_| unsupported(MISSING_BSON))?,
            uuid: uuid_module
                .getattr("UUID")
                .map_err(|_| unsupported(MISSING_BSON))?,
        })
    }

    fn into_output_types(self) -> PyResult<PythonBsonOutputTypes> {
        let decimal128_from_bid = self
            .decimal128
            .getattr("from_bid")
            .map_err(|_| unsupported(MISSING_BSON))?
            .unbind();
        Ok(PythonBsonOutputTypes {
            binary: self.binary.unbind(),
            code: self.code.unbind(),
            datetime_ms: self.datetime_ms.unbind(),
            decimal128_from_bid,
            int64: self.int64.unbind(),
            max_key: self.max_key.unbind(),
            min_key: self.min_key.unbind(),
            object_id: self.object_id.unbind(),
            regex: self.regex.unbind(),
            timestamp: self.timestamp.unbind(),
            datetime: self.datetime.unbind(),
            timedelta: self.timedelta.unbind(),
            timezone_utc: self.timezone_utc.unbind(),
            uuid: self.uuid.unbind(),
        })
    }
}

struct ConversionBudget {
    wire_remaining: usize,
    heap_remaining: usize,
}

impl ConversionBudget {
    const fn new() -> Self {
        Self {
            wire_remaining: BSON_MAX_DOCUMENT_BYTES,
            heap_remaining: BSON_MAX_DECODED_BYTES,
        }
    }

    fn wire(&mut self, bytes: usize) -> PyResult<()> {
        self.wire_remaining = self
            .wire_remaining
            .checked_sub(bytes)
            .ok_or_else(|| limit_exceeded(BSON_SIZE))?;
        Ok(())
    }

    fn heap(&mut self, bytes: usize) -> PyResult<()> {
        self.heap_remaining = self
            .heap_remaining
            .checked_sub(bytes)
            .ok_or_else(|| limit_exceeded(BSON_HEAP))?;
        Ok(())
    }
}

struct InputConverter<'py> {
    types: PythonBsonTypes<'py>,
    representation: PythonUuidRepresentation,
    active: HashSet<usize>,
    budget: ConversionBudget,
}

impl<'py> InputConverter<'py> {
    fn new(types: PythonBsonTypes<'py>, representation: PythonUuidRepresentation) -> Self {
        Self {
            types,
            representation,
            active: HashSet::new(),
            budget: ConversionBudget::new(),
        }
    }

    fn document(mut self, value: &Bound<'py, PyAny>) -> PyResult<BsonDocument> {
        let document = self.convert_document(value, 1)?;
        encode_document(&document).map_err(bson_error_to_python)?;
        Ok(document)
    }

    fn convert_document(
        &mut self,
        value: &Bound<'py, PyAny>,
        depth: usize,
    ) -> PyResult<BsonDocument> {
        self.ensure_depth(depth)?;
        let mapping = value
            .cast::<PyMapping>()
            .map_err(|_| type_mismatch(INVALID_BSON_MAPPING))?;
        self.enter(value)?;
        self.budget.wire(5)?;

        let result = (|| {
            let mut document = BsonDocument::new();
            let mut names: HashSet<String> = HashSet::new();
            let iterator = mapping
                .try_iter()
                .map_err(|_| invalid_value(INVALID_BSON_CONTAINER))?;
            for key in iterator {
                let key = key.map_err(|_| invalid_value(INVALID_BSON_CONTAINER))?;
                let key_string = key
                    .cast::<PyString>()
                    .map_err(|_| type_mismatch(INVALID_BSON_MAPPING))?;
                let key_len = self.preflight_string(key_string, 2)?;
                self.budget.wire(
                    key_len
                        .checked_add(2)
                        .ok_or_else(|| limit_exceeded(BSON_SIZE))?,
                )?;
                self.budget.heap(
                    size_of::<(String, BsonValue)>()
                        .saturating_mul(2)
                        .saturating_add(size_of::<&str>().saturating_mul(8))
                        .saturating_add(key_len.saturating_mul(2)),
                )?;
                let key_text = key_string
                    .to_cow()
                    .map_err(|_| invalid_text_encoding(INVALID_BSON_TEXT))?;
                if key_text.contains('\0') {
                    return Err(invalid_value("BSON field names cannot contain NUL"));
                }
                debug_assert_eq!(key_text.len(), key_len);
                let field = mapping
                    .get_item(&key)
                    .map_err(|_| invalid_value(INVALID_BSON_CONTAINER))?;
                let key_text = key_text.into_owned();
                names
                    .try_reserve(1)
                    .map_err(|_| limit_exceeded(BSON_HEAP))?;
                if !names.insert(key_text.clone()) {
                    return Err(invalid_value(
                        "BSON documents cannot contain duplicate field names",
                    ));
                }
                let field = self.convert_value(&field, depth)?;
                document
                    .push(key_text, field)
                    .map_err(bson_error_to_python)?;
            }
            Ok(document)
        })();
        self.leave(value);
        result
    }

    fn convert_array(
        &mut self,
        value: &Bound<'py, PyAny>,
        depth: usize,
    ) -> PyResult<Vec<BsonValue>> {
        self.ensure_depth(depth)?;
        self.enter(value)?;
        self.budget.wire(5)?;
        let result = (|| {
            let len = value
                .len()
                .map_err(|_| invalid_value(INVALID_BSON_CONTAINER))?;
            self.budget
                .heap(size_of::<BsonValue>().saturating_mul(len).saturating_mul(2))?;
            let mut values = Vec::new();
            values
                .try_reserve_exact(len)
                .map_err(|_| limit_exceeded(BSON_HEAP))?;
            for index in 0..len {
                let index_bytes = decimal_digits(index);
                self.budget.wire(index_bytes.saturating_add(2))?;
                let item = value
                    .get_item(index)
                    .map_err(|_| invalid_value(INVALID_BSON_CONTAINER))?;
                values.push(self.convert_value(&item, depth)?);
            }
            Ok(values)
        })();
        self.leave(value);
        result
    }

    fn convert_value(
        &mut self,
        value: &Bound<'py, PyAny>,
        parent_depth: usize,
    ) -> PyResult<BsonValue> {
        if value.is_none() {
            return Ok(BsonValue::Null);
        }
        if value.is_instance_of::<PyBool>() {
            self.budget.wire(1)?;
            return value.extract::<bool>().map(BsonValue::Boolean);
        }
        if value
            .is_instance(&self.types.int64)
            .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?
        {
            self.budget.wire(8)?;
            return value.extract::<i64>().map(BsonValue::Int64).map_err(|_| {
                numeric_out_of_range("BSON Int64 values must be between -2^63 and 2^63-1")
            });
        }
        if value.is_instance_of::<PyInt>() {
            if let Ok(integer) = value.extract::<i32>() {
                self.budget.wire(4)?;
                return Ok(BsonValue::Int32(integer));
            }
            self.budget.wire(8)?;
            return value.extract::<i64>().map(BsonValue::Int64).map_err(|_| {
                numeric_out_of_range("BSON integer values must be between -2^63 and 2^63-1")
            });
        }
        if value.is_instance_of::<PyFloat>() {
            self.budget.wire(8)?;
            return value.extract::<f64>().map(BsonValue::Double);
        }
        if value
            .is_instance(&self.types.code)
            .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?
        {
            return self.convert_code(value, parent_depth);
        }
        if value
            .is_instance(&self.types.binary)
            .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?
        {
            return self.convert_binary(value);
        }
        if value.is_instance_of::<PyString>() {
            return self.convert_string(value).map(BsonValue::String);
        }
        if let Ok(bytes) = value.cast::<PyBytes>() {
            return self.copy_binary(bytes.as_bytes(), 0);
        }
        if let Ok(bytes) = value.cast::<PyByteArray>() {
            self.ensure_binary_len(bytes.len(), 0)?;
            return Ok(BsonValue::Binary(BsonBinary::new(0, bytes.to_vec())));
        }
        if let Ok(memory_view) = value.cast::<PyMemoryView>() {
            let len = memory_view
                .getattr("nbytes")
                .and_then(|length| length.extract::<usize>())
                .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?;
            self.ensure_binary_len(len, 0)?;
            let bytes = memory_view
                .call_method0("tobytes")
                .and_then(|bytes| bytes.cast_into::<PyBytes>().map_err(PyErr::from))
                .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?;
            return self.copy_binary_after_budget(bytes.as_bytes(), 0);
        }
        if value
            .is_instance(&self.types.datetime_ms)
            .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?
        {
            self.budget.wire(8)?;
            let integer = value
                .call_method0("__int__")
                .and_then(|integer| integer.extract::<i64>())
                .map_err(|_| {
                    numeric_out_of_range("BSON DatetimeMS values must be between -2^63 and 2^63-1")
                })?;
            return Ok(BsonValue::DateTime(BsonDateTime::from_millis(integer)));
        }
        if value
            .is_instance(&self.types.datetime)
            .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?
        {
            self.budget.wire(8)?;
            return self
                .datetime_millis(value)
                .map(BsonDateTime::from_millis)
                .map(BsonValue::DateTime);
        }
        if value
            .is_instance(&self.types.uuid)
            .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?
        {
            let Some(representation) = self.representation.into_brisk() else {
                return Err(unsupported(
                    "native UUID values require an explicit uuid_representation",
                ));
            };
            self.budget.wire(21)?;
            return Ok(BsonValue::Uuid(BsonUuid::new(
                required_uuid_bytes(value)?,
                representation,
            )));
        }
        if value
            .is_instance(&self.types.object_id)
            .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?
        {
            self.budget.wire(12)?;
            let bytes = required_fixed_bytes(&value.getattr("binary"), 12, INVALID_BSON_ATTRIBUTE)?;
            return Ok(BsonValue::ObjectId(BsonObjectId::from_bytes(bytes)));
        }
        if value
            .is_instance(&self.types.decimal128)
            .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?
        {
            self.budget.wire(16)?;
            let bid = required_fixed_bytes(&value.getattr("bid"), 16, INVALID_BSON_ATTRIBUTE)?;
            return Ok(BsonValue::Decimal128(BsonDecimal128::from_bid(bid)));
        }
        if value
            .is_instance(&self.types.regex)
            .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?
            || value
                .is_instance(&self.types.regex_pattern)
                .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?
        {
            return self.convert_regex(value);
        }
        if value
            .is_instance(&self.types.timestamp)
            .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?
        {
            self.budget.wire(8)?;
            let time = extract_attr::<u32>(value, "time")?;
            let increment = extract_attr::<u32>(value, "inc")?;
            return Ok(BsonValue::Timestamp(BsonTimestamp::new(time, increment)));
        }
        if value
            .is_instance(&self.types.min_key)
            .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?
        {
            return Ok(BsonValue::MinKey);
        }
        if value
            .is_instance(&self.types.max_key)
            .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?
        {
            return Ok(BsonValue::MaxKey);
        }
        if value.cast::<PyMapping>().is_ok() {
            return self
                .convert_document(value, parent_depth + 1)
                .map(BsonValue::Document);
        }
        if value.cast::<PyList>().is_ok() || value.cast::<PyTuple>().is_ok() {
            return self
                .convert_array(value, parent_depth + 1)
                .map(BsonValue::Array);
        }
        Err(type_mismatch(INVALID_BSON_VALUE))
    }

    fn convert_string(&mut self, value: &Bound<'py, PyAny>) -> PyResult<String> {
        let string = value
            .cast::<PyString>()
            .map_err(|_| type_mismatch(INVALID_BSON_VALUE))?;
        let string_len = self.preflight_string(string, 5)?;
        self.budget.wire(
            string_len
                .checked_add(5)
                .ok_or_else(|| limit_exceeded(BSON_SIZE))?,
        )?;
        self.budget.heap(string_len)?;
        let string = string
            .to_cow()
            .map_err(|_| invalid_text_encoding(INVALID_BSON_TEXT))?;
        debug_assert_eq!(string.len(), string_len);
        Ok(string.into_owned())
    }

    fn convert_binary(&mut self, value: &Bound<'py, PyAny>) -> PyResult<BsonValue> {
        let subtype = extract_attr::<u8>(value, "subtype")?;
        let bytes = value
            .cast::<PyBytes>()
            .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?;
        self.copy_binary(bytes.as_bytes(), subtype)
    }

    fn copy_binary(&mut self, bytes: &[u8], subtype: u8) -> PyResult<BsonValue> {
        self.ensure_binary_len(bytes.len(), subtype)?;
        self.copy_binary_after_budget(bytes, subtype)
    }

    fn ensure_binary_len(&mut self, len: usize, subtype: u8) -> PyResult<()> {
        let framing = if subtype == 2 { 9 } else { 5 };
        self.budget.wire(
            len.checked_add(framing)
                .ok_or_else(|| limit_exceeded(BSON_SIZE))?,
        )?;
        self.budget.heap(len)
    }

    fn copy_binary_after_budget(&mut self, bytes: &[u8], subtype: u8) -> PyResult<BsonValue> {
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(bytes.len())
            .map_err(|_| limit_exceeded(BSON_HEAP))?;
        owned.extend_from_slice(bytes);
        Ok(BsonValue::Binary(BsonBinary::new(subtype, owned)))
    }

    fn convert_regex(&mut self, value: &Bound<'py, PyAny>) -> PyResult<BsonValue> {
        let pattern = value
            .getattr("pattern")
            .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?;
        let pattern = pattern
            .cast::<PyString>()
            .map_err(|_| type_mismatch("BSON regular expression patterns must be strings"))?;
        let pattern_len = self.preflight_string(pattern, 2)?;
        let flags = extract_attr::<u32>(value, "flags")?;
        const KNOWN_FLAGS: u32 = 2 | 4 | 8 | 16 | 32 | 64;
        if flags & !KNOWN_FLAGS != 0 {
            return Err(invalid_value(
                "BSON regular expression flags may contain only i, l, m, s, u, and x",
            ));
        }
        let mut options = String::new();
        for (bit, option) in [
            (2, 'i'),
            (4, 'l'),
            (8, 'm'),
            (16, 's'),
            (32, 'u'),
            (64, 'x'),
        ] {
            if flags & bit != 0 {
                options.push(option);
            }
        }
        self.budget.wire(
            pattern_len
                .checked_add(options.len())
                .and_then(|len| len.checked_add(2))
                .ok_or_else(|| limit_exceeded(BSON_SIZE))?,
        )?;
        self.budget
            .heap(pattern_len.saturating_add(options.len()))?;
        let pattern = pattern
            .to_cow()
            .map_err(|_| invalid_text_encoding(INVALID_BSON_TEXT))?;
        debug_assert_eq!(pattern.len(), pattern_len);
        BsonRegex::new(pattern, options)
            .map(BsonValue::RegularExpression)
            .map_err(bson_error_to_python)
    }

    fn convert_code(
        &mut self,
        value: &Bound<'py, PyAny>,
        parent_depth: usize,
    ) -> PyResult<BsonValue> {
        self.enter(value)?;
        let result = (|| {
            let code = value
                .cast::<PyString>()
                .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?;
            let code_len = self.preflight_string(code, 5)?;
            self.budget.wire(
                code_len
                    .checked_add(5)
                    .ok_or_else(|| limit_exceeded(BSON_SIZE))?,
            )?;
            self.budget.heap(code_len)?;
            let code = code
                .to_cow()
                .map_err(|_| invalid_text_encoding(INVALID_BSON_TEXT))?;
            debug_assert_eq!(code.len(), code_len);
            let scope = value
                .getattr("scope")
                .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?;
            if scope.is_none() {
                return Ok(BsonValue::JavaScript(BsonJavaScript::new(code)));
            }
            self.budget.wire(4)?;
            let scope = self.convert_document(&scope, parent_depth + 1)?;
            Ok(BsonValue::JavaScript(BsonJavaScript::with_scope(
                code, scope,
            )))
        })();
        self.leave(value);
        result
    }

    fn datetime_millis(&self, value: &Bound<'py, PyAny>) -> PyResult<i64> {
        let ordinal = value
            .call_method0("toordinal")
            .and_then(|ordinal| ordinal.extract::<i64>())
            .map_err(|_| invalid_value("invalid Python datetime value"))?;
        let hour = extract_attr::<i64>(value, "hour")?;
        let minute = extract_attr::<i64>(value, "minute")?;
        let second = extract_attr::<i64>(value, "second")?;
        let micros = extract_attr::<i64>(value, "microsecond")?;
        let offset = value
            .call_method0("utcoffset")
            .map_err(|_| invalid_value("invalid Python datetime UTC offset"))?;
        let offset_micros = if offset.is_none() {
            0_i128
        } else {
            timedelta_micros(&offset)?
        };
        let local_micros = i128::from(ordinal - 719_163)
            .checked_mul(86_400_000_000)
            .and_then(|total| total.checked_add(i128::from(hour) * 3_600_000_000))
            .and_then(|total| total.checked_add(i128::from(minute) * 60_000_000))
            .and_then(|total| total.checked_add(i128::from(second) * 1_000_000))
            .and_then(|total| total.checked_add(i128::from(micros)))
            .ok_or_else(|| numeric_out_of_range("Python datetime is outside BSON's range"))?;
        i64::try_from(
            local_micros
                .checked_sub(offset_micros)
                .ok_or_else(|| numeric_out_of_range("Python datetime is outside BSON's range"))?
                .div_euclid(1_000),
        )
        .map_err(|_| numeric_out_of_range("Python datetime is outside BSON's range"))
    }

    fn ensure_depth(&self, depth: usize) -> PyResult<()> {
        if depth > BSON_MAX_NESTING_DEPTH {
            Err(limit_exceeded(BSON_DEPTH))
        } else {
            Ok(())
        }
    }

    fn preflight_string(&self, value: &Bound<'py, PyString>, framing: usize) -> PyResult<usize> {
        let characters = value
            .len()
            .map_err(|_| invalid_text_encoding(INVALID_BSON_TEXT))?;
        if characters
            .checked_add(framing)
            .is_none_or(|bytes| bytes > self.budget.wire_remaining)
        {
            return Err(limit_exceeded(BSON_SIZE));
        }
        if characters > self.budget.heap_remaining {
            return Err(limit_exceeded(BSON_HEAP));
        }
        let is_ascii = value
            .py()
            .get_type::<PyString>()
            .getattr("isascii")
            .and_then(|method| method.call1((value,)))
            .and_then(|result| result.is_truthy())
            .map_err(|_| invalid_text_encoding(INVALID_BSON_TEXT))?;
        if is_ascii {
            return Ok(characters);
        }
        const PREFLIGHT_CHUNK_CHARACTERS: usize = 64 * 1024;
        let mut encoded_bytes = 0_usize;
        let mut start = 0_usize;
        while start < characters {
            let end = start
                .saturating_add(PREFLIGHT_CHUNK_CHARACTERS)
                .min(characters);
            // SAFETY: `value` is a live Python `str`, the GIL is held for
            // `'py`, and both indices are bounded by `PyUnicode_GetLength`
            // through `PyString::len` above. The returned new reference is
            // immediately owned by `Bound`.
            let chunk = unsafe {
                Bound::from_owned_ptr_or_err(
                    value.py(),
                    pyo3::ffi::PyUnicode_Substring(
                        value.as_ptr(),
                        start as pyo3::ffi::Py_ssize_t,
                        end as pyo3::ffi::Py_ssize_t,
                    ),
                )
            }
            .and_then(|chunk| chunk.cast_into::<PyString>().map_err(PyErr::from))
            .map_err(|_| invalid_text_encoding(INVALID_BSON_TEXT))?;
            let width = chunk
                .encode_utf8()
                .map_err(|_| invalid_text_encoding(INVALID_BSON_TEXT))?
                .len()
                .map_err(|_| invalid_text_encoding(INVALID_BSON_TEXT))?;
            encoded_bytes = encoded_bytes
                .checked_add(width)
                .ok_or_else(|| limit_exceeded(BSON_SIZE))?;
            if encoded_bytes
                .checked_add(framing)
                .is_none_or(|bytes| bytes > self.budget.wire_remaining)
            {
                return Err(limit_exceeded(BSON_SIZE));
            }
            if encoded_bytes > self.budget.heap_remaining {
                return Err(limit_exceeded(BSON_HEAP));
            }
            start = end;
        }
        Ok(encoded_bytes)
    }

    fn enter(&mut self, value: &Bound<'py, PyAny>) -> PyResult<()> {
        if self.active.insert(value.as_ptr() as usize) {
            Ok(())
        } else {
            Err(invalid_value(BSON_CYCLE))
        }
    }

    fn leave(&mut self, value: &Bound<'py, PyAny>) {
        self.active.remove(&(value.as_ptr() as usize));
    }
}

struct OutputConverter<'types, 'py> {
    py: Python<'py>,
    types: &'types PythonBsonOutputTypes,
    representation: PythonUuidRepresentation,
}

impl<'types, 'py> OutputConverter<'types, 'py> {
    fn new(
        py: Python<'py>,
        types: &'types PythonBsonOutputTypes,
        representation: PythonUuidRepresentation,
    ) -> Self {
        Self {
            py,
            types,
            representation,
        }
    }

    fn document(self, document: &BsonDocument) -> PyResult<Py<PyAny>> {
        encode_document(document).map_err(bson_error_to_python)?;
        self.document_unchecked(document)
            .map(|document| document.into_any().unbind())
    }

    fn value(self, value: &BsonValue) -> PyResult<Py<PyAny>> {
        let wrapper =
            BsonDocument::from_entries([("value", value.clone())]).map_err(bson_error_to_python)?;
        encode_document(&wrapper).map_err(bson_error_to_python)?;
        self.value_unchecked(value)
    }

    fn document_unchecked(&self, document: &BsonDocument) -> PyResult<Bound<'py, PyDict>> {
        let output = PyDict::new(self.py);
        for (name, value) in document.iter() {
            output.set_item(name, self.value_unchecked(value)?)?;
        }
        Ok(output)
    }

    fn value_unchecked(&self, value: &BsonValue) -> PyResult<Py<PyAny>> {
        let py = self.py;
        match value {
            BsonValue::Double(value) => value.into_py_any(py),
            BsonValue::String(value) => value.into_py_any(py),
            BsonValue::Document(value) => Ok(self.document_unchecked(value)?.into_any().unbind()),
            BsonValue::Array(values) => {
                let output = PyList::empty(py);
                for value in values {
                    output.append(self.value_unchecked(value)?)?;
                }
                Ok(output.into_any().unbind())
            }
            BsonValue::Binary(value) => self.binary_to_python(value),
            BsonValue::Uuid(value) => self.binary_to_python(&value.to_binary()),
            BsonValue::ObjectId(value) => self
                .types
                .object_id
                .bind(py)
                .call1((PyBytes::new(py, &value.bytes()),))
                .map(|value| value.unbind()),
            BsonValue::Boolean(value) => value.into_py_any(py),
            BsonValue::DateTime(value) => self.datetime_to_python(*value),
            BsonValue::Null => Ok(py.None()),
            BsonValue::RegularExpression(value) => self
                .types
                .regex
                .bind(py)
                .call1((value.pattern(), value.options()))
                .map(|value| value.unbind()),
            BsonValue::JavaScript(value) => {
                if let Some(scope) = value.scope() {
                    self.types
                        .code
                        .bind(py)
                        .call1((value.code(), self.document_unchecked(scope)?))
                        .map(|value| value.unbind())
                } else {
                    self.types
                        .code
                        .bind(py)
                        .call1((value.code(),))
                        .map(|value| value.unbind())
                }
            }
            BsonValue::Int32(value) => value.into_py_any(py),
            BsonValue::Timestamp(value) => self
                .types
                .timestamp
                .bind(py)
                .call1((value.time(), value.increment()))
                .map(|value| value.unbind()),
            BsonValue::Int64(value) => self
                .types
                .int64
                .bind(py)
                .call1((*value,))
                .map(|value| value.unbind()),
            BsonValue::Decimal128(value) => self
                .types
                .decimal128_from_bid
                .bind(py)
                .call1((PyBytes::new(py, &value.bid()),))
                .map(|value| value.unbind()),
            BsonValue::MinKey => self
                .types
                .min_key
                .bind(py)
                .call0()
                .map(|value| value.unbind()),
            BsonValue::MaxKey => self
                .types
                .max_key
                .bind(py)
                .call0()
                .map(|value| value.unbind()),
            _ => Err(unsupported("unsupported BriskDB BSON value")),
        }
    }

    fn binary_to_python(&self, binary: &BsonBinary) -> PyResult<Py<PyAny>> {
        let py = self.py;
        if binary.subtype() == 0 {
            return Ok(PyBytes::new(py, binary.bytes()).into_any().unbind());
        }
        if let Some(representation) = self.representation.into_brisk() {
            if let Ok(uuid) = BsonUuid::from_binary(binary, representation) {
                return uuid_to_python(py, self.types.uuid.bind(py), uuid.bytes());
            }
        }
        self.types
            .binary
            .bind(py)
            .call1((PyBytes::new(py, binary.bytes()), binary.subtype()))
            .map(|value| value.unbind())
    }

    fn datetime_to_python(&self, value: BsonDateTime) -> PyResult<Py<PyAny>> {
        let py = self.py;
        let kwargs = PyDict::new(py);
        kwargs.set_item("tzinfo", self.types.timezone_utc.bind(py))?;
        let epoch = self
            .types
            .datetime
            .bind(py)
            .call((1970, 1, 1), Some(&kwargs))
            .map_err(|_| invalid_value("unable to construct UTC datetime"))?;
        let delta_kwargs = PyDict::new(py);
        delta_kwargs.set_item("milliseconds", value.timestamp_millis())?;
        let converted = self
            .types
            .timedelta
            .bind(py)
            .call((), Some(&delta_kwargs))
            .and_then(|delta| epoch.call_method1("__add__", (delta,)));
        match converted {
            Ok(datetime) => Ok(datetime.unbind()),
            Err(_) => self
                .types
                .datetime_ms
                .bind(py)
                .call1((value.timestamp_millis(),))
                .map(|value| value.unbind())
                .map_err(|_| invalid_value("unable to construct BSON DatetimeMS value")),
        }
    }
}

fn required_fixed_bytes<const N: usize>(
    value: &PyResult<Bound<'_, PyAny>>,
    expected: usize,
    message: &'static str,
) -> PyResult<[u8; N]> {
    let value = value.as_ref().map_err(|_| invalid_value(message))?;
    let bytes = value
        .cast::<PyBytes>()
        .map_err(|_| invalid_value(message))?
        .as_bytes();
    if bytes.len() != expected || expected != N {
        return Err(invalid_value(message));
    }
    let mut output = [0; N];
    output.copy_from_slice(bytes);
    Ok(output)
}

fn required_uuid_bytes(value: &Bound<'_, PyAny>) -> PyResult<[u8; 16]> {
    required_fixed_bytes(&value.getattr("bytes"), 16, "invalid Python UUID value")
}

fn uuid_to_python(py: Python<'_>, uuid: &Bound<'_, PyAny>, bytes: [u8; 16]) -> PyResult<Py<PyAny>> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("bytes", PyBytes::new(py, &bytes))?;
    uuid.call((), Some(&kwargs)).map(|value| value.unbind())
}

fn extract_attr<'py, T>(value: &Bound<'py, PyAny>, name: &str) -> PyResult<T>
where
    T: FromPyObjectOwned<'py>,
{
    let attribute = value
        .getattr(name)
        .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))?;
    attribute
        .extract::<T>()
        .map_err(|_| invalid_value(INVALID_BSON_ATTRIBUTE))
}

fn timedelta_micros(value: &Bound<'_, PyAny>) -> PyResult<i128> {
    let days = extract_attr::<i64>(value, "days")?;
    let seconds = extract_attr::<i64>(value, "seconds")?;
    let micros = extract_attr::<i64>(value, "microseconds")?;
    i128::from(days)
        .checked_mul(86_400_000_000)
        .and_then(|total| total.checked_add(i128::from(seconds) * 1_000_000))
        .and_then(|total| total.checked_add(i128::from(micros)))
        .ok_or_else(|| numeric_out_of_range("Python datetime UTC offset is outside BSON's range"))
}

fn decimal_digits(mut value: usize) -> usize {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}
