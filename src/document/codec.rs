//! Bounded BSON wire encoding and decoding.

use std::{collections::HashSet, mem::size_of};

use bson::{
    raw::{
        CString, RawArray, RawArrayBuf, RawBson, RawBsonRef, RawDocument, RawDocumentBuf,
        RawJavaScriptCodeWithScope,
    },
    spec::BinarySubtype,
};

use super::{
    BsonBinary, BsonDateTime, BsonDecimal128, BsonDocument, BsonError, BsonErrorKind,
    BsonJavaScript, BsonObjectId, BsonRegex, BsonResult, BsonTimestamp, BsonUuid, BsonValue,
    UuidRepresentation,
};

/// MongoDB's BSON document size limit, in bytes.
pub const BSON_MAX_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;

/// Maximum number of document and array containers, including the root.
pub const BSON_MAX_NESTING_DEPTH: usize = 100;

/// Maximum conservative retained-heap budget for one decoded BSON document.
pub const BSON_MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;

const MAX_DIAGNOSTIC_PATH_BYTES: usize = 1_024;
const MAX_RAW_ERROR_DIAGNOSTIC_BYTES: usize = 128;
const INITIAL_DECODED_CONTAINER_CAPACITY: usize = 4;
const DECODED_ARRAY_SLOT_BYTES: usize = size_of::<BsonValue>();
const DECODED_DOCUMENT_SLOT_BYTES: usize = size_of::<(String, BsonValue)>();
const DECODED_HASH_NAME_BYTES: usize = size_of::<&str>() * 8;
const DECODED_REGEX_OPTIONS_BYTES: usize = 8;

/// How the codec handles repeated names within a document.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DuplicateFieldPolicy {
    /// Reject the first repeated name in every document, including code scope.
    Reject,
    /// Retain all occurrences in their physical wire order.
    Preserve,
}

/// Limits and representation choices used by the BSON codec.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BsonCodecOptions {
    /// Maximum encoded size of a document. This cannot exceed 16 MiB.
    pub max_document_bytes: usize,
    /// Maximum container depth, including the root document.
    pub max_nesting_depth: usize,
    /// Conservative maximum retained heap used by a decoded document.
    pub max_decoded_bytes: usize,
    /// Policy for repeated document field names.
    pub duplicate_field_policy: DuplicateFieldPolicy,
    /// Optional binary-to-UUID materialization convention.
    pub uuid_representation: Option<UuidRepresentation>,
}

impl BsonCodecOptions {
    /// Construct the default bounded, duplicate-rejecting options.
    pub const fn new() -> Self {
        Self {
            max_document_bytes: BSON_MAX_DOCUMENT_BYTES,
            max_nesting_depth: BSON_MAX_NESTING_DEPTH,
            max_decoded_bytes: BSON_MAX_DECODED_BYTES,
            duplicate_field_policy: DuplicateFieldPolicy::Reject,
            uuid_representation: None,
        }
    }

    pub const fn with_max_document_bytes(mut self, max_document_bytes: usize) -> Self {
        self.max_document_bytes = max_document_bytes;
        self
    }

    pub const fn with_max_nesting_depth(mut self, max_nesting_depth: usize) -> Self {
        self.max_nesting_depth = max_nesting_depth;
        self
    }

    pub const fn with_max_decoded_bytes(mut self, max_decoded_bytes: usize) -> Self {
        self.max_decoded_bytes = max_decoded_bytes;
        self
    }

    pub const fn with_duplicate_field_policy(
        mut self,
        duplicate_field_policy: DuplicateFieldPolicy,
    ) -> Self {
        self.duplicate_field_policy = duplicate_field_policy;
        self
    }

    pub const fn with_uuid_representation(
        mut self,
        uuid_representation: Option<UuidRepresentation>,
    ) -> Self {
        self.uuid_representation = uuid_representation;
        self
    }

    fn validate(&self) -> BsonResult<()> {
        if !(5..=BSON_MAX_DOCUMENT_BYTES).contains(&self.max_document_bytes) {
            return Err(BsonError::new(
                BsonErrorKind::InvalidValue,
                format!("BSON max_document_bytes must be between 5 and {BSON_MAX_DOCUMENT_BYTES}"),
            ));
        }
        if !(1..=BSON_MAX_NESTING_DEPTH).contains(&self.max_nesting_depth) {
            return Err(BsonError::new(
                BsonErrorKind::InvalidValue,
                format!("BSON max_nesting_depth must be between 1 and {BSON_MAX_NESTING_DEPTH}"),
            ));
        }
        if !(1..=BSON_MAX_DECODED_BYTES).contains(&self.max_decoded_bytes) {
            return Err(BsonError::new(
                BsonErrorKind::InvalidValue,
                format!("BSON max_decoded_bytes must be between 1 and {BSON_MAX_DECODED_BYTES}"),
            ));
        }
        Ok(())
    }
}

impl Default for BsonCodecOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Decode one exact BSON document using the safe default policy.
pub fn decode_document(bytes: &[u8]) -> BsonResult<BsonDocument> {
    decode_document_with_options(bytes, &BsonCodecOptions::default())
}

/// Decode one exact BSON document with explicit limits and representation choices.
pub fn decode_document_with_options(
    bytes: &[u8],
    options: &BsonCodecOptions,
) -> BsonResult<BsonDocument> {
    options.validate()?;
    validate_top_level_frame(bytes, options)?;
    let raw = RawDocument::from_bytes(bytes)
        .map_err(|error| map_raw_error(error, "$", "invalid BSON document frame"))?;
    let mut budget = DecodeBudget::new(options.max_decoded_bytes);
    decode_raw_document(raw, options, &mut budget, 1, "$")
}

/// Encode one BSON document using the safe default policy.
pub fn encode_document(document: &BsonDocument) -> BsonResult<Vec<u8>> {
    encode_document_with_options(document, &BsonCodecOptions::default())
}

/// Encode one BSON document with explicit limits and representation choices.
pub fn encode_document_with_options(
    document: &BsonDocument,
    options: &BsonCodecOptions,
) -> BsonResult<Vec<u8>> {
    options.validate()?;
    encoded_document_len(document, options, 1, "$")?;
    let raw = encode_raw_document(document, options, 1, "$")?;
    Ok(raw.into_bytes())
}

fn validate_top_level_frame(bytes: &[u8], options: &BsonCodecOptions) -> BsonResult<()> {
    if bytes.len() > options.max_document_bytes {
        return Err(oversized(bytes.len(), options.max_document_bytes, "$"));
    }
    if bytes.len() < 4 {
        return Err(BsonError::new(
            BsonErrorKind::Truncated,
            "BSON input ends before its four-byte document length",
        )
        .with_path("$"));
    }

    let declared = i32::from_le_bytes(bytes[..4].try_into().expect("four bytes checked above"));
    if declared < 5 {
        return Err(BsonError::new(
            BsonErrorKind::InvalidValue,
            format!("BSON document length must be at least 5, got {declared}"),
        )
        .with_path("$"));
    }
    let declared = usize::try_from(declared).expect("positive i32 fits usize");
    if declared > options.max_document_bytes {
        return Err(oversized(declared, options.max_document_bytes, "$"));
    }
    if bytes.len() < declared {
        return Err(BsonError::new(
            BsonErrorKind::Truncated,
            format!(
                "BSON input contains {} bytes but declares {declared}",
                bytes.len()
            ),
        )
        .with_path("$"));
    }
    if bytes.len() > declared {
        return Err(BsonError::new(
            BsonErrorKind::InvalidValue,
            format!(
                "BSON input contains {} trailing bytes after the declared {declared}-byte document",
                bytes.len() - declared
            ),
        )
        .with_path("$"));
    }
    if bytes[declared - 1] != 0 {
        return Err(BsonError::new(
            BsonErrorKind::InvalidValue,
            "BSON document is missing its terminal NUL",
        )
        .with_path("$"));
    }
    Ok(())
}

fn decode_raw_document(
    raw: &RawDocument,
    options: &BsonCodecOptions,
    budget: &mut DecodeBudget,
    depth: usize,
    path: &str,
) -> BsonResult<BsonDocument> {
    ensure_depth(depth, options, path)?;
    ensure_document_size(raw.as_bytes().len(), options, path)?;

    let mut document = BsonDocument::new();
    let mut reserved_slots = 0;
    let mut names: Option<HashSet<&str>> =
        (options.duplicate_field_policy == DuplicateFieldPolicy::Reject).then(HashSet::new);
    for element in raw.iter_elements() {
        let element =
            element.map_err(|error| map_raw_error(error, path, "invalid BSON document element"))?;
        let name = element.key().as_str();
        let value_path = field_path(path, name);
        if let Some(names) = &mut names {
            if names.contains(name) {
                return Err(duplicate_field(&value_path));
            }
            budget.consume(DECODED_HASH_NAME_BYTES, &value_path, "field-name index")?;
            names
                .try_reserve(1)
                .map_err(|_| allocation_failure(&value_path, "field-name index"))?;
            names.insert(name);
        }
        let raw_value = element
            .value()
            .map_err(|error| map_raw_error(error, &value_path, "invalid BSON element value"))?;
        reserve_document_slot(&mut document, &mut reserved_slots, budget, &value_path)?;
        let name = clone_string(name, budget, &value_path, "field name")?;
        let value = decode_raw_value(raw_value, options, budget, depth, &value_path)?;
        document
            .push(name, value)
            .map_err(|error| error.with_path(value_path))?;
    }
    Ok(document)
}

fn decode_raw_array(
    raw: &RawArray,
    options: &BsonCodecOptions,
    budget: &mut DecodeBudget,
    depth: usize,
    path: &str,
) -> BsonResult<Vec<BsonValue>> {
    ensure_depth(depth, options, path)?;
    ensure_document_size(raw.as_bytes().len(), options, path)?;

    let mut values = Vec::new();
    let mut reserved_slots = 0;
    for (index, element) in raw.iter_elements().enumerate() {
        let value_path = index_path(path, index);
        let element = element
            .map_err(|error| map_raw_error(error, &value_path, "invalid BSON array element"))?;
        let raw_value = element
            .value()
            .map_err(|error| map_raw_error(error, &value_path, "invalid BSON array value"))?;
        reserve_array_slot(&mut values, &mut reserved_slots, budget, &value_path)?;
        let value = decode_raw_value(raw_value, options, budget, depth, &value_path)?;
        values.push(value);
    }
    Ok(values)
}

fn decode_raw_value(
    value: RawBsonRef<'_>,
    options: &BsonCodecOptions,
    budget: &mut DecodeBudget,
    parent_depth: usize,
    path: &str,
) -> BsonResult<BsonValue> {
    Ok(match value {
        RawBsonRef::Double(value) => BsonValue::Double(value),
        RawBsonRef::String(value) => {
            BsonValue::String(clone_string(value, budget, path, "string")?)
        }
        RawBsonRef::Document(value) => BsonValue::Document(decode_raw_document(
            value,
            options,
            budget,
            parent_depth + 1,
            path,
        )?),
        RawBsonRef::Array(value) => BsonValue::Array(decode_raw_array(
            value,
            options,
            budget,
            parent_depth + 1,
            path,
        )?),
        RawBsonRef::Binary(value) => {
            let subtype = u8::from(value.subtype);
            if options
                .uuid_representation
                .is_some_and(|representation| subtype == uuid_subtype(representation))
                && value.bytes.len() != 16
            {
                return Err(BsonError::new(
                    BsonErrorKind::InvalidValue,
                    format!(
                        "BSON UUID subtype {subtype} requires exactly 16 bytes, got {}",
                        value.bytes.len()
                    ),
                )
                .with_path(path));
            }
            let bytes = clone_bytes(value.bytes, budget, path, "binary payload")?;
            let binary = BsonBinary::new(subtype, bytes);
            match options.uuid_representation {
                Some(representation) if subtype == uuid_subtype(representation) => BsonValue::Uuid(
                    BsonUuid::from_binary(&binary, representation)
                        .map_err(|error| error.with_path(path))?,
                ),
                _ => BsonValue::Binary(binary),
            }
        }
        RawBsonRef::ObjectId(value) => BsonValue::ObjectId(BsonObjectId::from_bytes(value.bytes())),
        RawBsonRef::Boolean(value) => BsonValue::Boolean(value),
        RawBsonRef::DateTime(value) => {
            BsonValue::DateTime(BsonDateTime::from_millis(value.timestamp_millis()))
        }
        RawBsonRef::Null => BsonValue::Null,
        RawBsonRef::RegularExpression(value) => {
            let pattern = clone_string(value.pattern.as_str(), budget, path, "regex pattern")?;
            budget.consume(DECODED_REGEX_OPTIONS_BYTES, path, "canonical regex options")?;
            BsonValue::RegularExpression(
                BsonRegex::new(pattern, value.options.as_str())
                    .map_err(|error| error.with_path(path))?,
            )
        }
        RawBsonRef::JavaScriptCode(value) => BsonValue::JavaScript(BsonJavaScript::new(
            clone_string(value, budget, path, "JavaScript source")?,
        )),
        RawBsonRef::JavaScriptCodeWithScope(value) => {
            let scope = decode_raw_document(
                value.scope,
                options,
                budget,
                parent_depth + 1,
                &field_path(path, "$scope"),
            )?;
            BsonValue::JavaScript(BsonJavaScript::with_scope(
                clone_string(value.code, budget, path, "JavaScript source")?,
                scope,
            ))
        }
        RawBsonRef::Int32(value) => BsonValue::Int32(value),
        RawBsonRef::Timestamp(value) => {
            BsonValue::Timestamp(BsonTimestamp::new(value.time, value.increment))
        }
        RawBsonRef::Int64(value) => BsonValue::Int64(value),
        RawBsonRef::Decimal128(value) => {
            BsonValue::Decimal128(BsonDecimal128::from_bid(value.bytes()))
        }
        RawBsonRef::MaxKey => BsonValue::MaxKey,
        RawBsonRef::MinKey => BsonValue::MinKey,
        RawBsonRef::Undefined => return Err(unsupported_type("Undefined", path)),
        RawBsonRef::DbPointer(_) => return Err(unsupported_type("DBPointer", path)),
        RawBsonRef::Symbol(_) => return Err(unsupported_type("Symbol", path)),
    })
}

fn encode_raw_document(
    document: &BsonDocument,
    options: &BsonCodecOptions,
    depth: usize,
    path: &str,
) -> BsonResult<RawDocumentBuf> {
    ensure_depth(depth, options, path)?;

    let mut raw = RawDocumentBuf::new();
    let mut names = HashSet::new();
    for (name, value) in document.iter() {
        let value_path = field_path(path, name);
        if options.duplicate_field_policy == DuplicateFieldPolicy::Reject && !names.insert(name) {
            return Err(duplicate_field(&value_path));
        }
        ensure_payload_bound(name.len(), options, &value_path, "field name")?;
        let key = CString::try_from(name)
            .map_err(|error| map_raw_error(error, &value_path, "invalid BSON field name"))?;
        let raw_value = encode_raw_value(value, options, depth, &value_path)?;
        raw.append(key, raw_value);
        ensure_document_size(raw.as_bytes().len(), options, path)?;
    }
    Ok(raw)
}

fn encoded_document_len(
    document: &BsonDocument,
    options: &BsonCodecOptions,
    depth: usize,
    path: &str,
) -> BsonResult<usize> {
    ensure_depth(depth, options, path)?;

    let mut encoded_len = 5_usize;
    let mut names = HashSet::new();
    for (name, value) in document.iter() {
        let value_path = field_path(path, name);
        if options.duplicate_field_policy == DuplicateFieldPolicy::Reject && !names.insert(name) {
            return Err(duplicate_field(&value_path));
        }
        if name.contains('\0') {
            return Err(BsonError::new(
                BsonErrorKind::InvalidValue,
                "BSON field names cannot contain NUL",
            )
            .with_path(value_path));
        }
        encoded_len = add_encoded_size(encoded_len, name.len(), options, path)?;
        encoded_len = add_encoded_size(encoded_len, 2, options, path)?;
        encoded_len = add_encoded_size(
            encoded_len,
            encoded_value_len(value, options, depth, &value_path)?,
            options,
            path,
        )?;
    }
    Ok(encoded_len)
}

fn encoded_array_len(
    values: &[BsonValue],
    options: &BsonCodecOptions,
    depth: usize,
    path: &str,
) -> BsonResult<usize> {
    ensure_depth(depth, options, path)?;

    let mut encoded_len = 5_usize;
    for (index, value) in values.iter().enumerate() {
        let value_path = index_path(path, index);
        let index_key_len = index.to_string().len();
        encoded_len = add_encoded_size(encoded_len, index_key_len, options, path)?;
        encoded_len = add_encoded_size(encoded_len, 2, options, path)?;
        encoded_len = add_encoded_size(
            encoded_len,
            encoded_value_len(value, options, depth, &value_path)?,
            options,
            path,
        )?;
    }
    Ok(encoded_len)
}

fn encoded_value_len(
    value: &BsonValue,
    options: &BsonCodecOptions,
    parent_depth: usize,
    path: &str,
) -> BsonResult<usize> {
    match value {
        BsonValue::Double(_) | BsonValue::DateTime(_) | BsonValue::Timestamp(_) => Ok(8),
        BsonValue::String(value) => {
            let len = add_encoded_size(5, value.len(), options, path)?;
            Ok(len)
        }
        BsonValue::Document(value) => encoded_document_len(value, options, parent_depth + 1, path),
        BsonValue::Array(value) => encoded_array_len(value, options, parent_depth + 1, path),
        BsonValue::Binary(value) => {
            let framing = if value.subtype() == 2 { 9 } else { 5 };
            add_encoded_size(framing, value.bytes().len(), options, path)
        }
        BsonValue::Uuid(_) => Ok(21),
        BsonValue::ObjectId(_) => Ok(12),
        BsonValue::Boolean(_) => Ok(1),
        BsonValue::Null | BsonValue::MinKey | BsonValue::MaxKey => Ok(0),
        BsonValue::RegularExpression(value) => {
            let len = add_encoded_size(2, value.pattern().len(), options, path)?;
            add_encoded_size(len, value.options().len(), options, path)
        }
        BsonValue::JavaScript(value) => {
            let code_len = add_encoded_size(5, value.code().len(), options, path)?;
            match value.scope() {
                None => Ok(code_len),
                Some(scope) => {
                    let scope_len = encoded_document_len(
                        scope,
                        options,
                        parent_depth + 1,
                        &field_path(path, "$scope"),
                    )?;
                    let len = add_encoded_size(4, code_len, options, path)?;
                    add_encoded_size(len, scope_len, options, path)
                }
            }
        }
        BsonValue::Int32(_) => Ok(4),
        BsonValue::Int64(_) => Ok(8),
        BsonValue::Decimal128(_) => Ok(16),
    }
}

fn add_encoded_size(
    current: usize,
    additional: usize,
    options: &BsonCodecOptions,
    path: &str,
) -> BsonResult<usize> {
    let total = current
        .checked_add(additional)
        .ok_or_else(|| oversized(usize::MAX, options.max_document_bytes, path))?;
    if total > options.max_document_bytes {
        return Err(oversized(total, options.max_document_bytes, path));
    }
    Ok(total)
}

fn encode_raw_array(
    values: &[BsonValue],
    options: &BsonCodecOptions,
    depth: usize,
    path: &str,
) -> BsonResult<RawArrayBuf> {
    ensure_depth(depth, options, path)?;

    let mut raw = RawArrayBuf::new();
    for (index, value) in values.iter().enumerate() {
        let value_path = index_path(path, index);
        raw.push(encode_raw_value(value, options, depth, &value_path)?);
        ensure_document_size(raw.as_bytes().len(), options, path)?;
    }
    Ok(raw)
}

fn encode_raw_value(
    value: &BsonValue,
    options: &BsonCodecOptions,
    parent_depth: usize,
    path: &str,
) -> BsonResult<RawBson> {
    Ok(match value {
        BsonValue::Double(value) => RawBson::Double(*value),
        BsonValue::String(value) => {
            ensure_length_encoded_string(value, options, path, "string")?;
            RawBson::String(value.clone())
        }
        BsonValue::Document(value) => {
            RawBson::Document(encode_raw_document(value, options, parent_depth + 1, path)?)
        }
        BsonValue::Array(value) => {
            RawBson::Array(encode_raw_array(value, options, parent_depth + 1, path)?)
        }
        BsonValue::Binary(value) => encode_binary(value, options, path)?,
        BsonValue::Uuid(value) => encode_binary(&value.to_binary(), options, path)?,
        BsonValue::ObjectId(value) => {
            RawBson::ObjectId(bson::oid::ObjectId::from_bytes(value.bytes()))
        }
        BsonValue::Boolean(value) => RawBson::Boolean(*value),
        BsonValue::DateTime(value) => {
            RawBson::DateTime(bson::DateTime::from_millis(value.timestamp_millis()))
        }
        BsonValue::Null => RawBson::Null,
        BsonValue::RegularExpression(value) => {
            ensure_payload_bound(value.pattern().len(), options, path, "regex pattern")?;
            ensure_payload_bound(value.options().len(), options, path, "regex options")?;
            let pattern = CString::try_from(value.pattern())
                .map_err(|error| map_raw_error(error, path, "invalid BSON regex pattern"))?;
            let options_value = CString::try_from(value.options())
                .map_err(|error| map_raw_error(error, path, "invalid BSON regex options"))?;
            RawBson::RegularExpression(bson::Regex {
                pattern,
                options: options_value,
            })
        }
        BsonValue::JavaScript(value) => {
            ensure_length_encoded_string(value.code(), options, path, "JavaScript source")?;
            match value.scope() {
                None => RawBson::JavaScriptCode(value.code().to_owned()),
                Some(scope) => RawBson::JavaScriptCodeWithScope(RawJavaScriptCodeWithScope {
                    code: value.code().to_owned(),
                    scope: encode_raw_document(
                        scope,
                        options,
                        parent_depth + 1,
                        &field_path(path, "$scope"),
                    )?,
                }),
            }
        }
        BsonValue::Int32(value) => RawBson::Int32(*value),
        BsonValue::Timestamp(value) => RawBson::Timestamp(bson::Timestamp {
            time: value.time(),
            increment: value.increment(),
        }),
        BsonValue::Int64(value) => RawBson::Int64(*value),
        BsonValue::Decimal128(value) => {
            RawBson::Decimal128(bson::Decimal128::from_bytes(value.bid()))
        }
        BsonValue::MaxKey => RawBson::MaxKey,
        BsonValue::MinKey => RawBson::MinKey,
    })
}

fn encode_binary(
    value: &BsonBinary,
    options: &BsonCodecOptions,
    path: &str,
) -> BsonResult<RawBson> {
    let framing_bytes = if value.subtype() == 2 { 9 } else { 5 };
    let encoded_len = value
        .bytes()
        .len()
        .checked_add(framing_bytes)
        .ok_or_else(|| oversized(usize::MAX, options.max_document_bytes, path))?;
    if encoded_len > options.max_document_bytes {
        return Err(oversized(encoded_len, options.max_document_bytes, path));
    }
    Ok(RawBson::Binary(bson::Binary {
        subtype: BinarySubtype::from(value.subtype()),
        bytes: value.bytes().to_vec(),
    }))
}

fn ensure_length_encoded_string(
    value: &str,
    options: &BsonCodecOptions,
    path: &str,
    description: &str,
) -> BsonResult<()> {
    let encoded_len = value
        .len()
        .checked_add(5)
        .ok_or_else(|| oversized(usize::MAX, options.max_document_bytes, path))?;
    if encoded_len > options.max_document_bytes {
        return Err(oversized(encoded_len, options.max_document_bytes, path));
    }
    ensure_payload_bound(value.len(), options, path, description)
}

fn ensure_payload_bound(
    len: usize,
    options: &BsonCodecOptions,
    path: &str,
    description: &str,
) -> BsonResult<()> {
    if len > options.max_document_bytes {
        return Err(BsonError::new(
            BsonErrorKind::Oversized,
            format!(
                "BSON {description} contains {len} bytes, exceeding the configured {}-byte limit",
                options.max_document_bytes
            ),
        )
        .with_path(path));
    }
    Ok(())
}

fn ensure_document_size(actual: usize, options: &BsonCodecOptions, path: &str) -> BsonResult<()> {
    if actual > options.max_document_bytes {
        return Err(oversized(actual, options.max_document_bytes, path));
    }
    Ok(())
}

fn ensure_depth(depth: usize, options: &BsonCodecOptions, path: &str) -> BsonResult<()> {
    if depth > options.max_nesting_depth {
        return Err(BsonError::new(
            BsonErrorKind::NestingLimit,
            format!(
                "BSON container depth {depth} exceeds the configured limit of {}",
                options.max_nesting_depth
            ),
        )
        .with_path(path));
    }
    Ok(())
}

fn uuid_subtype(representation: UuidRepresentation) -> u8 {
    match representation {
        UuidRepresentation::Standard => 4,
        UuidRepresentation::PythonLegacy
        | UuidRepresentation::JavaLegacy
        | UuidRepresentation::CSharpLegacy => 3,
    }
}

struct DecodeBudget {
    limit: usize,
    remaining: usize,
}

impl DecodeBudget {
    const fn new(limit: usize) -> Self {
        Self {
            limit,
            remaining: limit,
        }
    }

    fn consume(&mut self, bytes: usize, path: &str, description: &'static str) -> BsonResult<()> {
        self.remaining = self.remaining.checked_sub(bytes).ok_or_else(|| {
            BsonError::new(
                BsonErrorKind::Oversized,
                format!(
                    "decoded BSON exceeds the configured {}-byte allocation budget while retaining {description}",
                    self.limit
                ),
            )
            .with_path(path)
        })?;
        Ok(())
    }

    fn reserve_container_slots(
        &mut self,
        len: usize,
        reserved: &mut usize,
        slot_bytes: usize,
        path: &str,
        description: &'static str,
    ) -> BsonResult<Option<usize>> {
        if len < *reserved {
            return Ok(None);
        }
        debug_assert_eq!(len, *reserved);

        let desired_additional = if *reserved == 0 {
            INITIAL_DECODED_CONTAINER_CAPACITY
        } else {
            *reserved
        };
        let affordable = self.remaining / slot_bytes;
        if affordable == 0 {
            self.consume(slot_bytes, path, description)?;
            unreachable!("consuming an unaffordable container slot returns an error");
        }
        let additional_slots = desired_additional.min(affordable);
        let allocation_bytes = additional_slots
            .checked_mul(slot_bytes)
            .ok_or_else(|| allocation_failure(path, description))?;
        self.consume(allocation_bytes, path, description)?;
        *reserved = reserved
            .checked_add(additional_slots)
            .ok_or_else(|| allocation_failure(path, description))?;
        Ok(Some(additional_slots))
    }
}

fn reserve_document_slot(
    document: &mut BsonDocument,
    reserved: &mut usize,
    budget: &mut DecodeBudget,
    path: &str,
) -> BsonResult<()> {
    if let Some(additional) = budget.reserve_container_slots(
        document.len(),
        reserved,
        DECODED_DOCUMENT_SLOT_BYTES,
        path,
        "document entries",
    )? {
        document
            .try_reserve(additional)
            .map_err(|_| allocation_failure(path, "document entries"))?;
    }
    Ok(())
}

fn reserve_array_slot(
    values: &mut Vec<BsonValue>,
    reserved: &mut usize,
    budget: &mut DecodeBudget,
    path: &str,
) -> BsonResult<()> {
    if let Some(additional) = budget.reserve_container_slots(
        values.len(),
        reserved,
        DECODED_ARRAY_SLOT_BYTES,
        path,
        "array elements",
    )? {
        values
            .try_reserve_exact(additional)
            .map_err(|_| allocation_failure(path, "array elements"))?;
    }
    Ok(())
}

fn clone_string(
    value: &str,
    budget: &mut DecodeBudget,
    path: &str,
    description: &'static str,
) -> BsonResult<String> {
    budget.consume(value.len(), path, description)?;
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| allocation_failure(path, description))?;
    owned.push_str(value);
    Ok(owned)
}

fn clone_bytes(
    value: &[u8],
    budget: &mut DecodeBudget,
    path: &str,
    description: &'static str,
) -> BsonResult<Vec<u8>> {
    budget.consume(value.len(), path, description)?;
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| allocation_failure(path, description))?;
    owned.extend_from_slice(value);
    Ok(owned)
}

fn allocation_failure(path: &str, description: &'static str) -> BsonError {
    BsonError::new(
        BsonErrorKind::Oversized,
        format!("unable to reserve bounded decoded BSON storage for {description}"),
    )
    .with_path(path)
}

fn duplicate_field(path: &str) -> BsonError {
    BsonError::new(
        BsonErrorKind::DuplicateField,
        "BSON document contains a duplicate field name",
    )
    .with_path(path)
}

fn oversized(actual: usize, limit: usize, path: &str) -> BsonError {
    BsonError::new(
        BsonErrorKind::Oversized,
        format!(
            "BSON document contains {actual} bytes, exceeding the configured {limit}-byte limit"
        ),
    )
    .with_path(path)
}

fn unsupported_type(name: &str, path: &str) -> BsonError {
    BsonError::new(
        BsonErrorKind::UnsupportedType,
        format!("BSON {name} values are not supported"),
    )
    .with_path(path)
}

fn map_raw_error(error: bson::error::Error, path: &str, context: &'static str) -> BsonError {
    let (kind, detail) = match &error.kind {
        bson::error::ErrorKind::EndOfStream { .. } => (
            BsonErrorKind::Truncated,
            "input ended before the BSON value was complete",
        ),
        bson::error::ErrorKind::Utf8Encoding { .. } => {
            (BsonErrorKind::InvalidUtf8, "BSON text is not valid UTF-8")
        }
        bson::error::ErrorKind::MalformedBytes { .. } => {
            (BsonErrorKind::InvalidValue, "malformed BSON bytes")
        }
        _ => (BsonErrorKind::InvalidValue, "invalid BSON value"),
    };
    let diagnostic = format!("{context}: {detail}");
    debug_assert!(diagnostic.len() <= MAX_RAW_ERROR_DIAGNOSTIC_BYTES);
    BsonError::new(kind, diagnostic).with_path(path)
}

fn field_path(parent: &str, name: &str) -> String {
    let capacity = parent
        .len()
        .saturating_add(name.len())
        .saturating_add(4)
        .min(MAX_DIAGNOSTIC_PATH_BYTES);
    let mut path = String::with_capacity(capacity);
    push_bounded_to(
        &mut path,
        parent,
        MAX_DIAGNOSTIC_PATH_BYTES.saturating_sub(4),
    );
    path.push_str("[\"");
    'name: for character in name.chars() {
        for escaped in character.escape_default() {
            if path.len() + escaped.len_utf8() + 2 > MAX_DIAGNOSTIC_PATH_BYTES {
                break 'name;
            }
            path.push(escaped);
        }
    }
    path.push_str("\"]");
    path
}

fn index_path(parent: &str, index: usize) -> String {
    let suffix = format!("[{index}]");
    let capacity = parent
        .len()
        .saturating_add(suffix.len())
        .min(MAX_DIAGNOSTIC_PATH_BYTES);
    let mut path = String::with_capacity(capacity);
    push_bounded(&mut path, parent);
    push_bounded(&mut path, &suffix);
    path
}

fn push_bounded(target: &mut String, value: &str) {
    push_bounded_to(target, value, MAX_DIAGNOSTIC_PATH_BYTES);
}

fn push_bounded_to(target: &mut String, value: &str, limit: usize) {
    for character in value.chars() {
        if target.len() + character.len_utf8() > limit {
            break;
        }
        target.push(character);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_key_null_document(element_count: usize) -> Vec<u8> {
        let len = 5 + element_count * 2;
        let mut bytes = vec![0; len];
        bytes[..4].copy_from_slice(&i32::try_from(len).unwrap().to_le_bytes());
        for offset in (4..len - 1).step_by(2) {
            bytes[offset] = 0x0a;
        }
        bytes
    }

    fn empty_key_null_array(element_count: usize) -> Vec<u8> {
        let array_len = 5 + element_count * 2;
        let document_len = array_len + 8;
        let mut bytes = vec![0; document_len];
        bytes[..4].copy_from_slice(&i32::try_from(document_len).unwrap().to_le_bytes());
        bytes[4] = 0x04;
        bytes[5] = b'a';
        bytes[7..11].copy_from_slice(&i32::try_from(array_len).unwrap().to_le_bytes());
        for offset in (11..7 + array_len - 1).step_by(2) {
            bytes[offset] = 0x0a;
        }
        bytes
    }

    #[test]
    fn decoded_budget_rejects_empty_element_amplification() {
        let options = BsonCodecOptions::default()
            .with_duplicate_field_policy(DuplicateFieldPolicy::Preserve)
            .with_max_decoded_bytes(4_096);

        for bytes in [
            empty_key_null_document(500_000),
            empty_key_null_array(500_000),
        ] {
            let error = decode_document_with_options(&bytes, &options).unwrap_err();
            assert_eq!(error.kind(), BsonErrorKind::Oversized);
            assert!(error.diagnostic().contains("allocation budget"));
        }
    }

    #[test]
    fn default_decoded_budget_accepts_one_near_limit_string() {
        let payload_len = BSON_MAX_DOCUMENT_BYTES - 13;
        let mut bytes = Vec::with_capacity(BSON_MAX_DOCUMENT_BYTES);
        bytes.extend_from_slice(
            &i32::try_from(BSON_MAX_DOCUMENT_BYTES)
                .unwrap()
                .to_le_bytes(),
        );
        bytes.extend_from_slice(&[0x02, b's', 0]);
        bytes.extend_from_slice(&i32::try_from(payload_len + 1).unwrap().to_le_bytes());
        bytes.resize(bytes.len() + payload_len, b'x');
        bytes.extend_from_slice(&[0, 0]);

        let document = decode_document(&bytes).unwrap();
        let Some(BsonValue::String(value)) = document.get_unique("s").unwrap() else {
            panic!("expected string value")
        };
        assert_eq!(value.len(), payload_len);
    }

    #[test]
    fn attacker_controlled_names_do_not_expand_diagnostics() {
        let name = "x".repeat(MAX_DIAGNOSTIC_PATH_BYTES * 4);
        let document = BsonDocument::from_entries([
            (name.clone(), BsonValue::Null),
            (name.clone(), BsonValue::Null),
        ])
        .unwrap();
        let preserve =
            BsonCodecOptions::default().with_duplicate_field_policy(DuplicateFieldPolicy::Preserve);
        let bytes = encode_document_with_options(&document, &preserve).unwrap();

        for error in [
            decode_document(&bytes).unwrap_err(),
            encode_document(&document).unwrap_err(),
        ] {
            assert_eq!(error.kind(), BsonErrorKind::DuplicateField);
            assert_eq!(
                error.diagnostic(),
                "BSON document contains a duplicate field name"
            );
            assert!(error.path().unwrap().len() <= MAX_DIAGNOSTIC_PATH_BYTES);
        }

        let document_len = 8 + name.len();
        let mut malformed = Vec::with_capacity(document_len);
        malformed.extend_from_slice(&i32::try_from(document_len).unwrap().to_le_bytes());
        malformed.push(0x08);
        malformed.extend_from_slice(name.as_bytes());
        malformed.extend_from_slice(&[0, 2, 0]);
        let error = decode_document(&malformed).unwrap_err();
        assert_eq!(error.kind(), BsonErrorKind::InvalidValue);
        assert!(error.diagnostic().len() <= MAX_RAW_ERROR_DIAGNOSTIC_BYTES);
        assert!(!error.diagnostic().contains(&name));
        assert!(error.path().unwrap().len() <= MAX_DIAGNOSTIC_PATH_BYTES);
    }

    #[test]
    fn diagnostic_paths_escape_untrusted_field_names() {
        let name = "line\n.\"[\\\u{7f}";
        let document =
            BsonDocument::from_entries([(name, BsonValue::Null), (name, BsonValue::Null)]).unwrap();
        let preserve =
            BsonCodecOptions::default().with_duplicate_field_policy(DuplicateFieldPolicy::Preserve);
        let bytes = encode_document_with_options(&document, &preserve).unwrap();

        let error = decode_document(&bytes).unwrap_err();
        let path = error.path().unwrap();
        assert!(!path.contains('\n'));
        assert!(path.starts_with("$[\""));
        assert!(path.ends_with("\"]"));
        assert!(path.contains("\\n"));
        assert!(path.contains("\\\""));
        assert!(path.contains("\\\\"));
        assert!(path.contains("\\u{7f}"));
    }
}
