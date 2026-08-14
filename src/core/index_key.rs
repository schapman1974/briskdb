//! Versioned, protocol-neutral global-index key encoding.

use std::fmt;

use super::{EngineError, EngineErrorKind, EngineResult, Value};

/// Current canonical global-index key encoding.
///
/// This version is independent from the persisted shard-routing key encoding.
pub const INDEX_KEY_ENCODING_VERSION: u32 = 1;

const MAGIC: &[u8; 4] = b"BIDX";
const HEADER_LEN: usize = MAGIC.len() + size_of::<u32>();
const OPTIONS_BASE: u8 = 0x10;
const OPTIONS_DESCENDING: u8 = 0x01;
const OPTIONS_NULLS_LAST: u8 = 0x02;
const COLLATION_BINARY: u8 = 0x01;

const TAG_BOOLEAN: u8 = 0x10;
const TAG_INT64: u8 = 0x20;
const TAG_UINT64: u8 = 0x21;
const TAG_FLOAT64: u8 = 0x30;
const TAG_DATE: u8 = 0x40;
const TAG_TIMESTAMP: u8 = 0x41;
const TAG_TEXT: u8 = 0x50;
const TAG_BINARY: u8 = 0x51;

const VARIABLE_ESCAPE: u8 = 0xff;
const VARIABLE_TERMINATOR: u8 = 0x00;
const CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

/// Supported ordering direction for one global-index component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexKeyOrder {
    Ascending,
    Descending,
}

/// Explicit placement of NULL for one global-index component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexNullOrder {
    First,
    Last,
}

/// Uniqueness behavior when at least one compound-key component is NULL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UniqueNullSemantics {
    /// Match ordinary SQLite UNIQUE behavior: do not reserve a key containing NULL.
    Distinct,
    /// Encode NULL normally so only one identical NULL-containing key may be reserved.
    NotDistinct,
}

/// Collation supported by canonical global-index encoding.
///
/// Version 1 deliberately supports only exact bytewise `BINARY` text order.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexKeyCollation {
    Binary,
}

impl IndexKeyCollation {
    /// Resolve a SQLite collation name without silently accepting unsupported rules.
    pub fn from_name(name: &str) -> EngineResult<Self> {
        if name.eq_ignore_ascii_case("binary") {
            Ok(Self::Binary)
        } else {
            Err(EngineError::new(
                EngineErrorKind::Unsupported,
                "global-index key encoding version 1 supports only BINARY collation",
            ))
        }
    }

    /// Return the canonical SQLite spelling.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Binary => "BINARY",
        }
    }
}

/// Borrowed value accepted by the canonical global-index encoder.
///
/// Dates are signed days from `1970-01-01`. Timestamps are signed microseconds
/// from the Unix epoch after any time-zone normalization.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IndexKeyValueRef<'a> {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Decimal(&'a str),
    Text(&'a str),
    InvalidText(&'a [u8]),
    Binary(&'a [u8]),
    Date(i32),
    Timestamp(i64),
}

impl IndexKeyValueRef<'_> {
    const fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }
}

impl<'a> From<&'a Value> for IndexKeyValueRef<'a> {
    fn from(value: &'a Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Boolean(value) => Self::Boolean(*value),
            Value::Int64(value) => Self::Int64(*value),
            Value::UInt64(value) => Self::UInt64(*value),
            Value::Float64(value) => Self::Float64(*value),
            Value::Decimal(value) => Self::Decimal(value.as_str()),
            Value::Text(value) => Self::Text(value),
            Value::InvalidText(value) => Self::InvalidText(value),
            Value::Binary(value) => Self::Binary(value),
        }
    }
}

/// One component and its frozen ordering rules.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IndexKeyPart<'a> {
    value: IndexKeyValueRef<'a>,
    order: IndexKeyOrder,
    null_order: IndexNullOrder,
    collation: IndexKeyCollation,
}

impl<'a> IndexKeyPart<'a> {
    /// Construct an ascending, NULLS FIRST, BINARY component.
    pub const fn ascending(value: IndexKeyValueRef<'a>) -> Self {
        Self {
            value,
            order: IndexKeyOrder::Ascending,
            null_order: IndexNullOrder::First,
            collation: IndexKeyCollation::Binary,
        }
    }

    /// Construct a descending, NULLS LAST, BINARY component.
    pub const fn descending(value: IndexKeyValueRef<'a>) -> Self {
        Self {
            value,
            order: IndexKeyOrder::Descending,
            null_order: IndexNullOrder::Last,
            collation: IndexKeyCollation::Binary,
        }
    }

    /// Override the explicit NULL placement.
    pub const fn with_null_order(mut self, null_order: IndexNullOrder) -> Self {
        self.null_order = null_order;
        self
    }

    /// Override the validated collation.
    pub const fn with_collation(mut self, collation: IndexKeyCollation) -> Self {
        self.collation = collation;
        self
    }

    pub const fn value(self) -> IndexKeyValueRef<'a> {
        self.value
    }

    pub const fn order(self) -> IndexKeyOrder {
        self.order
    }

    pub const fn null_order(self) -> IndexNullOrder {
        self.null_order
    }

    pub const fn collation(self) -> IndexKeyCollation {
        self.collation
    }
}

/// Owned logical value recovered from a canonical key.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexKeyValue {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Text(String),
    Binary(Vec<u8>),
    Date(i32),
    Timestamp(i64),
}

impl IndexKeyValue {
    pub fn as_ref(&self) -> IndexKeyValueRef<'_> {
        match self {
            Self::Null => IndexKeyValueRef::Null,
            Self::Boolean(value) => IndexKeyValueRef::Boolean(*value),
            Self::Int64(value) => IndexKeyValueRef::Int64(*value),
            Self::UInt64(value) => IndexKeyValueRef::UInt64(*value),
            Self::Float64(value) => IndexKeyValueRef::Float64(*value),
            Self::Text(value) => IndexKeyValueRef::Text(value),
            Self::Binary(value) => IndexKeyValueRef::Binary(value),
            Self::Date(value) => IndexKeyValueRef::Date(*value),
            Self::Timestamp(value) => IndexKeyValueRef::Timestamp(*value),
        }
    }
}

/// One decoded component with the ordering metadata stored in the key.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedIndexKeyPart {
    value: IndexKeyValue,
    order: IndexKeyOrder,
    null_order: IndexNullOrder,
    collation: IndexKeyCollation,
}

impl DecodedIndexKeyPart {
    pub fn value(&self) -> &IndexKeyValue {
        &self.value
    }

    pub const fn order(&self) -> IndexKeyOrder {
        self.order
    }

    pub const fn null_order(&self) -> IndexNullOrder {
        self.null_order
    }

    pub const fn collation(&self) -> IndexKeyCollation {
        self.collation
    }

    pub fn as_ref(&self) -> IndexKeyPart<'_> {
        IndexKeyPart {
            value: self.value.as_ref(),
            order: self.order,
            null_order: self.null_order,
            collation: self.collation,
        }
    }
}

/// Validated canonical bytes for one compound global-index key.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CanonicalIndexKey {
    bytes: Box<[u8]>,
    component_count: usize,
}

impl CanonicalIndexKey {
    /// Encode one nonempty compound key.
    pub fn encode(parts: &[IndexKeyPart<'_>]) -> EngineResult<Self> {
        Self::encode_parts(parts.iter().copied())
    }

    fn encode_parts<'a>(
        parts: impl ExactSizeIterator<Item = IndexKeyPart<'a>>,
    ) -> EngineResult<Self> {
        let component_count = parts.len();
        if component_count == 0 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "a global-index key must contain at least one component",
            ));
        }

        let mut bytes = Vec::with_capacity(HEADER_LEN + component_count * 8);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&INDEX_KEY_ENCODING_VERSION.to_be_bytes());
        for part in parts {
            encode_part(&mut bytes, part)?;
        }
        Ok(Self {
            bytes: bytes.into_boxed_slice(),
            component_count,
        })
    }

    /// Encode protocol-neutral core values with SQLite's default ascending rules.
    pub fn encode_values(values: &[Value]) -> EngineResult<Self> {
        Self::encode_parts(
            values
                .iter()
                .map(|value| IndexKeyPart::ascending(value.into())),
        )
    }

    /// Encode a key for a unique reservation under explicit NULL semantics.
    ///
    /// `Distinct` returns `None` for any NULL-containing key, matching ordinary
    /// SQLite UNIQUE constraints. All components are still validated first.
    pub fn encode_unique(
        parts: &[IndexKeyPart<'_>],
        null_semantics: UniqueNullSemantics,
    ) -> EngineResult<Option<Self>> {
        let key = Self::encode(parts)?;
        if null_semantics == UniqueNullSemantics::Distinct
            && parts.iter().any(|part| part.value.is_null())
        {
            Ok(None)
        } else {
            Ok(Some(key))
        }
    }

    /// Validate and own bytes read from global-index storage.
    pub fn from_bytes(bytes: &[u8]) -> EngineResult<Self> {
        let parts = decode_parts(bytes)?;
        let canonical = Self::encode_parts(parts.iter().map(DecodedIndexKeyPart::as_ref))?;
        if canonical.as_bytes() != bytes {
            return Err(corrupt(
                "global-index key uses a noncanonical representation",
            ));
        }
        Ok(canonical)
    }

    /// Decode the logical components and their ordering metadata.
    pub fn decode(&self) -> EngineResult<Vec<DecodedIndexKeyPart>> {
        decode_parts(&self.bytes)
    }

    pub const fn encoding_version(&self) -> u32 {
        INDEX_KEY_ENCODING_VERSION
    }

    pub const fn component_count(&self) -> usize {
        self.component_count
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes.into_vec()
    }
}

impl fmt::Debug for CanonicalIndexKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CanonicalIndexKey")
            .field("encoding_version", &INDEX_KEY_ENCODING_VERSION)
            .field("component_count", &self.component_count)
            .field("bytes", &"<redacted>")
            .finish()
    }
}

fn encode_part(output: &mut Vec<u8>, part: IndexKeyPart<'_>) -> EngineResult<()> {
    let options = OPTIONS_BASE
        | match part.order {
            IndexKeyOrder::Ascending => 0,
            IndexKeyOrder::Descending => OPTIONS_DESCENDING,
        }
        | match part.null_order {
            IndexNullOrder::First => 0,
            IndexNullOrder::Last => OPTIONS_NULLS_LAST,
        };
    output.push(options);
    output.push(match part.collation {
        IndexKeyCollation::Binary => COLLATION_BINARY,
    });

    let is_null = part.value.is_null();
    output.push(null_rank(part.null_order, is_null));
    if is_null {
        return Ok(());
    }

    match part.value {
        IndexKeyValueRef::Null => unreachable!("NULL was handled before payload encoding"),
        IndexKeyValueRef::Boolean(value) => {
            push_ordered(output, TAG_BOOLEAN, part.order);
            push_ordered(output, u8::from(value), part.order);
        }
        IndexKeyValueRef::Int64(value) => {
            push_ordered(output, TAG_INT64, part.order);
            push_fixed(
                output,
                &(value as u64 ^ (1_u64 << 63)).to_be_bytes(),
                part.order,
            );
        }
        IndexKeyValueRef::UInt64(value) => {
            push_ordered(output, TAG_UINT64, part.order);
            push_fixed(output, &value.to_be_bytes(), part.order);
        }
        IndexKeyValueRef::Float64(value) => {
            push_ordered(output, TAG_FLOAT64, part.order);
            push_fixed(output, &ordered_float_bits(value).to_be_bytes(), part.order);
        }
        IndexKeyValueRef::Decimal(_) => {
            return Err(EngineError::new(
                EngineErrorKind::Unsupported,
                "decimal global-index keys are not supported by encoding version 1",
            ));
        }
        IndexKeyValueRef::Text(value) => {
            push_ordered(output, TAG_TEXT, part.order);
            push_variable(output, value.as_bytes(), part.order);
        }
        IndexKeyValueRef::InvalidText(_) => {
            return Err(EngineError::new(
                EngineErrorKind::InvalidTextEncoding,
                "global-index text keys must contain valid UTF-8",
            ));
        }
        IndexKeyValueRef::Binary(value) => {
            push_ordered(output, TAG_BINARY, part.order);
            push_variable(output, value, part.order);
        }
        IndexKeyValueRef::Date(value) => {
            push_ordered(output, TAG_DATE, part.order);
            push_fixed(
                output,
                &(value as u32 ^ (1_u32 << 31)).to_be_bytes(),
                part.order,
            );
        }
        IndexKeyValueRef::Timestamp(value) => {
            push_ordered(output, TAG_TIMESTAMP, part.order);
            push_fixed(
                output,
                &(value as u64 ^ (1_u64 << 63)).to_be_bytes(),
                part.order,
            );
        }
    }
    Ok(())
}

fn null_rank(null_order: IndexNullOrder, is_null: bool) -> u8 {
    match (null_order, is_null) {
        (IndexNullOrder::First, true) | (IndexNullOrder::Last, false) => 0,
        (IndexNullOrder::First, false) | (IndexNullOrder::Last, true) => 1,
    }
}

fn push_fixed(output: &mut Vec<u8>, bytes: &[u8], order: IndexKeyOrder) {
    output.extend(bytes.iter().map(|byte| ordered_byte(*byte, order)));
}

fn push_variable(output: &mut Vec<u8>, bytes: &[u8], order: IndexKeyOrder) {
    for byte in bytes {
        push_ordered(output, *byte, order);
        if *byte == VARIABLE_TERMINATOR {
            push_ordered(output, VARIABLE_ESCAPE, order);
        }
    }
    push_ordered(output, VARIABLE_TERMINATOR, order);
    push_ordered(output, VARIABLE_TERMINATOR, order);
}

fn push_ordered(output: &mut Vec<u8>, byte: u8, order: IndexKeyOrder) {
    output.push(ordered_byte(byte, order));
}

const fn ordered_byte(byte: u8, order: IndexKeyOrder) -> u8 {
    match order {
        IndexKeyOrder::Ascending => byte,
        IndexKeyOrder::Descending => !byte,
    }
}

fn ordered_float_bits(value: f64) -> u64 {
    let bits = if value.is_nan() {
        CANONICAL_NAN_BITS
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    };
    if bits & (1_u64 << 63) == 0 {
        bits ^ (1_u64 << 63)
    } else {
        !bits
    }
}

fn float_from_ordered_bits(ordered: u64) -> f64 {
    let bits = if ordered & (1_u64 << 63) == 0 {
        !ordered
    } else {
        ordered ^ (1_u64 << 63)
    };
    f64::from_bits(bits)
}

fn decode_parts(bytes: &[u8]) -> EngineResult<Vec<DecodedIndexKeyPart>> {
    let version = decode_header(bytes)?;
    if version != INDEX_KEY_ENCODING_VERSION {
        return Err(EngineError::new(
            EngineErrorKind::Unsupported,
            format!("unsupported global-index key encoding version {version}"),
        ));
    }

    let mut cursor = Cursor {
        bytes,
        offset: HEADER_LEN,
    };
    let mut parts = Vec::new();
    while !cursor.is_empty() {
        parts.push(decode_part(&mut cursor)?);
    }
    if parts.is_empty() {
        return Err(corrupt("global-index key has no components"));
    }
    Ok(parts)
}

fn decode_header(bytes: &[u8]) -> EngineResult<u32> {
    if bytes.len() < HEADER_LEN || &bytes[..MAGIC.len()] != MAGIC {
        return Err(corrupt("global-index key has an invalid header"));
    }
    Ok(u32::from_be_bytes(
        bytes[MAGIC.len()..HEADER_LEN]
            .try_into()
            .expect("the checked header contains a version"),
    ))
}

fn decode_part(cursor: &mut Cursor<'_>) -> EngineResult<DecodedIndexKeyPart> {
    let options = cursor.next("component options")?;
    if !(OPTIONS_BASE..=OPTIONS_BASE | OPTIONS_DESCENDING | OPTIONS_NULLS_LAST).contains(&options) {
        return Err(corrupt(
            "global-index key has unsupported component options",
        ));
    }
    let order = if options & OPTIONS_DESCENDING == 0 {
        IndexKeyOrder::Ascending
    } else {
        IndexKeyOrder::Descending
    };
    let null_order = if options & OPTIONS_NULLS_LAST == 0 {
        IndexNullOrder::First
    } else {
        IndexNullOrder::Last
    };
    if cursor.next("component collation")? != COLLATION_BINARY {
        return Err(corrupt(
            "global-index key has unsupported collation metadata",
        ));
    }
    let rank = cursor.next("component NULL rank")?;
    let expected_null_rank = null_rank(null_order, true);
    let non_null_rank = null_rank(null_order, false);
    if rank == expected_null_rank {
        return Ok(DecodedIndexKeyPart {
            value: IndexKeyValue::Null,
            order,
            null_order,
            collation: IndexKeyCollation::Binary,
        });
    }
    if rank != non_null_rank {
        return Err(corrupt("global-index key has an invalid NULL rank"));
    }

    let tag = cursor.next_ordered(order, "component type tag")?;
    let value = match tag {
        TAG_BOOLEAN => match cursor.next_ordered(order, "boolean payload")? {
            0 => IndexKeyValue::Boolean(false),
            1 => IndexKeyValue::Boolean(true),
            _ => return Err(corrupt("global-index key has an invalid boolean payload")),
        },
        TAG_INT64 => {
            let encoded = u64::from_be_bytes(cursor.fixed_ordered(order, "int64 payload")?);
            IndexKeyValue::Int64((encoded ^ (1_u64 << 63)) as i64)
        }
        TAG_UINT64 => IndexKeyValue::UInt64(u64::from_be_bytes(
            cursor.fixed_ordered(order, "uint64 payload")?,
        )),
        TAG_FLOAT64 => IndexKeyValue::Float64(float_from_ordered_bits(u64::from_be_bytes(
            cursor.fixed_ordered(order, "float64 payload")?,
        ))),
        TAG_DATE => {
            let encoded = u32::from_be_bytes(cursor.fixed_ordered(order, "date payload")?);
            IndexKeyValue::Date((encoded ^ (1_u32 << 31)) as i32)
        }
        TAG_TIMESTAMP => {
            let encoded = u64::from_be_bytes(cursor.fixed_ordered(order, "timestamp payload")?);
            IndexKeyValue::Timestamp((encoded ^ (1_u64 << 63)) as i64)
        }
        TAG_TEXT => {
            let bytes = cursor.variable_ordered(order)?;
            let text = String::from_utf8(bytes)
                .map_err(|_| corrupt("global-index text payload is not valid UTF-8"))?;
            IndexKeyValue::Text(text)
        }
        TAG_BINARY => IndexKeyValue::Binary(cursor.variable_ordered(order)?),
        _ => return Err(corrupt("global-index key has an unknown component type")),
    };
    Ok(DecodedIndexKeyPart {
        value,
        order,
        null_order,
        collation: IndexKeyCollation::Binary,
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl Cursor<'_> {
    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn next(&mut self, field: &'static str) -> EngineResult<u8> {
        let Some(byte) = self.bytes.get(self.offset).copied() else {
            return Err(corrupt(format!("global-index key has a truncated {field}")));
        };
        self.offset += 1;
        Ok(byte)
    }

    fn next_ordered(&mut self, order: IndexKeyOrder, field: &'static str) -> EngineResult<u8> {
        self.next(field).map(|byte| ordered_byte(byte, order))
    }

    fn fixed_ordered<const N: usize>(
        &mut self,
        order: IndexKeyOrder,
        field: &'static str,
    ) -> EngineResult<[u8; N]> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or_else(|| corrupt(format!("global-index key has a truncated {field}")))?;
        let Some(bytes) = self.bytes.get(self.offset..end) else {
            return Err(corrupt(format!("global-index key has a truncated {field}")));
        };
        self.offset = end;
        Ok(std::array::from_fn(|index| {
            ordered_byte(bytes[index], order)
        }))
    }

    fn variable_ordered(&mut self, order: IndexKeyOrder) -> EngineResult<Vec<u8>> {
        let mut output = Vec::new();
        loop {
            let byte = self.next_ordered(order, "variable-length payload")?;
            if byte != VARIABLE_TERMINATOR {
                output.push(byte);
                continue;
            }
            match self.next_ordered(order, "variable-length escape")? {
                VARIABLE_TERMINATOR => return Ok(output),
                VARIABLE_ESCAPE => output.push(VARIABLE_TERMINATOR),
                _ => {
                    return Err(corrupt(
                        "global-index key has an invalid variable-length escape",
                    ));
                }
            }
        }
    }
}

fn corrupt(diagnostic: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::DataCorruption, diagnostic)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, panic};

    use proptest::prelude::*;

    use super::*;

    fn encoded(part: IndexKeyPart<'_>) -> CanonicalIndexKey {
        CanonicalIndexKey::encode(&[part]).unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn assert_logical_value(actual: &IndexKeyValue, expected: IndexKeyValueRef<'_>) {
        match (actual, expected) {
            (IndexKeyValue::Null, IndexKeyValueRef::Null) => {}
            (IndexKeyValue::Boolean(actual), IndexKeyValueRef::Boolean(expected)) => {
                assert_eq!(*actual, expected);
            }
            (IndexKeyValue::Int64(actual), IndexKeyValueRef::Int64(expected)) => {
                assert_eq!(*actual, expected);
            }
            (IndexKeyValue::UInt64(actual), IndexKeyValueRef::UInt64(expected)) => {
                assert_eq!(*actual, expected);
            }
            (IndexKeyValue::Float64(actual), IndexKeyValueRef::Float64(expected)) => {
                if expected.is_nan() {
                    assert!(actual.is_nan());
                } else if expected == 0.0 {
                    assert_eq!(actual.to_bits(), 0.0_f64.to_bits());
                } else {
                    assert_eq!(actual.to_bits(), expected.to_bits());
                }
            }
            (IndexKeyValue::Text(actual), IndexKeyValueRef::Text(expected)) => {
                assert_eq!(actual, expected);
            }
            (IndexKeyValue::Binary(actual), IndexKeyValueRef::Binary(expected)) => {
                assert_eq!(actual, expected);
            }
            (IndexKeyValue::Date(actual), IndexKeyValueRef::Date(expected)) => {
                assert_eq!(*actual, expected);
            }
            (IndexKeyValue::Timestamp(actual), IndexKeyValueRef::Timestamp(expected)) => {
                assert_eq!(*actual, expected);
            }
            _ => panic!("decoded value changed its logical type"),
        }
    }

    #[test]
    fn golden_vectors_freeze_version_tags_framing_and_architecture_order() {
        let vectors = [
            (IndexKeyValueRef::Null, "4249445800000001100100"),
            (
                IndexKeyValueRef::Boolean(true),
                "42494458000000011001011001",
            ),
            (
                IndexKeyValueRef::Int64(-1),
                "4249445800000001100101207fffffffffffffff",
            ),
            (
                IndexKeyValueRef::UInt64(1),
                "4249445800000001100101210000000000000001",
            ),
            (
                IndexKeyValueRef::Float64(0.0),
                "4249445800000001100101308000000000000000",
            ),
            (
                IndexKeyValueRef::Date(0),
                "42494458000000011001014080000000",
            ),
            (
                IndexKeyValueRef::Timestamp(0),
                "4249445800000001100101418000000000000000",
            ),
            (
                IndexKeyValueRef::Text("A\0"),
                "4249445800000001100101504100ff0000",
            ),
            (
                IndexKeyValueRef::Binary(&[0, 255]),
                "42494458000000011001015100ffff0000",
            ),
        ];

        for (value, expected) in vectors {
            let key = encoded(IndexKeyPart::ascending(value));
            assert_eq!(hex(key.as_bytes()), expected);
            let decoded = key.decode().unwrap();
            assert_logical_value(decoded[0].value(), value);
        }

        let descending = encoded(IndexKeyPart::descending(IndexKeyValueRef::Text("a")));
        assert_eq!(hex(descending.as_bytes()), "4249445800000001130100af9effff");
    }

    #[test]
    fn compound_boundaries_are_unambiguous_and_round_trip_exactly() {
        let values = [
            IndexKeyPart::ascending(IndexKeyValueRef::Text("a\0b")),
            IndexKeyPart::descending(IndexKeyValueRef::Binary(&[0, 0, 255]))
                .with_null_order(IndexNullOrder::First),
            IndexKeyPart::ascending(IndexKeyValueRef::Int64(i64::MIN)),
            IndexKeyPart::descending(IndexKeyValueRef::Timestamp(i64::MAX)),
        ];
        let key = CanonicalIndexKey::encode(&values).unwrap();
        assert_eq!(key.component_count(), values.len());
        let decoded = key.decode().unwrap();
        assert_eq!(decoded.len(), values.len());
        for (actual, expected) in decoded.iter().zip(values) {
            assert_logical_value(actual.value(), expected.value());
            assert_eq!(actual.order(), expected.order());
            assert_eq!(actual.null_order(), expected.null_order());
            assert_eq!(actual.collation(), expected.collation());
        }
        assert_eq!(CanonicalIndexKey::from_bytes(key.as_bytes()).unwrap(), key);

        let left = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Binary(b"a")));
        let right = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Binary(b"a\0")));
        assert!(left.as_bytes() < right.as_bytes());
    }

    #[test]
    fn null_order_and_unique_semantics_are_explicit() {
        let null_first = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Null));
        let value_first = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Int64(1)));
        assert!(null_first.as_bytes() < value_first.as_bytes());

        let null_last = encoded(
            IndexKeyPart::ascending(IndexKeyValueRef::Null).with_null_order(IndexNullOrder::Last),
        );
        let value_last = encoded(
            IndexKeyPart::ascending(IndexKeyValueRef::Int64(1))
                .with_null_order(IndexNullOrder::Last),
        );
        assert!(null_last.as_bytes() > value_last.as_bytes());

        let parts = [
            IndexKeyPart::ascending(IndexKeyValueRef::Text("tenant")),
            IndexKeyPart::ascending(IndexKeyValueRef::Null),
        ];
        assert_eq!(
            CanonicalIndexKey::encode_unique(&parts, UniqueNullSemantics::Distinct).unwrap(),
            None
        );
        assert!(
            CanonicalIndexKey::encode_unique(&parts, UniqueNullSemantics::NotDistinct)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn float_zero_nan_and_infinities_have_one_frozen_total_order() {
        let negative_zero = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Float64(-0.0)));
        let positive_zero = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Float64(0.0)));
        assert_eq!(negative_zero, positive_zero);

        let negative_nan = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Float64(
            f64::from_bits(0xffff_ffff_ffff_ffff),
        )));
        let positive_nan = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Float64(f64::NAN)));
        assert_eq!(negative_nan, positive_nan);

        let ordered = [f64::NEG_INFINITY, -1.0, -0.0, 1.0, f64::INFINITY, f64::NAN]
            .map(|value| encoded(IndexKeyPart::ascending(IndexKeyValueRef::Float64(value))));
        assert!(
            ordered
                .windows(2)
                .all(|pair| pair[0].as_bytes() < pair[1].as_bytes())
        );
    }

    #[test]
    fn descending_reverses_value_order_without_changing_explicit_null_placement() {
        for values in [
            vec![IndexKeyValueRef::Int64(-2), IndexKeyValueRef::Int64(4)],
            vec![IndexKeyValueRef::Text("a"), IndexKeyValueRef::Text("b")],
            vec![
                IndexKeyValueRef::Binary(b"a"),
                IndexKeyValueRef::Binary(b"a\0"),
            ],
        ] {
            let left = encoded(IndexKeyPart::descending(values[0]));
            let right = encoded(IndexKeyPart::descending(values[1]));
            assert!(left.as_bytes() > right.as_bytes());
        }

        let null = encoded(IndexKeyPart::descending(IndexKeyValueRef::Null));
        let value = encoded(IndexKeyPart::descending(IndexKeyValueRef::Int64(1)));
        assert!(null.as_bytes() > value.as_bytes());
    }

    #[test]
    fn typed_values_cannot_collide_and_core_value_conversion_is_shared() {
        let values = [
            Value::Null,
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Int64(0),
            Value::UInt64(0),
            Value::Float64(0.0),
            Value::Text(String::new()),
            Value::Binary(Vec::new()),
        ];
        let keys = values
            .iter()
            .map(|value| CanonicalIndexKey::encode_values(std::slice::from_ref(value)).unwrap())
            .map(CanonicalIndexKey::into_bytes)
            .collect::<HashSet<_>>();
        assert_eq!(keys.len(), values.len());

        let direct = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Text("shared")));
        let through_core = CanonicalIndexKey::encode_values(&[Value::from("shared")]).unwrap();
        assert_eq!(direct, through_core);
    }

    #[test]
    fn unsupported_types_collations_and_empty_keys_fail_explicitly() {
        let decimal =
            CanonicalIndexKey::encode_values(&[Value::decimal("1.0").unwrap()]).unwrap_err();
        assert_eq!(decimal.kind(), EngineErrorKind::Unsupported);

        let invalid_text =
            CanonicalIndexKey::encode_values(&[Value::InvalidText(vec![0x80])]).unwrap_err();
        assert_eq!(invalid_text.kind(), EngineErrorKind::InvalidTextEncoding);

        assert_eq!(
            IndexKeyCollation::from_name("nocase").unwrap_err().kind(),
            EngineErrorKind::Unsupported
        );
        assert_eq!(
            CanonicalIndexKey::encode(&[]).unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
    }

    #[test]
    fn decoder_rejects_future_truncated_malformed_and_noncanonical_bytes() {
        let valid = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Text("value")));
        for length in 0..valid.as_bytes().len() {
            let error = CanonicalIndexKey::from_bytes(&valid.as_bytes()[..length]).unwrap_err();
            assert!(matches!(
                error.kind(),
                EngineErrorKind::DataCorruption | EngineErrorKind::Unsupported
            ));
        }

        let mut future = valid.as_bytes().to_vec();
        future[HEADER_LEN - 1] = 2;
        assert_eq!(
            CanonicalIndexKey::from_bytes(&future).unwrap_err().kind(),
            EngineErrorKind::Unsupported
        );

        for (offset, value) in [(HEADER_LEN, 0xff), (HEADER_LEN + 1, 0xff)] {
            let mut malformed = valid.as_bytes().to_vec();
            malformed[offset] = value;
            assert_eq!(
                CanonicalIndexKey::from_bytes(&malformed)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::DataCorruption
            );
        }

        let mut invalid_rank = valid.as_bytes().to_vec();
        invalid_rank[HEADER_LEN + 2] = 0xff;
        assert_eq!(
            CanonicalIndexKey::from_bytes(&invalid_rank)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );

        let mut invalid_tag = valid.as_bytes().to_vec();
        invalid_tag[HEADER_LEN + 3] = 0x7f;
        assert_eq!(
            CanonicalIndexKey::from_bytes(&invalid_tag)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );

        let mut invalid_escape = valid.as_bytes().to_vec();
        let end = invalid_escape.len();
        invalid_escape[end - 1] = 1;
        assert_eq!(
            CanonicalIndexKey::from_bytes(&invalid_escape)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn debug_output_never_contains_key_material() {
        let key = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Text(
            "private-index-value",
        )));
        let debug = format!("{key:?}");
        assert!(debug.contains("component_count"));
        assert!(!debug.contains("private-index-value"));
    }

    proptest! {
        #[test]
        fn signed_integer_bytes_preserve_order(left in any::<i64>(), right in any::<i64>()) {
            let left_key = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Int64(left)));
            let right_key = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Int64(right)));
            prop_assert_eq!(left_key.as_bytes().cmp(right_key.as_bytes()), left.cmp(&right));
        }

        #[test]
        fn unsigned_integer_bytes_preserve_order(left in any::<u64>(), right in any::<u64>()) {
            let left_key = encoded(IndexKeyPart::ascending(IndexKeyValueRef::UInt64(left)));
            let right_key = encoded(IndexKeyPart::ascending(IndexKeyValueRef::UInt64(right)));
            prop_assert_eq!(left_key.as_bytes().cmp(right_key.as_bytes()), left.cmp(&right));
        }

        #[test]
        fn float_date_and_timestamp_bytes_preserve_their_defined_order(
            left_float_bits in any::<u64>(),
            right_float_bits in any::<u64>(),
            left_date in any::<i32>(),
            right_date in any::<i32>(),
            left_timestamp in any::<i64>(),
            right_timestamp in any::<i64>(),
        ) {
            let left_float = f64::from_bits(left_float_bits);
            let right_float = f64::from_bits(right_float_bits);
            let left_key = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Float64(left_float)));
            let right_key = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Float64(right_float)));
            prop_assert_eq!(
                left_key.as_bytes().cmp(right_key.as_bytes()),
                ordered_float_bits(left_float).cmp(&ordered_float_bits(right_float)),
            );

            let left_key = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Date(left_date)));
            let right_key = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Date(right_date)));
            prop_assert_eq!(left_key.as_bytes().cmp(right_key.as_bytes()), left_date.cmp(&right_date));

            let left_key = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Timestamp(left_timestamp)));
            let right_key = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Timestamp(right_timestamp)));
            prop_assert_eq!(
                left_key.as_bytes().cmp(right_key.as_bytes()),
                left_timestamp.cmp(&right_timestamp),
            );
        }

        #[test]
        fn different_supported_type_domains_never_collide(
            signed in any::<i64>(),
            unsigned in any::<u64>(),
            bytes in proptest::collection::vec(any::<u8>(), 0..128),
            date in any::<i32>(),
            timestamp in any::<i64>(),
        ) {
            let signed = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Int64(signed)));
            let unsigned = encoded(IndexKeyPart::ascending(IndexKeyValueRef::UInt64(unsigned)));
            let binary = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Binary(&bytes)));
            let date = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Date(date)));
            let timestamp = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Timestamp(timestamp)));
            let keys = [&signed, &unsigned, &binary, &date, &timestamp];
            for (index, left) in keys.iter().enumerate() {
                for right in keys.iter().skip(index + 1) {
                    prop_assert_ne!(left.as_bytes(), right.as_bytes());
                }
            }
        }

        #[test]
        fn text_and_binary_bytes_preserve_binary_order(
            left in proptest::collection::vec(any::<u8>(), 0..128),
            right in proptest::collection::vec(any::<u8>(), 0..128),
        ) {
            let left_key = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Binary(&left)));
            let right_key = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Binary(&right)));
            prop_assert_eq!(left_key.as_bytes().cmp(right_key.as_bytes()), left.cmp(&right));

            if let (Ok(left), Ok(right)) = (
                std::str::from_utf8(&left),
                std::str::from_utf8(&right),
            ) {
                let left_key = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Text(left)));
                let right_key = encoded(IndexKeyPart::ascending(IndexKeyValueRef::Text(right)));
                prop_assert_eq!(left_key.as_bytes().cmp(right_key.as_bytes()), left.cmp(right));
            }
        }

        #[test]
        fn compound_order_is_lexicographic_and_round_trips(
            left_number in any::<i64>(),
            right_number in any::<i64>(),
            left_text in ".{0,64}",
            right_text in ".{0,64}",
        ) {
            let left_parts = [
                IndexKeyPart::ascending(IndexKeyValueRef::Int64(left_number)),
                IndexKeyPart::ascending(IndexKeyValueRef::Text(&left_text)),
            ];
            let right_parts = [
                IndexKeyPart::ascending(IndexKeyValueRef::Int64(right_number)),
                IndexKeyPart::ascending(IndexKeyValueRef::Text(&right_text)),
            ];
            let left = CanonicalIndexKey::encode(&left_parts).unwrap();
            let right = CanonicalIndexKey::encode(&right_parts).unwrap();
            let logical = left_number.cmp(&right_number).then_with(|| left_text.cmp(&right_text));
            prop_assert_eq!(left.as_bytes().cmp(right.as_bytes()), logical);
            prop_assert_eq!(CanonicalIndexKey::from_bytes(left.as_bytes()).unwrap(), left);
            prop_assert_eq!(CanonicalIndexKey::from_bytes(right.as_bytes()).unwrap(), right);
        }

        #[test]
        fn arbitrary_bytes_never_panic_the_decoder(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
            let result = panic::catch_unwind(|| CanonicalIndexKey::from_bytes(&bytes));
            prop_assert!(result.is_ok());
            if let Ok(Ok(key)) = result {
                prop_assert_eq!(key.as_bytes(), bytes.as_slice());
            }
        }
    }
}
