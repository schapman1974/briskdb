//! Stable semantic identity keys for BSON values.

use std::fmt;

use super::{BsonDocument, BsonError, BsonErrorKind, BsonRegex, BsonResult, BsonValue};
use crate::document::number::{
    CanonicalFinite, CanonicalNumber, MAX_CANONICAL_COEFFICIENT_BYTES, MAX_CANONICAL_EXPONENT_FIVE,
    MAX_CANONICAL_EXPONENT_TWO, MAX_DECIMAL128_COEFFICIENT, MIN_CANONICAL_EXPONENT_FIVE,
    MIN_CANONICAL_EXPONENT_TWO,
};

/// Current canonical BSON identity-key encoding.
pub const BSON_KEY_ENCODING_VERSION: u32 = 1;

/// Maximum encoded size of one canonical BSON identity key.
pub const BSON_MAX_CANONICAL_KEY_BYTES: usize = 16 * 1024 * 1024;

const MAGIC: &[u8; 4] = b"BBKY";
const HEADER_LEN: usize = MAGIC.len() + size_of::<u32>();
const MAX_KEY_NESTING_DEPTH: usize = 100;

const TAG_MIN_KEY: u8 = 0;
const TAG_NULL: u8 = 1;
const TAG_NUMBER: u8 = 2;
const TAG_STRING: u8 = 3;
const TAG_DOCUMENT: u8 = 4;
const TAG_ARRAY: u8 = 5;
const TAG_BINARY: u8 = 6;
const TAG_OBJECT_ID: u8 = 7;
const TAG_BOOLEAN: u8 = 8;
const TAG_DATETIME: u8 = 9;
const TAG_TIMESTAMP: u8 = 10;
const TAG_REGEX: u8 = 11;
const TAG_JAVASCRIPT: u8 = 12;
const TAG_JAVASCRIPT_WITH_SCOPE: u8 = 13;
const TAG_MAX_KEY: u8 = 14;

const NUMBER_NAN: u8 = 0;
const NUMBER_NEGATIVE_INFINITY: u8 = 1;
const NUMBER_FINITE: u8 = 2;
const NUMBER_POSITIVE_INFINITY: u8 = 3;

const FINITE_ZERO: u8 = 0;
const FINITE_POSITIVE: u8 = 1;
const FINITE_NEGATIVE: u8 = 2;

/// Validated, versioned bytes for one BSON semantic identity.
///
/// Numerically equal Int32, Int64, Double, and Decimal128 values have the same
/// key. UUIDs have the same key as the exact BSON Binary subtype/payload their
/// configured representation produces. Document field order and duplicate
/// occurrences remain significant. These bytes are designed for equality,
/// hashing, routing, grouping, and unique reservations; their lexicographic
/// order is not the BSON sort order.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CanonicalBsonKey {
    bytes: Box<[u8]>,
}

impl CanonicalBsonKey {
    /// Encode one BSON value using the current semantic identity format.
    pub fn encode(value: &BsonValue) -> BsonResult<Self> {
        let mut bytes = Vec::with_capacity(HEADER_LEN + 16);
        extend_key_bytes(&mut bytes, MAGIC)?;
        extend_key_bytes(&mut bytes, &BSON_KEY_ENCODING_VERSION.to_be_bytes())?;
        encode_value(&mut bytes, value, 0)?;
        Ok(Self {
            bytes: bytes.into_boxed_slice(),
        })
    }

    /// Validate and own canonical identity bytes read from storage.
    pub fn from_bytes(bytes: &[u8]) -> BsonResult<Self> {
        if bytes.len() > BSON_MAX_CANONICAL_KEY_BYTES {
            return Err(oversized_key(bytes.len()));
        }
        if bytes.len() < HEADER_LEN {
            return Err(invalid_key("canonical BSON key is shorter than its header"));
        }
        if &bytes[..MAGIC.len()] != MAGIC {
            return Err(invalid_key("canonical BSON key has an invalid magic value"));
        }
        let version = u32::from_be_bytes(
            bytes[MAGIC.len()..HEADER_LEN]
                .try_into()
                .expect("the canonical key header has a fixed width"),
        );
        if version != BSON_KEY_ENCODING_VERSION {
            return Err(invalid_key(
                "canonical BSON key uses an unsupported version",
            ));
        }

        let mut cursor = Cursor::new(&bytes[HEADER_LEN..]);
        validate_value(&mut cursor, 0)?;
        if !cursor.is_finished() {
            return Err(invalid_key("canonical BSON key has trailing bytes"));
        }
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(bytes.len())
            .map_err(|_| key_allocation_failure())?;
        owned.extend_from_slice(bytes);
        Ok(Self {
            bytes: owned.into_boxed_slice(),
        })
    }

    pub const fn encoding_version(&self) -> u32 {
        BSON_KEY_ENCODING_VERSION
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes.into_vec()
    }
}

impl fmt::Debug for CanonicalBsonKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CanonicalBsonKey")
            .field("encoding_version", &BSON_KEY_ENCODING_VERSION)
            .field("bytes", &"<redacted>")
            .finish()
    }
}

fn encode_value(output: &mut Vec<u8>, value: &BsonValue, depth: usize) -> BsonResult<()> {
    ensure_key_depth(depth)?;

    if let Some(number) = value.canonical_number() {
        push_key_byte(output, TAG_NUMBER)?;
        encode_number(output, &number)?;
        return Ok(());
    }
    if let Some(result) = value.with_binary_parts(|subtype, bytes| {
        push_key_byte(output, TAG_BINARY)?;
        push_key_byte(output, subtype)?;
        push_bytes(output, bytes)
    }) {
        return result;
    }

    match value {
        BsonValue::MinKey => push_key_byte(output, TAG_MIN_KEY)?,
        BsonValue::Null => push_key_byte(output, TAG_NULL)?,
        BsonValue::String(value) => {
            push_key_byte(output, TAG_STRING)?;
            push_string(output, value)?;
        }
        BsonValue::Document(document) => {
            push_key_byte(output, TAG_DOCUMENT)?;
            encode_document(output, document, depth + 1)?;
        }
        BsonValue::Array(values) => {
            ensure_key_depth(depth + 1)?;
            push_key_byte(output, TAG_ARRAY)?;
            push_len(output, values.len())?;
            for value in values {
                encode_value(output, value, depth + 1)?;
            }
        }
        BsonValue::ObjectId(value) => {
            push_key_byte(output, TAG_OBJECT_ID)?;
            extend_key_bytes(output, &value.bytes())?;
        }
        BsonValue::Boolean(value) => {
            push_key_byte(output, TAG_BOOLEAN)?;
            push_key_byte(output, u8::from(*value))?;
        }
        BsonValue::DateTime(value) => {
            push_key_byte(output, TAG_DATETIME)?;
            extend_key_bytes(output, &value.timestamp_millis().to_be_bytes())?;
        }
        BsonValue::Timestamp(value) => {
            push_key_byte(output, TAG_TIMESTAMP)?;
            extend_key_bytes(output, &value.time().to_be_bytes())?;
            extend_key_bytes(output, &value.increment().to_be_bytes())?;
        }
        BsonValue::RegularExpression(value) => {
            push_key_byte(output, TAG_REGEX)?;
            push_string(output, value.pattern())?;
            push_string(output, value.options())?;
        }
        BsonValue::JavaScript(value) => {
            if let Some(scope) = value.scope() {
                push_key_byte(output, TAG_JAVASCRIPT_WITH_SCOPE)?;
                push_string(output, value.code())?;
                encode_document(output, scope, depth + 1)?;
            } else {
                push_key_byte(output, TAG_JAVASCRIPT)?;
                push_string(output, value.code())?;
            }
        }
        BsonValue::MaxKey => push_key_byte(output, TAG_MAX_KEY)?,
        BsonValue::Double(_)
        | BsonValue::Binary(_)
        | BsonValue::Uuid(_)
        | BsonValue::Int32(_)
        | BsonValue::Int64(_)
        | BsonValue::Decimal128(_) => {
            unreachable!("numeric and binary families were encoded above")
        }
    }
    Ok(())
}

fn encode_document(output: &mut Vec<u8>, document: &BsonDocument, depth: usize) -> BsonResult<()> {
    ensure_key_depth(depth)?;
    push_len(output, document.len())?;
    for (name, value) in document.iter() {
        push_string(output, name)?;
        encode_value(output, value, depth)?;
    }
    Ok(())
}

fn encode_number(output: &mut Vec<u8>, number: &CanonicalNumber) -> BsonResult<()> {
    match number {
        CanonicalNumber::NaN => push_key_byte(output, NUMBER_NAN)?,
        CanonicalNumber::NegativeInfinity => {
            push_key_byte(output, NUMBER_NEGATIVE_INFINITY)?;
        }
        CanonicalNumber::PositiveInfinity => {
            push_key_byte(output, NUMBER_POSITIVE_INFINITY)?;
        }
        CanonicalNumber::Finite(value) => {
            push_key_byte(output, NUMBER_FINITE)?;
            encode_finite(output, *value)?;
        }
    }
    Ok(())
}

fn encode_finite(output: &mut Vec<u8>, value: CanonicalFinite) -> BsonResult<()> {
    let coefficient = value.coefficient();
    push_key_byte(
        output,
        if coefficient == 0 {
            FINITE_ZERO
        } else if value.is_negative() {
            FINITE_NEGATIVE
        } else {
            FINITE_POSITIVE
        },
    )?;

    if coefficient == 0 {
        push_bytes(output, &[])?;
    } else {
        let bytes = coefficient.to_be_bytes();
        let first_nonzero = bytes
            .iter()
            .position(|byte| *byte != 0)
            .expect("a nonzero coefficient has a nonzero byte");
        push_bytes(output, &bytes[first_nonzero..])?;
    }
    extend_key_bytes(output, &value.exponent_two().to_be_bytes())?;
    extend_key_bytes(output, &value.exponent_five().to_be_bytes())
}

fn push_string(output: &mut Vec<u8>, value: &str) -> BsonResult<()> {
    push_bytes(output, value.as_bytes())
}

fn push_bytes(output: &mut Vec<u8>, value: &[u8]) -> BsonResult<()> {
    push_len(output, value.len())?;
    extend_key_bytes(output, value)
}

fn push_len(output: &mut Vec<u8>, length: usize) -> BsonResult<()> {
    let length = u32::try_from(length).map_err(|_| {
        BsonError::new(
            BsonErrorKind::Oversized,
            "canonical BSON key component exceeds the supported size",
        )
    })?;
    extend_key_bytes(output, &length.to_be_bytes())
}

fn push_key_byte(output: &mut Vec<u8>, value: u8) -> BsonResult<()> {
    ensure_key_growth(output, 1)?;
    output.push(value);
    Ok(())
}

fn extend_key_bytes(output: &mut Vec<u8>, value: &[u8]) -> BsonResult<()> {
    ensure_key_growth(output, value.len())?;
    output.extend_from_slice(value);
    Ok(())
}

fn ensure_key_growth(output: &mut Vec<u8>, additional: usize) -> BsonResult<()> {
    let new_len = output
        .len()
        .checked_add(additional)
        .ok_or_else(|| oversized_key(usize::MAX))?;
    if new_len > BSON_MAX_CANONICAL_KEY_BYTES {
        return Err(oversized_key(new_len));
    }
    if new_len <= output.capacity() {
        return Ok(());
    }

    let doubled = output
        .capacity()
        .saturating_mul(2)
        .min(BSON_MAX_CANONICAL_KEY_BYTES);
    let target_capacity = doubled.max(new_len);
    output
        .try_reserve_exact(target_capacity - output.len())
        .map_err(|_| key_allocation_failure())
}

fn validate_value(cursor: &mut Cursor<'_>, depth: usize) -> BsonResult<()> {
    validate_key_depth(depth)?;
    match cursor.read_u8()? {
        TAG_MIN_KEY | TAG_NULL | TAG_MAX_KEY => {}
        TAG_NUMBER => validate_number(cursor)?,
        TAG_STRING => {
            cursor.read_string()?;
        }
        TAG_DOCUMENT => validate_document(cursor, depth + 1)?,
        TAG_ARRAY => {
            validate_key_depth(depth + 1)?;
            let count = cursor.read_u32()?;
            for _ in 0..count {
                validate_value(cursor, depth + 1)?;
            }
        }
        TAG_BINARY => {
            cursor.read_u8()?;
            cursor.read_bytes()?;
        }
        TAG_OBJECT_ID => {
            cursor.take(12)?;
        }
        TAG_BOOLEAN => {
            if cursor.read_u8()? > 1 {
                return Err(invalid_key("canonical BSON key has an invalid boolean"));
            }
        }
        TAG_DATETIME => {
            cursor.take(size_of::<i64>())?;
        }
        TAG_TIMESTAMP => {
            cursor.take(size_of::<u32>() * 2)?;
        }
        TAG_REGEX => {
            let pattern = cursor.read_string()?;
            let options = cursor.read_string()?;
            let regex = BsonRegex::new(pattern, options)
                .map_err(|_| invalid_key("canonical BSON key has an invalid regex"))?;
            if regex.options() != options {
                return Err(invalid_key(
                    "canonical BSON key has noncanonical regex options",
                ));
            }
        }
        TAG_JAVASCRIPT => {
            cursor.read_string()?;
        }
        TAG_JAVASCRIPT_WITH_SCOPE => {
            cursor.read_string()?;
            validate_document(cursor, depth + 1)?;
        }
        _ => return Err(invalid_key("canonical BSON key has an unknown type tag")),
    }
    Ok(())
}

fn validate_document(cursor: &mut Cursor<'_>, depth: usize) -> BsonResult<()> {
    validate_key_depth(depth)?;
    let count = cursor.read_u32()?;
    for _ in 0..count {
        let name = cursor.read_string()?;
        if name.contains('\0') {
            return Err(invalid_key(
                "canonical BSON key contains NUL in a field name",
            ));
        }
        validate_value(cursor, depth)?;
    }
    Ok(())
}

fn validate_number(cursor: &mut Cursor<'_>) -> BsonResult<()> {
    match cursor.read_u8()? {
        NUMBER_NAN | NUMBER_NEGATIVE_INFINITY | NUMBER_POSITIVE_INFINITY => Ok(()),
        NUMBER_FINITE => {
            let sign = cursor.read_u8()?;
            let coefficient_bytes = cursor.read_bytes()?;
            validate_magnitude(coefficient_bytes)?;
            if coefficient_bytes.len() > MAX_CANONICAL_COEFFICIENT_BYTES {
                return Err(invalid_key(
                    "canonical BSON key finite coefficient exceeds the supported size",
                ));
            }
            let coefficient = coefficient_bytes
                .iter()
                .fold(0_u128, |value, byte| (value << 8) | u128::from(*byte));
            let exponent_two = cursor.read_i16()?;
            let exponent_five = cursor.read_i16()?;

            match sign {
                FINITE_ZERO
                    if coefficient_bytes.is_empty() && exponent_two == 0 && exponent_five == 0 =>
                {
                    Ok(())
                }
                FINITE_POSITIVE | FINITE_NEGATIVE if !coefficient_bytes.is_empty() => {
                    if coefficient > MAX_DECIMAL128_COEFFICIENT {
                        return Err(invalid_key(
                            "canonical BSON key finite coefficient exceeds the BSON numeric domain",
                        ));
                    }
                    if coefficient % 2 == 0 || coefficient % 5 == 0 {
                        return Err(invalid_key(
                            "canonical BSON key finite coefficient is not factorized",
                        ));
                    }
                    if !(MIN_CANONICAL_EXPONENT_TWO..=MAX_CANONICAL_EXPONENT_TWO)
                        .contains(&exponent_two)
                        || !(MIN_CANONICAL_EXPONENT_FIVE..=MAX_CANONICAL_EXPONENT_FIVE)
                            .contains(&exponent_five)
                    {
                        return Err(invalid_key(
                            "canonical BSON key finite exponent is outside the supported domain",
                        ));
                    }
                    Ok(())
                }
                _ => Err(invalid_key(
                    "canonical BSON key has a noncanonical finite number",
                )),
            }
        }
        _ => Err(invalid_key("canonical BSON key has an unknown numeric tag")),
    }
}

fn validate_magnitude(bytes: &[u8]) -> BsonResult<()> {
    if bytes.first() == Some(&0) {
        return Err(invalid_key(
            "canonical BSON key integer magnitude has leading zeroes",
        ));
    }
    Ok(())
}

fn ensure_key_depth(depth: usize) -> BsonResult<()> {
    if depth > MAX_KEY_NESTING_DEPTH {
        return Err(BsonError::new(
            BsonErrorKind::NestingLimit,
            "canonical BSON key exceeds the nesting limit",
        ));
    }
    Ok(())
}

fn validate_key_depth(depth: usize) -> BsonResult<()> {
    if depth > MAX_KEY_NESTING_DEPTH {
        return Err(invalid_key("canonical BSON key exceeds the nesting limit"));
    }
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn is_finished(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn take(&mut self, length: usize) -> BsonResult<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| invalid_key("canonical BSON key length overflows"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| invalid_key("canonical BSON key is truncated"))?;
        self.position = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> BsonResult<u8> {
        Ok(self.take(1)?[0])
    }

    fn read_u32(&mut self) -> BsonResult<u32> {
        Ok(u32::from_be_bytes(
            self.take(size_of::<u32>())?
                .try_into()
                .expect("the cursor returned the requested fixed width"),
        ))
    }

    fn read_i16(&mut self) -> BsonResult<i16> {
        Ok(i16::from_be_bytes(
            self.take(size_of::<i16>())?
                .try_into()
                .expect("the cursor returned the requested fixed width"),
        ))
    }

    fn read_bytes(&mut self) -> BsonResult<&'a [u8]> {
        let length = usize::try_from(self.read_u32()?)
            .map_err(|_| invalid_key("canonical BSON key length is unsupported"))?;
        self.take(length)
    }

    fn read_string(&mut self) -> BsonResult<&'a str> {
        std::str::from_utf8(self.read_bytes()?)
            .map_err(|_| invalid_key("canonical BSON key contains invalid UTF-8"))
    }
}

fn invalid_key(diagnostic: &'static str) -> BsonError {
    BsonError::new(BsonErrorKind::InvalidCanonicalKey, diagnostic)
}

fn oversized_key(actual: usize) -> BsonError {
    BsonError::new(
        BsonErrorKind::Oversized,
        format!(
            "canonical BSON key contains {actual} bytes, exceeding the supported \
             {BSON_MAX_CANONICAL_KEY_BYTES}-byte limit"
        ),
    )
}

fn key_allocation_failure() -> BsonError {
    BsonError::new(
        BsonErrorKind::Oversized,
        "unable to reserve bounded canonical BSON key storage",
    )
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use proptest::prelude::*;

    use super::*;

    #[test]
    fn encoded_zero_is_minimal_and_self_validating() {
        let key = CanonicalBsonKey::encode(&BsonValue::Int32(0)).unwrap();
        assert_eq!(
            key.as_bytes(),
            &[
                b'B',
                b'B',
                b'K',
                b'Y',
                0,
                0,
                0,
                1,
                TAG_NUMBER,
                NUMBER_FINITE,
                FINITE_ZERO,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ]
        );
        assert_eq!(CanonicalBsonKey::from_bytes(key.as_bytes()).unwrap(), key);
    }

    #[test]
    fn key_depth_limit_applies_before_recursive_descent() {
        let mut accepted = BsonValue::Null;
        for _ in 0..MAX_KEY_NESTING_DEPTH {
            accepted = BsonValue::Array(vec![accepted]);
        }
        let key = CanonicalBsonKey::encode(&accepted).unwrap();
        assert_eq!(CanonicalBsonKey::from_bytes(key.as_bytes()).unwrap(), key);

        let rejected = BsonValue::Array(vec![accepted]);
        assert_eq!(
            CanonicalBsonKey::encode(&rejected).unwrap_err().kind(),
            BsonErrorKind::NestingLimit
        );
    }

    fn nested_empty_array(depth: usize) -> BsonValue {
        assert!(depth >= 1);
        let mut value = BsonValue::Array(Vec::new());
        for _ in 1..depth {
            value = BsonValue::Array(vec![value]);
        }
        value
    }

    fn nested_empty_document(depth: usize) -> BsonValue {
        assert!(depth >= 1);
        let mut value = BsonValue::Document(BsonDocument::new());
        for _ in 1..depth {
            value = BsonValue::Document(
                BsonDocument::from_entries([("nested", value)]).expect("valid field name"),
            );
        }
        value
    }

    #[test]
    fn key_depth_limit_counts_empty_array_and_document_containers() {
        for accepted in [
            nested_empty_array(MAX_KEY_NESTING_DEPTH),
            nested_empty_document(MAX_KEY_NESTING_DEPTH),
        ] {
            let key = CanonicalBsonKey::encode(&accepted).unwrap();
            assert_eq!(CanonicalBsonKey::from_bytes(key.as_bytes()).unwrap(), key);
        }

        for rejected in [
            nested_empty_array(MAX_KEY_NESTING_DEPTH + 1),
            nested_empty_document(MAX_KEY_NESTING_DEPTH + 1),
        ] {
            assert_eq!(
                CanonicalBsonKey::encode(&rejected).unwrap_err().kind(),
                BsonErrorKind::NestingLimit
            );
        }

        let mut fabricated = [MAGIC.as_slice(), &BSON_KEY_ENCODING_VERSION.to_be_bytes()].concat();
        for depth in 0..=MAX_KEY_NESTING_DEPTH {
            fabricated.push(TAG_ARRAY);
            fabricated.extend_from_slice(&(u32::from(depth < MAX_KEY_NESTING_DEPTH)).to_be_bytes());
        }
        assert_eq!(
            CanonicalBsonKey::from_bytes(&fabricated)
                .unwrap_err()
                .kind(),
            BsonErrorKind::InvalidCanonicalKey
        );
    }

    #[test]
    fn canonical_key_size_limit_is_inclusive_and_checked_on_input() {
        let payload_len = BSON_MAX_CANONICAL_KEY_BYTES - HEADER_LEN - 1 - size_of::<u32>();
        let at_limit = BsonValue::String("x".repeat(payload_len));
        let key = CanonicalBsonKey::encode(&at_limit).unwrap();
        assert_eq!(key.as_bytes().len(), BSON_MAX_CANONICAL_KEY_BYTES);
        assert_eq!(CanonicalBsonKey::from_bytes(key.as_bytes()).unwrap(), key);

        let over_limit = BsonValue::String("x".repeat(payload_len + 1));
        assert_eq!(
            CanonicalBsonKey::encode(&over_limit).unwrap_err().kind(),
            BsonErrorKind::Oversized
        );

        let oversized_input = vec![0; BSON_MAX_CANONICAL_KEY_BYTES + 1];
        assert_eq!(
            CanonicalBsonKey::from_bytes(&oversized_input)
                .unwrap_err()
                .kind(),
            BsonErrorKind::Oversized
        );
    }

    fn finite_key(sign: u8, coefficient: &[u8], exponent_two: i16, exponent_five: i16) -> Vec<u8> {
        let mut bytes = [MAGIC.as_slice(), &BSON_KEY_ENCODING_VERSION.to_be_bytes()].concat();
        bytes.extend_from_slice(&[TAG_NUMBER, NUMBER_FINITE, sign]);
        bytes.extend_from_slice(&u32::try_from(coefficient.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(coefficient);
        bytes.extend_from_slice(&exponent_two.to_be_bytes());
        bytes.extend_from_slice(&exponent_five.to_be_bytes());
        bytes
    }

    #[test]
    fn finite_number_parser_requires_the_unique_compact_factorization() {
        for valid in [
            finite_key(FINITE_ZERO, &[], 0, 0),
            finite_key(
                FINITE_POSITIVE,
                &[1],
                MIN_CANONICAL_EXPONENT_TWO,
                MIN_CANONICAL_EXPONENT_FIVE,
            ),
            finite_key(
                FINITE_NEGATIVE,
                &[3],
                MAX_CANONICAL_EXPONENT_TWO,
                MAX_CANONICAL_EXPONENT_FIVE,
            ),
        ] {
            CanonicalBsonKey::from_bytes(&valid).unwrap();
        }

        let oversized_coefficient = [1_u8; MAX_CANONICAL_COEFFICIENT_BYTES + 1];
        let above_domain = (MAX_DECIMAL128_COEFFICIENT + 1).to_be_bytes();
        let first = above_domain.iter().position(|byte| *byte != 0).unwrap();
        for invalid in [
            finite_key(FINITE_ZERO, &[1], 0, 0),
            finite_key(FINITE_ZERO, &[], 1, 0),
            finite_key(FINITE_POSITIVE, &[], 0, 0),
            finite_key(FINITE_POSITIVE, &[0, 1], 0, 0),
            finite_key(FINITE_POSITIVE, &oversized_coefficient, 0, 0),
            finite_key(FINITE_POSITIVE, &above_domain[first..], 0, 0),
            finite_key(FINITE_POSITIVE, &[2], 0, 0),
            finite_key(FINITE_POSITIVE, &[5], 0, 0),
            finite_key(FINITE_POSITIVE, &[1], MIN_CANONICAL_EXPONENT_TWO - 1, 0),
            finite_key(FINITE_POSITIVE, &[1], MAX_CANONICAL_EXPONENT_TWO + 1, 0),
            finite_key(FINITE_POSITIVE, &[1], 0, MIN_CANONICAL_EXPONENT_FIVE - 1),
            finite_key(FINITE_POSITIVE, &[1], 0, MAX_CANONICAL_EXPONENT_FIVE + 1),
            finite_key(3, &[1], 0, 0),
        ] {
            assert_eq!(
                CanonicalBsonKey::from_bytes(&invalid).unwrap_err().kind(),
                BsonErrorKind::InvalidCanonicalKey
            );
        }
    }

    #[test]
    fn decimal_extremes_have_small_self_validating_keys() {
        let mut lengths = Vec::new();

        for decimal in ["1E-6176", "9.999999999999999999999999999999999E+6144"] {
            let bid = bson::Decimal128::from_str(decimal).unwrap().bytes();
            let key = CanonicalBsonKey::encode(&BsonValue::Decimal128(
                crate::document::BsonDecimal128::from_bid(bid),
            ))
            .unwrap();
            assert_eq!(CanonicalBsonKey::from_bytes(key.as_bytes()).unwrap(), key);
            lengths.push(key.as_bytes().len());
        }
        assert!(lengths.into_iter().all(|length| length <= 34));
    }

    proptest! {
        #[test]
        fn arbitrary_key_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..2048)) {
            let _ = CanonicalBsonKey::from_bytes(&bytes);
        }
    }
}
