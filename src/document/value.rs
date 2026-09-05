use std::{
    cmp::Ordering,
    collections::HashSet,
    fmt,
    hash::{Hash, Hasher},
    str::FromStr,
};

use super::{BsonError, BsonErrorKind, BsonResult};
use crate::document::number::CanonicalNumber;

/// An ordered BSON document.
///
/// Entries are stored as a sequence rather than a map so field order and every
/// duplicate occurrence survive an explicitly duplicate-preserving decode.
/// Lookup methods make the caller choose first, last, all, or unique semantics
/// instead of silently overwriting one occurrence.
#[derive(Debug, Clone, Default)]
pub struct BsonDocument {
    entries: Vec<(String, BsonValue)>,
}

impl BsonDocument {
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn from_entries<I, K>(entries: I) -> BsonResult<Self>
    where
        I: IntoIterator<Item = (K, BsonValue)>,
        K: Into<String>,
    {
        let mut document = Self::new();
        for (name, value) in entries {
            document.push(name, value)?;
        }
        Ok(document)
    }

    pub fn push(&mut self, name: impl Into<String>, value: BsonValue) -> BsonResult<()> {
        let name = name.into();
        if name.contains('\0') {
            return Err(invalid_value("BSON field names cannot contain NUL"));
        }
        self.entries.push((name, value));
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn try_reserve(
        &mut self,
        additional: usize,
    ) -> Result<(), std::collections::TryReserveError> {
        self.entries.try_reserve_exact(additional)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&str, &BsonValue)> + ExactSizeIterator {
        self.entries
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    pub fn into_entries(self) -> Vec<(String, BsonValue)> {
        self.entries
    }

    pub fn get_first(&self, name: &str) -> Option<&BsonValue> {
        self.entries
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    }

    pub fn get_last(&self, name: &str) -> Option<&BsonValue> {
        self.entries
            .iter()
            .rev()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    }

    pub fn get_all<'a>(
        &'a self,
        name: &'a str,
    ) -> impl DoubleEndedIterator<Item = &'a BsonValue> + 'a {
        self.entries
            .iter()
            .filter_map(move |(candidate, value)| (candidate == name).then_some(value))
    }

    pub fn get_unique(&self, name: &str) -> BsonResult<Option<&BsonValue>> {
        let mut found = None;
        for (candidate, value) in &self.entries {
            if candidate == name {
                if found.is_some() {
                    return Err(duplicate_field());
                }
                found = Some(value);
            }
        }
        Ok(found)
    }

    /// Require unique names among this document's direct entries.
    ///
    /// Nested documents are independent values and validate themselves when a
    /// bounded codec traversal reaches them.
    pub fn validate_unique(&self) -> BsonResult<()> {
        let mut names = HashSet::new();
        names.try_reserve(self.entries.len()).map_err(|_| {
            BsonError::new(
                BsonErrorKind::Oversized,
                "unable to reserve bounded BSON duplicate-validation storage",
            )
        })?;
        if self.entries.iter().any(|(name, _)| !names.insert(name)) {
            return Err(duplicate_field());
        }
        Ok(())
    }

    pub fn representation_eq(&self, other: &Self) -> bool {
        self.entries.len() == other.entries.len()
            && self.entries.iter().zip(&other.entries).all(
                |((left_name, left_value), (right_name, right_value))| {
                    left_name == right_name && left_value.representation_eq(right_value)
                },
            )
    }
}

impl PartialEq for BsonDocument {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}

impl Eq for BsonDocument {}

impl Hash for BsonDocument {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.entries.len().hash(state);
        for (name, value) in &self.entries {
            name.hash(state);
            value.hash(state);
        }
    }
}

impl PartialOrd for BsonDocument {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BsonDocument {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut left = self.entries.iter();
        let mut right = other.entries.iter();
        loop {
            match (left.next(), right.next()) {
                (Some((left_name, left_value)), Some((right_name, right_value))) => {
                    let ordering = left_value
                        .type_rank()
                        .cmp(&right_value.type_rank())
                        .then_with(|| left_name.cmp(right_name))
                        .then_with(|| left_value.cmp(right_value));
                    if ordering != Ordering::Equal {
                        return ordering;
                    }
                }
                (Some(_), None) => return Ordering::Greater,
                (None, Some(_)) => return Ordering::Less,
                (None, None) => return Ordering::Equal,
            }
        }
    }
}

/// BSON binary data with its exact subtype.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BsonBinary {
    subtype: u8,
    bytes: Vec<u8>,
}

impl BsonBinary {
    pub fn new(subtype: u8, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            subtype,
            bytes: bytes.into(),
        }
    }

    pub const fn subtype(&self) -> u8 {
        self.subtype
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    fn encoded_len(&self) -> usize {
        self.bytes
            .len()
            .saturating_add(usize::from(self.subtype == 2) * 4)
    }
}

impl PartialOrd for BsonBinary {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BsonBinary {
    fn cmp(&self, other: &Self) -> Ordering {
        self.encoded_len()
            .cmp(&other.encoded_len())
            .then_with(|| self.subtype.cmp(&other.subtype))
            .then_with(|| self.bytes.cmp(&other.bytes))
    }
}

/// A 12-byte BSON ObjectId.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BsonObjectId([u8; 12]);

impl BsonObjectId {
    pub const fn from_bytes(bytes: [u8; 12]) -> Self {
        Self(bytes)
    }

    pub const fn bytes(self) -> [u8; 12] {
        self.0
    }

    pub fn from_hex(value: &str) -> BsonResult<Self> {
        if value.len() != 24 {
            return Err(invalid_value(
                "a BSON ObjectId hex value must contain 24 characters",
            ));
        }
        let mut bytes = [0_u8; 12];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = hex_nibble(pair[0]).ok_or_else(|| invalid_value("invalid ObjectId hex"))?;
            let low = hex_nibble(pair[1]).ok_or_else(|| invalid_value("invalid ObjectId hex"))?;
            bytes[index] = (high << 4) | low;
        }
        Ok(Self(bytes))
    }

    pub fn to_hex(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(24);
        for byte in self.0 {
            output.push(char::from(HEX[usize::from(byte >> 4)]));
            output.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        output
    }
}

impl fmt::Display for BsonObjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

impl FromStr for BsonObjectId {
    type Err = BsonError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_hex(value)
    }
}

/// Signed UTC milliseconds from the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BsonDateTime(i64);

impl BsonDateTime {
    pub const fn from_millis(milliseconds: i64) -> Self {
        Self(milliseconds)
    }

    /// Canonicalize signed microseconds to BSON's millisecond precision.
    ///
    /// Euclidean division is required before the Unix epoch: negative one
    /// microsecond belongs to millisecond `-1`, rather than truncating to zero.
    pub const fn from_micros(microseconds: i64) -> Self {
        Self(microseconds.div_euclid(1_000))
    }

    pub const fn timestamp_millis(self) -> i64 {
        self.0
    }
}

/// BSON's timestamp value, ordered by seconds and then increment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BsonTimestamp {
    time: u32,
    increment: u32,
}

impl BsonTimestamp {
    pub const fn new(time: u32, increment: u32) -> Self {
        Self { time, increment }
    }

    pub const fn time(self) -> u32 {
        self.time
    }

    pub const fn increment(self) -> u32 {
        self.increment
    }
}

/// A canonical BSON regular expression.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BsonRegex {
    pattern: String,
    options: String,
}

impl BsonRegex {
    pub fn new(pattern: impl Into<String>, options: impl AsRef<str>) -> BsonResult<Self> {
        let pattern = pattern.into();
        if pattern.contains('\0') {
            return Err(invalid_value("BSON regex patterns cannot contain NUL"));
        }

        let mut flags = [false; 6];
        for byte in options.as_ref().bytes() {
            let index = match byte {
                b'i' => 0,
                b'l' => 1,
                b'm' => 2,
                b's' => 3,
                b'u' => 4,
                b'x' => 5,
                _ => {
                    return Err(invalid_value(
                        "BSON regex options may contain only i, l, m, s, u, and x",
                    ));
                }
            };
            flags[index] = true;
        }
        let options = b"ilmsux"
            .iter()
            .zip(flags)
            .filter_map(|(flag, enabled)| enabled.then_some(char::from(*flag)))
            .collect();
        Ok(Self { pattern, options })
    }

    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    pub fn options(&self) -> &str {
        &self.options
    }
}

/// BSON JavaScript code, optionally carrying an ordered scope document.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BsonJavaScript {
    code: String,
    scope: Option<BsonDocument>,
}

impl BsonJavaScript {
    pub fn new(code: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            scope: None,
        }
    }

    pub fn with_scope(code: impl Into<String>, scope: BsonDocument) -> Self {
        Self {
            code: code.into(),
            scope: Some(scope),
        }
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn scope(&self) -> Option<&BsonDocument> {
        self.scope.as_ref()
    }

    pub fn into_parts(self) -> (String, Option<BsonDocument>) {
        (self.code, self.scope)
    }

    pub fn representation_eq(&self, other: &Self) -> bool {
        self.code == other.code
            && match (&self.scope, &other.scope) {
                (Some(left), Some(right)) => left.representation_eq(right),
                (None, None) => true,
                _ => false,
            }
    }
}

/// Raw IEEE-754 Decimal128 BID bytes with semantic numeric comparison.
#[derive(Debug, Clone, Copy)]
pub struct BsonDecimal128([u8; 16]);

impl BsonDecimal128 {
    pub const fn from_bid(bid: [u8; 16]) -> Self {
        Self(bid)
    }

    pub fn parse(value: &str) -> BsonResult<Self> {
        value
            .parse::<bson::Decimal128>()
            .map(|value| Self(value.bytes()))
            .map_err(|_| invalid_value("invalid BSON Decimal128 value"))
    }

    pub const fn bid(self) -> [u8; 16] {
        self.0
    }

    pub fn representation_eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    pub(crate) fn canonical_number(self) -> CanonicalNumber {
        CanonicalNumber::from_decimal_bid(self.0)
    }
}

impl fmt::Display for BsonDecimal128 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        bson::Decimal128::from_bytes(self.0).fmt(formatter)
    }
}

impl FromStr for BsonDecimal128 {
    type Err = BsonError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl PartialEq for BsonDecimal128 {
    fn eq(&self, other: &Self) -> bool {
        self.canonical_number() == other.canonical_number()
    }
}

impl Eq for BsonDecimal128 {}

impl Hash for BsonDecimal128 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.canonical_number().hash(state);
    }
}

impl PartialOrd for BsonDecimal128 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BsonDecimal128 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.canonical_number().cmp(&other.canonical_number())
    }
}

/// A driver's BSON encoding convention for a UUID.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UuidRepresentation {
    Standard,
    PythonLegacy,
    JavaLegacy,
    CSharpLegacy,
}

/// Logical RFC-4122 bytes together with the BSON representation that encoded them.
#[derive(Debug, Clone, Copy)]
pub struct BsonUuid {
    bytes: [u8; 16],
    representation: UuidRepresentation,
}

impl BsonUuid {
    pub const fn new(bytes: [u8; 16], representation: UuidRepresentation) -> Self {
        Self {
            bytes,
            representation,
        }
    }

    pub const fn bytes(self) -> [u8; 16] {
        self.bytes
    }

    pub const fn representation(self) -> UuidRepresentation {
        self.representation
    }

    pub fn to_binary(self) -> BsonBinary {
        let (subtype, bytes) = self.encoded_parts();
        BsonBinary::new(subtype, bytes)
    }

    pub fn from_binary(
        binary: &BsonBinary,
        representation: UuidRepresentation,
    ) -> BsonResult<Self> {
        let expected_subtype = representation.subtype();
        if binary.subtype != expected_subtype || binary.bytes.len() != 16 {
            return Err(invalid_value(
                "BSON binary value does not match the requested UUID representation",
            ));
        }
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&binary.bytes);
        representation.transform(&mut bytes);
        Ok(Self::new(bytes, representation))
    }

    pub fn representation_eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes && self.representation == other.representation
    }

    pub(crate) fn encoded_parts(self) -> (u8, [u8; 16]) {
        let mut bytes = self.bytes;
        self.representation.transform(&mut bytes);
        (self.representation.subtype(), bytes)
    }
}

impl UuidRepresentation {
    const fn subtype(self) -> u8 {
        match self {
            Self::Standard => 4,
            Self::PythonLegacy | Self::JavaLegacy | Self::CSharpLegacy => 3,
        }
    }

    fn transform(self, bytes: &mut [u8; 16]) {
        match self {
            Self::Standard | Self::PythonLegacy => {}
            Self::JavaLegacy => {
                bytes[..8].reverse();
                bytes[8..].reverse();
            }
            Self::CSharpLegacy => {
                bytes[..4].reverse();
                bytes[4..6].reverse();
                bytes[6..8].reverse();
            }
        }
    }
}

impl PartialEq for BsonUuid {
    fn eq(&self, other: &Self) -> bool {
        self.encoded_parts() == other.encoded_parts()
    }
}

impl Eq for BsonUuid {}

impl Hash for BsonUuid {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.encoded_parts().hash(state);
    }
}

impl PartialOrd for BsonUuid {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BsonUuid {
    fn cmp(&self, other: &Self) -> Ordering {
        let (left_subtype, left_bytes) = self.encoded_parts();
        let (right_subtype, right_bytes) = other.encoded_parts();
        left_subtype
            .cmp(&right_subtype)
            .then_with(|| left_bytes.cmp(&right_bytes))
    }
}

/// An owned value for every BSON family in the frozen TinyMongo contract.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum BsonValue {
    Double(f64),
    String(String),
    Document(BsonDocument),
    Array(Vec<BsonValue>),
    Binary(BsonBinary),
    Uuid(BsonUuid),
    ObjectId(BsonObjectId),
    Boolean(bool),
    DateTime(BsonDateTime),
    Null,
    RegularExpression(BsonRegex),
    JavaScript(BsonJavaScript),
    Int32(i32),
    Timestamp(BsonTimestamp),
    Int64(i64),
    Decimal128(BsonDecimal128),
    MinKey,
    MaxKey,
}

impl BsonValue {
    pub fn representation_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Double(left), Self::Double(right)) => left.to_bits() == right.to_bits(),
            (Self::String(left), Self::String(right)) => left == right,
            (Self::Document(left), Self::Document(right)) => left.representation_eq(right),
            (Self::Array(left), Self::Array(right)) => {
                left.len() == right.len()
                    && left
                        .iter()
                        .zip(right)
                        .all(|(left, right)| left.representation_eq(right))
            }
            (Self::Binary(left), Self::Binary(right)) => left == right,
            (Self::Uuid(left), Self::Uuid(right)) => left.representation_eq(right),
            (Self::Null, Self::Null)
            | (Self::MinKey, Self::MinKey)
            | (Self::MaxKey, Self::MaxKey) => true,
            (Self::ObjectId(left), Self::ObjectId(right)) => left == right,
            (Self::Boolean(left), Self::Boolean(right)) => left == right,
            (Self::DateTime(left), Self::DateTime(right)) => left == right,
            (Self::RegularExpression(left), Self::RegularExpression(right)) => left == right,
            (Self::JavaScript(left), Self::JavaScript(right)) => left.representation_eq(right),
            (Self::Int32(left), Self::Int32(right)) => left == right,
            (Self::Timestamp(left), Self::Timestamp(right)) => left == right,
            (Self::Int64(left), Self::Int64(right)) => left == right,
            (Self::Decimal128(left), Self::Decimal128(right)) => left.representation_eq(right),
            _ => false,
        }
    }

    pub(crate) fn type_rank(&self) -> u8 {
        match self {
            Self::MinKey => 0,
            Self::Null => 1,
            Self::Double(_) | Self::Int32(_) | Self::Int64(_) | Self::Decimal128(_) => 2,
            Self::String(_) => 3,
            Self::Document(_) => 4,
            Self::Array(_) => 5,
            Self::Binary(_) | Self::Uuid(_) => 6,
            Self::ObjectId(_) => 7,
            Self::Boolean(_) => 8,
            Self::DateTime(_) => 9,
            Self::Timestamp(_) => 10,
            Self::RegularExpression(_) => 11,
            Self::JavaScript(value) if value.scope.is_none() => 12,
            Self::JavaScript(_) => 13,
            Self::MaxKey => 14,
        }
    }

    pub(crate) fn canonical_number(&self) -> Option<CanonicalNumber> {
        match self {
            Self::Double(value) => Some(CanonicalNumber::from_f64(*value)),
            Self::Int32(value) => Some(CanonicalNumber::from_i32(*value)),
            Self::Int64(value) => Some(CanonicalNumber::from_i64(*value)),
            Self::Decimal128(value) => Some(value.canonical_number()),
            _ => None,
        }
    }

    pub(crate) fn with_binary_parts<R>(&self, f: impl FnOnce(u8, &[u8]) -> R) -> Option<R> {
        match self {
            Self::Binary(value) => Some(f(value.subtype, &value.bytes)),
            Self::Uuid(value) => {
                let (subtype, bytes) = value.encoded_parts();
                Some(f(subtype, &bytes))
            }
            _ => None,
        }
    }
}

impl PartialEq for BsonValue {
    fn eq(&self, other: &Self) -> bool {
        if let (Some(left), Some(right)) = (self.canonical_number(), other.canonical_number()) {
            return left == right;
        }
        if let Some(equal) = self.with_binary_parts(|left_subtype, left_bytes| {
            other
                .with_binary_parts(|right_subtype, right_bytes| {
                    left_subtype == right_subtype && left_bytes == right_bytes
                })
                .unwrap_or(false)
        }) {
            return equal;
        }

        match (self, other) {
            (Self::String(left), Self::String(right)) => left == right,
            (Self::Document(left), Self::Document(right)) => left == right,
            (Self::Array(left), Self::Array(right)) => left == right,
            (Self::Null, Self::Null)
            | (Self::MinKey, Self::MinKey)
            | (Self::MaxKey, Self::MaxKey) => true,
            (Self::ObjectId(left), Self::ObjectId(right)) => left == right,
            (Self::Boolean(left), Self::Boolean(right)) => left == right,
            (Self::DateTime(left), Self::DateTime(right)) => left == right,
            (Self::RegularExpression(left), Self::RegularExpression(right)) => left == right,
            (Self::JavaScript(left), Self::JavaScript(right)) => left == right,
            (Self::Timestamp(left), Self::Timestamp(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for BsonValue {}

impl Hash for BsonValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let Some(number) = self.canonical_number() {
            2_u8.hash(state);
            number.hash(state);
            return;
        }
        if self
            .with_binary_parts(|subtype, bytes| {
                6_u8.hash(state);
                subtype.hash(state);
                bytes.hash(state);
            })
            .is_some()
        {
            return;
        }

        self.type_rank().hash(state);
        match self {
            Self::String(value) => value.hash(state),
            Self::Document(value) => value.hash(state),
            Self::Array(value) => value.hash(state),
            Self::ObjectId(value) => value.hash(state),
            Self::Boolean(value) => value.hash(state),
            Self::DateTime(value) => value.hash(state),
            Self::RegularExpression(value) => value.hash(state),
            Self::JavaScript(value) => value.hash(state),
            Self::Timestamp(value) => value.hash(state),
            Self::Double(_)
            | Self::Binary(_)
            | Self::Uuid(_)
            | Self::Int32(_)
            | Self::Int64(_)
            | Self::Decimal128(_) => unreachable!("handled by a semantic family above"),
            Self::Null | Self::MinKey | Self::MaxKey => {}
        }
    }
}

impl PartialOrd for BsonValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BsonValue {
    fn cmp(&self, other: &Self) -> Ordering {
        let rank = self.type_rank().cmp(&other.type_rank());
        if rank != Ordering::Equal {
            return rank;
        }
        if let (Some(left), Some(right)) = (self.canonical_number(), other.canonical_number()) {
            return left.cmp(&right);
        }
        if let Some(ordering) = self.with_binary_parts(|left_subtype, left_bytes| {
            other.with_binary_parts(|right_subtype, right_bytes| {
                let left_len = left_bytes
                    .len()
                    .saturating_add(usize::from(left_subtype == 2) * 4);
                let right_len = right_bytes
                    .len()
                    .saturating_add(usize::from(right_subtype == 2) * 4);
                left_len
                    .cmp(&right_len)
                    .then_with(|| left_subtype.cmp(&right_subtype))
                    .then_with(|| left_bytes.cmp(right_bytes))
            })
        }) {
            return ordering.expect("equal binary-family ranks have binary payloads");
        }

        match (self, other) {
            (Self::String(left), Self::String(right)) => left.cmp(right),
            (Self::Document(left), Self::Document(right)) => left.cmp(right),
            (Self::Array(left), Self::Array(right)) => left.cmp(right),
            (Self::ObjectId(left), Self::ObjectId(right)) => left.cmp(right),
            (Self::Boolean(left), Self::Boolean(right)) => left.cmp(right),
            (Self::DateTime(left), Self::DateTime(right)) => left.cmp(right),
            (Self::RegularExpression(left), Self::RegularExpression(right)) => left.cmp(right),
            (Self::JavaScript(left), Self::JavaScript(right)) => left.cmp(right),
            (Self::Timestamp(left), Self::Timestamp(right)) => left.cmp(right),
            (Self::Null, Self::Null)
            | (Self::MinKey, Self::MinKey)
            | (Self::MaxKey, Self::MaxKey) => Ordering::Equal,
            _ => unreachable!("equal BSON type ranks have compatible payloads"),
        }
    }
}

impl From<bool> for BsonValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<i32> for BsonValue {
    fn from(value: i32) -> Self {
        Self::Int32(value)
    }
}

impl From<i64> for BsonValue {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<f64> for BsonValue {
    fn from(value: f64) -> Self {
        Self::Double(value)
    }
}

impl From<String> for BsonValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for BsonValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<BsonDocument> for BsonValue {
    fn from(value: BsonDocument) -> Self {
        Self::Document(value)
    }
}

impl From<Vec<BsonValue>> for BsonValue {
    fn from(value: Vec<BsonValue>) -> Self {
        Self::Array(value)
    }
}

impl From<BsonBinary> for BsonValue {
    fn from(value: BsonBinary) -> Self {
        Self::Binary(value)
    }
}

impl From<BsonUuid> for BsonValue {
    fn from(value: BsonUuid) -> Self {
        Self::Uuid(value)
    }
}

impl From<BsonObjectId> for BsonValue {
    fn from(value: BsonObjectId) -> Self {
        Self::ObjectId(value)
    }
}

impl From<BsonDateTime> for BsonValue {
    fn from(value: BsonDateTime) -> Self {
        Self::DateTime(value)
    }
}

impl From<BsonTimestamp> for BsonValue {
    fn from(value: BsonTimestamp) -> Self {
        Self::Timestamp(value)
    }
}

impl From<BsonRegex> for BsonValue {
    fn from(value: BsonRegex) -> Self {
        Self::RegularExpression(value)
    }
}

impl From<BsonJavaScript> for BsonValue {
    fn from(value: BsonJavaScript) -> Self {
        Self::JavaScript(value)
    }
}

impl From<BsonDecimal128> for BsonValue {
    fn from(value: BsonDecimal128) -> Self {
        Self::Decimal128(value)
    }
}

fn invalid_value(diagnostic: &'static str) -> BsonError {
    BsonError::new(BsonErrorKind::InvalidValue, diagnostic)
}

fn duplicate_field() -> BsonError {
    BsonError::new(
        BsonErrorKind::DuplicateField,
        "BSON document contains a duplicate field name",
    )
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_microseconds_floor_to_bson_milliseconds() {
        assert_eq!(BsonDateTime::from_micros(1_999).timestamp_millis(), 1);
        assert_eq!(BsonDateTime::from_micros(999).timestamp_millis(), 0);
        assert_eq!(BsonDateTime::from_micros(-1).timestamp_millis(), -1);
        assert_eq!(BsonDateTime::from_micros(-1_001).timestamp_millis(), -2);
    }
}
