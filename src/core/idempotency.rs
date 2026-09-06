//! Protocol-neutral identities and outcomes for durable idempotent writes.

use std::{
    collections::HashSet,
    error::Error,
    fmt,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use super::{
    EngineError, EngineErrorKind, EngineResult, GeneratedKey, LogicalDatabaseId, Routed, TableId,
    Value, WriteResult,
};

/// Version of the semantic request fingerprint stored in durable receipts.
pub const IDEMPOTENCY_FINGERPRINT_VERSION: u32 = 1;

/// Server-wall-clock retention interval stored for a successful write receipt.
pub const IDEMPOTENCY_RECEIPT_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

/// Maximum number of unexpired idempotency receipts retained on one shard.
pub const MAX_IDEMPOTENCY_RECEIPTS_PER_SHARD: usize = 4_096;

/// Number of fixed database-wide lock stripes used to serialize keys.
pub const IDEMPOTENCY_LOCK_STRIPES: usize = 256;

pub(crate) const IDEMPOTENCY_RECEIPT_RETENTION_MS: i64 = 24 * 60 * 60 * 1_000;

const KEY_DIGEST_CONTEXT: &str = "briskdb durable idempotency key digest v1";
const REQUEST_DIGEST_CONTEXT: &str = "briskdb durable idempotent write semantics v1";
const EXECUTE_WRITE_SCOPE: &[u8] = b"core.Engine.execute_idempotent_write.v1";

/// A caller-supplied, nonzero 128-bit identity for one durable write intent.
///
/// The canonical text representation is exactly 32 lowercase hexadecimal
/// characters. Debug output is redacted so routine structural logging does not
/// persist the caller's raw key; [`fmt::Display`] is the explicit conversion
/// for protocol adapters that need its canonical representation.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IdempotencyKey([u8; 16]);

impl IdempotencyKey {
    /// Construct a key from its opaque bytes.
    pub fn new(bytes: [u8; 16]) -> Result<Self, ParseIdempotencyKeyError> {
        if bytes == [0; 16] {
            Err(ParseIdempotencyKeyError)
        } else {
            Ok(Self(bytes))
        }
    }

    /// Return the opaque identity bytes.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl TryFrom<[u8; 16]> for IdempotencyKey {
    type Error = ParseIdempotencyKeyError;

    fn try_from(bytes: [u8; 16]) -> Result<Self, Self::Error> {
        Self::new(bytes)
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for IdempotencyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("IdempotencyKey")
            .field(&"[REDACTED]")
            .finish()
    }
}

/// Failure to parse a canonical durable idempotency key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseIdempotencyKeyError;

impl fmt::Display for ParseIdempotencyKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .write_str("idempotency keys must be 32 lowercase hexadecimal characters and nonzero")
    }
}

impl Error for ParseIdempotencyKeyError {}

impl FromStr for IdempotencyKey {
    type Err = ParseIdempotencyKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 32
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ParseIdempotencyKeyError);
        }
        let mut bytes = [0_u8; 16];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            bytes[index] = (decode_hex(pair[0]) << 4) | decode_hex(pair[1]);
        }
        Self::new(bytes)
    }
}

const fn decode_hex(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => 0,
    }
}

/// Whether an idempotent write was committed now or replayed from its receipt.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IdempotencyStatus {
    /// This invocation committed the application write and durable receipt.
    Created,
    /// A prior matching invocation had already committed successfully.
    Replayed,
}

impl IdempotencyStatus {
    /// Return the stable lowercase value used by protocol response metadata.
    pub const fn code(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Replayed => "replayed",
        }
    }

    /// Return the stable lowercase value used by protocol response metadata.
    pub const fn as_str(self) -> &'static str {
        self.code()
    }
}

impl fmt::Display for IdempotencyStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

/// Routed logical result of one durable idempotent write.
#[derive(Debug, Clone, PartialEq)]
pub struct IdempotentWriteResult {
    routed: Routed<WriteResult>,
    status: IdempotencyStatus,
}

impl IdempotentWriteResult {
    pub(crate) const fn new(shard: u16, value: WriteResult, status: IdempotencyStatus) -> Self {
        Self {
            routed: Routed { shard, value },
            status,
        }
    }

    /// Return whether this call created or replayed the durable receipt.
    pub const fn status(&self) -> IdempotencyStatus {
        self.status
    }

    /// Return the physical shard that owns both the application write and receipt.
    pub const fn shard(&self) -> u16 {
        self.routed.shard
    }

    /// Return the logical write result stored in or reconstructed from the receipt.
    pub const fn write_result(&self) -> &WriteResult {
        &self.routed.value
    }

    /// Return the number of rows affected by the original committed write.
    pub const fn rows_affected(&self) -> usize {
        self.routed.value.rows_affected
    }

    /// Return the generated key captured by the original committed write.
    ///
    /// The current idempotent-write eligibility contract rejects generated
    /// targets, so Engine-produced results currently return `None` here. The
    /// accessor preserves the ordinary [`WriteResult`] shape for future formats.
    pub fn generated_key(&self) -> Option<&GeneratedKey> {
        self.routed.value.generated_key.as_ref()
    }

    /// Consume the result into its routed write outcome and replay status.
    pub fn into_parts(self) -> (Routed<WriteResult>, IdempotencyStatus) {
        (self.routed, self.status)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IdempotencyDigests {
    pub(crate) key: [u8; 32],
    pub(crate) request: [u8; 32],
}

/// Exact in-process key admission used before requests wait for shard capacity.
#[derive(Debug, Default)]
pub(crate) struct ActiveIdempotencyKeys {
    keys: Mutex<HashSet<[u8; 32]>>,
}

impl ActiveIdempotencyKeys {
    pub(crate) fn try_acquire(
        self: &Arc<Self>,
        key_digest: [u8; 32],
    ) -> EngineResult<ActiveIdempotencyKeyGuard> {
        let mut keys = self
            .keys
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !keys.insert(key_digest) {
            return Err(EngineError::new(
                EngineErrorKind::Busy,
                "another request is handling this idempotency key",
            ));
        }
        Ok(ActiveIdempotencyKeyGuard {
            key_digest,
            keys: Arc::clone(self),
        })
    }
}

#[derive(Debug)]
pub(crate) struct ActiveIdempotencyKeyGuard {
    key_digest: [u8; 32],
    keys: Arc<ActiveIdempotencyKeys>,
}

impl Drop for ActiveIdempotencyKeyGuard {
    fn drop(&mut self) {
        self.keys
            .keys
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.key_digest);
    }
}

pub(crate) fn write_digests(
    key: IdempotencyKey,
    exact_sql: &str,
    params: &[Value],
    routing_key: Option<&str>,
    logical_database: LogicalDatabaseId,
    table: TableId,
    target_shard: u16,
) -> IdempotencyDigests {
    let key = *blake3::Hasher::new_derive_key(KEY_DIGEST_CONTEXT)
        .update(key.as_bytes())
        .finalize()
        .as_bytes();

    let mut request = blake3::Hasher::new_derive_key(REQUEST_DIGEST_CONTEXT);
    hash_bytes(&mut request, EXECUTE_WRITE_SCOPE);
    request.update(&IDEMPOTENCY_FINGERPRINT_VERSION.to_le_bytes());
    hash_bytes(&mut request, exact_sql.as_bytes());
    request.update(&(params.len() as u64).to_le_bytes());
    for parameter in params {
        hash_value(&mut request, parameter);
    }
    match routing_key {
        Some(routing_key) => {
            request.update(&[1]);
            hash_bytes(&mut request, routing_key.as_bytes());
        }
        None => {
            request.update(&[0]);
        }
    }
    request.update(&logical_database.get().to_le_bytes());
    request.update(&table.get().to_le_bytes());
    request.update(&target_shard.to_le_bytes());

    IdempotencyDigests {
        key,
        request: *request.finalize().as_bytes(),
    }
}

fn hash_value(hasher: &mut blake3::Hasher, value: &Value) {
    match value {
        Value::Null => {
            hasher.update(&[0]);
        }
        Value::Boolean(value) => {
            hasher.update(&[1, u8::from(*value)]);
        }
        Value::Int64(value) => {
            hasher.update(&[2]);
            hasher.update(&value.to_le_bytes());
        }
        Value::UInt64(value) => {
            hasher.update(&[3]);
            hasher.update(&value.to_le_bytes());
        }
        Value::Float64(value) => {
            hasher.update(&[4]);
            hasher.update(&value.to_bits().to_le_bytes());
        }
        Value::Decimal(value) => {
            hasher.update(&[5]);
            hash_bytes(hasher, value.as_str().as_bytes());
        }
        Value::Text(value) => {
            hasher.update(&[6]);
            hash_bytes(hasher, value.as_bytes());
        }
        Value::InvalidText(value) => {
            hasher.update(&[7]);
            hash_bytes(hasher, value);
        }
        Value::Binary(value) => {
            hasher.update(&[8]);
            hash_bytes(hasher, value);
        }
    }
}

fn hash_bytes(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Decimal;

    const KEY_TEXT: &str = "000102030405060708090a0b0c0d0e0f";

    #[test]
    fn keys_have_one_strict_nonzero_text_form_and_redacted_debug() {
        let key = KEY_TEXT.parse::<IdempotencyKey>().unwrap();
        assert_eq!(key.to_string(), KEY_TEXT);
        assert_eq!(
            key.as_bytes(),
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );
        assert_eq!(IdempotencyKey::try_from(*key.as_bytes()).unwrap(), key);
        assert!(!format!("{key:?}").contains(KEY_TEXT));

        for invalid in [
            "",
            "000102030405060708090a0b0c0d0e0",
            "000102030405060708090a0b0c0d0e0ff",
            "000102030405060708090A0B0C0D0E0F",
            "000102030405060708090a0b0c0d0e0g",
            "00000000000000000000000000000000",
        ] {
            assert!(invalid.parse::<IdempotencyKey>().is_err(), "{invalid}");
        }
        assert!(IdempotencyKey::new([0; 16]).is_err());
    }

    #[test]
    fn status_and_result_accessors_preserve_the_routed_outcome() {
        let result = IdempotentWriteResult::new(
            3,
            WriteResult::without_generated_key(2),
            IdempotencyStatus::Replayed,
        );
        assert_eq!(result.status(), IdempotencyStatus::Replayed);
        assert_eq!(result.status().code(), "replayed");
        assert_eq!(result.status().as_str(), "replayed");
        assert_eq!(result.shard(), 3);
        assert_eq!(result.rows_affected(), 2);
        assert_eq!(
            result.write_result(),
            &WriteResult::without_generated_key(2)
        );
        assert_eq!(result.generated_key(), None);
        assert_eq!(
            result.into_parts(),
            (
                Routed {
                    shard: 3,
                    value: WriteResult::without_generated_key(2),
                },
                IdempotencyStatus::Replayed,
            )
        );
    }

    #[test]
    fn semantic_digest_binds_typed_values_bits_and_execution_scope() {
        let key = KEY_TEXT.parse().unwrap();
        let database = LogicalDatabaseId::new(1).unwrap();
        let table = TableId::new(2).unwrap();
        let base = write_digests(
            key,
            "UPDATE events SET value = ?1 WHERE tenant_id = ?2",
            &[Value::Float64(0.0), Value::Text("tenant".to_owned())],
            Some("tenant"),
            database,
            table,
            3,
        );
        let repeat = write_digests(
            key,
            "UPDATE events SET value = ?1 WHERE tenant_id = ?2",
            &[Value::Float64(0.0), Value::Text("tenant".to_owned())],
            Some("tenant"),
            database,
            table,
            3,
        );
        let negative_zero = write_digests(
            key,
            "UPDATE events SET value = ?1 WHERE tenant_id = ?2",
            &[Value::Float64(-0.0), Value::Text("tenant".to_owned())],
            Some("tenant"),
            database,
            table,
            3,
        );
        assert_eq!(base, repeat);
        assert_eq!(base.key, negative_zero.key);
        assert_ne!(base.request, negative_zero.request);

        let different_key = write_digests(
            "102132435465768798a9bacbdcedfe0f".parse().unwrap(),
            "UPDATE events SET value = ?1 WHERE tenant_id = ?2",
            &[Value::Float64(0.0), Value::Text("tenant".to_owned())],
            Some("tenant"),
            database,
            table,
            3,
        );
        assert_ne!(base.key, different_key.key);
        assert_eq!(base.request, different_key.request);

        let changed_sql = write_digests(
            key,
            "UPDATE events SET value=?1 WHERE tenant_id=?2",
            &[Value::Float64(0.0), Value::Text("tenant".to_owned())],
            Some("tenant"),
            database,
            table,
            3,
        );
        assert_ne!(base.request, changed_sql.request);

        for changed_scope in [
            write_digests(
                key,
                "UPDATE events SET value = ?1 WHERE tenant_id = ?2",
                &[Value::Float64(0.0), Value::Text("tenant".to_owned())],
                None,
                database,
                table,
                3,
            ),
            write_digests(
                key,
                "UPDATE events SET value = ?1 WHERE tenant_id = ?2",
                &[Value::Float64(0.0), Value::Text("tenant".to_owned())],
                Some("tenant"),
                LogicalDatabaseId::new(2).unwrap(),
                table,
                3,
            ),
            write_digests(
                key,
                "UPDATE events SET value = ?1 WHERE tenant_id = ?2",
                &[Value::Float64(0.0), Value::Text("tenant".to_owned())],
                Some("tenant"),
                database,
                TableId::new(3).unwrap(),
                3,
            ),
            write_digests(
                key,
                "UPDATE events SET value = ?1 WHERE tenant_id = ?2",
                &[Value::Float64(0.0), Value::Text("tenant".to_owned())],
                Some("tenant"),
                database,
                table,
                4,
            ),
        ] {
            assert_ne!(base.request, changed_scope.request);
        }
    }

    #[test]
    fn fingerprint_v1_has_a_frozen_comprehensive_vector() {
        let digests = write_digests(
            KEY_TEXT.parse().unwrap(),
            "UPDATE events SET value = ?1 WHERE tenant_id = ?2",
            &[
                Value::Null,
                Value::Boolean(true),
                Value::Int64(i64::MIN),
                Value::UInt64(u64::MAX),
                Value::Float64(f64::from_bits(0x7ff8_0000_0000_0042)),
                Value::Decimal(Decimal::parse("-123.4500e+6").unwrap()),
                Value::Text("héllo\0雪".to_owned()),
                Value::InvalidText(vec![0xff, 0, 0x80]),
                Value::Binary(vec![0, 1, 0xfe, 0xff]),
            ],
            Some("tenant\0雪"),
            LogicalDatabaseId::new(0x0102_0304_0506_0708).unwrap(),
            TableId::new(0x1112_1314_1516_1718).unwrap(),
            63,
        );
        assert_eq!(
            digests.key,
            [
                0xfe, 0x46, 0x88, 0x01, 0xe9, 0x9b, 0xb6, 0x4c, 0x3b, 0x1f, 0x04, 0xfc, 0xed, 0xe9,
                0x6a, 0xee, 0x92, 0xb2, 0xac, 0xd4, 0xff, 0x12, 0x7d, 0x82, 0x6e, 0x8c, 0xd1, 0xd3,
                0xe1, 0xc0, 0x1e, 0x19,
            ]
        );
        assert_eq!(
            digests.request,
            [
                0x7e, 0x66, 0xac, 0x71, 0xd4, 0x83, 0x43, 0x3c, 0xb0, 0x18, 0x65, 0xb1, 0x00, 0xea,
                0x31, 0xad, 0x81, 0xa9, 0x7a, 0xb3, 0x0f, 0xcc, 0x92, 0x26, 0xa4, 0xb1, 0x0c, 0xb0,
                0x30, 0xff, 0x83, 0xba,
            ]
        );
    }

    #[test]
    fn public_limits_match_the_durable_contract() {
        assert_eq!(IDEMPOTENCY_FINGERPRINT_VERSION, 1);
        assert_eq!(IDEMPOTENCY_RECEIPT_RETENTION, Duration::from_secs(86_400));
        assert_eq!(IDEMPOTENCY_RECEIPT_RETENTION_MS, 86_400_000);
        assert_eq!(MAX_IDEMPOTENCY_RECEIPTS_PER_SHARD, 4_096);
        assert_eq!(IDEMPOTENCY_LOCK_STRIPES, 256);
    }

    #[test]
    fn active_key_admission_is_nonblocking_and_released_by_drop() {
        let active = Arc::new(ActiveIdempotencyKeys::default());
        let first = active.try_acquire([7; 32]).unwrap();
        let error = active.try_acquire([7; 32]).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Busy);
        let unrelated = active.try_acquire([8; 32]).unwrap();
        drop(first);
        active.try_acquire([7; 32]).unwrap();
        drop(unrelated);
    }
}
