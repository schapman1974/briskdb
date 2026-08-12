//! Opaque physical-row locators used by the writable virtual-table facade.
//!
//! Locators are never interpreted as SQL values. They identify the registered
//! table, the physical shard, and the row identity values selected from that
//! shard. The representation is deliberately versioned and length-delimited so
//! future versions can be rejected safely instead of being misinterpreted.

use super::RawCell;
use crate::core::{EngineError, EngineErrorKind, EngineResult};

const VERSION: u8 = 1;
const HEADER_BYTES: usize = 1 + size_of::<u64>() + size_of::<u16>() + size_of::<u16>();
const VALUE_HEADER_BYTES: usize = 1 + size_of::<u32>();

const TAG_NULL: u8 = 0;
const TAG_INTEGER: u8 = 1;
const TAG_REAL: u8 = 2;
const TAG_TEXT: u8 = 3;
const TAG_BLOB: u8 = 4;

/// Hard bound for both encoded and accepted locator values.
pub(super) const MAX_LOCATOR_BYTES: usize = 1024 * 1024;

#[derive(Debug)]
pub(super) struct DecodedLocator {
    pub(super) shard: u16,
    pub(super) values: Vec<RawCell>,
}

/// Encode a physical row identity into a bounded, table-specific locator.
pub(super) fn encode(table_id: u64, shard: u16, values: &[RawCell]) -> EngineResult<Vec<u8>> {
    let value_count = u16::try_from(values.len()).map_err(|_| {
        EngineError::new(
            EngineErrorKind::LimitExceeded,
            "brisk_shard locator contains more than 65535 identity values",
        )
    })?;

    let mut encoded_len = HEADER_BYTES;
    for value in values {
        let payload_len = payload_len(value);
        u32::try_from(payload_len).map_err(|_| locator_size_error())?;
        encoded_len = encoded_len
            .checked_add(VALUE_HEADER_BYTES)
            .and_then(|len| len.checked_add(payload_len))
            .ok_or_else(locator_size_error)?;
        if encoded_len > MAX_LOCATOR_BYTES {
            return Err(locator_size_error());
        }
    }

    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(encoded_len)
        .map_err(allocation_error)?;
    encoded.push(VERSION);
    encoded.extend_from_slice(&table_id.to_be_bytes());
    encoded.extend_from_slice(&shard.to_be_bytes());
    encoded.extend_from_slice(&value_count.to_be_bytes());

    for value in values {
        let (tag, payload_len) = match value {
            RawCell::Null => (TAG_NULL, 0),
            RawCell::Integer(_) => (TAG_INTEGER, size_of::<i64>()),
            RawCell::Real(_) => (TAG_REAL, size_of::<u64>()),
            RawCell::Text(value) => (TAG_TEXT, value.len()),
            RawCell::Blob(value) => (TAG_BLOB, value.len()),
        };
        let payload_len = u32::try_from(payload_len).map_err(|_| locator_size_error())?;
        encoded.push(tag);
        encoded.extend_from_slice(&payload_len.to_be_bytes());
        match value {
            RawCell::Null => {}
            RawCell::Integer(value) => encoded.extend_from_slice(&value.to_be_bytes()),
            RawCell::Real(value) => encoded.extend_from_slice(&value.to_bits().to_be_bytes()),
            RawCell::Text(value) | RawCell::Blob(value) => encoded.extend_from_slice(value),
        }
    }

    debug_assert_eq!(encoded.len(), encoded_len);
    Ok(encoded)
}

/// Decode and validate a locator for `expected_table_id`.
pub(super) fn decode(expected_table_id: u64, bytes: &[u8]) -> EngineResult<DecodedLocator> {
    if bytes.len() > MAX_LOCATOR_BYTES {
        return Err(locator_size_error());
    }

    let mut input = Input::new(bytes);
    let version = input.byte()?;
    if version != VERSION {
        return Err(locator_corruption(
            "brisk_shard locator has an unsupported format version",
        ));
    }

    let table_id = u64::from_be_bytes(input.array()?);
    if table_id != expected_table_id {
        return Err(locator_corruption(
            "brisk_shard locator belongs to a different registered table",
        ));
    }
    let shard = u16::from_be_bytes(input.array()?);
    let value_count = usize::from(u16::from_be_bytes(input.array()?));
    let minimum_value_bytes = value_count
        .checked_mul(VALUE_HEADER_BYTES)
        .ok_or_else(|| locator_corruption("brisk_shard locator value count overflowed"))?;
    if input.remaining() < minimum_value_bytes {
        return Err(locator_corruption("brisk_shard locator is truncated"));
    }

    let mut values = Vec::new();
    values
        .try_reserve_exact(value_count)
        .map_err(allocation_error)?;
    for _ in 0..value_count {
        let tag = input.byte()?;
        let payload_len_u32 = u32::from_be_bytes(input.array()?);
        let payload_len = usize::try_from(payload_len_u32).map_err(|_| locator_size_error())?;
        let payload = input.take(payload_len)?;

        let value = match tag {
            TAG_NULL => {
                require_payload_len(payload, 0, "NULL")?;
                RawCell::Null
            }
            TAG_INTEGER => {
                require_payload_len(payload, size_of::<i64>(), "integer")?;
                RawCell::Integer(i64::from_be_bytes(payload.try_into().map_err(|_| {
                    locator_corruption("brisk_shard locator has an invalid integer payload")
                })?))
            }
            TAG_REAL => {
                require_payload_len(payload, size_of::<u64>(), "real")?;
                let bits = u64::from_be_bytes(payload.try_into().map_err(|_| {
                    locator_corruption("brisk_shard locator has an invalid real payload")
                })?);
                RawCell::Real(f64::from_bits(bits))
            }
            TAG_TEXT => RawCell::Text(copy_payload(payload)?),
            TAG_BLOB => RawCell::Blob(copy_payload(payload)?),
            _ => {
                return Err(locator_corruption(
                    "brisk_shard locator has an unknown storage-class tag",
                ));
            }
        };
        values.push(value);
    }

    if !input.is_empty() {
        return Err(locator_corruption("brisk_shard locator has trailing bytes"));
    }

    Ok(DecodedLocator { shard, values })
}

fn payload_len(value: &RawCell) -> usize {
    match value {
        RawCell::Null => 0,
        RawCell::Integer(_) | RawCell::Real(_) => size_of::<u64>(),
        RawCell::Text(value) | RawCell::Blob(value) => value.len(),
    }
}

fn require_payload_len(payload: &[u8], expected: usize, storage_class: &str) -> EngineResult<()> {
    if payload.len() == expected {
        Ok(())
    } else {
        Err(locator_corruption(format!(
            "brisk_shard locator has a non-canonical {storage_class} payload length"
        )))
    }
}

fn copy_payload(payload: &[u8]) -> EngineResult<Vec<u8>> {
    let mut copy = Vec::new();
    copy.try_reserve_exact(payload.len())
        .map_err(allocation_error)?;
    copy.extend_from_slice(payload);
    Ok(copy)
}

fn locator_size_error() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "brisk_shard locator exceeds its 1 MiB encoded-size limit",
    )
}

fn locator_corruption(diagnostic: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::DataCorruption, diagnostic)
}

fn allocation_error(error: std::collections::TryReserveError) -> EngineError {
    EngineError::from_source(
        EngineErrorKind::OutOfMemory,
        "brisk_shard could not reserve bounded locator memory",
        error,
    )
}

struct Input<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Input<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn byte(&mut self) -> EngineResult<u8> {
        Ok(self.take(1)?[0])
    }

    fn array<const N: usize>(&mut self) -> EngineResult<[u8; N]> {
        let mut result = [0; N];
        result.copy_from_slice(self.take(N)?);
        Ok(result)
    }

    fn take(&mut self, len: usize) -> EngineResult<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| locator_corruption("brisk_shard locator length overflowed"))?;
        if end > self.bytes.len() {
            return Err(locator_corruption("brisk_shard locator is truncated"));
        }
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_every_storage_class_losslessly() {
        let nan_bits = 0x7ff8_0000_0000_0042;
        let values = vec![
            RawCell::Null,
            RawCell::Integer(i64::MIN),
            RawCell::Integer(i64::MAX),
            RawCell::Real(-0.0),
            RawCell::Real(f64::from_bits(nan_bits)),
            RawCell::Text(vec![0xff, 0x00, b'a']),
            RawCell::Blob(vec![0x00, 0x80, 0xff]),
        ];

        let encoded = encode(0x1020_3040_5060_7080, u16::MAX, &values).unwrap();
        let decoded = decode(0x1020_3040_5060_7080, &encoded).unwrap();

        assert_eq!(decoded.shard, u16::MAX);
        assert_eq!(decoded.values.len(), values.len());
        assert!(matches!(decoded.values[0], RawCell::Null));
        assert!(matches!(decoded.values[1], RawCell::Integer(i64::MIN)));
        assert!(matches!(decoded.values[2], RawCell::Integer(i64::MAX)));
        assert_real_bits(&decoded.values[3], (-0.0_f64).to_bits());
        assert_real_bits(&decoded.values[4], nan_bits);
        assert_bytes(&decoded.values[5], true, &[0xff, 0x00, b'a']);
        assert_bytes(&decoded.values[6], false, &[0x00, 0x80, 0xff]);
    }

    #[test]
    fn empty_identity_is_valid_and_table_specific() {
        let encoded = encode(7, 0, &[]).unwrap();
        assert_eq!(encoded.len(), HEADER_BYTES);

        let decoded = decode(7, &encoded).unwrap();
        assert_eq!(decoded.shard, 0);
        assert!(decoded.values.is_empty());

        let error = decode(8, &encoded).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
    }

    #[test]
    fn rejects_every_truncation_boundary_without_panicking() {
        let encoded = encode(
            9,
            3,
            &[
                RawCell::Integer(-27),
                RawCell::Text(b"identity".to_vec()),
                RawCell::Blob(vec![1, 2, 3]),
            ],
        )
        .unwrap();

        for end in 0..encoded.len() {
            let error = decode(9, &encoded[..end]).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption, "end={end}");
        }
        assert!(decode(9, &encoded).is_ok());
    }

    #[test]
    fn rejects_wrong_version_unknown_tag_and_trailing_bytes() {
        let mut wrong_version = encode(1, 2, &[]).unwrap();
        wrong_version[0] = VERSION.wrapping_add(1);
        assert_corrupt(decode(1, &wrong_version));

        let mut unknown_tag = encode(1, 2, &[RawCell::Null]).unwrap();
        unknown_tag[HEADER_BYTES] = 0xfe;
        assert_corrupt(decode(1, &unknown_tag));

        let mut trailing = encode(1, 2, &[]).unwrap();
        trailing.push(0);
        assert_corrupt(decode(1, &trailing));
    }

    #[test]
    fn rejects_noncanonical_fixed_width_payload_lengths() {
        for value in [RawCell::Null, RawCell::Integer(1), RawCell::Real(1.0)] {
            let mut encoded = encode(4, 5, &[value]).unwrap();
            let original_len = u32::from_be_bytes(
                encoded[HEADER_BYTES + 1..HEADER_BYTES + VALUE_HEADER_BYTES]
                    .try_into()
                    .unwrap(),
            );
            let invalid_len = if original_len == 0 {
                1_u32
            } else {
                original_len - 1
            };
            encoded[HEADER_BYTES + 1..HEADER_BYTES + VALUE_HEADER_BYTES]
                .copy_from_slice(&invalid_len.to_be_bytes());
            if original_len == 0 {
                encoded.push(0);
            } else {
                encoded.pop();
            }
            assert_corrupt(decode(4, &encoded));
        }
    }

    #[test]
    fn rejects_declared_payload_larger_than_remaining_input() {
        let mut encoded = encode(12, 6, &[RawCell::Blob(vec![1, 2, 3])]).unwrap();
        encoded[HEADER_BYTES + 1..HEADER_BYTES + VALUE_HEADER_BYTES]
            .copy_from_slice(&100_u32.to_be_bytes());
        assert_corrupt(decode(12, &encoded));
    }

    #[test]
    fn enforces_exact_one_mibibyte_boundary() {
        let payload_len = MAX_LOCATOR_BYTES - HEADER_BYTES - VALUE_HEADER_BYTES;
        let payload = vec![0xa5; payload_len];
        let encoded = encode(15, 8, &[RawCell::Blob(payload.clone())]).unwrap();
        assert_eq!(encoded.len(), MAX_LOCATOR_BYTES);
        let decoded = decode(15, &encoded).unwrap();
        assert_bytes(&decoded.values[0], false, &payload);

        let error = encode(15, 8, &[RawCell::Blob(vec![0; payload_len + 1])]).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);

        let oversized = vec![0; MAX_LOCATOR_BYTES + 1];
        let error = decode(15, &oversized).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    }

    #[test]
    fn supports_maximum_value_count_and_rejects_count_overflow() {
        let max_values = (0..usize::from(u16::MAX))
            .map(|_| RawCell::Null)
            .collect::<Vec<_>>();
        let encoded = encode(22, 11, &max_values).unwrap();
        let decoded = decode(22, &encoded).unwrap();
        assert_eq!(decoded.values.len(), usize::from(u16::MAX));

        let too_many = (0..=usize::from(u16::MAX))
            .map(|_| RawCell::Null)
            .collect::<Vec<_>>();
        let error = encode(22, 11, &too_many).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    }

    fn assert_corrupt(result: EngineResult<DecodedLocator>) {
        let error = result.unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
    }

    fn assert_real_bits(value: &RawCell, expected: u64) {
        match value {
            RawCell::Real(value) => assert_eq!(value.to_bits(), expected),
            value => panic!("expected real, got {value:?}"),
        }
    }

    fn assert_bytes(value: &RawCell, text: bool, expected: &[u8]) {
        match (value, text) {
            (RawCell::Text(value), true) | (RawCell::Blob(value), false) => {
                assert_eq!(value, expected)
            }
            (value, _) => panic!("unexpected storage class: {value:?}"),
        }
    }
}
