//! A framed equality tuple, deliberately separate from BSON and SQL key frames.

use super::{
    Arc, BsonErrorContext, CanonicalBsonKey, Component, DocumentIndexKey, EngineError,
    EngineErrorKind, EngineResult, MAX_VALUE_BYTES, MAX_WORK_BYTES, allocation, limit,
};
use crate::document::{BsonError, BsonErrorKind};

/// Current durable document secondary-index equality-key encoding.
pub const DOCUMENT_INDEX_KEY_ENCODING_VERSION: u32 = 1;
/// Hard bound for one encoded tuple, including its framing.
pub const MAX_DOCUMENT_INDEX_KEY_BYTES: usize = MAX_WORK_BYTES;

const MAGIC: &[u8; 4] = b"BDIK";
const HEADER_BYTES: usize = 12;
const MAX_COMPONENTS: usize = 32;
const EMPTY_ARRAY: u8 = 0;
const VALUE: u8 = 1;

impl DocumentIndexKey {
    pub const fn encoding_version(&self) -> u32 {
        DOCUMENT_INDEX_KEY_ENCODING_VERSION
    }

    pub fn component_count(&self) -> usize {
        self.components.len()
    }

    /// Serialize one equality tuple. This is not a BSON sort key, storage
    /// checksum, or index identity. Callers must supply collection/index scope.
    pub fn to_bytes(&self) -> EngineResult<Vec<u8>> {
        self.to_bytes_with_check(&mut || Ok(()))
    }

    /// Preflight every length before allocating the output; check interruption
    /// before/after copying each bounded component. No partial frame is returned.
    pub fn to_bytes_with_check(
        &self,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Vec<u8>> {
        let length = self.encoded_len_with_check(check)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(allocation)?;
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&DOCUMENT_INDEX_KEY_ENCODING_VERSION.to_be_bytes());
        bytes.extend_from_slice(&(self.components.len() as u32).to_be_bytes());
        for component in &self.components {
            check()?;
            match component.as_ref() {
                Component::EmptyArray => bytes.push(EMPTY_ARRAY),
                Component::Value(key) => {
                    bytes.push(VALUE);
                    bytes.extend_from_slice(&(key.as_bytes().len() as u32).to_be_bytes());
                    bytes.extend_from_slice(key.as_bytes());
                }
            }
            check()?;
        }
        check()?;
        Ok(bytes)
    }

    // Let the multi-index preparer charge output before allocating it. Keep
    // this preflight shared with serialization so framing charges cannot drift.
    pub(super) fn encoded_len_with_check(
        &self,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<usize> {
        check()?;
        if self.components.is_empty() || self.components.len() > MAX_COMPONENTS {
            return Err(limit());
        }
        let mut length = HEADER_BYTES;
        for component in &self.components {
            check()?;
            let size = match component.as_ref() {
                Component::EmptyArray => 1,
                Component::Value(key) => {
                    if key.as_bytes().len() > MAX_VALUE_BYTES {
                        return Err(limit());
                    }
                    5 + key.as_bytes().len()
                }
            };
            length = length
                .checked_add(size)
                .filter(|n| *n <= MAX_DOCUMENT_INDEX_KEY_BYTES)
                .ok_or_else(limit)?;
        }
        check()?;
        Ok(length)
    }

    /// Validate and own a complete persisted tuple. Malformed/future frames are
    /// data corruption, never silently normalized. The codec accepts canonical
    /// BSON component identities; field membership and supported index value
    /// semantics remain the generator's responsibility, not a framing rule.
    pub fn from_bytes(bytes: &[u8]) -> EngineResult<Self> {
        Self::from_bytes_with_check(bytes, &mut || Ok(()))
    }

    /// Checks interruption between bounded components. Neither successful
    /// prefixes nor partially validated keys escape after an error.
    pub fn from_bytes_with_check(
        bytes: &[u8],
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Self> {
        check()?;
        if bytes.len() > MAX_DOCUMENT_INDEX_KEY_BYTES {
            return Err(corrupt("document index key exceeds the frame limit"));
        }
        let mut cursor = Cursor { bytes, offset: 0 };
        if cursor.take(MAGIC.len())? != MAGIC {
            return Err(corrupt("document index key has invalid magic"));
        }
        if cursor.u32()? != DOCUMENT_INDEX_KEY_ENCODING_VERSION {
            return Err(corrupt("document index key has an unsupported version"));
        }
        let count = cursor.u32()? as usize;
        if !(1..=MAX_COMPONENTS).contains(&count) {
            return Err(corrupt("document index key has an invalid component count"));
        }
        let mut components = Vec::new();
        components.try_reserve_exact(count).map_err(allocation)?;
        for _ in 0..count {
            check()?;
            let component = match cursor.take(1)?[0] {
                EMPTY_ARRAY => Component::EmptyArray,
                VALUE => {
                    let length = cursor.u32()? as usize;
                    if length > MAX_VALUE_BYTES {
                        return Err(corrupt("document index key exceeds the component limit"));
                    }
                    let key = CanonicalBsonKey::from_bytes(cursor.take(length)?)
                        .map_err(bounded_component_error)?;
                    Component::Value(key)
                }
                _ => return Err(corrupt("document index key has an invalid component tag")),
            };
            check()?;
            components.push(Arc::new(component));
        }
        if cursor.offset != bytes.len() {
            return Err(corrupt("document index key has trailing bytes"));
        }
        check()?;
        Ok(Self { components })
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, length: usize) -> EngineResult<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|n| *n <= self.bytes.len())
            .ok_or_else(|| corrupt("document index key is truncated"))?;
        let bytes = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn u32(&mut self) -> EngineResult<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("fixed-width slice"),
        ))
    }
}

fn corrupt(message: &'static str) -> EngineError {
    EngineError::new(EngineErrorKind::DataCorruption, message)
}

fn bounded_component_error(error: BsonError) -> EngineError {
    // The frame has already enforced an 8 MiB component bound, below BBKY's
    // 16 MiB bound. Its validator's only remaining Oversized path is failure
    // to reserve the owned byte buffer; do not mislabel memory pressure as
    // corruption and potentially fence an otherwise healthy data root.
    if error.kind() == BsonErrorKind::Oversized {
        limit()
    } else {
        error.into_engine_error(BsonErrorContext::StoredData)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{BsonDocument, BsonValue, DocumentIndexKeyGenerator};
    use proptest::prelude::*;
    use std::hash::{Hash, Hasher};

    fn compound(values: &[BsonValue]) -> DocumentIndexKey {
        // The framing layer can carry every canonical BSON identity. Generator
        // eligibility/membership are deliberately separate and oracle-tested.
        DocumentIndexKey {
            components: values
                .iter()
                .map(|value| Arc::new(Component::Value(CanonicalBsonKey::encode(value).unwrap())))
                .collect(),
        }
    }

    fn empty_and_null() -> DocumentIndexKey {
        let fields =
            BsonDocument::from_entries([("v", BsonValue::Int32(1)), ("w", BsonValue::Int32(-1))])
                .unwrap();
        let input = BsonDocument::from_entries([("v", BsonValue::Array(vec![]))]).unwrap();
        DocumentIndexKeyGenerator::compile(&fields, false, None)
            .unwrap()
            .keys(&input)
            .unwrap()
            .remove(0)
    }

    fn hash(key: &DocumentIndexKey) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn fixed_v1_fixture_separates_empty_array_and_null() {
        let key = empty_and_null();
        let expected = b"BDIK\0\0\0\x01\0\0\0\x02\0\x01\0\0\0\x09BBKY\0\0\0\x01\x01";
        assert_eq!(expected.len(), 27);
        assert_eq!(key.to_bytes().unwrap(), expected);
        assert_eq!(key.encoding_version(), 1);
        assert_eq!(key.component_count(), 2);
        let decoded = DocumentIndexKey::from_bytes(expected).unwrap();
        assert_eq!(decoded, key);
        assert_eq!(hash(&decoded), hash(&key));
        assert_eq!(decoded.to_bytes().unwrap(), expected);
        assert_ne!(key, compound(&[BsonValue::Null, BsonValue::Null]));
    }

    #[test]
    fn tuple_framing_preserves_identity_order_and_all_canonical_families() {
        use crate::document::{
            BsonBinary, BsonDateTime, BsonDecimal128, BsonJavaScript, BsonObjectId, BsonRegex,
            BsonTimestamp,
        };
        let scope = BsonDocument::from_entries([("private", BsonValue::Int32(1))]).unwrap();
        let values = [
            BsonValue::MinKey,
            BsonValue::Null,
            BsonValue::Boolean(true),
            BsonValue::Int32(1),
            BsonValue::Int64(i64::MAX),
            BsonValue::Double(0.1),
            BsonValue::Double(f64::NAN),
            BsonValue::Double(f64::INFINITY),
            BsonValue::Decimal128(BsonDecimal128::parse("1E-6176").unwrap()),
            BsonValue::from("private\0é"),
            BsonValue::Binary(BsonBinary::new(128, b"private")),
            BsonValue::ObjectId(BsonObjectId::from_bytes([7; 12])),
            BsonValue::DateTime(BsonDateTime::from_millis(-1)),
            BsonValue::Timestamp(BsonTimestamp::new(7, 2)),
            BsonValue::RegularExpression(BsonRegex::new("private", "im").unwrap()),
            BsonValue::JavaScript(BsonJavaScript::new("private")),
            BsonValue::JavaScript(BsonJavaScript::with_scope("private", scope.clone())),
            BsonValue::Document(scope),
            BsonValue::Array(vec![BsonValue::Null]),
            BsonValue::MaxKey,
        ];
        let key = compound(&values);
        let bytes = key.to_bytes().unwrap();
        let restored = DocumentIndexKey::from_bytes(&bytes).unwrap();
        assert_eq!(restored, key);
        assert_eq!(restored.to_bytes().unwrap(), bytes);
        assert_eq!(hash(&restored), hash(&key));
        assert!(!format!("{restored:?}").contains("private"));
        assert_eq!(
            compound(&[BsonValue::Int32(1)]).to_bytes().unwrap(),
            compound(&[BsonValue::Double(1.0)]).to_bytes().unwrap()
        );
        assert_ne!(
            compound(&[BsonValue::from("ab"), BsonValue::from("c")])
                .to_bytes()
                .unwrap(),
            compound(&[BsonValue::from("a"), BsonValue::from("bc")])
                .to_bytes()
                .unwrap()
        );
        assert_ne!(
            compound(&[BsonValue::Int32(1), BsonValue::Int32(2)])
                .to_bytes()
                .unwrap(),
            compound(&[BsonValue::Int32(2), BsonValue::Int32(1)])
                .to_bytes()
                .unwrap()
        );
    }

    #[test]
    fn malformed_frames_are_corruption_without_payloads_or_normalization() {
        assert_eq!(
            bounded_component_error(BsonError::new(
                BsonErrorKind::Oversized,
                "allocation failed"
            ))
            .kind(),
            EngineErrorKind::LimitExceeded
        );
        assert_eq!(
            bounded_component_error(BsonError::new(
                BsonErrorKind::NestingLimit,
                "invalid stored depth"
            ))
            .kind(),
            EngineErrorKind::DataCorruption
        );
        let bytes = empty_and_null().to_bytes().unwrap();
        for end in 0..bytes.len() {
            assert_eq!(
                DocumentIndexKey::from_bytes(&bytes[..end])
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::DataCorruption
            );
        }
        let mut malformed = Vec::new();
        for (position, replacement) in [
            (0, b'X'),
            (7, 2),
            (11, 0),
            (11, 33),
            (12, 2),
            (13, 2),
            (17, 0),
            (18, b'X'),
            (25, 2),
            (26, 255),
        ] {
            let mut changed = bytes.clone();
            changed[position] = replacement;
            malformed.push(changed);
        }
        let mut oversized_length = bytes.clone();
        oversized_length[14..18].copy_from_slice(&u32::MAX.to_be_bytes());
        malformed.push(oversized_length);
        let mut trailing = bytes;
        trailing.extend_from_slice(b"private");
        malformed.push(trailing);
        malformed.push(
            CanonicalBsonKey::encode(&BsonValue::from("private"))
                .unwrap()
                .into_bytes(),
        );
        malformed.push(
            crate::core::CanonicalIndexKey::encode_values(&[crate::core::Value::Int64(1)])
                .unwrap()
                .into_bytes(),
        );
        for bytes in malformed {
            let error = DocumentIndexKey::from_bytes(&bytes).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
            assert!(!format!("{error:?}").contains("private"));
        }
    }

    #[test]
    fn component_count_and_byte_limits_are_preflighted() {
        let make = |count| {
            let shared = Arc::new(Component::EmptyArray);
            DocumentIndexKey {
                components: vec![shared; count],
            }
        };
        assert!(make(0).to_bytes().is_err());
        assert!(make(33).to_bytes().is_err());
        let maximum = make(32).to_bytes().unwrap();
        assert_eq!(
            DocumentIndexKey::from_bytes(&maximum)
                .unwrap()
                .component_count(),
            32
        );
        // Canonical string framing is 8 header + 1 tag + 4 length bytes.
        let exact =
            CanonicalBsonKey::encode(&BsonValue::String("x".repeat(MAX_VALUE_BYTES - 13))).unwrap();
        assert_eq!(exact.as_bytes().len(), MAX_VALUE_BYTES);
        let component = Arc::new(Component::Value(exact));
        let key = DocumentIndexKey {
            components: vec![Arc::clone(&component)],
        };
        let bytes = key.to_bytes().unwrap();
        assert_eq!(DocumentIndexKey::from_bytes(&bytes).unwrap(), key);
        let expanded = DocumentIndexKey {
            components: vec![component; 8],
        };
        assert_eq!(
            expanded.to_bytes().unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let over = compound(&[BsonValue::String("x".repeat(MAX_VALUE_BYTES - 12))]);
        assert_eq!(
            over.to_bytes().unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let bytes = vec![0; MAX_DOCUMENT_INDEX_KEY_BYTES + 1];
        assert_eq!(
            DocumentIndexKey::from_bytes(&bytes).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn every_control_checkpoint_can_interrupt_without_partial_frames_or_keys() {
        let key = empty_and_null();
        let mut checks = 0;
        let expected = key
            .to_bytes_with_check(&mut || {
                checks += 1;
                Ok(())
            })
            .unwrap();
        let cancel = || EngineError::new(EngineErrorKind::Cancelled, "cancelled");
        for stop in 1..=checks {
            let mut calls = 0;
            assert_eq!(
                key.to_bytes_with_check(&mut || {
                    calls += 1;
                    if calls == stop { Err(cancel()) } else { Ok(()) }
                })
                .unwrap_err()
                .kind(),
                EngineErrorKind::Cancelled
            );
        }
        let mut checks = 0;
        assert_eq!(
            DocumentIndexKey::from_bytes_with_check(&expected, &mut || {
                checks += 1;
                Ok(())
            })
            .unwrap(),
            key
        );
        for stop in 1..=checks {
            let mut calls = 0;
            assert_eq!(
                DocumentIndexKey::from_bytes_with_check(&expected, &mut || {
                    calls += 1;
                    if calls == stop { Err(cancel()) } else { Ok(()) }
                })
                .unwrap_err()
                .kind(),
                EngineErrorKind::Cancelled
            );
        }
        assert_eq!(
            DocumentIndexKey::from_bytes(&key.to_bytes().unwrap()).unwrap(),
            key
        );
    }

    proptest! {
        #[test]
        fn arbitrary_frames_never_panic_or_normalize(bytes in prop::collection::vec(any::<u8>(), 0..2048)) {
            if let Ok(key) = DocumentIndexKey::from_bytes(&bytes) {
                prop_assert_eq!(key.to_bytes().unwrap(), bytes);
            }
        }

        #[test]
        fn framed_arbitrary_bson_components_never_panic_or_normalize(tail in prop::collection::vec(any::<u8>(), 0..2048)) {
            let mut component = b"BBKY\0\0\0\x01".to_vec();
            component.extend_from_slice(&tail);
            let mut bytes = b"BDIK\0\0\0\x01\0\0\0\x01\x01".to_vec();
            bytes.extend_from_slice(&(component.len() as u32).to_be_bytes());
            bytes.extend_from_slice(&component);
            if let Ok(key) = DocumentIndexKey::from_bytes(&bytes) {
                prop_assert_eq!(key.to_bytes().unwrap(), bytes);
            }
        }

        #[test]
        fn generated_tuples_round_trip(number in any::<i64>(), text in ".{0,128}") {
            let key = compound(&[BsonValue::Int64(number), BsonValue::String(text)]);
            let bytes = key.to_bytes().unwrap();
            let restored = DocumentIndexKey::from_bytes(&bytes).unwrap();
            prop_assert_eq!(hash(&restored), hash(&key));
            prop_assert_eq!(&restored, &key);
            prop_assert_eq!(restored.to_bytes().unwrap(), bytes);
        }
    }
}
