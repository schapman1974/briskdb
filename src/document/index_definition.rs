//! Bounded, protocol-neutral normalization of ordinary index key declarations.
//!
//! This does not build an index or make a pending declaration enforce uniqueness.

use std::collections::BTreeSet;

use crate::core::{EngineError, EngineErrorKind, EngineResult};

use super::{
    BsonCodecOptions, BsonDocument, BsonErrorContext, BsonValue, MAX_DOCUMENT_INDEX_NAME_BYTES,
    encode_document_with_options,
};

const MAX_INDEX_FIELDS: usize = 32;
const MAX_PATH_COMPONENTS: usize = 100;
const MAX_SPEC_BYTES: usize = 1024 * 1024;

pub(crate) fn normalize_index_definition(
    keys: &BsonDocument,
    name: Option<&str>,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<(BsonDocument, String)> {
    check()?;
    if keys.is_empty() {
        return Err(invalid("document index requires at least one key"));
    }
    if keys.len() > MAX_INDEX_FIELDS {
        return Err(limit("document index exceeds the field limit"));
    }
    encode_document_with_options(
        keys,
        &BsonCodecOptions::new().with_max_document_bytes(MAX_SPEC_BYTES),
    )
    .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    let mut seen = BTreeSet::new();
    let mut normalized = BsonDocument::new();
    let mut generated_name = String::new();
    for (field, value) in keys.iter() {
        check()?;
        if !seen.insert(field) {
            return Err(invalid("document index fields must be distinct"));
        }
        let mut depth = 0;
        for part in field.split('.') {
            check()?;
            if part.is_empty() || part.starts_with('$') || part.contains('\0') {
                return Err(invalid("document index field path is invalid"));
            }
            depth += 1;
            if depth > MAX_PATH_COMPONENTS {
                return Err(limit("document index path exceeds the component limit"));
            }
        }
        // Numeric aliases describe the same direction. Booleans and special
        // index types are not directions, even when an adapter could coerce them.
        let direction = match value {
            BsonValue::Int32(_)
            | BsonValue::Int64(_)
            | BsonValue::Double(_)
            | BsonValue::Decimal128(_) => {
                if value == &BsonValue::Int32(1) {
                    1
                } else if value == &BsonValue::Int32(-1) {
                    -1
                } else {
                    return Err(invalid(
                        "document index direction must be one or negative one",
                    ));
                }
            }
            _ => return Err(invalid("document index direction must be numeric")),
        };
        normalized
            .push(field, BsonValue::Int32(direction))
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        if name.is_none() {
            let suffix = if direction == 1 { "_1" } else { "_-1" };
            let separator = usize::from(!generated_name.is_empty());
            if generated_name.len() + separator + field.len() + suffix.len()
                > MAX_DOCUMENT_INDEX_NAME_BYTES
            {
                return Err(limit(
                    "generated document index name exceeds the byte limit",
                ));
            }
            if separator != 0 {
                generated_name.push('_');
            }
            generated_name.push_str(field);
            generated_name.push_str(suffix);
        }
    }
    check()?;
    let name = name.unwrap_or(&generated_name);
    if name.is_empty()
        || name.len() > MAX_DOCUMENT_INDEX_NAME_BYTES
        || name.contains('\0')
        || matches!(name, "_id" | "_id_")
    {
        return Err(invalid("document index name is invalid or reserved"));
    }
    Ok((normalized, name.to_owned()))
}

fn invalid(message: &'static str) -> EngineError {
    EngineError::new(EngineErrorKind::InvalidArgument, message)
}

fn limit(message: &'static str) -> EngineError {
    EngineError::new(EngineErrorKind::LimitExceeded, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::BsonDecimal128;

    #[test]
    fn ordered_directions_have_canonical_bson_and_deterministic_names() {
        for direction in [
            BsonValue::Int32(1),
            BsonValue::Int64(1),
            BsonValue::Double(1.0),
            BsonValue::Decimal128(BsonDecimal128::parse("1.00").unwrap()),
        ] {
            let keys = BsonDocument::from_entries([
                ("profile.name", direction),
                ("rank", BsonValue::Int64(-1)),
            ])
            .unwrap();
            let (normalized, name) =
                normalize_index_definition(&keys, None, &mut || Ok(())).unwrap();
            assert_eq!(name, "profile.name_1_rank_-1");
            assert!(
                normalized
                    .get_first("profile.name")
                    .unwrap()
                    .representation_eq(&BsonValue::Int32(1))
            );
            assert!(
                normalized
                    .get_first("rank")
                    .unwrap()
                    .representation_eq(&BsonValue::Int32(-1))
            );
            assert_eq!(
                normalize_index_definition(&keys, Some("custom"), &mut || Ok(()))
                    .unwrap()
                    .1,
                "custom"
            );
        }
    }

    #[test]
    fn malformed_keys_and_reserved_names_fail_without_payloads() {
        for field in ["", ".a", "a.", "a..b", "$private", "a.$private"] {
            let keys = BsonDocument::from_entries([(field, BsonValue::Int32(1))]).unwrap();
            let error = normalize_index_definition(&keys, None, &mut || Ok(())).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
            assert!(!error.to_string().contains("private"));
        }
        for direction in [
            BsonValue::Boolean(true),
            BsonValue::Int32(0),
            BsonValue::Int64(2),
            BsonValue::Double(f64::NAN),
            BsonValue::Double(f64::INFINITY),
            BsonValue::from("hashed"),
            BsonValue::Null,
        ] {
            let keys = BsonDocument::from_entries([("private", direction)]).unwrap();
            assert!(normalize_index_definition(&keys, None, &mut || Ok(())).is_err());
        }
        let duplicate =
            BsonDocument::from_entries([("a", BsonValue::Int32(1)), ("a", BsonValue::Int32(-1))])
                .unwrap();
        assert!(normalize_index_definition(&duplicate, None, &mut || Ok(())).is_err());
        assert!(normalize_index_definition(&BsonDocument::new(), None, &mut || Ok(())).is_err());
        let keys = BsonDocument::from_entries([("a", BsonValue::Int32(1))]).unwrap();
        for name in ["", "_id", "_id_", "bad\0name"] {
            assert!(normalize_index_definition(&keys, Some(name), &mut || Ok(())).is_err());
        }
    }

    #[test]
    fn field_path_name_limits_and_cancellation_are_bounded() {
        let fields = (0..32).map(|i| (format!("f{i}"), BsonValue::Int32(1)));
        assert!(
            normalize_index_definition(
                &BsonDocument::from_entries(fields).unwrap(),
                None,
                &mut || Ok(())
            )
            .is_ok()
        );
        let keys = BsonDocument::from_entries([("x".repeat(MAX_SPEC_BYTES), BsonValue::Int32(1))])
            .unwrap();
        assert_eq!(
            normalize_index_definition(&keys, Some("short"), &mut || Ok(()))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        let keys = BsonDocument::from_entries([("é".repeat(127), BsonValue::Int32(1))]).unwrap();
        assert_eq!(
            normalize_index_definition(&keys, None, &mut || Ok(()))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        let fields = (0..33).map(|i| (format!("f{i}"), BsonValue::Int32(1)));
        assert_eq!(
            normalize_index_definition(
                &BsonDocument::from_entries(fields).unwrap(),
                None,
                &mut || Ok(())
            )
            .unwrap_err()
            .kind(),
            EngineErrorKind::LimitExceeded
        );
        let keys = BsonDocument::from_entries([("a".repeat(254), BsonValue::Int32(1))]).unwrap();
        assert_eq!(
            normalize_index_definition(&keys, None, &mut || Ok(()))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        assert!(normalize_index_definition(&keys, Some("short"), &mut || Ok(())).is_ok());
        let keys = BsonDocument::from_entries([(
            (0..101).map(|_| "a").collect::<Vec<_>>().join("."),
            BsonValue::Int32(1),
        )])
        .unwrap();
        assert_eq!(
            normalize_index_definition(&keys, Some("short"), &mut || Ok(()))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        let mut calls = 0;
        let error = normalize_index_definition(&keys, None, &mut || {
            calls += 1;
            if calls == 5 {
                Err(EngineError::new(
                    EngineErrorKind::Cancelled,
                    "index validation cancelled",
                ))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        assert_eq!(calls, 5);
    }
}
