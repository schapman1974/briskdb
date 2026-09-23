//! Bounded index-declaration normalization and borrowed retained-metadata views.
//!
//! This does not build an index or make a pending declaration enforce uniqueness.

use std::{collections::BTreeSet, fmt};

use crate::core::{EngineError, EngineErrorKind, EngineResult};

use super::{
    BsonCodecOptions, BsonDocument, BsonErrorContext, BsonValue, DocumentFilter,
    DocumentIndexKeyGenerator, DocumentIndexMetadata, DocumentIndexRequest,
    MAX_DOCUMENT_INDEX_NAME_BYTES, encode_document_with_options,
};

const MAX_INDEX_FIELDS: usize = 32;
const MAX_PATH_COMPONENTS: usize = 100;
const MAX_SPEC_BYTES: usize = 1024 * 1024;

pub(crate) enum DocumentIndexBuildDefinition {
    BuiltIn,
    Secondary {
        specification: BsonDocument,
        name: String,
        unique: bool,
    },
}

/// Eagerly normalize every entry before allowing any namespace/index mutation.
pub(crate) fn normalize_index_batch(
    indexes: Box<[DocumentIndexRequest]>,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<Vec<DocumentIndexBuildDefinition>> {
    let mut normalized = Vec::new();
    normalized.try_reserve_exact(indexes.len()).map_err(|_| {
        EngineError::new(
            EngineErrorKind::LimitExceeded,
            "unable to allocate document index batch",
        )
    })?;
    let mut retained = 0usize;
    for index in indexes {
        check()?;
        if index.keys().len() == 1
            && index
                .keys()
                .get_first("_id")
                .is_some_and(|value| value == &BsonValue::Int32(-1))
        {
            return Err(EngineError::new(
                EngineErrorKind::Unsupported,
                "descending built-in document ID index creation is not supported",
            ));
        }
        let builtin = index.keys().len() == 1
            && index
                .keys()
                .get_first("_id")
                .is_some_and(|value| value == &BsonValue::Int32(1));
        if builtin && index.unique() {
            return Err(super::DocumentIndexError::InvalidIdOptions.into_engine_error());
        }
        // The built-in name is not a user declaration. Still validate all keys
        // and membership predicates, even though a valid _id request is a no-op.
        let index = if builtin {
            index.with_name("_id_1")?
        } else {
            index
        };
        let (specification, name, unique) = normalize_index_request(index, check)?;
        let bytes = super::encode_document(&specification)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        retained = retained
            .checked_add(bytes.len() + name.len())
            .filter(|bytes| *bytes <= super::MAX_DOCUMENT_REQUEST_BYTES)
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::LimitExceeded,
                    "normalized document index batch exceeds capacity",
                )
            })?;
        normalized.push(if builtin {
            DocumentIndexBuildDefinition::BuiltIn
        } else {
            DocumentIndexBuildDefinition::Secondary {
                specification,
                name,
                unique,
            }
        });
    }
    check()?;
    Ok(normalized)
}

/// Borrowed keys and membership options from an understood catalog encoding.
///
/// This view does not normalize or rewrite stored BSON, validate documents,
/// activate an index, or confer planner/uniqueness authority. Compile the keys
/// and options with [`DocumentIndexKeyGenerator`] before using their semantics.
#[derive(Clone, Copy)]
pub struct DocumentIndexDefinition<'a> {
    keys: &'a BsonDocument,
    sparse: bool,
    partial_filter: Option<&'a BsonDocument>,
}

impl<'a> DocumentIndexDefinition<'a> {
    pub const fn keys(self) -> &'a BsonDocument {
        self.keys
    }

    pub const fn sparse(self) -> bool {
        self.sparse
    }

    pub const fn partial_filter(self) -> Option<&'a BsonDocument> {
        self.partial_filter
    }
}

impl fmt::Debug for DocumentIndexDefinition<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentIndexDefinition")
            .finish_non_exhaustive()
    }
}

/// Recognize only flat numeric keys or the exact supported v2 envelope. In
/// particular, fields named `key`, `v`, or `sparse` are ordinary flat keys when
/// their values are directions. Unknown legacy envelopes remain opaque/readable.
pub(crate) fn index_definition_view(
    index: &DocumentIndexMetadata,
) -> Option<DocumentIndexDefinition<'_>> {
    let specification = index.specification();
    if flat_numeric_keys(specification) {
        return Some(DocumentIndexDefinition {
            keys: specification,
            sparse: false,
            partial_filter: None,
        });
    }
    let built_in = index.is_built_in();
    if specification.len() != if built_in { 4 } else { 6 }
        || !matches!(specification.get_first("v"), Some(BsonValue::Int32(2)))
        || !matches!(specification.get_first("name"), Some(BsonValue::String(name)) if name == index.name())
        || !matches!(specification.get_first("unique"), Some(BsonValue::Boolean(unique)) if *unique == index.is_unique())
    {
        return None;
    }
    let Some(BsonValue::Document(keys)) = specification.get_first("key") else {
        return None;
    };
    if !flat_numeric_keys(keys) {
        return None;
    }
    let (sparse, partial_filter) = if built_in {
        (false, None)
    } else {
        let Some(BsonValue::Boolean(sparse)) = specification.get_first("sparse") else {
            return None;
        };
        let partial = match specification.get_first("partialFilterExpression")? {
            BsonValue::Null => None,
            BsonValue::Document(filter) => Some(filter),
            _ => return None,
        };
        if *sparse && partial.is_some() {
            return None;
        }
        (*sparse, partial)
    };
    // The required distinct field lookups plus exact count reject duplicates
    // and unknown options without silently discarding their meaning.
    Some(DocumentIndexDefinition {
        keys,
        sparse,
        partial_filter,
    })
}

fn flat_numeric_keys(keys: &BsonDocument) -> bool {
    !keys.is_empty()
        && keys.len() <= MAX_INDEX_FIELDS
        && keys.iter().all(|(_, value)| {
            matches!(
                value,
                BsonValue::Int32(_)
                    | BsonValue::Int64(_)
                    | BsonValue::Double(_)
                    | BsonValue::Decimal128(_)
            ) && (value == &BsonValue::Int32(1) || value == &BsonValue::Int32(-1))
        })
}

/// Preserve the flat encoding for ordinary declarations. Advanced declarations
/// use the same ordered v2 BSON envelope already retained by TinyMongo import.
pub(crate) fn normalize_index_request(
    index: DocumentIndexRequest,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<(BsonDocument, String, bool)> {
    let (keys, name, unique, sparse, partial) = index.into_parts();
    let (keys, name) = normalize_index_definition(&keys, name.as_deref(), check)?;
    if !sparse && partial.is_none() {
        return Ok((keys, name, unique));
    }
    // Reuse the source-locked membership dialect, including eager validation
    // of every predicate branch and rejection of sparse + partial together.
    DocumentIndexKeyGenerator::compile_with_check(
        &keys,
        sparse,
        partial.as_ref().map(DocumentFilter::document),
        check,
    )?;
    let specification = BsonDocument::from_entries([
        ("v", BsonValue::Int32(2)),
        ("name", BsonValue::String(name.clone())),
        ("key", BsonValue::Document(keys)),
        ("unique", BsonValue::Boolean(unique)),
        ("sparse", BsonValue::Boolean(sparse)),
        (
            "partialFilterExpression",
            partial
                .map(|filter| BsonValue::Document(filter.into_document()))
                .unwrap_or(BsonValue::Null),
        ),
    ])
    .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    // Bound the complete retained envelope, not just keys and filter separately.
    encode_document_with_options(
        &specification,
        &BsonCodecOptions::new().with_max_document_bytes(MAX_SPEC_BYTES),
    )
    .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    check()?;
    Ok((specification, name, unique))
}

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
    use crate::document::{
        BsonDecimal128, DocumentIndexId, DocumentIndexLifecycle, encode_document,
    };

    fn document<const N: usize>(entries: [(&str, BsonValue); N]) -> BsonDocument {
        BsonDocument::from_entries(entries).unwrap()
    }

    fn metadata(
        specification: BsonDocument,
        name: &str,
        unique: bool,
        built_in: bool,
    ) -> DocumentIndexMetadata {
        DocumentIndexMetadata::from_validated_parts(
            DocumentIndexId::from_validated(1),
            name.into(),
            specification,
            unique,
            built_in,
            if built_in {
                DocumentIndexLifecycle::Ready
            } else {
                DocumentIndexLifecycle::PendingBuild
            },
        )
    }

    fn advanced_request(sparse: bool, partial: Option<BsonDocument>) -> DocumentIndexRequest {
        let mut request = DocumentIndexRequest::new(document([
            ("profile.name", BsonValue::Int64(1)),
            ("rank", BsonValue::Double(-1.0)),
        ]))
        .unwrap()
        .with_unique(true)
        .with_sparse(sparse)
        .with_name("advanced")
        .unwrap();
        if let Some(filter) = partial {
            request = request.with_partial_filter(DocumentFilter::new(filter).unwrap());
        }
        request
    }

    #[test]
    fn advanced_declarations_retain_the_import_envelope_and_exact_partial_bson() {
        let filter = document([
            ("active", BsonValue::Boolean(true)),
            (
                "rank",
                BsonValue::Document(document([("$gte", BsonValue::Int64(2))])),
            ),
        ]);
        for (sparse, partial) in [(true, None), (false, Some(filter.clone()))] {
            let (spec, name, unique) =
                normalize_index_request(advanced_request(sparse, partial.clone()), &mut || Ok(()))
                    .unwrap();
            assert_eq!(name, "advanced");
            assert!(unique);
            assert_eq!(
                spec.iter().map(|(name, _)| name).collect::<Vec<_>>(),
                [
                    "v",
                    "name",
                    "key",
                    "unique",
                    "sparse",
                    "partialFilterExpression"
                ]
            );
            let bytes = encode_document(&spec).unwrap();
            let index = metadata(spec, &name, unique, false);
            let definition = index.definition().unwrap();
            assert_eq!(definition.sparse(), sparse);
            assert_eq!(
                definition
                    .partial_filter()
                    .map(|filter| encode_document(filter).unwrap()),
                partial
                    .as_ref()
                    .map(|filter| encode_document(filter).unwrap())
            );
            assert_eq!(
                encode_document(definition.keys()).unwrap(),
                encode_document(&document([
                    ("profile.name", BsonValue::Int32(1)),
                    ("rank", BsonValue::Int32(-1))
                ]))
                .unwrap()
            );
            let generator = DocumentIndexKeyGenerator::compile(
                definition.keys(),
                definition.sparse(),
                definition.partial_filter(),
            )
            .unwrap();
            assert!(generator.keys(&BsonDocument::new()).unwrap().is_empty());
            let member = document([
                ("active", BsonValue::Boolean(true)),
                ("rank", BsonValue::Int32(3)),
            ]);
            assert_eq!(generator.keys(&member).unwrap().len(), 1);
            assert_eq!(encode_document(index.specification()).unwrap(), bytes);
        }
    }

    #[test]
    fn ordinary_keys_named_like_options_remain_flat_and_builtin_view_is_distinct() {
        let keys = document([
            ("v", BsonValue::Int32(1)),
            ("name", BsonValue::Int32(1)),
            ("key", BsonValue::Int32(-1)),
            ("unique", BsonValue::Int32(1)),
            ("sparse", BsonValue::Int32(1)),
            ("partialFilterExpression", BsonValue::Int32(1)),
        ]);
        let request = DocumentIndexRequest::new(keys.clone())
            .unwrap()
            .with_name("flat")
            .unwrap();
        let (spec, _, _) = normalize_index_request(request, &mut || Ok(())).unwrap();
        assert_eq!(
            encode_document(&spec).unwrap(),
            encode_document(&keys).unwrap()
        );
        let index = metadata(spec, "flat", false, false);
        let definition = index.definition().unwrap();
        assert_eq!(definition.keys(), &keys);
        assert!(!definition.sparse());
        assert!(definition.partial_filter().is_none());
        let builtin = metadata(
            document([
                ("v", BsonValue::Int32(2)),
                ("name", BsonValue::from("_id_")),
                (
                    "key",
                    BsonValue::Document(document([("_id", BsonValue::Int32(1))])),
                ),
                ("unique", BsonValue::Boolean(true)),
            ]),
            "_id_",
            true,
            true,
        );
        assert_eq!(
            builtin.definition().unwrap().keys(),
            &document([("_id", BsonValue::Int32(1))])
        );
        assert!(!builtin.definition().unwrap().sparse());
    }

    #[test]
    fn unknown_or_inconsistent_envelopes_are_not_silently_reinterpreted() {
        let (spec, _, _) =
            normalize_index_request(advanced_request(true, None), &mut || Ok(())).unwrap();
        for (field, value) in [
            ("v", BsonValue::Int32(3)),
            ("v", BsonValue::Double(2.0)),
            ("name", BsonValue::from("other")),
            ("unique", BsonValue::Boolean(false)),
            ("sparse", BsonValue::Int32(1)),
            ("key", BsonValue::String("opaque".into())),
            ("partialFilterExpression", BsonValue::Boolean(false)),
            (
                "partialFilterExpression",
                BsonValue::Document(document([("active", BsonValue::Boolean(true))])),
            ),
        ] {
            let changed = BsonDocument::from_entries(spec.iter().map(|(key, original)| {
                (
                    key,
                    if key == field {
                        value.clone()
                    } else {
                        original.clone()
                    },
                )
            }))
            .unwrap();
            let bytes = encode_document(&changed).unwrap();
            let index = metadata(changed, "advanced", true, false);
            assert!(index.definition().is_none(), "{field}");
            assert_eq!(encode_document(index.specification()).unwrap(), bytes);
        }
        for field in ["unknown", "sparse"] {
            let mut changed = spec.clone();
            changed.push(field, BsonValue::Boolean(true)).unwrap();
            assert!(
                metadata(changed, "advanced", true, false)
                    .definition()
                    .is_none()
            );
        }
        let missing = BsonDocument::from_entries(
            spec.iter()
                .filter(|(field, _)| *field != "sparse")
                .map(|(field, value)| (field, value.clone())),
        )
        .unwrap();
        assert!(
            metadata(missing, "advanced", true, false)
                .definition()
                .is_none()
        );
    }

    #[test]
    fn invalid_membership_and_sparse_partial_combinations_fail_eagerly() {
        let good = document([("active", BsonValue::Boolean(true))]);
        let unsupported = document([(
            "rank",
            BsonValue::Document(document([("$ne", BsonValue::Int32(2))])),
        )]);
        let invalid_or = document([(
            "$or",
            BsonValue::Array(vec![
                BsonValue::Document(good.clone()),
                BsonValue::Document(unsupported.clone()),
            ]),
        )]);
        for (sparse, partial) in [
            (true, good),
            (false, BsonDocument::new()),
            (false, unsupported),
            (false, invalid_or),
        ] {
            let error =
                normalize_index_request(advanced_request(sparse, Some(partial)), &mut || Ok(()))
                    .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Unsupported);
        }
    }

    #[test]
    fn aggregate_envelope_has_one_bound_even_when_each_payload_fits() {
        let keys =
            BsonDocument::from_entries([("k".repeat(600 * 1024), BsonValue::Int32(1))]).unwrap();
        let filter = document([("value", BsonValue::String("v".repeat(600 * 1024)))]);
        assert!(encode_document(&keys).unwrap().len() < MAX_SPEC_BYTES);
        assert!(encode_document(&filter).unwrap().len() < MAX_SPEC_BYTES);
        let request = DocumentIndexRequest::new(keys)
            .unwrap()
            .with_name("bounded")
            .unwrap()
            .with_partial_filter(DocumentFilter::new(filter).unwrap());
        assert_eq!(
            normalize_index_request(request, &mut || Ok(()))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
    }

    #[test]
    fn advanced_normalization_checks_interruption_without_changing_input() {
        let request = advanced_request(
            false,
            Some(document([("active", BsonValue::Boolean(true))])),
        );
        let before = request.clone();
        let mut total = 0;
        normalize_index_request(request.clone(), &mut || {
            total += 1;
            Ok(())
        })
        .unwrap();
        for stop in [1, 2, total / 2, total] {
            let mut calls = 0;
            let error = normalize_index_request(request.clone(), &mut || {
                calls += 1;
                if calls == stop {
                    Err(EngineError::new(EngineErrorKind::Cancelled, "cancelled"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Cancelled);
            assert_eq!(calls, stop);
            assert_eq!(request, before);
        }
    }

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
