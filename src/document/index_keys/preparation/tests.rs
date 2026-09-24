use super::*;
use crate::document::{
    BsonDecimal128, BsonObjectId, BsonValue, DocumentCollectionOptions, DocumentDatabaseId,
    DocumentIndexKey, DocumentIndexLifecycle, DocumentIndexMetadata, DocumentPlacement,
};

fn doc(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(entries).unwrap()
}

fn keys() -> BsonDocument {
    doc([("v", BsonValue::Int32(1))])
}

fn secondary(id: u64, specification: BsonDocument) -> DocumentIndexMetadata {
    DocumentIndexMetadata::from_validated_parts(
        DocumentIndexId::from_validated(id),
        format!("private-index-{id}"),
        specification,
        true,
        false,
        DocumentIndexLifecycle::PendingBuild,
    )
}

fn membership(id: u64, sparse: bool, partial: Option<BsonDocument>) -> DocumentIndexMetadata {
    secondary(
        id,
        doc([
            ("v", BsonValue::Int32(2)),
            ("name", BsonValue::from(format!("private-index-{id}"))),
            ("key", BsonValue::Document(keys())),
            ("unique", BsonValue::Boolean(true)),
            ("sparse", BsonValue::Boolean(sparse)),
            (
                "partialFilterExpression",
                partial.map(BsonValue::Document).unwrap_or(BsonValue::Null),
            ),
        ]),
    )
}

fn collection(
    indexes: impl IntoIterator<Item = DocumentIndexMetadata>,
) -> DocumentCollectionMetadata {
    let builtin = DocumentIndexMetadata::from_validated_parts(
        DocumentIndexId::from_validated(1),
        "_id_".into(),
        doc([("_id", BsonValue::Int32(1))]),
        true,
        true,
        DocumentIndexLifecycle::Ready,
    );
    DocumentCollectionMetadata::from_validated_parts(
        DocumentCollectionId::from_validated(17),
        DocumentDatabaseId::from_validated(2),
        "private-database".into(),
        "private-collection".into(),
        DocumentCollectionOptions::empty(),
        DocumentPlacement::HashByIdV1,
        std::iter::once(builtin).chain(indexes).collect(),
    )
}

fn frames(result: &PreparedDocumentIndexEntries) -> Vec<(u64, bool, Vec<Vec<u8>>)> {
    result
        .indexes()
        .iter()
        .map(|index| {
            (
                index.index_id().get(),
                index.is_unique(),
                index.keys().to_vec(),
            )
        })
        .collect()
}

fn storage_preparation(unique: bool, fields: BsonDocument) -> DocumentIndexPreparation {
    let index = DocumentIndexMetadata::from_validated_parts(
        DocumentIndexId::from_validated(2),
        "fallback-test".into(),
        fields,
        unique,
        false,
        DocumentIndexLifecycle::Ready,
    );
    DocumentIndexPreparation::compile(&collection([index])).unwrap()
}

#[test]
fn nonunique_storage_fallback_is_separate_from_strict_and_unique_keys() {
    let ordinary = storage_preparation(false, keys());
    let unique = storage_preparation(true, keys());
    for value in [
        BsonValue::Document(doc([("x", BsonValue::Int32(2))])),
        BsonValue::Array(vec![BsonValue::Array(vec![BsonValue::Int32(1)])]),
        BsonValue::Double(f64::NAN),
        BsonValue::Double(f64::INFINITY),
    ] {
        let input = doc([("v", value)]);
        assert_eq!(
            ordinary.prepare(&input).unwrap_err().kind(),
            EngineErrorKind::Unsupported
        );
        assert_eq!(
            unique
                .prepare_for_storage_with_check(&input, &mut || Ok(()))
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
        let prepared = ordinary
            .prepare_for_storage_with_check(&input, &mut || Ok(()))
            .unwrap();
        assert_eq!(
            frames(&prepared),
            vec![(2, false, vec![NON_UNIQUE_FALLBACK_KEY.to_vec()])]
        );
        assert!(DocumentIndexKey::from_bytes(NON_UNIQUE_FALLBACK_KEY).is_err());
    }
    let scalar = doc([("v", BsonValue::Int32(3))]);
    assert_eq!(
        frames(&ordinary.prepare(&scalar).unwrap()),
        frames(
            &ordinary
                .prepare_for_storage_with_check(&scalar, &mut || Ok(()))
                .unwrap()
        )
    );
}

#[test]
fn storage_fallback_covers_intermediate_and_parallel_arrays_without_partial_keys() {
    for (fields, input) in [
        (
            doc([("v.x", BsonValue::Int32(1))]),
            doc([(
                "v",
                BsonValue::Array(vec![BsonValue::Document(doc([("x", BsonValue::Int32(2))]))]),
            )]),
        ),
        (
            doc([("a", BsonValue::Int32(1)), ("b", BsonValue::Int32(1))]),
            doc([
                ("a", BsonValue::Array(vec![BsonValue::Int32(1)])),
                ("b", BsonValue::Array(vec![BsonValue::Int32(2)])),
            ]),
        ),
        (
            keys(),
            doc([(
                "v",
                BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Document(doc([]))]),
            )]),
        ),
    ] {
        let preparation = storage_preparation(false, fields);
        let prepared = preparation
            .prepare_for_storage_with_check(&input, &mut || Ok(()))
            .unwrap();
        assert_eq!(
            frames(&prepared),
            vec![(2, false, vec![NON_UNIQUE_FALLBACK_KEY.to_vec()])]
        );
    }
}

#[test]
fn storage_fallback_keeps_known_membership_exclusions_and_uncertain_sparse_paths() {
    for (sparse, partial, fields, input, fallback) in [
        (
            false,
            Some(doc([("active", BsonValue::Boolean(true))])),
            keys(),
            doc([("v", BsonValue::Document(doc([])))]),
            false,
        ),
        (
            false,
            Some(doc([("active", BsonValue::Boolean(true))])),
            keys(),
            doc([
                ("v", BsonValue::Document(doc([]))),
                ("active", BsonValue::Boolean(true)),
            ]),
            true,
        ),
        (true, None, keys(), doc([]), false),
        (
            true,
            None,
            doc([("v.x", BsonValue::Int32(1))]),
            doc([("v", BsonValue::Array(vec![BsonValue::Document(doc([]))]))]),
            true,
        ),
    ] {
        let specification = doc([
            ("v", BsonValue::Int32(2)),
            ("name", BsonValue::from("membership")),
            ("key", BsonValue::Document(fields)),
            ("unique", BsonValue::Boolean(false)),
            ("sparse", BsonValue::Boolean(sparse)),
            (
                "partialFilterExpression",
                partial.map(BsonValue::Document).unwrap_or(BsonValue::Null),
            ),
        ]);
        let index = DocumentIndexMetadata::from_validated_parts(
            DocumentIndexId::from_validated(2),
            "membership".into(),
            specification,
            false,
            false,
            DocumentIndexLifecycle::Ready,
        );
        let preparation = DocumentIndexPreparation::compile(&collection([index])).unwrap();
        let prepared = preparation
            .prepare_for_storage_with_check(&input, &mut || Ok(()))
            .unwrap();
        assert_eq!(
            frames(&prepared),
            vec![(
                2,
                false,
                if fallback {
                    vec![NON_UNIQUE_FALLBACK_KEY.to_vec()]
                } else {
                    vec![]
                }
            )]
        );
    }
}

#[test]
fn storage_fallback_does_not_swallow_control_errors_or_key_limits() {
    let preparation = storage_preparation(false, keys());
    let input = doc([("v", BsonValue::Document(doc([])))]);
    for kind in [
        EngineErrorKind::Cancelled,
        EngineErrorKind::DataCorruption,
        EngineErrorKind::Unsupported,
        EngineErrorKind::LimitExceeded,
    ] {
        let error = preparation
            .prepare_for_storage_with_check(&input, &mut || {
                Err(EngineError::new(kind, "injected control error"))
            })
            .unwrap_err();
        assert_eq!(error.kind(), kind);
    }
    let too_many = doc([(
        "v",
        BsonValue::Array((0..=16_384).map(BsonValue::Int32).collect()),
    )]);
    assert_eq!(
        preparation
            .prepare_for_storage_with_check(&too_many, &mut || Ok(()))
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
}

#[test]
fn storage_selection_never_interprets_or_enforces_unselected_pending_definitions() {
    let ready = DocumentIndexMetadata::from_validated_parts(
        DocumentIndexId::from_validated(7),
        "ready".into(),
        keys(),
        false,
        false,
        DocumentIndexLifecycle::Ready,
    );
    let opaque = secondary(9, doc([("opaque", BsonValue::from("unknown"))]));
    let metadata = collection([opaque, ready]);
    assert!(DocumentIndexPreparation::compile(&metadata).is_err());
    let active = DocumentIndexPreparation::compile_selected_with_check(
        metadata.id(),
        metadata.indexes(),
        |index| index.lifecycle() == DocumentIndexLifecycle::Ready,
        &mut || Ok(()),
    )
    .unwrap();
    let prepared = active.prepare(&doc([("v", BsonValue::Int32(5))])).unwrap();
    assert_eq!(prepared.indexes().len(), 1);
    assert_eq!(prepared.indexes()[0].index_id().get(), 7);
    assert!(!active.is_empty());
    let empty = DocumentIndexPreparation::compile_selected_with_check(
        metadata.id(),
        metadata.indexes(),
        |_| false,
        &mut || Ok(()),
    )
    .unwrap();
    assert!(empty.is_empty());
}

#[test]
fn preparation_preserves_scoped_encoded_keys_membership_and_input() {
    let metadata = collection([
        secondary(7, keys()),
        secondary(
            3,
            doc([("v", BsonValue::Int32(-1)), ("tail", BsonValue::Int32(1))]),
        ),
        membership(5, true, None),
        membership(
            11,
            false,
            Some(doc([("enabled", BsonValue::Boolean(true))])),
        ),
    ]);
    let preparation = DocumentIndexPreparation::compile(&metadata).unwrap();
    assert_eq!(preparation.collection_id(), metadata.id());
    assert!(preparation.retained_bytes() > 256);
    for input in [
        BsonDocument::new(),
        doc([("v", BsonValue::Null)]),
        doc([("v", BsonValue::Array(vec![]))]),
        doc([
            (
                "_id",
                BsonValue::ObjectId(BsonObjectId::from_bytes([5; 12])),
            ),
            (
                "v",
                BsonValue::Array(vec![
                    BsonValue::Int32(1),
                    BsonValue::Int64(1),
                    BsonValue::Double(1.0),
                    BsonValue::Decimal128(BsonDecimal128::parse("1.00").unwrap()),
                    BsonValue::Boolean(true),
                    BsonValue::Null,
                ]),
            ),
            ("tail", BsonValue::from("private-value")),
            ("enabled", BsonValue::Boolean(true)),
        ]),
    ] {
        let original = encode_document(&input).unwrap();
        let result = preparation.prepare(&input).unwrap();
        assert_eq!(result.collection_id(), metadata.id());
        assert_eq!(result.indexes().len(), 4);
        for (index, expected) in result
            .indexes()
            .iter()
            .zip(metadata.indexes().iter().skip(1))
        {
            assert_eq!(index.index_id(), expected.id());
            assert_eq!(index.is_unique(), expected.is_unique());
            let definition = expected.definition().unwrap();
            let single = DocumentIndexKeyGenerator::compile(
                definition.keys(),
                definition.sparse(),
                definition.partial_filter(),
            )
            .unwrap()
            .keys(&input)
            .unwrap();
            assert_eq!(
                index.keys(),
                single
                    .iter()
                    .map(|key| key.to_bytes().unwrap())
                    .collect::<Vec<_>>()
            );
            for bytes in index.keys() {
                assert_eq!(
                    DocumentIndexKey::from_bytes(bytes)
                        .unwrap()
                        .to_bytes()
                        .unwrap(),
                    *bytes
                );
            }
            assert!(!format!("{index:?}").contains("private"));
        }
        assert_eq!(original, encode_document(&input).unwrap());
        assert!(!format!("{preparation:?} {result:?}").contains("private"));
    }
    let empty = preparation.prepare(&BsonDocument::new()).unwrap();
    assert_eq!(empty.indexes()[0].keys().len(), 1); // Ordinary missing => null.
    assert!(empty.indexes()[2].keys().is_empty()); // Sparse missing.
    assert!(empty.indexes()[3].keys().is_empty()); // Partial exclusion.
}

#[test]
fn late_index_failure_discards_everything_and_does_not_poison_reuse() {
    let metadata = collection([
        secondary(2, keys()),
        secondary(3, doc([("bad", BsonValue::Int32(1))])),
    ]);
    let preparation = DocumentIndexPreparation::compile(&metadata).unwrap();
    let input = doc([
        ("v", BsonValue::from("private")),
        ("bad", BsonValue::Document(keys())),
    ]);
    let original = encode_document(&input).unwrap();
    assert_eq!(
        preparation.prepare(&input).unwrap_err().kind(),
        EngineErrorKind::Unsupported
    );
    assert_eq!(original, encode_document(&input).unwrap());
    let valid = doc([("v", BsonValue::Int32(1)), ("bad", BsonValue::Int32(2))]);
    let first = preparation.prepare(&valid).unwrap();
    assert_eq!(first.indexes().len(), 2);
    // A declared unique flag does not turn pure preparation into enforcement.
    assert_eq!(
        frames(&first),
        frames(&preparation.prepare(&valid).unwrap())
    );
}

#[test]
fn interruption_at_every_checkpoint_returns_only_an_error() {
    let metadata = collection([secondary(2, keys()), membership(3, true, None)]);
    let mut compile_steps = 0;
    let preparation = DocumentIndexPreparation::compile_with_check(&metadata, &mut || {
        compile_steps += 1;
        Ok(())
    })
    .unwrap();
    let input = doc([(
        "v",
        BsonValue::Array(vec![BsonValue::Int32(2), BsonValue::Int32(1)]),
    )]);
    let mut prepare_steps = 0;
    let expected = preparation
        .prepare_with_check(&input, &mut || {
            prepare_steps += 1;
            Ok(())
        })
        .unwrap();
    for (compiling, steps) in [(true, compile_steps), (false, prepare_steps)] {
        for stop in 1..=steps {
            let kind = if stop % 2 == 0 {
                EngineErrorKind::Cancelled
            } else {
                EngineErrorKind::DeadlineExceeded
            };
            let mut seen = 0;
            let mut check = || {
                seen += 1;
                if seen == stop {
                    Err(EngineError::new(kind, "interrupted"))
                } else {
                    Ok(())
                }
            };
            let error = if compiling {
                DocumentIndexPreparation::compile_with_check(&metadata, &mut check).unwrap_err()
            } else {
                preparation
                    .prepare_with_check(&input, &mut check)
                    .unwrap_err()
            };
            assert_eq!(error.kind(), kind);
            assert_eq!(seen, stop);
        }
    }
    assert_eq!(
        frames(&expected),
        frames(&preparation.prepare(&input).unwrap())
    );
}

#[test]
fn unknown_envelopes_and_invalid_later_definitions_fail_closed_without_rewrite() {
    for specification in [
        doc([
            ("v", BsonValue::Int32(99)),
            ("private", BsonValue::from("opaque")),
        ]),
        doc([("bad..path", BsonValue::Int32(1))]),
    ] {
        let metadata = collection([secondary(2, keys()), secondary(3, specification)]);
        let before = metadata.clone();
        let error = DocumentIndexPreparation::compile(&metadata).unwrap_err();
        assert!(matches!(
            error.kind(),
            EngineErrorKind::Unsupported | EngineErrorKind::InvalidArgument
        ));
        assert!(!error.to_string().contains("private"));
        assert_eq!(metadata, before);
    }
}

#[test]
fn index_count_is_bounded_but_the_builtin_is_not_prepared() {
    let metadata =
        collection((0..MAX_DOCUMENT_PREPARED_INDEXES).map(|n| secondary(n as u64 + 2, keys())));
    let preparation = DocumentIndexPreparation::compile(&metadata).unwrap();
    assert_eq!(
        preparation.prepare(&keys()).unwrap().indexes().len(),
        MAX_DOCUMENT_PREPARED_INDEXES
    );
    let too_many =
        collection((0..=MAX_DOCUMENT_PREPARED_INDEXES).map(|n| secondary(n as u64 + 2, keys())));
    assert_eq!(
        DocumentIndexPreparation::compile(&too_many)
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    let none = DocumentIndexPreparation::compile(&collection([])).unwrap();
    assert!(none.prepare(&keys()).unwrap().indexes().is_empty());
    // Even no-op preparation must not accept structurally invalid BSON.
    let duplicates = doc([("v", BsonValue::Int32(1)), ("v", BsonValue::Int32(2))]);
    assert_eq!(
        none.prepare(&duplicates).unwrap_err().kind(),
        EngineErrorKind::InvalidArgument
    );
    let mut nested = BsonValue::Null;
    for _ in 0..102 {
        nested = BsonValue::Array(vec![nested]);
    }
    assert!(none.prepare(&doc([("v", nested)])).is_err());
}

#[test]
fn total_multikey_limit_is_not_reset_for_each_index() {
    let preparation = DocumentIndexPreparation::compile(&collection([
        secondary(2, keys()),
        secondary(3, keys()),
    ]))
    .unwrap();
    let input = doc([(
        "v",
        BsonValue::Array((0..8192).map(BsonValue::Int32).collect()),
    )]);
    let result = preparation.prepare(&input).unwrap();
    assert_eq!(
        result
            .indexes()
            .iter()
            .map(|index| index.keys().len())
            .sum::<usize>(),
        16_384
    );
    let input = doc([(
        "v",
        BsonValue::Array((0..8193).map(BsonValue::Int32).collect()),
    )]);
    assert_eq!(
        DocumentIndexKeyGenerator::compile(&keys(), false, None)
            .unwrap()
            .keys(&input)
            .unwrap()
            .len(),
        8193
    );
    assert_eq!(
        preparation.prepare(&input).unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
}

#[test]
fn encoded_outputs_and_repeated_scalar_work_share_one_byte_budget() {
    let input = doc([("v", BsonValue::from("x".repeat(4 * 1024 * 1024)))]);
    let single = DocumentIndexKeyGenerator::compile(&keys(), false, None).unwrap();
    assert_eq!(single.keys(&input).unwrap().len(), 1);
    let one = DocumentIndexPreparation::compile(&collection([secondary(2, keys())])).unwrap();
    assert_eq!(one.prepare(&input).unwrap().indexes().len(), 1);
    let many =
        DocumentIndexPreparation::compile(&collection((2..8).map(|id| secondary(id, keys()))))
            .unwrap();
    assert_eq!(
        many.prepare(&input).unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
    assert!(many.prepare(&BsonDocument::new()).is_ok());
}

#[test]
fn unproductive_path_searches_share_one_work_budget() {
    let input =
        BsonDocument::from_entries((0..10_000).map(|n| (format!("field{n}"), BsonValue::Null)))
            .unwrap();
    let paths =
        BsonDocument::from_entries((0..32).map(|n| (format!("missing{n}"), BsonValue::Int32(1))))
            .unwrap();
    let one = DocumentIndexKeyGenerator::compile(&paths, false, None).unwrap();
    assert_eq!(one.keys(&input).unwrap().len(), 1);
    let many = DocumentIndexPreparation::compile(&collection(
        (2..6).map(|id| secondary(id, paths.clone())),
    ))
    .unwrap();
    assert_eq!(
        many.prepare(&input).unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
    assert!(many.prepare(&BsonDocument::new()).is_ok());
}

#[test]
fn shared_budget_accepts_exact_bounds_and_rejects_one_over_and_overflow() {
    use super::super::{MAX_KEYS, MAX_STEPS};
    let mut check = || Ok(());
    let mut budget = Budget::new(&mut check);
    budget.charge(MAX_WORK_BYTES).unwrap();
    assert_eq!(
        budget.charge(1).unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
    budget.bytes = 1;
    assert_eq!(
        budget.charge(usize::MAX).unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
    budget.keys(MAX_KEYS).unwrap();
    assert_eq!(
        budget.keys(1).unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
    budget.keys = 1;
    assert_eq!(
        budget.keys(usize::MAX).unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
    budget.steps = MAX_STEPS - 1;
    budget.step().unwrap();
    assert_eq!(
        budget.step().unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
}

#[test]
fn retained_partial_programs_share_one_compilation_budget() {
    let filter = doc([("enabled", BsonValue::from("x".repeat(900_000)))]);
    let single = collection([membership(2, false, Some(filter.clone()))]);
    assert!(DocumentIndexPreparation::compile(&single).is_ok());
    let many = collection((2..7).map(|id| membership(id, false, Some(filter.clone()))));
    let original = many.clone();
    assert_eq!(
        DocumentIndexPreparation::compile(&many).unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
    assert_eq!(many, original);
    assert!(DocumentIndexPreparation::compile(&single).is_ok());
}
