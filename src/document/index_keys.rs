//! Bounded equality entries for the frozen ordinary secondary-index subset.
//! No catalog activation, storage format, planner authority or uniqueness is
//! implied by generating a key.

use std::{collections::HashSet, fmt, sync::Arc};

use super::{
    BsonCodecOptions, BsonDocument, BsonErrorContext, BsonValue, CanonicalBsonKey, DocumentMatcher,
    encode_document, encode_document_with_options, normalize_index_definition,
    number::CanonicalNumber,
};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

const MAX_SPEC_BYTES: usize = 1024 * 1024;
const MAX_PARTIAL_NODES: usize = 4096;
const MAX_DEPTH: usize = 100;
const MAX_KEYS: usize = 16_384;
const MAX_STEPS: usize = 1_000_000;
const MAX_VALUE_BYTES: usize = 8 * 1024 * 1024;
const MAX_WORK_BYTES: usize = 64 * 1024 * 1024;
static NULL: BsonValue = BsonValue::Null;

/// Compiled ordinary secondary-index paths and optional membership predicate.
///
/// Final arrays yield distinct immediate scalar entries, preserving encounter
/// order. At most one compound field may be an array. Missing and null have the
/// same identity; an empty array has a separate identity. Sparse compound keys
/// include a document if any field exists. Partial predicates run before key
/// extraction; sparse and partial cannot be combined.
///
/// This is the frozen TinyMongo subset, not all MongoDB indexing: intermediate
/// arrays, object/nested-array values, ObjectId/date values and nonfinite numbers
/// are rejected. Code-with-scope retains its complete BSON identity. Directions
/// are validated but do not affect equality. These keys are not ordered keys.
pub struct DocumentIndexKeyGenerator {
    paths: Vec<Vec<String>>,
    sparse: bool,
    partial: Option<DocumentMatcher>,
    retained_bytes: usize,
}

impl fmt::Debug for DocumentIndexKeyGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentIndexKeyGenerator")
            .finish_non_exhaustive()
    }
}

/// Opaque equality/hash identity of an ordered tuple, independent of document
/// representation. Compare only within one index; callers must supply index and
/// collection identity. No persistent byte representation is promised yet.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DocumentIndexKey {
    components: Vec<Arc<Component>>,
}

#[derive(PartialEq, Eq, Hash)]
enum Component {
    EmptyArray,
    Value(CanonicalBsonKey),
}

impl fmt::Debug for DocumentIndexKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentIndexKey").finish_non_exhaustive()
    }
}

impl DocumentIndexKeyGenerator {
    pub fn compile(
        keys: &BsonDocument,
        sparse: bool,
        partial: Option<&BsonDocument>,
    ) -> EngineResult<Self> {
        Self::compile_with_check(keys, sparse, partial, &mut || Ok(()))
    }

    pub fn compile_with_check(
        keys: &BsonDocument,
        sparse: bool,
        partial: Option<&BsonDocument>,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Self> {
        // A fixed internal name avoids imposing a generated-name bound on keys
        // whose declaration already has a valid explicit name.
        let (keys, _) = normalize_index_definition(keys, Some("index_keys"), check)?;
        if sparse && partial.is_some() {
            return Err(unsupported());
        }
        let partial = partial
            .map(|filter| {
                encode_document_with_options(
                    filter,
                    &BsonCodecOptions::new().with_max_document_bytes(MAX_SPEC_BYTES),
                )
                .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
                validate_partial(filter, 0, &mut 0, check)?;
                DocumentMatcher::compile_with_check(filter, check)
            })
            .transpose()?;
        let mut paths = Vec::new();
        let mut retained_bytes = 512 + partial.as_ref().map_or(0, DocumentMatcher::retained_bytes);
        for (field, _) in keys.iter() {
            let mut parts = Vec::new();
            for part in field.split('.') {
                check()?;
                parts.push(part.to_owned());
                retained_bytes += part.len() + 128;
            }
            paths.push(parts);
        }
        check()?;
        Ok(Self {
            paths,
            sparse,
            partial,
            retained_bytes,
        })
    }

    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub fn keys(&self, document: &BsonDocument) -> EngineResult<Vec<DocumentIndexKey>> {
        self.keys_with_check(document, &mut || Ok(()))
    }

    /// Validate caller-owned BSON and return all entries or an error, never a
    /// successful partial set. Input is not mutated; a failed call does not
    /// poison this immutable compiler. Work is independently bounded per call.
    pub fn keys_with_check(
        &self,
        document: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Vec<DocumentIndexKey>> {
        check()?;
        encode_document(document)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        let mut budget = Budget {
            steps: 0,
            bytes: 0,
            check,
        };
        budget.step()?;
        if let Some(partial) = &self.partial {
            if !partial.matches_with_check(document, &mut || budget.step())? {
                budget.step()?;
                return Ok(Vec::new());
            }
        }
        let mut selected = Vec::new();
        let mut arrays = 0;
        for path in &self.paths {
            let value = nested_value(document, path, &mut budget)?;
            arrays += usize::from(matches!(value, Some(BsonValue::Array(_))));
            selected.push(value);
        }
        if self.sparse && selected.iter().all(Option::is_none) {
            budget.step()?;
            return Ok(Vec::new());
        }
        if arrays > 1 {
            return Err(unsupported());
        }
        let mut components = Vec::new();
        let mut key_count: usize = 1;
        for value in selected {
            let values = component_keys(value, &mut budget)?;
            // At most one component has more than one key. Keep the explicit
            // product check so future traversal changes cannot bypass bounds.
            key_count = key_count
                .checked_mul(values.len())
                .filter(|n| *n <= MAX_KEYS)
                .ok_or_else(limit)?;
            components.push(values);
        }
        let mut output = Vec::new();
        // Arc sharing bounds allocations, but consumers must eventually hash or
        // persist every full tuple. Charge expanded key bytes as well: a large
        // scalar paired with a multikey array must not amplify into gigabytes.
        for values in &components {
            let mut bytes = 0_usize;
            for value in values {
                budget.step()?;
                let size = match value.as_ref() {
                    Component::EmptyArray => 1,
                    Component::Value(key) => key.as_bytes().len() + 8,
                };
                bytes = bytes.checked_add(size).ok_or_else(limit)?;
            }
            if values.len() == 1 {
                bytes = bytes.checked_mul(key_count).ok_or_else(limit)?;
            }
            budget.charge(bytes)?;
        }
        // Charge tuple vectors and Arc cells before allocating. Scalar key
        // bytes are shared across tuples and already charged by component_keys.
        budget.charge(key_count * (128 + self.paths.len() * 32))?;
        output.try_reserve_exact(key_count).map_err(allocation)?;
        for index in 0..key_count {
            budget.step()?;
            let mut tuple = Vec::new();
            tuple
                .try_reserve_exact(components.len())
                .map_err(allocation)?;
            for values in &components {
                budget.step()?;
                tuple.push(Arc::clone(
                    &values[if values.len() == 1 { 0 } else { index }],
                ));
            }
            output.push(DocumentIndexKey { components: tuple });
        }
        budget.step()?;
        Ok(output)
    }
}

fn nested_value<'a>(
    document: &'a BsonDocument,
    path: &[String],
    budget: &mut Budget<'_>,
) -> EngineResult<Option<&'a BsonValue>> {
    let mut current = document;
    for (index, part) in path.iter().enumerate() {
        budget.step()?;
        let mut found = None;
        for (name, value) in current.iter() {
            budget.step()?;
            if name == part {
                found = Some(value);
                break;
            }
        }
        if index + 1 == path.len() {
            return Ok(found);
        }
        match found {
            Some(BsonValue::Document(next)) => current = next,
            Some(BsonValue::Array(_)) => return Err(unsupported()),
            _ => return Ok(None),
        }
    }
    unreachable!("validated paths are nonempty")
}

fn component_keys(
    value: Option<&BsonValue>,
    budget: &mut Budget<'_>,
) -> EngineResult<Vec<Arc<Component>>> {
    budget.step()?;
    if matches!(value, Some(BsonValue::Array(values)) if values.is_empty()) {
        budget.charge(256)?;
        return Ok(vec![Arc::new(Component::EmptyArray)]);
    }
    let values = match value {
        Some(BsonValue::Array(values)) => values.as_slice(),
        value => std::slice::from_ref(value.unwrap_or(&NULL)),
    };
    let mut seen = HashSet::new();
    let mut output = Vec::new();
    for value in values {
        budget.step()?;
        if matches!(
            value,
            BsonValue::Document(_)
                | BsonValue::Array(_)
                | BsonValue::ObjectId(_)
                | BsonValue::DateTime(_)
        ) || matches!(value.canonical_number(), Some(number) if !matches!(number, CanonicalNumber::Finite(_)))
        {
            return Err(unsupported());
        }
        // Includes nested code scopes. Preflight bounded work before encoding
        // and hashing; duplicates also consume work rather than evading quotas.
        let charge = super::memory::value_bytes(value, MAX_VALUE_BYTES, &mut || budget.step())?;
        budget.charge(charge + 256)?;
        let key = Component::Value(
            CanonicalBsonKey::encode(value)
                .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?,
        );
        budget.step()?;
        if seen.contains(&key) {
            continue;
        }
        if output.len() >= MAX_KEYS {
            return Err(limit());
        }
        seen.try_reserve(1).map_err(allocation)?;
        output.try_reserve(1).map_err(allocation)?;
        let key = Arc::new(key);
        seen.insert(Arc::clone(&key));
        output.push(key);
    }
    budget.step()?;
    Ok(output)
}

fn validate_partial(
    filter: &BsonDocument,
    depth: usize,
    nodes: &mut usize,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    check()?;
    if depth > MAX_DEPTH {
        return Err(limit());
    }
    if filter.is_empty() {
        return Err(unsupported());
    }
    for (field, condition) in filter.iter() {
        check()?;
        *nodes += 1;
        if *nodes > MAX_PARTIAL_NODES {
            return Err(limit());
        }
        if matches!(field, "$and" | "$or") {
            let BsonValue::Array(children) = condition else {
                return Err(unsupported());
            };
            if children.is_empty() {
                return Err(unsupported());
            }
            for child in children {
                let BsonValue::Document(child) = child else {
                    return Err(unsupported());
                };
                validate_partial(child, depth + 1, nodes, check)?;
            }
            continue;
        }
        for part in field.split('.') {
            check()?;
            if part.is_empty() || part.starts_with('$') || part.contains('\0') {
                return Err(unsupported());
            }
        }
        let BsonValue::Document(operators) = condition else {
            continue;
        };
        if !operators.iter().any(|(key, _)| key.starts_with('$')) {
            continue;
        }
        for (operator, value) in operators.iter() {
            check()?;
            *nodes += 1;
            if *nodes > MAX_PARTIAL_NODES {
                return Err(limit());
            }
            match operator {
                "$eq" | "$gt" | "$gte" | "$lt" | "$lte" | "$type" => {}
                "$in" if matches!(value, BsonValue::Array(_)) => {}
                "$exists" if matches!(value, BsonValue::Boolean(true)) => {}
                _ => return Err(unsupported()),
            }
        }
    }
    Ok(())
}

struct Budget<'a> {
    steps: usize,
    bytes: usize,
    check: &'a mut dyn FnMut() -> EngineResult<()>,
}

impl Budget<'_> {
    fn step(&mut self) -> EngineResult<()> {
        (self.check)()?;
        self.steps += 1;
        if self.steps > MAX_STEPS {
            return Err(limit());
        }
        Ok(())
    }

    fn charge(&mut self, amount: usize) -> EngineResult<()> {
        self.bytes = self
            .bytes
            .checked_add(amount)
            .filter(|n| *n <= MAX_WORK_BYTES)
            .ok_or_else(limit)?;
        Ok(())
    }
}

fn limit() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "document index key resource limit exceeded",
    )
}

fn unsupported() -> EngineError {
    EngineError::new(
        EngineErrorKind::Unsupported,
        "unsupported document index key or membership expression",
    )
}

fn allocation(_: std::collections::TryReserveError) -> EngineError {
    limit()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{BsonBinary, BsonDecimal128, BsonJavaScript};

    fn doc(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
        BsonDocument::from_entries(entries).unwrap()
    }

    fn value(value: BsonValue) -> BsonDocument {
        doc([("v", value)])
    }

    fn generator() -> DocumentIndexKeyGenerator {
        DocumentIndexKeyGenerator::compile(&value(BsonValue::Int32(1)), false, None).unwrap()
    }

    #[test]
    fn numeric_cohorts_null_empty_and_typed_values_have_exact_equality() {
        let generator = generator();
        let one = generator.keys(&value(BsonValue::Int32(1))).unwrap();
        for number in [
            BsonValue::Int64(1),
            BsonValue::Double(1.0),
            BsonValue::Decimal128(BsonDecimal128::parse("1.00").unwrap()),
        ] {
            assert_eq!(one, generator.keys(&value(number)).unwrap());
        }
        assert_ne!(
            one,
            generator.keys(&value(BsonValue::Boolean(true))).unwrap()
        );
        assert_eq!(
            generator.keys(&BsonDocument::new()).unwrap(),
            generator.keys(&value(BsonValue::Null)).unwrap()
        );
        assert_ne!(
            generator.keys(&value(BsonValue::Array(vec![]))).unwrap(),
            generator.keys(&value(BsonValue::Null)).unwrap()
        );
        assert_ne!(
            generator.keys(&value(BsonValue::Double(0.1))).unwrap(),
            generator
                .keys(&value(BsonValue::Decimal128(
                    BsonDecimal128::parse("0.1").unwrap()
                )))
                .unwrap()
        );
        let input = value(BsonValue::Array(vec![
            BsonValue::Int32(1),
            BsonValue::Double(1.0),
            BsonValue::Boolean(true),
            BsonValue::Null,
        ]));
        let before = encode_document(&input).unwrap();
        let keys = generator.keys(&input).unwrap();
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], one[0]);
        assert_eq!(before, encode_document(&input).unwrap());
        let text = generator.keys(&value(BsonValue::from("private"))).unwrap();
        let code = generator
            .keys(&value(BsonValue::JavaScript(BsonJavaScript::new(
                "private",
            ))))
            .unwrap();
        assert_ne!(text, code);
        assert_ne!(
            generator
                .keys(&value(BsonValue::Binary(BsonBinary::new(0, b"x"))))
                .unwrap(),
            generator
                .keys(&value(BsonValue::Binary(BsonBinary::new(128, b"x"))))
                .unwrap()
        );
    }

    #[test]
    fn compound_order_single_array_and_literal_numeric_paths_are_preserved() {
        let keys = doc([("a.0", BsonValue::Int32(-1)), ("b", BsonValue::Int32(1))]);
        let compiled = DocumentIndexKeyGenerator::compile(&keys, false, None).unwrap();
        let input = doc([
            (
                "a",
                BsonValue::Document(doc([(
                    "0",
                    BsonValue::Array(vec![
                        BsonValue::Int32(3),
                        BsonValue::Int32(1),
                        BsonValue::Int64(3),
                    ]),
                )])),
            ),
            ("b", BsonValue::from("tail")),
        ]);
        let actual = compiled.keys(&input).unwrap();
        assert_eq!(actual.len(), 2);
        for (index, number) in [3, 1].into_iter().enumerate() {
            let scalar = doc([
                (
                    "a",
                    BsonValue::Document(doc([("0", BsonValue::Int32(number))])),
                ),
                ("b", BsonValue::from("tail")),
            ]);
            assert_eq!(actual[index], compiled.keys(&scalar).unwrap()[0]);
        }
        let swapped = DocumentIndexKeyGenerator::compile(
            &doc([("b", BsonValue::Int32(1)), ("a.0", BsonValue::Int32(1))]),
            false,
            None,
        )
        .unwrap();
        assert_ne!(actual, swapped.keys(&input).unwrap());
        assert_eq!(
            compiled
                .keys(&doc([("a", BsonValue::Array(vec![]))]))
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
        let parallel = doc([
            (
                "a",
                BsonValue::Document(doc([("0", BsonValue::Array(vec![]))])),
            ),
            ("b", BsonValue::Array(vec![])),
        ]);
        assert_eq!(
            compiled.keys(&parallel).unwrap_err().kind(),
            EngineErrorKind::Unsupported
        );
        // Public compilation/extraction retains the bounded codec's duplicate
        // rejection; ambiguous caller-owned BSON is not silently indexed.
        let duplicate = doc([("v", BsonValue::Int32(1)), ("v", BsonValue::Int32(2))]);
        assert_eq!(
            generator().keys(&duplicate).unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
    }

    #[test]
    fn sparse_and_partial_membership_precede_key_extraction() {
        let keys = doc([("v", BsonValue::Int32(1)), ("w", BsonValue::Int32(1))]);
        let sparse = DocumentIndexKeyGenerator::compile(&keys, true, None).unwrap();
        assert!(sparse.keys(&BsonDocument::new()).unwrap().is_empty());
        assert_eq!(sparse.keys(&value(BsonValue::Null)).unwrap().len(), 1);
        assert_eq!(
            sparse
                .keys(&doc([("w", BsonValue::Array(vec![]))]))
                .unwrap()
                .len(),
            1
        );
        let filter = doc([("enabled", BsonValue::Boolean(true))]);
        let partial = DocumentIndexKeyGenerator::compile(&keys, false, Some(&filter)).unwrap();
        let invalid_key = value(BsonValue::Array(vec![BsonValue::Array(vec![])]));
        assert!(partial.keys(&invalid_key).unwrap().is_empty());
        let mut member = invalid_key;
        member.push("enabled", BsonValue::Boolean(true)).unwrap();
        assert_eq!(
            partial.keys(&member).unwrap_err().kind(),
            EngineErrorKind::Unsupported
        );
        assert!(DocumentIndexKeyGenerator::compile(&keys, true, Some(&filter)).is_err());
        assert!(
            DocumentIndexKeyGenerator::compile(&keys, false, Some(&BsonDocument::new())).is_err()
        );
        let invalid_branch = doc([(
            "$or",
            BsonValue::Array(vec![
                BsonValue::Document(filter),
                BsonValue::Document(value(BsonValue::Document(doc([(
                    "$ne",
                    BsonValue::Int32(1),
                )])))),
            ]),
        )]);
        assert!(DocumentIndexKeyGenerator::compile(&keys, false, Some(&invalid_branch)).is_err());
    }

    #[test]
    fn unsupported_values_and_diagnostics_do_not_leak_payloads() {
        let generator = generator();
        for item in [
            BsonValue::Document(doc([("private", BsonValue::Int32(1))])),
            BsonValue::Array(vec![BsonValue::Array(vec![])]),
            BsonValue::Double(f64::NAN),
            BsonValue::Double(f64::INFINITY),
            BsonValue::Decimal128(BsonDecimal128::parse("sNaN").unwrap()),
        ] {
            let error = generator.keys(&value(item)).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Unsupported);
            assert!(!format!("{error:?}").contains("private"));
        }
        let keys = generator.keys(&value(BsonValue::from("private"))).unwrap();
        assert!(!format!("{keys:?}").contains("private"));
        assert!(!format!("{generator:?}").contains("private"));
        let scopes = [
            doc([("x", BsonValue::Int32(1)), ("y", BsonValue::Int32(2))]),
            doc([("x", BsonValue::Double(1.0)), ("y", BsonValue::Int64(2))]),
            doc([("y", BsonValue::Int32(2)), ("x", BsonValue::Int32(1))]),
        ];
        let keys: Vec<_> = scopes
            .into_iter()
            .map(|scope| {
                generator
                    .keys(&value(BsonValue::JavaScript(BsonJavaScript::with_scope(
                        "private", scope,
                    ))))
                    .unwrap()
            })
            .collect();
        assert_eq!(keys[0], keys[1]);
        assert_ne!(keys[0], keys[2]);
    }

    #[test]
    fn key_value_total_work_and_traversal_limits_are_bounded() {
        let generator = generator();
        let input = value(BsonValue::Array(
            (0..MAX_KEYS).map(|n| BsonValue::Int32(n as i32)).collect(),
        ));
        assert_eq!(generator.keys(&input).unwrap().len(), MAX_KEYS);
        let too_many = value(BsonValue::Array(
            (0..=MAX_KEYS).map(|n| BsonValue::Int32(n as i32)).collect(),
        ));
        assert_eq!(
            generator.keys(&too_many).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let oversized = value(BsonValue::String("x".repeat(MAX_VALUE_BYTES)));
        assert_eq!(
            generator.keys(&oversized).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        // Repeated values still consume work; deduplication cannot evade quotas.
        let duplicates = value(BsonValue::Array(vec![BsonValue::Int32(0); 200_000]));
        assert_eq!(
            generator.keys(&duplicates).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let keys = BsonDocument::from_entries(
            (0..32).map(|n| (format!("missing{n}"), BsonValue::Int32(1))),
        )
        .unwrap();
        let wide = BsonDocument::from_entries(
            (0..40_000).map(|n| (format!("unrelated{n}"), BsonValue::Null)),
        )
        .unwrap();
        assert_eq!(
            DocumentIndexKeyGenerator::compile(&keys, false, None)
                .unwrap()
                .keys(&wide)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        assert_eq!(
            generator.keys(&value(BsonValue::Int32(1))).unwrap().len(),
            1
        );
    }

    #[test]
    fn cancellation_at_every_checkpoint_returns_no_partial_keys() {
        let keys = doc([("v", BsonValue::Int32(1)), ("w", BsonValue::Int32(1))]);
        let filter = doc([("enabled", BsonValue::Boolean(true))]);
        let mut compile_checks = 0;
        let compiled =
            DocumentIndexKeyGenerator::compile_with_check(&keys, false, Some(&filter), &mut || {
                compile_checks += 1;
                Ok(())
            })
            .unwrap();
        let cancel = || EngineError::new(EngineErrorKind::Cancelled, "cancelled");
        for stop in 1..=compile_checks {
            let mut calls = 0;
            assert_eq!(
                DocumentIndexKeyGenerator::compile_with_check(
                    &keys,
                    false,
                    Some(&filter),
                    &mut || {
                        calls += 1;
                        if calls == stop { Err(cancel()) } else { Ok(()) }
                    }
                )
                .unwrap_err()
                .kind(),
                EngineErrorKind::Cancelled
            );
        }
        let input = doc([
            (
                "v",
                BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(2)]),
            ),
            ("enabled", BsonValue::Boolean(true)),
        ]);
        let before = encode_document(&input).unwrap();
        let mut checks = 0;
        let expected = compiled
            .keys_with_check(&input, &mut || {
                checks += 1;
                Ok(())
            })
            .unwrap();
        for stop in 1..=checks {
            let mut calls = 0;
            assert_eq!(
                compiled
                    .keys_with_check(&input, &mut || {
                        calls += 1;
                        if calls == stop { Err(cancel()) } else { Ok(()) }
                    })
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::Cancelled
            );
        }
        assert_eq!(expected, compiled.keys(&input).unwrap());
        assert_eq!(before, encode_document(&input).unwrap());
    }

    #[test]
    fn partial_predicates_have_eager_size_node_and_path_bounds() {
        let keys = value(BsonValue::Int32(1));
        let oversized = value(BsonValue::String("x".repeat(MAX_SPEC_BYTES)));
        assert_eq!(
            DocumentIndexKeyGenerator::compile(&keys, false, Some(&oversized))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        let too_many = doc([(
            "$or",
            BsonValue::Array(
                (0..MAX_PARTIAL_NODES)
                    .map(|_| BsonValue::Document(value(BsonValue::Int32(1))))
                    .collect(),
            ),
        )]);
        assert_eq!(
            DocumentIndexKeyGenerator::compile(&keys, false, Some(&too_many))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        let deep =
            BsonDocument::from_entries([("v.".repeat(101) + "v", BsonValue::Int32(1))]).unwrap();
        assert_eq!(
            DocumentIndexKeyGenerator::compile(&keys, false, Some(&deep))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        let invalid = value(BsonValue::Document(doc([(
            "$type",
            BsonValue::from("private-invalid-type"),
        )])));
        let error = DocumentIndexKeyGenerator::compile(&keys, false, Some(&invalid)).unwrap_err();
        assert!(!format!("{error:?}").contains("private-invalid-type"));
    }

    #[test]
    fn compound_fanout_is_bounded_even_when_scalar_allocations_are_shared() {
        let keys = doc([("v", BsonValue::Int32(1)), ("w", BsonValue::Int32(1))]);
        let compiled = DocumentIndexKeyGenerator::compile(&keys, false, None).unwrap();
        let make_input = |count| {
            doc([
                ("v", BsonValue::String("private".repeat(10_000))),
                (
                    "w",
                    BsonValue::Array((0..count).map(BsonValue::Int32).collect()),
                ),
            ])
        };
        assert_eq!(compiled.keys(&make_input(4)).unwrap().len(), 4);
        let input = make_input(1024);
        assert!(encode_document(&input).unwrap().len() < 100_000);
        assert_eq!(
            compiled.keys(&input).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        assert_eq!(compiled.keys(&make_input(4)).unwrap().len(), 4);
    }
}
