//! Shared BSON sort keys. No storage, cursor, or aggregation adapter defines
//! its own value ordering or array-path semantics.

use std::{cmp::Ordering, collections::BTreeMap, fmt};

use super::{
    BsonCodecOptions, BsonDocument, BsonErrorContext, BsonValue, encode_document,
    encode_document_with_options, matcher::query_error,
};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

const MAX_SPEC_BYTES: usize = 1024 * 1024;
const MAX_FIELDS: usize = 32;
const MAX_DEPTH: usize = 100;
const MAX_CANDIDATES: usize = 16_384;
const MAX_STEPS: usize = 1_000_000;
const MAX_WORK_BYTES: usize = 64 * 1024 * 1024;
const MAX_KEY_BYTES: usize = 8 * 1024 * 1024;
static NULL: BsonValue = BsonValue::Null;

struct Field {
    parts: Vec<String>,
    // Interned path prefixes avoid copying long names into array provenance.
    prefixes: Vec<usize>,
    descending: bool,
}

/// A bounded, validated ordinary BSON sort specification. Numeric directions
/// must be exactly 1 or -1; metadata/expression sorting is not supported.
pub struct DocumentSorter {
    fields: Vec<Field>,
    parents: Vec<usize>,
    retained_bytes: usize,
}

impl fmt::Debug for DocumentSorter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentSorter").finish_non_exhaustive()
    }
}

/// An owned BSON ordering key. Keys from the same sorter can be compared
/// directly; callers add their own stable natural-order tie-breaker. Debug
/// output never contains document values. Equality uses BSON semantics, not
/// representation identity (for example, Int32(1) and Int64(1) tie).
#[derive(Clone)]
pub struct DocumentSortKey {
    components: Vec<(bool, Atom)>,
    retained_bytes: usize,
}

impl fmt::Debug for DocumentSortKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentSortKey").finish_non_exhaustive()
    }
}

#[derive(Clone)]
enum Atom {
    EmptyArray,
    Value(BsonValue),
}

#[derive(Clone, Copy)]
enum AtomRef<'a> {
    EmptyArray,
    Value(&'a BsonValue),
}

impl Atom {
    fn borrowed(&self) -> AtomRef<'_> {
        match self {
            Self::EmptyArray => AtomRef::EmptyArray,
            Self::Value(value) => AtomRef::Value(value),
        }
    }
}

fn compare_atom(left: AtomRef<'_>, right: AtomRef<'_>) -> Ordering {
    match (left, right) {
        (AtomRef::EmptyArray, AtomRef::EmptyArray) => Ordering::Equal,
        (AtomRef::EmptyArray, AtomRef::Value(BsonValue::MinKey)) => Ordering::Greater,
        (AtomRef::EmptyArray, AtomRef::Value(_)) => Ordering::Less,
        (AtomRef::Value(_), AtomRef::EmptyArray) => compare_atom(right, left).reverse(),
        (AtomRef::Value(left), AtomRef::Value(right)) => left.cmp(right),
    }
}

impl PartialEq for DocumentSortKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for DocumentSortKey {}
impl PartialOrd for DocumentSortKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for DocumentSortKey {
    fn cmp(&self, other: &Self) -> Ordering {
        for ((descending, left), (other_direction, right)) in
            self.components.iter().zip(&other.components)
        {
            // Define a total order even for keys from different specifications.
            let direction = descending.cmp(other_direction);
            if direction != Ordering::Equal {
                return direction;
            }
            let order = compare_atom(left.borrowed(), right.borrowed());
            if order != Ordering::Equal {
                return if *descending { order.reverse() } else { order };
            }
        }
        self.components.len().cmp(&other.components.len())
    }
}

impl DocumentSortKey {
    /// Conservative owned-heap charge, including cloned BSON values.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

impl DocumentSorter {
    pub fn compile(spec: &BsonDocument) -> EngineResult<Self> {
        Self::compile_with_check(spec, &mut || Ok(()))
    }

    pub fn compile_with_check(
        spec: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Self> {
        check()?;
        let bytes = encode_document_with_options(
            spec,
            &BsonCodecOptions::new().with_max_document_bytes(MAX_SPEC_BYTES),
        )
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        if spec.is_empty() {
            return Err(query_error(15976));
        }
        if spec.len() > MAX_FIELDS {
            return Err(query_error(13103));
        }
        let mut fields = Vec::new();
        let mut parents = vec![0];
        let mut prefixes = BTreeMap::new();
        for (name, direction) in spec.iter() {
            check()?;
            if name.is_empty() {
                return Err(query_error(40352));
            }
            if name.starts_with('$') {
                return Err(query_error(16410));
            }
            if name.ends_with('.') {
                return Err(query_error(40353));
            }
            if name.split('.').any(str::is_empty) {
                return Err(query_error(15998));
            }
            if name.split('.').count() > MAX_DEPTH {
                return Err(limit());
            }
            let parts: Vec<_> = name.split('.').map(str::to_owned).collect();
            let mut path = vec![0];
            for part in &parts {
                check()?;
                if part.starts_with('$')
                    && !matches!(
                        part.as_str(),
                        "$db"
                            | "$id"
                            | "$recordId"
                            | "$ref"
                            | "$searchRootDocumentId"
                            | "$searchScore"
                            | "$searchSortValues"
                            | "$sortKey"
                    )
                {
                    return Err(query_error(16410));
                }
                let parent = *path.last().expect("root prefix");
                let id = *prefixes.entry((parent, part.clone())).or_insert_with(|| {
                    let id = parents.len();
                    parents.push(parent);
                    id
                });
                path.push(id);
            }
            if matches!(direction, BsonValue::Document(doc) if doc.get_first("$meta").is_some()) {
                return Err(query_error(115));
            }
            if direction.canonical_number().is_none() {
                return Err(query_error(15974));
            }
            let descending = if direction == &BsonValue::Int32(1) {
                false
            } else if direction == &BsonValue::Int32(-1) {
                true
            } else {
                return Err(query_error(15975));
            };
            fields.push(Field {
                parts,
                prefixes: path,
                descending,
            });
        }
        check()?;
        Ok(Self {
            fields,
            retained_bytes: bytes
                .len()
                .saturating_mul(4)
                .saturating_add(parents.len() * 128),
            parents,
        })
    }

    /// Validate caller-owned BSON and derive its key without modifying it.
    pub fn key(&self, document: &BsonDocument) -> EngineResult<DocumentSortKey> {
        self.key_with_check(document, &mut || Ok(()))
    }

    pub fn key_with_check(
        &self,
        document: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<DocumentSortKey> {
        check()?;
        encode_document(document)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        self.key_validated_with_check(document, check)
    }

    /// Conservative charge for retaining the compiled specification.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub(crate) fn key_validated_with_check(
        &self,
        document: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<DocumentSortKey> {
        let mut budget = Budget {
            steps: 0,
            bytes: 0,
            candidates: 0,
            check,
        };
        let mut lookups = Vec::new();
        for field in &self.fields {
            let mut candidates = Vec::new();
            walk_document(
                document,
                0,
                field,
                &mut Vec::new(),
                None,
                &mut candidates,
                &mut budget,
            )?;
            lookups.push(Lookup::new(candidates, &mut budget)?);
        }
        let mut selected = Vec::new();
        let mut best = None;
        let mut ambiguous = false;
        join(
            self,
            &lookups,
            0,
            &[],
            None,
            &mut selected,
            &mut best,
            &mut ambiguous,
            &mut budget,
        )?;
        if ambiguous {
            return Err(query_error(16746));
        }
        let best = best.ok_or_else(|| {
            EngineError::new(EngineErrorKind::Internal, "missing document sort key")
        })?;
        let mut bytes = 128;
        // Check the entire allocation before cloning any values.
        for key in &best {
            bytes += match key {
                AtomRef::EmptyArray => 128,
                AtomRef::Value(value) => value_bytes(value, &mut budget)?,
            };
            if bytes > MAX_KEY_BYTES {
                return Err(limit());
            }
        }
        budget.step()?;
        let components = self
            .fields
            .iter()
            .zip(best)
            .map(|(field, value)| {
                (
                    field.descending,
                    match value {
                        AtomRef::EmptyArray => Atom::EmptyArray,
                        AtomRef::Value(value) => Atom::Value(value.clone()),
                    },
                )
            })
            .collect();
        budget.step()?;
        Ok(DocumentSortKey {
            components,
            retained_bytes: bytes,
        })
    }

    fn ancestor(
        &self,
        ancestor: usize,
        mut descendant: usize,
        budget: &mut Budget<'_>,
    ) -> EngineResult<bool> {
        loop {
            budget.step()?;
            if ancestor == descendant {
                return Ok(true);
            }
            if descendant == 0 {
                return Ok(false);
            }
            descendant = self.parents[descendant];
        }
    }
}

struct Budget<'a> {
    steps: usize,
    bytes: usize,
    candidates: usize,
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
    fn charge(&mut self, bytes: usize) -> EngineResult<()> {
        self.step()?;
        self.bytes = self.bytes.saturating_add(bytes);
        if self.bytes > MAX_WORK_BYTES {
            return Err(limit());
        }
        Ok(())
    }
}

fn limit() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "document sorting resource limit exceeded",
    )
}

struct Candidate<'a> {
    // None defers ambiguity until all parallel-array checks have run.
    key: Option<AtomRef<'a>>,
    provenance: Vec<(usize, usize)>,
    // All sources visited by one path form a chain; its deepest source suffices.
    source: Option<usize>,
}

fn emit<'a>(
    key: Option<AtomRef<'a>>,
    provenance: &[(usize, usize)],
    source: Option<usize>,
    output: &mut Vec<Candidate<'a>>,
    budget: &mut Budget<'_>,
) -> EngineResult<()> {
    budget.candidates += 1;
    if budget.candidates > MAX_CANDIDATES {
        return Err(limit());
    }
    budget.charge(128 + provenance.len() * 32)?;
    output.push(Candidate {
        key,
        provenance: provenance.to_vec(),
        source,
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn walk_document<'a>(
    document: &'a BsonDocument,
    offset: usize,
    field: &Field,
    provenance: &mut Vec<(usize, usize)>,
    source: Option<usize>,
    output: &mut Vec<Candidate<'a>>,
    budget: &mut Budget<'_>,
) -> EngineResult<()> {
    // Explicitly charge field lookup work as well as path traversal.
    for (name, value) in document.iter() {
        budget.step()?;
        if name == field.parts[offset] {
            return walk(
                value,
                offset + 1,
                field,
                provenance,
                source,
                false,
                output,
                budget,
            );
        }
    }
    emit(
        Some(AtomRef::Value(&NULL)),
        provenance,
        source,
        output,
        budget,
    )
}

#[allow(clippy::too_many_arguments)]
fn walk<'a>(
    value: &'a BsonValue,
    offset: usize,
    field: &Field,
    provenance: &mut Vec<(usize, usize)>,
    source: Option<usize>,
    positional: bool,
    output: &mut Vec<Candidate<'a>>,
    budget: &mut Budget<'_>,
) -> EngineResult<()> {
    budget.step()?;
    if offset == field.parts.len() {
        let mut key = AtomRef::Value(value);
        let mut source = source;
        if let BsonValue::Array(values) = value {
            source = Some(field.prefixes[offset]);
            if values.is_empty() {
                key = AtomRef::EmptyArray;
            } else if !positional {
                key = AtomRef::Value(&values[0]);
                for value in &values[1..] {
                    budget.step()?;
                    let order = compare_atom(AtomRef::Value(value), key);
                    if (field.descending && order.is_gt()) || (!field.descending && order.is_lt()) {
                        key = AtomRef::Value(value);
                    }
                }
            }
        }
        return emit(Some(key), provenance, source, output, budget);
    }
    match value {
        BsonValue::Document(document) => {
            walk_document(document, offset, field, provenance, source, output, budget)
        }
        BsonValue::Array(values) => {
            let source = Some(field.prefixes[offset]);
            let part = &field.parts[offset];
            if part == "0"
                || (part.starts_with(['1', '2', '3', '4', '5', '6', '7', '8', '9'])
                    && part.bytes().all(|byte| byte.is_ascii_digit()))
            {
                let index = part.parse::<usize>().ok();
                let mut named = false;
                for value in values {
                    budget.step()?;
                    if let BsonValue::Document(document) = value {
                        for (name, _) in document.iter() {
                            budget.step()?;
                            named |= name == part;
                        }
                    }
                }
                if let Some(index) = index.filter(|index| *index < values.len()) {
                    if named {
                        return emit(None, provenance, source, output, budget);
                    }
                    return walk(
                        &values[index],
                        offset + 1,
                        field,
                        provenance,
                        source,
                        true,
                        output,
                        budget,
                    );
                }
                if !named {
                    return emit(
                        Some(AtomRef::Value(&NULL)),
                        provenance,
                        source,
                        output,
                        budget,
                    );
                }
            }
            if values.is_empty() {
                return emit(
                    Some(AtomRef::Value(&NULL)),
                    provenance,
                    source,
                    output,
                    budget,
                );
            }
            for (index, member) in values.iter().enumerate() {
                budget.step()?;
                provenance.push((field.prefixes[offset], index));
                if let BsonValue::Document(document) = member {
                    walk_document(document, offset, field, provenance, source, output, budget)?;
                } else {
                    emit(
                        Some(AtomRef::Value(&NULL)),
                        provenance,
                        source,
                        output,
                        budget,
                    )?;
                }
                provenance.pop();
            }
            Ok(())
        }
        _ => emit(
            Some(AtomRef::Value(&NULL)),
            provenance,
            source,
            output,
            budget,
        ),
    }
}

struct Lookup<'a> {
    candidates: Vec<Candidate<'a>>,
    by_source: BTreeMap<usize, BTreeMap<usize, Vec<usize>>>,
    without_source: BTreeMap<usize, Vec<usize>>,
}

impl<'a> Lookup<'a> {
    fn new(candidates: Vec<Candidate<'a>>, budget: &mut Budget<'_>) -> EngineResult<Self> {
        let mut by_source: BTreeMap<usize, BTreeMap<usize, Vec<usize>>> = BTreeMap::new();
        for (id, candidate) in candidates.iter().enumerate() {
            for &(source, index) in &candidate.provenance {
                budget.charge(128)?;
                by_source
                    .entry(source)
                    .or_default()
                    .entry(index)
                    .or_default()
                    .push(id);
            }
        }
        let mut without_source = BTreeMap::new();
        for &source in by_source.keys() {
            let mut missing = Vec::new();
            for (id, candidate) in candidates.iter().enumerate() {
                let mut has_source = false;
                for &(candidate_source, _) in &candidate.provenance {
                    budget.step()?;
                    has_source |= candidate_source == source;
                }
                if !has_source {
                    budget.charge(16)?;
                    missing.push(id);
                }
            }
            without_source.insert(source, missing);
        }
        Ok(Self {
            candidates,
            by_source,
            without_source,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn join<'a>(
    sorter: &DocumentSorter,
    lookups: &[Lookup<'a>],
    offset: usize,
    provenance: &[(usize, usize)],
    source: Option<usize>,
    selected: &mut Vec<Option<AtomRef<'a>>>,
    best: &mut Option<Vec<AtomRef<'a>>>,
    ambiguous: &mut bool,
    budget: &mut Budget<'_>,
) -> EngineResult<()> {
    budget.step()?;
    if offset == lookups.len() {
        if selected.iter().any(Option::is_none) {
            *ambiguous = true;
            return Ok(());
        }
        let mut order = Ordering::Less;
        if let Some(previous) = best {
            order = Ordering::Equal;
            for ((value, previous), field) in selected.iter().zip(previous).zip(&sorter.fields) {
                budget.step()?;
                order = compare_atom(value.expect("non-ambiguous candidate"), *previous);
                if field.descending {
                    order = order.reverse();
                }
                if order != Ordering::Equal {
                    break;
                }
            }
        }
        if order.is_lt() {
            *best = Some(
                selected
                    .iter()
                    .map(|key| key.expect("non-ambiguous candidate"))
                    .collect(),
            );
        }
        return Ok(());
    }
    let lookup = &lookups[offset];
    let mut indexes: Option<(&[usize], &[usize])> = None;
    for &(source, index) in provenance {
        budget.step()?;
        if let Some(groups) = lookup.by_source.get(&source) {
            let matching = groups.get(&index).map_or(&[][..], Vec::as_slice);
            let missing = lookup.without_source[&source].as_slice();
            if indexes.is_none_or(|(left, right)| {
                matching.len() + missing.len() < left.len() + right.len()
            }) {
                indexes = Some((matching, missing));
            }
        }
    }
    let count = indexes.map_or(lookup.candidates.len(), |(left, right)| {
        left.len() + right.len()
    });
    for position in 0..count {
        budget.step()?;
        let id = indexes.map_or(position, |(left, right)| {
            if position < left.len() {
                left[position]
            } else {
                right[position - left.len()]
            }
        });
        let candidate = &lookup.candidates[id];
        let mut compatible = true;
        for &(candidate_source, index) in &candidate.provenance {
            for &(selected_source, selected_index) in provenance {
                budget.step()?;
                if candidate_source == selected_source && index != selected_index {
                    compatible = false;
                    break;
                }
            }
            if !compatible {
                break;
            }
        }
        if !compatible {
            continue;
        }
        let merged_source = match (source, candidate.source) {
            (Some(left), Some(right)) => {
                if sorter.ancestor(left, right, budget)? {
                    Some(right)
                } else if sorter.ancestor(right, left, budget)? {
                    Some(left)
                } else {
                    return Err(query_error(2));
                }
            }
            (left, right) => left.or(right),
        };
        let mut merged = provenance.to_vec();
        for &(source, index) in &candidate.provenance {
            if !merged.iter().any(|(previous, _)| *previous == source) {
                merged.push((source, index));
            }
        }
        selected.push(candidate.key);
        join(
            sorter,
            lookups,
            offset + 1,
            &merged,
            merged_source,
            selected,
            best,
            ambiguous,
            budget,
        )?;
        selected.pop();
    }
    Ok(())
}

fn value_bytes(value: &BsonValue, budget: &mut Budget<'_>) -> EngineResult<usize> {
    super::memory::value_bytes(value, MAX_KEY_BYTES, &mut || budget.step())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::DocumentQueryError;
    use std::error::Error;

    fn doc(entries: &[(&str, BsonValue)]) -> BsonDocument {
        BsonDocument::from_entries(entries.iter().map(|(name, value)| (*name, value.clone())))
            .unwrap()
    }

    #[test]
    fn sorting_bson_ties_empty_arrays_and_redacted_owned_keys() {
        let sorter = DocumentSorter::compile(&doc(&[("secret", BsonValue::Int32(1))])).unwrap();
        let key = |value| sorter.key(&doc(&[("secret", value)])).unwrap();
        assert_eq!(key(BsonValue::Int32(1)), key(BsonValue::Int64(1)));
        assert_eq!(key(BsonValue::Double(-0.0)), key(BsonValue::Int32(0)));
        assert!(key(BsonValue::MinKey) < key(BsonValue::Array(vec![])));
        assert!(key(BsonValue::Array(vec![])) < key(BsonValue::Null));
        assert_eq!(sorter.key(&doc(&[])).unwrap(), key(BsonValue::Null));
        let document = doc(&[("secret", BsonValue::from("hidden-value"))]);
        let before = encode_document(&document).unwrap();
        let owned = sorter.key(&document).unwrap();
        assert_eq!(before, encode_document(&document).unwrap());
        drop(document);
        assert_eq!(owned, key(BsonValue::from("hidden-value")));
        assert!(owned.retained_bytes() >= 128 + "hidden-value".len());
        assert!(!format!("{sorter:?} {owned:?}").contains("secret"));
        assert!(!format!("{sorter:?} {owned:?}").contains("hidden-value"));
    }

    #[test]
    fn sorting_correlates_large_compound_arrays_without_cross_product() {
        let sorter = DocumentSorter::compile(&doc(&[
            ("v.x", BsonValue::Int32(1)),
            ("v.y", BsonValue::Int32(1)),
        ]))
        .unwrap();
        let array = doc(&[(
            "v",
            BsonValue::Array(
                (0..3000)
                    .map(|index| {
                        BsonValue::Document(doc(&[
                            ("x", BsonValue::Int32(index)),
                            ("y", BsonValue::Int32(3000 - index)),
                        ]))
                    })
                    .collect(),
            ),
        )]);
        let expected = doc(&[(
            "v",
            BsonValue::Document(doc(&[
                ("x", BsonValue::Int32(0)),
                ("y", BsonValue::Int32(3000)),
            ])),
        )]);
        assert_eq!(sorter.key(&array).unwrap(), sorter.key(&expected).unwrap());
        let incorrectly_uncorrelated = doc(&[(
            "v",
            BsonValue::Document(doc(&[
                ("x", BsonValue::Int32(0)),
                ("y", BsonValue::Int32(1)),
            ])),
        )]);
        assert!(sorter.key(&array).unwrap() > sorter.key(&incorrectly_uncorrelated).unwrap());
    }

    #[test]
    fn sorting_parallel_arrays_take_precedence_over_ambiguous_numeric_paths() {
        let sorter = DocumentSorter::compile(&doc(&[
            ("a.0", BsonValue::Int32(1)),
            ("b", BsonValue::Int32(1)),
        ]))
        .unwrap();
        let document = doc(&[
            (
                "a",
                BsonValue::Array(vec![BsonValue::Document(doc(&[(
                    "0",
                    BsonValue::Int32(1),
                )]))]),
            ),
            ("b", BsonValue::Array(vec![])),
        ]);
        let error = sorter.key(&document).unwrap_err();
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<DocumentQueryError>()
                .unwrap()
                .mongo_code(),
            2
        );
        let sorter = DocumentSorter::compile(&doc(&[("a.0", BsonValue::Int32(1))])).unwrap();
        let error = sorter.key(&document).unwrap_err();
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<DocumentQueryError>()
                .unwrap()
                .mongo_code(),
            16746
        );
    }

    #[test]
    fn sorting_bounds_spec_candidates_key_memory_and_cooperative_work() {
        for spec in [
            doc(&[(&vec!["a"; MAX_DEPTH + 1].join("."), BsonValue::Int32(1))]),
            doc(&[(&"a".repeat(MAX_SPEC_BYTES), BsonValue::Int32(1))]),
        ] {
            assert_eq!(
                DocumentSorter::compile(&spec).unwrap_err().kind(),
                EngineErrorKind::LimitExceeded
            );
        }
        let cancelled = || EngineError::new(EngineErrorKind::Cancelled, "test cancellation");
        let spec = doc(&[("v.x", BsonValue::Int32(1))]);
        assert_eq!(
            DocumentSorter::compile_with_check(&spec, &mut || Err(cancelled()))
                .unwrap_err()
                .kind(),
            EngineErrorKind::Cancelled
        );
        let sorter = DocumentSorter::compile(&spec).unwrap();
        let array = doc(&[(
            "v",
            BsonValue::Array(vec![BsonValue::Null; MAX_CANDIDATES + 1]),
        )]);
        assert_eq!(
            sorter.key(&array).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let mut steps = 0;
        assert_eq!(
            sorter
                .key_with_check(&array, &mut || {
                    steps += 1;
                    if steps == 20 {
                        Err(cancelled())
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err()
                .kind(),
            EngineErrorKind::Cancelled
        );
        let large = doc(&[("v", BsonValue::String("x".repeat(MAX_KEY_BYTES)))]);
        let sorter = DocumentSorter::compile(&doc(&[("v", BsonValue::Int32(1))])).unwrap();
        assert_eq!(
            sorter.key(&large).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let mut budget = Budget {
            steps: MAX_STEPS,
            candidates: 0,
            bytes: 0,
            check: &mut || Ok(()),
        };
        assert_eq!(
            budget.step().unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let mut budget = Budget {
            steps: 0,
            candidates: 0,
            bytes: MAX_WORK_BYTES,
            check: &mut || Ok(()),
        };
        assert_eq!(
            budget.charge(1).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
    }
}
