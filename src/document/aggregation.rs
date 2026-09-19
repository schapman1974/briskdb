//! Shared, bounded basic aggregation. Adapters must not reinterpret stages.

use std::{cmp::Reverse, collections::BinaryHeap, fmt};

use super::{
    BsonDocument, BsonErrorContext, BsonValue, DocumentMatcher, DocumentPipeline, DocumentSorter,
    aggregation_transform::Transform, encode_document, matcher::integer, matcher::query_error,
    memory,
};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

const MAX_ROWS: usize = 65_536;
const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_STEPS: usize = 4_000_000;
const ROW_BYTES: usize = 96;

enum Stage {
    Match(DocumentMatcher),
    Sort(DocumentSorter),
    Skip(u64),
    Limit(u64),
    Count(String),
    Transform(Transform),
}

/// An eagerly compiled basic and projection-stage aggregation pipeline.
/// Inputs are immutable and retained BSON representations survive unchanged.
/// Sorts are stable relative to the preceding stage, not the original input.
///
/// The borrowed `execute` API materializes its input. It rejects more
/// than 65,536 input rows, 64 MiB of conservatively charged working data (sort
/// keys included), or four million checked work steps per execution. Compiled
/// stages have a separate 64 MiB retention bound. A late limit does not bypass
/// these quotas. `into_stream` instead accepts incremental owned inputs.
/// Nonblocking stages are fused so limits stop upstream expression evaluation,
/// including after blocking stages. No spilling or storage access is provided.
pub struct DocumentAggregator {
    stages: Vec<Stage>,
    retained_bytes: usize,
}

impl fmt::Debug for DocumentAggregator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentAggregator")
            .field("stages", &self.stages.len())
            .finish_non_exhaustive()
    }
}

impl DocumentAggregator {
    pub fn compile(pipeline: &DocumentPipeline) -> EngineResult<Self> {
        Self::compile_with_check(pipeline, &mut || Ok(()))
    }

    /// Validate every stage, including stages that cannot receive any rows.
    pub fn compile_with_check(
        pipeline: &DocumentPipeline,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Self> {
        let mut budget = Budget { check, steps: 0 };
        budget.step()?;
        let mut stages = Vec::new();
        let mut retained_bytes = 128;
        for stage in pipeline.stages() {
            budget.step()?;
            if stage.len() != 1 {
                return Err(query_error(2));
            }
            let (name, argument) = stage.iter().next().expect("one stage field");
            if !name.starts_with('$') {
                return Err(query_error(2));
            }
            let (compiled, bytes) = match name {
                "$match" => {
                    let BsonValue::Document(filter) = argument else {
                        return Err(query_error(2));
                    };
                    let matcher =
                        DocumentMatcher::compile_with_check(filter, &mut || budget.step())?;
                    let bytes = matcher.retained_bytes();
                    (Stage::Match(matcher), bytes)
                }
                "$sort" => {
                    let BsonValue::Document(spec) = argument else {
                        return Err(query_error(15973));
                    };
                    let sorter = DocumentSorter::compile_with_check(spec, &mut || budget.step())?;
                    let bytes = sorter.retained_bytes();
                    (Stage::Sort(sorter), bytes)
                }
                "$skip" => (Stage::Skip(amount(argument, 5107200)?), 0),
                "$limit" => {
                    let amount = amount(argument, 5107201)?;
                    if amount == 0 {
                        return Err(query_error(15958));
                    }
                    (Stage::Limit(amount), 0)
                }
                "$count" => {
                    let BsonValue::String(field) = argument else {
                        return Err(query_error(40156));
                    };
                    if field.is_empty() {
                        return Err(query_error(40157));
                    }
                    if field.starts_with('$') {
                        return Err(query_error(40158));
                    }
                    if field.contains('\0') {
                        return Err(query_error(40159));
                    }
                    if field.contains('.') {
                        return Err(query_error(40160));
                    }
                    if field == "_id" {
                        return Err(query_error(15948));
                    }
                    (Stage::Count(field.clone()), field.len())
                }
                "$project" | "$set" | "$addFields" | "$unset" => {
                    let transform =
                        Transform::compile(name, stage, argument, &mut || budget.step())?;
                    let bytes = transform.retained_bytes();
                    (Stage::Transform(transform), bytes)
                }
                _ => return Err(query_error(115)),
            };
            add_bytes(&mut retained_bytes, bytes)?;
            add_bytes(&mut retained_bytes, 256)?;
            stages.push(compiled);
        }
        budget.step()?;
        Ok(Self {
            stages,
            retained_bytes,
        })
    }

    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Consume the compiled plan into an incremental, single-use execution.
    pub fn into_stream(self) -> DocumentAggregationStream {
        let boundary = self
            .stages
            .iter()
            .position(|stage| matches!(stage, Stage::Sort(_) | Stage::Count(_)))
            .unwrap_or(self.stages.len());
        let remaining = prefix_counters(&self.stages[..boundary]);
        DocumentAggregationStream {
            plan: self,
            boundary,
            remaining,
            rows: Vec::new(),
            count: 0,
            bytes: 0,
            steps: 0,
            consumed: 0,
            exhausted: false,
            failed: false,
        }
    }

    pub(crate) fn document_retained_bytes(
        document: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<usize> {
        Ok(memory::document_bytes(document, MAX_BYTES - ROW_BYTES, check)? + ROW_BYTES)
    }

    pub fn execute(&self, documents: &[BsonDocument]) -> EngineResult<Vec<BsonDocument>> {
        self.execute_with_check(documents, &mut || Ok(()))
    }

    /// Validate borrowed BSON and preflight its retained size before cloning.
    /// An error returns no partial result and does not poison the compiled plan.
    pub fn execute_with_check(
        &self,
        documents: &[BsonDocument],
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Vec<BsonDocument>> {
        let mut budget = Budget { check, steps: 0 };
        budget.step()?;
        if documents.len() > MAX_ROWS {
            return Err(limit());
        }
        let mut bytes = 0;
        let mut rows = Vec::new();
        for document in documents {
            budget.step()?;
            encode_document(document)
                .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
            let retained_bytes =
                memory::document_bytes(document, MAX_BYTES, &mut || budget.step())? + ROW_BYTES;
            add_bytes(&mut bytes, retained_bytes)?;
            rows.push(Row {
                document: document.clone(),
                retained_bytes,
            });
        }
        let rows = execute_stages(&self.stages, rows, &mut budget)?;
        output_documents(rows, &mut budget)
    }
}

/// Incremental execution of a compiled pipeline. Match/skip/limit/transform stages
/// emit at most one owned row per input; the first count retains only a counter,
/// and the first sort buffers bounded rows. `finish` flushes any blocking stage
/// through the remaining shared stage executor. Engine-validated source rows
/// are moved without cloning. Public pushes copy only rows retained by a sort,
/// so caller-reserved spare BSON capacity cannot escape retention accounting.
///
/// At most 65,536 consumed inputs and four million checked work steps are
/// admitted over the entire stream, including finalization. A failed push
/// poisons the stream, so a caller cannot convert a failed scan into a successful
/// final result. Earlier delivered batches cannot be retracted. Callers stop
/// reading input when `is_input_exhausted` becomes true, then call `finish`.
pub struct DocumentAggregationStream {
    plan: DocumentAggregator,
    boundary: usize,
    remaining: Vec<u64>,
    rows: Vec<Row>,
    count: usize,
    bytes: usize,
    steps: usize,
    consumed: usize,
    exhausted: bool,
    failed: bool,
}

impl fmt::Debug for DocumentAggregationStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentAggregationStream")
            .field("consumed", &self.consumed)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl DocumentAggregationStream {
    pub fn retained_bytes(&self) -> usize {
        self.plan
            .retained_bytes
            .saturating_add(self.bytes)
            .saturating_add(
                self.remaining
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
            .saturating_add(512)
    }

    pub const fn is_input_exhausted(&self) -> bool {
        self.exhausted
    }

    pub fn push(&mut self, document: BsonDocument) -> EngineResult<Option<BsonDocument>> {
        self.push_with_check(document, &mut || Ok(()))
    }

    pub fn push_with_check(
        &mut self,
        document: BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Option<BsonDocument>> {
        self.push_checked(document, true, check)
    }

    pub(crate) fn push_validated_with_check(
        &mut self,
        document: BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Option<BsonDocument>> {
        self.push_checked(document, false, check)
    }

    fn push_checked(
        &mut self,
        document: BsonDocument,
        validate: bool,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Option<BsonDocument>> {
        if self.failed {
            return Err(failed_stream());
        }
        self.failed = true;
        let mut budget = Budget {
            check,
            steps: self.steps,
        };
        let result = (|| {
            budget.step()?;
            if validate {
                encode_document(&document)
                    .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
            }
            if self.exhausted {
                return Ok(None);
            }
            if self.consumed == MAX_ROWS {
                return Err(limit());
            }
            self.consumed += 1;
            let retained_bytes =
                DocumentAggregator::document_retained_bytes(&document, &mut || budget.step())?;
            let Some(row) = execute_prefix(
                &self.plan.stages[..self.boundary],
                &mut self.remaining,
                Row {
                    document,
                    retained_bytes,
                },
                &mut self.exhausted,
                self.bytes,
                &mut budget,
            )?
            else {
                return Ok(None);
            };
            let Row {
                document,
                retained_bytes,
            } = row;
            budget.step()?;
            match self.plan.stages.get(self.boundary) {
                Some(Stage::Sort(_)) => {
                    add_bytes(&mut self.bytes, retained_bytes)?;
                    self.rows.push(Row {
                        document: if validate { document.clone() } else { document },
                        retained_bytes,
                    });
                    Ok(None)
                }
                Some(Stage::Count(_)) => {
                    self.count += 1;
                    Ok(None)
                }
                None => Ok(Some(document)),
                _ => unreachable!("boundary is the first blocking stage"),
            }
        })();
        self.steps = budget.steps;
        if result.is_ok() {
            self.failed = false;
        }
        result
    }

    pub fn finish(self) -> EngineResult<Vec<BsonDocument>> {
        self.finish_with_check(&mut || Ok(()))
    }

    pub fn finish_with_check(
        self,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Vec<BsonDocument>> {
        if self.failed {
            return Err(failed_stream());
        }
        let mut budget = Budget {
            check,
            steps: self.steps,
        };
        budget.step()?;
        let rows = match self.plan.stages.get(self.boundary) {
            Some(Stage::Sort(sorter)) => sort(self.rows, sorter, &mut budget)?,
            Some(Stage::Count(field)) => count_row(field, self.count, &mut budget)?,
            None => return Ok(Vec::new()),
            _ => unreachable!("boundary is the first blocking stage"),
        };
        let rows = execute_stages(&self.plan.stages[self.boundary + 1..], rows, &mut budget)?;
        output_documents(rows, &mut budget)
    }
}

fn failed_stream() -> EngineError {
    EngineError::new(
        EngineErrorKind::FailedPrecondition,
        "document aggregation stream has failed",
    )
}

fn execute_stages(
    mut stages: &[Stage],
    mut rows: Vec<Row>,
    budget: &mut Budget<'_>,
) -> EngineResult<Vec<Row>> {
    while !stages.is_empty() {
        budget.step()?;
        let boundary = stages
            .iter()
            .position(|stage| matches!(stage, Stage::Sort(_) | Stage::Count(_)))
            .unwrap_or(stages.len());
        let mut remaining = prefix_counters(&stages[..boundary]);
        let mut exhausted = false;
        let mut next = Vec::new();
        let mut count = 0;
        let mut bytes = 0;
        let mut unconsumed: usize = rows.iter().map(|row| row.retained_bytes).sum();
        let counting = matches!(stages.get(boundary), Some(Stage::Count(_)));
        // Fuse nonblocking stages. In particular, a later limit must stop
        // evaluating earlier expressions on rows that will never be consumed.
        for row in rows {
            budget.step()?;
            if exhausted {
                break;
            }
            unconsumed -= row.retained_bytes;
            if let Some(row) = execute_prefix(
                &stages[..boundary],
                &mut remaining,
                row,
                &mut exhausted,
                unconsumed + bytes,
                budget,
            )? {
                count += 1;
                if !counting {
                    add_bytes(&mut bytes, row.retained_bytes)?;
                    next.push(row);
                }
            }
        }
        rows = match stages.get(boundary) {
            Some(Stage::Sort(sorter)) => sort(next, sorter, budget)?,
            Some(Stage::Count(field)) => count_row(field, count, budget)?,
            None => return Ok(next),
            _ => unreachable!("blocking boundary"),
        };
        stages = &stages[boundary + 1..];
    }
    Ok(rows)
}

fn prefix_counters(stages: &[Stage]) -> Vec<u64> {
    stages
        .iter()
        .map(|stage| match stage {
            Stage::Skip(amount) | Stage::Limit(amount) => *amount,
            _ => 0,
        })
        .collect()
}

fn execute_prefix(
    stages: &[Stage],
    remaining: &mut [u64],
    mut row: Row,
    exhausted: &mut bool,
    retained_elsewhere: usize,
    budget: &mut Budget<'_>,
) -> EngineResult<Option<Row>> {
    for (stage, remaining) in stages.iter().zip(remaining) {
        budget.step()?;
        match stage {
            Stage::Match(matcher) => {
                if !matcher.matches_with_check(&row.document, &mut || budget.step())? {
                    return Ok(None);
                }
            }
            Stage::Skip(_) => {
                if *remaining != 0 {
                    *remaining -= 1;
                    return Ok(None);
                }
            }
            Stage::Limit(_) => {
                if *remaining == 0 {
                    *exhausted = true;
                    return Ok(None);
                }
                *remaining -= 1;
                *exhausted |= *remaining == 0;
            }
            Stage::Transform(transform) => {
                row.document = transform.apply(
                    row.document,
                    MAX_BYTES
                        .checked_sub(retained_elsewhere)
                        .ok_or_else(limit)?,
                    &mut || budget.step(),
                )?;
                row.retained_bytes =
                    DocumentAggregator::document_retained_bytes(&row.document, &mut || {
                        budget.step()
                    })?;
            }
            _ => unreachable!("nonblocking prefix"),
        }
    }
    Ok(Some(row))
}

fn count_row(field: &str, amount: usize, budget: &mut Budget<'_>) -> EngineResult<Vec<Row>> {
    if amount == 0 {
        return Ok(Vec::new());
    }
    let value =
        i32::try_from(amount).map_or_else(|_| BsonValue::Int64(amount as i64), BsonValue::Int32);
    let document = BsonDocument::from_entries([(field, value)]).expect("validated count field");
    let retained_bytes =
        DocumentAggregator::document_retained_bytes(&document, &mut || budget.step())?;
    Ok(vec![Row {
        document,
        retained_bytes,
    }])
}

fn output_documents(rows: Vec<Row>, budget: &mut Budget<'_>) -> EngineResult<Vec<BsonDocument>> {
    let mut result = Vec::new();
    for row in rows {
        budget.step()?;
        result.push(row.document);
    }
    budget.step()?;
    Ok(result)
}

struct Row {
    document: BsonDocument,
    retained_bytes: usize,
}

struct Budget<'a> {
    check: &'a mut dyn FnMut() -> EngineResult<()>,
    steps: usize,
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
}

fn sort(
    rows: Vec<Row>,
    sorter: &DocumentSorter,
    budget: &mut Budget<'_>,
) -> EngineResult<Vec<Row>> {
    let mut bytes = rows.iter().map(|row| row.retained_bytes).sum();
    let mut keys = BinaryHeap::new();
    for (index, row) in rows.iter().enumerate() {
        budget.step()?;
        let key = sorter.key_validated_with_check(&row.document, &mut || budget.step())?;
        add_bytes(&mut bytes, key.retained_bytes())?;
        add_bytes(&mut bytes, 96)?;
        // The index is relative to this stage's input, preserving the order
        // established by any previous sort when BSON keys compare equal.
        keys.push(Reverse((key, index)));
    }
    let mut source: Vec<_> = rows.into_iter().map(Some).collect();
    let mut result = Vec::new();
    while let Some(Reverse((_, index))) = keys.pop() {
        budget.step()?;
        result.push(source[index].take().expect("unique input index"));
    }
    Ok(result)
}

fn amount(value: &BsonValue, code: i32) -> EngineResult<u64> {
    integer(value, false)
        .and_then(|amount| u64::try_from(amount).ok())
        .ok_or_else(|| query_error(code))
}

fn add_bytes(bytes: &mut usize, amount: usize) -> EngineResult<()> {
    *bytes = bytes
        .checked_add(amount)
        .filter(|total| *total <= MAX_BYTES)
        .ok_or_else(limit)?;
    Ok(())
}

fn limit() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "document aggregation resource limit exceeded",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{BsonJavaScript, DocumentQueryError};
    use std::error::Error;

    fn doc(entries: &[(&str, BsonValue)]) -> BsonDocument {
        BsonDocument::from_entries(entries.iter().map(|(name, value)| (*name, value.clone())))
            .unwrap()
    }

    fn pipeline(entries: &[(&str, BsonValue)]) -> DocumentPipeline {
        DocumentPipeline::new(
            entries
                .iter()
                .map(|entry| doc(std::slice::from_ref(entry)))
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    fn rows() -> Vec<BsonDocument> {
        (0..12)
            .map(|index| {
                doc(&[
                    ("_id", BsonValue::Int64(index)),
                    ("group", BsonValue::Int32((index % 3) as i32)),
                    ("secret", BsonValue::String("private-value".into())),
                ])
            })
            .collect()
    }

    #[test]
    fn stage_order_stable_ties_empty_counts_and_bson_fidelity() {
        let original = rows();
        let before: Vec<_> = original
            .iter()
            .map(|row| encode_document(row).unwrap())
            .collect();
        let sorter = DocumentAggregator::compile(&pipeline(&[
            (
                "$sort",
                BsonValue::Document(doc(&[("_id", BsonValue::Int32(-1))])),
            ),
            (
                "$sort",
                BsonValue::Document(doc(&[("group", BsonValue::Int32(1))])),
            ),
            ("$skip", BsonValue::Double(1.0)),
            ("$limit", BsonValue::Int64(4)),
        ]))
        .unwrap();
        let result = sorter.execute(&original).unwrap();
        let ids: Vec<_> = result
            .iter()
            .map(|row| row.get_first("_id").unwrap().clone())
            .collect();
        assert_eq!(ids, [6, 3, 0, 10].map(BsonValue::Int64));
        assert_eq!(
            result
                .iter()
                .map(|row| encode_document(row).unwrap())
                .collect::<Vec<_>>(),
            [6, 3, 0, 10].map(|index| before[index].clone())
        );
        let counter = DocumentAggregator::compile(&pipeline(&[
            (
                "$match",
                BsonValue::Document(doc(&[("group", BsonValue::Int32(1))])),
            ),
            ("$skip", BsonValue::Int32(1)),
            ("$limit", BsonValue::Int32(2)),
            ("$count", BsonValue::String("total".into())),
        ]))
        .unwrap();
        assert_eq!(
            counter.execute(&original).unwrap(),
            [doc(&[("total", BsonValue::Int32(2))])]
        );
        assert!(counter.execute(&[]).unwrap().is_empty());
        let twice = DocumentAggregator::compile(&pipeline(&[
            ("$count", BsonValue::String("n".into())),
            ("$count", BsonValue::String("m".into())),
        ]))
        .unwrap();
        assert_eq!(
            twice.execute(&original).unwrap(),
            [doc(&[("m", BsonValue::Int32(1))])]
        );
        assert!(twice.execute(&[]).unwrap().is_empty());
        let identity = DocumentAggregator::compile(&pipeline(&[])).unwrap();
        assert_eq!(identity.execute(&original).unwrap(), original);
        assert_eq!(
            before,
            original
                .iter()
                .map(|row| encode_document(row).unwrap())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn eager_validation_has_specific_codes_and_redacted_diagnostics() {
        for (stage, argument, expected) in [
            ("match", BsonValue::Null, 2),
            ("$match", BsonValue::Null, 2),
            ("$sort", BsonValue::Null, 15973),
            ("$sort", BsonValue::Document(doc(&[])), 15976),
            ("$skip", BsonValue::Boolean(true), 5107200),
            ("$skip", BsonValue::Double(0.1), 5107200),
            ("$skip", BsonValue::Int32(-1), 5107200),
            ("$limit", BsonValue::Int32(-1), 5107201),
            ("$limit", BsonValue::Int32(0), 15958),
            (
                "$count",
                BsonValue::JavaScript(BsonJavaScript::new("n")),
                40156,
            ),
            ("$count", BsonValue::String("".into()), 40157),
            ("$count", BsonValue::String("$secret.\0".into()), 40158),
            ("$count", BsonValue::String("secret.\0".into()), 40159),
            ("$count", BsonValue::String("secret.field".into()), 40160),
            ("$count", BsonValue::String("_id".into()), 15948),
            ("$group", BsonValue::Document(doc(&[])), 115),
            ("$project", BsonValue::Document(doc(&[])), 51272),
            ("$unknown-secret", BsonValue::Null, 115),
        ] {
            let error = DocumentAggregator::compile(&pipeline(&[
                ("$skip", BsonValue::Int64(i64::MAX)),
                (stage, argument),
            ]))
            .unwrap_err();
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<DocumentQueryError>()
                    .unwrap()
                    .mongo_code(),
                expected
            );
            assert!(!format!("{error:?}").contains("secret"));
        }
        for stage in [
            doc(&[]),
            doc(&[
                ("$skip", BsonValue::Int32(1)),
                ("$limit", BsonValue::Int32(2)),
            ]),
        ] {
            assert!(
                DocumentAggregator::compile(&DocumentPipeline::new(vec![stage]).unwrap()).is_err()
            );
        }
        let runner = DocumentAggregator::compile(&pipeline(&[
            (
                "$match",
                BsonValue::Document(doc(&[(
                    "secret",
                    BsonValue::String("private-value".into()),
                )])),
            ),
            ("$count", BsonValue::String("private-count".into())),
        ]))
        .unwrap();
        assert!(!format!("{runner:?}").contains("private"));
        assert!(runner.retained_bytes() > 0);
    }

    #[test]
    fn every_compile_and_execute_checkpoint_can_cancel_without_poisoning() {
        let spec = pipeline(&[
            (
                "$match",
                BsonValue::Document(doc(&[("group", BsonValue::Int32(1))])),
            ),
            (
                "$sort",
                BsonValue::Document(doc(&[("_id", BsonValue::Int32(-1))])),
            ),
            ("$skip", BsonValue::Int32(1)),
            ("$limit", BsonValue::Int32(2)),
            ("$count", BsonValue::String("n".into())),
        ]);
        let mut compile_steps = 0;
        let runner = DocumentAggregator::compile_with_check(&spec, &mut || {
            compile_steps += 1;
            Ok(())
        })
        .unwrap();
        let documents = rows();
        let mut execute_steps = 0;
        let expected = runner
            .execute_with_check(&documents, &mut || {
                execute_steps += 1;
                Ok(())
            })
            .unwrap();
        for kind in [
            EngineErrorKind::Cancelled,
            EngineErrorKind::DeadlineExceeded,
        ] {
            for stop in 1..=compile_steps {
                let mut steps = 0;
                assert_eq!(
                    DocumentAggregator::compile_with_check(&spec, &mut || {
                        steps += 1;
                        if steps == stop {
                            Err(EngineError::new(kind, "test interruption"))
                        } else {
                            Ok(())
                        }
                    })
                    .unwrap_err()
                    .kind(),
                    kind
                );
            }
            for stop in 1..=execute_steps {
                let mut steps = 0;
                assert_eq!(
                    runner
                        .execute_with_check(&documents, &mut || {
                            steps += 1;
                            if steps == stop {
                                Err(EngineError::new(kind, "test interruption"))
                            } else {
                                Ok(())
                            }
                        })
                        .unwrap_err()
                        .kind(),
                    kind
                );
            }
        }
        assert_eq!(runner.execute(&documents).unwrap(), expected);
    }

    #[test]
    fn row_memory_compiled_retention_and_work_limits_are_fail_closed() {
        let identity = DocumentAggregator::compile(&pipeline(&[])).unwrap();
        let mut documents = vec![BsonDocument::new(); MAX_ROWS];
        let counter =
            DocumentAggregator::compile(&pipeline(&[("$count", BsonValue::String("n".into()))]))
                .unwrap();
        assert_eq!(
            counter.execute(&documents).unwrap(),
            [doc(&[("n", BsonValue::Int32(MAX_ROWS as i32))])]
        );
        documents.push(BsonDocument::new());
        assert_eq!(
            identity.execute(&documents).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let documents = vec![doc(&[("v", BsonValue::String("x".repeat(900_000)))]); 40];
        assert_eq!(identity.execute(&documents).unwrap().len(), 40);
        let sorted = DocumentAggregator::compile(&pipeline(&[(
            "$sort",
            BsonValue::Document(doc(&[("v", BsonValue::Int32(1))])),
        )]))
        .unwrap();
        // Input alone fits, but its owned sort keys do not.
        assert_eq!(
            sorted.execute(&documents).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let limited = DocumentAggregator::compile(&pipeline(&[
            ("$limit", BsonValue::Int32(1)),
            (
                "$sort",
                BsonValue::Document(doc(&[("v", BsonValue::Int32(1))])),
            ),
        ]))
        .unwrap();
        assert_eq!(limited.execute(&documents).unwrap().len(), 1);
        let documents = vec![documents[0].clone(); 75];
        assert_eq!(
            limited.execute(&documents).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let stage = doc(&[(
            "$match",
            BsonValue::Document(doc(&[("v", BsonValue::String("x".repeat(512 * 1024)))])),
        )]);
        assert_eq!(
            DocumentAggregator::compile(&DocumentPipeline::new(vec![stage; 9]).unwrap())
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        let mut budget = Budget {
            check: &mut || Ok(()),
            steps: MAX_STEPS,
        };
        assert_eq!(
            budget.step().unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let mut bytes = MAX_BYTES;
        assert_eq!(
            add_bytes(&mut bytes, 1).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let mut bytes = 1;
        assert_eq!(
            add_bytes(&mut bytes, usize::MAX).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
    }

    #[test]
    fn structural_bson_validation_precedes_recursive_execution() {
        let mut nested = BsonValue::Null;
        for _ in 0..110 {
            nested = BsonValue::Array(vec![nested]);
        }
        let document = doc(&[("v", nested)]);
        let runner = DocumentAggregator::compile(&pipeline(&[])).unwrap();
        assert!(runner.execute(&[document]).is_err());
        let duplicate = doc(&[("v", BsonValue::Int32(1)), ("v", BsonValue::Int32(2))]);
        assert!(runner.execute(&[duplicate]).is_err());
    }

    #[test]
    fn streaming_prefixes_stop_input_and_count_retains_only_a_counter() {
        let spec = pipeline(&[
            ("$limit", BsonValue::Int32(3)),
            (
                "$match",
                BsonValue::Document(doc(&[("group", BsonValue::Int32(1))])),
            ),
            ("$count", BsonValue::String("private-count".into())),
        ]);
        let mut stream = DocumentAggregator::compile(&spec).unwrap().into_stream();
        let bytes = stream.retained_bytes();
        for document in rows().into_iter().take(3) {
            assert!(stream.push(document).unwrap().is_none());
            assert_eq!(stream.retained_bytes(), bytes);
        }
        assert!(stream.is_input_exhausted());
        assert!(!format!("{stream:?}").contains("private"));
        assert_eq!(
            stream.finish().unwrap(),
            [doc(&[("private-count", BsonValue::Int32(1))])]
        );
        let mut stream = DocumentAggregator::compile(&pipeline(&[]))
            .unwrap()
            .into_stream();
        for document in rows() {
            let before = encode_document(&document).unwrap();
            let result = stream.push(document).unwrap().unwrap();
            assert_eq!(encode_document(&result).unwrap(), before);
            assert!(stream.rows.is_empty());
        }
        assert!(stream.finish().unwrap().is_empty());
    }

    #[test]
    fn stream_push_and_finish_interruptions_poison_without_successful_finalization() {
        for spec in [
            pipeline(&[("$count", BsonValue::String("n".into()))]),
            pipeline(&[
                (
                    "$sort",
                    BsonValue::Document(doc(&[("group", BsonValue::Int32(-1))])),
                ),
                ("$limit", BsonValue::Int32(2)),
            ]),
        ] {
            let mut baseline = DocumentAggregator::compile(&spec).unwrap().into_stream();
            let document = rows().remove(0);
            let mut push_steps = 0;
            baseline
                .push_with_check(document.clone(), &mut || {
                    push_steps += 1;
                    Ok(())
                })
                .unwrap();
            let mut finish_steps = 0;
            baseline
                .finish_with_check(&mut || {
                    finish_steps += 1;
                    Ok(())
                })
                .unwrap();
            for kind in [
                EngineErrorKind::Cancelled,
                EngineErrorKind::DeadlineExceeded,
            ] {
                for stop in 1..=push_steps {
                    let mut stream = DocumentAggregator::compile(&spec).unwrap().into_stream();
                    let mut steps = 0;
                    assert_eq!(
                        stream
                            .push_with_check(document.clone(), &mut || {
                                steps += 1;
                                if steps == stop {
                                    Err(EngineError::new(kind, "test interruption"))
                                } else {
                                    Ok(())
                                }
                            })
                            .unwrap_err()
                            .kind(),
                        kind
                    );
                    assert_eq!(
                        stream.push(document.clone()).unwrap_err().kind(),
                        EngineErrorKind::FailedPrecondition
                    );
                    assert_eq!(
                        stream.finish().unwrap_err().kind(),
                        EngineErrorKind::FailedPrecondition
                    );
                }
                for stop in 1..=finish_steps {
                    let mut stream = DocumentAggregator::compile(&spec).unwrap().into_stream();
                    stream.push(document.clone()).unwrap();
                    let mut steps = 0;
                    assert_eq!(
                        stream
                            .finish_with_check(&mut || {
                                steps += 1;
                                if steps == stop {
                                    Err(EngineError::new(kind, "test interruption"))
                                } else {
                                    Ok(())
                                }
                            })
                            .unwrap_err()
                            .kind(),
                        kind
                    );
                }
            }
        }
        let mut stream = DocumentAggregator::compile(&pipeline(&[]))
            .unwrap()
            .into_stream();
        stream.consumed = MAX_ROWS;
        assert_eq!(
            stream.push(BsonDocument::new()).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        assert!(stream.finish().is_err());
        let mut stream = DocumentAggregator::compile(&pipeline(&[]))
            .unwrap()
            .into_stream();
        stream.steps = MAX_STEPS;
        assert_eq!(
            stream.push(BsonDocument::new()).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let mut stream = DocumentAggregator::compile(&pipeline(&[(
            "$sort",
            BsonValue::Document(doc(&[("v", BsonValue::Int32(1))])),
        )]))
        .unwrap()
        .into_stream();
        stream.bytes = MAX_BYTES;
        assert_eq!(
            stream.push(BsonDocument::new()).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        assert!(stream.finish().is_err());
    }

    #[test]
    fn public_stream_does_not_retain_unaccounted_caller_reserved_capacity() {
        let mut payload = String::with_capacity(1024 * 1024);
        payload.push('x');
        let original = BsonDocument::from_entries([("v", BsonValue::String(payload))]).unwrap();
        let mut stream = DocumentAggregator::compile(&pipeline(&[(
            "$sort",
            BsonValue::Document(doc(&[("v", BsonValue::Int32(1))])),
        )]))
        .unwrap()
        .into_stream();
        stream.push(original).unwrap();
        let Some(BsonValue::String(value)) = stream.rows[0].document.get_first("v") else {
            panic!("string");
        };
        assert!(value.capacity() < 1024);
        assert!(stream.retained_bytes() < 4096);
        assert_eq!(
            stream.finish().unwrap(),
            [doc(&[("v", BsonValue::String("x".into()))])]
        );
    }

    #[test]
    fn transforms_respect_memory_already_retained_by_other_rows() {
        let plan = DocumentAggregator::compile(&pipeline(&[(
            "$set",
            BsonValue::Document(doc(&[("copy", BsonValue::from("$payload"))])),
        )]))
        .unwrap();
        let document = doc(&[("payload", BsonValue::from("x".repeat(1024 * 1024)))]);
        for retained in [0, MAX_BYTES - 2 * 1024 * 1024] {
            let mut check = || Ok(());
            let mut budget = Budget {
                check: &mut check,
                steps: 0,
            };
            let row = Row {
                document: document.clone(),
                retained_bytes: DocumentAggregator::document_retained_bytes(&document, &mut || {
                    Ok(())
                })
                .unwrap(),
            };
            let result = execute_prefix(
                &plan.stages,
                &mut prefix_counters(&plan.stages),
                row,
                &mut false,
                retained,
                &mut budget,
            );
            if retained == 0 {
                assert!(result.unwrap().is_some());
            } else {
                assert_eq!(result.err().unwrap().kind(), EngineErrorKind::LimitExceeded);
            }
        }
    }
}
