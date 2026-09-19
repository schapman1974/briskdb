//! Blocking, ordered BSON grouping shared by every aggregation adapter.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use super::{
    BsonDocument, BsonErrorContext, BsonValue,
    aggregation_expression::{Budget, Expression, MAX_BYTES, limit},
    aggregation_numeric::{Average, Sum},
    encode_document,
    matcher::query_error,
    memory,
};
use crate::core::EngineResult;

pub(super) struct Group {
    key: Expression,
    accumulators: Vec<(String, Operator, Expression)>,
    bytes: usize,
}

#[derive(Clone, Copy)]
enum Operator {
    Sum,
    Average,
    First,
    Last,
    Min,
    Max,
    Push,
    AddToSet,
}

impl Group {
    pub fn compile(
        stage: &BsonDocument,
        value: &BsonValue,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Self> {
        check()?;
        if encode_document(stage)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?
            .len()
            > 1024 * 1024
        {
            return Err(limit());
        }
        let BsonValue::Document(spec) = value else {
            return Err(query_error(2));
        };
        let key = spec.get_first("_id").ok_or_else(|| query_error(2))?;
        let mut budget = Budget::new(check);
        let key = Expression::compile(key, false, &mut budget, 1)?;
        let mut accumulators = Vec::new();
        let mut fields = HashSet::new();
        for (name, value) in spec.iter() {
            budget.step()?;
            if !fields.insert(name) {
                return Err(query_error(2));
            }
            if name == "_id" {
                continue;
            }
            let BsonValue::Document(accumulator) = value else {
                return Err(query_error(40234));
            };
            let Some((operator, operand)) = accumulator.iter().next() else {
                return Err(query_error(40234));
            };
            if !operator.starts_with('$') {
                return Err(query_error(40234));
            }
            if name.contains('.') {
                return Err(query_error(40235));
            }
            if name.starts_with('$') {
                return Err(query_error(40236));
            }
            if accumulator.len() != 1 {
                return Err(query_error(40238));
            }
            if matches!(operand, BsonValue::Array(_)) {
                return Err(query_error(40237));
            }
            let operator = match operator {
                "$sum" => Operator::Sum,
                "$avg" => Operator::Average,
                "$first" => Operator::First,
                "$last" => Operator::Last,
                "$min" => Operator::Min,
                "$max" => Operator::Max,
                "$push" => Operator::Push,
                "$addToSet" => Operator::AddToSet,
                _ => return Err(query_error(115)),
            };
            let name = budget.field(name)?;
            let expression = Expression::compile(operand, false, &mut budget, 1)?;
            accumulators.push((name, operator, expression));
        }
        budget.step()?;
        Ok(Self {
            key,
            accumulators,
            bytes: budget.bytes + 512,
        })
    }

    pub fn retained_bytes(&self) -> usize {
        self.bytes
    }
}

enum State {
    Sum(Sum),
    Average(Average),
    One(Option<BsonValue>),
    Push(Vec<BsonValue>),
    Set {
        values: Vec<Arc<BsonValue>>,
        seen: HashSet<Arc<BsonValue>>,
    },
}

impl State {
    fn new(operator: Operator) -> Self {
        match operator {
            Operator::Sum => Self::Sum(Sum::default()),
            Operator::Average => Self::Average(Average::default()),
            Operator::Push => Self::Push(Vec::new()),
            Operator::AddToSet => Self::Set {
                values: Vec::new(),
                seen: HashSet::new(),
            },
            _ => Self::One(None),
        }
    }

    fn finish(self, check: &mut dyn FnMut() -> EngineResult<()>) -> EngineResult<BsonValue> {
        check()?;
        Ok(match self {
            Self::Sum(value) => value.finish(),
            Self::Average(value) => value.finish(),
            Self::One(value) => value.unwrap_or(BsonValue::Null),
            Self::Push(values) => BsonValue::Array(values),
            Self::Set { values, seen } => {
                drop(seen);
                let mut result = Vec::new();
                for value in values {
                    check()?;
                    result.push(Arc::try_unwrap(value).expect("unshared set value"));
                }
                BsonValue::Array(result)
            }
        })
    }
}

struct Entry {
    key: Arc<BsonValue>,
    states: Vec<State>,
    bytes: usize,
}

pub(super) struct Groups {
    lookup: HashMap<Arc<BsonValue>, usize>,
    entries: Vec<Entry>,
    bytes: usize,
}

impl Default for Groups {
    fn default() -> Self {
        Self {
            lookup: HashMap::new(),
            entries: Vec::new(),
            bytes: 512,
        }
    }
}

impl Groups {
    pub fn retained_bytes(&self) -> usize {
        self.bytes
    }

    pub fn push(
        &mut self,
        plan: &Group,
        document: &BsonDocument,
        retained_elsewhere: usize,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<()> {
        let mut budget = Budget::new(check);
        budget.charge(self.bytes)?;
        budget.charge(retained_elsewhere)?;
        let source_bytes = memory::document_bytes(document, MAX_BYTES, &mut || budget.step())?;
        budget.charge(source_bytes)?;
        let key = plan.key.evaluate(document, &mut budget, 1)?;
        if key.is_none() {
            budget.charge(128)?;
        }
        let key = key.unwrap_or(BsonValue::Null);
        let key_bytes = memory::value_bytes(&key, MAX_BYTES, &mut || budget.step())?;
        budget.step()?;
        let index = match self.lookup.get(&key) {
            Some(index) => *index,
            None => {
                if self.entries.len() >= 65_536 {
                    return Err(limit());
                }
                // Includes hash/vec spare capacity, keys shared by Arc, all
                // accumulator states, and eventual output-container overhead.
                let overhead = 768 + plan.accumulators.len() * 256;
                budget.charge(key_bytes)?;
                budget.charge(overhead)?;
                let bytes = key_bytes + overhead;
                let index = self.entries.len();
                let key = Arc::new(key);
                self.lookup.insert(Arc::clone(&key), index);
                self.entries.push(Entry {
                    key,
                    states: plan
                        .accumulators
                        .iter()
                        .map(|(_, operator, _)| State::new(*operator))
                        .collect(),
                    bytes,
                });
                self.bytes += bytes;
                index
            }
        };
        budget.step()?;
        let entry = &mut self.entries[index];
        for ((_, operator, expression), state) in plan.accumulators.iter().zip(&mut entry.states) {
            // Even $first evaluates every operand, including ones not retained.
            let value = expression.evaluate(document, &mut budget, 1)?;
            let (added, removed) = update(state, *operator, value, &mut budget)?;
            self.bytes = self.bytes + added - removed;
            entry.bytes = entry.bytes + added - removed;
        }
        budget.step()
    }

    pub fn finish(
        self,
        plan: &Group,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Vec<BsonDocument>> {
        check()?;
        drop(self.lookup);
        let mut working = self.bytes;
        let mut results = Vec::new();
        for entry in self.entries {
            check()?;
            let names: usize = plan
                .accumulators
                .iter()
                .map(|(name, _, _)| name.len())
                .sum();
            if working
                .checked_add(names)
                .filter(|bytes| *bytes <= MAX_BYTES)
                .is_none()
            {
                return Err(limit());
            }
            let mut result = BsonDocument::new();
            result
                .push(
                    "_id",
                    Arc::try_unwrap(entry.key).expect("unshared group key"),
                )
                .expect("valid name");
            for ((name, _, _), state) in plan.accumulators.iter().zip(entry.states) {
                check()?;
                result
                    .push(name.clone(), state.finish(check)?)
                    .expect("validated output name");
            }
            encode_document(&result)
                .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
            let bytes = memory::document_bytes(&result, MAX_BYTES, check)? + 96;
            working = working - entry.bytes + bytes;
            if working > MAX_BYTES {
                return Err(limit());
            }
            results.push(result);
        }
        check()?;
        Ok(results)
    }
}

fn update(
    state: &mut State,
    operator: Operator,
    value: Option<BsonValue>,
    budget: &mut Budget<'_>,
) -> EngineResult<(usize, usize)> {
    budget.step()?;
    match state {
        State::Sum(state) => {
            if let Some(value) = value {
                state.add(&value);
            }
            Ok((0, 0))
        }
        State::Average(state) => {
            if let Some(value) = value {
                state.add(&value);
            }
            Ok((0, 0))
        }
        State::One(current) => {
            let retain = match operator {
                Operator::First => current.is_none(),
                Operator::Last => true,
                Operator::Min | Operator::Max => match (&value, &current) {
                    (None | Some(BsonValue::Null), _) => false,
                    (Some(_), None) => true,
                    (Some(value), Some(current)) => {
                        budget.step()?;
                        let order = value.cmp(current);
                        if matches!(operator, Operator::Min) {
                            order.is_le()
                        } else {
                            order.is_ge()
                        }
                    }
                },
                _ => unreachable!("single-value accumulator"),
            };
            if !retain {
                return Ok((0, 0));
            }
            if value.is_none() {
                budget.charge(128)?;
            }
            let value = value.unwrap_or(BsonValue::Null);
            let added = memory::value_bytes(&value, MAX_BYTES, &mut || budget.step())?;
            budget.charge(added)?;
            let removed = current
                .as_ref()
                .map(|value| memory::value_bytes(value, MAX_BYTES, &mut || budget.step()))
                .transpose()?
                .unwrap_or(0);
            *current = Some(value);
            Ok((added, removed))
        }
        State::Push(values) => {
            let Some(value) = value else {
                return Ok((0, 0));
            };
            if values.len() >= 65_536 {
                return Err(limit());
            }
            let bytes = memory::value_bytes(&value, MAX_BYTES, &mut || budget.step())?;
            budget.charge(bytes)?;
            budget.charge(128)?;
            values.push(value);
            Ok((bytes + 128, 0))
        }
        State::Set { values, seen } => {
            let Some(value) = value else {
                return Ok((0, 0));
            };
            let bytes = memory::value_bytes(&value, MAX_BYTES, &mut || budget.step())?;
            budget.step()?;
            if seen.contains(&value) {
                return Ok((0, 0));
            }
            budget.charge(bytes)?;
            if values.len() >= 65_536 {
                return Err(limit());
            }
            budget.charge(192)?;
            let value = Arc::new(value);
            seen.insert(Arc::clone(&value));
            values.push(value);
            Ok((bytes + 192, 0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::{EngineError, EngineErrorKind},
        document::{BsonDecimal128, DocumentAggregator, DocumentPipeline},
    };

    fn doc(entries: &[(&str, BsonValue)]) -> BsonDocument {
        BsonDocument::from_entries(entries.iter().cloned()).unwrap()
    }
    fn stage(key: BsonValue, operators: &[&str], expression: BsonValue) -> BsonDocument {
        let mut spec = doc(&[("_id", key)]);
        for operator in operators {
            spec.push(
                &operator[1..],
                BsonValue::Document(doc(&[(operator, expression.clone())])),
            )
            .unwrap();
        }
        doc(&[("$group", BsonValue::Document(spec))])
    }
    fn plan(stages: Vec<BsonDocument>) -> DocumentAggregator {
        DocumentAggregator::compile(&DocumentPipeline::new(stages).unwrap()).unwrap()
    }
    fn field(value: &str) -> BsonValue {
        BsonValue::String(value.into())
    }

    #[test]
    fn computed_keys_preserve_identity_missing_and_literal_representation() {
        let rows = [
            doc(&[("v", BsonValue::Int64(1))]),
            doc(&[("v", BsonValue::Double(1.0))]),
            doc(&[]),
            doc(&[("v", BsonValue::Null)]),
        ];
        let before: Vec<_> = rows
            .iter()
            .map(|row| encode_document(row).unwrap())
            .collect();
        for (key, expected) in [
            (BsonValue::Int32(1), vec![(BsonValue::Int32(1), 4)]),
            (
                BsonValue::Document(doc(&[("0", field("$v")), ("a.b", field("$missing"))])),
                vec![
                    (BsonValue::Document(doc(&[("0", BsonValue::Int64(1))])), 2),
                    (BsonValue::Document(doc(&[])), 1),
                    (BsonValue::Document(doc(&[("0", BsonValue::Null)])), 1),
                ],
            ),
            (
                BsonValue::Array(vec![field("$v"), field("$missing")]),
                vec![
                    (
                        BsonValue::Array(vec![BsonValue::Int64(1), BsonValue::Null]),
                        2,
                    ),
                    (BsonValue::Array(vec![BsonValue::Null, BsonValue::Null]), 2),
                ],
            ),
            (
                BsonValue::Document(doc(&[("$literal", field("$$REMOVE"))])),
                vec![(field("$$REMOVE"), 4)],
            ),
        ] {
            let runner = plan(vec![stage(key, &["$sum"], BsonValue::Int32(1))]);
            let expected: Vec<_> = expected
                .into_iter()
                .map(|(key, count)| {
                    encode_document(&doc(&[("_id", key), ("sum", BsonValue::Int32(count))]))
                        .unwrap()
                })
                .collect();
            let actual = runner.execute(&rows).unwrap();
            assert_eq!(
                actual
                    .iter()
                    .map(|row| encode_document(row).unwrap())
                    .collect::<Vec<_>>(),
                expected
            );
            let mut stream = runner.into_stream();
            for row in &rows {
                assert!(stream.push(row.clone()).unwrap().is_none());
            }
            assert_eq!(
                stream
                    .finish()
                    .unwrap()
                    .iter()
                    .map(|row| encode_document(row).unwrap())
                    .collect::<Vec<_>>(),
                expected
            );
        }
        assert_eq!(
            rows.iter()
                .map(|row| encode_document(row).unwrap())
                .collect::<Vec<_>>(),
            before
        );
    }

    #[test]
    fn computed_keys_validate_eagerly_and_runtime_failures_poison_the_stream() {
        for key in [
            field("$$REMOVE"),
            BsonValue::Array(vec![field("$$REMOVE")]),
            BsonValue::Document(doc(&[("nested", field("$$ROOT"))])),
        ] {
            let pipeline = DocumentPipeline::new(vec![
                doc(&[("$limit", BsonValue::Int32(1))]),
                stage(key, &[], BsonValue::Null),
            ])
            .unwrap();
            assert_eq!(
                DocumentAggregator::compile(&pipeline).unwrap_err().kind(),
                EngineErrorKind::Unsupported
            );
        }
        let group = stage(
            BsonValue::Document(doc(&[("$size", field("$v"))])),
            &["$sum"],
            BsonValue::Int32(1),
        );
        let rows = [
            doc(&[("v", BsonValue::Array(vec![]))]),
            doc(&[("v", BsonValue::Null)]),
        ];
        let runner = plan(vec![group.clone()]);
        assert!(runner.execute(&rows).is_err());
        let mut stream = runner.into_stream();
        assert!(stream.push(rows[0].clone()).unwrap().is_none());
        assert!(stream.push(rows[1].clone()).is_err());
        assert_eq!(
            stream.finish().unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(
            plan(vec![doc(&[("$limit", BsonValue::Int32(1))]), group])
                .execute(&rows)
                .unwrap()[0]
                .get_first("_id"),
            Some(&BsonValue::Int32(0))
        );
        // Computed key copies consume the same working budget as accumulator
        // expressions, before any oversized key can be retained.
        let key = BsonValue::Array(vec![field("$v"); 80]);
        let mut stream = plan(vec![stage(key, &[], BsonValue::Null)]).into_stream();
        let row = doc(&[("v", BsonValue::String("x".repeat(1024 * 1024)))]);
        assert_eq!(
            stream.push(row).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        assert_eq!(
            stream.finish().unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
    }

    #[test]
    fn group_identity_representations_field_order_missing_and_source_immutability() {
        let rows = [
            doc(&[("k", BsonValue::Int64(1))]),
            doc(&[("k", BsonValue::Double(1.0)), ("v", BsonValue::Int64(7))]),
            doc(&[("k", BsonValue::Int32(1)), ("v", BsonValue::Double(7.0))]),
            doc(&[("v", BsonValue::Null)]),
        ];
        let before: Vec<_> = rows
            .iter()
            .map(|row| encode_document(row).unwrap())
            .collect();
        let stages = vec![stage(
            field("$k"),
            &["$first", "$last", "$push", "$addToSet", "$min", "$max"],
            field("$v"),
        )];
        let runner = plan(stages);
        let actual = runner.execute(&rows).unwrap();
        let expected = doc(&[
            ("_id", BsonValue::Int64(1)),
            ("first", BsonValue::Null),
            ("last", BsonValue::Double(7.0)),
            (
                "push",
                BsonValue::Array(vec![BsonValue::Int64(7), BsonValue::Double(7.0)]),
            ),
            ("addToSet", BsonValue::Array(vec![BsonValue::Int64(7)])),
            ("min", BsonValue::Double(7.0)),
            ("max", BsonValue::Double(7.0)),
        ]);
        assert_eq!(
            encode_document(&actual[0]).unwrap(),
            encode_document(&expected).unwrap()
        );
        assert_eq!(actual[1].get_first("_id"), Some(&BsonValue::Null));
        let mut stream = runner.into_stream();
        for row in rows.iter().cloned() {
            assert!(stream.push(row).unwrap().is_none());
        }
        let streamed = stream.finish().unwrap();
        assert_eq!(
            streamed
                .iter()
                .map(|row| encode_document(row).unwrap())
                .collect::<Vec<_>>(),
            actual
                .iter()
                .map(|row| encode_document(row).unwrap())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            rows.iter()
                .map(|row| encode_document(row).unwrap())
                .collect::<Vec<_>>(),
            before
        );
    }

    #[test]
    fn numeric_promotion_quantum_nan_and_unencodable_reference_integer_boundary() {
        let runner = plan(vec![stage(
            BsonValue::Null,
            &["$sum", "$avg", "$first"],
            field("$v"),
        )]);
        let run = |values: Vec<BsonValue>| {
            runner
                .execute(
                    &values
                        .into_iter()
                        .map(|value| doc(&[("v", value)]))
                        .collect::<Vec<_>>(),
                )
                .unwrap()
                .remove(0)
        };
        for (values, expected) in [
            (
                vec![BsonValue::Int64(i64::MAX), BsonValue::Int32(1)],
                BsonValue::Double(2_f64.powi(63)),
            ),
            (
                vec![BsonValue::Int64(i64::MIN), BsonValue::Int32(-1)],
                BsonValue::Double(-2_f64.powi(63)),
            ),
            (vec![BsonValue::Int64(1)], BsonValue::Int32(1)),
            (
                vec![BsonValue::Boolean(true), BsonValue::Null],
                BsonValue::Int32(0),
            ),
        ] {
            assert!(
                run(values)
                    .get_first("sum")
                    .unwrap()
                    .representation_eq(&expected)
            );
        }
        let result = run(vec![
            BsonValue::Decimal128(BsonDecimal128::parse("1.00").unwrap()),
            BsonValue::Double(2.1),
        ]);
        assert!(
            result
                .get_first("avg")
                .unwrap()
                .representation_eq(&BsonValue::Decimal128(
                    BsonDecimal128::parse("1.550000000000000044408920985006262").unwrap()
                ))
        );
        let result = run(vec![
            BsonValue::Decimal128(BsonDecimal128::parse("1E34").unwrap()),
            BsonValue::Double(-1e16),
        ]);
        assert!(
            result
                .get_first("sum")
                .unwrap()
                .representation_eq(&BsonValue::Decimal128(
                    BsonDecimal128::parse("9999999999999999990000000000000000").unwrap()
                ))
        );
        let nan = f64::from_bits(0xfff8_0000_0000_0123);
        let result = run(vec![
            BsonValue::Double(nan),
            BsonValue::Double(f64::INFINITY),
        ]);
        assert!(
            result
                .get_first("sum")
                .unwrap()
                .representation_eq(&BsonValue::Double(f64::NAN))
        );
        assert!(
            result
                .get_first("avg")
                .unwrap()
                .representation_eq(&BsonValue::Double(f64::NAN))
        );
        assert!(
            result
                .get_first("first")
                .unwrap()
                .representation_eq(&BsonValue::Double(nan))
        );
        // Noncanonical BID coefficients are zero, consistently with shared
        // BSON identity and MongoDB's BID arithmetic. Python's to_decimal()
        // recovers some malformed payloads differently; it is not the oracle
        // for this pre-existing BSON-format boundary. Retain raw input bits.
        for (bits, zero) in [
            ((6176_u128 << 113) | 10_u128.pow(34), "0"),
            (
                (1_u128 << 127) | (6156_u128 << 113) | ((1_u128 << 113) - 1),
                "0E-20",
            ),
            ((3_u128 << 125) | (6176_u128 << 111), "0"),
        ] {
            let value = BsonValue::Decimal128(BsonDecimal128::from_bid(bits.to_le_bytes()));
            let result = run(vec![value.clone()]);
            let expected = BsonValue::Decimal128(BsonDecimal128::parse(zero).unwrap());
            for field in ["sum", "avg"] {
                assert!(
                    result
                        .get_first(field)
                        .unwrap()
                        .representation_eq(&expected)
                );
            }
            assert!(result.get_first("first").unwrap().representation_eq(&value));
        }
    }

    #[test]
    fn first_still_evaluates_later_rows_and_failed_streams_stay_failed() {
        let group = stage(
            BsonValue::Null,
            &["$first"],
            BsonValue::Document(doc(&[("$size", field("$v"))])),
        );
        let rows = [
            doc(&[("v", BsonValue::Array(vec![]))]),
            doc(&[("v", BsonValue::Null)]),
        ];
        let mut stream = plan(vec![group.clone()]).into_stream();
        assert!(stream.push(rows[0].clone()).unwrap().is_none());
        assert!(stream.push(rows[1].clone()).is_err());
        assert_eq!(
            stream.finish().unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        let limit = doc(&[("$limit", BsonValue::Int32(1))]);
        assert_eq!(
            plan(vec![limit.clone(), group.clone()])
                .execute(&rows)
                .unwrap()[0]
                .get_first("first"),
            Some(&BsonValue::Int32(0))
        );
        assert!(plan(vec![group, limit]).execute(&rows).is_err());
    }

    #[test]
    fn group_state_bson_output_specification_and_available_memory_are_bounded() {
        let group = stage(BsonValue::Null, &["$push"], field("$v"));
        let row = doc(&[("v", BsonValue::String("x".repeat(1024 * 1024)))]);
        let mut stream = plan(vec![group.clone()]).into_stream();
        for _ in 0..17 {
            assert!(stream.push(row.clone()).unwrap().is_none());
        }
        assert!(
            stream.finish().is_err(),
            "group result must fit one BSON document"
        );
        let mut stream = plan(vec![group.clone()]).into_stream();
        let mut rejected = false;
        for _ in 0..80 {
            if let Err(error) = stream.push(row.clone()) {
                assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
                rejected = true;
                break;
            }
            assert!(stream.retained_bytes() < MAX_BYTES + 4096);
        }
        assert!(rejected);
        assert_eq!(
            stream.finish().unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        let spec = group.get_first("$group").unwrap();
        let group = Group::compile(&group, spec, &mut || Ok(())).unwrap();
        let mut groups = Groups::default();
        assert_eq!(
            groups
                .push(&group, &row, MAX_BYTES - 1024, &mut || Ok(()))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        let big = stage(
            BsonValue::Null,
            &["$first"],
            BsonValue::String("x".repeat(1024 * 1024)),
        );
        assert_eq!(
            DocumentAggregator::compile(&DocumentPipeline::new(vec![big]).unwrap())
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        let mut many = doc(&[("_id", BsonValue::Null)]);
        for index in 0..4096 {
            many.push(
                format!("v{index}"),
                BsonValue::Document(doc(&[("$first", BsonValue::Int32(1))])),
            )
            .unwrap();
        }
        assert_eq!(
            DocumentAggregator::compile(
                &DocumentPipeline::new(vec![doc(&[("$group", BsonValue::Document(many))])])
                    .unwrap()
            )
            .unwrap_err()
            .kind(),
            EngineErrorKind::LimitExceeded
        );
    }

    #[test]
    fn group_compile_push_and_finish_check_every_cancellation_and_deadline_checkpoint() {
        let pipeline = DocumentPipeline::new(vec![stage(
            BsonValue::Document(doc(&[
                ("key", field("$k")),
                (
                    "array",
                    BsonValue::Array(vec![field("$k"), field("$missing")]),
                ),
            ])),
            &[
                "$sum",
                "$avg",
                "$first",
                "$last",
                "$min",
                "$max",
                "$push",
                "$addToSet",
            ],
            field("$v"),
        )])
        .unwrap();
        let rows = [
            doc(&[("k", BsonValue::Int32(0)), ("v", BsonValue::Int32(1))]),
            doc(&[("k", BsonValue::Int32(0)), ("v", BsonValue::Double(2.1))]),
            doc(&[("k", BsonValue::Int32(1)), ("v", BsonValue::Null)]),
        ];
        for kind in [
            EngineErrorKind::Cancelled,
            EngineErrorKind::DeadlineExceeded,
        ] {
            let mut total = 0;
            let runner = DocumentAggregator::compile_with_check(&pipeline, &mut || {
                total += 1;
                Ok(())
            })
            .unwrap();
            for stop in 1..=total {
                let mut step = 0;
                assert_eq!(
                    DocumentAggregator::compile_with_check(&pipeline, &mut || {
                        step += 1;
                        if step == stop {
                            Err(EngineError::new(kind, "interrupted"))
                        } else {
                            Ok(())
                        }
                    })
                    .unwrap_err()
                    .kind(),
                    kind
                );
            }
            total = 0;
            runner
                .execute_with_check(&rows, &mut || {
                    total += 1;
                    Ok(())
                })
                .unwrap();
            for stop in 1..=total {
                let mut step = 0;
                assert_eq!(
                    runner
                        .execute_with_check(&rows, &mut || {
                            step += 1;
                            if step == stop {
                                Err(EngineError::new(kind, "interrupted"))
                            } else {
                                Ok(())
                            }
                        })
                        .unwrap_err()
                        .kind(),
                    kind
                );
            }
            let execute = |stop: usize| {
                let mut stream = DocumentAggregator::compile(&pipeline)
                    .unwrap()
                    .into_stream();
                let mut step = 0;
                let mut check = || {
                    step += 1;
                    if step == stop {
                        Err(EngineError::new(kind, "interrupted"))
                    } else {
                        Ok(())
                    }
                };
                for row in rows.iter().cloned() {
                    if let Err(error) = stream.push_with_check(row, &mut check) {
                        assert_eq!(
                            stream.finish().unwrap_err().kind(),
                            EngineErrorKind::FailedPrecondition
                        );
                        return (step, Err(error));
                    }
                }
                let result = stream.finish_with_check(&mut check);
                (step, result)
            };
            let (steps, result) = execute(usize::MAX);
            result.unwrap();
            for stop in 1..=steps {
                assert_eq!(execute(stop).1.unwrap_err().kind(), kind);
            }
        }
    }
}
