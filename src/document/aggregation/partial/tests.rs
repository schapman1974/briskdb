use super::*;
use crate::document::BsonDecimal128;
use proptest::prelude::*;

fn doc(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(entries).unwrap()
}
fn group(operator: &str, operand: BsonValue) -> BsonDocument {
    doc([(
        "$group",
        BsonValue::Document(doc([
            ("_id", BsonValue::from("$key")),
            (
                "value",
                BsonValue::Document(BsonDocument::from_entries([(operator, operand)]).unwrap()),
            ),
        ])),
    )])
}
fn pipeline() -> DocumentPipeline {
    let mut fields = doc([("_id", BsonValue::from("$key"))]);
    for (name, operator, value) in [
        ("count", "$sum", BsonValue::Int32(1)),
        ("large", "$sum", BsonValue::Int64(i64::MAX)),
        ("negative", "$sum", BsonValue::Int64(i64::MIN)),
        ("first", "$first", BsonValue::from("$value")),
        ("last", "$last", BsonValue::from("$value")),
        ("min", "$min", BsonValue::from("$value")),
        ("max", "$max", BsonValue::from("$value")),
    ] {
        fields
            .push(
                name,
                BsonValue::Document(BsonDocument::from_entries([(operator, value)]).unwrap()),
            )
            .unwrap();
    }
    DocumentPipeline::new(vec![doc([("$group", BsonValue::Document(fields))])]).unwrap()
}
fn bytes(rows: Vec<BsonDocument>) -> Vec<Vec<u8>> {
    rows.iter()
        .map(|row| encode_document(row).unwrap())
        .collect()
}
fn partitioned(
    pipeline: &DocumentPipeline,
    rows: &[BsonDocument],
    shards: usize,
    reverse: bool,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<Vec<BsonDocument>> {
    let plan = DocumentAggregator::compile(pipeline)
        .unwrap()
        .into_partial();
    let mut budget = DocumentPartialBudget::default();
    let mut partials: Vec<_> = (0..shards).map(|_| budget.groups().unwrap()).collect();
    for (i, row) in rows.iter().enumerate() {
        let shard = (i * 7 + 3) % shards;
        plan.push(
            &mut partials[shard],
            row,
            (i as u64, shard as u16),
            &mut budget,
            check,
        )?;
    }
    if reverse {
        partials.reverse();
    }
    plan.finish(partials, budget, check)
}

#[test]
fn eligibility_never_reorders_numeric_rounding_arrays_or_preceding_stages() {
    for (operator, value, expected) in [
        ("$sum", BsonValue::Int32(1), true),
        ("$sum", BsonValue::Int64(i64::MAX), true),
        ("$sum", BsonValue::Double(1.0), false),
        (
            "$sum",
            BsonValue::Decimal128(BsonDecimal128::parse("1").unwrap()),
            false,
        ),
        ("$sum", BsonValue::from("$value"), false),
        ("$avg", BsonValue::Int32(1), false),
        ("$push", BsonValue::from("$value"), false),
        ("$addToSet", BsonValue::from("$value"), false),
        ("$first", BsonValue::from("$value"), true),
        ("$last", BsonValue::from("$value"), true),
        ("$min", BsonValue::from("$value"), true),
        ("$max", BsonValue::from("$value"), true),
    ] {
        let stage = group(operator, value);
        let plan =
            DocumentAggregator::compile(&DocumentPipeline::new(vec![stage.clone()]).unwrap())
                .unwrap();
        assert_eq!(plan.can_partition(), expected, "{operator}");
        for prefix in [
            doc([("$match", BsonValue::Document(BsonDocument::new()))]),
            doc([("$limit", BsonValue::Int32(1))]),
            doc([("$skip", BsonValue::Int32(1))]),
            doc([(
                "$sort",
                BsonValue::Document(doc([("value", BsonValue::Int32(-1))])),
            )]),
        ] {
            assert!(
                !DocumentAggregator::compile(
                    &DocumentPipeline::new(vec![prefix, stage.clone()]).unwrap()
                )
                .unwrap()
                .can_partition()
            );
        }
    }
}

#[test]
fn partials_preserve_exact_key_and_extrema_tie_representations_and_post_stages() {
    let values = [
        BsonValue::Int64(1),
        BsonValue::Double(1.0),
        BsonValue::Decimal128(BsonDecimal128::parse("1.00").unwrap()),
        BsonValue::Int32(1),
        BsonValue::Double(-0.0),
        BsonValue::Double(0.0),
        BsonValue::Double(f64::from_bits(0xfff8_0000_0000_0011)),
        BsonValue::Double(f64::NAN),
        BsonValue::Null,
        BsonValue::from("é\0x"),
        BsonValue::Document(doc([("x", BsonValue::Int32(1))])),
        BsonValue::Array(vec![BsonValue::Int64(1)]),
    ];
    let mut rows: Vec<_> = values
        .iter()
        .cycle()
        .take(72)
        .enumerate()
        .map(|(i, value)| {
            let mut row = doc([("key", values[i / 6].clone())]);
            if i % 5 != 0 {
                row.push("value", value.clone()).unwrap();
            }
            row
        })
        .collect();
    rows.push(BsonDocument::new());
    for suffix in [
        Vec::new(),
        vec![
            doc([(
                "$sort",
                BsonValue::Document(doc([("count", BsonValue::Int32(-1))])),
            )]),
            doc([("$skip", BsonValue::Int32(1))]),
            doc([("$limit", BsonValue::Int32(4))]),
        ],
    ] {
        let mut stages = pipeline().stages().to_vec();
        stages.extend(suffix);
        let pipeline = DocumentPipeline::new(stages).unwrap();
        let expected = bytes(
            DocumentAggregator::compile(&pipeline)
                .unwrap()
                .execute(&rows)
                .unwrap(),
        );
        for shards in [1, 2, 4, 16, 64] {
            for reverse in [false, true] {
                assert_eq!(
                    bytes(partitioned(&pipeline, &rows, shards, reverse, &mut || Ok(())).unwrap()),
                    expected
                );
            }
        }
        assert!(
            partitioned(&pipeline, &[], 16, true, &mut || Ok(()))
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn all_checkpoints_abort_without_partial_output_and_plans_are_reusable() {
    let rows = vec![
        doc([
            ("key", BsonValue::Int32(1)),
            ("value", BsonValue::from("abc"))
        ]);
        4
    ];
    let mut calls = 0;
    let expected = bytes(
        partitioned(&pipeline(), &rows, 4, true, &mut || {
            calls += 1;
            Ok(())
        })
        .unwrap(),
    );
    for stop in 0..calls {
        let mut count = 0;
        let error = partitioned(&pipeline(), &rows, 4, true, &mut || {
            if count == stop {
                return Err(EngineError::new(EngineErrorKind::Cancelled, "test"));
            }
            count += 1;
            Ok(())
        })
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
    }
    assert_eq!(
        bytes(partitioned(&pipeline(), &rows, 4, true, &mut || Ok(())).unwrap()),
        expected
    );
}

#[test]
fn row_work_and_memory_budgets_are_shared_not_reset_per_shard_or_merge() {
    let plan = DocumentAggregator::compile(&pipeline())
        .unwrap()
        .into_partial();
    let row = doc([
        ("key", BsonValue::Int32(1)),
        ("value", BsonValue::from("x")),
    ]);
    for kind in 0..3 {
        let mut budget = DocumentPartialBudget::default();
        let mut groups = budget.groups().unwrap();
        match kind {
            0 => budget.rows = MAX_ROWS,
            1 => budget.steps = MAX_STEPS,
            _ => budget.bytes = MAX_BYTES,
        }
        assert_eq!(
            plan.push(&mut groups, &row, (0, 0), &mut budget, &mut || Ok(()))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
    }
    let mut budget = DocumentPartialBudget::default();
    let groups = vec![budget.groups().unwrap(), budget.groups().unwrap()];
    budget.steps = MAX_STEPS;
    assert_eq!(
        plan.finish(groups, budget, &mut || Ok(()))
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
}

#[test]
fn simultaneous_shard_states_share_the_actual_retained_payload_ceiling() {
    let plan = DocumentAggregator::compile(&pipeline())
        .unwrap()
        .into_partial();
    let mut budget = DocumentPartialBudget::default();
    let mut groups: Vec<_> = (0..8).map(|_| budget.groups().unwrap()).collect();
    let mut rejected = false;
    for i in 0..32 {
        let row = doc([
            ("key", BsonValue::Int32(i)),
            ("value", BsonValue::from("x".repeat(1024 * 1024))),
        ]);
        let shard = i as usize % groups.len();
        match plan.push(
            &mut groups[shard],
            &row,
            (i as u64, shard as u16),
            &mut budget,
            &mut || Ok(()),
        ) {
            Ok(()) => assert_eq!(
                budget.bytes,
                groups
                    .iter()
                    .map(|group| group.retained_bytes())
                    .sum::<usize>()
            ),
            Err(error) => {
                assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
                assert!(
                    groups
                        .iter()
                        .all(|group| group.retained_bytes() < MAX_BYTES / 2)
                );
                rejected = true;
                break;
            }
        }
    }
    assert!(
        rejected,
        "eight independent quotas would incorrectly accept this workload"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn arrival_independent_partials_equal_the_authoritative_stream(
        input in prop::collection::vec((0i32..8, any::<i64>(), 0u8..5), 0..140),
        shards in 1usize..65,
        reverse in any::<bool>(),
    ) {
        let rows: Vec<_> = input.iter().map(|(key, value, kind)| {
            let key = if kind % 2 == 0 { BsonValue::Int64(i64::from(*key)) } else { BsonValue::Double(f64::from(*key)) };
            let mut row = doc([("key", key)]);
            let value = match kind {
                0 => None, 1 => Some(BsonValue::Null), 2 => Some(BsonValue::Int64(*value)),
                3 => Some(BsonValue::Double(*value as f64)), _ => Some(BsonValue::from(value.to_string())),
            };
            if let Some(value) = value { row.push("value", value).unwrap(); }
            row
        }).collect();
        let pipeline = pipeline();
        let expected = bytes(DocumentAggregator::compile(&pipeline).unwrap().execute(&rows).unwrap());
        prop_assert_eq!(bytes(partitioned(&pipeline, &rows, shards, reverse, &mut || Ok(())).unwrap()), expected);
    }
}
