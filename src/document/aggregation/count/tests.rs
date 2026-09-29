use super::*;
use crate::document::BsonDecimal128;
use proptest::prelude::*;

fn doc(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(entries).unwrap()
}
fn stage(name: &'static str, value: BsonValue) -> BsonDocument {
    doc([(name, value)])
}
fn group(key: BsonValue, operand: BsonValue) -> BsonDocument {
    stage(
        "$group",
        BsonValue::Document(doc([
            ("_id", key),
            ("n", BsonValue::Document(doc([("$sum", operand)]))),
        ])),
    )
}
fn plan(stages: Vec<BsonDocument>) -> DocumentAggregator {
    DocumentAggregator::compile(&DocumentPipeline::new(stages).unwrap()).unwrap()
}
fn count(stages: Vec<BsonDocument>) -> DocumentCountAggregation {
    plan(stages).into_count().expect("count plan")
}

#[test]
fn eligibility_never_rewrites_general_groups_or_reorders_stages() {
    for (key, operand, accepted) in [
        (BsonValue::Null, BsonValue::Int32(1), true),
        (BsonValue::Int32(1), BsonValue::Int64(1), true),
        (BsonValue::from("$field"), BsonValue::Int32(1), false),
        (BsonValue::Null, BsonValue::Int32(2), false),
        (BsonValue::Null, BsonValue::Double(1.0), false),
        (
            BsonValue::Null,
            BsonValue::Decimal128(BsonDecimal128::parse("1").unwrap()),
            false,
        ),
        (BsonValue::Null, BsonValue::from("$field"), false),
    ] {
        assert_eq!(
            plan(vec![group(key, operand)]).into_count().is_ok(),
            accepted
        );
    }
    let terminal = stage("$count", BsonValue::from("n"));
    for prefix in [
        vec![stage(
            "$project",
            BsonValue::Document(doc([("_id", BsonValue::Int32(1))])),
        )],
        vec![stage(
            "$sort",
            BsonValue::Document(doc([("_id", BsonValue::Int32(1))])),
        )],
        vec![
            stage("$limit", BsonValue::Int32(1)),
            stage("$match", BsonValue::Document(doc([]))),
        ],
        vec![
            stage("$match", BsonValue::Document(doc([]))),
            stage("$match", BsonValue::Document(doc([]))),
        ],
        vec![terminal.clone()],
    ] {
        assert!(
            plan(prefix.into_iter().chain([terminal.clone()]).collect())
                .into_count()
                .is_err()
        );
    }
    assert!(plan(vec![]).into_count().is_err());
    let multiple = stage(
        "$group",
        BsonValue::Document(doc([
            ("_id", BsonValue::Null),
            (
                "n",
                BsonValue::Document(doc([("$sum", BsonValue::Int32(1))])),
            ),
            (
                "m",
                BsonValue::Document(doc([("$sum", BsonValue::Int32(1))])),
            ),
        ])),
    );
    assert!(plan(vec![multiple]).into_count().is_err());
}

#[test]
fn scalar_promotion_empty_output_and_literal_key_bytes_are_exact() {
    for total in [
        0,
        1,
        i32::MAX as u64,
        i32::MAX as u64 + 1,
        i64::MAX as u64,
        i64::MAX as u64 + 1,
        u64::MAX,
    ] {
        for terminal in [
            stage("$count", BsonValue::from("n")),
            group(BsonValue::Int32(1), BsonValue::Int64(1)),
        ] {
            let output = count(vec![terminal]).finish_total(total).unwrap();
            if total == 0 {
                assert!(output.is_none());
                continue;
            }
            let expected = if total <= i32::MAX as u64 {
                BsonValue::Int32(total as i32)
            } else if total <= i64::MAX as u64 {
                BsonValue::Int64(total as i64)
            } else {
                BsonValue::Double(total as f64)
            };
            assert_eq!(output.unwrap().get_first("n"), Some(&expected));
        }
    }
    let key = BsonValue::Document(doc([
        ("$private", BsonValue::Double(-0.0)),
        (
            "decimal",
            BsonValue::Decimal128(BsonDecimal128::parse("1.00").unwrap()),
        ),
    ]));
    let literal = BsonValue::Document(doc([("$literal", key.clone())]));
    let actual = count(vec![group(literal, BsonValue::Int32(1))])
        .finish_total(7)
        .unwrap()
        .unwrap();
    assert_eq!(
        encode_document(&actual).unwrap(),
        encode_document(&doc([("_id", key), ("n", BsonValue::Int32(7))])).unwrap()
    );
}

#[test]
fn counts_do_not_have_a_general_aggregation_input_quota() {
    let mut counter = count(vec![stage("$count", BsonValue::from("n"))]);
    let row = doc([]);
    let retained = counter.retained_bytes();
    for _ in 0..75000 {
        counter.push(&row, &mut || Ok(())).unwrap();
    }
    assert_eq!(counter.retained_bytes(), retained);
    assert_eq!(
        counter.finish().unwrap().unwrap().get_first("n"),
        Some(&BsonValue::Int32(75000))
    );
    let ordinary = plan(vec![stage("$count", BsonValue::from("n"))]);
    assert!(
        ordinary.execute(&vec![row; 65537]).is_err(),
        "borrowed/general API still bounded"
    );
}

#[test]
fn filtered_limits_keep_the_matcher_and_stop_when_later_skip_discards_the_row() {
    let matched = stage(
        "$match",
        BsonValue::Document(doc([("yes", BsonValue::Boolean(true))])),
    );
    let terminal = stage("$count", BsonValue::from("n"));
    let mut unbounded = count(vec![matched.clone(), terminal.clone()]);
    assert!(unbounded.uses_native_count());
    let retained = unbounded.retained_bytes();
    let matcher = unbounded.take_source_matcher().unwrap();
    assert_eq!(
        unbounded.retained_bytes() + matcher.retained_bytes(),
        retained
    );
    let mut limited = count(vec![
        matched,
        stage("$limit", BsonValue::Int32(2)),
        stage("$skip", BsonValue::Int32(100)),
        terminal,
    ]);
    assert!(!limited.uses_native_count());
    for i in 0..3 {
        limited
            .push(&doc([("yes", BsonValue::Boolean(i != 0))]), &mut || Ok(()))
            .unwrap();
        assert_eq!(limited.is_input_exhausted(), i == 2);
    }
    assert!(limited.finish().unwrap().is_none());
}

#[test]
fn scalar_push_checks_cancellation_and_overflow() {
    let mut counter = count(vec![stage("$count", BsonValue::from("n"))]);
    let error = counter
        .push(&doc([]), &mut || {
            Err(EngineError::new(EngineErrorKind::Cancelled, "test"))
        })
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Cancelled);
    assert_eq!(counter.count, 0);
    counter.count = u64::MAX;
    assert_eq!(
        counter.push(&doc([]), &mut || Ok(())).unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
}

proptest! {
    #[test]
    fn scalar_and_ordered_stream_windows_match_the_original_executor(
        size in 0usize..100,
        windows in prop::collection::vec((any::<bool>(), 1i32..30), 0..12),
    ) {
        let mut stages: Vec<_> = windows.iter().map(|(limit, n)|
            stage(if *limit { "$limit" } else { "$skip" }, BsonValue::Int32(*n))).collect();
        stages.push(group(BsonValue::Null, BsonValue::Int32(1)));
        let rows = vec![doc([]); size];
        let expected = plan(stages.clone()).execute(&rows).unwrap();
        let scalar: Vec<_> = count(stages.clone()).finish_total(size as u64).unwrap().into_iter().collect();
        prop_assert_eq!(&scalar, &expected);
        let mut stream = count(stages);
        for row in &rows {
            if stream.is_input_exhausted() { break; }
            stream.push(row, &mut || Ok(())).unwrap();
        }
        let streamed: Vec<_> = stream.finish().unwrap().into_iter().collect();
        prop_assert_eq!(streamed, expected);
    }
}
