use super::*;
use briskdb::document::BsonDecimal128;

fn group() -> BsonValue {
    let mut fields = doc(&[("_id", BsonValue::from("$group"))]);
    for (name, operator, operand) in [
        ("count", "$sum", BsonValue::Int32(1)),
        ("large", "$sum", BsonValue::Int64(i64::MAX)),
        ("first", "$first", BsonValue::from("$value")),
        ("last", "$last", BsonValue::from("$value")),
        ("min", "$min", BsonValue::from("$value")),
        ("max", "$max", BsonValue::from("$value")),
    ] {
        fields
            .push(name, BsonValue::Document(doc(&[(operator, operand)])))
            .unwrap();
    }
    BsonValue::Document(fields)
}

#[tokio::test]
async fn exact_partial_groups_match_ordered_scan_with_zero_batch_paging_and_reopen() {
    let mut documents = source_rows();
    for (i, document) in documents.iter_mut().enumerate() {
        *document = doc(&[
            ("_id", BsonValue::Int32(i as i32)),
            (
                "group",
                if i % 2 == 0 {
                    BsonValue::Int64((i % 7) as i64)
                } else {
                    BsonValue::Double((i % 7) as f64)
                },
            ),
            (
                "value",
                match i % 8 {
                    0 => BsonValue::Null,
                    1 => BsonValue::Int64(1),
                    2 => BsonValue::Double(1.0),
                    3 => BsonValue::Decimal128(BsonDecimal128::parse("1.00").unwrap()),
                    4 => BsonValue::Double(-0.0),
                    5 => BsonValue::Double(f64::NAN),
                    6 => BsonValue::Array(vec![BsonValue::Int32(1)]),
                    _ => BsonValue::from("é\0x"),
                },
            ),
        ]);
    }
    let (root, engine) = setup(documents.clone()).await;
    for reopened in [false, true] {
        let opened;
        let engine = if reopened {
            engine.shutdown().await.unwrap();
            opened = Engine::open(root.path(), 4).await.unwrap();
            &opened
        } else {
            &engine
        };
        let session = engine.session();
        for suffix in [
            Vec::new(),
            vec![
                (
                    "$sort",
                    BsonValue::Document(doc(&[("count", BsonValue::Int32(-1))])),
                ),
                ("$skip", BsonValue::Int32(1)),
                ("$limit", BsonValue::Int32(4)),
            ],
        ] {
            let mut stages = vec![("$group", group())];
            stages.extend(suffix);
            let plan = pipeline(&stages);
            let expected = DocumentAggregator::compile(&plan)
                .unwrap()
                .execute(&documents)
                .unwrap();
            for size in [0, 1, 100] {
                let result = call(
                    engine,
                    &session,
                    aggregate(
                        plan.clone(),
                        DocumentReadOptions::new().with_batch_size(size).unwrap(),
                    ),
                )
                .await;
                assert_eq!(
                    encoded(&drain(engine, &session, result, 2).await),
                    encoded(&expected)
                );
            }
            let mut ordered = vec![("$match", BsonValue::Document(BsonDocument::new()))];
            ordered.extend(stages);
            let result = call(
                engine,
                &session,
                aggregate(pipeline(&ordered), DocumentReadOptions::new()),
            )
            .await;
            assert_eq!(
                encoded(&drain(engine, &session, result, 2).await),
                encoded(&expected)
            );
        }
        if reopened {
            engine.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn partial_group_result_limit_failure_does_not_publish_or_leak_a_cursor() {
    let (_root, engine) = setup(source_rows()).await;
    let session = engine.session();
    let command = || aggregate(pipeline(&[("$group", group())]), DocumentReadOptions::new());
    let error = engine
        .execute_document(
            &session,
            request(
                command(),
                RequestContext::new().with_result_limits(ResultLimits::new(1, 1024).unwrap()),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    let (cursor, rows) = cursor(call(&engine, &session, command()).await);
    assert!(cursor.is_none());
    assert_eq!(rows.len(), 3);
    engine.shutdown().await.unwrap();
}
