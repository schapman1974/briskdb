use super::*;
use proptest::{prelude::*, test_runner::TestRunner};

fn id() -> BoxedStrategy<BsonValue> {
    (-4_i32..5, 0_u8..9)
        .prop_map(|(number, kind)| match kind {
            0 => BsonValue::Int32(number),
            1 => BsonValue::Int64(i64::from(number)),
            2 => BsonValue::Double(f64::from(number)),
            3 => BsonValue::Decimal128(BsonDecimal128::parse(&number.to_string()).unwrap()),
            4 => BsonValue::from(format!("id{number}")),
            5 => BsonValue::Boolean(number >= 0),
            6 => BsonValue::Null,
            7 => BsonValue::Document(doc([("part", BsonValue::Int32(number))])),
            _ => BsonValue::Array(vec![BsonValue::Int32(number)]),
        })
        .boxed()
}

fn predicate() -> BoxedStrategy<BsonDocument> {
    let leaf = prop_oneof![
        id().prop_map(|value| doc([("_id", value)])),
        id().prop_map(|value| doc([("_id", BsonValue::Document(doc([("$eq", value)])))])),
        id().prop_map(|value| doc([("_id", BsonValue::Document(doc([("$ne", value)])))])),
        prop::collection::vec(id(), 0..8).prop_map(|values| doc([(
            "_id",
            BsonValue::Document(doc([("$in", BsonValue::Array(values))]))
        )])),
        (-4_i32..5).prop_map(|value| doc([("rank", BsonValue::Int32(value))])),
        (-4_i32..5).prop_map(|value| doc([("_id.part", BsonValue::Int32(value))])),
        (-4_i32..5).prop_map(|value| doc([(
            "items",
            BsonValue::Document(doc([(
                "$elemMatch",
                BsonValue::Document(doc([("_id", BsonValue::Int32(value))]))
            )]))
        )])),
        Just(doc([(
            "_id",
            BsonValue::Document(doc([("$regex", BsonValue::from("^id"))]))
        )])),
        Just(BsonDocument::new()),
    ];
    leaf.prop_recursive(3, 32, 4, |inner| {
        (0_u8..3, prop::collection::vec(inner, 1..4)).prop_map(|(kind, clauses)| {
            let operator = ["$and", "$or", "$nor"][usize::from(kind)];
            doc([(
                operator,
                BsonValue::Array(clauses.into_iter().map(BsonValue::Document).collect()),
            )])
        })
    })
    .boxed()
}

fn records() -> Vec<BsonDocument> {
    let mut records = Vec::new();
    for number in -4..5 {
        for id in [
            BsonValue::Int32(number),
            BsonValue::Int64(i64::from(number)),
            BsonValue::Double(f64::from(number)),
            BsonValue::Decimal128(BsonDecimal128::parse(&number.to_string()).unwrap()),
            BsonValue::from(format!("id{number}")),
            BsonValue::Boolean(number >= 0),
            BsonValue::Null,
            BsonValue::Document(doc([("part", BsonValue::Int32(number))])),
            BsonValue::Array(vec![BsonValue::Int32(number)]),
        ] {
            records.push(doc([
                ("_id", id),
                ("rank", BsonValue::Int32(number)),
                (
                    "items",
                    BsonValue::Array(vec![BsonValue::Document(doc([(
                        "_id",
                        BsonValue::Int32(-number),
                    )]))]),
                ),
            ]));
        }
    }
    records
}

#[test]
fn generated_logical_id_routes_never_prune_an_authoritative_match() {
    let rows = records();
    let token = CancellationToken::new();
    let control = OperationControl::new(None);
    for shards in [2, 3, 8, 64] {
        let root = tempfile::tempdir().unwrap();
        let storage = Storage::open(root.path(), shards).unwrap();
        let owners: Vec<_> = rows
            .iter()
            .map(|row| {
                storage
                    .prepare_document_id(row.get_first("_id").unwrap())
                    .unwrap()
                    .1
            })
            .collect();
        let mut runner = TestRunner::new(ProptestConfig {
            cases: 256,
            source_file: Some(file!()),
            ..ProptestConfig::default()
        });
        runner
            .run(&predicate(), |query| {
                let before = crate::document::encode_document(&query).unwrap();
                let matcher = DocumentMatcher::compile(&query).unwrap();
                let filter = DocumentFilter::new(query.clone()).unwrap();
                let routed = proven_id_shards(&storage, &filter, &token, &control).unwrap();
                prop_assert_eq!(
                    routed,
                    proven_id_shards(&storage, &filter, &token, &control).unwrap()
                );
                if let Some(bitmap) = routed {
                    prop_assert_ne!(bitmap, 0);
                    if shards < 64 {
                        prop_assert_eq!(bitmap >> shards, 0);
                    }
                    for (row, owner) in rows.iter().zip(&owners) {
                        if matcher.matches(row).unwrap() {
                            prop_assert_ne!(
                                bitmap & (1_u64 << owner),
                                0,
                                "lost a matching record on shard {} from {} shards",
                                owner,
                                shards
                            );
                        }
                    }
                }
                prop_assert_eq!(crate::document::encode_document(&query).unwrap(), before);
                Ok(())
            })
            .unwrap();
    }
}
