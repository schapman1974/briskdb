use super::*;

fn alternatives(branches: Vec<BsonDocument>) -> BsonDocument {
    doc([(
        "$or",
        BsonValue::Array(branches.into_iter().map(BsonValue::Document).collect()),
    )])
}

fn query() -> BsonDocument {
    alternatives(vec![
        doc([("a", BsonValue::Int32(1))]),
        doc([(
            "a",
            obj([(
                "$in",
                BsonValue::Array(vec![BsonValue::Int64(2), BsonValue::Double(1.0)]),
            )]),
        )]),
    ])
}

fn index() -> DocumentIndexRequest {
    DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap()
}

pub(super) fn queries() -> Vec<BsonDocument> {
    let mut compound = query();
    compound.push("b", BsonValue::Int32(1)).unwrap();
    let mut queries = vec![
        query(),
        compound,
        alternatives(vec![
            doc([("a", BsonValue::Int32(1)), ("b", BsonValue::Int32(1))]),
            doc([("a", BsonValue::Int32(2)), ("b", BsonValue::Int32(2))]),
        ]),
        doc([(
            "$and",
            BsonValue::Array(vec![
                BsonValue::Document(query()),
                obj([("enabled", BsonValue::Boolean(true))]),
            ]),
        )]),
        alternatives(vec![
            doc([("a", obj([("$exists", BsonValue::Boolean(false))]))]),
            doc([("a", BsonValue::Int32(2))]),
        ]),
        alternatives(vec![
            doc([("a", BsonValue::Int32(1))]),
            doc([("b", BsonValue::Int32(1))]),
        ]),
    ];
    for path in ["nested.x", "v", "v.score"] {
        queries.push(alternatives(vec![
            doc([(path, BsonValue::Int32(1))]),
            doc([(path, BsonValue::Int32(2))]),
        ]));
    }
    queries
}

#[tokio::test]
async fn alternative_mutations_and_upserts_match_scans_without_revisiting_array_matches() {
    presence::assert_mutations_and_upserts(index(), query(), 15).await;
}

#[tokio::test]
async fn alternatives_reselect_indexes_between_pages_and_rekey_out_of_all_branches() {
    presence::assert_churn_and_membership_change(
        index(),
        query(),
        15,
        doc([("$set", obj([("a", BsonValue::Int32(9))]))]),
    )
    .await;
}

#[tokio::test]
async fn alternatives_skip_nonmatching_bson_before_reads_and_writes() {
    membership::assert_physical_selection(index(), query(), 15, 0).await;
}

#[tokio::test]
async fn alternatives_validate_selected_multikey_entry_checksums() {
    membership::assert_candidate_checksum(index(), query(), 5, 2).await;
}

#[tokio::test]
#[ignore = "manual same-root logical candidate benchmark; timing is not a CI assertion"]
async fn logical_candidate_benchmark() {
    membership::benchmark(true).await;
}
