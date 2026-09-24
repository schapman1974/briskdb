use super::*;

fn query() -> BsonDocument {
    doc([("a", obj([("$exists", BsonValue::Boolean(false))]))])
}

fn index() -> DocumentIndexRequest {
    DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))])).unwrap()
}

pub(super) fn queries() -> Vec<BsonDocument> {
    let mut equality = query();
    equality.push("b", BsonValue::Int32(1)).unwrap();
    let mut membership = query();
    membership
        .push(
            "b",
            obj([(
                "$in",
                BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(2)]),
            )]),
        )
        .unwrap();
    let mut queries = vec![
        equality,
        membership,
        doc([(
            "$and",
            BsonValue::Array(vec![
                BsonValue::Document(query()),
                obj([("enabled", BsonValue::Boolean(true))]),
            ]),
        )]),
    ];
    for path in ["nested.x", "v", "v.score"] {
        queries.push(doc([(path, obj([("$exists", BsonValue::Boolean(false))]))]));
    }
    queries
}

#[tokio::test]
async fn absence_mutations_and_upserts_match_scans_and_survive_reopen() {
    presence::assert_mutations_and_upserts(index(), query(), 5).await;
}

#[tokio::test]
async fn absence_reselects_indexes_between_pages_and_rekeys_newly_present_fields() {
    presence::assert_churn_and_membership_change(
        index(),
        query(),
        5,
        doc([("$set", obj([("a", BsonValue::Int32(1))]))]),
    )
    .await;
}

#[tokio::test]
async fn absence_skips_nonnull_bson_before_reads_and_writes() {
    membership::assert_physical_selection(index(), query(), 5, 2).await;
}

#[tokio::test]
async fn absence_validates_selected_entry_checksums() {
    membership::assert_candidate_checksum(index(), query(), 0, 1).await;
}

#[tokio::test]
#[ignore = "manual same-root field-absence benchmark; timing is not a CI assertion"]
async fn absence_candidate_benchmark() {
    presence::benchmark(false).await;
}
