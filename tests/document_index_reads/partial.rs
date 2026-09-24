use super::*;
use briskdb::document::{
    DocumentCandidateKind, DocumentPlan, DocumentReadAccess, DocumentScanReason,
};

fn enabled() -> BsonDocument {
    doc([("enabled", BsonValue::Boolean(true))])
}

fn index(filter: BsonDocument) -> DocumentIndexRequest {
    DocumentIndexRequest::new(doc([("a", BsonValue::Int32(1))]))
        .unwrap()
        .with_partial_filter(DocumentFilter::new(filter).unwrap())
}

fn query() -> BsonDocument {
    doc([
        ("enabled", BsonValue::Boolean(true)),
        (
            "a",
            obj([(
                "$in",
                BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(2)]),
            )]),
        ),
    ])
}

fn logical(op: &'static str, children: Vec<BsonDocument>) -> BsonDocument {
    doc([(
        op,
        BsonValue::Array(children.into_iter().map(BsonValue::Document).collect()),
    )])
}

#[tokio::test]
async fn partial_candidates_match_scans_and_report_only_proven_reduced_work() {
    use DocumentCandidateKind::{Equality, LogicalFinite, NecessaryFinite};
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = ns("partial_proofs");
    seed(&engine, &session, &namespace, 35).await;
    call(
        &engine,
        &session,
        DocumentCommand::Insert(
            DocumentInsertRequest::new(
                namespace.clone(),
                [true, false]
                    .into_iter()
                    .enumerate()
                    .map(|(id, enabled)| {
                        doc([
                            ("_id", BsonValue::Int32(100 + id as i32)),
                            ("enabled", BsonValue::Boolean(enabled)),
                            (
                                "a",
                                BsonValue::Array(vec![BsonValue::Array(vec![
                                    BsonValue::Int32(1),
                                    BsonValue::Int32(2),
                                ])]),
                            ),
                            ("b", BsonValue::Int32(2)),
                        ])
                    })
                    .collect::<Vec<_>>(),
                DocumentWriteOptions::new(),
            )
            .unwrap(),
        ),
    )
    .await;
    let equality = doc([
        ("a", BsonValue::Int32(1)),
        ("enabled", BsonValue::Boolean(true)),
    ]);
    let other = doc([
        ("a", BsonValue::Null),
        ("enabled", BsonValue::Boolean(true)),
    ]);
    let cases = vec![
        (enabled(), equality.clone(), Some(Equality)),
        (enabled(), query(), Some(NecessaryFinite)),
        (
            enabled(),
            logical("$and", vec![enabled(), doc([("a", BsonValue::Int32(1))])]),
            Some(Equality),
        ),
        (
            enabled(),
            logical("$or", vec![equality.clone(), other]),
            Some(LogicalFinite),
        ),
        (
            enabled(),
            doc([
                ("enabled", BsonValue::Boolean(true)),
                ("a", obj([("$exists", BsonValue::Boolean(false))])),
            ]),
            Some(NecessaryFinite),
        ),
        (enabled(), doc([("a", BsonValue::Int32(1))]), None),
        (
            enabled(),
            logical("$or", vec![equality.clone(), doc([("a", BsonValue::Null)])]),
            None,
        ),
        (
            enabled(),
            doc([
                ("a", BsonValue::Int32(1)),
                ("enabled", obj([("$ne", BsonValue::Boolean(false))])),
            ]),
            None,
        ),
        (
            enabled(),
            doc([
                ("a", BsonValue::Int32(1)),
                (
                    "enabled",
                    obj([("$in", BsonValue::Array(vec![BsonValue::Boolean(true)]))]),
                ),
            ]),
            None,
        ),
        (
            doc([("b", BsonValue::Int32(2))]),
            doc([("a", BsonValue::Int32(1)), ("b", BsonValue::Int32(2))]),
            Some(Equality),
        ),
        (
            doc([("b", BsonValue::Int32(2))]),
            doc([("a", BsonValue::Int32(1)), ("b", BsonValue::Double(2.0))]),
            None,
        ),
        (
            doc([("b", obj([("$gt", BsonValue::Int32(0))]))]),
            doc([("a", BsonValue::Int32(1)), ("b", BsonValue::Int32(2))]),
            None,
        ),
        (
            doc([("b", obj([("$exists", BsonValue::Boolean(true))]))]),
            doc([
                ("a", BsonValue::Int32(1)),
                ("b", obj([("$exists", BsonValue::Boolean(true))])),
            ]),
            Some(Equality),
        ),
        (
            logical("$and", vec![enabled(), doc([("b", BsonValue::Int32(2))])]),
            equality.clone(),
            None,
        ),
        (
            logical("$or", vec![enabled(), doc([("b", BsonValue::Int32(2))])]),
            equality,
            Some(Equality),
        ),
    ];
    for (filter, query, expected_kind) in cases {
        let options = DocumentReadOptions::new()
            .with_plan_diagnostics(true)
            .with_execution_stats(true);
        let command = || {
            DocumentCommand::Find(DocumentFindRequest::new(
                namespace.clone(),
                DocumentFilter::new(query.clone()).unwrap(),
                options.clone(),
            ))
        };
        let scan = call(&engine, &session, command()).await;
        let scanned = scan.read_stats().unwrap().documents_examined();
        let expected = page(scan).1;
        build(&engine, &session, &namespace, index(filter.clone())).await;
        let indexed = call(&engine, &session, command()).await;
        let Some(DocumentPlan::Scatter(plan)) = indexed.plan() else {
            panic!("scatter")
        };
        match expected_kind {
            Some(kind) => {
                assert!(
                    matches!(plan.read_access(), Some(DocumentReadAccess::IndexCandidates { kind: actual, .. }) if actual == kind),
                    "filter={filter:?} query={query:?}"
                );
                assert!(indexed.read_stats().unwrap().documents_examined() < scanned);
            }
            None => {
                assert_eq!(
                    plan.read_access(),
                    Some(DocumentReadAccess::Scan {
                        reason: DocumentScanReason::NoSafeProbe
                    })
                );
                assert_eq!(indexed.read_stats().unwrap().documents_examined(), scanned);
            }
        }
        assert_eq!(
            page(indexed).1,
            expected,
            "filter={filter:?} query={query:?}"
        );
        assert_eq!(
            find(
                &engine,
                &session,
                &namespace,
                &query,
                DocumentReadOptions::new().with_batch_size(1).unwrap()
            )
            .await,
            expected
                .iter()
                .map(|row| encode_document(row).unwrap())
                .collect::<Vec<_>>()
        );
        drop_index(&engine, &session, &namespace).await;
    }
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn partial_mutations_and_upserts_match_scans_and_survive_reopen() {
    presence::assert_mutations_and_upserts(index(enabled()), query(), 7).await;
}

#[tokio::test]
async fn partial_cursor_reselects_authority_and_updates_out_of_membership() {
    presence::assert_churn_and_membership_change(
        index(enabled()),
        query(),
        7,
        doc([("$set", obj([("enabled", BsonValue::Boolean(false))]))]),
    )
    .await;
}

#[tokio::test]
async fn partial_candidates_skip_excluded_records_before_reads_and_writes() {
    membership::assert_physical_selection(index(enabled()), query(), 7, 3).await;
}

#[tokio::test]
async fn partial_candidates_validate_selected_multikey_entry_checksums() {
    membership::assert_candidate_checksum(index(enabled()), query(), 12, 2).await;
}
