//! Process-death recovery at every input/shard commit in a bounded write batch.

use std::{
    io::Read,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use crate::{
    core::{Engine, EngineErrorKind, RequestContext},
    document::{
        BsonDocument, BsonValue, DocumentCollectionOptions, DocumentCommand,
        DocumentCreateCollectionRequest, DocumentCreateIndexesRequest, DocumentDeleteRequest,
        DocumentExecution, DocumentFilter, DocumentFindRequest, DocumentIndexRequest,
        DocumentInsertRequest, DocumentMutationScope, DocumentNamespace, DocumentPlan,
        DocumentReadAccess, DocumentReadOptions, DocumentRequest, DocumentRequestId,
        DocumentResult, DocumentUpdate, DocumentUpdateRequest, DocumentWriteOptions,
        encode_document,
    },
};

fn doc(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(entries).unwrap()
}

fn namespace() -> DocumentNamespace {
    DocumentNamespace::new("recovery", "items").unwrap()
}

#[derive(Clone, Copy)]
struct Row {
    id: i32,
    shard: u16,
    ordinal: i32,
}

impl Row {
    fn value(self, updated: bool) -> i32 {
        self.ordinal * 100 + 10 + if updated { 10_000 } else { 0 }
    }

    fn document(self, updated: bool) -> BsonDocument {
        let version = i32::from(updated);
        doc([
            ("_id", BsonValue::Int32(self.id)),
            ("v", BsonValue::Int32(self.value(updated))),
            ("version", BsonValue::Int32(version)),
            (
                "tags",
                BsonValue::Array(vec![
                    BsonValue::Int32(version * 2),
                    BsonValue::Int32(version * 2 + 1),
                ]),
            ),
        ])
    }
}

fn rows(engine: &Engine) -> Vec<Row> {
    let mut next = 0;
    // Input order differs from shard order; shard 1 is empty and others uneven.
    [3, 0, 2, 0, 3, 0]
        .into_iter()
        .enumerate()
        .map(|(ordinal, shard)| {
            let id = (next..10_000)
                .find(|id| {
                    engine
                        .inner
                        .database
                        .storage
                        .prepare_document_id(&BsonValue::Int32(*id))
                        .unwrap()
                        .1
                        == shard
                })
                .unwrap();
            next = id + 1;
            Row {
                id,
                shard,
                ordinal: ordinal as i32,
            }
        })
        .collect()
}

async fn execute(engine: &Engine, command: DocumentCommand) -> DocumentExecution {
    tokio::time::timeout(
        Duration::from_secs(15),
        engine.execute_document(
            &engine.session(),
            DocumentRequest::new(
                DocumentRequestId::new([1; 16]).unwrap(),
                RequestContext::new().with_deadline(Instant::now() + Duration::from_secs(10)),
                command,
            ),
        ),
    )
    .await
    .expect("bounded document request")
    .unwrap()
}

fn command(mode: &str, rows: &[Row], ordered: bool) -> DocumentCommand {
    // Unordered is an insert-batch option, not a native update/delete option.
    let options = DocumentWriteOptions::new().with_ordered(ordered || mode != "insert");
    match mode {
        "insert" => DocumentCommand::Insert(
            DocumentInsertRequest::new(
                namespace(),
                rows.iter()
                    .map(|row| row.document(false))
                    .collect::<Vec<_>>(),
                options,
            )
            .unwrap(),
        ),
        "update" => DocumentCommand::Update(DocumentUpdateRequest::new(
            namespace(),
            DocumentFilter::new(doc([("version", BsonValue::Int32(0))])).unwrap(),
            DocumentUpdate::new(doc([
                (
                    "$inc",
                    BsonValue::Document(doc([
                        ("v", BsonValue::Int32(10_000)),
                        ("version", BsonValue::Int32(1)),
                    ])),
                ),
                (
                    "$set",
                    BsonValue::Document(doc([(
                        "tags",
                        BsonValue::Array(vec![BsonValue::Int32(2), BsonValue::Int32(3)]),
                    )])),
                ),
            ]))
            .unwrap(),
            DocumentMutationScope::Many,
            options,
        )),
        "delete" => DocumentCommand::Delete(DocumentDeleteRequest::new(
            namespace(),
            DocumentFilter::empty(),
            DocumentMutationScope::Many,
            options,
        )),
        _ => panic!("unknown crash test mode"),
    }
}

async fn setup(root: &Path, mode: &str) -> Vec<Row> {
    let engine = Engine::open(root, 4).await.unwrap();
    let rows = rows(&engine);
    execute(
        &engine,
        DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
            namespace(),
            DocumentCollectionOptions::empty(),
            DocumentWriteOptions::new(),
        )),
    )
    .await;
    execute(
        &engine,
        DocumentCommand::CreateIndexes(
            DocumentCreateIndexesRequest::new(
                namespace(),
                vec![
                    DocumentIndexRequest::new(doc([("v", BsonValue::Int32(1))]))
                        .unwrap()
                        .with_unique(true),
                    DocumentIndexRequest::new(doc([("tags", BsonValue::Int32(1))])).unwrap(),
                ],
                DocumentWriteOptions::new(),
            )
            .unwrap(),
        ),
    )
    .await;
    if mode != "insert" {
        execute(&engine, command("insert", &rows, true)).await;
    }
    engine.shutdown().await.unwrap();
    rows
}

#[tokio::test]
async fn mongo_write_crash_child() {
    let Ok(root) = std::env::var("BRISKDB_TEST_MONGO_WRITE_ROOT") else {
        return;
    };
    let engine = Engine::open(root, 4).await.unwrap();
    let mode = std::env::var("BRISKDB_TEST_MONGO_WRITE_MODE").unwrap();
    execute(&engine, command(&mode, &rows(&engine), true)).await;
    panic!("configured commit checkpoint was not reached");
}

struct ReapedChild(Child);

impl Drop for ReapedChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn crash(root: &Path, mode: &str, phase: &str, target: usize) {
    let mut child = ReapedChild(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "core::engine::document_engine::write_recovery::mongo_write_crash_child",
                "--nocapture",
            ])
            .env("BRISKDB_TEST_MONGO_WRITE_ROOT", root)
            .env("BRISKDB_TEST_MONGO_WRITE_MODE", mode)
            .env(
                "BRISKDB_TEST_MONGO_COMMIT_CRASH",
                format!("{phase}:{target}"),
            )
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "{mode}/{phase}/{target} timed out"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    let mut error = String::new();
    child
        .0
        .stderr
        .take()
        .unwrap()
        .take(64 * 1024)
        .read_to_string(&mut error)
        .unwrap();
    assert_eq!(status.code(), Some(73), "{mode}/{phase}/{target}: {error}");
}

async fn assert_query(engine: &Engine, filter: BsonDocument, expected: &[BsonDocument]) {
    let indexed = !filter.is_empty();
    let result = execute(
        engine,
        DocumentCommand::Find(DocumentFindRequest::new(
            namespace(),
            DocumentFilter::new(filter).unwrap(),
            DocumentReadOptions::new()
                .with_plan_diagnostics(true)
                .with_batch_size(100)
                .unwrap(),
        )),
    )
    .await;
    if indexed {
        assert!(matches!(result.plan(), Some(DocumentPlan::Scatter(plan))
            if matches!(plan.read_access(), Some(DocumentReadAccess::IndexCandidates { .. }))));
    }
    let DocumentResult::Cursor(batch) = result.result() else {
        panic!("expected cursor")
    };
    assert!(batch.cursor_id().is_none());
    let bytes = |documents: &[BsonDocument]| {
        documents
            .iter()
            .map(|document| encode_document(document).unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(bytes(batch.documents()), bytes(expected));
}

async fn assert_state(engine: &Engine, rows: &[Row], expected: &[BsonDocument]) {
    assert_query(engine, doc([]), expected).await;
    // Probe both old and new keys, including absent keys, through Ready indexes.
    for (field, value) in rows
        .iter()
        .flat_map(|row| [row.value(false), row.value(true)])
        .map(|value| ("v", value))
        .chain((0..4).map(|value| ("tags", value)))
    {
        let selected: Vec<_> = expected
            .iter()
            .filter(|document| match document.get_first(field).unwrap() {
                BsonValue::Array(values) => values.contains(&BsonValue::Int32(value)),
                actual => actual == &BsonValue::Int32(value),
            })
            .cloned()
            .collect();
        assert_query(engine, doc([(field, BsonValue::Int32(value))]), &selected).await;
    }
    assert_eq!(engine.readiness().active_schema_operations(), 0);
    assert!(
        engine
            .pool_snapshot_for_test()
            .unwrap()
            .shards
            .iter()
            .all(|shard| shard.active == 0 && shard.queued == 0)
    );
}

async fn recover(root: &Path, mode: &str, rows: &[Row], committed: usize) {
    let engine = Engine::open(root, 4).await.unwrap();
    let expected: Vec<_> = rows
        .iter()
        .filter(|row| match mode {
            "insert" => (row.ordinal as usize) < committed,
            "delete" => usize::from(row.shard) >= committed,
            _ => true,
        })
        .map(|row| row.document(mode == "update" && usize::from(row.shard) < committed))
        .collect();
    assert_state(&engine, rows, &expected).await;
    for attempt in 0..2 {
        let result = execute(&engine, command(mode, rows, false)).await;
        match result.result() {
            DocumentResult::Insert(insert) => {
                let duplicates = if attempt == 0 { committed } else { rows.len() };
                assert_eq!(insert.inserted_ids().len(), rows.len() - duplicates);
                assert_eq!(insert.write_errors().len(), duplicates);
                for (index, error) in insert.write_errors().iter().enumerate() {
                    assert_eq!(error.index(), index);
                    assert_eq!(error.kind(), EngineErrorKind::UniqueViolation);
                }
            }
            DocumentResult::Update(update) => {
                let remaining = if attempt == 0 {
                    rows.iter()
                        .filter(|row| usize::from(row.shard) >= committed)
                        .count()
                } else {
                    0
                };
                assert_eq!(update.matched_count(), remaining as u64);
                assert_eq!(update.modified_count(), remaining as u64);
            }
            DocumentResult::Delete(delete) => {
                assert_eq!(
                    delete.deleted_count(),
                    if attempt == 0 {
                        expected.len() as u64
                    } else {
                        0
                    }
                );
            }
            _ => panic!("expected write result"),
        }
    }
    let final_rows: Vec<_> = if mode == "delete" {
        vec![]
    } else {
        rows.iter()
            .map(|row| row.document(mode == "update"))
            .collect()
    };
    assert_state(&engine, rows, &final_rows).await;
    engine.shutdown().await.unwrap();
    drop(engine);
    let reopened = Engine::open(root, 4).await.unwrap();
    assert_state(&reopened, rows, &final_rows).await;
    reopened.shutdown().await.unwrap();
}

#[test]
fn every_batch_commit_recovers_records_indexes_and_conditional_retries() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for mode in ["insert", "update", "delete"] {
        let boundaries = if mode == "insert" { 6 } else { 4 };
        for phase in ["before", "after"] {
            for target in 1..=boundaries {
                eprintln!("recovering {mode}/{phase}/{target}");
                let root = tempfile::tempdir().unwrap();
                let rows = runtime.block_on(setup(root.path(), mode));
                crash(root.path(), mode, phase, target);
                let committed = target - 1 + usize::from(phase == "after");
                runtime.block_on(recover(root.path(), mode, &rows, committed));
            }
        }
    }
}
