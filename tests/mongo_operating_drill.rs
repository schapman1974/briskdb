#![cfg(all(feature = "mongo", feature = "tinymongo-import"))]

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use briskdb::{
    BriskDb, DocumentSupport, RequestContext,
    document::{
        DocumentBuildIndexRequest, DocumentCommand, DocumentNamespace, DocumentRequest,
        DocumentRequestId, DocumentWriteOptions,
    },
    import::{TinyMongoImportOptions, TinyMongoImportPlan, import_tinymongo_database},
    protocol::mongo::MongoServer,
};

async fn client(reference: bool, arguments: Vec<String>) {
    let variable = if reference {
        "BRISKDB_MONGO_ORACLE_PYTHON"
    } else {
        "BRISKDB_MONGO_WIRE_PYTHON"
    };
    let python =
        std::env::var(variable).expect("explicit isolated reference/driver interpreter required");
    let output = tokio::task::spawn_blocking(move || {
        Command::new(python)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/mongo_operating_client.py"
            ))
            .args(arguments)
            .output()
            .expect("launch operating drill client")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    print!("{}", String::from_utf8_lossy(&output.stdout));
}

fn tree(root: &Path) -> BTreeMap<PathBuf, blake3::Hash> {
    fn walk(root: &Path, directory: &Path, result: &mut BTreeMap<PathBuf, blake3::Hash>) {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            assert!(!kind.is_symlink());
            if kind.is_dir() {
                walk(root, &entry.path(), result);
            } else {
                assert!(kind.is_file());
                result.insert(
                    entry.path().strip_prefix(root).unwrap().to_owned(),
                    blake3::hash(&fs::read(entry.path()).unwrap()),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    walk(root, root, &mut result);
    result
}

// Test-only complete stopped-directory copy. Never applied to a live root or
// existing destination, and never presented as a production online backup API.
fn stopped_copy(source: &Path, destination: &Path) {
    fs::create_dir(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let kind = entry.file_type().unwrap();
        let target = destination.join(entry.file_name());
        if kind.is_dir() {
            stopped_copy(&entry.path(), &target);
        } else {
            assert!(kind.is_file() && !kind.is_symlink());
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

async fn open(root: &Path) -> (BriskDb, MongoServer) {
    let database = BriskDb::builder(root)
        .with_document_support(DocumentSupport::Enabled)
        .open()
        .await
        .unwrap();
    let server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    assert!(server.readiness().ready());
    (database, server)
}

async fn verify(server: &MongoServer, mode: &str) {
    client(
        false,
        vec![
            mode.to_owned(),
            format!("mongodb://{}/?directConnection=true", server.address()),
        ],
    )
    .await;
}

async fn stop(database: BriskDb, mut server: MongoServer) {
    server.close().await.unwrap();
    database.close().await.unwrap();
    drop(database);
    assert!(server.readiness().engine.is_none());
    assert_eq!(server.metrics().active_connections, 0);
    assert_eq!(server.metrics().cursors.active, 0);
}

#[tokio::test]
#[ignore = "requires separately installed source-locked TinyMongo and stock PyMongo"]
async fn stopped_import_backup_restore_and_source_rollback_preserve_mongo_data() {
    for backend in ["sqlite", "sqlite-sharded"] {
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source");
        let imported = temporary.path().join("imported");
        let backup = temporary.path().join("backup");
        let restored = temporary.path().join("restored");
        client(
            true,
            vec![
                "seed".into(),
                source.to_str().unwrap().into(),
                backend.into(),
            ],
        )
        .await;
        let source_before = tree(&source);
        let source_database = source.join(if backend == "sqlite" {
            "app.sqlite"
        } else {
            "app.sqlite-sharded"
        });
        let target = imported.clone();
        let report = tokio::task::spawn_blocking(move || {
            import_tinymongo_database(
                source_database,
                target,
                &TinyMongoImportPlan::new("app", ["items", "empty"]).unwrap(),
                TinyMongoImportOptions::new(4).unwrap(),
            )
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(report.documents(), 24);
        assert_eq!(report.collections(), 2);
        assert_eq!(report.custom_indexes(), 2);
        assert_eq!(report.target_shards(), Some(4));
        assert_eq!(
            report.source_shards(),
            if backend == "sqlite" { 1 } else { 2 }
        );
        assert_eq!(tree(&source), source_before);

        let (database, server) = open(&imported).await;
        verify(&server, "pending").await;
        let session = database.session();
        for name in ["email_unique", "score_index"] {
            database
                .execute_document(
                    &session,
                    DocumentRequest::new(
                        DocumentRequestId::new([1; 16]).unwrap(),
                        RequestContext::new(),
                        DocumentCommand::BuildIndex(
                            DocumentBuildIndexRequest::new(
                                DocumentNamespace::new("app", "items").unwrap(),
                                name,
                                DocumentWriteOptions::new(),
                            )
                            .unwrap(),
                        ),
                    ),
                )
                .await
                .unwrap();
        }
        drop(session);
        verify(&server, "ready").await;
        stop(database, server).await;

        let imported_before = tree(&imported);
        stopped_copy(&imported, &backup);
        let backup_before = tree(&backup);
        assert_eq!(backup_before, imported_before);
        stopped_copy(&backup, &restored);
        assert_eq!(tree(&restored), backup_before);
        let (database, server) = open(&restored).await;
        verify(&server, "mutate").await;
        stop(database, server).await;
        let (database, server) = open(&restored).await;
        verify(&server, "after-write").await;
        stop(database, server).await;
        assert_eq!(tree(&backup), backup_before);
        assert_eq!(tree(&imported), imported_before);
        assert_eq!(tree(&source), source_before);
        // A partial recovery point must fail closed, not create an empty shard.
        // Only this disposable, stopped test copy is damaged.
        let broken = temporary.path().join("broken-restore");
        stopped_copy(&backup, &broken);
        let missing = broken.join("shards/0001.sqlite");
        assert!(missing.is_file());
        fs::remove_file(&missing).unwrap();
        let error = BriskDb::builder(&broken)
            .with_document_support(DocumentSupport::Enabled)
            .open()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), briskdb::EngineErrorKind::DataCorruption);
        assert!(!missing.exists());
        assert_eq!(tree(&backup), backup_before);
        assert_eq!(tree(&source), source_before);
        // Rollback means reopening the unchanged original, not reverse-importing
        // writes made after cutover. The source must still have the old values.
        client(
            true,
            vec![
                "rollback".into(),
                source.to_str().unwrap().into(),
                backend.into(),
            ],
        )
        .await;
        println!(
            "Completed {backend} source -> four-shard BriskDB -> stopped backup/restore -> unchanged-source rollback drill"
        );
    }
}
