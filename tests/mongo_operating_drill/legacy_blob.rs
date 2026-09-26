use super::*;
use briskdb::import::TinyMongoSourceVariant;

async fn blob_client(reference: bool, arguments: Vec<String>) {
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
                "/tests/mongo_legacy_blob_client.py"
            ))
            .args(arguments)
            .output()
            .expect("launch legacy-blob import client")
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

#[tokio::test]
#[ignore = "requires separately installed source-locked TinyMongo and stock PyMongo"]
async fn genuine_legacy_blob_import_preserves_tagged_values_and_unchanged_source() {
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("source");
    let imported = temporary.path().join("imported");
    blob_client(true, vec!["seed".into(), source.to_str().unwrap().into()]).await;
    let before = tree(&source);
    let source_database = source.join("app.sqlite");
    let destination = imported.clone();
    let report = tokio::task::spawn_blocking(move || {
        import_tinymongo_database(
            source_database,
            destination,
            &TinyMongoImportPlan::new("app", ["users", "events", "empty"]).unwrap(),
            TinyMongoImportOptions::new(4).unwrap(),
        )
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(report.variant(), TinyMongoSourceVariant::LegacySingleRow);
    assert_eq!(report.documents(), 3);
    assert_eq!(report.collections(), 3);
    assert_eq!(report.target_shards(), Some(4));
    assert_eq!(tree(&source), before);
    for mode in ["mutate", "reopen"] {
        let (database, server) = open(&imported).await;
        blob_client(
            false,
            vec![
                mode.into(),
                format!("mongodb://{}/?directConnection=true", server.address()),
            ],
        )
        .await;
        stop(database, server).await;
        assert_eq!(tree(&source), before);
    }
    blob_client(
        true,
        vec!["source-check".into(), source.to_str().unwrap().into()],
    )
    .await;
    assert_eq!(tree(&source), before);
}
