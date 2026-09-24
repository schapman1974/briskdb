#![cfg(feature = "mongo")]

use briskdb::{BriskDb, DocumentSupport, protocol::mongo::MongoServer};
use std::process::Command;

/// Executes the entire unchanged frozen corpus through the BriskDB PyMongo adapter.
/// The candidate artifact remains separate from the immutable reference report.
#[tokio::test]
#[ignore = "requires the frozen Mongo contract Python dependencies"]
async fn frozen_supported_contracts_against_real_briskdb_endpoint() {
    let root = tempfile::tempdir().unwrap();
    let database = BriskDb::builder(root.path())
        .with_shard_count(4)
        .with_document_support(DocumentSupport::Enabled)
        .open()
        .await
        .unwrap();
    let mut server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let uri = format!("mongodb://{}/?directConnection=true", server.address());
    let report_root = tempfile::tempdir().unwrap();
    let report = std::env::var_os("BRISKDB_MONGO_CONTRACT_REPORT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| report_root.path().join("candidate-full.xml"));
    let output = tokio::task::spawn_blocking(move || run_contract(&uri, &report))
        .await
        .unwrap();
    server.close().await.unwrap();
    database.close().await.unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    println!("{}", String::from_utf8_lossy(&output.stdout));
}

fn run_contract(uri: &str, report: &std::path::Path) -> std::process::Output {
    let python =
        std::env::var("BRISKDB_MONGO_CONTRACT_PYTHON").unwrap_or_else(|_| "python3".to_owned());
    let output = Command::new(&python)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "-c",
            "/dev/null",
        ])
        .arg("--rootdir")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("compat/mongo/v1/runner/contracts")
        .args([
            "--mongo-contract-target=briskdb",
            "--mongo-contract-api=both",
            "--mongo-contract-require-target",
            "--mongo-contract-briskdb-uri",
            uri,
        ])
        .arg("--junitxml")
        .arg(report)
        .output()
        .expect("launch frozen contract runner");
    if output.status.success() {
        // Require the exact locked case/API set, not just pytest's exit code:
        // accidental filtering, skips, or duplicated cases must fail the gate.
        let validation = Command::new(python)
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .args([
                "-c",
                r#"
import json
import sys
from pathlib import Path
from scripts.mongo_parity import ingest_junit

with open('compat/mongo/v1/corpus.json', encoding='utf-8') as source:
    corpus = json.load(source)
expected = {(case['id'], api) for case in corpus['cases'] for api in case['apis']}
executions = ingest_junit(Path(sys.argv[1]), 'briskdb', corpus)['executions']
actual = {(item['case_id'], item['api']) for item in executions}
assert len(expected) == len(executions) == 456, ('locked suite coverage changed', len(expected), len(executions))
assert actual == expected, 'candidate suite omitted or substituted locked cases'
assert all(item['outcome'] == 'passed' and item['target'] == 'briskdb-briskdb'
           for item in executions), 'candidate suite skipped or failed a case'
print('Verified all 456 exact frozen candidate executions, with no skips.')
"#,
            ])
            .arg(report)
            .output()
            .expect("validate candidate JUnit coverage");
        if !validation.status.success() {
            return validation;
        }
        println!("{}", String::from_utf8_lossy(&validation.stdout));
    }
    output
}
