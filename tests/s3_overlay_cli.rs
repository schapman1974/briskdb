//! Exercise the real executable boundary without AWS or persistent resources.
#![cfg(all(unix, feature = "s3-overlay-cli"))]

use briskdb::s3_overlay::{Column, ColumnType, Config, Database, Table};
use object_store::memory::InMemory;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::Write,
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
};

fn fixture() -> tempfile::TempDir {
    let temporary = tempfile::tempdir().unwrap();
    let config = Config::new(
        "unused-private-bucket",
        "us-east-1",
        "test",
        vec![Table {
            name: "items".into(),
            columns: vec![Column {
                name: "id".into(),
                kind: ColumnType::Text,
                nullable: false,
            }],
            primary_key: vec!["id".into()],
            shard_key: "id".into(),
            indexes: vec![],
        }],
    )
    .unwrap();
    // Persist the real ISAM catalog locally. These tests never query tables,
    // so their in-memory heads do not need to exist in S3.
    drop(
        Database::create(
            temporary.path().join("db"),
            config,
            Arc::new(InMemory::new()),
            BTreeMap::new(),
        )
        .unwrap(),
    );
    temporary
}

fn command(program: &str) -> Command {
    let mut command = Command::new(program);
    for name in [
        "BRISKDB_OVERLAY_ROOT",
        "BRISKDB_OVERLAY_PARQUET_PRUNING",
        "BRISKDB_OVERLAY_READ_ONLY",
    ] {
        command.env_remove(name);
    }
    command.env("AWS_EC2_METADATA_DISABLED", "true");
    command
}

fn flag_smoke(program: &str, prefix: &[&str], root: &Path) {
    let output = command(program)
        .args(prefix)
        .args([
            "--root",
            root.to_str().unwrap(),
            "--read-only",
            "--parquet-pruning",
            "false",
            "settings",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["result"]["storage_mode"], "s3-overlay");
    assert_eq!(
        value["result"]["options"],
        json!({"parquet_pruning":false,"read_only":true})
    );
    assert_eq!(value["closed"], true);
    assert_eq!(value["open_stats"]["catalog_root_reads"], 1);
    assert_eq!(value["open_stats"]["catalog_lock_requests"], 1);
    assert!(
        value["open_ms"].as_f64().unwrap() >= value["open_stats"]["total_ms"].as_f64().unwrap()
    );
    assert_eq!(value["read_stats"]["sqlite_base_opens"], 0);

    let output = command(program)
        .args(prefix)
        .args([
            "--root",
            root.to_str().unwrap(),
            "query",
            "--sql",
            "SELECT ? AS answer",
            "--params-json",
            r#"[{"Integer":42}]"#,
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["result"]["rows"], json!([[{"Integer":42}]]));

    let absent = root.parent().unwrap().join("not-created");
    let output = command(program)
        .args(prefix)
        .args([
            "--root",
            absent.to_str().unwrap(),
            "--read-only",
            "execute",
            "--sql",
            "DELETE FROM items",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(error.contains("read-only"), "{error}");
    assert!(!absent.exists());
}

#[test]
fn standalone_flags_and_legacy_json_use_the_same_library() {
    let temporary = fixture();
    let root = temporary.path().join("db");
    let executable = env!("CARGO_BIN_EXE_briskdb-s3-overlay");
    flag_smoke(executable, &[], &root);
    let mut child = command(executable)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(
            json!({
                "action":"query", "root":root, "sql":"SELECT 7", "parquet_pruning":false,
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["result"]["rows"], json!([[{"Integer":7}]]));
    assert_eq!(value["closed"], true);
    assert_eq!(value["open_stats"]["catalog_root_reads"], 1);
    assert_eq!(value["open_stats"]["catalog_lock_requests"], 1);
    assert_eq!(value["read_stats"]["sqlite_base_opens"], 0);
}

#[cfg(feature = "server-cli")]
#[test]
fn main_binary_dispatches_overlay_before_entering_the_daemon_runtime() {
    let temporary = fixture();
    flag_smoke(
        env!("CARGO_BIN_EXE_briskdb"),
        &["overlay"],
        &temporary.path().join("db"),
    );
}
