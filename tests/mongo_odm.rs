#![cfg(feature = "mongo")]

use std::{
    io::Read,
    path::PathBuf,
    process::{Command, Output, Stdio},
    time::{Duration, Instant},
};

use briskdb::{BriskDb, DocumentSupport, protocol::mongo::MongoServer};

#[tokio::test]
#[ignore = "requires pinned real ODMs and source-locked upstream application fixtures"]
async fn unchanged_odm_applications_use_real_pymongo_before_and_after_restart() {
    run_application(
        "mongo_odm_client.py",
        "BRISKDB_MONGO_ODM_REPORT_DIR",
        Duration::from_secs(60),
    )
    .await;
}

#[tokio::test]
#[ignore = "requires pinned PyMongo/pytest and unchanged source-locked Talk Python fixtures"]
async fn unchanged_talkpython_contracts_use_real_pymongo_before_and_after_restart() {
    run_application(
        "mongo_talkpython_client.py",
        "BRISKDB_MONGO_TALKPYTHON_REPORT_DIR",
        Duration::from_secs(180),
    )
    .await;
}

async fn run_application(script: &'static str, report_env: &str, phase_timeout: Duration) {
    let root = tempfile::tempdir().unwrap();
    let reports = tempfile::tempdir().unwrap();
    let report_root = std::env::var_os(report_env)
        .map(PathBuf::from)
        .unwrap_or_else(|| reports.path().to_owned());
    for phase in ["initial", "reopened"] {
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
        let report = report_root.join(format!("{phase}.json"));
        let output = tokio::task::spawn_blocking(move || {
            run_client(uri, phase, report, script, phase_timeout)
        })
        .await;
        server.close().await.unwrap();
        database.close().await.unwrap();
        let output = output.unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        println!("{}", String::from_utf8_lossy(&output.stdout));
    }
}

fn run_client(
    uri: String,
    phase: &str,
    report: PathBuf,
    script: &str,
    phase_timeout: Duration,
) -> Output {
    let python = std::env::var_os("BRISKDB_MONGO_ODM_PYTHON").unwrap_or_else(|| "python3".into());
    let source = std::env::var_os("BRISKDB_MONGO_ODM_SOURCE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/tinymongo-source")
        });
    let mut stdout = tempfile::tempfile().unwrap();
    let mut stderr = tempfile::tempfile().unwrap();
    let mut child = Command::new(python)
        .arg(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests")
                .join(script),
        )
        .arg(uri)
        .arg(source)
        .arg(phase)
        .arg(report)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout.try_clone().unwrap()))
        .stderr(Stdio::from(stderr.try_clone().unwrap()))
        .spawn()
        .expect("launch pinned application runner");
    let started = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if started.elapsed() < phase_timeout => {
                std::thread::sleep(Duration::from_millis(20));
            }
            outcome => {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "application runner {script} did not finish within {phase_timeout:?}: {outcome:?}"
                );
            }
        }
    };
    use std::io::{Seek, SeekFrom};
    stdout.seek(SeekFrom::Start(0)).unwrap();
    stderr.seek(SeekFrom::Start(0)).unwrap();
    let mut captured_stdout = Vec::new();
    let mut captured_stderr = Vec::new();
    stdout
        .take(1024 * 1024)
        .read_to_end(&mut captured_stdout)
        .unwrap();
    stderr
        .take(1024 * 1024)
        .read_to_end(&mut captured_stderr)
        .unwrap();
    Output {
        status,
        stdout: captured_stdout,
        stderr: captured_stderr,
    }
}
