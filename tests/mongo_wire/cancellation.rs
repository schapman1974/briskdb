use super::*;
use briskdb::protocol::mongo::MongoCommandKind;
use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    path::Path,
    process::{Child, Stdio},
};

struct ClientProcess(Child);

impl Drop for ClientProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn signal(root: &Path, name: &str, child: &mut Child) -> io::Result<()> {
    while !root.join(name).exists() {
        if let Some(status) = child.try_wait()? {
            return Err(io::Error::other(format!(
                "client exited before {name}: {status}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

async fn exercise(server: &MongoServer, root: &Path, child: &mut Child) -> io::Result<()> {
    signal(root, "held", child).await?;
    let before = server.metrics();
    assert_eq!(before.cursors.active, 1);
    assert_eq!(before.command(MongoCommandKind::GetMore).completed, 1);
    assert_eq!(before.command(MongoCommandKind::GetMore).failed, 0);
    let old_connections: Vec<_> = server
        .client_metadata()
        .into_iter()
        .map(|item| item.connection_id)
        .collect();
    fs::write(root.join("cancel"), [])?;
    signal(root, "cancelled", child).await?;
    // A deadline bounds this entire protocol. Do not release the Python client
    // to reuse or close itself until cancellation alone drains the old cursor.
    while server.metrics().cursors.active != 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let after = server.metrics();
    assert!(after.closed_connections > before.closed_connections);
    assert_eq!(after.command(MongoCommandKind::GetMore).in_flight, 0);
    assert_eq!(after.cursors.closed, after.cursors.registered);
    assert!(old_connections.iter().any(|old| {
        !server
            .client_metadata()
            .iter()
            .any(|item| item.connection_id == *old)
    }));
    fs::write(root.join("reuse"), [])?;
    signal(root, "reused", child).await?;
    assert_eq!(server.metrics().cursors.active, 0);
    assert!(server.metrics().admitted_connections > before.admitted_connections);
    fs::write(root.join("finish"), [])?;
    while child.try_wait()?.is_none() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

pub(super) async fn run() {
    let storage = tempfile::tempdir().unwrap();
    for phase in ["initial", "reopened"] {
        let database = BriskDb::builder(storage.path())
            .with_shard_count(2)
            .with_document_support(DocumentSupport::Enabled)
            .open()
            .await
            .unwrap();
        let mut server = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        server.set_read_metrics_enabled(true);
        let signals = tempfile::tempdir().unwrap();
        let python =
            std::env::var_os("BRISKDB_MONGO_WIRE_PYTHON").unwrap_or_else(|| "python3".into());
        let mut stdout = tempfile::tempfile().unwrap();
        let mut stderr = tempfile::tempfile().unwrap();
        let mut process = ClientProcess(
            Command::new(python)
                .arg(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/mongo_cancel_client.py"
                ))
                .arg(format!("mongodb://{}/", server.address()))
                .arg(signals.path())
                .stdin(Stdio::null())
                .stdout(Stdio::from(stdout.try_clone().unwrap()))
                .stderr(Stdio::from(stderr.try_clone().unwrap()))
                .spawn()
                .unwrap(),
        );
        let outcome = timeout(
            Duration::from_secs(45),
            exercise(&server, signals.path(), &mut process.0),
        )
        .await;
        let status = process.0.try_wait().unwrap();
        if status.is_none() {
            let _ = process.0.kill();
            let _ = process.0.wait();
        }
        stdout.seek(SeekFrom::Start(0)).unwrap();
        stderr.seek(SeekFrom::Start(0)).unwrap();
        let mut logs = String::new();
        stdout.take(1024 * 1024).read_to_string(&mut logs).unwrap();
        stderr.take(1024 * 1024).read_to_string(&mut logs).unwrap();
        server.close().await.unwrap();
        assert!(server.client_metadata().is_empty());
        super::metrics::assert_driver_metrics_drained(&server.metrics());
        database.close().await.unwrap();
        assert!(
            matches!(outcome, Ok(Ok(()))) && status.is_some_and(|status| status.success()),
            "{phase}: {outcome:?} {status:?}\n{logs}"
        );
        println!("{phase}: {logs}");
    }
}
