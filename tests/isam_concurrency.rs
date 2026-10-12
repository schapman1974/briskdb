#![cfg(all(unix, feature = "experimental-isam"))]
use briskdb::isam::{Layout, LockPolicy, Mutation, Store};
use std::{
    process::Command,
    time::{Duration, Instant},
};

#[test]
fn writer_process() {
    let Some(path) = std::env::var_os("BRISK_ISAM_WRITER_PATH") else {
        return;
    };
    let owner: u8 = std::env::var("BRISK_ISAM_WRITER_ID")
        .unwrap()
        .parse()
        .unwrap();
    let mut store = Store::open_with_policy(
        path,
        LockPolicy::new(Duration::from_secs(10), Duration::from_millis(1)).unwrap(),
    )
    .unwrap();
    store.reset_operation_stats();
    for batch in 0..12_u8 {
        let operations: Vec<_> = (0..8_u8)
            .map(|i| {
                let key = [owner, batch * 8 + i];
                Mutation::insert(key, key)
            })
            .collect();
        store.write_batch(&operations).unwrap();
    }
    let stats = store.operation_stats();
    let roots_per_write = if store.format_version() == 4 { 3 } else { 1 };
    assert_eq!(
        (stats.syncs, stats.root_writes, stats.publication_retries),
        (24, 12 * roots_per_write, 0)
    );
}

#[test]
fn independent_writers_make_progress_while_old_and_fresh_readers_coexist() {
    independent_writer_snapshots(2);
}

#[test]
fn packed_independent_writers_preserve_old_and_fresh_snapshots() {
    independent_writer_snapshots(3);
}

#[test]
fn pipelined_independent_process_writers_preserve_atomic_snapshots() {
    independent_writer_snapshots(4);
}

fn independent_writer_snapshots(version: u16) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("concurrent.isam");
    let layout = Layout::new(2, 8).unwrap();
    let mut creator = match version {
        2 => Store::create(&path, layout),
        3 => Store::create_packed(&path, layout),
        4 => Store::create_pipelined(&path, layout),
        _ => unreachable!(),
    }
    .unwrap();
    creator
        .write_batch(&[Mutation::insert([0, 0], b"initial")])
        .unwrap();
    let mut old_reader = Store::open_read_only(&path).unwrap();
    let old_snapshot = old_reader.read_batch().unwrap();
    let mut fresh_reader = Store::open_read_only(&path).unwrap();
    fresh_reader.set_lock_policy(
        LockPolicy::new(Duration::from_secs(10), Duration::from_millis(1)).unwrap(),
    );
    let mut processes = Vec::new();
    for owner in 1..=4 {
        processes.push(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "writer_process"])
                .env("BRISK_ISAM_WRITER_PATH", &path)
                .env("BRISK_ISAM_WRITER_ID", owner.to_string())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap(),
        );
    }
    let mut previous = 1;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let read = fresh_reader.read_batch().unwrap();
        let records = read.range(&[0, 0], None, 1024).unwrap();
        assert!(records.len() >= previous);
        assert_eq!(
            (records.len() - 1) % 8,
            0,
            "no partially visible writer batch"
        );
        assert_eq!(read.verify().unwrap(), records.len() as u64);
        previous = records.len();
        if processes
            .iter_mut()
            .all(|p| p.try_wait().unwrap().is_some())
        {
            break;
        }
        if Instant::now() > deadline {
            for child in &mut processes {
                let _ = child.kill();
                let _ = child.wait();
            }
            panic!("writer processes exceeded the test deadline");
        }
        std::thread::yield_now();
    }
    for process in processes {
        let output = process.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    // No reader lifetime lock should have stalled any child commit.
    assert_eq!(old_snapshot.verify().unwrap(), 1);
    assert_eq!(old_snapshot.get(&[1, 0]).unwrap(), None);
    assert_eq!(
        fresh_reader.read_batch().unwrap().verify().unwrap(),
        1 + 4 * 12 * 8
    );
    for owner in 1..=4_u8 {
        let read = fresh_reader.read_batch().unwrap();
        for i in 0..96_u8 {
            assert_eq!(read.get(&[owner, i]).unwrap(), Some(vec![owner, i]));
        }
    }
}

#[cfg(feature = "embedded")]
#[tokio::test]
async fn enabling_isam_does_not_change_sqlite_or_its_existing_query_api() {
    use briskdb::{BriskDb, Statement, Value};
    let directory = tempfile::tempdir().unwrap();
    let sql_root = directory.path().join("sqlite");
    let db = BriskDb::builder(&sql_root)
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let session = db.session();
    session.set_routing_key("unchanged").await.unwrap();
    db.migrate(
        &session,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
    )
    .await
    .unwrap();
    db.execute_write(
        &session,
        Statement::new(
            "INSERT INTO notes VALUES (?1, ?2)",
            vec![Value::from(1_i64), Value::from("SQLite still works")],
        ),
    )
    .await
    .unwrap();
    let mut native = Store::create(
        directory.path().join("native.isam"),
        Layout::new(2, 32).unwrap(),
    )
    .unwrap();
    native
        .write_batch(&[Mutation::insert(b"aa", b"independent native record")])
        .unwrap();
    let result = db
        .query(
            &session,
            Statement::new("SELECT body FROM notes WHERE id = 1", vec![]),
        )
        .await
        .unwrap();
    assert_eq!(
        result.value.rows()[0].values()[0],
        Value::from("SQLite still works")
    );
    assert_eq!(native.read_batch().unwrap().verify().unwrap(), 1);
    assert!(sql_root.join("manifest.sqlite").exists());
    assert!(!sql_root.join("manifest.sqlite.writer.lock").exists());
    session.close().await.unwrap();
    db.close().await.unwrap();
}
