//! Matched v3/v4 local benchmark. Explicitly invoked, never a timing assertion.
#![cfg(all(unix, feature = "experimental-isam"))]

use briskdb::isam::{
    ColumnDefinition, ColumnType, IndexDefinition, LockPolicy, NativeCatalog, NativeValue,
    OperationStats, Store, TableDefinition,
};
use std::{
    fmt::Write as _,
    io::Write as _,
    path::Path,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

const ROWS: usize = 100_000;
const CHAPTER: usize = 36;
const WORKERS: usize = 30;

fn key(id: usize) -> String {
    format!("{id:011}")
}
fn original(id: usize) -> String {
    format!("body-{id:08}-synthetic-payload")
}
fn row(id: usize, body: String) -> Vec<NativeValue> {
    vec![NativeValue::Text(key(id)), NativeValue::Text(body)]
}
fn schema() -> TableDefinition {
    TableDefinition {
        name: "records".into(),
        schema_version: 1,
        columns: vec![
            ColumnDefinition {
                name: "id".into(),
                column_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                name: "body".into(),
                column_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
        indexes: vec![
            IndexDefinition {
                name: "by_id".into(),
                columns: vec!["id".into()],
                unique: true,
            },
            IndexDefinition {
                name: "by_body".into(),
                columns: vec!["body".into()],
                unique: false,
            },
        ],
    }
}
fn open(path: &Path) -> NativeCatalog {
    let mut c = NativeCatalog::open(path).unwrap();
    c.set_lock_policy(LockPolicy::new(Duration::from_secs(60), Duration::from_millis(1)).unwrap());
    c
}
fn read(c: &mut NativeCatalog, chapter: usize) -> Vec<Vec<NativeValue>> {
    c.range_index(
        "records",
        "by_id",
        &[NativeValue::Text(key(chapter * CHAPTER))],
        Some(&[NativeValue::Text(key((chapter + 1) * CHAPTER))]),
        CHAPTER,
    )
    .unwrap()
}
fn update(c: &mut NativeCatalog, id: usize, body: String) {
    assert!(
        c.update_row("records", &[NativeValue::Text(key(id))], &row(id, body))
            .unwrap()
    );
}
fn seed(path: &Path, version: u16) {
    let mut c = if version == 4 {
        NativeCatalog::create_pipelined(path)
    } else {
        NativeCatalog::create_packed(path)
    }
    .unwrap();
    c.create_table(&schema()).unwrap();
    for first in (0..ROWS).step_by(512) {
        let rows = (first..(first + 512).min(ROWS))
            .map(|id| row(id, original(id)))
            .collect::<Vec<_>>();
        c.insert_rows("records", &rows).unwrap();
    }
    let mut checked = 0;
    for ch in 0..ROWS.div_ceil(CHAPTER) {
        let rows = read(&mut c, ch);
        for (offset, value) in rows.iter().enumerate() {
            assert_eq!(
                *value,
                row(ch * CHAPTER + offset, original(ch * CHAPTER + offset))
            );
        }
        checked += rows.len();
    }
    assert_eq!(checked, ROWS);
}

#[derive(Default)]
struct Measurements {
    times: Vec<u128>,
    syncs: u64,
    sync_requests: u64,
    sync_ns: u64,
    lock_ns: u64,
    commit_ns: u64,
    stripe_ns: u64,
    roots: u64,
    pages: u64,
}
impl Measurements {
    fn add(&mut self, times: Vec<u128>, s: OperationStats) {
        self.times.extend(times);
        self.syncs += s.syncs;
        self.sync_requests += s.sync_requests;
        self.sync_ns += s.sync_ns;
        self.lock_ns += s.lock_wait_ns;
        self.commit_ns += s.commit_lock_wait_ns;
        self.stripe_ns += s.write_lock_wait_ns;
        self.roots += s.root_writes;
        self.pages += s.page_reads;
    }
    fn report(
        &mut self,
        version: u16,
        trial: usize,
        phase: &str,
        wall: Duration,
        output: &mut String,
    ) {
        self.times.sort_unstable();
        let n = self.times.len();
        let sum = self.times.iter().sum::<u128>();
        let p50 = self.times[(n - 1) / 2];
        let p99 = self.times[(n - 1) * 99 / 100];
        println!(
            "v{version} trial={trial} {phase}: wall={:.6}s n={n} mean={:.6}ms p50={:.6}ms p99={:.6}ms syncs={} lock={:.1}%",
            wall.as_secs_f64(),
            sum as f64 / n as f64 / 1e6,
            p50 as f64 / 1e6,
            p99 as f64 / 1e6,
            self.syncs,
            100. * self.lock_ns as f64 / sum as f64
        );
        writeln!(
            output,
            "{version}\t{trial}\t{phase}\t{}\t{n}\t{sum}\t{p50}\t{p99}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            wall.as_nanos(),
            self.syncs,
            self.sync_ns,
            self.lock_ns,
            self.commit_ns,
            self.stripe_ns,
            self.roots,
            self.pages
            , self.sync_requests
        )
        .unwrap();
    }
}

fn trial(version: u16, trial: usize, report: &mut String) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("catalog.isam");
    seed(&path, version);
    let mut c = open(&path);
    read(&mut c, 100);
    c.reset_operation_stats();
    let start = Instant::now();
    let mut times = Vec::new();
    for call in 0..2000 {
        let ch = call * 137 % (ROWS / CHAPTER);
        let start = Instant::now();
        let result = read(&mut c, ch);
        times.push(start.elapsed().as_nanos());
        assert_eq!(result.len(), CHAPTER);
    }
    let wall = start.elapsed();
    let mut result = Measurements::default();
    result.add(times, c.operation_stats());
    result.report(version, trial, "solo_read", wall, report);
    c.reset_operation_stats();
    let start = Instant::now();
    let mut times = Vec::new();
    for call in 0..100 {
        let start = Instant::now();
        update(&mut c, 5000 + call, format!("solo-{call}"));
        times.push(start.elapsed().as_nanos());
    }
    let wall = start.elapsed();
    let mut result = Measurements::default();
    result.add(times, c.operation_stats());
    result.report(version, trial, "solo_write", wall, report);
    let peers = (0..WORKERS)
        .map(|_| {
            let mut c = open(&path);
            read(&mut c, 0);
            c.reset_operation_stats();
            c
        })
        .collect::<Vec<_>>();
    let barrier = Arc::new(Barrier::new(WORKERS + 1));
    let (mut reads, mut writes, wall) = thread::scope(|scope| {
        let tasks = peers
            .into_iter()
            .enumerate()
            .map(|(worker, mut c)| {
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    let mut times = Vec::new();
                    barrier.wait();
                    for call in (worker..800).step_by(WORKERS) {
                        let start = Instant::now();
                        if worker % 2 == 0 {
                            let chapter = call / 2 * 137 % (ROWS / CHAPTER);
                            let result = read(&mut c, chapter);
                            times.push(start.elapsed().as_nanos());
                            assert_eq!(result.len(), CHAPTER);
                            for (offset, r) in result.iter().enumerate() {
                                assert_eq!(
                                    r[0],
                                    NativeValue::Text(key(chapter * CHAPTER + offset))
                                );
                            }
                        } else {
                            update(&mut c, worker, format!("mixed-{worker:08}-{call:08}"));
                            times.push(start.elapsed().as_nanos());
                        }
                    }
                    (worker % 2 == 0, times, c.operation_stats())
                })
            })
            .collect::<Vec<_>>();
        let start = Instant::now();
        barrier.wait();
        let (mut reads, mut writes) = (Measurements::default(), Measurements::default());
        for task in tasks {
            let (is_read, times, stats) = task.join().unwrap();
            if is_read {
                reads.add(times, stats);
            } else {
                writes.add(times, stats);
            }
        }
        (reads, writes, start.elapsed())
    });
    assert_eq!(reads.times.len(), 400);
    assert_eq!(writes.times.len(), 400);
    assert_eq!(writes.sync_requests, 800);
    assert!(writes.syncs > 0 && writes.syncs <= writes.sync_requests);
    if version == 3 {
        assert_eq!(writes.syncs, 800);
    }
    assert_eq!(writes.roots, if version == 4 { 1200 } else { 400 });
    reads.report(version, trial, "mixed_read", wall, report);
    writes.report(version, trial, "mixed_write", wall, report);
    // Check primary rows, unchanged unique index, changed index, and old owner removal after reopen.
    let mut c = open(&path);
    for worker in (1..WORKERS).step_by(2) {
        let last = (worker..800).step_by(WORKERS).next_back().unwrap();
        let body = format!("mixed-{worker:08}-{last:08}");
        assert_eq!(
            c.get_row("records", &[NativeValue::Text(key(worker))])
                .unwrap(),
            Some(row(worker, body.clone()))
        );
        assert_eq!(
            c.lookup_index("records", "by_id", &[NativeValue::Text(key(worker))], 2)
                .unwrap(),
            vec![row(worker, body.clone())]
        );
        assert_eq!(
            c.lookup_index("records", "by_body", &[NativeValue::Text(body)], 2)
                .unwrap()
                .len(),
            1
        );
        assert!(
            c.lookup_index(
                "records",
                "by_body",
                &[NativeValue::Text(original(worker))],
                2
            )
            .unwrap()
            .is_empty()
        );
    }
    assert_eq!(
        Store::open_read_only(&path)
            .unwrap()
            .read_batch()
            .unwrap()
            .verify()
            .unwrap(),
        3 * ROWS as u64 + 2
    );
}

#[test]
#[ignore = "release-mode performance experiment; local filesystem only"]
fn compare_packed_and_pipelined_400_reads_400_writes() {
    let mut report = String::from(
        "# local warm-cache 100000 typed rows, 2 secondary indexes; 400x36-row reads + 400 changing updates, 15 reader/15 writer threads; two durability barriers per update, v4 coalesces only within one process; not EFS\nversion\ttrial\tphase\twall_ns\tcalls\tsum_ns\tp50_ns\tp99_ns\tsyncs\tsync_ns\tlock_ns\tcommit_ns\tstripe_ns\troot_writes\tpage_reads\tsync_requests\n",
    );
    for round in 1..=3 {
        for version in if round % 2 == 1 { [3, 4] } else { [4, 3] } {
            trial(version, round, &mut report);
        }
    }
    if let Some(path) = std::env::var_os("BRISK_ISAM_PIPELINE_REPORT") {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        file.write_all(report.as_bytes()).unwrap();
    }
}
