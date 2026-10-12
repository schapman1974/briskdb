//! Direct-engine controls: identical logical records, one primary tree, and
//! explicit durable SQLite settings. This does not replace the BriskDB API
//! default-policy benchmark or establish power-loss/NFS qualification.
#![cfg(all(unix, feature = "isam-benchmark"))]

use briskdb::isam::{Layout, LockPolicy, Mutation, OperationStats, Store};
use rusqlite::{Connection, params};
use std::{
    path::Path,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

const ROWS: usize = 36;
const CHAPTERS: usize = 10;

fn packed_pages() -> bool {
    std::env::var("BRISKDB_ISAM_PACKED").is_ok_and(|value| value == "1")
}

fn key(chapter: usize, verse: usize) -> Vec<u8> {
    format!("{chapter:06}{verse:05}").into_bytes()
}

fn sqlite_open(path: &Path) -> Connection {
    let connection = Connection::open(path).unwrap();
    connection.busy_timeout(Duration::from_secs(30)).unwrap();
    connection
        .execute_batch(
            "PRAGMA page_size=4096; PRAGMA journal_mode=WAL;
         PRAGMA synchronous=FULL; PRAGMA fullfsync=ON;
         PRAGMA checkpoint_fullfsync=ON; PRAGMA wal_autocheckpoint=1000;",
        )
        .unwrap();
    for (name, expected) in [
        ("synchronous", 2),
        ("fullfsync", 1),
        ("checkpoint_fullfsync", 1),
        ("page_size", 4096),
        ("wal_autocheckpoint", 1000),
    ] {
        assert_eq!(
            connection
                .pragma_query_value(None, name, |row| row.get::<_, i64>(0))
                .unwrap(),
            expected
        );
    }
    assert_eq!(
        connection
            .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
            .unwrap(),
        "wal"
    );
    connection
}

enum Engine {
    Isam(Store),
    Sqlite(Connection),
}

impl Engine {
    fn open(path: &Path, sqlite: bool, create: bool) -> Self {
        if sqlite {
            let connection = sqlite_open(path);
            if create {
                connection.execute_batch("CREATE TABLE records (id BLOB PRIMARY KEY, body BLOB NOT NULL) WITHOUT ROWID").unwrap();
            }
            Self::Sqlite(connection)
        } else {
            let mut store = if create {
                let layout = Layout::new(11, 128).unwrap();
                if packed_pages() {
                    Store::create_packed(path, layout)
                } else {
                    Store::create(path, layout)
                }
                .unwrap()
            } else {
                Store::open(path).unwrap()
            };
            store.set_lock_policy(
                LockPolicy::new(Duration::from_secs(30), Duration::from_millis(1)).unwrap(),
            );
            Self::Isam(store)
        }
    }

    fn write(&mut self, keys: &[Vec<u8>], body: &[u8], delete: bool) {
        match self {
            Self::Isam(store) => {
                let mutations: Vec<_> = keys
                    .iter()
                    .map(|key| {
                        if delete {
                            Mutation::delete(key.as_slice())
                        } else {
                            Mutation::put(key.as_slice(), body)
                        }
                    })
                    .collect();
                store.write_batch(&mutations).unwrap();
            }
            Self::Sqlite(connection) => {
                let transaction = connection.transaction().unwrap();
                {
                    let sql = if delete {
                        "DELETE FROM records WHERE id=?1"
                    } else {
                        "INSERT INTO records VALUES (?1, ?2) ON CONFLICT(id) DO UPDATE SET body=excluded.body"
                    };
                    let mut statement = transaction.prepare_cached(sql).unwrap();
                    for key in keys {
                        if delete {
                            statement.execute(params![key]).unwrap();
                        } else {
                            statement.execute(params![key, body]).unwrap();
                        }
                    }
                }
                transaction.commit().unwrap();
            }
        }
    }

    fn read(&mut self, chapter: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let start = key(chapter, 0);
        let end = key(chapter + 1, 0);
        match self {
            Self::Isam(store) => store
                .read_batch()
                .unwrap()
                .range(&start, Some(&end), ROWS)
                .unwrap()
                .into_iter()
                .map(|record| (record.key, record.value))
                .collect(),
            Self::Sqlite(connection) => connection
                .prepare_cached(
                    "SELECT id, body FROM records WHERE id>=?1 AND id<?2 ORDER BY id LIMIT 36",
                )
                .unwrap()
                .query_map(params![start, end], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
        }
    }

    fn stats(&self) -> OperationStats {
        match self {
            Self::Isam(store) => store.operation_stats(),
            _ => OperationStats::default(),
        }
    }
}

struct CallCounts {
    reads: usize,
    writes: usize,
    syncs: u64,
}

fn row(
    report: &mut String,
    backend: &str,
    workload: &str,
    workers: usize,
    mut times: Vec<u128>,
    elapsed: Duration,
    counts: CallCounts,
) {
    let CallCounts {
        reads,
        writes,
        syncs,
    } = counts;
    times.sort_unstable();
    let percentile = |percent: usize| times[(times.len() * percent).div_ceil(100) - 1];
    let syncs = if backend == "isam" {
        syncs.to_string()
    } else {
        "NA".to_owned()
    };
    report.push_str(&format!(
        "{backend}\t{workload}\t{workers}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{syncs}\n",
        times.len(),
        elapsed.as_nanos(),
        percentile(50),
        percentile(95),
        percentile(99),
        reads as f64 / elapsed.as_secs_f64(),
        writes as f64 / elapsed.as_secs_f64()
    ));
}

fn run(samples: usize, calls: usize) -> String {
    let mut report = format!(
        "# schema=isam-commit-benchmark-v1\trevision={}\thost={}-{}\n",
        std::env::var("BRISKDB_ISAM_REVISION").unwrap_or_else(|_| "unspecified".into()),
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    report.push_str(&format!(
        "# native_format\tversion={}\tcompression=none\n",
        if packed_pages() { 3 } else { 2 }
    ));
    report.push_str("# control\tdirect engines; BLOB keys/values; SQLite WITHOUT ROWID; no secondary indexes; SQLite WAL/FULL/fullfsync=ON/checkpoint_fullfsync=ON; page_size=4096; autocheckpoint=1000; ISAM sync_all unchanged; matching requested durability, not proven identical crash guarantees\n");
    report.push_str("# workload\twarm retained handles; mixed: one 36-row chapter/read and one changed record/write; bounded thread workers, not separate-host qualification; checkpoint costs inside operations retained; setup and verification excluded\n");
    report.push_str("backend\tworkload\tworkers\tsamples\telapsed_ns\tp50_ns\tp95_ns\tp99_ns\tchapters_s\twrites_s\tsyncs\n");
    let directory = tempfile::tempdir().unwrap();
    // Rotate the backend that goes first between workloads.
    for (workload_index, workload) in ["read_36", "insert_36", "update_36", "delete_36"]
        .iter()
        .enumerate()
    {
        for sqlite in if workload_index % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let path = directory.path().join(format!("{workload}-{sqlite}"));
            let mut engine = Engine::open(&path, sqlite, true);
            let keys: Vec<_> = (0..ROWS).map(|verse| key(0, verse)).collect();
            let mut times = Vec::new();
            let mut syncs = 0;
            for sample in 0..samples {
                engine.write(&keys, b"seed", *workload == "insert_36");
                let before = engine.stats();
                let body = format!("changed-{sample:08}");
                let start = Instant::now();
                if *workload == "read_36" {
                    let records = engine.read(0);
                    assert_eq!(records.len(), ROWS);
                    std::hint::black_box(records);
                } else {
                    engine.write(&keys, body.as_bytes(), *workload == "delete_36");
                }
                times.push(start.elapsed().as_nanos());
                syncs += engine.stats().syncs - before.syncs;
                let records = engine.read(0);
                if *workload == "delete_36" {
                    assert!(records.is_empty());
                } else {
                    assert_eq!(records.len(), ROWS);
                    for (verse, (id, value)) in records.iter().enumerate() {
                        assert_eq!(*id, key(0, verse));
                        assert_eq!(
                            value.as_slice(),
                            if *workload == "read_36" {
                                b"seed"
                            } else {
                                body.as_bytes()
                            }
                        );
                    }
                }
            }
            let elapsed = Duration::from_nanos(times.iter().sum::<u128>() as u64);
            row(
                &mut report,
                if sqlite { "sqlite_strict" } else { "isam" },
                workload,
                1,
                times,
                elapsed,
                CallCounts {
                    reads: if *workload == "read_36" { samples } else { 0 },
                    writes: if *workload == "read_36" { 0 } else { samples },
                    syncs,
                },
            );
        }
    }
    for workers in [4, 10, 30] {
        for (workload_index, workload) in ["read_only", "write_only", "mixed"].iter().enumerate() {
            for sqlite in if workload_index % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                let path = directory
                    .path()
                    .join(format!("{workload}-{workers}-{sqlite}"));
                let mut seed = Engine::open(&path, sqlite, true);
                let keys: Vec<_> = (0..CHAPTERS)
                    .flat_map(|chapter| (0..ROWS).map(move |verse| key(chapter, verse)))
                    .collect();
                seed.write(&keys, b"seed", false);
                // All handles open before the timed interval, including SQLite
                // connection-local durability configuration.
                let mut engines: Vec<_> = (0..workers)
                    .map(|_| Engine::open(&path, sqlite, false))
                    .collect();
                let total_calls = if *workload == "mixed" {
                    calls * 2
                } else {
                    calls
                };
                let barrier = Arc::new(Barrier::new(workers + 1));
                let (read_times, write_times, syncs, elapsed) = thread::scope(|scope| {
                    let mut handles = Vec::new();
                    for (worker, engine) in engines.iter_mut().enumerate() {
                        let barrier = Arc::clone(&barrier);
                        handles.push(scope.spawn(move || {
                            let before = engine.stats();
                            let mut reads = Vec::new();
                            let mut writes = Vec::new();
                            barrier.wait();
                            for call in (worker..total_calls).step_by(workers) {
                                let read = *workload == "read_only"
                                    || (*workload == "mixed" && call % 2 == 0);
                                let start = Instant::now();
                                if read {
                                    let records = engine.read((call / 2) % CHAPTERS);
                                    assert_eq!(records.len(), ROWS);
                                    std::hint::black_box(records);
                                    reads.push(start.elapsed().as_nanos());
                                } else {
                                    // Each writer owns one key. Values change on
                                    // every call so neither backend can no-op.
                                    let body = format!("worker-{worker:02}-call-{call:08}");
                                    engine.write(
                                        &[key(worker / ROWS, worker % ROWS)],
                                        body.as_bytes(),
                                        false,
                                    );
                                    writes.push(start.elapsed().as_nanos());
                                }
                            }
                            (reads, writes, engine.stats().syncs - before.syncs)
                        }));
                    }
                    let start = Instant::now();
                    barrier.wait();
                    let mut reads = Vec::new();
                    let mut writes = Vec::new();
                    let mut syncs = 0;
                    for handle in handles {
                        let (r, w, s) = handle.join().unwrap();
                        reads.extend(r);
                        writes.extend(w);
                        syncs += s;
                    }
                    (reads, writes, syncs, start.elapsed())
                });
                for chapter in 0..CHAPTERS {
                    let records = seed.read(chapter);
                    assert_eq!(records.len(), ROWS);
                    for (verse, (id, value)) in records.iter().enumerate() {
                        assert_eq!(*id, key(chapter, verse));
                        let worker = chapter * ROWS + verse;
                        let last_write = (worker..total_calls).step_by(workers).rfind(|call| {
                            *workload == "write_only" || (*workload == "mixed" && call % 2 == 1)
                        });
                        let expected = if worker < workers {
                            last_write
                                .map(|call| {
                                    format!("worker-{worker:02}-call-{call:08}").into_bytes()
                                })
                                .unwrap_or_else(|| b"seed".to_vec())
                        } else {
                            b"seed".to_vec()
                        };
                        assert_eq!(*value, expected);
                    }
                }
                let backend = if sqlite { "sqlite_strict" } else { "isam" };
                if !read_times.is_empty() {
                    let reads = read_times.len();
                    row(
                        &mut report,
                        backend,
                        &format!("{workload}_reads"),
                        workers,
                        read_times,
                        elapsed,
                        CallCounts {
                            reads,
                            writes: write_times.len(),
                            syncs,
                        },
                    );
                }
                if !write_times.is_empty() {
                    let writes = write_times.len();
                    row(
                        &mut report,
                        backend,
                        &format!("{workload}_writes"),
                        workers,
                        write_times,
                        elapsed,
                        CallCounts {
                            reads: 0,
                            writes,
                            syncs,
                        },
                    );
                }
            }
        }
    }
    report
}

#[test]
fn strict_commit_control_smoke() {
    let report = run(2, 12);
    assert!(report.contains("sqlite_strict\tupdate_36\t"));
    assert!(report.contains("isam\tmixed_reads\t30\t"));
    for line in report.lines().filter(|line| !line.starts_with('#')) {
        assert_eq!(line.split('\t').count(), 11);
    }
}

#[test]
#[ignore = "matched direct-engine release benchmark; see docs/BENCHMARKS.md"]
fn release_strict_commit_control() {
    let report = run(100, 400);
    let path = std::env::var_os("BRISKDB_ISAM_COMMIT_OUTPUT").expect("set report output path");
    std::fs::write(path, &report).unwrap();
    print!("{report}");
}
