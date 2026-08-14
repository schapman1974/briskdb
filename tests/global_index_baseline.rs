//! Frozen before/after workload for the global-index rollout (issue #226).
//!
//! Timing is deliberately opt-in. Ordinary test runs validate the report and
//! regression-budget machinery; CI invokes the ignored smoke matrix explicitly.

use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use briskdb::core::{
    Database, Engine, EngineErrorKind, Session, ShardKeyMetadata, ShardKeyType, Statement,
    TableDeclaration, Value,
};
use rusqlite::{Connection, params};
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

const FORMAT_VERSION: &str = "global-index-baseline-v1";
const TABLE: &str = "global_index_baseline";
const SHARD_MATRIX: [u16; 4] = [2, 4, 10, 64];
const FULL_ROWS_PER_SHARD: usize = 16;
const FULL_OPERATIONS_PER_WORKER: usize = 256;
const FULL_WARMUP_OPERATIONS: usize = 16;
const FULL_PROCESS_WORKERS: usize = 4;
const SMOKE_ROWS_PER_SHARD: usize = 4;
const SMOKE_OPERATIONS_PER_WORKER: usize = 2;
const SMOKE_WARMUP_OPERATIONS: usize = 1;
const SMOKE_PROCESS_WORKERS: usize = 2;
const INSERT_ROW_BASE: i64 = 1_000_000;
const DELETE_ROW_BASE: i64 = 2_000_000;
const CONTENDED_ROW_BASE: i64 = 3_000_000;
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);
const TELEMETRY_INTERVAL: Duration = Duration::from_micros(200);

const CREATE_SCHEMA: &str = "
    CREATE TABLE global_index_baseline (
        tenant_id INTEGER NOT NULL,
        row_id INTEGER NOT NULL,
        lookup_value TEXT NOT NULL,
        unique_value TEXT NOT NULL,
        payload INTEGER NOT NULL,
        PRIMARY KEY (tenant_id, row_id),
        UNIQUE (tenant_id, unique_value)
    ) STRICT;
    CREATE INDEX global_index_baseline_lookup
        ON global_index_baseline (lookup_value);
";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Workload {
    PointRead,
    ScatterRead,
    IndexedHit,
    IndexedMiss,
    Insert,
    Update,
    Delete,
    ContendedUniqueInsert,
}

impl Workload {
    const ALL: [Self; 8] = [
        Self::PointRead,
        Self::ScatterRead,
        Self::IndexedHit,
        Self::IndexedMiss,
        Self::Insert,
        Self::Update,
        Self::Delete,
        Self::ContendedUniqueInsert,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::PointRead => "point_read",
            Self::ScatterRead => "scatter_read",
            Self::IndexedHit => "indexed_hit",
            Self::IndexedMiss => "indexed_miss",
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::ContendedUniqueInsert => "contended_unique_insert",
        }
    }

    const fn is_read(self) -> bool {
        matches!(
            self,
            Self::PointRead | Self::ScatterRead | Self::IndexedHit | Self::IndexedMiss
        )
    }

    const fn expected_shards(self, shard_count: u16) -> usize {
        if matches!(self, Self::PointRead) {
            1
        } else if self.is_read() {
            shard_count as usize
        } else {
            1
        }
    }
}

impl FromStr for Workload {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|workload| workload.name() == value)
            .ok_or_else(|| format!("unknown global-index benchmark workload {value:?}"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum RunMode {
    SingleProcess,
    MultiProcess,
}

impl RunMode {
    const ALL: [Self; 2] = [Self::SingleProcess, Self::MultiProcess];

    const fn name(self) -> &'static str {
        match self {
            Self::SingleProcess => "single_process",
            Self::MultiProcess => "multi_process",
        }
    }
}

impl FromStr for RunMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|mode| mode.name() == value)
            .ok_or_else(|| format!("unknown global-index benchmark mode {value:?}"))
    }
}

#[derive(Clone, Copy, Debug)]
struct MatrixControls {
    rows_per_shard: usize,
    operations_per_worker: usize,
    warmup_operations: usize,
    process_workers: usize,
}

impl MatrixControls {
    const fn smoke() -> Self {
        Self {
            rows_per_shard: SMOKE_ROWS_PER_SHARD,
            operations_per_worker: SMOKE_OPERATIONS_PER_WORKER,
            warmup_operations: SMOKE_WARMUP_OPERATIONS,
            process_workers: SMOKE_PROCESS_WORKERS,
        }
    }

    const fn full() -> Self {
        Self {
            rows_per_shard: FULL_ROWS_PER_SHARD,
            operations_per_worker: FULL_OPERATIONS_PER_WORKER,
            warmup_operations: FULL_WARMUP_OPERATIONS,
            process_workers: FULL_PROCESS_WORKERS,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct WorkerSpec {
    workload: Workload,
    shard_count: u16,
    rows_per_shard: usize,
    operations: usize,
    warmups: usize,
    worker: usize,
    workers: usize,
}

impl WorkerSpec {
    const fn total_operations(self) -> usize {
        self.operations + self.warmups
    }
}

struct BenchmarkFixture {
    root: TempDir,
    tenant_keys: Vec<i64>,
}

impl BenchmarkFixture {
    fn new(shard_count: u16, controls: MatrixControls) -> Self {
        let root = tempfile::tempdir().expect("create global-index benchmark directory");
        let mut database =
            Database::open(root.path(), shard_count).expect("open global-index benchmark database");
        let completed = database
            .broadcast(CREATE_SCHEMA)
            .expect("create global-index benchmark schema");
        assert_eq!(completed, (0..shard_count).collect::<Vec<_>>());

        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical_database,
                    TABLE,
                    ShardKeyMetadata::new("tenant_id", ShardKeyType::Int64)
                        .expect("declare benchmark shard key"),
                )
                .expect("declare benchmark table"),
            ])
            .expect("register global-index benchmark table");
        let tenant_keys = (0..shard_count)
            .map(|shard| integer_key_for_shard(&database, shard))
            .collect::<Vec<_>>();
        drop(database);

        seed_fixture(root.path(), &tenant_keys, controls);
        Self { root, tenant_keys }
    }

    fn path(&self) -> &Path {
        self.root.path()
    }
}

fn integer_key_for_shard(database: &Database, expected: u16) -> i64 {
    (1_i64..)
        .find(|value| database.shard_for_key(value.to_string().as_bytes()) == expected)
        .expect("the finite shard map has an integer benchmark key for every shard")
}

fn seed_fixture(root: &Path, tenant_keys: &[i64], controls: MatrixControls) {
    for (shard, tenant_key) in tenant_keys.iter().copied().enumerate() {
        let mut connection = Connection::open(shard_path(root, shard as u16))
            .expect("open physical shard for benchmark seeding");
        let transaction = connection
            .transaction()
            .expect("start global-index benchmark seed transaction");
        {
            let mut insert = transaction
                .prepare(
                    "INSERT INTO global_index_baseline
                     (tenant_id, row_id, lookup_value, unique_value, payload)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )
                .expect("prepare global-index benchmark seed insert");

            for row in 0..controls.rows_per_shard {
                insert
                    .execute(params![
                        tenant_key,
                        row as i64,
                        format!("lookup-{shard}-{row}"),
                        format!("base-{shard}-{row}"),
                        (row % 4) as i64,
                    ])
                    .expect("seed global-index benchmark base row");
            }

            let total_per_worker = controls.operations_per_worker + controls.warmup_operations;
            for worker in 0..controls.process_workers {
                if worker % tenant_keys.len() != shard {
                    continue;
                }
                for operation in 0..total_per_worker {
                    let row_id = delete_row_id(worker, operation, total_per_worker);
                    insert
                        .execute(params![
                            tenant_key,
                            row_id,
                            format!("delete-lookup-{worker}-{operation}"),
                            format!("delete-unique-{worker}-{operation}"),
                            -1_i64,
                        ])
                        .expect("seed global-index benchmark delete row");
                }
            }
        }
        transaction
            .commit()
            .expect("commit global-index benchmark seed transaction");
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .expect("checkpoint global-index benchmark seed WAL");
    }
}

fn shard_path(root: &Path, shard: u16) -> PathBuf {
    root.join("shards").join(format!("{shard:04}.sqlite"))
}

fn insert_row_id(worker: usize, operation: usize, total_per_worker: usize) -> i64 {
    indexed_row_id(INSERT_ROW_BASE, worker, operation, total_per_worker)
}

fn delete_row_id(worker: usize, operation: usize, total_per_worker: usize) -> i64 {
    indexed_row_id(DELETE_ROW_BASE, worker, operation, total_per_worker)
}

fn contended_row_id(worker: usize, operation: usize, total_per_worker: usize) -> i64 {
    indexed_row_id(CONTENDED_ROW_BASE, worker, operation, total_per_worker)
}

fn indexed_row_id(base: i64, worker: usize, operation: usize, total_per_worker: usize) -> i64 {
    let offset = worker
        .checked_mul(total_per_worker)
        .and_then(|value| value.checked_add(operation))
        .and_then(|value| i64::try_from(value).ok())
        .expect("benchmark row offset fits i64");
    base.checked_add(offset).expect("benchmark row ID fits i64")
}

#[derive(Clone, Debug, Default)]
struct OperationTotals {
    attempts: u64,
    successes: u64,
    constraints: u64,
    returned_rows: u64,
    visited_shards: u64,
    latency_micros: Vec<u64>,
}

impl OperationTotals {
    fn merge(&mut self, other: Self) {
        self.attempts += other.attempts;
        self.successes += other.successes;
        self.constraints += other.constraints;
        self.returned_rows += other.returned_rows;
        self.visited_shards += other.visited_shards;
        self.latency_micros.extend(other.latency_micros);
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ProcessUsage {
    cpu_micros: u64,
    peak_rss_bytes: u64,
    physical_write_bytes: u64,
}

impl ProcessUsage {
    fn delta(self, before: Self) -> Self {
        Self {
            cpu_micros: self.cpu_micros.saturating_sub(before.cpu_micros),
            peak_rss_bytes: self.peak_rss_bytes,
            physical_write_bytes: self
                .physical_write_bytes
                .saturating_sub(before.physical_write_bytes),
        }
    }
}

#[cfg(unix)]
fn current_process_usage() -> ProcessUsage {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided rusage on success.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(result, 0, "getrusage must succeed for benchmark telemetry");
    // SAFETY: the successful call above initialized the value.
    let usage = unsafe { usage.assume_init() };
    let user_micros = timeval_micros(usage.ru_utime);
    let system_micros = timeval_micros(usage.ru_stime);
    #[cfg(target_os = "macos")]
    let peak_rss_bytes = u64::try_from(usage.ru_maxrss).unwrap_or_default();
    #[cfg(not(target_os = "macos"))]
    let peak_rss_bytes = u64::try_from(usage.ru_maxrss)
        .unwrap_or_default()
        .saturating_mul(1024);
    let output_blocks = u64::try_from(usage.ru_oublock).unwrap_or_default();
    ProcessUsage {
        cpu_micros: user_micros.saturating_add(system_micros),
        peak_rss_bytes,
        physical_write_bytes: output_blocks.saturating_mul(512),
    }
}

#[cfg(unix)]
fn timeval_micros(value: libc::timeval) -> u64 {
    let seconds = u64::try_from(value.tv_sec).unwrap_or_default();
    let micros = u64::try_from(value.tv_usec).unwrap_or_default();
    seconds.saturating_mul(1_000_000).saturating_add(micros)
}

#[cfg(not(unix))]
fn current_process_usage() -> ProcessUsage {
    ProcessUsage::default()
}

#[derive(Clone, Debug)]
struct WorkerMeasurement {
    totals: OperationTotals,
    elapsed_micros: u64,
    usage: ProcessUsage,
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(8)
        .enable_all()
        .build()
        .expect("create global-index benchmark runtime")
}

fn run_worker(root: &Path, tenant_keys: &[i64], spec: WorkerSpec) -> WorkerMeasurement {
    let runtime = runtime();
    let engine = runtime
        .block_on(Engine::open(root, spec.shard_count))
        .expect("open global-index benchmark Engine");
    let session = engine.session();
    if !spec.workload.is_read() {
        let routed_worker = if matches!(spec.workload, Workload::ContendedUniqueInsert) {
            0
        } else {
            spec.worker
        };
        runtime
            .block_on(
                session.set_routing_key(tenant_keys[routed_worker % tenant_keys.len()].to_string()),
            )
            .expect("set global-index benchmark routing key");
    }

    for operation in spec.operations..spec.total_operations() {
        let _ = runtime.block_on(perform_operation(
            &engine,
            &session,
            tenant_keys,
            spec,
            operation,
        ));
    }

    wait_for_parent_start_if_requested();
    let before = current_process_usage();
    let started = Instant::now();
    let mut totals = OperationTotals::default();
    for operation in 0..spec.operations {
        let operation_started = Instant::now();
        let outcome = runtime.block_on(perform_operation(
            &engine,
            &session,
            tenant_keys,
            spec,
            operation,
        ));
        let latency = operation_started.elapsed();
        totals.attempts += 1;
        totals.latency_micros.push(duration_micros(latency));
        match outcome {
            Ok((rows, shards)) => {
                totals.successes += 1;
                totals.returned_rows += rows;
                totals.visited_shards += shards;
            }
            Err(kind)
                if matches!(spec.workload, Workload::ContendedUniqueInsert)
                    && matches!(
                        kind,
                        EngineErrorKind::ConstraintViolation | EngineErrorKind::UniqueViolation
                    ) =>
            {
                totals.constraints += 1;
                totals.visited_shards += 1;
            }
            Err(kind) => panic!(
                "global-index benchmark {} worker {} failed with {kind:?}",
                spec.workload.name(),
                spec.worker
            ),
        }
    }
    let elapsed_micros = duration_micros(started.elapsed());
    let usage = current_process_usage().delta(before);

    runtime
        .block_on(session.close())
        .expect("close global-index benchmark session");
    runtime
        .block_on(engine.shutdown())
        .expect("shut down global-index benchmark Engine");
    WorkerMeasurement {
        totals,
        elapsed_micros,
        usage,
    }
}

async fn perform_operation(
    engine: &Engine,
    session: &Session,
    tenant_keys: &[i64],
    spec: WorkerSpec,
    operation: usize,
) -> Result<(u64, u64), EngineErrorKind> {
    let worker_key = tenant_keys[spec.worker % tenant_keys.len()];
    let result = match spec.workload {
        Workload::PointRead => engine
            .query_logical(
                session,
                Statement::new(
                    "SELECT tenant_id, row_id, payload
                     FROM global_index_baseline
                     WHERE tenant_id = ?1 AND row_id = ?2",
                    vec![Value::from(worker_key), Value::from(0_i64)],
                ),
            )
            .await
            .map(|result| {
                assert_eq!(result.shards, [spec.worker as u16 % spec.shard_count]);
                assert_eq!(result.value.len(), 1);
                (1, result.shards.len() as u64)
            }),
        Workload::ScatterRead => engine
            .query_logical(
                session,
                Statement::new(
                    "SELECT tenant_id, row_id, payload
                     FROM global_index_baseline
                     WHERE payload = ?1",
                    vec![Value::from(1_i64)],
                ),
            )
            .await
            .map(|result| {
                let expected_per_shard =
                    (0..spec.rows_per_shard).filter(|row| row % 4 == 1).count();
                assert_eq!(result.shards.len(), spec.shard_count as usize);
                assert_eq!(
                    result.value.len(),
                    expected_per_shard * spec.shard_count as usize
                );
                (result.value.len() as u64, result.shards.len() as u64)
            }),
        Workload::IndexedHit => engine
            .query_logical(
                session,
                Statement::new(
                    "SELECT tenant_id, row_id, payload
                     FROM global_index_baseline
                     WHERE lookup_value = ?1",
                    vec![Value::from(format!(
                        "lookup-{}-0",
                        spec.worker % spec.shard_count as usize
                    ))],
                ),
            )
            .await
            .map(|result| {
                assert_eq!(result.shards.len(), spec.shard_count as usize);
                assert_eq!(result.value.len(), 1);
                (1, result.shards.len() as u64)
            }),
        Workload::IndexedMiss => engine
            .query_logical(
                session,
                Statement::new(
                    "SELECT tenant_id, row_id, payload
                     FROM global_index_baseline
                     WHERE lookup_value = ?1",
                    vec![Value::from("missing-global-index-key")],
                ),
            )
            .await
            .map(|result| {
                assert_eq!(result.shards.len(), spec.shard_count as usize);
                assert!(result.value.is_empty());
                (0, result.shards.len() as u64)
            }),
        Workload::Insert => engine
            .execute(
                session,
                Statement::new(
                    "INSERT INTO global_index_baseline
                     (tenant_id, row_id, lookup_value, unique_value, payload)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    vec![
                        Value::from(worker_key),
                        Value::from(insert_row_id(
                            spec.worker,
                            operation,
                            spec.total_operations(),
                        )),
                        Value::from(format!("insert-lookup-{}-{operation}", spec.worker)),
                        Value::from(format!("insert-unique-{}-{operation}", spec.worker)),
                        Value::from(7_i64),
                    ],
                ),
            )
            .await
            .map(|result| {
                assert_eq!(result.shard, (spec.worker % tenant_keys.len()) as u16);
                assert_eq!(result.value, 1);
                (1, 1)
            }),
        Workload::Update => engine
            .execute(
                session,
                Statement::new(
                    "UPDATE global_index_baseline
                     SET payload = payload + 1
                     WHERE tenant_id = ?1 AND row_id = ?2",
                    vec![
                        Value::from(worker_key),
                        Value::from((operation % spec.rows_per_shard) as i64),
                    ],
                ),
            )
            .await
            .map(|result| {
                assert_eq!(result.shard, (spec.worker % tenant_keys.len()) as u16);
                assert_eq!(result.value, 1);
                (1, 1)
            }),
        Workload::Delete => engine
            .execute(
                session,
                Statement::new(
                    "DELETE FROM global_index_baseline
                     WHERE tenant_id = ?1 AND row_id = ?2",
                    vec![
                        Value::from(worker_key),
                        Value::from(delete_row_id(
                            spec.worker,
                            operation,
                            spec.total_operations(),
                        )),
                    ],
                ),
            )
            .await
            .map(|result| {
                assert_eq!(result.shard, (spec.worker % tenant_keys.len()) as u16);
                assert_eq!(result.value, 1);
                (1, 1)
            }),
        Workload::ContendedUniqueInsert => {
            let tenant_key = tenant_keys[0];
            engine
                .execute(
                    session,
                    Statement::new(
                        "INSERT INTO global_index_baseline
                         (tenant_id, row_id, lookup_value, unique_value, payload)
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        vec![
                            Value::from(tenant_key),
                            Value::from(contended_row_id(
                                spec.worker,
                                operation,
                                spec.total_operations(),
                            )),
                            Value::from(format!("contended-lookup-{}-{operation}", spec.worker)),
                            Value::from(format!("contended-{operation}")),
                            Value::from(9_i64),
                        ],
                    ),
                )
                .await
                .map(|result| {
                    assert_eq!(result.shard, 0);
                    assert_eq!(result.value, 1);
                    (1, 1)
                })
        }
    };
    result.map_err(|error| error.kind())
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros())
        .unwrap_or(u64::MAX)
        .max(1)
}

fn wait_for_parent_start_if_requested() {
    let Ok(ready_path) = env::var("BRISKDB_BENCH_READY_PATH") else {
        return;
    };
    let start_path = PathBuf::from(
        env::var("BRISKDB_BENCH_START_PATH").expect("child benchmark start path is configured"),
    );
    fs::write(&ready_path, b"ready\n").expect("publish child benchmark readiness");
    let deadline = Instant::now() + CHILD_TIMEOUT;
    while !start_path.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for parent benchmark start"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

#[derive(Clone, Debug)]
struct MatrixRecord {
    mode: RunMode,
    shard_count: u16,
    workload: Workload,
    workers: usize,
    operations_per_worker: usize,
    totals: OperationTotals,
    elapsed_micros: u64,
    cpu_micros: u64,
    peak_rss_bytes: u64,
    physical_write_bytes: u64,
    peak_wal_growth_bytes: u64,
}

impl MatrixRecord {
    fn throughput_per_second(&self) -> f64 {
        self.totals.attempts as f64 * 1_000_000.0 / self.elapsed_micros.max(1) as f64
    }

    fn percentile_micros(&self, percentile: usize) -> u64 {
        let mut latencies = self.totals.latency_micros.clone();
        latencies.sort_unstable();
        assert!(!latencies.is_empty());
        let numerator = percentile
            .saturating_mul(latencies.len())
            .saturating_add(99);
        let rank = numerator / 100;
        latencies[rank.saturating_sub(1).min(latencies.len() - 1)]
    }

    fn to_tsv(&self) -> String {
        format!(
            "result\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.2}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\tFULL",
            self.mode.name(),
            self.shard_count,
            self.workload.name(),
            self.workers,
            self.operations_per_worker,
            self.totals.attempts,
            self.totals.successes,
            self.totals.constraints,
            self.totals.returned_rows,
            self.totals.visited_shards,
            self.elapsed_micros,
            self.throughput_per_second(),
            self.percentile_micros(50),
            self.percentile_micros(95),
            self.percentile_micros(99),
            self.cpu_micros,
            self.peak_rss_bytes,
            self.physical_write_bytes,
            self.peak_wal_growth_bytes,
            self.workload.expected_shards(self.shard_count),
        )
    }
}

fn run_single_process_case(
    shard_count: u16,
    workload: Workload,
    controls: MatrixControls,
) -> MatrixRecord {
    let fixture = BenchmarkFixture::new(shard_count, controls);
    let initial_wal = total_wal_bytes(fixture.path(), shard_count);
    let telemetry = WalTelemetry::start(fixture.path().to_path_buf(), shard_count, initial_wal);
    let measurement = run_worker(
        fixture.path(),
        &fixture.tenant_keys,
        WorkerSpec {
            workload,
            shard_count,
            rows_per_shard: controls.rows_per_shard,
            operations: controls.operations_per_worker,
            warmups: controls.warmup_operations,
            worker: 0,
            workers: 1,
        },
    );
    let peak_wal_growth_bytes = telemetry.stop();
    validate_case(
        fixture.path(),
        &fixture.tenant_keys,
        workload,
        controls,
        1,
        &measurement.totals,
    );
    MatrixRecord {
        mode: RunMode::SingleProcess,
        shard_count,
        workload,
        workers: 1,
        operations_per_worker: controls.operations_per_worker,
        totals: measurement.totals,
        elapsed_micros: measurement.elapsed_micros,
        cpu_micros: measurement.usage.cpu_micros,
        peak_rss_bytes: measurement.usage.peak_rss_bytes,
        physical_write_bytes: measurement.usage.physical_write_bytes,
        peak_wal_growth_bytes,
    }
}

fn run_multi_process_case(
    shard_count: u16,
    workload: Workload,
    controls: MatrixControls,
) -> MatrixRecord {
    let fixture = BenchmarkFixture::new(shard_count, controls);
    let coordination =
        tempfile::tempdir().expect("create benchmark process coordination directory");
    let start_path = coordination.path().join("start");
    let executable = env::current_exe().expect("resolve global-index benchmark test executable");
    let tenant_keys = fixture
        .tenant_keys
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let mut children = Vec::with_capacity(controls.process_workers);
    let mut result_paths = Vec::with_capacity(controls.process_workers);

    for worker in 0..controls.process_workers {
        let ready_path = coordination.path().join(format!("ready-{worker}"));
        let result_path = coordination.path().join(format!("result-{worker}.tsv"));
        let child = Command::new(&executable)
            .args(["--ignored", "--exact", "global_index_baseline_worker"])
            .env("BRISKDB_BENCH_CHILD", "1")
            .env("BRISKDB_BENCH_ROOT", fixture.path())
            .env("BRISKDB_BENCH_SHARDS", shard_count.to_string())
            .env("BRISKDB_BENCH_ROWS", controls.rows_per_shard.to_string())
            .env(
                "BRISKDB_BENCH_OPERATIONS",
                controls.operations_per_worker.to_string(),
            )
            .env(
                "BRISKDB_BENCH_WARMUPS",
                controls.warmup_operations.to_string(),
            )
            .env("BRISKDB_BENCH_WORKER", worker.to_string())
            .env(
                "BRISKDB_BENCH_WORKERS",
                controls.process_workers.to_string(),
            )
            .env("BRISKDB_BENCH_WORKLOAD", workload.name())
            .env("BRISKDB_BENCH_TENANT_KEYS", &tenant_keys)
            .env("BRISKDB_BENCH_READY_PATH", &ready_path)
            .env("BRISKDB_BENCH_START_PATH", &start_path)
            .env("BRISKDB_BENCH_RESULT_PATH", &result_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn global-index benchmark child");
        children.push((child, ready_path));
        result_paths.push(result_path);
    }

    let ready_deadline = Instant::now() + CHILD_TIMEOUT;
    while children.iter().any(|(_, ready)| !ready.exists()) {
        assert!(
            Instant::now() < ready_deadline,
            "timed out waiting for global-index benchmark children"
        );
        thread::sleep(Duration::from_millis(1));
    }

    let initial_wal = total_wal_bytes(fixture.path(), shard_count);
    let telemetry = WalTelemetry::start(fixture.path().to_path_buf(), shard_count, initial_wal);
    fs::write(&start_path, b"start\n").expect("release global-index benchmark children");

    for (child, _) in children {
        let output = child
            .wait_with_output()
            .expect("wait for global-index benchmark child");
        assert!(
            output.status.success(),
            "global-index benchmark child failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let peak_wal_growth_bytes = telemetry.stop();

    let mut totals = OperationTotals::default();
    let mut elapsed_micros = 0_u64;
    let mut cpu_micros = 0_u64;
    let mut peak_rss_bytes = 0_u64;
    let mut physical_write_bytes = 0_u64;
    for path in result_paths {
        let measurement = parse_worker_measurement(
            &fs::read_to_string(path).expect("read global-index benchmark child result"),
        );
        elapsed_micros = elapsed_micros.max(measurement.elapsed_micros);
        totals.merge(measurement.totals);
        cpu_micros = cpu_micros.saturating_add(measurement.usage.cpu_micros);
        peak_rss_bytes = peak_rss_bytes.saturating_add(measurement.usage.peak_rss_bytes);
        physical_write_bytes =
            physical_write_bytes.saturating_add(measurement.usage.physical_write_bytes);
    }
    validate_case(
        fixture.path(),
        &fixture.tenant_keys,
        workload,
        controls,
        controls.process_workers,
        &totals,
    );
    MatrixRecord {
        mode: RunMode::MultiProcess,
        shard_count,
        workload,
        workers: controls.process_workers,
        operations_per_worker: controls.operations_per_worker,
        totals,
        elapsed_micros,
        cpu_micros,
        peak_rss_bytes,
        physical_write_bytes,
        peak_wal_growth_bytes,
    }
}

struct WalTelemetry {
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<u64>,
}

impl WalTelemetry {
    fn start(root: PathBuf, shard_count: u16, initial_bytes: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let mut peak = initial_bytes;
            while !worker_stop.load(Ordering::Acquire) {
                peak = peak.max(total_wal_bytes(&root, shard_count));
                thread::sleep(TELEMETRY_INTERVAL);
            }
            peak.max(total_wal_bytes(&root, shard_count))
                .saturating_sub(initial_bytes)
        });
        Self { stop, handle }
    }

    fn stop(self) -> u64 {
        self.stop.store(true, Ordering::Release);
        self.handle.join().expect("join WAL telemetry thread")
    }
}

fn total_wal_bytes(root: &Path, shard_count: u16) -> u64 {
    let manifest = fs::metadata(root.join("manifest.sqlite-wal"))
        .map(|metadata| metadata.len())
        .unwrap_or_default();
    (0..shard_count).fold(manifest, |total, shard| {
        total.saturating_add(
            fs::metadata(format!("{}-wal", shard_path(root, shard).display()))
                .map(|metadata| metadata.len())
                .unwrap_or_default(),
        )
    })
}

fn validate_case(
    root: &Path,
    tenant_keys: &[i64],
    workload: Workload,
    controls: MatrixControls,
    workers: usize,
    totals: &OperationTotals,
) {
    let attempts = (workers * controls.operations_per_worker) as u64;
    assert_eq!(totals.attempts, attempts);
    assert_eq!(totals.latency_micros.len() as u64, attempts);
    assert_eq!(
        totals.visited_shards,
        attempts * workload.expected_shards(tenant_keys.len() as u16) as u64
    );
    if matches!(workload, Workload::ContendedUniqueInsert) {
        assert_eq!(totals.successes, controls.operations_per_worker as u64);
        assert_eq!(totals.successes + totals.constraints, attempts);
        let connection =
            Connection::open(shard_path(root, 0)).expect("open unique benchmark shard");
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM global_index_baseline
                 WHERE unique_value LIKE 'contended-%'",
                [],
                |row| row.get(0),
            )
            .expect("count contended benchmark rows");
        let expected = controls.operations_per_worker + controls.warmup_operations;
        assert_eq!(count, expected as i64);
    } else {
        assert_eq!(totals.successes, attempts);
        assert_eq!(totals.constraints, 0);
    }

    match workload {
        Workload::Insert => {
            assert_eq!(
                count_rows_with_prefix(root, tenant_keys, "insert-unique-%"),
                workers * (controls.operations_per_worker + controls.warmup_operations)
            );
        }
        Workload::Delete => {
            let seeded_per_worker = controls.operations_per_worker + controls.warmup_operations;
            assert_eq!(
                count_rows_with_prefix(root, tenant_keys, "delete-unique-%"),
                (controls.process_workers - workers) * seeded_per_worker
            );
        }
        _ => {}
    }
    validate_sqlite_files(root, tenant_keys.len() as u16);
}

fn count_rows_with_prefix(root: &Path, tenant_keys: &[i64], pattern: &str) -> usize {
    (0..tenant_keys.len())
        .map(|shard| {
            let connection = Connection::open(shard_path(root, shard as u16))
                .expect("open benchmark shard for row count");
            connection
                .query_row(
                    "SELECT COUNT(*) FROM global_index_baseline WHERE unique_value LIKE ?1",
                    [pattern],
                    |row| row.get::<_, i64>(0),
                )
                .expect("count benchmark rows") as usize
        })
        .sum()
}

fn validate_sqlite_files(root: &Path, shard_count: u16) {
    for shard in 0..shard_count {
        let connection = Connection::open(shard_path(root, shard))
            .expect("open benchmark shard for integrity check");
        let quick_check: String = connection
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .expect("run benchmark shard quick_check");
        assert_eq!(quick_check, "ok");
    }
}

fn worker_measurement_tsv(measurement: &WorkerMeasurement) -> String {
    format!(
        "worker\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        measurement.elapsed_micros,
        measurement.usage.cpu_micros,
        measurement.usage.peak_rss_bytes,
        measurement.usage.physical_write_bytes,
        measurement.totals.attempts,
        measurement.totals.successes,
        measurement.totals.constraints,
        measurement.totals.returned_rows,
        measurement.totals.visited_shards,
        join_u64(&measurement.totals.latency_micros),
    )
}

fn parse_worker_measurement(value: &str) -> WorkerMeasurement {
    let fields = value.trim_end().split('\t').collect::<Vec<_>>();
    assert_eq!(fields.len(), 11, "worker result has a fixed field count");
    assert_eq!(fields[0], "worker");
    WorkerMeasurement {
        elapsed_micros: parse_field(fields[1], "elapsed micros"),
        usage: ProcessUsage {
            cpu_micros: parse_field(fields[2], "CPU micros"),
            peak_rss_bytes: parse_field(fields[3], "peak RSS"),
            physical_write_bytes: parse_field(fields[4], "physical write bytes"),
        },
        totals: OperationTotals {
            attempts: parse_field(fields[5], "attempts"),
            successes: parse_field(fields[6], "successes"),
            constraints: parse_field(fields[7], "constraints"),
            returned_rows: parse_field(fields[8], "returned rows"),
            visited_shards: parse_field(fields[9], "visited shards"),
            latency_micros: fields[10]
                .split(',')
                .map(|field| parse_field(field, "latency"))
                .collect(),
        },
    }
}

fn join_u64(values: &[u64]) -> String {
    values
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_field<T>(value: &str, name: &str) -> T
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .unwrap_or_else(|error| panic!("invalid {name} {value:?}: {error}"))
}

fn run_matrix(shards: &[u16], controls: MatrixControls) -> Vec<MatrixRecord> {
    let mut records = Vec::new();
    for shard_count in shards.iter().copied() {
        for mode in RunMode::ALL {
            for workload in Workload::ALL {
                let record = match mode {
                    RunMode::SingleProcess => {
                        run_single_process_case(shard_count, workload, controls)
                    }
                    RunMode::MultiProcess => {
                        run_multi_process_case(shard_count, workload, controls)
                    }
                };
                println!("{}", record.to_tsv());
                records.push(record);
            }
        }
    }
    records
}

const RESULT_HEADER: &str = "record\tmode\tshards\tworkload\tworkers\toperations_per_worker\tattempts\tsuccesses\tconstraints\treturned_rows\tvisited_shards\telapsed_us\tthroughput_ops_s\tp50_us\tp95_us\tp99_us\tcpu_us\tpeak_rss_bytes\tphysical_write_bytes\tpeak_wal_growth_bytes\texpected_shards_per_operation\tsqlite_synchronous";

fn print_report_header(controls: MatrixControls) {
    println!("metadata\tformat\t{FORMAT_VERSION}");
    println!("metadata\tos\t{}", env::consts::OS);
    println!("metadata\tarch\t{}", env::consts::ARCH);
    println!("control\trows_per_shard\t{}", controls.rows_per_shard);
    println!(
        "control\toperations_per_worker\t{}",
        controls.operations_per_worker
    );
    println!("control\twarmup_operations\t{}", controls.warmup_operations);
    println!("control\tprocess_workers\t{}", controls.process_workers);
    println!("control\tcache_policy\twarm");
    println!("control\tsqlite_journal_mode\tWAL");
    println!("control\tsqlite_synchronous\tFULL");
    println!(
        "control\tfsync_observation\tSQLite FULL durability policy; syscall counts are not portable and are not inferred"
    );
    println!("{RESULT_HEADER}");
}

#[derive(Clone, Debug)]
struct ParsedBaseline {
    mode: RunMode,
    shards: u16,
    workload: Workload,
    throughput: f64,
    p99_micros: u64,
    cpu_micros: u64,
    peak_rss_bytes: u64,
    physical_write_bytes: u64,
    peak_wal_growth_bytes: u64,
    attempts: u64,
}

impl ParsedBaseline {
    fn parse(line: &str) -> Result<Self, String> {
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.len() != 22 || fields[0] != "result" {
            return Err(format!("invalid result record field count: {line}"));
        }
        Ok(Self {
            mode: fields[1].parse()?,
            shards: fields[2]
                .parse()
                .map_err(|error| format!("invalid shard count: {error}"))?,
            workload: fields[3].parse()?,
            attempts: fields[6]
                .parse()
                .map_err(|error| format!("invalid attempt count: {error}"))?,
            throughput: fields[12]
                .parse()
                .map_err(|error| format!("invalid throughput: {error}"))?,
            p99_micros: fields[15]
                .parse()
                .map_err(|error| format!("invalid p99: {error}"))?,
            cpu_micros: fields[16]
                .parse()
                .map_err(|error| format!("invalid CPU time: {error}"))?,
            peak_rss_bytes: fields[17]
                .parse()
                .map_err(|error| format!("invalid RSS: {error}"))?,
            physical_write_bytes: fields[18]
                .parse()
                .map_err(|error| format!("invalid write bytes: {error}"))?,
            peak_wal_growth_bytes: fields[19]
                .parse()
                .map_err(|error| format!("invalid WAL bytes: {error}"))?,
        })
    }

    fn key(&self) -> (RunMode, u16, Workload) {
        (self.mode, self.shards, self.workload)
    }
}

fn parse_report(
    report: &str,
) -> Result<BTreeMap<(RunMode, u16, Workload), ParsedBaseline>, String> {
    let mut records = BTreeMap::new();
    for line in report.lines().filter(|line| line.starts_with("result\t")) {
        let record = ParsedBaseline::parse(line)?;
        if records.insert(record.key(), record).is_some() {
            return Err(format!("duplicate result record: {line}"));
        }
    }
    if records.is_empty() {
        return Err("report contains no result records".to_owned());
    }
    Ok(records)
}

#[derive(Clone, Copy)]
struct RegressionBudget {
    minimum_throughput_ratio: f64,
    maximum_p99_ratio: f64,
    maximum_p99_jitter_micros: u64,
    maximum_cpu_per_attempt_ratio: f64,
    maximum_write_bytes_per_attempt_ratio: f64,
    maximum_wal_bytes_per_attempt_ratio: f64,
    maximum_rss_growth_bytes: u64,
}

impl RegressionBudget {
    const STABLE_HOST: Self = Self {
        minimum_throughput_ratio: 0.50,
        maximum_p99_ratio: 3.0,
        maximum_p99_jitter_micros: 5_000,
        maximum_cpu_per_attempt_ratio: 2.0,
        maximum_write_bytes_per_attempt_ratio: 2.0,
        maximum_wal_bytes_per_attempt_ratio: 2.0,
        maximum_rss_growth_bytes: 64 * 1024 * 1024,
    };

    fn compare(self, baseline: &ParsedBaseline, candidate: &ParsedBaseline) -> Vec<String> {
        let mut failures = Vec::new();
        if candidate.throughput < baseline.throughput * self.minimum_throughput_ratio {
            failures.push("throughput".to_owned());
        }
        let p99_limit = (baseline.p99_micros.max(1) as f64 * self.maximum_p99_ratio).max(
            baseline
                .p99_micros
                .saturating_add(self.maximum_p99_jitter_micros) as f64,
        );
        if candidate.p99_micros as f64 > p99_limit {
            failures.push("p99 latency".to_owned());
        }
        if per_attempt(candidate.cpu_micros, candidate.attempts)
            > per_attempt(baseline.cpu_micros, baseline.attempts)
                * self.maximum_cpu_per_attempt_ratio
        {
            failures.push("CPU per attempt".to_owned());
        }
        if per_attempt(candidate.physical_write_bytes, candidate.attempts)
            > per_attempt(baseline.physical_write_bytes, baseline.attempts)
                * self.maximum_write_bytes_per_attempt_ratio
        {
            failures.push("physical write bytes per attempt".to_owned());
        }
        if per_attempt(candidate.peak_wal_growth_bytes, candidate.attempts)
            > per_attempt(baseline.peak_wal_growth_bytes, baseline.attempts)
                * self.maximum_wal_bytes_per_attempt_ratio
        {
            failures.push("WAL bytes per attempt".to_owned());
        }
        if candidate.peak_rss_bytes
            > baseline
                .peak_rss_bytes
                .saturating_add(self.maximum_rss_growth_bytes)
        {
            failures.push("peak RSS".to_owned());
        }
        failures
    }
}

fn per_attempt(value: u64, attempts: u64) -> f64 {
    value as f64 / attempts.max(1) as f64
}

fn compare_reports(baseline: &str, candidate: &str) -> Result<(), String> {
    let baseline = parse_report(baseline)?;
    let candidate = parse_report(candidate)?;
    if baseline.keys().collect::<Vec<_>>() != candidate.keys().collect::<Vec<_>>() {
        return Err("baseline and candidate matrices do not contain identical cases".to_owned());
    }
    let mut failures = Vec::new();
    for (key, before) in &baseline {
        let after = &candidate[key];
        for metric in RegressionBudget::STABLE_HOST.compare(before, after) {
            failures.push(format!(
                "{} shards {} {} regressed: {metric}",
                key.1,
                key.0.name(),
                key.2.name()
            ));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

#[test]
fn report_parser_and_regression_budgets_are_deterministic() {
    let line = "result\tsingle_process\t2\tpoint_read\t1\t32\t32\t32\t0\t32\t32\t1000\t32000.00\t20\t30\t40\t500\t1000\t2000\t3000\t1\tFULL";
    let parsed = ParsedBaseline::parse(line).unwrap();
    assert_eq!(
        parsed.key(),
        (RunMode::SingleProcess, 2, Workload::PointRead)
    );
    assert_eq!(parse_report(line).unwrap().len(), 1);
    assert!(compare_reports(line, line).is_ok());

    let slow = line.replacen("32000.00", "1000.00", 1);
    let failure = compare_reports(line, &slow).unwrap_err();
    assert!(failure.contains("throughput"));

    let high_tail = line.replacen("\t40\t500\t", "\t6000\t500\t", 1);
    assert!(
        compare_reports(line, &high_tail)
            .unwrap_err()
            .contains("p99 latency")
    );

    let duplicate = format!("{line}\n{line}\n");
    assert!(parse_report(&duplicate).unwrap_err().contains("duplicate"));
}

#[test]
fn frozen_baseline_contains_every_ordered_matrix_case() {
    let report = include_str!("../docs/benchmarks/global-index-before-2026-08-14.tsv");
    let records = parse_report(report).unwrap();
    assert_eq!(
        records.len(),
        SHARD_MATRIX.len() * RunMode::ALL.len() * Workload::ALL.len()
    );
    for shard_count in SHARD_MATRIX {
        for mode in RunMode::ALL {
            for workload in Workload::ALL {
                let record = &records[&(mode, shard_count, workload)];
                assert!(record.attempts > 0);
                assert!(record.throughput > 0.0);
                assert!(record.p99_micros > 0);
            }
        }
    }
}

#[test]
#[ignore = "dedicated stable Linux correctness smoke for issue #226"]
fn global_index_baseline_smoke() {
    let controls = MatrixControls::smoke();
    print_report_header(controls);
    let records = run_matrix(&[2], controls);
    assert_eq!(records.len(), RunMode::ALL.len() * Workload::ALL.len());
}

#[test]
#[ignore = "manual release-mode global-index baseline matrix for issue #226"]
fn release_global_index_baseline() {
    if cfg!(debug_assertions) {
        panic!("run the full matrix with --release");
    }
    let controls = MatrixControls::full();
    print_report_header(controls);
    let records = run_matrix(&SHARD_MATRIX, controls);
    assert_eq!(
        records.len(),
        SHARD_MATRIX.len() * RunMode::ALL.len() * Workload::ALL.len()
    );
    if let Ok(path) = env::var("BRISKDB_BENCH_COMPARE") {
        let baseline = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read comparison baseline {path}: {error}"));
        let candidate = records
            .iter()
            .map(MatrixRecord::to_tsv)
            .collect::<Vec<_>>()
            .join("\n");
        compare_reports(&baseline, &candidate)
            .unwrap_or_else(|error| panic!("global-index benchmark regression: {error}"));
    }
}

#[test]
#[ignore = "internal subprocess entrypoint for the issue #226 benchmark"]
fn global_index_baseline_worker() {
    if env::var("BRISKDB_BENCH_CHILD").as_deref() != Ok("1") {
        return;
    }
    let root = PathBuf::from(required_env("BRISKDB_BENCH_ROOT"));
    let tenant_keys = required_env("BRISKDB_BENCH_TENANT_KEYS")
        .split(',')
        .map(|value| parse_field(value, "tenant key"))
        .collect::<Vec<i64>>();
    let spec = WorkerSpec {
        workload: required_env("BRISKDB_BENCH_WORKLOAD")
            .parse()
            .expect("parse child workload"),
        shard_count: parse_field(&required_env("BRISKDB_BENCH_SHARDS"), "shard count"),
        rows_per_shard: parse_field(&required_env("BRISKDB_BENCH_ROWS"), "rows per shard"),
        operations: parse_field(&required_env("BRISKDB_BENCH_OPERATIONS"), "operations"),
        warmups: parse_field(&required_env("BRISKDB_BENCH_WARMUPS"), "warmups"),
        worker: parse_field(&required_env("BRISKDB_BENCH_WORKER"), "worker"),
        workers: parse_field(&required_env("BRISKDB_BENCH_WORKERS"), "workers"),
    };
    assert_eq!(tenant_keys.len(), spec.shard_count as usize);
    assert!(spec.worker < spec.workers);
    let measurement = run_worker(&root, &tenant_keys, spec);
    fs::write(
        required_env("BRISKDB_BENCH_RESULT_PATH"),
        worker_measurement_tsv(&measurement),
    )
    .expect("write global-index benchmark child result");
}

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("required benchmark environment {name} is missing"))
}
