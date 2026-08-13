//! Manual release-mode benchmark matrix for the experimental shard facade.
//!
//! Run only when making the issue #126 rollout decision:
//!
//! ```text
//! cargo test --release --locked --features experimental-vtab --lib \
//!   storage::sharded_vtab::benchmarks::release_benchmark_matrix_reports_issue_126_comparison \
//!   -- --ignored --exact --nocapture --test-threads=1
//! ```
//!
//! Compare the two generated-ID allocators at 2, 4, 8, and 10 writers with:
//!
//! ```text
//! cargo test --release --locked --features experimental-vtab --lib \
//!   storage::sharded_vtab::benchmarks::release_benchmark_matrix_reports_issue_129_generated_write_comparison \
//!   -- --ignored --exact --nocapture --test-threads=1
//! ```

use std::{
    collections::BTreeSet,
    fmt::Write as _,
    fs,
    hint::black_box,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use rusqlite::{Connection, params};
use tokio::runtime::{Builder, Runtime};

use super::{MAX_CURSOR_BYTES, MAX_CURSOR_ROWS, ReadCoordinator, Storage, WriteCoordinator};
use crate::core::{
    Database, Engine, EngineError, EngineErrorKind, EngineOptions, EngineResult, GeneratedIdPolicy,
    ResultLimits, Session, ShardKeyMetadata, ShardKeyType, Statement, TableDeclaration, Value,
    canonical_shard_key_bytes,
    generated_id::{HiloV1Id, NativeRangeV1Id},
};

const SHARD_MATRIX: [u16; 3] = [2, 10, 64];
const ROWS_PER_SHARD: usize = 256;
const POINT_PROBE_ITERATIONS: usize = 100;
const TARGET_SCANNED_ROWS: usize = 100_000;
const ORDER_LIMIT: usize = 50;
const SAMPLE_COUNT: usize = 5;
const WARMUP_OPERATIONS: usize = 3;
const MIN_SAMPLE_DURATION: Duration = Duration::from_millis(250);
const TARGET_SAMPLE_DURATION: Duration = Duration::from_millis(500);
const MAX_CALIBRATED_ITERATIONS: usize = 1_000_000;
const GENERATED_WRITE_SHARDS: u16 = 4;
const GENERATED_WRITER_MATRIX: [usize; 4] = [2, 4, 8, 10];
const GENERATED_WRITES_PER_WORKER: usize = 10_000;

// Issue #131 freezes one comparison contract across both implementations.
// These values deliberately do not inherit the older issue #126/#129 harness
// constants: those historical measurements used different shard and sampling
// matrices and therefore cannot serve as the final rollout comparison.
const ROLLOUT_SHARDS: u16 = 10;
const ROLLOUT_CLIENT_MATRIX: [usize; 4] = [2, 4, 8, 10];
const ROLLOUT_TRIAL_COUNT: usize = 3;
const ROLLOUT_WARMUP_OPERATIONS_PER_CLIENT: usize = 100;
const ROLLOUT_TRIAL_DURATION: Duration = Duration::from_secs(10);
const ROLLOUT_TELEMETRY_INTERVAL: Duration = Duration::from_millis(50);
const ROLLOUT_CONNECTIONS_PER_SHARD: usize = 4;
const ROLLOUT_QUEUE_CAPACITY_PER_SHARD: usize = 32;
const ROLLOUT_RUNTIME_THREADS: usize = 2;
const ROLLOUT_BLOCKING_THREADS: usize = 10;

const CREATE_BENCHMARK_TABLES: &str = "
    CREATE TABLE benchmark_hash (
        tenant_id INTEGER NOT NULL,
        row_no INTEGER NOT NULL,
        payload TEXT NOT NULL,
        PRIMARY KEY (tenant_id, row_no)
    ) STRICT;
    CREATE TABLE benchmark_native (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payload TEXT NOT NULL
    ) STRICT;
";

const CREATE_GENERATED_WRITE_TABLES: &str = "
    CREATE TABLE benchmark_generated_native (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payload TEXT NOT NULL
    ) STRICT;
    CREATE TABLE benchmark_generated_hilo (
        id INTEGER PRIMARY KEY,
        payload TEXT NOT NULL
    ) STRICT;
";
const NATIVE_GENERATED_INSERT_SQL: &str =
    "INSERT INTO benchmark_generated_native (id, payload) VALUES (NULL, ?1)";
const HILO_GENERATED_INSERT_SQL: &str =
    "INSERT INTO benchmark_generated_hilo (id, payload) VALUES (NULL, ?1)";
const NATIVE_GENERATED_COUNT_SQL: &str = "SELECT COUNT(*) FROM benchmark_generated_native";
const HILO_GENERATED_COUNT_SQL: &str = "SELECT COUNT(*) FROM benchmark_generated_hilo";

const HASH_POINT_SQL: &str = "
    SELECT tenant_id, row_no, payload
    FROM benchmark_hash
    WHERE tenant_id = ?1 AND row_no = ?2
";
const EXPLICIT_INSERT_SQL: &str =
    "INSERT INTO benchmark_hash (tenant_id, row_no, payload) VALUES (?1, ?2, ?3)";
const HASH_FULL_SQL: &str = "SELECT tenant_id, row_no, payload FROM benchmark_hash";
const HASH_COUNT_SQL: &str = "SELECT COUNT(*) FROM benchmark_hash";
const HASH_ORDER_LIMIT_SQL: &str = "
    SELECT tenant_id, row_no, payload
    FROM benchmark_hash
    ORDER BY row_no DESC, tenant_id ASC
    LIMIT 50
";
const NATIVE_POINT_SQL: &str = "SELECT id, payload FROM benchmark_native WHERE id = ?1";

type HashRow = (i64, i64, String);

struct BenchmarkFixture {
    session: Session,
    engine: Engine,
    coordinator: ReadCoordinator,
    runtime: Runtime,
    hash_keys: Vec<i64>,
    native_ids: Vec<i64>,
    shard_count: u16,
    temp: tempfile::TempDir,
}

impl BenchmarkFixture {
    fn new(shard_count: u16) -> Self {
        let temp = tempfile::tempdir().expect("create issue #126 benchmark directory");
        let mut database =
            Database::open(temp.path(), shard_count).expect("open issue #126 benchmark database");
        database
            .broadcast(CREATE_BENCHMARK_TABLES)
            .expect("create issue #126 benchmark tables on every shard");
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical_database,
                    "benchmark_hash",
                    ShardKeyMetadata::new("tenant_id", ShardKeyType::Int64)
                        .expect("declare benchmark hash shard key"),
                )
                .expect("declare benchmark hash table"),
                TableDeclaration::sharded(
                    logical_database,
                    "benchmark_native",
                    ShardKeyMetadata::new("id", ShardKeyType::Int64)
                        .expect("declare benchmark native shard key"),
                )
                .expect("declare benchmark native table")
                .with_generated_id_policy(
                    GeneratedIdPolicy::native_range_v1("id")
                        .expect("declare benchmark native generated-ID policy"),
                )
                .expect("apply benchmark native generated-ID policy"),
            ])
            .expect("register issue #126 benchmark catalog");

        let hash_keys = find_hash_key_for_each_shard(&database, shard_count);
        let storage =
            Storage::open(temp.path(), shard_count).expect("open benchmark storage for seeding");
        let allocation_owners = storage
            .allocation_owner_map()
            .expect("current benchmark format has an allocation-owner map");
        let native_ids = (0..shard_count)
            .map(|shard| {
                let owner = allocation_owners
                    .owner_for_physical_shard(shard)
                    .expect("every benchmark shard has an allocation owner");
                NativeRangeV1Id::new(owner, 1)
                    .expect("benchmark native ID is valid")
                    .encode()
            })
            .collect::<Vec<_>>();
        seed_rows(&storage, &hash_keys, &native_ids);

        let coordinator = ReadCoordinator::open(storage).expect("open issue #126 read coordinator");
        let limits = ResultLimits::new(MAX_CURSOR_ROWS as u64, MAX_CURSOR_BYTES as u64)
            .expect("virtual-table result limits are valid engine limits");
        let engine = Engine::from_database_with_options(
            Arc::new(database),
            EngineOptions::default().with_result_limits(limits),
        )
        .expect("open comparison Engine");
        let session = engine.session();
        let runtime = Builder::new_multi_thread()
            .worker_threads(usize::from(shard_count).clamp(2, 8))
            .enable_all()
            .build()
            .expect("create issue #126 benchmark runtime");

        Self {
            session,
            engine,
            coordinator,
            runtime,
            hash_keys,
            native_ids,
            shard_count,
            temp,
        }
    }

    fn logical_row_count(&self) -> usize {
        usize::from(self.shard_count) * ROWS_PER_SHARD
    }

    fn point_shard(&self) -> u16 {
        self.shard_count - 1
    }

    fn point_key(&self) -> i64 {
        self.hash_keys[usize::from(self.point_shard())]
    }

    fn native_point_id(&self) -> i64 {
        self.native_ids[usize::from(self.point_shard())]
    }

    fn on_disk_bytes(&self) -> FixtureDiskBytes {
        FixtureDiskBytes::measure(self.temp.path(), self.shard_count)
    }

    fn vtab_hash_point(&self) -> HashRow {
        self.coordinator
            .connection()
            .query_row(
                HASH_POINT_SQL,
                params![self.point_key(), (ROWS_PER_SHARD / 2) as i64],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("query vtab benchmark hash point")
    }

    fn vtab_hash_full(&self) -> Vec<HashRow> {
        self.coordinator
            .connection()
            .prepare(HASH_FULL_SQL)
            .expect("prepare vtab benchmark hash scan")
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .expect("query vtab benchmark hash scan")
            .collect::<Result<Vec<_>, _>>()
            .expect("materialize vtab benchmark hash scan")
    }

    fn vtab_hash_count(&self) -> i64 {
        self.coordinator
            .connection()
            .query_row(HASH_COUNT_SQL, [], |row| row.get(0))
            .expect("query vtab benchmark count")
    }

    fn vtab_hash_order_limit(&self) -> Vec<HashRow> {
        self.coordinator
            .connection()
            .prepare(HASH_ORDER_LIMIT_SQL)
            .expect("prepare vtab benchmark ordered limit")
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .expect("query vtab benchmark ordered limit")
            .collect::<Result<Vec<_>, _>>()
            .expect("materialize vtab benchmark ordered limit")
    }

    fn vtab_native_point(&self) -> (i64, String) {
        self.coordinator
            .connection()
            .query_row(NATIVE_POINT_SQL, [self.native_point_id()], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .expect("query vtab benchmark native point")
    }

    fn engine_hash_point(&self) -> (Vec<u16>, Vec<HashRow>) {
        let result = self
            .runtime
            .block_on(self.engine.query_logical(
                &self.session,
                Statement::new(
                    HASH_POINT_SQL,
                    vec![
                        Value::from(self.point_key()),
                        Value::from((ROWS_PER_SHARD / 2) as i64),
                    ],
                ),
            ))
            .expect("query Engine benchmark hash point");
        (result.shards, result_set_hash_rows(&result.value))
    }

    fn engine_hash_full(&self) -> (Vec<u16>, Vec<HashRow>) {
        let result = self
            .runtime
            .block_on(
                self.engine
                    .query_logical(&self.session, Statement::new(HASH_FULL_SQL, vec![])),
            )
            .expect("query Engine benchmark hash scan");
        (result.shards, result_set_hash_rows(&result.value))
    }

    fn correctness_preflight(&self) {
        let _ = self.coordinator.take_opened_shards();
        let vtab_point = self.vtab_hash_point();
        assert_eq!(
            self.coordinator.take_opened_shards(),
            [self.point_shard()],
            "vtab hash point must open one physical shard"
        );
        let (engine_point_shards, engine_point) = self.engine_hash_point();
        assert_eq!(engine_point_shards, [self.point_shard()]);
        assert_eq!(engine_point, [vtab_point]);

        let mut vtab_full = self.vtab_hash_full();
        assert_eq!(
            self.coordinator.take_opened_shards(),
            (0..self.shard_count).collect::<Vec<_>>(),
            "vtab full scan must visit every physical shard once"
        );
        let (engine_full_shards, mut engine_full) = self.engine_hash_full();
        assert_eq!(
            engine_full_shards,
            (0..self.shard_count).collect::<Vec<_>>()
        );
        assert_eq!(vtab_full.len(), self.logical_row_count());
        vtab_full.sort_unstable();
        engine_full.sort_unstable();
        assert_eq!(engine_full, vtab_full);

        let mut expected_ordered = vtab_full.clone();
        expected_ordered.sort_unstable_by(|left, right| {
            right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
        });
        expected_ordered.truncate(ORDER_LIMIT);

        assert_eq!(
            usize::try_from(self.vtab_hash_count()).expect("benchmark count is non-negative"),
            self.logical_row_count()
        );
        assert_eq!(
            self.coordinator.take_opened_shards(),
            (0..self.shard_count).collect::<Vec<_>>(),
            "vtab count must visit every physical shard"
        );
        let ordered = self.vtab_hash_order_limit();
        assert_eq!(ordered, expected_ordered);
        assert_eq!(
            self.coordinator.take_opened_shards(),
            (0..self.shard_count).collect::<Vec<_>>(),
            "vtab ordered limit must visit every physical shard"
        );

        let _ = self.coordinator.take_opened_shards();
        let native = self.vtab_native_point();
        assert_eq!(native.0, self.native_point_id());
        assert_eq!(
            self.coordinator.take_opened_shards(),
            [self.point_shard()],
            "vtab native point must route through the allocation-owner map"
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeneratedWritePolicy {
    NativeRangeV1,
    HiloV1,
}

impl GeneratedWritePolicy {
    const fn name(self) -> &'static str {
        match self {
            Self::NativeRangeV1 => "native_range_v1",
            Self::HiloV1 => "hilo_v1",
        }
    }

    const fn insert_sql(self) -> &'static str {
        match self {
            Self::NativeRangeV1 => NATIVE_GENERATED_INSERT_SQL,
            Self::HiloV1 => HILO_GENERATED_INSERT_SQL,
        }
    }

    const fn count_sql(self) -> &'static str {
        match self {
            Self::NativeRangeV1 => NATIVE_GENERATED_COUNT_SQL,
            Self::HiloV1 => HILO_GENERATED_COUNT_SQL,
        }
    }
}

struct GeneratedWriteBenchmarkFixture {
    storage: Storage,
    native_table_id: u64,
    hilo_table_id: u64,
    _temp: tempfile::TempDir,
}

impl GeneratedWriteBenchmarkFixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("create issue #129 benchmark directory");
        let mut database = Database::open(temp.path(), GENERATED_WRITE_SHARDS)
            .expect("open issue #129 benchmark database");
        database
            .broadcast(CREATE_GENERATED_WRITE_TABLES)
            .expect("create issue #129 generated-ID tables on every shard");
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical_database,
                    "benchmark_generated_native",
                    ShardKeyMetadata::new("id", ShardKeyType::Int64)
                        .expect("declare benchmark native shard key"),
                )
                .expect("declare benchmark native table")
                .with_generated_id_policy(
                    GeneratedIdPolicy::native_range_v1("id")
                        .expect("declare benchmark native generated-ID policy"),
                )
                .expect("apply benchmark native generated-ID policy"),
                TableDeclaration::sharded(
                    logical_database,
                    "benchmark_generated_hilo",
                    ShardKeyMetadata::new("id", ShardKeyType::Int64)
                        .expect("declare benchmark hilo shard key"),
                )
                .expect("declare benchmark hilo table")
                .with_generated_id_policy(
                    GeneratedIdPolicy::hilo_v1("id")
                        .expect("declare benchmark hilo generated-ID policy"),
                )
                .expect("apply benchmark hilo generated-ID policy"),
            ])
            .expect("register issue #129 benchmark catalog");
        let native_table_id = database
            .catalog()
            .table("default", "benchmark_generated_native")
            .expect("look up benchmark native table")
            .expect("benchmark native table is registered")
            .id()
            .get();
        let hilo_table_id = database
            .catalog()
            .table("default", "benchmark_generated_hilo")
            .expect("look up benchmark hilo table")
            .expect("benchmark hilo table is registered")
            .id()
            .get();
        let storage = Storage::open(temp.path(), GENERATED_WRITE_SHARDS)
            .expect("open issue #129 benchmark storage");
        Self {
            storage,
            native_table_id,
            hilo_table_id,
            _temp: temp,
        }
    }

    const fn table_id(&self, policy: GeneratedWritePolicy) -> u64 {
        match policy {
            GeneratedWritePolicy::NativeRangeV1 => self.native_table_id,
            GeneratedWritePolicy::HiloV1 => self.hilo_table_id,
        }
    }

    fn measure_concurrent_writes(
        &self,
        policy: GeneratedWritePolicy,
        writers: usize,
        writes_per_worker: usize,
    ) -> Sample {
        assert!(writers > 0);
        assert!(writes_per_worker > 0);
        let coordinators = (0..writers)
            .map(|_| {
                WriteCoordinator::open(self.storage.clone())
                    .expect("open generated-write benchmark coordinator")
            })
            .collect::<Vec<_>>();
        let barrier = Arc::new(Barrier::new(writers + 1));
        let table_id = self.table_id(policy);
        let sql = policy.insert_sql();

        let (elapsed, affected_rows) = thread::scope(|scope| {
            let handles = coordinators
                .into_iter()
                .enumerate()
                .map(|(worker, mut coordinator)| {
                    let barrier = Arc::clone(&barrier);
                    scope.spawn(move || {
                        let payload = format!("issue-129-{}-worker-{worker}", policy.name());
                        barrier.wait();
                        let mut affected_rows = 0_usize;
                        for _ in 0..writes_per_worker {
                            let result = coordinator
                                .execute_generated_dml_auto(
                                    sql,
                                    params![payload.as_str()],
                                    table_id,
                                )
                                .expect("execute generated-write benchmark insert");
                            assert_eq!(result.affected_rows(), 1);
                            assert!(result.generated_key().is_some());
                            affected_rows = affected_rows
                                .checked_add(result.affected_rows())
                                .expect("benchmark affected-row count fits usize");
                        }
                        affected_rows
                    })
                })
                .collect::<Vec<_>>();
            let started = Instant::now();
            barrier.wait();
            let affected_rows = handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .expect("generated-write benchmark worker panicked")
                })
                .sum::<usize>();
            (started.elapsed(), affected_rows)
        });
        let expected = writers
            .checked_mul(writes_per_worker)
            .expect("benchmark operation count fits usize");
        assert_eq!(affected_rows, expected);
        Sample {
            iterations: affected_rows,
            elapsed,
        }
    }

    fn physical_row_count(&self, policy: GeneratedWritePolicy) -> usize {
        let total = (0..GENERATED_WRITE_SHARDS)
            .map(|shard| {
                self.storage
                    .open_shard(shard)
                    .expect("open generated-write benchmark shard for counting")
                    .query_row(policy.count_sql(), [], |row| row.get::<_, i64>(0))
                    .expect("count generated-write benchmark rows")
            })
            .sum::<i64>();
        usize::try_from(total).expect("generated-write benchmark count is non-negative")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RolloutPath {
    VirtualTable,
    ExistingRouter,
}

impl RolloutPath {
    const ALL: [Self; 2] = [Self::VirtualTable, Self::ExistingRouter];

    const fn name(self) -> &'static str {
        match self {
            Self::VirtualTable => "vtab",
            Self::ExistingRouter => "engine_logical",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RolloutWorkload {
    PointRead,
    ScatterRead,
    ExplicitWrite,
    NativeRangeWrite,
    HiloWrite,
}

impl RolloutWorkload {
    const ALL: [Self; 5] = [
        Self::PointRead,
        Self::ScatterRead,
        Self::ExplicitWrite,
        Self::NativeRangeWrite,
        Self::HiloWrite,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::PointRead => "point_read",
            Self::ScatterRead => "scatter_read",
            Self::ExplicitWrite => "explicit_write",
            Self::NativeRangeWrite => "native_range_write",
            Self::HiloWrite => "hilo_write",
        }
    }

    /// The logical statement text is part of the comparison identity. Both
    /// paths bind the same SQL and values; only their execution boundary may
    /// differ.
    const fn sql(self) -> &'static str {
        match self {
            Self::PointRead => HASH_POINT_SQL,
            Self::ScatterRead => HASH_FULL_SQL,
            Self::ExplicitWrite => EXPLICIT_INSERT_SQL,
            Self::NativeRangeWrite => NATIVE_GENERATED_INSERT_SQL,
            Self::HiloWrite => HILO_GENERATED_INSERT_SQL,
        }
    }

    const fn is_scatter(self) -> bool {
        matches!(self, Self::ScatterRead)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RolloutCase {
    clients: usize,
    path: RolloutPath,
    workload: RolloutWorkload,
}

fn rollout_cases() -> Vec<RolloutCase> {
    ROLLOUT_CLIENT_MATRIX
        .into_iter()
        .flat_map(|clients| {
            RolloutPath::ALL.into_iter().flat_map(move |path| {
                RolloutWorkload::ALL
                    .into_iter()
                    .map(move |workload| RolloutCase {
                        clients,
                        path,
                        workload,
                    })
            })
        })
        .collect()
}

fn rollout_unavailable_reason(case: RolloutCase) -> Option<&'static str> {
    match (case.path, case.workload) {
        (
            RolloutPath::ExistingRouter,
            RolloutWorkload::NativeRangeWrite | RolloutWorkload::HiloWrite,
        ) => Some(
            "no independent pre-vtab comparator: public Engine omitted-key writes delegate to the writable virtual-table coordinator",
        ),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RolloutControls {
    shards: u16,
    warmup_operations_per_client: usize,
    trial_count: usize,
    trial_duration: Duration,
    telemetry_interval: Duration,
    connections_per_shard: usize,
    queue_capacity_per_shard: usize,
    runtime_threads: usize,
    blocking_threads: usize,
    journal_mode: &'static str,
    synchronous: &'static str,
    cache_policy: &'static str,
}

impl RolloutControls {
    const fn frozen() -> Self {
        Self {
            shards: ROLLOUT_SHARDS,
            warmup_operations_per_client: ROLLOUT_WARMUP_OPERATIONS_PER_CLIENT,
            trial_count: ROLLOUT_TRIAL_COUNT,
            trial_duration: ROLLOUT_TRIAL_DURATION,
            telemetry_interval: ROLLOUT_TELEMETRY_INTERVAL,
            connections_per_shard: ROLLOUT_CONNECTIONS_PER_SHARD,
            queue_capacity_per_shard: ROLLOUT_QUEUE_CAPACITY_PER_SHARD,
            runtime_threads: ROLLOUT_RUNTIME_THREADS,
            blocking_threads: ROLLOUT_BLOCKING_THREADS,
            journal_mode: "WAL",
            synchronous: "FULL",
            cache_policy: "warm_os_cache",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RolloutErrorClass {
    Busy,
    Cancelled,
    Constraint,
    Corrupt,
    DiskFull,
    Other,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RolloutErrorCounts {
    busy: u64,
    cancelled: u64,
    constraint: u64,
    corrupt: u64,
    disk_full: u64,
    other: u64,
}

impl RolloutErrorCounts {
    fn record(&mut self, class: RolloutErrorClass) {
        let counter = match class {
            RolloutErrorClass::Busy => &mut self.busy,
            RolloutErrorClass::Cancelled => &mut self.cancelled,
            RolloutErrorClass::Constraint => &mut self.constraint,
            RolloutErrorClass::Corrupt => &mut self.corrupt,
            RolloutErrorClass::DiskFull => &mut self.disk_full,
            RolloutErrorClass::Other => &mut self.other,
        };
        *counter = counter
            .checked_add(1)
            .expect("rollout benchmark error count fits u64");
    }

    fn total(self) -> u64 {
        self.busy
            .checked_add(self.cancelled)
            .and_then(|total| total.checked_add(self.constraint))
            .and_then(|total| total.checked_add(self.corrupt))
            .and_then(|total| total.checked_add(self.disk_full))
            .and_then(|total| total.checked_add(self.other))
            .expect("rollout benchmark error total fits u64")
    }

    fn merge(&mut self, other: Self) {
        for (total, value) in [
            (&mut self.busy, other.busy),
            (&mut self.cancelled, other.cancelled),
            (&mut self.constraint, other.constraint),
            (&mut self.corrupt, other.corrupt),
            (&mut self.disk_full, other.disk_full),
            (&mut self.other, other.other),
        ] {
            *total = total
                .checked_add(value)
                .expect("rollout benchmark error count fits u64");
        }
    }
}

/// One cumulative process/pool/filesystem observation. A manual runner may
/// obtain CPU and RSS from a sidecar process monitor; keeping those values as
/// inputs makes accounting testable without embedding platform-specific
/// process APIs in the database benchmark.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RolloutTelemetryObservation {
    process_cpu: Duration,
    resident_bytes: u64,
    process_lifetime_peak_resident_bytes: u64,
    wal_bytes: u64,
    pool_active_by_shard: Vec<usize>,
    pool_queued_by_shard: Vec<usize>,
}

impl RolloutTelemetryObservation {
    fn validate(&self, shard_count: u16) -> Result<(), String> {
        let expected = usize::from(shard_count);
        if self.pool_active_by_shard.len() != expected
            || self.pool_queued_by_shard.len() != expected
        {
            return Err(format!(
                "rollout telemetry must contain pool occupancy for exactly {expected} shards"
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcessTelemetry {
    cumulative_cpu: Duration,
    resident_bytes: u64,
    lifetime_peak_resident_bytes: u64,
}

#[cfg(unix)]
fn sample_current_process() -> Result<ProcessTelemetry, String> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the supplied rusage object when it returns
    // zero. The object is not read on the error path.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return Err(format!(
            "getrusage(RUSAGE_SELF) failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: the successful getrusage call above initialized the object.
    let usage = unsafe { usage.assume_init() };
    let cumulative_cpu = timeval_duration(usage.ru_utime)?
        .checked_add(timeval_duration(usage.ru_stime)?)
        .ok_or_else(|| "process CPU duration overflowed".to_owned())?;
    let peak_resident = u64::try_from(usage.ru_maxrss)
        .map_err(|_| "getrusage returned a negative peak RSS".to_owned())?;
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    let lifetime_peak_resident_bytes = peak_resident;
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    let lifetime_peak_resident_bytes = peak_resident
        .checked_mul(1_024)
        .ok_or_else(|| "peak RSS byte count overflowed".to_owned())?;

    let process_id = std::process::id().to_string();
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", process_id.as_str()])
        .output()
        .map_err(|error| format!("run ps for current RSS: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "ps current-RSS sampler exited with status {}",
            output.status
        ));
    }
    let resident_kib = std::str::from_utf8(&output.stdout)
        .map_err(|error| format!("ps current RSS was not UTF-8: {error}"))?
        .trim()
        .parse::<u64>()
        .map_err(|error| format!("parse ps current RSS: {error}"))?;
    let resident_bytes = resident_kib
        .checked_mul(1_024)
        .ok_or_else(|| "current RSS byte count overflowed".to_owned())?;
    Ok(ProcessTelemetry {
        cumulative_cpu,
        resident_bytes,
        lifetime_peak_resident_bytes: lifetime_peak_resident_bytes.max(resident_bytes),
    })
}

#[cfg(not(unix))]
fn sample_current_process() -> Result<ProcessTelemetry, String> {
    Err("the rollout benchmark process sampler currently requires Unix getrusage and ps".to_owned())
}

#[cfg(unix)]
fn timeval_duration(time: libc::timeval) -> Result<Duration, String> {
    let seconds = u64::try_from(time.tv_sec)
        .map_err(|_| "getrusage returned negative CPU seconds".to_owned())?;
    let micros = u32::try_from(time.tv_usec)
        .ok()
        .filter(|&micros| micros < 1_000_000)
        .ok_or_else(|| "getrusage returned invalid CPU microseconds".to_owned())?;
    Ok(Duration::new(seconds, micros * 1_000))
}

fn total_wal_bytes(root: &Path, shard_count: u16) -> u64 {
    (0..shard_count)
        .map(|shard| {
            optional_file_bytes(&root.join("shards").join(format!("{shard:04}.sqlite-wal")))
        })
        .try_fold(
            optional_file_bytes(&root.join("manifest.sqlite-wal")),
            |total, bytes| total.checked_add(bytes),
        )
        .expect("rollout benchmark WAL byte total fits u64")
}

fn sample_rollout_telemetry(
    root: &Path,
    shard_count: u16,
    engine: Option<&Engine>,
) -> Result<RolloutTelemetryObservation, String> {
    let process = sample_current_process()?;
    let (pool_active_by_shard, pool_queued_by_shard) = if let Some(engine) = engine {
        let snapshot = engine
            .pool_snapshot_for_test()
            .map_err(|error| format!("snapshot Engine pools: {error}"))?;
        if snapshot.shards.len() != usize::from(shard_count) {
            return Err(format!(
                "Engine pool snapshot contained {} shards, expected {shard_count}",
                snapshot.shards.len()
            ));
        }
        (
            snapshot.shards.iter().map(|shard| shard.active).collect(),
            snapshot.shards.iter().map(|shard| shard.queued).collect(),
        )
    } else {
        (
            vec![0; usize::from(shard_count)],
            vec![0; usize::from(shard_count)],
        )
    };
    Ok(RolloutTelemetryObservation {
        process_cpu: process.cumulative_cpu,
        resident_bytes: process.resident_bytes,
        process_lifetime_peak_resident_bytes: process.lifetime_peak_resident_bytes,
        wal_bytes: total_wal_bytes(root, shard_count),
        pool_active_by_shard,
        pool_queued_by_shard,
    })
}

struct RolloutFixtureTemplate {
    temp: tempfile::TempDir,
    digest: [u8; 32],
}

impl RolloutFixtureTemplate {
    fn new(controls: RolloutControls) -> Self {
        let temp = tempfile::tempdir().expect("create issue #131 rollout template directory");
        let mut database = Database::open(temp.path(), controls.shards)
            .expect("open issue #131 rollout template database");
        database
            .broadcast(&format!(
                "{CREATE_BENCHMARK_TABLES}\n{CREATE_GENERATED_WRITE_TABLES}"
            ))
            .expect("create rollout template tables on every shard");
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(rollout_table_declarations(logical_database))
            .expect("register rollout template catalog");
        let hash_keys = find_hash_key_for_each_shard(&database, controls.shards);
        let storage = Storage::open(temp.path(), controls.shards)
            .expect("open rollout template storage for seeding");
        seed_rollout_hash_rows(&storage, &hash_keys);
        drop(storage);
        drop(database);
        let digest = rollout_fixture_digest(temp.path());
        Self { temp, digest }
    }
}

fn rollout_table_declarations(
    logical_database: crate::core::LogicalDatabaseId,
) -> Vec<TableDeclaration> {
    vec![
        TableDeclaration::sharded(
            logical_database,
            "benchmark_hash",
            ShardKeyMetadata::new("tenant_id", ShardKeyType::Int64)
                .expect("declare rollout hash shard key"),
        )
        .expect("declare rollout hash table"),
        TableDeclaration::sharded(
            logical_database,
            "benchmark_native",
            ShardKeyMetadata::new("id", ShardKeyType::Int64)
                .expect("declare rollout seeded native shard key"),
        )
        .expect("declare rollout seeded native table")
        .with_generated_id_policy(
            GeneratedIdPolicy::native_range_v1("id")
                .expect("declare rollout seeded native generated-ID policy"),
        )
        .expect("apply rollout seeded native generated-ID policy"),
        TableDeclaration::sharded(
            logical_database,
            "benchmark_generated_native",
            ShardKeyMetadata::new("id", ShardKeyType::Int64)
                .expect("declare rollout native shard key"),
        )
        .expect("declare rollout native table")
        .with_generated_id_policy(
            GeneratedIdPolicy::native_range_v1("id")
                .expect("declare rollout native generated-ID policy"),
        )
        .expect("apply rollout native generated-ID policy"),
        TableDeclaration::sharded(
            logical_database,
            "benchmark_generated_hilo",
            ShardKeyMetadata::new("id", ShardKeyType::Int64)
                .expect("declare rollout hilo shard key"),
        )
        .expect("declare rollout hilo table")
        .with_generated_id_policy(
            GeneratedIdPolicy::hilo_v1("id").expect("declare rollout hilo generated-ID policy"),
        )
        .expect("apply rollout hilo generated-ID policy"),
    ]
}

struct RolloutBenchmarkFixture {
    storage: Storage,
    engine: Engine,
    runtime: Runtime,
    hash_keys: Vec<i64>,
    expected_scatter_fingerprint: [u8; 32],
    native_table_id: u64,
    hilo_table_id: u64,
    baseline_digest: [u8; 32],
    temp: tempfile::TempDir,
}

impl RolloutBenchmarkFixture {
    fn new(controls: RolloutControls) -> Self {
        let template = RolloutFixtureTemplate::new(controls);
        Self::from_template(controls, &template)
    }

    fn from_template(controls: RolloutControls, template: &RolloutFixtureTemplate) -> Self {
        let temp = tempfile::tempdir().expect("create issue #131 rollout benchmark directory");
        copy_rollout_fixture(template.temp.path(), temp.path());
        assert_eq!(rollout_fixture_digest(temp.path()), template.digest);
        let database = Database::open(temp.path(), controls.shards)
            .expect("open issue #131 rollout benchmark database");
        let native_table_id = database
            .catalog()
            .table("default", "benchmark_generated_native")
            .expect("look up rollout native table")
            .expect("rollout native table is registered")
            .id()
            .get();
        let hilo_table_id = database
            .catalog()
            .table("default", "benchmark_generated_hilo")
            .expect("look up rollout hilo table")
            .expect("rollout hilo table is registered")
            .id()
            .get();
        let hash_keys = find_hash_key_for_each_shard(&database, controls.shards);
        let expected_scatter_fingerprint = rollout_expected_scatter_fingerprint(&hash_keys);
        let storage = Storage::open(temp.path(), controls.shards)
            .expect("open cloned rollout benchmark storage");
        let options = EngineOptions::new(
            controls.connections_per_shard,
            controls.queue_capacity_per_shard,
        )
        .expect("rollout Engine options are valid")
        .with_result_limits(
            ResultLimits::new(MAX_CURSOR_ROWS as u64, MAX_CURSOR_BYTES as u64)
                .expect("rollout result limits are valid"),
        );
        let engine = Engine::from_database_with_options(Arc::new(database), options)
            .expect("open rollout comparison Engine");
        let runtime = Builder::new_multi_thread()
            .worker_threads(controls.runtime_threads)
            .max_blocking_threads(controls.blocking_threads)
            .enable_all()
            .build()
            .expect("create rollout benchmark runtime");
        Self {
            storage,
            engine,
            runtime,
            hash_keys,
            expected_scatter_fingerprint,
            native_table_id,
            hilo_table_id,
            baseline_digest: template.digest,
            temp,
        }
    }

    fn table_id(&self, workload: RolloutWorkload) -> u64 {
        match workload {
            RolloutWorkload::NativeRangeWrite => self.native_table_id,
            RolloutWorkload::HiloWrite => self.hilo_table_id,
            _ => panic!("non-generated rollout workload has no generated table ID"),
        }
    }

    fn run_case(
        &self,
        controls: RolloutControls,
        case: RolloutCase,
    ) -> Result<MeasuredRolloutRun, String> {
        if let Some(reason) = rollout_unavailable_reason(case) {
            return Err(format!("unsupported rollout case reached runner: {reason}"));
        }
        match (case.path, case.workload) {
            (RolloutPath::VirtualTable, RolloutWorkload::PointRead) => {
                let clients = self.read_clients(case.clients)?;
                let keys = &self.hash_keys;
                run_sync_clients(
                    self,
                    controls,
                    case,
                    clients,
                    move |coordinator, worker, operation| {
                        let shard = rollout_key_index(worker, operation, keys.len())?;
                        let key = keys[shard];
                        let _ = coordinator.take_opened_shards();
                        let row = coordinator
                            .connection()
                            .query_row(
                                HASH_POINT_SQL,
                                params![key, (ROWS_PER_SHARD / 2) as i64],
                                |row| {
                                    Ok((
                                        row.get::<_, i64>(0)?,
                                        row.get::<_, i64>(1)?,
                                        row.get::<_, String>(2)?,
                                    ))
                                },
                            )
                            .map_err(crate::sqlite_error::storage)?;
                        if row
                            != (
                                key,
                                (ROWS_PER_SHARD / 2) as i64,
                                format!("issue-131-hash-{shard:02}-{:04}", ROWS_PER_SHARD / 2),
                            )
                        {
                            return Err(EngineError::new(
                                EngineErrorKind::DataCorruption,
                                "rollout vtab point read returned the wrong row",
                            ));
                        }
                        Ok(RolloutOperationSuccess::read(
                            coordinator.take_opened_shards(),
                        ))
                    },
                )
            }
            (RolloutPath::VirtualTable, RolloutWorkload::ScatterRead) => {
                let clients = self.read_clients(case.clients)?;
                let expected_fingerprint = self.expected_scatter_fingerprint;
                run_sync_clients(
                    self,
                    controls,
                    case,
                    clients,
                    move |coordinator, _, operation| {
                        let _ = coordinator.take_opened_shards();
                        let rows = coordinator
                            .connection()
                            .prepare(HASH_FULL_SQL)
                            .and_then(|mut statement| {
                                statement
                                    .query_map([], |row| {
                                        Ok((
                                            row.get::<_, i64>(0)?,
                                            row.get::<_, i64>(1)?,
                                            row.get::<_, String>(2)?,
                                        ))
                                    })?
                                    .collect::<Result<Vec<_>, _>>()
                            })
                            .map_err(crate::sqlite_error::storage)?;
                        if rows.len() != usize::from(controls.shards) * ROWS_PER_SHARD {
                            return Err(EngineError::new(
                                EngineErrorKind::DataCorruption,
                                "rollout vtab scatter read returned the wrong row count",
                            ));
                        }
                        if operation < controls.warmup_operations_per_client as u64 {
                            validate_rollout_scatter_rows(&rows, expected_fingerprint)?;
                        }
                        black_box(rows);
                        Ok(RolloutOperationSuccess::read(
                            coordinator.take_opened_shards(),
                        ))
                    },
                )
            }
            (RolloutPath::VirtualTable, RolloutWorkload::ExplicitWrite) => {
                let clients = self.write_clients(case.clients)?;
                let keys = &self.hash_keys;
                run_sync_clients(
                    self,
                    controls,
                    case,
                    clients,
                    move |coordinator, worker, op| {
                        let key = keys[rollout_key_index(worker, op, keys.len())?];
                        let row_no = rollout_row_number(worker, op)?;
                        let result = coordinator.execute_dml(
                            EXPLICIT_INSERT_SQL,
                            params![key, row_no, "issue-131-explicit"],
                        )?;
                        if result.affected_rows() != 1 {
                            return Err(EngineError::new(
                                EngineErrorKind::Internal,
                                "rollout explicit INSERT did not affect one row",
                            ));
                        }
                        result
                            .shard()
                            .map(|shard| {
                                RolloutOperationSuccess::explicit_write(
                                    shard,
                                    key,
                                    row_no,
                                    "issue-131-explicit",
                                )
                            })
                            .ok_or_else(|| {
                                EngineError::new(
                                    EngineErrorKind::Internal,
                                    "rollout explicit INSERT did not report a shard",
                                )
                            })
                    },
                )
            }
            (
                RolloutPath::VirtualTable,
                workload @ (RolloutWorkload::NativeRangeWrite | RolloutWorkload::HiloWrite),
            ) => {
                let clients = self.write_clients(case.clients)?;
                let table_id = self.table_id(workload);
                run_sync_clients(self, controls, case, clients, move |coordinator, _, _| {
                    let result = coordinator.execute_generated_dml_auto(
                        workload.sql(),
                        params!["issue-131-generated"],
                        table_id,
                    )?;
                    if result.affected_rows() != 1 || result.generated_key().is_none() {
                        return Err(EngineError::new(
                            EngineErrorKind::Internal,
                            "rollout generated INSERT did not return one row and a key",
                        ));
                    }
                    let key = result
                        .generated_key()
                        .and_then(|key| key.value.as_i64())
                        .ok_or_else(|| {
                            EngineError::new(
                                EngineErrorKind::Internal,
                                "rollout generated INSERT returned a non-integer key",
                            )
                        })?;
                    result
                        .shard()
                        .map(|shard| RolloutOperationSuccess::generated_write(shard, key))
                        .ok_or_else(|| {
                            EngineError::new(
                                EngineErrorKind::Internal,
                                "rollout generated INSERT did not report a shard",
                            )
                        })
                })
            }
            (RolloutPath::ExistingRouter, RolloutWorkload::PointRead) => {
                let clients = self.sessions(case.clients);
                let keys = &self.hash_keys;
                let engine = self.engine.clone();
                let handle = self.runtime.handle().clone();
                run_sync_clients(
                    self,
                    controls,
                    case,
                    clients,
                    move |session, worker, operation| {
                        let shard = rollout_key_index(worker, operation, keys.len())?;
                        let result = handle.block_on(engine.query_logical(
                            session,
                            Statement::new(
                                HASH_POINT_SQL,
                                vec![
                                    Value::from(keys[shard]),
                                    Value::from((ROWS_PER_SHARD / 2) as i64),
                                ],
                            ),
                        ))?;
                        let rows = result_set_hash_rows(&result.value);
                        if rows
                            != [(
                                keys[shard],
                                (ROWS_PER_SHARD / 2) as i64,
                                format!("issue-131-hash-{shard:02}-{:04}", ROWS_PER_SHARD / 2),
                            )]
                        {
                            return Err(EngineError::new(
                                EngineErrorKind::DataCorruption,
                                "rollout Engine point read returned the wrong row",
                            ));
                        }
                        Ok(RolloutOperationSuccess::read(result.shards))
                    },
                )
            }
            (RolloutPath::ExistingRouter, RolloutWorkload::ScatterRead) => {
                let clients = self.sessions(case.clients);
                let engine = self.engine.clone();
                let expected_fingerprint = self.expected_scatter_fingerprint;
                let handle = self.runtime.handle().clone();
                run_sync_clients(
                    self,
                    controls,
                    case,
                    clients,
                    move |session, _, operation| {
                        let result = handle.block_on(
                            engine.query_logical(session, Statement::new(HASH_FULL_SQL, vec![])),
                        )?;
                        if result.value.rows().len()
                            != usize::from(controls.shards) * ROWS_PER_SHARD
                        {
                            return Err(EngineError::new(
                                EngineErrorKind::DataCorruption,
                                "rollout Engine scatter read returned the wrong row count",
                            ));
                        }
                        if operation < controls.warmup_operations_per_client as u64 {
                            let rows = result_set_hash_rows(&result.value);
                            validate_rollout_scatter_rows(&rows, expected_fingerprint)?;
                        }
                        black_box(result.value);
                        Ok(RolloutOperationSuccess::read(result.shards))
                    },
                )
            }
            (RolloutPath::ExistingRouter, RolloutWorkload::ExplicitWrite) => {
                let clients = self.sessions(case.clients);
                let keys = &self.hash_keys;
                let engine = self.engine.clone();
                let handle = self.runtime.handle().clone();
                run_sync_clients(self, controls, case, clients, move |session, worker, op| {
                    let result = handle.block_on(engine.execute(
                        session,
                        Statement::new(
                            EXPLICIT_INSERT_SQL,
                            vec![
                                Value::from(keys[rollout_key_index(worker, op, keys.len())?]),
                                Value::from(rollout_row_number(worker, op)?),
                                Value::from("issue-131-explicit"),
                            ],
                        ),
                    ))?;
                    if result.value != 1 {
                        return Err(EngineError::new(
                            EngineErrorKind::Internal,
                            "rollout Engine explicit INSERT did not affect one row",
                        ));
                    }
                    Ok(RolloutOperationSuccess::explicit_write(
                        result.shard,
                        keys[rollout_key_index(worker, op, keys.len())?],
                        rollout_row_number(worker, op)?,
                        "issue-131-explicit",
                    ))
                })
            }
            (
                RolloutPath::ExistingRouter,
                RolloutWorkload::NativeRangeWrite | RolloutWorkload::HiloWrite,
            ) => unreachable!("unavailable rollout cases are rejected before dispatch"),
        }
    }

    fn read_clients(&self, count: usize) -> Result<Vec<ReadCoordinator>, String> {
        (0..count)
            .map(|_| {
                ReadCoordinator::open(self.storage.clone())
                    .map_err(|error| format!("open rollout read coordinator: {error}"))
            })
            .collect()
    }

    fn write_clients(&self, count: usize) -> Result<Vec<WriteCoordinator>, String> {
        (0..count)
            .map(|_| {
                WriteCoordinator::open(self.storage.clone())
                    .map_err(|error| format!("open rollout write coordinator: {error}"))
            })
            .collect()
    }

    fn sessions(&self, count: usize) -> Vec<Session> {
        (0..count).map(|_| self.engine.session()).collect()
    }

    fn validate_run_outcome(
        &self,
        case: RolloutCase,
        outcome: &RolloutRunOutcome,
        warmup_rows: &BTreeSet<RolloutAcknowledgedRow>,
    ) -> Result<(), String> {
        let expected_rows = outcome
            .acknowledged_rows
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if expected_rows.len() != outcome.acknowledged_rows.len() {
            return Err(format!(
                "{} acknowledged duplicate rows",
                case.workload.name()
            ));
        }
        if matches!(
            case.workload,
            RolloutWorkload::ExplicitWrite
                | RolloutWorkload::NativeRangeWrite
                | RolloutWorkload::HiloWrite
        ) && expected_rows.len()
            != usize::try_from(outcome.successful_operations)
                .map_err(|_| "rollout success count does not fit usize".to_owned())?
        {
            return Err(format!(
                "{} acknowledged-row count does not match successful operations",
                case.workload.name()
            ));
        }
        if matches!(
            case.workload,
            RolloutWorkload::NativeRangeWrite | RolloutWorkload::HiloWrite
        ) && expected_rows
            .iter()
            .map(|row| row.row_id)
            .collect::<BTreeSet<_>>()
            .len()
            != expected_rows.len()
        {
            return Err(format!(
                "{} acknowledged the same generated ID on multiple shards",
                case.workload.name()
            ));
        }

        let mut actual_rows = BTreeSet::new();
        let manifest = Connection::open(self.temp.path().join("manifest.sqlite"))
            .map_err(|error| format!("open manifest for rollout reconciliation: {error}"))?;
        validate_sqlite_integrity(&manifest, "manifest")?;
        for shard in 0..self.storage.shard_count() {
            let connection = self.storage.open_shard(shard).map_err(|error| {
                format!("open shard {shard} for rollout reconciliation: {error}")
            })?;
            validate_sqlite_integrity(&connection, &format!("shard {shard}"))?;
            match case.workload {
                RolloutWorkload::ExplicitWrite => {
                    let mut statement = connection
                        .prepare(
                            "SELECT tenant_id, row_no, payload FROM benchmark_hash \
                             WHERE row_no >= 1000000000",
                        )
                        .map_err(|error| {
                            format!("prepare explicit rollout reconciliation: {error}")
                        })?;
                    let rows = statement
                        .query_map([], |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        })
                        .map_err(|error| format!("query explicit rollout reconciliation: {error}"))?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|error| {
                            format!("read explicit rollout reconciliation: {error}")
                        })?;
                    for (tenant_id, row_no, payload) in rows {
                        let row =
                            RolloutAcknowledgedRow::explicit(shard, tenant_id, row_no, &payload);
                        if !actual_rows.insert(row) {
                            return Err(format!(
                                "rollout physical shard {shard} contains a duplicate explicit row ({tenant_id}, {row_no})"
                            ));
                        }
                    }
                }
                RolloutWorkload::NativeRangeWrite => collect_trial_rows(
                    &connection,
                    shard,
                    "SELECT id, payload FROM benchmark_generated_native",
                    &mut actual_rows,
                )?,
                RolloutWorkload::HiloWrite => collect_trial_rows(
                    &connection,
                    shard,
                    "SELECT id, payload FROM benchmark_generated_hilo",
                    &mut actual_rows,
                )?,
                RolloutWorkload::PointRead | RolloutWorkload::ScatterRead => {}
            }
        }
        let all_physical_rows = actual_rows.clone();
        if matches!(
            case.workload,
            RolloutWorkload::NativeRangeWrite | RolloutWorkload::HiloWrite
        ) && actual_rows
            .iter()
            .map(|row| row.row_id)
            .collect::<BTreeSet<_>>()
            .len()
            != actual_rows.len()
        {
            return Err(format!(
                "{} stored the same generated ID on multiple shards",
                case.workload.name()
            ));
        }
        if matches!(
            case.workload,
            RolloutWorkload::NativeRangeWrite | RolloutWorkload::HiloWrite
        ) && warmup_rows
            .iter()
            .map(|row| row.row_id)
            .collect::<BTreeSet<_>>()
            .len()
            != warmup_rows.len()
        {
            return Err(format!(
                "{} acknowledged the same generated ID during warm-up",
                case.workload.name()
            ));
        }
        actual_rows.retain(|row| !warmup_rows.contains(row));
        if actual_rows != expected_rows {
            let missing = expected_rows.difference(&actual_rows).count();
            let unexpected = actual_rows.difference(&expected_rows).count();
            return Err(format!(
                "{} post-trial reconciliation found missing={missing}, unexpected={unexpected} complete acknowledged rows",
                case.workload.name()
            ));
        }
        if case.workload == RolloutWorkload::ExplicitWrite {
            for row in &all_physical_rows {
                let tenant_id = row.tenant_id.ok_or_else(|| {
                    "explicit rollout physical row is missing its tenant identity".to_owned()
                })?;
                let encoded =
                    canonical_shard_key_bytes(crate::core::CanonicalShardKeyRef::Int64(tenant_id));
                let expected = self.storage.shard_for_key(encoded.as_ref());
                if row.shard != expected {
                    return Err(format!(
                        "explicit rollout tenant {tenant_id} is stored on shard {}, expected {expected}",
                        row.shard
                    ));
                }
            }
        }
        if case.workload == RolloutWorkload::NativeRangeWrite {
            let owners = self
                .storage
                .allocation_owner_map()
                .ok_or_else(|| "native rollout reconciliation has no owner map".to_owned())?;
            for row in &all_physical_rows {
                let shard = row.shard;
                let key = row.row_id;
                let decoded = NativeRangeV1Id::decode(key)
                    .map_err(|error| format!("decode native rollout ID {key}: {error}"))?;
                let expected_owner = owners
                    .owner_for_physical_shard(shard)
                    .ok_or_else(|| format!("native rollout shard {shard} has no owner"))?;
                if decoded.owner() != expected_owner {
                    return Err(format!(
                        "native rollout ID {key} owner does not match physical shard {shard}"
                    ));
                }
            }
        }
        if case.workload == RolloutWorkload::HiloWrite {
            for row in &all_physical_rows {
                let shard = row.shard;
                let key = row.row_id;
                HiloV1Id::decode(key)
                    .map_err(|error| format!("decode hilo rollout ID {key}: {error}"))?;
                let encoded =
                    canonical_shard_key_bytes(crate::core::CanonicalShardKeyRef::Int64(key));
                let expected = self.storage.shard_for_key(encoded.as_ref());
                if shard != expected {
                    return Err(format!(
                        "hilo rollout ID {key} is stored on shard {shard}, expected {expected}"
                    ));
                }
            }
        }
        Ok(())
    }
}

fn validate_sqlite_integrity(connection: &Connection, label: &str) -> Result<(), String> {
    let integrity = connection
        .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
        .map_err(|error| format!("quick_check {label}: {error}"))?;
    if integrity != "ok" {
        return Err(format!(
            "rollout post-trial quick_check failed on {label}: {integrity}"
        ));
    }
    let foreign_key_errors = connection
        .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(|error| format!("foreign_key_check {label}: {error}"))?;
    if foreign_key_errors != 0 {
        return Err(format!(
            "rollout post-trial foreign_key_check found {foreign_key_errors} violations on {label}"
        ));
    }
    Ok(())
}

fn collect_trial_rows(
    connection: &Connection,
    shard: u16,
    sql: &str,
    actual: &mut BTreeSet<RolloutAcknowledgedRow>,
) -> Result<(), String> {
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| format!("prepare rollout reconciliation query: {error}"))?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| format!("query rollout reconciliation rows: {error}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("read rollout reconciliation row: {error}"))?;
    for (row_id, payload) in rows {
        if !actual.insert(RolloutAcknowledgedRow::generated(shard, row_id, &payload)) {
            return Err(format!(
                "rollout physical shard {shard} contains duplicate generated row {row_id}"
            ));
        }
    }
    Ok(())
}

fn seed_rollout_hash_rows(storage: &Storage, hash_keys: &[i64]) {
    for shard in 0..storage.shard_count() {
        let mut connection = storage
            .open_shard(shard)
            .expect("open rollout physical shard for seeding");
        let transaction = connection
            .transaction()
            .expect("start rollout seed transaction");
        {
            let mut insert = transaction
                .prepare(EXPLICIT_INSERT_SQL)
                .expect("prepare rollout hash seed INSERT");
            for row_no in 0..ROWS_PER_SHARD {
                insert
                    .execute(params![
                        hash_keys[usize::from(shard)],
                        row_no as i64,
                        format!("issue-131-hash-{shard:02}-{row_no:04}")
                    ])
                    .expect("insert rollout hash seed row");
            }
        }
        transaction
            .commit()
            .expect("commit rollout hash seed transaction");
    }
}

fn copy_rollout_fixture(source: &Path, destination: &Path) {
    for relative in rollout_fixture_files(source) {
        let source_file = source.join(&relative);
        let destination_file = destination.join(&relative);
        if let Some(parent) = destination_file.parent() {
            fs::create_dir_all(parent).expect("create rollout clone directory");
        }
        fs::copy(&source_file, &destination_file).unwrap_or_else(|error| {
            panic!(
                "copy rollout fixture {} to {}: {error}",
                source_file.display(),
                destination_file.display()
            )
        });
    }
}

fn rollout_fixture_digest(root: &Path) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for relative in rollout_fixture_files(root) {
        let name = relative.to_string_lossy();
        let contents = fs::read(root.join(&relative))
            .unwrap_or_else(|error| panic!("read rollout fixture {}: {error}", relative.display()));
        hasher.update(&(name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(&(contents.len() as u64).to_le_bytes());
        hasher.update(&contents);
    }
    *hasher.finalize().as_bytes()
}

fn rollout_fixture_files(root: &Path) -> Vec<PathBuf> {
    fn visit(root: &Path, directory: &Path, files: &mut Vec<PathBuf>) {
        let mut entries = fs::read_dir(directory)
            .unwrap_or_else(|error| panic!("read rollout fixture directory: {error}"))
            .collect::<Result<Vec<_>, _>>()
            .expect("read every rollout fixture directory entry");
        entries.sort_unstable_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            let file_type = entry
                .file_type()
                .expect("inspect rollout fixture entry type");
            if file_type.is_dir() {
                visit(root, &path, files);
            } else if file_type.is_file() {
                if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with("-shm"))
                {
                    continue;
                }
                files.push(
                    path.strip_prefix(root)
                        .expect("rollout fixture file is below its root")
                        .to_owned(),
                );
            } else {
                panic!(
                    "rollout fixture contains unsupported entry {}",
                    path.display()
                );
            }
        }
    }

    let mut files = Vec::new();
    visit(root, root, &mut files);
    files.sort_unstable();
    files
}

fn rollout_row_number(worker: usize, operation: u64) -> EngineResult<i64> {
    let worker = u64::try_from(worker).map_err(|_| {
        EngineError::new(
            EngineErrorKind::NumericOutOfRange,
            "rollout worker index does not fit u64",
        )
    })?;
    let value = 1_000_000_000_u64
        .checked_add(worker.checked_mul(100_000_000).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                "rollout worker row-number range overflowed",
            )
        })?)
        .and_then(|base| base.checked_add(operation))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                "rollout row number overflowed",
            )
        })?;
    Ok(value)
}

fn rollout_key_index(worker: usize, operation: u64, key_count: usize) -> EngineResult<usize> {
    let operation = usize::try_from(operation).map_err(|_| {
        EngineError::new(
            EngineErrorKind::NumericOutOfRange,
            "rollout operation index does not fit usize",
        )
    })?;
    worker
        .checked_add(operation)
        .map(|index| index % key_count)
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                "rollout key rotation overflowed",
            )
        })
}

fn validate_rollout_scatter_rows(
    rows: &[HashRow],
    expected_fingerprint: [u8; 32],
) -> EngineResult<()> {
    if rows.len() != usize::from(ROLLOUT_SHARDS) * ROWS_PER_SHARD {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "rollout scatter read returned the wrong row count",
        ));
    }
    let fingerprint = rollout_rows_fingerprint(
        rows.iter()
            .map(|(key, row_no, payload)| (*key, *row_no, payload.as_str())),
    );
    if fingerprint != expected_fingerprint {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "rollout scatter read returned the wrong content fingerprint",
        ));
    }
    Ok(())
}

fn rollout_expected_scatter_fingerprint(keys: &[i64]) -> [u8; 32] {
    rollout_rows_fingerprint(keys.iter().enumerate().flat_map(|(shard, &key)| {
        (0..ROWS_PER_SHARD).map(move |row_no| {
            let payload = format!("issue-131-hash-{shard:02}-{row_no:04}");
            (key, row_no as i64, payload)
        })
    }))
}

fn rollout_rows_fingerprint<I, S>(rows: I) -> [u8; 32]
where
    I: IntoIterator<Item = (i64, i64, S)>,
    S: AsRef<str>,
{
    let mut xor = [0_u8; 32];
    let mut sum = [0_u64; 4];
    for (key, row_no, payload) in rows {
        let payload = payload.as_ref();
        let mut row = blake3::Hasher::new();
        row.update(&key.to_le_bytes());
        row.update(&row_no.to_le_bytes());
        row.update(&(payload.len() as u64).to_le_bytes());
        row.update(payload.as_bytes());
        let digest = *row.finalize().as_bytes();
        for (target, value) in xor.iter_mut().zip(digest) {
            *target ^= value;
        }
        for (index, chunk) in digest.chunks_exact(8).enumerate() {
            let value = u64::from_le_bytes(chunk.try_into().expect("digest chunk is eight bytes"));
            sum[index] = sum[index].wrapping_add(value);
        }
    }
    let mut combined = blake3::Hasher::new();
    combined.update(&xor);
    for value in sum {
        combined.update(&value.to_le_bytes());
    }
    *combined.finalize().as_bytes()
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
struct RolloutAcknowledgedRow {
    shard: u16,
    tenant_id: Option<i64>,
    row_id: i64,
    payload: String,
}

impl RolloutAcknowledgedRow {
    fn explicit(shard: u16, tenant_id: i64, row_id: i64, payload: &str) -> Self {
        Self {
            shard,
            tenant_id: Some(tenant_id),
            row_id,
            payload: payload.to_owned(),
        }
    }

    fn generated(shard: u16, row_id: i64, payload: &str) -> Self {
        Self {
            shard,
            tenant_id: None,
            row_id,
            payload: payload.to_owned(),
        }
    }
}

#[derive(Default)]
struct RolloutRunOutcome {
    successful_operations: u64,
    errors: RolloutErrorCounts,
    successful_shard_touches: Vec<u64>,
    successful_operations_by_worker: Vec<u64>,
    acknowledged_rows: Vec<RolloutAcknowledgedRow>,
}

impl RolloutRunOutcome {
    fn with_shards(shard_count: u16, clients: usize) -> Self {
        Self {
            successful_shard_touches: vec![0; usize::from(shard_count)],
            successful_operations_by_worker: vec![0; clients],
            ..Self::default()
        }
    }

    fn record(
        &mut self,
        controls: RolloutControls,
        case: RolloutCase,
        worker: usize,
        result: EngineResult<RolloutOperationSuccess>,
    ) {
        match result {
            Ok(success) => {
                if validate_rollout_shards(controls, case, &success.shards).is_err() {
                    self.errors.record(RolloutErrorClass::Other);
                    return;
                }
                if matches!(
                    case.workload,
                    RolloutWorkload::ExplicitWrite
                        | RolloutWorkload::NativeRangeWrite
                        | RolloutWorkload::HiloWrite
                ) && success.acknowledged_row.is_none()
                {
                    self.errors.record(RolloutErrorClass::Other);
                    return;
                }
                self.successful_operations = self
                    .successful_operations
                    .checked_add(1)
                    .expect("rollout benchmark operation count fits u64");
                self.successful_operations_by_worker[worker] = self.successful_operations_by_worker
                    [worker]
                    .checked_add(1)
                    .expect("rollout per-worker success count fits u64");
                for shard in success.shards {
                    let counter = &mut self.successful_shard_touches[usize::from(shard)];
                    *counter = counter
                        .checked_add(1)
                        .expect("rollout benchmark shard-touch count fits u64");
                }
                if let Some(row) = success.acknowledged_row {
                    self.acknowledged_rows.push(row);
                }
            }
            Err(error) => self.errors.record(classify_rollout_error(error.kind())),
        }
    }

    fn merge(&mut self, other: Self) {
        self.successful_operations = self
            .successful_operations
            .checked_add(other.successful_operations)
            .expect("rollout benchmark operation count fits u64");
        self.errors.merge(other.errors);
        for (total, value) in self
            .successful_shard_touches
            .iter_mut()
            .zip(other.successful_shard_touches)
        {
            *total = total
                .checked_add(value)
                .expect("rollout benchmark shard-touch count fits u64");
        }
        self.acknowledged_rows.extend(other.acknowledged_rows);
        for (total, value) in self
            .successful_operations_by_worker
            .iter_mut()
            .zip(other.successful_operations_by_worker)
        {
            *total = total
                .checked_add(value)
                .expect("rollout per-worker success count fits u64");
        }
    }
}

struct RolloutOperationSuccess {
    shards: Vec<u16>,
    acknowledged_row: Option<RolloutAcknowledgedRow>,
}

impl RolloutOperationSuccess {
    fn read(shards: Vec<u16>) -> Self {
        Self {
            shards,
            acknowledged_row: None,
        }
    }

    fn explicit_write(shard: u16, tenant_id: i64, row_id: i64, payload: &str) -> Self {
        Self {
            shards: vec![shard],
            acknowledged_row: Some(RolloutAcknowledgedRow::explicit(
                shard, tenant_id, row_id, payload,
            )),
        }
    }

    fn generated_write(shard: u16, row_id: i64) -> Self {
        Self {
            shards: vec![shard],
            acknowledged_row: Some(RolloutAcknowledgedRow::generated(
                shard,
                row_id,
                "issue-131-generated",
            )),
        }
    }
}

struct MeasuredRolloutRun {
    baseline_digest: [u8; 32],
    initial: RolloutTelemetryObservation,
    observations: Vec<RolloutTelemetryObservation>,
    outcome: RolloutRunOutcome,
    elapsed: Duration,
}

impl MeasuredRolloutRun {
    fn into_metrics(
        self,
        controls: RolloutControls,
        case: RolloutCase,
        trial: usize,
    ) -> Result<RolloutTrialMetrics, String> {
        let mut recorder =
            RolloutTrialRecorder::new(controls, case, trial, self.baseline_digest, self.initial)?;
        recorder.record_run_outcome(self.outcome)?;
        for observation in self.observations {
            recorder.observe(observation)?;
        }
        recorder.finish(self.elapsed)
    }
}

fn run_sync_clients<C, F>(
    fixture: &RolloutBenchmarkFixture,
    controls: RolloutControls,
    case: RolloutCase,
    mut clients: Vec<C>,
    operation: F,
) -> Result<MeasuredRolloutRun, String>
where
    C: Send,
    F: Fn(&mut C, usize, u64) -> EngineResult<RolloutOperationSuccess> + Sync,
{
    let mut warmup_rows = BTreeSet::new();
    for (worker, client) in clients.iter_mut().enumerate() {
        for operation_index in 0..controls.warmup_operations_per_client {
            let operation_index =
                u64::try_from(operation_index).expect("rollout warm-up operation index fits u64");
            let success = operation(client, worker, operation_index)
                .map_err(|error| format!("rollout warm-up operation failed: {error}"))?;
            validate_rollout_shards(controls, case, &success.shards)?;
            if let Some(row) = success.acknowledged_row {
                if !warmup_rows.insert(row) {
                    return Err(format!(
                        "{} acknowledged a duplicate row during warm-up",
                        case.workload.name()
                    ));
                }
            }
        }
    }

    let engine = (case.path == RolloutPath::ExistingRouter).then_some(&fixture.engine);
    let initial = sample_rollout_telemetry(fixture.temp.path(), controls.shards, engine)?;
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(clients.len() + 1));
    let mut observations = Vec::new();
    let mut sample_error = None;
    let (outcome, elapsed) = thread::scope(|scope| {
        let handles = clients
            .into_iter()
            .enumerate()
            .map(|(worker, mut client)| {
                let stop = Arc::clone(&stop);
                let barrier = Arc::clone(&barrier);
                let operation = &operation;
                scope.spawn(move || {
                    let mut outcome = RolloutRunOutcome::with_shards(controls.shards, case.clients);
                    let mut operation_index = u64::try_from(controls.warmup_operations_per_client)
                        .expect("rollout warm-up count fits u64");
                    barrier.wait();
                    while !stop.load(Ordering::Acquire) {
                        outcome.record(
                            controls,
                            case,
                            worker,
                            operation(&mut client, worker, operation_index),
                        );
                        operation_index = operation_index
                            .checked_add(1)
                            .expect("rollout per-worker operation index fits u64");
                    }
                    outcome
                })
            })
            .collect::<Vec<_>>();
        let started = Instant::now();
        barrier.wait();
        while started.elapsed() < controls.trial_duration {
            let remaining = controls.trial_duration.saturating_sub(started.elapsed());
            thread::sleep(controls.telemetry_interval.min(remaining));
            match sample_rollout_telemetry(fixture.temp.path(), controls.shards, engine) {
                Ok(observation) => observations.push(observation),
                Err(error) => {
                    sample_error = Some(error);
                    break;
                }
            }
        }
        stop.store(true, Ordering::Release);
        let mut total = RolloutRunOutcome::with_shards(controls.shards, case.clients);
        for handle in handles {
            total.merge(handle.join().expect("rollout benchmark worker panicked"));
        }
        (total, started.elapsed())
    });
    if let Some(error) = sample_error {
        return Err(error);
    }
    observations.push(sample_rollout_telemetry(
        fixture.temp.path(),
        controls.shards,
        engine,
    )?);
    fixture.validate_run_outcome(case, &outcome, &warmup_rows)?;
    Ok(MeasuredRolloutRun {
        baseline_digest: fixture.baseline_digest,
        initial,
        observations,
        outcome,
        elapsed,
    })
}

fn validate_rollout_shards(
    controls: RolloutControls,
    case: RolloutCase,
    shards: &[u16],
) -> Result<(), String> {
    let actual = shards.iter().copied().collect::<BTreeSet<_>>();
    if actual.len() != shards.len() || actual.iter().any(|&shard| shard >= controls.shards) {
        return Err(format!(
            "{} reported invalid shard touches {shards:?}",
            case.workload.name()
        ));
    }
    if case.workload.is_scatter() {
        let expected = (0..controls.shards).collect::<BTreeSet<_>>();
        if actual != expected {
            return Err(format!(
                "scatter read touched {actual:?}, expected every shard {expected:?}"
            ));
        }
    } else if actual.len() != 1 {
        return Err(format!(
            "{} must touch exactly one shard; got {actual:?}",
            case.workload.name()
        ));
    }
    Ok(())
}

fn classify_rollout_error(kind: EngineErrorKind) -> RolloutErrorClass {
    match kind {
        EngineErrorKind::Busy => RolloutErrorClass::Busy,
        EngineErrorKind::Cancelled | EngineErrorKind::DeadlineExceeded => {
            RolloutErrorClass::Cancelled
        }
        EngineErrorKind::ConstraintViolation
        | EngineErrorKind::UniqueViolation
        | EngineErrorKind::NotNullViolation
        | EngineErrorKind::ForeignKeyViolation
        | EngineErrorKind::CheckViolation => RolloutErrorClass::Constraint,
        EngineErrorKind::DataCorruption => RolloutErrorClass::Corrupt,
        EngineErrorKind::StorageFull => RolloutErrorClass::DiskFull,
        _ => RolloutErrorClass::Other,
    }
}

#[derive(Debug, Clone, PartialEq)]
struct RolloutTrialMetrics {
    case: RolloutCase,
    trial: usize,
    baseline_digest: [u8; 32],
    successful_operations: u64,
    errors: RolloutErrorCounts,
    elapsed: Duration,
    process_cpu: Duration,
    baseline_resident_bytes: u64,
    resident_bytes: u64,
    sampled_peak_resident_bytes: u64,
    process_lifetime_peak_resident_bytes: u64,
    wal_bytes_before: u64,
    wal_bytes_after: u64,
    peak_wal_bytes: u64,
    peak_pool_active: usize,
    peak_pool_queued: usize,
    max_pool_active_by_shard: Vec<usize>,
    max_pool_queued_by_shard: Vec<usize>,
    successful_shard_touches: Vec<u64>,
    minimum_successful_by_worker: u64,
    telemetry_samples: usize,
}

impl RolloutTrialMetrics {
    fn attempted_operations(&self) -> u64 {
        self.successful_operations
            .checked_add(self.errors.total())
            .expect("rollout benchmark attempt count fits u64")
    }

    fn operations_per_second(&self) -> f64 {
        self.successful_operations as f64 / self.elapsed.as_secs_f64()
    }

    fn cpu_percent(&self) -> f64 {
        self.process_cpu.as_secs_f64() / self.elapsed.as_secs_f64() * 100.0
    }

    fn sampled_peak_resident_growth_bytes(&self) -> u64 {
        self.sampled_peak_resident_bytes
            .saturating_sub(self.baseline_resident_bytes)
    }

    fn wal_growth_bytes(&self) -> i128 {
        i128::from(self.wal_bytes_after) - i128::from(self.wal_bytes_before)
    }

    fn shard_min(&self) -> u64 {
        self.successful_shard_touches
            .iter()
            .copied()
            .min()
            .unwrap_or(0)
    }

    fn shard_max(&self) -> u64 {
        self.successful_shard_touches
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
    }

    fn shard_mean(&self) -> f64 {
        self.successful_shard_touches.iter().sum::<u64>() as f64
            / self.successful_shard_touches.len() as f64
    }

    fn shard_max_over_mean(&self) -> f64 {
        let mean = self.shard_mean();
        if mean == 0.0 {
            0.0
        } else {
            self.shard_max() as f64 / mean
        }
    }
}

struct RolloutTrialRecorder {
    controls: RolloutControls,
    case: RolloutCase,
    trial: usize,
    baseline_digest: [u8; 32],
    successful_operations: u64,
    errors: RolloutErrorCounts,
    cpu_before: Duration,
    cpu_after: Duration,
    baseline_resident_bytes: u64,
    resident_bytes: u64,
    sampled_peak_resident_bytes: u64,
    process_lifetime_peak_resident_bytes: u64,
    wal_bytes_before: u64,
    wal_bytes_after: u64,
    peak_wal_bytes: u64,
    peak_pool_active: usize,
    peak_pool_queued: usize,
    max_pool_active_by_shard: Vec<usize>,
    max_pool_queued_by_shard: Vec<usize>,
    successful_shard_touches: Vec<u64>,
    minimum_successful_by_worker: u64,
    telemetry_samples: usize,
}

impl RolloutTrialRecorder {
    fn new(
        controls: RolloutControls,
        case: RolloutCase,
        trial: usize,
        baseline_digest: [u8; 32],
        initial: RolloutTelemetryObservation,
    ) -> Result<Self, String> {
        initial.validate(controls.shards)?;
        if !(1..=controls.trial_count).contains(&trial) {
            return Err(format!(
                "rollout trial {trial} is outside 1..={}",
                controls.trial_count
            ));
        }
        let peak_pool_active = initial.pool_active_by_shard.iter().sum();
        let peak_pool_queued = initial.pool_queued_by_shard.iter().sum();
        Ok(Self {
            controls,
            case,
            trial,
            baseline_digest,
            successful_operations: 0,
            errors: RolloutErrorCounts::default(),
            cpu_before: initial.process_cpu,
            cpu_after: initial.process_cpu,
            baseline_resident_bytes: initial.resident_bytes,
            resident_bytes: initial.resident_bytes,
            sampled_peak_resident_bytes: initial.resident_bytes,
            process_lifetime_peak_resident_bytes: initial.process_lifetime_peak_resident_bytes,
            wal_bytes_before: initial.wal_bytes,
            wal_bytes_after: initial.wal_bytes,
            peak_wal_bytes: initial.wal_bytes,
            peak_pool_active,
            peak_pool_queued,
            max_pool_active_by_shard: initial.pool_active_by_shard,
            max_pool_queued_by_shard: initial.pool_queued_by_shard,
            successful_shard_touches: vec![0; usize::from(controls.shards)],
            minimum_successful_by_worker: 0,
            telemetry_samples: 1,
        })
    }

    fn record_success(&mut self, touched_shards: &[u16]) -> Result<(), String> {
        let expected = if self.case.workload.is_scatter() {
            (0..self.controls.shards).collect::<BTreeSet<_>>()
        } else if touched_shards.len() == 1 {
            BTreeSet::from([touched_shards[0]])
        } else {
            return Err(format!(
                "{} must touch exactly one shard per successful operation",
                self.case.workload.name()
            ));
        };
        let actual = touched_shards.iter().copied().collect::<BTreeSet<_>>();
        if actual.len() != touched_shards.len()
            || actual != expected
            || actual.iter().any(|&shard| shard >= self.controls.shards)
        {
            return Err(format!(
                "{} reported invalid successful shard touches {touched_shards:?}",
                self.case.workload.name()
            ));
        }
        for &shard in touched_shards {
            let counter = &mut self.successful_shard_touches[usize::from(shard)];
            *counter = counter
                .checked_add(1)
                .expect("rollout benchmark shard-touch count fits u64");
        }
        self.successful_operations = self
            .successful_operations
            .checked_add(1)
            .expect("rollout benchmark operation count fits u64");
        Ok(())
    }

    fn record_error(&mut self, class: RolloutErrorClass) {
        self.errors.record(class);
    }

    fn record_run_outcome(&mut self, outcome: RolloutRunOutcome) -> Result<(), String> {
        if outcome.successful_shard_touches.len() != usize::from(self.controls.shards) {
            return Err("rollout outcome has the wrong shard-touch vector length".to_owned());
        }
        let touches = outcome.successful_shard_touches.iter().sum::<u64>();
        let touches_per_success = if self.case.workload.is_scatter() {
            u64::from(self.controls.shards)
        } else {
            1
        };
        let expected_touches = outcome
            .successful_operations
            .checked_mul(touches_per_success)
            .ok_or_else(|| "rollout expected shard-touch count overflowed".to_owned())?;
        if touches != expected_touches {
            return Err(format!(
                "rollout outcome recorded {touches} shard touches, expected {expected_touches}"
            ));
        }
        self.successful_operations = outcome.successful_operations;
        self.errors = outcome.errors;
        self.successful_shard_touches = outcome.successful_shard_touches;
        self.minimum_successful_by_worker = outcome
            .successful_operations_by_worker
            .iter()
            .copied()
            .min()
            .unwrap_or(0);
        Ok(())
    }

    fn observe(&mut self, observation: RolloutTelemetryObservation) -> Result<(), String> {
        observation.validate(self.controls.shards)?;
        if observation.process_cpu < self.cpu_after {
            return Err("rollout cumulative process CPU moved backwards".to_owned());
        }
        self.cpu_after = observation.process_cpu;
        self.sampled_peak_resident_bytes = self
            .sampled_peak_resident_bytes
            .max(observation.resident_bytes);
        self.process_lifetime_peak_resident_bytes = self
            .process_lifetime_peak_resident_bytes
            .max(observation.process_lifetime_peak_resident_bytes);
        self.resident_bytes = observation.resident_bytes;
        self.wal_bytes_after = observation.wal_bytes;
        self.peak_wal_bytes = self.peak_wal_bytes.max(observation.wal_bytes);
        self.peak_pool_active = self
            .peak_pool_active
            .max(observation.pool_active_by_shard.iter().sum());
        self.peak_pool_queued = self
            .peak_pool_queued
            .max(observation.pool_queued_by_shard.iter().sum());
        for (maximum, observed) in self
            .max_pool_active_by_shard
            .iter_mut()
            .zip(observation.pool_active_by_shard)
        {
            *maximum = (*maximum).max(observed);
        }
        for (maximum, observed) in self
            .max_pool_queued_by_shard
            .iter_mut()
            .zip(observation.pool_queued_by_shard)
        {
            *maximum = (*maximum).max(observed);
        }
        self.telemetry_samples = self
            .telemetry_samples
            .checked_add(1)
            .expect("rollout telemetry sample count fits usize");
        Ok(())
    }

    fn finish(self, elapsed: Duration) -> Result<RolloutTrialMetrics, String> {
        if elapsed < self.controls.trial_duration {
            return Err(format!(
                "rollout trial elapsed {elapsed:?} is shorter than the frozen {:?} duration",
                self.controls.trial_duration
            ));
        }
        let process_cpu = self
            .cpu_after
            .checked_sub(self.cpu_before)
            .ok_or_else(|| "rollout cumulative process CPU moved backwards".to_owned())?;
        if self.telemetry_samples < 2 {
            return Err("rollout trial requires baseline and final telemetry samples".to_owned());
        }
        if self.sampled_peak_resident_bytes == 0 {
            return Err("rollout trial did not record resident memory".to_owned());
        }
        if process_cpu.is_zero() {
            return Err("rollout trial did not record process CPU".to_owned());
        }
        let metrics = RolloutTrialMetrics {
            case: self.case,
            trial: self.trial,
            baseline_digest: self.baseline_digest,
            successful_operations: self.successful_operations,
            errors: self.errors,
            elapsed,
            process_cpu,
            baseline_resident_bytes: self.baseline_resident_bytes,
            resident_bytes: self.resident_bytes,
            sampled_peak_resident_bytes: self.sampled_peak_resident_bytes,
            process_lifetime_peak_resident_bytes: self.process_lifetime_peak_resident_bytes,
            wal_bytes_before: self.wal_bytes_before,
            wal_bytes_after: self.wal_bytes_after,
            peak_wal_bytes: self.peak_wal_bytes,
            peak_pool_active: self.peak_pool_active,
            peak_pool_queued: self.peak_pool_queued,
            max_pool_active_by_shard: self.max_pool_active_by_shard,
            max_pool_queued_by_shard: self.max_pool_queued_by_shard,
            successful_shard_touches: self.successful_shard_touches,
            minimum_successful_by_worker: self.minimum_successful_by_worker,
            telemetry_samples: self.telemetry_samples,
        };
        if metrics.attempted_operations() == 0 {
            return Err("rollout trial did not attempt any operations".to_owned());
        }
        Ok(metrics)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RolloutUnavailable {
    case: RolloutCase,
    reason: String,
}

#[derive(Debug, Clone, PartialEq)]
enum RolloutCaseRecord {
    Trial(Box<RolloutTrialMetrics>),
    Unavailable(RolloutUnavailable),
}

#[derive(Debug)]
struct RolloutReport {
    controls: RolloutControls,
    records: Vec<RolloutCaseRecord>,
}

impl RolloutReport {
    fn try_new(controls: RolloutControls, records: Vec<RolloutCaseRecord>) -> Result<Self, String> {
        let expected_cases = rollout_cases().into_iter().collect::<BTreeSet<_>>();
        for record in &records {
            let case = match record {
                RolloutCaseRecord::Trial(metrics) => metrics.case,
                RolloutCaseRecord::Unavailable(unavailable) => unavailable.case,
            };
            if !expected_cases.contains(&case) {
                return Err(format!("rollout report contains unexpected case {case:?}"));
            }
        }
        for case in expected_cases {
            let trials = records
                .iter()
                .filter_map(|record| match record {
                    RolloutCaseRecord::Trial(metrics) if metrics.case == case => {
                        Some(metrics.trial)
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            let unavailable = records
                .iter()
                .filter_map(|record| match record {
                    RolloutCaseRecord::Unavailable(value) if value.case == case => Some(value),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if let Some(required_reason) = rollout_unavailable_reason(case) {
                if !trials.is_empty()
                    || unavailable.len() != 1
                    || unavailable[0].reason != required_reason
                {
                    return Err(format!(
                        "rollout case {case:?} must be unavailable with its canonical reason"
                    ));
                }
                continue;
            }
            if !unavailable.is_empty() {
                return Err(format!(
                    "executable rollout case {case:?} cannot be marked unavailable"
                ));
            }
            let actual_trials = trials.iter().copied().collect::<BTreeSet<_>>();
            let expected_trials = (1..=controls.trial_count).collect::<BTreeSet<_>>();
            if actual_trials.len() != trials.len() || actual_trials != expected_trials {
                return Err(format!(
                    "rollout case {case:?} requires trials 1..={}; got {trials:?}",
                    controls.trial_count
                ));
            }
        }
        for clients in ROLLOUT_CLIENT_MATRIX {
            for workload in RolloutWorkload::ALL {
                for trial in 1..=controls.trial_count {
                    let digests = RolloutPath::ALL
                        .into_iter()
                        .filter_map(|path| {
                            let case = RolloutCase {
                                clients,
                                path,
                                workload,
                            };
                            if rollout_unavailable_reason(case).is_some() {
                                return None;
                            }
                            records.iter().find_map(|record| match record {
                                RolloutCaseRecord::Trial(metrics)
                                    if metrics.case == case && metrics.trial == trial =>
                                {
                                    Some(metrics.baseline_digest)
                                }
                                _ => None,
                            })
                        })
                        .collect::<BTreeSet<_>>();
                    if digests.len() > 1 {
                        return Err(format!(
                            "rollout comparison clients={clients}, workload={workload:?}, trial={trial} did not use byte-identical baseline files"
                        ));
                    }
                }
            }
        }
        Ok(Self { controls, records })
    }

    fn render_tsv(&self) -> String {
        const HEADER: &str = "record\tstatus\treason\tpath\tworkload\tclients\tshards\ttrial\twarmup_per_client\ttarget_trial_ms\ttelemetry_interval_ms\tconnections_per_shard\tqueue_capacity_per_shard\truntime_threads\tblocking_threads\tjournal_mode\tsynchronous\tcache_policy\tbaseline_digest\tsuccessful\terrors\tattempted\telapsed_ms\tops_per_sec\tprocess_cpu_ms\tcpu_percent\tbaseline_resident_bytes\tresident_bytes\tsampled_peak_resident_bytes\tsampled_peak_resident_growth_bytes\tprocess_lifetime_peak_resident_bytes\twal_before_bytes\twal_after_bytes\tpeak_wal_bytes\twal_growth_bytes\tsampled_peak_pool_active\tsampled_peak_pool_queued\tsampled_peak_pool_active_by_shard\tsampled_peak_pool_queued_by_shard\tshard_min\tshard_max\tshard_mean\tshard_max_over_mean\tshard_touches\tbusy\tcancelled\tconstraint\tcorrupt\tdisk_full\tother\ttelemetry_samples";
        let column_count = HEADER.split('\t').count();
        let mut output = format!("{HEADER}\n");
        let mut records = self.records.iter().collect::<Vec<_>>();
        records.sort_by_key(|record| match record {
            RolloutCaseRecord::Trial(metrics) => (metrics.case, metrics.trial),
            RolloutCaseRecord::Unavailable(unavailable) => (unavailable.case, 0),
        });
        for record in records {
            match record {
                RolloutCaseRecord::Trial(metrics) => {
                    let fields = vec![
                        "issue131_rollout".to_owned(),
                        "executed".to_owned(),
                        "-".to_owned(),
                        metrics.case.path.name().to_owned(),
                        metrics.case.workload.name().to_owned(),
                        metrics.case.clients.to_string(),
                        self.controls.shards.to_string(),
                        metrics.trial.to_string(),
                        self.controls.warmup_operations_per_client.to_string(),
                        self.controls.trial_duration.as_millis().to_string(),
                        self.controls.telemetry_interval.as_millis().to_string(),
                        self.controls.connections_per_shard.to_string(),
                        self.controls.queue_capacity_per_shard.to_string(),
                        self.controls.runtime_threads.to_string(),
                        self.controls.blocking_threads.to_string(),
                        self.controls.journal_mode.to_owned(),
                        self.controls.synchronous.to_owned(),
                        self.controls.cache_policy.to_owned(),
                        digest_hex(metrics.baseline_digest),
                        metrics.successful_operations.to_string(),
                        metrics.errors.total().to_string(),
                        metrics.attempted_operations().to_string(),
                        format!("{:.3}", metrics.elapsed.as_secs_f64() * 1_000.0),
                        format!("{:.2}", metrics.operations_per_second()),
                        format!("{:.3}", metrics.process_cpu.as_secs_f64() * 1_000.0),
                        format!("{:.2}", metrics.cpu_percent()),
                        metrics.baseline_resident_bytes.to_string(),
                        metrics.resident_bytes.to_string(),
                        metrics.sampled_peak_resident_bytes.to_string(),
                        metrics.sampled_peak_resident_growth_bytes().to_string(),
                        metrics.process_lifetime_peak_resident_bytes.to_string(),
                        metrics.wal_bytes_before.to_string(),
                        metrics.wal_bytes_after.to_string(),
                        metrics.peak_wal_bytes.to_string(),
                        metrics.wal_growth_bytes().to_string(),
                        metrics.peak_pool_active.to_string(),
                        metrics.peak_pool_queued.to_string(),
                        join_numbers(&metrics.max_pool_active_by_shard),
                        join_numbers(&metrics.max_pool_queued_by_shard),
                        metrics.shard_min().to_string(),
                        metrics.shard_max().to_string(),
                        format!("{:.3}", metrics.shard_mean()),
                        format!("{:.3}", metrics.shard_max_over_mean()),
                        join_numbers(&metrics.successful_shard_touches),
                        metrics.errors.busy.to_string(),
                        metrics.errors.cancelled.to_string(),
                        metrics.errors.constraint.to_string(),
                        metrics.errors.corrupt.to_string(),
                        metrics.errors.disk_full.to_string(),
                        metrics.errors.other.to_string(),
                        metrics.telemetry_samples.to_string(),
                    ];
                    assert_eq!(fields.len(), column_count);
                    writeln!(output, "{}", fields.join("\t"))
                        .expect("writing rollout TSV into a String cannot fail");
                }
                RolloutCaseRecord::Unavailable(unavailable) => {
                    let mut fields = vec!["-".to_owned(); column_count];
                    fields[0] = "issue131_rollout".to_owned();
                    fields[1] = "unsupported".to_owned();
                    fields[2] = unavailable.reason.replace(['\t', '\n', '\r'], " ");
                    fields[3] = unavailable.case.path.name().to_owned();
                    fields[4] = unavailable.case.workload.name().to_owned();
                    fields[5] = unavailable.case.clients.to_string();
                    fields[6] = self.controls.shards.to_string();
                    fields[8] = self.controls.warmup_operations_per_client.to_string();
                    fields[9] = self.controls.trial_duration.as_millis().to_string();
                    fields[10] = self.controls.telemetry_interval.as_millis().to_string();
                    fields[11] = self.controls.connections_per_shard.to_string();
                    fields[12] = self.controls.queue_capacity_per_shard.to_string();
                    fields[13] = self.controls.runtime_threads.to_string();
                    fields[14] = self.controls.blocking_threads.to_string();
                    fields[15] = self.controls.journal_mode.to_owned();
                    fields[16] = self.controls.synchronous.to_owned();
                    fields[17] = self.controls.cache_policy.to_owned();
                    writeln!(output, "{}", fields.join("\t"))
                        .expect("writing rollout TSV into a String cannot fail");
                }
            }
        }
        output
    }

    fn gate_summary(&self) -> RolloutGateSummary {
        RolloutGateSummary::from_report(self)
    }

    fn render_gate_tsv(&self) -> String {
        self.gate_summary().render_tsv()
    }
}

const ROLLOUT_MIN_THROUGHPUT_RATIO: f64 = 0.80;
const ROLLOUT_MAX_RESOURCE_RATIO: f64 = 1.25;
const ROLLOUT_MAX_HILO_SKEW: f64 = 1.25;
const ROLLOUT_MIN_GENERATED_WRITES_PER_CLIENT: u64 = 10_000;

#[derive(Debug)]
struct RolloutGateCase {
    case: RolloutCase,
    status: &'static str,
    reasons: Vec<String>,
    median_successful: Option<u64>,
    median_operations_per_second: Option<f64>,
    median_cpu_ms_per_operation: Option<f64>,
    median_sampled_peak_rss_growth: Option<f64>,
    median_wal_bytes_per_operation: Option<f64>,
    total_errors: Option<u64>,
    worst_shard_spread: Option<u64>,
    worst_shard_max_over_mean: Option<f64>,
    minimum_successful_per_client: Option<u64>,
}

#[derive(Debug)]
struct RolloutGateComparison {
    clients: usize,
    workload: RolloutWorkload,
    status: &'static str,
    reasons: Vec<String>,
    throughput_ratio: f64,
    cpu_ratio: f64,
    sampled_peak_rss_growth_ratio: f64,
    peak_wal_growth_ratio: f64,
}

#[derive(Debug)]
struct RolloutGateSummary {
    cases: Vec<RolloutGateCase>,
    comparisons: Vec<RolloutGateComparison>,
    advance: bool,
    reasons: Vec<String>,
}

impl RolloutGateSummary {
    fn from_report(report: &RolloutReport) -> Self {
        let cases = rollout_cases()
            .into_iter()
            .map(|case| summarize_gate_case(report, case))
            .collect::<Vec<_>>();
        let mut comparisons = Vec::new();
        for clients in ROLLOUT_CLIENT_MATRIX {
            for workload in [
                RolloutWorkload::PointRead,
                RolloutWorkload::ScatterRead,
                RolloutWorkload::ExplicitWrite,
            ] {
                comparisons.push(summarize_gate_comparison(&cases, clients, workload));
            }
        }
        let mut reasons = cases
            .iter()
            .filter(|case| case.status == "FAIL")
            .map(|case| {
                format!(
                    "{} {} clients {}: {}",
                    case.case.path.name(),
                    case.case.clients,
                    case.case.workload.name(),
                    case.reasons.join("; ")
                )
            })
            .collect::<Vec<_>>();
        reasons.extend(
            comparisons
                .iter()
                .filter(|comparison| comparison.status != "PASS")
                .map(|comparison| {
                    format!(
                        "comparison {} clients {}: {}",
                        comparison.clients,
                        comparison.workload.name(),
                        comparison.reasons.join("; ")
                    )
                }),
        );
        reasons.push(
            "sampled RSS growth is diagnostic because trials share one process; isolated trial processes are required for the resource gate"
                .to_owned(),
        );
        reasons.push(
            "sampled WAL file-size growth is diagnostic because checkpoints and WAL reuse make it non-monotonic; frame-level write accounting is required for the resource gate"
                .to_owned(),
        );
        reasons.push(
            "50 ms pool samples cannot prove true maximum occupancy; the pool-capacity gate remains unresolved"
                .to_owned(),
        );
        reasons.push(
            "cross-shard snapshot semantics and live PostgreSQL/MySQL evidence are outside this benchmark gate"
                .to_owned(),
        );
        Self {
            advance: false,
            cases,
            comparisons,
            reasons,
        }
    }

    fn render_tsv(&self) -> String {
        let mut output = String::from(
            "gate_record\tstatus\treason\tpath\tworkload\tclients\tmedian_successful\tmedian_ops_per_sec\tmedian_cpu_ms_per_operation\tmedian_sampled_peak_rss_growth\tmedian_wal_bytes_per_operation\ttotal_errors\tworst_shard_spread\tworst_shard_max_over_mean\tminimum_successful_per_client\tthroughput_ratio\tcpu_per_op_ratio\tsampled_peak_rss_growth_ratio\twal_per_op_ratio\n",
        );
        for case in &self.cases {
            writeln!(
                output,
                "issue131_gate_case\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t-\t-\t-\t-",
                case.status,
                gate_reason(&case.reasons),
                case.case.path.name(),
                case.case.workload.name(),
                case.case.clients,
                option_u64(case.median_successful),
                option_f64(case.median_operations_per_second),
                option_f64(case.median_cpu_ms_per_operation),
                option_f64(case.median_sampled_peak_rss_growth),
                option_f64(case.median_wal_bytes_per_operation),
                option_u64(case.total_errors),
                option_u64(case.worst_shard_spread),
                option_f64(case.worst_shard_max_over_mean),
                option_u64(case.minimum_successful_per_client),
            )
            .expect("writing rollout gate case TSV cannot fail");
        }
        for comparison in &self.comparisons {
            writeln!(
                output,
                "issue131_gate_comparison\t{}\t{}\tvtab_over_engine\t{}\t{}\t-\t-\t-\t-\t-\t-\t-\t-\t-\t{}\t{}\t{}\t{}",
                comparison.status,
                gate_reason(&comparison.reasons),
                comparison.workload.name(),
                comparison.clients,
                ratio_text(comparison.throughput_ratio),
                ratio_text(comparison.cpu_ratio),
                ratio_text(comparison.sampled_peak_rss_growth_ratio),
                ratio_text(comparison.peak_wal_growth_ratio),
            )
            .expect("writing rollout gate comparison TSV cannot fail");
        }
        writeln!(
            output,
            "issue131_benchmark_gate_overall\t{}\t{}\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-",
            if self.advance { "PASS" } else { "HOLD" },
            gate_reason(&self.reasons),
        )
        .expect("writing rollout overall gate TSV cannot fail");
        output
    }
}

fn summarize_gate_case(report: &RolloutReport, case: RolloutCase) -> RolloutGateCase {
    if let Some(reason) = rollout_unavailable_reason(case) {
        return RolloutGateCase {
            case,
            status: "UNAVAILABLE",
            reasons: vec![reason.to_owned()],
            median_successful: None,
            median_operations_per_second: None,
            median_cpu_ms_per_operation: None,
            median_sampled_peak_rss_growth: None,
            median_wal_bytes_per_operation: None,
            total_errors: None,
            worst_shard_spread: None,
            worst_shard_max_over_mean: None,
            minimum_successful_per_client: None,
        };
    }
    let trials = report
        .records
        .iter()
        .filter_map(|record| match record {
            RolloutCaseRecord::Trial(metrics) if metrics.case == case => Some(metrics.as_ref()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let total_errors = trials.iter().map(|trial| trial.errors.total()).sum::<u64>();
    let pool_ok = trials.iter().all(|trial| {
        trial
            .max_pool_active_by_shard
            .iter()
            .all(|&active| active <= report.controls.connections_per_shard)
            && trial
                .max_pool_queued_by_shard
                .iter()
                .all(|&queued| queued <= report.controls.queue_capacity_per_shard)
    });
    let worst_shard_spread = trials
        .iter()
        .map(|trial| trial.shard_max().saturating_sub(trial.shard_min()))
        .max();
    let worst_shard_max_over_mean = trials
        .iter()
        .map(|trial| trial.shard_max_over_mean())
        .max_by(f64::total_cmp);
    let minimum_successful_per_client = trials
        .iter()
        .map(|trial| trial.minimum_successful_by_worker)
        .min();
    let mut reasons = Vec::new();
    if total_errors != 0 {
        reasons.push(format!("{total_errors} classified operation errors"));
    }
    if !pool_ok {
        reasons.push("sampled pool occupancy exceeded configured per-shard capacity".to_owned());
    }
    match case.workload {
        RolloutWorkload::NativeRangeWrite => {
            if minimum_successful_per_client
                .is_none_or(|value| value < ROLLOUT_MIN_GENERATED_WRITES_PER_CLIENT)
            {
                reasons.push(format!(
                    "generated placement requires at least {ROLLOUT_MIN_GENERATED_WRITES_PER_CLIENT} successful writes per client in every trial"
                ));
            } else if worst_shard_spread.is_none_or(|spread| spread > 1) {
                reasons.push("shard placement spread exceeded one successful write".to_owned());
            }
        }
        RolloutWorkload::ExplicitWrite => {
            let maximum_expected_spread =
                u64::try_from(case.clients).expect("rollout client count fits u64");
            if worst_shard_spread.is_none_or(|spread| spread > maximum_expected_spread) {
                reasons.push(format!(
                    "explicit shard spread exceeded the {maximum_expected_spread}-row bound for per-client round-robin assignment"
                ));
            }
        }
        RolloutWorkload::HiloWrite => {
            if minimum_successful_per_client
                .is_none_or(|value| value < ROLLOUT_MIN_GENERATED_WRITES_PER_CLIENT)
            {
                reasons.push(format!(
                    "hi/lo skew requires at least {ROLLOUT_MIN_GENERATED_WRITES_PER_CLIENT} successful writes per client in every trial"
                ));
            } else if worst_shard_max_over_mean.is_none_or(|skew| skew > ROLLOUT_MAX_HILO_SKEW) {
                reasons.push(format!(
                    "hi/lo shard max/mean exceeded {ROLLOUT_MAX_HILO_SKEW:.2}"
                ));
            }
        }
        RolloutWorkload::PointRead | RolloutWorkload::ScatterRead => {}
    }
    RolloutGateCase {
        case,
        status: if reasons.is_empty() { "PASS" } else { "FAIL" },
        reasons,
        median_successful: Some(median_u64(
            trials
                .iter()
                .map(|trial| trial.successful_operations)
                .collect(),
        )),
        median_operations_per_second: Some(median_f64(
            trials
                .iter()
                .map(|trial| trial.operations_per_second())
                .collect(),
        )),
        median_cpu_ms_per_operation: Some(median_f64(
            trials
                .iter()
                .map(|trial| {
                    trial.process_cpu.as_secs_f64() * 1_000.0 / trial.successful_operations as f64
                })
                .collect(),
        )),
        median_sampled_peak_rss_growth: Some(median_f64(
            trials
                .iter()
                .map(|trial| trial.sampled_peak_resident_growth_bytes() as f64)
                .collect(),
        )),
        median_wal_bytes_per_operation: Some(median_f64(
            trials
                .iter()
                .map(|trial| {
                    trial.peak_wal_bytes.saturating_sub(trial.wal_bytes_before) as f64
                        / trial.successful_operations as f64
                })
                .collect(),
        )),
        total_errors: Some(total_errors),
        worst_shard_spread,
        worst_shard_max_over_mean,
        minimum_successful_per_client,
    }
}

fn summarize_gate_comparison(
    cases: &[RolloutGateCase],
    clients: usize,
    workload: RolloutWorkload,
) -> RolloutGateComparison {
    let find = |path| {
        cases
            .iter()
            .find(|case| {
                case.case
                    == RolloutCase {
                        clients,
                        path,
                        workload,
                    }
            })
            .expect("validated rollout gate contains every comparison case")
    };
    let vtab = find(RolloutPath::VirtualTable);
    let engine = find(RolloutPath::ExistingRouter);
    let throughput_ratio = ratio(
        vtab.median_operations_per_second.unwrap(),
        engine.median_operations_per_second.unwrap(),
    );
    let cpu_ratio = ratio(
        vtab.median_cpu_ms_per_operation.unwrap(),
        engine.median_cpu_ms_per_operation.unwrap(),
    );
    let sampled_peak_rss_growth_ratio = ratio(
        vtab.median_sampled_peak_rss_growth.unwrap(),
        engine.median_sampled_peak_rss_growth.unwrap(),
    );
    let peak_wal_growth_ratio = ratio(
        vtab.median_wal_bytes_per_operation.unwrap(),
        engine.median_wal_bytes_per_operation.unwrap(),
    );
    let mut reasons = Vec::new();
    if throughput_ratio < ROLLOUT_MIN_THROUGHPUT_RATIO {
        reasons.push(format!(
            "throughput ratio below {ROLLOUT_MIN_THROUGHPUT_RATIO:.2}"
        ));
    }
    if cpu_ratio > ROLLOUT_MAX_RESOURCE_RATIO {
        reasons.push(format!(
            "CPU ratio exceeded {ROLLOUT_MAX_RESOURCE_RATIO:.2}"
        ));
    }
    if vtab.status != "PASS" || engine.status != "PASS" {
        reasons.push("one or both compared case gates failed".to_owned());
    }
    let has_conclusive_failure = !reasons.is_empty();
    reasons.push(
        "RSS ratio is UNRESOLVED because compared trials share one process; value is diagnostic"
            .to_owned(),
    );
    reasons.push(
        "WAL ratio is UNRESOLVED because sampled file-size growth is non-monotonic across checkpoints and WAL reuse; value is diagnostic"
            .to_owned(),
    );
    RolloutGateComparison {
        clients,
        workload,
        status: if has_conclusive_failure {
            "FAIL"
        } else {
            "UNRESOLVED"
        },
        reasons,
        throughput_ratio,
        cpu_ratio,
        sampled_peak_rss_growth_ratio,
        peak_wal_growth_ratio,
    }
}

fn median_f64(mut values: Vec<f64>) -> f64 {
    assert!(!values.is_empty());
    values.sort_unstable_by(f64::total_cmp);
    values[values.len() / 2]
}

fn median_u64(mut values: Vec<u64>) -> u64 {
    assert!(!values.is_empty());
    values.sort_unstable();
    values[values.len() / 2]
}

fn ratio(numerator: f64, denominator: f64) -> f64 {
    if denominator == 0.0 {
        if numerator == 0.0 { 1.0 } else { f64::INFINITY }
    } else {
        numerator / denominator
    }
}

fn option_f64(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| format!("{value:.3}"))
}

fn option_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn ratio_text(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.3}")
    } else {
        "inf".to_owned()
    }
}

fn gate_reason(reasons: &[String]) -> String {
    if reasons.is_empty() {
        "-".to_owned()
    } else {
        reasons.join("; ").replace(['\t', '\n', '\r'], " ")
    }
}

fn join_numbers<T: std::fmt::Display>(values: &[T]) -> String {
    values
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn digest_hex(digest: [u8; 32]) -> String {
    digest
        .into_iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FixtureDiskBytes {
    manifest_database: u64,
    manifest_wal: u64,
    shard_databases: u64,
    shard_wals: u64,
}

impl FixtureDiskBytes {
    fn measure(root: &Path, shard_count: u16) -> Self {
        let manifest = root.join("manifest.sqlite");
        let manifest_database = required_file_bytes(&manifest);
        let manifest_wal = optional_file_bytes(&root.join("manifest.sqlite-wal"));
        let (shard_databases, shard_wals) =
            (0..shard_count).fold((0_u64, 0_u64), |(database_bytes, wal_bytes), shard| {
                let database = root.join("shards").join(format!("{shard:04}.sqlite"));
                (
                    database_bytes
                        .checked_add(required_file_bytes(&database))
                        .expect("benchmark shard database byte total fits u64"),
                    wal_bytes
                        .checked_add(optional_file_bytes(
                            &root.join("shards").join(format!("{shard:04}.sqlite-wal")),
                        ))
                        .expect("benchmark shard WAL byte total fits u64"),
                )
            });
        Self {
            manifest_database,
            manifest_wal,
            shard_databases,
            shard_wals,
        }
    }

    fn total(self) -> u64 {
        self.manifest_database
            .checked_add(self.manifest_wal)
            .and_then(|total| total.checked_add(self.shard_databases))
            .and_then(|total| total.checked_add(self.shard_wals))
            .expect("benchmark fixture byte total fits u64")
    }

    fn report(self, fixture: &BenchmarkFixture) {
        println!(
            "issue126_fixture_bytes\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            fixture.shard_count,
            ROWS_PER_SHARD,
            self.manifest_database,
            self.manifest_wal,
            self.shard_databases,
            self.shard_wals,
            self.total(),
        );
    }
}

fn required_file_bytes(path: &Path) -> u64 {
    regular_file_bytes(path).unwrap_or_else(|error| {
        panic!(
            "inspect required benchmark byte-accounting path {}: {error}",
            path.display()
        )
    })
}

fn optional_file_bytes(path: &Path) -> u64 {
    match regular_file_bytes(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => panic!(
            "inspect optional benchmark byte-accounting path {}: {error}",
            path.display()
        ),
    }
}

fn regular_file_bytes(path: &Path) -> std::io::Result<u64> {
    match fs::metadata(path) {
        Ok(metadata) => {
            assert!(
                metadata.is_file(),
                "benchmark byte-accounting path is not a regular file: {}",
                path.display()
            );
            Ok(metadata.len())
        }
        Err(error) => Err(error),
    }
}

fn find_hash_key_for_each_shard(database: &Database, shard_count: u16) -> Vec<i64> {
    let mut keys = vec![None; usize::from(shard_count)];
    for candidate in 1_i64..=1_000_000 {
        let shard = usize::from(database.shard_for_key(candidate.to_string().as_bytes()));
        if keys[shard].is_none() {
            keys[shard] = Some(candidate);
            if keys.iter().all(Option::is_some) {
                break;
            }
        }
    }
    keys.into_iter()
        .map(|key| key.expect("deterministic search found a hash key for every shard"))
        .collect()
}

fn seed_rows(storage: &Storage, hash_keys: &[i64], native_ids: &[i64]) {
    for shard in 0..storage.shard_count() {
        let mut connection = storage
            .open_shard(shard)
            .expect("open physical benchmark shard for seeding");
        let transaction = connection
            .transaction()
            .expect("start physical benchmark seed transaction");
        {
            let mut hash_insert = transaction
                .prepare(
                    "INSERT INTO benchmark_hash (tenant_id, row_no, payload) VALUES (?1, ?2, ?3)",
                )
                .expect("prepare benchmark hash insert");
            let mut native_insert = transaction
                .prepare("INSERT INTO benchmark_native (id, payload) VALUES (?1, ?2)")
                .expect("prepare benchmark native insert");
            for row_no in 0..ROWS_PER_SHARD {
                let payload = format!("hash-{shard:02}-{row_no:04}-briskdb-issue-126");
                hash_insert
                    .execute(params![
                        hash_keys[usize::from(shard)],
                        row_no as i64,
                        payload
                    ])
                    .expect("insert benchmark hash row");

                let owner = NativeRangeV1Id::decode(native_ids[usize::from(shard)])
                    .expect("decode benchmark owner")
                    .owner();
                let native_id = NativeRangeV1Id::new(owner, row_no as u64 + 1)
                    .expect("construct benchmark native row ID")
                    .encode();
                let payload = format!("native-{shard:02}-{row_no:04}-briskdb-issue-126");
                native_insert
                    .execute(params![native_id, payload])
                    .expect("insert benchmark native row");
            }
        }
        transaction
            .commit()
            .expect("commit physical benchmark seed transaction");
    }
}

fn result_set_hash_rows(result: &crate::core::ResultSet) -> Vec<HashRow> {
    result
        .rows()
        .iter()
        .map(|row| {
            (
                row.get(0)
                    .and_then(Value::as_i64)
                    .expect("Engine benchmark tenant_id is an integer"),
                row.get(1)
                    .and_then(Value::as_i64)
                    .expect("Engine benchmark row_no is an integer"),
                row.get(2)
                    .and_then(Value::as_str)
                    .expect("Engine benchmark payload is text")
                    .to_owned(),
            )
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    iterations: usize,
    elapsed: Duration,
}

impl Sample {
    fn operations_per_second(self) -> f64 {
        self.iterations as f64 / self.elapsed.as_secs_f64()
    }
}

#[derive(Debug, Clone, Copy)]
struct SampleSummary {
    median: Sample,
    sample_count: usize,
}

impl SampleSummary {
    fn from_samples(mut samples: Vec<Sample>) -> Self {
        assert_eq!(samples.len(), SAMPLE_COUNT);
        samples.sort_unstable_by(|left, right| {
            left.operations_per_second()
                .total_cmp(&right.operations_per_second())
        });
        Self {
            median: samples[samples.len() / 2],
            sample_count: samples.len(),
        }
    }

    fn operations_per_second(self) -> f64 {
        self.median.operations_per_second()
    }

    fn report(
        self,
        fixture: &BenchmarkFixture,
        path: &str,
        workload: &str,
        logical_rows_per_operation: usize,
    ) {
        let operations_per_second = self.operations_per_second();
        println!(
            "issue126_benchmark\t{}\t{}\t{path}\t{workload}\t{}\t{}\t{:.3}\t{operations_per_second:.2}\t{:.2}",
            fixture.shard_count,
            ROWS_PER_SHARD,
            self.sample_count,
            self.median.iterations,
            self.median.elapsed.as_secs_f64() * 1_000.0,
            operations_per_second * logical_rows_per_operation as f64,
        );
    }

    fn report_generated_writes(self, policy: GeneratedWritePolicy, writers: usize) {
        println!(
            "issue129_generated_write\t{}\t{}\t{}\t{}\t{}\t{}\t{:.3}\t{:.2}",
            policy.name(),
            GENERATED_WRITE_SHARDS,
            writers,
            GENERATED_WRITES_PER_WORKER,
            self.sample_count,
            self.median.iterations,
            self.median.elapsed.as_secs_f64() * 1_000.0,
            self.operations_per_second(),
        );
    }
}

fn measure_once(operation: &mut impl FnMut() -> usize, iterations: usize) -> Sample {
    let started = Instant::now();
    for _ in 0..iterations {
        black_box(operation());
    }
    Sample {
        iterations,
        elapsed: started.elapsed(),
    }
}

fn calibrated_iterations(sample: Sample) -> usize {
    if sample.elapsed.is_zero() {
        return sample
            .iterations
            .saturating_mul(10)
            .clamp(1, MAX_CALIBRATED_ITERATIONS);
    }
    let scaled = (sample.iterations as u128)
        .saturating_mul(TARGET_SAMPLE_DURATION.as_nanos())
        .div_ceil(sample.elapsed.as_nanos());
    usize::try_from(scaled)
        .unwrap_or(MAX_CALIBRATED_ITERATIONS)
        .clamp(1, MAX_CALIBRATED_ITERATIONS)
}

fn prepare_iterations(operation: &mut impl FnMut() -> usize, probe_iterations: usize) -> usize {
    for _ in 0..WARMUP_OPERATIONS {
        black_box(operation());
    }
    calibrated_iterations(measure_once(operation, probe_iterations.max(1)))
}

fn measure_at_least(operation: &mut impl FnMut() -> usize, iterations: &mut usize) -> Sample {
    loop {
        let sample = measure_once(operation, *iterations);
        if sample.elapsed >= MIN_SAMPLE_DURATION {
            return sample;
        }
        let calibrated = calibrated_iterations(sample);
        assert!(
            calibrated > *iterations,
            "benchmark could not reach its minimum sample duration within the iteration cap"
        );
        *iterations = calibrated;
    }
}

fn measure_series(mut operation: impl FnMut() -> usize, probe_iterations: usize) -> SampleSummary {
    let mut iterations = prepare_iterations(&mut operation, probe_iterations);
    let samples = (0..SAMPLE_COUNT)
        .map(|_| measure_at_least(&mut operation, &mut iterations))
        .collect();
    SampleSummary::from_samples(samples)
}

fn measure_pair(
    mut vtab: impl FnMut() -> usize,
    mut engine: impl FnMut() -> usize,
    probe_iterations: usize,
) -> (SampleSummary, SampleSummary) {
    let mut vtab_iterations = prepare_iterations(&mut vtab, probe_iterations);
    let mut engine_iterations = prepare_iterations(&mut engine, probe_iterations);
    let mut vtab_samples = Vec::with_capacity(SAMPLE_COUNT);
    let mut engine_samples = Vec::with_capacity(SAMPLE_COUNT);
    for sample in 0..SAMPLE_COUNT {
        if sample % 2 == 0 {
            vtab_samples.push(measure_at_least(&mut vtab, &mut vtab_iterations));
            engine_samples.push(measure_at_least(&mut engine, &mut engine_iterations));
        } else {
            engine_samples.push(measure_at_least(&mut engine, &mut engine_iterations));
            vtab_samples.push(measure_at_least(&mut vtab, &mut vtab_iterations));
        }
    }
    (
        SampleSummary::from_samples(vtab_samples),
        SampleSummary::from_samples(engine_samples),
    )
}

fn scan_iterations(logical_rows: usize) -> usize {
    TARGET_SCANNED_ROWS.div_ceil(logical_rows).max(1)
}

#[test]
fn fixture_disk_bytes_counts_database_and_wal_files_but_not_shm() {
    let temp = tempfile::tempdir().expect("create fixture-byte unit-test directory");
    fs::create_dir(temp.path().join("shards")).expect("create fixture-byte shard directory");
    fs::write(temp.path().join("manifest.sqlite"), [0_u8; 11])
        .expect("write fixture-byte manifest database");
    fs::write(temp.path().join("manifest.sqlite-wal"), [0_u8; 13])
        .expect("write fixture-byte manifest WAL");
    fs::write(temp.path().join("manifest.sqlite-shm"), [0_u8; 17])
        .expect("write excluded fixture-byte manifest SHM");
    fs::write(temp.path().join("shards/0000.sqlite"), [0_u8; 19])
        .expect("write fixture-byte first shard database");
    fs::write(temp.path().join("shards/0000.sqlite-wal"), [0_u8; 23])
        .expect("write fixture-byte first shard WAL");
    fs::write(temp.path().join("shards/0000.sqlite-shm"), [0_u8; 29])
        .expect("write excluded fixture-byte shard SHM");
    fs::write(temp.path().join("shards/0001.sqlite"), [0_u8; 31])
        .expect("write fixture-byte second shard database");

    let measured = FixtureDiskBytes::measure(temp.path(), 2);

    assert_eq!(
        measured,
        FixtureDiskBytes {
            manifest_database: 11,
            manifest_wal: 13,
            shard_databases: 50,
            shard_wals: 23,
        }
    );
    assert_eq!(measured.total(), 97);
}

fn synthetic_rollout_metrics(
    controls: RolloutControls,
    case: RolloutCase,
    trial: usize,
) -> RolloutTrialMetrics {
    let initial = RolloutTelemetryObservation {
        process_cpu: Duration::from_secs(1),
        resident_bytes: 10_000,
        process_lifetime_peak_resident_bytes: 12_000,
        wal_bytes: 1_000,
        pool_active_by_shard: vec![0; usize::from(controls.shards)],
        pool_queued_by_shard: vec![0; usize::from(controls.shards)],
    };
    let mut recorder =
        RolloutTrialRecorder::new(controls, case, trial, [0x5a; 32], initial).unwrap();
    let touched = if case.workload.is_scatter() {
        (0..controls.shards).collect::<Vec<_>>()
    } else {
        vec![u16::try_from(case.clients % usize::from(controls.shards)).unwrap()]
    };
    recorder.record_success(&touched).unwrap();
    for class in [
        RolloutErrorClass::Busy,
        RolloutErrorClass::Cancelled,
        RolloutErrorClass::Constraint,
        RolloutErrorClass::Corrupt,
        RolloutErrorClass::DiskFull,
        RolloutErrorClass::Other,
    ] {
        recorder.record_error(class);
    }
    recorder
        .observe(RolloutTelemetryObservation {
            process_cpu: Duration::from_millis(1_500),
            resident_bytes: 11_000,
            process_lifetime_peak_resident_bytes: 14_000,
            wal_bytes: 1_250,
            pool_active_by_shard: vec![1; usize::from(controls.shards)],
            pool_queued_by_shard: vec![2; usize::from(controls.shards)],
        })
        .unwrap();
    recorder.finish(controls.trial_duration).unwrap()
}

fn synthetic_rollout_records(controls: RolloutControls) -> Vec<RolloutCaseRecord> {
    rollout_cases()
        .into_iter()
        .flat_map(|case| {
            if let Some(reason) = rollout_unavailable_reason(case) {
                vec![RolloutCaseRecord::Unavailable(RolloutUnavailable {
                    case,
                    reason: reason.to_owned(),
                })]
            } else {
                (1..=controls.trial_count)
                    .map(|trial| {
                        RolloutCaseRecord::Trial(Box::new(synthetic_rollout_metrics(
                            controls, case, trial,
                        )))
                    })
                    .collect()
            }
        })
        .collect()
}

#[test]
fn rollout_benchmark_matrix_and_report_account_for_every_frozen_case_and_metric() {
    let controls = RolloutControls::frozen();
    assert_eq!(ROLLOUT_CLIENT_MATRIX, [2, 4, 8, 10]);
    assert_eq!(controls.shards, 10);
    assert_eq!(controls.journal_mode, "WAL");
    assert_eq!(controls.synchronous, "FULL");
    assert_eq!(controls.cache_policy, "warm_os_cache");

    let cases = rollout_cases();
    assert_eq!(cases.len(), 40);
    assert_eq!(cases.iter().copied().collect::<BTreeSet<_>>().len(), 40);
    for clients in ROLLOUT_CLIENT_MATRIX {
        for path in RolloutPath::ALL {
            for workload in RolloutWorkload::ALL {
                assert!(cases.contains(&RolloutCase {
                    clients,
                    path,
                    workload,
                }));
                assert!(!workload.sql().is_empty());
            }
        }
    }

    let records = synthetic_rollout_records(controls);
    let unavailable = records
        .iter()
        .filter(|record| matches!(record, RolloutCaseRecord::Unavailable(_)))
        .count();
    assert_eq!(unavailable, 8);
    assert_eq!(records.len(), 104);
    let report = RolloutReport::try_new(controls, records).unwrap();
    let rendered = report.render_tsv();
    let lines = rendered.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 105);
    let columns = lines[0].split('\t').count();
    assert!(lines.iter().all(|line| line.split('\t').count() == columns));
    for field in [
        "process_cpu_ms",
        "baseline_resident_bytes",
        "resident_bytes",
        "sampled_peak_resident_bytes",
        "sampled_peak_resident_growth_bytes",
        "process_lifetime_peak_resident_bytes",
        "wal_growth_bytes",
        "sampled_peak_pool_active_by_shard",
        "sampled_peak_pool_queued_by_shard",
        "shard_max_over_mean",
        "busy",
        "cancelled",
        "constraint",
        "corrupt",
        "disk_full",
        "other",
    ] {
        assert!(lines[0].split('\t').any(|column| column == field));
    }
    assert_eq!(rendered.matches("\tunsupported\t").count(), 8);
    assert!(rendered.contains("no independent pre-vtab comparator"));

    let gate = report.render_gate_tsv();
    let gate_lines = gate.lines().collect::<Vec<_>>();
    assert_eq!(
        gate_lines.len(),
        54,
        "header + 40 cases + 12 comparisons + overall"
    );
    assert!(gate.contains("issue131_benchmark_gate_overall\tHOLD\t"));
    assert!(gate.contains("classified operation errors"));
    assert!(gate.contains("requires at least 10000 successful writes per client"));
    assert!(gate.contains("RSS ratio is UNRESOLVED"));
    assert!(gate.contains("WAL ratio is UNRESOLVED"));
    assert!(gate.contains("sampled WAL file-size growth is diagnostic"));
    assert!(gate.contains("pool-capacity gate remains unresolved"));
}

#[test]
fn rollout_explicit_reconciliation_compares_tenant_row_and_payload_not_only_shard_and_row() {
    let controls = RolloutControls::frozen();
    let fixture = RolloutBenchmarkFixture::new(controls);
    let actual_tenant = fixture.hash_keys[0];
    let shard = fixture.storage.shard_for_key(
        canonical_shard_key_bytes(crate::core::CanonicalShardKeyRef::Int64(actual_tenant)).as_ref(),
    );
    let wrong_tenant = (1_i64..1_000_000)
        .map(|offset| actual_tenant.wrapping_add(offset))
        .find(|&candidate| {
            fixture.storage.shard_for_key(
                canonical_shard_key_bytes(crate::core::CanonicalShardKeyRef::Int64(candidate))
                    .as_ref(),
            ) == shard
        })
        .expect("find a distinct tenant routed to the same shard");
    let row_id = 9_000_000_000_i64;
    {
        let connection = fixture.storage.open_shard(shard).unwrap();
        connection
            .execute(
                EXPLICIT_INSERT_SQL,
                params![actual_tenant, row_id, "issue-131-explicit"],
            )
            .unwrap();
    }
    let case = RolloutCase {
        clients: 2,
        path: RolloutPath::VirtualTable,
        workload: RolloutWorkload::ExplicitWrite,
    };
    let outcome_for = |row| {
        let mut outcome = RolloutRunOutcome::with_shards(controls.shards, case.clients);
        outcome.successful_operations = 1;
        outcome.acknowledged_rows.push(row);
        outcome
    };
    let no_warmup = BTreeSet::new();

    let wrong_tenant_error = fixture
        .validate_run_outcome(
            case,
            &outcome_for(RolloutAcknowledgedRow::explicit(
                shard,
                wrong_tenant,
                row_id,
                "issue-131-explicit",
            )),
            &no_warmup,
        )
        .unwrap_err();
    assert!(wrong_tenant_error.contains("missing=1, unexpected=1"));

    let wrong_payload_error = fixture
        .validate_run_outcome(
            case,
            &outcome_for(RolloutAcknowledgedRow::explicit(
                shard,
                actual_tenant,
                row_id,
                "wrong-payload",
            )),
            &no_warmup,
        )
        .unwrap_err();
    assert!(wrong_payload_error.contains("missing=1, unexpected=1"));

    fixture
        .validate_run_outcome(
            case,
            &outcome_for(RolloutAcknowledgedRow::explicit(
                shard,
                actual_tenant,
                row_id,
                "issue-131-explicit",
            )),
            &no_warmup,
        )
        .unwrap();
}

#[test]
fn rollout_report_rejects_missing_duplicate_and_fabricated_comparators() {
    let controls = RolloutControls::frozen();
    let mut missing = synthetic_rollout_records(controls);
    missing.pop();
    assert!(
        RolloutReport::try_new(controls, missing)
            .unwrap_err()
            .contains("must be unavailable")
    );

    let mut duplicate = synthetic_rollout_records(controls);
    duplicate.push(duplicate[0].clone());
    assert!(
        RolloutReport::try_new(controls, duplicate)
            .unwrap_err()
            .contains("requires trials")
    );

    let generated_router = RolloutCase {
        clients: 2,
        path: RolloutPath::ExistingRouter,
        workload: RolloutWorkload::NativeRangeWrite,
    };
    let mut fabricated = synthetic_rollout_records(controls);
    fabricated.retain(|record| {
        !matches!(
            record,
            RolloutCaseRecord::Unavailable(value) if value.case == generated_router
        )
    });
    fabricated.extend((1..=controls.trial_count).map(|trial| {
        RolloutCaseRecord::Trial(Box::new(synthetic_rollout_metrics(
            controls,
            generated_router,
            trial,
        )))
    }));
    assert!(
        RolloutReport::try_new(controls, fabricated)
            .unwrap_err()
            .contains("must be unavailable")
    );

    let mut mismatched_files = synthetic_rollout_records(controls);
    let mismatched = mismatched_files
        .iter_mut()
        .find_map(|record| match record {
            RolloutCaseRecord::Trial(metrics)
                if metrics.case
                    == (RolloutCase {
                        clients: 2,
                        path: RolloutPath::ExistingRouter,
                        workload: RolloutWorkload::PointRead,
                    })
                    && metrics.trial == 1 =>
            {
                Some(metrics)
            }
            _ => None,
        })
        .unwrap();
    mismatched.baseline_digest[0] ^= 0xff;
    assert!(
        RolloutReport::try_new(controls, mismatched_files)
            .unwrap_err()
            .contains("byte-identical baseline files")
    );
}

#[cfg(unix)]
#[test]
fn rollout_benchmark_smoke_executes_both_independent_read_paths_with_real_telemetry() {
    let controls = RolloutControls {
        warmup_operations_per_client: 1,
        trial_count: 1,
        trial_duration: Duration::from_millis(20),
        telemetry_interval: Duration::from_millis(5),
        ..RolloutControls::frozen()
    };
    let fixture = RolloutBenchmarkFixture::new(controls);
    for path in RolloutPath::ALL {
        for workload in [RolloutWorkload::PointRead, RolloutWorkload::ScatterRead] {
            let case = RolloutCase {
                clients: 2,
                path,
                workload,
            };
            let metrics = fixture
                .run_case(controls, case)
                .unwrap()
                .into_metrics(controls, case, 1)
                .unwrap();
            assert!(metrics.successful_operations > 0);
            assert_eq!(metrics.errors.total(), 0);
            assert!(metrics.process_cpu > Duration::ZERO);
            assert!(metrics.resident_bytes > 0);
            assert!(metrics.sampled_peak_resident_bytes >= metrics.resident_bytes);
            assert!(
                metrics.process_lifetime_peak_resident_bytes >= metrics.sampled_peak_resident_bytes
            );
            assert!(metrics.telemetry_samples >= 2);
            assert_eq!(metrics.max_pool_active_by_shard.len(), 10);
            assert_eq!(metrics.max_pool_queued_by_shard.len(), 10);
            let expected_touches = if workload.is_scatter() {
                metrics
                    .successful_operations
                    .checked_mul(u64::from(controls.shards))
                    .unwrap()
            } else {
                metrics.successful_operations
            };
            assert_eq!(
                metrics.successful_shard_touches.iter().sum::<u64>(),
                expected_touches
            );
        }
    }

    for (workload, paths) in [
        (RolloutWorkload::ExplicitWrite, RolloutPath::ALL.as_slice()),
        (
            RolloutWorkload::NativeRangeWrite,
            [RolloutPath::VirtualTable].as_slice(),
        ),
        (
            RolloutWorkload::HiloWrite,
            [RolloutPath::VirtualTable].as_slice(),
        ),
    ] {
        for &path in paths {
            let case = RolloutCase {
                clients: 2,
                path,
                workload,
            };
            let fixture = RolloutBenchmarkFixture::new(controls);
            let metrics = fixture
                .run_case(controls, case)
                .unwrap()
                .into_metrics(controls, case, 1)
                .unwrap();
            assert!(metrics.successful_operations > 0);
            assert_eq!(metrics.errors.total(), 0);
            assert_eq!(
                metrics.successful_shard_touches.iter().sum::<u64>(),
                metrics.successful_operations
            );
        }
    }
}

#[test]
#[ignore = "manual release-mode issue #131 rollout benchmark"]
fn release_benchmark_matrix_reports_issue_131_rollout_gate() {
    if cfg!(debug_assertions) {
        panic!("run this ignored benchmark with cargo test --release");
    }
    let controls = RolloutControls::frozen();
    let mut records = Vec::new();
    for clients in ROLLOUT_CLIENT_MATRIX {
        for workload in RolloutWorkload::ALL {
            for trial in 1..=controls.trial_count {
                let template = RolloutFixtureTemplate::new(controls);
                let paths = if trial % 2 == 1 {
                    RolloutPath::ALL
                } else {
                    [RolloutPath::ExistingRouter, RolloutPath::VirtualTable]
                };
                for path in paths {
                    let case = RolloutCase {
                        clients,
                        path,
                        workload,
                    };
                    if let Some(reason) = rollout_unavailable_reason(case) {
                        if trial == 1 {
                            records.push(RolloutCaseRecord::Unavailable(RolloutUnavailable {
                                case,
                                reason: reason.to_owned(),
                            }));
                        }
                        continue;
                    }
                    let fixture = RolloutBenchmarkFixture::from_template(controls, &template);
                    let metrics = fixture
                        .run_case(controls, case)
                        .unwrap_or_else(|error| panic!("run {case:?}, trial {trial}: {error}"))
                        .into_metrics(controls, case, trial)
                        .unwrap_or_else(|error| panic!("account {case:?}, trial {trial}: {error}"));
                    records.push(RolloutCaseRecord::Trial(Box::new(metrics)));
                }
            }
        }
    }
    let report = RolloutReport::try_new(controls, records).expect("rollout matrix is complete");
    print!("{}", report.render_tsv());
    print!("{}", report.render_gate_tsv());
}

#[test]
fn generated_write_benchmark_smoke_covers_the_frozen_writer_matrix_and_both_policies() {
    assert_eq!(GENERATED_WRITER_MATRIX, [2, 4, 8, 10]);
    assert_eq!(GENERATED_WRITE_SHARDS, 4);

    let fixture = GeneratedWriteBenchmarkFixture::new();
    for policy in [
        GeneratedWritePolicy::NativeRangeV1,
        GeneratedWritePolicy::HiloV1,
    ] {
        let sample = fixture.measure_concurrent_writes(policy, 2, 2);
        assert_eq!(sample.iterations, 4);
        assert_eq!(fixture.physical_row_count(policy), 4);
    }
}

#[test]
#[ignore = "manual release-mode issue #129 generated-ID benchmark"]
fn release_benchmark_matrix_reports_issue_129_generated_write_comparison() {
    if cfg!(debug_assertions) {
        panic!("run this ignored benchmark with cargo test --release");
    }
    println!(
        "record\tpolicy\tshards\twriters\twrites_per_worker\tsamples\tmedian_total_writes\tmedian_elapsed_ms\tmedian_writes_per_sec"
    );
    println!("comparison_record\tshards\twriters\thilo_over_native");

    for writers in GENERATED_WRITER_MATRIX {
        let fixture = GeneratedWriteBenchmarkFixture::new();
        let mut native_samples = Vec::with_capacity(SAMPLE_COUNT);
        let mut hilo_samples = Vec::with_capacity(SAMPLE_COUNT);
        for sample in 0..SAMPLE_COUNT {
            if sample % 2 == 0 {
                native_samples.push(fixture.measure_concurrent_writes(
                    GeneratedWritePolicy::NativeRangeV1,
                    writers,
                    GENERATED_WRITES_PER_WORKER,
                ));
                hilo_samples.push(fixture.measure_concurrent_writes(
                    GeneratedWritePolicy::HiloV1,
                    writers,
                    GENERATED_WRITES_PER_WORKER,
                ));
            } else {
                hilo_samples.push(fixture.measure_concurrent_writes(
                    GeneratedWritePolicy::HiloV1,
                    writers,
                    GENERATED_WRITES_PER_WORKER,
                ));
                native_samples.push(fixture.measure_concurrent_writes(
                    GeneratedWritePolicy::NativeRangeV1,
                    writers,
                    GENERATED_WRITES_PER_WORKER,
                ));
            }
        }
        let expected_rows = writers
            .checked_mul(GENERATED_WRITES_PER_WORKER)
            .and_then(|rows| rows.checked_mul(SAMPLE_COUNT))
            .expect("benchmark row count fits usize");
        assert_eq!(
            fixture.physical_row_count(GeneratedWritePolicy::NativeRangeV1),
            expected_rows
        );
        assert_eq!(
            fixture.physical_row_count(GeneratedWritePolicy::HiloV1),
            expected_rows
        );

        let native = SampleSummary::from_samples(native_samples);
        let hilo = SampleSummary::from_samples(hilo_samples);
        native.report_generated_writes(GeneratedWritePolicy::NativeRangeV1, writers);
        hilo.report_generated_writes(GeneratedWritePolicy::HiloV1, writers);
        println!(
            "issue129_generated_comparison\t{}\t{}\t{:.3}",
            GENERATED_WRITE_SHARDS,
            writers,
            hilo.operations_per_second() / native.operations_per_second(),
        );
    }
}

#[test]
#[ignore = "manual release-mode issue #126 benchmark"]
fn release_benchmark_matrix_reports_issue_126_comparison() {
    if cfg!(debug_assertions) {
        panic!("run this ignored benchmark with cargo test --release");
    }
    println!(
        "record\tshards\trows_per_shard\tpath\tworkload\tsamples\tmedian_iterations\tmedian_elapsed_ms\tmedian_ops_per_sec\tmedian_logical_rows_per_sec"
    );
    println!(
        "fixture_record\tshards\trows_per_shard\tmanifest_db_bytes\tmanifest_wal_bytes\tshard_db_bytes\tshard_wal_bytes\ttotal_db_and_wal_bytes"
    );

    for shard_count in SHARD_MATRIX {
        let fixture = BenchmarkFixture::new(shard_count);
        fixture.correctness_preflight();
        fixture.on_disk_bytes().report(&fixture);
        let logical_rows = fixture.logical_row_count();
        let scan_iterations = scan_iterations(logical_rows);

        let (vtab_point, engine_point) = measure_pair(
            || {
                let row = fixture.vtab_hash_point();
                black_box(row);
                1
            },
            || {
                let (shards, rows) = fixture.engine_hash_point();
                black_box((shards, rows));
                1
            },
            POINT_PROBE_ITERATIONS,
        );
        vtab_point.report(&fixture, "vtab", "hash_point", 1);
        engine_point.report(&fixture, "engine_logical", "hash_point", 1);

        let (vtab_full, engine_full) = measure_pair(
            || {
                let rows = fixture.vtab_hash_full();
                let len = rows.len();
                black_box(rows);
                len
            },
            || {
                let (shards, rows) = fixture.engine_hash_full();
                let len = rows.len();
                black_box((shards, rows));
                len
            },
            scan_iterations,
        );
        vtab_full.report(&fixture, "vtab", "hash_full", logical_rows);
        engine_full.report(&fixture, "engine_logical", "hash_full", logical_rows);

        measure_series(
            || usize::try_from(fixture.vtab_hash_count()).expect("benchmark count is non-negative"),
            scan_iterations,
        )
        .report(&fixture, "vtab", "count", logical_rows);

        measure_series(
            || {
                let rows = fixture.vtab_hash_order_limit();
                let len = rows.len();
                black_box(rows);
                len
            },
            scan_iterations,
        )
        .report(&fixture, "vtab", "order_limit_50", logical_rows);

        measure_series(
            || {
                let row = fixture.vtab_native_point();
                black_box(row);
                1
            },
            POINT_PROBE_ITERATIONS,
        )
        .report(&fixture, "vtab", "native_point", 1);

        println!(
            "issue126_comparison\t{shard_count}\t{}\thash_point\t{:.3}\thash_full\t{:.3}",
            ROWS_PER_SHARD,
            vtab_point.operations_per_second() / engine_point.operations_per_second(),
            vtab_full.operations_per_second() / engine_full.operations_per_second(),
        );
    }
}
