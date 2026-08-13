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
    fs,
    hint::black_box,
    path::Path,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use rusqlite::params;
use tokio::runtime::{Builder, Runtime};

use super::{MAX_CURSOR_BYTES, MAX_CURSOR_ROWS, ReadCoordinator, Storage, WriteCoordinator};
use crate::core::{
    Database, Engine, EngineOptions, GeneratedIdPolicy, ResultLimits, Session, ShardKeyMetadata,
    ShardKeyType, Statement, TableDeclaration, Value, generated_id::NativeRangeV1Id,
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
