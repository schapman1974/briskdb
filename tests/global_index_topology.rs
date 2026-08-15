//! Isolated storage-topology prototypes and decision benchmark for issue #229.
//!
//! Neither prototype is linked into the BriskDB data path. The selected
//! routing contract lives in `GlobalIndexStorageTopology`; issue #230 consumes
//! it when the production index builder lands.

use std::{
    collections::BTreeSet,
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
    CanonicalIndexKey, GlobalIndexId, GlobalIndexStorageTopology,
    HASH_PARTITIONED_GLOBAL_INDEX_PARTITIONS_V1, Value,
};
use rusqlite::{Connection, ErrorCode, OpenFlags, params};

const FORMAT_VERSION: &str = "global-index-topology-v1";
const APPLICATION_ID: i32 = 0x4252_4947;
const STORAGE_VERSION: u32 = 1;
const INDEX_ID_VALUE: u64 = 1;
const CHILD_TIMEOUT: Duration = Duration::from_secs(20);
const BUSY_RETRY_TIMEOUT: Duration = Duration::from_secs(10);
const TELEMETRY_INTERVAL: Duration = Duration::from_millis(1);

const STORAGE_SCHEMA: &str = "
    CREATE TABLE briskdb_index_storage (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        topology_kind INTEGER NOT NULL CHECK (topology_kind IN (1, 2)),
        topology_version INTEGER NOT NULL CHECK (topology_version = 1),
        partition_id INTEGER NOT NULL CHECK (partition_id BETWEEN 0 AND 255),
        partition_count INTEGER NOT NULL CHECK (partition_count BETWEEN 1 AND 256),
        key_encoding_version INTEGER NOT NULL CHECK (key_encoding_version = 1),
        CHECK (partition_id < partition_count)
    ) STRICT;

    CREATE TABLE briskdb_index_entries (
        index_id INTEGER NOT NULL CHECK (index_id > 0),
        encoded_key BLOB NOT NULL CHECK (length(encoded_key) BETWEEN 9 AND 1048576),
        source_shard INTEGER NOT NULL CHECK (source_shard BETWEEN 0 AND 63),
        row_identity BLOB NOT NULL CHECK (length(row_identity) BETWEEN 1 AND 1024),
        payload INTEGER NOT NULL,
        PRIMARY KEY (index_id, encoded_key, source_shard, row_identity)
    ) STRICT, WITHOUT ROWID;

    CREATE INDEX briskdb_index_entries_lookup
        ON briskdb_index_entries (index_id, encoded_key);
";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum PrototypeTopology {
    Shared,
    Partitioned,
}

impl PrototypeTopology {
    const ALL: [Self; 2] = [Self::Shared, Self::Partitioned];

    const fn name(self) -> &'static str {
        match self {
            Self::Shared => "shared_sqlite_v1",
            Self::Partitioned => "hash_partitioned_sqlite_v1_16",
        }
    }

    const fn core(self) -> GlobalIndexStorageTopology {
        match self {
            Self::Shared => GlobalIndexStorageTopology::SharedSqliteV1,
            Self::Partitioned => GlobalIndexStorageTopology::HashPartitionedSqliteV1 {
                partitions: HASH_PARTITIONED_GLOBAL_INDEX_PARTITIONS_V1,
            },
        }
    }

    const fn kind(self) -> i64 {
        match self {
            Self::Shared => 1,
            Self::Partitioned => 2,
        }
    }

    const fn partition_count(self) -> u16 {
        self.core().partition_count()
    }
}

impl FromStr for PrototypeTopology {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|topology| topology.name() == value)
            .ok_or_else(|| format!("unknown topology {value:?}"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Entry {
    key: Vec<u8>,
    source_shard: u16,
    row_identity: Vec<u8>,
    payload: i64,
}

struct PrototypeStore {
    topology: PrototypeTopology,
    connections: Vec<Connection>,
}

impl PrototypeStore {
    fn create(root: &Path, topology: PrototypeTopology) -> Self {
        fs::create_dir_all(root).expect("create prototype storage directory");
        for partition in 0..topology.partition_count() {
            let path = partition_path(root, topology, partition);
            assert!(!path.exists(), "prototype storage must start empty");
            let connection = open_connection(&path, true);
            connection
                .execute_batch(STORAGE_SCHEMA)
                .expect("create prototype schema");
            connection
                .execute(
                    "INSERT INTO briskdb_index_storage (
                         singleton, topology_kind, topology_version,
                         partition_id, partition_count, key_encoding_version
                     ) VALUES (1, ?1, 1, ?2, ?3, 1)",
                    params![
                        topology.kind(),
                        i64::from(partition),
                        i64::from(topology.partition_count()),
                    ],
                )
                .expect("stamp prototype metadata");
        }
        Self::open(root, topology)
    }

    fn open(root: &Path, topology: PrototypeTopology) -> Self {
        let connections = (0..topology.partition_count())
            .map(|partition| {
                let path = partition_path(root, topology, partition);
                let connection = open_connection(&path, false);
                validate_connection(&connection, topology, partition);
                connection
            })
            .collect();
        Self {
            topology,
            connections,
        }
    }

    fn connection_for(&self, key: &CanonicalIndexKey) -> &Connection {
        let partition = self
            .topology
            .core()
            .partition_for_key(index_id(), key)
            .expect("prototype topology is assigned");
        &self.connections[usize::from(partition)]
    }

    fn insert(
        &self,
        key: &CanonicalIndexKey,
        source_shard: u16,
        row_identity: &[u8],
        payload: i64,
    ) -> rusqlite::Result<usize> {
        self.connection_for(key).execute(
            "INSERT INTO briskdb_index_entries (
                 index_id, encoded_key, source_shard, row_identity, payload
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                INDEX_ID_VALUE as i64,
                key.as_bytes(),
                i64::from(source_shard),
                row_identity,
                payload,
            ],
        )
    }

    fn upsert(
        &self,
        key: &CanonicalIndexKey,
        source_shard: u16,
        row_identity: &[u8],
        payload: i64,
    ) -> rusqlite::Result<usize> {
        self.connection_for(key).execute(
            "INSERT INTO briskdb_index_entries (
                 index_id, encoded_key, source_shard, row_identity, payload
             ) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (index_id, encoded_key, source_shard, row_identity)
             DO UPDATE SET payload = excluded.payload",
            params![
                INDEX_ID_VALUE as i64,
                key.as_bytes(),
                i64::from(source_shard),
                row_identity,
                payload,
            ],
        )
    }

    fn delete(
        &self,
        key: &CanonicalIndexKey,
        source_shard: u16,
        row_identity: &[u8],
    ) -> rusqlite::Result<usize> {
        self.connection_for(key).execute(
            "DELETE FROM briskdb_index_entries
             WHERE index_id = ?1 AND encoded_key = ?2
               AND source_shard = ?3 AND row_identity = ?4",
            params![
                INDEX_ID_VALUE as i64,
                key.as_bytes(),
                i64::from(source_shard),
                row_identity,
            ],
        )
    }

    fn lookup(&self, key: &CanonicalIndexKey) -> rusqlite::Result<Vec<Entry>> {
        let mut statement = self.connection_for(key).prepare(
            "SELECT encoded_key, source_shard, row_identity, payload
             FROM briskdb_index_entries
             WHERE index_id = ?1 AND encoded_key = ?2
             ORDER BY source_shard, row_identity",
        )?;
        statement
            .query_map(params![INDEX_ID_VALUE as i64, key.as_bytes()], decode_entry)?
            .collect()
    }

    fn all_entries(&self) -> rusqlite::Result<BTreeSet<Entry>> {
        let mut entries = BTreeSet::new();
        for connection in &self.connections {
            let mut statement = connection.prepare(
                "SELECT encoded_key, source_shard, row_identity, payload
                 FROM briskdb_index_entries
                 WHERE index_id = ?1
                 ORDER BY encoded_key, source_shard, row_identity",
            )?;
            entries.extend(
                statement
                    .query_map([INDEX_ID_VALUE as i64], decode_entry)?
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Ok(entries)
    }

    fn quick_check(&self) {
        for connection in &self.connections {
            let result: String = connection
                .query_row("PRAGMA quick_check", [], |row| row.get(0))
                .expect("run prototype quick_check");
            assert_eq!(result, "ok");
        }
    }

    fn checkpoint(&self) {
        for connection in &self.connections {
            connection
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .expect("checkpoint prototype WAL");
        }
    }
}

fn decode_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<Entry> {
    Ok(Entry {
        key: row.get(0)?,
        source_shard: row.get(1)?,
        row_identity: row.get(2)?,
        payload: row.get(3)?,
    })
}

fn index_id() -> GlobalIndexId {
    GlobalIndexId::new(INDEX_ID_VALUE).expect("fixed prototype index ID is valid")
}

fn key(value: impl Into<Value>) -> CanonicalIndexKey {
    CanonicalIndexKey::encode_values(&[value.into()]).expect("encode prototype key")
}

fn partition_path(root: &Path, topology: PrototypeTopology, partition: u16) -> PathBuf {
    match topology {
        PrototypeTopology::Shared => root.join("global.sqlite"),
        PrototypeTopology::Partitioned => root.join(format!("partition-{partition:04}.sqlite")),
    }
}

fn open_connection(path: &Path, initialize: bool) -> Connection {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | if initialize {
            OpenFlags::SQLITE_OPEN_CREATE
        } else {
            OpenFlags::empty()
        };
    let connection = Connection::open_with_flags(path, flags).expect("open prototype SQLite file");
    connection
        .busy_timeout(Duration::from_secs(5))
        .expect("configure prototype busy timeout");
    connection
        .pragma_update(None, "cell_size_check", "ON")
        .expect("enable prototype cell checks");
    connection
        .pragma_update(None, "synchronous", "FULL")
        .expect("configure prototype durability");
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .expect("enable prototype foreign keys");
    connection
        .pragma_update(None, "wal_autocheckpoint", 0_i64)
        .expect("disable prototype automatic checkpoints");
    if initialize {
        connection
            .pragma_update(None, "application_id", APPLICATION_ID)
            .expect("stamp prototype application ID");
        connection
            .pragma_update(None, "user_version", STORAGE_VERSION)
            .expect("stamp prototype storage version");
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .expect("enable prototype WAL");
    }
    let mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .expect("read prototype journal mode");
    assert_eq!(mode.to_ascii_lowercase(), "wal");
    connection
}

fn validate_connection(connection: &Connection, topology: PrototypeTopology, partition: u16) {
    let application_id: i32 = connection
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .expect("read prototype application ID");
    let user_version: u32 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("read prototype storage version");
    assert_eq!(application_id, APPLICATION_ID);
    assert_eq!(user_version, STORAGE_VERSION);
    let metadata = connection
        .query_row(
            "SELECT topology_kind, topology_version, partition_id,
                    partition_count, key_encoding_version
             FROM briskdb_index_storage WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .expect("read prototype metadata");
    assert_eq!(
        metadata,
        (
            topology.kind(),
            1,
            i64::from(partition),
            i64::from(topology.partition_count()),
            1,
        )
    );
}

fn is_busy(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(failure.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    )
}

fn retry_mutation(mut operation: impl FnMut() -> rusqlite::Result<usize>) -> u64 {
    let deadline = Instant::now() + BUSY_RETRY_TIMEOUT;
    let mut busy = 0;
    loop {
        match operation() {
            Ok(changed) => {
                assert_eq!(changed, 1);
                return busy;
            }
            Err(error) if is_busy(&error) && Instant::now() < deadline => {
                busy += 1;
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("prototype mutation failed after {busy} busy retries: {error}"),
        }
    }
}

#[test]
fn both_topologies_match_the_same_reference_model_after_reopen() {
    for topology in PrototypeTopology::ALL {
        let root = tempfile::tempdir().expect("create topology fixture");
        let store = PrototypeStore::create(root.path(), topology);
        let mut expected = BTreeSet::new();

        for ordinal in 0_u16..192 {
            let encoded = key(format!("model-{ordinal:04}"));
            let source_shard = ordinal % 64;
            let row_identity = ordinal.to_le_bytes().to_vec();
            let payload = i64::from(ordinal) * 7;
            store
                .insert(&encoded, source_shard, &row_identity, payload)
                .expect("insert modeled entry");
            expected.insert(Entry {
                key: encoded.as_bytes().to_vec(),
                source_shard,
                row_identity,
                payload,
            });
        }
        for ordinal in (0_u16..192).step_by(3) {
            let encoded = key(format!("model-{ordinal:04}"));
            let row_identity = ordinal.to_le_bytes();
            assert_eq!(
                store
                    .delete(&encoded, ordinal % 64, &row_identity)
                    .expect("delete modeled entry"),
                1
            );
            expected.retain(|entry| entry.key != encoded.as_bytes());
        }
        drop(store);

        let reopened = PrototypeStore::open(root.path(), topology);
        reopened.quick_check();
        assert_eq!(reopened.all_entries().unwrap(), expected);
        for entry in expected.iter().take(16) {
            let encoded = CanonicalIndexKey::from_bytes(&entry.key).unwrap();
            assert_eq!(
                reopened.lookup(&encoded).unwrap().as_slice(),
                std::slice::from_ref(entry)
            );
        }
    }
}

#[test]
fn selected_partition_router_uses_all_files_without_cross_file_duplicates() {
    let topology = PrototypeTopology::Partitioned;
    let root = tempfile::tempdir().expect("create routing fixture");
    let store = PrototypeStore::create(root.path(), topology);
    let mut counts = vec![0_usize; usize::from(topology.partition_count())];
    for ordinal in 0_u16..4_096 {
        let encoded = key(format!("routing-{ordinal}"));
        let partition = topology
            .core()
            .partition_for_key(index_id(), &encoded)
            .unwrap();
        counts[usize::from(partition)] += 1;
        store
            .insert(
                &encoded,
                ordinal % 64,
                &ordinal.to_le_bytes(),
                i64::from(ordinal),
            )
            .unwrap();
    }
    assert!(counts.iter().all(|count| *count > 200));
    assert!(counts.iter().all(|count| *count < 320));
    assert_eq!(store.all_entries().unwrap().len(), 4_096);
}

#[allow(clippy::too_many_arguments)]
fn spawn_child(
    root: &Path,
    topology: PrototypeTopology,
    mode: &str,
    worker: usize,
    ready: &Path,
    start: &Path,
    result: &Path,
    operations: usize,
) -> std::process::Child {
    Command::new(env::current_exe().expect("resolve topology test executable"))
        .args(["--exact", "global_index_topology_child", "--nocapture"])
        .env("BRISKDB_TOPOLOGY_CHILD", "1")
        .env("BRISKDB_TOPOLOGY_ROOT", root)
        .env("BRISKDB_TOPOLOGY_KIND", topology.name())
        .env("BRISKDB_TOPOLOGY_MODE", mode)
        .env("BRISKDB_TOPOLOGY_WORKER", worker.to_string())
        .env("BRISKDB_TOPOLOGY_READY", ready)
        .env("BRISKDB_TOPOLOGY_START", start)
        .env("BRISKDB_TOPOLOGY_RESULT", result)
        .env("BRISKDB_TOPOLOGY_OPERATIONS", operations.to_string())
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn topology child")
}

fn wait_for_paths(paths: &[PathBuf]) {
    let deadline = Instant::now() + CHILD_TIMEOUT;
    while paths.iter().any(|path| !path.exists()) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(2));
    }
    assert!(
        paths.iter().all(|path| path.exists()),
        "timed out waiting for topology child barriers"
    );
}

#[test]
fn both_topologies_recover_only_committed_rows_after_process_abort() {
    for topology in PrototypeTopology::ALL {
        let root = tempfile::tempdir().expect("create crash fixture");
        let store = PrototypeStore::create(root.path(), topology);
        store
            .insert(&key("parent"), 0, b"parent", 1)
            .expect("insert parent sentinel");
        drop(store);

        let ready = root.path().join("crash-ready");
        let start = root.path().join("crash-start");
        let result = root.path().join("unused-result");
        fs::write(&start, b"start").unwrap();
        let mut child = spawn_child(
            root.path(),
            topology,
            "crash",
            0,
            &ready,
            &start,
            &result,
            32,
        );
        let status = child.wait().expect("wait for crashing topology child");
        assert_eq!(status.code(), Some(91));

        let reopened = PrototypeStore::open(root.path(), topology);
        reopened.quick_check();
        let entries = reopened.all_entries().unwrap();
        assert_eq!(entries.len(), 33);
        assert!(reopened.lookup(&key("uncommitted")).unwrap().is_empty());
    }
}

#[test]
fn both_topologies_accept_disjoint_concurrent_process_mutations() {
    for topology in PrototypeTopology::ALL {
        let root = tempfile::tempdir().expect("create process fixture");
        drop(PrototypeStore::create(root.path(), topology));
        let start = root.path().join("writers-start");
        let mut children = Vec::new();
        let mut ready_paths = Vec::new();
        for worker in 0..4 {
            let ready = root.path().join(format!("writer-{worker}-ready"));
            let result = root.path().join(format!("writer-{worker}-result"));
            children.push(spawn_child(
                root.path(),
                topology,
                "writer",
                worker,
                &ready,
                &start,
                &result,
                64,
            ));
            ready_paths.push(ready);
        }
        wait_for_paths(&ready_paths);
        fs::write(&start, b"start").expect("release topology writers");
        for child in &mut children {
            assert!(child.wait().expect("wait for topology writer").success());
        }
        let reopened = PrototypeStore::open(root.path(), topology);
        reopened.quick_check();
        assert_eq!(reopened.all_entries().unwrap().len(), 4 * 64);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Workload {
    LookupHit,
    DistinctInsert,
    HotReplace,
}

impl Workload {
    const ALL: [Self; 3] = [Self::LookupHit, Self::DistinctInsert, Self::HotReplace];

    const fn name(self) -> &'static str {
        match self {
            Self::LookupHit => "lookup_hit",
            Self::DistinctInsert => "distinct_insert",
            Self::HotReplace => "hot_replace",
        }
    }
}

impl FromStr for Workload {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|workload| workload.name() == value)
            .ok_or_else(|| format!("unknown workload {value:?}"))
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

#[derive(Clone, Copy)]
struct Controls {
    rows_per_shard: usize,
    operations: usize,
    warmups: usize,
    process_workers: usize,
    trials: usize,
}

impl Controls {
    const fn smoke() -> Self {
        Self {
            rows_per_shard: 4,
            operations: 16,
            warmups: 2,
            process_workers: 2,
            trials: 1,
        }
    }

    const fn full() -> Self {
        Self {
            rows_per_shard: 16,
            operations: 256,
            warmups: 16,
            process_workers: 4,
            trials: 3,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct WorkerMeasurement {
    attempts: u64,
    busy_retries: u64,
    elapsed_micros: u64,
    latencies: Vec<u64>,
}

fn seed_store(root: &Path, topology: PrototypeTopology, shards: u16, rows_per_shard: usize) {
    let store = PrototypeStore::create(root, topology);
    for shard in 0..shards {
        for row in 0..rows_per_shard {
            let encoded = key(format!("seed-{shard}-{row}"));
            store
                .insert(&encoded, shard, &(row as u64).to_le_bytes(), row as i64)
                .expect("seed topology benchmark");
        }
    }
    store.checkpoint();
}

fn run_worker(
    root: &Path,
    topology: PrototypeTopology,
    workload: Workload,
    worker: usize,
    shards: u16,
    controls: Controls,
) -> WorkerMeasurement {
    let store = PrototypeStore::open(root, topology);
    for operation in controls.operations..controls.operations + controls.warmups {
        perform_operation(
            &store,
            workload,
            worker,
            operation,
            shards,
            controls.rows_per_shard,
        );
    }
    wait_for_start_if_requested();
    let started = Instant::now();
    let mut measurement = WorkerMeasurement::default();
    for operation in 0..controls.operations {
        let operation_started = Instant::now();
        measurement.busy_retries += perform_operation(
            &store,
            workload,
            worker,
            operation,
            shards,
            controls.rows_per_shard,
        );
        measurement.attempts += 1;
        measurement
            .latencies
            .push(duration_micros(operation_started.elapsed()));
    }
    measurement.elapsed_micros = duration_micros(started.elapsed());
    measurement
}

fn perform_operation(
    store: &PrototypeStore,
    workload: Workload,
    worker: usize,
    operation: usize,
    shards: u16,
    rows_per_shard: usize,
) -> u64 {
    match workload {
        Workload::LookupHit => {
            let shard = (worker + operation) % usize::from(shards);
            let row = operation % rows_per_shard;
            let entries = store
                .lookup(&key(format!("seed-{shard}-{row}")))
                .expect("run topology lookup");
            assert_eq!(entries.len(), 1);
            0
        }
        Workload::DistinctInsert => {
            let encoded = key(format!("insert-{worker}-{operation}"));
            retry_mutation(|| {
                store.insert(
                    &encoded,
                    (worker % usize::from(shards)) as u16,
                    &(operation as u64).to_le_bytes(),
                    operation as i64,
                )
            })
        }
        Workload::HotReplace => {
            let encoded = key("one-hot-key");
            retry_mutation(|| {
                store.upsert(
                    &encoded,
                    0,
                    b"one-hot-owner",
                    i64::try_from(worker * 1_000_000 + operation).unwrap(),
                )
            })
        }
    }
}

fn wait_for_start_if_requested() {
    let Ok(path) = env::var("BRISKDB_TOPOLOGY_START") else {
        return;
    };
    let deadline = Instant::now() + CHILD_TIMEOUT;
    while !Path::new(&path).exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        Path::new(&path).exists(),
        "topology start barrier timed out"
    );
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros())
        .unwrap_or(u64::MAX)
        .max(1)
}

fn worker_tsv(measurement: &WorkerMeasurement) -> String {
    format!(
        "worker\t{}\t{}\t{}\t{}\n",
        measurement.attempts,
        measurement.busy_retries,
        measurement.elapsed_micros,
        measurement
            .latencies
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn parse_worker_tsv(value: &str) -> WorkerMeasurement {
    let fields = value.trim_end().split('\t').collect::<Vec<_>>();
    assert_eq!(fields.len(), 5);
    assert_eq!(fields[0], "worker");
    WorkerMeasurement {
        attempts: fields[1].parse().unwrap(),
        busy_retries: fields[2].parse().unwrap(),
        elapsed_micros: fields[3].parse().unwrap(),
        latencies: fields[4]
            .split(',')
            .map(|value| value.parse().unwrap())
            .collect(),
    }
}

fn run_child_mode() {
    let root = PathBuf::from(required_env("BRISKDB_TOPOLOGY_ROOT"));
    let topology = required_env("BRISKDB_TOPOLOGY_KIND")
        .parse::<PrototypeTopology>()
        .unwrap();
    let mode = required_env("BRISKDB_TOPOLOGY_MODE");
    let worker = required_env("BRISKDB_TOPOLOGY_WORKER")
        .parse::<usize>()
        .unwrap();
    let operations = required_env("BRISKDB_TOPOLOGY_OPERATIONS")
        .parse::<usize>()
        .unwrap();
    let ready = PathBuf::from(required_env("BRISKDB_TOPOLOGY_READY"));
    fs::write(&ready, b"ready").expect("publish topology child readiness");

    match mode.as_str() {
        "crash" => {
            let store = PrototypeStore::open(&root, topology);
            for ordinal in 0..operations {
                store
                    .insert(
                        &key(format!("committed-{ordinal}")),
                        (ordinal % 64) as u16,
                        &(ordinal as u64).to_le_bytes(),
                        ordinal as i64,
                    )
                    .expect("commit crash fixture row");
            }
            drop(store);
            let pending = key("uncommitted");
            let partition = topology
                .core()
                .partition_for_key(index_id(), &pending)
                .unwrap();
            let connection = open_connection(&partition_path(&root, topology, partition), false);
            connection.execute_batch("BEGIN IMMEDIATE").unwrap();
            connection
                .execute(
                    "INSERT INTO briskdb_index_entries
                     (index_id, encoded_key, source_shard, row_identity, payload)
                     VALUES (?1, ?2, 0, ?3, 999)",
                    params![INDEX_ID_VALUE as i64, pending.as_bytes(), b"pending"],
                )
                .unwrap();
            // SAFETY: this dedicated child intentionally skips destructors to
            // emulate abrupt process loss with one uncommitted SQLite write.
            unsafe { libc::_exit(91) }
        }
        "writer" => {
            wait_for_start_if_requested();
            let store = PrototypeStore::open(&root, topology);
            let mut busy = 0;
            for ordinal in 0..operations {
                let encoded = key(format!("writer-{worker}-{ordinal}"));
                busy += retry_mutation(|| {
                    store.insert(
                        &encoded,
                        (worker % 64) as u16,
                        &(ordinal as u64).to_le_bytes(),
                        ordinal as i64,
                    )
                });
            }
            fs::write(required_env("BRISKDB_TOPOLOGY_RESULT"), busy.to_string())
                .expect("write topology writer result");
        }
        "benchmark" => {
            let workload = required_env("BRISKDB_TOPOLOGY_WORKLOAD")
                .parse::<Workload>()
                .unwrap();
            let shards = required_env("BRISKDB_TOPOLOGY_SHARDS")
                .parse::<u16>()
                .unwrap();
            let rows_per_shard = required_env("BRISKDB_TOPOLOGY_ROWS")
                .parse::<usize>()
                .unwrap();
            let warmups = required_env("BRISKDB_TOPOLOGY_WARMUPS")
                .parse::<usize>()
                .unwrap();
            let measurement = run_worker(
                &root,
                topology,
                workload,
                worker,
                shards,
                Controls {
                    rows_per_shard,
                    operations,
                    warmups,
                    process_workers: 1,
                    trials: 1,
                },
            );
            fs::write(
                required_env("BRISKDB_TOPOLOGY_RESULT"),
                worker_tsv(&measurement),
            )
            .expect("write topology benchmark result");
        }
        unexpected => panic!("unexpected topology child mode {unexpected:?}"),
    }
}

#[test]
fn global_index_topology_child() {
    if env::var("BRISKDB_TOPOLOGY_CHILD").as_deref() == Ok("1") {
        run_child_mode();
    }
}

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("required environment {name} is missing"))
}

struct WalTelemetry {
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<u64>,
}

impl WalTelemetry {
    fn start(root: PathBuf, initial: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let mut peak = initial;
            while !worker_stop.load(Ordering::Acquire) {
                peak = peak.max(wal_bytes(&root));
                thread::sleep(TELEMETRY_INTERVAL);
            }
            peak.max(wal_bytes(&root)).saturating_sub(initial)
        });
        Self { stop, handle }
    }

    fn stop(self) -> u64 {
        self.stop.store(true, Ordering::Release);
        self.handle.join().expect("join topology WAL sampler")
    }
}

fn wal_bytes(root: &Path) -> u64 {
    fs::read_dir(root)
        .expect("read topology directory")
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().ends_with("-wal"))
        .filter_map(|entry| entry.metadata().ok())
        .map(|metadata| metadata.len())
        .sum()
}

fn storage_bytes(root: &Path) -> u64 {
    fs::read_dir(root)
        .expect("read topology directory")
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.ends_with(".sqlite")
                || name.ends_with(".sqlite-wal")
                || name.ends_with(".sqlite-shm")
        })
        .filter_map(|entry| entry.metadata().ok())
        .filter(|metadata| metadata.is_file())
        .map(|metadata| metadata.len())
        .sum()
}

#[derive(Clone, Debug)]
struct CaseResult {
    topology: PrototypeTopology,
    mode: RunMode,
    shards: u16,
    workload: Workload,
    workers: usize,
    attempts: u64,
    busy_retries: u64,
    elapsed_micros: u64,
    latencies: Vec<u64>,
    peak_wal_growth_bytes: u64,
    recovery_micros: u64,
    sqlite_connections: usize,
    storage_file_count: usize,
    storage_bytes: u64,
}

impl CaseResult {
    fn to_tsv(&self) -> String {
        let mut latencies = self.latencies.clone();
        latencies.sort_unstable();
        let throughput = self.attempts as f64 * 1_000_000.0 / self.elapsed_micros.max(1) as f64;
        format!(
            "result\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{throughput:.2}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\tFULL",
            self.topology.name(),
            self.mode.name(),
            self.shards,
            self.workload.name(),
            self.workers,
            self.attempts,
            self.busy_retries,
            self.elapsed_micros,
            percentile(&latencies, 50),
            percentile(&latencies, 95),
            percentile(&latencies, 99),
            self.peak_wal_growth_bytes,
            self.recovery_micros,
            self.sqlite_connections,
            self.storage_file_count,
            self.storage_bytes,
        )
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = sorted
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(sorted.len().saturating_sub(1));
    sorted[index]
}

fn run_case(
    topology: PrototypeTopology,
    mode: RunMode,
    shards: u16,
    workload: Workload,
    controls: Controls,
) -> CaseResult {
    let root = tempfile::tempdir().expect("create topology benchmark fixture");
    seed_store(root.path(), topology, shards, controls.rows_per_shard);
    let initial_wal = wal_bytes(root.path());
    let telemetry = WalTelemetry::start(root.path().to_path_buf(), initial_wal);
    let workers = match mode {
        RunMode::SingleProcess => 1,
        RunMode::MultiProcess => controls.process_workers,
    };
    let started = Instant::now();
    let measurements = match mode {
        RunMode::SingleProcess => vec![run_worker(
            root.path(),
            topology,
            workload,
            0,
            shards,
            controls,
        )],
        RunMode::MultiProcess => {
            run_benchmark_processes(root.path(), topology, workload, shards, controls)
        }
    };
    let wall_micros = duration_micros(started.elapsed());
    let peak_wal_growth_bytes = telemetry.stop();
    let attempts = measurements.iter().map(|value| value.attempts).sum();
    let busy_retries = measurements.iter().map(|value| value.busy_retries).sum();
    let elapsed_micros = measurements
        .iter()
        .map(|value| value.elapsed_micros)
        .max()
        .unwrap_or(wall_micros);
    let latencies = measurements
        .into_iter()
        .flat_map(|value| value.latencies)
        .collect::<Vec<_>>();

    let recovery_started = Instant::now();
    let reopened = PrototypeStore::open(root.path(), topology);
    reopened.quick_check();
    let entries = reopened.all_entries().unwrap();
    let recovery_micros = duration_micros(recovery_started.elapsed());
    let seeded = usize::from(shards) * controls.rows_per_shard;
    match workload {
        Workload::LookupHit => assert_eq!(entries.len(), seeded),
        Workload::DistinctInsert => assert_eq!(
            entries.len(),
            seeded + workers * (controls.operations + controls.warmups)
        ),
        Workload::HotReplace => assert_eq!(entries.len(), seeded + 1),
    }
    drop(reopened);

    CaseResult {
        topology,
        mode,
        shards,
        workload,
        workers,
        attempts,
        busy_retries,
        elapsed_micros,
        latencies,
        peak_wal_growth_bytes,
        recovery_micros,
        sqlite_connections: workers * usize::from(topology.partition_count()),
        storage_file_count: usize::from(topology.partition_count()),
        storage_bytes: storage_bytes(root.path()),
    }
}

fn run_benchmark_processes(
    root: &Path,
    topology: PrototypeTopology,
    workload: Workload,
    shards: u16,
    controls: Controls,
) -> Vec<WorkerMeasurement> {
    let start = root.join("benchmark-start");
    let mut children = Vec::new();
    let mut ready_paths = Vec::new();
    let mut result_paths = Vec::new();
    for worker in 0..controls.process_workers {
        let ready = root.join(format!("benchmark-{worker}-ready"));
        let result = root.join(format!("benchmark-{worker}-result"));
        let mut command = Command::new(env::current_exe().expect("resolve topology executable"));
        command
            .args(["--exact", "global_index_topology_child", "--nocapture"])
            .env("BRISKDB_TOPOLOGY_CHILD", "1")
            .env("BRISKDB_TOPOLOGY_ROOT", root)
            .env("BRISKDB_TOPOLOGY_KIND", topology.name())
            .env("BRISKDB_TOPOLOGY_MODE", "benchmark")
            .env("BRISKDB_TOPOLOGY_WORKLOAD", workload.name())
            .env("BRISKDB_TOPOLOGY_WORKER", worker.to_string())
            .env("BRISKDB_TOPOLOGY_SHARDS", shards.to_string())
            .env("BRISKDB_TOPOLOGY_ROWS", controls.rows_per_shard.to_string())
            .env(
                "BRISKDB_TOPOLOGY_OPERATIONS",
                controls.operations.to_string(),
            )
            .env("BRISKDB_TOPOLOGY_WARMUPS", controls.warmups.to_string())
            .env("BRISKDB_TOPOLOGY_READY", &ready)
            .env("BRISKDB_TOPOLOGY_START", &start)
            .env("BRISKDB_TOPOLOGY_RESULT", &result)
            .stdout(Stdio::null());
        children.push(command.spawn().expect("spawn topology benchmark child"));
        ready_paths.push(ready);
        result_paths.push(result);
    }
    wait_for_paths(&ready_paths);
    fs::write(&start, b"start").expect("release topology benchmark children");
    for child in &mut children {
        assert!(child.wait().expect("wait for topology benchmark").success());
    }
    result_paths
        .into_iter()
        .map(|path| parse_worker_tsv(&fs::read_to_string(path).unwrap()))
        .collect()
}

const RESULT_HEADER: &str = "record\ttopology\tmode\tshards\tworkload\tworkers\tattempts\tbusy_retries\telapsed_us\tthroughput_ops_s\tp50_us\tp95_us\tp99_us\tpeak_wal_growth_bytes\trecovery_us\tsqlite_connections\tstorage_file_count\tstorage_bytes\tsqlite_synchronous";

fn run_matrix(shards: &[u16], controls: Controls) -> Vec<CaseResult> {
    println!("metadata\tformat\t{FORMAT_VERSION}");
    println!("metadata\tos\t{}", env::consts::OS);
    println!("metadata\tarch\t{}", env::consts::ARCH);
    println!("control\trows_per_shard\t{}", controls.rows_per_shard);
    println!("control\toperations\t{}", controls.operations);
    println!("control\twarmups\t{}", controls.warmups);
    println!("control\tprocess_workers\t{}", controls.process_workers);
    println!("control\ttrials\t{}", controls.trials);
    println!("control\ttrial_selection\tmedian_elapsed");
    println!("control\tjournal_mode\tWAL");
    println!("control\tsynchronous\tFULL");
    println!("control\twal_autocheckpoint\t0");
    println!("{RESULT_HEADER}");
    let mut results = Vec::new();
    for shards in shards.iter().copied() {
        for mode in RunMode::ALL {
            for workload in Workload::ALL {
                for topology in PrototypeTopology::ALL {
                    let mut trials = (0..controls.trials)
                        .map(|_| run_case(topology, mode, shards, workload, controls))
                        .collect::<Vec<_>>();
                    trials.sort_by_key(|result| result.elapsed_micros);
                    let result = trials.swap_remove(trials.len() / 2);
                    println!("{}", result.to_tsv());
                    results.push(result);
                }
            }
        }
    }
    results
}

#[test]
fn topology_report_rows_are_fixed_and_comparable() {
    let sample = CaseResult {
        topology: PrototypeTopology::Shared,
        mode: RunMode::SingleProcess,
        shards: 4,
        workload: Workload::LookupHit,
        workers: 1,
        attempts: 2,
        busy_retries: 0,
        elapsed_micros: 10,
        latencies: vec![2, 4],
        peak_wal_growth_bytes: 100,
        recovery_micros: 20,
        sqlite_connections: 1,
        storage_file_count: 1,
        storage_bytes: 4096,
    }
    .to_tsv();
    assert_eq!(
        sample.split('\t').count(),
        RESULT_HEADER.split('\t').count()
    );
    assert!(sample.starts_with("result\tshared_sqlite_v1\tsingle_process\t4\tlookup_hit"));
}

#[test]
fn frozen_topology_report_contains_every_paired_matrix_case() {
    let report = include_str!("../docs/benchmarks/global-index-topology-2026-08-15.tsv");
    assert!(report.contains("control\ttrials\t3"));
    assert!(report.contains("control\ttrial_selection\tmedian_elapsed"));
    let rows = report
        .lines()
        .filter(|line| line.starts_with("result\t"))
        .collect::<Vec<_>>();
    assert_eq!(
        rows.len(),
        4 * PrototypeTopology::ALL.len() * RunMode::ALL.len() * Workload::ALL.len()
    );
    assert!(
        rows.iter()
            .all(|row| row.split('\t').count() == RESULT_HEADER.split('\t').count())
    );
    for shards in [2, 4, 10, 64] {
        for mode in RunMode::ALL {
            for workload in Workload::ALL {
                for topology in PrototypeTopology::ALL {
                    let prefix = format!(
                        "result\t{}\t{}\t{shards}\t{}\t",
                        topology.name(),
                        mode.name(),
                        workload.name()
                    );
                    assert_eq!(
                        rows.iter().filter(|row| row.starts_with(&prefix)).count(),
                        1,
                        "missing or duplicate matrix row {prefix}"
                    );
                }
            }
        }
    }
}

#[test]
#[ignore = "stable Linux correctness smoke for the issue #229 topology decision"]
fn global_index_topology_smoke() {
    let results = run_matrix(&[4], Controls::smoke());
    assert_eq!(
        results.len(),
        PrototypeTopology::ALL.len() * RunMode::ALL.len() * Workload::ALL.len()
    );
}

#[test]
#[ignore = "manual release-mode issue #229 topology benchmark"]
fn release_global_index_topology_benchmark() {
    if cfg!(debug_assertions) {
        panic!("run the topology matrix with --release");
    }
    let results = run_matrix(&[2, 4, 10, 64], Controls::full());
    assert_eq!(
        results.len(),
        4 * PrototypeTopology::ALL.len() * RunMode::ALL.len() * Workload::ALL.len()
    );
}
