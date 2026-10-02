#![cfg(all(unix, feature = "isam-benchmark"))]

use std::{
    fs,
    hint::black_box,
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use briskdb::{
    core::Value,
    isam::{
        ColumnDefinition, ColumnType, IndexDefinition, Layout, LockPolicy, Mutation, NativeCatalog,
        NativeValue, OperationStats, Record, Store, TableDefinition,
    },
    storage::Database,
};
use serde::Serialize;

const CHAPTER_ROWS: usize = 36;
const SEED_CHAPTERS: u32 = 10;
const SEED_FIRST_CHAPTER: u32 = 100;
const SEED_REFRESH_CHAPTER: u32 = 103;
const SQLITE_CREATE: &str = "CREATE TABLE isam_bench (id TEXT PRIMARY KEY, body TEXT NOT NULL)";
const SQLITE_CATALOG_CREATE: &str =
    "CREATE TABLE isam_catalog_bench (id TEXT PRIMARY KEY, body TEXT NOT NULL)";
const SQLITE_CATALOG_INDEX: &str = "CREATE INDEX isam_catalog_body ON isam_catalog_bench (body)";
const SQLITE_READ: &str = "SELECT id, body FROM isam_bench WHERE id = ?1";
const SQLITE_RANGE: &str =
    "SELECT id, body FROM isam_bench WHERE id >= ?1 AND id < ?2 ORDER BY id LIMIT 36";
const SQLITE_UPSERT: &str = "INSERT INTO isam_bench (id, body) VALUES __VALUES__ ON CONFLICT(id) DO UPDATE SET body=excluded.body";
const FLAT_KEY_BYTES: usize = 11;
const FLAT_VALUE_BYTES: usize = 128;
const FLAT_RECORD_BYTES: usize = FLAT_KEY_BYTES + FLAT_VALUE_BYTES;
const CATALOG_TABLE: &str = "bench_rows";
const CATALOG_ID_INDEX: &str = "by_id";
const CATALOG_BODY_INDEX: &str = "by_body";

fn catalog_schema() -> TableDefinition {
    TableDefinition {
        name: CATALOG_TABLE.to_owned(),
        schema_version: 1,
        columns: vec![
            ColumnDefinition {
                name: "id".to_owned(),
                column_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                name: "body".to_owned(),
                column_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".to_owned()],
        indexes: vec![
            IndexDefinition {
                name: CATALOG_ID_INDEX.to_owned(),
                columns: vec!["id".to_owned()],
                unique: true,
            },
            IndexDefinition {
                name: CATALOG_BODY_INDEX.to_owned(),
                columns: vec!["body".to_owned()],
                unique: false,
            },
        ],
    }
}

fn catalog_values(id: String, body: String) -> Vec<NativeValue> {
    vec![NativeValue::Text(id), NativeValue::Text(body)]
}

fn catalog_id(sample: usize) -> String {
    format!("typed-{sample:06}")
}

fn catalog_key(id: &str) -> [NativeValue; 1] {
    [NativeValue::Text(id.to_owned())]
}

fn catalog_seed_rows() -> Vec<(String, String)> {
    (0..4_000)
        .map(|sample| (catalog_id(sample), format!("seed-body-{sample}")))
        .collect()
}

fn catalog_sql_upsert(rows: &[(String, String)]) -> SqlWrite {
    let mut values = Vec::with_capacity(rows.len());
    let mut params = Vec::with_capacity(rows.len() * 2);
    for (index, (id, body)) in rows.iter().enumerate() {
        let position = index * 2;
        values.push(format!("(?{}, ?{})", position + 1, position + 2));
        params.push(Value::from(id.clone()));
        params.push(Value::from(body.clone()));
    }
    SqlWrite {
        sql: format!(
            "INSERT INTO isam_catalog_bench (id, body) VALUES {} ON CONFLICT(id) DO UPDATE SET body=excluded.body",
            values.join(", ")
        ),
        params,
    }
}

fn catalog_sql_delete(ids: &[String]) -> SqlWrite {
    let placeholders = (1..=ids.len())
        .map(|position| format!("?{position}"))
        .collect::<Vec<_>>()
        .join(", ");
    SqlWrite {
        sql: format!("DELETE FROM isam_catalog_bench WHERE id IN ({placeholders})"),
        params: ids.iter().cloned().map(Value::from).collect(),
    }
}

#[derive(Debug)]
struct Measurement {
    backend: &'static str,
    workload: &'static str,
    durations: Vec<u128>,
    operations: Option<OperationStats>,
}

struct OperationTotal {
    file_opens: u128,
    file_closes: u128,
    file_stats: u128,
    root_reads: u128,
    root_writes: u128,
    page_reads: u128,
    page_writes: u128,
    root_read_ns: u128,
    root_write_ns: u128,
    page_read_ns: u128,
    page_write_ns: u128,
    syncs: u128,
    sync_requests: u128,
    sync_ns: u128,
    publication_ns: u128,
    preflight_rebases: u128,
    publication_retries: u128,
    commit_lock_wait_ns: u128,
    lock_requests: u128,
    lock_retries: u128,
    lock_wait_ns: u128,
    write_lock_batches: u128,
    write_lock_keys: u128,
    write_lock_requests: u128,
    write_lock_retries: u128,
    write_lock_wait_ns: u128,
    write_lock_local_retries: u128,
    write_lock_local_wait_ns: u128,
    write_lock_range_retries: u128,
    write_lock_range_wait_ns: u128,
    write_lock_stripes_acquired: u128,
    write_lock_stripe_acquisitions: [u128; briskdb::isam::KEY_LOCK_STRIPES],
    write_lock_stripe_retries: [u128; briskdb::isam::KEY_LOCK_STRIPES],
    write_lock_stripe_wait_ns: [u128; briskdb::isam::KEY_LOCK_STRIPES],
}

impl Default for OperationTotal {
    fn default() -> Self {
        Self {
            file_opens: 0,
            file_closes: 0,
            file_stats: 0,
            root_reads: 0,
            root_writes: 0,
            page_reads: 0,
            page_writes: 0,
            root_read_ns: 0,
            root_write_ns: 0,
            page_read_ns: 0,
            page_write_ns: 0,
            syncs: 0,
            sync_requests: 0,
            sync_ns: 0,
            publication_ns: 0,
            preflight_rebases: 0,
            publication_retries: 0,
            commit_lock_wait_ns: 0,
            lock_requests: 0,
            lock_retries: 0,
            lock_wait_ns: 0,
            write_lock_batches: 0,
            write_lock_keys: 0,
            write_lock_requests: 0,
            write_lock_retries: 0,
            write_lock_wait_ns: 0,
            write_lock_local_retries: 0,
            write_lock_local_wait_ns: 0,
            write_lock_range_retries: 0,
            write_lock_range_wait_ns: 0,
            write_lock_stripes_acquired: 0,
            write_lock_stripe_acquisitions: [0; briskdb::isam::KEY_LOCK_STRIPES],
            write_lock_stripe_retries: [0; briskdb::isam::KEY_LOCK_STRIPES],
            write_lock_stripe_wait_ns: [0; briskdb::isam::KEY_LOCK_STRIPES],
        }
    }
}

impl OperationTotal {
    fn add(&mut self, stats: OperationStats) {
        self.file_opens += u128::from(stats.file_opens);
        self.file_closes += u128::from(stats.file_closes);
        self.file_stats += u128::from(stats.file_stats);
        self.root_reads += u128::from(stats.root_reads);
        self.root_writes += u128::from(stats.root_writes);
        self.page_reads += u128::from(stats.page_reads);
        self.page_writes += u128::from(stats.page_writes);
        self.root_read_ns += u128::from(stats.root_read_ns);
        self.root_write_ns += u128::from(stats.root_write_ns);
        self.page_read_ns += u128::from(stats.page_read_ns);
        self.page_write_ns += u128::from(stats.page_write_ns);
        self.syncs += u128::from(stats.syncs);
        self.sync_requests += u128::from(stats.sync_requests);
        self.sync_ns += u128::from(stats.sync_ns);
        self.publication_ns += u128::from(stats.publication_ns);
        self.preflight_rebases += u128::from(stats.preflight_rebases);
        self.publication_retries += u128::from(stats.publication_retries);
        self.commit_lock_wait_ns += u128::from(stats.commit_lock_wait_ns);
        self.lock_requests += u128::from(stats.lock_requests);
        self.lock_retries += u128::from(stats.lock_retries);
        self.lock_wait_ns += u128::from(stats.lock_wait_ns);
        self.write_lock_batches += u128::from(stats.write_lock_batches);
        self.write_lock_keys += u128::from(stats.write_lock_keys);
        self.write_lock_requests += u128::from(stats.write_lock_requests);
        self.write_lock_retries += u128::from(stats.write_lock_retries);
        self.write_lock_wait_ns += u128::from(stats.write_lock_wait_ns);
        self.write_lock_local_retries += u128::from(stats.write_lock_local_retries);
        self.write_lock_local_wait_ns += u128::from(stats.write_lock_local_wait_ns);
        self.write_lock_range_retries += u128::from(stats.write_lock_range_retries);
        self.write_lock_range_wait_ns += u128::from(stats.write_lock_range_wait_ns);
        self.write_lock_stripes_acquired += u128::from(stats.write_lock_stripes_acquired);
        for stripe in 0..briskdb::isam::KEY_LOCK_STRIPES {
            self.write_lock_stripe_acquisitions[stripe] +=
                u128::from(stats.write_lock_stripe_acquisitions[stripe]);
            self.write_lock_stripe_retries[stripe] +=
                u128::from(stats.write_lock_stripe_retries[stripe]);
            self.write_lock_stripe_wait_ns[stripe] +=
                u128::from(stats.write_lock_stripe_wait_ns[stripe]);
        }
    }

    fn average(&self, samples: usize) -> String {
        if samples == 0 {
            return String::new();
        }
        [
            self.file_opens / samples as u128,
            self.file_closes / samples as u128,
            self.file_stats / samples as u128,
            self.root_reads / samples as u128,
            self.root_writes / samples as u128,
            self.page_reads / samples as u128,
            self.page_writes / samples as u128,
            self.root_read_ns / samples as u128,
            self.root_write_ns / samples as u128,
            self.page_read_ns / samples as u128,
            self.page_write_ns / samples as u128,
            self.syncs / samples as u128,
            self.sync_ns / samples as u128,
            self.publication_ns / samples as u128,
            self.preflight_rebases / samples as u128,
            self.publication_retries / samples as u128,
            self.commit_lock_wait_ns / samples as u128,
            self.lock_requests / samples as u128,
            self.lock_retries / samples as u128,
            self.lock_wait_ns / samples as u128,
            self.write_lock_batches / samples as u128,
            self.write_lock_keys / samples as u128,
            self.write_lock_requests / samples as u128,
            self.write_lock_retries / samples as u128,
            self.write_lock_wait_ns / samples as u128,
            self.write_lock_local_retries / samples as u128,
            self.write_lock_local_wait_ns / samples as u128,
            self.write_lock_range_retries / samples as u128,
            self.write_lock_range_wait_ns / samples as u128,
            self.write_lock_stripes_acquired / samples as u128,
        ]
        .map(|value| value.to_string())
        .join("\t")
    }
}

struct IsamFixture {
    _directory: tempfile::TempDir,
    path: PathBuf,
    store: Store,
}

fn packed_pages() -> bool {
    std::env::var("BRISKDB_ISAM_PACKED").is_ok_and(|value| value == "1")
}

fn create_store(path: &std::path::Path) -> briskdb::isam::Result<Store> {
    let layout = Layout::new(11, 128)?;
    if packed_pages() {
        Store::create_packed(path, layout)
    } else {
        Store::create(path, layout)
    }
}

impl IsamFixture {
    fn seeded() -> Self {
        let directory = tempfile::tempdir().expect("create ISAM benchmark directory");
        let path = directory.path().join("bench.isam");
        let mut store = create_store(&path).expect("create ISAM benchmark store");
        store
            .write_batch(&seed_mutations(SEED_FIRST_CHAPTER, SEED_CHAPTERS, "seed"))
            .expect("seed ISAM benchmark store");
        Self {
            _directory: directory,
            path,
            store,
        }
    }

    fn new_empty() -> Self {
        let directory = tempfile::tempdir().expect("create ISAM benchmark directory");
        let path = directory.path().join("bench.isam");
        let store = create_store(&path).expect("create ISAM benchmark store");
        Self {
            _directory: directory,
            path,
            store,
        }
    }
}

struct NativeCatalogFixture {
    _directory: tempfile::TempDir,
    catalog: NativeCatalog,
}

impl NativeCatalogFixture {
    fn seeded() -> Self {
        let directory = tempfile::tempdir().expect("create native catalog benchmark directory");
        let path = directory.path().join("catalog.isam");
        let mut catalog = if packed_pages() {
            NativeCatalog::create_packed(&path)
        } else {
            NativeCatalog::create(&path)
        }
        .expect("create native catalog fixture");
        catalog
            .create_table(&catalog_schema())
            .expect("create native catalog benchmark schema");
        let rows = catalog_seed_rows();
        for chunk in rows.chunks(1_000) {
            let values: Vec<_> = chunk
                .iter()
                .map(|(id, body)| catalog_values(id.clone(), body.clone()))
                .collect();
            catalog
                .insert_rows(CATALOG_TABLE, &values)
                .expect("seed native catalog benchmark");
        }
        catalog.reset_operation_stats();
        Self {
            _directory: directory,
            catalog,
        }
    }
}

struct SqliteFixture {
    _directory: tempfile::TempDir,
    root: PathBuf,
    database: Database,
}

impl SqliteFixture {
    fn seeded() -> Self {
        let directory = tempfile::tempdir().expect("create SQLite benchmark directory");
        let database = Database::open(directory.path(), 2).expect("open SQLite benchmark");
        database
            .broadcast(SQLITE_CREATE)
            .expect("create SQLite benchmark schema");
        let seed = sql_upsert(SEED_FIRST_CHAPTER, SEED_CHAPTERS, "seed");
        database
            .execute("benchmark", &seed.sql, &seed.params)
            .expect("seed SQLite benchmark");
        Self {
            root: directory.path().to_path_buf(),
            _directory: directory,
            database,
        }
    }

    fn new_empty() -> Self {
        let directory = tempfile::tempdir().expect("create SQLite benchmark directory");
        let database = Database::open(directory.path(), 2).expect("open SQLite benchmark");
        database
            .broadcast(SQLITE_CREATE)
            .expect("create SQLite benchmark schema");
        Self {
            root: directory.path().to_path_buf(),
            _directory: directory,
            database,
        }
    }
}

struct SqliteCatalogFixture {
    _directory: tempfile::TempDir,
    database: Database,
}

impl SqliteCatalogFixture {
    fn seeded() -> Self {
        let directory = tempfile::tempdir().expect("create SQLite catalog benchmark directory");
        let database = Database::open(directory.path(), 2).expect("open SQLite catalog benchmark");
        database
            .broadcast(SQLITE_CATALOG_CREATE)
            .expect("create SQLite catalog benchmark schema");
        database
            .broadcast(SQLITE_CATALOG_INDEX)
            .expect("create SQLite catalog benchmark index");
        let seed = catalog_sql_upsert(&catalog_seed_rows());
        database
            .execute("benchmark", &seed.sql, &seed.params)
            .expect("seed SQLite catalog benchmark");
        Self {
            _directory: directory,
            database,
        }
    }
}

struct FlatFileFixture {
    _directory: tempfile::TempDir,
    file: fs::File,
}

impl FlatFileFixture {
    fn seeded() -> Self {
        let directory = tempfile::tempdir().expect("create flat-file benchmark directory");
        let path = directory.path().join("records.flat");
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .expect("create flat-file fixture");
        for chapter_offset in 0..SEED_CHAPTERS {
            for verse in 0..CHAPTER_ROWS {
                let record = flat_record(
                    SEED_FIRST_CHAPTER + chapter_offset,
                    verse,
                    &format!("seed-{chapter_offset}-{verse}"),
                );
                file.write_all_at(
                    &record,
                    flat_offset(SEED_FIRST_CHAPTER + chapter_offset, verse),
                )
                .expect("write flat-file fixture");
            }
        }
        file.sync_all().expect("sync flat-file fixture");
        Self {
            _directory: directory,
            file,
        }
    }

    fn read_point(&self, chapter: u32, verse: usize) -> Vec<u8> {
        read_flat_record(&self.file, flat_offset(chapter, verse))
    }

    fn read_chapter(&self, chapter: u32) -> Vec<Vec<u8>> {
        (0..CHAPTER_ROWS)
            .map(|verse| self.read_point(chapter, verse))
            .collect()
    }

    fn refresh_chapter(&self, chapter: u32) {
        for verse in 0..CHAPTER_ROWS {
            let record = flat_record(chapter, verse, "refresh");
            self.file
                .write_all_at(&record, flat_offset(chapter, verse))
                .expect("refresh flat-file records");
        }
        self.file.sync_all().expect("sync flat-file refresh");
    }
}

struct SqlWrite {
    sql: String,
    params: Vec<Value>,
}

#[derive(Serialize)]
struct JsonBenchRow<'a> {
    id: &'a str,
    body: &'a str,
}

#[derive(Serialize)]
struct OwnedJsonBenchRow {
    id: String,
    body: String,
}

fn main_key(chapter: u32, verse: usize) -> String {
    format!("C{chapter:07}{verse:03}")
}

fn chapter_end(chapter: u32) -> String {
    format!("C{:07}000", chapter + 1)
}

fn flat_offset(chapter: u32, verse: usize) -> u64 {
    let chapter_index = chapter
        .checked_sub(SEED_FIRST_CHAPTER)
        .expect("flat-file chapter precedes fixture");
    u64::from(chapter_index)
        .checked_mul(CHAPTER_ROWS as u64)
        .and_then(|records| records.checked_add(verse as u64))
        .and_then(|record| record.checked_mul(FLAT_RECORD_BYTES as u64))
        .expect("flat-file record offset")
}

fn flat_record(chapter: u32, verse: usize, body: &str) -> Vec<u8> {
    let mut record = vec![0; FLAT_RECORD_BYTES];
    let key = main_key(chapter, verse);
    record[..FLAT_KEY_BYTES].copy_from_slice(key.as_bytes());
    let body = body.as_bytes();
    assert!(body.len() <= FLAT_VALUE_BYTES);
    record[FLAT_KEY_BYTES..FLAT_KEY_BYTES + body.len()].copy_from_slice(body);
    record
}

fn read_flat_record(file: &fs::File, offset: u64) -> Vec<u8> {
    let mut record = vec![0; FLAT_RECORD_BYTES];
    file.read_exact_at(&mut record, offset)
        .expect("read flat-file record");
    record
}

fn seed_mutations(chapter: u32, chapters: u32, value_prefix: &str) -> Vec<Mutation> {
    (0..chapters)
        .flat_map(|offset| {
            (0..CHAPTER_ROWS).map(move |verse| {
                Mutation::insert(
                    main_key(chapter + offset, verse).into_bytes(),
                    format!("{value_prefix}-{offset}-{verse}").into_bytes(),
                )
            })
        })
        .collect()
}

fn sql_upsert(chapter: u32, chapters: u32, value_prefix: &str) -> SqlWrite {
    let mut values = Vec::new();
    let mut params = Vec::new();
    for offset in 0..chapters {
        for verse in 0..CHAPTER_ROWS {
            let position = params.len();
            values.push(format!("(?{}, ?{})", position + 1, position + 2));
            params.push(Value::from(main_key(chapter + offset, verse)));
            params.push(Value::from(format!("{value_prefix}-{offset}-{verse}")));
        }
    }
    SqlWrite {
        sql: SQLITE_UPSERT.replace("__VALUES__", &values.join(", ")),
        params,
    }
}

fn stats_delta(before: OperationStats, after: OperationStats) -> OperationStats {
    OperationStats {
        file_opens: after.file_opens - before.file_opens,
        file_closes: after.file_closes - before.file_closes,
        file_stats: after.file_stats - before.file_stats,
        root_reads: after.root_reads - before.root_reads,
        root_writes: after.root_writes - before.root_writes,
        page_reads: after.page_reads - before.page_reads,
        page_writes: after.page_writes - before.page_writes,
        root_read_ns: after.root_read_ns - before.root_read_ns,
        root_write_ns: after.root_write_ns - before.root_write_ns,
        page_read_ns: after.page_read_ns - before.page_read_ns,
        page_write_ns: after.page_write_ns - before.page_write_ns,
        syncs: after.syncs - before.syncs,
        sync_requests: after.sync_requests - before.sync_requests,
        sync_ns: after.sync_ns - before.sync_ns,
        publication_ns: after.publication_ns - before.publication_ns,
        preflight_rebases: after.preflight_rebases - before.preflight_rebases,
        publication_retries: after.publication_retries - before.publication_retries,
        commit_lock_wait_ns: after.commit_lock_wait_ns - before.commit_lock_wait_ns,
        lock_requests: after.lock_requests - before.lock_requests,
        lock_retries: after.lock_retries - before.lock_retries,
        lock_wait_ns: after.lock_wait_ns - before.lock_wait_ns,
        write_lock_batches: after.write_lock_batches - before.write_lock_batches,
        write_lock_keys: after.write_lock_keys - before.write_lock_keys,
        write_lock_requests: after.write_lock_requests - before.write_lock_requests,
        write_lock_retries: after.write_lock_retries - before.write_lock_retries,
        write_lock_wait_ns: after.write_lock_wait_ns - before.write_lock_wait_ns,
        write_lock_local_retries: after.write_lock_local_retries - before.write_lock_local_retries,
        write_lock_local_wait_ns: after.write_lock_local_wait_ns - before.write_lock_local_wait_ns,
        write_lock_range_retries: after.write_lock_range_retries - before.write_lock_range_retries,
        write_lock_range_wait_ns: after.write_lock_range_wait_ns - before.write_lock_range_wait_ns,
        write_lock_stripes_acquired: after.write_lock_stripes_acquired
            - before.write_lock_stripes_acquired,
        write_lock_stripe_acquisitions: std::array::from_fn(|stripe| {
            after.write_lock_stripe_acquisitions[stripe]
                - before.write_lock_stripe_acquisitions[stripe]
        }),
        write_lock_stripe_retries: std::array::from_fn(|stripe| {
            after.write_lock_stripe_retries[stripe] - before.write_lock_stripe_retries[stripe]
        }),
        write_lock_stripe_wait_ns: std::array::from_fn(|stripe| {
            after.write_lock_stripe_wait_ns[stripe] - before.write_lock_stripe_wait_ns[stripe]
        }),
    }
}

fn measure_isam(
    workload: &'static str,
    samples: usize,
    mut operation: impl FnMut(usize) -> (Duration, OperationStats),
) -> Measurement {
    let mut durations = Vec::with_capacity(samples);
    let mut total = OperationTotal::default();
    for sample in 0..samples {
        let (duration, stats) = operation(sample);
        durations.push(duration.as_nanos());
        total.add(stats);
    }
    Measurement {
        backend: "isam",
        workload,
        durations,
        operations: Some(OperationStats {
            file_opens: total.file_opens.min(u64::MAX as u128) as u64,
            file_closes: total.file_closes.min(u64::MAX as u128) as u64,
            file_stats: total.file_stats.min(u64::MAX as u128) as u64,
            root_reads: total.root_reads.min(u64::MAX as u128) as u64,
            root_writes: total.root_writes.min(u64::MAX as u128) as u64,
            page_reads: total.page_reads.min(u64::MAX as u128) as u64,
            page_writes: total.page_writes.min(u64::MAX as u128) as u64,
            root_read_ns: total.root_read_ns.min(u64::MAX as u128) as u64,
            root_write_ns: total.root_write_ns.min(u64::MAX as u128) as u64,
            page_read_ns: total.page_read_ns.min(u64::MAX as u128) as u64,
            page_write_ns: total.page_write_ns.min(u64::MAX as u128) as u64,
            syncs: total.syncs.min(u64::MAX as u128) as u64,
            sync_requests: total.sync_requests.min(u64::MAX as u128) as u64,
            sync_ns: total.sync_ns.min(u64::MAX as u128) as u64,
            publication_ns: total.publication_ns.min(u64::MAX as u128) as u64,
            preflight_rebases: total.preflight_rebases.min(u64::MAX as u128) as u64,
            publication_retries: total.publication_retries.min(u64::MAX as u128) as u64,
            commit_lock_wait_ns: total.commit_lock_wait_ns.min(u64::MAX as u128) as u64,
            lock_requests: total.lock_requests.min(u64::MAX as u128) as u64,
            lock_retries: total.lock_retries.min(u64::MAX as u128) as u64,
            lock_wait_ns: total.lock_wait_ns.min(u64::MAX as u128) as u64,
            write_lock_batches: total.write_lock_batches.min(u64::MAX as u128) as u64,
            write_lock_keys: total.write_lock_keys.min(u64::MAX as u128) as u64,
            write_lock_requests: total.write_lock_requests.min(u64::MAX as u128) as u64,
            write_lock_retries: total.write_lock_retries.min(u64::MAX as u128) as u64,
            write_lock_wait_ns: total.write_lock_wait_ns.min(u64::MAX as u128) as u64,
            write_lock_local_retries: total.write_lock_local_retries.min(u64::MAX as u128) as u64,
            write_lock_local_wait_ns: total.write_lock_local_wait_ns.min(u64::MAX as u128) as u64,
            write_lock_range_retries: total.write_lock_range_retries.min(u64::MAX as u128) as u64,
            write_lock_range_wait_ns: total.write_lock_range_wait_ns.min(u64::MAX as u128) as u64,
            write_lock_stripes_acquired: total.write_lock_stripes_acquired.min(u64::MAX as u128)
                as u64,
            write_lock_stripe_acquisitions: std::array::from_fn(|stripe| {
                total.write_lock_stripe_acquisitions[stripe].min(u64::MAX as u128) as u64
            }),
            write_lock_stripe_retries: std::array::from_fn(|stripe| {
                total.write_lock_stripe_retries[stripe].min(u64::MAX as u128) as u64
            }),
            write_lock_stripe_wait_ns: std::array::from_fn(|stripe| {
                total.write_lock_stripe_wait_ns[stripe].min(u64::MAX as u128) as u64
            }),
        }),
    }
}

fn measure_catalog(
    workload: &'static str,
    catalog: &mut NativeCatalog,
    samples: usize,
    mut operation: impl FnMut(&mut NativeCatalog, usize) -> Duration,
) -> Measurement {
    let mut durations = Vec::with_capacity(samples);
    let mut total = OperationTotal::default();
    for sample in 0..samples {
        let before = catalog.operation_stats();
        durations.push(operation(catalog, sample).as_nanos());
        total.add(stats_delta(before, catalog.operation_stats()));
    }
    Measurement {
        backend: "isam_catalog",
        workload,
        durations,
        operations: Some(stats_from_total(total)),
    }
}

// Same rows, schema, values and starting state. The individual path deliberately
// has 36 transaction boundaries; the bulk path has one. This measures explicit
// batching, not automatic group commit or independent-client throughput.
fn measure_catalog_batching(samples: usize, measurements: &mut Vec<Measurement>) {
    let mut individual = NativeCatalogFixture::seeded();
    let mut bulk = NativeCatalogFixture::seeded();
    let sqlite = SqliteCatalogFixture::seeded();
    let mut results: Vec<_> = ["group_insert_36", "group_update_36", "group_delete_36"]
        .into_iter()
        .flat_map(|workload| {
            ["isam_individual", "isam_bulk", "sqlite"].map(|backend| Measurement {
                backend,
                workload,
                durations: Vec::with_capacity(samples),
                operations: (backend != "sqlite").then(OperationStats::default),
            })
        })
        .collect();
    let mut totals: Vec<_> = (0..9).map(|_| OperationTotal::default()).collect();
    for sample in 0..samples {
        let initial: Vec<_> = (0..CHAPTER_ROWS)
            .map(|offset| {
                (
                    catalog_id(100_000 + sample * CHAPTER_ROWS + offset),
                    format!("batch-insert-{sample}-{offset}"),
                )
            })
            .collect();
        let changed: Vec<_> = initial
            .iter()
            .enumerate()
            .map(|(offset, (id, _))| (id.clone(), format!("batch-update-{sample}-{offset}")))
            .collect();
        let keys: Vec<_> = initial
            .iter()
            .map(|(id, _)| catalog_key(id).to_vec())
            .collect();
        for operation in 0..3 {
            let rows = if operation == 0 { &initial } else { &changed };
            let values: Vec<_> = rows
                .iter()
                .map(|(id, body)| catalog_values(id.clone(), body.clone()))
                .collect();
            let updates: Vec<_> = keys.iter().cloned().zip(values.iter().cloned()).collect();
            let sql = if operation == 2 {
                catalog_sql_delete(&rows.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>())
            } else {
                catalog_sql_upsert(rows)
            };
            for backend in if sample % 2 == 0 {
                [0, 1, 2]
            } else {
                [2, 1, 0]
            } {
                let index = operation * 3 + backend;
                if backend == 2 {
                    let start = Instant::now();
                    assert_eq!(
                        sqlite
                            .database
                            .execute("benchmark", &sql.sql, &sql.params)
                            .unwrap(),
                        CHAPTER_ROWS
                    );
                    results[index].durations.push(start.elapsed().as_nanos());
                    for (id, body) in rows {
                        let found = sqlite
                            .database
                            .query(
                                "benchmark",
                                "SELECT id, body FROM isam_catalog_bench WHERE id = ?1",
                                &[Value::from(id.clone())],
                            )
                            .unwrap();
                        assert_eq!(record_count(&found), usize::from(operation != 2));
                        if operation != 2 {
                            let actual = json_rows_from_result(&found);
                            assert_eq!(
                                (actual[0].id, actual[0].body),
                                (id.as_str(), body.as_str())
                            );
                        }
                    }
                    continue;
                }
                let catalog = if backend == 0 {
                    &mut individual.catalog
                } else {
                    &mut bulk.catalog
                };
                let before = catalog.operation_stats();
                let start = Instant::now();
                if backend == 0 {
                    for row in 0..CHAPTER_ROWS {
                        match operation {
                            0 => catalog.insert_row(CATALOG_TABLE, &values[row]).unwrap(),
                            1 => assert!(
                                catalog
                                    .update_row(CATALOG_TABLE, &keys[row], &values[row])
                                    .unwrap()
                            ),
                            2 => assert!(catalog.delete_row(CATALOG_TABLE, &keys[row]).unwrap()),
                            _ => unreachable!(),
                        }
                    }
                } else {
                    match operation {
                        0 => catalog.insert_rows(CATALOG_TABLE, &values).unwrap(),
                        1 => assert!(catalog.update_rows(CATALOG_TABLE, &updates).unwrap()),
                        2 => assert!(catalog.delete_rows(CATALOG_TABLE, &keys).unwrap()),
                        _ => unreachable!(),
                    }
                }
                results[index].durations.push(start.elapsed().as_nanos());
                let stats = stats_delta(before, catalog.operation_stats());
                assert_eq!(stats.root_writes, if backend == 0 { 36 } else { 1 });
                assert_eq!(stats.syncs, if backend == 0 { 72 } else { 2 });
                totals[index].add(stats);
                // Validate results outside the timed interval, including the
                // maintained index. A fast but partial commit must fail the run.
                for (row, (id, body)) in rows.iter().enumerate() {
                    let expected = if operation == 2 {
                        None
                    } else {
                        Some(values[row].clone())
                    };
                    assert_eq!(
                        catalog.get_row(CATALOG_TABLE, &catalog_key(id)).unwrap(),
                        expected
                    );
                    assert_eq!(
                        catalog
                            .lookup_index(
                                CATALOG_TABLE,
                                CATALOG_BODY_INDEX,
                                &[NativeValue::Text(body.clone())],
                                2
                            )
                            .unwrap(),
                        expected.into_iter().collect::<Vec<_>>()
                    );
                }
            }
        }
    }
    for (mut result, total) in results.into_iter().zip(totals) {
        if result.operations.is_some() {
            result.operations = Some(stats_from_total(total));
        }
        measurements.push(result);
    }
}

fn measure_sqlite(
    workload: &'static str,
    samples: usize,
    mut operation: impl FnMut(usize) -> Duration,
) -> Measurement {
    let durations = (0..samples)
        .map(|sample| operation(sample).as_nanos())
        .collect();
    Measurement {
        backend: "sqlite",
        workload,
        durations,
        operations: None,
    }
}

fn measure_flat_file(
    workload: &'static str,
    samples: usize,
    mut operation: impl FnMut(usize) -> Duration,
) -> Measurement {
    Measurement {
        backend: "flat_file",
        workload,
        durations: (0..samples)
            .map(|sample| operation(sample).as_nanos())
            .collect(),
        operations: None,
    }
}

fn percentiles(samples: &[u128]) -> (u128, u128, u128) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let nearest_rank =
        |percent: usize| sorted[((sorted.len() * percent).div_ceil(100)).saturating_sub(1)];
    (nearest_rank(50), nearest_rank(95), nearest_rank(99))
}

fn stats_from_total(total: OperationTotal) -> OperationStats {
    OperationStats {
        file_opens: total.file_opens.min(u64::MAX as u128) as u64,
        file_closes: total.file_closes.min(u64::MAX as u128) as u64,
        file_stats: total.file_stats.min(u64::MAX as u128) as u64,
        root_reads: total.root_reads.min(u64::MAX as u128) as u64,
        root_writes: total.root_writes.min(u64::MAX as u128) as u64,
        page_reads: total.page_reads.min(u64::MAX as u128) as u64,
        page_writes: total.page_writes.min(u64::MAX as u128) as u64,
        root_read_ns: total.root_read_ns.min(u64::MAX as u128) as u64,
        root_write_ns: total.root_write_ns.min(u64::MAX as u128) as u64,
        page_read_ns: total.page_read_ns.min(u64::MAX as u128) as u64,
        page_write_ns: total.page_write_ns.min(u64::MAX as u128) as u64,
        syncs: total.syncs.min(u64::MAX as u128) as u64,
        sync_requests: total.sync_requests.min(u64::MAX as u128) as u64,
        sync_ns: total.sync_ns.min(u64::MAX as u128) as u64,
        publication_ns: total.publication_ns.min(u64::MAX as u128) as u64,
        preflight_rebases: total.preflight_rebases.min(u64::MAX as u128) as u64,
        publication_retries: total.publication_retries.min(u64::MAX as u128) as u64,
        commit_lock_wait_ns: total.commit_lock_wait_ns.min(u64::MAX as u128) as u64,
        lock_requests: total.lock_requests.min(u64::MAX as u128) as u64,
        lock_retries: total.lock_retries.min(u64::MAX as u128) as u64,
        lock_wait_ns: total.lock_wait_ns.min(u64::MAX as u128) as u64,
        write_lock_batches: total.write_lock_batches.min(u64::MAX as u128) as u64,
        write_lock_keys: total.write_lock_keys.min(u64::MAX as u128) as u64,
        write_lock_requests: total.write_lock_requests.min(u64::MAX as u128) as u64,
        write_lock_retries: total.write_lock_retries.min(u64::MAX as u128) as u64,
        write_lock_wait_ns: total.write_lock_wait_ns.min(u64::MAX as u128) as u64,
        write_lock_local_retries: total.write_lock_local_retries.min(u64::MAX as u128) as u64,
        write_lock_local_wait_ns: total.write_lock_local_wait_ns.min(u64::MAX as u128) as u64,
        write_lock_range_retries: total.write_lock_range_retries.min(u64::MAX as u128) as u64,
        write_lock_range_wait_ns: total.write_lock_range_wait_ns.min(u64::MAX as u128) as u64,
        write_lock_stripes_acquired: total.write_lock_stripes_acquired.min(u64::MAX as u128) as u64,
        write_lock_stripe_acquisitions: std::array::from_fn(|stripe| {
            total.write_lock_stripe_acquisitions[stripe].min(u64::MAX as u128) as u64
        }),
        write_lock_stripe_retries: std::array::from_fn(|stripe| {
            total.write_lock_stripe_retries[stripe].min(u64::MAX as u128) as u64
        }),
        write_lock_stripe_wait_ns: std::array::from_fn(|stripe| {
            total.write_lock_stripe_wait_ns[stripe].min(u64::MAX as u128) as u64
        }),
    }
}

fn measurement_line(measurement: &Measurement) -> String {
    let mut sorted = measurement.durations.clone();
    sorted.sort_unstable();
    let count = sorted.len();
    let total: u128 = sorted.iter().sum();
    let mean = total / count as u128;
    let (p50, p95, p99) = percentiles(&sorted);
    let ops = measurement
        .operations
        .map(|stats| {
            let mut total = OperationTotal::default();
            total.add(stats);
            total.average(count)
        })
        .unwrap_or_else(|| vec!["NA"; 30].join("\t"));
    format!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        measurement.backend,
        measurement.workload,
        count,
        mean,
        sorted[0],
        sorted[count - 1],
        p50,
        p95,
        p99,
        (count as f64 * 1_000_000_000.0 / total as f64),
        ops,
    )
}

fn output_path() -> PathBuf {
    std::env::var_os("BRISKDB_ISAM_BENCH_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/isam-benchmark.tsv"))
}

fn directory_file_bytes(directory: &std::path::Path) -> u64 {
    fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("list benchmark directory {}: {error}", directory.display()))
        .map(|entry| {
            let entry = entry.unwrap_or_else(|error| {
                panic!(
                    "read benchmark directory entry {}: {error}",
                    directory.display()
                )
            });
            let entry_type = entry.file_type().unwrap_or_else(|error| {
                panic!(
                    "read benchmark entry type {}: {error}",
                    entry.path().display()
                )
            });
            if entry_type.is_file() {
                entry
                    .metadata()
                    .unwrap_or_else(|error| {
                        panic!(
                            "read benchmark file metadata {}: {error}",
                            entry.path().display()
                        )
                    })
                    .len()
            } else if entry_type.is_dir() {
                directory_file_bytes(&entry.path())
            } else {
                0
            }
        })
        .try_fold(0_u64, u64::checked_add)
        .expect("benchmark directory byte count overflow")
}

fn save_report(
    measurements: &[Measurement],
    peak_rss_bytes: u64,
    disk_growth_bytes: [i128; 5],
    path: &std::path::Path,
) {
    let mut report = format!(
        "# schema=isam-benchmark-v7\trun_revision={}\thost={}-{}\tpeak_rss_bytes={}\n",
        std::env::var("BRISKDB_ISAM_REVISION").unwrap_or_else(|_| "unspecified".into()),
        std::env::consts::OS,
        std::env::consts::ARCH,
        peak_rss_bytes
    );
    report.push_str(&format!(
        "# native_format\tversion={}\tcompression=none\n",
        if packed_pages() { 3 } else { 2 }
    ));
    report.push_str(&format!(
        "# disk_growth_bytes\tisam={}\tsqlite={}\tflat_file={}\tisam_catalog={}\tsqlite_catalog={}\n",
        disk_growth_bytes[0],
        disk_growth_bytes[1],
        disk_growth_bytes[2],
        disk_growth_bytes[3],
        disk_growth_bytes[4]
    ));
    report.push_str("# write_lock_stats\trequests count OS byte-range attempts; local_* measures process-local mutex contention; range_* measures byte-range-lock contention; stripe IDs are stable hash buckets\n");
    report.push_str("# durability\tISAM retains two File::sync_all calls per commit; SQLite retains BriskDB WAL/FULL defaults; macOS flush primitives differ, so these are default-policy comparisons, not identical power-loss guarantees\n");
    report.push_str("# group_workloads\t36 separate native commits vs one explicit native bulk commit vs one SQLite statement; not automatic group commit; verification excluded from timings; per-workload phase counters overlap; disk_growth_bytes excludes these separate group fixtures\n");
    let mut lock_totals = OperationTotal::default();
    for measurement in measurements {
        if let Some(stats) = measurement.operations {
            lock_totals.add(stats);
        }
    }
    for (label, values) in [
        (
            "write_lock_stripe_acquisitions",
            &lock_totals.write_lock_stripe_acquisitions,
        ),
        (
            "write_lock_stripe_retries",
            &lock_totals.write_lock_stripe_retries,
        ),
        (
            "write_lock_stripe_wait_ns",
            &lock_totals.write_lock_stripe_wait_ns,
        ),
    ] {
        let histogram = values
            .iter()
            .enumerate()
            .map(|(stripe, value)| format!("stripe_{stripe:02}={value}"))
            .collect::<Vec<_>>()
            .join("\t");
        report.push_str(&format!("# {label}\t{histogram}\n"));
    }
    report.push_str("backend\tworkload\tsamples\tmean_ns\tlow_ns\thigh_ns\tp50_ns\tp95_ns\tp99_ns\tthroughput_ops_s\tavg_file_opens\tavg_file_closes\tavg_file_stats\tavg_root_reads\tavg_root_writes\tavg_page_reads\tavg_page_writes\tavg_root_read_ns\tavg_root_write_ns\tavg_page_read_ns\tavg_page_write_ns\tavg_syncs\tavg_sync_ns\tavg_publication_ns\tavg_preflight_rebases\tavg_publication_retries\tavg_commit_lock_wait_ns\tavg_lock_requests\tavg_lock_retries\tavg_lock_wait_ns\tavg_write_lock_batches\tavg_write_lock_keys\tavg_write_lock_requests\tavg_write_lock_retries\tavg_write_lock_wait_ns\tavg_write_lock_local_retries\tavg_write_lock_local_wait_ns\tavg_write_lock_range_retries\tavg_write_lock_range_wait_ns\tavg_write_lock_stripes_acquired\n");
    for measurement in measurements {
        report.push_str(&measurement_line(measurement));
        report.push('\n');
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create benchmark report directory");
    }
    fs::write(path, &report)
        .unwrap_or_else(|error| panic!("write ISAM benchmark report {}: {error}", path.display()));
    print!("{report}");
}

fn sample_count() -> usize {
    std::env::var("BRISKDB_ISAM_BENCH_SAMPLES")
        .map(|value| value.parse().expect("parse benchmark sample count"))
        .unwrap_or(100)
}

fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the supplied rusage on success.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(result, 0, "read process resource usage");
    // SAFETY: successful getrusage initialized every field.
    let usage = unsafe { usage.assume_init() };
    let value = u64::try_from(usage.ru_maxrss).unwrap_or_default();
    #[cfg(target_os = "macos")]
    {
        value
    }
    #[cfg(not(target_os = "macos"))]
    {
        value.saturating_mul(1024)
    }
}

fn record_count(result: &briskdb::core::ResultSet) -> usize {
    result.rows().len()
}

fn json_rows_from_records(records: &[Record]) -> Vec<JsonBenchRow<'_>> {
    records
        .iter()
        .map(|record| JsonBenchRow {
            id: std::str::from_utf8(&record.key).expect("benchmark key is UTF-8"),
            body: std::str::from_utf8(&record.value).expect("benchmark body is UTF-8"),
        })
        .collect()
}

fn json_rows_from_result(result: &briskdb::core::ResultSet) -> Vec<JsonBenchRow<'_>> {
    result
        .rows()
        .iter()
        .map(|row| JsonBenchRow {
            id: row
                .get(0)
                .and_then(Value::as_str)
                .expect("SQLite benchmark id is text"),
            body: row
                .get(1)
                .and_then(Value::as_str)
                .expect("SQLite benchmark body is text"),
        })
        .collect()
}

fn expected_json_rows(chapter: u32) -> Vec<u8> {
    let offset = chapter - SEED_FIRST_CHAPTER;
    let rows: Vec<_> = (0..CHAPTER_ROWS)
        .map(|verse| OwnedJsonBenchRow {
            id: main_key(chapter, verse),
            body: format!("seed-{offset}-{verse}"),
        })
        .collect();
    serde_json::to_vec(&rows).unwrap()
}

#[test]
fn bounded_comparison_smoke() {
    let directory = tempfile::tempdir().unwrap();
    let report_path = directory.path().join("isam-benchmark.tsv");
    run_comparison(2, &report_path);
    let report = fs::read_to_string(report_path).unwrap();
    assert!(report.starts_with("# schema=isam-benchmark-v7\t"));
    assert!(report.contains("isam\trange_36\t"));
    assert!(report.contains("isam\tresult_json_serialization_36\t"));
    assert!(report.contains("sqlite\tresult_json_serialization_36\t"));
    assert!(report.contains("sqlite\tchunk_delete_36\t"));
    assert!(report.contains("isam_catalog\tcatalog_insert_1\t"));
    assert!(report.contains("sqlite\tcatalog_update_1\t"));
    assert!(report.contains("isam_catalog\tcatalog_delete_1\t"));
    assert!(report.contains("isam_catalog\tcatalog_update_36\t"));
    assert!(report.contains("isam_individual\tgroup_insert_36\t"));
    assert!(report.contains("isam_bulk\tgroup_update_36\t"));
    assert!(report.contains("sqlite\tgroup_delete_36\t"));
    assert!(report.contains("isam_writer_3\tdisjoint_writer_latency\t"));
    assert!(report.contains("# disk_growth_bytes\tisam="));
    assert!(report.contains("# write_lock_stripe_acquisitions\tstripe_00="));
    let mut lines = report.lines().filter(|line| !line.starts_with('#'));
    let column_count = lines.next().unwrap().split('\t').count();
    for line in lines {
        assert_eq!(line.split('\t').count(), column_count, "malformed TSV row");
    }
}

#[test]
#[ignore = "release-mode comparative ISAM/SQLite benchmark; see docs/BENCHMARKS.md"]
fn release_isam_sqlite_comparison() {
    run_comparison(sample_count(), &output_path());
}

fn run_comparison(samples: usize, report_path: &std::path::Path) {
    assert!(
        (2..=100).contains(&samples),
        "fixed fixtures support 2..=100 samples"
    );
    let peak_rss_start = peak_rss_bytes();
    let mut isam = IsamFixture::seeded();
    let sqlite = SqliteFixture::seeded();
    let mut isam_empty = IsamFixture::new_empty();
    let sqlite_empty = SqliteFixture::new_empty();
    let mut isam_catalog = NativeCatalogFixture::seeded();
    let sqlite_catalog = SqliteCatalogFixture::seeded();
    let flat = FlatFileFixture::seeded();
    let isam_bytes_before = directory_file_bytes(isam._directory.path())
        + directory_file_bytes(isam_empty._directory.path());
    let sqlite_bytes_before = directory_file_bytes(sqlite._directory.path())
        + directory_file_bytes(sqlite_empty._directory.path());
    let native_catalog_bytes_before = directory_file_bytes(isam_catalog._directory.path());
    let sqlite_catalog_bytes_before = directory_file_bytes(sqlite_catalog._directory.path());
    let flat_bytes_before = directory_file_bytes(flat._directory.path());
    let mut measurements = Vec::new();

    let isam_open = measure_isam("open_existing", samples, |_| {
        let start = Instant::now();
        let reopened = Store::open(&isam.path).unwrap();
        let stats_handle = reopened.operation_stats_handle();
        drop(reopened);
        let elapsed = start.elapsed();
        (elapsed, stats_handle.snapshot())
    });
    let sqlite_root = sqlite.root.clone();
    let sqlite_open = measure_sqlite("open_existing", samples, |_| {
        let start = Instant::now();
        let reopened = Database::open(&sqlite_root, 2).unwrap();
        black_box(&reopened);
        drop(reopened);
        start.elapsed()
    });
    measurements.extend([isam_open, sqlite_open]);

    let isam_point = measure_isam("point_read", samples, |_| {
        let before = isam.store.operation_stats();
        let start = Instant::now();
        let batch = isam.store.read_batch().unwrap();
        let value = batch
            .get(main_key(SEED_FIRST_CHAPTER, 0).as_bytes())
            .unwrap();
        assert!(value.is_some());
        black_box(value);
        let elapsed = start.elapsed();
        let stats = stats_delta(before, isam.store.operation_stats());
        (elapsed, stats)
    });
    let sqlite_point = measure_sqlite("point_read", samples, |_| {
        let start = Instant::now();
        let result = sqlite
            .database
            .query(
                "benchmark",
                SQLITE_READ,
                &[Value::from(main_key(SEED_FIRST_CHAPTER, 0))],
            )
            .unwrap();
        assert_eq!(record_count(&result), 1);
        black_box(result);
        start.elapsed()
    });
    measurements.extend([isam_point, sqlite_point]);

    let isam_range = measure_isam("range_36", samples, |sample| {
        let before = isam.store.operation_stats();
        let chapter = SEED_FIRST_CHAPTER + (sample as u32 % SEED_CHAPTERS);
        let start = Instant::now();
        let rows = isam
            .store
            .read_batch()
            .unwrap()
            .range(
                main_key(chapter, 0).as_bytes(),
                Some(chapter_end(chapter).as_bytes()),
                CHAPTER_ROWS,
            )
            .unwrap();
        assert_eq!(rows.len(), CHAPTER_ROWS);
        black_box(rows);
        let elapsed = start.elapsed();
        let stats = stats_delta(before, isam.store.operation_stats());
        (elapsed, stats)
    });
    let sqlite_range = measure_sqlite("range_36", samples, |sample| {
        let chapter = SEED_FIRST_CHAPTER + (sample as u32 % SEED_CHAPTERS);
        let start = Instant::now();
        let result = sqlite
            .database
            .query(
                "benchmark",
                SQLITE_RANGE,
                &[
                    Value::from(main_key(chapter, 0)),
                    Value::from(chapter_end(chapter)),
                ],
            )
            .unwrap();
        assert_eq!(record_count(&result), CHAPTER_ROWS);
        black_box(result);
        start.elapsed()
    });
    measurements.extend([isam_range, sqlite_range]);

    let isam_serialization = measure_isam("result_json_serialization_36", samples, |sample| {
        let chapter = SEED_FIRST_CHAPTER + (sample as u32 % SEED_CHAPTERS);
        let records = isam
            .store
            .read_batch()
            .unwrap()
            .range(
                main_key(chapter, 0).as_bytes(),
                Some(chapter_end(chapter).as_bytes()),
                CHAPTER_ROWS,
            )
            .unwrap();
        assert_eq!(records.len(), CHAPTER_ROWS);
        let rows = json_rows_from_records(&records);
        let expected = expected_json_rows(chapter);
        let start = Instant::now();
        let encoded = serde_json::to_vec(&rows).unwrap();
        let elapsed = start.elapsed();
        assert_eq!(encoded, expected);
        black_box(encoded);
        (elapsed, OperationStats::default())
    });
    let sqlite_serialization = measure_sqlite("result_json_serialization_36", samples, |sample| {
        let chapter = SEED_FIRST_CHAPTER + (sample as u32 % SEED_CHAPTERS);
        let result = sqlite
            .database
            .query(
                "benchmark",
                SQLITE_RANGE,
                &[
                    Value::from(main_key(chapter, 0)),
                    Value::from(chapter_end(chapter)),
                ],
            )
            .unwrap();
        assert_eq!(record_count(&result), CHAPTER_ROWS);
        let rows = json_rows_from_result(&result);
        let expected = expected_json_rows(chapter);
        let start = Instant::now();
        let encoded = serde_json::to_vec(&rows).unwrap();
        let elapsed = start.elapsed();
        assert_eq!(encoded, expected);
        black_box(encoded);
        elapsed
    });
    measurements.extend([isam_serialization, sqlite_serialization]);

    let flat_point = measure_flat_file("point_read", samples, |_| {
        let start = Instant::now();
        let record = flat.read_point(SEED_FIRST_CHAPTER, 0);
        assert_eq!(
            &record[..FLAT_KEY_BYTES],
            main_key(SEED_FIRST_CHAPTER, 0).as_bytes()
        );
        black_box(record);
        start.elapsed()
    });
    let flat_range = measure_flat_file("range_36", samples, |sample| {
        let chapter = SEED_FIRST_CHAPTER + (sample as u32 % SEED_CHAPTERS);
        let start = Instant::now();
        let rows = flat.read_chapter(chapter);
        assert_eq!(rows.len(), CHAPTER_ROWS);
        black_box(rows);
        start.elapsed()
    });
    measurements.extend([flat_point, flat_range]);

    let isam_insert = measure_isam("chunk_insert_36", samples, |sample| {
        let chapter = 1_000_000 + sample as u32;
        let mutations = seed_mutations(chapter, 1, "insert");
        let before = isam_empty.store.operation_stats();
        let start = Instant::now();
        isam_empty.store.write_batch(&mutations).unwrap();
        let elapsed = start.elapsed();
        let stats = stats_delta(before, isam_empty.store.operation_stats());
        (elapsed, stats)
    });
    let sqlite_insert = measure_sqlite("chunk_insert_36", samples, |sample| {
        let write = sql_upsert(1_000_000 + sample as u32, 1, "insert");
        let start = Instant::now();
        let result = sqlite_empty
            .database
            .execute("benchmark", &write.sql, &write.params)
            .unwrap();
        assert_eq!(result, CHAPTER_ROWS);
        start.elapsed()
    });
    measurements.extend([isam_insert, sqlite_insert]);

    let isam_refresh = measure_isam("chunk_refresh_36", samples, |sample| {
        let chapter = SEED_REFRESH_CHAPTER + (sample as u32 % (SEED_CHAPTERS - 1));
        let mutations = seed_mutations(chapter, 1, "refresh");
        let puts: Vec<_> = mutations
            .into_iter()
            .map(|mutation| match mutation {
                Mutation::Insert(record) => Mutation::put(record.key, record.value),
                other => other,
            })
            .collect();
        let before = isam.store.operation_stats();
        let start = Instant::now();
        isam.store.write_batch(&puts).unwrap();
        let elapsed = start.elapsed();
        let stats = stats_delta(before, isam.store.operation_stats());
        (elapsed, stats)
    });
    let sqlite_refresh = measure_sqlite("chunk_refresh_36", samples, |sample| {
        let chapter = SEED_REFRESH_CHAPTER + (sample as u32 % (SEED_CHAPTERS - 1));
        let write = sql_upsert(chapter, 1, "refresh");
        let start = Instant::now();
        let result = sqlite
            .database
            .execute("benchmark", &write.sql, &write.params)
            .unwrap();
        assert_eq!(result, CHAPTER_ROWS);
        start.elapsed()
    });
    measurements.extend([isam_refresh, sqlite_refresh]);
    let flat_refresh = measure_flat_file("chunk_refresh_36", samples, |sample| {
        let chapter = SEED_REFRESH_CHAPTER + (sample as u32 % (SEED_CHAPTERS - 1));
        let start = Instant::now();
        flat.refresh_chapter(chapter);
        start.elapsed()
    });
    measurements.push(flat_refresh);

    let isam_delete = measure_isam("chunk_delete_36", samples, |sample| {
        let chapter = 2_000_000 + sample as u32;
        isam.store
            .write_batch(&seed_mutations(chapter, 1, "delete"))
            .unwrap();
        let deletes: Vec<_> = (0..CHAPTER_ROWS)
            .map(|verse| Mutation::delete(main_key(chapter, verse).into_bytes()))
            .collect();
        let before = isam.store.operation_stats();
        let start = Instant::now();
        isam.store.write_batch(&deletes).unwrap();
        let elapsed = start.elapsed();
        let stats = stats_delta(before, isam.store.operation_stats());
        (elapsed, stats)
    });
    let sqlite_delete = measure_sqlite("chunk_delete_36", samples, |sample| {
        let chapter = 2_000_000 + sample as u32;
        let seed = sql_upsert(chapter, 1, "delete");
        sqlite
            .database
            .execute("benchmark", &seed.sql, &seed.params)
            .unwrap();
        let start = Instant::now();
        let result = sqlite
            .database
            .execute(
                "benchmark",
                "DELETE FROM isam_bench WHERE id >= ?1 AND id < ?2",
                &[
                    Value::from(main_key(chapter, 0)),
                    Value::from(chapter_end(chapter)),
                ],
            )
            .unwrap();
        assert_eq!(result, CHAPTER_ROWS);
        start.elapsed()
    });
    measurements.extend([isam_delete, sqlite_delete]);

    let isam_catalog_insert = measure_catalog(
        "catalog_insert_1",
        &mut isam_catalog.catalog,
        samples,
        |catalog, sample| {
            let id = catalog_id(10_000 + sample);
            let values = catalog_values(id, "inserted".to_owned());
            let start = Instant::now();
            catalog
                .insert_row(CATALOG_TABLE, &values)
                .expect("insert native catalog benchmark row");
            start.elapsed()
        },
    );
    let sqlite_catalog_insert = measure_sqlite("catalog_insert_1", samples, |sample| {
        let start = Instant::now();
        let result = sqlite_catalog
            .database
            .execute(
                "benchmark",
                "INSERT INTO isam_catalog_bench (id, body) VALUES (?1, ?2)",
                &[
                    Value::from(catalog_id(10_000 + sample)),
                    Value::from("inserted"),
                ],
            )
            .expect("insert SQLite catalog benchmark row");
        assert_eq!(result, 1);
        start.elapsed()
    });
    let isam_catalog_update = measure_catalog(
        "catalog_update_1",
        &mut isam_catalog.catalog,
        samples,
        |catalog, sample| {
            let id = catalog_id(sample % 100);
            let values = catalog_values(id.clone(), format!("updated-{sample}"));
            let key = catalog_key(&id);
            let start = Instant::now();
            assert!(
                catalog
                    .update_row(CATALOG_TABLE, &key, &values)
                    .expect("update native catalog benchmark row")
            );
            start.elapsed()
        },
    );
    let sqlite_catalog_update = measure_sqlite("catalog_update_1", samples, |sample| {
        let start = Instant::now();
        let result = sqlite_catalog
            .database
            .execute(
                "benchmark",
                "UPDATE isam_catalog_bench SET body = ?2 WHERE id = ?1",
                &[
                    Value::from(catalog_id(sample % 100)),
                    Value::from(format!("updated-{sample}")),
                ],
            )
            .expect("update SQLite catalog benchmark row");
        assert_eq!(result, 1);
        start.elapsed()
    });
    let isam_catalog_delete = measure_catalog(
        "catalog_delete_1",
        &mut isam_catalog.catalog,
        samples,
        |catalog, sample| {
            let id = catalog_id(sample % 100);
            let key = catalog_key(&id);
            let start = Instant::now();
            assert!(
                catalog
                    .delete_row(CATALOG_TABLE, &key)
                    .expect("delete native catalog benchmark row")
            );
            start.elapsed()
        },
    );
    let sqlite_catalog_delete = measure_sqlite("catalog_delete_1", samples, |sample| {
        let start = Instant::now();
        let result = sqlite_catalog
            .database
            .execute(
                "benchmark",
                "DELETE FROM isam_catalog_bench WHERE id = ?1",
                &[Value::from(catalog_id(sample % 100))],
            )
            .expect("delete SQLite catalog benchmark row");
        assert_eq!(result, 1);
        start.elapsed()
    });
    measurements.extend([
        isam_catalog_insert,
        sqlite_catalog_insert,
        isam_catalog_update,
        sqlite_catalog_update,
        isam_catalog_delete,
        sqlite_catalog_delete,
    ]);

    let isam_catalog_batch_insert = measure_catalog(
        "catalog_insert_36",
        &mut isam_catalog.catalog,
        samples,
        |catalog, sample| {
            let rows: Vec<_> = (0..CHAPTER_ROWS)
                .map(|offset| {
                    catalog_values(
                        catalog_id(100_000 + sample * CHAPTER_ROWS + offset),
                        format!("batch-insert-{sample}-{offset}"),
                    )
                })
                .collect();
            let start = Instant::now();
            catalog
                .insert_rows(CATALOG_TABLE, &rows)
                .expect("insert native catalog batch");
            start.elapsed()
        },
    );
    let sqlite_catalog_batch_insert = measure_sqlite("catalog_insert_36", samples, |sample| {
        let rows: Vec<_> = (0..CHAPTER_ROWS)
            .map(|offset| {
                (
                    catalog_id(100_000 + sample * CHAPTER_ROWS + offset),
                    format!("batch-insert-{sample}-{offset}"),
                )
            })
            .collect();
        let write = catalog_sql_upsert(&rows);
        let start = Instant::now();
        let result = sqlite_catalog
            .database
            .execute("benchmark", &write.sql, &write.params)
            .expect("insert SQLite catalog batch");
        assert_eq!(result, CHAPTER_ROWS);
        start.elapsed()
    });
    let isam_catalog_batch_update = measure_catalog(
        "catalog_update_36",
        &mut isam_catalog.catalog,
        samples,
        |catalog, sample| {
            let start_index = 100 + sample * CHAPTER_ROWS;
            let updates: Vec<_> = (0..CHAPTER_ROWS)
                .map(|offset| {
                    let id = catalog_id(start_index + offset);
                    (
                        catalog_key(&id).to_vec(),
                        catalog_values(id, format!("batch-updated-{sample}-{offset}")),
                    )
                })
                .collect();
            let start = Instant::now();
            assert!(
                catalog
                    .update_rows(CATALOG_TABLE, &updates)
                    .expect("update native catalog batch")
            );
            start.elapsed()
        },
    );
    let sqlite_catalog_batch_update = measure_sqlite("catalog_update_36", samples, |sample| {
        let start_index = 100 + sample * CHAPTER_ROWS;
        let rows: Vec<_> = (0..CHAPTER_ROWS)
            .map(|offset| {
                (
                    catalog_id(start_index + offset),
                    format!("batch-updated-{sample}-{offset}"),
                )
            })
            .collect();
        let write = catalog_sql_upsert(&rows);
        let start = Instant::now();
        let result = sqlite_catalog
            .database
            .execute("benchmark", &write.sql, &write.params)
            .expect("update SQLite catalog batch");
        assert_eq!(result, CHAPTER_ROWS);
        start.elapsed()
    });
    let isam_catalog_batch_delete = measure_catalog(
        "catalog_delete_36",
        &mut isam_catalog.catalog,
        samples,
        |catalog, sample| {
            let start_index = 100 + sample * CHAPTER_ROWS;
            let keys: Vec<_> = (0..CHAPTER_ROWS)
                .map(|offset| catalog_key(&catalog_id(start_index + offset)).to_vec())
                .collect();
            let start = Instant::now();
            assert!(
                catalog
                    .delete_rows(CATALOG_TABLE, &keys)
                    .expect("delete native catalog batch")
            );
            start.elapsed()
        },
    );
    let sqlite_catalog_batch_delete = measure_sqlite("catalog_delete_36", samples, |sample| {
        let start_index = 100 + sample * CHAPTER_ROWS;
        let ids: Vec<_> = (0..CHAPTER_ROWS)
            .map(|offset| catalog_id(start_index + offset))
            .collect();
        let write = catalog_sql_delete(&ids);
        let start = Instant::now();
        let result = sqlite_catalog
            .database
            .execute("benchmark", &write.sql, &write.params)
            .expect("delete SQLite catalog batch");
        assert_eq!(result, CHAPTER_ROWS);
        start.elapsed()
    });
    measurements.extend([
        isam_catalog_batch_insert,
        sqlite_catalog_batch_insert,
        isam_catalog_batch_update,
        sqlite_catalog_batch_update,
        isam_catalog_batch_delete,
        sqlite_catalog_batch_delete,
    ]);

    let conflict_key = main_key(SEED_FIRST_CHAPTER, 0);
    let isam_conflict = measure_isam("same_key_conflict", samples, |_| {
        let before = isam.store.operation_stats();
        let start = Instant::now();
        assert!(matches!(
            isam.store
                .write_batch(&[Mutation::insert(conflict_key.as_bytes(), b"duplicate")]),
            Err(briskdb::isam::Error::Duplicate)
        ));
        let elapsed = start.elapsed();
        (elapsed, stats_delta(before, isam.store.operation_stats()))
    });
    let sqlite_conflict = measure_sqlite("same_key_conflict", samples, |_| {
        let start = Instant::now();
        assert!(
            sqlite
                .database
                .execute(
                    "benchmark",
                    "INSERT INTO isam_bench (id, body) VALUES (?1, ?2)",
                    &[Value::from(conflict_key.clone()), Value::from("duplicate")],
                )
                .is_err()
        );
        start.elapsed()
    });
    measurements.extend([isam_conflict, sqlite_conflict]);

    let mut isam_writers: Vec<_> = (0..4)
        .map(|_| Store::open(&isam.path).expect("open retained ISAM writer"))
        .collect();
    let writer_lock_policy =
        LockPolicy::new(Duration::from_secs(30), Duration::from_millis(1)).unwrap();
    for writer in &mut isam_writers {
        writer.set_lock_policy(writer_lock_policy);
    }
    let isam_writer_ops: Vec<_> = (0..4)
        .map(|worker| {
            vec![Mutation::put(
                main_key(SEED_FIRST_CHAPTER + worker, 0).into_bytes(),
                b"multiwriter",
            )]
        })
        .collect();
    let shared_sqlite = Arc::new(sqlite.database);
    let mut isam_writer_durations = Vec::with_capacity(samples);
    let mut isam_writer_total = OperationTotal::default();
    let mut sqlite_writer_durations = Vec::with_capacity(samples);
    let mut isam_writer_latency: [Vec<u128>; 4] =
        std::array::from_fn(|_| Vec::with_capacity(samples));
    let mut sqlite_writer_latency: [Vec<u128>; 4] =
        std::array::from_fn(|_| Vec::with_capacity(samples));
    for _ in 0..samples {
        let before: Vec<_> = isam_writers.iter().map(Store::operation_stats).collect();
        let start = Instant::now();
        let barrier = Arc::new(Barrier::new(5));
        thread::scope(|scope| {
            let mut handles = Vec::with_capacity(4);
            for (worker, store) in isam_writers.iter_mut().enumerate() {
                let barrier = Arc::clone(&barrier);
                let operations = &isam_writer_ops[worker];
                handles.push(scope.spawn(move || {
                    barrier.wait();
                    let start = Instant::now();
                    store.write_batch(operations).unwrap();
                    start.elapsed().as_nanos()
                }));
            }
            barrier.wait();
            for (worker, handle) in handles.into_iter().enumerate() {
                isam_writer_latency[worker].push(handle.join().unwrap());
            }
        });
        isam_writer_durations.push(start.elapsed().as_nanos());
        let after: Vec<_> = isam_writers.iter().map(Store::operation_stats).collect();
        for (before, after) in before.into_iter().zip(after) {
            isam_writer_total.add(stats_delta(before, after));
        }

        let start = Instant::now();
        let barrier = Arc::new(Barrier::new(5));
        thread::scope(|scope| {
            let mut handles = Vec::with_capacity(4);
            for worker in 0..4 {
                let barrier = Arc::clone(&barrier);
                let database = Arc::clone(&shared_sqlite);
                let key = main_key(SEED_FIRST_CHAPTER + worker, 0);
                handles.push(scope.spawn(move || {
                    barrier.wait();
                    let start = Instant::now();
                    database
                        .execute(
                            "benchmark",
                            "UPDATE isam_bench SET body = ?2 WHERE id = ?1",
                            &[Value::from(key.clone()), Value::from("multiwriter")],
                        )
                        .unwrap();
                    start.elapsed().as_nanos()
                }));
            }
            barrier.wait();
            for (worker, handle) in handles.into_iter().enumerate() {
                sqlite_writer_latency[worker].push(handle.join().unwrap());
            }
        });
        sqlite_writer_durations.push(start.elapsed().as_nanos());
    }
    measurements.push(Measurement {
        backend: "isam",
        workload: "disjoint_writers_4",
        durations: isam_writer_durations,
        operations: Some(stats_from_total(isam_writer_total)),
    });
    measurements.push(Measurement {
        backend: "sqlite",
        workload: "disjoint_writers_4",
        durations: sqlite_writer_durations,
        operations: None,
    });
    for worker in 0..4 {
        measurements.push(Measurement {
            backend: [
                "isam_writer_0",
                "isam_writer_1",
                "isam_writer_2",
                "isam_writer_3",
            ][worker],
            workload: "disjoint_writer_latency",
            durations: std::mem::take(&mut isam_writer_latency[worker]),
            operations: None,
        });
        measurements.push(Measurement {
            backend: [
                "sqlite_writer_0",
                "sqlite_writer_1",
                "sqlite_writer_2",
                "sqlite_writer_3",
            ][worker],
            workload: "disjoint_writer_latency",
            durations: std::mem::take(&mut sqlite_writer_latency[worker]),
            operations: None,
        });
    }
    let rss = peak_rss_bytes().max(peak_rss_start);
    let disk_growth = [
        i128::from(
            directory_file_bytes(isam._directory.path())
                + directory_file_bytes(isam_empty._directory.path()),
        ) - i128::from(isam_bytes_before),
        i128::from(
            directory_file_bytes(sqlite._directory.path())
                + directory_file_bytes(sqlite_empty._directory.path()),
        ) - i128::from(sqlite_bytes_before),
        i128::from(directory_file_bytes(flat._directory.path())) - i128::from(flat_bytes_before),
        i128::from(directory_file_bytes(isam_catalog._directory.path()))
            - i128::from(native_catalog_bytes_before),
        i128::from(directory_file_bytes(sqlite_catalog._directory.path()))
            - i128::from(sqlite_catalog_bytes_before),
    ];
    measure_catalog_batching(samples, &mut measurements);
    save_report(
        &measurements,
        rss.max(peak_rss_bytes()),
        disk_growth,
        report_path,
    );
}
