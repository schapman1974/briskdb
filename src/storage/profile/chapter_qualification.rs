//! Disposable SQL-engine benchmark only. Never a public EFS enablement switch.
//! Steady requests use Engine, catalog planning, pooled SQLite shards, and
//! actual result rows. Only fixture loading uses direct shard transactions.

use crate::storage::Storage;
use crate::{
    ContentionJitter, ContentionPolicy, EngineOptions, MetadataBackend, Statement, StorageProfile,
    Value,
    core::{
        Database, Engine, RawDataOperation, RawDataTarget, Session, ShardKeyMetadata, ShardKeyType,
        TableDeclaration,
    },
};
use serde_json::{Value as Json, json};
use std::{
    collections::HashMap,
    fs,
    io::{BufRead, Write},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const SHARDS: u16 = 4;
const ROUTE: &str = "web:JHN:3";
const SELECT: &str =
    "SELECT book_id, book, chapter, verse, text FROM verses WHERE chapter_key=?1 ORDER BY verse";

fn storage(root: &Path, backend: MetadataBackend) -> Result<Storage> {
    Ok(controlled_storage(root, backend, None)?)
}

fn controlled_storage(
    root: &Path,
    backend: MetadataBackend,
    control: Option<&Arc<crate::core::OperationControl>>,
) -> crate::core::EngineResult<Storage> {
    match backend {
        MetadataBackend::Sqlite => {
            Storage::open_with_profile_control(root, SHARDS, None, control, StorageProfile::Nfs)
        }
        MetadataBackend::Isam => Storage::open_native_metadata_qualification(root, SHARDS, control),
    }
}

struct Handle {
    engine: Engine,
    session: Session,
    write_session: Session,
    root: std::path::PathBuf,
    backend: MetadataBackend,
}

impl Handle {
    async fn open(root: &Path, backend: MetadataBackend) -> Result<Self> {
        // Both backends use the same finite lock budget. This changes only
        // lock/admission waiting, never the number of SQL executions.
        let wait_ms: u64 = std::env::var("BRISKDB_EFS_LOCK_WAIT_MS")
            .unwrap_or_else(|_| "60000".into())
            .parse()?;
        if !(1_000..=120_000).contains(&wait_ms) {
            return Err("benchmark lock budget must be 1000..=120000 milliseconds".into());
        }
        let policy = ContentionPolicy::new(
            Duration::from_millis(10),
            Duration::from_millis(250),
            2,
            ContentionJitter::Full,
            100_000,
            Duration::from_millis(wait_ms),
        )?;
        let options = EngineOptions::default()
            .with_metadata_backend(backend)
            .with_contention_policy(Some(policy))
            .with_request_timeout(Some(Duration::from_millis(wait_ms + 30_000)))?;
        // Match Engine::open_with_options: startup gets its own lock budget,
        // not a Python loop replaying a failed open or application statement.
        let startup = crate::core::OperationControl::with_contention_policy(
            Some(Instant::now() + Duration::from_millis(wait_ms + 30_000)),
            Some(policy),
        );
        let database =
            crate::storage::contention::with_control(Some(Arc::clone(&startup)), || {
                Database::from_test_storage(controlled_storage(root, backend, Some(&startup))?)
            })?;
        let database = Arc::new(database);
        let engine = Engine::from_database_with_options(database, options)?;
        let session = engine.session();
        let write_session = engine.session();
        session.set_routing_key(ROUTE).await?;
        Ok(Self {
            engine,
            session,
            write_session,
            root: root.into(),
            backend,
        })
    }

    async fn read(&self) -> Result<Json> {
        let result = self
            .engine
            .query(
                &self.session,
                Statement::new(SELECT, vec![Value::from(ROUTE)]),
            )
            .await?;
        let verses = result
            .value
            .rows()
            .iter()
            .map(|row| {
                Ok(json!({
                    "book_id": row.get(0).and_then(Value::as_str).ok_or("invalid book_id")?,
                    "book": row.get(1).and_then(Value::as_str).ok_or("invalid book")?,
                    "chapter": row.get(2).and_then(Value::as_i64).ok_or("invalid chapter")?,
                    "verse": row.get(3).and_then(Value::as_i64).ok_or("invalid verse")?,
                    "text": row.get(4).and_then(Value::as_str).ok_or("invalid text")?,
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        if verses.len() != 36 {
            return Err("expected all 36 verses of John 3".into());
        }
        Ok(json!({"ok":true, "verses":verses, "shard":result.shard}))
    }

    async fn write(&self, verse: &Json) -> Result<Json> {
        if verse["book_id"] != "JHN" || verse["chapter"] != 3 || verse["verse"] != 1 {
            return Err("only disposable John 3:1 updates are allowed".into());
        }
        let result = self
            .engine
            .execute(
                &self.session,
                Statement::new(
                    "UPDATE verses SET text=?1 WHERE chapter_key=?2 AND verse=1",
                    vec![
                        Value::from(verse["text"].as_str().ok_or("missing text")?),
                        Value::from(ROUTE),
                    ],
                ),
            )
            .await?;
        if result.value != 1 {
            return Err("expected exactly one updated verse".into());
        }
        Ok(json!({"ok":true, "verse":verse, "shard":result.shard}))
    }

    async fn write_record(&self, operation: &str, record: &Json) -> Result<Json> {
        let id = record["id"].as_str().ok_or("missing record id")?;
        let value = record["value"].as_str().ok_or("missing record value")?;
        let prefix = match operation {
            "insert" => "new-",
            "update" => "existing-",
            _ => return Err("unsupported record operation".into()),
        };
        let number = id.strip_prefix(prefix).ok_or("invalid record namespace")?;
        if number.len() != 4 || !number.bytes().all(|b| b.is_ascii_digit()) || value.len() > 512 {
            return Err("invalid disposable record".into());
        }
        let sql = if operation == "insert" {
            "INSERT INTO activity (id,value) VALUES (?1,?2)"
        } else {
            "UPDATE activity SET value=?2 WHERE id=?1"
        };
        // No verse/chapter routing key and no benchmark-selected shard.
        // The normal SQL planner infers the independent record ID's route.
        let before = self.engine.contention_statistics();
        let started = Instant::now();
        let result = self
            .engine
            .execute(
                &self.write_session,
                Statement::new(sql, vec![Value::from(id), Value::from(value)]),
            )
            .await;
        let after = self.engine.contention_statistics();
        let retries = after
            .retries_scheduled()
            .saturating_sub(before.retries_scheduled());
        let exhausted = after
            .exhausted_budgets()
            .saturating_sub(before.exhausted_budgets());
        let wait_ns = after.wait_nanos().saturating_sub(before.wait_nanos());
        // One request at a time per native worker: these are per-write deltas.
        // Never log SQL text, record IDs, values, or authentication material.
        eprintln!(
            "BRISKDB_WRITE {}",
            json!({
                "operation":operation, "backend":self.backend.as_str(),
                "ok":result.is_ok(), "shard":result.as_ref().ok().map(|r| r.shard),
                "error_kind":result.as_ref().err().map(|e| format!("{:?}", e.kind())),
                "retries":retries, "exhausted_budgets":exhausted,
                "wait_ms":wait_ns as f64 / 1_000_000.0,
                "elapsed_ms":started.elapsed().as_secs_f64() * 1000.0,
            })
        );
        let result = result?;
        if result.value != 1 {
            return Err("expected exactly one affected activity record".into());
        }
        Ok(json!({"ok":true,"record":record,"shard":result.shard,
                  "lock_retries":retries,"lock_wait_ms":wait_ns as f64 / 1_000_000.0}))
    }

    fn verify(&self) -> Result<Json> {
        let storage = storage(&self.root, self.backend)?;
        let mut count = 0;
        let mut activity = Vec::new();
        let mut activity_counts = Vec::new();
        for shard in 0..SHARDS {
            let connection = storage.open_shard(shard)?;
            let integrity: String =
                connection.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
            if integrity != "ok" {
                return Err(integrity.into());
            }
            count += usize::try_from(connection.query_row(
                "SELECT count(*) FROM verses",
                [],
                |r| r.get::<_, i64>(0),
            )?)?;
            let mut statement = connection.prepare("SELECT id,value FROM activity ORDER BY id")?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            let before = activity.len();
            for row in rows {
                let (id, value) = row?;
                activity.push(json!({"id":id,"value":value,"shard":shard}));
            }
            activity_counts.push(activity.len() - before);
            let mode: String = connection.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
            let sync: i64 = connection.pragma_query_value(None, "synchronous", |r| r.get(0))?;
            if mode != "persist" || sync != 3 {
                return Err("rollback durability policy changed".into());
            }
            for suffix in ["-wal", "-shm"] {
                if self
                    .root
                    .join(format!("shards/{shard:04}.sqlite{suffix}"))
                    .exists()
                {
                    return Err("WAL files are forbidden in this benchmark".into());
                }
            }
        }
        if self.root.join("manifest.sqlite").exists() != (self.backend == MetadataBackend::Sqlite) {
            return Err("wrong metadata backend".into());
        }
        Ok(json!({"records":count,"activity_records":activity,"activity_counts":activity_counts}))
    }
}

fn seed(root: &Path, backend: MetadataBackend, fixture: &[Json]) -> Result<()> {
    let started = Instant::now();
    if root.exists() {
        return Err("benchmark will not overwrite an existing root".into());
    }
    let mut db = Database::from_test_storage(storage(root, backend)?)?;
    eprintln!(
        "chapter seed {backend:?}: created storage {:?}",
        started.elapsed()
    );
    db.broadcast("CREATE TABLE verses(chapter_key TEXT NOT NULL, book_id TEXT NOT NULL, book TEXT NOT NULL, chapter INTEGER NOT NULL, verse INTEGER NOT NULL, text TEXT NOT NULL, PRIMARY KEY(chapter_key,verse)); CREATE TABLE activity(id TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL)")?;
    db.register_tables(vec![
        TableDeclaration::sharded(
            db.catalog().default_database().id(),
            "verses",
            ShardKeyMetadata::new("chapter_key", ShardKeyType::Text)?,
        )?,
        TableDeclaration::sharded(
            db.catalog().default_database().id(),
            "activity",
            ShardKeyMetadata::new("id", ShardKeyType::Text)?,
        )?,
    ])?;
    eprintln!(
        "chapter seed {backend:?}: registered table {:?}",
        started.elapsed()
    );
    drop(db);
    let storage = storage(root, backend)?;
    let db = Database::from_test_storage(storage.clone())?;
    let mut routes = HashMap::new();
    let mut existing_records = Vec::new();
    for number in 0..20 {
        let id = format!("existing-{number:04}");
        let plan = db
            .raw_data_plan(
                None,
                "SELECT value FROM activity WHERE id=?1",
                &[Value::from(id.as_str())],
                RawDataOperation::Query,
            )?
            .ok_or("missing activity route")?;
        let RawDataTarget::Exact(route) = plan.target else {
            return Err("activity requires an exact route".into());
        };
        existing_records.push((id, route));
    }
    for row in fixture {
        let key = format!(
            "web:{}:{}",
            row[3].as_str().ok_or("invalid book")?,
            row[5].as_i64().ok_or("invalid chapter")?
        );
        if let std::collections::hash_map::Entry::Vacant(entry) = routes.entry(key) {
            let key = entry.key();
            // Use BriskDB's authoritative planner without executing an empty
            // EFS query for every chapter during untimed fixture preparation.
            let plan = db
                .raw_data_plan(
                    Some(key),
                    SELECT,
                    &[Value::from(key.as_str())],
                    RawDataOperation::Query,
                )?
                .ok_or("missing catalog route")?;
            let RawDataTarget::Exact(route) = plan.target else {
                return Err("fixture requires an exact shard route".into());
            };
            entry.insert(route);
        }
    }
    eprintln!(
        "chapter seed {backend:?}: planned routes {:?}",
        started.elapsed()
    );
    // Untimed loading only. Timed reads and writes always enter Engine above.
    for shard in 0..SHARDS {
        let mut connection = storage.open_shard(shard)?;
        let transaction = connection.transaction()?;
        {
            for (id, route) in &existing_records {
                if *route == shard {
                    transaction.execute(
                        "INSERT INTO activity VALUES (?1,?2)",
                        rusqlite::params![id, format!("seeded-{id}")],
                    )?;
                }
            }
            let mut statement =
                transaction.prepare("INSERT INTO verses VALUES (?1,?2,?3,?4,?5,?6)")?;
            for row in fixture {
                let key = format!(
                    "web:{}:{}",
                    row[3].as_str().ok_or("invalid book")?,
                    row[5].as_i64().ok_or("invalid chapter")?
                );
                if routes[&key] == shard {
                    statement.execute(rusqlite::params![
                        key,
                        row[3].as_str(),
                        row[4].as_str(),
                        row[5].as_i64(),
                        row[6].as_i64(),
                        row[7].as_str()
                    ])?;
                }
            }
        }
        transaction.commit()?;
        eprintln!(
            "chapter seed {backend:?}: loaded shard {shard} {:?}",
            started.elapsed()
        );
    }
    Ok(())
}

fn target(event: &Json) -> Result<(std::path::PathBuf, MetadataBackend)> {
    let run = event["run"].as_str().ok_or("missing run")?;
    if run.len() != 32
        || !run
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err("invalid run".into());
    }
    let backend: MetadataBackend = event["engine"].as_str().ok_or("missing engine")?.parse()?;
    let parent = std::path::PathBuf::from(format!("/mnt/brp/briskdb-engine-chapters-{run}"));
    Ok((parent.join(backend.as_str()), backend))
}

fn validate_target(root: &Path) -> Result<()> {
    let parent = root.parent().ok_or("missing benchmark parent")?;
    let mount = fs::read_to_string("/proc/mounts")?;
    let line = mount
        .lines()
        .find(|l| l.split_whitespace().nth(1) == Some("/mnt/brp"))
        .ok_or("EFS mount missing")?;
    if !line.contains(" nfs4 ")
        || !line.contains("vers=4.1")
        || !line.contains("local_lock=none")
        || !line.contains("hard")
    {
        return Err("NFSv4.1 hard mount with remote locking required".into());
    }
    if parent.exists() && fs::symlink_metadata(parent)?.file_type().is_symlink() {
        return Err("symlink benchmark parent".into());
    }
    if root.exists() && fs::symlink_metadata(root)?.file_type().is_symlink() {
        return Err("symlink benchmark root".into());
    }
    Ok(())
}

#[test]
#[ignore = "explicit disposable EFS chapter benchmark; reads JSON requests from stdin"]
fn serve() -> Result<()> {
    if std::env::var("BRISKDB_EFS_CHAPTER_ACK").as_deref() != Ok("disposable-no-production-data") {
        return Err("explicit disposable benchmark acknowledgement required".into());
    }
    #[cfg(feature = "server-cli")]
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| format!("benchmark logging initialization failed: {error}"))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let mut handle: Option<Handle> = None;
    for line in std::io::stdin().lock().lines() {
        let answer = (|| -> Result<Json> {
            let event: Json = serde_json::from_str(&line?)?;
            let started = Instant::now();
            let action = event["action"].as_str().ok_or("missing action")?;
            if action == "close" {
                handle = None;
                return Ok(json!({"ok":true}));
            }
            let (root, backend) = target(&event)?;
            let mut open_ms = 0.0;
            if action == "init" {
                handle = None;
                validate_target(&root)?;
                let fixture: Vec<Json> = serde_json::from_slice(&fs::read("/opt/verses.json")?)?;
                seed(&root, backend, &fixture)?;
            }
            let reused = handle
                .as_ref()
                .is_some_and(|h| h.root == root && h.backend == backend);
            if !reused {
                handle = None;
                validate_target(&root)?;
                let opening = Instant::now();
                handle = Some(runtime.block_on(Handle::open(&root, backend))?);
                open_ms = opening.elapsed().as_secs_f64() * 1000.0;
            }
            let handle = handle.as_ref().unwrap();
            let mut answer = match action {
                "init" | "read" | "warm" | "verify" => runtime.block_on(handle.read())?,
                "write" => runtime.block_on(handle.write(&event["verse"]))?,
                "insert" | "update" => {
                    runtime.block_on(handle.write_record(action, &event["record"]))?
                }
                _ => return Err("unknown action".into()),
            };
            if matches!(action, "init" | "verify") {
                let verification = handle.verify()?;
                answer["records"] = verification["records"].clone();
                answer["activity_records"] = verification["activity_records"].clone();
                answer["activity_counts"] = verification["activity_counts"].clone();
            }
            answer["engine_ms"] = json!(started.elapsed().as_secs_f64() * 1000.0);
            answer["open_ms"] = json!(open_ms);
            answer["reused"] = json!(reused);
            answer["lock_budget_ms"] = json!(
                handle
                    .engine
                    .options()
                    .contention_policy()
                    .unwrap()
                    .max_elapsed()
                    .as_millis()
            );
            answer["request_timeout_ms"] = json!(
                handle
                    .engine
                    .options()
                    .request_timeout()
                    .unwrap()
                    .as_millis()
            );
            Ok(answer)
        })();
        let answer = answer.unwrap_or_else(|e| json!({"ok":false,"error":e.to_string()}));
        println!("BRISKDB_CHAPTER_JSON {}", serde_json::to_string(&answer)?);
        std::io::stdout().flush()?;
    }
    Ok(())
}

#[test]
fn matched_backends_use_real_engine_and_preserve_rollback_profile() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    for backend in [MetadataBackend::Sqlite, MetadataBackend::Isam] {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("chapters");
        let fixture: Vec<_> = (1..=36)
            .map(|v| json!([v, 1, 43, "JHN", "John", 3, v, format!("verse {v}")]))
            .collect();
        seed(&root, backend, &fixture)?;
        let handle = runtime.block_on(Handle::open(&root, backend))?;
        let answer = runtime.block_on(handle.read())?;
        assert_eq!(answer["verses"].as_array().unwrap().len(), 36);
        let mut verse = answer["verses"][0].clone();
        verse["text"] = json!("updated verse");
        runtime.block_on(handle.write(&verse))?;
        assert_eq!(runtime.block_on(handle.read())?["verses"][0], verse);
        assert_eq!(handle.verify()?["records"], 36);
        let mut counts = [0; SHARDS as usize];
        for number in 0..80 {
            let record =
                json!({"id":format!("new-{number:04}"),"value":format!("inserted-{number}")});
            let result = runtime.block_on(handle.write_record("insert", &record))?;
            counts[result["shard"].as_u64().unwrap() as usize] += 1;
        }
        assert!(counts.iter().all(|count| *count > 0));
        for number in 0..20 {
            let record =
                json!({"id":format!("existing-{number:04}"),"value":format!("updated-{number}")});
            runtime.block_on(handle.write_record("update", &record))?;
        }
        assert!(
            runtime
                .block_on(
                    handle.write_record("insert", &json!({"id":"new-0000","value":"duplicate"}))
                )
                .is_err()
        );
        drop(handle);
        let handle = runtime.block_on(Handle::open(&root, backend))?;
        assert_eq!(runtime.block_on(handle.read())?["verses"][0], verse);
        let verified = handle.verify()?;
        let records = verified["activity_records"].as_array().unwrap();
        assert_eq!(records.len(), 100);
        for record in records {
            let id = record["id"].as_str().unwrap();
            let (prefix, number) = id.split_once('-').unwrap();
            let number: usize = number.parse()?;
            assert_eq!(
                record["value"],
                if prefix == "new" {
                    format!("inserted-{number}")
                } else {
                    format!("updated-{number}")
                }
            );
        }
        drop(handle);
        assert!(Database::open_with_metadata_backend(&root, SHARDS, backend).is_err());
    }
    Ok(())
}

#[test]
fn both_metadata_backends_wait_beyond_five_seconds_and_insert_once() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    for backend in [MetadataBackend::Sqlite, MetadataBackend::Isam] {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("chapters");
        let fixture: Vec<_> = (1..=36)
            .map(|v| json!([v, 1, 43, "JHN", "John", 3, v, format!("verse {v}")]))
            .collect();
        seed(&root, backend, &fixture)?;
        let handle = runtime.block_on(Handle::open(&root, backend))?;
        let mut locks = Vec::new();
        for shard in 0..SHARDS {
            let lock = rusqlite::Connection::open(root.join(format!("shards/{shard:04}.sqlite")))?;
            lock.execute_batch("BEGIN IMMEDIATE")?;
            locks.push(lock);
        }
        let started = Instant::now();
        runtime.block_on(async {
            let record = json!({"id":"new-0000","value":"once"});
            let write = handle.write_record("insert", &record);
            let release = async {
                tokio::time::sleep(Duration::from_secs(6)).await;
                for lock in locks {
                    lock.execute_batch("ROLLBACK")?;
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            };
            // Keep both futures alive until rollback, even when a regression
            // makes the write fail early at the legacy five-second boundary.
            let (written, released) = tokio::join!(write, release);
            released?;
            let written = written?;
            assert!(written["lock_retries"].as_u64().unwrap() > 0);
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })?;
        assert!(started.elapsed() >= Duration::from_secs(6));
        assert!(started.elapsed() < Duration::from_secs(20));
        let verified = handle.verify()?;
        let records = verified["activity_records"].as_array().unwrap();
        assert_eq!(records.len(), 21);
        assert_eq!(
            records
                .iter()
                .filter(|r| r["id"] == "new-0000" && r["value"] == "once")
                .count(),
            1
        );
        // A uniqueness failure is not a lock retry and cannot overwrite data.
        let before = handle.engine.contention_statistics();
        assert!(
            runtime
                .block_on(
                    handle.write_record("insert", &json!({"id":"new-0000","value":"duplicate"}))
                )
                .is_err()
        );
        assert_eq!(handle.engine.contention_statistics(), before);
    }
    Ok(())
}
