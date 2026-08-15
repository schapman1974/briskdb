#![cfg(all(unix, feature = "embedded"))]

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

#[cfg(feature = "experimental-vtab")]
use briskdb::core::GeneratedIdPolicy;
use briskdb::{
    BriskDb, EngineError, EngineErrorKind, EngineOptions, Statement, Value,
    core::{
        Database, GlobalIndexDeclaration, GlobalIndexKeyPart, GlobalIndexKeySource,
        GlobalIndexKeyType, GlobalIndexStorageTopology, ShardKeyMetadata, ShardKeyType,
        TableDeclaration,
    },
};
#[cfg(feature = "experimental-vtab")]
use std::collections::BTreeSet;
#[cfg(feature = "server-cli")]
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
};

const SHARDS: u16 = 4;
const WAIT_TIMEOUT: Duration = Duration::from_secs(15);
const WRITES_PER_PROCESS: i64 = 40;

struct ChildGuard(Child);

impl ChildGuard {
    fn wait_success(&mut self) {
        let status = self.0.wait().expect("wait for child process");
        assert!(status.success(), "child process failed with {status}");
    }

    fn kill_and_wait(&mut self) {
        self.0.kill().expect("kill child process");
        let status = self.0.wait().expect("reap killed child process");
        assert!(!status.success(), "killed child unexpectedly succeeded");
    }

    #[cfg(feature = "server-cli")]
    fn terminate_and_wait(&mut self) {
        let result = unsafe { libc::kill(self.0.id() as libc::pid_t, libc::SIGTERM) };
        assert_eq!(result, 0, "send SIGTERM to service process");
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            if let Some(status) = self.0.try_wait().expect("poll service process") {
                assert!(status.success(), "service exited unsuccessfully: {status}");
                return;
            }
            assert!(
                Instant::now() < deadline,
                "service did not stop after SIGTERM"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn wait_for_paths(paths: &[PathBuf]) {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    while paths.iter().any(|path| !path.exists()) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert!(
        paths.iter().all(|path| path.exists()),
        "timed out waiting for subprocess barrier files: {paths:?}"
    );
}

fn wait_for_path(path: &Path) {
    wait_for_paths(&[path.to_path_buf()]);
}

fn spawn_test_child(
    root: &Path,
    mode: &str,
    worker: usize,
    route: Option<&str>,
    ready: &Path,
    go: &Path,
    output: &Path,
) -> ChildGuard {
    let mut command = Command::new(env::current_exe().expect("locate integration test binary"));
    command
        .arg("--exact")
        .arg("shared_root_process_child")
        .arg("--nocapture")
        .env("BRISKDB_PROCESS_TEST_ROOT", root)
        .env("BRISKDB_PROCESS_TEST_MODE", mode)
        .env("BRISKDB_PROCESS_TEST_WORKER", worker.to_string())
        .env("BRISKDB_PROCESS_TEST_READY", ready)
        .env("BRISKDB_PROCESS_TEST_GO", go)
        .env("BRISKDB_PROCESS_TEST_OUTPUT", output)
        .stdout(Stdio::null());
    if let Some(route) = route {
        command.env("BRISKDB_PROCESS_TEST_ROUTE", route);
    }
    ChildGuard(command.spawn().expect("spawn integration-test child"))
}

async fn retry_write<F, Fut>(mut operation: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), EngineError>>,
{
    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        match operation().await {
            Ok(()) => return,
            Err(error) if error.is_retryable() && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Err(error) => panic!(
                "shared-root write failed: {} ({})",
                error.code(),
                error.diagnostic()
            ),
        }
    }
}

async fn run_explicit_writer(
    root: &Path,
    worker: usize,
    route: &str,
    ready: &Path,
    go: &Path,
    output: &Path,
    crash: bool,
) {
    let options = EngineOptions::new(2, 32).expect("valid child pool limits");
    let database = BriskDb::builder(root)
        .with_shard_count(SHARDS)
        .with_engine_options(options)
        .open()
        .await
        .expect("open child embedded database");
    let session = database.session();
    session
        .set_routing_key(route)
        .await
        .expect("set child routing key");
    fs::write(ready, b"ready").expect("publish child readiness");
    wait_for_path(go);

    for ordinal in 0..WRITES_PER_PROCESS {
        let id = i64::try_from(worker).expect("worker fits i64") * 10_000 + ordinal;
        retry_write(|| async {
            database
                .execute_write(
                    &session,
                    Statement::new(
                        "INSERT INTO events (tenant_id, id, payload) VALUES (?1, ?2, ?3)",
                        vec![
                            Value::from(route),
                            Value::from(id),
                            Value::from(format!("worker-{worker}-{ordinal}")),
                        ],
                    ),
                )
                .await
                .map(|_| ())
        })
        .await;
        if ordinal == WRITES_PER_PROCESS / 2 {
            database
                .checkpoint()
                .await
                .expect("checkpoint while peer writers are active");
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    fs::write(output, b"committed").expect("publish committed work");
    if crash {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    session.close().await.expect("close child session");
    database.close().await.expect("close child database");
}

#[cfg(feature = "experimental-vtab")]
async fn run_hilo_writer(root: &Path, worker: usize, ready: &Path, go: &Path, output: &Path) {
    let options = EngineOptions::new(2, 32)
        .expect("valid child pool limits")
        .with_experimental_vtab_writes(true);
    let database = BriskDb::builder(root)
        .with_shard_count(SHARDS)
        .with_engine_options(options)
        .open()
        .await
        .expect("open generated-ID child database");
    let session = database.session();
    let mut ids = Vec::new();
    fs::write(ready, b"ready").expect("publish child readiness");
    wait_for_path(go);

    for ordinal in 0..WRITES_PER_PROCESS {
        let inserted = database
            .execute_write(
                &session,
                Statement::new(
                    "INSERT INTO hilo_events (payload) VALUES (?1)",
                    vec![Value::from(format!("worker-{worker}-{ordinal}"))],
                ),
            )
            .await
            .expect("insert a manifest-leased generated ID");
        let generated = inserted
            .value
            .generated_key
            .expect("generated write returns its ID");
        let Value::Int64(id) = generated.value else {
            panic!("hilo_v1 returned a non-integer ID");
        };
        ids.push(id);
        if ordinal == WRITES_PER_PROCESS / 2 {
            database
                .checkpoint()
                .await
                .expect("checkpoint generated-ID shards");
        }
    }

    fs::write(
        output,
        ids.into_iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .expect("write generated IDs");
    session.close().await.expect("close generated-ID session");
    database.close().await.expect("close generated-ID database");
}

#[test]
fn shared_root_process_child() {
    let Ok(root) = env::var("BRISKDB_PROCESS_TEST_ROOT") else {
        return;
    };
    let mode = env::var("BRISKDB_PROCESS_TEST_MODE").expect("child mode");
    let worker = env::var("BRISKDB_PROCESS_TEST_WORKER")
        .expect("child worker")
        .parse::<usize>()
        .expect("numeric child worker");
    let ready = PathBuf::from(env::var("BRISKDB_PROCESS_TEST_READY").expect("ready path"));
    let go = PathBuf::from(env::var("BRISKDB_PROCESS_TEST_GO").expect("go path"));
    let output = PathBuf::from(env::var("BRISKDB_PROCESS_TEST_OUTPUT").expect("output path"));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create child runtime");

    match mode.as_str() {
        "explicit" | "crash" => {
            let route = env::var("BRISKDB_PROCESS_TEST_ROUTE").expect("child route");
            runtime.block_on(run_explicit_writer(
                Path::new(&root),
                worker,
                &route,
                &ready,
                &go,
                &output,
                mode == "crash",
            ));
        }
        "lock" => {
            let route = env::var("BRISKDB_PROCESS_TEST_ROUTE").expect("lock route");
            runtime.block_on(async {
                let database = BriskDb::builder(&root)
                    .with_shard_count(SHARDS)
                    .open()
                    .await
                    .expect("open lock-holder database");
                let routing = Database::open(&root, SHARDS).expect("open routing helper");
                let shard = routing.shard_for_key(route.as_bytes());
                drop(routing);
                let connection = rusqlite::Connection::open(
                    Path::new(&root).join(format!("shards/{shard:04}.sqlite")),
                )
                .expect("open lock-holder shard");
                connection
                    .execute_batch("BEGIN IMMEDIATE")
                    .expect("acquire SQLite writer lock");
                fs::write(&ready, b"ready").expect("publish lock-holder readiness");
                wait_for_path(&go);
                connection
                    .execute_batch("ROLLBACK")
                    .expect("release SQLite writer lock");
                fs::write(&output, b"released").expect("publish lock release");
                database.close().await.expect("close lock-holder database");
            });
        }
        "index-holder" => {
            let _database = Database::open(&root, SHARDS).expect("open catalog holder");
            fs::write(&ready, b"ready").expect("publish catalog-holder readiness");
            wait_for_path(&go);
            let count = Database::inspect_global_indexes(&root)
                .expect("inspect catalog while holding root")
                .len();
            fs::write(&output, count.to_string()).expect("publish held catalog count");
        }
        "index-reader" => {
            fs::write(&ready, b"ready").expect("publish catalog-reader readiness");
            wait_for_path(&go);
            let observations = (0..128)
                .map(|_| {
                    let count = Database::inspect_global_indexes(&root)
                        .expect("inspect concurrent global-index catalog")
                        .len();
                    thread::yield_now();
                    count.to_string()
                })
                .collect::<Vec<_>>()
                .join("\n");
            fs::write(&output, observations).expect("publish catalog observations");
        }
        #[cfg(feature = "experimental-vtab")]
        "hilo" => {
            runtime.block_on(run_hilo_writer(
                Path::new(&root),
                worker,
                &ready,
                &go,
                &output,
            ));
        }
        unexpected => panic!("unexpected shared-root child mode: {unexpected}"),
    }
}

fn setup_events(root: &Path) -> (String, String) {
    let mut database = Database::open(root, SHARDS).expect("initialize shared root");
    database
        .broadcast(
            "CREATE TABLE events (
                tenant_id TEXT NOT NULL,
                id INTEGER NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (tenant_id, id)
             )",
        )
        .expect("create event table on every shard");
    let logical_database = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical_database,
                "events",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text)
                    .expect("declare text shard key"),
            )
            .expect("declare event table"),
        ])
        .expect("register event table");

    let same_route = "same-shard".to_owned();
    let same_shard = database.shard_for_key(same_route.as_bytes());
    let different_route = (0..10_000)
        .map(|candidate| format!("different-{candidate}"))
        .find(|candidate| database.shard_for_key(candidate.as_bytes()) != same_shard)
        .expect("find a route on another shard");
    (same_route, different_route)
}

fn global_index_declaration(database: &Database, name: &str) -> GlobalIndexDeclaration {
    let table_id = database
        .catalog()
        .table("default", "events")
        .unwrap()
        .unwrap()
        .id();
    GlobalIndexDeclaration::new(
        table_id,
        name,
        vec![GlobalIndexKeyPart::new(
            GlobalIndexKeySource::column("payload").unwrap(),
            GlobalIndexKeyType::Text,
        )],
    )
    .unwrap()
    .with_topology(GlobalIndexStorageTopology::SharedSqliteV1)
}

#[cfg(feature = "experimental-vtab")]
fn setup_hilo_events(root: &Path) {
    let mut database = Database::open(root, SHARDS).expect("initialize generated-ID root");
    database
        .broadcast(
            "CREATE TABLE hilo_events (
                id INTEGER PRIMARY KEY,
                payload TEXT NOT NULL
             )",
        )
        .expect("create generated-ID table");
    let logical_database = database.catalog().default_database().id();
    let declaration = TableDeclaration::sharded(
        logical_database,
        "hilo_events",
        ShardKeyMetadata::new("id", ShardKeyType::Int64).expect("declare integer shard key"),
    )
    .expect("declare generated-ID table")
    .with_generated_id_policy(GeneratedIdPolicy::hilo_v1("id").expect("declare hilo policy"))
    .expect("attach hilo policy");
    database
        .register_tables(vec![declaration])
        .expect("register generated-ID table");
}

fn total_rows(root: &Path, table: &str) -> i64 {
    (0..SHARDS)
        .map(|shard| {
            rusqlite::Connection::open(root.join(format!("shards/{shard:04}.sqlite")))
                .expect("open physical shard")
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("count physical rows")
        })
        .sum()
}

fn assert_root_integrity(root: &Path) {
    for path in std::iter::once(root.join("manifest.sqlite"))
        .chain((0..SHARDS).map(|shard| root.join(format!("shards/{shard:04}.sqlite"))))
    {
        let result = rusqlite::Connection::open(&path)
            .expect("open SQLite file for integrity check")
            .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
            .expect("run SQLite quick_check");
        assert_eq!(result, "ok", "integrity failure in {}", path.display());
    }
    drop(Database::open(root, SHARDS).expect("reopen validated BriskDB root"));
}

#[test]
fn independent_processes_overlap_same_and_cross_shard_writes_and_checkpoints() {
    let temp = tempfile::tempdir().expect("create shared root");
    let (same_route, different_route) = setup_events(temp.path());
    let go = temp.path().join("go");
    let routes = [&same_route, &same_route, &different_route];
    let mut children = Vec::new();
    let mut ready_paths = Vec::new();

    for (worker, route) in routes.into_iter().enumerate() {
        let ready = temp.path().join(format!("ready-{worker}"));
        let output = temp.path().join(format!("output-{worker}"));
        children.push(spawn_test_child(
            temp.path(),
            "explicit",
            worker,
            Some(route),
            &ready,
            &go,
            &output,
        ));
        ready_paths.push(ready);
    }

    wait_for_paths(&ready_paths);
    fs::write(&go, b"go").expect("release writers");
    for child in &mut children {
        child.wait_success();
    }

    assert_eq!(total_rows(temp.path(), "events"), WRITES_PER_PROCESS * 3);
    assert_root_integrity(temp.path());
}

#[test]
fn killed_writer_releases_files_and_a_surviving_embedded_process_continues() {
    let temp = tempfile::tempdir().expect("create crash root");
    let (route, _) = setup_events(temp.path());
    let ready = temp.path().join("crash-ready");
    let go = temp.path().join("crash-go");
    let committed = temp.path().join("crash-committed");
    let mut child = spawn_test_child(
        temp.path(),
        "crash",
        7,
        Some(&route),
        &ready,
        &go,
        &committed,
    );
    wait_for_path(&ready);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create survivor runtime");
    let database = runtime
        .block_on(
            BriskDb::builder(temp.path())
                .with_shard_count(SHARDS)
                .open(),
        )
        .expect("open surviving embedded process");
    let session = database.session();
    runtime
        .block_on(session.set_routing_key(&route))
        .expect("set survivor route");
    fs::write(&go, b"go").expect("release crash writer");
    wait_for_path(&committed);
    child.kill_and_wait();

    runtime.block_on(async {
        database
            .execute_write(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, id, payload) VALUES (?1, ?2, ?3)",
                    vec![route.clone().into(), 999_999_i64.into(), "survivor".into()],
                ),
            )
            .await
            .expect("survivor writes after peer crash");
        database.checkpoint().await.expect("checkpoint after crash");
        session.close().await.expect("close survivor session");
        database.close().await.expect("close survivor database");
    });

    assert_eq!(total_rows(temp.path(), "events"), WRITES_PER_PROCESS + 1);
    assert_root_integrity(temp.path());
}

#[test]
fn cross_process_sqlite_contention_is_retryable_and_the_exact_retry_succeeds() {
    let temp = tempfile::tempdir().expect("create contention root");
    let (route, _) = setup_events(temp.path());
    let ready = temp.path().join("lock-ready");
    let go = temp.path().join("lock-go");
    let released = temp.path().join("lock-released");
    let mut child = spawn_test_child(temp.path(), "lock", 0, Some(&route), &ready, &go, &released);
    wait_for_path(&ready);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create contention runtime");
    let database = runtime
        .block_on(
            BriskDb::builder(temp.path())
                .with_shard_count(SHARDS)
                .open(),
        )
        .expect("open contending database");
    let session = database.session();
    runtime
        .block_on(session.set_routing_key(&route))
        .expect("set contention route");
    let statement = || {
        Statement::new(
            "INSERT INTO events (tenant_id, id, payload) VALUES (?1, ?2, ?3)",
            vec![route.clone().into(), 77_i64.into(), "retry-me".into()],
        )
    };
    let error = runtime
        .block_on(database.execute_write(&session, statement()))
        .expect_err("locked peer must expose write contention");
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    assert!(error.is_retryable());

    fs::write(&go, b"release").expect("release lock-holder child");
    child.wait_success();
    runtime.block_on(async {
        database
            .execute_write(&session, statement())
            .await
            .expect("exact retry succeeds after peer releases lock");
        session.close().await.expect("close contention session");
        database.close().await.expect("close contention database");
    });

    assert_eq!(total_rows(temp.path(), "events"), 1);
    assert_root_integrity(temp.path());
}

#[test]
fn global_index_catalog_serializes_writers_and_readers_see_only_complete_snapshots() {
    let temp = tempfile::tempdir().expect("create global-index root");
    setup_events(temp.path());

    let holder_ready = temp.path().join("index-holder-ready");
    let holder_go = temp.path().join("index-holder-go");
    let holder_output = temp.path().join("index-holder-output");
    let mut holder = spawn_test_child(
        temp.path(),
        "index-holder",
        0,
        None,
        &holder_ready,
        &holder_go,
        &holder_output,
    );
    wait_for_path(&holder_ready);

    let mut database = Database::open(temp.path(), SHARDS).expect("open catalog writer");
    let declaration = global_index_declaration(&database, "events_payload_global");
    let error = database
        .create_global_index(declaration.clone())
        .expect_err("a live peer must fence catalog mutation");
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    assert!(error.is_retryable());
    assert!(
        Database::inspect_global_indexes(temp.path())
            .unwrap()
            .is_empty()
    );

    fs::write(&holder_go, b"release").expect("release catalog holder");
    holder.wait_success();
    assert_eq!(fs::read_to_string(&holder_output).unwrap(), "0");
    database
        .create_global_index(declaration)
        .expect("retry catalog mutation after peer closes");

    let reader_ready = temp.path().join("index-reader-ready");
    let reader_go = temp.path().join("index-reader-go");
    let reader_output = temp.path().join("index-reader-output");
    let mut reader = spawn_test_child(
        temp.path(),
        "index-reader",
        1,
        None,
        &reader_ready,
        &reader_go,
        &reader_output,
    );
    wait_for_path(&reader_ready);
    fs::write(&reader_go, b"race").expect("release catalog reader");
    let second_declaration = global_index_declaration(&database, "events_payload_global_2");
    database
        .create_global_index(second_declaration)
        .expect("commit catalog mutation beside read-only inspector");
    reader.wait_success();

    let observations = fs::read_to_string(&reader_output).unwrap();
    assert!(
        observations.lines().all(|count| matches!(count, "1" | "2")),
        "reader observed a partial catalog: {observations}"
    );
    assert_eq!(
        Database::inspect_global_indexes(temp.path()).unwrap().len(),
        2
    );
    drop(database);
    assert_root_integrity(temp.path());
}

#[test]
fn global_index_recovery_is_fenced_while_an_independent_process_uses_the_root() {
    let temp = tempfile::tempdir().expect("create global-index recovery root");
    setup_events(temp.path());
    let mut database = Database::open(temp.path(), SHARDS).expect("open global-index owner");
    let id = database
        .create_global_index(global_index_declaration(
            &database,
            "events_payload_recovery",
        ))
        .expect("create global index");
    database.build_global_index(id).expect("build global index");

    let ready = temp.path().join("recovery-holder-ready");
    let go = temp.path().join("recovery-holder-go");
    let output = temp.path().join("recovery-holder-output");
    let mut holder = spawn_test_child(temp.path(), "index-holder", 0, None, &ready, &go, &output);
    wait_for_path(&ready);

    for error in [
        database.validate_global_index(id).unwrap_err(),
        database.rebuild_global_index(id).unwrap_err(),
        database.repair_global_index(id).unwrap_err(),
    ] {
        assert_eq!(error.kind(), EngineErrorKind::Busy);
        assert!(error.is_retryable());
    }
    assert_eq!(
        database
            .catalog()
            .global_index_by_id(id)
            .unwrap()
            .lifecycle(),
        briskdb::core::GlobalIndexLifecycle::Ready
    );

    fs::write(&go, b"release").expect("release root holder");
    holder.wait_success();
    assert!(
        database
            .validate_global_index(id)
            .expect("retry validation after peer closes")
            .is_valid()
    );
}

#[cfg(feature = "experimental-vtab")]
#[test]
fn independent_processes_reserve_disjoint_hilo_ids_and_persist_every_row() {
    let temp = tempfile::tempdir().expect("create generated-ID root");
    setup_hilo_events(temp.path());
    let go = temp.path().join("hilo-go");
    let mut children = Vec::new();
    let mut ready_paths = Vec::new();
    let mut output_paths = Vec::new();

    for worker in 0..3 {
        let ready = temp.path().join(format!("hilo-ready-{worker}"));
        let output = temp.path().join(format!("hilo-output-{worker}"));
        children.push(spawn_test_child(
            temp.path(),
            "hilo",
            worker,
            None,
            &ready,
            &go,
            &output,
        ));
        ready_paths.push(ready);
        output_paths.push(output);
    }

    wait_for_paths(&ready_paths);
    fs::write(&go, b"go").expect("release generated-ID writers");
    for child in &mut children {
        child.wait_success();
    }

    let ids = output_paths
        .iter()
        .flat_map(|path| {
            fs::read_to_string(path)
                .expect("read child generated IDs")
                .lines()
                .map(|line| line.parse::<i64>().expect("parse generated ID"))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), (WRITES_PER_PROCESS * 3) as usize);
    assert_eq!(
        ids.iter().copied().collect::<BTreeSet<_>>().len(),
        ids.len()
    );
    assert_eq!(
        total_rows(temp.path(), "hilo_events"),
        WRITES_PER_PROCESS * 3
    );
    assert_root_integrity(temp.path());
}

#[cfg(feature = "server-cli")]
fn http_request(address: SocketAddr, method: &str, path: &str, body: Option<&str>) -> String {
    let body = body.unwrap_or("");
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(250))
        .expect("connect to BriskDB HTTP listener");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set HTTP read timeout");
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("write HTTP request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read HTTP response");
    response
}

#[cfg(feature = "server-cli")]
#[test]
fn service_and_embedded_process_share_one_ready_root() {
    let temp = tempfile::tempdir().expect("create service root");
    let (route, _) = setup_events(temp.path());
    let probe = TcpListener::bind("127.0.0.1:0").expect("reserve service port");
    let address = probe.local_addr().expect("read service address");
    drop(probe);
    let child = Command::new(env!("CARGO_BIN_EXE_briskdb"))
        .arg("--listen")
        .arg(address.to_string())
        .arg("--postgres-listen")
        .arg("disabled")
        .arg("--data-dir")
        .arg(temp.path())
        .arg("--shards")
        .arg(SHARDS.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start BriskDB service");
    let mut service = ChildGuard(child);

    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        if let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(50)) {
            stream
                .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .expect("write health request");
            let mut response = String::new();
            stream
                .read_to_string(&mut response)
                .expect("read health response");
            if response.contains("200 OK") && response.contains("\"status\":\"ok\"") {
                break;
            }
        }
        assert!(Instant::now() < deadline, "service did not become healthy");
        assert!(
            service
                .0
                .try_wait()
                .expect("poll service startup")
                .is_none(),
            "service exited before becoming healthy"
        );
        thread::sleep(Duration::from_millis(20));
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create embedded runtime");
    let database = runtime
        .block_on(
            BriskDb::builder(temp.path())
                .with_shard_count(SHARDS)
                .open(),
        )
        .expect("open embedded peer beside service");
    let session = database.session();
    runtime
        .block_on(session.set_routing_key(&route))
        .expect("set embedded route");
    runtime.block_on(async {
        database
            .execute_write(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, id, payload) VALUES (?1, ?2, ?3)",
                    vec![
                        route.clone().into(),
                        1_i64.into(),
                        "embedded-visible".into(),
                    ],
                ),
            )
            .await
            .expect("embedded peer writes while service is live");
    });

    let query = format!(
        "{{\"sql\":\"SELECT payload FROM events WHERE tenant_id = ?1 AND id = ?2\",\"params\":[\"{route}\",1]}}"
    );
    let response = http_request(address, "POST", "/v1/query", Some(&query));
    assert!(response.contains("200 OK"), "query failed: {response}");
    assert!(
        response.contains("embedded-visible"),
        "service missed embedded row"
    );

    let execute = format!(
        "{{\"shard_key\":\"{route}\",\"sql\":\"INSERT INTO events (tenant_id, id, payload) VALUES (?1, ?2, ?3)\",\"params\":[\"{route}\",2,\"service-visible\"]}}"
    );
    let response = http_request(address, "POST", "/v1/execute", Some(&execute));
    assert!(response.contains("200 OK"), "execute failed: {response}");

    runtime.block_on(async {
        let result = database
            .query(
                &session,
                Statement::new(
                    "SELECT payload FROM events WHERE tenant_id = ?1 AND id = ?2",
                    vec![route.clone().into(), 2_i64.into()],
                ),
            )
            .await
            .expect("embedded peer reads service row");
        assert_eq!(
            result.value.rows()[0].get(0).and_then(Value::as_str),
            Some("service-visible")
        );
    });

    service.terminate_and_wait();
    runtime.block_on(async {
        database
            .execute_write(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, id, payload) VALUES (?1, ?2, ?3)",
                    vec![route.into(), 3_i64.into(), "after-service".into()],
                ),
            )
            .await
            .expect("embedded peer continues after service exits");
        session.close().await.expect("close embedded session");
        database.close().await.expect("close embedded database");
    });

    assert_eq!(total_rows(temp.path(), "events"), 3);
    assert_root_integrity(temp.path());
}
