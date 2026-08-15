use std::path::Path;

#[cfg(unix)]
use std::{
    env, fs,
    path::PathBuf,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use briskdb::{
    BriskDb, EngineErrorKind, Statement, Value,
    core::{
        Database, Engine, GlobalIndexDeclaration, GlobalIndexId, GlobalIndexKeyPart,
        GlobalIndexKeySource, GlobalIndexKeyType, GlobalIndexStorageTopology, PrepareRequest,
        ShardKeyMetadata, ShardKeyType, TableDeclaration, UniqueNullSemantics,
    },
    sql,
};
use rusqlite::Connection;

const SHARDS: u16 = 4;

#[cfg(unix)]
const PROCESS_WAIT: Duration = Duration::from_secs(20);

fn setup(root: &Path) -> GlobalIndexId {
    let mut database = Database::open(root, SHARDS).unwrap();
    database
        .broadcast(
            "CREATE TABLE accounts (
                tenant_id TEXT PRIMARY KEY NOT NULL,
                email TEXT,
                active INTEGER NOT NULL
             )",
        )
        .unwrap();
    let logical = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "accounts",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    let table_id = database
        .catalog()
        .table("default", "accounts")
        .unwrap()
        .unwrap()
        .id();
    let declaration = GlobalIndexDeclaration::new(
        table_id,
        "accounts_email_unique",
        vec![GlobalIndexKeyPart::new(
            GlobalIndexKeySource::column("email").unwrap(),
            GlobalIndexKeyType::Text,
        )],
    )
    .unwrap()
    .unique(UniqueNullSemantics::Distinct)
    .with_topology(GlobalIndexStorageTopology::selected_v1());
    let index_id = database.create_global_index(declaration).unwrap();
    database.build_global_index(index_id).unwrap();
    index_id
}

async fn write(db: &BriskDb, route: &str, sql: &str, values: Vec<Value>) {
    let session = db.session();
    session.set_routing_key(route).await.unwrap();
    db.execute_write(&session, Statement::new(sql, values))
        .await
        .unwrap();
    session.close().await.unwrap();
}

async fn write_error(
    db: &BriskDb,
    route: &str,
    sql: &str,
    values: Vec<Value>,
) -> briskdb::EngineError {
    let session = db.session();
    session.set_routing_key(route).await.unwrap();
    let error = db
        .execute_write(&session, Statement::new(sql, values))
        .await
        .unwrap_err();
    session.close().await.unwrap();
    error
}

#[tokio::test]
async fn engine_writes_keep_global_unique_ownership_in_sync() {
    let root = tempfile::tempdir().unwrap();
    let index_id = setup(root.path());
    let db = BriskDb::open(root.path()).await.unwrap();

    write(
        &db,
        "tenant-a",
        "INSERT INTO accounts (tenant_id, email, active) VALUES (?1, ?2, 1)",
        vec!["tenant-a".into(), "first@example.test".into()],
    )
    .await;
    let authority = Connection::open(root.path().join("global-indexes/global.sqlite")).unwrap();
    assert_eq!(
        authority
            .query_row(
                "SELECT COUNT(*) FROM briskdb_global_index_unique_keys WHERE index_id = ?1",
                [i64::try_from(index_id.get()).unwrap()],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    drop(authority);

    let session = db.session();
    session.set_routing_key("tenant-b").await.unwrap();
    let duplicate = db
        .execute_write(
            &session,
            Statement::new(
                "INSERT INTO accounts (tenant_id, email, active) VALUES (?1, ?2, 1)",
                vec!["tenant-b".into(), "first@example.test".into()],
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(duplicate.kind(), EngineErrorKind::UniqueViolation);
    session.close().await.unwrap();

    let session = db.session();
    session.set_routing_key("tenant-ignore").await.unwrap();
    let ignored = db
        .execute_write(
            &session,
            Statement::new(
                "INSERT OR IGNORE INTO accounts (tenant_id, email, active) VALUES (?1, ?2, 1)",
                vec!["tenant-ignore".into(), "first@example.test".into()],
            ),
        )
        .await
        .unwrap();
    assert_eq!(ignored.value.rows_affected, 0);
    session.close().await.unwrap();

    let replaced = write_error(
        &db,
        "tenant-replace",
        "INSERT OR REPLACE INTO accounts (tenant_id, email, active) VALUES (?1, ?2, 1)",
        vec!["tenant-replace".into(), "first@example.test".into()],
    )
    .await;
    assert_eq!(replaced.kind(), EngineErrorKind::UniqueViolation);

    let upsert = write_error(
        &db,
        "tenant-upsert",
        "INSERT INTO accounts (tenant_id, email, active) VALUES (?1, ?2, 1)
         ON CONFLICT (tenant_id) DO UPDATE SET email = excluded.email",
        vec!["tenant-upsert".into(), "upsert@example.test".into()],
    )
    .await;
    assert_eq!(upsert.kind(), EngineErrorKind::Unsupported);

    write(
        &db,
        "tenant-a",
        "UPDATE accounts SET email = ?1 WHERE tenant_id = ?2",
        vec!["second@example.test".into(), "tenant-a".into()],
    )
    .await;
    write(
        &db,
        "tenant-b",
        "INSERT INTO accounts (tenant_id, email, active) VALUES (?1, ?2, 1)",
        vec!["tenant-b".into(), "first@example.test".into()],
    )
    .await;
    write(
        &db,
        "tenant-a",
        "DELETE FROM accounts WHERE tenant_id = ?1",
        vec!["tenant-a".into()],
    )
    .await;
    write(
        &db,
        "tenant-c",
        "INSERT INTO accounts (tenant_id, email, active) VALUES (?1, ?2, 1)",
        vec!["tenant-c".into(), "second@example.test".into()],
    )
    .await;

    db.close().await.unwrap();
    drop(db);
    let mut database = Database::open(root.path(), SHARDS).unwrap();
    let ignored = database
        .query(
            "tenant-ignore",
            "SELECT COUNT(*) FROM accounts WHERE tenant_id = ?1",
            &["tenant-ignore".into()],
        )
        .unwrap();
    assert_eq!(ignored.rows()[0].get(0).and_then(Value::as_i64), Some(0));
    let validation = database.validate_global_index(index_id).unwrap();
    assert!(validation.is_valid(), "{validation:?}");
}

#[tokio::test]
async fn null_distinct_rows_do_not_take_global_reservations() {
    let root = tempfile::tempdir().unwrap();
    let index_id = setup(root.path());
    let db = BriskDb::open(root.path()).await.unwrap();
    for tenant in ["null-a", "null-b"] {
        write(
            &db,
            tenant,
            "INSERT INTO accounts (tenant_id, email, active) VALUES (?1, NULL, 1)",
            vec![tenant.into()],
        )
        .await;
    }
    db.close().await.unwrap();
    drop(db);

    let mut database = Database::open(root.path(), SHARDS).unwrap();
    let validation = database.validate_global_index(index_id).unwrap();
    assert!(validation.is_valid(), "{validation:?}");
}

#[tokio::test]
async fn partial_compound_updates_reserve_only_qualifying_keys() {
    let root = tempfile::tempdir().unwrap();
    let mut database = Database::open(root.path(), SHARDS).unwrap();
    database
        .broadcast(
            "CREATE TABLE memberships (
                tenant_id TEXT NOT NULL,
                local_id INTEGER NOT NULL,
                namespace TEXT NOT NULL,
                external_id INTEGER NOT NULL,
                active INTEGER NOT NULL,
                PRIMARY KEY (tenant_id, local_id)
             ) WITHOUT ROWID",
        )
        .unwrap();
    let logical = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "memberships",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    let table_id = database
        .catalog()
        .table("default", "memberships")
        .unwrap()
        .unwrap()
        .id();
    let declaration = GlobalIndexDeclaration::new(
        table_id,
        "memberships_external_unique",
        vec![
            GlobalIndexKeyPart::new(
                GlobalIndexKeySource::column("namespace").unwrap(),
                GlobalIndexKeyType::Text,
            ),
            GlobalIndexKeyPart::new(
                GlobalIndexKeySource::expression("external_id * 2").unwrap(),
                GlobalIndexKeyType::Int64,
            ),
        ],
    )
    .unwrap()
    .unique(UniqueNullSemantics::Distinct)
    .with_predicate("active = 1")
    .unwrap()
    .with_topology(GlobalIndexStorageTopology::selected_v1());
    let index_id = database.create_global_index(declaration).unwrap();
    database.build_global_index(index_id).unwrap();
    drop(database);

    let db = BriskDb::open(root.path()).await.unwrap();
    for tenant in ["partial-a", "partial-b"] {
        write(
            &db,
            tenant,
            "INSERT INTO memberships (
                 tenant_id, local_id, namespace, external_id, active
             ) VALUES (?1, 1, 'github', 7, 0)",
            vec![tenant.into()],
        )
        .await;
    }
    write(
        &db,
        "partial-a",
        "UPDATE memberships SET active = 1 WHERE tenant_id = ?1 AND local_id = 1",
        vec!["partial-a".into()],
    )
    .await;
    let duplicate = write_error(
        &db,
        "partial-b",
        "UPDATE memberships SET active = 1 WHERE tenant_id = ?1 AND local_id = 1",
        vec!["partial-b".into()],
    )
    .await;
    assert_eq!(duplicate.kind(), EngineErrorKind::UniqueViolation);
    write(
        &db,
        "partial-a",
        "UPDATE memberships SET external_id = 8 WHERE tenant_id = ?1 AND local_id = 1",
        vec!["partial-a".into()],
    )
    .await;
    write(
        &db,
        "partial-b",
        "UPDATE memberships SET active = 1 WHERE tenant_id = ?1 AND local_id = 1",
        vec!["partial-b".into()],
    )
    .await;
    db.close().await.unwrap();
    drop(db);

    let mut database = Database::open(root.path(), SHARDS).unwrap();
    let validation = database.validate_global_index(index_id).unwrap();
    assert!(validation.is_valid(), "{validation:?}");
}

#[tokio::test]
async fn null_not_distinct_is_enforced_globally() {
    let root = tempfile::tempdir().unwrap();
    let mut database = Database::open(root.path(), SHARDS).unwrap();
    database
        .broadcast(
            "CREATE TABLE aliases (
                tenant_id TEXT PRIMARY KEY NOT NULL,
                alias TEXT
             )",
        )
        .unwrap();
    let logical = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "aliases",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    let table_id = database
        .catalog()
        .table("default", "aliases")
        .unwrap()
        .unwrap()
        .id();
    let declaration = GlobalIndexDeclaration::new(
        table_id,
        "aliases_unique",
        vec![GlobalIndexKeyPart::new(
            GlobalIndexKeySource::column("alias").unwrap(),
            GlobalIndexKeyType::Text,
        )],
    )
    .unwrap()
    .unique(UniqueNullSemantics::NotDistinct)
    .with_topology(GlobalIndexStorageTopology::selected_v1());
    let index_id = database.create_global_index(declaration).unwrap();
    database.build_global_index(index_id).unwrap();
    drop(database);

    let db = BriskDb::open(root.path()).await.unwrap();
    write(
        &db,
        "alias-a",
        "INSERT INTO aliases (tenant_id, alias) VALUES (?1, NULL)",
        vec!["alias-a".into()],
    )
    .await;
    let duplicate = write_error(
        &db,
        "alias-b",
        "INSERT INTO aliases (tenant_id, alias) VALUES (?1, NULL)",
        vec!["alias-b".into()],
    )
    .await;
    assert_eq!(duplicate.kind(), EngineErrorKind::UniqueViolation);
    db.close().await.unwrap();
    drop(db);

    let mut database = Database::open(root.path(), SHARDS).unwrap();
    let validation = database.validate_global_index(index_id).unwrap();
    assert!(validation.is_valid(), "{validation:?}");
}

#[tokio::test]
async fn multirow_authoritative_changes_are_rejected_before_commit() {
    let root = tempfile::tempdir().unwrap();
    let mut database = Database::open(root.path(), SHARDS).unwrap();
    database
        .broadcast(
            "CREATE TABLE contacts (
                tenant_id TEXT NOT NULL,
                local_id INTEGER NOT NULL,
                email TEXT NOT NULL,
                PRIMARY KEY (tenant_id, local_id)
             ) WITHOUT ROWID",
        )
        .unwrap();
    let logical = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "contacts",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    let table_id = database
        .catalog()
        .table("default", "contacts")
        .unwrap()
        .unwrap()
        .id();
    let declaration = GlobalIndexDeclaration::new(
        table_id,
        "contacts_email_unique",
        vec![GlobalIndexKeyPart::new(
            GlobalIndexKeySource::column("email").unwrap(),
            GlobalIndexKeyType::Text,
        )],
    )
    .unwrap()
    .unique(UniqueNullSemantics::Distinct)
    .with_topology(GlobalIndexStorageTopology::selected_v1());
    let index_id = database.create_global_index(declaration).unwrap();
    database.build_global_index(index_id).unwrap();
    drop(database);

    let db = BriskDb::open(root.path()).await.unwrap();
    let error = write_error(
        &db,
        "batch",
        "INSERT INTO contacts (tenant_id, local_id, email) VALUES
             (?1, 1, 'one@example.test'),
             (?1, 2, 'two@example.test')",
        vec!["batch".into()],
    )
    .await;
    assert_eq!(error.kind(), EngineErrorKind::Unsupported);
    db.close().await.unwrap();
    drop(db);

    let mut database = Database::open(root.path(), SHARDS).unwrap();
    let validation = database.validate_global_index(index_id).unwrap();
    assert!(validation.is_valid(), "{validation:?}");
    assert_eq!(validation.source_rows_examined(), 0);
}

#[tokio::test]
async fn prepared_postgres_dialect_writes_use_the_same_authority() {
    let root = tempfile::tempdir().unwrap();
    let index_id = setup(root.path());
    let engine = Engine::open(root.path(), SHARDS).await.unwrap();
    let session = engine.session();
    let logical = engine.catalog().default_database().id();
    let statement = engine
        .prepare_statement(
            &session,
            PrepareRequest::new(
                logical,
                sql::SqlDialect::PostgreSql,
                sql::SqlTranslationMode::Compatibility,
                "INSERT INTO accounts (tenant_id, email, active) VALUES ($1, $2, 1)",
            ),
        )
        .await
        .unwrap();
    for (tenant, expected) in [
        ("prepared-a", None),
        ("prepared-b", Some(EngineErrorKind::UniqueViolation)),
    ] {
        let portal = engine
            .bind_statement(
                &session,
                statement,
                vec![tenant.into(), "prepared@example.test".into()],
            )
            .await
            .unwrap();
        let result = engine.execute_portal(&session, portal).await;
        match expected {
            None => assert!(result.is_ok(), "{result:?}"),
            Some(kind) => assert_eq!(result.unwrap_err().kind(), kind),
        }
    }
    session.close().await.unwrap();
    engine.shutdown().await.unwrap();
    drop(engine);

    let mut database = Database::open(root.path(), SHARDS).unwrap();
    let validation = database.validate_global_index(index_id).unwrap();
    assert!(validation.is_valid(), "{validation:?}");
}

#[cfg(unix)]
fn wait_for_process_paths(paths: &[PathBuf]) {
    let deadline = Instant::now() + PROCESS_WAIT;
    while paths.iter().any(|path| !path.exists()) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert!(
        paths.iter().all(|path| path.exists()),
        "timed out waiting for global-index race barrier: {paths:?}"
    );
}

#[cfg(unix)]
fn run_process_race(root: &Path, mode: &str, routes: &[&str]) -> Vec<String> {
    let go = root.join(format!("{mode}-go"));
    let mut children = Vec::new();
    let mut ready = Vec::new();
    let mut outputs = Vec::new();
    for (worker, route) in routes.iter().enumerate() {
        let ready_path = root.join(format!("{mode}-ready-{worker}"));
        let output_path = root.join(format!("{mode}-output-{worker}"));
        let child = Command::new(env::current_exe().unwrap())
            .args(["--exact", "global_index_write_process_child", "--nocapture"])
            .env("BRISKDB_INDEX_WRITE_ROOT", root)
            .env("BRISKDB_INDEX_WRITE_MODE", mode)
            .env("BRISKDB_INDEX_WRITE_WORKER", worker.to_string())
            .env("BRISKDB_INDEX_WRITE_ROUTE", route)
            .env("BRISKDB_INDEX_WRITE_READY", &ready_path)
            .env("BRISKDB_INDEX_WRITE_GO", &go)
            .env("BRISKDB_INDEX_WRITE_OUTPUT", &output_path)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        children.push(child);
        ready.push(ready_path);
        outputs.push(output_path);
    }
    wait_for_process_paths(&ready);
    fs::write(&go, b"go").unwrap();
    for mut child in children {
        let status = child.wait().unwrap();
        assert!(status.success(), "global-index race child failed: {status}");
    }
    outputs
        .iter()
        .map(|path| fs::read_to_string(path).unwrap())
        .collect()
}

#[cfg(unix)]
async fn run_process_write(root: &Path, mode: &str, worker: usize, route: &str) -> String {
    let db = BriskDb::open(root).await.unwrap();
    let session = db.session();
    session.set_routing_key(route).await.unwrap();
    let statement = match (mode, worker) {
        ("insert", _) => Statement::new(
            "INSERT INTO accounts (tenant_id, email, active) VALUES (?1, ?2, 1)",
            vec![route.into(), "process-race@example.test".into()],
        ),
        ("update", _) => Statement::new(
            "UPDATE accounts SET email = ?1 WHERE tenant_id = ?2",
            vec!["process-update@example.test".into(), route.into()],
        ),
        ("update-delete", 0) => Statement::new(
            "UPDATE accounts SET email = ?1 WHERE tenant_id = ?2",
            vec!["process-final@example.test".into(), route.into()],
        ),
        ("update-delete", 1) => Statement::new(
            "DELETE FROM accounts WHERE tenant_id = ?1",
            vec![route.into()],
        ),
        _ => panic!("unknown indexed-write process mode {mode}/{worker}"),
    };
    let deadline = Instant::now() + PROCESS_WAIT;
    let result = loop {
        match db.execute_write(&session, statement.clone()).await {
            Err(error) if error.kind() == EngineErrorKind::Busy && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            result => break result,
        }
    };
    let output = match result {
        Ok(result) => format!("ok:{}", result.value.rows_affected),
        Err(error) if error.kind() == EngineErrorKind::UniqueViolation => {
            "unique_violation".to_owned()
        }
        Err(error) => panic!("unexpected indexed-write race error: {error}"),
    };
    session.close().await.unwrap();
    db.close().await.unwrap();
    output
}

#[cfg(unix)]
#[test]
fn global_index_write_process_child() {
    let Ok(root) = env::var("BRISKDB_INDEX_WRITE_ROOT") else {
        return;
    };
    let mode = env::var("BRISKDB_INDEX_WRITE_MODE").unwrap();
    let worker = env::var("BRISKDB_INDEX_WRITE_WORKER")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let route = env::var("BRISKDB_INDEX_WRITE_ROUTE").unwrap();
    let ready = PathBuf::from(env::var("BRISKDB_INDEX_WRITE_READY").unwrap());
    let go = PathBuf::from(env::var("BRISKDB_INDEX_WRITE_GO").unwrap());
    let output = PathBuf::from(env::var("BRISKDB_INDEX_WRITE_OUTPUT").unwrap());
    fs::write(&ready, b"ready").unwrap();
    wait_for_process_paths(&[go]);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = runtime.block_on(run_process_write(Path::new(&root), &mode, worker, &route));
    fs::write(output, result).unwrap();
}

#[cfg(unix)]
#[test]
fn process_races_and_exact_retries_preserve_global_unique_authority() {
    let insert_root = tempfile::tempdir().unwrap();
    let insert_index = setup(insert_root.path());
    let mut insert_results = run_process_race(
        insert_root.path(),
        "insert",
        &["process-insert-a", "process-insert-b"],
    );
    insert_results.sort();
    assert_eq!(insert_results, ["ok:1", "unique_violation"]);
    let mut database = Database::open(insert_root.path(), SHARDS).unwrap();
    assert!(
        database
            .validate_global_index(insert_index)
            .unwrap()
            .is_valid()
    );
    drop(database);

    let update_root = tempfile::tempdir().unwrap();
    let update_index = setup(update_root.path());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let db = BriskDb::open(update_root.path()).await.unwrap();
        for (route, email) in [
            ("process-update-a", "before-a@example.test"),
            ("process-update-b", "before-b@example.test"),
        ] {
            write(
                &db,
                route,
                "INSERT INTO accounts (tenant_id, email, active) VALUES (?1, ?2, 1)",
                vec![route.into(), email.into()],
            )
            .await;
        }
        db.close().await.unwrap();
    });
    let update_results = run_process_race(
        update_root.path(),
        "update",
        &["process-update-a", "process-update-b"],
    );
    let mut sorted_update_results = update_results.clone();
    sorted_update_results.sort();
    assert_eq!(sorted_update_results, ["ok:1", "unique_violation"]);
    let retry_route = if update_results[0] == "unique_violation" {
        "process-update-a"
    } else {
        "process-update-b"
    };
    runtime.block_on(async {
        let db = BriskDb::open(update_root.path()).await.unwrap();
        let retry = write_error(
            &db,
            retry_route,
            "UPDATE accounts SET email = ?1 WHERE tenant_id = ?2",
            vec!["process-update@example.test".into(), retry_route.into()],
        )
        .await;
        assert_eq!(retry.kind(), EngineErrorKind::UniqueViolation);
        write(
            &db,
            retry_route,
            "UPDATE accounts SET email = ?1 WHERE tenant_id = ?2",
            vec!["retry-safe@example.test".into(), retry_route.into()],
        )
        .await;
        db.close().await.unwrap();
    });
    let mut database = Database::open(update_root.path(), SHARDS).unwrap();
    assert!(
        database
            .validate_global_index(update_index)
            .unwrap()
            .is_valid()
    );
    drop(database);

    let delete_root = tempfile::tempdir().unwrap();
    let delete_index = setup(delete_root.path());
    runtime.block_on(async {
        let db = BriskDb::open(delete_root.path()).await.unwrap();
        write(
            &db,
            "process-delete",
            "INSERT INTO accounts (tenant_id, email, active) VALUES (?1, ?2, 1)",
            vec![
                "process-delete".into(),
                "process-before@example.test".into(),
            ],
        )
        .await;
        db.close().await.unwrap();
    });
    let delete_results = run_process_race(
        delete_root.path(),
        "update-delete",
        &["process-delete", "process-delete"],
    );
    assert!(
        delete_results
            .iter()
            .all(|result| result.starts_with("ok:"))
    );
    let mut database = Database::open(delete_root.path(), SHARDS).unwrap();
    assert!(
        database
            .validate_global_index(delete_index)
            .unwrap()
            .is_valid()
    );
}
