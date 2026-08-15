use std::{
    env, fs,
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use briskdb::core::{
    CancellationToken, Database, EngineErrorKind, GlobalIndexDeclaration, GlobalIndexKeyPart,
    GlobalIndexKeySource, GlobalIndexKeyType, GlobalIndexLifecycle, GlobalIndexStorageTopology,
    IndexKeyOrder, ShardKeyMetadata, ShardKeyType, TableDeclaration, TableId, UniqueNullSemantics,
    Value,
};
use rusqlite::Connection;

const SHARDS: u16 = 4;
const CHILD_ROOT: &str = "BRISKDB_GLOBAL_INDEX_BUILD_CHILD_ROOT";
const CHILD_READY: &str = "BRISKDB_GLOBAL_INDEX_BUILD_CHILD_READY";
const CHILD_RELEASE: &str = "BRISKDB_GLOBAL_INDEX_BUILD_CHILD_RELEASE";

fn setup(root: &Path, without_rowid: bool) -> Database {
    let mut database = Database::open(root, SHARDS).unwrap();
    let suffix = if without_rowid { " WITHOUT ROWID" } else { "" };
    database
        .broadcast(&format!(
            "CREATE TABLE events (
                tenant_id TEXT NOT NULL,
                local_id INTEGER NOT NULL,
                email TEXT,
                category TEXT NOT NULL,
                score INTEGER NOT NULL,
                active INTEGER NOT NULL,
                PRIMARY KEY (tenant_id, local_id)
             ){suffix}"
        ))
        .unwrap();
    let logical = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "events",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    database
}

fn table_id(database: &Database) -> TableId {
    database
        .catalog()
        .table("default", "events")
        .unwrap()
        .unwrap()
        .id()
}

fn declaration(
    database: &Database,
    name: &str,
    parts: Vec<GlobalIndexKeyPart>,
) -> GlobalIndexDeclaration {
    GlobalIndexDeclaration::new(table_id(database), name, parts)
        .unwrap()
        .with_topology(GlobalIndexStorageTopology::selected_v1())
}

fn seed(database: &Database, rows: usize) {
    for ordinal in 0..rows {
        let tenant = format!("tenant-{}", ordinal % 29);
        let email = format!("user-{ordinal}@example.test");
        let category = format!("category-{}", ordinal % 7);
        database
            .execute(
                &tenant,
                "INSERT INTO events (
                    tenant_id, local_id, email, category, score, active
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                &[
                    Value::from(tenant.as_str()),
                    Value::from(ordinal as i64),
                    Value::from(email),
                    Value::from(category),
                    Value::from((ordinal % 101) as i64),
                    Value::from(if ordinal % 3 == 0 { 1_i64 } else { 0_i64 }),
                ],
            )
            .unwrap();
    }
}

fn physical_count(root: &Path, table: &str, index_id: briskdb::GlobalIndexId) -> i64 {
    let connection = Connection::open(root.join("global-indexes/global.sqlite")).unwrap();
    connection
        .query_row(
            &format!("SELECT COUNT(*) FROM {table} WHERE index_id = ?1"),
            [i64::try_from(index_id.get()).unwrap()],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn offline_builder_covers_empty_large_compound_sparse_and_without_rowid_sources() {
    for without_rowid in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let mut database = setup(temp.path(), without_rowid);

        let empty = declaration(
            &database,
            "events_empty",
            vec![GlobalIndexKeyPart::new(
                GlobalIndexKeySource::column("email").unwrap(),
                GlobalIndexKeyType::Text,
            )],
        );
        let empty_id = database.create_global_index(empty).unwrap();
        let report = database.build_global_index(empty_id).unwrap();
        assert_eq!(report.index_id(), empty_id);
        assert_eq!(report.shard_count(), SHARDS);
        assert_eq!(report.indexed_rows(), 0);
        assert_eq!(
            database
                .catalog()
                .global_index_by_id(empty_id)
                .unwrap()
                .lifecycle(),
            GlobalIndexLifecycle::Ready
        );

        seed(&database, 2_000);
        let compound = declaration(
            &database,
            "events_compound_sparse",
            vec![
                GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::column("category").unwrap(),
                    GlobalIndexKeyType::Text,
                ),
                GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::expression("score * 2").unwrap(),
                    GlobalIndexKeyType::Int64,
                )
                .with_order(IndexKeyOrder::Descending),
            ],
        )
        .with_predicate("active = 1")
        .unwrap();
        let compound_id = database.create_global_index(compound).unwrap();
        let report = database.build_global_index(compound_id).unwrap();
        assert_eq!(report.indexed_rows(), 667);
        assert_eq!(
            physical_count(temp.path(), "briskdb_global_index_entries", compound_id),
            667
        );
        assert_eq!(
            physical_count(temp.path(), "briskdb_global_index_unique_keys", compound_id),
            0
        );
        let revalidated = database.build_global_index(compound_id).unwrap();
        assert_eq!(revalidated.resumed_from_shard(), SHARDS);
        assert_eq!(revalidated.indexed_rows(), 667);
    }
}

#[test]
fn unique_build_reports_both_sources_and_restarts_after_the_data_is_fixed() {
    let temp = tempfile::tempdir().unwrap();
    let mut database = setup(temp.path(), false);
    for (tenant, local_id) in [("tenant-a", 1_i64), ("tenant-b", 2_i64)] {
        database
            .execute(
                tenant,
                "INSERT INTO events (
                    tenant_id, local_id, email, category, score, active
                 ) VALUES (?1, ?2, 'duplicate@example.test', 'same', 1, 1)",
                &[Value::from(tenant), Value::from(local_id)],
            )
            .unwrap();
    }
    let unique = declaration(
        &database,
        "events_email_unique",
        vec![GlobalIndexKeyPart::new(
            GlobalIndexKeySource::column("email").unwrap(),
            GlobalIndexKeyType::Text,
        )],
    )
    .unique(UniqueNullSemantics::Distinct);
    let index_id = database.create_global_index(unique).unwrap();
    let error = database.build_global_index(index_id).unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::UniqueViolation);
    assert!(error.diagnostic().contains("shard"));
    assert!(error.diagnostic().contains("key bytes are redacted"));
    assert_eq!(
        database
            .catalog()
            .global_index_by_id(index_id)
            .unwrap()
            .lifecycle(),
        GlobalIndexLifecycle::Creating
    );

    database
        .execute(
            "tenant-b",
            "DELETE FROM events WHERE tenant_id = ?1 AND local_id = ?2",
            &[Value::from("tenant-b"), Value::from(2_i64)],
        )
        .unwrap();
    let report = database.build_global_index(index_id).unwrap();
    assert_eq!(report.indexed_rows(), 1);
    assert_eq!(
        physical_count(temp.path(), "briskdb_global_index_unique_keys", index_id),
        1
    );
}

#[test]
fn ready_revalidation_rejects_semantically_tampered_physical_entries() {
    let temp = tempfile::tempdir().unwrap();
    let mut database = setup(temp.path(), false);
    seed(&database, 4);
    let index = declaration(
        &database,
        "events_email_tamper",
        vec![GlobalIndexKeyPart::new(
            GlobalIndexKeySource::column("email").unwrap(),
            GlobalIndexKeyType::Text,
        )],
    );
    let index_id = database.create_global_index(index).unwrap();
    database.build_global_index(index_id).unwrap();
    Connection::open(temp.path().join("global-indexes/global.sqlite"))
        .unwrap()
        .execute(
            "UPDATE briskdb_global_index_entries
             SET encoded_key = x'00'
             WHERE index_id = ?1 AND source_ordinal = 0",
            [i64::try_from(index_id.get()).unwrap()],
        )
        .unwrap();
    let error = database.build_global_index(index_id).unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
    assert_eq!(
        database
            .query("tenant-0", "SELECT 1", &[])
            .unwrap_err()
            .kind(),
        EngineErrorKind::DataCorruption
    );
}

#[test]
fn cancellation_and_manual_ready_publication_leave_the_database_usable() {
    let temp = tempfile::tempdir().unwrap();
    let mut database = setup(temp.path(), false);
    seed(&database, 20);
    let index = declaration(
        &database,
        "events_email_cancelled",
        vec![GlobalIndexKeyPart::new(
            GlobalIndexKeySource::column("email").unwrap(),
            GlobalIndexKeyType::Text,
        )],
    );
    let index_id = database.create_global_index(index).unwrap();
    assert_eq!(
        database
            .transition_global_index(index_id, GlobalIndexLifecycle::Ready)
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
    let cancellation = CancellationToken::new();
    assert!(cancellation.cancel());
    assert_eq!(
        database
            .build_global_index_with_cancellation(index_id, &cancellation)
            .unwrap_err()
            .kind(),
        EngineErrorKind::Cancelled
    );
    assert_eq!(
        database
            .query(
                "tenant-0",
                "SELECT COUNT(*) FROM events WHERE tenant_id = ?1",
                &[Value::from("tenant-0")],
            )
            .unwrap()
            .rows()[0]
            .values()[0],
        Value::from(1_i64)
    );
    assert_eq!(
        database
            .build_global_index(index_id)
            .unwrap()
            .indexed_rows(),
        20
    );
    database
        .transition_global_index(index_id, GlobalIndexLifecycle::Dropping)
        .unwrap();
    database.remove_global_index(index_id).unwrap();
    assert_eq!(
        physical_count(temp.path(), "briskdb_global_index_builds", index_id),
        0
    );
}

#[test]
fn global_index_build_peer_child() {
    let Ok(root) = env::var(CHILD_ROOT) else {
        return;
    };
    let ready = env::var(CHILD_READY).unwrap();
    let release = env::var(CHILD_RELEASE).unwrap();
    let database = Database::open(root, SHARDS).unwrap();
    fs::write(&ready, b"ready").unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !Path::new(&release).exists() {
        assert!(Instant::now() < deadline, "timed out waiting for release");
        thread::sleep(Duration::from_millis(5));
    }
    drop(database);
}

#[test]
fn offline_builder_requires_sole_process_ownership() {
    let temp = tempfile::tempdir().unwrap();
    let mut database = setup(temp.path(), false);
    seed(&database, 10);
    let index = declaration(
        &database,
        "events_email_fenced",
        vec![GlobalIndexKeyPart::new(
            GlobalIndexKeySource::column("email").unwrap(),
            GlobalIndexKeyType::Text,
        )],
    );
    let index_id = database.create_global_index(index).unwrap();
    let ready = temp.path().join("peer-ready");
    let release = temp.path().join("peer-release");
    let mut child = Command::new(env::current_exe().unwrap())
        .args(["--exact", "global_index_build_peer_child", "--nocapture"])
        .env(CHILD_ROOT, temp.path())
        .env(CHILD_READY, &ready)
        .env(CHILD_RELEASE, &release)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready.exists() {
        assert!(Instant::now() < deadline, "timed out waiting for peer");
        thread::sleep(Duration::from_millis(5));
    }
    let error = database.build_global_index(index_id).unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    assert!(error.is_retryable());
    fs::write(&release, b"release").unwrap();
    assert!(child.wait().unwrap().success());
    assert_eq!(
        database
            .build_global_index(index_id)
            .unwrap()
            .indexed_rows(),
        10
    );
}
