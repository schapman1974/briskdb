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
    BriskDb, CancellationToken, EngineErrorKind, GlobalIndexOutboxCursor,
    GlobalIndexOutboxEventKind, MAX_GLOBAL_INDEX_OUTBOX_EVENTS_PER_SHARD, Statement, Value,
    core::{
        Database, GlobalIndexDeclaration, GlobalIndexId, GlobalIndexKeyPart, GlobalIndexKeySource,
        GlobalIndexKeyType, GlobalIndexLifecycle, GlobalIndexStorageTopology, ShardKeyMetadata,
        ShardKeyType, TableDeclaration,
    },
};

const SHARDS: u16 = 4;

fn setup(root: &Path) -> GlobalIndexId {
    let mut database = Database::open(root, SHARDS).unwrap();
    database
        .broadcast(
            "CREATE TABLE accounts (
                tenant_id TEXT PRIMARY KEY NOT NULL,
                email TEXT NOT NULL,
                active INTEGER NOT NULL CHECK (active IN (0, 1)),
                note TEXT NOT NULL
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
    let table = database
        .catalog()
        .table("default", "accounts")
        .unwrap()
        .unwrap()
        .id();
    let declaration = GlobalIndexDeclaration::new(
        table,
        "accounts_email",
        vec![GlobalIndexKeyPart::new(
            GlobalIndexKeySource::column("email").unwrap(),
            GlobalIndexKeyType::Text,
        )],
    )
    .unwrap()
    .with_predicate("active = 1")
    .unwrap()
    .with_topology(GlobalIndexStorageTopology::selected_v1());
    let index = database.create_global_index(declaration).unwrap();
    database.build_global_index(index).unwrap();
    index
}

async fn write(db: &BriskDb, route: &str, sql: &str, values: Vec<Value>) -> usize {
    let session = db.session();
    session.set_routing_key(route).await.unwrap();
    let affected = db
        .execute_write(&session, Statement::new(sql, values))
        .await
        .unwrap()
        .value
        .rows_affected;
    session.close().await.unwrap();
    affected
}

#[tokio::test]
async fn committed_nonunique_changes_replay_in_order_and_prune_durably() {
    let root = tempfile::tempdir().unwrap();
    let index = setup(root.path());
    let route = "outbox-account";
    let database = Database::open(root.path(), SHARDS).unwrap();
    let shard = database.shard_for_key(route.as_bytes());
    drop(database);

    let db = BriskDb::open(root.path()).await.unwrap();
    assert_eq!(
        write(
            &db,
            route,
            "INSERT INTO accounts (tenant_id, email, active, note)
             VALUES (?1, 'first@example.test', 1, 'a')",
            vec![route.into()],
        )
        .await,
        1
    );
    write(
        &db,
        route,
        "UPDATE accounts SET email = 'second@example.test' WHERE tenant_id = ?1",
        vec![route.into()],
    )
    .await;
    write(
        &db,
        route,
        "UPDATE accounts SET note = 'b' WHERE tenant_id = ?1",
        vec![route.into()],
    )
    .await;
    write(
        &db,
        route,
        "UPDATE accounts SET active = 0 WHERE tenant_id = ?1",
        vec![route.into()],
    )
    .await;
    write(
        &db,
        route,
        "UPDATE accounts SET active = 1 WHERE tenant_id = ?1",
        vec![route.into()],
    )
    .await;
    write(
        &db,
        route,
        "DELETE FROM accounts WHERE tenant_id = ?1",
        vec![route.into()],
    )
    .await;
    db.close().await.unwrap();
    drop(db);

    let database = Database::open(root.path(), SHARDS).unwrap();
    let batch = database
        .read_global_index_outbox(index, shard, GlobalIndexOutboxCursor::new(0), 32)
        .unwrap();
    assert_eq!(batch.high_water().get(), 5);
    assert_eq!(
        batch
            .events()
            .iter()
            .map(|event| event.kind())
            .collect::<Vec<_>>(),
        [
            GlobalIndexOutboxEventKind::Insert,
            GlobalIndexOutboxEventKind::Update,
            GlobalIndexOutboxEventKind::Tombstone,
            GlobalIndexOutboxEventKind::Insert,
            GlobalIndexOutboxEventKind::Delete,
        ]
    );
    assert_eq!(
        batch
            .events()
            .iter()
            .map(|event| event.cursor().get())
            .collect::<Vec<_>>(),
        [1, 2, 3, 4, 5]
    );
    assert!(
        batch
            .events()
            .windows(2)
            .all(|events| { events[0].operation_id() != events[1].operation_id() })
    );
    let status = database.global_index_outbox_status().unwrap();
    assert_eq!(status[usize::from(shard)].retained_events(), 5);
    assert_eq!(status[usize::from(shard)].lag(), 5);

    let advanced = database
        .advance_global_index_outbox(index, shard, batch.high_water())
        .unwrap();
    assert_eq!(advanced.minimum_durable_cursor().get(), 5);
    let pruned = database.prune_global_index_outbox(shard, 32).unwrap();
    assert_eq!(pruned.deleted_events(), 5);
    assert_eq!(pruned.pruned_through().get(), 5);
    drop(database);

    let reopened = Database::open(root.path(), SHARDS).unwrap();
    let status = reopened.global_index_outbox_status().unwrap();
    assert_eq!(status[usize::from(shard)].high_water().get(), 5);
    assert_eq!(status[usize::from(shard)].pruned_through().get(), 5);
    assert_eq!(status[usize::from(shard)].retained_events(), 0);
}

#[tokio::test]
async fn failed_and_cancelled_operations_publish_no_consumable_event() {
    let root = tempfile::tempdir().unwrap();
    let index = setup(root.path());
    let route = "outbox-rollback";
    let database = Database::open(root.path(), SHARDS).unwrap();
    let shard = database.shard_for_key(route.as_bytes());
    drop(database);

    let db = BriskDb::open(root.path()).await.unwrap();
    let session = db.session();
    session.set_routing_key(route).await.unwrap();
    let error = db
        .execute_write(
            &session,
            Statement::new(
                "INSERT INTO accounts (tenant_id, email, active, note)
                 VALUES (?1, 'bad@example.test', 2, 'bad')",
                vec![route.into()],
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::CheckViolation);
    session.close().await.unwrap();
    db.close().await.unwrap();
    drop(db);

    let database = Database::open(root.path(), SHARDS).unwrap();
    let batch = database
        .read_global_index_outbox(index, shard, GlobalIndexOutboxCursor::new(0), 1)
        .unwrap();
    assert_eq!(batch.high_water().get(), 0);
    assert!(batch.events().is_empty());

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = database
        .read_global_index_outbox_with_cancellation(
            index,
            shard,
            GlobalIndexOutboxCursor::new(0),
            1,
            &cancellation,
        )
        .unwrap_err();
    assert_eq!(cancelled.kind(), EngineErrorKind::Cancelled);
    assert_eq!(
        database
            .read_global_index_outbox(index, shard, GlobalIndexOutboxCursor::new(0), 0)
            .unwrap_err()
            .kind(),
        EngineErrorKind::InvalidArgument
    );
}

#[tokio::test]
async fn removing_an_index_releases_its_retention_fence() {
    let root = tempfile::tempdir().unwrap();
    let index = setup(root.path());
    let route = "outbox-drop";
    let database = Database::open(root.path(), SHARDS).unwrap();
    let shard = database.shard_for_key(route.as_bytes());
    drop(database);

    let db = BriskDb::open(root.path()).await.unwrap();
    write(
        &db,
        route,
        "INSERT INTO accounts (tenant_id, email, active, note)
         VALUES (?1, 'drop@example.test', 1, 'drop')",
        vec![route.into()],
    )
    .await;
    db.close().await.unwrap();
    drop(db);

    let mut database = Database::open(root.path(), SHARDS).unwrap();
    assert_eq!(
        database.global_index_outbox_status().unwrap()[usize::from(shard)].active_consumers(),
        1
    );
    database
        .transition_global_index(index, GlobalIndexLifecycle::Dropping)
        .unwrap();
    database.remove_global_index(index).unwrap();
    let status = database.global_index_outbox_status().unwrap();
    assert_eq!(status[usize::from(shard)].active_consumers(), 0);
    assert_eq!(
        database
            .prune_global_index_outbox(shard, 16)
            .unwrap()
            .deleted_events(),
        1
    );
}

#[tokio::test]
async fn pruning_waits_for_every_active_index_consumer() {
    let root = tempfile::tempdir().unwrap();
    let email_index = setup(root.path());
    let mut database = Database::open(root.path(), SHARDS).unwrap();
    let table = database
        .catalog()
        .table("default", "accounts")
        .unwrap()
        .unwrap()
        .id();
    let note_index = database
        .create_global_index(
            GlobalIndexDeclaration::new(
                table,
                "accounts_note",
                vec![GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::column("note").unwrap(),
                    GlobalIndexKeyType::Text,
                )],
            )
            .unwrap()
            .with_topology(GlobalIndexStorageTopology::selected_v1()),
        )
        .unwrap();
    database.build_global_index(note_index).unwrap();
    let route = "outbox-two-consumers";
    let shard = database.shard_for_key(route.as_bytes());
    drop(database);

    let db = BriskDb::open(root.path()).await.unwrap();
    write(
        &db,
        route,
        "INSERT INTO accounts (tenant_id, email, active, note)
         VALUES (?1, 'two@example.test', 1, 'indexed-note')",
        vec![route.into()],
    )
    .await;
    db.close().await.unwrap();
    drop(db);

    let database = Database::open(root.path(), SHARDS).unwrap();
    database
        .advance_global_index_outbox(email_index, shard, GlobalIndexOutboxCursor::new(2))
        .unwrap();
    assert_eq!(
        database
            .prune_global_index_outbox(shard, 16)
            .unwrap()
            .deleted_events(),
        0
    );
    database
        .advance_global_index_outbox(note_index, shard, GlobalIndexOutboxCursor::new(2))
        .unwrap();
    assert_eq!(
        database
            .prune_global_index_outbox(shard, 16)
            .unwrap()
            .deleted_events(),
        2
    );
}

#[tokio::test]
async fn retention_backpressure_rolls_back_the_application_row() {
    let root = tempfile::tempdir().unwrap();
    setup(root.path());
    let first = "outbox-capacity-first";
    let mut candidate = 0_u64;
    let (second, shard) = {
        let database = Database::open(root.path(), SHARDS).unwrap();
        let shard = database.shard_for_key(first.as_bytes());
        let second = loop {
            let route = format!("outbox-capacity-second-{candidate}");
            candidate += 1;
            if database.shard_for_key(route.as_bytes()) == shard {
                break route;
            }
        };
        (second, shard)
    };
    let db = BriskDb::open(root.path()).await.unwrap();
    write(
        &db,
        first,
        "INSERT INTO accounts (tenant_id, email, active, note)
         VALUES (?1, 'first-capacity@example.test', 1, 'first')",
        vec![first.into()],
    )
    .await;
    db.close().await.unwrap();
    drop(db);

    let shard_path = root
        .path()
        .join("shards")
        .join(format!("{shard:04}.sqlite"));
    let physical = rusqlite::Connection::open(&shard_path).unwrap();
    physical
        .execute(
            "UPDATE briskdb_global_index_outbox_state
             SET retained_events = ?1 WHERE singleton = 1",
            [i64::try_from(MAX_GLOBAL_INDEX_OUTBOX_EVENTS_PER_SHARD).unwrap()],
        )
        .unwrap();
    drop(physical);

    let db = BriskDb::open(root.path()).await.unwrap();
    let session = db.session();
    session.set_routing_key(&second).await.unwrap();
    let error = db
        .execute_write(
            &session,
            Statement::new(
                "INSERT INTO accounts (tenant_id, email, active, note)
                 VALUES (?1, 'second-capacity@example.test', 1, 'second')",
                vec![second.clone().into()],
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    session.close().await.unwrap();
    db.close().await.unwrap();
    drop(db);

    let physical = rusqlite::Connection::open(shard_path).unwrap();
    assert_eq!(
        physical
            .query_row(
                "SELECT COUNT(*) FROM accounts WHERE tenant_id = ?1",
                [&second],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[cfg(unix)]
#[test]
fn global_index_outbox_process_child() {
    let Ok(root) = env::var("BRISKDB_OUTBOX_ROOT") else {
        return;
    };
    let route = env::var("BRISKDB_OUTBOX_ROUTE").unwrap();
    let ready = PathBuf::from(env::var("BRISKDB_OUTBOX_READY").unwrap());
    let go = PathBuf::from(env::var("BRISKDB_OUTBOX_GO").unwrap());
    fs::write(ready, b"ready").unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !go.exists() {
        assert!(Instant::now() < deadline, "timed out waiting for parent");
        thread::sleep(Duration::from_millis(10));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let db = BriskDb::open(root).await.unwrap();
        write(
            &db,
            &route,
            "INSERT INTO accounts (tenant_id, email, active, note)
             VALUES (?1, ?2, 1, 'process')",
            vec![route.clone().into(), format!("{route}@example.test").into()],
        )
        .await;
        db.close().await.unwrap();
    });
}

#[cfg(unix)]
#[test]
fn independent_processes_share_one_ordered_shard_cursor() {
    const WRITERS: usize = 6;
    let root = tempfile::tempdir().unwrap();
    let index = setup(root.path());
    let database = Database::open(root.path(), SHARDS).unwrap();
    let mut routes = Vec::new();
    for candidate in 0..10_000 {
        let route = format!("outbox-process-{candidate}");
        if database.shard_for_key(route.as_bytes()) == 0 {
            routes.push(route);
            if routes.len() == WRITERS {
                break;
            }
        }
    }
    assert_eq!(routes.len(), WRITERS);
    drop(database);

    let coordination = tempfile::tempdir().unwrap();
    let go = coordination.path().join("go");
    let executable = env::current_exe().unwrap();
    let children = routes
        .iter()
        .enumerate()
        .map(|(worker, route)| {
            let ready = coordination.path().join(format!("ready-{worker}"));
            let child = Command::new(&executable)
                .arg("--exact")
                .arg("global_index_outbox_process_child")
                .arg("--nocapture")
                .env("BRISKDB_OUTBOX_ROOT", root.path())
                .env("BRISKDB_OUTBOX_ROUTE", route)
                .env("BRISKDB_OUTBOX_READY", &ready)
                .env("BRISKDB_OUTBOX_GO", &go)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            (child, ready)
        })
        .collect::<Vec<_>>();
    let deadline = Instant::now() + Duration::from_secs(20);
    while children.iter().any(|(_, ready)| !ready.exists()) {
        assert!(Instant::now() < deadline, "children did not become ready");
        thread::sleep(Duration::from_millis(10));
    }
    fs::write(&go, b"go").unwrap();
    for (child, _) in children {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "child failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let database = Database::open(root.path(), SHARDS).unwrap();
    let batch = database
        .read_global_index_outbox(index, 0, GlobalIndexOutboxCursor::new(0), WRITERS)
        .unwrap();
    assert_eq!(batch.events().len(), WRITERS);
    assert_eq!(batch.high_water().get(), WRITERS as u64);
    assert_eq!(
        batch
            .events()
            .iter()
            .map(|event| event.cursor().get())
            .collect::<Vec<_>>(),
        (1..=WRITERS as u64).collect::<Vec<_>>()
    );
    let mut operation_ids = batch
        .events()
        .iter()
        .map(|event| event.operation_id())
        .collect::<Vec<_>>();
    operation_ids.sort_unstable();
    operation_ids.dedup();
    assert_eq!(operation_ids.len(), WRITERS);
}
