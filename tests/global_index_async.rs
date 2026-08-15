use std::{path::Path, sync::Arc, time::Duration};

#[cfg(unix)]
use std::{fs, process::Command};

use briskdb::core::{
    Database, Engine, EngineErrorKind, GlobalIndexAsyncOptions, GlobalIndexRoutingKind,
    ShardKeyMetadata, ShardKeyType, TableDeclaration,
};
use briskdb::{
    GlobalIndexDeclaration, GlobalIndexKeyPart, GlobalIndexKeySource, GlobalIndexKeyType,
    GlobalIndexStorageTopology, Statement, Value,
};
use proptest::prelude::*;

fn routes(database: &Database) -> Vec<String> {
    let mut routes = vec![None; usize::from(database.shard_count())];
    for value in 0_u64..100_000 {
        let route = format!("async-tenant-{value}");
        let shard = usize::from(database.shard_for_key(route.as_bytes()));
        routes[shard].get_or_insert(route);
        if routes.iter().all(Option::is_some) {
            return routes.into_iter().map(Option::unwrap).collect();
        }
    }
    panic!("failed to find one route per shard");
}

fn setup(root: &Path) -> (Arc<Database>, Engine, Vec<String>, briskdb::GlobalIndexId) {
    setup_with_shards(root, 2)
}

fn setup_with_shards(
    root: &Path,
    shard_count: u16,
) -> (Arc<Database>, Engine, Vec<String>, briskdb::GlobalIndexId) {
    let mut database = Database::open(root, shard_count).unwrap();
    database
        .broadcast(
            "CREATE TABLE async_users (
                 tenant_id TEXT NOT NULL PRIMARY KEY,
                 email TEXT NOT NULL
             ) STRICT",
        )
        .unwrap();
    let logical = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "async_users",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    let routes = routes(&database);
    let table = database
        .catalog()
        .table("default", "async_users")
        .unwrap()
        .unwrap()
        .id();
    let index = database
        .create_global_index(
            GlobalIndexDeclaration::new(
                table,
                "async_users_email",
                vec![GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::column("email").unwrap(),
                    GlobalIndexKeyType::Text,
                )],
            )
            .unwrap()
            .with_topology(GlobalIndexStorageTopology::selected_v1()),
        )
        .unwrap();
    database.build_global_index(index).unwrap();
    let database = Arc::new(database);
    let engine = Engine::from_database(Arc::clone(&database));
    (database, engine, routes, index)
}

fn normalized(source: &str) -> briskdb::sql::NormalizedSql {
    let parsed = briskdb::sql::parse(briskdb::SqlDialect::Sqlite, source).unwrap();
    let common = briskdb::sql::validate_common_subset(parsed).unwrap();
    briskdb::sql::normalize_placeholders(common).unwrap()
}

async fn write(engine: &Engine, route: &str, sql: &str, parameters: Vec<Value>) {
    let session = engine.session();
    session.set_routing_key(route).await.unwrap();
    engine
        .execute_write(&session, Statement::new(sql, parameters))
        .await
        .unwrap();
    session.close().await.unwrap();
}

#[tokio::test]
async fn bounded_replay_makes_insert_update_delete_fresh_and_prunes_shards() {
    let temp = tempfile::tempdir().unwrap();
    let (database, engine, routes, index) = setup(temp.path());
    let options = GlobalIndexAsyncOptions::new(1, 5_000, 5).unwrap();

    for (shard, route) in routes.iter().enumerate() {
        write(
            &engine,
            route,
            "INSERT INTO async_users (tenant_id, email) VALUES (?1, ?2)",
            vec![
                route.clone().into(),
                format!("user-{shard}@example.test").into(),
            ],
        )
        .await;
    }
    assert!(database.global_index_async_status(index).unwrap().lag() >= 2);

    let logical = engine.catalog().default_database().id();
    let query = normalized("SELECT tenant_id FROM async_users WHERE email = ?1");
    let lagged = engine
        .plan_bound_statement(
            logical,
            &query,
            0,
            &[Value::from("user-0@example.test")],
            None,
        )
        .unwrap();
    assert_eq!(
        lagged.global_index_routing().kind(),
        GlobalIndexRoutingKind::Fallback
    );

    let report = database.process_global_index_async(index, options).unwrap();
    assert_eq!(report.applied_events(), 2);
    assert!(
        database
            .global_index_async_status(index)
            .unwrap()
            .is_fresh()
    );

    let fresh = engine
        .plan_bound_statement(
            logical,
            &query,
            0,
            &[Value::from("user-0@example.test")],
            None,
        )
        .unwrap();
    assert_eq!(
        fresh.global_index_routing().kind(),
        GlobalIndexRoutingKind::Routed
    );
    assert_eq!(fresh.global_index_routing().target_shards(), &[0]);
    let result = engine
        .query_logical(
            &engine.session(),
            Statement::new(
                "SELECT tenant_id FROM async_users WHERE email = ?1",
                vec![Value::from("user-0@example.test")],
            ),
        )
        .await
        .unwrap();
    assert_eq!(result.shards, vec![0]);
    assert_eq!(result.value.len(), 1);

    write(
        &engine,
        &routes[0],
        "UPDATE async_users SET email = 'moved@example.test' WHERE tenant_id = ?1",
        vec![routes[0].clone().into()],
    )
    .await;
    write(
        &engine,
        &routes[1],
        "DELETE FROM async_users WHERE tenant_id = ?1",
        vec![routes[1].clone().into()],
    )
    .await;
    assert_eq!(
        database
            .process_global_index_async(index, options)
            .unwrap()
            .applied_events(),
        2
    );
    assert!(
        database
            .global_index_async_status(index)
            .unwrap()
            .is_fresh()
    );

    let old = engine
        .query_logical(
            &engine.session(),
            Statement::new(
                "SELECT tenant_id FROM async_users WHERE email = ?1",
                vec![Value::from("user-0@example.test")],
            ),
        )
        .await
        .unwrap();
    assert!(old.value.is_empty());
    let moved = engine
        .query_logical(
            &engine.session(),
            Statement::new(
                "SELECT tenant_id FROM async_users WHERE email = ?1",
                vec![Value::from("moved@example.test")],
            ),
        )
        .await
        .unwrap();
    assert_eq!(moved.shards, vec![0]);
    assert_eq!(moved.value.len(), 1);
}

#[tokio::test]
async fn lag_scans_only_uncertain_shards_beside_verified_candidates() {
    let temp = tempfile::tempdir().unwrap();
    let (database, engine, routes, index) = setup_with_shards(temp.path(), 4);
    let options = GlobalIndexAsyncOptions::new(64, 5_000, 5).unwrap();
    write(
        &engine,
        &routes[0],
        "INSERT INTO async_users (tenant_id, email) VALUES (?1, 'candidate@example.test')",
        vec![routes[0].clone().into()],
    )
    .await;
    database.process_global_index_async(index, options).unwrap();
    write(
        &engine,
        &routes[3],
        "INSERT INTO async_users (tenant_id, email) VALUES (?1, 'lagging@example.test')",
        vec![routes[3].clone().into()],
    )
    .await;

    let plan = engine
        .plan_bound_statement(
            engine.catalog().default_database().id(),
            &normalized("SELECT tenant_id FROM async_users WHERE email = ?1"),
            0,
            &[Value::from("candidate@example.test")],
            None,
        )
        .unwrap();
    assert_eq!(
        plan.global_index_routing().kind(),
        GlobalIndexRoutingKind::Routed
    );
    assert_eq!(plan.global_index_routing().candidate_shards(), &[0]);
    assert_eq!(plan.global_index_routing().uncertain_shards(), &[3]);
    assert_eq!(plan.global_index_routing().target_shards(), &[0, 3]);
}

#[test]
fn legacy_raw_writes_force_safe_scanning_until_rebuild() {
    let temp = tempfile::tempdir().unwrap();
    let (database, engine, routes, index) = setup(temp.path());
    assert_eq!(
        database
            .execute(
                &routes[0],
                "INSERT INTO async_users (tenant_id, email) VALUES (?1, ?2)",
                &[routes[0].clone().into(), "bypass@example.test".into()],
            )
            .unwrap(),
        1
    );
    let status = database.global_index_async_status(index).unwrap();
    assert!(status.rebuild_required());
    assert!(!status.is_fresh());
    let plan = engine
        .plan_bound_statement(
            engine.catalog().default_database().id(),
            &normalized("SELECT tenant_id FROM async_users WHERE email = ?1"),
            0,
            &[Value::from("bypass@example.test")],
            None,
        )
        .unwrap();
    assert_eq!(
        plan.global_index_routing().kind(),
        GlobalIndexRoutingKind::Fallback
    );
    assert_eq!(plan.global_index_routing().target_shards(), &[0, 1]);
}

#[tokio::test]
async fn managed_worker_honors_pause_resume_and_catches_up() {
    let temp = tempfile::tempdir().unwrap();
    let (database, engine, routes, index) = setup(temp.path());
    let options = GlobalIndexAsyncOptions::new(64, 500, 5).unwrap();
    database.pause_global_index_async(index).unwrap();
    let mut worker = database.start_global_index_worker(options).unwrap();
    write(
        &engine,
        &routes[0],
        "INSERT INTO async_users (tenant_id, email) VALUES (?1, 'paused@example.test')",
        vec![routes[0].clone().into()],
    )
    .await;
    std::thread::sleep(Duration::from_millis(30));
    let paused = database.global_index_async_status(index).unwrap();
    assert!(paused.is_paused());
    assert!(paused.lag() > 0);

    database.resume_global_index_async(index).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !database
        .global_index_async_status(index)
        .unwrap()
        .is_fresh()
    {
        assert!(
            std::time::Instant::now() < deadline,
            "worker did not catch up"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!worker.stop());
    assert!(worker.is_finished());
}

#[cfg(unix)]
#[test]
fn global_index_async_process_child() {
    let Ok(root) = std::env::var("BRISKDB_ASYNC_CHILD_ROOT") else {
        return;
    };
    let index = briskdb::GlobalIndexId::new(
        std::env::var("BRISKDB_ASYNC_CHILD_INDEX")
            .unwrap()
            .parse()
            .unwrap(),
    )
    .unwrap();
    let start = Path::new(&root).join("async-child-start");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !start.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "child start timed out"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    let database = Database::open(&root, 2).unwrap();
    let options = GlobalIndexAsyncOptions::new(64, 100, 5).unwrap();
    if let Ok(stop) = std::env::var("BRISKDB_ASYNC_CHILD_STOP") {
        while !Path::new(&stop).exists() {
            if let Err(error) = database.process_global_index_async(index, options) {
                assert_eq!(error.kind(), EngineErrorKind::Busy);
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    if let Err(error) = database.process_global_index_async(index, options) {
        assert_eq!(error.kind(), EngineErrorKind::Busy);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn independent_processes_race_safely_and_replay_once() {
    let temp = tempfile::tempdir().unwrap();
    let (database, engine, routes, index) = setup(temp.path());
    write(
        &engine,
        &routes[0],
        "INSERT INTO async_users (tenant_id, email) VALUES (?1, 'race@example.test')",
        vec![routes[0].clone().into()],
    )
    .await;
    let executable = std::env::current_exe().unwrap();
    let mut children = (0..2)
        .map(|_| {
            Command::new(&executable)
                .args(["--exact", "global_index_async_process_child", "--nocapture"])
                .env("BRISKDB_ASYNC_CHILD_ROOT", temp.path())
                .env("BRISKDB_ASYNC_CHILD_INDEX", index.get().to_string())
                .spawn()
                .unwrap()
        })
        .collect::<Vec<_>>();
    fs::write(temp.path().join("async-child-start"), b"start\n").unwrap();
    for child in &mut children {
        assert!(child.wait().unwrap().success());
    }
    let status = database.global_index_async_status(index).unwrap();
    assert!(status.is_fresh());
    assert_eq!(
        status
            .shards()
            .iter()
            .map(|shard| shard.applied_events())
            .sum::<u64>(),
        1
    );
}

#[cfg(unix)]
#[tokio::test]
async fn independent_consumers_overlap_source_writes_and_converge() {
    let temp = tempfile::tempdir().unwrap();
    let (database, engine, routes, index) = setup(temp.path());
    let executable = std::env::current_exe().unwrap();
    let stop = temp.path().join("async-child-stop");
    let mut children = (0..2)
        .map(|_| {
            Command::new(&executable)
                .args(["--exact", "global_index_async_process_child", "--nocapture"])
                .env("BRISKDB_ASYNC_CHILD_ROOT", temp.path())
                .env("BRISKDB_ASYNC_CHILD_INDEX", index.get().to_string())
                .env("BRISKDB_ASYNC_CHILD_STOP", &stop)
                .spawn()
                .unwrap()
        })
        .collect::<Vec<_>>();
    fs::write(temp.path().join("async-child-start"), b"start\n").unwrap();

    for route in &routes {
        write(
            &engine,
            route,
            "INSERT INTO async_users (tenant_id, email) VALUES (?1, ?2)",
            vec![route.clone().into(), format!("initial-{route}").into()],
        )
        .await;
    }
    for revision in 0..20 {
        let route = &routes[revision % routes.len()];
        write(
            &engine,
            route,
            "UPDATE async_users SET email = ?2 WHERE tenant_id = ?1",
            vec![
                route.clone().into(),
                format!("revision-{revision}@example.test").into(),
            ],
        )
        .await;
    }
    fs::write(&stop, b"stop\n").unwrap();
    for child in &mut children {
        assert!(child.wait().unwrap().success());
    }

    std::thread::sleep(Duration::from_millis(120));
    let options = GlobalIndexAsyncOptions::new(64, 100, 5).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !database
        .global_index_async_status(index)
        .unwrap()
        .is_fresh()
    {
        database.process_global_index_async(index, options).unwrap();
        assert!(
            std::time::Instant::now() < deadline,
            "replay did not converge"
        );
    }
    let status = database.global_index_async_status(index).unwrap();
    assert_eq!(
        status
            .shards()
            .iter()
            .map(|shard| shard.applied_events())
            .sum::<u64>(),
        22
    );
    for (shard, revision) in [18, 19].into_iter().enumerate() {
        let result = engine
            .query_logical(
                &engine.session(),
                Statement::new(
                    "SELECT tenant_id FROM async_users WHERE email = ?1",
                    vec![Value::from(format!("revision-{revision}@example.test"))],
                ),
            )
            .await
            .unwrap();
        assert_eq!(result.shards, vec![u16::try_from(shard).unwrap()]);
        assert_eq!(result.value.len(), 1);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(6))]

    #[test]
    fn indexed_results_match_forced_scatter_through_lag_and_replay(
        operations in prop::collection::vec((0_u8..4, 0_u8..3, 0_u8..4, any::<bool>()), 1..16),
    ) {
        let temp = tempfile::tempdir().unwrap();
        let (database, engine, routes, index) = setup_with_shards(temp.path(), 4);
        let options = GlobalIndexAsyncOptions::new(3, 5_000, 5).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut present = [false; 4];
            for (shard, action, email, drain) in operations {
                let route = &routes[usize::from(shard)];
                match action {
                    0 if present[usize::from(shard)] => write(
                            &engine,
                            route,
                            "UPDATE async_users SET email = ?2 WHERE tenant_id = ?1",
                            vec![route.clone().into(), format!("property-{email}@example.test").into()],
                        ).await,
                    0 => {
                        write(
                            &engine,
                            route,
                            "INSERT INTO async_users (tenant_id, email) VALUES (?1, ?2)",
                            vec![route.clone().into(), format!("property-{email}@example.test").into()],
                        ).await;
                        present[usize::from(shard)] = true;
                    }
                    1 => {
                        write(
                            &engine,
                            route,
                            "DELETE FROM async_users WHERE tenant_id = ?1",
                            vec![route.clone().into()],
                        ).await;
                        present[usize::from(shard)] = false;
                    }
                    _ => write(
                        &engine,
                        route,
                        "UPDATE async_users SET email = ?2 WHERE tenant_id = ?1",
                        vec![route.clone().into(), format!("property-{email}@example.test").into()],
                    ).await,
                }
                if drain {
                    database.process_global_index_async(index, options).unwrap();
                }
            }
            for email in 0..5 {
                let value = format!("property-{email}@example.test");
                let indexed = engine
                    .query_logical(
                        &engine.session(),
                        Statement::new(
                            "SELECT tenant_id, email FROM async_users WHERE email = ?1",
                            vec![value.clone().into()],
                        ),
                    )
                    .await
                    .unwrap();
                let forced = engine
                    .query_logical(
                        &engine.session(),
                        Statement::new(
                            "SELECT tenant_id, email FROM async_users
                             WHERE email = ?1 OR tenant_id = '__force_scatter_never__'",
                            vec![value.into()],
                        ),
                    )
                    .await
                    .unwrap();
                prop_assert_eq!(indexed.value, forced.value);
            }
            Ok(())
        })?;
    }
}
