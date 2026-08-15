use std::{env, path::Path, process::Command, sync::Arc};

use briskdb::core::{
    Database, Engine, GlobalIndexAsyncOptions, GlobalIndexDeclaration, GlobalIndexHealthState,
    GlobalIndexId, GlobalIndexKeyPart, GlobalIndexKeySource, GlobalIndexKeyType,
    GlobalIndexStorageTopology, ShardKeyMetadata, ShardKeyType, Statement, TableDeclaration,
    UniqueNullSemantics, Value,
};
use tokio::runtime::{Builder, Runtime};

const SHARDS: u16 = 4;
const DEFAULT_OPERATIONS: usize = 64;
const CHILD_ROOT: &str = "BRISKDB_GLOBAL_INDEX_GATE_ROOT";
const CHILD_WORKER: &str = "BRISKDB_GLOBAL_INDEX_GATE_WORKER";
const CHILD_ROUTE: &str = "BRISKDB_GLOBAL_INDEX_GATE_ROUTE";
const CHILD_OPERATIONS: &str = "BRISKDB_GLOBAL_INDEX_GATE_OPERATIONS";

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(8)
        .enable_all()
        .build()
        .unwrap()
}

fn setup(root: &Path) -> (GlobalIndexId, GlobalIndexId, Vec<String>) {
    let mut database = Database::open(root, SHARDS).unwrap();
    database
        .broadcast(
            "CREATE TABLE gate_accounts (
                 tenant_id TEXT NOT NULL,
                 row_id INTEGER NOT NULL,
                 email TEXT NOT NULL,
                 tag TEXT NOT NULL,
                 PRIMARY KEY (tenant_id, row_id)
             ) STRICT",
        )
        .unwrap();
    let logical = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "gate_accounts",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    let table = database
        .catalog()
        .table("default", "gate_accounts")
        .unwrap()
        .unwrap()
        .id();
    let unique = database
        .create_global_index(
            GlobalIndexDeclaration::new(
                table,
                "gate_accounts_email_unique",
                vec![GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::column("email").unwrap(),
                    GlobalIndexKeyType::Text,
                )],
            )
            .unwrap()
            .unique(UniqueNullSemantics::Distinct)
            .with_topology(GlobalIndexStorageTopology::selected_v1()),
        )
        .unwrap();
    database.build_global_index(unique).unwrap();
    let lookup = database
        .create_global_index(
            GlobalIndexDeclaration::new(
                table,
                "gate_accounts_tag_lookup",
                vec![GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::column("tag").unwrap(),
                    GlobalIndexKeyType::Text,
                )],
            )
            .unwrap()
            .with_topology(GlobalIndexStorageTopology::selected_v1()),
        )
        .unwrap();
    database.build_global_index(lookup).unwrap();
    let mut routes = vec![None; usize::from(SHARDS)];
    for candidate in 0..100_000 {
        let key = format!("gate-tenant-{candidate}");
        let shard = usize::from(database.shard_for_key(key.as_bytes()));
        routes[shard].get_or_insert(key);
        if routes.iter().all(Option::is_some) {
            break;
        }
    }
    (
        unique,
        lookup,
        routes.into_iter().map(Option::unwrap).collect(),
    )
}

async fn run_worker(root: &Path, worker: usize, route: &str, operations: usize) {
    let engine = Engine::open(root, SHARDS).await.unwrap();
    let write_session = engine.session();
    write_session.set_routing_key(route).await.unwrap();
    let read_session = engine.session();
    for operation in 0..operations {
        let row_id = i64::try_from(worker * 1_000_000 + operation).unwrap();
        let first_email = format!("gate-{worker}-{operation}@before.test");
        let final_email = format!("gate-{worker}-{operation}@after.test");
        engine
            .execute_write(
                &write_session,
                Statement::new(
                    "INSERT INTO gate_accounts (tenant_id, row_id, email, tag)
                     VALUES (?1, ?2, ?3, ?4)",
                    vec![
                        route.into(),
                        row_id.into(),
                        first_email.clone().into(),
                        format!("tag-{operation}").into(),
                    ],
                ),
            )
            .await
            .unwrap();
        engine
            .execute_write(
                &write_session,
                Statement::new(
                    "UPDATE gate_accounts SET email = ?1, tag = ?2
                     WHERE tenant_id = ?3 AND row_id = ?4",
                    vec![
                        final_email.clone().into(),
                        format!("tag-updated-{operation}").into(),
                        route.into(),
                        row_id.into(),
                    ],
                ),
            )
            .await
            .unwrap();
        let found = engine
            .query_logical(
                &read_session,
                Statement::new(
                    "SELECT tenant_id FROM gate_accounts WHERE email = ?1",
                    vec![final_email.clone().into()],
                ),
            )
            .await
            .unwrap();
        assert_eq!(found.value.len(), 1);
        assert_eq!(found.shards.len(), 1);
        if operation % 2 == 0 {
            engine
                .execute_write(
                    &write_session,
                    Statement::new(
                        "DELETE FROM gate_accounts WHERE tenant_id = ?1 AND row_id = ?2",
                        vec![route.into(), row_id.into()],
                    ),
                )
                .await
                .unwrap();
            let deleted = engine
                .query_logical(
                    &read_session,
                    Statement::new(
                        "SELECT tenant_id FROM gate_accounts WHERE email = ?1",
                        vec![final_email.into()],
                    ),
                )
                .await
                .unwrap();
            assert!(deleted.value.is_empty());
            assert_eq!(deleted.shards.len(), 1);
        }
    }
    write_session.close().await.unwrap();
    read_session.close().await.unwrap();
    engine.shutdown().await.unwrap();
}

#[test]
#[ignore = "manual multi-process global-index soak for issue #239"]
fn global_index_release_soak() {
    let operations = env::var("BRISKDB_GLOBAL_INDEX_GATE_SOAK_OPERATIONS")
        .ok()
        .map(|value| value.parse().unwrap())
        .unwrap_or(DEFAULT_OPERATIONS);
    assert!(operations > 0 && operations % 2 == 0);
    let temp = tempfile::tempdir().unwrap();
    let (unique, lookup, routes) = setup(temp.path());

    runtime().block_on(run_worker(temp.path(), 0, &routes[0], operations));
    let executable = env::current_exe().unwrap();
    let mut children = Vec::new();
    for worker in 1..=SHARDS as usize {
        children.push(
            Command::new(&executable)
                .args(["--ignored", "--exact", "global_index_release_gate_child"])
                .env(CHILD_ROOT, temp.path())
                .env(CHILD_WORKER, worker.to_string())
                .env(CHILD_ROUTE, &routes[worker % routes.len()])
                .env(CHILD_OPERATIONS, operations.to_string())
                .spawn()
                .unwrap(),
        );
    }
    for mut child in children {
        assert!(child.wait().unwrap().success());
    }

    let database = Arc::new(Database::open(temp.path(), SHARDS).unwrap());
    while database.global_index_async_status(lookup).unwrap().lag() != 0 {
        database
            .process_global_index_async(lookup, GlobalIndexAsyncOptions::default())
            .unwrap();
    }
    let report = database.global_index_operational_report().unwrap();
    assert_eq!(report.state(), GlobalIndexHealthState::Healthy);
    let expected_rows = ((SHARDS as usize + 1) * operations / 2) as u64;
    for index in report.indexes() {
        assert_eq!(index.authority_entries(), expected_rows);
        assert_eq!(index.active_operations(), 0);
        assert_eq!(index.active_unique_reservations(), 0);
    }
    assert_eq!(
        report
            .indexes()
            .iter()
            .find(|index| index.index_id() == unique)
            .unwrap()
            .unique_keys(),
        expected_rows
    );

    let engine = Engine::from_database(database);
    let result = runtime().block_on(engine.query_logical(
        &engine.session(),
        Statement::new(
            "SELECT tenant_id FROM gate_accounts WHERE email = ?1",
            vec![Value::from("gate-4-1@after.test")],
        ),
    ));
    let result = result.unwrap();
    assert_eq!(result.value.len(), 1);
    assert_eq!(result.shards.len(), 1);
    runtime().block_on(engine.shutdown()).unwrap();
}

#[test]
#[ignore = "subprocess entrypoint for the issue #239 soak"]
fn global_index_release_gate_child() {
    let Ok(root) = env::var(CHILD_ROOT) else {
        return;
    };
    let worker = env::var(CHILD_WORKER).unwrap().parse().unwrap();
    let route = env::var(CHILD_ROUTE).unwrap();
    let operations = env::var(CHILD_OPERATIONS).unwrap().parse().unwrap();
    runtime().block_on(run_worker(Path::new(&root), worker, &route, operations));
}
