use std::{collections::BTreeSet, path::Path, sync::Arc};

use briskdb::core::{
    Database, Engine, EngineErrorKind, GlobalIndexRoutingKind, ShardKeyMetadata, ShardKeyType,
    ShardSummaryPredicateKind, ShardSummaryPruningReason, TableDeclaration,
};
use briskdb::{
    CancellationToken, GlobalIndexDeclaration, GlobalIndexId, GlobalIndexKeyPart,
    GlobalIndexKeySource, GlobalIndexKeyType, GlobalIndexShardSummaryState,
    GlobalIndexStorageTopology, IndexKeyCollation, Statement, Value,
};
use proptest::{
    prop_assert,
    test_runner::{Config, TestRunner},
};
use rusqlite::params;

struct Fixture {
    database: Arc<Database>,
    engine: Engine,
    routes: Vec<String>,
    email_index: GlobalIndexId,
    age_index: GlobalIndexId,
}

fn route_for_each_shard(database: &Database) -> Vec<String> {
    let mut routes = vec![None; usize::from(database.shard_count())];
    for value in 0_u64..100_000 {
        let route = format!("summary-tenant-{value}");
        let shard = usize::from(database.shard_for_key(route.as_bytes()));
        routes[shard].get_or_insert(route);
        if routes.iter().all(Option::is_some) {
            return routes.into_iter().map(Option::unwrap).collect();
        }
    }
    panic!("failed to find one route per shard");
}

fn normalized(sql: &str) -> briskdb::sql::NormalizedSql {
    let parsed = briskdb::sql::parse(briskdb::SqlDialect::Sqlite, sql).unwrap();
    let common = briskdb::sql::validate_common_subset(parsed).unwrap();
    briskdb::sql::normalize_placeholders(common).unwrap()
}

fn setup(root: &Path) -> Fixture {
    let mut database = Database::open(root, 4).unwrap();
    database
        .broadcast(
            "CREATE TABLE summary_users (
                 tenant_id TEXT NOT NULL PRIMARY KEY,
                 email TEXT NOT NULL,
                 age INTEGER
             ) STRICT",
        )
        .unwrap();
    let logical = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "summary_users",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    let routes = route_for_each_shard(&database);
    for (shard, route) in routes.iter().enumerate() {
        let age = match shard {
            0 => Value::Null,
            _ => Value::Int64((shard as i64 + 1) * 10),
        };
        database
            .execute(
                route,
                "INSERT INTO summary_users (tenant_id, email, age) VALUES (?1, ?2, ?3)",
                &[
                    route.clone().into(),
                    format!("user-{shard}@example.test").into(),
                    age,
                ],
            )
            .unwrap();
    }
    let table = database
        .catalog()
        .table("default", "summary_users")
        .unwrap()
        .unwrap()
        .id();
    let email_index = database
        .create_global_index(
            GlobalIndexDeclaration::new(
                table,
                "summary_users_email",
                vec![GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::column("email").unwrap(),
                    GlobalIndexKeyType::Text,
                )],
            )
            .unwrap()
            .with_topology(GlobalIndexStorageTopology::selected_v1()),
        )
        .unwrap();
    let age_index = database
        .create_global_index(
            GlobalIndexDeclaration::new(
                table,
                "summary_users_age",
                vec![GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::column("age").unwrap(),
                    GlobalIndexKeyType::Int64,
                )],
            )
            .unwrap()
            .with_topology(GlobalIndexStorageTopology::selected_v1()),
        )
        .unwrap();
    database.build_global_index(email_index).unwrap();
    database.build_global_index(age_index).unwrap();
    let database = Arc::new(database);
    let engine = Engine::from_database(Arc::clone(&database));
    Fixture {
        database,
        engine,
        routes,
        email_index,
        age_index,
    }
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
async fn bloom_prunes_equality_and_in_without_hiding_lagged_rows() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = setup(temp.path());
    for (shard, route) in fixture.routes.iter().enumerate() {
        write(
            &fixture.engine,
            route,
            "INSERT INTO summary_users (tenant_id, email, age) VALUES (?1, ?2, ?3)",
            vec![
                route_for_suffix(&fixture.database, shard as u16, shard),
                format!("lag-{shard}@example.test").into(),
                Value::Int64(100 + shard as i64),
            ],
        )
        .await;
    }

    let logical = fixture.engine.catalog().default_database().id();
    let equality = fixture
        .engine
        .plan_bound_statement(
            logical,
            &normalized("SELECT tenant_id FROM summary_users WHERE email = ?1"),
            0,
            &["user-2@example.test".into()],
            None,
        )
        .unwrap();
    assert_eq!(
        equality.shard_summary_routing().predicate_kind(),
        Some(ShardSummaryPredicateKind::Equality)
    );
    assert!(equality.global_index_routing().target_shards().contains(&2));
    assert!(equality.shard_summary_routing().pruned_shard_count() >= 2);
    assert!(
        equality
            .shard_summary_routing()
            .pruned_shards()
            .iter()
            .all(|pruned| pruned.reason() == ShardSummaryPruningReason::BloomMiss)
    );
    assert!(
        equality
            .shard_summary_routing()
            .estimated_false_positive_rate_ppm()
            .is_some()
    );

    let in_plan = fixture
        .engine
        .plan_bound_statement(
            logical,
            &normalized("SELECT tenant_id FROM summary_users WHERE email IN (?1, ?2, ?3)"),
            0,
            &[
                "user-1@example.test".into(),
                "user-3@example.test".into(),
                "absent@example.test".into(),
            ],
            None,
        )
        .unwrap();
    assert!(in_plan.global_index_routing().target_shards().contains(&1));
    assert!(in_plan.global_index_routing().target_shards().contains(&3));
    assert!(!in_plan.global_index_routing().target_shards().contains(&0));
    assert!(!in_plan.global_index_routing().target_shards().contains(&2));

    let result = fixture
        .engine
        .query_logical(
            &fixture.engine.session(),
            Statement::new(
                "SELECT tenant_id FROM summary_users WHERE email = ?1",
                vec!["user-2@example.test".into()],
            ),
        )
        .await
        .unwrap();
    assert_eq!(result.value.len(), 1);
    assert!(result.shards.contains(&2));
}

#[test]
fn typed_min_max_prunes_ranges_and_nulls_fall_back_conservatively() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = setup(temp.path());
    let logical = fixture.engine.catalog().default_database().id();
    let plan = fixture
        .engine
        .plan_bound_statement(
            logical,
            &normalized("SELECT tenant_id FROM summary_users WHERE age >= ?1 AND age < ?2"),
            0,
            &[Value::Int64(25), Value::Int64(35)],
            None,
        )
        .unwrap();
    assert_eq!(
        plan.shard_summary_routing().predicate_kind(),
        Some(ShardSummaryPredicateKind::Range)
    );
    assert_eq!(
        plan.global_index_routing().kind(),
        GlobalIndexRoutingKind::Routed
    );
    assert!(plan.global_index_routing().target_shards().contains(&2));
    assert!(!plan.global_index_routing().target_shards().contains(&1));
    assert!(!plan.global_index_routing().target_shards().contains(&3));
    assert!(
        plan.shard_summary_routing()
            .pruned_shards()
            .iter()
            .any(|pruned| matches!(
                pruned.reason(),
                ShardSummaryPruningReason::MaximumBelowLowerBound
                    | ShardSummaryPruningReason::MinimumAboveUpperBound
            ))
    );
    assert!(plan.shard_summary_routing().observed_pruning_rate_ppm() > 0);

    let shard_key_intersection = fixture
        .engine
        .plan_bound_statement(
            logical,
            &normalized("SELECT tenant_id FROM summary_users WHERE tenant_id = ?1 AND age > ?2"),
            0,
            &[fixture.routes[1].clone().into(), Value::Int64(1_000)],
            None,
        )
        .unwrap();
    assert_eq!(
        shard_key_intersection
            .shard_summary_routing()
            .examined_shards(),
        1
    );
    assert_eq!(
        shard_key_intersection
            .shard_summary_routing()
            .pruned_shard_count(),
        1
    );
    assert!(
        shard_key_intersection
            .global_index_routing()
            .target_shards()
            .is_empty()
    );

    let null_semantics = fixture
        .engine
        .plan_bound_statement(
            logical,
            &normalized("SELECT tenant_id FROM summary_users WHERE age IS NULL"),
            0,
            &[],
            None,
        )
        .unwrap();
    assert!(
        null_semantics
            .shard_summary_routing()
            .predicate_kind()
            .is_none()
    );
    assert_eq!(
        null_semantics.global_index_routing().target_shards(),
        &[0, 1, 2, 3]
    );

    let unsupported_type = fixture
        .engine
        .plan_bound_statement(
            logical,
            &normalized("SELECT tenant_id FROM summary_users WHERE age > ?1"),
            0,
            &["25".into()],
            None,
        )
        .unwrap();
    assert!(
        unsupported_type
            .shard_summary_routing()
            .predicate_kind()
            .is_none()
    );
    assert_eq!(
        unsupported_type.global_index_routing().target_shards(),
        &[0, 1, 2, 3]
    );

    let collation_error = IndexKeyCollation::from_name("NOCASE").unwrap_err();
    assert_eq!(collation_error.kind(), EngineErrorKind::Unsupported);
}

#[tokio::test]
async fn additions_are_transactional_deletes_are_conservative_and_rebuild_compacts() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = setup(temp.path());
    let route = &fixture.routes[1];
    let additions_before = fixture
        .database
        .global_index_shard_summary_status(fixture.email_index)
        .unwrap()
        .shards()[1]
        .additions();
    let failed = fixture.engine.session();
    failed.set_routing_key(route).await.unwrap();
    assert!(
        fixture
            .engine
            .execute_write(
                &failed,
                Statement::new(
                    "INSERT INTO summary_users (tenant_id, email, age) VALUES (?1, ?2, ?3)",
                    vec![
                        route.clone().into(),
                        "rolled-back@example.test".into(),
                        Value::Int64(99),
                    ],
                ),
            )
            .await
            .is_err()
    );
    failed.close().await.unwrap();
    assert_eq!(
        fixture
            .database
            .global_index_shard_summary_status(fixture.email_index)
            .unwrap()
            .shards()[1]
            .additions(),
        additions_before
    );

    write(
        &fixture.engine,
        route,
        "UPDATE summary_users SET email = 'moved@example.test' WHERE tenant_id = ?1",
        vec![route.clone().into()],
    )
    .await;
    let logical = fixture.engine.catalog().default_database().id();
    let moved = fixture
        .engine
        .plan_bound_statement(
            logical,
            &normalized("SELECT tenant_id FROM summary_users WHERE email = ?1"),
            0,
            &["moved@example.test".into()],
            None,
        )
        .unwrap();
    assert!(moved.global_index_routing().target_shards().contains(&1));

    write(
        &fixture.engine,
        route,
        "DELETE FROM summary_users WHERE tenant_id = ?1",
        vec![route.clone().into()],
    )
    .await;
    let before = fixture
        .database
        .global_index_shard_summary_status(fixture.email_index)
        .unwrap();
    assert!(before.shards()[1].additions() >= 1);
    let deleted_but_retained = fixture
        .engine
        .plan_bound_statement(
            logical,
            &normalized("SELECT tenant_id FROM summary_users WHERE email = ?1"),
            0,
            &["moved@example.test".into()],
            None,
        )
        .unwrap();
    assert!(
        deleted_but_retained
            .global_index_routing()
            .target_shards()
            .contains(&1)
    );

    let report = fixture
        .database
        .rebuild_global_index_shard_summaries(fixture.email_index)
        .unwrap();
    assert_eq!(report.rebuilt_shards(), 4);
    let compacted = fixture
        .engine
        .plan_bound_statement(
            logical,
            &normalized("SELECT tenant_id FROM summary_users WHERE email = ?1"),
            0,
            &["moved@example.test".into()],
            None,
        )
        .unwrap();
    assert!(
        !compacted
            .global_index_routing()
            .target_shards()
            .contains(&1)
    );
}

#[tokio::test]
async fn stale_saturated_corrupt_and_cancelled_states_never_exclude_their_shard() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = setup(temp.path());
    let shard_path = temp.path().join("shards/0002.sqlite");
    let connection = rusqlite::Connection::open(&shard_path).unwrap();
    connection
        .execute(
            "UPDATE briskdb_global_index_shard_summaries
             SET saturated = 1 WHERE index_id = ?1",
            [fixture.email_index.get() as i64],
        )
        .unwrap();
    drop(connection);
    let status = fixture
        .database
        .global_index_shard_summary_status(fixture.email_index)
        .unwrap();
    assert!(status.shards()[2].is_saturated());
    let authority = temp.path().join("global-indexes/global.sqlite");
    let unavailable = temp.path().join("global-indexes/global.sqlite.unavailable");
    std::fs::rename(&authority, &unavailable).unwrap();
    let saturated = fixture
        .engine
        .plan_bound_statement(
            fixture.engine.catalog().default_database().id(),
            &normalized("SELECT tenant_id FROM summary_users WHERE email = ?1"),
            0,
            &["definitely-absent@example.test".into()],
            None,
        )
        .unwrap();
    std::fs::rename(&unavailable, &authority).unwrap();
    assert_eq!(saturated.global_index_routing().target_shards(), &[2]);

    let building_path = temp.path().join("shards/0001.sqlite");
    let connection = rusqlite::Connection::open(&building_path).unwrap();
    connection
        .execute(
            "UPDATE briskdb_global_index_shard_summaries
             SET summary_state = 1 WHERE index_id = ?1",
            [fixture.age_index.get() as i64],
        )
        .unwrap();
    drop(connection);
    write(
        &fixture.engine,
        &fixture.routes[1],
        "INSERT INTO summary_users (tenant_id, email, age) VALUES (?1, ?2, ?3)",
        vec![
            route_for_suffix(&fixture.database, 1, 91),
            "during-rebuild@example.test".into(),
            Value::Int64(2_000),
        ],
    )
    .await;
    let interrupted = fixture
        .engine
        .plan_bound_statement(
            fixture.engine.catalog().default_database().id(),
            &normalized("SELECT tenant_id FROM summary_users WHERE age > ?1"),
            0,
            &[Value::Int64(1_000)],
            None,
        )
        .unwrap();
    assert_eq!(interrupted.global_index_routing().target_shards(), &[1]);
    fixture
        .database
        .rebuild_global_index_shard_summaries(fixture.age_index)
        .unwrap();
    let rebuilt = fixture
        .engine
        .plan_bound_statement(
            fixture.engine.catalog().default_database().id(),
            &normalized("SELECT tenant_id FROM summary_users WHERE age > ?1"),
            0,
            &[Value::Int64(1_000)],
            None,
        )
        .unwrap();
    assert!(rebuilt.global_index_routing().target_shards().contains(&1));

    let connection = rusqlite::Connection::open(&shard_path).unwrap();
    connection
        .execute_batch("PRAGMA ignore_check_constraints = ON")
        .unwrap();
    connection
        .execute(
            "UPDATE briskdb_global_index_shard_summaries
             SET format_version = 999 WHERE index_id = ?1",
            [fixture.age_index.get() as i64],
        )
        .unwrap();
    drop(connection);

    let plan = fixture
        .engine
        .plan_bound_statement(
            fixture.engine.catalog().default_database().id(),
            &normalized("SELECT tenant_id FROM summary_users WHERE age > ?1"),
            0,
            &[Value::Int64(1_000)],
            None,
        )
        .unwrap();
    // Shard 1 really matches; incompatible shard 2 is conservatively retained.
    assert_eq!(plan.global_index_routing().target_shards(), &[1, 2]);
    let status = fixture
        .database
        .global_index_shard_summary_status(fixture.age_index)
        .unwrap();
    assert_eq!(
        status.shards()[2].state(),
        GlobalIndexShardSummaryState::Incompatible
    );

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = fixture
        .database
        .rebuild_global_index_shard_summaries_with_cancellation(fixture.age_index, &cancellation)
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Cancelled);

    fixture
        .database
        .rebuild_global_index_shard_summaries(fixture.age_index)
        .unwrap();
    assert_eq!(
        fixture
            .database
            .global_index_shard_summary_status(fixture.age_index)
            .unwrap()
            .ready_shards(),
        4
    );
}

#[test]
fn range_pruning_property_never_excludes_a_matching_physical_shard() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = setup(temp.path());
    let logical = fixture.engine.catalog().default_database().id();
    let query = normalized("SELECT tenant_id FROM summary_users WHERE age >= ?1 AND age <= ?2");
    let mut runner = TestRunner::new(Config {
        cases: 256,
        ..Config::default()
    });
    runner
        .run(
            &(proptest::num::i64::ANY, proptest::num::i64::ANY),
            |(a, b)| {
                let lower = a.min(b);
                let upper = a.max(b);
                let plan = fixture
                    .engine
                    .plan_bound_statement(
                        logical,
                        &query,
                        0,
                        &[Value::Int64(lower), Value::Int64(upper)],
                        None,
                    )
                    .unwrap();
                let targets = plan
                    .global_index_routing()
                    .target_shards()
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>();
                for shard in 0..4_u16 {
                    let connection = rusqlite::Connection::open(
                        temp.path()
                            .join("shards")
                            .join(format!("{shard:04}.sqlite")),
                    )
                    .unwrap();
                    let matches = connection
                        .query_row(
                            "SELECT EXISTS(
                             SELECT 1 FROM summary_users WHERE age >= ?1 AND age <= ?2
                         )",
                            params![lower, upper],
                            |row| row.get::<_, bool>(0),
                        )
                        .unwrap();
                    prop_assert!(!matches || targets.contains(&shard));
                }
                Ok(())
            },
        )
        .unwrap();
}

fn route_for_suffix(database: &Database, shard: u16, suffix: usize) -> Value {
    for value in 100_000_u64..1_000_000 {
        let route = format!("summary-extra-{suffix}-{value}");
        if database.shard_for_key(route.as_bytes()) == shard {
            return route.into();
        }
    }
    panic!("failed to find an extra route for shard {shard}");
}
