use std::{path::Path, sync::Arc, time::Instant};

use briskdb::core::{
    CancellationToken, Database, Engine, EngineErrorKind, RequestContext, ShardKeyMetadata,
    ShardKeyType, TableDeclaration,
};
use briskdb::{
    CanonicalIndexKey, GlobalIndexDeclaration, GlobalIndexKeyPart, GlobalIndexKeySource,
    GlobalIndexKeyType, GlobalIndexRoutingFallback, GlobalIndexRoutingKind,
    GlobalIndexStorageTopology, Statement, Value,
};
use proptest::prelude::*;
use rusqlite::{Connection, params};

const SHARED_EMAIL: &str = "shared@example.test";

fn route_for_each_shard(database: &Database) -> Vec<String> {
    let mut routes = vec![None; usize::from(database.shard_count())];
    for value in 0_u64..100_000 {
        let route = format!("candidate-tenant-{value}");
        let shard = usize::from(database.shard_for_key(route.as_bytes()));
        routes[shard].get_or_insert(route);
        if routes.iter().all(Option::is_some) {
            return routes.into_iter().map(Option::unwrap).collect();
        }
    }
    panic!("failed to find one candidate-verification route per shard");
}

fn normalized(dialect: briskdb::SqlDialect, source: &str) -> briskdb::sql::NormalizedSql {
    let parsed = briskdb::sql::parse(dialect, source).unwrap();
    let common = briskdb::sql::validate_common_subset(parsed).unwrap();
    briskdb::sql::normalize_placeholders(common).unwrap()
}

fn setup(root: &Path) -> (Arc<Database>, Engine, Vec<String>, briskdb::GlobalIndexId) {
    let mut database = Database::open(root, 4).unwrap();
    database
        .broadcast(
            "CREATE TABLE candidate_users (
                 tenant_id TEXT NOT NULL,
                 email TEXT NOT NULL,
                 payload TEXT NOT NULL,
                 PRIMARY KEY (tenant_id, email)
             ) STRICT",
        )
        .unwrap();
    let logical = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "candidate_users",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    let routes = route_for_each_shard(&database);
    for (shard, route) in routes.iter().enumerate() {
        database
            .execute(
                route,
                "INSERT INTO candidate_users (tenant_id, email, payload)
                 VALUES (?1, ?2, ?3)",
                &[
                    route.clone().into(),
                    SHARED_EMAIL.into(),
                    if shard == 0 { "keep" } else { "reject" }.into(),
                ],
            )
            .unwrap();
    }
    database
        .execute(
            &routes[0],
            "INSERT INTO candidate_users (tenant_id, email, payload)
             VALUES (?1, 'invalid@example.test', 'keep')",
            &[routes[0].clone().into()],
        )
        .unwrap();
    let table = database
        .catalog()
        .table("default", "candidate_users")
        .unwrap()
        .unwrap()
        .id();
    let index = database
        .create_global_index(
            GlobalIndexDeclaration::new(
                table,
                "candidate_users_email",
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

fn inject_stale_candidates(
    root: &Path,
    database: &Database,
    routes: &[String],
    index: briskdb::GlobalIndexId,
) {
    database
        .execute(
            &routes[2],
            "DELETE FROM candidate_users WHERE tenant_id = ?1 AND email = ?2",
            &[routes[2].clone().into(), SHARED_EMAIL.into()],
        )
        .unwrap();
    database
        .execute(
            &routes[3],
            "UPDATE candidate_users SET email = 'moved@example.test'
             WHERE tenant_id = ?1 AND email = ?2",
            &[routes[3].clone().into(), SHARED_EMAIL.into()],
        )
        .unwrap();

    let shared = CanonicalIndexKey::encode_values(&[Value::from(SHARED_EMAIL)]).unwrap();
    let invalid = CanonicalIndexKey::encode_values(&[Value::from("invalid@example.test")]).unwrap();
    Connection::open(root.join("global-indexes/global.sqlite"))
        .unwrap()
        .execute(
            "UPDATE briskdb_global_index_entries
             SET encoded_key = ?1, source_locator = x'00'
             WHERE index_id = ?2 AND encoded_key = ?3",
            params![shared.as_bytes(), index.get() as i64, invalid.as_bytes()],
        )
        .unwrap();
}

#[tokio::test]
async fn stale_candidates_are_verified_repaired_and_never_change_query_results() {
    let temp = tempfile::tempdir().unwrap();
    let (database, engine, routes, index) = setup(temp.path());
    inject_stale_candidates(temp.path(), &database, &routes, index);
    let logical = engine.catalog().default_database().id();
    let query = normalized(
        briskdb::SqlDialect::Sqlite,
        "SELECT tenant_id, payload FROM candidate_users AS u
         WHERE u.email = ?1 AND u.payload = ?2",
    );
    let plan = engine
        .plan_bound_statement(
            logical,
            &query,
            0,
            &[SHARED_EMAIL.into(), "keep".into()],
            None,
        )
        .unwrap();
    let explain = plan.global_index_routing();
    assert_eq!(explain.kind(), GlobalIndexRoutingKind::Fallback);
    assert_eq!(
        explain.fallback_reason(),
        Some(GlobalIndexRoutingFallback::FreshnessUnproven)
    );
    assert!(!explain.authoritative());
    assert_eq!(explain.candidate_count(), 5);
    assert_eq!(explain.verified_candidate_count(), 1);
    assert_eq!(explain.rejected_candidate_count(), 1);
    assert_eq!(explain.stale_candidate_count(), 3);
    assert_eq!(explain.repairs_queued(), 3);
    assert_eq!(explain.repairs_applied(), 3);
    assert_eq!(explain.repairs_deferred(), 0);
    assert_eq!(explain.candidate_shards(), &[0]);
    assert_eq!(explain.target_shards(), &[0, 1, 2, 3]);

    let session = engine.session();
    let indexed = engine
        .query_logical(
            &session,
            Statement::new(
                "SELECT tenant_id, payload FROM candidate_users
                 WHERE email = ?1 AND payload = ?2",
                vec![SHARED_EMAIL.into(), "keep".into()],
            ),
        )
        .await
        .unwrap();
    let forced = engine
        .query_logical(
            &session,
            Statement::new(
                "SELECT tenant_id, payload FROM candidate_users
                 WHERE (email = ?1 AND payload = ?2) OR payload = 'never-match'",
                vec![SHARED_EMAIL.into(), "keep".into()],
            ),
        )
        .await
        .unwrap();
    assert_eq!(indexed.shards, vec![0, 1, 2, 3]);
    assert_eq!(indexed.value, forced.value);
    assert_eq!(indexed.value.len(), 1);

    let moved = engine
        .query_logical(
            &session,
            Statement::new(
                "SELECT tenant_id FROM candidate_users WHERE email = ?1",
                vec!["moved@example.test".into()],
            ),
        )
        .await
        .unwrap();
    assert_eq!(moved.shards, vec![0, 1, 2, 3]);
    assert_eq!(moved.value.len(), 1);

    let repair_database =
        Connection::open(temp.path().join("global-indexes/global.sqlite")).unwrap();
    let (repairs, minimum_observations) = repair_database
        .query_row(
            "SELECT COUNT(*), MIN(observation_count)
             FROM briskdb_global_index_read_repairs
             WHERE index_id = ?1 AND repair_state = 2",
            [index.get() as i64],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .unwrap();
    assert_eq!(repairs, 3);
    assert_eq!(minimum_observations, 1);

    let postgres = normalized(
        briskdb::SqlDialect::PostgreSql,
        "SELECT tenant_id FROM candidate_users AS u
         WHERE u.email = $1 AND u.payload = $2",
    );
    let postgres_plan = engine
        .plan_bound_statement(
            logical,
            &postgres,
            0,
            &[SHARED_EMAIL.into(), "keep".into()],
            None,
        )
        .unwrap();
    assert_eq!(
        postgres_plan.global_index_routing().fallback_reason(),
        Some(GlobalIndexRoutingFallback::FreshnessUnproven)
    );
}

#[test]
fn excessive_candidates_fall_back_before_physical_verification() {
    let temp = tempfile::tempdir().unwrap();
    let (_database, engine, _, index) = setup(temp.path());
    let shared = CanonicalIndexKey::encode_values(&[Value::from(SHARED_EMAIL)]).unwrap();
    let mut authority = Connection::open(temp.path().join("global-indexes/global.sqlite")).unwrap();
    let transaction = authority.transaction().unwrap();
    let first_ordinal = transaction
        .query_row(
            "SELECT COALESCE(MAX(source_ordinal), -1) + 1
             FROM briskdb_global_index_entries
             WHERE index_id = ?1 AND source_shard = 0",
            [index.get() as i64],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    {
        let mut insert = transaction
            .prepare(
                "INSERT INTO briskdb_global_index_entries (
                     index_id, encoded_key, source_shard, source_ordinal, source_locator
                 ) VALUES (?1, ?2, 0, ?3, ?4)",
            )
            .unwrap();
        for offset in 0_i64..4_097 {
            insert
                .execute(params![
                    index.get() as i64,
                    shared.as_bytes(),
                    first_ordinal + offset,
                    offset.to_be_bytes().to_vec(),
                ])
                .unwrap();
        }
    }
    transaction.commit().unwrap();

    let logical = engine.catalog().default_database().id();
    let query = normalized(
        briskdb::SqlDialect::Sqlite,
        "SELECT tenant_id FROM candidate_users WHERE email = ?1",
    );
    let plan = engine
        .plan_bound_statement(logical, &query, 0, &[SHARED_EMAIL.into()], None)
        .unwrap();
    assert_eq!(
        plan.global_index_routing().fallback_reason(),
        Some(GlobalIndexRoutingFallback::TooManyCandidates)
    );
    assert!(plan.global_index_routing().candidate_count() > 4_096);
    assert_eq!(plan.global_index_routing().target_shards(), &[0, 1, 2, 3]);
    assert_eq!(plan.global_index_routing().repairs_queued(), 0);
}

#[tokio::test]
async fn cancelled_and_expired_candidate_queries_stop_before_execution_and_recover() {
    let temp = tempfile::tempdir().unwrap();
    let (_database, engine, _, _) = setup(temp.path());
    let session = engine.session();
    let statement = || {
        Statement::new(
            "SELECT tenant_id FROM candidate_users WHERE email = ?1",
            vec![SHARED_EMAIL.into()],
        )
    };

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = engine
        .query_logical_with_context(
            &session,
            statement(),
            RequestContext::new().with_cancellation_token(cancellation),
        )
        .await
        .unwrap_err();
    assert_eq!(cancelled.kind(), EngineErrorKind::Cancelled);

    let expired = engine
        .query_logical_with_context(
            &session,
            statement(),
            RequestContext::new().with_deadline(Instant::now()),
        )
        .await
        .unwrap_err();
    assert_eq!(expired.kind(), EngineErrorKind::DeadlineExceeded);

    assert_eq!(
        engine
            .query_logical(&session, statement())
            .await
            .unwrap()
            .value
            .len(),
        4
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]

    #[test]
    fn indexed_reads_match_forced_scans_under_staleness(
        email_choice in 0_usize..4,
        payload_choice in 0_usize..3,
    ) {
        let emails = [
            SHARED_EMAIL,
            "moved@example.test",
            "invalid@example.test",
            "missing@example.test",
        ];
        let payloads = ["keep", "reject", "missing"];
        let temp = tempfile::tempdir().unwrap();
        let (database, engine, routes, index) = setup(temp.path());
        inject_stale_candidates(temp.path(), &database, &routes, index);
        let session = engine.session();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (indexed, forced) = runtime.block_on(async {
            let parameters = vec![emails[email_choice].into(), payloads[payload_choice].into()];
            let indexed = engine
                .query_logical(
                    &session,
                    Statement::new(
                        "SELECT tenant_id, email, payload FROM candidate_users
                         WHERE email = ?1 AND payload = ?2",
                        parameters.clone(),
                    ),
                )
                .await
                .unwrap();
            let forced = engine
                .query_logical(
                    &session,
                    Statement::new(
                        "SELECT tenant_id, email, payload FROM candidate_users
                         WHERE (email = ?1 AND payload = ?2)
                            OR tenant_id = '__forced_scatter_never_matches__'",
                        parameters,
                    ),
                )
                .await
                .unwrap();
            (indexed, forced)
        });
        prop_assert_eq!(indexed.value, forced.value);
        prop_assert_eq!(indexed.shards, vec![0, 1, 2, 3]);
    }
}
