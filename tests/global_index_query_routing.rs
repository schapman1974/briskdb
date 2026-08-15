use std::{fs, path::Path, sync::Arc};

use briskdb::core::{Database, Engine, ShardKeyMetadata, ShardKeyType, TableDeclaration};
use briskdb::{
    CanonicalIndexKey, GlobalIndexDeclaration, GlobalIndexKeyPart, GlobalIndexKeySource,
    GlobalIndexKeyType, GlobalIndexOwner, GlobalIndexRoutingFallback, GlobalIndexRoutingKind,
    GlobalIndexStorageTopology, GlobalOperationId, GlobalUniqueMutation, Statement,
    UniqueNullSemantics, Value,
};

fn route_for_each_shard(database: &Database) -> Vec<String> {
    let mut routes = vec![None; usize::from(database.shard_count())];
    for value in 0_u64..100_000 {
        let route = format!("tenant-{value}");
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

fn setup(root: &Path) -> (Arc<Database>, Engine, Vec<String>, briskdb::GlobalIndexId) {
    let mut database = Database::open(root, 4).unwrap();
    database
        .broadcast(
            "CREATE TABLE indexed_users (
                 tenant_id TEXT NOT NULL,
                 email TEXT NOT NULL,
                 region TEXT NOT NULL,
                 payload TEXT NOT NULL,
                 PRIMARY KEY (tenant_id, email),
                 UNIQUE (tenant_id, email, region)
             ) STRICT",
        )
        .unwrap();
    let logical = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "indexed_users",
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
                "INSERT INTO indexed_users (tenant_id, email, region, payload)
                 VALUES (?1, ?2, ?3, ?4)",
                &[
                    route.clone().into(),
                    format!("user-{shard}@example.test").into(),
                    if shard % 2 == 0 { "east" } else { "west" }.into(),
                    format!("payload-{shard}").into(),
                ],
            )
            .unwrap();
    }
    let table = database
        .catalog()
        .table("default", "indexed_users")
        .unwrap()
        .unwrap()
        .id();
    let email_index = database
        .create_global_index(
            GlobalIndexDeclaration::new(
                table,
                "indexed_users_email_unique",
                vec![GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::column("email").unwrap(),
                    GlobalIndexKeyType::Text,
                )],
            )
            .unwrap()
            .unique(UniqueNullSemantics::NotDistinct)
            .with_topology(GlobalIndexStorageTopology::selected_v1()),
        )
        .unwrap();
    database.build_global_index(email_index).unwrap();
    let compound = database
        .create_global_index(
            GlobalIndexDeclaration::new(
                table,
                "indexed_users_email_region_unique",
                vec![
                    GlobalIndexKeyPart::new(
                        GlobalIndexKeySource::column("email").unwrap(),
                        GlobalIndexKeyType::Text,
                    ),
                    GlobalIndexKeyPart::new(
                        GlobalIndexKeySource::column("region").unwrap(),
                        GlobalIndexKeyType::Text,
                    ),
                ],
            )
            .unwrap()
            .unique(UniqueNullSemantics::NotDistinct)
            .with_topology(GlobalIndexStorageTopology::selected_v1()),
        )
        .unwrap();
    database.build_global_index(compound).unwrap();
    let database = Arc::new(database);
    let engine = Engine::from_database(Arc::clone(&database));
    (database, engine, routes, email_index)
}

#[tokio::test]
async fn exact_in_compound_and_shard_key_intersection_route_only_possible_owners() {
    let temp = tempfile::tempdir().unwrap();
    let (_database, engine, routes, _) = setup(temp.path());
    let logical = engine.catalog().default_database().id();

    let equality = normalized("SELECT payload FROM indexed_users WHERE email = ?1");
    let plan = engine
        .plan_bound_statement(logical, &equality, 0, &["user-2@example.test".into()], None)
        .unwrap();
    assert_eq!(
        plan.global_index_routing().kind(),
        GlobalIndexRoutingKind::Routed
    );
    assert_eq!(plan.global_index_routing().target_shards(), &[2]);
    assert_eq!(plan.global_index_routing().lookup_key_count(), 1);
    assert_eq!(plan.global_index_routing().candidate_count(), 1);

    let compound = normalized(
        "SELECT payload FROM indexed_users
         WHERE email IN (?1, ?2) AND region = ?3",
    );
    let plan = engine
        .plan_bound_statement(
            logical,
            &compound,
            0,
            &[
                "user-0@example.test".into(),
                "user-2@example.test".into(),
                "east".into(),
            ],
            None,
        )
        .unwrap();
    assert_eq!(
        plan.global_index_routing().index_name(),
        Some("indexed_users_email_region_unique")
    );
    assert_eq!(plan.global_index_routing().target_shards(), &[0, 2]);

    let intersection = normalized(
        "SELECT payload FROM indexed_users
         WHERE tenant_id = ?1 AND email IN (?2, ?3)",
    );
    let plan = engine
        .plan_bound_statement(
            logical,
            &intersection,
            0,
            &[
                routes[0].clone().into(),
                "user-0@example.test".into(),
                "user-2@example.test".into(),
            ],
            None,
        )
        .unwrap();
    assert_eq!(plan.global_index_routing().target_shards(), &[0]);

    let session = engine.session();
    let result = engine
        .query_logical(
            &session,
            Statement::new(
                "SELECT payload FROM indexed_users WHERE email IN (?1, ?2)",
                vec!["user-0@example.test".into(), "user-2@example.test".into()],
            ),
        )
        .await
        .unwrap();
    assert_eq!(result.shards, vec![0, 2]);
    assert_eq!(result.value.len(), 2);
}

#[tokio::test]
async fn miss_active_write_and_unavailable_storage_remain_conservative() {
    let temp = tempfile::tempdir().unwrap();
    let (database, engine, _, email_index) = setup(temp.path());
    let logical = engine.catalog().default_database().id();
    let equality = normalized("SELECT payload FROM indexed_users WHERE email = ?1");

    let miss = engine
        .plan_bound_statement(
            logical,
            &equality,
            0,
            &["missing@example.test".into()],
            None,
        )
        .unwrap();
    assert_eq!(
        miss.global_index_routing().kind(),
        GlobalIndexRoutingKind::Empty
    );
    assert!(miss.global_index_routing().target_shards().is_empty());

    let operation = GlobalOperationId::new(91_u128.to_le_bytes()).unwrap();
    database
        .reserve_global_unique(
            operation,
            &GlobalUniqueMutation::claim(
                email_index,
                CanonicalIndexKey::encode_values(&[Value::from("pending@example.test")]).unwrap(),
                GlobalIndexOwner::new(3, b"pending-owner".to_vec()).unwrap(),
            ),
        )
        .unwrap();
    let pending = engine
        .plan_bound_statement(
            logical,
            &equality,
            0,
            &["pending@example.test".into()],
            None,
        )
        .unwrap();
    assert_eq!(pending.global_index_routing().target_shards(), &[3]);
    database.rollback_global_unique(operation).unwrap();

    let old_key = CanonicalIndexKey::encode_values(&[Value::from("old@example.test")]).unwrap();
    let new_key = CanonicalIndexKey::encode_values(&[Value::from("new@example.test")]).unwrap();
    let old_owner = GlobalIndexOwner::new(1, b"old-owner".to_vec()).unwrap();
    let new_owner = GlobalIndexOwner::new(3, b"new-owner".to_vec()).unwrap();
    let seed_operation = GlobalOperationId::new(92_u128.to_le_bytes()).unwrap();
    database
        .reserve_global_unique(
            seed_operation,
            &GlobalUniqueMutation::claim(email_index, old_key.clone(), old_owner.clone()),
        )
        .unwrap();
    database.finalize_global_unique(seed_operation).unwrap();
    let replace_operation = GlobalOperationId::new(93_u128.to_le_bytes()).unwrap();
    database
        .reserve_global_unique(
            replace_operation,
            &GlobalUniqueMutation::replace(email_index, old_key, old_owner, new_key, new_owner),
        )
        .unwrap();
    for email in ["old@example.test", "new@example.test"] {
        let replacing = engine
            .plan_bound_statement(logical, &equality, 0, &[email.into()], None)
            .unwrap();
        assert_eq!(replacing.global_index_routing().target_shards(), &[1, 3]);
    }
    database.rollback_global_unique(replace_operation).unwrap();

    let session = engine.session();
    let indexed = engine
        .query_logical(
            &session,
            Statement::new(
                "SELECT tenant_id, payload FROM indexed_users WHERE email = ?1",
                vec!["user-1@example.test".into()],
            ),
        )
        .await
        .unwrap();
    assert_eq!(indexed.shards, vec![1]);

    let authority = temp.path().join("global-indexes/global.sqlite");
    let unavailable = temp.path().join("global-indexes/global.sqlite.unavailable");
    fs::rename(&authority, &unavailable).unwrap();
    let fallback = engine
        .plan_bound_statement(logical, &equality, 0, &["user-1@example.test".into()], None)
        .unwrap();
    assert_eq!(
        fallback.global_index_routing().kind(),
        GlobalIndexRoutingKind::Fallback
    );
    assert_eq!(
        fallback.global_index_routing().fallback_reason(),
        Some(GlobalIndexRoutingFallback::IndexUnavailable)
    );
    assert_eq!(
        fallback.global_index_routing().target_shards(),
        &[0, 1, 2, 3]
    );
    let scattered = engine
        .query_logical(
            &session,
            Statement::new(
                "SELECT tenant_id, payload FROM indexed_users WHERE email = ?1",
                vec!["user-1@example.test".into()],
            ),
        )
        .await
        .unwrap();
    fs::rename(&unavailable, &authority).unwrap();
    assert_eq!(scattered.shards, vec![0, 1, 2, 3]);
    assert_eq!(scattered.value, indexed.value);
}
