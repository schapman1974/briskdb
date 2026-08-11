use std::{collections::BTreeSet, sync::Arc};

use briskdb::{core, sql};

const SHARD_COUNT: u16 = 4;

struct Fixture {
    _temp: tempfile::TempDir,
    database: Arc<core::Database>,
    engine: core::Engine,
    keys: [i64; SHARD_COUNT as usize],
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let mut database = core::Database::open(temp.path(), SHARD_COUNT).unwrap();
        database
            .broadcast(
                "CREATE TABLE events (
                    tenant_id INTEGER NOT NULL,
                    event_id INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, event_id)
                 );
                 CREATE TABLE global_settings (
                    code TEXT PRIMARY KEY,
                    label TEXT NOT NULL
                 );",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                core::TableDeclaration::sharded(
                    logical_database,
                    "events",
                    core::ShardKeyMetadata::new("tenant_id", core::ShardKeyType::Int64).unwrap(),
                )
                .unwrap(),
                core::TableDeclaration::global(logical_database, "global_settings").unwrap(),
            ])
            .unwrap();

        // Global rows are deliberately replicated in every physical file. A
        // logical Global read must still visit only canonical shard zero.
        for shard in 0..SHARD_COUNT {
            let connection =
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap();
            connection
                .execute_batch(
                    "INSERT INTO global_settings (code, label)
                     VALUES ('CA', 'Canada'), ('US', 'United States')",
                )
                .unwrap();
        }

        let keys = std::array::from_fn(|shard| integer_key_for_shard(&database, shard as u16));
        let database = Arc::new(database);
        let engine = core::Engine::from_database(Arc::clone(&database));
        Self {
            _temp: temp,
            database,
            engine,
            keys,
        }
    }

    async fn seed_sharded_rows(&self) {
        let session = self.engine.session();
        for (shard, key) in self.keys.iter().copied().enumerate() {
            session.set_routing_key(key.to_string()).await.unwrap();
            let inserted = self
                .engine
                .execute(
                    &session,
                    core::Statement::new(
                        "INSERT INTO events (tenant_id, event_id, payload)
                         VALUES (?1, 1, 'shared'), (?1, 2, ?2)",
                        vec![
                            core::Value::from(key),
                            core::Value::from(format!("only-{shard}")),
                        ],
                    ),
                )
                .await
                .unwrap();
            assert_eq!(inserted.shard, shard as u16);
            assert_eq!(inserted.value, 2);
        }
    }
}

fn integer_key_for_shard(database: &core::Database, expected: u16) -> i64 {
    (1_i64..)
        .find(|value| database.shard_for_key(value.to_string().as_bytes()) == expected)
        .expect("the finite shard map has an integer key for every shard")
}

fn event_rows(result: &core::ResultSet) -> BTreeSet<(i64, i64, String)> {
    result
        .rows()
        .iter()
        .map(|row| {
            (
                row.get(0).and_then(core::Value::as_i64).unwrap(),
                row.get(1).and_then(core::Value::as_i64).unwrap(),
                row.get(2).and_then(core::Value::as_str).unwrap().to_owned(),
            )
        })
        .collect()
}

#[tokio::test]
async fn logical_reads_union_all_metadata_selected_files_and_keep_legacy_routing() {
    let fixture = Fixture::new();
    fixture.seed_sharded_rows().await;
    let session = fixture.engine.session();

    let all = fixture
        .engine
        .query_logical(
            &session,
            core::Statement::new("SELECT tenant_id, event_id, payload FROM events", vec![]),
        )
        .await
        .unwrap();
    assert_eq!(all.shards(), [0, 1, 2, 3]);

    let expected = fixture
        .keys
        .iter()
        .copied()
        .enumerate()
        .flat_map(|(shard, key)| {
            [
                (key, 1, "shared".to_owned()),
                (key, 2, format!("only-{shard}")),
            ]
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(all.value.len(), expected.len());
    assert_eq!(event_rows(&all.value), expected);

    let duplicates = fixture
        .engine
        .query_logical(
            &session,
            core::Statement::new(
                "SELECT payload FROM events WHERE event_id = ?1",
                vec![core::Value::from(1_i64)],
            ),
        )
        .await
        .unwrap();
    assert_eq!(duplicates.shards(), [0, 1, 2, 3]);
    assert_eq!(duplicates.value.len(), SHARD_COUNT as usize);
    assert!(
        duplicates
            .value
            .rows()
            .iter()
            .all(|row| { row.get(0).and_then(core::Value::as_str) == Some("shared") })
    );

    let exact_key = fixture.keys[2];
    let exact = fixture
        .engine
        .query_logical(
            &session,
            core::Statement::new(
                "SELECT tenant_id, event_id, payload FROM events WHERE tenant_id = ?1",
                vec![core::Value::from(exact_key)],
            ),
        )
        .await
        .unwrap();
    assert_eq!(exact.shards(), [2]);
    assert_eq!(exact.value.len(), 2);
    assert!(
        exact
            .value
            .rows()
            .iter()
            .all(|row| row.get(0).and_then(core::Value::as_i64) == Some(exact_key))
    );

    let finite = fixture
        .engine
        .query_logical(
            &session,
            core::Statement::new(
                "SELECT tenant_id, event_id, payload FROM events
                 WHERE tenant_id = ?1 OR tenant_id = ?2 OR tenant_id = ?3",
                vec![
                    core::Value::from(fixture.keys[3]),
                    core::Value::from(fixture.keys[1]),
                    core::Value::from(fixture.keys[3]),
                ],
            ),
        )
        .await
        .unwrap();
    assert_eq!(finite.shards(), [1, 3]);
    assert_eq!(finite.value.len(), 4);

    let global = fixture
        .engine
        .query_logical(
            &session,
            core::Statement::new(
                "SELECT code, label FROM global_settings ORDER BY code",
                vec![],
            ),
        )
        .await
        .unwrap();
    assert_eq!(global.shards(), [0]);
    assert_eq!(global.value.len(), 2, "replicas must not be unioned");

    // The established query API is still explicitly routed and keeps its
    // single-shard result shape.
    session
        .set_routing_key(exact_key.to_string())
        .await
        .unwrap();
    let routed = fixture
        .engine
        .query(
            &session,
            core::Statement::new(
                "SELECT tenant_id, event_id, payload FROM events WHERE tenant_id = ?1",
                vec![core::Value::from(exact_key)],
            ),
        )
        .await
        .unwrap();
    assert_eq!(routed.shard, 2);
    assert_eq!(routed.value.len(), 2);
}

#[tokio::test]
async fn unsafe_multi_shard_shapes_are_rejected_before_execution() {
    let fixture = Fixture::new();
    fixture.seed_sharded_rows().await;
    let session = fixture.engine.session();

    for statement in [
        "SELECT COUNT(*) FROM events",
        "SELECT tenant_id FROM events ORDER BY tenant_id",
        "SELECT tenant_id FROM events LIMIT 1",
        "SELECT DISTINCT payload FROM events",
        "SELECT a.tenant_id FROM events AS a JOIN events AS b ON b.tenant_id = a.tenant_id",
        "SELECT tenant_id FROM (SELECT tenant_id FROM events) AS nested",
    ] {
        let error = fixture
            .engine
            .query_logical(&session, core::Statement::new(statement, vec![]))
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            core::EngineErrorKind::Unsupported,
            "unexpected rejection for {statement:?}: {}",
            error.diagnostic()
        );
    }
}

#[tokio::test]
async fn scatter_has_one_shared_row_and_byte_budget_and_recovers_after_failure() {
    let fixture = Fixture::new();
    fixture.seed_sharded_rows().await;
    let session = fixture.engine.session();
    let statement = || {
        core::Statement::new(
            "SELECT tenant_id AS v FROM events
             WHERE (tenant_id = ?1 OR tenant_id = ?2) AND event_id = ?3",
            vec![
                core::Value::from(fixture.keys[0]),
                core::Value::from(fixture.keys[1]),
                core::Value::from(1_i64),
            ],
        )
    };

    // Metadata is 26 logical bytes and each integer row is 25. The logical
    // two-shard result is therefore exactly two rows and 76 bytes.
    let exact = fixture
        .engine
        .query_logical_with_context(
            &session,
            statement(),
            core::RequestContext::new().with_result_limits(core::ResultLimits::new(2, 76).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(exact.shards(), [0, 1]);
    assert_eq!(exact.value.len(), 2);

    let row_error = fixture
        .engine
        .query_logical_with_context(
            &session,
            statement(),
            core::RequestContext::new().with_result_limits(core::ResultLimits::new(1, 76).unwrap()),
        )
        .await
        .unwrap_err();
    assert_eq!(row_error.kind(), core::EngineErrorKind::LimitExceeded);
    assert!(row_error.diagnostic().contains("row limit"));

    let byte_error = fixture
        .engine
        .query_logical_with_context(
            &session,
            statement(),
            core::RequestContext::new().with_result_limits(core::ResultLimits::new(2, 75).unwrap()),
        )
        .await
        .unwrap_err();
    assert_eq!(byte_error.kind(), core::EngineErrorKind::LimitExceeded);
    assert!(byte_error.diagnostic().contains("logical byte limit"));

    let recovered = fixture
        .engine
        .query_logical(&session, statement())
        .await
        .unwrap();
    assert_eq!(recovered.shards(), [0, 1]);
    assert_eq!(recovered.value.len(), 2);
}

#[tokio::test]
async fn prepared_logical_portal_fans_out_with_the_same_union_all_semantics() {
    let fixture = Fixture::new();
    fixture.seed_sharded_rows().await;
    let session = fixture.engine.session();
    let logical_database = fixture.database.catalog().default_database().id();

    let statement = fixture
        .engine
        .prepare_statement(
            &session,
            core::PrepareRequest::new(
                logical_database,
                sql::SqlDialect::PostgreSql,
                sql::SqlTranslationMode::Compatibility,
                "SELECT tenant_id, event_id, payload FROM events WHERE payload = $1",
            ),
        )
        .await
        .unwrap();
    let portal = fixture
        .engine
        .bind_statement(&session, statement, vec![core::Value::from("shared")])
        .await
        .unwrap();

    let executed = fixture
        .engine
        .execute_portal_logical(&session, portal)
        .await
        .unwrap();
    assert_eq!(executed.shards(), [0, 1, 2, 3]);
    let core::PreparedExecution::Rows(rows) = executed.value else {
        panic!("logical read portal must return rows")
    };
    assert_eq!(rows.len(), SHARD_COUNT as usize);
    assert!(
        rows.rows()
            .iter()
            .all(|row| row.get(2).and_then(core::Value::as_str) == Some("shared"))
    );
}
