//! Bound-value-aware, protocol-neutral statement routing plans.

use std::{collections::HashMap, fmt, sync::Arc};

use crate::sql::{
    NormalizedSql, ShardKeyInference, ShardKeyInferenceKind, ShardKeyValue, infer_shard_keys,
};

use super::{Catalog, EngineError, EngineErrorKind, EngineResult, LogicalDatabaseId, Value};

/// One owned canonical routing key and its selected physical shard.
#[derive(Clone, PartialEq, Eq)]
pub struct PlannedRoute {
    key_bytes: Arc<[u8]>,
    shard: u16,
}

impl PlannedRoute {
    /// Return the canonical version-1 routing-key bytes.
    pub fn key_bytes(&self) -> &[u8] {
        &self.key_bytes
    }

    /// Return the physical shard selected by the current bucket map.
    pub const fn shard(&self) -> u16 {
        self.shard
    }
}

impl fmt::Debug for PlannedRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlannedRoute")
            .field("key_bytes", &"<redacted>")
            .field("shard", &self.shard)
            .finish()
    }
}

/// Owned routing metadata produced from one statement's actual bound values.
///
/// Inferred routes remain aligned one-for-one with
/// [`ShardKeyInference::values`], including duplicate `INSERT` row values. An
/// explicit route is retained independently so the later write-policy layer
/// can compare it with inferred routes. This plan does not choose between
/// those sources, reject a plan, translate SQL, or execute anything.
#[derive(Clone, PartialEq, Eq)]
pub struct BoundStatementPlan {
    database: LogicalDatabaseId,
    schema_generation: u64,
    hash_version: u32,
    key_encoding_version: u32,
    bucket_algorithm_version: u32,
    map_generation: u64,
    statement_index: usize,
    inference: ShardKeyInference,
    inferred_routes: Vec<PlannedRoute>,
    explicit_route: Option<PlannedRoute>,
}

impl BoundStatementPlan {
    /// Return the logical database used for catalog resolution.
    pub const fn database(&self) -> LogicalDatabaseId {
        self.database
    }

    /// Return the application-schema generation observed while planning.
    pub const fn schema_generation(&self) -> u64 {
        self.schema_generation
    }

    /// Return the routing hash version used to select physical shards.
    pub const fn hash_version(&self) -> u32 {
        self.hash_version
    }

    /// Return the canonical routing-key encoding version used by this plan.
    pub const fn key_encoding_version(&self) -> u32 {
        self.key_encoding_version
    }

    /// Return the virtual-bucket selection algorithm version.
    pub const fn bucket_algorithm_version(&self) -> u32 {
        self.bucket_algorithm_version
    }

    /// Return the immutable routing-map generation used by this plan.
    pub const fn map_generation(&self) -> u64 {
        self.map_generation
    }

    /// Return the zero-based top-level statement index.
    pub const fn statement_index(&self) -> usize {
        self.statement_index
    }

    /// Return the typed shard-key inference retained by this plan.
    pub const fn inference(&self) -> &ShardKeyInference {
        &self.inference
    }

    /// Return one inferred route per inferred value, in matching order.
    pub fn inferred_routes(&self) -> &[PlannedRoute] {
        &self.inferred_routes
    }

    /// Return the independently planned explicit routing fallback, if supplied.
    pub const fn explicit_route(&self) -> Option<&PlannedRoute> {
        self.explicit_route.as_ref()
    }
}

impl fmt::Debug for BoundStatementPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundStatementPlan")
            .field("database", &self.database)
            .field("schema_generation", &self.schema_generation)
            .field("hash_version", &self.hash_version)
            .field("key_encoding_version", &self.key_encoding_version)
            .field("bucket_algorithm_version", &self.bucket_algorithm_version)
            .field("map_generation", &self.map_generation)
            .field("statement_index", &self.statement_index)
            .field("inference", &self.inference)
            .field("inferred_route_count", &self.inferred_routes.len())
            .field("has_explicit_route", &self.explicit_route.is_some())
            .finish()
    }
}

pub(super) struct BoundStatementPlanInput<'a> {
    catalog: &'a Catalog,
    database: LogicalDatabaseId,
    normalized: &'a NormalizedSql,
    statement_index: usize,
    parameters: &'a [Value],
    explicit_routing_key: Option<&'a [u8]>,
}

impl<'a> BoundStatementPlanInput<'a> {
    pub(super) const fn new(
        catalog: &'a Catalog,
        database: LogicalDatabaseId,
        normalized: &'a NormalizedSql,
        statement_index: usize,
        parameters: &'a [Value],
        explicit_routing_key: Option<&'a [u8]>,
    ) -> Self {
        Self {
            catalog,
            database,
            normalized,
            statement_index,
            parameters,
            explicit_routing_key,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct RoutingProvenance {
    hash_version: u32,
    key_encoding_version: u32,
    bucket_algorithm_version: u32,
    map_generation: u64,
}

impl RoutingProvenance {
    pub(super) const fn new(
        hash_version: u32,
        key_encoding_version: u32,
        bucket_algorithm_version: u32,
        map_generation: u64,
    ) -> Self {
        Self {
            hash_version,
            key_encoding_version,
            bucket_algorithm_version,
            map_generation,
        }
    }
}

pub(super) fn plan_bound_statement<F>(
    input: BoundStatementPlanInput<'_>,
    provenance: RoutingProvenance,
    mut shard_for_key: F,
) -> EngineResult<BoundStatementPlan>
where
    F: FnMut(&[u8]) -> u16,
{
    let BoundStatementPlanInput {
        catalog,
        database,
        normalized,
        statement_index,
        parameters,
        explicit_routing_key,
    } = input;
    let schema_generation = catalog.schema_generation();
    let inference = infer_shard_keys(catalog, database, normalized, statement_index, parameters)?;
    validate_inference_shape(&inference)?;

    let mut unique_routes = HashMap::<&ShardKeyValue, PlannedRoute>::new();
    let mut inferred_routes = Vec::with_capacity(inference.values().len());
    for value in inference.values() {
        if let Some(route) = unique_routes.get(value) {
            inferred_routes.push(route.clone());
            continue;
        }
        let key_bytes = canonical_key_bytes(value);
        let route = PlannedRoute {
            shard: shard_for_key(&key_bytes),
            key_bytes,
        };
        unique_routes.insert(value, route.clone());
        inferred_routes.push(route);
    }
    drop(unique_routes);

    let explicit_route = explicit_routing_key.map(|key| PlannedRoute {
        shard: shard_for_key(key),
        key_bytes: Arc::from(key),
    });

    Ok(BoundStatementPlan {
        database,
        schema_generation,
        hash_version: provenance.hash_version,
        key_encoding_version: provenance.key_encoding_version,
        bucket_algorithm_version: provenance.bucket_algorithm_version,
        map_generation: provenance.map_generation,
        statement_index,
        inference,
        inferred_routes,
        explicit_route,
    })
}

fn validate_inference_shape(inference: &ShardKeyInference) -> EngineResult<()> {
    let valid = match inference.kind() {
        ShardKeyInferenceKind::Exact | ShardKeyInferenceKind::Multiple => {
            !inference.values().is_empty()
        }
        ShardKeyInferenceKind::NotApplicable
        | ShardKeyInferenceKind::NotSharded
        | ShardKeyInferenceKind::Unconstrained
        | ShardKeyInferenceKind::Contradiction => inference.values().is_empty(),
    };
    if valid {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::Internal,
            "shard-key inference is inconsistent during bound statement planning",
        ))
    }
}

fn canonical_key_bytes(value: &ShardKeyValue) -> Arc<[u8]> {
    match value {
        ShardKeyValue::Int64(value) => Arc::from(value.to_string().into_bytes()),
        ShardKeyValue::Text(value) => Arc::from(value.as_bytes()),
        ShardKeyValue::Binary(value) => Arc::from(value.as_slice()),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use super::*;
    use crate::{
        core::{
            BUCKET_ALGORITHM_VERSION, Database, Engine, HASH_VERSION, INITIAL_MAP_GENERATION,
            KEY_ENCODING_VERSION, LogicalDatabaseMetadata, RoutingCatalog, ShardKeyMetadata,
            ShardKeyType, TableMetadata, TablePlacement, VIRTUAL_BUCKET_COUNT,
            initial_physical_shard,
        },
        sql::{SqlDialect, normalize_placeholders, parse, validate_common_subset},
    };

    const DEFAULT_DATABASE: u64 = 1;
    const TENANT_DATABASE: u64 = 9;
    const BLOBS_TABLE: u64 = 2;
    const COUNTRIES_TABLE: u64 = 3;
    const EVENTS_TABLE: u64 = 4;
    const INTERNAL_CATALOG_TABLE: u64 = 5;
    const ACCOUNTS_TABLE: u64 = 30;

    fn sample_catalog() -> Catalog {
        Catalog::from_validated_parts(
            1,
            7,
            DEFAULT_DATABASE,
            vec![
                LogicalDatabaseMetadata::from_validated(DEFAULT_DATABASE, "default".to_owned()),
                LogicalDatabaseMetadata::from_validated(TENANT_DATABASE, "tenant".to_owned()),
            ]
            .into_boxed_slice(),
            vec![
                TableMetadata::from_validated(
                    BLOBS_TABLE,
                    DEFAULT_DATABASE,
                    "blobs".to_owned(),
                    TablePlacement::Sharded(ShardKeyMetadata::from_validated(
                        "tenant_id".to_owned(),
                        ShardKeyType::Binary,
                    )),
                ),
                TableMetadata::from_validated(
                    COUNTRIES_TABLE,
                    DEFAULT_DATABASE,
                    "countries".to_owned(),
                    TablePlacement::Global,
                ),
                TableMetadata::from_validated(
                    EVENTS_TABLE,
                    DEFAULT_DATABASE,
                    "events".to_owned(),
                    TablePlacement::Sharded(ShardKeyMetadata::from_validated(
                        "tenant_id".to_owned(),
                        ShardKeyType::Int64,
                    )),
                ),
                TableMetadata::from_validated(
                    INTERNAL_CATALOG_TABLE,
                    DEFAULT_DATABASE,
                    "internal_catalog".to_owned(),
                    TablePlacement::Catalog,
                ),
                TableMetadata::from_validated(
                    ACCOUNTS_TABLE,
                    TENANT_DATABASE,
                    "accounts".to_owned(),
                    TablePlacement::Sharded(ShardKeyMetadata::from_validated(
                        "tenant_id".to_owned(),
                        ShardKeyType::Text,
                    )),
                ),
            ]
            .into_boxed_slice(),
        )
    }

    fn normalize(dialect: SqlDialect, source: &str) -> NormalizedSql {
        normalize_placeholders(validate_common_subset(parse(dialect, source).unwrap()).unwrap())
            .unwrap()
    }

    fn database_id(value: u64) -> LogicalDatabaseId {
        LogicalDatabaseId::new(value).unwrap()
    }

    fn routing_catalog(shard_count: u16) -> RoutingCatalog {
        RoutingCatalog::from_validated_parts(
            shard_count,
            HASH_VERSION,
            KEY_ENCODING_VERSION,
            BUCKET_ALGORITHM_VERSION,
            INITIAL_MAP_GENERATION,
            (0..VIRTUAL_BUCKET_COUNT)
                .map(|bucket| initial_physical_shard(bucket, shard_count))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
    }

    fn plan(
        catalog: &Catalog,
        database: u64,
        normalized: &NormalizedSql,
        statement_index: usize,
        parameters: &[Value],
        explicit_routing_key: Option<&[u8]>,
    ) -> EngineResult<BoundStatementPlan> {
        let routing = routing_catalog(4);
        plan_bound_statement(
            BoundStatementPlanInput::new(
                catalog,
                database_id(database),
                normalized,
                statement_index,
                parameters,
                explicit_routing_key,
            ),
            RoutingProvenance::new(
                routing.hash_version(),
                routing.key_encoding_version(),
                routing.bucket_algorithm_version(),
                routing.map_generation(),
            ),
            |key| routing.shard_for_key(key),
        )
    }

    #[test]
    fn public_plan_types_are_owned_cloneable_thread_safe_and_redacted() {
        fn assert_owned<T: Clone + Send + Sync + 'static>() {}
        assert_owned::<BoundStatementPlan>();
        assert_owned::<PlannedRoute>();

        let catalog = sample_catalog();
        let normalized = normalize(
            SqlDialect::PostgreSql,
            "INSERT INTO accounts (tenant_id) VALUES ('private-tenant')",
        );
        let plan = plan(
            &catalog,
            TENANT_DATABASE,
            &normalized,
            0,
            &[],
            Some(b"private-explicit-key"),
        )
        .unwrap();
        assert_eq!(plan.clone(), plan);
        assert_eq!(plan.inferred_routes()[0].key_bytes(), b"private-tenant");
        assert_eq!(
            plan.explicit_route().unwrap().key_bytes(),
            b"private-explicit-key"
        );

        let debug = format!("{plan:?} {:?}", plan.explicit_route().unwrap());
        assert!(!debug.contains("private-tenant"));
        assert!(!debug.contains("private-explicit-key"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn canonical_key_encoding_v1_has_frozen_typed_bytes() {
        for (value, expected) in [
            (ShardKeyValue::Int64(i64::MIN), i64::MIN.to_string()),
            (ShardKeyValue::Int64(-1), "-1".to_owned()),
            (ShardKeyValue::Int64(0), "0".to_owned()),
            (ShardKeyValue::Int64(1), "1".to_owned()),
            (ShardKeyValue::Int64(i64::MAX), i64::MAX.to_string()),
        ] {
            assert_eq!(canonical_key_bytes(&value).as_ref(), expected.as_bytes());
        }

        for text in ["", "a\0b", "snowman-☃", "é", "e\u{301}"] {
            assert_eq!(
                canonical_key_bytes(&ShardKeyValue::Text(text.to_owned())).as_ref(),
                text.as_bytes()
            );
        }
        assert_ne!("é".as_bytes(), "e\u{301}".as_bytes());

        for bytes in [
            Vec::new(),
            vec![0],
            vec![0, 1, 2, 0xff],
            vec![0xff, 0, 0x80],
        ] {
            assert_eq!(
                canonical_key_bytes(&ShardKeyValue::Binary(bytes.clone())).as_ref(),
                bytes
            );
        }
    }

    #[test]
    fn equivalent_integer_forms_converge_on_one_canonical_route() {
        let catalog = sample_catalog();
        let literal = plan(
            &catalog,
            DEFAULT_DATABASE,
            &normalize(
                SqlDialect::Sqlite,
                "SELECT * FROM events WHERE tenant_id = 1",
            ),
            0,
            &[],
            None,
        )
        .unwrap();
        let signed = plan(
            &catalog,
            DEFAULT_DATABASE,
            &normalize(
                SqlDialect::Sqlite,
                "SELECT * FROM events WHERE tenant_id = +001",
            ),
            0,
            &[],
            None,
        )
        .unwrap();
        let bound = plan(
            &catalog,
            DEFAULT_DATABASE,
            &normalize(
                SqlDialect::PostgreSql,
                "SELECT * FROM events WHERE tenant_id = $1",
            ),
            0,
            &[Value::Int64(1)],
            None,
        )
        .unwrap();

        for candidate in [&literal, &signed, &bound] {
            assert_eq!(candidate.inferred_routes()[0].key_bytes(), b"1");
            assert_eq!(candidate.inferred_routes()[0], literal.inferred_routes()[0]);
        }
    }

    #[test]
    fn actual_bound_values_drive_replanning_for_every_marker_dialect() {
        let catalog = sample_catalog();
        let cases = [
            (
                SqlDialect::PostgreSql,
                "SELECT $1 FROM events WHERE tenant_id = $2",
                vec![Value::Text("projection".to_owned()), Value::Int64(12)],
                vec![Value::Text("projection".to_owned()), Value::Int64(34)],
                b"12".as_slice(),
                b"34".as_slice(),
            ),
            (
                SqlDialect::MySql,
                "UPDATE events SET payload = ? WHERE tenant_id = ?",
                vec![Value::Text("first".to_owned()), Value::Int64(56)],
                vec![Value::Text("second".to_owned()), Value::Int64(78)],
                b"56".as_slice(),
                b"78".as_slice(),
            ),
            (
                SqlDialect::Sqlite,
                "SELECT ?2 FROM events WHERE tenant_id = ?1",
                vec![Value::Int64(90), Value::Text("first".to_owned())],
                vec![Value::Int64(91), Value::Text("second".to_owned())],
                b"90".as_slice(),
                b"91".as_slice(),
            ),
        ];

        for (dialect, source, first_values, second_values, first_key, second_key) in cases {
            let normalized = normalize(dialect, source);
            let first = plan(
                &catalog,
                DEFAULT_DATABASE,
                &normalized,
                0,
                &first_values,
                None,
            )
            .unwrap();
            let second = plan(
                &catalog,
                DEFAULT_DATABASE,
                &normalized,
                0,
                &second_values,
                None,
            )
            .unwrap();
            assert_eq!(first.inferred_routes()[0].key_bytes(), first_key);
            assert_eq!(second.inferred_routes()[0].key_bytes(), second_key);
            assert_ne!(first.inferred_routes()[0], second.inferred_routes()[0]);
        }
    }

    #[test]
    fn equivalent_typed_requests_produce_equal_plans_across_dialects() {
        let catalog = sample_catalog();
        let plans = [
            (
                SqlDialect::Sqlite,
                "SELECT * FROM events WHERE tenant_id = ?1",
            ),
            (
                SqlDialect::PostgreSql,
                "SELECT * FROM events WHERE tenant_id = $1",
            ),
            (
                SqlDialect::MySql,
                "SELECT * FROM events WHERE tenant_id = ?",
            ),
        ]
        .map(|(dialect, source)| {
            plan(
                &catalog,
                DEFAULT_DATABASE,
                &normalize(dialect, source),
                0,
                &[Value::Int64(42)],
                Some(b"frontend-independent-route"),
            )
            .unwrap()
        });

        assert_eq!(plans[0], plans[1]);
        assert_eq!(plans[1], plans[2]);
        assert_eq!(plans[0].inferred_routes()[0].key_bytes(), b"42");
    }

    #[test]
    fn every_inference_kind_keeps_explicit_routing_independent() {
        let catalog = sample_catalog();
        let cases = [
            ("SELECT 1", ShardKeyInferenceKind::NotApplicable, 0_usize),
            (
                "SELECT * FROM countries",
                ShardKeyInferenceKind::NotSharded,
                0,
            ),
            (
                "SELECT * FROM events",
                ShardKeyInferenceKind::Unconstrained,
                0,
            ),
            (
                "SELECT * FROM events WHERE tenant_id = NULL",
                ShardKeyInferenceKind::Contradiction,
                0,
            ),
            (
                "SELECT * FROM events WHERE tenant_id = 7",
                ShardKeyInferenceKind::Exact,
                1,
            ),
            (
                "SELECT * FROM events WHERE tenant_id = 7 OR tenant_id = 8",
                ShardKeyInferenceKind::Multiple,
                2,
            ),
        ];

        for (source, kind, route_count) in cases {
            let normalized = normalize(SqlDialect::Sqlite, source);
            for explicit in [None, Some(b"".as_slice()), Some(b"fallback".as_slice())] {
                let plan = plan(&catalog, DEFAULT_DATABASE, &normalized, 0, &[], explicit).unwrap();
                assert_eq!(plan.inference().kind(), kind, "{source}");
                assert_eq!(plan.inferred_routes().len(), route_count, "{source}");
                assert_eq!(
                    plan.explicit_route().is_some(),
                    explicit.is_some(),
                    "{source}"
                );
                if let Some(explicit) = explicit {
                    assert_eq!(plan.explicit_route().unwrap().key_bytes(), explicit);
                }
            }
        }
    }

    #[test]
    fn multirow_insert_routes_preserve_order_duplicates_and_shared_storage() {
        let catalog = sample_catalog();
        let normalized = normalize(
            SqlDialect::MySql,
            "INSERT INTO events (tenant_id) VALUES (?), (?), (?)",
        );
        let plan = plan(
            &catalog,
            DEFAULT_DATABASE,
            &normalized,
            0,
            &[Value::Int64(11), Value::Int64(22), Value::Int64(11)],
            None,
        )
        .unwrap();

        assert_eq!(plan.inference().kind(), ShardKeyInferenceKind::Multiple);
        assert_eq!(
            plan.inferred_routes()
                .iter()
                .map(PlannedRoute::key_bytes)
                .collect::<Vec<_>>(),
            [b"11".as_slice(), b"22".as_slice(), b"11".as_slice()]
        );
        assert!(Arc::ptr_eq(
            &plan.inferred_routes[0].key_bytes,
            &plan.inferred_routes[2].key_bytes
        ));
        assert_eq!(plan.inferred_routes[0], plan.inferred_routes[2]);
    }

    #[test]
    fn distinct_logical_keys_are_never_collapsed_by_physical_shard() {
        let catalog = sample_catalog();
        let normalized = normalize(
            SqlDialect::Sqlite,
            "SELECT * FROM events WHERE tenant_id = 1 OR tenant_id = 2",
        );
        let plan = plan_bound_statement(
            BoundStatementPlanInput::new(
                &catalog,
                database_id(DEFAULT_DATABASE),
                &normalized,
                0,
                &[],
                Some(b"explicit"),
            ),
            RoutingProvenance::new(1, 1, 1, 1),
            |_| 3,
        )
        .unwrap();

        assert_eq!(plan.inference().kind(), ShardKeyInferenceKind::Multiple);
        assert_eq!(plan.inferred_routes().len(), 2);
        assert_eq!(plan.inferred_routes()[0].key_bytes(), b"1");
        assert_eq!(plan.inferred_routes()[1].key_bytes(), b"2");
        assert!(
            plan.inferred_routes()
                .iter()
                .all(|route| route.shard() == 3)
        );
        assert_eq!(plan.explicit_route().unwrap().shard(), 3);
    }

    #[test]
    fn text_predicates_remain_conservative_while_text_inserts_are_routed() {
        let catalog = sample_catalog();
        let predicate = plan(
            &catalog,
            TENANT_DATABASE,
            &normalize(
                SqlDialect::PostgreSql,
                "SELECT * FROM accounts WHERE tenant_id = $1",
            ),
            0,
            &[Value::Text("tenant-a".to_owned())],
            Some(b"caller-route"),
        )
        .unwrap();
        assert_eq!(
            predicate.inference().kind(),
            ShardKeyInferenceKind::Unconstrained
        );
        assert!(predicate.inferred_routes().is_empty());
        assert_eq!(
            predicate.explicit_route().unwrap().key_bytes(),
            b"caller-route"
        );

        let insert = plan(
            &catalog,
            TENANT_DATABASE,
            &normalize(
                SqlDialect::PostgreSql,
                "INSERT INTO accounts (tenant_id) VALUES ($1)",
            ),
            0,
            &[Value::Text("tenant-a".to_owned())],
            None,
        )
        .unwrap();
        assert_eq!(insert.inference().kind(), ShardKeyInferenceKind::Exact);
        assert_eq!(insert.inferred_routes()[0].key_bytes(), b"tenant-a");
    }

    #[test]
    fn inference_errors_propagate_and_do_not_poison_later_plans() {
        let catalog = sample_catalog();
        let int_bound = normalize(
            SqlDialect::PostgreSql,
            "SELECT * FROM events WHERE tenant_id = $1",
        );
        for (parameters, kind) in [
            (Vec::new(), EngineErrorKind::InvalidArgument),
            (
                vec![Value::Text("wrong-type".to_owned())],
                EngineErrorKind::TypeMismatch,
            ),
            (
                vec![Value::UInt64(u64::MAX)],
                EngineErrorKind::NumericOutOfRange,
            ),
        ] {
            assert_eq!(
                plan(&catalog, DEFAULT_DATABASE, &int_bound, 0, &parameters, None,)
                    .unwrap_err()
                    .kind(),
                kind
            );
        }

        let invalid_text = plan(
            &catalog,
            TENANT_DATABASE,
            &normalize(
                SqlDialect::PostgreSql,
                "INSERT INTO accounts (tenant_id) VALUES ($1)",
            ),
            0,
            &[Value::InvalidText(vec![0xff])],
            None,
        )
        .unwrap_err();
        assert_eq!(invalid_text.kind(), EngineErrorKind::InvalidTextEncoding);

        let null_insert = plan(
            &catalog,
            DEFAULT_DATABASE,
            &normalize(
                SqlDialect::PostgreSql,
                "INSERT INTO events (tenant_id) VALUES ($1)",
            ),
            0,
            &[Value::Null],
            None,
        )
        .unwrap_err();
        assert_eq!(null_insert.kind(), EngineErrorKind::NotNullViolation);

        let missing_table = plan(
            &catalog,
            DEFAULT_DATABASE,
            &normalize(SqlDialect::Sqlite, "SELECT * FROM missing"),
            0,
            &[],
            None,
        )
        .unwrap_err();
        assert_eq!(missing_table.kind(), EngineErrorKind::InvalidQuery);

        let missing_database = plan(
            &catalog,
            99,
            &normalize(SqlDialect::Sqlite, "SELECT 1"),
            0,
            &[],
            None,
        )
        .unwrap_err();
        assert_eq!(missing_database.kind(), EngineErrorKind::InvalidArgument);

        let bad_statement_index =
            plan(&catalog, DEFAULT_DATABASE, &int_bound, 1, &[], None).unwrap_err();
        assert_eq!(bad_statement_index.kind(), EngineErrorKind::InvalidArgument);

        let recovered = plan(
            &catalog,
            DEFAULT_DATABASE,
            &int_bound,
            0,
            &[Value::Int64(42)],
            None,
        )
        .unwrap();
        assert_eq!(recovered.inferred_routes()[0].key_bytes(), b"42");
    }

    #[test]
    fn statement_local_batch_indexes_use_statement_local_parameters() {
        let catalog = sample_catalog();
        let normalized = normalize(
            SqlDialect::MySql,
            "SELECT ? FROM events WHERE tenant_id = ?; \
             SELECT ? FROM events WHERE tenant_id = ?",
        );
        let first = plan(
            &catalog,
            DEFAULT_DATABASE,
            &normalized,
            0,
            &[Value::Text("first".to_owned()), Value::Int64(12)],
            None,
        )
        .unwrap();
        let second = plan(
            &catalog,
            DEFAULT_DATABASE,
            &normalized,
            1,
            &[Value::Text("second".to_owned()), Value::Int64(34)],
            None,
        )
        .unwrap();
        assert_eq!(first.statement_index(), 0);
        assert_eq!(second.statement_index(), 1);
        assert_eq!(first.inferred_routes()[0].key_bytes(), b"12");
        assert_eq!(second.inferred_routes()[0].key_bytes(), b"34");

        let numbered_gap = normalize(
            SqlDialect::PostgreSql,
            "SELECT * FROM events WHERE tenant_id = $2",
        );
        let gap_plan = plan(
            &catalog,
            DEFAULT_DATABASE,
            &numbered_gap,
            0,
            &[Value::Text("unused-gap".to_owned()), Value::Int64(56)],
            None,
        )
        .unwrap();
        assert_eq!(gap_plan.inferred_routes()[0].key_bytes(), b"56");
    }

    #[test]
    fn concurrent_planning_is_deterministic_and_read_only() {
        let catalog = Arc::new(sample_catalog());
        let normalized = Arc::new(normalize(
            SqlDialect::PostgreSql,
            "INSERT INTO events (tenant_id) VALUES ($1), ($2), ($1)",
        ));
        let barrier = Arc::new(Barrier::new(9));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let catalog = Arc::clone(&catalog);
            let normalized = Arc::clone(&normalized);
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                barrier.wait();
                plan(
                    &catalog,
                    DEFAULT_DATABASE,
                    &normalized,
                    0,
                    &[Value::Int64(7), Value::Int64(8)],
                    Some(b"parallel-explicit"),
                )
                .unwrap()
            }));
        }
        barrier.wait();
        let plans = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert!(plans.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[test]
    fn engine_planning_records_provenance_and_does_not_enter_execution_lifecycle() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 4).unwrap());
        let engine = Engine::from_database(Arc::clone(&database));
        let normalized = normalize(SqlDialect::Sqlite, "SELECT 1");
        let logical_database = engine.catalog().default_database().id();

        assert_eq!(engine.active_operations_for_test(), 0);
        let before = engine
            .plan_bound_statement(logical_database, &normalized, 0, &[], Some(b"raw\0route"))
            .unwrap();
        assert_eq!(engine.active_operations_for_test(), 0);
        assert_eq!(before.database(), logical_database);
        assert_eq!(before.schema_generation(), 0);
        assert_eq!(before.hash_version(), HASH_VERSION);
        assert_eq!(before.key_encoding_version(), KEY_ENCODING_VERSION);
        assert_eq!(before.bucket_algorithm_version(), BUCKET_ALGORITHM_VERSION);
        assert_eq!(before.map_generation(), INITIAL_MAP_GENERATION);
        assert_eq!(before.statement_index(), 0);
        assert_eq!(before.explicit_route().unwrap().key_bytes(), b"raw\0route");
        assert_eq!(
            before.explicit_route().unwrap().shard(),
            database.shard_for_key(b"raw\0route")
        );

        database
            .broadcast("CREATE TABLE planning_generation (id INTEGER PRIMARY KEY)")
            .unwrap();
        let after = engine
            .plan_bound_statement(logical_database, &normalized, 0, &[], None)
            .unwrap();
        assert_eq!(after.schema_generation(), 1);
        assert_eq!(after.map_generation(), before.map_generation());
    }

    #[test]
    fn engine_planning_honors_schema_gate_states_and_recovers_after_migration() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 2).unwrap());
        let engine = Engine::from_database(Arc::clone(&database));
        let normalized = normalize(SqlDialect::Sqlite, "SELECT 1");
        let logical_database = engine.catalog().default_database().id();

        let migration = database.storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        assert_eq!(
            engine
                .plan_bound_statement(logical_database, &normalized, 0, &[], None)
                .unwrap_err()
                .kind(),
            EngineErrorKind::Busy
        );
        drop(migration);
        assert!(
            engine
                .plan_bound_statement(logical_database, &normalized, 0, &[], None)
                .is_ok()
        );

        let pending_root = tempfile::tempdir().unwrap();
        let pending_database = Arc::new(Database::open(pending_root.path(), 2).unwrap());
        let pending_engine = Engine::from_database(Arc::clone(&pending_database));
        let pending = pending_database.storage.begin_schema_migration().unwrap();
        pending.wait_for_quiescence_blocking();
        pending.publish_pending().unwrap();
        let pending_database_id = pending_engine.catalog().default_database().id();
        assert_eq!(
            pending_engine
                .plan_bound_statement(pending_database_id, &normalized, 0, &[], None)
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );

        let degraded_root = tempfile::tempdir().unwrap();
        let degraded_database = Arc::new(Database::open(degraded_root.path(), 2).unwrap());
        let degraded_engine = Engine::from_database(Arc::clone(&degraded_database));
        degraded_database.storage.mark_schema_degraded();
        let degraded_database_id = degraded_engine.catalog().default_database().id();
        assert_eq!(
            degraded_engine
                .plan_bound_statement(degraded_database_id, &normalized, 0, &[], None)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
    }
}
