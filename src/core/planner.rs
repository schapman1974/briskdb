//! Bound-value-aware, protocol-neutral statement routing plans.

use std::{borrow::Cow, collections::HashMap, fmt, sync::Arc};

use crate::sql::{
    GeneratedInsertShape, NormalizedSql, RoutedDml, ShardKeyInference, ShardKeyInferenceKind,
    ShardKeyValue, StatementBehavior, classify_normalized_statements, generated_insert_shape,
    infer_shard_keys, routed_dml_shape,
};

use super::{
    AllocationOwnerMap, Catalog, EngineError, EngineErrorKind, EngineResult, GeneratedIdPolicy,
    LogicalDatabaseId, TableId, TableMetadata, TablePlacement, Value,
    generated_id::{
        GeneratedIdClassification, HILO_V1_FORMAT_MARKER, classify_caller_generated_id,
    },
};

/// Borrowed typed shard key encoded identically by every routing entry point.
///
/// Version 1 intentionally has no type tag: signed integers use their minimal
/// decimal spelling, text uses its exact UTF-8 bytes, and binary keys use their
/// exact bytes. The persisted key-encoding version freezes this representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CanonicalShardKeyRef<'a> {
    Int64(i64),
    Text(&'a str),
    Binary(&'a [u8]),
}

pub(crate) fn canonical_shard_key_bytes<'a>(value: CanonicalShardKeyRef<'a>) -> Cow<'a, [u8]> {
    match value {
        CanonicalShardKeyRef::Int64(value) => Cow::Owned(value.to_string().into_bytes()),
        CanonicalShardKeyRef::Text(value) => Cow::Borrowed(value.as_bytes()),
        CanonicalShardKeyRef::Binary(value) => Cow::Borrowed(value),
    }
}

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

/// One structurally proven, single-row INSERT whose declared generated key is
/// absent from the caller's column list.
///
/// Target selection and allocation deliberately remain later execution work.
/// This plan records only immutable catalog intent and never treats an explicit
/// route or an explicitly supplied `NULL` as permission to generate a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GeneratedInsertPlan {
    table_id: TableId,
    policy: GeneratedIdPolicy,
}

impl GeneratedInsertPlan {
    pub(crate) const fn table_id(&self) -> TableId {
        self.table_id
    }

    #[cfg(any(feature = "experimental-vtab", test))]
    pub(crate) const fn policy(&self) -> &GeneratedIdPolicy {
        &self.policy
    }
}

/// Owned routing metadata produced from one statement's actual bound values.
///
/// Inferred routes remain aligned one-for-one with
/// [`ShardKeyInference::values`], including duplicate `INSERT` row values. An
/// explicit route is retained independently even after routing policy compares
/// it with inferred routes. A successful plan records the one assigned shard
/// when the statement has a valid single-shard route and retains its classified
/// behavior. The complete normalized batch must first satisfy statement-batch
/// policy. A plan does not translate SQL or execute anything.
#[derive(Clone, PartialEq, Eq)]
pub struct BoundStatementPlan {
    database: LogicalDatabaseId,
    schema_generation: u64,
    hash_version: u32,
    key_encoding_version: u32,
    bucket_algorithm_version: u32,
    map_generation: u64,
    statement_index: usize,
    behavior: StatementBehavior,
    inference: ShardKeyInference,
    inferred_routes: Vec<PlannedRoute>,
    explicit_route: Option<PlannedRoute>,
    generated_insert: Option<GeneratedInsertPlan>,
    assigned_shard: Option<u16>,
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

    /// Return the authoritative behavior of the selected statement.
    pub const fn behavior(&self) -> StatementBehavior {
        self.behavior
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

    /// Return a single-row omitted-key intent awaiting allocator target
    /// selection, if this statement declared one structurally.
    pub(crate) const fn generated_insert(&self) -> Option<&GeneratedInsertPlan> {
        self.generated_insert.as_ref()
    }

    /// Return the physical shard assigned by routing policy, if any.
    ///
    /// `None` is retained for non-sharded statements and reads that still need
    /// later scatter, empty-result planning, or generated-key allocation.
    /// Explicit-key sharded writes always have an assigned shard; an omitted
    /// generated key receives its target only during allocator execution.
    pub const fn assigned_shard(&self) -> Option<u16> {
        self.assigned_shard
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
            .field("behavior", &self.behavior)
            .field("inference", &self.inference)
            .field("inferred_route_count", &self.inferred_routes.len())
            .field("has_explicit_route", &self.explicit_route.is_some())
            .field("has_generated_insert", &self.generated_insert.is_some())
            .field("assigned_shard", &self.assigned_shard)
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
    allocation_owners: Option<&'a AllocationOwnerMap>,
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
            allocation_owners: None,
        }
    }

    pub(super) const fn with_allocation_owners(
        mut self,
        allocation_owners: Option<&'a AllocationOwnerMap>,
    ) -> Self {
        self.allocation_owners = allocation_owners;
        self
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
        allocation_owners,
    } = input;
    let classification = classify_normalized_statements(normalized)?;
    let behavior = classification.behavior(statement_index).ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::InvalidArgument,
            "SQL statement index is outside the normalized batch",
        )
    })?;
    let schema_generation = catalog.schema_generation();
    let inference = infer_shard_keys(catalog, database, normalized, statement_index, parameters)?;
    validate_inference_shape(&inference)?;
    let inferred_table = inference
        .table_id()
        .and_then(|table| catalog.table_by_id(table))
        .filter(|table| table.database_id() == database);
    let shard_key_column = sharded_key_column(catalog, database, &inference)?;
    let dml = shard_key_column
        .map(|column| routed_dml_shape(normalized, statement_index, column))
        .transpose()?
        .flatten();
    let generated_insert = plan_generated_insert(
        normalized,
        statement_index,
        dml,
        inferred_table,
        inference.kind(),
    )?;
    reject_hilo_allocator_namespace_insert(dml, inferred_table, inference.values())?;

    let mut unique_routes = HashMap::<&ShardKeyValue, PlannedRoute>::new();
    let mut inferred_routes = Vec::with_capacity(inference.values().len());
    for value in inference.values() {
        if let Some(route) = unique_routes.get(value) {
            inferred_routes.push(route.clone());
            continue;
        }
        let key_bytes = canonical_key_bytes(value);
        let route = PlannedRoute {
            shard: route_inferred_value(
                inferred_table,
                allocation_owners,
                value,
                &key_bytes,
                &mut shard_for_key,
            )?,
            key_bytes,
        };
        unique_routes.insert(value, route.clone());
        inferred_routes.push(route);
    }
    drop(unique_routes);

    let explicit_route = explicit_routing_key
        .filter(|_| generated_insert.is_none())
        .map(|key| {
            // A protocol routing key that is byte-for-byte the canonical inferred
            // key must resolve through the same table policy. In particular, a
            // native-range integer is owner-routed rather than re-hashed merely
            // because it arrived through session context as decimal text.
            let shard = inferred_routes
                .iter()
                .find(|route| route.key_bytes() == key)
                .map_or_else(|| shard_for_key(key), PlannedRoute::shard);
            PlannedRoute {
                shard,
                key_bytes: Arc::from(key),
            }
        });

    reject_retired_owner_insert(dml, inferred_table, allocation_owners, inference.values())?;
    let inferred_shards = inferred_shards(&inferred_routes);

    if matches!(
        dml,
        Some(RoutedDml::Update {
            assigns_shard_key: true
        })
    ) {
        return Err(EngineError::new(
            EngineErrorKind::InvalidQuery,
            "sharded UPDATE cannot assign the shard-key column",
        ));
    }
    validate_explicit_route(&inferred_routes, explicit_route.as_ref())?;
    validate_sharded_write(
        dml,
        inferred_shards,
        explicit_route.as_ref(),
        generated_insert.as_ref(),
    )?;

    let assigned_shard = if shard_key_column.is_some() {
        match inferred_shards {
            InferredShards::None => explicit_route.as_ref().map(PlannedRoute::shard),
            InferredShards::One(shard) => Some(shard),
            InferredShards::Multiple => None,
        }
    } else {
        None
    };

    Ok(BoundStatementPlan {
        database,
        schema_generation,
        hash_version: provenance.hash_version,
        key_encoding_version: provenance.key_encoding_version,
        bucket_algorithm_version: provenance.bucket_algorithm_version,
        map_generation: provenance.map_generation,
        statement_index,
        behavior,
        inference,
        inferred_routes,
        explicit_route,
        generated_insert,
        assigned_shard,
    })
}

fn plan_generated_insert(
    normalized: &NormalizedSql,
    statement_index: usize,
    dml: Option<RoutedDml>,
    table: Option<&TableMetadata>,
    inference_kind: ShardKeyInferenceKind,
) -> EngineResult<Option<GeneratedInsertPlan>> {
    if dml != Some(RoutedDml::Insert) {
        return Ok(None);
    }
    let Some(table) = table else {
        return Ok(None);
    };
    let policy = table.generated_id_policy();
    let Some(column) = policy.column() else {
        return Ok(None);
    };

    match generated_insert_shape(normalized, statement_index, column)? {
        Some(GeneratedInsertShape::ExplicitKey) => Ok(None),
        Some(GeneratedInsertShape::OmittedSingleRow) => {
            if inference_kind != ShardKeyInferenceKind::Unconstrained {
                return Err(planning_invariant());
            }
            Ok(Some(GeneratedInsertPlan {
                table_id: table.id(),
                policy: policy.clone(),
            }))
        }
        Some(GeneratedInsertShape::OmittedMultipleRows) => Err(EngineError::new(
            EngineErrorKind::Unsupported,
            "multi-row INSERT with an omitted generated key is not supported",
        )),
        None => Err(planning_invariant()),
    }
}

fn reject_hilo_allocator_namespace_insert(
    dml: Option<RoutedDml>,
    table: Option<&TableMetadata>,
    values: &[ShardKeyValue],
) -> EngineResult<()> {
    if dml != Some(RoutedDml::Insert)
        || !table.is_some_and(|table| {
            matches!(
                table.generated_id_policy(),
                GeneratedIdPolicy::HiloV1 { .. }
            )
        })
    {
        return Ok(());
    }

    let first_reserved =
        i64::try_from(HILO_V1_FORMAT_MARKER).expect("hilo_v1 reserves the signed high bit");
    if values
        .iter()
        .any(|value| matches!(value, ShardKeyValue::Int64(value) if *value >= first_reserved))
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "hilo_v1 generated-ID values are allocator-owned and cannot be supplied by an explicit INSERT",
        ));
    }
    Ok(())
}

fn reject_retired_owner_insert(
    dml: Option<RoutedDml>,
    table: Option<&TableMetadata>,
    allocation_owners: Option<&AllocationOwnerMap>,
    values: &[ShardKeyValue],
) -> EngineResult<()> {
    if dml != Some(RoutedDml::Insert) {
        return Ok(());
    }
    let Some(table) = table else {
        return Ok(());
    };
    let GeneratedIdPolicy::NativeRangeV1 { .. } = table.generated_id_policy() else {
        return Ok(());
    };
    let owners = allocation_owners.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::DataCorruption,
            "native_range_v1 table metadata is missing its allocation-owner map",
        )
    })?;
    for value in values {
        let ShardKeyValue::Int64(value) = value else {
            continue;
        };
        let GeneratedIdClassification::NativeRangeV1(native) =
            classify_caller_generated_id(table.generated_id_policy(), *value)?
        else {
            continue;
        };
        if owners.physical_shard(native.owner()).is_some()
            && !owners.owner_is_active(native.owner())
        {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "native_range_v1 owner {} is retired and cannot accept new IDs",
                    native.owner().get()
                ),
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InferredShards {
    None,
    One(u16),
    Multiple,
}

fn inferred_shards(routes: &[PlannedRoute]) -> InferredShards {
    let Some(first) = routes.first() else {
        return InferredShards::None;
    };
    if routes.iter().all(|route| route.shard == first.shard) {
        InferredShards::One(first.shard)
    } else {
        InferredShards::Multiple
    }
}

fn validate_explicit_route(
    inferred_routes: &[PlannedRoute],
    explicit_route: Option<&PlannedRoute>,
) -> EngineResult<()> {
    let Some(explicit_route) = explicit_route else {
        return Ok(());
    };
    if inferred_routes
        .iter()
        .all(|inferred| inferred.shard == explicit_route.shard)
    {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "explicit routing key conflicts with inferred statement routing",
        ))
    }
}

fn validate_sharded_write(
    dml: Option<RoutedDml>,
    inferred_shards: InferredShards,
    explicit_route: Option<&PlannedRoute>,
    generated_insert: Option<&GeneratedInsertPlan>,
) -> EngineResult<()> {
    match dml {
        Some(RoutedDml::Insert) => match inferred_shards {
            InferredShards::One(_) => Ok(()),
            InferredShards::None if generated_insert.is_some() => Ok(()),
            InferredShards::None | InferredShards::Multiple => Err(EngineError::new(
                EngineErrorKind::InvalidQuery,
                "sharded INSERT requires a proven routing key for every row",
            )),
        },
        Some(RoutedDml::Update {
            assigns_shard_key: false,
        })
        | Some(RoutedDml::Delete) => match inferred_shards {
            InferredShards::One(_) => Ok(()),
            InferredShards::None if explicit_route.is_some() => Ok(()),
            InferredShards::None => Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "sharded write requires an inferred or explicit routing key",
            )),
            InferredShards::Multiple => Err(EngineError::new(
                EngineErrorKind::InvalidQuery,
                "sharded write targets more than one physical shard",
            )),
        },
        Some(RoutedDml::Update {
            assigns_shard_key: true,
        }) => Err(EngineError::new(
            EngineErrorKind::Internal,
            "routing policy accepted an invalid shard-key update",
        )),
        None => Ok(()),
    }
}

fn sharded_key_column<'a>(
    catalog: &'a Catalog,
    database: LogicalDatabaseId,
    inference: &ShardKeyInference,
) -> EngineResult<Option<&'a str>> {
    let sharded = matches!(
        inference.kind(),
        ShardKeyInferenceKind::Unconstrained
            | ShardKeyInferenceKind::Contradiction
            | ShardKeyInferenceKind::Exact
            | ShardKeyInferenceKind::Multiple
    );
    if !sharded {
        return Ok(None);
    }
    let table = inference
        .table_id()
        .and_then(|id| catalog.table_by_id(id))
        .filter(|table| table.database_id() == database)
        .ok_or_else(planning_invariant)?;
    let TablePlacement::Sharded(shard_key) = table.placement() else {
        return Err(planning_invariant());
    };
    if inference.key_type() != Some(shard_key.key_type()) {
        return Err(planning_invariant());
    }
    Ok(Some(shard_key.column()))
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
        Err(planning_invariant())
    }
}

fn planning_invariant() -> EngineError {
    EngineError::new(
        EngineErrorKind::Internal,
        "shard-key inference is inconsistent during bound statement planning",
    )
}

fn route_inferred_value<F>(
    table: Option<&TableMetadata>,
    allocation_owners: Option<&AllocationOwnerMap>,
    value: &ShardKeyValue,
    canonical_key: &[u8],
    shard_for_key: &mut F,
) -> EngineResult<u16>
where
    F: FnMut(&[u8]) -> u16,
{
    let Some(table) = table else {
        return Ok(shard_for_key(canonical_key));
    };
    let ShardKeyValue::Int64(value) = value else {
        return Ok(shard_for_key(canonical_key));
    };
    if !matches!(
        table.generated_id_policy(),
        GeneratedIdPolicy::NativeRangeV1 { .. } | GeneratedIdPolicy::HiloV1 { .. }
    ) {
        return Ok(shard_for_key(canonical_key));
    }

    let classification = classify_caller_generated_id(table.generated_id_policy(), *value)?;
    let native = match classification {
        GeneratedIdClassification::Legacy(_) | GeneratedIdClassification::HiloV1(_) => {
            // Native tables keep their pre-native-marker legacy route. Hi/lo
            // tables hash both their global sequence values and the explicitly
            // supported negative/pre-hi/lo legacy interval.
            return Ok(shard_for_key(canonical_key));
        }
        GeneratedIdClassification::NativeRangeV1(native) => native,
    };
    let owners = allocation_owners.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::DataCorruption,
            "native_range_v1 table metadata is missing its allocation-owner map",
        )
    })?;
    owners.physical_shard(native.owner()).ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!(
                "native_range_v1 owner {} is not assigned to an active physical shard",
                native.owner().get()
            ),
        )
    })
}

fn canonical_key_bytes(value: &ShardKeyValue) -> Arc<[u8]> {
    let canonical = match value {
        ShardKeyValue::Int64(value) => {
            canonical_shard_key_bytes(CanonicalShardKeyRef::Int64(*value))
        }
        ShardKeyValue::Text(value) => canonical_shard_key_bytes(CanonicalShardKeyRef::Text(value)),
        ShardKeyValue::Binary(value) => {
            canonical_shard_key_bytes(CanonicalShardKeyRef::Binary(value))
        }
    };
    match canonical {
        Cow::Borrowed(value) => Arc::from(value),
        Cow::Owned(value) => Arc::from(value),
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
            ShardKeyType, TableDeclaration, TableMetadata, TablePlacement, VIRTUAL_BUCKET_COUNT,
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

    fn native_range_catalog() -> Catalog {
        Catalog::from_validated_parts(
            1,
            7,
            DEFAULT_DATABASE,
            vec![LogicalDatabaseMetadata::from_validated(
                DEFAULT_DATABASE,
                "default".to_owned(),
            )]
            .into_boxed_slice(),
            vec![TableMetadata::from_validated_with_generated_id_policy(
                EVENTS_TABLE,
                DEFAULT_DATABASE,
                "native_events".to_owned(),
                TablePlacement::Sharded(ShardKeyMetadata::from_validated(
                    "id".to_owned(),
                    ShardKeyType::Int64,
                )),
                GeneratedIdPolicy::native_range_v1("id").unwrap(),
            )]
            .into_boxed_slice(),
        )
    }

    fn hilo_catalog() -> Catalog {
        Catalog::from_validated_parts(
            1,
            7,
            DEFAULT_DATABASE,
            vec![LogicalDatabaseMetadata::from_validated(
                DEFAULT_DATABASE,
                "default".to_owned(),
            )]
            .into_boxed_slice(),
            vec![TableMetadata::from_validated_with_generated_id_policy(
                EVENTS_TABLE,
                DEFAULT_DATABASE,
                "hilo_events".to_owned(),
                TablePlacement::Sharded(ShardKeyMetadata::from_validated(
                    "id".to_owned(),
                    ShardKeyType::Int64,
                )),
                GeneratedIdPolicy::hilo_v1("id").unwrap(),
            )]
            .into_boxed_slice(),
        )
    }

    fn owner_map(shard_count: u16) -> AllocationOwnerMap {
        AllocationOwnerMap::try_from_pairs(
            shard_count,
            (0..shard_count)
                .map(|shard| (shard, shard))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
        .unwrap()
    }

    fn register_engine_catalog_fixture(database: &mut Database) {
        database
            .broadcast(
                "CREATE TABLE events (
                    tenant_id INTEGER PRIMARY KEY,
                    payload TEXT NOT NULL
                 );",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical_database,
                    "events",
                    ShardKeyMetadata::new("tenant_id", ShardKeyType::Int64).unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
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

    fn distinct_key_with_same_shard(routing: &RoutingCatalog, reference: &[u8]) -> Vec<u8> {
        let shard = routing.shard_for_key(reference);
        (0_u64..)
            .map(|candidate| format!("same-shard-{candidate}").into_bytes())
            .find(|candidate| {
                candidate.as_slice() != reference && routing.shard_for_key(candidate) == shard
            })
            .unwrap()
    }

    fn same_shard_int_pair(routing: &RoutingCatalog) -> (i64, i64) {
        let first = 0_i64;
        let shard = routing.shard_for_key(first.to_string().as_bytes());
        let second = (1_i64..)
            .find(|candidate| routing.shard_for_key(candidate.to_string().as_bytes()) == shard)
            .unwrap();
        (first, second)
    }

    fn split_test_router(key: &[u8]) -> u16 {
        match key {
            b"7" | b"same-shard-context" => 1,
            b"8" => 2,
            _ => 3,
        }
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
        plan_with_router(
            catalog,
            database,
            normalized,
            statement_index,
            parameters,
            explicit_routing_key,
            |key| routing.shard_for_key(key),
        )
    }

    fn plan_with_router<F>(
        catalog: &Catalog,
        database: u64,
        normalized: &NormalizedSql,
        statement_index: usize,
        parameters: &[Value],
        explicit_routing_key: Option<&[u8]>,
        shard_for_key: F,
    ) -> EngineResult<BoundStatementPlan>
    where
        F: FnMut(&[u8]) -> u16,
    {
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
                HASH_VERSION,
                KEY_ENCODING_VERSION,
                BUCKET_ALGORITHM_VERSION,
                INITIAL_MAP_GENERATION,
            ),
            shard_for_key,
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
        let routing = routing_catalog(4);
        let explicit_routing_key = distinct_key_with_same_shard(&routing, b"private-tenant");
        let plan = plan(
            &catalog,
            TENANT_DATABASE,
            &normalized,
            0,
            &[],
            Some(&explicit_routing_key),
        )
        .unwrap();
        assert_eq!(plan.clone(), plan);
        assert_eq!(
            plan.behavior(),
            StatementBehavior::Write(crate::sql::WriteBehavior::Insert)
        );
        assert_eq!(plan.inferred_routes()[0].key_bytes(), b"private-tenant");
        assert_eq!(
            plan.explicit_route().unwrap().key_bytes(),
            explicit_routing_key
        );

        let debug = format!("{plan:?} {:?}", plan.explicit_route().unwrap());
        assert!(!debug.contains("private-tenant"));
        assert!(!debug.contains(std::str::from_utf8(&explicit_routing_key).unwrap()));
        assert!(debug.contains("<redacted>"));
        assert_eq!(
            plan.assigned_shard(),
            Some(plan.inferred_routes()[0].shard())
        );
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
                Some(b"42"),
            )
            .unwrap()
        });

        assert_eq!(plans[0], plans[1]);
        assert_eq!(plans[1], plans[2]);
        assert_eq!(plans[0].inferred_routes()[0].key_bytes(), b"42");
    }

    #[test]
    fn equivalent_typed_writes_produce_equal_assignments_across_dialects() {
        let catalog = sample_catalog();
        let plans = [
            (
                SqlDialect::Sqlite,
                "UPDATE events SET payload = ?1 WHERE tenant_id = ?2",
            ),
            (
                SqlDialect::PostgreSql,
                "UPDATE events SET payload = $1 WHERE tenant_id = $2",
            ),
            (
                SqlDialect::MySql,
                "UPDATE events SET payload = ? WHERE tenant_id = ?",
            ),
        ]
        .map(|(dialect, source)| {
            plan(
                &catalog,
                DEFAULT_DATABASE,
                &normalize(dialect, source),
                0,
                &[Value::Text("same-write".to_owned()), Value::Int64(42)],
                Some(b"42"),
            )
            .unwrap()
        });

        assert_eq!(plans[0], plans[1]);
        assert_eq!(plans[1], plans[2]);
        assert_eq!(
            plans[0].assigned_shard(),
            Some(plans[0].inferred_routes()[0].shard())
        );
    }

    #[test]
    fn omitted_generated_key_plans_one_row_without_a_caller_selected_route() {
        let owners = owner_map(4);
        for dialect in SqlDialect::ALL.iter().copied() {
            for (catalog, table_name, expected_policy) in [
                (
                    native_range_catalog(),
                    "native_events",
                    GeneratedIdPolicy::native_range_v1("id").unwrap(),
                ),
                (
                    hilo_catalog(),
                    "hilo_events",
                    GeneratedIdPolicy::hilo_v1("id").unwrap(),
                ),
            ] {
                let normalized = normalize(
                    dialect,
                    &format!("INSERT INTO {table_name} (payload) VALUES ('one')"),
                );
                let plan = plan_bound_statement(
                    BoundStatementPlanInput::new(
                        &catalog,
                        database_id(DEFAULT_DATABASE),
                        &normalized,
                        0,
                        &[],
                        Some(b"caller-route-must-not-select-generated-owner"),
                    )
                    .with_allocation_owners(Some(&owners)),
                    RoutingProvenance::new(1, 1, 1, 1),
                    |_| panic!("omitted generated INSERT must not hash a caller route"),
                )
                .unwrap();

                assert_eq!(
                    plan.inference().kind(),
                    ShardKeyInferenceKind::Unconstrained
                );
                assert!(plan.inferred_routes().is_empty());
                assert_eq!(plan.explicit_route(), None);
                assert_eq!(plan.assigned_shard(), None);
                let generated = plan.generated_insert().unwrap();
                assert_eq!(generated.table_id(), TableId::new(EVENTS_TABLE).unwrap());
                assert_eq!(generated.policy(), &expected_policy);
                let debug = format!("{plan:?}");
                assert!(debug.contains("has_generated_insert: true"));
                assert!(!debug.contains("caller-route"));
            }
        }
    }

    #[test]
    fn explicit_generated_column_keeps_existing_routing_and_null_validation() {
        let catalog = native_range_catalog();
        let owners = owner_map(4);
        let owner = owners.owner_for_physical_shard(2).unwrap();
        let explicit_id = crate::core::generated_id::NativeRangeV1Id::new(owner, 41)
            .unwrap()
            .encode();

        for dialect in SqlDialect::ALL.iter().copied() {
            let normalized = normalize(
                dialect,
                &format!("INSERT INTO native_events (id, payload) VALUES ({explicit_id}, 'one')"),
            );
            let explicit = explicit_id.to_string();
            let plan = plan_bound_statement(
                BoundStatementPlanInput::new(
                    &catalog,
                    database_id(DEFAULT_DATABASE),
                    &normalized,
                    0,
                    &[],
                    Some(explicit.as_bytes()),
                )
                .with_allocation_owners(Some(&owners)),
                RoutingProvenance::new(1, 1, 1, 1),
                |_| 3,
            )
            .unwrap();
            assert_eq!(plan.generated_insert(), None);
            assert_eq!(plan.assigned_shard(), Some(2));
            assert_eq!(plan.explicit_route().unwrap().shard(), 2);

            let explicit_null = normalize(
                dialect,
                "INSERT INTO native_events (id, payload) VALUES (NULL, 'one')",
            );
            let error = plan_bound_statement(
                BoundStatementPlanInput::new(
                    &catalog,
                    database_id(DEFAULT_DATABASE),
                    &explicit_null,
                    0,
                    &[],
                    None,
                )
                .with_allocation_owners(Some(&owners)),
                RoutingProvenance::new(1, 1, 1, 1),
                |_| panic!("explicit NULL must fail before routing"),
            )
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::NotNullViolation);
        }
    }

    #[test]
    fn multi_row_omitted_generation_is_rejected_before_any_route_is_selected() {
        let owners = owner_map(4);
        for dialect in SqlDialect::ALL.iter().copied() {
            for (catalog, table_name) in [
                (native_range_catalog(), "native_events"),
                (hilo_catalog(), "hilo_events"),
            ] {
                let normalized = normalize(
                    dialect,
                    &format!("INSERT INTO {table_name} (payload) VALUES ('one'), ('two')"),
                );
                let error = plan_bound_statement(
                    BoundStatementPlanInput::new(
                        &catalog,
                        database_id(DEFAULT_DATABASE),
                        &normalized,
                        0,
                        &[],
                        Some(b"must-not-be-routed"),
                    )
                    .with_allocation_owners(Some(&owners)),
                    RoutingProvenance::new(1, 1, 1, 1),
                    |_| panic!("multi-row omitted generation must fail structurally"),
                )
                .unwrap_err();
                assert_eq!(error.kind(), EngineErrorKind::Unsupported);
                assert!(error.diagnostic().contains("multi-row"));
                assert!(!error.diagnostic().contains(table_name));
            }
        }
    }

    #[test]
    fn omitted_shard_key_without_a_generated_policy_remains_rejected() {
        let catalog = sample_catalog();
        let normalized = normalize(
            SqlDialect::Sqlite,
            "INSERT INTO events (payload) VALUES ('one')",
        );
        let error = plan_with_router(
            &catalog,
            DEFAULT_DATABASE,
            &normalized,
            0,
            &[],
            None,
            |_| panic!("missing ordinary shard key must fail before routing"),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
        assert!(error.diagnostic().contains("proven routing key"));
    }

    #[test]
    fn every_inference_kind_has_a_stable_read_assignment() {
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
            let without_explicit =
                plan(&catalog, DEFAULT_DATABASE, &normalized, 0, &[], None).unwrap();
            assert_eq!(without_explicit.inference().kind(), kind, "{source}");
            assert_eq!(
                without_explicit.inferred_routes().len(),
                route_count,
                "{source}"
            );
            let inferred_shards = inferred_shards(without_explicit.inferred_routes());
            let expected = match inferred_shards {
                InferredShards::One(shard) => Some(shard),
                InferredShards::None | InferredShards::Multiple => None,
            };
            assert_eq!(without_explicit.assigned_shard(), expected, "{source}");

            if route_count == 0 {
                let explicit = b"fallback";
                let plan = plan(
                    &catalog,
                    DEFAULT_DATABASE,
                    &normalized,
                    0,
                    &[],
                    Some(explicit),
                )
                .unwrap();
                assert_eq!(plan.explicit_route().unwrap().key_bytes(), explicit);
                let expected = matches!(
                    kind,
                    ShardKeyInferenceKind::Unconstrained | ShardKeyInferenceKind::Contradiction
                )
                .then_some(plan.explicit_route().unwrap().shard());
                assert_eq!(plan.assigned_shard(), expected, "{source}");
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
        let routing = routing_catalog(4);
        let (first, second) = same_shard_int_pair(&routing);
        let plan = plan(
            &catalog,
            DEFAULT_DATABASE,
            &normalized,
            0,
            &[
                Value::Int64(first),
                Value::Int64(second),
                Value::Int64(first),
            ],
            None,
        )
        .unwrap();

        assert_eq!(plan.inference().kind(), ShardKeyInferenceKind::Multiple);
        assert_eq!(
            plan.inferred_routes()
                .iter()
                .map(PlannedRoute::key_bytes)
                .collect::<Vec<_>>(),
            [
                first.to_string().as_bytes(),
                second.to_string().as_bytes(),
                first.to_string().as_bytes()
            ]
        );
        assert!(Arc::ptr_eq(
            &plan.inferred_routes[0].key_bytes,
            &plan.inferred_routes[2].key_bytes
        ));
        assert_eq!(plan.inferred_routes[0], plan.inferred_routes[2]);
        assert_eq!(
            plan.assigned_shard(),
            Some(plan.inferred_routes()[0].shard())
        );
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
        assert_eq!(plan.assigned_shard(), Some(3));
    }

    #[test]
    fn explicit_routes_compare_physical_shards_and_never_key_bytes() {
        let catalog = sample_catalog();
        let normalized = normalize(
            SqlDialect::PostgreSql,
            "UPDATE events SET payload = 1 WHERE tenant_id = 7",
        );

        for explicit in [b"7".as_slice(), b"same-shard-context".as_slice()] {
            let plan = plan_with_router(
                &catalog,
                DEFAULT_DATABASE,
                &normalized,
                0,
                &[],
                Some(explicit),
                split_test_router,
            )
            .unwrap();
            assert_eq!(plan.inferred_routes()[0].shard(), 1);
            assert_eq!(plan.explicit_route().unwrap().key_bytes(), explicit);
            assert_eq!(plan.explicit_route().unwrap().shard(), 1);
            assert_eq!(plan.assigned_shard(), Some(1));
        }

        for explicit in [
            b"different-shard-context".as_slice(),
            b"".as_slice(),
            b"private\0\xffcontext".as_slice(),
        ] {
            let error = plan_with_router(
                &catalog,
                DEFAULT_DATABASE,
                &normalized,
                0,
                &[],
                Some(explicit),
                split_test_router,
            )
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
            let diagnostic = error.to_string();
            assert!(!diagnostic.contains("different-shard-context"));
            assert!(!diagnostic.contains("private"));
        }
    }

    #[test]
    fn native_ids_route_by_persisted_owner_and_canonical_explicit_context() {
        let catalog = native_range_catalog();
        let owners = owner_map(4);
        let owner = owners.owner_for_physical_shard(2).unwrap();
        let value = crate::core::generated_id::NativeRangeV1Id::new(owner, 41)
            .unwrap()
            .encode();
        let normalized = normalize(
            SqlDialect::Sqlite,
            "INSERT INTO native_events (id) VALUES (?1)",
        );
        let explicit = value.to_string();
        let plan = plan_bound_statement(
            BoundStatementPlanInput::new(
                &catalog,
                database_id(DEFAULT_DATABASE),
                &normalized,
                0,
                &[Value::Int64(value)],
                Some(explicit.as_bytes()),
            )
            .with_allocation_owners(Some(&owners)),
            RoutingProvenance::new(1, 1, 1, 1),
            |_| 3,
        )
        .unwrap();

        assert_eq!(plan.inferred_routes()[0].shard(), 2);
        assert_eq!(plan.explicit_route().unwrap().shard(), 2);
        assert_eq!(plan.assigned_shard(), Some(2));
    }

    #[test]
    fn native_policy_preserves_legacy_hash_routes_exactly() {
        let catalog = native_range_catalog();
        let owners = owner_map(4);
        let routing = routing_catalog(4);
        for value in [-19_i64, 0, 42, 0x3fff_ffff_ffff_ffff] {
            let normalized = normalize(
                SqlDialect::Sqlite,
                "SELECT * FROM native_events WHERE id = ?1",
            );
            let expected = routing.shard_for_key(value.to_string().as_bytes());
            let plan = plan_bound_statement(
                BoundStatementPlanInput::new(
                    &catalog,
                    database_id(DEFAULT_DATABASE),
                    &normalized,
                    0,
                    &[Value::Int64(value)],
                    None,
                )
                .with_allocation_owners(Some(&owners)),
                RoutingProvenance::new(1, 1, 1, 1),
                |key| routing.shard_for_key(key),
            )
            .unwrap();
            assert_eq!(plan.inferred_routes()[0].shard(), expected, "{value}");
        }
    }

    #[test]
    fn hilo_ids_and_pre_marker_legacy_values_use_the_frozen_hash_route() {
        let catalog = hilo_catalog();
        let routing = routing_catalog(4);
        let generated = [
            crate::core::generated_id::HiloV1Id::new(1)
                .unwrap()
                .encode(),
            crate::core::generated_id::HiloV1Id::new(41)
                .unwrap()
                .encode(),
            crate::core::generated_id::HiloV1Id::new(
                crate::core::generated_id::MAX_HILO_V1_SEQUENCE,
            )
            .unwrap()
            .encode(),
        ];
        let legacy = [
            i64::MIN,
            -1,
            0,
            1,
            (crate::core::generated_id::HILO_V1_FORMAT_MARKER - 1) as i64,
        ];

        for value in generated.into_iter().chain(legacy) {
            for source in [
                "SELECT * FROM hilo_events WHERE id = ?1",
                "DELETE FROM hilo_events WHERE id = ?1",
            ] {
                let normalized = normalize(SqlDialect::Sqlite, source);
                let expected = routing.shard_for_key(value.to_string().as_bytes());
                let explicit = value.to_string();
                let plan = plan_bound_statement(
                    BoundStatementPlanInput::new(
                        &catalog,
                        database_id(DEFAULT_DATABASE),
                        &normalized,
                        0,
                        &[Value::Int64(value)],
                        Some(explicit.as_bytes()),
                    ),
                    RoutingProvenance::new(1, 1, 1, 1),
                    |key| routing.shard_for_key(key),
                )
                .unwrap();
                assert_eq!(plan.inferred_routes()[0].shard(), expected, "{value}");
                assert_eq!(plan.explicit_route().unwrap().shard(), expected, "{value}");
                assert_eq!(plan.assigned_shard(), Some(expected), "{value}");
            }
        }
    }

    #[test]
    fn hilo_explicit_insert_rejects_the_entire_allocator_namespace_before_routing() {
        let catalog = hilo_catalog();
        let normalized = normalize(
            SqlDialect::Sqlite,
            "INSERT INTO hilo_events (id) VALUES (?1)",
        );
        for value in [
            crate::core::generated_id::hilo_v1_sequence_floor(),
            crate::core::generated_id::hilo_v1_first_id(),
            0x3000_0000_0000_0041,
            crate::core::generated_id::hilo_v1_sequence_ceiling(),
            crate::core::generated_id::NATIVE_RANGE_V1_FORMAT_MARKER as i64,
            0x5000_0000_0000_0041,
            i64::MAX,
        ] {
            let error = plan_bound_statement(
                BoundStatementPlanInput::new(
                    &catalog,
                    database_id(DEFAULT_DATABASE),
                    &normalized,
                    0,
                    &[Value::Int64(value)],
                    None,
                ),
                RoutingProvenance::new(1, 1, 1, 1),
                |_| panic!("allocator-owned INSERT must fail before routing"),
            )
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
            assert!(error.diagnostic().contains("allocator-owned"));
        }

        let multirow = normalize(
            SqlDialect::Sqlite,
            "INSERT INTO hilo_events (id) VALUES (?1), (?2)",
        );
        let error = plan_bound_statement(
            BoundStatementPlanInput::new(
                &catalog,
                database_id(DEFAULT_DATABASE),
                &multirow,
                0,
                &[
                    Value::Int64(41),
                    Value::Int64(crate::core::generated_id::hilo_v1_first_id()),
                ],
                None,
            ),
            RoutingProvenance::new(1, 1, 1, 1),
            |_| panic!("a mixed multi-row INSERT must fail before routing"),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(error.diagnostic().contains("allocator-owned"));
    }

    #[test]
    fn hilo_explicit_insert_still_accepts_pre_marker_legacy_values() {
        let catalog = hilo_catalog();
        let routing = routing_catalog(4);
        let normalized = normalize(
            SqlDialect::Sqlite,
            "INSERT INTO hilo_events (id) VALUES (?1)",
        );
        for value in [
            i64::MIN,
            -1,
            0,
            1,
            (crate::core::generated_id::HILO_V1_FORMAT_MARKER - 1) as i64,
        ] {
            let expected = routing.shard_for_key(value.to_string().as_bytes());
            let plan = plan_bound_statement(
                BoundStatementPlanInput::new(
                    &catalog,
                    database_id(DEFAULT_DATABASE),
                    &normalized,
                    0,
                    &[Value::Int64(value)],
                    None,
                ),
                RoutingProvenance::new(1, 1, 1, 1),
                |key| routing.shard_for_key(key),
            )
            .unwrap();
            assert_eq!(plan.assigned_shard(), Some(expected), "{value}");
        }
    }

    #[test]
    fn hilo_reads_fail_closed_on_reserved_or_incompatible_namespaces() {
        let catalog = hilo_catalog();
        let normalized = normalize(
            SqlDialect::Sqlite,
            "SELECT * FROM hilo_events WHERE id = ?1",
        );
        for value in [
            crate::core::generated_id::hilo_v1_sequence_floor(),
            crate::core::generated_id::NATIVE_RANGE_V1_FORMAT_MARKER as i64,
            i64::MAX,
        ] {
            let error = plan_bound_statement(
                BoundStatementPlanInput::new(
                    &catalog,
                    database_id(DEFAULT_DATABASE),
                    &normalized,
                    0,
                    &[Value::Int64(value)],
                    None,
                ),
                RoutingProvenance::new(1, 1, 1, 1),
                |_| 0,
            )
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidArgument, "{value}");
        }
    }

    #[test]
    fn retired_native_owner_routes_reads_but_rejects_new_explicit_ids() {
        let catalog = native_range_catalog();
        let owners = AllocationOwnerMap::try_from_assignments(
            2,
            vec![
                (
                    0,
                    0,
                    crate::core::generated_id::AllocationOwnerState::Retired,
                ),
                (
                    2,
                    0,
                    crate::core::generated_id::AllocationOwnerState::Active,
                ),
                (
                    1,
                    1,
                    crate::core::generated_id::AllocationOwnerState::Active,
                ),
            ]
            .into_boxed_slice(),
        )
        .unwrap();
        let retired_id = crate::core::generated_id::NativeRangeV1Id::new(
            crate::core::generated_id::AllocationOwnerSlot::new(0).unwrap(),
            41,
        )
        .unwrap()
        .encode();

        for source in [
            "SELECT * FROM native_events WHERE id = ?1",
            "DELETE FROM native_events WHERE id = ?1",
        ] {
            let normalized = normalize(SqlDialect::Sqlite, source);
            let plan = plan_bound_statement(
                BoundStatementPlanInput::new(
                    &catalog,
                    database_id(DEFAULT_DATABASE),
                    &normalized,
                    0,
                    &[Value::Int64(retired_id)],
                    None,
                )
                .with_allocation_owners(Some(&owners)),
                RoutingProvenance::new(1, 1, 1, 1),
                |_| 1,
            )
            .unwrap();
            assert_eq!(plan.assigned_shard(), Some(0), "{source}");
        }

        let insert = normalize(
            SqlDialect::Sqlite,
            "INSERT INTO native_events (id) VALUES (?1)",
        );
        let error = plan_bound_statement(
            BoundStatementPlanInput::new(
                &catalog,
                database_id(DEFAULT_DATABASE),
                &insert,
                0,
                &[Value::Int64(retired_id)],
                None,
            )
            .with_allocation_owners(Some(&owners)),
            RoutingProvenance::new(1, 1, 1, 1),
            |_| 1,
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(error.diagnostic().contains("retired"));
    }

    #[test]
    fn reserved_or_unassigned_native_owners_fail_before_execution() {
        let catalog = native_range_catalog();
        let owners = owner_map(4);
        let normalized = normalize(
            SqlDialect::Sqlite,
            "INSERT INTO native_events (id) VALUES (?1)",
        );
        let reserved = crate::core::generated_id::native_range_v1_sequence_floor(
            owners.owner_for_physical_shard(1).unwrap(),
        );
        let unassigned = crate::core::generated_id::NativeRangeV1Id::new(
            crate::core::generated_id::AllocationOwnerSlot::new(9).unwrap(),
            1,
        )
        .unwrap()
        .encode();

        for (value, kind) in [
            (reserved, EngineErrorKind::InvalidArgument),
            (unassigned, EngineErrorKind::FailedPrecondition),
        ] {
            let error = plan_bound_statement(
                BoundStatementPlanInput::new(
                    &catalog,
                    database_id(DEFAULT_DATABASE),
                    &normalized,
                    0,
                    &[Value::Int64(value)],
                    None,
                )
                .with_allocation_owners(Some(&owners)),
                RoutingProvenance::new(1, 1, 1, 1),
                |_| 0,
            )
            .unwrap_err();
            assert_eq!(error.kind(), kind, "{value}: {error}");
        }
    }

    #[test]
    fn multi_key_writes_require_one_physical_shard_while_reads_defer_scatter() {
        let catalog = sample_catalog();
        let writes = [
            "INSERT INTO events (tenant_id) VALUES (7), (8)",
            "UPDATE events SET payload = 1 WHERE tenant_id = 7 OR tenant_id = 8",
            "DELETE FROM events WHERE tenant_id = 7 OR tenant_id = 8",
        ];

        for dialect in SqlDialect::ALL.iter().copied() {
            for source in writes {
                let normalized = normalize(dialect, source);
                let error = plan_with_router(
                    &catalog,
                    DEFAULT_DATABASE,
                    &normalized,
                    0,
                    &[],
                    None,
                    split_test_router,
                )
                .unwrap_err();
                assert_eq!(
                    error.kind(),
                    EngineErrorKind::InvalidQuery,
                    "{dialect}: {source}"
                );

                let conflict = plan_with_router(
                    &catalog,
                    DEFAULT_DATABASE,
                    &normalized,
                    0,
                    &[],
                    Some(b"7"),
                    split_test_router,
                )
                .unwrap_err();
                assert_eq!(
                    conflict.kind(),
                    EngineErrorKind::InvalidArgument,
                    "{dialect}: {source}"
                );

                let co_located = plan_with_router(
                    &catalog,
                    DEFAULT_DATABASE,
                    &normalized,
                    0,
                    &[],
                    Some(b"different-logical-key"),
                    |_| 2,
                )
                .unwrap();
                assert_eq!(co_located.inferred_routes().len(), 2);
                assert!(
                    co_located
                        .inferred_routes()
                        .iter()
                        .all(|route| route.shard() == 2)
                );
                assert_eq!(co_located.assigned_shard(), Some(2));
            }
        }

        let read = normalize(
            SqlDialect::Sqlite,
            "SELECT * FROM events WHERE tenant_id = 7 OR tenant_id = 8",
        );
        let deferred = plan_with_router(
            &catalog,
            DEFAULT_DATABASE,
            &read,
            0,
            &[],
            None,
            split_test_router,
        )
        .unwrap();
        assert_eq!(deferred.inference().kind(), ShardKeyInferenceKind::Multiple);
        assert_eq!(deferred.assigned_shard(), None);
        assert_eq!(
            deferred
                .inferred_routes()
                .iter()
                .map(PlannedRoute::shard)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(
            plan_with_router(
                &catalog,
                DEFAULT_DATABASE,
                &read,
                0,
                &[],
                Some(b"7"),
                split_test_router,
            )
            .unwrap_err()
            .kind(),
            EngineErrorKind::InvalidArgument
        );
    }

    #[test]
    fn predicate_writes_need_explicit_fallback_when_inference_has_no_route() {
        let catalog = sample_catalog();
        let statements = [
            (
                "UPDATE events SET payload = 1",
                ShardKeyInferenceKind::Unconstrained,
            ),
            (
                "UPDATE events SET payload = 1 WHERE tenant_id = NULL",
                ShardKeyInferenceKind::Contradiction,
            ),
            ("DELETE FROM events", ShardKeyInferenceKind::Unconstrained),
            (
                "DELETE FROM events WHERE tenant_id = NULL",
                ShardKeyInferenceKind::Contradiction,
            ),
        ];

        for dialect in SqlDialect::ALL.iter().copied() {
            for (source, kind) in statements {
                let normalized = normalize(dialect, source);
                let error = plan_with_router(
                    &catalog,
                    DEFAULT_DATABASE,
                    &normalized,
                    0,
                    &[],
                    None,
                    split_test_router,
                )
                .unwrap_err();
                assert_eq!(
                    error.kind(),
                    EngineErrorKind::InvalidArgument,
                    "{dialect}: {source}"
                );

                for explicit in [b"".as_slice(), b"fallback\0\xff".as_slice()] {
                    let plan = plan_with_router(
                        &catalog,
                        DEFAULT_DATABASE,
                        &normalized,
                        0,
                        &[],
                        Some(explicit),
                        split_test_router,
                    )
                    .unwrap();
                    assert_eq!(plan.inference().kind(), kind);
                    assert!(plan.inferred_routes().is_empty());
                    assert_eq!(
                        plan.assigned_shard(),
                        Some(plan.explicit_route().unwrap().shard())
                    );
                }
            }
        }
    }

    #[test]
    fn inserts_without_a_proven_row_key_reject_even_with_explicit_context() {
        let catalog = sample_catalog();
        let sources = [
            "INSERT INTO events (payload) VALUES (1)",
            "INSERT INTO events (tenant_id) VALUES (1 + 0)",
        ];

        for dialect in SqlDialect::ALL.iter().copied() {
            for source in sources {
                let normalized = normalize(dialect, source);
                for explicit in [None, Some(b"fallback".as_slice())] {
                    let error = plan_with_router(
                        &catalog,
                        DEFAULT_DATABASE,
                        &normalized,
                        0,
                        &[],
                        explicit,
                        split_test_router,
                    )
                    .unwrap_err();
                    assert_eq!(
                        error.kind(),
                        EngineErrorKind::InvalidQuery,
                        "{dialect}: {source}"
                    );
                }
            }
        }
    }

    #[test]
    fn shard_key_updates_are_rejected_before_other_routing_errors() {
        let catalog = sample_catalog();
        let sources = [
            "UPDATE events SET tenant_id = tenant_id WHERE tenant_id = 7",
            "UPDATE events SET TENANT_ID = 7 WHERE tenant_id = 7",
            "UPDATE events SET payload = 1, tenant_id = 8 WHERE tenant_id = NULL",
        ];

        for dialect in SqlDialect::ALL.iter().copied() {
            for source in sources {
                let error = plan_with_router(
                    &catalog,
                    DEFAULT_DATABASE,
                    &normalize(dialect, source),
                    0,
                    &[],
                    Some(b"different-shard-context"),
                    split_test_router,
                )
                .unwrap_err();
                assert_eq!(
                    error.kind(),
                    EngineErrorKind::InvalidQuery,
                    "{dialect}: {source}"
                );
                assert!(!error.to_string().contains("tenant_id"));
            }
        }

        for (dialect, source) in [
            (
                SqlDialect::Sqlite,
                "UPDATE events SET tenant_id = ?1 WHERE tenant_id = ?2",
            ),
            (
                SqlDialect::PostgreSql,
                "UPDATE events SET tenant_id = $1 WHERE tenant_id = $2",
            ),
            (
                SqlDialect::MySql,
                "UPDATE events SET tenant_id = ? WHERE tenant_id = ?",
            ),
        ] {
            let error = plan_with_router(
                &catalog,
                DEFAULT_DATABASE,
                &normalize(dialect, source),
                0,
                &[Value::Int64(7), Value::Int64(7)],
                None,
                split_test_router,
            )
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidQuery, "{dialect}");
        }

        let inference_first = plan_with_router(
            &catalog,
            DEFAULT_DATABASE,
            &normalize(
                SqlDialect::PostgreSql,
                "UPDATE events SET tenant_id = $1 WHERE tenant_id = $2",
            ),
            0,
            &[Value::Int64(7)],
            None,
            split_test_router,
        )
        .unwrap_err();
        assert_eq!(inference_first.kind(), EngineErrorKind::InvalidArgument);

        let batch = normalize(
            SqlDialect::Sqlite,
            "UPDATE events SET payload = 1 WHERE tenant_id = 7; \
             UPDATE events SET tenant_id = 8 WHERE tenant_id = 7",
        );
        let batch_error = plan_with_router(
            &catalog,
            DEFAULT_DATABASE,
            &batch,
            0,
            &[],
            None,
            split_test_router,
        )
        .unwrap_err();
        assert_eq!(batch_error.kind(), EngineErrorKind::Unsupported);

        let first_update = normalize(
            SqlDialect::Sqlite,
            "UPDATE events SET payload = 1 WHERE tenant_id = 7",
        );
        let first = plan_with_router(
            &catalog,
            DEFAULT_DATABASE,
            &first_update,
            0,
            &[],
            None,
            split_test_router,
        )
        .unwrap();
        assert_eq!(first.assigned_shard(), Some(1));

        let shard_key_update = normalize(
            SqlDialect::Sqlite,
            "UPDATE events SET tenant_id = 8 WHERE tenant_id = 7",
        );
        assert_eq!(
            plan_with_router(
                &catalog,
                DEFAULT_DATABASE,
                &shard_key_update,
                0,
                &[],
                None,
                split_test_router,
            )
            .unwrap_err()
            .kind(),
            EngineErrorKind::InvalidQuery
        );

        let global = plan_with_router(
            &catalog,
            DEFAULT_DATABASE,
            &normalize(SqlDialect::Sqlite, "UPDATE countries SET tenant_id = 1"),
            0,
            &[],
            None,
            split_test_router,
        )
        .unwrap();
        assert_eq!(global.inference().kind(), ShardKeyInferenceKind::NotSharded);
        assert_eq!(global.assigned_shard(), None);
    }

    #[test]
    fn routing_policy_errors_are_stateless_and_concurrent() {
        let catalog = Arc::new(sample_catalog());
        let normalized = Arc::new(normalize(
            SqlDialect::PostgreSql,
            "INSERT INTO events (tenant_id) VALUES (7), (8)",
        ));
        let barrier = Arc::new(Barrier::new(9));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let catalog = Arc::clone(&catalog);
            let normalized = Arc::clone(&normalized);
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                barrier.wait();
                plan_with_router(
                    &catalog,
                    DEFAULT_DATABASE,
                    &normalized,
                    0,
                    &[],
                    None,
                    split_test_router,
                )
                .unwrap_err()
            }));
        }
        barrier.wait();
        let errors = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert!(
            errors
                .iter()
                .all(|error| error.kind() == EngineErrorKind::InvalidQuery)
        );
        assert!(
            errors
                .windows(2)
                .all(|pair| pair[0].to_string() == pair[1].to_string())
        );

        let recovered = plan_with_router(
            &catalog,
            DEFAULT_DATABASE,
            &normalized,
            0,
            &[],
            Some(b"same-shard-context"),
            |_| 1,
        )
        .unwrap();
        assert_eq!(recovered.assigned_shard(), Some(1));

        let broad = normalize(SqlDialect::PostgreSql, "DELETE FROM events");
        assert_eq!(
            plan_with_router(
                &catalog,
                DEFAULT_DATABASE,
                &broad,
                0,
                &[],
                None,
                split_test_router,
            )
            .unwrap_err()
            .kind(),
            EngineErrorKind::InvalidArgument
        );
        assert!(
            plan_with_router(
                &catalog,
                DEFAULT_DATABASE,
                &broad,
                0,
                &[],
                Some(b"fallback"),
                split_test_router,
            )
            .is_ok()
        );
    }

    #[test]
    fn authoritative_binary_text_predicates_and_inserts_are_routed() {
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
            Some(b"tenant-a"),
        )
        .unwrap();
        assert_eq!(predicate.inference().kind(), ShardKeyInferenceKind::Exact);
        assert_eq!(predicate.inferred_routes()[0].key_bytes(), b"tenant-a");
        assert_eq!(predicate.explicit_route().unwrap().key_bytes(), b"tenant-a");

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
    fn whole_batch_policy_precedes_member_planning_and_allows_read_batches() {
        let catalog = sample_catalog();

        let empty = normalize(SqlDialect::Sqlite, "-- no statements");
        let empty_error = plan(&catalog, DEFAULT_DATABASE, &empty, 0, &[], None).unwrap_err();
        assert_eq!(empty_error.kind(), EngineErrorKind::InvalidArgument);
        assert_eq!(
            empty_error.diagnostic(),
            "a SQL request must contain at least one top-level statement"
        );

        let schema_batch = normalize(
            SqlDialect::Sqlite,
            "SELECT 1; CREATE TABLE later_table (id INTEGER)",
        );
        let schema_error = plan(
            &catalog,
            DEFAULT_DATABASE,
            &schema_batch,
            usize::MAX,
            &[],
            None,
        )
        .unwrap_err();
        assert_eq!(schema_error.kind(), EngineErrorKind::Unsupported);
        assert_eq!(
            schema_error.diagnostic(),
            "statement 2 has schema behavior; multi-statement requests may contain only read statements"
        );

        let session_batch = normalize(
            SqlDialect::Sqlite,
            "SELECT payload FROM events WHERE tenant_id = ?1; BEGIN",
        );
        let parameter_error = plan_with_router(
            &catalog,
            DEFAULT_DATABASE,
            &session_batch,
            0,
            &[],
            None,
            split_test_router,
        )
        .unwrap_err();
        assert_eq!(parameter_error.kind(), EngineErrorKind::Unsupported);
        assert_eq!(
            parameter_error.diagnostic(),
            "statement 2 has session behavior; multi-statement requests may contain only read statements"
        );

        let routing_error = plan_with_router(
            &catalog,
            DEFAULT_DATABASE,
            &session_batch,
            0,
            &[Value::Int64(7)],
            Some(b"8"),
            split_test_router,
        )
        .unwrap_err();
        assert_eq!(routing_error.kind(), EngineErrorKind::Unsupported);
        assert_eq!(routing_error.diagnostic(), parameter_error.diagnostic());

        let read_batch = normalize(
            SqlDialect::Sqlite,
            "SELECT payload FROM events WHERE tenant_id = ?1; \
             SELECT payload FROM events WHERE tenant_id = ?1",
        );
        let first = plan_with_router(
            &catalog,
            DEFAULT_DATABASE,
            &read_batch,
            0,
            &[Value::Int64(7)],
            None,
            split_test_router,
        )
        .unwrap();
        let second = plan_with_router(
            &catalog,
            DEFAULT_DATABASE,
            &read_batch,
            1,
            &[Value::Int64(8)],
            None,
            split_test_router,
        )
        .unwrap();
        assert_eq!(first.behavior(), StatementBehavior::Read);
        assert_eq!(second.behavior(), StatementBehavior::Read);
        assert_eq!(first.statement_index(), 0);
        assert_eq!(second.statement_index(), 1);
        assert_eq!(first.assigned_shard(), Some(1));
        assert_eq!(second.assigned_shard(), Some(2));

        let index_error = plan(
            &catalog,
            DEFAULT_DATABASE,
            &read_batch,
            2,
            &[Value::Int64(7)],
            None,
        )
        .unwrap_err();
        assert_eq!(index_error.kind(), EngineErrorKind::InvalidArgument);
        assert_eq!(
            index_error.diagnostic(),
            "SQL statement index is outside the normalized batch"
        );
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
        assert_eq!(first.behavior(), StatementBehavior::Read);
        assert_eq!(second.behavior(), StatementBehavior::Read);
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
        let routing = routing_catalog(4);
        let (first, second) = same_shard_int_pair(&routing);
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
                    &[Value::Int64(first), Value::Int64(second)],
                    None,
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

    #[test]
    fn schema_gate_precedes_write_policy_and_a_later_valid_plan_recovers() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 2).unwrap();
        register_engine_catalog_fixture(&mut database);
        let database = Arc::new(database);
        let engine = Engine::from_database(Arc::clone(&database));
        let logical_database = engine.catalog().default_database().id();
        let broad_write = normalize(SqlDialect::Sqlite, "DELETE FROM events");
        let invalid_bind = normalize(
            SqlDialect::Sqlite,
            "UPDATE events SET payload = ?1 WHERE tenant_id = ?2",
        );

        let migration = database.storage.begin_schema_migration().unwrap();
        migration.wait_for_quiescence_blocking();
        assert_eq!(
            engine
                .plan_bound_statement(logical_database, &broad_write, 0, &[], None)
                .unwrap_err()
                .kind(),
            EngineErrorKind::Busy
        );
        assert_eq!(
            engine
                .plan_bound_statement(logical_database, &invalid_bind, 0, &[Value::Int64(1)], None,)
                .unwrap_err()
                .kind(),
            EngineErrorKind::Busy
        );
        drop(migration);

        assert_eq!(
            engine
                .plan_bound_statement(logical_database, &invalid_bind, 0, &[Value::Int64(1)], None,)
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
        assert_eq!(
            engine
                .plan_bound_statement(logical_database, &broad_write, 0, &[], None)
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
        let recovered = engine
            .plan_bound_statement(
                logical_database,
                &normalize(SqlDialect::Sqlite, "DELETE FROM events WHERE tenant_id = 7"),
                0,
                &[],
                None,
            )
            .unwrap();
        assert_eq!(
            recovered.assigned_shard(),
            Some(database.shard_for_key(b"7"))
        );
    }
}
