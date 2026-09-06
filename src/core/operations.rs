//! Protocol-neutral operational inspection and query-cancellation types.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Instant,
};

use super::{
    CancellationToken, EngineError, EngineErrorKind, EngineResult, EngineState, RequestContext,
};

/// Maximum number of cancellable frontend queries retained by one engine.
pub const MAX_ACTIVE_QUERIES: usize = 1_024;

/// A random, opaque identity for one tracked query.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueryId([u8; 16]);

impl QueryId {
    fn random() -> EngineResult<Self> {
        loop {
            let mut bytes = [0_u8; 16];
            getrandom::fill(&mut bytes).map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::StorageUnavailable,
                    "could not generate a query identity",
                    error,
                )
            })?;
            if bytes != [0; 16] {
                return Ok(Self(bytes));
            }
        }
    }

    /// Return the opaque identity bytes.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Display for QueryId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for QueryId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("QueryId")
            .field(&self.to_string())
            .finish()
    }
}

/// Failure to parse the canonical lowercase hexadecimal query identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseQueryIdError;

impl fmt::Display for ParseQueryIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("query IDs must be 32 lowercase hexadecimal characters and nonzero")
    }
}

impl Error for ParseQueryIdError {}

impl FromStr for QueryId {
    type Err = ParseQueryIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 32
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ParseQueryIdError);
        }
        let mut bytes = [0_u8; 16];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            bytes[index] = (decode_hex(pair[0]) << 4) | decode_hex(pair[1]);
        }
        if bytes == [0; 16] {
            return Err(ParseQueryIdError);
        }
        Ok(Self(bytes))
    }
}

const fn decode_hex(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => 0,
    }
}

/// Redaction-safe status for one currently tracked query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveQueryStatus {
    id: QueryId,
    elapsed_ms: u64,
    sql_bytes: usize,
    cancellation_requested: bool,
}

impl ActiveQueryStatus {
    /// Return the opaque query identity.
    pub const fn id(&self) -> QueryId {
        self.id
    }

    /// Return whole milliseconds elapsed when this snapshot was taken.
    pub const fn elapsed_ms(&self) -> u64 {
        self.elapsed_ms
    }

    /// Return the number of exact SQL bytes without exposing the SQL.
    pub const fn sql_bytes(&self) -> usize {
        self.sql_bytes
    }

    /// Return whether cancellation had been requested at snapshot time.
    pub const fn cancellation_requested(&self) -> bool {
        self.cancellation_requested
    }
}

#[derive(Debug)]
struct ActiveQueryEntry {
    started: Instant,
    sql_bytes: usize,
    cancellation: CancellationToken,
}

/// Engine-shared bounded registry for frontend query cancellation.
#[derive(Debug, Default)]
pub(crate) struct ActiveQueryRegistry {
    entries: Mutex<BTreeMap<QueryId, ActiveQueryEntry>>,
}

impl ActiveQueryRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn begin(self: &Arc<Self>, sql: &str) -> EngineResult<TrackedQuery> {
        let cancellation = CancellationToken::new();
        let entry = ActiveQueryEntry {
            started: Instant::now(),
            sql_bytes: sql.len(),
            cancellation: cancellation.clone(),
        };

        // Random collisions are retried while the mutex makes capacity and
        // insertion one atomic decision. No entropy source or cancellation
        // callback runs while the registry is locked.
        for _ in 0..16 {
            let id = QueryId::random()?;
            let mut entries = self.lock();
            if entries.len() >= MAX_ACTIVE_QUERIES {
                return Err(EngineError::new(
                    EngineErrorKind::LimitExceeded,
                    format!("active query tracking exceeds the {MAX_ACTIVE_QUERIES}-query limit"),
                ));
            }
            if entries.contains_key(&id) {
                continue;
            }
            entries.insert(
                id,
                ActiveQueryEntry {
                    started: entry.started,
                    sql_bytes: entry.sql_bytes,
                    cancellation: cancellation.clone(),
                },
            );
            return Ok(TrackedQuery {
                registry: Arc::clone(self),
                id,
                cancellation,
            });
        }
        Err(EngineError::new(
            EngineErrorKind::Internal,
            "could not allocate a unique query identity",
        ))
    }

    pub(crate) fn snapshot(&self) -> Vec<ActiveQueryStatus> {
        let now = Instant::now();
        self.lock()
            .iter()
            .map(|(&id, entry)| ActiveQueryStatus {
                id,
                elapsed_ms: u64::try_from(now.saturating_duration_since(entry.started).as_millis())
                    .unwrap_or(u64::MAX),
                sql_bytes: entry.sql_bytes,
                cancellation_requested: entry.cancellation.is_cancelled(),
            })
            .collect()
    }

    pub(crate) fn cancel(&self, id: QueryId) -> Option<bool> {
        let cancellation = self.lock().get(&id).map(|entry| entry.cancellation.clone());
        // Cancellation wakes tasks and can invoke further code, so never call
        // it under the registry mutex.
        cancellation.map(|cancellation| cancellation.cancel())
    }

    fn remove(&self, id: QueryId) {
        self.lock().remove(&id);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<QueryId, ActiveQueryEntry>> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// RAII registration for one cancellable frontend query.
#[derive(Debug)]
#[must_use = "dropping the tracked query cancels it and removes operational inspection"]
pub struct TrackedQuery {
    registry: Arc<ActiveQueryRegistry>,
    id: QueryId,
    cancellation: CancellationToken,
}

impl TrackedQuery {
    /// Return the opaque query identity.
    pub const fn id(&self) -> QueryId {
        self.id
    }

    /// Create an engine request context observing this query's cancellation.
    pub fn request_context(&self) -> RequestContext {
        RequestContext::new().with_cancellation_token(self.cancellation.clone())
    }

    /// Return a clone of this query's cancellation token.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

impl Drop for TrackedQuery {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.registry.remove(self.id);
    }
}

/// Public application-schema admission state for readiness inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchemaState {
    Ready,
    Migrating,
    Pending,
    Degraded,
}

impl SchemaState {
    /// Return the stable machine-readable state name.
    pub const fn code(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Migrating => "migrating",
            Self::Pending => "pending",
            Self::Degraded => "degraded",
        }
    }
}

/// A bounded operational-readiness snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadinessSnapshot {
    pub(crate) lifecycle_state: EngineState,
    pub(crate) schema_state: SchemaState,
    pub(crate) schema_generation: u64,
    pub(crate) active_schema_operations: usize,
}

impl ReadinessSnapshot {
    /// Return whether ordinary work can currently be admitted.
    pub const fn ready(self) -> bool {
        matches!(self.lifecycle_state, EngineState::Running)
            && matches!(self.schema_state, SchemaState::Ready)
    }

    /// Return the engine lifecycle state observed by this snapshot.
    pub const fn lifecycle_state(self) -> EngineState {
        self.lifecycle_state
    }

    /// Return the application-schema admission state observed by this snapshot.
    pub const fn schema_state(self) -> SchemaState {
        self.schema_state
    }

    /// Return the neighboring live application-schema generation.
    pub const fn schema_generation(self) -> u64 {
        self.schema_generation
    }

    /// Return the number of admitted schema operations observed by this snapshot.
    pub const fn active_schema_operations(self) -> usize {
        self.active_schema_operations
    }
}

/// Operational state of one validated physical shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShardState {
    Ready,
}

impl ShardState {
    /// Return the stable machine-readable state name.
    pub const fn code(self) -> &'static str {
        match self {
            Self::Ready => "ready",
        }
    }
}

/// Redaction-safe status of one physical shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardStatus {
    pub(crate) shard_id: u16,
    pub(crate) state: ShardState,
}

impl ShardStatus {
    /// Return the physical shard identifier.
    pub const fn shard_id(self) -> u16 {
        self.shard_id
    }

    /// Return the validated operational state.
    pub const fn state(self) -> ShardState {
        self.state
    }
}

/// All-or-nothing result of validating every physical shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardStatusReport {
    pub(crate) schema_generation: u64,
    pub(crate) shards: Vec<ShardStatus>,
}

impl ShardStatusReport {
    /// Return the application-schema generation used for validation.
    pub const fn schema_generation(&self) -> u64 {
        self.schema_generation
    }

    /// Return validated shard statuses in shard-ID order.
    pub fn shards(&self) -> &[ShardStatus] {
        &self.shards
    }
}

/// Durable state of one schema migration journal row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchemaMigrationState {
    Applying,
    Complete,
}

impl SchemaMigrationState {
    /// Return the stable machine-readable state name.
    pub const fn code(self) -> &'static str {
        match self {
            Self::Applying => "applying",
            Self::Complete => "complete",
        }
    }
}

/// Redaction-safe metadata for one schema migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaMigrationStatus {
    pub(crate) generation: u64,
    pub(crate) source_generation: u64,
    pub(crate) target_generation: u64,
    pub(crate) state: SchemaMigrationState,
    pub(crate) shard_count: u16,
    pub(crate) next_shard: u16,
    pub(crate) sql_bytes: usize,
}

impl SchemaMigrationStatus {
    /// Return the migration history lookup generation.
    pub const fn generation(self) -> u64 {
        self.generation
    }

    /// Return the schema generation from which the migration began.
    pub const fn source_generation(self) -> u64 {
        self.source_generation
    }

    /// Return the schema generation published by the migration.
    pub const fn target_generation(self) -> u64 {
        self.target_generation
    }

    /// Return the durable migration journal state.
    pub const fn state(self) -> SchemaMigrationState {
        self.state
    }

    /// Return the number of physical shards in the migration.
    pub const fn shard_count(self) -> u16 {
        self.shard_count
    }

    /// Return the next shard that still requires application.
    pub const fn next_shard(self) -> u16 {
        self.next_shard
    }

    /// Return the number of shards durably completed so far.
    pub const fn completed_shards(self) -> u16 {
        self.next_shard
    }

    /// Return the retained migration SQL length without exposing its text.
    pub const fn sql_bytes(self) -> usize {
        self.sql_bytes
    }
}

/// Bounded schema migration history summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaMigrationSummary {
    pub(crate) schema_generation: u64,
    pub(crate) active: Option<SchemaMigrationStatus>,
    pub(crate) latest_complete: Option<SchemaMigrationStatus>,
}

impl SchemaMigrationSummary {
    /// Return the manifest schema generation observed by this summary.
    pub const fn schema_generation(self) -> u64 {
        self.schema_generation
    }

    /// Return the active resumable migration, if one exists.
    pub const fn active(self) -> Option<SchemaMigrationStatus> {
        self.active
    }

    /// Return the most recently completed migration, if one exists.
    pub const fn latest_complete(self) -> Option<SchemaMigrationStatus> {
        self.latest_complete
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_ids_have_one_strict_canonical_representation() {
        let id = "0123456789abcdef0123456789abcdef"
            .parse::<QueryId>()
            .unwrap();
        assert_eq!(id.to_string(), "0123456789abcdef0123456789abcdef");
        for invalid in [
            "",
            "0123456789abcdef0123456789abcde",
            "0123456789abcdef0123456789abcdef0",
            "0123456789ABCDEF0123456789ABCDEF",
            "g123456789abcdef0123456789abcdef",
            "00000000000000000000000000000000",
        ] {
            assert!(invalid.parse::<QueryId>().is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn registry_is_sorted_redacted_cancellable_and_raii_bounded() {
        let registry = ActiveQueryRegistry::new();
        let first = registry.begin("SELECT secret FROM records").unwrap();
        let second = registry.begin("SELECT 2").unwrap();
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot.windows(2).all(|pair| pair[0].id() < pair[1].id()));
        assert!(snapshot.iter().all(|status| status.sql_bytes() > 0));
        let first_status = snapshot
            .iter()
            .find(|status| status.id() == first.id())
            .unwrap();
        assert_eq!(first_status.sql_bytes(), 26);
        assert!(!format!("{snapshot:?}").contains("secret"));

        assert_eq!(registry.cancel(first.id()), Some(true));
        assert_eq!(registry.cancel(first.id()), Some(false));
        assert!(first.cancellation_token().is_cancelled());
        assert!(
            registry
                .snapshot()
                .iter()
                .find(|status| status.id() == first.id())
                .unwrap()
                .cancellation_requested()
        );
        let first_id = first.id();
        drop(first);
        assert_eq!(registry.cancel(first_id), None);
        assert_eq!(registry.snapshot().len(), 1);
        drop(second);
        assert!(registry.snapshot().is_empty());

        let dropped = registry.begin("SELECT 3").unwrap();
        let dropped_id = dropped.id();
        let dropped_token = dropped.cancellation_token();
        assert!(!dropped_token.is_cancelled());
        drop(dropped);
        assert!(dropped_token.is_cancelled());
        assert_eq!(registry.cancel(dropped_id), None);
    }

    #[test]
    fn registry_enforces_its_exact_capacity_and_recovers_on_drop() {
        let registry = ActiveQueryRegistry::new();
        let mut queries = Vec::with_capacity(MAX_ACTIVE_QUERIES);
        for _ in 0..MAX_ACTIVE_QUERIES {
            queries.push(registry.begin("SELECT 1").unwrap());
        }
        let error = registry.begin("SELECT 2").unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);

        queries.pop();
        let replacement = registry.begin("SELECT 3").unwrap();
        assert_eq!(registry.snapshot().len(), MAX_ACTIVE_QUERIES);
        drop(replacement);
        drop(queries);
        assert!(registry.snapshot().is_empty());
    }
}
