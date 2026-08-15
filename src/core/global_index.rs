//! Durable, protocol-neutral global-index catalog metadata.

use std::fmt;

use super::{
    CanonicalIndexKey, EngineError, EngineErrorKind, EngineResult, INDEX_KEY_ENCODING_VERSION,
    IndexKeyCollation, IndexKeyOrder, IndexNullOrder, TableId, UniqueNullSemantics,
    validate_catalog_identifier,
};

pub(crate) const MAX_GLOBAL_INDEXES: usize = 4_096;
pub(crate) const MAX_GLOBAL_INDEX_PARTS: usize = 16;
pub(crate) const MAX_GLOBAL_INDEX_SQL_BYTES: usize = 4_096;
pub(crate) const MAX_GLOBAL_OWNER_LOCATOR_BYTES: usize = 4_096;
pub(crate) const MAX_GLOBAL_INDEX_READ_CANDIDATES: usize = 4_096;
pub(crate) const MAX_GLOBAL_INDEX_READ_REPAIRS: usize = 64;
pub const MAX_GLOBAL_INDEX_OUTBOX_BATCH_EVENTS: usize = 4_096;
pub const MAX_GLOBAL_INDEX_OUTBOX_EVENTS_PER_SHARD: u64 = 1_000_000;
pub const MAX_GLOBAL_INDEX_OUTBOX_BYTES_PER_SHARD: u64 = 256 * 1024 * 1024;
pub const DEFAULT_GLOBAL_INDEX_ASYNC_BATCH_EVENTS: usize = 1_024;
pub const DEFAULT_GLOBAL_INDEX_ASYNC_LEASE_MS: u64 = 5_000;
pub const DEFAULT_GLOBAL_INDEX_ASYNC_POLL_MS: u64 = 25;
pub(crate) const MAX_GLOBAL_VALUE_LEASE_COUNT: u32 = 65_536;
const DEFAULT_MAX_REPORTED_VALIDATION_ISSUES: u16 = 128;
const MAX_REPORTED_VALIDATION_ISSUES: u16 = 1_024;
const MAX_VALIDATION_SAMPLES_PER_SHARD: u16 = 4_096;

/// Version-1 comparison and migration target for hash-partitioned index storage.
pub const HASH_PARTITIONED_GLOBAL_INDEX_PARTITIONS_V1: u16 = 16;

const PARTITION_ROUTING_DOMAIN_V1: &[u8] = b"briskdb.global-index.partition.v1\0";

/// Stable identity of one durable global index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GlobalIndexId(u64);

impl GlobalIndexId {
    /// Construct a positive stable global-index identity.
    pub fn new(value: u64) -> EngineResult<Self> {
        if value == 0 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "global-index IDs must be positive",
            ));
        }
        Ok(Self(value))
    }

    pub(crate) const fn from_validated(value: u64) -> Self {
        debug_assert!(value > 0);
        Self(value)
    }

    /// Return the persisted numeric identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for GlobalIndexId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Caller-owned, stable identity for one recoverable global authority operation.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GlobalOperationId([u8; 16]);

impl GlobalOperationId {
    /// Validate a nonzero operation identity. Retrying with the same identity
    /// and exact request returns the original durable result.
    pub fn new(value: [u8; 16]) -> EngineResult<Self> {
        if value == [0; 16] {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "global operation IDs must not be all zero",
            ));
        }
        Ok(Self(value))
    }

    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for GlobalOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("GlobalOperationId")
            .field(&format_args!("{:02x}{:02x}…", self.0[0], self.0[1]))
            .finish()
    }
}

/// Opaque physical owner recorded by the global uniqueness authority.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct GlobalIndexOwner {
    source_shard: u16,
    locator: Box<[u8]>,
}

impl GlobalIndexOwner {
    pub fn new(source_shard: u16, locator: impl Into<Vec<u8>>) -> EngineResult<Self> {
        let locator = locator.into();
        if source_shard > 63 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "global-index owner shards must be in 0..=63",
            ));
        }
        if locator.is_empty() || locator.len() > MAX_GLOBAL_OWNER_LOCATOR_BYTES {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!(
                    "global-index owner locators must contain 1..={MAX_GLOBAL_OWNER_LOCATOR_BYTES} bytes"
                ),
            ));
        }
        Ok(Self {
            source_shard,
            locator: locator.into_boxed_slice(),
        })
    }

    pub const fn source_shard(&self) -> u16 {
        self.source_shard
    }

    pub fn locator(&self) -> &[u8] {
        &self.locator
    }
}

impl fmt::Debug for GlobalIndexOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GlobalIndexOwner")
            .field("source_shard", &self.source_shard)
            .field("locator", &"<redacted>")
            .finish()
    }
}

/// Monotonic position in one physical shard's global-index outbox.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GlobalIndexOutboxCursor(u64);

impl GlobalIndexOutboxCursor {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Logical non-unique index change captured beside its owning shard row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GlobalIndexOutboxEventKind {
    Insert,
    Update,
    Delete,
    Tombstone,
}

impl GlobalIndexOutboxEventKind {
    pub(crate) const fn code(self) -> i64 {
        match self {
            Self::Insert => 1,
            Self::Update => 2,
            Self::Delete => 3,
            Self::Tombstone => 4,
        }
    }

    pub(crate) fn from_code(code: i64) -> EngineResult<Self> {
        match code {
            1 => Ok(Self::Insert),
            2 => Ok(Self::Update),
            3 => Ok(Self::Delete),
            4 => Ok(Self::Tombstone),
            _ => Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                format!("global-index outbox has unsupported event kind {code}"),
            )),
        }
    }
}

/// One durable shard-local non-unique index event.
#[derive(Clone, PartialEq, Eq)]
pub struct GlobalIndexOutboxEvent {
    format_version: u32,
    cursor: GlobalIndexOutboxCursor,
    index_id: GlobalIndexId,
    operation_id: GlobalOperationId,
    kind: GlobalIndexOutboxEventKind,
    old_key: Option<CanonicalIndexKey>,
    new_key: Option<CanonicalIndexKey>,
    old_owner: Option<GlobalIndexOwner>,
    new_owner: Option<GlobalIndexOwner>,
}

impl GlobalIndexOutboxEvent {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_validated_parts(
        format_version: u32,
        cursor: u64,
        index_id: GlobalIndexId,
        operation_id: GlobalOperationId,
        kind: GlobalIndexOutboxEventKind,
        old_key: Option<CanonicalIndexKey>,
        new_key: Option<CanonicalIndexKey>,
        old_owner: Option<GlobalIndexOwner>,
        new_owner: Option<GlobalIndexOwner>,
    ) -> Self {
        Self {
            format_version,
            cursor: GlobalIndexOutboxCursor::new(cursor),
            index_id,
            operation_id,
            kind,
            old_key,
            new_key,
            old_owner,
            new_owner,
        }
    }

    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    pub const fn cursor(&self) -> GlobalIndexOutboxCursor {
        self.cursor
    }

    pub const fn index_id(&self) -> GlobalIndexId {
        self.index_id
    }

    pub const fn operation_id(&self) -> GlobalOperationId {
        self.operation_id
    }

    pub const fn kind(&self) -> GlobalIndexOutboxEventKind {
        self.kind
    }

    pub fn old_key(&self) -> Option<&CanonicalIndexKey> {
        self.old_key.as_ref()
    }

    pub fn new_key(&self) -> Option<&CanonicalIndexKey> {
        self.new_key.as_ref()
    }

    pub fn old_owner(&self) -> Option<&GlobalIndexOwner> {
        self.old_owner.as_ref()
    }

    pub fn new_owner(&self) -> Option<&GlobalIndexOwner> {
        self.new_owner.as_ref()
    }
}

impl fmt::Debug for GlobalIndexOutboxEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GlobalIndexOutboxEvent")
            .field("format_version", &self.format_version)
            .field("cursor", &self.cursor)
            .field("index_id", &self.index_id)
            .field("operation_id", &self.operation_id)
            .field("kind", &self.kind)
            .field("old_key", &self.old_key)
            .field("new_key", &self.new_key)
            .field("old_owner", &self.old_owner)
            .field("new_owner", &self.new_owner)
            .finish()
    }
}

/// Bounded replay result from one index on one shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexOutboxBatch {
    shard: u16,
    index_id: GlobalIndexId,
    after: GlobalIndexOutboxCursor,
    high_water: GlobalIndexOutboxCursor,
    events: Box<[GlobalIndexOutboxEvent]>,
}

impl GlobalIndexOutboxBatch {
    pub(crate) fn new(
        shard: u16,
        index_id: GlobalIndexId,
        after: u64,
        high_water: u64,
        events: Vec<GlobalIndexOutboxEvent>,
    ) -> Self {
        Self {
            shard,
            index_id,
            after: GlobalIndexOutboxCursor::new(after),
            high_water: GlobalIndexOutboxCursor::new(high_water),
            events: events.into_boxed_slice(),
        }
    }

    pub const fn shard(&self) -> u16 {
        self.shard
    }

    pub const fn index_id(&self) -> GlobalIndexId {
        self.index_id
    }

    pub const fn after(&self) -> GlobalIndexOutboxCursor {
        self.after
    }

    pub const fn high_water(&self) -> GlobalIndexOutboxCursor {
        self.high_water
    }

    pub fn events(&self) -> &[GlobalIndexOutboxEvent] {
        &self.events
    }
}

/// Storage and worst-consumer lag for one physical shard outbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexOutboxShardStatus {
    shard: u16,
    high_water: GlobalIndexOutboxCursor,
    pruned_through: GlobalIndexOutboxCursor,
    retained_events: u64,
    retained_bytes: u64,
    active_consumers: u64,
    minimum_durable_cursor: GlobalIndexOutboxCursor,
}

impl GlobalIndexOutboxShardStatus {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        shard: u16,
        high_water: u64,
        pruned_through: u64,
        retained_events: u64,
        retained_bytes: u64,
        active_consumers: u64,
        minimum_durable_cursor: u64,
    ) -> Self {
        Self {
            shard,
            high_water: GlobalIndexOutboxCursor::new(high_water),
            pruned_through: GlobalIndexOutboxCursor::new(pruned_through),
            retained_events,
            retained_bytes,
            active_consumers,
            minimum_durable_cursor: GlobalIndexOutboxCursor::new(minimum_durable_cursor),
        }
    }

    pub const fn shard(&self) -> u16 {
        self.shard
    }

    pub const fn high_water(&self) -> GlobalIndexOutboxCursor {
        self.high_water
    }

    pub const fn pruned_through(&self) -> GlobalIndexOutboxCursor {
        self.pruned_through
    }

    pub const fn retained_events(&self) -> u64 {
        self.retained_events
    }

    pub const fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }

    pub const fn active_consumers(&self) -> u64 {
        self.active_consumers
    }

    pub const fn minimum_durable_cursor(&self) -> GlobalIndexOutboxCursor {
        self.minimum_durable_cursor
    }

    pub const fn lag(&self) -> u64 {
        self.high_water
            .get()
            .saturating_sub(self.minimum_durable_cursor.get())
    }

    pub const fn is_backpressured(&self) -> bool {
        self.retained_events >= MAX_GLOBAL_INDEX_OUTBOX_EVENTS_PER_SHARD
            || self.retained_bytes >= MAX_GLOBAL_INDEX_OUTBOX_BYTES_PER_SHARD
    }
}

/// Result of one bounded, consumer-safe shard-local prune.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalIndexOutboxPruneReport {
    shard: u16,
    deleted_events: u64,
    deleted_bytes: u64,
    pruned_through: GlobalIndexOutboxCursor,
}

impl GlobalIndexOutboxPruneReport {
    pub(crate) const fn new(
        shard: u16,
        deleted_events: u64,
        deleted_bytes: u64,
        pruned_through: u64,
    ) -> Self {
        Self {
            shard,
            deleted_events,
            deleted_bytes,
            pruned_through: GlobalIndexOutboxCursor::new(pruned_through),
        }
    }

    pub const fn shard(&self) -> u16 {
        self.shard
    }

    pub const fn deleted_events(&self) -> u64 {
        self.deleted_events
    }

    pub const fn deleted_bytes(&self) -> u64 {
        self.deleted_bytes
    }

    pub const fn pruned_through(&self) -> GlobalIndexOutboxCursor {
        self.pruned_through
    }
}

/// Configuration for a managed non-unique global-index consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalIndexAsyncOptions {
    batch_events: usize,
    lease_ms: u64,
    poll_ms: u64,
}

impl GlobalIndexAsyncOptions {
    pub fn new(batch_events: usize, lease_ms: u64, poll_ms: u64) -> EngineResult<Self> {
        if batch_events == 0 || batch_events > MAX_GLOBAL_INDEX_OUTBOX_BATCH_EVENTS {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!(
                    "global-index async batches must contain 1..={MAX_GLOBAL_INDEX_OUTBOX_BATCH_EVENTS} events"
                ),
            ));
        }
        if !(100..=60_000).contains(&lease_ms) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "global-index async leases must be between 100 and 60000 milliseconds",
            ));
        }
        if !(1..=60_000).contains(&poll_ms) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "global-index async polling must be between 1 and 60000 milliseconds",
            ));
        }
        Ok(Self {
            batch_events,
            lease_ms,
            poll_ms,
        })
    }

    pub const fn batch_events(self) -> usize {
        self.batch_events
    }

    pub const fn lease_ms(self) -> u64 {
        self.lease_ms
    }

    pub const fn poll_ms(self) -> u64 {
        self.poll_ms
    }
}

impl Default for GlobalIndexAsyncOptions {
    fn default() -> Self {
        Self {
            batch_events: DEFAULT_GLOBAL_INDEX_ASYNC_BATCH_EVENTS,
            lease_ms: DEFAULT_GLOBAL_INDEX_ASYNC_LEASE_MS,
            poll_ms: DEFAULT_GLOBAL_INDEX_ASYNC_POLL_MS,
        }
    }
}

/// Outcome of one shard in a bounded asynchronous indexing pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GlobalIndexAsyncShardOutcome {
    Applied,
    Current,
    LeasedElsewhere,
    Paused,
    Poisoned,
    RebuildRequired,
}

/// Work completed for one source shard in a bounded indexing pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexAsyncShardReport {
    shard: u16,
    outcome: GlobalIndexAsyncShardOutcome,
    from: GlobalIndexOutboxCursor,
    through: GlobalIndexOutboxCursor,
    applied_events: u64,
}

impl GlobalIndexAsyncShardReport {
    pub(crate) const fn new(
        shard: u16,
        outcome: GlobalIndexAsyncShardOutcome,
        from: u64,
        through: u64,
        applied_events: u64,
    ) -> Self {
        Self {
            shard,
            outcome,
            from: GlobalIndexOutboxCursor::new(from),
            through: GlobalIndexOutboxCursor::new(through),
            applied_events,
        }
    }

    pub const fn shard(&self) -> u16 {
        self.shard
    }

    pub const fn outcome(&self) -> GlobalIndexAsyncShardOutcome {
        self.outcome
    }

    pub const fn from(&self) -> GlobalIndexOutboxCursor {
        self.from
    }

    pub const fn through(&self) -> GlobalIndexOutboxCursor {
        self.through
    }

    pub const fn applied_events(&self) -> u64 {
        self.applied_events
    }
}

/// Result of one bounded pass over every source shard for an index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexAsyncProcessReport {
    index_id: GlobalIndexId,
    shards: Box<[GlobalIndexAsyncShardReport]>,
}

impl GlobalIndexAsyncProcessReport {
    pub(crate) fn new(index_id: GlobalIndexId, shards: Vec<GlobalIndexAsyncShardReport>) -> Self {
        Self {
            index_id,
            shards: shards.into_boxed_slice(),
        }
    }

    pub const fn index_id(&self) -> GlobalIndexId {
        self.index_id
    }

    pub fn shards(&self) -> &[GlobalIndexAsyncShardReport] {
        &self.shards
    }

    pub fn applied_events(&self) -> u64 {
        self.shards.iter().map(|shard| shard.applied_events).sum()
    }
}

/// Durable freshness and health for one index/source-shard pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexAsyncShardStatus {
    shard: u16,
    applied: GlobalIndexOutboxCursor,
    high_water: GlobalIndexOutboxCursor,
    applied_events: u64,
    failure_count: u64,
    last_batch_events: u64,
    last_batch_micros: u64,
    poison_cursor: Option<GlobalIndexOutboxCursor>,
    lease_fence: u64,
    leased: bool,
}

impl GlobalIndexAsyncShardStatus {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        shard: u16,
        applied: u64,
        high_water: u64,
        applied_events: u64,
        failure_count: u64,
        last_batch_events: u64,
        last_batch_micros: u64,
        poison_cursor: Option<u64>,
        lease_fence: u64,
        leased: bool,
    ) -> Self {
        Self {
            shard,
            applied: GlobalIndexOutboxCursor::new(applied),
            high_water: GlobalIndexOutboxCursor::new(high_water),
            applied_events,
            failure_count,
            last_batch_events,
            last_batch_micros,
            poison_cursor: poison_cursor.map(GlobalIndexOutboxCursor::new),
            lease_fence,
            leased,
        }
    }

    pub const fn shard(&self) -> u16 {
        self.shard
    }
    pub const fn applied(&self) -> GlobalIndexOutboxCursor {
        self.applied
    }
    pub const fn high_water(&self) -> GlobalIndexOutboxCursor {
        self.high_water
    }
    pub const fn lag(&self) -> u64 {
        self.high_water.get().saturating_sub(self.applied.get())
    }
    pub const fn applied_events(&self) -> u64 {
        self.applied_events
    }
    pub const fn failure_count(&self) -> u64 {
        self.failure_count
    }
    pub const fn last_batch_events(&self) -> u64 {
        self.last_batch_events
    }
    pub const fn last_batch_micros(&self) -> u64 {
        self.last_batch_micros
    }
    pub fn last_batch_events_per_second(&self) -> u64 {
        if self.last_batch_micros == 0 {
            return 0;
        }
        let rate =
            u128::from(self.last_batch_events) * 1_000_000 / u128::from(self.last_batch_micros);
        u64::try_from(rate).unwrap_or(u64::MAX)
    }
    pub const fn poison_cursor(&self) -> Option<GlobalIndexOutboxCursor> {
        self.poison_cursor
    }
    pub const fn lease_fence(&self) -> u64 {
        self.lease_fence
    }
    pub const fn is_leased(&self) -> bool {
        self.leased
    }
    pub const fn is_fresh(&self) -> bool {
        self.poison_cursor.is_none() && self.applied.get() >= self.high_water.get()
    }
}

/// Redaction-safe operator status for one asynchronous global index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexAsyncStatus {
    index_id: GlobalIndexId,
    paused: bool,
    rebuild_required: bool,
    shards: Box<[GlobalIndexAsyncShardStatus]>,
}

impl GlobalIndexAsyncStatus {
    pub(crate) fn new(
        index_id: GlobalIndexId,
        paused: bool,
        rebuild_required: bool,
        shards: Vec<GlobalIndexAsyncShardStatus>,
    ) -> Self {
        Self {
            index_id,
            paused,
            rebuild_required,
            shards: shards.into_boxed_slice(),
        }
    }

    pub const fn index_id(&self) -> GlobalIndexId {
        self.index_id
    }
    pub const fn is_paused(&self) -> bool {
        self.paused
    }
    pub const fn rebuild_required(&self) -> bool {
        self.rebuild_required
    }
    pub fn shards(&self) -> &[GlobalIndexAsyncShardStatus] {
        &self.shards
    }
    pub fn lag(&self) -> u64 {
        self.shards
            .iter()
            .map(GlobalIndexAsyncShardStatus::lag)
            .sum()
    }
    pub fn is_fresh(&self) -> bool {
        !self.paused
            && !self.rebuild_required
            && self
                .shards
                .iter()
                .all(GlobalIndexAsyncShardStatus::is_fresh)
    }
}

/// Storage result consumed by protocol-neutral global-index read planning.
/// Non-unique results remain incomplete until asynchronous freshness
/// watermarks prove which source shards may be excluded.
#[derive(Debug)]
pub(crate) struct GlobalIndexReadResolution {
    owners: Vec<GlobalIndexOwner>,
    candidate_count: usize,
    verified_count: usize,
    rejected_count: usize,
    stale_count: usize,
    repairs_queued: usize,
    repairs_applied: usize,
    repairs_deferred: usize,
    complete: bool,
    candidate_limit_exceeded: bool,
    uncertain_shards: Vec<u16>,
}

impl GlobalIndexReadResolution {
    pub(crate) fn authoritative(owners: Vec<GlobalIndexOwner>) -> Self {
        let candidate_count = owners.len();
        Self {
            owners,
            candidate_count,
            verified_count: 0,
            rejected_count: 0,
            stale_count: 0,
            repairs_queued: 0,
            repairs_applied: 0,
            repairs_deferred: 0,
            complete: true,
            candidate_limit_exceeded: false,
            uncertain_shards: Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn candidates(
        owners: Vec<GlobalIndexOwner>,
        candidate_count: usize,
        verified_count: usize,
        rejected_count: usize,
        stale_count: usize,
        repairs_queued: usize,
        repairs_applied: usize,
        repairs_deferred: usize,
        uncertain_shards: Vec<u16>,
    ) -> Self {
        Self {
            owners,
            candidate_count,
            verified_count,
            rejected_count,
            stale_count,
            repairs_queued,
            repairs_applied,
            repairs_deferred,
            complete: false,
            candidate_limit_exceeded: false,
            uncertain_shards,
        }
    }

    pub(crate) const fn candidate_limit_exceeded(candidate_count: usize) -> Self {
        Self {
            owners: Vec::new(),
            candidate_count,
            verified_count: 0,
            rejected_count: 0,
            stale_count: 0,
            repairs_queued: 0,
            repairs_applied: 0,
            repairs_deferred: 0,
            complete: false,
            candidate_limit_exceeded: true,
            uncertain_shards: Vec::new(),
        }
    }

    pub(crate) fn owners(&self) -> &[GlobalIndexOwner] {
        &self.owners
    }

    pub(crate) const fn candidate_count(&self) -> usize {
        self.candidate_count
    }

    pub(crate) const fn verified_count(&self) -> usize {
        self.verified_count
    }

    pub(crate) const fn rejected_count(&self) -> usize {
        self.rejected_count
    }

    pub(crate) const fn stale_count(&self) -> usize {
        self.stale_count
    }

    pub(crate) const fn repairs_queued(&self) -> usize {
        self.repairs_queued
    }

    pub(crate) const fn repairs_applied(&self) -> usize {
        self.repairs_applied
    }

    pub(crate) const fn repairs_deferred(&self) -> usize {
        self.repairs_deferred
    }

    pub(crate) const fn is_complete(&self) -> bool {
        self.complete
    }

    pub(crate) const fn is_candidate_limit_exceeded(&self) -> bool {
        self.candidate_limit_exceeded
    }

    pub(crate) fn uncertain_shards(&self) -> &[u16] {
        &self.uncertain_shards
    }
}

/// One atomic change to an authoritative global unique key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalUniqueMutation {
    index_id: GlobalIndexId,
    new: Option<(CanonicalIndexKey, GlobalIndexOwner)>,
    previous: Option<(CanonicalIndexKey, GlobalIndexOwner)>,
}

impl GlobalUniqueMutation {
    pub fn claim(index_id: GlobalIndexId, key: CanonicalIndexKey, owner: GlobalIndexOwner) -> Self {
        Self {
            index_id,
            new: Some((key, owner)),
            previous: None,
        }
    }

    pub fn release(
        index_id: GlobalIndexId,
        key: CanonicalIndexKey,
        owner: GlobalIndexOwner,
    ) -> Self {
        Self {
            index_id,
            new: None,
            previous: Some((key, owner)),
        }
    }

    pub fn replace(
        index_id: GlobalIndexId,
        previous_key: CanonicalIndexKey,
        previous_owner: GlobalIndexOwner,
        new_key: CanonicalIndexKey,
        new_owner: GlobalIndexOwner,
    ) -> Self {
        Self {
            index_id,
            new: Some((new_key, new_owner)),
            previous: Some((previous_key, previous_owner)),
        }
    }

    pub const fn index_id(&self) -> GlobalIndexId {
        self.index_id
    }

    pub fn new_entry(&self) -> Option<(&CanonicalIndexKey, &GlobalIndexOwner)> {
        self.new.as_ref().map(|(key, owner)| (key, owner))
    }

    pub fn previous_entry(&self) -> Option<(&CanonicalIndexKey, &GlobalIndexOwner)> {
        self.previous.as_ref().map(|(key, owner)| (key, owner))
    }
}

/// Durable lifecycle shared by uniqueness reservations and value leases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GlobalOperationState {
    Active,
    Finalized,
    RolledBack,
}

impl GlobalOperationState {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Finalized => "finalized",
            Self::RolledBack => "rolled_back",
        }
    }

    pub(crate) fn from_validated(value: i64) -> Self {
        match value {
            1 => Self::Active,
            2 => Self::Finalized,
            3 => Self::RolledBack,
            _ => unreachable!("validated global operation state"),
        }
    }
}

/// Durable result of reserving, finalizing, or rolling back one unique mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalUniqueReservation {
    operation_id: GlobalOperationId,
    index_id: GlobalIndexId,
    state: GlobalOperationState,
}

impl GlobalUniqueReservation {
    pub(crate) const fn from_validated(
        operation_id: GlobalOperationId,
        index_id: GlobalIndexId,
        state: GlobalOperationState,
    ) -> Self {
        Self {
            operation_id,
            index_id,
            state,
        }
    }

    pub const fn operation_id(&self) -> GlobalOperationId {
        self.operation_id
    }

    pub const fn index_id(&self) -> GlobalIndexId {
        self.index_id
    }

    pub const fn state(&self) -> GlobalOperationState {
        self.state
    }
}

/// One collision-free, irrevocable range of positive global integer values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalValueLease {
    operation_id: GlobalOperationId,
    index_id: GlobalIndexId,
    state: GlobalOperationState,
    first: u64,
    last: u64,
    fence_token: u64,
}

impl GlobalValueLease {
    pub(crate) const fn from_validated(
        operation_id: GlobalOperationId,
        index_id: GlobalIndexId,
        state: GlobalOperationState,
        first: u64,
        last: u64,
        fence_token: u64,
    ) -> Self {
        Self {
            operation_id,
            index_id,
            state,
            first,
            last,
            fence_token,
        }
    }

    pub const fn operation_id(&self) -> GlobalOperationId {
        self.operation_id
    }

    pub const fn index_id(&self) -> GlobalIndexId {
        self.index_id
    }

    pub const fn state(&self) -> GlobalOperationState {
        self.state
    }

    pub const fn first(&self) -> u64 {
        self.first
    }

    pub const fn last(&self) -> u64 {
        self.last
    }

    pub const fn count(&self) -> u64 {
        self.last - self.first + 1
    }

    pub const fn fence_token(&self) -> u64 {
        self.fence_token
    }
}

/// Durable lifecycle of a global-index definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GlobalIndexLifecycle {
    /// Metadata exists, but index data must not be used by queries.
    Creating,
    /// Index data is complete and eligible for its documented use.
    Ready,
    /// Validation failed or compatibility was lost; the index is fenced.
    Invalid,
    /// Replacement index data is being constructed and is not yet published.
    Rebuilding,
    /// The definition and any physical artifacts are being removed.
    Dropping,
}

impl GlobalIndexLifecycle {
    /// Return whether one durable lifecycle transition is legal.
    pub fn can_transition_to(self, target: Self) -> bool {
        if self == target {
            return true;
        }
        matches!(
            (self, target),
            (Self::Creating, Self::Ready | Self::Invalid | Self::Dropping)
                | (
                    Self::Ready,
                    Self::Invalid | Self::Rebuilding | Self::Dropping
                )
                | (Self::Invalid, Self::Rebuilding | Self::Dropping)
                | (
                    Self::Rebuilding,
                    Self::Ready | Self::Invalid | Self::Dropping
                )
        )
    }
}

/// Durable physical-layout choice for global-index data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GlobalIndexStorageTopology {
    /// No physical layout has been selected yet.
    Unassigned,
    /// One shared global-index SQLite file.
    SharedSqliteV1,
    /// Canonical keys are hash-partitioned across multiple SQLite files.
    HashPartitionedSqliteV1 { partitions: u16 },
}

impl GlobalIndexStorageTopology {
    /// Return the selected initial topology.
    pub const fn selected_v1() -> Self {
        Self::SharedSqliteV1
    }

    /// Construct a validated version-1 hash-partitioned topology.
    pub fn hash_partitioned_sqlite_v1(partitions: u16) -> EngineResult<Self> {
        if !(2..=256).contains(&partitions) || !partitions.is_power_of_two() {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "global-index partition count must be a power of two between 2 and 256",
            ));
        }
        Ok(Self::HashPartitionedSqliteV1 { partitions })
    }

    pub(crate) fn from_validated_parts(kind: i64, version: i64, partitions: i64) -> Self {
        match (kind, version, partitions) {
            (0, 0, 0) => Self::Unassigned,
            (1, 1, 1) => Self::SharedSqliteV1,
            (2, 1, partitions) => Self::HashPartitionedSqliteV1 {
                partitions: partitions as u16,
            },
            _ => unreachable!("validated global-index topology"),
        }
    }

    pub(crate) const fn persisted_parts(self) -> (i64, i64, i64) {
        match self {
            Self::Unassigned => (0, 0, 0),
            Self::SharedSqliteV1 => (1, 1, 1),
            Self::HashPartitionedSqliteV1 { partitions } => (2, 1, partitions as i64),
        }
    }

    /// Return the number of physical index databases in this topology.
    pub const fn partition_count(self) -> u16 {
        match self {
            Self::Unassigned => 0,
            Self::SharedSqliteV1 => 1,
            Self::HashPartitionedSqliteV1 { partitions } => partitions,
        }
    }

    /// Route a canonical key to its one authoritative index partition.
    ///
    /// Version 1 hashes the stable index ID and exact canonical key bytes with
    /// a domain-separated BLAKE3 digest, then masks the low 64-bit word. The
    /// partition count is constrained to a power of two, so this mapping is
    /// deterministic on every supported architecture.
    pub fn partition_for_key(
        self,
        index_id: GlobalIndexId,
        key: &CanonicalIndexKey,
    ) -> EngineResult<u16> {
        match self {
            Self::Unassigned => Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "global-index storage topology is not assigned",
            )),
            Self::SharedSqliteV1 => Ok(0),
            Self::HashPartitionedSqliteV1 { partitions } => {
                let mut hasher = blake3::Hasher::new();
                hasher.update(PARTITION_ROUTING_DOMAIN_V1);
                hasher.update(&index_id.get().to_le_bytes());
                hasher.update(key.as_bytes());
                let word = u64::from_le_bytes(
                    hasher.finalize().as_bytes()[..size_of::<u64>()]
                        .try_into()
                        .expect("BLAKE3 digest contains one routing word"),
                );
                Ok((word & u64::from(partitions - 1)) as u16)
            }
        }
    }
}

/// Declared logical type of one encoded global-index key component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GlobalIndexKeyType {
    Boolean,
    Int64,
    UInt64,
    Float64,
    Date,
    Timestamp,
    Text,
    Binary,
}

/// Source expression for one global-index key component.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GlobalIndexKeySource {
    /// A canonical catalog column name.
    Column(String),
    /// Exact canonical SQLite expression text retained for later evaluation.
    Expression(String),
}

impl GlobalIndexKeySource {
    /// Construct a validated column source.
    pub fn column(name: impl Into<String>) -> EngineResult<Self> {
        let name = name.into();
        ensure_identifier(&name, "global-index column")?;
        Ok(Self::Column(name))
    }

    /// Construct a bounded, NUL-free expression source.
    pub fn expression(sql: impl Into<String>) -> EngineResult<Self> {
        let sql = sql.into();
        ensure_sql_fragment(&sql, "global-index expression")?;
        Ok(Self::Expression(sql))
    }

    pub(crate) fn from_validated(kind: i64, source: String) -> Self {
        match kind {
            1 => Self::Column(source),
            2 => Self::Expression(source),
            _ => unreachable!("validated global-index key source"),
        }
    }

    pub(crate) const fn kind_code(&self) -> i64 {
        match self {
            Self::Column(_) => 1,
            Self::Expression(_) => 2,
        }
    }

    /// Return the canonical column name or exact expression text.
    pub fn source(&self) -> &str {
        match self {
            Self::Column(source) | Self::Expression(source) => source,
        }
    }
}

/// Frozen definition of one component in a compound global-index key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GlobalIndexKeyPart {
    source: GlobalIndexKeySource,
    key_type: GlobalIndexKeyType,
    order: IndexKeyOrder,
    null_order: IndexNullOrder,
    collation: IndexKeyCollation,
}

impl GlobalIndexKeyPart {
    /// Construct an ascending, NULLS FIRST, BINARY key component.
    pub const fn new(source: GlobalIndexKeySource, key_type: GlobalIndexKeyType) -> Self {
        Self {
            source,
            key_type,
            order: IndexKeyOrder::Ascending,
            null_order: IndexNullOrder::First,
            collation: IndexKeyCollation::Binary,
        }
    }

    pub const fn with_order(mut self, order: IndexKeyOrder) -> Self {
        self.order = order;
        self
    }

    pub const fn with_null_order(mut self, null_order: IndexNullOrder) -> Self {
        self.null_order = null_order;
        self
    }

    pub const fn with_collation(mut self, collation: IndexKeyCollation) -> Self {
        self.collation = collation;
        self
    }

    pub const fn source(&self) -> &GlobalIndexKeySource {
        &self.source
    }

    pub const fn key_type(&self) -> GlobalIndexKeyType {
        self.key_type
    }

    pub const fn order(&self) -> IndexKeyOrder {
        self.order
    }

    pub const fn null_order(&self) -> IndexNullOrder {
        self.null_order
    }

    pub const fn collation(&self) -> IndexKeyCollation {
        self.collation
    }

    pub(crate) const fn from_validated(
        source: GlobalIndexKeySource,
        key_type: GlobalIndexKeyType,
        order: IndexKeyOrder,
        null_order: IndexNullOrder,
        collation: IndexKeyCollation,
    ) -> Self {
        Self {
            source,
            key_type,
            order,
            null_order,
            collation,
        }
    }
}

/// Validated request to add one durable global-index definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexDeclaration {
    table_id: TableId,
    name: String,
    key_parts: Box<[GlobalIndexKeyPart]>,
    unique: bool,
    null_semantics: UniqueNullSemantics,
    predicate: Option<String>,
    topology: GlobalIndexStorageTopology,
}

impl GlobalIndexDeclaration {
    /// Define a non-unique global index in the unassigned topology.
    pub fn new(
        table_id: TableId,
        name: impl Into<String>,
        key_parts: Vec<GlobalIndexKeyPart>,
    ) -> EngineResult<Self> {
        let name = name.into();
        ensure_identifier(&name, "global-index name")?;
        ensure_key_parts(&key_parts)?;
        Ok(Self {
            table_id,
            name,
            key_parts: key_parts.into_boxed_slice(),
            unique: false,
            null_semantics: UniqueNullSemantics::Distinct,
            predicate: None,
            topology: GlobalIndexStorageTopology::Unassigned,
        })
    }

    /// Mark this definition unique and freeze its NULL semantics.
    pub const fn unique(mut self, null_semantics: UniqueNullSemantics) -> Self {
        self.unique = true;
        self.null_semantics = null_semantics;
        self
    }

    /// Attach an exact bounded predicate used for a partial index.
    pub fn with_predicate(mut self, predicate: impl Into<String>) -> EngineResult<Self> {
        let predicate = predicate.into();
        ensure_sql_fragment(&predicate, "global-index predicate")?;
        self.predicate = Some(predicate);
        Ok(self)
    }

    /// Select the durable storage topology. `Ready` publication remains a
    /// separate lifecycle transition.
    pub const fn with_topology(mut self, topology: GlobalIndexStorageTopology) -> Self {
        self.topology = topology;
        self
    }

    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key_parts(&self) -> &[GlobalIndexKeyPart] {
        &self.key_parts
    }

    pub const fn is_unique(&self) -> bool {
        self.unique
    }

    pub const fn null_semantics(&self) -> UniqueNullSemantics {
        self.null_semantics
    }

    pub fn predicate(&self) -> Option<&str> {
        self.predicate.as_deref()
    }

    pub const fn topology(&self) -> GlobalIndexStorageTopology {
        self.topology
    }
}

/// Fully validated read-only global-index metadata loaded from the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexMetadata {
    id: GlobalIndexId,
    table_id: TableId,
    name: String,
    key_parts: Box<[GlobalIndexKeyPart]>,
    unique: bool,
    null_semantics: UniqueNullSemantics,
    predicate: Option<String>,
    lifecycle: GlobalIndexLifecycle,
    key_encoding_version: u32,
    schema_generation: u64,
    topology: GlobalIndexStorageTopology,
}

/// Durable outcome of an offline global-index build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalIndexBuildReport {
    index_id: GlobalIndexId,
    shard_count: u16,
    resumed_from_shard: u16,
    indexed_rows: u64,
}

impl GlobalIndexBuildReport {
    pub(crate) const fn from_validated(
        index_id: GlobalIndexId,
        shard_count: u16,
        resumed_from_shard: u16,
        indexed_rows: u64,
    ) -> Self {
        Self {
            index_id,
            shard_count,
            resumed_from_shard,
            indexed_rows,
        }
    }

    /// Return the durable index identity that was built or revalidated.
    pub const fn index_id(self) -> GlobalIndexId {
        self.index_id
    }

    /// Return the number of source shards represented by the completed index.
    pub const fn shard_count(self) -> u16 {
        self.shard_count
    }

    /// Return the first shard that did not already have a reusable checkpoint.
    ///
    /// This equals [`Self::shard_count`] when every shard checkpoint could be
    /// revalidated and only final publication remained.
    pub const fn resumed_from_shard(self) -> u16 {
        self.resumed_from_shard
    }

    /// Return the exact number of qualifying physical source rows indexed.
    pub const fn indexed_rows(self) -> u64 {
        self.indexed_rows
    }
}

/// Amount of source data covered by one global-index validation pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GlobalIndexValidationMode {
    /// Compare every qualifying source row with physical index state.
    Full,
    /// Compare a deterministic, evenly distributed sample on each shard.
    Sampled,
}

impl GlobalIndexValidationMode {
    /// Return the stable machine-readable mode name.
    pub const fn code(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Sampled => "sampled",
        }
    }
}

/// Bounded options for a global-index validation pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalIndexValidationOptions {
    mode: GlobalIndexValidationMode,
    samples_per_shard: u16,
    max_reported_issues: u16,
}

impl GlobalIndexValidationOptions {
    /// Validate every qualifying source row and physical entry.
    pub const fn full() -> Self {
        Self {
            mode: GlobalIndexValidationMode::Full,
            samples_per_shard: 0,
            max_reported_issues: DEFAULT_MAX_REPORTED_VALIDATION_ISSUES,
        }
    }

    /// Validate an evenly distributed sample from each source shard.
    pub fn sampled(samples_per_shard: u16) -> EngineResult<Self> {
        if samples_per_shard == 0 || samples_per_shard > MAX_VALIDATION_SAMPLES_PER_SHARD {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!(
                    "sampled global-index validation requires 1..={MAX_VALIDATION_SAMPLES_PER_SHARD} samples per shard"
                ),
            ));
        }
        Ok(Self {
            mode: GlobalIndexValidationMode::Sampled,
            samples_per_shard,
            max_reported_issues: DEFAULT_MAX_REPORTED_VALIDATION_ISSUES,
        })
    }

    /// Bound the retained issue details while preserving the exact total count.
    pub fn with_max_reported_issues(mut self, maximum: u16) -> EngineResult<Self> {
        if maximum == 0 || maximum > MAX_REPORTED_VALIDATION_ISSUES {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!(
                    "global-index validation requires 1..={MAX_REPORTED_VALIDATION_ISSUES} reported issues"
                ),
            ));
        }
        self.max_reported_issues = maximum;
        Ok(self)
    }

    pub const fn mode(self) -> GlobalIndexValidationMode {
        self.mode
    }

    pub const fn samples_per_shard(self) -> u16 {
        self.samples_per_shard
    }

    pub const fn max_reported_issues(self) -> u16 {
        self.max_reported_issues
    }
}

impl Default for GlobalIndexValidationOptions {
    fn default() -> Self {
        Self::full()
    }
}

/// Typed condition detected while validating one global index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GlobalIndexValidationIssueKind {
    MissingPhysicalStorage,
    MissingBuildRecord,
    IncompleteBuild,
    DefinitionMismatch,
    MissingCheckpoint,
    UnexpectedCheckpoint,
    CheckpointMismatch,
    MissingEntry,
    DanglingEntry,
    StaleEntry,
    DuplicateAuthoritativeKey,
    BadShardTarget,
    IncompatibleKeyEncoding,
    IncompatibleLocatorEncoding,
    MissingUniqueReservation,
    DanglingUniqueReservation,
    MismatchedUniqueReservation,
    ActiveUniqueReservation,
}

impl GlobalIndexValidationIssueKind {
    /// Return the stable machine-readable issue code.
    pub const fn code(self) -> &'static str {
        match self {
            Self::MissingPhysicalStorage => "missing_physical_storage",
            Self::MissingBuildRecord => "missing_build_record",
            Self::IncompleteBuild => "incomplete_build",
            Self::DefinitionMismatch => "definition_mismatch",
            Self::MissingCheckpoint => "missing_checkpoint",
            Self::UnexpectedCheckpoint => "unexpected_checkpoint",
            Self::CheckpointMismatch => "checkpoint_mismatch",
            Self::MissingEntry => "missing_entry",
            Self::DanglingEntry => "dangling_entry",
            Self::StaleEntry => "stale_entry",
            Self::DuplicateAuthoritativeKey => "duplicate_authoritative_key",
            Self::BadShardTarget => "bad_shard_target",
            Self::IncompatibleKeyEncoding => "incompatible_key_encoding",
            Self::IncompatibleLocatorEncoding => "incompatible_locator_encoding",
            Self::MissingUniqueReservation => "missing_unique_reservation",
            Self::DanglingUniqueReservation => "dangling_unique_reservation",
            Self::MismatchedUniqueReservation => "mismatched_unique_reservation",
            Self::ActiveUniqueReservation => "active_unique_reservation",
        }
    }
}

/// One bounded, machine-readable global-index validation finding.
///
/// Key and row identities are eight-byte BLAKE3 prefixes. They identify repeat
/// findings without exposing application keys or physical primary-key values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexValidationIssue {
    kind: GlobalIndexValidationIssueKind,
    source_shard: Option<u16>,
    key_fingerprint: Option<[u8; 8]>,
    row_fingerprint: Option<[u8; 8]>,
}

impl GlobalIndexValidationIssue {
    pub(crate) const fn from_validated(
        kind: GlobalIndexValidationIssueKind,
        source_shard: Option<u16>,
        key_fingerprint: Option<[u8; 8]>,
        row_fingerprint: Option<[u8; 8]>,
    ) -> Self {
        Self {
            kind,
            source_shard,
            key_fingerprint,
            row_fingerprint,
        }
    }

    pub const fn kind(&self) -> GlobalIndexValidationIssueKind {
        self.kind
    }

    pub const fn source_shard(&self) -> Option<u16> {
        self.source_shard
    }

    pub const fn key_fingerprint(&self) -> Option<[u8; 8]> {
        self.key_fingerprint
    }

    pub const fn row_fingerprint(&self) -> Option<[u8; 8]> {
        self.row_fingerprint
    }
}

/// Bounded result of one offline global-index validation pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexValidationReport {
    index_id: GlobalIndexId,
    mode: GlobalIndexValidationMode,
    lifecycle_before: GlobalIndexLifecycle,
    lifecycle_after: GlobalIndexLifecycle,
    source_rows_examined: u64,
    physical_entries_examined: u64,
    total_issues: u64,
    issues: Box<[GlobalIndexValidationIssue]>,
}

impl GlobalIndexValidationReport {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_validated(
        index_id: GlobalIndexId,
        mode: GlobalIndexValidationMode,
        lifecycle_before: GlobalIndexLifecycle,
        lifecycle_after: GlobalIndexLifecycle,
        source_rows_examined: u64,
        physical_entries_examined: u64,
        total_issues: u64,
        issues: Vec<GlobalIndexValidationIssue>,
    ) -> Self {
        Self {
            index_id,
            mode,
            lifecycle_before,
            lifecycle_after,
            source_rows_examined,
            physical_entries_examined,
            total_issues,
            issues: issues.into_boxed_slice(),
        }
    }

    pub const fn index_id(&self) -> GlobalIndexId {
        self.index_id
    }

    pub const fn mode(&self) -> GlobalIndexValidationMode {
        self.mode
    }

    pub const fn lifecycle_before(&self) -> GlobalIndexLifecycle {
        self.lifecycle_before
    }

    pub const fn lifecycle_after(&self) -> GlobalIndexLifecycle {
        self.lifecycle_after
    }

    pub const fn source_rows_examined(&self) -> u64 {
        self.source_rows_examined
    }

    pub const fn physical_entries_examined(&self) -> u64 {
        self.physical_entries_examined
    }

    pub const fn total_issues(&self) -> u64 {
        self.total_issues
    }

    pub fn issues(&self) -> &[GlobalIndexValidationIssue] {
        &self.issues
    }

    pub const fn is_valid(&self) -> bool {
        self.total_issues == 0
    }

    pub fn issues_truncated(&self) -> bool {
        self.total_issues > self.issues.len() as u64
    }
}

/// Durable outcome of a bounded non-unique global-index repair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexRepairReport {
    index_id: GlobalIndexId,
    repaired_shards: Box<[u16]>,
    indexed_rows: u64,
    validation: GlobalIndexValidationReport,
}

impl GlobalIndexRepairReport {
    pub(crate) fn from_validated(
        index_id: GlobalIndexId,
        repaired_shards: Vec<u16>,
        indexed_rows: u64,
        validation: GlobalIndexValidationReport,
    ) -> Self {
        Self {
            index_id,
            repaired_shards: repaired_shards.into_boxed_slice(),
            indexed_rows,
            validation,
        }
    }

    pub const fn index_id(&self) -> GlobalIndexId {
        self.index_id
    }

    pub fn repaired_shards(&self) -> &[u16] {
        &self.repaired_shards
    }

    pub const fn indexed_rows(&self) -> u64 {
        self.indexed_rows
    }

    pub const fn validation(&self) -> &GlobalIndexValidationReport {
        &self.validation
    }
}

impl GlobalIndexMetadata {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_validated(
        id: u64,
        table_id: u64,
        name: String,
        key_parts: Box<[GlobalIndexKeyPart]>,
        unique: bool,
        null_semantics: UniqueNullSemantics,
        predicate: Option<String>,
        lifecycle: GlobalIndexLifecycle,
        schema_generation: u64,
        topology: GlobalIndexStorageTopology,
    ) -> Self {
        Self {
            id: GlobalIndexId::from_validated(id),
            table_id: TableId::from_validated(table_id),
            name,
            key_parts,
            unique,
            null_semantics,
            predicate,
            lifecycle,
            key_encoding_version: INDEX_KEY_ENCODING_VERSION,
            schema_generation,
            topology,
        }
    }

    pub const fn id(&self) -> GlobalIndexId {
        self.id
    }

    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key_parts(&self) -> &[GlobalIndexKeyPart] {
        &self.key_parts
    }

    pub const fn is_unique(&self) -> bool {
        self.unique
    }

    pub const fn null_semantics(&self) -> UniqueNullSemantics {
        self.null_semantics
    }

    pub fn predicate(&self) -> Option<&str> {
        self.predicate.as_deref()
    }

    pub const fn lifecycle(&self) -> GlobalIndexLifecycle {
        self.lifecycle
    }

    pub const fn key_encoding_version(&self) -> u32 {
        self.key_encoding_version
    }

    pub const fn schema_generation(&self) -> u64 {
        self.schema_generation
    }

    pub const fn topology(&self) -> GlobalIndexStorageTopology {
        self.topology
    }
}

fn ensure_identifier(value: &str, description: &str) -> EngineResult<()> {
    if validate_catalog_identifier(value) {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            format!("{description} must use canonical catalog spelling"),
        ))
    }
}

fn ensure_sql_fragment(value: &str, description: &str) -> EngineResult<()> {
    if value.is_empty() || value.len() > MAX_GLOBAL_INDEX_SQL_BYTES || value.as_bytes().contains(&0)
    {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            format!(
                "{description} must contain 1 through {MAX_GLOBAL_INDEX_SQL_BYTES} UTF-8 bytes without NUL"
            ),
        ));
    }
    Ok(())
}

fn ensure_key_parts(parts: &[GlobalIndexKeyPart]) -> EngineResult<()> {
    if parts.is_empty() || parts.len() > MAX_GLOBAL_INDEX_PARTS {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            format!("a global index requires 1 through {MAX_GLOBAL_INDEX_PARTS} key components"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_id() -> TableId {
        TableId::new(7).unwrap()
    }

    fn part() -> GlobalIndexKeyPart {
        GlobalIndexKeyPart::new(
            GlobalIndexKeySource::column("email").unwrap(),
            GlobalIndexKeyType::Text,
        )
    }

    #[test]
    fn lifecycle_transitions_are_explicit_and_idempotent() {
        use GlobalIndexLifecycle as State;
        assert!(State::Creating.can_transition_to(State::Ready));
        assert!(State::Ready.can_transition_to(State::Rebuilding));
        assert!(State::Rebuilding.can_transition_to(State::Invalid));
        assert!(State::Invalid.can_transition_to(State::Dropping));
        assert!(State::Dropping.can_transition_to(State::Dropping));
        assert!(!State::Dropping.can_transition_to(State::Ready));
        assert!(!State::Invalid.can_transition_to(State::Ready));
    }

    #[test]
    fn declarations_validate_identifiers_sql_and_compound_limits() {
        let declaration = GlobalIndexDeclaration::new(table_id(), "users_email", vec![part()])
            .unwrap()
            .unique(UniqueNullSemantics::NotDistinct)
            .with_predicate("active = 1")
            .unwrap()
            .with_topology(GlobalIndexStorageTopology::SharedSqliteV1);
        assert!(declaration.is_unique());
        assert_eq!(declaration.predicate(), Some("active = 1"));

        assert!(GlobalIndexDeclaration::new(table_id(), "Bad Name", vec![part()]).is_err());
        assert!(GlobalIndexDeclaration::new(table_id(), "empty", vec![]).is_err());
        assert!(GlobalIndexKeySource::expression("\0").is_err());
        assert!(GlobalIndexStorageTopology::hash_partitioned_sqlite_v1(3).is_err());
        assert!(GlobalIndexStorageTopology::hash_partitioned_sqlite_v1(16).is_ok());
    }

    #[test]
    fn partition_routing_has_frozen_cross_architecture_vectors() {
        let topology = GlobalIndexStorageTopology::HashPartitionedSqliteV1 {
            partitions: HASH_PARTITIONED_GLOBAL_INDEX_PARTITIONS_V1,
        };
        let vectors = [
            (1, "alpha", 0_u16),
            (1, "beta", 0_u16),
            (7, "alpha", 0_u16),
            (u64::MAX, "", 0_u16),
        ];
        let observed = vectors
            .into_iter()
            .map(|(id, value, _)| {
                let key = CanonicalIndexKey::encode_values(&[value.into()]).unwrap();
                topology
                    .partition_for_key(GlobalIndexId::new(id).unwrap(), &key)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(observed, [2, 5, 1, 12]);
        assert_eq!(
            topology.partition_count(),
            HASH_PARTITIONED_GLOBAL_INDEX_PARTITIONS_V1
        );
        assert_eq!(
            GlobalIndexStorageTopology::selected_v1(),
            GlobalIndexStorageTopology::SharedSqliteV1
        );
        assert!(
            GlobalIndexStorageTopology::Unassigned
                .partition_for_key(
                    GlobalIndexId::new(1).unwrap(),
                    &CanonicalIndexKey::encode_values(&["alpha".into()]).unwrap(),
                )
                .is_err()
        );
    }

    #[test]
    fn authority_types_are_bounded_owned_and_redact_locators() {
        assert!(GlobalOperationId::new([0; 16]).is_err());
        let operation = GlobalOperationId::new([9; 16]).unwrap();
        assert_eq!(operation.as_bytes(), [9; 16]);
        assert!(!format!("{operation:?}").contains("09090909"));

        assert!(GlobalIndexOwner::new(64, vec![1]).is_err());
        assert!(GlobalIndexOwner::new(0, Vec::new()).is_err());
        assert!(GlobalIndexOwner::new(0, vec![1; MAX_GLOBAL_OWNER_LOCATOR_BYTES + 1]).is_err());
        let owner = GlobalIndexOwner::new(7, b"secret-row".to_vec()).unwrap();
        assert_eq!(owner.source_shard(), 7);
        assert_eq!(owner.locator(), b"secret-row");
        assert!(!format!("{owner:?}").contains("secret-row"));

        let index_id = GlobalIndexId::new(3).unwrap();
        let key = CanonicalIndexKey::encode_values(&["key".into()]).unwrap();
        let mutation = GlobalUniqueMutation::replace(
            index_id,
            key.clone(),
            owner.clone(),
            key,
            GlobalIndexOwner::new(6, b"new-row".to_vec()).unwrap(),
        );
        assert_eq!(mutation.index_id(), index_id);
        assert!(mutation.previous_entry().is_some());
        assert!(mutation.new_entry().is_some());
    }

    #[test]
    fn outbox_types_preserve_cursor_event_and_redaction_contracts() {
        let index = GlobalIndexId::new(8).unwrap();
        let operation = GlobalOperationId::new([4; 16]).unwrap();
        let old_key = CanonicalIndexKey::encode_values(&["secret-old".into()]).unwrap();
        let new_key = CanonicalIndexKey::encode_values(&["secret-new".into()]).unwrap();
        let old_owner = GlobalIndexOwner::new(2, b"secret-owner-old".to_vec()).unwrap();
        let new_owner = GlobalIndexOwner::new(2, b"secret-owner-new".to_vec()).unwrap();
        let event = GlobalIndexOutboxEvent::from_validated_parts(
            1,
            11,
            index,
            operation,
            GlobalIndexOutboxEventKind::Update,
            Some(old_key),
            Some(new_key),
            Some(old_owner),
            Some(new_owner),
        );
        assert_eq!(event.format_version(), 1);
        assert_eq!(event.cursor(), GlobalIndexOutboxCursor::new(11));
        assert_eq!(event.index_id(), index);
        assert_eq!(event.operation_id(), operation);
        assert_eq!(event.kind(), GlobalIndexOutboxEventKind::Update);
        assert!(event.old_key().is_some());
        assert!(event.new_owner().is_some());
        let debug = format!("{event:?}");
        assert!(!debug.contains("secret-old"));
        assert!(!debug.contains("secret-owner-old"));

        let batch = GlobalIndexOutboxBatch::new(2, index, 7, 11, vec![event]);
        assert_eq!(batch.shard(), 2);
        assert_eq!(batch.after().get(), 7);
        assert_eq!(batch.high_water().get(), 11);
        assert_eq!(batch.events().len(), 1);
        let status = GlobalIndexOutboxShardStatus::new(2, 11, 3, 8, 512, 2, 7);
        assert_eq!(status.lag(), 4);
        assert!(!status.is_backpressured());
        let report = GlobalIndexOutboxPruneReport::new(2, 3, 192, 6);
        assert_eq!(report.deleted_events(), 3);
        assert_eq!(report.deleted_bytes(), 192);
        assert_eq!(report.pruned_through().get(), 6);
    }

    #[test]
    fn async_options_and_status_are_bounded_owned_and_redaction_safe() {
        assert!(GlobalIndexAsyncOptions::new(0, 5_000, 25).is_err());
        assert!(GlobalIndexAsyncOptions::new(4_097, 5_000, 25).is_err());
        assert!(GlobalIndexAsyncOptions::new(1, 99, 25).is_err());
        assert!(GlobalIndexAsyncOptions::new(1, 5_000, 0).is_err());
        let options = GlobalIndexAsyncOptions::new(64, 1_000, 10).unwrap();
        assert_eq!(options.batch_events(), 64);
        assert_eq!(options.lease_ms(), 1_000);
        assert_eq!(options.poll_ms(), 10);

        let shard = GlobalIndexAsyncShardStatus::new(2, 7, 11, 9, 1, 3, 250, Some(8), 4, true);
        assert_eq!(shard.lag(), 4);
        assert_eq!(shard.last_batch_events_per_second(), 12_000);
        assert_eq!(shard.poison_cursor().unwrap().get(), 8);
        assert!(!shard.is_fresh());
        let status =
            GlobalIndexAsyncStatus::new(GlobalIndexId::new(1).unwrap(), false, false, vec![shard]);
        assert_eq!(status.lag(), 4);
        assert!(!status.is_fresh());
        assert!(!format!("{status:?}").contains("secret"));
    }
}
