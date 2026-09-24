//! Bounded, session-owned cursors. No SQLite handles survive a request. Find
//! retains positions; aggregation also accounts for pipeline state and any
//! bounded results produced by blocking stages.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use crate::{
    core::{EngineError, EngineErrorKind, EngineResult},
    document::{
        BsonDocument, CanonicalBsonKey, DocumentAggregationStream, DocumentCollectionId,
        DocumentCursorError, DocumentCursorId, DocumentDatabaseId, DocumentMatcher,
        DocumentNamespace, DocumentPlan, DocumentPointPlan, DocumentProjector, DocumentScatterPlan,
        DocumentSortKey, DocumentSorter,
    },
    storage::ConnectionOwner,
};

const MAX_CURSORS: usize = 32;
const MAX_SESSION_CURSORS: usize = 8;
const MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Clone)]
pub(super) enum CursorSource {
    Point {
        id_key: CanonicalBsonKey,
        shard: u16,
    },
    Scatter(Option<Arc<DocumentMatcher>>),
    /// A proven nonempty subset of the at most 64 physical shards. The complete
    /// matcher remains authoritative, either here or in the aggregation runner;
    /// the bitmap is only a routing restriction.
    ShardSubset {
        matcher: Option<Arc<DocumentMatcher>>,
        shards: u64,
    },
}

impl CursorSource {
    pub(super) fn targets(&self, shard: u16) -> bool {
        match self {
            Self::Point { shard: target, .. } => *target == shard,
            Self::Scatter(_) => true,
            Self::ShardSubset { shards, .. } => shards & (1_u64 << shard) != 0,
        }
    }

    pub(super) fn shards(&self, shard_count: u16) -> impl Iterator<Item = u16> + '_ {
        (0..shard_count).filter(|shard| self.targets(*shard))
    }

    pub(super) fn matcher(&self) -> Option<&Arc<DocumentMatcher>> {
        match self {
            Self::Point { .. } => None,
            Self::Scatter(matcher) | Self::ShardSubset { matcher, .. } => matcher.as_ref(),
        }
    }

    pub(super) fn plan(
        &self,
        collection_id: DocumentCollectionId,
        shard_count: u16,
    ) -> EngineResult<DocumentPlan> {
        match self {
            Self::Point { id_key, shard } => {
                DocumentPointPlan::new(collection_id, *shard, id_key.clone())
                    .map(DocumentPlan::Point)
            }
            Self::Scatter(_) | Self::ShardSubset { .. } => DocumentScatterPlan::new(
                collection_id,
                self.shards(shard_count).collect::<Vec<_>>(),
            )
            .map(DocumentPlan::Scatter),
        }
    }
}

pub(super) struct CursorState {
    pub namespace: DocumentNamespace,
    pub collection_id: DocumentCollectionId,
    pub source: CursorSource,
    pub projection: Option<Arc<DocumentProjector>>,
    pub sorter: Option<Arc<DocumentSorter>>,
    pub sort_after: Option<Arc<SortPosition>>,
    pub after: Option<u64>,
    pub skip: u64,
    pub remaining: Option<u64>,
    pub batch_byte_limit: Option<u64>,
    pub aggregation: Option<AggregateCursor>,
}

/// Metadata cursors retain only bounded filter/position state, never catalog rows
/// or SQLite handles. Identities fence drop/recreate; the ceiling excludes later creations.
#[derive(Clone)]
pub(super) struct MetadataCursorState {
    pub namespace: DocumentNamespace,
    pub database_id: DocumentDatabaseId,
    pub upper_id: u64,
    pub after_id: u64,
    pub matcher: Arc<DocumentMatcher>,
    pub name_only: bool,
    pub batch_byte_limit: Option<u64>,
}

/// Index discovery keeps a stable collection identity and allocation ceiling,
/// not index definitions. None precedes the built-in; Some("") follows it.
#[derive(Clone)]
pub(super) struct IndexMetadataCursorState {
    pub namespace: DocumentNamespace,
    pub collection_id: DocumentCollectionId,
    pub upper_id: u64,
    pub after_name: Option<String>,
    pub batch_byte_limit: Option<u64>,
}

pub(super) enum RetainedCursorState {
    Documents(Box<CursorState>),
    Collections(MetadataCursorState),
    Indexes(IndexMetadataCursorState),
}

impl From<CursorState> for RetainedCursorState {
    fn from(state: CursorState) -> Self {
        Self::Documents(Box::new(state))
    }
}

impl From<Box<CursorState>> for RetainedCursorState {
    fn from(state: Box<CursorState>) -> Self {
        Self::Documents(state)
    }
}

impl RetainedCursorState {
    fn namespace(&self) -> &DocumentNamespace {
        match self {
            Self::Documents(state) => &state.namespace,
            Self::Collections(state) => &state.namespace,
            Self::Indexes(state) => &state.namespace,
        }
    }

    fn retained_bytes(&self) -> usize {
        match self {
            Self::Documents(state) => state.retained_bytes(),
            Self::Collections(state) => 4096usize.saturating_add(state.matcher.retained_bytes()),
            Self::Indexes(_) => 4096,
        }
    }
}

pub(super) struct AggregateCursor {
    pub runner: Option<DocumentAggregationStream>,
    pub pending: VecDeque<AggregateRow>,
    pub bytes: usize,
    pub source_exhausted: bool,
}

pub(super) struct AggregateRow {
    pub document: BsonDocument,
    pub encoded_len: usize,
    pub retained_bytes: usize,
}

impl AggregateCursor {
    fn retained_bytes(&self) -> usize {
        self.bytes
            .saturating_add(
                self.runner
                    .as_ref()
                    .map_or(0, |runner| runner.retained_bytes()),
            )
            // Pop-front does not shrink VecDeque's backing allocation.
            .saturating_add(
                self.pending
                    .capacity()
                    .saturating_mul(std::mem::size_of::<AggregateRow>()),
            )
            .saturating_add(512)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct SortPosition {
    pub key: DocumentSortKey,
    pub natural_order: u64,
}

impl CursorState {
    fn retained_bytes(&self) -> usize {
        4096usize
            .saturating_add(
                self.aggregation
                    .as_ref()
                    .map_or(0, |state| state.retained_bytes()),
            )
            .saturating_add(
                self.sorter
                    .as_ref()
                    .map_or(0, |sorter| sorter.retained_bytes()),
            )
            .saturating_add(
                self.sort_after
                    .as_ref()
                    .map_or(0, |position| position.key.retained_bytes()),
            )
            .saturating_add(
                self.projection
                    .as_ref()
                    .map_or(0, |projection| projection.retained_bytes()),
            )
            .saturating_add(match &self.source {
                CursorSource::Point { id_key, .. } => id_key.as_bytes().len(),
                CursorSource::Scatter(matcher) | CursorSource::ShardSubset { matcher, .. } => {
                    matcher
                        .as_ref()
                        .map_or(0, |matcher| matcher.retained_bytes())
                }
            })
    }
}

struct Entry {
    owner: ConnectionOwner,
    namespace: DocumentNamespace,
    touched: Instant,
    state: Option<RetainedCursorState>,
    retained_bytes: usize,
}

#[derive(Default)]
struct Inner {
    closed: bool,
    entries: BTreeMap<DocumentCursorId, Entry>,
}

#[derive(Default)]
pub(super) struct CursorRegistry(Mutex<Inner>);

impl std::fmt::Debug for CursorRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DocumentCursorRegistry")
            .finish_non_exhaustive()
    }
}

impl CursorRegistry {
    pub fn insert(
        self: &Arc<Self>,
        owner: ConnectionOwner,
        state: impl Into<RetainedCursorState>,
    ) -> EngineResult<DocumentCursorId> {
        let state = state.into();
        let mut inner = self.0.lock().unwrap_or_else(|error| error.into_inner());
        prune(&mut inner, Instant::now());
        if inner.closed {
            return Err(closed());
        }
        let retained_bytes = state.retained_bytes();
        let current_bytes: usize = inner
            .entries
            .values()
            .map(|entry| entry.retained_bytes)
            .sum();
        if inner.entries.len() >= MAX_CURSORS
            || inner
                .entries
                .values()
                .filter(|entry| entry.owner == owner)
                .count()
                >= MAX_SESSION_CURSORS
            || current_bytes.saturating_add(retained_bytes) > MAX_RETAINED_BYTES
        {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                "open document cursor limit exceeded",
            ));
        }
        let id = loop {
            let mut bytes = [0; 8];
            getrandom::fill(&mut bytes).map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::Internal,
                    "unable to allocate document cursor identity",
                    error,
                )
            })?;
            let value = u64::from_le_bytes(bytes) & i64::MAX as u64;
            if value != 0 {
                let id = DocumentCursorId::new(value)?;
                if !inner.entries.contains_key(&id) {
                    break id;
                }
            }
        };
        inner.entries.insert(
            id,
            Entry {
                owner,
                namespace: state.namespace().clone(),
                touched: Instant::now(),
                state: Some(state),
                retained_bytes,
            },
        );
        Ok(id)
    }

    pub fn checkout(
        self: &Arc<Self>,
        owner: ConnectionOwner,
        namespace: &DocumentNamespace,
        id: DocumentCursorId,
    ) -> EngineResult<CursorLease> {
        let mut inner = self.0.lock().unwrap_or_else(|error| error.into_inner());
        prune(&mut inner, Instant::now());
        let entry = inner
            .entries
            .get_mut(&id)
            .filter(|entry| entry.owner == owner && &entry.namespace == namespace)
            .ok_or_else(|| DocumentCursorError::NotFound.into_engine_error())?;
        let state = entry
            .state
            .take()
            .ok_or_else(|| DocumentCursorError::InUse.into_engine_error())?;
        Ok(CursorLease {
            registry: Arc::clone(self),
            id,
            state: Some(state),
            completed: false,
        })
    }

    pub fn kill(
        &self,
        owner: ConnectionOwner,
        namespace: &DocumentNamespace,
        id: DocumentCursorId,
    ) -> bool {
        let mut inner = self.0.lock().unwrap_or_else(|error| error.into_inner());
        prune(&mut inner, Instant::now());
        if inner
            .entries
            .get(&id)
            .is_some_and(|entry| entry.owner == owner && &entry.namespace == namespace)
        {
            inner.entries.remove(&id);
            true
        } else {
            false
        }
    }

    pub fn owner(self: &Arc<Self>, owner: ConnectionOwner) -> DocumentCursorOwner {
        DocumentCursorOwner {
            registry: Arc::downgrade(self),
            owner,
        }
    }

    pub fn close(&self) {
        let mut inner = self.0.lock().unwrap_or_else(|error| error.into_inner());
        inner.closed = true;
        inner.entries.clear();
    }
}

fn prune(inner: &mut Inner, now: Instant) {
    inner.entries.retain(|_, entry| {
        entry.state.is_none() || now.duration_since(entry.touched) < IDLE_TIMEOUT
    });
}

fn closed() -> EngineError {
    EngineError::new(
        EngineErrorKind::ShuttingDown,
        "document cursor registry is closed",
    )
}

pub(super) struct CursorLease {
    registry: Arc<CursorRegistry>,
    id: DocumentCursorId,
    pub state: Option<RetainedCursorState>,
    completed: bool,
}

impl CursorLease {
    pub fn complete(
        mut self,
        state: Option<RetainedCursorState>,
    ) -> EngineResult<Option<DocumentCursorId>> {
        let mut inner = self
            .registry
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if inner.closed {
            return Err(closed());
        }
        if let Some(state) = &state {
            let retained_bytes = state.retained_bytes();
            let others: usize = inner
                .entries
                .iter()
                .filter(|(id, _)| **id != self.id)
                .map(|(_, entry)| entry.retained_bytes)
                .sum();
            if others.saturating_add(retained_bytes) > MAX_RETAINED_BYTES {
                return Err(EngineError::new(
                    EngineErrorKind::LimitExceeded,
                    "open document cursor limit exceeded",
                ));
            }
        }
        let entry = inner
            .entries
            .get_mut(&self.id)
            .ok_or_else(|| DocumentCursorError::NotFound.into_engine_error())?;
        let retained = state.is_some();
        if retained {
            entry.retained_bytes = state.as_ref().expect("retained state").retained_bytes();
            entry.state = state;
            entry.touched = Instant::now();
        } else {
            inner.entries.remove(&self.id);
        }
        self.completed = true;
        Ok(retained.then_some(self.id))
    }
}

impl Drop for CursorLease {
    fn drop(&mut self) {
        if !self.completed {
            self.registry
                .0
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .entries
                .remove(&self.id);
        }
    }
}

/// Attached to the session only after it opens a cursor. Drop/close cleanup is
/// synchronous and never schedules SQLite work or depends on a live runtime.
pub(crate) struct DocumentCursorOwner {
    registry: Weak<CursorRegistry>,
    owner: ConnectionOwner,
}

impl Drop for DocumentCursorOwner {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry
                .0
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .entries
                .retain(|_, entry| entry.owner != self.owner);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{BsonDocument, BsonValue};

    fn state() -> CursorState {
        CursorState {
            namespace: DocumentNamespace::new("app", "items").unwrap(),
            collection_id: DocumentCollectionId::from_validated(1),
            source: CursorSource::Scatter(None),
            aggregation: None,
            projection: None,
            sorter: None,
            sort_after: None,
            after: None,
            skip: 0,
            remaining: None,
            batch_byte_limit: None,
        }
    }

    #[test]
    fn idle_expiry_in_use_and_shutdown_release_retained_state() {
        let registry = Arc::new(CursorRegistry::default());
        let owner = ConnectionOwner::new(1);
        let namespace = state().namespace;
        let id = registry.insert(owner, state()).unwrap();
        registry
            .0
            .lock()
            .unwrap()
            .entries
            .get_mut(&id)
            .unwrap()
            .touched = Instant::now() - IDLE_TIMEOUT;
        assert!(registry.checkout(owner, &namespace, id).is_err());
        assert!(registry.0.lock().unwrap().entries.is_empty());
        let id = registry.insert(owner, state()).unwrap();
        let mut lease = registry.checkout(owner, &namespace, id).unwrap();
        assert!(registry.checkout(owner, &namespace, id).is_err());
        let state = lease.state.take();
        assert_eq!(lease.complete(state).unwrap(), Some(id));
        let lease = registry.checkout(owner, &namespace, id).unwrap();
        registry.close();
        assert!(lease.complete(None).is_err());
        assert!(registry.0.lock().unwrap().entries.is_empty());
        assert!(registry.insert(owner, super::tests::state()).is_err());
    }

    #[test]
    fn matcher_retention_is_accounted_against_a_global_quota() {
        let registry = Arc::new(CursorRegistry::default());
        let query =
            BsonDocument::from_entries([("v", BsonValue::String("x".repeat(512 * 1024)))]).unwrap();
        let matcher = Arc::new(DocumentMatcher::compile(&query).unwrap());
        for owner in 1..=7 {
            let mut cursor = state();
            cursor.source = CursorSource::Scatter(Some(Arc::clone(&matcher)));
            registry
                .insert(ConnectionOwner::new(owner), cursor)
                .unwrap();
        }
        let mut cursor = state();
        cursor.source = CursorSource::Scatter(Some(matcher));
        assert_eq!(
            registry
                .insert(ConnectionOwner::new(8), cursor)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        drop(registry.owner(ConnectionOwner::new(1)));
        assert_eq!(registry.0.lock().unwrap().entries.len(), 6);
        registry.close();
        assert!(registry.0.lock().unwrap().entries.is_empty());
    }

    #[test]
    fn metadata_filters_share_the_document_cursor_memory_quota() {
        let registry = Arc::new(CursorRegistry::default());
        let query =
            BsonDocument::from_entries([("name", BsonValue::String("x".repeat(512 * 1024)))])
                .unwrap();
        let matcher = Arc::new(DocumentMatcher::compile(&query).unwrap());
        let metadata = || {
            RetainedCursorState::Collections(MetadataCursorState {
                namespace: DocumentNamespace::new("app", "$cmd.listCollections").unwrap(),
                database_id: DocumentDatabaseId::from_validated(1),
                upper_id: 1,
                after_id: 0,
                matcher: Arc::clone(&matcher),
                name_only: true,
                batch_byte_limit: None,
            })
        };
        for owner in 1..=7 {
            let retained = if owner % 2 == 0 {
                let mut cursor = state();
                cursor.source = CursorSource::Scatter(Some(Arc::clone(&matcher)));
                cursor.into()
            } else {
                metadata()
            };
            registry
                .insert(ConnectionOwner::new(owner), retained)
                .unwrap();
        }
        assert_eq!(
            registry
                .insert(ConnectionOwner::new(8), metadata())
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        drop(registry.owner(ConnectionOwner::new(1)));
        assert!(registry.insert(ConnectionOwner::new(8), metadata()).is_ok());
    }

    #[test]
    fn aggregate_retention_includes_backing_capacity_and_growth_releases_failed_leases() {
        use crate::document::{DocumentAggregator, DocumentPipeline};
        let registry = Arc::new(CursorRegistry::default());
        let owner = ConnectionOwner::new(1);
        let mut cursor = state();
        cursor.aggregation = Some(AggregateCursor {
            runner: Some(
                DocumentAggregator::compile(&DocumentPipeline::new(Vec::new()).unwrap())
                    .unwrap()
                    .into_stream(),
            ),
            pending: VecDeque::with_capacity(128),
            bytes: 0,
            source_exhausted: false,
        });
        assert!(cursor.retained_bytes() >= 4096 + 128 * std::mem::size_of::<AggregateRow>());
        let namespace = cursor.namespace.clone();
        let id = registry.insert(owner, cursor).unwrap();
        let mut lease = registry.checkout(owner, &namespace, id).unwrap();
        let RetainedCursorState::Documents(mut cursor) = lease.state.take().unwrap() else {
            panic!("document cursor")
        };
        cursor.aggregation.as_mut().unwrap().bytes = MAX_RETAINED_BYTES;
        assert_eq!(
            lease.complete(Some(cursor.into())).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        assert!(registry.checkout(owner, &namespace, id).is_err());
        assert!(registry.0.lock().unwrap().entries.is_empty());
        assert!(registry.insert(owner, state()).is_ok());
    }

    #[test]
    fn projection_retention_shares_the_global_cursor_quota() {
        let registry = Arc::new(CursorRegistry::default());
        let spec = crate::document::BsonDocument::from_entries([(
            "x".repeat(512 * 1024),
            crate::document::BsonValue::Int32(1),
        )])
        .unwrap();
        let projection = Arc::new(DocumentProjector::compile(&spec).unwrap());
        let mut retained = state();
        retained.projection = Some(projection.clone());
        assert!(retained.retained_bytes() > 8 * 1024 * 1024);
        for owner in 1..=7 {
            let mut retained = state();
            retained.projection = Some(projection.clone());
            registry
                .insert(ConnectionOwner::new(owner), retained)
                .unwrap();
        }
        assert_eq!(
            registry
                .insert(ConnectionOwner::new(8), retained)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
    }

    #[test]
    fn sort_key_growth_is_reaccounted_and_quota_failure_discards_the_cursor() {
        let registry = Arc::new(CursorRegistry::default());
        let sorter = Arc::new(
            DocumentSorter::compile(
                &BsonDocument::from_entries([("v", BsonValue::Int32(1))]).unwrap(),
            )
            .unwrap(),
        );
        let position = |size| {
            Arc::new(SortPosition {
                key: sorter
                    .key(
                        &BsonDocument::from_entries([("v", BsonValue::String("x".repeat(size)))])
                            .unwrap(),
                    )
                    .unwrap(),
                natural_order: 1,
            })
        };
        let small = position(7 * 1024 * 1024);
        let mut ids = Vec::new();
        for owner in 1..=9 {
            let mut retained = state();
            retained.sorter = Some(sorter.clone());
            retained.sort_after = Some(small.clone());
            ids.push(
                registry
                    .insert(ConnectionOwner::new(owner), retained)
                    .unwrap(),
            );
        }
        let mut lease = registry
            .checkout(ConnectionOwner::new(1), &state().namespace, ids[0])
            .unwrap();
        let RetainedCursorState::Documents(mut retained) = lease.state.take().unwrap() else {
            panic!("document cursor")
        };
        retained.sort_after = Some(position(8 * 1024 * 1024 - 256));
        assert_eq!(
            lease.complete(Some(retained.into())).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        assert!(!registry.0.lock().unwrap().entries.contains_key(&ids[0]));
        let mut lease = registry
            .checkout(ConnectionOwner::new(2), &state().namespace, ids[1])
            .unwrap();
        let RetainedCursorState::Documents(mut retained) = lease.state.take().unwrap() else {
            panic!("document cursor")
        };
        retained.sort_after = Some(position(1));
        lease.complete(Some(retained.into())).unwrap();
        assert!(registry.0.lock().unwrap().entries[&ids[1]].retained_bytes < 10_000);
    }
}
