//! Bounded global top-key windows from eligible ordered indexes or scan/sort
//! fallback. Remaining positions survive cursor continuations; no result
//! documents or SQLite leases are retained between requests.

use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    },
    time::Instant,
};

use super::{
    DOCUMENT_RESULT_ROW_BYTES, DOCUMENT_RESULT_VALUE_BYTES, Engine, RequestScope,
    add_document_result_budget, cursor_page_base_bytes, limit_exceeded, next_matching_document,
    result_size_overflow, validate_point_record,
};
use crate::{
    core::engine::document_cursor::{CursorState, ReadStats, SortPosition, SortWindow},
    core::engine::document_spool::{SortSpool, SpoolPosition},
    core::{CancellationToken, EngineError, EngineErrorKind, EngineResult, ResultLimits},
    document::{
        BsonDocument, DocumentCollectionId, DocumentMatcher, DocumentReadOptions, DocumentSortKey,
        DocumentSorter,
    },
    storage::{ConnectionOwner, Storage},
};

const MAX_WINDOW_KEYS: u64 = 65_536;
const MAX_WINDOW_BYTES: usize = 16 * 1024 * 1024;
// A handful of cached source scans can be cheaper than scratch writes when
// sort keys themselves are large. Keep that modest-overflow path, but never
// let cursor length turn it into an unbounded number of collection rescans.
const MAX_FALLBACK_WINDOWS: u8 = 8;

struct Entry {
    position: SortPosition,
    shard: u16,
}

impl Entry {
    fn bytes(&self) -> usize {
        128 + self.position.key.retained_bytes()
    }
}
impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.position == other.position
    }
}
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.position.cmp(&other.position)
    }
}

struct Window {
    keys: BinaryHeap<Entry>,
    capacity: usize,
    bytes: usize,
    byte_limit: usize,
    truncated: bool,
    considered: u64,
}

impl Window {
    fn new(capacity: usize, byte_limit: usize) -> Self {
        Self {
            keys: BinaryHeap::new(),
            capacity,
            bytes: 0,
            byte_limit,
            truncated: false,
            considered: 0,
        }
    }

    fn consider(&mut self, entry: Entry) -> EngineResult<()> {
        self.considered = self.considered.saturating_add(1);
        if self.keys.len() == self.capacity {
            self.truncated = true;
            if self.keys.peek().is_some_and(|largest| entry >= *largest) {
                return Ok(());
            }
            if let Some(previous) = self.keys.pop() {
                self.bytes -= previous.bytes();
            }
        }
        self.bytes += entry.bytes();
        self.keys.push(entry);
        while self.bytes > self.byte_limit {
            let removed = self.keys.pop().expect("nonempty over-budget window");
            self.bytes -= removed.bytes();
            self.truncated = true;
            // Never grow after trimming: a later, larger key must not leap
            // over a smaller key already discarded to meet the memory bound.
            self.capacity = self.keys.len();
        }
        if self.capacity == 0 {
            return Err(limit_exceeded(
                "document sort key cannot fit the bounded sort window",
            ));
        }
        Ok(())
    }
}

fn should_spool(window: &Window, fallback_scans: u8, skip: u64, remaining: Option<u64>) -> bool {
    window.truncated
        && remaining
            .is_none_or(|remaining| skip.saturating_add(remaining) > window.keys.len() as u64)
        && (window
            .considered
            .min(skip.saturating_add(remaining.unwrap_or(u64::MAX)))
            > window.keys.len() as u64 * u64::from(MAX_FALLBACK_WINDOWS)
            || fallback_scans >= MAX_FALLBACK_WINDOWS)
}

fn check(cancellation: &CancellationToken, deadline: Option<Instant>) -> EngineResult<()> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(EngineError::deadline_exceeded(
            "document sorting deadline exceeded",
        ));
    }
    if cancellation.is_cancelled() {
        return Err(EngineError::new(
            EngineErrorKind::Cancelled,
            "document sorting cancelled",
        ));
    }
    Ok(())
}

enum SelectedPosition {
    Memory(Arc<SortPosition>, u16),
    Spool(Arc<SpoolPosition>),
}

impl SelectedPosition {
    fn location(&self) -> (u64, u16) {
        match self {
            Self::Memory(position, shard) => (position.natural_order, *shard),
            Self::Spool(position) => (position.natural_order, position.shard),
        }
    }
}

fn consume_position(state: &mut CursorState) {
    if let Some(spool) = &mut state.sort_spool {
        spool
            .pending
            .pop_front()
            .expect("selected spooled position");
    } else {
        state.sort_after = Some(state.sort_window.as_mut().unwrap().consume());
    }
}

#[allow(clippy::too_many_arguments)]
fn try_ordered_window(
    storage: &Storage,
    connection: &rusqlite::Connection,
    collection: DocumentCollectionId,
    shard: u16,
    sorter: &DocumentSorter,
    matcher: Option<&DocumentMatcher>,
    after: Option<&SortPosition>,
    window: &Mutex<Window>,
    stats: Option<&ReadStats>,
    cancellation: &CancellationToken,
    deadline: Option<Instant>,
) -> EngineResult<bool> {
    let mut observe = |elapsed| {
        if let Some(stats) = stats {
            stats.streamed_storage_read(shard, elapsed);
        }
    };
    let observer = stats.map(|_| &mut observe as &mut dyn FnMut(std::time::Duration));
    storage.visit_ordered_documents_on_connection(
        connection,
        collection,
        shard,
        sorter,
        matcher,
        after.map(|position| (&position.key, position.natural_order)),
        &mut || check(cancellation, deadline),
        observer,
        |record, key| {
            if let Some(stats) = stats {
                stats.examine(shard, 1);
            }
            if let Some(matcher) = matcher {
                if let Some(stats) = stats {
                    stats.match_document();
                }
                if !matcher
                    .matches_with_check(record.document(), &mut || check(cancellation, deadline))?
                {
                    return Ok(true);
                }
            }
            if let Some(stats) = stats {
                stats.source_match(shard);
            }
            let position = SortPosition {
                key,
                natural_order: record.natural_order(),
            };
            let mut window = window.lock().map_err(|_| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    "document sort window lock poisoned",
                )
            })?;
            check(cancellation, deadline)?;
            let entry = Entry { position, shard };
            // This shard's next keys cannot beat the full global heap. Other
            // shards can only lower that frontier, so stopping here is safe.
            if window.keys.len() == window.capacity
                && window.keys.peek().is_some_and(|largest| entry >= *largest)
            {
                window.truncated = true;
                return Ok(false);
            }
            window.consider(entry)?;
            Ok(true)
        },
    )
}

impl Engine {
    async fn build_document_sort_spool(
        &self,
        owner: ConnectionOwner,
        state: &CursorState,
        cancellation: RequestScope,
        deadline: Option<Instant>,
        matcher: Option<Arc<DocumentMatcher>>,
    ) -> EngineResult<SortSpool> {
        let builder = Arc::new(Mutex::new(self.inner.document_cursors.sort_spool_builder()));
        let child_builder = builder.clone();
        let engine = self.clone();
        let collection = state.collection_id;
        let sorter = state.sorter.clone().expect("sorted cursor");
        let after = state.sort_after.clone();
        let stats = state.read_stats.clone();
        super::fanout::coordinate(
            state.source.shards(self.shard_count()).collect(),
            cancellation.clone(),
            self.inner.shutdown_cancel.clone(),
            deadline,
            move |shard, cancellation| {
                let engine = engine.clone();
                let builder = child_builder.clone();
                let sorter = sorter.clone();
                let matcher = matcher.clone();
                let after = after.clone();
                let stats = stats.clone();
                async move {
                    engine
                        .run_document_shard(
                            shard,
                            owner,
                            cancellation,
                            deadline,
                            move |storage, connection, cancellation| {
                                let mut natural_after = None;
                                while let Some(record) = next_matching_document(
                                    storage,
                                    connection,
                                    collection,
                                    shard,
                                    natural_after,
                                    matcher.as_deref(),
                                    cancellation,
                                    deadline,
                                    stats.as_deref(),
                                )? {
                                    validate_point_record(
                                        &record,
                                        collection,
                                        shard,
                                        record.id_key(),
                                    )?;
                                    natural_after = Some(record.natural_order());
                                    let mut check = || check(cancellation, deadline);
                                    let key = sorter
                                        .key_validated_with_check(record.document(), &mut check)?;
                                    let position = SortPosition {
                                        key,
                                        natural_order: record.natural_order(),
                                    };
                                    if after.as_ref().is_some_and(|after| position <= **after) {
                                        continue;
                                    }
                                    let position = SpoolPosition {
                                        key: position.key.ordered_bytes_with_check(&mut check)?,
                                        natural_order: position.natural_order,
                                        shard,
                                    };
                                    builder
                                        .lock()
                                        .map_err(|_| {
                                            EngineError::new(
                                                EngineErrorKind::Internal,
                                                "document sort spool lock poisoned",
                                            )
                                        })?
                                        .push(position, &mut check)?;
                                }
                                check(cancellation, deadline)
                            },
                        )
                        .await
                }
            },
        )
        .await?;
        let builder = Arc::try_unwrap(builder)
            .map_err(|_| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    "document sort spool still in use",
                )
            })?
            .into_inner()
            .map_err(|_| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    "document sort spool lock poisoned",
                )
            })?;
        self.run_document_storage_task(cancellation, deadline, move |cancellation, control| {
            builder.finish(&mut || super::ensure_document_cpu_active(cancellation, &control))
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn scan_sorted_document_page(
        &self,
        owner: ConnectionOwner,
        state: &mut CursorState,
        cancellation: RequestScope,
        deadline: Option<Instant>,
        matcher: Option<Arc<DocumentMatcher>>,
        options: &DocumentReadOptions,
        limits: ResultLimits,
    ) -> EngineResult<(Vec<BsonDocument>, bool)> {
        let sorter = state.sorter.clone().expect("sorted cursor");
        let collection_id = state.collection_id;
        let requested = state
            .remaining
            .unwrap_or(u64::MAX)
            .min(options.batch_size());
        let mut documents = Vec::new();
        let mut result_bytes = cursor_page_base_bytes(state, self.shard_count(), options);
        loop {
            check(&cancellation, deadline)?;
            if state.sort_spool.is_none()
                && state
                    .sort_window
                    .as_ref()
                    .is_none_or(|window| window.entries.is_empty())
            {
                if state
                    .sort_window
                    .as_ref()
                    .is_some_and(|window| !window.truncated)
                {
                    return Ok((documents, false));
                }
                if state
                    .sort_window
                    .as_ref()
                    .is_some_and(|window| window.spill_after)
                {
                    // Keep the already sorted prefix: spill only the remaining
                    // frontier after its keys have actually been consumed.
                    if !state.allow_sort_spill {
                        return Err(limit_exceeded(
                            "document sort exceeds its in-memory scan budget and disk use is disabled",
                        ));
                    }
                    state.sort_window = None;
                    state.sort_spool = Some(
                        self.build_document_sort_spool(
                            owner,
                            state,
                            cancellation.clone(),
                            deadline,
                            matcher.clone(),
                        )
                        .await?,
                    );
                } else {
                    let capacity = state
                        .skip
                        .saturating_add(state.remaining.unwrap_or(MAX_WINDOW_KEYS))
                        .saturating_add(1)
                        .min(MAX_WINDOW_KEYS) as usize;
                    // One global heap, not one allocation per shard. Only admitted
                    // blocking workers compare keys or hold this mutex; never across an
                    // await. At most eight decodes/key derivations are in flight, each
                    // with the existing BSON/work/key limits (keys are at most 8 MiB).
                    let window = Arc::new(Mutex::new(Window::new(capacity, MAX_WINDOW_BYTES)));
                    let child_window = window.clone();
                    let fallback = Arc::new(AtomicBool::new(false));
                    let child_fallback = fallback.clone();
                    let engine = self.clone();
                    let scan_sorter = sorter.clone();
                    let scan_matcher = matcher.clone();
                    let after = state.sort_after.clone();
                    let stats = state.read_stats.clone();
                    super::fanout::coordinate(
                        state.source.shards(self.shard_count()).collect(),
                        cancellation.clone(),
                        self.inner.shutdown_cancel.clone(),
                        deadline,
                        move |shard, cancellation| {
                            let engine = engine.clone();
                            let window = child_window.clone();
                            let sorter = scan_sorter.clone();
                            let matcher = scan_matcher.clone();
                            let after = after.clone();
                            let stats = stats.clone();
                            let fallback = child_fallback.clone();
                            async move {
                                engine
                                    .run_document_shard(
                                        shard,
                                        owner,
                                        cancellation,
                                        deadline,
                                        move |storage, connection, cancellation| {
                                            if try_ordered_window(
                                                storage,
                                                connection,
                                                collection_id,
                                                shard,
                                                &sorter,
                                                matcher.as_deref(),
                                                after.as_deref(),
                                                &window,
                                                stats.as_deref(),
                                                cancellation,
                                                deadline,
                                            )? {
                                                return Ok(());
                                            }
                                            fallback.store(true, AtomicOrdering::Relaxed);
                                            let mut natural_after = None;
                                            while let Some(record) = next_matching_document(
                                                storage,
                                                connection,
                                                collection_id,
                                                shard,
                                                natural_after,
                                                matcher.as_deref(),
                                                cancellation,
                                                deadline,
                                                stats.as_deref(),
                                            )? {
                                                validate_point_record(
                                                    &record,
                                                    collection_id,
                                                    shard,
                                                    record.id_key(),
                                                )?;
                                                natural_after = Some(record.natural_order());
                                                let key = sorter.key_validated_with_check(
                                                    record.document(),
                                                    &mut || check(cancellation, deadline),
                                                )?;
                                                let position = SortPosition {
                                                    key,
                                                    natural_order: record.natural_order(),
                                                };
                                                if after
                                                    .as_ref()
                                                    .is_none_or(|after| position > **after)
                                                {
                                                    let mut window = window.lock().map_err(|_| {
                                                    EngineError::new(
                                                        EngineErrorKind::Internal,
                                                        "document sort window lock poisoned",
                                                    )
                                                })?;
                                                    check(cancellation, deadline)?;
                                                    window.consider(Entry { position, shard })?;
                                                }
                                                check(cancellation, deadline)?;
                                            }
                                            Ok(())
                                        },
                                    )
                                    .await
                            }
                        },
                    )
                    .await?;
                    // All children have drained, including on error. No partial window
                    // is published. Arrival order may shorten a byte-trimmed page, but
                    // its keys always form a global prefix with natural-order ties.
                    let window = Arc::try_unwrap(window)
                        .map_err(|_| {
                            EngineError::new(
                                EngineErrorKind::Internal,
                                "document sort window still in use",
                            )
                        })?
                        .into_inner()
                        .map_err(|_| {
                            EngineError::new(
                                EngineErrorKind::Internal,
                                "document sort window lock poisoned",
                            )
                        })?;
                    let truncated = window.truncated;
                    let used_fallback = fallback.load(AtomicOrdering::Relaxed);
                    let fallback_scans = state
                        .sort_window
                        .as_ref()
                        .map_or(0, |window| window.fallback_scans)
                        .saturating_add(u8::from(used_fallback));
                    let spill_after = used_fallback
                        && should_spool(&window, fallback_scans, state.skip, state.remaining);
                    // Heap ordering and BSON key comparisons run inside admission too.
                    let entries = self
                        .run_document_storage_task(
                            cancellation.clone(),
                            deadline,
                            move |cancellation, control| {
                                super::ensure_document_cpu_active(cancellation, &control)?;
                                let mut keys = window.keys;
                                let mut entries =
                                    std::collections::VecDeque::with_capacity(keys.len());
                                while let Some(entry) = keys.pop() {
                                    super::ensure_document_cpu_active(cancellation, &control)?;
                                    entries.push_front((Arc::new(entry.position), entry.shard));
                                }
                                super::ensure_document_cpu_active(cancellation, &control)?;
                                Ok(entries)
                            },
                        )
                        .await?;
                    let mut retained = SortWindow::new(entries, truncated);
                    retained.fallback_scans = fallback_scans;
                    retained.spill_after = spill_after;
                    state.sort_window = Some(retained);
                }
            }
            loop {
                check(&cancellation, deadline)?;
                let selected = if let Some(mut spool) = state.sort_spool.take() {
                    if spool.pending.is_empty() {
                        spool = self
                            .run_document_storage_task(
                                cancellation.clone(),
                                deadline,
                                move |cancellation, control| {
                                    spool.advance(&mut || {
                                        super::ensure_document_cpu_active(cancellation, &control)
                                    })?;
                                    Ok(spool)
                                },
                            )
                            .await?;
                    }
                    let selected = spool.pending.front().cloned().map(SelectedPosition::Spool);
                    state.sort_spool = Some(spool);
                    selected
                } else {
                    state
                        .sort_window
                        .as_ref()
                        .and_then(|window| window.entries.front())
                        .cloned()
                        .map(|(position, shard)| SelectedPosition::Memory(position, shard))
                };
                let Some(selected) = selected else {
                    break;
                };
                if state.skip > 0 {
                    state.skip -= 1;
                    consume_position(state);
                    continue;
                }
                if documents.len() as u64 >= requested {
                    return Ok((documents, true));
                }
                let (natural_order, shard) = selected.location();
                let expected = selected;
                let fetch_sorter = sorter.clone();
                let fetch_matcher = matcher.clone();
                let fetch_stats = state.read_stats.clone();
                let record = self
                    .run_document_shard(
                        shard,
                        owner,
                        cancellation.clone(),
                        deadline,
                        move |storage, connection, cancellation| {
                            let record = ReadStats::observe_storage_read(
                                fetch_stats.as_deref(),
                                shard,
                                || {
                                    storage.scan_document_shard_on_connection(
                                        connection,
                                        collection_id,
                                        shard,
                                        natural_order.checked_sub(1).filter(|after| *after > 0),
                                        1,
                                        cancellation,
                                    )
                                },
                            )?
                            .into_iter()
                            .next();
                            if let Some(stats) = &fetch_stats {
                                stats.examine(shard, u64::from(record.is_some()));
                            }
                            if let Some(record) = &record {
                                validate_point_record(
                                    record,
                                    collection_id,
                                    shard,
                                    record.id_key(),
                                )?;
                            }
                            // Never return an unrelated successor after deletion,
                            // or a newly nonmatching / moved row after replacement.
                            // This is not a snapshot: moved keys may be omitted or
                            // encountered again in a later window.
                            let Some(record) =
                                record.filter(|record| record.natural_order() == natural_order)
                            else {
                                return Ok(None);
                            };
                            let stats = fetch_stats.as_deref().map(|stats| (shard, stats));
                            let mut check = || check(cancellation, deadline);
                            let selected = match expected {
                                SelectedPosition::Memory(expected, _) => still_selected(
                                    record.document(),
                                    fetch_matcher.as_deref(),
                                    &fetch_sorter,
                                    &expected.key,
                                    stats,
                                    &mut check,
                                )?,
                                SelectedPosition::Spool(expected) => match selected_key(
                                    record.document(),
                                    fetch_matcher.as_deref(),
                                    &fetch_sorter,
                                    stats,
                                    &mut check,
                                )? {
                                    Some(key) => {
                                        key.ordered_bytes_with_check(&mut check)? == expected.key
                                    }
                                    None => false,
                                },
                            };
                            if !selected {
                                return Ok(None);
                            }
                            Ok(Some(record))
                        },
                    )
                    .await?;
                let Some(record) = record else {
                    consume_position(state);
                    continue;
                };
                let (document, encoded_len) = self
                    .cursor_output_document(
                        record,
                        state.projection.clone(),
                        cancellation.clone(),
                        deadline,
                    )
                    .await?;
                let next_bytes = result_bytes
                    .checked_add(DOCUMENT_RESULT_ROW_BYTES)
                    .and_then(|bytes| bytes.checked_add(DOCUMENT_RESULT_VALUE_BYTES))
                    .and_then(|bytes| bytes.checked_add(encoded_len as u64))
                    .ok_or_else(result_size_overflow)?;
                if state
                    .batch_byte_limit
                    .is_some_and(|limit| next_bytes > limit)
                {
                    if documents.is_empty() {
                        return Err(limit_exceeded(
                            "document cannot fit the cursor batch byte limit",
                        ));
                    }
                    return Ok((documents, true));
                }
                add_document_result_budget(&mut result_bytes, encoded_len, limits)?;
                if documents.len() as u64 >= limits.max_rows() {
                    return Err(limit_exceeded(
                        "document result exceeds the request row limit",
                    ));
                }
                documents.push(document);
                consume_position(state);
                if let Some(remaining) = &mut state.remaining {
                    *remaining -= 1;
                }
                if state.remaining == Some(0) {
                    return Ok((documents, false));
                }
            }
            if state.sort_spool.is_some() || !state.sort_window.as_ref().unwrap().truncated {
                return Ok((documents, false));
            }
            // Skip may span several bounded windows. Once any rows are
            // returned, an internal window boundary is a valid short page.
            if !documents.is_empty() {
                return Ok((documents, true));
            }
        }
    }
}

fn still_selected(
    document: &BsonDocument,
    matcher: Option<&DocumentMatcher>,
    sorter: &DocumentSorter,
    expected: &DocumentSortKey,
    stats: Option<(u16, &ReadStats)>,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<bool> {
    Ok(selected_key(document, matcher, sorter, stats, check)?.as_ref() == Some(expected))
}

fn selected_key(
    document: &BsonDocument,
    matcher: Option<&DocumentMatcher>,
    sorter: &DocumentSorter,
    stats: Option<(u16, &ReadStats)>,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<Option<DocumentSortKey>> {
    check()?;
    if let Some(matcher) = matcher {
        if let Some((_, stats)) = stats {
            stats.match_document();
        }
        if !matcher.matches_with_check(document, check)? {
            return Ok(None);
        }
    }
    if let Some((shard, stats)) = stats {
        stats.source_match(shard);
    }
    let current = sorter.key_validated_with_check(document, check)?;
    check()?;
    Ok(Some(current))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{BsonValue, DocumentSorter};
    use proptest::prelude::*;

    #[test]
    fn modest_overflow_avoids_scratch_but_large_sets_and_repeated_fallback_are_bounded() {
        let sorter = DocumentSorter::compile(
            &BsonDocument::from_entries([("v", BsonValue::Int32(1))]).unwrap(),
        )
        .unwrap();
        for count in [8, 9, 64, 65] {
            let mut window = Window::new(8, 4096);
            for id in 0..count {
                window
                    .consider(Entry {
                        position: SortPosition {
                            key: sorter
                                .key(
                                    &BsonDocument::from_entries([("v", BsonValue::Int32(id))])
                                        .unwrap(),
                                )
                                .unwrap(),
                            natural_order: id as u64 + 1,
                        },
                        shard: 0,
                    })
                    .unwrap();
            }
            assert_eq!(window.considered, count as u64);
            assert_eq!(should_spool(&window, 1, 0, None), count > 64);
            assert_eq!(should_spool(&window, 8, 0, None), count > 8);
            assert!(!should_spool(&window, 8, 0, Some(8)));
            assert!(!should_spool(&window, 1, 0, Some(16)));
            assert_eq!(should_spool(&window, 8, 3, Some(6)), count > 8);
        }
    }

    proptest! {
        #[test]
        fn arbitrary_key_arrival_and_byte_trimming_always_retain_a_global_prefix(
            input in prop::collection::vec((0u16..100, 0usize..300), 1..80),
            capacity in 1usize..20,
            byte_limit in 2048usize..8192,
        ) {
            let sorter = DocumentSorter::compile(
                &BsonDocument::from_entries([("v", BsonValue::Int32(1))]).unwrap()
            ).unwrap();
            let entry = |index: usize, rank: u16, size: usize| Entry {
                position: SortPosition {
                    key: sorter.key(&BsonDocument::from_entries([
                        ("v", BsonValue::String(format!("{rank:03}{}", "x".repeat(size))))
                    ]).unwrap()).unwrap(),
                    natural_order: index as u64,
                },
                shard: (index % 16) as u16,
            };
            let mut expected: Vec<_> = input.iter().enumerate()
                .map(|(index, &(rank, size))| entry(index, rank, size)).collect();
            expected.sort();
            let mut window = Window::new(capacity, byte_limit);
            for (index, &(rank, size)) in input.iter().enumerate() {
                window.consider(entry(index, rank, size)).unwrap();
                prop_assert!(window.bytes <= byte_limit);
                prop_assert!(window.keys.len() <= capacity);
            }
            let actual = window.keys.into_sorted_vec();
            prop_assert!(!actual.is_empty());
            for (actual, expected) in actual.iter().zip(&expected) {
                prop_assert!(actual == expected);
                prop_assert_eq!(actual.shard, expected.shard);
            }
        }
    }

    #[test]
    fn selected_rows_are_rechecked_after_filter_or_sort_key_changes() {
        let document = |rank, visible, payload: &str| {
            BsonDocument::from_entries([
                ("rank", BsonValue::Int32(rank)),
                ("visible", BsonValue::Boolean(visible)),
                ("payload", BsonValue::from(payload)),
            ])
            .unwrap()
        };
        let original = document(1, true, "before");
        let sorter = DocumentSorter::compile(
            &BsonDocument::from_entries([("rank", BsonValue::Int32(1))]).unwrap(),
        )
        .unwrap();
        let matcher = DocumentMatcher::compile(
            &BsonDocument::from_entries([("visible", BsonValue::Boolean(true))]).unwrap(),
        )
        .unwrap();
        let expected = sorter.key(&original).unwrap();
        assert!(
            still_selected(
                &original,
                Some(&matcher),
                &sorter,
                &expected,
                None,
                &mut || Ok(())
            )
            .unwrap()
        );
        assert!(
            !still_selected(
                &document(1, false, "after"),
                Some(&matcher),
                &sorter,
                &expected,
                None,
                &mut || Ok(())
            )
            .unwrap()
        );
        assert!(
            !still_selected(
                &document(2, true, "after"),
                Some(&matcher),
                &sorter,
                &expected,
                None,
                &mut || Ok(())
            )
            .unwrap()
        );
        assert!(
            still_selected(
                &document(1, true, "after"),
                Some(&matcher),
                &sorter,
                &expected,
                None,
                &mut || Ok(())
            )
            .unwrap()
        );
        assert!(
            !still_selected(
                &document(2, true, "after"),
                None,
                &sorter,
                &expected,
                None,
                &mut || Ok(())
            )
            .unwrap()
        );
        let error = still_selected(&original, None, &sorter, &expected, None, &mut || {
            Err(EngineError::new(
                EngineErrorKind::Cancelled,
                "test cancellation",
            ))
        })
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
    }

    #[test]
    fn source_matches_are_counted_before_sort_position_rechecks() {
        let document = |rank, visible| {
            BsonDocument::from_entries([
                ("rank", BsonValue::Int32(rank)),
                ("visible", BsonValue::Boolean(visible)),
            ])
            .unwrap()
        };
        let original = document(1, true);
        let sorter = DocumentSorter::compile(
            &BsonDocument::from_entries([("rank", BsonValue::Int32(1))]).unwrap(),
        )
        .unwrap();
        let matcher = DocumentMatcher::compile(
            &BsonDocument::from_entries([("visible", BsonValue::Boolean(true))]).unwrap(),
        )
        .unwrap();
        let expected = sorter.key(&original).unwrap();
        let stats = ReadStats::default();
        stats.storage_read(5);
        for (row, predicate, selected, matches, evaluations) in [
            (document(2, true), Some(&matcher), false, 1, 1),
            (document(1, false), Some(&matcher), false, 1, 2),
            (original.clone(), Some(&matcher), true, 2, 3),
            (original.clone(), None, true, 3, 3),
        ] {
            assert_eq!(
                still_selected(
                    &row,
                    predicate,
                    &sorter,
                    &expected,
                    Some((5, &stats)),
                    &mut || Ok(())
                )
                .unwrap(),
                selected
            );
            assert_eq!(stats.snapshot().source_matches(), matches);
            assert_eq!(
                stats
                    .snapshot()
                    .shard_work()
                    .next()
                    .unwrap()
                    .source_matches(),
                matches
            );
            assert_eq!(stats.snapshot().matcher_evaluations(), evaluations);
        }
        let snapshot = stats.snapshot();
        let error = still_selected(
            &original,
            Some(&matcher),
            &sorter,
            &expected,
            Some((5, &stats)),
            &mut || {
                Err(EngineError::new(
                    EngineErrorKind::Cancelled,
                    "test cancellation",
                ))
            },
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        assert_eq!(stats.snapshot(), snapshot);
    }

    #[test]
    fn top_key_windows_keep_a_sorted_prefix_after_memory_trimming() {
        let sorter = DocumentSorter::compile(
            &BsonDocument::from_entries([("v", BsonValue::Int32(1))]).unwrap(),
        )
        .unwrap();
        let entry = |number: u64, size: usize| Entry {
            position: SortPosition {
                key: sorter
                    .key(
                        &BsonDocument::from_entries([(
                            "v",
                            BsonValue::String(format!("{number:03}{}", "x".repeat(size))),
                        )])
                        .unwrap(),
                    )
                    .unwrap(),
                natural_order: number,
            },
            shard: 0,
        };
        let mut window = Window::new(5, 1000);
        for number in [5, 3, 4, 2, 8, 1, 9] {
            window.consider(entry(number, 10)).unwrap();
        }
        assert!(window.truncated);
        assert!(window.bytes <= 1000);
        let numbers: Vec<_> = window
            .keys
            .into_sorted_vec()
            .into_iter()
            .map(|entry| entry.position.natural_order)
            .collect();
        assert_eq!(numbers, vec![1, 2]);
        let mut window = Window::new(5, 1000);
        window.consider(entry(1, 10)).unwrap();
        window.consider(entry(2, 500)).unwrap();
        window.consider(entry(3, 10)).unwrap();
        assert_eq!(
            window.keys.len(),
            1,
            "cannot skip the trimmed key to include a later key"
        );
        assert_eq!(window.keys.peek().unwrap().position.natural_order, 1);
    }
}
