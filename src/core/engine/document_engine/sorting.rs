//! Bounded global top-key windows. Until sorted indexes exist, each window
//! scans matching documents again. Only keys/positions, never result documents
//! or SQLite leases, survive a scan or a cursor continuation.

use std::{cmp::Ordering, collections::BinaryHeap, sync::Arc, time::Instant};

use super::{
    DOCUMENT_RESULT_ROW_BYTES, DOCUMENT_RESULT_VALUE_BYTES, Engine, add_document_result_budget,
    cursor_page_base_bytes, limit_exceeded, next_matching_document, result_size_overflow,
    validate_point_record,
};
use crate::{
    core::engine::document_cursor::{CursorState, SortPosition},
    core::{CancellationToken, EngineError, EngineErrorKind, EngineResult, ResultLimits},
    document::{BsonDocument, DocumentMatcher, DocumentReadOptions},
    storage::ConnectionOwner,
};

const MAX_WINDOW_KEYS: u64 = 1024;
const MAX_WINDOW_BYTES: usize = 64 * 1024 * 1024;

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
}

impl Window {
    fn new(capacity: usize, byte_limit: usize) -> Self {
        Self {
            keys: BinaryHeap::new(),
            capacity,
            bytes: 0,
            byte_limit,
            truncated: false,
        }
    }

    fn consider(&mut self, entry: Entry) -> EngineResult<()> {
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

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn scan_sorted_document_page(
        &self,
        owner: ConnectionOwner,
        state: &mut CursorState,
        cancellation: CancellationToken,
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
        let mut result_bytes = cursor_page_base_bytes(state, self.shard_count());
        loop {
            check(&cancellation, deadline)?;
            let capacity = state
                .skip
                .saturating_add(requested)
                .saturating_add(1)
                .min(MAX_WINDOW_KEYS) as usize;
            let mut window = Window::new(capacity, MAX_WINDOW_BYTES);
            for shard in 0..self.shard_count() {
                let sorter = sorter.clone();
                let matcher = matcher.clone();
                let after = state.sort_after.clone();
                window = self
                    .run_document_shard(
                        shard,
                        owner,
                        cancellation.clone(),
                        deadline,
                        move |storage, connection, cancellation| {
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
                            )? {
                                validate_point_record(
                                    &record,
                                    collection_id,
                                    shard,
                                    record.id_key(),
                                )?;
                                natural_after = Some(record.natural_order());
                                let key = sorter
                                    .key_validated_with_check(record.document(), &mut || {
                                        check(cancellation, deadline)
                                    })?;
                                let position = SortPosition {
                                    key,
                                    natural_order: record.natural_order(),
                                };
                                if after.as_ref().is_none_or(|after| position > **after) {
                                    window.consider(Entry { position, shard })?;
                                }
                                check(cancellation, deadline)?;
                            }
                            Ok(window)
                        },
                    )
                    .await?;
            }
            let truncated = window.truncated;
            // Heap ordering and BSON key comparisons run inside admission too.
            let entries = self
                .run_document_storage_task(
                    cancellation.clone(),
                    deadline,
                    move |cancellation, control| {
                        super::ensure_document_cpu_active(cancellation, &control)?;
                        let mut keys = window.keys;
                        let mut entries = Vec::with_capacity(keys.len());
                        while let Some(entry) = keys.pop() {
                            super::ensure_document_cpu_active(cancellation, &control)?;
                            entries.push(entry);
                        }
                        entries.reverse();
                        super::ensure_document_cpu_active(cancellation, &control)?;
                        Ok(entries)
                    },
                )
                .await?;
            for entry in entries {
                check(&cancellation, deadline)?;
                if state.skip > 0 {
                    state.skip -= 1;
                    state.sort_after = Some(Arc::new(entry.position));
                    continue;
                }
                if documents.len() as u64 >= requested {
                    return Ok((documents, true));
                }
                let natural_order = entry.position.natural_order;
                let shard = entry.shard;
                let record = self
                    .run_document_shard(
                        shard,
                        owner,
                        cancellation.clone(),
                        deadline,
                        move |storage, connection, cancellation| {
                            let record = storage
                                .scan_document_shard_on_connection(
                                    connection,
                                    collection_id,
                                    shard,
                                    natural_order.checked_sub(1).filter(|after| *after > 0),
                                    1,
                                    cancellation,
                                )?
                                .into_iter()
                                .next();
                            if let Some(record) = &record {
                                validate_point_record(
                                    record,
                                    collection_id,
                                    shard,
                                    record.id_key(),
                                )?;
                            }
                            // A deletion between scan and fetch must never return
                            // an unrelated successor. No read snapshot is promised.
                            Ok(record.filter(|record| record.natural_order() == natural_order))
                        },
                    )
                    .await?;
                let Some(record) = record else {
                    state.sort_after = Some(Arc::new(entry.position));
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
                state.sort_after = Some(Arc::new(entry.position));
                if let Some(remaining) = &mut state.remaining {
                    *remaining -= 1;
                }
                if state.remaining == Some(0) {
                    return Ok((documents, false));
                }
            }
            if !truncated {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{BsonValue, DocumentSorter};

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
