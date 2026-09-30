//! Index-ordered streams, admitted only from the Ready capability cache. Keep
//! the marker probe's aggregate row alive through the stream, pinning a single
//! SQLite snapshot without starting or ending a caller-owned transaction.

use super::*;
use crate::document::{DocumentSortKey, DocumentSorter};

const MARKERS: &str = "SELECT EXISTS (SELECT 1 FROM briskdb_document_ordered_entries_v1
    INDEXED BY briskdb_document_ordered_scan_v1
    WHERE collection_id = ?1 AND index_id = ?2 AND direction = ?3 AND sort_key IS NULL)";
const RECORDS: &str = "SELECT d.natural_order, d.id_key, d.document_bson, d.document_checksum,
    d.storage_format_version, e.sort_key, e.natural_order, e.entry_checksum, e.entry_format_version
    FROM briskdb_document_ordered_entries_v1 AS e INDEXED BY briskdb_document_ordered_scan_v1
    CROSS JOIN briskdb_documents_v1 AS d ON d.collection_id = e.collection_id AND d.id_key = e.id_key
    WHERE e.collection_id = ?1 AND e.index_id = ?2 AND e.direction = ?3
      AND (e.sort_key, e.natural_order) > (?4, ?5)
    ORDER BY e.sort_key, e.natural_order";

impl Storage {
    pub(crate) fn document_ordered_probe(
        &self,
        collection: DocumentCollectionId,
        sorter: &DocumentSorter,
        matcher: Option<&DocumentMatcher>,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Option<(DocumentIndexId, u8)>> {
        // Preserve finite candidate seeks: walking an unrelated sort index to
        // find one equality match must not undo the point/membership fixes.
        // Choosing between broad finite groups and ordered scans needs costing;
        // until then, finite candidate authority wins conservatively.
        if let Some(matcher) = matcher {
            if self
                .document_equality_probe(collection, matcher, check)?
                .is_some_and(|probe| matches!(probe.selection(), DocumentIndexSelection::Keys(_)))
            {
                return Ok(None);
            }
        }
        Ok(self
            .active_document_indexes(collection)?
            .and_then(|indexes| indexes.ordered_probe(sorter)))
    }

    /// False means no safe ordered path; no document has been visited then.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn visit_ordered_documents_on_connection(
        &self,
        connection: &Connection,
        collection: DocumentCollectionId,
        shard: u16,
        sorter: &DocumentSorter,
        matcher: Option<&DocumentMatcher>,
        after: Option<(&DocumentSortKey, u64)>,
        check: &mut dyn FnMut() -> EngineResult<()>,
        mut observe: Option<&mut dyn FnMut(std::time::Duration)>,
        mut visit: impl FnMut(DocumentStorageRecord, DocumentSortKey) -> EngineResult<bool>,
    ) -> EngineResult<bool> {
        let result = (|| {
            check()?;
            let Some((index, direction)) =
                self.document_ordered_probe(collection, sorter, matcher, check)?
            else {
                return Ok(false);
            };
            require_schema(connection)?;
            let (after_key, after_natural) = match after {
                Some((key, natural)) => (
                    key.ordered_bytes_with_check(check)?,
                    document_natural_order_to_sqlite(natural)?,
                ),
                None => (Vec::new(), 0),
            };
            let mut probe = connection
                .prepare_cached(MARKERS)
                .map_err(|error| shard_read_error(error, "ordered index coverage is missing"))?;
            let mut snapshot = probe
                .query(params![
                    collection.get() as i64,
                    index.get() as i64,
                    direction
                ])
                .map_err(sqlite_error::storage)?;
            let marker: bool = snapshot
                .next()
                .map_err(sqlite_error::storage)?
                .ok_or_else(|| corrupt("ordered index marker probe omitted its row"))?
                .get(0)
                .map_err(sqlite_error::storage)?;
            if marker {
                return Ok(false);
            }
            let mut statement = connection
                .prepare_cached(RECORDS)
                .map_err(sqlite_error::storage)?;
            let mut rows = statement
                .query(params![
                    collection.get() as i64,
                    index.get() as i64,
                    direction,
                    after_key,
                    after_natural
                ])
                .map_err(sqlite_error::storage)?;
            loop {
                check()?;
                let started = observe.as_ref().map(|_| std::time::Instant::now());
                let fetched: EngineResult<_> = (|| {
                    let Some(row) = rows.next().map_err(sqlite_error::storage)? else {
                        return Ok(None);
                    };
                    let record = decode_storage_record(
                        collection,
                        shard,
                        row.get(0).map_err(sqlite_error::storage)?,
                        row.get(1).map_err(sqlite_error::storage)?,
                        row.get(2).map_err(sqlite_error::storage)?,
                        row.get(3).map_err(sqlite_error::storage)?,
                        row.get(4).map_err(sqlite_error::storage)?,
                    )?;
                    if self.shard_for_key(record.id_key.as_bytes()) != shard {
                        return Err(corrupt(
                            "ordered document index returned a misrouted record",
                        ));
                    }
                    let key = row
                        .get_ref(5)
                        .and_then(|value| value.as_blob().map_err(Into::into))
                        .map_err(sqlite_error::storage)?;
                    let natural: i64 = row.get(6).map_err(sqlite_error::storage)?;
                    let digest = row
                        .get_ref(7)
                        .and_then(|value| value.as_blob().map_err(Into::into))
                        .map_err(sqlite_error::storage)?;
                    let version: i64 = row.get(8).map_err(sqlite_error::storage)?;
                    if version != 1
                        || natural != record.natural_order() as i64
                        || key.len() > 65536
                        || digest
                            != super::super::ordered_storage::checksum(
                                collection,
                                index,
                                shard,
                                record.id_key.as_bytes(),
                                direction,
                                Some(key),
                                natural,
                                &record.checksum,
                            )
                    {
                        return Err(corrupt("ordered index candidate is stale or damaged"));
                    }
                    let derived = sorter
                        .key_validated_with_check(record.document(), check)
                        .map_err(stored_index_error)?;
                    if derived.ordered_bytes_with_check(check)? != key {
                        return Err(corrupt(
                            "ordered index candidate disagrees with its document",
                        ));
                    }
                    Ok(Some((record, derived)))
                })();
                if let (Some(observe), Some(started)) = (observe.as_mut(), started) {
                    observe(started.elapsed());
                }
                let Some((record, derived)) = fetched? else {
                    break;
                };
                if !visit(record, derived)? {
                    break;
                }
            }
            check()?;
            // Explicit drop order documents that the admission snapshot lasts
            // through the final callback, including early stop/error cleanup.
            drop(rows);
            drop(snapshot);
            Ok(true)
        })();
        self.fail_closed_on_corruption(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_stream_seeks_its_frontier_without_temp_sorting_or_scanning_records() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(super::super::super::RECORDS_SCHEMA_SQL)
            .unwrap();
        let transaction = connection.transaction().unwrap();
        super::super::super::ordered_storage::ensure_schema(&transaction).unwrap();
        transaction.commit().unwrap();
        for analyzed in [false, true] {
            if analyzed {
                connection.execute_batch("ANALYZE").unwrap();
            }
            let plan: Vec<String> = connection
                .prepare(&format!("EXPLAIN QUERY PLAN {RECORDS}"))
                .unwrap()
                .query_map(params![1, 2, 0, b"BBSO-frontier".as_slice(), 4], |row| {
                    row.get(3)
                })
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert!(
                plan.iter().any(|step| step
                    .contains("SEARCH e USING COVERING INDEX briskdb_document_ordered_scan_v1")
                    && step.contains("sort_key")),
                "{plan:?}"
            );
            assert!(
                plan.iter().any(|step| step
                    .contains("SEARCH d USING PRIMARY KEY (collection_id=? AND id_key=?)")),
                "{plan:?}"
            );
            assert!(
                plan.iter()
                    .all(|step| !step.contains("TEMP B-TREE") && !step.contains("SCAN d")),
                "{plan:?}"
            );
            let markers: Vec<String> = connection
                .prepare(&format!("EXPLAIN QUERY PLAN {MARKERS}"))
                .unwrap()
                .query_map(params![1, 2, 0], |row| row.get(3))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert!(
                markers.iter().any(|step| step.contains("sort_key=?")),
                "{markers:?}"
            );
        }
    }
}
