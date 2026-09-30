//! Merge the ordered primary tree with startup's record-primary-tree walk.
//! One borrowed SQLite row is retained, never a collection-sized key buffer.

use super::{
    corrupt,
    entries::{expected_ordered_count, validate_ordered_row},
};
use crate::{
    core::EngineResult,
    document::{DocumentCollectionId, PreparedDocumentIndexEntries},
    sqlite_error,
};
use rusqlite::{Rows, Statement, fallible_streaming_iterator::FallibleStreamingIterator};

pub(in crate::storage::document) const STARTUP_ORDERED_SQL: &str =
    "SELECT index_id, direction, sort_key, natural_order, entry_checksum, entry_format_version,
            collection_id, id_key
     FROM briskdb_document_ordered_entries_v1 NOT INDEXED
     ORDER BY collection_id, id_key, index_id, direction";

pub(in crate::storage::document) struct StartupAudit<'stmt> {
    rows: Option<Rows<'stmt>>,
}

impl<'stmt> StartupAudit<'stmt> {
    pub(in crate::storage::document) fn new(
        statement: Option<&'stmt mut Statement<'_>>,
    ) -> EngineResult<Self> {
        let mut rows = statement
            .map(|statement| statement.query([]))
            .transpose()
            .map_err(sqlite_error::storage)?;
        if let Some(rows) = &mut rows {
            rows.advance().map_err(sqlite_error::storage)?;
        }
        Ok(Self { rows })
    }

    /// Caller pins an explicit read transaction before creating either cursor,
    /// supplies each checksum-validated source record exactly once in
    /// (collection_id, id_key) primary order, and calls finish at end of shard.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::storage::document) fn validate_record(
        &mut self,
        collection: DocumentCollectionId,
        shard: u16,
        id: &[u8],
        record: &[u8; 32],
        natural: i64,
        expected: Option<&PreparedDocumentIndexEntries>,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<()> {
        check()?;
        if expected.is_some_and(|entries| entries.collection_id() != collection) {
            return Err(corrupt("ordered audit used a different collection"));
        }
        let mut count = 0;
        if let Some(rows) = &mut self.rows {
            while let Some(row) = rows.get() {
                check()?;
                let owner: i64 = row.get(6).map_err(sqlite_error::storage)?;
                let key = row
                    .get_ref(7)
                    .and_then(|value| value.as_blob().map_err(Into::into))
                    .map_err(sqlite_error::storage)?;
                match (owner, key).cmp(&(collection.get() as i64, id)) {
                    std::cmp::Ordering::Less => {
                        return Err(corrupt("ordered audit entry has no source record"));
                    }
                    std::cmp::Ordering::Greater => break,
                    std::cmp::Ordering::Equal => {}
                }
                count += 1;
                validate_ordered_row(row, collection, shard, id, record, expected, natural, count)?;
                rows.advance().map_err(sqlite_error::storage)?;
            }
        }
        if count != expected_ordered_count(expected) {
            return Err(corrupt("ordered document index coverage is incomplete"));
        }
        check()
    }

    pub(in crate::storage::document) fn finish(self) -> EngineResult<()> {
        if self.rows.as_ref().is_some_and(|rows| rows.get().is_some()) {
            return Err(corrupt("ordered audit entry has no source record"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_ordered_cursor_and_source_walk_keep_one_snapshot_across_peer_insert() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("snapshot.sqlite");
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE briskdb_documents_v1(collection_id INTEGER, id_key BLOB, PRIMARY KEY(collection_id,id_key)) WITHOUT ROWID;"
        ).unwrap();
        connection.execute_batch(super::super::TABLE_SQL).unwrap();
        connection.execute_batch(super::super::SCAN_SQL).unwrap();
        let peer = rusqlite::Connection::open(&path).unwrap();
        {
            let _snapshot = connection.unchecked_transaction().unwrap();
            let mut statement = connection.prepare(STARTUP_ORDERED_SQL).unwrap();
            let audit = StartupAudit::new(Some(&mut statement)).unwrap();
            peer.execute_batch(
                "BEGIN;
                 INSERT INTO briskdb_documents_v1 VALUES (1, zeroblob(9));
                 INSERT INTO briskdb_document_ordered_entries_v1 VALUES (1,zeroblob(9),2,0,NULL,1,zeroblob(32),1);
                 COMMIT;"
            ).unwrap();
            let source_count: i64 = connection
                .query_row("SELECT count(*) FROM briskdb_documents_v1", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(
                source_count, 0,
                "empty ordered cursor must not observe an older snapshot than the source walk"
            );
            audit.finish().unwrap();
        }
        assert!(
            connection.is_autocommit(),
            "audit must release its read transaction"
        );
        let source_count: i64 = connection
            .query_row("SELECT count(*) FROM briskdb_documents_v1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(source_count, 1);
        let mut statement = connection.prepare(STARTUP_ORDERED_SQL).unwrap();
        assert!(
            StartupAudit::new(Some(&mut statement))
                .unwrap()
                .finish()
                .is_err(),
            "a new snapshot must see the newly committed ordered entry"
        );
    }

    #[test]
    fn startup_ordered_audit_walks_primary_tree_without_seeks_or_temp_sort() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection.execute_batch(super::super::TABLE_SQL).unwrap();
        connection.execute_batch(super::super::SCAN_SQL).unwrap();
        let plan = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {STARTUP_ORDERED_SQL}"))
            .unwrap()
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(
            plan.iter()
                .any(|step| step.contains("SCAN briskdb_document_ordered_entries_v1")),
            "{plan:?}"
        );
        assert!(
            plan.iter().all(|step| !step.contains("SEARCH")
                && !step.contains("TEMP B-TREE")
                && !step.contains("briskdb_document_ordered_scan_v1")),
            "{plan:?}"
        );
        let mut statement = connection.prepare(STARTUP_ORDERED_SQL).unwrap();
        StartupAudit::new(Some(&mut statement))
            .unwrap()
            .finish()
            .unwrap();
        StartupAudit::new(None).unwrap().finish().unwrap();
    }
}
