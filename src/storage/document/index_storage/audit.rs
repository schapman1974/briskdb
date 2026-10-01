//! Stream equality entries in source-record order with one borrowed row.

use crate::{
    core::EngineResult,
    document::{DocumentCollectionId, PreparedDocumentIndexEntries},
    sqlite_error,
};
use rusqlite::{Rows, Statement, fallible_streaming_iterator::FallibleStreamingIterator};

use super::{corrupt, entries::validate_equality_rows};

pub(in crate::storage::document) const STARTUP_EQUALITY_SQL: &str =
    "SELECT index_id, index_key, entry_checksum, entry_format_version, collection_id, id_key
     FROM briskdb_document_index_entries_v1
     INDEXED BY briskdb_document_index_entries_by_record_v1
     ORDER BY collection_id, id_key, index_id, index_key";

pub(in crate::storage::document) struct EqualityStartupAudit<'stmt> {
    rows: Rows<'stmt>,
}

impl<'stmt> EqualityStartupAudit<'stmt> {
    pub(in crate::storage::document) fn new(
        statement: &'stmt mut Statement<'_>,
    ) -> EngineResult<Self> {
        let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
        rows.advance().map_err(sqlite_error::storage)?;
        Ok(Self { rows })
    }

    /// Caller pins one explicit shard read transaction before either cursor is
    /// opened, visits every validated source record in primary order, and calls
    /// finish before releasing that snapshot. No stored key bytes are copied.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::storage::document) fn validate_record(
        &mut self,
        collection: DocumentCollectionId,
        shard: u16,
        id: &[u8],
        record: &[u8; 32],
        expected: Option<&PreparedDocumentIndexEntries>,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<()> {
        validate_equality_rows(collection, shard, id, record, expected, check, |visit| {
            while let Some(row) = self.rows.get() {
                let owner: i64 = row.get(4).map_err(sqlite_error::storage)?;
                let key = row
                    .get_ref(5)
                    .and_then(|value| value.as_blob().map_err(Into::into))
                    .map_err(sqlite_error::storage)?;
                match (owner, key).cmp(&(collection.get() as i64, id)) {
                    std::cmp::Ordering::Less => {
                        return Err(corrupt("equality audit entry has no source record"));
                    }
                    std::cmp::Ordering::Greater => break,
                    std::cmp::Ordering::Equal => {}
                }
                visit(row)?;
                self.rows.advance().map_err(sqlite_error::storage)?;
            }
            Ok(())
        })
    }

    pub(in crate::storage::document) fn finish(self) -> EngineResult<()> {
        if self.rows.get().is_some() {
            return Err(corrupt("equality audit entry has no source record"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equality_stream_uses_one_by_record_scan_without_temporary_sort() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection.execute_batch(super::super::ENTRIES_SQL).unwrap();
        connection
            .execute_batch(super::super::BY_RECORD_SQL)
            .unwrap();
        let plan = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {STARTUP_EQUALITY_SQL}"))
            .unwrap()
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(
            plan.iter()
                .any(|step| step.contains("SCAN") && step.contains(super::super::BY_RECORD)),
            "{plan:?}"
        );
        assert!(
            plan.iter()
                .all(|step| !step.contains("SEARCH") && !step.contains("TEMP B-TREE")),
            "{plan:?}"
        );
        let mut statement = connection.prepare(STARTUP_EQUALITY_SQL).unwrap();
        EqualityStartupAudit::new(&mut statement)
            .unwrap()
            .finish()
            .unwrap();
    }

    #[test]
    fn empty_equality_stream_keeps_source_snapshot_across_peer_commit() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("snapshot.sqlite");
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=OFF;
            CREATE TABLE briskdb_documents_v1(collection_id INTEGER, id_key BLOB, PRIMARY KEY(collection_id,id_key)) WITHOUT ROWID;").unwrap();
        connection.execute_batch(super::super::ENTRIES_SQL).unwrap();
        connection
            .execute_batch(super::super::BY_RECORD_SQL)
            .unwrap();
        let peer = rusqlite::Connection::open(&path).unwrap();
        peer.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
        {
            let _snapshot = connection.unchecked_transaction().unwrap();
            let mut statement = connection.prepare(STARTUP_EQUALITY_SQL).unwrap();
            let audit = EqualityStartupAudit::new(&mut statement).unwrap();
            peer.execute_batch("BEGIN;
                INSERT INTO briskdb_documents_v1 VALUES (1,zeroblob(9));
                INSERT INTO briskdb_document_index_entries_v1 VALUES (1,2,zeroblob(9),zeroblob(13),zeroblob(32),1);
                COMMIT;").unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT count(*) FROM briskdb_documents_v1", [], |row| row
                        .get::<_, i64>(
                        0
                    ))
                    .unwrap(),
                0
            );
            audit.finish().unwrap();
        }
        assert!(connection.is_autocommit());
        let mut statement = connection.prepare(STARTUP_EQUALITY_SQL).unwrap();
        assert_eq!(
            EqualityStartupAudit::new(&mut statement)
                .unwrap()
                .finish()
                .unwrap_err()
                .kind(),
            crate::core::EngineErrorKind::DataCorruption
        );
    }
}
