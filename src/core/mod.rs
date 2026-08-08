//! Protocol-neutral database orchestration.
//!
//! This module owns routing and coordinates storage and SQL execution. It does
//! not depend on a network protocol.

mod control;
mod engine;
mod error;
mod lifecycle;
mod options;
mod session;
mod types;
pub(crate) mod worker;

pub(crate) use control::{
    CancelOnDrop, CancellationReason, OperationControl, wait_for_cancellation, wait_pending,
};
pub use control::{CancellationToken, RequestContext};
pub use engine::{Engine, EngineStatus, Statement};
pub use error::{EngineError, EngineErrorKind, EngineResult};
pub use lifecycle::{EngineState, ShutdownReport};
pub(crate) use lifecycle::{Lifecycle, OperationLease};
pub use options::{
    DEFAULT_CONNECTIONS_PER_SHARD, DEFAULT_MAX_RESULT_BYTES, DEFAULT_MAX_RESULT_ROWS,
    DEFAULT_QUEUE_CAPACITY_PER_SHARD, DEFAULT_REQUEST_TIMEOUT_MS, DEFAULT_SHUTDOWN_GRACE_MS,
    EngineOptions, MAX_CONNECTIONS_PER_SHARD, MAX_QUEUE_CAPACITY_PER_SHARD, MAX_REQUEST_TIMEOUT_MS,
    MAX_RESULT_BYTES, MAX_RESULT_ROWS, MAX_SHUTDOWN_GRACE_MS, ResultLimits,
};
pub use session::{Session, SessionId, SessionState};
pub use types::{
    Column, DataType, Decimal, ParseDecimalError, ResultSet, ResultSetShapeError, Row, Value,
};
pub(crate) use worker::BlockingPool;

use session::SessionInner;

use std::path::Path;

use crate::{sql, storage::Storage};

#[derive(Debug)]
pub struct Database {
    storage: Storage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Routed<T> {
    pub shard: u16,
    pub value: T,
}

impl Database {
    pub fn open(root: impl AsRef<Path>, requested_shards: u16) -> EngineResult<Self> {
        Ok(Self {
            storage: Storage::open(root, requested_shards)?,
        })
    }

    pub fn shard_count(&self) -> u16 {
        self.storage.shard_count()
    }

    pub fn shard_for_key(&self, key: &[u8]) -> u16 {
        let digest = blake3::hash(key);
        let prefix: [u8; 8] = digest.as_bytes()[..8]
            .try_into()
            .expect("BLAKE3 digest always contains eight bytes");
        (u64::from_le_bytes(prefix) % u64::from(self.shard_count())) as u16
    }

    pub fn execute_routed(
        &self,
        shard_key: &str,
        statement: &str,
        params: &[Value],
    ) -> EngineResult<Routed<usize>> {
        let shard = self.shard_for_key(shard_key.as_bytes());
        let connection = self.storage.open_shard(shard)?;
        let value = sql::execute(&connection, statement, params)?;
        Ok(Routed { shard, value })
    }

    pub fn query_routed(
        &self,
        shard_key: &str,
        statement: &str,
        params: &[Value],
    ) -> EngineResult<Routed<ResultSet>> {
        let shard = self.shard_for_key(shard_key.as_bytes());
        let connection = self.storage.open_shard(shard)?;
        let value = sql::query(&connection, statement, params)?;
        Ok(Routed { shard, value })
    }

    pub fn execute(
        &self,
        shard_key: &str,
        statement: &str,
        params: &[Value],
    ) -> EngineResult<usize> {
        Ok(self.execute_routed(shard_key, statement, params)?.value)
    }

    pub fn query(
        &self,
        shard_key: &str,
        statement: &str,
        params: &[Value],
    ) -> EngineResult<ResultSet> {
        Ok(self.query_routed(shard_key, statement, params)?.value)
    }

    pub fn broadcast(&self, statement: &str) -> EngineResult<Vec<u16>> {
        let mut completed = Vec::with_capacity(usize::from(self.shard_count()));
        for shard in 0..self.shard_count() {
            let connection = self.storage.open_shard(shard).map_err(|error| {
                error.context(format!("broadcast failed to open shard {shard}"))
            })?;
            sql::execute_batch(&connection, statement)
                .map_err(|error| error.context(format!("broadcast failed on shard {shard}")))?;
            completed.push(shard);
        }
        Ok(completed)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[test]
    fn routing_is_stable() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 4).unwrap();

        let first = database.shard_for_key(b"customer-42");
        assert_eq!(first, database.shard_for_key(b"customer-42"));
        assert!(first < 4);
    }

    #[test]
    fn routed_execute_and_query_report_the_selected_shard() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 4).unwrap();
        database
            .broadcast("CREATE TABLE widgets (id TEXT PRIMARY KEY, name TEXT NOT NULL);")
            .unwrap();

        let write = database
            .execute_routed(
                "widget-1",
                "INSERT INTO widgets (id, name) VALUES (?1, ?2)",
                &[Value::from("widget-1"), Value::from("First widget")],
            )
            .unwrap();
        let read = database
            .query_routed(
                "widget-1",
                "SELECT id, name FROM widgets WHERE id = ?1",
                &[Value::from("widget-1")],
            )
            .unwrap();

        let expected_shard = database.shard_for_key(b"widget-1");
        assert_eq!(
            write,
            Routed {
                shard: expected_shard,
                value: 1
            }
        );
        assert_eq!(read.shard, expected_shard);
        assert_eq!(
            read.value,
            ResultSet::new(
                vec![
                    Column::new("id", DataType::Unknown),
                    Column::new("name", DataType::Unknown),
                ],
                vec![Row::new(vec![
                    Value::from("widget-1"),
                    Value::from("First widget"),
                ])],
            )
            .unwrap()
        );
    }

    #[test]
    fn compatibility_execute_and_query_methods_keep_their_results() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 4).unwrap();
        assert_eq!(
            database
                .broadcast("CREATE TABLE widgets (id TEXT PRIMARY KEY, name TEXT NOT NULL);")
                .unwrap(),
            vec![0, 1, 2, 3]
        );

        assert_eq!(
            database
                .execute(
                    "widget-1",
                    "INSERT INTO widgets (id, name) VALUES (?1, ?2)",
                    &[Value::from("widget-1"), Value::from("First widget")],
                )
                .unwrap(),
            1
        );
        assert_eq!(
            database
                .query(
                    "widget-1",
                    "SELECT id, name FROM widgets WHERE id = ?1",
                    &[Value::from("widget-1")],
                )
                .unwrap(),
            ResultSet::new(
                vec![
                    Column::new("id", DataType::Unknown),
                    Column::new("name", DataType::Unknown),
                ],
                vec![Row::new(vec![
                    Value::from("widget-1"),
                    Value::from("First widget"),
                ])],
            )
            .unwrap()
        );
    }

    #[test]
    fn database_preserves_error_kinds_across_the_engine_boundary() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 4).unwrap();

        assert_eq!(
            database
                .query("widget-1", "SELECT * FROM missing_table", &[])
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidQuery
        );
        assert_eq!(
            Database::open(temp.path(), 1).unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
    }

    #[test]
    fn broadcast_failure_preserves_kind_and_can_recover_remaining_shards() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 4).unwrap();
        let shard_one = database.storage.open_shard(1).unwrap();
        sql::execute_batch(&shard_one, "CREATE TABLE recovery_marker (id INTEGER);").unwrap();

        let error = database
            .broadcast("CREATE TABLE recovery_marker (id INTEGER);")
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
        assert_eq!(error.to_string(), "broadcast failed on shard 1");
        assert!(error.source().is_some());

        let shard_zero = database.storage.open_shard(0).unwrap();
        let shard_two = database.storage.open_shard(2).unwrap();
        let table_exists = |connection: &rusqlite::Connection| {
            connection
                .query_row(
                    "SELECT EXISTS (
                        SELECT 1 FROM sqlite_schema
                        WHERE type = 'table' AND name = 'recovery_marker'
                    )",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        };
        assert!(table_exists(&shard_zero));
        assert!(!table_exists(&shard_two));

        assert_eq!(
            database
                .broadcast("CREATE TABLE IF NOT EXISTS recovery_marker (id INTEGER);")
                .unwrap(),
            [0, 1, 2, 3]
        );
        assert!(table_exists(&shard_two));
    }
}
