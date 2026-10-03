use super::{
    Cell, Config, MAX_BYTES, MAX_ROWS, Result, Row, cloud, corrupt, create_base, file_index,
    invalid, is_nonce, limit, nonce, open_base,
    parquet::{self, Changes},
    schema::quote,
    storage_error,
    update::{Claim, Prepared, Receipt, operation_key, snapshot_conflict},
};
use crate::{EngineError, EngineErrorKind};
use bytes::Bytes;
use object_store::{PutMode, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Delta {
    pub id: String,
    pub hash: String,
    pub bytes: u64,
    pub sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Head {
    format: u32,
    database: String,
    table: usize,
    partition: u16,
    pub base: Option<String>,
    pub base_through: u64,
    pub sequence: u64,
    pub revision: u64,
    pub deltas: Vec<Delta>,
    pub receipts: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operations: Vec<Receipt>,
}

impl Head {
    pub fn new(config: &Config, table: usize, partition: u16, base: Option<String>) -> Self {
        Self {
            format: 1,
            database: config.database_id.clone(),
            table,
            partition,
            base,
            base_through: 0,
            sequence: 0,
            revision: 0,
            deltas: Vec::new(),
            receipts: Vec::new(),
            operations: Vec::new(),
        }
    }

    fn validate(&self, config: &Config, table: usize, partition: u16) -> Result<()> {
        if !matches!(self.format, 1 | 2)
            || self.format == 1 && !self.operations.is_empty()
            || self.database != config.database_id
            || self.table != table
            || self.partition != partition
            || self.base.as_ref().is_some_and(|v| !is_nonce(v))
            || self.base_through > self.sequence
            || self.revision < self.sequence
            || self.deltas.len() > config.max_pending_files
            || self.receipts.len() > 256
            || self.operations.len() > 256
            || self.sequence.checked_sub(self.base_through) != Some(self.deltas.len() as u64)
        {
            return Err(corrupt("invalid partition head identity/bounds"));
        }
        let mut seen = HashSet::new();
        let mut bytes = 0u64;
        for (i, delta) in self.deltas.iter().enumerate() {
            if !is_nonce(&delta.id)
                || !seen.insert(&delta.id)
                || delta.hash.len() != 64
                || !delta.hash.bytes().all(|b| b.is_ascii_hexdigit())
                || delta.bytes == 0
                || delta.bytes > 16 * 1024 * 1024
                || delta.sequence != self.base_through + i as u64 + 1
            {
                return Err(corrupt("invalid Parquet delta reference"));
            }
            bytes += delta.bytes;
        }
        if bytes > MAX_BYTES as u64 || self.receipts.iter().any(|id| !is_nonce(id)) {
            return Err(limit("pending deltas exceed bounded partition size"));
        }
        let mut operations = HashSet::new();
        for receipt in &self.operations {
            receipt.validate(config, table, partition)?;
            if !operations.insert(&receipt.claim.operation_id) {
                return Err(corrupt("duplicate operation receipt"));
            }
        }
        Ok(())
    }
}

pub(crate) fn head_key(config: &Config, table: usize, partition: u16) -> String {
    format!(
        "{}/tables/{table:04}/partitions/{partition:04}/head.json",
        config.namespace()
    )
}
fn delta_key(config: &Config, table: usize, partition: u16, id: &str) -> String {
    format!(
        "{}/tables/{table:04}/partitions/{partition:04}/deltas/{id}.parquet",
        config.namespace()
    )
}

#[derive(Clone)]
struct Snapshot {
    head: Head,
    version: UpdateVersion,
    latest: Changes,
    complete: bool,
    summaries: Option<Arc<Vec<Option<file_index::Summary>>>>,
}

/// Actual SQL-scan I/O for the last SQLite query or modifying statement.
/// Separate publication/rebase checks and automatic compaction are excluded.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReadStats {
    pub heads_read: u64,
    pub parquet_files_read: u64,
    pub parquet_files_skipped: u64,
    pub parquet_bytes_read: u64,
    pub index_files_opened: u64,
    pub index_fallback_files: u64,
}

#[derive(Clone)]
pub(crate) struct Located {
    pub table: usize,
    pub partition: u16,
    pub row: Row,
}

pub(crate) struct State {
    snapshots: HashMap<(usize, u16), Snapshot>,
    owner: Option<(usize, u16)>,
    changes: Changes,
    rows: HashMap<i64, Located>,
    rowids: HashMap<(usize, u16, Vec<u8>), i64>,
    next_rowid: i64,
    insert_only: bool,
    sql_scans: usize,
    snapshot_bytes: usize,
    locator_bytes: usize,
    started: Instant,
    pub stats: ReadStats,
    operation: Option<Claim>,
    point_guard: Option<PointGuard>,
    allow_compaction: bool,
}

struct PointGuard {
    predicates: Vec<(usize, Cell)>,
    original: Vec<Row>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            snapshots: HashMap::new(),
            owner: None,
            changes: Changes::new(),
            rows: HashMap::new(),
            rowids: HashMap::new(),
            next_rowid: 1,
            insert_only: false,
            sql_scans: 0,
            snapshot_bytes: 0,
            locator_bytes: 0,
            started: Instant::now(),
            stats: ReadStats::default(),
            operation: None,
            point_guard: None,
            allow_compaction: true,
        }
    }
}

impl State {
    pub fn expired(&self) -> bool {
        self.started.elapsed() > Duration::from_secs(120)
    }
}

pub(crate) struct Registry {
    pub root: PathBuf,
    pub config: Config,
    pub cloud: Arc<cloud::Cloud>,
    pub state: Mutex<State>,
    pub pruning: AtomicBool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResult {
    pub affected_rows: u64,
    pub commit_id: Option<String>,
    pub partition: Option<u16>,
    pub shard: Option<u16>,
    pub publication_retries: u32,
    #[serde(default)]
    pub deduplicated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionResult {
    pub table: String,
    pub partition: u16,
    pub merged_files: usize,
    pub rows: usize,
    pub published: bool,
}

impl Registry {
    pub(super) fn claim_update(&self, claim: &Claim) -> Result<()> {
        let bytes = Bytes::from(serde_json::to_vec(claim).map_err(storage_error)?);
        let key = operation_key(&self.config, &claim.operation_id, "request");
        if let Err(error) = self.cloud.put(&key, bytes.clone(), PutMode::Create) {
            // A lost acknowledgement of the immutable claim is safe to resolve.
            let existing = self.cloud.get_optional(&key, 4096)?;
            if existing.as_ref() == Some(&bytes) {
                return Ok(());
            }
            if existing.is_some() {
                return Err(EngineError::new(
                    EngineErrorKind::IdempotencyConflict,
                    "operation_id was already used for a different update",
                ));
            }
            return Err(storage_error(error));
        }
        Ok(())
    }

    fn archived_receipt(&self, claim: &Claim) -> Result<Option<Receipt>> {
        let key = operation_key(&self.config, &claim.operation_id, "result");
        let Some(bytes) = self.cloud.get_optional(&key, 8192)? else {
            return Ok(None);
        };
        let receipt: Receipt = serde_json::from_slice(&bytes).map_err(storage_error)?;
        receipt.validate(&self.config, claim.table, claim.partition)?;
        if receipt.claim != *claim {
            return Err(EngineError::new(
                EngineErrorKind::IdempotencyConflict,
                "operation_id was already used for a different update",
            ));
        }
        Ok(Some(receipt))
    }

    fn receipt_in_head(&self, head: &Head, claim: &Claim) -> Result<Option<Receipt>> {
        if let Some(receipt) = head
            .operations
            .iter()
            .find(|r| r.claim.operation_id == claim.operation_id)
        {
            if receipt.claim != *claim {
                return Err(corrupt("operation claim differs from committed receipt"));
            }
            return Ok(Some(receipt.clone()));
        }
        self.archived_receipt(claim)
    }

    pub(super) fn update_receipt(&self, claim: &Claim) -> Result<Option<Receipt>> {
        if let Some(receipt) = self.archived_receipt(claim)? {
            return Ok(Some(receipt));
        }
        let (head, _) = self.head(claim.table, claim.partition)?;
        // Recheck the archive after reading the head: a receipt can move between
        // our first archive GET and the head GET. It can never disappear from both.
        self.receipt_in_head(&head, claim)
    }

    fn archive_receipt(&self, receipt: &Receipt) -> Result<()> {
        let key = operation_key(&self.config, &receipt.claim.operation_id, "result");
        let bytes = Bytes::from(serde_json::to_vec(receipt).map_err(storage_error)?);
        if let Err(error) = self.cloud.put(&key, bytes.clone(), PutMode::Create) {
            if self.cloud.get_optional(&key, 8192)?.as_ref() != Some(&bytes) {
                return Err(storage_error(error));
            }
        }
        Ok(())
    }

    fn receipt_write(&self, receipt: Receipt) -> WriteResult {
        WriteResult {
            affected_rows: receipt.affected_rows,
            commit_id: Some(receipt.commit_id),
            partition: Some(receipt.claim.partition),
            shard: Some(receipt.claim.partition % self.config.shards),
            publication_retries: receipt.publication_retries,
            deduplicated: true,
        }
    }

    fn point_rows(
        &self,
        state: &mut State,
        table: usize,
        partition: u16,
        predicates: &[(usize, Cell)],
    ) -> Result<Vec<Row>> {
        Ok(self
            .read_rows(state, table, partition, predicates)?
            .into_iter()
            .filter(|row| {
                predicates
                    .iter()
                    .all(|(column, value)| row[*column] == *value)
            })
            .collect())
    }

    pub(super) fn begin_update(
        &self,
        prepared: &Prepared,
        rebase_disjoint: bool,
        allow_compaction: bool,
    ) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("overlay state poisoned"))?;
        state.owner = Some((prepared.claim.table, prepared.claim.partition));
        // Pin the head even if SQLite finds no matching row: a no-op result is
        // also recorded atomically, so a delayed retry cannot later change data.
        self.head_snapshot(&mut state, prepared.claim.table, prepared.claim.partition)?;
        if rebase_disjoint {
            let original = self.point_rows(
                &mut state,
                prepared.claim.table,
                prepared.claim.partition,
                &prepared.predicates,
            )?;
            state.point_guard = Some(PointGuard {
                predicates: prepared.predicates.clone(),
                original,
            });
        }
        state.operation = Some(prepared.claim.clone());
        state.allow_compaction = allow_compaction;
        Ok(())
    }

    #[cfg(feature = "experimental-duckdb-reader")]
    pub(super) fn duck_snapshot(
        &self,
        table: usize,
        partition: u16,
    ) -> Result<(Option<PathBuf>, Changes)> {
        let snapshot = self.snapshot(&mut State::default(), table, partition)?;
        let base = snapshot.head.base.map(|id| {
            super::base_directory(&self.root, &self.config, table, partition)
                .join(format!("base-{id}.sqlite"))
        });
        Ok((base, snapshot.latest))
    }

    pub fn reset(&self, insert: bool) -> Result<()> {
        *self
            .state
            .lock()
            .map_err(|_| corrupt("overlay request state poisoned"))? = State {
            insert_only: insert,
            ..Default::default()
        };
        Ok(())
    }

    fn head(&self, table: usize, partition: u16) -> Result<(Head, UpdateVersion)> {
        let (bytes, version) = self
            .cloud
            .get(&head_key(&self.config, table, partition), 1024 * 1024)?;
        let head: Head = serde_json::from_slice(&bytes).map_err(storage_error)?;
        head.validate(&self.config, table, partition)?;
        Ok((head, version))
    }

    fn head_snapshot(&self, state: &mut State, table: usize, partition: u16) -> Result<Snapshot> {
        if !state.snapshots.contains_key(&(table, partition)) {
            let (head, version) = self.head(table, partition)?;
            state.stats.heads_read += 1;
            state.snapshots.insert(
                (table, partition),
                Snapshot {
                    complete: head.deltas.is_empty(),
                    head,
                    version,
                    latest: Changes::new(),
                    summaries: None,
                },
            );
        }
        Ok(state.snapshots[&(table, partition)].clone())
    }

    fn read_snapshot(
        &self,
        state: &mut State,
        table: usize,
        partition: u16,
        predicates: &[(usize, Cell)],
    ) -> Result<Snapshot> {
        let mut snapshot = self.head_snapshot(state, table, partition)?;
        if snapshot.complete {
            return Ok(snapshot);
        }
        if !self.pruning.load(Ordering::Relaxed)
            || !file_index::applicable(&self.config.tables[table], predicates)
        {
            return self.snapshot(state, table, partition);
        }
        if snapshot.summaries.is_none() {
            let summaries = if snapshot.head.deltas.iter().any(|d| d.index_hash.is_some()) {
                state.stats.index_files_opened += 1;
                file_index::load(
                    &self.root,
                    &self.config,
                    table,
                    partition,
                    &snapshot.head.deltas,
                )
            } else {
                None
            };
            snapshot.summaries = Some(Arc::new(
                summaries.unwrap_or_else(|| vec![None; snapshot.head.deltas.len()]),
            ));
            state
                .snapshots
                .get_mut(&(table, partition))
                .unwrap()
                .summaries = snapshot.summaries.clone();
        }
        let summaries = snapshot.summaries.as_ref().unwrap();
        let selected = snapshot
            .head
            .deltas
            .iter()
            .zip(summaries.iter())
            .filter_map(|(delta, summary)| match summary {
                Some(s) if s.excludes(&self.config.tables[table], predicates) => {
                    state.stats.parquet_files_skipped += 1;
                    None
                }
                None => {
                    state.stats.index_fallback_files += 1;
                    Some(delta.clone())
                }
                _ => Some(delta.clone()),
            })
            .collect::<Vec<_>>();
        if selected.len() == snapshot.head.deltas.len() {
            return self.snapshot(state, table, partition);
        }
        // A pruned result is NOT a complete partition snapshot. Joins can probe
        // this same pinned head using a different key later in the statement.
        snapshot.latest = self.load_deltas(state, table, partition, &selected)?;
        Ok(snapshot)
    }

    fn load_deltas(
        &self,
        state: &mut State,
        table: usize,
        partition: u16,
        deltas: &[Delta],
    ) -> Result<Changes> {
        let mut latest = Changes::new();
        for delta in deltas {
            let (bytes, _) = self.cloud.get(
                &delta_key(&self.config, table, partition, &delta.id),
                delta.bytes,
            )?;
            state.stats.parquet_files_read += 1;
            state.stats.parquet_bytes_read += bytes.len() as u64;
            if bytes.len() as u64 != delta.bytes
                || blake3::hash(&bytes).to_hex().as_str() != delta.hash
            {
                return Err(corrupt("Parquet length/checksum mismatch"));
            }
            let changes = parquet::decode(&self.config.tables[table], bytes)?;
            for (key, row) in changes {
                if let Some(row) = &row {
                    if self
                        .config
                        .partition(&row[self.config.tables[table].routing_column()])?
                        != partition
                    {
                        return Err(corrupt("Parquet row is in the wrong partition"));
                    }
                }
                latest.insert(key, row);
            }
            let size = latest
                .iter()
                .map(|(key, row)| {
                    key.len()
                        + row
                            .as_ref()
                            .map_or(0, |row| row.iter().map(Cell::size).sum::<usize>())
                })
                .sum::<usize>();
            if latest.len() > MAX_ROWS || state.snapshot_bytes + size > MAX_BYTES {
                return Err(limit("request pending snapshots exceed memory limit"));
            }
        }
        Ok(latest)
    }

    fn snapshot(&self, state: &mut State, table: usize, partition: u16) -> Result<Snapshot> {
        let mut snapshot = self.head_snapshot(state, table, partition)?;
        if !snapshot.complete {
            snapshot.latest = self.load_deltas(state, table, partition, &snapshot.head.deltas)?;
            state.snapshot_bytes += snapshot
                .latest
                .iter()
                .map(|(key, row)| {
                    key.len()
                        + row
                            .as_ref()
                            .map_or(0, |row| row.iter().map(Cell::size).sum::<usize>())
                })
                .sum::<usize>();
            snapshot.complete = true;
            state.snapshots.insert((table, partition), snapshot.clone());
        }
        Ok(snapshot)
    }

    fn read_rows(
        &self,
        state: &mut State,
        table: usize,
        partition: u16,
        predicates: &[(usize, Cell)],
    ) -> Result<Vec<Row>> {
        self.cloud.check_deadline()?;
        if state.started.elapsed() > Duration::from_secs(120) {
            return Err(limit("overlay request exceeded 120-second deadline"));
        }
        let schema = &self.config.tables[table];
        let snapshot = self.read_snapshot(state, table, partition, predicates)?;
        let mut latest = snapshot.latest;
        if state.owner == Some((table, partition)) {
            latest.extend(state.changes.clone());
        }
        let mut rows = Vec::new();
        let mut bytes = 0;
        if let Some(base) = snapshot.head.base {
            let connection = open_base(&self.root, &self.config, table, partition, &base)?;
            let mut sql = format!(
                "SELECT {} FROM {}",
                schema.select_columns(),
                quote(&schema.name)
            );
            if !predicates.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(
                    &predicates
                        .iter()
                        .map(|(column, _)| format!("{} = ?", quote(&schema.columns[*column].name)))
                        .collect::<Vec<_>>()
                        .join(" AND "),
                );
            }
            let mut statement = connection.prepare(&sql).map_err(storage_error)?;
            let mut cursor = statement
                .query(rusqlite::params_from_iter(
                    predicates.iter().map(|(_, v)| v),
                ))
                .map_err(storage_error)?;
            while let Some(row) = cursor.next().map_err(storage_error)? {
                let row = (0..schema.columns.len())
                    .map(|i| Cell::from_sql(row.get_ref(i)?))
                    .collect::<rusqlite::Result<Row>>()
                    .map_err(storage_error)?;
                if !latest.contains_key(&schema.key(&row)) {
                    bytes += row.iter().map(Cell::size).sum::<usize>();
                    rows.push(row);
                    if rows.len() > MAX_ROWS || bytes > MAX_BYTES {
                        return Err(limit("partition scan exceeds memory limit"));
                    }
                }
            }
        }
        // SQLite itself evaluates the outer predicates/affinity/collations on
        // pending rows. Never discard an UPDATE merely because its old base
        // value matched (or did not match) a pushed-down predicate.
        for row in latest.into_values().flatten() {
            bytes += row.iter().map(Cell::size).sum::<usize>();
            rows.push(row);
        }
        if rows.len() > MAX_ROWS || bytes > MAX_BYTES {
            return Err(limit("partition scan exceeds memory limit"));
        }
        Ok(rows)
    }

    pub fn scan(&self, table: usize, predicates: &[(usize, Cell)]) -> Result<Vec<(i64, Row)>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("overlay request state poisoned"))?;
        state.sql_scans += 1;
        let schema = &self.config.tables[table];
        let routing = schema.routing_column();
        let exact = predicates.iter().find(|(i, value)| {
            *i == routing
                && matches!(
                    (schema.columns[routing].kind, value),
                    (super::ColumnType::Integer, Cell::Integer(_))
                        | (super::ColumnType::Text, Cell::Text(_))
                        | (super::ColumnType::Blob, Cell::Blob(_))
                )
        });
        let partitions = if let Some((_, value)) = exact {
            vec![self.config.partition(value)?]
        } else {
            (0..self.config.partitions).collect()
        };
        let mut output = Vec::new();
        let mut bytes = 0;
        for partition in partitions {
            for row in self.read_rows(&mut state, table, partition, predicates)? {
                bytes += row.iter().map(Cell::size).sum::<usize>();
                if output.len() >= MAX_ROWS || bytes > MAX_BYTES {
                    return Err(limit("SQL scan exceeds memory limit"));
                }
                let id = Self::locate(&mut state, table, partition, schema.key(&row), row.clone())?;
                output.push((id, row));
            }
        }
        Ok(output)
    }

    fn locate(
        state: &mut State,
        table: usize,
        partition: u16,
        key: Vec<u8>,
        row: Row,
    ) -> Result<i64> {
        let existing = state.rowids.get(&(table, partition, key.clone())).copied();
        let previous = existing
            .and_then(|id| state.rows.get(&id))
            .map_or(0, |r| r.row.iter().map(Cell::size).sum::<usize>());
        let bytes = state.locator_bytes - previous
            + row.iter().map(Cell::size).sum::<usize>()
            + if existing.is_none() { key.len() } else { 0 };
        if bytes > MAX_BYTES || (existing.is_none() && state.rows.len() >= MAX_ROWS) {
            return Err(limit("request row locators exceed memory limit"));
        }
        state.locator_bytes = bytes;
        let id = *state
            .rowids
            .entry((table, partition, key))
            .or_insert_with(|| {
                let id = state.next_rowid;
                state.next_rowid += 1;
                id
            });
        state.rows.insert(
            id,
            Located {
                table,
                partition,
                row,
            },
        );
        Ok(id)
    }

    fn owner(state: &mut State, table: usize, partition: u16) -> Result<()> {
        if state.owner.is_some_and(|v| v != (table, partition)) {
            return Err(EngineError::new(
                EngineErrorKind::Unsupported,
                "one modifying statement cannot cross table/key partitions; no changes were committed",
            ));
        }
        state.owner = Some((table, partition));
        Ok(())
    }

    fn exists(
        &self,
        state: &mut State,
        table: usize,
        partition: u16,
        row: &[Cell],
    ) -> Result<bool> {
        let schema = &self.config.tables[table];
        let key = schema.key(row);
        let predicates = schema
            .primary_key
            .iter()
            .map(|name| {
                let i = schema.columns.iter().position(|v| v.name == *name).unwrap();
                (i, row[i].clone())
            })
            .collect::<Vec<_>>();
        Ok(self
            .read_rows(state, table, partition, &predicates)?
            .iter()
            .any(|r| schema.key(r) == key))
    }

    pub fn insert(&self, table: usize, row: Row) -> Result<i64> {
        let schema = &self.config.tables[table];
        schema.check_row(&row)?;
        let partition = self.config.partition(&row[schema.routing_column()])?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("overlay request state poisoned"))?;
        Self::owner(&mut state, table, partition)?;
        if self.exists(&mut state, table, partition, &row)? {
            return Err(EngineError::new(
                EngineErrorKind::UniqueViolation,
                "duplicate primary key",
            ));
        }
        let key = schema.key(&row);
        state.changes.insert(key.clone(), Some(row.clone()));
        Self::locate(&mut state, table, partition, key, row)
    }

    pub fn delete(&self, table: usize, rowid: i64) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("overlay request state poisoned"))?;
        let located = state
            .rows
            .get(&rowid)
            .cloned()
            .ok_or_else(|| invalid("unknown row locator"))?;
        if located.table != table {
            return Err(invalid("row locator belongs to another table"));
        }
        Self::owner(&mut state, table, located.partition)?;
        state
            .changes
            .insert(self.config.tables[table].key(&located.row), None);
        state.insert_only = false;
        Ok(())
    }

    pub fn update(&self, table: usize, rowid: i64, row: Row) -> Result<()> {
        let schema = &self.config.tables[table];
        schema.check_row(&row)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("overlay request state poisoned"))?;
        let old = state
            .rows
            .get(&rowid)
            .cloned()
            .ok_or_else(|| invalid("unknown row locator"))?;
        let partition = self.config.partition(&row[schema.routing_column()])?;
        if old.table != table || partition != old.partition {
            return Err(invalid("UPDATE cannot move a record between partitions"));
        }
        Self::owner(&mut state, table, partition)?;
        let old_key = schema.key(&old.row);
        let new_key = schema.key(&row);
        if old_key != new_key && self.exists(&mut state, table, partition, &row)? {
            return Err(EngineError::new(
                EngineErrorKind::UniqueViolation,
                "duplicate primary key",
            ));
        }
        state.changes.insert(old_key, None);
        state.changes.insert(new_key, Some(row));
        state.insert_only = false;
        Ok(())
    }

    pub fn commit(&self, affected: u64) -> Result<WriteResult> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("overlay request state poisoned"))?;
        let Some((table, partition)) = state.owner else {
            return Ok(WriteResult {
                affected_rows: affected,
                commit_id: None,
                partition: None,
                shard: None,
                publication_retries: 0,
                deduplicated: false,
            });
        };
        let original = self.head_snapshot(&mut state, table, partition)?;
        let changes = state.changes.clone();
        let id = nonce()?;
        let bytes = if changes.is_empty() {
            Bytes::new()
        } else {
            parquet::encode(&self.config.tables[table], &changes)?
        };
        let hash = blake3::hash(&bytes).to_hex().to_string();
        let size = bytes.len() as u64;
        let key = delta_key(&self.config, table, partition, &id);
        // Immutable payload creation is safe to check after an ambiguous reply;
        // it is NOT a committed write until the head references it.
        if !bytes.is_empty() {
            if let Err(error) = self.cloud.put(&key, bytes.clone(), PutMode::Create) {
                let recovered = self.cloud.get(&key, size).is_ok_and(|(v, _)| v == bytes);
                if !recovered {
                    return Err(storage_error(error));
                }
            }
        }
        // This is advisory metadata. A failed index write must not reject an
        // otherwise durable application write or permit an unsafe skip.
        let index_hash = if !changes.is_empty() && self.pruning.load(Ordering::Relaxed) {
            let hash = file_index::publish(
                &self.root,
                &self.config,
                table,
                partition,
                &id,
                &hash,
                &changes,
            );
            if hash.is_none() {
                eprintln!("BriskDB overlay index unavailable; publishing without pruning metadata");
            }
            hash
        } else {
            None
        };
        let mut head = original.head.clone();
        let mut version = original.version;
        let mut retries = 0u32;
        let started = Instant::now();
        let mut last_log = started;
        loop {
            self.cloud.check_deadline()?;
            if let Some(claim) = &state.operation {
                if let Some(receipt) = self.receipt_in_head(&head, claim)? {
                    return Ok(self.receipt_write(receipt));
                }
            }
            if !changes.is_empty()
                && (head.deltas.len() >= self.config.compact_after_files
                    || head.deltas.iter().map(|v| v.bytes).sum::<u64>() + size > MAX_BYTES as u64)
            {
                if !state.allow_compaction {
                    return Err(EngineError::new(
                        EngineErrorKind::Busy,
                        "partition needs compaction; point update was not committed",
                    ));
                }
                self.compact(table, partition)?;
                let current = self.head(table, partition)?;
                head = current.0;
                version = current.1;
                self.validate_rebase(&state, table, partition, &original.head, &head, &changes)?;
            }
            if !changes.is_empty() && head.deltas.len() >= self.config.max_pending_files {
                return Err(limit("pending-file limit reached; compact before writing"));
            }
            let mut next = head.clone();
            if !changes.is_empty() {
                next.sequence = next
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| corrupt("sequence overflow"))?;
            }
            next.revision = next
                .revision
                .checked_add(1)
                .ok_or_else(|| corrupt("revision overflow"))?;
            if !changes.is_empty() {
                next.deltas.push(Delta {
                    id: id.clone(),
                    hash: hash.clone(),
                    bytes: size,
                    sequence: next.sequence,
                    index_hash: index_hash.clone(),
                });
                next.receipts.push(id.clone());
                if next.receipts.len() > 256 {
                    next.receipts.remove(0);
                }
            }
            if let Some(claim) = &state.operation {
                // Before removing a receipt from the atomic head, archive its
                // proven committed result durably. No time/size expiry can turn
                // a committed operation into a new operation on redelivery.
                if next.operations.len() == 256 {
                    self.archive_receipt(&next.operations[0])?;
                    next.operations.remove(0);
                }
                next.format = 2; // Old readers fail closed on the new receipt format.
                next.operations.push(Receipt {
                    claim: claim.clone(),
                    commit_id: id.clone(),
                    affected_rows: affected,
                    publication_retries: retries,
                });
            }
            next.validate(&self.config, table, partition)?;
            let result = self.cloud.put(
                &head_key(&self.config, table, partition),
                Bytes::from(serde_json::to_vec(&next).map_err(storage_error)?),
                PutMode::Update(version.clone()),
            );
            match result {
                Ok(()) => break,
                Err(error) => {
                    let current = self.head(table, partition);
                    if let Some(claim) = &state.operation {
                        if let Ok((head, _)) = &current {
                            if let Some(receipt) = self.receipt_in_head(head, claim)? {
                                return Ok(self.receipt_write(receipt));
                            }
                        }
                    }
                    if current
                        .as_ref()
                        .is_ok_and(|(h, _)| h.receipts.contains(&id))
                    {
                        break;
                    }
                    if !cloud::conflict(&error) {
                        if matches!(
                            error,
                            object_store::Error::PermissionDenied { .. }
                                | object_store::Error::Unauthenticated { .. }
                        ) {
                            return Err(storage_error(error));
                        }
                        return Err(EngineError::new(
                            EngineErrorKind::StorageUnavailable,
                            format!(
                                "commit outcome unknown for {id}; do not blindly replay SQL: {error}"
                            ),
                        ));
                    }
                    let (current, token) = current?;
                    // An SDK may retry a server error and receive a subsequent
                    // precondition failure after the first request committed.
                    // Absence proves failure only while that attempt's receipt
                    // would still fit in the bounded recent-commit window.
                    if state.operation.is_none()
                        && current.sequence.saturating_sub(head.sequence) > 256
                    {
                        return Err(EngineError::new(
                            EngineErrorKind::StorageUnavailable,
                            format!(
                                "commit outcome unknown for {id}; receipt window advanced; do not blindly replay SQL"
                            ),
                        ));
                    }
                    self.validate_rebase(
                        &state,
                        table,
                        partition,
                        &original.head,
                        &current,
                        &changes,
                    )?;
                    if started.elapsed() >= Duration::from_millis(self.config.write_retry_ms) {
                        eprintln!(
                            "BriskDB overlay publication retry budget exhausted: retries={retries}"
                        );
                        return Err(EngineError::new(
                            EngineErrorKind::Busy,
                            "conditional publication retry budget exhausted; write was not committed",
                        ));
                    }
                    retries += 1;
                    if last_log.elapsed() >= Duration::from_secs(5) {
                        eprintln!(
                            "BriskDB overlay waiting to publish: retries={retries} elapsed_ms={}",
                            started.elapsed().as_millis()
                        );
                        last_log = Instant::now();
                    }
                    let mut jitter = [0u8; 1];
                    getrandom::fill(&mut jitter).map_err(|e| invalid(e.to_string()))?;
                    self.cloud.pause(Duration::from_millis(
                        1 + u64::from(jitter[0]) % (5 * u64::from(retries.min(20))),
                    ))?;
                    head = current;
                    version = token;
                }
            }
        }
        Ok(WriteResult {
            affected_rows: affected,
            commit_id: Some(id),
            partition: Some(partition),
            shard: Some(partition % self.config.shards),
            publication_retries: retries,
            deduplicated: false,
        })
    }

    fn validate_rebase(
        &self,
        state: &State,
        table: usize,
        partition: u16,
        original: &Head,
        current: &Head,
        changes: &Changes,
    ) -> Result<()> {
        if current.sequence == original.sequence {
            return Ok(());
        } // Compaction only.
        if let Some(guard) = &state.point_guard {
            let fresh =
                self.point_rows(&mut State::default(), table, partition, &guard.predicates)?;
            if fresh == guard.original {
                return Ok(());
            }
        }
        if state.insert_only && state.sql_scans == 0 && changes.values().all(Option::is_some) {
            let mut fresh = State::default();
            for row in changes.values().flatten() {
                if self.exists(&mut fresh, table, partition, row)? {
                    return Err(EngineError::new(
                        EngineErrorKind::UniqueViolation,
                        "concurrent insert claimed the primary key",
                    ));
                }
            }
            Ok(())
        } else {
            Err(snapshot_conflict())
        }
    }

    pub fn compact(&self, table: usize, partition: u16) -> Result<CompactionResult> {
        let mut state = State::default();
        let snapshot = self.snapshot(&mut state, table, partition)?;
        let mut report = CompactionResult {
            table: self.config.tables[table].name.clone(),
            partition,
            merged_files: snapshot.head.deltas.len(),
            rows: 0,
            published: false,
        };
        if snapshot.head.deltas.is_empty() {
            return Ok(report);
        }
        let rows = self.read_rows(&mut state, table, partition, &[])?;
        let base = create_base(&self.root, &self.config, table, partition, &rows)?;
        report.rows = rows.len();
        let mut current = snapshot.head.clone();
        let mut version = snapshot.version;
        for _ in 0..8 {
            if current.base != snapshot.head.base
                || current.base_through != snapshot.head.base_through
            {
                // Another compactor published. This private candidate is an
                // orphan, never an authority; leave it for offline reclamation.
                return Ok(report);
            }
            if current.deltas.len() < snapshot.head.deltas.len()
                || !current
                    .deltas
                    .iter()
                    .zip(&snapshot.head.deltas)
                    .all(|(a, b)| a.id == b.id && a.hash == b.hash)
            {
                return Err(corrupt(
                    "compaction history is not an append-only extension",
                ));
            }
            let mut next = current.clone();
            next.base = Some(base.clone());
            next.base_through = snapshot.head.sequence;
            next.deltas.drain(..snapshot.head.deltas.len());
            next.revision = next
                .revision
                .checked_add(1)
                .ok_or_else(|| corrupt("revision overflow"))?;
            match self.cloud.put(
                &head_key(&self.config, table, partition),
                Bytes::from(serde_json::to_vec(&next).map_err(storage_error)?),
                PutMode::Update(version.clone()),
            ) {
                Ok(()) => {
                    report.published = true;
                    return Ok(report);
                }
                Err(error) => {
                    let refreshed = self.head(table, partition)?;
                    if refreshed.0.base.as_ref() == Some(&base) {
                        report.published = true;
                        return Ok(report);
                    }
                    if !cloud::conflict(&error) {
                        return Err(storage_error(error));
                    }
                    current = refreshed.0;
                    version = refreshed.1;
                }
            }
        }
        Ok(report)
    }
}
