//! Bounded, deterministic point updates. Operation receipts are part of the
//! same conditional head publication as the data, never an acknowledgement of
//! an uploaded-but-unpublished Parquet file.
use super::*;
use std::{
    error::Error,
    fmt,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetryOptions {
    pub timeout_ms: u64,
    pub max_retries: u32,
    pub backoff_ms: u64,
    pub max_backoff_ms: u64,
    /// Rebase only when the complete target row is unchanged. No range queries
    /// or user SQL enter this path.
    pub rebase_disjoint: bool,
    /// Foreground updates do not unexpectedly compact a large partition.
    pub allow_compaction: bool,
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self {
            timeout_ms: 1_000,
            max_retries: 2,
            backoff_ms: 20,
            max_backoff_ms: 100,
            rebase_disjoint: true,
            allow_compaction: false,
        }
    }
}

impl RetryOptions {
    pub(super) fn validate(&self) -> Result<()> {
        if !(1..=120_000).contains(&self.timeout_ms)
            || self.max_retries > 32
            || self.max_backoff_ms > 5_000
            || self.backoff_ms > self.max_backoff_ms
        {
            return Err(invalid("invalid update retry options"));
        }
        Ok(())
    }
}

/// An explicit deterministic update of ONE complete primary key. Reuse the
/// same operation_id and identical request after a lost response or redelivery.
/// Use `expected` for an application version/old-value condition. A failed
/// condition is a terminal result, not a retryable infrastructure conflict.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateRequest {
    pub operation_id: String,
    pub table: String,
    pub key: BTreeMap<String, Cell>,
    #[serde(default)]
    pub set: BTreeMap<String, Cell>,
    #[serde(default)]
    pub increment: BTreeMap<String, Cell>,
    #[serde(default)]
    pub expected: BTreeMap<String, Cell>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateResult {
    /// `committed` or `condition_not_met`. Neither means queued.
    pub status: String,
    pub operation_id: String,
    pub affected_rows: u64,
    pub commit_id: String,
    pub partition: u16,
    pub shard: u16,
    pub statement_retries: u32,
    pub publication_retries: u32,
    pub deduplicated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTarget {
    pub database_id: String,
    pub table: String,
    pub partition: u16,
    pub message_group_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Claim {
    pub format: u32,
    pub operation_id: String,
    pub digest: String,
    pub table: usize,
    pub partition: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Receipt {
    pub claim: Claim,
    pub commit_id: String,
    pub affected_rows: u64,
    pub publication_retries: u32,
}

impl Receipt {
    pub fn result(&self, config: &Config, deduplicated: bool) -> UpdateResult {
        UpdateResult {
            status: if self.affected_rows == 0 {
                "condition_not_met"
            } else {
                "committed"
            }
            .into(),
            operation_id: self.claim.operation_id.clone(),
            affected_rows: self.affected_rows,
            commit_id: self.commit_id.clone(),
            partition: self.claim.partition,
            shard: self.claim.partition % config.shards,
            statement_retries: 0,
            publication_retries: self.publication_retries,
            deduplicated,
        }
    }

    pub fn validate(&self, config: &Config, table: usize, partition: u16) -> Result<()> {
        if self.claim.format != 1
            || !is_nonce(&self.claim.operation_id)
            || !is_nonce(&self.commit_id)
            || self.claim.digest.len() != 64
            || !self.claim.digest.bytes().all(|b| b.is_ascii_hexdigit())
            || self.claim.table != table
            || self.claim.partition != partition
            || table >= config.tables.len()
            || partition >= config.partitions
            || self.affected_rows > 1
        {
            return Err(corrupt("invalid point-update receipt"));
        }
        Ok(())
    }
}

pub(super) fn operation_key(config: &Config, id: &str, name: &str) -> String {
    format!("{}/operations/{id}/{name}.json", config.namespace())
}

/// Typed marker: only this *definite no-commit* conflict can replay an update.
#[derive(Debug)]
pub(super) struct SnapshotConflict;
impl fmt::Display for SnapshotConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("partition changed since SQL snapshot; nothing committed, retry the entire statement explicitly")
    }
}
impl Error for SnapshotConflict {}

pub(super) fn snapshot_conflict() -> EngineError {
    EngineError::from_source(
        EngineErrorKind::Busy,
        SnapshotConflict.to_string(),
        SnapshotConflict,
    )
}

pub(super) struct Prepared {
    pub claim: Claim,
    pub sql: String,
    pub params: Vec<Cell>,
    pub predicates: Vec<(usize, Cell)>,
}

fn check_cell(column: &Column, value: &Cell) -> Result<()> {
    let valid = match (column.kind, value) {
        (_, Cell::Null) => column.nullable,
        (ColumnType::Integer, Cell::Integer(_))
        | (ColumnType::Text, Cell::Text(_))
        | (ColumnType::Blob, Cell::Blob(_)) => true,
        (ColumnType::Real, Cell::Real(v)) => v.is_finite(),
        _ => false,
    };
    if !valid || value.size() > 256 * 1024 {
        return Err(invalid("invalid typed update value"));
    }
    Ok(())
}

impl UpdateRequest {
    pub(super) fn prepare(&self, config: &Config) -> Result<Prepared> {
        if !is_nonce(&self.operation_id) {
            return Err(invalid(
                "operation_id must be 32 lowercase hexadecimal characters",
            ));
        }
        let table = config
            .tables
            .iter()
            .position(|t| t.name == self.table)
            .ok_or_else(|| invalid("unknown update table"))?;
        let schema = &config.tables[table];
        if self.key.len() != schema.primary_key.len()
            || schema.primary_key.iter().any(|k| !self.key.contains_key(k))
            || self.set.is_empty() && self.increment.is_empty()
        {
            return Err(invalid(
                "update requires a complete primary key and a change",
            ));
        }
        let column = |name: &str| {
            schema
                .columns
                .iter()
                .position(|c| c.name == name)
                .ok_or_else(|| invalid("unknown update column"))
        };
        for (name, value) in self
            .key
            .iter()
            .chain(&self.set)
            .chain(&self.expected)
            .chain(&self.increment)
        {
            check_cell(&schema.columns[column(name)?], value)?;
        }
        let mut params = Vec::new();
        let mut assignments = Vec::new();
        for (name, value) in &self.set {
            if schema.primary_key.contains(name) || self.increment.contains_key(name) {
                return Err(invalid(
                    "point updates cannot change primary keys or set and increment one column",
                ));
            }
            assignments.push(format!("{}=?", schema::quote(name)));
            params.push(value.clone());
        }
        for (name, value) in &self.increment {
            if schema.primary_key.contains(name)
                || !matches!(value, Cell::Integer(_) | Cell::Real(_))
            {
                return Err(invalid("increments require a non-key numeric column"));
            }
            assignments.push(format!("{}={}+?", schema::quote(name), schema::quote(name)));
            params.push(value.clone());
        }
        let mut predicates = Vec::new();
        let mut conditions = Vec::new();
        for (name, value) in &self.key {
            predicates.push((column(name)?, value.clone()));
            conditions.push(format!("{}=?", schema::quote(name)));
            params.push(value.clone());
        }
        for (name, value) in &self.expected {
            conditions.push(format!("{} IS ?", schema::quote(name)));
            params.push(value.clone());
        }
        let encoded = serde_json::to_vec(self).map_err(storage_error)?;
        if encoded.len() > 256 * 1024 {
            return Err(limit("update request exceeds 256 KiB"));
        }
        Ok(Prepared {
            claim: Claim {
                format: 1,
                operation_id: self.operation_id.clone(),
                digest: blake3::hash(&encoded).to_hex().to_string(),
                table,
                partition: config.partition(&self.key[&schema.shard_key])?,
            },
            sql: format!(
                "UPDATE {} SET {} WHERE {}",
                schema::quote(&schema.name),
                assignments.join(","),
                conditions.join(" AND ")
            ),
            params,
            predicates,
        })
    }
}

impl Database {
    /// Validate without publishing anything. Useful for a durable queue adapter.
    pub fn update_target(&self, request: &UpdateRequest) -> Result<UpdateTarget> {
        self.require_writable()?;
        let prepared = request.prepare(self.config())?;
        Ok(UpdateTarget {
            database_id: self.config().database_id.clone(),
            table: request.table.clone(),
            partition: prepared.claim.partition,
            message_group_id: format!(
                "{}:{}:{}",
                self.config().database_id,
                prepared.claim.table,
                prepared.claim.partition
            ),
        })
    }

    /// The budget covers storage calls and all retries after this method starts,
    /// not opening the database or uninterruptible operating-system filesystem I/O.
    /// A deadline during publication is UNKNOWN until its operation ID is reconciled.
    pub fn update(
        &mut self,
        request: &UpdateRequest,
        options: RetryOptions,
    ) -> Result<UpdateResult> {
        self.require_writable()?;
        options.validate()?;
        let prepared = request.prepare(self.config())?;
        let deadline = Instant::now() + Duration::from_millis(options.timeout_ms);
        self.registry.cloud.set_deadline(Some(deadline));
        let result = self.update_inner(&prepared, options, deadline);
        self.registry.cloud.set_deadline(None);
        // Clear every request-local snapshot even after cancellation/conflict.
        self.finish_statement()?;
        result
    }

    fn update_inner(
        &mut self,
        prepared: &Prepared,
        options: RetryOptions,
        deadline: Instant,
    ) -> Result<UpdateResult> {
        self.registry.claim_update(&prepared.claim)?;
        for attempt in 0..=options.max_retries {
            self.registry.cloud.check_deadline()?;
            if let Some(receipt) = self.registry.update_receipt(&prepared.claim)? {
                let mut result = receipt.result(self.config(), true);
                result.statement_retries = attempt;
                return Ok(result);
            }
            self.registry.reset(false)?;
            self.registry.begin_update(
                prepared,
                options.rebase_disjoint,
                options.allow_compaction,
            )?;
            let result = (|| {
                let affected = self
                    .connection
                    .execute(&prepared.sql, rusqlite::params_from_iter(&prepared.params))
                    .map_err(storage_error)?;
                self.registry.commit(affected as u64)
            })();
            match result {
                Ok(write) => {
                    let receipt = Receipt {
                        claim: prepared.claim.clone(),
                        commit_id: write
                            .commit_id
                            .ok_or_else(|| corrupt("update lacks commit receipt"))?,
                        affected_rows: write.affected_rows,
                        publication_retries: write.publication_retries,
                    };
                    let mut result = receipt.result(self.config(), write.deduplicated);
                    result.statement_retries = attempt;
                    return Ok(result);
                }
                Err(error)
                    if error.source().is_some_and(|e| e.is::<SnapshotConflict>())
                        && attempt < options.max_retries =>
                {
                    let cap = options
                        .backoff_ms
                        .saturating_mul(1u64 << attempt.min(16))
                        .min(options.max_backoff_ms);
                    let mut random = [0u8; 8];
                    getrandom::fill(&mut random).map_err(|e| invalid(e.to_string()))?;
                    let pause = Duration::from_millis(u64::from_le_bytes(random) % (cap + 1));
                    if Instant::now() + pause >= deadline {
                        return Err(EngineError::deadline_exceeded(
                            "update retry budget exhausted; last attempt did not commit",
                        ));
                    }
                    eprintln!(
                        "BriskDB overlay update retry: operation_id={} retry={} delay_ms={}",
                        prepared.claim.operation_id,
                        attempt + 1,
                        pause.as_millis()
                    );
                    self.registry.cloud.pause(pause)?;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("bounded update loop always returns")
    }

    /// None means no confirmed result, NOT proof that a timed-out write failed.
    pub fn update_status(
        &self,
        operation_id: &str,
        timeout_ms: u64,
    ) -> Result<Option<UpdateResult>> {
        if !is_nonce(operation_id) || !(1..=120_000).contains(&timeout_ms) {
            return Err(invalid("invalid operation ID or status timeout"));
        }
        self.registry
            .cloud
            .set_deadline(Some(Instant::now() + Duration::from_millis(timeout_ms)));
        let result = (|| {
            let key = operation_key(self.config(), operation_id, "request");
            let Some(bytes) = self.registry.cloud.get_optional(&key, 4096)? else {
                return Ok(None);
            };
            let claim: Claim = serde_json::from_slice(&bytes).map_err(storage_error)?;
            if claim.operation_id != operation_id
                || claim.table >= self.config().tables.len()
                || claim.partition >= self.config().partitions
            {
                return Err(corrupt("invalid operation claim"));
            }
            Ok(self
                .registry
                .update_receipt(&claim)?
                .map(|r| r.result(self.config(), true)))
        })();
        self.registry.cloud.set_deadline(None);
        result
    }
}
