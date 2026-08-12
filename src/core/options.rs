//! Configuration for the asynchronous engine execution boundary.

use std::time::Duration;

use super::{EngineError, EngineErrorKind, EngineResult};

/// Default number of concurrently leased SQLite connections per shard.
pub const DEFAULT_CONNECTIONS_PER_SHARD: usize = 4;

/// Default number of operations admitted to each shard's pending queue.
pub const DEFAULT_QUEUE_CAPACITY_PER_SHARD: usize = 32;

/// Default maximum number of rows materialized by one query.
pub const DEFAULT_MAX_RESULT_ROWS: u64 = 10_000;

/// Default maximum logical size of one materialized query result (16 MiB).
pub const DEFAULT_MAX_RESULT_BYTES: u64 = 16 * 1024 * 1024;

/// Default engine-enforced request deadline in milliseconds.
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;

/// Default graceful-shutdown drain period in milliseconds.
pub const DEFAULT_SHUTDOWN_GRACE_MS: u64 = 30_000;

/// Default maximum number of prepared statements retained by one session.
pub const DEFAULT_MAX_PREPARED_STATEMENTS_PER_SESSION: usize = 128;

/// Default maximum number of bound portals retained by one session.
pub const DEFAULT_MAX_PORTALS_PER_SESSION: usize = 128;

/// Default maximum accounted bound-value bytes retained by one session (16 MiB).
pub const DEFAULT_MAX_RETAINED_BOUND_VALUE_BYTES: u64 = 16 * 1024 * 1024;

/// Maximum configurable connections per shard.
pub const MAX_CONNECTIONS_PER_SHARD: usize = 16;

/// Maximum configurable pending operations per shard.
pub const MAX_QUEUE_CAPACITY_PER_SHARD: usize = 1_024;

/// Maximum configurable rows in one materialized query result.
pub const MAX_RESULT_ROWS: u64 = 1_000_000;

/// Maximum configurable logical size of one materialized query result (1 GiB).
pub const MAX_RESULT_BYTES: u64 = 1024 * 1024 * 1024;

/// Maximum configured request timeout (24 hours).
pub const MAX_REQUEST_TIMEOUT_MS: u64 = 24 * 60 * 60 * 1_000;

/// Maximum configured graceful-shutdown period (24 hours).
pub const MAX_SHUTDOWN_GRACE_MS: u64 = 24 * 60 * 60 * 1_000;

/// Maximum configurable prepared statements retained by one session.
pub const MAX_PREPARED_STATEMENTS_PER_SESSION: usize = 1_024;

/// Maximum configurable bound portals retained by one session.
pub const MAX_PORTALS_PER_SESSION: usize = 1_024;

/// Maximum configurable accounted bound-value bytes retained by one session (1 GiB).
pub const MAX_RETAINED_BOUND_VALUE_BYTES: u64 = 1024 * 1024 * 1024;

const MAX_TOTAL_ACTIVE_CONNECTIONS: usize = 512;

/// Validated finite limits for a materialized query result.
///
/// BriskDB accounts a result independently of any wire protocol: 16 bytes for
/// the result envelope, one type byte plus an eight-byte length and the UTF-8
/// name for each column, eight bytes for each row, and one type byte plus an
/// eight-byte length and the value payload for each value. Integer and floating
/// payloads are eight bytes, null has no payload, and text/blob payloads use
/// their byte length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultLimits {
    max_rows: u64,
    max_bytes: u64,
}

impl ResultLimits {
    /// Construct validated finite result limits.
    pub fn new(max_rows: u64, max_bytes: u64) -> EngineResult<Self> {
        if !(1..=MAX_RESULT_ROWS).contains(&max_rows) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!("maximum result rows must be between 1 and {MAX_RESULT_ROWS}"),
            ));
        }
        if !(1..=MAX_RESULT_BYTES).contains(&max_bytes) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!("maximum result bytes must be between 1 and {MAX_RESULT_BYTES}"),
            ));
        }

        Ok(Self {
            max_rows,
            max_bytes,
        })
    }

    /// Return the maximum number of rows materialized by one query.
    pub const fn max_rows(self) -> u64 {
        self.max_rows
    }

    /// Return the maximum protocol-neutral logical byte size of one result.
    pub const fn max_bytes(self) -> u64 {
        self.max_bytes
    }
}

impl Default for ResultLimits {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_MAX_RESULT_ROWS,
            max_bytes: DEFAULT_MAX_RESULT_BYTES,
        }
    }
}

/// Validated per-session limits for prepared statements and bound portals.
///
/// The retained byte limit accounts for protocol-neutral logical values owned
/// by all portals in a session. The same ceiling conservatively bounds one
/// bind's planning expansion by charging the captured route once and every
/// normalized marker occurrence twice, once for typed inference and once for
/// canonical routing. It is independent of the materialized query-result byte
/// limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedStatementLimits {
    max_statements_per_session: usize,
    max_portals_per_session: usize,
    max_retained_bound_value_bytes: u64,
}

impl PreparedStatementLimits {
    /// Construct validated finite per-session prepared-statement limits.
    pub fn new(
        max_statements_per_session: usize,
        max_portals_per_session: usize,
        max_retained_bound_value_bytes: u64,
    ) -> EngineResult<Self> {
        if !(1..=MAX_PREPARED_STATEMENTS_PER_SESSION).contains(&max_statements_per_session) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!(
                    "maximum prepared statements per session must be between 1 and \
                     {MAX_PREPARED_STATEMENTS_PER_SESSION}"
                ),
            ));
        }
        if !(1..=MAX_PORTALS_PER_SESSION).contains(&max_portals_per_session) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!(
                    "maximum portals per session must be between 1 and \
                     {MAX_PORTALS_PER_SESSION}"
                ),
            ));
        }
        if !(1..=MAX_RETAINED_BOUND_VALUE_BYTES).contains(&max_retained_bound_value_bytes) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!(
                    "maximum retained bound-value bytes must be between 1 and \
                     {MAX_RETAINED_BOUND_VALUE_BYTES}"
                ),
            ));
        }

        Ok(Self {
            max_statements_per_session,
            max_portals_per_session,
            max_retained_bound_value_bytes,
        })
    }

    /// Return the maximum prepared statements retained by one session.
    pub const fn max_statements_per_session(self) -> usize {
        self.max_statements_per_session
    }

    /// Return the maximum bound portals retained by one session.
    pub const fn max_portals_per_session(self) -> usize {
        self.max_portals_per_session
    }

    /// Return the maximum accounted retained and per-bind planning bytes.
    pub const fn max_retained_bound_value_bytes(self) -> u64 {
        self.max_retained_bound_value_bytes
    }
}

impl Default for PreparedStatementLimits {
    fn default() -> Self {
        Self {
            max_statements_per_session: DEFAULT_MAX_PREPARED_STATEMENTS_PER_SESSION,
            max_portals_per_session: DEFAULT_MAX_PORTALS_PER_SESSION,
            max_retained_bound_value_bytes: DEFAULT_MAX_RETAINED_BOUND_VALUE_BYTES,
        }
    }
}

/// Resource limits for the asynchronous engine.
///
/// These limits bound active SQLite connections, admitted work, materialized
/// results, session-owned prepared state, request duration, and
/// graceful-shutdown draining. A caller can narrow the result and time limits
/// for one request through `RequestContext`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineOptions {
    connections_per_shard: usize,
    queue_capacity_per_shard: usize,
    result_limits: ResultLimits,
    prepared_statement_limits: PreparedStatementLimits,
    request_timeout: Option<Duration>,
    shutdown_grace: Duration,
    #[cfg(feature = "experimental-vtab")]
    experimental_vtab_writes: bool,
}

impl EngineOptions {
    /// Construct validated engine resource limits.
    pub fn new(
        connections_per_shard: usize,
        queue_capacity_per_shard: usize,
    ) -> EngineResult<Self> {
        if !(1..=MAX_CONNECTIONS_PER_SHARD).contains(&connections_per_shard) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!("connections per shard must be between 1 and {MAX_CONNECTIONS_PER_SHARD}"),
            ));
        }
        if !(1..=MAX_QUEUE_CAPACITY_PER_SHARD).contains(&queue_capacity_per_shard) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                format!(
                    "queue capacity per shard must be between 1 and {MAX_QUEUE_CAPACITY_PER_SHARD}"
                ),
            ));
        }

        Ok(Self {
            connections_per_shard,
            queue_capacity_per_shard,
            result_limits: ResultLimits::default(),
            prepared_statement_limits: PreparedStatementLimits::default(),
            request_timeout: Some(Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS)),
            shutdown_grace: Duration::from_millis(DEFAULT_SHUTDOWN_GRACE_MS),
            #[cfg(feature = "experimental-vtab")]
            experimental_vtab_writes: false,
        })
    }

    /// Return the maximum number of concurrently leased connections per shard.
    pub const fn connections_per_shard(&self) -> usize {
        self.connections_per_shard
    }

    /// Return the number of pending operations admitted per shard.
    pub const fn queue_capacity_per_shard(&self) -> usize {
        self.queue_capacity_per_shard
    }

    /// Return the finite limits applied to each materialized query result.
    pub const fn result_limits(&self) -> ResultLimits {
        self.result_limits
    }

    /// Replace the finite limits applied to each materialized query result.
    #[must_use]
    pub const fn with_result_limits(mut self, result_limits: ResultLimits) -> Self {
        self.result_limits = result_limits;
        self
    }

    /// Return the finite per-session prepared-statement limits.
    pub const fn prepared_statement_limits(&self) -> PreparedStatementLimits {
        self.prepared_statement_limits
    }

    /// Replace the finite per-session prepared-statement limits.
    #[must_use]
    pub const fn with_prepared_statement_limits(
        mut self,
        prepared_statement_limits: PreparedStatementLimits,
    ) -> Self {
        self.prepared_statement_limits = prepared_statement_limits;
        self
    }

    /// Return the maximum duration of one operation, or `None` when the
    /// engine-wide deadline is disabled.
    pub const fn request_timeout(&self) -> Option<Duration> {
        self.request_timeout
    }

    /// Set or disable the engine-wide request deadline.
    ///
    /// Passing `None` disables only the engine default; an explicit
    /// `RequestContext` deadline is still enforced.
    pub fn with_request_timeout(mut self, timeout: Option<Duration>) -> EngineResult<Self> {
        if let Some(timeout) = timeout {
            validate_duration(timeout, MAX_REQUEST_TIMEOUT_MS, "request timeout")?;
        }
        self.request_timeout = timeout;
        Ok(self)
    }

    /// Return the grace period allowed before shutdown cancels admitted work.
    pub const fn shutdown_grace(&self) -> Duration {
        self.shutdown_grace
    }

    /// Set the finite graceful-shutdown drain period.
    pub fn with_shutdown_grace(mut self, grace: Duration) -> EngineResult<Self> {
        validate_duration(grace, MAX_SHUTDOWN_GRACE_MS, "shutdown grace period")?;
        self.shutdown_grace = grace;
        Ok(self)
    }

    /// Return whether registered autocommit writes use the experimental
    /// sharded virtual-table facade.
    #[cfg(feature = "experimental-vtab")]
    pub const fn experimental_vtab_writes(&self) -> bool {
        self.experimental_vtab_writes
    }

    /// Enable or disable the experimental sharded virtual-table write path.
    ///
    /// This runtime opt-in is available only when BriskDB is compiled with the
    /// `experimental-vtab` Cargo feature. The established pooled execution
    /// path remains the default even in feature-enabled builds.
    #[cfg(feature = "experimental-vtab")]
    #[must_use]
    pub const fn with_experimental_vtab_writes(mut self, enabled: bool) -> Self {
        self.experimental_vtab_writes = enabled;
        self
    }

    /// Validate limits that depend on the database's physical shard count.
    ///
    /// The returned value is the maximum number of active SQLite connections
    /// across all shards and is also the blocking-worker concurrency bound.
    pub(crate) fn worker_limit(self, shard_count: u16) -> EngineResult<usize> {
        let total_active_connections = self
            .connections_per_shard
            .checked_mul(usize::from(shard_count))
            .ok_or_else(|| invalid_total_connections(shard_count))?;

        if total_active_connections == 0 || total_active_connections > MAX_TOTAL_ACTIVE_CONNECTIONS
        {
            return Err(invalid_total_connections(shard_count));
        }

        Ok(total_active_connections)
    }
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            connections_per_shard: DEFAULT_CONNECTIONS_PER_SHARD,
            queue_capacity_per_shard: DEFAULT_QUEUE_CAPACITY_PER_SHARD,
            result_limits: ResultLimits {
                max_rows: DEFAULT_MAX_RESULT_ROWS,
                max_bytes: DEFAULT_MAX_RESULT_BYTES,
            },
            prepared_statement_limits: PreparedStatementLimits {
                max_statements_per_session: DEFAULT_MAX_PREPARED_STATEMENTS_PER_SESSION,
                max_portals_per_session: DEFAULT_MAX_PORTALS_PER_SESSION,
                max_retained_bound_value_bytes: DEFAULT_MAX_RETAINED_BOUND_VALUE_BYTES,
            },
            request_timeout: Some(Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS)),
            shutdown_grace: Duration::from_millis(DEFAULT_SHUTDOWN_GRACE_MS),
            #[cfg(feature = "experimental-vtab")]
            experimental_vtab_writes: false,
        }
    }
}

fn validate_duration(duration: Duration, maximum_ms: u64, name: &str) -> EngineResult<()> {
    if duration < Duration::from_millis(1) || duration > Duration::from_millis(maximum_ms) {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            format!("{name} must be between 1 and {maximum_ms} milliseconds"),
        ));
    }
    Ok(())
}

fn invalid_total_connections(shard_count: u16) -> EngineError {
    EngineError::new(
        EngineErrorKind::InvalidArgument,
        format!(
            "configured connection pools for {shard_count} shards must contain between 1 and \
             {MAX_TOTAL_ACTIVE_CONNECTIONS} total active connections"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_explicit_and_valid_for_the_supported_shard_range() {
        let options = EngineOptions::default();

        assert_eq!(options.connections_per_shard(), 4);
        assert_eq!(options.queue_capacity_per_shard(), 32);
        assert_eq!(options.result_limits(), ResultLimits::default());
        assert_eq!(options.result_limits().max_rows(), 10_000);
        assert_eq!(options.result_limits().max_bytes(), 16 * 1024 * 1024);
        assert_eq!(
            options.prepared_statement_limits(),
            PreparedStatementLimits::default()
        );
        assert_eq!(
            options
                .prepared_statement_limits()
                .max_statements_per_session(),
            128
        );
        assert_eq!(
            options
                .prepared_statement_limits()
                .max_portals_per_session(),
            128
        );
        assert_eq!(
            options
                .prepared_statement_limits()
                .max_retained_bound_value_bytes(),
            16 * 1024 * 1024
        );
        assert_eq!(options.request_timeout(), Some(Duration::from_secs(30)));
        assert_eq!(options.shutdown_grace(), Duration::from_secs(30));
        #[cfg(feature = "experimental-vtab")]
        assert!(!options.experimental_vtab_writes());
        assert_eq!(options.worker_limit(2).unwrap(), 8);
        assert_eq!(options.worker_limit(64).unwrap(), 256);
    }

    #[cfg(feature = "experimental-vtab")]
    #[test]
    fn experimental_vtab_writes_are_an_explicit_reversible_opt_in() {
        let enabled = EngineOptions::default().with_experimental_vtab_writes(true);
        assert!(enabled.experimental_vtab_writes());

        let disabled = enabled.with_experimental_vtab_writes(false);
        assert!(!disabled.experimental_vtab_writes());
        assert_eq!(disabled, EngineOptions::default());
    }

    #[test]
    fn constructor_rejects_zero_and_values_above_each_limit() {
        for (connections, queue_capacity) in [
            (0, 1),
            (MAX_CONNECTIONS_PER_SHARD + 1, 1),
            (1, 0),
            (1, MAX_QUEUE_CAPACITY_PER_SHARD + 1),
        ] {
            assert_eq!(
                EngineOptions::new(connections, queue_capacity)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::InvalidArgument
            );
        }
    }

    #[test]
    fn constructor_preserves_boundary_values_and_accessors() {
        let minimum = EngineOptions::new(1, 1).unwrap();
        assert_eq!(minimum.connections_per_shard(), 1);
        assert_eq!(minimum.queue_capacity_per_shard(), 1);

        let maximum =
            EngineOptions::new(MAX_CONNECTIONS_PER_SHARD, MAX_QUEUE_CAPACITY_PER_SHARD).unwrap();
        assert_eq!(maximum.connections_per_shard(), 16);
        assert_eq!(maximum.queue_capacity_per_shard(), 1_024);
        assert_eq!(minimum.result_limits(), ResultLimits::default());
        assert_eq!(maximum.result_limits(), ResultLimits::default());
        assert_eq!(
            minimum.prepared_statement_limits(),
            PreparedStatementLimits::default()
        );
        assert_eq!(
            maximum.prepared_statement_limits(),
            PreparedStatementLimits::default()
        );
    }

    #[test]
    fn result_limit_constructor_preserves_inclusive_boundaries() {
        let minimum = ResultLimits::new(1, 1).unwrap();
        assert_eq!(minimum.max_rows(), 1);
        assert_eq!(minimum.max_bytes(), 1);

        let maximum = ResultLimits::new(MAX_RESULT_ROWS, MAX_RESULT_BYTES).unwrap();
        assert_eq!(maximum.max_rows(), 1_000_000);
        assert_eq!(maximum.max_bytes(), 1024 * 1024 * 1024);
    }

    #[test]
    fn result_limit_constructor_rejects_zero_and_values_above_each_cap() {
        for (max_rows, max_bytes) in [
            (0, 1),
            (MAX_RESULT_ROWS + 1, 1),
            (1, 0),
            (1, MAX_RESULT_BYTES + 1),
        ] {
            let error = ResultLimits::new(max_rows, max_bytes).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
        }
    }

    #[test]
    fn prepared_statement_limit_constructor_preserves_inclusive_boundaries() {
        let minimum = PreparedStatementLimits::new(1, 1, 1).unwrap();
        assert_eq!(minimum.max_statements_per_session(), 1);
        assert_eq!(minimum.max_portals_per_session(), 1);
        assert_eq!(minimum.max_retained_bound_value_bytes(), 1);

        let maximum = PreparedStatementLimits::new(
            MAX_PREPARED_STATEMENTS_PER_SESSION,
            MAX_PORTALS_PER_SESSION,
            MAX_RETAINED_BOUND_VALUE_BYTES,
        )
        .unwrap();
        assert_eq!(maximum.max_statements_per_session(), 1_024);
        assert_eq!(maximum.max_portals_per_session(), 1_024);
        assert_eq!(maximum.max_retained_bound_value_bytes(), 1024 * 1024 * 1024);
    }

    #[test]
    fn prepared_statement_limit_constructor_rejects_each_invalid_value() {
        for (max_statements, max_portals, max_bound_bytes) in [
            (0, 1, 1),
            (MAX_PREPARED_STATEMENTS_PER_SESSION + 1, 1, 1),
            (1, 0, 1),
            (1, MAX_PORTALS_PER_SESSION + 1, 1),
            (1, 1, 0),
            (1, 1, MAX_RETAINED_BOUND_VALUE_BYTES + 1),
        ] {
            let error = PreparedStatementLimits::new(max_statements, max_portals, max_bound_bytes)
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
        }
    }

    #[test]
    fn engine_options_builder_replaces_only_result_limits() {
        let limits = ResultLimits::new(37, 4_096).unwrap();
        let options = EngineOptions::new(2, 7).unwrap().with_result_limits(limits);

        assert_eq!(options.connections_per_shard(), 2);
        assert_eq!(options.queue_capacity_per_shard(), 7);
        assert_eq!(options.result_limits(), limits);
        assert_eq!(
            options.prepared_statement_limits(),
            PreparedStatementLimits::default()
        );
        assert_eq!(options.request_timeout(), Some(Duration::from_secs(30)));
        assert_eq!(options.shutdown_grace(), Duration::from_secs(30));
    }

    #[test]
    fn engine_options_builder_replaces_only_prepared_statement_limits() {
        let limits = PreparedStatementLimits::new(37, 41, 4_096).unwrap();
        let options = EngineOptions::new(2, 7)
            .unwrap()
            .with_prepared_statement_limits(limits);

        assert_eq!(options.connections_per_shard(), 2);
        assert_eq!(options.queue_capacity_per_shard(), 7);
        assert_eq!(options.result_limits(), ResultLimits::default());
        assert_eq!(options.prepared_statement_limits(), limits);
        assert_eq!(options.request_timeout(), Some(Duration::from_secs(30)));
        assert_eq!(options.shutdown_grace(), Duration::from_secs(30));
    }

    #[test]
    fn timeout_and_shutdown_builders_validate_boundaries_and_disable_semantics() {
        let minimum = EngineOptions::default()
            .with_request_timeout(Some(Duration::from_millis(1)))
            .unwrap()
            .with_shutdown_grace(Duration::from_millis(1))
            .unwrap();
        assert_eq!(minimum.request_timeout(), Some(Duration::from_millis(1)));
        assert_eq!(minimum.shutdown_grace(), Duration::from_millis(1));

        let maximum = EngineOptions::default()
            .with_request_timeout(Some(Duration::from_millis(MAX_REQUEST_TIMEOUT_MS)))
            .unwrap()
            .with_shutdown_grace(Duration::from_millis(MAX_SHUTDOWN_GRACE_MS))
            .unwrap();
        assert_eq!(
            maximum.request_timeout(),
            Some(Duration::from_millis(MAX_REQUEST_TIMEOUT_MS))
        );
        assert_eq!(
            maximum.shutdown_grace(),
            Duration::from_millis(MAX_SHUTDOWN_GRACE_MS)
        );

        assert_eq!(
            EngineOptions::default()
                .with_request_timeout(None)
                .unwrap()
                .request_timeout(),
            None
        );
        for error in [
            EngineOptions::default()
                .with_request_timeout(Some(Duration::ZERO))
                .unwrap_err(),
            EngineOptions::default()
                .with_request_timeout(Some(Duration::from_nanos(1)))
                .unwrap_err(),
            EngineOptions::default()
                .with_request_timeout(Some(Duration::from_millis(MAX_REQUEST_TIMEOUT_MS + 1)))
                .unwrap_err(),
            EngineOptions::default()
                .with_shutdown_grace(Duration::ZERO)
                .unwrap_err(),
            EngineOptions::default()
                .with_shutdown_grace(Duration::from_nanos(1))
                .unwrap_err(),
            EngineOptions::default()
                .with_shutdown_grace(Duration::from_millis(MAX_SHUTDOWN_GRACE_MS + 1))
                .unwrap_err(),
        ] {
            assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
        }
    }

    #[test]
    fn shard_validation_uses_checked_total_active_connection_limit() {
        let at_limit = EngineOptions::new(8, 1).unwrap();
        assert_eq!(at_limit.worker_limit(64).unwrap(), 512);

        let over_limit = EngineOptions::new(16, 1).unwrap();
        assert_eq!(
            over_limit.worker_limit(64).unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
        assert_eq!(
            EngineOptions::default().worker_limit(0).unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
    }
}
