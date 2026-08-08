//! Configuration for the asynchronous engine execution boundary.

use super::{EngineError, EngineErrorKind, EngineResult};

/// Default number of concurrently leased SQLite connections per shard.
pub const DEFAULT_CONNECTIONS_PER_SHARD: usize = 4;

/// Default number of operations admitted to each shard's pending queue.
pub const DEFAULT_QUEUE_CAPACITY_PER_SHARD: usize = 32;

/// Maximum configurable connections per shard.
pub const MAX_CONNECTIONS_PER_SHARD: usize = 16;

/// Maximum configurable pending operations per shard.
pub const MAX_QUEUE_CAPACITY_PER_SHARD: usize = 1_024;

const MAX_TOTAL_ACTIVE_CONNECTIONS: usize = 512;

/// Resource limits for the asynchronous engine.
///
/// These limits bound active SQLite connections and the amount of work that
/// can be admitted ahead of each shard. They do not configure request
/// deadlines or cancellation, which are separate engine policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineOptions {
    connections_per_shard: usize,
    queue_capacity_per_shard: usize,
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
        }
    }
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
        assert_eq!(options.worker_limit(2).unwrap(), 8);
        assert_eq!(options.worker_limit(64).unwrap(), 256);
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
