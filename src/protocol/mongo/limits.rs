//! Host-owned limits can narrow, never raise, the listener's safety ceilings.

use std::{io, time::Duration};

/// Immutable resource policy for one Mongo listener.
///
/// Applies equally to its anonymous loopback connections, not authenticated
/// users. Engine-wide limits can narrow these limits further. Socket I/O,
/// BSON, cursor-count and other engine budgets remain independently bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MongoResourceLimits {
    max_connections: usize,
    command_timeout: Duration,
}

impl MongoResourceLimits {
    /// Select 1–8 connections and a positive command timeout of at most 15s.
    ///
    /// The timeout covers complete-frame decoding, preparation, engine
    /// admission and execution. Discovery commands and reply delivery have
    /// separate fixed bounds. Each getMore has its own command deadline, also
    /// narrowed by any remaining client-supplied cumulative maxTimeMS budget.
    pub fn new(max_connections: usize, command_timeout: Duration) -> io::Result<Self> {
        if !(1..=super::client_metadata::MAX_CONNECTIONS).contains(&max_connections) {
            return Err(super::invalid(
                "Mongo connection limit must be between 1 and 8",
            ));
        }
        if command_timeout.is_zero() || command_timeout > Duration::from_secs(15) {
            return Err(super::invalid(
                "Mongo command timeout must be positive and at most 15 seconds",
            ));
        }
        Ok(Self {
            max_connections,
            command_timeout,
        })
    }

    pub const fn max_connections(self) -> usize {
        self.max_connections
    }

    pub const fn command_timeout(self) -> Duration {
        self.command_timeout
    }
}

impl Default for MongoResourceLimits {
    fn default() -> Self {
        Self {
            max_connections: super::client_metadata::MAX_CONNECTIONS,
            command_timeout: Duration::from_secs(15),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_only_narrow_the_existing_ceilings() {
        assert_eq!(
            MongoResourceLimits::default(),
            MongoResourceLimits::new(8, Duration::from_secs(15)).unwrap()
        );
        for connections in [0, 9, usize::MAX] {
            assert!(MongoResourceLimits::new(connections, Duration::from_secs(1)).is_err());
        }
        for timeout in [
            Duration::ZERO,
            Duration::from_secs(15) + Duration::from_nanos(1),
            Duration::MAX,
        ] {
            assert!(MongoResourceLimits::new(1, timeout).is_err());
        }
        let limits = MongoResourceLimits::new(1, Duration::from_nanos(1)).unwrap();
        assert_eq!(limits.max_connections(), 1);
        assert_eq!(limits.command_timeout(), Duration::from_nanos(1));
    }
}
