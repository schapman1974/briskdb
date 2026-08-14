//! Protocol-neutral engine errors.

use std::{error::Error, fmt};

/// The stable category of an engine failure.
///
/// Protocol adapters map these categories to their own error representations.
/// The variants deliberately contain no HTTP, PostgreSQL, MySQL, or SQLite
/// types.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineErrorKind {
    InvalidArgument,
    NumericOutOfRange,
    InvalidTextEncoding,
    InvalidQuery,
    Unsupported,
    FailedPrecondition,
    TransactionAborted,
    TypeMismatch,
    ConstraintViolation,
    UniqueViolation,
    NotNullViolation,
    ForeignKeyViolation,
    CheckViolation,
    PermissionDenied,
    ReadOnly,
    Busy,
    Cancelled,
    DeadlineExceeded,
    LimitExceeded,
    ShuttingDown,
    StorageFull,
    OutOfMemory,
    StorageUnavailable,
    DataCorruption,
    Internal,
}

impl EngineErrorKind {
    /// Every currently defined kind, in compatibility-table order.
    pub const ALL: &'static [Self] = &[
        Self::InvalidArgument,
        Self::NumericOutOfRange,
        Self::InvalidTextEncoding,
        Self::InvalidQuery,
        Self::Unsupported,
        Self::FailedPrecondition,
        Self::TransactionAborted,
        Self::TypeMismatch,
        Self::ConstraintViolation,
        Self::UniqueViolation,
        Self::NotNullViolation,
        Self::ForeignKeyViolation,
        Self::CheckViolation,
        Self::PermissionDenied,
        Self::ReadOnly,
        Self::Busy,
        Self::Cancelled,
        Self::DeadlineExceeded,
        Self::LimitExceeded,
        Self::ShuttingDown,
        Self::StorageFull,
        Self::OutOfMemory,
        Self::StorageUnavailable,
        Self::DataCorruption,
        Self::Internal,
    ];

    /// Stable machine-readable identifier used by every protocol adapter.
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::NumericOutOfRange => "numeric_out_of_range",
            Self::InvalidTextEncoding => "invalid_text_encoding",
            Self::InvalidQuery => "invalid_query",
            Self::Unsupported => "unsupported",
            Self::FailedPrecondition => "failed_precondition",
            Self::TransactionAborted => "transaction_aborted",
            Self::TypeMismatch => "type_mismatch",
            Self::ConstraintViolation => "constraint_violation",
            Self::UniqueViolation => "unique_violation",
            Self::NotNullViolation => "not_null_violation",
            Self::ForeignKeyViolation => "foreign_key_violation",
            Self::CheckViolation => "check_violation",
            Self::PermissionDenied => "permission_denied",
            Self::ReadOnly => "read_only",
            Self::Busy => "busy",
            Self::Cancelled => "cancelled",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::LimitExceeded => "limit_exceeded",
            Self::ShuttingDown => "shutting_down",
            Self::StorageFull => "storage_full",
            Self::OutOfMemory => "out_of_memory",
            Self::StorageUnavailable => "storage_unavailable",
            Self::DataCorruption => "data_corruption",
            Self::Internal => "internal",
        }
    }

    /// Whether BriskDB recommends automatic retry without external remediation.
    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::Busy)
    }
}

/// A classified engine failure with a diagnostic cause chain.
///
/// `Display` and [`EngineError::diagnostic`] are intended for logs and trusted
/// Rust callers. Protocol adapters must serialize their fixed, safe mapping for
/// [`EngineError::kind`] rather than this diagnostic text.
#[derive(Debug)]
pub struct EngineError {
    kind: EngineErrorKind,
    diagnostic: String,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl EngineError {
    /// Construct an error without an underlying cause.
    pub fn new(kind: EngineErrorKind, diagnostic: impl Into<String>) -> Self {
        Self {
            kind,
            diagnostic: diagnostic.into(),
            source: None,
        }
    }

    /// Construct a request-deadline failure with trusted diagnostic context.
    pub fn deadline_exceeded(diagnostic: impl Into<String>) -> Self {
        Self::new(EngineErrorKind::DeadlineExceeded, diagnostic)
    }

    /// Construct a failure for work rejected while the engine is shutting down.
    pub fn shutting_down(diagnostic: impl Into<String>) -> Self {
        Self::new(EngineErrorKind::ShuttingDown, diagnostic)
    }

    pub(crate) fn from_source<E>(
        kind: EngineErrorKind,
        diagnostic: impl Into<String>,
        source: E,
    ) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            kind,
            diagnostic: diagnostic.into(),
            source: Some(Box::new(source)),
        }
    }

    pub(crate) fn context(self, diagnostic: impl Into<String>) -> Self {
        Self::from_source(self.kind, diagnostic, self)
    }

    /// Return the stable error category.
    pub const fn kind(&self) -> EngineErrorKind {
        self.kind
    }

    /// Return the stable machine-readable identifier.
    pub const fn code(&self) -> &'static str {
        self.kind.code()
    }

    /// Return diagnostic text for logs and trusted Rust callers.
    pub fn diagnostic(&self) -> &str {
        &self.diagnostic
    }

    /// Whether BriskDB recommends automatic retry without external remediation.
    pub const fn is_retryable(&self) -> bool {
        self.kind.is_retryable()
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.diagnostic)
    }
}

impl Error for EngineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

/// The result type returned by protocol-neutral engine operations.
pub type EngineResult<T> = Result<T, EngineError>;

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, io};

    use super::*;

    #[test]
    fn every_kind_has_a_unique_stable_code() {
        let codes = EngineErrorKind::ALL
            .iter()
            .copied()
            .map(EngineErrorKind::code)
            .collect::<Vec<_>>();
        assert_eq!(
            codes.iter().copied().collect::<HashSet<_>>().len(),
            codes.len()
        );
        assert!(codes.iter().all(|code| {
            !code.is_empty()
                && code
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
        }));
    }

    #[test]
    fn retryability_is_explicit_and_conservative() {
        let retryable = EngineErrorKind::ALL
            .iter()
            .copied()
            .filter(|kind| kind.is_retryable())
            .collect::<Vec<_>>();
        assert_eq!(retryable, [EngineErrorKind::Busy]);
    }

    #[test]
    fn lifecycle_constructors_preserve_trusted_diagnostics() {
        let deadline = EngineError::deadline_exceeded("query exceeded 25 ms");
        assert_eq!(deadline.kind(), EngineErrorKind::DeadlineExceeded);
        assert_eq!(deadline.code(), "deadline_exceeded");
        assert_eq!(deadline.diagnostic(), "query exceeded 25 ms");
        assert!(!deadline.is_retryable());

        let shutdown = EngineError::shutting_down("engine is draining");
        assert_eq!(shutdown.kind(), EngineErrorKind::ShuttingDown);
        assert_eq!(shutdown.code(), "shutting_down");
        assert_eq!(shutdown.diagnostic(), "engine is draining");
        assert!(!shutdown.is_retryable());
    }

    #[test]
    fn source_and_context_preserve_the_kind_and_cause_chain() {
        let error = EngineError::from_source(
            EngineErrorKind::StorageUnavailable,
            "could not open storage",
            io::Error::new(io::ErrorKind::NotFound, "private path"),
        )
        .context("broadcast failed on shard 2");

        assert_eq!(error.kind(), EngineErrorKind::StorageUnavailable);
        assert_eq!(error.code(), "storage_unavailable");
        assert!(!error.is_retryable());
        assert_eq!(error.to_string(), "broadcast failed on shard 2");

        let first = error.source().unwrap();
        assert_eq!(first.to_string(), "could not open storage");
        let root = first.source().unwrap();
        assert_eq!(
            root.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::NotFound
        );
    }

    #[test]
    fn engine_errors_are_thread_safe_standard_errors() {
        fn assert_error<T: Error + Send + Sync + 'static>() {}
        assert_error::<EngineError>();
    }
}
