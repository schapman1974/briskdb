//! BSON-specific errors and their protocol-neutral engine classification.

use std::{error::Error, fmt};

use crate::core::{EngineError, EngineErrorKind};

/// Stable category for failures produced by the BSON value and codec layer.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BsonErrorKind {
    /// A BSON value, type tag, or codec option is invalid.
    InvalidValue,
    /// A BSON string, key, or C string is not valid UTF-8.
    InvalidUtf8,
    /// A valid BSON wire type is intentionally outside BriskDB's supported subset.
    UnsupportedType,
    /// A document contains the same field name more than once.
    DuplicateField,
    /// A value cannot be represented by the canonical key format.
    InvalidCanonicalKey,
    /// The input ends before the declared BSON value is complete.
    Truncated,
    /// A BSON document, decoded representation, or canonical key exceeds its size limit.
    Oversized,
    /// Nested documents or arrays exceed the configured depth limit.
    NestingLimit,
}

impl BsonErrorKind {
    /// Stable machine-readable identifier for this error category.
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidValue => "invalid_value",
            Self::InvalidUtf8 => "invalid_utf8",
            Self::UnsupportedType => "unsupported_type",
            Self::DuplicateField => "duplicate_field",
            Self::InvalidCanonicalKey => "invalid_canonical_key",
            Self::Truncated => "truncated",
            Self::Oversized => "oversized",
            Self::NestingLimit => "nesting_limit",
        }
    }

    /// MongoDB-compatible numeric error code for a future wire adapter.
    ///
    /// MongoDB uses `10334` for size and resource failures classified as
    /// [`BsonErrorKind::Oversized`]. Other validation failures map to code `22`.
    pub const fn mongo_code(self) -> i32 {
        match self {
            Self::Oversized => 10_334,
            Self::InvalidValue
            | Self::InvalidUtf8
            | Self::UnsupportedType
            | Self::DuplicateField
            | Self::InvalidCanonicalKey
            | Self::Truncated
            | Self::NestingLimit => 22,
        }
    }
}

/// Identifies where invalid BSON entered the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BsonErrorContext {
    /// BSON supplied by a caller before it is accepted by the engine.
    ClientInput,
    /// BSON read from storage that the engine had already accepted.
    StoredData,
}

/// A classified BSON failure with diagnostic context for trusted callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BsonError {
    kind: BsonErrorKind,
    diagnostic: String,
    path: Option<String>,
}

impl BsonError {
    /// Construct an error without a value path.
    pub fn new(kind: BsonErrorKind, diagnostic: impl Into<String>) -> Self {
        Self {
            kind,
            diagnostic: diagnostic.into(),
            path: None,
        }
    }

    /// Return the stable error category.
    pub const fn kind(&self) -> BsonErrorKind {
        self.kind
    }

    /// Return the stable machine-readable identifier.
    pub const fn code(&self) -> &'static str {
        self.kind.code()
    }

    /// Return the MongoDB-compatible numeric error code.
    pub const fn mongo_code(&self) -> i32 {
        self.kind.mongo_code()
    }

    /// Return diagnostic text intended for logs and trusted Rust callers.
    pub fn diagnostic(&self) -> &str {
        &self.diagnostic
    }

    /// Return the best-effort path of the invalid value, when available.
    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    pub(crate) fn with_path(mut self, path: impl Into<String>) -> Self {
        if self.path.is_none() {
            self.path = Some(path.into());
        }
        self
    }

    /// Convert this BSON failure into the engine's protocol-neutral error.
    ///
    /// Invalid client input is either an invalid argument or a resource limit
    /// failure. Invalid persisted BSON is always data corruption, regardless
    /// of the original codec category.
    pub fn into_engine_error(self, context: BsonErrorContext) -> EngineError {
        let engine_kind = match context {
            BsonErrorContext::StoredData => EngineErrorKind::DataCorruption,
            BsonErrorContext::ClientInput => match self.kind {
                BsonErrorKind::Oversized | BsonErrorKind::NestingLimit => {
                    EngineErrorKind::LimitExceeded
                }
                BsonErrorKind::UnsupportedType => EngineErrorKind::Unsupported,
                BsonErrorKind::InvalidUtf8 => EngineErrorKind::InvalidTextEncoding,
                BsonErrorKind::InvalidValue
                | BsonErrorKind::DuplicateField
                | BsonErrorKind::InvalidCanonicalKey
                | BsonErrorKind::Truncated => EngineErrorKind::InvalidArgument,
            },
        };
        let diagnostic = self.to_string();
        EngineError::from_source(engine_kind, diagnostic, self)
    }
}

impl fmt::Display for BsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(path) = &self.path {
            write!(formatter, "{} at {path}", self.diagnostic)
        } else {
            formatter.write_str(&self.diagnostic)
        }
    }
}

impl Error for BsonError {}

/// Result type returned by BSON value, codec, and key operations.
pub type BsonResult<T> = Result<T, BsonError>;
