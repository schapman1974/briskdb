//! Safe protocol representations of protocol-neutral engine errors.
//!
//! PostgreSQL startup and simple-query responses use these fixed mappings.
//! No MySQL listener exists yet; its mapping remains the tested contract for
//! that later wire adapter.

use crate::core::EngineErrorKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct HttpErrorMapping {
    pub status: u16,
    pub problem_type: &'static str,
    pub title: &'static str,
    pub detail: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct PostgresErrorMapping {
    pub sqlstate: &'static str,
    pub message: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct MysqlErrorMapping {
    pub error_number: u16,
    pub sqlstate: &'static str,
    pub message: &'static str,
}

/// Return the RFC 9457 representation for an engine error kind.
pub const fn http_error(kind: EngineErrorKind) -> HttpErrorMapping {
    let status = match kind {
        EngineErrorKind::InvalidArgument => 400,
        EngineErrorKind::NumericOutOfRange
        | EngineErrorKind::InvalidTextEncoding
        | EngineErrorKind::InvalidQuery
        | EngineErrorKind::TypeMismatch => 422,
        EngineErrorKind::Unsupported => 501,
        EngineErrorKind::FailedPrecondition
        | EngineErrorKind::TransactionAborted
        | EngineErrorKind::ConstraintViolation
        | EngineErrorKind::UniqueViolation
        | EngineErrorKind::NotNullViolation
        | EngineErrorKind::ForeignKeyViolation
        | EngineErrorKind::CheckViolation => 409,
        EngineErrorKind::PermissionDenied | EngineErrorKind::ReadOnly => 403,
        EngineErrorKind::Busy
        | EngineErrorKind::OutOfMemory
        | EngineErrorKind::StorageUnavailable
        | EngineErrorKind::ShuttingDown => 503,
        EngineErrorKind::Cancelled => 500,
        EngineErrorKind::DeadlineExceeded => 504,
        EngineErrorKind::LimitExceeded => 422,
        EngineErrorKind::StorageFull => 507,
        EngineErrorKind::DataCorruption | EngineErrorKind::Internal => 500,
    };

    HttpErrorMapping {
        status,
        problem_type: problem_type(kind),
        title: title(kind),
        detail: detail(kind),
    }
}

/// Return the PostgreSQL SQLSTATE and safe message for an engine error kind.
pub const fn postgres_error(kind: EngineErrorKind) -> PostgresErrorMapping {
    let sqlstate = match kind {
        EngineErrorKind::InvalidArgument => "22023",
        EngineErrorKind::NumericOutOfRange => "22003",
        EngineErrorKind::InvalidTextEncoding => "22021",
        EngineErrorKind::InvalidQuery => "42000",
        EngineErrorKind::Unsupported => "0A000",
        EngineErrorKind::FailedPrecondition => "55000",
        EngineErrorKind::TransactionAborted => "25P02",
        EngineErrorKind::TypeMismatch => "42804",
        EngineErrorKind::ConstraintViolation => "23000",
        EngineErrorKind::UniqueViolation => "23505",
        EngineErrorKind::NotNullViolation => "23502",
        EngineErrorKind::ForeignKeyViolation => "23503",
        EngineErrorKind::CheckViolation => "23514",
        EngineErrorKind::PermissionDenied => "42501",
        EngineErrorKind::ReadOnly => "25006",
        EngineErrorKind::Busy => "55P03",
        EngineErrorKind::Cancelled => "57014",
        EngineErrorKind::DeadlineExceeded => "57014",
        EngineErrorKind::LimitExceeded => "54000",
        EngineErrorKind::ShuttingDown => "57P01",
        EngineErrorKind::StorageFull => "53100",
        EngineErrorKind::OutOfMemory => "53200",
        EngineErrorKind::StorageUnavailable => "58030",
        EngineErrorKind::DataCorruption => "XX001",
        EngineErrorKind::Internal => "XX000",
    };

    PostgresErrorMapping {
        sqlstate,
        message: detail(kind),
    }
}

/// Return the MySQL error number, SQLSTATE, and safe message for a kind.
pub const fn mysql_error(kind: EngineErrorKind) -> MysqlErrorMapping {
    let (error_number, sqlstate) = match kind {
        EngineErrorKind::InvalidArgument => (1210, "HY000"),
        EngineErrorKind::NumericOutOfRange => (1690, "22003"),
        EngineErrorKind::InvalidTextEncoding => (1366, "HY000"),
        EngineErrorKind::InvalidQuery => (1105, "HY000"),
        EngineErrorKind::Unsupported => (1235, "42000"),
        EngineErrorKind::FailedPrecondition => (1105, "HY000"),
        EngineErrorKind::TransactionAborted => (1105, "HY000"),
        EngineErrorKind::TypeMismatch => (1366, "HY000"),
        EngineErrorKind::ConstraintViolation => (1105, "HY000"),
        EngineErrorKind::UniqueViolation => (1062, "23000"),
        EngineErrorKind::NotNullViolation => (1048, "23000"),
        // SQLite does not report whether a foreign-key failure was caused by
        // the parent or child side, so using MySQL 1451 or 1452 would lie.
        EngineErrorKind::ForeignKeyViolation => (1105, "HY000"),
        EngineErrorKind::CheckViolation => (3819, "HY000"),
        EngineErrorKind::PermissionDenied => (1227, "42000"),
        EngineErrorKind::ReadOnly => (1290, "HY000"),
        EngineErrorKind::Busy => (1205, "HY000"),
        EngineErrorKind::Cancelled => (1317, "70100"),
        EngineErrorKind::DeadlineExceeded => (3024, "HY000"),
        EngineErrorKind::LimitExceeded => (1105, "HY000"),
        EngineErrorKind::ShuttingDown => (1053, "08S01"),
        EngineErrorKind::StorageFull => (1114, "HY000"),
        EngineErrorKind::OutOfMemory => (1037, "HY001"),
        EngineErrorKind::StorageUnavailable
        | EngineErrorKind::DataCorruption
        | EngineErrorKind::Internal => (1105, "HY000"),
    };

    MysqlErrorMapping {
        error_number,
        sqlstate,
        message: detail(kind),
    }
}

const fn problem_type(kind: EngineErrorKind) -> &'static str {
    match kind {
        EngineErrorKind::InvalidArgument => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#invalid-argument"
        }
        EngineErrorKind::NumericOutOfRange => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#numeric-out-of-range"
        }
        EngineErrorKind::InvalidTextEncoding => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#invalid-text-encoding"
        }
        EngineErrorKind::InvalidQuery => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#invalid-query"
        }
        EngineErrorKind::Unsupported => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#unsupported"
        }
        EngineErrorKind::FailedPrecondition => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#failed-precondition"
        }
        EngineErrorKind::TransactionAborted => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#transaction-aborted"
        }
        EngineErrorKind::TypeMismatch => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#type-mismatch"
        }
        EngineErrorKind::ConstraintViolation => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#constraint-violation"
        }
        EngineErrorKind::UniqueViolation => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#unique-violation"
        }
        EngineErrorKind::NotNullViolation => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#not-null-violation"
        }
        EngineErrorKind::ForeignKeyViolation => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#foreign-key-violation"
        }
        EngineErrorKind::CheckViolation => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#check-violation"
        }
        EngineErrorKind::PermissionDenied => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#permission-denied"
        }
        EngineErrorKind::ReadOnly => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#read-only"
        }
        EngineErrorKind::Busy => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#busy"
        }
        EngineErrorKind::Cancelled => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#cancelled"
        }
        EngineErrorKind::DeadlineExceeded => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#deadline-exceeded"
        }
        EngineErrorKind::LimitExceeded => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#limit-exceeded"
        }
        EngineErrorKind::ShuttingDown => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#shutting-down"
        }
        EngineErrorKind::StorageFull => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#storage-full"
        }
        EngineErrorKind::OutOfMemory => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#out-of-memory"
        }
        EngineErrorKind::StorageUnavailable => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#storage-unavailable"
        }
        EngineErrorKind::DataCorruption => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#data-corruption"
        }
        EngineErrorKind::Internal => {
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#internal"
        }
    }
}

const fn title(kind: EngineErrorKind) -> &'static str {
    match kind {
        EngineErrorKind::InvalidArgument => "Invalid argument",
        EngineErrorKind::NumericOutOfRange => "Numeric value out of range",
        EngineErrorKind::InvalidTextEncoding => "Invalid text encoding",
        EngineErrorKind::InvalidQuery => "Invalid query",
        EngineErrorKind::Unsupported => "Unsupported operation",
        EngineErrorKind::FailedPrecondition => "Failed precondition",
        EngineErrorKind::TransactionAborted => "Transaction aborted",
        EngineErrorKind::TypeMismatch => "Type mismatch",
        EngineErrorKind::ConstraintViolation => "Constraint violation",
        EngineErrorKind::UniqueViolation => "Unique constraint violation",
        EngineErrorKind::NotNullViolation => "Not-null constraint violation",
        EngineErrorKind::ForeignKeyViolation => "Foreign-key constraint violation",
        EngineErrorKind::CheckViolation => "Check constraint violation",
        EngineErrorKind::PermissionDenied => "Permission denied",
        EngineErrorKind::ReadOnly => "Read-only storage",
        EngineErrorKind::Busy => "Database busy",
        EngineErrorKind::Cancelled => "Request cancelled",
        EngineErrorKind::DeadlineExceeded => "Request deadline exceeded",
        EngineErrorKind::LimitExceeded => "Limit exceeded",
        EngineErrorKind::ShuttingDown => "Server shutting down",
        EngineErrorKind::StorageFull => "Storage full",
        EngineErrorKind::OutOfMemory => "Out of memory",
        EngineErrorKind::StorageUnavailable => "Storage unavailable",
        EngineErrorKind::DataCorruption => "Data corruption",
        EngineErrorKind::Internal => "Internal error",
    }
}

const fn detail(kind: EngineErrorKind) -> &'static str {
    match kind {
        EngineErrorKind::InvalidArgument => "The request contains an invalid argument.",
        EngineErrorKind::NumericOutOfRange => "A numeric value is outside the supported range.",
        EngineErrorKind::InvalidTextEncoding => "A text value has an unsupported encoding.",
        EngineErrorKind::InvalidQuery => "The query could not be processed.",
        EngineErrorKind::Unsupported => "The requested operation is not supported.",
        EngineErrorKind::FailedPrecondition => "The operation cannot run in the current state.",
        EngineErrorKind::TransactionAborted => {
            "The transaction is aborted; roll it back before continuing."
        }
        EngineErrorKind::TypeMismatch => "A value has an incompatible type.",
        EngineErrorKind::ConstraintViolation => "A database constraint was violated.",
        EngineErrorKind::UniqueViolation => "A unique constraint was violated.",
        EngineErrorKind::NotNullViolation => "A not-null constraint was violated.",
        EngineErrorKind::ForeignKeyViolation => "A foreign-key constraint was violated.",
        EngineErrorKind::CheckViolation => "A check constraint was violated.",
        EngineErrorKind::PermissionDenied => "The operation is not permitted.",
        EngineErrorKind::ReadOnly => "The storage is read-only.",
        EngineErrorKind::Busy => "The database is busy; retry the operation later.",
        EngineErrorKind::Cancelled => "The operation was cancelled.",
        EngineErrorKind::DeadlineExceeded => "The operation exceeded its request deadline.",
        EngineErrorKind::LimitExceeded => "The request exceeds an engine limit.",
        EngineErrorKind::ShuttingDown => {
            "The server is shutting down and cannot accept the operation."
        }
        EngineErrorKind::StorageFull => "The storage has no available space.",
        EngineErrorKind::OutOfMemory => "The engine does not have enough memory.",
        EngineErrorKind::StorageUnavailable => "The storage is unavailable.",
        EngineErrorKind::DataCorruption => "Stored data failed an integrity check.",
        EngineErrorKind::Internal => "An internal engine error occurred.",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn every_engine_kind_has_the_exact_cross_protocol_contract() {
        let expected = [
            (
                EngineErrorKind::InvalidArgument,
                400,
                "22023",
                1210,
                "HY000",
            ),
            (
                EngineErrorKind::NumericOutOfRange,
                422,
                "22003",
                1690,
                "22003",
            ),
            (
                EngineErrorKind::InvalidTextEncoding,
                422,
                "22021",
                1366,
                "HY000",
            ),
            (EngineErrorKind::InvalidQuery, 422, "42000", 1105, "HY000"),
            (EngineErrorKind::Unsupported, 501, "0A000", 1235, "42000"),
            (
                EngineErrorKind::FailedPrecondition,
                409,
                "55000",
                1105,
                "HY000",
            ),
            (
                EngineErrorKind::TransactionAborted,
                409,
                "25P02",
                1105,
                "HY000",
            ),
            (EngineErrorKind::TypeMismatch, 422, "42804", 1366, "HY000"),
            (
                EngineErrorKind::ConstraintViolation,
                409,
                "23000",
                1105,
                "HY000",
            ),
            (
                EngineErrorKind::UniqueViolation,
                409,
                "23505",
                1062,
                "23000",
            ),
            (
                EngineErrorKind::NotNullViolation,
                409,
                "23502",
                1048,
                "23000",
            ),
            (
                EngineErrorKind::ForeignKeyViolation,
                409,
                "23503",
                1105,
                "HY000",
            ),
            (EngineErrorKind::CheckViolation, 409, "23514", 3819, "HY000"),
            (
                EngineErrorKind::PermissionDenied,
                403,
                "42501",
                1227,
                "42000",
            ),
            (EngineErrorKind::ReadOnly, 403, "25006", 1290, "HY000"),
            (EngineErrorKind::Busy, 503, "55P03", 1205, "HY000"),
            (EngineErrorKind::Cancelled, 500, "57014", 1317, "70100"),
            (
                EngineErrorKind::DeadlineExceeded,
                504,
                "57014",
                3024,
                "HY000",
            ),
            (EngineErrorKind::LimitExceeded, 422, "54000", 1105, "HY000"),
            (EngineErrorKind::ShuttingDown, 503, "57P01", 1053, "08S01"),
            (EngineErrorKind::StorageFull, 507, "53100", 1114, "HY000"),
            (EngineErrorKind::OutOfMemory, 503, "53200", 1037, "HY001"),
            (
                EngineErrorKind::StorageUnavailable,
                503,
                "58030",
                1105,
                "HY000",
            ),
            (EngineErrorKind::DataCorruption, 500, "XX001", 1105, "HY000"),
            (EngineErrorKind::Internal, 500, "XX000", 1105, "HY000"),
        ];

        assert_eq!(expected.map(|row| row.0).as_slice(), EngineErrorKind::ALL);
        for (kind, status, postgres, mysql_number, mysql_state) in expected {
            let http = http_error(kind);
            let pg = postgres_error(kind);
            let mysql = mysql_error(kind);

            assert_eq!(http.status, status, "{} HTTP status", kind.code());
            assert_eq!(pg.sqlstate, postgres, "{} PostgreSQL SQLSTATE", kind.code());
            assert_eq!(
                mysql.error_number,
                mysql_number,
                "{} MySQL errno",
                kind.code()
            );
            assert_eq!(
                mysql.sqlstate,
                mysql_state,
                "{} MySQL SQLSTATE",
                kind.code()
            );
            assert_eq!(pg.message, http.detail);
            assert_eq!(mysql.message, http.detail);
            assert_eq!(pg.sqlstate.len(), 5);
            assert_eq!(mysql.sqlstate.len(), 5);
            assert!(!http.title.is_empty());
            assert!(!http.detail.is_empty());
            assert!(
                http.problem_type
                    .starts_with("https://github.com/schapman1974/briskdb/")
            );
            assert!(http.problem_type.ends_with(&kind.code().replace('_', "-")));
        }
    }

    #[test]
    fn every_problem_type_is_unique() {
        let problem_types = EngineErrorKind::ALL
            .iter()
            .copied()
            .map(|kind| http_error(kind).problem_type)
            .collect::<Vec<_>>();
        assert_eq!(
            problem_types.iter().copied().collect::<HashSet<_>>().len(),
            problem_types.len()
        );
    }

    #[test]
    fn unsupported_rollout_gate_has_fixed_cross_protocol_encodings() {
        let http = http_error(EngineErrorKind::Unsupported);
        let postgres = postgres_error(EngineErrorKind::Unsupported);
        let mysql = mysql_error(EngineErrorKind::Unsupported);

        assert_eq!(http.status, 501);
        assert_eq!(http.title, "Unsupported operation");
        assert_eq!(http.detail, "The requested operation is not supported.");
        assert_eq!(postgres.sqlstate, "0A000");
        assert_eq!(mysql.error_number, 1235);
        assert_eq!(mysql.sqlstate, "42000");
        assert_eq!(postgres.message, http.detail);
        assert_eq!(mysql.message, http.detail);
    }

    #[test]
    fn public_error_documentation_matches_the_executable_mapping_table() {
        let documentation = include_str!("../../docs/ERRORS.md");

        for &kind in EngineErrorKind::ALL {
            let http = http_error(kind);
            let postgres = postgres_error(kind);
            let mysql = mysql_error(kind);
            let anchor = kind.code().replace('_', "-");
            let expected_row = format!(
                "| <a id=\"{anchor}\"></a>`{kind:?}` | `{}` | {} | {} | {} | `{}` | {} | `{}` | {} |",
                kind.code(),
                http.status,
                http.title,
                http.detail,
                postgres.sqlstate,
                mysql.error_number,
                mysql.sqlstate,
                if kind.is_retryable() { "Yes" } else { "No" },
            );
            assert!(
                documentation.lines().any(|line| line == expected_row),
                "missing or stale documentation row for {}",
                kind.code()
            );
        }
    }
}
