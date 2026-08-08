//! Context-aware conversion from SQLite and filesystem failures.
//!
//! SQLite uses `SQLITE_ERROR` for several unrelated caller and engine failures,
//! so classification must know whether a failing statement came from a client
//! or from BriskDB's own storage implementation. Error messages are never
//! parsed because they are not a stable API.

use std::io;

use rusqlite::{Error, ffi};

use crate::core::{EngineError, EngineErrorKind};

#[derive(Clone, Copy)]
enum Context {
    Statement,
    Storage,
}

pub(crate) fn statement(error: Error) -> EngineError {
    convert(error, Context::Statement)
}

pub(crate) fn storage(error: Error) -> EngineError {
    convert(error, Context::Storage)
}

pub(crate) fn storage_io(error: io::Error, diagnostic: impl Into<String>) -> EngineError {
    let kind = match error.kind() {
        io::ErrorKind::PermissionDenied => EngineErrorKind::PermissionDenied,
        io::ErrorKind::ReadOnlyFilesystem => EngineErrorKind::ReadOnly,
        io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded => EngineErrorKind::StorageFull,
        io::ErrorKind::OutOfMemory => EngineErrorKind::OutOfMemory,
        io::ErrorKind::FileTooLarge => EngineErrorKind::LimitExceeded,
        io::ErrorKind::AlreadyExists
        | io::ErrorKind::InvalidInput
        | io::ErrorKind::InvalidData
        | io::ErrorKind::NotADirectory
        | io::ErrorKind::IsADirectory => EngineErrorKind::FailedPrecondition,
        io::ErrorKind::Unsupported => EngineErrorKind::Unsupported,
        _ => EngineErrorKind::StorageUnavailable,
    };
    EngineError::from_source(kind, diagnostic, error)
}

fn convert(error: Error, context: Context) -> EngineError {
    let kind = classify(&error, context);
    let diagnostic = error.to_string();
    EngineError::from_source(kind, diagnostic, error)
}

fn classify(error: &Error, context: Context) -> EngineErrorKind {
    match error {
        Error::SqliteFailure(error, _) | Error::SqlInputError { error, .. } => {
            classify_sqlite_failure(*error, context)
        }
        Error::InvalidParameterName(_) | Error::InvalidParameterCount(_, _)
            if matches!(context, Context::Statement) =>
        {
            EngineErrorKind::InvalidArgument
        }
        Error::IntegralValueOutOfRange(_, _) if matches!(context, Context::Statement) => {
            EngineErrorKind::NumericOutOfRange
        }
        Error::Utf8Error(_, _) if matches!(context, Context::Statement) => {
            EngineErrorKind::InvalidTextEncoding
        }
        Error::IntegralValueOutOfRange(_, _) | Error::Utf8Error(_, _) => {
            EngineErrorKind::DataCorruption
        }
        Error::FromSqlConversionFailure(_, _, _) | Error::InvalidColumnType(_, _, _) => {
            match context {
                Context::Statement => EngineErrorKind::TypeMismatch,
                Context::Storage => EngineErrorKind::DataCorruption,
            }
        }
        Error::ToSqlConversionFailure(_) if matches!(context, Context::Statement) => {
            EngineErrorKind::InvalidArgument
        }
        Error::NulError(_)
        | Error::ExecuteReturnedResults
        | Error::InvalidQuery
        | Error::MultipleStatement
            if matches!(context, Context::Statement) =>
        {
            EngineErrorKind::InvalidQuery
        }
        Error::InvalidPath(_) => EngineErrorKind::FailedPrecondition,
        _ => EngineErrorKind::Internal,
    }
}

fn classify_sqlite_failure(error: ffi::Error, context: Context) -> EngineErrorKind {
    use ffi::ErrorCode;

    match error.code {
        ErrorCode::ConstraintViolation if matches!(context, Context::Statement) => {
            classify_constraint(error.extended_code)
        }
        ErrorCode::ConstraintViolation => EngineErrorKind::Internal,
        ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked | ErrorCode::SchemaChanged => {
            EngineErrorKind::Busy
        }
        ErrorCode::OperationAborted | ErrorCode::OperationInterrupted => EngineErrorKind::Cancelled,
        ErrorCode::PermissionDenied | ErrorCode::AuthorizationForStatementDenied => {
            EngineErrorKind::PermissionDenied
        }
        ErrorCode::ReadOnly => EngineErrorKind::ReadOnly,
        ErrorCode::OutOfMemory => EngineErrorKind::OutOfMemory,
        ErrorCode::DiskFull => EngineErrorKind::StorageFull,
        ErrorCode::TooBig => EngineErrorKind::LimitExceeded,
        ErrorCode::SystemIoFailure
        | ErrorCode::CannotOpen
        | ErrorCode::FileLockingProtocolFailed => EngineErrorKind::StorageUnavailable,
        ErrorCode::NoLargeFileSupport => EngineErrorKind::Unsupported,
        ErrorCode::NotFound => EngineErrorKind::Internal,
        ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase => EngineErrorKind::DataCorruption,
        ErrorCode::TypeMismatch if matches!(context, Context::Statement) => {
            EngineErrorKind::TypeMismatch
        }
        ErrorCode::TypeMismatch => EngineErrorKind::DataCorruption,
        ErrorCode::ParameterOutOfRange if matches!(context, Context::Statement) => {
            EngineErrorKind::InvalidArgument
        }
        ErrorCode::ParameterOutOfRange => EngineErrorKind::Internal,
        ErrorCode::Unknown if matches!(context, Context::Statement) => {
            EngineErrorKind::InvalidQuery
        }
        ErrorCode::InternalMalfunction | ErrorCode::ApiMisuse | ErrorCode::Unknown => {
            EngineErrorKind::Internal
        }
        _ => EngineErrorKind::Internal,
    }
}

fn classify_constraint(extended_code: i32) -> EngineErrorKind {
    match extended_code {
        ffi::SQLITE_CONSTRAINT_UNIQUE
        | ffi::SQLITE_CONSTRAINT_PRIMARYKEY
        | ffi::SQLITE_CONSTRAINT_ROWID => EngineErrorKind::UniqueViolation,
        ffi::SQLITE_CONSTRAINT_NOTNULL => EngineErrorKind::NotNullViolation,
        ffi::SQLITE_CONSTRAINT_FOREIGNKEY => EngineErrorKind::ForeignKeyViolation,
        ffi::SQLITE_CONSTRAINT_CHECK => EngineErrorKind::CheckViolation,
        ffi::SQLITE_CONSTRAINT_DATATYPE => EngineErrorKind::TypeMismatch,
        _ => EngineErrorKind::ConstraintViolation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sqlite_failure(result_code: i32, message: &str) -> Error {
        Error::SqliteFailure(ffi::Error::new(result_code), Some(message.to_owned()))
    }

    #[test]
    fn constraint_extensions_have_precise_message_independent_kinds() {
        let cases = [
            (ffi::SQLITE_CONSTRAINT, EngineErrorKind::ConstraintViolation),
            (
                ffi::SQLITE_CONSTRAINT_UNIQUE,
                EngineErrorKind::UniqueViolation,
            ),
            (
                ffi::SQLITE_CONSTRAINT_PRIMARYKEY,
                EngineErrorKind::UniqueViolation,
            ),
            (
                ffi::SQLITE_CONSTRAINT_ROWID,
                EngineErrorKind::UniqueViolation,
            ),
            (
                ffi::SQLITE_CONSTRAINT_NOTNULL,
                EngineErrorKind::NotNullViolation,
            ),
            (
                ffi::SQLITE_CONSTRAINT_FOREIGNKEY,
                EngineErrorKind::ForeignKeyViolation,
            ),
            (
                ffi::SQLITE_CONSTRAINT_CHECK,
                EngineErrorKind::CheckViolation,
            ),
            (
                ffi::SQLITE_CONSTRAINT_DATATYPE,
                EngineErrorKind::TypeMismatch,
            ),
        ];

        for (code, expected) in cases {
            let first = statement(sqlite_failure(code, "first localized message"));
            let second = statement(sqlite_failure(code, "entirely different text"));
            assert_eq!(first.kind(), expected);
            assert_eq!(second.kind(), expected);
        }
    }

    #[test]
    fn sqlite_primary_codes_cover_operational_and_recovery_failures() {
        let cases = [
            (ffi::SQLITE_BUSY, EngineErrorKind::Busy),
            (ffi::SQLITE_LOCKED, EngineErrorKind::Busy),
            (ffi::SQLITE_SCHEMA, EngineErrorKind::Busy),
            (ffi::SQLITE_INTERRUPT, EngineErrorKind::Cancelled),
            (ffi::SQLITE_ABORT, EngineErrorKind::Cancelled),
            (ffi::SQLITE_PERM, EngineErrorKind::PermissionDenied),
            (ffi::SQLITE_AUTH, EngineErrorKind::PermissionDenied),
            (ffi::SQLITE_READONLY, EngineErrorKind::ReadOnly),
            (ffi::SQLITE_NOMEM, EngineErrorKind::OutOfMemory),
            (ffi::SQLITE_FULL, EngineErrorKind::StorageFull),
            (ffi::SQLITE_TOOBIG, EngineErrorKind::LimitExceeded),
            (ffi::SQLITE_IOERR, EngineErrorKind::StorageUnavailable),
            (ffi::SQLITE_CANTOPEN, EngineErrorKind::StorageUnavailable),
            (ffi::SQLITE_PROTOCOL, EngineErrorKind::StorageUnavailable),
            (ffi::SQLITE_NOLFS, EngineErrorKind::Unsupported),
            (ffi::SQLITE_NOTFOUND, EngineErrorKind::Internal),
            (ffi::SQLITE_CORRUPT, EngineErrorKind::DataCorruption),
            (ffi::SQLITE_NOTADB, EngineErrorKind::DataCorruption),
            (ffi::SQLITE_MISMATCH, EngineErrorKind::TypeMismatch),
            (ffi::SQLITE_RANGE, EngineErrorKind::InvalidArgument),
            (ffi::SQLITE_INTERNAL, EngineErrorKind::Internal),
            (ffi::SQLITE_MISUSE, EngineErrorKind::Internal),
        ];

        for (code, expected) in cases {
            for message in [
                "contradictory unique constraint text",
                "完全に異なるメッセージ",
            ] {
                assert_eq!(statement(sqlite_failure(code, message)).kind(), expected);
            }
        }
    }

    #[test]
    fn generic_sqlite_error_depends_on_the_trusted_statement_boundary() {
        for message in ["UNIQUE constraint failed", "構文とは無関係なメッセージ"] {
            let user_error = statement(sqlite_failure(ffi::SQLITE_ERROR, message));
            let storage_error = storage(sqlite_failure(ffi::SQLITE_ERROR, message));

            assert_eq!(user_error.kind(), EngineErrorKind::InvalidQuery);
            assert_eq!(storage_error.kind(), EngineErrorKind::Internal);
        }
    }

    #[test]
    fn client_constraint_codes_are_not_applied_to_internal_storage_sql() {
        let user_error = statement(sqlite_failure(ffi::SQLITE_CONSTRAINT_UNIQUE, "not parsed"));
        let storage_error = storage(sqlite_failure(ffi::SQLITE_CONSTRAINT_UNIQUE, "not parsed"));

        assert_eq!(user_error.kind(), EngineErrorKind::UniqueViolation);
        assert_eq!(storage_error.kind(), EngineErrorKind::Internal);
    }

    #[test]
    fn filesystem_errors_use_their_precise_available_kinds() {
        let cases = [
            (
                io::ErrorKind::PermissionDenied,
                EngineErrorKind::PermissionDenied,
            ),
            (io::ErrorKind::ReadOnlyFilesystem, EngineErrorKind::ReadOnly),
            (io::ErrorKind::StorageFull, EngineErrorKind::StorageFull),
            (io::ErrorKind::QuotaExceeded, EngineErrorKind::StorageFull),
            (io::ErrorKind::OutOfMemory, EngineErrorKind::OutOfMemory),
            (io::ErrorKind::FileTooLarge, EngineErrorKind::LimitExceeded),
            (
                io::ErrorKind::AlreadyExists,
                EngineErrorKind::FailedPrecondition,
            ),
            (
                io::ErrorKind::InvalidInput,
                EngineErrorKind::FailedPrecondition,
            ),
            (
                io::ErrorKind::InvalidData,
                EngineErrorKind::FailedPrecondition,
            ),
            (
                io::ErrorKind::NotADirectory,
                EngineErrorKind::FailedPrecondition,
            ),
            (
                io::ErrorKind::IsADirectory,
                EngineErrorKind::FailedPrecondition,
            ),
            (io::ErrorKind::Unsupported, EngineErrorKind::Unsupported),
            (io::ErrorKind::NotFound, EngineErrorKind::StorageUnavailable),
        ];

        for (io_kind, expected) in cases {
            let error = storage_io(io::Error::from(io_kind), "create directory");
            assert_eq!(error.kind(), expected, "{io_kind:?}");
        }
    }

    #[test]
    fn invalid_sqlite_paths_are_failed_preconditions() {
        let error = storage(Error::InvalidPath("invalid".into()));
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
    }
}
