use std::{panic::AssertUnwindSafe, sync::PoisonError};

use briskdb::{EngineError, EngineErrorKind};
use pyo3::{create_exception, exceptions::PyException, marker::Ungil, prelude::*, types::PyModule};

create_exception!(briskdb._briskdb, BriskDBError, PyException);
create_exception!(briskdb._briskdb, DataError, BriskDBError);
create_exception!(briskdb._briskdb, OperationalError, BriskDBError);
create_exception!(briskdb._briskdb, IntegrityError, BriskDBError);
create_exception!(briskdb._briskdb, ProgrammingError, BriskDBError);

create_exception!(briskdb._briskdb, InvalidArgumentError, ProgrammingError);
create_exception!(briskdb._briskdb, NumericOutOfRangeError, DataError);
create_exception!(briskdb._briskdb, InvalidTextEncodingError, DataError);
create_exception!(briskdb._briskdb, InvalidQueryError, ProgrammingError);
create_exception!(briskdb._briskdb, UnsupportedError, ProgrammingError);
create_exception!(briskdb._briskdb, FailedPreconditionError, OperationalError);
create_exception!(briskdb._briskdb, TypeMismatchError, DataError);
create_exception!(briskdb._briskdb, ConstraintViolationError, IntegrityError);
create_exception!(
    briskdb._briskdb,
    UniqueViolationError,
    ConstraintViolationError
);
create_exception!(
    briskdb._briskdb,
    NotNullViolationError,
    ConstraintViolationError
);
create_exception!(
    briskdb._briskdb,
    ForeignKeyViolationError,
    ConstraintViolationError
);
create_exception!(
    briskdb._briskdb,
    CheckViolationError,
    ConstraintViolationError
);
create_exception!(briskdb._briskdb, PermissionDeniedError, OperationalError);
create_exception!(briskdb._briskdb, ReadOnlyError, OperationalError);
create_exception!(briskdb._briskdb, BusyError, OperationalError);
create_exception!(briskdb._briskdb, CancelledError, OperationalError);
create_exception!(briskdb._briskdb, DeadlineExceededError, OperationalError);
create_exception!(briskdb._briskdb, LimitExceededError, OperationalError);
create_exception!(briskdb._briskdb, ShuttingDownError, OperationalError);
create_exception!(briskdb._briskdb, StorageFullError, OperationalError);
create_exception!(briskdb._briskdb, OutOfMemoryError, OperationalError);
create_exception!(briskdb._briskdb, StorageUnavailableError, OperationalError);
create_exception!(briskdb._briskdb, DataCorruptionError, OperationalError);
create_exception!(briskdb._briskdb, InternalError, OperationalError);

pub(crate) type NativeResult<T> = Result<T, NativeError>;

#[derive(Debug)]
pub(crate) enum NativeError {
    Engine(EngineError),
    Closed(&'static str),
    Runtime(String),
    Panic,
}

impl From<EngineError> for NativeError {
    fn from(error: EngineError) -> Self {
        Self::Engine(error)
    }
}

impl<T> From<PoisonError<T>> for NativeError {
    fn from(_: PoisonError<T>) -> Self {
        Self::Runtime("a native handle lock was poisoned".to_owned())
    }
}

impl NativeError {
    fn into_python(self) -> PyErr {
        match self {
            Self::Engine(error) => engine_error_to_python(error),
            Self::Closed(handle) => {
                FailedPreconditionError::new_err(format!("BriskDB {handle} is closed"))
            }
            Self::Runtime(message) => InternalError::new_err(message),
            Self::Panic => InternalError::new_err("BriskDB native operation panicked"),
        }
    }
}

impl From<NativeError> for PyErr {
    fn from(error: NativeError) -> Self {
        error.into_python()
    }
}

fn engine_error_to_python(error: EngineError) -> PyErr {
    let diagnostic = error.diagnostic().to_owned();
    match error.kind() {
        EngineErrorKind::InvalidArgument => InvalidArgumentError::new_err(diagnostic),
        EngineErrorKind::NumericOutOfRange => NumericOutOfRangeError::new_err(diagnostic),
        EngineErrorKind::InvalidTextEncoding => InvalidTextEncodingError::new_err(diagnostic),
        EngineErrorKind::InvalidQuery => InvalidQueryError::new_err(diagnostic),
        EngineErrorKind::Unsupported => UnsupportedError::new_err(diagnostic),
        EngineErrorKind::FailedPrecondition => FailedPreconditionError::new_err(diagnostic),
        EngineErrorKind::TypeMismatch => TypeMismatchError::new_err(diagnostic),
        EngineErrorKind::ConstraintViolation => ConstraintViolationError::new_err(diagnostic),
        EngineErrorKind::UniqueViolation => UniqueViolationError::new_err(diagnostic),
        EngineErrorKind::NotNullViolation => NotNullViolationError::new_err(diagnostic),
        EngineErrorKind::ForeignKeyViolation => ForeignKeyViolationError::new_err(diagnostic),
        EngineErrorKind::CheckViolation => CheckViolationError::new_err(diagnostic),
        EngineErrorKind::PermissionDenied => PermissionDeniedError::new_err(diagnostic),
        EngineErrorKind::ReadOnly => ReadOnlyError::new_err(diagnostic),
        EngineErrorKind::Busy => BusyError::new_err(diagnostic),
        EngineErrorKind::Cancelled => CancelledError::new_err(diagnostic),
        EngineErrorKind::DeadlineExceeded => DeadlineExceededError::new_err(diagnostic),
        EngineErrorKind::LimitExceeded => LimitExceededError::new_err(diagnostic),
        EngineErrorKind::ShuttingDown => ShuttingDownError::new_err(diagnostic),
        EngineErrorKind::StorageFull => StorageFullError::new_err(diagnostic),
        EngineErrorKind::OutOfMemory => OutOfMemoryError::new_err(diagnostic),
        EngineErrorKind::StorageUnavailable => StorageUnavailableError::new_err(diagnostic),
        EngineErrorKind::DataCorruption => DataCorruptionError::new_err(diagnostic),
        EngineErrorKind::Internal => InternalError::new_err(diagnostic),
        _ => InternalError::new_err(diagnostic),
    }
}

pub(crate) fn run_native<T, F>(py: Python<'_>, operation: F) -> PyResult<T>
where
    T: Ungil + Send,
    F: Ungil + Send + FnOnce() -> NativeResult<T>,
{
    let outcome = py.detach(move || std::panic::catch_unwind(AssertUnwindSafe(operation)));
    match outcome {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.into_python()),
        Err(_) => Err(NativeError::Panic.into_python()),
    }
}

pub(crate) fn invalid_value(message: impl Into<String>) -> PyErr {
    InvalidArgumentError::new_err(message.into())
}

pub(crate) fn numeric_out_of_range(message: impl Into<String>) -> PyErr {
    NumericOutOfRangeError::new_err(message.into())
}

pub(crate) fn type_mismatch(message: impl Into<String>) -> PyErr {
    TypeMismatchError::new_err(message.into())
}

pub(crate) fn unsupported(message: impl Into<String>) -> PyErr {
    UnsupportedError::new_err(message.into())
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    macro_rules! add_base {
        ($name:ty) => {{
            let class = module.py().get_type::<$name>();
            class.setattr("code", "briskdb_error")?;
            class.setattr("retryable", false)?;
            module.add(stringify!($name), class)?;
        }};
    }
    macro_rules! add_kind {
        ($name:ty, $kind:expr) => {{
            let class = module.py().get_type::<$name>();
            class.setattr("code", $kind.code())?;
            class.setattr("retryable", $kind.is_retryable())?;
            module.add(stringify!($name), class)?;
        }};
    }

    add_base!(BriskDBError);
    add_base!(DataError);
    add_base!(OperationalError);
    add_base!(IntegrityError);
    add_base!(ProgrammingError);
    add_kind!(InvalidArgumentError, EngineErrorKind::InvalidArgument);
    add_kind!(NumericOutOfRangeError, EngineErrorKind::NumericOutOfRange);
    add_kind!(
        InvalidTextEncodingError,
        EngineErrorKind::InvalidTextEncoding
    );
    add_kind!(InvalidQueryError, EngineErrorKind::InvalidQuery);
    add_kind!(UnsupportedError, EngineErrorKind::Unsupported);
    add_kind!(FailedPreconditionError, EngineErrorKind::FailedPrecondition);
    add_kind!(TypeMismatchError, EngineErrorKind::TypeMismatch);
    add_kind!(
        ConstraintViolationError,
        EngineErrorKind::ConstraintViolation
    );
    add_kind!(UniqueViolationError, EngineErrorKind::UniqueViolation);
    add_kind!(NotNullViolationError, EngineErrorKind::NotNullViolation);
    add_kind!(
        ForeignKeyViolationError,
        EngineErrorKind::ForeignKeyViolation
    );
    add_kind!(CheckViolationError, EngineErrorKind::CheckViolation);
    add_kind!(PermissionDeniedError, EngineErrorKind::PermissionDenied);
    add_kind!(ReadOnlyError, EngineErrorKind::ReadOnly);
    add_kind!(BusyError, EngineErrorKind::Busy);
    add_kind!(CancelledError, EngineErrorKind::Cancelled);
    add_kind!(DeadlineExceededError, EngineErrorKind::DeadlineExceeded);
    add_kind!(LimitExceededError, EngineErrorKind::LimitExceeded);
    add_kind!(ShuttingDownError, EngineErrorKind::ShuttingDown);
    add_kind!(StorageFullError, EngineErrorKind::StorageFull);
    add_kind!(OutOfMemoryError, EngineErrorKind::OutOfMemory);
    add_kind!(StorageUnavailableError, EngineErrorKind::StorageUnavailable);
    add_kind!(DataCorruptionError, EngineErrorKind::DataCorruption);
    add_kind!(InternalError, EngineErrorKind::Internal);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_engine_kind_has_a_specific_python_exception() {
        Python::initialize();
        for kind in EngineErrorKind::ALL {
            let error = EngineError::new(*kind, "safe diagnostic");
            let python_error = engine_error_to_python(error);
            Python::attach(|py| {
                assert!(python_error.is_instance_of::<BriskDBError>(py));
                assert_eq!(python_error.value(py).to_string(), "safe diagnostic");
            });
        }
    }

    #[test]
    fn panic_boundary_returns_an_error_instead_of_unwinding() {
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| -> NativeResult<()> {
            panic!("test panic must not cross the binding boundary")
        }));
        let error = match result {
            Ok(result) => result.unwrap_err(),
            Err(_) => NativeError::Panic,
        };
        assert!(matches!(error, NativeError::Panic));
    }
}
