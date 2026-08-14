use std::{panic::AssertUnwindSafe, sync::PoisonError};

use briskdb::EngineError;
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    marker::Ungil,
    prelude::*,
};

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
            Self::Engine(error) => {
                PyRuntimeError::new_err(format!("BriskDB {}: {}", error.code(), error.diagnostic()))
            }
            Self::Closed(handle) => PyRuntimeError::new_err(format!("BriskDB {handle} is closed")),
            Self::Runtime(message) => PyRuntimeError::new_err(message),
            Self::Panic => PyRuntimeError::new_err("BriskDB native operation panicked"),
        }
    }
}

impl From<NativeError> for PyErr {
    fn from(error: NativeError) -> Self {
        error.into_python()
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
    PyValueError::new_err(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

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
