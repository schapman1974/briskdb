use std::time::Duration;

use briskdb::core::{ContentionJitter, ContentionPolicy as NativePolicy};
use pyo3::prelude::*;

use crate::error::{NativeError, invalid_value};

/// Immutable Python value backed by the same validated policy as Rust.
#[pyclass(module = "briskdb._briskdb", frozen, skip_from_py_object)]
#[derive(Clone, Debug)]
pub(super) struct ContentionPolicy {
    inner: NativePolicy,
}

impl ContentionPolicy {
    pub(super) fn native(&self) -> NativePolicy {
        self.inner
    }

    pub(super) fn representation(&self) -> String {
        if self.is_fail_fast() {
            return "ContentionPolicy.fail_fast()".to_owned();
        }
        format!(
            "ContentionPolicy(initial_delay_ms={}, max_delay_ms={}, multiplier={}, jitter={:?}, max_retries={}, max_elapsed_ms={})",
            self.initial_delay_ms(),
            self.max_delay_ms(),
            self.multiplier(),
            self.jitter(),
            self.max_retries(),
            self.max_elapsed_ms(),
        )
    }
}

#[pymethods]
impl ContentionPolicy {
    #[new]
    #[pyo3(signature = (*, initial_delay_ms, max_delay_ms, multiplier, jitter, max_retries, max_elapsed_ms))]
    fn new(
        initial_delay_ms: u64,
        max_delay_ms: u64,
        multiplier: u32,
        jitter: &str,
        max_retries: u32,
        max_elapsed_ms: u64,
    ) -> PyResult<Self> {
        let jitter = match jitter {
            "none" => ContentionJitter::None,
            "full" => ContentionJitter::Full,
            _ => return Err(invalid_value("contention jitter must be 'none' or 'full'")),
        };
        let inner = NativePolicy::new(
            Duration::from_millis(initial_delay_ms),
            Duration::from_millis(max_delay_ms),
            multiplier,
            jitter,
            max_retries,
            Duration::from_millis(max_elapsed_ms),
        )
        .map_err(NativeError::from)?;
        Ok(Self { inner })
    }

    #[staticmethod]
    fn fail_fast() -> Self {
        Self {
            inner: NativePolicy::fail_fast(),
        }
    }

    #[getter]
    fn initial_delay_ms(&self) -> u64 {
        self.inner.initial_delay().as_millis() as u64
    }
    #[getter]
    fn max_delay_ms(&self) -> u64 {
        self.inner.max_delay().as_millis() as u64
    }
    #[getter]
    fn multiplier(&self) -> u32 {
        self.inner.multiplier()
    }
    #[getter]
    fn jitter(&self) -> &'static str {
        match self.inner.jitter() {
            ContentionJitter::None => "none",
            ContentionJitter::Full => "full",
        }
    }
    #[getter]
    fn max_retries(&self) -> u32 {
        self.inner.max_retries()
    }
    #[getter]
    fn max_elapsed_ms(&self) -> u64 {
        self.inner.max_elapsed().as_millis() as u64
    }
    #[getter]
    fn is_fail_fast(&self) -> bool {
        self.inner.max_retries() == 0
    }

    fn __repr__(&self) -> String {
        self.representation()
    }
}
