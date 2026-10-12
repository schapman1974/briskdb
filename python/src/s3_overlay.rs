//! Private JSON transport; public Python API lives in briskdb.s3_overlay.
use crate::error::{NativeError, run_native};
use briskdb::s3_overlay::{Cell, Config, Database, OpenOptions, RetryOptions, Row, UpdateRequest};
use pyo3::prelude::*;
use std::{collections::BTreeMap, sync::Mutex};

fn json_error(error: serde_json::Error) -> NativeError {
    briskdb::EngineError::new(briskdb::EngineErrorKind::InvalidArgument, error.to_string()).into()
}

#[pyclass(module = "briskdb._briskdb")]
pub(crate) struct S3OverlayDatabase {
    inner: Mutex<Option<Database>>,
}

#[pymethods]
impl S3OverlayDatabase {
    #[new]
    #[pyo3(signature = (root, options_json="{}"))]
    fn open(py: Python<'_>, root: String, options_json: &str) -> PyResult<Self> {
        let options: OpenOptions = serde_json::from_str(options_json).map_err(json_error)?;
        run_native(py, || {
            Ok(Self {
                inner: Mutex::new(Some(Database::open_s3_with_options(root, options)?)),
            })
        })
    }

    #[staticmethod]
    #[pyo3(signature = (root, config_json, seed_json, options_json="{}"))]
    fn create(
        py: Python<'_>,
        root: String,
        config_json: String,
        seed_json: String,
        options_json: &str,
    ) -> PyResult<Self> {
        let options: OpenOptions = serde_json::from_str(options_json).map_err(json_error)?;
        run_native(py, || {
            let config: Config = serde_json::from_str(&config_json).map_err(json_error)?;
            let seed: BTreeMap<String, Vec<Row>> =
                serde_json::from_str(&seed_json).map_err(json_error)?;
            Ok(Self {
                inner: Mutex::new(Some(Database::create_s3_with_options(
                    root, config, seed, options,
                )?)),
            })
        })
    }

    fn query(&self, py: Python<'_>, sql: String, params_json: String) -> PyResult<String> {
        run_native(py, || {
            let params: Vec<Cell> = serde_json::from_str(&params_json).map_err(json_error)?;
            let mut guard = self.inner.lock()?;
            let db = guard.as_mut().ok_or(NativeError::Closed("S3 overlay"))?;
            serde_json::to_string(&db.query(&sql, &params)?).map_err(json_error)
        })
    }

    fn execute(&self, py: Python<'_>, sql: String, params_json: String) -> PyResult<String> {
        run_native(py, || {
            let params: Vec<Cell> = serde_json::from_str(&params_json).map_err(json_error)?;
            let mut guard = self.inner.lock()?;
            let db = guard.as_mut().ok_or(NativeError::Closed("S3 overlay"))?;
            serde_json::to_string(&db.execute(&sql, &params)?).map_err(json_error)
        })
    }

    #[pyo3(signature = (request_json, retry_json="{}"))]
    fn update(&self, py: Python<'_>, request_json: String, retry_json: &str) -> PyResult<String> {
        let request: UpdateRequest = serde_json::from_str(&request_json).map_err(json_error)?;
        let options: RetryOptions = serde_json::from_str(retry_json).map_err(json_error)?;
        run_native(py, || {
            let mut guard = self.inner.lock()?;
            let db = guard.as_mut().ok_or(NativeError::Closed("S3 overlay"))?;
            serde_json::to_string(&db.update(&request, options)?).map_err(json_error)
        })
    }

    fn update_target(&self, py: Python<'_>, request_json: String) -> PyResult<String> {
        let request: UpdateRequest = serde_json::from_str(&request_json).map_err(json_error)?;
        run_native(py, || {
            let guard = self.inner.lock()?;
            let db = guard.as_ref().ok_or(NativeError::Closed("S3 overlay"))?;
            serde_json::to_string(&db.update_target(&request)?).map_err(json_error)
        })
    }

    #[pyo3(signature = (operation_id, timeout_ms=1000))]
    fn update_status(
        &self,
        py: Python<'_>,
        operation_id: String,
        timeout_ms: u64,
    ) -> PyResult<String> {
        run_native(py, || {
            let guard = self.inner.lock()?;
            let db = guard.as_ref().ok_or(NativeError::Closed("S3 overlay"))?;
            serde_json::to_string(&db.update_status(&operation_id, timeout_ms)?).map_err(json_error)
        })
    }

    fn set_parquet_pruning(&self, py: Python<'_>, enabled: bool) -> PyResult<()> {
        run_native(py, || {
            let mut guard = self.inner.lock()?;
            let db = guard.as_mut().ok_or(NativeError::Closed("S3 overlay"))?;
            db.set_parquet_pruning(enabled);
            Ok(())
        })
    }

    fn read_stats(&self, py: Python<'_>) -> PyResult<String> {
        run_native(py, || {
            let guard = self.inner.lock()?;
            let db = guard.as_ref().ok_or(NativeError::Closed("S3 overlay"))?;
            serde_json::to_string(db.read_stats()).map_err(json_error)
        })
    }

    fn open_stats(&self, py: Python<'_>) -> PyResult<String> {
        run_native(py, || {
            let guard = self.inner.lock()?;
            let db = guard.as_ref().ok_or(NativeError::Closed("S3 overlay"))?;
            serde_json::to_string(&db.open_stats()).map_err(json_error)
        })
    }

    fn settings(&self, py: Python<'_>) -> PyResult<String> {
        run_native(py, || {
            let guard = self.inner.lock()?;
            let db = guard.as_ref().ok_or(NativeError::Closed("S3 overlay"))?;
            Ok(
                serde_json::json!({"storage_mode":"s3-overlay", "metadata_backend":"isam",
                "data_backend":"sqlite", "write_backend":"s3-parquet",
                "config":db.config(), "options":db.options()})
                .to_string(),
            )
        })
    }

    #[cfg(feature = "experimental-duckdb-reader")]
    fn query_duckdb(
        &self,
        py: Python<'_>,
        table: String,
        key_json: String,
        sql: String,
        params_json: String,
        options_json: String,
    ) -> PyResult<String> {
        run_native(py, || {
            let key: Cell = serde_json::from_str(&key_json).map_err(json_error)?;
            let params: Vec<Cell> = serde_json::from_str(&params_json).map_err(json_error)?;
            let options = serde_json::from_str(&options_json).map_err(json_error)?;
            let mut guard = self.inner.lock()?;
            let db = guard.as_mut().ok_or(NativeError::Closed("S3 overlay"))?;
            serde_json::to_string(
                &db.query_partition_duckdb(&table, &key, &sql, &params, &options)?,
            )
            .map_err(json_error)
        })
    }

    #[pyo3(signature = (table=None, partition=None))]
    fn compact(
        &self,
        py: Python<'_>,
        table: Option<String>,
        partition: Option<u16>,
    ) -> PyResult<String> {
        run_native(py, || {
            let mut guard = self.inner.lock()?;
            let db = guard.as_mut().ok_or(NativeError::Closed("S3 overlay"))?;
            match (table, partition) {
                (None, None) => serde_json::to_string(&db.compact_all()?).map_err(json_error),
                (Some(table), Some(partition)) => {
                    serde_json::to_string(&vec![db.compact(&table, partition)?]).map_err(json_error)
                }
                _ => Err(briskdb::EngineError::new(
                    briskdb::EngineErrorKind::InvalidArgument,
                    "supply both table and partition or neither",
                )
                .into()),
            }
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        run_native(py, || {
            self.inner.lock()?.take();
            Ok(())
        })
    }
}
