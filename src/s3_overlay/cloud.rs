use super::{Result, storage_error};
use bytes::Bytes;
use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion, path::Path};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub(crate) struct Cloud {
    pub store: Arc<dyn ObjectStore>,
    runtime: tokio::runtime::Runtime,
    deadline: Mutex<Option<Instant>>,
}

impl Cloud {
    pub fn new(store: Arc<dyn ObjectStore>) -> Result<Self> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(super::invalid(
                "the synchronous overlay API must run on an OS thread outside Tokio",
            ));
        }
        Ok(Self {
            store,
            deadline: Mutex::new(None),
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(storage_error)?,
        })
    }

    pub(super) fn set_deadline(&self, deadline: Option<Instant>) {
        *self.deadline.lock().expect("overlay deadline poisoned") = deadline;
    }

    pub(super) fn check_deadline(&self) -> Result<()> {
        if self
            .deadline
            .lock()
            .expect("overlay deadline poisoned")
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(crate::EngineError::deadline_exceeded(
                "overlay update deadline exceeded",
            ));
        }
        Ok(())
    }

    pub(super) fn pause(&self, duration: Duration) -> Result<()> {
        self.check_deadline()?;
        let deadline = *self.deadline.lock().expect("overlay deadline poisoned");
        std::thread::sleep(deadline.map_or(duration, |end| {
            duration.min(end.saturating_duration_since(Instant::now()))
        }));
        self.check_deadline()
    }

    async fn bounded_read<T>(
        &self,
        future: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let deadline = *self.deadline.lock().expect("overlay deadline poisoned");
        match deadline {
            None => future.await,
            Some(deadline) => tokio::time::timeout_at(deadline.into(), future)
                .await
                .map_err(|_| {
                    crate::EngineError::deadline_exceeded("overlay read deadline exceeded")
                })?,
        }
    }

    async fn bounded<T>(
        &self,
        future: impl std::future::Future<Output = object_store::Result<T>>,
    ) -> object_store::Result<T> {
        let deadline = *self.deadline.lock().expect("overlay deadline poisoned");
        match deadline {
            None => future.await,
            Some(deadline) => tokio::time::timeout_at(deadline.into(), future)
                .await
                .map_err(|_| object_store::Error::Generic {
                    store: "briskdb update deadline",
                    source: std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "overlay update deadline exceeded; a submitted write may have committed",
                    )
                    .into(),
                })?,
        }
    }

    pub(super) fn get_optional(&self, key: &str, max_bytes: u64) -> Result<Option<Bytes>> {
        self.check_deadline()?;
        self.runtime.block_on(self.bounded_read(async {
            let answer = match self.store.get(&Path::from(key)).await {
                Ok(answer) => answer,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(storage_error(error)),
            };
            if answer.meta.size > max_bytes {
                return Err(super::limit("S3 object exceeds configured bound"));
            }
            let bytes = answer.bytes().await.map_err(storage_error)?;
            if bytes.len() as u64 > max_bytes {
                return Err(super::limit("S3 response exceeds configured bound"));
            }
            Ok(Some(bytes))
        }))
    }

    pub fn get(&self, key: &str, max_bytes: u64) -> Result<(Bytes, UpdateVersion)> {
        self.check_deadline()?;
        self.runtime.block_on(self.bounded_read(async {
            let answer = self
                .store
                .get(&Path::from(key))
                .await
                .map_err(storage_error)?;
            if answer.meta.size > max_bytes {
                return Err(super::limit("S3 object exceeds configured bound"));
            }
            let version = UpdateVersion {
                e_tag: answer.meta.e_tag.clone(),
                version: answer.meta.version.clone(),
            };
            if version.e_tag.is_none() {
                return Err(super::corrupt(
                    "object store does not return conditional-write ETags",
                ));
            }
            let bytes = answer.bytes().await.map_err(storage_error)?;
            if bytes.len() as u64 > max_bytes {
                return Err(super::limit("S3 response exceeds configured bound"));
            }
            Ok((bytes, version))
        }))
    }

    pub fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> object_store::Result<()> {
        self.runtime.block_on(self.bounded(async {
            self.store
                .put_opts(
                    &Path::from(key),
                    bytes.into(),
                    PutOptions {
                        mode,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        }))
    }
}

/// AWS credentials come from the standard provider chain (including Lambda's
/// temporary role credentials); never persist secrets in the ISAM catalog.
pub fn s3_store(bucket: &str, region: &str) -> Result<Arc<dyn ObjectStore>> {
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_allow_http(false)
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .with_client_options(
            object_store::ClientOptions::new()
                .with_connect_timeout(Duration::from_secs(3))
                .with_timeout(Duration::from_secs(10)),
        )
        .with_retry(object_store::RetryConfig {
            max_retries: 2,
            retry_timeout: Duration::from_secs(20),
            ..Default::default()
        })
        .build()
        .map_err(storage_error)?;
    Ok(Arc::new(store))
}

pub(crate) fn conflict(error: &object_store::Error) -> bool {
    matches!(
        error,
        object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. }
    )
}
