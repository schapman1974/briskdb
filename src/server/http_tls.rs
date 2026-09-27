//! TLS for the two loopback HTTP planes, without changing their authorization.

use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_rustls::TlsAcceptor;

const MAX_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
pub(super) const MAX_HTTP_CONNECTIONS_PER_PLANE: usize = 256;

/// Certificate/key for one HTTP/1.1 listener. Server identity only: data/admin
/// addresses remain loopback-only, and TLS does not authenticate HTTP callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpTlsConfig {
    certificate: PathBuf,
    private_key: PathBuf,
    handshake_timeout: Duration,
}

impl HttpTlsConfig {
    pub fn new(certificate: impl Into<PathBuf>, private_key: impl Into<PathBuf>) -> Self {
        Self {
            certificate: certificate.into(),
            private_key: private_key.into(),
            handshake_timeout: MAX_HANDSHAKE_TIMEOUT,
        }
    }

    /// Narrow, never remove or exceed, the positive 15-second handshake deadline.
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> io::Result<Self> {
        if timeout.is_zero() || timeout > MAX_HANDSHAKE_TIMEOUT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "HTTP TLS handshake timeout must be positive and at most 15 seconds",
            ));
        }
        self.handshake_timeout = timeout;
        Ok(self)
    }

    pub fn certificate(&self) -> &Path {
        &self.certificate
    }
    pub fn private_key(&self) -> &Path {
        &self.private_key
    }
    pub const fn handshake_timeout(&self) -> Duration {
        self.handshake_timeout
    }

    pub(super) fn load(self) -> io::Result<Arc<Loaded>> {
        let identity = crate::protocol::tls::load_server_identity(
            &self.certificate,
            &self.private_key,
            "HTTP",
            &[b"http/1.1"],
        )?;
        Ok(Arc::new(Loaded {
            acceptor: TlsAcceptor::from(identity.config),
            handshake_timeout: self.handshake_timeout,
        }))
    }
}

pub(super) struct Loaded {
    pub acceptor: TlsAcceptor,
    pub handshake_timeout: Duration,
}

impl std::fmt::Debug for Loaded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpTlsIdentity")
            .finish_non_exhaustive()
    }
}

#[derive(Default, Debug)]
pub(super) struct Planes {
    pub data: Option<Arc<Loaded>>,
    pub admin: Option<Arc<Loaded>>,
}

/// Keep independent finite admission for data/admin, including handshakes and
/// keep-alive requests. The permit stays with the connection until task exit.
#[derive(Debug)]
pub(super) struct Slots {
    data: Arc<Semaphore>,
    admin: Arc<Semaphore>,
}

impl Default for Slots {
    fn default() -> Self {
        Self::new(MAX_HTTP_CONNECTIONS_PER_PLANE)
    }
}

impl Slots {
    fn new(limit: usize) -> Self {
        Self {
            data: Arc::new(Semaphore::new(limit)),
            admin: Arc::new(Semaphore::new(limit)),
        }
    }

    pub fn admit(&self, admin: bool) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        let slots = if admin { &self.admin } else { &self.data };
        slots.clone().try_acquire_owned()
    }
}

#[cfg(test)]
mod tests;
