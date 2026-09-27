//! TLS authenticates the server, not Mongo users. Loopback remains mandatory.

use std::{
    io,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio_rustls::TlsAcceptor;

const MAX_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

/// Server certificate/key for an encrypted, still-anonymous loopback listener.
/// No certificate-verification bypass, client authentication or remote-bind
/// permission is implied. Requires the opt-in `mongo-tls` feature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MongoTlsConfig {
    certificate: PathBuf,
    private_key: PathBuf,
    handshake_timeout: Duration,
}

impl MongoTlsConfig {
    pub fn new(certificate: impl Into<PathBuf>, private_key: impl Into<PathBuf>) -> Self {
        Self {
            certificate: certificate.into(),
            private_key: private_key.into(),
            handshake_timeout: MAX_HANDSHAKE_TIMEOUT,
        }
    }

    /// Narrow the handshake timeout to a positive duration no greater than 15s.
    /// Handshakes also consume the listener's ordinary finite connection slots.
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> io::Result<Self> {
        if timeout.is_zero() || timeout > MAX_HANDSHAKE_TIMEOUT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Mongo TLS handshake timeout must be positive and at most 15 seconds",
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

    pub(super) fn load(&self) -> io::Result<TlsAcceptor> {
        // Mongo clients use direct TLS and do not require a PostgreSQL ALPN.
        let identity = crate::protocol::tls::load_server_identity(
            &self.certificate,
            &self.private_key,
            "Mongo",
            &[],
        )?;
        Ok(TlsAcceptor::from(identity.config))
    }
}
