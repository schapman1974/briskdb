//! Shared server identity loading, independent of any wire protocol or user model.
//! Selecting this feature never opens a listener or relaxes bind/authentication policy.
#![cfg_attr(not(feature = "postgres"), allow(dead_code))]

use std::{io, path::Path, sync::Arc};

use tokio_rustls::rustls;

mod configuration;
pub(crate) use configuration::read_configuration_file;

const MAX_TLS_PEM_BYTES: u64 = 1_048_576;

pub(crate) struct LoadedTlsIdentity {
    pub config: Arc<rustls::ServerConfig>,
    /// The exact loaded certificate bytes, for protocol channel-binding setup.
    pub certificate_pem: Arc<[u8]>,
}

/// The caller chooses its ALPN and a static diagnostic label. Authentication,
/// listener policy, reload lifetime and handshake deadlines belong to the host.
pub(crate) fn load_server_identity(
    certificate: &Path,
    private_key: &Path,
    protocol: &'static str,
    alpn: &[&[u8]],
) -> io::Result<LoadedTlsIdentity> {
    let certificate_label = format!("{protocol} TLS certificate");
    let key_label = format!("{protocol} TLS private key");
    let certificate_pem =
        read_configuration_file(certificate, &certificate_label, MAX_TLS_PEM_BYTES, false)?;
    let certificates = rustls_pemfile::certs(&mut certificate_pem.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            contextual_io_error(
                error,
                format!(
                    "failed to parse {certificate_label} {}",
                    certificate.display()
                ),
            )
        })?;
    if certificates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{certificate_label} {} contains no certificates",
                certificate.display()
            ),
        ));
    }
    let key_pem = read_configuration_file(private_key, &key_label, MAX_TLS_PEM_BYTES, true)?;
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .map_err(|error| {
            contextual_io_error(
                error,
                format!("failed to parse {key_label} {}", private_key.display()),
            )
        })?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{key_label} {} contains no supported private key",
                    private_key.display()
                ),
            )
        })?;
    drop(key_pem);
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{protocol} TLS certificate and private key are invalid: {error}"),
            )
        })?;
    config.alpn_protocols = alpn.iter().map(|value| value.to_vec()).collect();
    Ok(LoadedTlsIdentity {
        config: Arc::new(config),
        certificate_pem: certificate_pem.as_slice().to_vec().into(),
    })
}

pub(crate) fn contextual_io_error(error: io::Error, message: String) -> io::Error {
    io::Error::new(error.kind(), format!("{message}: {error}"))
}

#[cfg(test)]
mod tests;
