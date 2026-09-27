//! Shared server identity loading, independent of any wire protocol or user model.
//! Selecting this feature never opens a listener or relaxes bind/authentication policy.
#![cfg_attr(not(feature = "postgres"), allow(dead_code))]

use std::{
    fs::{self, File},
    io::{self, BufReader},
    path::Path,
    sync::Arc,
};

use tokio_rustls::rustls;

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
    validate_regular_file(certificate, &certificate_label, MAX_TLS_PEM_BYTES)?;
    validate_regular_file(private_key, &key_label, MAX_TLS_PEM_BYTES)?;
    validate_private_file(private_key, &key_label)?;

    let certificate_pem = fs::read(certificate).map_err(|error| {
        contextual_io_error(
            error,
            format!(
                "failed to read {certificate_label} {}",
                certificate.display()
            ),
        )
    })?;
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
    let key_file = File::open(private_key).map_err(|error| {
        contextual_io_error(
            error,
            format!("failed to read {key_label} {}", private_key.display()),
        )
    })?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
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
        certificate_pem: certificate_pem.into(),
    })
}

pub(crate) fn contextual_io_error(error: io::Error, message: String) -> io::Error {
    io::Error::new(error.kind(), format!("{message}: {error}"))
}

pub(crate) fn validate_regular_file(
    path: &Path,
    label: &str,
    maximum_bytes: u64,
) -> io::Result<()> {
    let metadata = fs::metadata(path).map_err(|error| {
        contextual_io_error(
            error,
            format!("failed to inspect {label} {}", path.display()),
        )
    })?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{label} {} is not a regular file", path.display()),
        ));
    }
    if metadata.len() > maximum_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{label} {} exceeds the {maximum_bytes}-byte limit",
                path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn validate_private_file(path: &Path, label: &str) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::metadata(path).map_err(|error| {
        contextual_io_error(
            error,
            format!("failed to inspect {label} {}", path.display()),
        )
    })?;
    let mode = metadata.mode() & 0o777;
    if mode & 0o037 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{label} {} must not be group-writable or accessible by other users (mode is {mode:03o})",
                path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn validate_private_file(path: &Path, label: &str) -> io::Result<()> {
    fs::metadata(path).map_err(|error| {
        contextual_io_error(
            error,
            format!("failed to inspect {label} {}", path.display()),
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests;
