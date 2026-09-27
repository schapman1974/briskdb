//! Read the inspected descriptor, not a path inspected before opening it.

use std::{
    fs::{File, OpenOptions},
    io::{self, Read},
    path::Path,
};

use zeroize::Zeroizing;

use super::contextual_io_error;

/// Deliberately has no Debug implementation. Buffers are wiped on every exit,
/// including parse/read errors and unwinding; callers only borrow their bytes.
pub(crate) struct ConfigurationBytes(Zeroizing<Vec<u8>>);

impl ConfigurationBytes {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

pub(crate) fn read_configuration_file(
    path: &Path,
    label: &str,
    maximum_bytes: u64,
    private: bool,
) -> io::Result<ConfigurationBytes> {
    let mut options = OpenOptions::new();
    options.read(true);
    // Opening a substituted FIFO must not block before we can reject its type.
    // Symlinks remain supported; the opened target is the authority checked.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options.open(path).map_err(|error| {
        contextual_io_error(error, format!("failed to open {label} {}", path.display()))
    })?;
    read_opened_file(file, path, label, maximum_bytes, private)
}

fn read_opened_file(
    file: File,
    path: &Path,
    label: &str,
    maximum_bytes: u64,
    private: bool,
) -> io::Result<ConfigurationBytes> {
    validate_opened_file(&file, path, label, maximum_bytes, private)?;
    read_limited(file, path, label, maximum_bytes)
}

pub(super) fn validate_opened_file(
    file: &File,
    path: &Path,
    label: &str,
    maximum_bytes: u64,
    private: bool,
) -> io::Result<()> {
    let metadata = file.metadata().map_err(|error| {
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
        return Err(size_error(path, label, maximum_bytes));
    }
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::MetadataExt;
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
    }
    #[cfg(not(unix))]
    let _ = private;
    Ok(())
}

fn size_error(path: &Path, label: &str, maximum_bytes: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
            "{label} {} exceeds the {maximum_bytes}-byte limit",
            path.display()
        ),
    )
}

fn read_limited(
    mut reader: impl Read,
    path: &Path,
    label: &str,
    maximum_bytes: u64,
) -> io::Result<ConfigurationBytes> {
    let limit = maximum_bytes
        .checked_add(1)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid configuration-file limit",
            )
        })?;
    // Fixed allocation: no reallocations can leave prior secret copies behind.
    // The extra byte detects growth past the limit after descriptor validation.
    let mut bytes = Zeroizing::new(vec![0; limit]);
    let mut filled = 0;
    while filled < limit {
        match reader.read(&mut bytes[filled..]) {
            Ok(0) => break,
            Ok(count) => filled += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                return Err(contextual_io_error(
                    error,
                    format!("failed to read {label} {}", path.display()),
                ));
            }
        }
    }
    if filled == limit {
        return Err(size_error(path, label, maximum_bytes));
    }
    bytes.truncate(filled);
    Ok(ConfigurationBytes(bytes))
}

#[cfg(test)]
mod tests;
