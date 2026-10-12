//! Backend identity checks shared by feature-enabled and SQLite-only builds.
use super::*;
use crate::MetadataBackend;

pub(super) fn present(path: &Path) -> EngineResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => Ok(true),
        Ok(_) => Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            format!("metadata path {} is not a regular file", path.display()),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(sqlite_error::storage_io(
            error,
            "cannot inspect metadata identity",
        )),
    }
}

pub(crate) fn validate_selection(root: &Path, selected: MetadataBackend) -> EngineResult<()> {
    selected.require_available()?;
    if present(&root.join("overlay.mode"))? || present(&root.join("overlay.isam"))? {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "this is an opt-in S3 overlay database; use the overlay API, not a normal BriskDB open",
        ));
    }
    let sqlite = present(&root.join("manifest.sqlite"))?;
    let isam = present(&root.join("manifest.isam"))?;
    if (sqlite && selected != MetadataBackend::Sqlite)
        || (isam && selected != MetadataBackend::Isam)
    {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "metadata backend does not match this database; automatic metadata conversion is not supported",
        ));
    }
    Ok(())
}

pub(crate) fn detect_shards(
    root: &Path,
    profile: crate::StorageProfile,
    backend: MetadataBackend,
) -> EngineResult<u16> {
    profile.require_available()?;
    validate_selection(root, backend)?;
    match backend {
        MetadataBackend::Sqlite => detect_shard_count_with_profile(root, profile),
        MetadataBackend::Isam => {
            #[cfg(all(unix, feature = "experimental-isam"))]
            {
                let root = native_manifest::NativeManifest::open(
                    &root.join(native_manifest::FILE_NAME),
                    true,
                )?
                .root()?
                .1;
                if root.rollback_journal {
                    return Err(EngineError::new(
                        EngineErrorKind::FailedPrecondition,
                        "native metadata uses a test-only rollback profile, not local storage",
                    ));
                }
                Ok(root.shards)
            }
            #[cfg(not(all(unix, feature = "experimental-isam")))]
            {
                unreachable!("availability checked above")
            }
        }
    }
}
