//! Metadata persistence, independent of application-shard storage and mounts.

use super::{EngineError, EngineErrorKind, EngineResult};

/// Authoritative catalog storage. Application shards remain SQLite in both modes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetadataBackend {
    #[default]
    Sqlite,
    /// Experimental native ISAM metadata; requires Unix and `experimental-isam`.
    Isam,
}

impl MetadataBackend {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Isam => "isam",
        }
    }

    pub(crate) fn require_available(self) -> EngineResult<()> {
        if self == Self::Isam && !cfg!(all(unix, feature = "experimental-isam")) {
            return Err(EngineError::new(
                EngineErrorKind::Unsupported,
                "ISAM metadata requires Unix and the experimental-isam Cargo feature",
            ));
        }
        Ok(())
    }
}

impl std::fmt::Display for MetadataBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for MetadataBackend {
    type Err = EngineError;
    fn from_str(value: &str) -> EngineResult<Self> {
        match value {
            "sqlite" => Ok(Self::Sqlite),
            "isam" => Ok(Self::Isam),
            _ => Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "metadata backend must be sqlite or isam",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::EngineOptions;
    #[cfg(not(all(unix, feature = "experimental-isam")))]
    use crate::core::{Database, Engine};

    #[test]
    fn default_and_names_do_not_change_data_storage_or_mount_profile() {
        assert_eq!(
            EngineOptions::default().metadata_backend(),
            MetadataBackend::Sqlite
        );
        for backend in [MetadataBackend::Sqlite, MetadataBackend::Isam] {
            assert_eq!(
                backend.as_str().parse::<MetadataBackend>().unwrap(),
                backend
            );
            assert_eq!(
                EngineOptions::default()
                    .with_metadata_backend(backend)
                    .storage_profile(),
                crate::StorageProfile::Local
            );
        }
        for value in ["", "ISAM", " isam", "native", "efs"] {
            assert_eq!(
                value.parse::<MetadataBackend>().unwrap_err().kind(),
                EngineErrorKind::InvalidArgument
            );
        }
    }

    #[cfg(not(all(unix, feature = "experimental-isam")))]
    #[tokio::test]
    async fn unavailable_native_option_never_touches_storage() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("absent");
        let options = EngineOptions::default().with_metadata_backend(MetadataBackend::Isam);
        assert_eq!(
            Database::open_with_metadata_backend(&root, 2, MetadataBackend::Isam)
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
        assert_eq!(
            Engine::open_with_options(&root, 2, options)
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
        assert_eq!(
            Engine::open_detected_with_options(&root, options)
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
        assert!(!root.exists());
    }
}
