//! Storage layout selection, separate from protocol and mount qualification.

use super::{EngineError, EngineErrorKind, EngineResult};

/// The storage contract requested when opening a BriskDB root.
///
/// `Local` preserves the local-filesystem/WAL contract. `Nfs` reserves the
/// rollback-journal profile; public opens currently reject it before touching
/// storage, until cross-host locking and recovery are implemented and qualified.
/// A mode name is not a claim that every NFS version or mount is supported.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageProfile {
    #[default]
    Local,
    Nfs,
}

impl StorageProfile {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Nfs => "nfs",
        }
    }

    pub(crate) fn require_available(self) -> EngineResult<()> {
        match self {
            Self::Local => Ok(()),
            Self::Nfs => Err(EngineError::new(
                EngineErrorKind::Unsupported,
                "NFS storage is not enabled: cross-host locking, recovery and qualification are required; local/NFS conversion is not supported",
            )),
        }
    }
}

impl std::fmt::Display for StorageProfile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for StorageProfile {
    type Err = EngineError;

    fn from_str(value: &str) -> EngineResult<Self> {
        match value {
            "local" => Ok(Self::Local),
            "nfs" => Ok(Self::Nfs),
            _ => Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "storage profile must be local or nfs",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Database, Engine, EngineOptions};

    #[test]
    fn storage_profile_names_are_explicit_and_local_remains_default() {
        assert_eq!(StorageProfile::default(), StorageProfile::Local);
        assert_eq!(
            EngineOptions::default().storage_profile(),
            StorageProfile::Local
        );
        for profile in [StorageProfile::Local, StorageProfile::Nfs] {
            assert_eq!(
                profile.to_string().parse::<StorageProfile>().unwrap(),
                profile
            );
        }
        for name in ["", "efs", "NFS", "nfs4.1", " local", "local "] {
            assert_eq!(
                name.parse::<StorageProfile>().unwrap_err().kind(),
                EngineErrorKind::InvalidArgument
            );
        }
    }

    #[tokio::test]
    async fn unsupported_storage_profile_is_rejected_before_any_root_creation() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("not-created");
        let options = EngineOptions::default().with_storage_profile(StorageProfile::Nfs);
        assert_eq!(
            options.validate_for_shards(4).unwrap_err().kind(),
            EngineErrorKind::Unsupported
        );
        assert_eq!(
            Database::open_with_profile(&root, 4, StorageProfile::Nfs)
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
        assert_eq!(
            Engine::open_with_options(&root, 4, options)
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
        #[cfg(feature = "auth-scram")]
        assert_eq!(
            Engine::open_authenticated(&root, 4, options)
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
        #[cfg(feature = "embedded")]
        assert_eq!(
            crate::BriskDb::builder(&root)
                .with_engine_options(options)
                .open()
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
        assert!(!root.exists());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn requesting_nfs_never_converts_an_existing_local_root() {
        let directory = tempfile::tempdir().unwrap();
        let database = std::sync::Arc::new(Database::open(directory.path(), 4).unwrap());
        let manifest = directory.path().join("manifest.sqlite");
        let before = std::fs::read(&manifest).unwrap();
        let options = EngineOptions::default().with_storage_profile(StorageProfile::Nfs);
        assert_eq!(
            Engine::open_with_options(directory.path(), 4, options)
                .await
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
        assert_eq!(
            Engine::from_database_with_options(database, options)
                .unwrap_err()
                .kind(),
            EngineErrorKind::Unsupported
        );
        assert_eq!(
            Database::detect_storage_profile(directory.path()).unwrap(),
            StorageProfile::Local
        );
        assert_eq!(std::fs::read(manifest).unwrap(), before);
    }

    #[test]
    fn storage_profile_detection_never_initializes_missing_storage() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("absent");
        assert!(Database::detect_storage_profile(&root).is_err());
        assert!(!root.exists());
    }
}
