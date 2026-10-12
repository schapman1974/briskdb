//! An explicit, validated root contract carried to every shared-file opener.
//!
//! A filesystem path alone is not permission to select or convert a journal.
//! This type stays inside storage; public NFS availability is separately gated.

use std::{
    ops::Deref,
    path::{Path, PathBuf},
};

use super::journal::JournalPolicy;

#[derive(Debug, Clone)]
pub(super) struct StorageRoot {
    path: PathBuf,
    journal: JournalPolicy,
    metadata_backend: crate::MetadataBackend,
}

impl StorageRoot {
    pub(super) fn new(path: PathBuf, journal: JournalPolicy) -> Self {
        Self {
            path,
            journal,
            metadata_backend: crate::MetadataBackend::Sqlite,
        }
    }

    #[cfg(all(unix, feature = "experimental-isam"))]
    pub(super) fn with_native_metadata(mut self) -> Self {
        self.metadata_backend = crate::MetadataBackend::Isam;
        self
    }

    pub(super) const fn metadata_backend(&self) -> crate::MetadataBackend {
        self.metadata_backend
    }

    pub(super) const fn journal(&self) -> JournalPolicy {
        self.journal
    }

    pub(super) fn set_validated_journal(&mut self, journal: JournalPolicy) {
        self.journal = journal;
    }
}

impl Deref for StorageRoot {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for StorageRoot {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, unix, feature = "experimental-isam", feature = "embedded"))]
mod chapter_qualification;

#[cfg(all(test, unix, feature = "documents"))]
mod efs_qualification;
