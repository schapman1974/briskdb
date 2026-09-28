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
}

impl StorageRoot {
    pub(super) fn new(path: PathBuf, journal: JournalPolicy) -> Self {
        Self { path, journal }
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

#[cfg(all(test, unix, feature = "documents"))]
mod efs_qualification;
