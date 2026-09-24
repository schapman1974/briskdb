//! Bounded, retained collection-writer stripes, separate from idempotency locks.

use super::*;

pub(in crate::storage) const STRIPES: usize = 256;

pub(in crate::storage) struct DocumentWriteFence {
    file: File,
    local: Arc<[AtomicBool; STRIPES]>,
    stripe: usize,
}

impl std::fmt::Debug for DocumentWriteFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocumentWriteFence").finish_non_exhaustive()
    }
}

impl DocumentWriteFence {
    pub(in crate::storage) fn try_acquire(
        root: &Path,
        collection: crate::document::DocumentCollectionId,
        local: Arc<[AtomicBool; STRIPES]>,
    ) -> EngineResult<Self> {
        // Collection IDs never repeat within a root. Collisions serialize
        // unrelated collections but cannot weaken protection or grow the
        // retained filesystem namespace beyond 256 files.
        let stripe = (collection.get() % STRIPES as u64) as usize;
        if local[stripe]
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(EngineError::new(
                EngineErrorKind::Busy,
                "another local writer owns this document collection stripe",
            ));
        }
        let result = (|| {
            let path = root.join(format!(".briskdb-document-write-{stripe:02x}.lock"));
            let file = open_regular_lock_file(&path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let metadata = file.metadata().map_err(|error| {
                    sqlite_error::storage_io(error, "failed to inspect document write lock")
                })?;
                if metadata.nlink() != 1 {
                    return Err(EngineError::new(
                        EngineErrorKind::FailedPrecondition,
                        "document write lock must have exactly one filesystem link",
                    ));
                }
            }
            lock_nonblocking(&file, LockRequest::Exclusive).map_err(|error| {
                map_lock_error(
                    error,
                    &path,
                    "another process owns this document collection stripe",
                )
            })?;
            Ok(file)
        })();
        match result {
            Ok(file) => Ok(Self {
                file,
                local,
                stripe,
            }),
            Err(error) => {
                local[stripe].store(false, Ordering::Release);
                Err(error)
            }
        }
    }
}

impl Drop for DocumentWriteFence {
    fn drop(&mut self) {
        // Unlock before making the same-process claim available. The retained
        // pathname is never removed: deleting it would permit lock inode ABA.
        let _ = unlock(&self.file);
        self.local[self.stripe].store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::DocumentCollectionId;

    #[test]
    fn stripes_are_bounded_root_local_retained_and_separate_from_idempotency() {
        let root = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let local = Arc::new(std::array::from_fn(|_| AtomicBool::new(false)));
        let acquire = |id| {
            DocumentWriteFence::try_acquire(
                root.path(),
                DocumentCollectionId::from_validated(id),
                Arc::clone(&local),
            )
        };
        let first = acquire(1).unwrap();
        assert_eq!(acquire(257).unwrap_err().kind(), EngineErrorKind::Busy);
        drop(acquire(2).unwrap());
        drop(
            DocumentWriteFence::try_acquire(
                other.path(),
                DocumentCollectionId::from_validated(1),
                Arc::new(std::array::from_fn(|_| AtomicBool::new(false))),
            )
            .unwrap(),
        );
        drop(
            IdempotencyStripeGuard::try_acquire(
                root.path(),
                [1; 32],
                Arc::new(std::array::from_fn(|_| AtomicBool::new(false))),
            )
            .unwrap(),
        );
        drop(first);
        for id in 1..=512 {
            drop(acquire(id).unwrap());
        }
        let count = std::fs::read_dir(root.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".briskdb-document-write-")
            })
            .count();
        assert_eq!(count, STRIPES);
    }

    #[cfg(unix)]
    #[test]
    fn unsafe_paths_fail_without_retaining_a_local_claim() {
        use std::os::unix::fs::symlink;
        for kind in ["symlink", "hardlink", "directory"] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join(".briskdb-document-write-01.lock");
            let target = root.path().join("target");
            std::fs::write(&target, b"not a lock").unwrap();
            match kind {
                "symlink" => symlink(&target, &path).unwrap(),
                "hardlink" => std::fs::hard_link(&target, &path).unwrap(),
                _ => std::fs::create_dir(&path).unwrap(),
            }
            let local = Arc::new(std::array::from_fn(|_| AtomicBool::new(false)));
            for _ in 0..2 {
                let error = DocumentWriteFence::try_acquire(
                    root.path(),
                    DocumentCollectionId::from_validated(1),
                    Arc::clone(&local),
                )
                .unwrap_err();
                assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition, "{kind}");
                assert!(!local[1].load(Ordering::Acquire));
            }
            assert_eq!(std::fs::read(&target).unwrap(), b"not a lock");
        }
    }
}
