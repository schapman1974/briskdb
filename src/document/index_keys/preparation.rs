//! Collection-wide preparation, not a physical index or catalog-freshness proof.

use super::{Budget, DocumentIndexKeyGenerator, MAX_WORK_BYTES, allocation, fmt, limit};
use crate::{
    core::{EngineError, EngineErrorKind, EngineResult},
    document::{
        BsonDocument, BsonErrorContext, DocumentCollectionId, DocumentCollectionMetadata,
        DocumentIndexId, DocumentIndexMetadata, encode_document, memory,
    },
};

/// Maximum secondary declarations in one preparation; the built-in `_id_`
/// index is handled by record storage and does not count toward this limit.
pub const MAX_DOCUMENT_PREPARED_INDEXES: usize = 64;

/// Compiled secondary-index definitions from one collection metadata snapshot.
///
/// Includes pending declarations, for future build validation. This is a pure
/// preflight helper: it neither activates indexes nor enforces uniqueness nor
/// certifies that metadata is current. A storage caller must fence the
/// catalog and maintain entries in the same transaction as the document.
/// Ordinary writes do not call this for today's non-enforcing pending indexes.
///
/// Compilation and each preparation independently share conservative 64-MiB
/// work charges and one million checkpoints across all indexes. One preparation
/// may emit at most 16,384 keys in total. Encoded frames and vector overhead are
/// charged before allocation. These are work limits, not an RSS guarantee.
pub struct DocumentIndexPreparation {
    collection_id: DocumentCollectionId,
    indexes: Vec<CompiledIndex>,
    retained_bytes: usize,
}

struct CompiledIndex {
    id: DocumentIndexId,
    unique: bool,
    generator: DocumentIndexKeyGenerator,
}

/// All secondary entries for one input, or nothing if preparation failed.
///
/// The collection/index IDs scope the encoded keys; they do not identify the
/// database root or establish physical coverage or catalog freshness.
pub struct PreparedDocumentIndexEntries {
    collection_id: DocumentCollectionId,
    indexes: Vec<PreparedDocumentIndexKeys>,
}

/// Encoded BDIK equality tuples for one index, in encounter order. Sparse and
/// partial exclusions retain the index identity with an empty key list.
pub struct PreparedDocumentIndexKeys {
    index_id: DocumentIndexId,
    unique: bool,
    keys: Vec<Vec<u8>>,
}

impl DocumentIndexPreparation {
    pub fn compile(collection: &DocumentCollectionMetadata) -> EngineResult<Self> {
        Self::compile_with_check(collection, &mut || Ok(()))
    }

    pub fn compile_with_check(
        collection: &DocumentCollectionMetadata,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Self> {
        check()?;
        // Check the supplied length before scanning or allocating. Catalog
        // snapshots have exactly one mandatory built-in index.
        if collection.indexes().len() > MAX_DOCUMENT_PREPARED_INDEXES + 1 {
            return Err(limit());
        }
        Self::compile_selected_with_check(collection.id(), collection.indexes(), |_| true, check)
    }

    /// Storage selects only authoritative Ready indexes for maintenance, or
    /// one explicitly journaled index for a build. Selection itself is not a
    /// freshness proof: its caller must retain schema admission/ownership.
    pub(crate) fn compile_selected_with_check<'a>(
        collection_id: DocumentCollectionId,
        metadata: impl IntoIterator<Item = &'a DocumentIndexMetadata>,
        include: impl Fn(&DocumentIndexMetadata) -> bool,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Self> {
        check()?;
        let mut budget = Budget::new(check);
        let mut indexes: Vec<CompiledIndex> = Vec::new();
        let mut retained_bytes = 256_usize;
        budget.charge(retained_bytes)?;
        for metadata in metadata {
            budget.step()?;
            if metadata.is_built_in() || !include(metadata) {
                continue;
            }
            if indexes.len() >= MAX_DOCUMENT_PREPARED_INDEXES {
                return Err(limit());
            }
            // Unknown imported envelopes must never silently lose membership
            // options. Catalog metadata itself remains readable and unchanged.
            let definition = metadata.definition().ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::Unsupported,
                    "cannot prepare an unsupported document index definition",
                )
            })?;
            let bytes =
                memory::document_bytes(metadata.specification(), MAX_WORK_BYTES, &mut || {
                    budget.step()
                })?;
            budget.charge(bytes)?;
            let generator = DocumentIndexKeyGenerator::compile_with_check(
                definition.keys(),
                definition.sparse(),
                definition.partial_filter(),
                &mut || budget.step(),
            )?;
            let retained = generator
                .retained_bytes()
                .checked_add(128)
                .ok_or_else(limit)?;
            budget.charge(retained)?;
            retained_bytes = retained_bytes.checked_add(retained).ok_or_else(limit)?;
            indexes.try_reserve_exact(1).map_err(allocation)?;
            indexes.push(CompiledIndex {
                id: metadata.id(),
                unique: metadata.is_unique(),
                generator,
            });
        }
        budget.step()?;
        Ok(Self {
            collection_id,
            indexes,
            retained_bytes,
        })
    }

    pub const fn collection_id(&self) -> DocumentCollectionId {
        self.collection_id
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    /// Conservative owned-heap charge for retained compiled definitions.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub fn prepare(&self, document: &BsonDocument) -> EngineResult<PreparedDocumentIndexEntries> {
        self.prepare_with_check(document, &mut || Ok(()))
    }

    /// Validate once, then prepare every index under one shared budget. Errors
    /// and cancellation discard all entries, including already encoded indexes.
    /// A failed call leaves this compiler reusable and never mutates the input.
    pub fn prepare_with_check(
        &self,
        document: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<PreparedDocumentIndexEntries> {
        let mut budget = Budget::new(check);
        budget.step()?;
        budget.charge(self.retained_bytes)?;
        // Charge traversal even for unindexed fields or an empty index set.
        // No repeated whole-document BSON allocation for each index.
        let validated = encode_document(document)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        budget.charge(validated.len())?;
        drop(validated);
        // The codec checks depth before the recursive heap-charge traversal.
        let bytes = memory::document_bytes(document, MAX_WORK_BYTES, &mut || budget.step())?;
        budget.charge(bytes)?;
        budget.step()?;
        budget.charge(128 + self.indexes.len() * 128)?;
        let mut indexes = Vec::new();
        indexes
            .try_reserve_exact(self.indexes.len())
            .map_err(allocation)?;
        for index in &self.indexes {
            budget.step()?;
            let tuples = index
                .generator
                .keys_validated_with_budget(document, &mut budget)?;
            budget.charge(tuples.len() * 32)?;
            let mut keys = Vec::new();
            keys.try_reserve_exact(tuples.len()).map_err(allocation)?;
            for tuple in tuples {
                let length = tuple.encoded_len_with_check(&mut || budget.step())?;
                budget.charge(length)?;
                keys.push(tuple.to_bytes_with_check(&mut || budget.step())?);
            }
            indexes.push(PreparedDocumentIndexKeys {
                index_id: index.id,
                unique: index.unique,
                keys,
            });
        }
        budget.step()?;
        Ok(PreparedDocumentIndexEntries {
            collection_id: self.collection_id,
            indexes,
        })
    }
}

impl PreparedDocumentIndexEntries {
    pub const fn collection_id(&self) -> DocumentCollectionId {
        self.collection_id
    }

    pub fn indexes(&self) -> &[PreparedDocumentIndexKeys] {
        &self.indexes
    }
}

impl PreparedDocumentIndexKeys {
    pub const fn index_id(&self) -> DocumentIndexId {
        self.index_id
    }

    /// The retained declaration flag, not a uniqueness-enforcement guarantee.
    pub const fn is_unique(&self) -> bool {
        self.unique
    }

    pub fn keys(&self) -> &[Vec<u8>] {
        &self.keys
    }
}

impl fmt::Debug for DocumentIndexPreparation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentIndexPreparation")
            .field("collection_id", &self.collection_id)
            .field("index_count", &self.indexes.len())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for PreparedDocumentIndexEntries {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedDocumentIndexEntries")
            .field("collection_id", &self.collection_id)
            .field("index_count", &self.indexes.len())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for PreparedDocumentIndexKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedDocumentIndexKeys")
            .field("index_id", &self.index_id)
            .field("key_count", &self.keys.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
