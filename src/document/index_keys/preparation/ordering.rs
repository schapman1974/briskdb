//! Optional order keys are derived independently of equality tuples. An array
//! selects different values in opposite directions, so reversing encoded bytes
//! is not a substitute for deriving the inverse sort specification.

use crate::{
    core::{EngineError, EngineErrorKind, EngineResult},
    document::{BsonDocument, BsonErrorContext, BsonValue, DocumentIndexMetadata, DocumentSorter},
};

pub(super) const MAX_ORDER_KEY_BYTES: usize = 64 * 1024;

pub(super) struct Ordering {
    sorters: [DocumentSorter; 2],
}

impl Ordering {
    pub(super) fn direction(&self, sorter: &DocumentSorter) -> Option<u8> {
        self.sorters
            .iter()
            .position(|candidate| candidate.same_specification(sorter))
            .map(|direction| direction as u8)
    }

    pub(super) fn compile(
        metadata: &DocumentIndexMetadata,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Option<Self>> {
        if !metadata.has_ordered_keys() {
            return Ok(None);
        }
        let definition = metadata.definition().ok_or_else(invalid)?;
        if metadata.is_built_in() || definition.sparse() || definition.partial_filter().is_some() {
            return Err(invalid());
        }
        let inverse =
            BsonDocument::from_entries(definition.keys().iter().map(|(name, direction)| {
                (
                    name,
                    BsonValue::Int32(if direction == &BsonValue::Int32(1) {
                        -1
                    } else {
                        1
                    }),
                )
            }))
            .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
        Ok(Some(Self {
            sorters: [
                DocumentSorter::compile_with_check(definition.keys(), check)?,
                DocumentSorter::compile_with_check(&inverse, check)?,
            ],
        }))
    }

    pub(super) fn retained_bytes(&self) -> usize {
        self.sorters
            .iter()
            .map(DocumentSorter::retained_bytes)
            .sum()
    }

    pub(super) fn prepare(
        &self,
        document: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<[Option<Vec<u8>>; 2]> {
        let mut keys = [None, None];
        for (sorter, result) in self.sorters.iter().zip(&mut keys) {
            // Never swallow a caller's cancellation, deadline, allocation or
            // shared-work failure as an unsupported sortable value.
            let mut interrupted = false;
            let mut guarded = || match check() {
                Ok(()) => Ok(()),
                Err(error) => {
                    interrupted = true;
                    Err(error)
                }
            };
            let encoded = sorter
                .key_validated_with_check(document, &mut guarded)
                .and_then(|key| key.ordered_bytes_bounded(MAX_ORDER_KEY_BYTES, &mut guarded));
            match encoded {
                Ok(key) => *result = Some(key),
                Err(error)
                    if !interrupted
                        && matches!(
                            error.kind(),
                            EngineErrorKind::InvalidArgument
                                | EngineErrorKind::InvalidQuery
                                | EngineErrorKind::Unsupported
                                | EngineErrorKind::LimitExceeded
                        ) =>
                {
                    // Persist a checksummed NULL marker, never a partial key.
                    // Readers fall back when any relevant record has a marker,
                    // preserving the full sorter's error/array semantics.
                    check()?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(keys)
    }
}

fn invalid() -> EngineError {
    EngineError::new(
        EngineErrorKind::DataCorruption,
        "ordered index capability has an unsupported definition",
    )
}
