//! Protocol-neutral options for document reads and writes.

use std::{fmt, num::NonZeroU64};

use crate::core::{EngineError, EngineErrorKind, EngineResult};

use super::{BSON_MAX_DECODED_BYTES, BsonDocument, BsonErrorContext, encode_document};

/// Default number of documents requested for one cursor batch.
pub const DEFAULT_DOCUMENT_BATCH_SIZE: u64 = 101;
/// Maximum number of documents accepted in one request or cursor batch.
pub const MAX_DOCUMENT_BATCH_SIZE: u64 = 100_000;
/// Maximum aggregate encoded BSON retained by one owned document request.
pub const MAX_DOCUMENT_REQUEST_BYTES: usize = BSON_MAX_DECODED_BYTES;

fn payload_len(document: &BsonDocument) -> EngineResult<usize> {
    encode_document(document)
        .map(|encoded| encoded.len())
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))
}

fn validate_payload(document: &BsonDocument) -> EngineResult<()> {
    payload_len(document).map(|_| ())
}

macro_rules! document_payload {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, PartialEq, Eq)]
        pub struct $name {
            document: BsonDocument,
        }

        impl $name {
            /// Validate and own an ordered BSON document.
            pub fn new(document: BsonDocument) -> EngineResult<Self> {
                validate_payload(&document)?;
                Ok(Self { document })
            }

            /// Borrow the exact ordered BSON document.
            pub const fn document(&self) -> &BsonDocument {
                &self.document
            }

            /// Consume this wrapper and return its ordered BSON document.
            pub fn into_document(self) -> BsonDocument {
                self.document
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("document", &"<redacted>")
                    .finish()
            }
        }
    };
}

document_payload!(
    DocumentFilter,
    "An owned, structurally valid BSON match expression."
);
document_payload!(
    DocumentProjection,
    "An owned, structurally valid BSON projection specification."
);
document_payload!(
    DocumentSort,
    "An owned, structurally valid BSON sort specification."
);
document_payload!(
    DocumentUpdate,
    "An owned, structurally valid BSON update specification."
);

impl DocumentFilter {
    /// Construct the empty filter, which matches every document once command
    /// semantics are applied by the document engine.
    pub const fn empty() -> Self {
        Self {
            document: BsonDocument::new(),
        }
    }

    /// Return whether this filter has no direct fields.
    pub fn is_empty(&self) -> bool {
        self.document.is_empty()
    }
}

impl Default for DocumentFilter {
    fn default() -> Self {
        Self::empty()
    }
}

/// An owned sequence of structurally valid aggregation stages.
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentPipeline {
    stages: Box<[BsonDocument]>,
}

impl DocumentPipeline {
    /// Validate and own aggregation stages without interpreting their
    /// operators.
    pub fn new(stages: impl Into<Vec<BsonDocument>>) -> EngineResult<Self> {
        let stages = stages.into();
        if stages.len() as u64 > MAX_DOCUMENT_BATCH_SIZE {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!("document pipeline must not exceed {MAX_DOCUMENT_BATCH_SIZE} stages"),
            ));
        }
        let mut encoded_bytes = 0_usize;
        for stage in &stages {
            encoded_bytes = encoded_bytes
                .checked_add(payload_len(stage)?)
                .filter(|bytes| *bytes <= MAX_DOCUMENT_REQUEST_BYTES)
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::LimitExceeded,
                        format!(
                            "document pipeline exceeds {MAX_DOCUMENT_REQUEST_BYTES} encoded BSON bytes"
                        ),
                    )
                })?;
        }
        Ok(Self {
            stages: stages.into_boxed_slice(),
        })
    }

    pub fn stages(&self) -> &[BsonDocument] {
        &self.stages
    }

    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    pub fn into_stages(self) -> Vec<BsonDocument> {
        self.stages.into_vec()
    }
}

impl fmt::Debug for DocumentPipeline {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentPipeline")
            .field("stage_count", &self.stages.len())
            .field("stages", &"<redacted>")
            .finish()
    }
}

/// Options applied while reading and materializing document results.
///
/// Projection and sort documents remain exact ordered BSON. A missing limit
/// means unbounded by the caller; engine-wide result limits still apply.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentReadOptions {
    projection: Option<DocumentProjection>,
    sort: Option<DocumentSort>,
    skip: u64,
    limit: Option<NonZeroU64>,
    batch_size: NonZeroU64,
}

impl DocumentReadOptions {
    /// Construct the default read options.
    pub const fn new() -> Self {
        Self {
            projection: None,
            sort: None,
            skip: 0,
            limit: None,
            batch_size: NonZeroU64::new(DEFAULT_DOCUMENT_BATCH_SIZE)
                .expect("the default document batch size is nonzero"),
        }
    }

    #[must_use]
    pub fn with_projection(mut self, projection: DocumentProjection) -> Self {
        self.projection = Some(projection);
        self
    }

    #[must_use]
    pub fn with_sort(mut self, sort: DocumentSort) -> Self {
        self.sort = Some(sort);
        self
    }

    #[must_use]
    pub const fn with_skip(mut self, skip: u64) -> Self {
        self.skip = skip;
        self
    }

    /// Set a positive logical result limit.
    pub fn with_limit(mut self, limit: u64) -> EngineResult<Self> {
        self.limit = Some(NonZeroU64::new(limit).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::InvalidArgument,
                "document result limit must be greater than zero",
            )
        })?);
        Ok(self)
    }

    /// Request a positive, bounded cursor batch size.
    pub fn with_batch_size(mut self, batch_size: u64) -> EngineResult<Self> {
        if batch_size > MAX_DOCUMENT_BATCH_SIZE {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!("document batch size must not exceed {MAX_DOCUMENT_BATCH_SIZE}"),
            ));
        }
        self.batch_size = NonZeroU64::new(batch_size).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::InvalidArgument,
                "document batch size must be greater than zero",
            )
        })?;
        Ok(self)
    }

    pub const fn projection(&self) -> Option<&DocumentProjection> {
        self.projection.as_ref()
    }

    pub const fn sort(&self) -> Option<&DocumentSort> {
        self.sort.as_ref()
    }

    pub const fn skip(&self) -> u64 {
        self.skip
    }

    pub const fn limit(&self) -> Option<u64> {
        match self.limit {
            Some(limit) => Some(limit.get()),
            None => None,
        }
    }

    pub const fn batch_size(&self) -> u64 {
        self.batch_size.get()
    }

    pub fn into_parts(
        self,
    ) -> (
        Option<DocumentProjection>,
        Option<DocumentSort>,
        u64,
        Option<u64>,
        u64,
    ) {
        (
            self.projection,
            self.sort,
            self.skip,
            self.limit.map(NonZeroU64::get),
            self.batch_size.get(),
        )
    }
}

impl Default for DocumentReadOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for DocumentReadOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentReadOptions")
            .field(
                "projection",
                &self.projection.as_ref().map(|_| "<redacted>"),
            )
            .field("sort", &self.sort.as_ref().map(|_| "<redacted>"))
            .field("skip", &self.skip)
            .field("limit", &self.limit())
            .field("batch_size", &self.batch_size())
            .finish()
    }
}

/// Options applied to one logical document write.
///
/// `ordered` controls batch failure order. `upsert` is consumed only by
/// commands that define upsert semantics. Bypassing application validation
/// never bypasses BriskDB's BSON, `_id`, routing, or storage validation.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DocumentWriteOptions {
    ordered: bool,
    upsert: bool,
    bypass_document_validation: bool,
}

impl DocumentWriteOptions {
    pub const fn new() -> Self {
        Self {
            ordered: true,
            upsert: false,
            bypass_document_validation: false,
        }
    }

    #[must_use]
    pub const fn with_ordered(mut self, ordered: bool) -> Self {
        self.ordered = ordered;
        self
    }

    #[must_use]
    pub const fn with_upsert(mut self, upsert: bool) -> Self {
        self.upsert = upsert;
        self
    }

    #[must_use]
    pub const fn with_bypass_document_validation(mut self, bypass: bool) -> Self {
        self.bypass_document_validation = bypass;
        self
    }

    pub const fn ordered(self) -> bool {
        self.ordered
    }

    pub const fn upsert(self) -> bool {
        self.upsert
    }

    pub const fn bypass_document_validation(self) -> bool {
        self.bypass_document_validation
    }
}

impl Default for DocumentWriteOptions {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::BsonValue;

    fn secret_document() -> BsonDocument {
        BsonDocument::from_entries([("password", BsonValue::from("correct horse"))]).unwrap()
    }

    #[test]
    fn payload_wrappers_preserve_order_and_redact_debug() {
        let filter = DocumentFilter::new(secret_document()).unwrap();
        assert_eq!(filter.document().iter().next().unwrap().0, "password");
        let debug = format!("{filter:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("correct horse"));
    }

    #[test]
    fn read_options_validate_bounds_and_hide_bson() {
        let projection = DocumentProjection::new(secret_document()).unwrap();
        let options = DocumentReadOptions::new()
            .with_projection(projection)
            .with_skip(3)
            .with_limit(7)
            .unwrap()
            .with_batch_size(11)
            .unwrap();
        assert_eq!(options.skip(), 3);
        assert_eq!(options.limit(), Some(7));
        assert_eq!(options.batch_size(), 11);
        assert!(DocumentReadOptions::new().with_batch_size(0).is_err());
        assert!(
            DocumentReadOptions::new()
                .with_batch_size(MAX_DOCUMENT_BATCH_SIZE + 1)
                .is_err()
        );
        assert!(!format!("{options:?}").contains("correct horse"));
    }

    #[test]
    fn pipeline_is_owned_cloneable_and_redacted() {
        fn assert_owned<T: Clone + Send + Sync + 'static>() {}
        assert_owned::<DocumentPipeline>();
        let pipeline = DocumentPipeline::new(vec![secret_document()]).unwrap();
        assert_eq!(pipeline.stages().len(), 1);
        assert!(!format!("{pipeline:?}").contains("correct horse"));
    }
}
