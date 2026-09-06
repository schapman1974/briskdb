//! Redaction-safe point and scatter plans for document commands.

use std::fmt;

use crate::core::{EngineError, EngineErrorKind, EngineResult};

use super::{CanonicalBsonKey, DocumentCollectionId};

const MAX_DOCUMENT_SHARDS: usize = 64;

/// A single-shard document plan proven from a canonical BSON identity.
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentPointPlan {
    collection_id: DocumentCollectionId,
    shard: u16,
    id_key: CanonicalBsonKey,
}

impl DocumentPointPlan {
    pub fn new(
        collection_id: DocumentCollectionId,
        shard: u16,
        id_key: CanonicalBsonKey,
    ) -> EngineResult<Self> {
        if usize::from(shard) >= MAX_DOCUMENT_SHARDS {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "document point-plan shard must be in 0..=63",
            ));
        }
        Ok(Self {
            collection_id,
            shard,
            id_key,
        })
    }

    pub const fn collection_id(&self) -> DocumentCollectionId {
        self.collection_id
    }

    pub const fn shard(&self) -> u16 {
        self.shard
    }

    pub const fn id_key(&self) -> &CanonicalBsonKey {
        &self.id_key
    }

    pub fn into_parts(self) -> (DocumentCollectionId, u16, CanonicalBsonKey) {
        (self.collection_id, self.shard, self.id_key)
    }
}

impl fmt::Debug for DocumentPointPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentPointPlan")
            .field("collection_id", &self.collection_id)
            .field("shard", &self.shard)
            .field("id_key", &"<redacted>")
            .finish()
    }
}

/// A deterministic set of physical shards for a document command.
///
/// Shards are nonempty, unique, and sorted in ascending order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentScatterPlan {
    collection_id: DocumentCollectionId,
    shards: Box<[u16]>,
}

impl DocumentScatterPlan {
    pub fn new(
        collection_id: DocumentCollectionId,
        shards: impl Into<Vec<u16>>,
    ) -> EngineResult<Self> {
        let mut shards = shards.into();
        if shards.is_empty() {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "document scatter plan must target at least one shard",
            ));
        }
        if shards.len() > MAX_DOCUMENT_SHARDS
            || shards
                .iter()
                .any(|shard| usize::from(*shard) >= MAX_DOCUMENT_SHARDS)
        {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "document scatter-plan shards must be unique values in 0..=63",
            ));
        }
        shards.sort_unstable();
        if shards.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "document scatter-plan shards must be unique values in 0..=63",
            ));
        }
        Ok(Self {
            collection_id,
            shards: shards.into_boxed_slice(),
        })
    }

    pub const fn collection_id(&self) -> DocumentCollectionId {
        self.collection_id
    }

    pub fn shards(&self) -> &[u16] {
        &self.shards
    }

    pub fn into_parts(self) -> (DocumentCollectionId, Vec<u16>) {
        (self.collection_id, self.shards.into_vec())
    }
}

/// Physical routing selected for one document command.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentPlan {
    Point(DocumentPointPlan),
    Scatter(DocumentScatterPlan),
}

impl DocumentPlan {
    pub const fn collection_id(&self) -> DocumentCollectionId {
        match self {
            Self::Point(plan) => plan.collection_id(),
            Self::Scatter(plan) => plan.collection_id(),
        }
    }

    pub fn shards(&self) -> &[u16] {
        match self {
            Self::Point(plan) => std::slice::from_ref(&plan.shard),
            Self::Scatter(plan) => plan.shards(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::BsonValue;

    fn collection_id() -> DocumentCollectionId {
        DocumentCollectionId::from_validated(7)
    }

    #[test]
    fn point_plan_owns_and_redacts_the_canonical_id() {
        fn assert_owned<T: Clone + Send + Sync + 'static>() {}
        assert_owned::<DocumentPlan>();
        let key = CanonicalBsonKey::encode(&BsonValue::from("private-id")).unwrap();
        let plan = DocumentPointPlan::new(collection_id(), 3, key).unwrap();
        assert_eq!(plan.shard(), 3);
        assert!(!format!("{plan:?}").contains("private-id"));
        assert!(format!("{plan:?}").contains("<redacted>"));
    }

    #[test]
    fn scatter_plan_sorts_and_rejects_invalid_targets() {
        let plan = DocumentScatterPlan::new(collection_id(), vec![3, 0, 2]).unwrap();
        assert_eq!(plan.shards(), &[0, 2, 3]);
        assert!(DocumentScatterPlan::new(collection_id(), Vec::new()).is_err());
        assert!(DocumentScatterPlan::new(collection_id(), vec![1, 1]).is_err());
        assert!(DocumentScatterPlan::new(collection_id(), vec![64]).is_err());
    }
}
