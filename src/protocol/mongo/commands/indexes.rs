//! Bounded, eager parsing before implicit collection creation or index builds.

use super::*;
use crate::document::{DocumentIndexError, DocumentIndexRequest, normalize_index_batch};

pub(super) fn prepare(
    request: &Request,
    namespace: DocumentNamespace,
    started: Instant,
    timeout: Duration,
) -> Result<DocumentCreateIndexesRequest> {
    if !request.sequences.is_empty() {
        return Err(CommandError::options());
    }
    let mut fields_seen = std::collections::BTreeSet::new();
    if request
        .body
        .iter()
        .any(|(name, _)| !fields_seen.insert(name))
    {
        return Err(CommandError::invalid());
    }
    let documents = write_documents(request, "indexes")?;
    let mut indexes = Vec::with_capacity(documents.len());
    for document in documents {
        let mut seen = std::collections::BTreeSet::new();
        for (field, value) in document.iter() {
            if !seen.insert(field) {
                return Err(CommandError::invalid());
            }
            let valid = match field {
                "key" | "partialFilterExpression" => matches!(value, BsonValue::Document(_)),
                "name" => matches!(value, BsonValue::String(_)),
                "unique" | "sparse" => matches!(value, BsonValue::Boolean(_)),
                _ => false,
            };
            if !valid {
                return Err(CommandError::options());
            }
        }
        let Some(BsonValue::Document(keys)) = document.get_first("key") else {
            return Err(CommandError::invalid());
        };
        if keys.len() == 1
            && keys
                .get_first("_id")
                .is_some_and(|value| value == &BsonValue::Int32(1))
            && document.get_first("unique").is_some()
        {
            return Err(DocumentIndexError::InvalidIdOptions
                .into_engine_error()
                .into());
        }
        let mut index = DocumentIndexRequest::new(keys.clone())?;
        if let Some(BsonValue::String(name)) = document.get_first("name") {
            index = index.with_name(name)?;
        }
        if let Some(BsonValue::Boolean(unique)) = document.get_first("unique") {
            index = index.with_unique(*unique);
        }
        if let Some(BsonValue::Boolean(sparse)) = document.get_first("sparse") {
            index = index.with_sparse(*sparse);
        }
        if let Some(BsonValue::Document(filter)) = document.get_first("partialFilterExpression") {
            index = index.with_partial_filter(DocumentFilter::new(filter.clone())?);
        }
        indexes.push(index);
    }
    let request =
        DocumentCreateIndexesRequest::new(namespace, indexes, DocumentWriteOptions::new())?;
    // Native execution repeats validation on its bounded worker. This first
    // pass is essential: a malformed late entry must not create a namespace.
    normalize_index_batch(request.indexes().to_vec().into_boxed_slice(), &mut || {
        if started.elapsed() >= timeout {
            Err(EngineError::deadline_exceeded(
                "Mongo index parsing deadline exceeded",
            ))
        } else {
            Ok(())
        }
    })?;
    Ok(request)
}
