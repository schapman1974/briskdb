//! Bounded, eager parsing before implicit collection creation or index builds.

use super::*;
use crate::document::{DocumentIndexError, DocumentIndexRequest, normalize_index_batch};

pub(in crate::protocol::mongo) struct PreparedIndexes {
    pub(super) request: DocumentCreateIndexesRequest,
    pub(super) warnings: Vec<BsonValue>,
}

pub(super) fn reply(before: u64, after: u64, warnings: Vec<BsonValue>) -> BsonDocument {
    let mut result = fields([
        ("ok", BsonValue::Double(1.0)),
        ("numIndexesBefore", BsonValue::Int64(before as i64)),
        ("numIndexesAfter", BsonValue::Int64(after as i64)),
    ]);
    if !warnings.is_empty() {
        result
            .push("briskdbIndexWarnings", BsonValue::Array(warnings))
            .expect("static index warning field");
    }
    result
}

pub(super) fn prepare_drop(
    request: &Request,
    namespace: DocumentNamespace,
) -> Result<DocumentDropIndexesRequest> {
    if !request.sequences.is_empty() {
        return Err(CommandError::options());
    }
    let mut seen = std::collections::BTreeSet::new();
    if request.body.iter().any(|(name, _)| !seen.insert(name)) {
        return Err(CommandError::invalid());
    }
    let index = request
        .body
        .get_first("index")
        .ok_or_else(CommandError::invalid)?;
    let BsonValue::String(name) = index else {
        return Err(CommandError::new(
            14,
            "TypeMismatch",
            "index selection must be a string",
        ));
    };
    if name == "*" {
        Ok(DocumentDropIndexesRequest::all(
            namespace,
            DocumentWriteOptions::new(),
        ))
    } else {
        DocumentDropIndexesRequest::new(namespace, name, DocumentWriteOptions::new())
            .map_err(Into::into)
    }
}

pub(super) fn prepare(
    request: &Request,
    namespace: DocumentNamespace,
    started: Instant,
    timeout: Duration,
) -> Result<PreparedIndexes> {
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
    let mut warnings = Vec::new();
    let mut check = || {
        if started.elapsed() >= timeout {
            Err(EngineError::deadline_exceeded(
                "Mongo index parsing deadline exceeded",
            ))
        } else {
            Ok(())
        }
    };
    for document in documents {
        check()?;
        let mut seen = std::collections::BTreeSet::new();
        for (field, value) in document.iter() {
            if !seen.insert(field) {
                return Err(CommandError::invalid());
            }
            let valid = match field {
                "key" | "partialFilterExpression" => matches!(value, BsonValue::Document(_)),
                "name" => matches!(value, BsonValue::String(_)),
                "unique" | "sparse" | "background" => matches!(value, BsonValue::Boolean(_)),
                "expireAfterSeconds" => valid_ttl(value),
                _ => false,
            };
            if !valid {
                return Err(CommandError::options());
            }
        }
        let Some(BsonValue::Document(keys)) = document.get_first("key") else {
            return Err(CommandError::invalid());
        };
        let hashed = keys
            .iter()
            .any(|(_, value)| matches!(value, BsonValue::String(value) if value == "hashed"));
        let ttl = document.get_first("expireAfterSeconds").is_some();
        let unique = document.get_first("unique") == Some(&BsonValue::Boolean(true));
        // A performance-only fallback must never weaken a uniqueness constraint,
        // including the implicit unique built-in ID index.
        if (hashed || ttl) && (unique || (keys.len() == 1 && keys.get_first("_id").is_some())) {
            return Err(CommandError::unsupported());
        }
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
        let mut effective = BsonDocument::new();
        for (field, value) in keys.iter() {
            check()?;
            effective
                .push(
                    field,
                    if matches!(value, BsonValue::String(value) if value == "hashed") {
                        BsonValue::Int32(1)
                    } else {
                        value.clone()
                    },
                )
                .map_err(|_| CommandError::invalid())?;
        }
        let mut index = DocumentIndexRequest::new(effective)?;
        if let Some(BsonValue::String(name)) = document.get_first("name") {
            index = index.with_name(name)?;
        } else if hashed {
            index = index.with_name(requested_name(keys)?)?;
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
        let mut reduced = Vec::new();
        if hashed {
            reduced.push(BsonValue::from("hashed: ascending equality indexing"));
        }
        if ttl {
            reduced.push(BsonValue::from("ttl: expiration is not performed"));
        }
        if document.get_first("background") == Some(&BsonValue::Boolean(true)) {
            reduced.push(BsonValue::from("background: builds run synchronously"));
        }
        if !reduced.is_empty() {
            // Validate/default the name with the same bounded native normalizer.
            let (_, name) = crate::document::normalize_index_definition(
                index.keys(),
                index.name(),
                &mut check,
            )?;
            warnings.push(BsonValue::Document(fields([
                ("name", BsonValue::String(name)),
                ("reducedBehavior", BsonValue::Array(reduced)),
            ])));
        }
        indexes.push(index);
    }
    let request =
        DocumentCreateIndexesRequest::new(namespace, indexes, DocumentWriteOptions::new())?;
    // Native execution repeats validation on its bounded worker. This first
    // pass is essential: a malformed late entry must not create a namespace.
    normalize_index_batch(request.indexes().to_vec().into_boxed_slice(), &mut check)?;
    // Counts are fixed-width Int64 values: preflight the exact reply shape before
    // any namespace or index mutation, not after an oversized acknowledgement.
    encode_document_with_options(
        &reply(0, 0, warnings.clone()),
        &BsonCodecOptions::new().with_max_document_bytes(wire::MAX_BOOTSTRAP_BSON_BYTES),
    )
    .map_err(|_| {
        CommandError::new(
            10334,
            "BSONObjectTooLarge",
            "index warnings exceed reply limit",
        )
    })?;
    check()?;
    Ok(PreparedIndexes { request, warnings })
}

fn valid_ttl(value: &BsonValue) -> bool {
    match value {
        BsonValue::Int32(value) => *value >= 0,
        BsonValue::Int64(value) => *value >= 0,
        BsonValue::Double(value) => value.is_finite() && *value >= 0.0,
        _ => false,
    }
}

fn requested_name(keys: &BsonDocument) -> Result<String> {
    let mut name = String::new();
    for (field, direction) in keys.iter() {
        let suffix = match direction {
            BsonValue::String(value) if value == "hashed" => "_hashed",
            BsonValue::Int32(_)
            | BsonValue::Int64(_)
            | BsonValue::Double(_)
            | BsonValue::Decimal128(_)
                if direction == &BsonValue::Int32(1) =>
            {
                "_1"
            }
            BsonValue::Int32(_)
            | BsonValue::Int64(_)
            | BsonValue::Double(_)
            | BsonValue::Decimal128(_)
                if direction == &BsonValue::Int32(-1) =>
            {
                "_-1"
            }
            _ => return Err(CommandError::invalid()),
        };
        if name.len() + usize::from(!name.is_empty()) + field.len() + suffix.len()
            > crate::document::MAX_DOCUMENT_INDEX_NAME_BYTES
        {
            return Err(CommandError::new(
                10334,
                "BSONObjectTooLarge",
                "generated index name exceeds limit",
            ));
        }
        if !name.is_empty() {
            name.push('_');
        }
        name.push_str(field);
        name.push_str(suffix);
    }
    Ok(name)
}

#[cfg(test)]
mod tests;
