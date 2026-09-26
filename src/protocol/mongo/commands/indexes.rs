//! Bounded, eager parsing before implicit collection creation or index builds.

use super::*;
use crate::document::{DocumentIndexError, DocumentIndexRequest, normalize_index_batch};

pub(in crate::protocol::mongo) struct PreparedIndexes {
    pub(super) request: DocumentCreateIndexesRequest,
    pub(super) warnings: Vec<BsonValue>,
    pub(super) model_names: Option<Vec<ModelName>>,
}

pub(super) struct ModelName {
    requested: String,
    index: Option<usize>,
    warning: Option<usize>,
}

pub(super) fn model_reply(
    before: u64,
    after: u64,
    mut warnings: Vec<BsonValue>,
    models: Vec<ModelName>,
    names: &[String],
) -> BsonDocument {
    let mut resolved = Vec::with_capacity(models.len());
    for model in models {
        let name = model.index.map_or(&model.requested, |index| &names[index]);
        if name != &model.requested {
            if let Some(warning) = model.warning {
                if let BsonValue::Document(warning) = &mut warnings[warning] {
                    warning
                        .push("reusedIndex", BsonValue::String(name.clone()))
                        .expect("static field");
                }
            }
        }
        resolved.push(BsonValue::String(name.clone()));
    }
    let mut result = reply(before, after, warnings);
    result
        .push("briskdbIndexNames", BsonValue::Array(resolved))
        .expect("static field");
    result
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
    let compatibility =
        request.body.get_first("briskdbIndexModelCompatibility") == Some(&BsonValue::Boolean(true));
    let mut indexes = Vec::with_capacity(documents.len());
    let mut warnings = Vec::new();
    let mut model_names = compatibility.then(Vec::new);
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
        let text = keys
            .iter()
            .any(|(_, value)| matches!(value, BsonValue::String(value) if value == "text"));
        let ttl = document.get_first("expireAfterSeconds").is_some();
        let descending =
            compatibility && keys.iter().any(|(_, value)| value == &BsonValue::Int32(-1));
        let unique = document.get_first("unique") == Some(&BsonValue::Boolean(true));
        // A performance-only fallback must never weaken a uniqueness constraint,
        // including the implicit unique built-in ID index.
        if ((hashed || ttl || text) && unique)
            || (!compatibility
                && !text
                && (hashed || ttl)
                && keys.len() == 1
                && keys.get_first("_id").is_some())
        {
            return Err(CommandError::unsupported());
        }
        if keys.len() == 1
            && keys
                .get_first("_id")
                .is_some_and(|value| value == &BsonValue::Int32(1))
            && (document.get_first("unique").is_some() && (!compatibility || unique))
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
                    if matches!(value, BsonValue::String(value) if value == "hashed" || value == "text")
                        || (descending && value == &BsonValue::Int32(-1)) {
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
        } else if hashed || text || descending {
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
        if text {
            if let Some(models) = &mut model_names {
                models.push(ModelName {
                    requested: index.name().expect("text name").to_owned(),
                    index: None,
                    warning: Some(warnings.len()),
                });
            }
            warnings.push(skipped_text_warning(&index, &mut check)?);
            continue;
        }
        let mut reduced = Vec::new();
        if descending {
            reduced.push(BsonValue::from("descending: ascending equality indexing"));
        }
        if hashed {
            reduced.push(BsonValue::from("hashed: ascending equality indexing"));
        }
        if ttl {
            reduced.push(BsonValue::from("ttl: expiration is not performed"));
        }
        if document.get_first("background") == Some(&BsonValue::Boolean(true)) {
            reduced.push(BsonValue::from("background: builds run synchronously"));
        }
        let warning = (!reduced.is_empty()).then_some(warnings.len());
        index = index.with_equivalent_reuse(compatibility && !reduced.is_empty());
        if let Some(models) = &mut model_names {
            models.push(ModelName {
                requested: match index.name() {
                    Some(name) => name.to_owned(),
                    None => requested_name(keys)?,
                },
                index: Some(indexes.len()),
                warning,
            });
        }
        if !reduced.is_empty() {
            // Built-in requests are validated/no-op'd by normalize_index_batch,
            // which ignores the requested alias. Do not apply the secondary
            // reserved-name rule to a legitimate explicit `_id_` request.
            let name = if index.keys().len() == 1
                && index.keys().get_first("_id") == Some(&BsonValue::Int32(1))
            {
                if compatibility {
                    index
                        .name()
                        .map(str::to_owned)
                        .unwrap_or(requested_name(keys)?)
                } else {
                    "_id_".to_owned()
                }
            } else {
                crate::document::normalize_index_definition(index.keys(), index.name(), &mut check)?
                    .1
            };
            warnings.push(BsonValue::Document(fields([
                ("name", BsonValue::String(name)),
                ("reducedBehavior", BsonValue::Array(reduced)),
            ])));
        }
        indexes.push(index);
    }
    if indexes.is_empty() {
        // Keep the normal exclusive schema/control/corruption checks and exact
        // Ready counts for an all-skipped batch. A built-in request is already
        // a storage no-op: never allocate a secondary identity or fake metadata.
        indexes.push(DocumentIndexRequest::new(fields([(
            "_id",
            BsonValue::Int32(1),
        )]))?);
    }
    let mut request =
        DocumentCreateIndexesRequest::new(namespace, indexes, DocumentWriteOptions::new())?;
    if compatibility {
        request = request.with_resolved_names();
    }
    // Native execution repeats validation on its bounded worker. This first
    // pass is essential: a malformed late entry must not create a namespace.
    normalize_index_batch(request.indexes().to_vec().into_boxed_slice(), &mut check)?;
    // Counts are fixed-width Int64 values: preflight the exact reply shape before
    // any namespace or index mutation, not after an oversized acknowledgement.
    let preflight = if let Some(models) = &model_names {
        // Reuse can select a longer existing name, and adds a warning field.
        // Bound the complete opt-in reply before implicit namespace creation.
        let worst = "x".repeat(crate::document::MAX_DOCUMENT_INDEX_NAME_BYTES);
        let mut preflight_warnings = warnings.clone();
        for warning in &mut preflight_warnings {
            if let BsonValue::Document(warning) = warning {
                warning
                    .push("reusedIndex", BsonValue::String(worst.clone()))
                    .expect("static field");
            }
        }
        let mut result = reply(0, 0, preflight_warnings);
        result
            .push(
                "briskdbIndexNames",
                BsonValue::Array(
                    models
                        .iter()
                        .map(|_| BsonValue::String(worst.clone()))
                        .collect(),
                ),
            )
            .expect("static field");
        result
    } else {
        reply(0, 0, warnings.clone())
    };
    encode_document_with_options(
        &preflight,
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
    Ok(PreparedIndexes {
        request,
        warnings,
        model_names,
    })
}

fn skipped_text_warning(
    index: &DocumentIndexRequest,
    check: &mut dyn FnMut() -> crate::core::EngineResult<()>,
) -> Result<BsonValue> {
    if index.sparse() && index.partial_filter().is_some() {
        return Err(CommandError::unsupported());
    }
    let name = index
        .name()
        .expect("text declarations have requested names");
    // Validate every path/direction and resource bound even though nothing is
    // built. Only a single _id declaration may use the reserved built-in alias.
    let validation_name = if index.keys().len() == 1
        && index.keys().get_first("_id").is_some()
        && matches!(name, "_id" | "_id_")
    {
        "_id_1"
    } else {
        name
    };
    crate::document::normalize_index_definition(index.keys(), Some(validation_name), check)?;
    // TinyMongo does not compile membership predicates for skipped text models.
    // No part of this compound declaration (including hashed/TTL/background
    // options) takes effect, so do not emit diagnostics claiming a partial build.
    Ok(BsonValue::Document(fields([
        ("name", BsonValue::from(name)),
        ("skipped", BsonValue::Boolean(true)),
        (
            "reducedBehavior",
            BsonValue::Array(vec![BsonValue::from(
                "text: entire index is skipped; $text queries are not supported",
            )]),
        ),
    ])))
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
            BsonValue::String(value) if value == "text" => "_text",
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
