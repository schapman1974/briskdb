//! Matching Ready declarations need schema stability, not sole-process ownership.
//!
//! Callers already hold local migration admission and the lifetime shared process
//! lease. Use a fresh validated manifest snapshot; never upgrade the lease here.
//! A real mutation returns to the original exclusive path, which reloads metadata
//! AFTER upgrading (flock upgrades are not atomic on every supported platform).

use super::*;

pub(super) fn matching_declaration<'a>(
    collection: &'a DocumentCollectionMetadata,
    name: &str,
    declaration: Option<(&BsonDocument, bool)>,
    strict_compatibility: bool,
    control: &OperationControl,
) -> EngineResult<Option<&'a DocumentIndexMetadata>> {
    let existing = collection
        .indexes()
        .iter()
        .find(|index| index.name() == name);
    if let Some((specification, unique)) = declaration {
        if strict_compatibility {
            let proposed = DocumentIndexMetadata::from_validated_parts(
                DocumentIndexId::from_validated(1),
                name.to_owned(),
                specification.clone(),
                unique,
                false,
                DocumentIndexLifecycle::PendingBuild,
            );
            if let Some(existing) = existing {
                if !equivalent_definition(existing, &proposed) {
                    return Err(DocumentIndexError::KeySpecsConflict.into_engine_error());
                }
            } else {
                for index in collection.indexes() {
                    ensure_control_active(control, "while checking document index conflicts")?;
                    if equivalent_definition(index, &proposed) {
                        return Err(DocumentIndexError::OptionsConflict.into_engine_error());
                    }
                }
            }
        }
        if let Some(existing) = existing.filter(|_| !strict_compatibility) {
            let canonical_keys = !specification.is_empty()
                && specification
                    .iter()
                    .all(|(_, value)| matches!(value, BsonValue::Int32(1 | -1)));
            let same_spec = existing.specification().representation_eq(specification)
                || (canonical_keys && existing.specification() == specification);
            if !same_spec || existing.is_unique() != unique {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "document index name already has a different declaration",
                ));
            }
        }
    }
    Ok(existing)
}

fn ready_count(collection: &DocumentCollectionMetadata) -> u64 {
    collection
        .indexes()
        .iter()
        .filter(|index| index.lifecycle() == DocumentIndexLifecycle::Ready)
        .count() as u64
}

impl Storage {
    pub(super) fn existing_index_noop(
        &self,
        database: &str,
        collection_name: &str,
        name: &str,
        declaration: Option<(&BsonDocument, bool)>,
        control: Arc<OperationControl>,
    ) -> EngineResult<Option<BuildOutcome>> {
        let read_control = Arc::clone(&control);
        let result = self.read_document_manifest(control, |connection| {
            ensure_control_active(&read_control, "before checking an existing document index")?;
            let catalog = load_catalog_rows(connection)?;
            let Some(collection) = catalog.collection(database, collection_name) else {
                return Ok(None);
            };
            let Some(index) =
                matching_declaration(collection, name, declaration, false, &read_control)?
            else {
                return Ok(None);
            };
            if index.is_built_in() {
                return Err(DocumentIndexError::Protected.into_engine_error());
            }
            if index.lifecycle() != DocumentIndexLifecycle::Ready {
                return Ok(None);
            }
            ensure_control_active(&read_control, "before returning an existing document index")?;
            let count = ready_count(collection);
            Ok(Some(BuildOutcome {
                metadata: index.clone(),
                before: count,
                after: count,
            }))
        });
        self.fail_closed_on_corruption(result)
    }

    pub(super) fn existing_index_batch_noop(
        &self,
        namespace: &DocumentNamespace,
        definitions: &[crate::document::DocumentIndexBuildDefinition],
        control: Arc<OperationControl>,
    ) -> EngineResult<Option<(u64, Box<[String]>)>> {
        let read_control = Arc::clone(&control);
        let result = self.read_document_manifest(control, |connection| {
            let catalog = load_catalog_rows(connection)?;
            let Some(collection) = catalog.collection(namespace.database(), namespace.collection())
            else {
                return Ok(None);
            };
            let mut names = Vec::with_capacity(definitions.len());
            for definition in definitions {
                ensure_control_active(&read_control, "while checking existing document indexes")?;
                let crate::document::DocumentIndexBuildDefinition::Secondary {
                    specification,
                    name,
                    unique,
                    reuse_equivalent,
                } = definition
                else {
                    names.push("_id_".to_owned());
                    continue;
                };
                if *reuse_equivalent {
                    let proposed = DocumentIndexMetadata::from_validated_parts(
                        DocumentIndexId::from_validated(1),
                        name.clone(),
                        specification.clone(),
                        *unique,
                        false,
                        DocumentIndexLifecycle::PendingBuild,
                    );
                    let mut reused = None;
                    for existing in collection.indexes() {
                        ensure_control_active(
                            &read_control,
                            "while resolving an equivalent document index",
                        )?;
                        if existing.lifecycle() == DocumentIndexLifecycle::Ready
                            && equivalent_definition(existing, &proposed)
                        {
                            reused = Some(existing.name().to_owned());
                            break;
                        }
                    }
                    if let Some(name) = reused {
                        names.push(name);
                        continue;
                    }
                }
                let existing = matching_declaration(
                    collection,
                    name,
                    Some((specification, *unique)),
                    true,
                    &read_control,
                )?;
                let Some(existing) = existing else {
                    return Ok(None);
                };
                if existing.is_built_in() {
                    return Err(DocumentIndexError::Protected.into_engine_error());
                }
                if existing.lifecycle() != DocumentIndexLifecycle::Ready {
                    // Stop at the FIRST would-mutate entry. Do not pre-report a
                    // later conflict before the normal pipeline's durable prefix.
                    return Ok(None);
                }
                names.push(existing.name().to_owned());
            }
            ensure_control_active(&read_control, "before returning existing document indexes")?;
            Ok(Some((ready_count(collection), names.into_boxed_slice())))
        });
        self.fail_closed_on_corruption(result)
    }
}
