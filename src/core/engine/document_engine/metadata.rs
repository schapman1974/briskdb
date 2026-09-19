//! Shared paged collection discovery. Retain positions, not catalog snapshots.

use super::*;
use crate::document::{BsonBinary, DocumentCursorBatch, DocumentListCollectionMetadataRequest};

impl Engine {
    pub(super) async fn list_document_database_names(
        &self,
        filter: DocumentFilter,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        limits: ResultLimits,
    ) -> EngineResult<Vec<String>> {
        let storage = self.inner.database.storage.clone();
        self.run_document_storage_task(cancellation, deadline, move |cancellation, control| {
            let mut check = || ensure_document_cpu_active(cancellation, &control);
            let matcher = DocumentMatcher::compile_with_check(filter.document(), &mut check)?;
            // Full Mongo listDatabases may filter by disk statistics even when
            // nameOnly is true. Never evaluate unavailable statistics as missing.
            require_database_name_filter(filter.document(), &mut check)?;
            let names = storage.document_database_names_controlled(Arc::clone(&control))?;
            let mut filtered = Vec::new();
            let mut budget = DocumentResultBudget::new(limits);
            for name in names {
                check()?;
                let row = BsonDocument::from_entries([("name", BsonValue::String(name.clone()))])
                    .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
                if matcher.matches_with_check(&row, &mut check)? {
                    budget.add_rows(1)?;
                    budget.add_bytes(
                        DOCUMENT_RESULT_ROW_BYTES + DOCUMENT_RESULT_VALUE_BYTES + name.len() as u64,
                    )?;
                    filtered.push(name);
                }
            }
            budget.finish()?;
            check()?;
            Ok(filtered)
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn start_collection_metadata_cursor(
        &self,
        owner: ConnectionOwner,
        session: &mut SessionInner,
        request_id: crate::document::DocumentRequestId,
        request: DocumentListCollectionMetadataRequest,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        limits: ResultLimits,
    ) -> EngineResult<DocumentExecution> {
        let (namespace, filter, name_only, options) = request.into_parts();
        require_catalog_read_options(&options)?;
        if options.skip() != 0 || options.limit().is_some() {
            return Err(unsupported(
                "collection metadata accepts only batch size and byte limits",
            ));
        }
        let storage = self.inner.database.storage.clone();
        let database = namespace.database().to_owned();
        let (identity, matcher) = self
            .run_document_storage_task(
                cancellation.clone(),
                deadline,
                move |cancellation, control| {
                    // Compile even for an absent database or a zero-sized first batch.
                    let matcher = Arc::new(DocumentMatcher::compile_with_check(
                        filter.document(),
                        &mut || ensure_document_cpu_active(cancellation, &control),
                    )?);
                    let identity =
                        storage.document_metadata_identity_controlled(&database, control)?;
                    Ok((identity, matcher))
                },
            )
            .await?;
        let Some((database_id, upper_id)) = identity else {
            return Ok(DocumentExecution::new(
                request_id,
                None,
                DocumentResult::Cursor(DocumentCursorBatch::from_validated(
                    namespace,
                    None,
                    Vec::new(),
                )),
            ));
        };
        let mut state = MetadataCursorState {
            namespace: namespace.clone(),
            database_id,
            upper_id,
            after_id: 0,
            matcher,
            name_only,
            batch_byte_limit: options.batch_byte_limit(),
        };
        let (documents, has_more) = self
            .read_collection_metadata_page(
                &mut state,
                cancellation,
                deadline,
                options.batch_size(),
                limits,
            )
            .await?;
        let cursor_id = if has_more {
            session
                .document_cursor_owner
                .get_or_insert_with(|| self.inner.document_cursors.owner(owner));
            Some(
                self.inner
                    .document_cursors
                    .insert(owner, RetainedCursorState::Collections(state))?,
            )
        } else {
            None
        };
        Ok(DocumentExecution::new(
            request_id,
            None,
            DocumentResult::Cursor(DocumentCursorBatch::from_validated(
                namespace, cursor_id, documents,
            )),
        ))
    }

    pub(super) async fn read_collection_metadata_page(
        &self,
        state: &mut MetadataCursorState,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        batch_size: u64,
        limits: ResultLimits,
    ) -> EngineResult<(Vec<BsonDocument>, bool)> {
        let mut next = state.clone();
        let storage = self.inner.database.storage.clone();
        let (after, documents, has_more) = self
            .run_document_storage_task(cancellation, deadline, move |cancellation, control| {
                let mut budget = DocumentResultBudget::new(limits);
                budget.add_bytes(next.namespace.to_string().len() as u64 + 8)?;
                let soft_limit = next.batch_byte_limit.unwrap_or(u64::MAX);
                if budget.bytes > soft_limit {
                    return Err(limit_exceeded(
                        "collection metadata cursor envelope exceeds batch byte limit",
                    ));
                }
                let mut documents = Vec::new();
                let mut has_more = batch_size == 0;
                if batch_size != 0 {
                    storage.scan_document_collection_metadata_controlled(
                        next.database_id,
                        (next.after_id, next.upper_id),
                        next.name_only,
                        Arc::clone(&control),
                        |id, name, options, uuid| {
                            let mut check = || ensure_document_cpu_active(cancellation, &control);
                            check()?;
                            let document = collection_metadata_document(name, options, uuid)?;
                            if !next.matcher.matches_with_check(&document, &mut check)? {
                                next.after_id = id;
                                return Ok(true);
                            }
                            if documents.len() as u64 == batch_size {
                                has_more = true;
                                return Ok(false);
                            }
                            let encoded = encode_document(&document).map_err(|error| {
                                // The stored options are valid; the additional
                                // metadata envelope can exceed BSON size/depth.
                                error.into_engine_error(BsonErrorContext::ClientInput)
                            })?;
                            check()?;
                            let bytes = DOCUMENT_RESULT_ROW_BYTES
                                + DOCUMENT_RESULT_VALUE_BYTES
                                + encoded.len() as u64;
                            if budget.bytes.saturating_add(bytes) > soft_limit {
                                if documents.is_empty() {
                                    return Err(limit_exceeded(
                                        "collection metadata row exceeds batch byte limit",
                                    ));
                                }
                                has_more = true;
                                return Ok(false);
                            }
                            budget.add_rows(1)?;
                            budget.add_bytes(bytes)?;
                            documents.push(document);
                            next.after_id = id;
                            Ok(true)
                        },
                    )?;
                }
                budget.finish()?;
                ensure_document_cpu_active(cancellation, &control)?;
                Ok((next.after_id, documents, has_more))
            })
            .await?;
        state.after_id = after;
        Ok((documents, has_more))
    }
}

fn require_database_name_filter(
    filter: &BsonDocument,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    let mut pending = vec![filter];
    while let Some(document) = pending.pop() {
        for (field, value) in document.iter() {
            check()?;
            match (field, value) {
                ("name", _) => {}
                ("$and" | "$or" | "$nor", BsonValue::Array(clauses)) => {
                    for clause in clauses {
                        if let BsonValue::Document(clause) = clause {
                            pending.push(clause);
                        }
                    }
                }
                _ => {
                    return Err(unsupported(
                        "database name discovery supports filters on name only; database statistics are not implemented",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn collection_metadata_document(
    name: String,
    options: Option<BsonDocument>,
    uuid: [u8; 16],
) -> EngineResult<BsonDocument> {
    let document = |entries| {
        BsonDocument::from_entries(entries)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))
    };
    let mut entries = vec![
        ("name", BsonValue::String(name)),
        ("type", BsonValue::String("collection".into())),
    ];
    if let Some(options) = options {
        entries.extend([
            ("options", BsonValue::Document(options)),
            (
                "info",
                BsonValue::Document(document(vec![
                    ("readOnly", BsonValue::Boolean(false)),
                    ("uuid", BsonValue::Binary(BsonBinary::new(4, uuid))),
                ])?),
            ),
            (
                "idIndex",
                BsonValue::Document(document(vec![
                    ("name", BsonValue::String("_id_".into())),
                    (
                        "key",
                        BsonValue::Document(document(vec![("_id", BsonValue::Int32(1))])?),
                    ),
                    ("unique", BsonValue::Boolean(true)),
                ])?),
            ),
        ]);
    }
    document(entries)
}
