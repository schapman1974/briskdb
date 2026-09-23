//! Protocol-neutral paged discovery of built index definitions.

use super::*;
use crate::document::{
    DocumentCursorBatch, DocumentIndexMetadata, DocumentListIndexMetadataRequest,
};

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn start_index_metadata_cursor(
        &self,
        owner: ConnectionOwner,
        session: &mut SessionInner,
        request_id: crate::document::DocumentRequestId,
        request: DocumentListIndexMetadataRequest,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        limits: ResultLimits,
    ) -> EngineResult<DocumentExecution> {
        let (namespace, options) = request.into_parts();
        require_catalog_read_options(&options)?;
        if options.skip() != 0 || options.limit().is_some() {
            return Err(unsupported(
                "index metadata accepts only batch size and byte limits",
            ));
        }
        let storage = self.inner.database.storage.clone();
        let target = namespace.clone();
        let identity = self
            .run_document_storage_task(
                cancellation.clone(),
                deadline,
                move |_cancellation, control| {
                    storage.document_index_metadata_identity_controlled(&target, control)
                },
            )
            .await?;
        let Some((collection_id, upper_id)) = identity else {
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
        let mut state = IndexMetadataCursorState {
            namespace: namespace.clone(),
            collection_id,
            upper_id,
            after_name: None,
            batch_byte_limit: options.batch_byte_limit(),
        };
        let (documents, has_more) = self
            .read_index_metadata_page(
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
                    .insert(owner, RetainedCursorState::Indexes(state))?,
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

    pub(super) async fn read_index_metadata_page(
        &self,
        state: &mut IndexMetadataCursorState,
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
                        "index metadata cursor envelope exceeds batch byte limit",
                    ));
                }
                let mut documents = Vec::new();
                let mut has_more = batch_size == 0;
                if batch_size != 0 {
                    let after = next.after_name.clone();
                    storage.scan_document_index_metadata_controlled(
                        next.collection_id,
                        next.upper_id,
                        after.as_deref(),
                        Arc::clone(&control),
                        |index| {
                            ensure_document_cpu_active(cancellation, &control)?;
                            if documents.len() as u64 == batch_size {
                                has_more = true;
                                return Ok(false);
                            }
                            let document = index_metadata_document(&index)?;
                            let encoded = encode_document(&document).map_err(|error| {
                                error.into_engine_error(BsonErrorContext::ClientInput)
                            })?;
                            ensure_document_cpu_active(cancellation, &control)?;
                            let bytes = DOCUMENT_RESULT_ROW_BYTES
                                + DOCUMENT_RESULT_VALUE_BYTES
                                + encoded.len() as u64;
                            if budget.bytes.saturating_add(bytes) > soft_limit {
                                if documents.is_empty() {
                                    return Err(limit_exceeded(
                                        "index metadata row exceeds batch byte limit",
                                    ));
                                }
                                has_more = true;
                                return Ok(false);
                            }
                            budget.add_rows(1)?;
                            budget.add_bytes(bytes)?;
                            next.after_name = Some(if index.is_built_in() {
                                String::new()
                            } else {
                                index.name().to_owned()
                            });
                            documents.push(document);
                            Ok(true)
                        },
                    )?;
                }
                budget.finish()?;
                ensure_document_cpu_active(cancellation, &control)?;
                Ok((next.after_name, documents, has_more))
            })
            .await?;
        state.after_name = after;
        Ok((documents, has_more))
    }
}

fn index_metadata_document(index: &DocumentIndexMetadata) -> EngineResult<BsonDocument> {
    let definition = index.definition().ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::DataCorruption,
            "built index has no supported definition",
        )
    })?;
    let mut fields = vec![
        ("name", BsonValue::String(index.name().to_owned())),
        ("key", BsonValue::Document(definition.keys().clone())),
    ];
    if !index.is_built_in() && index.is_unique() {
        fields.push(("unique", BsonValue::Boolean(true)));
    }
    if definition.sparse() {
        fields.push(("sparse", BsonValue::Boolean(true)));
    }
    if let Some(filter) = definition.partial_filter() {
        fields.push((
            "partialFilterExpression",
            BsonValue::Document(filter.clone()),
        ));
    }
    // The stored specification version is BriskDB's, not a MongoDB index version.
    BsonDocument::from_entries(fields)
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))
}
