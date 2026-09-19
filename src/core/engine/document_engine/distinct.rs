//! Bounded global distinct over the existing natural-order read path.

use super::*;
use crate::document::{
    DEFAULT_DOCUMENT_BATCH_SIZE, DocumentDistinct, DocumentDistinctRequest, DocumentRequestId,
};

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_document_distinct(
        &self,
        owner: ConnectionOwner,
        request_id: DocumentRequestId,
        request: DocumentDistinctRequest,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        limits: ResultLimits,
    ) -> EngineResult<DocumentExecution> {
        let (namespace, field, filter, options) = request.into_parts();
        require_catalog_read_options(&options)?;
        if options.skip() != 0
            || options.limit().is_some()
            || options.batch_size() != DEFAULT_DOCUMENT_BATCH_SIZE
            || options.batch_byte_limit().is_some()
        {
            return Err(unsupported("distinct does not accept pagination options"));
        }
        let storage = self.inner.database.storage.clone();
        let lookup = namespace.clone();
        let (collection_id, source, mut distinct) = self
            .run_document_storage_task(
                cancellation.clone(),
                deadline,
                move |cancellation, control| {
                    let distinct = DocumentDistinct::new(&field)?;
                    let source = prepare_filter_route(&storage, filter, cancellation, &control)?;
                    let collection = storage.document_collection_controlled(
                        lookup.database(),
                        lookup.collection(),
                        Arc::clone(&control),
                    )?;
                    ensure_document_cpu_active(cancellation, &control)?;
                    Ok((require_collection(collection)?.id(), source, distinct))
                },
            )
            .await?;
        let mut state = CursorState {
            namespace,
            collection_id,
            source,
            projection: None,
            sorter: None,
            sort_after: None,
            after: None,
            skip: 0,
            remaining: None,
            batch_byte_limit: None,
        };
        let plan = self.document_cursor_plan(&state)?;
        let mut budget = DocumentResultBudget::new(limits);
        budget.add_plan(&plan)?;
        budget.add_bytes(0)?;
        // Only one bounded source document is buffered per page, independent
        // of the caller's output budget. A tiny scalar distinct result must
        // still work when an input has a large unrelated payload. The existing
        // global merge also bounds its shard frontier. No public cursor slot,
        // transaction or SQLite lease is retained between these internal pages.
        let source_options = DocumentReadOptions::new().with_batch_size(1)?;
        let source_limits = ResultLimits::new(1, BSON_MAX_DECODED_BYTES as u64)?;
        loop {
            let (documents, has_more) = self
                .read_document_page(
                    owner,
                    &mut state,
                    cancellation.clone(),
                    deadline,
                    &source_options,
                    source_limits,
                )
                .await?;
            (distinct, budget) = self
                .run_document_storage_task(
                    cancellation.clone(),
                    deadline,
                    move |cancellation, control| {
                        for document in documents {
                            distinct.push_validated_with_check(
                                &document,
                                &mut || ensure_document_cpu_active(cancellation, &control),
                                &mut |value| {
                                    budget.add_rows(1)?;
                                    budget.add_value(value, &mut || {
                                        ensure_document_cpu_active(cancellation, &control)
                                    })
                                },
                            )?;
                        }
                        ensure_document_cpu_active(cancellation, &control)?;
                        Ok((distinct, budget))
                    },
                )
                .await?;
            if !has_more {
                break;
            }
        }
        let values = self
            .run_document_storage_task(cancellation, deadline, move |cancellation, control| {
                distinct.into_values_with_check(&mut || {
                    ensure_document_cpu_active(cancellation, &control)
                })
            })
            .await?;
        Ok(DocumentExecution::new(
            request_id,
            Some(plan),
            DocumentResult::Distinct(values.into_boxed_slice()),
        ))
    }
}
