//! Global aggregation over bounded source pages and shared pipeline execution.

use super::*;
use crate::document::{DocumentAggregateRequest, DocumentAggregator, DocumentRequestId};
use std::collections::VecDeque;

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_document_aggregate(
        &self,
        owner: ConnectionOwner,
        session: &mut SessionInner,
        request_id: DocumentRequestId,
        request: DocumentAggregateRequest,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        limits: ResultLimits,
    ) -> EngineResult<DocumentExecution> {
        let (namespace, pipeline, options) = request.into_parts();
        require_catalog_read_options(&options)?;
        if options.skip() != 0 || options.limit().is_some() {
            return Err(unsupported("aggregate pagination belongs in the pipeline"));
        }
        let storage = self.inner.database.storage.clone();
        let lookup = namespace.clone();
        let (collection_id, source, runner) = self
            .run_document_storage_task(
                cancellation.clone(),
                deadline,
                move |cancellation, control| {
                    let runner = DocumentAggregator::compile_with_check(&pipeline, &mut || {
                        ensure_document_cpu_active(cancellation, &control)
                    })?
                    .into_stream();
                    // Compile every stage first. The original leading match
                    // stays in the runner; only physical shard selection moves
                    // into the source, never filtering ahead of its work budget.
                    let source = id_routing::leading_match_source(
                        &storage,
                        &pipeline,
                        cancellation,
                        &control,
                    )?;
                    let collection = storage.document_collection_controlled(
                        lookup.database(),
                        lookup.collection(),
                        Arc::clone(&control),
                    )?;
                    ensure_document_cpu_active(cancellation, &control)?;
                    Ok((require_collection(collection)?.id(), source, runner))
                },
            )
            .await?;
        let mut state = CursorState {
            namespace: namespace.clone(),
            collection_id,
            source,
            projection: None,
            sorter: None,
            sort_after: None,
            after: None,
            skip: 0,
            remaining: None,
            batch_byte_limit: options.batch_byte_limit(),
            aggregation: Some(AggregateCursor {
                runner: Some(runner),
                pending: VecDeque::new(),
                bytes: 0,
                source_exhausted: false,
            }),
        };
        let plan = self
            .document_cursor_plan(&state, &options, cancellation.clone(), deadline)
            .await?;
        let (documents, has_more) = self
            .read_document_page(owner, &mut state, cancellation, deadline, &options, limits)
            .await?;
        let cursor_id = if has_more {
            session
                .document_cursor_owner
                .get_or_insert_with(|| self.inner.document_cursors.owner(owner));
            Some(self.inner.document_cursors.insert(owner, state)?)
        } else {
            None
        };
        Ok(DocumentExecution::new(
            request_id,
            Some(plan),
            DocumentResult::Cursor(crate::document::DocumentCursorBatch::from_validated(
                namespace, cursor_id, documents,
            )),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn read_aggregate_page(
        &self,
        owner: ConnectionOwner,
        state: &mut CursorState,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        options: &DocumentReadOptions,
        limits: ResultLimits,
    ) -> EngineResult<(Vec<BsonDocument>, bool)> {
        enforce_empty_result_limit(limits)?;
        let mut result_bytes = cursor_page_base_bytes(state, self.shard_count(), options);
        if state
            .batch_byte_limit
            .is_some_and(|limit| result_bytes > limit)
        {
            return Err(limit_exceeded(
                "cursor envelope cannot fit the batch byte limit",
            ));
        }
        if options.batch_size() == 0 {
            return Ok((Vec::new(), true));
        }
        let mut aggregate = state.aggregation.take().expect("aggregate cursor state");
        let byte_limit = state.batch_byte_limit.take();
        // Source documents are not output rows. A count with a small result
        // budget must still accept large unrelated payloads in its input.
        let source_options = DocumentReadOptions::new().with_batch_size(1)?;
        let source_limits = ResultLimits::new(1, BSON_MAX_DECODED_BYTES as u64)?;
        let mut documents = Vec::new();
        loop {
            if let Some(row) = aggregate.pending.front() {
                if documents.len() as u64 == options.batch_size() {
                    break;
                }
                let next_bytes = result_bytes
                    .checked_add(DOCUMENT_RESULT_ROW_BYTES + DOCUMENT_RESULT_VALUE_BYTES)
                    .and_then(|bytes| bytes.checked_add(row.encoded_len as u64))
                    .ok_or_else(result_size_overflow)?;
                if byte_limit.is_some_and(|limit| next_bytes > limit) {
                    if documents.is_empty() {
                        return Err(limit_exceeded(
                            "document cannot fit the cursor batch byte limit",
                        ));
                    }
                    break;
                }
                add_document_result_budget(&mut result_bytes, row.encoded_len, limits)?;
                if documents.len() as u64 >= limits.max_rows() {
                    return Err(limit_exceeded(
                        "document result exceeds the request row limit",
                    ));
                }
                let row = aggregate
                    .pending
                    .pop_front()
                    .expect("peeked aggregate output");
                aggregate.bytes -= row.retained_bytes;
                documents.push(row.document);
                continue;
            }
            let Some(runner) = aggregate.runner.as_ref() else {
                break;
            };
            if aggregate.source_exhausted || runner.is_input_exhausted() {
                aggregate = self
                    .run_document_storage_task(
                        cancellation.clone(),
                        deadline,
                        move |cancellation, control| {
                            let runner = aggregate.runner.take().expect("active aggregate stream");
                            let mut check = || ensure_document_cpu_active(cancellation, &control);
                            let output = runner.finish_with_check(&mut check)?;
                            for document in output {
                                push_output(&mut aggregate, document, &mut check)?;
                            }
                            check()?;
                            Ok(aggregate)
                        },
                    )
                    .await?;
                continue;
            }
            // A full streaming page retains its pipeline counters and source
            // position, not an eager collection-sized result buffer.
            if documents.len() as u64 == options.batch_size() {
                break;
            }
            let (source, has_more) = self
                .read_document_source_page(
                    owner,
                    state,
                    cancellation.clone(),
                    deadline,
                    &source_options,
                    source_limits,
                )
                .await?;
            aggregate.source_exhausted = !has_more;
            aggregate = self
                .run_document_storage_task(
                    cancellation.clone(),
                    deadline,
                    move |cancellation, control| {
                        let mut check = || ensure_document_cpu_active(cancellation, &control);
                        for document in source {
                            let result = aggregate
                                .runner
                                .as_mut()
                                .expect("active aggregate stream")
                                .push_validated_with_check(document, &mut check)?;
                            if let Some(document) = result {
                                push_output(&mut aggregate, document, &mut check)?;
                            }
                        }
                        check()?;
                        Ok(aggregate)
                    },
                )
                .await?;
        }
        let has_more = aggregate.runner.is_some() || !aggregate.pending.is_empty();
        state.batch_byte_limit = byte_limit;
        state.aggregation = Some(aggregate);
        Ok((documents, has_more))
    }
}

fn push_output(
    aggregate: &mut AggregateCursor,
    document: BsonDocument,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    check()?;
    let retained_bytes = DocumentAggregator::document_retained_bytes(&document, check)?;
    let encoded_len = encode_document(&document)
        .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?
        .len();
    aggregate.bytes = aggregate
        .bytes
        .checked_add(retained_bytes)
        .filter(|bytes| *bytes <= BSON_MAX_DECODED_BYTES)
        .ok_or_else(|| limit_exceeded("aggregate result retention limit exceeded"))?;
    aggregate.pending.push_back(AggregateRow {
        document,
        encoded_len,
        retained_bytes,
    });
    check()
}
