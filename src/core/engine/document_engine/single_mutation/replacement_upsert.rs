//! Replacement upserts. Namespace creation remains an adapter/catalog operation;
//! natural-order reservation must happen outside the shard write transaction.

use super::*;

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_document_replacement_upsert(
        &self,
        owner: ConnectionOwner,
        request_id: DocumentRequestId,
        request: DocumentReplaceRequest,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        limits: ResultLimits,
    ) -> EngineResult<DocumentExecution> {
        let max_document_bytes = request.max_document_bytes();
        let (namespace, filter, replacement, options) = request.into_parts();
        require_replacement_options(options.with_upsert(false))?;
        // The retained query is needed only after a miss. Clone bounded BSON on
        // a controlled worker, never on the async coordinator.
        let (filter, first_filter, replacement) = self
            .run_document_storage_task(
                cancellation.clone(),
                deadline,
                move |cancellation, control| {
                    ensure_document_cpu_active(cancellation, &control)?;
                    let first = filter.clone();
                    ensure_document_cpu_active(cancellation, &control)?;
                    Ok((Arc::new(filter), first, Arc::new(replacement)))
                },
            )
            .await?;
        let execution = self
            .run_document_single_mutation(
                owner,
                request_id,
                namespace.clone(),
                first_filter,
                DocumentReadOptions::new(),
                Mutation::Replace {
                    document: Arc::clone(&replacement),
                    max_document_bytes,
                    returns: MutationReturn::Counts,
                },
                cancellation.clone(),
                deadline,
                limits,
            )
            .await?;
        if !matches!(execution.result(), DocumentResult::Update(result) if result.matched_count() == 0)
        {
            return Ok(execution);
        }
        let (_, plan, _) = execution.into_parts();
        let plan = plan.expect("replacement plan");
        let storage = self.inner.database.storage.clone();
        let prepare_replacement = Arc::clone(&replacement);
        let (collection_id, matcher, prepared, inserted, first_order) = self
            .run_document_storage_task(
                cancellation.clone(),
                deadline,
                move |cancellation, control| {
                    let mut check = || ensure_document_cpu_active(cancellation, &control);
                    check()?;
                    let matcher =
                        DocumentMatcher::compile_with_check(filter.document(), &mut check)?;
                    let document = synthesize_replacement(
                        filter.document(),
                        &prepare_replacement,
                        max_document_bytes,
                        &mut check,
                    )?;
                    let id = document.get_first("_id").expect("upsert identity").clone();
                    let prepared = storage.prepare_document_write(&document)?;
                    enforce_prepared_write_budget(
                        std::slice::from_ref(&prepared),
                        cancellation,
                        &control,
                    )?;
                    let collection_id =
                        require_collection(storage.document_collection_controlled(
                            namespace.database(),
                            namespace.collection(),
                            Arc::clone(&control),
                        )?)?
                        .id();
                    let inserted = DocumentExecution::new(
                        request_id,
                        Some(plan),
                        DocumentResult::Update(DocumentUpdateResult::new(0, 0, Some(id))?),
                    );
                    enforce_execution_result_limits_with_check(&inserted, limits, &mut check)?;
                    check()?;
                    // Reservation can leave a gap if another writer wins, just like
                    // ordinary inserts. Never acquire the manifest lock under a
                    // shard write lock: that reverses the established lock order.
                    let first_order = storage.reserve_document_natural_orders_controlled(
                        collection_id,
                        1,
                        Arc::clone(&control),
                    )?;
                    Ok((collection_id, matcher, prepared, inserted, first_order))
                },
            )
            .await?;
        let shard = prepared.shard();
        self.run_document_shard_controlled(
            shard,
            owner,
            cancellation,
            deadline,
            move |storage, connection, cancellation, control| {
                let transaction =
                    Transaction::new_unchecked(connection, TransactionBehavior::Immediate)
                        .map_err(sqlite_error::statement)?;
                // Recheck the target shard under its write lock. Exact-ID
                // concurrent upserts therefore update the winner, not a stale
                // missing document. Non-ID queries do not promise a global
                // snapshot or uniqueness without a supporting unique index.
                let plan = inserted.plan().expect("upsert plan");
                let record = if let DocumentPlan::Point(point) = plan {
                    debug_assert_eq!(point.shard(), shard);
                    storage.get_document_on_connection(
                        &transaction,
                        collection_id,
                        shard,
                        point.id_key(),
                        cancellation,
                    )?
                } else {
                    select_candidate(
                        storage,
                        &transaction,
                        collection_id,
                        shard,
                        Some(&matcher),
                        None,
                        None,
                        cancellation,
                        control,
                    )?
                    .map(|current| {
                        storage.get_document_on_connection(
                            &transaction,
                            collection_id,
                            shard,
                            &current.key,
                            cancellation,
                        )
                    })
                    .transpose()?
                    .flatten()
                };
                let execution = if record.is_some() {
                    replace_record(
                        storage,
                        &transaction,
                        record,
                        &replacement,
                        max_document_bytes,
                        MutationReturn::Counts,
                        None,
                        request_id,
                        plan.clone(),
                        limits,
                        cancellation,
                        control,
                    )?
                } else {
                    ensure_document_cpu_active(cancellation, control)?;
                    storage.insert_prepared_document_on_connection(
                        &transaction,
                        collection_id,
                        first_order,
                        shard,
                        &prepared,
                        cancellation,
                    )?;
                    inserted
                };
                ensure_document_cpu_active(cancellation, control)?;
                transaction.commit().map_err(sqlite_error::statement)?;
                Ok(execution)
            },
        )
        .await
    }
}

fn equality_id(query: &BsonDocument) -> Option<&BsonValue> {
    let mut value = query.get_first("_id")?;
    if let BsonValue::Document(document) = value {
        if document.iter().any(|(name, _)| name.starts_with('$')) {
            if document.len() != 1 {
                return None;
            }
            value = document.get_first("$eq")?;
        }
    }
    (!matches!(value, BsonValue::RegularExpression(_))).then_some(value)
}

fn synthesize_replacement(
    query: &BsonDocument,
    replacement: &BsonDocument,
    max_document_bytes: usize,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<BsonDocument> {
    check()?;
    let query_id = equality_id(query);
    let replacement_id = replacement.get_first("_id");
    if let (Some(query), Some(replacement)) = (query_id, replacement_id) {
        if query != replacement {
            return Err(DocumentMutationError::ImmutableId.into_engine_error());
        }
    }
    let id = replacement_id.or(query_id).cloned().unwrap_or_else(|| {
        BsonValue::ObjectId(BsonObjectId::from_bytes(bson::oid::ObjectId::new().bytes()))
    });
    CanonicalBsonKey::encode(&id)
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    // A wire upsert ID is nested inside both the upserted array and its entry.
    // Reserve those two extra containers before any insert can commit; the
    // ordinary result-byte accounting alone cannot prove reply depth safety.
    let returned_id = BsonDocument::from_entries([("_id", id.clone())])
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    encode_document_with_options(
        &returned_id,
        &BsonCodecOptions::new().with_max_nesting_depth(BSON_MAX_NESTING_DEPTH - 2),
    )
    .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    check()?;
    let mut document = BsonDocument::new();
    document
        .push("_id", id)
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    for (name, value) in replacement.iter().filter(|(name, _)| *name != "_id") {
        check()?;
        let value = match value {
            BsonValue::Timestamp(value) if value.time() == 0 && value.increment() == 0 => {
                BsonValue::Timestamp(next_server_timestamp()?)
            }
            value => value.clone(),
        };
        document
            .push(name, value)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    }
    check()?;
    encode_document_with_options(
        &document,
        &BsonCodecOptions::new().with_max_document_bytes(max_document_bytes),
    )
    .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    Ok(document)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::BsonRegex;

    fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
        BsonDocument::from_entries(fields).unwrap()
    }

    #[test]
    fn replacement_upsert_infers_only_literal_or_sole_equality_ids() {
        let literal = BsonValue::Document(doc([("key", BsonValue::Int64(3))]));
        for id in [BsonValue::Null, BsonValue::Int64(7), literal] {
            for query_id in [id.clone(), BsonValue::Document(doc([("$eq", id.clone())]))] {
                let query = doc([("_id", query_id), ("unrelated", BsonValue::Boolean(true))]);
                let result =
                    synthesize_replacement(&query, &BsonDocument::new(), 1024, &mut || Ok(()))
                        .unwrap();
                assert!(result.get_first("_id").unwrap().representation_eq(&id));
            }
        }
        let regex = BsonValue::RegularExpression(BsonRegex::new("pattern", "").unwrap());
        for query in [
            BsonDocument::new(),
            doc([("_id", regex.clone())]),
            doc([("_id", BsonValue::Document(doc([("$eq", regex)])))]),
            doc([(
                "_id",
                BsonValue::Document(doc([("$gt", BsonValue::Int32(7))])),
            )]),
            doc([(
                "_id",
                BsonValue::Document(doc([
                    ("$eq", BsonValue::Int32(7)),
                    ("$lt", BsonValue::Int32(9)),
                ])),
            )]),
        ] {
            let a =
                synthesize_replacement(&query, &BsonDocument::new(), 1024, &mut || Ok(())).unwrap();
            let b =
                synthesize_replacement(&query, &BsonDocument::new(), 1024, &mut || Ok(())).unwrap();
            assert!(matches!(a.get_first("_id"), Some(BsonValue::ObjectId(_))));
            assert_ne!(a.get_first("_id"), b.get_first("_id"));
        }
        let query = doc([("_id", BsonValue::Int64(7))]);
        let replacement = doc([
            ("value", BsonValue::Boolean(true)),
            ("_id", BsonValue::Double(7.0)),
        ]);
        let result = synthesize_replacement(&query, &replacement, 1024, &mut || Ok(())).unwrap();
        assert!(result.representation_eq(&doc([
            ("_id", BsonValue::Double(7.0)),
            ("value", BsonValue::Boolean(true))
        ])));
        assert!(
            synthesize_replacement(
                &query,
                &doc([("_id", BsonValue::Boolean(true))]),
                1024,
                &mut || Ok(())
            )
            .is_err()
        );
    }

    #[test]
    fn replacement_upsert_preflights_return_depth_size_and_cancellation() {
        let mut id = BsonValue::Int32(1);
        for _ in 0..BSON_MAX_NESTING_DEPTH - 3 {
            id = BsonValue::Document(doc([("nested", id)]));
        }
        let replacement = doc([("_id", id.clone())]);
        assert!(
            synthesize_replacement(&BsonDocument::new(), &replacement, 4096, &mut || Ok(()))
                .is_ok()
        );
        let replacement = doc([("_id", BsonValue::Document(doc([("nested", id)])))]);
        assert!(encode_document(&replacement).is_ok());
        assert!(
            synthesize_replacement(&BsonDocument::new(), &replacement, 4096, &mut || Ok(()))
                .is_err()
        );
        assert!(
            synthesize_replacement(
                &BsonDocument::new(),
                &BsonDocument::new(),
                8,
                &mut || Ok(())
            )
            .is_err()
        );
        assert_eq!(
            synthesize_replacement(
                &BsonDocument::new(),
                &BsonDocument::new(),
                1024,
                &mut || Err(EngineError::new(EngineErrorKind::Cancelled, "cancelled"))
            )
            .unwrap_err()
            .kind(),
            EngineErrorKind::Cancelled
        );
    }
}
