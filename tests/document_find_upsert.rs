#![cfg(feature = "documents")]

use briskdb::{
    core::{CancellationToken, Engine, EngineErrorKind, RequestContext, ResultLimits, Session},
    document::{
        BsonDocument, BsonTimestamp, BsonValue, DocumentCollectionOptions, DocumentCommand,
        DocumentCreateCollectionRequest, DocumentFilter, DocumentFindOneAndReplaceRequest,
        DocumentFindOneAndUpdateRequest, DocumentFindRequest, DocumentMutationScope,
        DocumentNamespace, DocumentProjection, DocumentReadOptions, DocumentReplaceRequest,
        DocumentRequest, DocumentRequestId, DocumentResult, DocumentSort, DocumentUpdate,
        DocumentUpdateRequest, DocumentWriteOptions, encode_document,
    },
};

fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}
fn ns() -> DocumentNamespace {
    DocumentNamespace::new("app", "items").unwrap()
}
fn request(command: DocumentCommand, context: RequestContext) -> DocumentRequest {
    DocumentRequest::new(DocumentRequestId::new([1; 16]).unwrap(), context, command)
}
fn change(
    filter: BsonDocument,
    body: BsonDocument,
    replacement: bool,
    after: bool,
    options: DocumentReadOptions,
    max_bytes: usize,
) -> DocumentCommand {
    let filter = DocumentFilter::new(filter).unwrap();
    let write = DocumentWriteOptions::new().with_upsert(true);
    if replacement {
        DocumentCommand::FindOneAndReplace(
            DocumentFindOneAndReplaceRequest::new(
                DocumentReplaceRequest::new(ns(), filter, body, write)
                    .unwrap()
                    .with_max_document_bytes(max_bytes)
                    .unwrap(),
                options,
            )
            .with_return_after(after),
        )
    } else {
        DocumentCommand::FindOneAndUpdate(
            DocumentFindOneAndUpdateRequest::new(
                DocumentUpdateRequest::new(
                    ns(),
                    filter,
                    DocumentUpdate::new(body).unwrap(),
                    DocumentMutationScope::One,
                    write,
                )
                .with_max_document_bytes(max_bytes)
                .unwrap(),
                options,
            )
            .with_return_after(after),
        )
    }
}
async fn create(engine: &Engine, session: &Session) {
    engine
        .execute_document(
            session,
            request(
                DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                    ns(),
                    DocumentCollectionOptions::empty(),
                    DocumentWriteOptions::new(),
                )),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
}
async fn rows(engine: &Engine, session: &Session) -> Vec<BsonDocument> {
    let result = engine
        .execute_document(
            session,
            request(
                DocumentCommand::Find(DocumentFindRequest::new(
                    ns(),
                    DocumentFilter::empty(),
                    DocumentReadOptions::new(),
                )),
                RequestContext::new(),
            ),
        )
        .await
        .unwrap();
    let DocumentResult::Cursor(batch) = result.into_parts().2 else {
        panic!("cursor")
    };
    assert!(batch.is_exhausted());
    batch.into_parts().2
}

#[tokio::test]
async fn find_upserts_preserve_identity_projected_images_timestamps_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    create(&engine, &session).await;
    let mut number = 0;
    for replacement in [false, true] {
        for after in [false, true] {
            let id = if number == 0 {
                BsonValue::Null
            } else {
                BsonValue::Int64(number)
            };
            number += 1;
            let zero = BsonValue::Timestamp(BsonTimestamp::new(0, 0));
            let body = if replacement {
                doc([
                    ("counter", BsonValue::Int32(5)),
                    ("stamp", zero.clone()),
                    ("hidden", BsonValue::Boolean(true)),
                ])
            } else {
                doc([
                    (
                        "$inc",
                        BsonValue::Document(doc([("counter", BsonValue::Int32(2))])),
                    ),
                    (
                        "$set",
                        BsonValue::Document(doc([
                            ("stamp", zero.clone()),
                            ("hidden", BsonValue::Boolean(true)),
                        ])),
                    ),
                ])
            };
            let options = DocumentReadOptions::new()
                .with_projection(
                    DocumentProjection::new(doc([
                        ("counter", BsonValue::Int32(1)),
                        ("_id", BsonValue::Int32(0)),
                    ]))
                    .unwrap(),
                )
                .with_sort(DocumentSort::new(doc([("counter", BsonValue::Int32(1))])).unwrap());
            let execution = engine
                .execute_document(
                    &session,
                    request(
                        change(
                            doc([("_id", id.clone()), ("counter", BsonValue::Int32(3))]),
                            body.clone(),
                            replacement,
                            after,
                            options.clone(),
                            16 * 1024 * 1024,
                        ),
                        RequestContext::new(),
                    ),
                )
                .await
                .unwrap();
            let DocumentResult::UpsertedDocument(result) = execution.result() else {
                panic!("upsert")
            };
            assert!(result.upserted_id().representation_eq(&id));
            assert_eq!(
                result.document(),
                after
                    .then(|| doc([("counter", BsonValue::Int32(5))]))
                    .as_ref()
            );
            let stored = rows(&engine, &session)
                .await
                .into_iter()
                .find(|row| row.get_first("_id") == Some(&id))
                .unwrap();
            assert_eq!(stored.iter().next().unwrap().0, "_id");
            assert_eq!(stored.get_first("hidden"), Some(&BsonValue::Boolean(true)));
            assert_eq!(stored.get_first("stamp") == Some(&zero), !replacement);
            let execution = engine
                .execute_document(
                    &session,
                    request(
                        change(
                            doc([("_id", id)]),
                            body,
                            replacement,
                            after,
                            options,
                            16 * 1024 * 1024,
                        ),
                        RequestContext::new(),
                    ),
                )
                .await
                .unwrap();
            let DocumentResult::Document(Some(image)) = execution.result() else {
                panic!("matched image")
            };
            assert_eq!(
                image,
                &doc([(
                    "counter",
                    BsonValue::Int32(if after && !replacement { 7 } else { 5 })
                )])
            );
        }
        let body = if replacement {
            doc([("value", BsonValue::Int32(1))])
        } else {
            doc([(
                "$set",
                BsonValue::Document(doc([("value", BsonValue::Int32(1))])),
            )])
        };
        let execution = engine
            .execute_document(
                &session,
                request(
                    change(
                        doc([("missing", BsonValue::Boolean(replacement))]),
                        body,
                        replacement,
                        false,
                        DocumentReadOptions::new(),
                        1024,
                    ),
                    RequestContext::new(),
                ),
            )
            .await
            .unwrap();
        let DocumentResult::UpsertedDocument(result) = execution.result() else {
            panic!("generated upsert")
        };
        assert!(matches!(result.upserted_id(), BsonValue::ObjectId(_)));
        assert!(result.document().is_none());
    }
    let before: Vec<_> = rows(&engine, &session)
        .await
        .iter()
        .map(|row| encode_document(row).unwrap())
        .collect();
    drop(session);
    drop(engine);
    let engine = Engine::open(root.path(), 4).await.unwrap();
    assert_eq!(
        rows(&engine, &engine.session())
            .await
            .iter()
            .map(|row| encode_document(row).unwrap())
            .collect::<Vec<_>>(),
        before
    );
}

#[tokio::test]
async fn find_upserts_preflight_identity_metadata_even_without_a_returned_image() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    create(&engine, &session).await;
    for replacement in [false, true] {
        for after in [false, true] {
            let fields = doc([
                ("_id", BsonValue::from("x".repeat(4000))),
                ("value", BsonValue::Int32(1)),
            ]);
            let body = if replacement {
                fields
            } else {
                doc([("$set", BsonValue::Document(fields))])
            };
            let options = DocumentReadOptions::new().with_projection(
                DocumentProjection::new(doc([
                    ("value", BsonValue::Int32(1)),
                    ("_id", BsonValue::Int32(0)),
                ]))
                .unwrap(),
            );
            // The image is absent or tiny, but the inserted ID is still returned.
            let command = change(
                doc([("missing", BsonValue::Boolean(true))]),
                body.clone(),
                replacement,
                after,
                options.clone(),
                8192,
            );
            let error = engine
                .execute_document(
                    &session,
                    request(
                        command.clone(),
                        RequestContext::new()
                            .with_result_limits(ResultLimits::new(100, 1000).unwrap()),
                    ),
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
            let token = CancellationToken::new();
            token.cancel();
            let error = engine
                .execute_document(
                    &session,
                    request(
                        command,
                        RequestContext::new().with_cancellation_token(token),
                    ),
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Cancelled);
            let error = engine
                .execute_document(
                    &session,
                    request(
                        change(
                            doc([("missing", BsonValue::Boolean(true))]),
                            body,
                            replacement,
                            after,
                            options,
                            64,
                        ),
                        RequestContext::new(),
                    ),
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
            assert!(rows(&engine, &session).await.is_empty());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn find_upserts_concurrent_images_are_atomic_and_sorted_empty_images_are_matches() {
    let root = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(Engine::open(root.path(), 4).await.unwrap());
    let session = engine.session();
    create(&engine, &session).await;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(4));
    let mut workers = Vec::new();
    for _ in 0..4 {
        let engine = engine.clone();
        let barrier = std::sync::Arc::clone(&barrier);
        workers.push(tokio::spawn(async move {
            let session = engine.session();
            barrier.wait().await;
            let mut images = Vec::new();
            let mut inserted = 0;
            for _ in 0..4 {
                let execution = engine
                    .execute_document(
                        &session,
                        request(
                            change(
                                doc([("_id", BsonValue::Int64(1))]),
                                doc([(
                                    "$inc",
                                    BsonValue::Document(doc([("counter", BsonValue::Int32(1))])),
                                )]),
                                false,
                                true,
                                DocumentReadOptions::new(),
                                1024,
                            ),
                            RequestContext::new(),
                        ),
                    )
                    .await
                    .unwrap();
                let image = match execution.result() {
                    DocumentResult::UpsertedDocument(result) => {
                        inserted += 1;
                        assert!(result.upserted_id().representation_eq(&BsonValue::Int64(1)));
                        result.document().unwrap()
                    }
                    DocumentResult::Document(Some(image)) => image,
                    _ => panic!("image"),
                };
                let Some(BsonValue::Int32(counter)) = image.get_first("counter") else {
                    panic!("counter")
                };
                images.push(*counter);
            }
            (inserted, images)
        }));
    }
    let mut inserts = 0;
    let mut images = Vec::new();
    for worker in workers {
        let (count, values) = worker.await.unwrap();
        inserts += count;
        images.extend(values);
    }
    images.sort_unstable();
    assert_eq!(inserts, 1);
    assert_eq!(images, (1..=16).collect::<Vec<_>>());
    assert_eq!(rows(&engine, &session).await.len(), 1);
    for id in 2..=4 {
        engine
            .execute_document(
                &session,
                request(
                    change(
                        doc([("_id", BsonValue::Int64(id))]),
                        doc([("counter", BsonValue::Int32(id as i32))]),
                        true,
                        false,
                        DocumentReadOptions::new(),
                        1024,
                    ),
                    RequestContext::new(),
                ),
            )
            .await
            .unwrap();
    }
    let options = DocumentReadOptions::new()
        .with_sort(DocumentSort::new(doc([("counter", BsonValue::Int32(-1))])).unwrap())
        .with_projection(
            DocumentProjection::new(doc([
                ("absent", BsonValue::Int32(1)),
                ("_id", BsonValue::Int32(0)),
            ]))
            .unwrap(),
        );
    for replacement in [false, true] {
        let body = if replacement {
            doc([("counter", BsonValue::Int32(20))])
        } else {
            doc([(
                "$set",
                BsonValue::Document(doc([("counter", BsonValue::Int32(20))])),
            )])
        };
        let execution = engine
            .execute_document(
                &session,
                request(
                    change(
                        BsonDocument::new(),
                        body,
                        replacement,
                        true,
                        options.clone(),
                        1024,
                    ),
                    RequestContext::new(),
                ),
            )
            .await
            .unwrap();
        assert!(
            matches!(execution.result(), DocumentResult::Document(Some(image)) if image.is_empty())
        );
        let stored = rows(&engine, &session).await;
        assert_eq!(stored.len(), 4);
        assert_eq!(
            stored
                .iter()
                .find(|row| row.get_first("_id") == Some(&BsonValue::Int64(1)))
                .unwrap()
                .get_first("counter"),
            Some(&BsonValue::Int32(20))
        );
    }
}
