#![cfg(feature = "documents")]

use briskdb::{
    core::{Engine, RequestContext},
    document::{
        BsonDocument, BsonValue, DocumentCollectionOptions, DocumentCommand,
        DocumentCreateCollectionRequest, DocumentDeleteRequest, DocumentFilter,
        DocumentFindOneAndUpdateRequest, DocumentFindRequest, DocumentMutationError,
        DocumentMutationScope, DocumentNamespace, DocumentQueryError, DocumentReadOptions,
        DocumentRequest, DocumentRequestId, DocumentResult, DocumentUpdate, DocumentUpdateError,
        DocumentUpdateRequest, DocumentWriteOptions, decode_document, encode_document,
    },
};
use std::{error::Error, process::Command};

fn request(command: DocumentCommand) -> DocumentRequest {
    DocumentRequest::new(
        DocumentRequestId::new([1; 16]).unwrap(),
        RequestContext::new(),
        command,
    )
}

#[tokio::test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
async fn operator_upserts_match_locked_oracle_in_both_scopes() {
    let python = std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/document_upsert_oracle.py"
        ))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.len() < 8 * 1024 * 1024);
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), 4).await.unwrap();
    let session = engine.session();
    let namespace = DocumentNamespace::new("oracle", "upserts").unwrap();
    engine
        .execute_document(
            &session,
            request(DocumentCommand::CreateCollection(
                DocumentCreateCollectionRequest::new(
                    namespace.clone(),
                    DocumentCollectionOptions::empty(),
                    DocumentWriteOptions::new(),
                ),
            )),
        )
        .await
        .unwrap();
    let mut bytes = output.stdout.as_slice();
    let mut count = 0;
    while !bytes.is_empty() {
        let length = i32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let case = decode_document(&bytes[..length]).unwrap();
        bytes = &bytes[length..];
        let Some(BsonValue::Document(query)) = case.get_first("query") else {
            panic!("query")
        };
        let Some(BsonValue::Document(update)) = case.get_first("update") else {
            panic!("update")
        };
        for mode in 0..4 {
            let scope = if mode == 1 {
                DocumentMutationScope::Many
            } else {
                DocumentMutationScope::One
            };
            let update = DocumentUpdateRequest::new(
                namespace.clone(),
                DocumentFilter::new(query.clone()).unwrap(),
                DocumentUpdate::new(update.clone()).unwrap(),
                scope,
                DocumentWriteOptions::new().with_upsert(true),
            );
            let command = if mode < 2 {
                DocumentCommand::Update(update)
            } else {
                DocumentCommand::FindOneAndUpdate(
                    DocumentFindOneAndUpdateRequest::new(update, DocumentReadOptions::new())
                        .with_return_after(mode == 3),
                )
            };
            let result = engine.execute_document(&session, request(command)).await;
            if let Some(BsonValue::Int32(expected)) = case.get_first("error") {
                let error = result.unwrap_err();
                let mut source = error.source();
                let mut code = None;
                while let Some(cause) = source {
                    code = cause
                        .downcast_ref::<DocumentUpdateError>()
                        .map(|e| e.mongo_code())
                        .or_else(|| {
                            cause
                                .downcast_ref::<DocumentMutationError>()
                                .map(|e| e.mongo_code())
                        })
                        .or_else(|| {
                            cause
                                .downcast_ref::<DocumentQueryError>()
                                .map(|e| e.mongo_code())
                        });
                    if code.is_some() {
                        break;
                    }
                    source = cause.source();
                }
                assert_eq!(code, Some(*expected), "case {count}: {error}");
            } else {
                let execution = result.unwrap_or_else(|error| panic!("case {count}: {error}"));
                match execution.result() {
                    DocumentResult::Update(result) if mode < 2 => {
                        assert_eq!(
                            (
                                result.matched_count(),
                                result.modified_count(),
                                result.did_upsert()
                            ),
                            (0, 0, true)
                        );
                        assert!(
                            result
                                .upserted_id()
                                .unwrap()
                                .representation_eq(&BsonValue::Int64(7))
                        );
                    }
                    DocumentResult::UpsertedDocument(result) if mode >= 2 => {
                        assert!(result.upserted_id().representation_eq(&BsonValue::Int64(7)));
                        if mode == 2 {
                            assert!(result.document().is_none());
                        } else {
                            let Some(BsonValue::Document(expected)) = case.get_first("result")
                            else {
                                panic!("expected image")
                            };
                            assert_eq!(
                                encode_document(result.document().unwrap()).unwrap(),
                                encode_document(expected).unwrap(),
                                "image case {count}"
                            );
                        }
                    }
                    _ => panic!("unexpected result"),
                }
            }
            let execution = engine
                .execute_document(
                    &session,
                    request(DocumentCommand::Find(DocumentFindRequest::new(
                        namespace.clone(),
                        DocumentFilter::empty(),
                        DocumentReadOptions::new(),
                    ))),
                )
                .await
                .unwrap();
            let DocumentResult::Cursor(batch) = execution.result() else {
                panic!("cursor")
            };
            assert!(batch.is_exhausted());
            if let Some(BsonValue::Document(expected)) = case.get_first("result") {
                assert_eq!(batch.documents().len(), 1);
                assert_eq!(
                    encode_document(&batch.documents()[0]).unwrap(),
                    encode_document(expected).unwrap(),
                    "case {count}"
                );
            } else {
                assert!(
                    batch.documents().is_empty(),
                    "failed upsert wrote a document: {count}"
                );
            }
            engine
                .execute_document(
                    &session,
                    request(DocumentCommand::Delete(DocumentDeleteRequest::new(
                        namespace.clone(),
                        DocumentFilter::new(
                            BsonDocument::from_entries([("_id", BsonValue::Int64(7))]).unwrap(),
                        )
                        .unwrap(),
                        scope,
                        DocumentWriteOptions::new(),
                    ))),
                )
                .await
                .unwrap();
            count += 1;
        }
    }
    assert_eq!(count, 3544);
    drop(session);
    drop(engine);
    println!(
        "{count} source-locked operator-upsert executions passed, including exact stored BSON, insert metadata and atomic failures in both scopes and before/after find-and-modify images"
    );
}
