#![cfg(feature = "documents")]

use std::process::Command;

use briskdb::{
    core::{Engine, RequestContext},
    document::{
        BsonValue, DocumentCollectionOptions, DocumentCommand, DocumentCreateCollectionRequest,
        DocumentCreateIndexRequest, DocumentIndexLifecycle, DocumentIndexRequest,
        DocumentListIndexesRequest, DocumentNamespace, DocumentReadOptions, DocumentRequest,
        DocumentRequestId, DocumentResult, DocumentWriteOptions, decode_document, encode_document,
    },
};

fn request(command: DocumentCommand) -> DocumentRequest {
    DocumentRequest::new(
        DocumentRequestId::new([1; 16]).unwrap(),
        RequestContext::new(),
        command,
    )
}

#[tokio::test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
async fn index_definitions_match_locked_names_and_keys_without_claiming_physical_indexes() {
    let python = std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/document_index_definition_oracle.py"
        ))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.len() < 1024 * 1024);
    let root = tempfile::tempdir().unwrap();
    let namespace = DocumentNamespace::new("oracle", "definitions").unwrap();
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let session = engine.session();
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
    let mut cases = Vec::new();
    while !bytes.is_empty() {
        let length = i32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let case = decode_document(&bytes[..length]).unwrap();
        bytes = &bytes[length..];
        let Some(BsonValue::Document(keys)) = case.get_first("keys") else {
            panic!("keys")
        };
        let Some(BsonValue::Boolean(unique)) = case.get_first("unique") else {
            panic!("unique")
        };
        let mut index = DocumentIndexRequest::new(keys.clone())
            .unwrap()
            .with_unique(*unique);
        if let Some(BsonValue::String(name)) = case.get_first("name") {
            index = index.with_name(name).unwrap();
        }
        let execution = engine
            .execute_document(
                &session,
                request(DocumentCommand::CreateIndex(
                    DocumentCreateIndexRequest::new(
                        namespace.clone(),
                        index,
                        DocumentWriteOptions::new(),
                    ),
                )),
            )
            .await
            .unwrap();
        let DocumentResult::IndexName(name) = execution.result() else {
            panic!("name")
        };
        assert_eq!(
            case.get_first("expected_name"),
            Some(&BsonValue::from(name.as_str()))
        );
        cases.push(case);
    }
    assert_eq!(cases.len(), 64);
    drop(session);
    drop(engine);
    let engine = Engine::open(root.path(), 2).await.unwrap();
    let execution = engine
        .execute_document(
            &engine.session(),
            request(DocumentCommand::ListIndexes(
                DocumentListIndexesRequest::new(namespace, DocumentReadOptions::new()),
            )),
        )
        .await
        .unwrap();
    let DocumentResult::Indexes(indexes) = execution.result() else {
        panic!("indexes")
    };
    assert_eq!(indexes.len(), cases.len() + 1);
    for case in cases {
        let Some(BsonValue::String(name)) = case.get_first("expected_name") else {
            panic!("name")
        };
        let Some(BsonValue::Document(keys)) = case.get_first("expected_keys") else {
            panic!("keys")
        };
        let index = indexes.iter().find(|index| index.name() == name).unwrap();
        assert_eq!(
            encode_document(index.specification()).unwrap(),
            encode_document(keys).unwrap()
        );
        assert_eq!(index.lifecycle(), DocumentIndexLifecycle::PendingBuild);
        assert_eq!(
            case.get_first("unique"),
            Some(&BsonValue::Boolean(index.is_unique()))
        );
    }
    println!(
        "64 source-locked index definitions passed, including exact names, key order, flags and pending metadata after restart"
    );
}
