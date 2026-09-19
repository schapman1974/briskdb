#![no_main]

use briskdb::document::{
    BsonDocument, BsonValue, DocumentAggregator, DocumentPipeline, decode_document, encode_document,
};
use libfuzzer_sys::fuzz_target;

fn exercise(source: &[BsonDocument], pipeline: DocumentPipeline) {
    if let Ok(runner) = DocumentAggregator::compile(&pipeline) {
        let before: Vec<_> = source
            .iter()
            .map(|row| encode_document(row).unwrap())
            .collect();
        let first = runner.execute(source);
        let second = runner.execute_with_check(source, &mut || Ok(()));
        match (first, second) {
            (Ok(left), Ok(right)) => {
                let encode = |rows: Vec<BsonDocument>| {
                    rows.iter()
                        .map(|row| encode_document(row).unwrap())
                        .collect::<Vec<_>>()
                };
                assert_eq!(encode(left), encode(right));
            }
            (Err(left), Err(right)) => assert_eq!(left.kind(), right.kind()),
            _ => panic!("execution must be deterministic"),
        }
        assert_eq!(
            before,
            source
                .iter()
                .map(|row| encode_document(row).unwrap())
                .collect::<Vec<_>>()
        );
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > 1024 * 1024 {
        return;
    }
    if let Ok(envelope) = decode_document(data) {
        if let (Some(BsonValue::Array(stages)), Some(BsonValue::Array(rows))) = (
            envelope.get_first("pipeline"),
            envelope.get_first("documents"),
        ) {
            let documents = |values: &[BsonValue]| {
                values
                    .iter()
                    .map(|value| {
                        if let BsonValue::Document(document) = value {
                            Some(document.clone())
                        } else {
                            None
                        }
                    })
                    .collect::<Option<Vec<_>>>()
            };
            if stages.len() <= 64 && rows.len() <= 256 {
                if let (Some(stages), Some(rows)) = (documents(stages), documents(rows)) {
                    if let Ok(pipeline) = DocumentPipeline::new(stages) {
                        exercise(&rows, pipeline);
                    }
                }
            }
        }
        // Every valid document also exercises an executable pipeline, even
        // when the fuzzer has not discovered the structured envelope grammar.
        let sort = BsonDocument::from_entries([("v", BsonValue::Int32(1))]).unwrap();
        let stages = [
            ("$sort", BsonValue::Document(sort)),
            (
                "$skip",
                BsonValue::Int32(i32::from(data.first().copied().unwrap_or(0) % 3)),
            ),
            ("$limit", BsonValue::Int32(2)),
            ("$count", BsonValue::String("n".into())),
        ]
        .into_iter()
        .map(|(name, value)| BsonDocument::from_entries([(name, value)]).unwrap())
        .collect::<Vec<_>>();
        exercise(
            &[envelope.clone(), envelope],
            DocumentPipeline::new(stages).unwrap(),
        );
    }
});
