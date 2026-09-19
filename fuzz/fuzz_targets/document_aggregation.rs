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
        let streamed = (|| -> briskdb::core::EngineResult<Vec<BsonDocument>> {
            let mut stream = runner.into_stream();
            let mut result = Vec::new();
            for document in source.iter().cloned() {
                if stream.is_input_exhausted() {
                    break;
                }
                if let Some(document) = stream.push(document)? {
                    result.push(document);
                }
            }
            result.extend(stream.finish()?);
            Ok(result)
        })();
        match (first, second) {
            (Ok(left), Ok(right)) => {
                let encode = |rows: &[BsonDocument]| {
                    rows.iter()
                        .map(|row| encode_document(row).unwrap())
                        .collect::<Vec<_>>()
                };
                assert_eq!(encode(&left), encode(&right));
                // Streaming and materialization can hit different resource
                // bounds, but successful outputs must have identical BSON.
                if let Ok(streamed) = streamed {
                    assert_eq!(encode(&left), encode(&streamed));
                }
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
            &[envelope.clone(), envelope.clone()],
            DocumentPipeline::new(stages).unwrap(),
        );
        for name in ["$project", "$set", "$addFields"] {
            let spec = BsonDocument::from_entries([
                ("copy", BsonValue::from("$v")),
                ("array.copy", BsonValue::from("$payload")),
                ("shell.gone", BsonValue::from("$$REMOVE")),
                (
                    "literal",
                    BsonValue::Document(
                        BsonDocument::from_entries([(
                            "$literal",
                            BsonValue::Document(envelope.clone()),
                        )])
                        .unwrap(),
                    ),
                ),
            ])
            .unwrap();
            if let Ok(pipeline) = DocumentPipeline::new(vec![
                BsonDocument::from_entries([(name, BsonValue::Document(spec))]).unwrap(),
                BsonDocument::from_entries([("$limit", BsonValue::Int32(1))]).unwrap(),
            ]) {
                exercise(std::slice::from_ref(&envelope), pipeline);
            }
        }
        let mut group = BsonDocument::from_entries([("_id", BsonValue::from("$v"))]).unwrap();
        for operator in [
            "$addToSet",
            "$avg",
            "$first",
            "$last",
            "$max",
            "$min",
            "$push",
            "$sum",
        ] {
            group
                .push(
                    &operator[1..],
                    BsonValue::Document(
                        BsonDocument::from_entries([(operator, BsonValue::from("$v"))]).unwrap(),
                    ),
                )
                .unwrap();
        }
        for key in [
            BsonValue::from("$v"),
            BsonValue::Int32(1),
            BsonValue::Document(
                BsonDocument::from_entries([("value", BsonValue::from("$v"))]).unwrap(),
            ),
            BsonValue::Array(vec![BsonValue::from("$v"), BsonValue::from("$missing")]),
        ] {
            let mut spec = BsonDocument::from_entries([("_id", key)]).unwrap();
            for (name, value) in group.iter().skip(1) {
                spec.push(name, value.clone()).unwrap();
            }
            exercise(
                &[envelope.clone(), envelope.clone()],
                DocumentPipeline::new(vec![
                    BsonDocument::from_entries([("$group", BsonValue::Document(spec))]).unwrap(),
                ])
                .unwrap(),
            );
        }
    }
});
