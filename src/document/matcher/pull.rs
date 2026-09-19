//! Pull conditions reuse query predicates without the collection-level _id rule.

use super::*;
use crate::document::memory;

pub(in crate::document) struct PullMatcher {
    condition: Condition,
}

enum Condition {
    Literal(BsonValue),
    Fields(Vec<Predicate>),
    Document(DocumentMatcher),
}

fn logical(name: &str) -> bool {
    matches!(name, "$and" | "$or" | "$nor")
}

fn field_allowed(name: &str, document_field: bool) -> bool {
    matches!(
        name,
        "$all"
            | "$elemMatch"
            | "$eq"
            | "$exists"
            | "$in"
            | "$mod"
            | "$ne"
            | "$nin"
            | "$options"
            | "$regex"
            | "$size"
            | "$type"
            | "$gt"
            | "$gte"
            | "$lt"
            | "$lte"
    ) || document_field && name == "$not"
}

fn validate_field(
    value: &BsonValue,
    document_field: bool,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    check()?;
    let BsonValue::Document(document) = value else {
        return Ok(());
    };
    if !document.iter().any(|(name, _)| name.starts_with('$')) {
        return Ok(());
    }
    for (name, _) in document.iter() {
        check()?;
        if !field_allowed(name, document_field) {
            return Err(query_error(2));
        }
    }
    Ok(())
}

fn validate_document(
    document: &BsonDocument,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    for (name, value) in document.iter() {
        check()?;
        if logical(name) {
            let BsonValue::Array(clauses) = value else {
                return Err(query_error(2));
            };
            if clauses.is_empty() {
                return Err(query_error(2));
            }
            for clause in clauses {
                let BsonValue::Document(clause) = clause else {
                    return Err(query_error(2));
                };
                validate_document(clause, check)?;
            }
        } else if name == "$expr" {
            return Err(query_error(224));
        } else if name.starts_with('$') {
            return Err(query_error(2));
        } else {
            validate_field(value, true, check)?;
        }
    }
    Ok(())
}

// Preflight conservative AST and regex-program retention before compilation.
// Literal regexes/field names may overcount, but never undercount. Dotted
// selectors own a Vec<String>, not just the bytes in their BSON field name.
fn compilation_extras(
    value: &BsonValue,
    maximum: usize,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<usize> {
    check()?;
    let mut bytes = 0usize;
    let add = |bytes: &mut usize, amount: usize| -> EngineResult<()> {
        *bytes = bytes
            .checked_add(amount)
            .filter(|bytes| *bytes <= maximum)
            .ok_or_else(limit)?;
        Ok(())
    };
    match value {
        BsonValue::RegularExpression(_) => add(&mut bytes, 1024 * 1024)?,
        BsonValue::Array(values) => {
            for value in values {
                add(&mut bytes, compilation_extras(value, maximum, check)?)?;
            }
        }
        BsonValue::Document(document) => {
            for (name, value) in document.iter() {
                add(
                    &mut bytes,
                    name.split('.').count().checked_mul(128).ok_or_else(limit)?,
                )?;
                let extra = if name == "$regex" {
                    1024 * 1024
                } else {
                    compilation_extras(value, maximum, check)?
                };
                add(&mut bytes, extra)?;
            }
        }
        _ => {}
    }
    Ok(bytes)
}

fn update_error(error: EngineError) -> EngineError {
    let Some(query) = error
        .source()
        .and_then(|source| source.downcast_ref::<DocumentQueryError>())
    else {
        return error;
    };
    let code = if query.code == 115 { 2 } else { query.code };
    EngineError::from_source(
        EngineErrorKind::InvalidArgument,
        "invalid array update condition",
        DocumentQueryError { code },
    )
}

impl PullMatcher {
    /// The containing update has already passed the BSON size/depth bounds.
    pub(in crate::document) fn compile(
        value: &BsonValue,
        retained: &mut usize,
        maximum: usize,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Self> {
        Self::compile_inner(value, retained, maximum, check).map_err(update_error)
    }

    fn compile_inner(
        value: &BsonValue,
        retained: &mut usize,
        maximum: usize,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Self> {
        check()?;
        let BsonValue::Document(document) = value else {
            return Ok(Self {
                condition: Condition::Literal(value.clone()),
            });
        };
        if document.iter().any(|(name, _)| name == "$expr") {
            return Err(query_error(224));
        }
        let is_document = document.iter().any(|(name, _)| logical(name))
            || !document.iter().any(|(name, _)| name.starts_with('$'));
        if is_document {
            validate_document(document, check)?;
        } else {
            validate_field(value, false, check)?;
        }
        let bytes = memory::value_bytes(value, maximum.saturating_sub(*retained), check)?;
        let bytes = bytes.checked_mul(4).ok_or_else(limit)?;
        let extras = compilation_extras(value, maximum.saturating_sub(*retained), check)?;
        *retained = retained
            .checked_add(bytes)
            .and_then(|bytes| bytes.checked_add(extras))
            .filter(|bytes| *bytes <= maximum)
            .ok_or_else(limit)?;
        let mut compiler = Compiler {
            nodes: 0,
            regexes: 0,
            check,
        };
        let condition = if is_document {
            Condition::Document(compiler.document(document, 0, false)?)
        } else {
            Condition::Fields(compiler.predicates(document, 0, false)?)
        };
        Ok(Self { condition })
    }

    pub(in crate::document) fn matches(
        &self,
        value: &BsonValue,
        control: &mut dyn MatchControl,
    ) -> EngineResult<bool> {
        let mut work = Work {
            steps: 0,
            check: control,
        };
        work.step()?;
        match &self.condition {
            Condition::Literal(expected) => equal(Some(value), expected, true, &mut work),
            Condition::Fields(predicates) => {
                for predicate in predicates {
                    if !predicate.evaluate(Some(value), false, &mut work)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Condition::Document(matcher) => match value {
                BsonValue::Document(document) => {
                    matcher.evaluate(Root::Document(document), false, false, &mut work)
                }
                _ => Ok(false),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{DocumentMutationError, DocumentUpdateError, DocumentUpdater};

    fn doc<const N: usize>(values: [(&str, BsonValue); N]) -> BsonDocument {
        BsonDocument::from_entries(values).unwrap()
    }
    fn array(values: &[i32]) -> BsonValue {
        BsonValue::Array(values.iter().copied().map(BsonValue::Int32).collect())
    }
    fn specification(path: &str, condition: BsonValue) -> BsonDocument {
        doc([("$pull", BsonValue::Document(doc([(path, condition)])))])
    }
    fn apply(values: Vec<BsonValue>, condition: BsonValue) -> BsonValue {
        DocumentUpdater::compile(&specification("v", condition))
            .unwrap()
            .apply(&doc([
                ("_id", BsonValue::Int32(7)),
                ("v", BsonValue::Array(values)),
            ]))
            .unwrap()
            .get_first("v")
            .unwrap()
            .clone()
    }
    fn code(error: EngineError) -> i32 {
        let mut cause = error.source();
        while let Some(source) = cause {
            if let Some(error) = source.downcast_ref::<DocumentQueryError>() {
                return error.mongo_code();
            }
            if let Some(error) = source.downcast_ref::<DocumentUpdateError>() {
                return error.mongo_code();
            }
            if let Some(error) = source.downcast_ref::<DocumentMutationError>() {
                return error.mongo_code();
            }
            cause = source.source();
        }
        panic!("missing typed error: {error}")
    }

    #[test]
    fn pull_distinguishes_literal_equality_field_predicates_and_document_queries() {
        let values = vec![
            BsonValue::Int64(1),
            BsonValue::Double(1.0),
            BsonValue::Boolean(true),
            array(&[1, 2]),
        ];
        assert_eq!(
            apply(values.clone(), BsonValue::Int32(1)),
            BsonValue::Array(vec![BsonValue::Boolean(true), array(&[1, 2])])
        );
        assert_eq!(
            apply(
                values,
                BsonValue::Document(doc([("$eq", BsonValue::Int32(1))]))
            ),
            BsonValue::Array(vec![BsonValue::Boolean(true)])
        );
        let regex = BsonValue::RegularExpression(BsonRegex::new("^a", "i").unwrap());
        assert_eq!(
            apply(vec![regex.clone(), BsonValue::from("Alpha")], regex),
            BsonValue::Array(vec![BsonValue::from("Alpha")])
        );
        let values = vec![
            BsonValue::Document(doc([("_id", array(&[1, 2]))])),
            BsonValue::Document(doc([("_id", BsonValue::Int64(2))])),
            BsonValue::Document(doc([("_id", BsonValue::Int32(3))])),
            array(&[2]),
            BsonValue::Null,
        ];
        assert_eq!(
            apply(
                values,
                BsonValue::Document(doc([("_id", BsonValue::Int32(2))]))
            ),
            BsonValue::Array(vec![
                BsonValue::Document(doc([("_id", BsonValue::Int32(3))])),
                array(&[2]),
                BsonValue::Null
            ])
        );
        assert_eq!(
            apply(
                vec![
                    BsonValue::Document(BsonDocument::new()),
                    array(&[]),
                    BsonValue::Null
                ],
                BsonValue::Document(BsonDocument::new())
            ),
            BsonValue::Array(vec![array(&[]), BsonValue::Null])
        );
    }

    #[test]
    fn pull_eager_errors_keep_update_codes_and_do_not_short_circuit_validation() {
        for (condition, expected) in [
            (doc([("$expr", BsonValue::Null)]), 224),
            (
                doc([(
                    "$or",
                    BsonValue::Array(vec![
                        BsonValue::Document(BsonDocument::new()),
                        BsonValue::Document(doc([("$expr", BsonValue::Null)])),
                    ]),
                )]),
                224,
            ),
            (
                doc([(
                    "$not",
                    BsonValue::Document(doc([("$eq", BsonValue::Int32(1))])),
                )]),
                2,
            ),
            (
                doc([("a", BsonValue::Document(doc([("$expr", BsonValue::Null)])))]),
                2,
            ),
            (
                doc([(
                    "$elemMatch",
                    BsonValue::Document(doc([("$expr", BsonValue::Null)])),
                )]),
                2,
            ),
            (doc([("$or", BsonValue::Array(vec![BsonValue::Null]))]), 2),
            (doc([("$regex", BsonValue::from("["))]), 51091),
            (
                doc([
                    ("$regex", BsonValue::from("a")),
                    ("$options", BsonValue::from("q")),
                ]),
                51108,
            ),
        ] {
            let error =
                DocumentUpdater::compile(&specification("absent", BsonValue::Document(condition)))
                    .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
            assert_eq!(code(error), expected);
        }
    }

    #[test]
    fn pull_paths_identity_cancellation_and_original_bytes_are_preserved() {
        let original = doc([
            ("_id", BsonValue::Document(doc([("v", array(&[1, 2]))]))),
            ("v", BsonValue::Array(vec![array(&[1, 2]), BsonValue::Null])),
            ("scalar", BsonValue::Int32(1)),
        ]);
        let before = encode_document(&original).unwrap();
        for (path, expected) in [
            ("scalar", 2),
            ("scalar.x", 28),
            ("v.01", 28),
            ("v.1.x", 28),
            ("_id.v", 66),
        ] {
            assert_eq!(
                code(
                    DocumentUpdater::compile(&specification(path, BsonValue::Int32(1)))
                        .unwrap()
                        .apply(&original)
                        .unwrap_err()
                ),
                expected
            );
            assert_eq!(encode_document(&original).unwrap(), before);
        }
        for path in ["absent", "v.999999999999999999999999", "_id.v"] {
            let result = DocumentUpdater::compile(&specification(path, BsonValue::Int32(99)))
                .unwrap()
                .apply(&original)
                .unwrap();
            assert_eq!(encode_document(&result).unwrap(), before);
        }
        let result = DocumentUpdater::compile(&specification("v.0", BsonValue::Int32(1)))
            .unwrap()
            .apply(&original)
            .unwrap();
        assert_eq!(
            result.get_first("v"),
            Some(&BsonValue::Array(vec![array(&[2]), BsonValue::Null]))
        );
        let updater = DocumentUpdater::compile(&specification(
            "v.0",
            BsonValue::Document(doc([("$gte", BsonValue::Int32(1))])),
        ))
        .unwrap();
        let mut checks = 0;
        assert_eq!(
            updater
                .apply_with_check(&original, &mut || {
                    checks += 1;
                    if checks == 25 {
                        Err(EngineError::new(EngineErrorKind::Cancelled, "test"))
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err()
                .kind(),
            EngineErrorKind::Cancelled
        );
        assert_eq!(encode_document(&original).unwrap(), before);
    }

    #[test]
    fn pull_charges_values_regex_work_and_path_candidates_before_use() {
        struct Reject(&'static str);
        impl MatchControl for Reject {
            fn step(&mut self) -> EngineResult<()> {
                Ok(())
            }
            fn value(&mut self, _: &BsonValue) -> EngineResult<()> {
                if self.0 == "value" {
                    Err(limit())
                } else {
                    Ok(())
                }
            }
            fn comparison_bytes(&mut self, _: usize) -> EngineResult<()> {
                if self.0 == "regex" {
                    Err(limit())
                } else {
                    Ok(())
                }
            }
            fn allocation(&mut self, _: usize) -> EngineResult<()> {
                if self.0 == "path" {
                    Err(limit())
                } else {
                    Ok(())
                }
            }
        }
        for (condition, actual, reject) in [
            (BsonValue::Int32(1), BsonValue::Int32(1), "value"),
            (
                BsonValue::Document(doc([("$gt", BsonValue::Int32(1))])),
                BsonValue::Int32(2),
                "value",
            ),
            (
                BsonValue::Document(doc([("$regex", BsonValue::from("a"))])),
                BsonValue::from("a"),
                "regex",
            ),
            (
                BsonValue::Document(doc([("a", BsonValue::Int32(1))])),
                BsonValue::Document(doc([("a", BsonValue::Int32(1))])),
                "path",
            ),
        ] {
            let matcher =
                PullMatcher::compile(&condition, &mut 0, 64 * 1024 * 1024, &mut || Ok(())).unwrap();
            assert_eq!(
                matcher
                    .matches(&actual, &mut Reject(reject))
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::LimitExceeded
            );
        }
        let condition = BsonValue::Document(doc([("$regex", BsonValue::from("a"))]));
        assert_eq!(
            PullMatcher::compile(&condition, &mut 0, 1024, &mut || Ok(()))
                .err()
                .unwrap()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        let path = ".".repeat(99);
        let condition = BsonValue::Document(doc([(&path, BsonValue::Int32(1))]));
        assert_eq!(
            PullMatcher::compile(&condition, &mut 0, 2048, &mut || Ok(()))
                .err()
                .unwrap()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        assert!(PullMatcher::compile(&condition, &mut 0, 32768, &mut || Ok(())).is_ok());
        // Actual cumulative comparison work fails after a provisional removal;
        // neither short-circuiting nor in-place compaction leaks a partial image.
        let mut candidates = vec![BsonValue::from("drop")];
        candidates
            .extend((0..100).map(|index| BsonValue::from(format!("{}-{index}", "y".repeat(4000)))));
        let mut values = vec![BsonValue::from("drop")];
        values.extend((0..200).map(|_| BsonValue::from("x".repeat(4000))));
        let original = doc([
            ("_id", BsonValue::Int32(7)),
            ("v", BsonValue::Array(values)),
        ]);
        let before = encode_document(&original).unwrap();
        let updater = DocumentUpdater::compile(&specification(
            "v",
            BsonValue::Document(doc([("$in", BsonValue::Array(candidates))])),
        ))
        .unwrap();
        assert_eq!(
            updater.apply(&original).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        assert_eq!(encode_document(&original).unwrap(), before);
    }
}
