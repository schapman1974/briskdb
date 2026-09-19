//! Bounded, eagerly validated field updates. No storage or protocol policy.

use std::{error::Error, fmt};

use super::{
    BsonCodecOptions, BsonDocument, BsonErrorContext, BsonValue, CanonicalBsonKey,
    DocumentMutationError, encode_document, encode_document_with_options, memory,
};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

const MAX_SPEC_BYTES: usize = 1024 * 1024;
const MAX_OPERATIONS: usize = 4096;
const MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;
const MAX_STEPS: usize = 1_000_000;

/// Payload-free validation errors, shared by embedded and wire callers.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentUpdateError {
    InvalidExpression,
    UnsupportedOperator,
    InvalidPath,
    ConflictingPaths,
    PathNotViable,
}

impl DocumentUpdateError {
    pub const fn mongo_code(self) -> i32 {
        match self {
            Self::InvalidExpression => 9,
            Self::UnsupportedOperator => 115,
            Self::InvalidPath => 56,
            Self::ConflictingPaths => 40,
            Self::PathNotViable => 28,
        }
    }

    fn error(self) -> EngineError {
        let kind = if self == Self::UnsupportedOperator {
            EngineErrorKind::Unsupported
        } else {
            EngineErrorKind::InvalidArgument
        };
        EngineError::from_source(kind, self.to_string(), self)
    }
}

impl fmt::Display for DocumentUpdateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidExpression => "update requires operator documents",
            Self::UnsupportedOperator => "update operator or positional path is not supported",
            Self::InvalidPath => "update path contains an empty component",
            Self::ConflictingPaths => "update paths overlap",
            Self::PathNotViable => "update path cannot traverse this value",
        })
    }
}
impl Error for DocumentUpdateError {}

struct Operation {
    path: Vec<String>,
    value: Option<BsonValue>,
}

/// `$set`/`$unset` transformation preserving untouched BSON representations and
/// field order. Paths are non-positional; missing set parents become documents.
/// Operations follow specification order, as in the frozen TinyMongo contract.
pub struct DocumentUpdater {
    operations: Vec<Operation>,
    retained_bytes: usize,
}

impl fmt::Debug for DocumentUpdater {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentUpdater").finish_non_exhaustive()
    }
}

impl DocumentUpdater {
    pub fn compile(spec: &BsonDocument) -> EngineResult<Self> {
        Self::compile_with_check(spec, &mut || Ok(()))
    }

    pub fn compile_with_check(
        spec: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Self> {
        check()?;
        encode_document_with_options(
            spec,
            &BsonCodecOptions::new().with_max_document_bytes(MAX_SPEC_BYTES),
        )
        .map_err(|e| e.into_engine_error(BsonErrorContext::ClientInput))?;
        if spec.is_empty() {
            return Err(DocumentUpdateError::InvalidExpression.error());
        }
        let mut retained_bytes = memory::document_bytes(spec, MAX_RETAINED_BYTES, check)?;
        let mut operations = Vec::new();
        for (operator, operand) in spec.iter() {
            check()?;
            let set = match operator {
                "$set" => true,
                "$unset" => false,
                name if name.starts_with('$') => {
                    return Err(DocumentUpdateError::UnsupportedOperator.error());
                }
                _ => return Err(DocumentUpdateError::InvalidExpression.error()),
            };
            let BsonValue::Document(fields) = operand else {
                return Err(DocumentUpdateError::InvalidExpression.error());
            };
            for (path, value) in fields.iter() {
                check()?;
                if operations.len() == MAX_OPERATIONS {
                    return Err(limit());
                }
                let components = path.split('.').count();
                if components > 100 {
                    return Err(limit());
                }
                retained_bytes = retained_bytes
                    .checked_add(components * 128 + 128)
                    .filter(|bytes| *bytes <= MAX_RETAINED_BYTES)
                    .ok_or_else(limit)?;
                let path: Vec<String> = path.split('.').map(str::to_owned).collect();
                if path.iter().any(String::is_empty) {
                    return Err(DocumentUpdateError::InvalidPath.error());
                }
                if path.iter().any(|part| part.starts_with('$')) {
                    return Err(DocumentUpdateError::UnsupportedOperator.error());
                }
                operations.push(Operation {
                    path,
                    value: set.then(|| value.clone()),
                });
            }
        }
        let mut paths: Vec<_> = operations.iter().map(|op| &op.path).collect();
        paths.sort_unstable();
        for pair in paths.windows(2) {
            check()?;
            if pair[1].starts_with(pair[0]) {
                return Err(DocumentUpdateError::ConflictingPaths.error());
            }
        }
        Ok(Self {
            operations,
            retained_bytes,
        })
    }

    pub fn apply(&self, document: &BsonDocument) -> EngineResult<BsonDocument> {
        self.apply_with_check(document, &mut || Ok(()))
    }

    pub fn apply_with_check(
        &self,
        document: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<BsonDocument> {
        check()?;
        encode_document(document)
            .map_err(|e| e.into_engine_error(BsonErrorContext::ClientInput))?;
        let input_bytes = memory::document_bytes(document, MAX_RETAINED_BYTES, check)?;
        let mut budget = Budget {
            bytes: self.retained_bytes,
            steps: 0,
            check,
        };
        // Keep the original and a private post-image until the transaction has
        // preflighted both its write and its exact result. Charge before cloning.
        budget.charge(input_bytes.checked_mul(2).ok_or_else(limit)?)?;
        let mut result = document.clone();
        for operation in &self.operations {
            write_document(
                &mut result,
                &operation.path,
                operation.value.as_ref(),
                &mut budget,
            )?;
        }
        if let Some(original_id) = document.get_first("_id") {
            let id = result
                .get_first("_id")
                .ok_or_else(|| DocumentMutationError::ImmutableId.into_engine_error())?;
            let key = |value: &BsonValue| {
                CanonicalBsonKey::encode(value)
                    .map_err(|e| e.into_engine_error(BsonErrorContext::ClientInput))
            };
            if key(id)? != key(original_id)? {
                return Err(DocumentMutationError::ImmutableId.into_engine_error());
            }
            // Numeric aliases of the same identity cannot move or retype _id.
            if let Some((_, id)) = result
                .entries_mut()
                .iter_mut()
                .find(|(name, _)| name == "_id")
            {
                *id = original_id.clone();
            }
        }
        (budget.check)()?;
        encode_document(&result).map_err(|e| e.into_engine_error(BsonErrorContext::ClientInput))?;
        Ok(result)
    }
}

fn limit() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "document update resource limit exceeded",
    )
}

struct Budget<'a> {
    bytes: usize,
    steps: usize,
    check: &'a mut dyn FnMut() -> EngineResult<()>,
}
impl Budget<'_> {
    fn step(&mut self) -> EngineResult<()> {
        (self.check)()?;
        self.steps += 1;
        if self.steps > MAX_STEPS {
            return Err(limit());
        }
        Ok(())
    }
    fn charge(&mut self, amount: usize) -> EngineResult<()> {
        self.bytes = self
            .bytes
            .checked_add(amount)
            .filter(|n| *n <= MAX_RETAINED_BYTES)
            .ok_or_else(limit)?;
        Ok(())
    }
    fn clone_value(&mut self, value: &BsonValue) -> EngineResult<BsonValue> {
        let bytes = memory::value_bytes(value, MAX_RETAINED_BYTES, self.check)?;
        self.charge(bytes)?;
        Ok(value.clone())
    }
}

fn write_document(
    document: &mut BsonDocument,
    path: &[String],
    value: Option<&BsonValue>,
    budget: &mut Budget<'_>,
) -> EngineResult<()> {
    budget.step()?;
    let mut position = None;
    for (index, (name, _)) in document.iter().enumerate() {
        budget.step()?;
        if name == path[0] {
            position = Some(index);
            break;
        }
    }
    if path.len() == 1 {
        match (position, value) {
            (Some(index), Some(value)) => {
                document.entries_mut()[index].1 = budget.clone_value(value)?
            }
            (Some(index), None) => {
                document.entries_mut().remove(index);
            }
            (None, Some(value)) => {
                budget.charge(path[0].len() + 128)?;
                let value = budget.clone_value(value)?;
                document
                    .push(path[0].clone(), value)
                    .map_err(|e| e.into_engine_error(BsonErrorContext::ClientInput))?;
            }
            (None, None) => {}
        }
        return Ok(());
    }
    let position = match position {
        Some(index) => index,
        None if value.is_none() => return Ok(()),
        None => {
            budget.charge(path[0].len() + 256)?;
            document
                .push(path[0].clone(), BsonValue::Document(BsonDocument::new()))
                .map_err(|e| e.into_engine_error(BsonErrorContext::ClientInput))?;
            document.len() - 1
        }
    };
    write_value(
        &mut document.entries_mut()[position].1,
        &path[1..],
        value,
        budget,
    )
}

fn write_value(
    target: &mut BsonValue,
    path: &[String],
    value: Option<&BsonValue>,
    budget: &mut Budget<'_>,
) -> EngineResult<()> {
    budget.step()?;
    match target {
        BsonValue::Document(document) => write_document(document, path, value, budget),
        BsonValue::Array(array) => {
            let component = &path[0];
            if component != "0"
                && (component.starts_with('0') || !component.bytes().all(|b| b.is_ascii_digit()))
            {
                return if value.is_none() {
                    Ok(())
                } else {
                    Err(DocumentUpdateError::PathNotViable.error())
                };
            }
            let index = match component.parse::<usize>() {
                Ok(index) => index,
                Err(_) if value.is_none() => return Ok(()),
                Err(_) => return Err(limit()),
            };
            if index >= array.len() {
                if value.is_none() {
                    return Ok(());
                }
                let added = index
                    .checked_add(1)
                    .and_then(|n| n.checked_sub(array.len()))
                    .ok_or_else(limit)?;
                budget.charge(added.checked_mul(128).ok_or_else(limit)?)?;
                array.try_reserve_exact(added).map_err(|_| limit())?;
                array.resize(index + 1, BsonValue::Null);
                if path.len() > 1 {
                    array[index] = BsonValue::Document(BsonDocument::new());
                }
            }
            if path.len() == 1 {
                array[index] = match value {
                    Some(value) => budget.clone_value(value)?,
                    None => BsonValue::Null,
                };
                Ok(())
            } else {
                write_value(&mut array[index], &path[1..], value, budget)
            }
        }
        _ if value.is_none() => Ok(()),
        _ => Err(DocumentUpdateError::PathNotViable.error()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
        BsonDocument::from_entries(fields).unwrap()
    }
    fn spec(operator: &str, fields: BsonDocument) -> BsonDocument {
        doc([(operator, BsonValue::Document(fields))])
    }
    fn code(error: EngineError) -> i32 {
        let mut source = error.source();
        while let Some(cause) = source {
            if let Some(error) = cause.downcast_ref::<DocumentUpdateError>() {
                return error.mongo_code();
            }
            if let Some(error) = cause.downcast_ref::<DocumentMutationError>() {
                return error.mongo_code();
            }
            source = cause.source();
        }
        panic!("missing typed error: {error}")
    }

    #[test]
    fn field_updates_preserve_representation_order_and_literal_values() {
        let original = doc([
            ("_id", BsonValue::Int32(1)),
            ("x", BsonValue::Int32(1)),
            ("keep", BsonValue::from("yes")),
        ]);
        let update = spec(
            "$set",
            doc([
                ("x", BsonValue::Int64(1)),
                ("nested.0.a", BsonValue::from("$literal")),
                (
                    "stamp",
                    BsonValue::Timestamp(super::super::BsonTimestamp::new(0, 0)),
                ),
            ]),
        );
        let result = DocumentUpdater::compile(&update)
            .unwrap()
            .apply(&original)
            .unwrap();
        assert!(matches!(result.get_first("x"), Some(BsonValue::Int64(1))));
        assert_eq!(
            result.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            ["_id", "x", "keep", "nested", "stamp"]
        );
        assert_eq!(
            result.get_first("stamp"),
            update.get_first("$set").and_then(|v| match v {
                BsonValue::Document(d) => d.get_first("stamp"),
                _ => None,
            })
        );
        assert_ne!(
            encode_document(&original).unwrap(),
            encode_document(&result).unwrap()
        );
        let same = DocumentUpdater::compile(&spec("$set", doc([("_id", BsonValue::Double(1.0))])))
            .unwrap()
            .apply(&original)
            .unwrap();
        assert_eq!(
            encode_document(&same).unwrap(),
            encode_document(&original).unwrap()
        );
        assert!(!format!("{:?}", DocumentUpdater::compile(&update).unwrap()).contains("literal"));
    }

    #[test]
    fn arrays_extend_boundedly_and_unset_keeps_positions() {
        let original = doc([("a", BsonValue::Array(vec![BsonValue::Int32(7)]))]);
        let result = DocumentUpdater::compile(&spec("$set", doc([("a.3.x", BsonValue::Int32(1))])))
            .unwrap()
            .apply(&original)
            .unwrap();
        assert_eq!(
            result,
            doc([(
                "a",
                BsonValue::Array(vec![
                    BsonValue::Int32(7),
                    BsonValue::Null,
                    BsonValue::Null,
                    BsonValue::Document(doc([("x", BsonValue::Int32(1))]))
                ])
            )])
        );
        let result =
            DocumentUpdater::compile(&spec("$unset", doc([("a.0", BsonValue::from("ignored"))])))
                .unwrap()
                .apply(&result)
                .unwrap();
        let Some(BsonValue::Array(array)) = result.get_first("a") else {
            panic!()
        };
        assert_eq!(array.len(), 4);
        assert_eq!(array[0], BsonValue::Null);
        for index in ["18446744073709551615", "999999999999999999999999999999"] {
            let path = format!("a.{index}");
            let error = DocumentUpdater::compile(&spec("$set", doc([(&path, BsonValue::Null)])))
                .unwrap()
                .apply(&original)
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
            let result = DocumentUpdater::compile(&spec("$unset", doc([(&path, BsonValue::Null)])))
                .unwrap()
                .apply(&original)
                .unwrap();
            assert_eq!(result, original);
        }
    }

    #[test]
    fn invalid_shapes_paths_and_conflicts_are_eager() {
        for (spec, expected) in [
            (BsonDocument::new(), 9),
            (doc([("plain", BsonValue::Int32(1))]), 9),
            (doc([("$set", BsonValue::Int32(1))]), 9),
            (spec("$inc", BsonDocument::new()), 115),
            (spec("$set", doc([("a..b", BsonValue::Null)])), 56),
            (spec("$set", doc([("a.$[].b", BsonValue::Null)])), 115),
            (
                spec(
                    "$set",
                    doc([("a", BsonValue::Null), ("a.b", BsonValue::Null)]),
                ),
                40,
            ),
            (
                doc([
                    ("$set", BsonValue::Document(doc([("a.b", BsonValue::Null)]))),
                    ("$unset", BsonValue::Document(doc([("a", BsonValue::Null)]))),
                ]),
                40,
            ),
        ] {
            assert_eq!(code(DocumentUpdater::compile(&spec).unwrap_err()), expected);
        }
        DocumentUpdater::compile(&spec("$set", BsonDocument::new())).unwrap();
        DocumentUpdater::compile(&spec(
            "$set",
            doc([("a", BsonValue::Null), ("ab", BsonValue::Null)]),
        ))
        .unwrap();
    }

    #[test]
    fn failed_paths_and_identity_changes_leave_input_untouched() {
        let original = doc([("_id", BsonValue::Int32(1)), ("a", BsonValue::Int32(7))]);
        for (operator, path, value, expected) in [
            ("$set", "a.b", BsonValue::Null, 28),
            ("$set", "_id", BsonValue::Boolean(true), 66),
            ("$unset", "_id", BsonValue::Null, 66),
        ] {
            let before = encode_document(&original).unwrap();
            assert_eq!(
                code(
                    DocumentUpdater::compile(&spec(operator, doc([(path, value)])))
                        .unwrap()
                        .apply(&original)
                        .unwrap_err()
                ),
                expected
            );
            assert_eq!(encode_document(&original).unwrap(), before);
        }
        let unchanged = DocumentUpdater::compile(&spec(
            "$unset",
            doc([("a.b", BsonValue::Null), ("absent.x", BsonValue::Null)]),
        ))
        .unwrap()
        .apply(&original)
        .unwrap();
        assert_eq!(unchanged, original);
    }

    #[test]
    fn cancellation_and_growth_depth_are_bounded() {
        let update = spec("$set", doc([("x", BsonValue::Int32(1))]));
        assert_eq!(
            DocumentUpdater::compile_with_check(&update, &mut || Err(EngineError::new(
                EngineErrorKind::Cancelled,
                "stop"
            )))
            .unwrap_err()
            .kind(),
            EngineErrorKind::Cancelled
        );
        let updater = DocumentUpdater::compile(&update).unwrap();
        assert_eq!(
            updater
                .apply_with_check(&BsonDocument::new(), &mut || Err(EngineError::new(
                    EngineErrorKind::Cancelled,
                    "stop"
                )))
                .unwrap_err()
                .kind(),
            EngineErrorKind::Cancelled
        );
        let path = vec!["a"; 101].join(".");
        assert_eq!(
            DocumentUpdater::compile(&spec("$set", doc([(&path, BsonValue::Null)])))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        let fields =
            BsonDocument::from_entries((0..4097).map(|i| (format!("field{i}"), BsonValue::Null)))
                .unwrap();
        assert_eq!(
            DocumentUpdater::compile(&spec("$set", fields))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        let original = BsonDocument::from_entries(
            (0..1500).map(|i| (format!("field{i}"), BsonValue::Int32(i))),
        )
        .unwrap();
        let updates = spec(
            "$unset",
            BsonDocument::from_entries((0..1000).map(|i| (format!("missing{i}"), BsonValue::Null)))
                .unwrap(),
        );
        let updater = DocumentUpdater::compile(&updates).unwrap();
        assert_eq!(
            updater.apply(&original).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let mut checks = 0;
        assert_eq!(
            updater
                .apply_with_check(&original, &mut || {
                    checks += 1;
                    if checks > 1600 {
                        Err(EngineError::new(EngineErrorKind::Cancelled, "stop"))
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err()
                .kind(),
            EngineErrorKind::Cancelled
        );
    }
}
