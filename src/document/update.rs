//! Bounded, eagerly validated field updates. No storage or protocol policy.

mod push;

use std::{cmp::Ordering, error::Error, fmt};

use super::{
    BsonCodecOptions, BsonDocument, BsonErrorContext, BsonValue, CanonicalBsonKey,
    DocumentMutationError, encode_document, encode_document_with_options,
    matcher::{MatchControl, PullMatcher},
    memory,
};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

const MAX_SPEC_BYTES: usize = 1024 * 1024;
const MAX_OPERATIONS: usize = 4096;
const MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;
const MAX_STEPS: usize = 1_000_000;
const MAX_COMPARISON_BYTES: usize = 64 * 1024 * 1024;

/// Payload-free validation errors, shared by embedded and wire callers.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentUpdateError {
    InvalidExpression,
    BadValue,
    TypeMismatch,
    UnsupportedOperator,
    InvalidPath,
    ConflictingPaths,
    PathNotViable,
}

impl DocumentUpdateError {
    pub const fn mongo_code(self) -> i32 {
        match self {
            Self::InvalidExpression => 9,
            Self::BadValue => 2,
            Self::TypeMismatch => 14,
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
            Self::BadValue => "update operand or rename path is invalid",
            Self::TypeMismatch => "update target has an invalid BSON type",
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
    action: OperationAction,
}

enum OperationAction {
    Field(Action),
    Pop {
        front: bool,
    },
    Rename {
        target: Vec<String>,
    },
    ArrayMembership {
        values: Vec<BsonValue>,
        remove: bool,
    },
    Push(push::Push),
    Pull(PullMatcher),
}

enum Action {
    Set(BsonValue),
    Unset,
    Min(BsonValue),
    Max(BsonValue),
}

impl Action {
    fn value(&self) -> Option<&BsonValue> {
        match self {
            Self::Set(value) | Self::Min(value) | Self::Max(value) => Some(value),
            Self::Unset => None,
        }
    }

    fn replaces(&self, current: &BsonValue, budget: &mut Budget<'_>) -> EngineResult<bool> {
        match self {
            Self::Set(_) => Ok(true),
            Self::Unset => Ok(false),
            Self::Min(value) | Self::Max(value) => {
                // Whole-value BSON order, not query-sort array element order.
                // Charge traversal and comparison work even when no clone/write
                // follows, and check cancellation before/after comparison.
                budget.comparison_value(current)?;
                budget.comparison_value(value)?;
                let order = value.cmp(current);
                budget.step()?;
                Ok(order
                    == if matches!(self, Self::Min(_)) {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    })
            }
        }
    }
}

/// Field and array transformations preserving untouched BSON
/// representations and field order. Paths are non-positional; missing write
/// parents become documents. Equal min/max values preserve their stored type.
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
            match operator {
                "$set" | "$unset" | "$min" | "$max" | "$pop" | "$rename" | "$addToSet"
                | "$pullAll" | "$push" | "$pull" => {}
                name if name.starts_with('$') => {
                    return Err(DocumentUpdateError::UnsupportedOperator.error());
                }
                _ => return Err(DocumentUpdateError::InvalidExpression.error()),
            }
            let BsonValue::Document(fields) = operand else {
                return Err(DocumentUpdateError::InvalidExpression.error());
            };
            for (path, value) in fields.iter() {
                check()?;
                if operations.len() == MAX_OPERATIONS {
                    return Err(limit());
                }
                let path = compile_path(path, operator == "$rename", &mut retained_bytes)?;
                let action = match operator {
                    "$set" => OperationAction::Field(Action::Set(value.clone())),
                    "$unset" => OperationAction::Field(Action::Unset),
                    "$min" => OperationAction::Field(Action::Min(value.clone())),
                    "$max" => OperationAction::Field(Action::Max(value.clone())),
                    "$pop" => {
                        let front = match value {
                            value if *value == BsonValue::Int32(-1) => true,
                            value if *value == BsonValue::Int32(1) => false,
                            _ => return Err(DocumentUpdateError::InvalidExpression.error()),
                        };
                        OperationAction::Pop { front }
                    }
                    "$rename" => {
                        let BsonValue::String(target) = value else {
                            return Err(DocumentUpdateError::BadValue.error());
                        };
                        let target = compile_path(target, true, &mut retained_bytes)?;
                        if path.starts_with(&target) || target.starts_with(&path) {
                            return Err(DocumentUpdateError::BadValue.error());
                        }
                        OperationAction::Rename { target }
                    }
                    "$addToSet" => OperationAction::ArrayMembership {
                        values: add_to_set_values(value)?,
                        remove: false,
                    },
                    "$pullAll" => {
                        let BsonValue::Array(values) = value else {
                            return Err(DocumentUpdateError::BadValue.error());
                        };
                        OperationAction::ArrayMembership {
                            values: values.clone(),
                            remove: true,
                        }
                    }
                    "$push" => OperationAction::Push(push::Push::compile(
                        value,
                        &mut retained_bytes,
                        check,
                    )?),
                    "$pull" => OperationAction::Pull(PullMatcher::compile(
                        value,
                        &mut retained_bytes,
                        MAX_RETAINED_BYTES,
                        check,
                    )?),
                    _ => unreachable!("operator prevalidated"),
                };
                operations.push(Operation { path, action });
            }
        }
        let mut paths = Vec::with_capacity(operations.len() * 2);
        for operation in &operations {
            paths.push(&operation.path);
            if let OperationAction::Rename { target } = &operation.action {
                paths.push(target);
            }
        }
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
            comparison_bytes: 0,
            steps: 0,
            check,
        };
        // Keep the original and a private post-image until the transaction has
        // preflighted both its write and its exact result. Charge before cloning.
        budget.charge(input_bytes.checked_mul(2).ok_or_else(limit)?)?;
        let mut result = document.clone();
        for operation in &self.operations {
            match &operation.action {
                OperationAction::Field(action) => {
                    write_document(&mut result, &operation.path, action, true, &mut budget)?;
                }
                OperationAction::Pop { front } => {
                    pop_document(&mut result, &operation.path, *front, &mut budget)?;
                }
                OperationAction::Rename { target } => {
                    rename_document(&mut result, &operation.path, target, &mut budget)?;
                }
                OperationAction::ArrayMembership { values, remove } => {
                    array_membership_document(
                        &mut result,
                        &operation.path,
                        values,
                        *remove,
                        &mut budget,
                    )?;
                }
                OperationAction::Push(push) => {
                    push.apply_document(&mut result, &operation.path, &mut budget)?
                }
                OperationAction::Pull(matcher) => {
                    if let Some(value) =
                        existing_document_value(&mut result, &operation.path, true, &mut budget)?
                    {
                        let BsonValue::Array(values) = value else {
                            return Err(DocumentUpdateError::BadValue.error());
                        };
                        let mut kept = 0;
                        for read in 0..values.len() {
                            budget.step()?;
                            if !matcher.matches(&values[read], &mut budget)? {
                                values.swap(kept, read);
                                kept += 1;
                            }
                        }
                        values.truncate(kept);
                    }
                }
            }
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

fn add_to_set_values(value: &BsonValue) -> EngineResult<Vec<BsonValue>> {
    if let BsonValue::Document(modifiers) = value {
        if modifiers.iter().any(|(name, _)| name.starts_with('$')) {
            if modifiers.len() != 1 {
                return Err(DocumentUpdateError::BadValue.error());
            }
            let Some(BsonValue::Array(values)) = modifiers.get_first("$each") else {
                return Err(DocumentUpdateError::BadValue.error());
            };
            return Ok(values.clone());
        }
    }
    // A plain array/document is one literal element, not an implicit $each.
    Ok(vec![value.clone()])
}

fn compile_path(path: &str, rename: bool, retained_bytes: &mut usize) -> EngineResult<Vec<String>> {
    let components = path.split('.').count();
    if components > 100 {
        return Err(limit());
    }
    *retained_bytes = retained_bytes
        .checked_add(components * 128 + path.len() + 128)
        .filter(|bytes| *bytes <= MAX_RETAINED_BYTES)
        .ok_or_else(limit)?;
    if path.contains('\0') {
        return Err(DocumentUpdateError::BadValue.error());
    }
    let parts: Vec<String> = path.split('.').map(str::to_owned).collect();
    if parts.iter().any(String::is_empty) {
        return Err(DocumentUpdateError::InvalidPath.error());
    }
    if parts.iter().any(|part| part.starts_with('$')) {
        return Err(if rename {
            DocumentUpdateError::BadValue
        } else {
            DocumentUpdateError::UnsupportedOperator
        }
        .error());
    }
    Ok(parts)
}

fn limit() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "document update resource limit exceeded",
    )
}

struct Budget<'a> {
    bytes: usize,
    comparison_bytes: usize,
    steps: usize,
    check: &'a mut dyn FnMut() -> EngineResult<()>,
}

impl MatchControl for Budget<'_> {
    fn step(&mut self) -> EngineResult<()> {
        Budget::step(self)
    }
    fn value(&mut self, value: &BsonValue) -> EngineResult<()> {
        self.comparison_value(value)
    }
    fn comparison_bytes(&mut self, bytes: usize) -> EngineResult<()> {
        self.comparison_bytes = self
            .comparison_bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= MAX_COMPARISON_BYTES)
            .ok_or_else(limit)?;
        Ok(())
    }
    fn allocation(&mut self, bytes: usize) -> EngineResult<()> {
        self.charge(bytes)
    }
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

    fn comparison_value(&mut self, value: &BsonValue) -> EngineResult<()> {
        let remaining = MAX_COMPARISON_BYTES - self.comparison_bytes;
        let bytes = memory::value_bytes(value, remaining, &mut || self.step())?;
        self.comparison_bytes += bytes;
        Ok(())
    }
}

fn write_document(
    document: &mut BsonDocument,
    path: &[String],
    action: &Action,
    array_paths: bool,
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
        match (position, action.value()) {
            (Some(index), Some(value)) => {
                if action.replaces(&document.entries_mut()[index].1, budget)? {
                    document.entries_mut()[index].1 = budget.clone_value(value)?;
                }
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
        None if action.value().is_none() => return Ok(()),
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
        action,
        array_paths,
        budget,
    )
}

fn write_value(
    target: &mut BsonValue,
    path: &[String],
    action: &Action,
    array_paths: bool,
    budget: &mut Budget<'_>,
) -> EngineResult<()> {
    budget.step()?;
    match target {
        BsonValue::Document(document) => {
            write_document(document, path, action, array_paths, budget)
        }
        BsonValue::Array(array) => {
            let component = &path[0];
            if component != "0"
                && (component.starts_with('0') || !component.bytes().all(|b| b.is_ascii_digit()))
            {
                return if action.value().is_none() {
                    Ok(())
                } else {
                    Err(DocumentUpdateError::PathNotViable.error())
                };
            }
            if !array_paths {
                return Err(DocumentUpdateError::BadValue.error());
            }
            let index = match component.parse::<usize>() {
                Ok(index) => index,
                Err(_) if action.value().is_none() => return Ok(()),
                Err(_) => return Err(limit()),
            };
            let missing = index >= array.len();
            if missing {
                if action.value().is_none() {
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
                match action.value() {
                    Some(value) if missing || action.replaces(&array[index], budget)? => {
                        array[index] = budget.clone_value(value)?;
                    }
                    None => array[index] = BsonValue::Null,
                    _ => {}
                }
                Ok(())
            } else {
                write_value(&mut array[index], &path[1..], action, array_paths, budget)
            }
        }
        _ if action.value().is_none() => Ok(()),
        _ => Err(DocumentUpdateError::PathNotViable.error()),
    }
}

// Existing-only traversal: pop/rename never create paths while looking up a
// source. Canonical indices beyond usize are simply absent, without allocation.
fn existing_document_value<'a>(
    document: &'a mut BsonDocument,
    path: &[String],
    array_paths: bool,
    budget: &mut Budget<'_>,
) -> EngineResult<Option<&'a mut BsonValue>> {
    budget.step()?;
    for (name, value) in document.entries_mut() {
        budget.step()?;
        if name == &path[0] {
            return existing_value(value, &path[1..], array_paths, budget);
        }
    }
    Ok(None)
}

fn existing_value<'a>(
    value: &'a mut BsonValue,
    path: &[String],
    array_paths: bool,
    budget: &mut Budget<'_>,
) -> EngineResult<Option<&'a mut BsonValue>> {
    budget.step()?;
    if path.is_empty() {
        return Ok(Some(value));
    }
    match value {
        BsonValue::Document(document) => {
            existing_document_value(document, path, array_paths, budget)
        }
        BsonValue::Array(values) => {
            let part = &path[0];
            if part != "0" && (part.starts_with('0') || !part.bytes().all(|b| b.is_ascii_digit())) {
                return Err(DocumentUpdateError::PathNotViable.error());
            }
            if !array_paths {
                return Err(DocumentUpdateError::BadValue.error());
            }
            let Some(value) = part
                .parse::<usize>()
                .ok()
                .and_then(|index| values.get_mut(index))
            else {
                return Ok(None);
            };
            existing_value(value, &path[1..], array_paths, budget)
        }
        _ => Err(DocumentUpdateError::PathNotViable.error()),
    }
}

fn pop_document(
    document: &mut BsonDocument,
    path: &[String],
    front: bool,
    budget: &mut Budget<'_>,
) -> EngineResult<()> {
    let Some(value) = existing_document_value(document, path, true, budget)? else {
        return Ok(());
    };
    let BsonValue::Array(values) = value else {
        return Err(DocumentUpdateError::TypeMismatch.error());
    };
    if !values.is_empty() {
        // Charge possible element movement before mutating the private image.
        for _ in values.iter() {
            budget.step()?;
        }
        if front {
            values.remove(0);
        } else {
            values.pop();
        }
    }
    Ok(())
}

fn rename_document(
    document: &mut BsonDocument,
    source: &[String],
    target: &[String],
    budget: &mut Budget<'_>,
) -> EngineResult<()> {
    let Some(value) = existing_document_value(document, source, false, budget)? else {
        return Ok(());
    };
    if source[0] == "_id" || target[0] == "_id" {
        return Err(DocumentMutationError::ImmutableId.into_engine_error());
    }
    let value = budget.clone_value(value)?;
    write_document(document, source, &Action::Unset, false, budget)?;
    write_document(document, target, &Action::Set(value), false, budget)
}

fn contains_value(
    values: &[BsonValue],
    candidate: &BsonValue,
    budget: &mut Budget<'_>,
) -> EngineResult<bool> {
    for existing in values {
        // Bound equality work even for duplicate/no-op updates, and preserve
        // stored representations: BSON equality is not byte equality.
        budget.comparison_value(existing)?;
        budget.comparison_value(candidate)?;
        let equal = existing == candidate;
        budget.step()?;
        if equal {
            return Ok(true);
        }
    }
    Ok(false)
}

fn add_unique(
    values: &mut Vec<BsonValue>,
    candidates: &[BsonValue],
    budget: &mut Budget<'_>,
) -> EngineResult<()> {
    for candidate in candidates {
        budget.step()?;
        if !contains_value(values, candidate, budget)? {
            budget.charge(128)?;
            let value = budget.clone_value(candidate)?;
            values.try_reserve_exact(1).map_err(|_| limit())?;
            values.push(value);
        }
    }
    Ok(())
}

fn array_membership_document(
    document: &mut BsonDocument,
    path: &[String],
    candidates: &[BsonValue],
    remove: bool,
    budget: &mut Budget<'_>,
) -> EngineResult<()> {
    let Some(value) = existing_document_value(document, path, true, budget)? else {
        if !remove {
            let mut values = Vec::new();
            add_unique(&mut values, candidates, budget)?;
            // $each: [] still creates a missing array. Use the same bounded,
            // strict numeric-path writer as field updates, never overwrite a
            // scalar parent or allocate an unchecked array gap.
            write_document(
                document,
                path,
                &Action::Set(BsonValue::Array(values)),
                true,
                budget,
            )?;
        }
        return Ok(());
    };
    let BsonValue::Array(values) = value else {
        return Err(DocumentUpdateError::BadValue.error());
    };
    if remove {
        if candidates.is_empty() {
            return Ok(());
        }
        // Fallible, order-preserving compaction of the private post-image. No
        // extra array copy, and no storage mutation before every check passes.
        let mut kept = 0;
        for read in 0..values.len() {
            budget.step()?;
            if !contains_value(candidates, &values[read], budget)? {
                values.swap(kept, read);
                kept += 1;
            }
        }
        values.truncate(kept);
        Ok(())
    } else {
        add_unique(values, candidates, budget)
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
    fn array_membership_uses_literal_bson_equality_and_preserves_order() {
        let ordered = BsonValue::Document(doc([
            ("a", BsonValue::Int32(1)),
            ("b", BsonValue::Int32(2)),
        ]));
        let reversed = BsonValue::Document(doc([
            ("b", BsonValue::Int32(2)),
            ("a", BsonValue::Int32(1)),
        ]));
        let original = doc([
            ("_id", BsonValue::Int64(7)),
            (
                "values",
                BsonValue::Array(vec![
                    BsonValue::Int64(1),
                    BsonValue::Double(1.0),
                    BsonValue::Boolean(true),
                    ordered.clone(),
                ]),
            ),
            ("grid", BsonValue::Array(vec![BsonValue::Array(vec![])])),
        ]);
        let each = BsonValue::Document(doc([(
            "$each",
            BsonValue::Array(vec![
                BsonValue::Int32(1),
                reversed.clone(),
                reversed.clone(),
                BsonValue::Null,
            ]),
        )]));
        let added = DocumentUpdater::compile(&spec(
            "$addToSet",
            doc([
                ("values", each),
                ("grid.0", BsonValue::Array(vec![BsonValue::Int64(2)])),
                (
                    "grid.3",
                    BsonValue::Document(doc([("$each", BsonValue::Array(vec![]))])),
                ),
            ]),
        ))
        .unwrap()
        .apply(&original)
        .unwrap();
        let expected = BsonValue::Array(vec![
            BsonValue::Int64(1),
            BsonValue::Double(1.0),
            BsonValue::Boolean(true),
            ordered.clone(),
            reversed.clone(),
            BsonValue::Null,
        ]);
        assert_eq!(
            encode_document(&doc([(
                "values",
                added.get_first("values").unwrap().clone()
            )]))
            .unwrap(),
            encode_document(&doc([("values", expected)])).unwrap()
        );
        assert_eq!(
            added.get_first("grid"),
            Some(&BsonValue::Array(vec![
                BsonValue::Array(vec![BsonValue::Array(vec![BsonValue::Int64(2)])]),
                BsonValue::Null,
                BsonValue::Null,
                BsonValue::Array(vec![])
            ]))
        );
        let pulled = DocumentUpdater::compile(&spec(
            "$pullAll",
            doc([
                (
                    "values",
                    BsonValue::Array(vec![BsonValue::Double(1.0), ordered]),
                ),
                ("missing.x", BsonValue::Array(vec![BsonValue::Null])),
                (
                    "grid.0",
                    BsonValue::Array(vec![BsonValue::Array(vec![BsonValue::Int32(2)])]),
                ),
            ]),
        ))
        .unwrap()
        .apply(&added)
        .unwrap();
        assert_eq!(
            pulled.get_first("values"),
            Some(&BsonValue::Array(vec![
                BsonValue::Boolean(true),
                reversed,
                BsonValue::Null
            ]))
        );
        assert!(pulled.get_first("missing").is_none());
        assert_eq!(
            original.get_first("grid"),
            Some(&BsonValue::Array(vec![BsonValue::Array(vec![])]))
        );
    }

    #[test]
    fn array_membership_validates_operands_paths_ids_and_work_budgets() {
        for value in [
            BsonValue::Document(doc([("$each", BsonValue::Int32(1))])),
            BsonValue::Document(doc([("$sort", BsonValue::Int32(1))])),
            BsonValue::Document(doc([
                ("$each", BsonValue::Array(vec![])),
                ("extra", BsonValue::Null),
            ])),
        ] {
            assert_eq!(
                code(
                    DocumentUpdater::compile(&spec("$addToSet", doc([("v", value)]))).unwrap_err()
                ),
                2
            );
        }
        assert_eq!(
            code(
                DocumentUpdater::compile(&spec("$pullAll", doc([("v", BsonValue::Null)])))
                    .unwrap_err()
            ),
            2
        );
        let original = doc([
            (
                "_id",
                BsonValue::Document(doc([("v", BsonValue::Array(vec![BsonValue::Int32(1)]))])),
            ),
            ("scalar", BsonValue::Null),
            ("a", BsonValue::Array(vec![BsonValue::Null])),
        ]);
        for (operator, path, value, expected) in [
            ("$addToSet", "scalar.x", BsonValue::Null, 28),
            ("$addToSet", "a.0.x", BsonValue::Null, 28),
            ("$addToSet", "a.01", BsonValue::Null, 28),
            ("$addToSet", "scalar", BsonValue::Null, 2),
            ("$addToSet", "_id.v", BsonValue::Int32(2), 66),
            ("$pullAll", "scalar", BsonValue::Array(vec![]), 2),
            ("$pullAll", "scalar.x", BsonValue::Array(vec![]), 28),
            (
                "$pullAll",
                "_id.v",
                BsonValue::Array(vec![BsonValue::Int32(1)]),
                66,
            ),
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
        let no_id_change =
            DocumentUpdater::compile(&spec("$addToSet", doc([("_id.v", BsonValue::Double(1.0))])))
                .unwrap()
                .apply(&original)
                .unwrap();
        assert_eq!(
            encode_document(&no_id_change).unwrap(),
            encode_document(&original).unwrap()
        );
        let mut check = || Ok(());
        let mut budget = Budget {
            bytes: 0,
            comparison_bytes: MAX_COMPARISON_BYTES - 1,
            steps: 0,
            check: &mut check,
        };
        assert_eq!(
            contains_value(&[BsonValue::Int32(1)], &BsonValue::Int64(1), &mut budget)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        let mut budget = Budget {
            bytes: MAX_RETAINED_BYTES - 1,
            comparison_bytes: 0,
            steps: 0,
            check: &mut check,
        };
        let mut values = vec![];
        assert_eq!(
            add_unique(&mut values, &[BsonValue::Null], &mut budget)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        assert!(values.is_empty());
        let mut cancelled = || Err(EngineError::new(EngineErrorKind::Cancelled, "test"));
        let mut budget = Budget {
            bytes: 0,
            comparison_bytes: 0,
            steps: 0,
            check: &mut cancelled,
        };
        assert_eq!(
            contains_value(&[BsonValue::Int32(1)], &BsonValue::Int64(1), &mut budget)
                .unwrap_err()
                .kind(),
            EngineErrorKind::Cancelled
        );
    }

    #[test]
    fn pop_and_rename_preserve_order_missing_paths_and_error_atomicity() {
        let original = doc([
            ("_id", BsonValue::Int32(7)),
            (
                "from",
                BsonValue::Array(vec![BsonValue::Int64(1), BsonValue::Int64(2)]),
            ),
            ("to", BsonValue::Null),
            (
                "nested",
                BsonValue::Document(doc([("old", BsonValue::from("value"))])),
            ),
        ]);
        let update = doc([
            (
                "$pop",
                BsonValue::Document(doc([("from", BsonValue::Double(-1.0))])),
            ),
            (
                "$rename",
                BsonValue::Document(doc([("nested.old", BsonValue::from("to"))])),
            ),
        ]);
        let result = DocumentUpdater::compile(&update)
            .unwrap()
            .apply(&original)
            .unwrap();
        assert_eq!(
            encode_document(&result).unwrap(),
            encode_document(&doc([
                ("_id", BsonValue::Int32(7)),
                ("from", BsonValue::Array(vec![BsonValue::Int64(2)])),
                ("to", BsonValue::from("value")),
                ("nested", BsonValue::Document(BsonDocument::new())),
            ]))
            .unwrap()
        );
        for (operator, fields, expected) in [
            ("$pop", doc([("to", BsonValue::Int32(1))]), 14),
            (
                "$rename",
                doc([("nested.old", BsonValue::from("from.0"))]),
                2,
            ),
            (
                "$rename",
                doc([("nested.old", BsonValue::from("to.x"))]),
                28,
            ),
            ("$rename", doc([("_id", BsonValue::from("changed"))]), 66),
            ("$rename", doc([("to", BsonValue::from("_id"))]), 66),
        ] {
            let expression = doc([
                (
                    "$set",
                    BsonValue::Document(doc([("atomic_marker", BsonValue::Boolean(true))])),
                ),
                (operator, BsonValue::Document(fields)),
            ]);
            let before = encode_document(&original).unwrap();
            assert_eq!(
                code(
                    DocumentUpdater::compile(&expression)
                        .unwrap()
                        .apply(&original)
                        .unwrap_err()
                ),
                expected
            );
            assert_eq!(encode_document(&original).unwrap(), before);
        }
        let no_rename = DocumentUpdater::compile(&spec(
            "$rename",
            doc([("missing", BsonValue::from("from.0"))]),
        ))
        .unwrap()
        .apply(&original)
        .unwrap();
        assert_eq!(
            encode_document(&no_rename).unwrap(),
            encode_document(&original).unwrap()
        );
        let missing = DocumentUpdater::compile(&spec(
            "$pop",
            doc([("from.999999999999999999999999", BsonValue::Int32(1))]),
        ))
        .unwrap()
        .apply(&original)
        .unwrap();
        assert_eq!(
            encode_document(&missing).unwrap(),
            encode_document(&original).unwrap()
        );
    }

    #[test]
    fn pop_rename_validate_all_paths_and_budget_before_mutating() {
        for (expression, expected) in [
            (spec("$rename", doc([("a", BsonValue::from("b\0c"))])), 2),
            (spec("$rename", doc([("a", BsonValue::from("a.b"))])), 2),
            (spec("$rename", doc([("a", BsonValue::from("a"))])), 2),
            (spec("$rename", doc([("a", BsonValue::from("$x"))])), 2),
            (spec("$rename", doc([("a", BsonValue::from(""))])), 56),
            (
                spec(
                    "$rename",
                    doc([("a", BsonValue::from("b")), ("c", BsonValue::from("b"))]),
                ),
                40,
            ),
            (spec("$pop", doc([("a", BsonValue::Boolean(true))])), 9),
        ] {
            assert_eq!(
                code(DocumentUpdater::compile(&expression).unwrap_err()),
                expected
            );
        }
        assert_eq!(
            DocumentUpdater::compile(&spec(
                "$rename",
                doc([("a", BsonValue::from(vec!["b"; 101].join(".")))])
            ))
            .unwrap_err()
            .kind(),
            EngineErrorKind::LimitExceeded
        );
        let mut original = doc([("a", BsonValue::Array(vec![BsonValue::Int32(1); 1024]))]);
        let before = encode_document(&original).unwrap();
        let mut check = || Ok(());
        let mut budget = Budget {
            bytes: 0,
            comparison_bytes: 0,
            steps: MAX_STEPS - 20,
            check: &mut check,
        };
        assert_eq!(
            pop_document(&mut original, &["a".into()], true, &mut budget)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        assert_eq!(encode_document(&original).unwrap(), before);
        let mut visited = 0;
        let mut cancelled = || {
            visited += 1;
            if visited == 50 {
                Err(EngineError::new(EngineErrorKind::Cancelled, "test"))
            } else {
                Ok(())
            }
        };
        let mut budget = Budget {
            bytes: 0,
            comparison_bytes: 0,
            steps: 0,
            check: &mut cancelled,
        };
        assert_eq!(
            pop_document(&mut original, &["a".into()], false, &mut budget)
                .unwrap_err()
                .kind(),
            EngineErrorKind::Cancelled
        );
        assert_eq!(encode_document(&original).unwrap(), before);
    }

    #[test]
    fn min_max_compare_whole_values_and_preserve_equal_representations() {
        let original = doc([
            ("_id", BsonValue::Int64(5)),
            ("equal", BsonValue::Double(1.0)),
            ("array", BsonValue::Array(vec![BsonValue::Int32(2)])),
            ("slots", BsonValue::Array(vec![BsonValue::Null])),
            ("low", BsonValue::Null),
            ("high", BsonValue::Null),
        ]);
        let update = doc([
            (
                "$min",
                BsonValue::Document(doc([
                    ("_id", BsonValue::Int32(7)),
                    ("equal", BsonValue::Int32(1)),
                    (
                        "array",
                        BsonValue::Array(vec![BsonValue::Int32(1), BsonValue::Int32(99)]),
                    ),
                    ("slots.0", BsonValue::Int32(3)),
                    ("slots.3", BsonValue::Int32(3)),
                    ("low", BsonValue::Int32(1)),
                ])),
            ),
            (
                "$max",
                BsonValue::Document(doc([("high", BsonValue::Int32(1))])),
            ),
        ]);
        let updater = DocumentUpdater::compile(&update).unwrap();
        let result = updater.apply(&original).unwrap();
        assert!(matches!(result.get_first("_id"), Some(BsonValue::Int64(5))));
        assert!(matches!(
            result.get_first("equal"),
            Some(BsonValue::Double(1.0))
        ));
        assert_eq!(
            result.get_first("array"),
            Some(&BsonValue::Array(vec![
                BsonValue::Int32(1),
                BsonValue::Int32(99)
            ]))
        );
        assert_eq!(
            result.get_first("slots"),
            Some(&BsonValue::Array(vec![
                BsonValue::Null,
                BsonValue::Null,
                BsonValue::Null,
                BsonValue::Int32(3)
            ]))
        );
        assert_eq!(result.get_first("low"), Some(&BsonValue::Null));
        assert_eq!(result.get_first("high"), Some(&BsonValue::Int32(1)));
        assert_eq!(
            encode_document(&updater.apply(&result).unwrap()).unwrap(),
            encode_document(&result).unwrap()
        );
        for operator in ["$min", "$max"] {
            assert_eq!(
                code(
                    DocumentUpdater::compile(&spec(
                        operator,
                        doc([(
                            "_id",
                            BsonValue::Int32(if operator == "$min" { 4 } else { 6 })
                        )])
                    ))
                    .unwrap()
                    .apply(&original)
                    .unwrap_err()
                ),
                66
            );
            for path in ["equal.x", "slots.0.x", "slots.01", "slots.x"] {
                assert_eq!(
                    code(
                        DocumentUpdater::compile(&spec(
                            operator,
                            doc([(path, BsonValue::Int32(0))])
                        ))
                        .unwrap()
                        .apply(&original)
                        .unwrap_err()
                    ),
                    28
                );
            }
        }
    }

    #[test]
    fn comparison_noops_charge_work_and_observe_cancellation() {
        let value = BsonValue::Array(vec![BsonValue::Int32(1); 1024]);
        let action = Action::Min(value.clone());
        let mut check = || Ok(());
        let mut budget = Budget {
            bytes: 0,
            comparison_bytes: MAX_COMPARISON_BYTES - 1,
            steps: 0,
            check: &mut check,
        };
        assert_eq!(
            action.replaces(&value, &mut budget).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let mut budget = Budget {
            bytes: 0,
            comparison_bytes: 0,
            steps: MAX_STEPS - 1,
            check: &mut check,
        };
        assert_eq!(
            action.replaces(&value, &mut budget).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        let mut visited = 0;
        let mut cancelled = || {
            visited += 1;
            if visited == 500 {
                Err(EngineError::new(
                    EngineErrorKind::Cancelled,
                    "test cancellation",
                ))
            } else {
                Ok(())
            }
        };
        let mut budget = Budget {
            bytes: 0,
            comparison_bytes: 0,
            steps: 0,
            check: &mut cancelled,
        };
        assert_eq!(
            action.replaces(&value, &mut budget).unwrap_err().kind(),
            EngineErrorKind::Cancelled
        );
        assert_eq!(visited, 500);
        for operator in ["$min", "$max"] {
            let update = spec(
                operator,
                doc([("a.9999999999999999999999999", BsonValue::Int32(1))]),
            );
            assert_eq!(
                DocumentUpdater::compile(&update)
                    .unwrap()
                    .apply(&doc([("a", BsonValue::Array(vec![]))]))
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::LimitExceeded
            );
        }
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
