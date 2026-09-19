//! Bounded expressions shared by aggregation transforms and accumulators.

use super::{BsonDocument, BsonJavaScript, BsonValue, matcher::query_error, memory};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

pub(super) const MAX_BYTES: usize = 64 * 1024 * 1024;
pub(super) const MAX_DEPTH: usize = 100;
const MAX_NODES: usize = 4096;
const MAX_STEPS: usize = 1_000_000;

pub(super) enum Expression {
    Literal(BsonValue),
    Field(Vec<String>),
    Remove,
    Array(Vec<Self>),
    Object(Vec<(String, Self)>),
    IfNull(Vec<Self>),
    Size(Box<Self>),
}

/// Charges cumulative allocation work before copying, not just the final value.
/// This bounds discarded fallbacks and array-broadcast amplification as well.
pub(super) struct Budget<'a> {
    pub check: &'a mut dyn FnMut() -> EngineResult<()>,
    pub bytes: usize,
    pub nodes: usize,
    maximum: usize,
    steps: usize,
}

impl<'a> Budget<'a> {
    pub fn new(check: &'a mut dyn FnMut() -> EngineResult<()>) -> Self {
        Self {
            check,
            bytes: 0,
            nodes: 0,
            maximum: MAX_BYTES,
            steps: 0,
        }
    }

    pub fn with_maximum(mut self, maximum: usize) -> Self {
        self.maximum = maximum.min(MAX_BYTES);
        self
    }

    pub fn step(&mut self) -> EngineResult<()> {
        self.steps += 1;
        if self.steps > MAX_STEPS {
            return Err(limit());
        }
        (self.check)()
    }

    pub fn depth(&mut self, depth: usize) -> EngineResult<()> {
        self.step()?;
        if depth > MAX_DEPTH {
            return Err(limit());
        }
        Ok(())
    }

    pub fn node(&mut self, depth: usize) -> EngineResult<()> {
        self.depth(depth)?;
        self.nodes += 1;
        if self.nodes > MAX_NODES {
            return Err(limit());
        }
        self.charge(256)
    }

    pub fn charge(&mut self, bytes: usize) -> EngineResult<()> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= self.maximum)
            .ok_or_else(limit)?;
        Ok(())
    }

    pub fn field(&mut self, name: &str) -> EngineResult<String> {
        self.step()?;
        self.charge(128 + name.len())?;
        Ok(name.to_owned())
    }

    pub fn copy_document(
        &mut self,
        document: &BsonDocument,
        depth: usize,
    ) -> EngineResult<BsonDocument> {
        self.depth(depth)?;
        self.charge(128)?;
        let mut result = BsonDocument::new();
        for (name, value) in document.iter() {
            let name = self.field(name)?;
            let value = self.copy_value(value, depth + 1)?;
            result.push(name, value).expect("validated field name");
        }
        Ok(result)
    }

    pub fn copy_value(&mut self, value: &BsonValue, depth: usize) -> EngineResult<BsonValue> {
        self.step()?;
        match value {
            BsonValue::Document(document) => {
                Ok(BsonValue::Document(self.copy_document(document, depth)?))
            }
            BsonValue::Array(values) => {
                self.depth(depth)?;
                self.charge(128)?;
                let mut result = Vec::new();
                for value in values {
                    result.push(self.copy_value(value, depth + 1)?);
                }
                Ok(BsonValue::Array(result))
            }
            BsonValue::JavaScript(code) if code.scope().is_some() => {
                self.charge(128 + code.code().len())?;
                let scope = self.copy_document(code.scope().expect("scope"), depth + 1)?;
                Ok(BsonValue::JavaScript(BsonJavaScript::with_scope(
                    code.code(),
                    scope,
                )))
            }
            value => {
                let bytes =
                    memory::value_bytes(value, self.maximum - self.bytes, &mut || self.step())?;
                self.charge(bytes)?;
                Ok(value.clone())
            }
        }
    }
}

pub(super) fn limit() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "aggregation expression resource limit exceeded",
    )
}

pub(super) fn contains_operator(document: &BsonDocument) -> bool {
    document.iter().any(|(name, _)| name.starts_with('$'))
}

fn field_path(expression: &str, budget: &mut Budget<'_>) -> EngineResult<Vec<String>> {
    if expression.starts_with("$$") || !expression.starts_with('$') || expression.len() == 1 {
        return Err(query_error(115));
    }
    let path = &expression[1..];
    if path.ends_with('.') {
        return Err(query_error(40353));
    }
    let mut parts = Vec::new();
    for part in path.split('.') {
        budget.node(parts.len() + 1)?;
        if part.is_empty() {
            return Err(query_error(15998));
        }
        if part.starts_with('$')
            && !matches!(
                part,
                "$db"
                    | "$id"
                    | "$recordId"
                    | "$ref"
                    | "$searchRootDocumentId"
                    | "$searchScore"
                    | "$searchSortValues"
                    | "$sortKey"
            )
        {
            return Err(query_error(16410));
        }
        if part.contains('\0') {
            return Err(query_error(16411));
        }
        parts.push(budget.field(part)?);
    }
    Ok(parts)
}

impl Expression {
    pub fn compile(
        value: &BsonValue,
        allow_remove: bool,
        budget: &mut Budget<'_>,
        depth: usize,
    ) -> EngineResult<Self> {
        budget.node(depth)?;
        match value {
            BsonValue::String(value) if allow_remove && value == "$$REMOVE" => Ok(Self::Remove),
            BsonValue::String(value) if allow_remove && value.starts_with("$$REMOVE.") => {
                budget.charge(value.len() + 16)?;
                field_path(&format!("$placeholder.{}", &value[9..]), budget)?;
                Ok(Self::Remove)
            }
            BsonValue::String(value) if value.starts_with('$') => {
                Ok(Self::Field(field_path(value, budget)?))
            }
            BsonValue::Array(values) => {
                let mut result = Vec::new();
                for value in values {
                    result.push(Self::compile(value, allow_remove, budget, depth + 1)?);
                }
                Ok(Self::Array(result))
            }
            BsonValue::Document(document) if contains_operator(document) => {
                if document.len() != 1 {
                    return Err(query_error(2));
                }
                let (operator, operand) = document.iter().next().expect("one operator");
                match operator {
                    "$literal" => Ok(Self::Literal(budget.copy_value(operand, 1)?)),
                    "$ifNull" => {
                        let BsonValue::Array(values) = operand else {
                            return Err(query_error(1257300));
                        };
                        if values.len() < 2 {
                            return Err(query_error(1257300));
                        }
                        let mut result = Vec::new();
                        for value in values {
                            result.push(Self::compile(value, allow_remove, budget, depth + 1)?);
                        }
                        Ok(Self::IfNull(result))
                    }
                    "$size" => {
                        let operand = if let BsonValue::Array(values) = operand {
                            if values.len() != 1 {
                                return Err(query_error(16020));
                            }
                            &values[0]
                        } else {
                            operand
                        };
                        Ok(Self::Size(Box::new(Self::compile(
                            operand,
                            allow_remove,
                            budget,
                            depth + 1,
                        )?)))
                    }
                    _ => Err(query_error(115)),
                }
            }
            BsonValue::Document(document) => {
                let mut result = Vec::new();
                for (name, value) in document.iter() {
                    let name = budget.field(name)?;
                    result.push((name, Self::compile(value, allow_remove, budget, depth + 1)?));
                }
                Ok(Self::Object(result))
            }
            value => Ok(Self::Literal(budget.copy_value(value, 1)?)),
        }
    }

    pub fn evaluate(
        &self,
        document: &BsonDocument,
        budget: &mut Budget<'_>,
        depth: usize,
    ) -> EngineResult<Option<BsonValue>> {
        budget.depth(depth)?;
        match self {
            Self::Literal(value) => budget.copy_value(value, depth).map(Some),
            Self::Remove => Ok(None),
            Self::Field(parts) => resolve_document(document, parts, budget, depth),
            Self::Array(expressions) => {
                budget.charge(128)?;
                let mut values = Vec::new();
                for expression in expressions {
                    let value = expression.evaluate(document, budget, depth + 1)?;
                    if value.is_none() {
                        budget.charge(128)?;
                    }
                    values.push(value.unwrap_or(BsonValue::Null));
                }
                Ok(Some(BsonValue::Array(values)))
            }
            Self::Object(expressions) => {
                budget.charge(128)?;
                let mut result = BsonDocument::new();
                for (name, expression) in expressions {
                    if let Some(value) = expression.evaluate(document, budget, depth + 1)? {
                        result
                            .push(budget.field(name)?, value)
                            .expect("validated field");
                    }
                }
                Ok(Some(BsonValue::Document(result)))
            }
            Self::IfNull(expressions) => {
                for (index, expression) in expressions.iter().enumerate() {
                    let value = expression.evaluate(document, budget, depth + 1)?;
                    if index + 1 == expressions.len()
                        || !matches!(value, None | Some(BsonValue::Null))
                    {
                        return Ok(value);
                    }
                }
                unreachable!("validated nonempty ifNull")
            }
            Self::Size(expression) => {
                let Some(BsonValue::Array(values)) =
                    expression.evaluate(document, budget, depth + 1)?
                else {
                    return Err(query_error(17124));
                };
                let count = i32::try_from(values.len()).map_err(|_| limit())?;
                budget.charge(128)?;
                Ok(Some(BsonValue::Int32(count)))
            }
        }
    }
}

pub(super) fn lookup<'a>(
    document: &'a BsonDocument,
    name: &str,
    budget: &mut Budget<'_>,
) -> EngineResult<Option<&'a BsonValue>> {
    for (field, value) in document.iter() {
        budget.step()?;
        if field == name {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn resolve_document(
    document: &BsonDocument,
    parts: &[String],
    budget: &mut Budget<'_>,
    depth: usize,
) -> EngineResult<Option<BsonValue>> {
    budget.depth(depth)?;
    match lookup(document, &parts[0], budget)? {
        Some(value) => resolve_parts(value, &parts[1..], budget, depth + 1),
        None => Ok(None),
    }
}

fn resolve_parts(
    value: &BsonValue,
    parts: &[String],
    budget: &mut Budget<'_>,
    depth: usize,
) -> EngineResult<Option<BsonValue>> {
    budget.depth(depth)?;
    if parts.is_empty() {
        return budget.copy_value(value, 1).map(Some);
    }
    match value {
        BsonValue::Document(document) => resolve_document(document, parts, budget, depth),
        BsonValue::Array(values) => {
            budget.charge(128)?;
            let mut result = Vec::new();
            for value in values {
                budget.step()?;
                if let BsonValue::Document(document) = value {
                    if let Some(value) = resolve_document(document, parts, budget, depth + 1)? {
                        result.push(value);
                    }
                }
            }
            Ok(Some(BsonValue::Array(result)))
        }
        _ => Ok(None),
    }
}
