use std::{collections::HashSet, fmt};

use sqlparser::ast::{
    BinaryOperator, Expr, FromTable, Ident, ObjectName, ObjectNamePart, SetExpr,
    Statement as AstStatement, TableFactor, TableObject, TableWithJoins, UnaryOperator,
    Value as AstValue,
};
use sqlparser::tokenizer::Span;

use super::NormalizedSql;
use crate::core::{
    Catalog, EngineError, EngineErrorKind, EngineResult, LogicalDatabaseId, ShardKeyType, TableId,
    TableMetadata, TablePlacement, Value,
};

/// Result category for one statement's shard-key inference.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShardKeyInferenceKind {
    /// The statement does not reference a catalog table that needs routing.
    NotApplicable,
    /// The statement references a known global or catalog-placed table.
    NotSharded,
    /// The statement targets a sharded table but does not prove a finite key set.
    Unconstrained,
    /// The predicate proves that no non-null shard key can match.
    Contradiction,
    /// The supported analysis proves exactly one distinct shard-key value.
    Exact,
    /// The statement contains two or more distinct inferred shard-key values.
    Multiple,
}

/// One typed shard-key value inferred from SQL or a bound parameter.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ShardKeyValue {
    /// A signed 64-bit integer key.
    Int64(i64),
    /// A UTF-8 text key, without Unicode normalization.
    Text(String),
    /// An arbitrary binary key.
    Binary(Vec<u8>),
}

impl ShardKeyValue {
    /// Return this value's declared shard-key type.
    pub const fn key_type(&self) -> ShardKeyType {
        match self {
            Self::Int64(_) => ShardKeyType::Int64,
            Self::Text(_) => ShardKeyType::Text,
            Self::Binary(_) => ShardKeyType::Binary,
        }
    }

    /// Return the signed integer value when this is an `Int64` key.
    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int64(value) => Some(*value),
            Self::Text(_) | Self::Binary(_) => None,
        }
    }

    /// Return the text value when this is a text key.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Text(value) => Some(value),
            Self::Int64(_) | Self::Binary(_) => None,
        }
    }

    /// Return the bytes when this is a binary key.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Binary(value) => Some(value),
            Self::Int64(_) | Self::Text(_) => None,
        }
    }
}

impl fmt::Debug for ShardKeyValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int64(_) => formatter.write_str("Int64(<redacted>)"),
            Self::Text(value) => formatter
                .debug_struct("Text")
                .field("bytes", &value.len())
                .finish(),
            Self::Binary(value) => formatter
                .debug_struct("Binary")
                .field("bytes", &value.len())
                .finish(),
        }
    }
}

/// Owned shard-key inference for one normalized top-level statement.
///
/// This result describes values only. It does not encode or hash a key, choose
/// a physical shard, authorize a statement, or decide whether multiple or
/// unconstrained keys may execute.
#[derive(Clone, PartialEq, Eq)]
pub struct ShardKeyInference {
    table_id: Option<TableId>,
    key_type: Option<ShardKeyType>,
    kind: ShardKeyInferenceKind,
    values: Vec<ShardKeyValue>,
}

impl ShardKeyInference {
    /// Return the resolved catalog table, if this statement references one.
    pub const fn table_id(&self) -> Option<TableId> {
        self.table_id
    }

    /// Return the declared key type when the resolved table is sharded.
    pub const fn key_type(&self) -> Option<ShardKeyType> {
        self.key_type
    }

    /// Return the inference category.
    pub const fn kind(&self) -> ShardKeyInferenceKind {
        self.kind
    }

    /// Return inferred values in deterministic expression or INSERT-row order.
    ///
    /// Predicate results contain unique values. Complete INSERT results retain
    /// one value per row, including duplicates.
    pub fn values(&self) -> &[ShardKeyValue] {
        &self.values
    }
}

impl fmt::Debug for ShardKeyInference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShardKeyInference")
            .field("table_id", &self.table_id)
            .field("key_type", &self.key_type)
            .field("kind", &self.kind)
            .field("value_count", &self.values.len())
            .finish()
    }
}

/// Infer shard-key constraints and supplied values for one normalized statement.
///
/// `statement_index` is zero-based. `parameters` is the complete bound-value
/// slice for that statement and must match its normalized parameter count,
/// including unused gaps in numbered PostgreSQL or SQLite markers. Inference
/// is read-only and statement-local; later planning and policy layers decide
/// whether and where the statement can execute.
pub fn infer_shard_keys(
    catalog: &Catalog,
    database: LogicalDatabaseId,
    normalized: &NormalizedSql,
    statement_index: usize,
    parameters: &[Value],
) -> EngineResult<ShardKeyInference> {
    let Some(statement) = normalized.common().statements().get(statement_index) else {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "SQL statement index is outside the normalized batch",
        ));
    };
    let Some(layout) = normalized.statement_parameters().get(statement_index) else {
        return Err(inference_invariant());
    };
    if parameters.len() != layout.parameter_count() {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            format!(
                "statement {} requires exactly {} bound parameters for shard-key inference",
                statement_index + 1,
                layout.parameter_count()
            ),
        ));
    }
    let Some(database) = catalog.database_by_id(database) else {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "selected logical database does not exist",
        ));
    };

    let context = InferenceContext {
        catalog,
        database_name: database.name(),
        normalized,
        statement_index,
        parameters,
    };
    infer_statement(statement, &context)
}

struct InferenceContext<'a> {
    catalog: &'a Catalog,
    database_name: &'a str,
    normalized: &'a NormalizedSql,
    statement_index: usize,
    parameters: &'a [Value],
}

fn infer_statement(
    statement: &AstStatement,
    context: &InferenceContext<'_>,
) -> EngineResult<ShardKeyInference> {
    match statement {
        AstStatement::Query(query) => {
            let SetExpr::Select(select) = query.body.as_ref() else {
                return Err(inference_invariant());
            };
            let Some(table) = select.from.first() else {
                return Ok(not_applicable());
            };
            if select.from.len() != 1 {
                return Err(inference_invariant());
            }
            infer_predicate_statement(table, select.selection.as_ref(), context)
        }
        AstStatement::Update(update) => {
            infer_predicate_statement(&update.table, update.selection.as_ref(), context)
        }
        AstStatement::Delete(delete) => {
            let FromTable::WithFromKeyword(tables) = &delete.from else {
                return Err(inference_invariant());
            };
            let [table] = tables.as_slice() else {
                return Err(inference_invariant());
            };
            infer_predicate_statement(table, delete.selection.as_ref(), context)
        }
        AstStatement::Insert(insert) => infer_insert(insert, context),
        AstStatement::CreateTable(_)
        | AstStatement::CreateIndex(_)
        | AstStatement::StartTransaction { .. }
        | AstStatement::Commit { .. }
        | AstStatement::Rollback { .. } => Ok(not_applicable()),
        _ => Err(inference_invariant()),
    }
}

fn infer_predicate_statement(
    table: &TableWithJoins,
    predicate: Option<&Expr>,
    context: &InferenceContext<'_>,
) -> EngineResult<ShardKeyInference> {
    let binding = table_binding(table)?;
    let table = resolve_table(&binding, context)?;
    let shard_key = match table.placement() {
        TablePlacement::Sharded(shard_key) => shard_key,
        TablePlacement::Global | TablePlacement::Catalog => {
            return Ok(not_sharded(table.id()));
        }
    };

    let Some(predicate) = predicate else {
        return Ok(unconstrained(table.id(), shard_key.key_type()));
    };
    let target = PredicateTarget {
        qualifier: binding.qualifier,
        column: shard_key.column(),
        key_type: shard_key.key_type(),
    };
    let domain = infer_predicate(predicate, &target, context)?;
    Ok(finish_predicate(table.id(), shard_key.key_type(), domain))
}

fn infer_insert(
    insert: &sqlparser::ast::Insert,
    context: &InferenceContext<'_>,
) -> EngineResult<ShardKeyInference> {
    let TableObject::TableName(table_name) = &insert.table else {
        return Err(inference_invariant());
    };
    let binding = insert_binding(table_name)?;
    let table = resolve_table(&binding, context)?;
    let shard_key = match table.placement() {
        TablePlacement::Sharded(shard_key) => shard_key,
        TablePlacement::Global | TablePlacement::Catalog => {
            return Ok(not_sharded(table.id()));
        }
    };

    let mut column_index = None;
    for (index, column) in insert.columns.iter().enumerate() {
        if identifier_matches_catalog(column_ident(column)?, shard_key.column()) {
            column_index = Some(index);
            break;
        }
    }
    let Some(column_index) = column_index else {
        return Ok(unconstrained(table.id(), shard_key.key_type()));
    };
    let Some(source) = &insert.source else {
        return Err(inference_invariant());
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Err(inference_invariant());
    };

    let mut inferred = Vec::with_capacity(values.rows.len());
    let mut complete = true;
    for row in &values.rows {
        let Some(expression) = row.get(column_index) else {
            return Err(inference_invariant());
        };
        match infer_atom(expression, shard_key.key_type(), context)? {
            InferredAtom::Value(value) => inferred.push(value),
            InferredAtom::Null => {
                return Err(EngineError::new(
                    EngineErrorKind::NotNullViolation,
                    format!(
                        "statement {} supplies NULL for its non-null shard key",
                        context.statement_index + 1
                    ),
                ));
            }
            InferredAtom::Unresolved => {
                complete = false;
            }
        }
    }

    if !complete {
        return Ok(unconstrained(table.id(), shard_key.key_type()));
    }
    if inferred.is_empty() {
        return Err(inference_invariant());
    }
    let kind = if distinct_value_count(&inferred) == 1 {
        ShardKeyInferenceKind::Exact
    } else {
        ShardKeyInferenceKind::Multiple
    };
    Ok(ShardKeyInference {
        table_id: Some(table.id()),
        key_type: Some(shard_key.key_type()),
        kind,
        values: inferred,
    })
}

struct TableBinding {
    catalog_name: String,
    qualifier: String,
}

fn table_binding(table: &TableWithJoins) -> EngineResult<TableBinding> {
    if !table.joins.is_empty() {
        return Err(inference_invariant());
    }
    let TableFactor::Table { name, alias, .. } = &table.relation else {
        return Err(inference_invariant());
    };
    let source_name = object_identifier(name)?;
    let Some(catalog_name) = catalog_identifier(source_name) else {
        return Err(unknown_table());
    };
    let qualifier = alias.as_ref().map_or_else(
        || reference_identifier(source_name),
        |alias| reference_identifier(&alias.name),
    );
    Ok(TableBinding {
        catalog_name,
        qualifier,
    })
}

fn insert_binding(name: &ObjectName) -> EngineResult<TableBinding> {
    let source_name = object_identifier(name)?;
    let Some(catalog_name) = catalog_identifier(source_name) else {
        return Err(unknown_table());
    };
    Ok(TableBinding {
        qualifier: reference_identifier(source_name),
        catalog_name,
    })
}

fn object_identifier(name: &ObjectName) -> EngineResult<&Ident> {
    let [ObjectNamePart::Identifier(identifier)] = name.0.as_slice() else {
        return Err(inference_invariant());
    };
    Ok(identifier)
}

fn column_ident(name: &ObjectName) -> EngineResult<&Ident> {
    let [ObjectNamePart::Identifier(identifier)] = name.0.as_slice() else {
        return Err(inference_invariant());
    };
    Ok(identifier)
}

fn catalog_identifier(identifier: &Ident) -> Option<String> {
    let value = if identifier.quote_style.is_none() {
        identifier.value.to_ascii_lowercase()
    } else {
        identifier.value.clone()
    };
    crate::core::validate_catalog_identifier(&value).then_some(value)
}

fn reference_identifier(identifier: &Ident) -> String {
    if identifier.quote_style.is_none() {
        identifier.value.to_ascii_lowercase()
    } else {
        identifier.value.clone()
    }
}

fn identifier_matches_catalog(identifier: &Ident, canonical: &str) -> bool {
    if identifier.quote_style.is_none() {
        identifier.value.eq_ignore_ascii_case(canonical)
    } else {
        identifier.value == canonical
    }
}

fn resolve_table<'a>(
    binding: &TableBinding,
    context: &'a InferenceContext<'_>,
) -> EngineResult<&'a TableMetadata> {
    context
        .catalog
        .table(context.database_name, &binding.catalog_name)?
        .ok_or_else(unknown_table)
}

fn unknown_table() -> EngineError {
    EngineError::new(
        EngineErrorKind::InvalidQuery,
        "SQL statement references a table absent from the selected logical database",
    )
}

struct PredicateTarget<'a> {
    qualifier: String,
    column: &'a str,
    key_type: ShardKeyType,
}

enum KeyDomain {
    Any,
    Finite(Vec<ShardKeyValue>),
}

fn infer_predicate(
    expression: &Expr,
    target: &PredicateTarget<'_>,
    context: &InferenceContext<'_>,
) -> EngineResult<KeyDomain> {
    let expression = peel_nested(expression);
    match expression {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if is_shard_column(left, target) {
                return atom_domain(right, target.key_type, context);
            }
            if is_shard_column(right, target) {
                return atom_domain(left, target.key_type, context);
            }
            Ok(KeyDomain::Any)
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => Ok(intersect_domains(
            infer_predicate(left, target, context)?,
            infer_predicate(right, target, context)?,
        )),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => Ok(union_domains(
            infer_predicate(left, target, context)?,
            infer_predicate(right, target, context)?,
        )),
        _ => Ok(KeyDomain::Any),
    }
}

fn atom_domain(
    expression: &Expr,
    key_type: ShardKeyType,
    context: &InferenceContext<'_>,
) -> EngineResult<KeyDomain> {
    if matches!(key_type, ShardKeyType::Text) {
        return text_atom_domain(expression, context);
    }
    Ok(match infer_atom(expression, key_type, context)? {
        InferredAtom::Value(value) => KeyDomain::Finite(vec![value]),
        InferredAtom::Null => KeyDomain::Finite(Vec::new()),
        InferredAtom::Unresolved => KeyDomain::Any,
    })
}

fn text_atom_domain(expression: &Expr, context: &InferenceContext<'_>) -> EngineResult<KeyDomain> {
    let Expr::Value(value) = peel_nested(expression) else {
        return Ok(KeyDomain::Any);
    };
    match &value.value {
        AstValue::Placeholder(_) => match bound_parameter(value.span, context)? {
            Value::Null => Ok(KeyDomain::Finite(Vec::new())),
            Value::Text(_) => Ok(KeyDomain::Any),
            Value::InvalidText(_) => Err(EngineError::new(
                EngineErrorKind::InvalidTextEncoding,
                format!(
                    "statement {} binds non-UTF-8 text to its shard key",
                    context.statement_index + 1
                ),
            )),
            _ => Err(type_mismatch(context)),
        },
        AstValue::Null => Ok(KeyDomain::Finite(Vec::new())),
        AstValue::SingleQuotedString(_) => Ok(KeyDomain::Any),
        AstValue::Number(_, false) | AstValue::Boolean(_) => Err(type_mismatch(context)),
        _ => Err(inference_invariant()),
    }
}

fn is_shard_column(expression: &Expr, target: &PredicateTarget<'_>) -> bool {
    match peel_nested(expression) {
        Expr::Identifier(column) => identifier_matches_catalog(column, target.column),
        Expr::CompoundIdentifier(parts) => {
            let [qualifier, column] = parts.as_slice() else {
                return false;
            };
            reference_identifier(qualifier) == target.qualifier
                && identifier_matches_catalog(column, target.column)
        }
        _ => false,
    }
}

fn intersect_domains(left: KeyDomain, right: KeyDomain) -> KeyDomain {
    match (left, right) {
        (KeyDomain::Any, domain) | (domain, KeyDomain::Any) => domain,
        (KeyDomain::Finite(left), KeyDomain::Finite(right)) => KeyDomain::Finite(
            left.into_iter()
                .filter(|value| right.contains(value))
                .collect(),
        ),
    }
}

fn union_domains(left: KeyDomain, right: KeyDomain) -> KeyDomain {
    match (left, right) {
        (KeyDomain::Any, _) | (_, KeyDomain::Any) => KeyDomain::Any,
        (KeyDomain::Finite(mut left), KeyDomain::Finite(right)) => {
            for value in right {
                if !left.contains(&value) {
                    left.push(value);
                }
            }
            KeyDomain::Finite(left)
        }
    }
}

enum InferredAtom {
    Unresolved,
    Null,
    Value(ShardKeyValue),
}

fn infer_atom(
    expression: &Expr,
    key_type: ShardKeyType,
    context: &InferenceContext<'_>,
) -> EngineResult<InferredAtom> {
    let expression = peel_nested(expression);
    if matches!(key_type, ShardKeyType::Int64)
        && matches!(expression, Expr::UnaryOp { .. } | Expr::Value(_))
    {
        match signed_integer_literal(expression) {
            IntegerLiteral::Value(value) => {
                return Ok(InferredAtom::Value(ShardKeyValue::Int64(value)));
            }
            IntegerLiteral::OutOfRange => return Err(numeric_out_of_range(context)),
            IntegerLiteral::NonIntegral => return Err(type_mismatch(context)),
            IntegerLiteral::NotNumeric => {}
        }
    }

    let Expr::Value(value) = expression else {
        return Ok(InferredAtom::Unresolved);
    };
    match &value.value {
        AstValue::Placeholder(_) => {
            bound_atom(bound_parameter(value.span, context)?, key_type, context)
        }
        AstValue::Null => Ok(InferredAtom::Null),
        AstValue::SingleQuotedString(value) if matches!(key_type, ShardKeyType::Text) => {
            Ok(InferredAtom::Value(ShardKeyValue::Text(value.clone())))
        }
        AstValue::Number(_, false) | AstValue::SingleQuotedString(_) | AstValue::Boolean(_) => {
            Err(type_mismatch(context))
        }
        _ => Err(inference_invariant()),
    }
}

fn bound_parameter<'a>(span: Span, context: &'a InferenceContext<'_>) -> EngineResult<&'a Value> {
    let Some(index) = context
        .normalized
        .parameter_index(context.statement_index, span)
    else {
        return Err(inference_invariant());
    };
    index
        .checked_sub(1)
        .and_then(|index| context.parameters.get(index))
        .ok_or_else(inference_invariant)
}

fn bound_atom(
    value: &Value,
    key_type: ShardKeyType,
    context: &InferenceContext<'_>,
) -> EngineResult<InferredAtom> {
    if matches!(value, Value::Null) {
        return Ok(InferredAtom::Null);
    }
    let inferred = match (key_type, value) {
        (ShardKeyType::Int64, Value::Int64(value)) => ShardKeyValue::Int64(*value),
        (ShardKeyType::Int64, Value::UInt64(value)) => {
            let value = i64::try_from(*value).map_err(|_| numeric_out_of_range(context))?;
            ShardKeyValue::Int64(value)
        }
        (ShardKeyType::Text, Value::Text(value)) => ShardKeyValue::Text(value.clone()),
        (ShardKeyType::Text, Value::InvalidText(_)) => {
            return Err(EngineError::new(
                EngineErrorKind::InvalidTextEncoding,
                format!(
                    "statement {} binds non-UTF-8 text to its shard key",
                    context.statement_index + 1
                ),
            ));
        }
        (ShardKeyType::Binary, Value::Binary(value)) => ShardKeyValue::Binary(value.clone()),
        _ => return Err(type_mismatch(context)),
    };
    Ok(InferredAtom::Value(inferred))
}

enum IntegerLiteral {
    NotNumeric,
    NonIntegral,
    OutOfRange,
    Value(i64),
}

fn signed_integer_literal(expression: &Expr) -> IntegerLiteral {
    let mut expression = expression;
    let mut negative = false;
    loop {
        match peel_nested(expression) {
            Expr::UnaryOp {
                op: UnaryOperator::Plus,
                expr,
            } => expression = expr,
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => {
                negative = !negative;
                expression = expr;
            }
            Expr::Value(value) => {
                let AstValue::Number(number, false) = &value.value else {
                    return IntegerLiteral::NotNumeric;
                };
                if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
                    return IntegerLiteral::NonIntegral;
                }
                let limit = if negative {
                    i64::MAX as u64 + 1
                } else {
                    i64::MAX as u64
                };
                let mut magnitude = 0_u64;
                for byte in number.bytes() {
                    magnitude = match magnitude
                        .checked_mul(10)
                        .and_then(|value| value.checked_add(u64::from(byte - b'0')))
                    {
                        Some(value) if value <= limit => value,
                        Some(_) | None => return IntegerLiteral::OutOfRange,
                    };
                }
                if negative && magnitude == i64::MAX as u64 + 1 {
                    return IntegerLiteral::Value(i64::MIN);
                }
                let value = magnitude as i64;
                return IntegerLiteral::Value(if negative { -value } else { value });
            }
            _ => return IntegerLiteral::NotNumeric,
        }
    }
}

fn peel_nested(mut expression: &Expr) -> &Expr {
    while let Expr::Nested(inner) = expression {
        expression = inner;
    }
    expression
}

fn finish_predicate(
    table_id: TableId,
    key_type: ShardKeyType,
    domain: KeyDomain,
) -> ShardKeyInference {
    match domain {
        KeyDomain::Any => unconstrained(table_id, key_type),
        KeyDomain::Finite(values) if values.is_empty() => ShardKeyInference {
            table_id: Some(table_id),
            key_type: Some(key_type),
            kind: ShardKeyInferenceKind::Contradiction,
            values,
        },
        KeyDomain::Finite(values) if values.len() == 1 => ShardKeyInference {
            table_id: Some(table_id),
            key_type: Some(key_type),
            kind: ShardKeyInferenceKind::Exact,
            values,
        },
        KeyDomain::Finite(values) => ShardKeyInference {
            table_id: Some(table_id),
            key_type: Some(key_type),
            kind: ShardKeyInferenceKind::Multiple,
            values,
        },
    }
}

fn not_applicable() -> ShardKeyInference {
    ShardKeyInference {
        table_id: None,
        key_type: None,
        kind: ShardKeyInferenceKind::NotApplicable,
        values: Vec::new(),
    }
}

fn not_sharded(table_id: TableId) -> ShardKeyInference {
    ShardKeyInference {
        table_id: Some(table_id),
        key_type: None,
        kind: ShardKeyInferenceKind::NotSharded,
        values: Vec::new(),
    }
}

fn unconstrained(table_id: TableId, key_type: ShardKeyType) -> ShardKeyInference {
    ShardKeyInference {
        table_id: Some(table_id),
        key_type: Some(key_type),
        kind: ShardKeyInferenceKind::Unconstrained,
        values: Vec::new(),
    }
}

fn distinct_value_count(values: &[ShardKeyValue]) -> usize {
    values.iter().collect::<HashSet<_>>().len()
}

fn type_mismatch(context: &InferenceContext<'_>) -> EngineError {
    EngineError::new(
        EngineErrorKind::TypeMismatch,
        format!(
            "statement {} has a shard-key value incompatible with its catalog type",
            context.statement_index + 1
        ),
    )
}

fn numeric_out_of_range(context: &InferenceContext<'_>) -> EngineError {
    EngineError::new(
        EngineErrorKind::NumericOutOfRange,
        format!(
            "statement {} has a shard-key integer outside the signed 64-bit range",
            context.statement_index + 1
        ),
    )
}

fn inference_invariant() -> EngineError {
    EngineError::new(
        EngineErrorKind::Internal,
        "normalized SQL metadata is inconsistent during shard-key inference",
    )
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use super::*;
    use crate::{
        core::{LogicalDatabaseMetadata, ShardKeyMetadata},
        sql::{SqlDialect, normalize_placeholders, parse, validate_common_subset},
    };

    const DEFAULT_DATABASE: u64 = 1;
    const TENANT_DATABASE: u64 = 9;
    const BLOBS_TABLE: u64 = 2;
    const COUNTRIES_TABLE: u64 = 3;
    const EVENTS_TABLE: u64 = 4;
    const INTERNAL_CATALOG_TABLE: u64 = 5;
    const ACCOUNTS_TABLE: u64 = 30;

    fn sample_catalog() -> Catalog {
        Catalog::from_validated_parts(
            1,
            7,
            DEFAULT_DATABASE,
            vec![
                LogicalDatabaseMetadata::from_validated(DEFAULT_DATABASE, "default".to_owned()),
                LogicalDatabaseMetadata::from_validated(TENANT_DATABASE, "tenant".to_owned()),
            ]
            .into_boxed_slice(),
            vec![
                TableMetadata::from_validated(
                    BLOBS_TABLE,
                    DEFAULT_DATABASE,
                    "blobs".to_owned(),
                    TablePlacement::Sharded(ShardKeyMetadata::from_validated(
                        "tenant_id".to_owned(),
                        ShardKeyType::Binary,
                    )),
                ),
                TableMetadata::from_validated(
                    COUNTRIES_TABLE,
                    DEFAULT_DATABASE,
                    "countries".to_owned(),
                    TablePlacement::Global,
                ),
                TableMetadata::from_validated(
                    EVENTS_TABLE,
                    DEFAULT_DATABASE,
                    "events".to_owned(),
                    TablePlacement::Sharded(ShardKeyMetadata::from_validated(
                        "tenant_id".to_owned(),
                        ShardKeyType::Int64,
                    )),
                ),
                TableMetadata::from_validated(
                    INTERNAL_CATALOG_TABLE,
                    DEFAULT_DATABASE,
                    "internal_catalog".to_owned(),
                    TablePlacement::Catalog,
                ),
                TableMetadata::from_validated(
                    ACCOUNTS_TABLE,
                    TENANT_DATABASE,
                    "accounts".to_owned(),
                    TablePlacement::Sharded(ShardKeyMetadata::from_validated(
                        "tenant_id".to_owned(),
                        ShardKeyType::Text,
                    )),
                ),
            ]
            .into_boxed_slice(),
        )
    }

    fn normalize(dialect: SqlDialect, source: &str) -> NormalizedSql {
        let parsed = parse(dialect, source.to_owned()).unwrap();
        let common = validate_common_subset(parsed).unwrap();
        normalize_placeholders(common).unwrap()
    }

    fn infer(
        catalog: &Catalog,
        database: u64,
        dialect: SqlDialect,
        source: &str,
        parameters: &[Value],
    ) -> EngineResult<ShardKeyInference> {
        let normalized = normalize(dialect, source);
        infer_shard_keys(
            catalog,
            LogicalDatabaseId::new(database).unwrap(),
            &normalized,
            0,
            parameters,
        )
    }

    fn assert_result(
        inference: &ShardKeyInference,
        table_id: Option<u64>,
        key_type: Option<ShardKeyType>,
        kind: ShardKeyInferenceKind,
        values: &[ShardKeyValue],
    ) {
        assert_eq!(inference.table_id().map(TableId::get), table_id);
        assert_eq!(inference.key_type(), key_type);
        assert_eq!(inference.kind(), kind);
        assert_eq!(inference.values(), values);
    }

    fn assert_kind(error: EngineError, expected: EngineErrorKind) {
        assert_eq!(error.kind(), expected, "{error:?}");
    }

    #[test]
    fn public_values_and_results_are_owned_cloneable_and_redacted() {
        fn assert_public_shape<T: Clone + Send + Sync + 'static>() {}
        assert_public_shape::<ShardKeyInference>();
        assert_public_shape::<ShardKeyInferenceKind>();
        assert_public_shape::<ShardKeyValue>();

        let integer = ShardKeyValue::Int64(42);
        let text = ShardKeyValue::Text("private-tenant".to_owned());
        let binary = ShardKeyValue::Binary(vec![0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(integer.key_type(), ShardKeyType::Int64);
        assert_eq!(integer.as_i64(), Some(42));
        assert_eq!(integer.as_str(), None);
        assert_eq!(integer.as_bytes(), None);
        assert_eq!(text.key_type(), ShardKeyType::Text);
        assert_eq!(text.as_str(), Some("private-tenant"));
        assert_eq!(binary.key_type(), ShardKeyType::Binary);
        assert_eq!(binary.as_bytes(), Some(&[0xde, 0xad, 0xbe, 0xef][..]));

        let debug = format!("{integer:?} {text:?} {binary:?}");
        assert!(!debug.contains("42"));
        assert!(!debug.contains("private-tenant"));
        assert!(!debug.contains("deadbeef"));

        let catalog = sample_catalog();
        let result = infer(
            &catalog,
            TENANT_DATABASE,
            SqlDialect::PostgreSql,
            "INSERT INTO accounts (tenant_id) VALUES ('private-tenant')",
            &[],
        )
        .unwrap();
        let cloned = result.clone();
        assert_eq!(cloned, result);
        let debug = format!("{result:?}");
        assert!(!debug.contains("private-tenant"));
        assert!(debug.contains("value_count: 1"));
    }

    #[test]
    fn marker_spans_select_the_correct_statement_local_bound_value() {
        let catalog = sample_catalog();
        let postgres = infer(
            &catalog,
            DEFAULT_DATABASE,
            SqlDialect::PostgreSql,
            "SELECT $1 FROM events WHERE tenant_id = $2",
            &[Value::Text("projection".to_owned()), Value::Int64(72)],
        )
        .unwrap();
        assert_result(
            &postgres,
            Some(EVENTS_TABLE),
            Some(ShardKeyType::Int64),
            ShardKeyInferenceKind::Exact,
            &[ShardKeyValue::Int64(72)],
        );

        let mysql = infer(
            &catalog,
            DEFAULT_DATABASE,
            SqlDialect::MySql,
            "UPDATE events SET payload = ? WHERE tenant_id = ?",
            &[Value::Text("assignment".to_owned()), Value::Int64(81)],
        )
        .unwrap();
        assert_eq!(mysql.values(), &[ShardKeyValue::Int64(81)]);

        let sqlite = infer(
            &catalog,
            DEFAULT_DATABASE,
            SqlDialect::Sqlite,
            "SELECT ?2 FROM events WHERE tenant_id = ?1",
            &[Value::Int64(93), Value::Text("projection".to_owned())],
        )
        .unwrap();
        assert_eq!(sqlite.values(), &[ShardKeyValue::Int64(93)]);
    }

    #[test]
    fn predicate_domain_algebra_is_conservative_and_deterministic() {
        let catalog = sample_catalog();
        let cases = [
            (
                "tenant_id = 1 AND payload = 9",
                ShardKeyInferenceKind::Exact,
                vec![ShardKeyValue::Int64(1)],
            ),
            (
                "tenant_id = 1 OR payload = 9",
                ShardKeyInferenceKind::Unconstrained,
                vec![],
            ),
            (
                "tenant_id = 1 OR tenant_id = 2",
                ShardKeyInferenceKind::Multiple,
                vec![ShardKeyValue::Int64(1), ShardKeyValue::Int64(2)],
            ),
            (
                "tenant_id = 1 AND tenant_id = 2",
                ShardKeyInferenceKind::Contradiction,
                vec![],
            ),
            (
                "tenant_id = 1 OR tenant_id = 1",
                ShardKeyInferenceKind::Exact,
                vec![ShardKeyValue::Int64(1)],
            ),
            (
                "(1 = (tenant_id)) AND (tenant_id = 1 OR tenant_id = 3)",
                ShardKeyInferenceKind::Exact,
                vec![ShardKeyValue::Int64(1)],
            ),
            (
                "tenant_id = NULL OR tenant_id = 7",
                ShardKeyInferenceKind::Exact,
                vec![ShardKeyValue::Int64(7)],
            ),
            (
                "tenant_id = NULL AND payload = 9",
                ShardKeyInferenceKind::Contradiction,
                vec![],
            ),
        ];

        for (predicate, kind, values) in cases {
            let source = format!("SELECT * FROM events WHERE {predicate}");
            let result =
                infer(&catalog, DEFAULT_DATABASE, SqlDialect::Sqlite, &source, &[]).unwrap();
            assert_result(
                &result,
                Some(EVENTS_TABLE),
                Some(ShardKeyType::Int64),
                kind,
                &values,
            );
        }
    }

    #[test]
    fn bound_predicate_values_are_compared_after_binding() {
        let catalog = sample_catalog();
        for (operator, right, kind, values) in [
            (
                "AND",
                7,
                ShardKeyInferenceKind::Exact,
                vec![ShardKeyValue::Int64(7)],
            ),
            ("AND", 8, ShardKeyInferenceKind::Contradiction, vec![]),
            (
                "OR",
                7,
                ShardKeyInferenceKind::Exact,
                vec![ShardKeyValue::Int64(7)],
            ),
            (
                "OR",
                8,
                ShardKeyInferenceKind::Multiple,
                vec![ShardKeyValue::Int64(7), ShardKeyValue::Int64(8)],
            ),
        ] {
            let source =
                format!("SELECT * FROM events WHERE tenant_id = $1 {operator} tenant_id = $2");
            let result = infer(
                &catalog,
                DEFAULT_DATABASE,
                SqlDialect::PostgreSql,
                &source,
                &[Value::Int64(7), Value::Int64(right)],
            )
            .unwrap();
            assert_eq!(result.kind(), kind);
            assert_eq!(result.values(), values);
        }
    }

    #[test]
    fn direct_key_atom_errors_are_independent_of_logical_branch_order() {
        let catalog = sample_catalog();
        for predicate in [
            "payload = 1 OR tenant_id = 9223372036854775808",
            "tenant_id = 9223372036854775808 OR payload = 1",
            "payload = 1 AND tenant_id = 9223372036854775808",
            "tenant_id = 9223372036854775808 AND payload = 1",
        ] {
            let source = format!("SELECT * FROM events WHERE {predicate}");
            let error =
                infer(&catalog, DEFAULT_DATABASE, SqlDialect::Sqlite, &source, &[]).unwrap_err();
            assert_kind(error, EngineErrorKind::NumericOutOfRange);
        }
    }

    #[test]
    fn unsupported_predicate_shapes_do_not_claim_a_finite_key_set() {
        let catalog = sample_catalog();
        for predicate in [
            "tenant_id > 1",
            "tenant_id IN (1, 2)",
            "tenant_id BETWEEN 1 AND 2",
            "NOT tenant_id = 1",
            "tenant_id + 1 = 2",
            "CASE WHEN tenant_id = 1 THEN 1 ELSE 0 END = 1",
            "tenant_id IS NULL",
        ] {
            let source = format!("SELECT * FROM events WHERE {predicate}");
            let result =
                infer(&catalog, DEFAULT_DATABASE, SqlDialect::Sqlite, &source, &[]).unwrap();
            assert_eq!(result.kind(), ShardKeyInferenceKind::Unconstrained);
            assert!(result.values().is_empty());
        }
    }

    #[test]
    fn aliases_qualifiers_and_identifier_quoting_follow_sql_reference_rules() {
        let catalog = sample_catalog();
        for (source, kind) in [
            (
                "SELECT * FROM events e WHERE e.tenant_id = 4",
                ShardKeyInferenceKind::Exact,
            ),
            (
                "SELECT * FROM events e WHERE events.tenant_id = 4",
                ShardKeyInferenceKind::Unconstrained,
            ),
            (
                "SELECT * FROM events WHERE events.tenant_id = 4",
                ShardKeyInferenceKind::Exact,
            ),
            (
                "SELECT * FROM events e WHERE other.tenant_id = 4",
                ShardKeyInferenceKind::Unconstrained,
            ),
            (
                "SELECT * FROM EVENTS WHERE TENANT_ID = 4",
                ShardKeyInferenceKind::Exact,
            ),
            (
                "SELECT * FROM \"events\" WHERE \"tenant_id\" = 4",
                ShardKeyInferenceKind::Exact,
            ),
            (
                "SELECT * FROM events \"E\" WHERE \"E\".tenant_id = 4",
                ShardKeyInferenceKind::Exact,
            ),
            (
                "SELECT * FROM events \"E\" WHERE e.tenant_id = 4",
                ShardKeyInferenceKind::Unconstrained,
            ),
            (
                "SELECT * FROM events WHERE \"TENANT_ID\" = 4",
                ShardKeyInferenceKind::Unconstrained,
            ),
        ] {
            let result = infer(
                &catalog,
                DEFAULT_DATABASE,
                SqlDialect::PostgreSql,
                source,
                &[],
            )
            .unwrap();
            assert_eq!(result.kind(), kind, "{source}");
        }

        let error = infer(
            &catalog,
            DEFAULT_DATABASE,
            SqlDialect::PostgreSql,
            "SELECT * FROM \"EVENTS\" WHERE tenant_id = 4",
            &[],
        )
        .unwrap_err();
        assert_kind(error, EngineErrorKind::InvalidQuery);
    }

    #[test]
    fn select_update_and_delete_only_consider_their_where_predicate() {
        let catalog = sample_catalog();
        for source in [
            "SELECT tenant_id FROM events WHERE 8 = tenant_id ORDER BY tenant_id LIMIT 1",
            "UPDATE events SET tenant_id = 99, payload = 8 WHERE tenant_id = 8",
            "DELETE FROM events WHERE (tenant_id) = (8)",
        ] {
            let result =
                infer(&catalog, DEFAULT_DATABASE, SqlDialect::Sqlite, source, &[]).unwrap();
            assert_eq!(result.kind(), ShardKeyInferenceKind::Exact, "{source}");
            assert_eq!(result.values(), &[ShardKeyValue::Int64(8)]);
        }

        for source in [
            "SELECT tenant_id = 8 FROM events",
            "SELECT payload FROM events GROUP BY payload HAVING tenant_id = 8",
            "SELECT payload FROM events ORDER BY tenant_id = 8 LIMIT 1",
            "UPDATE events SET tenant_id = 8 WHERE payload = 1",
        ] {
            let result =
                infer(&catalog, DEFAULT_DATABASE, SqlDialect::Sqlite, source, &[]).unwrap();
            assert_eq!(
                result.kind(),
                ShardKeyInferenceKind::Unconstrained,
                "{source}"
            );
        }
    }

    #[test]
    fn multi_row_insert_preserves_row_order_and_duplicate_keys() {
        let catalog = sample_catalog();
        let result = infer(
            &catalog,
            TENANT_DATABASE,
            SqlDialect::PostgreSql,
            "INSERT INTO accounts (payload, tenant_id) VALUES ($1, 'alpha'), ($2, $3), ($4, 'alpha')",
            &[
                Value::Int64(1),
                Value::Int64(2),
                Value::Text("beta".to_owned()),
                Value::Int64(3),
            ],
        )
        .unwrap();
        assert_result(
            &result,
            Some(ACCOUNTS_TABLE),
            Some(ShardKeyType::Text),
            ShardKeyInferenceKind::Multiple,
            &[
                ShardKeyValue::Text("alpha".to_owned()),
                ShardKeyValue::Text("beta".to_owned()),
                ShardKeyValue::Text("alpha".to_owned()),
            ],
        );

        let same = infer(
            &catalog,
            TENANT_DATABASE,
            SqlDialect::MySql,
            "INSERT INTO accounts (tenant_id, payload) VALUES (?, 1), (?, 2), (?, 3)",
            &[
                Value::Text("same".to_owned()),
                Value::Text("same".to_owned()),
                Value::Text("same".to_owned()),
            ],
        )
        .unwrap();
        assert_eq!(same.kind(), ShardKeyInferenceKind::Exact);
        assert_eq!(same.values().len(), 3);
        assert!(
            same.values()
                .iter()
                .all(|value| value.as_str() == Some("same"))
        );

        let middle = infer(
            &catalog,
            DEFAULT_DATABASE,
            SqlDialect::Sqlite,
            "INSERT INTO events (left_value, tenant_id, right_value) VALUES (?2, ?1, ?3)",
            &[
                Value::Int64(17),
                Value::Text("left".to_owned()),
                Value::Boolean(true),
            ],
        )
        .unwrap();
        assert_eq!(middle.kind(), ShardKeyInferenceKind::Exact);
        assert_eq!(middle.values(), &[ShardKeyValue::Int64(17)]);
    }

    #[test]
    fn inserts_without_a_direct_value_for_every_shard_key_are_unconstrained() {
        let catalog = sample_catalog();
        for source in [
            "INSERT INTO events (payload) VALUES (1), (2)",
            "INSERT INTO events (tenant_id, payload) VALUES (1 + 1, 2)",
            "INSERT INTO events (payload, tenant_id) VALUES (1, 7), (2, 8 + 1)",
        ] {
            let result =
                infer(&catalog, DEFAULT_DATABASE, SqlDialect::Sqlite, source, &[]).unwrap();
            assert_eq!(
                result.kind(),
                ShardKeyInferenceKind::Unconstrained,
                "{source}"
            );
            assert!(result.values().is_empty());
        }
    }

    #[test]
    fn null_insert_keys_are_rejected_for_literals_and_bound_values() {
        let catalog = sample_catalog();
        for (source, parameters) in [
            (
                "INSERT INTO events (tenant_id, payload) VALUES (NULL, 1)",
                vec![],
            ),
            (
                "INSERT INTO events (tenant_id, payload) VALUES (?1, 1)",
                vec![Value::Null],
            ),
            (
                "INSERT INTO events (tenant_id) VALUES (1 + 1), (NULL)",
                vec![],
            ),
            (
                "INSERT INTO events (tenant_id) VALUES (NULL), (1 + 1)",
                vec![],
            ),
        ] {
            let error = infer(
                &catalog,
                DEFAULT_DATABASE,
                SqlDialect::Sqlite,
                source,
                &parameters,
            )
            .unwrap_err();
            assert_kind(error, EngineErrorKind::NotNullViolation);
        }

        for source in [
            "INSERT INTO events (tenant_id) VALUES (1 + 1), ('wrong')",
            "INSERT INTO events (tenant_id) VALUES ('wrong'), (1 + 1)",
        ] {
            let error =
                infer(&catalog, DEFAULT_DATABASE, SqlDialect::Sqlite, source, &[]).unwrap_err();
            assert_kind(error, EngineErrorKind::TypeMismatch);
        }

        for source in [
            "INSERT INTO events (tenant_id) VALUES (1 + 1), (9223372036854775808)",
            "INSERT INTO events (tenant_id) VALUES (9223372036854775808), (1 + 1)",
        ] {
            let error =
                infer(&catalog, DEFAULT_DATABASE, SqlDialect::Sqlite, source, &[]).unwrap_err();
            assert_kind(error, EngineErrorKind::NumericOutOfRange);
        }
    }

    #[test]
    fn integer_keys_accept_full_signed_range_and_lossless_unsigned_values() {
        let catalog = sample_catalog();
        for (literal, expected) in [
            ("0", 0),
            ("+42", 42),
            ("-(-7)", 7),
            ("9223372036854775807", i64::MAX),
            ("-9223372036854775808", i64::MIN),
        ] {
            let source = format!("SELECT * FROM events WHERE tenant_id = {literal}");
            let result =
                infer(&catalog, DEFAULT_DATABASE, SqlDialect::Sqlite, &source, &[]).unwrap();
            assert_eq!(result.values(), &[ShardKeyValue::Int64(expected)]);
        }

        for (value, expected) in [
            (Value::Int64(i64::MIN), i64::MIN),
            (Value::UInt64(i64::MAX as u64), i64::MAX),
        ] {
            let result = infer(
                &catalog,
                DEFAULT_DATABASE,
                SqlDialect::Sqlite,
                "SELECT * FROM events WHERE tenant_id = ?1",
                &[value],
            )
            .unwrap();
            assert_eq!(result.values()[0].as_i64(), Some(expected));
        }
    }

    #[test]
    fn integer_overflow_and_incompatible_key_types_have_stable_error_kinds() {
        let catalog = sample_catalog();
        for literal in ["9223372036854775808", "-9223372036854775809"] {
            let source = format!("SELECT * FROM events WHERE tenant_id = {literal}");
            let error =
                infer(&catalog, DEFAULT_DATABASE, SqlDialect::Sqlite, &source, &[]).unwrap_err();
            assert_kind(error, EngineErrorKind::NumericOutOfRange);
        }
        let error = infer(
            &catalog,
            DEFAULT_DATABASE,
            SqlDialect::Sqlite,
            "SELECT * FROM events WHERE tenant_id = ?1",
            &[Value::UInt64(i64::MAX as u64 + 1)],
        )
        .unwrap_err();
        assert_kind(error, EngineErrorKind::NumericOutOfRange);

        let incompatible = [
            Value::Boolean(true),
            Value::Float64(1.0),
            Value::decimal("1.0").unwrap(),
            Value::Text("1".to_owned()),
            Value::InvalidText(vec![b'1']),
            Value::Binary(vec![1]),
        ];
        for value in incompatible {
            let error = infer(
                &catalog,
                DEFAULT_DATABASE,
                SqlDialect::Sqlite,
                "SELECT * FROM events WHERE tenant_id = ?1",
                &[value],
            )
            .unwrap_err();
            assert_kind(error, EngineErrorKind::TypeMismatch);
        }
        for literal in ["1.5", "'1'", "TRUE"] {
            let source = format!("SELECT * FROM events WHERE tenant_id = {literal}");
            let error =
                infer(&catalog, DEFAULT_DATABASE, SqlDialect::Sqlite, &source, &[]).unwrap_err();
            assert_kind(error, EngineErrorKind::TypeMismatch);
        }
    }

    #[test]
    fn text_predicates_need_collation_metadata_and_binary_keys_are_exact() {
        let catalog = sample_catalog();
        let text = infer(
            &catalog,
            TENANT_DATABASE,
            SqlDialect::PostgreSql,
            "SELECT * FROM accounts WHERE tenant_id = $1",
            &[Value::Text("snowman-☃".to_owned())],
        )
        .unwrap();
        assert_eq!(text.kind(), ShardKeyInferenceKind::Unconstrained);
        assert!(text.values().is_empty());

        let text_literal = infer(
            &catalog,
            TENANT_DATABASE,
            SqlDialect::PostgreSql,
            "SELECT * FROM accounts WHERE tenant_id = 'snowman-☃'",
            &[],
        )
        .unwrap();
        assert_eq!(text_literal.kind(), ShardKeyInferenceKind::Unconstrained);
        assert!(text_literal.values().is_empty());

        let folded_candidates = infer(
            &catalog,
            TENANT_DATABASE,
            SqlDialect::MySql,
            "SELECT * FROM accounts WHERE tenant_id = 'A' OR tenant_id = 'a'",
            &[],
        )
        .unwrap();
        assert_eq!(
            folded_candidates.kind(),
            ShardKeyInferenceKind::Unconstrained
        );
        assert!(folded_candidates.values().is_empty());

        let bound_null = infer(
            &catalog,
            TENANT_DATABASE,
            SqlDialect::PostgreSql,
            "SELECT * FROM accounts WHERE tenant_id = $1",
            &[Value::Null],
        )
        .unwrap();
        assert_eq!(bound_null.kind(), ShardKeyInferenceKind::Contradiction);
        assert!(bound_null.values().is_empty());

        let invalid = infer(
            &catalog,
            TENANT_DATABASE,
            SqlDialect::PostgreSql,
            "SELECT * FROM accounts WHERE tenant_id = $1",
            &[Value::InvalidText(vec![0xff, 0xfe])],
        )
        .unwrap_err();
        assert_kind(invalid, EngineErrorKind::InvalidTextEncoding);

        let binary = infer(
            &catalog,
            DEFAULT_DATABASE,
            SqlDialect::MySql,
            "SELECT * FROM blobs WHERE tenant_id = ?",
            &[Value::Binary(vec![0, 1, 2, 255])],
        )
        .unwrap();
        assert_eq!(binary.values()[0].as_bytes(), Some(&[0, 1, 2, 255][..]));

        for (database, source, value) in [
            (
                TENANT_DATABASE,
                "SELECT * FROM accounts WHERE tenant_id = $1",
                Value::Binary(vec![1]),
            ),
            (
                DEFAULT_DATABASE,
                "SELECT * FROM blobs WHERE tenant_id = $1",
                Value::Text("bytes".to_owned()),
            ),
        ] {
            let error =
                infer(&catalog, database, SqlDialect::PostgreSql, source, &[value]).unwrap_err();
            assert_kind(error, EngineErrorKind::TypeMismatch);
        }
        let literal_binary = infer(
            &catalog,
            DEFAULT_DATABASE,
            SqlDialect::PostgreSql,
            "SELECT * FROM blobs WHERE tenant_id = 'bytes'",
            &[],
        )
        .unwrap_err();
        assert_kind(literal_binary, EngineErrorKind::TypeMismatch);

        for value in [
            Value::Boolean(true),
            Value::Int64(1),
            Value::UInt64(1),
            Value::Float64(1.0),
            Value::decimal("1.0").unwrap(),
            Value::Binary(vec![1]),
        ] {
            let error = infer(
                &catalog,
                TENANT_DATABASE,
                SqlDialect::PostgreSql,
                "SELECT * FROM accounts WHERE tenant_id = $1",
                &[value],
            )
            .unwrap_err();
            assert_kind(error, EngineErrorKind::TypeMismatch);
        }

        for value in [
            Value::Boolean(true),
            Value::Int64(1),
            Value::UInt64(1),
            Value::Float64(1.0),
            Value::decimal("1.0").unwrap(),
            Value::Text("one".to_owned()),
            Value::InvalidText(vec![0xff]),
        ] {
            let error = infer(
                &catalog,
                DEFAULT_DATABASE,
                SqlDialect::PostgreSql,
                "SELECT * FROM blobs WHERE tenant_id = $1",
                &[value],
            )
            .unwrap_err();
            assert_kind(error, EngineErrorKind::TypeMismatch);
        }

        for (database, source) in [
            (
                TENANT_DATABASE,
                "SELECT * FROM accounts WHERE tenant_id = NULL",
            ),
            (
                DEFAULT_DATABASE,
                "SELECT * FROM blobs WHERE tenant_id = NULL",
            ),
        ] {
            let result = infer(&catalog, database, SqlDialect::PostgreSql, source, &[]).unwrap();
            assert_eq!(result.kind(), ShardKeyInferenceKind::Contradiction);
            assert!(result.values().is_empty());
        }
    }

    #[test]
    fn nonsharded_and_non_table_statements_have_distinct_results() {
        let catalog = sample_catalog();
        for (source, table_id) in [
            ("SELECT * FROM countries", COUNTRIES_TABLE),
            (
                "DELETE FROM internal_catalog WHERE id = 1",
                INTERNAL_CATALOG_TABLE,
            ),
        ] {
            let result =
                infer(&catalog, DEFAULT_DATABASE, SqlDialect::Sqlite, source, &[]).unwrap();
            assert_result(
                &result,
                Some(table_id),
                None,
                ShardKeyInferenceKind::NotSharded,
                &[],
            );
        }

        for source in [
            "SELECT 1",
            "CREATE TABLE local_table (id INTEGER)",
            "CREATE INDEX local_index ON local_table (id)",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
        ] {
            let result =
                infer(&catalog, DEFAULT_DATABASE, SqlDialect::Sqlite, source, &[]).unwrap();
            assert_result(
                &result,
                None,
                None,
                ShardKeyInferenceKind::NotApplicable,
                &[],
            );
        }
    }

    #[test]
    fn database_and_table_resolution_are_explicit_and_database_scoped() {
        let catalog = sample_catalog();
        let accounts = infer(
            &catalog,
            TENANT_DATABASE,
            SqlDialect::PostgreSql,
            "SELECT * FROM accounts WHERE tenant_id = 'tenant-a'",
            &[],
        )
        .unwrap();
        assert_eq!(
            accounts.table_id(),
            Some(TableId::new(ACCOUNTS_TABLE).unwrap())
        );

        let missing_in_default = infer(
            &catalog,
            DEFAULT_DATABASE,
            SqlDialect::PostgreSql,
            "SELECT * FROM accounts WHERE tenant_id = 'tenant-a'",
            &[],
        )
        .unwrap_err();
        assert_kind(missing_in_default, EngineErrorKind::InvalidQuery);

        let missing_table = infer(
            &catalog,
            DEFAULT_DATABASE,
            SqlDialect::Sqlite,
            "SELECT * FROM missing WHERE tenant_id = 1",
            &[],
        )
        .unwrap_err();
        assert_kind(missing_table, EngineErrorKind::InvalidQuery);

        let normalized = normalize(SqlDialect::Sqlite, "SELECT 1");
        let missing_database = infer_shard_keys(
            &catalog,
            LogicalDatabaseId::new(99).unwrap(),
            &normalized,
            0,
            &[],
        )
        .unwrap_err();
        assert_kind(missing_database, EngineErrorKind::InvalidArgument);
    }

    #[test]
    fn exact_parameter_arity_and_statement_index_are_validated_first() {
        let catalog = sample_catalog();
        let normalized = normalize(
            SqlDialect::PostgreSql,
            "SELECT * FROM events WHERE tenant_id = $2",
        );
        for parameters in [vec![], vec![Value::Null], vec![Value::Null; 3]] {
            let error = infer_shard_keys(
                &catalog,
                LogicalDatabaseId::new(DEFAULT_DATABASE).unwrap(),
                &normalized,
                0,
                &parameters,
            )
            .unwrap_err();
            assert_kind(error, EngineErrorKind::InvalidArgument);
        }
        let exact = infer_shard_keys(
            &catalog,
            LogicalDatabaseId::new(DEFAULT_DATABASE).unwrap(),
            &normalized,
            0,
            &[Value::Text("gap".to_owned()), Value::Int64(6)],
        )
        .unwrap();
        assert_eq!(exact.values(), &[ShardKeyValue::Int64(6)]);

        let bad_index = infer_shard_keys(
            &catalog,
            LogicalDatabaseId::new(DEFAULT_DATABASE).unwrap(),
            &normalized,
            1,
            &[],
        )
        .unwrap_err();
        assert_kind(bad_index, EngineErrorKind::InvalidArgument);

        let empty = normalize(SqlDialect::Sqlite, "-- no statements");
        let empty_index = infer_shard_keys(
            &catalog,
            LogicalDatabaseId::new(DEFAULT_DATABASE).unwrap(),
            &empty,
            0,
            &[],
        )
        .unwrap_err();
        assert_kind(empty_index, EngineErrorKind::InvalidArgument);

        let unknown_table = normalize(SqlDialect::MySql, "SELECT ? FROM missing");
        let arity = infer_shard_keys(
            &catalog,
            LogicalDatabaseId::new(DEFAULT_DATABASE).unwrap(),
            &unknown_table,
            0,
            &[],
        )
        .unwrap_err();
        assert_kind(arity, EngineErrorKind::InvalidArgument);
    }

    #[test]
    fn batches_reset_parameter_numbering_and_select_statements_by_index() {
        let catalog = sample_catalog();
        let normalized = normalize(
            SqlDialect::MySql,
            "SELECT ? FROM events WHERE tenant_id = ?; SELECT ? FROM events WHERE tenant_id = ?",
        );
        assert_eq!(normalized.statement_count(), 2);
        for (statement_index, key) in [(0, 11), (1, 22)] {
            let result = infer_shard_keys(
                &catalog,
                LogicalDatabaseId::new(DEFAULT_DATABASE).unwrap(),
                &normalized,
                statement_index,
                &[Value::Text("projection".to_owned()), Value::Int64(key)],
            )
            .unwrap();
            assert_eq!(result.values(), &[ShardKeyValue::Int64(key)]);
        }

        let repeated = infer(
            &catalog,
            DEFAULT_DATABASE,
            SqlDialect::PostgreSql,
            "SELECT * FROM events WHERE tenant_id = $1 OR tenant_id = $1",
            &[Value::Int64(33)],
        )
        .unwrap();
        assert_eq!(repeated.kind(), ShardKeyInferenceKind::Exact);
        assert_eq!(repeated.values(), &[ShardKeyValue::Int64(33)]);
    }

    #[test]
    fn diagnostics_do_not_include_sql_or_bound_key_contents() {
        let catalog = sample_catalog();
        let sql_secret = "SQL_SECRET_7A9C";
        let value_secret = "VALUE_SECRET_4B2D";
        let source = format!("SELECT * FROM missing_{sql_secret} WHERE tenant_id = $1");
        let error = infer(
            &catalog,
            DEFAULT_DATABASE,
            SqlDialect::PostgreSql,
            &source,
            &[Value::Text(value_secret.to_owned())],
        )
        .unwrap_err();
        let debug = format!("{error:?}");
        assert!(!error.diagnostic().contains(sql_secret));
        assert!(!error.diagnostic().contains(value_secret));
        assert!(!debug.contains(sql_secret));
        assert!(!debug.contains(value_secret));

        let type_error = infer(
            &catalog,
            DEFAULT_DATABASE,
            SqlDialect::PostgreSql,
            "SELECT * FROM events WHERE tenant_id = $1",
            &[Value::Text(value_secret.to_owned())],
        )
        .unwrap_err();
        assert!(!type_error.diagnostic().contains(value_secret));
        assert!(!format!("{type_error:?}").contains(value_secret));
    }

    #[test]
    fn inference_is_deterministic_under_concurrency_and_recovers_after_errors() {
        const WORKERS: usize = 12;
        let catalog = Arc::new(sample_catalog());
        let normalized = Arc::new(normalize(
            SqlDialect::PostgreSql,
            "SELECT * FROM events WHERE tenant_id = $1 OR tenant_id = $2",
        ));
        let barrier = Arc::new(Barrier::new(WORKERS));
        let mut workers = Vec::new();
        for _ in 0..WORKERS {
            let catalog = Arc::clone(&catalog);
            let normalized = Arc::clone(&normalized);
            let barrier = Arc::clone(&barrier);
            workers.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    let result = infer_shard_keys(
                        &catalog,
                        LogicalDatabaseId::new(DEFAULT_DATABASE).unwrap(),
                        &normalized,
                        0,
                        &[Value::Int64(7), Value::Int64(9)],
                    )
                    .unwrap();
                    assert_eq!(result.kind(), ShardKeyInferenceKind::Multiple);
                    assert_eq!(
                        result.values(),
                        &[ShardKeyValue::Int64(7), ShardKeyValue::Int64(9)]
                    );
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }

        let failed = infer_shard_keys(
            &catalog,
            LogicalDatabaseId::new(DEFAULT_DATABASE).unwrap(),
            &normalized,
            0,
            &[Value::Int64(7)],
        )
        .unwrap_err();
        assert_kind(failed, EngineErrorKind::InvalidArgument);
        let recovered = infer_shard_keys(
            &catalog,
            LogicalDatabaseId::new(DEFAULT_DATABASE).unwrap(),
            &normalized,
            0,
            &[Value::Int64(7), Value::Int64(9)],
        )
        .unwrap();
        assert_eq!(recovered.kind(), ShardKeyInferenceKind::Multiple);
    }
}
