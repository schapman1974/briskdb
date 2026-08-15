//! Conservative exact-key inference for authoritative global-index reads.

use std::collections::HashSet;

use sqlparser::{
    ast::{
        BinaryOperator, Expr, Ident, ObjectNamePart, SetExpr, Statement as AstStatement,
        TableFactor, UnaryOperator, Value as AstValue,
    },
    tokenizer::Span,
};

use super::NormalizedSql;
use crate::core::{
    CanonicalIndexKey, Catalog, EngineError, EngineErrorKind, EngineResult, GlobalIndexId,
    GlobalIndexKeyPart, GlobalIndexKeySource, GlobalIndexKeyType, GlobalIndexLifecycle,
    IndexKeyPart, IndexKeyValue, LogicalDatabaseId, TableId, Value,
};

/// Bound exact-key lookups for one authoritative index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GlobalIndexLookupCandidate {
    index_id: GlobalIndexId,
    index_name: String,
    unique: bool,
    keys: Vec<CanonicalIndexKey>,
    query_predicate_sql: String,
    query_table_alias: Option<String>,
}

impl GlobalIndexLookupCandidate {
    pub(crate) const fn index_id(&self) -> GlobalIndexId {
        self.index_id
    }

    pub(crate) fn index_name(&self) -> &str {
        &self.index_name
    }

    pub(crate) const fn is_unique(&self) -> bool {
        self.unique
    }

    pub(crate) fn keys(&self) -> &[CanonicalIndexKey] {
        &self.keys
    }

    pub(crate) fn query_predicate_sql(&self) -> &str {
        &self.query_predicate_sql
    }

    pub(crate) fn query_table_alias(&self) -> Option<&str> {
        self.query_table_alias.as_deref()
    }
}

/// Why no authoritative exact-key lookup could be planned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GlobalIndexInferenceFallback {
    NoReadyAuthoritativeIndex,
    UnsupportedIndexDefinition,
    PredicateNotExact,
    TooManyKeys,
}

/// Bound the planner's Cartesian expansion for compound `IN` predicates.
pub(crate) const MAX_GLOBAL_INDEX_LOOKUP_KEYS: usize = 1_024;

pub(crate) fn infer_global_index_lookup(
    catalog: &Catalog,
    database: LogicalDatabaseId,
    normalized: &NormalizedSql,
    statement_index: usize,
    parameters: &[Value],
    table_id: TableId,
) -> EngineResult<Result<GlobalIndexLookupCandidate, GlobalIndexInferenceFallback>> {
    let statement = normalized
        .common()
        .statements()
        .get(statement_index)
        .ok_or_else(inference_invariant)?;
    let layout = normalized
        .statement_parameters()
        .get(statement_index)
        .ok_or_else(inference_invariant)?;
    if parameters.len() != layout.parameter_count() {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            format!(
                "statement {} requires exactly {} bound parameters for global-index inference",
                statement_index + 1,
                layout.parameter_count()
            ),
        ));
    }
    let database_name = catalog
        .database_by_id(database)
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::InvalidArgument,
                "selected logical database does not exist",
            )
        })?
        .name();
    let AstStatement::Query(query) = statement else {
        return Ok(Err(GlobalIndexInferenceFallback::PredicateNotExact));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(inference_invariant());
    };
    let [from] = select.from.as_slice() else {
        return Err(inference_invariant());
    };
    if !from.joins.is_empty() {
        return Err(inference_invariant());
    }
    let TableFactor::Table { name, alias, .. } = &from.relation else {
        return Err(inference_invariant());
    };
    let [ObjectNamePart::Identifier(source)] = name.0.as_slice() else {
        return Err(inference_invariant());
    };
    let source_name = catalog_identifier(source).ok_or_else(inference_invariant)?;
    let table = catalog
        .table(database_name, &source_name)?
        .ok_or_else(inference_invariant)?;
    if table.id() != table_id {
        return Err(inference_invariant());
    }
    let Some(predicate) = select.selection.as_ref() else {
        return Ok(Err(GlobalIndexInferenceFallback::PredicateNotExact));
    };
    let qualifier = alias.as_ref().map_or_else(
        || reference_identifier(source),
        |alias| reference_identifier(&alias.name),
    );
    let context = InferenceContext {
        normalized,
        statement_index,
        parameters,
        qualifier,
    };

    let mut saw_ready = false;
    let mut saw_supported = false;
    let mut saw_too_many = false;
    let mut candidates = Vec::new();
    for index in catalog
        .global_indexes()
        .iter()
        .filter(|index| index.table_id() == table_id)
    {
        if index.lifecycle() != GlobalIndexLifecycle::Ready {
            continue;
        }
        saw_ready = true;
        if index.predicate().is_some()
            || index
                .key_parts()
                .iter()
                .any(|part| !matches!(part.source(), GlobalIndexKeySource::Column(_)))
        {
            continue;
        }
        saw_supported = true;
        match infer_index_keys(predicate, index.key_parts(), &context)? {
            IndexKeys::Unconstrained => {}
            IndexKeys::TooMany => saw_too_many = true,
            IndexKeys::Finite(keys) => candidates.push(GlobalIndexLookupCandidate {
                index_id: index.id(),
                index_name: index.name().to_owned(),
                unique: index.is_unique(),
                keys,
                query_predicate_sql: String::new(),
                query_table_alias: alias
                    .as_ref()
                    .map(|alias| reference_identifier(&alias.name)),
            }),
        }
    }

    // Prefer the fewest authority probes, then the most selective compound
    // definition, then the stable catalog ID.
    candidates.sort_by_key(|candidate| {
        let parts = catalog
            .global_index_by_id(candidate.index_id)
            .map_or(0, |index| index.key_parts().len());
        (
            !candidate.unique,
            candidate.keys.len(),
            usize::MAX - parts,
            candidate.index_id.get(),
        )
    });
    if let Some(mut candidate) = candidates.into_iter().next() {
        candidate.query_predicate_sql =
            super::translator::translated_query_predicate(normalized, statement_index)?;
        return Ok(Ok(candidate));
    }
    Ok(Err(if saw_too_many {
        GlobalIndexInferenceFallback::TooManyKeys
    } else if saw_supported {
        GlobalIndexInferenceFallback::PredicateNotExact
    } else if saw_ready {
        GlobalIndexInferenceFallback::UnsupportedIndexDefinition
    } else {
        GlobalIndexInferenceFallback::NoReadyAuthoritativeIndex
    }))
}

struct InferenceContext<'a> {
    normalized: &'a NormalizedSql,
    statement_index: usize,
    parameters: &'a [Value],
    qualifier: String,
}

enum IndexKeys {
    Unconstrained,
    TooMany,
    Finite(Vec<CanonicalIndexKey>),
}

fn infer_index_keys(
    predicate: &Expr,
    parts: &[GlobalIndexKeyPart],
    context: &InferenceContext<'_>,
) -> EngineResult<IndexKeys> {
    let mut domains = Vec::with_capacity(parts.len());
    for part in parts {
        let GlobalIndexKeySource::Column(column) = part.source() else {
            return Ok(IndexKeys::Unconstrained);
        };
        match infer_predicate(predicate, column, part.key_type(), context)? {
            KeyDomain::Any => return Ok(IndexKeys::Unconstrained),
            KeyDomain::Finite(values) => domains.push(values),
        }
    }
    if domains.iter().any(Vec::is_empty) {
        return Ok(IndexKeys::Finite(Vec::new()));
    }

    let mut rows: Vec<Vec<IndexKeyValue>> = vec![Vec::with_capacity(parts.len())];
    for domain in domains {
        let expanded = rows.len().checked_mul(domain.len());
        if expanded.is_none_or(|count| count > MAX_GLOBAL_INDEX_LOOKUP_KEYS) {
            return Ok(IndexKeys::TooMany);
        }
        let mut next = Vec::with_capacity(expanded.unwrap_or(0));
        for row in &rows {
            for value in &domain {
                let mut expanded_row = row.clone();
                expanded_row.push(value.clone());
                next.push(expanded_row);
            }
        }
        rows = next;
    }

    let mut seen = HashSet::new();
    let mut keys = Vec::with_capacity(rows.len());
    for values in rows {
        let encoded_parts = values
            .iter()
            .zip(parts)
            .map(|(value, metadata)| {
                let part = match metadata.order() {
                    crate::core::IndexKeyOrder::Ascending => {
                        IndexKeyPart::ascending(value.as_ref())
                    }
                    crate::core::IndexKeyOrder::Descending => {
                        IndexKeyPart::descending(value.as_ref())
                    }
                };
                part.with_null_order(metadata.null_order())
                    .with_collation(metadata.collation())
            })
            .collect::<Vec<_>>();
        let key = CanonicalIndexKey::encode(&encoded_parts)?;
        if seen.insert(key.clone()) {
            keys.push(key);
        }
    }
    Ok(IndexKeys::Finite(keys))
}

enum KeyDomain {
    Any,
    Finite(Vec<IndexKeyValue>),
}

fn infer_predicate(
    expression: &Expr,
    column: &str,
    key_type: GlobalIndexKeyType,
    context: &InferenceContext<'_>,
) -> EngineResult<KeyDomain> {
    match peel_nested(expression) {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if is_target_column(left, column, context) {
                return atom_domain(right, key_type, context);
            }
            if is_target_column(right, column, context) {
                return atom_domain(left, key_type, context);
            }
            Ok(KeyDomain::Any)
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } if is_target_column(expr, column, context) => {
            let mut values = Vec::with_capacity(list.len());
            for expression in list {
                match infer_atom(expression, key_type, context)? {
                    InferredAtom::Value(value) => {
                        if !values.contains(&value) {
                            values.push(value);
                        }
                    }
                    // `x IN (..., NULL)` cannot match through the NULL member.
                    InferredAtom::Null => {}
                    InferredAtom::Unresolved => return Ok(KeyDomain::Any),
                }
            }
            Ok(KeyDomain::Finite(values))
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => Ok(intersect_domains(
            infer_predicate(left, column, key_type, context)?,
            infer_predicate(right, column, key_type, context)?,
        )),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => Ok(union_domains(
            infer_predicate(left, column, key_type, context)?,
            infer_predicate(right, column, key_type, context)?,
        )),
        _ => Ok(KeyDomain::Any),
    }
}

fn atom_domain(
    expression: &Expr,
    key_type: GlobalIndexKeyType,
    context: &InferenceContext<'_>,
) -> EngineResult<KeyDomain> {
    Ok(match infer_atom(expression, key_type, context)? {
        InferredAtom::Value(value) => KeyDomain::Finite(vec![value]),
        // SQL equality with NULL is never true.
        InferredAtom::Null => KeyDomain::Finite(Vec::new()),
        InferredAtom::Unresolved => KeyDomain::Any,
    })
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
    Value(IndexKeyValue),
}

fn infer_atom(
    expression: &Expr,
    key_type: GlobalIndexKeyType,
    context: &InferenceContext<'_>,
) -> EngineResult<InferredAtom> {
    let expression = peel_nested(expression);
    if let Expr::Value(value) = expression {
        match &value.value {
            AstValue::Placeholder(_) => {
                return Ok(bound_atom(bound_parameter(value.span, context)?, key_type));
            }
            AstValue::Null => return Ok(InferredAtom::Null),
            AstValue::Boolean(value) if key_type == GlobalIndexKeyType::Boolean => {
                return Ok(InferredAtom::Value(IndexKeyValue::Boolean(*value)));
            }
            AstValue::SingleQuotedString(value) if key_type == GlobalIndexKeyType::Text => {
                return Ok(InferredAtom::Value(IndexKeyValue::Text(value.clone())));
            }
            AstValue::Number(number, false) if key_type == GlobalIndexKeyType::Float64 => {
                return Ok(number
                    .parse::<f64>()
                    .ok()
                    .filter(|value| value.is_finite())
                    .map(IndexKeyValue::Float64)
                    .map_or(InferredAtom::Unresolved, InferredAtom::Value));
            }
            AstValue::Number(_, false) => {}
            _ => return Ok(InferredAtom::Unresolved),
        }
    }

    let Some(integer) = signed_integer_literal(expression) else {
        return Ok(InferredAtom::Unresolved);
    };
    Ok(match key_type {
        GlobalIndexKeyType::Int64 => InferredAtom::Value(IndexKeyValue::Int64(integer)),
        GlobalIndexKeyType::UInt64 => u64::try_from(integer)
            .map(IndexKeyValue::UInt64)
            .map_or(InferredAtom::Unresolved, InferredAtom::Value),
        GlobalIndexKeyType::Float64 => InferredAtom::Value(IndexKeyValue::Float64(integer as f64)),
        GlobalIndexKeyType::Date => i32::try_from(integer)
            .map(IndexKeyValue::Date)
            .map_or(InferredAtom::Unresolved, InferredAtom::Value),
        GlobalIndexKeyType::Timestamp => InferredAtom::Value(IndexKeyValue::Timestamp(integer)),
        GlobalIndexKeyType::Boolean | GlobalIndexKeyType::Text | GlobalIndexKeyType::Binary => {
            InferredAtom::Unresolved
        }
    })
}

fn bound_atom(value: &Value, key_type: GlobalIndexKeyType) -> InferredAtom {
    if matches!(value, Value::Null) {
        return InferredAtom::Null;
    }
    let value = match (key_type, value) {
        (GlobalIndexKeyType::Boolean, Value::Boolean(value)) => IndexKeyValue::Boolean(*value),
        (GlobalIndexKeyType::Int64, Value::Int64(value)) => IndexKeyValue::Int64(*value),
        (GlobalIndexKeyType::Int64, Value::UInt64(value)) => match i64::try_from(*value) {
            Ok(value) => IndexKeyValue::Int64(value),
            Err(_) => return InferredAtom::Unresolved,
        },
        (GlobalIndexKeyType::UInt64, Value::UInt64(value)) => IndexKeyValue::UInt64(*value),
        (GlobalIndexKeyType::UInt64, Value::Int64(value)) => match u64::try_from(*value) {
            Ok(value) => IndexKeyValue::UInt64(value),
            Err(_) => return InferredAtom::Unresolved,
        },
        (GlobalIndexKeyType::Float64, Value::Float64(value)) if value.is_finite() => {
            IndexKeyValue::Float64(*value)
        }
        (GlobalIndexKeyType::Float64, Value::Int64(value)) => IndexKeyValue::Float64(*value as f64),
        (GlobalIndexKeyType::Float64, Value::UInt64(value)) => {
            IndexKeyValue::Float64(*value as f64)
        }
        (GlobalIndexKeyType::Date, Value::Int64(value)) => match i32::try_from(*value) {
            Ok(value) => IndexKeyValue::Date(value),
            Err(_) => return InferredAtom::Unresolved,
        },
        (GlobalIndexKeyType::Timestamp, Value::Int64(value)) => IndexKeyValue::Timestamp(*value),
        (GlobalIndexKeyType::Text, Value::Text(value)) => IndexKeyValue::Text(value.clone()),
        (GlobalIndexKeyType::Binary, Value::Binary(value)) => IndexKeyValue::Binary(value.clone()),
        _ => return InferredAtom::Unresolved,
    };
    InferredAtom::Value(value)
}

fn bound_parameter<'a>(span: Span, context: &'a InferenceContext<'_>) -> EngineResult<&'a Value> {
    context
        .normalized
        .parameter_index(context.statement_index, span)
        .and_then(|index| index.checked_sub(1))
        .and_then(|index| context.parameters.get(index))
        .ok_or_else(inference_invariant)
}

fn is_target_column(
    expression: &Expr,
    target_column: &str,
    context: &InferenceContext<'_>,
) -> bool {
    match peel_nested(expression) {
        Expr::Identifier(column) => identifier_matches_catalog(column, target_column),
        Expr::CompoundIdentifier(parts) => {
            let [qualifier, column] = parts.as_slice() else {
                return false;
            };
            reference_identifier(qualifier) == context.qualifier
                && identifier_matches_catalog(column, target_column)
        }
        _ => false,
    }
}

fn signed_integer_literal(expression: &Expr) -> Option<i64> {
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
                    return None;
                };
                if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
                    return None;
                }
                let limit = if negative {
                    i64::MAX as u64 + 1
                } else {
                    i64::MAX as u64
                };
                let magnitude = number.bytes().try_fold(0_u64, |value, byte| {
                    value
                        .checked_mul(10)?
                        .checked_add(u64::from(byte - b'0'))
                        .filter(|value| *value <= limit)
                })?;
                if negative && magnitude == i64::MAX as u64 + 1 {
                    return Some(i64::MIN);
                }
                let value = magnitude as i64;
                return Some(if negative { -value } else { value });
            }
            _ => return None,
        }
    }
}

fn peel_nested(mut expression: &Expr) -> &Expr {
    while let Expr::Nested(inner) = expression {
        expression = inner;
    }
    expression
}

fn catalog_identifier(identifier: &Ident) -> Option<String> {
    let value = reference_identifier(identifier);
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

fn inference_invariant() -> EngineError {
    EngineError::new(
        EngineErrorKind::Internal,
        "global-index inference is inconsistent during bound statement planning",
    )
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::{
        core::{
            GlobalIndexMetadata, GlobalIndexStorageTopology, LogicalDatabaseMetadata,
            ShardKeyMetadata, ShardKeyType, TableMetadata, TablePlacement, UniqueNullSemantics,
        },
        sql::{SqlDialect, normalize_placeholders, parse, validate_common_subset},
    };

    const DATABASE: u64 = 1;
    const TABLE: u64 = 2;

    fn metadata(
        id: u64,
        name: &str,
        parts: Vec<GlobalIndexKeyPart>,
        predicate: Option<&str>,
        lifecycle: GlobalIndexLifecycle,
        unique: bool,
    ) -> GlobalIndexMetadata {
        GlobalIndexMetadata::from_validated(
            id,
            TABLE,
            name.to_owned(),
            parts.into_boxed_slice(),
            unique,
            UniqueNullSemantics::NotDistinct,
            predicate.map(str::to_owned),
            lifecycle,
            7,
            GlobalIndexStorageTopology::selected_v1(),
        )
    }

    fn column(name: &str, key_type: GlobalIndexKeyType) -> GlobalIndexKeyPart {
        GlobalIndexKeyPart::new(GlobalIndexKeySource::column(name).unwrap(), key_type)
    }

    fn catalog(indexes: Vec<GlobalIndexMetadata>) -> Catalog {
        Catalog::from_validated_parts(
            1,
            7,
            DATABASE,
            vec![LogicalDatabaseMetadata::from_validated(
                DATABASE,
                "default".to_owned(),
            )]
            .into_boxed_slice(),
            vec![TableMetadata::from_validated(
                TABLE,
                DATABASE,
                "users".to_owned(),
                TablePlacement::Sharded(ShardKeyMetadata::from_validated(
                    "tenant_id".to_owned(),
                    ShardKeyType::Text,
                )),
            )]
            .into_boxed_slice(),
        )
        .with_global_indexes(indexes.into_boxed_slice())
    }

    fn normalized(source: &str) -> NormalizedSql {
        normalize_placeholders(
            validate_common_subset(parse(SqlDialect::Sqlite, source).unwrap()).unwrap(),
        )
        .unwrap()
    }

    fn infer(
        catalog: &Catalog,
        source: &str,
        parameters: &[Value],
    ) -> Result<GlobalIndexLookupCandidate, GlobalIndexInferenceFallback> {
        let normalized = normalized(source);
        infer_global_index_lookup(
            catalog,
            LogicalDatabaseId::new(DATABASE).unwrap(),
            &normalized,
            0,
            parameters,
            TableId::new(TABLE).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn equality_in_compound_null_alias_and_mixed_predicates_are_conservative() {
        let catalog = catalog(vec![
            metadata(
                1,
                "email_unique",
                vec![column("email", GlobalIndexKeyType::Text)],
                None,
                GlobalIndexLifecycle::Ready,
                true,
            ),
            metadata(
                2,
                "email_region_unique",
                vec![
                    column("email", GlobalIndexKeyType::Text),
                    column("region", GlobalIndexKeyType::Text),
                ],
                None,
                GlobalIndexLifecycle::Ready,
                true,
            ),
        ]);

        let equality = infer(
            &catalog,
            "SELECT * FROM users AS u WHERE u.email = ?1 AND u.payload > 0",
            &["a@example.test".into()],
        )
        .unwrap();
        assert_eq!(equality.index_name(), "email_unique");
        assert_eq!(equality.keys().len(), 1);

        let compound = infer(
            &catalog,
            "SELECT * FROM users
             WHERE email IN (?1, NULL, ?2) AND region IN ('east', 'west')",
            &["a@example.test".into(), "b@example.test".into()],
        )
        .unwrap();
        assert_eq!(compound.index_name(), "email_unique");
        assert_eq!(compound.keys().len(), 2);

        let null_only = infer(&catalog, "SELECT * FROM users WHERE email = NULL", &[]).unwrap();
        assert!(null_only.keys().is_empty());

        assert_eq!(
            infer(
                &catalog,
                "SELECT * FROM users WHERE email = ?1 OR payload = 9",
                &["a@example.test".into()],
            ),
            Err(GlobalIndexInferenceFallback::PredicateNotExact)
        );
    }

    #[test]
    fn compound_index_is_used_when_every_part_is_exact() {
        let catalog = catalog(vec![metadata(
            1,
            "email_region_unique",
            vec![
                column("email", GlobalIndexKeyType::Text),
                column("region", GlobalIndexKeyType::Text),
            ],
            None,
            GlobalIndexLifecycle::Ready,
            true,
        )]);
        let candidate = infer(
            &catalog,
            "SELECT * FROM users
             WHERE email IN (?1, ?2) AND region IN ('east', 'west')",
            &["a@example.test".into(), "b@example.test".into()],
        )
        .unwrap();
        assert_eq!(candidate.keys().len(), 4);
        assert!(
            candidate
                .keys()
                .iter()
                .all(|key| key.component_count() == 2)
        );

        let first = (0..33)
            .map(|value| format!("'email-{value}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let second = (0..33)
            .map(|value| format!("'region-{value}'"))
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(
            infer(
                &catalog,
                &format!("SELECT * FROM users WHERE email IN ({first}) AND region IN ({second})"),
                &[],
            ),
            Err(GlobalIndexInferenceFallback::TooManyKeys)
        );
    }

    #[test]
    fn nonunique_indexes_produce_candidates_while_unsupported_and_invalid_indexes_fall_back() {
        let expression = GlobalIndexKeyPart::new(
            GlobalIndexKeySource::expression("lower(email)").unwrap(),
            GlobalIndexKeyType::Text,
        );
        for index in [
            metadata(
                1,
                "partial",
                vec![column("email", GlobalIndexKeyType::Text)],
                Some("region = 'east'"),
                GlobalIndexLifecycle::Ready,
                true,
            ),
            metadata(
                1,
                "expression",
                vec![expression],
                None,
                GlobalIndexLifecycle::Ready,
                true,
            ),
        ] {
            assert_eq!(
                infer(
                    &catalog(vec![index]),
                    "SELECT * FROM users WHERE email = 'a@example.test'",
                    &[],
                ),
                Err(GlobalIndexInferenceFallback::UnsupportedIndexDefinition)
            );
        }
        let candidate = infer(
            &catalog(vec![metadata(
                1,
                "nonunique",
                vec![column("email", GlobalIndexKeyType::Text)],
                None,
                GlobalIndexLifecycle::Ready,
                false,
            )]),
            "SELECT * FROM users AS u WHERE u.email = 'a@example.test'",
            &[],
        )
        .unwrap();
        assert!(!candidate.is_unique());
        assert_eq!(candidate.query_table_alias(), Some("u"));
        assert_eq!(
            candidate.query_predicate_sql(),
            "u.email = 'a@example.test'"
        );

        assert_eq!(
            infer(
                &catalog(vec![metadata(
                    1,
                    "invalid",
                    vec![column("email", GlobalIndexKeyType::Text)],
                    None,
                    GlobalIndexLifecycle::Invalid,
                    true,
                )]),
                "SELECT * FROM users WHERE email = 'a@example.test'",
                &[],
            ),
            Err(GlobalIndexInferenceFallback::NoReadyAuthoritativeIndex)
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn generated_in_predicates_never_drop_an_exact_value(
            values in proptest::collection::vec("[a-z]{1,8}", 1..32),
            include_null in any::<bool>(),
        ) {
            let catalog = catalog(vec![metadata(
                1,
                "email_unique",
                vec![column("email", GlobalIndexKeyType::Text)],
                None,
                GlobalIndexLifecycle::Ready,
                true,
            )]);
            let mut members = values
                .iter()
                .map(|value| format!("'{value}'"))
                .collect::<Vec<_>>();
            if include_null {
                members.push("NULL".to_owned());
            }
            let source = format!(
                "SELECT * FROM users WHERE email IN ({}) AND tenant_id = tenant_id",
                members.join(", ")
            );
            let candidate = infer(&catalog, &source, &[]).unwrap();
            let actual = candidate
                .keys()
                .iter()
                .map(|key| key.as_bytes().to_vec())
                .collect::<HashSet<_>>();
            for value in values {
                let expected = CanonicalIndexKey::encode_values(&[Value::from(value)]).unwrap();
                prop_assert!(actual.contains(expected.as_bytes()));
            }
        }
    }
}
