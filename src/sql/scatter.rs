//! Structural safety checks for unchanged scatter reads.
//!
//! Executing the same statement independently on every physical shard and
//! concatenating the rows is equivalent to logical `UNION ALL` only when the
//! statement transforms each input row independently. This module deliberately
//! recognizes a narrow shape instead of trying to repair global semantics after
//! execution. A later coordinator can add dedicated merge plans for ordering,
//! aggregation, limits, joins, and other rejected forms.

use sqlparser::ast::{
    Distinct, Expr, GroupByExpr, Select, SelectFlavor, SelectItem, SetExpr,
    Statement as AstStatement, TableFactor, TableWithJoins,
};

use super::TranslatedSql;
use crate::core::{EngineError, EngineErrorKind, EngineResult};

/// Validate that one translated read can run unchanged on every shard and have
/// its result rows concatenated with logical `UNION ALL` semantics.
///
/// The accepted shape is intentionally limited to a single base-table
/// `SELECT` with row-local projections and an optional row-local filter. The
/// caller must still decide that the referenced table is sharded, select the
/// physical shards, enforce deadlines and result limits, and verify schema
/// compatibility. This function grants only structural scatter safety.
pub(crate) fn validate_scatter_safe(sql: &TranslatedSql) -> EngineResult<()> {
    validate_statement_batch(sql.normalized_sql().common().statements())
}

fn validate_statement_batch(statements: &[AstStatement]) -> EngineResult<()> {
    let [statement] = statements else {
        return Err(scatter_unsupported(
            "exactly one top-level statement is required",
        ));
    };
    let AstStatement::Query(query) = statement else {
        return Err(scatter_unsupported("only SELECT statements are supported"));
    };

    if query.with.is_some() {
        return Err(scatter_unsupported(
            "common table expressions and subqueries require a merge plan",
        ));
    }
    if query.order_by.is_some() {
        return Err(scatter_unsupported("ORDER BY requires a global merge plan"));
    }
    if query.limit_clause.is_some() || query.fetch.is_some() {
        return Err(scatter_unsupported(
            "LIMIT, OFFSET, and FETCH require a global merge plan",
        ));
    }
    if !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(scatter_unsupported(
            "query suffix clauses are not row-local",
        ));
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(scatter_unsupported(
            "set operations, nested queries, and VALUES require a merge plan",
        ));
    };
    validate_select(select)
}

fn validate_select(select: &Select) -> EngineResult<()> {
    if matches!(&select.distinct, Some(Distinct::Distinct | Distinct::On(_))) {
        return Err(scatter_unsupported(
            "DISTINCT requires global duplicate elimination",
        ));
    }
    if select.top.is_some() {
        return Err(scatter_unsupported("TOP requires a global merge plan"));
    }
    if !select.optimizer_hints.is_empty()
        || select.select_modifiers.is_some()
        || select.top_before_distinct
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.window_before_qualify
        || select.value_table_mode.is_some()
        || !matches!(select.flavor, SelectFlavor::Standard)
    {
        return Err(scatter_unsupported("SELECT modifiers are not row-local"));
    }
    if select.projection.is_empty() {
        return Err(scatter_unsupported("a projection is required"));
    }

    let [table] = select.from.as_slice() else {
        return Err(scatter_unsupported(
            "exactly one base table is required for a scatter read",
        ));
    };
    validate_base_table(table)?;

    match &select.group_by {
        GroupByExpr::Expressions(expressions, modifiers)
            if expressions.is_empty() && modifiers.is_empty() => {}
        GroupByExpr::Expressions(_, _) | GroupByExpr::All(_) => {
            return Err(scatter_unsupported("GROUP BY requires global aggregation"));
        }
    }
    if select.having.is_some() {
        return Err(scatter_unsupported("HAVING requires global aggregation"));
    }

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expression)
            | SelectItem::ExprWithAlias {
                expr: expression, ..
            } => validate_row_local_expression(expression)?,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
            SelectItem::ExprWithAliases { .. } => {
                return Err(scatter_unsupported(
                    "projection alias lists are not row-local",
                ));
            }
        }
    }
    if let Some(selection) = &select.selection {
        validate_row_local_expression(selection)?;
    }
    Ok(())
}

fn validate_base_table(table: &TableWithJoins) -> EngineResult<()> {
    if !table.joins.is_empty() {
        return Err(scatter_unsupported("joins require a distributed join plan"));
    }
    let TableFactor::Table {
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
        ..
    } = &table.relation
    else {
        return Err(scatter_unsupported(
            "derived tables, subqueries, and table functions are not row-local",
        ));
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return Err(scatter_unsupported(
            "table reference options are not supported by unchanged scatter",
        ));
    }
    if alias
        .as_ref()
        .is_some_and(|alias| !alias.columns.is_empty() || alias.at.is_some())
    {
        return Err(scatter_unsupported(
            "table alias column lists are not row-local",
        ));
    }
    Ok(())
}

fn validate_row_local_expression(expression: &Expr) -> EngineResult<()> {
    match expression {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) => Ok(()),
        Expr::Nested(expression)
        | Expr::IsNull(expression)
        | Expr::IsNotNull(expression)
        | Expr::IsTrue(expression)
        | Expr::IsNotTrue(expression)
        | Expr::IsFalse(expression)
        | Expr::IsNotFalse(expression)
        | Expr::UnaryOp {
            expr: expression, ..
        } => validate_row_local_expression(expression),
        Expr::BinaryOp { left, right, .. } => {
            validate_row_local_expression(left)?;
            validate_row_local_expression(right)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_row_local_expression(expr)?;
            validate_row_local_expression(low)?;
            validate_row_local_expression(high)
        }
        Expr::InList { expr, list, .. } => {
            validate_row_local_expression(expr)?;
            for item in list {
                validate_row_local_expression(item)?;
            }
            Ok(())
        }
        Expr::Like {
            expr,
            pattern,
            any: false,
            escape_char: None,
            ..
        } => {
            validate_row_local_expression(expr)?;
            validate_row_local_expression(pattern)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                validate_row_local_expression(operand)?;
            }
            for branch in conditions {
                validate_row_local_expression(&branch.condition)?;
                validate_row_local_expression(&branch.result)?;
            }
            if let Some(else_result) = else_result {
                validate_row_local_expression(else_result)?;
            }
            Ok(())
        }
        Expr::Function(_) => Err(scatter_unsupported(
            "aggregate, window, and scalar functions need an explicit scatter policy",
        )),
        _ => Err(scatter_unsupported(
            "subqueries and non-row-local expression forms are not supported",
        )),
    }
}

fn scatter_unsupported(feature: &'static str) -> EngineError {
    EngineError::new(
        EngineErrorKind::Unsupported,
        format!("statement is not safe for unchanged scatter execution: {feature}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{
        SqlDialect, SqlTranslationMode, normalize_placeholders, parse, translate_sql,
        validate_common_subset,
    };

    fn translated(dialect: SqlDialect, source: &str) -> TranslatedSql {
        let parsed = parse(dialect, source).unwrap();
        let common = validate_common_subset(parsed).unwrap();
        let normalized = normalize_placeholders(common).unwrap();
        translate_sql(normalized, SqlTranslationMode::Compatibility).unwrap()
    }

    fn validate_raw(dialect: SqlDialect, source: &str) -> EngineResult<()> {
        let parsed = parse(dialect, source)?;
        validate_statement_batch(parsed.statements())
    }

    fn assert_unsupported(result: EngineResult<()>, expected: &str) {
        let error = result.unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Unsupported);
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?} in {error}"
        );
    }

    #[test]
    fn row_local_projection_and_filter_are_safe_in_every_dialect() {
        let cases = [
            (
                SqlDialect::Sqlite,
                "SELECT e.tenant_id, e.tenant_id + ?1 AS adjusted FROM events AS e WHERE e.tenant_id >= ?2 AND e.payload IS NOT NULL",
            ),
            (
                SqlDialect::PostgreSql,
                "SELECT e.tenant_id, e.tenant_id + $1 AS adjusted FROM events AS e WHERE e.tenant_id >= $2 AND e.payload IS NOT NULL",
            ),
            (
                SqlDialect::MySql,
                "SELECT e.tenant_id, e.tenant_id + ? AS adjusted FROM events AS e WHERE e.tenant_id >= ? AND e.payload IS NOT NULL",
            ),
        ];

        for (dialect, source) in cases {
            validate_scatter_safe(&translated(dialect, source))
                .unwrap_or_else(|error| panic!("{dialect} rejected {source}: {error}"));
        }
    }

    #[test]
    fn wildcards_aliases_and_row_local_conditionals_are_safe() {
        for source in [
            "SELECT * FROM events",
            "SELECT ALL tenant_id FROM events",
            "SELECT e.* FROM events AS e WHERE e.tenant_id BETWEEN 2 AND 9",
            "SELECT CASE WHEN payload IS NULL THEN 'missing' ELSE payload END AS display FROM events WHERE tenant_id IN (1, 2, 3)",
            "SELECT tenant_id + 1 AS next_id FROM events WHERE payload LIKE 'ok%'",
        ] {
            validate_scatter_safe(&translated(SqlDialect::Sqlite, source))
                .unwrap_or_else(|error| panic!("rejected {source}: {error}"));
        }
    }

    #[test]
    fn strict_sqlite_translation_is_supported() {
        let parsed = parse(
            SqlDialect::Sqlite,
            "SELECT tenant_id FROM events WHERE tenant_id = ?1",
        )
        .unwrap();
        let common = validate_common_subset(parsed).unwrap();
        let normalized = normalize_placeholders(common).unwrap();
        let translated = translate_sql(normalized, SqlTranslationMode::StrictSqlite).unwrap();

        validate_scatter_safe(&translated).unwrap();
    }

    #[test]
    fn constants_without_a_base_table_are_not_scattered_once_per_shard() {
        assert_unsupported(
            validate_scatter_safe(&translated(SqlDialect::Sqlite, "SELECT 1")),
            "exactly one base table",
        );
    }

    #[test]
    fn multiple_statements_and_non_queries_are_rejected() {
        assert_unsupported(
            validate_scatter_safe(&translated(
                SqlDialect::Sqlite,
                "SELECT id FROM events; SELECT id FROM events",
            )),
            "exactly one top-level statement",
        );
        assert_unsupported(
            validate_scatter_safe(&translated(
                SqlDialect::Sqlite,
                "UPDATE events SET payload = 'changed' WHERE tenant_id = 7",
            )),
            "only SELECT",
        );
    }

    #[test]
    fn distinct_and_aggregates_are_rejected() {
        for (source, expected) in [
            ("SELECT DISTINCT payload FROM events", "DISTINCT"),
            ("SELECT COUNT(*) FROM events", "aggregate"),
            ("SELECT SUM(tenant_id) FROM events", "aggregate"),
            ("SELECT MIN(payload) FROM events", "aggregate"),
        ] {
            assert_unsupported(
                validate_scatter_safe(&translated(SqlDialect::Sqlite, source)),
                expected,
            );
        }
    }

    #[test]
    fn grouping_and_having_are_rejected() {
        assert_unsupported(
            validate_scatter_safe(&translated(
                SqlDialect::Sqlite,
                "SELECT payload FROM events GROUP BY payload",
            )),
            "GROUP BY",
        );
        assert_unsupported(
            validate_scatter_safe(&translated(
                SqlDialect::Sqlite,
                "SELECT COUNT(*) FROM events HAVING COUNT(*) > 1",
            )),
            "HAVING",
        );
    }

    #[test]
    fn ordering_and_every_row_cap_form_are_rejected() {
        for (dialect, source, expected) in [
            (
                SqlDialect::Sqlite,
                "SELECT payload FROM events ORDER BY payload",
                "ORDER BY",
            ),
            (
                SqlDialect::Sqlite,
                "SELECT payload FROM events LIMIT 5",
                "LIMIT",
            ),
            (
                SqlDialect::Sqlite,
                "SELECT payload FROM events LIMIT 5 OFFSET 2",
                "LIMIT",
            ),
            (
                SqlDialect::MySql,
                "SELECT payload FROM events LIMIT 2, 5",
                "LIMIT",
            ),
        ] {
            assert_unsupported(
                validate_scatter_safe(&translated(dialect, source)),
                expected,
            );
        }
        assert_unsupported(
            validate_raw(
                SqlDialect::PostgreSql,
                "SELECT payload FROM events OFFSET 2 ROWS FETCH FIRST 5 ROWS ONLY",
            ),
            "LIMIT",
        );
    }

    #[test]
    fn joins_multiple_tables_and_derived_tables_are_rejected() {
        for (source, expected) in [
            (
                "SELECT e.id FROM events e JOIN tenants t ON t.id = e.tenant_id",
                "joins",
            ),
            (
                "SELECT events.id FROM events, tenants WHERE events.tenant_id = tenants.id",
                "exactly one base table",
            ),
            (
                "SELECT nested.id FROM (SELECT id FROM events) AS nested",
                "derived tables",
            ),
        ] {
            assert_unsupported(validate_raw(SqlDialect::Sqlite, source), expected);
        }
    }

    #[test]
    fn ctes_subqueries_and_set_operations_are_rejected() {
        for (source, expected) in [
            (
                "WITH selected AS (SELECT id FROM events) SELECT id FROM selected",
                "common table expressions",
            ),
            (
                "SELECT id FROM events WHERE tenant_id IN (SELECT id FROM tenants)",
                "subqueries",
            ),
            (
                "SELECT id FROM events UNION ALL SELECT id FROM archived_events",
                "set operations",
            ),
        ] {
            assert_unsupported(validate_raw(SqlDialect::Sqlite, source), expected);
        }
    }

    #[test]
    fn window_functions_and_scalar_functions_are_conservatively_rejected() {
        assert_unsupported(
            validate_raw(
                SqlDialect::PostgreSql,
                "SELECT row_number() OVER (ORDER BY id) FROM events",
            ),
            "function",
        );
        assert_unsupported(
            validate_raw(SqlDialect::Sqlite, "SELECT lower(payload) FROM events"),
            "function",
        );
    }
}
