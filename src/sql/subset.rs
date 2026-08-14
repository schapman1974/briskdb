use std::{collections::HashSet, fmt};

use sqlparser::ast::{
    AssignmentTarget, BeginTransactionKind, BinaryOperator, CheckConstraint, ColumnOption,
    CreateIndex, CreateTable, CreateTableOptions, DataType, Delete, Distinct, Expr, FromTable,
    Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, HiveDistributionStyle,
    IndexColumn, Insert, LimitClause, NullsDistinctOption, ObjectName, ObjectNamePart, OrderBy,
    OrderByExpr, OrderByKind, PrimaryKeyConstraint, Query, Select, SelectFlavor, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, Statement as AstStatement, TableConstraint,
    TableFactor, TableObject, TableWithJoins, UnaryOperator, UniqueConstraint, Update, Value,
    WildcardAdditionalOptions,
};
use sqlparser::tokenizer::Span;

use super::{ParsedSql, SqlDialect, generated};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

/// Maximum recursive expression depth accepted by common-subset validation.
///
/// This is independent of parser recursion: a long, flat operator chain can
/// parse iteratively but still produce a deeply nested AST.
pub const MAX_COMMON_SQL_EXPRESSION_DEPTH: usize = 128;

/// SQL that has passed BriskDB's protocol-neutral structural subset validator.
///
/// Validation does not normalize placeholders, translate types, infer a shard,
/// plan execution, or decide whether an empty or multi-statement request may
/// execute. Those remain separate layers over this opaque marker type.
#[derive(Clone, PartialEq, Eq)]
pub struct CommonSql {
    parsed: ParsedSql,
    statement_placeholders: Vec<Vec<CommonPlaceholder>>,
}

impl CommonSql {
    /// Return the explicitly selected source dialect.
    pub const fn dialect(&self) -> SqlDialect {
        self.parsed.dialect()
    }

    /// Return the caller's byte-exact SQL source.
    pub fn source(&self) -> &str {
        self.parsed.source()
    }

    /// Return the number of independently validated statements.
    pub fn statement_count(&self) -> usize {
        self.parsed.statement_count()
    }

    /// Return whether the parsed input contained no statements.
    pub fn is_empty(&self) -> bool {
        self.parsed.is_empty()
    }

    pub(super) fn statement_placeholders(&self) -> &[Vec<CommonPlaceholder>] {
        &self.statement_placeholders
    }

    pub(super) fn statements(&self) -> &[AstStatement] {
        self.parsed.statements()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct CommonPlaceholder {
    pub(super) marker: String,
    pub(super) span: Span,
}

impl fmt::Debug for CommonSql {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommonSql")
            .field("dialect", &self.dialect())
            .field("source_bytes", &self.source().len())
            .field("statement_count", &self.statement_count())
            .finish()
    }
}

/// Validate every parsed statement against BriskDB's first common SQL subset.
///
/// The batch is validated atomically and retains its exact source and order.
/// Empty batches and otherwise-valid statement combinations are accepted here;
/// [`super::classify_statements`] separately owns request-level batch policy.
pub fn validate_common_subset(parsed: ParsedSql) -> EngineResult<CommonSql> {
    let mut statement_placeholders = Vec::with_capacity(parsed.statement_count());
    for (index, statement) in parsed.statements().iter().enumerate() {
        let mut validation = ValidationState::default();
        validate_statement(statement, parsed.dialect(), index, &mut validation).map_err(
            |violation| {
                let diagnostic = if violation.kind == EngineErrorKind::LimitExceeded {
                    format!(
                        "statement {} exceeds the common SQL expression depth limit of {}",
                        index + 1,
                        MAX_COMMON_SQL_EXPRESSION_DEPTH
                    )
                } else {
                    format!(
                        "statement {} is outside the common SQL subset: {}",
                        index + 1,
                        violation.feature
                    )
                };
                EngineError::new(violation.kind, diagnostic)
            },
        )?;
        statement_placeholders.push(validation.placeholders);
    }

    Ok(CommonSql {
        parsed,
        statement_placeholders,
    })
}

#[derive(Default)]
struct ValidationState {
    placeholders: Vec<CommonPlaceholder>,
}

impl ValidationState {
    fn record_placeholder(&mut self, marker: &str, span: Span) {
        self.placeholders.push(CommonPlaceholder {
            marker: marker.to_owned(),
            span,
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SubsetViolation {
    kind: EngineErrorKind,
    feature: &'static str,
}

type SubsetResult<T = ()> = Result<T, SubsetViolation>;

const fn unsupported<T>(feature: &'static str) -> SubsetResult<T> {
    Err(SubsetViolation {
        kind: EngineErrorKind::Unsupported,
        feature,
    })
}

const fn expression_depth_exceeded<T>() -> SubsetResult<T> {
    Err(SubsetViolation {
        kind: EngineErrorKind::LimitExceeded,
        feature: "expression depth",
    })
}

fn validate_statement(
    statement: &AstStatement,
    dialect: SqlDialect,
    statement_index: usize,
    validation: &mut ValidationState,
) -> SubsetResult {
    match statement {
        AstStatement::CreateTable(table) => validate_create_table(table, dialect, statement_index),
        AstStatement::CreateIndex(index) => validate_create_index(index),
        AstStatement::Query(query) => validate_query(query, validation),
        AstStatement::Insert(insert) => validate_insert(insert, validation),
        AstStatement::Update(update) => validate_update(update, validation),
        AstStatement::Delete(delete) => validate_delete(delete, validation),
        AstStatement::StartTransaction {
            modes,
            begin,
            transaction,
            modifier,
            statements,
            exception,
            has_end_keyword,
        } => {
            if !begin {
                return unsupported("START TRANSACTION");
            }
            if !modes.is_empty()
                || modifier.is_some()
                || !statements.is_empty()
                || exception.is_some()
                || *has_end_keyword
            {
                return unsupported("transaction modes or procedural blocks");
            }
            if matches!(transaction, Some(BeginTransactionKind::Tran)) {
                return unsupported("BEGIN TRAN");
            }
            Ok(())
        }
        AstStatement::Commit {
            chain,
            end,
            modifier,
        } if !chain && !end && modifier.is_none() => Ok(()),
        AstStatement::Commit { .. } => unsupported("COMMIT modifiers"),
        AstStatement::Rollback { chain, savepoint } if !chain && savepoint.is_none() => Ok(()),
        AstStatement::Rollback { .. } => unsupported("ROLLBACK modifiers or savepoints"),
        _ => unsupported("statement type"),
    }
}

fn validate_create_table(
    table: &CreateTable,
    dialect: SqlDialect,
    statement_index: usize,
) -> SubsetResult {
    let CreateTable {
        or_replace,
        temporary,
        external,
        dynamic,
        global,
        if_not_exists: _,
        transient,
        volatile,
        iceberg,
        snapshot,
        name,
        columns,
        constraints,
        hive_distribution,
        hive_formats,
        table_options,
        file_format,
        location,
        query,
        without_rowid,
        like,
        clone,
        version,
        comment,
        on_commit,
        on_cluster,
        primary_key,
        order_by,
        partition_by,
        cluster_by,
        clustered_by,
        inherits,
        partition_of,
        for_values,
        strict,
        copy_grants,
        enable_schema_evolution,
        change_tracking,
        data_retention_time_in_days,
        max_data_extension_time_in_days,
        default_ddl_collation,
        with_aggregation_policy,
        with_row_access_policy,
        with_storage_lifecycle_policy,
        with_tags,
        external_volume,
        base_location,
        catalog,
        catalog_sync,
        storage_serialization_policy,
        target_lag,
        warehouse,
        refresh_mode,
        initialize,
        require_user,
        diststyle,
        distkey,
        sortkey,
        backup,
    } = table;

    if *or_replace
        || *temporary
        || *external
        || *dynamic
        || global.is_some()
        || *transient
        || *volatile
        || *iceberg
        || *snapshot
    {
        return unsupported("CREATE TABLE modifiers");
    }
    if !matches!(hive_distribution, HiveDistributionStyle::NONE)
        || hive_formats.is_some()
        || !matches!(table_options, CreateTableOptions::None)
        || file_format.is_some()
        || location.is_some()
    {
        return unsupported("CREATE TABLE storage options");
    }
    if query.is_some()
        || like.is_some()
        || clone.is_some()
        || version.is_some()
        || comment.is_some()
        || on_commit.is_some()
        || on_cluster.is_some()
    {
        return unsupported("CREATE TABLE source or lifecycle clauses");
    }
    if *without_rowid || *strict {
        return unsupported("SQLite-specific CREATE TABLE options");
    }
    if primary_key.is_some()
        || order_by.is_some()
        || partition_by.is_some()
        || cluster_by.is_some()
        || clustered_by.is_some()
        || inherits.is_some()
        || partition_of.is_some()
        || for_values.is_some()
    {
        return unsupported("CREATE TABLE partitioning or inheritance");
    }
    if *copy_grants
        || enable_schema_evolution.is_some()
        || change_tracking.is_some()
        || data_retention_time_in_days.is_some()
        || max_data_extension_time_in_days.is_some()
        || default_ddl_collation.is_some()
        || with_aggregation_policy.is_some()
        || with_row_access_policy.is_some()
        || with_storage_lifecycle_policy.is_some()
        || with_tags.is_some()
        || external_volume.is_some()
        || base_location.is_some()
        || catalog.is_some()
        || catalog_sync.is_some()
        || storage_serialization_policy.is_some()
        || target_lag.is_some()
        || warehouse.is_some()
        || refresh_mode.is_some()
        || initialize.is_some()
        || *require_user
        || diststyle.is_some()
        || distkey.is_some()
        || sortkey.is_some()
        || backup.is_some()
    {
        return unsupported("vendor-specific CREATE TABLE options");
    }

    validate_one_part_name(name, "qualified table names")?;
    if columns.is_empty() {
        return unsupported("CREATE TABLE without columns");
    }
    let generated =
        generated::analyze_create_table(dialect, statement_index, table).map_err(|()| {
            SubsetViolation {
                kind: EngineErrorKind::Unsupported,
                feature: "generated-key declaration",
            }
        })?;
    for (column_index, column) in columns.iter().enumerate() {
        if column.data_type == DataType::Unspecified {
            return unsupported("columns without an explicit type");
        }
        for (option_index, option) in column.options.iter().enumerate() {
            if generated.as_ref().is_some_and(|generated| {
                generated.owns_generated_option(column_index, option_index)
            }) {
                continue;
            }
            validate_column_option(option.name.is_some(), &option.option)?;
        }
    }
    for constraint in constraints {
        validate_table_constraint(constraint)?;
    }
    Ok(())
}

fn validate_column_option(named: bool, option: &ColumnOption) -> SubsetResult {
    match option {
        ColumnOption::Null | ColumnOption::NotNull if !named => Ok(()),
        ColumnOption::Default(expression) if !named => validate_default_expression(expression),
        ColumnOption::PrimaryKey(constraint) => validate_primary_key(constraint, true),
        ColumnOption::Unique(constraint) => validate_unique(constraint, true),
        ColumnOption::Check(constraint) => validate_check(constraint),
        ColumnOption::Null | ColumnOption::NotNull | ColumnOption::Default(_) => {
            unsupported("named NULL or DEFAULT column options")
        }
        _ => unsupported("column option"),
    }
}

fn validate_table_constraint(constraint: &TableConstraint) -> SubsetResult {
    match constraint {
        TableConstraint::PrimaryKey(constraint) => validate_primary_key(constraint, false),
        TableConstraint::Unique(constraint) => validate_unique(constraint, false),
        TableConstraint::Check(constraint) => validate_check(constraint),
        TableConstraint::ForeignKey(_) => unsupported("foreign keys"),
        _ => unsupported("table constraint"),
    }
}

fn validate_primary_key(constraint: &PrimaryKeyConstraint, allow_empty: bool) -> SubsetResult {
    if constraint.index_name.is_some()
        || constraint.index_type.is_some()
        || !constraint.index_options.is_empty()
        || constraint.characteristics.is_some()
    {
        return unsupported("PRIMARY KEY options");
    }
    if constraint.columns.is_empty() && !allow_empty {
        return unsupported("PRIMARY KEY without columns");
    }
    for column in &constraint.columns {
        validate_index_column(column)?;
    }
    Ok(())
}

fn validate_unique(constraint: &UniqueConstraint, allow_empty: bool) -> SubsetResult {
    if constraint.index_name.is_some()
        || !constraint.index_type_display.is_none()
        || constraint.index_type.is_some()
        || !constraint.index_options.is_empty()
        || constraint.characteristics.is_some()
        || !matches!(constraint.nulls_distinct, NullsDistinctOption::None)
    {
        return unsupported("UNIQUE constraint options");
    }
    if constraint.columns.is_empty() && !allow_empty {
        return unsupported("UNIQUE constraint without columns");
    }
    for column in &constraint.columns {
        validate_index_column(column)?;
    }
    Ok(())
}

fn validate_check(constraint: &CheckConstraint) -> SubsetResult {
    if constraint.enforced.is_some() {
        return unsupported("CHECK enforcement modifiers");
    }
    validate_expression(
        constraint.expr.as_ref(),
        ExprContext::CHECK,
        &mut ValidationState::default(),
    )
}

fn validate_create_index(index: &CreateIndex) -> SubsetResult {
    let Some(name) = &index.name else {
        return unsupported("unnamed indexes");
    };
    validate_one_part_name(name, "qualified index names")?;
    validate_one_part_name(&index.table_name, "qualified table names")?;
    if index.columns.is_empty() {
        return unsupported("indexes without columns");
    }
    if index.using.is_some()
        || index.concurrently
        || !index.include.is_empty()
        || index.nulls_distinct.is_some()
        || !index.with.is_empty()
        || index.predicate.is_some()
        || !index.index_options.is_empty()
        || !index.alter_options.is_empty()
    {
        return unsupported("CREATE INDEX options");
    }
    for column in &index.columns {
        validate_index_column(column)?;
    }
    Ok(())
}

fn validate_index_column(column: &IndexColumn) -> SubsetResult {
    if column.operator_class.is_some()
        || column.column.with_fill.is_some()
        || column.column.options.nulls_first.is_some()
    {
        return unsupported("index column options");
    }
    if !matches!(column.column.expr, Expr::Identifier(_)) {
        return unsupported("expression indexes");
    }
    Ok(())
}

fn validate_query(query: &Query, validation: &mut ValidationState) -> SubsetResult {
    let Query {
        with,
        body,
        order_by,
        limit_clause,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
        pipe_operators,
    } = query;

    if with.is_some() {
        return unsupported("common table expressions");
    }
    if fetch.is_some()
        || !locks.is_empty()
        || for_clause.is_some()
        || settings.is_some()
        || format_clause.is_some()
        || !pipe_operators.is_empty()
    {
        return unsupported("query suffix clauses");
    }

    let SetExpr::Select(select) = body.as_ref() else {
        return unsupported("set operations, VALUES, or nested queries");
    };
    validate_select(select, validation)?;
    if let Some(order_by) = order_by {
        validate_order_by(order_by, validation)?;
    }
    if let Some(limit_clause) = limit_clause {
        validate_limit_clause(limit_clause, validation)?;
    }
    Ok(())
}

fn validate_select(select: &Select, validation: &mut ValidationState) -> SubsetResult {
    let Select {
        select_token: _,
        optimizer_hints,
        distinct,
        select_modifiers,
        top,
        top_before_distinct,
        projection,
        exclude,
        into,
        from,
        lateral_views,
        prewhere,
        selection,
        connect_by,
        group_by,
        cluster_by,
        distribute_by,
        sort_by,
        having,
        named_window,
        qualify,
        window_before_qualify,
        value_table_mode,
        flavor,
    } = select;

    if !optimizer_hints.is_empty()
        || select_modifiers.is_some()
        || top.is_some()
        || *top_before_distinct
        || exclude.is_some()
        || into.is_some()
        || !lateral_views.is_empty()
        || prewhere.is_some()
        || !connect_by.is_empty()
        || !cluster_by.is_empty()
        || !distribute_by.is_empty()
        || !sort_by.is_empty()
        || !named_window.is_empty()
        || qualify.is_some()
        || *window_before_qualify
        || value_table_mode.is_some()
        || !matches!(flavor, SelectFlavor::Standard)
    {
        return unsupported("SELECT modifiers");
    }
    if matches!(distinct, Some(Distinct::On(_))) {
        return unsupported("DISTINCT ON");
    }
    if projection.is_empty() {
        return unsupported("SELECT without a projection");
    }
    if from.len() > 1 {
        return unsupported("multiple FROM tables");
    }
    if let Some(table) = from.first() {
        validate_simple_table(table, true)?;
    }
    for item in projection {
        validate_select_item(item, validation)?;
    }
    if let Some(selection) = selection {
        validate_expression(selection, ExprContext::SCALAR, validation)?;
    }
    match group_by {
        GroupByExpr::Expressions(expressions, modifiers) if modifiers.is_empty() => {
            for expression in expressions {
                validate_expression(expression, ExprContext::SCALAR, validation)?;
            }
        }
        GroupByExpr::Expressions(_, _) | GroupByExpr::All(_) => {
            return unsupported("GROUP BY modifiers");
        }
    }
    if let Some(having) = having {
        validate_expression(having, ExprContext::SELECT, validation)?;
    }
    Ok(())
}

fn validate_select_item(item: &SelectItem, validation: &mut ValidationState) -> SubsetResult {
    match item {
        SelectItem::UnnamedExpr(expression) => {
            validate_expression(expression, ExprContext::SELECT, validation)
        }
        SelectItem::ExprWithAlias { expr, alias: _ } => {
            validate_expression(expr, ExprContext::SELECT, validation)
        }
        SelectItem::Wildcard(options) if wildcard_options_are_empty(options) => Ok(()),
        SelectItem::QualifiedWildcard(
            SelectItemQualifiedWildcardKind::ObjectName(name),
            options,
        ) if wildcard_options_are_empty(options) => {
            validate_one_part_name(name, "qualified wildcard prefixes")
        }
        SelectItem::ExprWithAliases { .. }
        | SelectItem::QualifiedWildcard(_, _)
        | SelectItem::Wildcard(_) => unsupported("projection aliases or wildcard modifiers"),
    }
}

fn wildcard_options_are_empty(options: &WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_none()
        && options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_replace.is_none()
        && options.opt_rename.is_none()
        && options.opt_alias.is_none()
}

fn validate_order_by(order_by: &OrderBy, validation: &mut ValidationState) -> SubsetResult {
    if order_by.interpolate.is_some() {
        return unsupported("ORDER BY interpolation");
    }
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return unsupported("ORDER BY ALL");
    };
    for expression in expressions {
        validate_order_by_expression(expression, validation)?;
    }
    Ok(())
}

fn validate_order_by_expression(
    order_by: &OrderByExpr,
    validation: &mut ValidationState,
) -> SubsetResult {
    if order_by.with_fill.is_some() || order_by.options.nulls_first.is_some() {
        return unsupported("ORDER BY options");
    }
    validate_expression(&order_by.expr, ExprContext::SELECT, validation)
}

fn validate_limit_clause(
    limit_clause: &LimitClause,
    validation: &mut ValidationState,
) -> SubsetResult {
    match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            let Some(limit) = limit else {
                return unsupported("LIMIT ALL");
            };
            if !limit_by.is_empty() {
                return unsupported("LIMIT BY");
            }
            validate_limit_value(limit, validation)?;
            if let Some(offset) = offset {
                validate_limit_value(&offset.value, validation)?;
            }
        }
        LimitClause::OffsetCommaLimit { offset, limit } => {
            validate_limit_value(offset, validation)?;
            validate_limit_value(limit, validation)?;
        }
    }
    Ok(())
}

fn validate_insert(insert: &Insert, validation: &mut ValidationState) -> SubsetResult {
    let Insert {
        insert_token: _,
        optimizer_hints,
        or,
        ignore,
        into,
        table,
        table_alias,
        columns,
        overwrite,
        source,
        assignments,
        partitioned,
        after_columns,
        has_table_keyword,
        on,
        returning,
        output,
        replace_into,
        priority,
        insert_alias,
        settings,
        format_clause,
        multi_table_insert_type,
        multi_table_into_clauses,
        multi_table_when_clauses,
        multi_table_else_clause,
    } = insert;

    if !optimizer_hints.is_empty()
        || or.is_some()
        || *ignore
        || !into
        || table_alias.is_some()
        || *overwrite
        || !assignments.is_empty()
        || partitioned.is_some()
        || !after_columns.is_empty()
        || *has_table_keyword
        || on.is_some()
        || returning.is_some()
        || output.is_some()
        || *replace_into
        || priority.is_some()
        || insert_alias.is_some()
        || settings.is_some()
        || format_clause.is_some()
        || multi_table_insert_type.is_some()
        || !multi_table_into_clauses.is_empty()
        || !multi_table_when_clauses.is_empty()
        || multi_table_else_clause.is_some()
    {
        return unsupported("INSERT modifiers");
    }
    let TableObject::TableName(table_name) = table else {
        return unsupported("INSERT targets other than a named table");
    };
    validate_one_part_name(table_name, "qualified table names")?;
    if columns.is_empty() {
        return unsupported("INSERT without an explicit column list");
    }
    let mut seen = HashSet::new();
    for column in columns {
        let key = common_identifier_key(column, "qualified INSERT columns")?;
        if !seen.insert(key) {
            return unsupported("duplicate INSERT columns");
        }
    }
    let Some(source) = source else {
        return unsupported("INSERT without VALUES");
    };
    validate_insert_values(source, columns.len(), validation)
}

fn validate_insert_values(
    query: &Query,
    column_count: usize,
    validation: &mut ValidationState,
) -> SubsetResult {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return unsupported("INSERT source query clauses");
    }
    let SetExpr::Values(values) = query.body.as_ref() else {
        return unsupported("INSERT sources other than VALUES");
    };
    if values.explicit_row || values.value_keyword || values.rows.is_empty() {
        return unsupported("VALUES modifiers or empty VALUES");
    }
    for row in &values.rows {
        if row.len() != column_count {
            return unsupported("INSERT row width mismatch");
        }
        for expression in row.iter() {
            validate_expression(expression, ExprContext::INSERT_VALUE, validation)?;
        }
    }
    Ok(())
}

fn validate_update(update: &Update, validation: &mut ValidationState) -> SubsetResult {
    if !update.optimizer_hints.is_empty()
        || update.from.is_some()
        || update.returning.is_some()
        || update.output.is_some()
        || update.or.is_some()
        || !update.order_by.is_empty()
        || update.limit.is_some()
    {
        return unsupported("UPDATE modifiers");
    }
    validate_simple_table(&update.table, true)?;
    if update.assignments.is_empty() {
        return unsupported("UPDATE without assignments");
    }
    let mut seen = HashSet::new();
    for assignment in &update.assignments {
        let AssignmentTarget::ColumnName(column) = &assignment.target else {
            return unsupported("tuple UPDATE assignments");
        };
        let key = common_identifier_key(column, "qualified UPDATE assignment targets")?;
        if !seen.insert(key) {
            return unsupported("duplicate UPDATE assignment targets");
        }
        validate_expression(&assignment.value, ExprContext::SCALAR, validation)?;
    }
    if let Some(selection) = &update.selection {
        validate_expression(selection, ExprContext::SCALAR, validation)?;
    }
    Ok(())
}

fn validate_delete(delete: &Delete, validation: &mut ValidationState) -> SubsetResult {
    if !delete.optimizer_hints.is_empty()
        || !delete.tables.is_empty()
        || delete.using.is_some()
        || delete.returning.is_some()
        || delete.output.is_some()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
    {
        return unsupported("DELETE modifiers");
    }
    let FromTable::WithFromKeyword(tables) = &delete.from else {
        return unsupported("DELETE without FROM");
    };
    let [table] = tables.as_slice() else {
        return unsupported("DELETE with multiple tables");
    };
    validate_simple_table(table, true)?;
    if let Some(selection) = &delete.selection {
        validate_expression(selection, ExprContext::SCALAR, validation)?;
    }
    Ok(())
}

fn validate_simple_table(table: &TableWithJoins, allow_alias: bool) -> SubsetResult {
    if !table.joins.is_empty() {
        return unsupported("joins");
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = &table.relation
    else {
        return unsupported("derived tables or table functions");
    };
    validate_one_part_name(name, "qualified table names")?;
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return unsupported("table reference options");
    }
    if let Some(alias) = alias {
        if !allow_alias || !alias.columns.is_empty() || alias.at.is_some() {
            return unsupported("table alias options");
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct ExprContext {
    identifiers: bool,
    placeholders: bool,
    aggregates: bool,
}

impl ExprContext {
    const SCALAR: Self = Self {
        identifiers: true,
        placeholders: true,
        aggregates: false,
    };
    const SELECT: Self = Self {
        aggregates: true,
        ..Self::SCALAR
    };
    const INSERT_VALUE: Self = Self {
        identifiers: false,
        ..Self::SCALAR
    };
    const CHECK: Self = Self {
        placeholders: false,
        ..Self::SCALAR
    };

    const fn without_aggregates(self) -> Self {
        Self {
            aggregates: false,
            ..self
        }
    }
}

fn validate_expression(
    expression: &Expr,
    context: ExprContext,
    validation: &mut ValidationState,
) -> SubsetResult {
    validate_expression_at_depth(expression, context, 1, validation)
}

fn validate_expression_at_depth(
    expression: &Expr,
    context: ExprContext,
    depth: usize,
    validation: &mut ValidationState,
) -> SubsetResult {
    if depth > MAX_COMMON_SQL_EXPRESSION_DEPTH {
        return expression_depth_exceeded();
    }
    let child_depth = depth + 1;

    match expression {
        Expr::Identifier(_) if context.identifiers => Ok(()),
        Expr::CompoundIdentifier(parts) if context.identifiers && parts.len() == 2 => Ok(()),
        Expr::Value(value) => {
            validate_value(&value.value, context.placeholders)?;
            if let Value::Placeholder(marker) = &value.value {
                validation.record_placeholder(marker, value.span);
            }
            Ok(())
        }
        Expr::Nested(expression)
        | Expr::IsNull(expression)
        | Expr::IsNotNull(expression)
        | Expr::IsTrue(expression)
        | Expr::IsNotTrue(expression)
        | Expr::IsFalse(expression)
        | Expr::IsNotFalse(expression) => {
            validate_expression_at_depth(expression, context, child_depth, validation)
        }
        Expr::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus | UnaryOperator::Not,
            expr,
        } => validate_expression_at_depth(expr, context, child_depth, validation),
        Expr::BinaryOp { left, op, right } if binary_operator_is_common(op) => {
            validate_expression_at_depth(left, context, child_depth, validation)?;
            validate_expression_at_depth(right, context, child_depth, validation)
        }
        Expr::Between {
            expr,
            negated: _,
            low,
            high,
        } => {
            validate_expression_at_depth(expr, context, child_depth, validation)?;
            validate_expression_at_depth(low, context, child_depth, validation)?;
            validate_expression_at_depth(high, context, child_depth, validation)
        }
        Expr::InList {
            expr,
            list,
            negated: _,
        } if !list.is_empty() => {
            validate_expression_at_depth(expr, context, child_depth, validation)?;
            for item in list {
                validate_expression_at_depth(item, context, child_depth, validation)?;
            }
            Ok(())
        }
        Expr::Like {
            any,
            expr,
            pattern,
            escape_char,
            negated: _,
        } if !any && escape_char.is_none() => {
            validate_expression_at_depth(expr, context, child_depth, validation)?;
            validate_expression_at_depth(pattern, context, child_depth, validation)
        }
        Expr::Case {
            case_token: _,
            end_token: _,
            operand,
            conditions,
            else_result,
        } => {
            if let Some(operand) = operand {
                validate_expression_at_depth(operand, context, child_depth, validation)?;
            }
            for branch in conditions {
                validate_expression_at_depth(&branch.condition, context, child_depth, validation)?;
                validate_expression_at_depth(&branch.result, context, child_depth, validation)?;
            }
            if let Some(else_result) = else_result {
                validate_expression_at_depth(else_result, context, child_depth, validation)?;
            }
            Ok(())
        }
        Expr::Function(function) if context.aggregates => validate_aggregate(
            function,
            context.without_aggregates(),
            child_depth,
            validation,
        ),
        _ => unsupported("expression form"),
    }
}

fn validate_value(value: &Value, allow_placeholder: bool) -> SubsetResult {
    match value {
        Value::Number(_, false)
        | Value::SingleQuotedString(_)
        | Value::Boolean(_)
        | Value::Null => Ok(()),
        Value::Placeholder(_) if allow_placeholder => Ok(()),
        Value::Placeholder(_) => unsupported("placeholders in schema expressions"),
        _ => unsupported("literal form"),
    }
}

fn binary_operator_is_common(operator: &BinaryOperator) -> bool {
    matches!(
        operator,
        BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
            | BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::And
            | BinaryOperator::Or
    )
}

fn validate_aggregate(
    function: &Function,
    argument_context: ExprContext,
    argument_depth: usize,
    validation: &mut ValidationState,
) -> SubsetResult {
    let [ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else {
        return unsupported("qualified aggregate functions");
    };
    if name.quote_style.is_some() || function.uses_odbc_syntax {
        return unsupported("quoted or ODBC aggregate functions");
    }
    let aggregate = name.value.to_ascii_uppercase();
    if !matches!(aggregate.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
        return unsupported("scalar or unknown functions");
    }
    if !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported("aggregate function modifiers");
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return unsupported("aggregate function arguments");
    };
    if !arguments.clauses.is_empty() || arguments.args.len() != 1 {
        return unsupported("aggregate function arguments");
    }
    let FunctionArg::Unnamed(argument) = &arguments.args[0] else {
        return unsupported("named aggregate arguments");
    };
    match argument {
        FunctionArgExpr::Expr(expression) => {
            validate_expression_at_depth(expression, argument_context, argument_depth, validation)
        }
        FunctionArgExpr::Wildcard
            if aggregate == "COUNT" && arguments.duplicate_treatment.is_none() =>
        {
            Ok(())
        }
        FunctionArgExpr::QualifiedWildcard(_)
        | FunctionArgExpr::Wildcard
        | FunctionArgExpr::WildcardWithOptions(_) => unsupported("aggregate wildcard arguments"),
    }
}

fn validate_default_expression(expression: &Expr) -> SubsetResult {
    let mut expression = expression;
    loop {
        match expression {
            Expr::Nested(inner) => expression = inner,
            Expr::Value(value) => return validate_value(&value.value, false),
            Expr::UnaryOp {
                op: UnaryOperator::Plus | UnaryOperator::Minus,
                expr,
            } if is_numeric_literal(expr) => return Ok(()),
            _ => return unsupported("DEFAULT expressions other than literals"),
        }
    }
}

fn is_numeric_literal(expression: &Expr) -> bool {
    let mut expression = expression;
    loop {
        match expression {
            Expr::Nested(inner) => expression = inner,
            Expr::Value(value) => return matches!(value.value, Value::Number(_, false)),
            _ => return false,
        }
    }
}

fn validate_limit_value(expression: &Expr, validation: &mut ValidationState) -> SubsetResult {
    match expression {
        Expr::Value(value) => match &value.value {
            Value::Number(number, false) if number.bytes().all(|byte| byte.is_ascii_digit()) => {
                Ok(())
            }
            Value::Placeholder(marker) => {
                validation.record_placeholder(marker, value.span);
                Ok(())
            }
            _ => unsupported("LIMIT or OFFSET value"),
        },
        _ => unsupported("LIMIT or OFFSET value"),
    }
}

fn validate_one_part_name(name: &ObjectName, feature: &'static str) -> SubsetResult {
    if matches!(name.0.as_slice(), [ObjectNamePart::Identifier(_)]) {
        Ok(())
    } else {
        unsupported(feature)
    }
}

fn common_identifier_key(name: &ObjectName, feature: &'static str) -> SubsetResult<String> {
    let [ObjectNamePart::Identifier(identifier)] = name.0.as_slice() else {
        return unsupported(feature);
    };
    Ok(identifier.value.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use crate::sql::{MAX_PARSED_SQL_BYTES, parse};

    fn validate(dialect: SqlDialect, source: &str) -> EngineResult<CommonSql> {
        validate_common_subset(parse(dialect, source)?)
    }

    fn assert_supported(dialect: SqlDialect, source: &str) {
        validate(dialect, source)
            .unwrap_or_else(|error| panic!("{dialect} rejected {source}: {error}"));
    }

    fn assert_unsupported(dialect: SqlDialect, source: &str) {
        let error = match validate(dialect, source) {
            Ok(_) => panic!("{dialect} unexpectedly accepted {source}"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), EngineErrorKind::Unsupported, "{source}");
    }

    #[test]
    fn every_statement_family_has_a_common_form_in_all_source_dialects() {
        let common = [
            "CREATE TABLE IF NOT EXISTS widgets(id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL, score INTEGER DEFAULT 0, UNIQUE (tenant_id), CHECK (score >= 0))",
            "CREATE UNIQUE INDEX widgets_tenant ON widgets(tenant_id, score DESC)",
            "SELECT tenant_id, COUNT(*) AS total FROM widgets WHERE score BETWEEN 1 AND 9 AND tenant_id IS NOT NULL GROUP BY tenant_id HAVING COUNT(*) > 0 ORDER BY total DESC LIMIT 10 OFFSET 2",
            "SELECT ALL tenant_id FROM widgets",
            "SELECT DISTINCT tenant_id FROM widgets",
            "UPDATE widgets SET score = score + 1 WHERE tenant_id = 7",
            "DELETE FROM widgets WHERE tenant_id = 7",
            "BEGIN",
            "BEGIN TRANSACTION",
            "BEGIN WORK",
            "COMMIT",
            "ROLLBACK",
        ];

        for dialect in SqlDialect::ALL.iter().copied() {
            for source in common {
                assert_supported(dialect, source);
            }
        }

        for (dialect, placeholder) in [
            (SqlDialect::Sqlite, "?1"),
            (SqlDialect::PostgreSql, "$1"),
            (SqlDialect::MySql, "?"),
        ] {
            assert_supported(
                dialect,
                &format!(
                    "INSERT INTO widgets(id, tenant_id, score) VALUES ({placeholder}, 1, 2), (3, 1, 4)"
                ),
            );
        }
    }

    #[test]
    fn common_sql_is_owned_exact_and_redacted() {
        fn assert_owned<T: Clone + Send + Sync + 'static>() {}
        assert_owned::<CommonSql>();

        let source = "SELECT 'private value' AS label";
        let common = validate(SqlDialect::Sqlite, source).unwrap();
        let debug = format!("{common:?}");

        assert_eq!(common.dialect(), SqlDialect::Sqlite);
        assert_eq!(common.source(), source);
        assert_eq!(common.statement_count(), 1);
        assert!(!common.is_empty());
        assert_eq!(common.clone(), common);
        assert!(debug.contains("source_bytes"));
        assert!(debug.contains("statement_count"));
        assert!(!debug.contains("private value"));
    }

    #[test]
    fn empty_and_mixed_batches_are_validated_without_execution_policy() {
        let empty = validate(SqlDialect::Sqlite, " -- no statement\n").unwrap();
        assert!(empty.is_empty());

        let source = "CREATE TABLE widgets(id INTEGER PRIMARY KEY); INSERT INTO widgets(id) VALUES (1); SELECT id FROM widgets; BEGIN; COMMIT";
        let common = validate(SqlDialect::Sqlite, source).unwrap();
        assert_eq!(common.source(), source);
        assert_eq!(common.statement_count(), 5);

        for (source, ordinal) in [
            ("CREATE VIEW deferred AS SELECT 1; SELECT 2; SELECT 3", 1),
            ("SELECT 1; CREATE VIEW deferred AS SELECT 2; SELECT 3", 2),
            ("SELECT 1; SELECT 2; CREATE VIEW deferred AS SELECT 3", 3),
        ] {
            let error = validate(SqlDialect::Sqlite, source).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Unsupported);
            assert!(error.diagnostic().contains(&format!("statement {ordinal}")));
            assert!(!error.diagnostic().contains(source));
        }
    }

    #[test]
    fn unsupported_top_level_statements_are_not_confused_with_parse_errors() {
        for source in [
            "DROP TABLE widgets",
            "ALTER TABLE widgets ADD COLUMN label TEXT",
            "CREATE VIEW widget_ids AS SELECT id FROM widgets",
            "SAVEPOINT nested",
            "RELEASE SAVEPOINT nested",
        ] {
            assert_unsupported(SqlDialect::Sqlite, source);
        }

        let malformed = validate(SqlDialect::Sqlite, "SELECT +").unwrap_err();
        assert_eq!(malformed.kind(), EngineErrorKind::InvalidQuery);
    }

    #[test]
    fn create_table_recursively_rejects_non_common_options() {
        let cases = [
            "CREATE TEMP TABLE widgets(id INTEGER)",
            "CREATE TABLE widgets AS SELECT 1 AS id",
            "CREATE TABLE widgets(id INTEGER) WITHOUT ROWID",
            "CREATE TABLE widgets(id INTEGER) STRICT",
            "CREATE TABLE child(id INTEGER REFERENCES parent(id))",
            "CREATE TABLE child(id INTEGER, FOREIGN KEY(id) REFERENCES parent(id))",
            "CREATE TABLE widgets(id INTEGER GENERATED ALWAYS AS (1) STORED)",
            "CREATE TABLE widgets(id INTEGER DEFAULT (abs(1)))",
            "CREATE TABLE widgets(id INTEGER CHECK (id > ?1))",
            "CREATE TABLE qualified.widgets(id INTEGER)",
            "CREATE TABLE widgets(id)",
        ];
        for source in cases {
            assert_unsupported(SqlDialect::Sqlite, source);
        }

        assert_supported(
            SqlDialect::Sqlite,
            "CREATE TABLE widgets(id CUSTOM_TYPE DEFAULT -1, label TEXT NULL, CONSTRAINT widgets_id UNIQUE(id), CHECK(id >= 0))",
        );
    }

    #[test]
    fn generated_key_declarations_have_one_narrow_dialect_surface() {
        for (dialect, source) in [
            (
                SqlDialect::Sqlite,
                "CREATE TABLE widgets(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT)",
            ),
            (
                SqlDialect::MySql,
                "CREATE TABLE widgets(id BIGINT PRIMARY KEY AUTO_INCREMENT, payload TEXT)",
            ),
            (
                SqlDialect::PostgreSql,
                "CREATE TABLE widgets(id BIGSERIAL PRIMARY KEY, payload TEXT)",
            ),
            (
                SqlDialect::PostgreSql,
                "CREATE TABLE widgets(id BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY, payload TEXT)",
            ),
        ] {
            assert_supported(dialect, source);
        }

        for (dialect, source) in [
            (
                SqlDialect::Sqlite,
                "CREATE TABLE widgets(id INT PRIMARY KEY AUTOINCREMENT)",
            ),
            (
                SqlDialect::Sqlite,
                "CREATE TABLE widgets(id INTEGER AUTOINCREMENT)",
            ),
            (
                SqlDialect::Sqlite,
                "CREATE TABLE widgets(id INTEGER AUTOINCREMENT PRIMARY KEY)",
            ),
            (
                SqlDialect::Sqlite,
                "CREATE TABLE widgets(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL)",
            ),
            (
                SqlDialect::Sqlite,
                "CREATE TABLE IF NOT EXISTS widgets(id INTEGER PRIMARY KEY AUTOINCREMENT)",
            ),
            (
                SqlDialect::MySql,
                "CREATE TABLE widgets(id INT PRIMARY KEY AUTO_INCREMENT)",
            ),
            (
                SqlDialect::MySql,
                "CREATE TABLE widgets(id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT)",
            ),
            (
                SqlDialect::MySql,
                "CREATE TABLE widgets(id BIGINT AUTO_INCREMENT)",
            ),
            (
                SqlDialect::PostgreSql,
                "CREATE TABLE widgets(id SERIAL PRIMARY KEY)",
            ),
            (
                SqlDialect::PostgreSql,
                "CREATE TABLE widgets(id SMALLSERIAL PRIMARY KEY)",
            ),
            (
                SqlDialect::PostgreSql,
                "CREATE TABLE widgets(id BIGSERIAL UNIQUE)",
            ),
            (
                SqlDialect::PostgreSql,
                "CREATE TABLE widgets(id BIGSERIAL, PRIMARY KEY(id))",
            ),
            (
                SqlDialect::PostgreSql,
                "CREATE TABLE widgets(id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY)",
            ),
            (
                SqlDialect::PostgreSql,
                "CREATE TABLE widgets(id BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY (START WITH 10))",
            ),
            (
                SqlDialect::PostgreSql,
                "CREATE TABLE widgets(id BIGINT GENERATED BY DEFAULT AS IDENTITY)",
            ),
            (
                SqlDialect::PostgreSql,
                "CREATE TABLE widgets(id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY)",
            ),
            (
                SqlDialect::PostgreSql,
                "CREATE TABLE widgets(id BIGSERIAL PRIMARY KEY, other BIGSERIAL PRIMARY KEY)",
            ),
            (
                SqlDialect::Sqlite,
                "CREATE TABLE widgets(id INTEGER PRIMARY KEY AUTOINCREMENT, other TEXT PRIMARY KEY)",
            ),
        ] {
            let error = validate(dialect, source).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Unsupported, "{source}");
            assert!(error.diagnostic().contains("generated-key declaration"));
            assert!(!error.diagnostic().contains("widgets"));
        }
    }

    #[test]
    fn create_index_accepts_only_named_plain_columns() {
        assert_supported(
            SqlDialect::Sqlite,
            "CREATE INDEX IF NOT EXISTS widgets_idx ON widgets(id ASC, tenant_id DESC)",
        );
        for source in [
            "CREATE INDEX widgets_idx ON widgets((id + 1))",
            "CREATE INDEX widgets_idx ON widgets(id) WHERE id > 0",
        ] {
            assert_unsupported(SqlDialect::Sqlite, source);
        }
        for source in [
            "CREATE INDEX widgets_idx ON widgets USING btree (id)",
            "CREATE INDEX widgets_idx ON widgets(id) INCLUDE (tenant_id)",
        ] {
            assert_unsupported(SqlDialect::PostgreSql, source);
        }
    }

    #[test]
    fn common_expressions_and_aggregates_are_recursive() {
        let source = "SELECT CASE WHEN score IS NULL THEN 0 ELSE score + 1 END AS adjusted, COUNT(DISTINCT tenant_id), MIN(score), MAX(score), SUM(score), AVG(score) FROM widgets WHERE tenant_id IN (1, ?1) AND score NOT BETWEEN 2 AND 3 AND label LIKE 'a%' GROUP BY score HAVING SUM(score) > 0 ORDER BY COUNT(*) DESC LIMIT ?2 OFFSET 1";
        assert_supported(SqlDialect::Sqlite, source);
    }

    #[test]
    fn select_rejects_every_deferred_query_shape() {
        for source in [
            "WITH selected AS (SELECT 1) SELECT * FROM selected",
            "SELECT 1 UNION SELECT 2",
            "SELECT * FROM widgets JOIN tenants ON widgets.tenant_id = tenants.id",
            "SELECT * FROM widgets, tenants",
            "SELECT (SELECT 1)",
            "SELECT upper(label) FROM widgets",
            "SELECT CAST(score AS TEXT) FROM widgets",
            "SELECT id FROM widgets WHERE id IN (SELECT id FROM archived_widgets)",
            "SELECT id FROM widgets WHERE label LIKE 'a%' ESCAPE '!'",
            "SELECT * FROM widgets ORDER BY score NULLS FIRST",
            "SELECT schema.widgets.* FROM widgets",
        ] {
            assert_unsupported(SqlDialect::Sqlite, source);
        }
        assert_unsupported(
            SqlDialect::PostgreSql,
            "SELECT DISTINCT ON (tenant_id) tenant_id FROM widgets",
        );
        assert_unsupported(
            SqlDialect::PostgreSql,
            "SELECT COUNT(*) OVER () FROM widgets",
        );
        assert_unsupported(
            SqlDialect::PostgreSql,
            "SELECT COUNT(id, tenant_id) FROM widgets",
        );
    }

    #[test]
    fn comma_limit_is_structurally_admitted_for_later_translation() {
        assert_supported(SqlDialect::MySql, "SELECT * FROM widgets LIMIT 10, 20");
        assert_supported(SqlDialect::Sqlite, "SELECT * FROM widgets LIMIT 10, 20");
        assert_supported(SqlDialect::MySql, "SELECT * FROM widgets LIMIT ?, ?");
    }

    #[test]
    fn insert_requires_explicit_equal_width_values_without_modifiers() {
        assert_supported(
            SqlDialect::PostgreSql,
            "INSERT INTO widgets(id, tenant_id) VALUES ($1, 1), (2, $2)",
        );
        for source in [
            "INSERT INTO widgets VALUES (1)",
            "INSERT INTO widgets(id, tenant_id) VALUES (1)",
            "INSERT INTO widgets(id, id) VALUES (1, 2)",
            "INSERT INTO widgets(id, ID) VALUES (1, 2)",
            "INSERT INTO widgets(id) SELECT id FROM old_widgets",
            "INSERT INTO widgets(id) VALUES (1) RETURNING id",
        ] {
            assert_unsupported(SqlDialect::PostgreSql, source);
        }
        assert_unsupported(
            SqlDialect::Sqlite,
            "INSERT INTO widgets(id) VALUES (1) ON CONFLICT(id) DO NOTHING",
        );
        assert_unsupported(
            SqlDialect::MySql,
            "INSERT IGNORE INTO widgets(id) VALUES (1)",
        );
    }

    #[test]
    fn update_validates_shape_but_leaves_routing_for_later() {
        assert_supported(SqlDialect::Sqlite, "UPDATE widgets SET score = score + 1");
        assert_supported(
            SqlDialect::PostgreSql,
            "UPDATE widgets AS w SET score = $1 WHERE w.tenant_id = $2",
        );
        for source in [
            "UPDATE widgets SET score = 1, score = 2",
            "UPDATE widgets SET score = 1, SCORE = 2",
            "UPDATE widgets SET (score, tenant_id) = (1, 2)",
            "UPDATE widgets SET score = other.score FROM other WHERE widgets.id = other.id",
            "UPDATE widgets SET score = 1 RETURNING score",
        ] {
            assert_unsupported(SqlDialect::PostgreSql, source);
        }
        assert_unsupported(
            SqlDialect::MySql,
            "UPDATE widgets SET score = 1 ORDER BY id LIMIT 1",
        );
        assert_unsupported(
            SqlDialect::Sqlite,
            "UPDATE OR REPLACE widgets SET score = 1",
        );
    }

    #[test]
    fn delete_validates_shape_but_leaves_routing_for_later() {
        assert_supported(SqlDialect::Sqlite, "DELETE FROM widgets");
        assert_supported(
            SqlDialect::PostgreSql,
            "DELETE FROM widgets AS w WHERE w.tenant_id = $1",
        );
        assert_unsupported(
            SqlDialect::PostgreSql,
            "DELETE FROM widgets USING tenants WHERE widgets.tenant_id = tenants.id",
        );
        assert_unsupported(SqlDialect::PostgreSql, "DELETE FROM widgets RETURNING id");
        assert_unsupported(SqlDialect::MySql, "DELETE FROM widgets ORDER BY id LIMIT 1");
    }

    #[test]
    fn transaction_variants_are_narrow_and_state_free() {
        for source in [
            "ABORT",
            "COMMIT TRANSACTION",
            "COMMIT WORK",
            "COMMIT TRAN",
            "COMMIT AND NO CHAIN",
            "ROLLBACK TRANSACTION",
            "ROLLBACK WORK",
            "ROLLBACK TRAN",
            "ROLLBACK AND NO CHAIN",
            "ABORT AND NO CHAIN",
        ] {
            assert_supported(SqlDialect::PostgreSql, source);
        }

        for source in [
            "START TRANSACTION",
            "BEGIN IMMEDIATE",
            "ROLLBACK TO SAVEPOINT nested",
        ] {
            let dialect = if source == "BEGIN IMMEDIATE" {
                SqlDialect::Sqlite
            } else {
                SqlDialect::PostgreSql
            };
            assert_unsupported(dialect, source);
        }
        assert_unsupported(SqlDialect::PostgreSql, "COMMIT AND CHAIN");
    }

    #[test]
    fn unsupported_diagnostics_never_contain_source_sql() {
        let source = "SELECT upper('private literal')";
        let error = validate(SqlDialect::Sqlite, source).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Unsupported);
        assert!(!error.diagnostic().contains(source));
        assert!(!error.diagnostic().contains("private literal"));
        assert!(error.diagnostic().contains("statement 1"));
    }

    #[test]
    fn concurrent_validation_owns_no_shared_mutable_parser_state() {
        let parsed = parse(
            SqlDialect::PostgreSql,
            "SELECT tenant_id FROM widgets WHERE tenant_id = $1",
        )
        .unwrap();
        let threads = (0..24)
            .map(|_| {
                let parsed = parsed.clone();
                thread::spawn(move || validate_common_subset(parsed))
            })
            .collect::<Vec<_>>();

        for handle in threads {
            let common = handle.join().unwrap().unwrap();
            assert_eq!(common.statement_count(), 1);
        }
    }

    #[test]
    fn ordinary_parenthesized_expression_depth_remains_supported() {
        let expression = (0..16).fold("tenant_id = 1".to_owned(), |inner, _| {
            format!("({inner} AND tenant_id = 1)")
        });
        assert_supported(
            SqlDialect::PostgreSql,
            &format!("SELECT tenant_id FROM widgets WHERE {expression}"),
        );
    }

    #[test]
    fn flat_operator_chains_have_an_exact_independent_validation_depth_limit() {
        fn source_with_terms(terms: usize) -> String {
            let expression = std::iter::repeat_n("1", terms)
                .collect::<Vec<_>>()
                .join(" + ");
            format!("SELECT {expression}")
        }

        let source = source_with_terms(MAX_COMMON_SQL_EXPRESSION_DEPTH);
        assert_supported(SqlDialect::PostgreSql, &source);

        let source = source_with_terms(MAX_COMMON_SQL_EXPRESSION_DEPTH + 1);
        let error = validate(SqlDialect::PostgreSql, &source).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert!(
            error
                .diagnostic()
                .contains(&MAX_COMMON_SQL_EXPRESSION_DEPTH.to_string())
        );
        assert!(!error.diagnostic().contains(&source));

        let source = source_with_terms(12_000);
        assert!(source.len() < MAX_PARSED_SQL_BYTES);
        let error = validate(SqlDialect::PostgreSql, &source).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    }
}
