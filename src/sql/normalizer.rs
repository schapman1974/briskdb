use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use sqlparser::tokenizer::{Location, Span};

use super::{CommonSql, SqlDialect};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

/// BriskDB's largest parameter index for placeholder normalization.
///
/// This matches the default maximum variable number in BriskDB's bundled
/// SQLite configuration. Parameter indexes and counts are local to one
/// top-level statement.
pub const MAX_SQL_PARAMETERS: usize = 32_766;

/// Common SQL with source-dialect placeholders rewritten to numbered SQLite
/// parameter markers.
///
/// The original SQL remains available through [`Self::source`]. Only accepted
/// placeholder spans differ in [`Self::sqlite_parameter_sql`]; this layer does
/// not render the AST, translate other dialect syntax, bind values, or execute
/// a statement.
#[derive(Clone, PartialEq, Eq)]
pub struct NormalizedSql {
    common: CommonSql,
    sqlite_parameter_sql: String,
    statement_parameters: Vec<StatementParameters>,
}

impl NormalizedSql {
    /// Return the explicitly selected source dialect.
    pub const fn dialect(&self) -> SqlDialect {
        self.common.dialect()
    }

    /// Return the caller's byte-exact SQL source before marker normalization.
    pub fn source(&self) -> &str {
        self.common.source()
    }

    /// Return SQL with only placeholder markers converted to SQLite `?N` form.
    ///
    /// This is not necessarily executable SQLite SQL because later work owns
    /// translation of other source-dialect syntax.
    pub fn sqlite_parameter_sql(&self) -> &str {
        &self.sqlite_parameter_sql
    }

    /// Return the number of independently normalized top-level statements.
    pub fn statement_count(&self) -> usize {
        self.common.statement_count()
    }

    /// Return whether the parsed input contained no statements.
    pub fn is_empty(&self) -> bool {
        self.common.is_empty()
    }

    /// Return one parameter layout for each top-level statement, in order.
    pub fn statement_parameters(&self) -> &[StatementParameters] {
        &self.statement_parameters
    }
}

impl fmt::Debug for NormalizedSql {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NormalizedSql")
            .field("dialect", &self.dialect())
            .field("source_bytes", &self.source().len())
            .field(
                "sqlite_parameter_sql_bytes",
                &self.sqlite_parameter_sql.len(),
            )
            .field("statement_count", &self.statement_count())
            .finish()
    }
}

/// Parameter numbering for one normalized top-level statement.
///
/// Indexes are one-based SQLite binding indexes in lexical occurrence order.
/// `parameter_count` is the greatest referenced index, so it includes gaps in
/// PostgreSQL `$N` and SQLite `?N` numbering.
#[derive(Clone, PartialEq, Eq)]
pub struct StatementParameters {
    parameter_count: usize,
    parameter_indices: Vec<usize>,
}

impl StatementParameters {
    /// Return the greatest parameter index used by this statement.
    pub const fn parameter_count(&self) -> usize {
        self.parameter_count
    }

    /// Return the number of placeholder occurrences in this statement.
    pub fn occurrence_count(&self) -> usize {
        self.parameter_indices.len()
    }

    /// Return each one-based parameter index in lexical occurrence order.
    pub fn parameter_indices(&self) -> &[usize] {
        &self.parameter_indices
    }
}

impl fmt::Debug for StatementParameters {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StatementParameters")
            .field("parameter_count", &self.parameter_count)
            .field("occurrence_count", &self.parameter_indices.len())
            .finish()
    }
}

/// Normalize placeholders after common-subset validation.
///
/// PostgreSQL `$N` indexes are preserved, MySQL bare `?` markers are numbered
/// by occurrence, and SQLite positional `?`/`?N` markers retain SQLite's native
/// numbering. Numbering restarts for every top-level statement. No parameter
/// values enter this operation.
pub fn normalize_placeholders(common: CommonSql) -> EngineResult<NormalizedSql> {
    if common.statement_placeholders().len() != common.statement_count() {
        return Err(normalization_invariant());
    }

    let mut planned = Vec::new();
    let mut statement_parameters = Vec::with_capacity(common.statement_count());

    for (statement_index, placeholders) in common.statement_placeholders().iter().enumerate() {
        let mut placeholders = placeholders.iter().collect::<Vec<_>>();
        placeholders.sort_unstable_by_key(|placeholder| placeholder.span);

        let mut parameter_indices = Vec::with_capacity(placeholders.len());
        let mut greatest_index = 0;

        for (occurrence_index, placeholder) in placeholders.into_iter().enumerate() {
            let index = parameter_index(
                common.dialect(),
                &placeholder.marker,
                greatest_index,
                statement_index,
                occurrence_index,
            )?;
            greatest_index = greatest_index.max(index);
            parameter_indices.push(index);
            planned.push(PlannedPlaceholder {
                marker: placeholder.marker.clone(),
                span: placeholder.span,
                index,
            });
        }

        statement_parameters.push(StatementParameters {
            parameter_count: greatest_index,
            parameter_indices,
        });
    }

    let sqlite_parameter_sql = rewrite_placeholders(common.source(), &planned)?;
    Ok(NormalizedSql {
        common,
        sqlite_parameter_sql,
        statement_parameters,
    })
}

fn parameter_index(
    dialect: SqlDialect,
    marker: &str,
    greatest_index: usize,
    statement_index: usize,
    occurrence_index: usize,
) -> EngineResult<usize> {
    match dialect {
        SqlDialect::PostgreSql => parse_numbered_marker(marker, '$')
            .map_err(|failure| marker_failure(failure, dialect, statement_index, occurrence_index)),
        SqlDialect::MySql if marker == "?" => {
            next_parameter_index(greatest_index, statement_index, occurrence_index)
        }
        SqlDialect::MySql => Err(invalid_marker(dialect, statement_index, occurrence_index)),
        SqlDialect::Sqlite if marker == "?" => {
            next_parameter_index(greatest_index, statement_index, occurrence_index)
        }
        SqlDialect::Sqlite if marker.starts_with('?') => parse_numbered_marker(marker, '?')
            .map_err(|failure| marker_failure(failure, dialect, statement_index, occurrence_index)),
        SqlDialect::Sqlite
            if marker.starts_with(':') || marker.starts_with('@') || marker.starts_with('$') =>
        {
            Err(EngineError::new(
                EngineErrorKind::Unsupported,
                format!(
                    "statement {} placeholder {} uses a named SQLite parameter; only positional parameters are supported",
                    statement_index + 1,
                    occurrence_index + 1
                ),
            ))
        }
        SqlDialect::Sqlite => Err(invalid_marker(dialect, statement_index, occurrence_index)),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MarkerFailure {
    Invalid,
    LimitExceeded,
}

fn parse_numbered_marker(marker: &str, prefix: char) -> Result<usize, MarkerFailure> {
    let Some(digits) = marker.strip_prefix(prefix) else {
        return Err(MarkerFailure::Invalid);
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(MarkerFailure::Invalid);
    }

    let mut index = 0_usize;
    for byte in digits.bytes() {
        let digit = usize::from(byte - b'0');
        index = index
            .checked_mul(10)
            .and_then(|value| value.checked_add(digit))
            .ok_or(MarkerFailure::LimitExceeded)?;
        if index > MAX_SQL_PARAMETERS {
            return Err(MarkerFailure::LimitExceeded);
        }
    }

    if index == 0 {
        Err(MarkerFailure::Invalid)
    } else {
        Ok(index)
    }
}

fn next_parameter_index(
    greatest_index: usize,
    statement_index: usize,
    occurrence_index: usize,
) -> EngineResult<usize> {
    let index = greatest_index
        .checked_add(1)
        .ok_or_else(|| parameter_limit(statement_index, occurrence_index))?;
    if index > MAX_SQL_PARAMETERS {
        return Err(parameter_limit(statement_index, occurrence_index));
    }
    Ok(index)
}

fn marker_failure(
    failure: MarkerFailure,
    dialect: SqlDialect,
    statement_index: usize,
    occurrence_index: usize,
) -> EngineError {
    match failure {
        MarkerFailure::Invalid => invalid_marker(dialect, statement_index, occurrence_index),
        MarkerFailure::LimitExceeded => parameter_limit(statement_index, occurrence_index),
    }
}

fn invalid_marker(
    dialect: SqlDialect,
    statement_index: usize,
    occurrence_index: usize,
) -> EngineError {
    EngineError::new(
        EngineErrorKind::InvalidQuery,
        format!(
            "statement {} placeholder {} is not valid {} positional parameter syntax",
            statement_index + 1,
            occurrence_index + 1,
            dialect.name()
        ),
    )
}

fn parameter_limit(statement_index: usize, occurrence_index: usize) -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        format!(
            "statement {} placeholder {} exceeds the normalized SQL parameter limit of {MAX_SQL_PARAMETERS}",
            statement_index + 1,
            occurrence_index + 1
        ),
    )
}

struct PlannedPlaceholder {
    marker: String,
    span: Span,
    index: usize,
}

fn rewrite_placeholders(source: &str, planned: &[PlannedPlaceholder]) -> EngineResult<String> {
    if planned.is_empty() {
        return Ok(source.to_owned());
    }

    let mut planned = planned.iter().collect::<Vec<_>>();
    planned.sort_unstable_by_key(|placeholder| placeholder.span);

    let mut locations = BTreeSet::new();
    for placeholder in &planned {
        if placeholder.span.start >= placeholder.span.end
            || placeholder.span.start == Location::empty()
            || placeholder.span.end == Location::empty()
        {
            return Err(normalization_invariant());
        }
        locations.insert(placeholder.span.start);
        locations.insert(placeholder.span.end);
    }
    let offsets = source_offsets(source, &locations)?;

    let mut normalized = String::with_capacity(source.len());
    let mut cursor = 0;
    for placeholder in planned {
        let Some(&start) = offsets.get(&placeholder.span.start) else {
            return Err(normalization_invariant());
        };
        let Some(&end) = offsets.get(&placeholder.span.end) else {
            return Err(normalization_invariant());
        };
        if start < cursor || start >= end || end > source.len() {
            return Err(normalization_invariant());
        }
        if source.get(start..end) != Some(placeholder.marker.as_str()) {
            return Err(normalization_invariant());
        }

        normalized.push_str(&source[cursor..start]);
        normalized.push('?');
        normalized.push_str(&placeholder.index.to_string());
        cursor = end;
    }
    normalized.push_str(&source[cursor..]);
    Ok(normalized)
}

fn source_offsets(
    source: &str,
    locations: &BTreeSet<Location>,
) -> EngineResult<BTreeMap<Location, usize>> {
    let mut offsets = BTreeMap::new();
    let mut location = Location::new(1, 1);
    if locations.contains(&location) {
        offsets.insert(location, 0);
    }

    for (byte_offset, character) in source.char_indices() {
        location = if character == '\n' {
            Location::new(
                location
                    .line
                    .checked_add(1)
                    .ok_or_else(normalization_invariant)?,
                1,
            )
        } else {
            Location::new(
                location.line,
                location
                    .column
                    .checked_add(1)
                    .ok_or_else(normalization_invariant)?,
            )
        };
        if locations.contains(&location) {
            offsets.insert(location, byte_offset + character.len_utf8());
        }
    }

    if offsets.len() != locations.len() {
        return Err(normalization_invariant());
    }
    Ok(offsets)
}

fn normalization_invariant() -> EngineError {
    EngineError::new(
        EngineErrorKind::Internal,
        "placeholder metadata does not match the retained SQL text",
    )
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use crate::{
        core::Value,
        sql::{parse, query, validate_common_subset},
    };

    fn normalize(dialect: SqlDialect, source: &str) -> EngineResult<NormalizedSql> {
        normalize_placeholders(validate_common_subset(parse(dialect, source)?)?)
    }

    fn assert_layout(
        normalized: &NormalizedSql,
        statement: usize,
        count: usize,
        indices: &[usize],
    ) {
        let layout = &normalized.statement_parameters()[statement];
        assert_eq!(layout.parameter_count(), count);
        assert_eq!(layout.occurrence_count(), indices.len());
        assert_eq!(layout.parameter_indices(), indices);
    }

    #[test]
    fn dialect_markers_normalize_to_numbered_sqlite_parameters() {
        let postgres = normalize(SqlDialect::PostgreSql, "SELECT $2, $1, $2, $05").unwrap();
        assert_eq!(postgres.sqlite_parameter_sql(), "SELECT ?2, ?1, ?2, ?5");
        assert_layout(&postgres, 0, 5, &[2, 1, 2, 5]);

        let mysql = normalize(SqlDialect::MySql, "SELECT ?, ?, ?").unwrap();
        assert_eq!(mysql.sqlite_parameter_sql(), "SELECT ?1, ?2, ?3");
        assert_layout(&mysql, 0, 3, &[1, 2, 3]);

        let sqlite = normalize(SqlDialect::Sqlite, "SELECT ?2, ?, ?1, ?").unwrap();
        assert_eq!(sqlite.sqlite_parameter_sql(), "SELECT ?2, ?3, ?1, ?4");
        assert_layout(&sqlite, 0, 4, &[2, 3, 1, 4]);
    }

    #[test]
    fn exact_source_is_retained_and_only_ast_placeholder_spans_change() {
        let source = "-- $8 and ? stay comments\r\nSELECT 'é🙂 $9 ?' AS note,\r\n       $02 AS value -- $7 ?\r\n";
        let normalized = normalize(SqlDialect::PostgreSql, source).unwrap();
        assert_eq!(normalized.source(), source);
        assert_eq!(
            normalized.sqlite_parameter_sql(),
            "-- $8 and ? stay comments\r\nSELECT 'é🙂 $9 ?' AS note,\r\n       ?2 AS value -- $7 ?\r\n"
        );
        assert_layout(&normalized, 0, 2, &[2]);
    }

    #[test]
    fn quoted_identifiers_and_other_dialect_text_are_not_rewritten() {
        let postgres = normalize(
            SqlDialect::PostgreSql,
            "SELECT \"$1\" FROM widgets WHERE tenant_id = $1",
        )
        .unwrap();
        assert_eq!(
            postgres.sqlite_parameter_sql(),
            "SELECT \"$1\" FROM widgets WHERE tenant_id = ?1"
        );

        let mysql = normalize(SqlDialect::MySql, "SELECT $1").unwrap();
        assert_eq!(mysql.sqlite_parameter_sql(), "SELECT $1");
        assert_layout(&mysql, 0, 0, &[]);
    }

    #[test]
    fn numbering_and_layouts_are_statement_local() {
        let normalized = normalize(SqlDialect::MySql, "SELECT ?; SELECT 1; SELECT ?, ?").unwrap();
        assert_eq!(
            normalized.sqlite_parameter_sql(),
            "SELECT ?1; SELECT 1; SELECT ?1, ?2"
        );
        assert_eq!(normalized.statement_count(), 3);
        assert_eq!(normalized.statement_parameters().len(), 3);
        assert_layout(&normalized, 0, 1, &[1]);
        assert_layout(&normalized, 1, 0, &[]);
        assert_layout(&normalized, 2, 2, &[1, 2]);
    }

    #[test]
    fn empty_and_parameterless_inputs_have_complete_empty_layouts() {
        let empty = normalize(SqlDialect::Sqlite, " -- comment only\n").unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty.statement_count(), 0);
        assert!(empty.statement_parameters().is_empty());
        assert_eq!(empty.sqlite_parameter_sql(), empty.source());

        let batch = normalize(SqlDialect::Sqlite, "BEGIN; SELECT 1; COMMIT").unwrap();
        assert_eq!(batch.statement_parameters().len(), 3);
        assert!(
            batch.statement_parameters().iter().all(
                |layout| layout.parameter_count() == 0 && layout.parameter_indices().is_empty()
            )
        );
        assert_eq!(batch.sqlite_parameter_sql(), batch.source());
    }

    #[test]
    fn every_parameter_bearing_statement_family_records_occurrences() {
        let cases = [
            (
                "SELECT $1, COUNT($2) FROM widgets WHERE tenant_id = $3 GROUP BY $4 HAVING COUNT($5) > $6 ORDER BY $7 LIMIT $8 OFFSET $9",
                (9, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ),
            (
                "INSERT INTO widgets(id, tenant_id) VALUES ($2, $1), ($2, $1)",
                (2, vec![2, 1, 2, 1]),
            ),
            (
                "UPDATE widgets SET tenant_id = $2, id = $1 WHERE tenant_id = $2",
                (2, vec![2, 1, 2]),
            ),
            ("DELETE FROM widgets WHERE tenant_id = $1", (1, vec![1])),
        ];

        for (source, (count, indices)) in cases {
            let normalized = normalize(SqlDialect::PostgreSql, source).unwrap();
            assert_layout(&normalized, 0, count, &indices);
        }
    }

    #[test]
    fn recursive_expression_sites_retain_lexical_parameter_order() {
        let source = "SELECT CASE WHEN NOT ($1 BETWEEN $2 AND $3) THEN -$4 ELSE $5 END FROM widgets WHERE tenant_id IN ($6, $7) AND id LIKE $8";
        let normalized = normalize(SqlDialect::PostgreSql, source).unwrap();
        assert_layout(&normalized, 0, 8, &[1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn incompatible_zero_named_and_parser_broad_markers_are_classified() {
        for (dialect, source) in [
            (SqlDialect::PostgreSql, "SELECT $0"),
            (SqlDialect::PostgreSql, "SELECT $name"),
            (SqlDialect::PostgreSql, "SELECT $1abc"),
            (SqlDialect::PostgreSql, "SELECT :name"),
            (SqlDialect::MySql, "SELECT ?0"),
            (SqlDialect::MySql, "SELECT ?1"),
            (SqlDialect::MySql, "SELECT :name"),
            (SqlDialect::Sqlite, "SELECT ?0"),
        ] {
            let error = normalize(dialect, source).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidQuery, "{source}");
            assert!(!error.diagnostic().contains(source));
        }

        for source in ["SELECT :name", "SELECT @name", "SELECT $name"] {
            let error = normalize(SqlDialect::Sqlite, source).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Unsupported, "{source}");
            assert!(!error.diagnostic().contains(source));
        }
    }

    #[test]
    fn exact_parameter_limit_is_accepted_and_larger_indexes_are_rejected() {
        for (dialect, marker) in [
            (SqlDialect::PostgreSql, "$32766"),
            (SqlDialect::Sqlite, "?32766"),
        ] {
            let normalized = normalize(dialect, &format!("SELECT {marker}")).unwrap();
            assert_layout(&normalized, 0, MAX_SQL_PARAMETERS, &[MAX_SQL_PARAMETERS]);
        }

        for (dialect, marker) in [
            (SqlDialect::PostgreSql, "$32767"),
            (SqlDialect::PostgreSql, "$999999999999999999999999999999999"),
            (SqlDialect::Sqlite, "?32767"),
        ] {
            let source = format!("SELECT {marker}");
            let error = normalize(dialect, &source).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
            assert!(error.diagnostic().contains(&MAX_SQL_PARAMETERS.to_string()));
            assert!(!error.diagnostic().contains(marker));
        }

        let assigned_overflow = normalize(SqlDialect::Sqlite, "SELECT ?32766, ?").unwrap_err();
        assert_eq!(assigned_overflow.kind(), EngineErrorKind::LimitExceeded);
        assert!(
            assigned_overflow
                .diagnostic()
                .contains(&MAX_SQL_PARAMETERS.to_string())
        );
    }

    #[test]
    fn normalized_sql_executes_with_out_of_band_values() {
        let normalized = normalize(
            SqlDialect::PostgreSql,
            "SELECT $2 AS second, $1 AS first, $2 = $2 AS repeated",
        )
        .unwrap();
        let first = "O'Reilly; SELECT 99".to_owned();
        let second = "quoted ' value; -- unchanged".to_owned();
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        let result = query(
            &connection,
            normalized.sqlite_parameter_sql(),
            &[Value::Text(first.clone()), Value::Text(second.clone())],
        )
        .unwrap();

        assert_eq!(result.rows()[0].get(0), Some(&Value::Text(second)));
        assert_eq!(result.rows()[0].get(1), Some(&Value::Text(first)));
        assert_eq!(result.rows()[0].get(2), Some(&Value::Int64(1)));
        assert_eq!(
            normalized.sqlite_parameter_sql(),
            "SELECT ?2 AS second, ?1 AS first, ?2 = ?2 AS repeated"
        );
    }

    #[test]
    fn normalized_types_are_owned_cloneable_and_redacted() {
        fn assert_owned<T: Clone + Send + Sync + 'static>() {}
        assert_owned::<NormalizedSql>();
        assert_owned::<StatementParameters>();

        let source = "SELECT $1, 'private literal'";
        let normalized = normalize(SqlDialect::PostgreSql, source).unwrap();
        let debug = format!("{normalized:?}");
        let layout_debug = format!("{:?}", normalized.statement_parameters()[0]);

        assert_eq!(normalized.clone(), normalized);
        assert!(debug.contains("source_bytes"));
        assert!(debug.contains("sqlite_parameter_sql_bytes"));
        assert!(!debug.contains("private literal"));
        assert!(layout_debug.contains("occurrence_count"));
        assert!(!layout_debug.contains("private literal"));
    }

    #[test]
    fn concurrent_normalization_is_deterministic() {
        let common = validate_common_subset(
            parse(
                SqlDialect::PostgreSql,
                "SELECT $2, $1, $2 FROM widgets WHERE tenant_id = $1",
            )
            .unwrap(),
        )
        .unwrap();
        let threads = (0..24)
            .map(|_| {
                let common = common.clone();
                thread::spawn(move || normalize_placeholders(common))
            })
            .collect::<Vec<_>>();

        for thread in threads {
            let normalized = thread.join().unwrap().unwrap();
            assert_eq!(
                normalized.sqlite_parameter_sql(),
                "SELECT ?2, ?1, ?2 FROM widgets WHERE tenant_id = ?1"
            );
            assert_layout(&normalized, 0, 2, &[2, 1, 2, 1]);
        }
    }

    #[test]
    fn one_normalization_error_does_not_affect_later_calls() {
        let invalid = normalize(SqlDialect::PostgreSql, "SELECT $0").unwrap_err();
        assert_eq!(invalid.kind(), EngineErrorKind::InvalidQuery);

        let normalized = normalize(SqlDialect::PostgreSql, "SELECT $1").unwrap();
        assert_eq!(normalized.sqlite_parameter_sql(), "SELECT ?1");
        assert_layout(&normalized, 0, 1, &[1]);
    }

    #[test]
    fn source_span_invariant_failures_are_internal_and_redacted() {
        let cases = [
            vec![PlannedPlaceholder {
                marker: "$2".to_owned(),
                span: Span::new(Location::new(1, 8), Location::new(1, 10)),
                index: 2,
            }],
            vec![PlannedPlaceholder {
                marker: "$1".to_owned(),
                span: Span::new(Location::new(2, 1), Location::new(2, 3)),
                index: 1,
            }],
            vec![PlannedPlaceholder {
                marker: "$1".to_owned(),
                span: Span::new(Location::new(1, 8), Location::new(1, 8)),
                index: 1,
            }],
            vec![
                PlannedPlaceholder {
                    marker: "$1".to_owned(),
                    span: Span::new(Location::new(1, 8), Location::new(1, 10)),
                    index: 1,
                },
                PlannedPlaceholder {
                    marker: "$1".to_owned(),
                    span: Span::new(Location::new(1, 8), Location::new(1, 10)),
                    index: 1,
                },
            ],
        ];

        for planned in cases {
            let error = rewrite_placeholders("SELECT $1", &planned).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Internal);
            assert!(!error.diagnostic().contains("SELECT"));
            assert!(!error.diagnostic().contains("$1"));
            assert!(!error.diagnostic().contains("$2"));
        }
    }
}
