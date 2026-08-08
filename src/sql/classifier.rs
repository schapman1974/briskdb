//! Protocol-neutral statement behavior and request-batch classification.

use std::fmt;

use sqlparser::ast::Statement as AstStatement;

use super::{CommonSql, NormalizedSql};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

/// The data-changing operation performed by a supported write statement.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteBehavior {
    /// Insert one or more rows.
    Insert,
    /// Update existing rows.
    Update,
    /// Delete existing rows.
    Delete,
}

/// The schema-changing operation performed by a supported schema statement.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchemaBehavior {
    /// Create a table.
    CreateTable,
    /// Create an index.
    CreateIndex,
}

/// The session operation represented by a supported transaction statement.
///
/// This is syntax classification only. Transaction state and shard pinning are
/// owned by the later protocol-neutral session state machine.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionBehavior {
    /// Begin a transaction.
    Begin,
    /// Commit the current transaction.
    Commit,
    /// Roll back the current transaction.
    Rollback,
}

/// The protocol-neutral behavior of one validated top-level SQL statement.
///
/// The behavior comes only from BriskDB's opaque validated AST. It is
/// independent of source dialect, SQLite compile metadata, routing, catalog
/// placement, authorization, and the eventual wire protocol.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatementBehavior {
    /// Read rows without changing data, schema, or session state.
    Read,
    /// Change application data.
    Write(WriteBehavior),
    /// Change the application schema.
    Schema(SchemaBehavior),
    /// Change transaction or other session state.
    Session(SessionBehavior),
}

impl StatementBehavior {
    /// Return whether the statement has no data, schema, or session side
    /// effect according to the validated common SQL subset.
    pub const fn is_read_only(self) -> bool {
        matches!(self, Self::Read)
    }

    const fn category(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write(_) => "write",
            Self::Schema(_) => "schema",
            Self::Session(_) => "session",
        }
    }
}

/// Owned ordered behavior metadata for one accepted SQL request batch.
///
/// A successful classification is always nonempty. A multi-statement result is
/// always read-only; batches containing any data, schema, or session change are
/// rejected atomically by [`classify_statements`]. This result grants no
/// catalog, routing, authorization, or execution permission.
#[derive(Clone, PartialEq, Eq)]
pub struct StatementBatchClassification {
    behaviors: Box<[StatementBehavior]>,
}

impl StatementBatchClassification {
    /// Return one behavior for every top-level statement in source order.
    pub fn behaviors(&self) -> &[StatementBehavior] {
        &self.behaviors
    }

    /// Return the behavior at one zero-based top-level statement index.
    pub fn behavior(&self, statement_index: usize) -> Option<StatementBehavior> {
        self.behaviors.get(statement_index).copied()
    }

    /// Return the number of classified top-level statements.
    pub fn statement_count(&self) -> usize {
        self.behaviors.len()
    }

    /// Return whether every statement in this nonempty accepted batch is
    /// read-only.
    pub fn is_read_only(&self) -> bool {
        self.behaviors
            .iter()
            .copied()
            .all(StatementBehavior::is_read_only)
    }
}

impl fmt::Debug for StatementBatchClassification {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StatementBatchClassification")
            .field("statement_count", &self.statement_count())
            .field("behaviors", &self.behaviors)
            .finish()
    }
}

/// Classify a validated request and enforce BriskDB's general batch policy.
///
/// Empty requests are invalid. Every supported statement family may appear as
/// a singleton. A request with two or more statements is accepted only when
/// every statement is read-only. The first non-read statement determines the
/// deterministic rejection diagnostic; no SQL text or AST formatting is
/// included.
pub fn classify_statements(common: &CommonSql) -> EngineResult<StatementBatchClassification> {
    classify_statement_slice(common.statements())
}

/// Apply the same whole-request classifier to retained normalized SQL.
///
/// Core planning and execution use this helper so normalization cannot become
/// a path around the public batch policy.
pub(crate) fn classify_normalized_statements(
    normalized: &NormalizedSql,
) -> EngineResult<StatementBatchClassification> {
    classify_statement_slice(normalized.common().statements())
}

fn classify_statement_slice(
    statements: &[AstStatement],
) -> EngineResult<StatementBatchClassification> {
    if statements.is_empty() {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "a SQL request must contain at least one top-level statement",
        ));
    }

    let behaviors = statements
        .iter()
        .map(classify_statement)
        .collect::<EngineResult<Box<[_]>>>()?;

    if let Some((index, behavior)) = first_disallowed_batch_behavior(&behaviors) {
        return Err(EngineError::new(
            EngineErrorKind::Unsupported,
            format!(
                "statement {} has {} behavior; multi-statement requests may contain only read statements",
                index + 1,
                behavior.category()
            ),
        ));
    }

    Ok(StatementBatchClassification { behaviors })
}

fn first_disallowed_batch_behavior(
    behaviors: &[StatementBehavior],
) -> Option<(usize, StatementBehavior)> {
    if behaviors.len() <= 1 {
        return None;
    }
    behaviors
        .iter()
        .copied()
        .enumerate()
        .find(|(_, behavior)| !behavior.is_read_only())
}

fn classify_statement(statement: &AstStatement) -> EngineResult<StatementBehavior> {
    let behavior = match statement {
        AstStatement::Query(_) => StatementBehavior::Read,
        AstStatement::Insert(_) => StatementBehavior::Write(WriteBehavior::Insert),
        AstStatement::Update(_) => StatementBehavior::Write(WriteBehavior::Update),
        AstStatement::Delete(_) => StatementBehavior::Write(WriteBehavior::Delete),
        AstStatement::CreateTable(_) => StatementBehavior::Schema(SchemaBehavior::CreateTable),
        AstStatement::CreateIndex(_) => StatementBehavior::Schema(SchemaBehavior::CreateIndex),
        AstStatement::StartTransaction { .. } => StatementBehavior::Session(SessionBehavior::Begin),
        AstStatement::Commit { .. } => StatementBehavior::Session(SessionBehavior::Commit),
        AstStatement::Rollback { .. } => StatementBehavior::Session(SessionBehavior::Rollback),
        _ => return Err(classification_invariant()),
    };
    Ok(behavior)
}

fn classification_invariant() -> EngineError {
    EngineError::new(
        EngineErrorKind::Internal,
        "validated SQL contains an unclassified statement type",
    )
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use super::*;
    use crate::sql::{
        MAX_PARSED_SQL_STATEMENTS, SqlDialect, normalize_placeholders, parse,
        validate_common_subset,
    };

    fn common(dialect: SqlDialect, source: &str) -> EngineResult<CommonSql> {
        validate_common_subset(parse(dialect, source)?)
    }

    fn classify(dialect: SqlDialect, source: &str) -> EngineResult<StatementBatchClassification> {
        classify_statements(&common(dialect, source)?)
    }

    #[test]
    fn every_supported_family_has_dialect_independent_behavior() {
        let cases = [
            ("SELECT 1", StatementBehavior::Read),
            (
                "INSERT INTO widgets (id) VALUES (1)",
                StatementBehavior::Write(WriteBehavior::Insert),
            ),
            (
                "UPDATE widgets SET id = 1",
                StatementBehavior::Write(WriteBehavior::Update),
            ),
            (
                "DELETE FROM widgets",
                StatementBehavior::Write(WriteBehavior::Delete),
            ),
            (
                "CREATE TABLE widgets (id INTEGER)",
                StatementBehavior::Schema(SchemaBehavior::CreateTable),
            ),
            (
                "CREATE INDEX widgets_id ON widgets (id)",
                StatementBehavior::Schema(SchemaBehavior::CreateIndex),
            ),
            ("BEGIN", StatementBehavior::Session(SessionBehavior::Begin)),
            (
                "COMMIT",
                StatementBehavior::Session(SessionBehavior::Commit),
            ),
            (
                "ROLLBACK",
                StatementBehavior::Session(SessionBehavior::Rollback),
            ),
        ];

        for dialect in SqlDialect::ALL.iter().copied() {
            for (source, expected) in cases {
                let classified = classify(dialect, source)
                    .unwrap_or_else(|error| panic!("{dialect} rejected {source}: {error}"));
                assert_eq!(classified.behaviors(), [expected], "{dialect}: {source}");
                assert_eq!(classified.behavior(0), Some(expected));
                assert_eq!(classified.behavior(1), None);
                assert_eq!(classified.statement_count(), 1);
                assert_eq!(classified.is_read_only(), expected.is_read_only());
            }
        }
    }

    #[test]
    fn transaction_aliases_keep_their_semantic_session_behavior() {
        for source in ["BEGIN", "BEGIN TRANSACTION", "BEGIN WORK"] {
            assert_eq!(
                classify(SqlDialect::PostgreSql, source)
                    .unwrap()
                    .behaviors(),
                [StatementBehavior::Session(SessionBehavior::Begin)]
            );
        }
        for source in [
            "COMMIT",
            "COMMIT TRANSACTION",
            "COMMIT WORK",
            "COMMIT TRAN",
            "COMMIT AND NO CHAIN",
        ] {
            assert_eq!(
                classify(SqlDialect::PostgreSql, source)
                    .unwrap()
                    .behaviors(),
                [StatementBehavior::Session(SessionBehavior::Commit)]
            );
        }
        for source in [
            "ROLLBACK",
            "ROLLBACK TRANSACTION",
            "ROLLBACK WORK",
            "ROLLBACK TRAN",
            "ROLLBACK AND NO CHAIN",
            "ABORT",
            "ABORT AND NO CHAIN",
        ] {
            assert_eq!(
                classify(SqlDialect::PostgreSql, source)
                    .unwrap()
                    .behaviors(),
                [StatementBehavior::Session(SessionBehavior::Rollback)]
            );
        }
    }

    #[test]
    fn behavior_words_and_semicolons_in_lexical_content_do_not_change_classification() {
        for (dialect, quoted_alias) in [
            (SqlDialect::Sqlite, "\"DELETE\""),
            (SqlDialect::PostgreSql, "\"DELETE\""),
            (SqlDialect::MySql, "`DELETE`"),
        ] {
            let source = format!(
                "SELECT 'INSERT; UPDATE' AS {quoted_alias} \
                 /* CREATE TABLE; BEGIN */; \
                 SELECT 2 AS session_behavior -- COMMIT; ROLLBACK\n"
            );
            let classified = classify(dialect, &source).unwrap();
            assert_eq!(
                classified.behaviors(),
                [StatementBehavior::Read, StatementBehavior::Read],
                "{dialect}: {source}"
            );
        }
    }

    #[test]
    fn empty_requests_are_invalid_but_every_singleton_behavior_is_accepted() {
        for source in ["", " ", "-- comment only\n", "/* comment only */"] {
            let error = classify(SqlDialect::Sqlite, source).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
            assert_eq!(
                error.to_string(),
                "a SQL request must contain at least one top-level statement"
            );
        }

        for source in [
            "SELECT 1",
            "INSERT INTO widgets (id) VALUES (1)",
            "CREATE TABLE widgets (id INTEGER)",
            "BEGIN",
        ] {
            assert_eq!(
                classify(SqlDialect::Sqlite, source)
                    .unwrap()
                    .statement_count(),
                1,
                "{source}"
            );
        }
    }

    #[test]
    fn every_pair_of_coarse_behaviors_obeys_the_read_only_batch_matrix() {
        let representatives = [
            ("SELECT 1", "read"),
            ("INSERT INTO widgets (id) VALUES (1)", "write"),
            ("CREATE TABLE widgets (id INTEGER)", "schema"),
            ("BEGIN", "session"),
        ];

        for dialect in SqlDialect::ALL.iter().copied() {
            for (left, left_category) in representatives.iter().copied() {
                for (right, right_category) in representatives.iter().copied() {
                    let source = format!("{left}; {right}");
                    if left_category == "read" && right_category == "read" {
                        let classified = classify(dialect, &source).unwrap();
                        assert_eq!(classified.behaviors(), [StatementBehavior::Read; 2]);
                        assert!(classified.is_read_only());
                        continue;
                    }

                    let (ordinal, category) = if left_category == "read" {
                        (2, right_category)
                    } else {
                        (1, left_category)
                    };
                    let error = classify(dialect, &source).unwrap_err();
                    assert_eq!(
                        error.kind(),
                        EngineErrorKind::Unsupported,
                        "{dialect}: {source}"
                    );
                    assert_eq!(
                        error.to_string(),
                        format!(
                            "statement {ordinal} has {category} behavior; multi-statement requests may contain only read statements"
                        )
                    );
                }
            }
        }
    }

    #[test]
    fn first_non_read_statement_deterministically_controls_rejection() {
        for (source, ordinal, category) in [
            (
                "UPDATE widgets SET id = 1; CREATE TABLE later (id INTEGER); BEGIN",
                1,
                "write",
            ),
            (
                "SELECT 1; CREATE TABLE later (id INTEGER); DELETE FROM widgets",
                2,
                "schema",
            ),
            (
                "SELECT 1; SELECT 2; COMMIT; INSERT INTO widgets (id) VALUES (1)",
                3,
                "session",
            ),
        ] {
            let error = classify(SqlDialect::Sqlite, source).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Unsupported);
            assert!(error.diagnostic().contains(&format!("statement {ordinal}")));
            assert!(error.diagnostic().contains(category));
        }
    }

    #[test]
    fn parser_statement_boundaries_remain_exact() {
        let maximum = std::iter::repeat_n("SELECT 1", MAX_PARSED_SQL_STATEMENTS)
            .collect::<Vec<_>>()
            .join(";");
        let classified = classify(SqlDialect::Sqlite, &maximum).unwrap();
        assert_eq!(classified.statement_count(), MAX_PARSED_SQL_STATEMENTS);
        assert!(classified.is_read_only());

        let last_write = format!(
            "{}; UPDATE widgets SET id = 1",
            std::iter::repeat_n("SELECT 1", MAX_PARSED_SQL_STATEMENTS - 1)
                .collect::<Vec<_>>()
                .join(";")
        );
        let error = classify(SqlDialect::Sqlite, &last_write).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Unsupported);
        assert!(
            error
                .diagnostic()
                .contains(&format!("statement {MAX_PARSED_SQL_STATEMENTS}"))
        );

        let over_limit = format!("{maximum}; SELECT 1");
        let error = classify(SqlDialect::Sqlite, &over_limit).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    }

    #[test]
    fn earlier_frontend_errors_precede_batch_policy_and_later_normalization() {
        let unsupported = parse(
            SqlDialect::Sqlite,
            "SELECT 1; CREATE VIEW private_view AS SELECT 2",
        )
        .and_then(validate_common_subset)
        .and_then(|common| classify_statements(&common))
        .unwrap_err();
        assert_eq!(unsupported.kind(), EngineErrorKind::Unsupported);
        assert!(unsupported.diagnostic().contains("statement 2"));
        assert!(unsupported.diagnostic().contains("common SQL subset"));

        let common = common(
            SqlDialect::Sqlite,
            "SELECT 1; UPDATE widgets SET id = :private_marker",
        )
        .unwrap();
        let batch_error = classify_statements(&common).unwrap_err();
        assert_eq!(batch_error.kind(), EngineErrorKind::Unsupported);
        assert!(batch_error.diagnostic().contains("statement 2"));
        let marker_error = normalize_placeholders(common).unwrap_err();
        assert_eq!(marker_error.kind(), EngineErrorKind::Unsupported);
    }

    #[test]
    fn an_impossible_unvalidated_statement_family_is_an_internal_invariant() {
        let parsed = parse(SqlDialect::Sqlite, "DROP TABLE private_table").unwrap();
        let error = classify_statement(&parsed.statements()[0]).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert_eq!(
            error.to_string(),
            "validated SQL contains an unclassified statement type"
        );
        assert!(!error.diagnostic().contains("private_table"));
    }

    #[test]
    fn diagnostics_and_debug_never_render_source_sql() {
        let source = "SELECT 'private literal'; UPDATE private_table SET private_column = 1";
        let error = classify(SqlDialect::Sqlite, source).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Unsupported);
        for private in [source, "private literal", "private_table", "private_column"] {
            assert!(!error.diagnostic().contains(private));
        }

        let classified = classify(
            SqlDialect::Sqlite,
            "SELECT 'private literal'; SELECT private_column FROM private_table",
        )
        .unwrap();
        let debug = format!("{classified:?}");
        assert!(debug.contains("statement_count"));
        assert!(debug.contains("Read"));
        for private in ["private literal", "private_table", "private_column"] {
            assert!(!debug.contains(private));
        }
    }

    #[test]
    fn classification_is_owned_thread_safe_deterministic_and_recoverable() {
        fn assert_owned<T: Clone + Send + Sync + 'static>() {}
        assert_owned::<StatementBatchClassification>();
        assert_owned::<StatementBehavior>();
        assert_owned::<WriteBehavior>();
        assert_owned::<SchemaBehavior>();
        assert_owned::<SessionBehavior>();

        let shared_common =
            Arc::new(common(SqlDialect::PostgreSql, "SELECT $1; SELECT $2; SELECT $1").unwrap());
        let expected = classify_statements(&shared_common).unwrap();
        let workers = (0..24)
            .map(|_| {
                let common = Arc::clone(&shared_common);
                thread::spawn(move || classify_statements(&common).unwrap())
            })
            .collect::<Vec<_>>();
        for worker in workers {
            assert_eq!(worker.join().unwrap(), expected);
        }

        let rejected =
            Arc::new(common(SqlDialect::Sqlite, "SELECT 1; DELETE FROM private_table").unwrap());
        let expected_error = classify_statements(&rejected).unwrap_err();
        assert_eq!(expected_error.kind(), EngineErrorKind::Unsupported);
        let barrier = Arc::new(Barrier::new(25));
        let workers = (0..24)
            .map(|_| {
                let common = Arc::clone(&rejected);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    classify_statements(&common).unwrap_err()
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        for worker in workers {
            let error = worker.join().unwrap();
            assert_eq!(error.kind(), EngineErrorKind::Unsupported);
            assert_eq!(error.diagnostic(), expected_error.diagnostic());
        }

        assert_eq!(
            classify(SqlDialect::Sqlite, "SELECT 1; SELECT 2")
                .unwrap()
                .behaviors(),
            [StatementBehavior::Read; 2]
        );
    }

    #[test]
    fn normalized_helper_applies_the_identical_whole_batch_policy() {
        let read_common = common(SqlDialect::MySql, "SELECT ?; SELECT ?").unwrap();
        let public = classify_statements(&read_common).unwrap();
        let normalized = normalize_placeholders(read_common).unwrap();
        assert_eq!(classify_normalized_statements(&normalized).unwrap(), public);

        let mixed_common = common(
            SqlDialect::MySql,
            "SELECT ?; INSERT INTO widgets (id) VALUES (?)",
        )
        .unwrap();
        let public_error = classify_statements(&mixed_common).unwrap_err();
        let normalized = normalize_placeholders(mixed_common).unwrap();
        let helper_error = classify_normalized_statements(&normalized).unwrap_err();
        assert_eq!(helper_error.kind(), public_error.kind());
        assert_eq!(helper_error.diagnostic(), public_error.diagnostic());
    }
}
