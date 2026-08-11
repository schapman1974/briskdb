use std::{borrow::Cow, fmt, ops::ControlFlow};

use sqlparser::{
    ast::{Expr, ObjectNamePart, ObjectType, Statement as AstStatement, visit_expressions},
    dialect::{Dialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect},
    parser::{Parser, ParserError},
};

use crate::core::{EngineError, EngineErrorKind, EngineResult};

/// Maximum byte length accepted by the protocol-neutral parser facade.
pub const MAX_PARSED_SQL_BYTES: usize = 65_536;

/// Maximum number of AST statements returned from one parser invocation.
pub const MAX_PARSED_SQL_STATEMENTS: usize = 256;

/// Maximum recursive parser depth for one SQL input.
pub const SQL_PARSE_RECURSION_LIMIT: usize = 32;

/// The syntax dialect of SQL before any later compatibility normalization.
///
/// Callers must select the dialect from trusted connection or API context.
/// BriskDB never guesses a dialect and never falls back to a permissive union.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlDialect {
    Sqlite,
    PostgreSql,
    MySql,
}

impl SqlDialect {
    /// Every source dialect currently understood by the parser facade.
    pub const ALL: &'static [Self] = &[Self::Sqlite, Self::PostgreSql, Self::MySql];

    /// Return the stable display name used in trusted diagnostics.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Sqlite => "SQLite",
            Self::PostgreSql => "PostgreSQL",
            Self::MySql => "MySQL",
        }
    }
}

impl fmt::Display for SqlDialect {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

/// Byte-exact SQL source together with its ordered, opaque parsed AST.
///
/// The AST deliberately remains an implementation detail until the common SQL
/// subset and planner consume it. Rendering an AST can remove comments and
/// normalize spelling or whitespace, so [`ParsedSql::source`] is the only SQL
/// text that later execution may treat as the caller's original input.
#[derive(Clone, PartialEq, Eq)]
pub struct ParsedSql {
    dialect: SqlDialect,
    source: String,
    statements: Vec<AstStatement>,
}

impl ParsedSql {
    /// Return the explicitly selected source dialect.
    pub const fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    /// Return the caller's byte-exact SQL source.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Return the number of ordered top-level statements in the opaque AST.
    pub fn statement_count(&self) -> usize {
        self.statements.len()
    }

    /// Return whether the input contained no statements, such as whitespace or
    /// comments only.
    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    pub(super) fn statements(&self) -> &[AstStatement] {
        &self.statements
    }
}

impl fmt::Debug for ParsedSql {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParsedSql")
            .field("dialect", &self.dialect)
            .field("source_bytes", &self.source.len())
            .field("statement_count", &self.statements.len())
            .finish()
    }
}

/// Parse SQL with an explicit source dialect into an ordered, opaque AST.
///
/// This is a syntax operation only. A successful parse does not mean that the
/// statement belongs to BriskDB's supported subset, is semantically valid, is
/// authorized, has a routing plan, or can execute on SQLite.
pub fn parse<'a>(dialect: SqlDialect, source: impl Into<Cow<'a, str>>) -> EngineResult<ParsedSql> {
    let source = source.into();
    if source.len() > MAX_PARSED_SQL_BYTES {
        return Err(EngineError::new(
            EngineErrorKind::LimitExceeded,
            format!("SQL text exceeds the {MAX_PARSED_SQL_BYTES}-byte parser limit"),
        ));
    }
    if source.as_bytes().contains(&0) {
        return Err(EngineError::new(
            EngineErrorKind::InvalidQuery,
            "SQL text contains a NUL byte",
        ));
    }

    let statements = match dialect {
        SqlDialect::Sqlite => parse_with(&SQLiteDialect {}, &source),
        SqlDialect::PostgreSql => parse_with(&PostgreSqlDialect {}, &source),
        SqlDialect::MySql => parse_with(&MySqlDialect {}, &source),
    }
    .map_err(|error| classify_parser_error(dialect, error))?;

    if statements.len() > MAX_PARSED_SQL_STATEMENTS {
        return Err(EngineError::new(
            EngineErrorKind::LimitExceeded,
            format!("SQL text exceeds the {MAX_PARSED_SQL_STATEMENTS}-statement parser limit"),
        ));
    }

    Ok(ParsedSql {
        dialect,
        source: source.into_owned(),
        statements,
    })
}

/// Reject data-moving SQL from the application-schema migration path once an
/// authoritative placement catalog exists. Those migrations may alter schema
/// while preserving registered tables and shard keys, but row movement needs
/// a separate placement-aware coordinator.
pub(crate) fn validate_authoritative_schema_migration(source: &str) -> EngineResult<()> {
    let parsed = parse(SqlDialect::Sqlite, source)?;
    let unsafe_statement = parsed.statements.iter().any(|statement| match statement {
        AstStatement::Insert(_)
        | AstStatement::Update(_)
        | AstStatement::Delete(_)
        | AstStatement::Merge(_)
        | AstStatement::Truncate(_)
        | AstStatement::CreateTrigger(_) => true,
        AstStatement::CreateTable(table) => table.query.is_some(),
        AstStatement::Drop { object_type, .. } => *object_type == ObjectType::Table,
        _ => false,
    });
    if unsafe_statement {
        Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "authoritative-catalog schema migrations cannot move rows, drop tables, or create triggers",
        ))
    } else {
        Ok(())
    }
}

/// Reject stored schema expressions that can observe SQLite write history on
/// a physical connection reused by stateless catalog writes.
pub(crate) fn validate_stateless_catalog_schema_sql(source: &str) -> EngineResult<()> {
    let parsed = parse(SqlDialect::Sqlite, source).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::FailedPrecondition,
            "persistent SQLite schema SQL could not be validated for stateless write reuse",
            error,
        )
    })?;
    let found = parsed.statements().iter().any(|statement| {
        visit_expressions(statement, |expression| {
            let is_connection_local = match expression {
                Expr::Function(function) => match function.name.0.as_slice() {
                    [ObjectNamePart::Identifier(name)] => {
                        connection_local_counter_function(&name.value)
                    }
                    _ => false,
                },
                _ => false,
            };
            if is_connection_local {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .is_break()
    });
    if found {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "persistent SQLite schema expressions cannot use connection-local counter functions",
        ));
    }
    Ok(())
}

fn connection_local_counter_function(function: &str) -> bool {
    ["last_insert_rowid", "changes", "total_changes"]
        .iter()
        .any(|counter| function.eq_ignore_ascii_case(counter))
}

fn parse_with(dialect: &dyn Dialect, source: &str) -> Result<Vec<AstStatement>, ParserError> {
    let mut parser = Parser::new(dialect)
        .with_recursion_limit(SQL_PARSE_RECURSION_LIMIT)
        .try_with_sql(source)?;
    parser.parse_statements()
}

fn classify_parser_error(dialect: SqlDialect, error: ParserError) -> EngineError {
    match error {
        ParserError::RecursionLimitExceeded => EngineError::from_source(
            EngineErrorKind::LimitExceeded,
            format!(
                "{dialect} SQL exceeds the parser recursion limit of {SQL_PARSE_RECURSION_LIMIT}"
            ),
            error,
        ),
        ParserError::TokenizerError(_) | ParserError::ParserError(_) => EngineError::from_source(
            EngineErrorKind::InvalidQuery,
            format!("SQL is not valid {dialect} syntax"),
            error,
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        process::{Command, Stdio},
        thread,
        time::{Duration, Instant},
    };

    use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement, Value};

    use super::*;

    const INTERVAL_RECURSION_CHILD: &str = "BRISKDB_INTERVAL_RECURSION_CHILD";

    fn first_projection_expression(parsed: &ParsedSql) -> &Expr {
        let Statement::Query(query) = &parsed.statements[0] else {
            panic!("expected a query AST")
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected a SELECT AST")
        };
        let SelectItem::UnnamedExpr(expression) = &select.projection[0] else {
            panic!("expected an unnamed projection expression")
        };
        expression
    }

    fn assert_placeholder(parsed: &ParsedSql, expected: &str) {
        let Expr::Value(value) = first_projection_expression(parsed) else {
            panic!("expected a placeholder value")
        };
        assert_eq!(value.value, Value::Placeholder(expected.to_owned()));
    }

    #[test]
    fn dialect_names_and_iteration_are_stable() {
        assert_eq!(
            SqlDialect::ALL,
            &[
                SqlDialect::Sqlite,
                SqlDialect::PostgreSql,
                SqlDialect::MySql
            ]
        );
        assert_eq!(SqlDialect::Sqlite.name(), "SQLite");
        assert_eq!(SqlDialect::PostgreSql.name(), "PostgreSQL");
        assert_eq!(SqlDialect::MySql.name(), "MySQL");
        assert_eq!(SqlDialect::MySql.to_string(), "MySQL");
    }

    #[test]
    fn common_sql_has_the_same_ast_in_every_source_dialect() {
        let source = "SELECT tenant_id FROM widgets WHERE tenant_id = 7";
        let parsed = SqlDialect::ALL
            .iter()
            .copied()
            .map(|dialect| parse(dialect, source).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(parsed[0].statements, parsed[1].statements);
        assert_eq!(parsed[1].statements, parsed[2].statements);
        assert!(parsed.iter().all(|batch| batch.source() == source));
    }

    #[test]
    fn explicit_dialects_preserve_placeholder_and_quote_semantics_without_fallback() {
        let sqlite = parse(SqlDialect::Sqlite, "SELECT ?1").unwrap();
        assert_placeholder(&sqlite, "?1");
        let postgres = parse(SqlDialect::PostgreSql, "SELECT $1").unwrap();
        assert_placeholder(&postgres, "$1");
        let mysql = parse(SqlDialect::MySql, "SELECT ?").unwrap();
        assert_placeholder(&mysql, "?");

        assert_eq!(
            parse(SqlDialect::PostgreSql, "SELECT ?")
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidQuery
        );
        assert!(matches!(
            first_projection_expression(&parse(SqlDialect::MySql, "SELECT $1").unwrap()),
            Expr::Identifier(identifier) if identifier.value == "$1"
        ));
        assert!(matches!(
            first_projection_expression(
                &parse(SqlDialect::PostgreSql, "SELECT \"mixedCase\"").unwrap()
            ),
            Expr::Identifier(identifier) if identifier.value == "mixedCase"
        ));
        assert!(matches!(
            first_projection_expression(
                &parse(SqlDialect::MySql, "SELECT \"mixedCase\"").unwrap()
            ),
            Expr::Value(value) if value.value == Value::DoubleQuotedString("mixedCase".to_owned())
        ));
        assert!(matches!(
            first_projection_expression(
                &parse(SqlDialect::MySql, "SELECT `backtick_name`").unwrap()
            ),
            Expr::Identifier(identifier) if identifier.value == "backtick_name"
        ));
        assert!(matches!(
            first_projection_expression(
                &parse(SqlDialect::Sqlite, "SELECT [bracket_name]").unwrap()
            ),
            Expr::Identifier(identifier) if identifier.value == "bracket_name"
        ));
        assert_eq!(
            parse(SqlDialect::PostgreSql, "SELECT `backtick_name`")
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidQuery
        );

        assert!(parse(SqlDialect::Sqlite, "CREATE TABLE untyped(value)").is_ok());
        assert_eq!(
            parse(SqlDialect::PostgreSql, "CREATE TABLE untyped(value)")
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidQuery
        );
    }

    #[test]
    fn representative_phase_three_shapes_parse_without_defining_support() {
        let cases = [
            "CREATE TABLE widgets(id INTEGER PRIMARY KEY, tenant_id INTEGER)",
            "CREATE INDEX widgets_tenant ON widgets(tenant_id)",
            "SELECT id FROM widgets WHERE tenant_id = 1",
            "INSERT INTO widgets(id, tenant_id) VALUES (1, 1)",
            "UPDATE widgets SET tenant_id = 2 WHERE id = 1",
            "DELETE FROM widgets WHERE id = 1",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
        ];

        for dialect in SqlDialect::ALL.iter().copied() {
            for source in cases {
                let parsed = parse(dialect, source)
                    .unwrap_or_else(|error| panic!("{dialect} rejected {source}: {error}"));
                assert_eq!(parsed.statement_count(), 1, "{dialect}: {source}");
            }
        }
    }

    #[test]
    fn authoritative_schema_migrations_reject_every_row_moving_shape() {
        for source in [
            "INSERT INTO widgets (id) VALUES (1)",
            "UPDATE widgets SET id = 2 WHERE id = 1",
            "DELETE FROM widgets WHERE id = 1",
            "DROP TABLE widgets",
            "CREATE TABLE copied AS SELECT * FROM widgets",
            "CREATE TRIGGER move_row AFTER INSERT ON widgets BEGIN DELETE FROM widgets WHERE id = NEW.id; END",
        ] {
            assert_eq!(
                validate_authoritative_schema_migration(source)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::FailedPrecondition,
                "{source}"
            );
        }
        for source in [
            "CREATE INDEX widgets_id ON widgets (id)",
            "ALTER TABLE widgets ADD COLUMN payload TEXT",
            "CREATE VIEW widget_ids AS SELECT id FROM widgets",
        ] {
            validate_authoritative_schema_migration(source)
                .unwrap_or_else(|error| panic!("{source}: {error}"));
        }
    }

    #[test]
    fn stateless_catalog_schema_rejects_only_connection_local_function_calls() {
        for source in [
            "CREATE TABLE records(id INTEGER, previous INTEGER DEFAULT (last_insert_rowid()))",
            "CREATE TABLE records(id INTEGER, CHECK (ChAnGeS() >= 0))",
            "CREATE TABLE records(id INTEGER, CHECK (\"TOTAL_CHANGES\"() >= 0))",
            "CREATE TABLE records(id INTEGER, CHECK ([changes]() >= 0))",
            "CREATE TABLE records(id INTEGER, CHECK (`last_insert_rowid`() >= 0))",
            "CREATE TABLE records(id INTEGER, observed INTEGER GENERATED ALWAYS AS (changes()) STORED)",
            "CREATE INDEX records_recent ON records(id) WHERE total_changes() >= 0",
        ] {
            assert_eq!(
                validate_stateless_catalog_schema_sql(source)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::FailedPrecondition,
                "{source}"
            );
        }

        for source in [
            "CREATE TABLE changes(changes TEXT DEFAULT 'total_changes()', last_insert_rowid INTEGER, CHECK(changes <> 'changes()'))",
            "CREATE TABLE records(id INTEGER, normalized TEXT DEFAULT (lower('CHANGES')))",
            "CREATE INDEX total_changes ON records(id)",
        ] {
            validate_stateless_catalog_schema_sql(source)
                .unwrap_or_else(|error| panic!("{source}: {error}"));
        }
    }

    #[test]
    fn ordered_batches_do_not_split_semicolons_inside_literals_or_comments() {
        let source = "/* lead ; */ SELECT ';' AS marker; -- middle ;\nSELECT 2";
        let parsed = parse(SqlDialect::PostgreSql, source).unwrap();

        assert_eq!(parsed.source(), source);
        assert_eq!(parsed.dialect(), SqlDialect::PostgreSql);
        assert_eq!(parsed.statement_count(), 2);
        assert!(!parsed.is_empty());
        assert!(
            parsed
                .statements
                .iter()
                .all(|statement| matches!(statement, Statement::Query(_)))
        );
    }

    #[test]
    fn whitespace_and_comments_produce_an_empty_ast_without_policy_classification() {
        for source in ["", "   \n\t", "  -- no statement ;\n /* still none */ "] {
            let parsed = parse(SqlDialect::Sqlite, source).unwrap();

            assert!(parsed.is_empty());
            assert_eq!(parsed.statement_count(), 0);
            assert_eq!(parsed.source(), source);
        }
    }

    #[test]
    fn parser_is_syntax_only_and_does_not_define_semantic_support() {
        let parsed = parse(
            SqlDialect::PostgreSql,
            "CREATE TABLE duplicated(id INTEGER, id INTEGER)",
        )
        .unwrap();
        assert_eq!(parsed.statement_count(), 1);
    }

    #[test]
    fn source_is_byte_exact_while_debug_output_is_bounded_and_redacted() {
        let source = "select /* private comment */ 'sensitive literal'";
        let parsed = parse(SqlDialect::Sqlite, source).unwrap();
        let debug = format!("{parsed:?}");

        assert_eq!(parsed.source(), source);
        assert!(debug.contains("source_bytes"));
        assert!(debug.contains("statement_count"));
        assert!(!debug.contains("private comment"));
        assert!(!debug.contains("sensitive literal"));
        assert_eq!(parsed.clone(), parsed);
    }

    #[test]
    fn valid_unicode_corpus_is_owned_and_parsed_without_panicking() {
        let sources = [
            "SELECT '雪だるま ☃'",
            "SELECT 'مرحبا بالعالم'",
            "SELECT '👩‍💻; not a separator'",
            "-- Ελληνικό σχόλιο ;\nSELECT 'naïve café'",
        ];

        for dialect in SqlDialect::ALL.iter().copied() {
            for source in sources {
                let outcome = std::panic::catch_unwind(|| parse(dialect, source));
                let parsed = outcome
                    .unwrap_or_else(|_| panic!("{dialect} panicked for valid UTF-8 input"))
                    .unwrap_or_else(|error| panic!("{dialect} rejected the UTF-8 corpus: {error}"));
                assert_eq!(parsed.source().as_bytes(), source.as_bytes());
                assert_eq!(parsed.statement_count(), 1);
            }
        }
    }

    #[test]
    fn byte_and_statement_limits_are_exact() {
        let mut exact = "SELECT 1".to_owned();
        exact.push_str(&" ".repeat(MAX_PARSED_SQL_BYTES - exact.len()));
        let parsed = parse(SqlDialect::Sqlite, exact.clone()).unwrap();
        assert_eq!(parsed.source().len(), MAX_PARSED_SQL_BYTES);

        exact.push(' ');
        let too_long = parse(SqlDialect::Sqlite, exact).unwrap_err();
        assert_eq!(too_long.kind(), EngineErrorKind::LimitExceeded);

        let exact_batch = (0..MAX_PARSED_SQL_STATEMENTS)
            .map(|_| "SELECT 1")
            .collect::<Vec<_>>()
            .join(";");
        assert_eq!(
            parse(SqlDialect::Sqlite, exact_batch)
                .unwrap()
                .statement_count(),
            MAX_PARSED_SQL_STATEMENTS
        );

        let too_many = (0..=MAX_PARSED_SQL_STATEMENTS)
            .map(|_| "SELECT 1")
            .collect::<Vec<_>>()
            .join(";");
        let error = parse(SqlDialect::Sqlite, too_many).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    }

    #[test]
    fn nul_and_malformed_sql_are_invalid_queries_with_fixed_diagnostics() {
        let nul = parse(SqlDialect::Sqlite, "SELECT\0private").unwrap_err();
        assert_eq!(nul.kind(), EngineErrorKind::InvalidQuery);
        assert_eq!(nul.diagnostic(), "SQL text contains a NUL byte");

        let malformed_source = "SELECT 'sensitive literal' +";
        let malformed = parse(SqlDialect::PostgreSql, malformed_source).unwrap_err();
        assert_eq!(malformed.kind(), EngineErrorKind::InvalidQuery);
        assert_eq!(malformed.diagnostic(), "SQL is not valid PostgreSQL syntax");
        assert!(!malformed.diagnostic().contains(malformed_source));
        assert!(malformed.source().is_some());
    }

    #[test]
    fn configured_expression_recursion_is_a_limit_error() {
        let expression = (0..SQL_PARSE_RECURSION_LIMIT + 8)
            .fold("terminal_value".to_owned(), |nested, index| {
                format!("value_{index} OR ({nested})")
            });
        let source = format!("SELECT * FROM widgets WHERE {expression}");
        let error = parse(SqlDialect::PostgreSql, source).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert!(
            error
                .source()
                .is_some_and(|source| source.downcast_ref::<ParserError>()
                    == Some(&ParserError::RecursionLimitExceeded))
        );
    }

    #[test]
    fn deep_interval_recursion_is_rejected_without_aborting_the_process() {
        if std::env::var_os(INTERVAL_RECURSION_CHILD).is_some() {
            let source = format!("SELECT {}1", "INTERVAL ".repeat(1_000));
            let error = parse(SqlDialect::PostgreSql, source).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
            assert!(error.source().is_some_and(|source| {
                source.downcast_ref::<ParserError>() == Some(&ParserError::RecursionLimitExceeded)
            }));
            return;
        }

        let qualified_name = std::any::type_name_of_val(
            &deep_interval_recursion_is_rejected_without_aborting_the_process,
        );
        let crate_prefix = concat!(env!("CARGO_CRATE_NAME"), "::");
        let test_name = qualified_name
            .strip_prefix(crate_prefix)
            .unwrap_or(qualified_name);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(test_name)
            .arg("--nocapture")
            .env(INTERVAL_RECURSION_CHILD, "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                let output = child.wait_with_output().unwrap();
                panic!(
                    "parser regression child timed out: stdout={} stderr={}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            thread::sleep(Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "nested parser child failed: status={} stdout={} stderr={}",
            output.status,
            stdout,
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            stdout.contains("running 1 test") && stdout.contains(test_name),
            "parser regression child did not run the exact test: {stdout}"
        );
    }

    #[test]
    fn parsed_sql_is_owned_send_sync_and_parser_state_is_not_shared() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ParsedSql>();

        let threads = (0..24)
            .map(|index| {
                thread::spawn(move || {
                    let dialect = SqlDialect::ALL[index % SqlDialect::ALL.len()];
                    let source = format!("SELECT {index}");
                    parse(dialect, source)
                })
            })
            .collect::<Vec<_>>();

        for (index, handle) in threads.into_iter().enumerate() {
            let parsed = handle.join().unwrap().unwrap();
            assert_eq!(parsed.source(), format!("SELECT {index}"));
            assert_eq!(parsed.statement_count(), 1);
        }
    }
}
