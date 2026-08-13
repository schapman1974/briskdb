use std::{fmt, ops::ControlFlow};

use sqlparser::ast::{
    BinaryLength, CharacterLength, DataType as AstDataType, ExactNumberInfo, Ident, LimitClause,
    Offset, OffsetRows, Query as AstQuery, Statement as AstStatement, Value as AstValue,
    ValueWithSpan, VisitMut, VisitorMut,
};

use super::{GeneratedTableIntent, NormalizedSql, SqlDialect, StatementParameters, generated};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

/// The SQL compatibility policy applied after structural validation and
/// placeholder normalization.
///
/// BriskDB deliberately has no default translation mode. A caller selects one
/// from trusted connection or API context for every request.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlTranslationMode {
    /// Translate the documented common compatibility surface to canonical
    /// SQLite SQL.
    Compatibility,
    /// Preserve validated SQLite SQL directly, apart from the placeholder
    /// normalization that already produced [`NormalizedSql`].
    StrictSqlite,
}

impl SqlTranslationMode {
    /// Every currently implemented translation mode.
    pub const ALL: &'static [Self] = &[Self::Compatibility, Self::StrictSqlite];

    /// Return the stable display name used in trusted diagnostics.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Compatibility => "compatibility",
            Self::StrictSqlite => "strict SQLite",
        }
    }
}

impl fmt::Display for SqlTranslationMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

/// Owned SQL ready for a later SQLite prepare step.
///
/// The original validated and normalized request remains available through
/// [`Self::normalized_sql`]. Compatibility output is a separate canonical
/// representation; it is never authoritative for migration identity or source
/// diagnostics.
#[derive(Clone, PartialEq, Eq)]
pub struct TranslatedSql {
    normalized: NormalizedSql,
    mode: SqlTranslationMode,
    sqlite_sql: String,
    generated_table_intents: Box<[GeneratedTableIntent]>,
}

impl TranslatedSql {
    /// Return the explicitly selected source dialect.
    pub const fn dialect(&self) -> SqlDialect {
        self.normalized.dialect()
    }

    /// Return the translation policy used to produce the SQLite SQL.
    pub const fn mode(&self) -> SqlTranslationMode {
        self.mode
    }

    /// Return the caller's byte-exact original SQL source.
    pub fn source(&self) -> &str {
        self.normalized.source()
    }

    /// Return the separate SQL text for a later SQLite prepare step.
    pub fn sqlite_sql(&self) -> &str {
        &self.sqlite_sql
    }

    /// Return every generated-key table declaration retained from the source
    /// AST, in statement order.
    ///
    /// An empty slice means the SQL declared no supported generated-key table.
    /// These values are syntax intent only; they do not prove that catalog
    /// metadata has been durably installed.
    pub fn generated_table_intents(&self) -> &[GeneratedTableIntent] {
        &self.generated_table_intents
    }

    /// Borrow the validated placeholder-normalized request retained for
    /// routing inference and bound-statement planning.
    pub const fn normalized_sql(&self) -> &NormalizedSql {
        &self.normalized
    }

    /// Return one parameter layout for each top-level statement.
    pub fn statement_parameters(&self) -> &[StatementParameters] {
        self.normalized.statement_parameters()
    }

    /// Return the number of independently translated top-level statements.
    pub fn statement_count(&self) -> usize {
        self.normalized.statement_count()
    }

    /// Return whether the parsed input contained no statements.
    pub fn is_empty(&self) -> bool {
        self.normalized.is_empty()
    }
}

impl fmt::Debug for TranslatedSql {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TranslatedSql")
            .field("dialect", &self.dialect())
            .field("mode", &self.mode)
            .field("source_bytes", &self.source().len())
            .field("sqlite_sql_bytes", &self.sqlite_sql.len())
            .field("statement_count", &self.statement_count())
            .field(
                "generated_table_intents",
                &self.generated_table_intents.len(),
            )
            .finish()
    }
}

/// Translate normalized common SQL into a separate SQLite representation.
///
/// Strict mode accepts only input parsed with [`SqlDialect::Sqlite`] and
/// returns its source-preserving placeholder-normalized SQL unchanged.
/// Compatibility mode canonicalizes the documented finite type and syntax
/// matrix. This function is stateless and does not inspect a catalog, route,
/// prepare, authorize, or execute any statement.
pub fn translate_sql(
    normalized: NormalizedSql,
    mode: SqlTranslationMode,
) -> EngineResult<TranslatedSql> {
    let generated = generated::analyze_generated_tables(&normalized)?;
    let generated_table_intents = generated
        .iter()
        .map(|generated| generated.intent().clone())
        .collect::<Vec<_>>()
        .into_boxed_slice();
    match mode {
        SqlTranslationMode::StrictSqlite => {
            if normalized.dialect() != SqlDialect::Sqlite {
                return Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    "strict SQLite translation requires SQLite source SQL",
                ));
            }
            let sqlite_sql = normalized.sqlite_parameter_sql().to_owned();
            Ok(TranslatedSql {
                normalized,
                mode,
                sqlite_sql,
                generated_table_intents,
            })
        }
        SqlTranslationMode::Compatibility => {
            let sqlite_sql = translate_compatibility(&normalized, &generated)?;
            Ok(TranslatedSql {
                normalized,
                mode,
                sqlite_sql,
                generated_table_intents,
            })
        }
    }
}

fn translate_compatibility(
    normalized: &NormalizedSql,
    generated_tables: &[generated::AnalyzedGeneratedTable],
) -> EngineResult<String> {
    let mut statements = normalized.common().statements().to_vec();
    if statements.len() != normalized.statement_count()
        || normalized.statement_parameters().len() != normalized.statement_count()
    {
        return Err(translation_invariant());
    }

    for (statement_index, statement) in statements.iter_mut().enumerate() {
        let generated = generated_tables
            .iter()
            .find(|generated| generated.intent().statement_index() == statement_index);
        translate_column_types(normalized.dialect(), statement_index, statement, generated)?;
        if let Some(generated) = generated {
            generated::rewrite_to_native_sqlite(statement, generated)?;
        }

        let expected_placeholders =
            normalized.statement_parameters()[statement_index].occurrence_count();
        let mut visitor = CompatibilityVisitor {
            normalized,
            statement_index,
            placeholder_count: 0,
        };
        if statement.visit(&mut visitor).is_break()
            || visitor.placeholder_count != expected_placeholders
        {
            return Err(translation_invariant());
        }
    }

    let sqlite_sql = statements
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ");
    if sqlite_sql.as_bytes().contains(&0) {
        return Err(EngineError::new(
            EngineErrorKind::InvalidQuery,
            "translated SQL contains a NUL byte",
        ));
    }
    Ok(sqlite_sql)
}

fn translate_column_types(
    dialect: SqlDialect,
    statement_index: usize,
    statement: &mut AstStatement,
    generated: Option<&generated::AnalyzedGeneratedTable>,
) -> EngineResult<()> {
    let AstStatement::CreateTable(table) = statement else {
        return Ok(());
    };

    for (column_index, column) in table.columns.iter_mut().enumerate() {
        if generated.is_some_and(|generated| generated.column_index() == column_index) {
            continue;
        }
        let Some(translated) = translated_data_type(dialect, &column.data_type) else {
            return Err(EngineError::new(
                EngineErrorKind::Unsupported,
                format!(
                    "statement {} column {} uses a type outside the translated SQL compatibility set",
                    statement_index + 1,
                    column_index + 1
                ),
            ));
        };
        column.data_type = translated;
    }
    Ok(())
}

fn translated_data_type(dialect: SqlDialect, data_type: &AstDataType) -> Option<AstDataType> {
    let translated = match dialect {
        SqlDialect::Sqlite => sqlite_compatibility_type(data_type),
        SqlDialect::PostgreSql => postgres_compatibility_type(data_type),
        SqlDialect::MySql => mysql_compatibility_type(data_type),
    }?;
    Some(translated.into_ast())
}

#[derive(Clone, Copy)]
enum CompatibilityType {
    Integer,
    Boolean,
    Real,
    Text,
    Blob,
}

impl CompatibilityType {
    const fn into_ast(self) -> AstDataType {
        match self {
            Self::Integer => AstDataType::BigInt(None),
            Self::Boolean => AstDataType::Boolean,
            Self::Real => AstDataType::Real,
            Self::Text => AstDataType::Text,
            Self::Blob => AstDataType::Blob(None),
        }
    }
}

fn sqlite_compatibility_type(data_type: &AstDataType) -> Option<CompatibilityType> {
    match data_type {
        AstDataType::TinyInt(None)
        | AstDataType::SmallInt(None)
        | AstDataType::MediumInt(None)
        | AstDataType::Int(None)
        | AstDataType::Integer(None)
        | AstDataType::BigInt(None) => Some(CompatibilityType::Integer),
        AstDataType::Bool | AstDataType::Boolean => Some(CompatibilityType::Boolean),
        AstDataType::Real => Some(CompatibilityType::Real),
        AstDataType::Text => Some(CompatibilityType::Text),
        AstDataType::Varchar(length)
        | AstDataType::CharacterVarying(length)
        | AstDataType::CharVarying(length)
            if varying_character_length_is_supported(length) =>
        {
            Some(CompatibilityType::Text)
        }
        AstDataType::Blob(None) => Some(CompatibilityType::Blob),
        _ => None,
    }
}

fn postgres_compatibility_type(data_type: &AstDataType) -> Option<CompatibilityType> {
    match data_type {
        AstDataType::Int2(None)
        | AstDataType::SmallInt(None)
        | AstDataType::Int(None)
        | AstDataType::Integer(None)
        | AstDataType::Int4(None)
        | AstDataType::BigInt(None)
        | AstDataType::Int8(None) => Some(CompatibilityType::Integer),
        AstDataType::Bool | AstDataType::Boolean => Some(CompatibilityType::Boolean),
        AstDataType::Float8 | AstDataType::DoublePrecision => Some(CompatibilityType::Real),
        AstDataType::Text => Some(CompatibilityType::Text),
        AstDataType::Varchar(length)
        | AstDataType::CharacterVarying(length)
        | AstDataType::CharVarying(length)
            if varying_character_length_is_supported(length) =>
        {
            Some(CompatibilityType::Text)
        }
        AstDataType::Bytea => Some(CompatibilityType::Blob),
        _ => None,
    }
}

fn mysql_compatibility_type(data_type: &AstDataType) -> Option<CompatibilityType> {
    match data_type {
        AstDataType::TinyInt(Some(1)) | AstDataType::Bool | AstDataType::Boolean => {
            Some(CompatibilityType::Boolean)
        }
        AstDataType::TinyInt(None)
        | AstDataType::SmallInt(None)
        | AstDataType::MediumInt(None)
        | AstDataType::Int(None)
        | AstDataType::Integer(None)
        | AstDataType::BigInt(None) => Some(CompatibilityType::Integer),
        AstDataType::Double(ExactNumberInfo::None) | AstDataType::DoublePrecision => {
            Some(CompatibilityType::Real)
        }
        AstDataType::Text => Some(CompatibilityType::Text),
        AstDataType::Varchar(length)
        | AstDataType::CharacterVarying(length)
        | AstDataType::CharVarying(length)
            if varying_character_length_is_supported(length) =>
        {
            Some(CompatibilityType::Text)
        }
        AstDataType::Blob(None) => Some(CompatibilityType::Blob),
        AstDataType::Varbinary(length) if varying_binary_length_is_supported(length) => {
            Some(CompatibilityType::Blob)
        }
        _ => None,
    }
}

fn varying_character_length_is_supported(length: &Option<CharacterLength>) -> bool {
    matches!(
        length,
        None | Some(CharacterLength::IntegerLength { unit: None, .. })
    )
}

fn varying_binary_length_is_supported(length: &Option<BinaryLength>) -> bool {
    matches!(length, None | Some(BinaryLength::IntegerLength { .. }))
}

#[derive(Clone, Copy)]
struct TranslationInvariant;

struct CompatibilityVisitor<'a> {
    normalized: &'a NormalizedSql,
    statement_index: usize,
    placeholder_count: usize,
}

impl VisitorMut for CompatibilityVisitor<'_> {
    type Break = TranslationInvariant;

    fn pre_visit_statement(&mut self, statement: &mut AstStatement) -> ControlFlow<Self::Break> {
        if let AstStatement::StartTransaction { transaction, .. } = statement {
            *transaction = None;
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &mut AstQuery) -> ControlFlow<Self::Break> {
        query.limit_clause = query
            .limit_clause
            .take()
            .map(|limit_clause| match limit_clause {
                LimitClause::LimitOffset {
                    limit,
                    mut offset,
                    limit_by,
                } => {
                    if let Some(offset) = &mut offset {
                        offset.rows = OffsetRows::None;
                    }
                    LimitClause::LimitOffset {
                        limit,
                        offset,
                        limit_by,
                    }
                }
                LimitClause::OffsetCommaLimit { offset, limit } => LimitClause::LimitOffset {
                    limit: Some(limit),
                    offset: Some(Offset {
                        value: offset,
                        rows: OffsetRows::None,
                    }),
                    limit_by: Vec::new(),
                },
            });
        ControlFlow::Continue(())
    }

    fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<Self::Break> {
        match &value.value {
            AstValue::Placeholder(_) => {
                let Some(index) = self
                    .normalized
                    .parameter_index(self.statement_index, value.span)
                else {
                    return ControlFlow::Break(TranslationInvariant);
                };
                value.value = AstValue::Placeholder(format!("?{index}"));
                self.placeholder_count += 1;
            }
            AstValue::Boolean(boolean) => {
                value.value = AstValue::Number(if *boolean { "1" } else { "0" }.to_owned(), false);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_ident(&mut self, identifier: &mut Ident) -> ControlFlow<Self::Break> {
        if identifier.quote_style == Some('`') {
            identifier.quote_style = Some('"');
        }
        ControlFlow::Continue(())
    }
}

fn translation_invariant() -> EngineError {
    EngineError::new(
        EngineErrorKind::Internal,
        "translation metadata does not match the retained normalized SQL",
    )
}

#[cfg(test)]
mod tests {
    use std::thread;

    use rusqlite::Connection;

    use super::*;
    use crate::{
        core::Value,
        sql::{
            GeneratedIdPolicyIntent, execute_batch, normalize_placeholders, parse, query,
            validate_common_subset,
        },
    };

    fn normalize(dialect: SqlDialect, source: &str) -> EngineResult<NormalizedSql> {
        normalize_placeholders(validate_common_subset(parse(dialect, source)?)?)
    }

    fn translate(
        dialect: SqlDialect,
        source: &str,
        mode: SqlTranslationMode,
    ) -> EngineResult<TranslatedSql> {
        translate_sql(normalize(dialect, source)?, mode)
    }

    fn compatibility(dialect: SqlDialect, source: &str) -> EngineResult<TranslatedSql> {
        translate(dialect, source, SqlTranslationMode::Compatibility)
    }

    #[test]
    fn modes_and_owned_result_are_public_exact_and_redacted() {
        fn assert_owned<T: Clone + Send + Sync + 'static>() {}
        assert_owned::<SqlTranslationMode>();
        assert_owned::<TranslatedSql>();

        assert_eq!(
            SqlTranslationMode::ALL,
            &[
                SqlTranslationMode::Compatibility,
                SqlTranslationMode::StrictSqlite
            ]
        );
        assert_eq!(SqlTranslationMode::Compatibility.name(), "compatibility");
        assert_eq!(
            SqlTranslationMode::StrictSqlite.to_string(),
            "strict SQLite"
        );

        let source = "-- private π\r\nCREATE TABLE `private_table` (`id` PRIVATE_TYPE DEFAULT TRUE);\r\nSELECT ?2, ?, 'private ?'; -- tail\r\n";
        let translated =
            translate(SqlDialect::Sqlite, source, SqlTranslationMode::StrictSqlite).unwrap();

        assert_eq!(translated.dialect(), SqlDialect::Sqlite);
        assert_eq!(translated.mode(), SqlTranslationMode::StrictSqlite);
        assert_eq!(translated.source(), source);
        assert_eq!(
            translated.sqlite_sql(),
            "-- private π\r\nCREATE TABLE `private_table` (`id` PRIVATE_TYPE DEFAULT TRUE);\r\nSELECT ?2, ?3, 'private ?'; -- tail\r\n"
        );
        assert_eq!(translated.statement_count(), 2);
        assert!(!translated.is_empty());
        assert_eq!(translated.statement_parameters().len(), 2);
        assert_eq!(
            translated.normalized_sql().sqlite_parameter_sql(),
            translated.sqlite_sql()
        );
        assert_eq!(translated.clone(), translated);

        let debug = format!("{translated:?}");
        assert!(debug.contains("StrictSqlite"));
        assert!(debug.contains("source_bytes"));
        assert!(!debug.contains("private"));
        assert!(!debug.contains('π'));

        for dialect in [SqlDialect::PostgreSql, SqlDialect::MySql] {
            let marker = if dialect == SqlDialect::PostgreSql {
                "$1"
            } else {
                "?"
            };
            let error = translate(
                dialect,
                &format!("SELECT {marker}"),
                SqlTranslationMode::StrictSqlite,
            )
            .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
            assert!(!error.diagnostic().contains(marker));
        }
    }

    #[test]
    fn equivalent_dialect_types_have_one_canonical_sqlite_declaration() {
        let sources = [
            (
                SqlDialect::Sqlite,
                "CREATE TABLE \"t\" (\"i\" INTEGER PRIMARY KEY, \"b\" BOOLEAN, \"r\" REAL, \"s\" VARCHAR(12), \"x\" BLOB)",
            ),
            (
                SqlDialect::PostgreSql,
                "CREATE TABLE \"t\" (\"i\" INT8 PRIMARY KEY, \"b\" BOOL, \"r\" FLOAT8, \"s\" CHARACTER VARYING(12), \"x\" BYTEA)",
            ),
            (
                SqlDialect::MySql,
                "CREATE TABLE `t` (`i` BIGINT PRIMARY KEY, `b` TINYINT(1), `r` DOUBLE, `s` VARCHAR(12), `x` VARBINARY(12))",
            ),
        ];
        let expected = "CREATE TABLE \"t\" (\"i\" BIGINT PRIMARY KEY, \"b\" BOOLEAN, \"r\" REAL, \"s\" TEXT, \"x\" BLOB)";

        for (dialect, source) in sources {
            let translated = compatibility(dialect, source).unwrap();
            assert_eq!(translated.sqlite_sql(), expected, "{dialect}");
            assert_eq!(translated.source(), source);
        }

        let strict = translate(
            SqlDialect::Sqlite,
            sources[0].1,
            SqlTranslationMode::StrictSqlite,
        )
        .unwrap();
        assert_eq!(strict.sqlite_sql(), sources[0].1);
        assert!(strict.sqlite_sql().contains("INTEGER PRIMARY KEY"));
    }

    #[test]
    fn generated_key_ddl_has_one_physical_sql_and_logical_intent() {
        let sources = [
            (
                SqlDialect::Sqlite,
                "CREATE TABLE \"events\" (\"id\" INTEGER PRIMARY KEY AUTOINCREMENT, \"payload\" TEXT)",
            ),
            (
                SqlDialect::MySql,
                "CREATE TABLE `events` (`id` BIGINT PRIMARY KEY AUTO_INCREMENT, `payload` TEXT)",
            ),
            (
                SqlDialect::PostgreSql,
                "CREATE TABLE \"events\" (\"id\" BIGSERIAL PRIMARY KEY, \"payload\" TEXT)",
            ),
            (
                SqlDialect::PostgreSql,
                "CREATE TABLE \"events\" (\"id\" BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY, \"payload\" TEXT)",
            ),
        ];
        let expected =
            "CREATE TABLE \"events\" (\"id\" INTEGER PRIMARY KEY AUTOINCREMENT, \"payload\" TEXT)";

        for (dialect, source) in sources {
            let translated = compatibility(dialect, source).unwrap();
            assert_eq!(translated.sqlite_sql(), expected, "{dialect}");
            let [intent] = translated.generated_table_intents() else {
                panic!("{dialect} did not retain exactly one generated-table intent");
            };
            assert_eq!(intent.statement_index(), 0);
            assert_eq!(intent.table(), "events");
            assert_eq!(intent.column(), "id");
            assert_eq!(intent.policy(), GeneratedIdPolicyIntent::NativeRangeV1);

            let debug = format!("{intent:?}");
            assert!(debug.contains("NativeRangeV1"));
            assert!(!debug.contains("events"));
            assert!(!debug.contains("id"));

            let connection = Connection::open_in_memory().unwrap();
            execute_batch(&connection, translated.sqlite_sql()).unwrap();
            execute_batch(
                &connection,
                "INSERT INTO events(payload) VALUES ('generated')",
            )
            .unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT id FROM events", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                1
            );
        }
    }

    #[test]
    fn generated_key_rewrite_canonicalizes_option_order_and_retains_batch_position() {
        let translated = compatibility(
            SqlDialect::MySql,
            "CREATE TABLE events(id BIGINT AUTO_INCREMENT PRIMARY KEY)",
        )
        .unwrap();
        assert_eq!(
            translated.sqlite_sql(),
            "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT)"
        );

        let batch = compatibility(
            SqlDialect::Sqlite,
            "CREATE TABLE ordinary(value TEXT); CREATE TABLE generated(id INTEGER PRIMARY KEY AUTOINCREMENT)",
        )
        .unwrap();
        let [intent] = batch.generated_table_intents() else {
            panic!("batch did not retain exactly one generated-table intent");
        };
        assert_eq!(intent.statement_index(), 1);
        assert_eq!(intent.table(), "generated");

        let strict_source =
            "CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT)";
        let strict = translate(
            SqlDialect::Sqlite,
            strict_source,
            SqlTranslationMode::StrictSqlite,
        )
        .unwrap();
        assert_eq!(strict.sqlite_sql(), strict_source);
        assert_eq!(strict.generated_table_intents().len(), 1);

        let ordinary = compatibility(
            SqlDialect::Sqlite,
            "CREATE TABLE ordinary(id INTEGER PRIMARY KEY)",
        )
        .unwrap();
        assert!(ordinary.generated_table_intents().is_empty());
    }

    #[test]
    fn finite_type_alias_matrix_is_dialect_specific() {
        let cases: &[(SqlDialect, &[(&str, &str)])] = &[
            (
                SqlDialect::Sqlite,
                &[
                    ("TINYINT", "BIGINT"),
                    ("SMALLINT", "BIGINT"),
                    ("MEDIUMINT", "BIGINT"),
                    ("INT", "BIGINT"),
                    ("INTEGER", "BIGINT"),
                    ("BIGINT", "BIGINT"),
                    ("BOOL", "BOOLEAN"),
                    ("BOOLEAN", "BOOLEAN"),
                    ("REAL", "REAL"),
                    ("TEXT", "TEXT"),
                    ("VARCHAR(20)", "TEXT"),
                    ("CHARACTER VARYING(20)", "TEXT"),
                    ("CHAR VARYING", "TEXT"),
                    ("BLOB", "BLOB"),
                ],
            ),
            (
                SqlDialect::PostgreSql,
                &[
                    ("INT2", "BIGINT"),
                    ("SMALLINT", "BIGINT"),
                    ("INT", "BIGINT"),
                    ("INTEGER", "BIGINT"),
                    ("INT4", "BIGINT"),
                    ("BIGINT", "BIGINT"),
                    ("INT8", "BIGINT"),
                    ("BOOL", "BOOLEAN"),
                    ("BOOLEAN", "BOOLEAN"),
                    ("FLOAT8", "REAL"),
                    ("DOUBLE PRECISION", "REAL"),
                    ("TEXT", "TEXT"),
                    ("VARCHAR", "TEXT"),
                    ("VARCHAR(20)", "TEXT"),
                    ("CHARACTER VARYING(20)", "TEXT"),
                    ("CHAR VARYING", "TEXT"),
                    ("BYTEA", "BLOB"),
                ],
            ),
            (
                SqlDialect::MySql,
                &[
                    ("TINYINT", "BIGINT"),
                    ("TINYINT(1)", "BOOLEAN"),
                    ("SMALLINT", "BIGINT"),
                    ("MEDIUMINT", "BIGINT"),
                    ("INT", "BIGINT"),
                    ("INTEGER", "BIGINT"),
                    ("BIGINT", "BIGINT"),
                    ("BOOL", "BOOLEAN"),
                    ("BOOLEAN", "BOOLEAN"),
                    ("DOUBLE", "REAL"),
                    ("DOUBLE PRECISION", "REAL"),
                    ("TEXT", "TEXT"),
                    ("VARCHAR(20)", "TEXT"),
                    ("CHARACTER VARYING(20)", "TEXT"),
                    ("CHAR VARYING", "TEXT"),
                    ("BLOB", "BLOB"),
                    ("VARBINARY", "BLOB"),
                    ("VARBINARY(20)", "BLOB"),
                ],
            ),
        ];

        for (dialect, aliases) in cases {
            for (source_type, expected_type) in *aliases {
                let source = format!("CREATE TABLE types (value {source_type})");
                let translated = compatibility(*dialect, &source)
                    .unwrap_or_else(|error| panic!("{dialect} rejected {source_type}: {error}"));
                assert_eq!(
                    translated.sqlite_sql(),
                    format!("CREATE TABLE types (value {expected_type})"),
                    "{dialect} {source_type}"
                );
            }
        }
    }

    #[test]
    fn unsupported_types_are_redacted_ordered_and_recoverable() {
        let unsupported = [
            (SqlDialect::Sqlite, "PRIVATE_TYPE"),
            (SqlDialect::Sqlite, "NUMERIC"),
            (SqlDialect::PostgreSql, "REAL"),
            (SqlDialect::PostgreSql, "FLOAT4"),
            (SqlDialect::PostgreSql, "NUMERIC(12, 2)"),
            (SqlDialect::PostgreSql, "UUID"),
            (SqlDialect::PostgreSql, "JSONB"),
            (SqlDialect::PostgreSql, "TIMESTAMP"),
            (SqlDialect::PostgreSql, "INTERVAL"),
            (SqlDialect::PostgreSql, "BIT(8)"),
            (SqlDialect::PostgreSql, "INTEGER[]"),
            (SqlDialect::PostgreSql, "VARCHAR(MAX)"),
            (SqlDialect::PostgreSql, "VARCHAR(20 CHARACTERS)"),
            (SqlDialect::PostgreSql, "CHARACTER VARYING(20 OCTETS)"),
            (SqlDialect::PostgreSql, "CHAR(8)"),
            (SqlDialect::MySql, "INT(11)"),
            (SqlDialect::MySql, "TINYINT(2)"),
            (SqlDialect::MySql, "BIGINT UNSIGNED"),
            (SqlDialect::MySql, "FLOAT"),
            (SqlDialect::MySql, "REAL"),
            (SqlDialect::MySql, "DECIMAL(12, 2)"),
            (SqlDialect::MySql, "DATETIME"),
            (SqlDialect::MySql, "JSON"),
            (SqlDialect::MySql, "ENUM('a', 'b')"),
            (SqlDialect::MySql, "BIT(8)"),
            (SqlDialect::MySql, "VARBINARY(MAX)"),
            (SqlDialect::MySql, "CHAR(8)"),
            (SqlDialect::MySql, "BINARY(8)"),
        ];

        for (dialect, source_type) in unsupported {
            let source = format!("CREATE TABLE private_name (private_column {source_type})");
            let error = compatibility(dialect, &source).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Unsupported, "{source}");
            assert!(error.diagnostic().contains("statement 1 column 1"));
            assert!(!error.diagnostic().contains("private"));
            assert!(!error.diagnostic().contains(source_type));
        }

        let serial_error = compatibility(
            SqlDialect::PostgreSql,
            "CREATE TABLE private_name (private_column SERIAL PRIMARY KEY)",
        )
        .unwrap_err();
        assert_eq!(serial_error.kind(), EngineErrorKind::Unsupported);
        assert!(
            serial_error
                .diagnostic()
                .contains("generated-key declaration")
        );
        assert!(!serial_error.diagnostic().contains("private"));
        assert!(!serial_error.diagnostic().contains("SERIAL"));

        let batch = "CREATE TABLE first (ok BIGINT); CREATE TABLE second (ok TEXT, private_column PRIVATE_TYPE); CREATE TABLE third (later PRIVATE_TYPE)";
        let error = compatibility(SqlDialect::Sqlite, batch).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Unsupported);
        assert!(error.diagnostic().contains("statement 2 column 2"));
        assert!(!error.diagnostic().contains("private"));

        let recovered =
            compatibility(SqlDialect::Sqlite, "CREATE TABLE recovered (value BIGINT)").unwrap();
        assert_eq!(
            recovered.sqlite_sql(),
            "CREATE TABLE recovered (value BIGINT)"
        );
    }

    #[test]
    fn syntax_translation_is_canonical_and_identifier_safe() {
        let source = "CREATE TABLE `odd``table` (`a\"b` BOOLEAN DEFAULT TRUE, `select` BIGINT, CHECK (`a\"b` = FALSE)); SELECT `w`.`a\"b` AS `from` FROM `odd``table` AS `w` WHERE `w`.`a\"b` = TRUE ORDER BY `from`";
        let translated = compatibility(SqlDialect::MySql, source).unwrap();
        assert_eq!(
            translated.sqlite_sql(),
            "CREATE TABLE \"odd`table\" (\"a\"\"b\" BOOLEAN DEFAULT 1, \"select\" BIGINT, CHECK (\"a\"\"b\" = 0)); SELECT \"w\".\"a\"\"b\" AS \"from\" FROM \"odd`table\" AS \"w\" WHERE \"w\".\"a\"\"b\" = 1 ORDER BY \"from\""
        );

        let transactions = compatibility(
            SqlDialect::PostgreSql,
            "BEGIN WORK; COMMIT TRANSACTION; ABORT AND NO CHAIN",
        )
        .unwrap();
        assert_eq!(transactions.sqlite_sql(), "BEGIN; COMMIT; ROLLBACK");

        let type_like_text = compatibility(
            SqlDialect::MySql,
            "SELECT 'BIGINT `private` TRUE' AS `VARCHAR`",
        )
        .unwrap();
        assert_eq!(
            type_like_text.sqlite_sql(),
            "SELECT 'BIGINT `private` TRUE' AS \"VARCHAR\""
        );
    }

    #[test]
    fn every_common_statement_family_uses_the_same_canonical_rewrite() {
        let source = "CREATE TABLE `items` (`id` BIGINT PRIMARY KEY, `enabled` BOOLEAN); CREATE INDEX `items_enabled` ON `items` (`enabled`); INSERT INTO `items` (`id`, `enabled`) VALUES (?, TRUE), (?, FALSE); UPDATE `items` SET `enabled` = ? WHERE `id` = ?; SELECT `id` AS `value` FROM `items` WHERE `enabled` = ? ORDER BY `id`; DELETE FROM `items` WHERE `id` = ?";
        let translated = compatibility(SqlDialect::MySql, source).unwrap();

        assert_eq!(
            translated.sqlite_sql(),
            "CREATE TABLE \"items\" (\"id\" BIGINT PRIMARY KEY, \"enabled\" BOOLEAN); CREATE INDEX \"items_enabled\" ON \"items\"(\"enabled\"); INSERT INTO \"items\" (\"id\", \"enabled\") VALUES (?1, 1), (?2, 0); UPDATE \"items\" SET \"enabled\" = ?1 WHERE \"id\" = ?2; SELECT \"id\" AS \"value\" FROM \"items\" WHERE \"enabled\" = ?1 ORDER BY \"id\"; DELETE FROM \"items\" WHERE \"id\" = ?1"
        );
        assert_eq!(translated.statement_count(), 6);
        assert_eq!(
            translated
                .statement_parameters()
                .iter()
                .map(StatementParameters::parameter_indices)
                .collect::<Vec<_>>(),
            vec![
                &[][..],
                &[][..],
                &[1, 2][..],
                &[1, 2][..],
                &[1][..],
                &[1][..]
            ]
        );

        let sqlite_round_trip = normalize(SqlDialect::Sqlite, translated.sqlite_sql()).unwrap();
        assert_eq!(
            sqlite_round_trip.sqlite_parameter_sql(),
            translated.sqlite_sql()
        );
    }

    #[test]
    fn comma_limit_reorders_syntax_without_reordering_bind_identity() {
        let source = "SELECT `tenant_id` FROM `widgets` WHERE `tenant_id` = ? ORDER BY `tenant_id` LIMIT ?, ?";
        let translated = compatibility(SqlDialect::MySql, source).unwrap();
        assert_eq!(
            translated.sqlite_sql(),
            "SELECT \"tenant_id\" FROM \"widgets\" WHERE \"tenant_id\" = ?1 ORDER BY \"tenant_id\" LIMIT ?3 OFFSET ?2"
        );
        assert_eq!(translated.statement_parameters().len(), 1);
        assert_eq!(
            translated.statement_parameters()[0].parameter_indices(),
            &[1, 2, 3]
        );
        assert_eq!(
            translated.normalized_sql().sqlite_parameter_sql(),
            "SELECT `tenant_id` FROM `widgets` WHERE `tenant_id` = ?1 ORDER BY `tenant_id` LIMIT ?2, ?3"
        );

        let batch = compatibility(SqlDialect::MySql, "SELECT ?; SELECT ? LIMIT ?, ?").unwrap();
        assert_eq!(
            batch.sqlite_sql(),
            "SELECT ?1; SELECT ?1 LIMIT ?3 OFFSET ?2"
        );
        assert_eq!(batch.statement_parameters()[0].parameter_indices(), &[1]);
        assert_eq!(
            batch.statement_parameters()[1].parameter_indices(),
            &[1, 2, 3]
        );

        let postgres = compatibility(
            SqlDialect::PostgreSql,
            "SELECT $2, $1, $2 LIMIT $3 OFFSET $1",
        )
        .unwrap();
        assert_eq!(
            postgres.sqlite_sql(),
            "SELECT ?2, ?1, ?2 LIMIT ?3 OFFSET ?1"
        );
        assert_eq!(
            postgres.statement_parameters()[0].parameter_indices(),
            &[2, 1, 2, 3, 1]
        );

        for source in [
            "SELECT $1 LIMIT $2 OFFSET $3 ROW",
            "SELECT $1 LIMIT $2 OFFSET $3 ROWS",
        ] {
            assert_eq!(
                compatibility(SqlDialect::PostgreSql, source)
                    .unwrap()
                    .sqlite_sql(),
                "SELECT ?1 LIMIT ?2 OFFSET ?3"
            );
        }
    }

    #[test]
    fn translated_dialects_execute_with_equal_sqlite_results() {
        let cases = [
            (
                SqlDialect::Sqlite,
                "SELECT \"value\" FROM \"items\" WHERE \"value\" >= ?1 ORDER BY \"value\" LIMIT ?2 OFFSET ?3",
                vec![Value::Int64(2), Value::Int64(2), Value::Int64(1)],
            ),
            (
                SqlDialect::PostgreSql,
                "SELECT \"value\" FROM \"items\" WHERE \"value\" >= $1 ORDER BY \"value\" LIMIT $2 OFFSET $3",
                vec![Value::Int64(2), Value::Int64(2), Value::Int64(1)],
            ),
            (
                SqlDialect::MySql,
                "SELECT `value` FROM `items` WHERE `value` >= ? ORDER BY `value` LIMIT ?, ?",
                vec![Value::Int64(2), Value::Int64(1), Value::Int64(2)],
            ),
        ];

        let mut results = Vec::new();
        for (dialect, source, parameters) in cases {
            let connection = Connection::open_in_memory().unwrap();
            let ddl = compatibility(
                dialect,
                match dialect {
                    SqlDialect::Sqlite => {
                        "CREATE TABLE \"items\" (\"value\" INTEGER, \"enabled\" BOOLEAN)"
                    }
                    SqlDialect::PostgreSql => {
                        "CREATE TABLE \"items\" (\"value\" INT8, \"enabled\" BOOL)"
                    }
                    SqlDialect::MySql => {
                        "CREATE TABLE `items` (`value` BIGINT, `enabled` TINYINT(1))"
                    }
                },
            )
            .unwrap();
            execute_batch(&connection, ddl.sqlite_sql()).unwrap();
            execute_batch(
                &connection,
                "INSERT INTO items(value, enabled) VALUES (1, 1), (2, 0), (3, 1), (4, 1), (5, 0)",
            )
            .unwrap();

            let translated = compatibility(dialect, source).unwrap();
            results.push(query(&connection, translated.sqlite_sql(), &parameters).unwrap());
        }

        assert_eq!(results[0], results[1]);
        assert_eq!(results[1], results[2]);
        assert_eq!(
            results[0]
                .rows()
                .iter()
                .map(|row| row.get(0).cloned().unwrap())
                .collect::<Vec<_>>(),
            vec![Value::Int64(3), Value::Int64(4)]
        );
    }

    #[test]
    fn empty_batches_limits_concurrency_and_recovery_are_stateless() {
        let empty_source = "-- private empty\n";
        let compatible = compatibility(SqlDialect::Sqlite, empty_source).unwrap();
        assert!(compatible.is_empty());
        assert_eq!(compatible.sqlite_sql(), "");
        assert_eq!(compatible.source(), empty_source);
        let strict = translate(
            SqlDialect::Sqlite,
            empty_source,
            SqlTranslationMode::StrictSqlite,
        )
        .unwrap();
        assert_eq!(strict.sqlite_sql(), empty_source);

        let maximum_batch = "SELECT 1;".repeat(256);
        let translated = compatibility(SqlDialect::Sqlite, &maximum_batch).unwrap();
        assert_eq!(translated.statement_count(), 256);
        assert_eq!(translated.sqlite_sql().matches("SELECT 1").count(), 256);

        let normalized = normalize(
            SqlDialect::MySql,
            "SELECT `value` FROM `items` WHERE `value` = ? LIMIT ?, ?",
        )
        .unwrap();
        let expected = compatibility(SqlDialect::MySql, normalized.source())
            .unwrap()
            .sqlite_sql()
            .to_owned();
        let threads = (0..24)
            .map(|_| {
                let normalized = normalized.clone();
                thread::spawn(move || {
                    translate_sql(normalized, SqlTranslationMode::Compatibility)
                        .unwrap()
                        .sqlite_sql()
                        .to_owned()
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            assert_eq!(thread.join().unwrap(), expected);
        }

        let mismatch = translate(
            SqlDialect::PostgreSql,
            "SELECT $1",
            SqlTranslationMode::StrictSqlite,
        )
        .unwrap_err();
        assert_eq!(mismatch.kind(), EngineErrorKind::InvalidArgument);
        assert_eq!(
            compatibility(SqlDialect::PostgreSql, "SELECT $1")
                .unwrap()
                .sqlite_sql(),
            "SELECT ?1"
        );
    }

    #[test]
    fn nul_and_inconsistent_metadata_fail_with_stable_redacted_errors() {
        let source = r"SELECT 'private\0literal'";
        let error = compatibility(SqlDialect::MySql, source).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidQuery);
        assert!(!error.diagnostic().contains("private"));
        assert!(!error.diagnostic().contains("literal"));

        let mut normalized = normalize(SqlDialect::PostgreSql, "SELECT $1").unwrap();
        normalized.corrupt_first_placeholder_for_test();
        let error = translate_sql(normalized, SqlTranslationMode::Compatibility).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert!(!error.diagnostic().contains("$1"));

        assert_eq!(
            compatibility(SqlDialect::PostgreSql, "SELECT $1")
                .unwrap()
                .sqlite_sql(),
            "SELECT ?1"
        );
    }
}
