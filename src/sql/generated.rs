use std::fmt;

use sqlparser::{
    ast::{
        ColumnOption, ColumnOptionDef, CreateTable, DataType, GeneratedAs, ObjectName,
        ObjectNamePart, Statement as AstStatement, TableConstraint,
    },
    keywords::Keyword,
    tokenizer::Token,
};

use super::{NormalizedSql, SqlDialect};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

/// Generated-ID policy requested by one accepted logical DDL declaration.
///
/// This is syntax intent, not authoritative catalog metadata. The schema
/// coordinator must still validate and durably publish the corresponding
/// table policy before omitted-key writes are allowed.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GeneratedIdPolicyIntent {
    /// Use the shard-local SQLite `AUTOINCREMENT` allocation ranges defined by
    /// the protocol-neutral `native_range_v1` policy.
    NativeRangeV1,
}

/// One generated-key table declaration retained beside translated SQL.
///
/// The table and column names are decoded AST identifier values. Quoting and
/// source spelling remain available separately through `TranslatedSql::source`.
#[derive(Clone, PartialEq, Eq)]
pub struct GeneratedTableIntent {
    statement_index: usize,
    table: String,
    column: String,
    policy: GeneratedIdPolicyIntent,
}

impl GeneratedTableIntent {
    /// Return the zero-based statement containing this declaration.
    pub const fn statement_index(&self) -> usize {
        self.statement_index
    }

    /// Return the decoded logical table identifier.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Return the decoded generated-key column identifier.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Return the requested generated-ID policy.
    pub const fn policy(&self) -> GeneratedIdPolicyIntent {
        self.policy
    }
}

impl fmt::Debug for GeneratedTableIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeneratedTableIntent")
            .field("statement_index", &self.statement_index)
            .field("table_bytes", &self.table.len())
            .field("column_bytes", &self.column.len())
            .field("policy", &self.policy)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SerialAlias {
    Small,
    Regular,
    Big,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeneratorAttribute {
    SqliteAutoincrement,
    MysqlAutoIncrement,
    PostgresIdentityByDefault,
    Unsupported,
}

#[derive(Clone)]
pub(super) struct AnalyzedGeneratedTable {
    intent: GeneratedTableIntent,
    column_index: usize,
    generated_option_index: Option<usize>,
}

impl AnalyzedGeneratedTable {
    pub(super) fn intent(&self) -> &GeneratedTableIntent {
        &self.intent
    }

    pub(super) const fn column_index(&self) -> usize {
        self.column_index
    }

    pub(super) fn owns_generated_option(&self, column_index: usize, option_index: usize) -> bool {
        self.column_index == column_index && self.generated_option_index == Some(option_index)
    }
}

/// Analyze the generated-key declaration on one structurally parsed table.
///
/// `Err(())` means generator-like syntax was present but did not match the
/// finite dialect contract. Keeping this lower helper independent of public
/// error text lets subset validation retain its fixed redacted diagnostics.
pub(super) fn analyze_create_table(
    dialect: SqlDialect,
    statement_index: usize,
    table: &CreateTable,
) -> Result<Option<AnalyzedGeneratedTable>, ()> {
    let mut candidates = Vec::new();
    for (column_index, column) in table.columns.iter().enumerate() {
        let serial_alias = if dialect == SqlDialect::PostgreSql {
            postgres_serial_alias(&column.data_type)
        } else {
            None
        };
        let generated_attributes = column
            .options
            .iter()
            .enumerate()
            .filter_map(|(option_index, option)| {
                generator_attribute(&option.option).map(|attribute| (option_index, attribute))
            })
            .collect::<Vec<_>>();
        if serial_alias.is_some() || !generated_attributes.is_empty() {
            candidates.push((column_index, serial_alias, generated_attributes));
        }
    }

    let Some((column_index, serial_alias, generated_attributes)) = candidates.pop() else {
        return Ok(None);
    };
    if !candidates.is_empty()
        || table.if_not_exists
        || table
            .constraints
            .iter()
            .any(|constraint| matches!(constraint, TableConstraint::PrimaryKey(_)))
    {
        return Err(());
    }

    let inline_primary_keys = table
        .columns
        .iter()
        .enumerate()
        .flat_map(|(candidate_column, column)| {
            column
                .options
                .iter()
                .enumerate()
                .filter(|(_, option)| matches!(option.option, ColumnOption::PrimaryKey(_)))
                .map(move |(option_index, option)| (candidate_column, option_index, option))
        })
        .collect::<Vec<_>>();
    if !matches!(
        inline_primary_keys.as_slice(),
        [(primary_column, _, _)] if *primary_column == column_index
    ) {
        return Err(());
    }

    let column = &table.columns[column_index];
    let primary_key_options = column
        .options
        .iter()
        .enumerate()
        .filter(|(_, option)| matches!(option.option, ColumnOption::PrimaryKey(_)))
        .collect::<Vec<_>>();
    if primary_key_options.len() != 1 || primary_key_options[0].1.name.is_some() {
        return Err(());
    }

    let generated_option_index = match dialect {
        SqlDialect::Sqlite
            if column.data_type == DataType::Integer(None)
                && serial_alias.is_none()
                && column.options.len() == 2
                && matches!(
                    generated_attributes.as_slice(),
                    [(1, GeneratorAttribute::SqliteAutoincrement)]
                        if matches!(column.options[0].option, ColumnOption::PrimaryKey(_))
                            && column.options[1].name.is_none()
                ) =>
        {
            Some(generated_attributes[0].0)
        }
        SqlDialect::MySql
            if column.data_type == DataType::BigInt(None)
                && serial_alias.is_none()
                && column.options.len() == 2
                && matches!(
                    generated_attributes.as_slice(),
                    [(option_index, GeneratorAttribute::MysqlAutoIncrement)]
                        if column.options[*option_index].name.is_none()
                ) =>
        {
            Some(generated_attributes[0].0)
        }
        SqlDialect::PostgreSql
            if serial_alias == Some(SerialAlias::Big)
                && generated_attributes.is_empty()
                && column.options.len() == 1 =>
        {
            None
        }
        SqlDialect::PostgreSql
            if column.data_type == DataType::BigInt(None)
                && serial_alias.is_none()
                && column.options.len() == 2
                && matches!(
                    generated_attributes.as_slice(),
                    [(1, GeneratorAttribute::PostgresIdentityByDefault)]
                        if matches!(column.options[0].option, ColumnOption::PrimaryKey(_))
                            && column.options[1].name.is_none()
                ) =>
        {
            Some(generated_attributes[0].0)
        }
        _ => return Err(()),
    };

    let table_name = one_part_identifier(&table.name).ok_or(())?;
    Ok(Some(AnalyzedGeneratedTable {
        intent: GeneratedTableIntent {
            statement_index,
            table: table_name.to_owned(),
            column: column.name.value.clone(),
            policy: GeneratedIdPolicyIntent::NativeRangeV1,
        },
        column_index,
        generated_option_index,
    }))
}

pub(super) fn analyze_generated_tables(
    normalized: &NormalizedSql,
) -> EngineResult<Vec<AnalyzedGeneratedTable>> {
    let mut generated = Vec::new();
    for (statement_index, statement) in normalized.common().statements().iter().enumerate() {
        let AstStatement::CreateTable(table) = statement else {
            continue;
        };
        match analyze_create_table(normalized.dialect(), statement_index, table) {
            Ok(Some(intent)) => generated.push(intent),
            Ok(None) => {}
            Err(()) => {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "validated generated-key DDL does not match its retained AST intent",
                ));
            }
        }
    }
    Ok(generated)
}

pub(super) fn rewrite_to_native_sqlite(
    statement: &mut AstStatement,
    generated: &AnalyzedGeneratedTable,
) -> EngineResult<()> {
    let AstStatement::CreateTable(table) = statement else {
        return Err(generated_translation_invariant());
    };
    let Some(column) = table.columns.get_mut(generated.column_index) else {
        return Err(generated_translation_invariant());
    };
    let Some(primary_key) = column
        .options
        .iter()
        .find(|option| matches!(option.option, ColumnOption::PrimaryKey(_)))
        .cloned()
    else {
        return Err(generated_translation_invariant());
    };

    column.data_type = DataType::Integer(None);
    column.options = vec![
        primary_key,
        ColumnOptionDef {
            name: None,
            option: ColumnOption::DialectSpecific(vec![Token::make_keyword("AUTOINCREMENT")]),
        },
    ];
    Ok(())
}

fn one_part_identifier(name: &ObjectName) -> Option<&str> {
    match name.0.as_slice() {
        [ObjectNamePart::Identifier(identifier)] => Some(&identifier.value),
        _ => None,
    }
}

fn postgres_serial_alias(data_type: &DataType) -> Option<SerialAlias> {
    let DataType::Custom(name, modifiers) = data_type else {
        return None;
    };
    if !modifiers.is_empty() {
        return None;
    }
    let [ObjectNamePart::Identifier(identifier)] = name.0.as_slice() else {
        return None;
    };
    if identifier.quote_style.is_some() {
        return None;
    }
    if identifier.value.eq_ignore_ascii_case("smallserial") {
        Some(SerialAlias::Small)
    } else if identifier.value.eq_ignore_ascii_case("serial") {
        Some(SerialAlias::Regular)
    } else if identifier.value.eq_ignore_ascii_case("bigserial") {
        Some(SerialAlias::Big)
    } else {
        None
    }
}

fn generator_attribute(option: &ColumnOption) -> Option<GeneratorAttribute> {
    match option {
        ColumnOption::DialectSpecific(tokens) => auto_increment_attribute(tokens),
        ColumnOption::Generated {
            generated_as,
            sequence_options,
            generation_expr,
            generation_expr_mode,
            generated_keyword,
        } => Some(
            if *generated_as == GeneratedAs::ByDefault
                && sequence_options.as_ref().is_some_and(Vec::is_empty)
                && generation_expr.is_none()
                && generation_expr_mode.is_none()
                && *generated_keyword
            {
                GeneratorAttribute::PostgresIdentityByDefault
            } else {
                GeneratorAttribute::Unsupported
            },
        ),
        ColumnOption::Identity(_) => Some(GeneratorAttribute::Unsupported),
        _ => None,
    }
}

fn auto_increment_attribute(tokens: &[Token]) -> Option<GeneratorAttribute> {
    let [Token::Word(word)] = tokens else {
        return None;
    };
    match word.keyword {
        Keyword::AUTOINCREMENT if word.quote_style.is_none() => {
            Some(GeneratorAttribute::SqliteAutoincrement)
        }
        Keyword::AUTO_INCREMENT if word.quote_style.is_none() => {
            Some(GeneratorAttribute::MysqlAutoIncrement)
        }
        _ => None,
    }
}

fn generated_translation_invariant() -> EngineError {
    EngineError::new(
        EngineErrorKind::Internal,
        "generated-key translation metadata does not match the retained statement",
    )
}
