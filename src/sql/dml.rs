//! Narrow routed-DML inspection used by bound statement policy.

use sqlparser::ast::{AssignmentTarget, SetExpr, Statement as AstStatement};

use super::{NormalizedSql, inference::identifier_matches_catalog};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

/// The statement shapes whose physical placement can change shard data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoutedDml {
    Insert,
    Update { assigns_shard_key: bool },
    Delete,
}

/// Structural shape of an INSERT relative to one catalog-declared generated
/// column.
///
/// This records caller intent from the retained AST. In particular, an
/// explicitly supplied `NULL` is still [`Self::ExplicitKey`]; only absence
/// from the INSERT column list can authorize the generated-ID write seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GeneratedInsertShape {
    ExplicitKey,
    OmittedSingleRow,
    OmittedMultipleRows,
}

/// Inspect one validated statement for generated-key INSERT intent.
///
/// The common-subset validator has already limited INSERT sources to nonempty,
/// equal-width `VALUES` rows and rejected duplicate columns. Any disagreement
/// with those invariants is therefore an internal planning error rather than a
/// second, more permissive SQL grammar.
pub(crate) fn generated_insert_shape(
    normalized: &NormalizedSql,
    statement_index: usize,
    generated_column: &str,
) -> EngineResult<Option<GeneratedInsertShape>> {
    let Some(statement) = normalized.common().statements().get(statement_index) else {
        return Err(routing_policy_invariant());
    };
    let AstStatement::Insert(insert) = statement else {
        return Ok(None);
    };

    let mut supplies_generated_column = false;
    for column in &insert.columns {
        let identifier = super::inference::column_ident(column)?;
        if identifier_matches_catalog(identifier, generated_column) {
            supplies_generated_column = true;
        } else if identifier.value.eq_ignore_ascii_case(generated_column) {
            // SQLite resolves quoted identifiers case-insensitively for ASCII,
            // while catalog matching deliberately preserves quoted spelling.
            // Reject that semantic mismatch before routing: treating it as
            // omitted could generate a key, and treating it as an unrelated
            // column could admit a write whose physical key differs from the
            // planner's key. Keep scanning exact matches rather than returning
            // early so a mixed `id, "ID"` list also fails closed.
            return Err(EngineError::new(
                EngineErrorKind::InvalidQuery,
                "generated-key INSERT uses a quoted column spelling that SQLite aliases to the generated column",
            ));
        }
    }
    if supplies_generated_column {
        return Ok(Some(GeneratedInsertShape::ExplicitKey));
    }

    let Some(source) = &insert.source else {
        return Err(routing_policy_invariant());
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Err(routing_policy_invariant());
    };
    let shape = match values.rows.len() {
        1 => GeneratedInsertShape::OmittedSingleRow,
        2.. => GeneratedInsertShape::OmittedMultipleRows,
        0 => return Err(routing_policy_invariant()),
    };
    Ok(Some(shape))
}

/// Inspect only the DML details needed by single-shard routing policy.
///
/// Full statement behavior and batch policy live in the separate public
/// classifier; this helper retains the extra assignment detail needed only by
/// sharded-write planning.
pub(crate) fn routed_dml_shape(
    normalized: &NormalizedSql,
    statement_index: usize,
    shard_key_column: &str,
) -> EngineResult<Option<RoutedDml>> {
    let Some(statement) = normalized.common().statements().get(statement_index) else {
        return Err(routing_policy_invariant());
    };
    let shape = match statement {
        AstStatement::Insert(_) => Some(RoutedDml::Insert),
        AstStatement::Update(update) => {
            let mut assigns_shard_key = false;
            for assignment in &update.assignments {
                let AssignmentTarget::ColumnName(column) = &assignment.target else {
                    return Err(routing_policy_invariant());
                };
                let identifier = super::inference::column_ident(column)?;
                assigns_shard_key |= identifier_matches_catalog(identifier, shard_key_column);
            }
            Some(RoutedDml::Update { assigns_shard_key })
        }
        AstStatement::Delete(_) => Some(RoutedDml::Delete),
        _ => None,
    };
    Ok(shape)
}

fn routing_policy_invariant() -> EngineError {
    EngineError::new(
        EngineErrorKind::Internal,
        "normalized SQL metadata is inconsistent during routing policy inspection",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{SqlDialect, normalize_placeholders, parse, validate_common_subset};

    fn inspect(dialect: SqlDialect, source: &str) -> Option<RoutedDml> {
        let normalized = normalize_placeholders(
            validate_common_subset(parse(dialect, source).unwrap()).unwrap(),
        )
        .unwrap();
        routed_dml_shape(&normalized, 0, "tenant_id").unwrap()
    }

    fn generated_shape(dialect: SqlDialect, source: &str) -> Option<GeneratedInsertShape> {
        generated_shape_result(dialect, source).unwrap()
    }

    fn generated_shape_result(
        dialect: SqlDialect,
        source: &str,
    ) -> EngineResult<Option<GeneratedInsertShape>> {
        let normalized = normalize_placeholders(
            validate_common_subset(parse(dialect, source).unwrap()).unwrap(),
        )
        .unwrap();
        generated_insert_shape(&normalized, 0, "id")
    }

    #[test]
    fn routed_dml_shapes_are_dialect_independent() {
        for dialect in SqlDialect::ALL.iter().copied() {
            assert_eq!(
                inspect(dialect, "INSERT INTO events (tenant_id) VALUES (1)"),
                Some(RoutedDml::Insert)
            );
            assert_eq!(
                inspect(dialect, "UPDATE events SET payload = 1 WHERE tenant_id = 1"),
                Some(RoutedDml::Update {
                    assigns_shard_key: false
                })
            );
            assert_eq!(
                inspect(dialect, "DELETE FROM events WHERE tenant_id = 1"),
                Some(RoutedDml::Delete)
            );
            assert_eq!(
                inspect(dialect, "SELECT * FROM events WHERE tenant_id = 1"),
                None
            );
            assert_eq!(inspect(dialect, "BEGIN"), None);
        }
    }

    #[test]
    fn generated_insert_shape_is_ast_driven_and_dialect_independent() {
        for dialect in SqlDialect::ALL.iter().copied() {
            assert_eq!(
                generated_shape(dialect, "INSERT INTO events (payload) VALUES ('one')"),
                Some(GeneratedInsertShape::OmittedSingleRow),
                "{dialect}"
            );
            assert_eq!(
                generated_shape(
                    dialect,
                    "INSERT INTO events (payload) VALUES ('one'), ('two')"
                ),
                Some(GeneratedInsertShape::OmittedMultipleRows),
                "{dialect}"
            );
            assert_eq!(
                generated_shape(
                    dialect,
                    "INSERT INTO events (id, payload) VALUES (7, 'one')"
                ),
                Some(GeneratedInsertShape::ExplicitKey),
                "{dialect}"
            );
            assert_eq!(
                generated_shape(
                    dialect,
                    "INSERT INTO events (id, payload) VALUES (NULL, 'one')"
                ),
                Some(GeneratedInsertShape::ExplicitKey),
                "{dialect}"
            );
            assert_eq!(
                generated_shape(dialect, "UPDATE events SET payload = 'one'"),
                None,
                "{dialect}"
            );
        }
    }

    #[test]
    fn generated_insert_matching_fails_closed_for_sqlite_resolvable_quotes() {
        assert_eq!(
            generated_shape(
                SqlDialect::PostgreSql,
                "INSERT INTO events (ID, payload) VALUES (7, 'one')"
            ),
            Some(GeneratedInsertShape::ExplicitKey)
        );
        assert_eq!(
            generated_shape(
                SqlDialect::PostgreSql,
                "INSERT INTO events (\"id\", payload) VALUES (7, 'one')"
            ),
            Some(GeneratedInsertShape::ExplicitKey)
        );
        for (dialect, source) in [
            (
                SqlDialect::PostgreSql,
                "INSERT INTO events (\"ID\", payload) VALUES (7, 'one')",
            ),
            (
                SqlDialect::MySql,
                "INSERT INTO events (`ID`, payload) VALUES (7, 'one')",
            ),
        ] {
            let error = generated_shape_result(dialect, source).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidQuery, "{source}");
        }
    }

    #[test]
    fn shard_key_update_matching_obeys_catalog_identifier_rules() {
        for dialect in SqlDialect::ALL.iter().copied() {
            for source in [
                "UPDATE events SET tenant_id = 1",
                "UPDATE events SET TENANT_ID = TENANT_ID",
                "UPDATE events SET payload = 1, tenant_id = 2",
            ] {
                assert_eq!(
                    inspect(dialect, source),
                    Some(RoutedDml::Update {
                        assigns_shard_key: true
                    }),
                    "{dialect}: {source}"
                );
            }
            assert_eq!(
                inspect(dialect, "UPDATE events SET payload = tenant_id"),
                Some(RoutedDml::Update {
                    assigns_shard_key: false
                })
            );
        }

        assert_eq!(
            inspect(
                SqlDialect::PostgreSql,
                "UPDATE events SET \"tenant_id\" = 1"
            ),
            Some(RoutedDml::Update {
                assigns_shard_key: true
            })
        );
        assert_eq!(
            inspect(
                SqlDialect::PostgreSql,
                "UPDATE events SET \"TENANT_ID\" = 1"
            ),
            Some(RoutedDml::Update {
                assigns_shard_key: false
            })
        );
        assert_eq!(
            inspect(SqlDialect::MySql, "UPDATE events SET `tenant_id` = 1"),
            Some(RoutedDml::Update {
                assigns_shard_key: true
            })
        );
        assert_eq!(
            inspect(SqlDialect::MySql, "UPDATE events SET `TENANT_ID` = 1"),
            Some(RoutedDml::Update {
                assigns_shard_key: false
            })
        );
    }

    #[test]
    fn inconsistent_statement_index_is_an_internal_redacted_error() {
        let normalized = normalize_placeholders(
            validate_common_subset(
                parse(SqlDialect::Sqlite, "UPDATE events SET tenant_id = 1").unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        let error = routed_dml_shape(&normalized, 1, "tenant_id").unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert!(!error.to_string().contains("events"));
        assert!(!error.to_string().contains("tenant_id"));

        let error = generated_insert_shape(&normalized, 1, "private_id").unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert!(!error.to_string().contains("private_id"));
    }
}
