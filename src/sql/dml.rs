//! Narrow routed-DML inspection used by bound statement policy.

use sqlparser::ast::{AssignmentTarget, Statement as AstStatement};

use super::{NormalizedSql, inference::identifier_matches_catalog};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

/// The statement shapes whose physical placement can change shard data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoutedDml {
    Insert,
    Update { assigns_shard_key: bool },
    Delete,
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
    }
}
