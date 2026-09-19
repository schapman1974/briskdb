//! Extract positive equality clauses for operator-upsert seed documents.

use super::*;
use crate::document::{BSON_MAX_DECODED_BYTES, DocumentUpdater, memory};

impl DocumentMatcher {
    pub(crate) fn upsert_seed_with_check(
        &self,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<BsonDocument> {
        let mut equalities = Vec::new();
        collect_equalities(self, &mut equalities, check)?;
        // Detect overlaps before copying any operands. Sorting references does
        // not change the original clause order used to construct the seed.
        let mut paths: Vec<_> = equalities.iter().map(|(path, _)| *path).collect();
        paths.sort_unstable();
        for pair in paths.windows(2) {
            check()?;
            if pair[1].starts_with(pair[0]) {
                return Err(query_error(54)); // NotSingleValueField
            }
        }
        let mut retained = self.retained_bytes;
        let mut fields = BsonDocument::new();
        for (path, value) in equalities {
            check()?;
            let name_bytes = path.iter().map(String::len).sum::<usize>() + path.len();
            let value_bytes = memory::value_bytes(value, BSON_MAX_DECODED_BYTES, check)?;
            retained = retained
                .checked_add(name_bytes)
                .and_then(|bytes| bytes.checked_add(value_bytes))
                .filter(|bytes| *bytes <= BSON_MAX_DECODED_BYTES)
                .ok_or_else(limit)?;
            fields
                .push(path.join("."), value.clone())
                .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        }
        let expression = BsonDocument::from_entries([("$set", BsonValue::Document(fields))])
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        // Share strict object/array path rules and growth/work budgets with
        // ordinary updates; query equalities must not overwrite scalar parents.
        DocumentUpdater::compile_with_check(&expression, check)?
            .apply_with_check(&BsonDocument::new(), check)
    }
}

fn collect_equalities<'a>(
    matcher: &'a DocumentMatcher,
    output: &mut Vec<(&'a [String], &'a BsonValue)>,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    for clause in &matcher.clauses {
        check()?;
        match clause {
            Clause::Field {
                path, predicates, ..
            } => {
                if path.first().is_some_and(|part| part == "_id") && path.len() > 1 {
                    return Err(query_error(54));
                }
                for predicate in predicates {
                    check()?;
                    if let Predicate::Equal(value) = predicate {
                        if output.len() >= MAX_QUERY_NODES {
                            return Err(limit());
                        }
                        output.push((path, value));
                    }
                }
            }
            Clause::Logical {
                kind: Logical::And,
                children,
            } => {
                for child in children {
                    collect_equalities(child, output, check)?;
                }
            }
            // Negations, alternatives, ranges and regex predicates cannot
            // supply an unconditional equality value for a new document.
            Clause::Logical { .. } => (),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
        BsonDocument::from_entries(fields).unwrap()
    }

    #[test]
    fn upsert_seed_extracts_positive_equalities_without_predicate_values() {
        let literal = doc([("inner", BsonValue::Null)]);
        let query = doc([
            ("_id", BsonValue::Null),
            ("a.b", BsonValue::Int64(7)),
            ("literal", BsonValue::Document(literal.clone())),
            (
                "range",
                BsonValue::Document(doc([("$gt", BsonValue::Int32(3))])),
            ),
            (
                "regex",
                BsonValue::RegularExpression(BsonRegex::new("x", "").unwrap()),
            ),
            (
                "$and",
                BsonValue::Array(vec![BsonValue::Document(doc([(
                    "equal",
                    BsonValue::Document(doc([
                        ("$eq", BsonValue::Int32(4)),
                        ("$lt", BsonValue::Int32(8)),
                    ])),
                )]))]),
            ),
            (
                "$or",
                BsonValue::Array(vec![BsonValue::Document(doc([(
                    "ignored",
                    BsonValue::Boolean(true),
                )]))]),
            ),
        ]);
        let seed = DocumentMatcher::compile(&query)
            .unwrap()
            .upsert_seed_with_check(&mut || Ok(()))
            .unwrap();
        assert!(seed.representation_eq(&doc([
            ("_id", BsonValue::Null),
            ("a", BsonValue::Document(doc([("b", BsonValue::Int64(7))]))),
            ("literal", BsonValue::Document(literal)),
            ("equal", BsonValue::Int32(4)),
        ])));
    }

    #[test]
    fn upsert_seed_rejects_conflicting_equalities_dotted_ids_and_cancellation() {
        for query in [
            doc([("a", BsonValue::Int32(1)), ("a.b", BsonValue::Int32(2))]),
            doc([(
                "$and",
                BsonValue::Array(vec![
                    BsonValue::Document(doc([("a", BsonValue::Int32(1))])),
                    BsonValue::Document(doc([("a", BsonValue::Int32(1))])),
                ]),
            )]),
            doc([("_id.part", BsonValue::Int32(1))]),
        ] {
            let error = DocumentMatcher::compile(&query)
                .unwrap()
                .upsert_seed_with_check(&mut || Ok(()))
                .unwrap_err();
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<DocumentQueryError>()
                    .unwrap()
                    .mongo_code(),
                54
            );
        }
        let matcher = DocumentMatcher::compile(&doc([("a", BsonValue::Int32(1))])).unwrap();
        assert_eq!(
            matcher
                .upsert_seed_with_check(&mut || Err(EngineError::new(
                    EngineErrorKind::Cancelled,
                    "cancelled"
                )))
                .unwrap_err()
                .kind(),
            EngineErrorKind::Cancelled
        );
    }
}
