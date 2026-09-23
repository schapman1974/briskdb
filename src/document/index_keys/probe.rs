//! Necessary scalar equality tuples. This is a membership proof for a fully
//! maintained index, not catalog authority or a replacement for matching.

use super::*;

impl DocumentIndexKeyGenerator {
    /// Return a candidate equality tuple only when every indexed field has a
    /// necessary supported scalar equality. `None` requires an ordinary scan.
    ///
    /// Positive conjunctions are supported. Partial indexes, incomplete compound
    /// keys, array/object operands and sparse all-null tuples fall back. Missing
    /// fields match null, so sparse null lookups cannot exclude absent records.
    /// The caller must separately establish current, complete index authority
    /// and apply the entire matcher to every returned candidate.
    pub fn equality_key(
        &self,
        matcher: &DocumentMatcher,
    ) -> EngineResult<Option<DocumentIndexKey>> {
        self.equality_key_with_check(matcher, &mut || Ok(()))
    }

    pub fn equality_key_with_check(
        &self,
        matcher: &DocumentMatcher,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Option<DocumentIndexKey>> {
        let mut budget = Budget::new(check);
        budget.charge(self.retained_bytes)?;
        self.equality_key_with_budget(matcher, &mut budget)
    }

    pub(super) fn equality_key_with_budget(
        &self,
        matcher: &DocumentMatcher,
        budget: &mut Budget<'_>,
    ) -> EngineResult<Option<DocumentIndexKey>> {
        budget.step()?;
        if self.partial.is_some() {
            return Ok(None);
        }
        budget.charge(128 + self.paths.len() * 32)?;
        let mut components = Vec::new();
        components
            .try_reserve_exact(self.paths.len())
            .map_err(allocation)?;
        let mut nonnull = false;
        for path in &self.paths {
            let Some(value) = matcher.equality_for_index_path(path, &mut || budget.step())? else {
                return Ok(None);
            };
            if !supported_scalar(value) {
                return Ok(None);
            }
            nonnull |= !matches!(value, BsonValue::Null);
            let mut selected = component_keys(Some(value), budget)?;
            debug_assert_eq!(selected.len(), 1);
            components.push(
                selected
                    .pop()
                    .expect("a supported scalar has one equality key"),
            );
        }
        if self.sparse && !nonnull {
            return Ok(None);
        }
        budget.keys(1)?;
        budget.step()?;
        Ok(Some(DocumentIndexKey { components }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{BsonDateTime, BsonDecimal128, BsonObjectId};

    fn doc(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
        BsonDocument::from_entries(entries).unwrap()
    }

    fn object(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonValue {
        BsonValue::Document(doc(entries))
    }

    fn generator(sparse: bool, compound: bool) -> DocumentIndexKeyGenerator {
        let mut keys = vec![("a", BsonValue::Int32(1))];
        if compound {
            keys.push(("b", BsonValue::Int32(-1)));
        }
        DocumentIndexKeyGenerator::compile(&doc(keys), sparse, None).unwrap()
    }

    fn probe(
        generator: &DocumentIndexKeyGenerator,
        query: &BsonDocument,
    ) -> Option<DocumentIndexKey> {
        generator
            .equality_key(&DocumentMatcher::compile(query).unwrap())
            .unwrap()
    }

    #[test]
    fn probe_requires_a_complete_necessary_conjunction() {
        let one = BsonValue::Int32(1);
        let equality = object([("a", one.clone())]);
        for query in [
            doc([]),
            doc([("a", object([("$gt", one.clone())]))]),
            doc([("a", object([("$not", object([("$eq", one.clone())]))]))]),
            doc([("$or", BsonValue::Array(vec![equality.clone()]))]),
            doc([("$nor", BsonValue::Array(vec![equality.clone()]))]),
        ] {
            assert!(
                probe(&generator(false, false), &query).is_none(),
                "{query:?}"
            );
        }
        let compound = generator(false, true);
        assert!(probe(&compound, &doc([("a", one.clone())])).is_none());
        let query = doc([(
            "$and",
            BsonValue::Array(vec![
                equality,
                object([(
                    "$and",
                    BsonValue::Array(vec![object([("b", BsonValue::from("private"))])]),
                )]),
                object([("extra", object([("$gt", one.clone())]))]),
            ]),
        )]);
        let key = probe(&compound, &query).unwrap();
        assert_eq!(
            key,
            compound
                .keys(&doc([("a", one), ("b", BsonValue::from("private"))]))
                .unwrap()[0]
        );
        assert!(!format!("{key:?}").contains("private"));
    }

    #[test]
    fn sparse_null_and_partial_membership_do_not_omit_matches() {
        let null = doc([("a", BsonValue::Null)]);
        let ordinary = generator(false, false);
        assert_eq!(
            probe(&ordinary, &null).unwrap(),
            ordinary.keys(&doc([])).unwrap()[0]
        );
        assert!(probe(&generator(true, false), &null).is_none());
        assert!(
            probe(
                &generator(true, true),
                &doc([("a", BsonValue::Null), ("b", BsonValue::Null)])
            )
            .is_none()
        );
        let sparse = generator(true, true);
        let query = doc([("a", BsonValue::Null), ("b", BsonValue::Int32(1))]);
        assert_eq!(
            probe(&sparse, &query).unwrap(),
            sparse.keys(&doc([("b", BsonValue::Int64(1))])).unwrap()[0]
        );
        let partial = DocumentIndexKeyGenerator::compile(
            &doc([("a", BsonValue::Int32(1))]),
            false,
            Some(&doc([("enabled", BsonValue::Boolean(true))])),
        )
        .unwrap();
        assert!(
            probe(
                &partial,
                &doc([
                    ("a", BsonValue::Int32(1)),
                    ("enabled", BsonValue::Boolean(true))
                ])
            )
            .is_none()
        );
    }

    #[test]
    fn unsupported_equality_shapes_fall_back_without_query_errors() {
        for value in [
            BsonValue::Array(vec![]),
            object([]),
            BsonValue::ObjectId(BsonObjectId::from_bytes([1; 12])),
            BsonValue::DateTime(BsonDateTime::from_millis(123)),
            BsonValue::Double(f64::NAN),
            BsonValue::Double(f64::INFINITY),
            BsonValue::Decimal128(BsonDecimal128::parse("NaN").unwrap()),
        ] {
            let query = doc([("a", object([("$eq", value)]))]);
            assert!(probe(&generator(false, false), &query).is_none());
        }
    }

    #[test]
    fn multiple_equalities_restrict_candidates_without_replacing_the_matcher() {
        let query = doc([(
            "$and",
            BsonValue::Array(vec![
                object([("a", BsonValue::Int32(1))]),
                object([("a", BsonValue::Int32(2))]),
            ]),
        )]);
        let matcher = DocumentMatcher::compile(&query).unwrap();
        let generator = generator(false, false);
        let key = generator.equality_key(&matcher).unwrap().unwrap();
        let matching = doc([(
            "a",
            BsonValue::Array(vec![BsonValue::Int64(1), BsonValue::Int32(2)]),
        )]);
        assert!(matcher.matches(&matching).unwrap());
        assert!(generator.keys(&matching).unwrap().contains(&key));
        let residual_failure = doc([("a", BsonValue::Double(1.0))]);
        assert!(generator.keys(&residual_failure).unwrap().contains(&key));
        assert!(!matcher.matches(&residual_failure).unwrap());
    }

    #[test]
    fn probe_cancellation_is_checked_at_every_checkpoint_and_is_reusable() {
        let generator = generator(false, true);
        let matcher = DocumentMatcher::compile(&doc([
            ("a", BsonValue::from("private")),
            ("b", BsonValue::Null),
        ]))
        .unwrap();
        let expected = generator.equality_key(&matcher).unwrap().unwrap();
        let mut checkpoints = 0;
        generator
            .equality_key_with_check(&matcher, &mut || {
                checkpoints += 1;
                Ok(())
            })
            .unwrap();
        for stop in 1..=checkpoints {
            let mut seen = 0;
            let error = generator
                .equality_key_with_check(&matcher, &mut || {
                    seen += 1;
                    if seen == stop {
                        Err(EngineError::new(
                            EngineErrorKind::Cancelled,
                            "probe cancelled",
                        ))
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Cancelled);
            assert!(!error.to_string().contains("private"));
            assert_eq!(generator.equality_key(&matcher).unwrap().unwrap(), expected);
        }
        assert!(checkpoints > 10);
    }

    #[test]
    fn probe_does_not_reset_a_collection_preparations_work_budget() {
        let generator = generator(false, false);
        let matcher = DocumentMatcher::compile(&doc([("a", BsonValue::Int32(1))])).unwrap();
        for (steps, bytes, keys) in [(MAX_STEPS, 0, 0), (0, MAX_WORK_BYTES, 0), (0, 0, MAX_KEYS)] {
            let mut check = || Ok(());
            let mut budget = Budget {
                steps,
                bytes,
                keys,
                check: &mut check,
            };
            assert_eq!(
                generator
                    .equality_key_with_budget(&matcher, &mut budget)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::LimitExceeded
            );
        }
        assert!(generator.equality_key(&matcher).unwrap().is_some());
    }
}
