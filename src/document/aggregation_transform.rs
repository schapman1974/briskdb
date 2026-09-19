//! Projection-stage validation and ordered, immutable BSON transformation.

use std::{collections::BTreeSet, error::Error};

use super::{
    BsonCodecOptions, BsonDocument, BsonErrorContext, BsonValue, DocumentProjector,
    DocumentQueryError,
    aggregation_expression::{Budget, Expression, contains_operator, limit, lookup},
    encode_document, encode_document_with_options,
    matcher::query_error,
    memory,
    number::CanonicalNumber,
    projection::numeric_component,
};
use crate::core::EngineResult;

const MAX_SPEC_BYTES: usize = 1024 * 1024;

pub(super) struct Transform {
    kind: Kind,
    bytes: usize,
}

enum Kind {
    Basic(DocumentProjector),
    Project(Node, Vec<Expression>),
    Set(Vec<(Vec<String>, Expression)>),
}

#[derive(Default)]
struct Node {
    leaf: Option<Leaf>,
    children: Vec<(String, Self)>,
    computed: bool,
}

enum Leaf {
    Include,
    Computed(usize),
}

struct Flat<'a> {
    path: String,
    flag: Option<bool>,
    value: &'a BsonValue,
}

impl Transform {
    pub fn compile(
        name: &str,
        stage: &BsonDocument,
        argument: &BsonValue,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Self> {
        let mut budget = Budget::new(check);
        budget.step()?;
        let encoded = encode_document_with_options(
            stage,
            &BsonCodecOptions::new().with_max_document_bytes(MAX_SPEC_BYTES),
        )
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        budget.charge(encoded.len() * 16)?;
        let kind = if name == "$unset" {
            let paths = match argument {
                BsonValue::String(path) => vec![path.as_str()],
                BsonValue::Array(values) => {
                    if values.is_empty() {
                        return Err(query_error(31119));
                    }
                    values
                        .iter()
                        .map(|value| match value {
                            BsonValue::String(path) => Ok(path.as_str()),
                            _ => Err(query_error(31120)),
                        })
                        .collect::<EngineResult<Vec<_>>>()?
                }
                _ => return Err(query_error(31002)),
            };
            let items: Vec<_> = paths
                .into_iter()
                .map(|path| {
                    Ok(Flat {
                        path: budget.field(path)?,
                        flag: Some(false),
                        value: argument,
                    })
                })
                .collect::<EngineResult<_>>()?;
            validate_paths(&items, false, &mut budget)?;
            Kind::Basic(basic(&items, &mut budget)?)
        } else {
            let project = name == "$project";
            let BsonValue::Document(spec) = argument else {
                return Err(query_error(if project { 15969 } else { 40272 }));
            };
            if project && spec.is_empty() {
                return Err(query_error(51272));
            }
            let mut items = Vec::new();
            flatten(spec, None, project, 1, &mut items, &mut budget)?;
            validate_paths(&items, !project, &mut budget)?;
            if project {
                project_kind(&items, &mut budget)?
            } else {
                let mut assignments = Vec::new();
                for item in items {
                    let expression = Expression::compile(item.value, true, &mut budget, 1)?;
                    let parts = item
                        .path
                        .split('.')
                        .map(|part| budget.field(part))
                        .collect::<EngineResult<_>>()?;
                    assignments.push((parts, expression));
                }
                Kind::Set(assignments)
            }
        };
        budget.step()?;
        Ok(Self {
            kind,
            bytes: budget.bytes,
        })
    }

    pub const fn retained_bytes(&self) -> usize {
        self.bytes
    }

    pub fn apply(
        &self,
        document: BsonDocument,
        available_bytes: usize,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<BsonDocument> {
        if let Kind::Basic(projector) = &self.kind {
            return projector.project_owned_validated_with_check(document, check);
        }
        let mut budget = Budget::new(check).with_maximum(available_bytes);
        let bytes = memory::document_bytes(&document, available_bytes, &mut || budget.step())?;
        budget.charge(bytes)?;
        let result = match &self.kind {
            Kind::Basic(_) => unreachable!("handled above"),
            Kind::Project(tree, expressions) => {
                let mut values = Vec::new();
                for expression in expressions {
                    values.push(expression.evaluate(&document, &mut budget, 1)?);
                }
                render_document(Some(&document), tree, &values, &mut budget, 1)?
            }
            Kind::Set(assignments) => {
                let mut values = Vec::new();
                for (_, expression) in assignments {
                    values.push(expression.evaluate(&document, &mut budget, 1)?);
                }
                let mut result = BsonValue::Document(document);
                for ((parts, _), value) in assignments.iter().zip(&values) {
                    result = assign(result, parts, value.as_ref(), &mut budget, 1)?;
                }
                let BsonValue::Document(document) = result else {
                    unreachable!("document root");
                };
                document
            }
        };
        budget.step()?;
        // Transforms can create deeper/larger BSON than their inputs. Validate
        // every generated row before any later matcher/sorter consumes it.
        encode_document(&result)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        budget.step()?;
        Ok(result)
    }
}

fn flatten<'a>(
    spec: &'a BsonDocument,
    prefix: Option<&str>,
    project: bool,
    depth: usize,
    items: &mut Vec<Flat<'a>>,
    budget: &mut Budget<'_>,
) -> EngineResult<()> {
    for (name, value) in spec.iter() {
        budget.node(depth)?;
        if project && name.is_empty() {
            return Err(query_error(40352));
        }
        let length = prefix
            .map_or(0, |value| value.len() + 1)
            .checked_add(name.len())
            .ok_or_else(limit)?;
        budget.charge(128 + length)?;
        let path = prefix.map_or_else(|| name.to_owned(), |prefix| format!("{prefix}.{name}"));
        if let BsonValue::Document(nested) = value {
            if !contains_operator(nested) {
                if !nested.is_empty() {
                    flatten(nested, Some(&path), project, depth + 1, items, budget)?;
                    continue;
                }
                if project {
                    return Err(query_error(51270));
                }
            }
        }
        let flag = if project {
            match value {
                BsonValue::Boolean(flag) => Some(*flag),
                value => value
                    .canonical_number()
                    .map(|value| value != CanonicalNumber::from_i32(0)),
            }
        } else {
            None
        };
        items.push(Flat { path, flag, value });
    }
    Ok(())
}

fn validate_paths(items: &[Flat<'_>], set: bool, budget: &mut Budget<'_>) -> EngineResult<()> {
    // All syntax checks precede duplicates and ancestor collisions.
    for item in items {
        budget.step()?;
        if item.path.ends_with('.') {
            return Err(query_error(40353));
        }
        if item.path.split('.').any(|part| part.starts_with('$')) {
            return Err(query_error(16410));
        }
        if item.path.is_empty() || item.path.contains('\0') {
            return Err(query_error(40352));
        }
        for (index, part) in item.path.split('.').enumerate() {
            budget.node(index + 1)?;
            if part.is_empty() {
                return Err(query_error(15998));
            }
            if numeric_component(part) {
                return Err(query_error(115));
            }
        }
    }
    let mut seen = BTreeSet::new();
    for item in items {
        budget.step()?;
        if !seen.insert(&item.path) {
            return Err(query_error(if set { 40176 } else { 31250 }));
        }
    }
    let shape = BsonDocument::from_entries(
        items
            .iter()
            .map(|item| (item.path.as_str(), BsonValue::Boolean(true))),
    )
    .expect("validated paths");
    let result = DocumentProjector::compile_with_check(&shape, &mut || budget.step());
    match result {
        Ok(projector) => budget.charge(projector.retained_bytes()),
        Err(error) => {
            let code = error
                .source()
                .and_then(|source| source.downcast_ref::<DocumentQueryError>())
                .map(|error| error.mongo_code());
            if set && matches!(code, Some(31249 | 31250)) {
                Err(query_error(40176))
            } else {
                Err(error)
            }
        }
    }
}

fn basic(items: &[Flat<'_>], budget: &mut Budget<'_>) -> EngineResult<DocumentProjector> {
    let shape = BsonDocument::from_entries(items.iter().map(|item| {
        (
            item.path.as_str(),
            BsonValue::Boolean(item.flag.unwrap_or(true)),
        )
    }))
    .expect("validated paths");
    let projector = DocumentProjector::compile_with_check(&shape, &mut || budget.step())?;
    budget.charge(projector.retained_bytes())?;
    Ok(projector)
}

fn project_kind(items: &[Flat<'_>], budget: &mut Budget<'_>) -> EngineResult<Kind> {
    let mut mode = None;
    let mut expressions = Vec::new();
    for item in items {
        budget.step()?;
        if item.path == "_id" && item.flag.is_some() {
            continue;
        }
        if let Some(include) = item.flag {
            if let Some(previous) = mode {
                if previous != include {
                    return Err(query_error(if previous { 31254 } else { 31253 }));
                }
            }
            mode = Some(include);
        } else {
            if mode == Some(false)
                && matches!(item.value, BsonValue::Document(value) if contains_operator(value))
            {
                return Err(query_error(31252));
            }
            let expression = Expression::compile(item.value, true, budget, 1)?;
            if mode == Some(false) {
                return Err(query_error(31310));
            }
            mode = Some(true);
            expressions.push(expression);
        }
    }
    let projection = basic(items, budget)?;
    if expressions.is_empty() {
        return Ok(Kind::Basic(projection));
    }
    let mut tree = Node::default();
    if !items
        .iter()
        .any(|item| item.path == "_id" || item.path.starts_with("_id."))
    {
        add_path(&mut tree, &["_id"], Leaf::Include, budget)?;
    }
    let mut index = 0;
    for item in items {
        let leaf = match item.flag {
            Some(false) => continue,
            Some(true) => Leaf::Include,
            None => {
                let leaf = Leaf::Computed(index);
                index += 1;
                leaf
            }
        };
        add_path(
            &mut tree,
            &item.path.split('.').collect::<Vec<_>>(),
            leaf,
            budget,
        )?;
    }
    Ok(Kind::Project(tree, expressions))
}

fn add_path(
    node: &mut Node,
    parts: &[&str],
    leaf: Leaf,
    budget: &mut Budget<'_>,
) -> EngineResult<()> {
    budget.step()?;
    node.computed |= matches!(leaf, Leaf::Computed(_));
    if parts.is_empty() {
        node.leaf = Some(leaf);
        return Ok(());
    }
    let index = match child_index(node, parts[0], budget)? {
        Some(index) => index,
        None => {
            let name = budget.field(parts[0])?;
            budget.charge(256)?;
            node.children.push((name, Node::default()));
            node.children.len() - 1
        }
    };
    add_path(&mut node.children[index].1, &parts[1..], leaf, budget)
}

fn child_index(node: &Node, name: &str, budget: &mut Budget<'_>) -> EngineResult<Option<usize>> {
    for (index, (field, _)) in node.children.iter().enumerate() {
        budget.step()?;
        if field == name {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

fn render(
    source: Option<&BsonValue>,
    node: &Node,
    values: &[Option<BsonValue>],
    budget: &mut Budget<'_>,
    depth: usize,
) -> EngineResult<Option<BsonValue>> {
    budget.depth(depth)?;
    if let Some(leaf) = &node.leaf {
        let value = match leaf {
            Leaf::Include => source,
            Leaf::Computed(index) => values[*index].as_ref(),
        };
        return value
            .map(|value| budget.copy_value(value, depth))
            .transpose();
    }
    match source {
        Some(BsonValue::Document(document)) => Ok(Some(BsonValue::Document(render_document(
            Some(document),
            node,
            values,
            budget,
            depth,
        )?))),
        Some(BsonValue::Array(items)) => {
            budget.charge(128)?;
            let mut result = Vec::new();
            for item in items {
                budget.step()?;
                if !node.computed && !matches!(item, BsonValue::Document(_) | BsonValue::Array(_)) {
                    continue;
                }
                if let Some(value) = render(Some(item), node, values, budget, depth + 1)? {
                    result.push(value);
                }
            }
            Ok(Some(BsonValue::Array(result)))
        }
        _ if node.computed => Ok(Some(BsonValue::Document(render_document(
            None, node, values, budget, depth,
        )?))),
        _ => Ok(None),
    }
}

fn render_document(
    source: Option<&BsonDocument>,
    node: &Node,
    values: &[Option<BsonValue>],
    budget: &mut Budget<'_>,
    depth: usize,
) -> EngineResult<BsonDocument> {
    budget.depth(depth)?;
    budget.charge(128 + node.children.len())?;
    let mut result = BsonDocument::new();
    let mut traversed = vec![false; node.children.len()];
    if let Some(source) = source {
        for (name, value) in source.iter() {
            budget.step()?;
            let Some(index) = child_index(node, name, budget)? else {
                continue;
            };
            let child = &node.children[index].1;
            if matches!(child.leaf, Some(Leaf::Computed(_))) {
                continue;
            }
            traversed[index] = true;
            if let Some(value) = render(Some(value), child, values, budget, depth + 1)? {
                result
                    .push(budget.field(name)?, value)
                    .expect("validated field");
            }
        }
    }
    for (index, (name, child)) in node.children.iter().enumerate() {
        budget.step()?;
        if traversed[index] {
            continue;
        }
        let value = source
            .map(|source| lookup(source, name, budget))
            .transpose()?
            .flatten();
        if let Some(value) = render(value, child, values, budget, depth + 1)? {
            result
                .push(budget.field(name)?, value)
                .expect("validated field");
        }
    }
    Ok(result)
}

fn assign(
    container: BsonValue,
    parts: &[String],
    value: Option<&BsonValue>,
    budget: &mut Budget<'_>,
    depth: usize,
) -> EngineResult<BsonValue> {
    budget.depth(depth)?;
    budget.charge(128)?;
    if let BsonValue::Array(items) = container {
        let mut result = Vec::new();
        for item in items {
            result.push(assign(item, parts, value, budget, depth + 1)?);
        }
        return Ok(BsonValue::Array(result));
    }
    let mut entries = match container {
        BsonValue::Document(document) => document.into_entries(),
        _ => Vec::new(),
    };
    let mut found = None;
    for (index, (name, _)) in entries.iter().enumerate() {
        budget.step()?;
        budget.charge(128 + name.len())?;
        if *name == parts[0] {
            found = Some(index);
        }
    }
    if parts.len() == 1 {
        match (found, value) {
            (Some(index), None) => {
                entries.remove(index);
            }
            (Some(index), Some(value)) => {
                entries[index].1 = budget.copy_value(value, depth + 1)?;
            }
            (None, Some(value)) => {
                entries.push((
                    budget.field(&parts[0])?,
                    budget.copy_value(value, depth + 1)?,
                ));
            }
            (None, None) => {}
        }
    } else {
        let child = found.map_or(BsonValue::Null, |index| {
            std::mem::replace(&mut entries[index].1, BsonValue::Null)
        });
        let child = assign(child, &parts[1..], value, budget, depth + 1)?;
        if let Some(index) = found {
            entries[index].1 = child;
        } else {
            entries.push((budget.field(&parts[0])?, child));
        }
    }
    Ok(BsonValue::Document(
        BsonDocument::from_entries(entries).expect("validated fields"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::{EngineError, EngineErrorKind},
        document::{DocumentAggregator, DocumentPipeline},
    };

    fn doc(entries: &[(&str, BsonValue)]) -> BsonDocument {
        BsonDocument::from_entries(entries.iter().cloned()).unwrap()
    }

    fn pipeline(name: &str, value: BsonValue) -> DocumentPipeline {
        DocumentPipeline::new(vec![doc(&[(name, value)])]).unwrap()
    }

    #[test]
    fn transformations_preserve_order_original_inputs_and_independent_values() {
        let source = doc(&[
            ("_id", BsonValue::Int64(1)),
            (
                "a",
                BsonValue::Document(doc(&[
                    ("y", BsonValue::Int32(2)),
                    ("x", BsonValue::Int64(1)),
                    ("old", BsonValue::Int32(0)),
                ])),
            ),
            ("b", BsonValue::Int32(2)),
            ("source", BsonValue::Int64(9)),
            ("c", BsonValue::Int32(3)),
        ]);
        let before = encode_document(&source).unwrap();
        let spec = pipeline(
            "$project",
            BsonValue::Document(doc(&[
                ("_id", BsonValue::Int32(0)),
                ("new_one", BsonValue::from("$source")),
                ("c", BsonValue::Int32(1)),
                ("new_two", BsonValue::from("$source")),
                ("b", BsonValue::Int32(1)),
                ("a.old", BsonValue::from("$source")),
                ("a.x", BsonValue::Int32(1)),
                ("a.y", BsonValue::from("$source")),
            ])),
        );
        let runner = DocumentAggregator::compile(&spec).unwrap();
        let result = runner
            .execute(std::slice::from_ref(&source))
            .unwrap()
            .remove(0);
        assert_eq!(
            result.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            ["a", "b", "c", "new_one", "new_two"]
        );
        let Some(BsonValue::Document(a)) = result.get_first("a") else {
            panic!("document");
        };
        assert_eq!(
            a.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            ["x", "old", "y"]
        );
        assert!(matches!(a.get_first("y"), Some(BsonValue::Int64(9))));
        assert_eq!(encode_document(&source).unwrap(), before);
        let set = pipeline(
            "$set",
            BsonValue::Document(doc(&[
                ("source", BsonValue::Int32(4)),
                ("copied", BsonValue::from("$source")),
                ("gone.deep", BsonValue::from("$$REMOVE")),
                ("b", BsonValue::from("$$REMOVE")),
            ])),
        );
        let result = DocumentAggregator::compile(&set)
            .unwrap()
            .execute(&[source])
            .unwrap()
            .remove(0);
        assert!(matches!(
            result.get_first("copied"),
            Some(BsonValue::Int64(9))
        ));
        assert_eq!(
            result.get_first("gone"),
            Some(&BsonValue::Document(doc(&[])))
        );
        assert!(result.get_first("b").is_none());
    }

    #[test]
    fn computed_rows_obey_lazy_limits_before_and_after_blocking_stages() {
        let source = [
            doc(&[
                ("_id", BsonValue::Int32(1)),
                ("v", BsonValue::Array(vec![])),
            ]),
            doc(&[("_id", BsonValue::Int32(2)), ("v", BsonValue::Null)]),
        ];
        for sorted in [false, true] {
            let mut stages = Vec::new();
            if sorted {
                stages.push(doc(&[(
                    "$sort",
                    BsonValue::Document(doc(&[("_id", BsonValue::Int32(1))])),
                )]));
            }
            stages.push(doc(&[(
                "$set",
                BsonValue::Document(doc(&[(
                    "n",
                    BsonValue::Document(doc(&[("$size", BsonValue::from("$v"))])),
                )])),
            )]));
            stages.push(doc(&[("$limit", BsonValue::Int32(1))]));
            let plan = DocumentPipeline::new(stages).unwrap();
            let runner = DocumentAggregator::compile(&plan).unwrap();
            let result = runner.execute(&source).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].get_first("n"), Some(&BsonValue::Int32(0)));
            let mut stream = runner.into_stream();
            let mut streamed = Vec::new();
            for document in source.iter().cloned() {
                if stream.is_input_exhausted() {
                    break;
                }
                if let Some(document) = stream.push(document).unwrap() {
                    streamed.push(document);
                }
            }
            streamed.extend(stream.finish().unwrap());
            assert_eq!(streamed, result);
        }
    }

    #[test]
    fn allocation_amplification_depth_and_compilation_limits_fail_closed() {
        let source = doc(&[
            ("payload", BsonValue::from("x".repeat(1024 * 1024))),
            ("array", BsonValue::Array(vec![BsonValue::Null; 128])),
        ]);
        for name in ["$project", "$set", "$addFields"] {
            let runner = DocumentAggregator::compile(&pipeline(
                name,
                BsonValue::Document(doc(&[("array.copy", BsonValue::from("$payload"))])),
            ))
            .unwrap();
            assert_eq!(
                runner
                    .execute(std::slice::from_ref(&source))
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::LimitExceeded
            );
            let mut stream = runner.into_stream();
            assert_eq!(
                stream.push(source.clone()).unwrap_err().kind(),
                EngineErrorKind::LimitExceeded
            );
            assert_eq!(
                stream.finish().unwrap_err().kind(),
                EngineErrorKind::FailedPrecondition
            );
        }
        let mut deep = BsonValue::Int32(1);
        for _ in 0..70 {
            deep = BsonValue::Document(doc(&[("a", deep)]));
        }
        let source = doc(&[("deep", deep)]);
        for name in ["$project", "$set"] {
            let path = vec!["a"; 50].join(".");
            let runner = DocumentAggregator::compile(&pipeline(
                name,
                BsonValue::Document(doc(&[(&path, BsonValue::from("$deep"))])),
            ))
            .unwrap();
            assert_eq!(
                runner
                    .execute(std::slice::from_ref(&source))
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::LimitExceeded
            );
        }
        for value in [
            BsonValue::Document(doc(&[("v", BsonValue::from("x".repeat(MAX_SPEC_BYTES)))])),
            BsonValue::Document(doc(&[(&vec!["a"; 101].join("."), BsonValue::Int32(1))])),
            BsonValue::Document(
                BsonDocument::from_entries(
                    (0..4097).map(|index| (format!("f{index}"), BsonValue::Int32(1))),
                )
                .unwrap(),
            ),
        ] {
            let plan = pipeline("$set", value);
            assert_eq!(
                DocumentAggregator::compile(&plan).unwrap_err().kind(),
                EngineErrorKind::LimitExceeded
            );
        }
        let mut check = || Ok(());
        let mut budget = Budget::new(&mut check);
        budget.bytes = super::super::aggregation_expression::MAX_BYTES;
        assert_eq!(
            budget.copy_value(&BsonValue::Null, 1).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
    }

    #[test]
    fn every_transform_compile_and_execution_checkpoint_honors_interruptions() {
        let source = doc(&[
            ("_id", BsonValue::Int32(1)),
            ("source", BsonValue::Int64(7)),
            (
                "array",
                BsonValue::Array(vec![
                    BsonValue::Document(doc(&[("old", BsonValue::Int32(1))])),
                    BsonValue::Null,
                ]),
            ),
        ]);
        for (name, value) in [
            (
                "$project",
                BsonValue::Document(doc(&[
                    ("array.old", BsonValue::Int32(1)),
                    ("array.copy", BsonValue::from("$source")),
                    ("missing.x", BsonValue::from("$$REMOVE")),
                ])),
            ),
            (
                "$set",
                BsonValue::Document(doc(&[
                    ("array.copy", BsonValue::from("$source")),
                    (
                        "n",
                        BsonValue::Document(doc(&[("$size", BsonValue::from("$array"))])),
                    ),
                ])),
            ),
            ("$unset", BsonValue::from("array.old")),
        ] {
            let plan = pipeline(name, value);
            let mut compilation = 0;
            let runner = DocumentAggregator::compile_with_check(&plan, &mut || {
                compilation += 1;
                Ok(())
            })
            .unwrap();
            let mut execution = 0;
            let expected = runner
                .execute_with_check(std::slice::from_ref(&source), &mut || {
                    execution += 1;
                    Ok(())
                })
                .unwrap();
            for kind in [
                EngineErrorKind::Cancelled,
                EngineErrorKind::DeadlineExceeded,
            ] {
                for stop in 1..=compilation {
                    let mut count = 0;
                    let error = DocumentAggregator::compile_with_check(&plan, &mut || {
                        count += 1;
                        if count == stop {
                            Err(EngineError::new(kind, "test interruption"))
                        } else {
                            Ok(())
                        }
                    })
                    .unwrap_err();
                    assert_eq!(error.kind(), kind);
                }
                for stop in 1..=execution {
                    let mut count = 0;
                    let error = runner
                        .execute_with_check(std::slice::from_ref(&source), &mut || {
                            count += 1;
                            if count == stop {
                                Err(EngineError::new(kind, "test interruption"))
                            } else {
                                Ok(())
                            }
                        })
                        .unwrap_err();
                    assert_eq!(error.kind(), kind);
                    assert!(!format!("{error:?}").contains("$source"));
                }
            }
            assert_eq!(
                runner.execute(std::slice::from_ref(&source)).unwrap(),
                expected
            );
        }
    }
}
