//! Shared, bounded inclusion/exclusion projection over owned BSON values.

use std::{collections::BTreeMap, fmt, sync::OnceLock};

use fancy_regex::Regex;

use super::{
    BsonCodecOptions, BsonDocument, BsonErrorContext, BsonValue, encode_document,
    encode_document_with_options, matcher::query_error, number::CanonicalNumber,
};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

const MAX_SPEC_BYTES: usize = 1024 * 1024;
const MAX_NODES: usize = 4096;
const MAX_DEPTH: usize = 100;
const MAX_STEPS: usize = 1_000_000;

#[derive(Default)]
struct Node {
    leaf: bool,
    children: BTreeMap<String, Node>,
}

#[derive(Clone, Copy)]
enum Mode {
    Include,
    Exclude,
}

/// An eagerly validated basic Mongo projection. Filtering and sorting must use
/// the original document; this transformation preserves retained field order
/// and exact BSON representations without mutating the stored document.
pub struct DocumentProjector {
    mode: Option<Mode>,
    tree: Node,
    include_id: bool,
    retained_bytes: usize,
}

impl fmt::Debug for DocumentProjector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentProjector")
            .finish_non_exhaustive()
    }
}

impl DocumentProjector {
    pub fn compile(spec: &BsonDocument) -> EngineResult<Self> {
        Self::compile_with_check(spec, &mut || Ok(()))
    }

    /// Validate all flag/path shapes before mode and path-collision checks,
    /// following the locked TinyMongo normalization order.
    pub fn compile_with_check(
        spec: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Self> {
        check()?;
        let bytes = encode_document_with_options(
            spec,
            &BsonCodecOptions::new().with_max_document_bytes(MAX_SPEC_BYTES),
        )
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        let mut nodes = 0;
        visit(spec, &mut Vec::new(), &mut nodes, check, &mut |_, _| Ok(()))?;
        let mut projector = Self {
            mode: None,
            tree: Node::default(),
            include_id: true,
            retained_bytes: bytes.len().saturating_mul(16).saturating_add(nodes * 128),
        };
        visit(
            spec,
            &mut Vec::new(),
            &mut 0,
            check,
            &mut |parts, include| {
                let mut node = &mut projector.tree;
                for part in parts {
                    if node.leaf {
                        return Err(query_error(31249));
                    }
                    node = node.children.entry((*part).to_owned()).or_default();
                }
                if node.leaf || !node.children.is_empty() {
                    return Err(query_error(31250));
                }
                node.leaf = true;
                if parts == ["_id"] {
                    projector.include_id = include;
                } else if let Some(mode) = projector.mode {
                    if matches!(mode, Mode::Include) != include {
                        return Err(query_error(if matches!(mode, Mode::Include) {
                            31254
                        } else {
                            31253
                        }));
                    }
                } else {
                    projector.mode = Some(if include {
                        Mode::Include
                    } else {
                        Mode::Exclude
                    });
                }
                Ok(())
            },
        )?;
        if !spec.is_empty() && projector.mode.is_none() {
            projector.mode = Some(if projector.include_id {
                Mode::Include
            } else {
                Mode::Exclude
            });
        }
        if projector
            .tree
            .children
            .get("_id")
            .is_some_and(|node| node.leaf)
        {
            projector.tree.children.remove("_id");
        }
        check()?;
        Ok(projector)
    }

    /// Validate caller-owned BSON and return an independent projected copy.
    pub fn project(&self, document: &BsonDocument) -> EngineResult<BsonDocument> {
        encode_document(document)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        self.project_owned_validated_with_check(document.clone(), &mut || Ok(()))
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Storage has already validated these values. Called only within admitted
    /// blocking work; moving leaves avoids copying large BSON payloads.
    pub(crate) fn project_owned_validated_with_check(
        &self,
        document: BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<BsonDocument> {
        let mut budget = Budget { steps: 0, check };
        budget.step()?;
        let Some(mode) = self.mode else {
            return Ok(document);
        };
        let mut result = BsonDocument::new();
        for (name, value) in document.into_entries() {
            budget.step()?;
            let child = self.tree.children.get(&name);
            let projected = if name == "_id" && child.is_none() {
                self.include_id.then_some(value)
            } else {
                match mode {
                    Mode::Include => match child {
                        Some(child) => include(value, child, &mut budget)?,
                        None => None,
                    },
                    Mode::Exclude => match child {
                        Some(child) => exclude(value, child, &mut budget)?,
                        None => Some(value),
                    },
                }
            };
            if let Some(value) = projected {
                result.push(name, value).expect("validated BSON field name");
            }
        }
        budget.step()?;
        Ok(result)
    }
}

fn limit() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "document projection resource limit exceeded",
    )
}

pub(super) fn numeric_component(part: &str) -> bool {
    // Python str.isdigit includes Unicode decimal digits plus these digit
    // characters (superscripts, circled digits, and historic digit forms).
    // Numeric-but-not-digit characters such as fractions remain valid names.
    static DIGITS: OnceLock<Regex> = OnceLock::new();
    let digits = DIGITS.get_or_init(|| {
        Regex::new(concat!(
            r"^[\d\u{B2}\u{B3}\u{B9}\u{1369}-\u{1371}\u{19DA}\u{2070}",
            r"\u{2074}-\u{2079}\u{2080}-\u{2089}\u{2460}-\u{2468}",
            r"\u{2474}-\u{247C}\u{2488}-\u{2490}\u{24EA}\u{24F5}-\u{24FD}\u{24FF}",
            r"\u{2776}-\u{277E}\u{2780}-\u{2788}\u{278A}-\u{2792}",
            r"\u{10A40}-\u{10A43}\u{10E60}-\u{10E68}\u{11052}-\u{1105A}",
            r"\u{1F100}-\u{1F10A}]+$"
        ))
        .expect("static Unicode digit class")
    });
    digits
        .is_match(part.trim_start_matches('-'))
        .expect("linear digit expression")
}

fn visit<'a>(
    document: &'a BsonDocument,
    prefix: &mut Vec<&'a str>,
    nodes: &mut usize,
    check: &mut dyn FnMut() -> EngineResult<()>,
    emit: &mut dyn FnMut(&[&str], bool) -> EngineResult<()>,
) -> EngineResult<()> {
    for (name, value) in document.iter() {
        check()?;
        if name.is_empty() {
            return Err(query_error(40352));
        }
        let base = prefix.len();
        for part in name.split('.') {
            *nodes += 1;
            if *nodes > MAX_NODES || prefix.len() >= MAX_DEPTH {
                return Err(limit());
            }
            if part.is_empty() {
                return Err(query_error(15998));
            }
            if part.starts_with('$') || numeric_component(part) {
                return Err(query_error(115));
            }
            prefix.push(part);
        }
        if let BsonValue::Document(nested) = value {
            if nested.is_empty() {
                return Err(query_error(115));
            }
            visit(nested, prefix, nodes, check, emit)?;
        } else {
            let flag = match value {
                BsonValue::Boolean(flag) => *flag,
                value => value
                    .canonical_number()
                    .map(|number| number != CanonicalNumber::from_i32(0))
                    .ok_or_else(|| query_error(115))?,
            };
            emit(prefix, flag)?;
        }
        prefix.truncate(base);
    }
    Ok(())
}

struct Budget<'a> {
    steps: usize,
    check: &'a mut dyn FnMut() -> EngineResult<()>,
}

impl Budget<'_> {
    fn step(&mut self) -> EngineResult<()> {
        self.steps += 1;
        if self.steps > MAX_STEPS {
            return Err(limit());
        }
        (self.check)()
    }
}

fn include(
    value: BsonValue,
    node: &Node,
    budget: &mut Budget<'_>,
) -> EngineResult<Option<BsonValue>> {
    budget.step()?;
    if node.leaf {
        return Ok(Some(value));
    }
    match value {
        BsonValue::Document(document) => {
            let mut result = BsonDocument::new();
            for (name, value) in document.into_entries() {
                budget.step()?;
                if let Some(child) = node.children.get(&name) {
                    if let Some(value) = include(value, child, budget)? {
                        result.push(name, value).expect("validated BSON field name");
                    }
                }
            }
            Ok(Some(BsonValue::Document(result)))
        }
        BsonValue::Array(items) => {
            let mut result = Vec::new();
            for item in items {
                budget.step()?;
                if matches!(item, BsonValue::Document(_) | BsonValue::Array(_)) {
                    if let Some(value) = include(item, node, budget)? {
                        result.push(value);
                    }
                }
            }
            Ok(Some(BsonValue::Array(result)))
        }
        _ => Ok(None),
    }
}

fn exclude(
    value: BsonValue,
    node: &Node,
    budget: &mut Budget<'_>,
) -> EngineResult<Option<BsonValue>> {
    budget.step()?;
    if node.leaf {
        return Ok(None);
    }
    match value {
        BsonValue::Document(document) => {
            let mut result = BsonDocument::new();
            for (name, value) in document.into_entries() {
                budget.step()?;
                let projected = match node.children.get(&name) {
                    Some(child) => exclude(value, child, budget)?,
                    None => Some(value),
                };
                if let Some(value) = projected {
                    result.push(name, value).expect("validated BSON field name");
                }
            }
            Ok(Some(BsonValue::Document(result)))
        }
        BsonValue::Array(items) => {
            let mut result = Vec::new();
            for item in items {
                if let Some(value) = exclude(item, node, budget)? {
                    result.push(value);
                }
            }
            Ok(Some(BsonValue::Array(result)))
        }
        value => Ok(Some(value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn document(entries: &[(&str, BsonValue)]) -> BsonDocument {
        BsonDocument::from_entries(entries.iter().cloned()).unwrap()
    }

    #[test]
    fn projection_preserves_order_representation_and_unprojected_input() {
        let input = document(&[
            ("z", BsonValue::Int64(1)),
            ("_id", BsonValue::Int32(2)),
            ("a", BsonValue::Double(-0.0)),
            ("hidden", BsonValue::from("secret")),
        ]);
        let before = encode_document(&input).unwrap();
        let projector = DocumentProjector::compile(&document(&[
            ("a", BsonValue::Int32(1)),
            ("z", BsonValue::Int32(1)),
        ]))
        .unwrap();
        assert!(
            projector
                .project(&input)
                .unwrap()
                .representation_eq(&document(&[
                    ("z", BsonValue::Int64(1)),
                    ("_id", BsonValue::Int32(2)),
                    ("a", BsonValue::Double(-0.0)),
                ]))
        );
        assert_eq!(encode_document(&input).unwrap(), before);
        assert!(!format!("{projector:?}").contains("secret"));
    }

    #[test]
    fn projection_bounds_depth_nodes_bytes_and_cooperative_work() {
        for spec in [
            BsonDocument::from_entries(
                (0..=MAX_NODES).map(|index| (format!("f{index}"), BsonValue::Int32(1))),
            )
            .unwrap(),
            document(&[(&vec!["a"; MAX_DEPTH + 1].join("."), BsonValue::Int32(1))]),
            document(&[(&"x".repeat(MAX_SPEC_BYTES), BsonValue::Int32(1))]),
        ] {
            assert_eq!(
                DocumentProjector::compile(&spec).unwrap_err().kind(),
                EngineErrorKind::LimitExceeded
            );
        }
        let spec = document(&[("v.a", BsonValue::Int32(1))]);
        let cancelled = || EngineError::new(EngineErrorKind::Cancelled, "test cancellation");
        assert_eq!(
            DocumentProjector::compile_with_check(&spec, &mut || Err(cancelled()))
                .unwrap_err()
                .kind(),
            EngineErrorKind::Cancelled
        );
        let projector = DocumentProjector::compile(&spec).unwrap();
        let input = document(&[(
            "v",
            BsonValue::Array(vec![
                BsonValue::Document(document(&[(
                    "a",
                    BsonValue::Int32(1)
                )]));
                10
            ]),
        )]);
        let mut steps = 0;
        let error = projector
            .project_owned_validated_with_check(input, &mut || {
                steps += 1;
                if steps == 8 { Err(cancelled()) } else { Ok(()) }
            })
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
    }
}
