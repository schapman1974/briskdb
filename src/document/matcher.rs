//! Authoritative, bounded document matching. Semantics follow the source-locked
//! TinyMongo v1 contract; no SQLite predicate or wire adapter defines equality.

use std::{error::Error, fmt};

use fancy_regex::{Regex, RegexBuilder};
use num_bigint::BigUint;

use super::{
    BsonCodecOptions, BsonDocument, BsonErrorContext, BsonRegex, BsonValue, encode_document,
    encode_document_with_options, number::CanonicalNumber,
};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

mod pull;
mod regex_compat;
mod upsert;
pub(super) use pull::PullMatcher;

/// Optional value/allocation accounting for an updater sharing this matcher.
/// Ordinary reads retain their existing matcher bounds and request checks.
pub(super) trait MatchControl {
    fn step(&mut self) -> EngineResult<()>;
    fn value(&mut self, _value: &BsonValue) -> EngineResult<()> {
        Ok(())
    }
    fn comparison_bytes(&mut self, _bytes: usize) -> EngineResult<()> {
        Ok(())
    }
    fn allocation(&mut self, _bytes: usize) -> EngineResult<()> {
        Ok(())
    }
}

impl<F: FnMut() -> EngineResult<()>> MatchControl for F {
    fn step(&mut self) -> EngineResult<()> {
        self()
    }
}

const MAX_QUERY_BYTES: usize = 1024 * 1024;
const MAX_QUERY_NODES: usize = 4096;
const MAX_QUERY_DEPTH: usize = 100;
const MAX_PATH_CANDIDATES: usize = 16_384;
const MAX_MATCH_STEPS: usize = 1_000_000;
const MAX_REGEX_COUNT: usize = 32;
const MAX_REGEX_BYTES: usize = 4096;
const REGEX_BACKTRACK_LIMIT: usize = 100_000;

/// A payload-free query validation error with a stable Mongo-compatible code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DocumentQueryError {
    code: i32,
}

impl DocumentQueryError {
    pub const fn mongo_code(self) -> i32 {
        self.code
    }
}

impl fmt::Display for DocumentQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid or unsupported document query")
    }
}

impl Error for DocumentQueryError {}

pub(super) fn query_error(code: i32) -> EngineError {
    let kind = match code {
        115 => EngineErrorKind::Unsupported,
        14 => EngineErrorKind::TypeMismatch,
        _ => EngineErrorKind::InvalidQuery,
    };
    EngineError::from_source(
        kind,
        "invalid or unsupported document query",
        DocumentQueryError { code },
    )
}

fn limit() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "document matcher resource limit exceeded",
    )
}

/// An eagerly validated query. Compilation and execution have independent
/// bounds; all branches are validated even when a logical clause short-circuits.
pub struct DocumentMatcher {
    clauses: Vec<Clause>,
    retained_bytes: usize,
}

impl fmt::Debug for DocumentMatcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentMatcher")
            .field("query", &"<redacted>")
            .finish()
    }
}

enum Clause {
    Field {
        path: Vec<String>,
        exact_id: bool,
        predicates: Vec<Predicate>,
    },
    Logical {
        kind: Logical,
        children: Vec<DocumentMatcher>,
    },
}

#[derive(Clone, Copy)]
enum Logical {
    And,
    Or,
    Nor,
}

enum Predicate {
    Equal(BsonValue),
    NotEqual(BsonValue),
    Range {
        operand: BsonValue,
        greater: bool,
        inclusive: bool,
    },
    Exists(bool),
    In {
        members: Vec<Member>,
        negative: bool,
    },
    All(Vec<Member>),
    Element(Element),
    Not(Vec<Predicate>),
    Regex(CompiledRegex),
    Size(usize),
    Types(Vec<&'static str>),
    Mod(i64, i64),
}

enum Member {
    Literal(BsonValue),
    Regex(CompiledRegex),
    Element(Element),
}

enum Element {
    Fields(Vec<Predicate>),
    Document(DocumentMatcher),
}

struct CompiledRegex {
    identity: BsonRegex,
    expression: Option<Regex>,
}

struct Compiler<'a> {
    nodes: usize,
    regexes: usize,
    check: &'a mut dyn FnMut() -> EngineResult<()>,
}

impl DocumentMatcher {
    pub fn compile(filter: &BsonDocument) -> EngineResult<Self> {
        Self::compile_with_check(filter, &mut || Ok(()))
    }

    pub(crate) fn compile_with_check(
        filter: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Self> {
        check()?;
        let encoded = encode_document_with_options(
            filter,
            &BsonCodecOptions::new().with_max_document_bytes(MAX_QUERY_BYTES),
        )
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        let mut compiler = Compiler {
            nodes: 0,
            regexes: 0,
            check,
        };
        let mut matcher = compiler.document(filter, 0, false)?;
        // Conservative accounting for owned AST/value allocations and bounded
        // regex programs/caches. This is a retention quota, not an RSS metric.
        matcher.retained_bytes = encoded
            .len()
            .saturating_mul(16)
            .saturating_add(compiler.nodes.saturating_mul(128))
            .saturating_add(compiler.regexes.saturating_mul(1024 * 1024));
        Ok(matcher)
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Borrow one necessary equality, never an alternative or negation. Array
    /// documents may satisfy multiple equalities on one field, so selecting one
    /// is a candidate restriction, not a replacement for this matcher.
    pub(crate) fn equality_for_index_path<'a>(
        &'a self,
        requested: &[String],
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Option<&'a BsonValue>> {
        for clause in &self.clauses {
            check()?;
            match clause {
                Clause::Field {
                    path, predicates, ..
                } if path == requested => {
                    for predicate in predicates {
                        check()?;
                        if let Predicate::Equal(value) = predicate {
                            return Ok(Some(value));
                        }
                    }
                }
                Clause::Logical {
                    kind: Logical::And,
                    children,
                } => {
                    for child in children {
                        if let Some(value) = child.equality_for_index_path(requested, check)? {
                            return Ok(Some(value));
                        }
                    }
                }
                _ => (),
            }
        }
        check()?;
        Ok(None)
    }

    /// Borrow one necessary positive literal membership list. Alternatives,
    /// negations and regex members cannot establish a finite equality probe.
    /// This does not simplify the matcher or flatten array-valued operands.
    pub(crate) fn membership_for_index_path<'a>(
        &'a self,
        requested: &[String],
        max_values: usize,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Option<Vec<&'a BsonValue>>> {
        for clause in &self.clauses {
            check()?;
            match clause {
                Clause::Field {
                    path, predicates, ..
                } if path == requested => {
                    for predicate in predicates {
                        check()?;
                        let Predicate::In {
                            members,
                            negative: false,
                        } = predicate
                        else {
                            continue;
                        };
                        if members.is_empty() || members.len() > max_values {
                            continue;
                        }
                        let mut values = Vec::new();
                        values
                            .try_reserve_exact(members.len())
                            .map_err(|_| limit())?;
                        for member in members {
                            check()?;
                            let Member::Literal(value) = member else {
                                values.clear();
                                break;
                            };
                            values.push(value);
                        }
                        if !values.is_empty() {
                            return Ok(Some(values));
                        }
                    }
                }
                Clause::Logical {
                    kind: Logical::And,
                    children,
                } => {
                    for child in children {
                        if let Some(values) =
                            child.membership_for_index_path(requested, max_values, check)?
                        {
                            return Ok(Some(values));
                        }
                    }
                }
                _ => (),
            }
        }
        check()?;
        Ok(None)
    }

    /// Prove explicit field presence from a necessary positive clause only.
    /// Alternatives and negations cannot grant sparse-index authority.
    pub(crate) fn requires_index_path_presence(
        &self,
        requested: &[String],
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<bool> {
        for clause in &self.clauses {
            check()?;
            match clause {
                Clause::Field {
                    path, predicates, ..
                } if path == requested => {
                    for predicate in predicates {
                        check()?;
                        if matches!(predicate, Predicate::Exists(true)) {
                            return Ok(true);
                        }
                    }
                }
                Clause::Logical {
                    kind: Logical::And,
                    children,
                } => {
                    for child in children {
                        if child.requires_index_path_presence(requested, check)? {
                            return Ok(true);
                        }
                    }
                }
                _ => (),
            }
        }
        check()?;
        Ok(false)
    }

    /// Match caller-supplied BSON after structural size/depth validation.
    pub fn matches(&self, document: &BsonDocument) -> EngineResult<bool> {
        encode_document(document)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        self.matches_with_check(document, &mut || Ok(()))
    }

    /// Stored documents have already passed the bounded BSON codec.
    pub(crate) fn matches_with_check(
        &self,
        document: &BsonDocument,
        mut check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<bool> {
        let mut work = Work {
            steps: 0,
            check: &mut check,
        };
        self.evaluate(Root::Document(document), true, false, &mut work)
    }
}

impl Compiler<'_> {
    fn step(&mut self, depth: usize) -> EngineResult<()> {
        (self.check)()?;
        self.nodes += 1;
        if self.nodes > MAX_QUERY_NODES || depth > MAX_QUERY_DEPTH {
            return Err(limit());
        }
        Ok(())
    }

    fn document(
        &mut self,
        filter: &BsonDocument,
        depth: usize,
        inside_element: bool,
    ) -> EngineResult<DocumentMatcher> {
        self.step(depth)?;
        let mut clauses = Vec::new();
        for (name, value) in filter.iter() {
            self.step(depth)?;
            match name {
                "$comment" => {}
                "$and" | "$or" | "$nor" => {
                    let BsonValue::Array(values) = value else {
                        return Err(query_error(2));
                    };
                    if values.is_empty() {
                        return Err(query_error(2));
                    }
                    let mut children = Vec::new();
                    for child in values {
                        let BsonValue::Document(child) = child else {
                            return Err(query_error(14));
                        };
                        children.push(self.document(child, depth + 1, inside_element)?);
                    }
                    let kind = match name {
                        "$and" => Logical::And,
                        "$or" => Logical::Or,
                        _ => Logical::Nor,
                    };
                    clauses.push(Clause::Logical { kind, children });
                }
                _ if name.starts_with('$') => {
                    return Err(query_error(
                        if inside_element && (field_operator(name) || !known_operator(name)) {
                            2
                        } else {
                            115
                        },
                    ));
                }
                _ => {
                    let path: Vec<_> = name.split('.').map(str::to_owned).collect();
                    if path.len() > MAX_QUERY_DEPTH {
                        return Err(limit());
                    }
                    let predicates = match value {
                        BsonValue::Document(expression) if operator_document(expression) => {
                            self.predicates(expression, depth + 1, inside_element)?
                        }
                        BsonValue::RegularExpression(regex) => vec![Predicate::Regex(
                            self.regex(&BsonValue::RegularExpression(regex.clone()), "")?,
                        )],
                        _ => vec![Predicate::Equal(value.clone())],
                    };
                    clauses.push(Clause::Field {
                        path,
                        exact_id: name == "_id",
                        predicates,
                    });
                }
            }
        }
        Ok(DocumentMatcher {
            clauses,
            retained_bytes: 0,
        })
    }

    fn predicates(
        &mut self,
        expression: &BsonDocument,
        depth: usize,
        inside_element: bool,
    ) -> EngineResult<Vec<Predicate>> {
        self.step(depth)?;
        let mut result = Vec::new();
        for (operator, operand) in expression.iter() {
            self.step(depth)?;
            if !field_operator(operator) {
                return Err(query_error(
                    if !operator.starts_with('$')
                        || operator == "$comment"
                        || (inside_element && !known_operator(operator))
                    {
                        2
                    } else {
                        115
                    },
                ));
            }
            let predicate = match operator {
                "$eq" => Predicate::Equal(operand.clone()),
                "$ne" => {
                    if matches!(operand, BsonValue::RegularExpression(_)) {
                        return Err(query_error(2));
                    }
                    Predicate::NotEqual(operand.clone())
                }
                "$gt" | "$gte" | "$lt" | "$lte" => {
                    if matches!(operand, BsonValue::RegularExpression(_)) {
                        return Err(query_error(2));
                    }
                    Predicate::Range {
                        operand: operand.clone(),
                        greater: operator.starts_with("$g"),
                        inclusive: operator.ends_with('e'),
                    }
                }
                "$exists" => Predicate::Exists(truthy(operand)),
                "$in" | "$nin" | "$all" => {
                    let BsonValue::Array(values) = operand else {
                        return Err(query_error(2));
                    };
                    let mut members = Vec::new();
                    for value in values {
                        self.step(depth)?;
                        let member = match value {
                            BsonValue::RegularExpression(_) => {
                                Member::Regex(self.regex(value, "")?)
                            }
                            BsonValue::Document(doc) if operator_document(doc) => {
                                if operator != "$all" || doc.len() != 1 {
                                    return Err(query_error(2));
                                }
                                let Some(element) = doc.get_first("$elemMatch") else {
                                    return Err(query_error(2));
                                };
                                Member::Element(self.element(element, depth + 1)?)
                            }
                            _ => Member::Literal(value.clone()),
                        };
                        members.push(member);
                    }
                    if operator == "$all" {
                        Predicate::All(members)
                    } else {
                        Predicate::In {
                            members,
                            negative: operator == "$nin",
                        }
                    }
                }
                "$elemMatch" => Predicate::Element(self.element(operand, depth + 1)?),
                "$not" => {
                    let nested = match operand {
                        BsonValue::RegularExpression(_) => {
                            vec![Predicate::Regex(self.regex(operand, "")?)]
                        }
                        BsonValue::Document(doc) if !doc.is_empty() => {
                            self.predicates(doc, depth + 1, inside_element)?
                        }
                        _ => return Err(query_error(2)),
                    };
                    Predicate::Not(nested)
                }
                "$regex" => {
                    let options = match expression.get_first("$options") {
                        Some(BsonValue::String(options)) => options.as_str(),
                        None => "",
                        _ => return Err(query_error(2)),
                    };
                    Predicate::Regex(self.regex(operand, options)?)
                }
                "$options" => {
                    if expression.get_first("$regex").is_none() {
                        return Err(query_error(2));
                    }
                    continue;
                }
                "$size" => {
                    let size = integer(operand, false)
                        .filter(|value| (0..=i64::from(i32::MAX)).contains(value))
                        .ok_or_else(|| query_error(2))?;
                    Predicate::Size(size as usize)
                }
                "$mod" => {
                    let BsonValue::Array(values) = operand else {
                        return Err(query_error(2));
                    };
                    if values.len() != 2 {
                        return Err(query_error(2));
                    }
                    let divisor = integer(&values[0], true)
                        .filter(|value| *value != 0)
                        .ok_or_else(|| query_error(2))?;
                    let remainder = integer(&values[1], true).ok_or_else(|| query_error(2))?;
                    Predicate::Mod(divisor, remainder)
                }
                "$type" => Predicate::Types(types(operand, self.check)?),
                _ => unreachable!("validated field operator"),
            };
            result.push(predicate);
        }
        Ok(result)
    }

    fn element(&mut self, operand: &BsonValue, depth: usize) -> EngineResult<Element> {
        let BsonValue::Document(doc) = operand else {
            return Err(query_error(2));
        };
        let logical = doc
            .iter()
            .any(|(key, _)| matches!(key, "$and" | "$or" | "$nor"));
        if !logical && doc.iter().any(|(key, _)| field_operator(key)) {
            Ok(Element::Fields(self.predicates(doc, depth, true)?))
        } else {
            Ok(Element::Document(self.document(doc, depth, true)?))
        }
    }

    fn regex(&mut self, operand: &BsonValue, options: &str) -> EngineResult<CompiledRegex> {
        (self.check)()?;
        if options.bytes().any(|byte| !b"imsux".contains(&byte)) {
            return Err(query_error(51108));
        }
        let (pattern, flags) = match operand {
            BsonValue::String(pattern) => (pattern.as_str(), options),
            BsonValue::RegularExpression(regex) => {
                if !options.is_empty() && !regex.options().is_empty() {
                    return Err(query_error(51075));
                }
                (
                    regex.pattern(),
                    if options.is_empty() {
                        regex.options()
                    } else {
                        options
                    },
                )
            }
            _ => return Err(query_error(2)),
        };
        if pattern.contains('\0') {
            return Err(query_error(2));
        }
        self.regexes += 1;
        if self.regexes > MAX_REGEX_COUNT || pattern.len() > MAX_REGEX_BYTES {
            return Err(limit());
        }
        let identity = BsonRegex::new(pattern, flags).map_err(|_| query_error(2))?;
        // Locale regex values retain BSON identity but cannot execute against
        // Unicode strings, matching the locked oracle's Python regex behavior.
        let expression = if flags.contains('l') {
            None
        } else {
            let pattern = regex_compat::translate(pattern, flags, self.check)?;
            Some(
                RegexBuilder::new(&pattern)
                    .backtrack_limit(REGEX_BACKTRACK_LIMIT)
                    .delegate_size_limit(256 * 1024)
                    .delegate_dfa_size_limit(256 * 1024)
                    .build()
                    .map_err(|_| query_error(51091))?,
            )
        };
        Ok(CompiledRegex {
            identity,
            expression,
        })
    }
}

fn operator_document(document: &BsonDocument) -> bool {
    document.iter().any(|(key, _)| key.starts_with('$'))
}

fn field_operator(name: &str) -> bool {
    matches!(
        name,
        "$all"
            | "$elemMatch"
            | "$eq"
            | "$exists"
            | "$gt"
            | "$gte"
            | "$in"
            | "$lt"
            | "$lte"
            | "$ne"
            | "$nin"
            | "$not"
            | "$mod"
            | "$options"
            | "$regex"
            | "$size"
            | "$type"
    )
}

fn known_operator(name: &str) -> bool {
    field_operator(name)
        || matches!(
            name,
            "$and"
                | "$or"
                | "$nor"
                | "$comment"
                | "$bitsAllClear"
                | "$bitsAllSet"
                | "$bitsAnyClear"
                | "$bitsAnySet"
                | "$expr"
                | "$geoIntersects"
                | "$geoWithin"
                | "$jsonSchema"
                | "$near"
                | "$nearSphere"
                | "$text"
                | "$where"
        )
}

fn truthy(value: &BsonValue) -> bool {
    match value {
        BsonValue::Null => false,
        BsonValue::Boolean(value) => *value,
        BsonValue::Int32(value) => *value != 0,
        BsonValue::Int64(value) => *value != 0,
        BsonValue::Double(value) => *value != 0.0,
        BsonValue::String(value) => !value.is_empty(),
        BsonValue::Array(value) => !value.is_empty(),
        BsonValue::Document(value) => !value.is_empty(),
        BsonValue::Binary(value) => !value.bytes().is_empty(),
        BsonValue::JavaScript(value) => !value.code().is_empty(),
        _ => true,
    }
}

pub(super) fn integer(value: &BsonValue, truncate: bool) -> Option<i64> {
    match value {
        BsonValue::Int32(value) => return Some(i64::from(*value)),
        BsonValue::Int64(value) => return Some(*value),
        BsonValue::Double(value) => {
            let integer = value.trunc();
            return (value.is_finite()
                && (truncate || integer == *value)
                && integer >= i64::MIN as f64
                && integer < -(i64::MIN as f64))
                .then_some(integer as i64);
        }
        _ => {}
    }
    let CanonicalNumber::Finite(value) = value.canonical_number()? else {
        return None;
    };
    let mut numerator = BigUint::from(value.coefficient());
    let mut denominator = BigUint::from(1_u8);
    for (base, exponent) in [(2_u8, value.exponent_two()), (5_u8, value.exponent_five())] {
        let power = BigUint::from(base).pow(u32::from(exponent.unsigned_abs()));
        if exponent >= 0 {
            numerator *= power;
        } else {
            denominator *= power;
        }
    }
    if !truncate && &numerator % &denominator != BigUint::from(0_u8) {
        return None;
    }
    let digits = (numerator / denominator).to_u64_digits();
    if digits.len() > 1 {
        return None;
    }
    let magnitude = i128::from(digits.first().copied().unwrap_or(0));
    i64::try_from(if value.is_negative() {
        -magnitude
    } else {
        magnitude
    })
    .ok()
}

const TYPE_NAMES: &[(i64, &str)] = &[
    (-1, "minKey"),
    (1, "double"),
    (2, "string"),
    (3, "object"),
    (4, "array"),
    (5, "binData"),
    (6, "undefined"),
    (7, "objectId"),
    (8, "bool"),
    (9, "date"),
    (10, "null"),
    (11, "regex"),
    (12, "dbPointer"),
    (13, "javascript"),
    (14, "symbol"),
    (15, "javascriptWithScope"),
    (16, "int"),
    (17, "timestamp"),
    (18, "long"),
    (19, "decimal"),
    (127, "maxKey"),
];

fn types(
    operand: &BsonValue,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<Vec<&'static str>> {
    let values = match operand {
        BsonValue::Array(values) => values.as_slice(),
        _ => std::slice::from_ref(operand),
    };
    if values.is_empty() {
        return Err(query_error(9));
    }
    if values.len() > MAX_QUERY_NODES {
        return Err(limit());
    }
    let mut result = Vec::new();
    for value in values {
        check()?;
        let name = match value {
            BsonValue::String(name) if name == "number" => "number",
            BsonValue::String(name) => TYPE_NAMES
                .iter()
                .find(|(_, candidate)| *candidate == name)
                .map(|(_, name)| *name)
                .ok_or_else(|| query_error(2))?,
            value if value.canonical_number().is_some() => {
                let code = integer(value, false).ok_or_else(|| query_error(2))?;
                TYPE_NAMES
                    .iter()
                    .find(|(candidate, _)| *candidate == code)
                    .map(|(_, name)| *name)
                    .ok_or_else(|| query_error(2))?
            }
            _ => return Err(query_error(14)),
        };
        if !result.contains(&name) {
            result.push(name);
        }
    }
    Ok(result)
}

struct Work<'a> {
    steps: usize,
    check: &'a mut dyn MatchControl,
}

impl Work<'_> {
    fn step(&mut self) -> EngineResult<()> {
        self.check.step()?;
        self.steps += 1;
        if self.steps > MAX_MATCH_STEPS {
            return Err(limit());
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Root<'a> {
    Document(&'a BsonDocument),
    Value(&'a BsonValue),
}

#[derive(Clone, Copy)]
struct Candidate<'a> {
    value: Option<&'a BsonValue>,
    indexed: bool,
}

fn array_index(part: &str) -> Option<usize> {
    if part == "0"
        || (part
            .as_bytes()
            .first()
            .is_some_and(|byte| (b'1'..=b'9').contains(byte))
            && part.bytes().all(|byte| byte.is_ascii_digit()))
    {
        part.parse().ok()
    } else {
        None
    }
}

fn candidate<'a>(
    output: &mut Vec<Candidate<'a>>,
    value: Option<&'a BsonValue>,
    indexed: bool,
    work: &mut Work<'_>,
) -> EngineResult<()> {
    if output.len() == MAX_PATH_CANDIDATES {
        return Err(limit());
    }
    work.check.allocation(128)?;
    output.push(Candidate { value, indexed });
    Ok(())
}

fn resolve<'a>(
    root: Root<'a>,
    parts: &[String],
    output: &mut Vec<Candidate<'a>>,
    work: &mut Work<'_>,
) -> EngineResult<()> {
    work.step()?;
    let Some((part, remaining)) = parts.split_first() else {
        let Root::Value(value) = root else {
            return Err(query_error(2));
        };
        return candidate(output, Some(value), false, work);
    };
    match root {
        Root::Value(BsonValue::Document(document)) | Root::Document(document) => {
            // Check cancellation during wide-document lookup as well as recursion.
            let mut found = None;
            for (name, value) in document.iter() {
                work.step()?;
                if name == part {
                    found = Some(value);
                    break;
                }
            }
            match found {
                Some(value) => resolve(Root::Value(value), remaining, output, work),
                None => candidate(output, None, false, work),
            }
        }
        Root::Value(BsonValue::Array(values)) => {
            if let Some(index) = array_index(part) {
                if let Some(value) = values.get(index) {
                    if remaining.is_empty() {
                        candidate(output, Some(value), true, work)?;
                    } else if matches!(value, BsonValue::Document(_) | BsonValue::Array(_)) {
                        resolve(Root::Value(value), remaining, output, work)?;
                    }
                }
            }
            // Numeric path parts also address fields inside document members.
            // Do not recursively flatten raw nested arrays.
            for value in values {
                work.step()?;
                if let BsonValue::Document(document) = value {
                    resolve(Root::Document(document), parts, output, work)?;
                }
            }
            Ok(())
        }
        Root::Value(_) => candidate(output, None, false, work),
    }
}

impl DocumentMatcher {
    fn evaluate(
        &self,
        root: Root<'_>,
        exact_id: bool,
        array_document: bool,
        work: &mut Work<'_>,
    ) -> EngineResult<bool> {
        work.step()?;
        for clause in &self.clauses {
            work.step()?;
            let matches = match clause {
                Clause::Logical { kind, children } => {
                    let mut found = matches!(kind, Logical::And);
                    for child in children {
                        let matched = child.evaluate(root, exact_id, array_document, work)?;
                        match kind {
                            Logical::And if !matched => {
                                found = false;
                                break;
                            }
                            Logical::Or | Logical::Nor if matched => {
                                found = true;
                                break;
                            }
                            _ => {}
                        }
                    }
                    if matches!(kind, Logical::Nor) {
                        !found
                    } else {
                        found
                    }
                }
                Clause::Field {
                    path,
                    exact_id: field_id,
                    predicates,
                } => {
                    let mut candidates = Vec::new();
                    if array_document
                        && matches!(root, Root::Value(BsonValue::Array(_)))
                        && array_index(&path[0]).is_none()
                    {
                        candidate(&mut candidates, None, false, work)?;
                    } else {
                        resolve(root, path, &mut candidates, work)?;
                    }
                    // Positive predicates may match different path candidates.
                    // Negative predicates must hold across every candidate.
                    let mut matched = true;
                    for predicate in predicates {
                        let negative = predicate.negative();
                        let mut found = negative;
                        for candidate in &candidates {
                            let result = predicate.evaluate(
                                candidate.value,
                                exact_id && *field_id || candidate.indexed,
                                work,
                            )?;
                            if result != negative {
                                found = result;
                                break;
                            }
                        }
                        if !found {
                            matched = false;
                            break;
                        }
                    }
                    matched
                }
            };
            if !matches {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn equal(
    actual: Option<&BsonValue>,
    expected: &BsonValue,
    exact: bool,
    work: &mut Work<'_>,
) -> EngineResult<bool> {
    work.step()?;
    let Some(actual) = actual else {
        return Ok(matches!(expected, BsonValue::Null));
    };
    work.check.value(actual)?;
    work.check.value(expected)?;
    let matched = actual == expected;
    work.check.step()?;
    if matched {
        return Ok(true);
    }
    if !exact {
        if let BsonValue::Array(values) = actual {
            for value in values {
                work.step()?;
                work.check.value(value)?;
                work.check.value(expected)?;
                let matched = value == expected;
                work.check.step()?;
                if matched {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

fn scalar_or_members(
    actual: Option<&BsonValue>,
    exact: bool,
    include_array: bool,
    work: &mut Work<'_>,
    mut predicate: impl FnMut(Option<&BsonValue>, &mut Work<'_>) -> EngineResult<bool>,
) -> EngineResult<bool> {
    if !exact {
        if let Some(BsonValue::Array(values)) = actual {
            if include_array && predicate(actual, work)? {
                return Ok(true);
            }
            for value in values {
                work.step()?;
                if predicate(Some(value), work)? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
    }
    predicate(actual, work)
}

impl Predicate {
    fn negative(&self) -> bool {
        matches!(
            self,
            Self::NotEqual(_)
                | Self::In { negative: true, .. }
                | Self::Not(_)
                | Self::Exists(false)
        )
    }

    fn evaluate(
        &self,
        actual: Option<&BsonValue>,
        exact: bool,
        work: &mut Work<'_>,
    ) -> EngineResult<bool> {
        work.step()?;
        match self {
            Self::Equal(value) => equal(actual, value, exact, work),
            Self::NotEqual(value) => Ok(!equal(actual, value, exact, work)?),
            Self::Exists(exists) => Ok(actual.is_some() == *exists),
            Self::Range {
                operand,
                greater,
                inclusive,
            } => scalar_or_members(actual, exact, true, work, |value, work| {
                work.step()?;
                let value = value.unwrap_or(&BsonValue::Null);
                let crosses_types = matches!(operand, BsonValue::MinKey | BsonValue::MaxKey);
                if !crosses_types && value.type_rank() != operand.type_rank() {
                    return Ok(false);
                }
                if let (Some(left), Some(right)) =
                    (value.canonical_number(), operand.canonical_number())
                {
                    if matches!(left, CanonicalNumber::NaN) != matches!(right, CanonicalNumber::NaN)
                    {
                        return Ok(false);
                    }
                }
                work.check.value(value)?;
                work.check.value(operand)?;
                let comparison = value.cmp(operand);
                work.check.step()?;
                Ok(if comparison.is_eq() {
                    *inclusive
                } else if *greater {
                    comparison.is_gt()
                } else {
                    comparison.is_lt()
                })
            }),
            Self::In { members, negative } => {
                for member in members {
                    if member.evaluate(actual, exact, work)? {
                        return Ok(!negative);
                    }
                }
                Ok(*negative)
            }
            Self::All(members) => {
                if members.is_empty() {
                    return Ok(false);
                }
                for member in members {
                    let matched =
                        scalar_or_members(
                            actual,
                            exact,
                            false,
                            work,
                            |value, work| match member {
                                Member::Element(element) => element.evaluate(actual, work),
                                Member::Regex(regex) => regex.evaluate(value, exact, work),
                                _ => member.evaluate(value, true, work),
                            },
                        )?;
                    if !matched {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Self::Element(element) => element.evaluate(actual, work),
            Self::Not(predicates) => {
                for predicate in predicates {
                    if !predicate.evaluate(actual, exact, work)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Self::Regex(regex) => regex.evaluate(actual, exact, work),
            Self::Size(size) => {
                Ok(matches!(actual, Some(BsonValue::Array(values)) if values.len() == *size))
            }
            Self::Types(types) => scalar_or_members(actual, exact, true, work, |value, _work| {
                Ok(value.is_some_and(|value| types.iter().any(|name| type_matches(value, name))))
            }),
            Self::Mod(divisor, remainder) => {
                scalar_or_members(actual, exact, false, work, |value, _work| {
                    Ok(value
                        .and_then(|value| integer(value, true))
                        .is_some_and(|value| {
                            value.checked_rem(*divisor).unwrap_or(0) == *remainder
                        }))
                })
            }
        }
    }
}

impl Member {
    fn evaluate(
        &self,
        actual: Option<&BsonValue>,
        exact: bool,
        work: &mut Work<'_>,
    ) -> EngineResult<bool> {
        match self {
            Self::Literal(value) => equal(actual, value, exact, work),
            Self::Regex(regex) => regex.evaluate(actual, exact, work),
            Self::Element(element) => element.evaluate(actual, work),
        }
    }
}

impl Element {
    fn evaluate(&self, actual: Option<&BsonValue>, work: &mut Work<'_>) -> EngineResult<bool> {
        let Some(BsonValue::Array(values)) = actual else {
            return Ok(false);
        };
        for value in values {
            work.step()?;
            let matched = match self {
                Self::Fields(predicates) => {
                    let mut matched = true;
                    for predicate in predicates {
                        if !predicate.evaluate(Some(value), true, work)? {
                            matched = false;
                            break;
                        }
                    }
                    matched
                }
                Self::Document(matcher) => match value {
                    BsonValue::Document(document) => {
                        matcher.evaluate(Root::Document(document), false, false, work)?
                    }
                    BsonValue::Array(_) => {
                        matcher.evaluate(Root::Value(value), false, true, work)?
                    }
                    _ => false,
                },
            };
            if matched {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl CompiledRegex {
    fn evaluate(
        &self,
        actual: Option<&BsonValue>,
        exact: bool,
        work: &mut Work<'_>,
    ) -> EngineResult<bool> {
        scalar_or_members(actual, exact, false, work, |value, work| {
            work.step()?;
            if let Some(value) = value {
                work.check.value(value)?;
            }
            work.check.comparison_bytes(
                128 + self.identity.pattern().len() + self.identity.options().len(),
            )?;
            let result = match value {
                Some(BsonValue::RegularExpression(regex)) => Ok(regex == &self.identity),
                Some(BsonValue::String(value)) => match &self.expression {
                    Some(expression) => expression.is_match(value).map_err(|_| limit()),
                    None => Ok(false),
                },
                _ => Ok(false),
            };
            work.check.step()?;
            result
        })
    }
}

fn type_matches(value: &BsonValue, name: &str) -> bool {
    if name == "number" {
        return value.canonical_number().is_some();
    }
    let actual = match value {
        BsonValue::MinKey => "minKey",
        BsonValue::MaxKey => "maxKey",
        BsonValue::Null => "null",
        BsonValue::Double(_) => "double",
        BsonValue::Int32(_) => "int",
        // The locked TinyMongo query contract classifies integral values by
        // range; the stored BSON Int64 representation itself is unchanged.
        BsonValue::Int64(value) if i32::try_from(*value).is_ok() => "int",
        BsonValue::Int64(_) => "long",
        BsonValue::Decimal128(_) => "decimal",
        BsonValue::String(_) => "string",
        BsonValue::Document(_) => "object",
        BsonValue::Array(_) => "array",
        BsonValue::Binary(_) | BsonValue::Uuid(_) => "binData",
        BsonValue::ObjectId(_) => "objectId",
        BsonValue::Boolean(_) => "bool",
        BsonValue::DateTime(_) => "date",
        BsonValue::Timestamp(_) => "timestamp",
        BsonValue::RegularExpression(_) => "regex",
        BsonValue::JavaScript(code) if code.scope().is_some() => "javascriptWithScope",
        BsonValue::JavaScript(_) => "javascript",
    };
    actual == name
}

#[cfg(test)]
mod tests;
