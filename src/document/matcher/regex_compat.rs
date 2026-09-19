//! Normalize the oracle's Python regex conventions before bounded execution.
//! Work on the parsed tree so escapes, comments, classes, and scoped flags
//! cannot accidentally turn a literal into an anchor or an operator.

use std::fmt::Write;

use fancy_regex::{Assertion, Expr, LookAround, RegexBuilder};

use super::{EngineResult, MAX_QUERY_NODES, limit, query_error};

const WORD: &str = r"[\p{L}\p{N}_]";
const NON_WORD: &str = r"[^\p{L}\p{N}_]";
const SPACE: &str = r"[\p{White_Space}\x1c-\x1f]";
const NON_SPACE: &str = r"[^\p{White_Space}\x1c-\x1f]";

pub(super) fn translate(
    pattern: &str,
    flags: &str,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<String> {
    let flags: String = flags
        .chars()
        .filter(|flag| "imsx".contains(*flag))
        .collect();
    let source = if flags.is_empty() {
        pattern.to_owned()
    } else {
        format!("(?{flags}){pattern}")
    };
    let tree = Expr::parse_tree(&source).map_err(|_| query_error(51091))?;
    let mut output = String::new();
    render(&tree.expr, &mut output, &mut 0, check)?;
    Ok(output)
}

fn render(
    expr: &Expr,
    output: &mut String,
    nodes: &mut usize,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    check()?;
    *nodes += 1;
    if *nodes > MAX_QUERY_NODES || output.len() > 256 * 1024 {
        return Err(limit());
    }
    match expr {
        Expr::Empty => {}
        Expr::Any { .. } => expr.to_str(output, 0),
        Expr::Literal { val, casei } if *casei => {
            output.push_str("(?i:");
            for ch in val.chars() {
                if matches!(ch, 'i' | 'I' | 'İ' | 'ı') {
                    output.push_str("[iIİı]");
                } else {
                    Expr::Literal {
                        val: ch.to_string(),
                        casei: false,
                    }
                    .to_str(output, 0);
                }
            }
            output.push(')');
        }
        Expr::Literal { .. } => expr.to_str(output, 0),
        Expr::Assertion(assertion) => match assertion {
            Assertion::StartText => output.push_str(r"\A"),
            // Python '$' matches the absolute end or before ONE final LF.
            Assertion::EndText => output.push_str(r"(?=\n?\z)"),
            // In Python '\Z' is strict; fancy-regex's spelling is '\z'.
            Assertion::EndTextIgnoreTrailingNewlines { crlf: false } => output.push_str(r"\z"),
            Assertion::StartLine { crlf: false } => output.push_str("(?m:^)"),
            Assertion::EndLine { crlf: false } => output.push_str("(?m:$)"),
            Assertion::WordBoundary => {
                write!(output, "(?:(?<={WORD})(?!{WORD})|(?<!{WORD})(?={WORD}))").unwrap();
            }
            Assertion::NotWordBoundary => {
                write!(output, "(?:(?<={WORD})(?={WORD})|(?<!{WORD})(?!{WORD}))").unwrap();
            }
            _ => return Err(query_error(115)),
        },
        Expr::Concat(children) | Expr::Alt(children) => {
            output.push_str("(?:");
            for (index, child) in children.iter().enumerate() {
                if index != 0 && matches!(expr, Expr::Alt(_)) {
                    output.push('|');
                }
                render(child, output, nodes, check)?;
            }
            output.push(')');
        }
        Expr::Group(child) => {
            output.push('(');
            render(child, output, nodes, check)?;
            output.push(')');
        }
        Expr::LookAround(child, direction) => {
            output.push_str(match direction {
                LookAround::LookAhead => "(?=",
                LookAround::LookAheadNeg => "(?!",
                LookAround::LookBehind => "(?<=",
                LookAround::LookBehindNeg => "(?<!",
            });
            render(child, output, nodes, check)?;
            output.push(')');
        }
        Expr::Repeat {
            child,
            lo,
            hi,
            greedy,
        } => {
            output.push_str("(?:");
            render(child, output, nodes, check)?;
            output.push(')');
            if *hi == usize::MAX {
                write!(output, "{{{lo},}}").unwrap();
            } else {
                write!(output, "{{{lo},{hi}}}").unwrap();
            }
            if !greedy {
                output.push('?');
            }
        }
        Expr::Delegate { inner, casei } => {
            let inner = character_class(inner);
            if *casei {
                // Python's Unicode IGNORECASE additionally folds dotted and
                // dotless I. Test membership of the original single-character
                // atom, including negated/ranged classes, before extending it.
                let atom = RegexBuilder::new(&inner)
                    .case_insensitive(true)
                    .delegate_size_limit(256 * 1024)
                    .delegate_dfa_size_limit(256 * 1024)
                    .build()
                    .map_err(|_| query_error(51091))?;
                let membership: Vec<_> = ['i', 'İ', 'ı']
                    .into_iter()
                    .map(|ch| atom.is_match(&ch.to_string()).map_err(|_| limit()))
                    .collect::<EngineResult<_>>()?;
                let includes_i = if inner.starts_with("[^") {
                    membership.iter().all(|present| *present)
                } else {
                    membership.iter().any(|present| *present)
                };
                if includes_i {
                    write!(output, "(?i:{inner}|[iIİı])").unwrap();
                } else {
                    write!(output, "(?i:(?![iIİı]){inner})").unwrap();
                }
            } else {
                output.push_str(&inner);
            }
        }
        Expr::Backref { group, casei } => {
            // Group names were resolved to numbers by the first parse.
            write!(output, "(?{}:\\{group})", if *casei { "i" } else { "-i" }).unwrap();
        }
        Expr::AtomicGroup(child) => {
            output.push_str("(?>");
            render(child, output, nodes, check)?;
            output.push(')');
        }
        Expr::Conditional {
            condition,
            true_branch,
            false_branch,
        } => {
            let Expr::BackrefExistsCondition {
                group,
                relative_recursion_level: None,
            } = condition.as_ref()
            else {
                return Err(query_error(115));
            };
            write!(output, "(?({group})").unwrap();
            render(true_branch, output, nodes, check)?;
            output.push('|');
            render(false_branch, output, nodes, check)?;
            output.push(')');
        }
        // Engine-specific recursion, callouts and control verbs are not part
        // of the locked Python dialect and must not silently acquire meaning.
        _ => return Err(query_error(115)),
    }
    Ok(())
}

fn character_class(source: &str) -> String {
    let mut chars = source.chars();
    let mut output = String::new();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('w') => output.push_str(WORD),
                Some('W') => output.push_str(NON_WORD),
                Some('s') => output.push_str(SPACE),
                Some('S') => output.push_str(NON_SPACE),
                Some(escaped) => {
                    output.push('\\');
                    output.push(escaped);
                }
                None => output.push('\\'),
            }
        } else {
            output.push(ch);
        }
    }
    output
}
