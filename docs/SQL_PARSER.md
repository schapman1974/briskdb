# SQL parser decision record

Status: accepted for roadmap issue #19

BriskDB needs one syntax boundary that PostgreSQL, MySQL, HTTP, and future
frontends can share without making a protocol adapter understand SQLite or
invent its own routing rules. This record selects the parser dependency and
defines that boundary. The separate [common SQL subset contract](SQL_SUBSET.md)
defines the opt-in structural validator, and the [SQL parameter-normalization
contract](SQL_PARAMETERS.md) defines the opt-in dialect-specific placeholder
rewrite. None of these layers is in the current HTTP execution path.

## Decision

BriskDB uses the `sqlparser` package at version `0.62.0` from the exact
post-release upstream snapshot
[`33521a534eae72188e06ce0ce4836d19c7f3458a`](https://github.com/apache/datafusion-sqlparser-rs/commit/33521a534eae72188e06ce0ce4836d19c7f3458a).
That commit contains the required recursion-accounting correction as well as
other upstream work after the `v0.62.0` tag; BriskDB reviews and tests the pinned
snapshot as a whole rather than describing it as a one-change patch. The spike
selected the library because it is maintained, written in Rust, produces a
structured AST, and provides explicit SQLite, PostgreSQL, and MySQL dialect
implementations. The dependency is Apache-2.0 licensed; BriskDB continues to
use its own types and entry point so a later parser replacement does not require
protocol or engine APIs to expose `sqlparser` types.

Callers must select exactly one BriskDB dialect:

- SQLite
- PostgreSQL
- MySQL

There is no generic production dialect, dialect autodetection, or retry under a
different dialect after a parse error. Those behaviors can accept an ambiguous
statement differently depending on fallback order and would make protocol
behavior hard to reproduce. A PostgreSQL adapter will select PostgreSQL, a
MySQL adapter will select MySQL, and a strict SQLite surface will select
SQLite.

The SQL module retains the exact input text alongside an ordered parsed batch.
The upstream AST stays opaque outside BriskDB's SQL boundary. The implemented
common-subset validator and placeholder normalizer, and later planners, inspect
it through BriskDB-owned interfaces, but protocol adapters must not depend on
`sqlparser` AST types.

`sqlparser`'s formatter is not a source-preserving serializer: it can normalize
comments, whitespace, quoting, and keyword presentation. BriskDB therefore
never executes an AST's `Display` output and does not use that output as a
migration identity. Execution and byte-identity operations retain their exact
original SQL unless a later, documented translation produces separate SQL.

## Syntax-only contract

Successful parsing means only that the selected parser dialect recognizes the
input as syntax. It does not mean that BriskDB supports the statement, that the
statement has valid catalog references or types, that it can be routed, or that
executing it on SQLite would preserve the source dialect's semantics.

In particular, this layer does not:

- itself define or enforce the common SQL subset; issue #20 implements that as
  the separate `validate_common_subset(ParsedSql)` layer;
- normalize placeholders; issue #21 implements that as the separate
  `normalize_placeholders(CommonSql)` layer;
- infer shard keys or inspect bound values; issue #22 implements that as the
  separate `infer_shard_keys` layer;
- itself plan bound statements; issue #23 implements that as the separate
  synchronous `Engine::plan_bound_statement` layer;
- reject conflicting keys or unroutable writes (issue #24);
- translate types or syntax, or choose strict SQLite mode (issue #25);
- implement prepare/bind/describe/execute state or caches (issue #26); or
- classify statement behavior or reject unsafe multi-statement combinations
  (issue #27).

The parser may return several statements in source order. The 256-statement
limit below is a resource bound, not permission to execute a batch. Whether a
particular surface accepts one statement or a safe combination remains issue
#27. The implemented subset validator checks each ordered statement
independently and returns `Unsupported` for a parsed form outside its contract;
that still does not grant permission to execute an empty or mixed batch.

The existing HTTP execute, query, and migration paths remain raw SQLite
pass-through surfaces with their existing authorizer and endpoint-specific
rules. They call neither this parser, the opt-in common-subset validator, nor
the opt-in placeholder normalizer, shard-key inference, or bound statement
planner. Connecting those layers before translation, write policy, and
request-level statement policy are implemented would change the experimental
HTTP SQL surface.

## Resource and error boundaries

Parsing untrusted text is bounded before it can become planner or execution
work:

| Limit | Value | Classification |
| --- | ---: | --- |
| SQL input size | 65,536 UTF-8 bytes | `LimitExceeded` |
| Statements in one parsed batch | 256 | `LimitExceeded` |
| Parser recursion depth | 32 | `LimitExceeded` |

Empty, whitespace-only, and comment-only input successfully produces an empty
AST; whether a later request surface permits that is classification policy for
issue #27. NUL input, malformed tokens, incomplete syntax, and other parse
failures are `InvalidQuery`. A syntactically valid statement that is outside
BriskDB's common subset is not a parse failure; `validate_common_subset`
classifies it as `Unsupported`. Public protocol errors continue to use the fixed
safe text for the error kind and must not serialize SQL text, parser diagnostics,
source locations, or internal error chains.

The parser is stateless and has no catalog, storage, session, parameters,
filesystem, or routing access. Routing will consume structural AST information
through the later BriskDB planner. It must never search SQL text or formatted
AST text with regular expressions to infer a key or statement behavior.

## Dependency and MSRV policy

The registry `sqlparser` 0.62.0 source omits recursion accounting in
`parse_interval`: that path can recurse without consuming the configured
recursion budget. `Cargo.toml` therefore declares the exact upstream Git source
and revision containing the correction. The registry tag by itself is not the
selected or reviewed source. The direct source pin can be replaced only when
BriskDB deliberately upgrades to a release containing the correction and its
regression suite passes unchanged.

Git and path consumers inherit that direct source revision. Cargo normalizes a
Git dependency to its registry version when packaging a crate for crates.io, so
BriskDB sets `publish = false` while this temporary source pin is required. Do
not enable crates.io publishing until a registry release containing the
correction is selected, or an equivalently reviewed published dependency is
available.

BriskDB enables `sqlparser`'s recursive-protection support in addition to
setting the explicit depth limit. In the dependency graph selected by the
spike, the newest `psm` release admitted by `stacker` pulls in build
dependencies that require Rust 1.88. BriskDB supports Rust 1.85, so both
`Cargo.toml` and `Cargo.lock` constrain `psm` to `0.1.28`, whose dependency
graph builds on the declared MSRV. The direct constraint makes this behavior
carry into Git and path consumers instead of depending only on BriskDB's root
lockfile.

This compatibility constraint is intentional, not cleanup noise. Parser or
lockfile updates must run the complete locked test and documentation suite on
Rust 1.85 as well as stable, and must not raise the MSRV accidentally. Removing
recursive protection requires a separate parser-design review with deep-nesting
regression tests; merely retaining a numeric depth option is not sufficient
justification.

## Verification obligations

The parser boundary is covered independently of execution. Its unit tests must
include representative SQLite, PostgreSQL, and MySQL constructs; dialect-only
quoting and placeholders; strings and comments containing keywords or
semicolons; ordered multi-statement input; malformed and incomplete syntax;
and every exact resource boundary. Deeply nested and arbitrary valid UTF-8
input must return a classified result without panic, abort, or stack overflow.
The nested-`INTERVAL` regression that exercises the upstream 0.62.0 hole runs
in a subprocess so a future stack overflow or abort is observed as a failed
test instead of terminating the test harness. The child must exit normally
with the configured recursion-limit error.

Structural tests must assert AST meaning for tricky statements rather than
matching SQL substrings. Source review and dependency review must also confirm
that BriskDB has no regular-expression parser or routing helper. A transitive
regular-expression crate elsewhere in `Cargo.lock` is not evidence of SQL
routing; the architectural proof is that this layer returns structured syntax
and makes no routing decision.

This decision and the later opt-in subset validator change no HTTP request or
response shape, routing result, configuration, manifest schema, shard file, or
stored data.
