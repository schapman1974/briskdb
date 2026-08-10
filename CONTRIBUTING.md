# Contributing to BriskDB

BriskDB changes are developed one GitHub issue at a time. Create a focused
branch from `main`, keep the implementation and its verification together, and
merge only after the pull request is green.

## Testing policy

Automated tests are part of the implementation, not follow-up work.

- Every behavior-changing code contribution must add or update tests in the
  same pull request.
- Bug fixes must include a regression test that fails before the fix and passes
  afterward.
- Deterministic internal behavior belongs in unit tests close to the module.
- SQLite, HTTP, protocol, filesystem, and process boundaries need integration
  tests in addition to focused unit tests.
- Routing encodings, wire messages, type mappings, and error mappings need
  stable golden vectors where an upgrade could silently change behavior.
- Parser, router, and merge invariants should gain property tests when examples
  alone do not cover the state space.
- Crash and recovery claims require failure-injection tests before they are
  documented as guarantees.
- Tests must be deterministic, isolated, and safe to run concurrently. Use
  temporary directories and ephemeral ports instead of shared developer state.
- Do not delete, ignore, or weaken a test merely to make a change pass. Explain
  intentional contract changes and update the relevant compatibility document.

Pure documentation changes do not need artificial unit tests. They still need
link, example, and consistency validation. A pull request with no new automated
test must explain why the change cannot affect executable behavior.

## Required local checks

The crate declares Rust 1.85 as its minimum supported Rust version (MSRV).
Behavior-changing code must compile and pass tests on both the MSRV and current
stable Rust. The MSRV may only be raised deliberately, with release notes and a
CI update in the same pull request.

Run these before publishing a branch:

Admin-browser script checks require Node.js 18 or newer. The all-target Rust
suite invokes the same script checks so both CI Rust lanes exercise them.

```bash
cargo fmt --all --check
node --check src/protocol/http/admin/logic.js
node --check src/protocol/http/admin/app.js
node --test tests/admin_browser_logic.test.js
cargo test --locked --all-targets --all-features
cargo test --locked --doc --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
```

When Rust 1.85 is installed through `rustup`, also run:

```bash
cargo +1.85.0 test --locked --all-targets --all-features
cargo +1.85.0 test --locked --doc --all-features
```

Run targeted integration, compatibility, benchmark, or failure suites when the
change touches those areas. The pull request must list every check performed.

Storage-path changes must also run the benchmark correctness tests and smoke
mode before publishing:

```bash
cargo test --locked --test benchmark_workloads
cargo test --locked --bench storage
```

Run `cargo bench --locked --bench storage` on a quiet machine when collecting
timings. Performance comparisons are meaningful only on the same machine and
filesystem; see [the benchmark contract](docs/BENCHMARKS.md).

## Pull request loop

1. Confirm the issue scope and acceptance criteria.
2. Branch from the latest `main` using `agent/<short-description>`.
3. Implement the smallest complete vertical slice.
4. Add unit tests and the appropriate boundary tests.
5. Update public documentation and compatibility claims.
6. Run all relevant checks and inspect the final diff.
7. Open a pull request that closes exactly the intended issue.
8. Merge only after required checks pass, then verify the merge on `main` before
   starting the next issue.
