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

Run these before publishing a branch:

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

Run targeted integration, compatibility, benchmark, or failure suites when the
change touches those areas. The pull request must list every check performed.

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
