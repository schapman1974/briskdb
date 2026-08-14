# Python changelog

## Unreleased

- Added host-controlled loopback HTTP and PostgreSQL listeners through
  synchronous and asyncio `Database.serve()` APIs.
- Listener shutdown is deterministic and database shutdown closes every
  attached server before stopping the shared engine.
- `Database.serve()` can configure PostgreSQL TLS and SCRAM-SHA-256 with
  certificate, private-key, identity, and password-file arguments; remote
  PostgreSQL binds require that secure mode.

## 0.1.0-alpha.5 — 2026-08-14

- Added installed-wheel `spawn` multiprocessing coverage for concurrent
  same-root writes, checkpoints, abrupt child exit, reopen, and retryable schema
  ownership contention.
- Documented the same-host/local-filesystem boundary and the prohibition on
  inherited post-`fork()` handles.

## 0.1.0-alpha.4 — 2026-08-13

- Added `cp39-abi3` wheel automation for macOS and manylinux on x86-64/ARM64.
- Added packaged type information, artifact audits, sdist testing, release
  checksums/provenance, and token-authenticated PyPI publishing gates.
- Documented the exact runtime/platform and pre-1.0 compatibility policy.

## 0.1.0-alpha.3 — 2026-08-13

- Added listener-free `Database`, `Session`, and configuration handles.
- Added SQL value conversion and the stable `BriskDBError` hierarchy.
- Added synchronous cursors/context managers and asyncio facades with native
  deadline and cancellation propagation.
- Native document operations remain unavailable pending the document engine.
