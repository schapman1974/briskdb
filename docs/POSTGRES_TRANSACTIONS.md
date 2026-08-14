# PostgreSQL transactions

BriskDB supports explicit `BEGIN`, `COMMIT`, and `ROLLBACK` through both the
PostgreSQL simple-query and extended-query flows. The state machine lives in
the protocol-neutral core; the wire adapter reports PostgreSQL `ReadyForQuery`
status `I` (idle), `T` (in transaction), or `E` (failed transaction).

## Single-shard contract

- `BEGIN` starts a logical transaction without choosing a shard.
- The first executable read or write must resolve to exactly one physical
  shard. BriskDB starts a SQLite transaction there, pins the session, and
  retains that exact pooled connection until the transaction ends.
- Later statements reuse the retained connection, so reads observe earlier
  uncommitted writes. A different-shard or multi-shard target is rejected
  before it can mutate data.
- `COMMIT` makes the shard-local SQLite transaction durable. `ROLLBACK`, client
  disconnect, session close, and forced engine shutdown roll it back.
- Any execution failure enters failed state. Later work returns PostgreSQL
  SQLSTATE `25P02` until `ROLLBACK`; `COMMIT` in failed state rolls back and
  returns a `ROLLBACK` command tag, matching PostgreSQL behavior.
- A matching PostgreSQL `CancelRequest` interrupts the active core request and
  returns SQLSTATE `57014`. The transaction becomes failed, preserves its pinned
  connection until cleanup, and can be recovered with `ROLLBACK`.

An open transaction occupies one connection from its pinned shard and blocks
application-schema migration until it ends. Pool queue limits, cancellation,
deadlines, connection retirement, and shutdown cleanup remain enforced.

## Deliberate limits

There is no transaction across shard files. Unconstrained sharded reads,
multi-shard reads, cross-shard writes, generated-key allocation, DDL,
savepoints, transaction modes, and isolation-level options are rejected inside
this initial explicit transaction boundary. Use predicates that identify one
registered shard key throughout the transaction.
