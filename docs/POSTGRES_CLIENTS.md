# Tested PostgreSQL clients

BriskDB gates every pull request on a live PostgreSQL client matrix against an
imported two-shard database. The matrix proves the supported workflow for
these exact client lines; it is not a claim of general PostgreSQL compatibility.

| Client | Release-gating version | Exercised path |
| --- | --- | --- |
| `psql` | Ubuntu 24.04 distribution package | Simple-query transaction CRUD, error recovery, reconnect |
| tokio-postgres | 0.7.18 | Extended prepare/bind transaction CRUD, error recovery, reconnect |
| psycopg | 3.2.13 with binary package | Extended transaction CRUD, error recovery, reconnect |
| SQLAlchemy ORM | 2.0.43 with psycopg 3.2.13 | ORM-generated insert/select/update/delete, error recovery, reconnect |

All data work runs through the same protocol-neutral Engine, catalog, routing,
limits, and fixed-error boundary used by BriskDB's other frontends. The first
data statement pins each transaction to one physical shard. Query rows cross a
bounded 16-row Engine handoff; a slow client therefore stops SQLite stepping,
and PostgreSQL cancellation interrupts the exact leased SQLite handle.

## Bounded client adaptations

- Bare `START TRANSACTION`, emitted by tokio-postgres, maps to `BEGIN`.
  Transaction-mode clauses are still unsupported.
- SQLAlchemy must set `use_native_hstore=False`; its default hstore discovery
  uses an unsupported savepoint.
- A placeholder with an unparameterized `::VARCHAR` cast is accepted for
  SQLAlchemy-generated DML. Literal casts, other types, `VARCHAR(n)`, standard
  `CAST(...)`, and batches remain outside the supported SQL subset.
- Expression columns such as `SELECT 1` intentionally report conservative
  PostgreSQL `text`, so clients receive the string `"1"`.

DDL, savepoints, general `pg_catalog`, cross-shard transactions, `COPY`, roles,
and full PostgreSQL SQL behavior remain unsupported.

## Run the matrix

Install `psql` plus the pinned Python dependencies, then run:

```bash
python3 -m pip install -r tests/postgres_client_requirements.txt
bash tests/postgres_client_matrix.sh
```

`BRISKDB_POSTGRES_MATRIX_PYTHON` and `BRISKDB_POSTGRES_MATRIX_PSQL` can select
non-default executables. `BRISKDB_POSTGRES_MATRIX_PORT` and
`BRISKDB_HTTP_MATRIX_PORT` can move the temporary listeners. The script creates
and removes its own temporary imported database.
