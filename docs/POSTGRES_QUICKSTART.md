# PostgreSQL query quickstart

BriskDB's disabled-by-default loopback PostgreSQL listener can execute one
simple-query protocol statement at a time against tables registered in its
logical catalog. This walkthrough creates a standard SQLite source, imports it
as a four-shard BriskDB data directory, starts the listener, and writes and
reads data with `psql`.

Install `sqlite3` and the PostgreSQL client, then use either the release
`briskdb` and `briskdb-import` binaries or prefix those commands with
`cargo run --bin ... --` from a source checkout.

```bash
demo_dir="$(mktemp -d)"

sqlite3 "$demo_dir/source.sqlite" <<'SQL'
CREATE TABLE records (
    tenant_id TEXT NOT NULL PRIMARY KEY,
    payload TEXT NOT NULL
);
SQL

cat >"$demo_dir/import-plan.json" <<'JSON'
{
  "version": 1,
  "tables": [
    {
      "name": "records",
      "placement": "sharded",
      "shard_key": {
        "strategy": "column",
        "column": "tenant_id",
        "key_type": "text"
      }
    }
  ]
}
JSON

briskdb-import \
  --source "$demo_dir/source.sqlite" \
  --data-dir "$demo_dir/briskdb-data" \
  --plan "$demo_dir/import-plan.json" \
  --shards 4

briskdb \
  --data-dir "$demo_dir/briskdb-data" \
  --shards 4 \
  --postgres-listen 127.0.0.1:5433
```

Leave the server running and connect from another terminal. There is currently
no credential verification or TLS, so the listener accepts only a numeric
loopback address. The `user` value is a bounded session label and `default` is
the imported logical database.

```bash
psql "host=127.0.0.1 port=5433 user=briskdb dbname=default sslmode=disable" \
  -c "INSERT INTO records (tenant_id, payload) VALUES ('tenant-a', 'hello')"

psql "host=127.0.0.1 port=5433 user=briskdb dbname=default sslmode=disable" \
  -c "SELECT tenant_id, payload FROM records WHERE tenant_id = 'tenant-a'"

psql "host=127.0.0.1 port=5433 user=briskdb dbname=default sslmode=disable" \
  -c "UPDATE records SET payload = 'updated' WHERE tenant_id = 'tenant-a'"

psql "host=127.0.0.1 port=5433 user=briskdb dbname=default sslmode=disable" \
  -c "DELETE FROM records WHERE tenant_id = 'tenant-a'"
```

For the Debian service, first stop `briskdb`, run the importer as the
`briskdb` system user into a destination below `/var/lib/briskdb` that does not
already exist, set that destination as `BRISKDB_DATA_DIR`, and set
`BRISKDB_POSTGRES_LISTEN=127.0.0.1:5433` in `/etc/default/briskdb`. The shard
count in the service configuration must match the import. Restart the service
and inspect it with `systemctl status briskdb` and
`journalctl -u briskdb.service`.

## Current query boundary

- The simple-query protocol accepts exactly one non-empty statement. `psql -c`
  uses this path.
- Registered-table `SELECT`, `INSERT`, `UPDATE`, and `DELETE` execute through
  the same catalog, routing, limits, and fixed-error boundary as the core
  engine.
- Simple-query results use PostgreSQL text format. Recognized SQLite declared
  types map to boolean, signed integer, numeric, double precision, text, or
  bytea OIDs; expressions and unrecognized declarations remain conservative
  PostgreSQL `text`. Stored non-UTF-8 text is rejected instead of being
  replaced.
- Sharded writes must contain enough literal shard-key information to select
  exactly one owner. Reads use the engine's bounded logical read path.
- Parameterized prepared queries support named and unnamed
  `Parse`/`Bind`/`Describe`/`Execute`, basic OIDs, mixed text/binary values and
  results, portal suspension, `Flush`, `Sync`, and cascading `Close`.
- DDL, transactions, `COPY`, authentication, authorization, TLS, and full
  PostgreSQL compatibility are not supported. Initialize the catalog offline
  with `briskdb-import` before starting the server.

The detailed wire and lifecycle contract is in
[`POSTGRES_LISTENER.md`](POSTGRES_LISTENER.md). The import's validation and
publication rules are in [`SQLITE_IMPORT.md`](SQLITE_IMPORT.md).
