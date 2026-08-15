#!/usr/bin/env bash

set -euo pipefail

python_command="${BRISKDB_POSTGRES_MATRIX_PYTHON:-python3}"
psql_command="${BRISKDB_POSTGRES_MATRIX_PSQL:-psql}"

for required_command in cargo curl "${psql_command}" "${python_command}"; do
  command -v "${required_command}" >/dev/null
done

matrix_root="$(mktemp -d)"
server_pid=""

cleanup() {
  if [[ -n "${server_pid}" ]] && kill -0 "${server_pid}" 2>/dev/null; then
    kill -INT "${server_pid}"
    wait "${server_pid}" || true
  fi
  rm -rf "${matrix_root}"
}
trap cleanup EXIT

postgres_port="${BRISKDB_POSTGRES_MATRIX_PORT:-55438}"
http_port="${BRISKDB_HTTP_MATRIX_PORT:-17654}"
data_dir="${matrix_root}/data"
source_db="${matrix_root}/source.sqlite"
plan="${matrix_root}/import-plan.json"
server_log="${matrix_root}/server.log"

"${python_command}" - "${source_db}" <<'PY'
import sqlite3
import sys

with sqlite3.connect(sys.argv[1]) as connection:
    connection.execute(
        "CREATE TABLE records ("
        "tenant_id TEXT NOT NULL PRIMARY KEY, "
        "payload TEXT NOT NULL)"
    )
    connection.execute(
        "CREATE TABLE indexed_records ("
        "tenant_id TEXT NOT NULL PRIMARY KEY, "
        "payload TEXT NOT NULL)"
    )
PY

"${python_command}" - "${plan}" <<'PY'
import json
import sys

plan = {
    "version": 1,
    "tables": [
        {
            "name": "records",
            "placement": "sharded",
            "shard_key": {
                "strategy": "column",
                "column": "tenant_id",
                "key_type": "text",
            },
        },
        {
            "name": "indexed_records",
            "placement": "sharded",
            "shard_key": {
                "strategy": "column",
                "column": "tenant_id",
                "key_type": "text",
            },
        },
    ],
}
with open(sys.argv[1], "w", encoding="utf-8") as output:
    json.dump(plan, output)
PY

cargo build --locked --bins --test postgres_client_matrix
target/debug/briskdb-import \
  --source "${source_db}" \
  --data-dir "${data_dir}" \
  --plan "${plan}" \
  --shards 2

BRISKDB_POSTGRES_MATRIX_ROOT="${data_dir}" \
  cargo test --locked --test postgres_client_matrix \
  prepare_postgres_client_global_index -- --exact --ignored

target/debug/briskdb \
  --data-dir "${data_dir}" \
  --shards 2 \
  --listen "127.0.0.1:${http_port}" \
  --postgres-listen "127.0.0.1:${postgres_port}" \
  >"${server_log}" 2>&1 &
server_pid="$!"

for _ in {1..100}; do
  if curl --fail --silent "http://127.0.0.1:${http_port}/health" >/dev/null; then
    break
  fi
  if ! kill -0 "${server_pid}" 2>/dev/null; then
    sed -n '1,200p' "${server_log}" >&2
    exit 1
  fi
  sleep 0.1
done
curl --fail --silent "http://127.0.0.1:${http_port}/health" >/dev/null

matrix_dsn="host=127.0.0.1 port=${postgres_port} user=briskdb dbname=default sslmode=disable"
matrix_url="postgresql+psycopg://briskdb@127.0.0.1:${postgres_port}/default"

"${psql_command}" "${matrix_dsn}" -X --set ON_ERROR_STOP=1 --command \
  "INSERT INTO indexed_records (tenant_id, payload) VALUES ('psql-index-a', 'psql-global-key')" \
  >/dev/null
if "${psql_command}" "${matrix_dsn}" -X --set ON_ERROR_STOP=1 --command \
  "INSERT INTO indexed_records (tenant_id, payload) VALUES ('psql-index-b', 'psql-global-key')" \
  >/dev/null 2>&1; then
  exit 1
fi
indexed_recovered="$("${psql_command}" "${matrix_dsn}" -X --tuples-only --no-align --command "SELECT 1")"
test "${indexed_recovered}" = "1"

"${psql_command}" "${matrix_dsn}" -X --set ON_ERROR_STOP=1 >/dev/null <<'SQL'
BEGIN;
INSERT INTO records (tenant_id, payload) VALUES ('psql-client', 'created');
SELECT payload FROM records WHERE tenant_id = 'psql-client';
UPDATE records SET payload = 'updated' WHERE tenant_id = 'psql-client';
SELECT payload FROM records WHERE tenant_id = 'psql-client';
DELETE FROM records WHERE tenant_id = 'psql-client';
COMMIT;
SQL

remaining="$("${psql_command}" "${matrix_dsn}" -X --tuples-only --no-align --command \
  "SELECT tenant_id FROM records WHERE tenant_id = 'psql-client'")"
test -z "${remaining}"
if "${psql_command}" "${matrix_dsn}" -X --command "SHOW work_mem" >/dev/null 2>&1; then
  exit 1
fi
recovered="$("${psql_command}" "${matrix_dsn}" -X --tuples-only --no-align --command "SELECT 1")"
test "${recovered}" = "1"

BRISKDB_POSTGRES_MATRIX_DSN="${matrix_dsn}" \
  cargo test --locked --test postgres_client_matrix \
  tokio_postgres_client_matrix -- --exact --ignored

BRISKDB_POSTGRES_MATRIX_DSN="${matrix_dsn}" \
BRISKDB_POSTGRES_MATRIX_URL="${matrix_url}" \
  "${python_command}" tests/postgres_client_matrix.py --verbose
