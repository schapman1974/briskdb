"""Owned loopback fixture: connector credentials and TLS remain independent."""

import asyncio
import json
import struct
import sys
import urllib.error
import urllib.request

import psycopg
import pymongo

from mongo_tls_client import sync_checks, async_checks

TOKEN = "composed_sqlite_test_token_0123456789"
PASSWORD = "composed-test-secret"


def remote_request(url, token, body=None):
    request = urllib.request.Request(
        url, data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return response.read()


if __name__ == "__main__":
    mongo_port, postgres_port, remote_url, certificate = sys.argv[1:]
    assert pymongo.version == "4.17.0"
    assert psycopg.__version__ == "3.2.13"
    args = dict(host="localhost", port=int(postgres_port), dbname="default",
                user="briskdb", password=PASSWORD, sslmode="verify-full",
                sslrootcert=certificate, channel_binding="require",
                autocommit=True, connect_timeout=5)
    with psycopg.connect(**args) as connection:
        assert connection.execute("SELECT 17").fetchone() == ("17",)
        sync_checks(f"mongodb://localhost:{mongo_port}/?directConnection=true", certificate)
        asyncio.run(async_checks(f"mongodb://localhost:{mongo_port}/?directConnection=true", certificate))
        try:
            remote_request(remote_url + "/sqlite/v1/catalog", PASSWORD)
        except urllib.error.HTTPError as error:
            assert error.code == 401
        else:
            raise AssertionError("PostgreSQL password authenticated SQLite remote")
        catalog = json.loads(remote_request(remote_url + "/sqlite/v1/catalog", TOKEN))
        assert catalog["scope"] == "legacy-shard"
        assert [table["name"] for table in catalog["tables"]] == ["users"]
        body = dict(instance=catalog["instance"], generation=catalog["generation"], table="users")
        frame = remote_request(remote_url + "/sqlite/v1/scan", TOKEN, body)
        assert struct.unpack("<4sIIBq", frame) == (b"BRS1", 1, 1, 1, 7)
        try:
            with psycopg.connect(**dict(args, password=TOKEN)):
                pass
        except psycopg.OperationalError:
            pass
        else:
            raise AssertionError("SQLite remote token authenticated PostgreSQL")
        assert connection.execute("SELECT 23").fetchone() == ("23",)
    print("Verified Mongo TLS, PostgreSQL SCRAM-PLUS and SQLite-remote composition passed")
