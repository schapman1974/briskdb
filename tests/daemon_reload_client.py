"""Rotate only the fixture files/PID of a daemon owned by the Rust test harness."""

import asyncio
from contextlib import ExitStack
import os
from pathlib import Path
import shutil
import signal
import sys
import time

import psycopg
import pymongo


FIXTURES = Path(__file__).parent / "fixtures/postgres-tls"
ORIGINAL = "daemon-original-test-secret"
ROTATED = "daemon-rotated-test-secret"
RESTORED = "daemon-restored-test-secret"


def identity(directory, rotated):
    stem = "rotated" if rotated else "server"
    shutil.copyfile(FIXTURES / (stem + ".crt"), directory / "server.crt")
    shutil.copyfile(FIXTURES / (stem + ".key"), directory / "server.key")
    os.chmod(directory / "server.key", 0o600)


def main():
    pid, log, pg_port, mongo_port, pg_files, mongo_files = sys.argv[1:]
    pid, log, pg_files, mongo_files = int(pid), Path(log), Path(pg_files), Path(mongo_files)
    has_mongo = mongo_port != "disabled"

    def pg(rotated, password):
        return psycopg.connect(host="localhost", port=int(pg_port), dbname="default", user="briskdb",
                               password=password, sslmode="verify-full", channel_binding="require",
                               sslrootcert=str(FIXTURES / ("rotated.crt" if rotated else "server.crt")),
                               connect_timeout=3, autocommit=True)

    def mongo_options(rotated):
        return dict(host=f"mongodb://localhost:{mongo_port}/?directConnection=true", tls=True,
                    tlsCAFile=str(FIXTURES / ("rotated.crt" if rotated else "server.crt")),
                    serverSelectionTimeoutMS=600, connectTimeoutMS=300, socketTimeoutMS=3000)

    def mongo(rotated):
        return pymongo.MongoClient(**mongo_options(rotated))

    def rotate(expect):
        before = log.read_text().count(expect)
        os.kill(pid, signal.SIGHUP)
        deadline = time.monotonic() + 10
        while log.read_text().count(expect) <= before:
            assert time.monotonic() < deadline, log.read_text()
            os.kill(pid, 0)
            time.sleep(0.01)

    def password(value):
        (pg_files / "password").write_text(value + "\n", encoding="utf-8")

    def verified(rotated, secret):
        with pg(rotated, secret) as sql:
            assert sql.execute("SELECT 59").fetchone() == ("59",)
        if has_mongo:
            with mongo(rotated) as client:
                assert client.reload_demo.items.count_documents({}) == 1

    async def verified_async():
        async with pymongo.AsyncMongoClient(**mongo_options(True)) as client:
            assert await client.reload_demo.items.count_documents({}) == 1

    with ExitStack() as stack:
        original_sql = stack.enter_context(pg(False, ORIGINAL))
        original_mongo = stack.enter_context(mongo(False)) if has_mongo else None
        if original_mongo is not None:
            original_mongo.reload_demo.items.insert_one({"_id": 1, "value": "preserved"})

        identity(pg_files, True)
        password(ROTATED)
        if has_mongo:
            identity(mongo_files, True)
            # PostgreSQL prepares successfully first; the subsequent Mongo
            # failure must prevent either identity being published.
            shutil.copyfile(FIXTURES / "server.key", mongo_files / "server.key")
        else:
            shutil.copyfile(FIXTURES / "server.key", pg_files / "server.key")
        rotate("listener security reload rejected")
        verified(False, ORIGINAL)

        identity(pg_files, True)
        if has_mongo:
            identity(mongo_files, True)
        rotate("listener security reloaded")
        verified(True, ROTATED)
        assert original_sql.execute("SELECT 61").fetchone() == ("61",)
        if original_mongo is not None:
            assert original_mongo.reload_demo.items.count_documents({}) == 1
            asyncio.run(verified_async())
        for trust, secret in [(False, ORIGINAL), (True, ORIGINAL)]:
            try:
                pg(trust, secret).close()
            except psycopg.OperationalError:
                pass
            else:
                raise AssertionError("fresh PostgreSQL client accepted old trust/password")
        if has_mongo:
            with mongo(False) as rejected:
                try:
                    rejected.admin.command("ping")
                except pymongo.errors.ServerSelectionTimeoutError:
                    pass
                else:
                    raise AssertionError("fresh Mongo client accepted old trust")

        # An invalid PostgreSQL password file must also preserve Mongo while
        # there is a valid but different Mongo replacement ready to load.
        password("")
        if has_mongo:
            identity(mongo_files, False)
        rotate("listener security reload rejected")
        verified(True, ROTATED)
        identity(pg_files, False)
        password(RESTORED)
        rotate("listener security reloaded")
        verified(False, RESTORED)
        assert original_sql.execute("SELECT 67").fetchone() == ("67",)

    text = log.read_text()
    assert all(secret not in text for secret in [ORIGINAL, ROTATED, RESTORED, "BEGIN PRIVATE KEY"])
    print("SIGHUP verified TLS/SCRAM rotation, invalid bundle retention, live sessions and recovery passed")


if __name__ == "__main__":
    main()
