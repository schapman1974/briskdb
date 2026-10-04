"""Python surface tests; native concurrency/failure tests live in s3_overlay."""
import json
import math
import os

import pytest
from briskdb import s3_overlay as overlay


@pytest.mark.parametrize("value", [None, True, -(2**63), 2**63-1, 1.25, "Unicode α", b"\x00\xff"])
def test_lossless_values(value):
    assert overlay._value(overlay._cell(value)) == value


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, object(), {}])
def test_reject_unsupported_values(value):
    with pytest.raises(TypeError):
        overlay._cell(value)


def test_integer_bounds():
    with pytest.raises(OverflowError):
        overlay._cell(2**63)


def test_public_surface_and_close(monkeypatch, tmp_path):
    calls = []
    class Native:
        def __init__(self, path, options="{}"):
            calls.append(("open", path, json.loads(options)))
        @staticmethod
        def create(path, config, seed, options="{}"):
            calls.append(("create", json.loads(config), json.loads(seed)))
            return Native(path, options)
        def query(self, sql, params):
            calls.append(("query", sql, json.loads(params)))
            return json.dumps({"columns":["id"],"rows":[[{"Text":"one"}]]})
        def execute(self, sql, params):
            calls.append(("execute", sql, json.loads(params)))
            return json.dumps({"affected_rows":1,"commit_id":"receipt"})
        def compact(self, table, partition):
            calls.append(("compact", table, partition))
            return "[]"
        def set_parquet_pruning(self, enabled):
            calls.append(("pruning", enabled))
        def read_stats(self):
            return '{"parquet_files_skipped":31,"sqlite_base_opens":1,"sqlite_base_cache_hits":2,"sqlite_base_cache_evictions":0}'
        def open_stats(self):
            return "null"
        def settings(self):
            return '{"storage_mode":"s3-overlay","options":{"read_only":false}}'
        def close(self):
            calls.append(("close",))
    monkeypatch.setattr(overlay, "_native", lambda: Native)
    with overlay.Database.create(tmp_path/"new", bucket="private", region="us-east-1",
                                prefix="example/", tables=[], seed={"items":[["one",b"x"]]}) as db:
        assert db.query("SELECT id FROM items WHERE id=?", ["one"]).rows == [("one",)]
        assert db.execute("INSERT", ["two"])["affected_rows"] == 1
        assert db.compact("items",3) == []
        db.set_parquet_pruning(False)
        assert ("pruning", False) in calls
        assert db.read_stats()["parquet_files_skipped"] == 31
        assert db.read_stats()["sqlite_base_cache_hits"] == 2
        assert db.open_stats() is None
        assert db.settings()["storage_mode"] == "s3-overlay"
        with pytest.raises(TypeError):
            db.set_parquet_pruning("false")
    assert calls[0][1]["prefix"] == "example"
    assert calls[0][2] == {"items":[[{"Text":"one"},{"Blob":[120]}]]}
    assert calls[-1] == ("close",)


def test_open_stats_decodes_timings_and_catalog_counts(monkeypatch, tmp_path):
    stats = {"total_ms": 2.5, "catalog_ms": 1.2, "catalog_root_reads": 1,
             "catalog_lock_requests": 1}

    class Native:
        def __init__(self, path, options="{}"):
            self.closed = False
        def open_stats(self):
            if self.closed:
                raise RuntimeError("S3 overlay is closed")
            return json.dumps(stats)
        def close(self):
            self.closed = True

    monkeypatch.setattr(overlay, "_native", lambda: Native)
    with overlay.Database(tmp_path) as db:
        assert db.open_stats() == stats
    with pytest.raises(RuntimeError, match="closed"):
        db.open_stats()


def test_duckdb_opt_in_arguments(monkeypatch, tmp_path):
    calls = []
    class Native:
        def __init__(self, _path, _options="{}"):
            pass
        def query_duckdb(self, table, key, sql, params, options):
            calls.append((table, json.loads(key), sql, json.loads(params), json.loads(options)))
            return json.dumps({"columns": ["value"], "rows": [[{"Blob": [0, 255]}]]})
        def close(self):
            pass
    monkeypatch.setattr(overlay, "_native", lambda: Native)
    with overlay.Database(tmp_path) as db:
        result = db.query_partition_duckdb("items", "one", "SELECT value FROM items WHERE id=?",
                                          ["one"], library="/trusted/libduckdb.so",
                                          sqlite_extension="/trusted/sqlite_scanner.duckdb_extension", threads=4)
    assert result.rows == [(b"\x00\xff",)]
    assert calls[0][0:2] == ("items", {"Text": "one"})
    assert calls[0][3] == [{"Text": "one"}]
    assert calls[0][4]["threads"] == 4
    assert calls[0][4]["memory_mb"] == 256


def test_duckdb_does_not_silently_fallback(monkeypatch, tmp_path):
    class Native:
        def __init__(self, _path, _options="{}"):
            pass
        def close(self):
            pass
    monkeypatch.setattr(overlay, "_native", lambda: Native)
    with overlay.Database(tmp_path) as db:
        with pytest.raises(Exception, match="duckdb-reader"):
            db.query_partition_duckdb("items", "one", "SELECT * FROM items", library="/lib",
                                      sqlite_extension="/extension")


def test_open_flags_are_strict_and_environment_opt_in_is_explicit(monkeypatch):
    calls = []
    class Native:
        def __init__(self, path, options):
            calls.append((path, json.loads(options)))
        def close(self):
            pass
    monkeypatch.setattr(overlay, "_native", lambda: Native)
    assert overlay.OpenOptions() == overlay.OpenOptions(True, False)
    for kwargs in ({"read_only": 1}, {"parquet_pruning": "false"}):
        with pytest.raises(TypeError):
            overlay.OpenOptions(**kwargs)
    for flags in ({}, {"BRISKDB_STORAGE_MODE": "sqlite", "BRISKDB_OVERLAY_ROOT": "/missing"},
                  {"BRISKDB_STORAGE_MODE": "s3-overlay"}):
        with pytest.raises(ValueError):
            overlay.Database.from_env(flags)
    assert not calls
    env = {"BRISKDB_STORAGE_MODE": "s3-overlay", "BRISKDB_OVERLAY_ROOT": "/configured",
           "BRISKDB_OVERLAY_PARQUET_PRUNING": "false", "BRISKDB_OVERLAY_READ_ONLY": "true"}
    with overlay.Database.from_env(env):
        pass
    assert calls == [("/configured", {"parquet_pruning": False, "read_only": True})]
    for value in ("", "maybe", "0", "False"):
        with pytest.raises(ValueError):
            overlay.OpenOptions.from_env({"BRISKDB_OVERLAY_READ_ONLY": value})
    with pytest.raises(TypeError):
        overlay.Database("/missing", options={"read_only": True})
    with pytest.raises(ValueError, match="read-only"):
        overlay.Database.create("/missing", bucket="bucket", region="region", prefix="prefix", tables=[],
                                options=overlay.OpenOptions(read_only=True))
    assert len(calls) == 1


def test_briskdb_public_storage_mode_dispatch_preserves_normal_defaults(monkeypatch):
    import briskdb
    from briskdb import api
    normal = []
    selected = []
    marker = object()
    monkeypatch.setenv("BRISKDB_STORAGE_MODE", "s3-overlay")
    monkeypatch.setenv("BRISKDB_OVERLAY_READ_ONLY", "true")
    monkeypatch.setattr(api, "_native_open", lambda path, **kw: (normal.append((path, kw)) or marker))
    monkeypatch.setattr(overlay, "Database", lambda path, **kw: (selected.append((path, kw)) or marker))
    assert briskdb.open("normal") is marker
    assert briskdb.connect("normal", shards=2) is marker
    assert len(normal) == 2 and not selected
    assert normal[0][1] == dict(shards=None, documents=False, uuid_representation=None, config=None)
    flags = overlay.OpenOptions(read_only=True)
    assert briskdb.open("overlay", storage_mode="s3-overlay", overlay_options=flags) is marker
    assert selected == [("overlay", {"options": flags})]
    for kwargs in ({"storage_mode": "bogus"}, {"overlay_options": flags},
                   {"storage_mode": "s3-overlay", "documents": True},
                   {"storage_mode": "s3-overlay", "shards": 2},
                   {"storage_mode": "s3-overlay", "config": object()},
                   {"storage_mode": "s3-overlay", "uuid_representation": "standard"}):
        with pytest.raises(ValueError):
            briskdb.open("not-created", **kwargs)
    assert len(normal) == 2 and len(selected) == 1


def test_native_options_validate_before_touching_storage(tmp_path):
    import briskdb
    from briskdb import _briskdb
    native = getattr(_briskdb, "S3OverlayDatabase", None)
    if native is None:
        pytest.skip("requires a source wheel built with s3-overlay")
    root = tmp_path / "never-created"
    for options in ('{"unknown":true}', '{"read_only":"false"}', '{"parquet_pruning":0}'):
        with pytest.raises(briskdb.InvalidArgumentError):
            native(str(root), options)
        assert not root.exists()
    with pytest.raises(briskdb.InvalidArgumentError):
        native.create(str(root), "{}", "{}", '{"read_only":true}')
    assert not root.exists()


def test_native_read_only_existing_fixture_without_cloud_io():
    """Optional fixture from the local qualification runner; no table/S3 reads."""
    import briskdb
    root = os.environ.get("BRISKDB_TEST_OVERLAY_ROOT")
    if root is None:
        pytest.skip("set BRISKDB_TEST_OVERLAY_ROOT to a disposable overlay catalog")
    with briskdb.open(root, storage_mode="s3-overlay", overlay_options=overlay.OpenOptions(
        parquet_pruning=False, read_only=True,
    )) as db:
        assert db.settings()["options"] == {"parquet_pruning": False, "read_only": True}
        stats = db.open_stats()
        assert stats["catalog_root_reads"] == stats["catalog_lock_requests"] == 1
        assert stats["total_ms"] >= stats["catalog_ms"] >= 0
        assert db.query("SELECT ? AS value", [42]).rows == [(42,)]
        assert db.read_stats()["sqlite_base_opens"] == 0
        for operation in (lambda: db.execute("DELETE FROM items"), db.compact,
                          lambda: db.compact("items", 0)):
            with pytest.raises(briskdb.ReadOnlyError):
                operation()
        db.set_parquet_pruning(True)
        assert db.settings()["options"] == {"parquet_pruning": True, "read_only": True}
    with pytest.raises(briskdb.FailedPreconditionError):
        db.settings()
    with pytest.raises(briskdb.FailedPreconditionError):
        db.open_stats()
    with overlay.Database.from_env({"BRISKDB_STORAGE_MODE": "s3-overlay", "BRISKDB_OVERLAY_ROOT": root}) as reopened:
        assert reopened.settings()["options"] == {"parquet_pruning": True, "read_only": False}
        assert reopened.query("SELECT 7").rows == [(7,)]


def test_missing_feature_never_falls_back_to_normal_storage(monkeypatch):
    import briskdb
    def unsupported():
        raise briskdb.UnsupportedError("build with s3-overlay")
    monkeypatch.setattr(overlay, "_native", unsupported)
    with pytest.raises(briskdb.UnsupportedError, match="s3-overlay"):
        briskdb.open("not-created", storage_mode="s3-overlay")
