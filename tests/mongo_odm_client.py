"""Locked real-ODM bodies; only client construction is replaced by stock PyMongo.

No application methods, models, assertions, driver commands, or replies are
rewritten. TinyMongo is a test-only source/ID-factory dependency, never the
candidate data-access implementation. A missing dependency or source mismatch
fails this gate rather than producing a skip.
"""
import hashlib
import importlib.metadata
import json
import sys
import types
from pathlib import Path

import pymongo
import tinymongo

LOCK = "53cbf44e98b8caa036163725d195fd29592e1cc0"
FIXTURES = (
    (
        "beanie",
        "tests/test_beanie_odm_integration.py",
        "fabba170fdaf8d4c885a0e153cd35db7e54021c673843ebe57e7b64e518ebe45",
        "test_beanie_initializes_and_runs_crud_without_application_shims",
    ),
    (
        "mongoengine",
        "tests/test_mongoengine_contract.py",
        "51d6774b3b4f28271e39dfbd2750ec48df25b7bbd7e1e8b3db148db95b40a2d2",
        "test_mongoengine_crud_contract",
    ),
)
VERSIONS = {"beanie": "2.1.0", "mongoengine": "0.29.3", "pymongo": "4.17.0"}


def validate_executions(executions):
    assert [item["name"] for item in executions] == ["beanie", "mongoengine"]
    assert all(item["outcome"] == "passed" for item in executions)


def load_fixture(root, fixture):
    name, path, digest, _ = fixture
    source = (root / path).read_bytes()
    assert hashlib.sha256(source).hexdigest() == digest, f"locked {name} fixture changed"
    module = types.ModuleType(f"briskdb_locked_{name}")
    module.__file__ = str(root / path)
    sys.modules[module.__name__] = module
    exec(compile(source, module.__file__, "exec"), module.__dict__)
    return module


def connection_factories(uri):
    def async_client(**kwargs):
        assert set(kwargs) == {"tinymongo_folder", "backend"}
        assert kwargs["backend"] == "sqlite"
        return pymongo.AsyncMongoClient(
            uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=10000
        )

    def sync_client(**kwargs):
        assert "tinymongo_folder" in kwargs
        kwargs.pop("tinymongo_folder")
        kwargs["host"] = uri
        kwargs["serverSelectionTimeoutMS"] = 3000
        kwargs["socketTimeoutMS"] = 10000
        return pymongo.MongoClient(**kwargs)

    return {
        "beanie": types.SimpleNamespace(AsyncMongoClient=async_client),
        "mongoengine": types.SimpleNamespace(
            MongoClient=sync_client, generate_id=tinymongo.generate_id
        ),
    }


def verify_persisted_application_state(uri, native_people):
    with pymongo.MongoClient(
        uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=10000
    ) as client:
        assert client.odmtest.native_people.count_documents({"name": "Grace"}) == native_people
        assert client.odmtest.person.count_documents({}) == 0
        assert client.interviewcue.beanie_interviews.count_documents({}) == 0
        indexes = client.interviewcue.beanie_interviews.index_information()
        assert indexes["guest_name_asc"] == {"key": [("guest_name", 1)]}


def run(uri, source_root, phase, report_path):
    assert phase in ("initial", "reopened")
    versions = {name: importlib.metadata.version(name) for name in VERSIONS}
    assert versions == VERSIONS, versions
    assert importlib.metadata.version("tinymongo") == "1.3.0"
    factories = connection_factories(uri)
    report = {
        "source_commit": LOCK,
        "target": "briskdb-real-pymongo",
        "phase": phase,
        "versions": versions,
        "executions": [],
    }
    try:
        if phase == "reopened":
            verify_persisted_application_state(uri, 1)
        for fixture in FIXTURES:
            name, path, digest, test = fixture
            entry = {"name": name, "source_path": path, "sha256": digest, "outcome": "failed"}
            report["executions"].append(entry)
            module = load_fixture(source_root, fixture)
            # This is the fixture's connection configuration boundary, not a
            # replacement ODM/collection or a modification of any test body.
            module.tinymongo = factories[name]
            getattr(module, test)(report_path.parent)
            entry["outcome"] = "passed"
        validate_executions(report["executions"])
        verify_persisted_application_state(uri, 1 if phase == "initial" else 2)
        report["persisted_state"] = "passed"
    finally:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Verified both unchanged ODM bodies and application state: {phase}.")


if __name__ == "__main__":
    assert len(sys.argv) == 5, "URI SOURCE_ROOT PHASE REPORT_PATH"
    run(sys.argv[1], Path(sys.argv[2]), sys.argv[3], Path(sys.argv[4]))
