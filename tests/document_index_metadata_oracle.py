"""Built index discovery from the unchanged source-locked TinyMongo client.

Only the transport representation of ordered key pairs becomes a BSON document;
names, option presence, values and ordering are the reference's actual output.
"""
import hashlib
import sys
from pathlib import Path
from uuid import uuid4

from bson import BSON
import tinymongo.indexes as indexes
import tinymongo.tinymongo as reference
from tinymongo.storage_backends import clear_memory_namespace


def main():
    for module, digest in [
        (reference, "d372699407b46a7abefb5bb132d99d4645f3e7bfddc73213c1fe3fccbac476e0"),
        (indexes, "91001ac8d89a89eed65555ebe8196de8345735b15aeefeaa18851619e3519ca6"),
    ]:
        assert hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() == digest
    address = "memory://briskdb-index-metadata-" + uuid4().hex
    client = reference.TinyMongoClient(address, backend="memory")
    try:
        collection = client.oracle.indexes
        events = [
            {"action": "create", "name": "z", "keys": {"value": 1}},
            {"action": "create", "name": "!before_id", "keys": {"nested.value": 1, "tail": 1}, "sparse": True},
            {"action": "create", "name": "partial", "keys": {"other": 1}, "partial": {"active": True}},
            {"action": "create", "name": "é值", "keys": {"unicode.值": 1}},
            {"action": "drop", "name": "z"},
            {"action": "create", "name": "z", "keys": {"value": 1}},
        ]
        for event in events:
            if event["action"] == "drop":
                collection.drop_index(event["name"])
            else:
                options = {"name": event["name"]}
                if event.get("sparse"):
                    options["sparse"] = True
                if event.get("partial") is not None:
                    options["partialFilterExpression"] = event["partial"]
                assert collection.create_index(list(event["keys"].items()), **options) == event["name"]
            rows = collection.list_indexes()
            information = collection.index_information()
            assert information == {row["name"]: {key: value for key, value in row.items() if key != "name"} for row in rows}
            event["expected"] = [{key: dict(value) if key == "key" else value for key, value in row.items()} for row in rows]
            sys.stdout.buffer.write(BSON.encode(event))
    finally:
        client.close()
        clear_memory_namespace(address)


if __name__ == "__main__":
    main()
