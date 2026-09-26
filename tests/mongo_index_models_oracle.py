"""Source-locked public IndexModel outcomes (test-only; never a runtime dependency).

Writes BSON events for the independently installed wheel's differential test.
The reference sources, planner, exceptions and returned metadata are unchanged.
"""

import hashlib
from itertools import product
from pathlib import Path
import sys
import warnings
from uuid import uuid4

from bson import BSON
import tinymongo.indexes as indexes
import tinymongo.tinymongo as reference
from tinymongo.storage_backends import clear_memory_namespace
from tinymongo.errors import TinyMongoNotSupportedError


def main():
    for module, digest in [
        (reference, "d372699407b46a7abefb5bb132d99d4645f3e7bfddc73213c1fe3fccbac476e0"),
        (indexes, "91001ac8d89a89eed65555ebe8196de8345735b15aeefeaa18851619e3519ca6"),
    ]:
        assert hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() == digest
    address = "memory://briskdb-models-" + uuid4().hex
    client = reference.TinyMongoClient(address, backend="memory")
    try:
        cases = product([1, -1, "hashed", "text"], [False, True], [False, True],
                        [{}, {"sparse": True}, {"partialFilterExpression": {"active": True}}],
                        [{}, {"background": True}, {"expireAfterSeconds": 0.5}])
        for number, (direction, unique, compound, membership, behavior) in enumerate(cases):
            keys = {"value": direction}
            if compound:
                keys["tail"] = 1
            base = {"key": {field: 1 for field in keys}, "name": "existing",
                    "unique": unique, **membership}
            model = {"key": keys, "name": "requested", "unique": unique, **membership, **behavior}
            collection = client.app[f"case_{number}"]
            collection.create_indexes([base])
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                try:
                    names = collection.create_indexes([model, model])
                    outcome = {"names": names}
                except TinyMongoNotSupportedError:
                    # TinyMongo's local exception has no wire code; compare the
                    # explicit unsupported category to BriskDB's code 115.
                    outcome = {"unsupported": True}
                except Exception as error:
                    code = getattr(error, "code", None)
                    assert isinstance(code, int), type(error)
                    outcome = {"error": code}
            # Key pairs are BSON array transport, not a metadata-order rewrite.
            event = {"base": base, "models": [model, model], "outcome": outcome,
                     "warning_count": len(caught), "indexes": collection.index_information()}
            sys.stdout.buffer.write(BSON.encode(event))
    finally:
        client.close()
        clear_memory_namespace(address)


if __name__ == "__main__":
    main()
