"""Valid ascending integer-key declarations from the unchanged frozen index model.

Native descending directions, numeric direction normalization, and resource/error
boundaries have independent tests; no frozen output is rewritten to cover them.
"""
import hashlib
import sys
from pathlib import Path

from bson import BSON
import tinymongo.indexes as reference


def main():
    assert hashlib.sha256(Path(reference.__file__).read_bytes()).hexdigest() == "91001ac8d89a89eed65555ebe8196de8345735b15aeefeaa18851619e3519ca6"
    for case in range(64):
        fields = [f"f{case}.{part}" for part in ("a", "nested.b", "0", "é", "值", "under_score", "z", "tail")[:1 + case % 8]]
        keys = {field: 1 for field in fields}
        name = f"custom_{case}" if case % 2 else None
        spec = reference.IndexSpec(keys=list(keys.items()), name=name, unique=bool(case % 3))
        sys.stdout.buffer.write(BSON.encode({
            "keys": keys, "name": name, "unique": spec.unique,
            "expected_name": spec.name, "expected_keys": dict(spec.keys),
        }))


if __name__ == "__main__":
    main()
