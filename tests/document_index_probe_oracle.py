"""Prove candidate membership against unchanged, source-locked match/index helpers."""

import hashlib
import itertools
import sys
from datetime import datetime
from pathlib import Path

from bson import BSON, Binary, Code, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
import tinymongo.indexes as indexes
import tinymongo.table_backends as backend
from tinymongo.errors import TinyMongoNotSupportedError

from document_matcher_oracle import verify_source


def emit(keys, query, documents, *, sparse=False, partial=None, probe=True):
    envelope = BSON(BSON.encode(dict(keys=keys, query=query, documents=documents,
                                    sparse=sparse, partial=partial))).decode()
    spec = indexes.IndexSpec(keys=list(envelope["keys"].items()), sparse=sparse,
                             partial_filter=envelope["partial"])
    backend.validate_filter_operators(envelope["query"])
    valid, expected = [], []
    for document in envelope["documents"]:
        try:
            indexes.index_entry_tokens(document, spec)
        except TinyMongoNotSupportedError:
            continue  # Such a record cannot exist under a complete Ready index.
        valid.append(document)
        expected.append(backend.matches_filter(document, envelope["query"]))
    envelope.update(documents=valid, expected=expected, probe=probe)
    sys.stdout.buffer.write(BSON.encode(envelope))


def main():
    verify_source()
    assert hashlib.sha256(Path(indexes.__file__).read_bytes()).hexdigest() == (
        "91001ac8d89a89eed65555ebe8196de8345735b15aeefeaa18851619e3519ca6")
    scalars = [None, False, True, -1, 0, 1, Int64(1), Int64(2**63 - 1), 1.0,
               0.1, Decimal128("0.1"), Decimal128("1.00"), Decimal128("1E-6000"),
               "", "a", "private", Binary(b"a", 0), Binary(b"a", 128),
               Binary(bytes(16), 4), Binary(bytes(16), 3), Regex("a", "im"),
               Code("return x"), Code("return x", {"x": 1}),
               Code("return x", {"x": 1.0}), Timestamp(1, 2), MinKey(), MaxKey()]
    unsupported = [{}, {"x": 1}, [], [1, 2], ObjectId("64b000000000000000000001"),
                   datetime(2020, 1, 1), float("inf"), float("nan"), Decimal128("NaN")]
    documents = [{}] + [{"a": value} for value in scalars + unsupported]
    documents += [{"a": [left, right, left]} for left, right in itertools.product(scalars, repeat=2)]
    for value in scalars + unsupported:
        supported = not any(value is other for other in unsupported)
        query = {"a": {"$eq": value}}
        for sparse in [False, True]:
            eligible = supported and not (sparse and value is None)
            emit({"a": 1}, query, documents, sparse=sparse, probe=eligible)
            emit({"a": 1}, {"$and": [query, {"$or": [{"other": None}, {"other": 2}]}]},
                 documents, sparse=sparse, probe=eligible)
        emit({"a": 1}, {"$or": [query, {"missing": None}]}, documents, probe=False)
        emit({"a": 1}, {"$nor": [query]}, documents, probe=False)
        emit({"a": 1}, {"a": {"$not": {"$eq": value}}}, documents, probe=False)
    # Repeated equalities on a multikey field are not contradictions.
    for left, right in itertools.product(scalars, repeat=2):
        emit({"a": 1}, {"$and": [{"a": {"$eq": left}}, {"a": {"$eq": right}}]},
             [{"a": [left, right]}, {"a": left}, {"a": right}, {}])
    # Dotted object/numeric paths and complete sparse compound tuples.
    for path in ["a.x", "a.0", "a.01"]:
        leaf = path.split(".")[1]
        docs = [{}, {"a": None}, {"a": {}}, {"b": None}]
        docs += [{"a": {leaf: value}, "b": tail}
                 for value, tail in itertools.product([None, 1, 1.0, True, "a", [1, 2], []],
                                                      [None, 1, True, "a"])]
        docs += [{"a": {leaf: [None, 1]}, "b": "a"}, {"b": "a"}]
        for left, right in itertools.product([None, 1, True, "a"], repeat=2):
            for sparse in [False, True]:
                emit({path: 1, "b": 1}, {"$and": [{path: left}, {"b": right}]}, docs,
                     sparse=sparse, probe=not (sparse and left is None and right is None))
        emit({path: 1, "b": 1}, {path: 1}, docs, probe=False)
    for query in [{"a": 1}, {"a": 1, "enabled": True}]:
        emit({"a": 1}, query, [{"a": 1}, {"a": 1, "enabled": True}],
             partial={"enabled": True}, probe=False)
    for query in [{}, {"a": {"$gt": 0}}, {"a": {"$in": [1, 2]}},
                  {"a": {"$exists": True}}, {"a": {"$regex": "a"}}]:
        emit({"a": 1}, query, documents, probe=False)


if __name__ == "__main__":
    main()
