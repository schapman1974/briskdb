"""Stock-driver read-option compatibility, also run by the full wire gate."""

import asyncio
import sys

import pymongo
from pymongo.errors import OperationFailure
from pymongo.read_concern import ReadConcern


WARNING = "hint: accepted for TinyMongo compatibility; index selection remains automatic"
COMMENT = {"private-comment": ["must-not-leak", 42]}
DOCUMENTS = [{"_id": i, "v": i % 3, "tag": "T" if i % 2 == 0 else "t"} for i in range(6)]


def sync_read_options(uri, reopened):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        database = client.wire_read_options
        collection = database.items
        if not reopened:
            collection.insert_many(DOCUMENTS)
            collection.create_index("v")
        before_indexes = list(collection.list_indexes())
        for hint in ("v_1", "missing-private-index", {"v": -1}):
            cursor = collection.find({}, hint=hint, comment=COMMENT, allow_disk_use=False,
                                     return_key=False, show_record_id=False,
                                     collation={"locale": "simple"}, let={})
            assert [row["_id"] for row in cursor.sort("_id", -1).skip(1).limit(3).batch_size(1)] == [4, 3, 2]
        chained = collection.find({}).hint("missing-private-index").comment(COMMENT).sort("_id").batch_size(1)
        assert list(chained) == DOCUMENTS
        assert collection.find_one({"_id": 2}, hint="missing-private-index", comment=COMMENT) == DOCUMENTS[2]
        assert collection.count_documents({"v": {"$gte": 1}}, hint="missing-private-index", comment=COMMENT) == 4
        assert collection.estimated_document_count(comment=COMMENT) == 6
        assert collection.distinct("v", hint={"v": -1}, comment=COMMENT) == [0, 1, 2]
        local = collection.with_options(read_concern=ReadConcern("local"))
        assert list(local.find({"tag": "t"}, collation={"locale": "simple"}).sort("_id")) == DOCUMENTS[1::2]
        assert list(local.aggregate([{"$match": {"tag": "t"}}, {"$sort": {"_id": 1}}],
                                    hint="missing-private-index", comment=COMMENT, let={},
                                    collation={"locale": "simple"}, allowDiskUse=False)) == DOCUMENTS[1::2]
        assert list(collection.list_indexes()) == before_indexes

        for name, options, result_field in [
            ("find", {"filter": {}, "batchSize": 0}, "cursor"),
            ("aggregate", {"pipeline": [], "cursor": {"batchSize": 0}}, "cursor"),
            ("count", {}, "n"),
            ("distinct", {"key": "v"}, "values"),
        ]:
            baseline = database.command(name, "items", **options)
            hinted = database.command(name, "items", hint="missing-private-index", comment=COMMENT, **options)
            assert "briskdbReadWarnings" not in baseline
            assert hinted["briskdbReadWarnings"] == [WARNING]
            if result_field == "cursor":
                for reply in (baseline, hinted):
                    identifier = reply["cursor"]["id"]
                    rows = []
                    while identifier:
                        next_page = database.command("getMore", identifier, collection="items", batchSize=1, comment=COMMENT)
                        assert "briskdbReadWarnings" not in next_page
                        rows.extend(next_page["cursor"]["nextBatch"])
                        identifier = next_page["cursor"]["id"]
                    assert rows == DOCUMENTS
            else:
                assert hinted[result_field] == baseline[result_field]

        missing = client.unwritten_read_options
        assert list(missing.items.find({}, hint="missing", comment=COMMENT)) == []
        assert missing.items.count_documents({}, hint="missing", comment=COMMENT) == 0
        assert missing.items.distinct("v", hint="missing", comment=COMMENT) == []
        for name, base in [("find", {}), ("count", {}), ("distinct", {"key": "v"}),
                           ("aggregate", {"pipeline": [], "cursor": {}})]:
            for options in [
                {"hint": 1}, {"hint": []},
                {"collation": {"locale": "en"}}, {"collation": {"locale": "simple", "strength": 1}},
                {"readConcern": {"level": "majority"}}, {"readConcern": {"level": "snapshot"}},
                {"readConcern": {"level": "local", "afterClusterTime": 1}},
                {"arbitraryPrivateOption": "must-not-leak"},
            ]:
                for target in (database, missing):
                    try:
                        target.command(name, "items", comment=COMMENT, **base, **options)
                    except OperationFailure as error:
                        assert error.code == 72, (name, options, error.code)
                        assert "must-not-leak" not in str(error)
                        assert "briskdbReadWarnings" not in error.details
                    else:
                        raise AssertionError(("unsupported option accepted", name, options))
        for option in ("tailable", "awaitData", "noCursorTimeout", "allowPartialResults", "returnKey", "showRecordId", "allowDiskUse"):
            assert database.command("find", "items", **{option: False})["cursor"]["firstBatch"] == DOCUMENTS
            try:
                missing.command("find", "items", **{option: True})
            except OperationFailure as error:
                assert error.code == 72
            else:
                raise AssertionError(("unsupported true flag accepted", option))
        for value in (True, False):
            assert database.command("find", "items", oplogReplay=value)["cursor"]["firstBatch"] == DOCUMENTS
        assert "unwritten_read_options" not in client.list_database_names()
        assert list(collection.find({})) == DOCUMENTS


async def async_read_options(uri):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_read_options.items
        cursor = collection.find({}).hint("missing-private-index").comment(COMMENT).sort("_id", -1).skip(1).limit(3).batch_size(1)
        assert [row["_id"] for row in await cursor.to_list()] == [4, 3, 2]
        assert await collection.find_one({"_id": 2}, hint={"v": 1}, comment=COMMENT) == DOCUMENTS[2]
        assert await collection.count_documents({"v": {"$gte": 1}}, hint="missing", comment=COMMENT) == 4
        assert await collection.distinct("v", hint="missing", comment=COMMENT) == [0, 1, 2]
        local = collection.with_options(read_concern=ReadConcern("local"))
        assert await local.find({"tag": "t"}, collation={"locale": "simple"}).sort("_id").to_list() == DOCUMENTS[1::2]
        cursor = await local.aggregate([{"$sort": {"_id": 1}}], hint="missing", comment=COMMENT, let={})
        assert await cursor.to_list() == DOCUMENTS


if __name__ == "__main__":
    assert pymongo.version == "4.17.0"
    assert len(sys.argv) == 3 and sys.argv[2] in ("initial", "reopened")
    sync_read_options(sys.argv[1], sys.argv[2] == "reopened")
    asyncio.run(asyncio.wait_for(async_read_options(sys.argv[1]), timeout=20))
