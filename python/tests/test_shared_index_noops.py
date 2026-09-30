"""Initialized stores permit matching index declarations from independent workers."""

import asyncio
import inspect
import multiprocessing as mp
import tempfile
import unittest

import briskdb
from pymongo import IndexModel
from pymongo.errors import OperationFailure


def models():
    return [IndexModel("n"), IndexModel("email", unique=True),
            IndexModel("optional", sparse=True),
            IndexModel("active", partialFilterExpression={"active": True})]


async def peer_session(folder, pipe, asynchronous):
    Client = briskdb.AsyncMongoClient if asynchronous else briskdb.MongoClient
    client = Client(folder=folder)
    items = client.app.items

    async def invoke(operation):
        result = operation()
        return await result if inspect.isawaitable(result) else result

    operations = {
        "matching": lambda: items.create_indexes(models()),
        "builtin": lambda: items.create_index("_id"),
        "builtin_models": lambda: items.create_indexes([IndexModel("_id")]),
        "read": lambda: items.find_one({"n": 0}),
        "insert": lambda: items.insert_one({"_id": 99, "n": 99, "email": "child@example.com"}),
        "update": lambda: items.update_one({"n": 0}, {"$set": {"from_child": True}}),
        "conflict_key": lambda: items.create_index("different", name="n_1"),
        "conflict_unique": lambda: items.create_index("n", unique=True),
        "conflict_sparse": lambda: items.create_index("optional", sparse=False),
        "conflict_partial": lambda: items.create_index("active", partialFilterExpression={"active": False}),
        "raw_alias": lambda: client.app.command({"createIndexes": "items", "indexes": [
            {"key": {"n": 1}, "name": "other_name"}]}),
        "new_index": lambda: items.create_index("new_field"),
        "new_collection": lambda: client.app.new_collection.insert_one({"_id": 1}),
        "drop_index": lambda: items.drop_index("n_1"),
        "mixed": lambda: client.app.command({"createIndexes": "items", "indexes": [
            {"key": {"n": 1}, "name": "n_1"},
            {"key": {"new_field": 1}, "name": "new_field_1"},
            {"key": {"different": 1}, "name": "n_1"}]}),
    }
    try:
        pipe.send({"ready": True})
        while True:
            operation = await asyncio.to_thread(pipe.recv)
            if operation == "close":
                break
            try:
                value = await invoke(operations[operation])
                if operation in ("insert", "update"):
                    value = value.acknowledged
                pipe.send({"ok": True, "value": value})
            except OperationFailure as error:
                pipe.send({"ok": False, "code": error.code,
                           "name": error.details.get("codeName"), "message": error.details.get("errmsg", "")})
    finally:
        await invoke(client.close)


def peer_worker(folder, pipe, asynchronous):
    try:
        asyncio.run(peer_session(folder, pipe, asynchronous))
    except BaseException as error:
        pipe.send({"fatal": repr(error)})
        raise
    finally:
        pipe.close()


class SharedIndexNoopTests(unittest.TestCase):
    def check_workers(self, asynchronous):
        context = mp.get_context("spawn")
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.MongoClient(folder=folder, shards=2) as client:
                items = client.app.items
                items.insert_many([{"_id": i, "n": i, "email": f"user{i}@example.com", "active": True}
                                   for i in range(4)])
                names = items.create_indexes(models())
                original = items.index_information()
                parent, peer = context.Pipe()
                process = context.Process(target=peer_worker, args=(folder, peer, asynchronous))
                process.start()
                peer.close()

                def receive():
                    self.assertTrue(parent.poll(60), "peer did not respond")
                    reply = parent.recv()
                    self.assertNotIn("fatal", reply)
                    return reply

                def request(operation):
                    parent.send(operation)
                    return receive()

                try:
                    self.assertEqual(receive(), {"ready": True})
                    self.assertEqual(request("matching"), {"ok": True, "value": names})
                    # Ordinary PyMongo create_index retains its requested name;
                    # BriskDB's model helper returns the resolved built-in name.
                    self.assertEqual(request("builtin"), {"ok": True, "value": "_id_1"})
                    self.assertEqual(request("builtin_models"), {"ok": True, "value": ["_id_"]})
                    self.assertEqual(items.create_indexes(models()), names,
                                     "the original process must also be allowed a no-op")
                    self.assertEqual(request("read")["value"]["_id"], 0)
                    self.assertEqual(request("insert"), {"ok": True, "value": True})
                    self.assertEqual(request("update"), {"ok": True, "value": True})
                    self.assertTrue(items.find_one({"n": 0})["from_child"])
                    self.assertEqual(items.find_one({"n": 99})["email"], "child@example.com")
                    for operation in ("conflict_key", "conflict_unique", "conflict_sparse", "conflict_partial"):
                        reply = request(operation)
                        self.assertFalse(reply["ok"], (operation, reply))
                        self.assertEqual(reply["code"], 86, (operation, reply))
                    reply = request("raw_alias")
                    self.assertFalse(reply["ok"])
                    self.assertEqual(reply["code"], 85)
                    for operation in ("new_index", "new_collection", "drop_index", "mixed"):
                        reply = request(operation)
                        self.assertFalse(reply["ok"], (operation, reply))
                        self.assertEqual(reply["code"], 20, (operation, reply))
                        self.assertEqual(reply["name"], "IllegalOperation")
                        self.assertIn("sole-process ownership", reply["message"])
                        self.assertNotIn(folder, reply["message"])
                    with self.assertRaises(OperationFailure) as error:
                        items.create_index("new_field")
                    self.assertEqual(error.exception.code, 20)
                    self.assertEqual(items.index_information(), original)
                    self.assertNotIn("new_collection", client.app.list_collection_names())
                    self.assertEqual(request("matching"), {"ok": True, "value": names})
                    parent.send("close")
                    process.join(60)
                    self.assertEqual(process.exitcode, 0)
                    self.assertEqual(items.create_index("new_field"), "new_field_1")
                    client.app.new_collection.insert_one({"_id": 1})
                finally:
                    if process.is_alive():
                        process.terminate()
                        process.join(10)
                    parent.close()
            with briskdb.MongoClient(folder=folder) as reopened:
                self.assertEqual(reopened.app.items.count_documents({}), 5)
                self.assertIn("new_field_1", reopened.app.items.index_information())

    def test_sync_workers(self):
        self.check_workers(False)

    def test_async_workers(self):
        self.check_workers(True)


if __name__ == "__main__":
    unittest.main()
