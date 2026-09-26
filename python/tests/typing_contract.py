from typing import List, Literal, Optional, Tuple
from uuid import UUID
import sqlite3

import briskdb


def patched_client_contract(path: str) -> None:
    with briskdb.patch(folder=path, shards=4) as Client:
        with Client() as client:
            print(client.app.items.find_one({"_id": 1}))
    with briskdb.MongoClient(folder=path) as client:
        print(client.app.items.find().sort("score", briskdb.DESCENDING).to_list())
        names: List[str] = client.get_database("app").get_collection("items").create_indexes(
            {"key": {"score": -1}} for _ in range(1)
        )
        print(names)
        direct_name: str = client.app.items.create_index([("score", -1), "name"])
        print(direct_name)


async def async_patched_client_contract(path: str) -> None:
    async with briskdb.patch(folder=path):
        async with briskdb.AsyncMongoClient(folder=path) as client:
            print(await client.app.items.count_documents({}))
            names: List[str] = await client.get_database("app").get_collection("items").create_indexes(
                [{"key": {"score": 1}}]
            )
            print(names)
            direct_name: str = await client.app.items.create_index([("score", -1), "name"])
            print(direct_name)


def remote_contract(database: briskdb.Database, token: str) -> None:
    server: briskdb.Server = database.serve(
        admin=None, sqlite_remote_token=token, sqlite_remote_tables=["notes"],
        sqlite_remote_routing_key="typed",
    )
    connection = sqlite3.connect(":memory:")
    with briskdb.attach_remote(connection, "http://" + server.http_address, token=token) as remote:
        scope: str = remote.scope
        tables: Tuple[str, ...] = remote.tables
        print(scope, tables, connection.execute("SELECT * FROM remote.notes").fetchall())
    connection.close()
    server.close()


def mongo_contract(database: briskdb.Database) -> None:
    with database.serve(mongo="127.0.0.1:0") as server:
        address: Optional[str] = server.mongo_address
        print(address)


async def async_mongo_contract(database: briskdb.AsyncDatabase) -> None:
    async with await database.serve(mongo="127.0.0.1:0") as server:
        address: Optional[str] = server.mongo_address
        print(address)


def sync_contract(path: str) -> None:
    conflict_type: type[briskdb.IntegrityError] = briskdb.IdempotencyConflictError
    config: briskdb.Config = briskdb.Config(shards=2)
    database: briskdb.Database = briskdb.open(path, config=config)
    server: briskdb.Server = database.serve()
    address: str = server.http_address
    data_address: str = server.data_address
    admin_address: Optional[str] = server.admin_address
    server.close()
    session: briskdb.Session = database.session(routing_key="typed")
    write_result = session.execute("DELETE FROM notes WHERE id = ?1", [1])
    affected: int = write_result["rows_affected"]
    query_result = session.query("SELECT id FROM notes")
    rows: List[Tuple[object, ...]] = query_result["rows"]
    cursor: briskdb.Cursor = session.cursor("SELECT id FROM notes")
    row: Optional[Tuple[object, ...]] = cursor.fetchone()
    transaction: briskdb.Transaction = database.transaction(routing_key="typed")
    outcome: str = transaction.rollback()
    document_database: briskdb.Database = briskdb.open(
        path, documents=True, uuid_representation="standard"
    )
    document_session: briskdb.Session = document_database.session()
    request_id = UUID("00112233-4455-6677-8899-aabbccddeeff")
    collection_id: int = document_session.create_collection(
        "app", "notes", request_id=request_id
    )["collection"]["id"]
    index_lifecycle: Literal["pending_build", "ready"] = document_session.create_index(
        "app", "notes", {"body": 1}, sparse=True
    )["lifecycle"]
    document_session.create_index("app", "notes", {"body": 1}, name="partial", partial_filter={"active": True})
    built_lifecycle: Literal["pending_build", "ready"] = document_session.build_index("app", "notes", "partial")["lifecycle"]
    ready_count: int = document_session.create_built_index("app", "notes", {"body": 1}, sparse=True)["num_indexes_after"]
    print(built_lifecycle)
    index_metadata = document_session.list_indexes("app", "notes")["indexes"][0]
    sparse: bool = index_metadata.get("sparse", False)
    partial: Optional[dict[str, object]] = index_metadata.get("partial_filter")
    print(sparse, partial)
    dropped_index: bool = document_session.drop_index("app", "notes", "body_1")["acknowledged"]
    print(dropped_index)
    inserted: int = document_session.insert_one(
        "app", "notes", {"_id": 1, "body": "typed"}
    )["inserted_count"]
    documents: List[dict[str, object]] = document_session.find(
        "app", "notes", {"_id": 1}, projection=["body"], sort={"body": 1}, plan_diagnostics=True
    )["documents"]
    count: int = document_session.count_documents("app", "notes")["count"]
    exists: bool = document_session.collection_exists("app", "notes")["exists"]
    metadata: List[dict[str, object]] = document_session.list_collection_metadata("app", {"name": "notes"}, name_only=True, batch_size=1, batch_byte_limit=1024)["documents"]
    print(metadata)
    ready_indexes: List[dict[str, object]] = document_session.list_index_metadata("app", "notes", batch_size=1, batch_byte_limit=1024)["documents"]
    print(ready_indexes)
    database_names: List[str] = document_session.list_database_names({"name": "app"})["names"]
    print(database_names)
    dropped_collection: bool = document_session.drop_collection("app", "missing")["existed"]
    dropped_database: bool = document_session.drop_database("missing")["existed"]
    print(dropped_collection, dropped_database)
    distinct: List[object] = document_session.distinct("app", "notes", "body", plan_diagnostics=True)["values"]
    diagnostic_plan = document_session.find("app", "notes", plan_diagnostics=True)["plan"]
    measured = document_session.find("app", "notes", execution_stats=True)
    if "read_stats" in measured:
        examined: int = measured["read_stats"]["documents_examined"]
        read_shards: List[int] = measured["read_stats"]["shards_read"]
        print(examined, read_shards)
    if diagnostic_plan is not None and "read_access" in diagnostic_plan:
        access = diagnostic_plan["read_access"]
        if access["kind"] == "index_candidates":
            index_id: int = access["index_id"]
            print(index_id, access["candidate_kind"], access["key_count"])
        else:
            print(access["reason"])
    aggregated: List[dict[str, object]] = document_session.aggregate("app", "notes", [{"$count": "n"}], batch_size=1, plan_diagnostics=True)["documents"]
    print(aggregated)
    continued: List[dict[str, object]] = document_session.get_more("app", "notes", 1, plan_diagnostics=True)["documents"]
    killed: bool = document_session.kill_cursor("app", "notes", 1)["killed"]
    deleted: int = document_session.delete_one("app", "notes", {"_id": 1})[
        "deleted_count"
    ]
    deleted_many: int = document_session.delete_many("app", "notes", {"body": "typed"})["deleted_count"]
    removed: Optional[dict[str, object]] = document_session.find_one_and_delete("app", "notes", {}, projection=["body"], sort={"body": 1})["document"]
    updated: int = document_session.update_one("app", "notes", {}, {"$set": {"body": "updated"}})["modified_count"]
    updated_many: int = document_session.update_many("app", "notes", {}, {"$unset": {"body": 1}})["matched_count"]
    updated_image: Optional[dict[str, object]] = document_session.find_one_and_update("app", "notes", {}, {"$set": {"body": "new"}}, return_document=True)["document"]
    modified: int = document_session.replace_one("app", "notes", {}, {"body": "replacement"}, upsert=False)["modified_count"]
    did_upsert: bool = document_session.replace_one("app", "notes", {"_id": None}, {}, upsert=True)["did_upsert"]
    replaced: Optional[dict[str, object]] = document_session.find_one_and_replace("app", "notes", {}, {"body": "replacement"}, projection=["_id"], return_document=True)["document"]
    inserted_image: bool = document_session.find_one_and_update("app", "notes", {"_id": None}, {"$set": {}}, upsert=True)["did_upsert"]
    print(inserted_image)
    print(removed, modified, replaced, did_upsert)
    print(deleted_many)
    print(
        address,
        data_address,
        admin_address,
        affected,
        rows,
        row,
        outcome,
        collection_id,
        index_lifecycle,
        inserted,
        documents,
        count,
        exists,
        distinct,
        deleted,
        conflict_type,
        continued,
        killed,
    )


async def async_contract(path: str) -> None:
    database: briskdb.AsyncDatabase = await briskdb.open_async(path, shards=2)
    server: briskdb.AsyncServer = await database.serve()
    address: str = server.http_address
    data_address: str = server.data_address
    admin_address: Optional[str] = server.admin_address
    await server.close()
    session: briskdb.AsyncSession = await database.session(routing_key="typed")
    cursor: briskdb.AsyncCursor = await session.cursor("SELECT 1")
    rows: List[Tuple[object, ...]] = await cursor.fetchall()
    transaction: briskdb.AsyncTransaction = await database.transaction(
        routing_key="typed"
    )
    outcome: str = await transaction.rollback()
    created = await session.create_collection("app", "typed")
    namespace: str = created["collection"]["namespace"]
    await session.create_index("app", "typed", {"body": 1}, sparse=True)
    await session.build_index("app", "typed", "body_1")
    ready_count: int = (await session.create_built_index("app", "typed", {"body": 1}, sparse=True))["num_indexes_after"]
    await session.create_index("app", "typed", {"body": 1}, name="partial", partial_filter={"active": True})
    index_metadata = (await session.list_indexes("app", "typed"))["indexes"][0]
    sparse: bool = index_metadata.get("sparse", False)
    partial: Optional[dict[str, object]] = index_metadata.get("partial_filter")
    print(sparse, partial)
    dropped_index: bool = (await session.drop_index("app", "typed", "body_1"))["acknowledged"]
    print(dropped_index)
    await session.insert_one("app", "typed", {"_id": 1})
    document_count: int = (await session.count_documents("app", "typed"))["count"]
    exists: bool = (await session.collection_exists("app", "typed"))["exists"]
    metadata: List[dict[str, object]] = (await session.list_collection_metadata("app", name_only=True, batch_size=1))["documents"]
    print(metadata)
    ready_indexes: List[dict[str, object]] = (await session.list_index_metadata("app", "typed", batch_size=1, batch_byte_limit=1024))["documents"]
    print(ready_indexes)
    database_names: List[str] = (await session.list_database_names({"name": "app"}))["names"]
    print(database_names)
    dropped_collection: bool = (await session.drop_collection("app", "missing"))["existed"]
    dropped_database: bool = (await session.drop_database("missing"))["existed"]
    print(dropped_collection, dropped_database)
    distinct: List[object] = (await session.distinct("app", "typed", "_id", plan_diagnostics=True, execution_stats=True))["values"]
    aggregated: List[dict[str, object]] = (await session.aggregate("app", "typed", [{"$count": "n"}], batch_size=1, plan_diagnostics=True, execution_stats=True))["documents"]
    print(aggregated)
    projected: List[dict[str, object]] = (await session.find("app", "typed", projection={"_id": 1}, sort={"_id": -1}, plan_diagnostics=True, execution_stats=True))["documents"]
    continued: List[dict[str, object]] = (await session.get_more("app", "typed", 1, plan_diagnostics=True, execution_stats=True))["documents"]
    killed: bool = (await session.kill_cursor("app", "typed", 1))["killed"]
    deleted_many: int = (await session.delete_many("app", "typed", {}))["deleted_count"]
    removed: Optional[dict[str, object]] = (await session.find_one_and_delete("app", "typed", {}, projection={"_id": 1}, sort={"_id": -1}))["document"]
    updated: int = (await session.update_one("app", "typed", {}, {"$unset": {"body": 1}}))["modified_count"]
    updated_many: int = (await session.update_many("app", "typed", {}, {"$set": {"body": "updated"}}))["matched_count"]
    updated_image: Optional[dict[str, object]] = (await session.find_one_and_update("app", "typed", {}, {"$unset": {"body": 1}}, projection=["_id"]))["document"]
    modified: int = (await session.replace_one("app", "typed", {}, {"body": "replacement"}))["modified_count"]
    replaced: Optional[dict[str, object]] = (await session.find_one_and_replace("app", "typed", {}, {"body": "replacement"}, sort={"_id": 1}, return_document=True))["document"]
    inserted_image: bool = (await session.find_one_and_replace("app", "typed", {"_id": None}, {}, upsert=True))["did_upsert"]
    print(inserted_image)
    print(removed, modified, replaced)
    print(deleted_many)
    print(continued, killed, projected, exists, distinct)
    print(address, data_address, admin_address, rows, outcome, namespace, document_count)
