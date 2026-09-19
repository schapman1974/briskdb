from typing import List, Literal, Optional, Tuple
from uuid import UUID

import briskdb


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
    index_lifecycle: Literal["pending_build"] = document_session.create_index(
        "app", "notes", {"body": 1}, name="body_1"
    )["lifecycle"]
    inserted: int = document_session.insert_one(
        "app", "notes", {"_id": 1, "body": "typed"}
    )["inserted_count"]
    documents: List[dict[str, object]] = document_session.find(
        "app", "notes", {"_id": 1}, projection=["body"], sort={"body": 1}
    )["documents"]
    count: int = document_session.count_documents("app", "notes")["count"]
    exists: bool = document_session.collection_exists("app", "notes")["exists"]
    metadata: List[dict[str, object]] = document_session.list_collection_metadata("app", {"name": "notes"}, name_only=True, batch_size=1, batch_byte_limit=1024)["documents"]
    print(metadata)
    database_names: List[str] = document_session.list_database_names({"name": "app"})["names"]
    print(database_names)
    dropped_collection: bool = document_session.drop_collection("app", "missing")["existed"]
    dropped_database: bool = document_session.drop_database("missing")["existed"]
    print(dropped_collection, dropped_database)
    distinct: List[object] = document_session.distinct("app", "notes", "body")["values"]
    aggregated: List[dict[str, object]] = document_session.aggregate("app", "notes", [{"$count": "n"}], batch_size=1)["documents"]
    print(aggregated)
    continued: List[dict[str, object]] = document_session.get_more("app", "notes", 1)["documents"]
    killed: bool = document_session.kill_cursor("app", "notes", 1)["killed"]
    deleted: int = document_session.delete_one("app", "notes", {"_id": 1})[
        "deleted_count"
    ]
    deleted_many: int = document_session.delete_many("app", "notes", {"body": "typed"})["deleted_count"]
    removed: Optional[dict[str, object]] = document_session.find_one_and_delete("app", "notes", {}, projection=["body"], sort={"body": 1})["document"]
    updated: int = document_session.update_one("app", "notes", {}, {"$set": {"body": "updated"}})["modified_count"]
    modified: int = document_session.replace_one("app", "notes", {}, {"body": "replacement"}, upsert=False)["modified_count"]
    replaced: Optional[dict[str, object]] = document_session.find_one_and_replace("app", "notes", {}, {"body": "replacement"}, projection=["_id"], return_document=True)["document"]
    print(removed, modified, replaced)
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
    await session.insert_one("app", "typed", {"_id": 1})
    document_count: int = (await session.count_documents("app", "typed"))["count"]
    exists: bool = (await session.collection_exists("app", "typed"))["exists"]
    metadata: List[dict[str, object]] = (await session.list_collection_metadata("app", name_only=True, batch_size=1))["documents"]
    print(metadata)
    database_names: List[str] = (await session.list_database_names({"name": "app"}))["names"]
    print(database_names)
    dropped_collection: bool = (await session.drop_collection("app", "missing"))["existed"]
    dropped_database: bool = (await session.drop_database("missing"))["existed"]
    print(dropped_collection, dropped_database)
    distinct: List[object] = (await session.distinct("app", "typed", "_id"))["values"]
    aggregated: List[dict[str, object]] = (await session.aggregate("app", "typed", [{"$count": "n"}], batch_size=1))["documents"]
    print(aggregated)
    projected: List[dict[str, object]] = (await session.find("app", "typed", projection={"_id": 1}, sort={"_id": -1}))["documents"]
    continued: List[dict[str, object]] = (await session.get_more("app", "typed", 1))["documents"]
    killed: bool = (await session.kill_cursor("app", "typed", 1))["killed"]
    deleted_many: int = (await session.delete_many("app", "typed", {}))["deleted_count"]
    removed: Optional[dict[str, object]] = (await session.find_one_and_delete("app", "typed", {}, projection={"_id": 1}, sort={"_id": -1}))["document"]
    updated: int = (await session.update_one("app", "typed", {}, {"$unset": {"body": 1}}))["modified_count"]
    modified: int = (await session.replace_one("app", "typed", {}, {"body": "replacement"}))["modified_count"]
    replaced: Optional[dict[str, object]] = (await session.find_one_and_replace("app", "typed", {}, {"body": "replacement"}, sort={"_id": 1}, return_document=True))["document"]
    print(removed, modified, replaced)
    print(deleted_many)
    print(continued, killed, projected, exists, distinct)
    print(address, data_address, admin_address, rows, outcome, namespace, document_count)
