from typing import List, Literal, Optional, Tuple
from uuid import UUID

import briskdb


def sync_contract(path: str) -> None:
    config: briskdb.Config = briskdb.Config(shards=2)
    database: briskdb.Database = briskdb.open(path, config=config)
    server: briskdb.Server = database.serve()
    address: str = server.http_address
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
        "app", "notes", {"_id": 1}
    )["documents"]
    count: int = document_session.count_documents("app", "notes")["count"]
    deleted: int = document_session.delete_one("app", "notes", {"_id": 1})[
        "deleted_count"
    ]
    print(
        address,
        affected,
        rows,
        row,
        outcome,
        collection_id,
        index_lifecycle,
        inserted,
        documents,
        count,
        deleted,
    )


async def async_contract(path: str) -> None:
    database: briskdb.AsyncDatabase = await briskdb.open_async(path, shards=2)
    server: briskdb.AsyncServer = await database.serve()
    address: str = server.http_address
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
    print(address, rows, outcome, namespace, document_count)
