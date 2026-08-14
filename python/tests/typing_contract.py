from typing import List, Optional, Tuple

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
    print(affected, rows, row)


async def async_contract(path: str) -> None:
    database: briskdb.AsyncDatabase = await briskdb.open_async(path, shards=2)
    server: briskdb.AsyncServer = await database.serve()
    address: str = server.http_address
    await server.close()
    session: briskdb.AsyncSession = await database.session(routing_key="typed")
    cursor: briskdb.AsyncCursor = await session.cursor("SELECT 1")
    rows: List[Tuple[object, ...]] = await cursor.fetchall()
    print(address, rows)
