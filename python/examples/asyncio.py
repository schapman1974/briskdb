import asyncio

import briskdb


async def main() -> None:
    async with await briskdb.open_async("./briskdb-data", shards=4) as database:
        async with await database.session(routing_key="account-1") as session:
            result = await session.query(
                "SELECT id, body FROM notes WHERE id = ?1",
                [1],
                timeout_ms=2_000,
            )
            print(result)


asyncio.run(main())
