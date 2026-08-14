import briskdb


database = briskdb.connect("/mnt/persistent/briskdb", shards=4)


def handler(event: dict[str, object], _context: object) -> dict[str, object]:
    account_id = str(event["account_id"])
    row_id = int(str(event["id"]))
    with database.session(routing_key=account_id) as session:
        rows = session.query(
            "SELECT body FROM notes WHERE id = ?1", [row_id], timeout_ms=2_000
        )["rows"]
    return {"rows": rows}
