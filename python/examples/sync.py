import briskdb


with briskdb.connect("./briskdb-data", shards=4) as database:
    with database.session(routing_key="account-1") as session:
        session.migrate(
            "CREATE TABLE IF NOT EXISTS notes "
            "(id INTEGER PRIMARY KEY, body TEXT NOT NULL)"
        )
    with database.transaction(routing_key="account-1") as transaction:
        transaction.execute(
            "INSERT OR REPLACE INTO notes (id, body) VALUES (?1, ?2)",
            [1, "hello"],
        )
    with database.session(routing_key="account-1") as session:
        print(session.query("SELECT id, body FROM notes WHERE id = ?1", [1]))
