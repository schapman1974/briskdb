//! Minimal listener-free BriskDB program.

use std::path::PathBuf;

use briskdb::{BriskDb, Statement, Value};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let data_dir = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./briskdb-embedded-example"));
    let db = BriskDb::builder(data_dir)
        .with_shard_count(4)
        .open()
        .await?;
    let session = db.session();
    session.set_routing_key("example").await?;

    db.migrate(
        &session,
        "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
    )
    .await?;
    db.execute_write(
        &session,
        Statement::new(
            "INSERT OR REPLACE INTO notes (id, body) VALUES (?1, ?2)",
            vec![Value::from(1_i64), Value::from("hello from embedded Rust")],
        ),
    )
    .await?;

    let result = db
        .query(
            &session,
            Statement::new(
                "SELECT id, body FROM notes WHERE id = ?1",
                vec![1_i64.into()],
            ),
        )
        .await?;
    println!("shard {}: {:?}", result.shard, result.value.rows());
    println!("{} shards ready", db.status(&session).await?.shard_count());

    session.close().await?;
    db.close().await?;
    Ok(())
}
