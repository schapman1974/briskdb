use std::{
    array,
    sync::{Arc, Barrier},
    thread,
};

use anyhow::{Context, anyhow, bail};
use briskdb::{
    core::{ResultSet, Value},
    storage::Database,
};

pub const BENCHMARK_SHARDS: u16 = 4;

pub struct BenchmarkFixture {
    _directory: tempfile::TempDir,
    database: Arc<Database>,
    keys_by_shard: [String; BENCHMARK_SHARDS as usize],
}

impl BenchmarkFixture {
    pub fn new() -> anyhow::Result<Self> {
        let directory = tempfile::tempdir().context("create benchmark directory")?;
        let database = Arc::new(Database::open(directory.path(), BENCHMARK_SHARDS)?);
        database.broadcast(
            "CREATE TABLE benchmark_items (
                id TEXT PRIMARY KEY,
                writes INTEGER NOT NULL,
                payload TEXT NOT NULL
            );",
        )?;

        let keys_by_shard = find_key_for_each_shard(&database)?;
        for key in &keys_by_shard {
            let affected = database.execute(
                key,
                "INSERT INTO benchmark_items (id, writes, payload) VALUES (?1, ?2, ?3)",
                &[
                    Value::from(key.clone()),
                    Value::from(0_i64),
                    Value::from("baseline payload"),
                ],
            )?;
            if affected != 1 {
                bail!("benchmark seed insert affected {affected} rows")
            }
        }

        Ok(Self {
            _directory: directory,
            database,
            keys_by_shard,
        })
    }

    pub fn keys_by_shard(&self) -> &[String; BENCHMARK_SHARDS as usize] {
        &self.keys_by_shard
    }

    pub fn shard_for_key(&self, key: &str) -> u16 {
        self.database.shard_for_key(key.as_bytes())
    }

    pub fn point_read(&self) -> anyhow::Result<ResultSet> {
        let key = &self.keys_by_shard[0];
        Ok(self.database.query(
            key,
            "SELECT id, writes, payload FROM benchmark_items WHERE id = ?1",
            &[Value::from(key.clone())],
        )?)
    }

    pub fn point_write(&self) -> anyhow::Result<usize> {
        update_key(&self.database, &self.keys_by_shard[0])
    }

    pub fn four_shard_concurrent_write_wave(
        &self,
    ) -> anyhow::Result<[usize; BENCHMARK_SHARDS as usize]> {
        let barrier = Arc::new(Barrier::new(BENCHMARK_SHARDS as usize + 1));

        thread::scope(|scope| {
            let mut handles = Vec::with_capacity(BENCHMARK_SHARDS as usize);
            for key in &self.keys_by_shard {
                let database = Arc::clone(&self.database);
                let barrier = Arc::clone(&barrier);
                handles.push(scope.spawn(move || {
                    barrier.wait();
                    update_key(&database, key)
                }));
            }

            barrier.wait();
            let mut affected = Vec::with_capacity(BENCHMARK_SHARDS as usize);
            for handle in handles {
                let result = handle
                    .join()
                    .map_err(|_| anyhow!("concurrent benchmark worker panicked"))??;
                affected.push(result);
            }

            affected
                .try_into()
                .map_err(|_| anyhow!("concurrent benchmark returned the wrong result count"))
        })
    }

    pub fn write_count(&self, shard: usize) -> anyhow::Result<i64> {
        let key = self
            .keys_by_shard
            .get(shard)
            .context("benchmark shard index is out of range")?;
        let result = self.database.query(
            key,
            "SELECT writes FROM benchmark_items WHERE id = ?1",
            &[Value::from(key.clone())],
        )?;
        result
            .rows()
            .first()
            .and_then(|row| row.get(0))
            .and_then(Value::as_i64)
            .context("benchmark row did not contain an integer write count")
    }
}

fn update_key(database: &Database, key: &str) -> anyhow::Result<usize> {
    Ok(database.execute(
        key,
        "UPDATE benchmark_items SET writes = writes + 1 WHERE id = ?1",
        &[Value::from(key)],
    )?)
}

fn find_key_for_each_shard(
    database: &Database,
) -> anyhow::Result<[String; BENCHMARK_SHARDS as usize]> {
    const SEARCH_LIMIT: usize = 10_000;
    let mut keys: [Option<String>; BENCHMARK_SHARDS as usize] = array::from_fn(|_| None);

    for candidate in 0..SEARCH_LIMIT {
        let key = format!("benchmark-key-{candidate}");
        let shard = usize::from(database.shard_for_key(key.as_bytes()));
        if keys[shard].is_none() {
            keys[shard] = Some(key);
            if keys.iter().all(Option::is_some) {
                return Ok(keys.map(|key| key.expect("every shard key was populated")));
            }
        }
    }

    bail!("could not find a benchmark key for every shard in {SEARCH_LIMIT} candidates")
}
