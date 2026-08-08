use std::{
    array,
    sync::{Arc, Barrier},
    thread,
};

use anyhow::{Context, anyhow, bail};
use briskdb::{
    core::{Engine, ResultSet, Session, Statement, Value},
    storage::Database,
};
use tokio::{runtime::Runtime, task::JoinSet};

pub const BENCHMARK_SHARDS: u16 = 4;
const BENCHMARK_KEY_PREFIX: &str = "benchmark-key";

const CREATE_BENCHMARK_TABLE: &str = "CREATE TABLE benchmark_items (
    id TEXT PRIMARY KEY,
    writes INTEGER NOT NULL,
    payload TEXT NOT NULL
);";

const INSERT_BENCHMARK_ITEM: &str =
    "INSERT INTO benchmark_items (id, writes, payload) VALUES (?1, ?2, ?3)";

const READ_BENCHMARK_ITEM: &str = "SELECT id, writes, payload FROM benchmark_items WHERE id = ?1";

const UPDATE_BENCHMARK_ITEM: &str = "UPDATE benchmark_items SET writes = writes + 1 WHERE id = ?1";

const READ_WRITE_COUNT: &str = "SELECT writes FROM benchmark_items WHERE id = ?1";

pub fn engine_benchmark_runtime() -> anyhow::Result<Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("create engine benchmark runtime")
}

pub struct BenchmarkFixture {
    _directory: tempfile::TempDir,
    database: Arc<Database>,
    keys_by_shard: [String; BENCHMARK_SHARDS as usize],
}

pub struct EngineBenchmarkFixture {
    _directory: tempfile::TempDir,
    engine: Engine,
    keys_by_shard: [String; BENCHMARK_SHARDS as usize],
    sessions_by_shard: [Arc<Session>; BENCHMARK_SHARDS as usize],
}

impl EngineBenchmarkFixture {
    pub fn new(runtime: &Runtime) -> anyhow::Result<Self> {
        let directory = tempfile::tempdir().context("create engine benchmark directory")?;
        let engine = runtime.block_on(Engine::open(directory.path(), BENCHMARK_SHARDS))?;
        let keys_by_shard = find_engine_key_for_each_shard()?;

        let sessions_by_shard = runtime.block_on(async {
            let session = engine.session();
            let completed = engine
                .broadcast(&session, CREATE_BENCHMARK_TABLE.to_owned())
                .await?;
            if completed != (0..BENCHMARK_SHARDS).collect::<Vec<_>>() {
                bail!("engine benchmark schema broadcast returned unexpected shards")
            }

            for (expected_shard, key) in keys_by_shard.iter().enumerate() {
                let session = engine.session();
                session.set_routing_key(key).await?;
                let inserted = engine
                    .execute(
                        &session,
                        Statement::new(
                            INSERT_BENCHMARK_ITEM,
                            vec![
                                Value::from(key.clone()),
                                Value::from(0_i64),
                                Value::from("baseline payload"),
                            ],
                        ),
                    )
                    .await?;
                if inserted.shard != expected_shard as u16 || inserted.value != 1 {
                    bail!("engine benchmark seed insert returned an unexpected result")
                }
            }

            let mut sessions = Vec::with_capacity(BENCHMARK_SHARDS as usize);
            for key in &keys_by_shard {
                let session = Arc::new(engine.session());
                session.set_routing_key(key).await?;
                sessions.push(session);
            }
            sessions.try_into().map_err(|_| {
                anyhow!("engine benchmark created the wrong number of persistent sessions")
            })
        })?;

        Ok(Self {
            _directory: directory,
            engine,
            keys_by_shard,
            sessions_by_shard,
        })
    }

    pub fn keys_by_shard(&self) -> &[String; BENCHMARK_SHARDS as usize] {
        &self.keys_by_shard
    }

    pub fn shard_for_key(&self, key: &str) -> u16 {
        engine_shard_for_key(key)
    }

    pub async fn point_read(&self) -> anyhow::Result<ResultSet> {
        let key = &self.keys_by_shard[0];
        let result = self
            .engine
            .query(
                &self.sessions_by_shard[0],
                Statement::new(READ_BENCHMARK_ITEM, vec![Value::from(key.clone())]),
            )
            .await?;
        if result.shard != 0 {
            bail!(
                "engine point read routed to unexpected shard {}",
                result.shard
            )
        }
        Ok(result.value)
    }

    pub async fn point_write(&self) -> anyhow::Result<usize> {
        self.update_key(0).await
    }

    pub async fn four_shard_concurrent_write_wave(
        &self,
    ) -> anyhow::Result<[usize; BENCHMARK_SHARDS as usize]> {
        let mut tasks = JoinSet::new();

        for (expected_shard, key) in self.keys_by_shard.iter().cloned().enumerate() {
            let engine = self.engine.clone();
            let session = Arc::clone(&self.sessions_by_shard[expected_shard]);
            tasks.spawn(async move {
                let result = engine
                    .execute(
                        &session,
                        Statement::new(UPDATE_BENCHMARK_ITEM, vec![Value::from(key)]),
                    )
                    .await?;
                if result.shard != expected_shard as u16 {
                    bail!(
                        "engine concurrent write routed to unexpected shard {}",
                        result.shard
                    )
                }
                Ok::<_, anyhow::Error>(result.value)
            });
        }

        let mut affected = Vec::with_capacity(BENCHMARK_SHARDS as usize);
        while let Some(result) = tasks.join_next().await {
            affected.push(
                result.map_err(|error| anyhow!("engine benchmark worker failed: {error}"))??,
            );
        }

        affected
            .try_into()
            .map_err(|_| anyhow!("engine concurrent benchmark returned the wrong result count"))
    }

    pub async fn write_count(&self, shard: usize) -> anyhow::Result<i64> {
        let key = self
            .keys_by_shard
            .get(shard)
            .context("engine benchmark shard index is out of range")?;
        let result = self
            .engine
            .query(
                &self.sessions_by_shard[shard],
                Statement::new(READ_WRITE_COUNT, vec![Value::from(key.clone())]),
            )
            .await?;
        if usize::from(result.shard) != shard {
            bail!(
                "engine write-count query routed to unexpected shard {}",
                result.shard
            )
        }
        result
            .value
            .rows()
            .first()
            .and_then(|row| row.get(0))
            .and_then(Value::as_i64)
            .context("engine benchmark row did not contain an integer write count")
    }

    async fn update_key(&self, shard: usize) -> anyhow::Result<usize> {
        let key = self.keys_by_shard[shard].clone();
        let result = self
            .engine
            .execute(
                &self.sessions_by_shard[shard],
                Statement::new(UPDATE_BENCHMARK_ITEM, vec![Value::from(key)]),
            )
            .await?;
        if usize::from(result.shard) != shard {
            bail!(
                "engine point write routed to unexpected shard {}",
                result.shard
            )
        }
        Ok(result.value)
    }
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
        let key = format!("{BENCHMARK_KEY_PREFIX}-{candidate}");
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

fn find_engine_key_for_each_shard() -> anyhow::Result<[String; BENCHMARK_SHARDS as usize]> {
    const SEARCH_LIMIT: usize = 10_000;
    let mut keys: [Option<String>; BENCHMARK_SHARDS as usize] = array::from_fn(|_| None);

    for candidate in 0..SEARCH_LIMIT {
        let key = format!("{BENCHMARK_KEY_PREFIX}-{candidate}");
        let shard = usize::from(engine_shard_for_key(&key));
        if keys[shard].is_none() {
            keys[shard] = Some(key);
            if keys.iter().all(Option::is_some) {
                return Ok(keys.map(|key| key.expect("every engine shard key was populated")));
            }
        }
    }

    bail!("could not find an engine benchmark key for every shard in {SEARCH_LIMIT} candidates")
}

fn engine_shard_for_key(key: &str) -> u16 {
    let digest = blake3::hash(key.as_bytes());
    let prefix: [u8; 8] = digest.as_bytes()[..8]
        .try_into()
        .expect("BLAKE3 digest always contains eight bytes");
    (u64::from_le_bytes(prefix) % u64::from(BENCHMARK_SHARDS)) as u16
}
