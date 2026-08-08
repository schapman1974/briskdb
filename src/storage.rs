use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, bail};
use rusqlite::{
    Connection, OptionalExtension, params_from_iter,
    types::{Value as SqlValue, ValueRef},
};
use serde_json::{Map, Value, json};

const SCHEMA_VERSION: &str = "1";

#[derive(Debug)]
pub struct Database {
    root: PathBuf,
    shard_count: u16,
}

impl Database {
    pub fn open(root: impl AsRef<Path>, requested_shards: u16) -> anyhow::Result<Self> {
        if !(2..=64).contains(&requested_shards) {
            bail!("shard count must be between 2 and 64");
        }

        let root = root.as_ref().to_path_buf();
        let shards_dir = root.join("shards");
        fs::create_dir_all(&shards_dir)
            .with_context(|| format!("failed to create {}", shards_dir.display()))?;

        let manifest_path = root.join("manifest.sqlite");
        let mut manifest = Connection::open(&manifest_path)
            .with_context(|| format!("failed to open {}", manifest_path.display()))?;
        configure_connection(&manifest)?;
        manifest.execute_batch(
            "CREATE TABLE IF NOT EXISTS briskdb_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );",
        )?;

        let existing: Option<String> = manifest
            .query_row(
                "SELECT value FROM briskdb_metadata WHERE key = 'shard_count'",
                [],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(existing) = existing {
            let existing: u16 = existing
                .parse()
                .context("manifest has an invalid shard count")?;
            if existing != requested_shards {
                bail!(
                    "database was created with {existing} shards, but {requested_shards} were requested"
                );
            }
        } else {
            let transaction = manifest.transaction()?;
            transaction.execute(
                "INSERT INTO briskdb_metadata (key, value) VALUES ('shard_count', ?1)",
                [requested_shards.to_string()],
            )?;
            transaction.execute(
                "INSERT INTO briskdb_metadata (key, value) VALUES ('schema_version', ?1)",
                [SCHEMA_VERSION],
            )?;
            transaction.commit()?;
        }

        let database = Self {
            root,
            shard_count: requested_shards,
        };
        for shard in 0..requested_shards {
            database.open_shard(shard)?;
        }
        Ok(database)
    }

    pub fn shard_count(&self) -> u16 {
        self.shard_count
    }

    pub fn shard_for_key(&self, key: &[u8]) -> u16 {
        let digest = blake3::hash(key);
        let prefix: [u8; 8] = digest.as_bytes()[..8]
            .try_into()
            .expect("BLAKE3 digest always contains eight bytes");
        (u64::from_le_bytes(prefix) % u64::from(self.shard_count)) as u16
    }

    pub fn execute(&self, shard_key: &str, sql: &str, params: &[Value]) -> anyhow::Result<usize> {
        let shard = self.shard_for_key(shard_key.as_bytes());
        let connection = self.open_shard(shard)?;
        let params = params.iter().map(json_to_sql).collect::<Vec<_>>();
        Ok(connection.execute(sql, params_from_iter(params))?)
    }

    pub fn query(
        &self,
        shard_key: &str,
        sql: &str,
        params: &[Value],
    ) -> anyhow::Result<Vec<Value>> {
        let shard = self.shard_for_key(shard_key.as_bytes());
        let connection = self.open_shard(shard)?;
        query_connection(&connection, sql, params)
    }

    pub fn broadcast(&self, sql: &str) -> anyhow::Result<Vec<u16>> {
        let mut completed = Vec::with_capacity(usize::from(self.shard_count));
        for shard in 0..self.shard_count {
            let connection = self.open_shard(shard)?;
            connection
                .execute_batch(sql)
                .with_context(|| format!("broadcast failed on shard {shard}"))?;
            completed.push(shard);
        }
        Ok(completed)
    }

    fn shard_path(&self, shard: u16) -> PathBuf {
        self.root.join("shards").join(format!("{shard:04}.sqlite"))
    }

    fn open_shard(&self, shard: u16) -> anyhow::Result<Connection> {
        if shard >= self.shard_count {
            bail!("shard {shard} is outside the configured range");
        }
        let path = self.shard_path(shard);
        let connection = Connection::open(&path)
            .with_context(|| format!("failed to open shard {}", path.display()))?;
        configure_connection(&connection)?;
        Ok(connection)
    }
}

fn configure_connection(connection: &Connection) -> anyhow::Result<()> {
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    Ok(())
}

fn query_connection(
    connection: &Connection,
    sql: &str,
    params: &[Value],
) -> anyhow::Result<Vec<Value>> {
    let params = params.iter().map(json_to_sql).collect::<Vec<_>>();
    let mut statement = connection.prepare(sql)?;
    let column_names = statement
        .column_names()
        .into_iter()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let mut rows = statement.query(params_from_iter(params))?;
    let mut result = Vec::new();

    while let Some(row) = rows.next()? {
        let mut object = Map::with_capacity(column_names.len());
        for (index, name) in column_names.iter().enumerate() {
            object.insert(name.clone(), sql_to_json(row.get_ref(index)?));
        }
        result.push(Value::Object(object));
    }
    Ok(result)
}

fn json_to_sql(value: &Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Bool(value) => SqlValue::Integer(i64::from(*value)),
        Value::Number(value) => value
            .as_i64()
            .map(SqlValue::Integer)
            .or_else(|| value.as_f64().map(SqlValue::Real))
            .unwrap_or_else(|| SqlValue::Text(value.to_string())),
        Value::String(value) => SqlValue::Text(value.clone()),
        Value::Array(_) | Value::Object(_) => SqlValue::Text(value.to_string()),
    }
}

fn sql_to_json(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => json!(value),
        ValueRef::Real(value) => json!(value),
        ValueRef::Text(value) => Value::String(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => Value::Array(value.iter().map(|byte| json!(byte)).collect()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_layout_and_rejects_resharding() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 4).unwrap();

        assert_eq!(database.shard_count(), 4);
        assert!(temp.path().join("manifest.sqlite").exists());
        assert!(temp.path().join("shards/0003.sqlite").exists());
        assert!(Database::open(temp.path(), 4).is_ok());
        assert!(Database::open(temp.path(), 8).is_err());
    }

    #[test]
    fn routing_is_stable() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 4).unwrap();

        let first = database.shard_for_key(b"customer-42");
        assert_eq!(first, database.shard_for_key(b"customer-42"));
        assert!(first < 4);
    }

    #[test]
    fn executes_and_queries_on_the_routed_shard() {
        let temp = tempfile::tempdir().unwrap();
        let database = Database::open(temp.path(), 4).unwrap();
        database
            .broadcast("CREATE TABLE widgets (id TEXT PRIMARY KEY, name TEXT NOT NULL);")
            .unwrap();

        database
            .execute(
                "widget-1",
                "INSERT INTO widgets (id, name) VALUES (?1, ?2)",
                &[json!("widget-1"), json!("First widget")],
            )
            .unwrap();
        let rows = database
            .query(
                "widget-1",
                "SELECT id, name FROM widgets WHERE id = ?1",
                &[json!("widget-1")],
            )
            .unwrap();

        assert_eq!(
            rows,
            vec![json!({"id": "widget-1", "name": "First widget"})]
        );
    }

    #[test]
    fn json_number_conversion_matches_the_http_compatibility_contract() {
        assert_eq!(json_to_sql(&json!(42)), SqlValue::Integer(42));
        assert_eq!(json_to_sql(&json!(1.5)), SqlValue::Real(1.5));

        let above_signed_i64_range = json!(9_223_372_036_854_775_809_u64);
        assert_eq!(
            json_to_sql(&above_signed_i64_range),
            SqlValue::Real(9_223_372_036_854_775_808.0)
        );
        assert!(serde_json::from_str::<Value>("1e400").is_err());
    }

    #[test]
    fn sqlite_text_with_invalid_utf8_is_converted_lossily() {
        assert_eq!(
            sql_to_json(ValueRef::Text(&[b'f', 0x80])),
            Value::String("f\u{fffd}".to_owned())
        );
    }
}
