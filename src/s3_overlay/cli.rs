//! Flag-based process boundary shared by the one-shot tool and main binary.
//! No listeners, no implicit database creation, no background worker.
use super::{Cell, Config, Database, OpenOptions, Row, Table, invalid, storage_error};
use clap::{Args, Subcommand};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{collections::BTreeMap, fs::File, io::Read, path::PathBuf, time::Instant};

#[derive(Debug, Args)]
pub struct OverlayArgs {
    /// Shared root for this separate overlay mode. Never an ordinary database.
    #[arg(long, env = "BRISKDB_OVERLAY_ROOT", global = true)]
    pub root: Option<PathBuf>,
    /// Use and publish ISAM Parquet-file summaries; only affects this connection.
    #[arg(long, env = "BRISKDB_OVERLAY_PARQUET_PRUNING", default_value_t = true,
        action = clap::ArgAction::Set, global = true)]
    pub parquet_pruning: bool,
    /// Refuse writes, creation and compaction before publication.
    #[arg(long, env = "BRISKDB_OVERLAY_READ_ONLY", default_value_t = false,
        action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true",
        require_equals = true, global = true)]
    pub read_only: bool,
    #[command(subcommand)]
    pub action: Action,
}

#[derive(Debug, Subcommand)]
pub enum Action {
    /// Create a NEW overlay. Existing roots are never converted or overwritten.
    Create(CreateArgs),
    /// Run one read-only SQLite SELECT, including base and pending rows.
    Query(SqlArgs),
    /// Commit one modifying statement in one table/key partition.
    Execute(SqlArgs),
    /// Merge one partition, or all partitions if both selectors are omitted.
    Compact {
        #[arg(long, requires = "partition")]
        table: Option<String>,
        #[arg(long, requires = "table")]
        partition: Option<u16>,
    },
    /// Show effective flags and credential-free persisted configuration.
    Settings,
}

#[derive(Debug, Args)]
pub struct SqlArgs {
    #[arg(long)]
    pub sql: String,
    /// JSON list of typed scalar values, e.g. '[{"Text":"id-1"}]'.
    #[arg(long, default_value = "[]", value_parser = parse_params)]
    pub params_json: SqlParameters,
}

#[derive(Debug, Clone)]
pub struct SqlParameters(pub Vec<Cell>);

fn parse_params(value: &str) -> std::result::Result<SqlParameters, String> {
    if value.len() > 1024 * 1024 {
        return Err("params-json exceeds 1 MiB".into());
    }
    serde_json::from_str(value)
        .map(SqlParameters)
        .map_err(|e| e.to_string())
}

#[derive(Debug, Args)]
pub struct CreateArgs {
    #[arg(long, env = "BRISKDB_OVERLAY_BUCKET")]
    pub bucket: String,
    #[arg(long, env = "BRISKDB_OVERLAY_REGION")]
    pub region: String,
    #[arg(long, env = "BRISKDB_OVERLAY_PREFIX")]
    pub prefix: String,
    /// JSON list of tables (name, columns, primary_key, shard_key, indexes).
    #[arg(long)]
    pub schema_file: PathBuf,
    /// Optional JSON map of table names to typed rows.
    #[arg(long)]
    pub seed_file: Option<PathBuf>,
    #[arg(long, env = "BRISKDB_OVERLAY_SHARDS", default_value_t = 4)]
    pub shards: u16,
    #[arg(long, env = "BRISKDB_OVERLAY_PARTITIONS", default_value_t = 64)]
    pub partitions: u16,
    #[arg(
        long,
        env = "BRISKDB_OVERLAY_COMPACT_AFTER_FILES",
        default_value_t = 32
    )]
    pub compact_after_files: usize,
    #[arg(long, env = "BRISKDB_OVERLAY_MAX_PENDING_FILES", default_value_t = 64)]
    pub max_pending_files: usize,
    #[arg(long, env = "BRISKDB_OVERLAY_WRITE_RETRY_MS", default_value_t = 60_000)]
    pub write_retry_ms: u64,
}

fn read_json<T: DeserializeOwned>(path: &PathBuf) -> super::Result<T> {
    let mut data = Vec::new();
    File::open(path)
        .map_err(storage_error)?
        .take(super::MAX_BYTES as u64 + 1)
        .read_to_end(&mut data)
        .map_err(storage_error)?;
    if data.len() > super::MAX_BYTES {
        return Err(invalid("JSON file exceeds 64 MiB"));
    }
    serde_json::from_slice(&data).map_err(storage_error)
}

impl OverlayArgs {
    /// Run on an ordinary thread, not inside an asynchronous runtime.
    pub fn run(self) -> super::Result<Value> {
        let started = Instant::now();
        let root = self
            .root
            .ok_or_else(|| invalid("--root or BRISKDB_OVERLAY_ROOT is required"))?;
        let options = OpenOptions {
            parquet_pruning: self.parquet_pruning,
            read_only: self.read_only,
        };
        if let Action::Create(create) = self.action {
            if options.read_only {
                return Err(invalid("read-only overlay cannot create a database"));
            }
            let tables: Vec<Table> = read_json(&create.schema_file)?;
            let mut config = Config::new(create.bucket, create.region, create.prefix, tables)?;
            config.shards = create.shards;
            config.partitions = create.partitions;
            config.compact_after_files = create.compact_after_files;
            config.max_pending_files = create.max_pending_files;
            config.write_retry_ms = create.write_retry_ms;
            config.validate()?;
            let seed: BTreeMap<String, Vec<Row>> = create
                .seed_file
                .as_ref()
                .map(read_json)
                .transpose()?
                .unwrap_or_default();
            let database = Database::create_s3_with_options(root, config, seed, options)?;
            let result = json!({"config":database.config(),"options":database.options(),"storage_mode":"s3-overlay"});
            drop(database);
            return Ok(
                json!({"ok":true,"result":result,"total_ms":started.elapsed().as_secs_f64()*1000.0,"closed":true}),
            );
        }
        if options.read_only && matches!(self.action, Action::Execute(_) | Action::Compact { .. }) {
            return Err(super::EngineError::new(
                super::EngineErrorKind::ReadOnly,
                "overlay was opened read-only",
            ));
        }
        let mut database = Database::open_s3_with_options(root, options)?;
        let open_ms = started.elapsed().as_secs_f64() * 1000.0;
        let operation = Instant::now();
        let result = match self.action {
            Action::Query(sql) => json!(database.query(&sql.sql, &sql.params_json.0)?),
            Action::Execute(sql) => json!(database.execute(&sql.sql, &sql.params_json.0)?),
            Action::Compact {
                table: Some(table),
                partition: Some(partition),
            } => json!(database.compact(&table, partition)?),
            Action::Compact {
                table: None,
                partition: None,
            } => json!(database.compact_all()?),
            Action::Compact { .. } => {
                return Err(invalid("supply both table and partition or neither"));
            }
            Action::Settings => json!({"storage_mode":"s3-overlay","metadata_backend":"isam",
                "data_backend":"sqlite","write_backend":"s3-parquet","config":database.config(),"options":database.options()}),
            Action::Create(_) => unreachable!(),
        };
        let operation_ms = operation.elapsed().as_secs_f64() * 1000.0;
        let stats = database.read_stats().clone();
        let open_stats = database.open_stats().cloned();
        drop(database);
        Ok(
            json!({"ok":true,"result":result,"open_ms":open_ms,"operation_ms":operation_ms,
            "open_stats":open_stats,"read_stats":stats,"total_ms":started.elapsed().as_secs_f64()*1000.0,"closed":true}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        options: OverlayArgs,
    }

    #[test]
    fn flags_parse_and_default_to_the_tested_sqlite_reader() {
        Cli::command().debug_assert();
        let args = Cli::try_parse_from([
            "overlay",
            "--root",
            "/not/opened",
            "query",
            "--sql",
            "SELECT 1",
        ])
        .unwrap()
        .options;
        assert!(args.parquet_pruning);
        assert!(!args.read_only);
        let args = Cli::try_parse_from([
            "overlay",
            "query",
            "--root",
            "/not/opened",
            "--read-only",
            "--parquet-pruning",
            "false",
            "--sql",
            "SELECT * FROM items WHERE id=?",
            "--params-json",
            r#"[{"Text":"key"}]"#,
        ])
        .unwrap()
        .options;
        assert!(args.read_only);
        assert!(!args.parquet_pruning);
        let Action::Query(sql) = args.action else {
            panic!("query action")
        };
        assert_eq!(sql.params_json.0, vec![Cell::Text("key".into())]);
    }

    #[test]
    fn invalid_flags_and_read_only_mutations_fail_without_creating_a_root() {
        for params in ["not-json", r#"["untyped"]"#] {
            assert!(
                Cli::try_parse_from([
                    "overlay",
                    "--root",
                    "/not/opened",
                    "query",
                    "--sql",
                    "SELECT 1",
                    "--params-json",
                    params
                ])
                .is_err()
            );
        }
        assert!(
            Cli::try_parse_from([
                "overlay",
                "--root",
                "/not/opened",
                "--parquet-pruning",
                "maybe",
                "settings"
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "overlay",
                "--root",
                "/not/opened",
                "compact",
                "--table",
                "items"
            ])
            .is_err()
        );
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("absent");
        let args = Cli::try_parse_from([
            "overlay",
            "--root",
            root.to_str().unwrap(),
            "--read-only",
            "execute",
            "--sql",
            "DELETE FROM items",
        ])
        .unwrap()
        .options;
        assert_eq!(
            args.run().unwrap_err().kind(),
            crate::EngineErrorKind::ReadOnly
        );
        assert!(!root.exists());
    }

    #[test]
    fn environment_flags_and_cli_precedence_use_isolated_children() {
        const MARKER: &str = "BRISKDB_OVERLAY_ENV_TEST_CHILD";
        if std::env::var_os(MARKER).is_some() {
            let args = Cli::try_parse_from(["overlay", "settings"])
                .unwrap()
                .options;
            assert_eq!(args.root, Some(PathBuf::from("/env/root")));
            assert!(!args.parquet_pruning);
            assert!(args.read_only);
            let args = Cli::try_parse_from([
                "overlay",
                "--root",
                "/cli/root",
                "--read-only=false",
                "--parquet-pruning",
                "true",
                "settings",
            ])
            .unwrap()
            .options;
            assert_eq!(args.root, Some(PathBuf::from("/cli/root")));
            assert!(args.parquet_pruning);
            assert!(!args.read_only);
            return;
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","s3_overlay::cli::tests::environment_flags_and_cli_precedence_use_isolated_children","--nocapture"])
            .env(MARKER,"1").env("BRISKDB_OVERLAY_ROOT","/env/root")
            .env("BRISKDB_OVERLAY_READ_ONLY","true").env("BRISKDB_OVERLAY_PARQUET_PRUNING","false")
            .output().unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
