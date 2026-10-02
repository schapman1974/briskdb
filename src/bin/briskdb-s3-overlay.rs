//! One-shot JSON adapter for the opt-in overlay library. Useful for Lambda and
//! scheduled compaction; it never keeps a warmed database alive after a reply.
#[cfg(unix)]
use briskdb::s3_overlay::{Cell, Config, Database, Row};
#[cfg(unix)]
use serde::Deserialize;
#[cfg(unix)]
use serde_json::{Value, json};
#[cfg(unix)]
use std::{
    collections::BTreeMap,
    io::{self, Read},
    path::PathBuf,
    time::Instant,
};

#[derive(Deserialize)]
#[cfg(unix)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Create {
        root: PathBuf,
        config: Config,
        #[serde(default)]
        seed: BTreeMap<String, Vec<Row>>,
    },
    Query {
        root: PathBuf,
        sql: String,
        #[serde(default)]
        params: Vec<Cell>,
        #[serde(default = "pruning_default")]
        parquet_pruning: bool,
    },
    #[cfg(feature = "experimental-duckdb-reader")]
    QueryDuckdb {
        root: PathBuf,
        table: String,
        routing_key: Cell,
        sql: String,
        #[serde(default)]
        params: Vec<Cell>,
        options: briskdb::s3_overlay::DuckDbReadOptions,
    },
    Execute {
        root: PathBuf,
        sql: String,
        #[serde(default)]
        params: Vec<Cell>,
        #[serde(default = "pruning_default")]
        parquet_pruning: bool,
    },
    Compact {
        root: PathBuf,
        table: Option<String>,
        partition: Option<u16>,
    },
}

#[cfg(unix)]
fn pruning_default() -> bool {
    true
}

#[cfg(unix)]
fn run(request: Request) -> briskdb::EngineResult<Value> {
    let started = Instant::now();
    let root = match &request {
        Request::Create { root, .. }
        | Request::Query { root, .. }
        | Request::Execute { root, .. }
        | Request::Compact { root, .. } => root,
        #[cfg(feature = "experimental-duckdb-reader")]
        Request::QueryDuckdb { root, .. } => root,
    };
    if let Request::Create { root, config, seed } = request {
        let database = Database::create_s3(root, config, seed)?;
        let config = database.config().clone();
        drop(database);
        return Ok(
            json!({"ok":true,"config":config,"create_ms":started.elapsed().as_secs_f64()*1000.0,"closed":true}),
        );
    }
    let mut database = Database::open_s3(root)?;
    let open_ms = started.elapsed().as_secs_f64() * 1000.0;
    let operation = Instant::now();
    let result = match request {
        Request::Query {
            sql,
            params,
            parquet_pruning,
            ..
        } => {
            database.set_parquet_pruning(parquet_pruning);
            json!(database.query(&sql, &params)?)
        }
        #[cfg(feature = "experimental-duckdb-reader")]
        Request::QueryDuckdb {
            table,
            routing_key,
            sql,
            params,
            options,
            ..
        } => {
            json!(database.query_partition_duckdb(&table, &routing_key, &sql, &params, &options)?)
        }
        Request::Execute {
            sql,
            params,
            parquet_pruning,
            ..
        } => {
            database.set_parquet_pruning(parquet_pruning);
            json!(database.execute(&sql, &params)?)
        }
        Request::Compact {
            table: Some(table),
            partition: Some(partition),
            ..
        } => json!(database.compact(&table, partition)?),
        Request::Compact {
            table: None,
            partition: None,
            ..
        } => json!(database.compact_all()?),
        Request::Compact { .. } => {
            return Err(briskdb::EngineError::new(
                briskdb::EngineErrorKind::InvalidArgument,
                "supply both table and partition or neither",
            ));
        }
        Request::Create { .. } => unreachable!(),
    };
    let operation_ms = operation.elapsed().as_secs_f64() * 1000.0;
    let read_stats = database.read_stats().clone();
    drop(database);
    Ok(
        json!({"ok":true,"result":result,"open_ms":open_ms,"operation_ms":operation_ms,
        "total_ms":started.elapsed().as_secs_f64()*1000.0,"closed":true,"read_stats":read_stats}),
    )
}

#[cfg(unix)]
fn main() {
    #[cfg(feature = "s3-overlay-cli")]
    if std::env::args_os().len() > 1 {
        use clap::Parser;
        #[derive(Parser)]
        #[command(
            version,
            about = "Opt-in BriskDB ISAM/SQLite/S3-Parquet mode. No listeners."
        )]
        struct Cli {
            #[command(flatten)]
            overlay: briskdb::s3_overlay::cli::OverlayArgs,
        }
        match Cli::parse().overlay.run() {
            Ok(result) => println!("{result}"),
            Err(error) => {
                println!("{}", json!({"ok":false,"error":error.to_string()}));
                std::process::exit(1);
            }
        }
        return;
    }
    #[cfg(not(feature = "s3-overlay-cli"))]
    if std::env::args_os().len() > 1 {
        eprintln!(
            "flag-based commands require a build with --features s3-overlay-cli; without flags this tool accepts one JSON request on stdin"
        );
        std::process::exit(2);
    }
    let mut input = String::new();
    let outcome = (|| -> Result<Value, Box<dyn std::error::Error>> {
        io::stdin()
            .take(64 * 1024 * 1024 + 1)
            .read_to_string(&mut input)?;
        if input.len() > 64 * 1024 * 1024 {
            return Err("request exceeds 64 MiB".into());
        }
        Ok(run(serde_json::from_str(&input)?)?)
    })();
    match outcome {
        Ok(value) => println!("{value}"),
        Err(error) => {
            println!("{}", json!({"ok":false,"error":error.to_string()}));
            std::process::exit(1);
        }
    }
}

#[cfg(not(unix))]
fn main() {
    eprintln!("the experimental S3 overlay requires Unix ISAM support");
    std::process::exit(2);
}
