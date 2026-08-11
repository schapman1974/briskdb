use std::{
    io::{self, Write},
    path::PathBuf,
};

use briskdb::import::{SqliteImportOptions, SqliteImportPlan, import_sqlite_database};
use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Import one standard SQLite database into a new BriskDB layout"
)]
struct Args {
    /// Existing standard SQLite database opened read-only.
    #[arg(long, value_name = "SQLITE_FILE")]
    source: PathBuf,

    /// New BriskDB data directory; it must not already exist.
    #[arg(long, value_name = "DIRECTORY")]
    data_dir: PathBuf,

    /// Complete versioned JSON placement and shard-key plan.
    #[arg(long, value_name = "JSON_FILE")]
    plan: PathBuf,

    /// Fixed shard count for the new layout (2-64).
    #[arg(long, default_value_t = 4)]
    shards: u16,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let plan = SqliteImportPlan::from_json_file(&args.plan)?;
    let options = SqliteImportOptions::new(args.shards)?;
    let report = import_sqlite_database(&args.source, &args.data_dir, &plan, options)?;
    if let Err(error) = write_report(io::stdout().lock(), &report) {
        let _ = writeln!(
            io::stderr().lock(),
            "SQLite import was published at {}, but its JSON report could not be written: {error}",
            args.data_dir.display()
        );
    }
    Ok(())
}

fn write_report(
    mut writer: impl Write,
    report: &briskdb::import::SqliteImportReport,
) -> io::Result<()> {
    serde_json::to_writer_pretty(&mut writer, report).map_err(io::Error::other)?;
    writer.write_all(b"\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_requires_all_paths_and_preserves_the_shard_count() {
        assert!(Args::try_parse_from(["briskdb-import"]).is_err());
        let args = Args::try_parse_from([
            "briskdb-import",
            "--source",
            "source.db",
            "--data-dir",
            "target",
            "--plan",
            "plan.json",
            "--shards",
            "8",
        ])
        .unwrap();
        assert_eq!(args.source, PathBuf::from("source.db"));
        assert_eq!(args.data_dir, PathBuf::from("target"));
        assert_eq!(args.plan, PathBuf::from("plan.json"));
        assert_eq!(args.shards, 8);
    }

    #[test]
    fn report_writer_propagates_output_failure_for_post_publish_warning_handling() {
        struct BrokenWriter;

        impl Write for BrokenWriter {
            fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed output"))
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let report = briskdb::import::SqliteImportReport {
            receipt_version: briskdb::import::SQLITE_IMPORT_RECEIPT_VERSION,
            shard_count: 2,
            hash_version: 1,
            key_encoding_version: 1,
            bucket_algorithm_version: 1,
            map_generation: 1,
            source_schema_blake3: "0".repeat(64),
            plan_blake3: "1".repeat(64),
            tables: Vec::new(),
            omitted_foreign_keys: Vec::new(),
        };
        assert_eq!(
            write_report(BrokenWriter, &report).unwrap_err().kind(),
            io::ErrorKind::Other
        );
    }
}
