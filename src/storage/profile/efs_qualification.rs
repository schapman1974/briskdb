//! Explicitly invoked, disposable-data qualification, not a public NFS mode.
//! Coarse ownership intentionally serializes complete open/work/close scopes.
//! Advisory exclusion is NOT a demonstrated lock-loss/stale-writer fence.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    core::{
        ContentionJitter, ContentionPolicy, Database, EngineError, EngineErrorKind, EngineResult,
        OperationControl, StorageProfile, Value,
    },
    document::{BsonDocument, BsonValue, DocumentCollectionOptions},
    storage::{Storage, contention},
};

const LOCK: &str = ".briskdb-efs-test.lock";
const MARKER: &str = ".briskdb-efs-test.marker";
const ACK: &str = "disposable-no-production-data";
const TEST_NAME: &str = "storage::profile::efs_qualification::efs_qualification";

fn rejected(message: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::FailedPrecondition, message)
}

fn io(error: std::io::Error) -> EngineError {
    crate::sqlite_error::storage_io(error, "EFS qualification file operation failed")
}

fn sql(error: rusqlite::Error) -> EngineError {
    crate::sqlite_error::storage(error)
}

fn require(value: bool, message: &str) -> EngineResult<()> {
    if value {
        Ok(())
    } else {
        Err(rejected(message))
    }
}

fn label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

fn unescape_mount(value: &str) -> String {
    value
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

/// Match the directory descriptor's actual mount ID, not a potentially hidden
/// or stacked mount with a similar pathname. fdinfo/mountinfo are read-only.
fn validate_mountinfo(text: &str, id: &str, root: &Path) -> EngineResult<String> {
    let matches: Vec<_> = text
        .lines()
        .filter(|line| line.split_whitespace().next() == Some(id))
        .collect();
    require(matches.len() == 1, "mount identity is absent or ambiguous")?;
    let line = matches[0];
    let fields: Vec<_> = line.split_whitespace().collect();
    let separator = fields
        .iter()
        .position(|field| *field == "-")
        .ok_or_else(|| rejected("invalid mountinfo record"))?;
    require(
        separator >= 6 && fields.len() == separator + 4,
        "invalid mountinfo fields",
    )?;
    let mount = PathBuf::from(unescape_mount(fields[4]));
    require(
        root.starts_with(&mount) && root != mount,
        "use an isolated child directory, not a mount root",
    )?;
    require(
        matches!(fields[separator + 1], "nfs" | "nfs4"),
        "qualification requires an actual Linux NFS mount",
    )?;
    let options: Vec<_> = fields[5]
        .split(',')
        .chain(fields[separator + 3].split(','))
        .collect();
    for expected in ["rw", "vers=4.1", "hard", "proto=tcp", "local_lock=none"] {
        require(
            options.contains(&expected),
            &format!("qualification requires {expected}"),
        )?;
    }
    require(
        !options.iter().any(|option| {
            option.starts_with("soft")
                || *option == "nolock"
                || *option == "ro"
                || (option.starts_with("vers=") && *option != "vers=4.1")
                || (option.starts_with("proto=") && *option != "proto=tcp")
                || (option.starts_with("local_lock=") && *option != "local_lock=none")
        }),
        "unsafe or conflicting mount options",
    )?;
    Ok(line.to_owned())
}

fn inspect_mount(directory: &File, root: &Path) -> EngineResult<String> {
    require(
        cfg!(target_os = "linux"),
        "cloud qualification requires Linux; local tests are not EFS evidence",
    )?;
    let info =
        fs::read_to_string(format!("/proc/self/fdinfo/{}", directory.as_raw_fd())).map_err(io)?;
    let id = info
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:"))
        .map(str::trim)
        .ok_or_else(|| rejected("directory mount identity unavailable"))?;
    validate_mountinfo(
        &fs::read_to_string("/proc/self/mountinfo").map_err(io)?,
        id,
        root,
    )
}

#[derive(Debug)]
struct Target {
    root: PathBuf,
    directory: File,
    marker: String,
}

impl Target {
    fn open(root: &Path, run: &str, filesystem: &str) -> EngineResult<Self> {
        require(
            label(run) && label(filesystem) && filesystem.starts_with("fs-"),
            "invalid run or declared EFS identity",
        )?;
        require(
            root.is_absolute()
                && root.file_name().and_then(|v| v.to_str())
                    == Some(format!("briskdb-efs-test-{run}").as_str()),
            "root must be an absolute dedicated briskdb-efs-test-RUN directory",
        )?;
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(root)
            .map_err(io)?;
        let root = fs::canonicalize(root).map_err(io)?;
        let target = Self {
            root,
            directory,
            marker: format!("briskdb-efs-test-v1\n{run}\n{filesystem}\n"),
        };
        target.check_directory()?;
        Ok(target)
    }

    fn check_directory(&self) -> EngineResult<()> {
        let held = self.directory.metadata().map_err(io)?;
        let current = fs::symlink_metadata(&self.root).map_err(io)?;
        require(
            current.is_dir()
                && held.dev() == current.dev()
                && held.ino() == current.ino()
                && current.mode() & 0o077 == 0,
            "test directory identity changed or permissions are not owner-only",
        )
    }

    fn check_marker(&self) -> EngineResult<()> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(self.root.join(MARKER))
            .map_err(io)?;
        require(
            file.metadata().map_err(io)?.is_file(),
            "test marker is not a regular file",
        )?;
        let mut content = String::new();
        file.take(1024).read_to_string(&mut content).map_err(io)?;
        require(
            content == self.marker_for(&fs::symlink_metadata(self.root.join(LOCK)).map_err(io)?)?,
            "test marker does not match this run and declared EFS identity",
        )
    }

    fn marker_for(&self, lock: &fs::Metadata) -> EngineResult<String> {
        require(
            lock.is_file() && lock.nlink() == 1,
            "test lock identity is not a retained regular file",
        )?;
        // NFS file IDs, unlike the client-local device number, must agree on
        // the selected 64-bit Linux clients. This is identity, not a lease token.
        Ok(format!(
            "{}root_inode={}\nlock_inode={}\n",
            self.marker,
            self.directory.metadata().map_err(io)?.ino(),
            lock.ino()
        ))
    }

    fn acquire(&self, initialize: bool, control: &OperationControl) -> EngineResult<Guard<'_>> {
        self.check_directory()?;
        if initialize {
            require(
                fs::read_dir(&self.root).map_err(io)?.next().is_none(),
                "initialization requires an empty disposable directory; existing data is never adopted",
            )?;
        } else {
            self.check_marker()?;
        }
        let path = self.root.join(LOCK);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(initialize)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(&path)
            .map_err(io)?;
        let guard = Guard {
            target: self,
            file,
            path,
        };
        guard.check()?;
        loop {
            if let Some(reason) = control.reason() {
                return Err(reason.error());
            }
            // SAFETY: the owned descriptor is live; flock retains no Rust pointer.
            if unsafe { libc::flock(guard.file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                break;
            }
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::WouldBlock {
                return Err(io(error));
            }
            if control.wait_for_contention(None) != Some(true) {
                if let Some(reason) = control.reason() {
                    return Err(reason.error());
                }
                return Err(EngineError::new(
                    EngineErrorKind::Busy,
                    "EFS test root is owned; no database work started",
                ));
            }
            guard.check()?;
        }
        guard.check()?;
        if initialize {
            guard.file.sync_all().map_err(io)?;
            let mut marker = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(self.root.join(MARKER))
                .map_err(io)?;
            marker
                .write_all(
                    self.marker_for(&guard.file.metadata().map_err(io)?)?
                        .as_bytes(),
                )
                .map_err(io)?;
            marker.sync_all().map_err(io)?;
            self.directory.sync_all().map_err(io)?;
        } else {
            self.check_marker()?;
        }
        Ok(guard)
    }
}

struct Guard<'a> {
    target: &'a Target,
    file: File,
    path: PathBuf,
}

impl Guard<'_> {
    fn check(&self) -> EngineResult<()> {
        self.target.check_directory()?;
        let held = self.file.metadata().map_err(io)?;
        let current = fs::symlink_metadata(&self.path).map_err(io)?;
        require(
            current.is_file()
                && held.dev() == current.dev()
                && held.ino() == current.ino()
                && held.nlink() == 1
                && current.mode() & 0o077 == 0,
            "retained lock identity changed; do not retry uncertain work",
        )
    }
}

fn control(wait_ms: u64, probe: bool) -> EngineResult<Arc<OperationControl>> {
    require(
        (10..=60_000).contains(&wait_ms),
        "wait must be 10..60000 milliseconds",
    )?;
    let policy = if probe {
        ContentionPolicy::fail_fast()
    } else {
        ContentionPolicy::new(
            Duration::from_millis(2),
            Duration::from_millis(wait_ms.min(100)),
            2,
            ContentionJitter::None,
            10_000,
            Duration::from_millis(wait_ms),
        )?
    };
    Ok(OperationControl::with_contention_policy(None, Some(policy)))
}

fn document(key: &str) -> EngineResult<BsonDocument> {
    BsonDocument::from_entries([
        ("_id", BsonValue::String(key.to_owned())),
        ("value", BsonValue::Int32(7)),
    ])
    .map_err(|_| rejected("test document construction failed"))
}

fn database_work(
    root: &Path,
    action: &str,
    client: &str,
    rows: usize,
    control: &Arc<OperationControl>,
) -> EngineResult<()> {
    if action != "init" {
        require(
            fs::symlink_metadata(root.join("manifest.sqlite"))
                .map_err(io)?
                .is_file(),
            "test manifest missing or replaced; do not reinitialize this root",
        )?;
    }
    let started = Instant::now();
    let storage =
        Storage::open_with_profile_control(root, 2, None, Some(control), StorageProfile::Nfs)?;
    let database = Database::from_test_storage(storage.clone())?;
    println!("efs_test\topen_us\t{}", started.elapsed().as_micros());
    if action == "init" {
        database.broadcast("CREATE TABLE qualification_items(key TEXT NOT NULL PRIMARY KEY, value INTEGER NOT NULL)")?;
        storage.create_document_collection(
            "qualification",
            "items",
            &DocumentCollectionOptions::empty(),
        )?;
    } else if matches!(action, "crash-before-commit" | "crash-after-commit") {
        let shard = storage.shard_for_key(b"__crash__");
        let mut connection = storage.open_shard(shard)?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(sql)?;
        transaction
            .execute(
                "INSERT INTO qualification_items VALUES ('__crash__', 99)",
                [],
            )
            .map_err(sql)?;
        transaction.cache_flush().map_err(sql)?;
        if action == "crash-after-commit" {
            transaction.commit().map_err(sql)?;
        } else {
            let mut header = [0; 8];
            File::open(root.join(format!("shards/{shard:04}.sqlite-journal")))
                .map_err(io)?
                .read_exact(&mut header)
                .map_err(io)?;
            require(header != [0; 8], "crash test did not produce a hot journal")?;
        }
        println!("efs_test\texpected-process-exit\t73\tboundary\t{action}");
        std::io::stdout().flush().map_err(io)?;
        // Deliberately skip Rust/SQLite cleanup only inside the marked test root.
        std::process::exit(73);
    } else {
        let catalog = storage.document_catalog()?;
        let collection = catalog
            .collection("qualification", "items")
            .ok_or_else(|| rejected("test collection missing; do not reinitialize this root"))?;
        if action == "write" {
            for ordinal in 0..rows {
                let started = Instant::now();
                let key = format!("{client}:{ordinal}");
                database.execute(
                    &key,
                    "INSERT INTO qualification_items VALUES (?1, 1)",
                    &[Value::from(key.as_str())],
                )?;
                database.execute(
                    &key,
                    "UPDATE qualification_items SET value=2 WHERE key=?1",
                    &[Value::from(key.as_str())],
                )?;
                let transient = format!("{key}:deleted");
                database.execute(
                    &transient,
                    "INSERT INTO qualification_items VALUES (?1, 0)",
                    &[Value::from(transient.as_str())],
                )?;
                require(
                    database.execute(
                        &transient,
                        "DELETE FROM qualification_items WHERE key=?1",
                        &[Value::from(transient.as_str())],
                    )? == 1,
                    "delete did not affect its exact row",
                )?;
                storage.insert_document(collection.id(), &document(&key)?)?;
                println!(
                    "efs_test\tcommitted\t{key}\tcrud_us\t{}",
                    started.elapsed().as_micros()
                );
            }
        } else {
            for client in ["A", "B"] {
                for ordinal in 0..rows {
                    let key = format!("{client}:{ordinal}");
                    let result = database.query(
                        &key,
                        "SELECT value FROM qualification_items WHERE key=?1",
                        &[Value::from(key.as_str())],
                    )?;
                    require(
                        result.rows().len() == 1
                            && result.rows()[0].get(0) == Some(&Value::Int64(2)),
                        "acknowledged SQL row is missing or changed",
                    )?;
                    let found = storage
                        .get_document(collection.id(), &BsonValue::String(key.clone()))?
                        .ok_or_else(|| rejected("acknowledged document missing"))?;
                    require(
                        found.representation_eq(&document(&key)?),
                        "document content changed",
                    )?;
                }
            }
            let mut total = 0_i64;
            for shard in 0..2 {
                let connection = storage.open_shard(shard)?;
                total += connection
                    .query_row(
                        "SELECT COUNT(*) FROM qualification_items WHERE key != '__crash__'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(sql)?;
                require(
                    connection
                        .pragma_query_value(None, "integrity_check", |row| row.get::<_, String>(0))
                        .map_err(sql)?
                        == "ok",
                    "shard integrity check failed",
                )?;
            }
            require(
                total == (rows * 2) as i64,
                "unexpected SQL rows or incomplete test run",
            )?;
            let crash = database.query(
                "__crash__",
                "SELECT value FROM qualification_items WHERE key='__crash__'",
                &[],
            )?;
            let committed = action == "verify-committed";
            require(
                crash.rows().len() == usize::from(committed),
                "crashed transaction has an unexpected durable outcome",
            )?;
            if committed {
                require(
                    crash.rows()[0].get(0) == Some(&Value::Int64(99)),
                    "crashed commit content changed",
                )?;
            }
        }
    }
    // Neither pools nor database ownership escapes this lexical scope.
    drop(database);
    drop(storage);
    for directory in [root.to_owned(), root.join("shards")] {
        for entry in fs::read_dir(directory).map_err(io)? {
            let name = entry
                .map_err(io)?
                .file_name()
                .to_string_lossy()
                .into_owned();
            require(
                !name.ends_with("-wal") && !name.ends_with("-shm"),
                "rollback test created WAL/SHM files",
            )?;
        }
    }
    Ok(())
}

fn run(
    target: &Target,
    action: &str,
    client: &str,
    rows: usize,
    wait_ms: u64,
    hold_ms: u64,
) -> EngineResult<()> {
    require(
        matches!(
            action,
            "init"
                | "write"
                | "verify"
                | "verify-committed"
                | "hold"
                | "probe"
                | "crash-before-commit"
                | "crash-after-commit"
        ),
        "unknown qualification action",
    )?;
    require(
        matches!(client, "A" | "B")
            && (1..=1000).contains(&rows)
            && (1..=30_000).contains(&hold_ms),
        "invalid client, rows or hold duration",
    )?;
    let control = control(wait_ms, action == "probe")?;
    let acquired = target.acquire(action == "init", &control);
    if action == "probe" {
        return match acquired {
            Err(error) if error.kind() == EngineErrorKind::Busy => {
                println!("efs_test\texclusion\tbusy-as-required");
                Ok(())
            }
            Err(error) => Err(error),
            Ok(_) => Err(rejected(
                "expected peer ownership was absent: exclusion NOT demonstrated",
            )),
        };
    }
    let guard = acquired?;
    let result = if action == "hold" {
        println!("efs_test\theld\t{}\tclient\t{client}", std::process::id());
        std::io::stdout().flush().map_err(io)?;
        std::thread::sleep(Duration::from_millis(hold_ms));
        Ok(())
    } else {
        contention::with_control(Some(Arc::clone(&control)), || {
            database_work(&target.root, action, client, rows, &control)
        })
    };
    guard.check()?;
    result?;
    println!("efs_test\tscope_complete\t{action}\tclient\t{client}");
    Ok(())
}

fn env(name: &str) -> EngineResult<String> {
    std::env::var(name).map_err(|_| rejected(format!("set {name} explicitly")))
}

fn number(name: &str, default: u64) -> EngineResult<u64> {
    std::env::var(name).map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|_| rejected(format!("invalid {name}")))
    })
}

#[test]
#[ignore = "requires an explicitly approved disposable EFS directory; see python/SERVERLESS.md"]
fn efs_qualification() -> EngineResult<()> {
    require(
        env("BRISKDB_EFS_TEST_ACK")? == ACK,
        "disposable-data acknowledgement required",
    )?;
    let target = Target::open(
        Path::new(&env("BRISKDB_EFS_TEST_ROOT")?),
        &env("BRISKDB_EFS_TEST_RUN")?,
        &env("BRISKDB_EFS_TEST_FILESYSTEM")?,
    )?;
    let mount = inspect_mount(&target.directory, &target.root)?;
    println!(
        "efs_test\ttest\t{TEST_NAME}\nmount\t{mount}\nboot_id\t{}\nkernel\t{}\narchitecture\t{}\noperator_declared_efs\t{}",
        fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .map_err(io)?
            .trim(),
        fs::read_to_string("/proc/sys/kernel/osrelease")
            .map_err(io)?
            .trim(),
        std::env::consts::ARCH,
        env("BRISKDB_EFS_TEST_FILESYSTEM")?
    );
    run(
        &target,
        &env("BRISKDB_EFS_TEST_ACTION")?,
        &env("BRISKDB_EFS_TEST_CLIENT")?,
        usize::try_from(number("BRISKDB_EFS_TEST_ROWS", 20)?)
            .map_err(|_| rejected("row count exceeds platform size"))?,
        number("BRISKDB_EFS_TEST_WAIT_MS", 5000)?,
        number("BRISKDB_EFS_TEST_HOLD_MS", 30_000)?,
    )
}

#[cfg(test)]
mod tests;
