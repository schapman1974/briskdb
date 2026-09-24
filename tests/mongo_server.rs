//! Exercise the actual shipped daemon, including builds without Mongo support.
#![cfg(all(unix, feature = "server-cli"))]

use std::{
    fs::{self, File},
    path::Path,
    process::{Child, Command, ExitStatus},
    thread,
    time::{Duration, Instant},
};

const DEADLINE: Duration = Duration::from_secs(15);

struct Process(Child);

impl Process {
    fn wait(&mut self) -> ExitStatus {
        let until = Instant::now() + DEADLINE;
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < until,
                "daemon did not exit before deadline"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn start(data: &Path, log: &Path, mongo: Option<&str>) -> Process {
    let output = File::create(log).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_briskdb"));
    // Never let developer/service environment settings change these tests.
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("BRISKDB_") {
            command.env_remove(key);
        }
    }
    command
        .args([
            "--listen",
            "127.0.0.1:0",
            "--admin-listen",
            "disabled",
            "--postgres-listen",
            "disabled",
            "--shards",
            "2",
            "--data-dir",
        ])
        .arg(data);
    if let Some(address) = mongo {
        command.args(["--mongo-listen", address]);
    }
    command
        .env("RUST_LOG", "info")
        .env("NO_COLOR", "1")
        .stdout(output.try_clone().unwrap())
        .stderr(output);
    Process(command.spawn().unwrap())
}

fn ready(process: &mut Process, log: &Path) -> String {
    let until = Instant::now() + DEADLINE;
    loop {
        let text = fs::read_to_string(log).unwrap();
        if text.contains("BriskDB is ready") {
            return text;
        }
        assert!(
            process.0.try_wait().unwrap().is_none(),
            "daemon exited: {text}"
        );
        assert!(Instant::now() < until, "daemon not ready: {text}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn terminate(process: &mut Process) {
    // The readiness line is emitted only after installing signal handlers.
    assert_eq!(
        unsafe { libc::kill(process.0.id() as libc::pid_t, libc::SIGTERM) },
        0
    );
    assert!(process.wait().success());
}

#[test]
fn daemon_default_and_explicit_disabled_mongo_start_and_stop() {
    for mongo in [None, Some("disabled")] {
        let root = tempfile::tempdir().unwrap();
        let log = root.path().join("daemon.log");
        let mut process = start(&root.path().join("data"), &log, mongo);
        let text = ready(&mut process, &log);
        assert!(text.contains("mongo_listen=None"), "{text}");
        terminate(&mut process);
    }
}

#[cfg(not(feature = "mongo"))]
#[test]
fn daemon_without_mongo_rejects_activation_before_creating_files() {
    let root = tempfile::tempdir().unwrap();
    let log = root.path().join("daemon.log");
    let data = root.path().join("data");
    let mut process = start(&data, &log, Some("127.0.0.1:0"));
    assert!(!process.wait().success());
    let text = fs::read_to_string(log).unwrap();
    assert!(text.contains("`mongo` Cargo feature"), "{text}");
    assert!(!data.exists());
}

#[cfg(feature = "mongo")]
mod enabled {
    use std::{
        io::{Read, Write},
        net::{SocketAddr, TcpStream},
    };

    use briskdb::{
        document::{BsonDocument, BsonValue, decode_document, encode_document},
        protocol::mongo::{Frame, FrameCodec, MAX_BOOTSTRAP_MESSAGE_BYTES, Opcode},
    };
    use bytes::{BufMut, BytesMut};
    use tokio_util::codec::{Decoder, Encoder};

    use super::*;

    fn exchange(address: SocketAddr, command: BsonDocument) -> BsonDocument {
        let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(3)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut payload = BytesMut::new();
        payload.put_u32_le(0);
        payload.put_u8(0);
        payload.extend_from_slice(&encode_document(&command).unwrap());
        let mut packet = BytesMut::new();
        FrameCodec::default()
            .encode(
                Frame {
                    request_id: 77,
                    response_to: 0,
                    opcode: Opcode::Message,
                    payload: payload.freeze(),
                },
                &mut packet,
            )
            .unwrap();
        stream.write_all(&packet).unwrap();
        let mut length = [0; 4];
        stream.read_exact(&mut length).unwrap();
        let size = i32::from_le_bytes(length) as usize;
        assert!((21..=MAX_BOOTSTRAP_MESSAGE_BYTES).contains(&size));
        let mut response = BytesMut::zeroed(size);
        response[..4].copy_from_slice(&length);
        stream.read_exact(&mut response[4..]).unwrap();
        let frame = FrameCodec::default()
            .decode(&mut response)
            .unwrap()
            .unwrap();
        assert_eq!(frame.response_to, 77);
        assert_eq!(frame.opcode, Opcode::Message);
        let reply = decode_document(&frame.payload[5..]).unwrap();
        assert_eq!(
            reply.get_first("ok"),
            Some(&BsonValue::Double(1.0)),
            "{reply:?}"
        );
        reply
    }

    fn check_data(address: SocketAddr, insert: bool) {
        let doc = BsonDocument::from_entries([
            ("_id", BsonValue::Int32(123)),
            ("name", BsonValue::from("Ada")),
        ])
        .unwrap();
        if insert {
            let reply = exchange(
                address,
                BsonDocument::from_entries([
                    ("insert", BsonValue::from("users")),
                    (
                        "documents",
                        BsonValue::Array(vec![BsonValue::Document(doc.clone())]),
                    ),
                    ("$db", BsonValue::from("cli")),
                ])
                .unwrap(),
            );
            assert_eq!(reply.get_first("n"), Some(&BsonValue::Int32(1)));
        }
        let reply = exchange(
            address,
            BsonDocument::from_entries([
                ("find", BsonValue::from("users")),
                ("filter", BsonValue::Document(BsonDocument::new())),
                ("$db", BsonValue::from("cli")),
            ])
            .unwrap(),
        );
        let Some(BsonValue::Document(cursor)) = reply.get_first("cursor") else {
            panic!("{reply:?}")
        };
        assert_eq!(
            cursor.get_first("firstBatch"),
            Some(&BsonValue::Array(vec![BsonValue::Document(doc)]))
        );
    }

    #[test]
    fn daemon_mongo_port_zero_writes_sigterm_and_restart_preserve_documents() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        for initial in [true, false] {
            let log = root.path().join("daemon.log");
            let mut process = start(&data, &log, Some("127.0.0.1:0"));
            let text = ready(&mut process, &log);
            let raw = text
                .split("mongo_listen=Some(")
                .nth(1)
                .expect(&text)
                .split(')')
                .next()
                .unwrap();
            let address: SocketAddr = raw.parse().unwrap();
            assert_ne!(address.port(), 0);
            check_data(address, initial);
            // Shutdown must drain a client that has only sent a partial frame.
            let mut partial = TcpStream::connect(address).unwrap();
            partial
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            partial.write_all(&[1, 2]).unwrap();
            terminate(&mut process);
            match partial.read(&mut [0]) {
                Ok(0) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                    ) => {}
                result => panic!("partial Mongo client was not disconnected: {result:?}"),
            }
            assert!(TcpStream::connect(address).is_err());
        }
    }

    #[test]
    fn daemon_rejects_non_loopback_mongo_before_creating_files() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let log = root.path().join("daemon.log");
        let mut process = start(&data, &log, Some("0.0.0.0:0"));
        assert!(!process.wait().success());
        let text = fs::read_to_string(log).unwrap();
        assert!(text.contains("Mongo startup requires a loopback"), "{text}");
        assert!(!data.exists());
    }

    #[test]
    fn attached_mongo_uses_borrowed_document_engine_and_survives_listener_restart() {
        use briskdb::{
            BriskDb, DocumentSupport, EngineState, Statement,
            server::{AttachedServer, ListenerConfig},
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let root = tempfile::tempdir().unwrap();
        let db = runtime
            .block_on(
                BriskDb::builder(root.path())
                    .with_shard_count(2)
                    .with_document_support(DocumentSupport::Enabled)
                    .open(),
            )
            .unwrap();
        for initial in [true, false] {
            let mut server = runtime
                .block_on(AttachedServer::start_with_mongo(
                    &db,
                    ListenerConfig {
                        http_listen: "127.0.0.1:0".parse().unwrap(),
                        admin_listen: None,
                        postgres_listen: None,
                    },
                    "127.0.0.1:0".parse().unwrap(),
                ))
                .unwrap();
            check_data(server.addresses().mongo().unwrap(), initial);
            runtime.block_on(server.close()).unwrap();
            assert_eq!(db.state(), EngineState::Running);
        }
        runtime.block_on(async {
            let session = db.owned_session();
            session.set_routing_key("still-running").await.unwrap();
            assert_eq!(
                session
                    .query(Statement::new("SELECT 1", vec![]))
                    .await
                    .unwrap()
                    .value
                    .rows()
                    .len(),
                1
            );
            session.close().await.unwrap();
            db.close().await.unwrap();
        });
    }
}
