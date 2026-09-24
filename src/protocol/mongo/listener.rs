//! Host-owned, loopback-only Mongo listener over the shared document engine.

use std::{io, net::SocketAddr, sync::Arc, time::Duration};

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
};
use tokio_util::codec::Decoder;

use super::{Request, commands, compression, decode_request, invalid, wire};
use crate::{
    BriskDb, CancellationToken, EngineState,
    document::{BsonDocument, BsonValue},
};

const MAX_CONNECTIONS: usize = 8;
const IO_TIMEOUT: Duration = Duration::from_secs(15);
const IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// A caller-owned Mongo listener. Does not close the borrowed engine,
/// install signal handlers, or enable any listener through default features.
/// Supports discovery, insert batches, and bounded BSON-filter finds. Data
/// commands require the host's `DocumentSupport::Enabled` setting.
pub struct MongoServer {
    address: SocketAddr,
    shutdown: CancellationToken,
    task: Option<JoinHandle<io::Result<()>>>,
}

impl MongoServer {
    pub async fn start(database: &BriskDb, address: SocketAddr) -> io::Result<Self> {
        if !address.ip().is_loopback() {
            return Err(invalid("Mongo listener requires loopback"));
        }
        if database.engine().state() != EngineState::Running {
            return Err(invalid("Mongo listener requires a running engine"));
        }
        let listener = TcpListener::bind(address).await?;
        Self::from_bound(database, listener, CancellationToken::new())
    }

    /// Start only after the owning server has bound every configured listener.
    pub(crate) fn from_bound(
        database: &BriskDb,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> io::Result<Self> {
        let address = listener.local_addr()?;
        if !address.ip().is_loopback() {
            return Err(invalid("Mongo listener requires loopback"));
        }
        if database.engine().state() != EngineState::Running {
            return Err(invalid("Mongo listener requires a running engine"));
        }
        let token = shutdown.clone();
        let task = tokio::spawn(run(listener, database.clone(), token));
        Ok(Self {
            address,
            shutdown,
            task: Some(task),
        })
    }

    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn begin_close(&self) {
        self.shutdown.cancel();
    }

    /// Close sockets and join bounded connection/parser work. Safe to call twice.
    pub async fn close(&mut self) -> io::Result<()> {
        self.begin_close();
        self.wait().await
    }

    /// Cancellation-safe observation: a losing select must retain the task so
    /// close can still join it instead of accidentally detaching parser work.
    pub(crate) async fn wait(&mut self) -> io::Result<()> {
        if let Some(task) = self.task.as_mut() {
            let result = task.await;
            self.task.take();
            result.map_err(|_| io::Error::other("Mongo listener task failed"))??;
        }
        Ok(())
    }
}

impl Drop for MongoServer {
    fn drop(&mut self) {
        self.begin_close();
    }
}

async fn run(
    listener: TcpListener,
    database: BriskDb,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let executor = Arc::new(commands::Executor::new(database.clone()));
    let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut connections = JoinSet::new();
    let mut lifecycle = tokio::time::interval(Duration::from_millis(100));
    let mut outcome = Ok(());
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            _ = lifecycle.tick() => {
                executor.prune_cursors();
                if database.engine().state() != EngineState::Running { break; }
            }
            _ = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(connection) => connection,
                    Err(error) => { outcome = Err(error); break; }
                };
                let Ok(permit) = slots.clone().try_acquire_owned() else { drop(stream); continue; };
                let token = shutdown.clone();
                let executor = Arc::clone(&executor);
                connections.spawn(async move {
                    // This slot remains held while the blocking parser is awaited,
                    // including during shutdown; malformed clients cannot grow the queue.
                    let _permit = permit;
                    let _ = connection(stream, token, executor).await;
                });
            }
        }
    }
    shutdown.cancel();
    drop(listener);
    while connections.join_next().await.is_some() {}
    outcome
}

async fn connection(
    mut stream: TcpStream,
    shutdown: CancellationToken,
    executor: Arc<commands::Executor>,
) -> io::Result<()> {
    let session = executor.session();
    let _cursors = executor.connection_cursors(session.id().get());
    let mut codec = compression::TransportCodec::new()?;
    let mut source = BytesMut::with_capacity(8192);
    let mut response_id = 0i32;
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let mut deadline = tokio::time::Instant::now()
            + if source.is_empty() {
                IDLE_TIMEOUT
            } else {
                IO_TIMEOUT
            };
        let frame = loop {
            if let Some(frame) = codec.decode(&mut source)? {
                break frame;
            }
            // Read fixed-size chunks, never reserve the advertised frame length.
            let mut chunk = [0u8; 8192];
            let read = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                read = tokio::time::timeout_at(deadline, stream.read(&mut chunk)) =>
                    read.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Mongo read timeout"))??,
            };
            if read == 0 {
                codec.decode_eof(&mut source)?;
                return Ok(());
            }
            if source.is_empty() {
                deadline = tokio::time::Instant::now() + IO_TIMEOUT;
            }
            source.extend_from_slice(&chunk[..read]);
        };
        response_id = response_id.wrapping_add(1);
        let compressed = frame.is_compressed();
        let (request, prepared) = tokio::task::spawn_blocking(move || {
            let request = decode_request(frame.into_frame()?)?;
            if compressed {
                compression::validate_command(&request)?;
            }
            let prepared = commands::prepare(&request);
            Ok::<_, io::Error>((request, prepared))
        })
        .await
        .map_err(|_| io::Error::other("Mongo parser task failed"))??;
        let body = match prepared {
            Some(Ok(prepared)) => {
                executor
                    .execute(&session, request.request_id, prepared, shutdown.clone())
                    .await
            }
            Some(Err(error)) => error.document(),
            None => dispatch(&request),
        };
        if compression::negotiated_zlib(&request, &body) {
            codec.enable_zlib();
        }
        let cursor_id = commands::reply_cursor_id(&body);
        let (result, rejected) = tokio::task::spawn_blocking(move || {
            if request.more_to_come {
                return Ok((None, true));
            }
            let (body, rejected) = match commands::validate_response(&body) {
                Ok(()) => (body, false),
                Err(error) => (error.document(), true),
            };
            let reply = wire::reply(&request, &body, response_id)?;
            compression::encode_reply(reply, compressed).map(|reply| (Some(reply), rejected))
        })
        .await
        .map_err(|_| io::Error::other("Mongo parser task failed"))??;
        if rejected {
            if let Some(id) = cursor_id {
                executor.discard_cursor(id);
            }
        }
        if shutdown.is_cancelled() {
            return Ok(());
        }
        if let Some(destination) = result {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                written = tokio::time::timeout(IO_TIMEOUT, stream.write_all(&destination)) => {
                    written.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Mongo write timeout"))??;
                }
            }
        }
    }
}

fn fields(entries: &[(&str, BsonValue)]) -> BsonDocument {
    BsonDocument::from_entries(entries.iter().cloned()).expect("static Mongo reply field names")
}

fn error(code: i32, name: &str, message: &str) -> BsonDocument {
    fields(&[
        ("ok", BsonValue::Double(0.0)),
        ("code", BsonValue::Int32(code)),
        ("codeName", BsonValue::String(name.into())),
        ("errmsg", BsonValue::String(message.into())),
    ])
}

fn dispatch(request: &Request) -> BsonDocument {
    let Some((command, value)) = request.body.iter().next() else {
        return error(2, "BadValue", "missing command");
    };
    let hello = matches!(command, "hello" | "ismaster" | "isMaster");
    let known = hello || matches!(command, "ping" | "buildInfo" | "buildinfo");
    if !known {
        return error(
            59,
            "CommandNotFound",
            "command not implemented by BriskDB Mongo listener",
        );
    }
    if !matches!(
        value,
        BsonValue::Int32(1) | BsonValue::Int64(1) | BsonValue::Boolean(true)
    ) && !matches!(value, BsonValue::Double(number) if *number == 1.0)
    {
        return error(2, "BadValue", "command value must be one");
    }
    if !request.sequences.is_empty() {
        return error(
            72,
            "InvalidOptions",
            "document sequences unsupported for this command",
        );
    }
    for (name, value) in request.body.iter().skip(1) {
        let valid = match name {
            "$db" => matches!(value, BsonValue::String(_)),
            "$readPreference" => matches!(value, BsonValue::Document(_)),
            "helloOk" if hello => matches!(value, BsonValue::Boolean(_)),
            // Recent drivers offer backpressure on every handshake. Accept the
            // offer, but do not advertise support or emit backpressure replies.
            "backpressure" if hello => matches!(value, BsonValue::Boolean(_)),
            "client" if hello => matches!(value, BsonValue::Document(_)),
            "compression" if hello => {
                matches!(value, BsonValue::Array(items) if items.iter().all(|item| matches!(item, BsonValue::String(_))))
            }
            "loadBalanced" if hello => matches!(value, BsonValue::Boolean(false)),
            _ => false,
        };
        if !valid {
            return error(
                72,
                "InvalidOptions",
                "unsupported command option or option type",
            );
        }
    }
    if hello {
        fields(&[
            ("ok", BsonValue::Double(1.0)),
            ("ismaster", BsonValue::Boolean(true)),
            ("isWritablePrimary", BsonValue::Boolean(true)),
            ("helloOk", BsonValue::Boolean(true)),
            ("minWireVersion", BsonValue::Int32(0)),
            ("maxWireVersion", BsonValue::Int32(8)),
            (
                "maxBsonObjectSize",
                BsonValue::Int32(wire::MAX_BOOTSTRAP_BSON_BYTES as i32),
            ),
            (
                "maxMessageSizeBytes",
                BsonValue::Int32(if compression::offers_zlib(&request.body) {
                    compression::ADVERTISED_ZLIB_MESSAGE_BYTES as i32
                } else {
                    wire::MAX_BOOTSTRAP_MESSAGE_BYTES as i32
                }),
            ),
            ("maxWriteBatchSize", BsonValue::Int32(1000)),
            (
                "compression",
                BsonValue::Array(if compression::offers_zlib(&request.body) {
                    vec![BsonValue::from("zlib")]
                } else {
                    Vec::new()
                }),
            ),
        ])
    } else if command == "ping" {
        fields(&[("ok", BsonValue::Double(1.0))])
    } else {
        fields(&[
            ("ok", BsonValue::Double(1.0)),
            (
                "version",
                BsonValue::String(format!("{}-briskdb", env!("CARGO_PKG_VERSION"))),
            ),
            (
                "versionArray",
                BsonValue::Array(vec![
                    BsonValue::Int32(0),
                    BsonValue::Int32(1),
                    BsonValue::Int32(0),
                    BsonValue::Int32(0),
                ]),
            ),
            ("bits", BsonValue::Int32(64)),
        ])
    }
}
