//! BriskDB-owned boundary for the selected PostgreSQL wire library.
//!
//! The configured loopback listener delegates startup framing and dispatch to
//! this module. Each successfully started wire connection owns one
//! protocol-neutral core session; `server`, `core`, and BriskDB's public API do
//! not accept or return `pgwire` types.

use std::{
    collections::BTreeSet,
    fmt, io,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use futures::{Sink, SinkExt, StreamExt, stream};
use pgwire::{
    api::{
        ClientInfo, ClientPortalStore, DefaultClient, ErrorHandler, PgWireConnectionState,
        PgWireServerHandlers, Type,
        auth::StartupHandler,
        portal::Portal,
        query::{ExtendedQueryHandler, SimpleQueryHandler},
        results::{
            DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat,
            FieldInfo, QueryResponse, Response, Tag,
        },
        stmt::{NoopQueryParser, QueryParser, StoredStatement},
        store::PortalStore,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{
        PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion, SslNegotiationMetaMessage,
        extendedquery::Parse,
        response::{GssEncResponse, ReadyForQuery, SslResponse, TransactionStatus},
        startup::{Authentication, ParameterStatus},
    },
    tokio::server::{PgWireMessageServerCodec, process_error, process_message},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use tokio_util::codec::Framed;

use crate::{
    core::{
        DataType, DescribeTarget, Engine, EngineError, EngineResult, EngineStatus,
        LogicalDatabaseId, PrepareRequest, PreparedExecution, PreparedStatementDescription,
        PreparedStatementId, ResultSet, Session, SessionId, Value,
    },
    protocol::error::postgres_error,
    sql::{MAX_PARSED_SQL_BYTES, SqlDialect, SqlTranslationMode, StatementBehavior, WriteBehavior},
};

const POSTGRES_PROTOCOL_MAJOR: u16 = 3;
const POSTGRES_PROTOCOL_MINOR: u16 = 0;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_STARTUP_NAME_BYTES: usize = 63;
const MAX_STARTUP_PACKET_LENGTH: usize = 10_000;
const MAX_FRONTEND_MESSAGE_LENGTH: usize = MAX_PARSED_SQL_BYTES + 5;
const CANCEL_REQUEST_CODE: i32 = 80_877_102;
const SSL_REQUEST_CODE: i32 = 80_877_103;
const GSSENC_REQUEST_CODE: i32 = 80_877_104;
const SERVER_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "-briskdb");
const PARAMETER_STATUS: [(&str, &str); 5] = [
    ("server_version", SERVER_VERSION),
    ("server_encoding", "UTF8"),
    ("client_encoding", "UTF8"),
    ("standard_conforming_strings", "on"),
    ("integer_datetimes", "on"),
];

#[derive(Clone, Copy)]
enum FrontendFramePhase {
    Startup,
    Typed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartupFrameKind {
    Negotiation,
    Startup,
}

/// Bounded raw-frame gate in front of the pinned dependency decoder.
///
/// It releases exactly one complete validated frame at a time. That keeps a
/// declared oversized body out of the dependency buffer and prevents one
/// frame's decoder from consuming bytes belonging to the next frame.
struct GuardedPgStream<S> {
    inner: S,
    phase: FrontendFramePhase,
    pending: Vec<u8>,
    ready: Option<Vec<u8>>,
    ready_offset: usize,
}

impl<S> GuardedPgStream<S> {
    fn new(inner: S, pending: Vec<u8>) -> Self {
        Self {
            inner,
            phase: FrontendFramePhase::Startup,
            pending,
            ready: None,
            ready_offset: 0,
        }
    }

    fn expected_frame_length(&self) -> io::Result<Option<usize>> {
        let (length_offset, header_length, minimum, maximum, type_length) = match self.phase {
            FrontendFramePhase::Startup => (0, 4, 8, MAX_STARTUP_PACKET_LENGTH, 0),
            FrontendFramePhase::Typed => (1, 5, 4, MAX_FRONTEND_MESSAGE_LENGTH, 1),
        };
        if self.pending.len() < header_length {
            return Ok(None);
        }
        let declared = i32::from_be_bytes(
            self.pending[length_offset..length_offset + 4]
                .try_into()
                .expect("the guarded frame header length was checked"),
        );
        if declared < minimum {
            return Err(invalid_frontend_frame());
        }
        if matches!(self.phase, FrontendFramePhase::Startup) && self.pending.len() >= 8 {
            let code = i32::from_be_bytes(
                self.pending[4..8]
                    .try_into()
                    .expect("the guarded startup preamble length was checked"),
            );
            if code == CANCEL_REQUEST_CODE
                || (matches!(code, SSL_REQUEST_CODE | GSSENC_REQUEST_CODE) && declared != 8)
            {
                return Err(invalid_frontend_frame());
            }
        }
        let declared = usize::try_from(declared).map_err(|_| invalid_frontend_frame())?;
        if declared > maximum {
            return Err(invalid_frontend_frame());
        }
        Ok(Some(declared + type_length))
    }

    fn prepare_frame(&mut self) -> io::Result<bool> {
        if self.ready.is_some() {
            return Ok(true);
        }
        let Some(frame_length) = self.expected_frame_length()? else {
            return Ok(false);
        };
        if self.pending.len() < frame_length {
            return Ok(false);
        }

        let remainder = self.pending.split_off(frame_length);
        let frame = std::mem::replace(&mut self.pending, remainder);
        match self.phase {
            FrontendFramePhase::Startup => {
                if validate_startup_frame(&frame)? == StartupFrameKind::Startup {
                    self.phase = FrontendFramePhase::Typed;
                }
            }
            FrontendFramePhase::Typed => validate_typed_frame(&frame)?,
        }
        self.ready = Some(frame);
        Ok(true)
    }
}

impl<S> AsyncRead for GuardedPgStream<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            if let Some(ready) = &this.ready {
                let available = &ready[this.ready_offset..];
                let count = available.len().min(output.remaining());
                output.put_slice(&available[..count]);
                this.ready_offset += count;
                if this.ready_offset == ready.len() {
                    this.ready = None;
                    this.ready_offset = 0;
                }
                return Poll::Ready(Ok(()));
            }

            match this.prepare_frame() {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => return Poll::Ready(Err(error)),
            }

            let needed = match this.expected_frame_length() {
                Ok(Some(frame_length)) => frame_length.saturating_sub(this.pending.len()),
                Ok(None) => match this.phase {
                    FrontendFramePhase::Startup => 4 - this.pending.len(),
                    FrontendFramePhase::Typed => 5 - this.pending.len(),
                },
                Err(error) => return Poll::Ready(Err(error)),
            };
            let mut scratch = [0_u8; 4_096];
            let read_length = needed.min(scratch.len());
            let mut read = ReadBuf::new(&mut scratch[..read_length]);
            match Pin::new(&mut this.inner).poll_read(context, &mut read) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) if read.filled().is_empty() => {
                    return if this.pending.is_empty() {
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Ready(Err(invalid_frontend_frame()))
                    };
                }
                Poll::Ready(Ok(())) => this.pending.extend_from_slice(read.filled()),
            }
        }
    }
}

impl<S> AsyncWrite for GuardedPgStream<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(context, buffer)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
    }
}

fn validate_startup_frame(frame: &[u8]) -> io::Result<StartupFrameKind> {
    if frame.len() < 8
        || frame.len() > MAX_STARTUP_PACKET_LENGTH
        || !declared_frame_length_matches(frame, 0, 0)
    {
        return Err(invalid_frontend_frame());
    }
    let code = i32::from_be_bytes(
        frame[4..8]
            .try_into()
            .expect("the guarded startup frame length was checked"),
    );
    if matches!(code, SSL_REQUEST_CODE | GSSENC_REQUEST_CODE) {
        return if frame.len() == 8 {
            Ok(StartupFrameKind::Negotiation)
        } else {
            Err(invalid_frontend_frame())
        };
    }
    if code == CANCEL_REQUEST_CODE {
        // Backend cancellation identifiers are introduced by roadmap issue
        // #35. Until then, a CancelRequest is a well-framed but unsupported
        // startup packet and must not reach the dependency's no-op handler.
        return Err(invalid_frontend_frame());
    }
    if frame.len() < 9 {
        return Err(invalid_frontend_frame());
    }

    let mut cursor = 8;
    let mut keys = BTreeSet::new();
    loop {
        let (key, next) = consume_utf8_cstring(frame, cursor).ok_or_else(invalid_frontend_frame)?;
        cursor = next;
        if key.is_empty() {
            return if cursor == frame.len() {
                Ok(StartupFrameKind::Startup)
            } else {
                Err(invalid_frontend_frame())
            };
        }
        if !keys.insert(key) {
            return Err(invalid_frontend_frame());
        }
        let (_, next) = consume_utf8_cstring(frame, cursor).ok_or_else(invalid_frontend_frame)?;
        cursor = next;
    }
}

fn validate_typed_frame(frame: &[u8]) -> io::Result<()> {
    if frame.len() < 5
        || frame.len() > MAX_FRONTEND_MESSAGE_LENGTH + 1
        || !declared_frame_length_matches(frame, 1, 1)
    {
        return Err(invalid_frontend_frame());
    }
    let body = frame.get(5..).ok_or_else(invalid_frontend_frame)?;
    match frame.first().copied() {
        Some(b'Q') => {
            if body.last() == Some(&0)
                && !body[..body.len() - 1].contains(&0)
                && std::str::from_utf8(&body[..body.len() - 1]).is_ok()
            {
                Ok(())
            } else {
                Err(invalid_frontend_frame())
            }
        }
        Some(b'P') => {
            let (_, mut cursor) =
                consume_utf8_cstring(body, 0).ok_or_else(invalid_frontend_frame)?;
            let (_, next) =
                consume_utf8_cstring(body, cursor).ok_or_else(invalid_frontend_frame)?;
            cursor = next;
            let count_end = cursor.checked_add(2).ok_or_else(invalid_frontend_frame)?;
            let count_bytes = body
                .get(cursor..count_end)
                .ok_or_else(invalid_frontend_frame)?;
            let count = usize::from(u16::from_be_bytes(
                count_bytes
                    .try_into()
                    .expect("the guarded Parse count length was checked"),
            ));
            let oid_bytes = count.checked_mul(4).ok_or_else(invalid_frontend_frame)?;
            let expected_end = count_end
                .checked_add(oid_bytes)
                .ok_or_else(invalid_frontend_frame)?;
            if expected_end == body.len() {
                Ok(())
            } else {
                Err(invalid_frontend_frame())
            }
        }
        Some(b'S') | Some(b'X') if body.is_empty() => Ok(()),
        _ => Err(invalid_frontend_frame()),
    }
}

fn declared_frame_length_matches(frame: &[u8], offset: usize, type_length: usize) -> bool {
    let Some(length_bytes) = frame.get(offset..offset + 4) else {
        return false;
    };
    let declared = i32::from_be_bytes(
        length_bytes
            .try_into()
            .expect("the declared-length slice is exactly four bytes"),
    );
    let Ok(declared) = usize::try_from(declared) else {
        return false;
    };
    declared.checked_add(type_length) == Some(frame.len())
}

fn consume_utf8_cstring(buffer: &[u8], start: usize) -> Option<(&str, usize)> {
    let tail = buffer.get(start..)?;
    let offset = tail.iter().position(|byte| *byte == 0)?;
    let value = std::str::from_utf8(&tail[..offset]).ok()?;
    let end = start.checked_add(offset)?.checked_add(1)?;
    Some((value, end))
}

fn invalid_frontend_frame() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid bounded PostgreSQL frontend frame",
    )
}

/// A PostgreSQL protocol adapter backed only by BriskDB's public engine API.
///
/// Constructing an adapter does not bind a socket, accept a connection, or
/// create a session. A distinct core session is allocated only after an
/// explicit connection open or a fully validated wire startup.
#[derive(Clone)]
pub struct Adapter {
    engine: Engine,
    default_database: LogicalDatabaseId,
    default_database_name: Box<str>,
}

impl Adapter {
    /// Construct the BriskDB-owned PostgreSQL adapter boundary.
    pub fn new(engine: Engine) -> Self {
        let default_metadata = engine.catalog().default_database();
        let default_database = default_metadata.id();
        let default_database_name = default_metadata.name().into();
        Self {
            engine,
            default_database,
            default_database_name,
        }
    }

    /// Allocate independent protocol state for a direct adapter probe.
    ///
    /// The returned value owns exactly one non-cloneable BriskDB [`Session`].
    /// No socket is opened and no wire handshake is performed.
    pub fn open_connection(&self) -> Connection {
        self.connection(None, self.default_database, &self.default_database_name)
    }

    /// Allocate a connection for one validated PostgreSQL user/database pair.
    ///
    /// The user is a bounded session label until the later role-catalog work.
    /// Database selection is an exact lookup in the protocol-neutral catalog.
    pub fn open_connection_for(&self, user: &str, database: &str) -> EngineResult<Connection> {
        if !valid_user_label(user) {
            return Err(EngineError::new(
                crate::core::EngineErrorKind::InvalidArgument,
                "PostgreSQL user labels must be 1 to 63 bytes, start with lowercase ASCII or underscore, and then contain only lowercase ASCII, digits, or underscores",
            ));
        }
        let database = self.engine.catalog().database(database)?.ok_or_else(|| {
            EngineError::new(
                crate::core::EngineErrorKind::InvalidArgument,
                "the selected PostgreSQL logical database does not exist",
            )
        })?;
        Ok(self.connection(Some(user), database.id(), database.name()))
    }

    fn connection(
        &self,
        user: Option<&str>,
        database: LogicalDatabaseId,
        database_name: &str,
    ) -> Connection {
        let state = Arc::new(ConnectionState {
            engine: self.engine.clone(),
            session: self.engine.session(),
            user: user.map(Into::into),
            database,
            database_name: database_name.into(),
        });
        let wire_parser = Arc::new(PgWireQueryParser {
            state: Arc::clone(&state),
        });
        Connection { state, wire_parser }
    }

    pub(crate) fn wire_connection(&self) -> WireConnection {
        WireConnection {
            state: Arc::new(WireConnectionState {
                adapter: self.clone(),
                connection: OnceLock::new(),
            }),
        }
    }
}

impl fmt::Debug for Adapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Adapter")
            .field("default_database", &self.default_database)
            .field("shard_count", &self.engine.shard_count())
            .finish_non_exhaustive()
    }
}

struct ConnectionState {
    engine: Engine,
    session: Session,
    user: Option<Box<str>>,
    database: LogicalDatabaseId,
    database_name: Box<str>,
}

/// Protocol-owned state for one PostgreSQL connection.
///
/// This type exposes only BriskDB values. The selected wire crate remains an
/// implementation detail inside this module.
#[must_use = "a PostgreSQL connection context should be explicitly closed"]
pub struct Connection {
    state: Arc<ConnectionState>,
    // Retaining the parser here makes the selected pgwire trait contract part
    // of every connection without exposing that dependency in BriskDB's API.
    wire_parser: Arc<PgWireQueryParser>,
}

impl Connection {
    /// Return the process-unique core session identifier for this connection.
    pub fn session_id(&self) -> SessionId {
        self.state.session.id()
    }

    /// Return the selected user label, if this connection has an identity.
    pub fn user(&self) -> Option<&str> {
        self.state.user.as_deref()
    }

    /// Return the selected logical-database identity.
    pub fn database_id(&self) -> LogicalDatabaseId {
        self.state.database
    }

    /// Return the selected canonical logical-database name.
    pub fn database(&self) -> &str {
        &self.state.database_name
    }

    /// Read engine status through this connection's protocol-neutral session.
    ///
    /// Startup uses this operation to prove that the selected session can enter
    /// the controlled async engine boundary.
    pub async fn status(&self) -> EngineResult<EngineStatus> {
        self.state.engine.status(&self.state.session).await
    }

    async fn execute_simple_query(
        &self,
        sql: &str,
    ) -> EngineResult<(StatementBehavior, PreparedExecution)> {
        let statement = self
            .state
            .engine
            .prepare_statement(
                &self.state.session,
                PrepareRequest::new(
                    self.state.database,
                    SqlDialect::PostgreSql,
                    SqlTranslationMode::Compatibility,
                    sql,
                ),
            )
            .await?;

        let result = self.execute_prepared_simple_query(statement).await;
        let cleanup = self
            .state
            .engine
            .close_prepared_statement(&self.state.session, statement)
            .await;
        match (result, cleanup) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(result), Ok(_)) => Ok(result),
        }
    }

    async fn execute_prepared_simple_query(
        &self,
        statement: PreparedStatementId,
    ) -> EngineResult<(StatementBehavior, PreparedExecution)> {
        let description = self
            .state
            .engine
            .describe_prepared(&self.state.session, DescribeTarget::Statement(statement))
            .await?;
        let behavior = description.behavior();
        let portal = self
            .state
            .engine
            .bind_statement(&self.state.session, statement, Vec::new())
            .await?;
        let result = self
            .state
            .engine
            .execute_portal_logical(&self.state.session, portal)
            .await
            .map(|executed| (behavior, executed.value));
        let cleanup = self
            .state
            .engine
            .close_portal(&self.state.session, portal)
            .await;
        match (result, cleanup) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(result), Ok(_)) => Ok(result),
        }
    }

    /// Close this connection's core session.
    ///
    /// Closing is terminal and idempotent. The production socket wrapper calls
    /// this on ordinary termination, EOF, error, shutdown, and forced cleanup.
    pub async fn close(&self) -> EngineResult<()> {
        self.state.session.close().await
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Connection")
            .field("session_id", &self.session_id())
            .field("database", &self.state.database)
            .field("has_user", &self.state.user.is_some())
            .field("wire_parser", &self.wire_parser)
            .finish_non_exhaustive()
    }
}

/// One accepted PostgreSQL socket and its optional validated core connection.
///
/// The server retains a clone while the socket task runs so it can close the
/// core session even if the task panics or is forcefully cancelled.
#[derive(Clone)]
pub(crate) struct WireConnection {
    state: Arc<WireConnectionState>,
}

struct WireConnectionState {
    adapter: Adapter,
    connection: OnceLock<Arc<Connection>>,
}

impl WireConnection {
    fn installed(&self) -> PgWireResult<&Arc<Connection>> {
        self.state
            .connection
            .get()
            .ok_or_else(|| fatal_wire_error("08P01", "PostgreSQL startup has not completed"))
    }

    async fn install(&self, user: &str, database: &str) -> PgWireResult<()> {
        if self.state.connection.get().is_some() {
            return Err(fatal_wire_error(
                "08P01",
                "PostgreSQL startup was already completed",
            ));
        }

        let connection = Arc::new(
            self.state
                .adapter
                .open_connection_for(user, database)
                .map_err(|_| {
                    fatal_wire_error("3D000", "PostgreSQL database selection is unavailable")
                })?,
        );
        if let Err(connection) = self.state.connection.set(connection) {
            let _ = connection.close().await;
            return Err(fatal_wire_error(
                "08P01",
                "PostgreSQL startup was already completed",
            ));
        }
        let connection = self
            .state
            .connection
            .get()
            .expect("the validated PostgreSQL connection was just installed");
        if let Err(error) = connection.status().await {
            let _ = connection.close().await;
            return Err(engine_error_to_pgwire_with_severity(error, "FATAL"));
        }
        Ok(())
    }

    pub(crate) async fn serve<F>(&self, stream: TcpStream, shutdown: F) -> io::Result<()>
    where
        F: std::future::Future<Output = ()> + Send,
    {
        self.serve_with_startup_timeout(stream, shutdown, STARTUP_TIMEOUT)
            .await
    }

    async fn serve_with_startup_timeout<F>(
        &self,
        stream: TcpStream,
        shutdown: F,
        startup_timeout: Duration,
    ) -> io::Result<()>
    where
        F: std::future::Future<Output = ()> + Send,
    {
        let handlers = WireHandlers {
            connection: self.clone(),
        };
        tokio::pin!(shutdown);
        let wire_result = tokio::select! {
            biased;
            _ = &mut shutdown => Ok(()),
            result = run_socket(stream, handlers, startup_timeout) => result,
        };
        let close_result = self.close().await.map_err(io::Error::other);
        wire_result.and(close_result)
    }

    pub(crate) async fn close(&self) -> EngineResult<()> {
        if let Some(connection) = self.state.connection.get() {
            connection.close().await
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    fn connection(&self) -> Option<&Arc<Connection>> {
        self.state.connection.get()
    }
}

struct WireHandlers {
    connection: WireConnection,
}

#[async_trait]
impl StartupHandler for WireHandlers {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let PgWireFrontendMessage::Startup(startup) = message else {
            return Err(fatal_wire_error(
                "08P01",
                "a PostgreSQL startup message is required",
            ));
        };
        if (startup.protocol_number_major, startup.protocol_number_minor)
            != (POSTGRES_PROTOCOL_MAJOR, POSTGRES_PROTOCOL_MINOR)
        {
            return Err(fatal_wire_error(
                "08P01",
                "the PostgreSQL protocol version is unsupported",
            ));
        }

        for key in startup.parameters.keys() {
            if !matches!(
                key.as_str(),
                "user" | "database" | "client_encoding" | "application_name" | "replication"
            ) {
                return Err(fatal_wire_error(
                    "0A000",
                    "the PostgreSQL startup parameter is unsupported",
                ));
            }
        }

        let user = startup.parameters.get("user").ok_or_else(|| {
            fatal_wire_error("28000", "a valid PostgreSQL user label is required")
        })?;
        if !valid_user_label(user) {
            return Err(fatal_wire_error(
                "28000",
                "a valid PostgreSQL user label is required",
            ));
        }
        let database = startup
            .parameters
            .get("database")
            .map(String::as_str)
            .unwrap_or(user);

        if startup
            .parameters
            .get("client_encoding")
            .is_some_and(|encoding| !matches!(encoding.as_str(), "UTF8" | "UTF-8"))
        {
            return Err(fatal_wire_error(
                "22023",
                "PostgreSQL client encoding must be UTF8",
            ));
        }
        let application_name = startup.parameters.get("application_name");
        if application_name.is_some_and(|name| !valid_application_name(name)) {
            return Err(fatal_wire_error(
                "22023",
                "the PostgreSQL application name is invalid",
            ));
        }
        if startup
            .parameters
            .get("replication")
            .is_some_and(|value| value != "false")
        {
            return Err(fatal_wire_error(
                "0A000",
                "PostgreSQL replication startup is unsupported",
            ));
        }

        self.connection.install(user, database).await?;
        client.set_protocol_version(ProtocolVersion::PROTOCOL3_0);
        let metadata = client.metadata_mut();
        metadata.insert("user".to_owned(), user.to_owned());
        metadata.insert("database".to_owned(), database.to_owned());
        metadata.insert("client_encoding".to_owned(), "UTF8".to_owned());
        if let Some(application_name) = application_name {
            metadata.insert("application_name".to_owned(), application_name.to_owned());
        }

        client
            .feed(PgWireBackendMessage::Authentication(Authentication::Ok))
            .await?;
        for (name, value) in PARAMETER_STATUS {
            client
                .feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                    name.to_owned(),
                    value.to_owned(),
                )))
                .await?;
        }
        if let Some(application_name) = application_name {
            client
                .feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                    "application_name".to_owned(),
                    application_name.to_owned(),
                )))
                .await?;
        }
        client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                TransactionStatus::Idle,
            )))
            .await?;
        client.set_state(PgWireConnectionState::ReadyForQuery);
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for WireHandlers {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let (behavior, execution) = self
            .connection
            .installed()?
            .execute_simple_query(query)
            .await
            .map_err(engine_error_to_pgwire)?;
        Ok(vec![simple_query_response(behavior, execution)?])
    }
}

fn simple_query_response(
    behavior: StatementBehavior,
    execution: PreparedExecution,
) -> PgWireResult<Response> {
    match execution {
        PreparedExecution::Rows(result) if matches!(behavior, StatementBehavior::Read) => {
            result_set_response(result)
        }
        PreparedExecution::AffectedRows(rows) => {
            Ok(Response::Execution(write_tag(behavior, rows)?))
        }
        PreparedExecution::GeneratedWrite(result) => Ok(Response::Execution(write_tag(
            behavior,
            result.rows_affected,
        )?)),
        _ => Err(internal_query_error()),
    }
}

fn write_tag(behavior: StatementBehavior, rows: usize) -> PgWireResult<Tag> {
    let tag = match behavior {
        StatementBehavior::Write(WriteBehavior::Insert) => {
            Tag::new("INSERT").with_oid(0).with_rows(rows)
        }
        StatementBehavior::Write(WriteBehavior::Update) => Tag::new("UPDATE").with_rows(rows),
        StatementBehavior::Write(WriteBehavior::Delete) => Tag::new("DELETE").with_rows(rows),
        _ => return Err(internal_query_error()),
    };
    Ok(tag)
}

fn result_set_response(result: ResultSet) -> PgWireResult<Response> {
    let (columns, rows) = result.into_parts();
    let fields = Arc::new(
        columns
            .into_iter()
            .map(|column| {
                FieldInfo::new(
                    column.name,
                    None,
                    None,
                    postgres_type(column.data_type),
                    FieldFormat::Text,
                )
            })
            .collect::<Vec<_>>(),
    );
    let encoded = rows
        .into_iter()
        .map(|row| encode_data_row(Arc::clone(&fields), row.into_values()))
        .collect::<Vec<_>>();
    if encoded.iter().any(Result::is_err) {
        return Err(internal_query_error());
    }
    Ok(Response::Query(QueryResponse::new(
        fields,
        stream::iter(encoded),
    )))
}

fn postgres_type(data_type: DataType) -> Type {
    match data_type {
        DataType::Boolean => Type::BOOL,
        DataType::Int64 => Type::INT8,
        DataType::UInt64 | DataType::Decimal => Type::NUMERIC,
        DataType::Float64 => Type::FLOAT8,
        DataType::Binary => Type::BYTEA,
        DataType::Unknown | DataType::Null | DataType::Text => Type::TEXT,
    }
}

fn encode_data_row(
    fields: Arc<Vec<FieldInfo>>,
    values: Vec<Value>,
) -> PgWireResult<pgwire::messages::data::DataRow> {
    let mut encoder = DataRowEncoder::new(fields);
    for value in values {
        match value {
            Value::Null => encoder.encode_field(&None::<String>)?,
            Value::Boolean(value) => encoder.encode_field(&value)?,
            Value::Int64(value) => encoder.encode_field(&value)?,
            Value::UInt64(value) => encoder.encode_field(&value.to_string())?,
            Value::Float64(value) => encoder.encode_field(&value)?,
            Value::Decimal(value) => encoder.encode_field(&value.into_string())?,
            Value::Text(value) => encoder.encode_field(&value)?,
            Value::InvalidText(_) => return Err(invalid_text_query_error()),
            Value::Binary(value) => encoder.encode_field(&value)?,
        }
    }
    encoder.finish()
}

fn internal_query_error() -> PgWireError {
    engine_error_to_pgwire(EngineError::new(
        crate::core::EngineErrorKind::Internal,
        "the PostgreSQL response could not be encoded",
    ))
}

fn invalid_text_query_error() -> PgWireError {
    engine_error_to_pgwire(EngineError::new(
        crate::core::EngineErrorKind::InvalidTextEncoding,
        "PostgreSQL UTF8 output cannot represent stored invalid text",
    ))
}

#[async_trait]
impl ExtendedQueryHandler for WireHandlers {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser)
    }

    async fn on_parse<C>(&self, _client: &mut C, _message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(unsupported_query_error())
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        _target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(unsupported_query_error())
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(unsupported_query_error())
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(unsupported_query_error())
    }
}

impl ErrorHandler for WireHandlers {
    fn on_error<C>(&self, client: &C, error: &mut PgWireError)
    where
        C: ClientInfo,
    {
        if matches!(error, PgWireError::UserError(_)) {
            return;
        }
        *error = if matches!(
            client.state(),
            PgWireConnectionState::AwaitingStartup
                | PgWireConnectionState::AuthenticationInProgress
        ) {
            fatal_wire_error("08P01", "the PostgreSQL startup message is invalid")
        } else {
            unsupported_query_error()
        };
    }
}

impl PgWireServerHandlers for WireHandlers {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(Self {
            connection: self.connection.clone(),
        })
    }

    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(Self {
            connection: self.connection.clone(),
        })
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(Self {
            connection: self.connection.clone(),
        })
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(Self {
            connection: self.connection.clone(),
        })
    }
}

type GuardedSocket = Framed<GuardedPgStream<TcpStream>, PgWireMessageServerCodec<String>>;

async fn negotiate_plaintext(stream: TcpStream) -> io::Result<Option<GuardedSocket>> {
    let peer = stream.peer_addr()?;
    stream.set_nodelay(true)?;

    // Direct TLS is not enabled for the loopback-only listener. Match the
    // dependency's existing behavior by closing without interpreting a TLS
    // ClientHello as PostgreSQL framing.
    let mut first = [0_u8; 1];
    if stream.peek(&mut first).await? > 0 && first[0] == 0x16 {
        return Ok(None);
    }

    let client = DefaultClient::<String>::new(peer, false);
    let codec = PgWireMessageServerCodec::new(client);
    let mut socket = Framed::new(GuardedPgStream::new(stream, Vec::new()), codec);

    loop {
        match socket.next().await {
            Some(Ok(PgWireFrontendMessage::SslNegotiation(
                SslNegotiationMetaMessage::PostgresSsl(_),
            ))) => {
                socket
                    .send(PgWireBackendMessage::SslResponse(SslResponse::Refuse))
                    .await?;
            }
            Some(Ok(PgWireFrontendMessage::SslNegotiation(
                SslNegotiationMetaMessage::PostgresGss(_),
            ))) => {
                socket
                    .send(PgWireBackendMessage::GssEncResponse(GssEncResponse::Refuse))
                    .await?;
            }
            Some(Ok(PgWireFrontendMessage::SslNegotiation(SslNegotiationMetaMessage::None))) => {
                socket.set_state(PgWireConnectionState::AwaitingStartup);
                return Ok(Some(socket));
            }
            Some(Ok(_)) | Some(Err(_)) => {
                send_connection_error(
                    &mut socket,
                    fatal_wire_error("08P01", "the PostgreSQL protocol message is invalid"),
                )
                .await?;
                return Ok(None);
            }
            None => return Ok(None),
        }
    }
}

async fn run_socket(
    stream: TcpStream,
    handlers: WireHandlers,
    startup_timeout: Duration,
) -> io::Result<()> {
    let startup_timeout = tokio::time::sleep(startup_timeout);
    tokio::pin!(startup_timeout);
    let socket = tokio::select! {
        _ = &mut startup_timeout => return Ok(()),
        socket = negotiate_plaintext(stream) => socket?,
    };
    let Some(mut socket) = socket else {
        return Ok(());
    };

    let startup_handler = Arc::new(WireHandlers {
        connection: handlers.connection.clone(),
    });
    let simple_query_handler = Arc::new(WireHandlers {
        connection: handlers.connection.clone(),
    });
    let extended_query_handler = Arc::new(WireHandlers {
        connection: handlers.connection.clone(),
    });
    let copy_handler = handlers.copy_handler();
    let cancel_handler = handlers.cancel_handler();
    let error_handler = handlers.error_handler();

    loop {
        let startup_phase = matches!(
            socket.state(),
            PgWireConnectionState::AwaitingStartup
                | PgWireConnectionState::AuthenticationInProgress
        );
        let message = if startup_phase {
            tokio::select! {
                _ = &mut startup_timeout => None,
                message = socket.next() => message,
            }
        } else {
            socket.next().await
        };

        match message {
            Some(Ok(PgWireFrontendMessage::Terminate(_))) => break,
            Some(Ok(message)) => {
                let is_extended_query = match socket.state() {
                    PgWireConnectionState::CopyInProgress(is_extended_query) => is_extended_query,
                    _ => message.is_extended_query(),
                };
                if let Err(mut error) = process_message(
                    message,
                    &mut socket,
                    Arc::clone(&startup_handler),
                    Arc::clone(&simple_query_handler),
                    Arc::clone(&extended_query_handler),
                    Arc::clone(&copy_handler),
                    Arc::clone(&cancel_handler),
                )
                .await
                {
                    error_handler.on_error(&socket, &mut error);
                    if startup_phase {
                        send_connection_error(&mut socket, error).await?;
                        break;
                    }
                    process_error(&mut socket, error, is_extended_query).await?;
                }
            }
            Some(Err(_)) => {
                send_connection_error(
                    &mut socket,
                    fatal_wire_error("08P01", "the PostgreSQL protocol message is invalid"),
                )
                .await?;
                break;
            }
            None => break,
        }
    }
    Ok(())
}

async fn send_connection_error<C>(client: &mut C, error: PgWireError) -> io::Result<()>
where
    C: Sink<PgWireBackendMessage, Error = io::Error> + Unpin,
{
    let info: ErrorInfo = error.into();
    client
        .send(PgWireBackendMessage::ErrorResponse(info.into()))
        .await?;
    client.close().await
}

fn fatal_wire_error(code: &'static str, message: &'static str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".to_owned(),
        code.to_owned(),
        message.to_owned(),
    )))
}

fn unsupported_query_error() -> PgWireError {
    engine_error_to_pgwire(EngineError::new(
        crate::core::EngineErrorKind::Unsupported,
        "the PostgreSQL extended query flow is not implemented yet",
    ))
}

fn valid_user_label(user: &str) -> bool {
    let bytes = user.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_STARTUP_NAME_BYTES
        && matches!(bytes[0], b'a'..=b'z' | b'_')
        && bytes
            .iter()
            .all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_'))
}

fn valid_application_name(name: &str) -> bool {
    name.len() <= MAX_STARTUP_NAME_BYTES
        && !name.contains('\u{fffd}')
        && name.chars().all(|character| !character.is_control())
}

#[derive(Clone)]
struct PgWirePrepared {
    id: PreparedStatementId,
    description: PreparedStatementDescription,
}

impl fmt::Debug for PgWirePrepared {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PgWirePrepared")
            .field("id", &self.id)
            .field("description", &self.description)
            .finish()
    }
}

struct PgWireQueryParser {
    state: Arc<ConnectionState>,
}

impl fmt::Debug for PgWireQueryParser {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PgWireQueryParser")
            .field("session_id", &self.state.session.id())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl QueryParser for PgWireQueryParser {
    type Statement = PgWirePrepared;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // pgwire has already converted raw OIDs by this point. Both the
        // inference marker (OID 0) and an unknown/custom OID arrive as `None`,
        // so this boundary cannot safely distinguish them. Reject every
        // nonempty type list until BriskDB owns raw Parse-message validation.
        if !types.is_empty() {
            return Err(engine_error_to_pgwire(EngineError::new(
                crate::core::EngineErrorKind::Unsupported,
                "PostgreSQL parameter OID lists are deferred to type mapping",
            )));
        }

        let request = PrepareRequest::new(
            self.state.database,
            SqlDialect::PostgreSql,
            SqlTranslationMode::Compatibility,
            sql,
        );
        let id = self
            .state
            .engine
            .prepare_statement(&self.state.session, request)
            .await
            .map_err(engine_error_to_pgwire)?;
        let description = match self
            .state
            .engine
            .describe_prepared(&self.state.session, DescribeTarget::Statement(id))
            .await
        {
            Ok(description) => description,
            Err(error) => {
                let _ = self
                    .state
                    .engine
                    .close_prepared_statement(&self.state.session, id)
                    .await;
                return Err(engine_error_to_pgwire(error));
            }
        };

        Ok(PgWirePrepared { id, description })
    }
}

fn engine_error_to_pgwire(error: EngineError) -> PgWireError {
    engine_error_to_pgwire_with_severity(error, "ERROR")
}

fn engine_error_to_pgwire_with_severity(error: EngineError, severity: &str) -> PgWireError {
    let mapping = postgres_error(error.kind());
    PgWireError::UserError(Box::new(ErrorInfo::new(
        severity.to_owned(),
        mapping.sqlstate.to_owned(),
        mapping.message.to_owned(),
    )))
}

#[cfg(test)]
mod tests {
    use std::{fmt::Debug, net::SocketAddr, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use futures::Sink;
    use pgwire::{
        api::{
            ClientInfo, ClientPortalStore, DefaultClient, PgWireServerHandlers,
            auth::{StartupHandler, noop::NoopStartupHandler},
            query::SimpleQueryHandler,
            results::{Response, Tag},
            stmt::QueryParser,
        },
        error::{PgWireError, PgWireResult},
        messages::{PgWireBackendMessage, PgWireFrontendMessage},
        tokio::process_socket,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use super::*;
    use crate::core::{EngineErrorKind, SessionState};

    struct ProbeHandler {
        connection: Arc<Connection>,
    }

    #[async_trait]
    impl NoopStartupHandler for ProbeHandler {
        async fn post_startup<C>(
            &self,
            _client: &mut C,
            _message: PgWireFrontendMessage,
        ) -> PgWireResult<()>
        where
            C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
            C::Error: Debug,
            PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
        {
            self.connection
                .status()
                .await
                .map(|_| ())
                .map_err(engine_error_to_pgwire)
        }
    }

    #[async_trait]
    impl SimpleQueryHandler for ProbeHandler {
        async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
        where
            C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
            C::Error: Debug,
            PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
        {
            if query != "BRISKDB SPIKE" {
                return Err(engine_error_to_pgwire(EngineError::new(
                    EngineErrorKind::Unsupported,
                    "the compatibility probe accepts one sentinel command",
                )));
            }
            let status = self
                .connection
                .status()
                .await
                .map_err(engine_error_to_pgwire)?;
            Ok(vec![Response::Execution(
                Tag::new("BRISKDB SPIKE").with_rows(usize::from(status.shard_count())),
            )])
        }
    }

    struct ProbeFactory {
        handler: Arc<ProbeHandler>,
    }

    impl PgWireServerHandlers for ProbeFactory {
        fn startup_handler(&self) -> Arc<impl StartupHandler> {
            Arc::clone(&self.handler)
        }

        fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
            Arc::clone(&self.handler)
        }
    }

    async fn engine(shards: u16) -> (tempfile::TempDir, Engine) {
        let temp = tempfile::tempdir().unwrap();
        let engine = Engine::open(temp.path(), shards).await.unwrap();
        (temp, engine)
    }

    fn test_client() -> DefaultClient<String> {
        DefaultClient::new("127.0.0.1:7654".parse().unwrap(), false)
    }

    fn startup_packet_with(protocol: u32, parameters: &[(&str, &str)]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&protocol.to_be_bytes());
        for (name, value) in parameters {
            body.extend_from_slice(name.as_bytes());
            body.push(0);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);
        let length = u32::try_from(body.len() + 4).unwrap();
        let mut packet = Vec::with_capacity(body.len() + 4);
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(&body);
        packet
    }

    fn startup_packet() -> Vec<u8> {
        startup_packet_with(196_608, &[("user", "briskdb"), ("database", "default")])
    }

    fn typed_packet(message_type: u8, body: &[u8]) -> Vec<u8> {
        let length = u32::try_from(body.len() + 4).unwrap();
        let mut packet = Vec::with_capacity(body.len() + 5);
        packet.push(message_type);
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(body);
        packet
    }

    async fn read_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut header = [0_u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let length = u32::from_be_bytes(header[1..].try_into().unwrap());
            assert!((4..=1_048_576).contains(&length));
            let mut body = vec![0_u8; usize::try_from(length - 4).unwrap()];
            stream.read_exact(&mut body).await.unwrap();
            (header[0], body)
        })
        .await
        .expect("the pgwire compatibility probe returned a frame")
    }

    async fn read_until_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
        let mut frames = Vec::new();
        loop {
            let frame = read_frame(stream).await;
            let ready = frame.0 == b'Z';
            frames.push(frame);
            if ready {
                return frames;
            }
            assert!(frames.len() < 64, "bounded startup response frame count");
        }
    }

    fn message_fields(body: &[u8]) -> std::collections::BTreeMap<u8, String> {
        let mut fields = std::collections::BTreeMap::new();
        let mut index = 0;
        while index < body.len() && body[index] != 0 {
            let kind = body[index];
            index += 1;
            let end = body[index..]
                .iter()
                .position(|byte| *byte == 0)
                .map(|offset| index + offset)
                .expect("a backend field is null terminated");
            fields.insert(
                kind,
                std::str::from_utf8(&body[index..end]).unwrap().to_owned(),
            );
            index = end + 1;
        }
        fields
    }

    fn parameter_status(body: &[u8]) -> (String, String) {
        let mut fields = body.split(|byte| *byte == 0);
        let name = std::str::from_utf8(fields.next().unwrap())
            .unwrap()
            .to_owned();
        let value = std::str::from_utf8(fields.next().unwrap())
            .unwrap()
            .to_owned();
        assert_eq!(fields.next(), Some([].as_slice()));
        (name, value)
    }

    fn command_tag(body: &[u8]) -> &str {
        let (terminator, tag) = body.split_last().unwrap();
        assert_eq!(*terminator, 0);
        std::str::from_utf8(tag).unwrap()
    }

    fn data_row(body: &[u8]) -> Vec<Option<Vec<u8>>> {
        let count = usize::from(u16::from_be_bytes(body[..2].try_into().unwrap()));
        let mut fields = Vec::with_capacity(count);
        let mut offset = 2;
        for _ in 0..count {
            let length = i32::from_be_bytes(body[offset..offset + 4].try_into().unwrap());
            offset += 4;
            if length == -1 {
                fields.push(None);
            } else {
                let length = usize::try_from(length).unwrap();
                fields.push(Some(body[offset..offset + length].to_vec()));
                offset += length;
            }
        }
        assert_eq!(offset, body.len());
        fields
    }

    fn row_description(body: &[u8]) -> Vec<(String, u32)> {
        let count = usize::from(u16::from_be_bytes(body[..2].try_into().unwrap()));
        let mut fields = Vec::with_capacity(count);
        let mut offset = 2;
        for _ in 0..count {
            let end = body[offset..]
                .iter()
                .position(|byte| *byte == 0)
                .map(|relative| offset + relative)
                .unwrap();
            let name = std::str::from_utf8(&body[offset..end]).unwrap().to_owned();
            offset = end + 1;
            offset += 4 + 2;
            let oid = u32::from_be_bytes(body[offset..offset + 4].try_into().unwrap());
            offset += 4 + 2 + 4 + 2;
            fields.push((name, oid));
        }
        assert_eq!(offset, body.len());
        fields
    }

    #[test]
    fn simple_query_type_oids_and_text_values_are_stable() {
        let cases = [
            (DataType::Unknown, Type::TEXT),
            (DataType::Null, Type::TEXT),
            (DataType::Boolean, Type::BOOL),
            (DataType::Int64, Type::INT8),
            (DataType::UInt64, Type::NUMERIC),
            (DataType::Float64, Type::FLOAT8),
            (DataType::Decimal, Type::NUMERIC),
            (DataType::Text, Type::TEXT),
            (DataType::Binary, Type::BYTEA),
        ];
        for (data_type, expected) in cases {
            assert_eq!(postgres_type(data_type), expected);
        }

        let types = [
            DataType::Null,
            DataType::Boolean,
            DataType::Int64,
            DataType::UInt64,
            DataType::Float64,
            DataType::Decimal,
            DataType::Text,
            DataType::Binary,
        ];
        let fields = Arc::new(
            types
                .into_iter()
                .enumerate()
                .map(|(index, data_type)| {
                    FieldInfo::new(
                        format!("field_{index}"),
                        None,
                        None,
                        postgres_type(data_type),
                        FieldFormat::Text,
                    )
                })
                .collect(),
        );
        let row = encode_data_row(
            fields,
            vec![
                Value::Null,
                Value::Boolean(true),
                Value::Int64(-42),
                Value::UInt64(u64::MAX),
                Value::Float64(1.5),
                Value::decimal("12.3400").unwrap(),
                Value::Text("hello".to_owned()),
                Value::Binary(vec![0, 255]),
            ],
        )
        .unwrap();
        let mut body = row.field_count.to_be_bytes().to_vec();
        body.extend_from_slice(&row.data);
        assert_eq!(
            data_row(&body),
            [
                None,
                Some(b"t".to_vec()),
                Some(b"-42".to_vec()),
                Some(u64::MAX.to_string().into_bytes()),
                Some(b"1.5".to_vec()),
                Some(b"12.3400".to_vec()),
                Some(b"hello".to_vec()),
                Some(br"\x00ff".to_vec()),
            ]
        );

        let invalid = encode_data_row(
            Arc::new(vec![FieldInfo::new(
                "invalid".to_owned(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )]),
            vec![Value::InvalidText(vec![0x80])],
        )
        .unwrap_err();
        let PgWireError::UserError(info) = invalid else {
            panic!("invalid UTF-8 should use a fixed BriskDB wire error")
        };
        assert_eq!(info.code, "22021");
        assert_eq!(
            info.message,
            postgres_error(EngineErrorKind::InvalidTextEncoding).message
        );
    }

    async fn assert_eof(stream: &mut TcpStream) {
        let mut byte = [0_u8; 1];
        let count = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
            .await
            .expect("the PostgreSQL peer closed promptly")
            .unwrap();
        assert_eq!(count, 0);
    }

    async fn assert_closed_after_invalid_input(stream: &mut TcpStream) {
        let mut byte = [0_u8; 1];
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
            .await
            .expect("the PostgreSQL peer closed promptly")
        {
            Ok(0) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
                ) => {}
            Ok(count) => panic!("the closed PostgreSQL peer returned {count} unexpected bytes"),
            Err(error) => panic!("the PostgreSQL peer closed with an unexpected error: {error}"),
        }
    }

    async fn assert_protocol_fatal(stream: &mut TcpStream) {
        let frame = read_frame(stream).await;
        assert_eq!(frame.0, b'E');
        let fields = message_fields(&frame.1);
        assert_eq!(fields.get(&b'S').map(String::as_str), Some("FATAL"));
        assert_eq!(fields.get(&b'C').map(String::as_str), Some("08P01"));
        assert_eq!(
            fields.get(&b'M').map(String::as_str),
            Some("the PostgreSQL protocol message is invalid")
        );
    }

    async fn spawn_wire_server(
        adapter: &Adapter,
    ) -> (
        SocketAddr,
        WireConnection,
        tokio::task::JoinHandle<io::Result<()>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let connection = adapter.wire_connection();
        let served = connection.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            served.serve(stream, std::future::pending()).await
        });
        (address, connection, server)
    }

    async fn finish_wire_server(
        client: &mut TcpStream,
        server: tokio::task::JoinHandle<io::Result<()>>,
    ) {
        client.write_all(&typed_packet(b'X', &[])).await.unwrap();
        assert_eof(client).await;
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("the PostgreSQL connection task returned")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn production_startup_selects_identity_emits_owned_status_and_terminates() {
        let (_temp, engine) = engine(3).await;
        let adapter = Adapter::new(engine.clone());
        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();

        let mut ssl_request = Vec::new();
        ssl_request.extend_from_slice(&8_u32.to_be_bytes());
        ssl_request.extend_from_slice(&80_877_103_u32.to_be_bytes());
        client.write_all(&ssl_request).await.unwrap();
        let mut ssl_response = [0_u8; 1];
        client.read_exact(&mut ssl_response).await.unwrap();
        assert_eq!(ssl_response, *b"N");

        client
            .write_all(&startup_packet_with(
                196_608,
                &[
                    ("user", "client_user"),
                    ("database", "default"),
                    ("client_encoding", "UTF-8"),
                    ("application_name", "adapter-test"),
                    ("replication", "false"),
                ],
            ))
            .await
            .unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(frames.first().unwrap(), &(b'R', vec![0, 0, 0, 0]));
        assert_eq!(frames.last().unwrap(), &(b'Z', vec![b'I']));
        assert!(!frames.iter().any(|frame| frame.0 == b'K'));
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            vec![b'R', b'S', b'S', b'S', b'S', b'S', b'S', b'Z']
        );
        let statuses = frames
            .iter()
            .filter(|frame| frame.0 == b'S')
            .map(|frame| parameter_status(&frame.1))
            .collect::<Vec<_>>();
        assert_eq!(
            statuses,
            vec![
                ("server_version".to_owned(), SERVER_VERSION.to_owned()),
                ("server_encoding".to_owned(), "UTF8".to_owned()),
                ("client_encoding".to_owned(), "UTF8".to_owned()),
                ("standard_conforming_strings".to_owned(), "on".to_owned(),),
                ("integer_datetimes".to_owned(), "on".to_owned()),
                ("application_name".to_owned(), "adapter-test".to_owned()),
            ]
        );
        assert!(!statuses.iter().any(|(_, value)| value.contains("pgwire")));

        let core = Arc::clone(wire.connection().unwrap());
        assert_eq!(core.user(), Some("client_user"));
        assert_eq!(core.database(), "default");
        assert_eq!(core.database_id(), engine.catalog().default_database().id());
        assert_eq!(core.state().await, SessionState::Ready);

        finish_wire_server(&mut client, server).await;
        assert_eq!(core.state().await, SessionState::Closed);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn negotiation_requests_require_exact_boundaries_before_startup() {
        let (_temp, engine) = engine(2).await;
        let adapter = Adapter::new(engine.clone());

        for code in [SSL_REQUEST_CODE, GSSENC_REQUEST_CODE] {
            let (address, wire, server) = spawn_wire_server(&adapter).await;
            let mut client = TcpStream::connect(address).await.unwrap();
            let mut malformed_negotiation = Vec::new();
            malformed_negotiation.extend_from_slice(&100_u32.to_be_bytes());
            malformed_negotiation.extend_from_slice(&(code as u32).to_be_bytes());
            malformed_negotiation.extend_from_slice(&startup_packet());
            client.write_all(&malformed_negotiation).await.unwrap();

            assert_protocol_fatal(&mut client).await;
            assert_closed_after_invalid_input(&mut client).await;
            tokio::time::timeout(Duration::from_secs(2), server)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(wire.connection().is_none());
        }

        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        let mut gss_request = Vec::new();
        gss_request.extend_from_slice(&8_u32.to_be_bytes());
        gss_request.extend_from_slice(&(GSSENC_REQUEST_CODE as u32).to_be_bytes());
        client.write_all(&gss_request).await.unwrap();
        let mut response = [0_u8; 1];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, *b"N");

        client.write_all(&startup_packet()).await.unwrap();
        assert_eq!(read_until_ready(&mut client).await.last().unwrap().0, b'Z');
        let core = Arc::clone(wire.connection().unwrap());
        finish_wire_server(&mut client, server).await;
        assert_eq!(core.state().await, SessionState::Closed);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn startup_rejections_are_fixed_fatal_and_a_later_connection_recovers() {
        let (_temp, engine) = engine(2).await;
        let adapter = Adapter::new(engine.clone());
        let long_user = "a".repeat(MAX_STARTUP_NAME_BYTES + 1);
        let long_application_name = "a".repeat(MAX_STARTUP_NAME_BYTES + 1);
        let malformed_startup = vec![0, 0, 0, 12, 0, 3, 0, 0, b'u', b's', b'e', b'r'];
        let mut malformed_then_pipelined = malformed_startup.clone();
        malformed_then_pipelined.extend_from_slice(&typed_packet(b'X', &[]));
        let duplicate_user = startup_packet_with(
            196_608,
            &[
                ("user", "first_user"),
                ("user", "second_user"),
                ("database", "default"),
            ],
        );
        let mut invalid_utf8 = startup_packet();
        let user_value = invalid_utf8
            .windows(b"briskdb".len())
            .position(|window| window == b"briskdb")
            .unwrap();
        invalid_utf8[user_value] = 0xff;
        let cases = vec![
            (
                malformed_startup,
                "08P01",
                "the PostgreSQL protocol message is invalid",
            ),
            (
                malformed_then_pipelined,
                "08P01",
                "the PostgreSQL protocol message is invalid",
            ),
            (
                duplicate_user,
                "08P01",
                "the PostgreSQL protocol message is invalid",
            ),
            (
                invalid_utf8,
                "08P01",
                "the PostgreSQL protocol message is invalid",
            ),
            (
                startup_packet_with(131_072, &[("user", "client_user"), ("database", "default")]),
                "08P01",
                "the PostgreSQL protocol message is invalid",
            ),
            (
                startup_packet_with(196_608, &[("database", "default")]),
                "28000",
                "a valid PostgreSQL user label is required",
            ),
            (
                startup_packet_with(196_608, &[("user", "Uppercase"), ("database", "default")]),
                "28000",
                "a valid PostgreSQL user label is required",
            ),
            (
                startup_packet_with(196_608, &[("user", &long_user), ("database", "default")]),
                "28000",
                "a valid PostgreSQL user label is required",
            ),
            (
                startup_packet_with(196_608, &[("user", "client_user"), ("database", "missing")]),
                "3D000",
                "PostgreSQL database selection is unavailable",
            ),
            (
                startup_packet_with(196_608, &[("user", "client_user"), ("database", "")]),
                "3D000",
                "PostgreSQL database selection is unavailable",
            ),
            (
                startup_packet_with(196_608, &[("user", "client_user")]),
                "3D000",
                "PostgreSQL database selection is unavailable",
            ),
            (
                startup_packet_with(
                    196_608,
                    &[
                        ("user", "client_user"),
                        ("database", "default"),
                        ("client_encoding", "LATIN1"),
                    ],
                ),
                "22023",
                "PostgreSQL client encoding must be UTF8",
            ),
            (
                startup_packet_with(
                    196_608,
                    &[
                        ("user", "client_user"),
                        ("database", "default"),
                        ("application_name", "line\nbreak"),
                    ],
                ),
                "22023",
                "the PostgreSQL application name is invalid",
            ),
            (
                startup_packet_with(
                    196_608,
                    &[
                        ("user", "client_user"),
                        ("database", "default"),
                        ("application_name", &long_application_name),
                    ],
                ),
                "22023",
                "the PostgreSQL application name is invalid",
            ),
            (
                startup_packet_with(
                    196_608,
                    &[
                        ("user", "client_user"),
                        ("database", "default"),
                        ("options", "private-startup-value"),
                    ],
                ),
                "0A000",
                "the PostgreSQL startup parameter is unsupported",
            ),
            (
                startup_packet_with(
                    196_608,
                    &[
                        ("user", "client_user"),
                        ("database", "default"),
                        ("replication", "database"),
                    ],
                ),
                "0A000",
                "PostgreSQL replication startup is unsupported",
            ),
            (
                startup_packet_with(196_610, &[("user", "client_user"), ("database", "default")]),
                "08P01",
                "the PostgreSQL protocol version is unsupported",
            ),
        ];

        for (packet, expected_code, expected_message) in cases {
            let (address, wire, server) = spawn_wire_server(&adapter).await;
            let mut client = TcpStream::connect(address).await.unwrap();
            client.write_all(&packet).await.unwrap();
            let frame = read_frame(&mut client).await;
            assert_eq!(frame.0, b'E');
            let fields = message_fields(&frame.1);
            assert_eq!(fields.get(&b'S').map(String::as_str), Some("FATAL"));
            assert_eq!(fields.get(&b'C').map(String::as_str), Some(expected_code));
            assert_eq!(
                fields.get(&b'M').map(String::as_str),
                Some(expected_message)
            );
            assert!(!fields.get(&b'M').unwrap().contains("private-startup-value"));
            assert_closed_after_invalid_input(&mut client).await;
            tokio::time::timeout(Duration::from_secs(2), server)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(wire.connection().is_none());
        }

        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(&startup_packet_with(196_608, &[("user", "default")]))
            .await
            .unwrap();
        assert_eq!(read_until_ready(&mut client).await.last().unwrap().0, b'Z');
        let core = Arc::clone(wire.connection().unwrap());
        assert_eq!(core.user(), Some("default"));
        assert_eq!(core.database(), "default");
        finish_wire_server(&mut client, server).await;
        assert_eq!(core.state().await, SessionState::Closed);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn startup_status_failure_is_fatal_and_closes_the_selected_session() {
        let (_temp, engine) = engine(2).await;
        let adapter = Adapter::new(engine.clone());
        engine.begin_shutdown();
        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();

        let frame = read_frame(&mut client).await;
        assert_eq!(frame.0, b'E');
        let fields = message_fields(&frame.1);
        let expected = postgres_error(crate::core::EngineErrorKind::ShuttingDown);
        assert_eq!(fields.get(&b'S').map(String::as_str), Some("FATAL"));
        assert_eq!(
            fields.get(&b'C').map(String::as_str),
            Some(expected.sqlstate)
        );
        assert_eq!(
            fields.get(&b'M').map(String::as_str),
            Some(expected.message)
        );
        assert_eof(&mut client).await;
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            wire.connection().unwrap().state().await,
            SessionState::Closed
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn simple_query_executes_registered_writes_and_reads_and_recovers_from_errors() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = crate::core::Database::open(temp.path(), 2).unwrap();
        database
            .broadcast(
                "CREATE TABLE records (
                    tenant_id TEXT NOT NULL PRIMARY KEY,
                    payload TEXT NOT NULL
                 )",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                crate::core::TableDeclaration::sharded(
                    logical_database,
                    "records",
                    crate::core::ShardKeyMetadata::new(
                        "tenant_id",
                        crate::core::ShardKeyType::Text,
                    )
                    .unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
        let engine = Engine::from_database(Arc::new(database));
        let adapter = Adapter::new(engine.clone());
        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();
        read_until_ready(&mut client).await;
        let core = Arc::clone(wire.connection().unwrap());

        client
            .write_all(&typed_packet(
                b'Q',
                b"INSERT INTO records (tenant_id, payload) VALUES ('tenant-a', 'hello')\0",
            ))
            .await
            .unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'C', b'Z']
        );
        assert_eq!(command_tag(&frames[0].1), "INSERT 0 1");

        let duplicate = core
            .execute_simple_query(
                "INSERT INTO records (tenant_id, payload) VALUES ('tenant-a', 'duplicate')",
            )
            .await
            .unwrap_err();
        assert_eq!(duplicate.kind(), EngineErrorKind::UniqueViolation);
        for _ in 0..130 {
            core.execute_simple_query("SELECT 1").await.unwrap();
        }

        client
            .write_all(&typed_packet(
                b'Q',
                b"SELECT tenant_id, payload FROM records WHERE tenant_id = 'tenant-a'\0",
            ))
            .await
            .unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'T', b'D', b'C', b'Z']
        );
        assert_eq!(
            row_description(&frames[0].1),
            [
                ("tenant_id".to_owned(), Type::TEXT.oid()),
                ("payload".to_owned(), Type::TEXT.oid())
            ]
        );
        assert_eq!(
            data_row(&frames[1].1),
            [Some(b"tenant-a".to_vec()), Some(b"hello".to_vec())]
        );
        assert_eq!(command_tag(&frames[2].1), "SELECT 1");

        client
            .write_all(&typed_packet(
                b'Q',
                b"UPDATE records SET payload = 'updated' WHERE tenant_id = 'tenant-a'\0",
            ))
            .await
            .unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'C', b'Z']
        );
        assert_eq!(command_tag(&frames[0].1), "UPDATE 1");

        client
            .write_all(&typed_packet(
                b'Q',
                b"DELETE FROM records WHERE tenant_id = 'tenant-a'\0",
            ))
            .await
            .unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'C', b'Z']
        );
        assert_eq!(command_tag(&frames[0].1), "DELETE 1");

        client
            .write_all(&typed_packet(
                b'Q',
                b"SELECT 'private query text'; SELECT 2\0",
            ))
            .await
            .unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'E', b'Z']
        );
        let fields = message_fields(&frames[0].1);
        assert_eq!(fields.get(&b'S').map(String::as_str), Some("ERROR"));
        assert!(!fields.get(&b'M').unwrap().contains("private query text"));

        client.write_all(&typed_packet(b'Q', b"\0")).await.unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'I', b'Z']
        );
        assert_eq!(core.status().await.unwrap().shard_count(), 2);

        finish_wire_server(&mut client, server).await;
        assert_eq!(core.state().await, SessionState::Closed);
        engine.shutdown().await.unwrap();
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn extended_query_remains_blocked_when_simple_writes_are_enabled() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = crate::core::Database::open(temp.path(), 2).unwrap();
        database
            .broadcast(
                "CREATE TABLE records (
                    tenant_id TEXT NOT NULL PRIMARY KEY,
                    payload TEXT NOT NULL
                 )",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                crate::core::TableDeclaration::sharded(
                    logical_database,
                    "records",
                    crate::core::ShardKeyMetadata::new(
                        "tenant_id",
                        crate::core::ShardKeyType::Text,
                    )
                    .unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
        let database = Arc::new(database);
        let options = crate::core::EngineOptions::new(2, 16)
            .unwrap()
            .with_experimental_vtab_writes(true);
        let engine = Engine::from_database_with_options(Arc::clone(&database), options).unwrap();
        let adapter = Adapter::new(engine.clone());
        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();
        read_until_ready(&mut client).await;
        let expected = postgres_error(EngineErrorKind::Unsupported);

        for sql in ["BEGIN", "COMMIT", "ROLLBACK"] {
            let mut body = sql.as_bytes().to_vec();
            body.push(0);
            client.write_all(&typed_packet(b'Q', &body)).await.unwrap();
            let frames = read_until_ready(&mut client).await;
            assert_eq!(frames.len(), 2, "simple-query response for {sql}");
            let fields = message_fields(&frames[0].1);
            assert_eq!(fields.get(&b'S').map(String::as_str), Some("ERROR"));
            assert_eq!(
                fields.get(&b'C').map(String::as_str),
                Some(expected.sqlstate)
            );
            assert_eq!(
                fields.get(&b'M').map(String::as_str),
                Some(expected.message)
            );
            assert!(!fields.get(&b'M').unwrap().contains(sql));
            assert_eq!(frames[1], (b'Z', vec![b'I']));
        }

        let extended_sql = "INSERT INTO records (tenant_id, payload) VALUES ('extended-blocked', 'must-not-be-stored')";
        let mut parse = Vec::new();
        parse.push(0);
        parse.extend_from_slice(extended_sql.as_bytes());
        parse.push(0);
        parse.extend_from_slice(&0_u16.to_be_bytes());
        client.write_all(&typed_packet(b'P', &parse)).await.unwrap();
        let error = read_frame(&mut client).await;
        assert_eq!(error.0, b'E');
        let fields = message_fields(&error.1);
        assert_eq!(
            fields.get(&b'C').map(String::as_str),
            Some(expected.sqlstate)
        );
        assert_eq!(
            fields.get(&b'M').map(String::as_str),
            Some(expected.message)
        );
        assert!(!fields.get(&b'M').unwrap().contains(extended_sql));
        client.write_all(&typed_packet(b'S', &[])).await.unwrap();
        assert_eq!(read_frame(&mut client).await, (b'Z', vec![b'I']));

        for shard in 0..database.shard_count() {
            let rows =
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap()
                    .query_row("SELECT COUNT(*) FROM records", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap();
            assert_eq!(
                rows, 0,
                "extended PostgreSQL query mutated physical shard {shard}"
            );
        }

        finish_wire_server(&mut client, server).await;
        assert_eq!(
            wire.connection().unwrap().state().await,
            SessionState::Closed
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn extended_query_rejects_before_storage_and_sync_recovers() {
        let (_temp, engine) = engine(2).await;
        let adapter = Adapter::new(engine.clone());
        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();
        read_until_ready(&mut client).await;
        let core = Arc::clone(wire.connection().unwrap());

        let mut parse = Vec::new();
        parse.push(0);
        parse.extend_from_slice(b"SELECT 'private extended text'\0");
        parse.extend_from_slice(&0_u16.to_be_bytes());
        client.write_all(&typed_packet(b'P', &parse)).await.unwrap();
        let error = read_frame(&mut client).await;
        assert_eq!(error.0, b'E');
        let fields = message_fields(&error.1);
        assert_eq!(fields.get(&b'C').map(String::as_str), Some("0A000"));
        assert_eq!(
            fields.get(&b'M').map(String::as_str),
            Some(postgres_error(crate::core::EngineErrorKind::Unsupported).message)
        );
        assert!(!fields.get(&b'M').unwrap().contains("private extended text"));

        client.write_all(&typed_packet(b'S', &[])).await.unwrap();
        assert_eq!(read_frame(&mut client).await, (b'Z', vec![b'I']));
        assert_eq!(core.status().await.unwrap().shard_count(), 2);

        finish_wire_server(&mut client, server).await;
        assert_eq!(core.state().await, SessionState::Closed);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn malformed_message_after_startup_is_fatal_and_closes_the_core_session() {
        let (_temp, engine) = engine(2).await;
        let adapter = Adapter::new(engine.clone());
        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();
        read_until_ready(&mut client).await;
        let core = Arc::clone(wire.connection().unwrap());

        // Parse requires two C strings and a u16 OID count. This complete
        // frame deliberately ends before the count.
        client
            .write_all(&typed_packet(b'P', &[0, 0]))
            .await
            .unwrap();
        let error = read_frame(&mut client).await;
        assert_eq!(error.0, b'E');
        let fields = message_fields(&error.1);
        assert_eq!(fields.get(&b'S').map(String::as_str), Some("FATAL"));
        assert_eq!(fields.get(&b'C').map(String::as_str), Some("08P01"));
        assert_eof(&mut client).await;
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(core.state().await, SessionState::Closed);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn oversized_frame_headers_fail_without_waiting_for_the_declared_body() {
        let (_temp, engine) = engine(2).await;
        let adapter = Adapter::new(engine.clone());

        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        let mut oversized_startup_header = Vec::new();
        oversized_startup_header.extend_from_slice(
            &u32::try_from(MAX_STARTUP_PACKET_LENGTH + 1)
                .unwrap()
                .to_be_bytes(),
        );
        client.write_all(&oversized_startup_header).await.unwrap();
        assert_protocol_fatal(&mut client).await;
        assert_eof(&mut client).await;
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(wire.connection().is_none());

        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();
        read_until_ready(&mut client).await;
        let core = Arc::clone(wire.connection().unwrap());
        let mut oversized_query_header = vec![b'Q'];
        oversized_query_header.extend_from_slice(
            &u32::try_from(MAX_FRONTEND_MESSAGE_LENGTH + 1)
                .unwrap()
                .to_be_bytes(),
        );
        client.write_all(&oversized_query_header).await.unwrap();
        assert_protocol_fatal(&mut client).await;
        assert_eof(&mut client).await;
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(core.state().await, SessionState::Closed);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn eof_and_server_shutdown_both_close_the_selected_core_session() {
        let (_temp, engine) = engine(2).await;
        let adapter = Adapter::new(engine.clone());

        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();
        read_until_ready(&mut client).await;
        let eof_core = Arc::clone(wire.connection().unwrap());
        drop(client);
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(eof_core.state().await, SessionState::Closed);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let wire = adapter.wire_connection();
        let served = wire.clone();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            served
                .serve(stream, async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();
        read_until_ready(&mut client).await;
        let shutdown_core = Arc::clone(wire.connection().unwrap());
        shutdown_tx.send(()).unwrap();
        assert_eof(&mut client).await;
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(shutdown_core.state().await, SessionState::Closed);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn incomplete_startup_closes_at_the_configured_timeout_without_a_session() {
        assert_eq!(STARTUP_TIMEOUT, Duration::from_secs(60));
        let (_temp, engine) = engine(2).await;
        let adapter = Adapter::new(engine.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let wire = adapter.wire_connection();
        let served = wire.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            served
                .serve_with_startup_timeout(
                    stream,
                    std::future::pending(),
                    Duration::from_millis(20),
                )
                .await
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        assert_eof(&mut client).await;
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(wire.connection().is_none());
        engine.shutdown().await.unwrap();
    }

    #[test]
    fn raw_frame_validators_enforce_size_structure_boundaries_and_utf8() {
        let startup = startup_packet();
        assert_eq!(
            validate_startup_frame(&startup).unwrap(),
            StartupFrameKind::Startup
        );

        for code in [SSL_REQUEST_CODE, GSSENC_REQUEST_CODE] {
            let mut negotiation = Vec::new();
            negotiation.extend_from_slice(&8_u32.to_be_bytes());
            negotiation.extend_from_slice(&(code as u32).to_be_bytes());
            assert_eq!(
                validate_startup_frame(&negotiation).unwrap(),
                StartupFrameKind::Negotiation
            );

            negotiation[..4].copy_from_slice(&100_u32.to_be_bytes());
            assert!(validate_startup_frame(&negotiation).is_err());
        }

        let maximum_value = "a".repeat(MAX_STARTUP_PACKET_LENGTH - 12);
        let maximum_startup = startup_packet_with(196_608, &[("x", &maximum_value)]);
        assert_eq!(maximum_startup.len(), MAX_STARTUP_PACKET_LENGTH);
        assert_eq!(
            validate_startup_frame(&maximum_startup).unwrap(),
            StartupFrameKind::Startup
        );

        let mut oversized_startup = maximum_startup.clone();
        oversized_startup.insert(oversized_startup.len() - 1, b'a');
        let oversized_length = u32::try_from(oversized_startup.len()).unwrap();
        oversized_startup[..4].copy_from_slice(&oversized_length.to_be_bytes());
        assert!(validate_startup_frame(&oversized_startup).is_err());

        let mut missing_terminal = startup.clone();
        missing_terminal.pop();
        let missing_terminal_length = u32::try_from(missing_terminal.len()).unwrap();
        missing_terminal[..4].copy_from_slice(&missing_terminal_length.to_be_bytes());

        let mut trailing_after_terminal = startup.clone();
        trailing_after_terminal.push(b'x');
        let trailing_length = u32::try_from(trailing_after_terminal.len()).unwrap();
        trailing_after_terminal[..4].copy_from_slice(&trailing_length.to_be_bytes());

        let duplicate = startup_packet_with(
            196_608,
            &[
                ("user", "first"),
                ("user", "second"),
                ("database", "default"),
            ],
        );
        let missing_value = vec![0, 0, 0, 14, 0, 3, 0, 0, b'u', b's', b'e', b'r', 0, 0];
        let mut invalid_utf8 = startup.clone();
        let user_value = invalid_utf8
            .windows(b"briskdb".len())
            .position(|window| window == b"briskdb")
            .unwrap();
        invalid_utf8[user_value] = 0xff;
        let mut wrong_declared_length = startup.clone();
        wrong_declared_length[..4].copy_from_slice(&8_u32.to_be_bytes());
        let mut cancel = Vec::new();
        cancel.extend_from_slice(&16_u32.to_be_bytes());
        cancel.extend_from_slice(&(CANCEL_REQUEST_CODE as u32).to_be_bytes());
        cancel.extend_from_slice(&1_u32.to_be_bytes());
        cancel.extend_from_slice(&2_u32.to_be_bytes());

        for rejected in [
            missing_terminal,
            trailing_after_terminal,
            duplicate,
            missing_value,
            invalid_utf8,
            wrong_declared_length,
            cancel,
        ] {
            assert!(
                validate_startup_frame(&rejected).is_err(),
                "rejected startup frame: {rejected:?}"
            );
        }

        let mut maximum_query_body = vec![b'a'; MAX_PARSED_SQL_BYTES];
        maximum_query_body.push(0);
        let maximum_query = typed_packet(b'Q', &maximum_query_body);
        assert_eq!(
            u32::from_be_bytes(maximum_query[1..5].try_into().unwrap()) as usize,
            MAX_FRONTEND_MESSAGE_LENGTH
        );
        validate_typed_frame(&maximum_query).unwrap();
        validate_typed_frame(&typed_packet(b'Q', &[0])).unwrap();

        let mut parse = Vec::new();
        parse.extend_from_slice(b"statement\0SELECT 1\0");
        parse.extend_from_slice(&1_u16.to_be_bytes());
        parse.extend_from_slice(&23_u32.to_be_bytes());
        validate_typed_frame(&typed_packet(b'P', &parse)).unwrap();
        validate_typed_frame(&typed_packet(b'S', &[])).unwrap();
        validate_typed_frame(&typed_packet(b'X', &[])).unwrap();

        let mut oversized_query_body = vec![b'a'; MAX_PARSED_SQL_BYTES + 1];
        oversized_query_body.push(0);
        let invalid_typed_frames = [
            typed_packet(b'Q', b"SELECT 1"),
            typed_packet(b'Q', b"SELECT\0trailing\0"),
            typed_packet(b'Q', &[0xff, 0]),
            typed_packet(b'Q', &oversized_query_body),
            typed_packet(b'P', b"statement\0SELECT 1\0"),
            typed_packet(b'P', b"statement\0SELECT 1\0\0\x01"),
            typed_packet(b'P', b"statement\0SELECT 1\0\0\x01\0\0\0"),
            typed_packet(b'S', &[0]),
            typed_packet(b'X', &[0]),
            typed_packet(b'B', &[]),
        ];
        for rejected in invalid_typed_frames {
            assert!(
                validate_typed_frame(&rejected).is_err(),
                "rejected typed frame: {rejected:?}"
            );
        }

        let mut wrong_typed_length = typed_packet(b'Q', b"SELECT 1\0");
        wrong_typed_length[1..5].copy_from_slice(&4_u32.to_be_bytes());
        assert!(validate_typed_frame(&wrong_typed_length).is_err());
    }

    #[tokio::test]
    async fn raw_frame_guard_handles_fragmented_buffered_and_partial_input() {
        let startup = startup_packet();
        let terminate = typed_packet(b'X', &[]);
        let mut expected = startup.clone();
        expected.extend_from_slice(&terminate);

        let (mut writer, reader) = tokio::io::duplex(1);
        let fragmented = expected.clone();
        let writing = tokio::spawn(async move {
            writer.write_all(&fragmented).await.unwrap();
        });
        let mut guarded = GuardedPgStream::new(reader, Vec::new());
        let mut received = vec![0; expected.len()];
        guarded.read_exact(&mut received).await.unwrap();
        assert_eq!(received, expected);
        writing.await.unwrap();
        assert_eq!(guarded.read(&mut [0]).await.unwrap(), 0);

        let (writer, reader) = tokio::io::duplex(1);
        drop(writer);
        let mut guarded = GuardedPgStream::new(reader, expected.clone());
        let mut received = vec![0; expected.len()];
        guarded.read_exact(&mut received).await.unwrap();
        assert_eq!(received, expected);
        assert_eq!(guarded.read(&mut [0]).await.unwrap(), 0);

        let (mut writer, reader) = tokio::io::duplex(8);
        writer.write_all(&startup[..8]).await.unwrap();
        writer.shutdown().await.unwrap();
        let mut guarded = GuardedPgStream::new(reader, Vec::new());
        assert_eq!(
            guarded.read(&mut [0]).await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn startup_identity_validators_have_exact_byte_and_character_boundaries() {
        let max_user = "a".repeat(MAX_STARTUP_NAME_BYTES);
        for user in ["a", "_", "a0", "_0", max_user.as_str()] {
            assert!(valid_user_label(user), "accepted user boundary: {user:?}");
        }
        let too_long_user = "a".repeat(MAX_STARTUP_NAME_BYTES + 1);
        for user in [
            "",
            "0user",
            "Uppercase",
            "non_ascii_é",
            "a-b",
            &too_long_user,
        ] {
            assert!(!valid_user_label(user), "rejected user boundary: {user:?}");
        }

        let max_utf8_application = format!("{}a", "é".repeat(31));
        assert_eq!(max_utf8_application.len(), MAX_STARTUP_NAME_BYTES);
        for application in ["", "client", max_utf8_application.as_str()] {
            assert!(
                valid_application_name(application),
                "accepted application-name boundary: {application:?}"
            );
        }
        let too_long_utf8_application = "é".repeat(32);
        assert_eq!(too_long_utf8_application.len(), MAX_STARTUP_NAME_BYTES + 1);
        for application in [
            "line\nbreak",
            "replacement_\u{fffd}",
            too_long_utf8_application.as_str(),
        ] {
            assert!(
                !valid_application_name(application),
                "rejected application-name boundary: {application:?}"
            );
        }
    }

    #[tokio::test]
    async fn adapter_connections_are_independent_and_close_idempotently() {
        let (_temp, engine) = engine(3).await;
        let adapter = Adapter::new(engine.clone());
        let first = Arc::new(adapter.open_connection());
        let second = Arc::new(adapter.open_connection());

        assert_ne!(first.session_id(), second.session_id());
        let (first_status, second_status) = tokio::join!(first.status(), second.status());
        assert_eq!(first_status.unwrap().shard_count(), 3);
        assert_eq!(second_status.unwrap().shard_count(), 3);

        first.close().await.unwrap();
        first.close().await.unwrap();
        assert_eq!(first.state().await, SessionState::Closed);
        assert_eq!(
            first.status().await.unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(second.status().await.unwrap().shard_count(), 3);

        second.close().await.unwrap();
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn selected_query_parser_prepares_through_core_and_rejects_all_oid_lists() {
        let (_temp, engine) = engine(2).await;
        let adapter = Adapter::new(engine.clone());
        let connection = adapter.open_connection();
        let client = test_client();

        let prepared = connection
            .wire_parser
            .parse_sql(&client, "SELECT 1", &[])
            .await
            .unwrap();
        assert_eq!(prepared.description.parameter_types(), []);
        assert_eq!(prepared.description.columns().len(), 1);
        assert!(
            connection
                .state
                .engine
                .close_prepared_statement(&connection.state.session, prepared.id)
                .await
                .unwrap()
        );

        let inferred_or_unknown = connection
            .wire_parser
            .parse_sql(&client, "SELECT $1", &[None])
            .await
            .unwrap_err();
        let PgWireError::UserError(info) = inferred_or_unknown else {
            panic!("expected a BriskDB-owned PostgreSQL error")
        };
        assert_eq!(info.code, "0A000");
        assert_eq!(
            info.message,
            postgres_error(EngineErrorKind::Unsupported).message
        );

        let recognized = connection
            .wire_parser
            .parse_sql(&client, "SELECT $1", &[Some(Type::INT8)])
            .await
            .unwrap_err();
        let PgWireError::UserError(info) = recognized else {
            panic!("expected a BriskDB-owned PostgreSQL error")
        };
        assert_eq!(info.code, "0A000");
        assert_eq!(
            info.message,
            postgres_error(EngineErrorKind::Unsupported).message
        );

        let invalid = connection
            .wire_parser
            .parse_sql(&client, "SELECT 'private adapter literal' +", &[])
            .await
            .unwrap_err();
        let PgWireError::UserError(info) = invalid else {
            panic!("expected a classified parser error")
        };
        assert_eq!(info.code, "42000");
        assert_eq!(
            info.message,
            postgres_error(EngineErrorKind::InvalidQuery).message
        );
        assert!(!info.message.contains("private adapter literal"));
        assert_eq!(connection.status().await.unwrap().shard_count(), 2);

        connection.close().await.unwrap();
        engine.shutdown().await.unwrap();
    }

    #[test]
    fn every_engine_error_uses_the_fixed_postgres_mapping() {
        for kind in EngineErrorKind::ALL.iter().copied() {
            let wire = engine_error_to_pgwire(EngineError::new(
                kind,
                "private SQL, path, and SQLite diagnostic",
            ));
            let PgWireError::UserError(info) = wire else {
                panic!("{kind:?} did not produce a user error")
            };
            let expected = postgres_error(kind);
            assert_eq!(info.severity, "ERROR");
            assert_eq!(info.code, expected.sqlstate);
            assert_eq!(info.message, expected.message);
            assert!(!info.message.contains("private"));
            assert!(info.detail.is_none());
        }
    }

    #[tokio::test]
    async fn historical_pgwire_entrypoint_probe_remains_isolated_from_production_startup() {
        let (_temp, engine) = engine(3).await;
        let adapter = Adapter::new(engine.clone());
        let connection = Arc::new(adapter.open_connection());
        let observed = Arc::clone(&connection);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address: SocketAddr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let factory = ProbeFactory {
                handler: Arc::new(ProbeHandler {
                    connection: Arc::clone(&connection),
                }),
            };
            let result = process_socket(socket, None, factory).await;
            connection.close().await.unwrap();
            result
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();
        let startup = read_until_ready(&mut client).await;
        assert!(startup.iter().any(|frame| frame.0 == b'R'));
        assert!(startup.iter().any(|frame| frame.0 == b'S'));
        assert!(startup.iter().any(|frame| frame.0 == b'K'));
        assert_eq!(startup.last().unwrap().0, b'Z');

        client
            .write_all(&typed_packet(b'Q', b"BRISKDB SPIKE\0"))
            .await
            .unwrap();
        let query = read_until_ready(&mut client).await;
        let command = query.iter().find(|frame| frame.0 == b'C').unwrap();
        assert_eq!(command.1, b"BRISKDB SPIKE 3\0");
        assert_eq!(query.last().unwrap().0, b'Z');

        client.write_all(&typed_packet(b'X', &[])).await.unwrap();
        drop(client);
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("the pgwire probe returned after client EOF")
            .unwrap()
            .unwrap();
        assert_eq!(observed.state().await, SessionState::Closed);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rejected_startup_cleans_up_and_a_later_connection_remains_usable() {
        let (_temp, engine) = engine(2).await;
        let adapter = Adapter::new(engine.clone());
        let connection = Arc::new(adapter.open_connection());
        let observed = Arc::clone(&connection);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let factory = ProbeFactory {
                handler: Arc::new(ProbeHandler {
                    connection: Arc::clone(&connection),
                }),
            };
            let result = process_socket(socket, None, factory).await;
            connection.close().await.unwrap();
            result
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        let mut unsupported = startup_packet();
        unsupported[4..8].copy_from_slice(&0x0009_0009_u32.to_be_bytes());
        client.write_all(&unsupported).await.unwrap();
        let mut byte = [0_u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), client.read(&mut byte))
            .await
            .expect("the rejected startup closed promptly")
            .unwrap();
        assert_eq!(read, 0);
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("the rejected startup task returned")
            .unwrap()
            .unwrap();
        assert_eq!(observed.state().await, SessionState::Closed);

        let recovered = adapter.open_connection();
        assert_eq!(recovered.status().await.unwrap().shard_count(), 2);
        recovered.close().await.unwrap();
        engine.shutdown().await.unwrap();
    }

    impl Connection {
        async fn state(&self) -> SessionState {
            self.state.session.state().await
        }
    }
}
