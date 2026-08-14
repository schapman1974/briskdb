#![cfg_attr(not(feature = "listeners"), allow(dead_code))]

//! BriskDB-owned boundary for the selected PostgreSQL wire library.
//!
//! The configured loopback listener delegates startup framing and dispatch to
//! this module. Each successfully started wire connection owns one
//! protocol-neutral core session; `server`, `core`, and BriskDB's public API do
//! not accept or return `pgwire` types.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    fs::{self, File},
    io::{self, BufReader},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use futures::{Sink, SinkExt, StreamExt, stream};
use pgwire::{
    api::{
        ClientInfo, ClientPortalStore, DEFAULT_NAME, DefaultClient, ErrorHandler,
        PgWireConnectionState, PgWireServerHandlers, Type,
        auth::{
            AuthSource, LoginInfo, Password, StartupHandler,
            sasl::SASLState,
            sasl::scram::{SCRAM_ITERATIONS, ScramAuth, gen_salted_password},
        },
        cancel::CancelHandler,
        portal::{Portal, PortalExecutionState},
        query::{
            ExtendedQueryHandler, SimpleQueryHandler, send_execution_response,
            send_partial_query_response, send_query_response,
        },
        results::{
            DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
            QueryResponse, Response, Tag,
        },
        stmt::{QueryParser, StoredStatement},
        store::PortalStore,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{
        PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion, SslNegotiationMetaMessage,
        cancel::CancelRequest,
        data::{DataRow, NoData},
        extendedquery::{
            Bind, BindComplete, Close, CloseComplete, Execute, Parse, ParseComplete,
            TARGET_TYPE_BYTE_PORTAL, TARGET_TYPE_BYTE_STATEMENT,
        },
        response::{GssEncResponse, ReadyForQuery, SslResponse, TransactionStatus},
        startup::{
            Authentication, BackendKeyData, NegotiateProtocolVersion, ParameterStatus,
            PasswordMessageFamily, SecretKey, Startup,
        },
    },
    tokio::{
        server::{MaybeTls, PgWireMessageServerCodec, process_error, process_message},
        tokio_rustls::{TlsAcceptor, rustls},
    },
};
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    sync::Mutex as AsyncMutex,
};
use tokio_util::codec::Framed;

use crate::{
    core::{
        CancellationToken, DataType, DescribeTarget, Engine, EngineError, EngineResult,
        EngineStatus, LogicalDatabaseId, PortalId, PrepareRequest, PreparedExecution,
        PreparedStatementDescription, PreparedStatementId, RequestContext, ResultSet, Session,
        SessionId, TransactionExecution, Value,
    },
    protocol::error::postgres_error,
    sql::{MAX_PARSED_SQL_BYTES, SqlDialect, SqlTranslationMode, StatementBehavior, WriteBehavior},
};

const POSTGRES_PROTOCOL_MAJOR: u16 = 3;
const POSTGRES_PROTOCOL_MINOR: u16 = 0;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_STARTUP_NAME_BYTES: usize = 63;
const MAX_EXTENDED_NAME_BYTES: usize = 63;
const MAX_STARTUP_PACKET_LENGTH: usize = 10_000;
const MAX_FRONTEND_MESSAGE_LENGTH: usize = MAX_PARSED_SQL_BYTES + 5;
const MAX_TLS_PEM_BYTES: u64 = 1_048_576;
const MAX_PASSWORD_FILE_BYTES: u64 = 1_026;
const CANCEL_REQUEST_CODE: i32 = 80_877_102;
const SSL_REQUEST_CODE: i32 = 80_877_103;
const GSSENC_REQUEST_CODE: i32 = 80_877_104;
const POSTGRES_COMPATIBILITY_VERSION_NUM: &str = "140000";
const SERVER_VERSION: &str = "14.0-briskdb";
const BRISKDB_VERSION: &str = env!("CARGO_PKG_VERSION");
const VERSION_BANNER: &str = concat!(
    "PostgreSQL 14.0 (BriskDB ",
    env!("CARGO_PKG_VERSION"),
    " compatibility layer)"
);
const PARAMETER_STATUS: [(&str, &str); 6] = [
    ("server_version", SERVER_VERSION),
    ("briskdb_version", BRISKDB_VERSION),
    ("server_encoding", "UTF8"),
    ("client_encoding", "UTF8"),
    ("standard_conforming_strings", "on"),
    ("integer_datetimes", "on"),
];

/// Files and identity used by a TLS-protected PostgreSQL listener.
///
/// The password is read from `password_file`, never from a command-line value,
/// and is converted to a salted SCRAM secret while the listener is prepared.
/// BriskDB currently supports one authenticated identity; authorization and a
/// durable role catalog remain separate roadmap work.
#[derive(Clone, PartialEq, Eq)]
pub struct SecurityConfig {
    certificate: PathBuf,
    private_key: PathBuf,
    user: Box<str>,
    password_file: PathBuf,
}

impl SecurityConfig {
    pub fn new(
        certificate: impl Into<PathBuf>,
        private_key: impl Into<PathBuf>,
        user: impl Into<Box<str>>,
        password_file: impl Into<PathBuf>,
    ) -> io::Result<Self> {
        let user = user.into();
        if !valid_user_label(&user) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "PostgreSQL authentication user must be a valid lowercase user label",
            ));
        }
        Ok(Self {
            certificate: certificate.into(),
            private_key: private_key.into(),
            user,
            password_file: password_file.into(),
        })
    }

    pub fn certificate(&self) -> &Path {
        &self.certificate
    }

    pub fn private_key(&self) -> &Path {
        &self.private_key
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub fn password_file(&self) -> &Path {
        &self.password_file
    }

    pub(crate) fn load(&self) -> io::Result<LoadedSecurity> {
        validate_regular_file(
            &self.certificate,
            "PostgreSQL TLS certificate",
            MAX_TLS_PEM_BYTES,
        )?;
        validate_regular_file(
            &self.private_key,
            "PostgreSQL TLS private key",
            MAX_TLS_PEM_BYTES,
        )?;
        validate_regular_file(
            &self.password_file,
            "PostgreSQL password file",
            MAX_PASSWORD_FILE_BYTES,
        )?;
        validate_private_file(&self.private_key, "PostgreSQL TLS private key")?;
        validate_private_file(&self.password_file, "PostgreSQL password file")?;

        let certificate_pem = fs::read(&self.certificate).map_err(|error| {
            contextual_io_error(
                error,
                format!(
                    "failed to read PostgreSQL TLS certificate {}",
                    self.certificate.display()
                ),
            )
        })?;
        let certificates = rustls_pemfile::certs(&mut certificate_pem.as_slice())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                contextual_io_error(
                    error,
                    format!(
                        "failed to parse PostgreSQL TLS certificate {}",
                        self.certificate.display()
                    ),
                )
            })?;
        if certificates.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "PostgreSQL TLS certificate {} contains no certificates",
                    self.certificate.display()
                ),
            ));
        }

        let key_file = File::open(&self.private_key).map_err(|error| {
            contextual_io_error(
                error,
                format!(
                    "failed to read PostgreSQL TLS private key {}",
                    self.private_key.display()
                ),
            )
        })?;
        let private_key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
            .map_err(|error| {
                contextual_io_error(
                    error,
                    format!(
                        "failed to parse PostgreSQL TLS private key {}",
                        self.private_key.display()
                    ),
                )
            })?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "PostgreSQL TLS private key {} contains no supported private key",
                        self.private_key.display()
                    ),
                )
            })?;
        let mut tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("PostgreSQL TLS certificate and private key are invalid: {error}"),
                )
            })?;
        tls.alpn_protocols = vec![b"postgresql".to_vec()];

        let mut password = fs::read(&self.password_file).map_err(|error| {
            contextual_io_error(
                error,
                format!(
                    "failed to read PostgreSQL password file {}",
                    self.password_file.display()
                ),
            )
        })?;
        if password.last() == Some(&b'\n') {
            password.pop();
            if password.last() == Some(&b'\r') {
                password.pop();
            }
        }
        if password.is_empty()
            || password.len() > 1_024
            || password.contains(&0)
            || password.contains(&b'\n')
            || password.contains(&b'\r')
        {
            password.fill(0);
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "PostgreSQL password file must contain one non-empty UTF-8 line of at most 1024 bytes",
            ));
        }
        if std::str::from_utf8(&password).is_err() {
            password.fill(0);
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "PostgreSQL password file must contain valid UTF-8",
            ));
        }
        let mut salt = vec![0_u8; 16];
        if let Err(error) = getrandom::fill(&mut salt) {
            password.fill(0);
            return Err(io::Error::other(format!(
                "failed to generate PostgreSQL SCRAM salt: {error}"
            )));
        }
        let salted_password = gen_salted_password(
            std::str::from_utf8(&password).expect("the password encoding was checked"),
            &salt,
            SCRAM_ITERATIONS,
        );
        password.fill(0);

        let mut fake_salt = vec![0_u8; 16];
        getrandom::fill(&mut fake_salt).map_err(|error| {
            io::Error::other(format!(
                "failed to generate PostgreSQL SCRAM decoy salt: {error}"
            ))
        })?;
        let fake_password = gen_salted_password(
            "briskdb-invalid-authentication-secret",
            &fake_salt,
            SCRAM_ITERATIONS,
        );

        let loaded = LoadedSecurity {
            tls_acceptor: TlsAcceptor::from(Arc::new(tls)),
            certificate_pem: certificate_pem.into(),
            credentials: Arc::new(ScramCredentials {
                user: self.user.clone(),
                salt,
                salted_password,
                fake_salt,
                fake_password,
            }),
        };
        loaded.scram().map_err(io::Error::other)?;
        Ok(loaded)
    }
}

impl fmt::Debug for SecurityConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecurityConfig")
            .field("certificate", &self.certificate)
            .field("private_key", &self.private_key)
            .field("user", &self.user)
            .field("password_file", &self.password_file)
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct LoadedSecurity {
    tls_acceptor: TlsAcceptor,
    certificate_pem: Arc<[u8]>,
    credentials: Arc<ScramCredentials>,
}

impl LoadedSecurity {
    fn scram(&self) -> PgWireResult<ScramAuth> {
        let mut scram = ScramAuth::new(self.credentials.clone());
        scram.set_iterations(SCRAM_ITERATIONS);
        scram.configure_certificate(&self.certificate_pem)?;
        Ok(scram)
    }
}

impl fmt::Debug for LoadedSecurity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoadedSecurity")
            .field("user", &self.credentials.user)
            .finish_non_exhaustive()
    }
}

struct ScramCredentials {
    user: Box<str>,
    salt: Vec<u8>,
    salted_password: Vec<u8>,
    fake_salt: Vec<u8>,
    fake_password: Vec<u8>,
}

impl fmt::Debug for ScramCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScramCredentials")
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

impl Drop for ScramCredentials {
    fn drop(&mut self) {
        self.salted_password.fill(0);
        self.fake_password.fill(0);
    }
}

#[async_trait]
impl AuthSource for ScramCredentials {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let (salt, password) = if login.user() == Some(self.user.as_ref()) {
            (&self.salt, &self.salted_password)
        } else {
            (&self.fake_salt, &self.fake_password)
        };
        Ok(Password::new(Some(salt.clone()), password.clone()))
    }
}

fn contextual_io_error(error: io::Error, message: String) -> io::Error {
    io::Error::new(error.kind(), format!("{message}: {error}"))
}

fn validate_regular_file(path: &Path, label: &str, maximum_bytes: u64) -> io::Result<()> {
    let metadata = fs::metadata(path).map_err(|error| {
        contextual_io_error(
            error,
            format!("failed to inspect {label} {}", path.display()),
        )
    })?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{label} {} is not a regular file", path.display()),
        ));
    }
    if metadata.len() > maximum_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{label} {} exceeds the {maximum_bytes}-byte limit",
                path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_private_file(path: &Path, label: &str) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let metadata = fs::metadata(path).map_err(|error| {
        contextual_io_error(
            error,
            format!("failed to inspect {label} {}", path.display()),
        )
    })?;
    let mode = metadata.mode() & 0o777;
    if mode & 0o037 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{label} {} must not be group-writable or accessible by other users (mode is {mode:03o})",
                path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_private_file(path: &Path, label: &str) -> io::Result<()> {
    let metadata = fs::metadata(path).map_err(|error| {
        contextual_io_error(
            error,
            format!("failed to inspect {label} {}", path.display()),
        )
    })?;
    let _ = metadata;
    Ok(())
}

#[derive(Clone, Copy)]
enum FrontendFramePhase {
    Startup,
    Typed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartupFrameKind {
    Negotiation,
    Cancellation,
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
            if (code == CANCEL_REQUEST_CODE && declared != 16)
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
        return if frame.len() == 16 {
            Ok(StartupFrameKind::Cancellation)
        } else {
            Err(invalid_frontend_frame())
        };
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
        Some(b'B') => validate_bind_frame_body(body),
        Some(b'D') | Some(b'C') => validate_named_target_frame_body(body),
        Some(b'E') => validate_execute_frame_body(body),
        // SASLInitialResponse and SASLResponse share the PasswordMessage tag.
        // The selected decoder validates their state-dependent internals; this
        // gate still bounds the complete frame before admitting it.
        Some(b'p') if !body.is_empty() => Ok(()),
        Some(b'H') | Some(b'S') | Some(b'X') if body.is_empty() => Ok(()),
        _ => Err(invalid_frontend_frame()),
    }
}

fn validate_bind_frame_body(body: &[u8]) -> io::Result<()> {
    let (_, cursor) = consume_utf8_cstring(body, 0).ok_or_else(invalid_frontend_frame)?;
    let (_, mut cursor) = consume_utf8_cstring(body, cursor).ok_or_else(invalid_frontend_frame)?;

    let (format_count, next) = consume_u16(body, cursor)?;
    cursor = next;
    for _ in 0..format_count {
        let (format, next) = consume_i16(body, cursor)?;
        if !matches!(format, 0 | 1) {
            return Err(invalid_frontend_frame());
        }
        cursor = next;
    }

    let (parameter_count, next) = consume_u16(body, cursor)?;
    cursor = next;
    if !matches!(format_count, 0 | 1) && format_count != parameter_count {
        return Err(invalid_frontend_frame());
    }
    for _ in 0..parameter_count {
        let (length, next) = consume_i32(body, cursor)?;
        cursor = next;
        if length < -1 {
            return Err(invalid_frontend_frame());
        }
        if length >= 0 {
            let length = usize::try_from(length).map_err(|_| invalid_frontend_frame())?;
            cursor = cursor
                .checked_add(length)
                .filter(|end| *end <= body.len())
                .ok_or_else(invalid_frontend_frame)?;
        }
    }

    let (result_count, next) = consume_i16(body, cursor)?;
    let result_count = u16::try_from(result_count).map_err(|_| invalid_frontend_frame())?;
    cursor = next;
    for _ in 0..result_count {
        let (format, next) = consume_i16(body, cursor)?;
        if !matches!(format, 0 | 1) {
            return Err(invalid_frontend_frame());
        }
        cursor = next;
    }
    if cursor == body.len() {
        Ok(())
    } else {
        Err(invalid_frontend_frame())
    }
}

fn validate_named_target_frame_body(body: &[u8]) -> io::Result<()> {
    if !matches!(body.first(), Some(b'S' | b'P')) {
        return Err(invalid_frontend_frame());
    }
    let (_, end) = consume_utf8_cstring(body, 1).ok_or_else(invalid_frontend_frame)?;
    if end == body.len() {
        Ok(())
    } else {
        Err(invalid_frontend_frame())
    }
}

fn validate_execute_frame_body(body: &[u8]) -> io::Result<()> {
    let (_, cursor) = consume_utf8_cstring(body, 0).ok_or_else(invalid_frontend_frame)?;
    let (max_rows, end) = consume_i32(body, cursor)?;
    if max_rows >= 0 && end == body.len() {
        Ok(())
    } else {
        Err(invalid_frontend_frame())
    }
}

fn consume_u16(buffer: &[u8], start: usize) -> io::Result<(u16, usize)> {
    let end = start.checked_add(2).ok_or_else(invalid_frontend_frame)?;
    let bytes = buffer.get(start..end).ok_or_else(invalid_frontend_frame)?;
    Ok((
        u16::from_be_bytes(
            bytes
                .try_into()
                .expect("the guarded two-byte field length was checked"),
        ),
        end,
    ))
}

fn consume_i16(buffer: &[u8], start: usize) -> io::Result<(i16, usize)> {
    consume_u16(buffer, start).map(|(value, end)| (value as i16, end))
}

fn consume_i32(buffer: &[u8], start: usize) -> io::Result<(i32, usize)> {
    let end = start.checked_add(4).ok_or_else(invalid_frontend_frame)?;
    let bytes = buffer.get(start..end).ok_or_else(invalid_frontend_frame)?;
    Ok((
        i32::from_be_bytes(
            bytes
                .try_into()
                .expect("the guarded four-byte field length was checked"),
        ),
        end,
    ))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BackendKey {
    pid: i32,
    secret: i32,
}

#[derive(Debug, Default)]
struct CancellationRegistry {
    inner: Mutex<HashMap<i32, BackendCancellation>>,
}

#[derive(Debug)]
struct BackendCancellation {
    secret: i32,
    next_generation: u64,
    active: Option<(u64, CancellationToken)>,
}

impl CancellationRegistry {
    fn issue(self: &Arc<Self>) -> EngineResult<BackendKey> {
        for _ in 0..32 {
            let mut random = [0_u8; 8];
            getrandom::fill(&mut random).map_err(|error| {
                EngineError::from_source(
                    crate::core::EngineErrorKind::Internal,
                    "failed to generate a PostgreSQL backend cancellation key",
                    error,
                )
            })?;
            let pid = i32::from_be_bytes(random[..4].try_into().expect("four random PID bytes"))
                & i32::MAX;
            if pid == 0 {
                continue;
            }
            let secret = i32::from_be_bytes(
                random[4..]
                    .try_into()
                    .expect("four random cancellation-secret bytes"),
            );
            let mut inner = self.inner.lock().map_err(|_| {
                EngineError::new(
                    crate::core::EngineErrorKind::Internal,
                    "PostgreSQL cancellation registry lock was poisoned",
                )
            })?;
            if inner.contains_key(&pid) {
                continue;
            }
            inner.insert(
                pid,
                BackendCancellation {
                    secret,
                    next_generation: 1,
                    active: None,
                },
            );
            return Ok(BackendKey { pid, secret });
        }
        Err(EngineError::new(
            crate::core::EngineErrorKind::Internal,
            "failed to allocate a unique PostgreSQL backend identifier",
        ))
    }

    fn begin(self: &Arc<Self>, key: BackendKey) -> EngineResult<ActiveRequest> {
        let cancellation = CancellationToken::new();
        let generation = {
            let mut inner = self.inner.lock().map_err(|_| {
                EngineError::new(
                    crate::core::EngineErrorKind::Internal,
                    "PostgreSQL cancellation registry lock was poisoned",
                )
            })?;
            let backend = inner.get_mut(&key.pid).ok_or_else(|| {
                EngineError::new(
                    crate::core::EngineErrorKind::FailedPrecondition,
                    "the PostgreSQL backend cancellation key is no longer registered",
                )
            })?;
            if backend.secret != key.secret {
                return Err(EngineError::new(
                    crate::core::EngineErrorKind::FailedPrecondition,
                    "the PostgreSQL backend cancellation key does not match",
                ));
            }
            if backend.active.is_some() {
                return Err(EngineError::new(
                    crate::core::EngineErrorKind::Internal,
                    "the PostgreSQL backend already has an active request",
                ));
            }
            let generation = backend.next_generation;
            backend.next_generation = backend.next_generation.wrapping_add(1).max(1);
            backend.active = Some((generation, cancellation.clone()));
            generation
        };
        Ok(ActiveRequest {
            registry: Arc::clone(self),
            key,
            generation,
            cancellation,
        })
    }

    fn cancel(&self, pid: i32, secret: i32) -> bool {
        let cancellation = self.inner.lock().ok().and_then(|inner| {
            inner.get(&pid).and_then(|backend| {
                (backend.secret == secret)
                    .then(|| backend.active.as_ref().map(|(_, token)| token.clone()))
                    .flatten()
            })
        });
        cancellation.is_some_and(|token| token.cancel())
    }

    fn unregister(&self, key: BackendKey) {
        let cancellation = self.inner.lock().ok().and_then(|mut inner| {
            if inner
                .get(&key.pid)
                .is_some_and(|backend| backend.secret == key.secret)
            {
                inner
                    .remove(&key.pid)
                    .and_then(|backend| backend.active.map(|(_, token)| token))
            } else {
                None
            }
        });
        if let Some(cancellation) = cancellation {
            cancellation.cancel();
        }
    }

    fn finish(&self, key: BackendKey, generation: u64) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        let Some(backend) = inner.get_mut(&key.pid) else {
            return;
        };
        if backend.secret == key.secret
            && backend
                .active
                .as_ref()
                .is_some_and(|(active, _)| *active == generation)
        {
            backend.active = None;
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    #[cfg(test)]
    fn active_len(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .values()
            .filter(|backend| backend.active.is_some())
            .count()
    }
}

struct ActiveRequest {
    registry: Arc<CancellationRegistry>,
    key: BackendKey,
    generation: u64,
    cancellation: CancellationToken,
}

impl ActiveRequest {
    fn context(&self) -> RequestContext {
        RequestContext::new().with_cancellation_token(self.cancellation.clone())
    }
}

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.registry.finish(self.key, self.generation);
    }
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
    cancellations: Arc<CancellationRegistry>,
    security: Option<Arc<LoadedSecurity>>,
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
            cancellations: Arc::new(CancellationRegistry::default()),
            security: None,
        }
    }

    pub(crate) fn with_loaded_security(engine: Engine, security: LoadedSecurity) -> Self {
        let mut adapter = Self::new(engine);
        adapter.security = Some(Arc::new(security));
        adapter
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
            extended: Mutex::new(PgWireExtendedState::default()),
        });
        let wire_parser = Arc::new(PgWireQueryParser {
            state: Arc::clone(&state),
        });
        Connection { state, wire_parser }
    }

    pub(crate) fn wire_connection(&self) -> WireConnection {
        let scram = self.security.as_ref().map(|security| {
            security
                .scram()
                .expect("loaded PostgreSQL security was validated")
        });
        WireConnection {
            state: Arc::new(WireConnectionState {
                adapter: self.clone(),
                installed: OnceLock::new(),
                authentication: AsyncMutex::new(AuthenticationState::new(scram)),
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
            .field("secure", &self.security.is_some())
            .finish_non_exhaustive()
    }
}

struct ConnectionState {
    engine: Engine,
    session: Session,
    user: Option<Box<str>>,
    database: LogicalDatabaseId,
    database_name: Box<str>,
    extended: Mutex<PgWireExtendedState>,
}

#[derive(Default)]
struct PgWireExtendedState {
    portals: BTreeMap<String, PgWireBoundPortal>,
}

#[derive(Clone)]
struct PgWireBoundPortal {
    id: PortalId,
    statement: PreparedStatementId,
    fields: Arc<Vec<FieldInfo>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompatibilityShim {
    Static {
        column: &'static str,
        value: &'static str,
    },
    CurrentDatabase,
    CurrentSchema,
    CurrentUser {
        column: &'static str,
    },
    PsycopgTypeInfo,
}

impl CompatibilityShim {
    fn sqlite_sql(self, connection: &ConnectionState) -> String {
        if matches!(self, Self::PsycopgTypeInfo) {
            return "SELECT NULL AS \"name\", NULL AS \"oid\", NULL AS \"array_oid\", \
                    NULL AS \"regtype\", NULL AS \"delimiter\" \
                    WHERE $1 IS NULL AND 0"
                .to_owned();
        }
        let (column, value) = match self {
            Self::Static { column, value } => (column, value),
            Self::CurrentDatabase => ("current_database", connection.database_name.as_ref()),
            Self::CurrentSchema => ("current_schema", "public"),
            Self::CurrentUser { column } => (column, connection.user.as_deref().unwrap_or("")),
            Self::PsycopgTypeInfo => unreachable!("the catalog probe returned above"),
        };
        format!("SELECT {} AS \"{column}\"", sqlite_text_literal(value))
    }
}

fn postgres_compatibility_shim(sql: &str) -> Option<CompatibilityShim> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let [statement] = statements.as_slice() else {
        return None;
    };
    let canonical = statement.to_string().to_ascii_lowercase();
    match canonical.as_str() {
        "select version()" | "select pg_catalog.version()" => Some(CompatibilityShim::Static {
            column: "version",
            value: VERSION_BANNER,
        }),
        "select current_database()" | "select pg_catalog.current_database()" => {
            Some(CompatibilityShim::CurrentDatabase)
        }
        "select current_schema()" | "select pg_catalog.current_schema()" => {
            Some(CompatibilityShim::CurrentSchema)
        }
        "select current_user" => Some(CompatibilityShim::CurrentUser {
            column: "current_user",
        }),
        "select session_user" => Some(CompatibilityShim::CurrentUser {
            column: "session_user",
        }),
        "show server_version" => Some(CompatibilityShim::Static {
            column: "server_version",
            value: SERVER_VERSION,
        }),
        "show server_version_num" => Some(CompatibilityShim::Static {
            column: "server_version_num",
            value: POSTGRES_COMPATIBILITY_VERSION_NUM,
        }),
        "show server_encoding" => Some(CompatibilityShim::Static {
            column: "server_encoding",
            value: "UTF8",
        }),
        "show client_encoding" => Some(CompatibilityShim::Static {
            column: "client_encoding",
            value: "UTF8",
        }),
        "show standard_conforming_strings" => Some(CompatibilityShim::Static {
            column: "standard_conforming_strings",
            value: "on",
        }),
        "show integer_datetimes" => Some(CompatibilityShim::Static {
            column: "integer_datetimes",
            value: "on",
        }),
        "show timezone" => Some(CompatibilityShim::Static {
            column: "TimeZone",
            value: "UTC",
        }),
        "show transaction isolation level" => Some(CompatibilityShim::Static {
            column: "transaction_isolation",
            value: "serializable",
        }),
        "show transaction_read_only" => Some(CompatibilityShim::Static {
            column: "transaction_read_only",
            value: "off",
        }),
        "show search_path" => Some(CompatibilityShim::Static {
            column: "search_path",
            value: "public",
        }),
        "show datestyle" => Some(CompatibilityShim::Static {
            column: "DateStyle",
            value: "ISO, MDY",
        }),
        "show intervalstyle" => Some(CompatibilityShim::Static {
            column: "IntervalStyle",
            value: "postgres",
        }),
        "show extra_float_digits" => Some(CompatibilityShim::Static {
            column: "extra_float_digits",
            value: "1",
        }),
        "show default_transaction_isolation" => Some(CompatibilityShim::Static {
            column: "default_transaction_isolation",
            value: "serializable",
        }),
        "show default_transaction_read_only" => Some(CompatibilityShim::Static {
            column: "default_transaction_read_only",
            value: "off",
        }),
        "show session_authorization" => Some(CompatibilityShim::CurrentUser {
            column: "session_authorization",
        }),
        "select current_setting('server_version')" => Some(CompatibilityShim::Static {
            column: "current_setting",
            value: SERVER_VERSION,
        }),
        "select current_setting('server_version_num')" => Some(CompatibilityShim::Static {
            column: "current_setting",
            value: POSTGRES_COMPATIBILITY_VERSION_NUM,
        }),
        "select current_setting('standard_conforming_strings')" => {
            Some(CompatibilityShim::Static {
                column: "current_setting",
                value: "on",
            })
        }
        "select current_setting('timezone')" => Some(CompatibilityShim::Static {
            column: "current_setting",
            value: "UTC",
        }),
        "select typname as name, oid, typarray as array_oid, oid::regtype::text as regtype, typdelim as delimiter from pg_type t where t.oid = to_regtype($1) order by t.oid" => {
            Some(CompatibilityShim::PsycopgTypeInfo)
        }
        _ => None,
    }
}

fn sqlite_text_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn postgres_compatibility_sql<'a>(connection: &ConnectionState, sql: &'a str) -> Cow<'a, str> {
    postgres_compatibility_shim(sql).map_or(Cow::Borrowed(sql), |shim| {
        Cow::Owned(shim.sqlite_sql(connection))
    })
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
        context: RequestContext,
    ) -> EngineResult<(StatementBehavior, PreparedExecution)> {
        let sql = postgres_compatibility_sql(&self.state, sql);
        let statement = self
            .state
            .engine
            .prepare_statement_with_context(
                &self.state.session,
                PrepareRequest::new(
                    self.state.database,
                    SqlDialect::PostgreSql,
                    SqlTranslationMode::Compatibility,
                    sql.as_ref(),
                ),
                context.clone(),
            )
            .await?;

        let result = self.execute_prepared_simple_query(statement, context).await;
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
        context: RequestContext,
    ) -> EngineResult<(StatementBehavior, PreparedExecution)> {
        let description = self
            .state
            .engine
            .describe_prepared_with_context(
                &self.state.session,
                DescribeTarget::Statement(statement),
                context.clone(),
            )
            .await?;
        let behavior = description.behavior();
        let portal = self
            .state
            .engine
            .bind_statement_with_context(
                &self.state.session,
                statement,
                Vec::new(),
                context.clone(),
            )
            .await?;
        let result = self
            .state
            .engine
            .execute_portal_logical_with_context(&self.state.session, portal, context)
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
    installed: OnceLock<InstalledWireConnection>,
    authentication: AsyncMutex<AuthenticationState>,
}

struct AuthenticationState {
    scram: Option<ScramAuth>,
    sasl: SASLState,
    pending: Option<PendingStartup>,
}

impl AuthenticationState {
    fn new(scram: Option<ScramAuth>) -> Self {
        Self {
            scram,
            sasl: SASLState::Initial,
            pending: None,
        }
    }
}

#[derive(Clone)]
struct PendingStartup {
    user: Box<str>,
    database: Box<str>,
    application_name: Option<Box<str>>,
    protocol_minor: u16,
    unsupported_options: Vec<String>,
}

struct InstalledWireConnection {
    connection: Arc<Connection>,
    backend_key: BackendKey,
}

impl WireConnection {
    fn installed(&self) -> PgWireResult<&Arc<Connection>> {
        self.state
            .installed
            .get()
            .map(|installed| &installed.connection)
            .ok_or_else(|| fatal_wire_error("08P01", "PostgreSQL startup has not completed"))
    }

    fn backend_key(&self) -> PgWireResult<BackendKey> {
        self.state
            .installed
            .get()
            .map(|installed| installed.backend_key)
            .ok_or_else(|| fatal_wire_error("08P01", "PostgreSQL startup has not completed"))
    }

    fn begin_request(&self) -> PgWireResult<ActiveRequest> {
        self.state
            .adapter
            .cancellations
            .begin(self.backend_key()?)
            .map_err(engine_error_to_pgwire)
    }

    async fn install(&self, user: &str, database: &str) -> PgWireResult<()> {
        if self.state.installed.get().is_some() {
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
        let backend_key = self
            .state
            .adapter
            .cancellations
            .issue()
            .map_err(|error| engine_error_to_pgwire_with_severity(error, "FATAL"))?;
        let installed = InstalledWireConnection {
            connection,
            backend_key,
        };
        if let Err(installed) = self.state.installed.set(installed) {
            self.state
                .adapter
                .cancellations
                .unregister(installed.backend_key);
            let _ = installed.connection.close().await;
            return Err(fatal_wire_error(
                "08P01",
                "PostgreSQL startup was already completed",
            ));
        }
        let connection = self
            .state
            .installed
            .get()
            .map(|installed| &installed.connection)
            .expect("the validated PostgreSQL connection was just installed");
        if let Err(error) = connection.status().await {
            let _ = self.close().await;
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
        if let Some(installed) = self.state.installed.get() {
            self.state
                .adapter
                .cancellations
                .unregister(installed.backend_key);
            installed.connection.close().await
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    fn connection(&self) -> Option<&Arc<Connection>> {
        self.state
            .installed
            .get()
            .map(|installed| &installed.connection)
    }
}

struct WireHandlers {
    connection: WireConnection,
}

impl WireHandlers {
    async fn sync_failed_transaction<C>(&self, client: &C) -> PgWireResult<()>
    where
        C: ClientInfo,
    {
        if client.transaction_status() == TransactionStatus::Error {
            self.connection
                .installed()?
                .state
                .session
                .fail_transaction()
                .await;
        }
        Ok(())
    }

    async fn remove_statement<C>(&self, client: &mut C, name: &str) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = PgWirePrepared>,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let Some(statement) = client.portal_store().get_statement(name) else {
            return Ok(());
        };
        let connection = self.connection.installed()?;
        connection
            .state
            .engine
            .close_prepared_statement(&connection.state.session, statement.statement.id)
            .await
            .map_err(engine_error_to_pgwire)?;
        let portals =
            remove_bound_portals_for_statement(&connection.state, statement.statement.id)?;
        for portal in portals {
            client.portal_store().rm_portal(&portal);
        }
        client.portal_store().rm_statement(name);
        Ok(())
    }

    async fn remove_portal<C>(&self, client: &mut C, name: &str) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = PgWirePrepared>,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let connection = self.connection.installed()?;
        if let Some(portal) = bound_portal(&connection.state, name)? {
            connection
                .state
                .engine
                .close_portal(&connection.state.session, portal.id)
                .await
                .map_err(engine_error_to_pgwire)?;
            remove_bound_portal(&connection.state, name)?;
        }
        client.portal_store().rm_portal(name);
        Ok(())
    }

    async fn execute_bound_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<PgWirePrepared>,
        context: RequestContext,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = PgWirePrepared>,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let connection = self.connection.installed()?;
        let bound = bound_portal(&connection.state, portal.name.as_str())?
            .ok_or_else(|| PgWireError::PortalNotFound(portal.name.clone()))?;
        let behavior = connection
            .state
            .engine
            .describe_prepared_with_context(
                &connection.state.session,
                DescribeTarget::Portal(bound.id),
                context.clone(),
            )
            .await
            .map_err(engine_error_to_pgwire)?
            .behavior();
        let execution = connection
            .state
            .engine
            .execute_portal_logical_with_context(&connection.state.session, bound.id, context)
            .await
            .map_err(engine_error_to_pgwire)?;
        execution_response(behavior, execution.value, Some(Arc::clone(&bound.fields)))
    }
}

fn validate_startup(startup: &Startup) -> PgWireResult<PendingStartup> {
    if startup.protocol_number_major != POSTGRES_PROTOCOL_MAJOR {
        return Err(fatal_wire_error(
            "08P01",
            "the PostgreSQL protocol version is unsupported",
        ));
    }

    let mut unsupported_options = startup
        .parameters
        .keys()
        .filter(|key| key.starts_with("_pq_."))
        .cloned()
        .collect::<Vec<_>>();
    unsupported_options.sort_unstable();
    for key in startup.parameters.keys() {
        if !matches!(
            key.as_str(),
            "user" | "database" | "client_encoding" | "application_name" | "replication"
        ) && !key.starts_with("_pq_.")
        {
            return Err(fatal_wire_error(
                "0A000",
                "the PostgreSQL startup parameter is unsupported",
            ));
        }
    }

    let user = startup
        .parameters
        .get("user")
        .ok_or_else(|| fatal_wire_error("28000", "a valid PostgreSQL user label is required"))?;
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

    Ok(PendingStartup {
        user: user.as_str().into(),
        database: database.into(),
        application_name: application_name.map(|name| name.as_str().into()),
        protocol_minor: startup.protocol_number_minor,
        unsupported_options,
    })
}

fn set_startup_metadata<C>(client: &mut C, pending: &PendingStartup)
where
    C: ClientInfo,
{
    client.set_protocol_version(ProtocolVersion::PROTOCOL3_0);
    let metadata = client.metadata_mut();
    metadata.insert("user".to_owned(), pending.user.to_string());
    metadata.insert("database".to_owned(), pending.database.to_string());
    metadata.insert("client_encoding".to_owned(), "UTF8".to_owned());
    if let Some(application_name) = &pending.application_name {
        metadata.insert("application_name".to_owned(), application_name.to_string());
    }
}

async fn finish_startup<C>(
    connection: &WireConnection,
    client: &mut C,
    pending: PendingStartup,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: fmt::Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    connection.install(&pending.user, &pending.database).await?;
    let backend_key = connection.backend_key()?;
    client.set_pid_and_secret_key(backend_key.pid, SecretKey::I32(backend_key.secret));
    if pending.protocol_minor > POSTGRES_PROTOCOL_MINOR || !pending.unsupported_options.is_empty() {
        client
            .send(PgWireBackendMessage::NegotiateProtocolVersion(
                NegotiateProtocolVersion::new(
                    i32::from(POSTGRES_PROTOCOL_MINOR),
                    pending.unsupported_options,
                ),
            ))
            .await?;
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
    if let Some(application_name) = pending.application_name {
        client
            .feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                "application_name".to_owned(),
                application_name.to_string(),
            )))
            .await?;
    }
    client
        .feed(PgWireBackendMessage::BackendKeyData(BackendKeyData::new(
            backend_key.pid,
            SecretKey::I32(backend_key.secret),
        )))
        .await?;
    client
        .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
            TransactionStatus::Idle,
        )))
        .await?;
    client.set_state(PgWireConnectionState::ReadyForQuery);
    Ok(())
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
        match message {
            PgWireFrontendMessage::Startup(startup) => {
                let pending = validate_startup(&startup)?;
                set_startup_metadata(client, &pending);
                if self.connection.state.adapter.security.is_none() {
                    return finish_startup(&self.connection, client, pending).await;
                }
                if !client.is_secure() {
                    return Err(fatal_wire_error(
                        "08004",
                        "TLS is required for PostgreSQL authentication",
                    ));
                }
                let mut authentication = self.connection.state.authentication.lock().await;
                if authentication.pending.is_some() {
                    return Err(fatal_wire_error(
                        "08P01",
                        "PostgreSQL startup was already received",
                    ));
                }
                authentication.pending = Some(pending);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(Authentication::SASL(
                        vec!["SCRAM-SHA-256-PLUS".to_owned(), "SCRAM-SHA-256".to_owned()],
                    )))
                    .await
                    .map_err(Into::into)
            }
            PgWireFrontendMessage::PasswordMessageFamily(mut message) => {
                let mut authentication = self.connection.state.authentication.lock().await;
                if authentication.pending.is_none() || authentication.scram.is_none() {
                    return Err(fatal_wire_error(
                        "08P01",
                        "PostgreSQL SCRAM authentication was not started",
                    ));
                }
                if matches!(authentication.sasl, SASLState::Initial) {
                    let initial = message.into_sasl_initial_response()?;
                    if !matches!(
                        initial.auth_method.as_str(),
                        "SCRAM-SHA-256" | "SCRAM-SHA-256-PLUS"
                    ) {
                        return Err(fatal_wire_error(
                            "0A000",
                            "the PostgreSQL SASL mechanism is unsupported",
                        ));
                    }
                    authentication.sasl = SASLState::ScramClientFirstReceived;
                    message = PasswordMessageFamily::SASLInitialResponse(initial);
                } else {
                    message = PasswordMessageFamily::SASLResponse(message.into_sasl_response()?);
                }

                let (response, state) = authentication
                    .scram
                    .as_ref()
                    .expect("the SCRAM configuration was checked")
                    .process_scram_message(client, message, &authentication.sasl)
                    .await?;
                if matches!(state, SASLState::Finished) {
                    let pending = authentication
                        .pending
                        .as_ref()
                        .expect("finished SCRAM has validated startup metadata");
                    let configured_user = self
                        .connection
                        .state
                        .adapter
                        .security
                        .as_ref()
                        .expect("SCRAM is present only for secure adapters")
                        .credentials
                        .user
                        .as_ref();
                    if pending.user.as_ref() != configured_user {
                        return Err(PgWireError::InvalidPassword(String::new()));
                    }
                }
                client
                    .send(PgWireBackendMessage::Authentication(response))
                    .await?;
                authentication.sasl = state;
                if !matches!(authentication.sasl, SASLState::Finished) {
                    return Ok(());
                }
                let pending = authentication
                    .pending
                    .take()
                    .expect("finished SCRAM has validated startup metadata");
                drop(authentication);
                finish_startup(&self.connection, client, pending).await
            }
            _ => Err(fatal_wire_error(
                "08P01",
                "a PostgreSQL startup or SCRAM message is required",
            )),
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for WireHandlers {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.sync_failed_transaction(client).await?;
        let request = self.connection.begin_request()?;
        let (behavior, execution) = self
            .connection
            .installed()?
            .execute_simple_query(query, request.context())
            .await
            .map_err(engine_error_to_pgwire)?;
        Ok(vec![simple_query_response(behavior, execution)?])
    }
}

fn simple_query_response(
    behavior: StatementBehavior,
    execution: PreparedExecution,
) -> PgWireResult<Response> {
    execution_response(behavior, execution, None)
}

fn execution_response(
    behavior: StatementBehavior,
    execution: PreparedExecution,
    fields: Option<Arc<Vec<FieldInfo>>>,
) -> PgWireResult<Response> {
    match execution {
        PreparedExecution::Rows(result) if matches!(behavior, StatementBehavior::Read) => {
            match fields {
                Some(fields) => result_set_response_with_fields(result, fields),
                None => result_set_response(result),
            }
        }
        PreparedExecution::AffectedRows(rows) => {
            Ok(Response::Execution(write_tag(behavior, rows)?))
        }
        PreparedExecution::GeneratedWrite(result) => Ok(Response::Execution(write_tag(
            behavior,
            result.rows_affected,
        )?)),
        PreparedExecution::Transaction(TransactionExecution::Started) => {
            Ok(Response::TransactionStart(Tag::new("BEGIN")))
        }
        PreparedExecution::Transaction(TransactionExecution::Committed) => {
            Ok(Response::TransactionEnd(Tag::new("COMMIT")))
        }
        PreparedExecution::Transaction(TransactionExecution::RolledBack) => {
            Ok(Response::TransactionEnd(Tag::new("ROLLBACK")))
        }
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
    encode_query_response(rows, fields)
}

fn result_set_response_with_fields(
    result: ResultSet,
    fields: Arc<Vec<FieldInfo>>,
) -> PgWireResult<Response> {
    let (columns, rows) = result.into_parts();
    if columns.len() != fields.len() {
        return Err(internal_query_error());
    }
    encode_query_response(rows, fields)
}

fn encode_query_response(
    rows: Vec<crate::core::Row>,
    fields: Arc<Vec<FieldInfo>>,
) -> PgWireResult<Response> {
    let encoded = rows
        .into_iter()
        .map(|row| encode_data_row(Arc::clone(&fields), row.into_values()))
        .collect::<PgWireResult<Vec<_>>>()?;
    Ok(Response::Query(QueryResponse::new(
        fields,
        stream::iter(encoded.into_iter().map(Ok)),
    )))
}

fn description_fields(description: &PreparedStatementDescription) -> Vec<FieldInfo> {
    description_fields_with(description, |_| FieldFormat::Text)
}

fn description_fields_with(
    description: &PreparedStatementDescription,
    format_for: impl Fn(usize) -> FieldFormat,
) -> Vec<FieldInfo> {
    description
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            FieldInfo::new(
                column.name.clone(),
                None,
                None,
                postgres_type(column.data_type),
                format_for(index),
            )
        })
        .collect()
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

fn supported_parameter_type(data_type: &Type) -> bool {
    matches!(
        data_type,
        &Type::BOOL
            | &Type::INT2
            | &Type::INT4
            | &Type::INT8
            | &Type::OID
            | &Type::FLOAT4
            | &Type::FLOAT8
            | &Type::NUMERIC
            | &Type::BYTEA
            | &Type::TEXT
            | &Type::VARCHAR
            | &Type::BPCHAR
            | &Type::NAME
            | &Type::UNKNOWN
            | &Type::JSON
            | &Type::JSONB
    )
}

fn decode_bound_parameters(
    portal: &Portal<PgWirePrepared>,
    parameter_types: &[Type],
) -> PgWireResult<Vec<Value>> {
    if portal.parameters.len() != parameter_types.len() {
        return Err(query_wire_error(
            "08P01",
            "the PostgreSQL bound parameter count does not match the statement",
        ));
    }
    portal
        .parameters
        .iter()
        .zip(parameter_types)
        .enumerate()
        .map(|(index, (parameter, data_type))| match parameter {
            None => Ok(Value::Null),
            Some(parameter) => match portal.parameter_format.format_for(index) {
                FieldFormat::Text => decode_text_parameter(parameter, data_type),
                FieldFormat::Binary => decode_binary_parameter(parameter, data_type),
            },
        })
        .collect()
}

fn decode_text_parameter(parameter: &[u8], data_type: &Type) -> PgWireResult<Value> {
    if data_type == &Type::BYTEA {
        return decode_bytea_text(parameter).map(Value::Binary);
    }
    let parameter = std::str::from_utf8(parameter).map_err(|_| invalid_parameter_utf8_error())?;
    if data_type == &Type::BOOL {
        if ["t", "true", "y", "yes", "on", "1"]
            .iter()
            .any(|accepted| parameter.eq_ignore_ascii_case(accepted))
        {
            Ok(Value::Boolean(true))
        } else if ["f", "false", "n", "no", "off", "0"]
            .iter()
            .any(|accepted| parameter.eq_ignore_ascii_case(accepted))
        {
            Ok(Value::Boolean(false))
        } else {
            Err(invalid_parameter_value_error())
        }
    } else if data_type == &Type::INT2 {
        parameter
            .parse::<i16>()
            .map(|value| Value::Int64(i64::from(value)))
            .map_err(|_| invalid_parameter_value_error())
    } else if data_type == &Type::INT4 {
        parameter
            .parse::<i32>()
            .map(|value| Value::Int64(i64::from(value)))
            .map_err(|_| invalid_parameter_value_error())
    } else if data_type == &Type::INT8 {
        parameter
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|_| invalid_parameter_value_error())
    } else if data_type == &Type::OID {
        parameter
            .parse::<u32>()
            .map(|value| Value::UInt64(u64::from(value)))
            .map_err(|_| invalid_parameter_value_error())
    } else if data_type == &Type::FLOAT4 {
        parse_postgres_float(parameter)
            .and_then(|value| {
                let narrowed = value as f32;
                if narrowed.is_finite() || value.is_nan() || value.is_infinite() {
                    Some(Value::Float64(f64::from(narrowed)))
                } else {
                    None
                }
            })
            .ok_or_else(invalid_parameter_value_error)
    } else if data_type == &Type::FLOAT8 {
        parse_postgres_float(parameter)
            .map(Value::Float64)
            .ok_or_else(invalid_parameter_value_error)
    } else if data_type == &Type::NUMERIC {
        Value::decimal(parameter).map_err(|_| invalid_parameter_value_error())
    } else if is_postgres_text_type(data_type) || matches!(data_type, &Type::JSON | &Type::JSONB) {
        Ok(Value::Text(parameter.to_owned()))
    } else {
        Err(unsupported_parameter_type_error())
    }
}

fn decode_binary_parameter(parameter: &[u8], data_type: &Type) -> PgWireResult<Value> {
    if data_type == &Type::BOOL {
        return match parameter {
            [0] => Ok(Value::Boolean(false)),
            [1] => Ok(Value::Boolean(true)),
            _ => Err(invalid_parameter_value_error()),
        };
    }
    if data_type == &Type::INT2 {
        return parameter
            .try_into()
            .map(i16::from_be_bytes)
            .map(|value| Value::Int64(i64::from(value)))
            .map_err(|_| invalid_parameter_value_error());
    }
    if data_type == &Type::INT4 {
        return parameter
            .try_into()
            .map(i32::from_be_bytes)
            .map(|value| Value::Int64(i64::from(value)))
            .map_err(|_| invalid_parameter_value_error());
    }
    if data_type == &Type::INT8 {
        return parameter
            .try_into()
            .map(i64::from_be_bytes)
            .map(Value::Int64)
            .map_err(|_| invalid_parameter_value_error());
    }
    if data_type == &Type::OID {
        return parameter
            .try_into()
            .map(u32::from_be_bytes)
            .map(|value| Value::UInt64(u64::from(value)))
            .map_err(|_| invalid_parameter_value_error());
    }
    if data_type == &Type::FLOAT4 {
        return parameter
            .try_into()
            .map(f32::from_be_bytes)
            .map(|value| Value::Float64(f64::from(value)))
            .map_err(|_| invalid_parameter_value_error());
    }
    if data_type == &Type::FLOAT8 {
        return parameter
            .try_into()
            .map(f64::from_be_bytes)
            .map(Value::Float64)
            .map_err(|_| invalid_parameter_value_error());
    }
    if data_type == &Type::NUMERIC {
        return decode_numeric_binary(parameter);
    }
    if data_type == &Type::BYTEA {
        return Ok(Value::Binary(parameter.to_vec()));
    }
    if data_type == &Type::JSONB {
        let Some((1, json)) = parameter.split_first() else {
            return Err(invalid_parameter_value_error());
        };
        return std::str::from_utf8(json)
            .map(|json| Value::Text(json.to_owned()))
            .map_err(|_| invalid_parameter_utf8_error());
    }
    if is_postgres_text_type(data_type) || data_type == &Type::JSON {
        return std::str::from_utf8(parameter)
            .map(|value| Value::Text(value.to_owned()))
            .map_err(|_| invalid_parameter_utf8_error());
    }
    Err(unsupported_parameter_type_error())
}

fn parse_postgres_float(value: &str) -> Option<f64> {
    if value.eq_ignore_ascii_case("nan") {
        Some(f64::NAN)
    } else if value.eq_ignore_ascii_case("infinity") || value.eq_ignore_ascii_case("inf") {
        Some(f64::INFINITY)
    } else if value.eq_ignore_ascii_case("-infinity") || value.eq_ignore_ascii_case("-inf") {
        Some(f64::NEG_INFINITY)
    } else {
        value.parse().ok()
    }
}

fn decode_bytea_text(value: &[u8]) -> PgWireResult<Vec<u8>> {
    if let Some(hex) = value.strip_prefix(br"\x") {
        if hex.len() % 2 != 0 {
            return Err(invalid_parameter_value_error());
        }
        return hex
            .chunks_exact(2)
            .map(|pair| {
                let high = decode_hex(pair[0]).ok_or_else(invalid_parameter_value_error)?;
                let low = decode_hex(pair[1]).ok_or_else(invalid_parameter_value_error)?;
                Ok((high << 4) | low)
            })
            .collect();
    }

    let mut decoded = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        if value[index] != b'\\' {
            decoded.push(value[index]);
            index += 1;
        } else if value.get(index + 1) == Some(&b'\\') {
            decoded.push(b'\\');
            index += 2;
        } else {
            let octal = value
                .get(index + 1..index + 4)
                .filter(|digits| digits.iter().all(|digit| matches!(digit, b'0'..=b'7')))
                .ok_or_else(invalid_parameter_value_error)?;
            let value = u16::from(octal[0] - b'0') * 64
                + u16::from(octal[1] - b'0') * 8
                + u16::from(octal[2] - b'0');
            decoded.push(u8::try_from(value).map_err(|_| invalid_parameter_value_error())?);
            index += 4;
        }
    }
    Ok(decoded)
}

fn decode_hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn decode_numeric_binary(value: &[u8]) -> PgWireResult<Value> {
    if value.len() < 8 || (value.len() - 8) % 2 != 0 {
        return Err(invalid_parameter_value_error());
    }
    let ndigits = i16::from_be_bytes(value[0..2].try_into().unwrap());
    let weight = i16::from_be_bytes(value[2..4].try_into().unwrap());
    let sign = u16::from_be_bytes(value[4..6].try_into().unwrap());
    let dscale = i16::from_be_bytes(value[6..8].try_into().unwrap());
    if ndigits < 0
        || !(0..=16_383).contains(&dscale)
        || usize::try_from(ndigits).ok() != Some((value.len() - 8) / 2)
        || !matches!(sign, 0x0000 | 0x4000)
    {
        return Err(invalid_parameter_value_error());
    }
    let groups = value[8..]
        .chunks_exact(2)
        .map(|bytes| i16::from_be_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>();
    if groups.iter().any(|group| !(0..=9999).contains(group)) {
        return Err(invalid_parameter_value_error());
    }

    let mut integer = String::new();
    if weight >= 0 {
        for position in (0..=weight).rev() {
            let index = i32::from(weight) - i32::from(position);
            let group = usize::try_from(index)
                .ok()
                .and_then(|index| groups.get(index))
                .copied()
                .unwrap_or(0);
            if integer.is_empty() {
                integer.push_str(&group.to_string());
            } else {
                integer.push_str(&format!("{group:04}"));
            }
        }
    } else {
        integer.push('0');
    }
    if integer.is_empty() {
        integer.push('0');
    }

    let mut fraction = String::new();
    let fraction_groups = usize::from(u16::try_from(dscale).unwrap()).div_ceil(4);
    for offset in 0..fraction_groups {
        let position =
            -1_i32 - i32::try_from(offset).map_err(|_| invalid_parameter_value_error())?;
        let index = i32::from(weight) - position;
        let group = usize::try_from(index)
            .ok()
            .and_then(|index| groups.get(index))
            .copied()
            .unwrap_or(0);
        fraction.push_str(&format!("{group:04}"));
    }
    fraction.truncate(usize::from(u16::try_from(dscale).unwrap()));

    let mut decoded = String::new();
    if sign == 0x4000 {
        decoded.push('-');
    }
    decoded.push_str(&integer);
    if dscale > 0 {
        decoded.push('.');
        decoded.push_str(&fraction);
    }
    Value::decimal(decoded).map_err(|_| invalid_parameter_value_error())
}

fn unsupported_parameter_type_error() -> PgWireError {
    query_wire_error("0A000", "the PostgreSQL parameter type is unsupported")
}

fn invalid_parameter_value_error() -> PgWireError {
    query_wire_error("22P02", "the PostgreSQL parameter value is invalid")
}

fn invalid_parameter_utf8_error() -> PgWireError {
    query_wire_error("22021", "the PostgreSQL text parameter is not valid UTF8")
}

fn encode_data_row(fields: Arc<Vec<FieldInfo>>, values: Vec<Value>) -> PgWireResult<DataRow> {
    if fields.len() != values.len() {
        return Err(internal_query_error());
    }
    let field_count = i16::try_from(fields.len()).map_err(|_| internal_query_error())?;
    let mut data = BytesMut::new();
    for (field, value) in fields.iter().zip(values) {
        let Some(encoded) = encode_postgres_value(value, field.datatype(), field.format())? else {
            data.put_i32(-1);
            continue;
        };
        data.put_i32(i32::try_from(encoded.len()).map_err(|_| internal_query_error())?);
        data.extend_from_slice(&encoded);
    }
    Ok(DataRow::new(data, field_count))
}

fn encode_postgres_value(
    value: Value,
    data_type: &Type,
    format: FieldFormat,
) -> PgWireResult<Option<Vec<u8>>> {
    if matches!(value, Value::Null) {
        return Ok(None);
    }

    let text = || postgres_scalar_text(&value);
    let encoded = if data_type == &Type::BOOL {
        let value = postgres_bool(&value)?;
        match format {
            FieldFormat::Text => vec![if value { b't' } else { b'f' }],
            FieldFormat::Binary => vec![u8::from(value)],
        }
    } else if data_type == &Type::INT8 {
        let value = postgres_i64(&value)?;
        match format {
            FieldFormat::Text => value.to_string().into_bytes(),
            FieldFormat::Binary => value.to_be_bytes().to_vec(),
        }
    } else if data_type == &Type::FLOAT8 {
        let Value::Float64(value) = value else {
            return Err(result_type_mismatch_error());
        };
        match format {
            FieldFormat::Text => postgres_float_text(value).into_bytes(),
            FieldFormat::Binary => value.to_be_bytes().to_vec(),
        }
    } else if data_type == &Type::NUMERIC {
        let text = postgres_numeric_text(&value)?;
        match format {
            FieldFormat::Text => text.into_bytes(),
            FieldFormat::Binary => encode_numeric_binary(&text)?,
        }
    } else if data_type == &Type::BYTEA {
        let Value::Binary(value) = value else {
            return Err(result_type_mismatch_error());
        };
        match format {
            FieldFormat::Text => postgres_bytea_text(&value),
            FieldFormat::Binary => value,
        }
    } else if is_postgres_text_type(data_type) {
        text()?.into_bytes()
    } else {
        return Err(result_type_mismatch_error());
    };
    Ok(Some(encoded))
}

fn postgres_bool(value: &Value) -> PgWireResult<bool> {
    match value {
        Value::Boolean(value) => Ok(*value),
        Value::Int64(0) => Ok(false),
        Value::Int64(1) => Ok(true),
        _ => Err(result_type_mismatch_error()),
    }
}

fn postgres_i64(value: &Value) -> PgWireResult<i64> {
    match value {
        Value::Int64(value) => Ok(*value),
        Value::UInt64(value) => i64::try_from(*value).map_err(|_| result_type_mismatch_error()),
        _ => Err(result_type_mismatch_error()),
    }
}

fn postgres_numeric_text(value: &Value) -> PgWireResult<String> {
    match value {
        Value::Int64(value) => Ok(value.to_string()),
        Value::UInt64(value) => Ok(value.to_string()),
        Value::Float64(value) => Ok(postgres_float_text(*value)),
        Value::Decimal(value) => Ok(value.as_str().to_owned()),
        _ => Err(result_type_mismatch_error()),
    }
}

fn postgres_scalar_text(value: &Value) -> PgWireResult<String> {
    match value {
        Value::Null => unreachable!("NULL is handled before scalar encoding"),
        Value::Boolean(value) => Ok(if *value { "t" } else { "f" }.to_owned()),
        Value::Int64(value) => Ok(value.to_string()),
        Value::UInt64(value) => Ok(value.to_string()),
        Value::Float64(value) => Ok(postgres_float_text(*value)),
        Value::Decimal(value) => Ok(value.as_str().to_owned()),
        Value::Text(value) => Ok(value.clone()),
        Value::InvalidText(_) => Err(invalid_text_query_error()),
        Value::Binary(value) => {
            String::from_utf8(postgres_bytea_text(value)).map_err(|_| internal_query_error())
        }
    }
}

fn postgres_float_text(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "Infinity".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_owned()
    } else {
        value.to_string()
    }
}

fn postgres_bytea_text(value: &[u8]) -> Vec<u8> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = Vec::with_capacity(2 + value.len().saturating_mul(2));
    encoded.extend_from_slice(br"\x");
    for byte in value {
        encoded.push(HEX[usize::from(byte >> 4)]);
        encoded.push(HEX[usize::from(byte & 0x0f)]);
    }
    encoded
}

fn is_postgres_text_type(data_type: &Type) -> bool {
    matches!(
        data_type,
        &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR | &Type::NAME | &Type::UNKNOWN
    )
}

fn encode_numeric_binary(value: &str) -> PgWireResult<Vec<u8>> {
    let special_sign = match value {
        "NaN" => Some(0xc000_u16),
        "Infinity" => Some(0xd000),
        "-Infinity" => Some(0xf000),
        _ => None,
    };
    if let Some(sign) = special_sign {
        let mut encoded = Vec::with_capacity(8);
        encoded.extend_from_slice(&0_i16.to_be_bytes());
        encoded.extend_from_slice(&0_i16.to_be_bytes());
        encoded.extend_from_slice(&sign.to_be_bytes());
        encoded.extend_from_slice(&0_i16.to_be_bytes());
        return Ok(encoded);
    }

    let (negative, unsigned) = value
        .strip_prefix('-')
        .map(|value| (true, value))
        .or_else(|| value.strip_prefix('+').map(|value| (false, value)))
        .unwrap_or((false, value));
    let (mantissa, exponent) = unsigned
        .split_once(['e', 'E'])
        .map(|(mantissa, exponent)| {
            exponent
                .parse::<i32>()
                .map(|exponent| (mantissa, exponent))
                .map_err(|_| result_type_mismatch_error())
        })
        .transpose()?
        .unwrap_or((unsigned, 0));
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if integer.is_empty() && fraction.is_empty() {
        return Err(result_type_mismatch_error());
    }
    if !integer
        .bytes()
        .chain(fraction.bytes())
        .all(|byte| byte.is_ascii_digit())
    {
        return Err(result_type_mismatch_error());
    }

    let mut digits = String::with_capacity(integer.len() + fraction.len());
    digits.push_str(integer);
    digits.push_str(fraction);
    let decimal_index = i64::try_from(integer.len())
        .ok()
        .and_then(|length| length.checked_add(i64::from(exponent)))
        .ok_or_else(numeric_out_of_range_error)?;
    let digit_count = i64::try_from(digits.len()).map_err(|_| numeric_out_of_range_error())?;
    let dscale = digit_count.checked_sub(decimal_index).unwrap_or(0).max(0);
    if dscale > 16_383 {
        return Err(numeric_out_of_range_error());
    }
    let dscale = i16::try_from(dscale).map_err(|_| numeric_out_of_range_error())?;

    let mut whole = String::new();
    if decimal_index <= 0 {
        whole.push('0');
    } else {
        let integer_digits =
            usize::try_from(decimal_index).map_err(|_| numeric_out_of_range_error())?;
        let available = integer_digits.min(digits.len());
        whole.push_str(&digits[..available]);
        whole.extend(std::iter::repeat_n('0', integer_digits - available));
        if whole.is_empty() {
            whole.push('0');
        }
    }

    let mut fractional = String::new();
    if dscale > 0 {
        if decimal_index < 0 {
            fractional.extend(std::iter::repeat_n(
                '0',
                usize::try_from(-decimal_index).map_err(|_| numeric_out_of_range_error())?,
            ));
            fractional.push_str(&digits);
        } else {
            let start = usize::try_from(decimal_index)
                .map_err(|_| numeric_out_of_range_error())?
                .min(digits.len());
            fractional.push_str(&digits[start..]);
        }
        fractional.truncate(usize::try_from(dscale).expect("positive i16 fits usize"));
        fractional.extend(std::iter::repeat_n(
            '0',
            usize::try_from(dscale).expect("positive i16 fits usize") - fractional.len(),
        ));
    }

    let integer_groups = whole.len().div_ceil(4);
    let left_padding = integer_groups * 4 - whole.len();
    let fractional_groups = fractional.len().div_ceil(4);
    let mut grouped = String::with_capacity((integer_groups + fractional_groups) * 4);
    grouped.extend(std::iter::repeat_n('0', left_padding));
    grouped.push_str(&whole);
    grouped.push_str(&fractional);
    grouped.extend(std::iter::repeat_n(
        '0',
        fractional_groups * 4 - fractional.len(),
    ));
    let groups = grouped
        .as_bytes()
        .chunks_exact(4)
        .map(|chunk| {
            std::str::from_utf8(chunk)
                .ok()
                .and_then(|chunk| chunk.parse::<i16>().ok())
                .ok_or_else(result_type_mismatch_error)
        })
        .collect::<PgWireResult<Vec<_>>>()?;
    let leading_zeroes = groups.iter().take_while(|group| **group == 0).count();
    let trailing_zeroes = groups.iter().rev().take_while(|group| **group == 0).count();
    let retained_end = groups
        .len()
        .saturating_sub(trailing_zeroes)
        .max(leading_zeroes);
    let groups = &groups[leading_zeroes..retained_end];
    let mut weight = i32::try_from(integer_groups).map_err(|_| numeric_out_of_range_error())?
        - 1
        - i32::try_from(leading_zeroes).map_err(|_| numeric_out_of_range_error())?;
    if groups.is_empty() {
        weight = 0;
    }

    let mut encoded = Vec::with_capacity(8 + groups.len() * 2);
    encoded.extend_from_slice(
        &i16::try_from(groups.len())
            .map_err(|_| numeric_out_of_range_error())?
            .to_be_bytes(),
    );
    encoded.extend_from_slice(
        &i16::try_from(weight)
            .map_err(|_| numeric_out_of_range_error())?
            .to_be_bytes(),
    );
    encoded.extend_from_slice(&(if negative { 0x4000_u16 } else { 0 }).to_be_bytes());
    encoded.extend_from_slice(&dscale.to_be_bytes());
    for group in groups {
        encoded.extend_from_slice(&group.to_be_bytes());
    }
    Ok(encoded)
}

fn result_type_mismatch_error() -> PgWireError {
    engine_error_to_pgwire(EngineError::new(
        crate::core::EngineErrorKind::TypeMismatch,
        "a PostgreSQL result value does not match its advertised type",
    ))
}

fn numeric_out_of_range_error() -> PgWireError {
    engine_error_to_pgwire(EngineError::new(
        crate::core::EngineErrorKind::NumericOutOfRange,
        "a PostgreSQL numeric value exceeds the supported wire range",
    ))
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
    type Statement = PgWirePrepared;
    type QueryParser = PgWireQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(
            &self
                .connection
                .installed()
                .expect("the extended-query handler is called only after PostgreSQL startup")
                .wire_parser,
        )
    }

    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.sync_failed_transaction(client).await?;
        let request = self.connection.begin_request()?;
        let name = valid_extended_name(message.name.as_deref())?;
        if name != DEFAULT_NAME && client.portal_store().get_statement(name).is_some() {
            return Err(query_wire_error(
                "42P05",
                "a prepared statement with that name already exists",
            ));
        }

        if name == DEFAULT_NAME {
            self.remove_statement(client, name).await?;
        }

        for oid in &message.type_oids {
            if *oid != 0
                && !Type::from_oid(*oid)
                    .as_ref()
                    .is_some_and(supported_parameter_type)
            {
                return Err(unsupported_parameter_type_error());
            }
        }
        let parameter_types = message
            .type_oids
            .iter()
            .map(|oid| Type::from_oid(*oid))
            .collect::<Vec<_>>();
        let statement = self
            .query_parser()
            .parse_sql_with_context(client, &message.query, &parameter_types, request.context())
            .await?;
        let stored_parameter_types = statement
            .parameter_types
            .iter()
            .cloned()
            .map(Some)
            .collect();
        let statement = StoredStatement::new(name.to_owned(), statement, stored_parameter_types);
        client.portal_store().put_statement(Arc::new(statement));
        client
            .send(PgWireBackendMessage::ParseComplete(ParseComplete::new()))
            .await?;
        Ok(())
    }

    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.sync_failed_transaction(client).await?;
        let request = self.connection.begin_request()?;
        let portal_name = valid_extended_name(message.portal_name.as_deref())?;
        let statement_name = valid_extended_name(message.statement_name.as_deref())?;
        let statement = client
            .portal_store()
            .get_statement(statement_name)
            .ok_or_else(|| PgWireError::StatementNotFound(statement_name.to_owned()))?;

        let parameter_count = statement.statement.parameter_types.len();
        if message.parameters.len() != parameter_count {
            return Err(query_wire_error(
                "08P01",
                "the PostgreSQL bound parameter count does not match the statement",
            ));
        }
        if !matches!(message.parameter_format_codes.len(), 0 | 1)
            && message.parameter_format_codes.len() != parameter_count
        {
            return Err(query_wire_error(
                "08P01",
                "the PostgreSQL parameter format count does not match the parameters",
            ));
        }
        let result_formats = message.result_column_format_codes.as_slice();
        if portal_name != DEFAULT_NAME && client.portal_store().get_portal(portal_name).is_some() {
            return Err(query_wire_error(
                "42P03",
                "a portal with that name already exists",
            ));
        }
        let connection = self.connection.installed()?;
        let description = connection
            .state
            .engine
            .describe_prepared_with_context(
                &connection.state.session,
                DescribeTarget::Statement(statement.statement.id),
                request.context(),
            )
            .await
            .map_err(engine_error_to_pgwire)?;
        if !(result_formats.is_empty()
            || result_formats.len() == 1
            || result_formats.len() == description.columns().len())
        {
            return Err(query_wire_error(
                "22023",
                "the PostgreSQL result format count does not match the columns",
            ));
        }
        if portal_name == DEFAULT_NAME {
            self.remove_portal(client, portal_name).await?;
        }

        let portal = Portal::try_new(&message, Arc::clone(&statement))?;
        let parameters = decode_bound_parameters(&portal, &statement.statement.parameter_types)?;
        let fields = Arc::new(description_fields_with(&description, |index| {
            portal.result_column_format.format_for(index)
        }));

        let portal_id = connection
            .state
            .engine
            .bind_statement_with_context(
                &connection.state.session,
                statement.statement.id,
                parameters,
                request.context(),
            )
            .await
            .map_err(engine_error_to_pgwire)?;
        if let Err(error) = insert_bound_portal(
            &connection.state,
            portal_name,
            PgWireBoundPortal {
                id: portal_id,
                statement: statement.statement.id,
                fields,
            },
        ) {
            let _ = connection
                .state
                .engine
                .close_portal(&connection.state.session, portal_id)
                .await;
            return Err(error);
        }
        client.portal_store().put_portal(Arc::new(portal));
        client
            .send(PgWireBackendMessage::BindComplete(BindComplete::new()))
            .await?;
        Ok(())
    }

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.sync_failed_transaction(client).await?;
        let request = self.connection.begin_request()?;
        if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
            return Err(PgWireError::NotReadyForQuery);
        }
        let portal_name = valid_extended_name(message.name.as_deref())?;
        let max_rows = usize::try_from(message.max_rows).map_err(|_| {
            query_wire_error("22023", "the PostgreSQL Execute row limit is invalid")
        })?;
        let portal = client
            .portal_store()
            .get_portal(portal_name)
            .ok_or_else(|| PgWireError::PortalNotFound(portal_name.to_owned()))?;

        client.set_state(PgWireConnectionState::QueryInProgress);
        let portal_state_lock = portal.state();
        let mut portal_state = portal_state_lock.lock().await;
        match &mut *portal_state {
            PortalExecutionState::Initial => {
                match self
                    .execute_bound_portal(client, &portal, request.context())
                    .await?
                {
                    Response::Query(mut response) if max_rows > 0 => {
                        if send_partial_query_response(client, &mut response, max_rows).await? {
                            *portal_state = PortalExecutionState::Suspended(response);
                        } else {
                            *portal_state = PortalExecutionState::Finished;
                        }
                    }
                    Response::Query(mut response) => {
                        send_query_response(client, &mut response, false).await?;
                        *portal_state = PortalExecutionState::Finished;
                    }
                    Response::Execution(tag) => {
                        send_execution_response(client, tag).await?;
                        *portal_state = PortalExecutionState::Finished;
                    }
                    Response::TransactionStart(tag) => {
                        send_execution_response(client, tag).await?;
                        client.set_transaction_status(TransactionStatus::Transaction);
                        *portal_state = PortalExecutionState::Finished;
                    }
                    Response::TransactionEnd(tag) => {
                        send_execution_response(client, tag).await?;
                        client.set_transaction_status(TransactionStatus::Idle);
                        *portal_state = PortalExecutionState::Finished;
                    }
                    _ => return Err(internal_query_error()),
                }
            }
            PortalExecutionState::Suspended(response) => {
                if !send_partial_query_response(client, response, max_rows).await? {
                    *portal_state = PortalExecutionState::Finished;
                }
            }
            PortalExecutionState::Finished => {
                client
                    .send(PgWireBackendMessage::NoData(NoData::new()))
                    .await?;
            }
        }
        client.set_state(PgWireConnectionState::ReadyForQuery);
        Ok(())
    }

    async fn on_close<C>(&self, client: &mut C, message: Close) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = valid_extended_name(message.name.as_deref())?;
        match message.target_type {
            TARGET_TYPE_BYTE_STATEMENT => self.remove_statement(client, name).await?,
            TARGET_TYPE_BYTE_PORTAL => self.remove_portal(client, name).await?,
            target => return Err(PgWireError::InvalidTargetType(target)),
        }
        client
            .send(PgWireBackendMessage::CloseComplete(CloseComplete::new()))
            .await?;
        Ok(())
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.sync_failed_transaction(client).await?;
        let request = self.connection.begin_request()?;
        let description = self
            .connection
            .installed()?
            .state
            .engine
            .describe_prepared_with_context(
                &self.connection.installed()?.state.session,
                DescribeTarget::Statement(target.statement.id),
                request.context(),
            )
            .await
            .map_err(engine_error_to_pgwire)?;
        Ok(DescribeStatementResponse::new(
            target.statement.parameter_types.iter().cloned().collect(),
            description_fields(&description),
        ))
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.sync_failed_transaction(client).await?;
        let _request = self.connection.begin_request()?;
        let portal = bound_portal(&self.connection.installed()?.state, target.name.as_str())?
            .ok_or_else(|| PgWireError::PortalNotFound(target.name.clone()))?;
        Ok(DescribePortalResponse::new(portal.fields.as_ref().clone()))
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.sync_failed_transaction(client).await?;
        let request = self.connection.begin_request()?;
        self.execute_bound_portal(client, portal, request.context())
            .await
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
        if matches!(error, PgWireError::PortalNotFound(_)) {
            *error = query_wire_error("34000", "the PostgreSQL portal does not exist");
            return;
        }
        if matches!(error, PgWireError::StatementNotFound(_)) {
            *error = query_wire_error("26000", "the PostgreSQL prepared statement does not exist");
            return;
        }
        if matches!(error, PgWireError::InvalidPassword(_)) {
            *error = fatal_wire_error("28P01", "PostgreSQL password authentication failed");
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

#[async_trait]
impl CancelHandler for WireHandlers {
    async fn on_cancel_request(&self, request: CancelRequest) {
        if let Some(secret) = request.secret_key.as_i32() {
            self.connection
                .state
                .adapter
                .cancellations
                .cancel(request.pid, secret);
        }
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

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::new(Self {
            connection: self.connection.clone(),
        })
    }
}

type GuardedSocket = Framed<GuardedPgStream<MaybeTls>, PgWireMessageServerCodec<PgWirePrepared>>;

fn guarded_socket(stream: MaybeTls, peer: std::net::SocketAddr, secure: bool) -> GuardedSocket {
    let client = DefaultClient::<PgWirePrepared>::new(peer, secure);
    Framed::new(
        GuardedPgStream::new(stream, Vec::new()),
        PgWireMessageServerCodec::new(client),
    )
}

async fn negotiate_transport(
    stream: TcpStream,
    security: Option<&LoadedSecurity>,
) -> io::Result<Option<(GuardedSocket, Option<PgWireFrontendMessage>)>> {
    let peer = stream.peer_addr()?;
    stream.set_nodelay(true)?;

    let mut first = [0_u8; 1];
    if stream.peek(&mut first).await? > 0 && first[0] == 0x16 {
        let Some(security) = security else {
            return Ok(None);
        };
        let tls = security.tls_acceptor.accept(stream).await?;
        let (_, session) = tls.get_ref();
        if session.alpn_protocol() != Some(b"postgresql") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "direct PostgreSQL TLS requires the postgresql ALPN protocol",
            ));
        }
        let mut socket = guarded_socket(MaybeTls::Tls(Box::new(tls)), peer, true);
        socket.set_state(PgWireConnectionState::AwaitingStartup);
        return Ok(Some((socket, None)));
    }

    let mut socket = guarded_socket(MaybeTls::Plain(stream), peer, false);

    loop {
        match socket.next().await {
            Some(Ok(PgWireFrontendMessage::SslNegotiation(
                SslNegotiationMetaMessage::PostgresSsl(_),
            ))) => {
                let Some(security) = security else {
                    socket
                        .send(PgWireBackendMessage::SslResponse(SslResponse::Refuse))
                        .await?;
                    continue;
                };
                socket
                    .send(PgWireBackendMessage::SslResponse(SslResponse::Accept))
                    .await?;
                let guarded = socket.into_inner();
                let MaybeTls::Plain(stream) = guarded.inner else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "PostgreSQL TLS negotiation was repeated",
                    ));
                };
                let tls = security.tls_acceptor.accept(stream).await?;
                let mut socket = guarded_socket(MaybeTls::Tls(Box::new(tls)), peer, true);
                socket.set_state(PgWireConnectionState::AwaitingStartup);
                return Ok(Some((socket, None)));
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
                return Ok(Some((socket, None)));
            }
            Some(Ok(message @ PgWireFrontendMessage::CancelRequest(_))) => {
                return Ok(Some((socket, Some(message))));
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
        socket = negotiate_transport(stream, handlers.connection.state.adapter.security.as_deref()) => socket?,
    };
    let Some((mut socket, initial_message)) = socket else {
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

    if let Some(message) = initial_message {
        process_message(
            message,
            &mut socket,
            startup_handler,
            simple_query_handler,
            extended_query_handler,
            copy_handler,
            cancel_handler,
        )
        .await
        .map_err(io::Error::other)?;
        return Ok(());
    }

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

fn query_wire_error(code: &'static str, message: &'static str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
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

fn valid_extended_name(name: Option<&str>) -> PgWireResult<&str> {
    let name = name.unwrap_or(DEFAULT_NAME);
    if name.len() <= MAX_EXTENDED_NAME_BYTES {
        Ok(name)
    } else {
        Err(query_wire_error(
            "42622",
            "the PostgreSQL statement or portal name is too long",
        ))
    }
}

fn lock_extended_state(
    state: &ConnectionState,
) -> PgWireResult<std::sync::MutexGuard<'_, PgWireExtendedState>> {
    state.extended.lock().map_err(|_| internal_query_error())
}

fn bound_portal(state: &ConnectionState, name: &str) -> PgWireResult<Option<PgWireBoundPortal>> {
    Ok(lock_extended_state(state)?.portals.get(name).cloned())
}

fn insert_bound_portal(
    state: &ConnectionState,
    name: &str,
    portal: PgWireBoundPortal,
) -> PgWireResult<()> {
    let mut extended = lock_extended_state(state)?;
    if extended.portals.contains_key(name) {
        return Err(query_wire_error(
            "42P03",
            "a portal with that name already exists",
        ));
    }
    extended.portals.insert(name.to_owned(), portal);
    Ok(())
}

fn remove_bound_portal(
    state: &ConnectionState,
    name: &str,
) -> PgWireResult<Option<PgWireBoundPortal>> {
    Ok(lock_extended_state(state)?.portals.remove(name))
}

fn remove_bound_portals_for_statement(
    state: &ConnectionState,
    statement: PreparedStatementId,
) -> PgWireResult<Vec<String>> {
    let mut extended = lock_extended_state(state)?;
    let names = extended
        .portals
        .iter()
        .filter(|(_, portal)| portal.statement == statement)
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    for name in &names {
        extended.portals.remove(name);
    }
    Ok(names)
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
    parameter_types: Arc<Vec<Type>>,
}

impl fmt::Debug for PgWirePrepared {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PgWirePrepared")
            .field("id", &self.id)
            .field("description", &self.description)
            .field("parameter_types", &self.parameter_types)
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

impl PgWireQueryParser {
    async fn parse_sql_with_context<C>(
        &self,
        _client: &C,
        sql: &str,
        types: &[Option<Type>],
        context: RequestContext,
    ) -> PgWireResult<PgWirePrepared>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let supplied_types = types
            .iter()
            .map(|data_type| match data_type {
                Some(data_type) if supported_parameter_type(data_type) => Ok(data_type.clone()),
                Some(_) => Err(unsupported_parameter_type_error()),
                None => Ok(Type::TEXT),
            })
            .collect::<PgWireResult<Vec<_>>>()?;

        let sql = postgres_compatibility_sql(&self.state, sql);
        let request = PrepareRequest::new(
            self.state.database,
            SqlDialect::PostgreSql,
            SqlTranslationMode::Compatibility,
            sql.as_ref(),
        );
        let id = self
            .state
            .engine
            .prepare_statement_with_context(&self.state.session, request, context.clone())
            .await
            .map_err(engine_error_to_pgwire)?;
        let description = match self
            .state
            .engine
            .describe_prepared_with_context(
                &self.state.session,
                DescribeTarget::Statement(id),
                context,
            )
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

        if supplied_types.len() > description.parameter_types().len() {
            let _ = self
                .state
                .engine
                .close_prepared_statement(&self.state.session, id)
                .await;
            return Err(query_wire_error(
                "08P01",
                "the PostgreSQL parameter type count exceeds the statement parameters",
            ));
        }
        let mut parameter_types = supplied_types;
        parameter_types.resize(description.parameter_types().len(), Type::TEXT);

        Ok(PgWirePrepared {
            id,
            description,
            parameter_types: Arc::new(parameter_types),
        })
    }
}

#[async_trait]
impl QueryParser for PgWireQueryParser {
    type Statement = PgWirePrepared;

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        self.parse_sql_with_context(client, sql, types, RequestContext::new())
            .await
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
    use std::{fmt::Debug, net::SocketAddr, path::Path, sync::Arc, time::Duration};

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
    use postgres_protocol::authentication::sasl::{ChannelBinding, SCRAM_SHA_256, ScramSha256};
    use tokio::{
        io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
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

    fn test_security_config(directory: &Path) -> SecurityConfig {
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/postgres-tls");
        let certificate = directory.join("server.crt");
        let private_key = directory.join("server.key");
        let password_file = directory.join("postgres-password");
        std::fs::copy(fixtures.join("server.crt"), &certificate).unwrap();
        std::fs::copy(fixtures.join("server.key"), &private_key).unwrap();
        std::fs::write(&password_file, b"correct horse battery staple\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            std::fs::set_permissions(&private_key, std::fs::Permissions::from_mode(0o600)).unwrap();
            std::fs::set_permissions(&password_file, std::fs::Permissions::from_mode(0o640))
                .unwrap();
        }
        SecurityConfig::new(certificate, private_key, "briskdb", password_file).unwrap()
    }

    fn test_tls_connector() -> pgwire::tokio::tokio_rustls::TlsConnector {
        let mut roots = rustls::RootCertStore::empty();
        let certificate = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/postgres-tls/server.crt"
        ));
        for certificate in rustls_pemfile::certs(&mut certificate.as_slice()) {
            roots.add(certificate.unwrap()).unwrap();
        }
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        pgwire::tokio::tokio_rustls::TlsConnector::from(Arc::new(config))
    }

    type TestTlsStream = pgwire::tokio::tokio_rustls::client::TlsStream<TcpStream>;

    async fn secure_startup(
        address: SocketAddr,
        user: &str,
        password: &str,
    ) -> Result<TestTlsStream, std::collections::BTreeMap<u8, String>> {
        let mut tcp = TcpStream::connect(address).await.unwrap();
        tcp.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await.unwrap();
        let mut response = [0_u8; 1];
        tcp.read_exact(&mut response).await.unwrap();
        assert_eq!(response, *b"S");
        let server_name = rustls::pki_types::ServerName::try_from("localhost")
            .unwrap()
            .to_owned();
        let mut tls = test_tls_connector()
            .connect(server_name, tcp)
            .await
            .unwrap();
        tls.write_all(&startup_packet_with(
            196_608,
            &[("user", user), ("database", "default")],
        ))
        .await
        .unwrap();

        let (message_type, mechanisms) = read_frame(&mut tls).await;
        assert_eq!(message_type, b'R');
        assert_eq!(i32::from_be_bytes(mechanisms[..4].try_into().unwrap()), 10);
        assert!(
            mechanisms[4..]
                .windows(SCRAM_SHA_256.len())
                .any(|window| window == SCRAM_SHA_256.as_bytes())
        );

        let mut scram = ScramSha256::new(password.as_bytes(), ChannelBinding::unsupported());
        let mut initial = Vec::new();
        push_cstring(&mut initial, SCRAM_SHA_256);
        initial.extend_from_slice(&i32::try_from(scram.message().len()).unwrap().to_be_bytes());
        initial.extend_from_slice(scram.message());
        tls.write_all(&typed_packet(b'p', &initial)).await.unwrap();

        let (message_type, challenge) = read_frame(&mut tls).await;
        assert_eq!(message_type, b'R');
        assert_eq!(i32::from_be_bytes(challenge[..4].try_into().unwrap()), 11);
        scram.update(&challenge[4..]).unwrap();
        tls.write_all(&typed_packet(b'p', scram.message()))
            .await
            .unwrap();

        let (message_type, final_message) = read_frame(&mut tls).await;
        if message_type == b'E' {
            return Err(message_fields(&final_message));
        }
        assert_eq!(message_type, b'R');
        assert_eq!(
            i32::from_be_bytes(final_message[..4].try_into().unwrap()),
            12
        );
        scram.finish(&final_message[4..]).unwrap();
        let startup = read_until_ready(&mut tls).await;
        assert!(startup.iter().any(|(kind, body)| {
            *kind == b'R' && i32::from_be_bytes(body[..4].try_into().unwrap()) == 0
        }));
        Ok(tls)
    }

    async fn secure_server(
        engine: Engine,
        security: LoadedSecurity,
        connection_count: usize,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let adapter = Adapter::with_loaded_security(engine, security);
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            for _ in 0..connection_count {
                let (stream, _) = listener.accept().await.unwrap();
                let connection = adapter.wire_connection();
                connections.spawn(async move {
                    connection
                        .serve(stream, std::future::pending())
                        .await
                        .unwrap();
                });
            }
            while let Some(result) = connections.join_next().await {
                result.unwrap();
            }
        });
        (address, task)
    }

    fn cancel_packet(pid: i32, secret: i32) -> Vec<u8> {
        let mut packet = Vec::with_capacity(16);
        packet.extend_from_slice(&16_u32.to_be_bytes());
        packet.extend_from_slice(&(CANCEL_REQUEST_CODE as u32).to_be_bytes());
        packet.extend_from_slice(&pid.to_be_bytes());
        packet.extend_from_slice(&secret.to_be_bytes());
        packet
    }

    fn typed_packet(message_type: u8, body: &[u8]) -> Vec<u8> {
        let length = u32::try_from(body.len() + 4).unwrap();
        let mut packet = Vec::with_capacity(body.len() + 5);
        packet.push(message_type);
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(body);
        packet
    }

    fn push_cstring(body: &mut Vec<u8>, value: &str) {
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }

    fn parse_packet(name: &str, query: &str) -> Vec<u8> {
        parse_packet_with_oids(name, query, &[])
    }

    fn parse_packet_with_oids(name: &str, query: &str, type_oids: &[u32]) -> Vec<u8> {
        let mut body = Vec::new();
        push_cstring(&mut body, name);
        push_cstring(&mut body, query);
        body.extend_from_slice(&u16::try_from(type_oids.len()).unwrap().to_be_bytes());
        for oid in type_oids {
            body.extend_from_slice(&oid.to_be_bytes());
        }
        typed_packet(b'P', &body)
    }

    fn bind_packet(portal: &str, statement: &str) -> Vec<u8> {
        bind_packet_with(portal, statement, &[], &[], &[])
    }

    fn bind_packet_with(
        portal: &str,
        statement: &str,
        parameter_formats: &[i16],
        parameters: &[Option<&[u8]>],
        result_formats: &[i16],
    ) -> Vec<u8> {
        let mut body = Vec::new();
        push_cstring(&mut body, portal);
        push_cstring(&mut body, statement);
        body.extend_from_slice(
            &u16::try_from(parameter_formats.len())
                .unwrap()
                .to_be_bytes(),
        );
        for format in parameter_formats {
            body.extend_from_slice(&format.to_be_bytes());
        }
        body.extend_from_slice(&u16::try_from(parameters.len()).unwrap().to_be_bytes());
        for parameter in parameters {
            match parameter {
                Some(parameter) => {
                    body.extend_from_slice(&i32::try_from(parameter.len()).unwrap().to_be_bytes());
                    body.extend_from_slice(parameter);
                }
                None => body.extend_from_slice(&(-1_i32).to_be_bytes()),
            }
        }
        body.extend_from_slice(&i16::try_from(result_formats.len()).unwrap().to_be_bytes());
        for format in result_formats {
            body.extend_from_slice(&format.to_be_bytes());
        }
        typed_packet(b'B', &body)
    }

    fn describe_packet(target: u8, name: &str) -> Vec<u8> {
        let mut body = vec![target];
        push_cstring(&mut body, name);
        typed_packet(b'D', &body)
    }

    fn execute_packet(portal: &str, max_rows: i32) -> Vec<u8> {
        let mut body = Vec::new();
        push_cstring(&mut body, portal);
        body.extend_from_slice(&max_rows.to_be_bytes());
        typed_packet(b'E', &body)
    }

    fn close_packet(target: u8, name: &str) -> Vec<u8> {
        let mut body = vec![target];
        push_cstring(&mut body, name);
        typed_packet(b'C', &body)
    }

    async fn read_frame<S>(stream: &mut S) -> (u8, Vec<u8>)
    where
        S: AsyncRead + Unpin,
    {
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

    async fn read_until_ready<S>(stream: &mut S) -> Vec<(u8, Vec<u8>)>
    where
        S: AsyncRead + Unpin,
    {
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

    fn backend_key(body: &[u8]) -> BackendKey {
        assert_eq!(body.len(), 8);
        BackendKey {
            pid: i32::from_be_bytes(body[..4].try_into().unwrap()),
            secret: i32::from_be_bytes(body[4..].try_into().unwrap()),
        }
    }

    fn negotiated_protocol(body: &[u8]) -> (i32, Vec<String>) {
        let newest_minor = i32::from_be_bytes(body[..4].try_into().unwrap());
        let option_count =
            usize::try_from(i32::from_be_bytes(body[4..8].try_into().unwrap())).unwrap();
        let mut options = Vec::with_capacity(option_count);
        let mut offset = 8;
        for _ in 0..option_count {
            let end = body[offset..]
                .iter()
                .position(|byte| *byte == 0)
                .map(|length| offset + length)
                .unwrap();
            options.push(std::str::from_utf8(&body[offset..end]).unwrap().to_owned());
            offset = end + 1;
        }
        assert_eq!(offset, body.len());
        (newest_minor, options)
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
        row_description_fields(body)
            .into_iter()
            .map(|(name, oid, _)| (name, oid))
            .collect()
    }

    fn row_description_fields(body: &[u8]) -> Vec<(String, u32, i16)> {
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
            offset += 4 + 2 + 4;
            let format = i16::from_be_bytes(body[offset..offset + 2].try_into().unwrap());
            offset += 2;
            fields.push((name, oid, format));
        }
        assert_eq!(offset, body.len());
        fields
    }

    fn parameter_description(body: &[u8]) -> Vec<u32> {
        let count = usize::from(u16::from_be_bytes(body[..2].try_into().unwrap()));
        assert_eq!(body.len(), 2 + count * 4);
        body[2..]
            .chunks_exact(4)
            .map(|bytes| u32::from_be_bytes(bytes.try_into().unwrap()))
            .collect()
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

        let invalid_result = ResultSet::new(
            vec![crate::core::Column::new("invalid", DataType::Text)],
            vec![crate::core::Row::new(vec![Value::InvalidText(vec![0x80])])],
        )
        .unwrap();
        let Err(PgWireError::UserError(info)) = result_set_response(invalid_result) else {
            panic!("result response encoding must preserve the fixed invalid-text error")
        };
        assert_eq!(info.code, "22021");

        let mismatched_result = ResultSet::new(
            vec![crate::core::Column::new("count", DataType::Int64)],
            vec![crate::core::Row::new(vec![Value::Text(
                "not an integer".to_owned(),
            )])],
        )
        .unwrap();
        let Err(PgWireError::UserError(info)) = result_set_response(mismatched_result) else {
            panic!("result response encoding must preserve the fixed type-mismatch error")
        };
        assert_eq!(info.code, "42804");
    }

    #[test]
    fn postgres_binary_values_and_numeric_groups_are_exact() {
        let fields = Arc::new(vec![
            FieldInfo::new(
                "bool".to_owned(),
                None,
                None,
                Type::BOOL,
                FieldFormat::Binary,
            ),
            FieldInfo::new(
                "int".to_owned(),
                None,
                None,
                Type::INT8,
                FieldFormat::Binary,
            ),
            FieldInfo::new(
                "numeric".to_owned(),
                None,
                None,
                Type::NUMERIC,
                FieldFormat::Binary,
            ),
            FieldInfo::new(
                "float".to_owned(),
                None,
                None,
                Type::FLOAT8,
                FieldFormat::Binary,
            ),
            FieldInfo::new(
                "text".to_owned(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Binary,
            ),
            FieldInfo::new(
                "bytea".to_owned(),
                None,
                None,
                Type::BYTEA,
                FieldFormat::Binary,
            ),
        ]);
        let row = encode_data_row(
            fields,
            vec![
                Value::Int64(1),
                Value::Int64(-42),
                Value::decimal("12.3400").unwrap(),
                Value::Float64(1.5),
                Value::Text("hello".to_owned()),
                Value::Binary(vec![0, 255]),
            ],
        )
        .unwrap();
        let mut body = row.field_count.to_be_bytes().to_vec();
        body.extend_from_slice(&row.data);
        let numeric = [
            0, 2, // two base-10000 digits
            0, 0, // weight zero
            0, 0, // positive
            0, 4, // four decimal places
            0, 12, // 12
            13, 72, // 3400
        ];
        assert_eq!(
            data_row(&body),
            [
                Some(vec![1]),
                Some((-42_i64).to_be_bytes().to_vec()),
                Some(numeric.to_vec()),
                Some(1.5_f64.to_be_bytes().to_vec()),
                Some(b"hello".to_vec()),
                Some(vec![0, 255]),
            ]
        );
        assert_eq!(
            decode_numeric_binary(&numeric).unwrap(),
            Value::decimal("12.3400").unwrap()
        );
        for value in ["0", "-0.0012", "10000", ".5", "1e3", "1e-3"] {
            let encoded = encode_numeric_binary(value).unwrap();
            let Value::Decimal(decoded) = decode_numeric_binary(&encoded).unwrap() else {
                panic!("finite PostgreSQL numeric decoded as another BriskDB type")
            };
            let expected = match value {
                ".5" => "0.5",
                "1e3" => "1000",
                "1e-3" => "0.001",
                value => value,
            };
            assert_eq!(decoded.as_str(), expected);
        }
        let invalid_bytea = decode_bytea_text(br"\777").unwrap_err();
        let PgWireError::UserError(info) = invalid_bytea else {
            panic!("invalid bytea must use the fixed parameter error")
        };
        assert_eq!(info.code, "22P02");
    }

    #[test]
    fn generated_insert_uses_the_standard_command_tag_without_unsolicited_rows() {
        let execution =
            PreparedExecution::GeneratedWrite(crate::core::WriteResult::with_generated_key(
                1,
                crate::core::GeneratedKey::new("id", Value::Int64(4_620_693_217_682_128_897)),
            ));
        let response = execution_response(
            StatementBehavior::Write(WriteBehavior::Insert),
            execution,
            None,
        )
        .unwrap();
        let Response::Execution(tag) = response else {
            panic!("PostgreSQL must not invent a result row without RETURNING")
        };
        let complete: pgwire::messages::response::CommandComplete = tag.into();
        assert_eq!(complete.tag, "INSERT 0 1");
        assert!(!complete.tag.contains("4620693217682128897"));
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

    #[test]
    fn cancellation_registry_requires_the_exact_key_and_scopes_each_request_token() {
        let registry = Arc::new(CancellationRegistry::default());
        let key = registry.issue().unwrap();
        assert_eq!(registry.len(), 1);
        assert_eq!(registry.active_len(), 0);

        let first = registry.begin(key).unwrap();
        let first_generation = first.generation;
        assert_eq!(registry.active_len(), 1);
        assert!(!registry.cancel(key.pid.wrapping_add(1), key.secret));
        assert!(!registry.cancel(key.pid, key.secret ^ 1));
        assert!(!first.cancellation.is_cancelled());
        assert!(registry.cancel(key.pid, key.secret));
        assert!(first.cancellation.is_cancelled());
        drop(first);
        assert_eq!(registry.active_len(), 0);
        assert!(!registry.cancel(key.pid, key.secret));

        let second = registry.begin(key).unwrap();
        assert!(!second.cancellation.is_cancelled());
        registry.finish(key, first_generation);
        assert_eq!(registry.active_len(), 1);
        assert!(registry.cancel(key.pid, key.secret));
        assert!(second.cancellation.is_cancelled());
        registry.unregister(key);
        assert!(second.cancellation.is_cancelled());
        assert_eq!(registry.len(), 0);
        drop(second);
    }

    #[test]
    fn security_configuration_validates_identity_secrets_and_tls_material() {
        let temp = tempfile::tempdir().unwrap();
        let config = test_security_config(temp.path());
        let loaded = config.load().unwrap();
        assert_eq!(loaded.credentials.user.as_ref(), "briskdb");
        assert!(!format!("{loaded:?}").contains("correct horse"));
        assert!(
            SecurityConfig::new(
                config.certificate(),
                config.private_key(),
                "Invalid-User",
                config.password_file(),
            )
            .is_err()
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            std::fs::set_permissions(
                config.password_file(),
                std::fs::Permissions::from_mode(0o644),
            )
            .unwrap();
            let error = config.load().unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
            assert!(!error.to_string().contains("correct horse"));
        }
    }

    #[test]
    fn compatibility_shims_are_parser_bounded_and_finite() {
        for query in [
            "SELECT version()",
            " select pg_catalog.version() ; ",
            "SELECT current_database()",
            "SELECT pg_catalog.current_database()",
            "SELECT current_schema()",
            "SELECT pg_catalog.current_schema()",
            "SELECT current_user",
            "SELECT session_user",
            "SHOW server_version",
            "-- client probe\nSHOW server_version_num",
            "SHOW server_encoding",
            "SHOW client_encoding",
            "SHOW standard_conforming_strings",
            "SHOW integer_datetimes",
            "SHOW TRANSACTION ISOLATION LEVEL",
            "SHOW transaction_read_only",
            "SHOW search_path",
            "SHOW TimeZone",
            "SHOW DateStyle",
            "SHOW IntervalStyle",
            "SHOW extra_float_digits",
            "SHOW default_transaction_isolation",
            "SHOW default_transaction_read_only",
            "SHOW session_authorization",
            "SELECT current_setting('server_version')",
            "SELECT current_setting('server_version_num')",
            "SELECT current_setting('standard_conforming_strings')",
            "SELECT current_setting('TimeZone')",
            "SELECT typname AS name, oid, typarray AS array_oid, \
             oid::regtype::text AS regtype, typdelim AS delimiter \
             FROM pg_type t WHERE t.oid = to_regtype($1) ORDER BY t.oid",
        ] {
            assert!(
                postgres_compatibility_shim(query).is_some(),
                "expected a compatibility shim for {query:?}"
            );
        }

        for query in [
            "SELECT version() AS invented_alias",
            "SELECT version() FROM application_table",
            "SELECT version(); SELECT 1",
            "SHOW work_mem",
            "SELECT current_setting('work_mem')",
            "SELECT * FROM pg_catalog.pg_class",
        ] {
            assert!(
                postgres_compatibility_shim(query).is_none(),
                "unexpected compatibility shim for {query:?}"
            );
        }
    }

    #[tokio::test]
    async fn secure_listener_requires_tls_and_returns_a_fixed_startup_error() {
        let (temp, engine) = engine(2).await;
        let security = test_security_config(temp.path()).load().unwrap();
        let (address, server) = secure_server(engine.clone(), security, 1).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();
        let (kind, body) = read_frame(&mut client).await;
        assert_eq!(kind, b'E');
        let fields = message_fields(&body);
        assert_eq!(fields.get(&b'C').map(String::as_str), Some("08004"));
        assert_eq!(
            fields.get(&b'M').map(String::as_str),
            Some("TLS is required for PostgreSQL authentication")
        );
        assert_eof(&mut client).await;
        server.await.unwrap();
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn tls_scram_supports_queries_concurrency_and_recovery_after_bad_password() {
        let (temp, engine) = engine(2).await;
        let security = test_security_config(temp.path()).load().unwrap();
        let (address, server) = secure_server(engine.clone(), security, 5).await;

        let error = secure_startup(address, "briskdb", "wrong password")
            .await
            .unwrap_err();
        assert_eq!(error.get(&b'C').map(String::as_str), Some("28P01"));
        assert_eq!(
            error.get(&b'M').map(String::as_str),
            Some("PostgreSQL password authentication failed")
        );

        // The decoy credential keeps unknown-user proof work comparable, but
        // must never become a usable account even if its source value is known.
        let error = secure_startup(
            address,
            "unknown_user",
            "briskdb-invalid-authentication-secret",
        )
        .await
        .unwrap_err();
        assert_eq!(error.get(&b'C').map(String::as_str), Some("28P01"));

        let (first, second) = tokio::join!(
            secure_startup(address, "briskdb", "correct horse battery staple"),
            secure_startup(address, "briskdb", "correct horse battery staple"),
        );
        let mut first = first.unwrap();
        let mut second = second.unwrap();
        for client in [&mut first, &mut second] {
            client
                .write_all(&typed_packet(b'Q', b"SELECT 1\0"))
                .await
                .unwrap();
            let frames = read_until_ready(client).await;
            assert!(frames.iter().any(|(kind, _)| *kind == b'D'));
            client.write_all(&typed_packet(b'X', &[])).await.unwrap();
        }
        drop(first);
        drop(second);

        let mut recovered = secure_startup(address, "briskdb", "correct horse battery staple")
            .await
            .unwrap();
        recovered
            .write_all(&typed_packet(b'Q', b"SELECT 2\0"))
            .await
            .unwrap();
        let frames = read_until_ready(&mut recovered).await;
        assert!(frames.iter().any(|(kind, _)| *kind == b'D'));
        recovered.write_all(&typed_packet(b'X', &[])).await.unwrap();
        drop(recovered);

        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("all secure PostgreSQL connections should finish")
            .unwrap();
        engine.shutdown().await.unwrap();
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
        let keys = frames
            .iter()
            .filter(|frame| frame.0 == b'K')
            .map(|frame| backend_key(&frame.1))
            .collect::<Vec<_>>();
        assert_eq!(keys.len(), 1);
        assert_ne!(keys[0].pid, 0);
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            vec![b'R', b'S', b'S', b'S', b'S', b'S', b'S', b'S', b'K', b'Z']
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
                ("briskdb_version".to_owned(), BRISKDB_VERSION.to_owned()),
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
        assert_eq!(adapter.cancellations.len(), 1);

        finish_wire_server(&mut client, server).await;
        assert_eq!(core.state().await, SessionState::Closed);
        assert_eq!(adapter.cancellations.len(), 0);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn simple_compatibility_probes_return_exact_bounded_text_rows() {
        let (_temp, engine) = engine(2).await;
        let adapter = Adapter::new(engine.clone());
        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();
        read_until_ready(&mut client).await;

        for (query, column, value) in [
            ("SELECT version()", "version", VERSION_BANNER),
            ("SELECT pg_catalog.version()", "version", VERSION_BANNER),
            ("SHOW server_version", "server_version", SERVER_VERSION),
            (
                "SHOW server_version_num",
                "server_version_num",
                POSTGRES_COMPATIBILITY_VERSION_NUM,
            ),
            (
                "SHOW transaction isolation level",
                "transaction_isolation",
                "serializable",
            ),
            (
                "SHOW standard_conforming_strings",
                "standard_conforming_strings",
                "on",
            ),
            ("SHOW TimeZone", "TimeZone", "UTC"),
            ("SHOW search_path", "search_path", "public"),
            ("SELECT current_database()", "current_database", "default"),
            ("SELECT current_schema()", "current_schema", "public"),
            ("SELECT current_user", "current_user", "briskdb"),
            (
                "SHOW session_authorization",
                "session_authorization",
                "briskdb",
            ),
        ] {
            let mut packet = query.as_bytes().to_vec();
            packet.push(0);
            client
                .write_all(&typed_packet(b'Q', &packet))
                .await
                .unwrap();
            let frames = read_until_ready(&mut client).await;
            assert_eq!(
                frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
                [b'T', b'D', b'C', b'Z'],
                "unexpected response shape for {query:?}"
            );
            assert_eq!(
                row_description(&frames[0].1),
                [(column.to_owned(), Type::TEXT.oid())]
            );
            assert_eq!(data_row(&frames[1].1), [Some(value.as_bytes().to_vec())]);
            assert_eq!(command_tag(&frames[2].1), "SELECT 1");
        }

        client
            .write_all(&typed_packet(b'Q', b"SHOW work_mem\0"))
            .await
            .unwrap();
        let rejected = read_until_ready(&mut client).await;
        assert_eq!(
            rejected.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'E', b'Z']
        );
        let fields = message_fields(&rejected[0].1);
        assert_eq!(fields.get(&b'C').map(String::as_str), Some("0A000"));

        client
            .write_all(&typed_packet(b'Q', b"SELECT version()\0"))
            .await
            .unwrap();
        let recovery = read_until_ready(&mut client).await;
        assert_eq!(
            data_row(&recovery[1].1),
            [Some(VERSION_BANNER.as_bytes().to_vec())]
        );

        finish_wire_server(&mut client, server).await;
        assert_eq!(
            wire.connection().unwrap().state().await,
            SessionState::Closed
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn extended_compatibility_probe_describes_binds_executes_and_recovers() {
        let (_temp, engine) = engine(2).await;
        let adapter = Adapter::new(engine.clone());
        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();
        read_until_ready(&mut client).await;

        let flow = [
            parse_packet("orm_version", "SELECT pg_catalog.version()"),
            bind_packet("orm_portal", "orm_version"),
            describe_packet(TARGET_TYPE_BYTE_STATEMENT, "orm_version"),
            describe_packet(TARGET_TYPE_BYTE_PORTAL, "orm_portal"),
            execute_packet("orm_portal", 0),
            typed_packet(b'S', &[]),
        ]
        .concat();
        client.write_all(&flow).await.unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'1', b'2', b't', b'T', b'T', b'D', b'C', b'Z']
        );
        assert_eq!(
            row_description(&frames[3].1),
            [("version".to_owned(), Type::TEXT.oid())]
        );
        assert_eq!(row_description(&frames[3].1), row_description(&frames[4].1));
        assert_eq!(
            data_row(&frames[5].1),
            [Some(VERSION_BANNER.as_bytes().to_vec())]
        );

        let type_probe = [
            parse_packet(
                "psycopg_type",
                "SELECT typname AS name, oid, typarray AS array_oid, \
                 oid::regtype::text AS regtype, typdelim AS delimiter \
                 FROM pg_type t WHERE t.oid = to_regtype($1) ORDER BY t.oid",
            ),
            bind_packet_with(
                "psycopg_type_portal",
                "psycopg_type",
                &[],
                &[Some(b"hstore")],
                &[],
            ),
            describe_packet(TARGET_TYPE_BYTE_STATEMENT, "psycopg_type"),
            describe_packet(TARGET_TYPE_BYTE_PORTAL, "psycopg_type_portal"),
            execute_packet("psycopg_type_portal", 0),
            typed_packet(b'S', &[]),
        ]
        .concat();
        client.write_all(&type_probe).await.unwrap();
        let type_frames = read_until_ready(&mut client).await;
        assert_eq!(
            type_frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'1', b'2', b't', b'T', b'T', b'C', b'Z']
        );
        assert_eq!(
            row_description(&type_frames[3].1)
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            ["name", "oid", "array_oid", "regtype", "delimiter"]
        );
        assert_eq!(command_tag(&type_frames[5].1), "SELECT 0");

        client
            .write_all(&typed_packet(b'Q', b"SHOW client_encoding\0"))
            .await
            .unwrap();
        let recovery = read_until_ready(&mut client).await;
        assert_eq!(data_row(&recovery[1].1), [Some(b"UTF8".to_vec())]);

        finish_wire_server(&mut client, server).await;
        assert_eq!(
            wire.connection().unwrap().state().await,
            SessionState::Closed
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_request_interrupts_the_active_transaction_and_rollback_recovers() {
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
        let shard = database.shard_for_key(b"tenant-cancel");
        let engine = Engine::from_database(Arc::new(database));
        let adapter = Adapter::new(engine.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let wire = adapter.wire_connection();
        let served = wire.clone();
        let cancellation_adapter = adapter.clone();
        let (main_tx, main_rx) = tokio::sync::oneshot::channel();
        let cancellation_server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let main =
                tokio::spawn(async move { served.serve(stream, std::future::pending()).await });
            main_tx.send(main).unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            cancellation_adapter
                .wire_connection()
                .serve(stream, std::future::pending())
                .await
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        let main_server = main_rx.await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();
        let startup = read_until_ready(&mut client).await;
        let key = startup
            .iter()
            .find(|frame| frame.0 == b'K')
            .map(|frame| backend_key(&frame.1))
            .expect("production startup emits one backend cancellation key");
        client
            .write_all(&typed_packet(b'Q', b"BEGIN\0"))
            .await
            .unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(frames.last().unwrap(), &(b'Z', vec![b'T']));
        let lock = rusqlite::Connection::open(
            temp.path()
                .join("shards")
                .join(format!("{shard:04}.sqlite")),
        )
        .unwrap();
        lock.execute_batch("BEGIN IMMEDIATE").unwrap();
        client
            .write_all(&typed_packet(
                b'Q',
                b"INSERT INTO records (tenant_id, payload) VALUES ('tenant-cancel', 'blocked')\0",
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while adapter.cancellations.active_len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the blocked request entered cancellation admission");
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut cancellation = TcpStream::connect(address).await.unwrap();
        cancellation
            .write_all(&cancel_packet(key.pid, key.secret))
            .await
            .unwrap();
        assert_eof(&mut cancellation).await;
        drop(cancellation);
        tokio::time::timeout(Duration::from_secs(2), cancellation_server)
            .await
            .expect("the CancelRequest connection returned")
            .unwrap()
            .unwrap();

        let frames = read_until_ready(&mut client).await;
        assert_eq!(frames[0].0, b'E');
        assert_eq!(
            message_fields(&frames[0].1).get(&b'C').map(String::as_str),
            Some("57014")
        );
        assert_eq!(frames.last().unwrap(), &(b'Z', vec![b'E']));
        assert_eq!(adapter.cancellations.active_len(), 0);
        lock.execute_batch("ROLLBACK").unwrap();

        client
            .write_all(&typed_packet(b'Q', b"ROLLBACK\0"))
            .await
            .unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(frames.last().unwrap(), &(b'Z', vec![b'I']));

        client
            .write_all(&typed_packet(
                b'Q',
                b"SELECT tenant_id FROM records WHERE tenant_id = 'tenant-cancel'\0",
            ))
            .await
            .unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(frames.last().unwrap(), &(b'Z', vec![b'I']));

        client.write_all(&typed_packet(b'X', &[])).await.unwrap();
        assert_eof(&mut client).await;
        tokio::time::timeout(Duration::from_secs(2), main_server)
            .await
            .expect("the cancelled PostgreSQL backend connection returned")
            .unwrap()
            .unwrap();
        assert_eq!(adapter.cancellations.len(), 0);
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
    async fn newer_minor_versions_and_protocol_options_negotiate_to_the_exact_baseline() {
        let (_temp, engine) = engine(2).await;
        let adapter = Adapter::new(engine.clone());
        let cases = [
            (1_u16, Vec::new()),
            (2, Vec::new()),
            (
                u16::MAX,
                vec![("_pq_.feature_z", "enabled"), ("_pq_.feature_a", "enabled")],
            ),
            (0, vec![("_pq_.feature_only", "enabled")]),
        ];

        for (minor, options) in cases {
            let (address, wire, server) = spawn_wire_server(&adapter).await;
            let mut client = TcpStream::connect(address).await.unwrap();
            let mut parameters = vec![("user", "client_user"), ("database", "default")];
            parameters.extend(options.iter().copied());
            let protocol = (u32::from(POSTGRES_PROTOCOL_MAJOR) << 16) | u32::from(minor);
            client
                .write_all(&startup_packet_with(protocol, &parameters))
                .await
                .unwrap();

            let frames = read_until_ready(&mut client).await;
            assert_eq!(frames.first().unwrap().0, b'v');
            assert_eq!(frames.get(1).unwrap(), &(b'R', vec![0, 0, 0, 0]));
            assert_eq!(frames.last().unwrap(), &(b'Z', vec![b'I']));
            assert!(frames.iter().filter(|frame| frame.0 == b'S').any(|frame| {
                parameter_status(&frame.1)
                    == ("server_version".to_owned(), SERVER_VERSION.to_owned())
            }));
            let expected_options = options
                .iter()
                .map(|(name, _)| (*name).to_owned())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            assert_eq!(
                negotiated_protocol(&frames[0].1),
                (i32::from(POSTGRES_PROTOCOL_MINOR), expected_options)
            );

            client.write_all(&typed_packet(b'Q', &[0])).await.unwrap();
            assert_eq!(
                read_until_ready(&mut client).await,
                [(b'I', vec![]), (b'Z', vec![b'I'])]
            );
            let core = Arc::clone(wire.connection().unwrap());
            assert_eq!(core.state().await, SessionState::Ready);
            finish_wire_server(&mut client, server).await;
            assert_eq!(core.state().await, SessionState::Closed);
        }

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
                startup_packet_with(262_144, &[("user", "client_user"), ("database", "default")]),
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
        assert_eq!(adapter.cancellations.len(), 0);
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
                RequestContext::new(),
            )
            .await
            .unwrap_err();
        assert_eq!(duplicate.kind(), EngineErrorKind::UniqueViolation);
        for _ in 0..130 {
            core.execute_simple_query("SELECT 1", RequestContext::new())
                .await
                .unwrap();
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

    #[tokio::test]
    async fn simple_query_reports_real_transaction_status_and_requires_rollback_after_error() {
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
        assert_eq!(
            read_until_ready(&mut client).await.last().unwrap().1,
            [b'I']
        );
        let core = Arc::clone(wire.connection().unwrap());

        client
            .write_all(&typed_packet(b'Q', b"BEGIN\0"))
            .await
            .unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(command_tag(&frames[0].1), "BEGIN");
        assert_eq!(frames.last().unwrap(), &(b'Z', vec![b'T']));
        assert_eq!(core.state().await, SessionState::InTransaction);

        for payload in ["first", "duplicate"] {
            client
                .write_all(&typed_packet(
                    b'Q',
                    format!(
                        "INSERT INTO records (tenant_id, payload) VALUES ('tenant-tx', '{payload}')\0"
                    )
                    .as_bytes(),
                ))
                .await
                .unwrap();
            let frames = read_until_ready(&mut client).await;
            if payload == "first" {
                assert_eq!(command_tag(&frames[0].1), "INSERT 0 1");
                assert_eq!(frames.last().unwrap(), &(b'Z', vec![b'T']));
            } else {
                assert_eq!(frames[0].0, b'E');
                assert_eq!(frames.last().unwrap(), &(b'Z', vec![b'E']));
            }
        }
        assert_eq!(core.state().await, SessionState::FailedTransaction);

        client
            .write_all(&typed_packet(
                b'Q',
                b"SELECT payload FROM records WHERE tenant_id = 'tenant-tx'\0",
            ))
            .await
            .unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            message_fields(&frames[0].1).get(&b'C').map(String::as_str),
            Some("25P02")
        );
        assert_eq!(frames.last().unwrap(), &(b'Z', vec![b'E']));

        client
            .write_all(&typed_packet(b'Q', b"ROLLBACK\0"))
            .await
            .unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(command_tag(&frames[0].1), "ROLLBACK");
        assert_eq!(frames.last().unwrap(), &(b'Z', vec![b'I']));
        assert_eq!(core.state().await, SessionState::Ready);

        finish_wire_server(&mut client, server).await;
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn extended_sync_reports_transaction_and_idle_status() {
        let (_temp, engine) = engine(2).await;
        let adapter = Adapter::new(engine.clone());
        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();
        read_until_ready(&mut client).await;
        let core = Arc::clone(wire.connection().unwrap());

        let begin = [
            parse_packet("begin", "BEGIN"),
            bind_packet("begin_portal", "begin"),
            execute_packet("begin_portal", 0),
            typed_packet(b'S', &[]),
        ]
        .concat();
        client.write_all(&begin).await.unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'1', b'2', b'C', b'Z']
        );
        assert_eq!(command_tag(&frames[2].1), "BEGIN");
        assert_eq!(frames.last().unwrap(), &(b'Z', vec![b'T']));
        assert_eq!(core.state().await, SessionState::InTransaction);

        let rollback = [
            parse_packet("rollback", "ROLLBACK"),
            bind_packet("rollback_portal", "rollback"),
            execute_packet("rollback_portal", 0),
            typed_packet(b'S', &[]),
        ]
        .concat();
        client.write_all(&rollback).await.unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'1', b'2', b'C', b'Z']
        );
        assert_eq!(command_tag(&frames[2].1), "ROLLBACK");
        assert_eq!(frames.last().unwrap(), &(b'Z', vec![b'I']));
        assert_eq!(core.state().await, SessionState::Ready);

        finish_wire_server(&mut client, server).await;
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn extended_query_executes_named_writes_reads_describe_and_close() {
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
        let engine = Engine::from_database(Arc::clone(&database));
        let adapter = Adapter::new(engine.clone());
        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();
        read_until_ready(&mut client).await;

        let write_flow = [
            parse_packet(
                "write_record",
                "INSERT INTO records (tenant_id, payload) VALUES ('tenant-e', 'extended')",
            ),
            bind_packet("write_portal", "write_record"),
            execute_packet("write_portal", 0),
            execute_packet("write_portal", 0),
            typed_packet(b'S', &[]),
        ]
        .concat();
        client.write_all(&write_flow).await.unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'1', b'2', b'C', b'n', b'Z']
        );
        assert_eq!(command_tag(&frames[2].1), "INSERT 0 1");

        let read_flow = [
            parse_packet(
                "read_record",
                "SELECT tenant_id, payload FROM records WHERE tenant_id = 'tenant-e'",
            ),
            bind_packet("read_portal", "read_record"),
            describe_packet(TARGET_TYPE_BYTE_STATEMENT, "read_record"),
            describe_packet(TARGET_TYPE_BYTE_PORTAL, "read_portal"),
            execute_packet("read_portal", 1),
            execute_packet("read_portal", 1),
            typed_packet(b'H', &[]),
            typed_packet(b'S', &[]),
        ]
        .concat();
        client.write_all(&read_flow).await.unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'1', b'2', b't', b'T', b'T', b'D', b's', b'C', b'Z']
        );
        assert_eq!(u16::from_be_bytes(frames[2].1[..2].try_into().unwrap()), 0);
        assert_eq!(row_description(&frames[3].1), row_description(&frames[4].1));
        assert_eq!(
            data_row(&frames[5].1),
            [Some(b"tenant-e".to_vec()), Some(b"extended".to_vec())]
        );

        client
            .write_all(
                &[
                    close_packet(TARGET_TYPE_BYTE_STATEMENT, "read_record"),
                    typed_packet(b'S', &[]),
                ]
                .concat(),
            )
            .await
            .unwrap();
        assert_eq!(
            read_until_ready(&mut client).await,
            [(b'3', vec![]), (b'Z', vec![b'I'])]
        );

        client
            .write_all(&[execute_packet("read_portal", 0), typed_packet(b'S', &[])].concat())
            .await
            .unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'E', b'Z']
        );

        for shard in 0..database.shard_count() {
            let rows =
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap()
                    .query_row("SELECT COUNT(*) FROM records", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap();
            assert!(rows <= 1, "write was replayed on physical shard {shard}");
        }
        let total_rows = (0..database.shard_count())
            .map(|shard| {
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap()
                    .query_row("SELECT COUNT(*) FROM records", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap()
            })
            .sum::<i64>();
        assert_eq!(total_rows, 1, "a finished write portal must not re-execute");

        finish_wire_server(&mut client, server).await;
        assert_eq!(
            wire.connection().unwrap().state().await,
            SessionState::Closed
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn extended_parameters_and_binary_results_use_declared_postgres_types() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = crate::core::Database::open(temp.path(), 2).unwrap();
        database
            .broadcast(
                "CREATE TABLE typed_records (
                    tenant_id TEXT NOT NULL PRIMARY KEY,
                    enabled BOOLEAN NOT NULL,
                    count_value INTEGER NOT NULL,
                    ratio REAL NOT NULL,
                    payload BLOB NOT NULL
                 )",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                crate::core::TableDeclaration::sharded(
                    logical_database,
                    "typed_records",
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

        let insert_types = [
            Type::TEXT.oid(),
            Type::BOOL.oid(),
            Type::INT8.oid(),
            Type::FLOAT8.oid(),
            Type::BYTEA.oid(),
        ];
        let ratio = 1.5_f64.to_be_bytes();
        let insert = [
            parse_packet_with_oids(
                "insert_typed",
                "INSERT INTO typed_records
                 (tenant_id, enabled, count_value, ratio, payload)
                 VALUES ($1, $2, $3, $4, $5)",
                &insert_types,
            ),
            bind_packet_with(
                "insert_portal",
                "insert_typed",
                &[0, 1, 0, 1, 1],
                &[
                    Some(b"tenant-b"),
                    Some(&[1]),
                    Some(b"42"),
                    Some(&ratio),
                    Some(&[0, 255]),
                ],
                &[],
            ),
            execute_packet("insert_portal", 0),
            typed_packet(b'S', &[]),
        ]
        .concat();
        client.write_all(&insert).await.unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'1', b'2', b'C', b'Z']
        );
        assert_eq!(command_tag(&frames[2].1), "INSERT 0 1");

        client
            .write_all(
                &[
                    parse_packet_with_oids(
                        "select_typed",
                        "SELECT tenant_id, enabled, count_value, ratio, payload
                         FROM typed_records WHERE tenant_id = $1",
                        &[Type::TEXT.oid()],
                    ),
                    describe_packet(TARGET_TYPE_BYTE_STATEMENT, "select_typed"),
                ]
                .concat(),
            )
            .await
            .unwrap();
        assert_eq!(read_frame(&mut client).await, (b'1', vec![]));
        let parameter_frame = read_frame(&mut client).await;
        assert_eq!(parameter_frame.0, b't');
        assert_eq!(
            parameter_description(&parameter_frame.1),
            [Type::TEXT.oid()]
        );
        let statement_fields = read_frame(&mut client).await;
        assert_eq!(statement_fields.0, b'T');
        assert_eq!(
            row_description_fields(&statement_fields.1),
            [
                ("tenant_id".to_owned(), Type::TEXT.oid(), 0),
                ("enabled".to_owned(), Type::BOOL.oid(), 0),
                ("count_value".to_owned(), Type::INT8.oid(), 0),
                ("ratio".to_owned(), Type::FLOAT8.oid(), 0),
                ("payload".to_owned(), Type::BYTEA.oid(), 0),
            ]
        );

        let read = [
            bind_packet_with(
                "select_portal",
                "select_typed",
                &[1],
                &[Some(b"tenant-b")],
                &[1],
            ),
            describe_packet(TARGET_TYPE_BYTE_PORTAL, "select_portal"),
            execute_packet("select_portal", 0),
            typed_packet(b'S', &[]),
        ]
        .concat();
        client.write_all(&read).await.unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'2', b'T', b'D', b'C', b'Z']
        );
        assert_eq!(
            row_description_fields(&frames[1].1),
            [
                ("tenant_id".to_owned(), Type::TEXT.oid(), 1),
                ("enabled".to_owned(), Type::BOOL.oid(), 1),
                ("count_value".to_owned(), Type::INT8.oid(), 1),
                ("ratio".to_owned(), Type::FLOAT8.oid(), 1),
                ("payload".to_owned(), Type::BYTEA.oid(), 1),
            ]
        );
        assert_eq!(
            data_row(&frames[2].1),
            [
                Some(b"tenant-b".to_vec()),
                Some(vec![1]),
                Some(42_i64.to_be_bytes().to_vec()),
                Some(1.5_f64.to_be_bytes().to_vec()),
                Some(vec![0, 255]),
            ]
        );
        assert_eq!(command_tag(&frames[3].1), "SELECT 1");

        let core = Arc::clone(wire.connection().unwrap());
        finish_wire_server(&mut client, server).await;
        assert_eq!(core.state().await, SessionState::Closed);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn extended_query_errors_resync_and_unnamed_replacement_cleans_up_portals() {
        let (_temp, engine) = engine(2).await;
        let adapter = Adapter::new(engine.clone());
        let (address, wire, server) = spawn_wire_server(&adapter).await;
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&startup_packet()).await.unwrap();
        read_until_ready(&mut client).await;
        let core = Arc::clone(wire.connection().unwrap());

        client
            .write_all(&parse_packet_with_oids(
                "private_statement",
                "SELECT $1 || 'private extended text'",
                &[Type::DATE.oid()],
            ))
            .await
            .unwrap();
        let error = read_frame(&mut client).await;
        assert_eq!(error.0, b'E');
        let fields = message_fields(&error.1);
        assert_eq!(fields.get(&b'C').map(String::as_str), Some("0A000"));
        assert_eq!(
            fields.get(&b'M').map(String::as_str),
            Some("the PostgreSQL parameter type is unsupported")
        );
        assert!(!fields.get(&b'M').unwrap().contains("private extended text"));

        client.write_all(&typed_packet(b'S', &[])).await.unwrap();
        assert_eq!(read_frame(&mut client).await, (b'Z', vec![b'I']));

        client
            .write_all(&parse_packet_with_oids(
                "too_many_types",
                "SELECT $1",
                &[Type::INT8.oid(), Type::BOOL.oid()],
            ))
            .await
            .unwrap();
        let error = read_frame(&mut client).await;
        assert_eq!(error.0, b'E');
        assert_eq!(
            message_fields(&error.1).get(&b'C').map(String::as_str),
            Some("08P01")
        );
        client.write_all(&typed_packet(b'S', &[])).await.unwrap();
        assert_eq!(read_frame(&mut client).await, (b'Z', vec![b'I']));

        client
            .write_all(&parse_packet_with_oids(
                "parameterized",
                "SELECT $1",
                &[Type::INT8.oid()],
            ))
            .await
            .unwrap();
        assert_eq!(read_frame(&mut client).await, (b'1', vec![]));
        client
            .write_all(&bind_packet_with(
                "invalid_parameter_portal",
                "parameterized",
                &[1],
                &[Some(&1_i32.to_be_bytes())],
                &[],
            ))
            .await
            .unwrap();
        let error = read_frame(&mut client).await;
        assert_eq!(error.0, b'E');
        let fields = message_fields(&error.1);
        assert_eq!(fields.get(&b'C').map(String::as_str), Some("22P02"));
        client.write_all(&typed_packet(b'S', &[])).await.unwrap();
        assert_eq!(read_frame(&mut client).await, (b'Z', vec![b'I']));
        assert!(
            bound_portal(&core.state, "invalid_parameter_portal")
                .unwrap()
                .is_none()
        );

        client
            .write_all(&parse_packet(
                &"n".repeat(MAX_EXTENDED_NAME_BYTES + 1),
                "SELECT 1",
            ))
            .await
            .unwrap();
        let error = read_frame(&mut client).await;
        assert_eq!(
            message_fields(&error.1).get(&b'C').map(String::as_str),
            Some("42622")
        );
        client.write_all(&typed_packet(b'S', &[])).await.unwrap();
        assert_eq!(read_frame(&mut client).await, (b'Z', vec![b'I']));

        client
            .write_all(&parse_packet("duplicate_statement", "SELECT 1"))
            .await
            .unwrap();
        assert_eq!(read_frame(&mut client).await, (b'1', vec![]));
        client
            .write_all(&parse_packet("duplicate_statement", "SELECT 2"))
            .await
            .unwrap();
        let error = read_frame(&mut client).await;
        assert_eq!(
            message_fields(&error.1).get(&b'C').map(String::as_str),
            Some("42P05")
        );
        client.write_all(&typed_packet(b'S', &[])).await.unwrap();
        assert_eq!(read_frame(&mut client).await, (b'Z', vec![b'I']));

        client
            .write_all(&bind_packet("duplicate_portal", "duplicate_statement"))
            .await
            .unwrap();
        assert_eq!(read_frame(&mut client).await, (b'2', vec![]));
        client
            .write_all(&bind_packet("duplicate_portal", "duplicate_statement"))
            .await
            .unwrap();
        let error = read_frame(&mut client).await;
        assert_eq!(
            message_fields(&error.1).get(&b'C').map(String::as_str),
            Some("42P03")
        );
        client.write_all(&typed_packet(b'S', &[])).await.unwrap();
        assert_eq!(read_frame(&mut client).await, (b'Z', vec![b'I']));
        client
            .write_all(
                &[
                    close_packet(TARGET_TYPE_BYTE_STATEMENT, "duplicate_statement"),
                    typed_packet(b'S', &[]),
                ]
                .concat(),
            )
            .await
            .unwrap();
        assert_eq!(read_until_ready(&mut client).await[0].0, b'3');

        let replacement_flow = [
            parse_packet("", "SELECT 1 AS first_value"),
            bind_packet("", ""),
            parse_packet("", "SELECT 2 AS replacement_value"),
            execute_packet("", 0),
            typed_packet(b'S', &[]),
        ]
        .concat();
        client.write_all(&replacement_flow).await.unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            [b'1', b'2', b'1', b'E', b'Z']
        );
        let fields = message_fields(&frames[3].1);
        assert_eq!(fields.get(&b'C').map(String::as_str), Some("34000"));

        let replacement_execute = [
            bind_packet("", ""),
            execute_packet("", 0),
            close_packet(TARGET_TYPE_BYTE_PORTAL, ""),
            close_packet(TARGET_TYPE_BYTE_STATEMENT, ""),
            typed_packet(b'S', &[]),
        ]
        .concat();
        client.write_all(&replacement_execute).await.unwrap();
        let frames = read_until_ready(&mut client).await;
        assert_eq!(frames.first().unwrap().0, b'2');
        assert!(frames.iter().any(|frame| frame.0 == b'D'));
        assert_eq!(
            frames
                .iter()
                .rev()
                .take(3)
                .map(|frame| frame.0)
                .collect::<Vec<_>>(),
            [b'Z', b'3', b'3']
        );
        assert!(core.state.extended.lock().unwrap().portals.is_empty());
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
        assert_eq!(adapter.cancellations.len(), 0);

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
        assert_eq!(adapter.cancellations.len(), 0);

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
        let cancel = cancel_packet(1, 2);
        assert_eq!(
            validate_startup_frame(&cancel).unwrap(),
            StartupFrameKind::Cancellation
        );
        let mut short_cancel = cancel.clone();
        short_cancel.pop();
        short_cancel[..4].copy_from_slice(&15_u32.to_be_bytes());
        let mut wrong_cancel_length = cancel.clone();
        wrong_cancel_length[..4].copy_from_slice(&8_u32.to_be_bytes());

        for rejected in [
            missing_terminal,
            trailing_after_terminal,
            duplicate,
            missing_value,
            invalid_utf8,
            wrong_declared_length,
            short_cancel,
            wrong_cancel_length,
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
        validate_typed_frame(&bind_packet("portal", "statement")).unwrap();
        validate_typed_frame(&describe_packet(TARGET_TYPE_BYTE_STATEMENT, "statement")).unwrap();
        validate_typed_frame(&describe_packet(TARGET_TYPE_BYTE_PORTAL, "portal")).unwrap();
        validate_typed_frame(&execute_packet("portal", 1)).unwrap();
        validate_typed_frame(&close_packet(TARGET_TYPE_BYTE_STATEMENT, "statement")).unwrap();
        validate_typed_frame(&close_packet(TARGET_TYPE_BYTE_PORTAL, "portal")).unwrap();
        validate_typed_frame(&typed_packet(b'H', &[])).unwrap();
        validate_typed_frame(&typed_packet(b'S', &[])).unwrap();
        validate_typed_frame(&typed_packet(b'X', &[])).unwrap();

        let mut negative_parameter_length = Vec::new();
        push_cstring(&mut negative_parameter_length, "portal");
        push_cstring(&mut negative_parameter_length, "statement");
        negative_parameter_length.extend_from_slice(&0_u16.to_be_bytes());
        negative_parameter_length.extend_from_slice(&1_u16.to_be_bytes());
        negative_parameter_length.extend_from_slice(&(-2_i32).to_be_bytes());
        negative_parameter_length.extend_from_slice(&0_i16.to_be_bytes());
        let mut negative_result_count = bind_packet("portal", "statement");
        let end = negative_result_count.len();
        negative_result_count[end - 2..].copy_from_slice(&(-1_i16).to_be_bytes());

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
            typed_packet(b'H', &[0]),
            typed_packet(b'X', &[0]),
            typed_packet(b'B', &[]),
            typed_packet(b'B', &negative_parameter_length),
            negative_result_count,
            describe_packet(b'X', "statement"),
            execute_packet("portal", -1),
            close_packet(b'X', "statement"),
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
    async fn selected_query_parser_prepares_through_core_and_owns_parameter_types() {
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
            .unwrap();
        assert_eq!(inferred_or_unknown.parameter_types.as_ref(), &[Type::TEXT]);
        assert!(
            connection
                .state
                .engine
                .close_prepared_statement(&connection.state.session, inferred_or_unknown.id)
                .await
                .unwrap()
        );

        let recognized = connection
            .wire_parser
            .parse_sql(&client, "SELECT $1", &[Some(Type::INT8)])
            .await
            .unwrap();
        assert_eq!(recognized.parameter_types.as_ref(), &[Type::INT8]);
        assert!(
            connection
                .state
                .engine
                .close_prepared_statement(&connection.state.session, recognized.id)
                .await
                .unwrap()
        );

        let unsupported = connection
            .wire_parser
            .parse_sql(&client, "SELECT $1", &[Some(Type::DATE)])
            .await
            .unwrap_err();
        let PgWireError::UserError(info) = unsupported else {
            panic!("expected a BriskDB-owned PostgreSQL error")
        };
        assert_eq!(info.code, "0A000");
        assert_eq!(info.message, "the PostgreSQL parameter type is unsupported");

        let too_many = connection
            .wire_parser
            .parse_sql(&client, "SELECT $1", &[Some(Type::INT8), Some(Type::BOOL)])
            .await
            .unwrap_err();
        let PgWireError::UserError(info) = too_many else {
            panic!("expected a BriskDB-owned PostgreSQL error")
        };
        assert_eq!(info.code, "08P01");

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
