//! BriskDB-owned boundary for the selected PostgreSQL wire library.
//!
//! This module deliberately does not serve the configured PostgreSQL listener
//! yet. It owns the selected library integration and one protocol-neutral core
//! session per future wire connection so `server`, `core`, and public BriskDB
//! contracts do not depend on `pgwire` types.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use pgwire::{
    api::{ClientInfo, Type, stmt::QueryParser},
    error::{ErrorInfo, PgWireError, PgWireResult},
};

use crate::{
    core::{
        DescribeTarget, Engine, EngineError, EngineResult, EngineStatus, LogicalDatabaseId,
        PrepareRequest, PreparedStatementDescription, PreparedStatementId, Session, SessionId,
    },
    protocol::error::postgres_error,
    sql::{SqlDialect, SqlTranslationMode},
};

/// A PostgreSQL protocol adapter backed only by BriskDB's public engine API.
///
/// Constructing an adapter does not bind a socket, accept a connection, create
/// a session, or alter the current listener behavior. A distinct core session
/// is allocated only by [`Adapter::open_connection`].
#[derive(Clone)]
pub struct Adapter {
    engine: Engine,
    default_database: LogicalDatabaseId,
}

impl Adapter {
    /// Construct the BriskDB-owned PostgreSQL adapter boundary.
    pub fn new(engine: Engine) -> Self {
        let default_database = engine.catalog().default_database().id();
        Self {
            engine,
            default_database,
        }
    }

    /// Allocate independent protocol state for one future wire connection.
    ///
    /// The returned value owns exactly one non-cloneable BriskDB [`Session`].
    /// No socket is opened and no wire handshake is performed.
    pub fn open_connection(&self) -> Connection {
        let state = Arc::new(ConnectionState {
            engine: self.engine.clone(),
            session: self.engine.session(),
            database: self.default_database,
        });
        let wire_parser = Arc::new(PgWireQueryParser {
            state: Arc::clone(&state),
        });
        Connection { state, wire_parser }
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
    database: LogicalDatabaseId,
}

/// Protocol-owned state for one future PostgreSQL wire connection.
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

    /// Read engine status through this connection's protocol-neutral session.
    ///
    /// This small operation is the issue-29 proof that the selected wire
    /// adapter composes with the controlled async engine boundary. Query and
    /// prepared execution remain later roadmap work.
    pub async fn status(&self) -> EngineResult<EngineStatus> {
        self.state.engine.status(&self.state.session).await
    }

    /// Close this connection's core session.
    ///
    /// Closing is terminal and idempotent. Future production socket wrappers
    /// must call this on every return path from the wire library.
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
            .field("wire_parser", &self.wire_parser)
            .finish_non_exhaustive()
    }
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
    let mapping = postgres_error(error.kind());
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
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

    fn startup_packet() -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&196_608_u32.to_be_bytes());
        body.extend_from_slice(b"user\0briskdb\0database\0default\0\0");
        let length = u32::try_from(body.len() + 4).unwrap();
        let mut packet = Vec::with_capacity(body.len() + 4);
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(&body);
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
    async fn selected_pgwire_entrypoint_can_reach_core_without_enabling_the_listener() {
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
