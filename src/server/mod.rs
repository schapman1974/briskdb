//! Server process assembly and listener lifecycle.

use std::{
    collections::HashMap, future::Future, net::SocketAddr, path::PathBuf, pin::Pin, sync::Arc,
    time::Duration,
};

use anyhow::Context;
use axum::body::Body;
use hyper::{Request, body::Incoming, server::conn::http1};
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use tokio::{
    sync::{Notify, watch},
    task::{Id, JoinSet},
};
use tower::ServiceExt;
use tracing::{debug, info, warn};

use crate::{
    core::{Engine, EngineOptions},
    protocol::{http, postgres},
};

const MAX_POSTGRES_CONNECTIONS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub listen: SocketAddr,
    pub postgres_listen: Option<SocketAddr>,
    pub data_dir: PathBuf,
    pub shards: u16,
}

#[derive(Debug)]
struct BoundListeners {
    http: tokio::net::TcpListener,
    postgres: Option<tokio::net::TcpListener>,
}

impl BoundListeners {
    async fn bind(config: &Config) -> anyhow::Result<Self> {
        let http = tokio::net::TcpListener::bind(config.listen)
            .await
            .with_context(|| format!("failed to bind {}", config.listen))?;
        let postgres = match config.postgres_listen {
            Some(address) => Some(
                tokio::net::TcpListener::bind(address)
                    .await
                    .with_context(|| format!("failed to bind PostgreSQL listener {address}"))?,
            ),
            None => None,
        };
        Ok(Self { http, postgres })
    }

    #[cfg(test)]
    fn http_only(http: tokio::net::TcpListener) -> Self {
        Self {
            http,
            postgres: None,
        }
    }
}

pub async fn run(config: Config) -> anyhow::Result<()> {
    run_with_engine_options(config, EngineOptions::default()).await
}

/// Start the server with explicit bounded-worker and per-shard pool limits.
///
/// `options` is validated before any database files or listener are created.
/// [`run`] remains the compatibility entry point and delegates here with
/// [`EngineOptions::default`].
pub async fn run_with_engine_options(config: Config, options: EngineOptions) -> anyhow::Result<()> {
    validate_postgres_listener(&config)?;
    let engine = Engine::open_with_options(&config.data_dir, config.shards, options).await?;
    let listeners = match BoundListeners::bind(&config).await {
        Ok(listeners) => listeners,
        Err(error) => {
            engine.begin_shutdown();
            if let Err(shutdown_error) = engine.shutdown().await {
                warn!(error = %shutdown_error, "failed to clean up after listener startup error");
            }
            return Err(error);
        }
    };
    // Tokio's portable `ctrl_c()` future installs its handler only when first
    // polled. Construct platform signal streams synchronously here so a signal
    // cannot land between the readiness log and handler installation.
    let signal = match shutdown_signal() {
        Ok(signal) => signal,
        Err(error) => {
            engine.begin_shutdown();
            if let Err(shutdown_error) = engine.shutdown().await {
                warn!(error = %shutdown_error, "failed to clean up after signal startup error");
            }
            return Err(error);
        }
    };

    info!(
        listen = %config.listen,
        postgres_listen = ?config.postgres_listen,
        data_dir = %config.data_dir.display(),
        shards = engine.shard_count(),
        max_blocking_workers = engine.options().connections_per_shard()
            * usize::from(engine.shard_count()),
        connections_per_shard = engine.options().connections_per_shard(),
        queue_capacity_per_shard = engine.options().queue_capacity_per_shard(),
        max_result_rows = engine.options().result_limits().max_rows(),
        max_result_bytes = engine.options().result_limits().max_bytes(),
        max_prepared_statements_per_session = engine
            .options()
            .prepared_statement_limits()
            .max_statements_per_session(),
        max_portals_per_session = engine
            .options()
            .prepared_statement_limits()
            .max_portals_per_session(),
        max_retained_bound_value_bytes = engine
            .options()
            .prepared_statement_limits()
            .max_retained_bound_value_bytes(),
        request_timeout_ms = engine
            .options()
            .request_timeout()
            .map(|timeout| timeout.as_millis()),
        shutdown_grace_ms = engine.options().shutdown_grace().as_millis(),
        "BriskDB is ready"
    );

    serve_listeners_with_shutdown(listeners, engine, signal).await
}

fn validate_postgres_listener(config: &Config) -> anyhow::Result<()> {
    if let Some(address) = config
        .postgres_listen
        .filter(|address| !address.ip().is_loopback())
    {
        anyhow::bail!(
            "PostgreSQL wire startup currently requires a loopback listen address; received {address}"
        );
    }
    Ok(())
}

async fn serve_listeners_with_shutdown<F>(
    listeners: BoundListeners,
    engine: Engine,
    signal: F,
) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send,
{
    serve_listeners_with_shutdown_observed(listeners, engine, signal, None).await
}

#[cfg(test)]
async fn serve_with_shutdown_observed<F>(
    listener: tokio::net::TcpListener,
    engine: Engine,
    signal: F,
    accepted: Option<Arc<Notify>>,
) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send,
{
    serve_listeners_with_shutdown_observed(
        BoundListeners::http_only(listener),
        engine,
        signal,
        accepted,
    )
    .await
}

async fn serve_listeners_with_shutdown_observed<F>(
    listeners: BoundListeners,
    engine: Engine,
    signal: F,
    accepted: Option<Arc<Notify>>,
) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send,
{
    let mut shutdown_guard = ShutdownOnDrop::new(engine.clone());
    let router = http::router_with_engine(engine.clone());
    let postgres_adapter = listeners
        .postgres
        .as_ref()
        .map(|_| postgres::Adapter::new(engine.clone()));
    let (graceful_tx, _graceful_rx) = watch::channel(false);
    let mut http_connections = JoinSet::new();
    let mut postgres_connections = PostgresConnections::default();
    let mut server_error = None;
    tokio::pin!(signal);

    loop {
        tokio::select! {
            biased;
            _ = &mut signal => break,
            Some(result) = http_connections.join_next(), if !http_connections.is_empty() => {
                log_http_connection_join(result, false);
            }
            result = postgres_connections.join_next_result(), if !postgres_connections.is_empty() => {
                if let Some(result) = result {
                    postgres_connections.finish_join(result, false).await;
                }
            }
            accepted_connection = accept_next_connection(&listeners) => {
                match accepted_connection {
                    ListenerAccept::Http(Ok((stream, peer))) => {
                        let accepted = accepted.clone();
                        let service = TowerToHyperService::new(router.clone().map_request(
                            move |request: Request<Incoming>| {
                                if let Some(accepted) = &accepted {
                                    accepted.notify_one();
                                }
                                request.map(Body::new)
                            },
                        ));
                        let graceful_rx = graceful_tx.subscribe();
                        http_connections.spawn(async move {
                            serve_http_connection(stream, peer, service, graceful_rx).await;
                        });
                    }
                    ListenerAccept::Http(Err(error)) => {
                        server_error = Some(
                            anyhow::Error::from(error).context("HTTP listener accept failed")
                        );
                        break;
                    }
                    ListenerAccept::Postgres(Ok((stream, peer))) => {
                        let adapter = postgres_adapter
                            .as_ref()
                            .expect("an accepted PostgreSQL socket has an adapter");
                        if !postgres_connections.spawn(
                            stream,
                            peer,
                            adapter,
                            graceful_tx.subscribe(),
                        ) {
                            debug!(
                                %peer,
                                max_connections = postgres_connections.limit(),
                                "closing PostgreSQL connection because the finite task limit is full"
                            );
                        }
                    }
                    ListenerAccept::Postgres(Err(error)) => {
                        server_error = Some(
                            anyhow::Error::from(error)
                                .context("PostgreSQL listener accept failed")
                        );
                        break;
                    }
                }
            }
        }
    }

    // Admission must close before the listeners and connection signals so an
    // already-accepted request cannot enter the core after draining starts.
    engine.begin_shutdown();
    drop(listeners);
    let http_grace = engine.options().shutdown_grace();
    let core_shutdown = engine.shutdown();
    let connection_shutdown = drain_connections(
        graceful_tx,
        &mut http_connections,
        &mut postgres_connections,
        http_grace,
    );
    let (core_result, _) = tokio::join!(core_shutdown, connection_shutdown);
    let report = core_result?;
    info!(forced = report.forced(), "BriskDB core shutdown completed");

    shutdown_guard.disarm();
    if let Some(server_error) = server_error {
        return Err(server_error);
    }
    Ok(())
}

enum ListenerAccept {
    Http(std::io::Result<(tokio::net::TcpStream, SocketAddr)>),
    Postgres(std::io::Result<(tokio::net::TcpStream, SocketAddr)>),
}

async fn accept_next_connection(listeners: &BoundListeners) -> ListenerAccept {
    tokio::select! {
        accepted = listeners.http.accept() => ListenerAccept::Http(accepted),
        accepted = accept_optional(&listeners.postgres) => ListenerAccept::Postgres(accepted),
    }
}

async fn accept_optional(
    listener: &Option<tokio::net::TcpListener>,
) -> std::io::Result<(tokio::net::TcpStream, SocketAddr)> {
    match listener {
        Some(listener) => listener.accept().await,
        None => std::future::pending().await,
    }
}

struct TrackedPostgresConnection {
    connection: postgres::WireConnection,
    peer: SocketAddr,
}

struct PostgresConnections {
    tasks: JoinSet<std::io::Result<()>>,
    connections: HashMap<Id, TrackedPostgresConnection>,
    limit: usize,
}

impl Default for PostgresConnections {
    fn default() -> Self {
        Self {
            tasks: JoinSet::new(),
            connections: HashMap::new(),
            limit: MAX_POSTGRES_CONNECTIONS,
        }
    }
}

impl PostgresConnections {
    #[cfg(test)]
    fn with_limit(limit: usize) -> Self {
        assert!(limit > 0);
        Self {
            tasks: JoinSet::new(),
            connections: HashMap::new(),
            limit,
        }
    }

    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    fn len(&self) -> usize {
        self.tasks.len()
    }

    const fn limit(&self) -> usize {
        self.limit
    }

    fn spawn(
        &mut self,
        stream: tokio::net::TcpStream,
        peer: SocketAddr,
        adapter: &postgres::Adapter,
        mut shutdown: watch::Receiver<bool>,
    ) -> bool {
        if self.len() >= self.limit {
            return false;
        }

        let connection = adapter.wire_connection();
        let task_connection = connection.clone();
        let handle = self.tasks.spawn(async move {
            task_connection
                .serve(stream, wait_for_connection_shutdown(&mut shutdown))
                .await
        });
        self.connections
            .insert(handle.id(), TrackedPostgresConnection { connection, peer });
        true
    }

    fn abort_all(&mut self) {
        self.tasks.abort_all();
    }

    async fn join_next_result(
        &mut self,
    ) -> Option<Result<(Id, std::io::Result<()>), tokio::task::JoinError>> {
        self.tasks.join_next_with_id().await
    }

    async fn finish_join(
        &mut self,
        result: Result<(Id, std::io::Result<()>), tokio::task::JoinError>,
        forced: bool,
    ) {
        let id = match &result {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        let tracked = self
            .connections
            .get(&id)
            .map(|tracked| (tracked.connection.clone(), tracked.peer));
        if let Some((connection, peer)) = &tracked {
            if let Err(error) = connection.close().await {
                warn!(%peer, kind = ?error.kind(), "failed to close PostgreSQL core session");
            }
        }
        self.connections.remove(&id);

        match result {
            Ok((_, Ok(()))) => {}
            Ok((_, Err(error))) => {
                if let Some((_, peer)) = tracked {
                    debug!(%peer, %error, "PostgreSQL connection ended with a protocol error");
                } else {
                    debug!(%error, "PostgreSQL connection ended with a protocol error");
                }
            }
            Err(error) if forced && error.is_cancelled() => {}
            Err(error) => {
                if let Some((_, peer)) = tracked {
                    warn!(%peer, %error, "PostgreSQL connection task failed");
                } else {
                    warn!(%error, "PostgreSQL connection task failed");
                }
            }
        }
    }
}

impl Drop for PostgresConnections {
    fn drop(&mut self) {
        self.tasks.abort_all();
        let connections = std::mem::take(&mut self.connections)
            .into_values()
            .map(|tracked| tracked.connection)
            .collect::<Vec<_>>();
        if connections.is_empty() {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let _cleanup = runtime.spawn(async move {
                for connection in connections {
                    let _ = connection.close().await;
                }
            });
        }
    }
}

async fn serve_http_connection<S>(
    stream: tokio::net::TcpStream,
    peer: SocketAddr,
    service: S,
    mut graceful_rx: watch::Receiver<bool>,
) where
    S: hyper::service::Service<
            Request<Incoming>,
            Response = axum::response::Response,
            Error = std::convert::Infallible,
        > + Send
        + 'static,
    S::Future: Send + 'static,
{
    let builder = http1::Builder::new();
    let connection = builder
        .serve_connection(TokioIo::new(stream), service)
        .with_upgrades();
    tokio::pin!(connection);

    let result = if *graceful_rx.borrow() {
        connection.as_mut().graceful_shutdown();
        connection.await
    } else {
        tokio::select! {
            result = &mut connection => result,
            _ = wait_for_connection_shutdown(&mut graceful_rx) => {
                connection.as_mut().graceful_shutdown();
                connection.await
            }
        }
    };
    if let Err(error) = result {
        debug!(%peer, %error, "HTTP connection ended with a protocol error");
    }
}

async fn wait_for_connection_shutdown(graceful_rx: &mut watch::Receiver<bool>) {
    loop {
        if *graceful_rx.borrow_and_update() {
            return;
        }
        if graceful_rx.changed().await.is_err() {
            return;
        }
    }
}

async fn drain_connections(
    graceful_tx: watch::Sender<bool>,
    http_connections: &mut JoinSet<()>,
    postgres_connections: &mut PostgresConnections,
    grace: Duration,
) -> bool {
    let watched_connections = graceful_tx.receiver_count().saturating_sub(1);
    graceful_tx.send_replace(true);
    let graceful_drain = async {
        while !http_connections.is_empty() || !postgres_connections.is_empty() {
            tokio::select! {
                Some(result) = http_connections.join_next(), if !http_connections.is_empty() => {
                    log_http_connection_join(result, false);
                }
                result = postgres_connections.join_next_result(), if !postgres_connections.is_empty() => {
                    if let Some(result) = result {
                        postgres_connections.finish_join(result, false).await;
                    }
                }
            }
        }
    };

    if tokio::time::timeout(grace, graceful_drain).await.is_ok() {
        false
    } else {
        let remaining_http = http_connections.len();
        let remaining_postgres = postgres_connections.len();
        let remaining = remaining_http.saturating_add(remaining_postgres);
        if remaining > 0 {
            warn!(
                grace_ms = grace.as_millis(),
                connections = remaining,
                http_connections = remaining_http,
                postgres_connections = remaining_postgres,
                watched_connections,
                "connections exceeded the shutdown grace period; force-closing them"
            );
            http_connections.abort_all();
            postgres_connections.abort_all();
            while let Some(result) = http_connections.join_next().await {
                log_http_connection_join(result, true);
            }
            let postgres_cleanup = async {
                while !postgres_connections.is_empty() {
                    let Some(result) = postgres_connections.join_next_result().await else {
                        break;
                    };
                    postgres_connections.finish_join(result, true).await;
                }
            };
            if tokio::time::timeout(grace, postgres_cleanup).await.is_err() {
                warn!(
                    grace_ms = grace.as_millis(),
                    connections = postgres_connections.len(),
                    "PostgreSQL session cleanup exceeded the forced-close interval"
                );
            }
        }
        drop(graceful_tx);
        remaining > 0
    }
}

fn log_http_connection_join(result: Result<(), tokio::task::JoinError>, forced: bool) {
    if let Err(error) = result {
        if !(forced && error.is_cancelled()) {
            warn!(%error, "HTTP connection task failed");
        }
    }
}

struct ShutdownOnDrop {
    engine: Option<Engine>,
}

impl ShutdownOnDrop {
    fn new(engine: Engine) -> Self {
        Self {
            engine: Some(engine),
        }
    }

    fn disarm(&mut self) {
        self.engine = None;
    }
}

impl Drop for ShutdownOnDrop {
    fn drop(&mut self) {
        if let Some(engine) = self.engine.take() {
            // Drop cannot await cleanup, but it can atomically reject new work.
            // The lifecycle remains resumable by any surviving Engine clone.
            engine.begin_shutdown();
        }
    }
}

type PreparedShutdownSignal = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

#[cfg(unix)]
fn shutdown_signal() -> anyhow::Result<PreparedShutdownSignal> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut interrupt =
        signal(SignalKind::interrupt()).context("failed to install SIGINT shutdown handler")?;
    let mut terminate =
        signal(SignalKind::terminate()).context("failed to install SIGTERM shutdown handler")?;
    Ok(Box::pin(async move {
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
    }))
}

#[cfg(windows)]
fn shutdown_signal() -> anyhow::Result<PreparedShutdownSignal> {
    let mut ctrl_c =
        tokio::signal::windows::ctrl_c().context("failed to install Ctrl-C shutdown handler")?;
    Ok(Box::pin(async move {
        ctrl_c.recv().await;
    }))
}

#[cfg(not(any(unix, windows)))]
fn shutdown_signal() -> anyhow::Result<PreparedShutdownSignal> {
    anyhow::bail!("process shutdown signals are unsupported on this target")
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use axum::body::to_bytes;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        time::timeout,
    };

    use super::*;

    fn unavailable_address() -> (std::net::TcpListener, SocketAddr) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        (listener, address)
    }

    async fn assert_peer_closes(stream: &mut tokio::net::TcpStream) {
        timeout(Duration::from_secs(1), async {
            let mut buffer = [0_u8; 1_024];
            loop {
                match stream.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
            }
        })
        .await
        .expect("the peer should be closed");
    }

    fn postgres_startup_packet(user: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&196_608_u32.to_be_bytes());
        body.extend_from_slice(b"user\0");
        body.extend_from_slice(user.as_bytes());
        body.extend_from_slice(b"\0database\0default\0\0");
        let mut packet = Vec::new();
        packet.extend_from_slice(&u32::try_from(body.len() + 4).unwrap().to_be_bytes());
        packet.extend_from_slice(&body);
        packet
    }

    fn postgres_typed_packet(kind: u8, body: &[u8]) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.push(kind);
        packet.extend_from_slice(&u32::try_from(body.len() + 4).unwrap().to_be_bytes());
        packet.extend_from_slice(body);
        packet
    }

    async fn read_postgres_frame(stream: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
        timeout(Duration::from_secs(1), async {
            let mut header = [0_u8; 5];
            stream.read_exact(&mut header).await.unwrap();
            let length = u32::from_be_bytes(header[1..].try_into().unwrap());
            assert!((4..=1_048_576).contains(&length));
            let mut body = vec![0_u8; usize::try_from(length - 4).unwrap()];
            stream.read_exact(&mut body).await.unwrap();
            (header[0], body)
        })
        .await
        .expect("the PostgreSQL server returned a frame")
    }

    async fn start_postgres_session(address: SocketAddr, user: &str) -> tokio::net::TcpStream {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(&postgres_startup_packet(user))
            .await
            .unwrap();
        let mut kinds = Vec::new();
        loop {
            let frame = read_postgres_frame(&mut stream).await;
            kinds.push(frame.0);
            if frame.0 == b'Z' {
                assert_eq!(frame.1, vec![b'I']);
                break;
            }
            assert!(kinds.len() < 16);
        }
        assert_eq!(kinds.first(), Some(&b'R'));
        assert_eq!(kinds.iter().filter(|kind| **kind == b'S').count(), 5);
        assert!(!kinds.contains(&b'K'));
        stream
    }

    async fn terminate_postgres_session(stream: &mut tokio::net::TcpStream) {
        stream
            .write_all(&postgres_typed_packet(b'X', &[]))
            .await
            .unwrap();
        assert_peer_closes(stream).await;
    }

    async fn read_http_health(address: SocketAddr) -> Vec<u8> {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
            .await
            .expect("the HTTP health response should complete")
            .unwrap();
        response
    }

    fn partial_json_request(content_length: usize) -> Vec<u8> {
        format!(
            "POST /v1/query HTTP/1.1\r\nHost: localhost\r\n\
             Content-Type: application/json\r\nContent-Length: {content_length}\r\n\r\n{{"
        )
        .into_bytes()
    }

    #[tokio::test]
    async fn default_entry_point_validates_before_database_or_listener_startup() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("database");
        let (_occupied_listener, listen) = unavailable_address();
        let error = run(Config {
            listen,
            postgres_listen: None,
            data_dir: data_dir.clone(),
            shards: 1,
        })
        .await
        .unwrap_err();

        assert_eq!(error.to_string(), "shard count must be between 2 and 64");
        assert!(!data_dir.exists());
    }

    #[tokio::test]
    async fn non_loopback_postgres_activation_fails_before_database_or_listener_startup() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("database");
        let error = run(Config {
            listen: "127.0.0.1:0".parse().unwrap(),
            postgres_listen: Some("0.0.0.0:0".parse().unwrap()),
            data_dir: data_dir.clone(),
            shards: 2,
        })
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "PostgreSQL wire startup currently requires a loopback listen address; received 0.0.0.0:0"
        );
        assert!(!data_dir.exists());
    }

    #[tokio::test]
    async fn aggregate_pool_limit_fails_before_database_or_listener_startup() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("database");
        let (_occupied_listener, listen) = unavailable_address();
        let options = EngineOptions::new(16, 1).unwrap();
        let error = run_with_engine_options(
            Config {
                listen,
                postgres_listen: None,
                data_dir: data_dir.clone(),
                shards: 64,
            },
            options,
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("must contain between 1 and 512 total active connections")
        );
        assert!(!data_dir.exists());
    }

    #[tokio::test]
    async fn configured_listeners_bind_enabled_and_disabled_postgres_modes() {
        let temp = tempfile::tempdir().unwrap();
        let enabled = BoundListeners::bind(&Config {
            listen: "127.0.0.1:0".parse().unwrap(),
            postgres_listen: Some("127.0.0.1:0".parse().unwrap()),
            data_dir: temp.path().to_path_buf(),
            shards: 2,
        })
        .await
        .unwrap();
        let http_address = enabled.http.local_addr().unwrap();
        let postgres_address = enabled.postgres.as_ref().unwrap().local_addr().unwrap();
        assert_ne!(http_address, postgres_address);
        drop(enabled);

        let disabled = BoundListeners::bind(&Config {
            listen: "127.0.0.1:0".parse().unwrap(),
            postgres_listen: None,
            data_dir: temp.path().to_path_buf(),
            shards: 2,
        })
        .await
        .unwrap();
        assert!(disabled.postgres.is_none());
    }

    #[tokio::test]
    async fn postgres_task_limit_closes_overflow_and_reuses_a_completed_slot() {
        assert_eq!(MAX_POSTGRES_CONNECTIONS, 256);
        assert_eq!(
            PostgresConnections::default().limit(),
            MAX_POSTGRES_CONNECTIONS
        );
        let test_limit = 4;
        let temp = tempfile::tempdir().unwrap();
        let engine = Engine::open(temp.path(), 2).await.unwrap();
        let adapter = postgres::Adapter::new(engine.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, _) = watch::channel(false);
        let mut connections = PostgresConnections::with_limit(test_limit);
        let mut clients = Vec::with_capacity(test_limit);

        for _ in 0..test_limit {
            let (client, accepted) =
                tokio::join!(tokio::net::TcpStream::connect(address), listener.accept());
            let client = client.unwrap();
            let (stream, peer) = accepted.unwrap();
            assert!(connections.spawn(stream, peer, &adapter, shutdown_tx.subscribe()));
            clients.push(client);
        }
        assert_eq!(connections.len(), test_limit);

        let (mut overflow, accepted) =
            tokio::join!(tokio::net::TcpStream::connect(address), listener.accept());
        let (stream, peer) = accepted.unwrap();
        assert!(!connections.spawn(stream, peer, &adapter, shutdown_tx.subscribe()));
        assert_peer_closes(overflow.as_mut().unwrap()).await;

        drop(clients.pop());
        let completed = timeout(Duration::from_secs(2), connections.join_next_result())
            .await
            .expect("the closed client should release its tracked task")
            .expect("the task set should return one completion");
        connections.finish_join(completed, false).await;
        assert_eq!(connections.len(), test_limit - 1);

        let (replacement, accepted) =
            tokio::join!(tokio::net::TcpStream::connect(address), listener.accept());
        let (stream, peer) = accepted.unwrap();
        assert!(connections.spawn(stream, peer, &adapter, shutdown_tx.subscribe()));
        clients.push(replacement.unwrap());
        assert_eq!(connections.len(), test_limit);

        shutdown_tx.send_replace(true);
        timeout(Duration::from_secs(2), async {
            while let Some(result) = connections.join_next_result().await {
                connections.finish_join(result, false).await;
            }
        })
        .await
        .expect("all bounded PostgreSQL tasks should stop cooperatively");
        assert!(connections.is_empty());
        drop(clients);
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn http_bind_failure_precedes_postgres_bind_and_cleans_up_the_engine() {
        let temp = tempfile::tempdir().unwrap();
        let (_http_reservation, http_address) = unavailable_address();
        let (_postgres_reservation, postgres_address) = unavailable_address();

        let error = run(Config {
            listen: http_address,
            postgres_listen: Some(postgres_address),
            data_dir: temp.path().to_path_buf(),
            shards: 2,
        })
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), format!("failed to bind {http_address}"));

        let reopened = Engine::open(temp.path(), 2)
            .await
            .expect("the startup engine should complete cleanup after the HTTP bind failure");
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn postgres_bind_failure_releases_http_listener_and_engine() {
        let temp = tempfile::tempdir().unwrap();
        let (http_reservation, http_address) = unavailable_address();
        drop(http_reservation);
        let (postgres_reservation, postgres_address) = unavailable_address();
        let config = Config {
            listen: http_address,
            postgres_listen: Some(postgres_address),
            data_dir: temp.path().to_path_buf(),
            shards: 2,
        };

        let error = run(config.clone()).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("failed to bind PostgreSQL listener {postgres_address}")
        );

        let rebound = std::net::TcpListener::bind(http_address)
            .expect("the partially started HTTP listener should be released");
        drop(rebound);
        let reopened = Engine::open(temp.path(), 2)
            .await
            .expect("the startup engine should complete cleanup after the bind failure");
        drop(postgres_reservation);
        let rebound = BoundListeners::bind(&config)
            .await
            .expect("both listeners should bind on a clean retry");
        drop(rebound);
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn injected_signal_stops_both_listeners_and_fully_stops_the_engine() {
        let temp = tempfile::tempdir().unwrap();
        let options = EngineOptions::default()
            .with_shutdown_grace(Duration::from_millis(50))
            .unwrap();
        let engine = Engine::open_with_options(temp.path(), 2, options)
            .await
            .unwrap();
        let observer = engine.clone();
        let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_address = http.local_addr().unwrap();
        let postgres = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let postgres_address = postgres.local_addr().unwrap();
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let send_signal = tokio::spawn(async move {
            // Let the serve and signal futures both register before firing the
            // injected process-level event.
            tokio::task::yield_now().await;
            signal_tx.send(()).unwrap();
        });
        timeout(
            Duration::from_secs(2),
            serve_listeners_with_shutdown(
                BoundListeners {
                    http,
                    postgres: Some(postgres),
                },
                engine,
                async move {
                    let _ = signal_rx.await;
                },
            ),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "server should complete its injected graceful shutdown; engine state: {:?}",
                observer.state()
            )
        })
        .unwrap();
        send_signal.await.unwrap();

        assert_eq!(observer.state(), crate::core::EngineState::Stopped);
        assert!(tokio::net::TcpStream::connect(http_address).await.is_err());
        assert!(
            tokio::net::TcpStream::connect(postgres_address)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn postgres_startup_recovers_after_concurrent_connections_and_http_stays_live() {
        let temp = tempfile::tempdir().unwrap();
        let options = EngineOptions::default()
            .with_shutdown_grace(Duration::from_millis(50))
            .unwrap();
        let engine = Engine::open_with_options(temp.path(), 2, options)
            .await
            .unwrap();
        let observer = engine.clone();
        let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_address = http.local_addr().unwrap();
        let postgres = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let postgres_address = postgres.local_addr().unwrap();
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve_listeners_with_shutdown(
            BoundListeners {
                http,
                postgres: Some(postgres),
            },
            engine,
            async move {
                let _ = signal_rx.await;
            },
        ));

        let clients = (0..32)
            .map(|index| {
                tokio::spawn(async move {
                    let mut stream =
                        start_postgres_session(postgres_address, &format!("client_{index}")).await;
                    terminate_postgres_session(&mut stream).await;
                })
            })
            .collect::<Vec<_>>();
        let response = read_http_health(http_address).await;
        assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
        for client in clients {
            client.await.unwrap();
        }

        for index in 0..3 {
            let mut stream =
                start_postgres_session(postgres_address, &format!("recovery_{index}")).await;
            terminate_postgres_session(&mut stream).await;
        }
        assert!(
            read_http_health(http_address)
                .await
                .starts_with(b"HTTP/1.1 200 OK\r\n")
        );

        signal_tx.send(()).unwrap();
        timeout(Duration::from_secs(2), server)
            .await
            .expect("both listeners should finish shutdown")
            .unwrap()
            .unwrap();
        assert_eq!(observer.state(), crate::core::EngineState::Stopped);
    }

    #[tokio::test]
    async fn graceful_shutdown_closes_idle_and_partial_postgres_connections() {
        let temp = tempfile::tempdir().unwrap();
        let options = EngineOptions::default()
            .with_shutdown_grace(Duration::from_millis(100))
            .unwrap();
        let engine = Engine::open_with_options(temp.path(), 2, options)
            .await
            .unwrap();
        let observer = engine.clone();
        let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let postgres = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let postgres_address = postgres.local_addr().unwrap();
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve_listeners_with_shutdown(
            BoundListeners {
                http,
                postgres: Some(postgres),
            },
            engine,
            async move {
                let _ = signal_rx.await;
            },
        ));

        let mut idle = start_postgres_session(postgres_address, "idle_client").await;
        let mut partial = tokio::net::TcpStream::connect(postgres_address)
            .await
            .unwrap();
        partial
            .write_all(&[0, 0, 0, 8, 4, 210, 22, 47])
            .await
            .unwrap();
        let mut ssl_response = [0_u8; 1];
        partial.read_exact(&mut ssl_response).await.unwrap();
        assert_eq!(ssl_response, *b"N");
        partial.write_all(&[0, 0, 0, 32]).await.unwrap();

        signal_tx.send(()).unwrap();
        assert_peer_closes(&mut idle).await;
        assert_peer_closes(&mut partial).await;
        timeout(Duration::from_secs(2), server)
            .await
            .expect("tracked PostgreSQL connections should drain")
            .unwrap()
            .unwrap();
        assert_eq!(observer.state(), crate::core::EngineState::Stopped);
    }

    #[tokio::test]
    async fn already_ready_shutdown_signal_closes_both_listeners_without_lost_wakeup() {
        let temp = tempfile::tempdir().unwrap();
        let options = EngineOptions::default()
            .with_shutdown_grace(Duration::from_millis(50))
            .unwrap();
        let engine = Engine::open_with_options(temp.path(), 2, options)
            .await
            .unwrap();
        let observer = engine.clone();
        let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_address = http.local_addr().unwrap();
        let postgres = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let postgres_address = postgres.local_addr().unwrap();

        timeout(
            Duration::from_secs(2),
            serve_listeners_with_shutdown(
                BoundListeners {
                    http,
                    postgres: Some(postgres),
                },
                engine,
                async {},
            ),
        )
        .await
        .expect("ready signal should shut down immediately")
        .unwrap();
        assert_eq!(observer.state(), crate::core::EngineState::Stopped);
        assert!(tokio::net::TcpStream::connect(http_address).await.is_err());
        assert!(
            tokio::net::TcpStream::connect(postgres_address)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn stuck_request_body_is_force_closed_and_joined_at_http_deadline() {
        let temp = tempfile::tempdir().unwrap();
        let grace = Duration::from_millis(40);
        let options = EngineOptions::default()
            .with_request_timeout(None)
            .unwrap()
            .with_shutdown_grace(grace)
            .unwrap();
        let engine = Engine::open_with_options(temp.path(), 2, options)
            .await
            .unwrap();
        let observer = engine.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted = Arc::new(Notify::new());
        let accepted_for_server = Arc::clone(&accepted);
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve_with_shutdown_observed(
            listener,
            engine,
            async move {
                let _ = signal_rx.await;
            },
            Some(accepted_for_server),
        ));

        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client
            .write_all(&partial_json_request(1_000))
            .await
            .unwrap();
        timeout(Duration::from_secs(1), accepted.notified())
            .await
            .expect("the HTTP connection should be tracked before shutdown");
        let started = Instant::now();
        signal_tx.send(()).unwrap();
        timeout(Duration::from_secs(2), server)
            .await
            .expect("forced HTTP draining should be bounded")
            .unwrap()
            .unwrap();
        assert!(started.elapsed() >= grace);
        assert_eq!(observer.state(), crate::core::EngineState::Stopped);
        assert_peer_closes(&mut client).await;
        assert!(tokio::net::TcpStream::connect(address).await.is_err());
    }

    #[tokio::test]
    async fn aborting_server_future_closes_connections_and_leaves_resumable_drain() {
        let temp = tempfile::tempdir().unwrap();
        let options = EngineOptions::default()
            .with_request_timeout(None)
            .unwrap()
            .with_shutdown_grace(Duration::from_millis(50))
            .unwrap();
        let engine = Engine::open_with_options(temp.path(), 2, options)
            .await
            .unwrap();
        let observer = engine.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let postgres = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let postgres_address = postgres.local_addr().unwrap();
        let accepted = Arc::new(Notify::new());
        let accepted_for_server = Arc::clone(&accepted);
        let server = tokio::spawn(serve_listeners_with_shutdown_observed(
            BoundListeners {
                http: listener,
                postgres: Some(postgres),
            },
            engine,
            std::future::pending(),
            Some(accepted_for_server),
        ));

        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client
            .write_all(&partial_json_request(1_000))
            .await
            .unwrap();
        timeout(Duration::from_secs(1), accepted.notified())
            .await
            .expect("the connection should be owned before aborting the server");
        let mut postgres_client = start_postgres_session(postgres_address, "abort_client").await;
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        assert_eq!(observer.state(), crate::core::EngineState::Draining);
        assert_peer_closes(&mut client).await;
        assert_peer_closes(&mut postgres_client).await;
        assert!(tokio::net::TcpStream::connect(address).await.is_err());
        assert!(
            tokio::net::TcpStream::connect(postgres_address)
                .await
                .is_err()
        );

        timeout(Duration::from_secs(2), observer.shutdown())
            .await
            .expect("a surviving engine clone should resume cleanup")
            .unwrap();
        assert_eq!(observer.state(), crate::core::EngineState::Stopped);
    }

    #[tokio::test]
    async fn active_http_query_drains_through_core_cancellation() {
        let temp = tempfile::tempdir().unwrap();
        // Leave enough forced-cleanup time for heavily parallel CI while still
        // proving that the long-running query crosses the grace boundary.
        let grace = Duration::from_millis(250);
        let options = EngineOptions::new(1, 1)
            .unwrap()
            .with_request_timeout(None)
            .unwrap()
            .with_shutdown_grace(grace)
            .unwrap();
        let engine = Engine::open_with_options(temp.path(), 2, options)
            .await
            .unwrap();
        let observer = engine.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted = Arc::new(Notify::new());
        let accepted_for_server = Arc::clone(&accepted);
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve_with_shutdown_observed(
            listener,
            engine,
            async move {
                let _ = signal_rx.await;
            },
            Some(accepted_for_server),
        ));

        let body = serde_json::json!({
            "shard_key": "active-http",
            "sql": "WITH RECURSIVE numbers(value) AS (VALUES(0) UNION ALL SELECT value + 1 FROM numbers WHERE value < 1000000000) SELECT sum(value) FROM numbers",
            "params": []
        })
        .to_string();
        let request = format!(
            "POST /v1/query HTTP/1.1\r\nHost: localhost\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client.write_all(request.as_bytes()).await.unwrap();
        timeout(Duration::from_secs(1), accepted.notified())
            .await
            .expect("the active HTTP connection should be tracked");
        timeout(Duration::from_secs(1), async {
            while observer.active_operations_for_test() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the HTTP query should enter the shared engine before shutdown");
        signal_tx.send(()).unwrap();
        timeout(Duration::from_secs(2), server)
            .await
            .expect("core cancellation should finish the active HTTP drain")
            .unwrap()
            .unwrap();
        assert_eq!(observer.state(), crate::core::EngineState::Stopped);
        assert!(observer.shutdown().await.unwrap().forced());
        assert_peer_closes(&mut client).await;
    }

    #[tokio::test]
    async fn draining_engine_returns_safe_service_unavailable_problem() {
        let temp = tempfile::tempdir().unwrap();
        let engine = Engine::open(temp.path(), 2).await.unwrap();
        engine.begin_shutdown();
        let response = http::router_with_engine(engine.clone())
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        let body = to_bytes(response.into_body(), 16 * 1_024).await.unwrap();
        let problem: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            problem["type"],
            "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#shutting-down"
        );
        assert_eq!(problem["status"], 503);
        assert!(!String::from_utf8_lossy(&body).contains(temp.path().to_string_lossy().as_ref()));
        engine.shutdown().await.unwrap();
    }
}
