//! Server process assembly and listener lifecycle.

use std::{future::Future, net::SocketAddr, path::PathBuf, pin::Pin, sync::Arc, time::Duration};

use anyhow::Context;
use axum::body::Body;
use hyper::{Request, body::Incoming, server::conn::http1};
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use tokio::{
    sync::{Notify, watch},
    task::JoinSet,
};
use tower::ServiceExt;
use tracing::{debug, info, warn};

use crate::{
    core::{Engine, EngineOptions},
    protocol::http,
};

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
    let (graceful_tx, _graceful_rx) = watch::channel(false);
    let mut connections = JoinSet::new();
    let mut server_error = None;
    tokio::pin!(signal);

    loop {
        tokio::select! {
            biased;
            _ = &mut signal => break,
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                log_connection_join(result, false);
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
                        connections.spawn(async move {
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
                        debug!(%peer, "closing connection before PostgreSQL wire support is enabled");
                        drop(stream);
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
    let http_shutdown = drain_http_connections(graceful_tx, &mut connections, http_grace);
    let (core_result, _) = tokio::join!(core_shutdown, http_shutdown);
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
            _ = wait_for_http_shutdown(&mut graceful_rx) => {
                connection.as_mut().graceful_shutdown();
                connection.await
            }
        }
    };
    if let Err(error) = result {
        debug!(%peer, %error, "HTTP connection ended with a protocol error");
    }
}

async fn wait_for_http_shutdown(graceful_rx: &mut watch::Receiver<bool>) {
    loop {
        if *graceful_rx.borrow_and_update() {
            return;
        }
        if graceful_rx.changed().await.is_err() {
            return;
        }
    }
}

async fn drain_http_connections(
    graceful_tx: watch::Sender<bool>,
    connections: &mut JoinSet<()>,
    grace: Duration,
) -> bool {
    let watched_connections = graceful_tx.receiver_count().saturating_sub(1);
    graceful_tx.send_replace(true);
    let graceful_drain = async {
        while let Some(result) = connections.join_next().await {
            log_connection_join(result, false);
        }
    };

    if tokio::time::timeout(grace, graceful_drain).await.is_ok() {
        false
    } else {
        let remaining = connections.len();
        if remaining > 0 {
            warn!(
                grace_ms = grace.as_millis(),
                connections = remaining,
                watched_connections,
                "HTTP connections exceeded the shutdown grace period; force-closing them"
            );
            connections.abort_all();
            while let Some(result) = connections.join_next().await {
                log_connection_join(result, true);
            }
        }
        drop(graceful_tx);
        remaining > 0
    }
}

fn log_connection_join(result: Result<(), tokio::task::JoinError>, forced: bool) {
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

    async fn assert_peer_closes_without_bytes(stream: &mut tokio::net::TcpStream) {
        let mut buffer = [0_u8; 1_024];
        match timeout(Duration::from_secs(1), stream.read(&mut buffer)).await {
            Ok(Ok(0) | Err(_)) => {}
            Ok(Ok(bytes)) => panic!("the placeholder wrote {bytes} unexpected bytes"),
            Err(_) => panic!("the placeholder should close without writing bytes"),
        }
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
    async fn postgres_accept_loop_recovers_after_concurrent_connections_and_http_stays_live() {
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
            .map(|_| {
                tokio::spawn(async move {
                    let mut stream = tokio::net::TcpStream::connect(postgres_address)
                        .await
                        .unwrap();
                    let _ = stream.write_all(b"placeholder input").await;
                    assert_peer_closes_without_bytes(&mut stream).await;
                })
            })
            .collect::<Vec<_>>();
        let response = read_http_health(http_address).await;
        assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
        for client in clients {
            client.await.unwrap();
        }

        for _ in 0..3 {
            let mut stream = tokio::net::TcpStream::connect(postgres_address)
                .await
                .unwrap();
            assert_peer_closes_without_bytes(&mut stream).await;
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
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        assert_eq!(observer.state(), crate::core::EngineState::Draining);
        assert_peer_closes(&mut client).await;
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
        let grace = Duration::from_millis(20);
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
