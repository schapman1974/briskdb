use super::*;

#[derive(Clone, Default)]
pub(super) enum Transport {
    #[default]
    Plain,
    #[cfg(feature = "mongo-tls")]
    Tls {
        acceptor: tokio_rustls::TlsAcceptor,
        handshake_timeout: Duration,
    },
}

impl Transport {
    pub(super) const fn security(&self) -> super::super::MongoSecurityMode {
        match self {
            Self::Plain => super::super::MongoSecurityMode::AnonymousLoopback,
            #[cfg(feature = "mongo-tls")]
            Self::Tls { .. } => super::super::MongoSecurityMode::AnonymousTlsLoopback,
        }
    }

    pub(super) async fn serve(
        self,
        stream: TcpStream,
        shutdown: CancellationToken,
        executor: Arc<commands::Executor>,
        metrics: Arc<metrics::Metrics>,
        clients: Arc<client_metadata::Registry>,
        limits: MongoResourceLimits,
    ) -> io::Result<()> {
        match self {
            Self::Plain => connection(stream, shutdown, executor, metrics, clients, limits).await,
            #[cfg(feature = "mongo-tls")]
            Self::Tls {
                acceptor,
                handshake_timeout,
            } => {
                let stream = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return Ok(()),
                    result = tokio::time::timeout(handshake_timeout, acceptor.accept(stream)) =>
                        result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Mongo TLS handshake timeout"))??,
                };
                // Allocate the engine session/client registry only after TLS.
                connection(stream, shutdown, executor, metrics, clients, limits).await
            }
        }
    }
}
