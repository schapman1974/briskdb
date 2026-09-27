use super::*;

#[derive(Clone, Default)]
pub(super) enum Transport {
    #[default]
    Plain,
    #[cfg(feature = "mongo-tls")]
    Tls(super::super::tls::ReloadableTls),
}

impl Transport {
    pub(super) const fn security(&self) -> super::super::MongoSecurityMode {
        match self {
            Self::Plain => super::super::MongoSecurityMode::AnonymousLoopback,
            #[cfg(feature = "mongo-tls")]
            Self::Tls(_) => super::super::MongoSecurityMode::AnonymousTlsLoopback,
        }
    }

    #[cfg(feature = "mongo-tls")]
    pub(super) fn tls(&self) -> Option<super::super::tls::ReloadableTls> {
        match self {
            Self::Plain => None,
            Self::Tls(source) => Some(source.clone()),
        }
    }

    pub(super) fn snapshot(&self) -> ConnectionTransport {
        match self {
            Self::Plain => ConnectionTransport::Plain,
            #[cfg(feature = "mongo-tls")]
            Self::Tls(source) => ConnectionTransport::Tls(source.snapshot()),
        }
    }
}

pub(super) enum ConnectionTransport {
    Plain,
    #[cfg(feature = "mongo-tls")]
    Tls(Arc<super::super::tls::LoadedTls>),
}

impl ConnectionTransport {
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
            Self::Tls(identity) => {
                let stream = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return Ok(()),
                    result = tokio::time::timeout(identity.handshake_timeout, identity.acceptor.accept(stream)) =>
                        result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Mongo TLS handshake timeout"))??,
                };
                // Allocate the engine session/client registry only after TLS.
                connection(stream, shutdown, executor, metrics, clients, limits).await
            }
        }
    }
}
