//! Composable attached-listener configuration; existing constructors remain valid.

use super::*;

/// Optional connector configuration for [`AttachedServer::start_with_options`].
/// The default selects ordinary SQL HTTP and any addresses in `ListenerConfig`.
/// Credentials apply only to their own connector; Mongo always remains loopback.
#[derive(Default)]
pub struct AttachedServerOptions {
    pub(super) postgres_security: Option<postgres::SecurityConfig>,
    pub(super) sqlite_remote: Option<crate::protocol::sqlite_remote::Config>,
    pub(super) mongo_address: Option<SocketAddr>,
    #[cfg(feature = "mongo-tls")]
    pub(super) mongo_tls: Option<crate::protocol::mongo::MongoTlsConfig>,
}

impl AttachedServerOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable PostgreSQL TLS/SCRAM; the PostgreSQL address must also be enabled.
    #[must_use]
    pub fn with_postgres_security(mut self, security: postgres::SecurityConfig) -> Self {
        self.postgres_security = Some(security);
        self
    }

    /// Replace ordinary SQL HTTP with the dedicated authenticated SQLite-remote
    /// data router. The admin plane is separate; its address stays loopback-only.
    #[must_use]
    pub fn with_sqlite_remote(mut self, config: crate::protocol::sqlite_remote::Config) -> Self {
        self.sqlite_remote = Some(config);
        self
    }

    /// Select the Mongo address. This preserves any TLS identity already selected
    /// through `with_mongo_tls`; it never silently downgrades an encrypted host.
    #[cfg(feature = "mongo")]
    #[must_use]
    pub fn with_mongo(mut self, address: SocketAddr) -> Self {
        self.mongo_address = Some(address);
        self
    }

    /// Select encrypted, still-anonymous Mongo. This requires `mongo-tls` and
    /// never permits non-loopback addresses or authenticates Mongo users.
    #[cfg(feature = "mongo-tls")]
    #[must_use]
    pub fn with_mongo_tls(
        mut self,
        address: SocketAddr,
        config: crate::protocol::mongo::MongoTlsConfig,
    ) -> Self {
        self.mongo_address = Some(address);
        self.mongo_tls = Some(config);
        self
    }
}

impl std::fmt::Debug for AttachedServerOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = formatter.debug_struct("AttachedServerOptions");
        debug
            .field("postgres_security", &self.postgres_security.is_some())
            .field("sqlite_remote", &self.sqlite_remote.is_some())
            .field("mongo_address", &self.mongo_address);
        #[cfg(feature = "mongo-tls")]
        debug.field("mongo_tls", &self.mongo_tls.is_some());
        debug.finish_non_exhaustive()
    }
}

#[derive(Default)]
pub(super) struct PreparedOptions {
    pub postgres: Option<postgres::ReloadableSecurity>,
    pub data_router: Option<axum::Router>,
    pub mongo_address: Option<SocketAddr>,
    #[cfg(feature = "mongo-tls")]
    pub mongo_tls: Option<crate::protocol::mongo::ReloadableTls>,
}

impl AttachedServer {
    /// Compose connector configuration without changing `ListenerConfig` literals
    /// or legacy constructors. Validate addresses/document support first, prepare
    /// all security off-runtime, then bind every socket before serving. Failure
    /// releases listeners and leaves the borrowed database open.
    pub async fn start_with_options(
        database: &BriskDb,
        config: ListenerConfig,
        options: AttachedServerOptions,
    ) -> anyhow::Result<Self> {
        validate_listener_addresses(&config, options.postgres_security.is_some())?;
        validate_optional_mongo(&config, options.mongo_address)?;
        #[cfg(feature = "mongo")]
        if options.mongo_address.is_some() {
            mongo::validate_database(database)?;
        }
        let postgres = match options.postgres_security {
            Some(config) => Some(postgres::ReloadableSecurity::new(
                tokio::task::spawn_blocking(move || config.load())
                    .await
                    .context("PostgreSQL security preparation worker failed")?
                    .context("failed to prepare PostgreSQL TLS and SCRAM configuration")?,
            )),
            None => None,
        };
        #[cfg(feature = "mongo-tls")]
        let mongo_tls = match options.mongo_tls {
            Some(config) => Some(crate::protocol::mongo::ReloadableTls::new(
                tokio::task::spawn_blocking(move || config.load())
                    .await
                    .context("Mongo TLS preparation worker failed")?
                    .context("failed to prepare Mongo TLS configuration")?,
            )),
            None => None,
        };
        let data_router = options
            .sqlite_remote
            .map(|config| {
                crate::protocol::sqlite_remote::router(database.engine().clone(), config)
                    .map_err(anyhow::Error::msg)
            })
            .transpose()?;
        Self::start_prepared(
            database,
            config,
            PreparedOptions {
                postgres,
                data_router,
                mongo_address: options.mongo_address,
                #[cfg(feature = "mongo-tls")]
                mongo_tls,
            },
        )
        .await
    }
}

#[cfg(all(test, feature = "mongo-tls"))]
mod tests;
