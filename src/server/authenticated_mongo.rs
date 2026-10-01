//! Process-owned Mongo-only host for an already provisioned security root.
use super::*;
use crate::protocol::mongo::{MongoServer, MongoTlsConfig, ReloadableTls};

/// Explicit authenticated Mongo-only process configuration. No HTTP, admin or
/// PostgreSQL listeners are created, and no credentials are provisioned here.
#[derive(Clone)]
pub struct AuthenticatedMongoConfig {
    pub data_dir: PathBuf,
    pub shards: u16,
    pub listen: SocketAddr,
    pub tls: MongoTlsConfig,
}

impl std::fmt::Debug for AuthenticatedMongoConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthenticatedMongoConfig")
            .field("listen", &self.listen)
            .field("shards", &self.shards)
            .finish_non_exhaustive()
    }
}

/// Serve only TLS/SCRAM Mongo until SIGINT/SIGTERM (Ctrl-C on Windows).
/// Existing anonymous and composed server entry points are unchanged.
/// The security catalog must already exist; startup never falls back to an
/// anonymous root or opens another connector. TLS identity is loaded before
/// the database is opened. SIGHUP reload is not enabled by this entry point.
pub async fn run_authenticated_mongo(
    config: AuthenticatedMongoConfig,
    options: EngineOptions,
) -> anyhow::Result<()> {
    let signal = shutdown_signal()?;
    run_until(config, options, signal).await
}

async fn run_until(
    config: AuthenticatedMongoConfig,
    options: EngineOptions,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    options.validate_for_shards(config.shards)?;
    let tls = config.tls;
    let identity = tokio::task::spawn_blocking(move || tls.load())
        .await
        .context("Mongo TLS preparation worker failed")??;
    let database = BriskDb::builder(&config.data_dir)
        .with_shard_count(config.shards)
        .with_engine_options(options)
        .with_document_support(crate::DocumentSupport::Enabled)
        .with_authenticated_root()
        .open()
        .await?;
    let mut guard = ShutdownOnDrop::new(database.engine().clone());
    let result = async {
        let listener = tokio::net::TcpListener::bind(config.listen).await?;
        let mut server = MongoServer::from_bound_tls(
            &database,
            listener,
            crate::CancellationToken::new(),
            ReloadableTls::new(identity),
        )?;
        info!(mongo_listen = %server.address(), "Authenticated Mongo-only BriskDB is ready");
        let outcome = tokio::select! {
            () = shutdown => Ok(()),
            result = server.wait() => match result {
                Err(error) => Err(anyhow::Error::from(error)),
                Ok(()) => Err(anyhow::anyhow!("authenticated Mongo listener stopped unexpectedly")),
            },
        };
        let closed = server.close().await;
        outcome?;
        closed?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    // Close even after a bind/listener failure; cancellation fences the owned
    // engine through the guard rather than leaving it admitting new work.
    database.begin_close();
    let closed = database.close().await;
    guard.disarm();
    result?;
    closed?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(root: &std::path::Path, secrets: &std::path::Path) -> AuthenticatedMongoConfig {
        let certificate = secrets.join("server.crt");
        let key = secrets.join("server.key");
        std::fs::write(
            &certificate,
            include_bytes!("../../tests/fixtures/postgres-tls/server.crt"),
        )
        .unwrap();
        std::fs::write(
            &key,
            include_bytes!("../../tests/fixtures/postgres-tls/server.key"),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        AuthenticatedMongoConfig {
            data_dir: root.to_owned(),
            shards: 2,
            listen: "127.0.0.1:0".parse().unwrap(),
            tls: MongoTlsConfig::new(certificate, key),
        }
    }

    #[tokio::test]
    async fn valid_tls_does_not_activate_missing_or_anonymous_roots() {
        let parent = tempfile::tempdir().unwrap();
        let secrets = tempfile::tempdir().unwrap();
        let root = parent.path().join("absent");
        let configuration = config(&root, secrets.path());
        assert!(
            run_until(
                configuration.clone(),
                EngineOptions::default(),
                std::future::ready(())
            )
            .await
            .is_err()
        );
        assert!(!root.join("manifest.sqlite").exists());
        let ordinary = BriskDb::builder(&root)
            .with_shard_count(2)
            .with_document_support(crate::DocumentSupport::Enabled)
            .open()
            .await
            .unwrap();
        ordinary.close().await.unwrap();
        drop(ordinary);
        assert!(
            run_until(
                configuration,
                EngineOptions::default(),
                std::future::ready(())
            )
            .await
            .is_err()
        );
        assert!(!root.join("security.sqlite").exists());
        // Failed secure startup must not convert or damage the anonymous root.
        let reopened = BriskDb::builder(&root)
            .with_shard_count(2)
            .open()
            .await
            .unwrap();
        reopened.close().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bind_failure_and_normal_shutdown_release_the_owned_root() {
        use crate::core::{
            authentication::ScramSha256Verifier,
            security_catalog::{SecurityCatalog, SecurityName},
        };
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let secrets = tempfile::tempdir().unwrap();
        let ordinary = BriskDb::builder(root.path())
            .with_shard_count(2)
            .with_document_support(crate::DocumentSupport::Enabled)
            .open()
            .await
            .unwrap();
        ordinary.close().await.unwrap();
        drop(ordinary);
        let mut catalog = SecurityCatalog::new();
        catalog
            .create_user(
                SecurityName::new("admin", "test").unwrap(),
                ScramSha256Verifier::from_password_with_iterations("test-only-password", 4096)
                    .unwrap(),
                [],
            )
            .unwrap();
        Engine::provision_security(root.path(), 2, catalog)
            .await
            .unwrap();
        let mut configuration = config(root.path(), secrets.path());
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        configuration.listen = occupied.local_addr().unwrap();
        assert!(
            run_until(
                configuration.clone(),
                EngineOptions::default(),
                std::future::ready(())
            )
            .await
            .is_err()
        );
        drop(occupied);
        for _ in 0..2 {
            run_until(
                configuration.clone(),
                EngineOptions::default(),
                std::future::ready(()),
            )
            .await
            .unwrap();
            let released = tokio::net::TcpListener::bind(configuration.listen)
                .await
                .unwrap();
            drop(released);
        }
        let reopened = BriskDb::builder(root.path())
            .with_shard_count(2)
            .with_document_support(crate::DocumentSupport::Enabled)
            .with_authenticated_root()
            .open()
            .await
            .unwrap();
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_tls_never_creates_a_root_or_exposes_paths_in_config_debug() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("private-root");
        let config = AuthenticatedMongoConfig {
            data_dir: root.clone(),
            shards: 2,
            listen: "127.0.0.1:0".parse().unwrap(),
            tls: MongoTlsConfig::new("private-missing-cert", "private-missing-key"),
        };
        assert!(!format!("{config:?}").contains("private"));
        assert!(
            run_until(config, EngineOptions::default(), std::future::ready(()))
                .await
                .is_err()
        );
        assert!(!root.exists());
    }
}
