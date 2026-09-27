use super::*;
use crate::{EngineError, EngineErrorKind, RequestContext};

fn security_files() -> (tempfile::TempDir, postgres::SecurityConfig) {
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("server.crt");
    let key = directory.path().join("server.key");
    let password = directory.path().join("password");
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
    std::fs::write(&password, b"test-secret").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for path in [&key, &password] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
    }
    let config = postgres::SecurityConfig::new(certificate, key, "briskdb", password).unwrap();
    (directory, config)
}

async fn attached(db: &BriskDb, config: postgres::SecurityConfig) -> AttachedServer {
    AttachedServer::start_secure(
        db,
        ListenerConfig {
            http_listen: "127.0.0.1:0".parse().unwrap(),
            admin_listen: None,
            postgres_listen: Some("127.0.0.1:0".parse().unwrap()),
        },
        config,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn postgres_reload_rejects_engine_exit_before_and_during_preparation() {
    for stop_before_preparation in [true, false] {
        for fully_close in [false, true] {
            let data = tempfile::tempdir().unwrap();
            let (_secrets, config) = security_files();
            let db = BriskDb::builder(data.path())
                .with_shard_count(2)
                .open()
                .await
                .unwrap();
            let mut server = attached(&db, config.clone()).await;
            let original = server.postgres_security.as_ref().unwrap().snapshot();
            let prepared = std::sync::atomic::AtomicBool::new(false);
            let stop = async {
                if fully_close {
                    db.close().await.unwrap();
                } else {
                    db.begin_close();
                }
            };
            let result = if stop_before_preparation {
                stop.await;
                server
                    .reload_postgres_security_with(RequestContext::new(), async {
                        prepared.store(true, std::sync::atomic::Ordering::SeqCst);
                        Ok(config.load().unwrap())
                    })
                    .await
            } else {
                let loaded = config.load().unwrap();
                let (release, ready) = oneshot::channel();
                let mut operation = Box::pin(server.reload_postgres_security_with(
                    RequestContext::new(),
                    async {
                        ready.await.unwrap();
                        Ok(loaded)
                    },
                ));
                assert!(futures::poll!(operation.as_mut()).is_pending());
                stop.await;
                release.send(()).unwrap();
                operation.await
            };
            assert!(
                result.is_err(),
                "a draining/closed borrowed engine must reject PostgreSQL reload"
            );
            assert!(
                !prepared.load(std::sync::atomic::Ordering::SeqCst),
                "preflight must precede file I/O and derivation"
            );
            assert!(Arc::ptr_eq(
                &original,
                &server.postgres_security.as_ref().unwrap().snapshot()
            ));
            server.close().await.unwrap();
            db.close().await.unwrap();
            drop(db);
            assert!(server.engine_readiness.snapshot().is_none());
            let reopened = BriskDb::builder(data.path()).open().await.unwrap();
            assert!(server.reload_postgres_security(config).await.is_err());
            assert!(server.engine_readiness.snapshot().is_none());
            reopened.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn cancelled_reload_never_publishes_and_a_later_reload_succeeds() {
    let data = tempfile::tempdir().unwrap();
    let (_secrets, config) = security_files();
    let db = BriskDb::builder(data.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let mut server = attached(&db, config.clone()).await;
    let original = server.postgres_security.as_ref().unwrap().snapshot();
    let (release, ready) = oneshot::channel::<()>();
    let loaded = config.load().unwrap();
    let mut operation = Box::pin(server.reload_postgres_security_with(
        crate::RequestContext::new(),
        async {
            ready.await.unwrap();
            Ok(loaded)
        },
    ));
    assert!(futures::poll!(operation.as_mut()).is_pending());
    drop(operation);
    assert!(release.send(()).is_err());
    assert!(Arc::ptr_eq(
        &original,
        &server.postgres_security.as_ref().unwrap().snapshot()
    ));
    server.reload_postgres_security(config).await.unwrap();
    assert!(!Arc::ptr_eq(
        &original,
        &server.postgres_security.as_ref().unwrap().snapshot()
    ));
    server.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn listener_exit_during_preparation_prevents_publication() {
    let data = tempfile::tempdir().unwrap();
    let (_secrets, config) = security_files();
    let db = BriskDb::builder(data.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let mut server = attached(&db, config.clone()).await;
    let original = server.postgres_security.as_ref().unwrap().snapshot();
    let (release, ready) = oneshot::channel::<()>();
    let loaded = config.load().unwrap();
    let mut operation = Box::pin(server.reload_postgres_security_with(
        crate::RequestContext::new(),
        async {
            ready.await.unwrap();
            Ok(loaded)
        },
    ));
    assert!(futures::poll!(operation.as_mut()).is_pending());
    server.task.as_ref().unwrap().abort();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !server.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    release.send(()).unwrap();
    assert!(operation.await.unwrap_err().to_string().contains("running"));
    assert!(Arc::ptr_eq(
        &original,
        &server.postgres_security.as_ref().unwrap().snapshot()
    ));
    assert!(server.close().await.is_err());
    db.close().await.unwrap();
}

#[tokio::test]
async fn request_controls_are_rechecked_after_preparation_before_publication() {
    let data = tempfile::tempdir().unwrap();
    let (_secrets, config) = security_files();
    let db = BriskDb::builder(data.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let mut server = attached(&db, config.clone()).await;
    let original = server.postgres_security.as_ref().unwrap().snapshot();
    for cancel in [true, false] {
        let loaded = config.load().unwrap();
        let context = if cancel {
            RequestContext::new()
        } else {
            RequestContext::new()
                .with_timeout(Duration::from_millis(5))
                .unwrap()
        };
        let token = context.cancellation_token();
        let error = server
            .reload_postgres_security_with(context, async {
                // Deliberately finish without yielding: select has already polled
                // the controls before they become ready. Only the publication
                // guard can prevent this otherwise-ready candidate from committing.
                if cancel {
                    token.cancel();
                } else {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Ok(loaded)
            })
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<EngineError>().unwrap().kind(),
            if cancel {
                EngineErrorKind::Cancelled
            } else {
                EngineErrorKind::DeadlineExceeded
            }
        );
        assert!(Arc::ptr_eq(
            &original,
            &server.postgres_security.as_ref().unwrap().snapshot()
        ));
    }
    server.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn pre_cancelled_and_expired_controls_never_start_preparation() {
    let data = tempfile::tempdir().unwrap();
    let (_secrets, config) = security_files();
    let db = BriskDb::builder(data.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let mut server = attached(&db, config).await;
    let cancelled = RequestContext::new();
    cancelled.cancellation_token().cancel();
    for (context, kind) in [
        (cancelled, EngineErrorKind::Cancelled),
        (
            RequestContext::new().with_deadline(std::time::Instant::now()),
            EngineErrorKind::DeadlineExceeded,
        ),
    ] {
        let error = server
            .reload_postgres_security_with(context, async {
                panic!("pre-cancelled/expired reload must not start preparation")
            })
            .await
            .unwrap_err();
        assert_eq!(error.downcast_ref::<EngineError>().unwrap().kind(), kind);
    }
    server.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn request_controls_interrupt_pending_preparation_without_publication() {
    let data = tempfile::tempdir().unwrap();
    let (_secrets, config) = security_files();
    let db = BriskDb::builder(data.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let mut server = attached(&db, config).await;
    let original = server.postgres_security.as_ref().unwrap().snapshot();
    for cancel in [true, false] {
        let context = if cancel {
            RequestContext::new()
        } else {
            RequestContext::new()
                .with_timeout(Duration::from_millis(10))
                .unwrap()
        };
        let token = context.cancellation_token();
        let (entered, entered_rx) = oneshot::channel();
        let operation = server.reload_postgres_security_with(context, async {
            entered.send(()).unwrap();
            std::future::pending::<anyhow::Result<postgres::LoadedSecurity>>().await
        });
        let trigger = async {
            entered_rx.await.unwrap();
            if cancel {
                token.cancel();
            }
        };
        let (result, ()) = tokio::join!(operation, trigger);
        let error = result.unwrap_err();
        assert_eq!(
            error.downcast_ref::<EngineError>().unwrap().kind(),
            if cancel {
                EngineErrorKind::Cancelled
            } else {
                EngineErrorKind::DeadlineExceeded
            }
        );
        assert!(Arc::ptr_eq(
            &original,
            &server.postgres_security.as_ref().unwrap().snapshot()
        ));
    }
    server.close().await.unwrap();
    db.close().await.unwrap();
}
