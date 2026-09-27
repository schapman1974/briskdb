use super::*;
use std::{future::poll_fn, task::Poll};

async fn setup() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    BriskDb,
    AttachedServer,
    MongoTlsConfig,
) {
    let root = tempfile::tempdir().unwrap();
    let secrets = tempfile::tempdir().unwrap();
    let certificate = secrets.path().join("server.crt");
    let key = secrets.path().join("server.key");
    std::fs::write(
        &certificate,
        include_bytes!("../../../tests/fixtures/postgres-tls/server.crt"),
    )
    .unwrap();
    std::fs::write(
        &key,
        include_bytes!("../../../tests/fixtures/postgres-tls/server.key"),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let config = MongoTlsConfig::new(certificate, key);
    let db = BriskDb::builder(root.path())
        .with_shard_count(2)
        .with_document_support(crate::DocumentSupport::Enabled)
        .open()
        .await
        .unwrap();
    let server = AttachedServer::start_with_options(
        &db,
        listeners(),
        AttachedServerOptions::new().with_mongo_tls("127.0.0.1:0".parse().unwrap(), config.clone()),
    )
    .await
    .unwrap();
    (root, secrets, db, server, config)
}

fn listeners() -> ListenerConfig {
    ListenerConfig {
        http_listen: "127.0.0.1:0".parse().unwrap(),
        admin_listen: None,
        postgres_listen: None,
    }
}

#[tokio::test]
async fn request_controls_guard_preparation_and_final_publication() {
    let (_root, _secrets, db, mut server, config) = setup().await;
    let original = server.mongo_tls.as_ref().unwrap().identity.snapshot();
    let cancelled = RequestContext::new();
    cancelled.cancellation_token().cancel();
    for (context, kind) in [
        (cancelled, crate::EngineErrorKind::Cancelled),
        (
            RequestContext::new().with_deadline(std::time::Instant::now()),
            crate::EngineErrorKind::DeadlineExceeded,
        ),
    ] {
        let error = server
            .reload_mongo_tls_with(context, async { panic!("must not prepare") })
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<crate::EngineError>().unwrap().kind(),
            kind
        );
    }
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
            .reload_mongo_tls_with(context, async {
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
            error.downcast_ref::<crate::EngineError>().unwrap().kind(),
            if cancel {
                crate::EngineErrorKind::Cancelled
            } else {
                crate::EngineErrorKind::DeadlineExceeded
            }
        );
        assert!(Arc::ptr_eq(
            &original,
            &server.mongo_tls.as_ref().unwrap().identity.snapshot()
        ));
    }
    server.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn pending_preparation_is_interruptible_and_dropping_cannot_publish() {
    let (_root, _secrets, db, mut server, config) = setup().await;
    let original = server.mongo_tls.as_ref().unwrap().identity.snapshot();
    let (release, ready) = oneshot::channel::<()>();
    let loaded = config.load().unwrap();
    let mut operation = Box::pin(server.reload_mongo_tls_with(RequestContext::new(), async {
        ready.await.unwrap();
        Ok(loaded)
    }));
    poll_fn(|cx| {
        assert!(operation.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(operation);
    assert!(release.send(()).is_err());
    for cancel in [true, false] {
        let context = if cancel {
            RequestContext::new()
        } else {
            RequestContext::new()
                .with_timeout(Duration::from_millis(10))
                .unwrap()
        };
        let token = context.cancellation_token();
        let (entered, ready) = oneshot::channel();
        let operation = server.reload_mongo_tls_with(context, async {
            entered.send(()).unwrap();
            std::future::pending().await
        });
        let trigger = async {
            ready.await.unwrap();
            if cancel {
                token.cancel();
            }
        };
        let (result, ()) = tokio::join!(operation, trigger);
        assert_eq!(
            result
                .unwrap_err()
                .downcast_ref::<crate::EngineError>()
                .unwrap()
                .kind(),
            if cancel {
                crate::EngineErrorKind::Cancelled
            } else {
                crate::EngineErrorKind::DeadlineExceeded
            }
        );
        assert!(Arc::ptr_eq(
            &original,
            &server.mongo_tls.as_ref().unwrap().identity.snapshot()
        ));
    }
    server.reload_mongo_tls(config).await.unwrap();
    assert!(!Arc::ptr_eq(
        &original,
        &server.mongo_tls.as_ref().unwrap().identity.snapshot()
    ));
    server.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn engine_or_listener_exit_during_preparation_prevents_publication() {
    for stop_engine in [false, true] {
        let (_root, _secrets, db, mut server, config) = setup().await;
        let original = server.mongo_tls.as_ref().unwrap().identity.snapshot();
        let loaded = config.load().unwrap();
        let (entered, ready) = oneshot::channel();
        let (release, completed) = oneshot::channel();
        let operation = server.reload_mongo_tls_with(RequestContext::new(), async {
            entered.send(()).unwrap();
            completed.await.unwrap();
            Ok(loaded)
        });
        let trigger = async {
            ready.await.unwrap();
            if stop_engine {
                db.close().await.unwrap();
            } else {
                server.task.as_ref().unwrap().abort();
                while !server.is_closed() {
                    tokio::task::yield_now().await;
                }
            }
            release.send(()).unwrap();
        };
        let (result, ()) = tokio::join!(operation, trigger);
        assert!(result.is_err());
        assert!(Arc::ptr_eq(
            &original,
            &server.mongo_tls.as_ref().unwrap().identity.snapshot()
        ));
        let _ = server.close().await;
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn closed_handle_is_non_owning_and_plaintext_cannot_be_upgraded() {
    let (root, _secrets, db, mut server, config) = setup().await;
    let mut plain =
        AttachedServer::start_with_mongo(&db, listeners(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
    assert!(
        plain
            .reload_mongo_tls(config.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("already-encrypted")
    );
    plain.close().await.unwrap();
    server.begin_close();
    assert!(
        server
            .reload_mongo_tls(config.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("running")
    );
    server.close().await.unwrap();
    db.close().await.unwrap();
    drop(db);
    assert!(
        server
            .mongo_tls
            .as_ref()
            .unwrap()
            .engine
            .snapshot()
            .is_none()
    );
    let reopened = BriskDb::builder(root.path())
        .with_document_support(crate::DocumentSupport::Enabled)
        .open()
        .await
        .unwrap();
    assert!(server.reload_mongo_tls(config).await.is_err());
    assert!(
        server
            .mongo_tls
            .as_ref()
            .unwrap()
            .engine
            .snapshot()
            .is_none()
    );
    reopened.close().await.unwrap();
}
