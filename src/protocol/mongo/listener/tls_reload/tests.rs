use super::*;
use crate::protocol::mongo::MongoTlsConfig;
use std::{future::poll_fn, task::Poll};
use tokio::sync::oneshot;

#[test]
fn concurrent_publications_keep_certificate_and_budget_in_one_generation() {
    let (_files, first) = files();
    let (_other_files, second) = files();
    std::fs::write(
        second.certificate(),
        include_bytes!("../../../../../tests/fixtures/postgres-tls/rotated.crt"),
    )
    .unwrap();
    std::fs::write(
        second.private_key(),
        include_bytes!("../../../../../tests/fixtures/postgres-tls/rotated.key"),
    )
    .unwrap();
    let first = first
        .with_handshake_timeout(Duration::from_secs(1))
        .unwrap()
        .load()
        .unwrap();
    let second = second
        .with_handshake_timeout(Duration::from_secs(2))
        .unwrap()
        .load()
        .unwrap();
    let source = crate::protocol::mongo::tls::ReloadableTls::new(first.clone());
    let retained = source.snapshot();
    std::thread::scope(|scope| {
        for index in 0..4 {
            let source = source.clone();
            let first = &first;
            let second = &second;
            scope.spawn(move || {
                for _ in 0..500 {
                    source.replace(if index % 2 == 0 {
                        first.clone()
                    } else {
                        second.clone()
                    });
                    let current = source.snapshot();
                    if Arc::ptr_eq(current.acceptor.config(), first.acceptor.config()) {
                        assert_eq!(current.handshake_timeout, Duration::from_secs(1));
                    } else {
                        assert!(Arc::ptr_eq(
                            current.acceptor.config(),
                            second.acceptor.config()
                        ));
                        assert_eq!(current.handshake_timeout, Duration::from_secs(2));
                    }
                }
            });
        }
    });
    assert!(Arc::ptr_eq(
        retained.acceptor.config(),
        first.acceptor.config()
    ));
    assert_eq!(retained.handshake_timeout, Duration::from_secs(1));
}

fn files() -> (tempfile::TempDir, MongoTlsConfig) {
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("server.crt");
    let key = directory.path().join("server.key");
    std::fs::write(
        &certificate,
        include_bytes!("../../../../../tests/fixtures/postgres-tls/server.crt"),
    )
    .unwrap();
    std::fs::write(
        &key,
        include_bytes!("../../../../../tests/fixtures/postgres-tls/server.key"),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    (directory, MongoTlsConfig::new(certificate, key))
}

async fn setup() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    BriskDb,
    MongoServer,
    MongoTlsConfig,
) {
    let root = tempfile::tempdir().unwrap();
    let (files, config) = files();
    let database = BriskDb::builder(root.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let server = MongoServer::start_tls(&database, "127.0.0.1:0".parse().unwrap(), config.clone())
        .await
        .unwrap();
    (root, files, database, server, config)
}

#[tokio::test]
async fn controls_are_checked_before_preparation_and_immediately_before_publication() {
    let (_root, _files, database, mut server, config) = setup().await;
    let target = server.tls.as_ref().unwrap();
    let original = target.snapshot();
    let cancelled = RequestContext::new();
    cancelled.cancellation_token().cancel();
    for (context, kind) in [
        (cancelled, io::ErrorKind::Interrupted),
        (
            RequestContext::new().with_deadline(Instant::now()),
            io::ErrorKind::TimedOut,
        ),
    ] {
        let error = server
            .reload_tls_with(context, async { panic!("must not prepare") })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), kind);
        assert!(Arc::ptr_eq(&original, &target.snapshot()));
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
            .reload_tls_with(context, async {
                // Complete without yielding: only the final guard can catch this race.
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
            error.kind(),
            if cancel {
                io::ErrorKind::Interrupted
            } else {
                io::ErrorKind::TimedOut
            }
        );
        assert!(Arc::ptr_eq(&original, &target.snapshot()));
    }
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn dropped_or_interrupted_preparations_cannot_publish_and_later_reload_recovers() {
    let (_root, _files, database, mut server, config) = setup().await;
    let original = server.tls.as_ref().unwrap().snapshot();
    let (release, ready) = oneshot::channel::<()>();
    let loaded = config.load().unwrap();
    let mut operation = Box::pin(server.reload_tls_with(RequestContext::new(), async {
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
        let operation = server.reload_tls_with(context, async {
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
            result.unwrap_err().kind(),
            if cancel {
                io::ErrorKind::Interrupted
            } else {
                io::ErrorKind::TimedOut
            }
        );
        assert!(Arc::ptr_eq(
            &original,
            &server.tls.as_ref().unwrap().snapshot()
        ));
    }
    server.reload_tls(config).await.unwrap();
    assert!(!Arc::ptr_eq(
        &original,
        &server.tls.as_ref().unwrap().snapshot()
    ));
    server.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn closing_or_engine_exit_during_preparation_never_publishes() {
    for stop_engine in [false, true] {
        let (_root, _files, database, mut server, config) = setup().await;
        let original = server.tls.as_ref().unwrap().snapshot();
        let loaded = config.load().unwrap();
        let (entered, ready) = oneshot::channel();
        let (release, completed) = oneshot::channel();
        let operation = server.reload_tls_with(RequestContext::new(), async {
            entered.send(()).unwrap();
            completed.await.unwrap();
            Ok(loaded)
        });
        let trigger = async {
            ready.await.unwrap();
            if stop_engine {
                database.close().await.unwrap();
            } else {
                server.begin_close();
            }
            let _ = release.send(());
        };
        let (result, ()) = tokio::join!(operation, trigger);
        assert!(result.is_err());
        assert!(Arc::ptr_eq(
            &original,
            &server.tls.as_ref().unwrap().snapshot()
        ));
        server.close().await.unwrap();
        database.close().await.unwrap();
    }
}

#[tokio::test]
async fn plaintext_closing_closed_and_failed_listeners_reject_reload_before_file_io() {
    let (_root, _files, database, mut server, _config) = setup().await;
    let missing = MongoTlsConfig::new("not-present", "not-present");
    let mut plaintext = MongoServer::start(&database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    assert!(
        plaintext
            .reload_tls(missing.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("already-encrypted")
    );
    plaintext.close().await.unwrap();
    server.begin_close();
    assert!(
        server
            .reload_tls(missing.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("running")
    );
    server.close().await.unwrap();
    assert!(
        server
            .reload_tls(missing.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("running")
    );
    let (_files, config) = files();
    let mut failed = MongoServer::start_tls(&database, "127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();
    failed.task.as_ref().unwrap().abort();
    poll_fn(|cx| {
        if failed.task.as_ref().unwrap().is_finished() {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
    assert!(
        failed
            .reload_tls(missing)
            .await
            .unwrap_err()
            .to_string()
            .contains("running")
    );
    assert!(failed.close().await.is_err());
    database.close().await.unwrap();
}
