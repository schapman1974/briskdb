use super::*;
use std::{future::poll_fn, task::Poll};

fn sources(root: &std::path::Path) -> Sources {
    let certificate = root.join("certificate");
    let key = root.join("key");
    let password = root.join("password");
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
    std::fs::write(&password, b"daemon-reload-test-secret\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for path in [&key, &password] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
    }
    Sources {
        postgres: Some(
            postgres::SecurityConfig::new(&certificate, &key, "briskdb", password).unwrap(),
        ),
        #[cfg(feature = "mongo-tls")]
        mongo: Some(crate::protocol::mongo::MongoTlsConfig::new(
            certificate,
            key,
        )),
    }
}

fn targets(sources: &Sources) -> Targets {
    let loaded = sources.clone().load().unwrap();
    Targets {
        postgres: loaded.postgres.map(postgres::ReloadableSecurity::new),
        #[cfg(feature = "mongo-tls")]
        mongo: loaded.mongo.map(crate::protocol::mongo::ReloadableTls::new),
    }
}

async fn database() -> (tempfile::TempDir, BriskDb) {
    let root = tempfile::tempdir().unwrap();
    let db = BriskDb::builder(root.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    (root, db)
}

#[test]
fn explicit_options_validate_reload_and_do_not_print_identity_paths() {
    assert!(!DaemonOptions::new().reload_on_sighup);
    assert!(DaemonOptions::new().validate_reload(false).is_ok());
    assert!(
        DaemonOptions::new()
            .with_sighup_reload()
            .validate_reload(false)
            .is_err()
    );
    assert_eq!(
        DaemonOptions::new()
            .with_sighup_reload()
            .validate_reload(true)
            .is_ok(),
        cfg!(unix)
    );
    #[cfg(feature = "mongo-tls")]
    {
        let options = DaemonOptions::new()
            .with_mongo_tls(
                "127.0.0.1:1".parse().unwrap(),
                crate::protocol::mongo::MongoTlsConfig::new("hidden-cert", "hidden-key"),
            )
            .with_mongo("127.0.0.1:2".parse().unwrap())
            .with_sighup_reload();
        assert!(options.tls.is_some());
        assert_eq!(options.address.unwrap().port(), 2);
        assert_eq!(options.validate_reload(false).is_ok(), cfg!(unix));
        let text = format!("{options:?}");
        assert!(text.contains("mongo_tls: true"));
        assert!(!text.contains("hidden"));
    }
    assert_eq!(
        failure_reason(&anyhow::anyhow!("secret-value")),
        "invalid_configuration_or_lifecycle"
    );
}

#[tokio::test]
async fn controls_are_checked_before_and_after_preparation() {
    let (_root, db) = database().await;
    let secrets = tempfile::tempdir().unwrap();
    let sources = sources(secrets.path());
    let targets = targets(&sources);
    let original = targets.postgres.as_ref().unwrap().snapshot();
    let shutdown = CancellationToken::new();
    let cancelled = RequestContext::new();
    cancelled.cancellation_token().cancel();
    for context in [
        cancelled,
        RequestContext::new().with_deadline(std::time::Instant::now()),
    ] {
        assert!(
            targets
                .publish_after(db.engine(), &shutdown, context, async {
                    panic!("must not poll")
                })
                .await
                .is_err()
        );
    }
    for cancel in [true, false] {
        let loaded = sources.clone().load().unwrap();
        let context = if cancel {
            RequestContext::new()
        } else {
            RequestContext::new()
                .with_timeout(Duration::from_millis(5))
                .unwrap()
        };
        let token = context.cancellation_token();
        let error = targets
            .publish_after(db.engine(), &shutdown, context, async {
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
            failure_reason(&error),
            if cancel {
                "cancelled"
            } else {
                "deadline_exceeded"
            }
        );
        assert!(Arc::ptr_eq(
            &original,
            &targets.postgres.as_ref().unwrap().snapshot()
        ));
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn shutdown_engine_exit_and_dropped_preparation_cannot_publish() {
    for stop_engine in [false, true] {
        let (_root, db) = database().await;
        let secrets = tempfile::tempdir().unwrap();
        let sources = sources(secrets.path());
        let targets = targets(&sources);
        let original = targets.postgres.as_ref().unwrap().snapshot();
        let shutdown = CancellationToken::new();
        let (entered, started) = oneshot::channel();
        let (release, ready) = oneshot::channel();
        let loaded = sources.clone().load().unwrap();
        let operation =
            targets.publish_after(db.engine(), &shutdown, RequestContext::new(), async {
                entered.send(()).unwrap();
                ready.await.unwrap();
                Ok(loaded)
            });
        let trigger = async {
            started.await.unwrap();
            if stop_engine {
                db.close().await.unwrap();
            } else {
                shutdown.cancel();
            }
            let _ = release.send(());
        };
        let (result, ()) = tokio::join!(operation, trigger);
        assert!(result.is_err());
        assert!(Arc::ptr_eq(
            &original,
            &targets.postgres.as_ref().unwrap().snapshot()
        ));
        db.close().await.unwrap();
    }
    let (_root, db) = database().await;
    let secrets = tempfile::tempdir().unwrap();
    let sources = sources(secrets.path());
    let targets = targets(&sources);
    let shutdown = CancellationToken::new();
    let (release, ready) = oneshot::channel::<()>();
    let mut operation =
        Box::pin(
            targets.publish_after(db.engine(), &shutdown, RequestContext::new(), async {
                ready.await.unwrap();
                sources.load()
            }),
        );
    poll_fn(|cx| {
        assert!(operation.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(operation);
    assert!(release.send(()).is_err());
    db.close().await.unwrap();
}

#[tokio::test]
async fn failed_complete_bundle_preserves_every_target_and_success_replaces_them() {
    let (_root, db) = database().await;
    let secrets = tempfile::tempdir().unwrap();
    let sources = sources(secrets.path());
    let targets = targets(&sources);
    let postgres = targets.postgres.as_ref().unwrap().snapshot();
    #[cfg(feature = "mongo-tls")]
    let mongo = targets.mongo.as_ref().unwrap().snapshot();
    let shutdown = CancellationToken::new();
    let mut invalid = sources.clone();
    #[cfg(feature = "mongo-tls")]
    {
        invalid.mongo = Some(crate::protocol::mongo::MongoTlsConfig::new(
            "missing", "missing",
        ));
    }
    #[cfg(not(feature = "mongo-tls"))]
    {
        invalid.postgres = Some(
            postgres::SecurityConfig::new("missing", "missing", "briskdb", "missing").unwrap(),
        );
    }
    assert!(
        targets
            .publish_after(db.engine(), &shutdown, RequestContext::new(), async {
                invalid.load()
            })
            .await
            .is_err()
    );
    assert!(Arc::ptr_eq(
        &postgres,
        &targets.postgres.as_ref().unwrap().snapshot()
    ));
    #[cfg(feature = "mongo-tls")]
    assert!(Arc::ptr_eq(
        &mongo,
        &targets.mongo.as_ref().unwrap().snapshot()
    ));
    let mut mismatched = sources.clone().load().unwrap();
    mismatched.postgres = None;
    assert!(
        targets
            .publish_after(db.engine(), &shutdown, RequestContext::new(), async {
                Ok(mismatched)
            })
            .await
            .is_err()
    );
    assert!(Arc::ptr_eq(
        &postgres,
        &targets.postgres.as_ref().unwrap().snapshot()
    ));
    targets
        .publish_after(db.engine(), &shutdown, RequestContext::new(), async {
            sources.load()
        })
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(
        &postgres,
        &targets.postgres.as_ref().unwrap().snapshot()
    ));
    #[cfg(feature = "mongo-tls")]
    assert!(!Arc::ptr_eq(
        &mongo,
        &targets.mongo.as_ref().unwrap().snapshot()
    ));
    db.close().await.unwrap();
}

#[tokio::test]
async fn expired_worker_is_joined_without_publication_before_another_attempt() {
    for stop_waiting in [false, true] {
        let (_root, db) = database().await;
        let secrets = tempfile::tempdir().unwrap();
        let sources = sources(secrets.path());
        let targets = targets(&sources);
        let original = targets.postgres.as_ref().unwrap().snapshot();
        let shutdown = CancellationToken::new();
        let loaded = sources.clone().load().unwrap();
        let (release, ready) = oneshot::channel();
        let mut worker = tokio::spawn(async {
            ready.await.unwrap();
            Ok(loaded)
        });
        let context = RequestContext::new().with_deadline(std::time::Instant::now());
        let mut attempt =
            Box::pin(targets.finish_attempt(db.engine(), &shutdown, context, &mut worker));
        poll_fn(|cx| {
            assert!(attempt.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        // Deadline rejection alone must not make the controller available to
        // launch a second worker. Shutdown may abandon waiting, never publish.
        if stop_waiting {
            shutdown.cancel();
            assert_eq!(
                failure_reason(&attempt.await.unwrap_err()),
                "deadline_exceeded"
            );
            release.send(()).unwrap();
            worker.await.unwrap().unwrap();
        } else {
            release.send(()).unwrap();
            assert_eq!(
                failure_reason(&attempt.await.unwrap_err()),
                "deadline_exceeded"
            );
        }
        assert!(Arc::ptr_eq(
            &original,
            &targets.postgres.as_ref().unwrap().snapshot()
        ));
        db.close().await.unwrap();
    }
}
