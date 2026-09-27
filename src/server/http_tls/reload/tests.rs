use super::*;
use crate::server::http_tls::tests::{
    closed, config, database, identity, permits, request, tls, tls_stream,
};
use crate::server::{
    AttachedServerOptions, BoundListeners, EngineShutdown, serve_listeners_with_shutdown_mode,
};
use std::{future::poll_fn, task::Poll};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::oneshot,
    time::timeout,
};

async fn setup() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    crate::BriskDb,
    AttachedServer,
    HttpTlsConfig,
) {
    let (root, db) = database().await;
    let files = tempfile::tempdir().unwrap();
    let identity = identity(files.path(), false);
    let server = AttachedServer::start_with_options(
        &db,
        config(),
        AttachedServerOptions::new()
            .with_http_tls(identity.clone())
            .with_admin_tls(identity.clone()),
    )
    .await
    .unwrap();
    (root, files, db, server, identity)
}

fn snapshot(server: &AttachedServer, plane: Plane) -> Arc<Loaded> {
    plane.identity(&server.http_tls).unwrap().snapshot()
}

#[tokio::test]
async fn request_controls_guard_preparation_and_recheck_before_publication() {
    let (_root, _files, db, mut server, identity) = setup().await;
    for plane in [Plane::Data, Plane::Admin] {
        let original = snapshot(&server, plane);
        let cancelled = RequestContext::new();
        cancelled.cancellation_token().cancel();
        for (context, expected) in [
            (cancelled, crate::EngineErrorKind::Cancelled),
            (
                RequestContext::new().with_deadline(std::time::Instant::now()),
                crate::EngineErrorKind::DeadlineExceeded,
            ),
        ] {
            let error = server
                .reload_http_plane_with(plane, context, async { panic!("must not prepare") })
                .await
                .unwrap_err();
            assert_eq!(
                error.downcast_ref::<crate::EngineError>().unwrap().kind(),
                expected
            );
        }
        for cancel in [true, false] {
            let loaded = identity.clone().load().unwrap();
            let context = if cancel {
                RequestContext::new()
            } else {
                RequestContext::new()
                    .with_timeout(Duration::from_millis(5))
                    .unwrap()
            };
            let token = context.cancellation_token();
            let error = server
                .reload_http_plane_with(plane, context, async {
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
            assert!(Arc::ptr_eq(&original, &snapshot(&server, plane)));
        }
    }
    server.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn pending_and_dropped_preparations_cannot_publish_and_later_reload_recovers() {
    let (_root, _files, db, mut server, identity) = setup().await;
    for plane in [Plane::Data, Plane::Admin] {
        let original = snapshot(&server, plane);
        let (release, ready) = oneshot::channel::<()>();
        let loaded = identity.clone().load().unwrap();
        let mut operation =
            Box::pin(
                server.reload_http_plane_with(plane, RequestContext::new(), async {
                    ready.await.unwrap();
                    Ok(loaded)
                }),
            );
        poll_fn(|cx| {
            assert!(operation.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(operation);
        assert!(release.send(()).is_err());
        assert!(Arc::ptr_eq(&original, &snapshot(&server, plane)));
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
            let operation = server.reload_http_plane_with(plane, context, async {
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
            assert!(Arc::ptr_eq(&original, &snapshot(&server, plane)));
        }
        server
            .reload_http_plane(plane, identity.clone(), RequestContext::new())
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&original, &snapshot(&server, plane)));
    }
    server.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn concurrent_successes_publish_whole_identities_in_completion_order() {
    let (_root, _files, db, mut server, original) = setup().await;
    let files = tempfile::tempdir().unwrap();
    let first = original
        .with_handshake_timeout(Duration::from_secs(1))
        .unwrap()
        .load()
        .unwrap();
    let second = identity(files.path(), true)
        .with_handshake_timeout(Duration::from_secs(2))
        .unwrap()
        .load()
        .unwrap();
    for plane in [Plane::Data, Plane::Admin] {
        let (entered_first, ready_first) = oneshot::channel();
        let (entered_second, ready_second) = oneshot::channel();
        let (release_first, loaded_first) = oneshot::channel();
        let (release_second, loaded_second) = oneshot::channel();
        let (published_second, second_done) = oneshot::channel();
        let context = RequestContext::new();
        let after_publication = context.cancellation_token();
        let first_operation = server.reload_http_plane_with(plane, context, async {
            entered_first.send(()).unwrap();
            loaded_first.await.unwrap();
            Ok(first.clone())
        });
        let second_operation = async {
            server
                .reload_http_plane_with(plane, RequestContext::new(), async {
                    entered_second.send(()).unwrap();
                    loaded_second.await.unwrap();
                    Ok(second.clone())
                })
                .await
                .unwrap();
            published_second.send(()).unwrap();
        };
        let trigger = async {
            ready_first.await.unwrap();
            ready_second.await.unwrap();
            release_second.send(()).unwrap();
            second_done.await.unwrap();
            assert!(Arc::ptr_eq(&second, &snapshot(&server, plane)));
            assert_eq!(
                snapshot(&server, plane).handshake_timeout,
                Duration::from_secs(2)
            );
            release_first.send(()).unwrap();
        };
        let (result, (), ()) = timeout(Duration::from_secs(3), async {
            tokio::join!(first_operation, second_operation, trigger)
        })
        .await
        .unwrap();
        result.unwrap();
        after_publication.cancel();
        assert!(Arc::ptr_eq(&first, &snapshot(&server, plane)));
        assert_eq!(
            snapshot(&server, plane).handshake_timeout,
            Duration::from_secs(1)
        );
    }
    server.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn lifecycle_exit_during_preparation_rejects_publication_without_owning_the_engine() {
    for plane in [Plane::Data, Plane::Admin] {
        for stop_engine in [false, true] {
            let (root, _files, db, mut server, identity) = setup().await;
            let original = snapshot(&server, plane);
            let loaded = identity.clone().load().unwrap();
            let (entered, ready) = oneshot::channel();
            let (release, completed) = oneshot::channel();
            let operation = server.reload_http_plane_with(plane, RequestContext::new(), async {
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
                    timeout(Duration::from_secs(3), async {
                        while !server.is_closed() {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                }
                release.send(()).unwrap();
            };
            let (result, ()) = tokio::join!(operation, trigger);
            assert!(result.is_err());
            assert!(Arc::ptr_eq(&original, &snapshot(&server, plane)));
            let _ = server.close().await;
            db.close().await.unwrap();
            drop(db);
            assert!(server.engine_readiness.snapshot().is_none());
            let reopened = crate::BriskDb::builder(root.path()).open().await.unwrap();
            assert!(
                server
                    .reload_http_plane(plane, identity, RequestContext::new())
                    .await
                    .is_err()
            );
            assert!(server.engine_readiness.snapshot().is_none());
            reopened.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn public_reloads_rotate_only_the_selected_plane_and_invalid_material_keeps_the_identity() {
    let (_root, _files, db, mut server, original) = setup().await;
    let replacement_files = tempfile::tempdir().unwrap();
    let replacement = identity(replacement_files.path(), true);
    let addresses = server.addresses();
    for plane in [Plane::Data, Plane::Admin] {
        let (address, path, other) = match plane {
            Plane::Data => (addresses.http(), "/v1", Plane::Admin),
            Plane::Admin => (addresses.admin().unwrap(), "/health", Plane::Data),
        };
        let mut established = tls(address, Some(original.certificate()), "localhost")
            .await
            .unwrap();
        let other_before = snapshot(&server, other);
        match plane {
            Plane::Data => server.reload_http_tls(replacement.clone()).await.unwrap(),
            Plane::Admin => server.reload_admin_tls(replacement.clone()).await.unwrap(),
        }
        assert!(Arc::ptr_eq(&other_before, &snapshot(&server, other)));
        assert!(
            tls(address, Some(original.certificate()), "localhost")
                .await
                .is_err()
        );
        assert!(
            tls(address, Some(replacement.certificate()), "wrong.invalid")
                .await
                .is_err()
        );
        let mut fresh = tls(address, Some(replacement.certificate()), "localhost")
            .await
            .unwrap();
        assert!(
            request(&mut fresh, path, "")
                .await
                .starts_with("HTTP/1.1 200")
        );
        assert!(
            request(&mut established, path, "")
                .await
                .starts_with("HTTP/1.1 200")
        );
        let before_failure = snapshot(&server, plane);
        let mismatch = HttpTlsConfig::new(replacement.certificate(), original.private_key());
        assert!(
            server
                .reload_http_plane(plane, mismatch, RequestContext::new())
                .await
                .is_err()
        );
        assert!(Arc::ptr_eq(&before_failure, &snapshot(&server, plane)));
        let mut fresh = tls(address, Some(replacement.certificate()), "localhost")
            .await
            .unwrap();
        assert!(
            request(&mut fresh, path, "")
                .await
                .starts_with("HTTP/1.1 200")
        );
    }
    server.begin_close();
    assert!(
        server
            .reload_http_tls(original.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("running")
    );
    assert!(
        server
            .reload_admin_tls(original)
            .await
            .unwrap_err()
            .to_string()
            .contains("running")
    );
    server.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn plaintext_and_disabled_planes_cannot_be_upgraded_or_read_identity_files() {
    let (_root, db) = database().await;
    let missing = || HttpTlsConfig::new("missing-cert", "missing-key");
    for disabled in [false, true] {
        let mut listeners = config();
        if disabled {
            listeners.admin_listen = None;
        }
        let mut server = AttachedServer::start(&db, listeners).await.unwrap();
        for result in [
            server.reload_http_tls(missing()).await,
            server.reload_admin_tls(missing()).await,
        ] {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("already-encrypted")
            );
        }
        let mut client = TcpStream::connect(server.addresses().http()).await.unwrap();
        assert!(
            request(&mut client, "/v1", "")
                .await
                .starts_with("HTTP/1.1 200")
        );
        server.close().await.unwrap();
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn admitted_handshakes_keep_their_identity_and_timeout_across_replacement() {
    for admin in [false, true] {
        let (_root, db) = database().await;
        let old_files = tempfile::tempdir().unwrap();
        let new_files = tempfile::tempdir().unwrap();
        let old = identity(old_files.path(), false);
        let new = identity(new_files.path(), true)
            .with_handshake_timeout(Duration::from_millis(300))
            .unwrap();
        let live = Reloadable::new(old.clone().load().unwrap());
        let mut listeners = BoundListeners::bind(&config()).await.unwrap();
        listeners.http_slots = Slots::new(3);
        let slots = if admin {
            listeners.http_tls.admin = Some(live.clone());
            listeners.http_slots.admin.clone()
        } else {
            listeners.http_tls.data = Some(live.clone());
            listeners.http_slots.data.clone()
        };
        let addresses = listeners.addresses().unwrap();
        let address = if admin {
            addresses.admin().unwrap()
        } else {
            addresses.http()
        };
        let path = if admin { "/health" } else { "/v1" };
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(serve_listeners_with_shutdown_mode(
            listeners,
            db.engine().clone(),
            async {
                let _ = stopped.await;
            },
            None,
            EngineShutdown::Borrowed,
            None,
            None,
        ));
        let mut pending = TcpStream::connect(address).await.unwrap();
        let mut established = tls(address, Some(old.certificate()), "localhost")
            .await
            .unwrap();
        permits(&slots, 1).await;
        live.replace(new.clone().load().unwrap());
        let mut expires = TcpStream::connect(address).await.unwrap();
        expires.write_all(&[22, 3, 3]).await.unwrap();
        closed(&mut expires).await;
        permits(&slots, 1).await;
        assert!(
            timeout(Duration::from_millis(20), pending.read(&mut [0]))
                .await
                .is_err()
        );
        let mut pending = tls_stream(pending, Some(old.certificate()), "localhost")
            .await
            .unwrap();
        assert!(
            request(&mut pending, path, "")
                .await
                .starts_with("HTTP/1.1 200")
        );
        assert!(
            request(&mut established, path, "")
                .await
                .starts_with("HTTP/1.1 200")
        );
        assert!(
            tls(address, Some(old.certificate()), "localhost")
                .await
                .is_err()
        );
        let mut fresh = tls(address, Some(new.certificate()), "localhost")
            .await
            .unwrap();
        assert!(
            request(&mut fresh, path, "")
                .await
                .starts_with("HTTP/1.1 200")
        );
        stop.send(()).unwrap();
        timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        permits(&slots, 3).await;
        db.close().await.unwrap();
    }
}
