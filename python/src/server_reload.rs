//! Attached-server reload controls, including cancellation while queued.

use std::{future::Future, sync::TryLockError, time::Duration};

use briskdb::{EngineError, EngineErrorKind, RequestContext};

use super::{
    PostgresSecurityConfig, ServerShared,
    error::{NativeError, NativeResult, listener_error},
};

impl ServerShared {
    pub(crate) fn reload_security_native(
        &self,
        config: PostgresSecurityConfig,
        context: RequestContext,
    ) -> NativeResult<()> {
        self.runtime.runtime.block_on(controlled(
            &context,
            self.reload_serialized(config, &context),
        ))
    }

    // Deliberately serialize reload with close on the existing synchronous
    // handle mutex. This future is polled by block_on on the detached Python
    // caller thread, never spawned onto a runtime worker. Other reloads use
    // try_lock so a queued request still observes cancellation/deadlines.
    #[allow(clippy::await_holding_lock)]
    async fn reload_serialized(
        &self,
        config: PostgresSecurityConfig,
        context: &RequestContext,
    ) -> NativeResult<()> {
        let slot = loop {
            match self.server.try_lock() {
                Ok(slot) => break slot,
                Err(TryLockError::Poisoned(error)) => return Err(error.into()),
                Err(TryLockError::WouldBlock) => {}
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };
        let server = slot.as_ref().ok_or(NativeError::Closed("server"))?;
        server
            .reload_postgres_security_with_context(config, context.clone())
            .await
            .map_err(|error| match error.downcast::<EngineError>() {
                Ok(error) => NativeError::Engine(error),
                Err(error) => listener_error(error),
            })
    }
}

async fn controlled<T>(
    context: &RequestContext,
    operation: impl Future<Output = NativeResult<T>>,
) -> NativeResult<T> {
    let cancellation = context.cancellation_token();
    // A Tokio timer may first yield Pending even for a just-expired instant.
    // Do not let an already-ready operation win in that registration window.
    if cancellation.is_cancelled() {
        return Err(EngineError::new(
            EngineErrorKind::Cancelled,
            "PostgreSQL security reload was cancelled before completion",
        )
        .into());
    }
    if context
        .deadline()
        .is_some_and(|deadline| deadline <= std::time::Instant::now())
    {
        return Err(EngineError::new(
            EngineErrorKind::DeadlineExceeded,
            "PostgreSQL security reload deadline elapsed",
        )
        .into());
    }
    let deadline = async {
        if let Some(deadline) = context.deadline() {
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(EngineError::new(
            EngineErrorKind::Cancelled, "PostgreSQL security reload was cancelled before completion",
        ).into()),
        _ = deadline => Err(EngineError::new(
            EngineErrorKind::DeadlineExceeded, "PostgreSQL security reload deadline elapsed",
        ).into()),
        result = operation => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn queued_reload_deadline_does_not_wait_for_the_handle_mutex() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let data = tempfile::tempdir().unwrap();
        let db = runtime
            .block_on(
                briskdb::BriskDb::builder(data.path())
                    .with_shard_count(2)
                    .open(),
            )
            .unwrap();
        let server = runtime
            .block_on(briskdb::server::AttachedServer::start(
                &db,
                briskdb::server::ListenerConfig {
                    http_listen: "127.0.0.1:0".parse().unwrap(),
                    admin_listen: None,
                    postgres_listen: None,
                },
            ))
            .unwrap();
        let addresses = server.addresses();
        let shared = std::sync::Arc::new(ServerShared {
            server: std::sync::Mutex::new(Some(server)),
            runtime: std::sync::Arc::new(super::super::RuntimeOwner { runtime }),
            addresses,
        });
        let held = shared.server.lock().unwrap();
        let worker_shared = shared.clone();
        let (done, finished) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let config = PostgresSecurityConfig::new(
                "unused.crt",
                "unused.key",
                "briskdb",
                "unused.password",
            )
            .unwrap();
            let context = RequestContext::new()
                .with_timeout(Duration::from_millis(10))
                .unwrap();
            let result = worker_shared.reload_security_native(config, context);
            done.send(matches!(result, Err(NativeError::Engine(error)) if error.kind() == EngineErrorKind::DeadlineExceeded)).unwrap();
        });
        let result = finished.recv_timeout(Duration::from_secs(5));
        drop(held);
        worker.join().unwrap();
        assert!(result.unwrap());
        shared.close_native().unwrap();
        shared.runtime.runtime.block_on(db.close()).unwrap();
    }

    #[tokio::test]
    async fn pre_cancelled_and_expired_requests_never_poll_publication() {
        let cancelled = RequestContext::new();
        cancelled.cancellation_token().cancel();
        for (context, kind) in [
            (cancelled, EngineErrorKind::Cancelled),
            (
                RequestContext::new().with_deadline(std::time::Instant::now()),
                EngineErrorKind::DeadlineExceeded,
            ),
        ] {
            let polled = AtomicBool::new(false);
            let result = controlled(&context, async {
                polled.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await;
            assert!(matches!(result, Err(NativeError::Engine(error)) if error.kind() == kind));
            assert!(!polled.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn cancellation_and_timeout_drop_pending_work_without_publication() {
        for cancel in [true, false] {
            let context = if cancel {
                RequestContext::new()
            } else {
                RequestContext::new()
                    .with_timeout(Duration::from_millis(10))
                    .unwrap()
            };
            let (started, ready) = tokio::sync::oneshot::channel();
            let (release, wait) = tokio::sync::oneshot::channel::<()>();
            let token = context.cancellation_token();
            let operation = controlled(&context, async {
                started.send(()).unwrap();
                wait.await.unwrap();
                Ok(())
            });
            let trigger = async {
                ready.await.unwrap();
                if cancel {
                    token.cancel();
                }
            };
            let (result, ()) = tokio::join!(operation, trigger);
            let expected = if cancel {
                EngineErrorKind::Cancelled
            } else {
                EngineErrorKind::DeadlineExceeded
            };
            assert!(matches!(result, Err(NativeError::Engine(error)) if error.kind() == expected));
            assert!(release.send(()).is_err());
        }
        assert_eq!(
            controlled(&RequestContext::new(), async { Ok(17) })
                .await
                .unwrap(),
            17
        );
    }
}
