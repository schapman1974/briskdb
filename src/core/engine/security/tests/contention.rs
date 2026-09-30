use super::*;
use crate::core::{
    ContentionJitter, ContentionPolicy,
    security_catalog::{RoleInfoRequest, UserInfoRequest},
    user_management::UserManagementCommand,
};
use std::sync::{atomic::AtomicUsize, mpsc};

mod sqlite;

struct HeldAuthority {
    release: mpsc::Sender<()>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl HeldAuthority {
    fn new(engine: &Engine) -> Self {
        let authority = engine.security_authority().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let _guard = authority.lock().unwrap();
            ready_tx.send(()).unwrap();
            let _ = released.recv_timeout(Duration::from_secs(10));
        });
        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        Self {
            release,
            worker: Some(worker),
        }
    }
}

impl Drop for HeldAuthority {
    fn drop(&mut self) {
        let _ = self.release.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn backoff(retries: u32) -> ContentionPolicy {
    ContentionPolicy::new(
        Duration::from_millis(500),
        Duration::from_millis(500),
        1,
        ContentionJitter::None,
        retries,
        Duration::from_secs(5),
    )
    .unwrap()
}

async fn configured(policy: ContentionPolicy) -> (tempfile::TempDir, Engine) {
    let (root, engine) = secure(&[Action::ConnectDatabase, Action::ReadData]).await;
    engine.shutdown().await.unwrap();
    drop(engine);
    // Other parallel tests can briefly own the process-wide registry even for
    // unrelated roots. Establish this fixture before testing its deliberately
    // contended authority/SQLite operation; do not retry that operation or
    // change the fail-fast policy/counters of the successfully opened engine.
    let started = std::time::Instant::now();
    let engine = loop {
        let result = Engine::open_authenticated(
            root.path(),
            2,
            EngineOptions::default().with_contention_policy(Some(policy)),
        )
        .await;
        if result.as_ref().is_err_and(|error| {
            fixture_registry_is_busy(error) && started.elapsed() < Duration::from_secs(5)
        }) {
            tokio::time::sleep(Duration::from_millis(1)).await;
            continue;
        }
        break result.unwrap();
    };
    (root, engine)
}

fn fixture_registry_is_busy(error: &EngineError) -> bool {
    error.kind() == EngineErrorKind::Busy
        && error.diagnostic() == "root schema coordination registry is busy"
}

#[test]
fn fixture_retry_does_not_hide_security_or_storage_failures() {
    assert!(fixture_registry_is_busy(&EngineError::new(
        EngineErrorKind::Busy,
        "root schema coordination registry is busy",
    )));
    for (kind, diagnostic) in [
        (EngineErrorKind::Busy, "security authority is busy"),
        (EngineErrorKind::Busy, "database is locked"),
        (
            EngineErrorKind::DataCorruption,
            "root schema coordination registry is busy",
        ),
        (
            EngineErrorKind::DeadlineExceeded,
            "root schema coordination registry is busy",
        ),
    ] {
        assert!(!fixture_registry_is_busy(&EngineError::new(
            kind, diagnostic
        )));
    }
}

async fn wait_for_retry(engine: &Engine, count: u64) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while engine.contention_statistics().retries_scheduled() < count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn fail_fast_authority_lock_does_not_run_or_replay_work() {
    let (_root, engine) = configured(ContentionPolicy::fail_fast()).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let held = HeldAuthority::new(&engine);
    let observed = Arc::clone(&calls);
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        engine.update_security_catalog(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(engine.contention_statistics().exhausted_budgets(), 1);
    assert_eq!(engine.contention_statistics().retries_scheduled(), 0);
    drop(held);
    let observed = Arc::clone(&calls);
    engine
        .update_security_catalog(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn bounded_authority_retry_can_acquire_released_lock_and_runs_work_once() {
    let (_root, engine) = configured(backoff(2)).await;
    let held = HeldAuthority::new(&engine);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let worker = engine.clone();
    let call = tokio::spawn(async move {
        worker
            .update_security_catalog(move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
    });
    wait_for_retry(&engine, 1).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(held);
    tokio::time::timeout(Duration::from_secs(2), call)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.contention_statistics().retries_scheduled(), 1);
    assert_eq!(engine.contention_statistics().exhausted_budgets(), 0);
}

#[tokio::test]
async fn security_commands_share_session_and_authority_wait_budget() {
    let (root, engine) = configured(backoff(1)).await;
    let actor = Arc::new(login(&engine).await);
    let before = fs::read(root.path().join("security.sqlite")).unwrap();
    let mut cases = vec![0, 1, 2, 4, 5, 6, 7];
    if cfg!(feature = "documents") {
        cases.push(3);
    }
    for (index, case) in cases.into_iter().enumerate() {
        let held = HeldAuthority::new(&engine);
        let session_guard = Arc::clone(&actor.inner).lock_owned().await;
        let client = engine.clone();
        let session = Arc::clone(&actor);
        let call = tokio::spawn(async move {
            match case {
                0 => client
                    .user_info(
                        &session,
                        RequestContext::new(),
                        UserInfoRequest::names([user()]).unwrap(),
                    )
                    .await
                    .map(|_| ()),
                1 => client
                    .role_info(
                        &session,
                        RequestContext::new(),
                        RoleInfoRequest::names([role()]).unwrap(),
                    )
                    .await
                    .map(|_| ()),
                2 => {
                    client
                        .execute_user_management(
                            &session,
                            RequestContext::new(),
                            UserManagementCommand::drop_user(user()),
                        )
                        .await
                }
                4 => {
                    client
                        .drop_role(&session, RequestContext::new(), role())
                        .await
                }
                5 => {
                    client
                        .create_document_role(
                            &session,
                            RequestContext::new(),
                            role(),
                            Policy::default(),
                        )
                        .await
                }
                #[cfg(feature = "documents")]
                3 => {
                    use crate::document::*;
                    let command = DocumentCommand::Count(DocumentCountRequest::new(
                        DocumentNamespace::new("app", "items").unwrap(),
                        DocumentFilter::default(),
                        DocumentReadOptions::default(),
                    ));
                    client
                        .execute_document(
                            &session,
                            DocumentRequest::new(
                                DocumentRequestId::new([1; 16]).unwrap(),
                                RequestContext::new(),
                                command,
                            ),
                        )
                        .await
                        .map(|_| ())
                }
                6 => {
                    client
                        .grant_document_role_privileges(
                            &session,
                            RequestContext::new(),
                            role(),
                            Policy::default(),
                        )
                        .await
                }
                7 => {
                    client
                        .revoke_document_role_privileges(
                            &session,
                            RequestContext::new(),
                            role(),
                            Policy::default(),
                        )
                        .await
                }
                _ => unreachable!(),
            }
        });
        wait_for_retry(&engine, index as u64 + 1).await;
        drop(session_guard); // Admission completes during its first window.
        let error = tokio::time::timeout(Duration::from_secs(2), call)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Busy, "case {case}");
        assert_eq!(
            engine.contention_statistics().retries_scheduled(),
            index as u64 + 1
        );
        assert_eq!(
            engine.contention_statistics().exhausted_budgets(),
            index as u64 + 1
        );
        drop(held);
    }
    assert_eq!(
        before,
        fs::read(root.path().join("security.sqlite")).unwrap()
    );
    assert_eq!(
        engine
            .user_info(
                &actor,
                RequestContext::new(),
                UserInfoRequest::names([user()]).unwrap()
            )
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn parent_cancellation_and_deadline_stop_authority_waits_before_work() {
    for cancelled in [true, false] {
        let (_root, engine) = configured(backoff(10)).await;
        let held = HeldAuthority::new(&engine);
        let token = CancellationToken::new();
        let context = if cancelled {
            RequestContext::new().with_cancellation_token(token.clone())
        } else {
            RequestContext::new().with_deadline(Instant::now() + Duration::from_millis(20))
        };
        let parent = engine.operation_lifecycle(context).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let worker = engine.clone();
        let call = tokio::spawn(async move {
            worker
                .security_call_from_parent(&parent, move |_| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
        });
        if cancelled {
            wait_for_retry(&engine, 1).await;
            token.cancel();
        }
        let error = tokio::time::timeout(Duration::from_secs(2), call)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.kind(),
            if cancelled {
                EngineErrorKind::Cancelled
            } else {
                EngineErrorKind::DeadlineExceeded
            }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.contention_statistics().exhausted_budgets(), 0);
        drop(held);
    }
}

#[tokio::test]
async fn child_completion_keeps_parent_phase_independent_and_started_work_runs_once() {
    let (_root, engine) = configured(backoff(10)).await;
    let parent = engine.operation_lifecycle(RequestContext::new()).unwrap();
    engine
        .security_call_from_parent(&parent, |_| Ok(()))
        .await
        .unwrap();
    parent.control.request_cancel(CancellationReason::Cancelled);
    assert_eq!(parent.control.reason(), Some(CancellationReason::Cancelled));

    let token = CancellationToken::new();
    let parent = engine
        .operation_lifecycle(RequestContext::new().with_cancellation_token(token.clone()))
        .unwrap();
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, released) = mpsc::channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let worker = engine.clone();
    let committed_role = SecurityName::new("test", "committed-once").unwrap();
    let role_to_write = committed_role.clone();
    let call = tokio::spawn(async move {
        worker
            .security_call_from_parent(&parent, move |authority| {
                observed.fetch_add(1, Ordering::SeqCst);
                started.send(()).unwrap();
                released.recv_timeout(Duration::from_secs(2)).unwrap();
                authority.update(|catalog| catalog.create_role(role_to_write, Policy::default()))
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    token.cancel();
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), call)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.contention_statistics().exhausted_budgets(), 0);
    // A known successful edit is not replayed or reclassified as cancellation.
    let error = engine
        .update_security_catalog(move |catalog| {
            catalog.create_role(committed_role, Policy::default())
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::UniqueViolation);
}
