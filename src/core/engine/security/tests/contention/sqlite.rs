use super::*;
use rusqlite::Connection;

#[tokio::test]
async fn authenticated_startup_sqlite_obeys_policy_and_restores_retained_handle() {
    let (root, engine) = configured(ContentionPolicy::fail_fast()).await;
    engine.shutdown().await.unwrap();
    drop(engine);
    let blocker = Connection::open(root.path().join(security_root::FILE_NAME)).unwrap();
    blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let options =
        EngineOptions::default().with_contention_policy(Some(ContentionPolicy::fail_fast()));
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        Engine::open_authenticated(root.path(), 2, options),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    blocker.execute_batch("ROLLBACK").unwrap();
    let reopened = Engine::open_authenticated(root.path(), 2, options)
        .await
        .unwrap();
    reopened.begin_authentication(user()).await.unwrap();
    // Reusing the retained store must select each new call's budget, not the
    // exhausted startup scope left on the previous blocking worker.
    blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        reopened.begin_authentication(user()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    assert_eq!(reopened.contention_statistics().exhausted_budgets(), 1);
    blocker.execute_batch("ROLLBACK").unwrap();
}

#[tokio::test]
async fn sqlite_fail_fast_refresh_preserves_fail_closed_authority() {
    let (root, engine) = configured(ContentionPolicy::fail_fast()).await;
    let blocker = Connection::open(root.path().join(security_root::FILE_NAME)).unwrap();
    blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let error = tokio::time::timeout(Duration::from_secs(1), engine.begin_authentication(user()))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    assert_eq!(engine.contention_statistics().exhausted_budgets(), 1);
    assert!(
        engine
            .security_authority()
            .unwrap()
            .lock()
            .unwrap()
            .is_fenced()
    );
    blocker.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        engine
            .begin_authentication(user())
            .await
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
    engine.shutdown().await.unwrap();
    drop(engine);
    let reopened = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    reopened.begin_authentication(user()).await.unwrap();
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn authority_and_sqlite_waits_share_one_budget() {
    let (root, engine) = configured(backoff(1)).await;
    let blocker = Connection::open(root.path().join(security_root::FILE_NAME)).unwrap();
    blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let held = HeldAuthority::new(&engine);
    let worker = engine.clone();
    let call = tokio::spawn(async move { worker.begin_authentication(user()).await });
    wait_for_retry(&engine, 1).await;
    drop(held);
    let error = tokio::time::timeout(Duration::from_secs(2), call)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    assert_eq!(engine.contention_statistics().retries_scheduled(), 1);
    assert_eq!(engine.contention_statistics().exhausted_budgets(), 1);
    blocker.execute_batch("ROLLBACK").unwrap();
}

#[tokio::test]
async fn sqlite_release_during_backoff_allows_one_successful_refresh() {
    let (root, engine) = configured(backoff(2)).await;
    let blocker = Connection::open(root.path().join(security_root::FILE_NAME)).unwrap();
    blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let worker = engine.clone();
    let call = tokio::spawn(async move { worker.begin_authentication(user()).await });
    wait_for_retry(&engine, 1).await;
    blocker.execute_batch("ROLLBACK").unwrap();
    tokio::time::timeout(Duration::from_secs(2), call)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(engine.contention_statistics().retries_scheduled(), 1);
    assert_eq!(engine.contention_statistics().exhausted_budgets(), 0);
    assert!(
        !engine
            .security_authority()
            .unwrap()
            .lock()
            .unwrap()
            .is_fenced()
    );
}

#[tokio::test]
async fn contended_security_replace_never_replays_edit_or_changes_revision() {
    let (root, engine) = configured(ContentionPolicy::fail_fast()).await;
    let path = root.path().join(security_root::FILE_NAME);
    let before = fs::read(&path).unwrap();
    let revision = engine
        .security_authority()
        .unwrap()
        .lock()
        .unwrap()
        .revision();
    let blocker = Connection::open(&path).unwrap();
    // A reserved lock admits the initial read, but excludes the later write.
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        engine.update_security_catalog(move |catalog| {
            observed.fetch_add(1, Ordering::SeqCst);
            catalog.create_role(
                SecurityName::new("test", "must-not-commit")?,
                Policy::default(),
            )
        }),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let authority = engine.security_authority().unwrap();
    assert!(authority.lock().unwrap().is_fenced());
    assert_eq!(authority.lock().unwrap().revision(), revision);
    blocker.execute_batch("ROLLBACK").unwrap();
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(engine.contention_statistics().exhausted_budgets(), 1);
}

#[tokio::test]
async fn cancellation_and_deadline_stop_sqlite_waits_without_cached_authorization() {
    for cancelled in [true, false] {
        let (root, engine) = configured(backoff(10)).await;
        let blocker = Connection::open(root.path().join(security_root::FILE_NAME)).unwrap();
        blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
        let token = CancellationToken::new();
        let context = if cancelled {
            RequestContext::new().with_cancellation_token(token.clone())
        } else {
            RequestContext::new().with_deadline(Instant::now() + Duration::from_millis(200))
        };
        let worker = engine.clone();
        let call = tokio::spawn(async move {
            worker
                .security_call_with_context(context, |authority| authority.begin_scram(&user()))
                .await
        });
        wait_for_retry(&engine, 1).await;
        if cancelled {
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
        assert_eq!(engine.contention_statistics().exhausted_budgets(), 0);
        assert!(
            engine
                .security_authority()
                .unwrap()
                .lock()
                .unwrap()
                .is_fenced()
        );
        blocker.execute_batch("ROLLBACK").unwrap();
    }
}
