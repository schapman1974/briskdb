use super::super::{MongoCommandKind, metrics::Metrics};
use crate::document::{BsonDocument, BsonValue};
use std::{sync::Arc, time::Instant};

mod capture {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/mongo_trace_capture.rs"
    ));
}
use capture::Capture;

fn doc(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(entries).unwrap()
}

#[test]
fn first_unsubscribed_request_cannot_poison_host_tracing() {
    const CHILD: &str = "BRISKDB_TRACE_FIRST_CALL_CHILD";
    const VERIFIED: &str = "BRISKDB_TRACE_FIRST_CALL_VERIFIED";
    if std::env::var_os(CHILD).as_deref() != Some(std::ffi::OsStr::new("1")) {
        // Callsite interest is process-global. Other tests registering subscribers
        // or priming the same callsites would hide this startup ordering defect.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "protocol::mongo::events::tests::first_unsubscribed_request_cannot_poison_host_tracing",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains(VERIFIED),
            "the child regression must actually execute"
        );
        return;
    }

    let capture = Capture::default();
    // Register exactly one subscriber, but do not install it on this thread yet.
    let dispatch = tracing::Dispatch::new(capture.clone());
    let metrics = Arc::new(Metrics::default());
    assert!(tracing::dispatcher::get_default(|dispatch| {
        dispatch.is::<tracing::subscriber::NoSubscriber>()
    }));
    metrics
        .command("ping", Instant::now())
        .with_correlation(1, 1, 1)
        .complete(&doc([("ok", BsonValue::Int32(1))]), false);

    let guard = tracing::dispatcher::with_default(&dispatch, || {
        metrics
            .command("find", Instant::now())
            .with_correlation(2, 1, 1)
    });
    let worker_metrics = Arc::clone(&metrics);
    std::thread::spawn(move || {
        // A second unsubscribed command must not steal or suppress the host's
        // completion, which deliberately runs off the originating thread.
        worker_metrics
            .command("ping", Instant::now())
            .with_correlation(1, 2, 2)
            .complete(&doc([("ok", BsonValue::Int32(1))]), false);
        guard.complete(&doc([("ok", BsonValue::Int32(1))]), false);
    })
    .join()
    .unwrap();
    tracing::dispatcher::with_default(&dispatch, || {
        drop(
            metrics
                .command("find", Instant::now())
                .with_correlation(2, 2, 2),
        );
    });
    let captured = capture.0.lock().unwrap();
    assert_eq!(
        (
            captured.spans.len(),
            captured.events.len(),
            captured.live.len()
        ),
        (2, 2, 0)
    );
    assert_eq!(captured.events[0]["outcome"], "completed");
    assert_eq!(captured.events[1]["outcome"], "aborted");
    for (index, event) in captured.events.iter().enumerate() {
        assert_eq!(event["connection_id"], "2");
        assert_eq!(event["sequence"], (index + 1).to_string());
    }
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.command(MongoCommandKind::Ping).completed, 2);
    assert_eq!(snapshot.command(MongoCommandKind::Find).completed, 1);
    assert_eq!(snapshot.command(MongoCommandKind::Find).aborted, 1);
    println!("{VERIFIED}");
}

#[test]
fn command_events_keep_only_bounded_correlation_and_final_outcomes() {
    let capture = Capture::default();
    let metrics = Arc::new(Metrics::default());
    let secret = "secret-namespace-query-comment-credential-diagnostic";
    tracing::subscriber::with_default(capture.clone(), || {
        metrics
            .command("ping", Instant::now())
            .with_correlation(7, -91, 1)
            .complete(&doc([("ok", BsonValue::Int32(1))]), false);
        metrics
            .command(secret, Instant::now())
            .with_correlation(7, -91, 2)
            .complete(
                &doc([
                    ("ok", BsonValue::Double(0.0)),
                    ("code", BsonValue::Int32(59)),
                    ("errmsg", BsonValue::from(secret)),
                ]),
                false,
            );
        let error = doc([
            ("code", BsonValue::Int32(11000)),
            ("errmsg", BsonValue::from(secret)),
        ]);
        metrics
            .command("insert", Instant::now())
            .with_correlation(7, i32::MAX, 3)
            .complete(
                &doc([
                    ("ok", BsonValue::Double(1.0)),
                    (
                        "writeErrors",
                        BsonValue::Array(vec![
                            BsonValue::Document(error.clone()),
                            BsonValue::Document(error),
                        ]),
                    ),
                ]),
                true,
            );
        metrics
            .command("update", Instant::now())
            .with_correlation(8, 2, 1)
            .complete(
                &doc([
                    ("ok", BsonValue::Double(1.0)),
                    (
                        "writeConcernError",
                        BsonValue::Document(doc([
                            ("code", BsonValue::Int32(999999)),
                            ("errmsg", BsonValue::from(secret)),
                        ])),
                    ),
                ]),
                false,
            );
        metrics
            .command("insert", Instant::now())
            .with_correlation(8, 3, 2)
            .complete(
                &doc([
                    ("ok", BsonValue::Double(1.0)),
                    ("writeErrors", BsonValue::Array(vec![BsonValue::Null])),
                ]),
                false,
            );
        drop(
            metrics
                .command("find", Instant::now())
                .with_correlation(8, 4, 3),
        );
    });
    let state = capture.0.lock().unwrap();
    assert_eq!(
        (state.events.len(), state.spans.len(), state.live.len()),
        (6, 6, 0)
    );
    assert!(!format!("{:?}{:?}", state.events, state.spans).contains(secret));
    for event in &state.events {
        assert_eq!(
            event.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "audit_user",
                "authentication",
                "command",
                "connection_id",
                "credential_generation",
                "elapsed_micros",
                "error_category",
                "error_code",
                "message",
                "outcome",
                "response_suppressed",
                "sequence",
                "wire_request_id",
                "write_errors"
            ]
        );
        assert!(event["elapsed_micros"].parse::<u64>().is_ok());
        assert_eq!(event["authentication"], "anonymous");
        assert_eq!(event["audit_user"], "");
        assert_eq!(event["credential_generation"], "0");
    }
    assert_eq!(state.events[0]["outcome"], "completed");
    assert_eq!(state.events[0]["wire_request_id"], "-91");
    assert_eq!(state.events[0]["error_category"], "none");
    assert_eq!(state.events[1]["command"], "other");
    assert_eq!(state.events[1]["error_code"], "59");
    assert_eq!(state.events[1]["sequence"], "2");
    assert_eq!(state.events[2]["outcome"], "failed");
    assert_eq!(state.events[2]["error_code"], "11000");
    assert_eq!(state.events[2]["write_errors"], "2");
    assert_eq!(state.events[2]["response_suppressed"], "true");
    for index in [3, 4] {
        assert_eq!(state.events[index]["error_category"], "other");
        assert_eq!(state.events[index]["error_code"], "0");
    }
    assert_eq!(state.events[5]["outcome"], "aborted");
    assert_eq!(
        metrics.snapshot().command(MongoCommandKind::Find).in_flight,
        0
    );
}

#[cfg(feature = "auth-scram")]
#[test]
fn authentication_events_follow_session_installation_without_raw_identity_or_metric_labels() {
    use super::AuditContext;
    let capture = Capture::default();
    let metrics = Arc::new(Metrics::default());
    let identity = blake3::keyed_hash(&[7; 32], &41u64.to_le_bytes());
    let mut guard = tracing::subscriber::with_default(capture.clone(), || {
        let mut guard = metrics
            .command("saslContinue", Instant::now())
            .with_correlation(1, 1, 1);
        guard.authentication(AuditContext::unauthenticated());
        guard
    });
    // Final SASL success changes the established session before reply encoding.
    guard.authentication(AuditContext::authenticated(identity, 3));
    std::thread::spawn(move || guard.complete(&doc([("ok", BsonValue::Int32(1))]), false))
        .join()
        .unwrap();
    tracing::subscriber::with_default(capture.clone(), || {
        let mut guard = metrics
            .command("saslStart", Instant::now())
            .with_correlation(2, 1, 1);
        guard.authentication(AuditContext::unauthenticated());
        guard.complete(
            &doc([("ok", BsonValue::Int32(0)), ("code", BsonValue::Int32(18))]),
            false,
        );
        let mut guard = metrics
            .command("dropUser", Instant::now())
            .with_correlation(1, 2, 2);
        guard.authentication(AuditContext::authenticated(identity, 3));
        drop(guard);
    });
    let state = capture.0.lock().unwrap();
    for index in [0, 2] {
        assert_eq!(state.events[index]["authentication"], "authenticated");
        assert_eq!(
            state.events[index]["audit_user"],
            identity.to_hex().as_str()
        );
        assert_eq!(state.events[index]["credential_generation"], "3");
        assert_eq!(
            state.spans[index]["audit_user"],
            state.events[index]["audit_user"]
        );
    }
    assert_eq!(state.events[1]["authentication"], "unauthenticated");
    assert_eq!(state.events[1]["audit_user"], "");
    assert_eq!(state.events[1]["error_code"], "18");
    assert_eq!(state.events[1]["error_category"], "known");
    assert_eq!(state.events[2]["outcome"], "aborted");
    assert!(state.live.is_empty());
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.errors_with_code(18), Some(1));
    assert_eq!(
        snapshot.command(MongoCommandKind::SaslContinue).completed,
        1
    );
    assert_eq!(snapshot.command(MongoCommandKind::SaslStart).failed, 1);
    assert_eq!(snapshot.command(MongoCommandKind::DropUser).aborted, 1);
    assert!(!format!("{snapshot:?}").contains(identity.to_hex().as_str()));
}

#[test]
fn command_events_follow_the_host_dispatcher_across_worker_threads_and_unwinding() {
    let capture = Capture::default();
    let metrics = Arc::new(Metrics::default());
    let guard = tracing::subscriber::with_default(capture.clone(), || {
        metrics
            .command("find", Instant::now())
            .with_correlation(41, 1, 1)
    });
    // No thread-local or global subscriber is installed on this worker. The
    // command still completes in the dispatcher captured by its owner.
    std::thread::spawn(move || guard.complete(&doc([("ok", BsonValue::Int32(1))]), false))
        .join()
        .unwrap();
    let guard = tracing::subscriber::with_default(capture.clone(), || {
        metrics
            .command("find", Instant::now())
            .with_correlation(41, 1, 2)
    });
    let result = std::thread::spawn(move || {
        let _guard = guard;
        panic!("test-only unwind");
    })
    .join();
    assert!(result.is_err());
    let state = capture.0.lock().unwrap();
    assert_eq!(state.events.len(), 2);
    assert_eq!(state.events[0]["outcome"], "completed");
    assert_eq!(state.events[1]["outcome"], "aborted");
    assert_eq!(state.events[1]["sequence"], "2");
    assert!(state.live.is_empty());
    let snapshot = metrics.snapshot();
    let find = snapshot.command(MongoCommandKind::Find);
    assert_eq!(
        (find.started, find.completed, find.aborted, find.in_flight),
        (2, 1, 1, 0)
    );
}
