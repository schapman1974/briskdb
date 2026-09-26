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
                "command",
                "connection_id",
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
