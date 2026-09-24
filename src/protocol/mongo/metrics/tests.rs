use super::*;

fn doc(fields: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}

#[test]
fn metrics_classify_only_fixed_names_codes_and_outcomes_without_payloads() {
    let metrics = Arc::new(Metrics::default());
    for kind in MongoCommandKind::ALL {
        assert_eq!(MongoCommandKind::classify(kind.name()), kind);
    }
    for name in ["isMaster", "ismaster"] {
        assert_eq!(MongoCommandKind::classify(name), MongoCommandKind::Hello);
    }
    assert_eq!(
        MongoCommandKind::classify("buildinfo"),
        MongoCommandKind::BuildInfo
    );
    let secret = "private-command-or-query-content";
    let failure = doc([
        ("ok", BsonValue::Double(0.0)),
        ("code", BsonValue::Int32(59)),
        ("errmsg", BsonValue::from(secret)),
    ]);
    metrics
        .command(secret, Instant::now())
        .complete(&failure, false);
    let errors = [11000, 11000, 999999]
        .into_iter()
        .map(|code| {
            BsonValue::Document(doc([
                ("code", BsonValue::Int32(code)),
                ("errmsg", BsonValue::from(secret)),
            ]))
        })
        .collect();
    let partial = doc([
        ("ok", BsonValue::Double(1.0)),
        ("writeErrors", BsonValue::Array(errors)),
    ]);
    metrics
        .command("insert", Instant::now())
        .complete(&partial, true);
    metrics.command("update", Instant::now()).complete(
        &doc([
            ("ok", BsonValue::Int32(1)),
            (
                "writeConcernError",
                BsonValue::Document(doc([("code", BsonValue::Int64(50))])),
            ),
        ]),
        false,
    );
    metrics
        .command("ping", Instant::now())
        .complete(&doc([("ok", BsonValue::Int64(1))]), false);
    drop(metrics.command("find", Instant::now()));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.commands().len(), 22);
    assert_eq!(snapshot.error_codes().count(), 31);
    assert_eq!(snapshot.errors_with_code(59), Some(1));
    assert_eq!(snapshot.errors_with_code(11000), Some(2));
    assert_eq!(snapshot.errors_with_code(50), Some(1));
    assert_eq!(snapshot.errors_with_code(999999), None);
    assert_eq!(snapshot.other_error_codes, 1);
    assert_eq!(snapshot.write_errors, 3);
    assert_eq!(snapshot.command(MongoCommandKind::Insert).failed, 1);
    assert_eq!(
        snapshot
            .command(MongoCommandKind::Insert)
            .suppressed_responses,
        1
    );
    assert_eq!(snapshot.command(MongoCommandKind::Ping).failed, 0);
    assert_eq!(snapshot.command(MongoCommandKind::Find).aborted, 1);
    assert!(!format!("{snapshot:?}").contains(secret));
    for command in snapshot.commands() {
        assert_eq!(command.in_flight, 0);
        assert_eq!(command.started, command.completed + command.aborted);
        assert_eq!(command.latency_buckets.iter().sum::<u64>(), command.started);
        assert!(command.elapsed_micros >= command.max_elapsed_micros);
    }
}

#[test]
fn metrics_saturate_totals_and_use_disjoint_latency_and_transport_buckets() {
    let counter = AtomicU64::new(u64::MAX - 1);
    add(&counter, 1);
    add(&counter, 3);
    assert_eq!(get(&counter), u64::MAX);
    assert_eq!(latency_bucket(0), 0);
    for (i, bound) in MONGO_LATENCY_UPPER_BOUNDS_MICROS.into_iter().enumerate() {
        assert_eq!(latency_bucket(bound), i);
        assert_eq!(latency_bucket(bound + 1), i + 1);
    }
    assert_eq!(latency_bucket(u64::MAX), 7);
    let metrics = Metrics::default();
    for kind in [
        io::ErrorKind::InvalidData,
        io::ErrorKind::UnexpectedEof,
        io::ErrorKind::TimedOut,
        io::ErrorKind::BrokenPipe,
    ] {
        metrics.connection_error(kind);
    }
    metrics.accept_failed();
    metrics.task_failed();
    metrics.response_rejected();
    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot.transport_failures,
        MongoTransportFailures {
            malformed: 1,
            truncated: 1,
            timed_out: 1,
            io: 1
        }
    );
    assert_eq!(
        (
            snapshot.accept_failures,
            snapshot.connection_task_failures,
            snapshot.response_limit_rejections
        ),
        (1, 1, 1)
    );
}

#[test]
fn metrics_guards_drain_concurrent_connections_and_aborted_commands_without_loss() {
    let metrics = Arc::new(Metrics::default());
    let barrier = Arc::new(std::sync::Barrier::new(9));
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let metrics = Arc::clone(&metrics);
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                metrics.accepted();
                let _connection = metrics.admit();
                barrier.wait();
                let body = doc([("ok", BsonValue::Double(1.0))]);
                for n in 0..250 {
                    let command = metrics.command("ping", Instant::now());
                    if n % 5 != 0 {
                        command.complete(&body, false);
                    }
                }
            });
        }
        barrier.wait();
    });
    let snapshot = metrics.snapshot();
    assert_eq!(
        (
            snapshot.accepted_connections,
            snapshot.admitted_connections,
            snapshot.closed_connections,
            snapshot.peak_connections
        ),
        (8, 8, 8, 8)
    );
    assert_eq!(snapshot.active_connections, 0);
    let ping = snapshot.command(MongoCommandKind::Ping);
    assert_eq!(
        (ping.started, ping.completed, ping.aborted, ping.in_flight),
        (2000, 1600, 400, 0)
    );
    assert_eq!(ping.latency_buckets.iter().sum::<u64>(), 2000);
}

#[test]
fn metrics_guards_release_live_gauges_during_unwinding() {
    let metrics = Arc::new(Metrics::default());
    let failed = std::panic::catch_unwind(|| {
        metrics.accepted();
        let _connection = metrics.admit();
        let _command = metrics.command("update", Instant::now());
        panic!("injected command unwind");
    });
    assert!(failed.is_err());
    let snapshot = metrics.snapshot();
    assert_eq!(
        (snapshot.active_connections, snapshot.closed_connections),
        (0, 1)
    );
    let update = snapshot.command(MongoCommandKind::Update);
    assert_eq!(
        (
            update.started,
            update.in_flight,
            update.completed,
            update.aborted
        ),
        (1, 0, 0, 1)
    );
    assert_eq!(update.latency_buckets.iter().sum::<u64>(), 1);
}
