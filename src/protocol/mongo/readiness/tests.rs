use super::*;

fn snapshot(lifecycle_state: EngineState, schema_state: SchemaState) -> MongoReadinessSnapshot {
    MongoReadinessSnapshot {
        listener: MongoListenerState::Running,
        engine: Some(ReadinessSnapshot {
            lifecycle_state,
            schema_state,
            schema_generation: 7,
            active_schema_operations: 19,
        }),
        document_support: DocumentSupport::Enabled,
        security: MongoSecurityMode::AnonymousLoopback,
    }
}

#[test]
fn reasons_are_exhaustive_fixed_codes_with_explicit_precedence() {
    use MongoReadinessReason as Reason;
    let ready = snapshot(EngineState::Running, SchemaState::Ready);
    assert!(ready.ready()); // Occupancy is not a promise of spare capacity.
    assert_eq!(ready.security.code(), "anonymous_loopback");
    for (state, reason, code) in [
        (
            SchemaState::Migrating,
            Reason::SchemaMigrating,
            "schema_migrating",
        ),
        (
            SchemaState::Pending,
            Reason::SchemaPending,
            "schema_pending",
        ),
        (
            SchemaState::Degraded,
            Reason::SchemaDegraded,
            "schema_degraded",
        ),
    ] {
        let status = snapshot(EngineState::Running, state);
        assert!(!status.ready());
        assert_eq!(status.reason(), Some(reason));
        assert_eq!(reason.code(), code);
    }
    for (state, reason, code) in [
        (
            EngineState::Draining,
            Reason::EngineDraining,
            "engine_draining",
        ),
        (
            EngineState::Stopped,
            Reason::EngineStopped,
            "engine_stopped",
        ),
    ] {
        assert_eq!(
            snapshot(state, SchemaState::Degraded).reason(),
            Some(reason)
        );
        assert_eq!(reason.code(), code);
    }
    let mut status = ready;
    status.engine = None;
    assert_eq!(status.reason(), Some(Reason::EngineUnavailable));
    assert_eq!(Reason::EngineUnavailable.code(), "engine_unavailable");
    status.document_support = DocumentSupport::Disabled;
    assert_eq!(status.reason(), Some(Reason::DocumentsDisabled));
    assert_eq!(Reason::DocumentsDisabled.code(), "documents_disabled");
    for (state, reason, code, state_code) in [
        (
            MongoListenerState::Closing,
            Reason::ListenerClosing,
            "listener_closing",
            "closing",
        ),
        (
            MongoListenerState::Closed,
            Reason::ListenerClosed,
            "listener_closed",
            "closed",
        ),
        (
            MongoListenerState::Failed,
            Reason::ListenerFailed,
            "listener_failed",
            "failed",
        ),
    ] {
        status.listener = state;
        assert_eq!(status.reason(), Some(reason));
        assert_eq!(reason.code(), code);
        assert_eq!(state.code(), state_code);
    }
    assert_eq!(MongoListenerState::Running.code(), "running");
}

#[test]
fn listener_health_preserves_terminal_states_and_marks_unpolled_or_unwound_failure() {
    for closing in [false, true] {
        for outcome in [None, Some(false), Some(true)] {
            let health = Arc::new(ListenerHealth::default());
            let guard = health.guard();
            assert_eq!(health.state(false), MongoListenerState::Running);
            assert_eq!(health.state(true), MongoListenerState::Closing);
            if closing {
                health.begin_close();
                assert_eq!(health.state(false), MongoListenerState::Closing);
            }
            if let Some(success) = outcome {
                guard.finish(success);
            } else {
                // An unpolled task drops its captured guard too.
                drop(guard);
            }
            let expected = if outcome == Some(true) {
                MongoListenerState::Closed
            } else {
                MongoListenerState::Failed
            };
            health.begin_close();
            assert_eq!(health.state(false), expected);
            assert_eq!(health.state(true), expected);
            assert_eq!(Arc::strong_count(&health), 1);
        }
    }
    let health = Arc::new(ListenerHealth::default());
    let guard = health.guard();
    assert!(
        std::panic::catch_unwind(move || {
            let _guard = guard;
            panic!("test-only listener panic");
        })
        .is_err()
    );
    assert_eq!(health.state(false), MongoListenerState::Failed);
}
