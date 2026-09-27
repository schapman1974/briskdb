//! Host-owned tracing: bounded scalar correlation, never request/response data.

use tracing::{Dispatch, Span};

use super::metrics::MongoCommandKind;

#[cfg(test)]
mod tests;

/// Established session context, not a current authorization decision or a
/// durable audit record. Only authentication can mint the opaque user label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AuditContext {
    state: &'static str,
    user: String,
    generation: u64,
}

impl Default for AuditContext {
    fn default() -> Self {
        Self {
            state: "anonymous",
            user: String::new(),
            generation: 0,
        }
    }
}

#[cfg(feature = "auth-scram")]
impl AuditContext {
    pub(super) fn unauthenticated() -> Self {
        Self {
            state: "unauthenticated",
            ..Self::default()
        }
    }

    pub(super) fn authenticated(user: blake3::Hash, generation: u64) -> Self {
        Self {
            state: "authenticated",
            user: user.to_hex().to_string(),
            generation,
        }
    }
}

pub(super) struct RequestTrace {
    dispatch: Dispatch,
    span: Span,
    connection_id: u64,
    wire_request_id: i32,
    sequence: u64,
    command: MongoCommandKind,
    outcome: &'static str,
    error_code: Option<i32>,
    has_error: bool,
    write_errors: u64,
    suppressed: bool,
    authentication: AuditContext,
}

impl RequestTrace {
    pub(super) fn new(
        command: MongoCommandKind,
        connection_id: u64,
        wire_request_id: i32,
        sequence: u64,
    ) -> Self {
        Self {
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            span: tracing::debug_span!(
                target: "briskdb::mongo", "mongo.command",
                connection_id, wire_request_id, sequence, command = command.name(),
                authentication = "anonymous", audit_user = "", credential_generation = 0u64
            ),
            connection_id,
            wire_request_id,
            sequence,
            command,
            outcome: "aborted",
            error_code: None,
            has_error: false,
            write_errors: 0,
            suppressed: false,
            authentication: AuditContext::default(),
        }
    }

    #[cfg(feature = "auth-scram")]
    pub(super) fn authentication(&mut self, context: AuditContext) {
        self.span.record("authentication", context.state);
        self.span.record("audit_user", context.user.as_str());
        self.span
            .record("credential_generation", context.generation);
        self.authentication = context;
    }

    /// `code` has already been classified against the fixed metrics vocabulary.
    /// Keep the first error's category, never arbitrary diagnostic text or codes.
    pub(super) fn error(&mut self, code: Option<i32>) {
        if !self.has_error {
            self.error_code = code;
            self.has_error = true;
        }
    }

    pub(super) fn complete(&mut self, failed: bool, write_errors: u64, suppressed: bool) {
        self.outcome = if failed { "failed" } else { "completed" };
        self.write_errors = write_errors;
        self.suppressed = suppressed;
    }

    pub(super) fn finish(self, elapsed_micros: u64) {
        // Reply encoding and guard drop may run on a different blocking worker.
        // Carry the host's dispatcher explicitly; never install a global logger.
        tracing::dispatcher::with_default(&self.dispatch, || {
            let _entered = self.span.enter();
            tracing::debug!(
                target: "briskdb::mongo",
                connection_id = self.connection_id,
                wire_request_id = self.wire_request_id,
                sequence = self.sequence,
                command = self.command.name(),
                authentication = self.authentication.state,
                audit_user = self.authentication.user.as_str(),
                credential_generation = self.authentication.generation,
                outcome = self.outcome,
                error_code = self.error_code.unwrap_or(0),
                error_category = if self.error_code.is_some() { "known" }
                    else if self.has_error { "other" } else { "none" },
                write_errors = self.write_errors,
                response_suppressed = self.suppressed,
                elapsed_micros,
                "Mongo command finished"
            );
        });
    }
}
