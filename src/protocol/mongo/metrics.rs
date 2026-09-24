//! Fixed-cardinality, payload-free listener counters. No query, namespace,
//! connection identity, arbitrary command name or diagnostic becomes a label.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use crate::document::{BsonDocument, BsonValue};

const COMMANDS: usize = 22;
#[cfg(test)]
mod tests;
const ERROR_CODES: [i32; 31] = [
    1, 2, 9, 13, 14, 20, 26, 27, 28, 40, 43, 48, 50, 52, 54, 56, 59, 66, 72, 73, 85, 86, 91, 112,
    115, 197, 224, 237, 10334, 11000, 11601,
];

/// Inclusive histogram bounds in microseconds; the eighth bucket is overflow.
/// Buckets are disjoint, not cumulative. Both completed and aborted admitted
/// commands contribute. Timing starts at a complete frame and ends after reply
/// construction (or abort), excluding socket framing and reply delivery.
pub const MONGO_LATENCY_UPPER_BOUNDS_MICROS: [u64; 7] = [
    100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 60_000_000,
];

/// Closed label vocabulary. Unknown command names share `Other`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[repr(usize)]
pub enum MongoCommandKind {
    Hello,
    Ping,
    BuildInfo,
    Insert,
    Find,
    GetMore,
    KillCursors,
    Count,
    Distinct,
    Aggregate,
    Update,
    Delete,
    FindAndModify,
    ListDatabases,
    ListCollections,
    ListIndexes,
    Create,
    CreateIndexes,
    Drop,
    DropIndexes,
    DropDatabase,
    Other,
}

impl MongoCommandKind {
    const ALL: [Self; COMMANDS] = [
        Self::Hello,
        Self::Ping,
        Self::BuildInfo,
        Self::Insert,
        Self::Find,
        Self::GetMore,
        Self::KillCursors,
        Self::Count,
        Self::Distinct,
        Self::Aggregate,
        Self::Update,
        Self::Delete,
        Self::FindAndModify,
        Self::ListDatabases,
        Self::ListCollections,
        Self::ListIndexes,
        Self::Create,
        Self::CreateIndexes,
        Self::Drop,
        Self::DropIndexes,
        Self::DropDatabase,
        Self::Other,
    ];

    pub(super) fn classify(name: &str) -> Self {
        match name {
            "hello" | "ismaster" | "isMaster" => Self::Hello,
            "ping" => Self::Ping,
            "buildInfo" | "buildinfo" => Self::BuildInfo,
            "insert" => Self::Insert,
            "find" => Self::Find,
            "getMore" => Self::GetMore,
            "killCursors" => Self::KillCursors,
            "count" => Self::Count,
            "distinct" => Self::Distinct,
            "aggregate" => Self::Aggregate,
            "update" => Self::Update,
            "delete" => Self::Delete,
            "findAndModify" => Self::FindAndModify,
            "listDatabases" => Self::ListDatabases,
            "listCollections" => Self::ListCollections,
            "listIndexes" => Self::ListIndexes,
            "create" => Self::Create,
            "createIndexes" => Self::CreateIndexes,
            "drop" => Self::Drop,
            "dropIndexes" => Self::DropIndexes,
            "dropDatabase" => Self::DropDatabase,
            _ => Self::Other,
        }
    }

    /// Stable, non-user-controlled exporter label.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Hello => "hello",
            Self::Ping => "ping",
            Self::BuildInfo => "buildInfo",
            Self::Insert => "insert",
            Self::Find => "find",
            Self::GetMore => "getMore",
            Self::KillCursors => "killCursors",
            Self::Count => "count",
            Self::Distinct => "distinct",
            Self::Aggregate => "aggregate",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::FindAndModify => "findAndModify",
            Self::ListDatabases => "listDatabases",
            Self::ListCollections => "listCollections",
            Self::ListIndexes => "listIndexes",
            Self::Create => "create",
            Self::CreateIndexes => "createIndexes",
            Self::Drop => "drop",
            Self::DropIndexes => "dropIndexes",
            Self::DropDatabase => "dropDatabase",
            Self::Other => "other",
        }
    }
}

/// Counters for one fixed command family. A completed command has produced an
/// encoded reply, or its deliberately suppressed one-way outcome. Completion
/// does not certify delivery, a successful mutation, or global atomicity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MongoCommandMetrics {
    pub kind: MongoCommandKind,
    /// Decoded requests whose preparation returned, including validation errors.
    pub started: u64,
    /// Awaiting execution/reply preparation; raw parser work is not in this gauge.
    pub in_flight: u64,
    pub completed: u64,
    /// Completed outcomes containing top-level, write, or write-concern errors.
    pub failed: u64,
    /// Admitted commands dropped before completion, including task unwinding.
    pub aborted: u64,
    pub suppressed_responses: u64,
    pub elapsed_micros: u64,
    pub max_elapsed_micros: u64,
    pub latency_buckets: [u64; 8],
}

/// Fatal connection errors, separate from Mongo-shaped command errors.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MongoTransportFailures {
    pub malformed: u64,
    pub truncated: u64,
    pub timed_out: u64,
    /// Other socket/parser I/O failures; no diagnostic text is retained.
    pub io: u64,
}

/// Listener-local cumulative counters. Concurrent fields are sampled separately,
/// not as a globally atomic snapshot; compare accounting identities after drain.
/// Totals saturate instead of wrapping. Live gauges are bounded by admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MongoMetricsSnapshot {
    pub accepted_connections: u64,
    pub admitted_connections: u64,
    pub rejected_connections: u64,
    pub active_connections: u64,
    /// Closed admitted connections; rejected sockets do not enter this count.
    pub closed_connections: u64,
    pub peak_connections: u64,
    pub accept_failures: u64,
    pub connection_task_failures: u64,
    pub transport_failures: MongoTransportFailures,
    pub write_errors: u64,
    pub response_limit_rejections: u64,
    commands: [MongoCommandMetrics; COMMANDS],
    error_codes: [u64; ERROR_CODES.len()],
    /// Error occurrences without a code in the fixed exporter vocabulary.
    pub other_error_codes: u64,
}

impl MongoMetricsSnapshot {
    pub fn commands(&self) -> &[MongoCommandMetrics] {
        &self.commands
    }
    pub fn command(&self, kind: MongoCommandKind) -> &MongoCommandMetrics {
        &self.commands[kind as usize]
    }
    /// Fixed known code/count pairs, including zero counts. No arbitrary label
    /// is allocated when a future error code is encountered.
    pub fn error_codes(&self) -> impl Iterator<Item = (i32, u64)> + '_ {
        ERROR_CODES.into_iter().zip(self.error_codes)
    }
    pub fn errors_with_code(&self, code: i32) -> Option<u64> {
        ERROR_CODES
            .iter()
            .position(|known| *known == code)
            .map(|index| self.error_codes[index])
    }
}

#[derive(Default)]
struct CommandCounters {
    started: AtomicU64,
    in_flight: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    aborted: AtomicU64,
    suppressed: AtomicU64,
    elapsed: AtomicU64,
    max_elapsed: AtomicU64,
    latency: [AtomicU64; 8],
}

#[derive(Default)]
pub(super) struct Metrics {
    accepted: AtomicU64,
    admitted: AtomicU64,
    rejected: AtomicU64,
    active: AtomicU64,
    closed: AtomicU64,
    peak: AtomicU64,
    accept_failures: AtomicU64,
    task_failures: AtomicU64,
    transport: [AtomicU64; 4],
    write_errors: AtomicU64,
    response_limits: AtomicU64,
    commands: [CommandCounters; COMMANDS],
    error_codes: [AtomicU64; 31],
    other_codes: AtomicU64,
}

fn add(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
        Some(old.saturating_add(amount))
    });
}
fn get(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

fn latency_bucket(micros: u64) -> usize {
    MONGO_LATENCY_UPPER_BOUNDS_MICROS
        .iter()
        .position(|bound| micros <= *bound)
        .unwrap_or(7)
}

impl Metrics {
    pub(super) fn snapshot(&self) -> MongoMetricsSnapshot {
        MongoMetricsSnapshot {
            accepted_connections: get(&self.accepted),
            admitted_connections: get(&self.admitted),
            rejected_connections: get(&self.rejected),
            active_connections: get(&self.active),
            closed_connections: get(&self.closed),
            peak_connections: get(&self.peak),
            accept_failures: get(&self.accept_failures),
            connection_task_failures: get(&self.task_failures),
            transport_failures: MongoTransportFailures {
                malformed: get(&self.transport[0]),
                truncated: get(&self.transport[1]),
                timed_out: get(&self.transport[2]),
                io: get(&self.transport[3]),
            },
            write_errors: get(&self.write_errors),
            response_limit_rejections: get(&self.response_limits),
            commands: std::array::from_fn(|i| {
                let c = &self.commands[i];
                MongoCommandMetrics {
                    kind: MongoCommandKind::ALL[i],
                    started: get(&c.started),
                    in_flight: get(&c.in_flight),
                    completed: get(&c.completed),
                    failed: get(&c.failed),
                    aborted: get(&c.aborted),
                    suppressed_responses: get(&c.suppressed),
                    elapsed_micros: get(&c.elapsed),
                    max_elapsed_micros: get(&c.max_elapsed),
                    latency_buckets: std::array::from_fn(|i| get(&c.latency[i])),
                }
            }),
            error_codes: std::array::from_fn(|i| get(&self.error_codes[i])),
            other_error_codes: get(&self.other_codes),
        }
    }

    pub(super) fn accepted(&self) {
        add(&self.accepted, 1);
    }
    pub(super) fn rejected(&self) {
        add(&self.rejected, 1);
    }
    pub(super) fn accept_failed(&self) {
        add(&self.accept_failures, 1);
    }
    pub(super) fn task_failed(&self) {
        add(&self.task_failures, 1);
    }
    pub(super) fn response_rejected(&self) {
        add(&self.response_limits, 1);
    }
    pub(super) fn connection_error(&self, kind: io::ErrorKind) {
        let i = match kind {
            io::ErrorKind::InvalidData => 0,
            io::ErrorKind::UnexpectedEof => 1,
            io::ErrorKind::TimedOut => 2,
            _ => 3,
        };
        add(&self.transport[i], 1);
    }
    pub(super) fn admit(self: &Arc<Self>) -> ConnectionGuard {
        add(&self.admitted, 1);
        let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(active, Ordering::Relaxed);
        ConnectionGuard(Arc::clone(self))
    }
    pub(super) fn command(self: &Arc<Self>, name: &str, started: Instant) -> CommandGuard {
        let kind = MongoCommandKind::classify(name);
        let counters = &self.commands[kind as usize];
        add(&counters.started, 1);
        counters.in_flight.fetch_add(1, Ordering::Relaxed);
        CommandGuard {
            metrics: Arc::clone(self),
            kind,
            started,
            completed: false,
        }
    }
    fn error_code(&self, document: &BsonDocument) {
        let code = match document.get_first("code") {
            Some(BsonValue::Int32(code)) => Some(*code),
            Some(BsonValue::Int64(code)) => i32::try_from(*code).ok(),
            _ => None,
        };
        if let Some(index) =
            code.and_then(|code| ERROR_CODES.iter().position(|known| *known == code))
        {
            add(&self.error_codes[index], 1);
        } else {
            add(&self.other_codes, 1);
        }
    }
}

pub(super) struct ConnectionGuard(Arc<Metrics>);
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::Relaxed);
        add(&self.0.closed, 1);
    }
}

pub(super) struct CommandGuard {
    metrics: Arc<Metrics>,
    kind: MongoCommandKind,
    started: Instant,
    completed: bool,
}

impl CommandGuard {
    pub(super) fn complete(mut self, body: &BsonDocument, suppressed: bool) {
        let success = matches!(body.get_first("ok"), Some(BsonValue::Double(value)) if *value == 1.0)
            || matches!(
                body.get_first("ok"),
                Some(BsonValue::Int32(1) | BsonValue::Int64(1))
            );
        let mut failed = !success;
        if failed {
            self.metrics.error_code(body);
        }
        if let Some(BsonValue::Array(errors)) = body.get_first("writeErrors") {
            failed |= !errors.is_empty();
            add(&self.metrics.write_errors, errors.len() as u64);
            for error in errors {
                if let BsonValue::Document(error) = error {
                    self.metrics.error_code(error);
                } else {
                    add(&self.metrics.other_codes, 1);
                }
            }
        }
        if let Some(BsonValue::Document(error)) = body.get_first("writeConcernError") {
            failed = true;
            self.metrics.error_code(error);
        }
        let counters = &self.metrics.commands[self.kind as usize];
        add(&counters.completed, 1);
        if failed {
            add(&counters.failed, 1);
        }
        if suppressed {
            add(&counters.suppressed, 1);
        }
        self.completed = true;
    }
}

impl Drop for CommandGuard {
    fn drop(&mut self) {
        let counters = &self.metrics.commands[self.kind as usize];
        if !self.completed {
            add(&counters.aborted, 1);
        }
        let micros = u64::try_from(self.started.elapsed().as_micros()).unwrap_or(u64::MAX);
        add(&counters.elapsed, micros);
        counters.max_elapsed.fetch_max(micros, Ordering::Relaxed);
        let bucket = latency_bucket(micros);
        add(&counters.latency[bucket], 1);
        counters.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}
