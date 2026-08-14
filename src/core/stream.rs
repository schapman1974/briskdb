//! Bounded protocol-neutral row streaming.

use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use tokio::sync::Notify;

use super::{CancellationReason, Column, EngineResult, OperationControl, Row};

/// The maximum number of decoded rows retained between SQLite and a frontend.
pub const DEFAULT_STREAM_BUFFER_ROWS: usize = 16;

struct StreamState {
    rows: VecDeque<Row>,
    terminal: Option<EngineResult<()>>,
    receiver_alive: bool,
}

struct Shared {
    capacity: usize,
    state: Mutex<StreamState>,
    not_full: Condvar,
    not_empty: Notify,
    control: Arc<OperationControl>,
    request_cancellation: super::CancellationToken,
    shutdown_cancellation: super::CancellationToken,
    deadline: Option<Instant>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, StreamState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The engine side of one bounded row stream.
pub(crate) struct RowProducer {
    shared: Arc<Shared>,
}

impl RowProducer {
    pub(crate) fn send(&self, row: Row) -> EngineResult<()> {
        let mut row = Some(row);
        let mut state = self.shared.lock();
        loop {
            if !state.receiver_alive {
                return Err(CancellationReason::Cancelled.error());
            }
            if self.shared.control.should_stop() {
                return Err(self
                    .shared
                    .control
                    .reason()
                    .unwrap_or(CancellationReason::Cancelled)
                    .error());
            }
            if state.rows.len() < self.shared.capacity {
                state
                    .rows
                    .push_back(row.take().expect("a streamed row is queued once"));
                drop(state);
                self.shared.not_empty.notify_one();
                return Ok(());
            }
            let waited = self
                .shared
                .not_full
                .wait_timeout(state, Duration::from_millis(25));
            state = match waited {
                Ok((state, _)) => state,
                Err(poisoned) => poisoned.into_inner().0,
            };
        }
    }

    pub(crate) fn finish(&self, result: EngineResult<()>) {
        let mut state = self.shared.lock();
        if state.terminal.is_none() {
            state.terminal = Some(result);
        }
        drop(state);
        self.shared.not_empty.notify_waiters();
    }
}

impl Clone for RowProducer {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

/// A bounded, asynchronous stream of protocol-neutral rows.
///
/// SQLite only steps ahead by [`DEFAULT_STREAM_BUFFER_ROWS`] rows. Dropping the
/// stream cancels the operation and interrupts its currently leased SQLite
/// connection before that connection can be reused.
#[must_use = "dropping a row stream cancels its unfinished query"]
pub struct RowStream {
    columns: Vec<Column>,
    shared: Arc<Shared>,
    complete: bool,
}

impl RowStream {
    pub(crate) fn channel(
        control: Arc<OperationControl>,
        request_cancellation: super::CancellationToken,
        shutdown_cancellation: super::CancellationToken,
        deadline: Option<Instant>,
    ) -> (Self, RowProducer) {
        let shared = Arc::new(Shared {
            capacity: DEFAULT_STREAM_BUFFER_ROWS,
            state: Mutex::new(StreamState {
                rows: VecDeque::new(),
                terminal: None,
                receiver_alive: true,
            }),
            not_full: Condvar::new(),
            not_empty: Notify::new(),
            control,
            request_cancellation,
            shutdown_cancellation,
            deadline,
        });
        (
            Self {
                columns: Vec::new(),
                shared: Arc::clone(&shared),
                complete: false,
            },
            RowProducer { shared },
        )
    }

    pub(crate) fn set_columns(&mut self, columns: Vec<Column>) {
        self.columns = columns;
    }

    /// Return the stable column metadata published before the first row.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Wait for the next row, terminal query error, or clean end of stream.
    pub async fn next_row(&mut self) -> Option<EngineResult<Row>> {
        if self.complete {
            return None;
        }
        loop {
            let notified = self.shared.not_empty.notified();
            {
                let mut state = self.shared.lock();
                let pending_reason = if self.shared.request_cancellation.is_cancelled()
                    || self.shared.shutdown_cancellation.is_cancelled()
                {
                    Some(CancellationReason::Cancelled)
                } else if self
                    .shared
                    .deadline
                    .is_some_and(|deadline| Instant::now() >= deadline)
                {
                    Some(CancellationReason::DeadlineExceeded)
                } else {
                    self.shared.control.reason()
                };
                if let Some(reason) = pending_reason {
                    self.shared.control.request_cancel(reason);
                    state.receiver_alive = false;
                    state.rows.clear();
                    self.complete = true;
                    self.shared.not_full.notify_all();
                    return Some(Err(reason.error()));
                }
                if let Some(row) = state.rows.pop_front() {
                    self.shared.not_full.notify_one();
                    return Some(Ok(row));
                }
                if let Some(result) = state.terminal.take() {
                    state.receiver_alive = false;
                    self.complete = true;
                    return result.err().map(Err);
                }
            }
            notified.await;
        }
    }
}

impl Drop for RowStream {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        state.receiver_alive = false;
        state.rows.clear();
        drop(state);
        self.shared.not_full.notify_all();
        if !self.complete {
            self.shared
                .control
                .request_cancel(CancellationReason::Cancelled);
        }
    }
}

impl fmt::Debug for RowStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.shared.lock();
        formatter
            .debug_struct("RowStream")
            .field("columns", &self.columns)
            .field("buffered_rows", &state.rows.len())
            .field("capacity", &self.shared.capacity)
            .field("complete", &self.complete)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use tokio::time::{Duration, timeout};

    use super::*;
    use crate::core::{CancellationToken, EngineError, EngineErrorKind, Value};

    fn row(value: i64) -> Row {
        Row::new(vec![Value::Int64(value)])
    }

    #[tokio::test]
    async fn producer_waits_for_bounded_capacity() {
        let control = OperationControl::new(None);
        let (mut stream, producer) = RowStream::channel(
            control,
            CancellationToken::new(),
            CancellationToken::new(),
            None,
        );
        for value in 0..DEFAULT_STREAM_BUFFER_ROWS {
            producer.send(row(value as i64)).unwrap();
        }
        let blocked = tokio::task::spawn_blocking(move || {
            producer.send(row(99))?;
            producer.finish(Ok(()));
            Ok::<_, EngineError>(())
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!blocked.is_finished());
        assert_eq!(stream.next_row().await.unwrap().unwrap(), row(0));
        timeout(Duration::from_secs(2), blocked)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn dropping_consumer_cancels_a_blocked_producer() {
        let control = OperationControl::new(Some(Instant::now() + Duration::from_secs(10)));
        let (stream, producer) = RowStream::channel(
            Arc::clone(&control),
            CancellationToken::new(),
            CancellationToken::new(),
            None,
        );
        for value in 0..DEFAULT_STREAM_BUFFER_ROWS {
            producer.send(row(value as i64)).unwrap();
        }
        let blocked = tokio::task::spawn_blocking(move || producer.send(row(99)));
        drop(stream);
        let error = timeout(Duration::from_secs(2), blocked)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        assert_eq!(control.reason(), Some(CancellationReason::Cancelled));
    }
}
