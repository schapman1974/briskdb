//! Busy-only controls for synchronous, operation-owned storage handles.
//!
//! Unlike pooled-connection controls this does not own an interrupt slot or
//! replay work. A scope must drop every handle configured inside it before
//! returning (or restore its native busy timeout if the handle is retained).

use std::{
    cell::RefCell,
    sync::{Arc, Mutex, MutexGuard, TryLockError},
    time::Duration,
};

use rusqlite::Connection;

use crate::{
    core::{EngineError, EngineErrorKind, EngineResult, OperationControl},
    sqlite_error,
};

thread_local! {
    static CONTROL: RefCell<Option<Arc<OperationControl>>> = const { RefCell::new(None) };
}

struct Scope(Option<Arc<OperationControl>>);

impl Drop for Scope {
    fn drop(&mut self) {
        CONTROL.with(|slot| slot.replace(self.0.take()));
    }
}

pub(crate) fn with_control<T>(
    control: Option<Arc<OperationControl>>,
    work: impl FnOnce() -> EngineResult<T>,
) -> EngineResult<T> {
    let selected = control.filter(|control| control.has_contention_policy());
    let _scope = Scope(CONTROL.with(|slot| slot.replace(selected.clone())));
    let result = work();
    match result {
        Err(error) if error.kind() == EngineErrorKind::Busy => {
            match selected.and_then(|c| c.reason()) {
                Some(reason) => Err(reason.error()),
                None => Err(error),
            }
        }
        // A known success or corruption/uncertain-outcome error wins a late
        // cancellation race. Do not relabel it or execute the closure twice.
        result => result,
    }
}

pub(super) fn current() -> Option<Arc<OperationControl>> {
    CONTROL.with(|slot| slot.borrow().clone())
}

pub(super) fn is_configured() -> bool {
    CONTROL.with(|slot| slot.borrow().is_some())
}

/// A process mutex protecting storage I/O must not hide an unbounded wait
/// before the configured SQLite busy handler is reached.
pub(super) fn lock<'a, T>(mutex: &'a Mutex<T>, name: &str) -> EngineResult<MutexGuard<'a, T>> {
    let poisoned = || EngineError::new(EngineErrorKind::Internal, format!("{name} is poisoned"));
    let Some(control) = current() else {
        return mutex.lock().map_err(|_| poisoned());
    };
    loop {
        match mutex.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(TryLockError::Poisoned(_)) => return Err(poisoned()),
            Err(TryLockError::WouldBlock) => {
                if control.wait_for_contention(None) != Some(true) {
                    return Err(control.reason().map_or_else(
                        || EngineError::new(EngineErrorKind::Busy, format!("{name} is busy")),
                        |reason| reason.error(),
                    ));
                }
            }
        }
    }
}

fn busy_handler(_: i32) -> bool {
    current()
        .and_then(|control| control.wait_for_contention(None))
        .unwrap_or(false)
}

pub(super) fn configure(connection: &Connection, legacy: Duration) -> EngineResult<()> {
    if is_configured() {
        connection.busy_handler(Some(busy_handler))
    } else {
        connection.busy_timeout(legacy)
    }
    .map_err(sqlite_error::storage)
}

#[cfg(test)]
mod tests;
