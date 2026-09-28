//! A synchronous authority call shares one budget across its SQLite handles.
//! This controls busy waits only: it never replays edits or interrupts an
//! uncontended commit after an edit has already started.

use std::{
    cell::RefCell,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::Arc,
};

use super::*;
use crate::core::OperationControl;

#[cfg(test)]
mod tests;

thread_local! {
    static AUTHORITY_CONTROL: RefCell<Option<Arc<OperationControl>>> = const { RefCell::new(None) };
}

struct Scope(Option<Arc<OperationControl>>);

impl Drop for Scope {
    fn drop(&mut self) {
        AUTHORITY_CONTROL.with(|slot| slot.replace(self.0.take()));
    }
}

pub(crate) fn with_operation_control<T>(
    control: Arc<OperationControl>,
    work: impl FnOnce() -> EngineResult<T>,
) -> EngineResult<T> {
    // Unconfigured calls retain SQLite's exact native two-second busy policy.
    let selected = control.has_contention_policy().then_some(control);
    let _scope = Scope(AUTHORITY_CONTROL.with(|slot| slot.replace(selected)));
    work()
}

fn busy_handler(_: i32) -> bool {
    let control = AUTHORITY_CONTROL.with(|slot| slot.borrow().clone());
    control
        .and_then(|control| control.wait_for_contention(None))
        .unwrap_or(false)
}

pub(super) fn with_connection<T>(
    connection: &mut Connection,
    work: impl FnOnce(&mut Connection) -> EngineResult<T>,
) -> EngineResult<T> {
    let Some(control) = AUTHORITY_CONTROL.with(|slot| slot.borrow().clone()) else {
        return work(connection);
    };
    connection
        .busy_handler(Some(busy_handler))
        .map_err(storage_error)?;
    let outcome = catch_unwind(AssertUnwindSafe(|| work(connection)));
    let cleanup = connection
        .busy_timeout(SECURITY_BUSY_TIMEOUT)
        .map_err(storage_error);
    match outcome {
        Err(panic) => resume_unwind(panic),
        Ok(Ok(value)) => {
            cleanup?;
            // A confirmed successful commit wins a late cancellation race.
            Ok(value)
        }
        Ok(Err(error)) => {
            // Do not replace corruption, identity or durability failures with
            // cancellation; the authority must still fence failed refreshes.
            if error.kind() == EngineErrorKind::Busy {
                if let Some(reason) = control.reason() {
                    return Err(reason.error());
                }
            }
            Err(error)
        }
    }
}
