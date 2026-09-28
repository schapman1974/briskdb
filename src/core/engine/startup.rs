//! One optional contention budget for pre-engine startup admission.

use super::*;

pub(super) struct StartupOperation {
    pub(super) control: Option<Arc<OperationControl>>,
    metrics: Arc<ContentionMetrics>,
    cancel_on_drop: Option<CancelOnDrop>,
}

impl StartupOperation {
    pub(super) fn new(policy: Option<crate::core::ContentionPolicy>) -> Self {
        let metrics = Arc::new(ContentionMetrics::default());
        let control = policy.map(|policy| {
            // Engine request timeouts govern requests after open, not startup.
            OperationControl::with_contention_metrics(None, Some(policy), Arc::clone(&metrics))
        });
        let cancel_on_drop = control
            .as_ref()
            .map(|control| CancelOnDrop::new(Arc::clone(control)));
        Self {
            control,
            metrics,
            cancel_on_drop,
        }
    }

    pub(super) fn complete(mut self) -> Arc<ContentionMetrics> {
        if let Some(cancel) = self.cancel_on_drop.as_mut() {
            cancel.disarm();
        }
        self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successful_startup_keeps_metrics_and_disarms_cancellation() {
        let startup = StartupOperation::new(Some(crate::core::ContentionPolicy::fail_fast()));
        let control = Arc::clone(startup.control.as_ref().unwrap());
        assert_eq!(control.wait_for_contention(None), Some(false));
        let metrics = startup.complete();
        assert_eq!(metrics.snapshot().exhausted_budgets(), 1);
        assert_eq!(control.reason(), None);
    }

    #[test]
    fn abandoned_startup_cancels_but_legacy_has_no_control() {
        assert!(StartupOperation::new(None).control.is_none());
        let startup = StartupOperation::new(Some(crate::core::ContentionPolicy::fail_fast()));
        let control = Arc::clone(startup.control.as_ref().unwrap());
        drop(startup);
        assert_eq!(control.reason(), Some(CancellationReason::Cancelled));
    }
}
