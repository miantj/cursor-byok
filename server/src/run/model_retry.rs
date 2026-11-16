//! Defines retry policy for one logical model call.

use std::time::Duration;

use super::ModelCycleFailure;

pub(super) const MAX_MODEL_RETRIES: u32 = 3;
pub(super) const MODEL_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

pub(super) fn should_retry(failure: &ModelCycleFailure, retries: u32) -> bool {
    failure.retryable
        && !failure.output_started
        && retries < MAX_MODEL_RETRIES
        // A longer Retry-After ends this call instead of retrying earlier than requested.
        && retry_delay(failure, retries) <= MAX_RETRY_DELAY
}

/// `retries` counts retries already performed, before scheduling the next one.
pub(super) fn retry_delay(failure: &ModelCycleFailure, retries: u32) -> Duration {
    MODEL_RETRY_DELAY
        .saturating_mul(1 << retries.min(MAX_MODEL_RETRIES))
        .max(failure.retry_after.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run::{ModelCycleFailure, RunFailure};

    fn failure(retryable: bool) -> ModelCycleFailure {
        ModelCycleFailure {
            failure: RunFailure::Provider("failed".into()),
            partial_text: String::new(),
            partial_reasoning: String::new(),
            usage: None,
            retryable,
            retry_after: None,
            output_started: false,
        }
    }

    #[test]
    fn retry_budget_is_three_after_the_initial_attempt() {
        assert_eq!(MAX_MODEL_RETRIES, 3);
    }

    #[test]
    fn permits_only_the_configured_retry_budget() {
        let retryable = failure(true);
        for retries in 0..MAX_MODEL_RETRIES {
            assert!(should_retry(&retryable, retries));
        }
        assert!(!should_retry(&retryable, MAX_MODEL_RETRIES));
    }

    #[test]
    fn terminal_failures_never_retry() {
        assert!(!should_retry(&failure(false), 0));
        let mut visible = failure(true);
        visible.output_started = true;
        assert!(!should_retry(&visible, 0));
    }

    #[test]
    fn exponential_delay_respects_retry_after_without_unbounded_waits() {
        let mut failure = failure(true);
        for (retries, seconds) in [(0, 1), (1, 2), (2, 4)] {
            assert_eq!(retry_delay(&failure, retries), Duration::from_secs(seconds));
        }
        failure.retry_after = Some(Duration::from_secs(10));
        assert!(should_retry(&failure, 0));
        assert_eq!(retry_delay(&failure, 0), Duration::from_secs(10));
        failure.retry_after = Some(Duration::from_secs(61));
        assert!(!should_retry(&failure, 0));
    }
}
