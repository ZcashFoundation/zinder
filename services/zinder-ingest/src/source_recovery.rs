//! Loop-recovery decisions for long-running writer subsystems.
//!
//! Every Zinder writer subsystem (canonical follower, live mempool owner,
//! chain-tip notification subscriber) runs an indefinite loop that
//! observes upstream node state. When an iteration fails, the loop must
//! decide whether to drain readiness and continue, or exit so the operator
//! can intervene. This module owns that decision.
//!
//! ## Posture
//!
//! - **Source-shaped errors** (every variant of [`SourceError`]) are
//!   recoverable. The loop drains readiness with operator-actionable
//!   detail, backs off according to the failure class, and continues. A
//!   classification scheme that puts unknown upstream failures into a
//!   process-exit path is fragile by construction.
//! - **Source-retry-budget exhaustion** and **per-call deadline
//!   exhaustion** ([`IngestError::SourceRetryBudgetExceeded`],
//!   [`IngestError::SourceRetryDeadlineExceeded`]) are recoverable; the
//!   loop resets and re-observes the upstream view.
//! - **Storage failures** and **internal logic errors** (for example
//!   `BlockingTaskFailed`) are fatal: data integrity is at stake, or the
//!   failure indicates a Zinder bug. The loop exits
//!   so the supervising runtime can decide whether to restart, replay, or
//!   require manual reset.
//!
//! See [`docs/architecture/chain-ingestion.md`](../../../docs/architecture/chain-ingestion.md)
//! and [`docs/adrs/0013-source-failure-recovery-topology.md`](../../../docs/adrs/0013-source-failure-recovery-topology.md)
//! for the corresponding architectural contract.

use std::{borrow::Cow, time::Duration};

use zinder_runtime::NodeUnavailableDetail;
use zinder_source::{SourceError, SourceFailureClass};

use crate::chain_ingest::IngestError;

/// Maximum length of the `last_reason` string carried in readiness payloads.
///
/// Bounds the public readiness JSON and Prometheus payload regardless of
/// how verbose a particular upstream error message is. Strings longer than
/// this are truncated with an ASCII ellipsis suffix.
const READINESS_REASON_MAX_BYTES: usize = 256;

/// What a long-running writer loop should do after seeing an ingest error.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SourceRecoveryDecision {
    /// Drain readiness with the supplied detail, sleep `backoff`, then
    /// continue the loop.
    Recover {
        /// Operator-actionable narrative for the readiness payload. The
        /// caller is responsible for combining this with the current
        /// outage window (see [`SourceOutageTracker`]).
        failure_class: SourceFailureClass,
        /// Sanitized failure reason. Bounded in length by
        /// [`READINESS_REASON_MAX_BYTES`].
        last_reason: Cow<'static, str>,
        /// Backoff before the loop tries again.
        backoff: Duration,
    },
    /// Exit the loop with the original error.
    Exit,
}

/// Backoff schedule for the three failure-class buckets.
///
/// The defaults match the runbook's expected operator experience:
/// transient transport failures and view-stale races back off short (the
/// writer re-observes the upstream tip on each iteration anyway); stream
/// disconnections back off slightly longer to avoid thundering-herd
/// re-subscribes; structural failures (capability missing, protocol
/// mismatch, malformed bytes, configuration) back off longest because they
/// require operator action and the loop is alive only so operators can
/// observe the typed readiness state.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SourceRecoveryBackoff {
    /// Backoff applied when the upstream node is unreachable
    /// ([`SourceFailureClass::NodeUnreachable`]).
    pub node_unreachable: Duration,
    /// Backoff applied when the upstream view changed under us
    /// ([`SourceFailureClass::UpstreamViewChanged`]).
    pub view_changed: Duration,
    /// Backoff applied when a long-lived subscription disconnected
    /// ([`SourceFailureClass::StreamDisconnected`]).
    pub stream_disconnected: Duration,
    /// Backoff applied for operator-action failures
    /// ([`SourceFailureClass::CapabilityMissing`],
    /// [`SourceFailureClass::ProtocolMismatch`],
    /// [`SourceFailureClass::Malformed`],
    /// [`SourceFailureClass::Configuration`]). Long, because retrying
    /// will not change the outcome until the operator acts.
    pub operator_action: Duration,
    /// Backoff applied when the per-call retry budget or deadline was
    /// exhausted. Matches `node_unreachable` by default.
    pub retry_exhausted: Duration,
}

impl SourceRecoveryBackoff {
    /// Production defaults.
    #[cfg(not(test))]
    pub(crate) const PRODUCTION: Self = Self {
        node_unreachable: Duration::from_secs(2),
        view_changed: Duration::from_millis(250),
        stream_disconnected: Duration::from_secs(2),
        operator_action: Duration::from_secs(10),
        retry_exhausted: Duration::from_secs(2),
    };

    /// Test defaults — every backoff collapses to 1ms so loops drain
    /// readiness quickly without making the test suite slow.
    #[cfg(test)]
    pub(crate) const FAST_FOR_TESTS: Self = Self {
        node_unreachable: Duration::from_millis(1),
        view_changed: Duration::from_millis(1),
        stream_disconnected: Duration::from_millis(1),
        operator_action: Duration::from_millis(1),
        retry_exhausted: Duration::from_millis(1),
    };

    /// Backoff for a recoverable failure class.
    #[allow(
        clippy::wildcard_enum_match_arm,
        reason = "SourceFailureClass is #[non_exhaustive]; new variants default to operator-action backoff until a dedicated cadence is documented."
    )]
    pub(crate) fn for_class(self, class: SourceFailureClass) -> Duration {
        match class {
            SourceFailureClass::NodeUnreachable => self.node_unreachable,
            SourceFailureClass::UpstreamViewChanged => self.view_changed,
            SourceFailureClass::StreamDisconnected => self.stream_disconnected,
            _ => self.operator_action,
        }
    }
}

/// Picks the canonical recovery backoff for the current build profile.
///
/// Production binaries use [`SourceRecoveryBackoff::PRODUCTION`]; tests
/// (gated by `cfg(test)`) use the fast collapse defined by
/// [`SourceRecoveryBackoff::FAST_FOR_TESTS`].
pub(crate) const fn default_recovery_backoff() -> SourceRecoveryBackoff {
    #[cfg(not(test))]
    {
        SourceRecoveryBackoff::PRODUCTION
    }
    #[cfg(test)]
    {
        SourceRecoveryBackoff::FAST_FOR_TESTS
    }
}

/// Returns the recovery decision for `error`.
///
/// Source-shaped errors are always recoverable; the class drives the
/// readiness payload and backoff selection. Storage failures and internal
/// logic errors exit the loop.
pub(crate) fn decide_recovery(
    error: &IngestError,
    backoff: SourceRecoveryBackoff,
) -> SourceRecoveryDecision {
    match error {
        IngestError::Source(source) => {
            let class = source.upstream_classification();
            SourceRecoveryDecision::Recover {
                failure_class: class,
                last_reason: sanitize_reason(source),
                backoff: backoff.for_class(class),
            }
        }
        IngestError::SourceRetryBudgetExceeded { operation, .. }
        | IngestError::SourceRetryDeadlineExceeded { operation, .. } => {
            SourceRecoveryDecision::Recover {
                failure_class: SourceFailureClass::NodeUnreachable,
                last_reason: Cow::Owned(truncate_reason(&format!(
                    "per-call retry budget exhausted during {operation}"
                ))),
                backoff: backoff.retry_exhausted,
            }
        }
        IngestError::Store(_)
        | IngestError::CanonicalStore(_)
        | IngestError::MaterializedViewHistoryIncomplete { .. }
        | IngestError::TransparentPrevoutUnresolved { .. }
        | IngestError::CommitmentTreeCheckpointMissing { .. }
        | IngestError::CommitmentTreeState { .. }
        | IngestError::UnknownNodeSource { .. }
        | IngestError::CanonicalBlockConstruction(_)
        | IngestError::BlockingTaskFailed { .. }
        | IngestError::SourceSegmentFetchTaskStopped { .. }
        | IngestError::MaterializedViewDispatch(_)
        | IngestError::MaterializedViewCursorUnreadable { .. }
        | IngestError::MaterializedViewStore(_) => SourceRecoveryDecision::Exit,
    }
}

/// Produces a [`NodeUnavailableDetail`] for the first iteration of a new
/// outage from the supplied recovery decision.
///
/// `class` and `last_reason` come from the decision the loop just made;
/// the iteration counter starts at one and `outage_seconds` is zero.
#[must_use]
pub(crate) fn detail_for_new_outage(
    failure_class: SourceFailureClass,
    last_reason: Cow<'static, str>,
) -> NodeUnavailableDetail {
    NodeUnavailableDetail::first_iteration(failure_class.label(), last_reason)
}

/// Extends an outage with the latest recovery decision and elapsed time.
#[must_use]
pub(crate) fn detail_for_ongoing_outage(
    previous: &NodeUnavailableDetail,
    failure_class: SourceFailureClass,
    last_reason: Cow<'static, str>,
    outage_seconds: u32,
) -> NodeUnavailableDetail {
    NodeUnavailableDetail::extend_with(previous, failure_class.label(), last_reason, outage_seconds)
}

fn sanitize_reason(error: &SourceError) -> Cow<'static, str> {
    Cow::Owned(truncate_reason(&error.to_string()))
}

fn truncate_reason(reason: &str) -> String {
    if reason.len() <= READINESS_REASON_MAX_BYTES {
        reason.to_owned()
    } else {
        let mut truncated = reason
            .char_indices()
            .take_while(|(byte_index, _)| *byte_index < READINESS_REASON_MAX_BYTES)
            .map(|(_, ch)| ch)
            .collect::<String>();
        truncated.push('…');
        truncated
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SourceRecoveryBackoff, SourceRecoveryDecision, decide_recovery, default_recovery_backoff,
        sanitize_reason, truncate_reason,
    };
    use crate::chain_ingest::IngestError;
    use std::borrow::Cow;
    use zinder_core::BlockHeight;
    use zinder_source::{SourceError, SourceFailureClass};

    #[test]
    fn node_unavailable_is_recoverable() {
        let error = IngestError::Source(SourceError::NodeUnavailable {
            reason: "connection refused".to_owned(),
        });
        let decision = decide_recovery(&error, SourceRecoveryBackoff::FAST_FOR_TESTS);
        assert!(matches!(
            decision,
            SourceRecoveryDecision::Recover {
                failure_class: SourceFailureClass::NodeUnreachable,
                ..
            }
        ));
    }

    #[test]
    fn view_stale_block_unavailable_is_recoverable() {
        // Zebra can report a best-chain view change with a non-`-28` code.
        // The typed failure remains recoverable regardless of the raw code.
        let error = IngestError::Source(SourceError::BlockUnavailable {
            height: BlockHeight::new(4_013_801),
            reason: "block height not in best chain".to_owned(),
        });
        let decision = decide_recovery(&error, SourceRecoveryBackoff::FAST_FOR_TESTS);
        let SourceRecoveryDecision::Recover {
            failure_class,
            last_reason,
            ..
        } = decision
        else {
            unreachable!("BlockUnavailable must be recoverable")
        };
        assert_eq!(failure_class, SourceFailureClass::UpstreamViewChanged);
        assert!(last_reason.contains("block height not in best chain"));
    }

    #[test]
    fn protocol_mismatch_stays_recoverable_with_operator_action_backoff() {
        let error = IngestError::Source(SourceError::SourceProtocolMismatch {
            reason: "missing block hash",
        });
        let decision = decide_recovery(&error, SourceRecoveryBackoff::FAST_FOR_TESTS);
        assert!(matches!(
            decision,
            SourceRecoveryDecision::Recover {
                failure_class: SourceFailureClass::ProtocolMismatch,
                ..
            }
        ));
    }

    #[test]
    fn store_failures_exit_the_loop() {
        // We cannot easily construct a real StoreError without a live
        // store, so we exercise a missing commitment-tree checkpoint (a
        // storage-integrity exit path) instead. Storage failures take the
        // same branch by structural exhaustion.
        let error = IngestError::CommitmentTreeCheckpointMissing {
            height: BlockHeight::new(100),
        };
        assert_eq!(
            decide_recovery(&error, SourceRecoveryBackoff::FAST_FOR_TESTS),
            SourceRecoveryDecision::Exit,
        );
    }

    #[test]
    fn retry_exhaustion_is_recoverable() {
        let error = IngestError::SourceRetryDeadlineExceeded {
            operation: "fetch_block at height 1234".to_owned(),
            reason: "transient timeout".to_owned(),
        };
        let decision = decide_recovery(&error, SourceRecoveryBackoff::FAST_FOR_TESTS);
        let SourceRecoveryDecision::Recover {
            failure_class,
            last_reason,
            ..
        } = decision
        else {
            unreachable!("retry exhaustion must be recoverable")
        };
        assert_eq!(failure_class, SourceFailureClass::NodeUnreachable);
        assert!(last_reason.contains("fetch_block at height 1234"));
    }

    #[test]
    fn truncate_keeps_short_reasons_unchanged() {
        let short = "short reason";
        assert_eq!(truncate_reason(short), short);
    }

    #[test]
    fn truncate_appends_ellipsis_when_too_long() {
        let long = "x".repeat(super::READINESS_REASON_MAX_BYTES + 50);
        let truncated = truncate_reason(&long);
        assert!(truncated.ends_with('…'));
        assert!(truncated.len() <= super::READINESS_REASON_MAX_BYTES + '…'.len_utf8());
    }

    #[test]
    fn sanitize_reason_emits_owned_string_for_source_error() {
        let error = SourceError::NodeUnavailable {
            reason: "connection refused".to_owned(),
        };
        let reason = sanitize_reason(&error);
        assert!(matches!(reason, Cow::Owned(_)));
        assert!(reason.contains("connection refused"));
    }

    #[test]
    fn source_failure_class_labels_match_runbook_table() {
        // Pin the labels the runbook table promises operators. Renaming any of
        // these requires updating dashboards and alert rules in lock-step.
        assert_eq!(
            SourceFailureClass::NodeUnreachable.label(),
            "node_unreachable"
        );
        assert_eq!(
            SourceFailureClass::UpstreamViewChanged.label(),
            "upstream_view_changed"
        );
        assert_eq!(
            SourceFailureClass::StreamDisconnected.label(),
            "stream_disconnected"
        );
        assert_eq!(
            SourceFailureClass::ProtocolMismatch.label(),
            "protocol_mismatch"
        );
        assert_eq!(
            SourceFailureClass::CapabilityMissing.label(),
            "capability_missing"
        );
        assert_eq!(SourceFailureClass::Malformed.label(), "malformed");
        assert_eq!(SourceFailureClass::Configuration.label(), "configuration");
    }

    #[test]
    fn default_recovery_backoff_uses_fast_collapse_in_tests() {
        let backoff = default_recovery_backoff();
        assert_eq!(
            backoff.node_unreachable,
            std::time::Duration::from_millis(1),
        );
    }
}
