//! Background task that drives `cause=upstream_not_ready` readiness from
//! [`NodeSource::poll_upstream_health`].
//!
//! Spawned once at writer startup per [ADR-0015 §Upstream sync detection].
//! The task is independent of the ingest loop: probe failures never cancel
//! the loop and never propagate beyond a warning log. The probe owns the
//! [`ReadinessInput::NodeSource`] input of the writer's
//! [`ComposedReadiness`]: a non-ready snapshot publishes
//! [`ReadinessCause::UpstreamNotReady`] and a healthy snapshot publishes
//! [`ReadinessCause::Ready`]. The follow loop owns the admission state, so its
//! lag-derived writes cannot clear the probe's observation.
//!
//! A failed probe keeps the last observation, because an unreachable or
//! malformed health response is not evidence that the upstream is ready.
//! When the node failover switches endpoints it resets the input to
//! `starting` as part of the switch, before the probe can observe it, and the
//! probe observes the new endpoint at once instead of at its next tick. A
//! switch abandons a probe of the previous endpoint still in flight, and a
//! probe publishes only while the endpoint it probed is still active, so only
//! the active endpoint updates the input.
//!
//! [ADR-0015 §Upstream sync detection]:
//!     ../../../docs/adrs/0015-phase-driven-ingest.md#upstream-sync-detection

use std::{sync::Arc, time::Duration};

use tokio::{sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use zinder_runtime::{
    ComposedReadiness, ReadinessCause, ReadinessInput, UpstreamHealth, UpstreamNotReadyDetail,
};
use zinder_source::{NodeSource, SourceError, UpstreamHealthSnapshot};

/// Spawns the upstream-health probe task.
///
/// The task ticks every `poll_interval`, calls
/// [`NodeSource::poll_upstream_health`] on `source`, and publishes the
/// snapshot as the [`ReadinessInput::NodeSource`] input of `readiness`.
/// Probe errors keep the last published observation and log a warning; the
/// ingest loop continues to own the admission state. When `endpoint_switches`
/// is supplied, each change of the active node endpoint abandons the probe in
/// flight and probes the new endpoint immediately; the switch already reset
/// the input for the new endpoint. Cancellation also abandons a probe in
/// flight.
#[must_use = "drop the handle to detach the task or await it for symmetric shutdown"]
pub fn spawn_upstream_health_probe_task<Source>(
    source: Arc<Source>,
    readiness: ComposedReadiness,
    poll_interval: Duration,
    endpoint_switches: Option<watch::Receiver<usize>>,
    cancel: CancellationToken,
) -> JoinHandle<()>
where
    Source: NodeSource + 'static,
{
    tokio::spawn(async move {
        let mut endpoint_switches = endpoint_switches;
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(poll_interval) => {}
                Some(()) = endpoint_switched(endpoint_switches.as_mut()) => {}
            }
            tokio::select! {
                () = cancel.cancelled() => break,
                () = run_upstream_health_probe_once(
                    source.as_ref(),
                    &readiness,
                    endpoint_switches.as_mut(),
                ) => {}
            }
        }
    })
}

/// Resolves on the next endpoint switch, and never when there is no node
/// failover or its sender is gone.
async fn endpoint_switched(endpoint_switches: Option<&mut watch::Receiver<usize>>) -> Option<()> {
    match endpoint_switches {
        Some(endpoint_switches) => endpoint_switches.changed().await.ok(),
        None => std::future::pending().await,
    }
}

async fn run_upstream_health_probe_once<Source>(
    source: &Source,
    readiness: &ComposedReadiness,
    endpoint_switches: Option<&mut watch::Receiver<usize>>,
) where
    Source: NodeSource,
{
    let Some(endpoint_switches) = endpoint_switches else {
        publish_probe_outcome(source.poll_upstream_health().await, readiness);
        return;
    };
    endpoint_switches.mark_unchanged();
    // The clone sees the switch that abandons this probe and leaves it unseen
    // on `endpoint_switches`, so the task probes the new endpoint at once.
    let mut probed_endpoint = endpoint_switches.clone();
    let probe_outcome = tokio::select! {
        biased;
        Ok(()) = probed_endpoint.changed() => return,
        probe_outcome = source.poll_upstream_health() => probe_outcome,
    };
    // The borrow holds the switch channel's read lock through the
    // publication, and a switch resets the input while it holds the write
    // lock. A switch that landed first is seen here; one that lands later
    // waits for the lock, so a probe of the previous endpoint never replaces
    // that reset, and a probe of the new endpoint always follows it.
    let probed_endpoint_still_active = endpoint_switches.borrow();
    if !probed_endpoint_still_active.has_changed() {
        publish_probe_outcome(probe_outcome, readiness);
    }
}

fn publish_probe_outcome(
    probe_outcome: Result<UpstreamHealthSnapshot, SourceError>,
    readiness: &ComposedReadiness,
) {
    match probe_outcome {
        Ok(snapshot) => apply_upstream_health_snapshot(snapshot, readiness),
        Err(error) => {
            tracing::warn!(
                target: "zinder::ingest",
                event = "upstream_health_probe_failed",
                error = %error,
                "upstream health probe failed; keeping the last upstream observation"
            );
        }
    }
}

fn apply_upstream_health_snapshot(snapshot: UpstreamHealthSnapshot, readiness: &ComposedReadiness) {
    let cause = if snapshot.ready_for_queries {
        ReadinessCause::Ready
    } else {
        ReadinessCause::UpstreamNotReady(upstream_not_ready_detail_from_snapshot(snapshot))
    };
    readiness.publish_input(ReadinessInput::NodeSource, cause);
}

fn upstream_not_ready_detail_from_snapshot(
    snapshot: UpstreamHealthSnapshot,
) -> UpstreamNotReadyDetail {
    UpstreamNotReadyDetail {
        upstream_committed_height: snapshot.upstream_committed_height,
        upstream_estimated_height: snapshot.upstream_estimated_height,
        upstream_verification_progress: snapshot.upstream_verification_progress,
        upstream_health: UpstreamHealth {
            source: std::borrow::Cow::Borrowed(snapshot.source),
            reason: snapshot.reason,
        },
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use super::*;
    use zinder_runtime::{Readiness, ReadinessState};
    use zinder_source::UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT;

    fn ready_snapshot() -> UpstreamHealthSnapshot {
        UpstreamHealthSnapshot::ready(
            UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT,
            Some(42),
            Some(42),
            Some(1.0),
        )
    }

    fn syncing_snapshot() -> UpstreamHealthSnapshot {
        UpstreamHealthSnapshot::not_ready(
            UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT,
            "syncing",
            Some(15),
            Some(100),
            Some(0.5),
        )
    }

    fn composed_with_admission(admission: ReadinessState) -> (Readiness, ComposedReadiness) {
        let readiness = Readiness::default();
        let composed = ComposedReadiness::new(readiness.clone(), &[]);
        composed.publish_admission(admission);
        (readiness, composed)
    }

    #[test]
    fn healthy_snapshot_clears_only_the_probe_observation() {
        let (readiness, composed) = composed_with_admission(ReadinessState::ready(Some(42)));
        apply_upstream_health_snapshot(syncing_snapshot(), &composed);
        assert!(!readiness.report().is_ready);

        apply_upstream_health_snapshot(ready_snapshot(), &composed);
        let report = readiness.report();
        assert!(matches!(report.cause, ReadinessCause::Ready));
        assert_eq!(report.current_height, Some(42));

        composed.publish_admission(ReadinessState::syncing(Some(5), Some(37), Some(42)));
        apply_upstream_health_snapshot(ready_snapshot(), &composed);
        assert!(matches!(
            readiness.report().cause,
            ReadinessCause::Syncing { .. }
        ));
    }

    #[test]
    fn unhealthy_snapshot_sets_upstream_not_ready_with_current_height_preserved()
    -> Result<(), &'static str> {
        let (readiness, composed) = composed_with_admission(ReadinessState::ready(Some(17)));
        apply_upstream_health_snapshot(syncing_snapshot(), &composed);
        let report = readiness.report();
        let ReadinessCause::UpstreamNotReady(detail) = report.cause else {
            return Err("expected UpstreamNotReady cause");
        };
        assert_eq!(detail.upstream_health.source, "zebra_ready_endpoint");
        assert_eq!(detail.upstream_health.reason.as_ref(), "syncing");
        assert_eq!(detail.upstream_committed_height, Some(15));
        assert_eq!(detail.upstream_estimated_height, Some(100));
        assert_eq!(detail.upstream_verification_progress, Some(0.5));
        assert_eq!(report.current_height, Some(17));
        Ok(())
    }
}
