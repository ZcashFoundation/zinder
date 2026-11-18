//! Standby evaluation of every node endpoint.
//!
//! The monitor runs beside the canonical follower and never switches. It
//! records the active endpoint's health, for the sustained-unready trigger,
//! and whether each other endpoint could take over, so a switch only
//! rechecks each candidate's readiness, tip, and one block, and the writer's
//! control plane stays responsive during a failover.

use std::{sync::Arc, time::Instant};

use futures_util::{StreamExt as _, stream::FuturesUnordered};
use tokio::{sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use zinder_core::NetworkUpgradeActivations;
use zinder_source::{NodeSource, SourceError, SourceFailureClass};

use super::{
    CandidateEvaluation, EndpointAdmission, EndpointExclusion, FailoverShared, FenceTips,
    IneligibleReason, NodeEndpointState, ValuePoolCapabilities, record_state_metrics,
};

/// Background evaluation of the endpoints a [`super::NodeFailover`] switches
/// between.
pub struct NodeEndpointMonitor<Endpoint> {
    shared: Arc<FailoverShared<Endpoint>>,
}

impl<Endpoint> NodeEndpointMonitor<Endpoint> {
    pub(super) const fn new(shared: Arc<FailoverShared<Endpoint>>) -> Self {
        Self { shared }
    }
}

/// Spawns the standby monitor.
///
/// A candidate qualifies only when its activation table equals
/// `admitted_activations`, the schedule the canonical store admitted, because
/// ingest does not admit another table while it runs.
///
/// Cancellation stops the monitor at once, even during a round that waits on
/// an endpoint that does not answer. A round that ran before the follower
/// published its fence leaves candidates pending, so the monitor evaluates
/// again as soon as the fence appears instead of one interval later. A switch
/// leaves the previous active endpoint pending, so it abandons the round in
/// flight, or the wait for the next one, and the monitor evaluates every
/// endpoint in its new role at once.
#[must_use = "drop the handle to detach the task or await it for symmetric shutdown"]
pub fn spawn_node_endpoint_monitor<Endpoint>(
    monitor: NodeEndpointMonitor<Endpoint>,
    admitted_activations: Arc<NetworkUpgradeActivations>,
    cancel: CancellationToken,
) -> JoinHandle<()>
where
    Endpoint: NodeSource,
{
    tokio::spawn(async move {
        let shared = monitor.shared.as_ref();
        let interval = shared.policy.standby_evaluation_interval;
        let mut fence = shared.fence.subscribe();
        let mut switches = shared.nodes.subscribe_switches();
        loop {
            let round_fence = *fence.borrow_and_update();
            switches.mark_unchanged();
            tokio::select! {
                () = cancel.cancelled() => break,
                () = evaluate_round(shared, &admitted_activations, round_fence) => {}
                () = endpoint_switched(&mut switches) => continue,
            }
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
                () = fence_published(&mut fence), if round_fence.is_none() => {}
                () = endpoint_switched(&mut switches) => {}
            }
        }
    })
}

/// Resolves once the follower has published a fence.
async fn fence_published(fence: &mut watch::Receiver<Option<FenceTips>>) {
    if fence.wait_for(Option::is_some).await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Resolves on the next switch of the active endpoint.
async fn endpoint_switched(switches: &mut watch::Receiver<usize>) {
    if switches.changed().await.is_err() {
        std::future::pending::<()>().await;
    }
}

#[derive(Clone, Copy)]
enum Observation {
    ActiveHealth {
        ready: Option<bool>,
    },
    Standby {
        state: NodeEndpointState,
        evaluation: Option<CandidateEvaluation>,
    },
}

impl Observation {
    const fn timed_out(active: bool) -> Self {
        if active {
            Self::ActiveHealth { ready: None }
        } else {
            Self::Standby {
                state: NodeEndpointState::Unreachable,
                evaluation: None,
            }
        }
    }
}

/// Evaluates every endpoint that is not excluded, each within one
/// evaluation timeout, and applies each observation as soon as it arrives.
///
/// Applying observations one at a time keeps a candidate's evaluation as
/// fresh as its own answer: an active endpoint that hangs delays only its
/// own health observation, never the evidence that another endpoint can
/// take over.
async fn evaluate_round<Endpoint>(
    shared: &FailoverShared<Endpoint>,
    admitted_activations: &NetworkUpgradeActivations,
    fence: Option<FenceTips>,
) where
    Endpoint: NodeSource,
{
    let active_position = shared.nodes.active_position();
    let excluded = shared
        .board
        .lock()
        .records
        .iter()
        .map(|record| record.excluded.is_some())
        .collect::<Vec<_>>();
    let evaluation_timeout = shared.policy.evaluation_timeout();
    let mut observations = shared
        .nodes
        .endpoints()
        .filter(|(position, _)| !excluded.get(*position).copied().unwrap_or(true))
        .map(|(position, endpoint)| async move {
            let active = position == active_position;
            let observation = tokio::time::timeout(
                evaluation_timeout,
                observe(shared, endpoint, active, admitted_activations, fence),
            )
            .await
            .unwrap_or_else(|_elapsed| Observation::timed_out(active));
            (position, observation)
        })
        .collect::<FuturesUnordered<_>>();
    while let Some((position, observation)) = observations.next().await {
        if !apply_observation(shared, active_position, position, observation) {
            break;
        }
    }
}

async fn observe<Endpoint>(
    shared: &FailoverShared<Endpoint>,
    endpoint: &Endpoint,
    active: bool,
    admitted_activations: &NetworkUpgradeActivations,
    fence: Option<FenceTips>,
) -> Observation
where
    Endpoint: NodeSource,
{
    if active {
        Observation::ActiveHealth {
            ready: readiness_of(endpoint).await.ok(),
        }
    } else {
        evaluate_candidate(shared, endpoint, admitted_activations, fence).await
    }
}

/// Records one observation, or returns `false` once a switch since the
/// round started has made the round's observations meaningless.
fn apply_observation<Endpoint>(
    shared: &FailoverShared<Endpoint>,
    observed_active_position: usize,
    position: usize,
    observation: Observation,
) -> bool
where
    Endpoint: NodeSource,
{
    let mut board = shared.board.lock();
    // A switch changes every endpoint's role, so observations taken for the
    // previous roles no longer describe them.
    if shared.nodes.active_position() != observed_active_position {
        return false;
    }
    match observation {
        Observation::ActiveHealth { ready } => {
            if let Some(record) = board.records.get_mut(position) {
                match ready {
                    Some(true) => record.not_ready_since = None,
                    Some(false) => {
                        record.not_ready_since.get_or_insert_with(Instant::now);
                    }
                    None => {}
                }
            }
        }
        Observation::Standby { state, evaluation } => {
            if let Some(record) = board.records.get_mut(position) {
                if record.state != state {
                    log_state_change(position, state);
                }
                record.state = state;
                record.evaluation = evaluation;
            }
            board.evaluations_applied = board.evaluations_applied.wrapping_add(1);
        }
    }
    record_state_metrics(&board.records);
    true
}

/// Returns whether the endpoint reports ready, or the error when it did not
/// answer.
///
/// An endpoint without a readiness signal counts as ready: the tip checks
/// still require it to hold ingest's chain.
pub(super) async fn readiness_of<Endpoint>(endpoint: &Endpoint) -> Result<bool, SourceError>
where
    Endpoint: NodeSource + ?Sized,
{
    match endpoint.poll_upstream_health().await {
        Ok(snapshot) => Ok(snapshot.ready_for_queries),
        Err(error) if error.upstream_classification() == SourceFailureClass::CapabilityMissing => {
            Ok(true)
        }
        Err(error) => Err(error),
    }
}

async fn evaluate_candidate<Endpoint>(
    shared: &FailoverShared<Endpoint>,
    endpoint: &Endpoint,
    admitted_activations: &NetworkUpgradeActivations,
    fence: Option<FenceTips>,
) -> Observation
where
    Endpoint: NodeSource,
{
    let ineligible = |reason| Observation::Standby {
        state: NodeEndpointState::Ineligible(reason),
        evaluation: None,
    };
    let unreachable = Observation::Standby {
        state: NodeEndpointState::Unreachable,
        evaluation: None,
    };
    // The genesis check covers endpoints that startup deferred or found
    // unreachable, which were never checked against the configured network.
    match EndpointAdmission::check(endpoint, shared.genesis_hash).await {
        EndpointAdmission::Admitted(admitted) => {
            if admitted.activations != *admitted_activations {
                return ineligible(IneligibleReason::Refused(
                    EndpointExclusion::ScheduleConflict,
                ));
            }
            if ValuePoolCapabilities::of(admitted.capabilities) != shared.value_pool_capabilities {
                return ineligible(IneligibleReason::CapabilityMismatch);
            }
        }
        EndpointAdmission::Excluded(exclusion) => {
            return ineligible(IneligibleReason::Refused(exclusion));
        }
        EndpointAdmission::Ineligible(reason) => return ineligible(reason),
        EndpointAdmission::Unreachable(_) | EndpointAdmission::Deferred => return unreachable,
    }
    match readiness_of(endpoint).await {
        Ok(true) => {}
        Ok(false) => return ineligible(IneligibleReason::NotReady),
        Err(_) => return unreachable,
    }
    let Some(fence) = fence else {
        return Observation::Standby {
            state: NodeEndpointState::Pending,
            evaluation: None,
        };
    };
    match chain_standing(endpoint, fence).await {
        Ok(Some(diverged)) => Observation::Standby {
            state: if diverged {
                NodeEndpointState::StandbyDiverged
            } else {
                NodeEndpointState::Standby
            },
            evaluation: Some(CandidateEvaluation {
                diverged,
                observed_at: Instant::now(),
            }),
        },
        Ok(None) => ineligible(IneligibleReason::Lagging),
        Err(ChainStandingError::DivergedAtOrBelowSettlement) => {
            ineligible(IneligibleReason::DivergedAtOrBelowSettlement)
        }
        Err(ChainStandingError::Unreachable(_)) => unreachable,
    }
}

pub(super) enum ChainStandingError {
    DivergedAtOrBelowSettlement,
    Unreachable(SourceError),
}

/// Returns `Some(false)` when the endpoint holds ingest's visible tip,
/// `Some(true)` when it shares the settled tip but not the visible tip, and
/// `None` when its tip is below ingest's.
pub(super) async fn chain_standing<Endpoint>(
    endpoint: &Endpoint,
    fence: FenceTips,
) -> Result<Option<bool>, ChainStandingError>
where
    Endpoint: NodeSource + ?Sized,
{
    let tip = endpoint
        .tip_id()
        .await
        .map_err(ChainStandingError::Unreachable)?;
    if tip.height < fence.local_tip.height {
        return Ok(None);
    }
    let at_local_tip = endpoint
        .fetch_block_at(fence.local_tip.height)
        .await
        .map_err(ChainStandingError::Unreachable)?;
    if at_local_tip.hash == fence.local_tip.hash {
        return Ok(Some(false));
    }
    let at_settled_tip = endpoint
        .fetch_block_at(fence.settled_tip.height)
        .await
        .map_err(ChainStandingError::Unreachable)?;
    if at_settled_tip.hash == fence.settled_tip.hash {
        Ok(Some(true))
    } else {
        Err(ChainStandingError::DivergedAtOrBelowSettlement)
    }
}

fn log_state_change(position: usize, state: NodeEndpointState) {
    match state {
        NodeEndpointState::Ineligible(reason) => tracing::info!(
            target: "zinder::ingest",
            event = "node_endpoint_skipped",
            position,
            reason = reason.label(),
            "node endpoint cannot take over now"
        ),
        NodeEndpointState::Active
        | NodeEndpointState::Pending
        | NodeEndpointState::Standby
        | NodeEndpointState::StandbyDiverged
        | NodeEndpointState::Unreachable
        | NodeEndpointState::Excluded(_) => tracing::info!(
            target: "zinder::ingest",
            event = "node_endpoint_state_changed",
            position,
            state = state.metric_label(),
            "node endpoint standing changed"
        ),
    }
}
