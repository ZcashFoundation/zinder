//! Standby evaluation of every node endpoint.
//!
//! The monitor runs beside the canonical follower and never switches. It
//! records the active endpoint's health, for the sustained-unready trigger,
//! and whether each other endpoint could take over, so a switch only
//! rechecks the chosen candidate's tip and one block, and the writer's
//! control plane stays responsive during a failover.

use std::{sync::Arc, time::Instant};

use futures_util::future::join_all;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use zinder_core::NetworkUpgradeActivations;
use zinder_source::{NodeSource, SourceFailureClass};

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
        let interval = monitor.shared.policy.standby_evaluation_interval;
        loop {
            evaluate_round(&monitor.shared, &admitted_activations).await;
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }
        }
    })
}

enum Observation {
    ActiveHealth {
        ready: Option<bool>,
    },
    Standby {
        state: NodeEndpointState,
        evaluation: Option<CandidateEvaluation>,
    },
}

async fn evaluate_round<Endpoint>(
    shared: &FailoverShared<Endpoint>,
    admitted_activations: &NetworkUpgradeActivations,
) where
    Endpoint: NodeSource,
{
    let active_position = shared.nodes.active_position();
    let (fence, excluded) = {
        let board = shared.board.lock();
        let excluded = board
            .records
            .iter()
            .map(|record| record.excluded.is_some())
            .collect::<Vec<_>>();
        (board.fence, excluded)
    };
    let observations = join_all(
        shared
            .nodes
            .endpoints()
            .filter(|(position, _)| !excluded.get(*position).copied().unwrap_or(true))
            .map(|(position, endpoint)| async move {
                let observation = if position == active_position {
                    Observation::ActiveHealth {
                        ready: health_of(endpoint).await,
                    }
                } else {
                    evaluate_candidate(
                        endpoint,
                        admitted_activations,
                        fence,
                        shared.value_pool_capabilities,
                    )
                    .await
                };
                (position, observation)
            }),
    )
    .await;
    apply_observations(shared, active_position, observations);
}

fn apply_observations<Endpoint>(
    shared: &FailoverShared<Endpoint>,
    observed_active_position: usize,
    observations: Vec<(usize, Observation)>,
) where
    Endpoint: NodeSource,
{
    let mut board = shared.board.lock();
    // A switch during the round changes every endpoint's role, so the round's
    // observations no longer describe them.
    if shared.nodes.active_position() != observed_active_position {
        return;
    }
    for (position, observation) in observations {
        let Some(record) = board.records.get_mut(position) else {
            continue;
        };
        match observation {
            Observation::ActiveHealth { ready } => match ready {
                Some(true) => record.not_ready_since = None,
                Some(false) => {
                    record.not_ready_since.get_or_insert_with(Instant::now);
                }
                None => {}
            },
            Observation::Standby { state, evaluation } => {
                if record.state != state {
                    log_state_change(position, state);
                }
                record.state = state;
                record.evaluation = evaluation;
            }
        }
    }
    record_state_metrics(&board.records);
}

/// Returns the endpoint's readiness, or `None` when it did not answer.
///
/// An endpoint without a readiness signal counts as ready: the tip checks
/// still require it to hold ingest's chain.
async fn health_of<Endpoint>(endpoint: &Endpoint) -> Option<bool>
where
    Endpoint: NodeSource,
{
    match endpoint.poll_upstream_health().await {
        Ok(snapshot) => Some(snapshot.ready_for_queries),
        Err(error) if error.upstream_classification() == SourceFailureClass::CapabilityMissing => {
            Some(true)
        }
        Err(_) => None,
    }
}

async fn evaluate_candidate<Endpoint>(
    endpoint: &Endpoint,
    admitted_activations: &NetworkUpgradeActivations,
    fence: Option<FenceTips>,
    value_pool_capabilities: ValuePoolCapabilities,
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
    match EndpointAdmission::check(endpoint, None).await {
        EndpointAdmission::Admitted(admitted) => {
            if admitted.activations != *admitted_activations {
                return ineligible(IneligibleReason::Refused(
                    EndpointExclusion::ScheduleConflict,
                ));
            }
            if ValuePoolCapabilities::of(admitted.capabilities) != value_pool_capabilities {
                return ineligible(IneligibleReason::CapabilityMismatch);
            }
        }
        EndpointAdmission::Excluded(exclusion) => {
            return ineligible(IneligibleReason::Refused(exclusion));
        }
        EndpointAdmission::Unreachable(_) => return unreachable,
    }
    match health_of(endpoint).await {
        Some(true) => {}
        Some(false) => return ineligible(IneligibleReason::NotReady),
        None => return unreachable,
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
        Err(ChainStandingError::Unreachable) => unreachable,
    }
}

enum ChainStandingError {
    DivergedAtOrBelowSettlement,
    Unreachable,
}

/// Returns `Some(false)` when the endpoint holds ingest's visible tip,
/// `Some(true)` when it shares the settled tip but not the visible tip, and
/// `None` when its tip is below ingest's.
async fn chain_standing<Endpoint>(
    endpoint: &Endpoint,
    fence: FenceTips,
) -> Result<Option<bool>, ChainStandingError>
where
    Endpoint: NodeSource,
{
    let tip = endpoint
        .tip_id()
        .await
        .map_err(|_| ChainStandingError::Unreachable)?;
    if tip.height < fence.local_tip.height {
        return Ok(None);
    }
    let at_local_tip = endpoint
        .fetch_block_at(fence.local_tip.height)
        .await
        .map_err(|_| ChainStandingError::Unreachable)?;
    if at_local_tip.hash == fence.local_tip.hash {
        return Ok(Some(false));
    }
    let at_settled_tip = endpoint
        .fetch_block_at(fence.settled_tip.height)
        .await
        .map_err(|_| ChainStandingError::Unreachable)?;
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
