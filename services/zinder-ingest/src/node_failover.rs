//! Ingest failover across the ordered node list.
//!
//! `[node]` is position 0 and `[node_fallbacks.N]` is position `N`. Ingest
//! reads from one active endpoint through a [`NodeSet`]. A standby monitor
//! evaluates the other endpoints in the background, and the canonical
//! follower switches between follow iterations, so every read that prepares
//! one block reaches one endpoint. There is no timed failback: a later switch
//! walks positions from 0 again, and a restart starts at position 0.
//!
//! See [ADR-0013](../../../docs/adrs/0013-source-failure-recovery-topology.md)
//! for the switch rule and
//! [ADR-0039](../../../docs/adrs/0039-writer-admitted-network-upgrade-schedules.md)
//! for the activation-table rule.

mod admission;
mod monitor;

use std::{
    fmt,
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures_util::{StreamExt as _, stream::FuturesOrdered};
use parking_lot::Mutex;
use zinder_core::{BlockHash, BlockId};
use zinder_runtime::{ActiveNodeEndpoint, ComposedReadiness, ReadinessCause, ReadinessInput};
use zinder_source::{NodeSet, NodeSetSwitch, NodeSource, SourceFailureClass};

pub use admission::{
    AdmittedEndpoint, EndpointAdmission, EndpointExclusion, NoNodeEndpointAdmitted,
    NodeEndpointAdmissions, REQUIRED_INGEST_NODE_CAPABILITIES, missing_required_capability,
};
pub use monitor::{NodeEndpointMonitor, spawn_node_endpoint_monitor};

/// Thresholds that decide when ingest leaves its active endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeFailoverPolicy {
    /// Consecutive follower recoveries of one transport class,
    /// `node_unreachable` or `stream_disconnected`, that switch endpoints.
    pub transport_failures_before_switch: NonZeroU32,
    /// How long the active endpoint reports not ready before a switch.
    pub upstream_not_ready_before_switch: Duration,
    /// Cadence of the standby evaluation of every endpoint.
    pub standby_evaluation_interval: Duration,
    /// Longest the follower waits for a candidate's tip before skipping it.
    pub switch_check_timeout: Duration,
}

impl Default for NodeFailoverPolicy {
    fn default() -> Self {
        Self {
            transport_failures_before_switch: NonZeroU32::MIN.saturating_add(2),
            upstream_not_ready_before_switch: Duration::from_mins(5),
            standby_evaluation_interval: Duration::from_millis(
                zinder_source::DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS,
            ),
            switch_check_timeout: Duration::from_secs(5),
        }
    }
}

impl NodeFailoverPolicy {
    /// Returns whether a streak of follower recoveries switches endpoints.
    ///
    /// Only transport failures switch. A protocol mismatch, malformed
    /// payload, missing capability, or configuration failure stays on the
    /// endpoint: switching would pick whichever node's answer parses, and
    /// could hide a Zinder defect.
    #[must_use]
    pub fn switches_after_failures(
        self,
        failure_class: SourceFailureClass,
        consecutive_same_class: u32,
    ) -> bool {
        is_transport_failure(failure_class)
            && consecutive_same_class >= self.transport_failures_before_switch.get()
    }

    /// Returns whether the active endpoint has reported not ready for long
    /// enough to switch.
    #[must_use]
    pub fn switches_after_unready(self, not_ready_for: Option<Duration>) -> bool {
        not_ready_for.is_some_and(|duration| duration >= self.upstream_not_ready_before_switch)
    }

    /// Longest one endpoint's standby evaluation may take before the monitor
    /// records it as unreachable.
    fn evaluation_timeout(self) -> Duration {
        self.standby_evaluation_interval
            .max(self.switch_check_timeout)
    }

    /// Age past which a standby evaluation no longer qualifies a candidate.
    ///
    /// Two evaluations of an endpoint that keeps answering are at most one
    /// interval and two evaluation timeouts apart, whatever the other
    /// endpoints do, so this bound only expires evaluations of an endpoint
    /// that stopped answering.
    fn evaluation_max_age(self) -> Duration {
        self.evaluation_timeout().saturating_mul(4)
    }
}

const fn is_transport_failure(failure_class: SourceFailureClass) -> bool {
    matches!(
        failure_class,
        SourceFailureClass::NodeUnreachable | SourceFailureClass::StreamDisconnected
    )
}

/// Why an endpoint that answered cannot take over now.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IneligibleReason {
    /// The endpoint reports that it is not synchronized.
    NotReady,
    /// The endpoint's tip is below ingest's visible tip.
    Lagging,
    /// The endpoint's chain differs from ingest's at or below the settled
    /// tip, so following it would park the writer.
    DivergedAtOrBelowSettlement,
    /// The endpoint answered with evidence it cannot serve this store, such
    /// as pruned storage or another activation table.
    Refused(EndpointExclusion),
    /// The endpoint advertises other value-pool capabilities than the
    /// endpoint ingest started on, which would change ingest-control's
    /// advertised capabilities.
    CapabilityMismatch,
}

impl IneligibleReason {
    /// Stable label used in logs.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::NotReady => "not_ready",
            Self::Lagging => "lagging",
            Self::DivergedAtOrBelowSettlement => "diverged_at_or_below_settlement",
            Self::Refused(exclusion) => exclusion.label(),
            Self::CapabilityMismatch => "capability_mismatch",
        }
    }
}

/// Standing of one endpoint as the failover last observed it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NodeEndpointState {
    /// Ingest reads from this endpoint.
    Active,
    /// Not evaluated yet.
    Pending,
    /// Ready, at or past ingest's tip, and holding ingest's visible tip.
    Standby,
    /// Ready and at or past ingest's tip, on another branch above the settled
    /// tip. Chosen only when no [`Self::Standby`] endpoint exists.
    StandbyDiverged,
    /// Answered, but cannot take over now.
    Ineligible(IneligibleReason),
    /// Did not answer its last evaluation.
    Unreachable,
    /// Refused at startup until ingest restarts.
    Excluded(EndpointExclusion),
}

impl NodeEndpointState {
    /// Every metric label, in the order the state gauge reports them.
    pub const METRIC_LABELS: &'static [&'static str] = &[
        "active",
        "pending",
        "standby",
        "standby_diverged",
        "ineligible",
        "unreachable",
        "excluded",
    ];

    /// Label of the `state` dimension of `zinder_ingest_node_endpoint_state`.
    #[must_use]
    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Pending => "pending",
            Self::Standby => "standby",
            Self::StandbyDiverged => "standby_diverged",
            Self::Ineligible(_) => "ineligible",
            Self::Unreachable => "unreachable",
            Self::Excluded(_) => "excluded",
        }
    }
}

/// One completed switch of the active endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeEndpointSwitch {
    /// Position ingest read from before the switch.
    pub from_position: usize,
    /// Position ingest reads from after the switch.
    pub to_position: usize,
    /// Trigger label: `node_unreachable`, `stream_disconnected`, or
    /// `upstream_not_ready`.
    pub failure_class: &'static str,
    /// Whether the new endpoint holds another branch above the settled tip.
    /// Otherwise its final switch check found the visible tip block on it.
    pub diverged_above_settlement: bool,
}

/// Handle the canonical follower consults to switch node endpoints.
///
/// Cloning is cheap. Build it with [`NodeFailover::new`] and pass it in
/// [`crate::CanonicalFollowConfig::node_failover`].
#[derive(Clone)]
pub struct NodeFailover {
    shared: Arc<dyn EndpointSwitching>,
}

impl fmt::Debug for NodeFailover {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeFailover")
            .field("active_position", &self.active_position())
            .finish_non_exhaustive()
    }
}

impl NodeFailover {
    /// Builds the follower's failover handle and the standby monitor that
    /// feeds it, starting on the endpoint `admissions` selected.
    ///
    /// When `gates_node_source_input` is set, `[node.health]` gates readiness,
    /// so a switch resets the [`ReadinessInput::NodeSource`] input to
    /// `starting` until the probe observes the new endpoint.
    #[must_use]
    pub fn new<Endpoint>(
        nodes: NodeSet<Endpoint>,
        switch: NodeSetSwitch<Endpoint>,
        admissions: &NodeEndpointAdmissions,
        policy: NodeFailoverPolicy,
        readiness: FailoverReadiness,
    ) -> (Self, NodeEndpointMonitor<Endpoint>)
    where
        Endpoint: NodeSource,
    {
        // Position 0 is already active when `[node]` was admitted.
        let _switched = switch.switch_to(admissions.active_position());
        let records = admissions
            .verdicts()
            .map(|(position, verdict)| {
                EndpointRecord::from_admission(position, admissions, verdict)
            })
            .collect();
        let shared = Arc::new(FailoverShared {
            nodes,
            switch,
            policy,
            readiness,
            genesis_hash: admissions.genesis_hash(),
            value_pool_capabilities: admissions
                .active()
                .map(|active| ValuePoolCapabilities::of(active.capabilities))
                .unwrap_or_default(),
            board: Mutex::new(FailoverBoard {
                fence: None,
                records,
                evaluations_applied: 0,
                failed_switch_after: None,
            }),
        });
        shared.publish_active_position();
        (
            Self {
                shared: Arc::clone(&shared) as Arc<dyn EndpointSwitching>,
            },
            NodeEndpointMonitor::new(shared),
        )
    }

    /// Position ingest reads from now.
    #[must_use]
    pub fn active_position(&self) -> usize {
        self.shared.active_position()
    }

    /// Standing of every endpoint, in position order.
    #[must_use]
    pub fn endpoint_states(&self) -> Vec<NodeEndpointState> {
        self.shared.endpoint_states()
    }

    pub(crate) fn observe_fence(&self, local_tip: BlockId, settled_tip: BlockId) {
        self.shared.observe_fence(FenceTips {
            local_tip,
            settled_tip,
        });
    }

    pub(crate) async fn switch_after_failures(
        &self,
        failure_class: SourceFailureClass,
        consecutive_same_class: u32,
    ) -> Option<NodeEndpointSwitch> {
        if !self
            .shared
            .policy()
            .switches_after_failures(failure_class, consecutive_same_class)
        {
            return None;
        }
        self.shared.switch_away(failure_class.label()).await
    }

    pub(crate) async fn switch_after_unready(&self) -> Option<NodeEndpointSwitch> {
        if !self
            .shared
            .policy()
            .switches_after_unready(self.shared.active_not_ready_for())
        {
            return None;
        }
        self.shared.switch_away(UPSTREAM_NOT_READY_TRIGGER).await
    }
}

const UPSTREAM_NOT_READY_TRIGGER: &str = "upstream_not_ready";

/// Readiness handles a switch updates.
#[derive(Clone, Debug)]
pub struct FailoverReadiness {
    /// The writer's composed readiness.
    pub readiness: ComposedReadiness,
    /// Whether `[node.health]` gates readiness through
    /// [`ReadinessInput::NodeSource`].
    pub gates_node_source_input: bool,
}

/// Erases the endpoint type so the follower's configuration stays concrete.
#[async_trait]
trait EndpointSwitching: Send + Sync {
    fn policy(&self) -> NodeFailoverPolicy;
    fn active_position(&self) -> usize;
    fn endpoint_states(&self) -> Vec<NodeEndpointState>;
    fn observe_fence(&self, fence: FenceTips);
    fn active_not_ready_for(&self) -> Option<Duration>;
    async fn switch_away(&self, failure_class: &'static str) -> Option<NodeEndpointSwitch>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FenceTips {
    local_tip: BlockId,
    settled_tip: BlockId,
}

impl FenceTips {
    const fn of_store(store: &zinder_store::CanonicalReadyStoreSummary) -> Self {
        Self {
            local_tip: store.visible_tip(),
            settled_tip: store.settled_tip(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ValuePoolCapabilities {
    chain_value_pools: bool,
    block_value_pool_balances: bool,
}

impl ValuePoolCapabilities {
    fn of(capabilities: zinder_source::NodeCapabilities) -> Self {
        Self {
            chain_value_pools: capabilities
                .supports(zinder_source::NodeCapability::ChainValuePools),
            block_value_pool_balances: capabilities
                .supports(zinder_source::NodeCapability::BlockValuePoolBalances),
        }
    }
}

struct FailoverShared<Endpoint> {
    nodes: NodeSet<Endpoint>,
    switch: NodeSetSwitch<Endpoint>,
    policy: NodeFailoverPolicy,
    readiness: FailoverReadiness,
    genesis_hash: BlockHash,
    value_pool_capabilities: ValuePoolCapabilities,
    board: Mutex<FailoverBoard>,
}

struct FailoverBoard {
    fence: Option<FenceTips>,
    records: Vec<EndpointRecord>,
    /// Standby evaluations the monitor has applied since ingest started.
    evaluations_applied: u64,
    /// `evaluations_applied` when the last switch attempt failed. Until the
    /// monitor applies another evaluation, the board cannot offer a better
    /// candidate, so a follower looping at its tip neither repeats the
    /// attempt nor logs it again.
    failed_switch_after: Option<u64>,
}

#[derive(Clone, Debug)]
struct EndpointRecord {
    excluded: Option<EndpointExclusion>,
    state: NodeEndpointState,
    evaluation: Option<CandidateEvaluation>,
    not_ready_since: Option<Instant>,
}

impl EndpointRecord {
    fn from_admission(
        position: usize,
        admissions: &NodeEndpointAdmissions,
        verdict: &EndpointAdmission,
    ) -> Self {
        let (excluded, state) = match verdict {
            EndpointAdmission::Excluded(exclusion) => {
                (Some(*exclusion), NodeEndpointState::Excluded(*exclusion))
            }
            EndpointAdmission::Admitted(_) if position == admissions.active_position() => {
                (None, NodeEndpointState::Active)
            }
            EndpointAdmission::Admitted(_) | EndpointAdmission::Deferred => {
                (None, NodeEndpointState::Pending)
            }
            EndpointAdmission::Unreachable(_) => (None, NodeEndpointState::Unreachable),
            EndpointAdmission::Ineligible(reason) => (None, NodeEndpointState::Ineligible(*reason)),
        };
        Self {
            excluded,
            state,
            evaluation: None,
            not_ready_since: None,
        }
    }
}

/// Standby evidence that a candidate can take over.
#[derive(Clone, Copy, Debug)]
struct CandidateEvaluation {
    diverged: bool,
    observed_at: Instant,
}

impl<Endpoint> FailoverShared<Endpoint>
where
    Endpoint: NodeSource,
{
    fn publish_active_position(&self) {
        let active_position = self.nodes.active_position();
        self.readiness
            .readiness
            .runtime_readiness()
            .set_active_node(Some(ActiveNodeEndpoint {
                position: u32::try_from(active_position).unwrap_or(u32::MAX),
            }));
        for (position, _) in self.nodes.endpoints() {
            metrics::gauge!(
                "zinder_ingest_node_endpoint_active",
                "position" => position.to_string()
            )
            .set(if position == active_position {
                1.0
            } else {
                0.0
            });
        }
        let mut board = self.board.lock();
        board.failed_switch_after = None;
        for (position, record) in board.records.iter_mut().enumerate() {
            if position == active_position {
                record.state = NodeEndpointState::Active;
                record.evaluation = None;
                record.not_ready_since = None;
            } else if record.state == NodeEndpointState::Active {
                record.state = NodeEndpointState::Pending;
                record.not_ready_since = None;
            }
        }
        record_state_metrics(&board.records);
    }

    /// Candidates in the order a switch tries them: endpoints holding the
    /// local tip first, then diverged ones, each in position order.
    fn ranked_candidates(&self) -> Vec<(usize, CandidateEvaluation)> {
        let active_position = self.nodes.active_position();
        let max_age = self.policy.evaluation_max_age();
        let mut candidates = self
            .board
            .lock()
            .records
            .iter()
            .enumerate()
            .filter(|(position, record)| {
                *position != active_position
                    && record.excluded.is_none()
                    && matches!(
                        record.state,
                        NodeEndpointState::Standby | NodeEndpointState::StandbyDiverged
                    )
            })
            .filter_map(|(position, record)| {
                record
                    .evaluation
                    .filter(|evaluation| evaluation.observed_at.elapsed() <= max_age)
                    .map(|evaluation| (position, evaluation))
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(position, evaluation)| (evaluation.diverged, *position));
        candidates
    }

    /// Rechecks a candidate within one bounded wait: it must still report
    /// ready, its tip must still reach the visible tip, and the block it
    /// holds at the visible tip, or at the settled tip for a diverged
    /// candidate, must still match.
    async fn confirm_candidate(&self, position: usize, evaluation: CandidateEvaluation) -> bool {
        let Some(endpoint) = self.nodes.endpoint(position) else {
            return false;
        };
        let Some(fence) = self.board.lock().fence else {
            return false;
        };
        let check = async {
            if !monitor::readiness_of(endpoint).await? {
                return Ok(Some(IneligibleReason::NotReady.label()));
            }
            let tip = endpoint.tip_id().await?;
            if tip.height < fence.local_tip.height {
                return Ok(Some(IneligibleReason::Lagging.label()));
            }
            let expected = if evaluation.diverged {
                fence.settled_tip
            } else {
                fence.local_tip
            };
            let held = endpoint.fetch_block_at(expected.height).await?;
            Ok::<_, zinder_source::SourceError>(
                (held.hash != expected.hash).then_some("chain_changed_since_evaluation"),
            )
        };
        match tokio::time::timeout(self.policy.switch_check_timeout, check).await {
            Ok(Ok(None)) => true,
            Ok(Ok(Some(reason))) => {
                log_skipped_candidate(position, reason);
                false
            }
            Ok(Err(error)) => {
                tracing::warn!(
                    target: "zinder::ingest",
                    event = "node_endpoint_skipped",
                    position,
                    reason = "unreachable",
                    %error,
                    "node endpoint did not answer the final switch check"
                );
                false
            }
            Err(_elapsed) => {
                log_skipped_candidate(position, "switch_check_timeout");
                false
            }
        }
    }

    fn complete_switch(
        &self,
        from_position: usize,
        to_position: usize,
        failure_class: &'static str,
        diverged: bool,
    ) -> NodeEndpointSwitch {
        if self.readiness.gates_node_source_input {
            self.readiness
                .readiness
                .publish_input(ReadinessInput::NodeSource, ReadinessCause::Starting);
        }
        self.publish_active_position();
        metrics::counter!(
            "zinder_ingest_node_endpoint_switches_total",
            "from_position" => from_position.to_string(),
            "to_position" => to_position.to_string(),
            "failure_class" => failure_class
        )
        .increment(1);
        tracing::warn!(
            target: "zinder::ingest",
            event = "node_endpoint_switched",
            from_position,
            to_position,
            failure_class,
            diverged_above_settlement = diverged,
            "canonical follower switched node endpoints between follow iterations"
        );
        NodeEndpointSwitch {
            from_position,
            to_position,
            failure_class,
            diverged_above_settlement: diverged,
        }
    }
}

#[async_trait]
impl<Endpoint> EndpointSwitching for FailoverShared<Endpoint>
where
    Endpoint: NodeSource,
{
    fn policy(&self) -> NodeFailoverPolicy {
        self.policy
    }

    fn active_position(&self) -> usize {
        self.nodes.active_position()
    }

    fn endpoint_states(&self) -> Vec<NodeEndpointState> {
        self.board
            .lock()
            .records
            .iter()
            .map(|record| record.state)
            .collect()
    }

    fn observe_fence(&self, fence: FenceTips) {
        self.board.lock().fence = Some(fence);
    }

    fn active_not_ready_for(&self) -> Option<Duration> {
        let active_position = self.nodes.active_position();
        self.board
            .lock()
            .records
            .get(active_position)
            .and_then(|record| record.not_ready_since)
            .map(|since| since.elapsed())
    }

    async fn switch_away(&self, failure_class: &'static str) -> Option<NodeEndpointSwitch> {
        let from_position = self.nodes.active_position();
        let evaluations_applied = {
            let board = self.board.lock();
            if board.failed_switch_after == Some(board.evaluations_applied) {
                return None;
            }
            board.evaluations_applied
        };
        let candidates = self.ranked_candidates();
        if candidates.is_empty() {
            tracing::warn!(
                target: "zinder::ingest",
                event = "node_endpoint_switch_unavailable",
                active_position = from_position,
                failure_class,
                "no standby node endpoint can take over; the follower stays on its endpoint"
            );
        }
        // Every candidate is rechecked at once, so the attempt takes one
        // switch-check bound however many candidates hang, and the first
        // candidate in rank order that passes is chosen as soon as it does.
        let mut confirmations = candidates
            .into_iter()
            .map(|(position, evaluation)| async move {
                let confirmed = self.confirm_candidate(position, evaluation).await;
                (position, evaluation, confirmed)
            })
            .collect::<FuturesOrdered<_>>();
        while let Some((position, evaluation, confirmed)) = confirmations.next().await {
            if confirmed && self.switch.switch_to(position) {
                return Some(self.complete_switch(
                    from_position,
                    position,
                    failure_class,
                    evaluation.diverged,
                ));
            }
        }
        self.board.lock().failed_switch_after = Some(evaluations_applied);
        None
    }
}

fn log_skipped_candidate(position: usize, reason: &'static str) {
    tracing::info!(
        target: "zinder::ingest",
        event = "node_endpoint_skipped",
        position,
        reason,
        "node endpoint cannot take over now"
    );
}

fn record_state_metrics(records: &[EndpointRecord]) {
    for (position, record) in records.iter().enumerate() {
        let active_label = record.state.metric_label();
        for label in NodeEndpointState::METRIC_LABELS {
            metrics::gauge!(
                "zinder_ingest_node_endpoint_state",
                "position" => position.to_string(),
                "state" => *label
            )
            .set(if *label == active_label { 1.0 } else { 0.0 });
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use zinder_core::{BlockHeight, Network};
    use zinder_runtime::Readiness;
    use zinder_source::{
        NodeCapabilities, SourceBlock, SourceError, UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT,
        UpstreamHealthSnapshot,
    };

    use super::*;

    /// The visible tip every test endpoint holds.
    fn visible_tip() -> BlockId {
        BlockId::new(BlockHeight::new(4), BlockHash::from_bytes([4; 32]))
    }

    /// A candidate that holds [`visible_tip`] and answers, or not, as set.
    #[derive(Clone, Copy)]
    struct CandidateEndpoint {
        ready: bool,
        hangs: bool,
    }

    impl CandidateEndpoint {
        const READY: Self = Self {
            ready: true,
            hangs: false,
        };

        async fn answer(self) {
            if self.hangs {
                std::future::pending::<()>().await;
            }
        }
    }

    #[async_trait]
    impl NodeSource for CandidateEndpoint {
        fn capabilities(&self) -> NodeCapabilities {
            NodeCapabilities::default()
        }

        async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
            self.answer().await;
            Ok(SourceBlock {
                network: Network::ZcashRegtest,
                height,
                hash: visible_tip().hash,
                parent_hash: BlockHash::from_bytes([3; 32]),
                block_time_seconds: 0,
                raw_block_bytes: Vec::new(),
            })
        }

        async fn tip_id(&self) -> Result<BlockId, SourceError> {
            self.answer().await;
            Ok(visible_tip())
        }

        async fn poll_upstream_health(&self) -> Result<UpstreamHealthSnapshot, SourceError> {
            self.answer().await;
            Ok(if self.ready {
                UpstreamHealthSnapshot::ready(
                    UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT,
                    None,
                    None,
                    None,
                )
            } else {
                UpstreamHealthSnapshot::not_ready(
                    UPSTREAM_HEALTH_SOURCE_ZEBRA_READY_ENDPOINT,
                    "syncing",
                    None,
                    None,
                    None,
                )
            })
        }
    }

    /// Failover state whose standby evaluations, recorded just now, found
    /// every endpoint after `[node]` holding the visible tip.
    fn failover_with_standbys(
        active: CandidateEndpoint,
        standbys: Vec<CandidateEndpoint>,
        policy: NodeFailoverPolicy,
    ) -> FailoverShared<CandidateEndpoint> {
        let records = std::iter::once(NodeEndpointState::Active)
            .chain(standbys.iter().map(|_| NodeEndpointState::Standby))
            .map(|state| EndpointRecord {
                excluded: None,
                state,
                evaluation: (state == NodeEndpointState::Standby).then(|| CandidateEvaluation {
                    diverged: false,
                    observed_at: Instant::now(),
                }),
                not_ready_since: None,
            })
            .collect();
        let (nodes, switch) = NodeSet::new(active, standbys);
        FailoverShared {
            nodes,
            switch,
            policy,
            readiness: FailoverReadiness {
                readiness: ComposedReadiness::new(Readiness::default(), &[]),
                gates_node_source_input: false,
            },
            genesis_hash: Network::ZcashRegtest.genesis_hash(),
            value_pool_capabilities: ValuePoolCapabilities::default(),
            board: Mutex::new(FailoverBoard {
                fence: Some(FenceTips {
                    local_tip: visible_tip(),
                    settled_tip: visible_tip(),
                }),
                records,
                evaluations_applied: 1,
                failed_switch_after: None,
            }),
        }
    }

    /// Candidates that hang during the final check cost one switch-check
    /// bound together, not one each, so the follower's control loop waits
    /// at most that bound.
    #[tokio::test]
    async fn hanging_candidates_share_one_switch_check_bound() {
        let hanging = CandidateEndpoint {
            hangs: true,
            ..CandidateEndpoint::READY
        };
        let switch_check_timeout = Duration::from_millis(300);
        let failover = failover_with_standbys(
            CandidateEndpoint::READY,
            vec![hanging, hanging, CandidateEndpoint::READY],
            NodeFailoverPolicy {
                switch_check_timeout,
                ..NodeFailoverPolicy::default()
            },
        );

        let started_at = Instant::now();
        let switch = failover.switch_away("node_unreachable").await;
        let elapsed = started_at.elapsed();

        assert_eq!(switch.map(|switch| switch.to_position), Some(3));
        assert!(
            elapsed < switch_check_timeout.saturating_mul(2),
            "the switch attempt took {elapsed:?}"
        );
    }

    /// A standby evaluated ready that reports not ready by the time a switch
    /// rechecks it is skipped for the next candidate.
    #[tokio::test]
    async fn a_switch_skips_a_standby_that_stopped_reporting_ready() {
        let syncing = CandidateEndpoint {
            ready: false,
            ..CandidateEndpoint::READY
        };
        let failover = failover_with_standbys(
            CandidateEndpoint::READY,
            vec![syncing, CandidateEndpoint::READY],
            NodeFailoverPolicy::default(),
        );

        let switch = failover.switch_away("node_unreachable").await;

        assert_eq!(switch.map(|switch| switch.to_position), Some(2));
    }

    #[test]
    fn transport_failures_switch_at_the_threshold_and_not_before() {
        let policy = NodeFailoverPolicy::default();
        for class in [
            SourceFailureClass::NodeUnreachable,
            SourceFailureClass::StreamDisconnected,
        ] {
            assert!(!policy.switches_after_failures(class, 2));
            assert!(policy.switches_after_failures(class, 3));
            assert!(policy.switches_after_failures(class, 4));
        }
    }

    #[test]
    fn other_failure_classes_never_switch() {
        let policy = NodeFailoverPolicy::default();
        for class in [
            SourceFailureClass::UpstreamViewChanged,
            SourceFailureClass::ProtocolMismatch,
            SourceFailureClass::Malformed,
            SourceFailureClass::CapabilityMissing,
            SourceFailureClass::Configuration,
        ] {
            assert!(
                !policy.switches_after_failures(class, u32::MAX),
                "{class:?}"
            );
        }
    }

    #[test]
    fn evaluations_of_an_answering_endpoint_never_expire_between_rounds() {
        for (interval_ms, check_timeout_ms) in [(30_000, 5_000), (1_000, 5_000), (20, 1_000)] {
            let policy = NodeFailoverPolicy {
                standby_evaluation_interval: Duration::from_millis(interval_ms),
                switch_check_timeout: Duration::from_millis(check_timeout_ms),
                ..NodeFailoverPolicy::default()
            };
            let longest_gap = policy
                .standby_evaluation_interval
                .saturating_add(policy.evaluation_timeout().saturating_mul(2));
            assert!(policy.evaluation_max_age() > longest_gap, "{policy:?}");
        }
    }

    #[test]
    fn a_short_unready_flap_does_not_switch() {
        let policy = NodeFailoverPolicy::default();
        assert!(!policy.switches_after_unready(None));
        assert!(!policy.switches_after_unready(Some(Duration::from_mins(2))));
        assert!(policy.switches_after_unready(Some(Duration::from_mins(5))));
    }
}
