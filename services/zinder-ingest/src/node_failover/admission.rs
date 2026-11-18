//! Startup admission of every endpoint in the ordered node list.

use std::{fmt, sync::Arc};

use futures_util::{StreamExt as _, stream::FuturesUnordered};
use thiserror::Error;
use zinder_core::{BlockHash, BlockHeight, Network, NetworkUpgradeActivations};
use zinder_source::{
    NodeCapabilities, NodeCapability, NodeSet, NodeSource, NodeStorageMode, SourceError,
};
use zinder_store::CanonicalReadyStoreSummary;

use super::{
    FenceTips, IneligibleReason,
    monitor::{ChainStandingError, chain_standing, readiness_of},
};

/// Capabilities every node endpoint ingest reads from must advertise.
pub const REQUIRED_INGEST_NODE_CAPABILITIES: &[NodeCapability] = &[
    NodeCapability::JsonRpc,
    NodeCapability::BestChainBlocks,
    NodeCapability::TipId,
    NodeCapability::TreeState,
    NodeCapability::SubtreeRoots,
    NodeCapability::HistoricalBlockBodies,
];

/// Positive evidence that refuses an endpoint until ingest restarts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EndpointExclusion {
    /// The node reports pruned block storage.
    Pruned,
    /// The node answered capability discovery without a capability ingestion
    /// requires.
    CapabilityMissing(NodeCapability),
    /// The node's block 0 is not the configured network's genesis block.
    GenesisMismatch,
    /// The node advertises an activation table other than the active
    /// endpoint's, or a fallback that would start ingest advertises one other
    /// than the table the store follows.
    ScheduleConflict,
}

impl EndpointExclusion {
    /// Stable label used in logs, metrics, and `/readyz`-adjacent diagnostics.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Pruned => "pruned",
            Self::CapabilityMissing(_) => "capability_missing",
            Self::GenesisMismatch => "genesis_mismatch",
            Self::ScheduleConflict => "schedule_conflict",
        }
    }
}

impl fmt::Display for EndpointExclusion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pruned => formatter.write_str("the node reports pruned block storage"),
            Self::CapabilityMissing(capability) => {
                write!(formatter, "the node does not advertise {capability}")
            }
            Self::GenesisMismatch => formatter
                .write_str("the node's block 0 is not the configured network's genesis block"),
            Self::ScheduleConflict => formatter.write_str(
                "the node's network upgrade activations differ from the active endpoint's or the store's admitted schedule",
            ),
        }
    }
}

/// An endpoint that passed every startup check.
#[derive(Clone, Debug)]
pub struct AdmittedEndpoint {
    /// Capabilities the endpoint advertised.
    pub capabilities: NodeCapabilities,
    /// Activation table the endpoint advertised.
    pub activations: NetworkUpgradeActivations,
}

/// Startup verdict for one endpoint.
#[derive(Debug)]
#[non_exhaustive]
pub enum EndpointAdmission {
    /// The endpoint passed every check.
    Admitted(AdmittedEndpoint),
    /// A check failed without evidence against the endpoint, such as a
    /// timeout or a storage mode it did not report. The endpoint is
    /// evaluated again while ingest runs.
    Unreachable(SourceError),
    /// The endpoint answered with evidence that it cannot serve this store.
    Excluded(EndpointExclusion),
    /// A fallback that passed every endpoint check cannot start ingest on
    /// the store now, for example because its chain differs from the store's
    /// at or below the settled tip. The endpoint is evaluated again while
    /// ingest runs.
    Ineligible(IneligibleReason),
    /// The endpoint was still being checked when a lower position was
    /// admitted. The standby monitor checks it, its genesis block included.
    Deferred,
}

impl EndpointAdmission {
    /// Checks one endpoint the way ingest checks it before reading from it.
    ///
    /// `genesis_hash` is checked only when supplied: a single-endpoint
    /// deployment keeps its startup reads unchanged.
    pub async fn check<Source>(endpoint: &Source, genesis_hash: Option<BlockHash>) -> Self
    where
        Source: NodeSource + ?Sized,
    {
        let capabilities = match endpoint.probe_capabilities().await {
            Ok(capabilities) => capabilities,
            Err(error) => return Self::Unreachable(error),
        };
        if let Some(missing) = missing_required_capability(capabilities) {
            return Self::missing_capability(endpoint, missing).await;
        }
        let activations = match endpoint.fetch_network_upgrade_activations().await {
            Ok(activations) => activations,
            Err(error) => return Self::Unreachable(error),
        };
        if let Some(genesis_hash) = genesis_hash {
            match endpoint.fetch_block_at(BlockHeight::new(0)).await {
                Ok(genesis) if genesis.hash == genesis_hash => {}
                Ok(_) => return Self::Excluded(EndpointExclusion::GenesisMismatch),
                Err(error) => return Self::Unreachable(error),
            }
        }
        Self::Admitted(AdmittedEndpoint {
            capabilities,
            activations,
        })
    }

    async fn missing_capability<Source>(endpoint: &Source, missing: NodeCapability) -> Self
    where
        Source: NodeSource + ?Sized,
    {
        if missing != NodeCapability::HistoricalBlockBodies {
            return Self::Excluded(EndpointExclusion::CapabilityMissing(missing));
        }
        // Capability discovery withholds archive storage whenever the
        // storage read fails, so only a node that reports `pruned: true` is
        // evidence against the endpoint.
        match endpoint.fetch_storage_mode().await {
            Ok(NodeStorageMode::Pruned) => Self::Excluded(EndpointExclusion::Pruned),
            Ok(_) => Self::Unreachable(SourceError::NodeCapabilityMissing {
                capability: NodeCapability::HistoricalBlockBodies,
            }),
            Err(error) => Self::Unreachable(error),
        }
    }

    /// Checks that a fallback admitted by [`Self::check`] could take over
    /// the READY `store` as a switch target could: it reports ready, its tip
    /// is at or above the store's visible tip, and it holds the visible tip
    /// or, on another branch above the settled tip, the settled tip.
    ///
    /// A fallback that fails is never followed, because a chain that differs
    /// at or below the settled tip would park the writer.
    async fn check_takeover<Source>(
        self,
        endpoint: &Source,
        store: &CanonicalReadyStoreSummary,
    ) -> Self
    where
        Source: NodeSource + ?Sized,
    {
        let Self::Admitted(admitted) = self else {
            return self;
        };
        match readiness_of(endpoint).await {
            Ok(true) => {}
            Ok(false) => return Self::Ineligible(IneligibleReason::NotReady),
            Err(error) => return Self::Unreachable(error),
        }
        match chain_standing(endpoint, FenceTips::of_store(store)).await {
            Ok(Some(_)) => Self::Admitted(admitted),
            Ok(None) => Self::Ineligible(IneligibleReason::Lagging),
            Err(ChainStandingError::DivergedAtOrBelowSettlement) => {
                Self::Ineligible(IneligibleReason::DivergedAtOrBelowSettlement)
            }
            Err(ChainStandingError::Unreachable(error)) => Self::Unreachable(error),
        }
    }

    /// Stable label of the verdict.
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Admitted(_) => "admitted",
            Self::Unreachable(_) => "unreachable",
            Self::Excluded(exclusion) => exclusion.label(),
            Self::Ineligible(reason) => reason.label(),
            Self::Deferred => "deferred",
        }
    }
}

/// Returns the first required ingest capability `capabilities` lacks.
#[must_use]
pub fn missing_required_capability(capabilities: NodeCapabilities) -> Option<NodeCapability> {
    REQUIRED_INGEST_NODE_CAPABILITIES
        .iter()
        .copied()
        .find(|required| !capabilities.supports(*required))
}

/// Startup verdicts of every endpoint and the endpoint ingest reads from.
#[derive(Debug)]
pub struct NodeEndpointAdmissions {
    active_position: usize,
    active_activations: Arc<NetworkUpgradeActivations>,
    genesis_hash: BlockHash,
    verdicts: Vec<EndpointAdmission>,
}

impl NodeEndpointAdmissions {
    /// Checks every endpoint concurrently and selects the first admitted one
    /// in position order.
    ///
    /// `store` is the READY store the writer will open, if any; see
    /// [`crate::read_ready_store_summary`]. A fallback that would become
    /// active with a table other than the one the store follows is excluded
    /// with [`EndpointExclusion::ScheduleConflict`], so only `[node]` can
    /// introduce a new revision, the first one included, and a fallback never
    /// rewrites the schedule while `[node]` is merely slow to answer. The
    /// comparison waits until every lower position has a verdict, so a
    /// fallback that already advertises the table `[node]` brings is not
    /// refused for answering first.
    ///
    /// Admission returns as soon as every position below the first admitted
    /// one has a verdict, so a fallback that does not answer never delays a
    /// healthy `[node]`. Endpoints still being checked then are
    /// [`EndpointAdmission::Deferred`]. Another admitted endpoint whose
    /// activation table differs from the selected one is excluded with
    /// [`EndpointExclusion::ScheduleConflict`].
    pub async fn admit<Endpoint>(
        nodes: &NodeSet<Endpoint>,
        network: Network,
        store: Option<&CanonicalReadyStoreSummary>,
    ) -> Result<Self, NoNodeEndpointAdmitted>
    where
        Endpoint: NodeSource,
    {
        let genesis_hash = network.genesis_hash();
        let mut checks = nodes
            .endpoints()
            .map(|(position, endpoint)| async move {
                let verdict = EndpointAdmission::check(endpoint, Some(genesis_hash)).await;
                let verdict = match store {
                    Some(store) if position > 0 => verdict.check_takeover(endpoint, store).await,
                    Some(_) | None => verdict,
                };
                (position, verdict)
            })
            .collect::<FuturesUnordered<_>>();
        let mut decided = std::iter::repeat_with(|| None)
            .take(nodes.endpoint_count())
            .collect::<Vec<Option<EndpointAdmission>>>();
        let active_position = loop {
            if let Some(position) = settled_active_position(&decided) {
                let Some(slot) = decided.get_mut(position) else {
                    break None;
                };
                if starts_on_another_schedule(position, slot.as_ref(), store) {
                    *slot = Some(EndpointAdmission::Excluded(
                        EndpointExclusion::ScheduleConflict,
                    ));
                    continue;
                }
                break Some(position);
            }
            let Some((position, verdict)) = checks.next().await else {
                break None;
            };
            if let Some(slot) = decided.get_mut(position) {
                *slot = Some(verdict);
            }
        };
        drop(checks);
        let mut verdicts = decided
            .into_iter()
            .map(|verdict| verdict.unwrap_or(EndpointAdmission::Deferred))
            .collect::<Vec<_>>();
        let Some(active_position) = active_position else {
            return Err(NoNodeEndpointAdmitted { verdicts });
        };
        let active_activations = match verdicts.get(active_position) {
            Some(EndpointAdmission::Admitted(admitted)) => Arc::new(admitted.activations.clone()),
            _ => return Err(NoNodeEndpointAdmitted { verdicts }),
        };
        for verdict in &mut verdicts {
            if let EndpointAdmission::Admitted(admitted) = verdict
                && admitted.activations != *active_activations
            {
                *verdict = EndpointAdmission::Excluded(EndpointExclusion::ScheduleConflict);
            }
        }
        Ok(Self {
            active_position,
            active_activations,
            genesis_hash,
            verdicts,
        })
    }

    /// Position of the endpoint ingest reads from first.
    #[must_use]
    pub const fn active_position(&self) -> usize {
        self.active_position
    }

    /// Activation table the active endpoint advertised during admission.
    ///
    /// Admission compared this table with the store's, so the writer opens
    /// the store with it instead of reading the table again: a later read
    /// could return a table no check has seen.
    #[must_use]
    pub fn active_activations(&self) -> Arc<NetworkUpgradeActivations> {
        Arc::clone(&self.active_activations)
    }

    /// The endpoint ingest reads from first.
    #[must_use]
    pub fn active(&self) -> Option<&AdmittedEndpoint> {
        match self.verdicts.get(self.active_position) {
            Some(EndpointAdmission::Admitted(admitted)) => Some(admitted),
            _ => None,
        }
    }

    /// Every verdict with its position, in position order.
    pub fn verdicts(&self) -> impl Iterator<Item = (usize, &EndpointAdmission)> {
        self.verdicts.iter().enumerate()
    }

    /// Genesis hash of the configured network.
    pub(super) const fn genesis_hash(&self) -> BlockHash {
        self.genesis_hash
    }
}

/// Returns whether an admitted fallback would start the writer with a table
/// other than the one the store follows.
///
/// `[node]` may still bring a new table, which the store admits only as a
/// future-only revision.
fn starts_on_another_schedule(
    position: usize,
    verdict: Option<&EndpointAdmission>,
    store: Option<&CanonicalReadyStoreSummary>,
) -> bool {
    match (verdict, store) {
        (Some(EndpointAdmission::Admitted(admitted)), Some(store)) => {
            position > 0 && !store.follows_activations(&admitted.activations)
        }
        _ => false,
    }
}

/// Returns the first admitted position once every lower position has a
/// verdict.
fn settled_active_position(decided: &[Option<EndpointAdmission>]) -> Option<usize> {
    for (position, verdict) in decided.iter().enumerate() {
        match verdict {
            None => return None,
            Some(EndpointAdmission::Admitted(_)) => return Some(position),
            Some(
                EndpointAdmission::Unreachable(_)
                | EndpointAdmission::Excluded(_)
                | EndpointAdmission::Ineligible(_)
                | EndpointAdmission::Deferred,
            ) => {}
        }
    }
    None
}

/// No configured node endpoint passed its startup checks.
#[derive(Debug, Error)]
#[error("no configured node endpoint was admitted: {}", summarize(.verdicts))]
pub struct NoNodeEndpointAdmitted {
    verdicts: Vec<EndpointAdmission>,
}

impl NoNodeEndpointAdmitted {
    /// The verdict of `[node]`, position 0.
    #[must_use]
    pub fn primary(&self) -> Option<&EndpointAdmission> {
        self.verdicts.first()
    }
}

fn summarize(verdicts: &[EndpointAdmission]) -> String {
    verdicts
        .iter()
        .enumerate()
        .map(|(position, verdict)| match verdict {
            EndpointAdmission::Admitted(_) => format!("position {position}: admitted"),
            EndpointAdmission::Unreachable(error) => {
                format!("position {position}: unreachable ({error})")
            }
            EndpointAdmission::Excluded(exclusion) => {
                format!("position {position}: excluded ({exclusion})")
            }
            EndpointAdmission::Ineligible(reason) => {
                format!("position {position}: ineligible ({})", reason.label())
            }
            EndpointAdmission::Deferred => format!("position {position}: deferred"),
        })
        .collect::<Vec<_>>()
        .join("; ")
}
