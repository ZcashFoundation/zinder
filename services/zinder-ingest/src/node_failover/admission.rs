//! Startup admission of every endpoint in the ordered node list.

use std::fmt;

use futures_util::future::join_all;
use thiserror::Error;
use zinder_core::{BlockHash, BlockHeight, Network, NetworkUpgradeActivations};
use zinder_source::{
    NodeCapabilities, NodeCapability, NodeSet, NodeSource, NodeStorageMode, SourceError,
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
    /// endpoint's.
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
                "the node's network upgrade activations differ from the active endpoint's",
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

    /// Stable label of the verdict.
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Admitted(_) => "admitted",
            Self::Unreachable(_) => "unreachable",
            Self::Excluded(exclusion) => exclusion.label(),
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
    verdicts: Vec<EndpointAdmission>,
}

impl NodeEndpointAdmissions {
    /// Checks every endpoint concurrently and selects the first admitted one
    /// in position order.
    ///
    /// Another admitted endpoint whose activation table differs from the
    /// selected one is excluded with [`EndpointExclusion::ScheduleConflict`].
    pub async fn admit<Endpoint>(
        nodes: &NodeSet<Endpoint>,
        network: Network,
    ) -> Result<Self, NoNodeEndpointAdmitted>
    where
        Endpoint: NodeSource,
    {
        let genesis_hash = Some(network.genesis_hash());
        let mut verdicts = join_all(
            nodes
                .endpoints()
                .map(|(_, endpoint)| EndpointAdmission::check(endpoint, genesis_hash)),
        )
        .await;
        let Some(active_position) = verdicts
            .iter()
            .position(|verdict| matches!(verdict, EndpointAdmission::Admitted(_)))
        else {
            return Err(NoNodeEndpointAdmitted { verdicts });
        };
        let active_activations = match verdicts.get(active_position) {
            Some(EndpointAdmission::Admitted(admitted)) => admitted.activations.clone(),
            _ => return Err(NoNodeEndpointAdmitted { verdicts }),
        };
        for verdict in &mut verdicts {
            if let EndpointAdmission::Admitted(admitted) = verdict
                && admitted.activations != active_activations
            {
                *verdict = EndpointAdmission::Excluded(EndpointExclusion::ScheduleConflict);
            }
        }
        Ok(Self {
            active_position,
            verdicts,
        })
    }

    /// Position of the endpoint ingest reads from first.
    #[must_use]
    pub const fn active_position(&self) -> usize {
        self.active_position
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
        })
        .collect::<Vec<_>>()
        .join("; ")
}
