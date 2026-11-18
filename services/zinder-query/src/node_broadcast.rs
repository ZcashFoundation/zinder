//! Transaction broadcast through a node endpoint verified on first contact.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::OnceCell;
use zinder_core::{RawTransactionBytes, TransactionBroadcastOutcome};
use zinder_source::{BroadcastNodeProbe, NodeCapability, SourceError, TransactionBroadcaster};

use crate::{UpstreamNodeCapabilities, WalletServingPairSlot};

/// The configured node endpoint a wallet query broadcasts transactions
/// through.
///
/// A reader starts without contacting its node, so the endpoint is verified
/// on first contact instead: `rpc.discover` must list `sendrawtransaction`,
/// the node's activation table must agree with the writer-admitted schedule
/// of the serving pair through its visible tip, and the node's best chain
/// must hold the pair's visible tip block. A node on another network fails
/// the table check, and a node on another chain with the same schedule fails
/// the block check. Until a verification passes, every broadcast fails with
/// [`SourceError::NodeUnavailable`] and no transaction bytes reach the
/// endpoint. A passed verification holds for the life of the process, and
/// clones share it.
#[derive(Clone)]
pub struct NodeBroadcast<Source> {
    source: Source,
    verification: NodeVerification,
}

impl<Source> NodeBroadcast<Source> {
    /// Wraps the configured endpoint, unverified.
    #[must_use]
    pub fn new(source: Source) -> Self {
        Self {
            source,
            verification: NodeVerification::default(),
        }
    }

    /// Returns the configured endpoint.
    #[must_use]
    pub const fn source(&self) -> &Source {
        &self.source
    }

    /// Returns the capabilities the endpoint advertised when it passed
    /// verification, or `None` while it is unverified.
    #[must_use]
    pub fn verified_capabilities(&self) -> Option<&UpstreamNodeCapabilities> {
        self.verification.verified_capabilities()
    }

    pub(crate) fn verification(&self) -> NodeVerification {
        self.verification.clone()
    }
}

impl<Source> NodeBroadcast<Source>
where
    Source: BroadcastNodeProbe,
{
    /// Verifies the endpoint against the pair `pair_slot` currently serves,
    /// unless an earlier verification already passed.
    ///
    /// Concurrent calls share one verification. A failed verification leaves
    /// the endpoint unverified, so a later call tries again.
    ///
    /// # Errors
    ///
    /// Returns the node's transport or protocol failure,
    /// [`SourceError::NodeCapabilityMissing`] when the node does not list
    /// `sendrawtransaction`, [`SourceError::BlockUnavailable`] when the node
    /// has not reached the visible tip, or
    /// [`SourceError::SourceProtocolMismatch`] when its activation table
    /// differs from the writer-admitted schedule at or below the visible tip
    /// or its best chain holds another block at the visible tip height.
    pub async fn verify(
        &self,
        pair_slot: &WalletServingPairSlot,
    ) -> Result<&UpstreamNodeCapabilities, SourceError> {
        self.verification
            .verified
            .get_or_try_init(|| verify_node_endpoint(&self.source, pair_slot))
            .await
    }
}

#[async_trait]
impl<Source> TransactionBroadcaster for NodeBroadcast<Source>
where
    Source: TransactionBroadcaster,
{
    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, SourceError> {
        if self.verification.verified_capabilities().is_none() {
            return Err(SourceError::NodeUnavailable {
                reason: "the node endpoint has not passed first-contact verification; \
                         the transaction was not sent"
                    .to_owned(),
            });
        }
        self.source.broadcast_transaction(raw_transaction).await
    }
}

/// First-contact verification of one node endpoint, shared by its clones and
/// by the query that reports the verified endpoint's capabilities.
#[derive(Clone, Default)]
pub(crate) struct NodeVerification {
    verified: Arc<OnceCell<UpstreamNodeCapabilities>>,
}

impl NodeVerification {
    pub(crate) fn verified_capabilities(&self) -> Option<&UpstreamNodeCapabilities> {
        self.verified.get()
    }
}

async fn verify_node_endpoint(
    source: &impl BroadcastNodeProbe,
    pair_slot: &WalletServingPairSlot,
) -> Result<UpstreamNodeCapabilities, SourceError> {
    let capabilities = source.probe_capabilities().await?;
    if !capabilities.supports(NodeCapability::TransactionBroadcast) {
        return Err(SourceError::NodeCapabilityMissing {
            capability: NodeCapability::TransactionBroadcast,
        });
    }
    let node_activations = source.fetch_network_upgrade_activations().await?;
    let pair = pair_slot.capture();
    let visible_tip = pair.canonical_fence().visible_tip();
    if !node_activations.preserves_through(pair.network_upgrade_activations(), visible_tip.height) {
        return Err(SourceError::SourceProtocolMismatch {
            reason: "node network-upgrade activations differ from the writer-admitted \
                     schedule at or below the served tip",
        });
    }
    drop(pair);
    // A tip block replaced by a reorg the pair has not published yet also
    // fails here; the background probe verifies again on its next round.
    if !source.holds_block(visible_tip).await? {
        return Err(SourceError::SourceProtocolMismatch {
            reason: "node best chain holds another block at the served tip height",
        });
    }
    Ok(UpstreamNodeCapabilities::from_probed(capabilities))
}
