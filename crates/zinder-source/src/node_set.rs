//! An ordered list of node endpoints that reads from one active endpoint.
//!
//! Position 0 is `[node]`; positions 1, 2, and so on are the
//! `[node_fallbacks.N]` tables. [`NodeSet`] implements [`NodeSource`] by
//! delegating every call to the active endpoint, so code written against one
//! node keeps working unchanged. Only the [`NodeSetSwitch`] returned with the
//! set changes the active endpoint.

use std::{num::NonZeroU32, sync::Arc};

use async_trait::async_trait;
use futures_util::{StreamExt as _, stream};
use tokio::sync::watch;
use zinder_core::{
    BlockHeight, BlockId, BlockValuePoolBalances, ChainValuePools, CommitmentTreeCheckpoint,
    NetworkUpgradeActivations, ShieldedProtocol, SubtreeRootIndex, SubtreeRootRange,
};

use crate::{
    MempoolSource, MempoolSourceAdmissionLimits, MempoolSourceCapabilities,
    MempoolSourceEventStream, MempoolSourceFactory, NodeCapabilities, NodeSource, NodeStorageMode,
    NodeTarget, SourceBlock, SourceChainSegment, SourceChainSegmentLimits, SourceError,
    SourceSubtreeRoots, UpstreamHealthSnapshot, ZebraJsonRpcSource,
};

/// Reason carried by the stream item that ends a mempool event stream when
/// the active endpoint changes.
pub const ACTIVE_NODE_ENDPOINT_CHANGED_REASON: &str = "active node endpoint changed";

/// Ordered node endpoints, read through the active one.
///
/// Cloning is cheap; every clone observes the same active endpoint.
pub struct NodeSet<Endpoint> {
    inner: Arc<NodeSetInner<Endpoint>>,
}

struct NodeSetInner<Endpoint> {
    primary: Endpoint,
    fallbacks: Box<[Endpoint]>,
    active: Arc<watch::Sender<usize>>,
}

impl<Endpoint> Clone for NodeSet<Endpoint> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<Endpoint> NodeSet<Endpoint> {
    /// Builds a set whose position 0 is `primary`, followed by `fallbacks` in
    /// order, with `primary` active.
    ///
    /// The returned [`NodeSetSwitch`] is the only handle that changes the
    /// active endpoint.
    #[must_use]
    pub fn new(
        primary: Endpoint,
        fallbacks: impl IntoIterator<Item = Endpoint>,
    ) -> (Self, NodeSetSwitch<Endpoint>) {
        let (active, _) = watch::channel(0);
        let inner = Arc::new(NodeSetInner {
            primary,
            fallbacks: fallbacks.into_iter().collect(),
            active: Arc::new(active),
        });
        (
            Self {
                inner: Arc::clone(&inner),
            },
            NodeSetSwitch { inner },
        )
    }

    /// Returns how many endpoints the set holds, `[node]` included.
    #[must_use]
    pub fn endpoint_count(&self) -> usize {
        self.inner.fallbacks.len().saturating_add(1)
    }

    /// Returns whether any `[node_fallbacks.N]` endpoint is configured.
    #[must_use]
    pub fn has_fallbacks(&self) -> bool {
        !self.inner.fallbacks.is_empty()
    }

    /// Returns the position of the endpoint calls currently reach.
    #[must_use]
    pub fn active_position(&self) -> usize {
        *self.inner.active.borrow()
    }

    /// Returns the endpoint calls currently reach.
    #[must_use]
    pub fn active(&self) -> &Endpoint {
        self.endpoint(self.active_position())
            .unwrap_or(&self.inner.primary)
    }

    /// Returns the endpoint at `position`, or `None` past the last one.
    #[must_use]
    pub fn endpoint(&self, position: usize) -> Option<&Endpoint> {
        position.checked_sub(1).map_or_else(
            || Some(&self.inner.primary),
            |fallback_index| self.inner.fallbacks.get(fallback_index),
        )
    }

    /// Returns every endpoint with its position, in position order.
    pub fn endpoints(&self) -> impl Iterator<Item = (usize, &Endpoint)> {
        std::iter::once(&self.inner.primary)
            .chain(self.inner.fallbacks.iter())
            .enumerate()
    }

    /// Returns a receiver that observes every change of the active position.
    #[must_use]
    pub fn subscribe_switches(&self) -> watch::Receiver<usize> {
        self.inner.active.subscribe()
    }
}

impl NodeSet<ZebraJsonRpcSource> {
    /// Builds the JSON-RPC source of every resolved target, `primary` at
    /// position 0 and `fallbacks` after it in order.
    pub fn connect(
        primary: &NodeTarget,
        fallbacks: &[NodeTarget],
    ) -> Result<(Self, NodeSetSwitch<ZebraJsonRpcSource>), SourceError> {
        let primary = ZebraJsonRpcSource::from_node_target(primary)?;
        let fallbacks = fallbacks
            .iter()
            .map(ZebraJsonRpcSource::from_node_target)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::new(primary, fallbacks))
    }
}

impl<Endpoint> NodeSet<Endpoint>
where
    Endpoint: MempoolSourceFactory,
{
    /// Builds a mempool source that follows the active endpoint's mempool.
    ///
    /// Each endpoint's own source is built once, here. A switch ends the open
    /// event stream with one [`SourceError::MempoolStreamUnavailable`] item,
    /// so the consumer rebuilds its snapshot from the newly active endpoint as
    /// it does after any stream failure.
    #[must_use]
    pub fn mempool_source(
        &self,
        admission_limits: MempoolSourceAdmissionLimits,
    ) -> Arc<dyn MempoolSource> {
        Arc::new(ActiveEndpointMempoolSource {
            sources: self
                .endpoints()
                .map(|(_, endpoint)| endpoint.mempool_source(admission_limits))
                .collect(),
            active: Arc::clone(&self.inner.active),
        })
    }
}

/// The only handle that changes which endpoint a [`NodeSet`] reads from.
///
/// It is deliberately not `Clone`: one owner decides when a switch is safe.
pub struct NodeSetSwitch<Endpoint> {
    inner: Arc<NodeSetInner<Endpoint>>,
}

impl<Endpoint> NodeSetSwitch<Endpoint> {
    /// Makes the endpoint at `position` active for every later call.
    ///
    /// Returns `false` without a change when `position` names no endpoint or
    /// is already active.
    #[must_use = "a refused switch leaves the active endpoint unchanged"]
    pub fn switch_to(&self, position: usize) -> bool {
        if position > self.inner.fallbacks.len() {
            return false;
        }
        self.inner.active.send_if_modified(|active| {
            if *active == position {
                false
            } else {
                *active = position;
                true
            }
        })
    }
}

struct ActiveEndpointMempoolSource {
    sources: Box<[Arc<dyn MempoolSource>]>,
    active: Arc<watch::Sender<usize>>,
}

impl ActiveEndpointMempoolSource {
    fn source_at(&self, position: usize) -> Result<&Arc<dyn MempoolSource>, SourceError> {
        self.sources
            .get(position)
            .ok_or_else(|| SourceError::MempoolStreamUnavailable {
                reason: "active node endpoint has no mempool source".to_owned(),
            })
    }
}

#[async_trait]
impl MempoolSource for ActiveEndpointMempoolSource {
    fn capabilities(&self) -> MempoolSourceCapabilities {
        let position = *self.active.borrow();
        self.sources
            .get(position)
            .or_else(|| self.sources.first())
            .map_or_else(MempoolSourceCapabilities::polling, |source| {
                source.capabilities()
            })
    }

    async fn events(&self) -> Result<MempoolSourceEventStream, SourceError> {
        let mut switches = self.active.subscribe();
        let position = *switches.borrow_and_update();
        let events = self.source_at(position)?.events().await?;
        Ok(end_on_switch(events, switches))
    }
}

/// Forwards `events` until the active endpoint changes, then yields one
/// [`SourceError::MempoolStreamUnavailable`] item and ends.
fn end_on_switch(
    events: MempoolSourceEventStream,
    switches: watch::Receiver<usize>,
) -> MempoolSourceEventStream {
    Box::pin(stream::unfold(
        Some((events, switches)),
        |state| async move {
            let (mut events, mut switches) = state?;
            tokio::select! {
                biased;
                Ok(()) = switches.changed() => Some((
                    Err(SourceError::MempoolStreamUnavailable {
                        reason: ACTIVE_NODE_ENDPOINT_CHANGED_REASON.to_owned(),
                    }),
                    None,
                )),
                event = events.next() => event.map(|event| (event, Some((events, switches)))),
            }
        },
    ))
}

#[async_trait]
impl<Endpoint> NodeSource for NodeSet<Endpoint>
where
    Endpoint: NodeSource,
{
    fn capabilities(&self) -> NodeCapabilities {
        self.active().capabilities()
    }

    async fn probe_capabilities(&self) -> Result<NodeCapabilities, SourceError> {
        self.active().probe_capabilities().await
    }

    async fn fetch_network_upgrade_activations(
        &self,
    ) -> Result<NetworkUpgradeActivations, SourceError> {
        self.active().fetch_network_upgrade_activations().await
    }

    async fn fetch_storage_mode(&self) -> Result<NodeStorageMode, SourceError> {
        self.active().fetch_storage_mode().await
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.active().fetch_block_at(height).await
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        self.active().fetch_chain_segment(limits).await
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.active()
            .fetch_chain_checkpoint(height, network_upgrade_activations)
            .await
    }

    async fn fetch_chain_checkpoint_for_block(
        &self,
        block_id: BlockId,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.active()
            .fetch_chain_checkpoint_for_block(block_id, network_upgrade_activations)
            .await
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        self.active().tip_id().await
    }

    async fn fetch_subtree_roots(
        &self,
        protocol: ShieldedProtocol,
        start_index: SubtreeRootIndex,
        max_entries: NonZeroU32,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.active()
            .fetch_subtree_roots(protocol, start_index, max_entries)
            .await
    }

    async fn fetch_subtree_root_range(
        &self,
        range: SubtreeRootRange,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.active().fetch_subtree_root_range(range).await
    }

    async fn fetch_chain_value_pools_at_tip(&self) -> Result<ChainValuePools, SourceError> {
        self.active().fetch_chain_value_pools_at_tip().await
    }

    async fn fetch_block_value_pool_balances(
        &self,
        block_id: BlockId,
    ) -> Result<BlockValuePoolBalances, SourceError> {
        self.active()
            .fetch_block_value_pool_balances(block_id)
            .await
    }

    async fn poll_upstream_health(&self) -> Result<UpstreamHealthSnapshot, SourceError> {
        self.active().poll_upstream_health().await
    }
}
