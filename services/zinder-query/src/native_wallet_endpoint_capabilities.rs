//! Immutable native capability evidence owned by an admitted wallet query.

use std::sync::Arc;

use zinder_proto::capabilities::{self, CapabilitySurface, capabilities_for_surface};
use zinder_source::{NodeCapabilities, NodeCapability};
use zinder_store::RawBlobRetention;

use crate::AdmittedIngestControl;

/// Immutable native capability set for one admitted wallet endpoint.
///
/// Callers can inspect this value, but cannot construct it from arbitrary
/// strings or mutate it after query composition. The private constructors
/// below bind every claim to the concrete query implementation and its
/// admitted storage or upstream evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeWalletEndpointCapabilities {
    ordered: Arc<[&'static str]>,
}

impl NativeWalletEndpointCapabilities {
    /// Returns whether this endpoint structurally supports `capability`.
    #[must_use]
    pub fn contains(&self, capability: &str) -> bool {
        self.ordered.contains(&capability)
    }

    /// Iterates capability identifiers in stable registry order.
    #[must_use]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &'static str> + '_ {
        self.ordered.iter().copied()
    }

    /// Shares the exact immutable identifier slice owned by this admitted
    /// query with an operational discovery surface.
    #[must_use]
    pub fn shared_identifiers(&self) -> Arc<[&'static str]> {
        Arc::clone(&self.ordered)
    }

    pub(crate) fn has_node_backed_capabilities(&self) -> bool {
        self.contains(capabilities::WALLET_READ_TREE_STATE_AT_HEIGHT_V2)
            || self.contains(capabilities::WALLET_BROADCAST_TRANSACTION_V1)
    }

    /// Derives the capabilities implemented by the shared exact-pair query.
    ///
    /// `node_capabilities` must be the result of the probe performed on the
    /// same source handle installed as the query's tree-state provider and
    /// transaction broadcaster.
    pub(crate) fn for_wallet_serving_pair(
        raw_blob_retention: RawBlobRetention,
        node_capabilities: NodeCapabilities,
    ) -> Self {
        Self::from_predicate(|capability| {
            Self::wallet_serving_pair_supports(capability, raw_blob_retention, node_capabilities)
        })
    }

    /// Derives the native endpoint contract after its ingest control is admitted.
    ///
    /// The shared exact-pair query does not require this native-only dependency.
    /// Its admitted type state is supplied only by the native composition root.
    pub(crate) fn for_admitted_native_wallet_query(
        raw_blob_retention: RawBlobRetention,
        node_capabilities: NodeCapabilities,
        ingest_control: &AdmittedIngestControl,
    ) -> Self {
        Self::from_predicate(|capability| {
            let transaction_lookup_supported =
                ingest_control.supports(capabilities::INGEST_CONTROL_MEMPOOL_TRANSACTION_V2);
            Self::wallet_serving_pair_supports(capability, raw_blob_retention, node_capabilities)
                || (capability == capabilities::WALLET_READ_TRANSACTION_BY_ID_V2
                    && transaction_lookup_supported)
                || (capability == capabilities::WALLET_READ_TRANSACTION_BYTES_V1
                    && transaction_lookup_supported
                    && raw_blob_retention.retains_transaction_blobs())
                || (capability == capabilities::WALLET_SNAPSHOT_MEMPOOL_V3
                    && ingest_control.supports(capabilities::INGEST_CONTROL_MEMPOOL_SNAPSHOT_V3))
                || (capability == capabilities::WALLET_EVENTS_MEMPOOL_V2
                    && ingest_control.supports(capabilities::INGEST_CONTROL_MEMPOOL_EVENTS_V2))
        })
    }

    fn wallet_serving_pair_supports(
        capability: &str,
        raw_blob_retention: RawBlobRetention,
        node_capabilities: NodeCapabilities,
    ) -> bool {
        let node_was_probed = node_capabilities.supports(NodeCapability::OpenRpcDiscovery);
        let implemented_without_optional_evidence = matches!(
            capability,
            capabilities::WALLET_READ_VISIBLE_TIP_BLOCK_V1
                | capabilities::WALLET_READ_SETTLED_TIP_BLOCK_V1
                | capabilities::WALLET_READ_BLOCK_ID_BY_SELECTOR_V1
                | capabilities::WALLET_READ_COMPACT_BLOCK_AT_V2
                | capabilities::WALLET_READ_COMPACT_BLOCK_RANGE_V2
                | capabilities::WALLET_READ_COMPACT_BLOCK_IRONWOOD_V2
                | capabilities::WALLET_READ_LATEST_TREE_STATE_CHECKPOINT_V2
                | capabilities::WALLET_READ_SUBTREE_ROOTS_IN_RANGE_V1
                | capabilities::WALLET_READ_SUBTREE_ROOTS_IRONWOOD_V1
                | capabilities::WALLET_READ_SERVER_INFO_V2
                | capabilities::WALLET_READ_NETWORK_UPGRADE_ACTIVATIONS_V1
                | capabilities::WALLET_EVENTS_CHAIN_V1
                | capabilities::WALLET_ADDRESS_TRANSPARENT_UNSPENT_OUTPUTS_V1
                | capabilities::WALLET_ADDRESS_TRANSPARENT_HISTORY_V1
        );
        implemented_without_optional_evidence
            || (matches!(
                capability,
                capabilities::WALLET_READ_FULL_BLOCK_AT_V1
                    | capabilities::WALLET_READ_FULL_BLOCK_RANGE_V1
            ) && raw_blob_retention.retains_block_blobs())
            || (capability == capabilities::WALLET_READ_TREE_STATE_AT_HEIGHT_V2
                && node_was_probed
                && node_capabilities.supports(NodeCapability::TreeState))
            || (capability == capabilities::WALLET_BROADCAST_TRANSACTION_V1
                && node_was_probed
                && node_capabilities.supports(NodeCapability::TransactionBroadcast))
    }

    fn from_predicate(mut is_supported: impl FnMut(&str) -> bool) -> Self {
        let ordered = capabilities_for_surface(CapabilitySurface::Wallet)
            .map(|spec| spec.string)
            .filter(|capability| is_supported(capability))
            .collect::<Vec<_>>()
            .into();
        Self { ordered }
    }
}

/// Diagnostic snapshot from the upstream node probe used by this query.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpstreamNodeCapabilities {
    /// Node-reported semantic version when available.
    pub version: Option<String>,
    capabilities: Arc<[&'static str]>,
}

impl UpstreamNodeCapabilities {
    /// Captures stable diagnostic names from the successful source probe.
    pub(crate) fn from_probed(node_capabilities: NodeCapabilities) -> Self {
        Self {
            version: None,
            capabilities: node_capabilities
                .iter()
                .map(NodeCapability::name)
                .collect::<Vec<_>>()
                .into(),
        }
    }

    pub(crate) fn names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.capabilities.iter().copied()
    }
}
