//! Client-owned wallet capability vocabulary.

use zinder_proto::capabilities::{
    WALLET_ADDRESS_TRANSPARENT_HISTORY_V1, WALLET_ADDRESS_TRANSPARENT_UNSPENT_OUTPUTS_V1,
    WALLET_BROADCAST_TRANSACTION_V1, WALLET_EVENTS_CHAIN_V1, WALLET_EVENTS_MEMPOOL_V2,
    WALLET_READ_BLOCK_ID_BY_SELECTOR_V1, WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1,
    WALLET_READ_COMPACT_BLOCK_IRONWOOD_V2, WALLET_READ_COMPACT_BLOCK_RANGE_V2,
    WALLET_READ_FULL_BLOCK_AT_V1, WALLET_READ_FULL_BLOCK_RANGE_V1,
    WALLET_READ_NETWORK_UPGRADE_ACTIVATIONS_V1, WALLET_READ_SERVER_INFO_V2,
    WALLET_READ_SETTLED_TIP_BLOCK_V1, WALLET_READ_SUBTREE_ROOTS_IN_RANGE_V1,
    WALLET_READ_SUBTREE_ROOTS_IRONWOOD_V1, WALLET_READ_TRANSACTION_BY_ID_V2,
    WALLET_READ_TRANSPARENT_OUTPOINT_SPEND_STATUS_V1, WALLET_READ_TREE_STATE_AT_HEIGHT_V2,
    WALLET_READ_VISIBLE_TIP_BLOCK_V1, WALLET_SNAPSHOT_MEMPOOL_V3,
};

/// A wallet-plane capability advertised by a Zinder endpoint.
///
/// Known variants cover the operations consumers preflight today.
/// [`Self::Unknown`] preserves additive capability strings emitted by a newer
/// server so callers can still perform exact-match discovery.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum Capability {
    /// Raw transaction broadcast.
    Broadcast,
    /// Wallet endpoint identity and contract metadata.
    ServerInfo,
    /// Visible canonical tip block.
    VisibleTipBlock,
    /// Settled canonical tip block.
    SettledTipBlock,
    /// Canonical block identity resolved by height or hash selector.
    BlockIdBySelector,
    /// Stream of compact blocks over a bounded range.
    CompactBlockRange,
    /// Ironwood fields in compact block responses.
    CompactBlockIronwood,
    /// Shielded tree state at an exact height.
    TreeState,
    /// Shielded subtree roots over a bounded index range.
    SubtreeRoots,
    /// Ironwood subtree-root responses.
    SubtreeRootsIronwood,
    /// Canonical-or-mempool transaction lookup by transaction identifier.
    Transaction,
    /// Transparent unspent outputs for one address.
    TransparentAddressUnspentOutputs,
    /// Ascending transparent-address transaction history.
    TransparentAddressHistory,
    /// Unspent, spent, or not-found status of transparent outpoints.
    TransparentOutpointSpendStatus,
    /// Cursor-resumable chain-event stream.
    ChainEvents,
    /// Tip-certified bounded mempool snapshot.
    MempoolSnapshot,
    /// Replayable mempool-event stream.
    MempoolEvents,
    /// Chain value-pool totals at the upstream tip.
    ChainValuePools,
    /// Serialized full block at one height.
    FullBlock,
    /// Stream of serialized full blocks over a bounded range.
    FullBlockRange,
    /// Named network-upgrade activation heights and branch identifiers.
    NetworkUpgradeActivations,
    /// Additive capability emitted by a newer server.
    Unknown(String),
}

impl Capability {
    /// Returns the exact wire string advertised by the server.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Broadcast => WALLET_BROADCAST_TRANSACTION_V1,
            Self::ServerInfo => WALLET_READ_SERVER_INFO_V2,
            Self::VisibleTipBlock => WALLET_READ_VISIBLE_TIP_BLOCK_V1,
            Self::SettledTipBlock => WALLET_READ_SETTLED_TIP_BLOCK_V1,
            Self::BlockIdBySelector => WALLET_READ_BLOCK_ID_BY_SELECTOR_V1,
            Self::CompactBlockRange => WALLET_READ_COMPACT_BLOCK_RANGE_V2,
            Self::CompactBlockIronwood => WALLET_READ_COMPACT_BLOCK_IRONWOOD_V2,
            Self::TreeState => WALLET_READ_TREE_STATE_AT_HEIGHT_V2,
            Self::SubtreeRoots => WALLET_READ_SUBTREE_ROOTS_IN_RANGE_V1,
            Self::SubtreeRootsIronwood => WALLET_READ_SUBTREE_ROOTS_IRONWOOD_V1,
            Self::Transaction => WALLET_READ_TRANSACTION_BY_ID_V2,
            Self::TransparentAddressUnspentOutputs => WALLET_ADDRESS_TRANSPARENT_UNSPENT_OUTPUTS_V1,
            Self::TransparentAddressHistory => WALLET_ADDRESS_TRANSPARENT_HISTORY_V1,
            Self::TransparentOutpointSpendStatus => {
                WALLET_READ_TRANSPARENT_OUTPOINT_SPEND_STATUS_V1
            }
            Self::ChainEvents => WALLET_EVENTS_CHAIN_V1,
            Self::MempoolSnapshot => WALLET_SNAPSHOT_MEMPOOL_V3,
            Self::MempoolEvents => WALLET_EVENTS_MEMPOOL_V2,
            Self::ChainValuePools => WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1,
            Self::FullBlock => WALLET_READ_FULL_BLOCK_AT_V1,
            Self::FullBlockRange => WALLET_READ_FULL_BLOCK_RANGE_V1,
            Self::NetworkUpgradeActivations => WALLET_READ_NETWORK_UPGRADE_ACTIVATIONS_V1,
            Self::Unknown(capability) => capability,
        }
    }

    pub(crate) fn from_wire_name(capability: String) -> Self {
        match capability.as_str() {
            WALLET_BROADCAST_TRANSACTION_V1 => Self::Broadcast,
            WALLET_READ_SERVER_INFO_V2 => Self::ServerInfo,
            WALLET_READ_VISIBLE_TIP_BLOCK_V1 => Self::VisibleTipBlock,
            WALLET_READ_SETTLED_TIP_BLOCK_V1 => Self::SettledTipBlock,
            WALLET_READ_BLOCK_ID_BY_SELECTOR_V1 => Self::BlockIdBySelector,
            WALLET_READ_COMPACT_BLOCK_RANGE_V2 => Self::CompactBlockRange,
            WALLET_READ_COMPACT_BLOCK_IRONWOOD_V2 => Self::CompactBlockIronwood,
            WALLET_READ_TREE_STATE_AT_HEIGHT_V2 => Self::TreeState,
            WALLET_READ_SUBTREE_ROOTS_IN_RANGE_V1 => Self::SubtreeRoots,
            WALLET_READ_SUBTREE_ROOTS_IRONWOOD_V1 => Self::SubtreeRootsIronwood,
            WALLET_READ_TRANSACTION_BY_ID_V2 => Self::Transaction,
            WALLET_ADDRESS_TRANSPARENT_UNSPENT_OUTPUTS_V1 => Self::TransparentAddressUnspentOutputs,
            WALLET_ADDRESS_TRANSPARENT_HISTORY_V1 => Self::TransparentAddressHistory,
            WALLET_READ_TRANSPARENT_OUTPOINT_SPEND_STATUS_V1 => {
                Self::TransparentOutpointSpendStatus
            }
            WALLET_EVENTS_CHAIN_V1 => Self::ChainEvents,
            WALLET_SNAPSHOT_MEMPOOL_V3 => Self::MempoolSnapshot,
            WALLET_EVENTS_MEMPOOL_V2 => Self::MempoolEvents,
            WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1 => Self::ChainValuePools,
            WALLET_READ_FULL_BLOCK_AT_V1 => Self::FullBlock,
            WALLET_READ_FULL_BLOCK_RANGE_V1 => Self::FullBlockRange,
            WALLET_READ_NETWORK_UPGRADE_ACTIVATIONS_V1 => Self::NetworkUpgradeActivations,
            _ => Self::Unknown(capability),
        }
    }
}

/// Exact-match capability discovery for a server descriptor.
pub trait CapabilityDescriptor {
    /// Returns true when the descriptor advertises `capability` exactly.
    fn has(&self, capability: &str) -> bool;

    /// Returns true when the descriptor advertises the typed capability.
    fn supports(&self, capability: Capability) -> bool {
        self.has(capability.as_str())
    }
}
