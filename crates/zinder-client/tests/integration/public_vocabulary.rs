//! Public SDK vocabulary must remain independent of generated protobuf names.

use zinder_client::{Capability, CapabilityDescriptor, ErrorReason, ServerInfo};
use zinder_proto::capabilities::{
    WALLET_ADDRESS_TRANSPARENT_HISTORY_V1, WALLET_ADDRESS_TRANSPARENT_UNSPENT_OUTPUTS_V1,
    WALLET_EVENTS_CHAIN_V1, WALLET_READ_BLOCK_ID_BY_SELECTOR_V1,
    WALLET_READ_COMPACT_BLOCK_IRONWOOD_V2, WALLET_READ_COMPACT_BLOCK_RANGE_V2,
    WALLET_READ_FULL_BLOCK_AT_V1, WALLET_READ_FULL_BLOCK_RANGE_V1,
    WALLET_READ_NETWORK_UPGRADE_ACTIVATIONS_V1, WALLET_READ_SERVER_INFO_V2,
    WALLET_READ_SETTLED_TIP_BLOCK_V1, WALLET_READ_SUBTREE_ROOTS_IN_RANGE_V1,
    WALLET_READ_SUBTREE_ROOTS_IRONWOOD_V1, WALLET_READ_TRANSACTION_BY_ID_V2,
    WALLET_READ_TRANSPARENT_OUTPOINT_SPEND_STATUS_V1, WALLET_READ_TREE_STATE_AT_HEIGHT_V2,
    WALLET_READ_VISIBLE_TIP_BLOCK_V1,
};

#[test]
fn wallet_capabilities_use_client_owned_exact_match_vocabulary() {
    for (capability, expected) in [
        (Capability::FullBlock, WALLET_READ_FULL_BLOCK_AT_V1),
        (Capability::FullBlockRange, WALLET_READ_FULL_BLOCK_RANGE_V1),
        (
            Capability::NetworkUpgradeActivations,
            WALLET_READ_NETWORK_UPGRADE_ACTIVATIONS_V1,
        ),
    ] {
        assert_eq!(capability.as_str(), expected);
    }
}

#[test]
fn wallet_sync_capabilities_use_client_owned_exact_match_vocabulary() {
    for (capability, expected) in [
        (Capability::ServerInfo, WALLET_READ_SERVER_INFO_V2),
        (
            Capability::VisibleTipBlock,
            WALLET_READ_VISIBLE_TIP_BLOCK_V1,
        ),
        (
            Capability::SettledTipBlock,
            WALLET_READ_SETTLED_TIP_BLOCK_V1,
        ),
        (
            Capability::BlockIdBySelector,
            WALLET_READ_BLOCK_ID_BY_SELECTOR_V1,
        ),
        (
            Capability::CompactBlockRange,
            WALLET_READ_COMPACT_BLOCK_RANGE_V2,
        ),
        (
            Capability::CompactBlockIronwood,
            WALLET_READ_COMPACT_BLOCK_IRONWOOD_V2,
        ),
        (Capability::TreeState, WALLET_READ_TREE_STATE_AT_HEIGHT_V2),
        (
            Capability::SubtreeRoots,
            WALLET_READ_SUBTREE_ROOTS_IN_RANGE_V1,
        ),
        (
            Capability::SubtreeRootsIronwood,
            WALLET_READ_SUBTREE_ROOTS_IRONWOOD_V1,
        ),
        (Capability::Transaction, WALLET_READ_TRANSACTION_BY_ID_V2),
        (
            Capability::TransparentAddressUnspentOutputs,
            WALLET_ADDRESS_TRANSPARENT_UNSPENT_OUTPUTS_V1,
        ),
        (
            Capability::TransparentAddressHistory,
            WALLET_ADDRESS_TRANSPARENT_HISTORY_V1,
        ),
        (
            Capability::TransparentOutpointSpendStatus,
            WALLET_READ_TRANSPARENT_OUTPOINT_SPEND_STATUS_V1,
        ),
        (Capability::ChainEvents, WALLET_EVENTS_CHAIN_V1),
    ] {
        assert_eq!(capability.as_str(), expected);
    }
}

#[test]
fn future_capability_and_error_reason_values_are_preserved() {
    let capability = Capability::Unknown("wallet.read.future_v7".to_owned());
    assert_eq!(capability.as_str(), "wallet.read.future_v7");

    let reason = ErrorReason::from_wire_name("FUTURE_SERVER_REASON");
    assert_eq!(
        reason,
        ErrorReason::Unknown("FUTURE_SERVER_REASON".to_owned())
    );
    assert_eq!(reason.as_str(), "FUTURE_SERVER_REASON");
}

#[allow(
    dead_code,
    reason = "compile-time guard for the client-owned server descriptor"
)]
fn server_info_supports_typed_capabilities(server_info: &ServerInfo) {
    let _ = server_info.supports(Capability::FullBlock);
    let _ = server_info.has("wallet.read.future_v7");
}
