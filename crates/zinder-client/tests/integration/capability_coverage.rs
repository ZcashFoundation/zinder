//! Capability-coverage contract test.
//!
//! The single source of truth is the [`CAPABILITIES`] table in
//! `zinder-proto`. Each row binds a capability string to its surface, the
//! proto method it gates, and an advertise policy. The
//! `capability-table-vs-descriptor` guard in `zinder-proto` cross-checks the
//! table's proto-method bindings against the compiled `FileDescriptorSet`;
//! this test guards the client-facing trait surface those wallet capabilities
//! map onto.
//!
//! `assert_wallet_chain_index_methods_compile` takes a function-item
//! reference to each `ChainIndex` method a wallet capability calls. Renaming
//! or removing any of those trait methods makes this file fail to compile, so
//! the wallet capability surface stays bound to the trait at build time.

use std::collections::{BTreeMap, BTreeSet};

use zinder_client::{
    Capability, CapabilityDescriptor, ChainIndex, ChainSnapshot, EndpointBackedIndex,
    OwnedChainSnapshot, RemoteChainIndex,
};
use zinder_proto::capabilities::*;

#[test]
fn every_capability_row_has_a_non_empty_string() {
    for spec in CAPABILITIES {
        assert!(
            !spec.string.is_empty(),
            "a {:?} capability row has an empty capability string",
            spec.surface
        );
    }
}

/// Regression guard for the ingest wire surface.
///
/// `ingest.writer.phase_v1` must stay always-on so federated clients can rely
/// on the writer-phase vocabulary advertised by every Zinder deployment.
/// ADR-0015.
#[test]
fn ingest_writer_phase_is_always_on() {
    assert!(
        always_on_capability_strings(CapabilitySurface::Ingest).contains(&INGEST_WRITER_PHASE_V1),
        "{INGEST_WRITER_PHASE_V1} must stay always-on so every ingest deployment advertises it; \
         the ingest wire surface depends on this invariant."
    );
}

/// Compile-time existence check for the [`ChainIndex`] base reads that back
/// the canonical and wallet-projection capabilities.
///
/// The function body references each base trait method a wallet capability
/// calls; renaming or removing any of them breaks the build. The function
/// itself is never called.
#[allow(
    dead_code,
    reason = "compile-time existence check for ChainIndex methods backing wallet capabilities"
)]
fn assert_wallet_chain_index_methods_compile<T: ChainIndex>() {
    let _ = T::network_upgrade_activations;
    let _ = T::visible_tip_block;
    let _ = T::settled_tip_block;
    let _ = T::block_id_by_selector;
    let _ = T::block_header_by_selector;
    let _ = T::compact_block_at;
    let _ = T::compact_blocks_in_range;
    let _ = T::full_block_at;
    let _ = T::full_blocks_in_range;
    let _ = T::tree_state_at;
    let _ = T::latest_tree_state_checkpoint;
    let _ = T::subtree_roots_in_range;
    let _ = T::transaction_by_id;
    let _ = T::transparent_address_unspent_outputs;
    let _ = T::transparent_address_tx_ids_in_range;
}

#[allow(
    dead_code,
    reason = "compile-time existence check for every epoch-pinnable ChainSnapshot read"
)]
fn assert_chain_snapshot_methods_compile<T: ChainIndex>() {
    let _ = ChainSnapshot::<T>::chain_epoch;
    let _ = ChainSnapshot::<T>::visible_tip_block;
    let _ = ChainSnapshot::<T>::settled_tip_block;
    let _ = ChainSnapshot::<T>::block_id_by_selector;
    let _ = ChainSnapshot::<T>::block_header_by_selector;
    let _ = ChainSnapshot::<T>::compact_block_at;
    let _ = ChainSnapshot::<T>::compact_blocks_in_range;
    let _ = ChainSnapshot::<T>::full_block_at;
    let _ = ChainSnapshot::<T>::full_blocks_in_range;
    let _ = ChainSnapshot::<T>::tree_state_at;
    let _ = ChainSnapshot::<T>::latest_tree_state_checkpoint;
    let _ = ChainSnapshot::<T>::subtree_roots_in_range;
    let _ = ChainSnapshot::<T>::transaction_by_id;
    let _ = ChainSnapshot::<T>::transparent_address_unspent_outputs;
}

#[allow(
    dead_code,
    reason = "compile-time parity check between borrowed and owned snapshot methods"
)]
fn assert_owned_chain_snapshot_methods_compile<T: ChainIndex>() {
    let _ = OwnedChainSnapshot::<T>::chain_epoch;
    let _ = OwnedChainSnapshot::<T>::visible_tip_block;
    let _ = OwnedChainSnapshot::<T>::settled_tip_block;
    let _ = OwnedChainSnapshot::<T>::block_id_by_selector;
    let _ = OwnedChainSnapshot::<T>::block_header_by_selector;
    let _ = OwnedChainSnapshot::<T>::compact_block_at;
    let _ = OwnedChainSnapshot::<T>::compact_blocks_in_range;
    let _ = OwnedChainSnapshot::<T>::full_block_at;
    let _ = OwnedChainSnapshot::<T>::full_blocks_in_range;
    let _ = OwnedChainSnapshot::<T>::tree_state_at;
    let _ = OwnedChainSnapshot::<T>::latest_tree_state_checkpoint;
    let _ = OwnedChainSnapshot::<T>::subtree_roots_in_range;
    let _ = OwnedChainSnapshot::<T>::transaction_by_id;
    let _ = OwnedChainSnapshot::<T>::transparent_address_unspent_outputs;
    let _ = OwnedChainSnapshot::<T>::transparent_address_tx_ids_in_range;
}

/// Compile-time existence check for the [`EndpointBackedIndex`] methods that
/// back the endpoint-only wallet capabilities.
///
/// These methods need a live ingest-control/broadcast endpoint, so they live
/// on the extension trait; renaming or removing any of them breaks the build.
#[allow(
    dead_code,
    reason = "compile-time existence check for EndpointBackedIndex methods backing wallet capabilities"
)]
fn assert_wallet_endpoint_methods_compile<T: EndpointBackedIndex>() {
    let _ = T::server_info;
    let _ = T::broadcast_transaction;
    let _ = T::chain_events;
    let _ = T::mempool_snapshot;
    let _ = T::mempool_events;
    let _ = T::is_in_mempool;
    let _ = T::chain_value_pools_at_tip;
}

/// The typed [`Capability`] probe maps each variant onto the same wire string
/// the [`CAPABILITIES`] table advertises, so a typed probe and the advertised
/// capability cannot drift.
#[test]
fn typed_capability_variants_match_table_strings() {
    let advertised: std::collections::BTreeSet<&str> =
        CAPABILITIES.iter().map(|spec| spec.string).collect();
    for capability in [
        Capability::Broadcast,
        Capability::ChainEvents,
        Capability::MempoolSnapshot,
        Capability::MempoolEvents,
        Capability::ChainValuePools,
        Capability::NetworkUpgradeActivations,
        Capability::FullBlock,
        Capability::FullBlockRange,
        Capability::BlockIdBySelector,
        Capability::NetworkUpgradeActivations,
    ] {
        assert!(
            advertised.contains(capability.as_str()),
            "typed Capability::{capability:?} string {} is not in the CAPABILITIES table",
            capability.as_str()
        );
    }
}

/// The default [`CapabilityDescriptor::supports`] probe reads through to the
/// raw `has` lookup over the typed variant's wire string.
#[test]
fn capability_descriptor_supports_reads_typed_variant() {
    struct StubDescriptor {
        advertised: Vec<&'static str>,
    }
    impl CapabilityDescriptor for StubDescriptor {
        fn has(&self, capability: &str) -> bool {
            self.advertised.contains(&capability)
        }
    }

    let descriptor = StubDescriptor {
        advertised: vec![WALLET_BROADCAST_TRANSACTION_V1],
    };
    assert!(descriptor.supports(Capability::Broadcast));
    assert!(!descriptor.supports(Capability::ChainValuePools));
}

/// Every wallet capability is classified against the typed client operation
/// that consumes its RPC or gated response field.
#[test]
#[allow(
    clippy::too_many_lines,
    reason = "The exhaustive wallet-capability classification stays in one test so additions fail one auditable gate."
)]
fn typed_chain_index_covers_every_advertised_wallet_capability() {
    assert_wallet_chain_index_methods_compile::<RemoteChainIndex>();
    assert_wallet_endpoint_methods_compile::<RemoteChainIndex>();
    assert_chain_snapshot_methods_compile::<RemoteChainIndex>();
    assert_owned_chain_snapshot_methods_compile::<RemoteChainIndex>();

    let coverage_rows = [
        (
            WALLET_READ_VISIBLE_TIP_BLOCK_V1,
            "ChainIndex::visible_tip_block",
        ),
        (
            WALLET_READ_SETTLED_TIP_BLOCK_V1,
            "ChainIndex::settled_tip_block",
        ),
        (
            WALLET_READ_BLOCK_ID_BY_SELECTOR_V1,
            "ChainIndex::block_id_by_selector",
        ),
        (
            WALLET_READ_BLOCK_HEADER_BY_SELECTOR_V1,
            "ChainIndex::block_header_by_selector",
        ),
        (
            WALLET_READ_COMPACT_BLOCK_AT_V2,
            "ChainIndex::compact_block_at",
        ),
        (
            WALLET_READ_COMPACT_BLOCK_RANGE_V2,
            "ChainIndex::compact_blocks_in_range",
        ),
        (
            WALLET_READ_COMPACT_BLOCK_IRONWOOD_V2,
            "ChainIndex::compact_block_at",
        ),
        (WALLET_READ_FULL_BLOCK_AT_V1, "ChainIndex::full_block_at"),
        (
            WALLET_READ_FULL_BLOCK_RANGE_V1,
            "ChainIndex::full_blocks_in_range",
        ),
        (
            WALLET_READ_TREE_STATE_AT_HEIGHT_V2,
            "ChainIndex::tree_state_at",
        ),
        (
            WALLET_READ_LATEST_TREE_STATE_CHECKPOINT_V2,
            "ChainIndex::latest_tree_state_checkpoint",
        ),
        (
            WALLET_READ_SUBTREE_ROOTS_IN_RANGE_V1,
            "ChainIndex::subtree_roots_in_range",
        ),
        (
            WALLET_READ_SUBTREE_ROOTS_IRONWOOD_V1,
            "ChainIndex::subtree_roots_in_range",
        ),
        (
            WALLET_READ_TRANSACTION_BY_ID_V2,
            "ChainIndex::transaction_by_id",
        ),
        (
            WALLET_READ_TRANSACTION_BYTES_V1,
            "ChainIndex::transaction_by_id",
        ),
        (
            WALLET_READ_SERVER_INFO_V2,
            "EndpointBackedIndex::server_info",
        ),
        (
            WALLET_READ_NETWORK_UPGRADE_ACTIVATIONS_V1,
            "ChainIndex::network_upgrade_activations",
        ),
        (
            WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1,
            "EndpointBackedIndex::chain_value_pools_at_tip",
        ),
        (
            WALLET_BROADCAST_TRANSACTION_V1,
            "EndpointBackedIndex::broadcast_transaction",
        ),
        (WALLET_EVENTS_CHAIN_V1, "EndpointBackedIndex::chain_events"),
        (
            WALLET_SNAPSHOT_MEMPOOL_V3,
            "EndpointBackedIndex::mempool_snapshot",
        ),
        (
            WALLET_EVENTS_MEMPOOL_V2,
            "EndpointBackedIndex::mempool_events",
        ),
        (
            WALLET_ADDRESS_TRANSPARENT_UNSPENT_OUTPUTS_V1,
            "ChainIndex::transparent_address_unspent_outputs",
        ),
        (
            WALLET_ADDRESS_TRANSPARENT_HISTORY_V1,
            "ChainIndex::transparent_address_tx_ids_in_range",
        ),
    ];
    let covered: BTreeMap<_, _> = coverage_rows.into_iter().collect();
    assert_eq!(
        covered.len(),
        coverage_rows.len(),
        "each wallet capability must have exactly one client coverage classification"
    );
    let advertised: BTreeSet<_> = CAPABILITIES
        .iter()
        .filter(|spec| spec.surface == CapabilitySurface::Wallet)
        .map(|spec| spec.string)
        .collect();
    let classified: BTreeSet<_> = covered.keys().copied().collect();

    assert_eq!(classified, advertised);
    assert!(covered.values().all(|operation| !operation.is_empty()));
}
