#![allow(
    missing_docs,
    reason = "Integration test names describe the capability-registry contract under test."
)]

use std::collections::BTreeSet;

use zinder_proto::capabilities::{
    AdvertisePolicy, CAPABILITIES, CapabilitySurface, EXPLORER_MEMPOOL_EVENT_COUNTS_V1,
    EXPLORER_VALUE_POOL_BALANCE_HISTORY_V1, EXPLORER_VALUE_POOL_SUMMARY_V1, ExplorerReadiness,
    WALLET_READ_FULL_BLOCK_AT_V1, WALLET_READ_FULL_BLOCK_RANGE_V1,
    WALLET_READ_TRANSACTION_BY_ID_V2, WALLET_READ_TRANSACTION_BYTES_V1, capabilities_for_surface,
};

use zinder_proto::v1::wallet::WalletServerInfo;

fn explorer_policy(capability: &str) -> Option<AdvertisePolicy> {
    capabilities_for_surface(CapabilitySurface::Explorer)
        .find(|spec| spec.string == capability)
        .map(|spec| spec.policy)
}

fn wallet_policy(capability: &str) -> Option<AdvertisePolicy> {
    capabilities_for_surface(CapabilitySurface::Wallet)
        .find(|spec| spec.string == capability)
        .map(|spec| spec.policy)
}

#[test]
fn wallet_registry_preserves_table_order_and_unique_identifiers() {
    let wallet_capabilities: Vec<&str> = capabilities_for_surface(CapabilitySurface::Wallet)
        .map(|spec| spec.string)
        .collect();
    let registry_wallet_capabilities: Vec<&str> = CAPABILITIES
        .iter()
        .filter(|spec| spec.surface == CapabilitySurface::Wallet)
        .map(|spec| spec.string)
        .collect();
    let unique_wallet_capabilities: BTreeSet<&str> = wallet_capabilities.iter().copied().collect();

    assert_ne!(wallet_capabilities, [] as [&str; 0]);
    assert_eq!(wallet_capabilities, registry_wallet_capabilities);
    assert_eq!(
        wallet_capabilities.len(),
        unique_wallet_capabilities.len(),
        "wallet capability identifiers must be unique"
    );
}

#[test]
fn wallet_registry_records_requirements_without_resolving_runtime_support() {
    assert_eq!(
        wallet_policy(WALLET_READ_FULL_BLOCK_AT_V1),
        Some(AdvertisePolicy::RequiresBlockBlobs)
    );
    assert_eq!(
        wallet_policy(WALLET_READ_FULL_BLOCK_RANGE_V1),
        Some(AdvertisePolicy::RequiresBlockBlobs)
    );
    assert_eq!(
        wallet_policy(WALLET_READ_TRANSACTION_BYTES_V1),
        Some(AdvertisePolicy::RequiresTransactionBlobs)
    );
    assert_eq!(
        wallet_policy(WALLET_READ_TRANSACTION_BY_ID_V2),
        Some(AdvertisePolicy::AlwaysOn)
    );
}

#[test]
fn wallet_only_policies_do_not_satisfy_other_surfaces() {
    let readiness = ExplorerReadiness {
        wallet_query_online: true,
        canonical_store_online: true,
        materialized_view_store_online: true,
        prevout_resolution_online: true,
        ..ExplorerReadiness::default()
    };
    assert!(!AdvertisePolicy::RequiresBlockBlobs.explorer_satisfied(readiness));
    assert!(!AdvertisePolicy::RequiresTransactionBlobs.explorer_satisfied(readiness));
    assert!(!AdvertisePolicy::RequiresBlockBlobs.ingest_satisfied(true));
    assert!(!AdvertisePolicy::RequiresTransactionBlobs.ingest_satisfied(true));
}

#[test]
fn transaction_history_policies_require_typed_projection_readiness_and_wallet_query() {
    let available = ExplorerReadiness {
        wallet_query_online: true,
        transaction_history_available: true,
        ..ExplorerReadiness::default()
    };
    assert!(AdvertisePolicy::RequiresTransactionHistory.explorer_satisfied(available));
    assert!(!AdvertisePolicy::RequiresCompleteTransactionHistory.explorer_satisfied(available));

    let complete = ExplorerReadiness {
        transaction_history_complete: true,
        ..available
    };
    assert!(AdvertisePolicy::RequiresCompleteTransactionHistory.explorer_satisfied(complete));
    let disconnected = ExplorerReadiness {
        wallet_query_online: false,
        ..complete
    };
    assert!(!AdvertisePolicy::RequiresTransactionHistory.explorer_satisfied(disconnected));
    assert!(!AdvertisePolicy::RequiresCompleteTransactionHistory.explorer_satisfied(disconnected));
}

#[test]
fn explorer_value_pool_and_mempool_count_policies_require_runtime_support() {
    let readiness = ExplorerReadiness {
        wallet_query_online: true,
        materialized_view_store_online: true,
        ..ExplorerReadiness::default()
    };
    let wallet = WalletServerInfo {
        common: Some(zinder_proto::v1::ops::ServerInfo {
            capabilities: vec![
                zinder_proto::capabilities::Capability::ChainValuePools
                    .as_str()
                    .into(),
            ],
            ..Default::default()
        }),
        ..Default::default()
    };
    let summary = AdvertisePolicy::RequiresChainValuePools;
    assert_eq!(
        explorer_policy(EXPLORER_VALUE_POOL_SUMMARY_V1),
        Some(summary)
    );
    assert!(!summary.explorer_satisfied(readiness));
    assert!(!summary.explorer_satisfied_with_wallet(readiness, Some(&WalletServerInfo::default())));
    assert!(summary.explorer_satisfied_with_wallet(readiness, Some(&wallet)));
    assert!(!summary.explorer_satisfied_with_wallet(ExplorerReadiness::default(), Some(&wallet)));
    for capability in [
        EXPLORER_MEMPOOL_EVENT_COUNTS_V1,
        EXPLORER_VALUE_POOL_BALANCE_HISTORY_V1,
    ] {
        assert_eq!(explorer_policy(capability), Some(AdvertisePolicy::Unserved));
    }
    assert!(!AdvertisePolicy::Unserved.explorer_satisfied_with_wallet(readiness, Some(&wallet)));
}

/// Capabilities retired because no packaged consumer owned them.
///
/// The strings stay out of the registry so no runtime can advertise them again
/// by accident. ADR-0017 records the ownership evidence and the old-to-new
/// mapping.
const RETIRED_CAPABILITIES: &[&str] = &[
    "wallet.read.transparent_outputs_by_outpoint_v1",
    "wallet.read.transparent_spends_by_outpoint_v1",
    "wallet.read.transparent_unspent_outputs_by_outpoint_v1",
    "wallet.read.transparent_utxo_set_summary_v1",
    "wallet.read.transparent_utxo_set_commitment_v1",
    "wallet.mempool.transparent_outputs_by_address_v1",
    "wallet.mempool.transparent_spends_by_outpoint_v1",
    "wallet.mempool.transparent_outputs_by_outpoint_v1",
    "wallet.address.transparent_balance_v1",
    "explorer.utxo_set.summary_v1",
    "explorer.utxo_set.commitment_v1",
    "ingest.control.transparent_mempool_outputs_by_address_v1",
    "ingest.control.transparent_mempool_spends_by_outpoint_v1",
    "ingest.control.transparent_mempool_outputs_by_outpoint_v1",
];

#[test]
fn retired_transparent_capabilities_stay_out_of_the_registry() {
    let registered: BTreeSet<&str> = CAPABILITIES.iter().map(|spec| spec.string).collect();
    for retired in RETIRED_CAPABILITIES {
        assert!(
            !registered.contains(retired),
            "{retired} has no packaged consumer and must not be registered"
        );
    }
}
