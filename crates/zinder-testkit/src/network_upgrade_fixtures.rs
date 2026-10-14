//! Network-upgrade-activation fixtures and conversions for tests.
//!
//! Splits the activation-domain helpers out from
//! [`crate::transparent_signer`]: the signer module owns key derivation and
//! v5 transaction building, while this module owns the
//! [`zinder_core::NetworkUpgradeActivations`] shapes that tests need to drive
//! consensus-aware code paths (`MinedTransactionChainContext.consensus_branch_id`,
//! `GetLightdInfo`, transparent-signing branch-id selection).
//!
//! Two helpers live here:
//!
//! - [`sample_regtest_upgrade_activations`] returns a hand-built table that
//!   matches ZFND's `z3` regtest sidecar defaults
//!   (Overwinter..Canopy at 1, NU5 at 2, NU6 at 2, NU6.1 and NU6.2 at 3,
//!   NU6.3 at 603, and NU7 at 604). Intended for in-process integration tests that
//!   exercise `GetLightdInfo` or `MinedTransactionChainContext.consensus_branch_id` without a
//!   live node. Live tests must discover the activations from the running node,
//!   not hard-code them here.
//! - [`local_network_from_activations`] converts a node-discovered
//!   [`zinder_core::NetworkUpgradeActivations`] into the
//!   [`zcash_protocol::local_consensus::LocalNetwork`] shape consumed by
//!   `TransparentTestKey::from_seed_with_local_network`. Live tests that
//!   broadcast through Zebra MUST use this; the activations come from
//!   `ZebraJsonRpcSource::fetch_network_upgrade_activations()` and reflect
//!   what the running node is actually configured with. Mismatched heights
//!   produce an `incorrect consensus branch id` rejection from Zebra's
//!   mempool.

use zcash_protocol::consensus::BlockHeight;
use zcash_protocol::local_consensus::LocalNetwork;
use zinder_core::{
    BlockHeight as CoreBlockHeight, ConsensusBranchId, NetworkUpgradeActivation,
    NetworkUpgradeActivations,
};

/// Sample regtest [`NetworkUpgradeActivations`] for in-process tests.
///
/// Matches the activation heights of the captured ZFND `z3` regtest fixtures:
/// Overwinter..Canopy at 1, NU5 and NU6 at 2, NU6.1 and NU6.2 at 3, and
/// NU6.3 at 603 and NU7 at 604. Intended for integration tests that exercise
/// `GetLightdInfo` or `MinedTransactionChainContext.consensus_branch_id` without a live node.
/// Live tests must discover the activations from the running node, not
/// hard-code them here.
#[must_use]
#[allow(
    clippy::expect_used,
    reason = "The activation list is a hand-built fixture with unique branch ids; if construction ever fails it's a test-code bug, not a runtime condition."
)]
pub fn sample_regtest_upgrade_activations() -> NetworkUpgradeActivations {
    NetworkUpgradeActivations::new(
        zinder_core::Network::ZcashRegtest,
        vec![
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x5ba8_1b19),
                activation_height: CoreBlockHeight::new(1),
                name: "Overwinter".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x76b8_09bb),
                activation_height: CoreBlockHeight::new(1),
                name: "Sapling".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x2bb4_0e60),
                activation_height: CoreBlockHeight::new(1),
                name: "Blossom".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0xf5b9_230b),
                activation_height: CoreBlockHeight::new(1),
                name: "Heartwood".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0xe9ff_75a6),
                activation_height: CoreBlockHeight::new(1),
                name: "Canopy".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0xc2d6_d0b4),
                activation_height: CoreBlockHeight::new(2),
                name: "NU5".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0xc8e7_1055),
                activation_height: CoreBlockHeight::new(2),
                name: "NU6".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x4dec_4df0),
                activation_height: CoreBlockHeight::new(3),
                name: "NU6.1".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x5437_f330),
                activation_height: CoreBlockHeight::new(3),
                name: "NU6.2".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x37a5_165b),
                activation_height: CoreBlockHeight::new(603),
                name: "NU6.3".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x7719_0ad9),
                activation_height: CoreBlockHeight::new(604),
                name: "NU7".to_owned(),
            },
        ],
    )
    .expect("hand-built regtest activations must have unique branch ids")
}

/// Builds a [`LocalNetwork`] from a node-discovered upgrade table.
///
/// Populates each consensus upgrade's activation height from the entry whose
/// `name` matches (case-insensitive). Upgrades the table does not advertise
/// stay `None`, which `zcash_primitives` reads as "not yet activated". Use
/// this for any signing path that needs to match the running node's active
/// consensus rules. The activations come from
/// `ZebraJsonRpcSource::fetch_network_upgrade_activations()` and reflect the
/// node's `getblockchaininfo.upgrades` advertisement.
#[must_use]
pub fn local_network_from_activations(activations: &NetworkUpgradeActivations) -> LocalNetwork {
    let lookup = |name: &str| {
        activations
            .activation_height_by_name(name)
            .map(|height| BlockHeight::from_u32(height.value()))
    };
    LocalNetwork {
        overwinter: lookup("Overwinter"),
        sapling: lookup("Sapling"),
        blossom: lookup("Blossom"),
        heartwood: lookup("Heartwood"),
        canopy: lookup("Canopy"),
        nu5: lookup("NU5"),
        nu6: lookup("NU6"),
        nu6_1: lookup("NU6.1"),
        nu6_2: lookup("NU6.2"),
        nu6_3: lookup("NU6.3"),
        nu7: lookup("NU7"),
    }
}
