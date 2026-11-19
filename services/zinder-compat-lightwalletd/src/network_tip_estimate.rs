//! The network chain height a lightwalletd client is told to expect.
//!
//! lightwalletd reports `LightdInfo.estimatedHeight` from its node's
//! `getblockchaininfo.estimatedheight`. Zebra computes that value from its own
//! tip: it adds the blocks the target spacing would have produced between the
//! tip's block time and now, never reporting less than the tip. This module
//! applies the same rule to the tip Zinder serves, with each era's spacing
//! taken from the admitted network-upgrade schedule, so the estimate keeps
//! rising while the served tip stands still, as it does during a node outage.

use zebra_chain::parameters::NetworkUpgrade;
use zinder_core::{BlockHeight, NetworkUpgradeActivations};

/// Estimates the network's chain height at `now_unix_seconds` from a tip at
/// `tip_height` mined at `tip_block_time_unix_seconds`.
///
/// Each era uses the target spacing of the upgrade active in it, so an
/// extrapolation that crosses an activation, such as NU7's change from 75 to
/// 25 seconds, changes pace there. The estimate is never below the tip, and a
/// tip whose block time is ahead of `now_unix_seconds` estimates the tip
/// itself, as Zebra does.
#[must_use]
pub fn estimated_network_height(
    activations: &NetworkUpgradeActivations,
    tip_height: BlockHeight,
    tip_block_time_unix_seconds: i64,
    now_unix_seconds: i64,
) -> BlockHeight {
    if now_unix_seconds <= tip_block_time_unix_seconds {
        return tip_height;
    }
    let eras = target_spacing_eras(activations);
    let mut reference_height = i64::from(tip_height.value());
    let mut reference_time = tip_block_time_unix_seconds;
    let mut spacing = spacing_at(&eras, tip_height);
    for &(change_height, next_spacing) in eras
        .iter()
        .filter(|(change_height, _)| *change_height > tip_height)
    {
        // The era ends at the block before the change; the interval ending
        // at the activation block uses the new spacing.
        let era_end = i64::from(change_height.value()) - 1;
        reference_time =
            reference_time.saturating_add((era_end - reference_height).saturating_mul(spacing));
        reference_height = era_end;
        if reference_time >= now_unix_seconds {
            break;
        }
        spacing = next_spacing;
    }
    let blocks_since_reference = (now_unix_seconds - reference_time).div_euclid(spacing);
    let estimate = reference_height.saturating_add(blocks_since_reference);
    u32::try_from(estimate.max(i64::from(tip_height.value())))
        .map_or(BlockHeight::new(u32::MAX), BlockHeight::new)
}

/// Target spacing in seconds before the first admitted activation.
fn genesis_spacing() -> i64 {
    NetworkUpgrade::Genesis.target_spacing().num_seconds()
}

/// The admitted activations that change the target spacing, in height
/// order, each with the spacing that starts there.
///
/// An upgrade this build does not know keeps the spacing before it.
fn target_spacing_eras(activations: &NetworkUpgradeActivations) -> Vec<(BlockHeight, i64)> {
    let mut spacing = genesis_spacing();
    let mut eras = Vec::new();
    // `activations()` is sorted by activation height.
    for activation in activations.activations() {
        if let Ok(upgrade) = NetworkUpgrade::try_from(activation.branch_id.value()) {
            spacing = upgrade.target_spacing().num_seconds().max(1);
        }
        eras.push((activation.activation_height, spacing));
    }
    eras
}

/// The target spacing of the era that contains `height`.
fn spacing_at(eras: &[(BlockHeight, i64)], height: BlockHeight) -> i64 {
    eras.iter()
        .rev()
        .find(|(change_height, _)| *change_height <= height)
        .map_or_else(genesis_spacing, |(_, spacing)| *spacing)
}

#[cfg(test)]
mod tests {
    use zinder_core::{
        BlockHeight, ConsensusBranchId, Network, NetworkUpgradeActivation,
        NetworkUpgradeActivations,
    };

    use super::estimated_network_height;

    /// Blossom (75-second spacing) at 100 and NU7 (25-second spacing) at
    /// 1,000, as a node of this network admits them.
    fn activations() -> Result<NetworkUpgradeActivations, Box<dyn std::error::Error>> {
        Ok(NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(0x2bb4_0e60),
                    activation_height: BlockHeight::new(100),
                    name: "Blossom".to_owned(),
                },
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(0x7719_0ad9),
                    activation_height: BlockHeight::new(1_000),
                    name: "NU7".to_owned(),
                },
            ],
        )?)
    }

    const TIP_TIME: i64 = 1_800_000_000;

    #[test]
    fn a_fresh_tip_estimates_itself() -> Result<(), Box<dyn std::error::Error>> {
        let activations = activations()?;
        let tip = BlockHeight::new(500);
        assert_eq!(
            estimated_network_height(&activations, tip, TIP_TIME, TIP_TIME),
            tip
        );
        // Less than one target spacing has passed.
        assert_eq!(
            estimated_network_height(&activations, tip, TIP_TIME, TIP_TIME + 74),
            tip
        );
        // A tip mined in the future of the local clock is the estimate.
        assert_eq!(
            estimated_network_height(&activations, tip, TIP_TIME, TIP_TIME - 600),
            tip
        );
        Ok(())
    }

    #[test]
    fn an_old_tip_extrapolates_at_its_era_spacing() -> Result<(), Box<dyn std::error::Error>> {
        let activations = activations()?;
        // 40 Blossom-era blocks of 75 seconds.
        assert_eq!(
            estimated_network_height(
                &activations,
                BlockHeight::new(500),
                TIP_TIME,
                TIP_TIME + 3_000
            ),
            BlockHeight::new(540)
        );
        // 120 NU7-era blocks of 25 seconds.
        assert_eq!(
            estimated_network_height(
                &activations,
                BlockHeight::new(2_000),
                TIP_TIME,
                TIP_TIME + 3_000
            ),
            BlockHeight::new(2_120)
        );
        Ok(())
    }

    #[test]
    fn an_extrapolation_across_an_activation_changes_pace_there()
    -> Result<(), Box<dyn std::error::Error>> {
        let activations = activations()?;
        // From 990, nine blocks of 75 seconds reach 999 at 675 seconds; the
        // remaining 2,325 seconds are 93 blocks of 25 seconds.
        assert_eq!(
            estimated_network_height(
                &activations,
                BlockHeight::new(990),
                TIP_TIME,
                TIP_TIME + 3_000
            ),
            BlockHeight::new(999 + 93)
        );
        // Before the era boundary, the old spacing still applies.
        assert_eq!(
            estimated_network_height(
                &activations,
                BlockHeight::new(990),
                TIP_TIME,
                TIP_TIME + 600
            ),
            BlockHeight::new(998)
        );
        Ok(())
    }

    #[test]
    fn an_unknown_upgrade_keeps_the_spacing_before_it() -> Result<(), Box<dyn std::error::Error>> {
        let activations = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(0x2bb4_0e60),
                    activation_height: BlockHeight::new(100),
                    name: "Blossom".to_owned(),
                },
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(0x0bad_cafe),
                    activation_height: BlockHeight::new(200),
                    name: "Future".to_owned(),
                },
            ],
        )?;
        assert_eq!(
            estimated_network_height(
                &activations,
                BlockHeight::new(500),
                TIP_TIME,
                TIP_TIME + 3_000
            ),
            BlockHeight::new(540)
        );
        Ok(())
    }
}
