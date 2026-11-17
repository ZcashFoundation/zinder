#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

use std::num::NonZeroU32;

use eyre::{Result, eyre};
use zinder_core::{
    BlockHeight, CommitmentTreeAccumulator, Network, NetworkUpgradeActivations, ShieldedProtocol,
    SubtreeRootArtifact, SubtreeRootIndex, SubtreeRootRange,
};
use zinder_ingest::{RawBlobPolicy, prepare_canonical_block};
use zinder_source::NodeSource;
use zinder_testkit::live::{init, require_live_for};

use crate::common::{fetch_live_network_upgrade_activations, zebra_source_for_live_env};

/// Ingest commits the subtree roots it derives from each block's note
/// commitments, so a derived root must equal the node's root, byte for byte.
#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn latest_completed_subtree_roots_derive_from_their_completing_blocks() -> Result<()> {
    let _guard = init();
    // A Regtest chain does not fill a 65,536-leaf subtree.
    let Some(env) = require_live_for(&[Network::ZcashTestnet, Network::ZcashMainnet])? else {
        return Ok(());
    };
    let source = zebra_source_for_live_env(&env)?;
    let activations = fetch_live_network_upgrade_activations(&env).await?;
    let tip = source.tip_id().await?;
    let tip_metadata = source
        .fetch_chain_checkpoint_for_block(tip, &activations)
        .await?
        .tip_metadata();
    let mut checked_pools = 0_u32;
    for protocol in [
        ShieldedProtocol::Sapling,
        ShieldedProtocol::Orchard,
        ShieldedProtocol::Ironwood,
    ] {
        let Some(latest_index) = tip_metadata
            .completed_subtree_count(protocol)
            .checked_sub(1)
        else {
            continue;
        };
        let node_roots = source
            .fetch_subtree_root_range(SubtreeRootRange::new(
                protocol,
                SubtreeRootIndex::new(latest_index),
                NonZeroU32::MIN,
            ))
            .await?;
        let [node_root] = node_roots.subtree_roots.as_slice() else {
            return Err(eyre!(
                "{protocol:?} subtree {latest_index} returned {} roots",
                node_roots.subtree_roots.len()
            ));
        };
        let completing_height = node_root.completing_block_height;
        let derived_roots =
            derive_completed_subtree_roots(&source, &activations, completing_height).await?;
        let derived_root = derived_roots
            .iter()
            .find(|root| root.protocol == protocol && root.subtree_index == node_root.subtree_index)
            .ok_or_else(|| {
                eyre!(
                    "block {} completes no {protocol:?} subtree {latest_index}; derived {derived_roots:?}",
                    completing_height.value()
                )
            })?;
        assert_eq!(
            derived_root.root_hash, node_root.root_hash,
            "{protocol:?} subtree {latest_index} root"
        );
        assert_eq!(derived_root.completing_block_height, completing_height);
        checked_pools += 1;
    }
    if checked_pools == 0 {
        return Err(eyre!(
            "no pool has completed a subtree at tip {}; point the test at a synced node",
            tip.height.value()
        ));
    }
    Ok(())
}

/// Derives the subtree roots that the block at `height` completes from its note
/// commitments and the node's tree state for the block before it.
async fn derive_completed_subtree_roots(
    source: &impl NodeSource,
    activations: &NetworkUpgradeActivations,
    height: BlockHeight,
) -> Result<Vec<SubtreeRootArtifact>> {
    let predecessor_height = height
        .value()
        .checked_sub(1)
        .map(BlockHeight::new)
        .ok_or_else(|| eyre!("no block precedes genesis"))?;
    let predecessor = source
        .fetch_chain_checkpoint(predecessor_height, activations)
        .await?;
    let block = source.fetch_block_at(height).await?;
    let compact_block =
        prepare_canonical_block(&block, activations, RawBlobPolicy::None)?.partial_compact_block;
    let mut sapling = Vec::new();
    let mut orchard = Vec::new();
    let mut ironwood = Vec::new();
    for transaction in compact_block.transactions() {
        let shielded = &transaction.data;
        sapling.extend(
            shielded
                .sapling_outputs
                .iter()
                .map(|output| output.commitment),
        );
        orchard.extend(
            shielded
                .orchard_actions
                .iter()
                .map(|action| action.commitment),
        );
        ironwood.extend(
            shielded
                .ironwood_actions
                .iter()
                .map(|action| action.commitment),
        );
    }
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        predecessor.block_id.height,
        &predecessor.frontiers,
        activations,
    )?;
    accumulator.append_block_commitments(height, &sapling, &orchard, &ironwood)?;
    Ok(accumulator.completed_subtree_roots(block.hash))
}
