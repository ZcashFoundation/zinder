//! Live regression: `MinedTransactionChainContext.consensus_branch_id` transitions correctly
//! across a network-upgrade activation height.
//!
//! The companion test [`mined_consensus_branch_id_parity`] checks a single
//! sampled tip coinbase; this file pins the *transition* itself by sampling
//! three heights that straddle the latest activation in the node's
//! upgrade table. The window is `(h-1, h, h+1)` where `h` is the highest
//! activation height that is reachable from the current chain tip.
//!
//! When the regtest node advertises an activation above height 1, the test
//! constructs and publishes a production canonical store from genesis through
//! the block after that activation, serves the projected pair, and asserts the
//! consensus branch id below the activation differs from the branch id at it,
//! and that every sampled height matches what
//! [`zinder_core::NetworkUpgradeActivations::consensus_branch_id_at`]
//! reports. A regtest node that activates every upgrade at height 1 has no
//! boundary to cross, so the test returns early.
//!
//! The serving pair requires genesis-complete history, which only fits test
//! budgets on regtest.
//!
//! Reference: `docs/adrs/0008-network-parameter-discovery.md`.
//!
//! [`mined_consensus_branch_id_parity`]: ./mined_consensus_branch_id_parity.rs

#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

use std::sync::Arc;

use eyre::{Result, eyre};
use zinder_core::{
    BlockHeight, ConsensusBranchId, MinedTransaction, Network, NetworkUpgradeActivations, TxStatus,
};
use zinder_query::{TransactionStatus, WalletQueryApi, WalletServingQuery};
use zinder_testkit::live::{init, require_live_for};

use crate::common::{
    coinbase_transaction_id, fetch_live_network_upgrade_activations, fetch_live_tip_height,
    live_serving_pair, regtest_generate_blocks,
};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
#[allow(
    clippy::too_many_lines,
    reason = "Boundary-crossing live test keeps env resolution, schedule discovery, construction, and three-height assertions in one auditable path."
)]
async fn mined_consensus_branch_id_advances_across_latest_activation_height() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };

    // Make sure regtest has at least one block above the activation window.
    let tip_before = fetch_live_tip_height(&env).await?;
    if tip_before.value() < 4 {
        regtest_generate_blocks(&env, 4_u32.saturating_sub(tip_before.value())).await?;
    }
    let tip_height = fetch_live_tip_height(&env).await?;

    let activations = fetch_live_network_upgrade_activations(&env).await?;

    // Default regtest activates every upgrade at height 1, so there is no
    // mid-chain boundary to cross; the assertion only applies where the node
    // advertises an activation above height 1.
    let Some(activation_height) = latest_reachable_activation_height(&activations, tip_height)
    else {
        return Ok(());
    };
    let window_start = BlockHeight::new(activation_height.value().saturating_sub(1));
    let window_end = BlockHeight::new(activation_height.value().saturating_add(1));

    let live = live_serving_pair(&env, &activations, window_end).await?;
    let wallet_query =
        WalletServingQuery::from_serving_pair_slot(live.slot, (), Arc::clone(&activations));

    let pre_activation_branch_id = read_coinbase_consensus_branch_id(&wallet_query, window_start)
        .await
        .map_err(|error| {
            eyre!(
                "could not resolve coinbase at pre-activation height {}: {error}",
                window_start.value()
            )
        })?;
    let activation_branch_id = read_coinbase_consensus_branch_id(&wallet_query, activation_height)
        .await
        .map_err(|error| {
            eyre!(
                "could not resolve coinbase at activation height {}: {error}",
                activation_height.value()
            )
        })?;
    let post_activation_branch_id = read_coinbase_consensus_branch_id(&wallet_query, window_end)
        .await
        .map_err(|error| {
            eyre!(
                "could not resolve coinbase at post-activation height {}: {error}",
                window_end.value()
            )
        })?;

    let expected_pre = activations.consensus_branch_id_at(window_start);
    let expected_activation = activations.consensus_branch_id_at(activation_height);
    let expected_post = activations.consensus_branch_id_at(window_end);

    assert_eq!(
        pre_activation_branch_id,
        expected_pre,
        "branch id at pre-activation height {} must match activations table",
        window_start.value()
    );
    assert_eq!(
        activation_branch_id,
        expected_activation,
        "branch id at activation height {} must match activations table",
        activation_height.value()
    );
    assert_eq!(
        post_activation_branch_id,
        expected_post,
        "branch id at post-activation height {} must match activations table",
        window_end.value()
    );
    assert_ne!(
        pre_activation_branch_id, activation_branch_id,
        "consensus branch id must change at the activation boundary; \
         pre={pre_activation_branch_id:?}, activation={activation_branch_id:?}"
    );
    assert_eq!(
        activation_branch_id, post_activation_branch_id,
        "no further activation should occur within the window; \
         activation={activation_branch_id:?}, post={post_activation_branch_id:?}"
    );
    drop(live.store_fixture);
    Ok(())
}

/// Highest `activation_height` strictly greater than 1 (so a meaningful
/// transition exists below it) that the chain tip has already crossed
/// (so construction can sample heights below, at, and above it).
fn latest_reachable_activation_height(
    activations: &NetworkUpgradeActivations,
    tip_height: BlockHeight,
) -> Option<BlockHeight> {
    let tip_value = tip_height.value();
    activations
        .activations()
        .iter()
        .map(|activation| activation.activation_height)
        .filter(|height| height.value() > 1)
        .filter(|height| height.value() < tip_value)
        .max_by_key(|height| height.value())
}

async fn read_coinbase_consensus_branch_id<QueryApi: WalletQueryApi>(
    wallet_query: &QueryApi,
    block_height: BlockHeight,
) -> Result<ConsensusBranchId> {
    let coinbase_transaction_id = coinbase_transaction_id(wallet_query, block_height).await?;
    let status = wallet_query
        .transaction(coinbase_transaction_id, None)
        .await?;
    let TransactionStatus {
        status: TxStatus::Mined(MinedTransaction { chain_context, .. }),
        ..
    } = status
    else {
        return Err(eyre!(
            "coinbase at height {} must surface as TxStatus::Mined; got {:?}",
            block_height.value(),
            status.status
        ));
    };
    Ok(chain_context.consensus_branch_id)
}
