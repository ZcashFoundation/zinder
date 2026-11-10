#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

//! Live regression: mined transaction lookups report the node's consensus
//! branch id.
//!
//! `MinedTransactionChainContext.consensus_branch_id` returned by the typed
//! `WalletQueryApi::transaction` lookup must match what the running node's
//! upgrade activations say is active at the mined height.
//!
//! Closes the wire-level gap left open by `lightwalletd_grpc` integration
//! tests, which only exercise the in-process adapter against a synthetic
//! table. This test follows the end-to-end production path:
//!
//! 1. Fetch the activations from the running Zebra via
//!    `ZebraJsonRpcSource::fetch_network_upgrade_activations()`.
//! 2. Construct and publish a production canonical store from genesis to the
//!    tip, project its wallet store, and serve the pair.
//! 3. Pick the tip block's coinbase from its compact block.
//! 4. Look up that txid via `WalletQueryApi::transaction(...)`.
//! 5. Assert `MinedTransactionChainContext.consensus_branch_id == activations.consensus_branch_id_at(mined_height)`.
//!
//! The serving pair requires genesis-complete history, which only fits test
//! budgets on regtest.

use std::sync::Arc;

use eyre::{Result, eyre};
use zinder_core::{MinedTransaction, Network, TxStatus};
use zinder_query::{TransactionStatus, WalletQueryApi, WalletServingQuery};
use zinder_testkit::live::{init, require_live_for};

use crate::common::{
    coinbase_transaction_id, fetch_live_network_upgrade_activations, fetch_live_tip_height,
    live_serving_pair, regtest_generate_blocks,
};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn mined_details_consensus_branch_id_matches_node_upgrade_activations() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };

    let tip_before = fetch_live_tip_height(&env).await?;
    if tip_before.value() == 0 {
        regtest_generate_blocks(&env, 1).await?;
    }
    let tip_height = fetch_live_tip_height(&env).await?;

    let activations = fetch_live_network_upgrade_activations(&env).await?;
    let live = live_serving_pair(&env, &activations, tip_height).await?;
    let wallet_query =
        WalletServingQuery::from_serving_pair_slot(live.slot, (), Arc::clone(&activations));

    let coinbase_transaction_id = coinbase_transaction_id(&wallet_query, tip_height).await?;
    let status = wallet_query
        .transaction(coinbase_transaction_id, None)
        .await?;
    let TransactionStatus {
        status:
            TxStatus::Mined(MinedTransaction {
                location,
                chain_context,
                ..
            }),
        ..
    } = status
    else {
        return Err(eyre!(
            "tip coinbase {coinbase_transaction_id:?} must surface as TxStatus::Mined, got {:?}",
            status.status
        ));
    };

    assert_eq!(
        location.block_height, tip_height,
        "TxStatus::Mined location block_height must match the coinbase's block height"
    );
    let expected_branch_id = activations.consensus_branch_id_at(tip_height);
    assert_eq!(
        chain_context.consensus_branch_id,
        expected_branch_id,
        "MinedTransactionChainContext.consensus_branch_id must match the activations' branch \
         id at the mined height (mined_height={}, activations_say={:#010x}, \
         MinedTransactionChainContext_says={:#010x})",
        tip_height.value(),
        expected_branch_id,
        chain_context.consensus_branch_id,
    );
    drop(live.store_fixture);
    Ok(())
}
