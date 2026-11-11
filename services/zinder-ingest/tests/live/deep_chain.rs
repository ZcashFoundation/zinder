#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

use std::sync::Arc;

use eyre::{Result, eyre};
use zinder_core::{BlockHeight, BlockSelector, Network, TxStatus};
use zinder_query::{WalletQueryApi, WalletServingQuery};
use zinder_testkit::live::{init, require_live_for};

use crate::common::{
    fetch_live_network_upgrade_activations, fetch_live_tip_height, live_serving_pair,
};

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn deep_chain_serves_every_block_and_transactions_by_id() -> Result<()> {
    let _guard = init();
    // Constructing [1, tip] only fits in CI budgets on regtest. Hosted
    // networks use the checkpoint-bounded construction tests instead.
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };

    let tip_height = fetch_live_tip_height(&env).await?;
    if tip_height.value() < 10 {
        return Err(eyre!(
            "deep-chain test needs at least 10 blocks; got tip {}; advance the chain (e.g. via the regtest `generate` RPC)",
            tip_height.value()
        ));
    }

    let activations = fetch_live_network_upgrade_activations(&env).await?;
    let live = live_serving_pair(&env, &activations, tip_height).await?;
    let wallet_query =
        WalletServingQuery::from_serving_pair_slot(live.slot, (), Arc::clone(&activations));
    let chain_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;
    assert_eq!(chain_epoch.network, env.network());
    assert_eq!(chain_epoch.visible_tip_height, tip_height);

    for height_value in [1_u32, 5, tip_height.value() / 2, tip_height.value()] {
        let height = BlockHeight::new(height_value);
        let block = wallet_query.compact_block_at(height, None).await?;
        let transaction_id = block
            .compact_block
            .transactions()
            .first()
            .ok_or_else(|| eyre!("compact block at height {height_value} has no transactions"))?
            .transaction_id;
        let status = wallet_query.transaction(transaction_id, None).await?;
        let TxStatus::Mined(mined) = status.status else {
            return Err(eyre!(
                "transaction at height {height_value} must be mined, got {:?}",
                status.status
            ));
        };
        assert_eq!(mined.location.block_height, height);
        assert!(
            mined
                .raw_transaction_bytes
                .as_ref()
                .is_some_and(|bytes| !bytes.is_empty()),
            "by-txid transaction at height {height_value} returned no payload"
        );
    }

    for height_value in 1..=tip_height.value() {
        let height = BlockHeight::new(height_value);
        let block = wallet_query.compact_block_at(height, None).await?;
        assert_eq!(block.compact_block.height(), height);
        let header = wallet_query
            .block_header_by_selector(BlockSelector::Height(height), None)
            .await?
            .block_header;
        assert_eq!(header.block_id.hash, block.compact_block.block_hash());
        assert_eq!(
            header.previous_block_hash,
            block.compact_block.previous_block_hash()
        );
    }
    drop(live.store_fixture);
    Ok(())
}
