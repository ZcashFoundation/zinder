#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

use std::sync::Arc;

use eyre::{Result, eyre};
use zinder_core::Network;
use zinder_core::wire::encode_zinder_native_chain_name;
use zinder_core::{BlockHeight, BlockHeightRange};
use zinder_query::{WalletQueryApi, WalletServingQuery};
use zinder_testkit::live::{init, require_live_for};

use crate::common::{
    coinbase_transaction_id, fetch_live_network_upgrade_activations, fetch_live_tip_height,
    live_serving_pair,
};

const TRANSACTION_LOOKUP_ITERATIONS: u32 = 100;

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
#[allow(
    clippy::too_many_lines,
    reason = "single calibration test reports five read-path measurements end-to-end against a real node; splitting per-measurement obscures the operator-facing trace line at the bottom"
)]
async fn read_endpoint_latency_baseline() -> Result<()> {
    let _guard = init();
    // The baseline constructs [1, tip], which only fits in CI budgets on
    // regtest.
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };

    let tip_height = fetch_live_tip_height(&env).await?;
    if tip_height.value() < 50 {
        return Err(eyre!(
            "latency baseline test needs at least 50 blocks; got tip {}",
            tip_height.value()
        ));
    }

    let activations = fetch_live_network_upgrade_activations(&env).await?;
    let live = live_serving_pair(&env, &activations, tip_height).await?;
    let wallet_query = WalletServingQuery::from_probed_node_source(
        live.slot,
        live.source,
        Arc::clone(&activations),
    )?;

    let measurement_start = std::time::Instant::now();
    let _latest = wallet_query.visible_tip_block(None).await?;
    let visible_tip_block_micros = measurement_start.elapsed().as_micros();

    let measurement_start = std::time::Instant::now();
    let _block = wallet_query
        .compact_block_at(BlockHeight::new(1), None)
        .await?;
    let compact_block_at_micros = measurement_start.elapsed().as_micros();

    let measurement_start = std::time::Instant::now();
    let range = wallet_query
        .compact_blocks_in_range(
            BlockHeightRange::inclusive(
                BlockHeight::new(1),
                BlockHeight::new(tip_height.value().min(50)),
            ),
            None,
        )
        .await?;
    let compact_block_range_50_micros = measurement_start.elapsed().as_micros();
    assert!(!range.compact_blocks.is_empty());

    let measurement_start = std::time::Instant::now();
    let _tree = wallet_query.tree_state_at(tip_height, None).await?;
    let tree_state_at_micros = measurement_start.elapsed().as_micros();

    // Pick a real txid from the constructed chain by reading the coinbase at
    // height 1 through its compact block. Then call `transaction()`
    // repeatedly so the average covers the location lookup, header read, and
    // consensus-branch-id path that powers `MinedTransactionChainContext`
    // enrichment against real Zcash blocks.
    let coinbase_txid = coinbase_transaction_id(&wallet_query, BlockHeight::new(1)).await?;

    let measurement_start = std::time::Instant::now();
    for _ in 0..TRANSACTION_LOOKUP_ITERATIONS {
        let _status = wallet_query.transaction(coinbase_txid, None).await?;
    }
    let transaction_total_micros = measurement_start.elapsed().as_micros();
    let transaction_avg_micros =
        transaction_total_micros / u128::from(TRANSACTION_LOOKUP_ITERATIONS);

    #[allow(
        clippy::print_stderr,
        reason = "calibration test reports measurements for operator review"
    )]
    {
        eprintln!(
            "live_latency_baseline network={} tip={} visible_tip_block={}us compact_block_at={}us \
             compact_block_range_50={}us tree_state_at={}us transaction_avg={}us \
             (n={} total={}us)",
            encode_zinder_native_chain_name(env.network()),
            tip_height.value(),
            visible_tip_block_micros,
            compact_block_at_micros,
            compact_block_range_50_micros,
            tree_state_at_micros,
            transaction_avg_micros,
            TRANSACTION_LOOKUP_ITERATIONS,
            transaction_total_micros,
        );
    }
    Ok(())
}
