#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

use std::time::Duration;

use eyre::{Result, eyre};
use zinder_core::{Network, RawTransactionBytes, TransactionBroadcastOutcome};
use zinder_query::{OrderedBroadcaster, WalletQueryApi, WalletServingQuery};
use zinder_source::{NodeAuth, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};
use zinder_testkit::live::{init, require_live_for};

use crate::common::{
    fetch_live_network_upgrade_activations, fetch_live_tip_height, live_serving_pair,
};

/// Address that refuses every connection, standing in for a stopped node.
const REFUSING_NODE_ADDR: &str = "http://127.0.0.1:9";

/// With `[node]` stopped, a broadcast reaches the node configured as its
/// fallback, and that node becomes the preferred endpoint.
#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn broadcast_reaches_the_fallback_node_while_the_first_node_is_down() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };
    let tip_height = fetch_live_tip_height(&env).await?;
    let activations = fetch_live_network_upgrade_activations(&env).await?;
    let live = live_serving_pair(&env, &activations, tip_height).await?;
    let stopped_node = ZebraJsonRpcSource::with_options(
        env.target.network,
        REFUSING_NODE_ADDR,
        NodeAuth::None,
        ZebraJsonRpcSourceOptions {
            request_timeout: Duration::from_secs(2),
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )?;
    let nodes = OrderedBroadcaster::new(stopped_node, [live.source]);
    nodes.verify(&live.slot).await?;
    let wallet_query = WalletServingQuery::from_serving_pair_slot(live.slot)
        .with_node_broadcast(Some(nodes.clone()));

    // A byte that is no transaction makes the reachable node answer with a
    // refusal, which proves the broadcast reached it without spending funds.
    let outcome = wallet_query
        .broadcast_transaction(RawTransactionBytes::new(vec![0x00]))
        .await?;

    if matches!(outcome, TransactionBroadcastOutcome::Accepted(_)) {
        return Err(eyre!(
            "the node accepted a one-byte transaction: {outcome:?}"
        ));
    }
    assert_eq!(nodes.preferred_position(), 1);
    Ok(())
}
