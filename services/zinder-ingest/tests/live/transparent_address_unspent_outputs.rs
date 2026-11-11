#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

//! Live regtest assertion that the typed
//! `WalletQueryApi::transparent_address_unspent_outputs` surfaces UTXOs
//! paid to the seed-derived test address.
//!
//! The production canonical store and wallet projection are built from
//! genesis before the query runs.
//!
//! # Operator precondition
//!
//! Zebra's regtest sidecar must mine to the test address derived from
//! [`TRANSPARENT_BROADCAST_TEST_SEED`] (see `live::mempool_broadcast_cycle`).
//! The exact address is logged by the test on startup.

use std::sync::Arc;

use eyre::{Result, eyre};
use sha2::{Digest, Sha256};
use zinder_core::TransparentAddressScriptHash;
use zinder_core::{BlockHeight, Network};
use zinder_query::{TransparentAddressUnspentOutputsRequest, WalletQueryApi, WalletServingQuery};
use zinder_testkit::live::{init, require_live_for};
use zinder_testkit::{TRANSPARENT_BROADCAST_TEST_SEED, TransparentTestKey};

use crate::common::{
    fetch_live_network_upgrade_activations, fetch_live_tip_height, live_serving_pair,
    regtest_generate_blocks,
};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn transparent_address_unspent_outputs_surface_through_typed_wallet_query() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };
    let test_key = TransparentTestKey::from_seed(&TRANSPARENT_BROADCAST_TEST_SEED)
        .map_err(|error| eyre!("could not derive test key: {error}"))?;
    let test_address = test_key.address_base58();
    tracing::info!(
        target: "zinder::live",
        event = "transparent_address_unspent_outputs_test_address",
        address = %test_address,
        "regtest UTXO query: configure mining.miner_address to this value"
    );

    // Mine enough blocks that the seed-derived address has at least one
    // confirmed coinbase before construction.
    let tip_before = fetch_live_tip_height(&env).await?;
    if tip_before.value() == 0 {
        regtest_generate_blocks(&env, 1).await?;
    }
    let tip_height = fetch_live_tip_height(&env).await?;

    let activations = fetch_live_network_upgrade_activations(&env).await?;
    let live = live_serving_pair(&env, &activations, tip_height).await?;
    let wallet_query =
        WalletServingQuery::from_serving_pair_slot(live.slot, (), Arc::clone(&activations));
    let mut hasher = Sha256::new();
    hasher.update(test_key.address_script_bytes());
    let address_script_hash = TransparentAddressScriptHash::from_bytes(hasher.finalize().into());
    let response = wallet_query
        .transparent_address_unspent_outputs(
            TransparentAddressUnspentOutputsRequest {
                address_script_hash,
                start_height: BlockHeight::new(0),
            },
            None,
        )
        .await?;

    assert!(
        !response.outputs.is_empty(),
        "expected at least one UTXO paid to {test_address}; ensure Zebra mines its coinbase to that address"
    );
    for utxo in &response.outputs {
        assert_eq!(utxo.address_script_hash, address_script_hash);
    }
    drop(live.store_fixture);
    Ok(())
}
