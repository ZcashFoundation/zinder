#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

use eyre::{Result, eyre};
use zebra_chain::parameters::Network as ZebraNetwork;
use zebra_chain::serialization::ZcashDeserializeInto;
use zebra_chain::transparent::Address as ZebraTransparentAddress;
use zinder_core::{BlockHeight, Network, TransactionId, TransparentOutPoint};
use zinder_source::{
    NodeSource, TransparentAddressLookup, TransparentAddressRowLimit, ZebraJsonRpcSource,
    ZebraJsonRpcSourceOptions,
};
use zinder_testkit::live::{LiveTestEnv, init, require_live_for};

/// Blocks below the tip searched for a coinbase that pays a transparent address.
const COINBASE_SEARCH_DEPTH: usize = 20;

/// A transparent coinbase output the node mined.
struct MinedCoinbaseOutput {
    address: ZebraTransparentAddress,
    outpoint: TransparentOutPoint,
    value_zat: u64,
    block_height: BlockHeight,
}

/// The three lookups agree on a coinbase output near the tip. The output is
/// immature, so no concurrent test can spend it between the lookups.
#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn transparent_address_lookups_agree_on_a_mined_coinbase_output() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };
    let source = zebra_source(&env)?;
    let mined = find_transparent_coinbase_output(&source).await?;
    let lookup = TransparentAddressLookup::new(env.target.network, [mined.address])?;

    let balance = source.fetch_transparent_address_balance(&lookup).await?;
    let transaction_ids = source
        .fetch_transparent_address_transaction_ids(&lookup, mined.block_height, mined.block_height)
        .await?;
    let utxos = source
        .fetch_transparent_address_utxos(&lookup, mined.block_height)
        .await?;
    let prefix = source
        .fetch_transparent_address_utxo_prefix(
            &lookup,
            mined.block_height,
            TransparentAddressRowLimit::new(1).ok_or_else(|| eyre!("row limit"))?,
        )
        .await?;

    assert!(balance.balance_zat >= mined.value_zat, "{balance:?}");
    assert!(balance.received_zat >= balance.balance_zat, "{balance:?}");
    assert!(
        transaction_ids.contains(&mined.outpoint.transaction_id),
        "getaddresstxids at height {} omits the coinbase",
        mined.block_height.value()
    );
    let first = utxos
        .outputs
        .first()
        .ok_or_else(|| eyre!("getaddressutxos omits the coinbase output"))?;
    assert_eq!(first.outpoint, mined.outpoint);
    assert_eq!(first.value_zat, mined.value_zat);
    assert!(utxos.tip.height >= mined.block_height);
    for output in &utxos.outputs {
        assert_eq!(output.address, mined.address);
        assert!(output.block_height >= mined.block_height);
    }
    assert_eq!(prefix.as_slice(), std::slice::from_ref(first));
    Ok(())
}

/// Returns the newest coinbase output near the tip that pays a transparent
/// address a nonzero value.
async fn find_transparent_coinbase_output(
    source: &ZebraJsonRpcSource,
) -> Result<MinedCoinbaseOutput> {
    let tip_height = NodeSource::tip_id(source).await?.height.value();
    let address_network = ZebraNetwork::new_default_testnet();
    for height in (1..=tip_height).rev().take(COINBASE_SEARCH_DEPTH) {
        let block_height = BlockHeight::new(height);
        let block: zebra_chain::block::Block = source
            .fetch_block_at(block_height)
            .await?
            .raw_block_bytes
            .as_slice()
            .zcash_deserialize_into()
            .map_err(|error| eyre!("block {height} does not parse: {error}"))?;
        let coinbase = block
            .transactions
            .first()
            .ok_or_else(|| eyre!("block {height} has no coinbase"))?;
        let transaction_id = TransactionId::from_bytes(coinbase.hash().0);
        for (output_index, output) in (0_u32..).zip(coinbase.outputs()) {
            let value_zat = u64::from(output.value());
            if let Some(address) = output.address(&address_network)
                && value_zat > 0
            {
                return Ok(MinedCoinbaseOutput {
                    address,
                    outpoint: TransparentOutPoint::new(transaction_id, output_index),
                    value_zat,
                    block_height,
                });
            }
        }
    }
    Err(eyre!(
        "no coinbase within {COINBASE_SEARCH_DEPTH} blocks of tip {tip_height} pays a \
         transparent address; mine to a transparent miner address first"
    ))
}

fn zebra_source(env: &LiveTestEnv) -> Result<ZebraJsonRpcSource> {
    Ok(ZebraJsonRpcSource::with_options(
        env.target.network,
        &env.target.json_rpc_addr,
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )?)
}
