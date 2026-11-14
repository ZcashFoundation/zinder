#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

use eyre::{Result, eyre};
use zebra_chain::block::Block as ZebraBlock;
use zebra_chain::serialization::{ZcashDeserializeInto, ZcashSerialize as _};
use zinder_core::{
    AuthDigest, BlockHash, BlockHeight, ChainEpoch, ChainEpochId, ChainTipMetadata, Network,
    RawTransactionBytes, TransactionId, UnixTimestampMillis,
};
use zinder_ingest::build_mempool_entry;
use zinder_source::{
    MempoolSourceEntry, NodeSource, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions,
};
use zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION;
use zinder_testkit::live::{LiveTestEnv, init, require_live};

/// Validates canonical hydration from a real Zebra-emitted transaction.
///
/// The decoded `MempoolEntry` must have raw bytes that round-trip,
/// structured wallet scan data and transparent overlays that match the parsed
/// transaction. The parsed identifiers must
/// agree with `zebra-chain`'s view.
///
/// This exercises the parsing pipeline used for an observed `Added` event
/// against the tip's coinbase transaction, so it needs no wallet setup. A
/// version-6 coinbase may pay the miner entirely in shielded notes, so the
/// transparent overlay must match the parsed output count, which can be zero.
/// The signed-transaction broadcast cycle is covered by
/// `mempool_broadcast_cycle.rs`.
#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn build_mempool_entry_decodes_real_zebra_coinbase_into_canonical_form() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live()? else {
        return Ok(());
    };
    let json_rpc = json_rpc_source(&env)?;

    let tip_height = NodeSource::tip_id(&json_rpc).await?.height;
    let coinbase = fetch_tip_coinbase(&json_rpc, tip_height).await?;

    let observed_at = UnixTimestampMillis::new(1_700_000_000_000);
    let source_entry = MempoolSourceEntry {
        transaction_id: coinbase.transaction_id,
        auth_digest: coinbase.auth_digest,
        raw_transaction_bytes: RawTransactionBytes::new(coinbase.raw_bytes.clone()),
        observed_at_unix_millis: observed_at,
    };
    let chain_epoch = synthetic_chain_epoch_at(env.network(), tip_height);
    let mempool_entry = build_mempool_entry(source_entry, chain_epoch)?;

    assert_eq!(mempool_entry.transaction_id(), coinbase.transaction_id);
    assert_eq!(mempool_entry.auth_digest(), coinbase.auth_digest);
    assert_eq!(mempool_entry.first_seen_unix_millis(), observed_at);
    assert_eq!(mempool_entry.first_seen_chain_epoch(), chain_epoch);
    assert_eq!(
        mempool_entry.raw_transaction_bytes().as_slice(),
        coinbase.raw_bytes.as_slice()
    );
    assert!(
        mempool_entry.transparent_spends().is_empty(),
        "coinbase contributes no transparent spends; got {} spends",
        mempool_entry.transparent_spends().len()
    );
    assert_eq!(
        mempool_entry.transparent_outputs().len(),
        coinbase.transparent_output_count,
        "transparent overlay must carry every transparent coinbase output"
    );
    for transparent_output in mempool_entry.transparent_outputs() {
        assert_eq!(
            transparent_output.outpoint.transaction_id, coinbase.transaction_id,
            "transparent overlay outpoint must reference the coinbase txid"
        );
    }

    let scan_data = mempool_entry.compact_transaction_data();
    assert!(scan_data.transparent_inputs.is_empty());
    assert_eq!(
        scan_data.transparent_outputs.len(),
        mempool_entry.transparent_outputs().len()
    );
    Ok(())
}

fn json_rpc_source(env: &LiveTestEnv) -> Result<ZebraJsonRpcSource> {
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

struct ZebraCoinbase {
    transaction_id: TransactionId,
    auth_digest: Option<AuthDigest>,
    raw_bytes: Vec<u8>,
    transparent_output_count: usize,
}

async fn fetch_tip_coinbase(
    json_rpc: &ZebraJsonRpcSource,
    tip_height: BlockHeight,
) -> Result<ZebraCoinbase> {
    let source_block = json_rpc.fetch_block_at(tip_height).await?;
    let parsed_block: ZebraBlock = source_block
        .raw_block_bytes
        .as_slice()
        .zcash_deserialize_into()
        .map_err(|error| eyre!("zebra-chain block parse failed: {error}"))?;
    let coinbase_transaction = parsed_block
        .transactions
        .first()
        .ok_or_else(|| eyre!("tip block has no coinbase transaction"))?;
    let raw_bytes = coinbase_transaction
        .zcash_serialize_to_vec()
        .map_err(|error| eyre!("coinbase serialize failed: {error}"))?;
    Ok(ZebraCoinbase {
        transaction_id: TransactionId::from_bytes(coinbase_transaction.hash().0),
        auth_digest: coinbase_transaction
            .auth_digest()
            .map(|digest| AuthDigest::from_bytes(digest.0)),
        raw_bytes,
        transparent_output_count: coinbase_transaction.outputs().len(),
    })
}

fn synthetic_chain_epoch_at(network: Network, tip_height: BlockHeight) -> ChainEpoch {
    ChainEpoch {
        id: ChainEpochId::new(1),
        network,
        visible_tip_height: tip_height,
        visible_tip_hash: BlockHash::from_bytes([0x42; 32]),
        settled_tip_height: tip_height,
        settled_tip_hash: BlockHash::from_bytes([0x42; 32]),
        artifact_schema_version: CURRENT_ARTIFACT_SCHEMA_VERSION,
        tip_metadata: ChainTipMetadata::empty(),
        created_at: UnixTimestampMillis::new(1_700_000_000_000),
    }
}
