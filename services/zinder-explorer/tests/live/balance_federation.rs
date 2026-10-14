#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

//! Live federation coverage for a canonical transparent-address balance.
//!
//! This consumer suite owns the public wallet-to-Explorer boundary. Live
//! mempool mutation remains covered by the canonical writer control suite;
//! this test intentionally does not reconstruct writer internals.

use std::{net::SocketAddr, num::NonZeroU32, sync::Arc, time::Duration};

use eyre::{Result, eyre};
use sha2::{Digest, Sha256};
use tempfile::{TempDir, tempdir};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, transport::Channel};
use zebra_chain::{block::Block as ZebraBlock, serialization::ZcashDeserializeInto};
use zinder_core::{BlockHeight, Network, TransparentAddressScriptHash};
use zinder_ingest::run_bulk_catchup;
use zinder_proto::v1::wallet::{
    AddressLookup, TransparentAddressBalanceRequest, address_lookup,
    wallet_query_client::WalletQueryClient,
};
use zinder_query::{
    ServerInfoSettings, TransparentAddressUnspentOutputsRequest, WalletQuery, WalletQueryApi,
    WalletQueryGrpcAdapter,
};
use zinder_source::{NodeSource as _, SourceBlock};
use zinder_store::PrimaryChainStore;
use zinder_testkit::{
    live::{LiveTestEnv, init, require_live_for},
    sample_regtest_upgrade_activations,
};

use crate::common::{
    fetch_live_network_upgrade_activations, fetch_live_tip_height, live_bulk_catchup_run_config,
    zebra_source_from_bulk_catchup,
};

const BACKFILL_DEPTH_BLOCKS: u32 = 50;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn federated_balance_matches_visible_utxo_sum() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[
        Network::ZcashRegtest,
        Network::ZcashTestnet,
        Network::ZcashMainnet,
    ])?
    else {
        return Ok(());
    };
    let (tempdir, store, address_script_hash, from_height) = catch_up_and_sample(&env).await?;
    let wallet_query = WalletQuery::new(store, (), Arc::new(sample_regtest_upgrade_activations()));
    let expected = wallet_query
        .transparent_address_unspent_outputs(
            TransparentAddressUnspentOutputsRequest {
                address_script_hash,
                start_height: from_height,
            },
            None,
        )
        .await?
        .outputs
        .iter()
        .map(|output| output.value_zat)
        .fold(0_u64, u64::saturating_add);
    let (address, mut server) = serve_wallet_query(wallet_query).await?;
    let response = WalletQueryClient::new(
        Channel::from_shared(format!("http://{address}"))?
            .connect()
            .await?,
    )
    .transparent_address_balance(Request::new(TransparentAddressBalanceRequest {
        addresses: vec![AddressLookup {
            selector: Some(address_lookup::Selector::ScriptHash(
                address_script_hash.as_bytes().to_vec(),
            )),
        }],
        at_epoch_id: None,
    }))
    .await?
    .into_inner();
    assert_eq!(response.confirmed_zat, expected);
    assert_eq!(response.unconfirmed_delta_zat, 0);
    assert_eq!(response.address_count, 1);
    assert!(
        response
            .chain_view
            .and_then(|view| view.chain_epoch)
            .is_some()
    );
    server.abort();
    let _ = (&mut server).await;
    drop(tempdir);
    Ok(())
}

async fn catch_up_and_sample(
    env: &LiveTestEnv,
) -> Result<(
    TempDir,
    PrimaryChainStore,
    TransparentAddressScriptHash,
    BlockHeight,
)> {
    let tip = fetch_live_tip_height(env).await?;
    if tip.value() <= BACKFILL_DEPTH_BLOCKS {
        return Err(eyre!(
            "upstream tip {} is too low for the balance fixture",
            tip.value()
        ));
    }
    let checkpoint_height = BlockHeight::new(tip.value() - BACKFILL_DEPTH_BLOCKS - 1);
    let from_height = BlockHeight::new(checkpoint_height.value() + 1);
    let tempdir = tempdir()?;
    let storage_path = tempdir.path().join("zinder-store");
    let activations = fetch_live_network_upgrade_activations(env).await?;
    let mut config = live_bulk_catchup_run_config(
        env,
        &storage_path,
        from_height,
        tip,
        NonZeroU32::new(1_000).ok_or_else(|| eyre!("test batch size is zero"))?,
        true,
        activations,
    );
    let source = zebra_source_from_bulk_catchup(&config)?;
    config.checkpoint = Some(
        source
            .fetch_chain_checkpoint(checkpoint_height, &config.network_upgrade_activations)
            .await?,
    );
    run_bulk_catchup(&config, &source)
        .await?
        .ok_or_else(|| eyre!("bulk catchup did not commit"))?;
    let block = source.fetch_block_at(tip).await?;
    let script_hash = sample_coinbase_script_hash(&block)?;
    let store = PrimaryChainStore::open(&storage_path, config.canonical_store_options())?;
    Ok((tempdir, store, script_hash, from_height))
}

fn sample_coinbase_script_hash(block: &SourceBlock) -> Result<TransparentAddressScriptHash> {
    let parsed: ZebraBlock = block.raw_block_bytes.as_slice().zcash_deserialize_into()?;
    let outputs = parsed
        .transactions
        .first()
        .ok_or_else(|| eyre!("tip block has no coinbase"))?
        .outputs();
    let output = outputs
        .iter()
        .find(|output| !output.lock_script.as_raw_bytes().is_empty())
        .ok_or_else(|| eyre!("tip coinbase has no transparent output"))?;
    let mut hasher = Sha256::new();
    hasher.update(output.lock_script.as_raw_bytes());
    Ok(TransparentAddressScriptHash::from_bytes(
        hasher.finalize().into(),
    ))
}

async fn serve_wallet_query(
    wallet_query: WalletQuery<PrimaryChainStore>,
) -> Result<(SocketAddr, JoinHandle<Result<(), tonic::transport::Error>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                WalletQueryGrpcAdapter::new(wallet_query, ServerInfoSettings::default())
                    .into_server(),
            )
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    for _ in 0..40 {
        if Channel::from_shared(format!("http://{address}"))?
            .connect()
            .await
            .is_ok()
        {
            return Ok((address, server));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    server.abort();
    Err(eyre!("wallet test server did not become reachable"))
}
