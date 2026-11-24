//! Fixed-fence comparisons against the ordinary wallet-serving process.

use std::fmt::Debug;

use eyre::{Result, WrapErr as _, eyre};
use tonic::{Code, Response, Status, Streaming, transport::Channel};
use tonic_types::StatusExt as _;
use zinder_core::Network;
use zinder_proto::compat::lightwalletd::{
    self as rpc, BlockId, BlockRange, ChainSpec, Empty,
    compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_testkit::live::{init, optional_env, require_live_for};

use super::parity_against_lightwalletd::{open_parity_clients, parity_transparent_address};

type Client = CompactTxStreamerClient<Channel>;

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn methods_match_wallet_serving_at_the_same_fence() -> Result<()> {
    let _guard = init();
    if require_live_for(&[Network::ZcashRegtest])?.is_none() {
        return Ok(());
    }
    let Some(policy) = optional_env("ZINDER_TEST_LIGHT_WALLET_RAW_BLOB_POLICY")? else {
        return Ok(());
    };
    assert!(matches!(policy.as_str(), "none" | "transactions"));
    let node_stopped = std::env::var("ZINDER_TEST_LIGHT_WALLET_NODE_STOPPED")?.parse::<bool>()?;
    let (mut candidate, mut wallet) = open_parity_clients()
        .await?
        .ok_or_else(|| eyre!("both light-wallet and wallet-serving gRPC endpoints are required"))?;
    let tip = compare_tip(&mut candidate, &mut wallet).await?;
    compare_canonical_reads(&mut candidate, &mut wallet).await?;
    let address = parity_transparent_address()?;
    let start = tip
        .height
        .checked_sub(3)
        .ok_or_else(|| eyre!("chain is too short"))?;
    compare_balance(&mut candidate, &mut wallet, &address, node_stopped).await?;
    compare_history(
        &mut candidate,
        &mut wallet,
        rpc::TransparentAddressBlockFilter {
            address: address.clone(),
            range: Some(block_range(start, tip.height - 1)),
        },
        node_stopped,
    )
    .await?;
    let mined = compare_utxos(
        &mut candidate,
        &mut wallet,
        rpc::GetAddressUtxosArg {
            addresses: vec![address],
            start_height: start,
            max_entries: 2,
        },
        node_stopped,
    )
    .await?;
    let query = rpc::TxFilter {
        hash: mined.txid,
        ..Default::default()
    };
    let expected = wallet.get_transaction(query.clone()).await?.into_inner();
    assert_eq!(expected.height, mined.height);
    assert!(
        !expected.data.is_empty(),
        "the wallet must return the mined transaction bytes"
    );
    compare_node_read(
        "GetTransaction",
        candidate
            .get_transaction(query)
            .await
            .map(Response::into_inner),
        &expected,
        node_stopped && policy == "none",
    )?;

    let fee = candidate.get_standard_fee(Empty {}).await;
    if node_stopped {
        assert_node_unavailable("GetStandardFee", fee)?;
        assert_node_unavailable(
            "SendTransaction",
            candidate.send_transaction(expected).await,
        )?;
    } else {
        assert_eq!(
            fee?.into_inner(),
            wallet.get_standard_fee(Empty {}).await?.into_inner()
        );
    }
    Ok(())
}

async fn compare_tip(candidate: &mut Client, wallet: &mut Client) -> Result<BlockId> {
    let tip = wallet.get_latest_block(ChainSpec {}).await?.into_inner();
    assert_eq!(
        candidate.get_latest_block(ChainSpec {}).await?.into_inner(),
        tip
    );
    let candidate_info = candidate.get_lightd_info(Empty {}).await?.into_inner();
    let wallet_info = wallet.get_lightd_info(Empty {}).await?.into_inner();
    assert!(candidate_info.taddr_support);
    assert_eq!(candidate_info.block_height, tip.height);
    assert_eq!(candidate_info.chain_name, wallet_info.chain_name);
    assert_eq!(
        candidate_info.lightwallet_protocol_version,
        wallet_info.lightwallet_protocol_version
    );
    assert_eq!(candidate_info.upgrade_name, wallet_info.upgrade_name);
    assert_eq!(candidate_info.upgrade_height, wallet_info.upgrade_height);
    assert_eq!(
        candidate_info.consensus_branch_id,
        wallet_info.consensus_branch_id
    );
    assert_eq!(
        candidate_info.sapling_activation_height,
        wallet_info.sapling_activation_height
    );
    Ok(tip)
}

#[allow(deprecated, reason = "Wallets still call both nullifier RPCs.")]
async fn compare_canonical_reads(candidate: &mut Client, wallet: &mut Client) -> Result<()> {
    // The premine puts shielded coinbase outputs in this range on both nodes.
    let block = block_id(10);
    let range = block_range(6, 10);
    assert_eq!(
        candidate.get_block(block.clone()).await?.into_inner(),
        wallet.get_block(block.clone()).await?.into_inner()
    );
    // Height 10 pays a shielded receiver; height 145 pays only transparent outputs.
    for height in [10, 145] {
        let selector = block_id(height);
        let expected = wallet
            .get_block_nullifiers(selector.clone())
            .await?
            .into_inner();
        assert!(
            !expected.vtx.is_empty(),
            "unary nullifiers include the coinbase"
        );
        assert_eq!(
            candidate.get_block_nullifiers(selector).await?.into_inner(),
            expected
        );
    }
    let expected = messages(wallet.get_block_range(range.clone()).await).await?;
    assert_eq!(expected.len(), 5);
    assert_eq!(
        messages(candidate.get_block_range(range.clone()).await).await?,
        expected
    );
    let nullifiers = messages(wallet.get_block_range_nullifiers(range.clone()).await).await?;
    assert_eq!(nullifiers.len(), 5);
    assert_eq!(
        messages(candidate.get_block_range_nullifiers(range).await).await?,
        nullifiers
    );
    assert_eq!(
        candidate.get_tree_state(block.clone()).await?.into_inner(),
        wallet.get_tree_state(block).await?.into_inner()
    );
    assert_eq!(
        candidate
            .get_latest_tree_state(Empty {})
            .await?
            .into_inner(),
        wallet.get_latest_tree_state(Empty {}).await?.into_inner()
    );
    for pool in [
        rpc::ShieldedProtocol::Sapling,
        rpc::ShieldedProtocol::Orchard,
        rpc::ShieldedProtocol::Ironwood,
    ] {
        let request = rpc::GetSubtreeRootsArg {
            shielded_protocol: pool as i32,
            ..Default::default()
        };
        assert_eq!(
            messages(candidate.get_subtree_roots(request).await).await?,
            messages(wallet.get_subtree_roots(request).await).await?,
            "{pool:?} subtree roots",
        );
    }
    Ok(())
}

async fn compare_balance(
    candidate: &mut Client,
    wallet: &mut Client,
    address: &str,
    node_stopped: bool,
) -> Result<()> {
    let addresses = rpc::AddressList {
        addresses: vec![address.to_owned()],
    };
    let expected_balance = wallet
        .get_taddress_balance(addresses.clone())
        .await?
        .into_inner();
    assert!(expected_balance.value_zat > 0);
    compare_node_read(
        "GetTaddressBalance",
        candidate
            .get_taddress_balance(addresses)
            .await
            .map(Response::into_inner),
        &expected_balance,
        node_stopped,
    )?;
    let address_stream = || {
        tokio_stream::iter([rpc::Address {
            address: address.to_owned(),
        }])
    };
    assert_eq!(
        wallet
            .get_taddress_balance_stream(address_stream())
            .await?
            .into_inner(),
        expected_balance
    );
    compare_node_read(
        "GetTaddressBalanceStream",
        candidate
            .get_taddress_balance_stream(address_stream())
            .await
            .map(Response::into_inner),
        &expected_balance,
        node_stopped,
    )
}

async fn compare_utxos(
    candidate: &mut Client,
    wallet: &mut Client,
    utxos: rpc::GetAddressUtxosArg,
    node_stopped: bool,
) -> Result<rpc::GetAddressUtxosReply> {
    let expected_utxos = wallet.get_address_utxos(utxos.clone()).await?.into_inner();
    assert_eq!(expected_utxos.address_utxos.len(), 2);
    assert_eq!(
        messages(wallet.get_address_utxos_stream(utxos.clone()).await).await?,
        expected_utxos.address_utxos
    );
    compare_node_read(
        "GetAddressUtxos",
        candidate
            .get_address_utxos(utxos.clone())
            .await
            .map(Response::into_inner),
        &expected_utxos,
        node_stopped,
    )?;
    compare_node_read(
        "GetAddressUtxosStream",
        messages(candidate.get_address_utxos_stream(utxos).await).await,
        &expected_utxos.address_utxos,
        node_stopped,
    )?;

    expected_utxos
        .address_utxos
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("missing mined fixture"))
}

async fn compare_history(
    candidate: &mut Client,
    wallet: &mut Client,
    history: rpc::TransparentAddressBlockFilter,
    node_stopped: bool,
) -> Result<()> {
    let expected_history =
        messages(wallet.get_taddress_transactions(history.clone()).await).await?;
    assert!(
        !expected_history.is_empty(),
        "the miner must have history in the requested range"
    );
    assert!(
        expected_history
            .iter()
            .all(|tx| tx.height > 0 && !tx.data.is_empty())
    );
    assert_eq!(
        messages(wallet.get_taddress_txids(history.clone()).await).await?,
        expected_history
    );
    compare_node_read(
        "GetTaddressTransactions",
        messages(candidate.get_taddress_transactions(history.clone()).await).await,
        &expected_history,
        node_stopped,
    )?;
    compare_node_read(
        "GetTaddressTxids",
        messages(candidate.get_taddress_txids(history).await).await,
        &expected_history,
        node_stopped,
    )
}

fn block_id(height: u64) -> BlockId {
    BlockId {
        height,
        hash: Vec::new(),
    }
}

fn block_range(start: u64, end: u64) -> BlockRange {
    BlockRange {
        start: Some(block_id(start)),
        end: Some(block_id(end)),
        pool_types: Vec::new(),
    }
}

async fn messages<T>(call: Result<Response<Streaming<T>>, Status>) -> Result<Vec<T>, Status> {
    let mut stream = call?.into_inner();
    let mut messages = Vec::new();
    while let Some(message) = stream.message().await? {
        messages.push(message);
    }
    Ok(messages)
}

fn compare_node_read<T: PartialEq + Debug>(
    method: &str,
    actual: Result<T, Status>,
    expected: &T,
    unavailable: bool,
) -> Result<()> {
    if unavailable {
        assert_node_unavailable(method, actual)
    } else {
        assert_eq!(
            &actual.wrap_err_with(|| format!("{method} failed"))?,
            expected,
            "{method}"
        );
        Ok(())
    }
}

fn assert_node_unavailable<T>(method: &str, reply: Result<T, Status>) -> Result<()> {
    let status = reply
        .err()
        .ok_or_else(|| eyre!("{method} answered with the node stopped"))?;
    assert_eq!(status.code(), Code::Unavailable, "{method}: {status}");
    assert_eq!(
        status
            .get_details_error_info()
            .as_ref()
            .map(|details| details.reason.as_str()),
        Some(zinder_proto::v1::ops::ErrorReason::NodeUnavailable.as_str_name()),
        "{method}: {status}"
    );
    Ok(())
}
