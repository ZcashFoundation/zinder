//! Forwarded answers stay within the served chain view when the node is ahead
//! of it or on another chain.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use eyre::{Result, eyre};
use serde_json::{Value, json};
use tokio_stream::StreamExt as _;
use tonic::{Code, Request, Status};
use tonic_types::StatusExt as _;
use zinder_core::{
    BlockHash, BlockHeight, BlockId, Network, TransactionId,
    wire::{
        encode_internal_transaction_id, encode_rpc_block_hash_hex, encode_rpc_transaction_id_hex,
    },
};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_server::CompactTxStreamer as _,
};
use zinder_testkit::{ChainFixture, FixtureTransactionRows, JsonRpcStub, RpcReply, method};

use super::{ADDRESS, Fixture, balance_request, history_request};

/// Served by the reader at height 1.
const SERVED: TransactionId = TransactionId::from_bytes([0x55; 32]);
/// Mined only in the node's block above the served tip.
const ABOVE: TransactionId = TransactionId::from_bytes([0x66; 32]);

/// The node's best chain and the address activity it indexes on it: block 1
/// pays [`ADDRESS`] in [`SERVED`] and, while `touched` is set, block 3 pays
/// it in [`ABOVE`].
struct NodeChain {
    blocks: Vec<BlockId>,
    touched: Arc<AtomicBool>,
}

impl NodeChain {
    fn hash_at(&self, height: u64) -> Option<BlockHash> {
        self.blocks
            .iter()
            .find(|block| u64::from(block.height.value()) == height)
            .map(|block| block.hash)
    }

    fn mined_height(&self, id: &Value) -> Option<u32> {
        [(SERVED, 1), (ABOVE, 3)]
            .into_iter()
            .find(|(txid, _)| *id == encode_rpc_transaction_id_hex(*txid))
            .map(|(_, height)| height)
            .filter(|height| self.hash_at(u64::from(*height)).is_some())
    }

    fn indexed(&self) -> Vec<(TransactionId, u64)> {
        let mut rows = vec![(SERVED, 1)];
        if self.touched.load(Ordering::Acquire) && self.hash_at(3).is_some() {
            rows.push((ABOVE, 3));
        }
        rows
    }

    fn chain_stubs(self: &Arc<Self>) -> Result<[JsonRpcStub; 3]> {
        let tip = self
            .blocks
            .last()
            .copied()
            .ok_or_else(|| eyre!("empty node chain"))?;
        let (hashes, transactions) = (Arc::clone(self), Arc::clone(self));
        Ok([
            method("getbestblockheightandhash").reply_with(move |_| {
                RpcReply::result(json!({"height": tip.height.value(), "hash": tip.hash.as_bytes()}))
            }),
            method("getblockhash").reply_with(move |params| {
                params[0]
                    .as_u64()
                    .and_then(|height| hashes.hash_at(height))
                    .map_or_else(
                        || RpcReply::error_with_code(-8, "block height not in best chain"),
                        |hash| RpcReply::result(json!(encode_rpc_block_hash_hex(hash))),
                    )
            }),
            method("getrawtransaction").reply_with(move |params| {
                let Some(height) = transactions.mined_height(&params[0]) else {
                    return RpcReply::error_with_code(
                        -5,
                        "No such mempool or main chain transaction",
                    );
                };
                if params[1] == 0 {
                    return RpcReply::result(json!("1221"));
                }
                let hash = transactions
                    .hash_at(u64::from(height))
                    .unwrap_or(BlockHash::from_bytes([0; 32]));
                RpcReply::result(json!({
                    "hex": "1221",
                    "txid": params[0],
                    "height": height,
                    "blockhash": encode_rpc_block_hash_hex(hash),
                }))
            }),
        ])
    }

    fn address_stubs(self: &Arc<Self>) -> Result<[JsonRpcStub; 3]> {
        let script = hex::encode(
            zinder_source::parse_transparent_address(ADDRESS)?
                .script()
                .as_raw_bytes(),
        );
        let (balance, utxos, history) = (Arc::clone(self), Arc::clone(self), Arc::clone(self));
        Ok([
            method("getaddressbalance").reply_with(move |_| {
                let zatoshis = balance.indexed().len();
                RpcReply::result(json!({"balance": zatoshis, "received": zatoshis}))
            }),
            method("getaddressutxos").reply_with(move |_| {
                let rows: Vec<_> = utxos
                    .indexed()
                    .into_iter()
                    .map(|(txid, height)| {
                        json!({"address": ADDRESS, "txid": encode_rpc_transaction_id_hex(txid),
                        "outputIndex": 0, "script": script, "satoshis": 1, "height": height})
                    })
                    .collect();
                RpcReply::result(json!(rows))
            }),
            method("getaddresstxids").reply_with(move |params| {
                let start = params[0]["start"].as_u64().unwrap_or_default();
                let end = params[0]["end"].as_u64().unwrap_or_default();
                let ids: Vec<_> = history
                    .indexed()
                    .into_iter()
                    .filter(|(_, height)| (start..=end).contains(height))
                    .map(|(txid, _)| encode_rpc_transaction_id_hex(txid))
                    .collect();
                RpcReply::result(json!(ids))
            }),
        ])
    }
}

/// Serves a two-block chain whose block 1 mines [`SERVED`], with a node whose
/// best chain `node_chain` derives from the served blocks.
fn served_view_fixture(
    node_chain: impl FnOnce(Vec<BlockId>) -> Vec<BlockId>,
    touched: Arc<AtomicBool>,
) -> Result<Fixture> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let first = chain
        .blocks()
        .first()
        .ok_or_else(|| eyre!("missing block"))?;
    let rows = FixtureTransactionRows::coinbase(SERVED, first.height, first.hash, vec![0x55]);
    let served = chain
        .blocks()
        .iter()
        .map(|block| BlockId::new(block.height, block.hash))
        .collect();
    let node = Arc::new(NodeChain {
        blocks: node_chain(served),
        touched,
    });
    let replies = node.chain_stubs()?.into_iter().chain(node.address_stubs()?);
    Fixture::from_chain(
        &chain.with_transaction_rows(rows),
        &super::one_upgrade_per_height()?,
        replies,
        std::time::Duration::from_secs(2),
    )
}

fn transaction(id: TransactionId) -> Request<lightwalletd::TxFilter> {
    Request::new(lightwalletd::TxFilter {
        hash: encode_internal_transaction_id(id).to_vec(),
        ..Default::default()
    })
}

fn utxo_request() -> Request<lightwalletd::GetAddressUtxosArg> {
    Request::new(lightwalletd::GetAddressUtxosArg {
        addresses: vec![ADDRESS.to_owned()],
        start_height: 1,
        max_entries: 0,
    })
}

fn node_unavailable(answer: Result<impl Sized, Status>) -> Result<()> {
    let status = answer
        .err()
        .ok_or_else(|| eyre!("expected UNAVAILABLE, got an answer"))?;
    assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    assert_eq!(
        status
            .get_error_details()
            .error_info()
            .map(|detail| detail.reason.as_str()),
        Some("NODE_UNAVAILABLE")
    );
    Ok(())
}

async fn history_heights(fixture: &Fixture, txids: bool) -> Result<Vec<u64>> {
    let request = history_request(1, 2);
    let mut history = if txids {
        fixture.adapter.get_taddress_txids(request).await?
    } else {
        fixture.adapter.get_taddress_transactions(request).await?
    }
    .into_inner();
    let mut heights = Vec::new();
    while let Some(row) = history.next().await.transpose()? {
        heights.push(row.height);
    }
    Ok(heights)
}

async fn utxo_heights(fixture: &Fixture) -> Result<(Vec<u64>, Vec<u64>)> {
    let unary = fixture
        .adapter
        .get_address_utxos(utxo_request())
        .await?
        .into_inner()
        .address_utxos
        .into_iter()
        .map(|row| row.height)
        .collect();
    let mut stream = fixture
        .adapter
        .get_address_utxos_stream(utxo_request())
        .await?
        .into_inner();
    let mut streamed = Vec::new();
    while let Some(row) = stream.next().await.transpose()? {
        streamed.push(row.height);
    }
    Ok((unary, streamed))
}

#[tokio::test]
async fn a_node_one_block_ahead_answers_only_for_the_served_tip() -> Result<()> {
    let touched = Arc::new(AtomicBool::new(true));
    let fixture = served_view_fixture(
        |mut served| {
            served.push(BlockId::new(
                BlockHeight::new(3),
                BlockHash::from_bytes([0x33; 32]),
            ));
            served
        },
        Arc::clone(&touched),
    )?;

    // The full-wallet control serves the view at height 2: SERVED is mined at
    // height 1, ABOVE is not mined, and ADDRESS holds one output at height 1.
    let mined = fixture
        .adapter
        .get_transaction(transaction(SERVED))
        .await?
        .into_inner();
    assert_eq!((mined.height, mined.data), (1, vec![0x12, 0x21]));
    let above = fixture
        .adapter
        .get_transaction(transaction(ABOVE))
        .await
        .err()
        .ok_or_else(|| eyre!("served a transaction mined above the served tip"))?;
    assert_eq!(above.code(), Code::NotFound);
    for txids in [true, false] {
        assert_eq!(history_heights(&fixture, txids).await?, [1]);
    }

    // Block 3 pays ADDRESS, so the node's balance and outputs describe a later
    // chain state than the view.
    node_unavailable(
        fixture
            .adapter
            .get_taddress_balance(balance_request())
            .await,
    )?;
    node_unavailable(fixture.adapter.get_address_utxos(utxo_request()).await)?;
    node_unavailable(
        fixture
            .adapter
            .get_address_utxos_stream(utxo_request())
            .await,
    )?;

    // Once no block above the view touches ADDRESS, the node's answer is the
    // view's answer.
    touched.store(false, Ordering::Release);
    assert_eq!(
        fixture
            .adapter
            .get_taddress_balance(balance_request())
            .await?
            .into_inner()
            .value_zat,
        1
    );
    assert_eq!(utxo_heights(&fixture).await?, (vec![1], vec![1]));
    let raw_reads = fixture.node.requests_for("getrawtransaction")?;
    assert!(
        raw_reads
            .iter()
            .all(|read| read.params[0] == encode_rpc_transaction_id_hex(SERVED)),
        "asked the node for a transaction the view does not mine"
    );
    Ok(())
}

#[tokio::test]
async fn a_node_on_another_chain_at_the_served_height_is_unavailable() -> Result<()> {
    let fixture = served_view_fixture(
        |mut served| {
            if let Some(tip) = served.last_mut() {
                tip.hash = BlockHash::from_bytes([0xdd; 32]);
            }
            served
        },
        Arc::new(AtomicBool::new(false)),
    )?;
    let missing = TransactionId::from_bytes([3; 32]);
    for id in [SERVED, ABOVE, missing] {
        node_unavailable(fixture.adapter.get_transaction(transaction(id)).await)?;
    }
    node_unavailable(
        fixture
            .adapter
            .get_taddress_balance(balance_request())
            .await,
    )?;
    node_unavailable(fixture.adapter.get_address_utxos(utxo_request()).await)?;
    node_unavailable(
        fixture
            .adapter
            .get_address_utxos_stream(utxo_request())
            .await,
    )?;
    node_unavailable(
        fixture
            .adapter
            .get_taddress_txids(history_request(1, 2))
            .await,
    )?;
    node_unavailable(
        fixture
            .adapter
            .get_taddress_transactions(history_request(1, 2))
            .await,
    )?;
    for forwarded in [
        "getrawtransaction",
        "getaddressbalance",
        "getaddressutxos",
        "getaddresstxids",
    ] {
        assert!(
            fixture.node.requests_for(forwarded)?.is_empty(),
            "{forwarded} reached a node on another chain"
        );
    }
    let latest = fixture
        .adapter
        .get_latest_block(Request::new(lightwalletd::ChainSpec {}))
        .await?
        .into_inner();
    assert_eq!(latest.height, 2);
    Ok(())
}
