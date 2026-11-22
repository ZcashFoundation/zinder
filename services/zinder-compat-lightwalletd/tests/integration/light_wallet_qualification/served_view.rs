//! Forwarded answers stay within the served chain view when the node is ahead
//! of it or on another chain.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use eyre::{Result, eyre};
use serde_json::json;
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

use super::{ADDRESS, Fixture, address_output, balance_request, history_request, utxos_at};

/// Served by the reader at height 1.
const SERVED: TransactionId = TransactionId::from_bytes([0x55; 32]);
/// Mined only in the node's block 3, above the served tip.
const ABOVE: TransactionId = TransactionId::from_bytes([0x66; 32]);
const ABOVE_TIP: BlockId = BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([0x33; 32]));

/// The node's best chain and the address activity it indexes on it: block 1
/// pays [`ADDRESS`] in [`SERVED`], and block 3, held while `ahead` is set,
/// pays it in [`ABOVE`].
struct NodeChain {
    served: Vec<BlockId>,
    ahead: AtomicBool,
}

impl NodeChain {
    fn blocks(&self) -> Vec<BlockId> {
        let mut blocks = self.served.clone();
        if self.ahead.load(Ordering::Acquire) {
            blocks.push(ABOVE_TIP);
        }
        blocks
    }

    fn tip(&self) -> BlockId {
        self.blocks().last().copied().unwrap_or(ABOVE_TIP)
    }

    fn hash_at(&self, height: u64) -> Option<BlockHash> {
        self.blocks()
            .into_iter()
            .find(|block| u64::from(block.height.value()) == height)
            .map(|block| block.hash)
    }

    fn indexed(&self) -> Vec<(TransactionId, u32)> {
        [(SERVED, 1), (ABOVE, 3)]
            .into_iter()
            .filter(|(_, height)| self.hash_at(u64::from(*height)).is_some())
            .collect()
    }

    fn chain_stubs(self: &Arc<Self>) -> [JsonRpcStub; 3] {
        let (tips, hashes, transactions) = (Arc::clone(self), Arc::clone(self), Arc::clone(self));
        [
            method("getbestblockheightandhash").reply_with(move |_| {
                let tip = tips.tip();
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
                let mined = transactions
                    .indexed()
                    .into_iter()
                    .find(|(txid, _)| params[0] == encode_rpc_transaction_id_hex(*txid));
                let Some((_, height)) = mined else {
                    return RpcReply::error_with_code(
                        -5,
                        "No such mempool or main chain transaction",
                    );
                };
                if params[1] == 0 {
                    return RpcReply::result(json!("55"));
                }
                let hash = transactions
                    .hash_at(u64::from(height))
                    .unwrap_or(BlockHash::from_bytes([0; 32]));
                RpcReply::result(json!({
                    "hex": "55",
                    "txid": params[0],
                    "height": height,
                    "blockhash": encode_rpc_block_hash_hex(hash),
                }))
            }),
        ]
    }

    fn address_stubs(self: &Arc<Self>) -> Result<[JsonRpcStub; 2]> {
        let script = hex::encode(
            zinder_source::parse_transparent_address(ADDRESS)?
                .script()
                .as_raw_bytes(),
        );
        let (utxos, history) = (Arc::clone(self), Arc::clone(self));
        Ok([
            method("getaddressutxos").reply_with(move |_| {
                let rows: Vec<_> = utxos
                    .indexed()
                    .into_iter()
                    .map(|(txid, height)| {
                        address_output(&encode_rpc_transaction_id_hex(txid), height, &script)
                    })
                    .collect();
                utxos_at(utxos.tip(), &json!(rows))
            }),
            method("getaddresstxids").reply_with(move |params| {
                let start = params[0]["start"].as_u64().unwrap_or_default();
                let end = params[0]["end"].as_u64().unwrap_or_default();
                let ids: Vec<_> = history
                    .indexed()
                    .into_iter()
                    .filter(|(_, height)| (start..=end).contains(&u64::from(*height)))
                    .map(|(txid, _)| encode_rpc_transaction_id_hex(txid))
                    .collect();
                RpcReply::result(json!(ids))
            }),
        ])
    }
}

/// Serves a two-block chain whose block 1 mines [`SERVED`]. The node holds
/// the served blocks after `fork` rewrites them, plus [`ABOVE_TIP`] while
/// `ahead` is set. `overrides` replace the node's scripted replies.
fn served_view_fixture(
    fork: impl FnOnce(&mut Vec<BlockId>),
    ahead: bool,
    overrides: impl IntoIterator<Item = JsonRpcStub>,
) -> Result<(Fixture, Arc<NodeChain>)> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let first = chain
        .blocks()
        .first()
        .ok_or_else(|| eyre!("missing block"))?;
    let rows = FixtureTransactionRows::coinbase(SERVED, first.height, first.hash, vec![0x55]);
    let mut served = chain
        .blocks()
        .iter()
        .map(|block| BlockId::new(block.height, block.hash))
        .collect();
    fork(&mut served);
    let node = Arc::new(NodeChain {
        served,
        ahead: AtomicBool::new(ahead),
    });
    let replies = node
        .chain_stubs()
        .into_iter()
        .chain(node.address_stubs()?)
        .chain(overrides);
    let fixture = Fixture::from_chain(
        &chain.with_transaction_rows(rows),
        &super::one_upgrade_per_height()?,
        replies,
        std::time::Duration::from_secs(2),
    )?;
    Ok((fixture, node))
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

async fn address_state_unavailable(fixture: &Fixture) -> Result<()> {
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
    )
}

#[tokio::test]
async fn a_node_one_block_ahead_answers_only_for_the_served_tip() -> Result<()> {
    let (fixture, node) = served_view_fixture(|_| {}, true, [])?;

    // The full-wallet control serves the view at height 2: SERVED is mined at
    // height 1, ABOVE is not mined, and ADDRESS holds one output at height 1.
    let mined = fixture
        .adapter
        .get_transaction(transaction(SERVED))
        .await?
        .into_inner();
    assert_eq!((mined.height, mined.data), (1, vec![0x55]));
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

    // The node answers address state at its own tip, which the view lacks.
    address_state_unavailable(&fixture).await?;

    // Once the node's tip is the served tip, its answers are the view's.
    node.ahead.store(false, Ordering::Release);
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
async fn address_state_from_a_tip_the_node_left_and_regained_is_unavailable() -> Result<()> {
    // Tip reads keep reporting the served tip, but the node answers address
    // lookups at block 3, as it would when it moved there and back.
    let served_tip = super::served_tip(2)?;
    let (fixture, _) = served_view_fixture(
        |_| {},
        true,
        [method("getbestblockheightandhash").reply_with(move |_| {
            RpcReply::result(
                json!({"height": served_tip.height.value(), "hash": served_tip.hash.as_bytes()}),
            )
        })],
    )?;
    address_state_unavailable(&fixture).await?;
    assert!(fixture.node.requests_for("getaddressbalance")?.is_empty());
    Ok(())
}

#[tokio::test]
async fn a_node_on_another_chain_at_the_served_height_is_unavailable() -> Result<()> {
    let (fixture, _) = served_view_fixture(
        |served| {
            if let Some(tip) = served.last_mut() {
                tip.hash = BlockHash::from_bytes([0xdd; 32]);
            }
        },
        false,
        [],
    )?;
    let missing = TransactionId::from_bytes([3; 32]);
    for id in [SERVED, ABOVE, missing] {
        node_unavailable(fixture.adapter.get_transaction(transaction(id)).await)?;
    }
    address_state_unavailable(&fixture).await?;
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

/// A node that answers history from a fork, then returns to the served chain.
///
/// The served tip is the reader's A2 at height 2. The node moves to the
/// higher-work B3, whose block at height 2 is B2, for its first two tip reads,
/// then to A4, which extends A2, before any later check.
fn leave_and_regain_stubs(served_tip: BlockId) -> [JsonRpcStub; 3] {
    let on_fork = Arc::new(AtomicBool::new(false));
    let tip_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (tips, hashes, history) = (Arc::clone(&on_fork), Arc::clone(&on_fork), on_fork);
    [
        method("getbestblockheightandhash").reply_with(move |_| {
            let read = tip_reads.fetch_add(1, Ordering::AcqRel);
            tips.store(read == 0, Ordering::Release);
            let (height, hash) = if read < 2 {
                (3, [0xb3; 32])
            } else {
                (4, [0xa4; 32])
            };
            RpcReply::result(json!({"height": height, "hash": hash}))
        }),
        method("getblockhash").reply_with(move |params| {
            let on_fork = hashes.load(Ordering::Acquire);
            let hash = match params[0].as_u64() {
                Some(2) if on_fork => BlockHash::from_bytes([0xb2; 32]),
                Some(2) => served_tip.hash,
                Some(3) if on_fork => BlockHash::from_bytes([0xb3; 32]),
                _ => BlockHash::from_bytes([0xa4; 32]),
            };
            RpcReply::result(json!(encode_rpc_block_hash_hex(hash)))
        }),
        method("getaddresstxids").reply_with(move |_| {
            // The fork does not hold the served payment.
            let ids: Vec<String> = if history.load(Ordering::Acquire) {
                Vec::new()
            } else {
                vec![encode_rpc_transaction_id_hex(SERVED)]
            };
            RpcReply::result(json!(ids))
        }),
    ]
}

#[tokio::test]
async fn history_answered_from_a_fork_the_node_leaves_again_is_unavailable() -> Result<()> {
    let served_tip = super::served_tip(2)?;
    for txids in [true, false] {
        let (fixture, _) = served_view_fixture(|_| {}, false, leave_and_regain_stubs(served_tip))?;
        let request = history_request(1, 2);
        let answer = if txids {
            fixture.adapter.get_taddress_txids(request).await
        } else {
            fixture.adapter.get_taddress_transactions(request).await
        };
        let status = answer
            .err()
            .ok_or_else(|| eyre!("served history the node read from a fork"))?;
        assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    }
    Ok(())
}

#[tokio::test]
async fn history_rows_need_the_served_block_and_committed_bytes() -> Result<()> {
    let served_block_one = ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(2)
        .blocks()
        .first()
        .map(|block| block.hash)
        .ok_or_else(|| eyre!("missing block"))?;
    // A sibling of served block 1, then the served block with other bytes.
    for (reported, body, expected) in [
        (BlockHash::from_bytes([0x99; 32]), "55", Code::Unavailable),
        (served_block_one, "56", Code::DataLoss),
    ] {
        let (fixture, _) = served_view_fixture(
            |_| {},
            false,
            [method("getrawtransaction").reply_with(move |params| {
                RpcReply::result(json!({
                    "hex": body,
                    "txid": params[0],
                    "height": 1,
                    "blockhash": encode_rpc_block_hash_hex(reported),
                }))
            })],
        )?;
        let mut history = fixture
            .adapter
            .get_taddress_transactions(history_request(1, 2))
            .await?
            .into_inner();
        let status = history
            .next()
            .await
            .and_then(Result::err)
            .ok_or_else(|| eyre!("served a row that is not the served transaction"))?;
        assert_eq!(status.code(), expected, "{status:?}");
    }
    Ok(())
}

#[tokio::test]
async fn history_after_the_node_rolls_back_below_the_range_is_unavailable() -> Result<()> {
    // Admission sees the served tip at height 2; the history lookup for 2..=2
    // then meets a node rolled back to height 1.
    let served_tip = super::served_tip(2)?;
    for txids in [true, false] {
        let (fixture, _) = served_view_fixture(
            |_| {},
            false,
            [
                method("getbestblockheightandhash").reply_with(|_| {
                    RpcReply::result(json!({"height": 1, "hash": vec![0xa1_u8; 32]}))
                }),
                method("getblockhash").reply(RpcReply::result(json!(encode_rpc_block_hash_hex(
                    served_tip.hash
                )))),
                method("getblockhash").reply_with(|_| {
                    RpcReply::error_with_code(-8, "block height not in best chain")
                }),
            ],
        )?;
        let request = history_request(2, 2);
        let answer = if txids {
            fixture.adapter.get_taddress_txids(request).await
        } else {
            fixture.adapter.get_taddress_transactions(request).await
        };
        let status = answer
            .err()
            .ok_or_else(|| eyre!("served empty history from a rolled-back node"))?;
        assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    }
    Ok(())
}
