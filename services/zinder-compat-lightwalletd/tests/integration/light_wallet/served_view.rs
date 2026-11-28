//! Forwarded answers stay within the served chain view when the node is ahead
//! of it or on another chain.

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use eyre::{Result, eyre};
use serde_json::json;
use tokio::{net::TcpListener, sync::Notify};
use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
use tonic::{Code, Request, Status, transport::Channel};
use tonic_types::StatusExt as _;
use zinder_compat_lightwalletd::{
    CanonicalReaderSlot, DEFAULT_TIP_WAIT_MAX_BLOCKS, DEFAULT_TIP_WAIT_MAX_WAITERS,
    LightwalletdGrpcAdapter, NodeBackedLightWallet, TipWait,
};
use zinder_core::{
    BlockHash, BlockHeight, BlockId, Network, TransactionId,
    wire::{
        encode_internal_transaction_id, encode_rpc_block_hash_hex, encode_rpc_transaction_id_hex,
    },
};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_client::CompactTxStreamerClient,
    compact_tx_streamer_server::CompactTxStreamer as _,
};
use zinder_query::{
    DEFAULT_TREE_STATE_REPLAY_WAIT, DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT,
    ForwardedReadConfig, ForwardedReadLimit, ForwardedResponseKind, RocksDbCanonicalReader,
};
use zinder_source::{NodeAuth, ZebraJsonRpcSource};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureTransactionRows, JsonRpcStub,
    JsonRpcTestServer, RpcReply, WalletServingStoreFixture, method,
};

use super::{ADDRESS, Fixture, address_output, balance_request, history_request, utxos_at};
use crate::common::verified_node::{one_upgrade_per_height, verifiable_node};

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

pub(super) fn served_tip_mismatch(answer: Result<impl Sized, Status>) -> Result<()> {
    let status = answer
        .err()
        .ok_or_else(|| eyre!("expected UNAVAILABLE, got an answer"))?;
    assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    assert_eq!(
        status
            .get_error_details()
            .error_info()
            .map(|detail| (detail.domain.as_str(), detail.reason.as_str())),
        Some(("zinder.dev", "SERVED_TIP_MISMATCH"))
    );
    assert!(
        !status.message().contains("upstream node is unavailable"),
        "{status:?}"
    );
    assert_eq!(
        status
            .metadata()
            .get("grpc-retry-pushback-ms")
            .and_then(|pushback| pushback.to_str().ok()),
        Some("1000")
    );
    Ok(())
}

fn balance_stream_request() -> impl tokio_stream::Stream<Item = lightwalletd::Address> {
    tokio_stream::iter([lightwalletd::Address {
        address: ADDRESS.to_owned(),
    }])
}

async fn with_served_view_client<T>(
    adapter: LightwalletdGrpcAdapter<NodeBackedLightWallet>,
    run: impl AsyncFnOnce(CompactTxStreamerClient<Channel>) -> Result<T>,
) -> Result<T> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _stopped = stopped.await;
            })
            .await
    });
    let client = CompactTxStreamerClient::connect(endpoint).await?;
    let outcome = tokio::time::timeout(Duration::from_secs(10), run(client)).await?;
    let _stopped = stop.send(());
    server.await??;
    outcome
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
    served_tip_mismatch(
        fixture
            .adapter
            .get_taddress_balance(balance_request())
            .await,
    )?;
    served_tip_mismatch(fixture.adapter.get_address_utxos(utxo_request()).await)?;
    served_tip_mismatch(
        fixture
            .adapter
            .get_address_utxos_stream(utxo_request())
            .await,
    )?;
    with_served_view_client(fixture.adapter.clone(), async |mut client| {
        served_tip_mismatch(
            client
                .get_taddress_balance_stream(balance_stream_request())
                .await,
        )
    })
    .await
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
    with_served_view_client(fixture.adapter.clone(), async |mut client| {
        assert_eq!(
            client
                .get_taddress_balance_stream(balance_stream_request())
                .await?
                .into_inner()
                .value_zat,
            1
        );
        Ok(())
    })
    .await?;
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
async fn a_node_on_another_chain_at_the_served_height_answers_only_self_attested_reads()
-> Result<()> {
    let (fixture, _) = served_view_fixture(
        |served| {
            if let Some(tip) = served.last_mut() {
                tip.hash = BlockHash::from_bytes([0xdd; 32]);
            }
        },
        false,
        [],
    )?;
    // SERVED is mined at height 1, below the fork: the node reports it in the
    // served block with the recorded bytes, which binds the answer to the
    // served view without a tip check.
    let mined = fixture
        .adapter
        .get_transaction(transaction(SERVED))
        .await?
        .into_inner();
    assert_eq!((mined.height, mined.data), (1, vec![0x55]));
    let missing = TransactionId::from_bytes([3; 32]);
    for id in [ABOVE, missing] {
        node_unavailable(fixture.adapter.get_transaction(transaction(id)).await)?;
    }
    // The node attests its own tip with every unspent-output page, and the
    // history page request checks the served tip before it asks.
    address_state_unavailable(&fixture).await?;
    for history in [
        fixture
            .adapter
            .get_taddress_txids(history_request(1, 2))
            .await
            .map(drop),
        fixture
            .adapter
            .get_taddress_transactions(history_request(1, 2))
            .await
            .map(drop),
    ] {
        let status = history
            .err()
            .ok_or_else(|| eyre!("history answered from another chain"))?;
        assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    }
    let fetched: Vec<_> = fixture
        .node
        .requests_for("getrawtransaction")?
        .into_iter()
        .map(|request| request.params[0].clone())
        .collect();
    assert_eq!(fetched, [json!(encode_rpc_transaction_id_hex(SERVED))]);
    for forwarded in ["getaddressbalance", "getaddresstxids"] {
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

#[tokio::test]
async fn history_after_a_rollback_the_node_recovers_from_is_unavailable() -> Result<()> {
    // The tip read for the lookup of 2..=2 catches a rollback to height 1,
    // and every hash read still finds the served tip.
    for txids in [true, false] {
        let (fixture, _) = served_view_fixture(
            |_| {},
            false,
            [method("getbestblockheightandhash")
                .reply_with(|_| RpcReply::result(json!({"height": 1, "hash": vec![0xa1_u8; 32]})))],
        )?;
        let request = history_request(2, 2);
        let answer = if txids {
            fixture.adapter.get_taddress_txids(request).await
        } else {
            fixture.adapter.get_taddress_transactions(request).await
        };
        let status = answer
            .err()
            .ok_or_else(|| eyre!("served empty history from a stale tip read"))?;
        assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    }
    Ok(())
}

fn catch_up_node_stubs(
    node_tip: &Arc<parking_lot::Mutex<BlockId>>,
    utxo_replies: &Arc<AtomicUsize>,
    utxo_reply_notify: &Arc<Notify>,
) -> Result<[JsonRpcStub; 2]> {
    let script = hex::encode(
        zinder_source::parse_transparent_address(ADDRESS)?
            .script()
            .as_raw_bytes(),
    );
    let tips_for_best = Arc::clone(node_tip);
    let tips_for_utxos = Arc::clone(node_tip);
    let replies = Arc::clone(utxo_replies);
    let notify = Arc::clone(utxo_reply_notify);
    Ok([
        method("getbestblockheightandhash").reply_with(move |_| {
            let tip = *tips_for_best.lock();
            RpcReply::result(json!({"height": tip.height.value(), "hash": tip.hash.as_bytes()}))
        }),
        method("getaddressutxos").reply_with(move |_| {
            let tip = *tips_for_utxos.lock();
            let mut rows = vec![address_output(
                &encode_rpc_transaction_id_hex(SERVED),
                1_u32,
                &script,
            )];
            if tip.height.value() >= 3 {
                rows.push(address_output(
                    &encode_rpc_transaction_id_hex(ABOVE),
                    3_u32,
                    &script,
                ));
            }
            let reply = utxos_at(tip, &json!(rows));
            replies.fetch_add(1, Ordering::Release);
            notify.notify_waiters();
            reply
        }),
    ])
}

struct CatchUpFixture {
    adapter: LightwalletdGrpcAdapter<NodeBackedLightWallet>,
    node: JsonRpcTestServer,
    slot: CanonicalReaderSlot,
    initial_reader: Arc<RocksDbCanonicalReader>,
    next_reader: Arc<RocksDbCanonicalReader>,
    node_tip: Arc<parking_lot::Mutex<BlockId>>,
    utxo_replies: Arc<AtomicUsize>,
    utxo_reply_notify: Arc<Notify>,
    block2: BlockId,
    block3: BlockId,
    block4: BlockId,
    _initial_store: WalletServingStoreFixture,
    _next_store: WalletServingStoreFixture,
}

impl CatchUpFixture {
    fn new(tip_wait: TipWait, reads: ForwardedReadLimit) -> Result<Self> {
        let schedule = one_upgrade_per_height()?;
        let base_chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        let first = base_chain
            .blocks()
            .first()
            .map(|block| (block.height, block.hash))
            .ok_or_else(|| eyre!("missing block 1"))?;
        let block2 = base_chain
            .blocks()
            .get(1)
            .map(|block| BlockId::new(block.height, block.hash))
            .ok_or_else(|| eyre!("missing block 2"))?;
        let block3 = base_chain
            .blocks()
            .get(2)
            .map(|block| BlockId::new(block.height, block.hash))
            .ok_or_else(|| eyre!("missing block 3"))?;
        let chain = base_chain.with_transaction_rows(FixtureTransactionRows::coinbase(
            SERVED,
            first.0,
            first.1,
            vec![0x55],
        ));
        let block4 = BlockId::new(BlockHeight::new(4), BlockHash::from_bytes([0x44; 32]));
        let mut writer = CanonicalWriterFixture::publish(
            &CanonicalBaseline::new(&chain).through(BlockHeight::new(2)),
            &schedule,
        )?;
        let mut initial_store = writer.serving_store_fixture()?;
        let (mut initial_canonical, _) = initial_store.take_readers()?;
        initial_canonical.try_catch_up()?;
        let initial_reader = Arc::new(RocksDbCanonicalReader::new(initial_canonical)?);

        writer.append_live_block(&chain)?;
        let mut next_store = writer.serving_store_fixture()?;
        let (mut next_canonical, _) = next_store.take_readers()?;
        next_canonical.try_catch_up()?;
        let next_reader = Arc::new(RocksDbCanonicalReader::new(next_canonical)?);

        let node_tip = Arc::new(parking_lot::Mutex::new(block3));
        let utxo_replies = Arc::new(AtomicUsize::new(0));
        let utxo_reply_notify = Arc::new(Notify::new());
        let node = verifiable_node(
            &schedule,
            block2.hash,
            catch_up_node_stubs(&node_tip, &utxo_replies, &utxo_reply_notify)?,
        )?;
        let source = ZebraJsonRpcSource::new(
            Network::ZcashRegtest,
            node.url(),
            NodeAuth::None,
            Duration::from_secs(2),
        )?;
        let slot = CanonicalReaderSlot::new(Arc::clone(&initial_reader));
        let adapter = LightwalletdGrpcAdapter::for_light_wallet(
            slot.clone(),
            source,
            DEFAULT_TREE_STATE_REPLAY_WAIT,
            DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT,
            reads,
        )
        .with_tip_wait(tip_wait);
        Ok(Self {
            adapter,
            node,
            slot,
            initial_reader,
            next_reader,
            node_tip,
            utxo_replies,
            utxo_reply_notify,
            block2,
            block3,
            block4,
            _initial_store: initial_store,
            _next_store: next_store,
        })
    }

    fn reset_to_block2(&self) {
        drop(self.slot.publish(Arc::clone(&self.initial_reader)));
        *self.node_tip.lock() = self.block3;
    }

    fn publish_block3(&self) {
        drop(self.slot.publish(Arc::clone(&self.next_reader)));
    }

    fn utxo_calls(&self) -> Result<usize> {
        Ok(self.node.requests_for("getaddressutxos")?.len())
    }

    fn utxo_reply_count(&self) -> usize {
        self.utxo_replies.load(Ordering::Acquire)
    }

    async fn await_utxo_replies(&self, target_replies: usize) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let notified = self.utxo_reply_notify.notified();
                if self.utxo_replies.load(Ordering::Acquire) >= target_replies {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| eyre!("timed out waiting for {target_replies} getaddressutxos replies"))?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum ForwardedMethod {
    AddressUtxos,
    AddressUtxosStream,
    TaddressBalance,
    TaddressBalanceStream,
}

const ALL_FORWARDED_METHODS: [ForwardedMethod; 4] = [
    ForwardedMethod::AddressUtxos,
    ForwardedMethod::AddressUtxosStream,
    ForwardedMethod::TaddressBalance,
    ForwardedMethod::TaddressBalanceStream,
];

#[derive(Debug, Eq, PartialEq)]
enum ForwardedAnswer {
    Balance(i64),
    UtxoHeights(Vec<u64>),
}

async fn call_forwarded_method(
    client: &mut CompactTxStreamerClient<Channel>,
    forwarded_method: ForwardedMethod,
) -> Result<ForwardedAnswer, Status> {
    match forwarded_method {
        ForwardedMethod::AddressUtxos => {
            let reply = client.get_address_utxos(utxo_request()).await?;
            Ok(ForwardedAnswer::UtxoHeights(
                reply
                    .into_inner()
                    .address_utxos
                    .into_iter()
                    .map(|row| row.height)
                    .collect(),
            ))
        }
        ForwardedMethod::AddressUtxosStream => {
            let mut stream = client
                .get_address_utxos_stream(utxo_request())
                .await?
                .into_inner();
            let mut heights = Vec::new();
            while let Some(row) = stream.next().await.transpose()? {
                heights.push(row.height);
            }
            Ok(ForwardedAnswer::UtxoHeights(heights))
        }
        ForwardedMethod::TaddressBalance => {
            let reply = client.get_taddress_balance(balance_request()).await?;
            Ok(ForwardedAnswer::Balance(reply.into_inner().value_zat))
        }
        ForwardedMethod::TaddressBalanceStream => {
            let reply = client
                .get_taddress_balance_stream(balance_stream_request())
                .await?;
            Ok(ForwardedAnswer::Balance(reply.into_inner().value_zat))
        }
    }
}

fn default_tip_wait(max_wait: Duration) -> TipWait {
    TipWait::new(
        max_wait,
        DEFAULT_TIP_WAIT_MAX_BLOCKS,
        DEFAULT_TIP_WAIT_MAX_WAITERS,
    )
}

#[tokio::test]
async fn utxo_and_balance_refuse_served_tip_mismatch_immediately_when_tip_wait_disabled()
-> Result<()> {
    let fixture = CatchUpFixture::new(TipWait::disabled(), ForwardedReadLimit::default())?;
    with_served_view_client(fixture.adapter.clone(), async |mut client| {
        for forwarded_method in ALL_FORWARDED_METHODS {
            let started = Instant::now();
            served_tip_mismatch(call_forwarded_method(&mut client, forwarded_method).await)?;
            assert!(started.elapsed() < Duration::from_secs(1));
        }
        Ok(())
    })
    .await?;
    assert_eq!(fixture.utxo_calls()?, ALL_FORWARDED_METHODS.len());
    Ok(())
}

#[tokio::test]
async fn utxo_and_balance_refuse_immediately_when_forwarded_read_wait_is_zero() -> Result<()> {
    let zero_wait =
        ForwardedReadLimit::new(ForwardedReadConfig::resolve(None, Some(0), None, None)?);
    let fixture = CatchUpFixture::new(default_tip_wait(Duration::from_secs(5)), zero_wait)?;
    with_served_view_client(fixture.adapter.clone(), async |mut client| {
        for forwarded_method in ALL_FORWARDED_METHODS {
            let started = Instant::now();
            served_tip_mismatch(call_forwarded_method(&mut client, forwarded_method).await)?;
            assert!(started.elapsed() < Duration::from_secs(1));
        }
        Ok(())
    })
    .await?;
    assert_eq!(fixture.utxo_calls()?, ALL_FORWARDED_METHODS.len());
    Ok(())
}

#[tokio::test]
async fn utxo_and_balance_catch_up_within_tip_wait_and_retry_once() -> Result<()> {
    let fixture = CatchUpFixture::new(
        default_tip_wait(Duration::from_secs(3)),
        ForwardedReadLimit::default(),
    )?;
    for forwarded_method in ALL_FORWARDED_METHODS {
        fixture.reset_to_block2();
        let before_calls = fixture.utxo_calls()?;
        let before_replies = fixture.utxo_reply_count();
        let adapter = fixture.adapter.clone();
        let in_flight = tokio::spawn(async move {
            with_served_view_client(adapter, async |mut client| {
                call_forwarded_method(&mut client, forwarded_method)
                    .await
                    .map_err(|status| {
                        eyre!("{forwarded_method:?} failed after catch-up: {status:?}")
                    })
            })
            .await
        });
        fixture.await_utxo_replies(before_replies + 1).await?;
        fixture.publish_block3();
        let answer = in_flight.await??;
        match forwarded_method {
            ForwardedMethod::AddressUtxos | ForwardedMethod::AddressUtxosStream => {
                assert_eq!(answer, ForwardedAnswer::UtxoHeights(vec![1, 3]));
            }
            ForwardedMethod::TaddressBalance | ForwardedMethod::TaddressBalanceStream => {
                assert_eq!(answer, ForwardedAnswer::Balance(2));
            }
        }
        assert_eq!(fixture.utxo_calls()? - before_calls, 2);
    }
    Ok(())
}

#[tokio::test]
async fn utxo_and_balance_expire_tip_wait_and_refuse_without_extra_retry() -> Result<()> {
    let wait_duration = Duration::from_millis(120);
    let fixture = CatchUpFixture::new(
        default_tip_wait(wait_duration),
        ForwardedReadLimit::default(),
    )?;
    with_served_view_client(fixture.adapter.clone(), async |mut client| {
        for forwarded_method in ALL_FORWARDED_METHODS {
            let started = Instant::now();
            served_tip_mismatch(call_forwarded_method(&mut client, forwarded_method).await)?;
            assert!(
                started.elapsed() >= wait_duration,
                "{forwarded_method:?} returned before tip wait expired"
            );
        }
        Ok(())
    })
    .await?;
    assert_eq!(fixture.utxo_calls()?, ALL_FORWARDED_METHODS.len());
    Ok(())
}

#[tokio::test]
async fn utxo_tip_wait_releases_forwarded_response_permit_before_waiting() -> Result<()> {
    let one = NonZeroU32::MIN;
    let reads = ForwardedReadLimit::new(ForwardedReadConfig::default().with_response_limit(
        ForwardedResponseKind::UtxoResponse,
        one,
        0,
    ));
    let fixture = CatchUpFixture::new(default_tip_wait(Duration::from_secs(5)), reads.clone())?;
    for forwarded_method in [
        ForwardedMethod::AddressUtxos,
        ForwardedMethod::AddressUtxosStream,
    ] {
        fixture.reset_to_block2();
        let before_calls = fixture.utxo_calls()?;
        let before_replies = fixture.utxo_reply_count();
        let adapter = fixture.adapter.clone();
        let in_flight = tokio::spawn(async move {
            with_served_view_client(adapter, async |mut client| {
                call_forwarded_method(&mut client, forwarded_method)
                    .await
                    .map_err(|status| eyre!("{forwarded_method:?} failed: {status:?}"))
            })
            .await
        });
        fixture.await_utxo_replies(before_replies + 1).await?;
        let probe_permit = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(permit) = reads
                    .admit_response(
                        ForwardedResponseKind::UtxoResponse,
                        tokio::time::Instant::now(),
                    )
                    .await
                {
                    return permit;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|_| eyre!("response permit stayed held during TipWait"))?;
        assert!(!in_flight.is_finished());
        drop(probe_permit);
        fixture.publish_block3();
        let answer = in_flight.await??;
        assert_eq!(answer, ForwardedAnswer::UtxoHeights(vec![1, 3]));
        assert_eq!(fixture.utxo_calls()? - before_calls, 2);
    }
    Ok(())
}

#[tokio::test]
async fn utxo_and_balance_do_not_wait_on_same_height_fork_and_retry_at_most_once() -> Result<()> {
    let fixture = CatchUpFixture::new(
        default_tip_wait(Duration::from_secs(5)),
        ForwardedReadLimit::default(),
    )?;
    *fixture.node_tip.lock() =
        BlockId::new(fixture.block2.height, BlockHash::from_bytes([0xdd; 32]));
    with_served_view_client(fixture.adapter.clone(), async |mut client| {
        for forwarded_method in ALL_FORWARDED_METHODS {
            let started = Instant::now();
            served_tip_mismatch(call_forwarded_method(&mut client, forwarded_method).await)?;
            assert!(started.elapsed() < Duration::from_secs(1));
        }
        Ok(())
    })
    .await?;
    assert_eq!(fixture.utxo_calls()?, ALL_FORWARDED_METHODS.len());

    for forwarded_method in [
        ForwardedMethod::AddressUtxos,
        ForwardedMethod::TaddressBalance,
    ] {
        fixture.reset_to_block2();
        let before_calls = fixture.utxo_calls()?;
        let before_replies = fixture.utxo_reply_count();
        let started = Instant::now();
        let adapter = fixture.adapter.clone();
        let in_flight = tokio::spawn(async move {
            with_served_view_client(adapter, async |mut client| {
                served_tip_mismatch(call_forwarded_method(&mut client, forwarded_method).await)
            })
            .await
        });
        fixture.await_utxo_replies(before_replies + 1).await?;
        *fixture.node_tip.lock() = fixture.block4;
        fixture.publish_block3();
        in_flight.await??;
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(fixture.utxo_calls()? - before_calls, 2);
    }
    Ok(())
}
