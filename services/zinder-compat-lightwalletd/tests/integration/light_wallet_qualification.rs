//! Forwarded RPC behavior and storage availability under a node outage.

use std::{
    fs::File,
    net::{SocketAddr, TcpListener},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::common::verified_node::{one_upgrade_per_height, verifiable_node};
use eyre::{Result, eyre};
use serde_json::{Value, json};
use tokio_stream::StreamExt as _;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, transport::Channel};
use tonic_types::StatusExt as _;
use zinder_compat_lightwalletd::{
    CanonicalReaderSlot, LightwalletdGrpcAdapter, QualificationWallet,
};
use zinder_core::{
    BlockHash, BlockHeight, BlockId, Network, TransactionId, wire::encode_rpc_block_hash_hex,
};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_client::CompactTxStreamerClient,
    compact_tx_streamer_server::CompactTxStreamer as _,
};
use zinder_query::{
    AdmittedIngestControl, DEFAULT_TREE_STATE_REPLAY_WAIT,
    DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT, RocksDbCanonicalReader, WalletServingReadiness,
    spawn_wallet_ingest_control_readiness_probe,
};
use zinder_runtime::{Readiness, ReadinessState};
use zinder_source::{NodeAuth, ZebraJsonRpcSource};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureTransactionRows,
    IngestControlFixture, JsonRpcStub, JsonRpcTestServer, RpcReply, WalletServingStoreFixture,
    method,
};

mod address_validation;
mod canonical;
mod served_view;
mod utxo_transport;

const ADDRESS: &str = "tmDpFafuBHKGUYmuwLsrxWJrwcnSyzEEtYx";

struct Fixture {
    adapter: LightwalletdGrpcAdapter<QualificationWallet>,
    node: JsonRpcTestServer,
    slot: CanonicalReaderSlot,
    _store: WalletServingStoreFixture,
}

impl Fixture {
    fn new(replies: impl IntoIterator<Item = JsonRpcStub>) -> Result<Self> {
        Self::with_node_timeout(1, replies, Duration::from_secs(2))
    }

    fn with_node_timeout(
        blocks: u32,
        replies: impl IntoIterator<Item = JsonRpcStub>,
        timeout: Duration,
    ) -> Result<Self> {
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(blocks);
        Self::from_chain(&chain, &one_upgrade_per_height()?, replies, timeout)
    }

    fn from_chain(
        chain: &ChainFixture,
        schedule: &zinder_core::NetworkUpgradeActivations,
        replies: impl IntoIterator<Item = JsonRpcStub>,
        timeout: Duration,
    ) -> Result<Self> {
        let tip = chain
            .blocks()
            .last()
            .ok_or_else(|| eyre!("missing fixture tip"))?;
        let mut store = WalletServingStoreFixture::from_chain(chain, schedule)?;
        let (canonical, _) = store.take_readers()?;
        let (height, hash) = (tip.height.value(), tip.hash);
        let served_tip = method("getbestblockheightandhash").reply_with(move |_| {
            RpcReply::result(json!({"height": height, "hash": hash.as_bytes()}))
        });
        let node = verifiable_node(
            schedule,
            tip.hash,
            std::iter::once(served_tip).chain(replies),
        )?;
        let source =
            ZebraJsonRpcSource::new(Network::ZcashRegtest, node.url(), NodeAuth::None, timeout)?;
        let slot = CanonicalReaderSlot::new(Arc::new(RocksDbCanonicalReader::new(canonical)?));
        let adapter = LightwalletdGrpcAdapter::for_light_wallet_qualification(
            slot.clone(),
            source,
            DEFAULT_TREE_STATE_REPLAY_WAIT,
            DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT,
        );
        Ok(Self {
            adapter,
            node,
            slot,
            _store: store,
        })
    }
}

/// The tip of the `blocks`-block chain a [`Fixture`] serves.
fn served_tip(blocks: u32) -> Result<BlockId> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(blocks);
    let tip = chain
        .blocks()
        .last()
        .ok_or_else(|| eyre!("missing fixture tip"))?;
    Ok(BlockId::new(tip.height, tip.hash))
}

/// A `getaddressutxos` reply for `chainInfo` that answers at `tip`.
fn utxos_at(tip: BlockId, rows: &Value) -> RpcReply {
    RpcReply::result(json!({
        "utxos": rows,
        "hash": encode_rpc_block_hash_hex(tip.hash),
        "height": tip.height.value(),
    }))
}

fn address_output(txid: &str, height: impl Into<u64>, script: &str) -> Value {
    json!({"address": ADDRESS, "txid": txid, "outputIndex": 0, "script": script,
        "satoshis": 1, "height": height.into()})
}

fn balance_request() -> Request<lightwalletd::AddressList> {
    Request::new(lightwalletd::AddressList {
        addresses: vec![ADDRESS.to_owned()],
    })
}

fn history_request(start: u64, end: u64) -> Request<lightwalletd::TransparentAddressBlockFilter> {
    let block = |height| {
        Some(lightwalletd::BlockId {
            height,
            hash: vec![],
        })
    };
    Request::new(lightwalletd::TransparentAddressBlockFilter {
        address: ADDRESS.to_owned(),
        range: Some(lightwalletd::BlockRange {
            start: block(start),
            end: block(end),
            ..Default::default()
        }),
    })
}

fn history_fixture(
    replies: impl IntoIterator<Item = JsonRpcStub>,
) -> Result<(Fixture, Arc<AtomicBool>)> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(4);
    let hashes: Vec<_> = chain.blocks().iter().map(|block| block.hash).collect();
    let chain = HISTORY.iter().fold(chain, |chain, &(byte, height, index)| {
        let block = hashes[usize::try_from(height - 1).unwrap_or_default()];
        chain.with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            TransactionId::from_bytes([byte; 32]),
            BlockHeight::new(height),
            block,
            index,
            vec![byte],
        ))
    });
    let replaced = Arc::new(AtomicBool::new(false));
    let fork = Arc::clone(&replaced);
    let block_hash = move |height: u64| {
        let served = usize::try_from(height)
            .ok()
            .and_then(|height| hashes.get(height.checked_sub(1)?).copied());
        match served {
            Some(_) if fork.load(Ordering::Acquire) => BlockHash::from_bytes([0xff; 32]),
            Some(hash) => hash,
            None => BlockHash::from_bytes([0xee; 32]),
        }
    };
    let tip_hash = block_hash.clone();
    let fixture = Fixture::from_chain(
        &chain,
        &one_upgrade_per_height()?,
        [
            method("getbestblockheightandhash").reply_with(move |_| {
                RpcReply::result(json!({"height": 4, "hash": tip_hash(4).as_bytes()}))
            }),
            method("getblockhash").reply_with(move |params| {
                let height = params[0].as_u64().unwrap_or_default();
                RpcReply::result(json!(encode_rpc_block_hash_hex(block_hash(height))))
            }),
            method("getaddresstxids").reply_with(|params| {
                let start = params[0]["start"].as_u64().unwrap_or_default();
                let end = params[0]["end"].as_u64().unwrap_or_default();
                if end > start {
                    return RpcReply::error(
                        "more than 100000 transactions match; narrow the height range",
                    );
                }
                let ids = match start {
                    1 => vec!["03".repeat(32), "02".repeat(32)],
                    3 => vec!["04".repeat(32)],
                    4 => vec!["01".repeat(32)],
                    _ => vec![],
                };
                RpcReply::result(json!(ids))
            }),
        ]
        .into_iter()
        .chain(replies),
        Duration::from_secs(2),
    )?;
    Ok((fixture, replaced))
}

fn history_ranges(node: &JsonRpcTestServer) -> Result<Vec<(Value, Value)>> {
    Ok(node
        .requests_for("getaddresstxids")?
        .into_iter()
        .map(|r| (r.params[0]["start"].clone(), r.params[0]["end"].clone()))
        .collect())
}

/// The served history transactions: their repeated ID byte, which is also
/// their one-byte body, and their height and block position.
const HISTORY: [(u8, u32, u32); 5] = [
    (0x03, 1, 0),
    (0x02, 1, 1),
    (0x04, 3, 0),
    (0x01, 4, 0),
    (0x05, 4, 1),
];

/// The hash of block `height` of the four-block chain [`history_fixture`] serves.
fn history_block(height: u32) -> BlockHash {
    ChainFixture::new(Network::ZcashRegtest)
        .extend_blocks(4)
        .blocks()
        .iter()
        .find(|block| block.height.value() == height)
        .map_or(BlockHash::from_bytes([0xee; 32]), |block| block.hash)
}

/// The node's verbose reply mining the history transaction `txid` in served
/// block `height`, with the body [`history_fixture`] stores for it.
fn verbose_reply(txid: &Value, height: u32) -> RpcReply {
    let body = txid.as_str().and_then(|id| id.get(..2)).unwrap_or_default();
    RpcReply::result(json!({
        "hex": body,
        "txid": txid,
        "height": height,
        "blockhash": encode_rpc_block_hash_hex(history_block(height)),
    }))
}

#[tokio::test]
async fn history_aliases_share_admission_until_the_stream_drains_or_is_dropped() -> Result<()> {
    let tip = served_tip(4)?;
    let (fixture, _) = history_fixture([
        method("getrawtransaction").reply_with(|params| verbose_reply(&params[0], 1)),
        method("getaddressutxos").reply_with(move |_| utxos_at(tip, &json!([]))),
    ])?;
    let concurrent = fixture.adapter.clone();
    for drain in [true, false] {
        let mut history = fixture
            .adapter
            .get_taddress_transactions(history_request(1, 1))
            .await?
            .into_inner();
        assert!(history.next().await.transpose()?.is_some());
        let before = fixture.node.requests()?.len();
        let refused = concurrent
            .get_taddress_txids(history_request(1, 1))
            .await
            .err()
            .ok_or_else(|| eyre!("a second request bypassed history admission"))?;
        assert_eq!(refused.code(), Code::ResourceExhausted);
        assert_eq!(fixture.node.requests()?.len(), before);
        concurrent.get_address_utxos(utxo_request(0)).await?;

        if drain {
            assert!(history.next().await.transpose()?.is_some());
            assert!(history.next().await.is_none());
        } else {
            drop(history);
        }
        concurrent.get_taddress_txids(history_request(1, 1)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn cancelling_the_first_history_page_frees_admission() -> Result<()> {
    let page_requested = Arc::new(tokio::sync::Notify::new());
    let received = Arc::clone(&page_requested);
    let (resume_node, paused_node) = std::sync::mpsc::channel::<()>();
    let paused_node = parking_lot::Mutex::new(paused_node);
    let (fixture, _) = history_fixture([method("getaddresstxids").reply_with(move |_| {
        received.notify_one();
        let _ = paused_node.lock().recv();
        RpcReply::result(json!([]))
    })])?;
    let mut pending = fixture.adapter.get_taddress_txids(history_request(1, 1));
    tokio::select! {
        _ = &mut pending => return Err(eyre!("first history request completed before cancellation")),
        () = page_requested.notified() => {}
    }
    let before = fixture.node.requests()?.len();
    let refused = fixture
        .adapter
        .get_taddress_transactions(history_request(1, 1))
        .await
        .err()
        .ok_or_else(|| eyre!("history stream bypassed a pending page fetch"))?;
    assert_eq!(refused.code(), Code::ResourceExhausted);
    assert_eq!(fixture.node.requests()?.len(), before);
    drop(pending);
    drop(resume_node);
    fixture
        .adapter
        .get_taddress_transactions(history_request(1, 1))
        .await?;
    Ok(())
}

#[tokio::test]
async fn oversized_history_splits_into_disjoint_ranges_in_chain_order() -> Result<()> {
    let (fixture, _) = history_fixture([method("getrawtransaction").reply_with(|params| {
        let bytes = params[0]
            .as_str()
            .and_then(|id| id.get(..2))
            .unwrap_or_default();
        let height = match bytes {
            "04" => 3,
            "01" => 4,
            _ => 1,
        };
        verbose_reply(&params[0], height)
    })])?;
    let mut history = fixture
        .adapter
        .get_taddress_txids(history_request(1, 4))
        .await?
        .into_inner();
    let mut received = Vec::new();
    while let Some(row) = history.next().await.transpose()? {
        received.push((row.height, row.data));
    }
    assert_eq!(
        received,
        [(1, vec![3]), (1, vec![2]), (3, vec![4]), (4, vec![1])]
    );
    assert_eq!(
        history_ranges(&fixture.node)?,
        [(1, 4), (1, 2), (1, 1), (2, 2), (3, 4), (3, 3), (4, 4)].map(|(a, b)| (json!(a), json!(b)))
    );
    let reads = fixture.node.requests_for("getrawtransaction")?;
    assert_eq!(reads.len(), 4);
    assert!(reads.iter().all(|read| read.params[1] == 1));
    Ok(())
}

#[tokio::test]
async fn cancelled_transparent_history_stops_raw_transaction_calls() -> Result<()> {
    let (fixture, _) = history_fixture([method("getrawtransaction").reply_with(|params| {
        let reply = verbose_reply(&params[0], 1);
        if params[0] == "02".repeat(32) {
            RpcReply::delayed(Duration::from_millis(80), reply)
        } else {
            reply
        }
    })])?;
    let mut history = fixture
        .adapter
        .get_taddress_transactions(history_request(1, 4))
        .await?
        .into_inner();
    assert_eq!(
        history
            .next()
            .await
            .ok_or_else(|| eyre!("empty history"))??
            .height,
        1
    );
    assert_eq!(fixture.node.requests_for("getrawtransaction")?.len(), 1);
    tokio::select! {
        _ = history.next() => return Err(eyre!("delayed transaction arrived before cancellation")),
        outcome = tokio::time::timeout(Duration::from_secs(2), async {
            while fixture.node.requests_for("getrawtransaction")?.len() < 2 { tokio::task::yield_now().await; }
            Ok::<(),eyre::Report>(())
        }) => outcome??,
    }
    drop(history);
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(fixture.node.requests_for("getrawtransaction")?.len(), 2);
    assert_eq!(
        history_ranges(&fixture.node)?,
        [(1, 4), (1, 2), (1, 1)].map(|(a, b)| (json!(a), json!(b)))
    );
    Ok(())
}

#[tokio::test]
async fn a_transaction_mined_outside_its_history_page_ends_the_stream() -> Result<()> {
    let (fixture, _) = history_fixture([
        method("getrawtransaction").reply_with(|params| verbose_reply(&params[0], 4))
    ])?;
    let mut history = fixture
        .adapter
        .get_taddress_transactions(history_request(1, 4))
        .await?
        .into_inner();
    assert!(
        matches!(history.next().await, Some(Err(status)) if status.code() == Code::Unavailable)
    );
    assert!(history.next().await.is_none());
    assert_eq!(fixture.node.requests_for("getrawtransaction")?.len(), 1);
    fixture
        .adapter
        .get_taddress_txids(history_request(2, 2))
        .await?;
    Ok(())
}

#[tokio::test]
async fn a_reorg_within_the_last_history_page_refuses_the_next_row() -> Result<()> {
    for start in [1, 4] {
        let (fixture, replaced) = history_fixture([
            method("getaddresstxids").reply_with(|params| {
                let start = &params[0]["start"];
                let end = &params[0]["end"];
                if start != end {
                    RpcReply::error("more than 100000 transactions match; narrow the height range")
                } else if start == 4 {
                    // Both are served at height 4, so only the replacement
                    // can refuse the second row.
                    RpcReply::result(json!(["01".repeat(32), "05".repeat(32)]))
                } else {
                    RpcReply::result(json!([]))
                }
            }),
            method("getrawtransaction").reply_with(|params| verbose_reply(&params[0], 4)),
        ])?;
        let mut history = fixture
            .adapter
            .get_taddress_transactions(history_request(start, 4))
            .await?
            .into_inner();
        assert_eq!(
            history.next().await.transpose()?.map(|row| row.height),
            Some(4)
        );
        replaced.store(true, Ordering::Release);
        assert!(
            matches!(history.next().await, Some(Err(status)) if status.code() == Code::Unavailable),
            "a replacement at the end of {start}..=4 must abort the stream"
        );
        assert!(history.next().await.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn a_reorg_after_the_last_history_row_requires_retry_to_finish() -> Result<()> {
    let (fixture, replaced) = history_fixture([
        method("getrawtransaction").reply_with(|params| verbose_reply(&params[0], 1))
    ])?;
    let mut history = fixture
        .adapter
        .get_taddress_txids(history_request(1, 1))
        .await?
        .into_inner();
    assert!(history.next().await.transpose()?.is_some());
    assert!(history.next().await.transpose()?.is_some());
    replaced.store(true, Ordering::Release);
    assert!(
        matches!(history.next().await, Some(Err(status)) if status.code() == Code::Unavailable)
    );
    assert!(history.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn a_reorg_between_history_pages_ends_the_stream() -> Result<()> {
    let (fixture, replaced) = history_fixture([
        method("getrawtransaction").reply_with(|params| verbose_reply(&params[0], 1))
    ])?;
    let mut history = fixture
        .adapter
        .get_taddress_transactions(history_request(1, 4))
        .await?
        .into_inner();
    assert!(history.next().await.transpose()?.is_some());
    assert!(history.next().await.transpose()?.is_some());
    replaced.store(true, Ordering::Release);
    assert!(
        matches!(history.next().await, Some(Err(status)) if status.code() == Code::Unavailable)
    );
    assert!(history.next().await.is_none());
    assert_eq!(fixture.node.requests_for("getrawtransaction")?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn one_oversized_history_height_preserves_the_node_refusal() -> Result<()> {
    let fixture = Fixture::new([
        method("getbestblockheightandhash")
            .reply_with(|_| RpcReply::result(json!({"height": 1, "hash": vec![0; 32]}))),
        method("getaddresstxids").reply(RpcReply::error(
            "more than 100000 transactions match; narrow the height range",
        )),
        method("getaddresstxids").reply_with(|_| RpcReply::result(json!([]))),
    ])?;
    let status = fixture
        .adapter
        .get_taddress_transactions(history_request(1, 1))
        .await
        .err()
        .ok_or_else(|| eyre!("served an incomplete history height"))?;
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert!(fixture.node.requests_for("getrawtransaction")?.is_empty());
    let mut empty = fixture
        .adapter
        .get_taddress_txids(history_request(1, 1))
        .await?
        .into_inner();
    fixture
        .adapter
        .get_taddress_transactions(history_request(1, 1))
        .await?;
    assert!(empty.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn address_index_refusal_and_concurrent_commit_have_distinct_statuses() -> Result<()> {
    let fixture = Fixture::new([
        method("getaddressutxos").reply(RpcReply::error_with_code(
            -1,
            "transparent address and spender indexes are unavailable",
        )),
        method("getaddressutxos").reply(RpcReply::error_with_code(
            -1,
            "state was committing a block",
        )),
    ])?;
    let missing = fixture
        .adapter
        .get_taddress_balance(balance_request())
        .await
        .err()
        .ok_or_else(|| eyre!("unexpected success: missing index"))?;
    assert_eq!(missing.code(), Code::FailedPrecondition);
    assert!(missing.message().contains("address index"));
    let race = fixture
        .adapter
        .get_taddress_balance(balance_request())
        .await
        .err()
        .ok_or_else(|| eyre!("unexpected success: block commit"))?;
    assert_eq!(race.code(), Code::Unavailable);
    assert!(race.message().contains("backoff"));
    Ok(())
}

#[tokio::test]
async fn node_outage_keeps_canonical_reads_and_readyz_available() -> Result<()> {
    let script = hex::encode(
        zinder_source::parse_transparent_address(ADDRESS)?
            .script()
            .as_raw_bytes(),
    );
    let output = address_output(&"01".repeat(32), 1_u32, &script);
    let fixture =
        Fixture::new(
            [method("getaddressutxos").reply(utxos_at(served_tip(1)?, &json!([output])))],
        )?;
    assert_eq!(
        fixture
            .adapter
            .get_taddress_balance(balance_request())
            .await?
            .into_inner()
            .value_zat,
        1
    );
    drop(fixture.node);
    let refused = fixture
        .adapter
        .get_taddress_balance(balance_request())
        .await
        .err()
        .ok_or_else(|| eyre!("unexpected success: stopped node"))?;
    assert_eq!(refused.code(), Code::Unavailable);
    assert_eq!(
        refused
            .get_error_details()
            .error_info()
            .map(|detail| detail.reason.as_str()),
        Some("NODE_UNAVAILABLE")
    );
    assert_eq!(
        fixture
            .adapter
            .get_latest_block(Request::new(lightwalletd::ChainSpec {}))
            .await?
            .into_inner()
            .height,
        1
    );
    assert_degraded_node_readiness(&fixture.adapter).await
}

#[tokio::test]
async fn light_wallet_profile_serves_canonical_and_forwarded_reads_from_the_binary() -> Result<()> {
    let schedule = one_upgrade_per_height()?;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let tip = served_tip(2)?;
    let writer = CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &schedule)?;
    let primary = writer.canonical_primary()?;
    assert_eq!(
        primary.raw_blob_retention(),
        zinder_store::RawBlobRetention::None
    );
    let script = hex::encode(
        zinder_source::parse_transparent_address(ADDRESS)?
            .script()
            .as_raw_bytes(),
    );
    let output = address_output(&"01".repeat(32), 1_u32, &script);
    let node = verifiable_node(
        &schedule,
        tip.hash,
        [method("getaddressutxos").reply(utxos_at(tip, &json!([output])))],
    )?;
    let files = tempfile::tempdir()?;
    let cancel = CancellationToken::new();
    let _cancel_on_exit = cancel.clone().drop_guard();
    let control =
        spawn_binary_ingest_control(primary, files.path().join("checkpoints"), cancel).await?;
    let log_path = files.path().join("compat.stderr");
    let reservation = TcpListener::bind("127.0.0.1:0")?;
    let addr = reservation.local_addr()?;
    drop(reservation);
    let mut child =
        tokio::process::Command::new(zinder_testkit::bin_exe!("zinder-compat-lightwalletd"))
            .env_clear()
            .env("ZINDER_DEPLOYMENT__PROFILE", "light-wallet")
            .env("ZINDER_NETWORK__NAME", "zcash-regtest")
            .env("ZINDER_NODE__JSON_RPC_ADDR", node.url().as_str())
            .env("ZINDER_INGEST_CONTROL__ADDR", control)
            .env("ZINDER_COMPAT__STALE_TIP_LIMIT_MS", "0")
            .env(
                "ZINDER_COMPAT__REORG_WINDOW_BLOCKS",
                primary.reorg_policy().reorg_window_blocks().to_string(),
            )
            .env("ZINDER_OPS__LISTEN_ADDR", "127.0.0.1:0")
            .env("ZINDER_COMPAT__LISTEN_ADDR", addr.to_string())
            .env("ZINDER_STORAGE__PATH", writer.canonical_primary_path())
            .env(
                "ZINDER_STORAGE__SECONDARY_PATH",
                files.path().join("secondary"),
            )
            .stderr(File::create(&log_path)?)
            .kill_on_drop(true)
            .spawn()?;
    let mut client = wait_for_compat(&mut child, addr, &log_path).await?;
    assert_eq!(
        client
            .get_latest_block(lightwalletd::ChainSpec {})
            .await?
            .into_inner()
            .height,
        2
    );
    assert_binary_block_range(&mut client, 1..=2).await?;
    assert_eq!(
        client
            .get_taddress_balance(balance_request())
            .await?
            .into_inner()
            .value_zat,
        1
    );
    child.kill().await?;
    Ok(())
}

#[tokio::test]
async fn every_light_wallet_selection_refuses_the_same_checkpointed_history() -> Result<()> {
    let schedule = one_upgrade_per_height()?;
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(zinder_store::RawBlobRetention::Transactions)
        .extend_blocks(2);
    let baseline = CanonicalBaseline::new(&chain).with_history_floor(BlockHeight::new(1));
    let writer = CanonicalWriterFixture::publish(&baseline, &schedule)?;
    let primary = writer.canonical_primary()?;
    let node = verifiable_node(&schedule, served_tip(2)?.hash, [])?;
    let files = tempfile::tempdir()?;
    let cancel = CancellationToken::new();
    let _cancel_on_exit = cancel.clone().drop_guard();
    let control =
        spawn_binary_ingest_control(primary, files.path().join("checkpoints"), cancel).await?;

    let mut refusal = None;
    for (label, selection) in [
        ("profile", "[deployment]\nprofile = \"light-wallet\""),
        ("serving", "[compat]\nserving = \"light-wallet\""),
        (
            "legacy",
            "[compat]\nserving = \"light-wallet-qualification\"",
        ),
    ] {
        let config_path = files.path().join(format!("{label}.toml"));
        std::fs::write(&config_path, selection)?;
        let child =
            tokio::process::Command::new(zinder_testkit::bin_exe!("zinder-compat-lightwalletd"))
                .args([
                    "--config",
                    config_path
                        .to_str()
                        .ok_or_else(|| eyre!("non-UTF-8 config path"))?,
                ])
                .env_clear()
                .env("ZINDER_NETWORK__NAME", "zcash-regtest")
                .env("ZINDER_NODE__JSON_RPC_ADDR", node.url())
                .env("ZINDER_INGEST_CONTROL__ADDR", &control)
                .env(
                    "ZINDER_COMPAT__REORG_WINDOW_BLOCKS",
                    primary.reorg_policy().reorg_window_blocks().to_string(),
                )
                .env("ZINDER_OPS__LISTEN_ADDR", "127.0.0.1:0")
                .env("ZINDER_COMPAT__LISTEN_ADDR", "127.0.0.1:0")
                .env("ZINDER_STORAGE__PATH", writer.canonical_primary_path())
                .env("ZINDER_STORAGE__SECONDARY_PATH", files.path().join(label))
                .kill_on_drop(true)
                .output();
        let output = tokio::time::timeout(Duration::from_secs(10), child).await??;
        assert!(
            !output.status.success(),
            "{label} accepted checkpointed history"
        );
        let stderr = String::from_utf8(output.stderr)?;
        let diagnostic = stderr
            .lines()
            .find_map(|line| {
                line.split_once("lacks required BlockHistory coverage")
                    .map(|(_, tail)| tail)
            })
            .ok_or_else(|| eyre!("{label} missed the history refusal: {stderr}"))?;
        assert!(
            diagnostic.contains(
                "required from Some(BlockHeight(1)), available from Some(BlockHeight(2))"
            ),
            "{diagnostic}"
        );
        if let Some(previous) = &refusal {
            assert_eq!(previous, diagnostic);
        }
        refusal = Some(diagnostic.to_owned());
    }
    Ok(())
}

#[tokio::test]
async fn wallet_pair_bootstraps_and_serves_with_a_missing_early_root() -> Result<()> {
    use zinder_wallet_rocksdb::{RocksDbWalletBuildOptions, build_wallet_from_canonical};

    let writer = super::lightwalletd_grpc::floor_with_completed_subtree_writer(
        zinder_store::RawBlobRetention::Transactions,
    )?;
    let primary = writer.canonical_primary()?;
    let files = tempfile::tempdir()?;
    let wallet_path = files.path().join("wallet-primary");
    let reorg_window = primary.reorg_policy().reorg_window_blocks();
    let projection = build_wallet_from_canonical(
        primary,
        &wallet_path,
        RocksDbWalletBuildOptions {
            supported_reorg_depth: reorg_window,
            ..RocksDbWalletBuildOptions::for_local_tests()
        },
    )?;
    drop(projection.store);
    let cancel = CancellationToken::new();
    let _cancel_on_exit = cancel.clone().drop_guard();
    let control =
        spawn_binary_ingest_control(primary, files.path().join("checkpoints"), cancel).await?;
    let reservation = TcpListener::bind("127.0.0.1:0")?;
    let addr = reservation.local_addr()?;
    drop(reservation);
    let log_path = files.path().join("wallet.stderr");
    let mut wallet =
        tokio::process::Command::new(zinder_testkit::bin_exe!("zinder-compat-lightwalletd"))
            .env_clear()
            .env("ZINDER_NETWORK__NAME", "zcash-regtest")
            .env("ZINDER_INGEST_CONTROL__ADDR", &control)
            .env("ZINDER_COMPAT__SERVING", "wallet")
            .env(
                "ZINDER_COMPAT__REORG_WINDOW_BLOCKS",
                reorg_window.to_string(),
            )
            .env("ZINDER_OPS__LISTEN_ADDR", "127.0.0.1:0")
            .env("ZINDER_COMPAT__LISTEN_ADDR", addr.to_string())
            .env("ZINDER_STORAGE__PATH", writer.canonical_primary_path())
            .env(
                "ZINDER_STORAGE__SECONDARY_PATH",
                files.path().join("canonical-secondary"),
            )
            .env("ZINDER_WALLET__PATH", wallet_path)
            .env(
                "ZINDER_WALLET__SECONDARY_PATH",
                files.path().join("wallet-secondary"),
            )
            .kill_on_drop(true)
            .stderr(File::create(&log_path)?)
            .spawn()?;
    let mut client = wait_for_compat(&mut wallet, addr, &log_path).await?;
    assert_checkpointed_wallet_reads(&mut client).await?;
    wallet.kill().await?;
    Ok(())
}

async fn assert_checkpointed_wallet_reads(
    client: &mut CompactTxStreamerClient<Channel>,
) -> Result<()> {
    assert_binary_block_range(client, 2..=2).await?;
    let missing = client
        .get_subtree_roots(lightwalletd::GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: lightwalletd::ShieldedProtocol::Sapling as i32,
            max_entries: 1,
        })
        .await
        .err()
        .ok_or_else(|| eyre!("the missing root unexpectedly opened a stream"))?;
    assert_eq!(missing.code(), Code::NotFound);
    assert_eq!(
        missing
            .get_error_details()
            .error_info()
            .map(|detail| detail.reason.as_str()),
        Some("ARTIFACT_UNAVAILABLE")
    );
    Ok(())
}

async fn assert_binary_block_range(
    client: &mut CompactTxStreamerClient<Channel>,
    heights: std::ops::RangeInclusive<u64>,
) -> Result<()> {
    let block_id = |height| lightwalletd::BlockId {
        height,
        hash: Vec::new(),
    };
    let mut blocks = client
        .get_block_range(lightwalletd::BlockRange {
            start: Some(block_id(*heights.start())),
            end: Some(block_id(*heights.end())),
            pool_types: Vec::new(),
        })
        .await?
        .into_inner();
    for height in heights {
        assert_eq!(
            blocks.message().await?.map(|block| block.height),
            Some(height)
        );
    }
    assert!(blocks.message().await?.is_none());
    Ok(())
}

async fn wait_for_compat(
    child: &mut tokio::process::Child,
    addr: SocketAddr,
    log_path: &Path,
) -> Result<CompactTxStreamerClient<Channel>> {
    use tonic::transport::Endpoint;

    let endpoint = Endpoint::from_shared(format!("http://{addr}"))?
        .connect_timeout(Duration::from_millis(300))
        .timeout(Duration::from_secs(5));
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(channel) = endpoint.connect().await {
                break Ok(CompactTxStreamerClient::new(channel));
            }
            if let Some(status) = child.try_wait()? {
                break Err(eyre!(
                    "compat exited with {status}: {}",
                    std::fs::read_to_string(log_path)?
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}

async fn spawn_binary_ingest_control(
    store: &zinder_store::RocksDbCanonicalStore,
    checkpoint_path: std::path::PathBuf,
    cancel: CancellationToken,
) -> Result<String> {
    use tokio_stream::wrappers::TcpListenerStream;
    use zinder_ingest::{
        CanonicalCheckpointStagingRoot, CanonicalControlCommand, CanonicalControlGrpcAdapter,
        canonical_control_channel,
    };
    use zinder_proto::{
        v1::ingest::{CanonicalWriterStatusResponse, ingest_control_server::IngestControlServer},
        wire::{canonical_writer_fence_message, encode_canonical_network_upgrade_schedule},
    };

    let fence = store.event_fence();
    let schedule = store
        .network_upgrade_schedule()
        .ok_or_else(|| eyre!("missing schedule"))?;
    let status = CanonicalWriterStatusResponse {
        network_name: zinder_core::wire::encode_zinder_native_chain_name(store.network())
            .to_owned(),
        fence: Some(canonical_writer_fence_message(
            fence.chain_epoch_id(),
            fence.chain_event_sequence(),
            fence.visible_tip(),
            fence.sequence_digest(),
        )),
        oldest_retained_event_sequence: store.canonical_event_retention_floor()?,
        network_upgrade_schedule: Some(encode_canonical_network_upgrade_schedule(
            schedule.activations(),
            schedule.revision(),
        )),
        ..CanonicalWriterStatusResponse::default()
    };
    let (handle, mut commands) = canonical_control_channel();
    tokio::spawn(async move {
        while let Some(command) = commands.recv().await {
            if let CanonicalControlCommand::WriterStatus { reply } = command {
                let _ = reply.send(Ok(status.clone()));
            }
        }
    });
    let canonical = CanonicalControlGrpcAdapter::new(
        handle,
        CanonicalCheckpointStagingRoot::new(checkpoint_path),
        zinder_store::RocksDbResourceBudget::for_local_tests(),
        zinder_ingest::CanonicalStoreDamageSignal::default(),
    );
    let ingest = zinder_testkit::IngestControlFixtureService::new(store.network());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(canonical.into_server())
            .add_service(IngestControlServer::new(ingest))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                cancel.cancelled_owned(),
            )
            .await
    });
    Ok(format!("http://{addr}"))
}

async fn assert_degraded_node_readiness(
    adapter: &LightwalletdGrpcAdapter<QualificationWallet>,
) -> Result<()> {
    let readiness = Readiness::default();
    let composed = WalletServingReadiness::with_node_and_ingest_control(readiness.clone());
    composed
        .canonical_admission()
        .publish_admission(ReadinessState::ready(Some(1)));
    let control = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    let ingest =
        AdmittedIngestControl::connect(control.endpoint(), None, Network::ZcashRegtest).await?;
    let (address, _ops) = super::health::spawn_readyz(&readiness).await?;
    let cancel = CancellationToken::new();
    let interval = Duration::from_millis(20);
    let task = adapter.spawn_node_readiness_probe(composed.clone(), interval, cancel.clone());
    let ingest_task =
        spawn_wallet_ingest_control_readiness_probe(ingest, composed, interval, cancel.clone());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let report = reqwest::get(format!("http://{address}/readyz")).await?;
            assert_eq!(report.status(), reqwest::StatusCode::OK);
            let body: Value = serde_json::from_str(&report.text().await?)?;
            if body["degraded"].as_array().is_some_and(|rows| {
                rows.iter().any(|row| {
                    row["input"] == "node_source"
                        && row["dependency_cause"]["node_unavailable"].is_object()
                })
            }) {
                break;
            }
            tokio::time::sleep(interval).await;
        }
        Ok::<(), eyre::Report>(())
    })
    .await??;
    cancel.cancel();
    task.await?;
    ingest_task.await?;
    control.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn utxo_prefix_completes_its_last_height_at_one_node_tip() -> Result<()> {
    let script = hex::encode(
        zinder_source::parse_transparent_address(ADDRESS)?
            .script()
            .as_raw_bytes(),
    );
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let served = chain
        .blocks()
        .last()
        .ok_or_else(|| eyre!("missing fixture tip"))?
        .hash;
    let moved = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([1; 32]));
    let served = BlockId::new(BlockHeight::new(2), served);
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let replies = [method("getaddressutxos").reply_with(move |params| {
        // Each request reads two pages; the second request's last page
        // is answered after the node moved.
        let tip = if calls.fetch_add(1, Ordering::AcqRel) < 3 {
            served
        } else {
            moved
        };
        let requested = params[0]["maxEntries"]
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(0);
        let start = params[0]["startHeight"].as_u64().unwrap_or(0);
        let rows: Vec<_> = [(1, "04"), (2, "03"), (2, "02"), (2, "01")]
            .into_iter()
            .filter(|(height, _)| *height >= start)
            .take(requested)
            .map(|(height, id)| address_output(&id.repeat(32), height, &script))
            .collect();
        utxos_at(tip, &json!(rows))
    })];
    let fixture = Fixture::from_chain(
        &chain,
        &one_upgrade_per_height()?,
        replies,
        Duration::from_secs(2),
    )?;
    let request = || {
        Request::new(lightwalletd::GetAddressUtxosArg {
            addresses: vec![ADDRESS.to_owned()],
            start_height: 1,
            max_entries: 2,
        })
    };
    let outputs = fixture
        .adapter
        .get_address_utxos(request())
        .await?
        .into_inner()
        .address_utxos;
    assert_eq!(outputs.len(), 2);
    assert_eq!(outputs[0].txid, vec![4; 32]);
    assert_eq!(outputs[1].txid, vec![1; 32]);
    let changed = fixture
        .adapter
        .get_address_utxos(request())
        .await
        .err()
        .ok_or_else(|| eyre!("combined UTXOs from different node tips"))?;
    assert_eq!(changed.code(), Code::Unavailable);
    assert!(changed.message().contains("retry with backoff"));
    Ok(())
}

#[tokio::test]
async fn balance_address_limit_is_checked_before_contacting_the_node() -> Result<()> {
    let fixture = Fixture::new([])?;
    let addresses = (0u32..=256)
        .map(|index| {
            let mut hash = [0; 20];
            hash[..4].copy_from_slice(&index.to_le_bytes());
            zebra_chain::transparent::Address::from_pub_key_hash(
                zebra_chain::parameters::NetworkKind::Testnet,
                hash,
            )
            .to_string()
        })
        .collect();
    let rejected = fixture
        .adapter
        .get_taddress_balance(Request::new(lightwalletd::AddressList { addresses }))
        .await
        .err()
        .ok_or_else(|| eyre!("accepted more than 256 addresses"))?;
    assert_eq!(rejected.code(), Code::InvalidArgument);
    assert!(fixture.node.requests()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn maximum_utxo_prefix_uses_a_bounded_node_lookup() -> Result<()> {
    let tip = served_tip(1)?;
    let fixture = Fixture::new([method("getaddressutxos").reply_with(move |params| {
        if params[0]["maxEntries"] == 100_000 {
            utxos_at(tip, &json!([]))
        } else {
            RpcReply::error_with_code(-1, "expected an explicit 100,000-row prefix")
        }
    })])?;
    let result = fixture
        .adapter
        .get_address_utxos(Request::new(lightwalletd::GetAddressUtxosArg {
            addresses: vec![ADDRESS.to_owned()],
            start_height: 1,
            max_entries: 100_000,
        }))
        .await?
        .into_inner();
    assert_eq!(
        result.address_utxos,
        Vec::<lightwalletd::GetAddressUtxosReply>::new()
    );
    Ok(())
}

#[derive(Clone, Copy)]
enum UtxoNode {
    Zebra,
    Zakura,
}

fn populated_utxo_fixture(
    first_height_rows: u32,
    next_height_rows: u32,
    node: UtxoNode,
) -> Result<Fixture> {
    let script = hex::encode(
        zinder_source::parse_transparent_address(ADDRESS)?
            .script()
            .as_raw_bytes(),
    );
    let mut first_rpc_id = [0; 32];
    first_rpc_id[31] = 1;
    let mut last_rpc_id = [0; 32];
    last_rpc_id[0] = 2;
    let first_id = hex::encode(first_rpc_id);
    let last_id = hex::encode(last_rpc_id);
    // The second txid sorts first in the wallet's internal byte order.
    let groups = [
        (5_u64, first_id, first_height_rows - 1),
        (5, last_id, 1),
        (6, "03".repeat(32), next_height_rows),
    ];
    let tip = served_tip(6)?;
    Fixture::with_node_timeout(
        6,
        [method("getaddressutxos").reply_with(move |params| {
            let (start, limit) = match node {
                UtxoNode::Zebra => (
                    params[0]["startHeight"].as_u64().unwrap_or(0),
                    params[0]["maxEntries"].as_u64().unwrap_or(0),
                ),
                UtxoNode::Zakura => (0, 0),
            };
            let matching: u32 = groups
                .iter()
                .filter(|(height, _, _)| *height >= start)
                .map(|(_, _, count)| count)
                .sum();
            if matches!(node, UtxoNode::Zebra) && limit == 0 && matching > 100_000 {
                return RpcReply::error(
                    "more than 100000 unspent outputs match; page with startHeight and maxEntries",
                );
            }
            let mut rows = Vec::new();
            for (height, txid, count) in &groups {
                if *height < start {
                    continue;
                }
                for index in 0..*count {
                    rows.push(
                        json!({"address": ADDRESS, "txid": txid, "outputIndex": index,
                            "script": script, "satoshis": 1, "height": height}),
                    );
                    if u64::try_from(rows.len()) == Ok(limit) {
                        return utxos_at(tip, &Value::Array(rows));
                    }
                }
            }
            utxos_at(tip, &Value::Array(rows))
        })],
        Duration::from_secs(30),
    )
}

async fn populated_wallet_utxos(
    fixture: &Fixture,
    max_entries: u32,
) -> Result<Vec<lightwalletd::GetAddressUtxosReply>> {
    let outputs = fixture
        .adapter
        .get_address_utxos(Request::new(lightwalletd::GetAddressUtxosArg {
            addresses: vec![ADDRESS.to_owned()],
            start_height: 1,
            max_entries,
        }))
        .await?
        .into_inner()
        .address_utxos;
    let mut expected_first = [0; 32];
    expected_first[31] = 2;
    assert_eq!(outputs.len(), usize::try_from(max_entries)?);
    assert_eq!(outputs[0].txid, expected_first);
    Ok(outputs)
}

#[tokio::test]
async fn cap_sized_cutoff_height_returns_a_smaller_wallet_ordered_prefix() -> Result<()> {
    let fixture = populated_utxo_fixture(100_000, 0, UtxoNode::Zebra)?;
    let rows = populated_wallet_utxos(&fixture, 99_999).await?;
    assert_eq!((rows[99_998].height, rows[99_998].index), (5, 99_997));
    let reads = fixture.node.requests_for("getaddressutxos")?;
    let last = reads.last().ok_or_else(|| eyre!("missing cutoff lookup"))?;
    assert_eq!(last.params[0]["startHeight"], 5);
    assert_eq!(last.params[0]["maxEntries"], 0);
    Ok(())
}

#[tokio::test]
async fn cap_sized_multi_height_prefix_stays_bounded_and_wallet_ordered() -> Result<()> {
    let fixture = populated_utxo_fixture(2, 99_998, UtxoNode::Zebra)?;
    let rows = populated_wallet_utxos(&fixture, 100_000).await?;
    assert_eq!((rows[99_999].height, rows[99_999].index), (6, 99_997));
    let reads = fixture.node.requests_for("getaddressutxos")?;
    assert_eq!(reads.len(), 2);
    assert_eq!(reads[1].params[0]["startHeight"], 6);
    assert!(
        reads
            .iter()
            .all(|read| read.params[0]["maxEntries"] == 100_000)
    );
    Ok(())
}

fn utxo_request(max_entries: u32) -> Request<lightwalletd::GetAddressUtxosArg> {
    Request::new(lightwalletd::GetAddressUtxosArg {
        addresses: vec![ADDRESS.to_owned()],
        start_height: 5,
        max_entries,
    })
}

#[tokio::test]
async fn later_outputs_can_prevent_proving_a_full_cutoff_height() -> Result<()> {
    for node in [UtxoNode::Zebra, UtxoNode::Zakura] {
        let fixture = populated_utxo_fixture(100_000, 1, node)?;
        let error = fixture
            .adapter
            .get_address_utxos(utxo_request(99_999))
            .await
            .err()
            .ok_or_else(|| eyre!("returned a prefix with unknown final-height order"))?;
        assert_eq!(error.code(), Code::ResourceExhausted);
        let reads = fixture.node.requests_for("getaddressutxos")?;
        let last = reads.last().ok_or_else(|| eyre!("missing cutoff lookup"))?;
        assert_eq!(last.params[0]["startHeight"], 5);
        assert_eq!(last.params[0]["maxEntries"], 0);
    }
    Ok(())
}

#[tokio::test]
async fn an_above_limit_wallet_request_succeeds_only_when_the_complete_set_fits() -> Result<()> {
    let small = populated_utxo_fixture(2, 1, UtxoNode::Zebra)?;
    let outputs = small
        .adapter
        .get_address_utxos(utxo_request(100_001))
        .await?
        .into_inner();
    assert_eq!(outputs.address_utxos.len(), 3);

    let full = populated_utxo_fixture(100_001, 0, UtxoNode::Zakura)?;
    let error = full
        .adapter
        .get_address_utxos_stream(utxo_request(100_001))
        .await
        .err()
        .ok_or_else(|| eyre!("opened a stream for an oversized complete UTXO set"))?;
    assert_eq!(error.code(), Code::ResourceExhausted);
    Ok(())
}

#[tokio::test]
async fn a_node_that_supplies_the_complete_utxo_page_needs_one_read() -> Result<()> {
    let fixture = populated_utxo_fixture(17, 1, UtxoNode::Zakura)?;
    populated_wallet_utxos(&fixture, 1).await?;
    let reads = fixture.node.requests_for("getaddressutxos")?;
    assert_eq!(reads.len(), 1);
    assert_eq!(reads[0].params[0]["maxEntries"], 2);
    Ok(())
}

#[tokio::test]
async fn utxo_admission_is_shared_until_the_stream_drains_or_is_dropped() -> Result<()> {
    let fixture = populated_utxo_fixture(2, 1, UtxoNode::Zakura)?;
    let concurrent = fixture.adapter.clone();
    for cancel in [false, true] {
        let mut stream = fixture
            .adapter
            .get_address_utxos_stream(utxo_request(2))
            .await?
            .into_inner();
        assert!(stream.next().await.transpose()?.is_some());
        let before = fixture.node.requests_for("getaddressutxos")?.len();
        let refused = concurrent
            .get_address_utxos(utxo_request(2))
            .await
            .err()
            .ok_or_else(|| eyre!("a second request bypassed UTXO admission"))?;
        assert_eq!(refused.code(), Code::ResourceExhausted);
        let balance = concurrent
            .get_taddress_balance(balance_request())
            .await
            .err()
            .ok_or_else(|| eyre!("a balance bypassed UTXO admission"))?;
        assert_eq!(balance.code(), Code::ResourceExhausted);
        assert_eq!(fixture.node.requests_for("getaddressutxos")?.len(), before);
        if cancel {
            drop(stream);
        } else {
            assert!(stream.next().await.transpose()?.is_some());
            assert!(stream.next().await.is_none());
        }
        populated_wallet_utxos(&fixture, 2).await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_pending_balance_holds_utxo_admission_until_it_answers() -> Result<()> {
    let tip = served_tip(1)?;
    let fixture = Fixture::new([
        method("getaddressutxos").reply(RpcReply::delayed(
            Duration::from_millis(300),
            utxos_at(tip, &json!([])),
        )),
        method("getaddressutxos").reply(utxos_at(tip, &json!([]))),
    ])?;
    let mut pending = Box::pin(fixture.adapter.get_taddress_balance(balance_request()));
    tokio::select! {
        _ = &mut pending => return Err(eyre!("delayed balance arrived before inspection")),
        outcome = tokio::time::timeout(Duration::from_secs(2), async {
            while fixture.node.requests_for("getaddressutxos")?.is_empty() { tokio::task::yield_now().await; }
            Ok::<(),eyre::Report>(())
        }) => outcome??,
    }
    let utxos = fixture.adapter.get_address_utxos(utxo_request(0)).await;
    let balance = fixture
        .adapter
        .get_taddress_balance(balance_request())
        .await;
    for refused in [utxos.err(), balance.err()] {
        let status = refused.ok_or_else(|| eyre!("a request bypassed a pending balance"))?;
        assert_eq!(status.code(), Code::ResourceExhausted);
    }
    assert_eq!(pending.await?.into_inner().value_zat, 0);
    assert_eq!(fixture.node.requests_for("getaddressutxos")?.len(), 1);
    fixture.adapter.get_address_utxos(utxo_request(0)).await?;
    Ok(())
}

#[tokio::test]
async fn cancelling_a_pending_unary_utxo_read_frees_admission() -> Result<()> {
    let tip = served_tip(1)?;
    let fixture = Fixture::new([
        method("getaddressutxos").reply(RpcReply::delayed(
            Duration::from_millis(200),
            utxos_at(tip, &json!([])),
        )),
        method("getaddressutxos").reply(utxos_at(tip, &json!([]))),
    ])?;
    let mut pending = Box::pin(fixture.adapter.get_address_utxos(utxo_request(0)));
    tokio::select! {
        _ = &mut pending => return Err(eyre!("delayed read arrived before cancellation")),
        outcome = tokio::time::timeout(Duration::from_secs(2), async {
            while fixture.node.requests_for("getaddressutxos")?.is_empty() { tokio::task::yield_now().await; }
            Ok::<(),eyre::Report>(())
        }) => outcome??,
    }
    let refused = fixture
        .adapter
        .get_address_utxos_stream(utxo_request(0))
        .await
        .err()
        .ok_or_else(|| eyre!("stream bypassed pending unary UTXO read"))?;
    assert_eq!(refused.code(), Code::ResourceExhausted);
    assert_eq!(fixture.node.requests_for("getaddressutxos")?.len(), 1);
    drop(pending);
    assert_eq!(
        fixture
            .adapter
            .get_address_utxos(utxo_request(0))
            .await?
            .into_inner()
            .address_utxos,
        Vec::<lightwalletd::GetAddressUtxosReply>::new()
    );
    Ok(())
}

#[tokio::test]
async fn history_rows_out_of_canonical_order_end_the_stream() -> Result<()> {
    // Block 1 mines 03 then 02; the node lists them the other way round.
    let (fixture, _) = history_fixture([
        method("getaddresstxids")
            .reply_with(|_| RpcReply::result(json!(["02".repeat(32), "03".repeat(32)]))),
        method("getrawtransaction").reply_with(|params| verbose_reply(&params[0], 1)),
    ])?;
    let mut history = fixture
        .adapter
        .get_taddress_transactions(history_request(1, 1))
        .await?
        .into_inner();
    assert_eq!(
        history.next().await.transpose()?.map(|row| row.data),
        Some(vec![0x02])
    );
    let status = history
        .next()
        .await
        .and_then(Result::err)
        .ok_or_else(|| eyre!("served history out of canonical order"))?;
    assert_eq!(status.code(), Code::DataLoss);
    Ok(())
}

/// A node whose first page shows one output at height 1 and two at height 2,
/// and whose reread from height 2 returns `continuation`.
fn utxo_cutoff_fixture(continuation: RpcReply) -> Result<Fixture> {
    let script = hex::encode(
        zinder_source::parse_transparent_address(ADDRESS)?
            .script()
            .as_raw_bytes(),
    );
    let tip = served_tip(2)?;
    let first = [(1_u32, "04"), (2, "03"), (2, "02")]
        .map(|(height, id)| address_output(&id.repeat(32), height, &script));
    Fixture::with_node_timeout(
        2,
        [
            method("getaddressutxos").reply(utxos_at(tip, &json!(first))),
            method("getaddressutxos").reply(continuation),
        ],
        Duration::from_secs(2),
    )
}

fn two_utxos() -> Request<lightwalletd::GetAddressUtxosArg> {
    Request::new(lightwalletd::GetAddressUtxosArg {
        addresses: vec![ADDRESS.to_owned()],
        start_height: 1,
        max_entries: 2,
    })
}

#[tokio::test]
async fn a_reread_that_drops_outputs_at_the_cutoff_height_is_unavailable() -> Result<()> {
    let fixture = utxo_cutoff_fixture(utxos_at(served_tip(2)?, &json!([])))?;
    let status = fixture
        .adapter
        .get_address_utxos(two_utxos())
        .await
        .err()
        .ok_or_else(|| eyre!("shortened the response from contradictory pages"))?;
    assert_eq!(status.code(), Code::Unavailable);
    assert_eq!(fixture.node.requests_for("getaddressutxos")?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn a_reread_that_changes_an_output_value_is_unavailable() -> Result<()> {
    let script = hex::encode(
        zinder_source::parse_transparent_address(ADDRESS)?
            .script()
            .as_raw_bytes(),
    );
    // The reread repeats both outpoints at height 2 but values one of them
    // differently.
    let mut changed = [(2_u32, "03"), (2, "02")]
        .map(|(height, id)| address_output(&id.repeat(32), height, &script));
    changed[1]["satoshis"] = json!(7);
    let fixture = utxo_cutoff_fixture(utxos_at(served_tip(2)?, &json!(changed)))?;
    let status = fixture
        .adapter
        .get_address_utxos(two_utxos())
        .await
        .err()
        .ok_or_else(|| eyre!("accepted contradictory output values"))?;
    assert_eq!(status.code(), Code::Unavailable);
    Ok(())
}

#[tokio::test]
async fn a_continuation_page_without_chain_info_is_refused() -> Result<()> {
    let fixture = utxo_cutoff_fixture(RpcReply::result(json!([])))?;
    let status = fixture
        .adapter
        .get_address_utxos(two_utxos())
        .await
        .err()
        .ok_or_else(|| eyre!("accepted a page that names no tip"))?;
    assert_eq!(status.code(), Code::DataLoss);
    Ok(())
}

/// A node serving `blocks` blocks that answers successive `getaddressutxos`
/// reads with `pages`, each naming the served tip. A page lists
/// `(height, repeated txid byte)` outputs.
fn utxo_pages_fixture(blocks: u32, pages: &[&[(u32, &str)]]) -> Result<Fixture> {
    let script = hex::encode(
        zinder_source::parse_transparent_address(ADDRESS)?
            .script()
            .as_raw_bytes(),
    );
    let tip = served_tip(blocks)?;
    let replies: Vec<_> = pages
        .iter()
        .map(|page| {
            let rows: Vec<_> = page
                .iter()
                .map(|(height, id)| address_output(&id.repeat(32), *height, &script))
                .collect();
            method("getaddressutxos").reply(utxos_at(tip, &json!(rows)))
        })
        .collect();
    Fixture::with_node_timeout(blocks, replies, Duration::from_secs(2))
}

fn bounded_utxos(max_entries: u32) -> Request<lightwalletd::GetAddressUtxosArg> {
    Request::new(lightwalletd::GetAddressUtxosArg {
        addresses: vec![ADDRESS.to_owned()],
        start_height: 1,
        max_entries,
    })
}

#[tokio::test]
async fn a_later_reread_that_drops_outputs_an_earlier_reread_showed_is_unavailable() -> Result<()> {
    // maxEntries 1: the first read shows two outputs at the cutoff height, the
    // four-row reread repeats both and adds two, and the final reread returns
    // only the original two.
    let fixture = utxo_pages_fixture(
        2,
        &[
            &[(2, "05"), (2, "06")],
            &[(2, "05"), (2, "06"), (2, "01"), (2, "02")],
            &[(2, "05"), (2, "06")],
        ],
    )?;
    let status = fixture
        .adapter
        .get_address_utxos(bounded_utxos(1))
        .await
        .err()
        .ok_or_else(|| eyre!("dropped outputs an earlier reread showed"))?;
    assert_eq!(status.code(), Code::Unavailable);
    assert_eq!(fixture.node.requests_for("getaddressutxos")?.len(), 3);
    Ok(())
}

#[tokio::test]
async fn a_reread_that_moves_an_earlier_output_above_the_cutoff_is_unavailable() -> Result<()> {
    // The first read places 04 below the cutoff height 2; the reread from 2
    // places the same output at height 3, above it.
    let fixture = utxo_pages_fixture(
        3,
        &[
            &[(1, "04"), (2, "03"), (2, "02")],
            &[(2, "03"), (2, "02"), (3, "04")],
        ],
    )?;
    let status = fixture
        .adapter
        .get_address_utxos(bounded_utxos(2))
        .await
        .err()
        .ok_or_else(|| eyre!("accepted an output at two heights"))?;
    assert_eq!(status.code(), Code::Unavailable);
    Ok(())
}
