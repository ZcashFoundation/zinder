//! Forwarded RPC behavior and storage availability under a node outage.

use std::{
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
use tonic::{Code, Request};
use tonic_types::StatusExt as _;
use zinder_compat_lightwalletd::{
    CanonicalReaderSlot, LightwalletdGrpcAdapter, QualificationWallet,
};
use zinder_core::{BlockHash, Network, wire::encode_rpc_block_hash_hex};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_server::CompactTxStreamer as _,
};
use zinder_query::{
    AdmittedIngestControl, DEFAULT_TREE_STATE_REPLAY_WAIT,
    DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT, RocksDbCanonicalReader, WalletServingReadiness,
    spawn_wallet_ingest_control_readiness_probe,
};
use zinder_runtime::{Readiness, ReadinessState};
use zinder_source::{NodeAuth, ZebraJsonRpcSource};
use zinder_testkit::{
    ChainFixture, IngestControlFixture, JsonRpcStub, JsonRpcTestServer, RpcReply,
    WalletServingStoreFixture, method,
};

mod address_validation;
mod canonical;

const ADDRESS: &str = "tmDpFafuBHKGUYmuwLsrxWJrwcnSyzEEtYx";

struct Fixture {
    adapter: LightwalletdGrpcAdapter<QualificationWallet>,
    node: JsonRpcTestServer,
    slot: CanonicalReaderSlot,
    _store: WalletServingStoreFixture,
}

impl Fixture {
    fn new(replies: impl IntoIterator<Item = JsonRpcStub>) -> Result<Self> {
        Self::with_node_timeout(replies, Duration::from_secs(2))
    }

    fn with_node_timeout(
        replies: impl IntoIterator<Item = JsonRpcStub>,
        timeout: Duration,
    ) -> Result<Self> {
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
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
        let node = verifiable_node(schedule, tip.hash, replies)?;
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
    let starting_hash = chain
        .blocks()
        .last()
        .ok_or_else(|| eyre!("missing history tip"))?
        .hash;
    let replaced = Arc::new(AtomicBool::new(false));
    let fork = Arc::clone(&replaced);
    let block_hash = move || {
        if fork.load(Ordering::Acquire) {
            BlockHash::from_bytes([0xff; 32])
        } else {
            starting_hash
        }
    };
    let tip_hash = block_hash.clone();
    let fixture = Fixture::from_chain(
        &chain,
        &one_upgrade_per_height()?,
        [
            method("getbestblockheightandhash").reply_with(move |_| {
                RpcReply::result(json!({"height": 4, "hash": tip_hash().as_bytes()}))
            }),
            method("getblockhash").reply_with(move |_| {
                RpcReply::result(json!(encode_rpc_block_hash_hex(block_hash())))
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

fn verbose_reply(txid: &Value, height: u32, bytes: &str) -> RpcReply {
    RpcReply::result(
        json!({"hex": bytes, "txid": txid, "height": height, "blockhash": "00".repeat(32)}),
    )
}

#[tokio::test]
async fn history_aliases_share_admission_until_the_stream_drains_or_is_dropped() -> Result<()> {
    let (fixture, _) = history_fixture([
        method("getrawtransaction").reply_with(|params| verbose_reply(&params[0], 1, "01")),
        method("getaddressutxos").reply_with(|_| RpcReply::result(json!([]))),
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
        verbose_reply(&params[0], height, bytes)
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
        let reply = verbose_reply(&params[0], 1, "02");
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
        method("getrawtransaction").reply_with(|params| verbose_reply(&params[0], 4, "01"))
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
                    RpcReply::result(json!(["01".repeat(32), "02".repeat(32)]))
                } else {
                    RpcReply::result(json!([]))
                }
            }),
            method("getrawtransaction").reply_with(|params| verbose_reply(&params[0], 4, "01")),
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
        method("getrawtransaction").reply_with(|params| verbose_reply(&params[0], 1, "01"))
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
        method("getrawtransaction").reply_with(|params| verbose_reply(&params[0], 1, "01"))
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
        method("getaddressbalance").reply(RpcReply::error_with_code(
            -1,
            "transparent address and spender indexes are unavailable",
        )),
        method("getaddressbalance").reply(RpcReply::error_with_code(
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
    let fixture = Fixture::new([
        method("getaddressbalance").reply(RpcReply::result(json!({"balance":1,"received":1})))
    ])?;
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
    let tip = |hash| {
        method("getbestblockheightandhash").reply(RpcReply::result(
            json!({"height": 2, "hash": vec![hash; 32]}),
        ))
    };
    let fixture = Fixture::new([
        tip(0),
        tip(0),
        tip(0),
        tip(1),
        method("getaddressutxos").reply_with(move |params| {
            let requested = params[0]["maxEntries"]
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .unwrap_or(0);
            let start = params[0]["startHeight"].as_u64().unwrap_or(0);
            let rows: Vec<_> = [(1, "04"), (2, "03"), (2, "02"), (2, "01")]
                .into_iter()
                .filter(|(height, _)| *height >= start)
                .take(requested)
                .map(|(height, id)| {
                    json!({"address": ADDRESS, "txid": id.repeat(32),
                    "outputIndex":0, "script":script, "satoshis": 1, "height": height})
                })
                .collect();
            RpcReply::result(json!(rows))
        }),
    ])?;
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
    let fixture = Fixture::new([
        method("getbestblockheightandhash")
            .reply(RpcReply::result(json!({"height":1, "hash":vec![0;32]}))),
        method("getaddressutxos").reply_with(|params| {
            if params[0]["maxEntries"] == 100_000 {
                RpcReply::result(json!([]))
            } else {
                RpcReply::error_with_code(-1, "expected an explicit 100,000-row prefix")
            }
        }),
    ])?;
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
    Fixture::with_node_timeout(
        [
            method("getbestblockheightandhash")
                .reply_with(|_| RpcReply::result(json!({"height":6, "hash":vec![0;32]}))),
            method("getaddressutxos").reply_with(move |params| {
                let (start, limit) = match node {
                    UtxoNode::Zebra => (
                        params[0]["startHeight"].as_u64().unwrap_or(0),
                        params[0]["maxEntries"].as_u64().unwrap_or(0),
                    ),
                    UtxoNode::Zakura => (0, 0),
                };
                let matching: u32 = groups.iter().filter(|(height, _, _)| *height >= start)
                    .map(|(_, _, count)| count).sum();
                if matches!(node, UtxoNode::Zebra) && limit == 0 && matching > 100_000 {
                    return RpcReply::error("more than 100000 unspent outputs match; page with startHeight and maxEntries");
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
                            return RpcReply::result(Value::Array(rows));
                        }
                    }
                }
                RpcReply::result(Value::Array(rows))
            }),
        ],
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
async fn cancelling_a_pending_unary_utxo_read_frees_admission() -> Result<()> {
    let fixture = Fixture::new([
        method("getaddressutxos").reply(RpcReply::delayed(
            Duration::from_millis(200),
            RpcReply::result(json!([])),
        )),
        method("getaddressutxos").reply(RpcReply::result(json!([]))),
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
