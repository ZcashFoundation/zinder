//! Forwarded RPC behavior and storage availability under a node outage.

use std::{sync::Arc, time::Duration};

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
use zinder_core::{BlockHeight, Network};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_server::CompactTxStreamer as _,
};
use zinder_query::{
    AdmittedIngestControl, DEFAULT_TREE_STATE_REPLAY_WAIT,
    DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT, RocksDbCanonicalReader,
    spawn_qualification_dependency_probe,
};
use zinder_runtime::{ComposedReadiness, Readiness, ReadinessInput, ReadinessState};
use zinder_source::{NodeAuth, ZebraJsonRpcSource};
use zinder_testkit::{
    ChainFixture, IngestControlFixture, JsonRpcStub, JsonRpcTestServer, RpcReply,
    WalletServingStoreFixture, method,
};

const ADDRESS: &str = "tmDpFafuBHKGUYmuwLsrxWJrwcnSyzEEtYx";

struct Fixture {
    adapter: LightwalletdGrpcAdapter<QualificationWallet>,
    node: JsonRpcTestServer,
    source: ZebraJsonRpcSource,
    _store: WalletServingStoreFixture,
}

impl Fixture {
    fn new(replies: impl IntoIterator<Item = JsonRpcStub>) -> Result<Self> {
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
        let tip = chain
            .block_at(BlockHeight::new(1))
            .ok_or_else(|| eyre!("missing fixture tip"))?;
        let schedule = one_upgrade_per_height()?;
        let mut store = WalletServingStoreFixture::from_chain(&chain, &schedule)?;
        let (canonical, _) = store.take_readers()?;
        let node = verifiable_node(&schedule, tip.hash, replies)?;
        let source = ZebraJsonRpcSource::new(
            Network::ZcashRegtest,
            node.url(),
            NodeAuth::None,
            Duration::from_secs(2),
        )?;
        let adapter = LightwalletdGrpcAdapter::for_light_wallet_qualification(
            CanonicalReaderSlot::new(Arc::new(RocksDbCanonicalReader::new(canonical)?)),
            source.clone(),
            DEFAULT_TREE_STATE_REPLAY_WAIT,
            DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT,
        );
        Ok(Self {
            adapter,
            node,
            source,
            _store: store,
        })
    }
}

fn balance_request() -> Request<lightwalletd::AddressList> {
    Request::new(lightwalletd::AddressList {
        addresses: vec![ADDRESS.to_owned()],
    })
}

#[tokio::test]
async fn cancelled_transparent_history_stops_raw_transaction_calls() -> Result<()> {
    let fixture = Fixture::new([
        method("getbestblockheightandhash")
            .reply_with(|_| RpcReply::result(json!({"height": 1, "hash": vec![0; 32]}))),
        method("getaddresstxids").reply(RpcReply::result(json!([
            "01".repeat(32),
            "02".repeat(32),
            "03".repeat(32)
        ]))),
        method("getrawtransaction").reply_with(|params| {
            if params[0] == "02".repeat(32) {
                RpcReply::Delay(
                    Duration::from_millis(80),
                    Box::new(RpcReply::result(json!("02"))),
                )
            } else if params[1] == 0 {
                RpcReply::result(json!("01"))
            } else {
                RpcReply::result(json!({"height": 1, "blockhash": "00".repeat(32)}))
            }
        }),
    ])?;
    let range = lightwalletd::BlockRange {
        start: Some(lightwalletd::BlockId {
            height: 1,
            hash: vec![],
        }),
        ..Default::default()
    };
    let mut history = fixture
        .adapter
        .get_taddress_transactions(Request::new(lightwalletd::TransparentAddressBlockFilter {
            address: ADDRESS.to_owned(),
            range: Some(range),
        }))
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
    assert_eq!(fixture.node.requests_for("getrawtransaction")?.len(), 2);
    tokio::select! {
        _ = history.next() => return Err(eyre!("delayed transaction arrived before cancellation")),
        outcome = tokio::time::timeout(Duration::from_secs(2), async {
            while fixture.node.requests_for("getrawtransaction")?.len() < 3 { tokio::task::yield_now().await; }
            Ok::<(),eyre::Report>(())
        }) => outcome??,
    }
    drop(history);
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(fixture.node.requests_for("getrawtransaction")?.len(), 3);
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
    let readiness = Readiness::default();
    let composed = ComposedReadiness::with_dependency_inputs(
        readiness.clone(),
        &[ReadinessInput::NodeSource, ReadinessInput::IngestControl],
    );
    composed.publish_admission(ReadinessState::ready(Some(1)));
    let control = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    let ingest =
        AdmittedIngestControl::connect(control.endpoint(), None, Network::ZcashRegtest).await?;
    let (address, _ops) = super::health::spawn_readyz(&readiness).await?;
    let cancel = CancellationToken::new();
    let interval = Duration::from_millis(20);
    let task = spawn_qualification_dependency_probe(
        fixture.source,
        ingest,
        composed,
        (interval, interval),
        cancel.clone(),
    );
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
    control.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn utxo_prefix_includes_the_lowest_txid_at_its_last_height() -> Result<()> {
    let script = hex::encode(
        zinder_source::parse_transparent_address(ADDRESS)?
            .script()
            .as_raw_bytes(),
    );
    let fixture = Fixture::new([method("getaddressutxos").reply_with(move |params| {
        let rows = ["03", "02", "01"].map(|id| {
            json!({"address": ADDRESS, "txid": id.repeat(32),
                "outputIndex":0, "script":script, "satoshis": 1, "height":1})
        });
        let requested = params[0]["maxEntries"]
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(0);
        RpcReply::result(json!(&rows[..requested.min(rows.len())]))
    })])?;
    let outputs = fixture
        .adapter
        .get_address_utxos(Request::new(lightwalletd::GetAddressUtxosArg {
            addresses: vec![ADDRESS.to_owned()],
            start_height: 1,
            max_entries: 1,
        }))
        .await?
        .into_inner()
        .address_utxos;
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].txid, vec![1; 32]);
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
