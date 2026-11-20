//! `GetStandardFee` statuses for the answers a node's `getstandardfee` RPC
//! can give, read through a verified JSON-RPC node source.

use std::{sync::Arc, time::Duration};

use eyre::eyre;
use serde_json::{Value, json};
use tonic::{Code, Request};
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_core::{
    BlockHash, BlockHeight, Network, NetworkUpgradeActivation, NetworkUpgradeActivations,
    wire::{encode_branch_id_hex, encode_rpc_block_hash_hex},
};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_server::CompactTxStreamer as _,
};
use zinder_query::{
    OrderedBroadcaster, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_source::{NodeAuth, ZebraJsonRpcSource};
use zinder_testkit::{
    ChainFixture, JsonRpcTestServer, RpcReply, WalletServingStoreFixture, method,
    sample_regtest_upgrade_activations,
};

#[tokio::test]
async fn get_standard_fee_returns_the_fee_a_verified_node_reports() -> eyre::Result<()> {
    let fixture = NodeFeeFixture::answering(json!({"standard_fee": 1_000, "version": 0})).await?;

    let fee = fixture
        .adapter
        .get_standard_fee(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();

    assert_eq!((fee.fee_zat, fee.estimator_version), (1_000, 0));
    Ok(())
}

/// A zero fee, or a reply that is not a fee and estimator version, is a node
/// bug rather than an outage, so it is `INTERNAL`, as Go lightwalletd returns.
#[tokio::test]
async fn get_standard_fee_is_internal_for_an_unusable_node_reply() -> eyre::Result<()> {
    for (case, reply) in [
        ("zero fee", json!({"standard_fee": 0, "version": 0})),
        ("string instead of a fee object", json!("not json")),
        ("null", Value::Null),
        (
            "fee as a string",
            json!({"standard_fee": "1000", "version": 0}),
        ),
        (
            "fractional fee",
            json!({"standard_fee": 1000.5, "version": 0}),
        ),
    ] {
        let fixture = NodeFeeFixture::answering(reply).await?;

        let status = fixture
            .adapter
            .get_standard_fee(Request::new(lightwalletd::Empty {}))
            .await
            .err()
            .ok_or_else(|| eyre!("{case}: an unusable reply was served"))?;

        assert_eq!(status.code(), Code::Internal, "{case}: {status:?}");
    }
    Ok(())
}

type NodeFeeAdapter =
    LightwalletdGrpcAdapter<WalletServingQuery<Option<OrderedBroadcaster<ZebraJsonRpcSource>>>>;

/// A compatibility adapter whose one node passed first-contact verification
/// and then answers `getstandardfee` once.
struct NodeFeeFixture {
    adapter: NodeFeeAdapter,
    _node: JsonRpcTestServer,
    _store_fixture: WalletServingStoreFixture,
}

impl NodeFeeFixture {
    async fn answering(fee_reply: Value) -> eyre::Result<Self> {
        let schedule = one_upgrade_per_height()?;
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
        let tip_hash = chain
            .block_at(BlockHeight::new(1))
            .ok_or_else(|| eyre!("fixture must contain height 1"))?
            .hash;
        let mut store_fixture = WalletServingStoreFixture::from_chain(&chain, &schedule)?;
        let (canonical, wallet) = store_fixture.take_readers()?;
        let slot = WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
            Arc::new(canonical),
            Arc::new(wallet),
        )?));
        let node = verifiable_node(&schedule, tip_hash, fee_reply)?;
        let nodes = OrderedBroadcaster::new(
            ZebraJsonRpcSource::new(
                Network::ZcashRegtest,
                node.url(),
                NodeAuth::None,
                Duration::from_secs(5),
            )?,
            [],
        );
        nodes.verify(&slot).await?;
        let query =
            WalletServingQuery::from_serving_pair_slot(slot).with_node_broadcast(Some(nodes));
        Ok(Self {
            adapter: LightwalletdGrpcAdapter::new(query),
            _node: node,
            _store_fixture: store_fixture,
        })
    }
}

/// The sample regtest upgrades with one upgrade per height, so a node table
/// compares equal whatever order its `upgrades` object lists them in.
fn one_upgrade_per_height() -> eyre::Result<NetworkUpgradeActivations> {
    Ok(NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        sample_regtest_upgrade_activations()
            .activations()
            .iter()
            .zip(1..)
            .map(|(activation, height)| NetworkUpgradeActivation {
                activation_height: BlockHeight::new(height),
                ..activation.clone()
            })
            .collect(),
    )?)
}

/// Starts a node double that passes first-contact verification once for a
/// pair serving `schedule` with `tip_hash` at its tip, then answers
/// `getstandardfee` once with `fee_reply`.
fn verifiable_node(
    schedule: &NetworkUpgradeActivations,
    tip_hash: BlockHash,
    fee_reply: Value,
) -> eyre::Result<JsonRpcTestServer> {
    let upgrades: serde_json::Map<String, Value> = schedule
        .activations()
        .iter()
        .map(|activation| {
            (
                encode_branch_id_hex(activation.branch_id),
                json!({
                    "name": activation.name,
                    "activationheight": activation.activation_height.value(),
                    "status": "active",
                }),
            )
        })
        .collect();
    JsonRpcTestServer::start([
        method("rpc.discover").reply(RpcReply::result(json!({
            "openrpc": "1.3.2",
            "methods": [
                {"name": "rpc.discover"},
                {"name": "sendrawtransaction"},
                {"name": "getstandardfee"},
            ],
        }))),
        method("getblockchaininfo").reply(RpcReply::result(json!({ "upgrades": upgrades }))),
        method("getblockhash").reply(RpcReply::result(json!(encode_rpc_block_hash_hex(tip_hash)))),
        method("getstandardfee").reply(RpcReply::result(fee_reply)),
    ])
}
