//! `GetStandardFee` statuses for the answers a node's `getstandardfee` RPC
//! can give, read through a verified JSON-RPC node source.

use std::{sync::Arc, time::Duration};

use crate::common::verified_node::{one_upgrade_per_height, verifiable_node};
use eyre::eyre;
use serde_json::{Value, json};
use tonic::{Code, Request};
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_core::{BlockHeight, Network};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_server::CompactTxStreamer as _,
};
use zinder_query::{
    OrderedBroadcaster, RocksDbCanonicalReader, RocksDbWalletReader, WalletServingPairSlot,
    WalletServingQuery, WalletServingReadPair,
};
use zinder_source::{NodeAuth, ZebraJsonRpcSource};
use zinder_testkit::{
    ChainFixture, JsonRpcTestServer, RpcReply, WalletServingStoreFixture, method,
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
        ("array of the fee and version", json!([7_000, 3])),
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
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?));
        let node = verifiable_node(
            &schedule,
            tip_hash,
            [method("getstandardfee").reply(RpcReply::result(fee_reply))],
        )?;
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
