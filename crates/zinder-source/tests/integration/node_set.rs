#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use serde_json::json;
use tokio_stream::StreamExt as _;
use zinder_core::{BlockHash, BlockHeight, BlockId, Network};
use zinder_source::{
    ACTIVE_NODE_ENDPOINT_CHANGED_REASON, DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES, MempoolSource,
    MempoolSourceAdmissionLimits, MempoolSourceEvent, MempoolSourceFactory, NodeAuth,
    NodeCapabilities, NodeSet, NodeSource, NodeTarget, SourceBlock, SourceError,
};
use zinder_testkit::{
    JsonRpcTestServer, LogCapture, MockMempoolSource, MockMempoolSourceControl, RpcReply, method,
};

/// One endpoint of a test node set: it reports a fixed tip and owns a
/// scripted mempool source.
#[derive(Clone)]
struct FixedTipEndpoint {
    tip: BlockId,
    mempool: MockMempoolSource,
}

impl FixedTipEndpoint {
    fn at_height(height: u32) -> (Self, MockMempoolSourceControl) {
        let (mempool, control) = MockMempoolSource::streaming();
        let tag = u8::try_from(height).unwrap_or(u8::MAX);
        (
            Self {
                tip: BlockId::new(BlockHeight::new(height), BlockHash::from_bytes([tag; 32])),
                mempool,
            },
            control,
        )
    }
}

#[async_trait]
impl NodeSource for FixedTipEndpoint {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        Err(SourceError::BlockUnavailable {
            height,
            reason: "fixed-tip endpoint serves no blocks".to_owned(),
        })
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        Ok(self.tip)
    }
}

impl MempoolSourceFactory for FixedTipEndpoint {
    fn mempool_source(
        &self,
        _admission_limits: MempoolSourceAdmissionLimits,
    ) -> Arc<dyn MempoolSource> {
        Arc::new(self.mempool.clone())
    }
}

#[tokio::test]
async fn calls_reach_the_active_endpoint_and_follow_a_switch() -> eyre::Result<()> {
    let (primary, _) = FixedTipEndpoint::at_height(10);
    let (fallback, _) = FixedTipEndpoint::at_height(20);
    let (nodes, switch) = NodeSet::new(primary.clone(), [fallback.clone()]);

    assert_eq!(nodes.endpoint_count(), 2);
    assert!(nodes.has_fallbacks());
    assert_eq!(nodes.active_position(), 0);
    assert_eq!(nodes.tip_id().await?, primary.tip);

    assert!(switch.switch_to(1));
    assert_eq!(nodes.active_position(), 1);
    assert_eq!(nodes.clone().tip_id().await?, fallback.tip);
    Ok(())
}

#[tokio::test]
async fn switch_refuses_unknown_positions_and_the_active_one() {
    let (primary, _) = FixedTipEndpoint::at_height(10);
    let (fallback, _) = FixedTipEndpoint::at_height(20);
    let (nodes, switch) = NodeSet::new(primary, [fallback]);
    let mut switches = nodes.subscribe_switches();

    assert!(!switch.switch_to(0));
    assert!(!switch.switch_to(2));
    assert!(!switches.has_changed().unwrap_or(true));

    assert!(switch.switch_to(1));
    assert!(switches.has_changed().unwrap_or(false));
    assert_eq!(*switches.borrow_and_update(), 1);
}

#[tokio::test]
async fn a_switch_ends_the_open_mempool_stream_and_reopens_on_the_new_endpoint() -> eyre::Result<()>
{
    let (primary, primary_control) = FixedTipEndpoint::at_height(10);
    let (fallback, fallback_control) = FixedTipEndpoint::at_height(20);
    let primary_tip = primary.tip;
    let fallback_tip = fallback.tip;
    let (nodes, switch) = NodeSet::new(primary, [fallback]);
    let mempool = nodes.mempool_source(MempoolSourceAdmissionLimits::default());

    let mut events = mempool.events().await?;
    primary_control.complete_initial_snapshot(primary_tip)?;
    let first = tokio::time::timeout(Duration::from_secs(5), events.next()).await?;
    assert!(matches!(
        first,
        Some(Ok(MempoolSourceEvent::InitialSnapshotComplete { source_tip })) if source_tip == primary_tip
    ));

    assert!(switch.switch_to(1));
    let ended = tokio::time::timeout(Duration::from_secs(5), events.next()).await?;
    assert!(matches!(
        ended,
        Some(Err(SourceError::MempoolStreamUnavailable { ref reason }))
            if reason == ACTIVE_NODE_ENDPOINT_CHANGED_REASON
    ));
    assert!(
        tokio::time::timeout(Duration::from_secs(5), events.next())
            .await?
            .is_none()
    );

    let mut reopened = mempool.events().await?;
    assert_eq!(primary_control.open_count(), 1);
    assert_eq!(fallback_control.open_count(), 1);
    fallback_control.complete_initial_snapshot(fallback_tip)?;
    let from_fallback = tokio::time::timeout(Duration::from_secs(5), reopened.next()).await?;
    assert!(matches!(
        from_fallback,
        Some(Ok(MempoolSourceEvent::InitialSnapshotComplete { source_tip })) if source_tip == fallback_tip
    ));
    Ok(())
}

/// A fallback whose cookie file is missing does not fail building the set.
///
/// A node writes its cookie file when it starts. Until then that endpoint
/// fails as node-unavailable without being contacted, and the other
/// endpoints answer.
#[tokio::test]
async fn connect_builds_a_fallback_whose_cookie_file_is_missing() -> eyre::Result<()> {
    let upgrades = json!({ "upgrades": {} });
    let primary_server = JsonRpcTestServer::start([
        method("getblockchaininfo").reply(RpcReply::result(upgrades.clone()))
    ])?;
    let fallback_server =
        JsonRpcTestServer::start([method("getblockchaininfo").reply(RpcReply::result(upgrades))])?;
    let cookie_dir = tempfile::tempdir()?;
    let target = |server: &JsonRpcTestServer, node_auth| {
        NodeTarget::new(
            Network::ZcashRegtest,
            server.url(),
            node_auth,
            Duration::from_secs(5),
            DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES,
        )
    };

    let (nodes, switch) = NodeSet::connect(
        &target(&primary_server, NodeAuth::None),
        &[target(
            &fallback_server,
            NodeAuth::cookie_file(cookie_dir.path().join(".cookie")),
        )],
    )?;
    nodes.fetch_network_upgrade_activations().await?;
    assert!(switch.switch_to(1));
    let waiting = nodes.fetch_network_upgrade_activations().await.err();

    assert!(
        matches!(&waiting, Some(SourceError::NodeUnavailable { reason }) if reason.contains("cookie")),
        "unexpected fallback outcome before its cookie exists: {waiting:?}"
    );
    assert_eq!(primary_server.requests()?.len(), 1);
    assert!(fallback_server.requests()?.is_empty());
    Ok(())
}

/// Building the mempool sources names each endpoint by position, never by
/// address, because an address can embed credentials.
#[tokio::test]
async fn mempool_source_selection_logs_positions_not_addresses() -> eyre::Result<()> {
    let capture = LogCapture::install_for_target("zinder::source");
    let target = |json_rpc_addr: &str, indexer_grpc_addr: Option<&str>| {
        NodeTarget::new(
            Network::ZcashRegtest,
            json_rpc_addr.to_owned(),
            NodeAuth::None,
            Duration::from_secs(5),
            DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES,
        )
        .with_indexer_grpc_addr(indexer_grpc_addr.map(str::to_owned))
    };
    let (nodes, _switch) = NodeSet::connect(
        &target("http://127.0.0.1:1", None),
        &[target(
            "http://zebra:rpc-secret@127.0.0.1:2",
            Some("http://zebra:indexer-secret@127.0.0.1:3/?token=query-secret"),
        )],
    )?;

    let _mempool = nodes.mempool_source(MempoolSourceAdmissionLimits::default());

    let selected = capture
        .events()
        .into_iter()
        .filter(|event| event.field("event") == Some("mempool_source_selected"))
        .collect::<Vec<_>>();
    let described = selected
        .iter()
        .map(|event| (event.field("position"), event.field("backend")))
        .collect::<Vec<_>>();
    assert_eq!(
        described,
        [(Some("0"), Some("polling")), (Some("1"), Some("streaming"))]
    );
    for event in &selected {
        for value in event.fields.values() {
            assert!(
                !value.contains("secret") && !value.contains("127.0.0.1"),
                "a mempool source event logged an endpoint address: {event:?}"
            );
        }
    }
    Ok(())
}
