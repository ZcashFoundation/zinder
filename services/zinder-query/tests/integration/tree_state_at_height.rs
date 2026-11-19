#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

//! `TreeStateAtHeight` answers every retained height from canonical storage.
//!
//! Each store below holds construction checkpoints only every 100 blocks, so
//! most heights are reconstructed from the nearest checkpoint and the stored
//! compact blocks. The expected trees come from appending every block's note
//! commitments from genesis, independent of any stored checkpoint.
//!
//! The tests probe the heights beside each checkpoint, pool activation, and
//! replaced range: the first height above a seed, the longest replay below a
//! checkpoint, and replays that cross an activation. Heights between them take
//! the same replay path, so probing them would only add cost: each probe
//! derives every pool's root, which takes milliseconds in a debug build.
//!
//! A height outside retained history is not found, while a retained height
//! whose seed checkpoint is missing is store damage.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use eyre::eyre;
use serde_json::{Map, Value, json};
use tonic::{Code, Request};
use tonic_types::StatusExt as _;
use zinder_core::{
    BlockHeight, BlockId, CommitmentTreeCheckpoint, Network, NetworkUpgradeActivation,
    NetworkUpgradeActivations, ShieldedProtocol,
    wire::{encode_height_key_ascending, encode_rpc_block_hash_hex},
};
use zinder_proto::capabilities::WALLET_READ_TREE_STATE_AT_HEIGHT_V2;
use zinder_proto::v1::wallet::{self, wallet_query_server::WalletQuery as WalletQueryService};
use zinder_query::{
    AdmittedIngestControl, OrderedBroadcaster, WalletEndpointMetadata, WalletQueryGrpcAdapter,
    WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_source::{NodeCapabilities, NodeCapability, NodeSource, SourceBlock, SourceError};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, IngestControlFixture,
    WalletServingStoreFixture, commitment_tree_checkpoints, sample_regtest_upgrade_activations,
    with_note_commitments,
};

/// Orchard activates inside the first construction gap and Ironwood inside
/// the second, so replays cross both activation boundaries.
const ORCHARD_ACTIVATION_HEIGHT: u32 = 37;
const IRONWOOD_ACTIVATION_HEIGHT: u32 = 150;

#[tokio::test]
async fn tree_state_around_checkpoints_and_activations_matches_the_trees_its_compact_blocks_build()
-> eyre::Result<()> {
    let activations = gap_activations()?;
    let chain = with_note_commitments(
        &ChainFixture::new(Network::ZcashRegtest).extend_blocks(230),
        &activations,
    )?;
    let probed_heights = [
        1, 2, 36, 37, 38, 99, 100, 101, 149, 150, 151, 199, 200, 201, 229, 230,
    ]
    .map(BlockHeight::new);
    let expected = commitment_tree_checkpoints(&chain, &activations, &probed_heights)?;
    let writer = CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?;
    let served = NodelessQuery::over(writer.into_serving_store_fixture()?).await?;

    assert!(
        served
            .advertised_capabilities()
            .await?
            .iter()
            .any(|capability| capability == WALLET_READ_TREE_STATE_AT_HEIGHT_V2),
        "tree state at height must be advertised without a node"
    );
    for checkpoint in &expected {
        served.assert_tree_state(checkpoint).await?;
    }
    let status = served
        .tree_state_status(BlockHeight::new(231))
        .await?
        .ok_or_else(|| eyre!("a height above the visible tip must be refused"))?;
    assert_eq!(status.code(), Code::NotFound);
    assert_eq!(reason(&status), "BLOCK_NOT_IN_BEST_CHAIN");
    assert_eq!(
        served.node_calls(),
        0,
        "tree state must never contact the node"
    );
    served.shutdown().await
}

#[tokio::test]
async fn tree_state_at_a_checkpointed_store_replays_from_its_history_floor() -> eyre::Result<()> {
    let activations = gap_activations()?;
    let floor_height = BlockHeight::new(130);
    let chain = with_note_commitments(
        &ChainFixture::new(Network::ZcashRegtest).extend_blocks(260),
        &activations,
    )?;
    let probed_heights =
        [130, 131, 132, 149, 150, 151, 199, 200, 201, 259, 260].map(BlockHeight::new);
    let expected = commitment_tree_checkpoints(&chain, &activations, &probed_heights)?;
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_history_floor(floor_height),
        &activations,
    )?;
    let served = NodelessQuery::over(writer.into_serving_store_fixture()?).await?;

    for below_floor in [1, 100, floor_height.value() - 1] {
        let status = served
            .tree_state_status(BlockHeight::new(below_floor))
            .await?
            .ok_or_else(|| eyre!("height {below_floor} below the history floor was served"))?;
        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(reason(&status), "BLOCK_NOT_IN_BEST_CHAIN");
    }
    for checkpoint in &expected {
        served.assert_tree_state(checkpoint).await?;
    }
    assert_eq!(served.node_calls(), 0);
    served.shutdown().await
}

#[tokio::test]
async fn tree_state_after_a_live_replacement_serves_the_replacement_chain() -> eyre::Result<()> {
    let activations = gap_activations()?;
    let first_replaced_height = BlockHeight::new(131);
    let base = ChainFixture::new(Network::ZcashRegtest).extend_blocks(150);
    let original = with_note_commitments(&base, &activations)?;
    let replacement = with_note_commitments(
        &base.fork_at(first_replaced_height)?.extend_blocks(25),
        &activations,
    )?;
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&original).with_settled_tip(BlockHeight::new(120)),
        &activations,
    )?;
    writer.replace_live_blocks(&replacement, first_replaced_height)?;
    let probed_heights = [
        1, 99, 100, 101, 119, 120, 121, 130, 131, 132, 149, 150, 151, 154, 155,
    ]
    .map(BlockHeight::new);
    let expected = commitment_tree_checkpoints(&replacement, &activations, &probed_heights)?;
    let replaced_heights = [131, 132, 149, 150].map(BlockHeight::new);
    let displaced = commitment_tree_checkpoints(&original, &activations, &replaced_heights)?;
    let replacing = commitment_tree_checkpoints(&replacement, &activations, &replaced_heights)?;
    for (displaced, replacing) in displaced.iter().zip(&replacing) {
        assert_ne!(
            displaced.frontiers,
            replacing.frontiers,
            "the replacement chain must change the trees at height {}",
            displaced.block_id.height.value()
        );
    }
    let served = NodelessQuery::over(writer.into_serving_store_fixture()?).await?;

    for checkpoint in &expected {
        served.assert_tree_state(checkpoint).await?;
    }
    assert_eq!(served.node_calls(), 0);
    served.shutdown().await
}

#[tokio::test]
async fn tree_state_without_its_history_predecessor_checkpoint_reports_damaged_storage()
-> eyre::Result<()> {
    let activations = gap_activations()?;
    let chain = with_note_commitments(
        &ChainFixture::new(Network::ZcashRegtest).extend_blocks(3),
        &activations,
    )?;
    let tip_height = BlockHeight::new(3);
    let expected_tip = commitment_tree_checkpoints(&chain, &activations, &[tip_height])?;
    let writer = CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?;
    let canonical_primary_path = writer.canonical_primary_path().to_owned();
    let mut store_fixture = writer.into_serving_store_fixture()?;
    delete_tree_state_checkpoint(&canonical_primary_path, BlockHeight::new(0))?;
    let (mut canonical, wallet) = store_fixture.take_readers()?;
    canonical.try_catch_up()?;
    let slot = WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
        Arc::new(canonical),
        Arc::new(wallet),
    )?));
    let served = NodelessQuery::over_slot(slot, store_fixture).await?;

    for seedless in [1, 2] {
        let status = served
            .tree_state_status(BlockHeight::new(seedless))
            .await?
            .ok_or_else(|| eyre!("height {seedless} was served without a seed checkpoint"))?;
        assert_eq!(status.code(), Code::Unavailable, "height {seedless}");
        assert_eq!(reason(&status), "STORAGE_UNAVAILABLE", "height {seedless}");
    }
    for checkpoint in &expected_tip {
        served.assert_tree_state(checkpoint).await?;
    }
    let above_tip = served
        .tree_state_status(BlockHeight::new(4))
        .await?
        .ok_or_else(|| eyre!("a height above the visible tip must be refused"))?;
    assert_eq!(above_tip.code(), Code::NotFound);
    assert_eq!(reason(&above_tip), "BLOCK_NOT_IN_BEST_CHAIN");
    served.shutdown().await
}

/// Regtest activations with Orchard and Ironwood activating between
/// construction checkpoints.
fn gap_activations() -> eyre::Result<NetworkUpgradeActivations> {
    let sample = sample_regtest_upgrade_activations();
    let activations = sample
        .activations()
        .iter()
        .map(|activation| {
            let activation_height = match activation.name.as_str() {
                "NU5" | "NU6" | "NU6.1" | "NU6.2" => BlockHeight::new(ORCHARD_ACTIVATION_HEIGHT),
                "NU6.3" => BlockHeight::new(IRONWOOD_ACTIVATION_HEIGHT),
                _ => activation.activation_height,
            };
            NetworkUpgradeActivation {
                activation_height,
                ..activation.clone()
            }
        })
        .collect();
    Ok(NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        activations,
    )?)
}

/// A native query whose node advertises nothing it can serve and counts
/// every call it receives.
struct NodelessQuery {
    adapter: WalletQueryGrpcAdapter<
        WalletServingQuery<Option<OrderedBroadcaster<UnreachableNode>>, AdmittedIngestControl>,
    >,
    node: UnreachableNode,
    _store_fixture: WalletServingStoreFixture,
    ingest_control_fixture: IngestControlFixture,
}

impl NodelessQuery {
    async fn over(mut store_fixture: WalletServingStoreFixture) -> eyre::Result<Self> {
        let slot = crate::common::serving_pair_slot(&mut store_fixture)?;
        Self::over_slot(slot, store_fixture).await
    }

    /// Serves `slot`, whose readers `store_fixture` keeps the paths of.
    async fn over_slot(
        slot: WalletServingPairSlot,
        store_fixture: WalletServingStoreFixture,
    ) -> eyre::Result<Self> {
        let node = UnreachableNode::new()?;
        let (ingest_control, ingest_control_fixture) =
            crate::common::admitted_ingest_control_fixture().await?;
        let query = WalletServingQuery::from_admitted_native_serving_pair(slot, ingest_control)
            .with_node_broadcast(Some(OrderedBroadcaster::new(node.clone(), [])));
        Ok(Self {
            adapter: WalletQueryGrpcAdapter::new(query, WalletEndpointMetadata::default()),
            node,
            _store_fixture: store_fixture,
            ingest_control_fixture,
        })
    }

    async fn advertised_capabilities(&self) -> eyre::Result<Vec<String>> {
        Ok(WalletQueryService::server_info(
            &self.adapter,
            Request::new(wallet::ServerInfoRequest {}),
        )
        .await?
        .into_inner()
        .info
        .and_then(|server_info| server_info.common)
        .ok_or_else(|| eyre!("ServerInfo omitted common metadata"))?
        .capabilities)
    }

    async fn assert_tree_state(&self, expected: &CommitmentTreeCheckpoint) -> eyre::Result<()> {
        let height = expected.block_id.height;
        let response = WalletQueryService::tree_state_at_height(
            &self.adapter,
            Request::new(wallet::TreeStateAtHeightRequest {
                height: height.value(),
                at_epoch_id: None,
            }),
        )
        .await
        .map_err(|status| eyre!("height {} was refused: {status:?}", height.value()))?
        .into_inner();
        let payload: Value = serde_json::from_slice(&response.payload_bytes)?;
        assert_eq!(response.height, height.value());
        assert_eq!(
            response.block_hash,
            encode_rpc_block_hash_hex(expected.block_id.hash),
            "block hash at height {}",
            height.value()
        );
        assert_eq!(
            response.block_time_seconds,
            Some(expected.block_time_seconds),
            "block time at height {}",
            height.value()
        );
        assert_eq!(
            payload,
            expected_payload(expected),
            "tree state at height {}",
            height.value()
        );
        Ok(())
    }

    async fn tree_state_status(&self, height: BlockHeight) -> eyre::Result<Option<tonic::Status>> {
        Ok(WalletQueryService::tree_state_at_height(
            &self.adapter,
            Request::new(wallet::TreeStateAtHeightRequest {
                height: height.value(),
                at_epoch_id: None,
            }),
        )
        .await
        .err())
    }

    fn node_calls(&self) -> usize {
        self.node.calls.load(Ordering::SeqCst)
    }

    async fn shutdown(self) -> eyre::Result<()> {
        self.ingest_control_fixture.shutdown().await?;
        Ok(())
    }
}

fn expected_payload(checkpoint: &CommitmentTreeCheckpoint) -> Value {
    let mut payload = Map::new();
    for protocol in [
        ShieldedProtocol::Sapling,
        ShieldedProtocol::Orchard,
        ShieldedProtocol::Ironwood,
    ] {
        if let Some(frontier) = checkpoint.frontiers.get(protocol) {
            payload.insert(
                protocol.rpc_pool_name().to_owned(),
                json!({
                    "commitments": {
                        "finalState": hex::encode(frontier.final_state_bytes())
                    }
                }),
            );
        }
    }
    Value::Object(payload)
}

/// Deletes the stored tree-state checkpoint at `height` from a closed
/// canonical primary, as store damage would.
fn delete_tree_state_checkpoint(
    canonical_primary_path: &std::path::Path,
    height: BlockHeight,
) -> eyre::Result<()> {
    let options = rust_rocksdb::Options::default();
    let families = rust_rocksdb::DB::list_cf(&options, canonical_primary_path)?;
    let db = rust_rocksdb::DB::open_cf(&options, canonical_primary_path, &families)?;
    let family = db
        .cf_handle("tree_state_checkpoint")
        .ok_or_else(|| eyre!("the canonical primary has no tree-state checkpoint family"))?;
    db.delete_cf(&family, encode_height_key_ascending(height))?;
    db.flush_cf(&family)?;
    Ok(())
}

fn reason(status: &tonic::Status) -> String {
    status
        .get_details_error_info()
        .map_or_default(|error_info| error_info.reason)
}

/// Node double that admits discovery only and fails every call.
#[derive(Clone)]
struct UnreachableNode {
    capabilities: NodeCapabilities,
    calls: Arc<AtomicUsize>,
}

impl UnreachableNode {
    fn new() -> eyre::Result<Self> {
        Ok(Self {
            capabilities: NodeCapabilities::new([NodeCapability::OpenRpcDiscovery])?,
            calls: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn unreachable(&self) -> SourceError {
        self.calls.fetch_add(1, Ordering::SeqCst);
        SourceError::NodeUnavailable {
            reason: "tree-state test node is unreachable".to_owned(),
        }
    }
}

#[async_trait]
impl NodeSource for UnreachableNode {
    fn capabilities(&self) -> NodeCapabilities {
        self.capabilities
    }

    async fn fetch_block_at(&self, _height: BlockHeight) -> Result<SourceBlock, SourceError> {
        Err(self.unreachable())
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        Err(self.unreachable())
    }
}

#[async_trait]
impl zinder_source::TransactionBroadcaster for UnreachableNode {
    async fn broadcast_transaction(
        &self,
        _raw_transaction: zinder_core::RawTransactionBytes,
    ) -> Result<zinder_core::TransactionBroadcastOutcome, SourceError> {
        Err(self.unreachable())
    }
}
