#![allow(
    missing_docs,
    reason = "Integration test names describe the production control-plane contract under test."
)]

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde_json::Value;
use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use zinder_compat_lightwalletd::spawn_ingest_control_tip_change_publisher;
use zinder_core::{
    BlockHeight, BlockId, ChainEpochId, CommitmentTreeAccumulator, CommitmentTreeCheckpoint,
    CommitmentTreeFrontiers, Network, NetworkUpgradeActivations, SubtreeRootRange,
};
use zinder_ingest::{
    CanonicalConstructionConfig, CanonicalFollowConfig, CanonicalIngestControlGrpcAdapter,
    CanonicalWriterConfig, LiveMempoolOwner, canonical_control_channel,
    run_canonical_writer_with_control,
};
use zinder_proto::v1::ingest::{WriterStatusRequest, ingest_control_client::IngestControlClient};
use zinder_runtime::Readiness;
use zinder_source::{
    NodeCapabilities, NodeCapability, NodeSource, SourceBlock, SourceChainSegment,
    SourceChainSegmentLimits, SourceError, SourceSubtreeRoots,
};
use zinder_store::RocksDbResourceBudget;
use zinder_testkit::sample_regtest_upgrade_activations;

const SERIALIZED_HEADER_PARENT_OFFSET: usize = 4;
const SERIALIZED_HEADER_PARENT_END: usize = SERIALIZED_HEADER_PARENT_OFFSET + 32;
const COINBASE_HEIGHT_OPCODE_OFFSET: usize = 1_534;
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "The production publisher contract requires one continuous writer startup, retained-fence capture, post-wait advance, observation, and symmetric shutdown scenario."
)]
async fn production_tip_change_publisher_observes_a_post_wait_canonical_event() -> eyre::Result<()>
{
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let blocks = build_parseable_chain(10)?;
    let checkpoints = build_commitment_tree_checkpoints(&blocks, activations.as_ref())?;
    let source = MutableTipSource::new(blocks, checkpoints, BlockHeight::new(2));
    let temporary = tempfile::tempdir()?;
    let cancel = CancellationToken::new();
    let readiness = Readiness::default();
    let (canonical, commands) = canonical_control_channel();
    let writer = tokio::spawn(
        CanonicalWriterTask {
            source: source.clone(),
            activations: activations.clone(),
            storage_path: temporary.path().join("canonical"),
            readiness: readiness.clone(),
            cancel: cancel.clone(),
            commands,
        }
        .run(),
    );
    let writer_status = tokio::time::timeout(TEST_TIMEOUT, canonical.writer_status())
        .await
        .map_err(|_| eyre::eyre!("canonical writer did not begin serving control requests"))?;
    let initial_event_sequence = match writer_status {
        Ok(status) => {
            status
                .fence
                .ok_or_else(|| eyre::eyre!("writer status omitted its canonical fence"))?
                .event_sequence
        }
        Err(control_error) => {
            return match writer.await? {
                Ok(()) => Err(control_error.into()),
                Err(writer_error) => Err(writer_error.into()),
            };
        }
    };

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let adapter = CanonicalIngestControlGrpcAdapter::new(
        Network::ZcashRegtest,
        canonical.clone(),
        LiveMempoolOwner::default(),
        Arc::new(source.clone()),
        readiness,
    );
    let server_cancel = cancel.clone();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                server_cancel.cancelled_owned(),
            )
            .await
    });
    wait_for_ingest_control(&endpoint).await?;
    let (watcher, publisher) =
        spawn_ingest_control_tip_change_publisher(endpoint, None, cancel.clone());
    let (waiting_sender, waiting_receiver) = oneshot::channel();
    let mut waiting = tokio::spawn(async move {
        let _ = waiting_sender.send(());
        watcher
            .await_tip_change_after(ChainEpochId::new(initial_event_sequence))
            .await
    });
    waiting_receiver.await?;
    let mut observed_height = None;
    for height_value in 3..=10 {
        source.set_tip_height(BlockHeight::new(height_value));
        if let Ok(waiting_result) =
            tokio::time::timeout(Duration::from_millis(500), &mut waiting).await
        {
            waiting_result??;
            observed_height = Some(height_value);
            break;
        }
    }
    let observed_height = observed_height
        .ok_or_else(|| eyre::eyre!("tip-change watcher did not observe a canonical event"))?;
    let status = canonical.writer_status().await?;
    let writer_fence = status
        .fence
        .ok_or_else(|| eyre::eyre!("writer status omitted its canonical fence"))?;
    assert_eq!(writer_fence.visible_tip_height, observed_height);
    assert!(writer_fence.event_sequence >= 2);

    cancel.cancel();
    server.await??;
    publisher.await?;
    writer.await??;
    Ok(())
}

struct CanonicalWriterTask {
    source: MutableTipSource,
    activations: Arc<NetworkUpgradeActivations>,
    storage_path: std::path::PathBuf,
    readiness: Readiness,
    cancel: CancellationToken,
    commands: tokio::sync::mpsc::Receiver<zinder_ingest::CanonicalControlCommand>,
}

impl CanonicalWriterTask {
    async fn run(self) -> Result<(), zinder_ingest::CanonicalWriterError> {
        let config = CanonicalWriterConfig {
            storage_path: self.storage_path,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            construction: CanonicalConstructionConfig::for_local_tests(
                Duration::from_secs(5),
                self.activations.clone(),
            ),
            checkpoint_height: Some(BlockHeight::new(1)),
            raw_blob_retention: zinder_store::RawBlobRetention::Transactions,
            reorg_window_blocks: 100,
            follow: CanonicalFollowConfig {
                request_timeout: Duration::from_secs(5),
                poll_interval: Duration::from_millis(10),
                lag_threshold_blocks: 0,
                target_height: None,
                event_retention_window: None,
                event_retention_check_interval: Duration::from_secs(1),
                mempool_ready_gate: None,
                max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
            },
        };
        let store = run_canonical_writer_with_control(
            &self.source,
            self.activations,
            config,
            &self.readiness,
            &self.cancel,
            Some(self.commands),
        )
        .await?;
        drop(store);
        Ok(())
    }
}

async fn wait_for_ingest_control(endpoint: &str) -> eyre::Result<()> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if let Ok(mut client) = IngestControlClient::connect(endpoint.to_owned()).await
                && client.writer_status(WriterStatusRequest {}).await.is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| eyre::eyre!("ingest-control endpoint did not become ready"))?;
    Ok(())
}

#[derive(Clone)]
struct MutableTipSource {
    blocks: Arc<BTreeMap<BlockHeight, SourceBlock>>,
    checkpoints: Arc<BTreeMap<BlockHeight, CommitmentTreeCheckpoint>>,
    tip_height: Arc<std::sync::atomic::AtomicU32>,
}

impl MutableTipSource {
    fn new(
        blocks: BTreeMap<BlockHeight, SourceBlock>,
        checkpoints: BTreeMap<BlockHeight, CommitmentTreeCheckpoint>,
        tip_height: BlockHeight,
    ) -> Self {
        Self {
            blocks: Arc::new(blocks),
            checkpoints: Arc::new(checkpoints),
            tip_height: Arc::new(std::sync::atomic::AtomicU32::new(tip_height.value())),
        }
    }

    fn set_tip_height(&self, height: BlockHeight) {
        self.tip_height
            .store(height.value(), std::sync::atomic::Ordering::SeqCst);
    }

    fn block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.blocks
            .get(&height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "tip-change fixture has no block at that height".to_owned(),
            })
    }
}

#[async_trait]
impl NodeSource for MutableTipSource {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.block_at(height)
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        let Some(start_height) = limits.cursor.next_connected_height() else {
            return Ok(SourceChainSegment::default());
        };
        let tip_height =
            BlockHeight::new(self.tip_height.load(std::sync::atomic::Ordering::SeqCst));
        Ok(SourceChainSegment::connected_blocks(
            self.blocks
                .range(start_height..=tip_height)
                .take(usize::try_from(limits.max_connected_blocks.get()).unwrap_or(usize::MAX))
                .map(|(_, block)| block.clone()),
        ))
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        _network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.checkpoints
            .get(&height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "tip-change fixture has no checkpoint at that height".to_owned(),
            })
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        let height = BlockHeight::new(self.tip_height.load(std::sync::atomic::Ordering::SeqCst));
        let block = self.block_at(height)?;
        Ok(BlockId::new(height, block.hash))
    }

    async fn fetch_subtree_root_range(
        &self,
        _range: SubtreeRootRange,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        Err(SourceError::NodeCapabilityMissing {
            capability: NodeCapability::SubtreeRoots,
        })
    }
}

fn build_parseable_chain(block_count: u32) -> eyre::Result<BTreeMap<BlockHeight, SourceBlock>> {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../zinder-ingest/tests/fixtures/z3-regtest-block-1.json"
    ))?;
    let raw_block_hex = fixture
        .get("raw_block_hex")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre::eyre!("regtest fixture omitted raw_block_hex"))?;
    let template = hex::decode(raw_block_hex)?;
    let mut parent_hash = Network::ZcashRegtest.genesis_hash();
    let mut blocks = BTreeMap::new();
    for height_value in 1..=block_count {
        let height = BlockHeight::new(height_value);
        let mut raw_block_bytes = template.clone();
        raw_block_bytes[SERIALIZED_HEADER_PARENT_OFFSET..SERIALIZED_HEADER_PARENT_END]
            .copy_from_slice(&parent_hash.as_bytes());
        raw_block_bytes[COINBASE_HEIGHT_OPCODE_OFFSET] = 0x50_u8.saturating_add(
            u8::try_from(height_value)
                .map_err(|_| eyre::eyre!("fixture height must fit one opcode"))?,
        );
        let block =
            SourceBlock::from_raw_block_bytes(Network::ZcashRegtest, height, raw_block_bytes)?;
        parent_hash = block.hash;
        blocks.insert(height, block);
    }
    Ok(blocks)
}

fn build_commitment_tree_checkpoints(
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    activations: &NetworkUpgradeActivations,
) -> eyre::Result<BTreeMap<BlockHeight, CommitmentTreeCheckpoint>> {
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        BlockHeight::new(0),
        &CommitmentTreeFrontiers::default(),
        activations,
    )?;
    let mut checkpoints = BTreeMap::new();
    for (height, block) in blocks {
        accumulator.append_block_commitments(*height, &[], &[], &[])?;
        checkpoints.insert(
            *height,
            CommitmentTreeCheckpoint::new(
                BlockId::new(*height, block.hash),
                block.block_time_seconds,
                accumulator.validated_frontiers()?,
            ),
        );
    }
    Ok(checkpoints)
}
