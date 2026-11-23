#![allow(
    missing_docs,
    reason = "Integration test names describe the production control-plane contract under test."
)]

use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    pin::pin,
    sync::Arc,
    task::Poll,
    time::Duration,
};

use async_trait::async_trait;
use eyre::WrapErr;
use serde_json::Value;
use tokio::{net::TcpListener, time::Instant};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use zinder_compat_lightwalletd::spawn_ingest_control_tip_change_publisher;
use zinder_core::{
    BlockHeight, BlockId, ChainEpochId, CommitmentTreeAccumulator, CommitmentTreeCheckpoint,
    CommitmentTreeFrontiers, Network, NetworkUpgradeActivations, SubtreeRootRange,
};
use zinder_ingest::{
    CanonicalConstructionConfig, CanonicalControlHandle, CanonicalFollowConfig,
    CanonicalIngestControlGrpcAdapter, CanonicalWriterConfig, LiveMempoolOwner,
    canonical_control_channel, run_canonical_writer_with_control,
};
use zinder_proto::v1::ingest::{WriterStatusRequest, ingest_control_client::IngestControlClient};
use zinder_runtime::{ComposedReadiness, Readiness};
use zinder_source::{
    NodeCapabilities, NodeCapability, NodeSource, SourceBlock, SourceChainSegment,
    SourceChainSegmentLimits, SourceError, SourceSubtreeRoots,
};
use zinder_store::RocksDbResourceBudget;
use zinder_testkit::sample_regtest_upgrade_activations;

const SERIALIZED_HEADER_PARENT_OFFSET: usize = 4;
const SERIALIZED_HEADER_PARENT_END: usize = SERIALIZED_HEADER_PARENT_OFFSET + 32;
const COINBASE_HEIGHT_OPCODE_OFFSET: usize = 1_534;

/// Bounds the whole scenario below nextest's kill window.
///
/// `.config/nextest.toml` grants this test 120 s, so a hang reports the
/// condition it was waiting on instead of being killed without one. `cargo test`
/// has no kill window, so the same bound stops a hang there.
const SCENARIO_DEADLINE: Duration = Duration::from_secs(90);

type WriterHandle = tokio::task::JoinHandle<Result<(), zinder_ingest::CanonicalWriterError>>;

struct ScenarioDeadline(Instant);

impl ScenarioDeadline {
    fn start() -> Self {
        Self(Instant::now() + SCENARIO_DEADLINE)
    }

    async fn wait_for<T>(
        &self,
        outstanding_condition: &'static str,
        wait: impl Future<Output = T>,
    ) -> eyre::Result<T> {
        tokio::time::timeout_at(self.0, wait).await.map_err(|_| {
            eyre::eyre!(
                "the {SCENARIO_DEADLINE:?} scenario deadline passed while waiting for \
                 {outstanding_condition}"
            )
        })
    }
}

/// Ends `wait` early with an error when the canonical writer task stops
/// before the condition `wait` is for.
async fn until_writer_stops<T>(
    writer: &mut WriterHandle,
    outstanding_condition: &'static str,
    wait: impl Future<Output = T>,
) -> eyre::Result<T> {
    tokio::select! {
        outcome = wait => Ok(outcome),
        writer_outcome = &mut *writer => match writer_outcome? {
            Ok(()) => Err(eyre::eyre!(
                "canonical writer stopped before {outstanding_condition}"
            )),
            Err(writer_error) => Err(writer_error.into()),
        },
    }
}

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
    let writer_readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let (canonical, commands) = canonical_control_channel();
    let mut writer = tokio::spawn(
        CanonicalWriterTask {
            source: source.clone(),
            activations: activations.clone(),
            storage_path: temporary.path().join("canonical"),
            readiness: writer_readiness.clone(),
            cancel: cancel.clone(),
            commands,
        }
        .run(),
    );
    let deadline = ScenarioDeadline::start();
    let initial_event_sequence = deadline
        .wait_for(
            "the canonical writer to answer a startup status request",
            until_writer_stops(
                &mut writer,
                "answering a startup status request",
                startup_event_sequence(&canonical),
            ),
        )
        .await???;
    let baseline_predecessor = initial_event_sequence
        .checked_sub(1)
        .ok_or_else(|| eyre::eyre!("the writer started without a retained canonical event"))?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    // Nothing serves the endpoint yet, so the publisher's channel holds its
    // starting value of zero, and a watcher fenced at zero must stay pending.
    // No replay timing can decide this check, and the baseline wait below
    // relies on the strict comparison it proves.
    let (watcher, publisher) =
        spawn_ingest_control_tip_change_publisher(endpoint.clone(), None, cancel.clone());
    let unpublished_poll = poll_once(watcher.await_tip_change_after(ChainEpochId::new(0))).await;
    assert!(
        unpublished_poll.is_pending(),
        "the tip-change watcher resolved at its own fence before any event was published: \
         {unpublished_poll:?}"
    );

    let adapter = CanonicalIngestControlGrpcAdapter::new(
        Network::ZcashRegtest,
        canonical.clone(),
        LiveMempoolOwner::default(),
        Arc::new(source.clone()),
        writer_readiness,
    );
    let server_cancel = cancel.clone();
    let mut server = tokio::spawn(async move {
        Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                server_cancel.cancelled_owned(),
            )
            .await
    });
    deadline
        .wait_for(
            "the ingest-control endpoint to answer writer status requests",
            wait_for_ingest_control(&endpoint, &mut server),
        )
        .await??;
    // The publisher replays retained events asynchronously, so wait until it
    // has published the starting sequence. Only then does a pending first poll
    // prove the watcher rejects that sequence, and the event that resolves it
    // is one the advance below produces.
    deadline
        .wait_for(
            "the tip-change publisher to replay the retained baseline event",
            until_writer_stops(
                &mut writer,
                "the publisher replayed the retained baseline event",
                watcher.await_tip_change_after(ChainEpochId::new(baseline_predecessor)),
            ),
        )
        .await???;
    let mut waiting =
        pin!(watcher.await_tip_change_after(ChainEpochId::new(initial_event_sequence)));
    let baseline_poll = poll_once(waiting.as_mut()).await;
    assert!(
        baseline_poll.is_pending(),
        "the tip-change watcher resolved at the retained baseline before the source advanced: \
         {baseline_poll:?}"
    );
    let observed_height = 3;
    source.set_tip_height(BlockHeight::new(observed_height));
    deadline
        .wait_for(
            "the tip-change watcher to observe the advanced tip",
            until_writer_stops(
                &mut writer,
                "the tip-change watcher observed an event",
                waiting,
            ),
        )
        .await???;
    let status = deadline
        .wait_for("the final writer status", canonical.writer_status())
        .await??;
    let writer_fence = status
        .fence
        .ok_or_else(|| eyre::eyre!("writer status omitted its canonical fence"))?;
    assert_eq!(writer_fence.visible_tip_height, observed_height);
    assert!(writer_fence.event_sequence >= 2);

    cancel.cancel();
    deadline
        .wait_for("the ingest-control server task to stop", server)
        .await?
        .wrap_err("the ingest-control server task did not complete")?
        .wrap_err("the ingest-control server task failed")?;
    deadline
        .wait_for("the tip-change publisher task to stop", publisher)
        .await?
        .wrap_err("the tip-change publisher task did not complete")?;
    deadline
        .wait_for("the canonical writer task to stop", writer)
        .await?
        .wrap_err("the canonical writer task did not complete")?
        .wrap_err("the canonical writer task failed")?;
    Ok(())
}

/// Polls `future` once and reports whether it completed.
async fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
    let mut future = pin!(future);
    poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
}

/// Reads the writer's starting event sequence, retrying failed status
/// requests.
///
/// Each request carries the control plane's own reply timeout, which a slow
/// construction can exceed while the writer stays healthy. The caller bounds
/// the retries by the scenario deadline and by the writer task stopping.
async fn startup_event_sequence(canonical: &CanonicalControlHandle) -> eyre::Result<u64> {
    loop {
        if let Ok(status) = canonical.writer_status().await {
            return Ok(status
                .fence
                .ok_or_else(|| eyre::eyre!("writer status omitted its canonical fence"))?
                .event_sequence);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

struct CanonicalWriterTask {
    source: MutableTipSource,
    activations: Arc<NetworkUpgradeActivations>,
    storage_path: std::path::PathBuf,
    readiness: ComposedReadiness,
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
            tree_state_checkpoint_selection: None,
            follow: CanonicalFollowConfig {
                request_timeout: Duration::from_secs(5),
                poll_interval: Duration::from_millis(10),
                lag_threshold_blocks: 0,
                target_height: None,
                event_retention_window: None,
                event_retention_check_interval: Duration::from_secs(1),
                mempool_ready_gate: None,
                max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
                node_failover: None,
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

async fn wait_for_ingest_control(
    endpoint: &str,
    server: &mut tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) -> eyre::Result<()> {
    let ready = async {
        loop {
            if let Ok(mut client) = IngestControlClient::connect(endpoint.to_owned()).await
                && client.writer_status(WriterStatusRequest {}).await.is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::select! {
        () = ready => Ok(()),
        server_outcome = server => {
            server_outcome??;
            Err(eyre::eyre!("ingest-control server stopped before it became ready"))
        }
    }
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
