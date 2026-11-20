#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{
    collections::BTreeMap,
    num::{NonZeroU32, NonZeroU64},
    ops::Bound,
    path::Path,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use parking_lot::Mutex;
use tempfile::tempdir;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use zebra_chain::{block::Block as ZebraBlock, serialization::ZcashDeserializeInto};
use zinder_core::{
    BlockHeight, BlockId, CommitmentTreeAccumulator, CommitmentTreeCheckpoint,
    CommitmentTreeFrontier, CommitmentTreeFrontiers, MempoolEvictionReason, Network,
    NetworkUpgradeActivations, SUBTREE_LEAF_COUNT, ShieldedProtocol, SubtreeRootArtifact,
    SubtreeRootIndex, SubtreeRootRange, TransactionId, UnixTimestampMillis,
};
use zinder_ingest::{
    CanonicalConstructionConfig, CanonicalControlCommand, CanonicalFollowConfig, CanonicalFollower,
    CheckpointComponent, RawBlobPolicy, follow_canonical_tip, follow_canonical_tip_with_control,
    load_fresh_canonical, prepare_canonical_block,
};
use zinder_proto::v1::ingest::CanonicalWriterStatusResponse;
use zinder_runtime::{ComposedReadiness, NodeUnavailableDetail, Readiness, ReadinessCause};
use zinder_source::{
    NodeCapabilities, NodeSource, SourceBlock, SourceChainSegment, SourceChainSegmentLimits,
    SourceError, SourceSubtreeRoot, SourceSubtreeRoots,
};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalReorgPolicy, CanonicalStoreBuildPlan,
    CanonicalStoreWorkload, MempoolEvent, MempoolEventEnvelope, MempoolEventRetentionConfig,
    MempoolEventRetentionStepBudget, RocksDbCanonicalBuilder, RocksDbResourceBudget,
};
use zinder_testkit::{frontier_of_size, sample_regtest_upgrade_activations};

use super::{
    canonical_block_context::{chained_source_block, pool_commitments, shielded_transactions},
    fixture_block::{
        fixture_orchard_source_block, fixture_sapling_source_block, fixture_source_block,
        fixture_zakura_orchard_source_block,
    },
};

const SERIALIZED_HEADER_PARENT_OFFSET: usize = 4;
const SERIALIZED_HEADER_PARENT_END: usize = SERIALIZED_HEADER_PARENT_OFFSET + 32;
const SERIALIZED_HEADER_TIME_OFFSET: usize = 100;
const COINBASE_HEIGHT_OPCODE_OFFSET: usize = 1_534;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceCall {
    Tip,
    Block(BlockHeight),
    Checkpoint(BlockHeight),
    Subtree(SubtreeRootRange),
}

/// This source is intentionally test-local: it makes the production follower's
/// source boundary observable without adding an adapter to production code.
#[derive(Clone)]
pub(super) struct RecordingParseableSource {
    blocks: Arc<Mutex<BTreeMap<BlockHeight, SourceBlock>>>,
    checkpoints: Arc<Mutex<BTreeMap<BlockHeight, CommitmentTreeCheckpoint>>>,
    tip_height: Arc<Mutex<BlockHeight>>,
    calls: Arc<Mutex<Vec<SourceCall>>>,
    tip_call_count: Arc<Mutex<u32>>,
    tip_call_gate: Arc<Mutex<Option<SourceTipCallGate>>>,
    cancel_after_tip_call: Arc<Mutex<Option<(u32, CancellationToken)>>>,
    cancel_after_checkpoint: Arc<Mutex<Option<(BlockHeight, CancellationToken)>>>,
    switch_before_checkpoint: Arc<Mutex<Option<(BlockHeight, PendingChainSwitch)>>>,
    switch_before_subtree_read: Arc<Mutex<Option<PendingChainSwitch>>>,
    switch_back_after_subtree_read: Arc<Mutex<Option<(BlockHeight, PendingChainSwitch)>>>,
    subtree_roots: Arc<Mutex<Vec<(ShieldedProtocol, SourceSubtreeRoot)>>>,
}

/// A best-chain switch the source applies before a later read, after it
/// already served a block of the previous chain.
struct PendingChainSwitch {
    blocks: BTreeMap<BlockHeight, SourceBlock>,
    checkpoints: BTreeMap<BlockHeight, CommitmentTreeCheckpoint>,
    subtree_roots: Vec<(ShieldedProtocol, SourceSubtreeRoot)>,
    tip_height: BlockHeight,
}

impl PendingChainSwitch {
    /// A switch to a parseable chain, which completes no subtree, with its
    /// tip at `tip_height`.
    fn to_parseable_chain(
        blocks: BTreeMap<BlockHeight, SourceBlock>,
        activations: &NetworkUpgradeActivations,
        tip_height: BlockHeight,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            checkpoints: checkpoints_for_parseable_chain(&blocks, activations)?,
            blocks,
            subtree_roots: Vec::new(),
            tip_height,
        })
    }
}

#[derive(Clone)]
struct SourceTipCallGate {
    call_count: u32,
    entered: Arc<Notify>,
    resume: Arc<Notify>,
}

impl RecordingParseableSource {
    pub(super) fn new(
        blocks: BTreeMap<BlockHeight, SourceBlock>,
        checkpoints: BTreeMap<BlockHeight, CommitmentTreeCheckpoint>,
        tip_height: BlockHeight,
    ) -> Self {
        Self {
            blocks: Arc::new(Mutex::new(blocks)),
            checkpoints: Arc::new(Mutex::new(checkpoints)),
            tip_height: Arc::new(Mutex::new(tip_height)),
            calls: Arc::new(Mutex::new(Vec::new())),
            tip_call_count: Arc::new(Mutex::new(0)),
            tip_call_gate: Arc::new(Mutex::new(None)),
            cancel_after_tip_call: Arc::new(Mutex::new(None)),
            cancel_after_checkpoint: Arc::new(Mutex::new(None)),
            switch_before_checkpoint: Arc::new(Mutex::new(None)),
            switch_before_subtree_read: Arc::new(Mutex::new(None)),
            switch_back_after_subtree_read: Arc::new(Mutex::new(None)),
            subtree_roots: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Switches to `switch` when the tree state at `height` is next read.
    fn switch_chain_before_checkpoint(&self, height: BlockHeight, switch: PendingChainSwitch) {
        *self.switch_before_checkpoint.lock() = Some((height, switch));
    }

    /// Switches to `switch` when subtree roots are next read.
    fn switch_chain_before_subtree_read(&self, switch: PendingChainSwitch) {
        *self.switch_before_subtree_read.lock() = Some(switch);
    }

    /// Switches to `switch` when subtree roots are next read, and to
    /// `switch_back` when the tree state at `height` is read after that.
    fn switch_chain_around_subtree_read(
        &self,
        switch: PendingChainSwitch,
        height: BlockHeight,
        switch_back: PendingChainSwitch,
    ) {
        self.switch_chain_before_subtree_read(switch);
        *self.switch_back_after_subtree_read.lock() = Some((height, switch_back));
    }

    /// Returns a source that serves `chain`.
    fn serving(chain: PendingChainSwitch) -> Self {
        let source = Self::new(BTreeMap::new(), BTreeMap::new(), chain.tip_height);
        source.apply_chain_switch(chain);
        source
    }

    fn apply_chain_switch(&self, switch: PendingChainSwitch) {
        self.replace_chain(switch.blocks, switch.checkpoints, switch.tip_height);
        self.replace_subtree_roots(switch.subtree_roots);
    }

    fn replace_subtree_roots(&self, subtree_roots: Vec<(ShieldedProtocol, SourceSubtreeRoot)>) {
        *self.subtree_roots.lock() = subtree_roots;
    }

    pub(super) fn set_tip_height(&self, height: BlockHeight) {
        *self.tip_height.lock() = height;
    }

    fn replace_chain(
        &self,
        blocks: BTreeMap<BlockHeight, SourceBlock>,
        checkpoints: BTreeMap<BlockHeight, CommitmentTreeCheckpoint>,
        tip_height: BlockHeight,
    ) {
        *self.blocks.lock() = blocks;
        *self.checkpoints.lock() = checkpoints;
        *self.tip_height.lock() = tip_height;
    }

    fn clear_calls(&self) {
        self.calls.lock().clear();
        *self.tip_call_count.lock() = 0;
    }

    fn cancel_after_tip_call(&self, call_count: u32, cancel: CancellationToken) {
        *self.cancel_after_tip_call.lock() = Some((call_count, cancel));
    }

    fn pause_tip_call(&self, call_count: u32) -> (Arc<Notify>, Arc<Notify>) {
        let entered = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        *self.tip_call_gate.lock() = Some(SourceTipCallGate {
            call_count,
            entered: Arc::clone(&entered),
            resume: Arc::clone(&resume),
        });
        (entered, resume)
    }

    fn cancel_after_checkpoint(&self, height: BlockHeight, cancel: CancellationToken) {
        *self.cancel_after_checkpoint.lock() = Some((height, cancel));
    }

    fn calls(&self) -> Vec<SourceCall> {
        self.calls.lock().clone()
    }

    fn block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.blocks
            .lock()
            .get(&height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "parseable follower fixture has no block at this height".to_owned(),
            })
    }

    fn checkpoint_at(&self, height: BlockHeight) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.checkpoints
            .lock()
            .get(&height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "parseable follower fixture has no checkpoint at this height".to_owned(),
            })
    }
}

#[async_trait]
impl NodeSource for RecordingParseableSource {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.calls.lock().push(SourceCall::Block(height));
        self.block_at(height)
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        let Some(start_height) = limits.cursor.next_connected_height() else {
            return Ok(SourceChainSegment::default());
        };
        let tip_height = *self.tip_height.lock();
        let max_blocks = limits.max_connected_blocks.get();
        let blocks = self
            .blocks
            .lock()
            .range(start_height..=tip_height)
            .take(usize::try_from(max_blocks).unwrap_or(usize::MAX))
            .map(|(_, block)| block.clone())
            .collect::<Vec<_>>();
        Ok(SourceChainSegment::connected_blocks(blocks))
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        _network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.calls.lock().push(SourceCall::Checkpoint(height));
        let switch = self
            .switch_before_checkpoint
            .lock()
            .take_if(|(switch_height, _)| *switch_height == height);
        if let Some((_, switch)) = switch {
            self.apply_chain_switch(switch);
        }
        let checkpoint = self.checkpoint_at(height)?;
        if let Some((cancel_at, cancel)) = self.cancel_after_checkpoint.lock().as_ref()
            && height == *cancel_at
        {
            cancel.cancel();
        }
        Ok(checkpoint)
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        self.calls.lock().push(SourceCall::Tip);
        let call_count = {
            let mut count = self.tip_call_count.lock();
            *count = count.saturating_add(1);
            *count
        };
        if let Some((cancel_at, cancel)) = self.cancel_after_tip_call.lock().as_ref()
            && call_count == *cancel_at
        {
            cancel.cancel();
        }
        let tip_call_gate = self
            .tip_call_gate
            .lock()
            .as_ref()
            .filter(|gate| gate.call_count == call_count)
            .cloned();
        if let Some(gate) = tip_call_gate {
            gate.entered.notify_one();
            gate.resume.notified().await;
        }
        let height = *self.tip_height.lock();
        let block = self.block_at(height)?;
        Ok(BlockId::new(height, block.hash))
    }

    /// Answers from the current chain's subtree roots, which, like
    /// `z_getsubtreesbyindex`, name no block, and refuses a short range as
    /// the JSON-RPC source does.
    async fn fetch_subtree_root_range(
        &self,
        range: SubtreeRootRange,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.calls.lock().push(SourceCall::Subtree(range));
        let switch = self.switch_before_subtree_read.lock().take();
        if let Some(switch) = switch {
            self.apply_chain_switch(switch);
            let switch_back = self.switch_back_after_subtree_read.lock().take();
            if let Some((height, switch_back)) = switch_back {
                self.switch_chain_before_checkpoint(height, switch_back);
            }
        }
        let subtree_roots = self.subtree_roots.lock().clone();
        let roots = range
            .into_iter()
            .map(|index| {
                subtree_roots
                    .iter()
                    .find(|(protocol, root)| {
                        *protocol == range.protocol && root.subtree_index == index
                    })
                    .map(|(_, root)| root.clone())
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| SourceError::SubtreeRootsUnavailable {
                protocol: range.protocol,
                start_index: range.start_index,
                reason: "parseable follower fixture has no root at this index".to_owned(),
            })?;
        Ok(SourceSubtreeRoots::new(
            range.protocol,
            range.start_index,
            roots,
        ))
    }
}

/// Answers a block-bound tree-state read the way Zebra and Zakura do.
///
/// The read is keyed by the block's hash, so a block that left the best chain
/// is refused rather than answered with the block that replaced it.
#[derive(Clone)]
struct HashKeyedTreeStateSource(RecordingParseableSource);

#[async_trait]
impl NodeSource for HashKeyedTreeStateSource {
    fn capabilities(&self) -> NodeCapabilities {
        self.0.capabilities()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.0.fetch_block_at(height).await
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        self.0.fetch_chain_segment(limits).await
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.0
            .fetch_chain_checkpoint(height, network_upgrade_activations)
            .await
    }

    async fn fetch_chain_checkpoint_for_block(
        &self,
        block_id: BlockId,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        let checkpoint = self
            .0
            .fetch_chain_checkpoint(block_id.height, network_upgrade_activations)
            .await?;
        if checkpoint.block_id == block_id {
            Ok(checkpoint)
        } else {
            Err(SourceError::BlockUnavailable {
                height: block_id.height,
                reason: "the requested block is not in the main chain".to_owned(),
            })
        }
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        self.0.tip_id().await
    }

    async fn fetch_subtree_root_range(
        &self,
        range: SubtreeRootRange,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.0.fetch_subtree_root_range(range).await
    }
}

/// Proves settlement moves from 1 to the *locally stored* block 2 only when
/// the next visible height reaches 4 under a two-block reorg window.
#[tokio::test]
async fn canonical_follower_settles_from_local_header_without_historical_source_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let blocks = parseable_multi_block_chain()?;
    let checkpoints = checkpoints_for_parseable_chain(&blocks, &activations)?;
    let source = RecordingParseableSource::new(blocks.clone(), checkpoints, BlockHeight::new(2));
    let temporary = tempdir()?;
    let store_path = temporary.path().join("canonical");
    let first = block_id(&blocks, 1)?;
    let second = block_id(&blocks, 2)?;
    let build_tip = second;
    let build_plan = CanonicalStoreBuildPlan::complete(
        &activations,
        blocks
            .get(&BlockHeight::new(1))
            .ok_or("parseable chain must contain height 1")?
            .block_time_seconds
            .saturating_sub(1),
        build_tip,
        zinder_store::RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(2)?,
    )?;
    let builder = RocksDbCanonicalBuilder::create_fresh(
        &store_path,
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let construction = CanonicalConstructionConfig::for_local_tests(
        Duration::from_secs(5),
        Arc::clone(&activations),
    );
    let built = load_fresh_canonical(builder, &source, &construction).await?;
    let validated = built.builder.prepare_cold_certified_publication()?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        first,
        UnixTimestampMillis::new(1_783_933_200_000),
    ))?;
    let store = validated.publish_baseline(publication)?;

    assert_eq!(store.event_fence().visible_tip(), second);
    assert_eq!(store.chain_epoch()?.settled_tip_height, BlockHeight::new(1));
    source.set_tip_height(BlockHeight::new(4));
    source.clear_calls();

    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let follower = CanonicalFollower::new(
        &source,
        activations,
        CanonicalFollowConfig {
            request_timeout: Duration::from_secs(5),
            poll_interval: Duration::ZERO,
            lag_threshold_blocks: 0,
            target_height: Some(BlockHeight::new(4)),
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
            node_failover: None,
        },
        &readiness,
        &cancel,
    );
    let store = follow_canonical_tip(store, follower).await?;

    let fourth = block_id(&blocks, 4)?;
    assert_eq!(store.event_fence().visible_tip(), fourth);
    let epoch = store.chain_epoch()?;
    assert_eq!(epoch.visible_tip_height, BlockHeight::new(4));
    assert_eq!(epoch.settled_tip_height, BlockHeight::new(2));
    assert_eq!(epoch.settled_tip_hash, second.hash);
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Tip,
            SourceCall::Block(BlockHeight::new(3)),
            SourceCall::Checkpoint(BlockHeight::new(3)),
            SourceCall::Tip,
            SourceCall::Block(BlockHeight::new(4)),
            SourceCall::Checkpoint(BlockHeight::new(4)),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn canonical_follower_cancellation_after_reorg_preparation_preserves_original_fence()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    let source = RecordingParseableSource::new(
        original.clone(),
        checkpoints_for_parseable_chain(&original, &activations)?,
        BlockHeight::new(4),
    );
    let temporary = tempdir()?;
    let store_path = temporary.path().join("canonical");
    let store = publish_parseable_store(
        &source,
        &original,
        Arc::clone(&activations),
        &store_path,
        3,
        BlockHeight::new(1),
    )
    .await?;
    let original_fence = store.event_fence();

    let replacement = forked_parseable_chain(&original, 3, 4, 59)?;
    source.replace_chain(
        replacement.clone(),
        checkpoints_for_parseable_chain(&replacement, &activations)?,
        BlockHeight::new(4),
    );
    source.clear_calls();
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    source.cancel_after_checkpoint(BlockHeight::new(4), cancel.clone());
    let follower = CanonicalFollower::new(
        &source,
        Arc::clone(&activations),
        CanonicalFollowConfig {
            request_timeout: Duration::from_secs(5),
            poll_interval: Duration::ZERO,
            lag_threshold_blocks: 0,
            target_height: None,
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
            node_failover: None,
        },
        &readiness,
        &cancel,
    );
    let store = follow_canonical_tip(store, follower).await?;

    assert_eq!(store.event_fence(), original_fence);
    assert_eq!(store.displaced_block_count()?, 0);
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Tip,
            SourceCall::Block(BlockHeight::new(4)),
            SourceCall::Block(BlockHeight::new(3)),
            SourceCall::Block(BlockHeight::new(2)),
            SourceCall::Checkpoint(BlockHeight::new(3)),
            SourceCall::Checkpoint(BlockHeight::new(4)),
        ]
    );
    drop(store);

    let reopened = zinder_store::RocksDbCanonicalStore::open_ready(
        &store_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        zinder_store::RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(3)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(reopened.event_fence(), original_fence);
    assert_eq!(reopened.displaced_block_count()?, 0);
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one public follower proof covers bounded discovery, atomic replacement, archive, reopen, and the subsequent append"
)]
async fn canonical_follower_replaces_shallow_fork_reopens_and_continues_appending()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    let original_checkpoints = checkpoints_for_parseable_chain(&original, &activations)?;
    let source =
        RecordingParseableSource::new(original.clone(), original_checkpoints, BlockHeight::new(4));
    let temporary = tempdir()?;
    let store_path = temporary.path().join("canonical");
    let store = publish_parseable_store(
        &source,
        &original,
        Arc::clone(&activations),
        &store_path,
        3,
        BlockHeight::new(1),
    )
    .await?;
    let original_fence = store.event_fence();
    let original_three = block_id(&original, 3)?;
    let original_four = block_id(&original, 4)?;

    let replacement = forked_parseable_chain(&original, 3, 5, 31)?;
    let replacement_checkpoints = checkpoints_for_parseable_chain(&replacement, &activations)?;
    let replacement_three = block_id(&replacement, 3)?;
    let replacement_four = block_id(&replacement, 4)?;
    source.replace_chain(
        replacement.clone(),
        replacement_checkpoints,
        BlockHeight::new(4),
    );
    source.clear_calls();
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    source.cancel_after_tip_call(2, cancel.clone());
    let follower = CanonicalFollower::new(
        &source,
        Arc::clone(&activations),
        CanonicalFollowConfig {
            request_timeout: Duration::from_secs(5),
            poll_interval: Duration::ZERO,
            lag_threshold_blocks: 0,
            target_height: None,
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
            node_failover: None,
        },
        &readiness,
        &cancel,
    );
    let store = follow_canonical_tip(store, follower).await?;

    assert_eq!(store.event_fence().chain_epoch_id().value(), 2);
    assert_eq!(store.event_fence().chain_event_sequence(), 2);
    assert_eq!(store.event_fence().visible_tip(), replacement_four);
    assert_ne!(
        store.event_fence().sequence_digest(),
        original_fence.sequence_digest()
    );
    assert_eq!(store.chain_epoch()?.settled_tip_height, BlockHeight::new(1));
    assert_eq!(store.displaced_block_count()?, 2);
    assert_eq!(
        store
            .displaced_blocks_for_event(
                2,
                std::num::NonZeroU32::new(2).ok_or("archive page limit must be nonzero")?,
            )?
            .into_iter()
            .map(|block| BlockId::new(block.header.height, block.block_hash))
            .collect::<Vec<_>>(),
        vec![original_four, original_three]
    );
    assert_eq!(
        store
            .block_header_at(BlockHeight::new(3))?
            .map(|header| BlockId::new(header.height, header.block_hash)),
        Some(replacement_three)
    );
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Tip,
            SourceCall::Block(BlockHeight::new(4)),
            SourceCall::Block(BlockHeight::new(3)),
            SourceCall::Block(BlockHeight::new(2)),
            SourceCall::Checkpoint(BlockHeight::new(3)),
            SourceCall::Checkpoint(BlockHeight::new(4)),
            SourceCall::Tip,
        ]
    );
    let replacement_fence = store.event_fence();
    drop(store);

    let reopened = zinder_store::RocksDbCanonicalStore::open_ready(
        &store_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        zinder_store::RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(3)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(reopened.event_fence(), replacement_fence);
    assert_eq!(reopened.displaced_block_count()?, 2);

    source.set_tip_height(BlockHeight::new(5));
    source.clear_calls();
    let append_cancel = CancellationToken::new();
    let append_readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let follower = CanonicalFollower::new(
        &source,
        activations,
        CanonicalFollowConfig {
            request_timeout: Duration::from_secs(5),
            poll_interval: Duration::ZERO,
            lag_threshold_blocks: 0,
            target_height: Some(BlockHeight::new(5)),
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
            node_failover: None,
        },
        &append_readiness,
        &append_cancel,
    );
    let store = follow_canonical_tip(reopened, follower).await?;
    assert_eq!(
        store.event_fence().visible_tip(),
        block_id(&replacement, 5)?
    );
    assert_eq!(store.event_fence().chain_epoch_id().value(), 3);
    assert_eq!(store.chain_epoch()?.settled_tip_height, BlockHeight::new(2));
    assert_eq!(store.displaced_block_count()?, 2);
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Tip,
            SourceCall::Block(BlockHeight::new(5)),
            SourceCall::Checkpoint(BlockHeight::new(5)),
        ]
    );
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one refusal proof binds typed readiness, exact source bounds, reopen, and no-mutation assertions"
)]
async fn canonical_follower_refuses_a_fork_below_settlement_without_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    let source = RecordingParseableSource::new(
        original.clone(),
        checkpoints_for_parseable_chain(&original, &activations)?,
        BlockHeight::new(4),
    );
    let temporary = tempdir()?;
    let store_path = temporary.path().join("canonical");
    let store = publish_parseable_store(
        &source,
        &original,
        Arc::clone(&activations),
        &store_path,
        2,
        BlockHeight::new(2),
    )
    .await?;
    let original_fence = store.event_fence();
    let original_four = block_id(&original, 4)?;
    let over_window = forked_parseable_chain(&original, 2, 4, 47)?;
    source.replace_chain(
        over_window.clone(),
        checkpoints_for_parseable_chain(&over_window, &activations)?,
        BlockHeight::new(4),
    );
    source.clear_calls();
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let follower = CanonicalFollower::new(
        &source,
        Arc::clone(&activations),
        CanonicalFollowConfig {
            request_timeout: Duration::from_secs(5),
            poll_interval: Duration::ZERO,
            lag_threshold_blocks: 0,
            target_height: None,
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
            node_failover: None,
        },
        &readiness,
        &cancel,
    );
    let error = follow_canonical_tip(store, follower)
        .await
        .err()
        .ok_or("over-window fork must fail")?;
    assert!(matches!(
        error,
        zinder_ingest::CanonicalFollowError::ReorgWindowExceeded(evidence)
            if evidence.required_depth == 3 && evidence.configured_window_blocks == 2
    ));
    assert!(matches!(
        readiness.runtime_readiness().report().cause,
        ReadinessCause::ReorgWindowExceeded {
            depth: 3,
            configured: 2,
            covered: Some(2),
        }
    ));
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Tip,
            SourceCall::Block(BlockHeight::new(4)),
            SourceCall::Block(BlockHeight::new(3)),
        ]
    );

    let reopened = zinder_store::RocksDbCanonicalStore::open_ready(
        &store_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        zinder_store::RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(2)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(reopened.event_fence(), original_fence);
    assert_eq!(reopened.displaced_block_count()?, 0);
    assert_eq!(
        reopened
            .block_header_at(BlockHeight::new(4))?
            .map(|header| BlockId::new(header.height, header.block_hash)),
        Some(original_four)
    );
    Ok(())
}

/// A writer parked on an over-window fork keeps serving its control plane
/// from the frozen fence, and its halt survives other readiness writers until
/// termination.
#[tokio::test]
async fn parked_follower_serves_its_frozen_fence_until_termination()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    let source = RecordingParseableSource::new(
        original.clone(),
        checkpoints_for_parseable_chain(&original, &activations)?,
        BlockHeight::new(4),
    );
    let temporary = tempdir()?;
    let store = publish_parseable_store(
        &source,
        &original,
        Arc::clone(&activations),
        &temporary.path().join("canonical"),
        2,
        BlockHeight::new(2),
    )
    .await?;
    let original_fence = store.event_fence();
    let over_window = forked_parseable_chain(&original, 2, 4, 91)?;
    source.replace_chain(
        over_window.clone(),
        checkpoints_for_parseable_chain(&over_window, &activations)?,
        BlockHeight::new(4),
    );
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let (canonical, commands) = zinder_ingest::canonical_control_channel();
    let follower = single_pass_follower(&source, &activations, &readiness, &cancel);
    // Polled first, so its status request is queued before the reopened
    // writer's first source observation.
    let observe_parked = async {
        let before_observation = canonical.writer_status().await;
        while !matches!(
            readiness.runtime_readiness().report().cause,
            ReadinessCause::ReorgWindowExceeded { .. }
        ) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        readiness.publish_admission(zinder_runtime::ReadinessState::ready(Some(4)));
        let snapshot = canonical.writer_status().await;
        cancel.cancel();
        (before_observation, snapshot)
    };
    let ((before_observation, snapshot), outcome) = tokio::join!(
        observe_parked,
        follow_canonical_tip_with_control(store, follower, commands),
    );

    // The reopened writer never reports its persisted fence as confirmed:
    // it goes from an unconfirmed tip straight to the halt.
    let before_observation = before_observation?;
    assert!(before_observation.tip_unconfirmed);
    assert!(before_observation.reorg_window_exceeded.is_none());
    let snapshot = snapshot?;
    assert!(snapshot.tip_unconfirmed);
    // Canonical-status readers attest the frozen fence, so the halt must ride
    // on the same response or they would report ready on a stopped chain.
    let halt = snapshot
        .reorg_window_exceeded
        .ok_or("a parked writer must report its halt with its writer status")?;
    assert_eq!((halt.depth, halt.configured), (3, 2));
    let frozen_fence = snapshot
        .fence
        .ok_or("a parked writer must report its frozen fence")?;
    assert_eq!(
        frozen_fence.event_sequence,
        original_fence.chain_event_sequence()
    );
    assert_eq!(
        frozen_fence.visible_tip_height,
        original_fence.visible_tip().height.value()
    );
    assert!(matches!(
        readiness.runtime_readiness().report().cause,
        ReadinessCause::ReorgWindowExceeded {
            depth: 3,
            configured: 2,
            covered: Some(2),
        }
    ));
    assert!(matches!(
        outcome,
        Err(zinder_ingest::CanonicalFollowError::ReorgWindowExceeded(_))
    ));
    Ok(())
}

/// A reopened writer reports its persisted tip as unconfirmed until a source
/// observation finds that tip on the node's branch, including while the
/// source is unreachable, and only then as confirmed.
#[tokio::test]
async fn reopened_writer_reports_an_unconfirmed_tip_until_the_source_confirms_it()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    let source = RecordingParseableSource::new(
        original.clone(),
        checkpoints_for_parseable_chain(&original, &activations)?,
        BlockHeight::new(4),
    );
    let temporary = tempdir()?;
    let store = publish_parseable_store(
        &source,
        &original,
        Arc::clone(&activations),
        &temporary.path().join("canonical"),
        3,
        BlockHeight::new(1),
    )
    .await?;
    // The node is unreachable when the writer reopens: its tip has no block.
    source.set_tip_height(BlockHeight::new(99));
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let (canonical, commands) = zinder_ingest::canonical_control_channel();
    let follower = single_pass_follower(&source, &activations, &readiness, &cancel);
    let observe = async {
        let before_observation = canonical.writer_status().await?;
        while !matches!(
            readiness.runtime_readiness().report().cause,
            ReadinessCause::NodeUnavailable(_)
        ) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let while_unreachable = canonical.writer_status().await?;
        source.set_tip_height(BlockHeight::new(4));
        while !readiness.runtime_readiness().report().is_ready {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let after_confirmation = canonical.writer_status().await?;
        cancel.cancel();
        Ok::<_, tonic::Status>((before_observation, while_unreachable, after_confirmation))
    };
    let (statuses, outcome) = tokio::join!(
        observe,
        follow_canonical_tip_with_control(store, follower, commands),
    );
    let (before_observation, while_unreachable, after_confirmation) = statuses?;
    outcome?;

    assert!(before_observation.tip_unconfirmed);
    assert!(while_unreachable.tip_unconfirmed);
    assert!(!after_confirmation.tip_unconfirmed);
    assert_eq!(
        before_observation.fence, after_confirmation.fence,
        "the persisted fence alone must not confirm the tip"
    );
    Ok(())
}

/// A node tip at or below the settled tip on the writer's own branch is behind,
/// not forked.
///
/// This is a node restart that lost its unfinalized blocks: the writer waits
/// unready and unconfirmed, without parking or mutating, and resumes once the
/// node catches up.
#[tokio::test]
async fn node_behind_the_settled_tip_on_the_canonical_branch_waits_instead_of_parking()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    for behind_tip in [BlockHeight::new(2), BlockHeight::new(1)] {
        let source = RecordingParseableSource::new(
            original.clone(),
            checkpoints_for_parseable_chain(&original, &activations)?,
            BlockHeight::new(4),
        );
        let temporary = tempdir()?;
        let store = publish_parseable_store(
            &source,
            &original,
            Arc::clone(&activations),
            &temporary.path().join("canonical"),
            2,
            BlockHeight::new(2),
        )
        .await?;
        let original_fence = store.event_fence();
        source.set_tip_height(behind_tip);
        let readiness = ComposedReadiness::new(Readiness::default(), &[]);
        let cancel = CancellationToken::new();
        let (canonical, commands) = zinder_ingest::canonical_control_channel();
        let follower = single_pass_follower(&source, &activations, &readiness, &cancel);
        let observe = async {
            while !matches!(
                readiness.runtime_readiness().report().cause,
                ReadinessCause::NodeUnavailable(ref detail)
                    if detail.failure_class == "upstream_view_changed"
                        && detail.last_reason.contains("at or below the settled tip")
            ) {
                if matches!(
                    readiness.runtime_readiness().report().cause,
                    ReadinessCause::ReorgWindowExceeded { .. }
                ) {
                    cancel.cancel();
                    return Err(tonic::Status::failed_precondition(
                        "a node behind on the canonical branch must not park the writer",
                    ));
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let while_behind = canonical.writer_status().await?;
            source.set_tip_height(BlockHeight::new(4));
            while !readiness.runtime_readiness().report().is_ready {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let after_catch_up = canonical.writer_status().await?;
            cancel.cancel();
            Ok::<_, tonic::Status>((while_behind, after_catch_up))
        };
        let (statuses, outcome) = tokio::join!(
            observe,
            follow_canonical_tip_with_control(store, follower, commands),
        );
        let (while_behind, after_catch_up) = statuses?;
        let store = outcome?;

        assert!(while_behind.tip_unconfirmed);
        assert!(while_behind.reorg_window_exceeded.is_none());
        assert!(!after_catch_up.tip_unconfirmed);
        assert_eq!(store.event_fence(), original_fence);
        assert_eq!(store.displaced_block_count()?, 0);
    }
    Ok(())
}

/// A node tip at the settled height on another branch is a fork below
/// settlement, so the writer parks on it as on any deeper fork.
#[tokio::test]
async fn node_behind_the_settled_tip_on_a_conflicting_branch_parks()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    let source = RecordingParseableSource::new(
        original.clone(),
        checkpoints_for_parseable_chain(&original, &activations)?,
        BlockHeight::new(4),
    );
    let temporary = tempdir()?;
    let store = publish_parseable_store(
        &source,
        &original,
        Arc::clone(&activations),
        &temporary.path().join("canonical"),
        2,
        BlockHeight::new(2),
    )
    .await?;
    let conflicting = forked_parseable_chain(&original, 2, 2, 53)?;
    source.replace_chain(
        conflicting.clone(),
        checkpoints_for_parseable_chain(&conflicting, &activations)?,
        BlockHeight::new(2),
    );
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let follower = single_pass_follower(&source, &activations, &readiness, &cancel);
    let error = follow_canonical_tip(store, follower)
        .await
        .err()
        .ok_or("a conflicting node tip below settlement must park")?;
    assert!(matches!(
        error,
        zinder_ingest::CanonicalFollowError::ReorgWindowExceeded(evidence)
            if evidence.required_depth == 3
                && evidence.source_tip.height == BlockHeight::new(2)
    ));
    assert!(matches!(
        readiness.runtime_readiness().report().cause,
        ReadinessCause::ReorgWindowExceeded { depth: 3, .. }
    ));
    Ok(())
}

/// A committed append confirms a reopened writer's tip: the appended block
/// named the visible tip as its parent.
#[tokio::test]
async fn committed_append_confirms_a_reopened_writer_tip() -> Result<(), Box<dyn std::error::Error>>
{
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    let source = RecordingParseableSource::new(
        original.clone(),
        checkpoints_for_parseable_chain(&original, &activations)?,
        BlockHeight::new(3),
    );
    let temporary = tempdir()?;
    let built = original
        .range(..=BlockHeight::new(3))
        .map(|(height, block)| (*height, block.clone()))
        .collect::<BTreeMap<_, _>>();
    let store = publish_parseable_store(
        &source,
        &built,
        Arc::clone(&activations),
        &temporary.path().join("canonical"),
        3,
        BlockHeight::new(1),
    )
    .await?;
    source.set_tip_height(BlockHeight::new(4));
    let (before, after) =
        Box::pin(statuses_around_first_commit(store, &source, &activations)).await?;
    assert!(before.tip_unconfirmed);
    assert!(!after.tip_unconfirmed);
    assert_eq!(after.fence.map(|fence| fence.visible_tip_height), Some(4));
    Ok(())
}

/// A committed replacement confirms a reopened writer's tip: the store then
/// carries the node's branch.
#[tokio::test]
async fn committed_replacement_confirms_a_reopened_writer_tip()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    let source = RecordingParseableSource::new(
        original.clone(),
        checkpoints_for_parseable_chain(&original, &activations)?,
        BlockHeight::new(4),
    );
    let temporary = tempdir()?;
    let store = publish_parseable_store(
        &source,
        &original,
        Arc::clone(&activations),
        &temporary.path().join("canonical"),
        3,
        BlockHeight::new(1),
    )
    .await?;
    let original_fence = store.event_fence();
    let fork = forked_parseable_chain(&original, 3, 4, 107)?;
    source.replace_chain(
        fork.clone(),
        checkpoints_for_parseable_chain(&fork, &activations)?,
        BlockHeight::new(4),
    );
    let (before, after) =
        Box::pin(statuses_around_first_commit(store, &source, &activations)).await?;
    assert!(before.tip_unconfirmed);
    assert!(!after.tip_unconfirmed);
    assert_ne!(
        after.fence.map(|fence| fence.event_sequence),
        Some(original_fence.chain_event_sequence())
    );
    Ok(())
}

/// Returns the writer status before the reopened writer's first source
/// observation and the one after its first commit.
///
/// The source becomes unreachable once that commit is done, so no at-tip
/// observation can confirm the tip in its place.
async fn statuses_around_first_commit(
    store: zinder_store::RocksDbCanonicalStore,
    source: &RecordingParseableSource,
    activations: &Arc<NetworkUpgradeActivations>,
) -> Result<
    (
        zinder_proto::v1::ingest::CanonicalWriterStatusResponse,
        zinder_proto::v1::ingest::CanonicalWriterStatusResponse,
    ),
    Box<dyn std::error::Error>,
> {
    source.clear_calls();
    let (second_tip_call, resume_second_tip_call) = source.pause_tip_call(2);
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let (canonical, commands) = zinder_ingest::canonical_control_channel();
    let follower = single_pass_follower(source, activations, &readiness, &cancel);
    // Polled first, so its first request is queued before the writer's first
    // source observation.
    let observe = async {
        let before = canonical.writer_status().await?;
        second_tip_call.notified().await;
        source.set_tip_height(BlockHeight::new(99));
        resume_second_tip_call.notify_one();
        while !matches!(
            readiness.runtime_readiness().report().cause,
            ReadinessCause::NodeUnavailable(_)
        ) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let after = canonical.writer_status().await?;
        cancel.cancel();
        Ok::<_, tonic::Status>((before, after))
    };
    let (statuses, outcome) = tokio::join!(
        observe,
        follow_canonical_tip_with_control(store, follower, commands),
    );
    outcome?;
    Ok(statuses?)
}

/// With a three-block window over tip 4 and settled tip 1, forks two and
/// three blocks deep are replaced and a four-block fork parks the writer.
#[tokio::test]
async fn canonical_follower_replaces_through_its_window_and_refuses_one_block_deeper()
-> Result<(), Box<dyn std::error::Error>> {
    const WINDOW: u32 = 3;
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    for depth in [WINDOW - 1, WINDOW, WINDOW + 1] {
        let source = RecordingParseableSource::new(
            original.clone(),
            checkpoints_for_parseable_chain(&original, &activations)?,
            BlockHeight::new(4),
        );
        let temporary = tempdir()?;
        let store = publish_parseable_store(
            &source,
            &original,
            Arc::clone(&activations),
            &temporary.path().join("canonical"),
            WINDOW,
            BlockHeight::new(1),
        )
        .await?;
        let original_fence = store.event_fence();
        let fork_height = 5 - depth;
        let fork = forked_parseable_chain(
            &original,
            fork_height,
            4,
            70_u8.saturating_add(u8::try_from(depth)?),
        )?;
        source.replace_chain(
            fork.clone(),
            checkpoints_for_parseable_chain(&fork, &activations)?,
            BlockHeight::new(4),
        );
        source.clear_calls();
        let readiness = ComposedReadiness::new(Readiness::default(), &[]);
        let cancel = CancellationToken::new();
        source.cancel_after_tip_call(2, cancel.clone());
        let outcome = follow_canonical_tip(
            store,
            single_pass_follower(&source, &activations, &readiness, &cancel),
        )
        .await;
        if depth <= WINDOW {
            let store = outcome?;
            assert_eq!(store.event_fence().visible_tip(), block_id(&fork, 4)?);
            assert_eq!(store.chain_epoch()?.settled_tip_height, BlockHeight::new(1));
            assert_eq!(store.displaced_block_count()?, u64::from(depth));
        } else {
            let error = outcome
                .err()
                .ok_or("a fork deeper than the window must fail")?;
            assert!(matches!(
                error,
                zinder_ingest::CanonicalFollowError::ReorgWindowExceeded(evidence)
                    if evidence.required_depth == depth
                        && evidence.configured_window_blocks == WINDOW
                        && evidence.local_tip == original_fence.visible_tip()
            ));
            assert!(matches!(
                readiness.runtime_readiness().report().cause,
                ReadinessCause::ReorgWindowExceeded {
                    depth: 4,
                    configured: 3,
                    covered: Some(3),
                }
            ));
        }
    }
    Ok(())
}

/// A writer that reopens with a larger window holds its settled tip, so the
/// replaceable suffix grows with new blocks and a fork deeper than the
/// construction window is followed.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one proof binds the in-place raise, the held settlement, and the deeper replacement"
)]
async fn canonical_follower_follows_a_deeper_fork_after_an_in_place_window_raise()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    let source = RecordingParseableSource::new(
        original.clone(),
        checkpoints_for_parseable_chain(&original, &activations)?,
        BlockHeight::new(4),
    );
    let temporary = tempdir()?;
    let store_path = temporary.path().join("canonical");
    drop(
        publish_parseable_store(
            &source,
            &original,
            Arc::clone(&activations),
            &store_path,
            2,
            BlockHeight::new(2),
        )
        .await?,
    );
    let store = zinder_store::RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
        &store_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        zinder_store::RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(3)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(store.reorg_policy(), CanonicalReorgPolicy::new(3)?);
    assert_eq!(
        store.build_plan().reorg_policy(),
        CanonicalReorgPolicy::new(2)?
    );

    let extended = forked_parseable_chain(&original, 5, 5, 0)?;
    source.replace_chain(
        extended.clone(),
        checkpoints_for_parseable_chain(&extended, &activations)?,
        BlockHeight::new(5),
    );
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let follower = CanonicalFollower::new(
        &source,
        Arc::clone(&activations),
        CanonicalFollowConfig {
            request_timeout: Duration::from_secs(5),
            poll_interval: Duration::ZERO,
            lag_threshold_blocks: 0,
            target_height: Some(BlockHeight::new(5)),
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
            node_failover: None,
        },
        &readiness,
        &cancel,
    );
    let store = follow_canonical_tip(store, follower).await?;
    assert_eq!(store.event_fence().visible_tip(), block_id(&extended, 5)?);
    // Under the construction window the settled tip would advance to 3.
    assert_eq!(store.chain_epoch()?.settled_tip_height, BlockHeight::new(2));

    let fork = forked_parseable_chain(&extended, 3, 5, 83)?;
    source.replace_chain(
        fork.clone(),
        checkpoints_for_parseable_chain(&fork, &activations)?,
        BlockHeight::new(5),
    );
    source.clear_calls();
    let cancel = CancellationToken::new();
    source.cancel_after_tip_call(2, cancel.clone());
    let store = follow_canonical_tip(
        store,
        single_pass_follower(&source, &activations, &readiness, &cancel),
    )
    .await?;
    assert_eq!(store.event_fence().visible_tip(), block_id(&fork, 5)?);
    assert_eq!(store.chain_epoch()?.settled_tip_height, BlockHeight::new(2));
    assert_eq!(store.displaced_block_count()?, 3);
    drop(store);

    let error = zinder_store::RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
        &store_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        zinder_store::RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(2)?,
        RocksDbResourceBudget::for_local_tests(),
    )
    .err()
    .ok_or("lowering the raised window must be refused")?;
    assert!(
        error.to_string().contains("never lowered in place"),
        "{error}"
    );
    Ok(())
}

/// Right after a raise, the replaceable depth is still the construction
/// window, so a halt reports the depth the window covered rather than implying
/// that the new window was exceeded.
#[tokio::test]
async fn halt_during_a_raised_window_ramp_reports_the_covered_depth()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    let source = RecordingParseableSource::new(
        original.clone(),
        checkpoints_for_parseable_chain(&original, &activations)?,
        BlockHeight::new(4),
    );
    let temporary = tempdir()?;
    let store_path = temporary.path().join("canonical");
    drop(
        publish_parseable_store(
            &source,
            &original,
            Arc::clone(&activations),
            &store_path,
            2,
            BlockHeight::new(2),
        )
        .await?,
    );
    let store = zinder_store::RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
        &store_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        zinder_store::RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(3)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let fork = forked_parseable_chain(&original, 2, 4, 97)?;
    source.replace_chain(
        fork.clone(),
        checkpoints_for_parseable_chain(&fork, &activations)?,
        BlockHeight::new(4),
    );
    source.clear_calls();
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    source.cancel_after_tip_call(2, cancel.clone());
    let error = follow_canonical_tip(
        store,
        single_pass_follower(&source, &activations, &readiness, &cancel),
    )
    .await
    .err()
    .ok_or("a fork below the held settled tip must park")?;

    assert!(matches!(
        error,
        zinder_ingest::CanonicalFollowError::ReorgWindowExceeded(evidence)
            if evidence.required_depth == 3
                && evidence.configured_window_blocks == 3
                && evidence.covered_depth_blocks == 2
    ));
    assert!(matches!(
        readiness.runtime_readiness().report().cause,
        ReadinessCause::ReorgWindowExceeded {
            depth: 3,
            configured: 3,
            covered: Some(2),
        }
    ));
    Ok(())
}

/// The replacement budget covers the source blocks fork discovery holds.
///
/// A replacement whose fetched blocks alone exceed it fails during discovery,
/// before preparing any block, with a typed error and no canonical write.
#[tokio::test]
async fn replacement_beyond_its_memory_budget_fails_closed_without_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    let fork = forked_parseable_chain(&original, 3, 4, 101)?;
    let error =
        follow_fork_with_replacement_budget(&original, &fork, &activations, NonZeroU64::MIN)
            .await?;
    assert!(
        matches!(
            error,
            zinder_ingest::CanonicalFollowError::ReplacementBytesExceeded {
                prepared_blocks: 0,
                max_replacement_bytes: 1,
                held_bytes: 2..,
                ..
            }
        ),
        "{error}"
    );
    Ok(())
}

/// A fork already known to start below settlement parks with
/// `ReorgWindowExceeded` even when discovery exceeds the replacement budget,
/// instead of exiting on the budget.
#[tokio::test]
async fn fork_below_settlement_parks_before_the_replacement_budget_applies()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    let fork = forked_parseable_chain(&original, 1, 4, 103)?;
    let error =
        follow_fork_with_replacement_budget(&original, &fork, &activations, NonZeroU64::MIN)
            .await?;
    assert!(
        matches!(
            error,
            zinder_ingest::CanonicalFollowError::ReorgWindowExceeded(ref evidence)
                if evidence.configured_window_blocks == 3
        ),
        "{error}"
    );
    Ok(())
}

async fn follow_fork_with_replacement_budget(
    original: &BTreeMap<BlockHeight, SourceBlock>,
    fork: &BTreeMap<BlockHeight, SourceBlock>,
    activations: &Arc<NetworkUpgradeActivations>,
    max_replacement_bytes: NonZeroU64,
) -> Result<zinder_ingest::CanonicalFollowError, Box<dyn std::error::Error>> {
    let source = RecordingParseableSource::new(
        original.clone(),
        checkpoints_for_parseable_chain(original, activations)?,
        BlockHeight::new(4),
    );
    let temporary = tempdir()?;
    let store = publish_parseable_store(
        &source,
        original,
        Arc::clone(activations),
        &temporary.path().join("canonical"),
        3,
        BlockHeight::new(1),
    )
    .await?;
    let original_fence = store.event_fence();
    source.replace_chain(
        fork.clone(),
        checkpoints_for_parseable_chain(fork, activations)?,
        BlockHeight::new(4),
    );
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let follower = CanonicalFollower::new(
        &source,
        Arc::clone(activations),
        CanonicalFollowConfig {
            request_timeout: Duration::from_secs(5),
            poll_interval: Duration::ZERO,
            lag_threshold_blocks: 0,
            target_height: None,
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes,
            node_failover: None,
        },
        &readiness,
        &cancel,
    );
    let error = follow_canonical_tip(store, follower)
        .await
        .err()
        .ok_or("a replacement beyond its budget must fail")?;
    let reopened = zinder_store::RocksDbCanonicalStore::open_ready(
        temporary.path().join("canonical"),
        activations,
        CanonicalStoreWorkload::Wallet,
        zinder_store::RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(3)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(reopened.event_fence(), original_fence);
    assert_eq!(reopened.displaced_block_count()?, 0);
    Ok(error)
}

fn follower_to_target<'a, Source: NodeSource>(
    source: &'a Source,
    activations: &Arc<NetworkUpgradeActivations>,
    target_height: BlockHeight,
    readiness: &'a ComposedReadiness,
    cancel: &'a CancellationToken,
) -> CanonicalFollower<'a, Source> {
    CanonicalFollower::new(
        source,
        Arc::clone(activations),
        CanonicalFollowConfig {
            request_timeout: Duration::from_secs(5),
            poll_interval: Duration::ZERO,
            lag_threshold_blocks: 0,
            target_height: Some(target_height),
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
            node_failover: None,
        },
        readiness,
        cancel,
    )
}

fn single_pass_follower<'a>(
    source: &'a RecordingParseableSource,
    activations: &Arc<NetworkUpgradeActivations>,
    readiness: &'a ComposedReadiness,
    cancel: &'a CancellationToken,
) -> CanonicalFollower<'a, RecordingParseableSource> {
    CanonicalFollower::new(
        source,
        Arc::clone(activations),
        CanonicalFollowConfig {
            request_timeout: Duration::from_secs(5),
            poll_interval: Duration::ZERO,
            lag_threshold_blocks: 0,
            target_height: None,
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
            node_failover: None,
        },
        readiness,
        cancel,
    )
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the fairness proof keeps both queued maintenance steps, the gated source observation, and cancellation in one causal timeline"
)]
async fn retention_steps_yield_to_canonical_source_observation()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempdir()?;
    let store_path = temporary.path().join("canonical");
    let blocks = parseable_multi_block_chain()?;
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let checkpoints = checkpoints_for_parseable_chain(&blocks, &activations)?;
    let source = RecordingParseableSource::new(blocks.clone(), checkpoints, BlockHeight::new(4));
    let store = publish_parseable_store(
        &source,
        &blocks,
        Arc::clone(&activations),
        &store_path,
        4,
        BlockHeight::new(4),
    )
    .await?;
    for transaction_tag in 1_u8..=3 {
        let _envelope = store.append_mempool_event(
            MempoolEvent::Invalidated {
                transaction_id: TransactionId::from_bytes([transaction_tag; 32]),
                reason: MempoolEvictionReason::Unknown,
            },
            UnixTimestampMillis::new(1_000),
        )?;
    }

    let retention = MempoolEventRetentionConfig::new(
        Some(Duration::from_millis(1)),
        Some(Duration::from_millis(1)),
    );
    let budget = MempoolEventRetentionStepBudget::new(
        NonZeroU32::new(1).ok_or("retention event budget must be nonzero")?,
        NonZeroU64::new(1_000_000).ok_or("retention byte budget must be nonzero")?,
    );
    let (command_sender, control_commands) = mpsc::channel(2);
    let (first_reply, first_outcome) = oneshot::channel();
    command_sender
        .send(CanonicalControlCommand::PruneMempoolEvents {
            now: UnixTimestampMillis::new(10_000),
            retention,
            budget,
            reply: first_reply,
        })
        .await?;
    let (second_reply, second_outcome) = oneshot::channel();
    command_sender
        .send(CanonicalControlCommand::PruneMempoolEvents {
            now: UnixTimestampMillis::new(10_000),
            retention,
            budget,
            reply: second_reply,
        })
        .await?;
    drop(command_sender);

    source.clear_calls();
    let cancel = CancellationToken::new();
    source.cancel_after_tip_call(2, cancel.clone());
    let (first_tip_entered, resume_first_tip) = source.pause_tip_call(1);
    let follower_source = source.clone();
    let follower_task = tokio::spawn(async move {
        let readiness = ComposedReadiness::new(Readiness::default(), &[]);
        let follower = CanonicalFollower::new(
            &follower_source,
            activations,
            CanonicalFollowConfig {
                request_timeout: Duration::from_secs(5),
                poll_interval: Duration::ZERO,
                lag_threshold_blocks: 0,
                target_height: None,
                event_retention_window: None,
                event_retention_check_interval: Duration::from_secs(1),
                mempool_ready_gate: None,
                max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
                node_failover: None,
            },
            &readiness,
            &cancel,
        );
        follow_canonical_tip_with_control(store, follower, control_commands).await
    });

    tokio::time::timeout(Duration::from_secs(1), first_tip_entered.notified()).await?;
    let first_outcome = tokio::time::timeout(Duration::from_secs(1), first_outcome).await???;
    assert!(first_outcome.has_immediate_work());
    let mut second_outcome = second_outcome;
    assert!(matches!(
        second_outcome.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    resume_first_tip.notify_one();
    let second_outcome = tokio::time::timeout(Duration::from_secs(1), second_outcome).await???;
    assert!(second_outcome.has_immediate_work());
    let _store = tokio::time::timeout(Duration::from_secs(1), follower_task).await???;
    assert_eq!(source.calls(), vec![SourceCall::Tip, SourceCall::Tip]);
    Ok(())
}

/// A source-shaped failure keeps the follower alive with a typed
/// `node_unavailable` readiness detail, and the follower resumes appending
/// once the upstream view recovers.
#[tokio::test]
async fn canonical_follower_reports_source_outage_and_resumes_after_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let blocks = parseable_multi_block_chain()?;
    let checkpoints = checkpoints_for_parseable_chain(&blocks, &activations)?;
    let published: BTreeMap<_, _> = blocks
        .range(..=BlockHeight::new(2))
        .map(|(height, block)| (*height, block.clone()))
        .collect();
    let source =
        RecordingParseableSource::new(published.clone(), checkpoints.clone(), BlockHeight::new(2));
    let temporary = tempdir()?;
    let store = publish_parseable_store(
        &source,
        &published,
        Arc::clone(&activations),
        &temporary.path().join("canonical"),
        2,
        BlockHeight::new(1),
    )
    .await?;
    // The node advertises a tip whose block it cannot serve yet.
    source.set_tip_height(BlockHeight::new(3));

    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let follower_source = source.clone();
    let follower_readiness = readiness.clone();
    let follower_task = tokio::spawn(async move {
        let follower = CanonicalFollower::new(
            &follower_source,
            activations,
            CanonicalFollowConfig {
                request_timeout: Duration::from_secs(5),
                poll_interval: Duration::ZERO,
                lag_threshold_blocks: 0,
                target_height: Some(BlockHeight::new(3)),
                event_retention_window: None,
                event_retention_check_interval: Duration::from_secs(1),
                mempool_ready_gate: None,
                max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
                node_failover: None,
            },
            &follower_readiness,
            &cancel,
        );
        follow_canonical_tip(store, follower).await
    });

    let outage = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let ReadinessCause::NodeUnavailable(detail) =
                readiness.runtime_readiness().report().cause
            {
                return detail;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    assert_eq!(outage.failure_class, "upstream_view_changed");
    assert!(
        outage.last_reason.contains("no block at this height"),
        "{}",
        outage.last_reason
    );
    assert!(!follower_task.is_finished());

    source.replace_chain(blocks.clone(), checkpoints, BlockHeight::new(3));
    let store = tokio::time::timeout(Duration::from_secs(5), follower_task).await???;
    assert_eq!(store.event_fence().visible_tip(), block_id(&blocks, 3)?);
    assert!(!matches!(
        readiness.runtime_readiness().report().cause,
        ReadinessCause::NodeUnavailable(_)
    ));
    Ok(())
}

/// A reader's deadline for one `WriterStatus` request.
const READER_WRITER_STATUS_DEADLINE: Duration = Duration::from_secs(15);

/// A node request that stalls past a reader's writer-status deadline does not
/// stall the writer's control plane.
///
/// The first tip request never answers, so it fails at the default request
/// timeout. Meanwhile the follower answers every `WriterStatus` at once, as a
/// writer that has not yet confirmed its reopened tip, and holds the mempool
/// writes sent during the stall. It applies them, in arrival order, only once
/// the stalled request has failed.
#[tokio::test]
async fn writer_status_answers_while_a_node_request_stalls()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, activations, store, _temporary) = follower_fixture_at_its_node_tip().await?;
    let (stalled_tip_entered, _never_resumed) = source.pause_tip_call(1);
    tokio::time::pause();
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let (commands, control_commands) =
        mpsc::channel(zinder_ingest::CANONICAL_CONTROL_COMMAND_CAPACITY);
    let follower =
        follower_with_default_request_timeout(&source, &activations, &readiness, &cancel);
    let observe = async {
        let observed = async {
            stalled_tip_entered.notified().await;
            let stall_started = tokio::time::Instant::now();
            let mut appends = Vec::new();
            for (stall_offset_secs, append_tag) in [(0, None), (5, Some(1)), (15, Some(2)), (29, None)]
            {
                tokio::time::sleep_until(stall_started + Duration::from_secs(stall_offset_secs))
                    .await;
                if let Some(transaction_tag) = append_tag {
                    appends.push(send_mempool_append(&commands, transaction_tag).await?);
                }
                let (status, answered_after) = timed_writer_status(&commands).await?;
                assert_eq!(answered_after, Duration::ZERO, "at {stall_offset_secs}s");
                assert!(status.tip_unconfirmed);
                for append in &mut appends {
                    assert!(
                        matches!(append.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
                        "a write sent during the stall waits for the node request at {stall_offset_secs}s"
                    );
                }
            }
            let mut appended_sequences = Vec::new();
            for append in appends {
                let envelope =
                    tokio::time::timeout(READER_WRITER_STATUS_DEADLINE, append).await???;
                appended_sequences.push(envelope.event_sequence);
            }
            assert!(stall_started.elapsed() >= Duration::from_secs(30));
            assert_eq!(appended_sequences, vec![1, 2]);

            tokio::time::timeout(Duration::from_mins(1), async {
                while !readiness.runtime_readiness().report().is_ready {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await?;
            let (status, _answered_after) = timed_writer_status(&commands).await?;
            assert!(!status.tip_unconfirmed);
            Ok::<_, Box<dyn std::error::Error>>(())
        }
        .await;
        cancel.cancel();
        observed
    };
    let (observed, outcome) = tokio::join!(
        observe,
        follow_canonical_tip_with_control(store, follower, control_commands),
    );
    observed?;
    outcome?;
    Ok(())
}

/// The follower answers `WriterStatus` while it waits out a source-recovery
/// backoff, and applies a write sent during the backoff once it ends.
#[tokio::test]
async fn writer_status_answers_during_the_source_recovery_backoff()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, activations, store, _temporary) = follower_fixture_at_its_node_tip().await?;
    // The node advertises a tip whose block it cannot serve, so each
    // observation fails at once and the follower waits out its backoff.
    source.set_tip_height(BlockHeight::new(99));
    tokio::time::pause();
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let (commands, control_commands) =
        mpsc::channel(zinder_ingest::CANONICAL_CONTROL_COMMAND_CAPACITY);
    let follower =
        follower_with_default_request_timeout(&source, &activations, &readiness, &cancel);
    let observe = async {
        let observed = async {
            yield_until_node_unavailable(&readiness).await?;
            let (status, answered_after) = timed_writer_status(&commands).await?;
            assert_eq!(answered_after, Duration::ZERO);
            assert!(status.tip_unconfirmed);

            let append_sent_at = tokio::time::Instant::now();
            let mut append = send_mempool_append(&commands, 1).await?;
            let (_status, answered_after) = timed_writer_status(&commands).await?;
            assert_eq!(answered_after, Duration::ZERO);
            assert!(matches!(
                append.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            let envelope = tokio::time::timeout(READER_WRITER_STATUS_DEADLINE, append).await???;
            assert!(
                append_sent_at.elapsed() > Duration::ZERO,
                "a write sent during the backoff applies once the backoff ends"
            );
            assert_eq!(envelope.event_sequence, 1);
            Ok::<_, Box<dyn std::error::Error>>(())
        }
        .await;
        cancel.cancel();
        observed
    };
    let (observed, outcome) = tokio::join!(
        observe,
        follow_canonical_tip_with_control(store, follower, control_commands),
    );
    observed?;
    outcome?;
    Ok(())
}

/// A canonical store published through height 4, the tip of its source.
pub(super) async fn follower_fixture_at_its_node_tip() -> Result<
    (
        RecordingParseableSource,
        Arc<NetworkUpgradeActivations>,
        zinder_store::RocksDbCanonicalStore,
        tempfile::TempDir,
    ),
    Box<dyn std::error::Error>,
> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let blocks = parseable_multi_block_chain()?;
    let source = RecordingParseableSource::new(
        blocks.clone(),
        checkpoints_for_parseable_chain(&blocks, &activations)?,
        BlockHeight::new(4),
    );
    let temporary = tempdir()?;
    let store = publish_parseable_store(
        &source,
        &blocks,
        Arc::clone(&activations),
        &temporary.path().join("canonical"),
        4,
        BlockHeight::new(4),
    )
    .await?;
    Ok((source, activations, store, temporary))
}

/// A follower that bounds each node request by the default
/// `node.request_timeout_secs`.
pub(super) fn follower_with_default_request_timeout<'a>(
    source: &'a RecordingParseableSource,
    activations: &Arc<NetworkUpgradeActivations>,
    readiness: &'a ComposedReadiness,
    cancel: &'a CancellationToken,
) -> CanonicalFollower<'a, RecordingParseableSource> {
    CanonicalFollower::new(
        source,
        Arc::clone(activations),
        CanonicalFollowConfig {
            request_timeout: Duration::from_secs(30),
            poll_interval: Duration::from_secs(1),
            lag_threshold_blocks: 0,
            target_height: None,
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
            node_failover: None,
        },
        readiness,
        cancel,
    )
}

/// Asks the follower for its writer status within a reader's deadline and
/// returns it with the time the follower took to answer.
async fn timed_writer_status(
    commands: &mpsc::Sender<CanonicalControlCommand>,
) -> Result<(CanonicalWriterStatusResponse, Duration), Box<dyn std::error::Error>> {
    let asked_at = tokio::time::Instant::now();
    let (reply, response) = oneshot::channel();
    commands
        .send(CanonicalControlCommand::WriterStatus { reply })
        .await?;
    let status = tokio::time::timeout(READER_WRITER_STATUS_DEADLINE, response).await???;
    Ok((status, asked_at.elapsed()))
}

/// Sends one durable mempool append and returns the receiver of its reply.
async fn send_mempool_append(
    commands: &mpsc::Sender<CanonicalControlCommand>,
    transaction_tag: u8,
) -> Result<
    oneshot::Receiver<Result<MempoolEventEnvelope, tonic::Status>>,
    Box<dyn std::error::Error>,
> {
    let (reply, response) = oneshot::channel();
    commands
        .send(CanonicalControlCommand::AppendMempoolEvent {
            event: Box::new(MempoolEvent::Invalidated {
                transaction_id: TransactionId::from_bytes([transaction_tag; 32]),
                reason: MempoolEvictionReason::Unknown,
            }),
            observed_at: UnixTimestampMillis::new(1_000),
            reply,
        })
        .await?;
    Ok(response)
}

/// Yields until the follower reports a node outage. Yielding keeps a paused
/// clock still, so the follower is then waiting out its recovery backoff.
async fn yield_until_node_unavailable(
    readiness: &ComposedReadiness,
) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..10_000 {
        if matches!(
            readiness.runtime_readiness().report().cause,
            ReadinessCause::NodeUnavailable(_)
        ) {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    Err("the follower never reported the node outage".into())
}

/// The node switches its best chain to a sibling block after the follower
/// read the block at the next height and before it read that block's tree
/// state.
///
/// The follower observes the node again once, reporting the changed view as
/// `upstream_view_changed`, and appends the sibling instead of exiting.
#[tokio::test]
async fn best_chain_switch_between_block_and_tree_state_reads_appends_the_new_block()
-> Result<(), Box<dyn std::error::Error>> {
    let switch = switch_best_chain_before_tree_state_read().await?;
    let source = switch.source.clone();
    let recorder = PrometheusBuilder::new().build_recorder();
    let recorded_metrics = recorder.handle();
    let _local_recorder = metrics::set_default_local_recorder(&recorder);
    Box::pin(append_sibling_after_one_recovery(
        switch,
        &source,
        "tree-state hash disagrees with the fetched block hash",
        &recorded_metrics,
    ))
    .await
}

/// The same switch against a node that answers a tree-state read by block
/// hash, as Zebra and Zakura do, and refuses the block that left its best
/// chain.
#[tokio::test]
async fn hash_keyed_tree_state_refusal_after_a_best_chain_switch_appends_the_new_block()
-> Result<(), Box<dyn std::error::Error>> {
    let switch = switch_best_chain_before_tree_state_read().await?;
    let source = HashKeyedTreeStateSource(switch.source.clone());
    let recorder = PrometheusBuilder::new().build_recorder();
    let recorded_metrics = recorder.handle();
    let _local_recorder = metrics::set_default_local_recorder(&recorder);
    Box::pin(append_sibling_after_one_recovery(
        switch,
        &source,
        "the requested block is not in the main chain",
        &recorded_metrics,
    ))
    .await
}

/// A store published through height 2 whose source switches its best chain to
/// a sibling at height 3 after serving the original block 3 and before
/// serving that block's tree state.
struct BestChainSwitchBeforeTreeState {
    source: RecordingParseableSource,
    store: zinder_store::RocksDbCanonicalStore,
    activations: Arc<NetworkUpgradeActivations>,
    sibling_tip: BlockId,
    _temporary: tempfile::TempDir,
}

async fn switch_best_chain_before_tree_state_read()
-> Result<BestChainSwitchBeforeTreeState, Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    let original_checkpoints = checkpoints_for_parseable_chain(&original, &activations)?;
    let published: BTreeMap<_, _> = original
        .range(..=BlockHeight::new(2))
        .map(|(height, block)| (*height, block.clone()))
        .collect();
    let source = RecordingParseableSource::new(
        published.clone(),
        original_checkpoints.clone(),
        BlockHeight::new(2),
    );
    let temporary = tempdir()?;
    let store = publish_parseable_store(
        &source,
        &published,
        Arc::clone(&activations),
        &temporary.path().join("canonical"),
        2,
        BlockHeight::new(1),
    )
    .await?;
    source.replace_chain(original.clone(), original_checkpoints, BlockHeight::new(3));
    let sibling = forked_parseable_chain(&original, 3, 3, 31)?;
    let sibling_tip = block_id(&sibling, 3)?;
    assert_ne!(sibling_tip, block_id(&original, 3)?);
    source.switch_chain_before_checkpoint(
        BlockHeight::new(3),
        PendingChainSwitch::to_parseable_chain(sibling, &activations, BlockHeight::new(3))?,
    );
    source.clear_calls();
    Ok(BestChainSwitchBeforeTreeState {
        source,
        store,
        activations,
        sibling_tip,
        _temporary: temporary,
    })
}

/// Follows `switch` to height 3 through `follower_source` and asserts one
/// `upstream_view_changed` recovery naming `expected_reason`, a second
/// observation of the node, and the sibling appended.
///
/// The caller installs the thread-local recorder behind `recorded_metrics`.
async fn append_sibling_after_one_recovery<Source: NodeSource>(
    switch: BestChainSwitchBeforeTreeState,
    follower_source: &Source,
    expected_reason: &str,
    recorded_metrics: &PrometheusHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let BestChainSwitchBeforeTreeState {
        source,
        store,
        activations,
        sibling_tip,
        _temporary,
    } = switch;
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let follower = follower_to_target(
        follower_source,
        &activations,
        BlockHeight::new(3),
        &readiness,
        &cancel,
    );
    let (outage, outcome) = tokio::join!(
        first_node_unavailable_detail(&readiness),
        follow_canonical_tip(store, follower),
    );
    let store = outcome?;
    let outage = outage?;

    assert_eq!(store.event_fence().visible_tip(), sibling_tip);
    assert_eq!(outage.failure_class, "upstream_view_changed");
    assert!(
        outage.last_reason.contains(expected_reason),
        "{}",
        outage.last_reason
    );
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Tip,
            SourceCall::Block(BlockHeight::new(3)),
            SourceCall::Checkpoint(BlockHeight::new(3)),
            SourceCall::Tip,
            SourceCall::Block(BlockHeight::new(3)),
            SourceCall::Checkpoint(BlockHeight::new(3)),
        ]
    );
    assert!(
        recorded_metrics.render().lines().any(|line| line
            == r#"zinder_ingest_canonical_source_recoveries_total{failure_class="upstream_view_changed"} 1"#),
        "exactly one changed-view source recovery must be recorded"
    );
    Ok(())
}

/// The node switches its best chain again while the follower prepares a
/// replacement, between a replacement block read and its tree-state read.
///
/// The follower discards the half-prepared replacement, observes the node
/// again, and commits the replacement the node now holds.
#[tokio::test]
async fn best_chain_switch_during_replacement_reobserves_and_replaces_with_the_new_branch()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    let published: BTreeMap<_, _> = original
        .range(..=BlockHeight::new(3))
        .map(|(height, block)| (*height, block.clone()))
        .collect();
    let source = RecordingParseableSource::new(
        published.clone(),
        checkpoints_for_parseable_chain(&published, &activations)?,
        BlockHeight::new(3),
    );
    let temporary = tempdir()?;
    let store = publish_parseable_store(
        &source,
        &published,
        Arc::clone(&activations),
        &temporary.path().join("canonical"),
        3,
        BlockHeight::new(1),
    )
    .await?;
    let first_branch = forked_parseable_chain(&original, 3, 4, 41)?;
    let second_branch = forked_parseable_chain(&original, 3, 4, 43)?;
    let second_tip = block_id(&second_branch, 4)?;
    assert_ne!(block_id(&first_branch, 3)?, block_id(&second_branch, 3)?);
    source.replace_chain(
        first_branch.clone(),
        checkpoints_for_parseable_chain(&first_branch, &activations)?,
        BlockHeight::new(4),
    );
    source.switch_chain_before_checkpoint(
        BlockHeight::new(3),
        PendingChainSwitch::to_parseable_chain(second_branch, &activations, BlockHeight::new(4))?,
    );
    source.clear_calls();

    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let follower = follower_to_target(
        &source,
        &activations,
        BlockHeight::new(4),
        &readiness,
        &cancel,
    );
    let (outage, outcome) = tokio::join!(
        first_node_unavailable_detail(&readiness),
        follow_canonical_tip(store, follower),
    );
    let store = outcome?;
    let outage = outage?;

    assert_eq!(store.event_fence().visible_tip(), second_tip);
    assert_eq!(outage.failure_class, "upstream_view_changed");
    assert!(
        outage
            .last_reason
            .contains("tree-state hash disagrees with the fetched block hash"),
        "{}",
        outage.last_reason
    );
    let discovery_through_first_tree_state = [
        SourceCall::Tip,
        SourceCall::Block(BlockHeight::new(4)),
        SourceCall::Block(BlockHeight::new(3)),
        SourceCall::Block(BlockHeight::new(2)),
        SourceCall::Checkpoint(BlockHeight::new(3)),
    ];
    assert_eq!(
        source.calls(),
        [
            discovery_through_first_tree_state.as_slice(),
            discovery_through_first_tree_state.as_slice(),
            &[SourceCall::Checkpoint(BlockHeight::new(4))],
        ]
        .concat()
    );
    Ok(())
}

/// A tree state that names the fetched block but disagrees with the locally
/// derived checkpoint is a contradiction, not a changed view.
///
/// The follower stops on the first read with an error naming the differing
/// part, and the store keeps its fence.
#[tokio::test]
async fn same_block_tree_state_that_contradicts_the_derived_checkpoint_fails_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let original = parseable_multi_block_chain()?;
    let original_checkpoints = checkpoints_for_parseable_chain(&original, &activations)?;
    let honest = original_checkpoints
        .get(&BlockHeight::new(3))
        .ok_or("parseable chain must have a checkpoint at height 3")?
        .clone();
    let contradictions = [
        (
            CommitmentTreeCheckpoint::new(
                honest.block_id,
                honest.block_time_seconds.wrapping_add(1),
                honest.frontiers.clone(),
            ),
            CheckpointComponent::BlockTime,
        ),
        (
            CommitmentTreeCheckpoint::new(
                honest.block_id,
                honest.block_time_seconds,
                CommitmentTreeFrontiers::default(),
            ),
            CheckpointComponent::CommitmentTreeFrontiers,
        ),
    ];
    for (contradiction, expected_component) in contradictions {
        assert_ne!(contradiction, honest);
        let mut checkpoints = original_checkpoints.clone();
        checkpoints.insert(BlockHeight::new(3), contradiction);
        let error =
            append_against_tree_states(&original, &original_checkpoints, checkpoints, &activations)
                .await?;

        assert!(
            matches!(
                error,
                zinder_ingest::CanonicalFollowError::SourceCheckpointMismatch { height, component }
                    if height == BlockHeight::new(3) && component == expected_component
            ),
            "{error}"
        );
    }
    Ok(())
}

/// Publishes `original` through height 2, then follows to height 3 while the
/// source serves `follow_checkpoints`, and returns the error that stopped the
/// follower.
///
/// Asserts that the follower made exactly one attempt and left the store's
/// fence unchanged.
async fn append_against_tree_states(
    original: &BTreeMap<BlockHeight, SourceBlock>,
    original_checkpoints: &BTreeMap<BlockHeight, CommitmentTreeCheckpoint>,
    follow_checkpoints: BTreeMap<BlockHeight, CommitmentTreeCheckpoint>,
    activations: &Arc<NetworkUpgradeActivations>,
) -> Result<zinder_ingest::CanonicalFollowError, Box<dyn std::error::Error>> {
    let published: BTreeMap<_, _> = original
        .range(..=BlockHeight::new(2))
        .map(|(height, block)| (*height, block.clone()))
        .collect();
    let source = RecordingParseableSource::new(
        published.clone(),
        original_checkpoints.clone(),
        BlockHeight::new(2),
    );
    let temporary = tempdir()?;
    let store_path = temporary.path().join("canonical");
    let store = publish_parseable_store(
        &source,
        &published,
        Arc::clone(activations),
        &store_path,
        2,
        BlockHeight::new(1),
    )
    .await?;
    let original_fence = store.event_fence();
    source.replace_chain(original.clone(), follow_checkpoints, BlockHeight::new(3));
    source.clear_calls();

    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let follower = follower_to_target(
        &source,
        activations,
        BlockHeight::new(3),
        &readiness,
        &cancel,
    );
    let error = follow_canonical_tip(store, follower)
        .await
        .err()
        .ok_or("a contradicting tree state must stop the follower")?;

    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Tip,
            SourceCall::Block(BlockHeight::new(3)),
            SourceCall::Checkpoint(BlockHeight::new(3)),
        ]
    );
    let reopened = zinder_store::RocksDbCanonicalStore::open_ready(
        &store_path,
        activations,
        CanonicalStoreWorkload::Wallet,
        zinder_store::RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(2)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(reopened.event_fence(), original_fence);
    Ok(error)
}

/// A block that completes a subtree commits the root derived from its own
/// note commitments, with one subtree-root read and no other added read.
#[tokio::test]
async fn a_block_that_completes_a_subtree_commits_the_root_derived_from_its_commitments()
-> Result<(), Box<dyn std::error::Error>> {
    let siblings = subtree_completing_siblings()?;
    let temporary = tempdir()?;
    let (source, store) =
        publish_subtree_boundary_store(&siblings, 3, &temporary.path().join("canonical")).await?;
    let first_root =
        siblings.serve_chain_through(&source, &siblings.first, SUBTREE_COMPLETION_HEIGHT)?;
    source.clear_calls();

    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let follower = follower_to_target(
        &source,
        &siblings.activations,
        SUBTREE_COMPLETION_HEIGHT,
        &readiness,
        &cancel,
    );
    let store = follow_canonical_tip(store, follower).await?;

    assert_eq!(
        store.event_fence().visible_tip(),
        BlockId::new(SUBTREE_COMPLETION_HEIGHT, siblings.first.hash)
    );
    assert_eq!(
        store.subtree_roots(orchard_subtree_zero())?,
        vec![completed_orchard_subtree_zero(&first_root, &siblings.first)]
    );
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Tip,
            SourceCall::Block(SUBTREE_COMPLETION_HEIGHT),
            SourceCall::Checkpoint(SUBTREE_COMPLETION_HEIGHT),
            SourceCall::Subtree(orchard_subtree_zero()),
        ]
    );
    Ok(())
}

/// The node switches to a sibling that completes the same subtree at the
/// same height between the block read and the subtree-root read.
///
/// The node's root disagrees with the root derived from the block ingest
/// read, and the block is no longer on the node's best chain. The follower
/// reports a changed upstream view, observes the node again, and appends the
/// sibling with the sibling's own root.
#[tokio::test]
async fn best_chain_switch_before_the_subtree_root_read_appends_the_sibling_with_its_root()
-> Result<(), Box<dyn std::error::Error>> {
    let race = switch_best_chain_before_subtree_root_read(3, SUBTREE_COMPLETION_HEIGHT).await?;
    let source = race.source.clone();
    Box::pin(follow_subtree_root_race(
        race,
        &source,
        "tree-state hash disagrees with the fetched block hash",
        &append_race_calls(),
    ))
    .await
}

/// The same switch against a node that reads a tree state by block hash, as
/// Zebra and Zakura do, and refuses the block that left its best chain.
#[tokio::test]
async fn hash_keyed_refusal_after_a_switch_before_the_subtree_root_read_appends_the_sibling()
-> Result<(), Box<dyn std::error::Error>> {
    let race = switch_best_chain_before_subtree_root_read(3, SUBTREE_COMPLETION_HEIGHT).await?;
    let source = HashKeyedTreeStateSource(race.source.clone());
    Box::pin(follow_subtree_root_race(
        race,
        &source,
        "the requested block is not in the main chain",
        &append_race_calls(),
    ))
    .await
}

/// The same switch while the follower prepares a replacement: the local
/// block at height 4 completed no subtree, and each sibling branch replaces it
/// with a block that does, extended to height 5.
#[tokio::test]
async fn best_chain_switch_before_a_replacement_subtree_root_read_replaces_with_the_sibling()
-> Result<(), Box<dyn std::error::Error>> {
    let fork_tip = BlockHeight::new(SUBTREE_COMPLETION_HEIGHT.value().saturating_add(1));
    let race = switch_best_chain_before_subtree_root_read(4, fork_tip).await?;
    let source = HashKeyedTreeStateSource(race.source.clone());
    let attempt = [
        SourceCall::Tip,
        SourceCall::Block(fork_tip),
        SourceCall::Block(SUBTREE_COMPLETION_HEIGHT),
        SourceCall::Checkpoint(SUBTREE_COMPLETION_HEIGHT),
        SourceCall::Subtree(orchard_subtree_zero()),
    ];
    let expected_calls = [
        attempt.as_slice(),
        &[SourceCall::Checkpoint(SUBTREE_COMPLETION_HEIGHT)],
        attempt.as_slice(),
        &[SourceCall::Checkpoint(fork_tip)],
    ]
    .concat();
    Box::pin(follow_subtree_root_race(
        race,
        &source,
        "the requested block is not in the main chain",
        &expected_calls,
    ))
    .await
}

/// A subtree root for the block ingest read that differs from the root
/// derived from that block, while the node still holds the block, is a
/// contradiction, not a changed view.
///
/// The follower re-reads the block and the subtree roots, stops with an error
/// naming the subtree, and the store keeps its fence and stores no root.
#[tokio::test]
async fn a_subtree_root_that_contradicts_the_derived_root_for_the_same_block_fails_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let siblings = subtree_completing_siblings()?;
    let first_root = siblings
        .chain_switch(&siblings.first, SUBTREE_COMPLETION_HEIGHT)?
        .subtree_roots;
    let first_root = only_subtree_root(&first_root)?;
    let second_root = siblings
        .chain_switch(&siblings.second, SUBTREE_COMPLETION_HEIGHT)?
        .subtree_roots;
    let second_root = only_subtree_root(&second_root)?;
    assert_ne!(first_root.root_hash, second_root.root_hash);
    let contradictions = [
        SourceSubtreeRoot::new(
            first_root.subtree_index,
            second_root.root_hash,
            first_root.completing_block_height,
        ),
        SourceSubtreeRoot::new(
            first_root.subtree_index,
            first_root.root_hash,
            BlockHeight::new(SUBTREE_COMPLETION_HEIGHT.value().saturating_add(1)),
        ),
    ];
    for contradiction in contradictions {
        let temporary = tempdir()?;
        let store_path = temporary.path().join("canonical");
        let (source, store) = publish_subtree_boundary_store(&siblings, 3, &store_path).await?;
        let original_fence = store.event_fence();
        siblings.serve_chain_through(&source, &siblings.first, SUBTREE_COMPLETION_HEIGHT)?;
        source.replace_subtree_roots(vec![(ShieldedProtocol::Orchard, contradiction)]);
        source.clear_calls();

        let readiness = ComposedReadiness::new(Readiness::default(), &[]);
        let cancel = CancellationToken::new();
        let follower = follower_to_target(
            &source,
            &siblings.activations,
            SUBTREE_COMPLETION_HEIGHT,
            &readiness,
            &cancel,
        );
        let error = follow_canonical_tip(store, follower)
            .await
            .err()
            .ok_or("a contradicting subtree root must stop the follower")?;

        assert!(
            matches!(
                error,
                zinder_ingest::CanonicalFollowError::SourceSubtreeRootMismatch {
                    height,
                    protocol: ShieldedProtocol::Orchard,
                    subtree_index,
                } if height == SUBTREE_COMPLETION_HEIGHT && subtree_index == SubtreeRootIndex::new(0)
            ),
            "{error}"
        );
        assert_eq!(
            source.calls(),
            vec![
                SourceCall::Tip,
                SourceCall::Block(SUBTREE_COMPLETION_HEIGHT),
                SourceCall::Checkpoint(SUBTREE_COMPLETION_HEIGHT),
                SourceCall::Subtree(orchard_subtree_zero()),
                SourceCall::Checkpoint(SUBTREE_COMPLETION_HEIGHT),
                SourceCall::Subtree(orchard_subtree_zero()),
            ]
        );
        let reopened = zinder_store::RocksDbCanonicalStore::open_ready(
            &store_path,
            &siblings.activations,
            CanonicalStoreWorkload::Wallet,
            zinder_store::RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(2)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(reopened.event_fence(), original_fence);
        assert!(reopened.subtree_roots(orchard_subtree_zero()).is_err());
    }
    Ok(())
}

/// The node switches to a sibling that completes the same subtree before the
/// subtree-root read, and back before the block is read again by hash.
///
/// The node holds the block at that read, so the follower reads the subtree
/// roots once more and appends the block with its own root in the same
/// iteration, instead of stopping or observing the node again.
#[tokio::test]
async fn a_switch_away_and_back_around_the_subtree_root_read_appends_the_block_with_its_root()
-> Result<(), Box<dyn std::error::Error>> {
    let siblings = subtree_completing_siblings()?;
    let temporary = tempdir()?;
    let (source, store) =
        publish_subtree_boundary_store(&siblings, 3, &temporary.path().join("canonical")).await?;
    let first_root =
        siblings.serve_chain_through(&source, &siblings.first, SUBTREE_COMPLETION_HEIGHT)?;
    source.switch_chain_around_subtree_read(
        siblings.chain_switch(&siblings.second, SUBTREE_COMPLETION_HEIGHT)?,
        SUBTREE_COMPLETION_HEIGHT,
        siblings.chain_switch(&siblings.first, SUBTREE_COMPLETION_HEIGHT)?,
    );
    source.clear_calls();

    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let follower = follower_to_target(
        &source,
        &siblings.activations,
        SUBTREE_COMPLETION_HEIGHT,
        &readiness,
        &cancel,
    );
    let store = follow_canonical_tip(store, follower).await?;

    assert_eq!(
        store.event_fence().visible_tip(),
        BlockId::new(SUBTREE_COMPLETION_HEIGHT, siblings.first.hash)
    );
    assert_eq!(
        store.subtree_roots(orchard_subtree_zero())?,
        vec![completed_orchard_subtree_zero(&first_root, &siblings.first)]
    );
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Tip,
            SourceCall::Block(SUBTREE_COMPLETION_HEIGHT),
            SourceCall::Checkpoint(SUBTREE_COMPLETION_HEIGHT),
            SourceCall::Subtree(orchard_subtree_zero()),
            SourceCall::Checkpoint(SUBTREE_COMPLETION_HEIGHT),
            SourceCall::Subtree(orchard_subtree_zero()),
        ]
    );
    Ok(())
}

/// Construction stores the subtree roots derived from the blocks it loads, in
/// the order the node lists them, when the blocks complete one pool's subtree
/// before another's.
#[tokio::test]
async fn construction_stores_the_subtree_roots_derived_from_the_loaded_blocks()
-> Result<(), Box<dyn std::error::Error>> {
    let chains = InterleavedSubtreeChains::new()?;
    let chain = chains.chain(&fixture_orchard_source_block()?, 1)?;
    let orchard_block = block_id(&chain.blocks, 3)?;
    let sapling_block = block_id(&chain.blocks, 4)?;
    let expected_roots = InterleavedSubtreeChains::stored_roots(&chain)?;
    let completion_order = chain
        .subtree_roots
        .iter()
        .map(|(protocol, root)| (*protocol, root.completing_block_height))
        .collect::<Vec<_>>();
    assert_eq!(
        completion_order,
        vec![
            (ShieldedProtocol::Orchard, orchard_block.height),
            (ShieldedProtocol::Sapling, sapling_block.height),
        ]
    );
    let temporary = tempdir()?;
    let source = RecordingParseableSource::serving(chain);

    let built = chains
        .construct(&source, &temporary.path().join("canonical"))
        .await??;

    assert_eq!(built.subtree_root_evidence.subtree_root_count, 2);
    assert_eq!(
        InterleavedSubtreeChains::publish_and_read_roots(built, orchard_block)?,
        expected_roots
    );
    Ok(())
}

/// The node reports a sibling chain's Orchard root for the blocks
/// construction loaded.
///
/// A node that switched to the sibling and keeps it refuses the fixed-tip read
/// that follows as a changed view. A node that holds the fixed tip and reports
/// the sibling's root on both reads contradicts the loaded blocks.
/// Construction stores neither root.
#[tokio::test]
async fn construction_refuses_subtree_roots_read_from_a_sibling_chain()
-> Result<(), Box<dyn std::error::Error>> {
    let chains = InterleavedSubtreeChains::new()?;
    let first = || chains.chain(&fixture_orchard_source_block()?, 1);
    let second = || chains.chain(&fixture_zakura_orchard_source_block()?, 2);

    let temporary = tempdir()?;
    let source = RecordingParseableSource::serving(first()?);
    source.switch_chain_before_subtree_read(second()?);
    let error = chains
        .construct(&source, &temporary.path().join("kept-sibling"))
        .await?
        .err()
        .ok_or("a node that left the fixed tip must refuse construction")?;
    assert!(
        matches!(
            error,
            zinder_ingest::CanonicalConstructionError::Source { .. }
        ),
        "{error}"
    );

    let source = RecordingParseableSource::serving(first()?);
    source.replace_subtree_roots(second()?.subtree_roots);
    let error = chains
        .construct(&source, &temporary.path().join("contradicting-roots"))
        .await?
        .err()
        .ok_or("a sibling's subtree root must refuse construction")?;
    assert!(
        matches!(
            error,
            zinder_ingest::CanonicalConstructionError::SourceSubtreeRootMismatch {
                protocol: ShieldedProtocol::Orchard,
                subtree_index,
            } if subtree_index == SubtreeRootIndex::new(0)
        ),
        "{error}"
    );
    assert_eq!(
        subtree_and_checkpoint_calls(&source),
        construction_reread_calls()
    );
    Ok(())
}

/// The node switches to a sibling chain before construction reads the loaded
/// blocks' subtree roots, and back before it reads the fixed tip by hash.
///
/// The node holds the fixed tip at that read, so construction reads the roots
/// once more and stores the roots derived from the loaded blocks.
#[tokio::test]
async fn construction_reads_subtree_roots_again_after_a_switch_away_and_back()
-> Result<(), Box<dyn std::error::Error>> {
    let chains = InterleavedSubtreeChains::new()?;
    let first = chains.chain(&fixture_orchard_source_block()?, 1)?;
    let settled = block_id(&first.blocks, 3)?;
    let expected_roots = InterleavedSubtreeChains::stored_roots(&first)?;
    let temporary = tempdir()?;
    let source = RecordingParseableSource::serving(first);
    source.switch_chain_around_subtree_read(
        chains.chain(&fixture_zakura_orchard_source_block()?, 2)?,
        BlockHeight::new(4),
        chains.chain(&fixture_orchard_source_block()?, 1)?,
    );

    let built = chains
        .construct(&source, &temporary.path().join("canonical"))
        .await??;

    assert_eq!(
        subtree_and_checkpoint_calls(&source),
        construction_reread_calls()
    );
    assert_eq!(
        InterleavedSubtreeChains::publish_and_read_roots(built, settled)?,
        expected_roots
    );
    Ok(())
}

/// Returns the subtree-root and tree-state reads `source` served.
fn subtree_and_checkpoint_calls(source: &RecordingParseableSource) -> Vec<SourceCall> {
    source
        .calls()
        .into_iter()
        .filter(|call| matches!(call, SourceCall::Subtree(_) | SourceCall::Checkpoint(_)))
        .collect()
}

/// Returns the reads of a construction whose first subtree-root reads differ
/// from the derived roots: both ranges, the fixed tip by hash, and both
/// ranges again.
fn construction_reread_calls() -> Vec<SourceCall> {
    let ranges = [
        SourceCall::Subtree(subtree_zero(ShieldedProtocol::Sapling)),
        SourceCall::Subtree(orchard_subtree_zero()),
    ];
    [
        ranges.as_slice(),
        &[SourceCall::Checkpoint(BlockHeight::new(4))],
        ranges.as_slice(),
    ]
    .concat()
}

/// Chains on the parseable chain's block at height 2, whose tree state is one
/// Sapling and one Orchard commitment short of subtree 0.
///
/// Block 3 of each chain completes Orchard subtree 0 and block 4 then
/// completes Sapling subtree 0, which is the reverse of the order in which a
/// node lists the two pools' roots.
struct InterleavedSubtreeChains {
    activations: Arc<NetworkUpgradeActivations>,
    predecessor: CommitmentTreeCheckpoint,
}

impl InterleavedSubtreeChains {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let SubtreeCompletingSiblings {
            activations,
            predecessor,
            ..
        } = subtree_completing_siblings()?;
        let predecessor = CommitmentTreeCheckpoint::new(
            predecessor.block_id,
            predecessor.block_time_seconds,
            CommitmentTreeFrontiers::from_validated_parts(
                Some(frontier_of_size(
                    ShieldedProtocol::Sapling,
                    SUBTREE_LEAF_COUNT - 1,
                )?),
                Some(frontier_of_size(
                    ShieldedProtocol::Orchard,
                    SUBTREE_LEAF_COUNT - 1,
                )?),
                None,
            )?,
        );
        Ok(Self {
            activations,
            predecessor,
        })
    }

    /// Returns the chain whose block 3 carries `orchard_source`'s shielded
    /// transactions, with the checkpoints and subtree roots a node serves.
    fn chain(
        &self,
        orchard_source: &SourceBlock,
        time_nonce: u8,
    ) -> Result<PendingChainSwitch, Box<dyn std::error::Error>> {
        let orchard_block = chained_source_block(
            BlockHeight::new(3),
            self.predecessor.block_id.hash,
            time_nonce,
            shielded_transactions(orchard_source)?,
        )?;
        let sapling_block = chained_source_block(
            BlockHeight::new(4),
            orchard_block.hash,
            time_nonce,
            shielded_transactions(&fixture_sapling_source_block()?)?,
        )?;
        let blocks = BTreeMap::from([
            (orchard_block.height, orchard_block),
            (sapling_block.height, sapling_block),
        ]);
        let (checkpoints, subtree_roots) =
            tree_facts(&self.activations, &self.predecessor, &blocks)?;
        Ok(PendingChainSwitch {
            blocks,
            checkpoints,
            subtree_roots,
            tip_height: BlockHeight::new(4),
        })
    }

    /// Returns the Sapling and Orchard subtree-zero roots a store built from
    /// `chain` holds, attributed to the chain's completing blocks.
    fn stored_roots(
        chain: &PendingChainSwitch,
    ) -> Result<Vec<SubtreeRootArtifact>, Box<dyn std::error::Error>> {
        [
            (ShieldedProtocol::Sapling, 4),
            (ShieldedProtocol::Orchard, 3),
        ]
        .into_iter()
        .map(|(protocol, height)| {
            let block = block_id(&chain.blocks, height)?;
            chain
                .subtree_roots
                .iter()
                .find(|(root_protocol, _)| *root_protocol == protocol)
                .map(|(_, root)| {
                    SubtreeRootArtifact::new(
                        protocol,
                        root.subtree_index,
                        root.root_hash,
                        block.height,
                        block.hash,
                    )
                })
                .ok_or_else(|| "the chain completes a subtree in each pool".into())
        })
        .collect()
    }

    /// Publishes `built` settled at `settled` and returns its Sapling and
    /// Orchard subtree-zero roots.
    fn publish_and_read_roots(
        built: zinder_ingest::CanonicalSourceLoadOutcome,
        settled: BlockId,
    ) -> Result<Vec<SubtreeRootArtifact>, Box<dyn std::error::Error>> {
        let validated = built.builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            settled,
            UnixTimestampMillis::new(1_783_933_200_000),
        ))?;
        let store = validated.publish_baseline(publication)?;
        let mut roots = store.subtree_roots(subtree_zero(ShieldedProtocol::Sapling))?;
        roots.extend(store.subtree_roots(orchard_subtree_zero())?);
        Ok(roots)
    }

    /// Constructs a store from the predecessor through the block `source`
    /// serves at height 4.
    async fn construct(
        &self,
        source: &RecordingParseableSource,
        store_path: &Path,
    ) -> Result<
        Result<
            zinder_ingest::CanonicalSourceLoadOutcome,
            zinder_ingest::CanonicalConstructionError,
        >,
        Box<dyn std::error::Error>,
    > {
        let build_plan = CanonicalStoreBuildPlan::checkpointed(
            &self.activations,
            self.predecessor.clone(),
            source
                .fetch_block_at(BlockHeight::new(4))
                .await
                .map(|block| BlockId::new(block.height, block.hash))?,
            zinder_store::RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(2)?,
        )?;
        let builder = RocksDbCanonicalBuilder::create_fresh(
            store_path,
            CanonicalStoreWorkload::Wallet,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let construction = CanonicalConstructionConfig::for_local_tests(
            Duration::from_secs(5),
            Arc::clone(&self.activations),
        );
        Ok(load_fresh_canonical(builder, source, &construction).await)
    }
}

/// Height at which each subtree-completing sibling completes Orchard subtree 0.
const SUBTREE_COMPLETION_HEIGHT: BlockHeight = BlockHeight::new(4);

/// Two blocks at [`SUBTREE_COMPLETION_HEIGHT`] on the same parent that each
/// complete Orchard subtree 0, with different first Orchard commitments and so
/// different roots.
struct SubtreeCompletingSiblings {
    activations: Arc<NetworkUpgradeActivations>,
    /// Tree state at height 2, one Orchard commitment short of subtree 0.
    predecessor: CommitmentTreeCheckpoint,
    /// Coinbase-only blocks at heights 3 and [`SUBTREE_COMPLETION_HEIGHT`].
    base: BTreeMap<BlockHeight, SourceBlock>,
    first: SourceBlock,
    second: SourceBlock,
}

fn subtree_completing_siblings() -> Result<SubtreeCompletingSiblings, Box<dyn std::error::Error>> {
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let parseable = parseable_multi_block_chain()?;
    let predecessor_block = parseable
        .get(&BlockHeight::new(2))
        .ok_or("parseable chain must contain height 2")?;
    let predecessor = CommitmentTreeCheckpoint::new(
        BlockId::new(predecessor_block.height, predecessor_block.hash),
        predecessor_block.block_time_seconds,
        CommitmentTreeFrontiers::from_validated_parts(
            Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling)),
            Some(frontier_of_size(
                ShieldedProtocol::Orchard,
                SUBTREE_LEAF_COUNT - 1,
            )?),
            None,
        )?,
    );
    let base: BTreeMap<_, _> = parseable
        .range(BlockHeight::new(3)..=SUBTREE_COMPLETION_HEIGHT)
        .map(|(height, block)| (*height, block.clone()))
        .collect();
    let parent_hash = block_id(&parseable, 3)?.hash;
    // Time nonces keep both siblings' headers distinct from the coinbase-only
    // block at the same height.
    let first = chained_source_block(
        SUBTREE_COMPLETION_HEIGHT,
        parent_hash,
        1,
        shielded_transactions(&fixture_orchard_source_block()?)?,
    )?;
    let second = chained_source_block(
        SUBTREE_COMPLETION_HEIGHT,
        parent_hash,
        2,
        shielded_transactions(&fixture_zakura_orchard_source_block()?)?,
    )?;
    Ok(SubtreeCompletingSiblings {
        activations,
        predecessor,
        base,
        first,
        second,
    })
}

impl SubtreeCompletingSiblings {
    /// Returns the chain through height 3 extended by `sibling` and then by
    /// coinbase-only blocks through `tip_height`.
    fn chain(
        &self,
        sibling: &SourceBlock,
        tip_height: BlockHeight,
    ) -> Result<BTreeMap<BlockHeight, SourceBlock>, Box<dyn std::error::Error>> {
        let mut blocks: BTreeMap<_, _> = self
            .base
            .range(..=BlockHeight::new(3))
            .map(|(height, block)| (*height, block.clone()))
            .collect();
        blocks.insert(sibling.height, sibling.clone());
        let mut parent_hash = sibling.hash;
        for height in sibling.height.value().saturating_add(1)..=tip_height.value() {
            let block = chained_source_block(BlockHeight::new(height), parent_hash, 0, Vec::new())?;
            parent_hash = block.hash;
            blocks.insert(block.height, block);
        }
        Ok(blocks)
    }

    /// Makes `source` serve [`Self::chain`] with its checkpoints and subtree
    /// roots, and returns the subtree root `sibling` completes.
    fn serve_chain_through(
        &self,
        source: &RecordingParseableSource,
        sibling: &SourceBlock,
        tip_height: BlockHeight,
    ) -> Result<SourceSubtreeRoot, Box<dyn std::error::Error>> {
        let switch = self.chain_switch(sibling, tip_height)?;
        let root = only_subtree_root(&switch.subtree_roots)?;
        source.apply_chain_switch(switch);
        Ok(root)
    }

    fn chain_switch(
        &self,
        sibling: &SourceBlock,
        tip_height: BlockHeight,
    ) -> Result<PendingChainSwitch, Box<dyn std::error::Error>> {
        let blocks = self.chain(sibling, tip_height)?;
        let (checkpoints, subtree_roots) =
            tree_facts(&self.activations, &self.predecessor, &blocks)?;
        Ok(PendingChainSwitch {
            blocks,
            checkpoints,
            subtree_roots,
            tip_height,
        })
    }
}

/// Returns each block above `predecessor`'s checkpoint and the subtree roots
/// the blocks complete, in completion order, as a node reports them.
#[expect(
    clippy::type_complexity,
    reason = "the fixture returns the two node views a test source serves"
)]
fn tree_facts(
    activations: &NetworkUpgradeActivations,
    predecessor: &CommitmentTreeCheckpoint,
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
) -> Result<
    (
        BTreeMap<BlockHeight, CommitmentTreeCheckpoint>,
        Vec<(ShieldedProtocol, SourceSubtreeRoot)>,
    ),
    Box<dyn std::error::Error>,
> {
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        predecessor.block_id.height,
        &predecessor.frontiers,
        activations,
    )?;
    let mut checkpoints = BTreeMap::new();
    let mut subtree_roots = Vec::new();
    for (height, block) in blocks.range((
        Bound::Excluded(predecessor.block_id.height),
        Bound::Unbounded,
    )) {
        let commitments = pool_commitments(block)?;
        accumulator.append_block_commitments(
            *height,
            &commitments.sapling,
            &commitments.orchard,
            &commitments.ironwood,
        )?;
        checkpoints.insert(
            *height,
            CommitmentTreeCheckpoint::new(
                BlockId::new(*height, block.hash),
                block.block_time_seconds,
                accumulator.validated_frontiers()?,
            ),
        );
        subtree_roots.extend(accumulator.completed_subtree_roots().iter().map(|root| {
            (
                root.protocol,
                SourceSubtreeRoot::new(
                    root.subtree_index,
                    root.root_hash,
                    root.completing_block_height,
                ),
            )
        }));
    }
    Ok((checkpoints, subtree_roots))
}

/// Publishes a store from the siblings' predecessor through the coinbase-only
/// block at `build_tip_height`, settled at height 3, and returns it with a
/// source that serves that chain.
async fn publish_subtree_boundary_store(
    siblings: &SubtreeCompletingSiblings,
    build_tip_height: u32,
    store_path: &Path,
) -> Result<
    (
        RecordingParseableSource,
        zinder_store::RocksDbCanonicalStore,
    ),
    Box<dyn std::error::Error>,
> {
    let published: BTreeMap<_, _> = siblings
        .base
        .range(..=BlockHeight::new(build_tip_height))
        .map(|(height, block)| (*height, block.clone()))
        .collect();
    let (checkpoints, subtree_roots) =
        tree_facts(&siblings.activations, &siblings.predecessor, &published)?;
    assert!(
        subtree_roots.is_empty(),
        "the published chain must not complete a subtree"
    );
    let source = RecordingParseableSource::new(
        published.clone(),
        checkpoints,
        BlockHeight::new(build_tip_height),
    );
    let build_plan = CanonicalStoreBuildPlan::checkpointed(
        &siblings.activations,
        siblings.predecessor.clone(),
        block_id(&published, build_tip_height)?,
        zinder_store::RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(2)?,
    )?;
    let builder = RocksDbCanonicalBuilder::create_fresh(
        store_path,
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let construction = CanonicalConstructionConfig::for_local_tests(
        Duration::from_secs(5),
        Arc::clone(&siblings.activations),
    );
    let built = load_fresh_canonical(builder, &source, &construction).await?;
    let validated = built.builder.prepare_cold_certified_publication()?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        block_id(&published, 3)?,
        UnixTimestampMillis::new(1_783_933_200_000),
    ))?;
    Ok((source, validated.publish_baseline(publication)?))
}

/// A store at `build_tip_height` whose source serves the first sibling's
/// chain through `tip_height` and switches to the second sibling's chain
/// before its next subtree-root read.
struct SubtreeRootRace {
    source: RecordingParseableSource,
    store: zinder_store::RocksDbCanonicalStore,
    siblings: SubtreeCompletingSiblings,
    tip_height: BlockHeight,
    second_tip: BlockId,
    first_root: SourceSubtreeRoot,
    second_root: SourceSubtreeRoot,
    _temporary: tempfile::TempDir,
}

async fn switch_best_chain_before_subtree_root_read(
    build_tip_height: u32,
    tip_height: BlockHeight,
) -> Result<SubtreeRootRace, Box<dyn std::error::Error>> {
    let siblings = subtree_completing_siblings()?;
    let temporary = tempdir()?;
    let (source, store) = publish_subtree_boundary_store(
        &siblings,
        build_tip_height,
        &temporary.path().join("canonical"),
    )
    .await?;
    let first_root = siblings.serve_chain_through(&source, &siblings.first, tip_height)?;
    let switch = siblings.chain_switch(&siblings.second, tip_height)?;
    let second_root = only_subtree_root(&switch.subtree_roots)?;
    let second_tip = block_id(&switch.blocks, tip_height.value())?;
    assert_eq!(first_root.subtree_index, second_root.subtree_index);
    assert_eq!(
        first_root.completing_block_height,
        second_root.completing_block_height
    );
    assert_ne!(first_root.root_hash, second_root.root_hash);
    source.switch_chain_before_subtree_read(switch);
    source.clear_calls();
    Ok(SubtreeRootRace {
        source,
        store,
        siblings,
        tip_height,
        second_tip,
        first_root,
        second_root,
        _temporary: temporary,
    })
}

/// Follows `race` through `follower_source` and checks its outcome.
///
/// The follower must report one `upstream_view_changed` recovery naming
/// `expected_reason`, make the source reads `expected_calls`, and commit the
/// second sibling's chain with the second sibling's own root.
async fn follow_subtree_root_race<Source: NodeSource>(
    race: SubtreeRootRace,
    follower_source: &Source,
    expected_reason: &str,
    expected_calls: &[SourceCall],
) -> Result<(), Box<dyn std::error::Error>> {
    let SubtreeRootRace {
        source,
        store,
        siblings,
        tip_height,
        second_tip,
        first_root,
        second_root,
        _temporary,
    } = race;
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let follower = follower_to_target(
        follower_source,
        &siblings.activations,
        tip_height,
        &readiness,
        &cancel,
    );
    let (outage, outcome) = tokio::join!(
        first_node_unavailable_detail(&readiness),
        follow_canonical_tip(store, follower),
    );
    let store = outcome?;
    let outage = outage?;

    assert_eq!(store.event_fence().visible_tip(), second_tip);
    let stored_roots = store.subtree_roots(orchard_subtree_zero())?;
    assert_eq!(
        stored_roots,
        vec![completed_orchard_subtree_zero(
            &second_root,
            &siblings.second
        )]
    );
    assert!(
        stored_roots
            .iter()
            .all(|root| root.root_hash != first_root.root_hash)
    );
    assert_eq!(outage.failure_class, "upstream_view_changed");
    assert!(
        outage.last_reason.contains(expected_reason),
        "{}",
        outage.last_reason
    );
    assert_eq!(source.calls(), expected_calls);
    Ok(())
}

/// Returns the reads of an append attempt that fails on its subtree-root
/// read, the block re-read that refuses it, and the second attempt.
fn append_race_calls() -> Vec<SourceCall> {
    let attempt = [
        SourceCall::Tip,
        SourceCall::Block(SUBTREE_COMPLETION_HEIGHT),
        SourceCall::Checkpoint(SUBTREE_COMPLETION_HEIGHT),
        SourceCall::Subtree(orchard_subtree_zero()),
    ];
    [
        attempt.as_slice(),
        &[SourceCall::Checkpoint(SUBTREE_COMPLETION_HEIGHT)],
        attempt.as_slice(),
    ]
    .concat()
}

fn orchard_subtree_zero() -> SubtreeRootRange {
    subtree_zero(ShieldedProtocol::Orchard)
}

fn subtree_zero(protocol: ShieldedProtocol) -> SubtreeRootRange {
    SubtreeRootRange::new(protocol, SubtreeRootIndex::new(0), NonZeroU32::MIN)
}

fn completed_orchard_subtree_zero(
    root: &SourceSubtreeRoot,
    completing_block: &SourceBlock,
) -> SubtreeRootArtifact {
    SubtreeRootArtifact::new(
        ShieldedProtocol::Orchard,
        SubtreeRootIndex::new(0),
        root.root_hash,
        completing_block.height,
        completing_block.hash,
    )
}

fn only_subtree_root(
    subtree_roots: &[(ShieldedProtocol, SourceSubtreeRoot)],
) -> Result<SourceSubtreeRoot, Box<dyn std::error::Error>> {
    match subtree_roots {
        [(ShieldedProtocol::Orchard, root)] => Ok(root.clone()),
        _ => Err(format!("expected one Orchard subtree root, found {subtree_roots:?}").into()),
    }
}

/// Returns the first `node_unavailable` readiness detail the writer publishes.
async fn first_node_unavailable_detail(
    readiness: &ComposedReadiness,
) -> Result<NodeUnavailableDetail, tokio::time::error::Elapsed> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let ReadinessCause::NodeUnavailable(detail) =
                readiness.runtime_readiness().report().cause
            {
                return detail;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "the test builder keeps source, fixture identity, activation identity, path, window, and settlement explicit"
)]
pub(super) async fn publish_parseable_store(
    source: &RecordingParseableSource,
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    activations: Arc<NetworkUpgradeActivations>,
    store_path: &Path,
    reorg_window_blocks: u32,
    settled_height: BlockHeight,
) -> Result<zinder_store::RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    let first_height = *blocks
        .keys()
        .next()
        .ok_or("parseable chain must not be empty")?;
    let build_tip = blocks
        .keys()
        .next_back()
        .copied()
        .ok_or("parseable chain must have a tip")?;
    let build_plan = CanonicalStoreBuildPlan::complete(
        &activations,
        blocks
            .get(&first_height)
            .ok_or("parseable chain must contain its first block")?
            .block_time_seconds
            .saturating_sub(1),
        block_id(blocks, build_tip.value())?,
        zinder_store::RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(reorg_window_blocks)?,
    )?;
    let builder = RocksDbCanonicalBuilder::create_fresh(
        store_path,
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let construction = CanonicalConstructionConfig::for_local_tests(
        Duration::from_secs(5),
        Arc::clone(&activations),
    );
    let built = load_fresh_canonical(builder, source, &construction).await?;
    let validated = built.builder.prepare_cold_certified_publication()?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        block_id(blocks, settled_height.value())?,
        UnixTimestampMillis::new(1_783_933_200_000),
    ))?;
    Ok(validated.publish_baseline(publication)?)
}

pub(super) fn parseable_multi_block_chain()
-> Result<BTreeMap<BlockHeight, SourceBlock>, Box<dyn std::error::Error>> {
    let fixture = fixture_source_block()?;
    let mut parent_hash = Network::ZcashRegtest.genesis_hash();
    let mut blocks = BTreeMap::new();
    for height in 1..=4 {
        let height = BlockHeight::new(height);
        let mut raw_block_bytes = fixture.raw_block_bytes.clone();
        raw_block_bytes[SERIALIZED_HEADER_PARENT_OFFSET..SERIALIZED_HEADER_PARENT_END]
            .copy_from_slice(&parent_hash.as_bytes());
        raw_block_bytes[COINBASE_HEIGHT_OPCODE_OFFSET] = 0x50_u8.saturating_add(
            u8::try_from(height.value()).map_err(|_| "fixture height must fit one opcode")?,
        );
        let block =
            SourceBlock::from_raw_block_bytes(Network::ZcashRegtest, height, raw_block_bytes)?;
        let parsed: ZebraBlock = block.raw_block_bytes.as_slice().zcash_deserialize_into()?;
        assert_eq!(
            parsed
                .coinbase_height()
                .map(|coinbase_height| coinbase_height.0),
            Some(height.value())
        );
        prepare_canonical_block(
            &block,
            &sample_regtest_upgrade_activations(),
            RawBlobPolicy::Transactions,
        )?;
        parent_hash = block.hash;
        blocks.insert(height, block);
    }
    Ok(blocks)
}

pub(super) fn forked_parseable_chain(
    original: &BTreeMap<BlockHeight, SourceBlock>,
    fork_height: u32,
    tip_height: u32,
    time_nonce: u8,
) -> Result<BTreeMap<BlockHeight, SourceBlock>, Box<dyn std::error::Error>> {
    let fixture = fixture_source_block()?;
    let mut blocks = original
        .range(..BlockHeight::new(fork_height))
        .map(|(height, block)| (*height, block.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut parent_hash = if fork_height <= 1 {
        Network::ZcashRegtest.genesis_hash()
    } else {
        block_id(original, fork_height.saturating_sub(1))?.hash
    };
    for height in fork_height..=tip_height {
        let height = BlockHeight::new(height);
        let mut raw_block_bytes = fixture.raw_block_bytes.clone();
        raw_block_bytes[SERIALIZED_HEADER_PARENT_OFFSET..SERIALIZED_HEADER_PARENT_END]
            .copy_from_slice(&parent_hash.as_bytes());
        raw_block_bytes[SERIALIZED_HEADER_TIME_OFFSET] =
            raw_block_bytes[SERIALIZED_HEADER_TIME_OFFSET].wrapping_add(time_nonce);
        raw_block_bytes[COINBASE_HEIGHT_OPCODE_OFFSET] = 0x50_u8.saturating_add(
            u8::try_from(height.value()).map_err(|_| "fixture height must fit one opcode")?,
        );
        let block =
            SourceBlock::from_raw_block_bytes(Network::ZcashRegtest, height, raw_block_bytes)?;
        let parsed: ZebraBlock = block.raw_block_bytes.as_slice().zcash_deserialize_into()?;
        assert_eq!(
            parsed
                .coinbase_height()
                .map(|coinbase_height| coinbase_height.0),
            Some(height.value())
        );
        prepare_canonical_block(
            &block,
            &sample_regtest_upgrade_activations(),
            RawBlobPolicy::Transactions,
        )?;
        parent_hash = block.hash;
        blocks.insert(height, block);
    }
    Ok(blocks)
}

pub(super) fn checkpoints_for_parseable_chain(
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    activations: &NetworkUpgradeActivations,
) -> Result<BTreeMap<BlockHeight, CommitmentTreeCheckpoint>, Box<dyn std::error::Error>> {
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

pub(super) fn block_id(
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    height: u32,
) -> Result<BlockId, Box<dyn std::error::Error>> {
    let height = BlockHeight::new(height);
    let block = blocks
        .get(&height)
        .ok_or_else(|| format!("parseable chain is missing height {}", height.value()))?;
    Ok(BlockId::new(height, block.hash))
}
