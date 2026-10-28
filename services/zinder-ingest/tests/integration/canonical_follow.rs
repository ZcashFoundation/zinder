#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{
    collections::BTreeMap,
    num::{NonZeroU32, NonZeroU64},
    path::Path,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use parking_lot::Mutex;
use tempfile::tempdir;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use zebra_chain::{block::Block as ZebraBlock, serialization::ZcashDeserializeInto};
use zinder_core::{
    BlockHeight, BlockId, CommitmentTreeAccumulator, CommitmentTreeCheckpoint,
    CommitmentTreeFrontiers, MempoolEvictionReason, Network, NetworkUpgradeActivations,
    SubtreeRootRange, TransactionId, UnixTimestampMillis,
};
use zinder_ingest::{
    CanonicalConstructionConfig, CanonicalControlCommand, CanonicalFollowConfig, CanonicalFollower,
    RawBlobPolicy, follow_canonical_tip, follow_canonical_tip_with_control, load_fresh_canonical,
    prepare_canonical_block,
};
use zinder_runtime::{Readiness, ReadinessCause};
use zinder_source::{
    NodeCapabilities, NodeCapability, NodeSource, SourceBlock, SourceChainSegment,
    SourceChainSegmentLimits, SourceError, SourceSubtreeRoots,
};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalReorgPolicy, CanonicalStoreBuildPlan,
    CanonicalStoreWorkload, MempoolEvent, MempoolEventRetentionConfig,
    MempoolEventRetentionStepBudget, RocksDbCanonicalBuilder, RocksDbResourceBudget,
};
use zinder_testkit::sample_regtest_upgrade_activations;

use super::fixture_block::fixture_source_block;

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
struct RecordingParseableSource {
    blocks: Arc<Mutex<BTreeMap<BlockHeight, SourceBlock>>>,
    checkpoints: Arc<Mutex<BTreeMap<BlockHeight, CommitmentTreeCheckpoint>>>,
    tip_height: Arc<Mutex<BlockHeight>>,
    calls: Arc<Mutex<Vec<SourceCall>>>,
    tip_call_count: Arc<Mutex<u32>>,
    tip_call_gate: Arc<Mutex<Option<SourceTipCallGate>>>,
    cancel_after_tip_call: Arc<Mutex<Option<(u32, CancellationToken)>>>,
    cancel_after_checkpoint: Arc<Mutex<Option<(BlockHeight, CancellationToken)>>>,
}

#[derive(Clone)]
struct SourceTipCallGate {
    call_count: u32,
    entered: Arc<Notify>,
    resume: Arc<Notify>,
}

impl RecordingParseableSource {
    fn new(
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
        }
    }

    fn set_tip_height(&self, height: BlockHeight) {
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

    async fn fetch_subtree_root_range(
        &self,
        range: SubtreeRootRange,
    ) -> Result<SourceSubtreeRoots, SourceError> {
        self.calls.lock().push(SourceCall::Subtree(range));
        Err(SourceError::NodeCapabilityMissing {
            capability: NodeCapability::SubtreeRoots,
        })
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

    let readiness = Readiness::default();
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
    let readiness = Readiness::default();
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
    let readiness = Readiness::default();
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
    let append_readiness = Readiness::default();
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
    let readiness = Readiness::default();
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
        readiness.report().cause,
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
    let readiness = Readiness::default();
    let cancel = CancellationToken::new();
    let (canonical, commands) = zinder_ingest::canonical_control_channel();
    let follower = single_pass_follower(&source, &activations, &readiness, &cancel);
    let observe_parked = async {
        while !matches!(
            readiness.report().cause,
            ReadinessCause::ReorgWindowExceeded { .. }
        ) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        readiness.set(zinder_runtime::ReadinessState::ready(Some(4)));
        let snapshot = canonical.writer_status().await;
        cancel.cancel();
        snapshot
    };
    let (outcome, snapshot) = tokio::join!(
        follow_canonical_tip_with_control(store, follower, commands),
        observe_parked,
    );

    let snapshot = snapshot?;
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
        readiness.report().cause,
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
        let readiness = Readiness::default();
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
                readiness.report().cause,
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
    let readiness = Readiness::default();
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
    let readiness = Readiness::default();
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
        readiness.report().cause,
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
    let readiness = Readiness::default();
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

fn single_pass_follower<'a>(
    source: &'a RecordingParseableSource,
    activations: &Arc<NetworkUpgradeActivations>,
    readiness: &'a Readiness,
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
        let readiness = Readiness::default();
        let follower = CanonicalFollower::new(
            &follower_source,
            activations,
            CanonicalFollowConfig {
                request_timeout: Duration::from_secs(5),
                poll_interval: Duration::from_secs(1),
                lag_threshold_blocks: 0,
                target_height: None,
                event_retention_window: None,
                event_retention_check_interval: Duration::from_secs(1),
                mempool_ready_gate: None,
                max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
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

#[expect(
    clippy::too_many_arguments,
    reason = "the test builder keeps source, fixture identity, activation identity, path, window, and settlement explicit"
)]
async fn publish_parseable_store(
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

fn parseable_multi_block_chain()
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

fn forked_parseable_chain(
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

fn checkpoints_for_parseable_chain(
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

fn block_id(
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    height: u32,
) -> Result<BlockId, Box<dyn std::error::Error>> {
    let height = BlockHeight::new(height);
    let block = blocks
        .get(&height)
        .ok_or_else(|| format!("parseable chain is missing height {}", height.value()))?;
    Ok(BlockId::new(height, block.hash))
}
