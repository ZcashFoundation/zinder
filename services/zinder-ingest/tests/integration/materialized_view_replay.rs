#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{collections::BTreeMap, error::Error, path::Path, sync::Arc, time::Duration};

use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use prost::Message as _;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use zinder_core::{
    BlockHash, BlockHeight, BlockId, CommitmentTreeAccumulator, CommitmentTreeCheckpoint,
    CommitmentTreeFrontiers, Network, NetworkUpgradeActivations, SubtreeRootRange,
    UnixTimestampMillis,
};
use zinder_ingest::{
    CanonicalConstructionConfig, CanonicalFollowConfig, CanonicalFollower, IngestError,
    MaterializedViewReplayConfig, MaterializedViewTailer, follow_canonical_tip,
    load_fresh_canonical, spawn_materialized_view_tailer_task,
};
use zinder_materialized_views::{
    BLOCK_SUMMARY_COLUMN_FAMILY, BlockSummaryConsumer, MaterializedViewStore,
    MaterializedViewStoreOptions, REORG_INCIDENTS_CONSUMER_NAME, decode_stored_record,
};
use zinder_proto::v1::wallet::{MaterializedViewHealth, MaterializedViewStatus};
use zinder_runtime::{IngestPhase, Readiness};
use zinder_source::{
    NodeCapabilities, NodeCapability, NodeSource, SourceBlock, SourceChainSegment,
    SourceChainSegmentLimits, SourceError, SourceSubtreeRoots,
};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalReorgPolicy, CanonicalStoreBuildPlan,
    CanonicalStoreWorkload, RocksDbCanonicalBuilder, RocksDbCanonicalSecondary,
    RocksDbCanonicalStore, RocksDbResourceBudget,
};
use zinder_testkit::sample_regtest_upgrade_activations;

use super::fixture_block::fixture_source_block;

const SERIALIZED_HEADER_PARENT_OFFSET: usize = 4;
const SERIALIZED_HEADER_PARENT_END: usize = SERIALIZED_HEADER_PARENT_OFFSET + 32;
const SERIALIZED_HEADER_TIME_OFFSET: usize = 100;
const COINBASE_HEIGHT_OPCODE_OFFSET: usize = 1_534;
const BASELINE_TIP_HEIGHT: u32 = 2;
const CHAIN_TIP_HEIGHT: u32 = 4;
const REPLACEMENT_TIP_HEIGHT: u32 = 5;
const REORG_WINDOW_BLOCKS: u32 = 2;

#[tokio::test]
async fn a_fresh_view_store_rebuilds_the_rows_event_replay_would_have_written()
-> Result<(), Box<dyn Error>> {
    let mut harness = CanonicalHarness::baseline().await?;
    let (_incremental_directory, incremental) = view_store()?;
    harness.tailer(&incremental).catch_up()?;
    assert_eq!(
        block_summary_hashes(&incremental, BASELINE_TIP_HEIGHT)?,
        harness.canonical_hashes(BASELINE_TIP_HEIGHT)
    );

    harness.follow_to_tip().await?;
    harness.tailer(&incremental).catch_up()?;

    let (_rebuilt_directory, rebuilt) = view_store()?;
    harness.tailer(&rebuilt).catch_up()?;

    assert_eq!(
        block_summary_hashes(&incremental, CHAIN_TIP_HEIGHT)?,
        harness.canonical_hashes(CHAIN_TIP_HEIGHT)
    );
    assert_eq!(
        block_summary_hashes(&rebuilt, CHAIN_TIP_HEIGHT)?,
        block_summary_hashes(&incremental, CHAIN_TIP_HEIGHT)?
    );
    assert_eq!(
        chain_event_cursors(&rebuilt)?,
        chain_event_cursors(&incremental)?
    );
    Ok(())
}

#[tokio::test]
async fn a_reorg_transition_reverts_and_reapplies_the_replaced_height() -> Result<(), Box<dyn Error>>
{
    let mut harness = CanonicalHarness::baseline().await?;
    harness.follow_to_tip().await?;
    let (_directory, view) = view_store()?;
    harness.tailer(&view).catch_up()?;
    let replaced = harness.canonical_hashes(CHAIN_TIP_HEIGHT);

    harness.replace_tip().await?;
    harness.tailer(&view).catch_up()?;

    let reapplied = harness.canonical_hashes(REPLACEMENT_TIP_HEIGHT);
    assert_ne!(replaced, harness.canonical_hashes(CHAIN_TIP_HEIGHT));
    assert_eq!(
        block_summary_hashes(&view, REPLACEMENT_TIP_HEIGHT)?,
        reapplied
    );
    assert!(
        view.get_chain_event_cursor(REORG_INCIDENTS_CONSUMER_NAME)?
            .is_some(),
        "the reorg incident log must advance through the replacement transition"
    );
    Ok(())
}

#[tokio::test]
async fn an_expired_cursor_recovers_onto_the_rows_a_clean_rebuild_produces()
-> Result<(), Box<dyn Error>> {
    let mut harness = CanonicalHarness::baseline().await?;
    let (_recovered_directory, recovered) = view_store()?;
    harness.tailer(&recovered).catch_up()?;

    harness.follow_to_tip().await?;
    harness.prune_events_to_fence()?;
    harness.tailer(&recovered).catch_up()?;

    let (_rebuilt_directory, rebuilt) = view_store()?;
    harness.tailer(&rebuilt).catch_up()?;

    assert_eq!(
        block_summary_hashes(&recovered, CHAIN_TIP_HEIGHT)?,
        harness.canonical_hashes(CHAIN_TIP_HEIGHT)
    );
    assert_eq!(
        block_summary_hashes(&recovered, CHAIN_TIP_HEIGHT)?,
        block_summary_hashes(&rebuilt, CHAIN_TIP_HEIGHT)?
    );
    Ok(())
}

#[tokio::test]
async fn an_undecodable_persisted_cursor_names_the_view_store_for_rebuild()
-> Result<(), Box<dyn Error>> {
    let harness = CanonicalHarness::baseline().await?;
    let (directory, view) = view_store()?;
    for consumer_name in MaterializedViewStore::bundled_chain_event_consumer_names() {
        view.put_chain_event_cursor(*consumer_name, &[0xA5; 64])?;
    }

    let error = harness
        .tailer(&view)
        .catch_up()
        .err()
        .ok_or("an undecodable cursor must refuse replay")?;

    let IngestError::MaterializedViewCursorUnreadable { path, .. } = error else {
        return Err(format!("expected an undecodable-cursor refusal, got {error:?}").into());
    };
    assert_eq!(path, directory.path());
    Ok(())
}

#[tokio::test]
async fn the_tailer_publishes_a_live_status_and_opens_the_historical_work_gate()
-> Result<(), Box<dyn Error>> {
    let mut harness = CanonicalHarness::baseline().await?;
    harness.follow_to_tip().await?;
    let (_directory, view) = view_store()?;
    let readiness = Readiness::default();
    readiness.set_phase(IngestPhase::FollowingTip);
    let gate = zinder_ingest::HistoricalWorkGate::new(readiness);
    let cancel = CancellationToken::new();

    let handle = spawn_materialized_view_tailer_task(
        harness.tailer(&view),
        Duration::from_millis(10),
        gate.clone(),
        cancel.clone(),
    );
    let opened = tokio::time::timeout(Duration::from_secs(30), async {
        while !gate.is_open() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    cancel.cancel();
    handle.await?;
    opened.map_err(|_| "the tailer must open the historical-work gate at the canonical tip")?;

    let status = materialized_view_status(&view)?;
    assert_eq!(status.indexed_height, CHAIN_TIP_HEIGHT);
    assert_eq!(status.lag_blocks, 0);
    assert_eq!(status.health, MaterializedViewHealth::Live as i32);
    Ok(())
}

fn view_store() -> Result<(TempDir, MaterializedViewStore), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let store = MaterializedViewStore::open(
        directory.path(),
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: MaterializedViewStore::bundled_consumers(),
            rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
        },
    )?;
    Ok((directory, store))
}

fn block_summary_hashes(
    view: &MaterializedViewStore,
    through_height: u32,
) -> Result<BTreeMap<u32, String>, Box<dyn Error>> {
    let mut hashes = BTreeMap::new();
    for height in 1..=through_height {
        let Some(encoded) = view.get_consumer(
            BLOCK_SUMMARY_COLUMN_FAMILY,
            &BlockSummaryConsumer::key_for_height(BlockHeight::new(height)),
        )?
        else {
            continue;
        };
        let summary = decode_stored_record(&encoded)?
            .summary
            .ok_or("a block summary record must carry a summary")?;
        hashes.insert(height, summary.block_hash);
    }
    Ok(hashes)
}

type ChainEventCursors = BTreeMap<&'static str, Option<Vec<u8>>>;

fn chain_event_cursors(view: &MaterializedViewStore) -> Result<ChainEventCursors, Box<dyn Error>> {
    let mut cursors = BTreeMap::new();
    for consumer_name in view.chain_event_consumer_names() {
        cursors.insert(
            consumer_name.as_str(),
            view.get_chain_event_cursor(consumer_name)?,
        );
    }
    Ok(cursors)
}

fn materialized_view_status(
    view: &MaterializedViewStore,
) -> Result<MaterializedViewStatus, Box<dyn Error>> {
    let encoded = view
        .get_materialized_view_status()?
        .ok_or("the tailer must persist a materialized-view status record")?;
    Ok(MaterializedViewStatus::decode(encoded.as_slice())?)
}

/// A published canonical store the driver tests extend, reorg, and prune.
struct CanonicalHarness {
    blocks: BTreeMap<BlockHeight, SourceBlock>,
    source: StaticChainSource,
    store: Option<RocksDbCanonicalStore>,
    canonical: Arc<RwLock<RocksDbCanonicalSecondary>>,
    activations: Arc<NetworkUpgradeActivations>,
    _temporary: TempDir,
}

impl CanonicalHarness {
    async fn baseline() -> Result<Self, Box<dyn Error>> {
        let activations = Arc::new(sample_regtest_upgrade_activations());
        let blocks = chained_blocks(CHAIN_TIP_HEIGHT, 0)?;
        let source = StaticChainSource::new(blocks.clone(), &activations)?;
        source.set_tip_height(BlockHeight::new(BASELINE_TIP_HEIGHT));
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let build_plan = CanonicalStoreBuildPlan::complete(
            &activations,
            block_at(&blocks, 1)?.block_time_seconds.saturating_sub(1),
            block_id(&blocks, BASELINE_TIP_HEIGHT)?,
            zinder_store::RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?,
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
            block_id(&blocks, 1)?,
            UnixTimestampMillis::new(1_783_933_200_000),
        ))?;
        let store = validated.publish_baseline(publication)?;
        let canonical = open_secondary(&store_path, temporary.path(), &activations)?;
        Ok(Self {
            blocks,
            source,
            store: Some(store),
            canonical: Arc::new(RwLock::new(canonical)),
            activations,
            _temporary: temporary,
        })
    }

    fn tailer(&self, view: &MaterializedViewStore) -> MaterializedViewTailer {
        MaterializedViewTailer {
            canonical: Arc::clone(&self.canonical),
            materialized_view_store: view.clone(),
            config: MaterializedViewReplayConfig::DEFAULT,
            activations: Arc::clone(&self.activations),
            reorg_window_blocks: REORG_WINDOW_BLOCKS,
            chain_event_retention_window: Some(Duration::from_hours(168)),
            cursor_at_risk_warning: Duration::from_hours(24),
        }
    }

    async fn follow_to_tip(&mut self) -> Result<(), Box<dyn Error>> {
        self.source
            .set_tip_height(BlockHeight::new(CHAIN_TIP_HEIGHT));
        self.follow(BlockHeight::new(CHAIN_TIP_HEIGHT)).await
    }

    async fn replace_tip(&mut self) -> Result<(), Box<dyn Error>> {
        self.blocks = chained_blocks(REPLACEMENT_TIP_HEIGHT, 1)?;
        self.source
            .replace_chain(self.blocks.clone(), &self.activations)?;
        self.source
            .set_tip_height(BlockHeight::new(REPLACEMENT_TIP_HEIGHT));
        self.follow(BlockHeight::new(REPLACEMENT_TIP_HEIGHT)).await
    }

    async fn follow(&mut self, target_height: BlockHeight) -> Result<(), Box<dyn Error>> {
        let store = self
            .store
            .take()
            .ok_or("the canonical store must be open")?;
        let readiness = Readiness::default();
        let cancel = CancellationToken::new();
        let follower = CanonicalFollower::new(
            &self.source,
            Arc::clone(&self.activations),
            CanonicalFollowConfig {
                request_timeout: Duration::from_secs(5),
                poll_interval: Duration::ZERO,
                lag_threshold_blocks: 0,
                target_height: Some(target_height),
                event_retention_window: None,
                event_retention_check_interval: Duration::from_secs(1),
                mempool_ready_gate: None,
                max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
            },
            &readiness,
            &cancel,
        );
        self.store = Some(follow_canonical_tip(store, follower).await?);
        Ok(())
    }

    /// Prunes every retained transition below the current fence, expiring any
    /// consumer cursor that still names an earlier one.
    fn prune_events_to_fence(&self) -> Result<(), Box<dyn Error>> {
        let store = self
            .store
            .as_ref()
            .ok_or("the canonical store must be open")?;
        store.prune_canonical_events_before(
            store.event_fence().chain_event_sequence(),
            UnixTimestampMillis::new(1_783_933_200_000),
        )?;
        Ok(())
    }

    fn canonical_hashes(&self, through_height: u32) -> BTreeMap<u32, String> {
        self.blocks
            .iter()
            .filter(|(height, _)| height.value() <= through_height)
            .map(|(height, block)| {
                (
                    height.value(),
                    zinder_core::wire::encode_rpc_block_hash_hex(block.hash),
                )
            })
            .collect()
    }
}

fn open_secondary(
    store_path: &Path,
    temporary_path: &Path,
    activations: &NetworkUpgradeActivations,
) -> Result<RocksDbCanonicalSecondary, Box<dyn Error>> {
    Ok(RocksDbCanonicalSecondary::open_ready(
        store_path,
        temporary_path.join("canonical-secondary"),
        activations,
        CanonicalStoreWorkload::Wallet,
        zinder_store::RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?,
        RocksDbResourceBudget::for_local_tests(),
    )?)
}

/// Deterministic replay of one prepared chain, without a node.
#[derive(Clone)]
struct StaticChainSource {
    blocks: Arc<Mutex<BTreeMap<BlockHeight, SourceBlock>>>,
    checkpoints: Arc<Mutex<BTreeMap<BlockHeight, CommitmentTreeCheckpoint>>>,
    tip_height: Arc<Mutex<BlockHeight>>,
}

impl StaticChainSource {
    fn new(
        blocks: BTreeMap<BlockHeight, SourceBlock>,
        activations: &NetworkUpgradeActivations,
    ) -> Result<Self, Box<dyn Error>> {
        let checkpoints = chain_checkpoints(&blocks, activations)?;
        Ok(Self {
            blocks: Arc::new(Mutex::new(blocks)),
            checkpoints: Arc::new(Mutex::new(checkpoints)),
            tip_height: Arc::new(Mutex::new(BlockHeight::new(CHAIN_TIP_HEIGHT))),
        })
    }

    fn set_tip_height(&self, height: BlockHeight) {
        *self.tip_height.lock() = height;
    }

    fn replace_chain(
        &self,
        blocks: BTreeMap<BlockHeight, SourceBlock>,
        activations: &NetworkUpgradeActivations,
    ) -> Result<(), Box<dyn Error>> {
        *self.checkpoints.lock() = chain_checkpoints(&blocks, activations)?;
        *self.blocks.lock() = blocks;
        Ok(())
    }

    fn block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.blocks
            .lock()
            .get(&height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "static chain has no block at the requested height".to_owned(),
            })
    }
}

#[async_trait]
impl NodeSource for StaticChainSource {
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
        let tip_height = *self.tip_height.lock();
        let blocks = self
            .blocks
            .lock()
            .range(start_height..=tip_height)
            .take(usize::try_from(limits.max_connected_blocks.get()).unwrap_or(usize::MAX))
            .map(|(_, block)| block.clone())
            .collect::<Vec<_>>();
        Ok(SourceChainSegment::connected_blocks(blocks))
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        _network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.checkpoints
            .lock()
            .get(&height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "static chain has no checkpoint at the requested height".to_owned(),
            })
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        let height = *self.tip_height.lock();
        Ok(BlockId::new(height, self.block_at(height)?.hash))
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

fn chain_checkpoints(
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    activations: &NetworkUpgradeActivations,
) -> Result<BTreeMap<BlockHeight, CommitmentTreeCheckpoint>, Box<dyn Error>> {
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

fn chained_blocks(
    tip_height: u32,
    time_nonce: u8,
) -> Result<BTreeMap<BlockHeight, SourceBlock>, Box<dyn Error>> {
    let fixture = fixture_source_block()?;
    let mut parent_hash = Network::ZcashRegtest.genesis_hash();
    let mut blocks = BTreeMap::new();
    for height in 1..=tip_height {
        let block = chained_block(
            &fixture,
            BlockHeight::new(height),
            parent_hash,
            if height > BASELINE_TIP_HEIGHT {
                time_nonce
            } else {
                0
            },
        )?;
        parent_hash = block.hash;
        blocks.insert(block.height, block);
    }
    Ok(blocks)
}

fn chained_block(
    fixture: &SourceBlock,
    height: BlockHeight,
    parent_hash: BlockHash,
    time_nonce: u8,
) -> Result<SourceBlock, Box<dyn Error>> {
    let mut raw_block_bytes = fixture.raw_block_bytes.clone();
    raw_block_bytes
        .get_mut(SERIALIZED_HEADER_PARENT_OFFSET..SERIALIZED_HEADER_PARENT_END)
        .ok_or("fixture block is too short to carry a parent hash")?
        .copy_from_slice(&parent_hash.as_bytes());
    let block_time = raw_block_bytes
        .get_mut(SERIALIZED_HEADER_TIME_OFFSET)
        .ok_or("fixture block is too short to carry a block time")?;
    *block_time = block_time.wrapping_add(time_nonce);
    let coinbase_height_opcode = raw_block_bytes
        .get_mut(COINBASE_HEIGHT_OPCODE_OFFSET)
        .ok_or("fixture block is too short to carry a coinbase height")?;
    *coinbase_height_opcode = 0x50_u8.saturating_add(u8::try_from(height.value())?);
    Ok(SourceBlock::from_raw_block_bytes(
        Network::ZcashRegtest,
        height,
        raw_block_bytes,
    )?)
}

fn block_at(
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    height: u32,
) -> Result<&SourceBlock, Box<dyn Error>> {
    blocks
        .get(&BlockHeight::new(height))
        .ok_or_else(|| format!("the chain must contain height {height}").into())
}

fn block_id(
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    height: u32,
) -> Result<BlockId, Box<dyn Error>> {
    Ok(BlockId::new(
        BlockHeight::new(height),
        block_at(blocks, height)?.hash,
    ))
}
