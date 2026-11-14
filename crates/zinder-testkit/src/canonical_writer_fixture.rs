//! Production canonical writer fixtures that publish, follow, reorg, and prune.
//!
//! [`CanonicalWriterFixture`] drives the release canonical writer API with
//! [`ChainFixture`] blocks: cold construction and baseline publication through
//! [`RocksDbCanonicalBuilder`], then live appends, live replacements, and event
//! pruning on the admitted [`RocksDbCanonicalStore`]. Wallet-serving tests turn
//! the result into a [`WalletServingStoreFixture`], so every artifact a query
//! reads was accepted by the same validation a deployed writer runs.
//!
//! Commitment trees follow the chain fixture's compact blocks. Each block's
//! tree sizes are its compact-block chain metadata. Live blocks derive their
//! frontiers by appending every note commitment in the compact block to the
//! persisted frontier, exactly as the store re-derives them. Cold baseline
//! blocks append their commitments only when they declare tree growth, or
//! take the frontiers declared with
//! [`ChainFixture::with_commitment_tree_frontiers_at`]; a cold build does not
//! authenticate note commitments against tree positions, so compact blocks
//! that carry commitments without declaring growth keep empty trees.

use std::{convert::Infallible, path::PathBuf, sync::Arc};

use eyre::{Context as _, ensure, eyre};
use tempfile::TempDir;
use zinder_core::{
    BlockHeight, BlockId, CanonicalBlockReplayEnvelope, ChainTipMetadata,
    CommitmentTreeAccumulator, CommitmentTreeCheckpoint, CommitmentTreeFrontiers,
    CompactBlockArtifact, NetworkUpgradeActivations, UnixTimestampMillis,
    decode_canonical_block_replay,
};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalBuildSubtreeRoot,
    CanonicalEventFence, CanonicalEventRetentionReport, CanonicalLiveAppend,
    CanonicalLiveReplacement, CanonicalReorgPolicy, CanonicalReplacementBlock,
    CanonicalStoreBuildPlan, CanonicalStoreWorkload, RocksDbCanonicalBuilder,
    RocksDbCanonicalStore, RocksDbResourceBudget, TREE_STATE_CHECKPOINT_STRIDE,
};

use crate::{ChainFixture, FixtureBlock, FixtureTransactionRows, WalletServingStoreFixture};

/// Replacement depth, in blocks, of every store built by these fixtures.
const TEST_REORG_DEPTH: u32 = 100;

/// Shape of the first canonical publication built from a [`ChainFixture`].
///
/// The default publishes complete history from height 1 through the chain
/// tip, settles the tip, and loads no subtree roots.
#[derive(Clone, Debug)]
pub struct CanonicalBaseline<'chain> {
    chain: &'chain ChainFixture,
    tip_height: Option<BlockHeight>,
    history_floor: Option<BlockHeight>,
    settled_tip_height: Option<BlockHeight>,
    subtree_roots: Vec<CanonicalBuildSubtreeRoot>,
    checkpoint_frontiers: CheckpointFrontiers,
}

impl<'chain> CanonicalBaseline<'chain> {
    /// Publishes every block of `chain` as complete canonical history.
    #[must_use]
    pub const fn new(chain: &'chain ChainFixture) -> Self {
        Self {
            chain,
            tip_height: None,
            history_floor: None,
            settled_tip_height: None,
            subtree_roots: Vec::new(),
            checkpoint_frontiers: CheckpointFrontiers::ActivePools,
        }
    }

    /// Publishes blocks only through `tip_height`, leaving later blocks of the
    /// chain for [`CanonicalWriterFixture::append_live_block`].
    #[must_use]
    pub const fn through(mut self, tip_height: BlockHeight) -> Self {
        self.tip_height = Some(tip_height);
        self
    }

    /// Publishes checkpointed history whose authenticated predecessor is the
    /// block at `floor_height`.
    ///
    /// The store retains blocks from `floor_height + 1`; the floor block's
    /// commitment trees become the build plan's predecessor checkpoint.
    #[must_use]
    pub const fn with_history_floor(mut self, floor_height: BlockHeight) -> Self {
        self.history_floor = Some(floor_height);
        self
    }

    /// Settles the baseline at `settled_height` instead of its tip, leaving the
    /// blocks above it replaceable by [`CanonicalWriterFixture::replace_live_blocks`].
    #[must_use]
    pub const fn with_settled_tip(mut self, settled_height: BlockHeight) -> Self {
        self.settled_tip_height = Some(settled_height);
        self
    }

    /// Loads one source-authenticated subtree root completed by a retained block.
    ///
    /// Roots must be added in protocol order (Sapling, Orchard, Ironwood) and
    /// index order, and cover every subtree the retained blocks complete.
    #[must_use]
    pub fn with_subtree_root(mut self, subtree_root: CanonicalBuildSubtreeRoot) -> Self {
        self.subtree_roots.push(subtree_root);
        self
    }

    /// Omits the frontier of every pool whose tree is still empty from each
    /// checkpoint, which a cold baseline publication accepts.
    ///
    /// Live following cannot seed from such a checkpoint, so a writer built
    /// this way only serves its baseline.
    #[must_use]
    pub(crate) const fn omitting_empty_frontiers(mut self) -> Self {
        self.checkpoint_frontiers = CheckpointFrontiers::NonEmptyPools;
        self
    }
}

/// Pools whose frontier a fixture checkpoint carries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckpointFrontiers {
    /// Every pool active at the checkpoint height, as live following requires.
    ActivePools,
    /// Only pools with at least one note commitment.
    NonEmptyPools,
}

/// A READY production canonical primary built from [`ChainFixture`] blocks.
///
/// The fixture owns its temporary directory. Each serving fixture it hands out
/// shares that directory, so readers stay valid after the writer is dropped.
pub struct CanonicalWriterFixture {
    canonical_primary: Option<RocksDbCanonicalStore>,
    canonical_primary_path: PathBuf,
    network_upgrade_activations: NetworkUpgradeActivations,
    serving_fixture_count: u32,
    temporary_directory: Arc<TempDir>,
}

impl CanonicalWriterFixture {
    /// Builds, cold-certifies, and publishes `baseline` as epoch 1.
    ///
    /// # Errors
    ///
    /// Returns an error when the baseline shape is inconsistent with its
    /// chain, a declared commitment tree disagrees with its compact block, or
    /// the production builder rejects the construction or publication.
    pub fn publish(
        baseline: &CanonicalBaseline<'_>,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> eyre::Result<Self> {
        ensure!(
            baseline.chain.network() == network_upgrade_activations.network(),
            "chain fixture and network-upgrade activations must use the same network"
        );
        let artifacts = FixtureChainArtifacts::new(baseline.chain);
        let tip_height = baseline
            .tip_height
            .or_else(|| artifacts.chain.tip_height())
            .ok_or_else(|| eyre!("a canonical baseline requires at least one block"))?;
        let tip_block = artifacts.fixture_block(tip_height)?;
        let tip = BlockId::new(tip_height, tip_block.hash);
        let mut trees = FixtureCommitmentTrees::after(
            BlockHeight::new(0),
            &CommitmentTreeFrontiers::default(),
            network_upgrade_activations,
        )?;
        let (build_plan, first_retained_height) =
            baseline_build_plan(&artifacts, baseline, tip, &mut trees)?;
        let temporary_directory =
            TempDir::new().wrap_err("create canonical writer fixture directory")?;
        let canonical_primary_path = temporary_directory.path().join("canonical-primary");
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            &canonical_primary_path,
            CanonicalStoreWorkload::Wallet,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let blocks = baseline_blocks(
            &artifacts,
            &mut trees,
            BaselineRange {
                first_height: first_retained_height,
                tip_height,
                checkpoint_frontiers: baseline.checkpoint_frontiers,
            },
        )?;
        let tip_checkpoint = blocks
            .last()
            .and_then(|block| block.tree_state_checkpoint.clone())
            .ok_or_else(|| eyre!("the baseline tip must carry a tree-state checkpoint"))?;
        builder.bulk_load_blocks(blocks.into_iter().map(Ok::<_, Infallible>))?;
        builder.load_subtree_roots(baseline.subtree_roots.iter().copied())?;
        builder.confirm_source_tip_checkpoint(&tip_checkpoint)?;
        let validated = builder.prepare_cold_certified_publication()?;
        let settled_height = baseline.settled_tip_height.unwrap_or(tip_height);
        let settled_block = artifacts.fixture_block(settled_height)?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            BlockId::new(settled_height, settled_block.hash),
            fixture_created_at(tip_block),
        ))?;
        let canonical_primary = validated.publish_baseline(publication)?;

        Ok(Self {
            canonical_primary: Some(canonical_primary),
            canonical_primary_path,
            network_upgrade_activations: network_upgrade_activations.clone(),
            serving_fixture_count: 0,
            temporary_directory: Arc::new(temporary_directory),
        })
    }

    /// Returns the filesystem path of the canonical primary, for tests that
    /// open their own secondary over it.
    #[must_use]
    pub fn canonical_primary_path(&self) -> &std::path::Path {
        &self.canonical_primary_path
    }

    /// Returns the admitted canonical primary.
    ///
    /// # Errors
    ///
    /// Returns an error if an earlier transition consumed the primary and failed.
    pub fn canonical_primary(&self) -> eyre::Result<&RocksDbCanonicalStore> {
        self.canonical_primary
            .as_ref()
            .ok_or_else(|| eyre!("an earlier canonical transition failed and closed the primary"))
    }

    /// Admits `activations` as the writer's next network-upgrade schedule
    /// revision.
    ///
    /// Later live transitions commit with that table, and serving fixtures
    /// open their canonical readers with it, as a serving publisher does after
    /// it follows writer status.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer refuses the schedule revision.
    pub fn admit_network_upgrade_schedule(
        &mut self,
        activations: &NetworkUpgradeActivations,
    ) -> eyre::Result<()> {
        self.canonical_primary
            .as_mut()
            .ok_or_else(|| eyre!("an earlier canonical transition failed and closed the primary"))?
            .admit_network_upgrade_schedule(activations)?;
        self.network_upgrade_activations = activations.clone();
        Ok(())
    }

    /// Appends the block of `chain` immediately above the visible tip.
    ///
    /// The settled tip stays where it is, so the appended block remains
    /// replaceable.
    ///
    /// # Errors
    ///
    /// Returns an error when `chain` has no block at the next height, its
    /// compact block does not advance the persisted commitment trees to its
    /// declared sizes, or the store rejects the append.
    pub fn append_live_block(&mut self, chain: &ChainFixture) -> eyre::Result<CanonicalEventFence> {
        let canonical_primary = self.take_canonical_primary()?;
        let anchor = canonical_primary.append_anchor()?;
        let height = anchor
            .event_fence()
            .visible_tip()
            .height
            .next()
            .ok_or_else(|| eyre!("the visible tip height cannot advance"))?;
        let artifacts = FixtureChainArtifacts::new(chain);
        let mut trees = FixtureCommitmentTrees::after(
            anchor.tip_checkpoint().block_id.height,
            &anchor.tip_checkpoint().frontiers,
            &self.network_upgrade_activations,
        )?;
        let block = artifacts.live_block(height, &mut trees)?;
        let created_at = fixture_created_at(artifacts.fixture_block(height)?);
        let (canonical_primary, fence) = canonical_primary.commit_live_append(
            CanonicalLiveAppend::new(
                anchor.event_fence(),
                block,
                Vec::new(),
                anchor.settled_tip(),
                created_at,
            ),
            &self.network_upgrade_activations,
        )?;
        self.canonical_primary = Some(canonical_primary);
        Ok(fence)
    }

    /// Replaces the canonical suffix from `first_replaced_height` with the
    /// blocks of `replacement_chain` from that height through its tip.
    ///
    /// `replacement_chain` must share the block below `first_replaced_height`
    /// with the store, as a fork built with [`ChainFixture::fork_at`] does.
    /// Every displaced block must start with a coinbase, such as
    /// [`FixtureTransactionRows::coinbase`], because the store archives it.
    ///
    /// # Errors
    ///
    /// Returns an error when the replacement range is empty, its parent is not
    /// replaceable, or the store rejects the replacement.
    pub fn replace_live_blocks(
        &mut self,
        replacement_chain: &ChainFixture,
        first_replaced_height: BlockHeight,
    ) -> eyre::Result<CanonicalEventFence> {
        let canonical_primary = self.take_canonical_primary()?;
        let expected_fence = canonical_primary.event_fence();
        let artifacts = FixtureChainArtifacts::new(replacement_chain);
        let parent_height = BlockHeight::new(
            first_replaced_height
                .value()
                .checked_sub(1)
                .ok_or_else(|| eyre!("canonical replacement cannot begin at height zero"))?,
        );
        let parent = BlockId::new(parent_height, artifacts.fixture_block(parent_height)?.hash);
        let parent_checkpoint = canonical_primary
            .replacement_parent_checkpoint(parent, &self.network_upgrade_activations)?;
        let mut trees = FixtureCommitmentTrees::after(
            parent_height,
            &parent_checkpoint.frontiers,
            &self.network_upgrade_activations,
        )?;
        let tip_height = artifacts
            .chain
            .tip_height()
            .filter(|tip_height| *tip_height >= first_replaced_height)
            .ok_or_else(|| eyre!("the replacement chain has no block to replace with"))?;
        let mut blocks = Vec::new();
        for height in first_replaced_height.value()..=tip_height.value() {
            blocks.push(CanonicalReplacementBlock::new(
                artifacts.live_block(BlockHeight::new(height), &mut trees)?,
                Vec::new(),
            ));
        }
        let created_at = fixture_created_at(artifacts.fixture_block(tip_height)?);
        let (canonical_primary, fence) = canonical_primary.commit_live_replacement(
            CanonicalLiveReplacement::new(expected_fence, blocks, created_at),
            &self.network_upgrade_activations,
        )?;
        self.canonical_primary = Some(canonical_primary);
        Ok(fence)
    }

    /// Prunes retained canonical events below `oldest_retained_sequence`.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested floor is outside retained history.
    pub fn prune_canonical_events_before(
        &self,
        oldest_retained_sequence: u64,
    ) -> eyre::Result<CanonicalEventRetentionReport> {
        Ok(self
            .canonical_primary()?
            .prune_canonical_events_before(oldest_retained_sequence, UnixTimestampMillis::now())?)
    }

    /// Projects a wallet store at the current fence and admits immutable
    /// readers beside the still-open canonical primary.
    ///
    /// Later transitions do not move the returned readers, so a test can hold
    /// a pair from before a transition and request another after it.
    ///
    /// # Errors
    ///
    /// Returns an error when the wallet projection or either reader admission fails.
    pub fn serving_store_fixture(&mut self) -> eyre::Result<WalletServingStoreFixture> {
        self.serving_fixture_count = self.serving_fixture_count.saturating_add(1);
        let canonical_primary = self.canonical_primary.as_ref().ok_or_else(|| {
            eyre!("an earlier canonical transition failed and closed the primary")
        })?;
        WalletServingStoreFixture::admit_beside_open_primary(
            Arc::clone(&self.temporary_directory),
            self.serving_fixture_count,
            &self.canonical_primary_path,
            canonical_primary,
            &self.network_upgrade_activations,
        )
    }

    /// Closes the canonical primary, then projects the wallet store and admits
    /// immutable readers at its final fence.
    ///
    /// # Errors
    ///
    /// Returns an error when the wallet projection or either reader admission fails.
    pub fn into_serving_store_fixture(mut self) -> eyre::Result<WalletServingStoreFixture> {
        let canonical_primary = self.take_canonical_primary()?;
        WalletServingStoreFixture::admit_closing_primary(
            self.temporary_directory,
            &self.canonical_primary_path,
            canonical_primary,
            &self.network_upgrade_activations,
        )
    }

    fn take_canonical_primary(&mut self) -> eyre::Result<RocksDbCanonicalStore> {
        self.canonical_primary
            .take()
            .ok_or_else(|| eyre!("an earlier canonical transition failed and closed the primary"))
    }
}

/// Retained-height range of one cold baseline.
#[derive(Clone, Copy)]
struct BaselineRange {
    first_height: BlockHeight,
    tip_height: BlockHeight,
    checkpoint_frontiers: CheckpointFrontiers,
}

/// Builds the plan for `baseline` and advances `trees` through its predecessor.
///
/// Returns the plan and the first retained height.
fn baseline_build_plan(
    artifacts: &FixtureChainArtifacts,
    baseline: &CanonicalBaseline<'_>,
    tip: BlockId,
    trees: &mut FixtureCommitmentTrees<'_>,
) -> eyre::Result<(CanonicalStoreBuildPlan, BlockHeight)> {
    let raw_blob_retention = artifacts.chain.raw_blob_retention();
    let reorg_policy = CanonicalReorgPolicy::new(TEST_REORG_DEPTH)?;
    let Some(floor_height) = baseline.history_floor else {
        let first_block = artifacts.fixture_block(BlockHeight::new(1))?;
        let build_plan = CanonicalStoreBuildPlan::complete(
            trees.activations,
            first_block.block_time_seconds.saturating_sub(1),
            tip,
            raw_blob_retention,
            reorg_policy,
        )?;
        return Ok((build_plan, BlockHeight::new(1)));
    };
    ensure!(
        floor_height.value() > 0 && floor_height < tip.height,
        "a history floor must lie strictly between genesis and the baseline tip"
    );
    for height in 1..=floor_height.value() {
        let height = BlockHeight::new(height);
        trees.apply_cold(&artifacts.chain, height, artifacts.compact_block(height)?)?;
    }
    let floor_block = artifacts.fixture_block(floor_height)?;
    let predecessor = CommitmentTreeCheckpoint::new(
        BlockId::new(floor_height, floor_block.hash),
        floor_block.block_time_seconds,
        trees.frontiers(CheckpointFrontiers::ActivePools)?,
    );
    let build_plan = CanonicalStoreBuildPlan::checkpointed(
        trees.activations,
        predecessor,
        tip,
        raw_blob_retention,
        reorg_policy,
    )?;
    let first_retained_height = floor_height
        .next()
        .ok_or_else(|| eyre!("the history floor height cannot advance"))?;
    Ok((build_plan, first_retained_height))
}

fn baseline_blocks(
    artifacts: &FixtureChainArtifacts,
    trees: &mut FixtureCommitmentTrees<'_>,
    range: BaselineRange,
) -> eyre::Result<Vec<CanonicalBuildBlock>> {
    let mut blocks = Vec::new();
    for height in range.first_height.value()..=range.tip_height.value() {
        let height = BlockHeight::new(height);
        let tip_metadata =
            trees.apply_cold(&artifacts.chain, height, artifacts.compact_block(height)?)?;
        let checkpoint_required = height == range.tip_height
            || height.value().is_multiple_of(TREE_STATE_CHECKPOINT_STRIDE);
        let checkpoint_frontiers = checkpoint_required
            .then(|| trees.frontiers(range.checkpoint_frontiers))
            .transpose()?;
        blocks.push(artifacts.build_block(height, tip_metadata, checkpoint_frontiers)?);
    }
    Ok(blocks)
}

fn fixture_created_at(block: &FixtureBlock) -> UnixTimestampMillis {
    UnixTimestampMillis::new(u64::from(block.block_time_seconds).saturating_mul(1_000))
}

/// Every per-block artifact of one chain fixture, derived once.
struct FixtureChainArtifacts {
    chain: ChainFixture,
    transaction_rows: Vec<FixtureTransactionRows>,
    replay_envelopes: Vec<CanonicalBlockReplayEnvelope>,
    compact_blocks: Vec<CompactBlockArtifact>,
}

impl FixtureChainArtifacts {
    fn new(chain: &ChainFixture) -> Self {
        let chain = chain.clone().with_canonical_genesis_parent();
        Self {
            transaction_rows: chain.canonical_transaction_rows(),
            replay_envelopes: chain.block_replay_envelopes(),
            compact_blocks: chain.compact_block_artifacts(),
            chain,
        }
    }

    fn block_index(&self, height: BlockHeight) -> eyre::Result<usize> {
        self.chain
            .blocks()
            .iter()
            .position(|block| block.height == height)
            .ok_or_else(|| eyre!("chain fixture has no block at height {}", height.value()))
    }

    fn fixture_block(&self, height: BlockHeight) -> eyre::Result<&FixtureBlock> {
        let index = self.block_index(height)?;
        self.chain
            .blocks()
            .get(index)
            .ok_or_else(|| eyre!("chain fixture has no block at height {}", height.value()))
    }

    fn compact_block(&self, height: BlockHeight) -> eyre::Result<&CompactBlockArtifact> {
        let index = self.block_index(height)?;
        self.compact_blocks
            .get(index)
            .ok_or_else(|| eyre!("chain fixture has no compact block at {}", height.value()))
    }

    /// Builds one live or replacement block with its exact transition checkpoint.
    fn live_block(
        &self,
        height: BlockHeight,
        trees: &mut FixtureCommitmentTrees<'_>,
    ) -> eyre::Result<CanonicalBuildBlock> {
        let tip_metadata = trees.apply_live(&self.chain, height, self.compact_block(height)?)?;
        let frontiers = trees.frontiers(CheckpointFrontiers::ActivePools)?;
        self.build_block(height, tip_metadata, Some(frontiers))
    }

    fn build_block(
        &self,
        height: BlockHeight,
        tip_metadata: ChainTipMetadata,
        checkpoint_frontiers: Option<CommitmentTreeFrontiers>,
    ) -> eyre::Result<CanonicalBuildBlock> {
        let index = self.block_index(height)?;
        let fixture_block = self.fixture_block(height)?;
        let replay_envelope =
            self.replay_envelopes.get(index).cloned().ok_or_else(|| {
                eyre!("chain fixture has no replay envelope at {}", height.value())
            })?;
        let compact_block = self.compact_block(height)?.clone();
        let facts = decode_canonical_block_replay(replay_envelope.as_bytes())
            .wrap_err_with(|| {
                format!(
                    "decode canonical fixture replay at height {}",
                    height.value()
                )
            })?
            .into_facts();
        let raw_blob_retention = self.chain.raw_blob_retention();
        let transaction_blobs = if raw_blob_retention.retains_transaction_blobs() {
            self.block_transaction_blobs(fixture_block)?
        } else {
            Vec::new()
        };

        Ok(CanonicalBuildBlock {
            facts,
            replay_envelope,
            compact_block,
            tip_metadata,
            tree_state_checkpoint: checkpoint_frontiers.map(|frontiers| {
                CommitmentTreeCheckpoint::new(
                    BlockId::new(height, fixture_block.hash),
                    fixture_block.block_time_seconds,
                    frontiers,
                )
            }),
            block_final_note_commitment_roots: None,
            transaction_blobs,
            block_blob: raw_blob_retention
                .retains_block_blobs()
                .then(|| fixture_block.block_blob_artifact()),
        })
    }

    fn block_transaction_blobs(
        &self,
        fixture_block: &FixtureBlock,
    ) -> eyre::Result<Vec<zinder_core::TransactionBlobArtifact>> {
        let mut block_transaction_rows = self
            .transaction_rows
            .iter()
            .filter(|rows| {
                rows.location.block_height == fixture_block.height
                    && rows.location.block_hash == fixture_block.hash
            })
            .collect::<Vec<_>>();
        block_transaction_rows.sort_by_key(|rows| rows.location.tx_index_in_block);
        block_transaction_rows
            .into_iter()
            .map(|rows| {
                rows.blob.clone().ok_or_else(|| {
                    eyre!(
                        "production canonical fixture transaction {:?} has no raw blob",
                        rows.location.transaction_id
                    )
                })
            })
            .collect()
    }
}

/// Commitment trees threaded through consecutive fixture blocks.
struct FixtureCommitmentTrees<'activations> {
    accumulator: CommitmentTreeAccumulator,
    activations: &'activations NetworkUpgradeActivations,
}

impl<'activations> FixtureCommitmentTrees<'activations> {
    fn after(
        height: BlockHeight,
        frontiers: &CommitmentTreeFrontiers,
        activations: &'activations NetworkUpgradeActivations,
    ) -> eyre::Result<Self> {
        Ok(Self {
            accumulator: CommitmentTreeAccumulator::from_validated_frontiers(
                height,
                frontiers,
                activations,
            )
            .map_err(|source| {
                eyre!(
                    "commitment trees after height {} cannot seed the fixture: {source}",
                    height.value()
                )
            })?,
            activations,
        })
    }

    /// Applies one cold baseline block and returns its declared tree sizes.
    fn apply_cold(
        &mut self,
        chain: &ChainFixture,
        height: BlockHeight,
        compact_block: &CompactBlockArtifact,
    ) -> eyre::Result<ChainTipMetadata> {
        let declared = declared_tip_metadata(compact_block);
        if let Some(frontiers) = chain.commitment_tree_frontiers_at(height) {
            ensure!(
                frontiers.tip_metadata() == declared,
                "declared commitment trees at height {} disagree with the compact block's chain metadata",
                height.value()
            );
            *self = Self::after(height, frontiers, self.activations)?;
            return Ok(declared);
        }
        if declared == self.accumulator.tip_metadata() {
            self.append(height, &CompactNoteCommitments::default())?;
        } else {
            self.append(height, &CompactNoteCommitments::of(compact_block))?;
        }
        ensure!(
            self.accumulator.tip_metadata() == declared,
            "compact block at height {} declares tree sizes its note commitments do not reach",
            height.value()
        );
        Ok(declared)
    }

    /// Applies one live block's note commitments, as the store re-derives them.
    fn apply_live(
        &mut self,
        chain: &ChainFixture,
        height: BlockHeight,
        compact_block: &CompactBlockArtifact,
    ) -> eyre::Result<ChainTipMetadata> {
        ensure!(
            chain.commitment_tree_frontiers_at(height).is_none(),
            "live block {} cannot declare its commitment trees; the store derives them",
            height.value()
        );
        self.append(height, &CompactNoteCommitments::of(compact_block))?;
        let declared = declared_tip_metadata(compact_block);
        ensure!(
            self.accumulator.tip_metadata() == declared,
            "live block {} note commitments do not reach its declared tree sizes",
            height.value()
        );
        Ok(declared)
    }

    fn append(
        &mut self,
        height: BlockHeight,
        commitments: &CompactNoteCommitments,
    ) -> eyre::Result<()> {
        self.accumulator
            .append_block_commitments(
                height,
                &commitments.sapling,
                &commitments.orchard,
                &commitments.ironwood,
            )
            .map_err(|source| {
                eyre!(
                    "note commitments at height {} cannot advance the fixture trees: {source}",
                    height.value()
                )
            })
    }

    fn frontiers(
        &mut self,
        presence: CheckpointFrontiers,
    ) -> eyre::Result<CommitmentTreeFrontiers> {
        let frontiers = self
            .accumulator
            .validated_frontiers()
            .map_err(|source| eyre!("fixture commitment trees cannot be checkpointed: {source}"))?;
        Ok(match presence {
            CheckpointFrontiers::ActivePools => frontiers,
            CheckpointFrontiers::NonEmptyPools => {
                let non_empty = |frontier: Option<&zinder_core::CommitmentTreeFrontier>| {
                    frontier
                        .filter(|frontier| frontier.tree_size() > 0)
                        .cloned()
                };
                CommitmentTreeFrontiers::from_validated_parts(
                    non_empty(frontiers.sapling()),
                    non_empty(frontiers.orchard()),
                    non_empty(frontiers.ironwood()),
                )
            }
        })
    }
}

fn declared_tip_metadata(compact_block: &CompactBlockArtifact) -> ChainTipMetadata {
    let metadata = compact_block.chain_metadata();
    ChainTipMetadata::new(
        metadata.sapling_commitment_tree_size,
        metadata.orchard_commitment_tree_size,
        metadata.ironwood_commitment_tree_size,
    )
}

/// Note commitments of one compact block, in wire order.
#[derive(Default)]
struct CompactNoteCommitments {
    sapling: Vec<[u8; 32]>,
    orchard: Vec<[u8; 32]>,
    ironwood: Vec<[u8; 32]>,
}

impl CompactNoteCommitments {
    fn of(compact_block: &CompactBlockArtifact) -> Self {
        let mut commitments = Self::default();
        for transaction in compact_block.transactions() {
            for output in &transaction.data.sapling_outputs {
                commitments.sapling.push(output.commitment);
            }
            for action in &transaction.data.orchard_actions {
                commitments.orchard.push(action.commitment);
            }
            for action in &transaction.data.ironwood_actions {
                commitments.ironwood.push(action.commitment);
            }
        }
        commitments
    }
}
