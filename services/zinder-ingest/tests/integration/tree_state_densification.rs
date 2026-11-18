//! The tree-state densification pass over a constructed store whose blocks
//! carry note commitments.
//!
//! The baseline publishes 360 blocks with a checkpoint every 100 blocks and at
//! its tip, and settles at the tip, so the strides ending at 100, 200, and 300
//! are settled and the one ending at 400 is not. Expected trees come from
//! appending every block's commitments from genesis, independently of any
//! stored checkpoint.

use std::{
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use zinder_core::{BlockHeight, CommitmentTreeCheckpoint, Network, NetworkUpgradeActivations};
use zinder_ingest::{
    HistoricalWorkGate, TreeStateDensificationConfig, TreeStateDensificationContext,
    TreeStateStrideWriteError, TreeStateStrideWriter, spawn_tree_state_densification_task,
};
use zinder_query::{
    WalletQueryApi, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_runtime::{ComposedReadiness, IngestPhase, Readiness};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreError, CanonicalStoreWorkload, DensifiedTreeStateStride,
    RawBlobRetention, RocksDbCanonicalSecondary, RocksDbCanonicalStore, RocksDbResourceBudget,
    TreeStateCheckpointSelection, TreeStateStrideCommit,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, commitment_tree_checkpoints,
    sample_regtest_upgrade_activations, with_note_commitments,
};

const BASELINE_TIP: u32 = 360;
const CHAIN_TIP: u32 = 380;
const DENSIFIED_THROUGH: u32 = 300;
const TEST_REORG_WINDOW: u32 = 100;
const PASS_DEADLINE: Duration = Duration::from_mins(2);
const EVERY_BLOCK: TreeStateCheckpointSelection = TreeStateCheckpointSelection::EveryBlock;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_block_pass_stores_the_tree_state_of_every_settled_height() -> eyre::Result<()> {
    let harness = Harness::new()?;

    harness
        .run_pass_to_completion(EVERY_BLOCK, &Hooks::default())
        .await?;

    harness.with_primary(|primary| {
        for height in 0..=DENSIFIED_THROUGH {
            eyre::ensure!(
                stored_checkpoint_heights(primary, height, height)?.len() == 1,
                "height {height} has no stored checkpoint"
            );
        }
        eyre::ensure!(
            stored_checkpoint_heights(primary, DENSIFIED_THROUGH + 1, BASELINE_TIP)?
                == vec![BASELINE_TIP],
            "the unsettled stride must keep only its construction checkpoint"
        );
        Ok(())
    })?;
    harness.assert_reads_match_the_chain(1..=BASELINE_TIP)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commitment_budget_pass_stores_a_checkpoint_where_each_budget_fills() -> eyre::Result<()> {
    let harness = Harness::new()?;
    let budget = 20_u64;
    let selection = TreeStateCheckpointSelection::CommitmentBudget(
        NonZeroU32::new(u32::try_from(budget)?).ok_or_else(|| eyre::eyre!("zero budget"))?,
    );

    harness
        .run_pass_to_completion(selection, &Hooks::default())
        .await?;

    harness.with_primary(|primary| {
        for stride_start in (0..DENSIFIED_THROUGH).step_by(100) {
            let rows = stored_checkpoint_heights(primary, stride_start, stride_start + 100)?;
            eyre::ensure!(
                rows.len() > 2,
                "stride {stride_start} gained no checkpoints"
            );
            for pair in rows.windows(2) {
                let (previous, next) = (pair[0], pair[1]);
                let before_next = commitments_through(primary, next - 1)?
                    - commitments_through(primary, previous)?;
                eyre::ensure!(
                    before_next < budget,
                    "checkpoints {previous} and {next} leave {before_next} commitments unanchored"
                );
                if next % 100 != 0 {
                    eyre::ensure!(
                        commitments_through(primary, next)?
                            - commitments_through(primary, previous)?
                            >= budget,
                        "checkpoint {next} was stored before its budget filled"
                    );
                }
            }
        }
        Ok(())
    })?;
    harness.assert_reads_match_the_chain(1..=DENSIFIED_THROUGH)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_every_block_pass_completes_strides_an_earlier_budget_pass_densified() -> eyre::Result<()>
{
    let harness = Harness::new()?;
    let budget = TreeStateCheckpointSelection::CommitmentBudget(
        NonZeroU32::new(20).ok_or_else(|| eyre::eyre!("zero budget"))?,
    );
    harness
        .run_pass_to_completion(budget, &Hooks::default())
        .await?;

    let commits = harness
        .run_pass_to_completion(EVERY_BLOCK, &Hooks::default())
        .await?;

    eyre::ensure!(
        commits.len() == 3,
        "{} strides were completed",
        commits.len()
    );
    eyre::ensure!(
        commits
            .iter()
            .all(|commit| commit.rows_already_present == 0 && commit.rows_written > 0),
        "the budget rows must be kept, not rewritten: {commits:?}"
    );
    harness.with_primary(|primary| {
        eyre::ensure!(stored_checkpoint_heights(primary, 0, DENSIFIED_THROUGH)?.len() == 301);
        Ok(())
    })?;
    harness.assert_reads_match_the_chain(1..=DENSIFIED_THROUGH)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_history_predecessor_off_the_cadence_seeds_the_first_stride() -> eyre::Result<()> {
    let harness = Harness::with_history_floor(130)?;

    let commits = harness
        .run_pass_to_completion(EVERY_BLOCK, &Hooks::default())
        .await?;

    eyre::ensure!(
        commits
            .iter()
            .map(|commit| commit.rows_written)
            .collect::<Vec<_>>()
            == vec![69, 99],
        "the strides from 130 and from 200 must be densified: {commits:?}"
    );
    harness.with_primary(|primary| {
        eyre::ensure!(stored_checkpoint_heights(primary, 130, DENSIFIED_THROUGH)?.len() == 171);
        Ok(())
    })?;
    harness.assert_reads_match_the_chain(131..=DENSIFIED_THROUGH)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pass_stopped_after_its_first_commit_resumes_without_rewriting_a_row() -> eyre::Result<()>
{
    let harness = Harness::new()?;
    let first_run = CancellationToken::new();
    let hooks = Hooks {
        cancel_after_commits: Some((1, first_run.clone())),
        ..Hooks::default()
    };

    let commits = harness.run_pass(EVERY_BLOCK, &hooks, first_run).await?;
    eyre::ensure!(
        commits.len() == 1,
        "the first run committed {} strides",
        commits.len()
    );
    let resumed = harness
        .run_pass_to_completion(EVERY_BLOCK, &Hooks::default())
        .await?;

    let mut all = commits;
    all.extend(resumed);
    eyre::ensure!(
        all.len() == 3,
        "the strides were committed {} times",
        all.len()
    );
    eyre::ensure!(
        all.iter()
            .all(|commit| commit.rows_written == 99 && commit.rows_already_present == 0),
        "a resumed stride rewrote or skipped rows: {all:?}"
    );
    harness.assert_reads_match_the_chain(1..=DENSIFIED_THROUGH)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stride_the_writer_refuses_as_contradictory_is_skipped_and_the_rest_densify()
-> eyre::Result<()> {
    let harness = Harness::new()?;
    let hooks = Hooks {
        refuse_seed_height: Some(100),
        ..Hooks::default()
    };

    harness.run_pass_to_completion(EVERY_BLOCK, &hooks).await?;

    harness.with_primary(|primary| {
        eyre::ensure!(stored_checkpoint_heights(primary, 0, 100)?.len() == 101);
        eyre::ensure!(
            stored_checkpoint_heights(primary, 100, 200)? == vec![100, 200],
            "a refused stride must keep only its construction checkpoints"
        );
        eyre::ensure!(stored_checkpoint_heights(primary, 200, 300)?.len() == 101);
        Ok(())
    })?;
    harness.assert_reads_match_the_chain(1..=DENSIFIED_THROUGH)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_appends_between_stride_commits_keep_both_histories_exact() -> eyre::Result<()> {
    let harness = Harness::new()?;
    let hooks = Hooks {
        append_live_blocks_before_commit: true,
        ..Hooks::default()
    };

    let commits = harness.run_pass_to_completion(EVERY_BLOCK, &hooks).await?;

    eyre::ensure!(commits.len() == 3);
    harness.with_primary(|primary| {
        let visible_tip = primary.event_fence().visible_tip().height.value();
        eyre::ensure!(
            visible_tip == BASELINE_TIP + 3,
            "each commit should follow one live append, visible tip is {visible_tip}"
        );
        eyre::ensure!(
            stored_checkpoint_heights(primary, BASELINE_TIP, visible_tip)?.len() == 4,
            "live appends must keep their per-block checkpoints"
        );
        Ok(())
    })?;
    harness.assert_reads_match_the_chain(1..=BASELINE_TIP + 3)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_runtime_admits_and_serves_a_densified_store() -> eyre::Result<()> {
    let harness = Harness::new()?;
    harness
        .run_pass_to_completion(EVERY_BLOCK, &Hooks::default())
        .await?;
    let primary_path = harness.primary_path()?;
    let expected = harness.expected.clone();
    let activations = harness.activations.clone();
    // The projector projects the wallet store from the densified primary and
    // the serving readers admit both stores, as the serving pair does.
    let mut serving = harness.into_serving_store_fixture()?;
    let (canonical_reader, wallet_reader) = serving.take_readers()?;
    let query = WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(Arc::new(
        WalletServingReadPair::new(Arc::new(canonical_reader), Arc::new(wallet_reader))?,
    )));
    for height in [1, 57, 150, 299, 300] {
        let tree_state = query.tree_state_at(BlockHeight::new(height), None).await?;
        eyre::ensure!(tree_state.height == BlockHeight::new(height));
        eyre::ensure!(
            tree_state.block_hash == expected_at(&expected, height)?.block_id.hash,
            "query served another block's tree state at {height}"
        );
    }

    // The ingest writer reopens the primary with the node's activations.
    let primary = RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
        &primary_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::None,
        CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    eyre::ensure!(
        primary
            .next_tree_state_stride(BlockHeight::new(0), EVERY_BLOCK)?
            .is_none(),
        "a reopened writer must see every settled stride densified"
    );
    // Compatibility, the projector, and query open secondaries this way.
    let secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        &primary_path,
        primary_path.with_extension("compat-secondary"),
        Network::ZcashRegtest,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::None,
        CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    for height in [1, 99, 299] {
        eyre::ensure!(
            secondary
                .tree_state_checkpoint_at(BlockHeight::new(height), &activations)?
                .as_ref()
                == Some(expected_at(&expected, height)?),
            "secondary tree state at {height}"
        );
        // Query answers a height whose nearest stored checkpoint is that
        // height before it takes a replay slot.
        eyre::ensure!(
            secondary
                .tree_state_checkpoint_at_or_before(BlockHeight::new(height))?
                .is_some_and(|stored| stored.block_id.height == BlockHeight::new(height)),
            "height {height} is not an exact stored checkpoint"
        );
    }
    drop(serving);
    Ok(())
}

/// The writer commits a stride sent through its command queue between follow
/// iterations, and refuses one replayed on another chain as data loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_following_writer_commits_strides_from_its_command_queue() -> eyre::Result<()> {
    let harness = Harness::new()?;
    let primary_path = harness.primary_path()?;
    let activations = Arc::new(harness.activations.clone());
    let tip = harness.with_primary(|primary| Ok(primary.event_fence().visible_tip()))?;
    let densified = harness.with_primary(|primary| first_stride(primary, &activations))?;
    // A fork below 200 stores another checkpoint at 300, the end of its third
    // stride.
    let fork = Harness::with_chain(
        &ChainFixture::new(Network::ZcashRegtest)
            .extend_blocks(CHAIN_TIP)
            .fork_at(BlockHeight::new(150))?
            .extend_blocks(CHAIN_TIP - 149),
    )?;
    let foreign = fork.with_primary(|primary| {
        let stride = (0..2).try_fold(BlockHeight::new(0), |from, _| {
            primary
                .next_tree_state_stride(from, EVERY_BLOCK)?
                .map(zinder_store::TreeStateStride::end_height)
                .ok_or_else(|| eyre::eyre!("the fork has too few strides"))
        })?;
        let stride = primary
            .next_tree_state_stride(stride, EVERY_BLOCK)?
            .ok_or_else(|| eyre::eyre!("the fork's third stride needs densifying"))?;
        Ok(primary.densify_tree_state_stride(stride, EVERY_BLOCK, &activations)?)
    })?;
    let serving = harness.into_serving_store_fixture()?;
    let store = RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
        &primary_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::None,
        CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let source = StaticTipSource { tip };
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let (canonical, commands) = zinder_ingest::canonical_control_channel();
    let writer = idle_follower(&source, &activations, &readiness, &cancel);
    let commits = async {
        let first =
            TreeStateStrideWriter::commit_tree_state_stride(&canonical, densified.clone()).await;
        let repeated = TreeStateStrideWriter::commit_tree_state_stride(&canonical, densified).await;
        let foreign = TreeStateStrideWriter::commit_tree_state_stride(&canonical, foreign).await;
        cancel.cancel();
        (first, repeated, foreign)
    };
    let ((first, repeated, foreign), follow_outcome) = tokio::join!(
        commits,
        zinder_ingest::follow_canonical_tip_with_control(store, writer, commands),
    );
    let store = follow_outcome?;

    let first = first?;
    eyre::ensure!((first.rows_written, first.rows_already_present) == (99, 0));
    let repeated = repeated?;
    eyre::ensure!((repeated.rows_written, repeated.rows_already_present) == (0, 99));
    eyre::ensure!(
        matches!(foreign, Err(TreeStateStrideWriteError::Integrity { .. })),
        "a stride from another chain must be refused as an integrity failure: {foreign:?}"
    );
    eyre::ensure!(
        store
            .next_tree_state_stride(BlockHeight::new(0), EVERY_BLOCK)?
            .is_some_and(|stride| stride.seed_height() == BlockHeight::new(100)),
        "the queue must have committed only the first stride"
    );
    drop(serving);
    Ok(())
}

/// A follower that polls `source` often and never reaches a run target.
fn idle_follower<'follow>(
    source: &'follow StaticTipSource,
    activations: &Arc<NetworkUpgradeActivations>,
    readiness: &'follow ComposedReadiness,
    cancel: &'follow CancellationToken,
) -> zinder_ingest::CanonicalFollower<'follow, StaticTipSource> {
    zinder_ingest::CanonicalFollower::new(
        source,
        Arc::clone(activations),
        zinder_ingest::CanonicalFollowConfig {
            request_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(5),
            lag_threshold_blocks: 0,
            target_height: None,
            event_retention_window: None,
            event_retention_check_interval: Duration::from_mins(1),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
            node_failover: None,
        },
        readiness,
        cancel,
    )
}

fn first_stride(
    primary: &RocksDbCanonicalStore,
    activations: &NetworkUpgradeActivations,
) -> eyre::Result<DensifiedTreeStateStride> {
    let stride = primary
        .next_tree_state_stride(BlockHeight::new(0), EVERY_BLOCK)?
        .ok_or_else(|| eyre::eyre!("the first stride needs densifying"))?;
    Ok(primary.densify_tree_state_stride(stride, EVERY_BLOCK, activations)?)
}

/// A node that always reports the same tip, so the follower only services
/// its command queue.
struct StaticTipSource {
    tip: zinder_core::BlockId,
}

#[async_trait::async_trait]
impl zinder_source::NodeSource for StaticTipSource {
    fn capabilities(&self) -> zinder_source::NodeCapabilities {
        zinder_source::NodeCapabilities::default()
    }

    async fn fetch_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<zinder_source::SourceBlock, zinder_source::SourceError> {
        Err(zinder_source::SourceError::BlockUnavailable {
            height,
            reason: "the static-tip node serves no blocks".to_owned(),
        })
    }

    async fn tip_id(&self) -> Result<zinder_core::BlockId, zinder_source::SourceError> {
        Ok(self.tip)
    }
}

/// A published store, its chain, and the trees every height must read.
struct Harness {
    fixture: Arc<Mutex<Option<CanonicalWriterFixture>>>,
    chain: ChainFixture,
    activations: NetworkUpgradeActivations,
    expected: Vec<CommitmentTreeCheckpoint>,
    secondary_root: tempfile::TempDir,
}

impl Harness {
    fn new() -> eyre::Result<Self> {
        Self::with_chain(&ChainFixture::new(Network::ZcashRegtest).extend_blocks(CHAIN_TIP))
    }

    /// Publishes `chain`, with note commitments added, through the baseline tip.
    fn with_chain(chain: &ChainFixture) -> eyre::Result<Self> {
        Self::publish(chain, None)
    }

    /// Publishes the standard chain with history from `floor + 1`, so the
    /// history predecessor sits at `floor`.
    fn with_history_floor(floor: u32) -> eyre::Result<Self> {
        Self::publish(
            &ChainFixture::new(Network::ZcashRegtest).extend_blocks(CHAIN_TIP),
            Some(BlockHeight::new(floor)),
        )
    }

    fn publish(chain: &ChainFixture, history_floor: Option<BlockHeight>) -> eyre::Result<Self> {
        let activations = sample_regtest_upgrade_activations();
        let chain = with_note_commitments(chain, &activations)?;
        let heights = (1..=CHAIN_TIP).map(BlockHeight::new).collect::<Vec<_>>();
        let expected = commitment_tree_checkpoints(&chain, &activations, &heights)?;
        let baseline = CanonicalBaseline::new(&chain).through(BlockHeight::new(BASELINE_TIP));
        let baseline = match history_floor {
            Some(floor) => baseline.with_history_floor(floor),
            None => baseline,
        };
        let fixture = CanonicalWriterFixture::publish(&baseline, &activations)?;
        Ok(Self {
            fixture: Arc::new(Mutex::new(Some(fixture))),
            chain,
            activations,
            expected,
            secondary_root: tempfile::tempdir()?,
        })
    }

    fn with_primary<T>(
        &self,
        read: impl FnOnce(&RocksDbCanonicalStore) -> eyre::Result<T>,
    ) -> eyre::Result<T> {
        self.fixture
            .lock()
            .as_ref()
            .ok_or_else(|| eyre::eyre!("the canonical fixture was closed"))
            .and_then(|fixture| read(fixture.canonical_primary()?))
    }

    fn primary_path(&self) -> eyre::Result<PathBuf> {
        let fixture = self.fixture.lock();
        Ok(fixture
            .as_ref()
            .ok_or_else(|| eyre::eyre!("the canonical fixture was closed"))?
            .canonical_primary_path()
            .to_path_buf())
    }

    fn into_serving_store_fixture(self) -> eyre::Result<zinder_testkit::WalletServingStoreFixture> {
        self.fixture
            .lock()
            .take()
            .ok_or_else(|| eyre::eyre!("the canonical fixture was closed"))?
            .into_serving_store_fixture()
    }

    /// Runs the pass until every settled stride satisfies `selection`.
    async fn run_pass_to_completion(
        &self,
        selection: TreeStateCheckpointSelection,
        hooks: &Hooks,
    ) -> eyre::Result<Vec<TreeStateStrideCommit>> {
        self.run_pass(selection, hooks, CancellationToken::new())
            .await
    }

    /// Runs the pass until `cancel` fires or every settled stride satisfies
    /// `selection`, and returns every commit the writer made.
    async fn run_pass(
        &self,
        selection: TreeStateCheckpointSelection,
        hooks: &Hooks,
        cancel: CancellationToken,
    ) -> eyre::Result<Vec<TreeStateStrideCommit>> {
        let writer = FixtureWriter {
            fixture: Arc::clone(&self.fixture),
            chain: self.chain.clone(),
            hooks: hooks.clone(),
            commits: Arc::new(Mutex::new(Vec::new())),
        };
        let commits = Arc::clone(&writer.commits);
        let task = spawn_tree_state_densification_task(
            TreeStateDensificationContext {
                canonical_primary_path: self.primary_path()?,
                secondary_path: next_secondary_path(self.secondary_root.path()),
                network_upgrade_activations: Arc::new(self.activations.clone()),
                workload: CanonicalStoreWorkload::Wallet,
                raw_blob_retention: RawBlobRetention::None,
                reorg_policy: CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
                resource_budget: RocksDbResourceBudget::for_local_tests(),
                writer,
                gate: following_tip_gate(),
                config: TreeStateDensificationConfig {
                    enabled: true,
                    selection,
                    workers: NonZeroU32::new(2).ok_or_else(|| eyre::eyre!("zero workers"))?,
                    max_blocks_per_second: NonZeroU32::MAX,
                },
            },
            cancel.clone(),
        );
        let refused = hooks.refuse_seed_height.map(BlockHeight::new);
        let finished = tokio::time::timeout(PASS_DEADLINE, async {
            loop {
                if cancel.is_cancelled() {
                    return Ok(());
                }
                let next = self.with_primary(|primary| {
                    let mut from = BlockHeight::new(0);
                    while let Some(stride) = primary.next_tree_state_stride(from, selection)? {
                        if Some(stride.seed_height()) != refused {
                            return Ok(Some(stride));
                        }
                        from = stride.end_height();
                    }
                    Ok(None)
                })?;
                if next.is_none() {
                    return Ok::<_, eyre::Report>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        cancel.cancel();
        task.await?;
        finished.map_err(|_| eyre::eyre!("the pass did not finish within its deadline"))??;
        let commits = commits.lock().clone();
        Ok(commits)
    }

    fn assert_reads_match_the_chain(
        &self,
        heights: std::ops::RangeInclusive<u32>,
    ) -> eyre::Result<()> {
        self.with_primary(|primary| {
            for height in heights {
                let read = primary
                    .tree_state_checkpoint_at(BlockHeight::new(height), &self.activations)?;
                eyre::ensure!(
                    read.as_ref() == Some(expected_at(&self.expected, height)?),
                    "tree state at height {height} differs from the chain's trees"
                );
            }
            Ok(())
        })
    }
}

/// Test behaviour the writer applies around each commit.
#[derive(Clone, Default)]
struct Hooks {
    /// Stops the pass once this many strides are committed.
    cancel_after_commits: Option<(usize, CancellationToken)>,
    /// Refuses the stride with this seed as contradicting stored checkpoints.
    refuse_seed_height: Option<u32>,
    /// Commits the chain's next block live before each stride commit.
    append_live_blocks_before_commit: bool,
}

/// The writer the pass reaches, committing straight to the fixture primary.
#[derive(Clone)]
struct FixtureWriter {
    fixture: Arc<Mutex<Option<CanonicalWriterFixture>>>,
    chain: ChainFixture,
    hooks: Hooks,
    commits: Arc<Mutex<Vec<TreeStateStrideCommit>>>,
}

impl TreeStateStrideWriter for FixtureWriter {
    #[allow(
        clippy::significant_drop_tightening,
        reason = "holding the fixture through the live append and the commit serializes them as the writer's queue does"
    )]
    async fn commit_tree_state_stride(
        &self,
        stride: DensifiedTreeStateStride,
    ) -> Result<TreeStateStrideCommit, TreeStateStrideWriteError> {
        let unavailable = |reason: String| TreeStateStrideWriteError::Unavailable { reason };
        if self
            .hooks
            .refuse_seed_height
            .is_some_and(|height| stride.stride().seed_height().value() == height)
        {
            return Err(TreeStateStrideWriteError::Integrity {
                reason: "refused by the test writer".to_owned(),
            });
        }
        let mut fixture = self.fixture.lock();
        let fixture = fixture
            .as_mut()
            .ok_or_else(|| unavailable("the canonical fixture was closed".to_owned()))?;
        if self.hooks.append_live_blocks_before_commit {
            fixture
                .append_live_block(&self.chain)
                .map_err(|error| unavailable(error.to_string()))?;
        }
        let commit = fixture
            .canonical_primary()
            .map_err(|error| unavailable(error.to_string()))?
            .commit_tree_state_stride(&stride)
            .map_err(|error| {
                if matches!(error, CanonicalStoreError::TreeStateStrideMismatch { .. }) {
                    TreeStateStrideWriteError::Integrity {
                        reason: error.to_string(),
                    }
                } else {
                    unavailable(error.to_string())
                }
            })?;
        let mut commits = self.commits.lock();
        commits.push(commit);
        if let Some((limit, cancel)) = &self.hooks.cancel_after_commits
            && commits.len() >= *limit
        {
            cancel.cancel();
        }
        Ok(commit)
    }
}

fn following_tip_gate() -> HistoricalWorkGate {
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    readiness.update_admission(|admission| admission.phase = Some(IngestPhase::FollowingTip));
    HistoricalWorkGate::without_materialized_views(readiness)
}

fn next_secondary_path(root: &Path) -> PathBuf {
    root.join(format!("secondary-{}", rand_suffix()))
}

fn rand_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos())
}

fn expected_at(
    expected: &[CommitmentTreeCheckpoint],
    height: u32,
) -> eyre::Result<&CommitmentTreeCheckpoint> {
    expected
        .get(usize::try_from(height)? - 1)
        .ok_or_else(|| eyre::eyre!("no expected tree state at height {height}"))
}

/// Heights from `low` through `high` that hold a stored checkpoint.
fn stored_checkpoint_heights(
    primary: &RocksDbCanonicalStore,
    low: u32,
    high: u32,
) -> eyre::Result<Vec<u32>> {
    let mut heights = Vec::new();
    for candidate in low..=high {
        if primary
            .tree_state_checkpoint_at_or_before(BlockHeight::new(candidate))?
            .is_some_and(|checkpoint| checkpoint.block_id.height.value() == candidate)
        {
            heights.push(candidate);
        }
    }
    Ok(heights)
}

/// Note commitments, over every pool, appended through `height`.
fn commitments_through(primary: &RocksDbCanonicalStore, height: u32) -> eyre::Result<u64> {
    if height == 0 {
        return Ok(0);
    }
    let metadata = primary
        .compact_block_at(BlockHeight::new(height))?
        .ok_or_else(|| eyre::eyre!("compact block {height} is absent"))?
        .chain_metadata();
    Ok(u64::from(metadata.sapling_commitment_tree_size)
        + u64::from(metadata.orchard_commitment_tree_size)
        + u64::from(metadata.ironwood_commitment_tree_size))
}
