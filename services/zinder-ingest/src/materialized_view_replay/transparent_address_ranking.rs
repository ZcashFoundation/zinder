//! Publishes the transparent-address ranking generation the explorer reads.
//!
//! The ranking keeps cumulative per-address state, so it cannot join the
//! shared replay boundary partway through history. A view store that has not
//! materialized any height activates an empty generation at genesis, and the
//! ranking then replays from height 1 with every other view. A store whose
//! views materialized heights without the ranking builds an inactive
//! generation from height 1 instead, on its own worker beside the replay
//! tailer. Each build step extends it over settled history for a bounded
//! time, never past the height the other views have materialized.
//!
//! The build activates in one of two ways. While the other views rebuild from
//! canonical heights without a shared cursor, a build that reaches their
//! materialized height joins them there, and the shared replay applies every
//! later height to the ranking too. Once the views sit at the canonical fence
//! instead, the step that reaches the settled tip applies the unsettled tail
//! and activates the generation at the shared replay cursor in one write.
//!
//! While the other views rebuild without a shared cursor, the spawned replay
//! tailer waits for a build that trails them, so the build catches up and
//! joins instead of walking the same history a second time behind replay.

use std::{
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use zinder_core::{BlockHeight, BlockHeightRange, BlockId, ChainEpoch};
use zinder_materialized_views::{
    BlockCommitContext, MaterializedViewStore, TRANSPARENT_ADDRESS_RANKING_CONSUMER_NAME,
    TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME, TransparentAddressRankingConsumer,
    TransparentAddressRankingConsumerError, TransparentAddressRankingCoverage,
    TransparentAddressRankingMetadata, TransparentAddressRankingSnapshotPlan,
};
use zinder_runtime::Readiness;
use zinder_store::{CanonicalEventCursor, RocksDbCanonicalSecondary};

use super::{
    MaterializedViewReplayBudget, MaterializedViewTailer, ReplayPass, fence_cursor,
    materialized_view_write_guard, persisted_chain_event_cursor, replay_page,
};
use crate::{
    CanonicalBlockContextReader, IngestError, PrevoutCache,
    canonical_block_context::PREVOUT_CACHE_WARMUP_MIN_REMAINING_BLOCKS,
    require_genesis_complete_history, runtime_config::sleep_or_cancel,
};

/// Longest one build step holds the canonical secondary before it yields, so
/// the replay tailer can advance the secondary to a newer fence.
const RANKING_BUILD_STEP_BUDGET: Duration = Duration::from_secs(10);

/// Height interval between ranking build progress log lines.
const RANKING_BUILD_PROGRESS_INTERVAL: u32 = 100_000;

/// Longest a trailing ranking build may go without applying history before
/// replay stops waiting for it.
///
/// A healthy build applies a page or a warm-up window every few seconds, and
/// a step never holds work for much longer than its 10-second budget plus one
/// page, so a build this quiet has failed or stopped. Replay then continues
/// without it, and the build activates at the fence later instead.
const RANKING_BUILD_STALL_TIMEOUT: Duration = Duration::from_mins(5);

/// Ranking build progress the build worker shares with the replay tailer.
///
/// The build records every page and warm-up window it applies. Replay waits
/// for a build that trails the other views only while the build keeps
/// recording progress, so a failing build cannot hold replay back.
#[derive(Debug)]
pub(super) struct RankingBuildProgress {
    stall_timeout: Duration,
    last_advance: Mutex<Instant>,
    advances: AtomicU64,
    replay_waiting: AtomicBool,
    stall_reported: AtomicBool,
}

/// A pending ranking build that has not yet joined the views replay rebuilds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct TrailingRankingBuild {
    /// Height through which the inactive build has applied blocks.
    built_through: BlockHeight,
    /// Height the build walks toward: the views' materialized height, capped
    /// at the settled tip.
    join_height: BlockHeight,
}

impl TrailingRankingBuild {
    /// Returns the build at `built_through` when replay should wait for it.
    ///
    /// The build walks toward the views' materialized height, capped at the
    /// settled tip. Below that height it still has history to apply. At the
    /// views' own settled height it has applied everything and joins them on
    /// its next step, which may follow the step that applied the last page,
    /// so replay keeps waiting. A build stopped at a settled tip below the
    /// views cannot join them there, so replay does not wait for it.
    const fn behind(
        built_through: BlockHeight,
        materialized_height: BlockHeight,
        settled_tip_height: BlockHeight,
    ) -> Option<Self> {
        let join_height = if materialized_height.value() < settled_tip_height.value() {
            materialized_height
        } else {
            settled_tip_height
        };
        let waits = built_through.value() < join_height.value()
            || (built_through.value() == join_height.value()
                && join_height.value() == materialized_height.value());
        if waits {
            Some(Self {
                built_through,
                join_height,
            })
        } else {
            None
        }
    }
}

impl RankingBuildProgress {
    /// Starts tracking a build worker that has not reported yet.
    ///
    /// The stall clock starts now, so replay gives a newly started worker
    /// the full timeout to warm its cache and apply its first page.
    pub(super) fn new() -> Self {
        Self::with_stall_timeout(RANKING_BUILD_STALL_TIMEOUT)
    }

    fn with_stall_timeout(stall_timeout: Duration) -> Self {
        metrics::gauge!("zinder_materialized_view_replay_waiting_for_ranking_build").set(0.0);
        Self {
            stall_timeout,
            last_advance: Mutex::new(Instant::now()),
            advances: AtomicU64::new(0),
            replay_waiting: AtomicBool::new(false),
            stall_reported: AtomicBool::new(false),
        }
    }

    /// Records that the build applied a page or a warm-up window.
    pub(super) fn record_advance(&self) {
        *self.last_advance.lock() = Instant::now();
        self.advances.fetch_add(1, Ordering::Relaxed);
        self.stall_reported.store(false, Ordering::Relaxed);
    }

    /// Returns how many pages and warm-up windows the build has applied.
    pub(super) fn advances(&self) -> u64 {
        self.advances.load(Ordering::Relaxed)
    }

    fn stalled(&self) -> bool {
        self.last_advance.lock().elapsed() >= self.stall_timeout
    }

    #[cfg(test)]
    fn backdate_last_advance(&self, by: Duration) {
        let mut last_advance = self.last_advance.lock();
        *last_advance = last_advance.checked_sub(by).unwrap_or(*last_advance);
    }

    /// Returns whether replay waits for `trailing`, logging each change.
    pub(super) fn replay_waits_for(&self, trailing: Option<TrailingRankingBuild>) -> bool {
        let stalled = trailing.is_some() && self.stalled();
        let waits = trailing.is_some() && !stalled;
        if stalled && !self.stall_reported.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                target: "zinder::ingest",
                event = "materialized_view_replay_ranking_build_stalled",
                stalled_seconds = self.stall_timeout.as_secs(),
                built_through_height = trailing.map_or(0, |build| build.built_through.value()),
                "transparent-address ranking build applied no history within the stall timeout; Explorer view replay continues without waiting for it"
            );
        }
        if self.replay_waiting.swap(waits, Ordering::Relaxed) == waits {
            return waits;
        }
        metrics::gauge!("zinder_materialized_view_replay_waiting_for_ranking_build")
            .set(if waits { 1.0 } else { 0.0 });
        if let Some(build) = trailing.filter(|_| waits) {
            tracing::info!(
                target: "zinder::ingest",
                event = "materialized_view_replay_waiting_for_ranking_build",
                built_through_height = build.built_through.value(),
                join_height = build.join_height.value(),
                "Explorer view replay waits for the transparent-address ranking build to join it, so later heights are hydrated once for every view"
            );
        } else {
            tracing::info!(
                target: "zinder::ingest",
                event = "materialized_view_replay_resumed_after_ranking_build",
                "Explorer view replay resumed after waiting for the transparent-address ranking build"
            );
        }
        waits
    }
}

/// What one transparent-address ranking build step did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransparentAddressRankingBuildStep {
    /// The store declares no ranking, or its ranking is already active.
    NotPending,
    /// The inactive build covers settled history through `built_through` and
    /// has more history to apply or waits for the other views to advance.
    Building {
        /// Height through which the inactive build has applied blocks.
        built_through: BlockHeight,
    },
    /// The build activated with its coverage ending at `height`.
    Activated {
        /// Last height the activated generation applied.
        height: BlockHeight,
    },
}

impl MaterializedViewTailer {
    /// Activates an empty ranking generation before a view store's first build.
    pub(super) fn join_transparent_address_ranking_at_genesis(
        &self,
        canonical: &RocksDbCanonicalSecondary,
    ) -> Result<(), IngestError> {
        if !transparent_address_ranking_is_unpublished(&self.materialized_view_store)?
            || self.materialized_height()?.is_some()
        {
            return Ok(());
        }
        require_genesis_complete_history(canonical)?;
        let metadata = {
            let _write_guard = materialized_view_write_guard();
            TransparentAddressRankingConsumer::activate_genesis_generation(
                &self.materialized_view_store,
            )
            .map_err(ranking_error)?
        };
        tracing::info!(
            target: "zinder::ingest",
            event = "transparent_address_ranking_joined_at_genesis",
            generation = metadata.generation,
            "transparent-address ranking joined the first Explorer views build at genesis"
        );
        Ok(())
    }

    /// Advances the ranking build on a view store that materialized heights
    /// without it, for at most one bounded step.
    ///
    /// The step reads through the shared canonical secondary and resolves
    /// spends through the ranking build's own prevout cache, so it can run on
    /// its own thread while the replay tailer walks other heights. The ingest
    /// runtime calls it from the ranking build worker; tests call it directly
    /// to place a build at a chosen replay position.
    pub fn advance_transparent_address_ranking_build(
        &self,
    ) -> Result<TransparentAddressRankingBuildStep, IngestError> {
        let mut pass = ReplayPass::new(
            MaterializedViewReplayBudget::new(self.config)
                .with_volume_space_guard(self.volume_space_guard.clone()),
        );
        self.advance_ranking_build_step(&mut pass)
    }

    /// Returns the pending ranking build when it trails views that rebuild
    /// from canonical heights without a shared cursor.
    ///
    /// Such a build can still join the views at their materialized height, so
    /// replay that keeps advancing only moves the join point away and leaves
    /// the build to hydrate every height a second time. Views that follow a
    /// shared cursor never wait: the build activates at the fence instead.
    pub(super) fn trailing_ranking_build(
        &self,
        canonical: &RocksDbCanonicalSecondary,
    ) -> Result<Option<TrailingRankingBuild>, IngestError> {
        let store = &self.materialized_view_store;
        if !transparent_address_ranking_is_unpublished(store)? {
            return Ok(None);
        }
        let Some(materialized_height) = self.materialized_height()? else {
            return Ok(None);
        };
        if persisted_chain_event_cursor(store)?.is_some() {
            return Ok(None);
        }
        let chain_epoch = canonical.chain_epoch()?;
        let built_through = TransparentAddressRankingConsumer::build_metadata(store)?
            .map_or(BlockHeight::new(0), |build| {
                build.coverage.balance_complete_through_height
            });
        Ok(TrailingRankingBuild::behind(
            built_through,
            materialized_height,
            chain_epoch
                .settled_tip_height
                .min(chain_epoch.visible_tip_height),
        ))
    }

    /// Advances a ranking build that trails rebuilding views until it joins
    /// them or a step applies no history.
    pub(super) fn join_trailing_ranking_build(
        &self,
        budget: MaterializedViewReplayBudget,
    ) -> Result<(), IngestError> {
        self.canonical.write().try_catch_up()?;
        let progress = std::sync::Arc::new(RankingBuildProgress::new());
        let mut pass = ReplayPass::new(budget).with_ranking_build(std::sync::Arc::clone(&progress));
        while self
            .trailing_ranking_build(&self.canonical.read())?
            .is_some()
        {
            let advances = progress.advances();
            if let Err(error) = self.advance_ranking_build_step(&mut pass) {
                // Replay still runs; the step after it reports the failure.
                tracing::warn!(
                    target: "zinder::ingest",
                    event = "transparent_address_ranking_build_failed",
                    error = %error,
                    "transparent-address ranking build step failed before replay; replaying without it"
                );
                break;
            }
            if progress.advances() == advances {
                break;
            }
        }
        Ok(())
    }

    pub(super) fn advance_ranking_build_step(
        &self,
        pass: &mut ReplayPass,
    ) -> Result<TransparentAddressRankingBuildStep, IngestError> {
        let canonical = self.canonical.read();
        self.advance_ranking_build(&canonical, pass)
    }

    pub(super) fn advance_ranking_build(
        &self,
        canonical: &RocksDbCanonicalSecondary,
        pass: &mut ReplayPass,
    ) -> Result<TransparentAddressRankingBuildStep, IngestError> {
        if !transparent_address_ranking_is_unpublished(&self.materialized_view_store)? {
            self.prevout_caches.ranking_build.lock().clear();
            record_ranking_build_progress(None);
            return Ok(TransparentAddressRankingBuildStep::NotPending);
        }
        if self.materialized_height()?.is_none() {
            // A store with no materialized height joins at genesis on the
            // tailer's first pass instead of building.
            return Ok(TransparentAddressRankingBuildStep::Building {
                built_through: BlockHeight::new(0),
            });
        }
        require_genesis_complete_history(canonical)?;
        let chain_epoch = canonical.chain_epoch()?;
        let mut prevouts = self.prevout_caches.ranking_build.lock();
        RankingBuild::resume(self, canonical, &mut prevouts, chain_epoch)?.run(pass)
    }
}

/// Starts the ranking build worker beside the spawned replay tailer and
/// returns the progress it shares with replay.
pub(super) fn spawn_transparent_address_ranking_builder(
    tailer: &std::sync::Arc<MaterializedViewTailer>,
    poll_interval: Duration,
    readiness: Readiness,
    cancel: CancellationToken,
) -> (
    std::sync::Arc<RankingBuildProgress>,
    tokio::task::JoinHandle<()>,
) {
    let progress = std::sync::Arc::new(RankingBuildProgress::new());
    let worker = tokio::spawn(run_transparent_address_ranking_builder(
        std::sync::Arc::clone(tailer),
        poll_interval,
        readiness,
        std::sync::Arc::clone(&progress),
        cancel,
    ));
    (progress, worker)
}

/// Builds the transparent-address ranking beside the replay tailer until the
/// store holds no pending build.
///
/// Every step evaluates the same replay budget the tailer does, so the build
/// pauses with replay under the canonical-phase gate, memory pressure, and the
/// free-space floor, and empties its prevout cache while paused. A step that
/// applied history is followed by the next one at once; the worker sleeps only
/// after a step that found nothing to apply.
async fn run_transparent_address_ranking_builder(
    tailer: std::sync::Arc<MaterializedViewTailer>,
    poll_interval: Duration,
    readiness: Readiness,
    progress: std::sync::Arc<RankingBuildProgress>,
    cancel: CancellationToken,
) {
    let fresh_budget = || {
        MaterializedViewReplayBudget::with_phase_gate_and_cancel(
            tailer.config,
            readiness.clone(),
            cancel.clone(),
        )
        .with_volume_space_guard(tailer.volume_space_guard.clone())
    };
    let mut budget = fresh_budget();
    loop {
        let advances_before = progress.advances();
        if budget.evaluate_current().state.is_paused() {
            tailer.prevout_caches.release_ranking_build();
        } else {
            let step_tailer = std::sync::Arc::clone(&tailer);
            let step_progress = std::sync::Arc::clone(&progress);
            let joined = tokio::task::spawn_blocking(move || {
                let mut pass = ReplayPass::new(budget).with_ranking_build(step_progress);
                let outcome = step_tailer.advance_ranking_build_step(&mut pass);
                (pass.budget, outcome)
            })
            .await;
            match joined {
                Ok((_, Ok(TransparentAddressRankingBuildStep::NotPending))) => return,
                Ok((returned, outcome)) => {
                    budget = returned;
                    if let Err(error) = outcome {
                        tracing::warn!(
                            target: "zinder::ingest",
                            event = "transparent_address_ranking_build_failed",
                            error = %error,
                            "transparent-address ranking build step failed; retrying"
                        );
                    }
                }
                Err(join_error) => {
                    tracing::warn!(
                        target: "zinder::ingest",
                        event = "transparent_address_ranking_build_failed",
                        error = %join_error,
                        "transparent-address ranking build step did not complete; retrying"
                    );
                    budget = fresh_budget();
                }
            }
        }
        let idle = progress.advances() == advances_before;
        if cancel.is_cancelled() || (idle && sleep_or_cancel(poll_interval, &cancel).await) {
            return;
        }
    }
}

/// One bounded step of a ranking build pinned to a canonical fence.
struct RankingBuild<'build> {
    tailer: &'build MaterializedViewTailer,
    canonical: &'build RocksDbCanonicalSecondary,
    hydrator: CanonicalBlockContextReader<'build>,
    chain_epoch: ChainEpoch,
    generation: u64,
    resumed_through: BlockHeight,
}

impl<'build> RankingBuild<'build> {
    /// Resumes the inactive build under this fence's visible tip, or starts
    /// one from genesis.
    ///
    /// The build walks settled history in order across steps, so `prevouts`
    /// carries the outputs earlier steps saw produced.
    fn resume(
        tailer: &'build MaterializedViewTailer,
        canonical: &'build RocksDbCanonicalSecondary,
        prevouts: &'build mut PrevoutCache,
        chain_epoch: ChainEpoch,
    ) -> Result<Self, IngestError> {
        let store = &tailer.materialized_view_store;
        let generation = TransparentAddressRankingConsumer::build_metadata(store)?
            .map_or(1, |build| build.generation);
        {
            let _write_guard = materialized_view_write_guard();
            TransparentAddressRankingConsumer::initialize_snapshot_generation(
                store,
                TransparentAddressRankingSnapshotPlan {
                    generation,
                    base_height: BlockHeight::new(0),
                    base_block_hash: chain_epoch.network.genesis_hash(),
                    target_height: chain_epoch.visible_tip_height,
                    target_block_hash: chain_epoch.visible_tip_hash,
                    expected_summary_count: 0,
                    base_coverage: TransparentAddressRankingCoverage::at_genesis(),
                },
            )
            .map_err(ranking_error)?;
            TransparentAddressRankingConsumer::finalize_snapshot_base(store, generation)
                .map_err(ranking_error)?;
        }
        let resumed_through = build_through(store)?;
        if resumed_through == BlockHeight::new(0) {
            tracing::info!(
                target: "zinder::ingest",
                event = "transparent_address_ranking_build_started",
                generation,
                target_height = chain_epoch.visible_tip_height.value(),
                "transparent-address ranking is building from genesis because the Explorer views materialized heights without it"
            );
        }
        Ok(Self {
            tailer,
            canonical,
            hydrator: CanonicalBlockContextReader::with_prevout_cache(
                canonical,
                &tailer.activations,
                prevouts,
            ),
            chain_epoch,
            generation,
            resumed_through,
        })
    }

    /// Extends the build toward the other views' materialized height over
    /// settled history, then activates it when it can join them.
    fn run(
        mut self,
        pass: &mut ReplayPass,
    ) -> Result<TransparentAddressRankingBuildStep, IngestError> {
        let settled_tip_height = self
            .chain_epoch
            .settled_tip_height
            .min(self.chain_epoch.visible_tip_height);
        let started_at = Instant::now();
        let mut through = self.resumed_through;
        let mut cache_positioned = false;
        loop {
            let shared_height = self
                .tailer
                .materialized_height()?
                .unwrap_or(BlockHeight::new(0));
            let goal = settled_tip_height.min(shared_height);
            record_ranking_build_progress(Some((through, goal)));
            let limits = pass.evaluate();
            if limits.state.is_paused()
                || pass.budget.is_cancelled()
                || started_at.elapsed() >= RANKING_BUILD_STEP_BUDGET
            {
                return Ok(self.paused_at(through));
            }
            if !cache_positioned && through < goal {
                cache_positioned = true;
                if through > BlockHeight::new(0)
                    && goal.value().saturating_sub(through.value())
                        >= PREVOUT_CACHE_WARMUP_MIN_REMAINING_BLOCKS
                    && !self.warm_prevout_cache(through, started_at, pass)?
                {
                    return Ok(self.paused_at(through));
                }
            }
            if through < goal {
                let page = replay_page(next_height(through)?, goal, limits.batch_blocks)?;
                let blocks = self.read_page(page)?;
                let _write_guard = materialized_view_write_guard();
                TransparentAddressRankingConsumer::write_snapshot_tail_blocks(
                    &self.tailer.materialized_view_store,
                    self.generation,
                    &blocks,
                )
                .map_err(ranking_error)?;
                through = page.end;
                pass.record_ranking_build_advance();
                continue;
            }
            if through == shared_height
                && let Some(metadata) = self.join_shared_replay(through)?
            {
                self.hand_prevouts_to_replay(through);
                return Ok(activated(
                    &metadata,
                    "transparent_address_ranking_joined_replay",
                ));
            }
            if through == settled_tip_height
                && let Some(metadata) = self.activate_at_fence(through, pass)?
            {
                return Ok(activated(
                    &metadata,
                    "transparent_address_ranking_activated",
                ));
            }
            // A replay page that landed since `shared_height` was read leaves
            // more history to apply in this step; otherwise wait for one.
            if self.tailer.materialized_height()? == Some(shared_height) {
                return Ok(self.paused_at(through));
            }
        }
    }

    /// Joins the other views at `through` while they rebuild from canonical
    /// heights without a shared cursor.
    ///
    /// The checks run under the materialized-view write lock, so no replay
    /// page lands between them and the activation. `through` is settled, so
    /// no reorg can replace the joined block.
    fn join_shared_replay(
        &self,
        through: BlockHeight,
    ) -> Result<Option<TransparentAddressRankingMetadata>, IngestError> {
        let store = &self.tailer.materialized_view_store;
        let _write_guard = materialized_view_write_guard();
        if self.tailer.materialized_height()? != Some(through)
            || persisted_chain_event_cursor(store)?.is_some()
        {
            return Ok(None);
        }
        let Some(state) = store.consumer_state(TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME)? else {
            return Ok(None);
        };
        if state.tip_height != through {
            return Ok(None);
        }
        match TransparentAddressRankingConsumer::activate_snapshot_generation_at_block(
            store,
            self.generation,
            BlockId::new(through, state.tip_hash),
        ) {
            Ok(metadata) => Ok(Some(metadata)),
            // The views hold a block canonical history replaced; the tailer
            // reverts it before the build can join.
            Err(TransparentAddressRankingConsumerError::SnapshotTargetMismatch) => Ok(None),
            Err(error) => Err(ranking_error(error)),
        }
    }

    /// Warms the build's empty prevout cache through `through` within the
    /// step budget, recording each window it applies as build progress.
    ///
    /// A window counts only once the next one is requested or the warm-up
    /// returns, so a window whose read fails never refreshes the stall clock.
    fn warm_prevout_cache(
        &mut self,
        through: BlockHeight,
        started_at: Instant,
        pass: &mut ReplayPass,
    ) -> Result<bool, IngestError> {
        let mut window_open = false;
        let warmed = self.hydrator.warm_prevout_cache(through, &mut || {
            if window_open {
                pass.record_ranking_build_advance();
            }
            let window = (started_at.elapsed() < RANKING_BUILD_STEP_BUDGET)
                .then(|| pass.warm_up_window())
                .flatten();
            window_open = window.is_some();
            window
        })?;
        if window_open {
            pass.record_ranking_build_advance();
        }
        Ok(warmed)
    }

    /// Hands the cache this build walked to `joined` over to replay, which
    /// resumes at the next height, so replay does not warm its own cache over
    /// the same history again.
    ///
    /// Replay holds its cache only while it hydrates a transition, and a
    /// replay that is hydrating keeps its own cache.
    fn hand_prevouts_to_replay(&mut self, joined: BlockHeight) {
        let Some(mut replay) = self.tailer.prevout_caches.replay.try_lock() else {
            return;
        };
        let Some(build) = self.hydrator.prevout_cache_mut() else {
            return;
        };
        if replay.take_over(build) {
            tracing::info!(
                target: "zinder::ingest",
                event = "materialized_view_prevout_cache_handed_to_replay",
                joined_height = joined.value(),
                cached_outputs = replay.len(),
                "Explorer view replay took over the prevout cache the transparent-address ranking build walked to the joined height"
            );
        }
    }

    /// Activates the build at the shared replay cursor once the other views
    /// cover this fence's visible tip.
    ///
    /// The unsettled tail above the settled tip is at most one reorg window
    /// long and is applied only in the activating write, so the inactive build
    /// never holds a replaceable block.
    fn activate_at_fence(
        &mut self,
        through: BlockHeight,
        pass: &mut ReplayPass,
    ) -> Result<Option<TransparentAddressRankingMetadata>, IngestError> {
        let cursor = fence_cursor(self.canonical)?;
        if !self.views_cover_fence(cursor)? {
            return Ok(None);
        }
        let unsettled_tail = self.read_unsettled_tail(through, pass)?;
        if pass.yields() {
            return Ok(None);
        }
        let store = &self.tailer.materialized_view_store;
        let _write_guard = materialized_view_write_guard();
        if !self.views_cover_fence(cursor)? {
            return Ok(None);
        }
        TransparentAddressRankingConsumer::activate_snapshot_generation_at_cursor(
            store,
            self.generation,
            &unsettled_tail,
            &cursor.as_bytes(),
        )
        .map(Some)
        .map_err(ranking_error)
    }

    fn views_cover_fence(&self, cursor: CanonicalEventCursor) -> Result<bool, IngestError> {
        Ok(
            persisted_chain_event_cursor(&self.tailer.materialized_view_store)? == Some(cursor)
                && self.tailer.materialized_height()? == Some(self.chain_epoch.visible_tip_height),
        )
    }

    /// Reads the blocks above `through` up to the visible tip.
    fn read_unsettled_tail(
        &mut self,
        through: BlockHeight,
        pass: &mut ReplayPass,
    ) -> Result<Vec<BlockCommitContext>, IngestError> {
        let visible_tip_height = self.chain_epoch.visible_tip_height;
        let mut tail = Vec::new();
        let mut next = next_height(through)?;
        while next <= visible_tip_height {
            let page = replay_page(next, visible_tip_height, pass.evaluate().batch_blocks)?;
            tail.extend(self.read_page(page)?);
            next = next_height(page.end)?;
        }
        Ok(tail)
    }

    fn read_page(
        &mut self,
        page: BlockHeightRange,
    ) -> Result<Vec<BlockCommitContext>, IngestError> {
        let blocks = self.hydrator.read_ordered_block_commit_contexts(page)?;
        if blocks.len() != page.into_iter().len() {
            return Err(IngestError::MaterializedViewDispatch(format!(
                "canonical heights {}..={} hydrated {} blocks for the transparent-address ranking build",
                page.start.value(),
                page.end.value(),
                blocks.len(),
            )));
        }
        Ok(blocks)
    }

    fn paused_at(&self, through: BlockHeight) -> TransparentAddressRankingBuildStep {
        self.log_progress(through);
        TransparentAddressRankingBuildStep::Building {
            built_through: through,
        }
    }

    fn log_progress(&self, through: BlockHeight) {
        if through.value() / RANKING_BUILD_PROGRESS_INTERVAL
            == self.resumed_through.value() / RANKING_BUILD_PROGRESS_INTERVAL
        {
            return;
        }
        tracing::info!(
            target: "zinder::ingest",
            event = "transparent_address_ranking_build_progress",
            generation = self.generation,
            through_height = through.value(),
            target_height = self.chain_epoch.visible_tip_height.value(),
            "transparent-address ranking build advanced over settled history"
        );
    }
}

/// Clears the build-progress gauges and logs the activation.
fn activated(
    metadata: &TransparentAddressRankingMetadata,
    event: &'static str,
) -> TransparentAddressRankingBuildStep {
    record_ranking_build_progress(None);
    let height = metadata.coverage.balance_complete_through_height;
    tracing::info!(
        target: "zinder::ingest",
        event,
        generation = metadata.generation,
        through_height = height.value(),
        positive_address_count = metadata.positive_address_count,
        total_positive_balance_zat = metadata.total_positive_balance_zat,
        "transparent-address ranking built from canonical history and activated"
    );
    TransparentAddressRankingBuildStep::Activated { height }
}

/// Publishes how far the inactive ranking build has applied history and the
/// height it is walking toward, or clears both once no build is pending.
fn record_ranking_build_progress(progress: Option<(BlockHeight, BlockHeight)>) {
    let (built_through, goal) = progress.map_or((0.0, 0.0), |(built, goal)| {
        (f64::from(built.value()), f64::from(goal.value()))
    });
    metrics::gauge!("zinder_materialized_view_transparent_address_ranking_build_pending")
        .set(if progress.is_some() { 1.0 } else { 0.0 });
    metrics::gauge!("zinder_materialized_view_transparent_address_ranking_build_height")
        .set(built_through);
    metrics::gauge!("zinder_materialized_view_transparent_address_ranking_build_target_height")
        .set(goal);
}

/// Returns whether the store declares the ranking and has not activated it.
fn transparent_address_ranking_is_unpublished(
    store: &MaterializedViewStore,
) -> Result<bool, IngestError> {
    Ok(
        store.has_consumer(TRANSPARENT_ADDRESS_RANKING_CONSUMER_NAME)
            && TransparentAddressRankingConsumer::active_metadata(store)?.is_none(),
    )
}

fn build_through(store: &MaterializedViewStore) -> Result<BlockHeight, IngestError> {
    TransparentAddressRankingConsumer::build_metadata(store)?
        .map(|build| build.coverage.balance_complete_through_height)
        .ok_or_else(|| {
            IngestError::MaterializedViewDispatch(
                "transparent-address ranking build disappeared after initialization".to_owned(),
            )
        })
}

fn next_height(height: BlockHeight) -> Result<BlockHeight, IngestError> {
    height.next().ok_or_else(|| {
        IngestError::MaterializedViewDispatch(
            "transparent-address ranking build height overflow".to_owned(),
        )
    })
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "the signature is used directly by Result::map_err"
)]
fn ranking_error(error: TransparentAddressRankingConsumerError) -> IngestError {
    IngestError::MaterializedViewDispatch(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRAILING: Option<TrailingRankingBuild> = Some(TrailingRankingBuild {
        built_through: BlockHeight::new(10),
        join_height: BlockHeight::new(500),
    });

    #[test]
    fn replay_waits_for_a_build_below_or_at_the_views_settled_height() {
        let height = BlockHeight::new;
        assert_eq!(
            TrailingRankingBuild::behind(height(10), height(500), height(900)),
            TRAILING,
            "a build below the views has history left to apply"
        );
        assert_eq!(
            TrailingRankingBuild::behind(height(500), height(500), height(900)),
            Some(TrailingRankingBuild {
                built_through: height(500),
                join_height: height(500),
            }),
            "a build at the views' settled height joins them on its next step"
        );
        assert_eq!(
            TrailingRankingBuild::behind(height(400), height(500), height(400)),
            None,
            "a build at a settled tip below the views cannot join them there"
        );
        assert_eq!(
            TrailingRankingBuild::behind(height(300), height(500), height(400)),
            Some(TrailingRankingBuild {
                built_through: height(300),
                join_height: height(400),
            })
        );
    }

    #[test]
    fn replay_waits_only_while_a_build_trails_it() {
        let progress = RankingBuildProgress::with_stall_timeout(Duration::from_hours(1));
        assert!(progress.replay_waits_for(TRAILING));
        assert!(progress.replay_waits_for(TRAILING));
        assert!(
            !progress.replay_waits_for(None),
            "a build that reached the views' height releases replay"
        );
    }

    #[test]
    fn a_build_that_applies_nothing_within_the_timeout_releases_replay() {
        let progress = RankingBuildProgress::with_stall_timeout(Duration::from_hours(1));
        assert!(progress.replay_waits_for(TRAILING));
        progress.backdate_last_advance(Duration::from_hours(2));
        assert!(
            !progress.replay_waits_for(TRAILING),
            "a stalled build must not hold replay back"
        );

        progress.record_advance();
        assert_eq!(progress.advances(), 1);
        assert!(
            progress.replay_waits_for(TRAILING),
            "a build that applies history again resumes the wait"
        );
    }
}
