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

use std::time::{Duration, Instant};

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

/// Builds the transparent-address ranking beside the replay tailer until the
/// store holds no pending build.
///
/// Every step evaluates the same replay budget the tailer does, so the build
/// pauses with replay under the canonical-phase gate, memory pressure, and the
/// free-space floor, and empties its prevout cache while paused.
pub(super) async fn run_transparent_address_ranking_builder(
    tailer: std::sync::Arc<MaterializedViewTailer>,
    poll_interval: Duration,
    readiness: Readiness,
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
        if budget.evaluate_current().state.is_paused() {
            tailer.prevout_caches.ranking_build.lock().clear();
        } else {
            let step_tailer = std::sync::Arc::clone(&tailer);
            let joined = tokio::task::spawn_blocking(move || {
                let mut pass = ReplayPass::new(budget);
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
        if sleep_or_cancel(poll_interval, &cancel).await {
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
                    && !self
                        .hydrator
                        .warm_prevout_cache(through, &mut || pass.yields())?
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
                continue;
            }
            if through == shared_height
                && let Some(metadata) = self.join_shared_replay(through)?
            {
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
