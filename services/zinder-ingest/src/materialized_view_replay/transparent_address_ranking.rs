//! Publishes the transparent-address ranking generation the explorer reads.
//!
//! The ranking keeps cumulative per-address state, so it cannot join the
//! shared replay boundary partway through history. A view store that has not
//! materialized any height activates an empty generation at genesis, and the
//! ranking then replays from height 1 with every other view. A store whose
//! views materialized heights without the ranking builds an inactive
//! generation from height 1 instead: each replay pass extends it over settled
//! history for a bounded time, and the pass that reaches the settled tip
//! applies the unsettled tail and activates the generation at the shared
//! replay cursor in one write.

use std::time::{Duration, Instant};

use zinder_core::{BlockHeight, BlockHeightRange, ChainEpoch};
use zinder_materialized_views::{
    BlockCommitContext, MaterializedViewStore, TRANSPARENT_ADDRESS_RANKING_CONSUMER_NAME,
    TransparentAddressRankingConsumer, TransparentAddressRankingConsumerError,
    TransparentAddressRankingCoverage, TransparentAddressRankingSnapshotPlan,
};
use zinder_store::{CanonicalEventCursor, RocksDbCanonicalSecondary};

use super::{
    MaterializedViewTailer, ReplayPass, fence_cursor, materialized_view_write_guard,
    persisted_chain_event_cursor, replay_page,
};
use crate::{CanonicalBlockContextReader, IngestError, require_genesis_complete_history};

/// Longest one replay pass extends the ranking build over settled history
/// before it yields, so the other views keep following new canonical events.
const RANKING_BUILD_PASS_BUDGET: Duration = Duration::from_secs(10);

/// Height interval between ranking build progress log lines.
const RANKING_BUILD_PROGRESS_INTERVAL: u32 = 100_000;

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

    /// Builds and activates the ranking on a view store that materialized
    /// heights without it.
    ///
    /// The build starts only while the shared replay sits at the canonical
    /// fence, so the activation cursor names exactly the visible tip the
    /// generation reaches.
    pub(super) fn build_transparent_address_ranking(
        &self,
        canonical: &RocksDbCanonicalSecondary,
        pass: &mut ReplayPass,
    ) -> Result<(), IngestError> {
        if !transparent_address_ranking_is_unpublished(&self.materialized_view_store)? {
            return Ok(());
        }
        let Some(cursor) = persisted_chain_event_cursor(&self.materialized_view_store)? else {
            return Ok(());
        };
        let chain_epoch = canonical.chain_epoch()?;
        if cursor != fence_cursor(canonical)?
            || self.materialized_height()? != Some(chain_epoch.visible_tip_height)
        {
            return Ok(());
        }
        RankingBuild::resume(self, canonical, chain_epoch)?.run(pass, cursor)
    }
}

/// One replay pass's share of a ranking build pinned to a canonical fence.
struct RankingBuild<'tailer> {
    store: &'tailer MaterializedViewStore,
    hydrator: CanonicalBlockContextReader<'tailer>,
    chain_epoch: ChainEpoch,
    generation: u64,
    resumed_through: BlockHeight,
}

impl<'tailer> RankingBuild<'tailer> {
    /// Resumes the inactive build under this fence's visible tip, or starts
    /// one from genesis.
    fn resume(
        tailer: &'tailer MaterializedViewTailer,
        canonical: &'tailer RocksDbCanonicalSecondary,
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
            store,
            hydrator: CanonicalBlockContextReader::new(canonical, &tailer.activations),
            chain_epoch,
            generation,
            resumed_through,
        })
    }

    /// Extends the build over settled history, then activates it once the
    /// settled tip is reached within this pass.
    fn run(
        mut self,
        pass: &mut ReplayPass,
        cursor: CanonicalEventCursor,
    ) -> Result<(), IngestError> {
        let settled_tip_height = self
            .chain_epoch
            .settled_tip_height
            .min(self.chain_epoch.visible_tip_height);
        let started_at = Instant::now();
        let mut through = self.resumed_through;
        while through < settled_tip_height {
            let limits = pass.evaluate();
            if limits.state.is_paused()
                || pass.budget.is_cancelled()
                || started_at.elapsed() >= RANKING_BUILD_PASS_BUDGET
            {
                self.log_progress(through);
                return Ok(());
            }
            let page = replay_page(
                next_height(through)?,
                settled_tip_height,
                limits.batch_blocks,
            )?;
            let blocks = self.read_page(page)?;
            let _write_guard = materialized_view_write_guard();
            TransparentAddressRankingConsumer::write_snapshot_tail_blocks(
                self.store,
                self.generation,
                &blocks,
            )
            .map_err(ranking_error)?;
            through = page.end;
        }
        let unsettled_tail = self.read_unsettled_tail(through, pass)?;
        let metadata = {
            let _write_guard = materialized_view_write_guard();
            TransparentAddressRankingConsumer::activate_snapshot_generation_at_cursor(
                self.store,
                self.generation,
                &unsettled_tail,
                &cursor.as_bytes(),
            )
            .map_err(ranking_error)?
        };
        tracing::info!(
            target: "zinder::ingest",
            event = "transparent_address_ranking_activated",
            generation = metadata.generation,
            through_height = metadata.coverage.balance_complete_through_height.value(),
            positive_address_count = metadata.positive_address_count,
            total_positive_balance_zat = metadata.total_positive_balance_zat,
            "transparent-address ranking built from canonical history and activated at the shared replay cursor"
        );
        Ok(())
    }

    /// Reads the blocks above `through` up to the visible tip.
    ///
    /// They are at most one reorg window long and are applied only in the
    /// activating write, so the inactive build never holds a replaceable block.
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
