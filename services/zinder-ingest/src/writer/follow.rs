//! Continuous append-only following for the canonical store.

use std::{collections::BTreeMap, future::Future, num::NonZeroU32, sync::Arc, time::Duration};

use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zinder_core::{
    BlockHeight, BlockId, ChainTipMetadata, CommitmentTreeAccumulator,
    CommitmentTreeAccumulatorError, CommitmentTreeCheckpoint, NetworkUpgradeActivations,
    ShieldedProtocol, SubtreeRootIndex, SubtreeRootRange, UnixTimestampMillis,
};
use zinder_runtime::{IngestPhase, Readiness, ReadinessCause, ReadinessState};
use zinder_source::{NodeSource, SourceBlock, SourceError};
use zinder_store::{
    CanonicalAppendAnchor, CanonicalBuildBlock, CanonicalBuildSubtreeRoot, CanonicalLiveAppend,
    CanonicalLiveReplacement, CanonicalReplacementBlock, CanonicalStoreError,
    RocksDbCanonicalStore,
};

use crate::{
    CanonicalBlockConstructionError, CanonicalConstructionError, CommitmentTreeSizes, IngestError,
    MempoolReadyGate, position_canonical_block, prepare_canonical_block,
    source_recovery::{
        SourceRecoveryDecision, decide_recovery, default_recovery_backoff, detail_for_new_outage,
        detail_for_ongoing_outage,
    },
    writer::construction::{
        canonical_build_block, compact_block_commitments, register_prohibited_read_metrics,
    },
    writer::control::{
        CanonicalControlCommand, CanonicalControlScheduling, apply_canonical_control_command,
    },
    writer::raw_blob_policy_for_retention,
};

/// Polling and bounded-source settings for canonical following.
#[derive(Clone, Debug)]
pub struct CanonicalFollowConfig {
    /// Maximum wall time for one source request.
    pub request_timeout: Duration,
    /// Delay before re-observing an unchanged tip.
    pub poll_interval: Duration,
    /// Maximum ready lag from the latest atomic source observation.
    pub lag_threshold_blocks: u64,
    /// Optional deterministic stop height for certification runs.
    pub target_height: Option<BlockHeight>,
    /// Exact age window for canonical event retention.
    pub event_retention_window: Option<Duration>,
    /// Minimum cadence between primary-owned retention passes.
    pub event_retention_check_interval: Duration,
    /// Optional live-mempool hydration gate. When present, canonical lag may
    /// publish `Ready` only after the current source generation has completed
    /// its in-memory snapshot.
    pub mempool_ready_gate: Option<MempoolReadyGate>,
}

/// Source and operational state bound to one canonical follower lane.
pub struct CanonicalFollower<'a, Source> {
    source: &'a Source,
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    config: CanonicalFollowConfig,
    readiness: &'a Readiness,
    cancel: &'a CancellationToken,
}

impl<'a, Source> CanonicalFollower<'a, Source>
where
    Source: NodeSource,
{
    /// Binds one source and admitted activation table to a follower lane.
    #[must_use]
    pub fn new(
        source: &'a Source,
        network_upgrade_activations: Arc<NetworkUpgradeActivations>,
        config: CanonicalFollowConfig,
        readiness: &'a Readiness,
        cancel: &'a CancellationToken,
    ) -> Self {
        Self {
            source,
            network_upgrade_activations,
            config,
            readiness,
            cancel,
        }
    }

    async fn recover_source_failure(
        &self,
        source_error: SourceError,
        source_outage: &mut SourceOutage,
        visible_height: BlockHeight,
    ) -> Result<bool, CanonicalFollowError> {
        let ingest_error = IngestError::from(source_error);
        let SourceRecoveryDecision::Recover {
            failure_class,
            last_reason,
            backoff,
        } = decide_recovery(&ingest_error, default_recovery_backoff())
        else {
            return Err(CanonicalFollowError::SourceRecoveryRejected);
        };
        let detail = source_outage.as_ref().map_or_else(
            || detail_for_new_outage(failure_class, last_reason.clone()),
            |(started_at, previous)| {
                detail_for_ongoing_outage(
                    previous,
                    failure_class,
                    last_reason.clone(),
                    u32::try_from(started_at.elapsed().as_secs()).unwrap_or(u32::MAX),
                )
            },
        );
        source_outage.get_or_insert_with(|| (std::time::Instant::now(), detail.clone()));
        if let Some((_, current_detail)) = source_outage.as_mut() {
            *current_detail = detail.clone();
        }
        self.readiness
            .set(ReadinessState::node_unavailable_with_detail(
                detail,
                Some(visible_height.value()),
            ));
        metrics::counter!("zinder_ingest_canonical_source_recoveries_total").increment(1);
        Ok(tokio::select! {
            () = self.cancel.cancelled() => true,
            () = tokio::time::sleep(backoff) => false,
        })
    }

    fn commit_append(
        &self,
        store: RocksDbCanonicalStore,
        append: CanonicalLiveAppend,
        observed_tip: BlockId,
    ) -> Result<RocksDbCanonicalStore, CanonicalFollowError> {
        let commit_started = std::time::Instant::now();
        let (store, fence) =
            store.commit_live_append(append, self.network_upgrade_activations.as_ref())?;
        metrics::counter!("zinder_ingest_canonical_live_appends_total").increment(1);
        metrics::histogram!("zinder_ingest_canonical_live_commit_seconds")
            .record(commit_started.elapsed().as_secs_f64());
        record_fence_metrics(&store, observed_tip);
        set_follow_readiness(
            self.readiness,
            &store,
            observed_tip,
            self.config.lag_threshold_blocks,
            self.config.mempool_ready_gate.as_ref(),
        );
        tracing::info!(
            target: "zinder::ingest",
            event = "canonical_live_append_committed",
            chain_epoch = fence.chain_epoch_id().value(),
            chain_event_sequence = fence.chain_event_sequence(),
            visible_tip_height = fence.visible_tip().height.value(),
            visible_tip_hash = ?fence.visible_tip().hash,
            sequence_digest = ?fence.sequence_digest().as_bytes(),
            source_tip_height = observed_tip.height.value(),
            historical_prevout_reads = 0_u64,
            cross_block_wallet_reads = 0_u64,
            "committed one authenticated canonical append"
        );
        Ok(store)
    }

    fn commit_replacement(
        &self,
        store: RocksDbCanonicalStore,
        replacement: CanonicalLiveReplacement,
        observed_tip: BlockId,
    ) -> Result<RocksDbCanonicalStore, CanonicalFollowError> {
        let commit_started = std::time::Instant::now();
        let previous_tip = store.event_fence().visible_tip();
        let (store, fence) = store
            .commit_live_replacement(replacement, self.network_upgrade_activations.as_ref())?;
        metrics::counter!("zinder_ingest_canonical_live_replacements_total").increment(1);
        metrics::histogram!("zinder_ingest_canonical_live_replacement_commit_seconds")
            .record(commit_started.elapsed().as_secs_f64());
        record_fence_metrics(&store, observed_tip);
        set_follow_readiness(
            self.readiness,
            &store,
            observed_tip,
            self.config.lag_threshold_blocks,
            self.config.mempool_ready_gate.as_ref(),
        );
        tracing::info!(
            target: "zinder::ingest",
            event = "canonical_live_replacement_committed",
            chain_epoch = fence.chain_epoch_id().value(),
            chain_event_sequence = fence.chain_event_sequence(),
            previous_tip_height = previous_tip.height.value(),
            previous_tip_hash = ?previous_tip.hash,
            visible_tip_height = fence.visible_tip().height.value(),
            visible_tip_hash = ?fence.visible_tip().hash,
            sequence_digest = ?fence.sequence_digest().as_bytes(),
            source_tip_height = observed_tip.height.value(),
            historical_prevout_reads = 0_u64,
            cross_block_wallet_reads = 0_u64,
            "committed one authenticated canonical suffix replacement"
        );
        Ok(store)
    }
}

type SourceOutage = Option<(std::time::Instant, zinder_runtime::NodeUnavailableDetail)>;

/// Exact bounded-discovery evidence for a fork beyond authenticated settlement.
#[derive(Debug, Error)]
#[error(
    "source fork exceeds the canonical replacement window: local tip {local_tip:?}, source tip {source_tip:?}, settled tip {settled_tip:?}, required depth {required_depth}, configured window {configured_window_blocks}"
)]
pub struct CanonicalReorgWindowExceeded {
    /// Authenticated local visible tip.
    pub local_tip: BlockId,
    /// Latest atomic source observation.
    pub source_tip: BlockId,
    /// Authenticated settlement boundary that source discovery may not cross.
    pub settled_tip: BlockId,
    /// Minimum replacement depth required to continue discovery.
    pub required_depth: u32,
    /// Persisted maximum replacement depth.
    pub configured_window_blocks: u32,
    /// Replaceable depth the window covered: the distance from the settled
    /// tip to the local tip. Below `configured_window_blocks` while a raised
    /// window is still growing toward it.
    pub covered_depth_blocks: u32,
}

/// Failure while following the clean canonical store.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CanonicalFollowError {
    /// The upstream source request failed before a canonical write began.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// The shared recovery classifier rejected a source-shaped failure.
    #[error("source-shaped canonical follower failure was classified as fatal")]
    SourceRecoveryRejected,
    /// Block-local canonical preparation rejected the source payload.
    #[error(transparent)]
    BlockConstruction(#[from] CanonicalBlockConstructionError),
    /// Shared canonical preparation rejected the compact block artifacts.
    #[error(transparent)]
    CanonicalPreparation(#[from] CanonicalConstructionError),
    /// Commitment-tree preparation rejected the next source block.
    #[error("canonical live commitment-tree state failed at height {height:?}")]
    CommitmentTreeState {
        /// Height being applied.
        height: BlockHeight,
        /// Exact accumulator failure.
        #[source]
        source: CommitmentTreeAccumulatorError,
    },
    /// The source checkpoint did not authenticate the locally derived next frontier.
    #[error("source checkpoint differs from the locally derived live checkpoint at {height:?}")]
    SourceCheckpointMismatch {
        /// Height whose tree state diverged.
        height: BlockHeight,
    },
    /// Size-only compact metadata diverged from the typed frontier accumulator.
    #[error(
        "live block {height:?} commitment-tree positions diverged: positioned={positioned:?}, accumulated={accumulated:?}"
    )]
    CommitmentTreePositionMismatch {
        /// Block whose independent position calculations diverged.
        height: BlockHeight,
        /// Position stamped into the compact block.
        positioned: ChainTipMetadata,
        /// Position derived from typed frontiers.
        accumulated: ChainTipMetadata,
    },
    /// A completed-subtree position regressed between adjacent blocks.
    #[error("{protocol:?} completed-subtree count regressed from {previous_count} to {next_count}")]
    SubtreePositionRegression {
        /// Shielded pool whose completed-subtree count regressed.
        protocol: ShieldedProtocol,
        /// Completed subtrees at the authenticated predecessor.
        previous_count: u32,
        /// Completed subtrees after the next block.
        next_count: u32,
    },
    /// The source no longer extends the authenticated local fence.
    #[error(
        "append-only following requires reorg handling: local tip {local_tip:?}, source tip {source_tip:?}"
    )]
    ReorgRequired {
        /// Authenticated local visible tip.
        local_tip: BlockId,
        /// Latest atomic source observation.
        source_tip: BlockId,
    },
    /// The source fork has no common parent above the authenticated settlement boundary.
    #[error(transparent)]
    ReorgWindowExceeded(Box<CanonicalReorgWindowExceeded>),
    /// The retained canonical row needed to advance settlement is absent.
    #[error("canonical settled-tip header is absent at height {height:?}")]
    SettlementHeaderAbsent {
        /// Minimum settled height required by the admitted reorg window.
        height: BlockHeight,
    },
    /// A blocking preparation task stopped without returning its result.
    #[error("canonical live block preparation task stopped: {reason}")]
    PreparationTaskStopped {
        /// Tokio task failure.
        reason: String,
    },
    /// Runtime shutdown cancelled read-only preparation before mutation.
    #[error("canonical follower cancelled before commit")]
    Cancelled,
    /// The consuming atomic store transition failed or could not be verified.
    #[error(transparent)]
    Store(#[from] CanonicalStoreError),
}

/// Follows atomic Zebra tip observations through canonical appends.
///
/// Source failures happen before the consuming store commit and are retried
/// with the admitted writer handle. Any store error terminates the writer lane;
/// the caller must reopen through normal READY admission to determine the
/// durable outcome.
pub async fn follow_canonical_tip<Source>(
    store: RocksDbCanonicalStore,
    follower: CanonicalFollower<'_, Source>,
) -> Result<RocksDbCanonicalStore, CanonicalFollowError>
where
    Source: NodeSource,
{
    follow_canonical_tip_controlled(store, follower, None).await
}

/// Follows while servicing bounded commands against the same primary handle.
///
/// The receiver is owned by this loop, not by a storage adapter. Commands may
/// queue while source preparation is in flight, but `AtTip` waits always select
/// between a command, cancellation, and the configured poll delay.
pub async fn follow_canonical_tip_with_control<Source>(
    store: RocksDbCanonicalStore,
    follower: CanonicalFollower<'_, Source>,
    control_commands: mpsc::Receiver<CanonicalControlCommand>,
) -> Result<RocksDbCanonicalStore, CanonicalFollowError>
where
    Source: NodeSource,
{
    follow_canonical_tip_controlled(store, follower, Some(control_commands)).await
}

#[expect(
    clippy::too_many_lines,
    reason = "the writer loop keeps source recovery, typed readiness, command servicing, and consuming append/replacement dispatch in one ownership scope"
)]
async fn follow_canonical_tip_controlled<Source>(
    mut store: RocksDbCanonicalStore,
    follower: CanonicalFollower<'_, Source>,
    mut control_commands: Option<mpsc::Receiver<CanonicalControlCommand>>,
) -> Result<RocksDbCanonicalStore, CanonicalFollowError>
where
    Source: NodeSource,
{
    register_prohibited_read_metrics();
    follower.readiness.set_phase(IngestPhase::FollowingTip);
    let mut source_outage = None;
    let mut next_event_retention_check = std::time::Instant::now();
    let mut mempool_hydration_changes = follower.config.mempool_ready_gate.clone();

    loop {
        if let Some(control_commands) = control_commands.as_mut() {
            while let Ok(command) = control_commands.try_recv() {
                if apply_canonical_control_command(&mut store, command)
                    == CanonicalControlScheduling::YieldToCanonical
                {
                    break;
                }
            }
        }
        if follower.cancel.is_cancelled() {
            return Ok(store);
        }
        if std::time::Instant::now() >= next_event_retention_check {
            if let Some(window) = follower.config.event_retention_window {
                let now = UnixTimestampMillis::now();
                let window_millis = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
                let cutoff = UnixTimestampMillis::new(now.value().saturating_sub(window_millis));
                store.prune_canonical_events_before_created_at(cutoff, now)?;
            }
            next_event_retention_check = std::time::Instant::now()
                .checked_add(follower.config.event_retention_check_interval)
                .unwrap_or_else(std::time::Instant::now);
        }
        if follower
            .config
            .target_height
            .is_some_and(|target| store.event_fence().visible_tip().height >= target)
        {
            let visible_tip = store.event_fence().visible_tip();
            set_follow_readiness(
                follower.readiness,
                &store,
                visible_tip,
                follower.config.lag_threshold_blocks,
                follower.config.mempool_ready_gate.as_ref(),
            );
            record_fence_metrics(&store, visible_tip);
            return Ok(store);
        }

        let Some(prepared) = await_follow_preparation_or_mempool_change(
            prepare_follow_iteration(
                &store,
                follower.source,
                Arc::clone(&follower.network_upgrade_activations),
                follower.config.clone(),
                follower.cancel,
            ),
            &mut mempool_hydration_changes,
            follower.config.mempool_ready_gate.as_ref(),
            follower.readiness,
            store.event_fence().visible_tip(),
        )
        .await
        else {
            // Source preparation is read-only, so dropping it on a mempool
            // lifecycle transition preserves the admitted canonical fence.
            // Restarting the loop requires a fresh source-tip observation
            // before either subsystem can publish Ready again.
            continue;
        };
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(CanonicalFollowError::Source(source_error)) => {
                if follower
                    .recover_source_failure(
                        source_error,
                        &mut source_outage,
                        store.event_fence().visible_tip().height,
                    )
                    .await?
                {
                    return Ok(store);
                }
                continue;
            }
            Err(CanonicalFollowError::ReorgWindowExceeded(evidence)) => {
                follower.readiness.park(
                    ReadinessState::reorg_window_exceeded(
                        u64::from(evidence.required_depth),
                        u64::from(evidence.configured_window_blocks),
                        Some(u64::from(evidence.covered_depth_blocks)),
                        Some(evidence.local_tip.height.value()),
                    )
                    .with_phase(IngestPhase::FollowingTip),
                );
                if let Some(control_commands) = control_commands.as_mut() {
                    serve_parked_writer(&mut store, control_commands, follower.cancel).await;
                }
                return Err(CanonicalFollowError::ReorgWindowExceeded(evidence));
            }
            Err(CanonicalFollowError::Cancelled) => return Ok(store),
            Err(error) => return Err(error),
        };
        source_outage = None;

        // Preparation is read-only. Shutdown at this boundary must preserve
        // the previously admitted canonical fence and displaced archive.
        if follower.cancel.is_cancelled() {
            return Ok(store);
        }

        match prepared {
            FollowIteration::AtTip { observed_tip } => {
                record_fence_metrics(&store, observed_tip);
                set_follow_readiness(
                    follower.readiness,
                    &store,
                    observed_tip,
                    follower.config.lag_threshold_blocks,
                    follower.config.mempool_ready_gate.as_ref(),
                );
                if let Some(control_receiver) = control_commands.as_mut() {
                    let control_channel_closed = tokio::select! {
                        () = follower.cancel.cancelled() => return Ok(store),
                        hydration_changed = wait_for_mempool_hydration_change(&mut mempool_hydration_changes) => {
                            apply_mempool_hydration_change(
                                hydration_changed,
                                &mut mempool_hydration_changes,
                                follower.readiness,
                                follower.config.mempool_ready_gate.as_ref(),
                                store.event_fence().visible_tip(),
                            );
                            false
                        }
                        command = control_receiver.recv() => {
                            command.is_none_or(|command| {
                                apply_canonical_control_command(&mut store, command);
                                false
                            })
                        }
                        () = tokio::time::sleep(follower.config.poll_interval) => false,
                    };
                    if control_channel_closed {
                        control_commands = None;
                    }
                } else {
                    tokio::select! {
                        () = follower.cancel.cancelled() => return Ok(store),
                        hydration_changed = wait_for_mempool_hydration_change(&mut mempool_hydration_changes) => {
                            apply_mempool_hydration_change(
                                hydration_changed,
                                &mut mempool_hydration_changes,
                                follower.readiness,
                                follower.config.mempool_ready_gate.as_ref(),
                                store.event_fence().visible_tip(),
                            );
                        }
                        () = tokio::time::sleep(follower.config.poll_interval) => {}
                    }
                }
            }
            FollowIteration::Append {
                append,
                observed_tip,
            } => {
                store = follower.commit_append(store, *append, observed_tip)?;
            }
            FollowIteration::Replacement {
                replacement,
                observed_tip,
            } => {
                store = follower.commit_replacement(store, *replacement, observed_tip)?;
            }
        }
    }
}

enum FollowIteration {
    AtTip {
        observed_tip: BlockId,
    },
    Append {
        append: Box<CanonicalLiveAppend>,
        observed_tip: BlockId,
    },
    Replacement {
        replacement: Box<CanonicalLiveReplacement>,
        observed_tip: BlockId,
    },
}

#[expect(
    clippy::too_many_lines,
    reason = "one source observation must choose exactly one at-tip, append, or replacement preparation without splitting ownership"
)]
async fn prepare_follow_iteration<Source>(
    store: &RocksDbCanonicalStore,
    source: &Source,
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    config: CanonicalFollowConfig,
    cancel: &CancellationToken,
) -> Result<FollowIteration, CanonicalFollowError>
where
    Source: NodeSource,
{
    let anchor = store.append_anchor()?;
    let observed_tip = source_request(config.request_timeout, cancel, source.tip_id()).await?;
    let local_tip = anchor.event_fence().visible_tip();
    if observed_tip.height < local_tip.height
        || (observed_tip.height == local_tip.height && observed_tip.hash != local_tip.hash)
    {
        return prepare_replacement_iteration(
            store,
            source,
            network_upgrade_activations,
            config,
            cancel,
            anchor,
            observed_tip,
            None,
        )
        .await;
    }
    let effective_target = config.target_height.map_or(observed_tip.height, |target| {
        target.min(observed_tip.height)
    });
    if effective_target <= local_tip.height {
        return Ok(FollowIteration::AtTip { observed_tip });
    }
    let next_height = local_tip
        .height
        .next()
        .ok_or(CanonicalFollowError::ReorgRequired {
            local_tip,
            source_tip: observed_tip,
        })?;
    let settled_tip = next_append_settled_tip(store, &anchor, next_height)?;
    let source_block = source_request(
        config.request_timeout,
        cancel,
        source.fetch_block_at(next_height),
    )
    .await?;
    if source_block.parent_hash != local_tip.hash {
        metrics::counter!("zinder_ingest_canonical_fork_trigger_block_reads_total").increment(1);
        return prepare_replacement_iteration(
            store,
            source,
            network_upgrade_activations,
            config,
            cancel,
            anchor,
            observed_tip,
            Some(source_block),
        )
        .await;
    }
    let source_checkpoint = source_request(
        config.request_timeout,
        cancel,
        source.fetch_chain_checkpoint(next_height, network_upgrade_activations.as_ref()),
    )
    .await?;
    let predecessor_checkpoint = anchor.tip_checkpoint().clone();
    let raw_blob_retention = store.raw_blob_retention();
    let preparation_started = std::time::Instant::now();
    let activations_for_prepare = Arc::clone(&network_upgrade_activations);
    let (block, next_tip_metadata) = tokio::task::spawn_blocking(move || {
        prepare_live_block(
            &source_block,
            &predecessor_checkpoint,
            &source_checkpoint,
            activations_for_prepare.as_ref(),
            raw_blob_retention,
        )
    })
    .await
    .map_err(|source| CanonicalFollowError::PreparationTaskStopped {
        reason: source.to_string(),
    })??;
    metrics::histogram!("zinder_ingest_canonical_live_prepare_seconds")
        .record(preparation_started.elapsed().as_secs_f64());
    let subtree_roots = fetch_live_subtree_roots(
        source,
        config.request_timeout,
        cancel,
        anchor.tip_checkpoint().tip_metadata(),
        next_tip_metadata,
    )
    .await?;
    Ok(FollowIteration::Append {
        append: Box::new(CanonicalLiveAppend::new(
            anchor.event_fence(),
            block,
            subtree_roots,
            settled_tip,
            UnixTimestampMillis::now(),
        )),
        observed_tip,
    })
}

#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "bounded fork discovery and suffix preparation keep the exact source, fence, activation, timeout, and prefetched-block identities together"
)]
async fn prepare_replacement_iteration<Source>(
    store: &RocksDbCanonicalStore,
    source: &Source,
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    config: CanonicalFollowConfig,
    cancel: &CancellationToken,
    anchor: CanonicalAppendAnchor,
    observed_tip: BlockId,
    prefetched_source_block: Option<SourceBlock>,
) -> Result<FollowIteration, CanonicalFollowError>
where
    Source: NodeSource,
{
    let local_tip = anchor.event_fence().visible_tip();
    let settled_tip = anchor.settled_tip();
    let configured_window_blocks = store.reorg_policy().reorg_window_blocks();
    let search_start = local_tip.height.min(observed_tip.height);
    let mut source_blocks = prefetched_source_block
        .map(|block| BTreeMap::from([(block.height, block)]))
        .unwrap_or_default();
    let mut search_height = Some(search_start);
    let mut common_parent = None;
    let discovery_started = std::time::Instant::now();
    let mut discovery_reads = 0_u32;

    while let Some(height) = search_height.filter(|height| *height > settled_tip.height) {
        let source_block = source_request(
            config.request_timeout,
            cancel,
            source.fetch_block_at(height),
        )
        .await?;
        discovery_reads = discovery_reads.saturating_add(1);
        metrics::counter!("zinder_ingest_canonical_fork_discovery_block_reads_total").increment(1);
        let local_header =
            store
                .block_header_at(height)?
                .ok_or(CanonicalFollowError::ReorgRequired {
                    local_tip,
                    source_tip: observed_tip,
                })?;
        if source_block.hash == local_header.block_hash {
            common_parent = Some(BlockId::new(height, source_block.hash));
        } else if height.value() == settled_tip.height.value().saturating_add(1)
            && source_block.parent_hash == settled_tip.hash
        {
            common_parent = Some(settled_tip);
        }
        source_blocks.insert(height, source_block);
        if common_parent.is_some() {
            break;
        }
        search_height = height.value().checked_sub(1).map(BlockHeight::new);
    }
    metrics::histogram!("zinder_ingest_canonical_fork_discovery_seconds")
        .record(discovery_started.elapsed().as_secs_f64());

    let Some(common_parent) = common_parent else {
        let unsettled_depth = local_tip
            .height
            .value()
            .saturating_sub(settled_tip.height.value());
        return Err(CanonicalFollowError::ReorgWindowExceeded(Box::new(
            CanonicalReorgWindowExceeded {
                local_tip,
                source_tip: observed_tip,
                settled_tip,
                required_depth: unsettled_depth.saturating_add(1),
                configured_window_blocks,
                covered_depth_blocks: unsettled_depth,
            },
        )));
    };
    let Some(first_replacement_height) = common_parent.height.next() else {
        return Err(CanonicalFollowError::ReorgRequired {
            local_tip,
            source_tip: observed_tip,
        });
    };
    let requested_target = config.target_height.map_or(observed_tip.height, |target| {
        target.min(observed_tip.height)
    });
    let replacement_window_tip = BlockHeight::new(
        settled_tip
            .height
            .value()
            .saturating_add(configured_window_blocks),
    );
    let replacement_tip_height = requested_target.min(replacement_window_tip);
    if first_replacement_height > replacement_tip_height
        || first_replacement_height > local_tip.height
    {
        return Err(CanonicalFollowError::Source(
            SourceError::BlockReorgDuringFetch {
                height: observed_tip.height,
                reason: "source rewind did not yet expose a nonempty replacement suffix",
            },
        ));
    }

    let mut predecessor_checkpoint =
        store.replacement_parent_checkpoint(common_parent, network_upgrade_activations.as_ref())?;
    let raw_blob_retention = store.raw_blob_retention();
    let mut previous_tip_metadata = predecessor_checkpoint.tip_metadata();
    let mut replacement_blocks = Vec::new();
    let mut height = Some(first_replacement_height);
    while let Some(current_height) = height.filter(|height| *height <= replacement_tip_height) {
        let source_block = if let Some(source_block) = source_blocks.remove(&current_height) {
            source_block
        } else {
            metrics::counter!("zinder_ingest_canonical_replacement_source_block_reads_total")
                .increment(1);
            source_request(
                config.request_timeout,
                cancel,
                source.fetch_block_at(current_height),
            )
            .await?
        };
        if source_block.parent_hash != predecessor_checkpoint.block_id.hash {
            return Err(CanonicalFollowError::Source(
                SourceError::BlockReorgDuringFetch {
                    height: current_height,
                    reason: "replacement suffix changed during bounded fork discovery",
                },
            ));
        }
        metrics::counter!("zinder_ingest_canonical_replacement_source_checkpoint_reads_total")
            .increment(1);
        let source_checkpoint = source_request(
            config.request_timeout,
            cancel,
            source.fetch_chain_checkpoint(current_height, network_upgrade_activations.as_ref()),
        )
        .await?;
        let source_block_for_prepare = source_block.clone();
        let predecessor_for_prepare = predecessor_checkpoint.clone();
        let checkpoint_for_prepare = source_checkpoint.clone();
        let activations_for_prepare = Arc::clone(&network_upgrade_activations);
        let preparation_started = std::time::Instant::now();
        let (block, next_tip_metadata) = tokio::task::spawn_blocking(move || {
            prepare_live_block(
                &source_block_for_prepare,
                &predecessor_for_prepare,
                &checkpoint_for_prepare,
                activations_for_prepare.as_ref(),
                raw_blob_retention,
            )
        })
        .await
        .map_err(|source| CanonicalFollowError::PreparationTaskStopped {
            reason: source.to_string(),
        })??;
        metrics::histogram!("zinder_ingest_canonical_live_replacement_prepare_seconds")
            .record(preparation_started.elapsed().as_secs_f64());
        let subtree_roots = fetch_live_subtree_roots(
            source,
            config.request_timeout,
            cancel,
            previous_tip_metadata,
            next_tip_metadata,
        )
        .await?;
        replacement_blocks.push(CanonicalReplacementBlock::new(block, subtree_roots));
        predecessor_checkpoint = source_checkpoint;
        previous_tip_metadata = next_tip_metadata;
        height = current_height.next();
    }
    metrics::counter!("zinder_ingest_canonical_fork_discoveries_total").increment(1);
    tracing::info!(
        target: "zinder::ingest",
        event = "canonical_source_fork_discovered",
        common_parent_height = common_parent.height.value(),
        common_parent_hash = ?common_parent.hash,
        local_tip_height = local_tip.height.value(),
        source_tip_height = observed_tip.height.value(),
        replacement_tip_height = replacement_tip_height.value(),
        discovery_block_reads = discovery_reads,
        replacement_block_count = replacement_blocks.len(),
        historical_prevout_reads = 0_u64,
        cross_block_wallet_reads = 0_u64,
        "prepared one bounded canonical source suffix replacement"
    );
    Ok(FollowIteration::Replacement {
        replacement: Box::new(CanonicalLiveReplacement::new(
            anchor.event_fence(),
            replacement_blocks,
            UnixTimestampMillis::now(),
        )),
        observed_tip,
    })
}

/// Serves owner commands from a writer parked on a reorg deeper than its
/// window until cancellation.
///
/// The writer no longer follows, but readers still need its frozen fence,
/// retained events, retention leases, and owner checkpoints, and its control
/// plane keeps reporting the typed halt. No command changes canonical rows.
async fn serve_parked_writer(
    store: &mut RocksDbCanonicalStore,
    control_commands: &mut mpsc::Receiver<CanonicalControlCommand>,
    cancel: &CancellationToken,
) {
    tracing::warn!(
        target: "zinder::ingest",
        event = "canonical_writer_parked",
        visible_tip_height = store.event_fence().visible_tip().height.value(),
        "canonical writer parked; serving its control plane until termination"
    );
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            command = control_commands.recv() => {
                let Some(command) = command else {
                    cancel.cancelled().await;
                    return;
                };
                apply_canonical_control_command(store, command);
            }
        }
    }
}

fn next_append_settled_tip(
    store: &RocksDbCanonicalStore,
    anchor: &zinder_store::CanonicalAppendAnchor,
    next_visible_height: BlockHeight,
) -> Result<BlockId, CanonicalFollowError> {
    let selected_height = next_settled_height(
        store.ready_evidence().first_retained_block.height,
        anchor.settled_tip().height,
        next_visible_height,
        store.reorg_policy().reorg_window_blocks(),
    );
    if selected_height == anchor.settled_tip().height {
        return Ok(anchor.settled_tip());
    }
    let header = store.block_header_at(selected_height)?.ok_or(
        CanonicalFollowError::SettlementHeaderAbsent {
            height: selected_height,
        },
    )?;
    Ok(BlockId::new(selected_height, header.block_hash))
}

fn next_settled_height(
    first_retained_height: BlockHeight,
    current_settled_height: BlockHeight,
    next_visible_height: BlockHeight,
    reorg_window_blocks: u32,
) -> BlockHeight {
    BlockHeight::new(
        next_visible_height
            .value()
            .saturating_sub(reorg_window_blocks)
            .max(first_retained_height.value())
            .max(current_settled_height.value()),
    )
}

fn prepare_live_block(
    source_block: &SourceBlock,
    predecessor_checkpoint: &CommitmentTreeCheckpoint,
    source_checkpoint: &CommitmentTreeCheckpoint,
    network_upgrade_activations: &NetworkUpgradeActivations,
    raw_blob_retention: zinder_store::RawBlobRetention,
) -> Result<(CanonicalBuildBlock, ChainTipMetadata), CanonicalFollowError> {
    let height = source_block.height;
    let prepared = prepare_canonical_block(
        source_block,
        network_upgrade_activations,
        raw_blob_policy_for_retention(raw_blob_retention),
    )?;
    let commitments = compact_block_commitments(&prepared)?;
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        predecessor_checkpoint.block_id.height,
        &predecessor_checkpoint.frontiers,
        network_upgrade_activations,
    )
    .map_err(|source| CanonicalFollowError::CommitmentTreeState { height, source })?;
    accumulator
        .append_block_commitments(
            height,
            &commitments.sapling,
            &commitments.orchard,
            &commitments.ironwood,
        )
        .map_err(|source| CanonicalFollowError::CommitmentTreeState { height, source })?;
    let mut running_tree_sizes =
        CommitmentTreeSizes::from_tip_metadata(predecessor_checkpoint.tip_metadata());
    let positioned = position_canonical_block(prepared, &mut running_tree_sizes)?;
    let accumulated_tip_metadata = accumulator.tip_metadata();
    if positioned.tip_metadata != accumulated_tip_metadata {
        return Err(CanonicalFollowError::CommitmentTreePositionMismatch {
            height,
            positioned: positioned.tip_metadata,
            accumulated: accumulated_tip_metadata,
        });
    }
    let derived_checkpoint = CommitmentTreeCheckpoint::new(
        BlockId::new(height, source_block.hash),
        source_block.block_time_seconds,
        accumulator
            .validated_frontiers()
            .map_err(|source| CanonicalFollowError::CommitmentTreeState { height, source })?,
    );
    if &derived_checkpoint != source_checkpoint {
        return Err(CanonicalFollowError::SourceCheckpointMismatch { height });
    }
    let tip_metadata = positioned.tip_metadata;
    Ok((
        canonical_build_block(positioned, Some(derived_checkpoint), None),
        tip_metadata,
    ))
}

async fn fetch_live_subtree_roots<Source>(
    source: &Source,
    request_timeout: Duration,
    cancel: &CancellationToken,
    previous_tip_metadata: ChainTipMetadata,
    next_tip_metadata: ChainTipMetadata,
) -> Result<Vec<CanonicalBuildSubtreeRoot>, CanonicalFollowError>
where
    Source: NodeSource,
{
    let mut roots = Vec::new();
    for range in live_subtree_root_ranges(previous_tip_metadata, next_tip_metadata)? {
        let response = source_request(
            request_timeout,
            cancel,
            source.fetch_subtree_root_range(range),
        )
        .await?;
        roots.extend(
            response
                .subtree_roots
                .into_iter()
                .map(|root| CanonicalBuildSubtreeRoot {
                    protocol: response.protocol,
                    subtree_index: root.subtree_index,
                    root_hash: root.root_hash,
                    completing_block_height: root.completing_block_height,
                }),
        );
    }
    Ok(roots)
}

fn live_subtree_root_ranges(
    previous: ChainTipMetadata,
    next: ChainTipMetadata,
) -> Result<Vec<SubtreeRootRange>, CanonicalFollowError> {
    let mut ranges = Vec::with_capacity(3);
    for protocol in [
        ShieldedProtocol::Sapling,
        ShieldedProtocol::Orchard,
        ShieldedProtocol::Ironwood,
    ] {
        let start_index = previous.completed_subtree_count(protocol);
        let completed_at_next = next.completed_subtree_count(protocol);
        let root_count = completed_at_next.checked_sub(start_index).ok_or(
            CanonicalFollowError::SubtreePositionRegression {
                protocol,
                previous_count: start_index,
                next_count: completed_at_next,
            },
        )?;
        if let Some(max_entries) = NonZeroU32::new(root_count) {
            ranges.push(SubtreeRootRange::new(
                protocol,
                SubtreeRootIndex::new(start_index),
                max_entries,
            ));
        }
    }
    Ok(ranges)
}

async fn source_request<T>(
    timeout: Duration,
    cancel: &CancellationToken,
    request: impl Future<Output = Result<T, SourceError>>,
) -> Result<T, CanonicalFollowError> {
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(CanonicalFollowError::Cancelled),
        request_result = tokio::time::timeout(timeout, request) => request_result
            .map_err(|_| SourceError::NodeUnavailable {
                reason: format!("canonical follow source request exceeded {timeout:?}"),
            })?
            .map_err(CanonicalFollowError::from),
    }
}

async fn await_follow_preparation_or_mempool_change<T>(
    preparation: impl Future<Output = T>,
    mempool_hydration_changes: &mut Option<MempoolReadyGate>,
    mempool_ready_gate: Option<&MempoolReadyGate>,
    readiness: &Readiness,
    canonical_tip: BlockId,
) -> Option<T> {
    tokio::select! {
        biased;
        hydration_changed = wait_for_mempool_hydration_change(mempool_hydration_changes) => {
            apply_mempool_hydration_change(
                hydration_changed,
                mempool_hydration_changes,
                readiness,
                mempool_ready_gate,
                canonical_tip,
            );
            None
        }
        prepared = preparation => Some(prepared),
    }
}

fn apply_mempool_hydration_change(
    hydration_changed: bool,
    mempool_hydration_changes: &mut Option<MempoolReadyGate>,
    readiness: &Readiness,
    mempool_ready_gate: Option<&MempoolReadyGate>,
    canonical_tip: BlockId,
) {
    if !hydration_changed {
        *mempool_hydration_changes = None;
    }
    withdraw_follow_readiness_for_mempool_hydration(readiness, canonical_tip, mempool_ready_gate);
}

fn set_follow_readiness(
    readiness: &Readiness,
    store: &RocksDbCanonicalStore,
    observed_tip: BlockId,
    lag_threshold_blocks: u64,
    mempool_ready_gate: Option<&MempoolReadyGate>,
) {
    let current_height = store.event_fence().visible_tip().height;
    let lag = u64::from(
        observed_tip
            .height
            .value()
            .saturating_sub(current_height.value()),
    );
    let state = if lag <= lag_threshold_blocks {
        ReadinessState::ready_with_target(
            Some(current_height.value()),
            Some(observed_tip.height.value()),
        )
    } else {
        ReadinessState::syncing(
            Some(lag),
            Some(current_height.value()),
            Some(observed_tip.height.value()),
        )
    };
    metrics::gauge!("zinder_ingest_canonical_lag_blocks").set(f64::from(
        observed_tip
            .height
            .value()
            .saturating_sub(store.event_fence().visible_tip().height.value()),
    ));
    let state = gate_canonical_readiness_on_mempool_hydration(
        state,
        store.event_fence().visible_tip(),
        mempool_ready_gate,
    );
    publish_follow_readiness(readiness, state.with_phase(IngestPhase::FollowingTip));
}

/// Publishes one follow observation without clearing an orthogonal warning.
///
/// The materialized-view tailer owns [`ReadinessCause::CursorAtRisk`], which
/// still permits traffic. A lag-derived `Ready` advances the observed heights
/// underneath it instead of replacing it.
fn publish_follow_readiness(readiness: &Readiness, state: ReadinessState) {
    readiness.update(|current_state| {
        let warning_owns_cause = matches!(state.cause, ReadinessCause::Ready)
            && matches!(current_state.cause, ReadinessCause::CursorAtRisk { .. });
        if warning_owns_cause {
            current_state.current_height = state.current_height;
            current_state.target_height = state.target_height;
            current_state.phase = state.phase;
            return;
        }
        *current_state = state;
    });
}

fn gate_canonical_readiness_on_mempool_hydration(
    state: ReadinessState,
    canonical_tip: BlockId,
    mempool_ready_gate: Option<&MempoolReadyGate>,
) -> ReadinessState {
    if matches!(&state.cause, zinder_runtime::ReadinessCause::Ready)
        && mempool_ready_gate.is_some_and(|gate| !gate.admits_canonical_tip(canonical_tip))
    {
        ReadinessState::syncing(None, state.current_height, state.target_height)
    } else {
        state
    }
}

fn withdraw_follow_readiness_for_mempool_hydration(
    readiness: &Readiness,
    canonical_tip: BlockId,
    mempool_ready_gate: Option<&MempoolReadyGate>,
) {
    if mempool_ready_gate.is_none_or(|gate| gate.admits_canonical_tip(canonical_tip)) {
        return;
    }

    readiness.update(|state| {
        if state.cause.permits_traffic() {
            state.cause = ReadinessCause::Syncing { lag_blocks: None };
        }
    });
}

async fn wait_for_mempool_hydration_change(
    mempool_hydration_changes: &mut Option<MempoolReadyGate>,
) -> bool {
    match mempool_hydration_changes {
        Some(gate) => gate.changed().await.is_ok(),
        None => std::future::pending().await,
    }
}

fn record_fence_metrics(store: &RocksDbCanonicalStore, observed_tip: BlockId) {
    let fence = store.event_fence();
    metrics::gauge!("zinder_ingest_canonical_chain_epoch").set(f64::from(
        u32::try_from(fence.chain_epoch_id().value()).unwrap_or(u32::MAX),
    ));
    metrics::gauge!("zinder_ingest_canonical_chain_event_sequence").set(f64::from(
        u32::try_from(fence.chain_event_sequence()).unwrap_or(u32::MAX),
    ));
    metrics::gauge!("zinder_ingest_canonical_tip_height")
        .set(f64::from(fence.visible_tip().height.value()));
    metrics::gauge!("zinder_ingest_canonical_lag_blocks").set(f64::from(
        observed_tip
            .height
            .value()
            .saturating_sub(fence.visible_tip().height.value()),
    ));
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, time::Duration};

    use zinder_core::{
        BlockHash, BlockHeight, BlockId, ChainTipMetadata, SUBTREE_LEAF_COUNT, ShieldedProtocol,
        SubtreeRootIndex, SubtreeRootRange,
    };

    use super::{
        await_follow_preparation_or_mempool_change, gate_canonical_readiness_on_mempool_hydration,
        live_subtree_root_ranges, next_settled_height, publish_follow_readiness,
        wait_for_mempool_hydration_change, withdraw_follow_readiness_for_mempool_hydration,
    };
    use crate::mempool_ready_channel;
    use zinder_runtime::{IngestPhase, Readiness, ReadinessCause, ReadinessState};

    #[test]
    fn ready_follow_observation_keeps_an_active_cursor_at_risk_warning() {
        let readiness = Readiness::default();
        readiness.set(ReadinessState::cursor_at_risk(25, 168, Some(90)));

        publish_follow_readiness(
            &readiness,
            ReadinessState::ready_with_target(Some(100), Some(100))
                .with_phase(IngestPhase::FollowingTip),
        );

        let report = readiness.report();
        assert!(matches!(
            report.cause,
            ReadinessCause::CursorAtRisk {
                oldest_retained_age_hours: 25,
                retention_hours: 168,
            }
        ));
        assert_eq!(report.current_height, Some(100));
        assert_eq!(report.target_height, Some(100));
        assert_eq!(report.phase, Some(IngestPhase::FollowingTip));
    }

    #[test]
    fn lagging_follow_observation_replaces_a_cursor_at_risk_warning() {
        let readiness = Readiness::default();
        readiness.set(ReadinessState::cursor_at_risk(25, 168, Some(90)));

        publish_follow_readiness(
            &readiness,
            ReadinessState::syncing(Some(10), Some(90), Some(100))
                .with_phase(IngestPhase::FollowingTip),
        );

        assert!(matches!(
            readiness.report().cause,
            ReadinessCause::Syncing {
                lag_blocks: Some(10)
            }
        ));
    }

    fn block_id(height: u32, hash_tag: u8) -> BlockId {
        BlockId::new(
            BlockHeight::new(height),
            BlockHash::from_bytes([hash_tag; 32]),
        )
    }

    #[test]
    fn hydration_marker_does_not_make_a_syncing_canonical_writer_ready() {
        let (signal, gate) = mempool_ready_channel();
        signal.withdraw_certification();
        let canonical_syncing = ReadinessState::syncing(Some(3), Some(100), Some(103));
        let canonical_tip = block_id(100, 1);

        let while_hydrating = gate_canonical_readiness_on_mempool_hydration(
            canonical_syncing.clone(),
            canonical_tip,
            Some(&gate),
        );
        assert!(matches!(
            &while_hydrating.cause,
            ReadinessCause::Syncing {
                lag_blocks: Some(3)
            }
        ));
        assert!(!while_hydrating.cause.permits_traffic());

        signal.certify_source_tip(canonical_tip);
        let after_marker = gate_canonical_readiness_on_mempool_hydration(
            canonical_syncing,
            canonical_tip,
            Some(&gate),
        );
        assert!(matches!(
            &after_marker.cause,
            ReadinessCause::Syncing {
                lag_blocks: Some(3)
            }
        ));
        assert!(!after_marker.cause.permits_traffic());
    }

    #[test]
    fn canonical_ready_requires_a_mempool_generation_certified_at_its_exact_tip() {
        let (signal, gate) = mempool_ready_channel();
        let canonical_ready = ReadinessState::ready_with_target(Some(100), Some(100));
        let canonical_tip = block_id(100, 2);

        let while_hydrating = gate_canonical_readiness_on_mempool_hydration(
            canonical_ready.clone(),
            canonical_tip,
            Some(&gate),
        );
        assert!(matches!(
            &while_hydrating.cause,
            ReadinessCause::Syncing { .. }
        ));
        assert!(!while_hydrating.cause.permits_traffic());

        signal.certify_source_tip(block_id(99, 1));
        let after_stale_marker = gate_canonical_readiness_on_mempool_hydration(
            canonical_ready.clone(),
            canonical_tip,
            Some(&gate),
        );
        assert!(matches!(
            &after_stale_marker.cause,
            ReadinessCause::Syncing { .. }
        ));

        signal.certify_source_tip(canonical_tip);
        let after_current_marker = gate_canonical_readiness_on_mempool_hydration(
            canonical_ready,
            canonical_tip,
            Some(&gate),
        );
        assert!(matches!(&after_current_marker.cause, ReadinessCause::Ready));
        assert!(after_current_marker.cause.permits_traffic());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn hydration_withdrawal_cancels_inflight_preparation_and_requires_fresh_observation() {
        let (signal, gate) = mempool_ready_channel();
        let mut hydration_changes = Some(gate.clone());
        let canonical_tip = block_id(100, 1);
        signal.certify_source_tip(canonical_tip);
        assert!(wait_for_mempool_hydration_change(&mut hydration_changes).await);
        let readiness = Readiness::new(
            ReadinessState::ready_with_target(Some(100), Some(100))
                .with_phase(IngestPhase::FollowingTip),
        );
        let (preparation_sender, preparation_receiver) = tokio::sync::oneshot::channel::<()>();

        let (preparation_outcome, ()) = tokio::join!(
            await_follow_preparation_or_mempool_change(
                preparation_receiver,
                &mut hydration_changes,
                Some(&gate),
                &readiness,
                canonical_tip,
            ),
            async {
                tokio::task::yield_now().await;
                signal.withdraw_certification();
            },
        );

        assert!(preparation_outcome.is_none());
        assert!(
            preparation_sender.send(()).is_err(),
            "the read-only canonical preparation must be dropped on withdrawal"
        );
        let withdrawn = readiness.report();
        assert!(matches!(
            withdrawn.cause,
            ReadinessCause::Syncing { lag_blocks: None }
        ));
        assert_eq!(withdrawn.current_height, Some(100));
        assert_eq!(withdrawn.target_height, Some(100));
        assert_eq!(withdrawn.phase, Some(IngestPhase::FollowingTip));

        signal.certify_source_tip(canonical_tip);
        let interrupted_by_rehydration = await_follow_preparation_or_mempool_change(
            std::future::ready(()),
            &mut hydration_changes,
            Some(&gate),
            &readiness,
            canonical_tip,
        )
        .await;

        assert!(
            interrupted_by_rehydration.is_none(),
            "snapshot completion must restart canonical observation instead of restoring stale Ready"
        );
        assert!(matches!(
            readiness.report().cause,
            ReadinessCause::Syncing { .. }
        ));

        let fresh_observation = await_follow_preparation_or_mempool_change(
            std::future::ready(()),
            &mut hydration_changes,
            Some(&gate),
            &readiness,
            canonical_tip,
        )
        .await;
        assert!(fresh_observation.is_some());
    }

    #[test]
    fn hydration_withdrawal_preserves_canonical_non_ready_authority() {
        let (_signal, gate) = mempool_ready_channel();
        let canonical_syncing = ReadinessState::syncing(Some(3), Some(100), Some(103))
            .with_phase(IngestPhase::FollowingTip);
        let readiness = Readiness::new(canonical_syncing.clone());
        let canonical_tip = block_id(100, 1);

        withdraw_follow_readiness_for_mempool_hydration(&readiness, canonical_tip, Some(&gate));

        let report = readiness.report();
        assert_eq!(report.cause, canonical_syncing.cause);
        assert_eq!(report.current_height, canonical_syncing.current_height);
        assert_eq!(report.target_height, canonical_syncing.target_height);
        assert_eq!(report.phase, canonical_syncing.phase);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn hydration_watch_interrupts_at_tip_wait_on_each_transition() {
        let (signal, gate) = mempool_ready_channel();
        let canonical_ready = ReadinessState::ready_with_target(Some(100), Some(100));
        let canonical_tip = block_id(100, 1);
        signal.certify_source_tip(canonical_tip);

        let (withdraw_sender, withdraw_receiver) = tokio::sync::oneshot::channel();
        let mut hydration_changes = Some(gate.clone());
        tokio::spawn(async move {
            let woke_for_hydration_change = tokio::select! {
                changed = wait_for_mempool_hydration_change(&mut hydration_changes) => changed,
                () = std::future::pending::<()>() => false,
            };
            let _ = withdraw_sender.send(woke_for_hydration_change);
        });
        tokio::task::yield_now().await;
        signal.withdraw_certification();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), withdraw_receiver)
                .await
                .ok()
                .and_then(Result::ok),
            Some(true),
            "a reconnect must interrupt the at-tip wait instead of waiting for the poll delay"
        );
        let withdrawn = gate_canonical_readiness_on_mempool_hydration(
            canonical_ready.clone(),
            canonical_tip,
            Some(&gate),
        );
        assert!(matches!(&withdrawn.cause, ReadinessCause::Syncing { .. }));

        let (rehydration_sender, rehydration_receiver) = tokio::sync::oneshot::channel();
        let mut hydration_changes = Some(gate.clone());
        tokio::spawn(async move {
            let woke_for_hydration_change = tokio::select! {
                changed = wait_for_mempool_hydration_change(&mut hydration_changes) => changed,
                () = std::future::pending::<()>() => false,
            };
            let _ = rehydration_sender.send(woke_for_hydration_change);
        });
        tokio::task::yield_now().await;
        signal.certify_source_tip(canonical_tip);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rehydration_receiver)
                .await
                .ok()
                .and_then(Result::ok),
            Some(true),
            "snapshot completion must interrupt the wait so canonical state is observed again"
        );
        let after_fresh_observation = gate_canonical_readiness_on_mempool_hydration(
            canonical_ready,
            canonical_tip,
            Some(&gate),
        );
        assert!(matches!(
            &after_fresh_observation.cause,
            ReadinessCause::Ready
        ));
    }

    #[test]
    fn settlement_advances_one_block_only_after_the_next_tip_exceeds_window_two() {
        let first_retained = BlockHeight::new(1);
        let settled = BlockHeight::new(1);

        assert_eq!(
            next_settled_height(first_retained, settled, BlockHeight::new(3), 2),
            settled
        );
        assert_eq!(
            next_settled_height(first_retained, settled, BlockHeight::new(4), 2),
            BlockHeight::new(2)
        );
    }

    #[test]
    fn live_ranges_fetch_only_newly_completed_subtrees() -> Result<(), Box<dyn std::error::Error>> {
        let previous = ChainTipMetadata::new(SUBTREE_LEAF_COUNT * 2 + 9, SUBTREE_LEAF_COUNT, 0);
        let next = ChainTipMetadata::new(SUBTREE_LEAF_COUNT * 4, SUBTREE_LEAF_COUNT * 2 + 1, 12);

        assert_eq!(
            live_subtree_root_ranges(previous, next)?,
            vec![
                SubtreeRootRange::new(
                    ShieldedProtocol::Sapling,
                    SubtreeRootIndex::new(2),
                    NonZeroU32::new(2).ok_or("Sapling range must be nonzero")?,
                ),
                SubtreeRootRange::new(
                    ShieldedProtocol::Orchard,
                    SubtreeRootIndex::new(1),
                    NonZeroU32::new(1).ok_or("Orchard range must be nonzero")?,
                ),
            ]
        );
        Ok(())
    }

    #[test]
    fn live_ranges_reject_completed_subtree_regression() {
        let outcome = live_subtree_root_ranges(
            ChainTipMetadata::new(SUBTREE_LEAF_COUNT, 0, 0),
            ChainTipMetadata::empty(),
        );

        assert!(outcome.is_err());
    }
}
