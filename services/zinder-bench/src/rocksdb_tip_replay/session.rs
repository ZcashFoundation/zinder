//! One replay over scratch stores: each fixture block goes through the
//! production canonical follower, then the production wallet transition.

use std::{
    num::{NonZeroU32, NonZeroU64},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use zinder_bench::{BenchError, tip_replay_checkpoints::TipReplayNodeSource};
use zinder_core::{BlockHeight, BlockId, Network, NetworkUpgradeActivations};
use zinder_ingest::{
    CanonicalFollowConfig, CanonicalFollower, DEFAULT_MAX_REPLACEMENT_BYTES,
    DEFAULT_TIP_FOLLOW_LAG_THRESHOLD_BLOCKS, follow_canonical_tip,
};
use zinder_runtime::{ComposedReadiness, Readiness};
use zinder_store::{
    CanonicalEventFence, CanonicalEventHistoryRequest, CanonicalEventKind, CanonicalReorgPolicy,
    CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    RocksDbResourceBudget,
};
use zinder_wallet_projection::{
    WalletCanonicalSourceIdentity, WalletProjectionReadyEvidence, WalletProjectionSourcePosition,
};
use zinder_wallet_rocksdb::{RocksDbWalletFollowingStore, RocksDbWalletStore};

use super::measurement::{BlockTimings, BlockWorkload};

/// Production histogram recorded around `RocksDbCanonicalStore::commit_live_append`.
const LIVE_COMMIT_HISTOGRAM: &str = "zinder_ingest_canonical_live_commit_seconds";
/// Production histogram recorded around live block preparation.
const LIVE_PREPARE_HISTOGRAM: &str = "zinder_ingest_canonical_live_prepare_seconds";
/// Production histogram recorded around a live suffix replacement.
const LIVE_REPLACEMENT_HISTOGRAM: &str = "zinder_ingest_canonical_live_replacement_commit_seconds";
const SOURCE_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Store settings shared by every repeat.
pub(super) struct StoreSettings {
    pub(super) network: Network,
    pub(super) fixture_activations: NetworkUpgradeActivations,
    pub(super) raw_blob_retention: RawBlobRetention,
    pub(super) reorg_policy: CanonicalReorgPolicy,
    pub(super) canonical_writer_budget: RocksDbResourceBudget,
    pub(super) canonical_secondary_budget: RocksDbResourceBudget,
    pub(super) wallet_budget: RocksDbResourceBudget,
    pub(super) max_transition_logical_bytes: NonZeroU64,
}

/// Scratch store paths for one repeat.
pub(super) struct StorePaths<'a> {
    pub(super) canonical: &'a Path,
    pub(super) canonical_secondary: &'a Path,
    pub(super) wallet: &'a Path,
}

/// Canonical fence and settled tip at one point of a replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CanonicalPosition {
    pub(super) fence: CanonicalEventFence,
    pub(super) settled_tip: BlockId,
}

/// I/O modes `RocksDB` resolved for each open.
pub(super) struct ResolvedIoModes {
    pub(super) canonical_primary: &'static str,
    pub(super) canonical_secondary: &'static str,
    pub(super) wallet: &'static str,
}

/// State left when a repeat's stores close.
pub(super) struct FinishedStores {
    pub(super) canonical: CanonicalPosition,
    pub(super) wallet: WalletProjectionReadyEvidence,
    pub(super) io_modes: ResolvedIoModes,
}

/// The canonical writer, its projector-shaped secondary, and the following wallet.
pub(super) struct ReplayStores {
    canonical: Option<RocksDbCanonicalStore>,
    secondary: RocksDbCanonicalSecondary,
    wallet: RocksDbWalletFollowingStore,
    follow_activations: Arc<NetworkUpgradeActivations>,
    max_transition_logical_bytes: NonZeroU64,
    readiness: ComposedReadiness,
    cancel: CancellationToken,
}

impl ReplayStores {
    /// Opens the scratch stores the way `zinder-ingest` and `zinder-projector` do.
    pub(super) fn open(
        paths: &StorePaths<'_>,
        settings: &StoreSettings,
    ) -> Result<Self, BenchError> {
        let canonical = RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
            paths.canonical,
            &settings.fixture_activations,
            CanonicalStoreWorkload::Wallet,
            settings.raw_blob_retention,
            settings.reorg_policy,
            settings.canonical_writer_budget,
        )?;
        let follow_activations = Arc::new(
            canonical
                .network_upgrade_activations()
                .cloned()
                .ok_or_else(|| {
                    BenchError::tip_replay(
                        "the canonical writer did not admit an activation schedule",
                    )
                })?,
        );
        let effective_reorg_policy = canonical.reorg_policy();
        let secondary = RocksDbCanonicalSecondary::open_ready(
            paths.canonical,
            paths.canonical_secondary,
            &settings.fixture_activations,
            CanonicalStoreWorkload::Wallet,
            settings.raw_blob_retention,
            effective_reorg_policy,
            settings.canonical_secondary_budget,
        )?;
        let mut wallet = RocksDbWalletStore::open_ready_for_following(
            paths.wallet,
            settings.network,
            settings.wallet_budget,
        )?;
        wallet.raise_supported_reorg_depth(effective_reorg_policy.reorg_window_blocks())?;
        Ok(Self {
            canonical: Some(canonical),
            secondary,
            wallet,
            follow_activations,
            max_transition_logical_bytes: settings.max_transition_logical_bytes,
            readiness: ComposedReadiness::new(Readiness::default(), &[]),
            cancel: CancellationToken::new(),
        })
    }

    /// Returns the canonical position, after checking the wallet represents it.
    pub(super) fn start_position(&self) -> Result<CanonicalPosition, BenchError> {
        let position = self.canonical_position()?;
        if WalletCanonicalSourceIdentity::from_ready_evidence(self.wallet.ready_evidence())
            != wallet_source_identity(position)
        {
            return Err(BenchError::invalid_argument(
                "--wallet-store does not represent the canonical fence of --canonical-store",
            ));
        }
        Ok(position)
    }

    /// The wallet's READY evidence as currently persisted.
    pub(super) fn wallet_evidence(&self) -> &WalletProjectionReadyEvidence {
        self.wallet.ready_evidence()
    }

    /// The effective canonical reorg window after admission.
    pub(super) fn reorg_window_blocks(&self) -> Result<u32, BenchError> {
        Ok(self.canonical_store()?.reorg_policy().reorg_window_blocks())
    }

    /// Appends one fixture block, then applies its wallet transition.
    pub(super) fn replay_block(
        &mut self,
        runtime: &tokio::runtime::Runtime,
        source: &TipReplayNodeSource,
        recorder: &FollowStepRecorder,
        expected_tip: BlockId,
    ) -> Result<BlockTimings, BenchError> {
        let store = self.canonical.take().ok_or_else(|| {
            BenchError::tip_replay("the canonical writer was lost by an earlier step")
        })?;
        recorder.clear();
        let follower = CanonicalFollower::new(
            source,
            Arc::clone(&self.follow_activations),
            single_append_follow_config(expected_tip.height),
            &self.readiness,
            &self.cancel,
        );
        let follow_started = Instant::now();
        let store = runtime.block_on(follow_canonical_tip(store, follower))?;
        let canonical_follow_step = follow_started.elapsed();
        let fence = store.event_fence();
        self.canonical = Some(store);
        if fence.visible_tip() != expected_tip {
            return Err(BenchError::tip_replay(format!(
                "the canonical follower stopped at {:?} instead of fixture block {expected_tip:?}",
                fence.visible_tip()
            )));
        }
        let FollowStepSamples { commit, prepare } = recorder.take_single_append()?;

        let wallet_started = Instant::now();
        let catch_up_started = Instant::now();
        self.secondary.try_catch_up()?;
        let canonical_secondary_catch_up = catch_up_started.elapsed();
        let wallet_apply = self.apply_wallet_transition(fence)?;
        let wallet_step = wallet_started.elapsed();

        Ok(BlockTimings {
            canonical_follow_step: milliseconds(canonical_follow_step),
            canonical_prepare: prepare * 1_000.0,
            canonical_commit: commit * 1_000.0,
            canonical_secondary_catch_up: milliseconds(canonical_secondary_catch_up),
            wallet_apply: milliseconds(wallet_apply),
            wallet_step: milliseconds(wallet_step),
        })
    }

    /// Applies the one retained event that produced `fence`, as the projector
    /// does for an append, and returns the time spent in the wallet transition.
    fn apply_wallet_transition(
        &mut self,
        fence: CanonicalEventFence,
    ) -> Result<Duration, BenchError> {
        if self.secondary.event_fence() != fence {
            return Err(BenchError::tip_replay(
                "the canonical secondary did not catch up to the writer fence",
            ));
        }
        let wallet_source =
            WalletCanonicalSourceIdentity::from_ready_evidence(self.wallet.ready_evidence());
        let cursor = wallet_source.source_position().event_cursor.as_bytes();
        let events = self
            .secondary
            .canonical_event_history(CanonicalEventHistoryRequest::new(
                Some(&cursor),
                NonZeroU32::MIN,
            ))?;
        let [event] = events.as_slice() else {
            return Err(BenchError::tip_replay(format!(
                "expected one retained event after the wallet cursor, found {}",
                events.len()
            )));
        };
        let committed = event.committed_range();
        if !matches!(event.kind(), CanonicalEventKind::Committed)
            || event.resulting_fence() != fence
            || committed.start != fence.visible_tip().height
            || committed.end != fence.visible_tip().height
        {
            return Err(BenchError::tip_replay(
                "the next retained event is not the one-block append that produced the writer fence",
            ));
        }
        let epoch = self.secondary.chain_epoch_at(event.resulting_epoch_id())?;
        let position = CanonicalPosition {
            fence,
            settled_tip: BlockId::new(epoch.settled_tip_height, epoch.settled_tip_hash),
        };
        let replay_rows = self.secondary.scan_canonical_replay_range(committed)?;
        let apply_started = Instant::now();
        self.wallet.apply_canonical_event_range(
            wallet_source,
            *event,
            fence,
            position.settled_tip,
            self.max_transition_logical_bytes,
            replay_rows,
        )?;
        let wallet_apply = apply_started.elapsed();
        if WalletCanonicalSourceIdentity::from_ready_evidence(self.wallet.ready_evidence())
            != wallet_source_identity(position)
        {
            return Err(BenchError::tip_replay(
                "the wallet transition published a source other than the retained event's result",
            ));
        }
        Ok(wallet_apply)
    }

    /// Counts the transparent workload of retained blocks at `heights`.
    ///
    /// Heights outside the retained canonical range are skipped.
    pub(super) fn block_workloads(
        &self,
        heights: std::ops::RangeInclusive<u32>,
    ) -> Result<Vec<(u32, BlockWorkload)>, BenchError> {
        let mut workloads = Vec::new();
        for height in heights {
            if let Some(facts) = self
                .secondary
                .block_replay_facts_at(BlockHeight::new(height))?
            {
                workloads.push((height, BlockWorkload::from_facts(&facts)));
            }
        }
        Ok(workloads)
    }

    /// Closes the stores after checking the wallet converged on the writer's final fence.
    pub(super) fn finish(self) -> Result<FinishedStores, BenchError> {
        let canonical = self.canonical_position()?;
        let canonical_primary = self.canonical_store()?.io_mode().as_str();
        let canonical_secondary = self.secondary.io_mode().as_str();
        let wallet = self
            .wallet
            .into_ready_store(wallet_source_identity(canonical))?;
        Ok(FinishedStores {
            canonical,
            wallet: wallet.ready_evidence().clone(),
            io_modes: ResolvedIoModes {
                canonical_primary,
                canonical_secondary,
                wallet: wallet.io_mode().as_str(),
            },
        })
    }

    fn canonical_store(&self) -> Result<&RocksDbCanonicalStore, BenchError> {
        self.canonical.as_ref().ok_or_else(|| {
            BenchError::tip_replay("the canonical writer was lost by an earlier step")
        })
    }

    fn canonical_position(&self) -> Result<CanonicalPosition, BenchError> {
        let store = self.canonical_store()?;
        let epoch = store.chain_epoch()?;
        Ok(CanonicalPosition {
            fence: store.event_fence(),
            settled_tip: BlockId::new(epoch.settled_tip_height, epoch.settled_tip_hash),
        })
    }
}

/// Follows to exactly one more block. Event-retention pruning stays off: the
/// writer runs it at most once per interval, but each call here starts a new
/// follow loop, which would prune before every block.
fn single_append_follow_config(target_height: BlockHeight) -> CanonicalFollowConfig {
    CanonicalFollowConfig {
        request_timeout: SOURCE_REQUEST_TIMEOUT,
        poll_interval: Duration::ZERO,
        lag_threshold_blocks: DEFAULT_TIP_FOLLOW_LAG_THRESHOLD_BLOCKS,
        target_height: Some(target_height),
        event_retention_window: None,
        event_retention_check_interval: Duration::from_mins(1),
        mempool_ready_gate: None,
        max_replacement_bytes: DEFAULT_MAX_REPLACEMENT_BYTES,
    }
}

fn wallet_source_identity(position: CanonicalPosition) -> WalletCanonicalSourceIdentity {
    WalletCanonicalSourceIdentity::new(
        WalletProjectionSourcePosition::new(
            position.fence.chain_epoch_id(),
            position.fence.visible_tip(),
            position.fence.chain_event_sequence(),
        ),
        position.fence.sequence_digest(),
        position.settled_tip,
    )
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

/// The production timings, in seconds, one follow iteration recorded for its append.
struct FollowStepSamples {
    commit: f64,
    prepare: f64,
}

/// Histogram samples, in seconds, recorded since the last follow step began.
#[derive(Default)]
struct RecordedHistograms {
    commits: Vec<f64>,
    preparations: Vec<f64>,
    replacements: Vec<f64>,
}

/// Thread-local metrics recorder that keeps the follower's commit and
/// prepare histogram samples, so the report uses the production timers.
///
/// It must be the default recorder of the thread that drives the follower
/// on a current-thread runtime; every other metric is discarded.
#[derive(Default)]
pub(super) struct FollowStepRecorder {
    histograms: Arc<Mutex<RecordedHistograms>>,
}

impl FollowStepRecorder {
    fn clear(&self) {
        *self.histograms.lock() = RecordedHistograms::default();
    }

    fn take_single_append(&self) -> Result<FollowStepSamples, BenchError> {
        let recorded = std::mem::take(&mut *self.histograms.lock());
        match (
            recorded.commits.as_slice(),
            recorded.preparations.as_slice(),
            recorded.replacements.is_empty(),
        ) {
            ([commit], [prepare], true) => Ok(FollowStepSamples {
                commit: *commit,
                prepare: *prepare,
            }),
            _ => Err(BenchError::tip_replay(format!(
                "one follow step must record exactly one live append; it recorded {} commits, {} preparations, and {} replacements",
                recorded.commits.len(),
                recorded.preparations.len(),
                recorded.replacements.len()
            ))),
        }
    }
}

#[derive(Clone, Copy)]
enum FollowHistogram {
    Commit,
    Prepare,
    Replacement,
}

struct RecordedHistogram {
    histogram: FollowHistogram,
    histograms: Arc<Mutex<RecordedHistograms>>,
}

impl metrics::HistogramFn for RecordedHistogram {
    fn record(&self, sample: f64) {
        let mut histograms = self.histograms.lock();
        match self.histogram {
            FollowHistogram::Commit => histograms.commits.push(sample),
            FollowHistogram::Prepare => histograms.preparations.push(sample),
            FollowHistogram::Replacement => histograms.replacements.push(sample),
        }
    }
}

impl metrics::Recorder for FollowStepRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn register_counter(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Counter {
        metrics::Counter::noop()
    }

    fn register_gauge(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        let histogram = match key.name() {
            LIVE_COMMIT_HISTOGRAM => FollowHistogram::Commit,
            LIVE_PREPARE_HISTOGRAM => FollowHistogram::Prepare,
            LIVE_REPLACEMENT_HISTOGRAM => FollowHistogram::Replacement,
            _ => return metrics::Histogram::noop(),
        };
        metrics::Histogram::from_arc(Arc::new(RecordedHistogram {
            histogram,
            histograms: Arc::clone(&self.histograms),
        }))
    }
}
