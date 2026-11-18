//! Background densification of settled tree-state checkpoints.
//!
//! Construction checkpoints the commitment trees every 100 blocks, so a
//! tree-state read between two checkpoints replays stored blocks. This pass
//! fills each settled stride with the checkpoints its selection picks, so
//! those reads become stored-checkpoint reads. A canonical secondary replays
//! the stride on a bounded number of blocking threads, and the writer commits
//! the rows through its command queue between live commits. The pass runs only
//! while historical work may run, and resumes from the first stride whose
//! stored checkpoints do not satisfy the selection, so a restart neither
//! repeats committed strides nor needs a progress record.

use std::{
    collections::VecDeque,
    future::Future,
    num::NonZeroU32,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::RwLock;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use zinder_core::{BlockHeight, NetworkUpgradeActivations};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreError, CanonicalStoreWorkload, DensifiedTreeStateStride,
    RawBlobRetention, RocksDbCanonicalSecondary, RocksDbResourceBudget,
    TREE_STATE_CHECKPOINT_STRIDE, TreeStateCheckpointSelection, TreeStateStride,
    TreeStateStrideCommit,
};

use crate::{
    runtime_config::{
        HistoricalWorkGate, sleep_or_cancel, wait_until_historical_work_or_cancelled,
    },
    writer::control::CanonicalControlHandle,
};

/// Default number of strides replayed at once.
pub const DEFAULT_TREE_STATE_DENSIFICATION_WORKERS: u32 = 2;
/// Default ceiling on densified checkpoint rows committed per second.
pub const DEFAULT_TREE_STATE_DENSIFICATION_MAX_ROWS_PER_SECOND: u32 = 4_000;
/// Default commitment budget of the budgeted selection.
pub const DEFAULT_TREE_STATE_COMMITMENT_BUDGET: u32 = 1_024;

/// How long the pass waits before looking for newly settled strides once every
/// settled stride satisfies the selection.
const CAUGHT_UP_POLL_INTERVAL: Duration = Duration::from_mins(5);
/// How long the pass waits before retrying after a store or writer error.
const RETRY_INTERVAL: Duration = Duration::from_secs(30);
/// How often the pass reopens its canonical secondary when no store is
/// published yet.
const OPEN_RETRY_INTERVAL: Duration = Duration::from_secs(10);

/// Resolved `[ingest.tree_state_densification]` settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TreeStateDensificationConfig {
    /// Whether the pass runs.
    pub enabled: bool,
    /// Which blocks of a stride receive a stored checkpoint.
    pub selection: TreeStateCheckpointSelection,
    /// Strides replayed at once, each on one blocking thread.
    pub workers: NonZeroU32,
    /// Ceiling on densified rows committed per second.
    pub max_rows_per_second: NonZeroU32,
}

impl Default for TreeStateDensificationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            selection: TreeStateCheckpointSelection::EveryBlock,
            workers: NonZeroU32::new(DEFAULT_TREE_STATE_DENSIFICATION_WORKERS)
                .unwrap_or(NonZeroU32::MIN),
            max_rows_per_second: NonZeroU32::new(
                DEFAULT_TREE_STATE_DENSIFICATION_MAX_ROWS_PER_SECOND,
            )
            .unwrap_or(NonZeroU32::MIN),
        }
    }
}

/// The canonical writer the pass hands densified strides to.
pub trait TreeStateStrideWriter: Send + Sync + 'static {
    /// Commits `stride` between live commits and returns what it wrote.
    fn commit_tree_state_stride(
        &self,
        stride: DensifiedTreeStateStride,
    ) -> impl Future<Output = Result<TreeStateStrideCommit, TreeStateStrideWriteError>> + Send;
}

/// Why the writer did not commit a densified stride.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TreeStateStrideWriteError {
    /// The stored checkpoints contradict the stride's replay, so the stride is
    /// skipped and reported.
    #[error("tree-state stride contradicts stored checkpoints: {reason}")]
    Integrity {
        /// Exact check that failed.
        reason: String,
    },
    /// The writer could not commit now, so the pass retries later.
    #[error("canonical writer did not commit the tree-state stride: {reason}")]
    Unavailable {
        /// Why the writer refused or did not answer.
        reason: String,
    },
}

impl TreeStateStrideWriter for CanonicalControlHandle {
    async fn commit_tree_state_stride(
        &self,
        stride: DensifiedTreeStateStride,
    ) -> Result<TreeStateStrideCommit, TreeStateStrideWriteError> {
        Self::commit_tree_state_stride(self, stride)
            .await
            .map_err(|status| {
                let reason = status.message().to_owned();
                if status.code() == tonic::Code::DataLoss {
                    TreeStateStrideWriteError::Integrity { reason }
                } else {
                    TreeStateStrideWriteError::Unavailable { reason }
                }
            })
    }
}

/// Everything the pass needs to open its canonical secondary and reach the
/// writer.
pub struct TreeStateDensificationContext<Writer> {
    /// Path of the canonical primary the writer publishes.
    pub canonical_primary_path: PathBuf,
    /// Path of this pass's own canonical secondary.
    pub secondary_path: PathBuf,
    /// Node-observed activations used to admit the store and replay strides.
    pub network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    /// Workload the canonical store was built for.
    pub workload: CanonicalStoreWorkload,
    /// Raw-blob retention the canonical store was built with.
    pub raw_blob_retention: RawBlobRetention,
    /// Reorg policy the canonical store was built with.
    pub reorg_policy: CanonicalReorgPolicy,
    /// `RocksDB` budget of the secondary.
    pub resource_budget: RocksDbResourceBudget,
    /// Writer that commits densified strides.
    pub writer: Writer,
    /// Gate that admits historical work while the writer follows the tip.
    pub gate: HistoricalWorkGate,
    /// Resolved pass settings.
    pub config: TreeStateDensificationConfig,
}

/// Spawns the densification pass, which returns once `cancel` fires or at once
/// when the pass is disabled.
#[must_use]
pub fn spawn_tree_state_densification_task<Writer: TreeStateStrideWriter>(
    context: TreeStateDensificationContext<Writer>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(run_tree_state_densification(context, cancel))
}

async fn run_tree_state_densification<Writer: TreeStateStrideWriter>(
    context: TreeStateDensificationContext<Writer>,
    cancel: CancellationToken,
) {
    if !context.config.enabled {
        tracing::info!(
            target: "zinder::ingest",
            event = "tree_state_densification_disabled",
            "tree-state densification is disabled by configuration"
        );
        return;
    }
    let Some(secondary) = open_secondary_when_published(&context, &cancel).await else {
        return;
    };
    tracing::info!(
        target: "zinder::ingest",
        event = "tree_state_densification_started",
        selection = ?context.config.selection,
        workers = context.config.workers.get(),
        max_rows_per_second = context.config.max_rows_per_second.get(),
        "tree-state densification started"
    );
    let mut pass = DensificationPass::new(Arc::new(RwLock::new(secondary)), &context);
    loop {
        if wait_until_historical_work_or_cancelled(&context.gate, &cancel).await {
            return;
        }
        let pause = match pass.step(&context.writer, &cancel).await {
            Ok(StepOutcome::Progressed) => continue,
            Ok(StepOutcome::Cancelled) => return,
            Ok(StepOutcome::CaughtUp) => {
                pass.log_caught_up();
                CAUGHT_UP_POLL_INTERVAL
            }
            Err(reason) => {
                metrics::counter!("zinder_ingest_tree_state_densification_retries_total")
                    .increment(1);
                tracing::warn!(
                    target: "zinder::ingest",
                    event = "tree_state_densification_retry",
                    reason = %reason,
                    retry_interval_seconds = RETRY_INTERVAL.as_secs(),
                    "tree-state densification stopped on a transient error and will retry"
                );
                RETRY_INTERVAL
            }
        };
        if sleep_or_cancel(pause, &cancel).await {
            return;
        }
    }
}

async fn open_secondary_when_published<Writer: TreeStateStrideWriter>(
    context: &TreeStateDensificationContext<Writer>,
    cancel: &CancellationToken,
) -> Option<RocksDbCanonicalSecondary> {
    let mut waiting_logged = false;
    loop {
        match RocksDbCanonicalSecondary::open_ready(
            &context.canonical_primary_path,
            &context.secondary_path,
            &context.network_upgrade_activations,
            context.workload,
            context.raw_blob_retention,
            context.reorg_policy,
            context.resource_budget,
        ) {
            Ok(secondary) => return Some(secondary),
            Err(error) if !waiting_logged => {
                waiting_logged = true;
                tracing::info!(
                    target: "zinder::ingest",
                    event = "tree_state_densification_secondary_unavailable",
                    error = %error,
                    retry_interval_seconds = OPEN_RETRY_INTERVAL.as_secs(),
                    "waiting for a published canonical store before densifying tree states"
                );
            }
            Err(_) => {}
        }
        if sleep_or_cancel(OPEN_RETRY_INTERVAL, cancel).await {
            return None;
        }
    }
}

enum StepOutcome {
    Progressed,
    CaughtUp,
    Cancelled,
}

/// Resumable state of one run of the pass.
///
/// The first scan of a run walks every settled stride once; later scans start
/// where the previous one ended, so only newly settled strides are examined
/// again. A stride skipped as an integrity failure is revisited only by the
/// next run's first scan.
struct DensificationPass {
    secondary: Arc<RwLock<RocksDbCanonicalSecondary>>,
    activations: Arc<NetworkUpgradeActivations>,
    config: TreeStateDensificationConfig,
    /// Strides found by the last scan and not yet committed or skipped.
    queue: VecDeque<TreeStateStride>,
    /// Height the next scan starts from.
    scan_from: BlockHeight,
    caught_up_logged: bool,
}

impl DensificationPass {
    fn new<Writer>(
        secondary: Arc<RwLock<RocksDbCanonicalSecondary>>,
        context: &TreeStateDensificationContext<Writer>,
    ) -> Self {
        Self {
            secondary,
            activations: Arc::clone(&context.network_upgrade_activations),
            config: context.config,
            queue: VecDeque::new(),
            scan_from: BlockHeight::new(0),
            caught_up_logged: false,
        }
    }

    /// Densifies and commits up to one queued stride per worker, scanning for
    /// strides first when none are queued.
    async fn step(
        &mut self,
        writer: &impl TreeStateStrideWriter,
        cancel: &CancellationToken,
    ) -> Result<StepOutcome, String> {
        if self.queue.is_empty() {
            let scan = self.scan().await?;
            self.scan_from = scan.next_scan_from;
            self.queue = scan.strides;
            record_remaining(self.queue.len());
            if self.queue.is_empty() {
                return Ok(StepOutcome::CaughtUp);
            }
        }
        self.caught_up_logged = false;
        let workers = usize::try_from(self.config.workers.get()).unwrap_or(usize::MAX);
        let batch = self
            .queue
            .drain(..workers.min(self.queue.len()))
            .collect::<Vec<_>>();
        let replays = match self.catch_up_and_replay(&batch).await {
            Ok(replays) => replays,
            Err(reason) => {
                for retried in batch.iter().rev() {
                    self.queue.push_front(*retried);
                }
                return Err(reason);
            }
        };
        let mut committed = 0;
        for (stride, replay) in batch.iter().zip(replays) {
            match self.commit(writer, *stride, replay).await {
                Ok(rows_written) => {
                    committed += 1;
                    record_remaining(self.queue.len() + batch.len() - committed);
                    if self.pace(rows_written, cancel).await {
                        return Ok(StepOutcome::Cancelled);
                    }
                }
                Err(reason) => {
                    // This stride and the rest of the batch are retried first.
                    for retried in batch[committed..].iter().rev() {
                        self.queue.push_front(*retried);
                    }
                    return Err(reason);
                }
            }
        }
        Ok(StepOutcome::Progressed)
    }

    /// Catches the secondary up and lists every settled stride from
    /// `scan_from` that does not satisfy the selection, on a blocking thread.
    async fn scan(&self) -> Result<StrideScan, String> {
        let secondary = Arc::clone(&self.secondary);
        let selection = self.config.selection;
        let from = self.scan_from;
        tokio::task::spawn_blocking(move || {
            secondary.write().try_catch_up()?;
            scan_strides(&secondary.read(), from, selection)
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())
    }

    /// Replays `strides` in parallel, one blocking thread each.
    /// Catches the secondary up to the primary, so a long queue never reads
    /// files the primary has since compacted away, then replays `strides`.
    async fn catch_up_and_replay(
        &self,
        strides: &[TreeStateStride],
    ) -> Result<Vec<Result<DensifiedTreeStateStride, CanonicalStoreError>>, String> {
        let secondary = Arc::clone(&self.secondary);
        tokio::task::spawn_blocking(move || secondary.write().try_catch_up().map(drop))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        self.replay(strides).await
    }

    async fn replay(
        &self,
        strides: &[TreeStateStride],
    ) -> Result<Vec<Result<DensifiedTreeStateStride, CanonicalStoreError>>, String> {
        let replays = strides.iter().map(|stride| {
            let secondary = Arc::clone(&self.secondary);
            let activations = Arc::clone(&self.activations);
            let selection = self.config.selection;
            let stride = *stride;
            tokio::task::spawn_blocking(move || {
                let started_at = Instant::now();
                let replay =
                    secondary
                        .read()
                        .densify_tree_state_stride(stride, selection, &activations);
                metrics::histogram!(
                    "zinder_ingest_tree_state_densification_stride_duration_seconds"
                )
                .record(started_at.elapsed());
                replay
            })
        });
        futures_util::future::join_all(replays)
            .await
            .into_iter()
            .map(|joined| joined.map_err(|error| error.to_string()))
            .collect()
    }

    /// Commits one replayed stride, returning the rows written.
    ///
    /// A stride whose replay contradicts the stored checkpoints is recorded
    /// as an integrity failure and skipped. Any other error is returned for
    /// a retry.
    async fn commit(
        &self,
        writer: &impl TreeStateStrideWriter,
        stride: TreeStateStride,
        replay: Result<DensifiedTreeStateStride, CanonicalStoreError>,
    ) -> Result<u32, String> {
        let densified = match replay {
            Ok(densified) => densified,
            Err(error @ CanonicalStoreError::TreeStateStrideMismatch { .. }) => {
                record_integrity_failure(
                    stride.seed_height(),
                    stride.end_height(),
                    &error.to_string(),
                );
                return Ok(0);
            }
            Err(error) => return Err(error.to_string()),
        };
        let replayed_commitments = densified.replayed_commitment_count();
        match writer.commit_tree_state_stride(densified).await {
            Ok(commit) => {
                metrics::counter!(
                    "zinder_ingest_tree_state_densification_strides_total",
                    "outcome" => "densified"
                )
                .increment(1);
                metrics::counter!("zinder_ingest_tree_state_densification_rows_written_total")
                    .increment(u64::from(commit.rows_written));
                metrics::counter!(
                    "zinder_ingest_tree_state_densification_replayed_commitments_total"
                )
                .increment(replayed_commitments);
                Ok(commit.rows_written)
            }
            Err(TreeStateStrideWriteError::Integrity { reason }) => {
                record_integrity_failure(stride.seed_height(), stride.end_height(), &reason);
                Ok(0)
            }
            Err(error) => Err(error.to_string()),
        }
    }

    /// Holds the pass so committed rows stay under the configured rate.
    async fn pace(&self, rows_written: u32, cancel: &CancellationToken) -> bool {
        let pause = Duration::from_secs_f64(
            f64::from(rows_written) / f64::from(self.config.max_rows_per_second.get()),
        );
        cancel.is_cancelled() || (!pause.is_zero() && sleep_or_cancel(pause, cancel).await)
    }

    fn log_caught_up(&mut self) {
        if self.caught_up_logged {
            return;
        }
        self.caught_up_logged = true;
        tracing::info!(
            target: "zinder::ingest",
            event = "tree_state_densification_caught_up",
            "every settled stride holds the selected tree-state checkpoints"
        );
    }
}

/// Lists every settled stride from `from` that does not satisfy `selection`.
fn scan_strides(
    secondary: &RocksDbCanonicalSecondary,
    from: BlockHeight,
    selection: TreeStateCheckpointSelection,
) -> Result<StrideScan, CanonicalStoreError> {
    let settled_height = secondary.sequence_checkpoint().through().height;
    let mut strides = VecDeque::new();
    let mut next = from;
    while let Some(stride) = next_stride(secondary, next, selection)? {
        next = stride.end_height();
        strides.push_back(stride);
    }
    // Every stride ending at or below the settled tip was examined, so the
    // next scan starts at the seed of the first one that was not.
    let unsettled_seed = BlockHeight::new(
        settled_height.value() - settled_height.value() % TREE_STATE_CHECKPOINT_STRIDE,
    );
    Ok(StrideScan {
        strides,
        next_scan_from: from.max(unsettled_seed),
    })
}

/// Strides one scan found and where the next scan starts.
struct StrideScan {
    strides: VecDeque<TreeStateStride>,
    next_scan_from: BlockHeight,
}

/// Returns the next stride to densify at or above `from`.
///
/// A stride whose stored rows or blocks are damaged cannot be densified, so it
/// is recorded as an integrity failure and skipped.
fn next_stride(
    secondary: &RocksDbCanonicalSecondary,
    mut from: BlockHeight,
    selection: TreeStateCheckpointSelection,
) -> Result<Option<TreeStateStride>, CanonicalStoreError> {
    loop {
        match secondary.next_tree_state_stride(from, selection) {
            Err(
                error @ CanonicalStoreError::TreeStateStrideMismatch {
                    seed_height,
                    end_height,
                    ..
                },
            ) => {
                record_integrity_failure(seed_height, end_height, &error.to_string());
                from = end_height;
            }
            found => return found,
        }
    }
}

fn record_remaining(remaining: usize) {
    #[allow(
        clippy::cast_precision_loss,
        reason = "a stride count stays far below 2^52"
    )]
    metrics::gauge!("zinder_ingest_tree_state_densification_remaining_strides")
        .set(remaining as f64);
}

fn record_integrity_failure(seed_height: BlockHeight, end_height: BlockHeight, reason: &str) {
    metrics::counter!(
        "zinder_ingest_tree_state_densification_strides_total",
        "outcome" => "integrity_failure"
    )
    .increment(1);
    tracing::error!(
        target: "zinder::ingest",
        event = "tree_state_densification_integrity_failure",
        seed_height = seed_height.value(),
        end_height = end_height.value(),
        reason,
        "a settled tree-state stride does not replay to its stored checkpoints; it was not densified and reads there keep replaying"
    );
}
