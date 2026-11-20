//! Background densification of settled tree-state checkpoints.
//!
//! Construction checkpoints the commitment trees every 100 blocks, so a
//! tree-state read between two checkpoints replays stored blocks. This pass
//! fills each settled stride with the checkpoints its selection picks, so
//! those reads become stored-checkpoint reads. A canonical secondary replays
//! the stride on a bounded number of blocking threads, and the writer commits
//! the rows through its command queue between live commits. The pass runs only
//! while historical work may run. It records its progress as a watermark in
//! [`TREE_STATE_DENSIFICATION_WATERMARK_FILE`] in the canonical store
//! directory, bound to the store's construction manifest and the selection. A
//! restart resumes at that watermark once the stride ending there still
//! satisfies the selection, and otherwise scans from the first stride. A
//! stride skipped as an integrity failure holds the watermark below it, so the
//! next run examines it again.

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
    MAX_TREE_STATE_STRIDE_SCAN_READS, RawBlobRetention, RocksDbCanonicalSecondary,
    RocksDbResourceBudget, TREE_STATE_CHECKPOINT_STRIDE, TreeStateCheckpointSelection,
    TreeStateStride, TreeStateStrideCommit,
};

use crate::{
    background_pass::{BackgroundSecondary, write_progress_record},
    runtime_config::{
        HistoricalWorkGate, sleep_or_cancel, wait_until_historical_work_or_cancelled,
    },
    writer::control::CanonicalControlHandle,
};

/// Default number of strides replayed at once.
pub const DEFAULT_TREE_STATE_DENSIFICATION_WORKERS: u32 = 2;
/// Default average ceiling on stored blocks the pass replays per second.
///
/// Replaying a block reads its compact block and header, and a scan is
/// charged for every checkpoint row and compact block it reads, so this also
/// bounds the pass's reads, and the rows it writes, which never exceed the
/// blocks it replays.
pub const DEFAULT_TREE_STATE_DENSIFICATION_MAX_BLOCKS_PER_SECOND: u32 = 500;
/// Default commitment budget of the budgeted selection, which is not the
/// default selection.
pub const DEFAULT_TREE_STATE_COMMITMENT_BUDGET: u32 = 1_024;

/// How long the pass waits before looking for newly settled strides once every
/// settled stride satisfies the selection.
const CAUGHT_UP_POLL_INTERVAL: Duration = Duration::from_mins(5);
/// How long the pass waits before retrying after a store or writer error.
const RETRY_INTERVAL: Duration = Duration::from_secs(30);
/// File, inside the canonical store directory, in which the pass records its
/// progress.
pub const TREE_STATE_DENSIFICATION_WATERMARK_FILE: &str = "tree-state-densification-watermark.json";
const WATERMARK_VERSION: u32 = 1;
/// Least time between two progress records while strides are committed.
const WATERMARK_WRITE_INTERVAL: Duration = Duration::from_secs(10);
/// Scan reads charged as one replayed block against the replay rate.
///
/// A replay reads a compact block and a header for each block, and a scan
/// counts each checkpoint row it visits and each compact block it reads.
const SCAN_READS_PER_BLOCK: u32 = 2;
/// Reads one scan chunk makes before the pass paces it, the reads of one
/// densified stride, so a scan bursts no more than a replay does.
const SCAN_CHUNK_READS: u32 = SCAN_READS_PER_BLOCK * TREE_STATE_CHECKPOINT_STRIDE;
/// Replayed blocks charged for a scan chunk that failed, so its reads are
/// unknown.
///
/// A chunk stops only after the stride that reaches [`SCAN_CHUNK_READS`], so
/// it makes up to that many reads less one, plus the most one more stride
/// adds.
const FAILED_SCAN_CHARGE_BLOCKS: u32 =
    (SCAN_CHUNK_READS + MAX_TREE_STATE_STRIDE_SCAN_READS).div_ceil(SCAN_READS_PER_BLOCK);

/// Resolved `[ingest.tree_state_densification]` settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TreeStateDensificationConfig {
    /// Whether the pass runs.
    pub enabled: bool,
    /// Which blocks of a stride receive a stored checkpoint.
    pub selection: TreeStateCheckpointSelection,
    /// Strides replayed at once, each on one blocking thread.
    pub workers: NonZeroU32,
    /// Ceiling on stored blocks replayed per second, which bounds the pass's
    /// reads and writes.
    pub max_blocks_per_second: NonZeroU32,
}

impl Default for TreeStateDensificationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            selection: TreeStateCheckpointSelection::EveryBlock,
            workers: NonZeroU32::new(DEFAULT_TREE_STATE_DENSIFICATION_WORKERS)
                .unwrap_or(NonZeroU32::MIN),
            max_blocks_per_second: NonZeroU32::new(
                DEFAULT_TREE_STATE_DENSIFICATION_MAX_BLOCKS_PER_SECOND,
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
    let Some(secondary) = BackgroundSecondary {
        canonical_primary_path: &context.canonical_primary_path,
        secondary_path: &context.secondary_path,
        network_upgrade_activations: &context.network_upgrade_activations,
        workload: context.workload,
        raw_blob_retention: context.raw_blob_retention,
        reorg_policy: context.reorg_policy,
        resource_budget: context.resource_budget,
    }
    .open_when_published(
        || context.gate.record_open_metric(),
        &cancel,
        "tree_state_densification_secondary_unavailable",
    )
    .await
    else {
        return;
    };
    tracing::info!(
        target: "zinder::ingest",
        event = "tree_state_densification_started",
        selection = ?context.config.selection,
        workers = context.config.workers.get(),
        max_blocks_per_second = context.config.max_blocks_per_second.get(),
        "tree-state densification started"
    );
    let mut pass = DensificationPass::resume(Arc::new(RwLock::new(secondary)), &context).await;
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

enum StepOutcome {
    Progressed,
    CaughtUp,
    Cancelled,
}

/// Resumable state of one run of the pass.
///
/// A run resumes at its verified watermark, or at the first stride when there
/// is none, and scans forward in bounded chunks. Later scans start where the
/// previous one ended, so only newly settled strides are examined again. A
/// stride skipped as an integrity failure holds the watermark below it, so the
/// next run examines it again.
struct DensificationPass {
    secondary: Arc<RwLock<RocksDbCanonicalSecondary>>,
    activations: Arc<NetworkUpgradeActivations>,
    config: TreeStateDensificationConfig,
    watermark_file: WatermarkFile,
    /// Strides found by scanning and not yet committed or skipped.
    queue: VecDeque<TreeStateStride>,
    /// Height the next scan chunk starts from.
    scan_from: BlockHeight,
    /// Whether the last scan chunk reached the first unsettled stride.
    scan_reached_unsettled: bool,
    /// Height of the history predecessor, below every stride end.
    predecessor_height: BlockHeight,
    /// Lowest seed of a stride skipped as an integrity failure in this run.
    damaged_floor: Option<BlockHeight>,
    /// Height the watermark file records for this run's selection.
    watermark: Option<BlockHeight>,
    last_watermark_write: Instant,
    caught_up_logged: bool,
}

impl DensificationPass {
    /// Starts a run at the recorded watermark when the stored rows confirm it.
    async fn resume<Writer: TreeStateStrideWriter>(
        secondary: Arc<RwLock<RocksDbCanonicalSecondary>>,
        context: &TreeStateDensificationContext<Writer>,
    ) -> Self {
        let predecessor_height = secondary
            .read()
            .build_plan()
            .history_predecessor()
            .block_id
            .height;
        let watermark_file = WatermarkFile::new(
            &context.canonical_primary_path,
            secondary
                .read()
                .ready_evidence()
                .construction_manifest_sha256,
            context.config.selection,
        );
        let watermark = {
            let secondary = Arc::clone(&secondary);
            let watermark_file = watermark_file.clone();
            let verified =
                tokio::task::spawn_blocking(move || watermark_file.verified(&secondary.read()))
                    .await
                    .map_err(|error| error.to_string())
                    .and_then(|verified| verified.map_err(|error| error.to_string()));
            verified.unwrap_or_else(|reason| {
                tracing::warn!(
                    target: "zinder::ingest",
                    event = "tree_state_densification_resume_failed",
                    reason = %reason,
                    "tree-state densification could not check its recorded progress and scans every settled stride"
                );
                None
            })
        };
        if let Some(watermark) = watermark {
            metrics::gauge!("zinder_ingest_tree_state_densification_watermark_height")
                .set(f64::from(watermark.value()));
        }
        Self {
            secondary,
            activations: Arc::clone(&context.network_upgrade_activations),
            config: context.config,
            watermark_file,
            queue: VecDeque::new(),
            scan_from: watermark.unwrap_or(BlockHeight::new(0)),
            scan_reached_unsettled: false,
            predecessor_height,
            damaged_floor: None,
            watermark,
            last_watermark_write: Instant::now(),
            caught_up_logged: false,
        }
    }

    /// Densifies and commits up to one queued stride per worker, or scans one
    /// bounded chunk for strides when none are queued.
    async fn step(
        &mut self,
        writer: &impl TreeStateStrideWriter,
        cancel: &CancellationToken,
    ) -> Result<StepOutcome, String> {
        if self.queue.is_empty() {
            if self.scan_reached_unsettled {
                self.scan_reached_unsettled = false;
                self.record_watermark(true).await;
                return Ok(StepOutcome::CaughtUp);
            }
            let reads = match self.scan_chunk().await {
                Ok(reads) => reads,
                Err(reason) => {
                    // A failed chunk loses its read count, so the retry is
                    // charged the most a chunk can read.
                    if self.pace(FAILED_SCAN_CHARGE_BLOCKS, cancel).await {
                        return Ok(StepOutcome::Cancelled);
                    }
                    return Err(reason);
                }
            };
            self.record_watermark(false).await;
            if self
                .pace(reads.div_ceil(SCAN_READS_PER_BLOCK), cancel)
                .await
            {
                return Ok(StepOutcome::Cancelled);
            }
            return Ok(StepOutcome::Progressed);
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
                Ok(()) => {
                    committed += 1;
                    record_remaining(self.queue.len() + batch.len() - committed);
                    if self.pace(replayed_blocks(*stride), cancel).await {
                        return Ok(StepOutcome::Cancelled);
                    }
                }
                Err(reason) => {
                    // This stride and the rest of the batch are retried first.
                    // Their replays are discarded but were already made, so
                    // they count against the rate before the retry.
                    let discarded = &batch[committed..];
                    for retried in discarded.iter().rev() {
                        self.queue.push_front(*retried);
                    }
                    let discarded_blocks = discarded.iter().copied().map(replayed_blocks).sum();
                    if self.pace(discarded_blocks, cancel).await {
                        return Ok(StepOutcome::Cancelled);
                    }
                    return Err(reason);
                }
            }
        }
        self.record_watermark(false).await;
        Ok(StepOutcome::Progressed)
    }

    /// Catches the secondary up and examines one chunk of settled strides, up
    /// to [`SCAN_CHUNK_READS`] reads, on a blocking thread, returning the
    /// reads it made.
    async fn scan_chunk(&mut self) -> Result<u32, String> {
        let secondary = Arc::clone(&self.secondary);
        let selection = self.config.selection;
        let from = self.scan_from;
        let read_budget = NonZeroU32::new(SCAN_CHUNK_READS).unwrap_or(NonZeroU32::MIN);
        let scan = tokio::task::spawn_blocking(move || {
            secondary.write().try_catch_up()?;
            secondary
                .read()
                .scan_tree_state_strides(from, selection, read_budget)
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
        for (stride, reason) in &scan.damaged {
            self.record_damaged(*stride, reason);
        }
        self.queue.extend(scan.incomplete);
        self.scan_from = scan.next_from;
        self.scan_reached_unsettled = scan.reached_unsettled;
        record_remaining(self.queue.len());
        Ok(scan.reads)
    }

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
        &mut self,
        writer: &impl TreeStateStrideWriter,
        stride: TreeStateStride,
        replay: Result<DensifiedTreeStateStride, CanonicalStoreError>,
    ) -> Result<(), String> {
        let densified = match replay {
            Ok(densified) => densified,
            Err(error @ CanonicalStoreError::TreeStateStrideMismatch { .. }) => {
                self.record_damaged(stride, &error.to_string());
                return Ok(());
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
                Ok(())
            }
            Err(TreeStateStrideWriteError::Integrity { reason }) => {
                self.record_damaged(stride, &reason);
                Ok(())
            }
            Err(error) => Err(error.to_string()),
        }
    }

    /// Holds the pass so the blocks it replays, and the blocks a scan is
    /// charged for, stay under the configured rate on average.
    async fn pace(&self, blocks: u32, cancel: &CancellationToken) -> bool {
        let pause = Duration::from_secs_f64(
            f64::from(blocks) / f64::from(self.config.max_blocks_per_second.get()),
        );
        cancel.is_cancelled() || (!pause.is_zero() && sleep_or_cancel(pause, cancel).await)
    }

    fn record_damaged(&mut self, stride: TreeStateStride, reason: &str) {
        record_integrity_failure(stride.seed_height(), stride.end_height(), reason);
        let seed_height = stride.seed_height();
        self.damaged_floor = Some(
            self.damaged_floor
                .map_or(seed_height, |floor| floor.min(seed_height)),
        );
    }

    /// Records the highest height below which every settled stride is known
    /// to satisfy the selection, at most every
    /// [`WATERMARK_WRITE_INTERVAL`] unless `now` is set.
    ///
    /// Every stride below the lowest queued stride, the lowest damaged
    /// stride, and the next scan start was committed or found satisfied, and
    /// commits are synced before they return, so the file never claims a
    /// stride whose rows are not durable.
    async fn record_watermark(&mut self, now: bool) {
        let candidate = [
            self.queue.front().map(|stride| stride.seed_height()),
            self.damaged_floor,
            Some(self.scan_from),
        ]
        .into_iter()
        .flatten()
        .min()
        .filter(|height| {
            height.value().is_multiple_of(TREE_STATE_CHECKPOINT_STRIDE)
                && *height > self.predecessor_height
        });
        let Some(candidate) = candidate else {
            return;
        };
        if self
            .watermark
            .is_some_and(|watermark| watermark >= candidate)
            || (!now && self.last_watermark_write.elapsed() < WATERMARK_WRITE_INTERVAL)
        {
            return;
        }
        let watermark_file = self.watermark_file.clone();
        let written = tokio::task::spawn_blocking(move || watermark_file.write(candidate))
            .await
            .map_err(|error| error.to_string())
            .and_then(|written| written.map_err(|error| error.to_string()));
        match written {
            Ok(()) => {
                self.watermark = Some(candidate);
                self.last_watermark_write = Instant::now();
                metrics::gauge!("zinder_ingest_tree_state_densification_watermark_height")
                    .set(f64::from(candidate.value()));
            }
            Err(reason) => tracing::warn!(
                target: "zinder::ingest",
                event = "tree_state_densification_watermark_write_failed",
                reason = %reason,
                "could not record tree-state densification progress; a restart rescans from the last recorded height"
            ),
        }
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

/// The pass's progress file inside the canonical store directory.
///
/// It records the store's construction manifest digest and the selection
/// the progress was made under, so a rebuilt store or a changed selection
/// starts over instead of trusting it.
#[derive(Clone)]
struct WatermarkFile {
    path: PathBuf,
    construction_manifest_sha256: String,
    selection: TreeStateCheckpointSelection,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct WatermarkRecord {
    version: u32,
    construction_manifest_sha256: String,
    selection: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    commitment_budget: Option<u32>,
    through_height: u32,
}

impl WatermarkFile {
    fn new(
        canonical_primary_path: &std::path::Path,
        construction_manifest_sha256: [u8; 32],
        selection: TreeStateCheckpointSelection,
    ) -> Self {
        Self {
            path: canonical_primary_path.join(TREE_STATE_DENSIFICATION_WATERMARK_FILE),
            construction_manifest_sha256: hex::encode(construction_manifest_sha256),
            selection,
        }
    }

    /// Names the selection as the file records it, or `None` for a
    /// selection the file format does not know, whose progress is then
    /// neither recorded nor trusted.
    fn selection_fields(&self) -> Option<(String, Option<u32>)> {
        match self.selection {
            TreeStateCheckpointSelection::EveryBlock => Some(("every-block".to_owned(), None)),
            TreeStateCheckpointSelection::CommitmentBudget(budget) => {
                Some(("commitment-budget".to_owned(), Some(budget.get())))
            }
            _ => None,
        }
    }

    /// Returns the recorded height when it was made for this store and
    /// selection and the stride that ends at it still satisfies the
    /// selection in the stored rows.
    fn verified(
        &self,
        secondary: &RocksDbCanonicalSecondary,
    ) -> Result<Option<BlockHeight>, CanonicalStoreError> {
        let Ok(encoded) = std::fs::read(&self.path) else {
            return Ok(None);
        };
        let Some((selection, commitment_budget)) = self.selection_fields() else {
            return Ok(None);
        };
        let Some(height) = serde_json::from_slice::<WatermarkRecord>(&encoded)
            .ok()
            .filter(|record| {
                record.version == WATERMARK_VERSION
                    && record.construction_manifest_sha256 == self.construction_manifest_sha256
                    && record.selection == selection
                    && record.commitment_budget == commitment_budget
            })
            .map(|record| BlockHeight::new(record.through_height))
        else {
            self.log_discarded("it was recorded for another store or selection");
            return Ok(None);
        };
        let satisfied = height.value().is_multiple_of(TREE_STATE_CHECKPOINT_STRIDE)
            && height.value() > 0
            && match secondary.tree_state_stride_satisfied(height, self.selection) {
                Ok(satisfied) => satisfied,
                Err(CanonicalStoreError::PublicationRefused { .. }) => false,
                Err(error) => return Err(error),
            };
        if !satisfied {
            self.log_discarded("the stored rows do not satisfy the stride it ends");
            return Ok(None);
        }
        tracing::info!(
            target: "zinder::ingest",
            event = "tree_state_densification_resumed",
            through_height = height.value(),
            "tree-state densification resumes from its recorded progress"
        );
        Ok(Some(height))
    }

    fn log_discarded(&self, reason: &'static str) {
        tracing::warn!(
            target: "zinder::ingest",
            event = "tree_state_densification_watermark_discarded",
            path = %self.path.display(),
            reason,
            "tree-state densification ignores its recorded progress and scans every settled stride"
        );
    }

    /// Replaces the file atomically with `height`.
    fn write(&self, height: BlockHeight) -> std::io::Result<()> {
        let Some((selection, commitment_budget)) = self.selection_fields() else {
            return Ok(());
        };
        write_progress_record(
            &self.path,
            &WatermarkRecord {
                version: WATERMARK_VERSION,
                construction_manifest_sha256: self.construction_manifest_sha256.clone(),
                selection,
                commitment_budget,
                through_height: height.value(),
            },
        )
    }
}

/// Blocks a replay of `stride` appends, which the pass charges against its
/// rate.
const fn replayed_blocks(stride: TreeStateStride) -> u32 {
    stride.end_height().value() - stride.seed_height().value()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failed_scan_is_charged_the_worst_case_reads_of_a_mixed_density_chunk() {
        // Strides of 99 and 100 reads leave the chunk under its budget, so a
        // third stride that reads the most any stride can still runs.
        let chunk_reads = (SCAN_CHUNK_READS - 1).max(99 + 100) + MAX_TREE_STATE_STRIDE_SCAN_READS;
        let charged_reads = FAILED_SCAN_CHARGE_BLOCKS * SCAN_READS_PER_BLOCK;

        assert!(
            charged_reads >= chunk_reads,
            "charged {charged_reads} reads for a chunk that can make {chunk_reads}"
        );
        assert!(charged_reads > SCAN_CHUNK_READS);
    }
}
