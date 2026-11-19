//! Background verification of stored tree-state checkpoints.
//!
//! A stored tree-state read serves a checkpoint row after checking only its
//! framing, so this pass reads every settled row again and checks it as a
//! writer does: it re-derives each frontier's root, compares it with the
//! stored root, and requires a frontier for exactly the pools active at the
//! row's height. A canonical secondary reads the rows on a blocking thread,
//! and the pass holds itself to a configured average number of rows per
//! second. It runs only while historical work may run.
//!
//! The pass works in cycles. Each cycle verifies every row from the history
//! predecessor through the settled tip, including rows that densification or
//! live following added since the previous cycle, so a damaged row is found
//! within one cycle of the time it became settled or damaged. The pass records
//! the height its current cycle continues from in
//! [`TREE_STATE_VERIFICATION_WATERMARK_FILE`] in the canonical store
//! directory, bound to the store's construction manifest, so a restart
//! resumes the cycle there.
//!
//! A row that fails is store damage. The pass logs its height and pool,
//! withdraws ingest readiness with `storage_unavailable` until the process
//! stops, and ends. It records the damaged row's height as its progress, so
//! the next start finds the row again at once.

use std::{
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use zinder_core::{BlockHeight, NetworkUpgradeActivations, ShieldedProtocol};
use zinder_runtime::{ReadinessCause, ReadinessState};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalSecondary,
    RocksDbResourceBudget, TREE_STATE_CHECKPOINT_STRIDE, TreeStateCheckpointMismatch,
    TreeStateCheckpointVerification,
};

use crate::{
    background_pass::{BackgroundSecondary, write_progress_record},
    runtime_config::{
        HistoricalWorkGate, sleep_or_cancel, wait_until_historical_work_or_cancelled,
    },
};

/// Default average ceiling on stored checkpoint rows the pass verifies per
/// second.
///
/// Verifying a row re-derives one root per pool it holds, about 4.3 ms of CPU
/// for a row with three pools, so the default holds the pass near a tenth of
/// one CPU.
pub const DEFAULT_TREE_STATE_VERIFICATION_MAX_ROWS_PER_SECOND: u32 = 25;

/// File, inside the canonical store directory, in which the pass records the
/// height its current cycle continues from.
pub const TREE_STATE_VERIFICATION_WATERMARK_FILE: &str = "tree-state-verification-watermark.json";
const WATERMARK_VERSION: u32 = 1;

/// Rows one verification step reads before the pass paces it: one cadence
/// stride of rows at every-block density.
const STEP_ROWS: NonZeroU32 = match NonZeroU32::new(TREE_STATE_CHECKPOINT_STRIDE) {
    Some(rows) => rows,
    None => NonZeroU32::MIN,
};
/// How long the pass waits after a cycle before it starts the next.
const CYCLE_INTERVAL: Duration = Duration::from_mins(5);
/// How long the pass waits before retrying after a store error.
const RETRY_INTERVAL: Duration = Duration::from_secs(30);
/// Least time between two progress records within a cycle.
const WATERMARK_WRITE_INTERVAL: Duration = Duration::from_mins(1);

/// Resolved `[ingest.tree_state_verification]` settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TreeStateVerificationConfig {
    /// Whether the pass runs.
    pub enabled: bool,
    /// Ceiling on stored checkpoint rows verified per second, on average.
    pub max_rows_per_second: NonZeroU32,
}

impl Default for TreeStateVerificationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_rows_per_second: NonZeroU32::new(
                DEFAULT_TREE_STATE_VERIFICATION_MAX_ROWS_PER_SECOND,
            )
            .unwrap_or(NonZeroU32::MIN),
        }
    }
}

/// Everything the pass needs to open its canonical secondary and report
/// store damage.
pub struct TreeStateVerificationContext {
    /// Path of the canonical primary the writer publishes.
    pub canonical_primary_path: PathBuf,
    /// Path of this pass's own canonical secondary.
    pub secondary_path: PathBuf,
    /// Node-observed activations that decide which pools each row carries.
    pub network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    /// Workload the canonical store was built for.
    pub workload: CanonicalStoreWorkload,
    /// Raw-blob retention the canonical store was built with.
    pub raw_blob_retention: RawBlobRetention,
    /// Reorg policy the canonical store was built with.
    pub reorg_policy: CanonicalReorgPolicy,
    /// `RocksDB` budget of the secondary.
    pub resource_budget: RocksDbResourceBudget,
    /// Gate that admits historical work while the writer follows the tip,
    /// whose readiness the pass withdraws on store damage.
    pub gate: HistoricalWorkGate,
    /// Resolved pass settings.
    pub config: TreeStateVerificationConfig,
}

/// Spawns the verification pass, which returns once `cancel` fires, once it
/// finds store damage, or at once when the pass is disabled.
#[must_use]
pub fn spawn_tree_state_verification_task(
    context: TreeStateVerificationContext,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(run_tree_state_verification(context, cancel))
}

async fn run_tree_state_verification(
    context: TreeStateVerificationContext,
    cancel: CancellationToken,
) {
    if !context.config.enabled {
        tracing::info!(
            target: "zinder::ingest",
            event = "tree_state_verification_disabled",
            "tree-state verification is disabled by configuration"
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
        &context.gate,
        &cancel,
        "tree_state_verification_secondary_unavailable",
    )
    .await
    else {
        return;
    };
    let mut pass = VerificationPass::resume(secondary, &context);
    tracing::info!(
        target: "zinder::ingest",
        event = "tree_state_verification_started",
        from_height = pass.next_from.value(),
        max_rows_per_second = context.config.max_rows_per_second.get(),
        "tree-state verification started"
    );
    loop {
        if wait_until_historical_work_or_cancelled(&context.gate, &cancel).await {
            return;
        }
        let pause = match pass.step(&cancel).await {
            Ok(StepOutcome::Progressed) => continue,
            Ok(StepOutcome::Cancelled) => return,
            Ok(StepOutcome::CycleCompleted) => CYCLE_INTERVAL,
            Ok(StepOutcome::Damaged(mismatch)) => {
                withdraw_readiness(&context.gate, &mismatch);
                return;
            }
            Err(reason) => {
                metrics::counter!("zinder_ingest_tree_state_verification_retries_total")
                    .increment(1);
                tracing::warn!(
                    target: "zinder::ingest",
                    event = "tree_state_verification_retry",
                    reason = %reason,
                    retry_interval_seconds = RETRY_INTERVAL.as_secs(),
                    "tree-state verification stopped on a store error and will retry"
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
    CycleCompleted,
    Cancelled,
    Damaged(TreeStateCheckpointMismatch),
}

/// Resumable state of the pass.
struct VerificationPass {
    secondary: Arc<Mutex<RocksDbCanonicalSecondary>>,
    activations: Arc<NetworkUpgradeActivations>,
    max_rows_per_second: NonZeroU32,
    watermark_file: WatermarkFile,
    /// Height the next step verifies from.
    next_from: BlockHeight,
    /// Height the watermark file records.
    recorded: Option<BlockHeight>,
    last_watermark_write: Instant,
    cycle_rows: u64,
    cycle_started_at: Instant,
}

impl VerificationPass {
    /// Starts at the recorded height when the file names this store.
    fn resume(
        secondary: RocksDbCanonicalSecondary,
        context: &TreeStateVerificationContext,
    ) -> Self {
        let watermark_file = WatermarkFile::new(
            &context.canonical_primary_path,
            secondary.ready_evidence().construction_manifest_sha256,
        );
        let recorded = watermark_file.recorded();
        let next_from = recorded.unwrap_or(BlockHeight::new(0));
        record_watermark_metric(next_from);
        Self {
            secondary: Arc::new(Mutex::new(secondary)),
            activations: Arc::clone(&context.network_upgrade_activations),
            max_rows_per_second: context.config.max_rows_per_second,
            watermark_file,
            next_from,
            recorded,
            last_watermark_write: Instant::now(),
            cycle_rows: 0,
            cycle_started_at: Instant::now(),
        }
    }

    /// Verifies up to [`STEP_ROWS`] rows and paces the pass by the rows read.
    async fn step(&mut self, cancel: &CancellationToken) -> Result<StepOutcome, String> {
        let verification = self.verify_from_next().await?;
        metrics::counter!("zinder_ingest_tree_state_verification_rows_total")
            .increment(u64::from(verification.verified_rows));
        self.cycle_rows = self
            .cycle_rows
            .saturating_add(u64::from(verification.verified_rows));
        if let Some(mismatch) = verification.mismatch {
            self.next_from = mismatch.height;
            self.record_watermark(true).await;
            return Ok(StepOutcome::Damaged(mismatch));
        }
        self.next_from = verification.next_from;
        if self.pace(verification.verified_rows, cancel).await {
            return Ok(StepOutcome::Cancelled);
        }
        if verification.reached_settled_tip {
            self.complete_cycle().await;
            return Ok(StepOutcome::CycleCompleted);
        }
        self.record_watermark(false).await;
        Ok(StepOutcome::Progressed)
    }

    /// Catches the secondary up and verifies the next rows on a blocking
    /// thread.
    async fn verify_from_next(&self) -> Result<TreeStateCheckpointVerification, String> {
        let secondary = Arc::clone(&self.secondary);
        let activations = Arc::clone(&self.activations);
        let from = self.next_from;
        tokio::task::spawn_blocking(move || {
            let mut secondary = secondary.lock();
            secondary.try_catch_up()?;
            secondary.verify_tree_state_checkpoints(from, STEP_ROWS, &activations)
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())
    }

    /// Holds the pass so the rows it verifies stay under the configured rate
    /// on average, returning whether it was cancelled.
    async fn pace(&self, rows: u32, cancel: &CancellationToken) -> bool {
        let pause =
            Duration::from_secs_f64(f64::from(rows) / f64::from(self.max_rows_per_second.get()));
        cancel.is_cancelled() || (!pause.is_zero() && sleep_or_cancel(pause, cancel).await)
    }

    async fn complete_cycle(&mut self) {
        metrics::counter!("zinder_ingest_tree_state_verification_cycles_total").increment(1);
        tracing::info!(
            target: "zinder::ingest",
            event = "tree_state_verification_cycle_completed",
            rows_verified = self.cycle_rows,
            duration_seconds = self.cycle_started_at.elapsed().as_secs(),
            "every settled tree-state checkpoint passed verification"
        );
        self.next_from = BlockHeight::new(0);
        self.cycle_rows = 0;
        self.cycle_started_at = Instant::now();
        self.record_watermark(true).await;
    }

    /// Records the height the cycle continues from, at most every
    /// [`WATERMARK_WRITE_INTERVAL`] unless `now` is set.
    async fn record_watermark(&mut self, now: bool) {
        record_watermark_metric(self.next_from);
        if self.recorded == Some(self.next_from)
            || (!now && self.last_watermark_write.elapsed() < WATERMARK_WRITE_INTERVAL)
        {
            return;
        }
        let watermark_file = self.watermark_file.clone();
        let next_from = self.next_from;
        let written = tokio::task::spawn_blocking(move || watermark_file.write(next_from))
            .await
            .map_err(|error| error.to_string())
            .and_then(|written| written.map_err(|error| error.to_string()));
        match written {
            Ok(()) => {
                self.recorded = Some(self.next_from);
                self.last_watermark_write = Instant::now();
            }
            Err(error) => tracing::warn!(
                target: "zinder::ingest",
                event = "tree_state_verification_watermark_write_failed",
                reason = %error,
                "could not record tree-state verification progress; a restart resumes from the last recorded height"
            ),
        }
    }
}

/// Withdraws ingest readiness for the rest of the process after the pass
/// found damage.
fn withdraw_readiness(gate: &HistoricalWorkGate, mismatch: &TreeStateCheckpointMismatch) {
    metrics::counter!("zinder_ingest_tree_state_verification_mismatches_total").increment(1);
    tracing::error!(
        target: "zinder::ingest",
        event = "tree_state_verification_mismatch",
        height = mismatch.height.value(),
        pool = mismatch.pool.map_or("none", ShieldedProtocol::rpc_pool_name),
        reason = %mismatch.reason,
        "a stored tree-state checkpoint failed verification; ingest withdraws readiness until the canonical store is rebuilt"
    );
    let readiness = gate.readiness();
    let admission = readiness.admission_state();
    readiness.park_admission(ReadinessState {
        current_height: admission.current_height,
        target_height: admission.target_height,
        ..ReadinessState::not_ready(ReadinessCause::StorageUnavailable)
    });
}

fn record_watermark_metric(height: BlockHeight) {
    metrics::gauge!("zinder_ingest_tree_state_verification_watermark_height")
        .set(f64::from(height.value()));
}

/// The pass's progress file inside the canonical store directory.
///
/// It records the store's construction manifest digest, so a rebuilt store
/// starts a new cycle instead of trusting it.
#[derive(Clone)]
struct WatermarkFile {
    path: PathBuf,
    construction_manifest_sha256: String,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct WatermarkRecord {
    version: u32,
    construction_manifest_sha256: String,
    next_height: u32,
}

impl WatermarkFile {
    fn new(canonical_primary_path: &Path, construction_manifest_sha256: [u8; 32]) -> Self {
        Self {
            path: canonical_primary_path.join(TREE_STATE_VERIFICATION_WATERMARK_FILE),
            construction_manifest_sha256: hex::encode(construction_manifest_sha256),
        }
    }

    /// Returns the recorded height when the file exists and was written for
    /// this store.
    fn recorded(&self) -> Option<BlockHeight> {
        let encoded = std::fs::read(&self.path).ok()?;
        let recorded = serde_json::from_slice::<WatermarkRecord>(&encoded)
            .ok()
            .filter(|record| {
                record.version == WATERMARK_VERSION
                    && record.construction_manifest_sha256 == self.construction_manifest_sha256
            })
            .map(|record| BlockHeight::new(record.next_height));
        if recorded.is_none() {
            tracing::warn!(
                target: "zinder::ingest",
                event = "tree_state_verification_watermark_discarded",
                path = %self.path.display(),
                "tree-state verification ignores progress recorded for another store and starts a new cycle"
            );
        }
        recorded
    }

    fn write(&self, next_height: BlockHeight) -> std::io::Result<()> {
        write_progress_record(
            &self.path,
            &WatermarkRecord {
                version: WATERMARK_VERSION,
                construction_manifest_sha256: self.construction_manifest_sha256.clone(),
                next_height: next_height.value(),
            },
        )
    }
}
