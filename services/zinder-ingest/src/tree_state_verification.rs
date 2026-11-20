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
//! A row that fails is store damage. The pass first reports it through
//! [`CanonicalStoreDamageSignal`], which every `CanonicalControl.WriterStatus`
//! answer carries to the store's readers, and parks ingest readiness with
//! `canonical_store_damaged` unless another terminal cause already parks it.
//! Only then does it record the damaged row in its progress file, and it
//! ends. A restart reports the recorded damage from the first writer status
//! it answers and checks the row again as soon as the store is published,
//! without waiting for historical work: damage still there parks readiness
//! again, and a row that now passes clears the report.

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
use zinder_proto::{v1::ops::CanonicalStoreDamagedDetail, wire::canonical_store_damaged_detail};
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
/// height its current cycle continues from and any damage it found.
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

/// Damage the verification pass found in the canonical store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalStoreDamage {
    /// Height of the first stored row that failed verification.
    pub height: BlockHeight,
    /// Pool whose frontier failed, or `None` when the row failed outside any
    /// one pool.
    pub pool: Option<ShieldedProtocol>,
}

impl CanonicalStoreDamage {
    /// Returns the detail `CanonicalControl.WriterStatus` reports for this
    /// damage.
    #[must_use]
    pub fn detail(self) -> CanonicalStoreDamagedDetail {
        canonical_store_damaged_detail(self.height, self.pool)
    }

    /// Returns the readiness cause that reports this damage.
    #[must_use]
    pub const fn readiness_cause(self) -> ReadinessCause {
        ReadinessCause::canonical_store_damaged(self.height, self.pool)
    }
}

impl From<&TreeStateCheckpointMismatch> for CanonicalStoreDamage {
    fn from(mismatch: &TreeStateCheckpointMismatch) -> Self {
        Self {
            height: mismatch.height,
            pool: mismatch.pool,
        }
    }
}

/// Store damage ingest reports to the canonical store's readers.
///
/// The verification pass sets it when it finds a damaged row, and the
/// canonical control plane adds it to every `WriterStatus` answer, so readers
/// refuse traffic on their next writer-status read. Clones share one report.
#[derive(Clone, Debug, Default)]
pub struct CanonicalStoreDamageSignal {
    damage: Arc<Mutex<Option<CanonicalStoreDamage>>>,
}

impl CanonicalStoreDamageSignal {
    /// Returns a signal that reports the damage the verification pass last
    /// recorded in the canonical store directory at `canonical_primary_path`,
    /// or no damage.
    ///
    /// Ingest creates it before its control plane answers, so a restart over
    /// a store it found damaged does not attest that store to its readers
    /// before the pass has checked the damaged row again. The pass discards
    /// the report when the record names another store.
    #[must_use]
    pub fn recorded_in(canonical_primary_path: &Path) -> Self {
        let signal = Self::default();
        let recorded = read_watermark_record(
            &canonical_primary_path.join(TREE_STATE_VERIFICATION_WATERMARK_FILE),
        )
        .and_then(|record| record.damage());
        if let Some(damage) = recorded {
            tracing::warn!(
                target: "zinder::ingest",
                event = "tree_state_verification_recorded_damage_reported",
                height = damage.height.value(),
                pool = damage.pool.map_or("none", ShieldedProtocol::rpc_pool_name),
                "ingest reports the canonical store damage it recorded before it stopped until tree-state verification checks that row again"
            );
            signal.report(damage);
        }
        signal
    }

    /// Returns the damage reported now.
    #[must_use]
    pub fn current(&self) -> Option<CanonicalStoreDamage> {
        *self.damage.lock()
    }

    pub(crate) fn report(&self, damage: CanonicalStoreDamage) {
        *self.damage.lock() = Some(damage);
    }

    fn clear(&self) {
        *self.damage.lock() = None;
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
    /// whose readiness the pass parks on store damage.
    pub gate: HistoricalWorkGate,
    /// Signal through which the pass reports store damage to the canonical
    /// control plane, usually from [`CanonicalStoreDamageSignal::recorded_in`].
    pub store_damage: CanonicalStoreDamageSignal,
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
        log_disabled(context.store_damage.current());
        return;
    }
    // A store recorded as damaged is checked again as soon as it is
    // published: its readers are refused until then.
    let rechecks_recorded_damage = context.store_damage.current().is_some();
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
        (!rechecks_recorded_damage).then_some(&context.gate),
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
        from_height = pass.progress.next_from.value(),
        max_rows_per_second = context.config.max_rows_per_second.get(),
        "tree-state verification started"
    );
    if pass.recorded_damage.is_some() && !pass.recheck_recorded_damage(&context, &cancel).await {
        return;
    }
    loop {
        if wait_until_historical_work_or_cancelled(&context.gate, &cancel).await {
            return;
        }
        let pause = match pass.step(&cancel).await {
            StepOutcome::Progressed => continue,
            StepOutcome::Cancelled => return,
            StepOutcome::CycleCompleted => CYCLE_INTERVAL,
            StepOutcome::Damaged(mismatch) => {
                pass.record_damage(&context, &mismatch).await;
                return;
            }
            StepOutcome::Interrupted(reason) => {
                log_retry(&reason);
                RETRY_INTERVAL
            }
        };
        if sleep_or_cancel(pause, &cancel).await {
            return;
        }
    }
}

fn log_disabled(recorded_damage: Option<CanonicalStoreDamage>) {
    if let Some(damage) = recorded_damage {
        tracing::warn!(
            target: "zinder::ingest",
            event = "tree_state_verification_disabled",
            recorded_damage_height = damage.height.value(),
            "tree-state verification is disabled by configuration, so the canonical store damage it recorded stays reported until the store is rebuilt"
        );
    } else {
        tracing::info!(
            target: "zinder::ingest",
            event = "tree_state_verification_disabled",
            "tree-state verification is disabled by configuration"
        );
    }
}

fn log_retry(reason: &str) {
    metrics::counter!("zinder_ingest_tree_state_verification_retries_total").increment(1);
    tracing::warn!(
        target: "zinder::ingest",
        event = "tree_state_verification_retry",
        reason = %reason,
        retry_interval_seconds = RETRY_INTERVAL.as_secs(),
        "tree-state verification stopped on a store error and will retry"
    );
}

enum StepOutcome {
    Progressed,
    CycleCompleted,
    Cancelled,
    Damaged(TreeStateCheckpointMismatch),
    /// A store error stopped the step after the rows the progress already
    /// counts.
    Interrupted(String),
}

/// How far the current cycle has come.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CycleProgress {
    /// Height the next step verifies from.
    next_from: BlockHeight,
    /// Rows the cycle has verified.
    rows: u64,
}

impl CycleProgress {
    const fn starting_at(next_from: BlockHeight) -> Self {
        Self { next_from, rows: 0 }
    }

    /// Counts `verified_rows` that passed and moves to `next_from`, whether
    /// the step that verified them ended normally or on a store error, so
    /// rows a failed step verified are not verified again.
    fn advance(&mut self, verified_rows: u32, next_from: Option<BlockHeight>) {
        self.rows = self.rows.saturating_add(u64::from(verified_rows));
        if let Some(next_from) = next_from {
            self.next_from = next_from;
        }
        metrics::counter!("zinder_ingest_tree_state_verification_rows_total")
            .increment(u64::from(verified_rows));
    }
}

/// How long the pass waits after reading `rows` so it stays under
/// `max_rows_per_second` on average.
fn pacing_pause(rows: u32, max_rows_per_second: NonZeroU32) -> Duration {
    Duration::from_secs_f64(f64::from(rows) / f64::from(max_rows_per_second.get()))
}

/// A step that stopped on a store error, with the rows it verified first.
struct StepFailure {
    verified_rows: u32,
    next_from: Option<BlockHeight>,
    reason: String,
}

impl StepFailure {
    fn before_any_row(reason: impl std::fmt::Display) -> Self {
        Self {
            verified_rows: 0,
            next_from: None,
            reason: reason.to_string(),
        }
    }
}

/// Resumable state of the pass.
struct VerificationPass {
    secondary: Arc<Mutex<RocksDbCanonicalSecondary>>,
    activations: Arc<NetworkUpgradeActivations>,
    max_rows_per_second: NonZeroU32,
    watermark_file: WatermarkFile,
    progress: CycleProgress,
    /// Damage the watermark file records, which a restart checks again.
    recorded_damage: Option<CanonicalStoreDamage>,
    /// Height the watermark file records.
    recorded: Option<BlockHeight>,
    last_watermark_write: Instant,
    cycle_started_at: Instant,
}

impl VerificationPass {
    /// Starts at the recorded height when the file names this store, and
    /// keeps reporting the damage it records. Damage reported from a record
    /// that names another store is cleared.
    fn resume(
        secondary: RocksDbCanonicalSecondary,
        context: &TreeStateVerificationContext,
    ) -> Self {
        let watermark_file = WatermarkFile::new(
            &context.canonical_primary_path,
            secondary.ready_evidence().construction_manifest_sha256,
        );
        let recorded = watermark_file.recorded();
        let recorded_damage = recorded.and_then(|record| record.damage);
        match recorded_damage {
            Some(damage) => context.store_damage.report(damage),
            None if context.store_damage.current().is_some() => {
                tracing::warn!(
                    target: "zinder::ingest",
                    event = "tree_state_verification_recorded_damage_discarded",
                    "the recorded canonical store damage names another store, so ingest no longer reports it"
                );
                context.store_damage.clear();
            }
            None => {}
        }
        let next_from = recorded.map_or(BlockHeight::new(0), |record| record.next_height);
        record_watermark_metric(next_from);
        Self {
            secondary: Arc::new(Mutex::new(secondary)),
            activations: Arc::clone(&context.network_upgrade_activations),
            max_rows_per_second: context.config.max_rows_per_second,
            watermark_file,
            progress: CycleProgress::starting_at(next_from),
            recorded_damage,
            recorded: recorded.map(|record| record.next_height),
            last_watermark_write: Instant::now(),
            cycle_started_at: Instant::now(),
        }
    }

    /// Checks the recorded damaged row again before the pass waits for
    /// historical work, retrying store errors, and returns whether the pass
    /// continues: `false` once the damage is confirmed or `cancel` fires.
    async fn recheck_recorded_damage(
        &mut self,
        context: &TreeStateVerificationContext,
        cancel: &CancellationToken,
    ) -> bool {
        loop {
            match self.step(cancel).await {
                StepOutcome::Damaged(mismatch) => {
                    self.record_damage(context, &mismatch).await;
                    return false;
                }
                StepOutcome::Cancelled => return false,
                StepOutcome::Progressed | StepOutcome::CycleCompleted => {
                    tracing::warn!(
                        target: "zinder::ingest",
                        event = "tree_state_verification_recorded_damage_cleared",
                        height = self.recorded_damage.map_or(0, |damage| damage.height.value()),
                        "the stored tree-state checkpoint recorded as damaged now passes verification; ingest attests the canonical store again"
                    );
                    context.store_damage.clear();
                    self.recorded_damage = None;
                    self.record_watermark(true).await;
                    return true;
                }
                StepOutcome::Interrupted(reason) => {
                    log_retry(&reason);
                    if sleep_or_cancel(RETRY_INTERVAL, cancel).await {
                        return false;
                    }
                }
            }
        }
    }

    /// Verifies up to [`STEP_ROWS`] rows and paces the pass by the rows read,
    /// including rows a step that fails verified before it failed.
    async fn step(&mut self, cancel: &CancellationToken) -> StepOutcome {
        let verification = match self.verify_from_next().await {
            Ok(verification) => verification,
            Err(failure) => {
                self.progress
                    .advance(failure.verified_rows, failure.next_from);
                if self.pace(failure.verified_rows, cancel).await {
                    return StepOutcome::Cancelled;
                }
                self.record_watermark(false).await;
                return StepOutcome::Interrupted(failure.reason);
            }
        };
        if let Some(mismatch) = verification.mismatch {
            self.progress.advance(verification.verified_rows, None);
            return StepOutcome::Damaged(mismatch);
        }
        self.progress
            .advance(verification.verified_rows, Some(verification.next_from));
        if self.pace(verification.verified_rows, cancel).await {
            return StepOutcome::Cancelled;
        }
        if verification.reached_settled_tip {
            self.complete_cycle().await;
            return StepOutcome::CycleCompleted;
        }
        self.record_watermark(false).await;
        StepOutcome::Progressed
    }

    /// Catches the secondary up and verifies the next rows on a blocking
    /// thread.
    async fn verify_from_next(&self) -> Result<TreeStateCheckpointVerification, StepFailure> {
        let secondary = Arc::clone(&self.secondary);
        let activations = Arc::clone(&self.activations);
        let from = self.progress.next_from;
        tokio::task::spawn_blocking(move || {
            let mut secondary = secondary.lock();
            secondary
                .try_catch_up()
                .map_err(StepFailure::before_any_row)?;
            secondary
                .verify_tree_state_checkpoints(from, STEP_ROWS, &activations)
                .map_err(|error| StepFailure {
                    verified_rows: error.verified_rows,
                    next_from: Some(error.next_from),
                    reason: error.to_string(),
                })
        })
        .await
        .map_err(StepFailure::before_any_row)?
    }

    /// Holds the pass so the rows it verifies stay under the configured rate
    /// on average, returning whether it was cancelled.
    async fn pace(&self, rows: u32, cancel: &CancellationToken) -> bool {
        let pause = pacing_pause(rows, self.max_rows_per_second);
        cancel.is_cancelled() || (!pause.is_zero() && sleep_or_cancel(pause, cancel).await)
    }

    async fn complete_cycle(&mut self) {
        metrics::counter!("zinder_ingest_tree_state_verification_cycles_total").increment(1);
        tracing::info!(
            target: "zinder::ingest",
            event = "tree_state_verification_cycle_completed",
            rows_verified = self.progress.rows,
            duration_seconds = self.cycle_started_at.elapsed().as_secs(),
            "every settled tree-state checkpoint passed verification"
        );
        self.progress = CycleProgress::starting_at(BlockHeight::new(0));
        self.cycle_started_at = Instant::now();
        self.record_watermark(true).await;
    }

    /// Reports `mismatch` to the store's readers and ingest readiness, and
    /// then records it as the pass's progress, so a restart reports and
    /// checks the damaged row again.
    ///
    /// The report comes first: recording waits on two syncs, and readers
    /// must not attest the store meanwhile.
    async fn record_damage(
        &mut self,
        context: &TreeStateVerificationContext,
        mismatch: &TreeStateCheckpointMismatch,
    ) {
        let damage = CanonicalStoreDamage::from(mismatch);
        report_damage(context, damage, &mismatch.reason);
        self.recorded_damage = Some(damage);
        self.progress.next_from = mismatch.height;
        self.record_watermark(true).await;
    }

    /// Records where the cycle continues from and any damage found, at most
    /// every [`WATERMARK_WRITE_INTERVAL`] unless `now` is set.
    async fn record_watermark(&mut self, now: bool) {
        let next_from = self.progress.next_from;
        record_watermark_metric(next_from);
        if !now
            && (self.recorded == Some(next_from)
                || self.last_watermark_write.elapsed() < WATERMARK_WRITE_INTERVAL)
        {
            return;
        }
        let watermark_file = self.watermark_file.clone();
        let damage = self.recorded_damage;
        let written = tokio::task::spawn_blocking(move || watermark_file.write(next_from, damage))
            .await
            .map_err(|error| error.to_string())
            .and_then(|written| written.map_err(|error| error.to_string()));
        match written {
            Ok(()) => {
                self.recorded = Some(next_from);
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

/// Reports `damage` through the store-damage signal and parks ingest
/// readiness with it, unless another terminal cause, such as a writer parked
/// on a reorg deeper than its window, already parks it.
fn report_damage(
    context: &TreeStateVerificationContext,
    damage: CanonicalStoreDamage,
    reason: &str,
) {
    context.store_damage.report(damage);
    let readiness = context.gate.readiness();
    let admission = readiness.admission_state();
    let parked = readiness.park_admission_unless_parked(ReadinessState {
        current_height: admission.current_height,
        target_height: admission.target_height,
        ..ReadinessState::not_ready(damage.readiness_cause())
    });
    metrics::counter!("zinder_ingest_tree_state_verification_mismatches_total").increment(1);
    tracing::error!(
        target: "zinder::ingest",
        event = "tree_state_verification_mismatch",
        height = damage.height.value(),
        pool = damage.pool.map_or("none", ShieldedProtocol::rpc_pool_name),
        reason = %reason,
        readiness_parked = parked,
        "a stored tree-state checkpoint failed verification; ingest reports canonical_store_damaged until the canonical store is rebuilt"
    );
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
    /// The damaged row the pass found, absent while it has found none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    damaged: Option<RecordedDamage>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct RecordedDamage {
    height: u32,
    /// Node RPC name of the pool whose frontier failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pool: Option<String>,
}

impl WatermarkRecord {
    fn damage(&self) -> Option<CanonicalStoreDamage> {
        self.damaged.as_ref().map(|damaged| CanonicalStoreDamage {
            height: BlockHeight::new(damaged.height),
            pool: damaged
                .pool
                .as_deref()
                .and_then(ShieldedProtocol::from_rpc_pool_name),
        })
    }
}

/// What a watermark file written for this store records.
#[derive(Clone, Copy)]
struct RecordedProgress {
    next_height: BlockHeight,
    damage: Option<CanonicalStoreDamage>,
}

/// Reads the record at `path` when it exists and has this release's version,
/// for whichever store wrote it.
fn read_watermark_record(path: &Path) -> Option<WatermarkRecord> {
    let encoded = std::fs::read(path).ok()?;
    serde_json::from_slice::<WatermarkRecord>(&encoded)
        .ok()
        .filter(|record| record.version == WATERMARK_VERSION)
}

impl WatermarkFile {
    fn new(canonical_primary_path: &Path, construction_manifest_sha256: [u8; 32]) -> Self {
        Self {
            path: canonical_primary_path.join(TREE_STATE_VERIFICATION_WATERMARK_FILE),
            construction_manifest_sha256: hex::encode(construction_manifest_sha256),
        }
    }

    /// Returns what the file records when it exists and was written for this
    /// store.
    fn recorded(&self) -> Option<RecordedProgress> {
        let recorded = read_watermark_record(&self.path)
            .filter(|record| {
                record.construction_manifest_sha256 == self.construction_manifest_sha256
            })
            .map(|record| RecordedProgress {
                next_height: BlockHeight::new(record.next_height),
                damage: record.damage(),
            });
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

    fn write(
        &self,
        next_height: BlockHeight,
        damage: Option<CanonicalStoreDamage>,
    ) -> std::io::Result<()> {
        write_progress_record(
            &self.path,
            &WatermarkRecord {
                version: WATERMARK_VERSION,
                construction_manifest_sha256: self.construction_manifest_sha256.clone(),
                next_height: next_height.value(),
                damaged: damage.map(|damage| RecordedDamage {
                    height: damage.height.value(),
                    pool: damage.pool.map(|pool| pool.rpc_pool_name().to_owned()),
                }),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rows a step verified before a store error stopped it are counted and
    /// skipped, and the pass paces them as it paces a step that succeeded.
    #[test]
    fn a_failed_step_keeps_its_verified_rows_and_charges_them_to_the_rate() {
        let mut progress = CycleProgress::starting_at(BlockHeight::new(0));

        progress.advance(60, Some(BlockHeight::new(60)));

        assert_eq!(
            progress,
            CycleProgress {
                next_from: BlockHeight::new(60),
                rows: 60,
            }
        );
        let rate = NonZeroU32::new(25).unwrap_or(NonZeroU32::MIN);
        assert_eq!(pacing_pause(60, rate), Duration::from_millis(2_400));

        // A step that failed before any row keeps the cycle where it was.
        progress.advance(0, None);
        assert_eq!(progress.next_from, BlockHeight::new(60));
    }
}
