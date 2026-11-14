//! Arm 1, Arm 3, and the density arm: one synced commit per block, H0+1..T.

use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufWriter, Write},
    num::NonZeroU64,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use clap::Args;
use rust_rocksdb::WriteBatch;
use serde::Serialize;
use tokio::sync::mpsc;
use zinder_bench::{BenchError, fixture::FixtureNodeSource};
use zinder_core::{BlockHeight, NetworkUpgradeActivations, UnixTimestampMillis};
use zinder_ingest::spike_next_append_settled_tip;
use zinder_store::{
    CanonicalEventFence, CanonicalLiveAppend, RawBlobRetention, RocksDbCanonicalSecondary,
    RocksDbCanonicalStore,
};
use zinder_wallet_projection::{
    WalletCanonicalSourceIdentity, WalletProjectionBuildState, WalletProjectionDigestBuilder,
    WalletProjectionReadyEvidence,
};
use zinder_wallet_rocksdb::{
    MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES, RocksDbWalletStore,
    SPIKE_WALLET_CONTROL_COLUMN_FAMILY, SpikeMirrorPlanOverlay, spike_decode_wallet_control,
};

use super::{
    CommitMode, CommonArgs, NETWORK_LABEL_TESTNET, hex32,
    layout::{
        BudgetView, CopyStats, Layout, absolute, copy_closed_store, directory_bytes,
        ensure_column_families, open_canonical, open_secondary, wallet_family_budget,
        writer_budget,
    },
    metrics::{
        LatencySummary, PerfProbe, PropertySampler, PropertySamples, StallRecorder, StallSummary,
        ValueSummary, current_rss_bytes, install_stall_listener, peak_rss_bytes, summarize_seconds,
        summarize_values, ticker_delta, ticker_snapshot,
    },
    mirror::{
        MirrorCounts, MirrorFamilies, MirrorFamilyNames, MirrorWork, PressureStats,
        PressureWriter, apply_lanes,
    },
    pipeline::{DerivedBlock, spawn_pipeline},
    secondary_follow::SecondaryChild,
    spike_error,
};

const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

/// Replay arm inputs.
#[derive(Args)]
pub(crate) struct ReplayArgs {
    #[command(flatten)]
    pub(crate) common: CommonArgs,
    /// Golden store at H0 (from `prepare`); copied, never opened.
    #[arg(long)]
    golden: PathBuf,
    /// Fresh path for this arm's working copy (kept for `verify`).
    #[arg(long)]
    work: PathBuf,
    /// Canonical-only baseline or combined canonical plus wallet batch.
    #[arg(long, value_enum)]
    mode: CommitMode,
    /// Last height to replay (default: the fixture's last height).
    #[arg(long)]
    through_height: Option<u32>,
    /// Per-block row volume multiplier for the synthetic density arm (1 = real rows only).
    #[arg(long, default_value_t = 1)]
    density_multiplier: u32,
    /// Background mirror lanes written per committed block (Arm 3; 0 = off).
    #[arg(long, default_value_t = 0)]
    pressure_lanes: u32,
    /// Which real rows the pressure lanes mirror.
    #[arg(long, value_enum, default_value_t = MirrorFamilies::All)]
    pressure_families: MirrorFamilies,
    /// Committed blocks the pressure writer may queue before the committer waits.
    #[arg(long, default_value_t = 256)]
    pressure_queue_blocks: usize,
    /// Directory for a canonical secondary opened on the work store.
    #[arg(long)]
    secondary_dir: Option<PathBuf>,
    /// Secondary catch-up poll interval during the replay (0 = only after it).
    #[arg(long, default_value_t = 1000)]
    secondary_poll_ms: u64,
    /// Run the secondary inside the writer process instead of a child process.
    ///
    /// The default child process keeps the reader's memory out of the
    /// writer's peak RSS, as separate reader runtimes do.
    #[arg(long, default_value_t = false)]
    secondary_in_process: bool,
    /// Wallet transition logical-byte ceiling passed to the planner.
    #[arg(long, default_value_t = MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES)]
    wallet_max_transition_bytes: u64,
    /// Blocks derived ahead of the committer (production follow holds one).
    #[arg(long, default_value_t = 2)]
    pipeline_depth: usize,
    /// Log progress every this many blocks.
    #[arg(long, default_value_t = 1000)]
    progress_every: u32,
    /// Per-block JSON-lines samples file (refuses to overwrite).
    #[arg(long)]
    samples: Option<PathBuf>,
    /// Property sampling interval in milliseconds.
    #[arg(long, default_value_t = 250)]
    property_sample_ms: u64,
    /// Recompute the wallet digest from rows after the replay (combined only).
    #[arg(long, default_value_t = false)]
    cold_readback: bool,
    /// Disable the per-commit `RocksDB` perf context.
    #[arg(long, default_value_t = false)]
    no_perf_context: bool,
}

/// Identity of one block.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct BlockRef {
    pub(crate) height: u32,
    pub(crate) hash_hex: String,
}

/// One canonical fence.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(crate) struct FenceView {
    pub(crate) chain_epoch: u64,
    pub(crate) event_sequence: u64,
    pub(crate) tip_height: u32,
    pub(crate) tip_hash_hex: String,
    pub(crate) sequence_block_count: u64,
    pub(crate) sequence_digest_hex: String,
}

impl From<CanonicalEventFence> for FenceView {
    fn from(fence: CanonicalEventFence) -> Self {
        Self {
            chain_epoch: fence.chain_epoch_id().value(),
            event_sequence: fence.chain_event_sequence(),
            tip_height: fence.visible_tip().height.value(),
            tip_hash_hex: hex32(fence.visible_tip().hash.as_bytes()),
            sequence_block_count: fence.sequence_digest().block_count(),
            sequence_digest_hex: hex32(fence.sequence_digest().as_bytes()),
        }
    }
}

/// One wallet READY evidence.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct WalletView {
    pub(crate) projection_digest_hex: String,
    pub(crate) source_epoch: u64,
    pub(crate) source_event_sequence: u64,
    pub(crate) source_tip_height: u32,
    pub(crate) settled_tip_height: u32,
    pub(crate) source_sequence_digest_hex: String,
    pub(crate) unspent_outputs: u64,
    pub(crate) spent_outputs: u64,
    pub(crate) address_transactions: u64,
    pub(crate) address_balances: u64,
    pub(crate) reorg_undo_rows: u64,
    pub(crate) utxo_count: u64,
    pub(crate) utxo_total_value_zat: u64,
}

impl From<&WalletProjectionReadyEvidence> for WalletView {
    fn from(evidence: &WalletProjectionReadyEvidence) -> Self {
        Self {
            projection_digest_hex: hex::encode(evidence.projection_digest.as_bytes()),
            source_epoch: evidence.source_position.chain_epoch_id.value(),
            source_event_sequence: evidence.source_position.event_sequence,
            source_tip_height: evidence.source_position.tip.height.value(),
            settled_tip_height: evidence.settled_tip.height.value(),
            source_sequence_digest_hex: hex32(evidence.source_sequence_digest.as_bytes()),
            unspent_outputs: evidence.row_counts.transparent_unspent_output_count,
            spent_outputs: evidence.row_counts.transparent_spent_output_count,
            address_transactions: evidence.row_counts.transparent_address_transaction_count,
            address_balances: evidence.row_counts.transparent_address_balance_count,
            reorg_undo_rows: evidence.row_counts.reorg_undo_count,
            utxo_count: evidence.utxo_summary.utxo_count,
            utxo_total_value_zat: evidence.utxo_summary.total_value_zat,
        }
    }
}

/// Pass or fail against one spec threshold.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct ThresholdCheck {
    pub(crate) check: &'static str,
    pub(crate) threshold: String,
    pub(crate) observed: String,
    pub(crate) passed: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct LatencyBlock {
    /// Canonical prepare (anchor, settled tip, validation, row build) plus
    /// wallet plan, mirror lanes, synced write, readback, and wallet adopt.
    pub(crate) commit_total: LatencySummary,
    pub(crate) canonical_prepare: LatencySummary,
    pub(crate) wallet_plan: LatencySummary,
    pub(crate) mirror_lanes: LatencySummary,
    pub(crate) synced_write: LatencySummary,
    pub(crate) canonical_readback: LatencySummary,
    pub(crate) wallet_adopt: LatencySummary,
    pub(crate) fixture_fetch: LatencySummary,
    pub(crate) block_derive: LatencySummary,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct BatchBlock {
    pub(crate) total_bytes: ValueSummary,
    pub(crate) operations: ValueSummary,
    pub(crate) canonical_bytes: ValueSummary,
    pub(crate) wallet_bytes: ValueSummary,
    pub(crate) mirror_bytes: ValueSummary,
    pub(crate) wallet_plan_logical_bytes: ValueSummary,
    /// Mirror wallet rows charged under the planner's rules (density arm).
    pub(crate) mirror_wallet_plan_logical_bytes: ValueSummary,
    pub(crate) wallet_point_reads: ValueSummary,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct WorkloadBlock {
    pub(crate) transactions: ValueSummary,
    pub(crate) transparent_inputs: ValueSummary,
    pub(crate) transparent_outputs: ValueSummary,
    pub(crate) source_block_bytes: ValueSummary,
    pub(crate) chain_time_seconds: i64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct PerfBlock {
    pub(crate) enabled: bool,
    pub(crate) commits_with_write_delay: u64,
    pub(crate) write_delay_total_seconds: f64,
    pub(crate) write_delay_max_ms: f64,
    pub(crate) write_wal_total_seconds: f64,
    pub(crate) write_memtable_total_seconds: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct RssBlock {
    pub(crate) start_bytes: Option<u64>,
    pub(crate) end_bytes: Option<u64>,
    pub(crate) max_sampled_bytes: u64,
    /// VmHWM after the replay and the final secondary catch-up, before any cold readback.
    pub(crate) peak_vmhwm_bytes: Option<u64>,
    /// VmHWM after the optional cold readback.
    pub(crate) peak_vmhwm_after_cold_readback_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct WriteAmplification {
    pub(crate) user_bytes_written: u64,
    pub(crate) wal_bytes: u64,
    pub(crate) flush_write_bytes: u64,
    pub(crate) compaction_write_bytes: u64,
    /// (flush + compaction writes) / user bytes written.
    pub(crate) amplification: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct SecondaryReport {
    pub(crate) path: String,
    pub(crate) open_seconds: f64,
    pub(crate) poll_ms: u64,
    pub(crate) periodic_catch_ups: LatencySummary,
    pub(crate) max_observed_lag_blocks: u32,
    pub(crate) final_catch_up_seconds: f64,
    pub(crate) final_fence: Option<FenceView>,
    pub(crate) final_fence_matches_primary: bool,
    /// Combined only: the wallet control read through the secondary names
    /// exactly the canonical fence the secondary admitted.
    pub(crate) wallet_control_matches_fence: Option<bool>,
    /// The secondary ran in a separate process.
    pub(crate) separate_process: bool,
    pub(crate) follower_peak_vmhwm_bytes: Option<u64>,
    pub(crate) follower_report: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct StallRates {
    /// `rocksdb.stall.micros` (every writer, delay plus stop), seconds.
    pub(crate) ticker_stall_seconds: f64,
    pub(crate) ticker_stall_seconds_per_chain_hour: f64,
    pub(crate) ticker_stall_seconds_per_wall_hour: f64,
    /// Commit-thread write delay from the perf context, seconds.
    pub(crate) commit_write_delay_seconds_per_chain_hour: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct RawFiles {
    pub(crate) samples: Option<String>,
    pub(crate) stall_transitions: Option<String>,
    pub(crate) rocksdb_stats: Option<String>,
}

/// Report of one replay arm.
#[derive(Serialize)]
pub(crate) struct ReplayReport {
    pub(crate) spike: &'static str,
    pub(crate) action: &'static str,
    pub(crate) network_label: &'static str,
    pub(crate) label: Option<String>,
    pub(crate) mode: CommitMode,
    pub(crate) layout: Layout,
    pub(crate) writer_budget: BudgetView,
    pub(crate) wallet_family_budget: Option<BudgetView>,
    pub(crate) golden: String,
    pub(crate) work: String,
    pub(crate) copy: CopyStats,
    pub(crate) h0: BlockRef,
    pub(crate) through: BlockRef,
    pub(crate) blocks: u64,
    pub(crate) density_multiplier: u32,
    pub(crate) pressure: Option<PressureStats>,
    /// Pressure writer bytes divided by the committed batches' bytes.
    pub(crate) pressure_to_commit_bytes_ratio: Option<f64>,
    pub(crate) density_mirror_counts: MirrorCounts,
    /// (canonical + wallet + mirror) batch bytes divided by (canonical + wallet).
    pub(crate) density_effective_byte_multiplier: f64,
    pub(crate) open_seconds: f64,
    pub(crate) wall_seconds: f64,
    pub(crate) blocks_per_second: f64,
    pub(crate) commit_path_seconds: f64,
    pub(crate) commit_path_blocks_per_second: f64,
    pub(crate) pipeline_wait_seconds: f64,
    pub(crate) latency: LatencyBlock,
    pub(crate) batch: BatchBlock,
    pub(crate) workload: WorkloadBlock,
    pub(crate) perf: PerfBlock,
    pub(crate) rss: RssBlock,
    pub(crate) tickers: BTreeMap<&'static str, u64>,
    pub(crate) write_amplification: WriteAmplification,
    pub(crate) stall_rates: StallRates,
    pub(crate) stalls: StallSummary,
    pub(crate) properties: PropertySamples,
    pub(crate) secondary: Option<SecondaryReport>,
    pub(crate) final_fence: FenceView,
    pub(crate) wallet_final: Option<WalletView>,
    pub(crate) cold_readback_seconds: Option<f64>,
    pub(crate) work_store_bytes: u64,
    pub(crate) thresholds: Vec<ThresholdCheck>,
    pub(crate) raw_files: RawFiles,
}

#[derive(Serialize)]
struct BlockSample {
    height: u32,
    transactions: u64,
    transparent_inputs: u64,
    transparent_outputs: u64,
    source_bytes: u64,
    fetch_ms: f64,
    derive_ms: f64,
    pipeline_wait_ms: f64,
    canonical_prepare_ms: f64,
    wallet_plan_ms: f64,
    mirror_ms: f64,
    write_ms: f64,
    readback_ms: f64,
    wallet_adopt_ms: f64,
    commit_ms: f64,
    batch_bytes: u64,
    batch_operations: u64,
    canonical_bytes: u64,
    wallet_bytes: u64,
    mirror_bytes: u64,
    wallet_plan_logical_bytes: u64,
    mirror_wallet_plan_logical_bytes: u64,
    wallet_point_reads: u64,
    perf_write_delay_us: f64,
    perf_write_wal_us: f64,
    rss_bytes: Option<u64>,
}

#[derive(Default)]
struct Series {
    commit_total: Vec<f64>,
    canonical_prepare: Vec<f64>,
    wallet_plan: Vec<f64>,
    mirror_lanes: Vec<f64>,
    synced_write: Vec<f64>,
    canonical_readback: Vec<f64>,
    wallet_adopt: Vec<f64>,
    fixture_fetch: Vec<f64>,
    block_derive: Vec<f64>,
    batch_bytes: Vec<f64>,
    batch_ops: Vec<f64>,
    canonical_bytes: Vec<f64>,
    wallet_bytes: Vec<f64>,
    mirror_bytes: Vec<f64>,
    wallet_plan_logical: Vec<f64>,
    mirror_wallet_plan_logical: Vec<f64>,
    wallet_point_reads: Vec<f64>,
    transactions: Vec<f64>,
    transparent_inputs: Vec<f64>,
    transparent_outputs: Vec<f64>,
    source_bytes: Vec<f64>,
}

/// Committer state moved into the blocking commit thread.
pub(crate) struct Committer {
    pub(crate) store: Option<RocksDbCanonicalStore>,
    pub(crate) wallet: Option<RocksDbWalletStore>,
    pub(crate) activations: Arc<NetworkUpgradeActivations>,
    pub(crate) wallet_max: NonZeroU64,
    pub(crate) density_lanes: u8,
    pub(crate) mirror_names: MirrorFamilyNames,
    pub(crate) pressure: Option<PressureWriter>,
    pub(crate) pressure_families: MirrorFamilies,
    pub(crate) density_accumulator: WalletProjectionDigestBuilder,
    pub(crate) density_counts: MirrorCounts,
    pub(crate) perf_enabled: bool,
}

/// Measured phases of one combined or canonical-only commit.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CommitPhases {
    pub(crate) canonical_prepare: f64,
    pub(crate) wallet_plan: f64,
    pub(crate) mirror: f64,
    pub(crate) write: f64,
    pub(crate) readback: f64,
    pub(crate) wallet_adopt: f64,
    pub(crate) total: f64,
    pub(crate) batch_bytes: u64,
    pub(crate) batch_ops: u64,
    pub(crate) canonical_bytes: u64,
    pub(crate) wallet_bytes: u64,
    pub(crate) mirror_bytes: u64,
    pub(crate) wallet_plan_logical_bytes: u64,
    pub(crate) mirror_wallet_plan_logical_bytes: u64,
    pub(crate) wallet_point_reads: u64,
    pub(crate) perf: super::metrics::PerfWrite,
}

impl Committer {
    /// Commits one derived block as a live append, with or without wallet rows.
    pub(crate) fn commit_append(
        &mut self,
        derived: DerivedBlock,
        mut perf: Option<&mut PerfProbe>,
    ) -> Result<CommitPhases, BenchError> {
        let mut phases = CommitPhases::default();
        let store = self
            .store
            .take()
            .ok_or_else(|| BenchError::spike("canonical store handle lost"))?;
        let started = Instant::now();
        let anchor = store.append_anchor()?;
        let height = derived.block.facts.block_header.height;
        let settled_tip = spike_next_append_settled_tip(&store, &anchor, height)
            .map_err(|source| spike_error("settled tip", source))?;
        let append = CanonicalLiveAppend::new(
            anchor.event_fence(),
            derived.block,
            derived.subtree_roots,
            settled_tip,
            UnixTimestampMillis::now(),
        );
        let prepared = store.spike_prepare_live_append(append, &self.activations)?;
        let canonical_prepared = Instant::now();
        phases.canonical_prepare = (canonical_prepared - started).as_secs_f64();

        let record_ops = self.density_lanes > 0 || self.pressure.is_some();
        let mut batch = WriteBatch::default();
        let mut pending = None;
        let mut wallet_ops = None;
        if let Some(wallet) = self.wallet.as_ref() {
            let event = prepared.retained_event()?;
            let replays = prepared.committed_replays()?;
            let expected_source =
                WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
            let planned = wallet.spike_plan_canonical_event_range(
                expected_source,
                event,
                prepared.resulting_fence(),
                prepared.settled_tip(),
                self.wallet_max,
                replays.into_iter().map(Ok),
                batch,
                record_ops,
            )?;
            batch = planned.batch;
            phases.wallet_plan_logical_bytes =
                planned.stats.batch_logical_bytes + planned.stats.overlay_logical_bytes;
            phases.wallet_point_reads = planned.stats.point_reads;
            pending = Some(planned.pending);
            wallet_ops = planned.ops;
        }
        let header_bytes = WriteBatch::default().size_in_bytes() as u64;
        let after_wallet = batch.size_in_bytes() as u64;
        phases.wallet_bytes = after_wallet - header_bytes;
        let wallet_planned = Instant::now();
        phases.wallet_plan = (wallet_planned - canonical_prepared).as_secs_f64();

        store.spike_put_prepared_live_append(&prepared, &mut batch)?;
        phases.canonical_bytes = batch.size_in_bytes() as u64 - after_wallet;
        let mirror_started = Instant::now();
        let mut mirror_work = None;
        let mut mirror_overlay = None;
        if record_ops {
            let mut canonical_rows = Vec::new();
            prepared.for_each_block_row(|family, key, value| {
                canonical_rows.push((family, key.to_vec(), value.to_vec()));
            });
            let work = MirrorWork {
                canonical_rows,
                wallet: wallet_ops,
            };
            if self.density_lanes > 0 {
                let before = batch.size_in_bytes();
                let overlay = mirror_overlay.insert(SpikeMirrorPlanOverlay::default());
                let counts = apply_lanes(
                    &store.spike_bounded_open().db,
                    &self.mirror_names,
                    &mut batch,
                    &work,
                    1..=self.density_lanes,
                    MirrorFamilies::All,
                    true,
                    Some(&mut self.density_accumulator),
                    Some(overlay),
                )?;
                phases.mirror_wallet_plan_logical_bytes =
                    overlay.batch_logical_bytes() + overlay.overlay_logical_bytes();
                self.density_counts.puts += counts.puts;
                self.density_counts.deletes += counts.deletes;
                self.density_counts.reads += counts.reads;
                self.density_counts.read_hits += counts.read_hits;
                phases.mirror_bytes = (batch.size_in_bytes() - before) as u64;
            }
            mirror_work = Some(work);
        }
        let mirrored = Instant::now();
        phases.mirror = (mirrored - mirror_started).as_secs_f64();
        phases.batch_bytes = batch.size_in_bytes() as u64;
        phases.batch_ops = batch.len() as u64;

        if let Some(perf) = perf.as_deref_mut() {
            perf.reset();
        }
        let (store, _fence, timing) = store.spike_commit_prepared_live_append(prepared, batch)?;
        if let Some(perf) = perf.as_deref() {
            phases.perf = perf.read();
        }
        // Held until the write finished, as the planner holds its overlay.
        drop(mirror_overlay);
        self.store = Some(store);
        phases.write = timing.write.as_secs_f64();
        phases.readback = timing.readback.as_secs_f64();
        let adopt_started = Instant::now();
        if let (Some(wallet), Some(pending)) = (self.wallet.as_mut(), pending) {
            wallet.spike_complete_planned_transition(pending)?;
        }
        let finished = Instant::now();
        phases.wallet_adopt = (finished - adopt_started).as_secs_f64();
        phases.total = (finished - started).as_secs_f64();
        if let (Some(pressure), Some(work)) = (self.pressure.as_mut(), mirror_work) {
            let work = if self.pressure_families == MirrorFamilies::All {
                work
            } else {
                MirrorWork {
                    canonical_rows: Vec::new(),
                    wallet: work.wallet,
                }
            };
            pressure.submit(work)?;
        }
        Ok(phases)
    }

    pub(crate) fn store(&self) -> Result<&RocksDbCanonicalStore, BenchError> {
        self.store
            .as_ref()
            .ok_or_else(|| BenchError::spike("canonical store handle lost"))
    }
}

struct SecondaryPoller {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<Result<(RocksDbCanonicalSecondary, Vec<f64>, u32), BenchError>>,
}

impl SecondaryPoller {
    fn spawn(
        mut secondary: RocksDbCanonicalSecondary,
        poll: Duration,
        primary_tip: Arc<AtomicU32>,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let mut durations = Vec::new();
            let mut max_lag = 0_u32;
            while !thread_stop.load(Ordering::Relaxed) {
                std::thread::sleep(poll);
                let started = Instant::now();
                secondary.try_catch_up()?;
                durations.push(started.elapsed().as_secs_f64());
                let lag = primary_tip
                    .load(Ordering::Relaxed)
                    .saturating_sub(secondary.event_fence().visible_tip().height.value());
                max_lag = max_lag.max(lag);
            }
            Ok((secondary, durations, max_lag))
        });
        Self { stop, handle }
    }

    fn finish(self) -> Result<(RocksDbCanonicalSecondary, Vec<f64>, u32), BenchError> {
        self.stop.store(true, Ordering::Relaxed);
        self.handle
            .join()
            .map_err(|_| BenchError::spike("secondary poller panicked"))?
    }
}

/// Final catch-up of a secondary and its single-fence checks.
pub(crate) fn finish_secondary(
    secondary: &mut RocksDbCanonicalSecondary,
    primary_fence: CanonicalEventFence,
    combined: bool,
) -> Result<(f64, FenceView, bool, Option<bool>), BenchError> {
    let started = Instant::now();
    secondary.try_catch_up()?;
    let seconds = started.elapsed().as_secs_f64();
    let fence = secondary.event_fence();
    let wallet_matches = if combined {
        let control = spike_decode_wallet_control(
            secondary.spike_bounded_open(),
            SPIKE_WALLET_CONTROL_COLUMN_FAMILY,
        )?;
        Some(
            if let WalletProjectionBuildState::Ready(evidence) = control.build_state {
                evidence.source_position.tip == fence.visible_tip()
                    && evidence.source_position.chain_epoch_id == fence.chain_epoch_id()
                    && evidence.source_position.event_sequence == fence.chain_event_sequence()
                    && evidence.source_sequence_digest == fence.sequence_digest()
            } else {
                false
            },
        )
    } else {
        None
    };
    Ok((seconds, FenceView::from(fence), fence == primary_fence, wallet_matches))
}

/// Scratch accumulator for mirror lanes, seeded with large row counts so a
/// lane may remove rows it never inserted (outputs created before H0).
pub(crate) fn mirror_scratch_accumulator() -> WalletProjectionDigestBuilder {
    let seed = u64::MAX / 2;
    let empty = WalletProjectionDigestBuilder::new();
    WalletProjectionDigestBuilder::from_parts(
        empty.accumulator().clone(),
        zinder_wallet_projection::WalletProjectionFamilyRowCounts {
            transparent_unspent_output_count: seed,
            transparent_unspent_output_by_address_count: seed,
            transparent_spent_output_count: seed,
            transparent_address_transaction_count: seed,
            transparent_address_balance_count: seed,
            reorg_undo_count: seed,
        },
    )
}

fn create_samples_file(path: &Path) -> Result<BufWriter<File>, BenchError> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| BenchError::io(path, source))?;
    Ok(BufWriter::new(file))
}

fn sibling(report: Option<&Path>, suffix: &str) -> Option<PathBuf> {
    report.map(|path| {
        let mut name = path.file_name().map_or_else(Default::default, |name| name.to_os_string());
        name.push(suffix);
        path.with_file_name(name)
    })
}

/// Runs one replay arm.
pub(crate) async fn run_replay(args: ReplayArgs) -> Result<ReplayReport, BenchError> {
    let network = args.common.network()?;
    let (manifest, activations) = args.common.manifest_and_activations()?;
    let activations = Arc::new(activations);
    if args.density_multiplier == 0 || args.density_multiplier + args.pressure_lanes > 255 {
        return Err(BenchError::invalid_argument(
            "--density-multiplier must be >= 1 and density plus pressure lanes <= 255",
        ));
    }
    let combined = args.mode == CommitMode::Combined;
    let density_lanes = u8::try_from(args.density_multiplier - 1)
        .map_err(|_| BenchError::invalid_argument("density multiplier too large"))?;
    let pressure_lanes = u8::try_from(args.pressure_lanes)
        .map_err(|_| BenchError::invalid_argument("pressure lanes too large"))?;
    let layout = Layout {
        wallet: combined,
        mirrors: density_lanes > 0 || pressure_lanes > 0,
    };
    let wallet_budget = wallet_family_budget();
    let budget = writer_budget(
        combined,
        args.common.budget_profile,
        args.common.max_background_jobs,
        args.common.statistics_level,
    );

    let copy = copy_closed_store(&args.golden, &args.work)?;
    ensure_column_families(&args.work, &layout.extra_names())?;
    layout.register(wallet_budget);
    let stalls = StallRecorder::new();
    install_stall_listener(&stalls);

    let rss_start = current_rss_bytes();
    let open_started = Instant::now();
    let store = open_canonical(&args.work, &activations, args.common.reorg_window_blocks, budget)?;
    let wallet = if combined {
        Some(RocksDbWalletStore::spike_open_shared(
            store.spike_bounded_open().clone(),
            network,
        )?)
    } else {
        None
    };
    let open_seconds = open_started.elapsed().as_secs_f64();

    let anchor = store.append_anchor()?;
    let h0 = anchor.event_fence().visible_tip();
    let from = h0
        .height
        .next()
        .ok_or_else(|| BenchError::spike("H0 is u32::MAX"))?;
    let through = BlockHeight::new(args.through_height.unwrap_or(manifest.to_height));
    if manifest.from_height > from.value() || through.value() > manifest.to_height || through < from
    {
        return Err(BenchError::invalid_argument(format!(
            "fixture covers {}..={} but the replay needs {}..={}",
            manifest.from_height,
            manifest.to_height,
            from.value(),
            through.value()
        )));
    }
    if let Some(wallet) = wallet.as_ref() {
        let source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        if source.source_position().tip != h0 {
            return Err(BenchError::spike(
                "combined wallet source tip differs from the canonical H0 fence",
            ));
        }
    }
    let expected_blocks = u64::from(through.value() - from.value() + 1);
    let primary_tip = Arc::new(AtomicU32::new(h0.height.value()));

    let mut secondary_report = None;
    let mut secondary_state = None;
    let mut secondary_child = None;
    if let Some(dir) = args.secondary_dir.as_ref() {
        if args.secondary_in_process {
            let started = Instant::now();
            let secondary =
                open_secondary(&args.work, dir, &activations, args.common.reorg_window_blocks)?;
            let open = started.elapsed().as_secs_f64();
            secondary_report = Some(SecondaryReport {
                path: absolute(dir).display().to_string(),
                open_seconds: open,
                poll_ms: args.secondary_poll_ms,
                ..SecondaryReport::default()
            });
            secondary_state = Some(if args.secondary_poll_ms > 0 {
                Err(SecondaryPoller::spawn(
                    secondary,
                    Duration::from_millis(args.secondary_poll_ms),
                    Arc::clone(&primary_tip),
                ))
            } else {
                Ok(secondary)
            });
        } else {
            secondary_report = Some(SecondaryReport {
                path: absolute(dir).display().to_string(),
                poll_ms: args.secondary_poll_ms,
                separate_process: true,
                ..SecondaryReport::default()
            });
            secondary_child = Some(SecondaryChild::spawn(
                &args.common.fixture,
                &args.common.network,
                &args.work,
                dir,
                args.secondary_poll_ms,
                args.common.reorg_window_blocks,
            )?);
        }
    }

    let db = Arc::clone(&store.spike_bounded_open().db);
    let statistics = Arc::clone(&store.spike_bounded_open().statistics);
    let family_names = db.cf_names();
    let sampler = PropertySampler::spawn(
        Arc::clone(&db),
        family_names,
        Duration::from_millis(args.property_sample_ms.max(10)),
    );
    let tickers_before = ticker_snapshot(&statistics);
    let mirror_names = MirrorFamilyNames::new();
    let pressure = (pressure_lanes > 0).then(|| {
        PressureWriter::spawn(
            Arc::clone(&db),
            mirror_names.clone(),
            density_lanes + 1,
            pressure_lanes,
            args.pressure_families,
            args.pressure_queue_blocks,
        )
    });

    let source = FixtureNodeSource::open(&args.common.fixture, &manifest)?;
    let receiver = spawn_pipeline(
        &tokio::runtime::Handle::current(),
        source,
        anchor.tip_checkpoint().clone(),
        from,
        through,
        Arc::clone(&activations),
        RawBlobRetention::Transactions,
        args.pipeline_depth,
    );
    let samples_writer = args.samples.as_deref().map(create_samples_file).transpose()?;

    let committer = Committer {
        store: Some(store),
        wallet,
        activations: Arc::clone(&activations),
        wallet_max: NonZeroU64::new(args.wallet_max_transition_bytes)
            .ok_or_else(|| BenchError::invalid_argument("wallet ceiling must be non-zero"))?,
        density_lanes,
        mirror_names,
        pressure,
        pressure_families: args.pressure_families,
        density_accumulator: mirror_scratch_accumulator(),
        density_counts: MirrorCounts::default(),
        perf_enabled: !args.no_perf_context,
    };
    let progress_every = u64::from(args.progress_every.max(1));
    let loop_tip = Arc::clone(&primary_tip);
    let (committer, series, loop_stats, samples_writer) = tokio::task::spawn_blocking(move || {
        commit_loop(
            committer,
            receiver,
            expected_blocks,
            progress_every,
            &loop_tip,
            samples_writer,
        )
    })
    .await
    .map_err(|source| spike_error("commit loop join", source))??;
    let Committer {
        store,
        wallet,
        pressure,
        density_counts,
        ..
    } = committer;
    let store = store.ok_or_else(|| BenchError::spike("canonical store handle lost"))?;
    if let Some(mut writer) = samples_writer {
        writer
            .flush()
            .map_err(|source| spike_error("flush samples", source))?;
    }

    let pressure_stats = pressure.map(PressureWriter::finish).transpose()?;
    let properties = sampler.finish();
    let tickers_after = ticker_snapshot(&statistics);
    let tickers = ticker_delta(&tickers_before, &tickers_after);
    let stall_summary = stalls.summary();
    let final_fence = store.event_fence();

    if let (Some(report), Some(child)) = (secondary_report.as_mut(), secondary_child) {
        let follower_report = child.report_path().display().to_string();
        let follow = child.finish()?;
        report.open_seconds = follow.open_seconds;
        report.periodic_catch_ups = LatencySummary {
            count: follow.periodic_catch_up_count,
            p50_ms: follow.periodic_catch_up_p50_ms,
            p99_ms: follow.periodic_catch_up_p99_ms,
            max_ms: follow.periodic_catch_up_max_ms,
            ..LatencySummary::default()
        };
        report.final_catch_up_seconds = follow.final_catch_up_seconds;
        report.final_fence_matches_primary = follow.final_tip_height
            == final_fence.visible_tip().height.value()
            && follow.final_event_sequence == final_fence.chain_event_sequence();
        report.wallet_control_matches_fence = follow.wallet_control_matches_fence;
        report.follower_peak_vmhwm_bytes = follow.peak_vmhwm_bytes;
        report.follower_report = Some(follower_report);
    }
    if let (Some(report), Some(state)) = (secondary_report.as_mut(), secondary_state) {
        let mut secondary = match state {
            Ok(secondary) => secondary,
            Err(poller) => {
                let (secondary, durations, max_lag) = poller.finish()?;
                report.periodic_catch_ups = summarize_seconds(&durations);
                report.max_observed_lag_blocks = max_lag;
                secondary
            }
        };
        let (seconds, fence, matches, wallet_matches) =
            finish_secondary(&mut secondary, final_fence, combined)?;
        report.final_catch_up_seconds = seconds;
        report.final_fence = Some(fence);
        report.final_fence_matches_primary = matches;
        report.wallet_control_matches_fence = wallet_matches;
    }

    // Writer peak for the replay itself, read before the optional cold readback.
    let peak = peak_rss_bytes();
    let rss_end = current_rss_bytes();
    let mut cold_readback_seconds = None;
    if let (true, Some(wallet)) = (args.cold_readback, wallet.as_ref()) {
        let readback = wallet.spike_cold_readback(512 * 1024 * 1024)?;
        cold_readback_seconds = Some(readback.elapsed.as_secs_f64());
    }
    let wallet_final = wallet
        .as_ref()
        .map(|wallet| WalletView::from(wallet.ready_evidence()));

    let peak_after_cold_readback = peak_rss_bytes();
    let raw_files = write_raw_files(&args, &stalls, &db)?;
    let wall_seconds = loop_stats.wall.as_secs_f64();
    let blocks = loop_stats.blocks;
    let chain_time_seconds = loop_stats.last_block_time - loop_stats.first_block_time_parent;
    let chain_hours = (chain_time_seconds.max(1) as f64) / 3600.0;
    let stall_seconds = tickers.get("rocksdb.stall.micros").copied().unwrap_or(0) as f64 / 1e6;
    let user_bytes = tickers.get("rocksdb.bytes.written").copied().unwrap_or(0);
    let flush_bytes = tickers.get("rocksdb.flush.write.bytes").copied().unwrap_or(0);
    let compaction_bytes = tickers.get("rocksdb.compact.write.bytes").copied().unwrap_or(0);
    let commit_total = summarize_seconds(&series.commit_total);
    let commit_path_seconds = commit_total.total_seconds;
    let blocks_per_second = blocks as f64 / wall_seconds.max(f64::EPSILON);
    let perf = loop_stats.perf.clone();
    let stall_rates = StallRates {
        ticker_stall_seconds: stall_seconds,
        ticker_stall_seconds_per_chain_hour: stall_seconds / chain_hours,
        ticker_stall_seconds_per_wall_hour: stall_seconds / (wall_seconds.max(1.0) / 3600.0),
        commit_write_delay_seconds_per_chain_hour: perf.write_delay_total_seconds / chain_hours,
    };
    let thresholds = vec![
        ThresholdCheck {
            check: "tip commit p99 at most 1 s",
            threshold: "<= 1000 ms".to_owned(),
            observed: format!("{:.3} ms", commit_total.p99_ms),
            passed: commit_total.p99_ms <= 1000.0,
        },
        ThresholdCheck {
            check: "replay at least 10 blocks per second",
            threshold: ">= 10 blocks/s".to_owned(),
            observed: format!("{blocks_per_second:.2} blocks/s"),
            passed: blocks_per_second >= 10.0,
        },
        ThresholdCheck {
            check: "writer RSS at most 6 GiB",
            threshold: "<= 6 GiB".to_owned(),
            observed: format!("{:.3} GiB", peak.unwrap_or(0) as f64 / GIB),
            passed: peak.is_some_and(|bytes| bytes as f64 <= 6.0 * GIB),
        },
        ThresholdCheck {
            check: "zero stop stalls",
            threshold: "0 stop entries".to_owned(),
            observed: format!(
                "{} stop entries, {:.3} s stopped condition, {} stopped samples",
                stall_summary.db_stop_entries,
                stall_summary.db_stopped_condition_seconds,
                properties.write_stopped_samples
            ),
            passed: stall_summary.db_stop_entries == 0 && properties.write_stopped_samples == 0,
        },
        ThresholdCheck {
            check: "delay stalls at most 1 s per replayed chain hour",
            threshold: "<= 1 s per chain hour".to_owned(),
            observed: format!(
                "{:.4} s per chain hour (ticker), {:.4} s commit-thread delay per chain hour",
                stall_rates.ticker_stall_seconds_per_chain_hour,
                stall_rates.commit_write_delay_seconds_per_chain_hour
            ),
            passed: stall_rates.ticker_stall_seconds_per_chain_hour <= 1.0,
        },
    ];

    Ok(ReplayReport {
        spike: "A-combined-commit",
        action: "replay",
        network_label: NETWORK_LABEL_TESTNET,
        label: args.common.label.clone(),
        mode: args.mode,
        layout,
        writer_budget: BudgetView::from(budget),
        wallet_family_budget: combined.then(|| BudgetView::from(wallet_budget)),
        golden: absolute(&args.golden).display().to_string(),
        work: absolute(&args.work).display().to_string(),
        copy,
        h0: BlockRef {
            height: h0.height.value(),
            hash_hex: hex32(h0.hash.as_bytes()),
        },
        through: BlockRef {
            height: final_fence.visible_tip().height.value(),
            hash_hex: hex32(final_fence.visible_tip().hash.as_bytes()),
        },
        blocks,
        density_multiplier: args.density_multiplier,
        pressure_to_commit_bytes_ratio: pressure_stats.map(|stats| {
            stats.bytes as f64 / summarize_values(&series.batch_bytes).total.max(1.0)
        }),
        pressure: pressure_stats,
        density_mirror_counts: density_counts,
        density_effective_byte_multiplier: {
            let real = summarize_values(&series.canonical_bytes).total
                + summarize_values(&series.wallet_bytes).total;
            (real + summarize_values(&series.mirror_bytes).total) / real.max(1.0)
        },
        open_seconds,
        wall_seconds,
        blocks_per_second,
        commit_path_seconds,
        commit_path_blocks_per_second: blocks as f64 / commit_path_seconds.max(f64::EPSILON),
        pipeline_wait_seconds: loop_stats.pipeline_wait.as_secs_f64(),
        latency: LatencyBlock {
            commit_total,
            canonical_prepare: summarize_seconds(&series.canonical_prepare),
            wallet_plan: summarize_seconds(&series.wallet_plan),
            mirror_lanes: summarize_seconds(&series.mirror_lanes),
            synced_write: summarize_seconds(&series.synced_write),
            canonical_readback: summarize_seconds(&series.canonical_readback),
            wallet_adopt: summarize_seconds(&series.wallet_adopt),
            fixture_fetch: summarize_seconds(&series.fixture_fetch),
            block_derive: summarize_seconds(&series.block_derive),
        },
        batch: BatchBlock {
            total_bytes: summarize_values(&series.batch_bytes),
            operations: summarize_values(&series.batch_ops),
            canonical_bytes: summarize_values(&series.canonical_bytes),
            wallet_bytes: summarize_values(&series.wallet_bytes),
            mirror_bytes: summarize_values(&series.mirror_bytes),
            wallet_plan_logical_bytes: summarize_values(&series.wallet_plan_logical),
            mirror_wallet_plan_logical_bytes: summarize_values(&series.mirror_wallet_plan_logical),
            wallet_point_reads: summarize_values(&series.wallet_point_reads),
        },
        workload: WorkloadBlock {
            transactions: summarize_values(&series.transactions),
            transparent_inputs: summarize_values(&series.transparent_inputs),
            transparent_outputs: summarize_values(&series.transparent_outputs),
            source_block_bytes: summarize_values(&series.source_bytes),
            chain_time_seconds,
        },
        perf,
        rss: RssBlock {
            start_bytes: rss_start,
            end_bytes: rss_end,
            max_sampled_bytes: loop_stats.max_rss,
            peak_vmhwm_bytes: peak,
            peak_vmhwm_after_cold_readback_bytes: peak_after_cold_readback,
        },
        tickers,
        write_amplification: WriteAmplification {
            user_bytes_written: user_bytes,
            wal_bytes: tickers_after
                .get("rocksdb.wal.bytes")
                .copied()
                .unwrap_or(0)
                .saturating_sub(tickers_before.get("rocksdb.wal.bytes").copied().unwrap_or(0)),
            flush_write_bytes: flush_bytes,
            compaction_write_bytes: compaction_bytes,
            amplification: (flush_bytes + compaction_bytes) as f64 / (user_bytes.max(1) as f64),
        },
        stall_rates,
        stalls: stall_summary,
        properties,
        secondary: secondary_report,
        final_fence: FenceView::from(final_fence),
        wallet_final,
        cold_readback_seconds,
        work_store_bytes: directory_bytes(&args.work),
        thresholds,
        raw_files,
    })
}

fn write_raw_files(
    args: &ReplayArgs,
    stalls: &StallRecorder,
    db: &rust_rocksdb::DB,
) -> Result<RawFiles, BenchError> {
    let mut raw = RawFiles {
        samples: args.samples.as_ref().map(|path| absolute(path).display().to_string()),
        ..RawFiles::default()
    };
    if let Some(path) = sibling(args.common.report.as_deref(), ".stall-transitions.json") {
        let encoded = serde_json::to_vec_pretty(&stalls.transitions())?;
        std::fs::write(&path, encoded).map_err(|source| BenchError::io(&path, source))?;
        raw.stall_transitions = Some(absolute(&path).display().to_string());
    }
    if let Some(path) = sibling(args.common.report.as_deref(), ".rocksdb-stats.txt") {
        let stats = db
            .property_value("rocksdb.stats")
            .map_err(|source| spike_error("rocksdb.stats", source))?
            .unwrap_or_default();
        std::fs::write(&path, stats).map_err(|source| BenchError::io(&path, source))?;
        raw.rocksdb_stats = Some(absolute(&path).display().to_string());
    }
    Ok(raw)
}

#[derive(Default)]
struct LoopStats {
    blocks: u64,
    wall: Duration,
    pipeline_wait: Duration,
    max_rss: u64,
    first_block_time_parent: i64,
    last_block_time: i64,
    perf: PerfBlock,
}

fn commit_loop(
    mut committer: Committer,
    mut receiver: mpsc::Receiver<Result<DerivedBlock, BenchError>>,
    expected_blocks: u64,
    progress_every: u64,
    primary_tip: &AtomicU32,
    mut samples: Option<BufWriter<File>>,
) -> Result<(Committer, Series, LoopStats, Option<BufWriter<File>>), BenchError> {
    let mut perf = committer
        .perf_enabled
        .then(PerfProbe::enable_for_thread);
    let mut series = Series::default();
    let mut stats = LoopStats {
        perf: PerfBlock {
            enabled: committer.perf_enabled,
            ..PerfBlock::default()
        },
        ..LoopStats::default()
    };
    let started = Instant::now();
    let mut window_started = started;
    loop {
        let wait_started = Instant::now();
        let Some(next) = receiver.blocking_recv() else {
            break;
        };
        let wait = wait_started.elapsed();
        stats.pipeline_wait += wait;
        let derived = next?;
        let height = derived.block.facts.block_header.height.value();
        let block_time = derived.block.facts.block_header.block_time;
        if stats.blocks == 0 {
            let parent_time = committer
                .store()?
                .block_header_at(BlockHeight::new(height.saturating_sub(1)))?
                .map_or(block_time, |header| header.block_time);
            stats.first_block_time_parent = parent_time;
        }
        stats.last_block_time = block_time;
        let fetch = derived.fetch_seconds;
        let derive = derived.derive_seconds;
        let transactions = derived.transaction_count;
        let inputs = derived.transparent_input_count;
        let outputs = derived.transparent_output_count;
        let source_bytes = derived.source_bytes;
        let phases = committer.commit_append(derived, perf.as_mut())?;
        primary_tip.store(height, Ordering::Relaxed);
        stats.blocks += 1;

        series.commit_total.push(phases.total);
        series.canonical_prepare.push(phases.canonical_prepare);
        series.wallet_plan.push(phases.wallet_plan);
        series.mirror_lanes.push(phases.mirror);
        series.synced_write.push(phases.write);
        series.canonical_readback.push(phases.readback);
        series.wallet_adopt.push(phases.wallet_adopt);
        series.fixture_fetch.push(fetch);
        series.block_derive.push(derive);
        series.batch_bytes.push(phases.batch_bytes as f64);
        series.batch_ops.push(phases.batch_ops as f64);
        series.canonical_bytes.push(phases.canonical_bytes as f64);
        series.wallet_bytes.push(phases.wallet_bytes as f64);
        series.mirror_bytes.push(phases.mirror_bytes as f64);
        series
            .wallet_plan_logical
            .push(phases.wallet_plan_logical_bytes as f64);
        series
            .mirror_wallet_plan_logical
            .push(phases.mirror_wallet_plan_logical_bytes as f64);
        series
            .wallet_point_reads
            .push(phases.wallet_point_reads as f64);
        series.transactions.push(transactions as f64);
        series.transparent_inputs.push(inputs as f64);
        series.transparent_outputs.push(outputs as f64);
        series.source_bytes.push(source_bytes as f64);
        let delay_seconds = phases.perf.write_delay_ns as f64 / 1e9;
        if phases.perf.write_delay_ns > 0 {
            stats.perf.commits_with_write_delay += 1;
        }
        stats.perf.write_delay_total_seconds += delay_seconds;
        stats.perf.write_delay_max_ms = stats.perf.write_delay_max_ms.max(delay_seconds * 1000.0);
        stats.perf.write_wal_total_seconds += phases.perf.write_wal_ns as f64 / 1e9;
        stats.perf.write_memtable_total_seconds += phases.perf.write_memtable_ns as f64 / 1e9;

        let sample_rss = stats.blocks % 100 == 0 || stats.blocks == 1;
        let rss = if sample_rss { current_rss_bytes() } else { None };
        if let Some(rss) = rss {
            stats.max_rss = stats.max_rss.max(rss);
        }
        if let Some(writer) = samples.as_mut() {
            serde_json::to_writer(
                &mut *writer,
                &BlockSample {
                    height,
                    transactions,
                    transparent_inputs: inputs,
                    transparent_outputs: outputs,
                    source_bytes,
                    fetch_ms: fetch * 1000.0,
                    derive_ms: derive * 1000.0,
                    pipeline_wait_ms: wait.as_secs_f64() * 1000.0,
                    canonical_prepare_ms: phases.canonical_prepare * 1000.0,
                    wallet_plan_ms: phases.wallet_plan * 1000.0,
                    mirror_ms: phases.mirror * 1000.0,
                    write_ms: phases.write * 1000.0,
                    readback_ms: phases.readback * 1000.0,
                    wallet_adopt_ms: phases.wallet_adopt * 1000.0,
                    commit_ms: phases.total * 1000.0,
                    batch_bytes: phases.batch_bytes,
                    batch_operations: phases.batch_ops,
                    canonical_bytes: phases.canonical_bytes,
                    wallet_bytes: phases.wallet_bytes,
                    mirror_bytes: phases.mirror_bytes,
                    wallet_plan_logical_bytes: phases.wallet_plan_logical_bytes,
                    mirror_wallet_plan_logical_bytes: phases.mirror_wallet_plan_logical_bytes,
                    wallet_point_reads: phases.wallet_point_reads,
                    perf_write_delay_us: phases.perf.write_delay_ns as f64 / 1000.0,
                    perf_write_wal_us: phases.perf.write_wal_ns as f64 / 1000.0,
                    rss_bytes: rss,
                },
            )?;
            writer
                .write_all(b"\n")
                .map_err(|source| spike_error("write samples", source))?;
        }
        if stats.blocks % progress_every == 0 {
            let window = window_started.elapsed().as_secs_f64();
            tracing::info!(
                target: "zinder::bench::spike",
                event = "spike_replay_progress",
                height,
                blocks = stats.blocks,
                expected_blocks,
                window_blocks_per_second = progress_every as f64 / window.max(f64::EPSILON),
                elapsed_seconds = started.elapsed().as_secs_f64(),
                "spike replay progress"
            );
            window_started = Instant::now();
        }
    }
    stats.wall = started.elapsed();
    if stats.blocks != expected_blocks {
        return Err(BenchError::spike(format!(
            "replayed {} blocks, expected {expected_blocks}",
            stats.blocks
        )));
    }
    if let Some(rss) = current_rss_bytes() {
        stats.max_rss = stats.max_rss.max(rss);
    }
    Ok((committer, series, stats, samples))
}
