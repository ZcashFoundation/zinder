//! Spike B: in-process serving from `RocksDB` primary snapshots.
//!
//! Throwaway harness. One process owns the canonical primary and the wallet
//! primary (two real stores, because the wallet families cannot join the
//! canonical database without the store changes Spike A is testing). A writer
//! thread replays real Testnet blocks through the production preparation and
//! commit paths, applies each canonical event to the wallet in the same
//! process, and readers in the same process hold primary snapshots while it
//! commits. See `plan.rs` for the phase model.

#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::wildcard_enum_match_arm,
    clippy::allow_attributes_without_reason,
    dead_code,
    missing_docs,
    unreachable_pub,
    reason = "throwaway in-process serving spike harness; not for merge"
)]

mod index_attach;
mod observer;
mod output;
mod plan;
mod readers;
mod sampler;
mod source;
mod stats;
mod writer;

use std::{
    collections::BTreeMap,
    fs,
    num::NonZeroU64,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use clap::Args;
use parking_lot::Mutex;
use rust_rocksdb::{DB, IteratorMode, Options};
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use zinder_bench::BenchError;
use zinder_core::{BlockHeight, BlockHeightRange, wire::decode_zinder_native_chain_name};
use zinder_ingest::CanonicalFollowConfig;
use zinder_source::{
    CookieSource, NodeAuth, NodeSource, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions,
};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalStore,
    RocksDbResourceBudget, SpikeCompactBlockRead, spike_compact_blocks,
    spike_install_batch_observer,
};
use zinder_wallet_rocksdb::{RocksDbWalletBuildOptions, RocksDbWalletStore};

use self::{
    index_attach::{AttachContext, IndexAttachRecord, run_index_attach},
    observer::{BALLAST_COLUMN_FAMILY, SpikeObserver},
    output::{Outputs, RunClock},
    plan::{Phase, Plan, ReaderSpec},
    readers::{
        CompactReaderParams, LoadKind, LoadLatencies, LoadSamples, LoadSummary,
        MANIFEST_COLUMN_FAMILY, ReaderCycleRecord, WalletReaderParams, compact_range_cycle,
        run_load_worker, run_reader_loop, wallet_family_cycle,
    },
    sampler::{DbProbe, SampleRecord, cf_stall_counts, run_sampler, sample_db},
    source::PacedReplaySource,
    stats::{LatencySummary, directory_bytes, process_memory},
    writer::{
        ReplacementRecord, Shared, WriterRequest, WriterState, apply_wallet_events,
        run_writer_phase,
    },
};

const WALLET_OUTPOINT_SORT_MEMORY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER: u64 = 1024 * 1024 * 1024;
const WALLET_TEMPORARY_FILE_BYTES_PER_SORTER: u64 = 64 * 1024 * 1024 * 1024;
const WALLET_SST_TARGET_LOGICAL_BYTES: u64 = 128 * 1024 * 1024;
const WALLET_ACCOUNTED_REORG_UNDO_BYTES: u64 = 512 * 1024 * 1024;
const PROJECTOR_DEFAULT_MAX_TRANSITION_BYTES: u64 = 512 * 1024 * 1024;

/// CLI contract for the in-process serving spike.
#[derive(Args)]
pub(crate) struct InProcessServingArgs {
    /// Network name, such as zcash-testnet.
    #[arg(long)]
    network: String,
    /// Node JSON-RPC URL used for activations and replay blocks.
    #[arg(long = "json-rpc-addr")]
    json_rpc_addr: String,
    /// Node cookie file.
    #[arg(long = "node-auth-cookie")]
    node_auth_cookie: Option<PathBuf>,
    /// READY canonical store to copy from; never opened or modified.
    #[arg(long = "canonical-source")]
    canonical_source: PathBuf,
    /// READY wallet store to copy from; never opened or modified.
    #[arg(long = "wallet-source")]
    wallet_source: Option<PathBuf>,
    /// Absent scratch directory that receives the store copies and staging.
    #[arg(long = "work-dir")]
    work_dir: PathBuf,
    /// Absent directory for the report and JSON-lines results.
    #[arg(long = "out-dir")]
    out_dir: PathBuf,
    /// `hardlink` links immutable SST files and copies the rest; `copy` copies everything.
    #[arg(long = "copy-mode", default_value = "hardlink")]
    copy_mode: String,
    /// Phase plan JSON; overrides --plan-preset.
    #[arg(long)]
    plan: Option<PathBuf>,
    /// Built-in plan: `measured` or `smoke`.
    #[arg(long = "plan-preset", default_value = "measured")]
    plan_preset: String,
    /// Write the selected plan as JSON to this path and exit.
    #[arg(long = "print-plan")]
    print_plan: Option<PathBuf>,
    /// Persisted canonical raw-blob retention.
    #[arg(long = "raw-blob-retention", default_value = "transactions")]
    raw_blob_retention: String,
    /// Canonical reorg window and wallet undo depth.
    #[arg(long, default_value_t = zinder_store::DEFAULT_REORG_WINDOW_BLOCKS)]
    reorg_window_blocks: u32,
    /// Last replayed height; defaults to the node tip at start minus 20.
    #[arg(long = "replay-end-height")]
    replay_end_height: Option<u32>,
    /// Run even when the plan needs more blocks than the replay range holds.
    #[arg(long = "allow-short-replay")]
    allow_short_replay: bool,
    /// Sampling interval for RocksDB properties.
    #[arg(long = "sample-interval-ms", default_value_t = 1000)]
    sample_interval_ms: u64,
    /// Include the live-file listing every N samples (0 disables).
    #[arg(long = "live-files-every", default_value_t = 5)]
    live_files_every: u32,
    /// Blocks prefetched above the writer tip.
    #[arg(long = "prefetch-window", default_value_t = 2000)]
    prefetch_window: u32,
    /// Concurrent node requests while prefetching.
    #[arg(long = "prefetch-concurrency", default_value_t = 8)]
    prefetch_concurrency: usize,
    /// Per-request node timeout.
    #[arg(long = "request-timeout-secs", default_value_t = 30)]
    request_timeout_secs: u64,
    /// Wallet transition logical-byte ceiling (projector default 512 MiB).
    #[arg(long = "wallet-max-transition-bytes", default_value_t = PROJECTOR_DEFAULT_MAX_TRANSITION_BYTES)]
    wallet_max_transition_bytes: u64,
    /// Phase whose commit latencies are the no-reader baseline; defaults to the first phase.
    #[arg(long = "baseline-phase")]
    baseline_phase: Option<String>,
    /// Exact source revision of the measured binary.
    #[arg(long = "software-revision", default_value = "unknown")]
    software_revision: String,
    /// Copy the stores into --work-dir and exit (for running other tools on a private copy).
    #[arg(long = "prepare-only")]
    prepare_only: bool,
    /// Replace the multiplier of every density phase (phases whose multiplier is above 1).
    #[arg(long = "density-multiplier-override")]
    density_multiplier_override: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct SpaceSummary {
    live_sst_bytes_start: u64,
    live_sst_bytes_max: u64,
    live_sst_bytes_end: u64,
    disk_sst_bytes_start: u64,
    disk_sst_bytes_max: u64,
    disk_sst_bytes_end: u64,
    obsolete_sst_bytes_max: u64,
    disk_minus_live_files_bytes_max: Option<u64>,
    pending_compaction_bytes_max: u64,
    /// Live SST bytes minus RocksDB's live-data estimate: versions and tombstones not yet dropped.
    garbage_estimate_bytes_start: u64,
    garbage_estimate_bytes_max: u64,
    garbage_estimate_bytes_end: u64,
    max_l0_files: u64,
    num_snapshots_max: u64,
    num_live_versions_max: u64,
    write_stopped_samples: u64,
    delayed_write_rate_max: u64,
    stall_micros_delta: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
struct StallDelta {
    cf_counts: BTreeMap<String, u64>,
    db_counts: BTreeMap<String, u64>,
    stall_micros: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
struct ReaderGroupSummary {
    kind: String,
    snapshot: bool,
    held_iterator: bool,
    family: Option<String>,
    hold_target_s: f64,
    cycles: usize,
    hold_actual_s_max: f64,
    truncated_cycles: usize,
    pass1_rows_per_s: LatencySummary,
    pass1_mib_per_s: LatencySummary,
    pass1_seconds: LatencySummary,
    paced_reads: u64,
    violation_count: u64,
    cycles_with_violations: usize,
    errors: usize,
    first_violations: Vec<String>,
    first_errors: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct CompactionAtEnd {
    canonical_seconds: f64,
    canonical_live_sst_before: u64,
    canonical_live_sst_after: u64,
    wallet_seconds: Option<f64>,
    wallet_live_sst_before: Option<u64>,
    wallet_live_sst_after: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct PhaseSummary {
    name: String,
    baseline_name: Option<String>,
    pacing: String,
    block_interval_ms: u64,
    density_multiplier: f64,
    t_start: f64,
    t_end: f64,
    start_tip: u32,
    end_tip: u32,
    commits: usize,
    paced_commits: usize,
    source_exhausted: bool,
    canonical_commit_s: LatencySummary,
    canonical_write_s: LatencySummary,
    wallet_apply_s: LatencySummary,
    wallet_write_s: LatencySummary,
    total_commit_s: LatencySummary,
    canonical_batch_bytes_mean: f64,
    canonical_base_bytes_mean: f64,
    wallet_batch_bytes_mean: f64,
    wallet_base_bytes_mean: f64,
    replacements: Vec<ReplacementRecord>,
    readers: Vec<ReaderGroupSummary>,
    load: Option<LoadSummary>,
    index_attach: Option<IndexAttachRecord>,
    commits_during_attach_stage: BTreeMap<String, LatencySummary>,
    canonical_space: SpaceSummary,
    wallet_space: Option<SpaceSummary>,
    canonical_stalls: StallDelta,
    wallet_stalls: Option<StallDelta>,
    compaction_at_end: Option<CompactionAtEnd>,
    wallet_detached: Option<String>,
    peak_rss_bytes_end: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct PhaseCheck {
    phase: String,
    baseline_phase: String,
    total_commit_p99_s: f64,
    canonical_commit_p99_s: f64,
    total_p99_ratio_to_baseline: Option<f64>,
    canonical_p99_ratio_to_baseline: Option<f64>,
    p99_over_1s: bool,
    p99_over_2x_baseline: bool,
    stop_stalls: u64,
    write_stopped_samples: u64,
    snapshot_reader_violations: u64,
    control_reader_violations: u64,
    /// Longest writer block for one replacement (canonical commit plus wallet apply).
    max_replacement_writer_blocked_s: Option<f64>,
    /// Longest writer block for the index-attach final switch.
    attach_switch_writer_blocked_s: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct FailConditionChecks {
    baseline_phase: String,
    baseline_total_commit_p99_s: f64,
    baseline_canonical_commit_p99_s: f64,
    any_stop_stall: bool,
    any_snapshot_reader_torn_view: bool,
    control_readers_detected_changes: bool,
    any_p99_over_1s: bool,
    any_p99_over_2x_baseline: bool,
    phases: Vec<PhaseCheck>,
    note: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct StoreSetup {
    canonical_source: String,
    wallet_source: Option<String>,
    work_dir: String,
    copy_mode: String,
    copy_seconds: f64,
    canonical_bytes_start: u64,
    wallet_bytes_start: Option<u64>,
    canonical_tip_start: u32,
    wallet_tip_start: Option<u32>,
    wallet_events_applied_to_align: u32,
    first_retained_height: u32,
    canonical_io_mode: String,
    canonical_budget: String,
    wallet_budget: String,
    ballast_pool_rows: (usize, usize),
    sampled_addresses: usize,
    sampled_transaction_ids: usize,
}

#[derive(Clone, Debug, Serialize)]
struct InProcessServingReport {
    contract_identity: &'static str,
    network_label: String,
    software_revision: String,
    node_json_rpc_addr: String,
    node_tip_at_start: u32,
    replay_start_height: u32,
    replay_end_height: u32,
    plan_required_blocks_estimate: u64,
    stores: StoreSetup,
    plan: Plan,
    phases: Vec<PhaseSummary>,
    fail_conditions: FailConditionChecks,
    node_fetches: u64,
    source_cache_hits: u64,
    peak_rss_bytes: Option<u64>,
    total_seconds: f64,
    results: BTreeMap<&'static str, String>,
}

/// Output of one run: the report path.
pub(crate) struct InProcessServingOutput {
    pub(crate) report_path: Option<PathBuf>,
}

fn load_plan(args: &InProcessServingArgs) -> Result<Plan, BenchError> {
    let plan = if let Some(path) = &args.plan {
        let bytes = fs::read(path).map_err(|source| BenchError::io(path, source))?;
        serde_json::from_slice::<Plan>(&bytes)?
    } else {
        match args.plan_preset.as_str() {
            "measured" => plan::measured_plan(),
            "smoke" => plan::smoke_plan(),
            other => {
                return Err(BenchError::invalid_argument(format!(
                    "unknown --plan-preset {other}"
                )));
            }
        }
    };
    let mut plan = plan;
    if let Some(multiplier) = args.density_multiplier_override {
        for phase in &mut plan.phases {
            if phase.density_multiplier > 1.0 {
                phase.density_multiplier = multiplier;
            }
        }
    }
    for phase in &plan.phases {
        phase.validate().map_err(BenchError::invalid_argument)?;
    }
    if plan.phases.is_empty() {
        return Err(BenchError::invalid_argument("plan has no phases"));
    }
    Ok(plan)
}

fn required_blocks(plan: &Plan) -> u64 {
    plan.phases
        .iter()
        .map(|phase| {
            let by_duration = match (phase.duration_seconds, phase.block_interval_ms) {
                (Some(seconds), 0) => seconds * 50,
                (Some(seconds), interval) => seconds * 1000 / interval + 1,
                (None, _) => u64::MAX,
            };
            phase
                .blocks
                .map_or(by_duration, |blocks| u64::from(blocks).min(by_duration))
        })
        .sum()
}

/// Runs the spike end to end.
pub(crate) async fn run_in_process_serving(
    args: InProcessServingArgs,
) -> Result<InProcessServingOutput, BenchError> {
    let plan = load_plan(&args)?;
    if let Some(path) = &args.print_plan {
        fs::write(path, serde_json::to_vec_pretty(&plan)?)
            .map_err(|source| BenchError::io(path, source))?;
        return Ok(InProcessServingOutput {
            report_path: Some(path.clone()),
        });
    }
    let network = decode_zinder_native_chain_name(&args.network)
        .map_err(|error| BenchError::invalid_argument(error.to_string()))?;
    let node_auth = args
        .node_auth_cookie
        .clone()
        .map_or(NodeAuth::None, |path| {
            NodeAuth::Cookie(CookieSource::File(path))
        });
    let source = ZebraJsonRpcSource::with_options(
        network,
        args.json_rpc_addr.clone(),
        node_auth,
        ZebraJsonRpcSourceOptions {
            request_timeout: Duration::from_secs(args.request_timeout_secs),
            max_response_bytes: NonZeroU64::new(256 * 1024 * 1024).unwrap_or(NonZeroU64::MIN),
            broadcast_timeout: None,
        },
    )?;
    let activations = source
        .discover_network_upgrade_activations("zinder-bench-in-process-serving")
        .await?;
    let node_tip = source.tip_id().await?.height.value();
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        run_blocking(args, plan, network, source, activations, node_tip, handle)
    })
    .await
    .map_err(|error| BenchError::invalid_argument(format!("spike task failed: {error}")))?
}

fn copy_store(source: &Path, target: &Path, hardlink: bool) -> Result<(), BenchError> {
    fs::create_dir_all(target).map_err(|error| BenchError::io(target, error))?;
    for entry in fs::read_dir(source).map_err(|error| BenchError::io(source, error))? {
        let entry = entry.map_err(|error| BenchError::io(source, error))?;
        let path = entry.path();
        let metadata = entry
            .metadata()
            .map_err(|error| BenchError::io(&path, error))?;
        let destination = target.join(entry.file_name());
        if metadata.is_dir() {
            copy_store(&path, &destination, hardlink)?;
            continue;
        }
        let is_sst = path.extension().is_some_and(|extension| extension == "sst");
        if hardlink && is_sst && fs::hard_link(&path, &destination).is_ok() {
            continue;
        }
        fs::copy(&path, &destination).map_err(|error| BenchError::io(&path, error))?;
    }
    Ok(())
}

fn parse_raw_blob_retention(encoded: &str) -> Result<RawBlobRetention, BenchError> {
    RawBlobRetention::from_kebab_case(encoded).ok_or_else(|| {
        BenchError::invalid_argument("--raw-blob-retention must be none, transactions, or all")
    })
}

fn ensure_absent(path: &Path, flag: &str) -> Result<(), BenchError> {
    if path.exists() {
        return Err(BenchError::invalid_argument(format!(
            "{flag} must name an absent directory: {}",
            path.display()
        )));
    }
    Ok(())
}

fn canonical_pool(db: &DB, tip: u32) -> Vec<Vec<u8>> {
    let mut pool = Vec::new();
    for family in ["block_replay", "compact_block", "transaction_blob"] {
        let Some(handle) = db.cf_handle(family) else {
            continue;
        };
        for row in db.iterator_cf(&handle, IteratorMode::End).take(1_500) {
            if let Ok((_, value)) = row {
                pool.push(value.to_vec());
            }
        }
    }
    let _ = tip;
    pool
}

fn wallet_pool(db: &DB) -> Vec<Vec<u8>> {
    let mut pool = Vec::new();
    for family in [
        "transparent_unspent_output",
        "transparent_spent_output",
        "transparent_address_transaction",
    ] {
        let Some(handle) = db.cf_handle(family) else {
            continue;
        };
        for row in db.iterator_cf(&handle, IteratorMode::End).take(1_500) {
            if let Ok((key, value)) = row {
                let mut combined = key.to_vec();
                combined.extend_from_slice(&value);
                pool.push(combined);
            }
        }
    }
    pool
}

fn load_samples(canonical_db: &DB, wallet_db: Option<&DB>, tip: u32, first: u32) -> LoadSamples {
    let mut samples = LoadSamples::default();
    if let Some(db) = wallet_db
        && let Some(handle) = db.cf_handle("transparent_address_balance")
    {
        for (index, row) in db.iterator_cf(&handle, IteratorMode::Start).enumerate() {
            let Ok((key, _)) = row else { break };
            if index % 3 == 0 && key.len() == 32 {
                let mut address = [0_u8; 32];
                address.copy_from_slice(&key);
                samples.addresses.push(address);
            }
            if samples.addresses.len() >= 20_000 {
                break;
            }
        }
    }
    let start = tip.saturating_sub(4_999).max(first.max(1));
    let mut height = start;
    while height <= tip && samples.transaction_ids.len() < 20_000 {
        let end = (height + 499).min(tip);
        if let Ok(reads) = spike_compact_blocks(
            canonical_db,
            None,
            BlockHeightRange::inclusive(BlockHeight::new(height), BlockHeight::new(end)),
        ) {
            for read in reads {
                if let SpikeCompactBlockRead::Present(row) = read {
                    samples.transaction_ids.extend(
                        row.compact
                            .transactions()
                            .iter()
                            .map(|tx| tx.transaction_id),
                    );
                }
            }
        }
        height = end + 1;
    }
    samples
}

#[derive(Clone, Default)]
struct StallMark {
    cf: BTreeMap<String, u64>,
    db: BTreeMap<String, u64>,
    stall_micros: u64,
}

fn stall_mark(probe: &DbProbe<'_>) -> StallMark {
    let sample = sample_db(probe, false);
    StallMark {
        cf: cf_stall_counts(probe.db),
        db: sample.db_stall_counts,
        stall_micros: sample.stall_micros,
    }
}

fn stall_delta(start: &StallMark, end: &StallMark) -> StallDelta {
    let diff = |left: &BTreeMap<String, u64>, right: &BTreeMap<String, u64>| {
        right
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    value.saturating_sub(left.get(key).copied().unwrap_or(0)),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    StallDelta {
        cf_counts: diff(&start.cf, &end.cf),
        db_counts: diff(&start.db, &end.db),
        stall_micros: end.stall_micros.saturating_sub(start.stall_micros),
    }
}

fn space_summary(
    samples: &[&SampleRecord],
    pick: impl Fn(&SampleRecord) -> Option<&sampler::DbSample>,
) -> Option<SpaceSummary> {
    let series = samples
        .iter()
        .filter_map(|sample| pick(sample))
        .collect::<Vec<_>>();
    let (first, last) = (series.first()?, series.last()?);
    Some(SpaceSummary {
        live_sst_bytes_start: first.live_sst_bytes,
        live_sst_bytes_max: series.iter().map(|s| s.live_sst_bytes).max().unwrap_or(0),
        live_sst_bytes_end: last.live_sst_bytes,
        disk_sst_bytes_start: first.disk_sst_bytes,
        disk_sst_bytes_max: series.iter().map(|s| s.disk_sst_bytes).max().unwrap_or(0),
        disk_sst_bytes_end: last.disk_sst_bytes,
        obsolete_sst_bytes_max: series
            .iter()
            .filter_map(|s| s.obsolete_sst_bytes)
            .max()
            .unwrap_or(0),
        disk_minus_live_files_bytes_max: series
            .iter()
            .filter_map(|s| {
                s.live_files_bytes
                    .map(|live| s.disk_sst_bytes.saturating_sub(live))
            })
            .max(),
        pending_compaction_bytes_max: series
            .iter()
            .map(|s| s.pending_compaction_bytes)
            .max()
            .unwrap_or(0),
        garbage_estimate_bytes_start: first
            .live_sst_bytes
            .saturating_sub(first.estimate_live_data_bytes),
        garbage_estimate_bytes_max: series
            .iter()
            .map(|s| s.live_sst_bytes.saturating_sub(s.estimate_live_data_bytes))
            .max()
            .unwrap_or(0),
        garbage_estimate_bytes_end: last
            .live_sst_bytes
            .saturating_sub(last.estimate_live_data_bytes),
        max_l0_files: series.iter().map(|s| s.max_l0_files).max().unwrap_or(0),
        num_snapshots_max: series.iter().map(|s| s.num_snapshots).max().unwrap_or(0),
        num_live_versions_max: series
            .iter()
            .map(|s| s.num_live_versions)
            .max()
            .unwrap_or(0),
        write_stopped_samples: series.iter().filter(|s| s.is_write_stopped > 0).count() as u64,
        delayed_write_rate_max: series
            .iter()
            .map(|s| s.actual_delayed_write_rate)
            .max()
            .unwrap_or(0),
        stall_micros_delta: last.stall_micros.saturating_sub(first.stall_micros),
    })
}

fn reader_groups(cycles: &[ReaderCycleRecord]) -> Vec<ReaderGroupSummary> {
    let mut groups: BTreeMap<(String, bool, bool, Option<String>, u64), Vec<&ReaderCycleRecord>> =
        BTreeMap::new();
    for cycle in cycles {
        groups
            .entry((
                cycle.kind.to_owned(),
                cycle.snapshot,
                cycle.held_iterator,
                cycle.family.clone(),
                cycle.hold_target_s.to_bits(),
            ))
            .or_default()
            .push(cycle);
    }
    groups
        .into_iter()
        .map(
            |((kind, snapshot, held_iterator, family, hold_bits), cycles)| {
                let values = |pick: fn(&ReaderCycleRecord) -> f64| {
                    LatencySummary::from_values(
                        &cycles.iter().map(|cycle| pick(cycle)).collect::<Vec<_>>(),
                    )
                };
                ReaderGroupSummary {
                    kind,
                    snapshot,
                    held_iterator,
                    family,
                    hold_target_s: f64::from_bits(hold_bits),
                    cycles: cycles.len(),
                    hold_actual_s_max: cycles
                        .iter()
                        .map(|cycle| cycle.hold_actual_s)
                        .fold(0.0, f64::max),
                    truncated_cycles: cycles.iter().filter(|cycle| cycle.hold_truncated).count(),
                    pass1_rows_per_s: values(|cycle| cycle.pass1_rows_per_s),
                    pass1_mib_per_s: values(|cycle| cycle.pass1_mib_per_s),
                    pass1_seconds: values(|cycle| cycle.pass1_seconds),
                    paced_reads: cycles.iter().map(|cycle| cycle.paced_reads).sum(),
                    violation_count: cycles.iter().map(|cycle| cycle.violation_count).sum(),
                    cycles_with_violations: cycles
                        .iter()
                        .filter(|cycle| cycle.violation_count > 0)
                        .count(),
                    errors: cycles.iter().filter(|cycle| cycle.error.is_some()).count(),
                    first_violations: cycles
                        .iter()
                        .flat_map(|cycle| cycle.violations.iter().cloned())
                        .take(10)
                        .collect(),
                    first_errors: cycles
                        .iter()
                        .filter_map(|cycle| cycle.error.clone())
                        .take(10)
                        .collect(),
                }
            },
        )
        .collect()
}

fn compact_all(db: &DB) -> f64 {
    let started = Instant::now();
    for name in db.cf_names() {
        if let Some(handle) = db.cf_handle(&name) {
            db.compact_range_cf(&handle, None::<&[u8]>, None::<&[u8]>);
        }
    }
    started.elapsed().as_secs_f64()
}

fn latency(values: impl Iterator<Item = f64>) -> LatencySummary {
    LatencySummary::from_values(&values.collect::<Vec<_>>())
}

#[allow(clippy::too_many_arguments)]
fn run_blocking(
    args: InProcessServingArgs,
    plan: Plan,
    network: zinder_core::Network,
    node: ZebraJsonRpcSource,
    activations: Arc<zinder_core::NetworkUpgradeActivations>,
    node_tip: u32,
    runtime: tokio::runtime::Handle,
) -> Result<InProcessServingOutput, BenchError> {
    let run_started = Instant::now();
    ensure_absent(&args.out_dir, "--out-dir")?;
    ensure_absent(&args.work_dir, "--work-dir")?;
    fs::create_dir_all(&args.out_dir).map_err(|error| BenchError::io(&args.out_dir, error))?;
    fs::create_dir_all(&args.work_dir).map_err(|error| BenchError::io(&args.work_dir, error))?;
    let canonical_dir = args.work_dir.join("canonical");
    let wallet_dir = args
        .wallet_source
        .as_ref()
        .map(|_| args.work_dir.join("wallet"));
    let staging_dir = args.work_dir.join("staging");
    fs::create_dir_all(&staging_dir).map_err(|error| BenchError::io(&staging_dir, error))?;

    let hardlink = match args.copy_mode.as_str() {
        "hardlink" => true,
        "copy" => false,
        other => {
            return Err(BenchError::invalid_argument(format!(
                "unknown --copy-mode {other}"
            )));
        }
    };
    let copy_started = Instant::now();
    copy_store(&args.canonical_source, &canonical_dir, hardlink)?;
    if let (Some(source), Some(target)) = (&args.wallet_source, &wallet_dir) {
        copy_store(source, target, hardlink)?;
    }
    let copy_seconds = copy_started.elapsed().as_secs_f64();
    if args.prepare_only {
        tracing::info!(
            target: "zinder::bench::spike",
            canonical = %canonical_dir.display(),
            wallet = ?wallet_dir,
            copy_seconds,
            "copied stores; --prepare-only exits before opening them"
        );
        return Ok(InProcessServingOutput { report_path: None });
    }

    let canonical_budget = RocksDbResourceBudget::canonical_writer_defaults();
    let wallet_budget = RocksDbResourceBudget::wallet_projection_writer_defaults();
    let reorg_policy = CanonicalReorgPolicy::new(args.reorg_window_blocks)?;
    let canonical = RocksDbCanonicalStore::open_ready(
        &canonical_dir,
        &activations,
        CanonicalStoreWorkload::Wallet,
        parse_raw_blob_retention(&args.raw_blob_retention)?,
        reorg_policy,
        canonical_budget,
    )?;
    let canonical_db_arc = canonical.spike_shared_db();
    let canonical_stats = canonical.spike_statistics();
    let canonical_tip_start = canonical.event_fence().visible_tip().height.value();
    let first_retained_height = canonical
        .ready_evidence()
        .first_retained_block
        .height
        .value();
    let canonical_io_mode = format!("{:?}", canonical.io_mode());
    let canonical_cf_options = canonical.spike_data_cf_options(canonical_budget);
    canonical_db_arc
        .create_cf(BALLAST_COLUMN_FAMILY, &canonical_cf_options)
        .map_err(|error| BenchError::invalid_argument(format!("ballast cf: {error}")))?;

    let mut wallet = match &wallet_dir {
        Some(path) => {
            let mut wallet =
                RocksDbWalletStore::open_ready_for_following(path, network, wallet_budget)?;
            if wallet.supported_reorg_depth() < args.reorg_window_blocks {
                wallet.raise_supported_reorg_depth(args.reorg_window_blocks)?;
            }
            Some(wallet)
        }
        None => None,
    };
    let wallet_db_arc = wallet.as_ref().map(|wallet| wallet.spike_shared_db());
    let wallet_stats = wallet.as_ref().map(|wallet| wallet.spike_statistics());
    let wallet_cf_options = wallet.as_ref().map_or_else(Options::default, |wallet| {
        wallet.spike_data_cf_options(wallet_budget)
    });
    if let Some(db) = &wallet_db_arc {
        db.create_cf(BALLAST_COLUMN_FAMILY, &wallet_cf_options)
            .map_err(|error| BenchError::invalid_argument(format!("ballast cf: {error}")))?;
        db.create_cf(MANIFEST_COLUMN_FAMILY, &wallet_cf_options)
            .map_err(|error| BenchError::invalid_argument(format!("manifest cf: {error}")))?;
    }
    let wallet_tip_before_align = wallet
        .as_ref()
        .map(|wallet| wallet.ready_evidence().source_position.tip.height.value());
    if let Some(tip) = wallet_tip_before_align
        && tip > canonical_tip_start
    {
        return Err(BenchError::invalid_argument(format!(
            "wallet tip {tip} is above canonical tip {canonical_tip_start}; copy a wallet at or below the canonical fence"
        )));
    }

    let replay_end = args
        .replay_end_height
        .unwrap_or(node_tip.saturating_sub(20))
        .min(node_tip);
    if replay_end <= canonical_tip_start {
        return Err(BenchError::invalid_argument(format!(
            "no replay range: store tip {canonical_tip_start}, replay end {replay_end}"
        )));
    }
    let required = required_blocks(&plan);
    let available = u64::from(replay_end - canonical_tip_start);
    if required > available && !args.allow_short_replay {
        return Err(BenchError::invalid_argument(format!(
            "plan needs about {required} blocks but only {available} are above the store tip; pass --allow-short-replay or shorten the plan"
        )));
    }

    let canonical_db: &DB = canonical_db_arc.as_ref();
    let wallet_db: Option<&DB> = wallet_db_arc.as_deref();
    let observer = Arc::new(SpikeObserver::new(
        canonical_pool(canonical_db, canonical_tip_start),
        wallet_db.map(wallet_pool).unwrap_or_default(),
    ));
    let samples = load_samples(
        canonical_db,
        wallet_db,
        canonical_tip_start,
        first_retained_height,
    );
    let source = Arc::new(PacedReplaySource::new(
        node,
        Arc::clone(&activations),
        canonical_tip_start,
        replay_end,
    ));
    let prefetcher = runtime.spawn(Arc::clone(&source).run_prefetcher(
        args.prefetch_window,
        args.prefetch_concurrency,
        1_100,
    ));

    let outputs = Outputs::create(&args.out_dir)?;
    let clock = RunClock::new(plan.phases.iter().map(|phase| phase.name.clone()).collect());
    let shared = Shared {
        canonical_db,
        wallet_db,
        canonical_dir: canonical_dir.clone(),
        wallet_dir: wallet_dir.clone(),
        staging_dir: staging_dir.clone(),
        network,
        source: Arc::clone(&source),
        observer: Arc::clone(&observer),
        outputs: &outputs,
        clock: &clock,
        runtime: runtime.clone(),
        cancel: CancellationToken::new(),
        active_readers: AtomicU32::new(0),
        attach_stage: AtomicU32::new(0),
        first_retained_height,
        wallet_attached: AtomicBool::new(wallet.is_some()),
    };
    let mut state = WriterState {
        canonical: Some(canonical),
        wallet: wallet.take(),
        activations: Arc::clone(&activations),
        follow_config: CanonicalFollowConfig {
            request_timeout: Duration::from_secs(args.request_timeout_secs),
            poll_interval: Duration::from_millis(100),
            lag_threshold_blocks: 2,
            target_height: None,
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(3_600),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
        },
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        wallet_max_transition_bytes: NonZeroU64::new(args.wallet_max_transition_bytes)
            .ok_or_else(|| BenchError::invalid_argument("wallet max transition bytes is zero"))?,
    };
    spike_install_batch_observer(Some(observer.clone()));
    let aligned = apply_wallet_events(&mut state, &shared)?;
    let wallet_events_applied_to_align = aligned.map_or(0, |(_, events)| events);
    if let Some(wallet) = &state.wallet {
        let wallet_tip = wallet.ready_evidence().source_position.tip.height.value();
        if wallet_tip != canonical_tip_start {
            return Err(BenchError::invalid_argument(format!(
                "wallet tip {wallet_tip} does not match canonical tip {canonical_tip_start} after alignment"
            )));
        }
    }

    let store_setup = StoreSetup {
        canonical_source: args.canonical_source.display().to_string(),
        wallet_source: args
            .wallet_source
            .as_ref()
            .map(|path| path.display().to_string()),
        work_dir: args.work_dir.display().to_string(),
        copy_mode: args.copy_mode.clone(),
        copy_seconds,
        canonical_bytes_start: directory_bytes(&canonical_dir),
        wallet_bytes_start: wallet_dir.as_deref().map(directory_bytes),
        canonical_tip_start,
        wallet_tip_start: wallet_tip_before_align,
        wallet_events_applied_to_align,
        first_retained_height,
        canonical_io_mode,
        canonical_budget: format!("{canonical_budget:?}"),
        wallet_budget: format!("{wallet_budget:?}"),
        ballast_pool_rows: observer.pool_sizes(),
        sampled_addresses: samples.addresses.len(),
        sampled_transaction_ids: samples.transaction_ids.len(),
    };
    clock.event(
        &outputs,
        "setup_complete",
        serde_json::to_value(&store_setup).unwrap_or_default(),
    );

    let canonical_probe = DbProbe {
        name: "canonical",
        db: canonical_db,
        dir: canonical_dir.clone(),
        statistics: canonical_stats,
    };
    let wallet_probe = match (wallet_db, &wallet_dir, wallet_stats) {
        (Some(db), Some(dir), Some(statistics)) => Some(DbProbe {
            name: "wallet",
            db,
            dir: dir.clone(),
            statistics,
        }),
        _ => None,
    };
    let build_options = RocksDbWalletBuildOptions {
        resource_budget: wallet_budget,
        max_outpoint_sort_memory_bytes: WALLET_OUTPOINT_SORT_MEMORY_BYTES,
        max_secondary_sort_memory_bytes_per_sorter: WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER,
        max_temporary_file_bytes_per_sorter: WALLET_TEMPORARY_FILE_BYTES_PER_SORTER,
        sst_target_logical_bytes: WALLET_SST_TARGET_LOGICAL_BYTES,
        max_accounted_reorg_undo_bytes: WALLET_ACCOUNTED_REORG_UNDO_BYTES,
        supported_reorg_depth: args.reorg_window_blocks,
    };

    let sampler_stop = AtomicBool::new(false);
    let kept_samples = Mutex::new(Vec::<SampleRecord>::new());
    let mut phase_summaries = Vec::new();
    let mut all_cycles = Vec::<ReaderCycleRecord>::new();
    let cycles_sink = Mutex::new(Vec::<ReaderCycleRecord>::new());
    let mut attach_index = 0_u32;

    let run_result: Result<(), BenchError> = std::thread::scope(|scope| {
        scope.spawn(|| {
            run_sampler(
                &shared,
                &canonical_probe,
                wallet_probe.as_ref(),
                Duration::from_millis(args.sample_interval_ms),
                args.live_files_every,
                &sampler_stop,
                &kept_samples,
            );
        });
        let result = (|| -> Result<(), BenchError> {
            for (index, phase) in plan.phases.iter().enumerate() {
                clock.set_phase(index);
                attach_index += 1;
                let summary = run_phase(
                    &shared,
                    &mut state,
                    phase,
                    &canonical_probe,
                    wallet_probe.as_ref(),
                    &samples,
                    &kept_samples,
                    &cycles_sink,
                    &AttachContext {
                        wallet_cf_options: &wallet_cf_options,
                        build_options,
                        attach_index,
                    },
                )?;
                outputs.flush();
                phase_summaries.push(summary);
            }
            Ok(())
        })();
        sampler_stop.store(true, Ordering::SeqCst);
        result
    });
    source.stop();
    prefetcher.abort();
    spike_install_batch_observer(None);
    all_cycles.extend(cycles_sink.lock().drain(..));
    let peak = process_memory();
    outputs.flush();

    let fail_conditions = fail_checks(
        &phase_summaries,
        &all_cycles,
        args.baseline_phase.as_deref(),
    );
    let (node_fetches, cache_hits) = source.counters();
    let report_path = args.out_dir.join("report.json");
    let mut results = BTreeMap::new();
    for name in [
        "commits.jsonl",
        "samples.jsonl",
        "reader-cycles.jsonl",
        "events.jsonl",
        "report.json",
    ] {
        results.insert(
            name_key(name),
            args.out_dir.join(name).display().to_string(),
        );
    }
    let report = InProcessServingReport {
        contract_identity: "spike-in-process-serving",
        network_label: format!("{network:?} (Testnet replay; not Mainnet)"),
        software_revision: args.software_revision.clone(),
        node_json_rpc_addr: args.json_rpc_addr.clone(),
        node_tip_at_start: node_tip,
        replay_start_height: canonical_tip_start + 1,
        replay_end_height: replay_end,
        plan_required_blocks_estimate: required,
        stores: store_setup,
        plan,
        phases: phase_summaries,
        fail_conditions,
        node_fetches,
        source_cache_hits: cache_hits,
        peak_rss_bytes: peak.peak_rss_bytes,
        total_seconds: run_started.elapsed().as_secs_f64(),
        results,
    };
    fs::write(&report_path, serde_json::to_vec_pretty(&report)?)
        .map_err(|error| BenchError::io(&report_path, error))?;
    drop(state);
    run_result?;
    Ok(InProcessServingOutput {
        report_path: Some(report_path),
    })
}

fn name_key(name: &str) -> &'static str {
    match name {
        "commits.jsonl" => "commits",
        "samples.jsonl" => "samples",
        "reader-cycles.jsonl" => "reader_cycles",
        "events.jsonl" => "events",
        _ => "report",
    }
}

#[allow(clippy::too_many_arguments)]
fn run_phase<'db>(
    shared: &Shared<'db>,
    state: &mut WriterState,
    phase: &Phase,
    canonical_probe: &DbProbe<'_>,
    wallet_probe: Option<&DbProbe<'_>>,
    samples: &LoadSamples,
    kept_samples: &Mutex<Vec<SampleRecord>>,
    cycles_sink: &Mutex<Vec<ReaderCycleRecord>>,
    attach_context: &AttachContext<'_>,
) -> Result<PhaseSummary, BenchError> {
    let t_start = shared.clock.t();
    shared.clock.event(
        shared.outputs,
        "phase_started",
        serde_json::json!({ "pacing": phase.pacing_label(), "tip": state.tip_height() }),
    );
    let canonical_stall_start = stall_mark(canonical_probe);
    let wallet_stall_start = wallet_probe.map(stall_mark);
    let stop = AtomicBool::new(false);
    let hold_open = AtomicBool::new(phase.index_attach.is_some());
    let (requests_tx, requests_rx) = mpsc::channel::<WriterRequest<'db>>();
    let latencies = LoadLatencies::default();
    let phase_started = Instant::now();
    let deadline = phase
        .duration_seconds
        .map(|seconds| phase_started + Duration::from_secs(seconds));
    let mut attach_record = None;
    let mut cycles = Vec::new();

    let writer_outcome = std::thread::scope(|scope| {
        let mut reader_handles = Vec::new();
        for (group, spec) in phase.readers.iter().enumerate() {
            match spec {
                ReaderSpec::CompactRange {
                    count,
                    hold_seconds,
                    range_blocks,
                    chunk_blocks,
                    held_iterator,
                    snapshot,
                    paced_interval_ms,
                } => {
                    let params = CompactReaderParams {
                        hold: Duration::from_secs_f64(*hold_seconds),
                        range_blocks: *range_blocks,
                        chunk_blocks: (*chunk_blocks).max(1),
                        held_iterator: *held_iterator,
                        snapshot: *snapshot,
                        paced_interval: Duration::from_millis((*paced_interval_ms).max(1)),
                    };
                    for reader in 0..*count {
                        let stop = &stop;
                        let name = format!("g{group}-compact-{reader}");
                        reader_handles.push(scope.spawn(move || {
                            let mut local = Vec::new();
                            run_reader_loop(shared, name, stop, deadline, params.hold, |name| {
                                let record = compact_range_cycle(shared, name, params, stop);
                                local.push(record.clone());
                                record
                            });
                            local
                        }));
                    }
                }
                ReaderSpec::WalletFamily {
                    count,
                    hold_seconds,
                    family,
                    snapshot,
                    paced_interval_ms,
                } => {
                    let params = WalletReaderParams {
                        hold: Duration::from_secs_f64(*hold_seconds),
                        family: family.clone(),
                        snapshot: *snapshot,
                        paced_interval: Duration::from_millis((*paced_interval_ms).max(1)),
                    };
                    for reader in 0..*count {
                        let stop = &stop;
                        let params = params.clone();
                        let name = format!("g{group}-wallet-{family}-{reader}");
                        reader_handles.push(scope.spawn(move || {
                            let mut local = Vec::new();
                            run_reader_loop(shared, name, stop, deadline, params.hold, |name| {
                                let record = wallet_family_cycle(shared, name, &params, stop);
                                local.push(record.clone());
                                record
                            });
                            local
                        }));
                    }
                }
            }
        }
        if let Some(load) = &phase.load {
            let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
            for (kind, workers) in [
                (LoadKind::Compact, load.compact_workers),
                (LoadKind::Wallet, load.wallet_workers),
                (LoadKind::Lookup, load.lookup_workers),
            ] {
                for _ in 0..workers {
                    seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                    let (stop, latencies) = (&stop, &latencies);
                    scope.spawn(move || {
                        run_load_worker(shared, kind, load, samples, latencies, stop, seed);
                    });
                }
            }
        }
        let attach_handle = phase.index_attach.as_ref().map(|spec| {
            let requests = requests_tx.clone();
            let hold_open = &hold_open;
            scope.spawn(move || {
                let record =
                    run_index_attach(shared, spec, attach_context, &requests, phase_started);
                hold_open.store(false, Ordering::SeqCst);
                record
            })
        });
        drop(requests_tx);
        let writer = run_writer_phase_held(state, shared, phase, &requests_rx, &hold_open);
        // Dropping the receiver fails any request still waiting on the writer.
        drop(requests_rx);
        stop.store(true, Ordering::SeqCst);
        if let Some(handle) = attach_handle {
            attach_record = handle.join().ok();
        }
        for handle in reader_handles {
            if let Ok(local) = handle.join() {
                cycles.extend(local);
            }
        }
        writer
    })?;

    let t_end = shared.clock.t();
    let compaction_at_end = if phase.compact_all_at_end {
        let before = sample_db(canonical_probe, false).live_sst_bytes;
        let canonical_seconds = compact_all(canonical_probe.db);
        let after = sample_db(canonical_probe, false).live_sst_bytes;
        let wallet = wallet_probe.map(|probe| {
            let before = sample_db(probe, false).live_sst_bytes;
            let seconds = compact_all(probe.db);
            (seconds, before, sample_db(probe, false).live_sst_bytes)
        });
        Some(CompactionAtEnd {
            canonical_seconds,
            canonical_live_sst_before: before,
            canonical_live_sst_after: after,
            wallet_seconds: wallet.map(|wallet| wallet.0),
            wallet_live_sst_before: wallet.map(|wallet| wallet.1),
            wallet_live_sst_after: wallet.map(|wallet| wallet.2),
        })
    } else {
        None
    };
    let canonical_stalls = stall_delta(&canonical_stall_start, &stall_mark(canonical_probe));
    let wallet_stalls = match (wallet_probe, &wallet_stall_start) {
        (Some(probe), Some(start)) => Some(stall_delta(start, &stall_mark(probe))),
        _ => None,
    };
    let phase_seconds = phase_started.elapsed().as_secs_f64();

    let kept = kept_samples.lock();
    let window = kept
        .iter()
        .filter(|sample| sample.t >= t_start && sample.t <= t_end)
        .collect::<Vec<_>>();
    let canonical_space =
        space_summary(&window, |sample| Some(&sample.canonical)).unwrap_or_default();
    let wallet_space = space_summary(&window, |sample| sample.wallet.as_ref());
    drop(kept);

    let commits = &writer_outcome.commits;
    let paced = commits
        .iter()
        .filter(|commit| !commit.catch_up)
        .collect::<Vec<_>>();
    let mean = |values: Vec<f64>| {
        if values.is_empty() {
            0.0
        } else {
            values.iter().sum::<f64>() / values.len() as f64
        }
    };
    let mut commits_during_attach_stage = BTreeMap::new();
    for stage in 1..=6_u32 {
        let values = paced
            .iter()
            .filter(|commit| commit.attach_stage == stage)
            .map(|commit| commit.total_commit_s)
            .collect::<Vec<_>>();
        if !values.is_empty() {
            let label = [
                "",
                "sst_build",
                "ingest",
                "catch_up",
                "switch",
                "drop_and_wait_for_files",
                "verify_and_wait_before_drop",
            ][stage as usize];
            commits_during_attach_stage
                .insert(label.to_owned(), LatencySummary::from_values(&values));
        }
    }
    let summary = PhaseSummary {
        name: phase.name.clone(),
        baseline_name: phase.baseline_phase.clone(),
        pacing: phase.pacing_label(),
        block_interval_ms: phase.block_interval_ms,
        density_multiplier: phase.density_multiplier,
        t_start,
        t_end,
        start_tip: writer_outcome.start_tip,
        end_tip: writer_outcome.end_tip,
        commits: commits.len(),
        paced_commits: paced.len(),
        source_exhausted: writer_outcome.source_exhausted,
        canonical_commit_s: latency(paced.iter().map(|commit| commit.canonical_commit_s)),
        canonical_write_s: latency(paced.iter().filter_map(|commit| commit.canonical_write_s)),
        wallet_apply_s: latency(paced.iter().filter_map(|commit| commit.wallet_apply_s)),
        wallet_write_s: latency(paced.iter().filter_map(|commit| commit.wallet_write_s)),
        total_commit_s: latency(paced.iter().map(|commit| commit.total_commit_s)),
        canonical_batch_bytes_mean: mean(
            paced
                .iter()
                .filter_map(|commit| commit.canonical_batch_bytes.map(|bytes| bytes as f64))
                .collect(),
        ),
        canonical_base_bytes_mean: mean(
            paced
                .iter()
                .filter_map(|commit| commit.canonical_base_bytes.map(|bytes| bytes as f64))
                .collect(),
        ),
        wallet_batch_bytes_mean: mean(
            paced
                .iter()
                .filter_map(|commit| commit.wallet_batch_bytes.map(|bytes| bytes as f64))
                .collect(),
        ),
        wallet_base_bytes_mean: mean(
            paced
                .iter()
                .filter_map(|commit| commit.wallet_base_bytes.map(|bytes| bytes as f64))
                .collect(),
        ),
        replacements: writer_outcome.replacements.clone(),
        readers: reader_groups(&cycles),
        load: phase
            .load
            .as_ref()
            .map(|_| latencies.summary(phase_seconds)),
        index_attach: attach_record,
        commits_during_attach_stage,
        canonical_space,
        wallet_space,
        canonical_stalls,
        wallet_stalls,
        compaction_at_end,
        wallet_detached: writer_outcome.wallet_detached.clone(),
        peak_rss_bytes_end: process_memory().peak_rss_bytes,
    };
    shared.clock.event(
        shared.outputs,
        "phase_finished",
        serde_json::json!({
            "commits": summary.commits,
            "end_tip": summary.end_tip,
            "total_commit_p99_s": summary.total_commit_s.p99,
            "snapshot_violations": summary.readers.iter().filter(|group| group.snapshot).map(|group| group.violation_count).sum::<u64>(),
        }),
    );
    cycles_sink.lock().extend(cycles);
    Ok(summary)
}

fn run_writer_phase_held<'db>(
    state: &mut WriterState,
    shared: &Shared<'db>,
    phase: &Phase,
    requests: &mpsc::Receiver<WriterRequest<'db>>,
    hold_open: &AtomicBool,
) -> Result<writer::WriterPhaseOutcome, BenchError> {
    let mut outcome = run_writer_phase(state, shared, phase, requests)?;
    // An index attach still running keeps the writer committing past the deadline.
    while hold_open.load(Ordering::SeqCst) {
        let mut extension = phase.clone();
        extension.duration_seconds = Some(5);
        extension.blocks = None;
        extension.replacements.clear();
        let more = run_writer_phase(state, shared, &extension, requests)?;
        outcome.commits.extend(more.commits);
        outcome.end_tip = more.end_tip;
        if more.source_exhausted {
            outcome.source_exhausted = true;
            while hold_open.load(Ordering::SeqCst) {
                if let Ok(request) = requests.recv_timeout(Duration::from_millis(100)) {
                    writer::handle_request(state, shared, request);
                }
            }
        }
    }
    Ok(outcome)
}

fn fail_checks(
    phases: &[PhaseSummary],
    cycles: &[ReaderCycleRecord],
    baseline_name: Option<&str>,
) -> FailConditionChecks {
    let baseline = baseline_name
        .and_then(|name| phases.iter().find(|phase| phase.name == name))
        .or_else(|| phases.first());
    let (baseline_total, baseline_canonical, name) =
        baseline.map_or((0.0, 0.0, String::new()), |phase| {
            (
                phase.total_commit_s.p99,
                phase.canonical_commit_s.p99,
                phase.name.clone(),
            )
        });
    let mut checks = FailConditionChecks {
        baseline_phase: name,
        baseline_total_commit_p99_s: baseline_total,
        baseline_canonical_commit_p99_s: baseline_canonical,
        note: "each phase's p99 is compared with its own baseline_phase when the plan names one, else with the run baseline; replacement and attach-switch writer blocks are reported separately and are not in the per-commit percentiles",
        ..FailConditionChecks::default()
    };
    for phase in phases {
        let stop_stalls = phase
            .canonical_stalls
            .cf_counts
            .get("total-stops")
            .copied()
            .unwrap_or(0)
            + phase
                .wallet_stalls
                .as_ref()
                .and_then(|stalls| stalls.cf_counts.get("total-stops").copied())
                .unwrap_or(0)
            + phase
                .canonical_stalls
                .db_counts
                .iter()
                .filter(|(key, _)| key.contains("stops"))
                .map(|(_, value)| *value)
                .sum::<u64>()
            + phase
                .wallet_stalls
                .as_ref()
                .map(|stalls| {
                    stalls
                        .db_counts
                        .iter()
                        .filter(|(key, _)| key.contains("stops"))
                        .map(|(_, value)| *value)
                        .sum::<u64>()
                })
                .unwrap_or(0);
        let write_stopped_samples = phase.canonical_space.write_stopped_samples
            + phase
                .wallet_space
                .as_ref()
                .map_or(0, |space| space.write_stopped_samples);
        let ratio = |value: f64, base: f64| (base > 0.0).then(|| value / base);
        let own_baseline = phase
            .baseline_name
            .as_deref()
            .and_then(|name| phases.iter().find(|candidate| candidate.name == name));
        let (phase_baseline_total, phase_baseline_canonical, phase_baseline_name) = own_baseline
            .map_or(
                (
                    baseline_total,
                    baseline_canonical,
                    checks.baseline_phase.clone(),
                ),
                |base| {
                    (
                        base.total_commit_s.p99,
                        base.canonical_commit_s.p99,
                        base.name.clone(),
                    )
                },
            );
        let total_ratio = ratio(phase.total_commit_s.p99, phase_baseline_total);
        let canonical_ratio = ratio(phase.canonical_commit_s.p99, phase_baseline_canonical);
        let snapshot_reader_violations = cycles
            .iter()
            .filter(|cycle| cycle.phase == phase.name && cycle.snapshot)
            .map(|cycle| cycle.violation_count)
            .sum();
        let control_reader_violations = cycles
            .iter()
            .filter(|cycle| cycle.phase == phase.name && !cycle.snapshot)
            .map(|cycle| cycle.violation_count)
            .sum();
        let check = PhaseCheck {
            phase: phase.name.clone(),
            baseline_phase: phase_baseline_name,
            total_commit_p99_s: phase.total_commit_s.p99,
            canonical_commit_p99_s: phase.canonical_commit_s.p99,
            total_p99_ratio_to_baseline: total_ratio,
            canonical_p99_ratio_to_baseline: canonical_ratio,
            p99_over_1s: phase.total_commit_s.p99 > 1.0,
            p99_over_2x_baseline: total_ratio.is_some_and(|ratio| ratio > 2.0),
            stop_stalls,
            write_stopped_samples,
            snapshot_reader_violations,
            control_reader_violations,
            max_replacement_writer_blocked_s: phase
                .replacements
                .iter()
                .map(|replacement| {
                    replacement.canonical_commit_s + replacement.wallet_apply_s.unwrap_or(0.0)
                })
                .reduce(f64::max),
            attach_switch_writer_blocked_s: phase
                .index_attach
                .as_ref()
                .map(|attach| attach.final_switch_writer_blocked_seconds),
        };
        checks.any_stop_stall |= stop_stalls > 0 || write_stopped_samples > 0;
        checks.any_snapshot_reader_torn_view |= snapshot_reader_violations > 0;
        checks.control_readers_detected_changes |= control_reader_violations > 0;
        checks.any_p99_over_1s |= check.p99_over_1s;
        checks.any_p99_over_2x_baseline |= check.p99_over_2x_baseline;
        checks.phases.push(check);
    }
    checks
}
