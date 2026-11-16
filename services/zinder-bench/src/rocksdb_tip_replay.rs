//! Tip replay of a captured fixture through production canonical and wallet code.
//!
//! The replay works on scratch copies of a canonical store and a wallet store
//! positioned just before the fixture. Each block runs one production follow iteration (`follow_canonical_tip`
//! stopping at that height) against the fixture, then catches up a canonical
//! secondary and applies the block's retained event to the following wallet
//! with `apply_canonical_event_range`, as `zinder-projector` does. The two
//! stores stay separate databases, as they are in production.

mod compare;
mod measurement;
mod report;
mod scratch;
mod session;
mod statistics;

use std::{
    num::NonZeroU64,
    path::PathBuf,
    time::{Duration, Instant},
};

use clap::Args;
use zinder_bench::{
    BenchError,
    fixture::FixtureManifest,
    rss::peak_rss,
    tip_replay_checkpoints::{TIP_REPLAY_CHECKPOINTS_FILE_NAME, TipReplayNodeSource},
};
use zinder_core::{
    BlockHeight, BlockId, UnixTimestampMillis, wire::encode_zinder_native_chain_name,
};
use zinder_runtime::{
    RocksDbResourceBudgetSection, resolve_canonical_reader_rocksdb_budget,
    resolve_canonical_writer_rocksdb_budget, resolve_wallet_projection_writer_rocksdb_budget,
};
use zinder_source::NodeSource;
use zinder_store::{CanonicalReorgPolicy, RawBlobRetention, RocksDbIoMode, RocksDbResourceBudget};
use zinder_wallet_projection::WalletProjectionReadyEvidence;
use zinder_wallet_rocksdb::MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES;

pub(crate) use compare::{CompareTipReplayReportsArgs, run_compare_tip_replay_reports};
use measurement::{BlockSample, BlockWorkload, PERCENTILE_METHOD, STRATUM_RULES};
use report::{
    BudgetRecord, Configuration, FenceRecord, FixtureInput, Inputs, IoModeConfiguration, IoModes,
    KeptScratch, Method, ProcessResources, Provenance, RepeatReport, RequestedIoModes, StoreInput,
    TipReplayReport, WalletRecord, WalletStatisticsConfiguration,
};
use scratch::{RepeatScratch, ScratchRoot};
use session::{
    CanonicalPosition, FinishedStores, FollowStepRecorder, ReplayStores, ReplayedBlock, StorePaths,
    StoreSettings,
};

/// The projector's default ceiling on one wallet transition's logical bytes.
const DEFAULT_MAX_TRANSITION_LOGICAL_BYTES: u64 = 512 * 1024 * 1024;
/// Retained blocks before the fixture read to find a dense predecessor.
const STRATUM_HISTORY_BLOCKS: u32 = 1_000;
const PROGRESS_INTERVAL_BLOCKS: u32 = 1_000;

/// CLI contract for one tip replay.
#[derive(Args)]
pub(crate) struct RocksDbTipReplayArgs {
    /// Fixture holding the blocks after the store tip and their tip replay checkpoints.
    #[arg(long)]
    fixture: PathBuf,
    /// Closed READY canonical store whose tip is the block before the fixture; never modified.
    #[arg(long = "canonical-store")]
    canonical_store: PathBuf,
    /// Closed READY wallet store at the canonical store's fence; never modified.
    #[arg(long = "wallet-store")]
    wallet_store: PathBuf,
    /// Absent directory for scratch copies of both stores.
    #[arg(long = "scratch-dir")]
    scratch_dir: PathBuf,
    /// Absent path for the JSON report.
    #[arg(long)]
    report: PathBuf,
    /// Source revision of the measured binary, such as `$(git rev-parse HEAD)`.
    #[arg(long = "software-revision")]
    software_revision: String,
    /// Replays to run, each from fresh scratch copies.
    #[arg(long, default_value_t = 1)]
    repeat: u32,
    /// Raw-blob retention persisted in the canonical store.
    #[arg(
        long = "raw-blob-retention",
        default_value = "transactions",
        value_parser = crate::rocksdb_wallet_rebuild::parse_raw_blob_retention
    )]
    raw_blob_retention: RawBlobRetention,
    /// Canonical reorg window passed to the writer, as `zinder-ingest` does.
    #[arg(long, default_value_t = zinder_store::DEFAULT_REORG_WINDOW_BLOCKS)]
    reorg_window_blocks: u32,
    /// Logical-byte ceiling for one wallet transition.
    #[arg(long, default_value_t = DEFAULT_MAX_TRANSITION_LOGICAL_BYTES)]
    max_transition_logical_bytes: u64,
    /// Block cache of the canonical writer (default: the production writer budget).
    #[arg(long)]
    canonical_block_cache_bytes: Option<u64>,
    /// Block cache of the canonical secondary (default: the production reader budget).
    #[arg(long)]
    canonical_secondary_block_cache_bytes: Option<u64>,
    /// Block cache of the wallet store (default: the production wallet writer budget).
    #[arg(long)]
    wallet_block_cache_bytes: Option<u64>,
    /// I/O mode of the canonical writer and its secondary: `direct` tries direct
    /// reads, as production does, and falls back to buffered reads when the
    /// filesystem rejects them; `buffered` skips the direct attempt.
    #[arg(
        long = "canonical-io-mode",
        default_value = "direct",
        value_parser = parse_io_mode
    )]
    canonical_io_mode: RocksDbIoMode,
    /// I/O mode of the wallet store, with the same choices as `--canonical-io-mode`.
    #[arg(
        long = "wallet-io-mode",
        default_value = "direct",
        value_parser = parse_io_mode
    )]
    wallet_io_mode: RocksDbIoMode,
    /// Collect the wallet store's `RocksDB` statistics tickers and report their
    /// per-block deltas. Off by default, which also opens the wallet store with
    /// statistics collection off, because collection can perturb timings.
    #[arg(long = "wallet-rocksdb-statistics")]
    wallet_rocksdb_statistics: bool,
    /// Keep the final repeat's scratch stores instead of deleting them.
    #[arg(long)]
    keep_scratch: bool,
    /// Free-form label copied into the report.
    #[arg(long)]
    label: Option<String>,
}

/// Validated inputs and settings for every repeat.
struct TipReplayPlan {
    args: RocksDbTipReplayArgs,
    fixture: PathBuf,
    canonical_store: PathBuf,
    wallet_store: PathBuf,
    scratch_dir: PathBuf,
    report: PathBuf,
    manifest: FixtureManifest,
    manifest_sha256: String,
    source: TipReplayNodeSource,
    fixture_blocks: Vec<BlockId>,
    /// The block the fixture's first block extends.
    predecessor: BlockId,
    canonical_store_bytes: u64,
    wallet_store_bytes: u64,
    settings: StoreSettings,
}

/// Everything one repeat measured.
struct RepeatOutcome {
    repeat_index: u32,
    scratch_copy: Duration,
    store_open: Duration,
    replay: ProcessSpan,
    start: CanonicalPosition,
    start_wallet: WalletProjectionReadyEvidence,
    effective_reorg_window_blocks: u32,
    finished: FinishedStores,
    blocks: Vec<ReplayedBlock>,
    history: Vec<(u32, BlockWorkload)>,
    replayed: Vec<(u32, BlockWorkload)>,
    peak_rss_bytes: Option<u64>,
    kept_scratch: Option<KeptScratch>,
}

/// Runs a tip replay and writes its report.
pub(crate) async fn run_rocksdb_tip_replay(args: RocksDbTipReplayArgs) -> Result<(), BenchError> {
    tokio::task::spawn_blocking(move || run_tip_replay(args))
        .await
        .map_err(|source| {
            BenchError::tip_replay(format!("the tip replay thread stopped: {source}"))
        })?
}

/// Drives every follow step on this thread's current-thread runtime, so the
/// thread-local recorder sees the follower's production histograms.
fn run_tip_replay(args: RocksDbTipReplayArgs) -> Result<(), BenchError> {
    let started_at = UnixTimestampMillis::now();
    let process = ProcessClock::start();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| {
            BenchError::tip_replay(format!("could not build the replay runtime: {source}"))
        })?;
    let plan = TipReplayPlan::admit(args, &runtime)?;
    let recorder = FollowStepRecorder::default();
    let _recorder_guard = metrics::set_default_local_recorder(&recorder);
    let scratch_root = ScratchRoot::create(&plan.scratch_dir, plan.args.keep_scratch)?;
    let mut repeats = Vec::new();
    for repeat_index in 0..plan.args.repeat {
        let keep = plan.args.keep_scratch && repeat_index + 1 == plan.args.repeat;
        repeats.push(run_repeat(
            &plan,
            &runtime,
            &recorder,
            RepeatScratch::create(&scratch_root, repeat_index, keep)?,
            repeat_index,
        )?);
    }
    drop(scratch_root);
    let process = process.finish();
    let report = build_report(&plan, repeats, process, started_at)?;
    let encoded = serde_json::to_vec_pretty(&report)?;
    crate::create_report_file(&plan.report, &encoded)?;
    tracing::info!(
        target: "zinder::bench",
        event = "tip_replay_complete",
        report = %plan.report.display(),
        repeats = plan.args.repeat,
        block_count = plan.manifest.block_count,
        "tip replay complete"
    );
    Ok(())
}

impl TipReplayPlan {
    fn admit(
        args: RocksDbTipReplayArgs,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<Self, BenchError> {
        validate_scalar_args(&args)?;
        let fixture = scratch::resolve_existing_directory(&args.fixture, "fixture")?;
        let canonical_store =
            scratch::resolve_existing_directory(&args.canonical_store, "canonical-store")?;
        let wallet_store = scratch::resolve_existing_directory(&args.wallet_store, "wallet-store")?;
        let scratch_dir = scratch::resolve_absent_path(&args.scratch_dir, "scratch-dir")?;
        let report = scratch::resolve_absent_path(&args.report, "report")?;
        for (input, flag) in [
            (&fixture, "fixture"),
            (&canonical_store, "canonical-store"),
            (&wallet_store, "wallet-store"),
        ] {
            scratch::require_disjoint((&scratch_dir, "scratch-dir"), (input, flag))?;
            scratch::require_disjoint((&report, "report"), (input, flag))?;
        }
        scratch::require_disjoint((&scratch_dir, "scratch-dir"), (&report, "report"))?;
        scratch::require_disjoint(
            (&canonical_store, "canonical-store"),
            (&wallet_store, "wallet-store"),
        )?;
        let manifest = FixtureManifest::read(&fixture)?;
        let manifest_sha256 = manifest.digest_sha256()?;
        let source = TipReplayNodeSource::open(&fixture, &manifest)?;
        let fixture_blocks = fixture_block_ids(&manifest, &source)?;
        let first_block =
            runtime.block_on(source.fetch_block_at(BlockHeight::new(manifest.from_height)))?;
        let predecessor_height = manifest.from_height.checked_sub(1).ok_or_else(|| {
            BenchError::invalid_argument("a tip replay fixture cannot start at genesis")
        })?;
        let predecessor = BlockId::new(
            BlockHeight::new(predecessor_height),
            first_block.parent_hash,
        );
        let canonical_store_bytes = scratch::directory_bytes(&canonical_store)?;
        let wallet_store_bytes = scratch::directory_bytes(&wallet_store)?;
        let scratch_parent = scratch_dir.parent().unwrap_or(&scratch_dir);
        scratch::require_scratch_space(
            scratch_parent,
            canonical_store_bytes.saturating_add(wallet_store_bytes),
        )?;
        let settings = store_settings(&args, &manifest)?;
        tracing::info!(
            target: "zinder::bench",
            event = "tip_replay_admitted",
            from_height = manifest.from_height,
            to_height = manifest.to_height,
            "tip replay inputs admitted"
        );
        Ok(Self {
            args,
            fixture,
            canonical_store,
            wallet_store,
            scratch_dir,
            report,
            manifest,
            manifest_sha256,
            source,
            fixture_blocks,
            predecessor,
            canonical_store_bytes,
            wallet_store_bytes,
            settings,
        })
    }
}

fn validate_scalar_args(args: &RocksDbTipReplayArgs) -> Result<(), BenchError> {
    if args.software_revision.trim().is_empty() {
        return Err(BenchError::invalid_argument(
            "--software-revision must not be empty",
        ));
    }
    if args.repeat == 0 {
        return Err(BenchError::invalid_argument(
            "--repeat must be greater than zero",
        ));
    }
    if args.max_transition_logical_bytes == 0
        || args.max_transition_logical_bytes > MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES
    {
        return Err(BenchError::invalid_argument(format!(
            "--max-transition-logical-bytes must be between 1 and {MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES}"
        )));
    }
    Ok(())
}

fn store_settings(
    args: &RocksDbTipReplayArgs,
    manifest: &FixtureManifest,
) -> Result<StoreSettings, BenchError> {
    let budget_override =
        |block_cache_bytes, statistics_level: Option<&str>| RocksDbResourceBudgetSection {
            block_cache_bytes,
            statistics_level: statistics_level.map(str::to_owned),
            ..RocksDbResourceBudgetSection::default()
        };
    let invalid_budget =
        |error: zinder_runtime::ConfigError| BenchError::invalid_argument(error.to_string());
    Ok(StoreSettings {
        network: manifest.network_typed()?,
        fixture_activations: manifest.activations_typed()?,
        raw_blob_retention: args.raw_blob_retention,
        reorg_policy: CanonicalReorgPolicy::new(args.reorg_window_blocks)?,
        canonical_writer_budget: RocksDbResourceBudget {
            preferred_io_mode: args.canonical_io_mode,
            ..resolve_canonical_writer_rocksdb_budget(budget_override(
                args.canonical_block_cache_bytes,
                None,
            ))
            .map_err(invalid_budget)?
        },
        canonical_secondary_budget: RocksDbResourceBudget {
            preferred_io_mode: args.canonical_io_mode,
            ..resolve_canonical_reader_rocksdb_budget(budget_override(
                args.canonical_secondary_block_cache_bytes,
                None,
            ))
            .map_err(invalid_budget)?
        },
        wallet_budget: RocksDbResourceBudget {
            preferred_io_mode: args.wallet_io_mode,
            ..resolve_wallet_projection_writer_rocksdb_budget(budget_override(
                args.wallet_block_cache_bytes,
                Some(if args.wallet_rocksdb_statistics {
                    "tickers"
                } else {
                    "off"
                }),
            ))
            .map_err(invalid_budget)?
        },
        collect_wallet_tickers: args.wallet_rocksdb_statistics,
        max_transition_logical_bytes: NonZeroU64::new(args.max_transition_logical_bytes)
            .ok_or_else(|| {
                BenchError::invalid_argument("--max-transition-logical-bytes must be non-zero")
            })?,
    })
}

fn parse_io_mode(text: &str) -> Result<RocksDbIoMode, String> {
    [RocksDbIoMode::Direct, RocksDbIoMode::Buffered]
        .into_iter()
        .find(|mode| mode.as_str() == text)
        .ok_or_else(|| format!("expected `direct` or `buffered`, found `{text}`"))
}

fn fixture_block_ids(
    manifest: &FixtureManifest,
    source: &TipReplayNodeSource,
) -> Result<Vec<BlockId>, BenchError> {
    (manifest.from_height..=manifest.to_height)
        .map(|height| {
            source
                .checkpoint_at(BlockHeight::new(height))
                .map(|checkpoint| checkpoint.block_id)
                .ok_or_else(|| {
                    BenchError::fixture_format(format!(
                        "{TIP_REPLAY_CHECKPOINTS_FILE_NAME} has no checkpoint at height {height}"
                    ))
                })
        })
        .collect()
}

fn run_repeat(
    plan: &TipReplayPlan,
    runtime: &tokio::runtime::Runtime,
    recorder: &FollowStepRecorder,
    scratch: RepeatScratch,
    repeat_index: u32,
) -> Result<RepeatOutcome, BenchError> {
    let copy_started = Instant::now();
    scratch::copy_store(&plan.canonical_store, &scratch.canonical_path())?;
    scratch::copy_store(&plan.wallet_store, &scratch.wallet_path())?;
    let scratch_copy = copy_started.elapsed();

    let open_started = Instant::now();
    let canonical_path = scratch.canonical_path();
    let secondary_path = scratch.secondary_path();
    let wallet_path = scratch.wallet_path();
    let mut stores = ReplayStores::open(
        &StorePaths {
            canonical: &canonical_path,
            canonical_secondary: &secondary_path,
            wallet: &wallet_path,
        },
        &plan.settings,
        &plan.source,
    )?;
    let start = stores.start_position()?;
    require_fixture_extends(plan, start)?;
    let start_wallet = stores.wallet_evidence().clone();
    let effective_reorg_window_blocks = stores.reorg_window_blocks()?;
    let store_open = open_started.elapsed();

    let replay_clock = ProcessClock::start();
    let mut blocks = Vec::with_capacity(plan.fixture_blocks.len());
    for (replayed, expected_tip) in (1_u32..).zip(&plan.fixture_blocks) {
        blocks.push(stores.replay_block(runtime, recorder, *expected_tip)?);
        if replayed % PROGRESS_INTERVAL_BLOCKS == 0 {
            tracing::info!(
                target: "zinder::bench",
                event = "tip_replay_progress",
                repeat_index,
                height = expected_tip.height.value(),
                replayed_blocks = replayed,
                "tip replay progress"
            );
        }
    }
    let replay = replay_clock.finish();

    let from_height = plan.manifest.from_height;
    let history = stores.block_workloads(
        from_height.saturating_sub(STRATUM_HISTORY_BLOCKS)..=from_height.saturating_sub(1),
    )?;
    let replayed = stores.block_workloads(from_height..=plan.manifest.to_height)?;
    let finished = stores.finish()?;
    let kept_scratch = scratch.is_kept().then(|| KeptScratch {
        canonical_store: canonical_path.display().to_string(),
        wallet_store: wallet_path.display().to_string(),
    });
    drop(scratch);
    Ok(RepeatOutcome {
        repeat_index,
        scratch_copy,
        store_open,
        replay,
        start,
        start_wallet,
        effective_reorg_window_blocks,
        finished,
        blocks,
        history,
        replayed,
        peak_rss_bytes: peak_rss().bytes,
        kept_scratch,
    })
}

fn require_fixture_extends(
    plan: &TipReplayPlan,
    start: CanonicalPosition,
) -> Result<(), BenchError> {
    let tip = start.fence.visible_tip();
    if tip != plan.predecessor {
        return Err(BenchError::invalid_argument(format!(
            "--canonical-store tip {tip:?} is not the parent {:?} of the first fixture block",
            plan.predecessor
        )));
    }
    Ok(())
}

/// Wall time and whole-process CPU time across one span.
struct ProcessClock {
    wall_started: Instant,
    cpu_started: Duration,
}

/// The measured wall and CPU time of one span.
#[derive(Clone, Copy)]
struct ProcessSpan {
    wall: Duration,
    cpu: Duration,
}

impl ProcessClock {
    fn start() -> Self {
        Self {
            wall_started: Instant::now(),
            cpu_started: process_cpu_time(),
        }
    }

    fn finish(self) -> ProcessSpan {
        ProcessSpan {
            wall: self.wall_started.elapsed(),
            cpu: process_cpu_time().saturating_sub(self.cpu_started),
        }
    }
}

/// CPU time consumed by every thread of this process.
fn process_cpu_time() -> Duration {
    let cpu = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    Duration::new(
        u64::try_from(cpu.tv_sec).unwrap_or(0),
        u32::try_from(cpu.tv_nsec).unwrap_or(0),
    )
}

fn build_report(
    plan: &TipReplayPlan,
    outcomes: Vec<RepeatOutcome>,
    process: ProcessSpan,
    started_at: UnixTimestampMillis,
) -> Result<TipReplayReport, BenchError> {
    let first = outcomes
        .first()
        .ok_or_else(|| BenchError::tip_replay("the replay ran no repeats"))?;
    require_identical_outcomes(first, &outcomes, plan)?;
    let strata = measurement::classify_strata(&first.history, &first.replayed);
    let inputs = report_inputs(plan, first);
    let configuration = report_configuration(plan, first);
    let repeats = outcomes
        .into_iter()
        .map(|outcome| repeat_report(outcome, &strata))
        .collect::<Vec<_>>();
    let pooled: Vec<&BlockSample> = repeats
        .iter()
        .flat_map(|repeat| repeat.blocks.iter())
        .collect();
    let aggregate = measurement::TimerReport::from_samples(&pooled);
    let peak = peak_rss();
    Ok(TipReplayReport {
        contract_identity: report::REPORT_CONTRACT_IDENTITY.to_owned(),
        report_format_version: report::REPORT_FORMAT_VERSION,
        provenance: Provenance {
            benchmark_version: env!("CARGO_PKG_VERSION").to_owned(),
            software_revision: plan.args.software_revision.clone(),
            label: plan.args.label.clone(),
            build: report::BuildProvenance::of_this_binary(),
            host: report::HostProvenance::current(),
            started_at_unix_millis: started_at.value(),
            completed_at_unix_millis: UnixTimestampMillis::now().value(),
        },
        inputs,
        configuration,
        method: Method {
            percentile_method: PERCENTILE_METHOD.to_owned(),
            stratum_rules: STRATUM_RULES,
        },
        process: ProcessResources {
            wall_seconds: process.wall.as_secs_f64(),
            cpu_seconds: process.cpu.as_secs_f64(),
            peak_rss_bytes: peak.bytes,
            peak_rss_source: peak.source.to_owned(),
        },
        aggregate,
        unavailable_measurements: report::unavailable_measurements(
            plan.args.wallet_rocksdb_statistics,
        ),
        repeats,
    })
}

fn require_identical_outcomes(
    first: &RepeatOutcome,
    outcomes: &[RepeatOutcome],
    plan: &TipReplayPlan,
) -> Result<(), BenchError> {
    if first.replayed.len() != plan.fixture_blocks.len() {
        return Err(BenchError::tip_replay(format!(
            "the replayed canonical store retains {} of {} fixture blocks",
            first.replayed.len(),
            plan.fixture_blocks.len()
        )));
    }
    for outcome in outcomes {
        if outcome.start != first.start
            || outcome.finished.canonical != first.finished.canonical
            || outcome.finished.wallet != first.finished.wallet
            || outcome.replayed != first.replayed
        {
            return Err(BenchError::tip_replay(format!(
                "repeat {} ended in a different canonical or wallet state than repeat 0",
                outcome.repeat_index
            )));
        }
    }
    Ok(())
}

fn report_inputs(plan: &TipReplayPlan, first: &RepeatOutcome) -> Inputs {
    Inputs {
        network: encode_zinder_native_chain_name(plan.settings.network).to_owned(),
        fixture: FixtureInput {
            path: plan.fixture.display().to_string(),
            manifest_sha256: plan.manifest_sha256.clone(),
            tip_replay_checkpoints_sha256: plan.source.checkpoints_sha256().to_owned(),
            from_height: plan.manifest.from_height,
            to_height: plan.manifest.to_height,
            block_count: plan.manifest.block_count,
            tip_hash_hex: plan.manifest.tip_hash_hex.clone(),
            workload_density: plan.manifest.workload_density,
        },
        canonical_store: StoreInput {
            path: plan.canonical_store.display().to_string(),
            bytes: plan.canonical_store_bytes,
            start: FenceRecord::from(first.start),
        },
        wallet_store: StoreInput {
            path: plan.wallet_store.display().to_string(),
            bytes: plan.wallet_store_bytes,
            start: WalletRecord::from(&first.start_wallet),
        },
    }
}

fn report_configuration(plan: &TipReplayPlan, first: &RepeatOutcome) -> Configuration {
    Configuration {
        repeat_count: plan.args.repeat,
        scratch_directory: plan.scratch_dir.display().to_string(),
        keep_scratch: plan.args.keep_scratch,
        raw_blob_retention: plan.settings.raw_blob_retention.as_kebab_case().to_owned(),
        requested_reorg_window_blocks: plan.args.reorg_window_blocks,
        effective_reorg_window_blocks: first.effective_reorg_window_blocks,
        max_transition_logical_bytes: plan.settings.max_transition_logical_bytes.get(),
        canonical_writer_budget: BudgetRecord::from(plan.settings.canonical_writer_budget),
        canonical_secondary_budget: BudgetRecord::from(plan.settings.canonical_secondary_budget),
        wallet_budget: BudgetRecord::from(plan.settings.wallet_budget),
        rocksdb_io_mode: IoModeConfiguration {
            policy: "direct opens with direct I/O and falls back to buffered I/O when direct I/O cannot open; buffered skips the direct attempt".to_owned(),
            requested: RequestedIoModes {
                canonical: plan.args.canonical_io_mode.as_str().to_owned(),
                wallet: plan.args.wallet_io_mode.as_str().to_owned(),
            },
            resolved: IoModes::from(&first.finished.io_modes),
        },
        wallet_statistics: WalletStatisticsConfiguration {
            collected: plan.args.wallet_rocksdb_statistics,
            tickers: if plan.args.wallet_rocksdb_statistics {
                statistics::reported_ticker_names()
            } else {
                Vec::new()
            },
        },
        event_retention_pruning: "disabled".to_owned(),
    }
}

fn repeat_report(outcome: RepeatOutcome, strata: &[Vec<measurement::Stratum>]) -> RepeatReport {
    let blocks: Vec<BlockSample> = outcome
        .replayed
        .iter()
        .zip(&outcome.blocks)
        .zip(strata)
        .map(|(((height, workload), replayed), strata)| BlockSample {
            height: *height,
            workload: *workload,
            strata: strata.clone(),
            timings_ms: replayed.timings,
            wallet_ticker_deltas: replayed.wallet_tickers.clone(),
        })
        .collect();
    let replay_wall_seconds = outcome.replay.wall.as_secs_f64();
    let block_count = u32::try_from(blocks.len()).unwrap_or(u32::MAX);
    RepeatReport {
        repeat_index: outcome.repeat_index,
        scratch_copy_seconds: outcome.scratch_copy.as_secs_f64(),
        store_open_seconds: outcome.store_open.as_secs_f64(),
        replay_wall_seconds,
        replay_process_cpu_seconds: outcome.replay.cpu.as_secs_f64(),
        blocks_per_second: if replay_wall_seconds > 0.0 {
            f64::from(block_count) / replay_wall_seconds
        } else {
            0.0
        },
        peak_rss_bytes_at_end: outcome.peak_rss_bytes,
        io_modes: IoModes::from(&outcome.finished.io_modes),
        final_canonical: FenceRecord::from(outcome.finished.canonical),
        final_wallet: WalletRecord::from(&outcome.finished.wallet),
        kept_scratch: outcome.kept_scratch,
        timers: measurement::TimerReport::from_samples(&blocks.iter().collect::<Vec<_>>()),
        blocks,
    }
}
