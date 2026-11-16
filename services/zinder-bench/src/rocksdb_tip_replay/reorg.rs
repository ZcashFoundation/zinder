//! A deep reorg at the tip through production canonical following and the
//! wallet transition `zinder-projector` runs for it.
//!
//! The command replays fixture blocks up to the reorg tip, as a tip replay
//! does, then presents a competing branch that rewrites the newest blocks and
//! extends one block past them. The production canonical follower switches to
//! that branch in one suffix replacement. The wallet then applies the retained
//! reorg event exactly as the projector applies one event, under the
//! configured logical-byte ceiling, and the report records whether the
//! ceiling admitted it, the planner's accounting, and the process memory.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use clap::Args;
use serde::Serialize;
use zinder_bench::{
    BenchError,
    fixture::FixtureManifest,
    rss::{current_rss_bytes, peak_rss, reset_peak_rss},
    tip_replay_checkpoints::{TIP_REPLAY_CHECKPOINTS_FILE_NAME, TipReplayNodeSource},
};
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockId, UnixTimestampMillis,
    wire::encode_zinder_native_chain_name,
};
use zinder_wallet_rocksdb::{RocksDbWalletError, WalletTransitionFootprint};

use super::{
    ProcessClock, ReplayStoreArgs, StoreSettings,
    fork::{ForkKind, ForkRewrite, ForkedFixtureSource},
    measurement::BlockWorkload,
    report::{
        BudgetRecord, FenceRecord, FixtureInput, IoModes, KeptScratch, ProcessResources,
        Provenance, RequestedIoModes, StoreInput, WalletRecord, WalletStatisticsConfiguration,
    },
    scratch::{self, RepeatScratch, ScratchRoot},
    session::{FollowStepRecorder, ForkFollowSamples, ReplayStores, StorePaths},
    statistics::TickerDeltas,
    store_settings, validate_max_transition_logical_bytes,
};

const REPORT_CONTRACT_IDENTITY: &str = "rocksdb-tip-reorg";
const REPORT_FORMAT_VERSION: u32 = 1;
const PROGRESS_INTERVAL_BLOCKS: u32 = 1_000;
/// Interval of the resident-memory sampler that runs during the transition.
const RSS_SAMPLE_INTERVAL: Duration = Duration::from_millis(20);

/// CLI contract for one tip reorg.
#[derive(Args)]
pub(crate) struct RocksDbTipReorgArgs {
    /// Fixture holding the blocks after the store tip through one block past
    /// `--reorg-tip-height`, and their tip replay checkpoints.
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
    /// Newest block the reorg replaces; the fixture is replayed through it first.
    #[arg(long = "reorg-tip-height")]
    reorg_tip_height: u32,
    /// Blocks the reorg replaces, ending at `--reorg-tip-height`.
    #[arg(long, default_value_t = zinder_store::DEFAULT_REORG_WINDOW_BLOCKS)]
    depth: u32,
    /// How the competing branch differs from the blocks it replaces.
    #[arg(long, value_enum, default_value_t = ForkKind::DistinctCoinbases)]
    fork: ForkKind,
    #[command(flatten)]
    stores: ReplayStoreArgs,
    /// Keep the scratch stores instead of deleting them.
    #[arg(long)]
    keep_scratch: bool,
    /// Free-form label copied into the report.
    #[arg(long)]
    label: Option<String>,
}

/// Runs a tip reorg and writes its report.
pub(crate) async fn run_rocksdb_tip_reorg(args: RocksDbTipReorgArgs) -> Result<(), BenchError> {
    tokio::task::spawn_blocking(move || run_tip_reorg(args))
        .await
        .map_err(|source| {
            BenchError::tip_replay(format!("the tip reorg thread stopped: {source}"))
        })?
}

/// Validated inputs of one tip reorg.
struct TipReorgPlan {
    args: RocksDbTipReorgArgs,
    fixture: PathBuf,
    canonical_store: PathBuf,
    wallet_store: PathBuf,
    scratch_dir: PathBuf,
    report: PathBuf,
    manifest: FixtureManifest,
    manifest_sha256: String,
    source: TipReplayNodeSource,
    /// Fixture block identities from the fixture start through the reorg tip.
    replayed_blocks: Vec<BlockId>,
    /// Heights the reorg replaces.
    reverted: BlockHeightRange,
    canonical_store_bytes: u64,
    wallet_store_bytes: u64,
    settings: StoreSettings,
}

/// Drives the follower on this thread's current-thread runtime, so the
/// thread-local recorder sees its production histograms.
fn run_tip_reorg(args: RocksDbTipReorgArgs) -> Result<(), BenchError> {
    let started_at = UnixTimestampMillis::now();
    let process = ProcessClock::start();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| {
            BenchError::tip_replay(format!("could not build the reorg runtime: {source}"))
        })?;
    let plan = TipReorgPlan::admit(args)?;
    let recorder = FollowStepRecorder::default();
    let _recorder_guard = metrics::set_default_local_recorder(&recorder);
    let scratch_root = ScratchRoot::create(&plan.scratch_dir, plan.args.keep_scratch)?;
    let scratch = RepeatScratch::create(&scratch_root, 0, plan.args.keep_scratch)?;
    let measured = run_reorg(&plan, &runtime, &recorder, &scratch)?;
    let kept_scratch = scratch.is_kept().then(|| KeptScratch {
        canonical_store: scratch.canonical_path().display().to_string(),
        wallet_store: scratch.wallet_path().display().to_string(),
    });
    drop(scratch);
    drop(scratch_root);
    let process = process.finish();
    // The probe resets `VmHWM` before the transition, so the whole-process
    // peak is the larger of the peaks before and after that reset.
    let peak = peak_rss();
    let peak_rss_bytes = peak
        .bytes
        .max(measured.wallet_transition.memory.process_peak_before_bytes);
    let report = TipReorgReport {
        contract_identity: REPORT_CONTRACT_IDENTITY.to_owned(),
        report_format_version: REPORT_FORMAT_VERSION,
        provenance: Provenance {
            benchmark_version: env!("CARGO_PKG_VERSION").to_owned(),
            software_revision: plan.args.software_revision.clone(),
            label: plan.args.label.clone(),
            build: super::report::BuildProvenance::of_this_binary(),
            host: super::report::HostProvenance::current(),
            started_at_unix_millis: started_at.value(),
            completed_at_unix_millis: UnixTimestampMillis::now().value(),
        },
        inputs: plan.report_inputs(&measured),
        configuration: plan.report_configuration(measured.effective_reorg_window_blocks),
        catch_up: measured.catch_up,
        fork: measured.fork,
        canonical_replacement: measured.canonical_replacement,
        wallet_transition: measured.wallet_transition,
        io_modes: measured.io_modes,
        kept_scratch,
        process: ProcessResources {
            wall_seconds: process.wall.as_secs_f64(),
            cpu_seconds: process.cpu.as_secs_f64(),
            peak_rss_bytes,
            peak_rss_source: peak.source.to_owned(),
        },
    };
    let encoded = serde_json::to_vec_pretty(&report)?;
    crate::create_report_file(&plan.report, &encoded)?;
    tracing::info!(
        target: "zinder::bench",
        event = "tip_reorg_complete",
        report = %plan.report.display(),
        outcome = report.wallet_transition.outcome.as_str(),
        "tip reorg complete"
    );
    Ok(())
}

impl TipReorgPlan {
    fn admit(args: RocksDbTipReorgArgs) -> Result<Self, BenchError> {
        if args.software_revision.trim().is_empty() {
            return Err(BenchError::invalid_argument(
                "--software-revision must not be empty",
            ));
        }
        validate_max_transition_logical_bytes(&args.stores)?;
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
        let reverted = reverted_range(&args, &manifest)?;
        let source = TipReplayNodeSource::open(&fixture, &manifest)?;
        let replayed_blocks = (manifest.from_height..=args.reorg_tip_height)
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
            .collect::<Result<Vec<_>, _>>()?;
        let canonical_store_bytes = scratch::directory_bytes(&canonical_store)?;
        let wallet_store_bytes = scratch::directory_bytes(&wallet_store)?;
        scratch::require_scratch_space(
            scratch_dir.parent().unwrap_or(&scratch_dir),
            canonical_store_bytes.saturating_add(wallet_store_bytes),
        )?;
        let settings = store_settings(&args.stores, &manifest)?;
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
            replayed_blocks,
            reverted,
            canonical_store_bytes,
            wallet_store_bytes,
            settings,
        })
    }

    fn report_inputs(&self, measured: &MeasuredReorg) -> ReorgInputs {
        ReorgInputs {
            network: encode_zinder_native_chain_name(self.settings.network).to_owned(),
            fixture: FixtureInput {
                path: self.fixture.display().to_string(),
                manifest_sha256: self.manifest_sha256.clone(),
                tip_replay_checkpoints_sha256: self.source.checkpoints_sha256().to_owned(),
                from_height: self.manifest.from_height,
                to_height: self.manifest.to_height,
                block_count: self.manifest.block_count,
                tip_hash_hex: self.manifest.tip_hash_hex.clone(),
                workload_density: self.manifest.workload_density,
            },
            canonical_store: StoreInput {
                path: self.canonical_store.display().to_string(),
                bytes: self.canonical_store_bytes,
                start: measured.start_canonical.clone(),
            },
            wallet_store: StoreInput {
                path: self.wallet_store.display().to_string(),
                bytes: self.wallet_store_bytes,
                start: measured.start_wallet.clone(),
            },
        }
    }

    fn report_configuration(&self, effective_reorg_window_blocks: u32) -> ReorgConfiguration {
        ReorgConfiguration {
            scratch_directory: self.scratch_dir.display().to_string(),
            keep_scratch: self.args.keep_scratch,
            raw_blob_retention: self.settings.raw_blob_retention.as_kebab_case().to_owned(),
            requested_reorg_window_blocks: self.args.stores.reorg_window_blocks,
            effective_reorg_window_blocks,
            max_transition_logical_bytes: self.settings.max_transition_logical_bytes.get(),
            reorg_tip_height: self.args.reorg_tip_height,
            depth: self.args.depth,
            fork: self.args.fork,
            canonical_writer_budget: BudgetRecord::from(self.settings.canonical_writer_budget),
            canonical_secondary_budget: BudgetRecord::from(
                self.settings.canonical_secondary_budget,
            ),
            wallet_budget: BudgetRecord::from(self.settings.wallet_budget),
            requested_io_modes: self.args.stores.requested_io_modes(),
            wallet_statistics: self.args.stores.wallet_statistics(),
            event_retention_pruning: "disabled".to_owned(),
        }
    }
}

/// The heights the reorg replaces, after checking the fixture holds them and
/// the block that extends the branch past them.
fn reverted_range(
    args: &RocksDbTipReorgArgs,
    manifest: &FixtureManifest,
) -> Result<BlockHeightRange, BenchError> {
    let first = args
        .depth
        .checked_sub(1)
        .and_then(|below_tip| args.reorg_tip_height.checked_sub(below_tip))
        .filter(|first| args.depth > 0 && *first > 0)
        .ok_or_else(|| {
            BenchError::invalid_argument("--depth must be between 1 and --reorg-tip-height")
        })?;
    if first < manifest.from_height || args.reorg_tip_height >= manifest.to_height {
        return Err(BenchError::invalid_argument(format!(
            "the fixture holds heights {}..={}; a {}-block reorg at {} needs {first}..={}",
            manifest.from_height,
            manifest.to_height,
            args.depth,
            args.reorg_tip_height,
            args.reorg_tip_height.saturating_add(1)
        )));
    }
    Ok(BlockHeightRange::inclusive(
        BlockHeight::new(first),
        BlockHeight::new(args.reorg_tip_height),
    ))
}

/// Everything one tip reorg measured.
struct MeasuredReorg {
    start_canonical: FenceRecord,
    start_wallet: WalletRecord,
    effective_reorg_window_blocks: u32,
    catch_up: CatchUpReport,
    fork: ForkReport,
    canonical_replacement: CanonicalReplacementReport,
    wallet_transition: WalletTransitionReport,
    io_modes: Option<IoModes>,
}

fn run_reorg(
    plan: &TipReorgPlan,
    runtime: &tokio::runtime::Runtime,
    recorder: &FollowStepRecorder,
    scratch: &RepeatScratch,
) -> Result<MeasuredReorg, BenchError> {
    scratch::copy_store(&plan.canonical_store, &scratch.canonical_path())?;
    scratch::copy_store(&plan.wallet_store, &scratch.wallet_path())?;
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
    if start.fence.visible_tip().height.next() != Some(BlockHeight::new(plan.manifest.from_height))
    {
        return Err(BenchError::invalid_argument(format!(
            "--canonical-store tip {:?} is not the block before the fixture",
            start.fence.visible_tip()
        )));
    }
    let start_wallet = WalletRecord::from(stores.wallet_evidence());
    let effective_reorg_window_blocks = stores.reorg_window_blocks()?;
    let catch_up = catch_up_to_reorg_tip(plan, runtime, recorder, &mut stores)?;
    let (fork, canonical_replacement) = switch_to_fork(plan, runtime, recorder, &mut stores)?;
    let (wallet_transition, io_modes) = apply_wallet_reorg(plan, stores)?;
    Ok(MeasuredReorg {
        start_canonical: FenceRecord::from(start),
        start_wallet,
        effective_reorg_window_blocks,
        catch_up,
        fork,
        canonical_replacement,
        wallet_transition,
        io_modes,
    })
}

/// Builds the competing branch and lets the production follower switch to it.
fn switch_to_fork(
    plan: &TipReorgPlan,
    runtime: &tokio::runtime::Runtime,
    recorder: &FollowStepRecorder,
    stores: &mut ReplayStores,
) -> Result<(ForkReport, CanonicalReplacementReport), BenchError> {
    let before_fork = stores.position()?;
    if before_fork.settled_tip.height >= plan.reverted.start {
        return Err(BenchError::invalid_argument(format!(
            "the reorg starts at {} but the canonical store settled through {}; raise --reorg-window-blocks",
            plan.reverted.start.value(),
            before_fork.settled_tip.height.value()
        )));
    }
    // The follower replaces through the settled tip plus the window. Past the
    // reorg tip, that takes in the branch's extra block, and no append follows.
    let window_blocks = stores.reorg_window_blocks()?;
    let replaceable_through = before_fork
        .settled_tip
        .height
        .value()
        .saturating_add(window_blocks);
    if replaceable_through > plan.reverted.end.value() {
        return Err(BenchError::invalid_argument(format!(
            "the canonical store settled through {} under a {window_blocks}-block reorg window, so the follower would replace the branch's extra block too; raise --reorg-tip-height to at least {replaceable_through} so the window fills first",
            before_fork.settled_tip.height.value()
        )));
    }
    let replaced_workload = total_workload(
        &stores.block_workloads(plan.reverted.start.value()..=plan.reverted.end.value())?,
    );
    let branch_tip = plan
        .reverted
        .end
        .next()
        .ok_or_else(|| BenchError::invalid_argument("--reorg-tip-height is the last height"))?;
    let fork = runtime.block_on(ForkedFixtureSource::build(
        plan.source.clone(),
        plan.args.fork,
        plan.reverted.start,
        branch_tip,
    ))?;
    let rewrite = fork.rewrite();
    tracing::info!(
        target: "zinder::bench",
        event = "tip_reorg_fork_built",
        first_height = plan.reverted.start.value(),
        branch_tip_height = branch_tip.value(),
        rewritten_coinbases = rewrite.coinbases,
        rewritten_spending_transactions = rewrite.spending_transactions,
        "competing branch built"
    );
    let samples = stores.follow_fork(runtime, recorder, fork)?;
    Ok((
        ForkReport {
            kind: plan.args.fork,
            first_height: plan.reverted.start.value(),
            replaced_tip_height: plan.reverted.end.value(),
            branch_tip_height: branch_tip.value(),
            replaced_workload,
            rewrite,
        },
        CanonicalReplacementReport {
            before: FenceRecord::from(before_fork),
            after: FenceRecord::from(stores.position()?),
            samples,
        },
    ))
}

/// Applies the retained reorg as the projector does, under the configured
/// ceiling, and on admission the append that follows it.
fn apply_wallet_reorg(
    plan: &TipReorgPlan,
    mut stores: ReplayStores,
) -> Result<(WalletTransitionReport, Option<IoModes>), BenchError> {
    let reorg = stores.next_retained_reorg(plan.reverted)?;
    let evidence_before = stores.wallet_evidence().clone();
    let tickers_before = stores.wallet_tickers();
    let memory = MemoryProbe::start();
    let transition_started = Instant::now();
    let outcome =
        stores.apply_retained_reorg(&reorg, plan.settings.max_transition_logical_bytes)?;
    let elapsed = transition_started.elapsed();
    let memory = memory.finish();
    let wallet_ticker_deltas = tickers_before
        .zip(stores.wallet_tickers())
        .map(|(before, after)| after.delta_since(&before));
    tracing::info!(
        target: "zinder::bench",
        event = "tip_reorg_wallet_transition",
        admitted = outcome.is_ok(),
        elapsed_seconds = elapsed.as_secs_f64(),
        "wallet reorg transition finished"
    );
    let (report, io_modes) = match outcome {
        Ok(()) => {
            let footprint = stores.wallet_footprint().ok_or_else(|| {
                BenchError::tip_replay("the admitted reorg recorded no footprint")
            })?;
            stores.apply_appended_block()?;
            let finished = stores.finish()?;
            (
                WalletTransitionReport::admitted(
                    plan.settings.max_transition_logical_bytes.get(),
                    elapsed,
                    memory,
                    footprint,
                    WalletRecord::from(&finished.wallet),
                ),
                Some(IoModes::from(&finished.io_modes)),
            )
        }
        Err(RocksDbWalletError::TransitionLogicalByteLimit {
            limit_bytes,
            required_bytes,
        }) => {
            let wallet_unchanged = stores.wallet_evidence() == &evidence_before;
            drop(stores);
            (
                WalletTransitionReport::refused(
                    limit_bytes,
                    required_bytes,
                    elapsed,
                    memory,
                    wallet_unchanged,
                ),
                None,
            )
        }
        Err(error) => return Err(error.into()),
    };
    Ok((
        WalletTransitionReport {
            wallet_ticker_deltas,
            ..report
        },
        io_modes,
    ))
}

/// Replays every fixture block through the reorg tip, as a tip replay does.
fn catch_up_to_reorg_tip(
    plan: &TipReorgPlan,
    runtime: &tokio::runtime::Runtime,
    recorder: &FollowStepRecorder,
    stores: &mut ReplayStores,
) -> Result<CatchUpReport, BenchError> {
    let started = Instant::now();
    for (replayed, expected_tip) in (1_u32..).zip(&plan.replayed_blocks) {
        stores.replay_block(runtime, recorder, *expected_tip)?;
        if replayed % PROGRESS_INTERVAL_BLOCKS == 0 {
            tracing::info!(
                target: "zinder::bench",
                event = "tip_reorg_catch_up_progress",
                height = expected_tip.height.value(),
                replayed_blocks = replayed,
                "tip reorg catch-up progress"
            );
        }
    }
    Ok(CatchUpReport {
        blocks: u32::try_from(plan.replayed_blocks.len()).unwrap_or(u32::MAX),
        wall_seconds: started.elapsed().as_secs_f64(),
    })
}

fn total_workload(blocks: &[(u32, BlockWorkload)]) -> ReplacedWorkload {
    blocks
        .iter()
        .fold(ReplacedWorkload::default(), |mut total, (_, workload)| {
            total.blocks += 1;
            total.transactions += u64::from(workload.transactions);
            total.transparent_inputs += u64::from(workload.transparent_inputs);
            total.transparent_outputs += u64::from(workload.transparent_outputs);
            total
        })
}

/// Resident memory across one span: the size before it, the kernel's peak
/// during it, and a sampled peak as a cross-check.
struct MemoryProbe {
    process_peak_before_bytes: Option<u64>,
    rss_before_bytes: Option<u64>,
    peak_reset: bool,
    stop: Arc<AtomicBool>,
    sampled_peak: Arc<AtomicU64>,
    sampler: Option<thread::JoinHandle<u64>>,
}

/// What a [`MemoryProbe`] observed.
#[derive(Clone, Copy, Debug, Serialize)]
struct MemoryObservation {
    /// `VmHWM` of the whole process before the span.
    process_peak_before_bytes: Option<u64>,
    rss_before_bytes: Option<u64>,
    /// `VmHWM` read after the span. It covers only the span when
    /// `peak_reset` is true, and the whole process otherwise.
    peak_rss_bytes: Option<u64>,
    peak_reset: bool,
    /// Largest `VmRSS` sample taken during the span.
    sampled_peak_rss_bytes: u64,
    sample_interval_ms: u64,
    samples: u64,
    rss_after_bytes: Option<u64>,
}

impl MemoryProbe {
    fn start() -> Self {
        let process_peak_before_bytes = peak_rss().bytes;
        let rss_before_bytes = current_rss_bytes();
        let peak_reset = reset_peak_rss();
        let stop = Arc::new(AtomicBool::new(false));
        let sampled_peak = Arc::new(AtomicU64::new(rss_before_bytes.unwrap_or(0)));
        let sampler = {
            let stop = Arc::clone(&stop);
            let sampled_peak = Arc::clone(&sampled_peak);
            thread::Builder::new()
                .name("rss-sampler".to_owned())
                .spawn(move || {
                    let mut samples = 0_u64;
                    while !stop.load(Ordering::Relaxed) {
                        if let Some(rss) = current_rss_bytes() {
                            sampled_peak.fetch_max(rss, Ordering::Relaxed);
                        }
                        samples += 1;
                        thread::sleep(RSS_SAMPLE_INTERVAL);
                    }
                    samples
                })
                .ok()
        };
        Self {
            process_peak_before_bytes,
            rss_before_bytes,
            peak_reset,
            stop,
            sampled_peak,
            sampler,
        }
    }

    fn finish(self) -> MemoryObservation {
        self.stop.store(true, Ordering::Relaxed);
        let samples = self
            .sampler
            .and_then(|sampler| sampler.join().ok())
            .unwrap_or(0);
        MemoryObservation {
            process_peak_before_bytes: self.process_peak_before_bytes,
            rss_before_bytes: self.rss_before_bytes,
            peak_rss_bytes: peak_rss().bytes,
            peak_reset: self.peak_reset,
            sampled_peak_rss_bytes: self.sampled_peak.load(Ordering::Relaxed),
            sample_interval_ms: u64::try_from(RSS_SAMPLE_INTERVAL.as_millis()).unwrap_or(0),
            samples,
            rss_after_bytes: current_rss_bytes(),
        }
    }
}

/// The JSON report of one tip reorg.
#[derive(Serialize)]
struct TipReorgReport {
    contract_identity: String,
    report_format_version: u32,
    provenance: Provenance,
    inputs: ReorgInputs,
    configuration: ReorgConfiguration,
    catch_up: CatchUpReport,
    fork: ForkReport,
    canonical_replacement: CanonicalReplacementReport,
    wallet_transition: WalletTransitionReport,
    /// I/O modes the stores resolved, recorded when they close normally.
    io_modes: Option<IoModes>,
    kept_scratch: Option<KeptScratch>,
    process: ProcessResources,
}

#[derive(Serialize)]
struct ReorgInputs {
    network: String,
    fixture: FixtureInput,
    canonical_store: StoreInput<FenceRecord>,
    wallet_store: StoreInput<WalletRecord>,
}

#[derive(Serialize)]
struct ReorgConfiguration {
    scratch_directory: String,
    keep_scratch: bool,
    raw_blob_retention: String,
    requested_reorg_window_blocks: u32,
    effective_reorg_window_blocks: u32,
    max_transition_logical_bytes: u64,
    reorg_tip_height: u32,
    depth: u32,
    fork: ForkKind,
    canonical_writer_budget: BudgetRecord,
    canonical_secondary_budget: BudgetRecord,
    wallet_budget: BudgetRecord,
    requested_io_modes: RequestedIoModes,
    wallet_statistics: WalletStatisticsConfiguration,
    event_retention_pruning: String,
}

#[derive(Clone, Copy, Serialize)]
struct CatchUpReport {
    blocks: u32,
    wall_seconds: f64,
}

#[derive(Serialize)]
struct ForkReport {
    kind: ForkKind,
    first_height: u32,
    replaced_tip_height: u32,
    branch_tip_height: u32,
    replaced_workload: ReplacedWorkload,
    rewrite: ForkRewrite,
}

#[derive(Clone, Copy, Default, Serialize)]
struct ReplacedWorkload {
    blocks: u32,
    transactions: u64,
    transparent_inputs: u64,
    transparent_outputs: u64,
}

#[derive(Serialize)]
struct CanonicalReplacementReport {
    before: FenceRecord,
    after: FenceRecord,
    #[serde(flatten)]
    samples: ForkFollowSamples,
}

#[derive(Serialize)]
struct WalletTransitionReport {
    limit_bytes: u64,
    outcome: TransitionOutcome,
    /// The refusal's `required_bytes`: a lower bound on the whole plan.
    refused_required_bytes: Option<u64>,
    /// Whether the READY evidence after a refusal equals the evidence before it.
    wallet_unchanged: Option<bool>,
    footprint: Option<FootprintReport>,
    /// The call from the replay-row scan through the synchronous write or the refusal.
    elapsed_seconds: f64,
    memory: MemoryObservation,
    /// What each wallet statistics ticker counted across the call, when collected.
    wallet_ticker_deltas: Option<TickerDeltas>,
    /// READY evidence after the reorg and the one-block append that follows it.
    final_wallet: Option<WalletRecord>,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "kebab-case")]
enum TransitionOutcome {
    Admitted,
    Refused,
}

impl TransitionOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Refused => "refused",
        }
    }
}

#[derive(Serialize)]
struct FootprintReport {
    accounted_logical_bytes: u64,
    planned_rows: u64,
    batch_records: u64,
    batch_bytes: u64,
    durable_point_reads: u64,
    write_seconds: f64,
    /// `elapsed_seconds` less the write: planning and building the batch.
    plan_seconds: f64,
}

impl WalletTransitionReport {
    fn admitted(
        limit_bytes: u64,
        elapsed: Duration,
        memory: MemoryObservation,
        footprint: WalletTransitionFootprint,
        final_wallet: WalletRecord,
    ) -> Self {
        Self {
            limit_bytes,
            outcome: TransitionOutcome::Admitted,
            refused_required_bytes: None,
            wallet_unchanged: None,
            footprint: Some(FootprintReport {
                accounted_logical_bytes: footprint.accounted_logical_bytes,
                planned_rows: footprint.planned_rows,
                batch_records: footprint.batch_records,
                batch_bytes: footprint.batch_bytes,
                durable_point_reads: footprint.durable_point_reads,
                write_seconds: footprint.write_duration.as_secs_f64(),
                plan_seconds: elapsed
                    .saturating_sub(footprint.write_duration)
                    .as_secs_f64(),
            }),
            elapsed_seconds: elapsed.as_secs_f64(),
            memory,
            wallet_ticker_deltas: None,
            final_wallet: Some(final_wallet),
        }
    }

    const fn refused(
        limit_bytes: u64,
        required_bytes: u64,
        elapsed: Duration,
        memory: MemoryObservation,
        wallet_unchanged: bool,
    ) -> Self {
        Self {
            limit_bytes,
            outcome: TransitionOutcome::Refused,
            refused_required_bytes: Some(required_bytes),
            wallet_unchanged: Some(wallet_unchanged),
            footprint: None,
            elapsed_seconds: elapsed.as_secs_f64(),
            memory,
            wallet_ticker_deltas: None,
            final_wallet: None,
        }
    }
}
