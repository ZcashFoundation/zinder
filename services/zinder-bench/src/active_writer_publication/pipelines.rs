//! Concurrent canonical writer, wallet projector, and 2-generation secondary
//! catch-up pipelines using level-triggered `watch` channels and RAII cancellation.

use std::{
    collections::BTreeMap,
    fs,
    num::{NonZeroU32, NonZeroU64},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use tempfile::TempDir;
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use zinder_core::{BlockHeight, BlockId, NetworkUpgradeActivations};
use zinder_ingest::{
    CanonicalFollowConfig, CanonicalFollower, DEFAULT_MAX_REPLACEMENT_BYTES,
    DEFAULT_TIP_FOLLOW_LAG_THRESHOLD_BLOCKS, follow_canonical_tip,
};
use zinder_runtime::{ComposedReadiness, Readiness};
use zinder_store::{
    CanonicalEventHistoryRequest, CanonicalEventKind, CanonicalReorgPolicy,
    CanonicalStoreRequirements, RawBlobRetention, RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    RocksDbIoMode,
};
use zinder_wallet_projection::WalletCanonicalSourceIdentity;
use zinder_wallet_rocksdb::{
    MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES, RocksDbWalletError,
    RocksDbWalletFollowingStore, RocksDbWalletStore,
};

use super::{
    ActiveWriterPublicationConfig, WriterMode,
    chain_fixture::{
        AdvancingChainSource, GeneratedChain, TemplateStores, create_scratch_tempdir,
        reader_budget, writer_budget,
    },
    mempool::BenchmarkMempoolSurface,
    report::{
        IngestStageMetrics, LatencySummaryMs, NodeRpcCounts, ProjectorMetrics, StoreTickerDelta,
        TickerCounterSnapshot,
    },
    serving::{
        TwoGenCanonicalPublisher, TwoGenWalletPairPublisher, capture_canonical_sec_tickers,
        catch_up_canonical_secondary,
    },
};
use crate::BenchError;

const LIVE_COMMIT_HISTOGRAM: &str = "zinder_ingest_canonical_live_commit_seconds";
const LIVE_PREPARE_HISTOGRAM: &str = "zinder_ingest_canonical_live_prepare_seconds";
const MAX_MISSING_TABLE_RETRIES: usize = 8;

/// Level-triggered writer progress shared with projector and readers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct WriterProgress {
    pub(crate) last_committed_height: u32,
    pub(crate) commits_completed: u32,
    pub(crate) commits_done: bool,
}

/// Level-triggered state of the gRPC-exposed serving generation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ServedState {
    pub(crate) tip_height: u32,
    pub(crate) epoch_id: u64,
    pub(crate) generation_seq: u64,
    pub(crate) serving_caught_up_done: bool,
}

/// Abstracts the 2-generation publisher held by either `ingest_active` or wallet modes.
#[derive(Clone)]
pub(crate) enum ModeServingHandle {
    CanonicalOnly(Arc<TwoGenCanonicalPublisher>),
    WalletPair(Arc<TwoGenWalletPairPublisher>),
}

impl ModeServingHandle {
    async fn try_catch_up_and_publish(
        &self,
    ) -> Result<(u32, zinder_core::ChainEpoch, u64), BenchError> {
        match self {
            Self::CanonicalOnly(pub_ref) => pub_ref.try_catch_up_and_publish().await,
            Self::WalletPair(pub_ref) => pub_ref.try_catch_up_and_publish().await,
        }
    }

    pub(crate) fn request_rotation(&self) {
        match self {
            Self::CanonicalOnly(pub_ref) => pub_ref.request_rotation(),
            Self::WalletPair(pub_ref) => pub_ref.request_rotation(),
        }
    }

    pub(crate) fn has_rotation_request(&self) -> bool {
        match self {
            Self::CanonicalOnly(pub_ref) => pub_ref.has_rotation_request(),
            Self::WalletPair(pub_ref) => pub_ref.has_rotation_request(),
        }
    }

    pub(crate) async fn force_rotate_to_latest(&self) -> Result<(), BenchError> {
        match self {
            Self::CanonicalOnly(pub_ref) => pub_ref.force_rotate_to_latest().await,
            Self::WalletPair(pub_ref) => pub_ref.force_rotate_to_latest().await,
        }
    }

    pub(crate) async fn rotate_generation_for_cold_tail(
        &self,
        cancel: &CancellationToken,
    ) -> Result<(u32, zinder_core::ChainEpoch, u64), BenchError> {
        match self {
            Self::CanonicalOnly(pub_ref) => pub_ref.rotate_generation_for_cold_tail(cancel).await,
            Self::WalletPair(pub_ref) => pub_ref.rotate_generation_for_cold_tail(cancel).await,
        }
    }

    pub(crate) fn snapshot_and_reset_tickers(&self) -> (StoreTickerDelta, StoreTickerDelta) {
        match self {
            Self::CanonicalOnly(pub_ref) => (
                pub_ref.snapshot_and_reset_tickers(),
                StoreTickerDelta::default(),
            ),
            Self::WalletPair(pub_ref) => pub_ref.snapshot_and_reset_tickers(),
        }
    }

    pub(crate) fn io_modes(&self) -> (Vec<String>, Vec<String>) {
        match self {
            Self::CanonicalOnly(pub_ref) => (pub_ref.io_modes.clone(), Vec::new()),
            Self::WalletPair(pub_ref) => (
                pub_ref.canonical_io_modes.clone(),
                pub_ref.wallet_io_modes.clone(),
            ),
        }
    }

    pub(crate) async fn wait_reclaim_all(&self) {
        match self {
            Self::CanonicalOnly(pub_ref) => pub_ref.wait_reclaim_all().await,
            Self::WalletPair(pub_ref) => pub_ref.wait_reclaim_all().await,
        }
    }
}

fn is_wallet_replay_missing_table(err: &RocksDbWalletError) -> bool {
    err.is_missing_table_file()
        || matches!(err, RocksDbWalletError::CanonicalReplay { source } if source.is_missing_table_file())
}

/// On-disk scratch directories for one mode execution.
pub(crate) struct ModePaths {
    pub(crate) _root: TempDir,
    pub(crate) canonical_dir: PathBuf,
    pub(crate) wallet_dir: PathBuf,
    pub(crate) canonical_sec_dirs: Vec<PathBuf>,
    pub(crate) wallet_sec_dirs: Vec<PathBuf>,
    pub(crate) projector_sec_dir: PathBuf,
}

impl ModePaths {
    pub(crate) fn prepare(
        template: &TemplateStores,
        scratch_dir: Option<&Path>,
        label: &str,
    ) -> Result<Self, BenchError> {
        let root = create_scratch_tempdir(scratch_dir, label)?;
        let canonical_dir = root.path().join("canonical");
        let wallet_dir = root.path().join("wallet");

        template.clone_to(&canonical_dir, &wallet_dir)?;

        let mut canonical_sec_dirs = Vec::with_capacity(4);
        let mut wallet_sec_dirs = Vec::with_capacity(4);
        for index in 0..4 {
            let can_sec = root.path().join(format!("reader-can-sec-{index}"));
            let wal_sec = root.path().join(format!("reader-wal-sec-{index}"));
            fs::create_dir_all(&can_sec).map_err(|source| BenchError::io(&can_sec, source))?;
            fs::create_dir_all(&wal_sec).map_err(|source| BenchError::io(&wal_sec, source))?;
            canonical_sec_dirs.push(can_sec);
            wallet_sec_dirs.push(wal_sec);
        }
        let projector_sec_dir = root.path().join("projector-sec");
        fs::create_dir_all(&projector_sec_dir)
            .map_err(|source| BenchError::io(&projector_sec_dir, source))?;

        Ok(Self {
            _root: root,
            canonical_dir,
            wallet_dir,
            canonical_sec_dirs,
            wallet_sec_dirs,
            projector_sec_dir,
        })
    }
}

/// Controls optional wallet replay during quiescent baseline setup.
#[derive(Clone, Copy)]
pub(crate) struct QuiescentPreAdvance {
    pub(crate) advance_wallet: bool,
}

/// Pre-advances closed canonical and optional wallet stores for quiescent controls.
pub(crate) async fn pre_advance_quiescent_stores(
    paths: &ModePaths,
    config: &ActiveWriterPublicationConfig,
    chain: &GeneratedChain,
    opts: QuiescentPreAdvance,
) -> Result<(), BenchError> {
    let reorg_policy = CanonicalReorgPolicy::new(chain.reorg_window_blocks)?;
    let source = AdvancingChainSource::new(chain, chain.final_tip_height);
    let canonical = RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
        &paths.canonical_dir,
        &chain.activations,
        RawBlobRetention::All,
        reorg_policy,
        writer_budget(config.io_mode),
    )?;
    let activations = Arc::new(
        canonical
            .network_upgrade_activations()
            .cloned()
            .ok_or_else(|| BenchError::tip_replay("canonical missing activations"))?,
    );
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();
    let follower = CanonicalFollower::new(
        &source,
        activations,
        follow_config_for(chain.final_tip_height),
        &readiness,
        &cancel,
    );
    let canonical = follow_canonical_tip(canonical, follower).await?;
    drop(canonical);

    if !opts.advance_wallet {
        return Ok(());
    }

    let mut secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        &paths.canonical_dir,
        &paths.projector_sec_dir,
        chain.network,
        CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All),
        reorg_policy,
        reader_budget(config.io_mode),
    )?;
    advance_quiescent_wallet(paths, config, chain, reorg_policy, &mut secondary)?;
    drop(secondary);
    Ok(())
}

fn advance_quiescent_wallet(
    paths: &ModePaths,
    config: &ActiveWriterPublicationConfig,
    chain: &GeneratedChain,
    reorg_policy: CanonicalReorgPolicy,
    secondary: &mut RocksDbCanonicalSecondary,
) -> Result<(), BenchError> {
    let mut wallet = RocksDbWalletStore::open_ready_for_following(
        &paths.wallet_dir,
        chain.network,
        writer_budget(config.io_mode),
    )?;
    wallet.raise_supported_reorg_depth(reorg_policy.reorg_window_blocks())?;

    let mut unused_durations = Vec::new();
    let mut unused_transitions = 0_u32;
    while drain_projector_events(
        secondary,
        &mut wallet,
        &mut unused_durations,
        &mut unused_transitions,
    )? > 0
    {}
    let wallet_height = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence())
        .source_position()
        .tip
        .height;
    if wallet_height < chain.final_tip_height {
        return Err(BenchError::tip_replay(format!(
            "quiescent wallet pre-advance stopped at {} < {}",
            wallet_height.value(),
            chain.final_tip_height.value()
        )));
    }
    drop(wallet);
    Ok(())
}

/// Active pipeline telemetry aggregated upon clean completion.
#[derive(Default)]
pub(crate) struct ActiveOutcome {
    pub(crate) canonical_io_mode: Option<String>,
    pub(crate) wallet_io_mode: Option<String>,
    pub(crate) projector_secondary_io_mode: Option<String>,
    pub(crate) ingest: IngestStageMetrics,
    pub(crate) projector: ProjectorMetrics,
    pub(crate) canonical_delta: StoreTickerDelta,
    pub(crate) wallet_delta: StoreTickerDelta,
    pub(crate) projector_secondary_delta: StoreTickerDelta,
    pub(crate) node_rpcs: NodeRpcCounts,
}

/// Shared inputs handed to `drive_active_pipelines`.
pub(crate) struct ActivePipelineArgs<'a> {
    pub(crate) mode: WriterMode,
    pub(crate) config: &'a ActiveWriterPublicationConfig,
    pub(crate) chain: &'a GeneratedChain,
    pub(crate) paths: &'a ModePaths,
    pub(crate) serving: ModeServingHandle,
    pub(crate) mempool: &'a Arc<BenchmarkMempoolSurface>,
    pub(crate) tip_watcher_tx: &'a watch::Sender<u64>,
    pub(crate) source: AdvancingChainSource,
    pub(crate) writer_progress_tx: watch::Sender<WriterProgress>,
    pub(crate) served_tx: watch::Sender<ServedState>,
    pub(crate) commit_instants: Arc<Mutex<BTreeMap<u32, Instant>>>,
    pub(crate) cancel: CancellationToken,
}

/// Drives the active canonical writer, optional wallet projector, and 2-generation serving catch-up.
pub(crate) async fn drive_active_pipelines(
    args: ActivePipelineArgs<'_>,
) -> Result<ActiveOutcome, BenchError> {
    let (canonical_store, activations, canonical_io_mode) = open_active_canonical_store(&args)?;
    let (projector_tip_tx, projector_tip_rx) = watch::channel(args.chain.base_tip_height.value());
    let writer_rx = args.writer_progress_tx.subscribe();
    let has_projector = args.mode.has_wallet_serving();

    let catchup_task = spawn_serving_catchup_loop(CatchupLoopInputs {
        serving: args.serving.clone(),
        has_projector,
        writer_rx: writer_rx.clone(),
        projector_tip_rx,
        mempool: Arc::clone(args.mempool),
        tip_watcher_tx: args.tip_watcher_tx.clone(),
        served_tx: args.served_tx.clone(),
        final_tip_height: args.chain.final_tip_height.value(),
        cancel: args.cancel.clone(),
    });
    let projector_handle = has_projector
        .then(|| {
            spawn_projector_loop(ProjectorLoopInputs {
                paths: args.paths,
                chain: args.chain,
                io_mode: args.config.io_mode,
                writer_rx,
                projector_tip_tx,
                cancel: args.cancel.clone(),
            })
        })
        .transpose()?;

    let writer_res = spawn_writer_thread(WriterThreadArgs {
        store: canonical_store,
        source: args.source.clone(),
        activations,
        start_height: args.chain.base_tip_height.value().saturating_add(1),
        end_height: args.chain.final_tip_height.value(),
        commit_interval: args.config.commit_interval,
        writer_progress_tx: args.writer_progress_tx.clone(),
        commit_instants: Arc::clone(&args.commit_instants),
        cancel: args.cancel.clone(),
    })
    .await;

    args.writer_progress_tx.send_modify(|prog| {
        prog.commits_done = true;
    });
    let writer_output = match writer_res {
        Ok(output) => output,
        Err(err) => {
            abort_active_handles(&args.cancel, catchup_task, projector_handle).await;
            return Err(err);
        }
    };

    finalize_pipelines(
        PipelineHandles {
            canonical_io_mode,
            catchup_task,
            projector_handle,
            served_tx: args.served_tx,
            cancel: args.cancel,
        },
        writer_output,
        args.source.rpc_counts(),
    )
    .await
}

fn open_active_canonical_store(
    args: &ActivePipelineArgs<'_>,
) -> Result<
    (
        RocksDbCanonicalStore,
        Arc<zinder_core::NetworkUpgradeActivations>,
        String,
    ),
    BenchError,
> {
    let reorg_policy = CanonicalReorgPolicy::new(args.chain.reorg_window_blocks)?;
    let store = RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
        &args.paths.canonical_dir,
        &args.chain.activations,
        RawBlobRetention::All,
        reorg_policy,
        writer_budget(args.config.io_mode),
    )?;
    let io_mode = store.io_mode().as_str().to_owned();
    let activations = Arc::new(
        store
            .network_upgrade_activations()
            .cloned()
            .ok_or_else(|| BenchError::tip_replay("canonical store missing upgrade activations"))?,
    );
    Ok((store, activations, io_mode))
}

async fn abort_active_handles(
    cancel: &CancellationToken,
    catchup_task: JoinHandle<Result<(), BenchError>>,
    projector_handle: Option<JoinHandle<Result<ProjectorOutput, BenchError>>>,
) {
    cancel.cancel();
    let _ = catchup_task.await;
    if let Some(handle) = projector_handle {
        let _ = handle.await;
    }
}

struct PipelineHandles {
    canonical_io_mode: String,
    catchup_task: JoinHandle<Result<(), BenchError>>,
    projector_handle: Option<JoinHandle<Result<ProjectorOutput, BenchError>>>,
    served_tx: watch::Sender<ServedState>,
    cancel: CancellationToken,
}

async fn finalize_pipelines(
    handles: PipelineHandles,
    writer_output: WriterOutput,
    node_rpcs: NodeRpcCounts,
) -> Result<ActiveOutcome, BenchError> {
    let projector_out = match handles.projector_handle {
        Some(handle) => Some(
            handle
                .await
                .map_err(|err| BenchError::tip_replay(err.to_string()))??,
        ),
        None => None,
    };
    handles
        .catchup_task
        .await
        .map_err(|err| BenchError::tip_replay(err.to_string()))??;
    handles.served_tx.send_modify(|state| {
        state.serving_caught_up_done = true;
    });
    let _ = handles.cancel;

    let mut outcome = ActiveOutcome {
        canonical_io_mode: Some(handles.canonical_io_mode),
        ingest: writer_output.ingest,
        canonical_delta: writer_output.delta,
        node_rpcs,
        ..ActiveOutcome::default()
    };
    if let Some(proj) = projector_out {
        outcome.wallet_io_mode = Some(proj.wallet_io_mode);
        outcome.projector_secondary_io_mode = Some(proj.secondary_io_mode);
        outcome.projector = proj.metrics;
        outcome.wallet_delta = proj.wallet_delta;
        outcome.projector_secondary_delta = proj.secondary_delta;
    }
    Ok(outcome)
}

struct WriterThreadArgs {
    store: RocksDbCanonicalStore,
    source: AdvancingChainSource,
    activations: Arc<NetworkUpgradeActivations>,
    start_height: u32,
    end_height: u32,
    commit_interval: Duration,
    writer_progress_tx: watch::Sender<WriterProgress>,
    commit_instants: Arc<Mutex<BTreeMap<u32, Instant>>>,
    cancel: CancellationToken,
}

struct WriterOutput {
    ingest: IngestStageMetrics,
    delta: StoreTickerDelta,
}

async fn spawn_writer_thread(args: WriterThreadArgs) -> Result<WriterOutput, BenchError> {
    let (done_tx, done_rx) = oneshot::channel::<Result<WriterOutput, BenchError>>();
    std::thread::Builder::new()
        .name("zinder-bench-writer".to_owned())
        .spawn(move || {
            let outcome = execute_writer_loop(args);
            let _ = done_tx.send(outcome);
        })
        .map_err(|source| BenchError::io("writer-thread", source))?;
    done_rx
        .await
        .map_err(|_| BenchError::tip_replay("writer thread terminated unexpectedly"))?
}

fn execute_writer_loop(args: WriterThreadArgs) -> Result<WriterOutput, BenchError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| BenchError::tip_replay(source.to_string()))?;
    let recorder = FollowStepRecorder::new(Arc::clone(&args.commit_instants));
    let _guard = metrics::set_default_local_recorder(&recorder);
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);

    let ticker_before =
        TickerCounterSnapshot::capture(|ticker| args.store.rocksdb_ticker_count(ticker));
    let mut active_store = args.store;
    let mut prepare_ms = Vec::new();
    let mut commit_ms = Vec::new();

    for raw_height in args.start_height..=args.end_height {
        if !args.commit_interval.is_zero() {
            let cancelled = runtime.block_on(async {
                tokio::select! {
                    biased;
                    () = args.cancel.cancelled() => true,
                    () = tokio::time::sleep(args.commit_interval) => false,
                }
            });
            if cancelled {
                return Err(BenchError::tip_replay("writer cancelled during interval"));
            }
        }
        if args.cancel.is_cancelled() {
            return Err(BenchError::tip_replay("writer cancelled"));
        }
        #[cfg(test)]
        args.source.check_injected_failure(raw_height)?;
        let target = BlockHeight::new(raw_height);
        args.source.advance_tip_to(target);
        recorder.begin_height(raw_height);

        let follower = CanonicalFollower::new(
            &args.source,
            Arc::clone(&args.activations),
            follow_config_for(target),
            &readiness,
            &args.cancel,
        );
        active_store = runtime.block_on(follow_canonical_tip(active_store, follower))?;
        let step = recorder.take_step()?;
        prepare_ms.push(step.prepare_secs * 1_000.0);
        commit_ms.push(step.commit_secs * 1_000.0);
        args.writer_progress_tx.send_modify(|prog| {
            prog.last_committed_height = raw_height;
            prog.commits_completed = prog.commits_completed.saturating_add(1);
        });
    }

    let delta = TickerCounterSnapshot::capture(|ticker| active_store.rocksdb_ticker_count(ticker))
        .delta_since(&ticker_before);
    drop(active_store);
    Ok(WriterOutput {
        ingest: IngestStageMetrics {
            live_prepare_ms: LatencySummaryMs::from_f64(&prepare_ms),
            live_commit_ms: LatencySummaryMs::from_f64(&commit_ms),
            blocks_committed: args
                .end_height
                .saturating_sub(args.start_height)
                .saturating_add(1),
        },
        delta,
    })
}

struct ProjectorOutput {
    wallet_io_mode: String,
    secondary_io_mode: String,
    metrics: ProjectorMetrics,
    wallet_delta: StoreTickerDelta,
    secondary_delta: StoreTickerDelta,
}

struct ProjectorLoopInputs<'a> {
    paths: &'a ModePaths,
    chain: &'a GeneratedChain,
    io_mode: RocksDbIoMode,
    writer_rx: watch::Receiver<WriterProgress>,
    projector_tip_tx: watch::Sender<u32>,
    cancel: CancellationToken,
}

fn spawn_projector_loop(
    inputs: ProjectorLoopInputs<'_>,
) -> Result<JoinHandle<Result<ProjectorOutput, BenchError>>, BenchError> {
    let reorg_policy = CanonicalReorgPolicy::new(inputs.chain.reorg_window_blocks)?;
    let secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        &inputs.paths.canonical_dir,
        &inputs.paths.projector_sec_dir,
        inputs.chain.network,
        CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All),
        reorg_policy,
        reader_budget(inputs.io_mode),
    )?;
    let secondary_io_mode = secondary.io_mode().as_str().to_owned();
    let mut wallet = RocksDbWalletStore::open_ready_for_following(
        &inputs.paths.wallet_dir,
        inputs.chain.network,
        writer_budget(inputs.io_mode),
    )?;
    wallet.raise_supported_reorg_depth(reorg_policy.reorg_window_blocks())?;
    let final_height = inputs.chain.final_tip_height;

    Ok(tokio::spawn(drive_projector_task(
        (secondary, wallet),
        secondary_io_mode,
        final_height,
        inputs.writer_rx,
        (inputs.projector_tip_tx, inputs.cancel),
    )))
}

async fn watch_changed_while_open<T: Send + Sync>(rx: &mut watch::Receiver<T>, open: &mut bool) {
    if !*open {
        std::future::pending::<()>().await;
        return;
    }
    if rx.changed().await.is_err() {
        *open = false;
    }
}

async fn drive_projector_task(
    (secondary, wallet): (RocksDbCanonicalSecondary, RocksDbWalletFollowingStore),
    secondary_io_mode: String,
    final_height: BlockHeight,
    mut writer_rx: watch::Receiver<WriterProgress>,
    (projector_tip_tx, cancel): (watch::Sender<u32>, CancellationToken),
) -> Result<ProjectorOutput, BenchError> {
    let wal_before = TickerCounterSnapshot::capture(|ticker| wallet.rocksdb_ticker_count(ticker));
    let sec_before = capture_canonical_sec_tickers(&secondary);
    let mut handles = Some((secondary, wallet));
    let mut apply_durations = Vec::new();
    let mut transitions = 0_u32;
    let mut writer_open = true;

    loop {
        let writer_snapshot = *writer_rx.borrow_and_update();
        if cancel.is_cancelled() {
            break;
        }
        let (mut sec, mut wal) = handles
            .take()
            .ok_or_else(|| BenchError::tip_replay("projector handles missing"))?;
        let (sec, wal, drained, step_durs, step_trans, wallet_tip) =
            tokio::task::spawn_blocking(move || {
                let mut durs = Vec::new();
                let mut trans = 0_u32;
                let drained = drain_projector_events(&mut sec, &mut wal, &mut durs, &mut trans)?;
                let wallet_tip =
                    WalletCanonicalSourceIdentity::from_ready_evidence(wal.ready_evidence())
                        .source_position()
                        .tip
                        .height;
                Ok::<_, BenchError>((sec, wal, drained, durs, trans, wallet_tip))
            })
            .await
            .map_err(|err| BenchError::tip_replay(err.to_string()))??;
        handles = Some((sec, wal));
        apply_durations.extend(step_durs);
        transitions = transitions.saturating_add(step_trans);
        let _ = projector_tip_tx.send_if_modified(|cur| {
            if wallet_tip.value() > *cur {
                *cur = wallet_tip.value();
                true
            } else {
                false
            }
        });

        if writer_snapshot.commits_done && drained == 0 && wallet_tip >= final_height {
            break;
        }
        if drained == 0 {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                () = watch_changed_while_open(&mut writer_rx, &mut writer_open) => {}
                () = tokio::time::sleep(Duration::from_millis(2)) => {}
            }
        }
    }

    finalize_projector_handles(
        handles,
        cancel,
        secondary_io_mode,
        (&sec_before, &wal_before),
        (&apply_durations, transitions),
    )
    .await
}

async fn finalize_projector_handles(
    handles: Option<(RocksDbCanonicalSecondary, RocksDbWalletFollowingStore)>,
    cancel: CancellationToken,
    secondary_io_mode: String,
    (sec_before, wal_before): (&TickerCounterSnapshot, &TickerCounterSnapshot),
    (apply_durations, transitions): (&[Duration], u32),
) -> Result<ProjectorOutput, BenchError> {
    let (secondary, wallet) =
        handles.ok_or_else(|| BenchError::tip_replay("projector handles missing on finalize"))?;
    if cancel.is_cancelled() {
        let _ = tokio::task::spawn_blocking(move || {
            drop(wallet);
            drop(secondary);
        })
        .await;
        return Err(BenchError::tip_replay(
            "projector cancelled after upstream pipeline abort",
        ));
    }
    let sec_before = sec_before.clone();
    let wal_before = wal_before.clone();
    let (wallet_io_mode, wallet_delta, secondary_delta) =
        tokio::task::spawn_blocking(move || -> Result<_, BenchError> {
            let wallet_delta =
                TickerCounterSnapshot::capture(|ticker| wallet.rocksdb_ticker_count(ticker))
                    .delta_since(&wal_before);
            let secondary_delta =
                capture_canonical_sec_tickers(&secondary).delta_since(&sec_before);
            let final_identity =
                WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
            let ready_wallet = wallet.into_ready_store(final_identity)?;
            let wallet_io = ready_wallet.io_mode().as_str().to_owned();
            drop(ready_wallet);
            drop(secondary);
            Ok((wallet_io, wallet_delta, secondary_delta))
        })
        .await
        .map_err(|err| BenchError::tip_replay(err.to_string()))??;

    Ok(ProjectorOutput {
        wallet_io_mode,
        secondary_io_mode,
        metrics: ProjectorMetrics {
            apply_ms: LatencySummaryMs::from_durations(apply_durations),
            transitions_applied: transitions,
        },
        wallet_delta,
        secondary_delta,
    })
}

fn drain_projector_events(
    secondary: &mut RocksDbCanonicalSecondary,
    wallet: &mut RocksDbWalletFollowingStore,
    durations: &mut Vec<Duration>,
    transitions: &mut u32,
) -> Result<usize, BenchError> {
    let mut total_applied = 0_usize;
    for attempt in 0..=MAX_MISSING_TABLE_RETRIES {
        let (applied, hit_missing_table) =
            try_drain_projector_batch(secondary, wallet, durations, transitions)?;
        total_applied = total_applied.saturating_add(applied);
        if !hit_missing_table || attempt == MAX_MISSING_TABLE_RETRIES {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    Ok(total_applied)
}

fn try_drain_projector_batch(
    secondary: &mut RocksDbCanonicalSecondary,
    wallet: &mut RocksDbWalletFollowingStore,
    durations: &mut Vec<Duration>,
    transitions: &mut u32,
) -> Result<(usize, bool), BenchError> {
    if let Err(err) = catch_up_canonical_secondary(secondary) {
        if err.is_missing_table_file() {
            return Ok((0, true));
        }
        return Err(err.into());
    }
    let wallet_source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
    let cursor = wallet_source.source_position().event_cursor.as_bytes();
    let batch_limit = NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN);
    let events = match secondary.canonical_event_history(CanonicalEventHistoryRequest::new(
        Some(&cursor),
        batch_limit,
    )) {
        Ok(events) => events,
        Err(err) if err.is_missing_table_file() => return Ok((0, true)),
        Err(err) => return Err(err.into()),
    };
    let mut applied = 0_usize;
    for event in events {
        if !matches!(event.kind(), CanonicalEventKind::Committed) {
            continue;
        }
        let committed = event.committed_range();
        let fence = event.resulting_fence();
        let epoch = match secondary.chain_epoch_at(event.resulting_epoch_id()) {
            Ok(epoch) => epoch,
            Err(err) if err.is_missing_table_file() => return Ok((applied, true)),
            Err(err) => return Err(err.into()),
        };
        let settled_tip = BlockId::new(epoch.settled_tip_height, epoch.settled_tip_hash);
        let current_source =
            WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        let started = Instant::now();
        let replay_rows = match secondary.scan_canonical_replay_range(committed) {
            Ok(rows) => rows,
            Err(err) if err.is_missing_table_file() => return Ok((applied, true)),
            Err(err) => return Err(err.into()),
        };
        let max_bytes = NonZeroU64::new(MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES)
            .unwrap_or(NonZeroU64::MIN);
        match wallet.apply_canonical_event_range(
            current_source,
            event,
            fence,
            settled_tip,
            max_bytes,
            replay_rows,
        ) {
            Ok(()) => {}
            Err(ref err) if is_wallet_replay_missing_table(err) => return Ok((applied, true)),
            Err(err) => return Err(err.into()),
        }
        durations.push(started.elapsed());
        *transitions = transitions.saturating_add(1);
        applied = applied.saturating_add(1);
    }
    Ok((applied, false))
}

struct CatchupLoopInputs {
    serving: ModeServingHandle,
    has_projector: bool,
    writer_rx: watch::Receiver<WriterProgress>,
    projector_tip_rx: watch::Receiver<u32>,
    mempool: Arc<BenchmarkMempoolSurface>,
    tip_watcher_tx: watch::Sender<u64>,
    served_tx: watch::Sender<ServedState>,
    final_tip_height: u32,
    cancel: CancellationToken,
}

fn spawn_serving_catchup_loop(mut inputs: CatchupLoopInputs) -> JoinHandle<Result<(), BenchError>> {
    tokio::spawn(async move {
        let mut last_tip = 0_u32;
        let mut last_epoch_id = 0_u64;
        let mut writer_open = true;
        let mut projector_open = inputs.has_projector;
        loop {
            let writer_snap = *inputs.writer_rx.borrow_and_update();
            let proj_tip = *inputs.projector_tip_rx.borrow_and_update();
            if inputs.cancel.is_cancelled() {
                break;
            }

            let (tip, epoch, gen_seq) = inputs.serving.try_catch_up_and_publish().await?;
            let epoch_id = epoch.id.value();
            if tip > last_tip || epoch_id > last_epoch_id {
                last_tip = last_tip.max(tip);
                last_epoch_id = last_epoch_id.max(epoch_id);
                inputs.mempool.advance_epoch(epoch);
                inputs.tip_watcher_tx.send_replace(epoch_id);
                inputs.served_tx.send_modify(|state| {
                    state.tip_height = state.tip_height.max(tip);
                    state.epoch_id = epoch_id;
                    state.generation_seq = gen_seq;
                });
            }

            let target_reached = tip >= inputs.final_tip_height
                && (!inputs.has_projector || proj_tip >= inputs.final_tip_height);
            if writer_snap.commits_done && target_reached && !inputs.serving.has_rotation_request()
            {
                break;
            }

            tokio::select! {
                biased;
                () = inputs.cancel.cancelled() => break,
                () = watch_changed_while_open(&mut inputs.writer_rx, &mut writer_open) => {}
                () = watch_changed_while_open(&mut inputs.projector_tip_rx, &mut projector_open) => {}
                () = tokio::time::sleep(Duration::from_millis(2)) => {}
            }
        }
        if !inputs.cancel.is_cancelled() && last_tip < inputs.final_tip_height {
            return Err(BenchError::tip_replay(format!(
                "serving catch-up stopped at tip {last_tip} below final height {}",
                inputs.final_tip_height
            )));
        }
        Ok(())
    })
}

fn follow_config_for(target_height: BlockHeight) -> CanonicalFollowConfig {
    CanonicalFollowConfig {
        request_timeout: Duration::from_secs(10),
        poll_interval: Duration::ZERO,
        lag_threshold_blocks: DEFAULT_TIP_FOLLOW_LAG_THRESHOLD_BLOCKS,
        target_height: Some(target_height),
        event_retention_window: None,
        event_retention_check_interval: Duration::from_secs(60),
        mempool_ready_gate: None,
        max_replacement_bytes: DEFAULT_MAX_REPLACEMENT_BYTES,
        node_failover: None,
    }
}

#[derive(Default)]
struct StepHistograms {
    prepares: Vec<f64>,
    commits: Vec<f64>,
}

struct StepSamples {
    prepare_secs: f64,
    commit_secs: f64,
}

struct FollowStepRecorder {
    samples: Arc<Mutex<StepHistograms>>,
    active_height: Arc<Mutex<Option<u32>>>,
    commit_instants: Arc<Mutex<BTreeMap<u32, Instant>>>,
}

impl FollowStepRecorder {
    fn new(commit_instants: Arc<Mutex<BTreeMap<u32, Instant>>>) -> Self {
        Self {
            samples: Arc::new(Mutex::new(StepHistograms::default())),
            active_height: Arc::new(Mutex::new(None)),
            commit_instants,
        }
    }

    fn begin_height(&self, height: u32) {
        *self.samples.lock() = StepHistograms::default();
        *self.active_height.lock() = Some(height);
    }

    fn take_step(&self) -> Result<StepSamples, BenchError> {
        let taken = std::mem::take(&mut *self.samples.lock());
        match (taken.prepares.as_slice(), taken.commits.as_slice()) {
            ([prepare], [commit]) => Ok(StepSamples {
                prepare_secs: *prepare,
                commit_secs: *commit,
            }),
            _ => Err(BenchError::tip_replay(format!(
                "expected 1 prepare and 1 commit sample per step, got {} prepare / {} commit",
                taken.prepares.len(),
                taken.commits.len()
            ))),
        }
    }
}

#[derive(Clone, Copy)]
enum StepKind {
    Prepare,
    Commit,
}

struct StepHistogramHandle {
    kind: StepKind,
    samples: Arc<Mutex<StepHistograms>>,
    active_height: Arc<Mutex<Option<u32>>>,
    commit_instants: Arc<Mutex<BTreeMap<u32, Instant>>>,
}

impl metrics::HistogramFn for StepHistogramHandle {
    fn record(&self, reading: f64) {
        if matches!(self.kind, StepKind::Commit)
            && let Some(height) = *self.active_height.lock()
        {
            self.commit_instants.lock().insert(height, Instant::now());
        }
        let mut guard = self.samples.lock();
        match self.kind {
            StepKind::Prepare => guard.prepares.push(reading),
            StepKind::Commit => guard.commits.push(reading),
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
        let kind = match key.name() {
            LIVE_PREPARE_HISTOGRAM => StepKind::Prepare,
            LIVE_COMMIT_HISTOGRAM => StepKind::Commit,
            _ => return metrics::Histogram::noop(),
        };
        metrics::Histogram::from_arc(Arc::new(StepHistogramHandle {
            kind,
            samples: Arc::clone(&self.samples),
            active_height: Arc::clone(&self.active_height),
            commit_instants: Arc::clone(&self.commit_instants),
        }))
    }
}
