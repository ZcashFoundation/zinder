//! Orchestrator for the 4 matched `active-writer-publication` modes.
//!
//! Coordinates the live canonical follower, secondary reader generations,
//! wallet projector, shielded history publisher, and compatibility gRPC server.

use std::{
    borrow::Borrow,
    fs,
    net::SocketAddr,
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use tempfile::TempDir;
use tokio::{
    net::TcpListener,
    sync::{Notify, oneshot, watch},
    task::JoinHandle,
};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use zinder_compat_lightwalletd::{
    CanonicalReaderSlot, LightwalletdGrpcAdapter, PublicationConcurrency, PublishedShieldedHistory,
    ShieldedCatalogPublisher, TipWait,
};
use zinder_core::{BlockHeight, BlockId, ChainEpoch, Network, NetworkUpgradeActivations};
use zinder_ingest::{
    CanonicalFollowConfig, CanonicalFollower, DEFAULT_MAX_REPLACEMENT_BYTES,
    DEFAULT_TIP_FOLLOW_LAG_THRESHOLD_BLOCKS, follow_canonical_tip,
};
use zinder_query::{CanonicalReader, ForwardedReadLimit, RocksDbCanonicalReader};
use zinder_runtime::{
    AuthenticatedChannel, ComposedReadiness, Readiness, connect_zinder_grpc_with_timeout,
};
use zinder_source::{NodeAuth, ZebraJsonRpcSource};
use zinder_store::{
    CanonicalEventHistoryRequest, CanonicalEventKind, CanonicalReorgPolicy,
    CanonicalStoreRequirements, CanonicalStoreWorkload, RawBlobRetention,
    RocksDbCanonicalSecondary, RocksDbCanonicalStore, RocksDbResourceBudget,
    RocksDbStatisticsLevel, SharedSecondaryResources,
};
use zinder_wallet_projection::WalletCanonicalSourceIdentity;
use zinder_wallet_rocksdb::{
    MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES, RocksDbWalletFollowingStore, RocksDbWalletStore,
};

use crate::BenchError;

use super::{
    ActiveWriterPublicationConfig,
    chain_fixture::{AdvancingChainSource, ChainSpecConfig, GeneratedChain, TemplateStores},
    mempool::{BenchmarkMempoolSurface, as_mempool_surface, create_tip_watcher},
    readers::{
        FleetSignals, ReaderFleetOutput, ReaderFleetSpec, collect_final_tip_parity,
        spawn_reader_fleet,
    },
    report::{
        ACTIVE_WRITER_PUBLICATION_CONTRACT, ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION,
        ActiveWriterPublicationReport, ByteParityReport, IngestStageMetrics, IoModeReport,
        LatencySummaryMs, ModeDifferential, ModeParityDigests, ModeReport, ProjectorMetrics,
        PublisherMetrics, RocksDbActivityReport, StoreTickerDelta, TickerCounterSnapshot,
        WorkloadGeometryReport, WriterFsyncCounts, WriterMode,
    },
};

struct CapturedCanonical(Arc<RocksDbCanonicalReader>);

impl Borrow<dyn CanonicalReader> for CapturedCanonical {
    fn borrow(&self) -> &(dyn CanonicalReader + 'static) {
        self.0.as_ref()
    }
}

const LIVE_COMMIT_HISTOGRAM: &str = "zinder_ingest_canonical_live_commit_seconds";
const LIVE_PREPARE_HISTOGRAM: &str = "zinder_ingest_canonical_live_prepare_seconds";
const DEFAULT_MAX_TRANSITION_BYTES: u64 = MAX_WALLET_PROJECTION_TRANSITION_LOGICAL_BYTES;
const READER_GENERATION_SLOTS: usize = 3;

/// Runs all four matched writer/publication modes against one shared advancing
/// Regtest workload and returns the structured comparison and parity report.
pub async fn run_active_writer_publication(
    config: &ActiveWriterPublicationConfig,
) -> Result<ActiveWriterPublicationReport, BenchError> {
    validate_config(config)?;
    let chain = GeneratedChain::build(ChainSpecConfig {
        base_blocks: config.base_blocks,
        live_blocks: config.live_blocks,
        reorg_window_blocks: config.reorg_window_blocks,
        dense_block_cadence: config.dense_block_cadence,
        dense_txs_per_block: config.dense_txs_per_block,
    })?;
    let template_stores = TemplateStores::build(&chain).await?;

    let mut mode_reports = Vec::with_capacity(4);
    let mut parity_digests = Vec::with_capacity(4);
    for mode in [
        WriterMode::QuiescentControl,
        WriterMode::IngestActive,
        WriterMode::IngestAndProjector,
        WriterMode::IngestProjectorAndPublisher,
    ] {
        let (mode_report, parity) = run_single_mode(mode, config, &chain, &template_stores).await?;
        mode_reports.push(mode_report);
        parity_digests.push(parity);
    }

    let baseline = mode_reports
        .first()
        .ok_or_else(|| BenchError::tip_replay("quiescent_control report missing"))?;
    let differentials_vs_quiescent = mode_reports
        .iter()
        .skip(1)
        .map(|candidate| ModeDifferential::between(baseline, candidate))
        .collect();
    let byte_parity = ByteParityReport::evaluate(parity_digests);

    let crossed_boundary = ((chain
        .base_tip_height
        .value()
        .saturating_sub(chain.reorg_window_blocks))
        / config.segment_blocks)
        .saturating_add(1)
        .saturating_mul(config.segment_blocks.get());

    Ok(ActiveWriterPublicationReport {
        contract_identity: ACTIVE_WRITER_PUBLICATION_CONTRACT.to_owned(),
        report_format_version: ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION,
        workload: WorkloadGeometryReport {
            network: zinder_core::wire::encode_zinder_native_chain_name(chain.network).to_owned(),
            base_blocks: config.base_blocks,
            live_blocks: config.live_blocks,
            final_tip_height: chain.final_tip_height.value(),
            reorg_window_blocks: config.reorg_window_blocks,
            segment_blocks: config.segment_blocks.get(),
            crossed_segment_boundary_height: crossed_boundary,
            sparse_block_count: chain.sparse_block_count,
            dense_block_count: chain.dense_block_count,
            mempool_churn_sizes: config.mempool_churn_sizes.clone(),
        },
        modes: mode_reports,
        differentials_vs_quiescent,
        byte_parity,
    })
}

fn validate_config(config: &ActiveWriterPublicationConfig) -> Result<(), BenchError> {
    if config.base_blocks <= config.reorg_window_blocks {
        return Err(BenchError::invalid_argument(
            "base_blocks must exceed reorg_window_blocks",
        ));
    }
    if config.live_blocks == 0 {
        return Err(BenchError::invalid_argument("live_blocks must be positive"));
    }
    if config.mempool_churn_sizes.is_empty() {
        return Err(BenchError::invalid_argument(
            "mempool_churn_sizes must name at least one batch size",
        ));
    }
    Ok(())
}

struct ModePaths {
    _root: TempDir,
    canonical_dir: PathBuf,
    wallet_dir: PathBuf,
    catalog_dir: PathBuf,
    publisher_priv_key: PathBuf,
    publisher_pub_key: PathBuf,
    secondary_dirs: Vec<PathBuf>,
    projector_secondary_dir: PathBuf,
    publisher_secondary_dir: PathBuf,
}

impl ModePaths {
    fn prepare(template: &TemplateStores) -> Result<Self, BenchError> {
        let root = tempfile::tempdir().map_err(|source| BenchError::io("mode tempdir", source))?;
        let canonical_dir = root.path().join("canonical");
        let wallet_dir = root.path().join("wallet");
        let catalog_dir = root.path().join("shielded-catalog");
        fs::create_dir_all(&catalog_dir).map_err(|source| BenchError::io(&catalog_dir, source))?;

        template.clone_to(&canonical_dir, &wallet_dir)?;

        let publisher_priv_key = root.path().join("publisher.pk8");
        let publisher_pub_key = root.path().join("publisher.pub");
        write_ephemeral_signing_keys(&publisher_priv_key, &publisher_pub_key)?;

        let mut secondary_dirs = Vec::with_capacity(READER_GENERATION_SLOTS);
        for index in 0..READER_GENERATION_SLOTS {
            let dir = root.path().join(format!("reader-sec-{index}"));
            fs::create_dir_all(&dir).map_err(|source| BenchError::io(&dir, source))?;
            secondary_dirs.push(dir);
        }
        let projector_secondary_dir = root.path().join("projector-sec");
        let publisher_secondary_dir = root.path().join("publisher-sec");
        fs::create_dir_all(&projector_secondary_dir)
            .map_err(|source| BenchError::io(&projector_secondary_dir, source))?;
        fs::create_dir_all(&publisher_secondary_dir)
            .map_err(|source| BenchError::io(&publisher_secondary_dir, source))?;

        Ok(Self {
            _root: root,
            canonical_dir,
            wallet_dir,
            catalog_dir,
            publisher_priv_key,
            publisher_pub_key,
            secondary_dirs,
            projector_secondary_dir,
            publisher_secondary_dir,
        })
    }
}

fn write_ephemeral_signing_keys(priv_path: &Path, pub_path: &Path) -> Result<(), BenchError> {
    let rng = SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng)
        .map_err(|_| BenchError::tip_replay("failed to generate ephemeral Ed25519 key"))?;
    let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref())
        .map_err(|_| BenchError::tip_replay("failed to parse ephemeral Ed25519 key"))?;
    fs::write(priv_path, pkcs8.as_ref()).map_err(|source| BenchError::io(priv_path, source))?;
    fs::write(pub_path, pair.public_key().as_ref())
        .map_err(|source| BenchError::io(pub_path, source))?;
    Ok(())
}

fn tickers_writer_budget() -> RocksDbResourceBudget {
    let mut budget = RocksDbResourceBudget::canonical_writer_defaults();
    budget.block_cache_bytes = 32 * 1024 * 1024;
    budget.write_buffer_bytes = 4 * 1024 * 1024;
    budget.memtable_budget_bytes = 32 * 1024 * 1024;
    budget.max_wal_bytes = 64 * 1024 * 1024;
    budget.statistics_level = RocksDbStatisticsLevel::Tickers;
    budget
}

fn tickers_reader_budget() -> RocksDbResourceBudget {
    let mut budget = RocksDbResourceBudget::for_local_tests();
    budget.block_cache_bytes = 32 * 1024 * 1024;
    budget.statistics_level = RocksDbStatisticsLevel::Tickers;
    budget
}

struct ModeRuntimeBootstrap {
    paths: ModePaths,
    boundary_height: u32,
    publisher_opt: Option<ShieldedCatalogPublisher>,
    reader_pool: Arc<ReaderSecondaryPool>,
    history: Arc<PublishedShieldedHistory>,
    mempool: Arc<BenchmarkMempoolSurface>,
    tip_watcher_tx: watch::Sender<u64>,
    server_shutdown: oneshot::Sender<()>,
    server_task: JoinHandle<()>,
    channel: AuthenticatedChannel,
    signals: Arc<FleetSignals>,
    fleet_spec: ReaderFleetSpec,
}

async fn bootstrap_mode_runtime(
    mode: WriterMode,
    config: &ActiveWriterPublicationConfig,
    chain: &GeneratedChain,
    template: &TemplateStores,
) -> Result<ModeRuntimeBootstrap, BenchError> {
    let paths = ModePaths::prepare(template)?;
    let boundary_height = next_segment_boundary(config, chain);

    if mode == WriterMode::QuiescentControl {
        pre_advance_quiescent_stores(&paths, config, chain).await?;
    }

    let bootstrap_publisher = ShieldedCatalogPublisher::open(
        &paths.catalog_dir,
        &paths.publisher_priv_key,
        BlockHeight::new(1),
        config.segment_blocks,
    )
    .map_err(|source| BenchError::io(&paths.catalog_dir, source))?;
    let publisher_opt = if mode == WriterMode::IngestProjectorAndPublisher {
        Some(bootstrap_publisher)
    } else {
        drop(bootstrap_publisher);
        None
    };

    let reader_pool = ReaderSecondaryPool::open(&paths, chain)?;
    let initial_reader = reader_pool.capture_slot().capture();
    let initial_epoch = initial_reader.chain_epoch();
    let initial_tip = initial_epoch.visible_tip_height.value();

    let history = Arc::new(
        PublishedShieldedHistory::follow_publisher_captured(
            &paths.catalog_dir,
            &paths.publisher_pub_key,
            false,
            || CapturedCanonical(reader_pool.capture_slot().capture()),
        )
        .await
        .map_err(|status| BenchError::canonical_read(status.to_string()))?,
    );
    let mempool = BenchmarkMempoolSurface::new(initial_epoch);
    let (tip_watcher_tx, tip_watcher) = create_tip_watcher(initial_epoch.id);

    let (server_addr, server_shutdown, server_task) = spawn_compat_grpc_server(
        reader_pool.capture_slot(),
        Arc::clone(&history),
        Arc::clone(&mempool),
        tip_watcher,
    )
    .await?;
    let channel = connect_channel(server_addr).await?;

    let signals = FleetSignals::new(initial_tip);
    let fleet_spec = ReaderFleetSpec {
        mode,
        base_tip_height: config.base_blocks,
        final_tip_height: chain.final_tip_height.value(),
        reorg_window_blocks: config.reorg_window_blocks,
        segment_boundary_height: boundary_height,
        slow_reader_frame_delay: config.slow_reader_frame_delay,
        mempool_churn_sizes: config.mempool_churn_sizes.clone(),
        quiescent_rounds: config.quiescent_rounds,
        sample_v5_tx: Arc::clone(&chain.sample_v5_tx),
    };

    Ok(ModeRuntimeBootstrap {
        paths,
        boundary_height,
        publisher_opt,
        reader_pool,
        history,
        mempool,
        tip_watcher_tx,
        server_shutdown,
        server_task,
        channel,
        signals,
        fleet_spec,
    })
}

async fn run_single_mode(
    mode: WriterMode,
    config: &ActiveWriterPublicationConfig,
    chain: &GeneratedChain,
    template: &TemplateStores,
) -> Result<(ModeReport, ModeParityDigests), BenchError> {
    let boot = bootstrap_mode_runtime(mode, config, chain, template).await?;
    let workload_started = Instant::now();
    let fleet_handle = spawn_reader_fleet(
        boot.channel.clone(),
        boot.fleet_spec,
        Arc::clone(&boot.signals),
        Arc::clone(&boot.mempool),
    );

    let active_outcome = if mode == WriterMode::QuiescentControl {
        drop(boot.publisher_opt);
        ActiveOutcome::default()
    } else {
        drive_active_pipelines(
            mode,
            config,
            chain,
            &boot.paths,
            PipelineShared {
                reader_pool: &boot.reader_pool,
                history: &boot.history,
                mempool: &boot.mempool,
                tip_watcher_tx: &boot.tip_watcher_tx,
                signals: &boot.signals,
                publisher: boot.publisher_opt,
            },
        )
        .await?
    };

    boot.signals.writer_done.store(true, Ordering::Release);
    let fleet_output = fleet_handle
        .await
        .map_err(|err| BenchError::canonical_read(err.to_string()))??;
    let workload_duration_ms = workload_started.elapsed().as_secs_f64() * 1_000.0;

    let parity = collect_final_tip_parity(
        boot.channel,
        mode,
        config.base_blocks,
        chain.final_tip_height.value(),
        boot.boundary_height,
    )
    .await?;

    let (reader_io_modes, reader_secondary_delta) = boot.reader_pool.collect_metrics();
    let _ = boot.server_shutdown.send(());
    let _ = boot.server_task.await;
    drop(boot.reader_pool);

    let report = assemble_mode_report(ModeReportAssembly {
        mode,
        workload_duration_ms,
        reader_io_modes,
        reader_delta: reader_secondary_delta,
        fleet: fleet_output,
        active: active_outcome,
        errors: boot.signals.error_collector.lock().to_report(),
    });
    Ok((report, parity))
}

struct PipelineShared<'a> {
    reader_pool: &'a Arc<ReaderSecondaryPool>,
    history: &'a Arc<PublishedShieldedHistory>,
    mempool: &'a Arc<BenchmarkMempoolSurface>,
    tip_watcher_tx: &'a watch::Sender<u64>,
    signals: &'a Arc<FleetSignals>,
    publisher: Option<ShieldedCatalogPublisher>,
}

#[derive(Default)]
struct ActiveOutcome {
    canonical_io_mode: Option<String>,
    wallet_io_mode: Option<String>,
    publisher_io_mode: Option<String>,
    ingest: IngestStageMetrics,
    projector: ProjectorMetrics,
    publisher: PublisherMetrics,
    canonical_delta: StoreTickerDelta,
    wallet_delta: StoreTickerDelta,
    publisher_delta: StoreTickerDelta,
    node_rpcs: super::report::NodeRpcCounts,
}

struct ActiveHandles {
    canonical_io_mode: String,
    reader_task: JoinHandle<Result<(), BenchError>>,
    projector_handle: Option<JoinHandle<Result<ProjectorOutput, BenchError>>>,
    publisher_handle: Option<JoinHandle<Result<PublisherOutput, BenchError>>>,
    cancel: CancellationToken,
}

fn open_active_canonical_store(
    paths: &ModePaths,
    chain: &GeneratedChain,
) -> Result<
    (
        RocksDbCanonicalStore,
        String,
        Arc<NetworkUpgradeActivations>,
    ),
    BenchError,
> {
    let reorg_policy = CanonicalReorgPolicy::new(chain.reorg_window_blocks)?;
    let canonical_store = RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
        &paths.canonical_dir,
        &chain.activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        reorg_policy,
        tickers_writer_budget(),
    )?;
    let canonical_io_mode = canonical_store.io_mode().as_str().to_owned();
    let activations = canonical_store
        .network_upgrade_activations()
        .cloned()
        .ok_or_else(|| BenchError::tip_replay("canonical store missing upgrade activations"))?;
    Ok((canonical_store, canonical_io_mode, Arc::new(activations)))
}

async fn drive_active_pipelines(
    mode: WriterMode,
    config: &ActiveWriterPublicationConfig,
    chain: &GeneratedChain,
    paths: &ModePaths,
    shared: PipelineShared<'_>,
) -> Result<ActiveOutcome, BenchError> {
    let source = AdvancingChainSource::new(chain, chain.base_tip_height);
    let (canonical_store, canonical_io_mode, activations) =
        open_active_canonical_store(paths, chain)?;

    let cancel = CancellationToken::new();
    let commit_wake = Arc::new(Notify::new());
    let writer_finished = Arc::new(AtomicBool::new(false));
    let reader_task = spawn_reader_catchup_loop(ReaderCatchupInputs {
        pool: Arc::clone(shared.reader_pool),
        commit_wake: Arc::clone(&commit_wake),
        writer_finished: Arc::clone(&writer_finished),
        signals: Arc::clone(shared.signals),
        mempool: Arc::clone(shared.mempool),
        tip_watcher_tx: shared.tip_watcher_tx.clone(),
        final_tip_height: chain.final_tip_height,
    });

    let projector_handle = matches!(
        mode,
        WriterMode::IngestAndProjector | WriterMode::IngestProjectorAndPublisher
    )
    .then(|| {
        spawn_projector_loop(
            paths,
            chain,
            Arc::clone(&commit_wake),
            Arc::clone(&writer_finished),
        )
    })
    .transpose()?;

    let publisher_handle = shared
        .publisher
        .map(|publisher| {
            spawn_publisher_loop(
                paths,
                chain,
                publisher,
                PublisherLoopSync {
                    reader_slot: shared.reader_pool.capture_slot(),
                    history: Arc::clone(shared.history),
                    commit_wake: Arc::clone(&commit_wake),
                    writer_finished: Arc::clone(&writer_finished),
                    cancel: cancel.clone(),
                },
            )
        })
        .transpose()?;

    let writer_output = run_writer_thread(
        canonical_store,
        source.clone(),
        activations,
        WriterThreadSpec {
            start_height: chain.base_tip_height.value().saturating_add(1),
            end_height: chain.final_tip_height.value(),
            commit_interval: config.commit_interval,
            signals: Arc::clone(shared.signals),
            commit_wake: Arc::clone(&commit_wake),
        },
    )
    .await?;

    writer_finished.store(true, Ordering::Release);
    commit_wake.notify_waiters();

    let handles = ActiveHandles {
        canonical_io_mode,
        reader_task,
        projector_handle,
        publisher_handle,
        cancel,
    };
    finalize_active_outcome(handles, writer_output, source.rpc_counts()).await
}

async fn finalize_active_outcome(
    handles: ActiveHandles,
    writer_output: WriterThreadOutput,
    node_rpcs: super::report::NodeRpcCounts,
) -> Result<ActiveOutcome, BenchError> {
    handles
        .reader_task
        .await
        .map_err(|err| BenchError::tip_replay(err.to_string()))??;

    let projector_out = match handles.projector_handle {
        Some(handle) => Some(
            handle
                .await
                .map_err(|err| BenchError::tip_replay(err.to_string()))??,
        ),
        None => None,
    };

    let publisher_out = match handles.publisher_handle {
        Some(handle) => Some(
            handle
                .await
                .map_err(|err| BenchError::tip_replay(err.to_string()))??,
        ),
        None => None,
    };
    handles.cancel.cancel();

    Ok(ActiveOutcome {
        canonical_io_mode: Some(handles.canonical_io_mode),
        wallet_io_mode: projector_out.as_ref().map(|proj| proj.io_mode.clone()),
        publisher_io_mode: publisher_out.as_ref().map(|publ| publ.io_mode.clone()),
        ingest: writer_output.ingest,
        projector: projector_out
            .as_ref()
            .map_or_else(ProjectorMetrics::default, |proj| proj.metrics),
        publisher: publisher_out
            .as_ref()
            .map_or_else(PublisherMetrics::default, |publ| publ.metrics),
        canonical_delta: writer_output.delta,
        wallet_delta: projector_out.map_or_else(StoreTickerDelta::default, |proj| proj.delta),
        publisher_delta: publisher_out.map_or_else(StoreTickerDelta::default, |publ| publ.delta),
        node_rpcs,
    })
}

#[derive(Clone)]
struct WriterThreadSpec {
    start_height: u32,
    end_height: u32,
    commit_interval: Duration,
    signals: Arc<FleetSignals>,
    commit_wake: Arc<Notify>,
}

struct WriterThreadOutput {
    ingest: IngestStageMetrics,
    delta: StoreTickerDelta,
}

async fn run_writer_thread(
    store: RocksDbCanonicalStore,
    source: AdvancingChainSource,
    activations: Arc<NetworkUpgradeActivations>,
    spec: WriterThreadSpec,
) -> Result<WriterThreadOutput, BenchError> {
    let (done_tx, done_rx) = oneshot::channel::<Result<WriterThreadOutput, BenchError>>();
    std::thread::spawn(move || {
        let outcome = (|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|source_err| BenchError::tip_replay(source_err.to_string()))?;
            let recorder = FollowStepRecorder::default();
            let _guard = metrics::set_default_local_recorder(&recorder);
            let readiness = ComposedReadiness::new(Readiness::default(), &[]);
            let cancel = CancellationToken::new();

            let ticker_before = capture_canonical_writer_tickers(&store);
            let mut active_store = store;
            let mut prepare_ms = Vec::new();
            let mut commit_ms = Vec::new();

            for raw_height in spec.start_height..=spec.end_height {
                if !spec.commit_interval.is_zero() {
                    std::thread::sleep(spec.commit_interval);
                }
                let target = BlockHeight::new(raw_height);
                source.advance_tip_to(target);
                recorder.clear();

                let follower = CanonicalFollower::new(
                    &source,
                    Arc::clone(&activations),
                    follow_config_for(target),
                    &readiness,
                    &cancel,
                );
                active_store = runtime.block_on(follow_canonical_tip(active_store, follower))?;
                spec.signals.record_commit(raw_height);
                spec.commit_wake.notify_waiters();

                let step = recorder.take_step()?;
                prepare_ms.push(step.prepare_secs * 1_000.0);
                commit_ms.push(step.commit_secs * 1_000.0);
            }

            let delta = capture_canonical_writer_tickers(&active_store).delta_since(&ticker_before);
            drop(active_store);
            Ok(WriterThreadOutput {
                ingest: IngestStageMetrics {
                    live_prepare_ms: LatencySummaryMs::from_f64(&prepare_ms),
                    live_commit_ms: LatencySummaryMs::from_f64(&commit_ms),
                    blocks_committed: spec
                        .end_height
                        .saturating_sub(spec.start_height)
                        .saturating_add(1),
                },
                delta,
            })
        })();
        let _ = done_tx.send(outcome);
    });

    done_rx
        .await
        .map_err(|_| BenchError::tip_replay("writer thread terminated unexpectedly"))?
}

struct ProjectorOutput {
    io_mode: String,
    metrics: ProjectorMetrics,
    delta: StoreTickerDelta,
}

fn spawn_projector_loop(
    paths: &ModePaths,
    chain: &GeneratedChain,
    commit_wake: Arc<Notify>,
    writer_finished: Arc<AtomicBool>,
) -> Result<JoinHandle<Result<ProjectorOutput, BenchError>>, BenchError> {
    let reorg_policy = CanonicalReorgPolicy::new(chain.reorg_window_blocks)?;
    let mut secondary = RocksDbCanonicalSecondary::open_ready(
        &paths.canonical_dir,
        &paths.projector_secondary_dir,
        &chain.activations,
        CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::Transactions),
        reorg_policy,
        tickers_reader_budget(),
    )?;
    let mut wallet = RocksDbWalletStore::open_ready_for_following(
        &paths.wallet_dir,
        chain.network,
        tickers_writer_budget(),
    )?;
    wallet.raise_supported_reorg_depth(reorg_policy.reorg_window_blocks())?;
    let final_height = chain.final_tip_height;

    Ok(tokio::task::spawn_blocking(move || {
        let before = capture_wallet_tickers(&wallet);
        let mut apply_durations = Vec::new();
        let mut transitions = 0_u32;

        loop {
            let drained = drain_projector_events(
                &mut secondary,
                &mut wallet,
                &mut apply_durations,
                &mut transitions,
            )?;
            let wallet_pos =
                WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
            let wallet_height = wallet_pos.source_position().tip.height;
            if writer_finished.load(Ordering::Acquire) && wallet_height >= final_height {
                break;
            }
            if drained == 0 {
                std::thread::sleep(Duration::from_millis(2));
            }
            let _ = &commit_wake;
        }

        let delta = capture_wallet_tickers(&wallet).delta_since(&before);
        let final_identity =
            WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        let ready_wallet = wallet.into_ready_store(final_identity)?;
        let wallet_io = ready_wallet.io_mode().as_str().to_owned();
        drop(ready_wallet);
        drop(secondary);
        Ok(ProjectorOutput {
            io_mode: wallet_io,
            metrics: ProjectorMetrics {
                apply_ms: LatencySummaryMs::from_durations(&apply_durations),
                transitions_applied: transitions,
            },
            delta,
        })
    }))
}

fn drain_projector_events(
    secondary: &mut RocksDbCanonicalSecondary,
    wallet: &mut RocksDbWalletFollowingStore,
    durations: &mut Vec<Duration>,
    transitions: &mut u32,
) -> Result<usize, BenchError> {
    secondary.try_catch_up()?;
    let wallet_source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
    let cursor = wallet_source.source_position().event_cursor.as_bytes();
    let batch_limit = NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN);
    let events = secondary.canonical_event_history(CanonicalEventHistoryRequest::new(
        Some(&cursor),
        batch_limit,
    ))?;
    let count = events.len();
    for event in events {
        if !matches!(event.kind(), CanonicalEventKind::Committed) {
            continue;
        }
        let committed = event.committed_range();
        let fence = event.resulting_fence();
        let epoch = secondary.chain_epoch_at(event.resulting_epoch_id())?;
        let settled_tip = BlockId::new(epoch.settled_tip_height, epoch.settled_tip_hash);
        let current_source =
            WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
        let started = Instant::now();
        let replay_rows = secondary.scan_canonical_replay_range(committed)?;
        let max_bytes = NonZeroU64::new(DEFAULT_MAX_TRANSITION_BYTES).unwrap_or(NonZeroU64::MIN);
        wallet.apply_canonical_event_range(
            current_source,
            event,
            fence,
            settled_tip,
            max_bytes,
            replay_rows,
        )?;
        durations.push(started.elapsed());
        *transitions = transitions.saturating_add(1);
    }
    Ok(count)
}

struct PublisherLoopSync {
    reader_slot: CanonicalReaderSlot,
    history: Arc<PublishedShieldedHistory>,
    commit_wake: Arc<Notify>,
    writer_finished: Arc<AtomicBool>,
    cancel: CancellationToken,
}

struct PublisherOutput {
    io_mode: String,
    metrics: PublisherMetrics,
    delta: StoreTickerDelta,
}

struct PublisherStepOutcome {
    secondary: RocksDbCanonicalSecondary,
    publisher: ShieldedCatalogPublisher,
    elapsed: Option<Duration>,
    sealed: usize,
    refreshed: bool,
}

async fn step_publisher_once(
    secondary: RocksDbCanonicalSecondary,
    mut publisher: ShieldedCatalogPublisher,
    sync: &PublisherLoopSync,
) -> Result<PublisherStepOutcome, BenchError> {
    let canonical_reader = RocksDbCanonicalReader::new(secondary)
        .map_err(|err| BenchError::canonical_read(err.to_string()))?;
    let concurrency = PublicationConcurrency {
        workers: NonZeroUsize::MIN,
        commit_batch: NonZeroUsize::MIN,
    };
    let started = Instant::now();
    let (published_now, returned_reader, returned_publisher) =
        tokio::task::spawn_blocking(move || {
            let sealed_outcome = publisher.publish_available(
                &canonical_reader,
                concurrency,
                &CancellationToken::new(),
            );
            (sealed_outcome, canonical_reader, publisher)
        })
        .await
        .map_err(|err| BenchError::tip_replay(err.to_string()))?;

    let recovered_sec = returned_reader
        .into_secondary()
        .map_err(|_| BenchError::tip_replay("publisher secondary held unexpectedly"))?;
    let sealed = published_now.map_err(|err| BenchError::io(paths_catalog_label(sync), err))?;
    if sealed == 0 {
        return Ok(PublisherStepOutcome {
            secondary: recovered_sec,
            publisher: returned_publisher,
            elapsed: None,
            sealed: 0,
            refreshed: false,
        });
    }

    let elapsed = started.elapsed();
    let refreshed = sync
        .history
        .refresh_captured(|| CapturedCanonical(sync.reader_slot.capture()))
        .await
        .map_err(|status| BenchError::canonical_read(status.to_string()))?;
    Ok(PublisherStepOutcome {
        secondary: recovered_sec,
        publisher: returned_publisher,
        elapsed: Some(elapsed),
        sealed,
        refreshed,
    })
}

async fn run_publisher_task(
    secondary: RocksDbCanonicalSecondary,
    mut publisher: ShieldedCatalogPublisher,
    sync: PublisherLoopSync,
    publisher_io: String,
    final_settled_height: BlockHeight,
) -> Result<PublisherOutput, BenchError> {
    let before = capture_secondary_tickers(&secondary);
    let mut active_sec = secondary;
    let mut publish_durations = Vec::new();
    let mut total_published = 0_usize;
    let mut refreshes = 0_u64;

    loop {
        active_sec.try_catch_up()?;
        let settled_tip = active_sec.chain_epoch()?.settled_tip_height;
        let step = step_publisher_once(active_sec, publisher, &sync).await?;
        active_sec = step.secondary;
        publisher = step.publisher;

        if let Some(elapsed) = step.elapsed {
            publish_durations.push(elapsed);
            total_published = total_published.saturating_add(step.sealed);
            if step.refreshed {
                refreshes = refreshes.saturating_add(1);
            }
        }

        if sync.writer_finished.load(Ordering::Acquire) && settled_tip >= final_settled_height {
            break;
        }
        tokio::select! {
            () = sync.commit_wake.notified() => {}
            () = tokio::time::sleep(Duration::from_millis(3)) => {}
            () = sync.cancel.cancelled() => break,
        }
    }

    let delta = capture_secondary_tickers(&active_sec).delta_since(&before);
    drop(active_sec);
    let total_ms = publish_durations
        .iter()
        .map(|dur| dur.as_secs_f64() * 1_000.0)
        .sum();

    Ok(PublisherOutput {
        io_mode: publisher_io,
        metrics: PublisherMetrics {
            total_ms,
            segment_publish_ms: LatencySummaryMs::from_durations(&publish_durations),
            segments_published: total_published,
            catalog_refreshes: refreshes,
        },
        delta,
    })
}

fn spawn_publisher_loop(
    paths: &ModePaths,
    chain: &GeneratedChain,
    publisher: ShieldedCatalogPublisher,
    sync: PublisherLoopSync,
) -> Result<JoinHandle<Result<PublisherOutput, BenchError>>, BenchError> {
    let secondary = RocksDbCanonicalSecondary::open_ready(
        &paths.canonical_dir,
        &paths.publisher_secondary_dir,
        &chain.activations,
        CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::Transactions),
        CanonicalReorgPolicy::new(chain.reorg_window_blocks)?,
        tickers_reader_budget(),
    )?;
    let publisher_io = secondary.io_mode().as_str().to_owned();
    let final_settled_height = BlockHeight::new(
        chain
            .final_tip_height
            .value()
            .saturating_sub(chain.reorg_window_blocks),
    );

    Ok(tokio::spawn(run_publisher_task(
        secondary,
        publisher,
        sync,
        publisher_io,
        final_settled_height,
    )))
}

struct ReaderSecondaryPool {
    slot: CanonicalReaderSlot,
    shared_resources: SharedSecondaryResources,
    spares: Mutex<Vec<RocksDbCanonicalSecondary>>,
    retired: Mutex<Vec<Arc<RocksDbCanonicalReader>>>,
    initial_tickers: TickerCounterSnapshot,
    io_modes: Vec<String>,
}

impl ReaderSecondaryPool {
    fn open(paths: &ModePaths, chain: &GeneratedChain) -> Result<Arc<Self>, BenchError> {
        let shared_resources = SharedSecondaryResources::new(tickers_reader_budget());
        let reorg_policy = CanonicalReorgPolicy::new(chain.reorg_window_blocks)?;
        let requirements =
            CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::Transactions);

        let mut opened = Vec::with_capacity(paths.secondary_dirs.len());
        let mut io_modes = Vec::with_capacity(paths.secondary_dirs.len());
        for dir in &paths.secondary_dirs {
            let secondary = RocksDbCanonicalSecondary::open_ready(
                &paths.canonical_dir,
                dir,
                &chain.activations,
                requirements.clone(),
                reorg_policy,
                &shared_resources,
            )?;
            io_modes.push(secondary.io_mode().as_str().to_owned());
            opened.push(secondary);
        }

        let initial_tickers = capture_secondary_tickers(&opened[0]);
        let primary_sec = opened.remove(0);
        let reader = RocksDbCanonicalReader::new(primary_sec)
            .map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let slot = CanonicalReaderSlot::new(Arc::new(reader));

        Ok(Arc::new(Self {
            slot,
            shared_resources,
            spares: Mutex::new(opened),
            retired: Mutex::new(Vec::new()),
            initial_tickers,
            io_modes,
        }))
    }

    fn capture_slot(&self) -> CanonicalReaderSlot {
        self.slot.clone()
    }

    fn try_catch_up_and_publish(&self) -> Result<(u32, ChainEpoch), BenchError> {
        self.reclaim_retired();
        let current_epoch = self.slot.capture().chain_epoch();
        let spare = self.spares.lock().pop();
        let Some(mut secondary) = spare else {
            return Ok((current_epoch.visible_tip_height.value(), current_epoch));
        };
        secondary.try_catch_up()?;
        let caught_up_epoch = secondary.chain_epoch()?;
        if caught_up_epoch.id == current_epoch.id {
            self.spares.lock().push(secondary);
            return Ok((current_epoch.visible_tip_height.value(), current_epoch));
        }
        let new_reader = Arc::new(
            RocksDbCanonicalReader::new(secondary)
                .map_err(|err| BenchError::canonical_read(err.to_string()))?,
        );
        let old_reader = self.slot.publish(new_reader);
        self.return_or_retire(old_reader);
        Ok((caught_up_epoch.visible_tip_height.value(), caught_up_epoch))
    }

    fn reclaim_retired(&self) {
        let mut retired = self.retired.lock();
        let mut remaining = Vec::with_capacity(retired.len());
        for reader in retired.drain(..) {
            match Arc::try_unwrap(reader) {
                Ok(sole) => {
                    if let Ok(sec) = sole.into_secondary() {
                        self.spares.lock().push(sec);
                    }
                }
                Err(shared) => remaining.push(shared),
            }
        }
        *retired = remaining;
    }

    fn return_or_retire(&self, old_reader: Arc<RocksDbCanonicalReader>) {
        match Arc::try_unwrap(old_reader) {
            Ok(sole) => {
                if let Ok(sec) = sole.into_secondary() {
                    self.spares.lock().push(sec);
                }
            }
            Err(shared) => self.retired.lock().push(shared),
        }
    }

    fn collect_metrics(&self) -> (Vec<String>, StoreTickerDelta) {
        let reader = self.slot.capture();
        let delta =
            capture_secondary_tickers(reader.secondary()).delta_since(&self.initial_tickers);
        let _ = &self.shared_resources;
        (self.io_modes.clone(), delta)
    }
}

struct ReaderCatchupInputs {
    pool: Arc<ReaderSecondaryPool>,
    commit_wake: Arc<Notify>,
    writer_finished: Arc<AtomicBool>,
    signals: Arc<FleetSignals>,
    mempool: Arc<BenchmarkMempoolSurface>,
    tip_watcher_tx: watch::Sender<u64>,
    final_tip_height: BlockHeight,
}

fn spawn_reader_catchup_loop(inputs: ReaderCatchupInputs) -> JoinHandle<Result<(), BenchError>> {
    tokio::spawn(async move {
        let mut last_published_tip = 0_u32;
        loop {
            let (tip, epoch) = inputs.pool.try_catch_up_and_publish()?;
            if tip > last_published_tip {
                last_published_tip = tip;
                inputs
                    .signals
                    .highest_served_tip
                    .fetch_max(tip, Ordering::AcqRel);
                inputs.mempool.advance_epoch(epoch);
                inputs.tip_watcher_tx.send_replace(epoch.id.value());
            }
            if inputs.writer_finished.load(Ordering::Acquire)
                && tip >= inputs.final_tip_height.value()
            {
                break;
            }
            tokio::select! {
                () = inputs.commit_wake.notified() => {}
                () = tokio::time::sleep(Duration::from_millis(2)) => {}
            }
        }
        Ok(())
    })
}

fn paths_catalog_label(sync: &PublisherLoopSync) -> String {
    let _ = sync;
    "shielded-catalog".to_owned()
}

async fn pre_advance_quiescent_stores(
    paths: &ModePaths,
    config: &ActiveWriterPublicationConfig,
    chain: &GeneratedChain,
) -> Result<(), BenchError> {
    let reorg_policy = CanonicalReorgPolicy::new(chain.reorg_window_blocks)?;
    let source = AdvancingChainSource::new(chain, chain.final_tip_height);
    let canonical = RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
        &paths.canonical_dir,
        &chain.activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        reorg_policy,
        tickers_writer_budget(),
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

    let mut secondary = RocksDbCanonicalSecondary::open_ready(
        &paths.canonical_dir,
        &paths.publisher_secondary_dir,
        &chain.activations,
        CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::Transactions),
        reorg_policy,
        tickers_reader_budget(),
    )?;
    let mut wallet = RocksDbWalletStore::open_ready_for_following(
        &paths.wallet_dir,
        chain.network,
        tickers_writer_budget(),
    )?;
    wallet.raise_supported_reorg_depth(reorg_policy.reorg_window_blocks())?;
    let mut unused_durations = Vec::new();
    let mut unused_transitions = 0_u32;
    drain_projector_events(
        &mut secondary,
        &mut wallet,
        &mut unused_durations,
        &mut unused_transitions,
    )?;
    drop(wallet);

    let canonical_reader = RocksDbCanonicalReader::new(secondary)
        .map_err(|err| BenchError::canonical_read(err.to_string()))?;
    let mut publisher = ShieldedCatalogPublisher::open(
        &paths.catalog_dir,
        &paths.publisher_priv_key,
        BlockHeight::new(1),
        config.segment_blocks,
    )
    .map_err(|source_err| BenchError::io(&paths.catalog_dir, source_err))?;
    let concurrency = PublicationConcurrency {
        workers: NonZeroUsize::MIN,
        commit_batch: NonZeroUsize::MIN,
    };
    publisher
        .publish_available(&canonical_reader, concurrency, &CancellationToken::new())
        .map_err(|source_err| BenchError::io(&paths.catalog_dir, source_err))?;
    drop(publisher);
    drop(canonical_reader);
    Ok(())
}

async fn spawn_compat_grpc_server(
    slot: CanonicalReaderSlot,
    history: Arc<PublishedShieldedHistory>,
    mempool: Arc<BenchmarkMempoolSurface>,
    tip_watcher: zinder_compat_lightwalletd::SharedTipChangeWatcher,
) -> Result<(SocketAddr, oneshot::Sender<()>, JoinHandle<()>), BenchError> {
    let dummy_node = ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        "http://127.0.0.1:1",
        NodeAuth::None,
        Duration::from_secs(5),
    )?;
    let adapter = LightwalletdGrpcAdapter::for_light_wallet(
        slot,
        dummy_node,
        Duration::from_secs(2),
        NonZeroU32::new(4).unwrap_or(NonZeroU32::MIN),
        ForwardedReadLimit::default(),
    )
    .with_published_shielded_history(history)
    .with_mempool_surface(as_mempool_surface(mempool))
    .with_tip_change_watcher(tip_watcher)
    .with_tip_wait(TipWait::default());

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|source| BenchError::io("127.0.0.1:0", source))?;
    let addr = listener
        .local_addr()
        .map_err(|source| BenchError::io("127.0.0.1:0", source))?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    Ok((addr, shutdown_tx, handle))
}

async fn connect_channel(addr: SocketAddr) -> Result<AuthenticatedChannel, BenchError> {
    let url = format!("http://{addr}");
    connect_zinder_grpc_with_timeout(&url, None, Duration::from_secs(2))
        .await
        .map_err(|err| BenchError::canonical_read(err.to_string()))
}

struct ModeReportAssembly {
    mode: WriterMode,
    workload_duration_ms: f64,
    reader_io_modes: Vec<String>,
    reader_delta: StoreTickerDelta,
    fleet: ReaderFleetOutput,
    active: ActiveOutcome,
    errors: super::report::ProtocolErrorCounts,
}

fn assemble_mode_report(assembly: ModeReportAssembly) -> ModeReport {
    let ModeReportAssembly {
        mode,
        workload_duration_ms,
        reader_io_modes,
        reader_delta,
        fleet,
        active,
        errors,
    } = assembly;
    ModeReport {
        mode,
        workload_duration_ms,
        io_modes: IoModeReport {
            canonical_primary: active.canonical_io_mode,
            canonical_secondaries: reader_io_modes,
            wallet_primary: active.wallet_io_mode,
            publisher_secondary: active.publisher_io_mode,
        },
        ingest: active.ingest,
        commit_to_served_tip_ms: fleet.commit_to_served_tip_ms,
        catalog_range: fleet.catalog_range,
        tail_refill: fleet.tail_refill,
        slow_boundary_range: fleet.slow_boundary_range,
        tree_state_ms: fleet.tree_state_ms,
        mempool_churn: fleet.mempool_churn,
        projector: active.projector,
        publisher: active.publisher,
        writer_fsyncs: WriterFsyncCounts {
            canonical_wal_synced: active.canonical_delta.wal_synced,
            canonical_wal_bytes: active.canonical_delta.wal_bytes,
            wallet_wal_synced: active.wallet_delta.wal_synced,
            wallet_wal_bytes: active.wallet_delta.wal_bytes,
        },
        node_rpcs: active.node_rpcs,
        rocksdb_activity: RocksDbActivityReport {
            canonical_writer: active.canonical_delta,
            reader_secondaries: reader_delta,
            wallet_writer: active.wallet_delta,
            publisher_secondary: active.publisher_delta,
        },
        errors,
    }
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

fn next_segment_boundary(config: &ActiveWriterPublicationConfig, chain: &GeneratedChain) -> u32 {
    let base_settled = chain
        .base_tip_height
        .value()
        .saturating_sub(chain.reorg_window_blocks);
    let width = config.segment_blocks.get();
    ((base_settled / width).saturating_add(1))
        .saturating_mul(width)
        .min(chain.final_tip_height.value())
}

fn capture_canonical_writer_tickers(store: &RocksDbCanonicalStore) -> TickerCounterSnapshot {
    TickerCounterSnapshot::capture(|ticker| store.rocksdb_ticker_count(ticker))
}

fn capture_secondary_tickers(secondary: &RocksDbCanonicalSecondary) -> TickerCounterSnapshot {
    TickerCounterSnapshot::capture(|ticker| secondary.rocksdb_ticker_count(ticker))
}

fn capture_wallet_tickers(wallet: &RocksDbWalletFollowingStore) -> TickerCounterSnapshot {
    TickerCounterSnapshot::capture(|ticker| wallet.rocksdb_ticker_count(ticker))
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

#[derive(Default)]
struct FollowStepRecorder {
    samples: Arc<Mutex<StepHistograms>>,
}

impl FollowStepRecorder {
    fn clear(&self) {
        *self.samples.lock() = StepHistograms::default();
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
}

impl metrics::HistogramFn for StepHistogramHandle {
    fn record(&self, reading: f64) {
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
        }))
    }
}
