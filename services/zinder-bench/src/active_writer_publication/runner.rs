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
    CanonicalReaderSlot, EncodedTailLookupEvent, EncodedTailLookupObserver,
    LightwalletdGrpcAdapter, PublicationConcurrency, PublishedShieldedHistory,
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
        FleetSignals, ParityProbeRequest, ReaderFleetChannels, ReaderFleetOutput, ReaderFleetSpec,
        collect_final_tip_parity, spawn_reader_fleet,
    },
    report::{
        ACTIVE_WRITER_PUBLICATION_CONTRACT, ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION,
        ActiveWriterPublicationReport, ByteParityReport, DifferentialBaselines, IngestStageMetrics,
        IoModeReport, LatencySummaryMs, ModeDifferential, ModeParityDigests, ModeReport,
        ProjectorMetrics, PublisherMetrics, RocksDbActivityReport, StoreTickerDelta,
        TickerCounterSnapshot, WorkloadGeometryReport, WriterFsyncCounts, WriterMode,
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
const MAX_CATALOG_REFRESH_RETRIES: u32 = 500;

#[derive(Clone, Copy)]
struct ModeRunSpec {
    mode: WriterMode,
    label: &'static str,
    publish_catalog: bool,
}

/// Runs all matched writer/publication arms across one advancing Regtest chain.
///
/// Exercises both unpublished and published quiescent baselines alongside active
/// writer, projector, and publisher combinations, returning structured timing and parity.
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

    let (published_quiescent_control, published_parity) = run_single_mode(
        ModeRunSpec {
            mode: WriterMode::QuiescentControl,
            label: "published_quiescent_control",
            publish_catalog: true,
        },
        config,
        &chain,
        &template_stores,
    )
    .await?;

    let mut mode_reports = Vec::with_capacity(4);
    let mut parity_digests = Vec::with_capacity(5);
    parity_digests.push(published_parity);

    for run_spec in [
        ModeRunSpec {
            mode: WriterMode::QuiescentControl,
            label: "quiescent_control",
            publish_catalog: false,
        },
        ModeRunSpec {
            mode: WriterMode::IngestActive,
            label: "ingest_active",
            publish_catalog: false,
        },
        ModeRunSpec {
            mode: WriterMode::IngestAndProjector,
            label: "ingest_and_projector",
            publish_catalog: false,
        },
        ModeRunSpec {
            mode: WriterMode::IngestProjectorAndPublisher,
            label: "ingest_projector_and_publisher",
            publish_catalog: true,
        },
    ] {
        let (mode_report, parity) =
            run_single_mode(run_spec, config, &chain, &template_stores).await?;
        mode_reports.push(mode_report);
        parity_digests.push(parity);
    }

    let unpublished_baseline = mode_reports
        .first()
        .ok_or_else(|| BenchError::tip_replay("quiescent_control report missing"))?;
    let differentials_vs_quiescent = build_matched_differentials(
        unpublished_baseline,
        &published_quiescent_control,
        &mode_reports[1..],
    );
    let byte_parity = ByteParityReport::evaluate(parity_digests);
    let workload = build_workload_geometry(config, &chain);

    Ok(ActiveWriterPublicationReport {
        contract_identity: ACTIVE_WRITER_PUBLICATION_CONTRACT.to_owned(),
        report_format_version: ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION,
        workload,
        published_quiescent_control,
        modes: mode_reports,
        differentials_vs_quiescent,
        byte_parity,
    })
}

fn build_matched_differentials(
    unpublished_baseline: &ModeReport,
    published_baseline: &ModeReport,
    active_modes: &[ModeReport],
) -> Vec<ModeDifferential> {
    active_modes
        .iter()
        .map(|candidate| {
            let (baseline_label, baseline_catalog_published) =
                if candidate.mode == WriterMode::IngestProjectorAndPublisher {
                    ("published_quiescent_control", true)
                } else {
                    ("quiescent_control", false)
                };
            ModeDifferential::between(
                DifferentialBaselines {
                    unpublished: unpublished_baseline,
                    published: published_baseline,
                    baseline_label,
                    baseline_catalog_published,
                },
                candidate,
            )
        })
        .collect()
}

fn build_workload_geometry(
    config: &ActiveWriterPublicationConfig,
    chain: &GeneratedChain,
) -> WorkloadGeometryReport {
    let crossed_boundary = ((chain
        .base_tip_height
        .value()
        .saturating_sub(chain.reorg_window_blocks))
        / config.segment_blocks)
        .saturating_add(1)
        .saturating_mul(config.segment_blocks.get());
    WorkloadGeometryReport {
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
    }
}

fn validate_config(config: &ActiveWriterPublicationConfig) -> Result<(), BenchError> {
    if config.base_blocks <= config.reorg_window_blocks {
        return Err(BenchError::invalid_argument(
            "base_blocks must exceed reorg_window_blocks",
        ));
    }
    if config.reorg_window_blocks == 0 {
        return Err(BenchError::invalid_argument(
            "reorg_window_blocks must be positive",
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
    if u64::from(config.live_blocks)
        < u64::try_from(config.mempool_churn_sizes.len()).unwrap_or(u64::MAX)
    {
        return Err(BenchError::invalid_argument(
            "live_blocks must be at least mempool_churn_sizes.len() so every mempool tier overlaps live commits",
        ));
    }
    let base_settled = config
        .base_blocks
        .saturating_sub(config.reorg_window_blocks);
    let width = config.segment_blocks.get();
    let next_boundary = ((base_settled / width).saturating_add(1)).saturating_mul(width);
    let final_tip = config.base_blocks.saturating_add(config.live_blocks);
    let final_settled = final_tip.saturating_sub(config.reorg_window_blocks);
    if final_settled < next_boundary {
        return Err(BenchError::invalid_argument(format!(
            "final settled height ({final_settled}) must reach next segment boundary ({next_boundary})"
        )));
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

struct ModeServers {
    fleet_shutdown: oneshot::Sender<()>,
    fleet_task: JoinHandle<()>,
    tail_shutdown: oneshot::Sender<()>,
    tail_task: JoinHandle<()>,
}

struct ModeRuntimeBootstrap {
    paths: ModePaths,
    boundary_height: u32,
    publisher_opt: Option<ShieldedCatalogPublisher>,
    reader_pool: Arc<ReaderSecondaryPool>,
    history: Arc<PublishedShieldedHistory>,
    mempool: Arc<BenchmarkMempoolSurface>,
    tip_watcher_tx: watch::Sender<u64>,
    servers: ModeServers,
    channel: AuthenticatedChannel,
    tail_channel: AuthenticatedChannel,
    tail_lookup_events: Arc<Mutex<Vec<EncodedTailLookupEvent>>>,
    signals: Arc<FleetSignals>,
    fleet_spec: ReaderFleetSpec,
}

async fn spawn_mode_servers(
    slot: CanonicalReaderSlot,
    history: &Arc<PublishedShieldedHistory>,
    mempool: &Arc<BenchmarkMempoolSurface>,
    tip_watcher: zinder_compat_lightwalletd::SharedTipChangeWatcher,
    tail_lookup_events: &Arc<Mutex<Vec<EncodedTailLookupEvent>>>,
) -> Result<(ModeServers, AuthenticatedChannel, AuthenticatedChannel), BenchError> {
    let (fleet_addr, fleet_shutdown, fleet_task) = spawn_compat_grpc_server(CompatServerDeps {
        slot: slot.clone(),
        history: Arc::clone(history),
        mempool: Arc::clone(mempool),
        tip_watcher: Arc::clone(&tip_watcher),
        tail_observer: None,
    })
    .await?;
    let observer_sink = Arc::clone(tail_lookup_events);
    let tail_observer: EncodedTailLookupObserver =
        Arc::new(move |event| observer_sink.lock().push(event));
    let (tail_addr, tail_shutdown, tail_task) = match spawn_compat_grpc_server(CompatServerDeps {
        slot,
        history: Arc::clone(history),
        mempool: Arc::clone(mempool),
        tip_watcher,
        tail_observer: Some(tail_observer),
    })
    .await
    {
        Ok(spawned) => spawned,
        Err(err) => {
            let _ = fleet_shutdown.send(());
            let _ = fleet_task.await;
            return Err(err);
        }
    };
    let channel = connect_channel(fleet_addr).await?;
    let tail_channel = connect_channel(tail_addr).await?;
    Ok((
        ModeServers {
            fleet_shutdown,
            fleet_task,
            tail_shutdown,
            tail_task,
        },
        channel,
        tail_channel,
    ))
}

async fn bootstrap_mode_runtime(
    run_spec: ModeRunSpec,
    config: &ActiveWriterPublicationConfig,
    chain: &GeneratedChain,
    template: &TemplateStores,
) -> Result<ModeRuntimeBootstrap, BenchError> {
    let paths = ModePaths::prepare(template)?;
    let boundary_height = next_segment_boundary(config, chain);

    if run_spec.mode == WriterMode::QuiescentControl {
        pre_advance_quiescent_stores(&paths, config, chain, run_spec.publish_catalog).await?;
    }

    let publisher_opt = if run_spec.mode == WriterMode::QuiescentControl && run_spec.publish_catalog
    {
        None
    } else {
        let bootstrap_publisher = ShieldedCatalogPublisher::open(
            &paths.catalog_dir,
            &paths.publisher_priv_key,
            BlockHeight::new(1),
            config.segment_blocks,
        )
        .map_err(|source| BenchError::io(&paths.catalog_dir, source))?;
        if run_spec.mode == WriterMode::IngestProjectorAndPublisher {
            Some(bootstrap_publisher)
        } else {
            drop(bootstrap_publisher);
            None
        }
    };

    let reader_pool = ReaderSecondaryPool::open(&paths, chain)?;
    let initial_epoch = reader_pool.slot.capture().chain_epoch();
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
    let tail_lookup_events = Arc::new(Mutex::new(Vec::new()));
    let (servers, channel, tail_channel) = spawn_mode_servers(
        reader_pool.capture_slot(),
        &history,
        &mempool,
        tip_watcher,
        &tail_lookup_events,
    )
    .await?;

    let signals = FleetSignals::new(initial_tip, initial_epoch.id.value());
    let fleet_spec = ReaderFleetSpec {
        mode: run_spec.mode,
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
        servers,
        channel,
        tail_channel,
        tail_lookup_events,
        signals,
        fleet_spec,
    })
}

async fn shutdown_mode_runtime(servers: ModeServers, reader_pool: Arc<ReaderSecondaryPool>) {
    let _ = servers.fleet_shutdown.send(());
    let _ = servers.tail_shutdown.send(());
    let _ = servers.fleet_task.await;
    let _ = servers.tail_task.await;
    drop(reader_pool);
}

async fn run_mode_workload(
    run_spec: ModeRunSpec,
    config: &ActiveWriterPublicationConfig,
    chain: &GeneratedChain,
    boot: &mut ModeRuntimeBootstrap,
) -> Result<(ActiveOutcome, ReaderFleetOutput), BenchError> {
    let source = AdvancingChainSource::new(chain, chain.base_tip_height);
    run_mode_workload_with_source(run_spec, config, chain, boot, source).await
}

async fn run_mode_workload_with_source(
    run_spec: ModeRunSpec,
    config: &ActiveWriterPublicationConfig,
    chain: &GeneratedChain,
    boot: &mut ModeRuntimeBootstrap,
    source: AdvancingChainSource,
) -> Result<(ActiveOutcome, ReaderFleetOutput), BenchError> {
    let fleet_handle = spawn_reader_fleet(
        ReaderFleetChannels {
            fleet: boot.channel.clone(),
            tail: boot.tail_channel.clone(),
            tail_lookup_events: Arc::clone(&boot.tail_lookup_events),
        },
        boot.fleet_spec.clone(),
        Arc::clone(&boot.signals),
        Arc::clone(&boot.mempool),
        Arc::clone(&boot.history),
    );
    let publisher = boot.publisher_opt.take();
    let active_outcome = if run_spec.mode == WriterMode::QuiescentControl {
        drop(publisher);
        ActiveOutcome::default()
    } else {
        match drive_active_pipelines(
            run_spec.mode,
            config,
            chain,
            &boot.paths,
            PipelineShared {
                reader_pool: &boot.reader_pool,
                history: &boot.history,
                mempool: &boot.mempool,
                tip_watcher_tx: &boot.tip_watcher_tx,
                signals: &boot.signals,
                publisher,
                source,
            },
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                boot.signals.mark_writer_commits_done();
                boot.signals.writer_done.store(true, Ordering::Release);
                fleet_handle.cancel_and_drain().await;
                return Err(err);
            }
        }
    };

    boot.signals.writer_done.store(true, Ordering::Release);
    let fleet_output = fleet_handle.join().await?;
    Ok((active_outcome, fleet_output))
}

async fn run_single_mode(
    run_spec: ModeRunSpec,
    config: &ActiveWriterPublicationConfig,
    chain: &GeneratedChain,
    template: &TemplateStores,
) -> Result<(ModeReport, ModeParityDigests), BenchError> {
    let mut boot = bootstrap_mode_runtime(run_spec, config, chain, template).await?;
    let workload_started = Instant::now();
    let workload_res = run_mode_workload(run_spec, config, chain, &mut boot).await;
    let (active_outcome, fleet_output) = match workload_res {
        Ok(outputs) => outputs,
        Err(err) => {
            shutdown_mode_runtime(boot.servers, boot.reader_pool).await;
            return Err(err);
        }
    };
    let workload_duration_ms = workload_started.elapsed().as_secs_f64() * 1_000.0;

    let parity_res = collect_final_tip_parity(
        boot.channel,
        &ParityProbeRequest {
            mode: run_spec.mode,
            label: run_spec.label,
            base_tip_height: config.base_blocks,
            final_tip_height: chain.final_tip_height.value(),
            segment_boundary_height: boot.boundary_height,
        },
    )
    .await;
    let (reader_io_modes, reader_secondary_delta) = boot.reader_pool.collect_metrics();
    shutdown_mode_runtime(boot.servers, boot.reader_pool).await;
    let parity = parity_res?;

    let report = assemble_mode_report(ModeReportAssembly {
        mode: run_spec.mode,
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
    source: AdvancingChainSource,
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

async fn cancel_and_drain_active(
    handles: ActiveHandles,
    writer_finished: &AtomicBool,
    commit_wake: &Notify,
) {
    handles.cancel.cancel();
    writer_finished.store(true, Ordering::Release);
    commit_wake.notify_waiters();
    let _ = handles.reader_task.await;
    if let Some(handle) = handles.projector_handle {
        let _ = handle.await;
    }
    if let Some(handle) = handles.publisher_handle {
        let _ = handle.await;
    }
}

async fn spawn_active_background_tasks(
    mode: WriterMode,
    chain: &GeneratedChain,
    paths: &ModePaths,
    canonical_io_mode: String,
    shared: PipelineShared<'_>,
) -> Result<(ActiveHandles, Arc<Notify>, Arc<AtomicBool>), BenchError> {
    let cancel = CancellationToken::new();
    let commit_wake = Arc::new(Notify::new());
    let writer_finished = Arc::new(AtomicBool::new(false));
    let reader_task = spawn_reader_catchup_loop(ReaderCatchupInputs {
        pool: Arc::clone(shared.reader_pool),
        commit_wake: Arc::clone(&commit_wake),
        writer_finished: Arc::clone(&writer_finished),
        cancel: cancel.clone(),
        signals: Arc::clone(shared.signals),
        mempool: Arc::clone(shared.mempool),
        tip_watcher_tx: shared.tip_watcher_tx.clone(),
        final_tip_height: chain.final_tip_height,
    });
    let mut handles = ActiveHandles {
        canonical_io_mode,
        reader_task,
        projector_handle: None,
        publisher_handle: None,
        cancel: cancel.clone(),
    };

    let init_res: Result<(), BenchError> = (|| {
        if matches!(
            mode,
            WriterMode::IngestAndProjector | WriterMode::IngestProjectorAndPublisher
        ) {
            handles.projector_handle = Some(spawn_projector_loop(
                paths,
                chain,
                Arc::clone(&commit_wake),
                Arc::clone(&writer_finished),
                cancel.clone(),
            )?);
        }
        if let Some(publisher) = shared.publisher {
            handles.publisher_handle = Some(spawn_publisher_loop(
                paths,
                chain,
                publisher,
                PublisherLoopSync {
                    reader_slot: shared.reader_pool.capture_slot(),
                    history: Arc::clone(shared.history),
                    commit_wake: Arc::clone(&commit_wake),
                    writer_finished: Arc::clone(&writer_finished),
                    cancel,
                },
            )?);
        }
        Ok(())
    })();

    if let Err(err) = init_res {
        cancel_and_drain_active(handles, &writer_finished, &commit_wake).await;
        return Err(err);
    }
    Ok((handles, commit_wake, writer_finished))
}

async fn drive_active_pipelines(
    mode: WriterMode,
    config: &ActiveWriterPublicationConfig,
    chain: &GeneratedChain,
    paths: &ModePaths,
    shared: PipelineShared<'_>,
) -> Result<ActiveOutcome, BenchError> {
    let source = shared.source.clone();
    let (canonical_store, canonical_io_mode, activations) =
        open_active_canonical_store(paths, chain)?;
    let signals = Arc::clone(shared.signals);
    let (handles, commit_wake, writer_finished) =
        spawn_active_background_tasks(mode, chain, paths, canonical_io_mode, shared).await?;

    let writer_outcome = run_writer_thread(
        canonical_store,
        source.clone(),
        activations,
        WriterThreadSpec {
            start_height: chain.base_tip_height.value().saturating_add(1),
            end_height: chain.final_tip_height.value(),
            commit_interval: config.commit_interval,
            signals,
            commit_wake: Arc::clone(&commit_wake),
        },
    )
    .await;

    let writer_output = match writer_outcome {
        Ok(output) => {
            writer_finished.store(true, Ordering::Release);
            commit_wake.notify_waiters();
            output
        }
        Err(err) => {
            cancel_and_drain_active(handles, &writer_finished, &commit_wake).await;
            return Err(err);
        }
    };

    finalize_active_outcome(handles, writer_output, source.rpc_counts()).await
}

async fn finalize_active_outcome(
    handles: ActiveHandles,
    writer_output: WriterThreadOutput,
    node_rpcs: super::report::NodeRpcCounts,
) -> Result<ActiveOutcome, BenchError> {
    let reader_outcome = handles.reader_task.await;
    if reader_outcome.as_ref().is_err() || reader_outcome.as_ref().is_ok_and(Result::is_err) {
        handles.cancel.cancel();
    }
    let projector_outcome = match handles.projector_handle {
        Some(handle) => Some(handle.await),
        None => None,
    };
    if projector_outcome
        .as_ref()
        .is_some_and(|res| res.is_err() || res.as_ref().is_err())
    {
        handles.cancel.cancel();
    }
    let publisher_outcome = match handles.publisher_handle {
        Some(handle) => Some(handle.await),
        None => None,
    };
    handles.cancel.cancel();

    reader_outcome.map_err(|err| BenchError::tip_replay(err.to_string()))??;
    let projector_out = projector_outcome
        .map(|res| res.map_err(|err| BenchError::tip_replay(err.to_string()))?)
        .transpose()?;
    let publisher_out = publisher_outcome
        .map(|res| res.map_err(|err| BenchError::tip_replay(err.to_string()))?)
        .transpose()?;

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
        let outcome = execute_writer_commit_loop(store, &source, &activations, &spec);
        spec.signals.mark_writer_commits_done();
        let _ = done_tx.send(outcome);
    });

    done_rx
        .await
        .map_err(|_| BenchError::tip_replay("writer thread terminated unexpectedly"))?
}

fn execute_writer_commit_loop(
    store: RocksDbCanonicalStore,
    source: &AdvancingChainSource,
    activations: &Arc<NetworkUpgradeActivations>,
    spec: &WriterThreadSpec,
) -> Result<WriterThreadOutput, BenchError> {
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
        #[cfg(test)]
        source.check_injected_failure(raw_height)?;
        let target = BlockHeight::new(raw_height);
        source.advance_tip_to(target);
        recorder.clear();

        let follower = CanonicalFollower::new(
            source,
            Arc::clone(activations),
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
    cancel: CancellationToken,
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
            if cancel.is_cancelled() {
                break;
            }
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
            if cancel.is_cancelled() {
                break;
            }
            if drained == 0 {
                std::thread::sleep(Duration::from_millis(2));
            }
            let _ = &commit_wake;
        }

        if cancel.is_cancelled() {
            drop(wallet);
            drop(secondary);
            return Err(BenchError::tip_replay(
                "projector cancelled after upstream pipeline abort",
            ));
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

struct PublisherStepInput {
    secondary: RocksDbCanonicalSecondary,
    publisher: ShieldedCatalogPublisher,
    catalog_label: String,
}

struct PublisherStepOutcome {
    secondary: RocksDbCanonicalSecondary,
    publisher: ShieldedCatalogPublisher,
    settled_tip: BlockHeight,
    elapsed: Option<Duration>,
    sealed: usize,
}

async fn run_publisher_blocking_step(
    input: PublisherStepInput,
) -> Result<PublisherStepOutcome, BenchError> {
    tokio::task::spawn_blocking(move || {
        let PublisherStepInput {
            mut secondary,
            mut publisher,
            catalog_label,
        } = input;
        secondary.try_catch_up()?;
        let settled_tip = secondary.chain_epoch()?.settled_tip_height;
        let canonical_reader = RocksDbCanonicalReader::new(secondary)
            .map_err(|err| BenchError::canonical_read(err.to_string()))?;
        let concurrency = PublicationConcurrency {
            workers: NonZeroUsize::MIN,
            commit_batch: NonZeroUsize::MIN,
        };
        let started = Instant::now();
        let sealed_outcome =
            publisher.publish_available(&canonical_reader, concurrency, &CancellationToken::new());
        let recovered_sec = canonical_reader
            .into_secondary()
            .map_err(|_| BenchError::tip_replay("publisher secondary held unexpectedly"))?;
        let sealed = sealed_outcome.map_err(|err| BenchError::io(&catalog_label, err))?;
        let elapsed = (sealed > 0).then(|| started.elapsed());
        Ok(PublisherStepOutcome {
            secondary: recovered_sec,
            publisher,
            settled_tip,
            elapsed,
            sealed,
        })
    })
    .await
    .map_err(|err| BenchError::tip_replay(err.to_string()))?
}

async fn refresh_pending_catalog(
    sync: &PublisherLoopSync,
    precondition_retries: &mut u32,
    refreshes: &mut u64,
) -> Result<bool, BenchError> {
    match sync
        .history
        .refresh_captured(|| CapturedCanonical(sync.reader_slot.capture()))
        .await
    {
        Ok(swapped) => {
            *precondition_retries = 0;
            if swapped {
                *refreshes = refreshes.saturating_add(1);
            }
            Ok(false)
        }
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            *precondition_retries = precondition_retries.saturating_add(1);
            if *precondition_retries >= MAX_CATALOG_REFRESH_RETRIES {
                return Err(BenchError::canonical_read(status.to_string()));
            }
            Ok(true)
        }
        Err(status) => Err(BenchError::canonical_read(status.to_string())),
    }
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
    let mut pending_refresh = false;
    let mut precondition_retries = 0_u32;

    loop {
        if sync.cancel.is_cancelled() {
            break;
        }
        let step = run_publisher_blocking_step(PublisherStepInput {
            secondary: active_sec,
            publisher,
            catalog_label: paths_catalog_label(&sync),
        })
        .await?;
        active_sec = step.secondary;
        publisher = step.publisher;

        if let Some(elapsed) = step.elapsed {
            publish_durations.push(elapsed);
            total_published = total_published.saturating_add(step.sealed);
            pending_refresh = true;
        }
        if pending_refresh {
            pending_refresh =
                refresh_pending_catalog(&sync, &mut precondition_retries, &mut refreshes).await?;
        }

        if !pending_refresh
            && sync.writer_finished.load(Ordering::Acquire)
            && step.settled_tip >= final_settled_height
        {
            break;
        }
        tokio::select! {
            () = sync.commit_wake.notified() => {}
            () = tokio::time::sleep(Duration::from_millis(2)) => {}
            () = sync.cancel.cancelled() => break,
        }
    }

    let delta = tokio::task::spawn_blocking(move || {
        let delta = capture_secondary_tickers(&active_sec).delta_since(&before);
        drop(active_sec);
        delta
    })
    .await
    .map_err(|err| BenchError::tip_replay(err.to_string()))?;
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

    async fn try_catch_up_and_publish(self: &Arc<Self>) -> Result<(u32, ChainEpoch), BenchError> {
        let pool = Arc::clone(self);
        tokio::task::spawn_blocking(move || pool.try_catch_up_and_publish_blocking())
            .await
            .map_err(|err| BenchError::tip_replay(err.to_string()))?
    }

    fn try_catch_up_and_publish_blocking(&self) -> Result<(u32, ChainEpoch), BenchError> {
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
                Ok(sole) => match sole.into_secondary() {
                    Ok(sec) => self.spares.lock().push(sec),
                    Err(still_busy) => remaining.push(Arc::new(still_busy)),
                },
                Err(shared) => remaining.push(shared),
            }
        }
        *retired = remaining;
    }

    fn return_or_retire(&self, old_reader: Arc<RocksDbCanonicalReader>) {
        match Arc::try_unwrap(old_reader) {
            Ok(sole) => match sole.into_secondary() {
                Ok(sec) => self.spares.lock().push(sec),
                Err(still_busy) => self.retired.lock().push(Arc::new(still_busy)),
            },
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
    cancel: CancellationToken,
    signals: Arc<FleetSignals>,
    mempool: Arc<BenchmarkMempoolSurface>,
    tip_watcher_tx: watch::Sender<u64>,
    final_tip_height: BlockHeight,
}

fn spawn_reader_catchup_loop(inputs: ReaderCatchupInputs) -> JoinHandle<Result<(), BenchError>> {
    tokio::spawn(async move {
        let mut last_published_tip = 0_u32;
        let mut last_epoch_id = 0_u64;
        loop {
            if inputs.cancel.is_cancelled() {
                break;
            }
            let (tip, epoch) = inputs.pool.try_catch_up_and_publish().await?;
            let epoch_id = epoch.id.value();
            if tip > last_published_tip || epoch_id > last_epoch_id {
                last_published_tip = last_published_tip.max(tip);
                last_epoch_id = last_epoch_id.max(epoch_id);
                inputs
                    .signals
                    .highest_served_tip
                    .fetch_max(tip, Ordering::AcqRel);
                inputs
                    .signals
                    .current_epoch_id
                    .store(epoch_id, Ordering::Release);
                inputs.signals.tip_published_wake.notify_waiters();
                inputs.mempool.advance_epoch(epoch);
                inputs.tip_watcher_tx.send_replace(epoch_id);
            }
            if inputs.writer_finished.load(Ordering::Acquire)
                && tip >= inputs.final_tip_height.value()
            {
                break;
            }
            tokio::select! {
                () = inputs.commit_wake.notified() => {}
                () = tokio::time::sleep(Duration::from_millis(2)) => {}
                () = inputs.cancel.cancelled() => break,
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
    publish_catalog: bool,
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

    if publish_catalog {
        publish_quiescent_catalog(paths, config, secondary)?;
    } else {
        drop(secondary);
    }
    Ok(())
}

fn publish_quiescent_catalog(
    paths: &ModePaths,
    config: &ActiveWriterPublicationConfig,
    secondary: RocksDbCanonicalSecondary,
) -> Result<(), BenchError> {
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

struct CompatServerDeps {
    slot: CanonicalReaderSlot,
    history: Arc<PublishedShieldedHistory>,
    mempool: Arc<BenchmarkMempoolSurface>,
    tip_watcher: zinder_compat_lightwalletd::SharedTipChangeWatcher,
    tail_observer: Option<EncodedTailLookupObserver>,
}

async fn spawn_compat_grpc_server(
    deps: CompatServerDeps,
) -> Result<(SocketAddr, oneshot::Sender<()>, JoinHandle<()>), BenchError> {
    let dummy_node = ZebraJsonRpcSource::new(
        Network::ZcashRegtest,
        "http://127.0.0.1:1",
        NodeAuth::None,
        Duration::from_secs(5),
    )?;
    let mut adapter = LightwalletdGrpcAdapter::for_light_wallet(
        deps.slot,
        dummy_node,
        Duration::from_secs(2),
        NonZeroU32::new(4).unwrap_or(NonZeroU32::MIN),
        ForwardedReadLimit::default(),
    )
    .with_published_shielded_history(deps.history)
    .with_mempool_surface(as_mempool_surface(deps.mempool))
    .with_tip_change_watcher(deps.tip_watcher)
    .with_tip_wait(TipWait::default());
    if let Some(observer) = deps.tail_observer {
        adapter = adapter.with_tail_lookup_observer(observer);
    }

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|source| BenchError::io("127.0.0.1:0", source))?;
    let addr = listener
        .local_addr()
        .map_err(|source| BenchError::io("127.0.0.1:0", source))?;
    let incoming = tokio_stream::StreamExt::map(TcpListenerStream::new(listener), |accepted| {
        if let Ok(ref stream) = accepted {
            let _ = stream.set_nodelay(true);
        }
        accepted
    });
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming_shutdown(incoming, async move {
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
        catalog_range_unpublished: fleet.catalog_range_unpublished,
        catalog_range_published: fleet.catalog_range_published,
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

#[cfg(test)]
mod tests {
    use std::{error::Error, num::NonZeroU32, sync::atomic::Ordering, time::Duration};

    use super::{
        ActiveWriterPublicationConfig, AdvancingChainSource, ChainSpecConfig, GeneratedChain,
        ModeRunSpec, TemplateStores, WriterMode, bootstrap_mode_runtime,
        run_mode_workload_with_source, shutdown_mode_runtime, validate_config,
    };

    #[test]
    fn validate_config_rejects_short_live_window_before_segment_seal() {
        let config = ActiveWriterPublicationConfig {
            base_blocks: 4_092,
            live_blocks: 6,
            reorg_window_blocks: 4,
            segment_blocks: NonZeroU32::new(4_096).unwrap_or(NonZeroU32::MIN),
            ..ActiveWriterPublicationConfig::default()
        };
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn validate_config_rejects_zero_reorg_window() {
        let config = ActiveWriterPublicationConfig {
            base_blocks: 60,
            live_blocks: 12,
            reorg_window_blocks: 0,
            segment_blocks: NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN),
            ..ActiveWriterPublicationConfig::default()
        };
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn validate_config_rejects_fewer_live_blocks_than_mempool_tiers() {
        let config = ActiveWriterPublicationConfig {
            base_blocks: 67,
            live_blocks: 1,
            reorg_window_blocks: 4,
            segment_blocks: NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN),
            mempool_churn_sizes: vec![50, 500],
            ..ActiveWriterPublicationConfig::default()
        };
        assert!(validate_config(&config).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn writer_failure_cancels_and_drains_fleet_projector_publisher_and_server()
    -> Result<(), Box<dyn Error>> {
        let config = ActiveWriterPublicationConfig {
            base_blocks: 60,
            live_blocks: 12,
            reorg_window_blocks: 4,
            segment_blocks: NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN),
            dense_block_cadence: 3,
            dense_txs_per_block: 1,
            commit_interval: Duration::from_millis(8),
            slow_reader_frame_delay: Duration::from_millis(4),
            mempool_churn_sizes: vec![50, 500],
            quiescent_rounds: 3,
        };
        let chain = GeneratedChain::build(ChainSpecConfig {
            base_blocks: config.base_blocks,
            live_blocks: config.live_blocks,
            reorg_window_blocks: config.reorg_window_blocks,
            dense_block_cadence: config.dense_block_cadence,
            dense_txs_per_block: config.dense_txs_per_block,
        })?;
        let template = TemplateStores::build(&chain).await?;
        let run_spec = ModeRunSpec {
            mode: WriterMode::IngestProjectorAndPublisher,
            label: "ingest_projector_and_publisher",
            publish_catalog: true,
        };
        let mut boot = bootstrap_mode_runtime(run_spec, &config, &chain, &template).await?;
        let signals = std::sync::Arc::clone(&boot.signals);
        let source = AdvancingChainSource::new(&chain, chain.base_tip_height)
            .fail_at_height(config.base_blocks.saturating_add(2));

        let workload_res = tokio::time::timeout(Duration::from_secs(15), async {
            let outcome =
                run_mode_workload_with_source(run_spec, &config, &chain, &mut boot, source).await;
            shutdown_mode_runtime(boot.servers, boot.reader_pool).await;
            outcome
        })
        .await?;

        assert!(workload_res.is_err());
        assert!(signals.fleet_cancel.is_cancelled());
        assert!(signals.writer_commits_done.load(Ordering::Acquire));
        assert!(signals.writer_done.load(Ordering::Acquire));
        Ok(())
    }
}
