//! Orchestrator for the 4 matched `active-writer-publication` modes plus the
//! published quiescent baseline.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use metrics_exporter_prometheus::PrometheusHandle;
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use zinder_compat_lightwalletd::{PublishedShieldedHistory, ShieldedCatalogPublisher};
use zinder_core::{BlockHeight, ChainEpoch};
use zinder_query::CanonicalReader as _;
use zinder_runtime::{AuthenticatedChannel, connect_zinder_grpc_with_timeout};

use super::{
    ActiveWriterPublicationConfig,
    chain_fixture::{
        AdvancingChainSource, ChainSpecConfig, GeneratedChain, TemplateStores, reader_budget,
    },
    mempool::{BenchmarkMempoolSurface, as_mempool_surface, create_tip_watcher},
    parity::{ParityProbeRequest, collect_final_tip_parity},
    pipelines::{
        ActiveOutcome, ActivePipelineArgs, ModePaths, ModeServingHandle, QuiescentPreAdvance,
        ServedState, WriterProgress, drive_active_pipelines, pre_advance_quiescent_stores,
        resolve_publish_workers,
    },
    readers::{ErrorCollector, ReaderFleetContext, ReaderFleetOutput, spawn_reader_fleet},
    report::{
        ACTIVE_WRITER_PUBLICATION_CONTRACT, ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION,
        ActiveWriterPublicationReport, ByteParityReport, DifferentialBaselines, IoModeReport,
        ModeDifferential, ModeParityDigests, ModeReport, ProtocolErrorCounts,
        RocksDbActivityReport, StoreTickerDelta, TailRefillMetrics, WorkloadGeometryReport,
        WriterFsyncCounts, WriterMode,
    },
    serving::{
        CancelGuard, CapturedCanonical, CapturedWalletPair, EncodedTailMetricSnapshot,
        SupervisedGrpcServer, TwoGenCanonicalPublisher, TwoGenWalletPairPublisher,
        WalletPairOpenConfig,
    },
    trace::WorkloadTrace,
    validate_config,
};
use crate::BenchError;

#[derive(Clone, Copy)]
struct ModeRunSpec {
    mode: WriterMode,
    label: &'static str,
    publish_catalog: bool,
    wallet_serving: bool,
}

/// Runs all matched writer/publication arms across one advancing Regtest chain.
///
/// # Errors
///
/// Returns [`BenchError`] if fixture generation, store catch-up, concurrent
/// pipelines, gRPC serving, or final-tip byte parity checks fail.
pub async fn run_active_writer_publication(
    config: &ActiveWriterPublicationConfig,
) -> Result<ActiveWriterPublicationReport, BenchError> {
    validate_config(config)?;
    crate::recorder::install_tracing_subscriber();
    let chain = build_generated_chain(config)?;
    let template_stores = TemplateStores::build(&chain, config).await?;
    let segment_boundary = next_segment_boundary(config, &chain);
    let trace = WorkloadTrace::build(
        config,
        segment_boundary,
        chain.tracked_taddresses.clone(),
        chain.settled_txids.clone(),
        chain.live_txids.clone(),
    );
    let (prom_handle, _) = EncodedTailMetricSnapshot::install_and_capture()?;
    let shared = (&chain, &template_stores, &trace, &prom_handle);

    let (canonical_quiescent_control, canonical_parity) = Box::pin(run_single_mode(
        ModeRunSpec {
            mode: WriterMode::QuiescentControl,
            label: "canonical_quiescent_control",
            publish_catalog: false,
            wallet_serving: false,
        },
        config,
        shared,
    ))
    .await?;
    let (published_quiescent_control, published_parity) = Box::pin(run_single_mode(
        ModeRunSpec {
            mode: WriterMode::QuiescentControl,
            label: "published_quiescent_control",
            publish_catalog: true,
            wallet_serving: true,
        },
        config,
        shared,
    ))
    .await?;
    let (mode_reports, mut parity_digests) = Box::pin(run_matched_modes(
        config,
        shared,
        (canonical_parity, published_parity),
    ))
    .await?;

    let unpublished_baseline = mode_reports
        .first()
        .ok_or_else(|| BenchError::tip_replay("quiescent_control report missing"))?;
    let differentials_vs_quiescent = build_matched_differentials(
        &canonical_quiescent_control,
        unpublished_baseline,
        &published_quiescent_control,
        &mode_reports[1..],
    );
    let byte_parity = ByteParityReport::evaluate(std::mem::take(&mut parity_digests));
    let workload = build_workload_geometry(config, &chain, segment_boundary, &trace.digest_hex);

    Ok(ActiveWriterPublicationReport {
        contract_identity: ACTIVE_WRITER_PUBLICATION_CONTRACT.to_owned(),
        report_format_version: ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION,
        workload,
        canonical_quiescent_control,
        published_quiescent_control,
        modes: mode_reports,
        differentials_vs_quiescent,
        byte_parity,
    })
}

fn build_generated_chain(
    config: &ActiveWriterPublicationConfig,
) -> Result<GeneratedChain, BenchError> {
    GeneratedChain::build(ChainSpecConfig {
        base_blocks: config.base_blocks,
        live_blocks: config.live_blocks,
        reorg_window_blocks: config.reorg_window_blocks,
        dense_block_cadence: config.dense_block_cadence,
        dense_txs_per_block: config.dense_txs_per_block,
        dense_orchard_actions_per_tx: config.dense_orchard_actions_per_tx,
    })
}

async fn run_matched_modes(
    config: &ActiveWriterPublicationConfig,
    shared: (
        &GeneratedChain,
        &TemplateStores,
        &WorkloadTrace,
        &PrometheusHandle,
    ),
    (canonical_parity, published_parity): (ModeParityDigests, ModeParityDigests),
) -> Result<(Vec<ModeReport>, Vec<ModeParityDigests>), BenchError> {
    let mut mode_reports = Vec::with_capacity(4);
    let mut parity_digests = Vec::with_capacity(6);
    parity_digests.push(canonical_parity);
    parity_digests.push(published_parity);

    for run_spec in [
        ModeRunSpec {
            mode: WriterMode::QuiescentControl,
            label: "quiescent_control",
            publish_catalog: false,
            wallet_serving: true,
        },
        ModeRunSpec {
            mode: WriterMode::IngestActive,
            label: "ingest_active",
            publish_catalog: false,
            wallet_serving: false,
        },
        ModeRunSpec {
            mode: WriterMode::IngestAndProjector,
            label: "ingest_and_projector",
            publish_catalog: false,
            wallet_serving: true,
        },
        ModeRunSpec {
            mode: WriterMode::IngestProjectorAndPublisher,
            label: "ingest_projector_and_publisher",
            publish_catalog: true,
            wallet_serving: true,
        },
    ] {
        let (mode_report, parity) = run_single_mode(run_spec, config, shared).await?;
        mode_reports.push(mode_report);
        parity_digests.push(parity);
    }
    Ok((mode_reports, parity_digests))
}

fn build_matched_differentials(
    canonical_baseline: &ModeReport,
    unpublished_baseline: &ModeReport,
    published_baseline: &ModeReport,
    active_modes: &[ModeReport],
) -> Vec<ModeDifferential> {
    active_modes
        .iter()
        .map(|candidate| {
            let (baseline_label, baseline_catalog_published, unpublished) = match candidate.mode {
                WriterMode::IngestActive => {
                    ("canonical_quiescent_control", false, canonical_baseline)
                }
                WriterMode::IngestAndProjector | WriterMode::QuiescentControl => {
                    ("quiescent_control", false, unpublished_baseline)
                }
                WriterMode::IngestProjectorAndPublisher => {
                    ("published_quiescent_control", true, unpublished_baseline)
                }
            };
            ModeDifferential::between(
                DifferentialBaselines {
                    unpublished,
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
    crossed_boundary: u32,
    trace_digest_hex: &str,
) -> WorkloadGeometryReport {
    let publish_workers =
        u32::try_from(resolve_publish_workers(config.publish_workers).get()).unwrap_or(1);
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
        dense_orchard_actions_per_tx: config.dense_orchard_actions_per_tx,
        transparent_spend_txs: chain.transparent_spend_txs,
        tail_probe_blocks: config.tail_probe_blocks,
        requested_io_mode: config.io_mode.as_str().to_owned(),
        publish_workers,
        mempool_churn_sizes: config.mempool_churn_sizes.clone(),
        request_trace_digest: trace_digest_hex.to_owned(),
    }
}

struct ModeRuntimeBootstrap {
    paths: ModePaths,
    boundary_height: u32,
    publisher_opt: Option<ShieldedCatalogPublisher>,
    serving: ModeServingHandle,
    initial_epoch: ChainEpoch,
    initial_catalog_end: Option<u32>,
    history: Arc<PublishedShieldedHistory>,
    mempool: Arc<BenchmarkMempoolSurface>,
    tip_watcher_tx: watch::Sender<u64>,
    server: SupervisedGrpcServer,
    channel: AuthenticatedChannel,
}

async fn bootstrap_mode_runtime(
    run_spec: ModeRunSpec,
    config: &ActiveWriterPublicationConfig,
    chain: &GeneratedChain,
    template: &TemplateStores,
) -> Result<ModeRuntimeBootstrap, BenchError> {
    let paths = ModePaths::prepare(template, config.scratch_dir.as_deref(), run_spec.label)?;
    let boundary_height = next_segment_boundary(config, chain);

    if run_spec.mode == WriterMode::QuiescentControl {
        pre_advance_quiescent_stores(
            &paths,
            config,
            chain,
            QuiescentPreAdvance {
                publish_catalog: run_spec.publish_catalog,
                advance_wallet: run_spec.wallet_serving,
            },
        )
        .await?;
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

    let (serving, initial_epoch) = open_mode_serving(&run_spec, config, chain, &paths)?;
    let initial_catalog_end = (run_spec.mode == WriterMode::QuiescentControl
        && run_spec.publish_catalog)
        .then_some(boundary_height);
    let history = Arc::new(open_history(&paths, &serving).await?);
    let mempool = BenchmarkMempoolSurface::new(initial_epoch);
    let (tip_watcher_tx, tip_watcher) = create_tip_watcher(initial_epoch.id);
    let server = match &serving {
        ModeServingHandle::CanonicalOnly(can) => {
            SupervisedGrpcServer::spawn_canonical_only(
                can.capture_slot(),
                Arc::clone(&history),
                as_mempool_surface(Arc::clone(&mempool)),
                tip_watcher,
            )
            .await?
        }
        ModeServingHandle::WalletPair(wal) => {
            SupervisedGrpcServer::spawn_wallet_serving(
                wal.query(),
                Arc::clone(&history),
                as_mempool_surface(Arc::clone(&mempool)),
                tip_watcher,
            )
            .await?
        }
    };
    let channel = connect_channel(server.addr).await?;

    Ok(ModeRuntimeBootstrap {
        paths,
        boundary_height,
        publisher_opt,
        serving,
        initial_epoch,
        initial_catalog_end,
        history,
        mempool,
        tip_watcher_tx,
        server,
        channel,
    })
}

fn open_mode_serving(
    run_spec: &ModeRunSpec,
    config: &ActiveWriterPublicationConfig,
    chain: &GeneratedChain,
    paths: &ModePaths,
) -> Result<(ModeServingHandle, ChainEpoch), BenchError> {
    let budget = reader_budget(config.io_mode);
    if run_spec.wallet_serving {
        let publisher = TwoGenWalletPairPublisher::open(WalletPairOpenConfig {
            canonical_dir: &paths.canonical_dir,
            wallet_dir: &paths.wallet_dir,
            secondary_dirs: &paths.canonical_sec_dirs,
            wallet_secondary_dirs: &paths.wallet_sec_dirs,
            reorg_window_blocks: chain.reorg_window_blocks,
            budget,
        })?;
        let initial_epoch = publisher.query().capture_pair().chain_epoch();
        Ok((ModeServingHandle::WalletPair(publisher), initial_epoch))
    } else {
        let publisher = TwoGenCanonicalPublisher::open(
            &paths.canonical_dir,
            &paths.canonical_sec_dirs,
            chain.reorg_window_blocks,
            budget,
        )?;
        let initial_epoch = publisher.capture_slot().capture().chain_epoch();
        Ok((ModeServingHandle::CanonicalOnly(publisher), initial_epoch))
    }
}

async fn open_history(
    paths: &ModePaths,
    serving: &ModeServingHandle,
) -> Result<PublishedShieldedHistory, BenchError> {
    match serving {
        ModeServingHandle::CanonicalOnly(can) => {
            let slot = can.capture_slot();
            PublishedShieldedHistory::follow_publisher_captured(
                &paths.catalog_dir,
                &paths.publisher_pub_key,
                false,
                || CapturedCanonical(slot.capture()),
            )
            .await
        }
        ModeServingHandle::WalletPair(wal) => {
            let query = wal.query();
            PublishedShieldedHistory::follow_publisher_captured(
                &paths.catalog_dir,
                &paths.publisher_pub_key,
                false,
                || CapturedWalletPair(query.capture_pair()),
            )
            .await
        }
    }
    .map_err(|status| BenchError::canonical_read(status.to_string()))
}

async fn run_single_mode(
    run_spec: ModeRunSpec,
    config: &ActiveWriterPublicationConfig,
    (chain, template, trace, prom_handle): (
        &GeneratedChain,
        &TemplateStores,
        &WorkloadTrace,
        &PrometheusHandle,
    ),
) -> Result<(ModeReport, ModeParityDigests), BenchError> {
    let mut boot = bootstrap_mode_runtime(run_spec, config, chain, template).await?;
    let source = AdvancingChainSource::new(chain, chain.base_tip_height);
    let _ = boot.serving.snapshot_and_reset_tickers();
    let tail_prom_before = EncodedTailMetricSnapshot::capture(prom_handle);
    let workload_started = Instant::now();
    let (active_outcome, fleet_output, errors) =
        match run_mode_workload_with_source(ModeWorkloadInputs {
            run_spec,
            config,
            chain,
            boot: &mut boot,
            source,
            trace,
            prom_handle,
        })
        .await
        {
            Ok(outputs) => outputs,
            Err(err) => {
                shutdown_mode_runtime(boot.server, &boot.serving).await;
                return Err(err);
            }
        };
    let workload_duration_ms = workload_started.elapsed().as_secs_f64() * 1_000.0;
    let tail_prom_after = EncodedTailMetricSnapshot::capture(prom_handle);
    let (can_reader_delta, wal_reader_delta) = boot.serving.snapshot_and_reset_tickers();
    let (can_io_modes, wal_io_modes) = boot.serving.io_modes();

    if run_spec.mode != WriterMode::QuiescentControl {
        boot.serving.wait_reclaim_all().await;
        boot.serving.force_rotate_to_latest().await?;
    }

    let probe_txids = collect_probe_txids(chain);
    let parity_res = collect_final_tip_parity(
        boot.channel,
        &ParityProbeRequest {
            mode: run_spec.mode,
            label: run_spec.label,
            wallet_serving: run_spec.wallet_serving,
            base_tip_height: config.base_blocks,
            final_tip_height: chain.final_tip_height.value(),
            segment_boundary_height: boot.boundary_height,
            wallet_addresses: &chain.tracked_taddresses,
            probe_txids: &probe_txids,
        },
    )
    .await;
    let server_res = boot.server.shutdown().await;
    boot.serving.wait_reclaim_all().await;
    drop(boot.serving);
    drop(boot.history);
    let parity = parity_res?;
    server_res?;

    let tail_delta = tail_prom_after.delta_since(&tail_prom_before);
    let report = assemble_mode_report(ModeReportAssembly {
        mode: run_spec.mode,
        workload_duration_ms,
        can_io_modes,
        wal_io_modes,
        can_reader_delta,
        wal_reader_delta,
        tail_delta,
        fleet: fleet_output,
        active: active_outcome,
        errors,
    });
    Ok((report, parity))
}

struct ModeWorkloadInputs<'a> {
    run_spec: ModeRunSpec,
    config: &'a ActiveWriterPublicationConfig,
    chain: &'a GeneratedChain,
    boot: &'a mut ModeRuntimeBootstrap,
    source: AdvancingChainSource,
    trace: &'a WorkloadTrace,
    prom_handle: &'a PrometheusHandle,
}

async fn run_mode_workload_with_source(
    inputs: ModeWorkloadInputs<'_>,
) -> Result<(ActiveOutcome, ReaderFleetOutput, ProtocolErrorCounts), BenchError> {
    let cancel = CancellationToken::new();
    let _cancel_guard = CancelGuard(cancel.clone());
    let (writer_progress_tx, writer_rx) = watch::channel(WriterProgress::default());
    let initial_served = ServedState {
        tip_height: inputs.boot.initial_epoch.visible_tip_height.value(),
        epoch_id: inputs.boot.initial_epoch.id.value(),
        generation_seq: 1,
        catalog_end_height: inputs.boot.initial_catalog_end,
        catalog_generation: u64::from(inputs.boot.initial_catalog_end.is_some()),
        serving_caught_up_done: inputs.run_spec.mode == WriterMode::QuiescentControl,
    };
    let (served_tx, served_rx) = watch::channel(initial_served);
    let commit_instants = Arc::new(Mutex::new(BTreeMap::new()));
    let error_collector = Arc::new(Mutex::new(ErrorCollector::default()));
    let quiescent_rotator =
        (inputs.run_spec.mode == WriterMode::QuiescentControl).then(|| inputs.boot.serving.clone());

    let fleet_handle = spawn_reader_fleet(
        inputs.boot.channel.clone(),
        ReaderFleetContext {
            mode: inputs.run_spec.mode,
            wallet_serving: inputs.run_spec.wallet_serving,
            trace: inputs.trace.clone(),
            sample_v5_tx: Arc::clone(&inputs.chain.sample_v5_tx),
            served_rx,
            writer_rx,
            commit_instants: Arc::clone(&commit_instants),
            cancel: cancel.clone(),
            error_collector: Arc::clone(&error_collector),
            prom_handle: inputs.prom_handle.clone(),
            server_port: inputs.boot.server.addr.port(),
            quiescent_rotator,
            rotation_requester: Some(inputs.boot.serving.clone()),
        },
        Arc::clone(&inputs.boot.mempool),
    );

    let publisher = inputs.boot.publisher_opt.take();
    let active_outcome = if inputs.run_spec.mode == WriterMode::QuiescentControl {
        drop(publisher);
        ActiveOutcome::default()
    } else {
        match drive_active_pipelines(ActivePipelineArgs {
            mode: inputs.run_spec.mode,
            config: inputs.config,
            chain: inputs.chain,
            paths: &inputs.boot.paths,
            serving: inputs.boot.serving.clone(),
            history: &inputs.boot.history,
            mempool: &inputs.boot.mempool,
            tip_watcher_tx: &inputs.boot.tip_watcher_tx,
            publisher,
            source: inputs.source,
            writer_progress_tx,
            served_tx,
            commit_instants,
            cancel: cancel.clone(),
        })
        .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                fleet_handle.cancel_and_drain().await;
                return Err(err);
            }
        }
    };

    let fleet_output = fleet_handle.join().await?;
    let errors = error_collector.lock().to_report();
    Ok((active_outcome, fleet_output, errors))
}

async fn shutdown_mode_runtime(server: SupervisedGrpcServer, serving: &ModeServingHandle) {
    let _ = server.shutdown().await;
    serving.wait_reclaim_all().await;
}

async fn connect_channel(addr: std::net::SocketAddr) -> Result<AuthenticatedChannel, BenchError> {
    let url = format!("http://{addr}");
    connect_zinder_grpc_with_timeout(&url, None, Duration::from_secs(2))
        .await
        .map_err(|err| BenchError::canonical_read(err.to_string()))
}

struct ModeReportAssembly {
    mode: WriterMode,
    workload_duration_ms: f64,
    can_io_modes: Vec<String>,
    wal_io_modes: Vec<String>,
    can_reader_delta: StoreTickerDelta,
    wal_reader_delta: StoreTickerDelta,
    tail_delta: super::serving::EncodedTailMetricDelta,
    fleet: ReaderFleetOutput,
    active: ActiveOutcome,
    errors: ProtocolErrorCounts,
}

fn assemble_mode_report(assembly: ModeReportAssembly) -> ModeReport {
    let ModeReportAssembly {
        mode,
        workload_duration_ms,
        can_io_modes,
        wal_io_modes,
        can_reader_delta,
        wal_reader_delta,
        tail_delta,
        fleet,
        active,
        errors,
    } = assembly;
    let byte_cap_exceeded = tail_delta.byte_capped_fills > 0 || tail_delta.byte_cap_evictions > 0;
    ModeReport {
        mode,
        workload_duration_ms,
        io_modes: IoModeReport {
            canonical_primary: active.canonical_io_mode,
            canonical_secondaries: can_io_modes,
            wallet_primary: active.wallet_io_mode,
            wallet_secondaries: wal_io_modes,
            projector_secondary: active.projector_secondary_io_mode,
            publisher_secondary: active.publisher_io_mode,
        },
        ingest: active.ingest,
        latest_block_rpc_ms: fleet.latest_block_rpc_ms,
        commit_to_served_tip_ms: fleet.commit_to_served_tip_ms,
        catalog_range: fleet.catalog_range,
        catalog_range_unpublished: fleet.catalog_range_unpublished,
        catalog_range_published: fleet.catalog_range_published,
        catalog_range_during_seal: fleet.catalog_range_during_seal,
        tail_refill: TailRefillMetrics {
            cold_after_advance: fleet.tail_cold_after_advance,
            warm_repeat: fleet.tail_warm_repeat,
            unpublished_cold_after_advance: fleet.tail_unpublished_cold_after_advance,
            unpublished_warm_repeat: fleet.tail_unpublished_warm_repeat,
            published_cold_after_advance: fleet.tail_published_cold_after_advance,
            published_warm_repeat: fleet.tail_published_warm_repeat,
            published_mismatched_span_pairs: fleet.published_mismatched_span_pairs,
            peak_tail_response_bytes: fleet.peak_tail_response_bytes,
            peak_encoded_tail_bytes: fleet.peak_encoded_tail_bytes,
            encoded_tail_byte_cap_evictions: tail_delta.byte_cap_evictions,
            encoded_tail_byte_capped_fills: tail_delta.byte_capped_fills,
            encoded_tail_cache_fills: tail_delta.fills,
            encoded_tail_cache_hits: tail_delta.hits,
            encoded_tail_fallback_native: tail_delta.fallback_native,
            encoded_tail_bypass: tail_delta.bypass,
            unpaired_tail_probes: fleet.unpaired_tail_probes,
            byte_cap_exceeded,
        },
        slow_boundary_range: fleet.slow_boundary_range,
        tree_state_ms: fleet.tree_state_ms,
        wallet_rpc_ms: fleet.wallet_rpc_ms,
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
            reader_canonical_secondaries: can_reader_delta,
            reader_wallet_secondaries: wal_reader_delta,
            wallet_writer: active.wallet_delta,
            projector_secondary: active.projector_secondary_delta,
            publisher_secondary: active.publisher_delta,
        },
        errors,
    }
}

fn collect_probe_txids(chain: &GeneratedChain) -> Vec<[u8; 32]> {
    let mut txids = Vec::with_capacity(6);
    if let Some(&first) = chain.settled_txids.first() {
        txids.push(first);
    }
    if let Some(&last) = chain.settled_txids.last()
        && !txids.contains(&last)
    {
        txids.push(last);
    }
    for &(_, txid) in chain.live_txids.iter().take(4) {
        if !txids.contains(&txid) {
            txids.push(txid);
        }
    }
    txids
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

#[cfg(test)]
mod tests {
    use std::{error::Error, num::NonZeroU32, time::Duration};

    use zinder_store::RocksDbIoMode;

    use super::{
        ActiveWriterPublicationConfig, AdvancingChainSource, ChainSpecConfig,
        EncodedTailMetricSnapshot, GeneratedChain, ModeRunSpec, TemplateStores, WorkloadTrace,
        WriterMode, bootstrap_mode_runtime, build_matched_differentials, next_segment_boundary,
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
    fn validate_config_rejects_tail_probe_blocks_below_two() {
        for width in [0, 1] {
            let config = ActiveWriterPublicationConfig {
                base_blocks: 60,
                live_blocks: 12,
                reorg_window_blocks: 4,
                segment_blocks: NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN),
                tail_probe_blocks: width,
                ..ActiveWriterPublicationConfig::default()
            };
            assert!(validate_config(&config).is_err());
        }
    }

    #[test]
    fn build_matched_differentials_pairs_ingest_active_with_canonical_quiescent_control()
    -> Result<(), Box<dyn Error>> {
        let report_template: super::ActiveWriterPublicationReport =
            serde_json::from_str(&serde_json::to_string(&sample_report_fixture())?)?;
        let diffs = build_matched_differentials(
            &report_template.canonical_quiescent_control,
            &report_template.modes[0],
            &report_template.published_quiescent_control,
            &report_template.modes[1..],
        );
        assert_eq!(diffs.len(), 3);
        assert_eq!(diffs[0].mode, WriterMode::IngestActive);
        assert_eq!(diffs[0].baseline_label, "canonical_quiescent_control");
        assert!(!diffs[0].baseline_catalog_published);
        assert_eq!(diffs[1].mode, WriterMode::IngestAndProjector);
        assert_eq!(diffs[1].baseline_label, "quiescent_control");
        assert_eq!(diffs[2].mode, WriterMode::IngestProjectorAndPublisher);
        assert_eq!(diffs[2].baseline_label, "published_quiescent_control");
        assert!(diffs[2].baseline_catalog_published);
        Ok(())
    }

    fn sample_report_fixture() -> serde_json::Value {
        let zero_lat =
            serde_json::json!({ "samples": 0, "p50": 0.0, "p95": 0.0, "max": 0.0, "mean": 0.0 });
        let lat =
            serde_json::json!({ "samples": 2, "p50": 4.0, "p95": 5.0, "max": 5.0, "mean": 4.0 });
        let stream = serde_json::json!({ "first_frame_ms": lat, "completion_ms": lat, "frames_delivered": 2, "payload_bytes_delivered": 32 });
        let zero_tick = serde_json::json!({ "wal_synced": 0, "wal_bytes": 0, "flush_write_bytes": 0, "compact_read_bytes": 0, "compact_write_bytes": 0, "bytes_read": 100, "bytes_written": 0, "block_cache_data_hit": 0, "block_cache_data_miss": 0, "stall_micros": 0 });
        let tail = serde_json::json!({
            "cold_after_advance": stream, "warm_repeat": stream,
            "unpublished_cold_after_advance": stream, "unpublished_warm_repeat": stream,
            "published_cold_after_advance": stream, "published_warm_repeat": stream,
            "published_mismatched_span_pairs": 0, "peak_tail_response_bytes": 32,
            "peak_encoded_tail_bytes": 32, "encoded_tail_byte_cap_evictions": 0,
            "encoded_tail_byte_capped_fills": 0, "encoded_tail_cache_fills": 1,
            "encoded_tail_cache_hits": 1, "encoded_tail_fallback_native": 0,
            "encoded_tail_bypass": 0, "unpaired_tail_probes": 0, "byte_cap_exceeded": false
        });
        let arm = |mode: WriterMode, wallet: bool| {
            let wal = if wallet { &lat } else { &zero_lat };
            serde_json::json!({
                "mode": mode, "workload_duration_ms": 10.0,
                "io_modes": { "canonical_primary": None::<&str>, "canonical_secondaries": ["buffered"], "wallet_primary": None::<&str>, "wallet_secondaries": if wallet { vec!["buffered"] } else { vec![] }, "projector_secondary": None::<&str>, "publisher_secondary": None::<&str> },
                "ingest": { "live_prepare_ms": zero_lat, "live_commit_ms": zero_lat, "blocks_committed": 0 },
                "latest_block_rpc_ms": lat, "commit_to_served_tip_ms": zero_lat,
                "catalog_range": stream, "catalog_range_unpublished": stream, "catalog_range_published": stream, "catalog_range_during_seal": stream,
                "tail_refill": tail, "slow_boundary_range": stream, "tree_state_ms": lat,
                "wallet_rpc_ms": { "taddress_balance_ms": wal, "address_utxos_ms": wal, "get_transaction_ms": wal },
                "mempool_churn": [], "projector": { "apply_ms": zero_lat, "transitions_applied": 0 },
                "publisher": { "total_ms": 0.0, "segment_publish_ms": zero_lat, "segments_published": 0, "catalog_refreshes": 0 },
                "writer_fsyncs": { "canonical_wal_synced": 0, "canonical_wal_bytes": 0, "wallet_wal_synced": 0, "wallet_wal_bytes": 0 },
                "node_rpcs": { "tip_id": 0, "fetch_block_at": 0, "fetch_chain_segment": 0, "fetch_chain_checkpoint": 0, "fetch_subtree_roots": 0, "total": 0 },
                "rocksdb_activity": { "canonical_writer": zero_tick, "reader_canonical_secondaries": zero_tick, "reader_wallet_secondaries": zero_tick, "wallet_writer": zero_tick, "projector_secondary": zero_tick, "publisher_secondary": zero_tick },
                "errors": { "unexpected_grpc_errors": 0, "unexpected_error_samples": [], "transient_retry_count": 0 }
            })
        };
        serde_json::json!({
            "contract_identity": "active-writer-publication",
            "report_format_version": 2,
            "workload": {
                "network": "zcash-regtest", "base_blocks": 60, "live_blocks": 4, "final_tip_height": 64,
                "reorg_window_blocks": 4, "segment_blocks": 64, "crossed_segment_boundary_height": 64,
                "sparse_block_count": 50, "dense_block_count": 14, "dense_orchard_actions_per_tx": 2,
                "transparent_spend_txs": 4, "tail_probe_blocks": 24, "requested_io_mode": "buffered",
                "publish_workers": 1, "mempool_churn_sizes": [50], "request_trace_digest": "aa"
            },
            "canonical_quiescent_control": arm(WriterMode::QuiescentControl, false),
            "published_quiescent_control": arm(WriterMode::QuiescentControl, true),
            "modes": [
                arm(WriterMode::QuiescentControl, true),
                arm(WriterMode::IngestActive, false),
                arm(WriterMode::IngestAndProjector, true),
                arm(WriterMode::IngestProjectorAndPublisher, true)
            ],
            "differentials_vs_quiescent": [],
            "byte_parity": { "all_matched": true, "digests": [], "mismatches": [] }
        })
    }

    #[test]
    fn validate_config_rejects_zero_reorg_and_zero_actions() {
        let zero_reorg = ActiveWriterPublicationConfig {
            base_blocks: 60,
            live_blocks: 12,
            reorg_window_blocks: 0,
            segment_blocks: NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN),
            ..ActiveWriterPublicationConfig::default()
        };
        assert!(validate_config(&zero_reorg).is_err());

        let zero_actions = ActiveWriterPublicationConfig {
            base_blocks: 60,
            live_blocks: 12,
            reorg_window_blocks: 4,
            segment_blocks: NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN),
            dense_orchard_actions_per_tx: 0,
            ..ActiveWriterPublicationConfig::default()
        };
        assert!(validate_config(&zero_actions).is_err());
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
            dense_orchard_actions_per_tx: 2,
            tail_probe_blocks: 24,
            commit_interval: Duration::from_millis(8),
            slow_reader_frame_delay: Duration::from_millis(4),
            mempool_churn_sizes: vec![50, 500],
            quiescent_rounds: 3,
            scratch_dir: None,
            io_mode: RocksDbIoMode::Buffered,
            publish_workers: None,
            require_tail_eviction: false,
        };
        let chain = GeneratedChain::build(ChainSpecConfig {
            base_blocks: config.base_blocks,
            live_blocks: config.live_blocks,
            reorg_window_blocks: config.reorg_window_blocks,
            dense_block_cadence: config.dense_block_cadence,
            dense_txs_per_block: config.dense_txs_per_block,
            dense_orchard_actions_per_tx: config.dense_orchard_actions_per_tx,
        })?;
        let template = TemplateStores::build(&chain, &config).await?;
        let boundary = next_segment_boundary(&config, &chain);
        let trace = WorkloadTrace::build(
            &config,
            boundary,
            chain.tracked_taddresses.clone(),
            chain.settled_txids.clone(),
            chain.live_txids.clone(),
        );
        let (prom_handle, _) = EncodedTailMetricSnapshot::install_and_capture()?;
        let run_spec = ModeRunSpec {
            mode: WriterMode::IngestProjectorAndPublisher,
            label: "ingest_projector_and_publisher",
            publish_catalog: true,
            wallet_serving: true,
        };
        let mut boot = bootstrap_mode_runtime(run_spec, &config, &chain, &template).await?;
        let source = AdvancingChainSource::new(&chain, chain.base_tip_height)
            .fail_at_height(config.base_blocks.saturating_add(2));

        let workload_res = tokio::time::timeout(Duration::from_secs(15), async {
            let outcome = run_mode_workload_with_source(super::ModeWorkloadInputs {
                run_spec,
                config: &config,
                chain: &chain,
                boot: &mut boot,
                source,
                trace: &trace,
                prom_handle: &prom_handle,
            })
            .await;
            shutdown_mode_runtime(boot.server, &boot.serving).await;
            outcome
        })
        .await?;

        assert!(workload_res.is_err());
        Ok(())
    }
}
