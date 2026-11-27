//! Orchestrator for the 3 matched `active-writer-publication` modes plus the
//! canonical quiescent baseline.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use zinder_core::ChainEpoch;
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
        CancelGuard, SupervisedGrpcServer, TwoGenCanonicalPublisher, TwoGenWalletPairPublisher,
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
    wallet_serving: bool,
}

/// Runs all matched writer/serving arms across one advancing Regtest chain.
///
/// # Errors
///
/// Returns [`BenchError`] if fixture generation, store catch-up, concurrent
/// pipelines, gRPC serving, or final-tip byte parity checks fail.
pub async fn run_active_writer_publication(
    config: &ActiveWriterPublicationConfig,
) -> Result<ActiveWriterPublicationReport, BenchError> {
    validate_config(config)?;
    crate::recorder::install_tracing_subscriber()?;
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
    let shared = (&chain, &template_stores, &trace);

    let (canonical_quiescent_control, canonical_parity) = Box::pin(run_single_mode(
        ModeRunSpec {
            mode: WriterMode::QuiescentControl,
            label: "canonical_quiescent_control",
            wallet_serving: false,
        },
        config,
        shared,
    ))
    .await?;
    let (mode_reports, mut parity_digests) =
        Box::pin(run_matched_modes(config, shared, canonical_parity)).await?;

    let unpublished_baseline = mode_reports
        .first()
        .ok_or_else(|| BenchError::tip_replay("quiescent_control report missing"))?;
    let differentials_vs_quiescent = build_matched_differentials(
        &canonical_quiescent_control,
        unpublished_baseline,
        &mode_reports[1..],
    );
    let byte_parity = ByteParityReport::evaluate(std::mem::take(&mut parity_digests));
    let workload = build_workload_geometry(config, &chain, segment_boundary, &trace.digest_hex);

    Ok(ActiveWriterPublicationReport {
        contract_identity: ACTIVE_WRITER_PUBLICATION_CONTRACT.to_owned(),
        report_format_version: ACTIVE_WRITER_PUBLICATION_FORMAT_VERSION,
        workload,
        canonical_quiescent_control,
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
    shared: (&GeneratedChain, &TemplateStores, &WorkloadTrace),
    canonical_parity: ModeParityDigests,
) -> Result<(Vec<ModeReport>, Vec<ModeParityDigests>), BenchError> {
    let mut mode_reports = Vec::with_capacity(3);
    let mut parity_digests = Vec::with_capacity(4);
    parity_digests.push(canonical_parity);

    for run_spec in [
        ModeRunSpec {
            mode: WriterMode::QuiescentControl,
            label: "quiescent_control",
            wallet_serving: true,
        },
        ModeRunSpec {
            mode: WriterMode::IngestActive,
            label: "ingest_active",
            wallet_serving: false,
        },
        ModeRunSpec {
            mode: WriterMode::IngestAndProjector,
            label: "ingest_and_projector",
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
    wallet_baseline: &ModeReport,
    active_modes: &[ModeReport],
) -> Vec<ModeDifferential> {
    active_modes
        .iter()
        .map(|candidate| {
            let (baseline_label, baseline) = match candidate.mode {
                WriterMode::IngestActive => ("canonical_quiescent_control", canonical_baseline),
                WriterMode::IngestAndProjector | WriterMode::QuiescentControl => {
                    ("quiescent_control", wallet_baseline)
                }
            };
            ModeDifferential::between(
                DifferentialBaselines {
                    baseline,
                    baseline_label,
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
        mempool_churn_sizes: config.mempool_churn_sizes.clone(),
        request_trace_digest: trace_digest_hex.to_owned(),
    }
}

struct ModeRuntimeBootstrap {
    paths: ModePaths,
    boundary_height: u32,
    serving: ModeServingHandle,
    initial_epoch: ChainEpoch,
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
    crate::recorder::install_tracing_subscriber()?;
    let paths = ModePaths::prepare(template, config.scratch_dir.as_deref(), run_spec.label)?;
    let boundary_height = next_segment_boundary(config, chain);

    if run_spec.mode == WriterMode::QuiescentControl {
        pre_advance_quiescent_stores(
            &paths,
            config,
            chain,
            QuiescentPreAdvance {
                advance_wallet: run_spec.wallet_serving,
            },
        )
        .await?;
    }

    let (serving, initial_epoch) = open_mode_serving(&run_spec, config, chain, &paths)?;
    let mempool = BenchmarkMempoolSurface::new(initial_epoch);
    let (tip_watcher_tx, tip_watcher) = create_tip_watcher(initial_epoch.id);
    let server = match &serving {
        ModeServingHandle::CanonicalOnly(can) => {
            SupervisedGrpcServer::spawn_canonical_only(
                can.capture_slot(),
                as_mempool_surface(Arc::clone(&mempool)),
                tip_watcher,
            )
            .await?
        }
        ModeServingHandle::WalletPair(wal) => {
            SupervisedGrpcServer::spawn_wallet_serving(
                wal.query(),
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
        serving,
        initial_epoch,
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

async fn run_single_mode(
    run_spec: ModeRunSpec,
    config: &ActiveWriterPublicationConfig,
    (chain, template, trace): (&GeneratedChain, &TemplateStores, &WorkloadTrace),
) -> Result<(ModeReport, ModeParityDigests), BenchError> {
    let mut boot = bootstrap_mode_runtime(run_spec, config, chain, template).await?;
    let source = AdvancingChainSource::new(chain, chain.base_tip_height);
    let _ = boot.serving.snapshot_and_reset_tickers();
    let workload_started = Instant::now();
    let (active_outcome, fleet_output, errors) =
        match run_mode_workload_with_source(ModeWorkloadInputs {
            run_spec,
            config,
            chain,
            boot: &mut boot,
            source,
            trace,
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
    let parity = parity_res?;
    server_res?;

    let report = assemble_mode_report(ModeReportAssembly {
        mode: run_spec.mode,
        workload_duration_ms,
        can_io_modes,
        wal_io_modes,
        can_reader_delta,
        wal_reader_delta,
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
            quiescent_rotator,
            rotation_requester: Some(inputs.boot.serving.clone()),
        },
        Arc::clone(&inputs.boot.mempool),
    );

    let active_outcome = if inputs.run_spec.mode == WriterMode::QuiescentControl {
        ActiveOutcome::default()
    } else {
        match drive_active_pipelines(ActivePipelineArgs {
            mode: inputs.run_spec.mode,
            config: inputs.config,
            chain: inputs.chain,
            paths: &inputs.boot.paths,
            serving: inputs.boot.serving.clone(),
            mempool: &inputs.boot.mempool,
            tip_watcher_tx: &inputs.boot.tip_watcher_tx,
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
        fleet,
        active,
        errors,
    } = assembly;
    ModeReport {
        mode,
        workload_duration_ms,
        io_modes: IoModeReport {
            canonical_primary: active.canonical_io_mode,
            canonical_secondaries: can_io_modes,
            wallet_primary: active.wallet_io_mode,
            wallet_secondaries: wal_io_modes,
            projector_secondary: active.projector_secondary_io_mode,
        },
        ingest: active.ingest,
        latest_block_rpc_ms: fleet.latest_block_rpc_ms,
        commit_to_served_tip_ms: fleet.commit_to_served_tip_ms,
        catalog_range: fleet.catalog_range,
        tail_refill: TailRefillMetrics {
            cold_after_advance: fleet.tail_cold_after_advance,
            warm_repeat: fleet.tail_warm_repeat,
            peak_tail_response_bytes: fleet.peak_tail_response_bytes,
            unpaired_tail_probes: fleet.unpaired_tail_probes,
        },
        slow_boundary_range: fleet.slow_boundary_range,
        tree_state_ms: fleet.tree_state_ms,
        wallet_rpc_ms: fleet.wallet_rpc_ms,
        mempool_churn: fleet.mempool_churn,
        projector: active.projector,
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
        ActiveWriterPublicationConfig, AdvancingChainSource, ChainSpecConfig, GeneratedChain,
        ModeRunSpec, TemplateStores, WorkloadTrace, WriterMode, bootstrap_mode_runtime,
        build_matched_differentials, next_segment_boundary, run_mode_workload_with_source,
        shutdown_mode_runtime, validate_config,
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
    fn validate_config_rejects_require_tail_eviction() {
        let config = ActiveWriterPublicationConfig {
            base_blocks: 60,
            live_blocks: 12,
            reorg_window_blocks: 4,
            segment_blocks: NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN),
            require_tail_eviction: true,
            ..ActiveWriterPublicationConfig::default()
        };
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn build_matched_differentials_pairs_ingest_active_with_canonical_quiescent_control()
    -> Result<(), Box<dyn Error>> {
        let report_template: super::ActiveWriterPublicationReport =
            serde_json::from_str(&serde_json::to_string(&sample_report_fixture())?)?;
        let diffs = build_matched_differentials(
            &report_template.canonical_quiescent_control,
            &report_template.modes[0],
            &report_template.modes[1..],
        );
        assert_eq!(diffs.len(), 2);
        assert_eq!(diffs[0].mode, WriterMode::IngestActive);
        assert_eq!(diffs[0].baseline_label, "canonical_quiescent_control");
        assert_eq!(diffs[1].mode, WriterMode::IngestAndProjector);
        assert_eq!(diffs[1].baseline_label, "quiescent_control");
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
            "peak_tail_response_bytes": 32, "unpaired_tail_probes": 0
        });
        let arm = |mode: WriterMode, wallet: bool| {
            let wal = if wallet { &lat } else { &zero_lat };
            serde_json::json!({
                "mode": mode, "workload_duration_ms": 10.0,
                "io_modes": { "canonical_primary": None::<&str>, "canonical_secondaries": ["buffered"], "wallet_primary": None::<&str>, "wallet_secondaries": if wallet { vec!["buffered"] } else { vec![] }, "projector_secondary": None::<&str> },
                "ingest": { "live_prepare_ms": zero_lat, "live_commit_ms": zero_lat, "blocks_committed": 0 },
                "latest_block_rpc_ms": lat, "commit_to_served_tip_ms": zero_lat,
                "catalog_range": stream,
                "tail_refill": tail, "slow_boundary_range": stream, "tree_state_ms": lat,
                "wallet_rpc_ms": { "taddress_balance_ms": wal, "address_utxos_ms": wal, "get_transaction_ms": wal },
                "mempool_churn": [], "projector": { "apply_ms": zero_lat, "transitions_applied": 0 },
                "writer_fsyncs": { "canonical_wal_synced": 0, "canonical_wal_bytes": 0, "wallet_wal_synced": 0, "wallet_wal_bytes": 0 },
                "node_rpcs": { "tip_id": 0, "fetch_block_at": 0, "fetch_chain_segment": 0, "fetch_chain_checkpoint": 0, "fetch_subtree_roots": 0, "total": 0 },
                "rocksdb_activity": { "canonical_writer": zero_tick, "reader_canonical_secondaries": zero_tick, "reader_wallet_secondaries": zero_tick, "wallet_writer": zero_tick, "projector_secondary": zero_tick },
                "errors": { "unexpected_grpc_errors": 0, "unexpected_error_samples": [], "transient_retry_count": 0 }
            })
        };
        serde_json::json!({
            "contract_identity": "active-writer-publication",
            "report_format_version": 3,
            "workload": {
                "network": "zcash-regtest", "base_blocks": 60, "live_blocks": 4, "final_tip_height": 64,
                "reorg_window_blocks": 4, "segment_blocks": 64, "crossed_segment_boundary_height": 64,
                "sparse_block_count": 50, "dense_block_count": 14, "dense_orchard_actions_per_tx": 2,
                "transparent_spend_txs": 4, "tail_probe_blocks": 24, "requested_io_mode": "buffered",
                "mempool_churn_sizes": [50], "request_trace_digest": "aa"
            },
            "canonical_quiescent_control": arm(WriterMode::QuiescentControl, false),
            "modes": [
                arm(WriterMode::QuiescentControl, true),
                arm(WriterMode::IngestActive, false),
                arm(WriterMode::IngestAndProjector, true)
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
    async fn writer_failure_cancels_and_drains_fleet_projector_and_server()
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
        let run_spec = ModeRunSpec {
            mode: WriterMode::IngestAndProjector,
            label: "ingest_and_projector",
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
