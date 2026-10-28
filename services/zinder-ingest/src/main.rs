//! Zinder ingestion command-line entry point.

use std::{
    ffi::OsString, net::SocketAddr, path::PathBuf, process::ExitCode, sync::Arc, time::Duration,
};

use clap::{Parser, Subcommand};
use parking_lot::RwLock;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use zinder_core::wire::encode_zinder_native_chain_name;
use zinder_core::{BlockHeight, NetworkUpgradeActivations};
use zinder_ingest::{
    CanonicalCheckpointStagingRoot, CanonicalConstructionConfig, CanonicalControlCommand,
    CanonicalControlGrpcAdapter, CanonicalFollowConfig, CanonicalIngestControlGrpcAdapter,
    CanonicalRunOverrides, CanonicalWriterConfig, ConventionalFeeDistributionBackfillConfig,
    ConventionalFeeDistributionBackfillContext, DEFAULT_MATERIALIZED_VIEW_TAILER_POLL_INTERVAL,
    DEFAULT_RUNTIME_MEMORY_METRICS_INTERVAL, HistoricalWorkGate, IngestError, LiveMempoolOwner,
    MaterializedViewReplayConfig, MaterializedViewTailer, MempoolIngestSettings, NodeSourceKind,
    TransactionComponentBackfillConfig, TransactionComponentBackfillContext,
    canonical_control_channel, classify_phase, mempool_ready_channel,
    open_primary_materialized_view_store, run_canonical_writer_with_control,
    run_live_mempool_owner, run_mempool_retention, seed_backfill_owned_consumer_cursors,
    spawn_conventional_fee_distribution_backfill_task,
    spawn_materialized_view_replay_budget_metrics_task, spawn_materialized_view_tailer_task,
    spawn_runtime_memory_metrics_task, spawn_transaction_component_backfill_task,
    spawn_upstream_health_probe_task,
};
use zinder_materialized_views::{MaterializedViewPreset, MaterializedViewStore};
use zinder_runtime::{
    OpsEndpointHandle, OpsServerError, Readiness, ReadinessState, RuntimeService, StartupPhase,
    cancel_on_terminating_signal, host_cpu_meets_compiled_baseline, install_tracing_subscriber,
    spawn_ops_endpoint_for,
};
use zinder_source::{
    JsonRpcMempoolSource, JsonRpcMempoolSourceOptions, MempoolSource, NodeCapabilities,
    NodeCapability, NodeSource, NodeTarget, ZebraIndexerMempoolSource,
    ZebraIndexerMempoolSourceOptions, ZebraIndexerSourceTarget, ZebraJsonRpcSource,
    ZebraJsonRpcSourceOptions,
};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, ChainStoreOptions, MempoolEventRetentionConfig,
    RocksDbCanonicalSecondary, RocksDbResourceBudget, SecondaryChainStore,
};

use crate::config::{
    CanonicalReplayVerificationConfigOverrides, IngestCommandConfig, IngestConfigError,
    IngestConfigOverrides, IngestCoverage,
};

mod cli;
mod config;
mod replay_verification;

const REQUIRED_INGEST_NODE_CAPABILITIES: &[NodeCapability] = &[
    NodeCapability::JsonRpc,
    NodeCapability::BestChainBlocks,
    NodeCapability::TipId,
    NodeCapability::TreeState,
    NodeCapability::SubtreeRoots,
];

/// Suffix of the reader-local metadata directory the in-process canonical
/// secondary owns, as ADR-0003 requires of every reader.
const MATERIALIZED_VIEW_SECONDARY_PATH_SUFFIX: &str = ".materialized-view-secondary";

/// Delay between attempts to open the canonical secondary while the writer is
/// still constructing or publishing its primary.
const CANONICAL_SECONDARY_OPEN_RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// Cadence at which the replay budget sampler refreshes its gauges.
const MATERIALIZED_VIEW_REPLAY_BUDGET_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Parser)]
#[command(name = "zinder-ingest")]
#[command(about = "Zinder canonical chain ingestion")]
#[command(version)]
struct Cli {
    /// TOML configuration file loaded before environment variables and CLI overrides.
    #[arg(long = "config", global = true)]
    config_path: Option<PathBuf>,
    /// Print the resolved command configuration without opening storage or connecting.
    #[arg(long = "print-config", global = true)]
    print_config: bool,
    /// Operational HTTP endpoint listen address for /healthz, /readyz, /metrics.
    #[arg(long = "ops-listen-addr", global = true)]
    ops_listen_addr: Option<SocketAddr>,
    /// Network name, such as zcash-regtest.
    #[arg(long, global = true)]
    network: Option<String>,
    /// Upstream node source, currently zebra-json-rpc.
    #[arg(long = "node-source", global = true)]
    node_source: Option<String>,
    /// Zebra JSON-RPC address.
    #[arg(long = "json-rpc-addr", global = true)]
    json_rpc_addr: Option<String>,
    /// Node auth method, such as none, basic, or cookie.
    #[arg(long = "node-auth-method", global = true)]
    node_auth_method: Option<String>,
    /// Node auth username when the method is basic.
    #[arg(long = "node-auth-username", global = true)]
    node_auth_username: Option<String>,
    /// Node auth cookie path when the method is cookie.
    #[arg(long = "node-auth-path", global = true)]
    node_auth_path: Option<PathBuf>,
    /// Canonical Zinder store path.
    #[arg(long = "storage-path", global = true)]
    storage_path: Option<PathBuf>,
    /// Node request timeout in seconds.
    #[arg(long = "request-timeout-secs", global = true)]
    request_timeout_secs: Option<u64>,
    /// Maximum JSON-RPC response body size in bytes.
    #[arg(long = "max-response-bytes", global = true)]
    max_response_bytes: Option<u64>,
    /// Number of near-tip blocks that may be replaced by a reorg.
    #[arg(long = "reorg-window-blocks", global = true)]
    reorg_window_blocks: Option<u32>,
    /// Phase-classifier boundary; gaps above this trigger `BulkCatchup`.
    #[arg(long = "catchup-threshold-blocks", global = true)]
    catchup_threshold_blocks: Option<u32>,
    /// Maximum number of blocks committed in one bulk-catchup batch.
    #[arg(long = "canonical-batch-max-blocks", global = true)]
    canonical_batch_max_blocks: Option<u32>,
    /// Maximum canonical artifact bytes accumulated before closing a batch.
    #[arg(long = "canonical-batch-max-artifact-bytes", global = true)]
    canonical_batch_max_artifact_bytes: Option<u64>,
    /// Maximum estimated canonical write bytes accumulated before closing a batch.
    #[arg(long = "canonical-batch-max-estimated-write-bytes", global = true)]
    canonical_batch_max_estimated_write_bytes: Option<u64>,
    /// Minimum blocks before estimated write bytes can close a batch.
    #[arg(
        long = "canonical-batch-min-blocks-before-estimated-write-close",
        global = true
    )]
    canonical_batch_min_blocks_before_estimated_write_close: Option<u32>,
    /// Maximum connected blocks requested from the source in one bulk-catchup segment.
    #[arg(long = "source-segment-max-blocks", global = true)]
    source_segment_max_blocks: Option<u32>,
    /// Target response bytes for adaptive source segment sizing.
    #[arg(long = "source-segment-target-response-bytes", global = true)]
    source_segment_target_response_bytes: Option<u64>,
    /// Maximum concurrent source segment fetch requests.
    #[arg(long = "source-fetch-max-in-flight-requests", global = true)]
    source_fetch_max_in_flight_requests: Option<u32>,
    /// Maximum reserved response bytes across source segment fetches.
    #[arg(long = "source-fetch-max-in-flight-bytes", global = true)]
    source_fetch_max_in_flight_bytes: Option<u64>,
    /// Number of parallel `prepare_canonical_block` invocations on the blocking pool.
    #[arg(long = "block-prepare-concurrency", global = true)]
    block_prepare_concurrency: Option<u32>,
    /// Delay between upstream node tip polls, in milliseconds.
    #[arg(long = "poll-interval-ms", global = true)]
    poll_interval_ms: Option<u64>,
    /// Lag threshold (in blocks) below which tip-follow reports `Ready`.
    #[arg(long = "lag-threshold-blocks", global = true)]
    lag_threshold_blocks: Option<u64>,
    /// Stop committing after reaching this height.
    #[arg(long = "target-height", global = true)]
    target_height: Option<u32>,
    /// Bootstrap an empty store from the upstream node's chain state at
    /// this height (the ingest loop begins at `checkpoint_height + 1`).
    #[arg(long = "checkpoint-height", global = true)]
    checkpoint_height: Option<u32>,
    /// Allow bulk-catchup batches to advance the settled tip inside the upstream
    /// node's reorg window. Disposable-store recovery only.
    #[arg(long = "allow-reorg-window-settlement", action = clap::ArgAction::SetTrue, global = true)]
    allow_reorg_window_settlement: bool,
    /// Derive the bulk-catchup floor needed by native wallets from
    /// node-advertised activation heights.
    #[arg(long = "wallet-serving", action = clap::ArgAction::SetTrue, global = true)]
    wallet_serving: bool,
    /// Private ingest-control gRPC listen address.
    #[arg(long = "ingest-control-listen-addr", global = true)]
    ingest_control_listen_addr: Option<SocketAddr>,
    /// Path to a file containing the shared-secret bearer token enforced
    /// by the ingest-control endpoint.
    #[arg(long = "ingest-control-token-path", global = true)]
    ingest_control_token_path: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Print store + upstream state, the phase the loop would run, and the
    /// upstream-health snapshot, then exit. Diagnostic only; does not ingest
    /// or commit chain data.
    Probe,
    /// Check replay-envelope integrity and canonical-header parity through a
    /// reader-local `RocksDB` secondary.
    VerifyCanonicalReplay(CanonicalReplayVerificationArgs),
}

#[derive(Parser)]
struct CanonicalReplayVerificationArgs {
    /// Network name, such as zcash-regtest.
    #[arg(long)]
    network: Option<String>,
    /// Canonical Zinder store path opened as the secondary's primary source.
    #[arg(long = "storage-path")]
    storage_path: Option<PathBuf>,
    /// Reader-local metadata path for this verification run.
    #[arg(long = "secondary-path")]
    secondary_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    install_tracing_subscriber();

    if !host_cpu_meets_compiled_baseline() {
        return ExitCode::FAILURE;
    }

    if cli.print_config {
        return run_print_config(cli);
    }

    Box::pin(run_runtime(cli)).await
}

#[allow(
    clippy::print_stdout,
    reason = "probe is a CLI diagnostic; the structured snapshot is what operators read."
)]
async fn run_probe(
    config_path: Option<PathBuf>,
    overrides: IngestConfigOverrides,
) -> Result<(), IngestConfigError> {
    let command_config = config::load_ingest_config(config_path, overrides)?;
    let runtime_config = command_config.runtime_config;
    let source =
        zebra_json_rpc_source_for_target(runtime_config.node_source, &runtime_config.node)?;
    let activations = source
        .discover_network_upgrade_activations("zinder-ingest-probe")
        .await
        .map_err(IngestError::from)?;
    let upstream_tip = source
        .tip_id()
        .await
        .map_err(IngestError::from)?
        .height
        .value();
    let store = zinder_store::RocksDbCanonicalStore::open_ready(
        &runtime_config.storage_path,
        &activations,
        zinder_store::CanonicalStoreWorkload::Wallet,
        runtime_config.raw_blob_policy.to_retention(),
        zinder_store::CanonicalReorgPolicy::new(runtime_config.reorg_window_blocks)
            .map_err(zinder_ingest::CanonicalWriterError::from)?,
        runtime_config.canonical_rocksdb_budget,
    )
    .map_err(zinder_ingest::CanonicalWriterError::from)
    .map_err(IngestConfigError::from)?;
    let store_tip_height = store.event_fence().visible_tip().height.value();
    let store_tip = Some(store_tip_height);
    let phase = classify_phase(
        store_tip,
        upstream_tip,
        runtime_config.phase_classification.catchup_threshold_blocks,
    );
    let gap_blocks = i64::from(upstream_tip).saturating_sub(i64::from(store_tip_height));
    let upstream_health = match source.poll_upstream_health().await {
        Ok(snapshot) => format!("{}/{}", snapshot.source, snapshot.reason),
        Err(error) => format!("unavailable ({error})"),
    };

    println!(
        "{{\"store_tip\": {store_tip:?}, \"upstream_tip\": {upstream_tip}, \
         \"gap_blocks\": {gap_blocks}, \"phase_that_would_run\": \"{phase}\", \
         \"upstream_health\": \"{upstream_health}\"}}",
        phase = phase.wire_label(),
    );
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "ingest startup and lifecycle shutdown remain together so worker drain, reorg parking, and ops shutdown order stays auditable"
)]
async fn run_ingest(
    config_path: Option<PathBuf>,
    overrides: IngestConfigOverrides,
) -> Result<(), IngestConfigError> {
    let load_config_phase = StartupPhase::LoadConfig.start();
    let mut command_config = config::load_ingest_config(config_path, overrides)?;
    load_config_phase.complete();
    let readiness = Readiness::default();
    let start_api_phase = StartupPhase::StartApi.start();
    let ops_handle = spawn_ops_endpoint_for(
        RuntimeService::Ingest,
        command_config.ops_listen_addr,
        env!("CARGO_PKG_VERSION"),
        encode_zinder_native_chain_name(command_config.runtime_config.node.network),
        readiness.clone(),
        Arc::from(zinder_proto::capabilities::always_on_capability_strings(
            zinder_proto::capabilities::CapabilitySurface::Ingest,
        )),
    )
    .await?;

    let connect_node_phase = StartupPhase::ConnectNode.start();
    let source = zebra_json_rpc_source_for_target(
        command_config.runtime_config.node_source,
        &command_config.runtime_config.node,
    )?;
    connect_node_phase.complete();
    let check_schema_phase = StartupPhase::CheckSchema.start();
    ensure_node_capabilities(&source, &readiness).await?;
    let network_upgrade_activations = source
        .discover_network_upgrade_activations("zinder-ingest")
        .await
        .map_err(IngestError::from)?;
    check_schema_phase.complete();

    let recover_state_phase = StartupPhase::RecoverState.start();
    resolve_wallet_serving_modifiers(&mut command_config);
    recover_state_phase.complete();
    let termination = CancellationToken::new();
    let _signal_handle = cancel_on_terminating_signal(termination.clone());
    let worker_cancel = termination.child_token();
    // The control listener outlives the worker drain so a parked writer can
    // still report its typed halt to readers until termination.
    let control_server_cancel = termination.child_token();
    record_explorer_views_admitted(false);
    let materialized_view_plane =
        materialized_view_plane_spec(&command_config, Arc::clone(&network_upgrade_activations))?;
    tracing::info!(
        target: "zinder::ingest",
        event = "explorer_views_config_resolved",
        coverage = command_config.coverage.as_kebab_case(),
        explorer_views = command_config.explorer_views,
        "resolved Explorer views configuration"
    );
    let worker_tasks = IngestWorkerTaskHandles {
        upstream_health_probe: spawn_upstream_health_probe_for(
            &command_config.runtime_config.node,
            &source,
            readiness.clone(),
            worker_cancel.clone(),
        ),
        memory_metrics: spawn_runtime_memory_metrics_task(
            DEFAULT_RUNTIME_MEMORY_METRICS_INTERVAL,
            worker_cancel.clone(),
        ),
        materialized_view_plane: tokio::spawn(run_materialized_view_plane(
            materialized_view_plane,
            readiness.clone(),
            HistoricalWorkGate::new(readiness.clone()),
            worker_cancel.clone(),
        )),
        replay_budget_metrics: command_config.explorer_views.then(|| {
            spawn_materialized_view_replay_budget_metrics_task(
                MaterializedViewReplayConfig::DEFAULT,
                DEFAULT_MATERIALIZED_VIEW_TAILER_POLL_INTERVAL,
                MATERIALIZED_VIEW_REPLAY_BUDGET_SAMPLE_INTERVAL,
                readiness.clone(),
                worker_cancel.clone(),
            )
        }),
    };
    start_api_phase.complete();
    StartupPhase::Ready.start().complete();

    let mut writer_config =
        canonical_writer_config(&command_config, Arc::clone(&network_upgrade_activations));
    let mut canonical_control_tasks = spawn_canonical_control_tasks(
        &command_config,
        &source,
        &readiness,
        ControlTaskCancellation {
            workers: &worker_cancel,
            server: &control_server_cancel,
        },
        &mut writer_config,
    );
    log_canonical_writer_start(&command_config, &writer_config);
    let writer = run_canonical_writer_with_control(
        &source,
        network_upgrade_activations,
        writer_config,
        &readiness,
        &worker_cancel,
        canonical_control_tasks.commands.take(),
    );
    let writer_result = Box::pin(supervise_canonical_writer(
        writer,
        &worker_cancel,
        &mut canonical_control_tasks,
    ))
    .await;
    let control_server = canonical_control_tasks
        .server
        .take()
        .filter(|_| !canonical_control_tasks.server_completed);
    coordinate_canonical_writer_lifecycle(
        writer_result,
        &termination,
        &readiness,
        shutdown_ingest_worker_tasks(&worker_cancel, canonical_control_tasks, worker_tasks),
        async move {
            control_server_cancel.cancel();
            await_canonical_control_server_shutdown(control_server).await;
            shutdown_ops_endpoint(ops_handle).await
        },
    )
    .await
}

/// Cancellation scopes for the canonical control tasks.
#[derive(Clone, Copy)]
struct ControlTaskCancellation<'a> {
    /// Stops the mempool owner and retention workers with the other workers.
    workers: &'a CancellationToken,
    /// Stops the shared control listener after the ops endpoint's last use.
    server: &'a CancellationToken,
}

type CanonicalControlServer = JoinHandle<Result<(), tonic::transport::Error>>;

struct CanonicalControlTasks {
    server: Option<CanonicalControlServer>,
    commands: Option<mpsc::Receiver<CanonicalControlCommand>>,
    mempool_owner: Option<JoinHandle<()>>,
    mempool_retention: Option<JoinHandle<()>>,
    server_completed: bool,
}

fn spawn_canonical_control_tasks(
    command_config: &IngestCommandConfig,
    source: &ZebraJsonRpcSource,
    readiness: &Readiness,
    cancellation: ControlTaskCancellation<'_>,
    writer_config: &mut CanonicalWriterConfig,
) -> CanonicalControlTasks {
    if let Some(listen_addr) = command_config.ingest_control_listen_addr {
        let (canonical_control_handle, canonical_control_commands) = canonical_control_channel();
        let mempool = command_config.runtime_config.mempool;
        let mempool_owner =
            LiveMempoolOwner::with_reconciliation_batch_target_raw_transaction_bytes(
                mempool.reconciliation_batch_target_raw_transaction_bytes,
            );
        let (mempool_ready_signal, mempool_ready_gate) = mempool_ready_channel();
        writer_config.follow.mempool_ready_gate = Some(mempool_ready_gate);
        let mempool_source =
            build_live_mempool_source(&command_config.runtime_config.node, source, mempool);
        let mempool_owner_task = tokio::spawn(run_live_mempool_owner(
            mempool_source,
            canonical_control_handle.clone(),
            mempool_owner.clone(),
            mempool_ready_signal,
            cancellation.workers.clone(),
        ));
        let mempool_retention = MempoolEventRetentionConfig::new(
            command_config.retention.mempool_mined_window(),
            command_config.retention.mempool_invalidated_window(),
        );
        let mempool_retention_task = tokio::spawn(run_mempool_retention(
            canonical_control_handle.clone(),
            mempool_owner.clone(),
            zinder_ingest::MempoolRetentionSettings {
                retention: mempool_retention,
                budget: command_config.retention.mempool_step_budget(),
                check_interval: command_config.retention.mempool_check_interval(),
            },
            cancellation.workers.clone(),
        ));
        let canonical_adapter = CanonicalControlGrpcAdapter::new(
            canonical_control_handle.clone(),
            CanonicalCheckpointStagingRoot::new(
                command_config
                    .ingest_control_checkpoint_staging_root
                    .clone(),
            ),
            command_config.runtime_config.canonical_rocksdb_budget,
        )
        .with_bearer_token(command_config.ingest_control_bearer_token.clone())
        .with_checkpoint_bearer_token(
            command_config
                .ingest_control_checkpoint_bearer_token
                .clone(),
        );
        let node_source: Arc<dyn NodeSource> = Arc::new(source.clone());
        let ingest_adapter = CanonicalIngestControlGrpcAdapter::new(
            command_config.runtime_config.node.network,
            canonical_control_handle,
            mempool_owner,
            node_source,
            readiness.clone(),
        )
        .with_bearer_token(command_config.ingest_control_bearer_token.clone());
        let server_cancel = cancellation.server.clone();
        let canonical_control_server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(canonical_adapter.into_server())
                .add_service(ingest_adapter.into_server())
                .serve_with_shutdown(listen_addr, server_cancel.cancelled_owned())
                .await
        });
        CanonicalControlTasks {
            server: Some(canonical_control_server),
            commands: Some(canonical_control_commands),
            mempool_owner: Some(mempool_owner_task),
            mempool_retention: Some(mempool_retention_task),
            server_completed: false,
        }
    } else {
        CanonicalControlTasks {
            server: None,
            commands: None,
            mempool_owner: None,
            mempool_retention: None,
            server_completed: false,
        }
    }
}

async fn supervise_canonical_writer<WriterOutput>(
    writer: impl std::future::Future<Output = Result<WriterOutput, zinder_ingest::CanonicalWriterError>>,
    cancel: &CancellationToken,
    control_tasks: &mut CanonicalControlTasks,
) -> Result<(), zinder_ingest::CanonicalWriterError> {
    tokio::pin!(writer);
    let writer_result = if let Some(canonical_control_server) = control_tasks.server.as_mut() {
        tokio::select! {
            biased;
            () = cancel.cancelled() => writer.await,
            writer_result = &mut writer => writer_result,
            server_result = canonical_control_server => {
                control_tasks.server_completed = true;
                if cancel.is_cancelled() {
                    return writer.await.map(drop);
                }
                cancel.cancel();
                let reason = match server_result {
                    Ok(Ok(())) => "canonical control server stopped unexpectedly".to_owned(),
                    Ok(Err(error)) => error.to_string(),
                    Err(error) => error.to_string(),
                };
                Err(zinder_ingest::CanonicalWriterError::ControlServer { reason })
            }
            mempool_owner_result = await_mempool_owner_exit(&mut control_tasks.mempool_owner) => {
                let _completed_owner_task = control_tasks.mempool_owner.take();
                if cancel.is_cancelled() {
                    return writer.await.map(drop);
                }
                cancel.cancel();
                let reason = match mempool_owner_result {
                    Ok(()) => "live mempool owner stopped unexpectedly".to_owned(),
                    Err(join_error) => join_error.to_string(),
                };
                Err(zinder_ingest::CanonicalWriterError::MempoolOwner { reason })
            }
            mempool_retention_result = await_mempool_retention_exit(
                &mut control_tasks.mempool_retention,
            ) => {
                let _completed_retention_task = control_tasks.mempool_retention.take();
                if cancel.is_cancelled() {
                    return writer.await.map(drop);
                }
                cancel.cancel();
                let reason = match mempool_retention_result {
                    Ok(()) => "mempool retention task stopped unexpectedly".to_owned(),
                    Err(join_error) => join_error.to_string(),
                };
                Err(zinder_ingest::CanonicalWriterError::MempoolRetention { reason })
            }
        }
    } else {
        writer.await
    };
    writer_result.map(drop)
}

async fn await_mempool_owner_exit(
    mempool_owner: &mut Option<JoinHandle<()>>,
) -> Result<(), tokio::task::JoinError> {
    match mempool_owner {
        Some(mempool_owner) => mempool_owner.await,
        None => std::future::pending().await,
    }
}

async fn await_mempool_retention_exit(
    mempool_retention: &mut Option<JoinHandle<()>>,
) -> Result<(), tokio::task::JoinError> {
    match mempool_retention {
        Some(mempool_retention) => mempool_retention.await,
        None => std::future::pending().await,
    }
}

async fn coordinate_canonical_writer_lifecycle<DrainWorkers, ShutdownOps>(
    writer_result: Result<(), zinder_ingest::CanonicalWriterError>,
    termination: &CancellationToken,
    readiness: &Readiness,
    drain_workers: DrainWorkers,
    shutdown_ops: ShutdownOps,
) -> Result<(), IngestConfigError>
where
    DrainWorkers: std::future::Future<Output = ()>,
    ShutdownOps: std::future::Future<Output = Result<(), OpsServerError>>,
{
    drain_workers.await;
    match writer_result {
        Err(zinder_ingest::CanonicalWriterError::Follow(
            zinder_ingest::CanonicalFollowError::ReorgWindowExceeded(evidence),
        )) => {
            readiness.park(evidence.parked_readiness());
            tracing::warn!(
                target: "zinder::ingest",
                event = "canonical_writer_reorg_window_exceeded",
                local_tip_height = evidence.local_tip.height.value(),
                source_tip_height = evidence.source_tip.height.value(),
                settled_tip_height = evidence.settled_tip.height.value(),
                required_depth = evidence.required_depth,
                configured_window_blocks = evidence.configured_window_blocks,
                covered_depth_blocks = evidence.covered_depth_blocks,
                "canonical writer parked with readiness drained for operator review"
            );
            termination.cancelled().await;
            shutdown_ops.await?;
            Ok(())
        }
        outcome => {
            let ops_outcome = shutdown_ops.await;
            match outcome {
                Err(error) => {
                    if let Err(ops_error) = ops_outcome {
                        tracing::warn!(
                            target: "zinder::ingest",
                            event = "ops_endpoint_shutdown_failed",
                            error = %ops_error,
                            "operational endpoint shutdown also failed"
                        );
                    }
                    Err(IngestConfigError::from(error))
                }
                Ok(()) => ops_outcome.map_err(IngestConfigError::from),
            }
        }
    }
}

/// Background tasks the ingest runtime drains before the ops endpoint closes.
struct IngestWorkerTaskHandles {
    memory_metrics: JoinHandle<()>,
    upstream_health_probe: Option<JoinHandle<()>>,
    materialized_view_plane: JoinHandle<()>,
    replay_budget_metrics: Option<JoinHandle<()>>,
}

async fn shutdown_ingest_worker_tasks(
    cancel: &CancellationToken,
    mut control_tasks: CanonicalControlTasks,
    worker_tasks: IngestWorkerTaskHandles,
) {
    cancel.cancel();
    if let Some(mempool_owner_task) = control_tasks.mempool_owner.take() {
        await_worker_task("mempool_owner", mempool_owner_task).await;
    }
    if let Some(mempool_retention_task) = control_tasks.mempool_retention.take() {
        await_worker_task("mempool_retention", mempool_retention_task).await;
    }
    await_worker_task("runtime_memory_metrics", worker_tasks.memory_metrics).await;
    if let Some(upstream_health_probe) = worker_tasks.upstream_health_probe {
        await_worker_task("upstream_health_probe", upstream_health_probe).await;
    }
    await_worker_task(
        "materialized_view_plane",
        worker_tasks.materialized_view_plane,
    )
    .await;
    if let Some(replay_budget_metrics) = worker_tasks.replay_budget_metrics {
        await_worker_task(
            "materialized_view_replay_budget_metrics",
            replay_budget_metrics,
        )
        .await;
    }
}

async fn await_worker_task(worker: &'static str, handle: JoinHandle<()>) {
    if let Err(join_error) = handle.await {
        tracing::warn!(
            target: "zinder::ingest",
            event = "ingest_worker_join_failed",
            worker,
            error = %join_error,
            "ingest worker task did not shut down cleanly"
        );
    }
}

async fn shutdown_ops_endpoint(
    ops_handle: Option<OpsEndpointHandle>,
) -> Result<(), OpsServerError> {
    match ops_handle {
        Some(handle) => handle.shutdown().await,
        None => Ok(()),
    }
}

async fn await_canonical_control_server_shutdown(server: Option<CanonicalControlServer>) {
    let Some(server) = server else {
        return;
    };
    if let Err(join_error) = server.await {
        tracing::warn!(
            target: "zinder::ingest",
            event = "canonical_control_server_join_failed",
            error = %join_error,
            "canonical control server did not shut down cleanly"
        );
    }
}

/// Everything the in-process materialized-view plane needs to open its own
/// storage handles once the canonical writer has published a READY primary.
struct MaterializedViewPlaneSpec {
    explorer_views: bool,
    storage_path: PathBuf,
    secondary_path: PathBuf,
    canonical_rocksdb_budget: RocksDbResourceBudget,
    materialized_view_rocksdb_budget: RocksDbResourceBudget,
    activations: Arc<NetworkUpgradeActivations>,
    raw_blob_retention: zinder_store::RawBlobRetention,
    reorg_policy: CanonicalReorgPolicy,
    chain_event_retention_window: Option<Duration>,
    cursor_at_risk_warning: Duration,
}

fn materialized_view_plane_spec(
    command_config: &IngestCommandConfig,
    activations: Arc<NetworkUpgradeActivations>,
) -> Result<MaterializedViewPlaneSpec, IngestConfigError> {
    let runtime_config = &command_config.runtime_config;
    Ok(MaterializedViewPlaneSpec {
        explorer_views: command_config.explorer_views,
        secondary_path: materialized_view_secondary_path(&runtime_config.storage_path),
        storage_path: runtime_config.storage_path.clone(),
        canonical_rocksdb_budget: runtime_config.canonical_rocksdb_budget,
        materialized_view_rocksdb_budget: runtime_config.materialized_view_rocksdb_budget,
        activations,
        raw_blob_retention: runtime_config.raw_blob_policy.to_retention(),
        reorg_policy: CanonicalReorgPolicy::new(runtime_config.reorg_window_blocks)
            .map_err(zinder_ingest::CanonicalWriterError::from)?,
        chain_event_retention_window: command_config.retention.chain_event_window(),
        cursor_at_risk_warning: command_config.retention.cursor_at_risk_warning(),
    })
}

fn materialized_view_secondary_path(storage_path: &std::path::Path) -> PathBuf {
    let mut secondary_path = OsString::from(storage_path.as_os_str());
    secondary_path.push(MATERIALIZED_VIEW_SECONDARY_PATH_SUFFIX);
    PathBuf::from(secondary_path)
}

/// Builds and follows the selected materialized views from canonical storage.
///
/// The canonical primary may not exist yet: a fresh deployment publishes it
/// only after the writer finishes construction. The view store nests inside the
/// canonical directory, so it is opened strictly after a successful secondary
/// open; creating it earlier would make the canonical path exist and route the
/// writer's fresh construction into a reopen it cannot satisfy.
async fn run_materialized_view_plane(
    spec: MaterializedViewPlaneSpec,
    readiness: Readiness,
    historical_work_gate: HistoricalWorkGate,
    cancel: CancellationToken,
) {
    if !spec.explorer_views {
        tracing::info!(
            target: "zinder::ingest",
            event = "explorer_views_disabled",
            explorer_views = false,
            "Explorer views are disabled by resolved configuration"
        );
        return;
    }
    let Some(canonical) = open_canonical_secondary_when_published(&spec, &cancel).await else {
        return;
    };
    let Some(materialized_view_preset) = explorer_view_preset(spec.explorer_views) else {
        return;
    };
    let Some(materialized_view_store) =
        open_and_admit_materialized_view_store(&spec, materialized_view_preset, &readiness)
    else {
        return;
    };
    record_explorer_views_admitted(true);
    tracing::info!(
        target: "zinder::ingest",
        event = "explorer_views_admitted",
        explorer_views = true,
        preset = materialized_view_preset.as_str(),
        "Explorer views admitted after store open"
    );
    if let Err(error) = seed_backfill_owned_consumer_cursors(
        &canonical,
        &spec.activations,
        &materialized_view_store,
    ) {
        tracing::warn!(
            target: "zinder::ingest",
            event = "materialized_view_backfill_cursor_seed_failed",
            error = %error,
            "failed to seed backfill-owned consumer cursors; those consumers replay retained history instead"
        );
    }
    let canonical = Arc::new(RwLock::new(canonical));
    let tailer = spawn_materialized_view_tailer_task(
        MaterializedViewTailer {
            canonical: Arc::clone(&canonical),
            materialized_view_store: materialized_view_store.clone(),
            config: MaterializedViewReplayConfig::DEFAULT,
            activations: Arc::clone(&spec.activations),
            reorg_window_blocks: spec.reorg_policy.reorg_window_blocks(),
            chain_event_retention_window: spec.chain_event_retention_window,
            cursor_at_risk_warning: spec.cursor_at_risk_warning,
        },
        DEFAULT_MATERIALIZED_VIEW_TAILER_POLL_INTERVAL,
        historical_work_gate.clone(),
        cancel.clone(),
    );
    await_materialized_view_workers(
        MaterializedViewWorkerContext {
            explorer_views: spec.explorer_views,
            canonical,
            activations: spec.activations,
            materialized_view_store,
        },
        tailer,
        historical_work_gate,
        cancel,
    )
    .await;
}

fn open_and_admit_materialized_view_store(
    spec: &MaterializedViewPlaneSpec,
    preset: MaterializedViewPreset,
    readiness: &Readiness,
) -> Option<MaterializedViewStore> {
    let materialized_view_store = match open_primary_materialized_view_store(
        &spec.storage_path,
        preset,
        spec.materialized_view_rocksdb_budget,
    ) {
        Ok(materialized_view_store) => materialized_view_store,
        Err(error) => {
            tracing::error!(
                target: "zinder::ingest",
                event = "materialized_view_store_open_failed",
                error = %error,
                explorer_views = spec.explorer_views,
                "failed to open the materialized-view store; selected views stay unavailable"
            );
            return None;
        }
    };
    readiness.set_materialized_view_workload(
        preset.as_str(),
        preset
            .consumer_schemas()
            .iter()
            .map(|schema| schema.name.as_str().to_owned())
            .collect(),
    );
    Some(materialized_view_store)
}

struct MaterializedViewWorkerContext {
    explorer_views: bool,
    canonical: Arc<RwLock<RocksDbCanonicalSecondary>>,
    activations: Arc<NetworkUpgradeActivations>,
    materialized_view_store: MaterializedViewStore,
}

async fn await_materialized_view_workers(
    context: MaterializedViewWorkerContext,
    tailer: JoinHandle<()>,
    historical_work_gate: HistoricalWorkGate,
    cancel: CancellationToken,
) {
    let (conventional_fee_distribution_backfill, transaction_component_backfill) =
        if explorer_views_start_backfills(context.explorer_views) {
            (
                Some(spawn_conventional_fee_distribution_backfill_task(
                    ConventionalFeeDistributionBackfillConfig::DEFAULT,
                    ConventionalFeeDistributionBackfillContext::new(
                        Arc::clone(&context.canonical),
                        Arc::clone(&context.activations),
                        context.materialized_view_store.clone(),
                    ),
                    historical_work_gate.clone(),
                    cancel.clone(),
                )),
                Some(spawn_transaction_component_backfill_task(
                    TransactionComponentBackfillConfig::DEFAULT,
                    TransactionComponentBackfillContext::new(
                        context.canonical,
                        context.activations,
                        context.materialized_view_store,
                    ),
                    historical_work_gate,
                    cancel,
                )),
            )
        } else {
            (None, None)
        };
    await_worker_task("materialized_view_tailer", tailer).await;
    if let Some(conventional_fee_distribution_backfill) = conventional_fee_distribution_backfill {
        await_worker_task(
            "conventional_fee_distribution_backfill",
            conventional_fee_distribution_backfill,
        )
        .await;
    }
    if let Some(transaction_component_backfill) = transaction_component_backfill {
        await_worker_task(
            "transaction_component_backfill",
            transaction_component_backfill,
        )
        .await;
    }
}

const fn explorer_views_start_backfills(explorer_views: bool) -> bool {
    explorer_views
}

const fn explorer_view_preset(explorer_views: bool) -> Option<MaterializedViewPreset> {
    if explorer_views {
        Some(MaterializedViewPreset::Explorer)
    } else {
        None
    }
}

fn record_explorer_views_admitted(admitted: bool) {
    metrics::gauge!("zinder_explorer_views_admitted").set(if admitted { 1.0 } else { 0.0 });
}

/// Retries the canonical secondary open until the writer publishes a READY
/// primary or the runtime is cancelled.
async fn open_canonical_secondary_when_published(
    spec: &MaterializedViewPlaneSpec,
    cancel: &CancellationToken,
) -> Option<RocksDbCanonicalSecondary> {
    let mut waiting_logged = false;
    loop {
        match RocksDbCanonicalSecondary::open_ready(
            &spec.storage_path,
            &spec.secondary_path,
            &spec.activations,
            CanonicalStoreWorkload::Wallet,
            spec.raw_blob_retention,
            spec.reorg_policy,
            spec.canonical_rocksdb_budget,
        ) {
            Ok(canonical) => {
                tracing::info!(
                    target: "zinder::ingest",
                    event = "materialized_view_canonical_secondary_opened",
                    storage_path = %spec.storage_path.display(),
                    secondary_path = %spec.secondary_path.display(),
                    "materialized-view plane opened its canonical secondary"
                );
                return Some(canonical);
            }
            Err(error) if !waiting_logged => {
                waiting_logged = true;
                tracing::info!(
                    target: "zinder::ingest",
                    event = "materialized_view_canonical_secondary_unavailable",
                    error = %error,
                    retry_interval_seconds = CANONICAL_SECONDARY_OPEN_RETRY_INTERVAL.as_secs(),
                    "waiting for the canonical writer to publish a ready store before building materialized views"
                );
            }
            Err(_) => {}
        }
        tokio::select! {
            () = cancel.cancelled() => return None,
            () = tokio::time::sleep(CANONICAL_SECONDARY_OPEN_RETRY_INTERVAL) => {}
        }
    }
}

fn canonical_writer_config(
    command_config: &IngestCommandConfig,
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
) -> CanonicalWriterConfig {
    CanonicalWriterConfig {
        storage_path: command_config.runtime_config.storage_path.clone(),
        resource_budget: command_config.runtime_config.canonical_rocksdb_budget,
        construction: CanonicalConstructionConfig {
            request_timeout: command_config.runtime_config.node.request_timeout,
            pipeline_limits: command_config.runtime_config.construction.pipeline_limits,
            network_upgrade_activations,
        },
        checkpoint_height: command_config
            .runtime_config
            .run_overrides
            .checkpoint_height,
        raw_blob_retention: command_config.runtime_config.raw_blob_policy.to_retention(),
        reorg_window_blocks: command_config.runtime_config.reorg_window_blocks,
        follow: CanonicalFollowConfig {
            request_timeout: command_config.runtime_config.node.request_timeout,
            poll_interval: command_config.runtime_config.follow.poll_interval,
            lag_threshold_blocks: command_config.runtime_config.follow.lag_threshold_blocks,
            max_replacement_bytes: command_config.runtime_config.follow.max_replacement_bytes,
            target_height: command_config.runtime_config.run_overrides.target_height,
            event_retention_window: command_config.retention.chain_event_window(),
            event_retention_check_interval: command_config.retention.chain_event_check_interval(),
            mempool_ready_gate: None,
        },
    }
}

fn log_canonical_writer_start(
    command_config: &IngestCommandConfig,
    writer_config: &CanonicalWriterConfig,
) {
    tracing::info!(
        target: "zinder::ingest",
        event = "canonical_writer_started",
        network = encode_zinder_native_chain_name(command_config.runtime_config.node.network),
        storage_path = %writer_config.storage_path.display(),
        json_rpc_addr = command_config.runtime_config.node.json_rpc_addr.as_str(),
        workload = "wallet",
        schema_version = zinder_store::CANONICAL_STORE_SCHEMA_VERSION,
        raw_blob_retention = %writer_config.raw_blob_retention,
        reorg_window_blocks = writer_config.reorg_window_blocks,
        checkpoint_height = ?writer_config.checkpoint_height.map(BlockHeight::value),
        target_height = ?writer_config.follow.target_height.map(BlockHeight::value),
        historical_prevout_reads = 0_u64,
        cross_block_wallet_reads = 0_u64,
        "canonical writer started"
    );
}

fn resolve_wallet_serving_modifiers(command_config: &mut IngestCommandConfig) {
    if !matches!(command_config.coverage, IngestCoverage::WalletServing) {
        return;
    }

    let checkpoint_height = BlockHeight::new(0);
    command_config.runtime_config.run_overrides = CanonicalRunOverrides {
        target_height: command_config.runtime_config.run_overrides.target_height,
        checkpoint_height: Some(checkpoint_height),
        allow_reorg_window_settlement: command_config
            .runtime_config
            .run_overrides
            .allow_reorg_window_settlement,
        checkpoint: command_config
            .runtime_config
            .run_overrides
            .checkpoint
            .clone(),
    };
    tracing::info!(
        target: "zinder::ingest",
        event = "wallet_serving_modifiers_resolved",
        from_height = 1_u32,
        checkpoint_height = checkpoint_height.value(),
        "resolved complete wallet-serving history"
    );
}

async fn ensure_node_capabilities(
    source: &ZebraJsonRpcSource,
    readiness: &Readiness,
) -> Result<NodeCapabilities, IngestConfigError> {
    let probed_capabilities = match source.probe_capabilities().await {
        Ok(capabilities) => capabilities,
        Err(probe_error) => {
            let detail = zinder_runtime::NodeUnavailableDetail::first_iteration(
                probe_error.upstream_classification().label(),
                probe_error.to_string(),
            );
            readiness.set(ReadinessState::node_unavailable_with_detail(detail, None));
            tracing::warn!(
                target: "zinder::ingest",
                event = "node_capability_probe_failed",
                error = %probe_error,
                "node capability probe failed"
            );
            return Err(IngestError::from(probe_error).into());
        }
    };

    let advertised: Vec<&'static str> = probed_capabilities
        .iter()
        .map(zinder_source::NodeCapability::name)
        .collect();
    if probed_capabilities.supports(NodeCapability::OpenRpcDiscovery) {
        tracing::info!(
            target: "zinder::ingest",
            event = "node_capabilities_probed",
            advertised = ?advertised,
            "node advertised capabilities discovered via rpc.discover"
        );
    } else {
        tracing::warn!(
            target: "zinder::ingest",
            event = "node_capability_probe_fallback",
            advertised = ?advertised,
            "node capability probe used baseline capabilities because rpc.discover was unavailable"
        );
    }

    if let Err(error) = require_ingest_node_capabilities(probed_capabilities) {
        if let zinder_source::SourceError::NodeCapabilityMissing { capability } = &error {
            readiness.set(ReadinessState::node_capability_missing(capability.name()));
            tracing::error!(
                target: "zinder::ingest",
                event = "node_capability_missing",
                capability = capability.name(),
                "upstream node is missing a capability required by ingestion"
            );
        }
        return Err(IngestError::from(error).into());
    }

    Ok(probed_capabilities)
}

fn require_ingest_node_capabilities(
    capabilities: NodeCapabilities,
) -> Result<(), zinder_source::SourceError> {
    for required in REQUIRED_INGEST_NODE_CAPABILITIES {
        if !capabilities.supports(*required) {
            return Err(zinder_source::SourceError::NodeCapabilityMissing {
                capability: *required,
            });
        }
    }

    Ok(())
}

fn zebra_json_rpc_source_for_target(
    node_source: NodeSourceKind,
    target: &NodeTarget,
) -> Result<ZebraJsonRpcSource, IngestConfigError> {
    match node_source {
        NodeSourceKind::ZebraJsonRpc => ZebraJsonRpcSource::with_options(
            target.network,
            &target.json_rpc_addr,
            target.node_auth.clone(),
            ZebraJsonRpcSourceOptions {
                request_timeout: target.request_timeout,
                max_response_bytes: target.max_response_bytes,
                broadcast_timeout: target.broadcast_timeout,
            },
        )
        .map(|source| source.with_health_config(target.health.clone()))
        .map_err(IngestError::from)
        .map_err(IngestConfigError::from),
    }
}

fn build_live_mempool_source(
    node_target: &NodeTarget,
    json_rpc: &ZebraJsonRpcSource,
    mempool: MempoolIngestSettings,
) -> Arc<dyn MempoolSource> {
    node_target.indexer_grpc_addr.as_ref().map_or_else(
        || {
            tracing::info!(
                target: "zinder::ingest",
                event = "mempool_source_selected",
                backend = "zebra-json-rpc-polling",
                "selected polling mempool source"
            );
            Arc::new(JsonRpcMempoolSource::with_options(
                json_rpc.clone(),
                JsonRpcMempoolSourceOptions {
                    admission_limits: mempool.source_admission_limits,
                    ..JsonRpcMempoolSourceOptions::default()
                },
            )) as Arc<dyn MempoolSource>
        },
        |indexer_endpoint| {
            tracing::info!(
                target: "zinder::ingest",
                event = "mempool_source_selected",
                backend = "zebra-indexer-grpc",
                indexer_endpoint = %indexer_endpoint,
                "selected streaming mempool source"
            );
            Arc::new(ZebraIndexerMempoolSource::with_options(
                ZebraIndexerSourceTarget::new(indexer_endpoint.clone()),
                json_rpc.clone(),
                ZebraIndexerMempoolSourceOptions {
                    admission_limits: mempool.source_admission_limits,
                    ..ZebraIndexerMempoolSourceOptions::default()
                },
            )) as Arc<dyn MempoolSource>
        },
    )
}

fn spawn_upstream_health_probe_for(
    node_target: &NodeTarget,
    json_rpc_source: &ZebraJsonRpcSource,
    readiness: Readiness,
    cancel: CancellationToken,
) -> Option<JoinHandle<()>> {
    let health_config = node_target.health.as_ref()?;
    tracing::info!(
        target: "zinder::ingest",
        event = "upstream_health_probe_started",
        addr = health_config.addr.as_str(),
        poll_interval_ms = u64::try_from(health_config.poll_interval.as_millis())
            .unwrap_or(u64::MAX),
        "upstream health probe started"
    );
    Some(spawn_upstream_health_probe_task(
        Arc::new(json_rpc_source.clone()),
        readiness,
        health_config.poll_interval,
        cancel,
    ))
}

#[allow(
    clippy::print_stdout,
    reason = "--print-config is a structured TOML data dump, not a log event"
)]
fn run_print_config(cli: Cli) -> ExitCode {
    let ops_listen_addr_override = cli.ops_listen_addr;
    let overrides = ingest_overrides_with_ops(&cli, ops_listen_addr_override);
    let config_path = cli.config_path.clone();
    let render_result = match cli.command {
        None | Some(Command::Probe) => print_ingest_config(config_path, overrides),
        Some(Command::VerifyCanonicalReplay(args)) => {
            print_canonical_replay_verification_config(config_path, args)
        }
    };

    match render_result {
        Ok(rendered_toml) => {
            println!("{rendered_toml}");
            ExitCode::SUCCESS
        }
        Err(error) => emit_runtime_error(&error),
    }
}

async fn run_runtime(cli: Cli) -> ExitCode {
    let ops_listen_addr_override = cli.ops_listen_addr;
    let overrides = ingest_overrides_with_ops(&cli, ops_listen_addr_override);
    let config_path = cli.config_path.clone();
    let runtime_result = match cli.command {
        Some(Command::Probe) => run_probe(config_path, overrides).await,
        Some(Command::VerifyCanonicalReplay(args)) => {
            run_canonical_replay_verification(config_path, args)
        }
        None => Box::pin(run_ingest(config_path, overrides)).await,
    };

    match runtime_result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => emit_runtime_error(&error),
    }
}

fn emit_runtime_error(error: &IngestConfigError) -> ExitCode {
    tracing::error!(
        target: "zinder::ingest",
        event = "ingest_run_failed",
        error = %error,
        "ingest run failed"
    );
    ExitCode::FAILURE
}

fn ingest_overrides_with_ops(
    cli: &Cli,
    ops_listen_addr_override: Option<SocketAddr>,
) -> IngestConfigOverrides {
    IngestConfigOverrides {
        network: cli.network.clone(),
        node_source: cli.node_source.clone(),
        json_rpc_addr: cli.json_rpc_addr.clone(),
        node_auth_method: cli.node_auth_method.clone(),
        node_auth_username: cli.node_auth_username.clone(),
        node_auth_path: cli.node_auth_path.clone(),
        storage_path: cli.storage_path.clone(),
        request_timeout_secs: cli.request_timeout_secs,
        max_response_bytes: cli.max_response_bytes,
        reorg_window_blocks: cli.reorg_window_blocks,
        catchup_threshold_blocks: cli.catchup_threshold_blocks,
        canonical_batch_max_blocks: cli.canonical_batch_max_blocks,
        canonical_batch_max_artifact_bytes: cli.canonical_batch_max_artifact_bytes,
        canonical_batch_max_estimated_write_bytes: cli.canonical_batch_max_estimated_write_bytes,
        canonical_batch_min_blocks_before_estimated_write_close: cli
            .canonical_batch_min_blocks_before_estimated_write_close,
        source_segment_max_blocks: cli.source_segment_max_blocks,
        source_segment_target_response_bytes: cli.source_segment_target_response_bytes,
        source_fetch_max_in_flight_requests: cli.source_fetch_max_in_flight_requests,
        source_fetch_max_in_flight_bytes: cli.source_fetch_max_in_flight_bytes,
        block_prepare_concurrency: cli.block_prepare_concurrency,
        poll_interval_ms: cli.poll_interval_ms,
        lag_threshold_blocks: cli.lag_threshold_blocks,
        target_height: cli.target_height,
        checkpoint_height: cli.checkpoint_height,
        allow_reorg_window_settlement: cli.allow_reorg_window_settlement.then_some(true),
        wallet_serving: cli.wallet_serving.then_some(true),
        ingest_control_listen_addr: cli.ingest_control_listen_addr,
        ingest_control_bearer_token_path: cli.ingest_control_token_path.clone(),
        ingest_control_checkpoint_bearer_token_path: None,
        ops_listen_addr: ops_listen_addr_override,
    }
}

#[allow(
    clippy::print_stdout,
    reason = "canonical replay verification emits one machine-readable operator report"
)]
fn run_canonical_replay_verification(
    config_path: Option<PathBuf>,
    args: CanonicalReplayVerificationArgs,
) -> Result<(), IngestConfigError> {
    let command_config =
        config::load_canonical_replay_verification_config(config_path, args.into())?;
    let secondary_store = SecondaryChainStore::open(
        &command_config.storage_path,
        &command_config.secondary_path,
        ChainStoreOptions {
            rocksdb_resource_budget: command_config.canonical_rocksdb_budget,
            ..ChainStoreOptions::for_network(command_config.network)
        },
    )
    .map_err(IngestError::from)?;
    secondary_store.try_catch_up().map_err(IngestError::from)?;
    let report = replay_verification::verify_canonical_replay_store(&secondary_store)?;
    let report_json = report.to_json()?;
    println!("{report_json}");

    Ok(())
}

fn print_ingest_config(
    config_path: Option<PathBuf>,
    overrides: IngestConfigOverrides,
) -> Result<String, IngestConfigError> {
    let command_config = config::load_ingest_config(config_path, overrides)?;
    config::redacted_ingest_config_toml(&command_config)
}

fn print_canonical_replay_verification_config(
    config_path: Option<PathBuf>,
    args: CanonicalReplayVerificationArgs,
) -> Result<String, IngestConfigError> {
    let command_config =
        config::load_canonical_replay_verification_config(config_path, args.into())?;
    config::redacted_canonical_replay_verification_config_toml(&command_config)
}

impl From<CanonicalReplayVerificationArgs> for CanonicalReplayVerificationConfigOverrides {
    fn from(args: CanonicalReplayVerificationArgs) -> Self {
        Self {
            network: args.network,
            storage_path: args.storage_path,
            secondary_path: args.secondary_path,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use tempfile::tempdir;
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    use super::{
        CanonicalControlTasks, MaterializedViewPlaneSpec, NodeCapabilities, NodeCapability,
        ZebraJsonRpcSource, coordinate_canonical_writer_lifecycle, explorer_view_preset,
        explorer_views_start_backfills, materialized_view_secondary_path,
        require_ingest_node_capabilities, run_materialized_view_plane, supervise_canonical_writer,
    };

    #[test]
    fn explorer_views_task_composition_matches_admission() {
        assert!(!explorer_views_start_backfills(false));
        assert!(explorer_views_start_backfills(true));
        assert!(explorer_view_preset(false).is_none());
        assert_eq!(
            explorer_view_preset(true),
            Some(zinder_materialized_views::MaterializedViewPreset::Explorer)
        );
    }

    #[tokio::test]
    async fn disabled_explorer_views_leaves_storage_and_readiness_untouched()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempdir()?;
        let storage_path = temporary.path().join("canonical");
        let materialized_view_path =
            zinder_materialized_views::MaterializedViewStore::path_for_canonical(&storage_path);
        fs::create_dir_all(&materialized_view_path)?;
        let sentinel_path = materialized_view_path.join("disabled-sentinel");
        fs::write(&sentinel_path, b"leave this store untouched")?;
        let readiness = zinder_runtime::Readiness::default();
        let spec = MaterializedViewPlaneSpec {
            explorer_views: false,
            secondary_path: materialized_view_secondary_path(&storage_path),
            storage_path,
            canonical_rocksdb_budget: zinder_store::RocksDbResourceBudget::for_local_tests(),
            materialized_view_rocksdb_budget: zinder_store::RocksDbResourceBudget::for_local_tests(
            ),
            activations: Arc::new(zinder_core::NetworkUpgradeActivations::empty(
                zinder_core::Network::ZcashRegtest,
            )),
            raw_blob_retention: zinder_store::RawBlobRetention::None,
            reorg_policy: zinder_store::CanonicalReorgPolicy::new(100)?,
            chain_event_retention_window: None,
            cursor_at_risk_warning: std::time::Duration::from_mins(1),
        };

        run_materialized_view_plane(
            spec,
            readiness.clone(),
            zinder_ingest::HistoricalWorkGate::new(readiness.clone()),
            CancellationToken::new(),
        )
        .await;

        let report = readiness.report();
        assert!(report.materialized_view_preset.is_none());
        assert!(report.materialized_view_identities.is_empty());
        assert!(materialized_view_path.is_dir());
        assert_eq!(fs::read(&sentinel_path)?, b"leave this store untouched");
        assert!(
            !temporary
                .path()
                .join("canonical.materialized-view-secondary")
                .exists()
        );
        Ok(())
    }

    fn reorg_window_exceeded_writer_result() -> Result<(), zinder_ingest::CanonicalWriterError> {
        Err(zinder_ingest::CanonicalWriterError::Follow(
            zinder_ingest::CanonicalFollowError::ReorgWindowExceeded(Box::new(
                zinder_ingest::CanonicalReorgWindowExceeded {
                    local_tip: zinder_core::BlockId::new(
                        zinder_core::BlockHeight::new(4),
                        zinder_core::BlockHash::from_bytes([4; 32]),
                    ),
                    source_tip: zinder_core::BlockId::new(
                        zinder_core::BlockHeight::new(4),
                        zinder_core::BlockHash::from_bytes([5; 32]),
                    ),
                    settled_tip: zinder_core::BlockId::new(
                        zinder_core::BlockHeight::new(2),
                        zinder_core::BlockHash::from_bytes([2; 32]),
                    ),
                    required_depth: 3,
                    configured_window_blocks: 2,
                    covered_depth_blocks: 2,
                },
            )),
        ))
    }

    #[tokio::test]
    async fn writer_reorg_window_exceeded_drains_workers_then_parks_ops_until_termination()
    -> Result<(), Box<dyn std::error::Error>> {
        let readiness = zinder_runtime::Readiness::default();
        let termination = tokio_util::sync::CancellationToken::new();
        let workers_drained = Arc::new(AtomicBool::new(false));
        let ops_shutdown = Arc::new(AtomicBool::new(false));
        let drain_workers = {
            let workers_drained = Arc::clone(&workers_drained);
            let readiness = readiness.clone();
            async move {
                readiness.set(zinder_runtime::ReadinessState::starting());
                workers_drained.store(true, Ordering::SeqCst);
            }
        };
        let shutdown_ops = {
            let ops_shutdown = Arc::clone(&ops_shutdown);
            async move {
                ops_shutdown.store(true, Ordering::SeqCst);
                Ok(())
            }
        };
        let writer_result = reorg_window_exceeded_writer_result();
        let mut lifecycle = Box::pin(coordinate_canonical_writer_lifecycle(
            writer_result,
            &termination,
            &readiness,
            drain_workers,
            shutdown_ops,
        ));

        tokio::select! {
            outcome = &mut lifecycle => {
                return Err(std::io::Error::other(format!(
                    "reorg-window lifecycle returned before termination: {outcome:?}"
                )).into());
            }
            () = async {
                while !workers_drained.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            } => {}
        }

        let readiness_report = readiness.report();
        assert!(matches!(
            readiness_report.cause,
            zinder_runtime::ReadinessCause::ReorgWindowExceeded {
                depth: 3,
                configured: 2,
                covered: Some(2),
            }
        ));
        assert_eq!(readiness_report.current_height, Some(4));
        assert_eq!(
            readiness_report.phase,
            Some(zinder_runtime::IngestPhase::FollowingTip)
        );
        assert!(!ops_shutdown.load(Ordering::SeqCst));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut lifecycle)
                .await
                .is_err(),
            "reorg-window lifecycle must keep the ops endpoint alive until termination"
        );

        termination.cancel();
        lifecycle.await?;
        assert!(ops_shutdown.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn non_reorg_writer_error_drains_workers_shuts_ops_and_returns_without_termination()
    -> Result<(), Box<dyn std::error::Error>> {
        let readiness = zinder_runtime::Readiness::default();
        let termination = tokio_util::sync::CancellationToken::new();
        let workers_drained = Arc::new(AtomicBool::new(false));
        let ops_shutdown = Arc::new(AtomicBool::new(false));
        let drain_workers = {
            let workers_drained = Arc::clone(&workers_drained);
            async move {
                workers_drained.store(true, Ordering::SeqCst);
            }
        };
        let shutdown_ops = {
            let ops_shutdown = Arc::clone(&ops_shutdown);
            async move {
                ops_shutdown.store(true, Ordering::SeqCst);
                Ok(())
            }
        };
        let writer_result = Err(zinder_ingest::CanonicalWriterError::ControlServer {
            reason: "test control server stopped".to_owned(),
        });

        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            coordinate_canonical_writer_lifecycle(
                writer_result,
                &termination,
                &readiness,
                drain_workers,
                shutdown_ops,
            ),
        )
        .await?;
        let error = outcome
            .err()
            .ok_or("non-reorg writer error must propagate")?;

        assert!(matches!(
            error,
            crate::config::IngestConfigError::CanonicalWriter(
                zinder_ingest::CanonicalWriterError::ControlServer { reason }
            )
                if reason == "test control server stopped"
        ));
        assert!(workers_drained.load(Ordering::SeqCst));
        assert!(ops_shutdown.load(Ordering::SeqCst));
        assert!(!termination.is_cancelled());
        Ok(())
    }

    #[tokio::test]
    async fn successful_writer_propagates_ops_shutdown_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let readiness = zinder_runtime::Readiness::default();
        let termination = CancellationToken::new();
        let shutdown_ops = async {
            Err(zinder_runtime::OpsServerError::Transport {
                source: std::io::Error::other("synthetic ops shutdown failure"),
            })
        };

        let error = coordinate_canonical_writer_lifecycle(
            Ok(()),
            &termination,
            &readiness,
            async {},
            shutdown_ops,
        )
        .await
        .err()
        .ok_or("operational endpoint shutdown failure must propagate")?;

        assert!(matches!(
            error,
            crate::config::IngestConfigError::OpsServer(
                zinder_runtime::OpsServerError::Transport { .. }
            )
        ));
        Ok(())
    }

    fn test_control_tasks(mempool_retention: JoinHandle<()>) -> CanonicalControlTasks {
        CanonicalControlTasks {
            server: Some(tokio::spawn(std::future::pending::<
                Result<(), tonic::transport::Error>,
            >())),
            commands: None,
            mempool_owner: Some(tokio::spawn(std::future::pending())),
            mempool_retention: Some(mempool_retention),
            server_completed: false,
        }
    }

    async fn stop_test_control_tasks(control_tasks: &mut CanonicalControlTasks) {
        if let Some(server) = control_tasks.server.take() {
            server.abort();
            let _join_result = server.await;
        }
        if let Some(mempool_owner) = control_tasks.mempool_owner.take() {
            mempool_owner.abort();
            let _join_result = mempool_owner.await;
        }
        if let Some(mempool_retention) = control_tasks.mempool_retention.take() {
            mempool_retention.abort();
            let _join_result = mempool_retention.await;
        }
    }

    #[test]
    fn ingest_capability_validation_accepts_zebra_baseline()
    -> Result<(), Box<dyn std::error::Error>> {
        let capabilities = ZebraJsonRpcSource::baseline_capabilities();

        require_ingest_node_capabilities(capabilities)?;

        Ok(())
    }

    #[test]
    fn ingest_capability_validation_rejects_missing_tree_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let capabilities = NodeCapabilities::new([
            NodeCapability::JsonRpc,
            NodeCapability::BestChainBlocks,
            NodeCapability::TipId,
            NodeCapability::SubtreeRoots,
        ])?;

        let error = require_ingest_node_capabilities(capabilities)
            .err()
            .ok_or("missing tree-state capability must be rejected")?;
        assert!(matches!(
            error,
            zinder_source::SourceError::NodeCapabilityMissing {
                capability: NodeCapability::TreeState
            }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn supervisor_fails_closed_when_mempool_retention_stops()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancel = CancellationToken::new();
        let mut control_tasks = test_control_tasks(tokio::spawn(async {}));

        let error = supervise_canonical_writer(
            std::future::pending::<Result<(), zinder_ingest::CanonicalWriterError>>(),
            &cancel,
            &mut control_tasks,
        )
        .await
        .err()
        .ok_or("retention exit must fail the canonical writer")?;

        assert!(matches!(
            error,
            zinder_ingest::CanonicalWriterError::MempoolRetention { ref reason }
                if reason == "mempool retention task stopped unexpectedly"
        ));
        assert!(cancel.is_cancelled());
        assert!(control_tasks.mempool_retention.is_none());
        stop_test_control_tasks(&mut control_tasks).await;

        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic,
        reason = "the supervisor test must inject a panicked retention task"
    )]
    async fn supervisor_reports_mempool_retention_join_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancel = CancellationToken::new();
        let mempool_retention = tokio::spawn(async {
            std::panic::panic_any("mempool retention test fault");
        });
        let mut control_tasks = test_control_tasks(mempool_retention);

        let error = supervise_canonical_writer(
            std::future::pending::<Result<(), zinder_ingest::CanonicalWriterError>>(),
            &cancel,
            &mut control_tasks,
        )
        .await
        .err()
        .ok_or("retention join failure must fail the canonical writer")?;

        assert!(matches!(
            error,
            zinder_ingest::CanonicalWriterError::MempoolRetention { ref reason }
                if reason.contains("panicked")
        ));
        assert!(cancel.is_cancelled());
        assert!(control_tasks.mempool_retention.is_none());
        stop_test_control_tasks(&mut control_tasks).await;

        Ok(())
    }

    #[tokio::test]
    async fn supervisor_prioritizes_cancellation_over_completed_retention()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancel = CancellationToken::new();
        let retention_cancel = cancel.clone();
        let mut control_tasks = test_control_tasks(tokio::spawn(async move {
            retention_cancel.cancelled().await;
        }));
        let writer_cancel = cancel.clone();
        let cancellation_trigger = cancel.clone();
        let cancellation_task = tokio::spawn(async move {
            tokio::task::yield_now().await;
            cancellation_trigger.cancel();
        });

        supervise_canonical_writer(
            async move {
                writer_cancel.cancelled().await;
                Ok::<(), zinder_ingest::CanonicalWriterError>(())
            },
            &cancel,
            &mut control_tasks,
        )
        .await?;

        cancellation_task.await?;
        assert!(control_tasks.mempool_retention.is_some());
        stop_test_control_tasks(&mut control_tasks).await;

        Ok(())
    }
}
