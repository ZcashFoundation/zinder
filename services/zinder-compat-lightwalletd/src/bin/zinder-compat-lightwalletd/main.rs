//! Zinder lightwalletd-compatible gRPC server entry point.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
};
use zinder_core::wire::encode_zinder_native_chain_name;

use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;
use zinder_compat_lightwalletd::{
    IngestControlMempoolSurface, LightwalletdGrpcAdapter, LightwalletdPing, NodeTipWitness,
    spawn_ingest_control_tip_change_publisher, spawn_node_tip_witness,
};
use zinder_runtime::{
    OpsHealthcheckArgs, OpsServerError, Readiness, RuntimeService, StartupPhase, SupervisedRuntime,
    TracingSubscriberError, host_cpu_meets_compiled_baseline,
    host_cpu_meets_pasta_backend_baseline, install_metrics_recorder_for_service,
    install_tracing_subscriber, section_defaults::default_ops_listen_addr, spawn_ops_endpoint_for,
};
use zinder_source::{DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS, NodeTarget, ZebraJsonRpcSource};

mod browser_transport;
mod catalog_refresh;
mod publish_history;
use catalog_refresh::{CatalogRefresh, CatalogSource};
mod compact_block_serving;
mod config;
mod supervision;
use config::{CompatServing, LightwalletdConfigError, LightwalletdConfigOverrides};
use supervision::{CompactBlocksServingTasks, WalletServingTasks};
use zinder_query::{
    AdmittedIngestControl, CanonicalReader as _, DEFAULT_TREE_STATE_REPLAY_WAIT,
    DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT, OrderedBroadcaster, WalletServingPairConfig,
    WalletServingPairPublisher, WalletServingQuery, WalletServingReadiness,
    spawn_wallet_ingest_control_readiness_probe, spawn_wallet_node_readiness_probe,
};

#[global_allocator]
static GLOBAL_ALLOCATOR: zinder_runtime::RuntimeAllocator = zinder_runtime::RuntimeAllocator;

#[derive(Parser)]
#[command(name = "zinder-compat-lightwalletd")]
#[command(about = "Zinder lightwalletd-compatible gRPC server")]
#[command(version)]
struct Cli {
    /// TOML configuration file loaded before environment variables and CLI overrides.
    #[arg(long = "config", global = true)]
    config_path: Option<PathBuf>,
    /// Print the resolved configuration without opening storage or binding.
    #[arg(long = "print-config", global = true)]
    print_config: bool,
    /// Network name, such as zcash-regtest.
    #[arg(long)]
    network: Option<String>,
    /// Compatibility capability: wallet or compact-blocks.
    #[arg(long)]
    serving: Option<CompatServing>,
    /// Canonical primary path replicated only through an immutable secondary.
    #[arg(long = "canonical-primary-path")]
    canonical_primary_path: Option<PathBuf>,
    /// Root containing this process's two canonical secondary generations.
    #[arg(long = "canonical-secondary-root")]
    canonical_secondary_root: Option<PathBuf>,
    /// Wallet primary path replicated only through an immutable secondary.
    #[arg(long = "wallet-primary-path")]
    wallet_primary_path: Option<PathBuf>,
    /// Root containing this process's two wallet secondary generations.
    #[arg(long = "wallet-secondary-root")]
    wallet_secondary_root: Option<PathBuf>,
    /// Private `zinder-ingest` control gRPC endpoint.
    #[arg(long = "ingest-control-addr")]
    ingest_control_addr: Option<String>,
    /// Path to a file containing the shared-secret bearer token used by the
    /// `IngestControl` writer. Required when the writer enforces auth.
    #[arg(long = "ingest-control-token-path")]
    ingest_control_token_path: Option<PathBuf>,
    /// Lightwalletd-compatible gRPC listen address, such as 127.0.0.1:9067.
    #[arg(long = "listen-addr")]
    listen_addr: Option<SocketAddr>,
    /// Exact canonical replacement-depth identity expected from the writer.
    #[arg(long = "reorg-window-blocks")]
    reorg_window_blocks: Option<u32>,
    /// Operational HTTP endpoint listen address for /healthz, /readyz, /metrics.
    #[arg(long = "ops-listen-addr")]
    ops_listen_addr: Option<SocketAddr>,
    /// Node JSON-RPC address used for transaction broadcast and the recommended fee. Omit to disable both.
    #[arg(long = "node-json-rpc-addr")]
    node_json_rpc_addr: Option<String>,
    /// Answer the `Ping` load-testing RPC, as `lightwalletd --ping-very-insecure` does.
    #[arg(long = "ping-very-insecure")]
    ping_very_insecure: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Probe the operational endpoint once and exit 0 only when it answers
    /// HTTP 200. Reads only `[ops] listen_addr` from the `--config` file.
    Healthcheck(OpsHealthcheckArgs),
    /// Append complete settled segments to a signed wallet history catalog.
    PublishShieldedHistory(publish_history::PublishHistory),
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Some(Command::Healthcheck(healthcheck)) = &cli.command {
        return run_healthcheck(healthcheck, cli.config_path.as_deref());
    }
    if let Err(error) = install_tracing_subscriber() {
        return emit_tracing_subscriber_error(&error);
    }

    if !host_cpu_meets_compiled_baseline() {
        return ExitCode::FAILURE;
    }
    if !host_cpu_meets_pasta_backend_baseline() {
        return ExitCode::FAILURE;
    }

    if cli.print_config {
        return run_print_config(cli);
    }

    run_runtime(cli).await
}

#[allow(
    clippy::print_stdout,
    reason = "--print-config is a structured TOML data dump, not a log event"
)]
fn run_print_config(cli: Cli) -> ExitCode {
    let config_path = cli.config_path.clone();
    let render_result = config::load_lightwalletd_config(config_path, cli.into())
        .and_then(|cfg| config::lightwalletd_config_toml(&cfg));

    match render_result {
        Ok(rendered_toml) => {
            println!("{rendered_toml}");
            ExitCode::SUCCESS
        }
        Err(error) => emit_runtime_error(&error),
    }
}

async fn run_runtime(cli: Cli) -> ExitCode {
    match run_lightwalletd(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => emit_runtime_error(&error),
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "lightwalletd bootstrap composes the readiness, ops, store, node broadcast, mempool surface, tip-change publisher, secondary-catchup, and gRPC-server subsystems; the linear sequence is the operator-facing flow and intentionally lives in one function so failure ordering is auditable."
)]
async fn run_lightwalletd(mut cli: Cli) -> Result<(), LightwalletdConfigError> {
    let load_config_phase = StartupPhase::LoadConfig.start();
    let config_path = cli.config_path.clone();
    let command = cli.command.take();
    let lightwalletd_config = match config::load_lightwalletd_config(config_path, cli.into()) {
        Ok(cfg) => {
            load_config_phase.complete();
            cfg
        }
        Err(error) => {
            load_config_phase.fail(&error);
            return Err(error);
        }
    };
    if let Some(Command::PublishShieldedHistory(args)) = command {
        return args.publish(&lightwalletd_config).await;
    }
    tracing::info!(
        target: "zinder::compat_lightwalletd",
        event = "compat_config_resolved",
        requested_serving = lightwalletd_config.serving.as_str(),
        "resolved compatibility serving configuration"
    );
    if lightwalletd_config.serving != CompatServing::Wallet {
        return run_canonical(lightwalletd_config).await;
    }
    let Some(wallet_primary_path) = lightwalletd_config.wallet_primary_path.clone() else {
        return Err(LightwalletdConfigError::Config(
            zinder_runtime::ConfigError::invalid("wallet serving requires wallet.path"),
        ));
    };
    let Some(wallet_secondary_root) = lightwalletd_config.wallet_secondary_root.clone() else {
        return Err(LightwalletdConfigError::Config(
            zinder_runtime::ConfigError::invalid("wallet serving requires wallet.secondary_path"),
        ));
    };
    let Some(wallet_rocksdb_budget) = lightwalletd_config.wallet_rocksdb_budget else {
        return Err(LightwalletdConfigError::Config(
            zinder_runtime::ConfigError::invalid("wallet serving requires wallet.rocksdb"),
        ));
    };
    if lightwalletd_config.ops_listen_addr.is_some() {
        install_metrics_recorder_for_service(
            RuntimeService::CompatLightwalletd,
            env!("CARGO_PKG_VERSION"),
            encode_zinder_native_chain_name(lightwalletd_config.network),
        )
        .map_err(OpsServerError::from)?;
    }
    let readiness = Readiness::default();
    let serving_readiness = WalletServingReadiness::with_node_and_ingest_control(readiness.clone());
    let admit_ingest_control_phase = StartupPhase::AdmitIngestControl.start();
    let ingest_control = match AdmittedIngestControl::connect(
        &lightwalletd_config.ingest_control_addr,
        lightwalletd_config.ingest_control_bearer_token.as_ref(),
        lightwalletd_config.network,
    )
    .await
    {
        Ok(ingest_control) => {
            admit_ingest_control_phase.complete();
            ingest_control
        }
        Err(error) => {
            admit_ingest_control_phase.fail(&error);
            return Err(error.into());
        }
    };
    let start_api_phase = StartupPhase::StartApi.start();

    let node_broadcast = match build_node_broadcast(
        lightwalletd_config.broadcaster.as_ref(),
        &lightwalletd_config.node_fallbacks,
    ) {
        Ok(node_broadcast) => node_broadcast,
        Err(error) => {
            start_api_phase.fail(&error);
            return Err(error);
        }
    };
    let open_storage_phase = StartupPhase::OpenStorage.start();
    let (serving_pair_publisher, serving_pair_slot) =
        WalletServingPairPublisher::bootstrap_with_admitted_ingest_control(
            WalletServingPairConfig {
                canonical_primary_path: lightwalletd_config.storage.path.clone(),
                canonical_secondary_root: lightwalletd_config.storage.secondary_path.clone(),
                wallet_primary_path,
                wallet_secondary_root,
                network: lightwalletd_config.network,
                expected_raw_blob_retention: lightwalletd_config
                    .storage
                    .expected_raw_blob_retention,
                canonical_reorg_policy: lightwalletd_config.canonical_reorg_policy,
                canonical_resource_budget: lightwalletd_config.storage.canonical_rocksdb_budget,
                wallet_resource_budget: wallet_rocksdb_budget,
                catchup_interval: lightwalletd_config.storage.secondary_catchup_interval,
                convergence_timeout: lightwalletd_config.storage.initial_catchup_timeout,
                convergence_attempts: lightwalletd_config.pair_convergence_attempts,
                replica_lag_threshold_chain_epochs: lightwalletd_config
                    .storage
                    .secondary_replica_lag_threshold_chain_epochs,
                serving_pair_staleness_ceiling: lightwalletd_config
                    .storage
                    .serving_pair_staleness_ceiling,
            },
            serving_readiness.clone(),
            &ingest_control,
        )
        .await?;
    let catalog_refresh = match &lightwalletd_config.shielded_history {
        Some(args) => Some(
            CatalogRefresh::admit(args, CatalogSource::Wallet(serving_pair_slot.clone())).await?,
        ),
        None => None,
    };
    let visible_height = Some(
        serving_pair_slot
            .capture()
            .canonical_fence()
            .visible_tip()
            .height
            .value(),
    );
    open_storage_phase.complete();
    let (incoming, listen_addr) = browser_transport::bind(lightwalletd_config.listen_addr)?;

    let tree_state_replay_wait = lightwalletd_config
        .tree_state_replay_wait
        .unwrap_or(DEFAULT_TREE_STATE_REPLAY_WAIT);
    let tree_state_replay_waiters_per_slot = lightwalletd_config
        .tree_state_replay_waiters_per_slot
        .unwrap_or(DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT);
    let wallet_query = WalletServingQuery::from_serving_pair_slot(serving_pair_slot.clone())
        .with_node_broadcast(node_broadcast.clone())
        .with_tree_state_replay_wait(tree_state_replay_wait)
        .with_tree_state_replay_waiters_per_slot(tree_state_replay_waiters_per_slot);
    let runtime = SupervisedRuntime::new(RuntimeService::CompatLightwalletd);
    let cancel = runtime.stopping();
    let (node_tip_witness, node_tip_witness_handle) =
        spawn_configured_node_tip_witness(&lightwalletd_config, &cancel);
    let grpc_adapter = LightwalletdGrpcAdapter::from_admitted_compatibility_query(wallet_query)?
        .with_max_concurrent_block_range_streams(
            lightwalletd_config.max_concurrent_block_range_streams,
        )
        .with_stale_tip_limit(lightwalletd_config.stale_tip_limit)
        .with_node_tip_witness(node_tip_witness)
        .with_ping(configured_ping(&lightwalletd_config, &cancel));
    let grpc_adapter = match &catalog_refresh {
        Some(refresh) => grpc_adapter.with_published_shielded_history(refresh.history.clone()),
        None => grpc_adapter,
    };
    let ops_handle = spawn_ops_endpoint_for(
        RuntimeService::CompatLightwalletd,
        lightwalletd_config.ops_listen_addr,
        env!("CARGO_PKG_VERSION"),
        encode_zinder_native_chain_name(lightwalletd_config.network),
        serving_readiness.runtime_readiness(),
        Arc::from([]),
    )
    .await?;
    let _signal_handle = runtime.request_shutdown_on_terminating_signal();
    let node_readiness_handle = spawn_wallet_node_readiness_probe(
        node_broadcast,
        serving_pair_slot,
        serving_readiness.clone(),
        lightwalletd_config
            .broadcaster
            .as_ref()
            .and_then(|target| target.health.as_ref())
            .map_or_else(
                || std::time::Duration::from_millis(DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS),
                |health| health.poll_interval,
            ),
        cancel.clone(),
    );
    // Health and serving-pair refresh observe the same ingest control plane, so
    // they intentionally share one configured freshness cadence.
    let ingest_control_readiness_handle = spawn_wallet_ingest_control_readiness_probe(
        ingest_control,
        serving_readiness.clone(),
        lightwalletd_config.storage.secondary_catchup_interval,
        cancel.clone(),
    );
    let serving_pair_publisher_handle =
        serving_pair_publisher.spawn(cancel.clone(), runtime.serving_drained());
    metrics::gauge!("zinder_compat_serving_info", "serving" => "wallet").set(1.0);
    let (grpc_adapter, tip_publisher_handle) =
        with_ingest_mempool(grpc_adapter, &lightwalletd_config, &cancel);

    tracing::info!(
        target: "zinder::compat_lightwalletd",
        event = "compat_started",
        serving = "wallet",
        network = encode_zinder_native_chain_name(lightwalletd_config.network),
        %listen_addr,
        visible_height = ?visible_height,
        max_concurrent_block_range_streams =
            lightwalletd_config.max_concurrent_block_range_streams.get(),
        stale_tip_limit_secs = ?lightwalletd_config
            .stale_tip_limit
            .limit()
            .map(|limit| limit.as_secs()),
        tree_state_replay_wait_ms = tree_state_replay_wait.as_millis(),
        tree_state_replay_waiters_per_slot = tree_state_replay_waiters_per_slot.get(),
        ping_very_insecure = lightwalletd_config.ping_very_insecure,
        "lightwalletd-compatible gRPC server started"
    );

    let server = browser_transport::serve(
        &runtime,
        grpc_adapter.into_server(),
        incoming,
        &lightwalletd_config,
        &readiness,
    )?;

    start_api_phase.complete();
    StartupPhase::Ready.start().complete();

    let tasks = WalletServingTasks {
        serving_pair_publisher: serving_pair_publisher_handle,
        node_readiness_probe: node_readiness_handle,
        ingest_control_readiness_probe: ingest_control_readiness_handle,
        tip_change_publisher: tip_publisher_handle,
        node_tip_witness: node_tip_witness_handle,
        operations: ops_handle,
    }
    .supervised();
    let tasks = CatalogRefresh::supervise(
        catalog_refresh,
        lightwalletd_config.storage.secondary_catchup_interval,
        cancel,
        tasks,
    );
    let runtime_outcome = runtime
        .supervise(server, tasks, || serving_readiness.publish_shutting_down())
        .await;

    tracing::info!(
        target: "zinder::compat_lightwalletd",
        event = "compat_stopped",
        serving = "wallet",
        "lightwalletd-compatible gRPC server stopped"
    );
    Ok(runtime_outcome?)
}

fn with_ingest_mempool<Query>(
    grpc_adapter: LightwalletdGrpcAdapter<Query>,
    lightwalletd_config: &config::LightwalletdConfig,
    cancel: &CancellationToken,
) -> (LightwalletdGrpcAdapter<Query>, tokio::task::JoinHandle<()>) {
    let mempool_surface = Arc::new({
        let mut surface =
            IngestControlMempoolSurface::new(lightwalletd_config.ingest_control_addr.clone());
        if let Some(token) = lightwalletd_config.ingest_control_bearer_token.clone() {
            surface = surface.with_bearer_token(token);
        }
        surface
    });
    let (tip_change_watcher, tip_publisher_handle) = spawn_ingest_control_tip_change_publisher(
        lightwalletd_config.ingest_control_addr.clone(),
        lightwalletd_config.ingest_control_bearer_token.clone(),
        cancel.clone(),
    );
    let grpc_adapter = grpc_adapter
        .with_mempool_surface(mempool_surface)
        .with_tip_change_watcher(tip_change_watcher);

    (grpc_adapter, tip_publisher_handle)
}

enum CanonicalComposition {
    CompactBlocks,
    #[cfg(feature = "light-wallet-qualification")]
    Qualification {
        source: Box<ZebraJsonRpcSource>,
        ingest: AdmittedIngestControl,
        poll_interval: std::time::Duration,
    },
}

#[cfg(feature = "light-wallet-qualification")]
impl CanonicalComposition {
    async fn admit(config: &config::LightwalletdConfig) -> Result<Self, LightwalletdConfigError> {
        match config.serving {
            CompatServing::LightWalletQualification => {
                let target = config.broadcaster.as_ref().ok_or_else(|| {
                    zinder_runtime::ConfigError::invalid(
                        "light-wallet qualification requires [node]",
                    )
                })?;
                let source = ZebraJsonRpcSource::from_node_target(target)
                    .map_err(|error| LightwalletdConfigError::Source(Box::new(error)))?;
                let ingest = AdmittedIngestControl::connect(
                    &config.ingest_control_addr,
                    config.ingest_control_bearer_token.as_ref(),
                    config.network,
                )
                .await?;
                let poll_interval = target.health.as_ref().map_or_else(
                    || std::time::Duration::from_millis(DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS),
                    |health| health.poll_interval,
                );
                Ok(Self::Qualification {
                    source: Box::new(source),
                    ingest,
                    poll_interval,
                })
            }
            CompatServing::CompactBlocks | CompatServing::Wallet => Ok(Self::CompactBlocks),
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "The compact serving branch keeps admission, readiness, server startup, and shutdown ordering auditable in one linear composition root."
)]
async fn run_canonical(
    lightwalletd_config: config::LightwalletdConfig,
) -> Result<(), LightwalletdConfigError> {
    if lightwalletd_config.ops_listen_addr.is_some() {
        install_metrics_recorder_for_service(
            RuntimeService::CompatLightwalletd,
            env!("CARGO_PKG_VERSION"),
            encode_zinder_native_chain_name(lightwalletd_config.network),
        )
        .map_err(OpsServerError::from)?;
    }
    let readiness = Readiness::default();
    #[cfg(feature = "light-wallet-qualification")]
    let composition = CanonicalComposition::admit(&lightwalletd_config).await?;
    #[cfg(not(feature = "light-wallet-qualification"))]
    let composition = CanonicalComposition::CompactBlocks;
    let serving_readiness = match &composition {
        #[cfg(feature = "light-wallet-qualification")]
        CanonicalComposition::Qualification { .. } => {
            compact_block_serving::CompactServingReadiness::for_qualification(readiness.clone())
        }
        CanonicalComposition::CompactBlocks => {
            compact_block_serving::CompactServingReadiness::new(readiness.clone())
        }
    };
    let start_api_phase = StartupPhase::StartApi.start();
    let open_storage_phase = StartupPhase::OpenStorage.start();
    let (publisher, slot) = compact_block_serving::CompactBlockPublisher::bootstrap(
        compact_block_serving::CompactBlockServingConfig {
            canonical_primary_path: lightwalletd_config.storage.path.clone(),
            canonical_secondary_root: lightwalletd_config.storage.secondary_path.clone(),
            network: lightwalletd_config.network,
            raw_blob_retention: lightwalletd_config.storage.expected_raw_blob_retention,
            reorg_policy: lightwalletd_config.canonical_reorg_policy,
            resource_budget: lightwalletd_config.storage.canonical_rocksdb_budget,
            catchup_interval: lightwalletd_config.storage.secondary_catchup_interval,
            convergence_timeout: lightwalletd_config.storage.initial_catchup_timeout,
            convergence_attempts: lightwalletd_config.pair_convergence_attempts.get(),
            staleness_ceiling: lightwalletd_config.storage.serving_pair_staleness_ceiling,
            lag_threshold: lightwalletd_config
                .storage
                .secondary_replica_lag_threshold_chain_epochs,
        },
        serving_readiness.clone(),
        &lightwalletd_config.ingest_control_addr,
        lightwalletd_config.ingest_control_bearer_token.as_ref(),
    )
    .await
    .map_err(LightwalletdConfigError::CompactServing)?;
    let catalog_refresh = match &lightwalletd_config.shielded_history {
        Some(args) => {
            Some(CatalogRefresh::admit(args, CatalogSource::Compact(slot.clone())).await?)
        }
        None => None,
    };
    let visible_height = Some(slot.capture().event_fence().visible_tip().height.value());
    open_storage_phase.complete();
    let (incoming, listen_addr) = browser_transport::bind(lightwalletd_config.listen_addr)?;
    let runtime = SupervisedRuntime::new(RuntimeService::CompatLightwalletd);
    let cancel = runtime.stopping();
    let (node_tip_witness, node_tip_witness_handle) =
        spawn_configured_node_tip_witness(&lightwalletd_config, &cancel);
    let ops_handle = spawn_ops_endpoint_for(
        RuntimeService::CompatLightwalletd,
        lightwalletd_config.ops_listen_addr,
        env!("CARGO_PKG_VERSION"),
        encode_zinder_native_chain_name(lightwalletd_config.network),
        serving_readiness.runtime(),
        Arc::from([]),
    )
    .await?;
    let _signal_handle = runtime.request_shutdown_on_terminating_signal();
    let tasks = CompactBlocksServingTasks {
        compact_block_publisher: publisher.spawn(cancel.clone(), runtime.serving_drained()),
        node_tip_witness: node_tip_witness_handle,
        operations: ops_handle,
    }
    .supervised();
    let (server, tasks) = match composition {
        #[cfg(feature = "light-wallet-qualification")]
        CanonicalComposition::Qualification {
            source,
            ingest,
            poll_interval,
        } => {
            let adapter = LightwalletdGrpcAdapter::for_light_wallet_qualification(
                slot,
                *source,
                lightwalletd_config
                    .tree_state_replay_wait
                    .unwrap_or(DEFAULT_TREE_STATE_REPLAY_WAIT),
                lightwalletd_config
                    .tree_state_replay_waiters_per_slot
                    .unwrap_or(DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT),
            )
            .with_max_concurrent_block_range_streams(
                lightwalletd_config.max_concurrent_block_range_streams,
            )
            .with_stale_tip_limit(lightwalletd_config.stale_tip_limit)
            .with_node_tip_witness(node_tip_witness)
            .with_ping(configured_ping(&lightwalletd_config, &cancel));
            let adapter = match &catalog_refresh {
                Some(refresh) => adapter.with_published_shielded_history(refresh.history.clone()),
                None => adapter,
            };
            let (adapter, tip_publisher) =
                with_ingest_mempool(adapter, &lightwalletd_config, &cancel);
            let node_probe = adapter.spawn_node_readiness_probe(
                serving_readiness.dependencies(),
                poll_interval,
                cancel.clone(),
            );
            let ingest_probe = spawn_wallet_ingest_control_readiness_probe(
                ingest,
                serving_readiness.dependencies(),
                lightwalletd_config.storage.secondary_catchup_interval,
                cancel.clone(),
            );
            (
                browser_transport::serve(
                    &runtime,
                    adapter.into_server(),
                    incoming,
                    &lightwalletd_config,
                    &readiness,
                )?,
                tasks
                    .with("qualification node probe", node_probe)
                    .with("qualification ingest probe", ingest_probe)
                    .with("tip-change publisher", tip_publisher),
            )
        }
        CanonicalComposition::CompactBlocks => {
            let grpc_adapter = zinder_compat_lightwalletd::CompactBlocksGrpcAdapter::new(slot)
                .with_max_concurrent_block_range_streams(
                    lightwalletd_config.max_concurrent_block_range_streams,
                )
                .with_stale_tip_limit(lightwalletd_config.stale_tip_limit)
                .with_node_tip_witness(node_tip_witness)
                .with_ping(configured_ping(&lightwalletd_config, &cancel));
            let grpc_adapter = match &catalog_refresh {
                Some(refresh) => {
                    grpc_adapter.with_published_shielded_history(refresh.history.clone())
                }
                None => grpc_adapter,
            };
            (
                browser_transport::serve(
                    &runtime,
                    grpc_adapter.into_server(),
                    incoming,
                    &lightwalletd_config,
                    &readiness,
                )?,
                tasks,
            )
        }
    };
    metrics::gauge!("zinder_compat_serving_info", "serving" => lightwalletd_config.serving.as_str()).set(1.0);
    tracing::info!(
        target: "zinder::compat_lightwalletd",
        event = "compat_started",
        serving = lightwalletd_config.serving.as_str(),
        network = encode_zinder_native_chain_name(lightwalletd_config.network),
        %listen_addr,
        visible_height = ?visible_height,
        max_concurrent_block_range_streams =
            lightwalletd_config.max_concurrent_block_range_streams.get(),
        stale_tip_limit_secs = ?lightwalletd_config
            .stale_tip_limit
            .limit()
            .map(|limit| limit.as_secs()),
        ping_very_insecure = lightwalletd_config.ping_very_insecure,
        "lightwalletd-compatible gRPC server started"
    );
    let tasks = CatalogRefresh::supervise(
        catalog_refresh,
        lightwalletd_config.storage.secondary_catchup_interval,
        cancel,
        tasks,
    );
    start_api_phase.complete();
    StartupPhase::Ready.start().complete();
    let runtime_outcome = runtime
        .supervise(server, tasks, || serving_readiness.publish_shutting_down())
        .await;
    tracing::info!(
        target: "zinder::compat_lightwalletd",
        event = "compat_stopped",
        serving = lightwalletd_config.serving.as_str(),
        "lightwalletd-compatible gRPC server stopped"
    );
    Ok(runtime_outcome?)
}

/// Builds the broadcast endpoints from `[node]` and `[node_fallbacks.N]`
/// without contacting any node, or `None` when no node is configured.
fn build_node_broadcast(
    node_target: Option<&NodeTarget>,
    node_fallbacks: &[NodeTarget],
) -> Result<Option<OrderedBroadcaster<ZebraJsonRpcSource>>, LightwalletdConfigError> {
    let Some(node_target) = node_target else {
        tracing::info!(
            target: "zinder::compat_lightwalletd",
            event = "transaction_broadcast_disabled",
            "transaction broadcast disabled because [node] is not configured"
        );
        return Ok(None);
    };
    let nodes = OrderedBroadcaster::connect(node_target, node_fallbacks)
        .map_err(|source| LightwalletdConfigError::Source(Box::new(source)))?;
    tracing::info!(
        target: "zinder::compat_lightwalletd",
        event = "transaction_broadcast_enabled",
        endpoint_count = nodes.endpoint_count(),
        "transaction broadcast enabled through each node endpoint once it passes first-contact verification"
    );
    Ok(Some(nodes))
}

#[allow(
    clippy::print_stderr,
    reason = "the container runtime records the healthcheck's stderr as its failure reason"
)]
fn run_healthcheck(healthcheck: &OpsHealthcheckArgs, config_path: Option<&Path>) -> ExitCode {
    match healthcheck.run(
        config_path,
        default_ops_listen_addr(RuntimeService::CompatLightwalletd),
    ) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}: healthcheck failed: {error}", env!("CARGO_BIN_NAME"));
            ExitCode::FAILURE
        }
    }
}

#[allow(
    clippy::print_stderr,
    reason = "no tracing subscriber is installed, so stderr is the only channel left"
)]
fn emit_tracing_subscriber_error(error: &TracingSubscriberError) -> ExitCode {
    eprintln!("{}: {error}", env!("CARGO_BIN_NAME"));
    ExitCode::FAILURE
}

fn emit_runtime_error(error: &LightwalletdConfigError) -> ExitCode {
    tracing::error!(
        target: "zinder::compat_lightwalletd",
        event = "compat_run_failed",
        error = %error,
        "compat run failed"
    );
    ExitCode::FAILURE
}

impl From<Cli> for LightwalletdConfigOverrides {
    fn from(cli: Cli) -> Self {
        Self {
            network: cli.network,
            serving: cli.serving,
            canonical_primary_path: cli.canonical_primary_path,
            canonical_secondary_root: cli.canonical_secondary_root,
            wallet_primary_path: cli.wallet_primary_path,
            wallet_secondary_root: cli.wallet_secondary_root,
            ingest_control_addr: cli.ingest_control_addr,
            ingest_control_bearer_token_path: cli.ingest_control_token_path,
            listen_addr: cli.listen_addr,
            ops_listen_addr: cli.ops_listen_addr,
            node_json_rpc_addr: cli.node_json_rpc_addr,
            reorg_window_blocks: cli.reorg_window_blocks,
            ping_very_insecure: cli.ping_very_insecure,
        }
    }
}

/// Starts reading the writer's report of its node from ingest control, at
/// the cadence the reader already observes ingest control with.
fn spawn_configured_node_tip_witness(
    lightwalletd_config: &config::LightwalletdConfig,
    cancel: &CancellationToken,
) -> (NodeTipWitness, tokio::task::JoinHandle<()>) {
    spawn_node_tip_witness(
        lightwalletd_config.ingest_control_addr.clone(),
        lightwalletd_config.ingest_control_bearer_token.clone(),
        lightwalletd_config.network,
        lightwalletd_config.storage.secondary_catchup_interval,
        cancel.clone(),
    )
}

fn configured_ping(
    lightwalletd_config: &config::LightwalletdConfig,
    shutdown: &CancellationToken,
) -> LightwalletdPing {
    if lightwalletd_config.ping_very_insecure {
        LightwalletdPing::enabled(shutdown.clone())
    } else {
        LightwalletdPing::default()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use zinder_core::Network;
    use zinder_source::{DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES, NodeAuth};

    use super::*;

    fn node_target(json_rpc_addr: &str, node_auth: NodeAuth) -> NodeTarget {
        NodeTarget::new(
            Network::ZcashRegtest,
            json_rpc_addr.to_owned(),
            node_auth,
            Duration::from_secs(5),
            DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES,
        )
    }

    /// The node writes its cookie file when it starts, so a compatibility
    /// server that starts first keeps an unverified broadcast endpoint
    /// instead of exiting.
    #[test]
    fn a_missing_node_cookie_file_leaves_broadcast_unverified()
    -> Result<(), Box<dyn std::error::Error>> {
        let cookie_dir = tempfile::tempdir()?;
        let node = node_target(
            "http://127.0.0.1:1",
            NodeAuth::cookie_file(cookie_dir.path().join(".cookie")),
        );

        let node_broadcast = build_node_broadcast(Some(&node), &[])?;

        assert!(node_broadcast.is_some_and(|node| node.verified_capabilities().is_none()));
        Ok(())
    }

    /// Each endpoint's node writes its own cookie file, so a missing fallback
    /// cookie leaves that endpoint unverified instead of failing startup.
    #[test]
    fn a_missing_fallback_cookie_file_leaves_broadcast_unverified()
    -> Result<(), Box<dyn std::error::Error>> {
        let cookie_dir = tempfile::tempdir()?;
        let node = node_target("http://127.0.0.1:1", NodeAuth::None);
        let fallback = node_target(
            "http://127.0.0.1:2",
            NodeAuth::cookie_file(cookie_dir.path().join(".cookie")),
        );

        let node_broadcast = build_node_broadcast(Some(&node), &[fallback])?;

        assert!(node_broadcast.is_some_and(|nodes| {
            nodes.endpoint_count() == 2 && nodes.verified_capabilities().is_none()
        }));
        Ok(())
    }

    #[test]
    fn a_malformed_fallback_node_address_fails_startup() {
        let node = node_target("http://127.0.0.1:1", NodeAuth::None);
        let fallback = node_target("not a node address", NodeAuth::None);

        assert!(matches!(
            build_node_broadcast(Some(&node), &[fallback]),
            Err(LightwalletdConfigError::Source(_))
        ));
    }

    #[test]
    fn a_malformed_node_address_fails_startup() {
        let node = node_target("not a node address", NodeAuth::None);

        assert!(matches!(
            build_node_broadcast(Some(&node), &[]),
            Err(LightwalletdConfigError::Source(_))
        ));
    }
}
