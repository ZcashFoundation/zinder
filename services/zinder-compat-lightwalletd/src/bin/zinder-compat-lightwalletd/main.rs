//! Zinder lightwalletd-compatible gRPC server entry point.

use std::{
    future::Future,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
    time::Duration,
};
use zinder_core::wire::encode_zinder_native_chain_name;

use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;
use zinder_compat_lightwalletd::{
    IngestControlMempoolSurface, LightwalletdGrpcAdapter, LightwalletdPing,
    spawn_ingest_control_tip_change_publisher,
};
use zinder_runtime::{
    OpsHealthcheckArgs, OpsServerError, Readiness, RuntimeService, StartupPhase,
    TracingSubscriberError, TrafficReadinessInterceptor, cancel_on_terminating_signal,
    install_metrics_recorder_for_service, install_tracing_subscriber,
    section_defaults::default_ops_listen_addr, spawn_ops_endpoint_for,
};
use zinder_source::{DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS, NodeTarget, ZebraJsonRpcSource};

mod compact_block_serving;
mod config;
use config::{CompatServing, LightwalletdConfigError, LightwalletdConfigOverrides};
use zinder_query::{
    AdmittedIngestControl, OrderedBroadcaster, WalletServingPairConfig, WalletServingPairPublisher,
    WalletServingQuery, WalletServingReadiness, spawn_wallet_ingest_control_readiness_probe,
    spawn_wallet_node_readiness_probe,
};

#[global_allocator]
static GLOBAL_ALLOCATOR: zinder_runtime::RuntimeAllocator = zinder_runtime::RuntimeAllocator;

/// Longest the gRPC server may drain after shutdown starts before open calls
/// are abandoned.
///
/// A graceful drain waits for every open call, and a block-range stream to a
/// client that reads slowly stays open until its range ends. This bound keeps
/// such calls from holding the process past the wallet-serving supervisor's
/// 20-second grace window, with room left for secondary teardown.
const GRPC_SERVER_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

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
    /// Node JSON-RPC address used for transaction broadcast. Omit to disable broadcast.
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
async fn run_lightwalletd(cli: Cli) -> Result<(), LightwalletdConfigError> {
    let load_config_phase = StartupPhase::LoadConfig.start();
    let config_path = cli.config_path.clone();
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
    tracing::info!(
        target: "zinder::compat_lightwalletd",
        event = "compat_config_resolved",
        requested_serving = lightwalletd_config.serving.as_str(),
        "resolved compatibility serving configuration"
    );
    if lightwalletd_config.serving == CompatServing::CompactBlocks {
        return run_compact_blocks(lightwalletd_config).await;
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
    let visible_height = Some(
        serving_pair_slot
            .capture()
            .canonical_fence()
            .visible_tip()
            .height
            .value(),
    );
    open_storage_phase.complete();

    let wallet_query = WalletServingQuery::from_serving_pair_slot(serving_pair_slot.clone())
        .with_node_broadcast(node_broadcast.clone());
    let cancel = CancellationToken::new();
    let grpc_adapter = LightwalletdGrpcAdapter::from_admitted_compatibility_query(wallet_query)?
        .with_max_concurrent_block_range_streams(
            lightwalletd_config.max_concurrent_block_range_streams,
        )
        .with_ping(configured_ping(&lightwalletd_config, &cancel));
    let ops_handle = spawn_ops_endpoint_for(
        RuntimeService::CompatLightwalletd,
        lightwalletd_config.ops_listen_addr,
        env!("CARGO_PKG_VERSION"),
        encode_zinder_native_chain_name(lightwalletd_config.network),
        serving_readiness.runtime_readiness(),
        Arc::from([]),
    )
    .await?;
    let _signal_handle = cancel_on_terminating_signal(cancel.clone());
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
    let serving_runtime_drained = CancellationToken::new();
    let serving_pair_publisher_handle =
        serving_pair_publisher.spawn(cancel.clone(), serving_runtime_drained.clone());
    metrics::gauge!("zinder_compat_serving_info", "serving" => "wallet").set(1.0);
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

    tracing::info!(
        target: "zinder::compat_lightwalletd",
        event = "compat_started",
        serving = "wallet",
        network = encode_zinder_native_chain_name(lightwalletd_config.network),
        listen_addr = %lightwalletd_config.listen_addr,
        visible_height = ?visible_height,
        max_concurrent_block_range_streams =
            lightwalletd_config.max_concurrent_block_range_streams.get(),
        ping_very_insecure = lightwalletd_config.ping_very_insecure,
        "lightwalletd-compatible gRPC server started"
    );

    // Expose `grpc.reflection.v1.ServerReflection` so legacy lightwalletd
    // wallets and `grpcurl` can discover the served `CompactTxStreamer`
    // surface without an out-of-band proto.
    let reflection_service = zinder_runtime::public_grpc_reflection(
        zinder_proto::LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET,
    )?;

    start_api_phase.complete();
    StartupPhase::Ready.start().complete();

    let traffic_readiness = TrafficReadinessInterceptor::new(readiness.clone());
    let reflection_readiness = TrafficReadinessInterceptor::new(readiness.clone());
    let grpc_service = tonic::service::interceptor::InterceptedService::new(
        grpc_adapter.into_server(),
        traffic_readiness,
    );
    let reflection_service = tonic::service::interceptor::InterceptedService::new(
        reflection_service,
        reflection_readiness,
    );
    let server_result = serve_with_bounded_drain(
        zinder_runtime::public_grpc_server()
            .add_service(grpc_service)
            .add_service(reflection_service)
            .serve_with_shutdown(
                lightwalletd_config.listen_addr,
                cancel.clone().cancelled_owned(),
            ),
        &cancel,
    )
    .await;

    serving_readiness.publish_shutting_down();
    cancel.cancel();
    serving_runtime_drained.cancel();

    tracing::info!(
        target: "zinder::compat_lightwalletd",
        event = "compat_stopped",
        serving = "wallet",
        "lightwalletd-compatible gRPC server stopped"
    );

    if let Some(handle) = ops_handle {
        handle.shutdown().await?;
    }

    // Drain the tip-change publisher so a panic in its task surfaces in
    // the operator's log instead of vanishing silently. The publisher's
    // run loop exits when `cancel` fires, so the join completes promptly.
    match tip_publisher_handle.await {
        Ok(()) => {}
        Err(join_error) if join_error.is_panic() => tracing::warn!(
            target: "zinder::compat_lightwalletd",
            event = "tip_change_publisher_panic",
            error = %join_error,
            "tip-change publisher task panicked",
        ),
        Err(join_error) => tracing::warn!(
            target: "zinder::compat_lightwalletd",
            event = "tip_change_publisher_join_failed",
            error = %join_error,
            "tip-change publisher task did not exit cleanly",
        ),
    }

    match serving_pair_publisher_handle.await {
        Ok(()) => {}
        Err(join_error) if join_error.is_panic() => tracing::warn!(
            target: "zinder::compat_lightwalletd",
            event = "serving_pair_publisher_panic",
            error = %join_error,
            "wallet-serving pair publisher task panicked"
        ),
        Err(join_error) => tracing::warn!(
            target: "zinder::compat_lightwalletd",
            event = "serving_pair_publisher_join_failed",
            error = %join_error,
            "wallet-serving pair publisher task did not exit cleanly"
        ),
    }

    match node_readiness_handle.await {
        Ok(()) => {}
        Err(join_error) if join_error.is_panic() => tracing::warn!(
            target: "zinder::compat_lightwalletd",
            event = "wallet_node_readiness_probe_panic",
            error = %join_error,
            "wallet node-readiness probe task panicked"
        ),
        Err(join_error) => tracing::warn!(
            target: "zinder::compat_lightwalletd",
            event = "wallet_node_readiness_probe_join_failed",
            error = %join_error,
            "wallet node-readiness probe task did not exit cleanly"
        ),
    }

    match ingest_control_readiness_handle.await {
        Ok(()) => {}
        Err(join_error) if join_error.is_panic() => tracing::warn!(
            target: "zinder::compat_lightwalletd",
            event = "wallet_ingest_control_readiness_probe_panic",
            error = %join_error,
            "wallet ingest-control readiness probe task panicked"
        ),
        Err(join_error) => tracing::warn!(
            target: "zinder::compat_lightwalletd",
            event = "wallet_ingest_control_readiness_probe_join_failed",
            error = %join_error,
            "wallet ingest-control readiness probe task did not exit cleanly"
        ),
    }

    server_result.map_err(LightwalletdConfigError::Transport)
}

#[allow(
    clippy::too_many_lines,
    reason = "The compact serving branch keeps admission, readiness, server startup, and shutdown ordering auditable in one linear composition root."
)]
async fn run_compact_blocks(
    lightwalletd_config: config::LightwalletdConfig,
) -> Result<(), LightwalletdConfigError> {
    if lightwalletd_config.wallet_primary_path.is_some()
        || lightwalletd_config.wallet_secondary_root.is_some()
        || lightwalletd_config.wallet_rocksdb_budget.is_some()
    {
        return Err(LightwalletdConfigError::Config(
            zinder_runtime::ConfigError::invalid(
                "compat.serving = \"compact-blocks\" does not accept wallet.path, wallet.secondary_path, or wallet.rocksdb",
            ),
        ));
    }
    if lightwalletd_config.ops_listen_addr.is_some() {
        install_metrics_recorder_for_service(
            RuntimeService::CompatLightwalletd,
            env!("CARGO_PKG_VERSION"),
            encode_zinder_native_chain_name(lightwalletd_config.network),
        )
        .map_err(OpsServerError::from)?;
    }
    let readiness = Readiness::default();
    let serving_readiness = compact_block_serving::CompactServingReadiness::new(readiness.clone());
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
    let visible_height = Some(slot.capture().event_fence().visible_tip().height.value());
    open_storage_phase.complete();
    let cancel = CancellationToken::new();
    let grpc_adapter = compact_block_serving::CompactBlockAdapter::new(slot)
        .with_max_concurrent_block_range_streams(
            lightwalletd_config.max_concurrent_block_range_streams,
        )
        .with_ping(configured_ping(&lightwalletd_config, &cancel));
    let ops_handle = spawn_ops_endpoint_for(
        RuntimeService::CompatLightwalletd,
        lightwalletd_config.ops_listen_addr,
        env!("CARGO_PKG_VERSION"),
        encode_zinder_native_chain_name(lightwalletd_config.network),
        serving_readiness.runtime(),
        Arc::from([]),
    )
    .await?;
    let _signal_handle = cancel_on_terminating_signal(cancel.clone());
    let serving_runtime_drained = CancellationToken::new();
    let publisher_handle = publisher.spawn(cancel.clone(), serving_runtime_drained.clone());
    metrics::gauge!("zinder_compat_serving_info", "serving" => "compact-blocks").set(1.0);
    tracing::info!(
        target: "zinder::compat_lightwalletd",
        event = "compat_started",
        serving = "compact-blocks",
        network = encode_zinder_native_chain_name(lightwalletd_config.network),
        listen_addr = %lightwalletd_config.listen_addr,
        visible_height = ?visible_height,
        max_concurrent_block_range_streams =
            lightwalletd_config.max_concurrent_block_range_streams.get(),
        ping_very_insecure = lightwalletd_config.ping_very_insecure,
        "lightwalletd-compatible gRPC server started"
    );
    let reflection_service = zinder_runtime::public_grpc_reflection(
        zinder_proto::LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET,
    )?;
    start_api_phase.complete();
    StartupPhase::Ready.start().complete();
    let traffic_readiness = TrafficReadinessInterceptor::new(serving_readiness.runtime());
    let reflection_readiness = TrafficReadinessInterceptor::new(serving_readiness.runtime());
    let grpc_service = tonic::service::interceptor::InterceptedService::new(
        grpc_adapter.into_server(),
        traffic_readiness,
    );
    let reflection_service = tonic::service::interceptor::InterceptedService::new(
        reflection_service,
        reflection_readiness,
    );
    let server_result = serve_with_bounded_drain(
        zinder_runtime::public_grpc_server()
            .add_service(grpc_service)
            .add_service(reflection_service)
            .serve_with_shutdown(
                lightwalletd_config.listen_addr,
                cancel.clone().cancelled_owned(),
            ),
        &cancel,
    )
    .await;
    serving_readiness.publish_shutting_down();
    cancel.cancel();
    serving_runtime_drained.cancel();
    tracing::info!(
        target: "zinder::compat_lightwalletd",
        event = "compat_stopped",
        serving = "compact-blocks",
        "lightwalletd-compatible gRPC server stopped"
    );
    if let Some(handle) = ops_handle {
        handle.shutdown().await?;
    }
    match publisher_handle.await {
        Ok(()) => {}
        Err(error) => tracing::error!(
            target: "zinder::compat_lightwalletd",
            event = "compact_publisher_join_failed",
            error = %error,
            "compact publisher task did not exit cleanly"
        ),
    }
    server_result.map_err(LightwalletdConfigError::Transport)
}

/// Runs the gRPC server until it stops, abandoning the calls still open
/// [`GRPC_SERVER_DRAIN_TIMEOUT`] after `shutdown` is cancelled.
async fn serve_with_bounded_drain<Server>(
    server: Server,
    shutdown: &CancellationToken,
) -> Result<(), tonic::transport::Error>
where
    Server: Future<Output = Result<(), tonic::transport::Error>>,
{
    let mut server = std::pin::pin!(server);
    tokio::select! {
        server_outcome = &mut server => return server_outcome,
        () = shutdown.cancelled() => {}
    }
    if let Ok(server_outcome) = tokio::time::timeout(GRPC_SERVER_DRAIN_TIMEOUT, server).await {
        return server_outcome;
    }
    tracing::warn!(
        target: "zinder::compat_lightwalletd",
        event = "compat_server_drain_timed_out",
        timeout_seconds = GRPC_SERVER_DRAIN_TIMEOUT.as_secs(),
        "lightwalletd-compatible gRPC server did not drain before its shutdown deadline; abandoning open calls"
    );
    Ok(())
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

    /// A server whose open calls never finish stops holding the process once
    /// the drain bound passes.
    #[tokio::test(start_paused = true)]
    async fn requested_shutdown_abandons_a_server_that_does_not_drain() {
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server = async move {
            server_shutdown.cancelled().await;
            std::future::pending::<Result<(), tonic::transport::Error>>().await
        };
        let started_at = tokio::time::Instant::now();
        shutdown.cancel();

        let server_outcome = serve_with_bounded_drain(server, &shutdown).await;

        assert!(server_outcome.is_ok());
        assert_eq!(started_at.elapsed(), GRPC_SERVER_DRAIN_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn a_server_that_drains_within_the_bound_stops_when_it_drains() {
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server = async move {
            server_shutdown.cancelled().await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(())
        };
        let started_at = tokio::time::Instant::now();
        shutdown.cancel();

        let server_outcome = serve_with_bounded_drain(server, &shutdown).await;

        assert!(server_outcome.is_ok());
        assert_eq!(started_at.elapsed(), Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn a_server_that_stops_on_its_own_does_not_wait_for_shutdown() {
        let shutdown = CancellationToken::new();
        let server = async { Ok(()) };
        let started_at = tokio::time::Instant::now();

        let server_outcome = serve_with_bounded_drain(server, &shutdown).await;

        assert!(server_outcome.is_ok());
        assert_eq!(started_at.elapsed(), Duration::ZERO);
    }
}
