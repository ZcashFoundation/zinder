//! Zinder lightwalletd-compatible gRPC server entry point.

use std::{net::SocketAddr, path::PathBuf, process::ExitCode, sync::Arc};
use zinder_core::wire::encode_zinder_native_chain_name;

use clap::Parser;
use tokio_util::sync::CancellationToken;
use zinder_compat_lightwalletd::{
    IngestControlMempoolSurface, LightwalletdGrpcAdapter, spawn_ingest_control_tip_change_publisher,
};
use zinder_runtime::{
    OpsServerError, Readiness, RuntimeService, StartupPhase, TrafficReadinessInterceptor,
    cancel_on_terminating_signal, install_metrics_recorder_for_service, install_tracing_subscriber,
    spawn_ops_endpoint_for,
};
use zinder_source::{
    DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS, NodeTarget, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions,
};

mod compact_block_serving;
mod config;
use config::{CompatServing, LightwalletdConfigError, LightwalletdConfigOverrides};
use zinder_query::{
    WalletQueryApi, WalletServingPairConfig, WalletServingPairPublisher, WalletServingReadiness,
    spawn_wallet_node_readiness_probe,
};

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
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    install_tracing_subscriber();

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
    reason = "lightwalletd bootstrap composes the readiness, ops, store, broadcaster, mempool surface, tip-change publisher, secondary-catchup, and gRPC-server subsystems; the linear sequence is the operator-facing flow and intentionally lives in one function so failure ordering is auditable."
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
    let serving_readiness = WalletServingReadiness::awaiting_node_source(readiness.clone());
    let start_api_phase = StartupPhase::StartApi.start();

    let connect_node_phase = StartupPhase::ConnectNode.start();
    let broadcaster = match build_broadcaster(lightwalletd_config.broadcaster.as_ref(), true) {
        Ok(broadcaster) => broadcaster,
        Err(error) => {
            connect_node_phase.fail(&error);
            start_api_phase.fail(&error);
            return Err(error);
        }
    };
    let Some(broadcaster_source) = broadcaster.as_ref() else {
        let wrapped = LightwalletdConfigError::Source(Box::new(
            zinder_source::SourceError::SourceProtocolMismatch {
                reason: "[node] section is required so GetLightdInfo can serve a \
                         node-discovered consensus branch id",
            },
        ));
        connect_node_phase.fail(&wrapped);
        start_api_phase.fail(&wrapped);
        return Err(wrapped);
    };
    broadcaster_source
        .probe_capabilities()
        .await
        .map_err(|source_error| LightwalletdConfigError::Source(Box::new(source_error)))?;
    let network_upgrade_activations = match broadcaster_source
        .discover_network_upgrade_activations("zinder-compat-lightwalletd")
        .await
    {
        Ok(activations) => {
            connect_node_phase.complete();
            activations
        }
        Err(source_error) => {
            let wrapped = LightwalletdConfigError::Source(Box::new(source_error));
            connect_node_phase.fail(&wrapped);
            start_api_phase.fail(&wrapped);
            return Err(wrapped);
        }
    };
    let open_storage_phase = StartupPhase::OpenStorage.start();
    let (serving_pair_publisher, serving_pair_slot) =
        WalletServingPairPublisher::bootstrap_from_writer_status_endpoint(
            WalletServingPairConfig {
                canonical_primary_path: lightwalletd_config.storage.path.clone(),
                canonical_secondary_root: lightwalletd_config.storage.secondary_path.clone(),
                wallet_primary_path,
                wallet_secondary_root,
                network: lightwalletd_config.network,
                network_upgrade_activations: Arc::clone(&network_upgrade_activations),
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
            &lightwalletd_config.ingest_control_addr,
            lightwalletd_config.ingest_control_bearer_token.as_ref(),
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

    let wallet_query = zinder_query::WalletServingQuery::from_probed_node_source(
        serving_pair_slot.clone(),
        broadcaster_source.clone(),
        network_upgrade_activations.clone(),
    )?;
    let native_endpoint_capabilities = wallet_query.native_endpoint_capabilities().clone();
    let grpc_adapter = LightwalletdGrpcAdapter::from_admitted_compatibility_query(wallet_query)?;
    let ops_handle = spawn_ops_endpoint_for(
        RuntimeService::CompatLightwalletd,
        lightwalletd_config.ops_listen_addr,
        env!("CARGO_PKG_VERSION"),
        encode_zinder_native_chain_name(lightwalletd_config.network),
        serving_readiness.runtime_readiness(),
        Arc::from([]),
    )
    .await?;
    let cancel = CancellationToken::new();
    let _signal_handle = cancel_on_terminating_signal(cancel.clone());
    let node_readiness_handle = spawn_wallet_node_readiness_probe(
        broadcaster_source.clone(),
        &native_endpoint_capabilities,
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
    )?;
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
        "lightwalletd-compatible gRPC server started"
    );

    // Expose `grpc.reflection.v1.ServerReflection` so legacy lightwalletd
    // wallets and `grpcurl` can discover the served `CompactTxStreamer`
    // surface without an out-of-band proto.
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(zinder_proto::LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET)
        .build_v1()?;

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
    let server_result = tonic::transport::Server::builder()
        .add_service(grpc_service)
        .add_service(reflection_service)
        .serve_with_shutdown(
            lightwalletd_config.listen_addr,
            cancel.clone().cancelled_owned(),
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
    let connect_node_phase = StartupPhase::ConnectNode.start();
    let broadcaster = build_broadcaster(lightwalletd_config.broadcaster.as_ref(), false)?;
    let Some(broadcaster_source) = broadcaster.as_ref() else {
        let error = LightwalletdConfigError::Source(Box::new(
            zinder_source::SourceError::SourceProtocolMismatch {
                reason: "[node] section is required so compact-blocks GetLightdInfo can serve node identity",
            },
        ));
        connect_node_phase.fail(&error);
        start_api_phase.fail(&error);
        return Err(error);
    };
    broadcaster_source
        .probe_capabilities()
        .await
        .map_err(|error| LightwalletdConfigError::Source(Box::new(error)))?;
    let activations = broadcaster_source
        .discover_network_upgrade_activations("zinder-compat-lightwalletd")
        .await
        .map_err(|error| LightwalletdConfigError::Source(Box::new(error)))?;
    connect_node_phase.complete();

    let open_storage_phase = StartupPhase::OpenStorage.start();
    let (publisher, slot) = compact_block_serving::CompactBlockPublisher::bootstrap(
        compact_block_serving::CompactBlockServingConfig {
            canonical_primary_path: lightwalletd_config.storage.path.clone(),
            canonical_secondary_root: lightwalletd_config.storage.secondary_path.clone(),
            network: lightwalletd_config.network,
            activations: Arc::clone(&activations),
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
    let grpc_adapter = compact_block_serving::CompactBlockAdapter::new(slot, activations);
    let ops_handle = spawn_ops_endpoint_for(
        RuntimeService::CompatLightwalletd,
        lightwalletd_config.ops_listen_addr,
        env!("CARGO_PKG_VERSION"),
        encode_zinder_native_chain_name(lightwalletd_config.network),
        serving_readiness.runtime(),
        Arc::from([]),
    )
    .await?;
    let cancel = CancellationToken::new();
    let _signal_handle = cancel_on_terminating_signal(cancel.clone());
    let node_readiness_handle = compact_block_serving::spawn_node_readiness_probe(
        broadcaster_source.clone(),
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
    )?;
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
        "lightwalletd-compatible gRPC server started"
    );
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(zinder_proto::LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET)
        .build_v1()?;
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
    let server_result = tonic::transport::Server::builder()
        .add_service(grpc_service)
        .add_service(reflection_service)
        .serve_with_shutdown(
            lightwalletd_config.listen_addr,
            cancel.clone().cancelled_owned(),
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
    match node_readiness_handle.await {
        Ok(()) => {}
        Err(error) => tracing::error!(
            target: "zinder::compat_lightwalletd",
            event = "compact_node_readiness_join_failed",
            error = %error,
            "compact node readiness task did not exit cleanly"
        ),
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

fn build_broadcaster(
    broadcaster_target: Option<&NodeTarget>,
    announce_broadcast: bool,
) -> Result<Option<ZebraJsonRpcSource>, LightwalletdConfigError> {
    let Some(broadcaster_target) = broadcaster_target else {
        tracing::info!(
            target: "zinder::compat_lightwalletd",
            event = "transaction_broadcast_disabled",
            "transaction broadcast disabled because [node] is not configured"
        );
        return Ok(None);
    };

    let source = ZebraJsonRpcSource::with_options(
        broadcaster_target.network,
        broadcaster_target.json_rpc_addr.clone(),
        broadcaster_target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: broadcaster_target.request_timeout,
            max_response_bytes: broadcaster_target.max_response_bytes,
            broadcast_timeout: broadcaster_target.broadcast_timeout,
        },
    )
    .map_err(|source| LightwalletdConfigError::Source(Box::new(source)))?
    .with_health_config(broadcaster_target.health.clone());

    if announce_broadcast {
        tracing::info!(
            target: "zinder::compat_lightwalletd",
            event = "transaction_broadcast_enabled",
            json_rpc_addr = %broadcaster_target.json_rpc_addr,
            "transaction broadcast enabled via Zebra JSON-RPC"
        );
    }
    Ok(Some(source))
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
        }
    }
}
